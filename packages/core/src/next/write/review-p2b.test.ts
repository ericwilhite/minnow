/**
 * Review B3-p2b-1 findings 1 and 2: the reviewer's foreign-key probes, as tests (the generated
 * coverage is fk-generated.test.ts).
 */
import { PGlite } from "@electric-sql/pglite";
import { expect, it } from "vitest";
import type { TableInput } from "../access/catalog.js";
import { encodeKeyBatch, KeyBatch } from "../keys/key-encoding.js";
import { need } from "./bytes.js";
import type { WritePathImpl } from "./engine.js";
import {
  autocommit,
  batch,
  column,
  Harness,
  INT,
  ints,
  locators,
  rowsOf,
  tableInput,
} from "./test-support.js";

const FREE = { allocate: () => true, free: () => undefined };

async function create(
  path: WritePathImpl,
  name: string,
  input: TableInput["columns"],
  extra: Partial<TableInput> = {},
): Promise<number> {
  let id = 0;
  await autocommit(path, async (s) => {
    id =
      (await s.changeCatalog({ kind: "createTable", table: tableInput(name, input, extra) })).id ??
      0;
  });
  return id;
}

function fk(parent: number, onDelete: "restrict" | "cascade" | "setNull", columns = [1]) {
  return {
    name: `fk_${String(parent)}_${onDelete}`,
    columns,
    parentTableId: parent,
    parentIndexId: "primary" as const,
    onDelete,
    enforced: true,
  };
}

async function locate(path: WritePathImpl, table: number, keys: number[]): Promise<number[]> {
  const view = path.pin();
  const encoded = new KeyBatch(FREE);
  encodeKeyBatch(
    "equality",
    [{ vector: ints(keys), direction: "asc", nulls: "first" }],
    { length: keys.length, selection: null },
    encoded,
  );
  const out = {
    probes: new Int32Array(keys.length),
    locators: new Float64Array(keys.length),
    count: 0,
  };
  await view.index(table, path.catalog.table(table)?.primaryKey ?? 0).probe(encoded, out);
  view.release();
  encoded.close();
  return Array.from(out.locators.subarray(0, out.count));
}

async function sorted(path: WritePathImpl, table: number) {
  const view = path.pin();
  const rows = await rowsOf(view, table);
  view.release();
  return rows.sort((a, b) => Number(a[0]) - Number(b[0]));
}

it("review: overlapping old/new parent keys follow PG", async () => {
  const pg = await PGlite.create();
  const h = new Harness();
  const path = await h.open();
  try {
    await pg.exec(
      "CREATE TABLE p(id int primary key); CREATE TABLE c(id int primary key, ref int REFERENCES p ON UPDATE CASCADE); INSERT INTO p VALUES(2),(1); INSERT INTO c VALUES(10,1),(20,2); UPDATE p SET id=id+1",
    );
    const expected = (
      await pg.query<{ id: number; ref: number }>("SELECT id,ref FROM c ORDER BY id")
    ).rows.map((r) => [r.id, r.ref]);
    const p = await create(path, "p", [column("id", INT, false)]);
    const c = await create(path, "c", [column("id", INT, false), column("ref", INT)], {
      foreignKeys: [fk(p, "cascade")],
    });
    await autocommit(path, (s) => s.table(p).insert(batch(ints([2, 1])), null));
    await autocommit(path, (s) => s.table(c).insert(batch(ints([10, 20]), ints([1, 2])), null));
    const at = await locate(path, p, [2, 1]);
    await autocommit(path, (s) =>
      s
        .table(p, { assigned: [need(need(path.catalog.table(p)).columns[0]).id] })
        .update(batch(locators(at, p), ints([3, 2])), null),
    );
    expect(await sorted(path, c)).toEqual(expected);
  } finally {
    await h.close();
    await pg.close();
  }
}, 60000);

it("review: diamond delete cascade", async () => {
  const h = new Harness();
  const path = await h.open();
  try {
    const a = await create(path, "a", [column("id", INT, false)]);
    const b = await create(path, "b", [column("id", INT, false), column("a", INT)], {
      foreignKeys: [fk(a, "cascade")],
    });
    const c = await create(path, "c", [column("id", INT, false), column("a", INT)], {
      foreignKeys: [fk(a, "cascade")],
    });
    const d = await create(
      path,
      "d",
      [column("id", INT, false), column("b", INT), column("c", INT)],
      { foreignKeys: [fk(b, "cascade", [1]), fk(c, "cascade", [2])] },
    );
    for (const t of [a, b, c, d])
      await autocommit(path, (s) =>
        s
          .table(t)
          .insert(
            t === a
              ? batch(ints([1]))
              : t === d
                ? batch(ints([1]), ints([1]), ints([1]))
                : batch(ints([1]), ints([1])),
            null,
          ),
      );
    const at = await locate(path, a, [1]);
    await autocommit(path, (s) => s.table(a).delete(batch(locators(at, a)), null));
    for (const t of [a, b, c, d]) expect(await sorted(path, t)).toEqual([]);
  } finally {
    await h.close();
  }
});

it("review: two SET NULL paths with a freeze between actions", async () => {
  const h = new Harness("strict", "memory", 1);
  const path = await h.open();
  try {
    const p = await create(path, "p", [column("id", INT, false)]);
    const c = await create(
      path,
      "c",
      [column("id", INT, false), column("r1", INT), column("r2", INT)],
      {
        foreignKeys: [
          { ...fk(p, "setNull", [1]), name: "f1" },
          { ...fk(p, "setNull", [2]), name: "f2" },
        ],
      },
    );
    await autocommit(path, (s) => s.table(p).insert(batch(ints([1])), null));
    await autocommit(path, (s) => s.table(c).insert(batch(ints([10]), ints([1]), ints([1])), null));
    const at = await locate(path, p, [1]);
    await autocommit(path, (s) => s.table(p).delete(batch(locators(at, p)), null));
    expect(await sorted(path, c)).toEqual([[10, null, null]]);
  } finally {
    await h.close();
  }
});

it("review: PG accepts both SET NULL paths and composite MATCH SIMPLE", async () => {
  const pg = await PGlite.create();
  try {
    await pg.exec(
      "CREATE TABLE p(id int primary key);CREATE TABLE c(id int primary key,r1 int REFERENCES p ON DELETE SET NULL,r2 int REFERENCES p ON DELETE SET NULL);INSERT INTO p VALUES(1);INSERT INTO c VALUES(10,1,1);DELETE FROM p",
    );
    expect((await pg.query("SELECT * FROM c")).rows).toEqual([{ id: 10, r1: null, r2: null }]);
  } finally {
    await pg.close();
  }
  const h = new Harness();
  const path = await h.open();
  try {
    const p = await create(path, "p", [column("a", INT, false), column("b", INT, false)], {
      primaryKey: [0, 1],
    });
    const c = await create(
      path,
      "c",
      [column("id", INT, false), column("a", INT), column("b", INT)],
      { foreignKeys: [fk(p, "setNull", [1, 2])] },
    );
    await autocommit(path, (s) => s.table(p).insert(batch(ints([1]), ints([2])), null));
    await autocommit(path, (s) =>
      s
        .table(c)
        .insert(batch(ints([10, 20, 30]), ints([1, null, 999]), ints([2, 999, null])), null),
    );
    // First inserted row has locator zero; delete the composite parent.
    await autocommit(path, (s) => s.table(p).delete(batch(locators([0], p)), null));
    expect(await sorted(path, c)).toEqual([
      [10, null, null],
      [20, null, 999],
      [30, 999, null],
    ]);
  } finally {
    await h.close();
  }
}, 60000);

it("review 2 B1: two foreign keys sharing a child column follow an overlapping rekey once", async () => {
  const pg = await PGlite.create();
  const h = new Harness();
  const path = await h.open();
  try {
    await pg.exec(
      "CREATE TABLE p(id int primary key); CREATE TABLE c(id int primary key, ref int REFERENCES p ON UPDATE CASCADE, FOREIGN KEY(ref) REFERENCES p ON UPDATE CASCADE); INSERT INTO p VALUES(2),(1); INSERT INTO c VALUES(10,1),(20,2); UPDATE p SET id=id+1",
    );
    const expected = (
      await pg.query<{ id: number; ref: number }>("SELECT id,ref FROM c ORDER BY id")
    ).rows.map((r) => [r.id, r.ref]);
    expect(expected).toEqual([
      [10, 2],
      [20, 3],
    ]);
    const p = await create(path, "p", [column("id", INT, false)]);
    const c = await create(path, "c", [column("id", INT, false), column("ref", INT)], {
      foreignKeys: [
        { ...fk(p, "cascade"), name: "f1" },
        { ...fk(p, "cascade"), name: "f2" },
      ],
    });
    await autocommit(path, (s) => s.table(p).insert(batch(ints([2, 1])), null));
    await autocommit(path, (s) => s.table(c).insert(batch(ints([10, 20]), ints([1, 2])), null));
    const at = await locate(path, p, [2, 1]);
    await autocommit(path, (s) =>
      s
        .table(p, { assigned: [need(need(path.catalog.table(p)).columns[0]).id] })
        .update(batch(locators(at, p), ints([3, 2])), null),
    );
    expect(await sorted(path, c)).toEqual(expected);
  } finally {
    await h.close();
    await pg.close();
  }
}, 60000);
