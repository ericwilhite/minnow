/**
 * Foreign-key actions at statement end (access/modify.ts): ON DELETE CASCADE / SET NULL /
 * RESTRICT and rekey follow, through the same writers (their own constraints and actions apply).
 */
import { PGlite } from "@electric-sql/pglite";
import { describe, expect, it } from "vitest";
import type { TableInput } from "../access/catalog.js";
import { ForeignKeyViolationError, NotNullViolationError } from "../access/modify.js";
import { encodeKeyBatch, KeyBatch } from "../keys/key-encoding.js";
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

function fk(parent: number | "self", onDelete: "restrict" | "cascade" | "setNull", columns = [1]) {
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
  return Array.from(out.locators.subarray(0, out.count));
}

async function sorted(path: WritePathImpl, table: number) {
  const view = path.pin();
  const rows = await rowsOf(view, table);
  view.release();
  return rows.sort((a, b) => Number(a[0]) - Number(b[0]));
}

describe("foreign-key actions", () => {
  it("cascades deletes through two levels", async () => {
    const harness = new Harness();
    const path = await harness.open();
    const a = await create(path, "a", [column("id", INT, false)]);
    const b = await create(path, "b", [column("id", INT, false), column("a", INT)], {
      foreignKeys: [fk(a, "cascade")],
    });
    const c = await create(path, "c", [column("id", INT, false), column("b", INT)], {
      foreignKeys: [fk(b, "cascade")],
    });
    await autocommit(path, (s) => s.table(a).insert(batch(ints([1, 2])), null));
    await autocommit(path, (s) =>
      s.table(b).insert(batch(ints([10, 11, 20]), ints([1, 1, 2])), null),
    );
    await autocommit(path, (s) =>
      s.table(c).insert(batch(ints([100, 110, 200]), ints([10, 11, 20])), null),
    );
    const [one] = await locate(path, a, [1]);
    await autocommit(path, (s) => s.table(a).delete(batch(locators([one ?? 0], a)), null));
    expect(await sorted(path, a)).toEqual([[2]]);
    expect(await sorted(path, b)).toEqual([[20, 2]]);
    expect(await sorted(path, c)).toEqual([[200, 20]]);
    await harness.close();
  });

  it("sets references to NULL, and fails a NOT NULL reference", async () => {
    const harness = new Harness();
    const path = await harness.open();
    const p = await create(path, "p", [column("id", INT, false)]);
    const loose = await create(path, "loose", [column("id", INT, false), column("p", INT)], {
      foreignKeys: [fk(p, "setNull")],
    });
    const strict = await create(
      path,
      "strict",
      [column("id", INT, false), column("p", INT, false)],
      { foreignKeys: [fk(p, "setNull")] },
    );
    await autocommit(path, (s) => s.table(p).insert(batch(ints([1, 2])), null));
    await autocommit(path, (s) => s.table(loose).insert(batch(ints([10, 11]), ints([1, 2])), null));
    await autocommit(path, (s) => s.table(strict).insert(batch(ints([20]), ints([2])), null));
    const [one, two] = await locate(path, p, [1, 2]);
    await autocommit(path, (s) => s.table(p).delete(batch(locators([one ?? 0], p)), null));
    expect(await sorted(path, loose)).toEqual([
      [10, null],
      [11, 2],
    ]);
    await expect(
      autocommit(path, (s) => s.table(p).delete(batch(locators([two ?? 0], p)), null)),
    ).rejects.toThrow(NotNullViolationError);
    expect(await sorted(path, p)).toEqual([[2]]);
    await harness.close();
  });

  it("restricts, and lets a key move when references follow it (rekey)", async () => {
    const harness = new Harness();
    const path = await harness.open();
    const p = await create(path, "p", [column("id", INT, false), column("n", INT)]);
    const r = await create(path, "r", [column("id", INT, false), column("p", INT)], {
      foreignKeys: [fk(p, "restrict")],
    });
    await autocommit(path, (s) => s.table(p).insert(batch(ints([1, 2]), ints([0, 0])), null));
    await autocommit(path, (s) => s.table(r).insert(batch(ints([10, 11]), ints([1, 1])), null));
    const [one] = await locate(path, p, [1]);
    await expect(
      autocommit(path, (s) => s.table(p).delete(batch(locators([one ?? 0], p)), null)),
    ).rejects.toThrow(ForeignKeyViolationError);
    const id = path.catalog.table(p)?.columns[0]?.id ?? 0;
    await autocommit(path, (s) =>
      s.table(p, { assigned: [id] }).update(batch(locators([one ?? 0], p), ints([7])), null),
    );
    expect(await sorted(path, p)).toEqual([
      [2, 0],
      [7, 0],
    ]);
    expect(await sorted(path, r)).toEqual([
      [10, 7],
      [11, 7],
    ]);
    // An update that keeps the key leaves references alone.
    const [seven] = await locate(path, p, [7]);
    const n = path.catalog.table(p)?.columns[1]?.id ?? 0;
    await autocommit(path, (s) =>
      s.table(p, { assigned: [n] }).update(batch(locators([seven ?? 0], p), ints([5])), null),
    );
    expect(await sorted(path, r)).toEqual([
      [10, 7],
      [11, 7],
    ]);
    await harness.close();
  });

  it("cascades a self-referencing tree, and survives a reopen", async () => {
    const harness = new Harness();
    const path = await harness.open();
    const t = await create(path, "tree", [column("id", INT, false), column("up", INT)], {
      foreignKeys: [fk("self", "cascade")],
    });
    await autocommit(path, (s) =>
      s.table(t).insert(batch(ints([1, 2, 3, 4, 5]), ints([null, 1, 2, 3, null])), null),
    );
    const [root] = await locate(path, t, [1]);
    await autocommit(path, (s) => s.table(t).delete(batch(locators([root ?? 0], t)), null));
    expect(await sorted(path, t)).toEqual([[5, null]]);
    const reopened = await harness.reopen();
    expect(await sorted(reopened, t)).toEqual([[5, null]]);
    await harness.close();
  });

  describe("cycles end, as PostgreSQL's (review B3-p2a-2 follow-up: can the action loop hang?)", () => {
    // The loop ends because each round consumes one parent check, and new checks come only from a
    // live row deleted or a referenced key changed. A row is changed at most once per statement
    // by the actions (onRepeat "first"), and deleted rows are not live. Each case here would
    // otherwise loop; PostgreSQL gives the expected rows.
    async function postgres(statements: readonly string[], table: string): Promise<unknown[][]> {
      const pg = await PGlite.create();
      try {
        for (const text of statements) await pg.exec(text);
        const result = await pg.query<{ id: number; ref: number | null }>(
          `SELECT id, ref FROM ${table} ORDER BY id`,
        );
        return result.rows.map((row) => [row.id, row.ref]);
      } finally {
        await pg.close();
      }
    }

    async function selfTable(onDelete: "restrict" | "cascade" | "setNull") {
      const harness = new Harness();
      const path = await harness.open();
      const t = await create(path, "cyc", [column("id", INT, false), column("ref", INT)], {
        foreignKeys: [fk("self", onDelete)],
      });
      return { harness, path, t };
    }

    const ddl = (action: string) =>
      `CREATE TABLE cyc(id INTEGER PRIMARY KEY, ref INTEGER REFERENCES cyc(id)${action} ON UPDATE CASCADE)`;

    it("a CASCADE cycle (1 → 2 → 1) and a self reference delete to nothing", async () => {
      const { harness, path, t } = await selfTable("cascade");
      await autocommit(path, (s) =>
        s.table(t).insert(batch(ints([1, 2, 3]), ints([2, 1, 3])), null),
      );
      const [one] = await locate(path, t, [1]);
      await autocommit(path, (s) => s.table(t).delete(batch(locators([one ?? 0], t)), null));
      const expected = await postgres(
        [
          ddl(" ON DELETE CASCADE"),
          "INSERT INTO cyc VALUES (1, 2), (2, 1), (3, 3)",
          "DELETE FROM cyc WHERE id = 1",
        ],
        "cyc",
      );
      expect(await sorted(path, t)).toEqual(expected);
      const [three] = await locate(path, t, [3]);
      await autocommit(path, (s) => s.table(t).delete(batch(locators([three ?? 0], t)), null));
      expect(await sorted(path, t)).toEqual([]);
      await harness.close();
    }, 30_000);

    it("a SET NULL cycle", async () => {
      const { harness, path, t } = await selfTable("setNull");
      await autocommit(path, (s) => s.table(t).insert(batch(ints([1, 2]), ints([2, 1])), null));
      const [one] = await locate(path, t, [1]);
      await autocommit(path, (s) => s.table(t).delete(batch(locators([one ?? 0], t)), null));
      expect(await sorted(path, t)).toEqual(
        await postgres(
          [
            ddl(" ON DELETE SET NULL"),
            "INSERT INTO cyc VALUES (1, 2), (2, 1)",
            "DELETE FROM cyc WHERE id = 1",
          ],
          "cyc",
        ),
      );
      await harness.close();
    }, 30_000);

    it("a rekey of a row that references itself, and of a cycle", async () => {
      const { harness, path, t } = await selfTable("restrict");
      await autocommit(path, (s) =>
        s.table(t).insert(batch(ints([1, 2, 7, 8]), ints([1, 1, 8, 7])), null),
      );
      const idColumn = path.catalog.table(t)?.columns[0]?.id ?? 0;
      const rekey = async (from: number, to: number) => {
        const [at] = await locate(path, t, [from]);
        await autocommit(path, (s) =>
          s
            .table(t, { assigned: [idColumn] })
            .update(batch(locators([at ?? 0], t), ints([to])), null),
        );
      };
      await rekey(1, 5);
      await rekey(7, 70);
      expect(await sorted(path, t)).toEqual(
        await postgres(
          [
            ddl(""),
            "INSERT INTO cyc VALUES (1, 1), (2, 1), (7, 8), (8, 7)",
            "UPDATE cyc SET id = 5 WHERE id = 1",
            "UPDATE cyc SET id = 70 WHERE id = 7",
          ],
          "cyc",
        ),
      );
      await harness.close();
    }, 30_000);
  });
});
