/**
 * The write path's behaviour through the access contracts: DDL, modify with its constraint timing,
 * statement atomicity and savepoints, visibility under strict durability, sequences, the ledger,
 * ON CONFLICT probes, and recovery by reopen.
 */
import { describe, expect, it } from "vitest";
import { CatalogChangeError } from "../access/catalog.js";
import {
  CardinalityViolationError,
  CheckViolationError,
  ConflictKind,
  ForeignKeyViolationError,
  NotNullViolationError,
  UniqueViolationError,
} from "../access/modify.js";
import { SequenceExhaustedError } from "../access/write-path.js";
import { encodeKeyBatch, KeyBatch } from "../keys/key-encoding.js";
import { allocateSelection } from "../vector/validity.js";
import type { FlatVector } from "../vector/vector.js";
import type { WritePathImpl } from "./engine.js";
import {
  autocommit,
  batch,
  BIG,
  column,
  Harness,
  INT,
  ints,
  locators,
  rowsOf,
  rules,
  tableInput,
  TEXT,
  texts,
} from "./test-support.js";

const FREE = { allocate: () => true, free: () => undefined };

async function createTable(
  path: WritePathImpl,
  input: Parameters<typeof tableInput>[1],
  extra = {},
  name = "t",
): Promise<number> {
  let id = 0;
  await autocommit(path, async (statement) => {
    id =
      (
        await statement.changeCatalog({
          kind: "createTable",
          table: tableInput(name, input, extra),
        })
      ).id ?? 0;
  });
  return id;
}

async function rows(path: WritePathImpl, tableId: number) {
  const view = path.pin();
  try {
    return await rowsOf(view, tableId);
  } finally {
    view.release();
  }
}

/** Locators of rows whose first column is in `ids`, in a fresh statement view. */
async function locate(
  path: WritePathImpl,
  tableId: number,
  keys: readonly number[],
): Promise<number[]> {
  const view = path.pin();
  try {
    const def = path.catalog.table(tableId);
    const batchKeys = new KeyBatch(FREE);
    encodeKeyBatch(
      "equality",
      [{ vector: ints(keys), direction: "asc", nulls: "first" }],
      { length: keys.length, selection: null },
      batchKeys,
    );
    const out = {
      probes: new Int32Array(keys.length),
      locators: new Float64Array(keys.length),
      count: 0,
    };
    await view.index(tableId, def?.primaryKey ?? 0).probe(batchKeys, out);
    return Array.from(out.locators.subarray(0, out.count));
  } finally {
    view.release();
  }
}

describe("modify", () => {
  it("updates rows by locator: assigned columns replaced, the others kept, keys moved", async () => {
    const harness = new Harness();
    const path = await harness.open();
    const t = await createTable(path, [
      column("id", INT, false),
      column("name", TEXT),
      column("n", INT),
    ]);
    await autocommit(path, (s) =>
      s.table(t).insert(batch(ints([1, 2]), texts(["a", "b"]), ints([10, 20])), null),
    );
    await autocommit(path, async (s) => {
      const view = s.view;
      const def = path.catalog.table(t);
      const keys = new KeyBatch(FREE);
      encodeKeyBatch(
        "equality",
        [{ vector: ints([2]), direction: "asc", nulls: "first" }],
        { length: 1, selection: null },
        keys,
      );
      const out = { probes: new Int32Array(1), locators: new Float64Array(1), count: 0 };
      await view.index(t, def?.primaryKey ?? 0).probe(keys, out);
      const nameId = def?.columns[1]?.id ?? 0;
      const idId = def?.columns[0]?.id ?? 0;
      await s
        .table(t, { assigned: [idId, nameId] })
        .update(batch(locators([out.locators[0] ?? 0], t), ints([7]), texts(["z"])), null);
    });
    expect(await rows(path, t)).toEqual([
      [1, "a", 10],
      [7, "z", 20],
    ]);
    expect(await locate(path, t, [2])).toEqual([]);
    expect((await locate(path, t, [7])).length).toBe(1);
    await harness.close();
  });

  it("deletes by locator; the key leaves the index; deleted rows leave scans", async () => {
    const harness = new Harness();
    const path = await harness.open();
    const t = await createTable(path, [column("id", INT, false)]);
    await autocommit(path, (s) => s.table(t).insert(batch(ints([1, 2, 3])), null));
    const [two] = await locate(path, t, [2]);
    await autocommit(path, (s) => s.table(t).delete(batch(locators([two ?? 0], t)), null));
    expect(await rows(path, t)).toEqual([[1], [3]]);
    expect(await locate(path, t, [2])).toEqual([]);
    const view = path.pin();
    const table = view.table(t, { budget: FREE });
    expect(table.liveRowCount).toBe(2);
    view.release();
    await harness.close();
  });

  it("checks NOT NULL and CHECK in the call, unique keys at the statement's end", async () => {
    const harness = new Harness();
    const path = await harness.open();
    const t = await createTable(path, [column("id", INT, false), column("n", INT)], {
      checks: [{ name: "n_positive", conditionSql: "n > 0", columns: [1] }],
    });
    await expect(
      autocommit(path, (s) => s.table(t).insert(batch(ints([null]), ints([1])), null)),
    ).rejects.toThrow(NotNullViolationError);
    const failing = () => ({
      check: () => ({ row: 0, checkId: path.catalog.table(t)?.checks[0]?.id ?? 0 }),
      generate: () => undefined,
    });
    const tx = await path.begin();
    const statement = tx.statement({ rules: failing });
    expect(() => statement.table(t).insert(batch(ints([1]), ints([-1])), null)).toThrow(
      CheckViolationError,
    );
    statement.rollback();
    tx.rollback();
    await autocommit(path, (s) => s.table(t).insert(batch(ints([1]), ints([1])), null));
    await expect(
      autocommit(path, (s) => s.table(t).insert(batch(ints([1]), ints([2])), null)),
    ).rejects.toThrow(UniqueViolationError);
    expect(await rows(path, t)).toEqual([[1, 1]]);
    await harness.close();
  });

  it("lets one statement swap two keys (unique checked at the end, not per row)", async () => {
    const harness = new Harness();
    const path = await harness.open();
    const t = await createTable(path, [column("id", INT, false)]);
    await autocommit(path, (s) => s.table(t).insert(batch(ints([1, 2])), null));
    const located = await locate(path, t, [1, 2]);
    const idColumn = path.catalog.table(t)?.columns[0]?.id ?? 0;
    await autocommit(path, (s) =>
      s.table(t, { assigned: [idColumn] }).update(batch(locators(located, t), ints([2, 1])), null),
    );
    expect((await rows(path, t)).sort()).toEqual([[1], [2]]);
    await harness.close();
  });

  it("refuses a second update of one row (error), or keeps the first (first)", async () => {
    const harness = new Harness();
    const path = await harness.open();
    const t = await createTable(path, [column("id", INT, false), column("n", INT)]);
    await autocommit(path, (s) => s.table(t).insert(batch(ints([1]), ints([0])), null));
    const [one] = await locate(path, t, [1]);
    const n = path.catalog.table(t)?.columns[1]?.id ?? 0;
    const twice = batch(locators([one ?? 0, one ?? 0], t), ints([5, 6]));
    await expect(
      autocommit(path, (s) => s.table(t, { assigned: [n] }).update(twice, null)),
    ).rejects.toThrow(CardinalityViolationError);
    let written = 0;
    await autocommit(path, async (s) => {
      written = await s.table(t, { assigned: [n], onRepeat: "first" }).update(twice, null);
    });
    expect(written).toBe(1);
    expect(await rows(path, t)).toEqual([[1, 5]]);
    await harness.close();
  });

  it("enforces foreign keys at the statement's end, self references included", async () => {
    const harness = new Harness();
    const path = await harness.open();
    const parent = await createTable(path, [column("id", INT, false)], {}, "parent");
    const child = await createTable(
      path,
      [column("id", INT, false), column("parent", INT)],
      {
        foreignKeys: [
          {
            name: "fk",
            columns: [1],
            parentTableId: parent,
            parentIndexId: "primary",
            onDelete: "restrict",
            enforced: true,
          },
        ],
      },
      "child",
    );
    await expect(
      autocommit(path, (s) => s.table(child).insert(batch(ints([1]), ints([9])), null)),
    ).rejects.toThrow(ForeignKeyViolationError);
    // A child inserted before its parent in one transaction's statements: each statement checks.
    await autocommit(path, async (s) => {
      await s.table(parent).insert(batch(ints([9])), null);
    });
    await autocommit(path, (s) =>
      s.table(child).insert(batch(ints([1, 2]), ints([9, null])), null),
    );
    const [nine] = await locate(path, parent, [9]);
    await expect(
      autocommit(path, (s) => s.table(parent).delete(batch(locators([nine ?? 0], parent)), null)),
    ).rejects.toThrow(ForeignKeyViolationError);
    const tree = await createTable(
      path,
      [column("id", INT, false), column("up", INT)],
      {
        foreignKeys: [
          {
            name: "up",
            columns: [1],
            parentTableId: "self",
            parentIndexId: "primary",
            onDelete: "restrict",
            enforced: true,
          },
        ],
      },
      "tree",
    );
    // A row referencing itself, and one referencing a row the same statement writes.
    await autocommit(path, (s) => s.table(tree).insert(batch(ints([1, 2]), ints([1, 1])), null));
    expect(await rows(path, tree)).toEqual([
      [1, 1],
      [2, 1],
    ]);
    await harness.close();
  });

  it("reports ON CONFLICT clashes: existing, this statement, earlier in the batch", async () => {
    const harness = new Harness();
    const path = await harness.open();
    const t = await createTable(path, [column("id", INT, false)]);
    await autocommit(path, (s) => s.table(t).insert(batch(ints([1])), null));
    const tx = await path.begin();
    const s = tx.statement({ rules });
    const writer = s.table(t);
    await writer.insert(batch(ints([2])), null);
    const out = {
      kinds: new Uint8Array(4),
      locators: new Float64Array(4),
      earlier: new Int32Array(4),
      indexIds: new Int32Array(4),
    };
    await writer.conflicts(batch(ints([1, 2, 3, 3])), [], out);
    expect(Array.from(out.kinds)).toEqual([
      ConflictKind.existing,
      ConflictKind.statement,
      ConflictKind.none,
      ConflictKind.batch,
    ]);
    expect(out.earlier[3]).toBe(2);
    await s.end();
    tx.rollback();
    await harness.close();
  });

  it("a statement view, made on first use, shows the catalog the statement began with", async () => {
    const harness = new Harness();
    const path = await harness.open();
    const tx = await path.begin();
    const s = tx.statement({ rules });
    await s.changeCatalog({
      kind: "createTable",
      table: tableInput("late", [column("id", INT, false)]),
    });
    // The view is first read after the statement's own DDL: it still predates it.
    expect(s.view.catalog.tableByName("late")).toBeUndefined();
    expect(tx.catalog.tableByName("late")).toBeDefined();
    await s.end();
    const next = tx.statement({ rules });
    expect(next.view.catalog.tableByName("late")).toBeDefined();
    await next.end();
    tx.rollback();
    await harness.close();
  });
});

describe("transactions", () => {
  it("rolls back a statement, to a savepoint, or the whole transaction", async () => {
    const harness = new Harness();
    const path = await harness.open();
    const t = await createTable(path, [column("id", INT, false)]);
    const tx = await path.begin();
    let s = tx.statement({ rules });
    await s.table(t).insert(batch(ints([1])), null);
    await s.end();
    const point = tx.savepoint();
    s = tx.statement({ rules });
    await s.table(t).insert(batch(ints([2])), null);
    await s.end();
    s = tx.statement({ rules });
    await s.table(t).insert(batch(ints([3])), null);
    s.rollback();
    tx.rollbackTo(point);
    s = tx.statement({ rules });
    await s.table(t).insert(batch(ints([4])), null);
    await s.end();
    await (
      await tx.commit(null)
    ).durable;
    expect(await rows(path, t)).toEqual([[1], [4]]);
    const tx2 = await path.begin();
    const s2 = tx2.statement({ rules });
    await s2.table(t).insert(batch(ints([5])), null);
    await s2.end();
    tx2.rollback();
    expect(await rows(path, t)).toEqual([[1], [4]]);
    // Row ids of rolled back rows are reused only by rows nobody saw.
    await autocommit(path, (s3) => s3.table(t).insert(batch(ints([6])), null));
    expect(await rows(path, t)).toEqual([[1], [4], [6]]);
    await harness.close();
  });

  it("hides a statement's own writes from its view and shows them to the next statement", async () => {
    const harness = new Harness();
    const path = await harness.open();
    const t = await createTable(path, [column("id", INT, false)]);
    await autocommit(path, (s) => s.table(t).insert(batch(ints([0])), null));
    const tx = await path.begin();
    const s1 = tx.statement({ rules });
    await s1.table(t).insert(batch(ints([1])), null);
    expect(await rowsOf(s1.view, t)).toEqual([[0]]);
    await s1.end();
    const s2 = tx.statement({ rules });
    expect(await rowsOf(s2.view, t)).toEqual([[0], [1]]);
    await s2.end();
    // Readers see nothing pending.
    expect(await rows(path, t)).toEqual([[0]]);
    tx.rollback();
    await harness.close();
  });

  it("shows strict readers only durable commits; the writer slot is FIFO", async () => {
    const harness = new Harness();
    const path = await harness.open();
    const t = await createTable(path, [column("id", INT, false)]);
    const tx = await path.begin();
    const order: number[] = [];
    const second = path.begin().then((next) => {
      order.push(2);
      next.rollback();
    });
    const s = tx.statement({ rules });
    await s.table(t).insert(batch(ints([1])), null);
    await s.end();
    order.push(1);
    const receipt = await tx.commit(null);
    expect(receipt.wrote).toBe(true);
    const early = path.pin();
    expect(early.bound).toBeLessThan(receipt.lsn);
    expect(await rowsOf(early, t)).toEqual([]);
    early.release();
    await receipt.durable;
    expect(await rows(path, t)).toEqual([[1]]);
    await second;
    expect(order).toEqual([1, 2]);
    await harness.close();
  });

  it("writes no frame for a transaction with no effects", async () => {
    const harness = new Harness();
    const path = await harness.open();
    const before = path.appliedLsn;
    const tx = await path.begin();
    const receipt = await tx.commit({
      clientId: new Uint8Array(16),
      sequence: 1,
      sentAtMs: 0,
      counts: {},
    });
    expect(receipt.wrote).toBe(false);
    expect(path.appliedLsn).toBe(before);
    await harness.close();
  });

  it("records committed outcomes in the ledger once durable", async () => {
    const harness = new Harness();
    const path = await harness.open();
    const t = await createTable(path, [column("id", INT, false)]);
    const clientId = new Uint8Array(16).fill(7);
    const identity = {
      clientId: "07".repeat(16),
      sessionId: "s",
      generation: 1,
      sequence: 3,
      sentAtMs: Date.now(),
      resolvedThrough: 0,
      scope: "standalone" as const,
    };
    expect(path.ledger.admit(identity, Date.now()).kind).toBe("execute");
    const tx = await path.begin();
    const s = tx.statement({ rules });
    await s.table(t).insert(batch(ints([1])), null);
    await s.end();
    const receipt = await tx.commit({
      clientId,
      sequence: 3,
      sentAtMs: identity.sentAtMs,
      counts: { inserted: 1 },
    });
    await receipt.durable;
    await Promise.resolve();
    expect(path.ledger.admit(identity, Date.now()).kind).toBe("duplicate");
    // The outcome survives a reopen (replayed from the frame's request).
    const reopened = await harness.reopen();
    expect(reopened.ledger.admit(identity, Date.now()).kind).toBe("duplicate");
    await harness.close();
  });
});

describe("sequences and catalog", () => {
  it("creates owned identity sequences with the table; values are not reused across a reopen", async () => {
    const harness = new Harness();
    let path = await harness.open();
    const t = await createTable(path, [
      { ...column("id", BIG, false), identity: { start: 1n, increment: 1n } },
    ]);
    const def = path.catalog.table(t);
    const sequenceId = def?.columns[0]?.identitySequence ?? -1;
    expect(path.catalog.sequence(sequenceId)?.ownedBy).toEqual({
      tableId: t,
      columnId: def?.columns[0]?.id,
    });
    const out = { repr: "safe" as const, values: new Float64Array(4) };
    const lsn = path.sequences.next(sequenceId, 3, out, 0);
    expect(Array.from(out.values.subarray(0, 3))).toEqual([1, 2, 3]);
    await path.durable(lsn);
    path = await harness.reopen();
    path.sequences.next(sequenceId, 1, out, 0);
    expect(out.values[0]).toBeGreaterThan(3);
    await harness.close();
  });

  it("refuses exhausted sequences without consuming", async () => {
    const harness = new Harness();
    const path = await harness.open();
    let id = 0;
    await autocommit(path, async (s) => {
      id =
        (
          await s.changeCatalog({
            kind: "createSequence",
            sequence: {
              name: "s",
              increment: 1n,
              min: 1n,
              max: 2n,
              start: 1n,
              cycle: false,
              ownedBy: null,
            },
          })
        ).id ?? 0;
    });
    const out = { repr: "safe" as const, values: new Float64Array(3) };
    expect(() => path.sequences.next(id, 3, out, 0)).toThrow(SequenceExhaustedError);
    path.sequences.next(id, 2, out, 0);
    expect(Array.from(out.values.subarray(0, 2))).toEqual([1, 2]);
    await harness.close();
  });

  it("keeps enum types in the catalog and refuses dropping one in use", async () => {
    const harness = new Harness();
    const path = await harness.open();
    let typeId = 0;
    await autocommit(path, async (s) => {
      typeId =
        (
          await s.changeCatalog({
            kind: "createType",
            type: { name: "mood", labels: ["sad", "ok"] },
          })
        ).id ?? 0;
    });
    expect(path.catalog.typeByName("mood")?.id).toBe(typeId);
    await createTable(path, [
      column("id", INT, false),
      column("m", { kind: "enum", name: "mood", labels: ["sad", "ok"] }),
    ]);
    await expect(
      autocommit(path, (s) => s.changeCatalog({ kind: "dropType", typeId })),
    ).rejects.toThrow(CatalogChangeError);
    await harness.close();
  });

  it("drops a table and its owned sequences; ids are not reused", async () => {
    const harness = new Harness();
    const path = await harness.open();
    const t = await createTable(path, [column("id", INT, false)]);
    await autocommit(path, (s) => s.changeCatalog({ kind: "dropTable", tableId: t }));
    expect(path.catalog.table(t)).toBeUndefined();
    const u = await createTable(path, [column("id", INT, false)], {}, "u");
    expect(u).toBeGreaterThan(t);
    await harness.close();
  });

  it("serves synchronous planning statistics", async () => {
    const harness = new Harness();
    const path = await harness.open();
    const t = await createTable(path, [column("id", INT, false), column("name", TEXT)]);
    await autocommit(path, (s) =>
      s.table(t).insert(batch(ints([1, 2, 3]), texts(["a", null, "c"])), null),
    );
    expect(path.statistics(t)?.rowCount).toBe(3);
    expect(path.statistics(9999)).toBeNull();
    await harness.close();
  });

  it("creates an index over existing rows and probes it", async () => {
    const harness = new Harness();
    const path = await harness.open();
    const t = await createTable(path, [column("id", INT, false), column("n", INT)]);
    await autocommit(path, (s) => s.table(t).insert(batch(ints([1, 2, 3]), ints([5, 5, 6])), null));
    const n = path.catalog.table(t)?.columns[1]?.id ?? 0;
    let indexId = 0;
    await autocommit(path, async (s) => {
      indexId =
        (
          await s.changeCatalog({
            kind: "createIndex",
            index: { name: "by_n", tableId: t, columnIds: [n], unique: false, role: "secondary" },
          })
        ).id ?? 0;
    });
    const probe = async (target: WritePathImpl) => {
      const view = target.pin();
      const keys = new KeyBatch(FREE);
      encodeKeyBatch(
        "equality",
        [{ vector: ints([5]), direction: "asc", nulls: "first" }],
        { length: 1, selection: null },
        keys,
      );
      const out = { probes: new Int32Array(4), locators: new Float64Array(4), count: 0 };
      await view.index(t, indexId).probe(keys, out);
      view.release();
      return out.count;
    };
    expect(await probe(path)).toBe(2);
    expect(await probe(await harness.reopen())).toBe(2);
    await harness.close();
  });
});

describe("index reads", () => {
  it("resumes a probe whose matches overflow the output, inside one key", async () => {
    const harness = new Harness();
    const path = await harness.open();
    const t = await createTable(path, [column("id", INT, false), column("n", INT)]);
    let indexId = 0;
    const n = () => path.catalog.table(t)?.columns[1]?.id ?? 0;
    await autocommit(path, async (s) => {
      indexId =
        (
          await s.changeCatalog({
            kind: "createIndex",
            index: { name: "by_n", tableId: t, columnIds: [n()], unique: false, role: "secondary" },
          })
        ).id ?? 0;
    });
    await autocommit(path, (s) =>
      s.table(t).insert(batch(ints([1, 2, 3, 4]), ints([5, 5, 5, 6])), null),
    );
    const view = path.pin();
    const keys = new KeyBatch(FREE);
    encodeKeyBatch(
      "equality",
      [{ vector: ints([5, 6]), direction: "asc", nulls: "first" }],
      { length: 2, selection: null },
      keys,
    );
    const out = { probes: new Int32Array(2), locators: new Float64Array(2), count: 0 };
    const reader = view.index(t, indexId);
    const seen: Array<[number, number]> = [];
    let result = await reader.probe(keys, out);
    for (;;) {
      for (let m = 0; m < out.count; m++) seen.push([out.probes[m] ?? -1, out.locators[m] ?? -1]);
      if (result.done) break;
      result = await reader.probe(keys, out, result.resume);
    }
    expect(seen).toEqual([
      [0, 0],
      [0, 1],
      [0, 2],
      [1, 3],
    ]);
    const cursor = reader.range({ lower: null, upper: null }, "desc");
    const located = new Float64Array(8);
    const count = await cursor.next(located);
    expect(Array.from(located.subarray(0, count))).toEqual([3, 2, 1, 0]);
    cursor.close();
    view.release();
    await harness.close();
  });
});

describe("snapshot isolation of deletes", () => {
  it("keeps a row visible to a reader pinned before its delete committed", async () => {
    const harness = new Harness();
    const path = await harness.open();
    const t = await createTable(path, [column("id", INT, false)]);
    await autocommit(path, (s) => s.table(t).insert(batch(ints([1, 2])), null));
    const before = path.pin();
    const [two] = await locate(path, t, [2]);
    await autocommit(path, (s) => s.table(t).delete(batch(locators([two ?? 0], t)), null));
    expect(await rowsOf(before, t)).toEqual([[1], [2]]);
    const keys = new KeyBatch(FREE);
    encodeKeyBatch(
      "equality",
      [{ vector: ints([2]), direction: "asc", nulls: "first" }],
      { length: 1, selection: null },
      keys,
    );
    const out = { probes: new Int32Array(1), locators: new Float64Array(1), count: 0 };
    await before.index(t, path.catalog.table(t)?.primaryKey ?? 0).probe(keys, out);
    expect(out.count).toBe(1);
    before.release();
    expect(await locate(path, t, [2])).toEqual([]);
    await harness.close();
  });
});

describe("deletion vectors", () => {
  it("filter live rows in a selection", async () => {
    const harness = new Harness();
    const path = await harness.open();
    const t = await createTable(path, [column("id", INT, false)]);
    await autocommit(path, (s) => s.table(t).insert(batch(ints([1, 2, 3, 4])), null));
    const [two, four] = await locate(path, t, [2, 4]);
    await autocommit(path, (s) =>
      s.table(t).delete(batch(locators([two ?? 0, four ?? 0], t)), null),
    );
    const view = path.pin();
    const table = view.table(t, { budget: FREE });
    const segment = table.segments[0];
    const deletions = await segment?.deletions();
    const out = allocateSelection(4);
    deletions?.liveRows(0, 4, null, out);
    expect(Array.from(out.rows.subarray(0, out.count))).toEqual([0, 2]);
    view.release();
    await harness.close();
  });
});

export type { FlatVector };

describe("values stored out of line (page/large-value.ts)", () => {
  // Values past LARGE_VALUE_BYTES live in the chunk's own extent and decode only once
  // ChunkReader.loadValues has read them; every write-path read of a published chunk must.
  it("update, delete and an index build read a flushed row whose text is over 64 KiB", async () => {
    const harness = new Harness("strict", "memory", 2);
    let path = await harness.open();
    const t = await createTable(path, [
      column("id", INT, false),
      column("body", TEXT),
      column("n", INT),
    ]);
    const big = "x".repeat(100_000);
    await autocommit(path, (s) =>
      s
        .table(t)
        .insert(
          batch(ints([1, 2, 3, 4]), texts([big, `${big}y`, "small", big]), ints([1, 2, 3, 4])),
          null,
        ),
    );
    // A reader pinned before the flush is re-pointed to the published groups (view.ts).
    const early = path.pin();
    // A later commit freezes the memtable the big commit ended in, so the whole commit flushes.
    await autocommit(path, (s) =>
      s.table(t).insert(batch(ints([5, 6]), texts(["e", "f"]), ints([5, 6])), null),
    );
    await path.quiesce();
    expect(path.publications).toBeGreaterThan(0);
    expect(path.stats().repointPins).toBe(1);
    const repointed = (await rowsOf(early, t)).sort((a, b) => Number(a[0]) - Number(b[0]));
    early.release();
    expect(repointed.map((row) => row[1])).toEqual([big, `${big}y`, "small", big]);
    path = await harness.reopen();
    const pinned = path.pin();
    const kinds = pinned.table(t, { budget: FREE }).segments.map((segment) => segment.kind);
    pinned.release();
    expect(kinds.slice(0, 2)).toEqual(["group", "group"]);
    const n = path.catalog.table(t)?.columns[2]?.id ?? 0;
    await autocommit(path, async (s) => {
      await s.changeCatalog({
        kind: "createIndex",
        index: { name: "by_n", tableId: t, columnIds: [n], unique: false, role: "secondary" },
      });
    });
    const [one] = await locate(path, t, [1]);
    await autocommit(path, (s) =>
      s.table(t, { assigned: [n] }).update(batch(locators([one ?? 0], t), ints([10])), null),
    );
    const [two] = await locate(path, t, [2]);
    await autocommit(path, (s) => s.table(t).delete(batch(locators([two ?? 0], t)), null));
    const live = (await rows(path, t)).sort((a, b) => Number(a[0]) - Number(b[0]));
    expect(live).toEqual([
      [1, big, 10],
      [3, "small", 3],
      [4, big, 4],
      [5, "e", 5],
      [6, "f", 6],
    ]);
    await harness.close();
  }, 120_000);
});
