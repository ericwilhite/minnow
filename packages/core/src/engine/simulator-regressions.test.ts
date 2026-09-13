/**
 * Defects the interaction-plan simulator found (packages/core/src/testing/interaction-simulator.ts),
 * each reduced to the smallest deterministic statement sequence that reproduces it. The seeded
 * suites keep exploring; these pin what they already caught.
 */
import { IDBFactory } from "fake-indexeddb";
import { describe, expect, it } from "vitest";
import { MemoryOpfs } from "../testing/opfs-shim.js";
import {
  IndexedDbBlockStore,
  MemoryBlockStore,
  OpfsBlockStore,
  type BlockStore,
} from "../storage/index.js";
import { MinnowDatabase } from "./database.js";

const stores: ReadonlyArray<{ name: string; open: () => Promise<BlockStore> }> = [
  { name: "memory", open: async () => new MemoryBlockStore() },
  {
    name: "indexeddb",
    open: () =>
      IndexedDbBlockStore.open({ name: crypto.randomUUID(), indexedDB: new IDBFactory() }),
  },
  {
    name: "opfs",
    open: () => OpfsBlockStore.open({ name: crypto.randomUUID(), root: new MemoryOpfs().root }),
  },
];

/** Ids that sort backwards, so the second segment of a commit sorts before the first. */
function descendingIds(): () => string {
  let next = 1_000_000;
  return () => `id-${String(next--)}`;
}

describe.each(stores)("simulator regressions over $name", ({ open }) => {
  it("folds a transaction that wrote two segments to one table", async () => {
    // A SQL transaction holding a predicate DELETE and an INSERT on the same table commits two
    // segments with one logical order and one committed version. The merge planner ordered
    // its sources by segment id as the tie-break while readers order them by position within
    // the commit; whenever the ids disagreed with that position the fold refused with
    // "Mutation compaction sources are not in canonical logical order", and the table could
    // never be compacted again.
    const store = await open();
    const database = new MinnowDatabase(store, {
      rowsPerBlock: 4,
      autoCompact: false,
      autoCollect: false,
      createId: descendingIds(),
    });
    await database.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)");
    await database.execute("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)");
    await database.execute("BEGIN");
    await database.execute("DELETE FROM t WHERE v >= 20");
    await database.execute("INSERT INTO t VALUES (2, 21), (4, 40)");
    await database.execute("COMMIT");
    const before = await database.query("SELECT * FROM t ORDER BY id", { memoize: false });
    expect(before.rows).toEqual([
      { id: 1, v: 10 },
      { id: 2, v: 21 },
      { id: 4, v: 40 },
    ]);
    const compaction = await database.compactTable("t");
    expect(compaction.compacted).toBe(true);
    const after = await database.query("SELECT * FROM t ORDER BY id", { memoize: false });
    expect(after.rows).toEqual(before.rows);
    await database.close();
    store.close();
  });
});

describe.each(stores)("composite index over nullable columns on $name", ({ open }) => {
  it("does not prune rows whose unconstrained trailing column is NULL", async () => {
    // A tuple-v1 composite index only held rows whose every indexed value was non-null, so an
    // index on (c1, c0) had no entry for a row with c0 NULL. A prefix lookup on c1 alone that
    // pruned through it dropped that row: a SELECT missed it and an UPDATE reported one row too
    // few. tuple-v2 names such a row under a NULL component marker, so the lookup both prunes and
    // returns it; row 5 is the one with c0 NULL.
    const store = await open();
    const database = new MinnowDatabase(store, { rowsPerBlock: 4, autoCompact: false });
    await database.execute(
      "CREATE TABLE t (id INTEGER PRIMARY KEY, c0 DOUBLE PRECISION, c1 INTEGER NOT NULL)",
    );
    await database.execute(
      "INSERT INTO t VALUES (1, -9.75, -4), (5, NULL, 50), (6, 13.5, 1), (9, -4, -13), (11, 2.25, 47), (14, 9.5, -1), (17, 15.75, -32), (22, 2.25, -40), (32, 2.25, -31), (39, 2.25, 36)",
    );
    await database.execute("CREATE INDEX t_c1_c0 ON t (c1, c0)");
    expect(await database.explain("SELECT id FROM t WHERE c1 > 18 ORDER BY id")).toContain(
      "a ready secondary index prunes",
    );
    const selected = await database.query("SELECT id FROM t WHERE c1 > 18 ORDER BY id", {
      memoize: false,
    });
    expect(selected.rows.map((row) => row.id)).toEqual([5, 11, 39]);
    const updated = await database.execute("UPDATE t SET c1 = c1 + 2 WHERE c1 > 18");
    expect(updated.kind === "update" && updated.rowCount).toBe(3);
    const after = await database.query("SELECT id, c1 FROM t WHERE c1 > 18 ORDER BY id", {
      memoize: false,
    });
    expect(after.rows).toEqual([
      { id: 5, c1: 52 },
      { id: 11, c1: 49 },
      { id: 39, c1: 38 },
    ]);
    // The same prefix through a constrained leading column still prunes when the trailing
    // column is constrained too (every candidate row is non-null there by the predicate).
    const both = await database.query("SELECT id FROM t WHERE c1 = 49 AND c0 = 2.25", {
      memoize: false,
    });
    expect(both.rows.map((row) => row.id)).toEqual([11]);
    await database.close();
    store.close();
  });
});

describe.each(stores)("block-level index pruning keeps every delta on $name", ({ open }) => {
  it("never serves a stale intermediate value from a set-operation member", async () => {
    // Two updates to one row: the first moved it into the predicate's range along with its
    // neighbours, the second moved it back out. The index no longer names the row, so pruning
    // dropped the second update's segment while the whole block holding the row was kept for
    // its neighbours -- and the first update was replayed onto it. The UNION ALL member (a
    // materialized input) then returned the row at the value the second update had replaced.
    const store = await open();
    const database = new MinnowDatabase(store, { rowsPerBlock: 4, autoCompact: false });
    await database.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, c1 DOUBLE PRECISION)");
    await database.execute(
      "INSERT INTO t VALUES (1, 5), (2, 5), (3, 5), (4, 5), (5, 5), (6, 5), (7, 5), (8, 5), (9, 5), (10, 5)",
    );
    await database.execute("UPDATE t SET c1 = -17.75 WHERE id <> 10");
    await database.execute("UPDATE t SET c1 = -18.5 WHERE id = 3");
    // Built after both updates, as a rebuilt base is: it names row 3 under -18.5 only.
    await database.execute("CREATE INDEX t_c1 ON t (c1)");
    const expected = [1, 2, 4, 5, 6, 7, 8, 9];
    const member = await database.query(
      "SELECT id FROM t WHERE c1 = -17.75 UNION ALL SELECT id FROM t WHERE FALSE",
      { memoize: false },
    );
    expect(member.rows.map((row) => row.id).sort((a, b) => Number(a) - Number(b))).toEqual(
      expected,
    );
    const range = await database.query(
      "SELECT id FROM t WHERE c1 >= -18 UNION ALL SELECT id FROM t WHERE FALSE",
      { memoize: false },
    );
    expect(range.rows.map((row) => row.id).sort((a, b) => Number(a) - Number(b))).toEqual([
      ...expected,
      10,
    ]);
    const plain = await database.query("SELECT id FROM t WHERE c1 = -17.75 ORDER BY id", {
      memoize: false,
    });
    expect(plain.rows.map((row) => row.id)).toEqual(expected);
    await database.close();
    store.close();
  });
});

describe.each(stores)("zone-map pruning keeps every delete on $name", ({ open }) => {
  it("replays a re-inserted key after a pruned range query", async () => {
    // Delete a key, insert it again in a block that also spans the probed range, then update
    // rows so the replay has keys to map. Zone-map pruning dropped the delete because its key
    // block could not match the predicate, so the replay saw the key in two kept blocks with
    // nothing unmapping it in between and refused every keyed range read, UPDATE, and DELETE
    // on the table as "Stored table contains a duplicate unique key".
    const store = await open();
    const database = new MinnowDatabase(store, { rowsPerBlock: 8, autoCompact: false });
    await database.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)");
    await database.execute("INSERT INTO t VALUES (1, 1), (2, 2), (3, 3), (4, 4), (5, 5), (6, 6)");
    await database.execute("DELETE FROM t WHERE id = 3");
    await database.execute("INSERT INTO t VALUES (3, 33), (7, 7)");
    await database.execute("UPDATE t SET v = v + 1 WHERE id IN (3, 5)");
    const inList = await database.query("SELECT id, v FROM t WHERE id IN (5)", { memoize: false });
    expect(inList.rows).toEqual([{ id: 5, v: 6 }]);
    const range = await database.query("SELECT id, v FROM t WHERE id > 4 ORDER BY id", {
      memoize: false,
    });
    expect(range.rows).toEqual([
      { id: 5, v: 6 },
      { id: 6, v: 6 },
      { id: 7, v: 7 },
    ]);
    const updated = await database.execute("UPDATE t SET v = 55 WHERE id IN (5)");
    expect(updated.kind === "update" && updated.rowCount).toBe(1);
    const deleted = await database.execute("DELETE FROM t WHERE id = 5");
    expect(deleted.kind === "delete" && deleted.rowCount).toBe(1);
    const all = await database.query("SELECT id, v FROM t ORDER BY id", { memoize: false });
    expect(all.rows).toEqual([
      { id: 1, v: 1 },
      { id: 2, v: 2 },
      { id: 3, v: 34 },
      { id: 4, v: 4 },
      { id: 6, v: 6 },
      { id: 7, v: 7 },
    ]);
    await database.close();
    store.close();
  });
});

describe("IndexedDB unique-key cache across adapter instances", () => {
  it("does not carry a stale key set past another instance's commit", async () => {
    // Two tabs over one IndexedDB database. Tab B cached t3's keys, tab A inserted key 9 into
    // t3, then tab B committed to another table. That commit moved B's cached key set to the
    // new manifest version as if nothing had changed in between, so B's next insert of key 9
    // checked a set without it and was accepted: two rows with one primary key.
    const indexedDB = new IDBFactory();
    const name = crypto.randomUUID();
    const storeA = await IndexedDbBlockStore.open({ name, indexedDB });
    const storeB = await IndexedDbBlockStore.open({ name, indexedDB });
    const a = new MinnowDatabase(storeA, { autoCompact: false, autoCollect: false });
    const b = new MinnowDatabase(storeB, { autoCompact: false, autoCollect: false });
    await a.execute("CREATE TABLE t3 (id INTEGER PRIMARY KEY, c0 BOOLEAN)");
    await a.execute("CREATE TABLE t2 (id INTEGER PRIMARY KEY, c0 INTEGER)");
    await a.execute("INSERT INTO t2 VALUES (1, 1)");
    await b.execute("INSERT INTO t3 VALUES (7, TRUE)");
    await a.execute("INSERT INTO t3 VALUES (9, FALSE), (4, TRUE)");
    await b.execute("UPDATE t2 SET c0 = 2 WHERE id = 1");
    await expect(b.execute("INSERT INTO t3 VALUES (14, TRUE), (9, TRUE)")).rejects.toThrow(
      /duplicate/iu,
    );
    for (const database of [a, b]) {
      const rows = await database.query("SELECT id FROM t3 ORDER BY id", { memoize: false });
      expect(rows.rows.map((row) => row.id)).toEqual([4, 7, 9]);
    }
    await a.close();
    await b.close();
    storeA.close();
    storeB.close();
  });
});

describe.each(stores)("index pruning keeps every delete on $name", ({ open }) => {
  it("replays a range-deleted, reinserted key under an exact index selection", async () => {
    // The index names the live rows exactly, so the delete segment covering the old row had no
    // candidate and was dropped from the pruned scan. The replay still maps every touched key
    // to the base rows holding it, and with the delete gone the reinserted key was mapped twice
    // and the whole table refused as holding a duplicate unique key.
    const store = await open();
    const database = new MinnowDatabase(store, { rowsPerBlock: 4, autoCompact: false });
    await database.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, c0 INTEGER, c1 BOOLEAN)");
    await database.execute(
      "INSERT INTO t VALUES (1, 0, TRUE), (2, 0, FALSE), (3, 0, FALSE), (4, 0, FALSE), (5, 0, FALSE), (6, 0, FALSE), (7, 0, FALSE), (8, 0, FALSE)",
    );
    await database.execute("CREATE INDEX t_c1 ON t (c1)");
    // Two blocks of deleted keys, neither holding a row the predicate will name.
    await database.execute("DELETE FROM t WHERE id >= 3");
    await database.execute("INSERT INTO t VALUES (3, 0, FALSE), (9, 0, TRUE)");
    await database.execute("UPDATE t SET c0 = c0 + 1 WHERE id IN (1, 3, 9)");
    const count = await database.query("SELECT COUNT(*) AS n FROM t WHERE c1 = TRUE", {
      memoize: false,
    });
    expect(count.rows[0]?.n).toBe(2);
    const rows = await database.query("SELECT id, c0 FROM t WHERE c1 = TRUE ORDER BY id", {
      memoize: false,
    });
    expect(rows.rows).toEqual([
      { id: 1, c0: 1 },
      { id: 9, c0: 1 },
    ]);
    await database.close();
    store.close();
  });
});
