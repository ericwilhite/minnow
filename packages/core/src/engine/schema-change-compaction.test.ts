/**
 * Folds across schema changes. A segment written before ALTER TABLE ADD COLUMN has no blocks
 * for the new column, and reads give its rows the column's backfill (a literal DEFAULT) or NULL.
 * After DROP COLUMN, an update segment whose only changed column was the dropped one keeps just
 * its key, and changes nothing. Folds must produce exactly what reads show — through every fold
 * path: keyed merges with update, delete, and upsert deltas, partitioned level-one folds, keyless
 * rechunks, and level-two promotions — and retire the inert updates, on every store.
 *
 * Through 0.15.0 every fold over such a table was refused, so level-zero segments piled up one per
 * commit until the 4,096-segment ceiling refused every write to it.
 */
import { IDBFactory } from "fake-indexeddb";
import { describe, expect, it, vi } from "vitest";
import {
  IndexedDbBlockStore,
  MemoryBlockStore,
  OpfsBlockStore,
  type BlockStore,
} from "../storage/index.js";
import { MemoryOpfs } from "../testing/opfs-shim.js";
import { MinnowDatabase, type CompactTableOptions } from "./database.js";
import { allVisibleSegments, heavyTestTimeout } from "./storage-test-helpers.js";

vi.setConfig({ testTimeout: heavyTestTimeout(120_000) });

const stores = [
  { name: "memory", create: async (): Promise<BlockStore> => new MemoryBlockStore() },
  {
    name: "indexeddb",
    create: async (): Promise<BlockStore> =>
      IndexedDbBlockStore.open({ name: crypto.randomUUID(), indexedDB: new IDBFactory() }),
  },
  {
    name: "opfs",
    create: async (): Promise<BlockStore> =>
      OpfsBlockStore.open({ name: crypto.randomUUID(), root: new MemoryOpfs().root }),
  },
];

interface Scenario {
  name: string;
  table: string;
  /** Orders the comparison query; every scenario's rows are unique by it. */
  orderBy: string;
  setup: readonly string[];
  /** Statements after the first fold, so a second fold sees the first one's output too. */
  after?: readonly string[];
  fold?: CompactTableOptions;
  /** Folds the history before the schema change, so the change lands on folded output. */
  foldBeforeChange?: boolean;
  change?: readonly string[];
}

const seedKeyed = [
  "CREATE TABLE t (id INTEGER PRIMARY KEY, c INTEGER, c0 INTEGER)",
  "INSERT INTO t VALUES (1, 1, 1), (2, 2, 2), (3, 3, 3), (4, 4, 4)",
  "INSERT INTO t VALUES (5, 5, 5), (6, 6, 6)",
  "UPDATE t SET c = 30 WHERE id = 3",
  "DELETE FROM t WHERE id = 4",
  "INSERT INTO t VALUES (6, 60, 61), (7, 7, 7) ON CONFLICT (id) DO UPDATE SET c = excluded.c, c0 = excluded.c0",
];

const scenarios: Scenario[] = [
  {
    name: "ADD COLUMN on a populated table, then writes using it",
    table: "t",
    orderBy: "id",
    setup: seedKeyed,
    change: [
      "ALTER TABLE t ADD COLUMN a BOOLEAN",
      "INSERT INTO t VALUES (8, 8, 8, TRUE)",
      "UPDATE t SET a = FALSE WHERE id = 2",
      "UPDATE t SET c = 10 WHERE id = 1",
      "DELETE FROM t WHERE id = 5",
      "INSERT INTO t VALUES (9, 9, 9, NULL), (1, 11, 12, TRUE) ON CONFLICT (id) DO UPDATE SET c = excluded.c, a = excluded.a",
    ],
    after: ["INSERT INTO t VALUES (10, 10, 10, TRUE)", "UPDATE t SET a = TRUE WHERE id = 3"],
  },
  {
    name: "DROP COLUMN leaving key-only update segments",
    table: "t",
    orderBy: "id",
    setup: seedKeyed,
    change: [
      "UPDATE t SET c0 = 9 WHERE id = 1",
      "UPDATE t SET c0 = 19 WHERE id = 2",
      "ALTER TABLE t DROP COLUMN c0",
    ],
    after: ["UPDATE t SET c = 99 WHERE id = 1", "INSERT INTO t VALUES (20, 20)"],
  },
  {
    name: "a column added, written, then dropped",
    table: "t",
    orderBy: "id",
    setup: seedKeyed,
    change: [
      "ALTER TABLE t ADD COLUMN a INTEGER",
      "UPDATE t SET a = 5 WHERE id = 2",
      "INSERT INTO t VALUES (8, 8, 8, 8)",
      "ALTER TABLE t DROP COLUMN a",
    ],
    after: ["INSERT INTO t VALUES (9, 9, 9)"],
  },
  {
    name: "a column dropped and added back under the same name",
    table: "t",
    orderBy: "id",
    setup: seedKeyed,
    change: [
      "UPDATE t SET c0 = 70 WHERE id = 7",
      "ALTER TABLE t DROP COLUMN c0",
      "ALTER TABLE t ADD COLUMN c0 TEXT",
      "INSERT INTO t VALUES (8, 8, 'eight')",
      "UPDATE t SET c0 = 'two' WHERE id = 2",
    ],
    after: ["UPDATE t SET c0 = 'one' WHERE id = 1"],
  },
  {
    name: "NOT NULL DEFAULT constants backfill earlier rows",
    table: "t",
    orderBy: "id",
    setup: seedKeyed,
    change: [
      "ALTER TABLE t ADD COLUMN n INTEGER NOT NULL DEFAULT 7",
      "ALTER TABLE t ADD COLUMN s TEXT NOT NULL DEFAULT 'west'",
      "ALTER TABLE t ADD COLUMN f BOOLEAN DEFAULT TRUE",
      "ALTER TABLE t ADD COLUMN d TIMESTAMP DEFAULT TIMESTAMP '2024-02-03 04:05:06'",
      "ALTER TABLE t ADD COLUMN m NUMERIC(10, 2) DEFAULT 1.5",
      "INSERT INTO t (id, c, c0) VALUES (8, 8, 8)",
      "INSERT INTO t VALUES (9, 9, 9, 1, 'east', FALSE, '2025-01-01 00:00:00', 2.25)",
      "UPDATE t SET s = 'north' WHERE id = 2",
    ],
    after: ["UPDATE t SET n = 70 WHERE id = 3"],
  },
  {
    name: "expression and absent defaults leave earlier rows NULL",
    table: "t",
    orderBy: "id",
    setup: seedKeyed,
    change: [
      "ALTER TABLE t ADD COLUMN e TEXT DEFAULT (lower('ABC'))",
      "ALTER TABLE t ADD COLUMN plain INTEGER",
      "INSERT INTO t (id, c, c0) VALUES (8, 8, 8)",
    ],
  },
  {
    name: "a renamed column next to an added one",
    table: "t",
    orderBy: "id",
    setup: seedKeyed,
    change: [
      "ALTER TABLE t RENAME COLUMN c TO amount",
      "ALTER TABLE t ADD COLUMN note TEXT",
      "UPDATE t SET amount = 33, note = 'x' WHERE id = 3",
      "INSERT INTO t VALUES (8, 8, 8, 'eight')",
    ],
  },
  {
    name: "a composite key",
    table: "ck",
    orderBy: "a, b",
    setup: [
      "CREATE TABLE ck (a INTEGER, b INTEGER, v INTEGER, PRIMARY KEY (a, b))",
      "INSERT INTO ck VALUES (1, 1, 1), (1, 2, 2), (2, 1, 3)",
      "UPDATE ck SET v = 20 WHERE a = 1 AND b = 2",
    ],
    change: [
      "ALTER TABLE ck ADD COLUMN w TEXT DEFAULT 'w'",
      "INSERT INTO ck VALUES (3, 3, 9, 'z')",
      "UPDATE ck SET w = 'u' WHERE a = 2 AND b = 1",
      "DELETE FROM ck WHERE a = 1 AND b = 1",
    ],
  },
  {
    name: "a stored generated column",
    table: "g",
    orderBy: "id",
    setup: [
      "CREATE TABLE g (id INTEGER PRIMARY KEY, x INTEGER, y INTEGER GENERATED ALWAYS AS (x * 2) STORED)",
      "INSERT INTO g (id, x) VALUES (1, 1), (2, 2), (3, 3)",
      "UPDATE g SET x = 30 WHERE id = 3",
    ],
    change: [
      "ALTER TABLE g ADD COLUMN z INTEGER",
      "INSERT INTO g (id, x, z) VALUES (4, 4, 4)",
      "UPDATE g SET x = 20, z = 2 WHERE id = 2",
    ],
  },
  {
    name: "a keyless table (rechunk)",
    table: "logs",
    orderBy: "seq",
    setup: [
      "CREATE TABLE logs (seq INTEGER, msg TEXT, level INTEGER)",
      "INSERT INTO logs VALUES (1, 'a', 1), (2, 'b', 2)",
      "INSERT INTO logs VALUES (3, 'c', 3)",
    ],
    change: [
      "ALTER TABLE logs ADD COLUMN host TEXT",
      "ALTER TABLE logs ADD COLUMN region TEXT NOT NULL DEFAULT 'west'",
      "INSERT INTO logs VALUES (4, 'd', 4, 'h4', 'east')",
      "ALTER TABLE logs DROP COLUMN level",
      "INSERT INTO logs VALUES (5, 'e', 'h5', 'north')",
    ],
    after: ["INSERT INTO logs VALUES (6, 'f', NULL, 'south')"],
  },
  {
    name: "a keyless table promoted to level two",
    table: "logs",
    orderBy: "seq",
    fold: { targetLevel: 2, maxWriteAmplification: 1_000 },
    setup: [
      "CREATE TABLE logs (seq INTEGER, msg TEXT)",
      "INSERT INTO logs VALUES (1, 'a'), (2, 'b')",
      "INSERT INTO logs VALUES (3, 'c')",
    ],
    change: [
      "ALTER TABLE logs ADD COLUMN host TEXT DEFAULT 'h'",
      "INSERT INTO logs VALUES (4, 'd', 'h4')",
    ],
    after: ["INSERT INTO logs VALUES (5, 'e', 'h5')"],
  },
  {
    name: "partitioned level-one folds touching earlier partitions",
    table: "p",
    orderBy: "id",
    fold: { partitionRows: 8 },
    foldBeforeChange: true,
    setup: [
      "CREATE TABLE p (id INTEGER PRIMARY KEY, v INTEGER, w INTEGER)",
      ...[0, 10, 20, 30].map(
        (start) =>
          `INSERT INTO p VALUES ${Array.from({ length: 10 }, (_, offset) => `(${String(start + offset)}, ${String(start + offset)}, ${String(start + offset)})`).join(", ")}`,
      ),
    ],
    change: [
      "UPDATE p SET w = 100 WHERE id = 3",
      "UPDATE p SET w = 200 WHERE id = 30",
      "ALTER TABLE p ADD COLUMN tag TEXT DEFAULT 'old'",
      "ALTER TABLE p DROP COLUMN w",
      "UPDATE p SET tag = 'new' WHERE id = 12",
      "UPDATE p SET v = -1 WHERE id = 25",
      "DELETE FROM p WHERE id = 17",
      "INSERT INTO p VALUES (40, 40, 'tail'), (41, 41, NULL)",
    ],
    after: ["UPDATE p SET tag = 'later' WHERE id = 1", "INSERT INTO p VALUES (42, 42, 'x')"],
  },
];

/** Folds every level-zero segment, one fold at a time, until none is left. */
async function foldAll(
  database: MinnowDatabase,
  table: string,
  options: CompactTableOptions = {},
): Promise<number> {
  let folds = 0;
  for (let attempt = 0; attempt < 32; attempt += 1) {
    const result = await database.compactTable(table, { minimumLevel0Segments: 1, ...options });
    if (!result.compacted) {
      expect(result.skipReason).toBe("below-segment-threshold");
      return folds;
    }
    folds += 1;
  }
  throw new Error(`Folds of ${table} did not converge`);
}

async function levelZeroCount(
  database: MinnowDatabase,
  store: BlockStore,
  table: string,
): Promise<number> {
  let count = 0;
  for (const segment of await allVisibleSegments(database, table)) {
    const record = await store.getSegment(segment.id);
    if (record === undefined) throw new Error(`Visible segment is missing: ${segment.id}`);
    if (record.level === 0) count += 1;
  }
  return count;
}

async function contents(database: MinnowDatabase, scenario: Scenario): Promise<unknown[]> {
  return (
    await database.query(`SELECT * FROM ${scenario.table} ORDER BY ${scenario.orderBy}`, {
      memoize: false,
    })
  ).rows;
}

describe.each(stores)("folds across schema changes on $name", ({ create }) => {
  it.each(scenarios)("$name", async (scenario) => {
    const store = await create();
    const options = { autoCompact: false, autoCollect: false } as const;
    const database = new MinnowDatabase(store, options);
    try {
      for (const sql of scenario.setup) await database.execute(sql);
      if (scenario.foldBeforeChange === true) {
        expect(await foldAll(database, scenario.table, scenario.fold)).toBeGreaterThan(0);
      }
      for (const sql of scenario.change ?? []) await database.execute(sql);
      const expected = await contents(database, scenario);
      expect(await levelZeroCount(database, store, scenario.table)).toBeGreaterThan(0);

      expect(await foldAll(database, scenario.table, scenario.fold)).toBeGreaterThan(0);
      expect(await levelZeroCount(database, store, scenario.table)).toBe(0);
      expect(await contents(database, scenario)).toEqual(expected);

      // Another connection reads the folded output cold.
      const cold = new MinnowDatabase(store, options);
      expect(await contents(cold, scenario)).toEqual(expected);

      // Writes continue, and the next fold reads the first fold's output.
      for (const sql of scenario.after ?? []) await database.execute(sql);
      const later = await contents(database, scenario);
      if ((scenario.after ?? []).length > 0) {
        expect(await foldAll(database, scenario.table, scenario.fold)).toBeGreaterThan(0);
      }
      expect(await levelZeroCount(database, store, scenario.table)).toBe(0);
      expect(await contents(database, scenario)).toEqual(later);
      expect(await contents(cold, scenario)).toEqual(later);
    } finally {
      await database.close();
    }
  });

  it("keeps automatic folds running on a table that gained a column", async () => {
    const store = await create();
    const errors: unknown[] = [];
    const database = new MinnowDatabase(store, {
      onBackgroundError: (error) => errors.push(error),
    });
    try {
      await database.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, c INTEGER, c0 INTEGER)");
      await database.execute("INSERT INTO t VALUES (1, 1, 1), (2, 2, 2)");
      await database.execute("UPDATE t SET c0 = 9 WHERE id = 1");
      await database.execute("ALTER TABLE t ADD COLUMN a BOOLEAN");
      await database.execute("ALTER TABLE t DROP COLUMN c0");
      for (let id = 3; id < 300; id += 1) {
        await database.execute("INSERT INTO t VALUES (?, ?, ?)", [id, id, id % 2 === 0]);
      }
      let quiet = 0;
      for (let attempt = 0; attempt < 400 && quiet < 10; attempt += 1) {
        await new Promise((resolve) => setTimeout(resolve, 20));
        const active = (await database.listCompactionJobs()).some(
          (job) =>
            job.state !== "published" && job.state !== "cancelled" && job.state !== "aborted",
        );
        quiet = !active && (await levelZeroCount(database, store, "t")) < 48 ? quiet + 1 : 0;
      }
      expect(errors).toEqual([]);
      expect(quiet).toBe(10);
      expect((await database.listCompactionJobs()).some((job) => job.state === "published")).toBe(
        true,
      );
      expect(
        (await database.query("SELECT COUNT(*) AS n, COUNT(a) AS a FROM t", { memoize: false }))
          .rows,
      ).toEqual([{ n: 299, a: 297 }]);
      expect(
        (await database.query("SELECT * FROM t WHERE id <= 3 ORDER BY id", { memoize: false }))
          .rows,
      ).toEqual([
        { id: 1, c: 1, a: null },
        { id: 2, c: 2, a: null },
        { id: 3, c: 3, a: false },
      ]);
    } finally {
      await database.close();
    }
  });
});

/**
 * Reductions of interaction seeds that ran once ADD and DROP COLUMN reached populated tables
 * (--constraints --schema-changes --full-text --burst-commits 300): memory seeds 116, 144, and
 * 159 failed `compactTable` with "Compaction table schema changed after planning" instead of
 * planning afresh, and OPFS seeds 500 to 517 refused to reopen ("table schema differs from its
 * rewrite plan") because recovery re-proved a fold's output against a table altered after it.
 */
describe("folds that meet ALTER TABLE mid-flight or in recovery", () => {
  const seed = async (database: MinnowDatabase): Promise<void> => {
    await database.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, c INTEGER, c0 INTEGER)");
    for (let start = 0; start < 200; start += 50) {
      await database.insertBatch(
        "t",
        Array.from({ length: 50 }, (_, offset) => ({
          id: start + offset,
          c: start + offset,
          c0: offset,
        })),
      );
    }
    await database.execute("UPDATE t SET c0 = -1 WHERE id % 7 = 0");
  };
  const read = async (database: MinnowDatabase): Promise<unknown[]> =>
    (await database.query("SELECT * FROM t ORDER BY id", { memoize: false })).rows;
  const alters = [
    ["ADD COLUMN", "ALTER TABLE t ADD COLUMN a TEXT DEFAULT 'x'"],
    ["DROP COLUMN", "ALTER TABLE t DROP COLUMN c0"],
  ] as const;

  it.each(alters)("abandons a fold planned before %s, and the next fold runs", async (_, alter) => {
    const errors: unknown[] = [];
    const store = new MemoryBlockStore();
    const database = new MinnowDatabase(store, {
      autoCompact: false,
      autoCollect: false,
      onBackgroundError: (error) => errors.push(error),
    });
    try {
      await seed(database);
      const step = await database.compactTableStep("t", {
        minimumLevel0Segments: 1,
        targetBlockBytes: 64,
        maxBlocks: 1,
      });
      expect(step.result).toBeNull();
      if (step.jobId === null) throw new Error("Expected a fold in flight");
      await database.execute(alter);
      const job = (await database.listCompactionJobs("t")).find(({ id }) => id === step.jobId);
      if (job !== undefined && job.state !== "cancelled") {
        // The job's planned layout predates the schema: it is cancelled, never published.
        await expect(database.resumeCompactionJob(step.jobId)).rejects.toMatchObject({
          name: "SchemaConflictError",
        });
        expect(
          (await database.listCompactionJobs("t")).find(({ id }) => id === step.jobId)?.state,
        ).toBe("cancelled");
      }
      const transactionId = job?.transactionId ?? null;
      if (transactionId !== null) {
        expect((await store.getTransaction(transactionId))?.status).toBe("aborted");
      }
      // Its transaction ended with it: nothing holds the table against another column drop.
      await database.execute("ALTER TABLE t DROP COLUMN c");
      const expected = await read(database);
      const folded = await database.compactTable("t", { minimumLevel0Segments: 1 });
      expect(folded.compacted).toBe(true);
      expect(await read(database)).toEqual(expected);
      expect(errors).toEqual([]);
    } finally {
      await database.close();
    }
  });

  it.each(alters)(
    "reopens OPFS after %s follows a published fold and a fold in flight",
    async (_, alter) => {
      const root = new MemoryOpfs().root;
      const options = { autoCompact: false, autoCollect: false } as const;
      let store = await OpfsBlockStore.open({ name: "alter-recovery", root });
      let database = new MinnowDatabase(store, options);
      await seed(database);
      // A published fold whose sources the store still retains, then one left in flight.
      const first = await database.compactTable("t", { minimumLevel0Segments: 1 });
      expect(first.compacted).toBe(true);
      await database.execute("UPDATE t SET c = c + 1 WHERE id < 20");
      await database.insertBatch("t", [{ id: 500, c: 500, c0: 5 }]);
      const step = await database.compactTableStep("t", {
        minimumLevel0Segments: 1,
        targetBlockBytes: 64,
        maxBlocks: 1,
      });
      expect(step.result).toBeNull();
      await database.execute(alter);
      const expected = await read(database);
      await database.close();
      store._crashForTests();

      // Recovery proves only the folds whose plans still describe the table.
      store = await OpfsBlockStore.open({ name: "alter-recovery", root });
      database = new MinnowDatabase(store, options);
      expect(await read(database)).toEqual(expected);
      expect(await store.checkIntegrity({ mode: "full" })).toMatchObject({ ok: true });
      const folded = await database.compactTable("t", { minimumLevel0Segments: 1 });
      expect(folded.compacted).toBe(true);
      expect(await read(database)).toEqual(expected);
      await database.close();
      store._crashForTests();
      store = await OpfsBlockStore.open({ name: "alter-recovery", root });
      database = new MinnowDatabase(store, options);
      expect(await read(database)).toEqual(expected);
      await database.close();
      store.close();
    },
  );

  it.each(alters)(
    "checks IndexedDB integrity after %s follows a fold in flight",
    async (_, alter) => {
      const indexedDB = new IDBFactory();
      const name = crypto.randomUUID();
      const options = { autoCompact: false, autoCollect: false } as const;
      let store = await IndexedDbBlockStore.open({ name, indexedDB });
      let database = new MinnowDatabase(store, options);
      await seed(database);
      const step = await database.compactTableStep("t", {
        minimumLevel0Segments: 1,
        targetBlockBytes: 64,
        maxBlocks: 1,
      });
      expect(step.result).toBeNull();
      await database.execute(alter);
      const expected = await read(database);
      await database.close();
      store.close();

      store = await IndexedDbBlockStore.open({ name, indexedDB });
      database = new MinnowDatabase(store, options);
      expect((await store.checkIntegrity({ mode: "full" })).issues).toEqual([]);
      const folded = await database.compactTable("t", { minimumLevel0Segments: 1 });
      expect(folded.compacted).toBe(true);
      expect(await read(database)).toEqual(expected);
      expect((await store.checkIntegrity({ mode: "full" })).issues).toEqual([]);
      await database.close();
      store.close();
    },
  );
});
