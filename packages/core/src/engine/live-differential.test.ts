/**
 * Live queries against a fresh execution, over random write sequences.
 *
 * A live result is kept up to date in pieces: a commit window's changed keys are re-evaluated
 * and merged into the retained rows, aggregates are patched, zone proofs skip windows that
 * cannot matter, and a rekey moves rows and their references. Each of those is a shortcut that
 * must land exactly where running the statement again would. This property runs every kind of
 * write the engine has -- keyed and predicate writes, upserts, rekeys, transactions that commit
 * or roll back, writes from a second connection, compaction, collection, and index builds --
 * and after each one compares every subscription's latest delivery with a fresh query.
 */
import fc from "fast-check";
import { describe, expect, it, vi } from "vitest";
import { MemoryBlockStore } from "../storage/memory.js";
import { MinnowDatabase } from "./database.js";
import { heavyTestTimeout } from "./storage-test-helpers.js";
import type { QueryResult } from "./query.js";

vi.setConfig({ testTimeout: heavyTestTimeout(120_000) });

const STATEMENTS = [
  "SELECT id, g, x FROM a ORDER BY id",
  "SELECT id, x FROM a WHERE x > 10 ORDER BY x, id",
  "SELECT id, x FROM a WHERE x BETWEEN -5 AND 15 ORDER BY x DESC, id LIMIT 3",
  "SELECT g, COUNT(*) AS n, SUM(x) AS s, MIN(x) AS lo, MAX(x) AS hi FROM a GROUP BY g ORDER BY g",
  "SELECT COUNT(*) AS n FROM a WHERE g = 'k'",
  "SELECT id, g FROM a WHERE g IN ('k', 'm') ORDER BY id LIMIT 4",
  "SELECT a.id, a.x, b.id AS b_id, b.y FROM a JOIN b ON b.a_id = a.id ORDER BY a.id, b.id",
  "SELECT b.id, b.a_id FROM b ORDER BY b.id",
  "SELECT id FROM a WHERE id IN (SELECT a_id FROM b WHERE y > 0) ORDER BY id",
  "SELECT a.id, p.id AS parent FROM a LEFT JOIN a p ON p.id = a.p ORDER BY a.id",
  "SELECT id, x FROM a WHERE id = 3",
  "SELECT id, x FROM a WHERE id = -3 OR id = 4 ORDER BY id",
  "SELECT DISTINCT g FROM a ORDER BY g",
  "SELECT id, x FROM a WHERE x IS NULL ORDER BY id",
];

type Write =
  | { kind: "insert"; table: "a" | "b"; id: number; x: number | null; g: string; ref: number }
  | { kind: "upsert"; id: number; x: number | null; g: string }
  | { kind: "updateKey"; id: number; x: number | null }
  | { kind: "updateWhere"; low: number; by: number }
  | { kind: "deleteKey"; table: "a" | "b"; id: number }
  | { kind: "deleteWhere"; low: number }
  | { kind: "rekey"; from: number; to: number }
  | { kind: "shift"; low: number; by: number }
  | { kind: "transaction"; writes: Write[]; commit: boolean }
  | { kind: "other"; write: Write }
  | { kind: "compact"; table: "a" | "b" }
  | { kind: "collect" }
  | { kind: "index" };

const key = fc.integer({ min: -8, max: 12 });
const value = fc.option(fc.integer({ min: -20, max: 30 }), { nil: null, freq: 6 });
const group = fc.constantFrom("k", "m", "q");

const simpleWrite: fc.Arbitrary<Write> = fc.oneof(
  {
    weight: 4,
    arbitrary: fc.record({
      kind: fc.constant("insert" as const),
      table: fc.constantFrom("a" as const, "b" as const),
      id: key,
      x: value,
      g: group,
      ref: key,
    }),
  },
  {
    weight: 3,
    arbitrary: fc.record({ kind: fc.constant("upsert" as const), id: key, x: value, g: group }),
  },
  {
    weight: 3,
    arbitrary: fc.record({ kind: fc.constant("updateKey" as const), id: key, x: value }),
  },
  {
    weight: 2,
    arbitrary: fc.record({
      kind: fc.constant("updateWhere" as const),
      low: fc.integer({ min: -20, max: 30 }),
      by: fc.integer({ min: -9, max: 9 }),
    }),
  },
  {
    weight: 2,
    arbitrary: fc.record({
      kind: fc.constant("deleteKey" as const),
      table: fc.constantFrom("a" as const, "b" as const),
      id: key,
    }),
  },
  {
    weight: 1,
    arbitrary: fc.record({
      kind: fc.constant("deleteWhere" as const),
      low: fc.integer({ min: -20, max: 30 }),
    }),
  },
  { weight: 3, arbitrary: fc.record({ kind: fc.constant("rekey" as const), from: key, to: key }) },
  {
    weight: 1,
    arbitrary: fc.record({
      kind: fc.constant("shift" as const),
      low: key,
      by: fc.constantFrom(-2, -1, 1, 2),
    }),
  },
);

const write: fc.Arbitrary<Write> = fc.oneof(
  { weight: 12, arbitrary: simpleWrite },
  {
    weight: 2,
    arbitrary: fc.record({
      kind: fc.constant("transaction" as const),
      writes: fc.array(simpleWrite, { minLength: 1, maxLength: 3 }),
      commit: fc.boolean(),
    }),
  },
  { weight: 3, arbitrary: fc.record({ kind: fc.constant("other" as const), write: simpleWrite }) },
  {
    weight: 1,
    arbitrary: fc.record({
      kind: fc.constant("compact" as const),
      table: fc.constantFrom("a" as const, "b" as const),
    }),
  },
  { weight: 1, arbitrary: fc.constant({ kind: "collect" as const }) },
  { weight: 1, arbitrary: fc.constant({ kind: "index" as const }) },
);

function literal(value: number | null): string {
  return value === null ? "NULL" : String(value);
}

function sqlOf(entry: Write): string[] {
  switch (entry.kind) {
    case "insert":
      return [
        entry.table === "a"
          ? `INSERT INTO a VALUES (${String(entry.id)}, '${entry.g}', ${literal(entry.x)}, ${String(entry.ref)})`
          : `INSERT INTO b VALUES (${String(entry.id)}, ${String(entry.ref)}, ${literal(entry.x)})`,
      ];
    case "upsert":
      return [
        `INSERT INTO a VALUES (${String(entry.id)}, '${entry.g}', ${literal(entry.x)}, NULL) ON CONFLICT (id) DO UPDATE SET g = excluded.g, x = excluded.x`,
      ];
    case "updateKey":
      return [`UPDATE a SET x = ${literal(entry.x)} WHERE id = ${String(entry.id)}`];
    case "updateWhere":
      return [`UPDATE a SET x = x + ${String(entry.by)} WHERE x >= ${String(entry.low)}`];
    case "deleteKey":
      return [`DELETE FROM ${entry.table} WHERE id = ${String(entry.id)}`];
    case "deleteWhere":
      return [`DELETE FROM a WHERE x < ${String(entry.low)}`];
    case "rekey":
      return [`UPDATE a SET id = ${String(entry.to)} WHERE id = ${String(entry.from)}`];
    case "shift":
      return [`UPDATE a SET id = id + ${String(entry.by)} WHERE id >= ${String(entry.low)}`];
    default:
      return [];
  }
}

/** Runs one write; refusals the engine documents (duplicates, retired keys) are expected. */
async function apply(
  database: MinnowDatabase,
  other: MinnowDatabase,
  entry: Write,
  indexed: { done: boolean },
): Promise<void> {
  const tolerate = async (run: () => Promise<unknown>): Promise<void> => {
    try {
      await run();
    } catch (error) {
      if (!(error instanceof Error) || error.name !== "UniqueConstraintError") throw error;
    }
  };
  switch (entry.kind) {
    case "transaction": {
      await database.execute("BEGIN");
      let failed = false;
      for (const inner of entry.writes) {
        for (const sql of sqlOf(inner)) {
          try {
            await database.execute(sql);
          } catch (error) {
            if (!(error instanceof Error) || error.name !== "UniqueConstraintError") throw error;
            failed = true;
          }
          if (failed) break;
        }
        if (failed) break;
      }
      await database.execute(entry.commit && !failed ? "COMMIT" : "ROLLBACK");
      return;
    }
    case "other":
      for (const sql of sqlOf(entry.write)) await tolerate(() => other.execute(sql));
      return;
    case "compact":
      await database.compactTable(entry.table);
      return;
    case "collect":
      await other.collectGarbage();
      return;
    case "index":
      if (!indexed.done) {
        indexed.done = true;
        await database.execute("CREATE INDEX a_x ON a (x)");
      }
      return;
    default:
      for (const sql of sqlOf(entry)) await tolerate(() => database.execute(sql));
  }
}

async function check(writes: readonly Write[], incremental: boolean): Promise<void> {
  const store = new MemoryBlockStore();
  const options = { autoCompact: false, autoCollect: false, rowsPerBlock: 4 } as const;
  const database = new MinnowDatabase(store, options);
  const other = new MinnowDatabase(store, options);
  const live = database.liveQueries({ incremental });
  try {
    await database.execute(
      "CREATE TABLE a (id INTEGER PRIMARY KEY, g TEXT, x INTEGER, p INTEGER REFERENCES a (id) NOT ENFORCED)",
    );
    await database.execute(
      "CREATE TABLE b (id INTEGER PRIMARY KEY, a_id INTEGER REFERENCES a (id) NOT ENFORCED, y INTEGER)",
    );
    await database.execute(
      "INSERT INTO a VALUES (1, 'k', 5, NULL), (2, 'm', 12, 1), (3, 'k', -3, 2), (4, 'q', 20, 3)",
    );
    await database.execute("INSERT INTO b VALUES (1, 1, 1), (2, 3, -1), (3, 3, 4)");
    const latest = new Map<string, QueryResult>();
    for (const sql of STATEMENTS) {
      await live.subscribe(sql, { onChange: (result) => latest.set(sql, result) });
    }
    const indexed = { done: false };
    for (const [step, entry] of writes.entries()) {
      await apply(database, other, entry, indexed);
      await live.refresh();
      for (const sql of STATEMENTS) {
        const fresh = await database.query(sql, { memoize: false });
        expect(
          latest.get(sql)?.rows,
          `${sql} after step ${String(step)}: ${JSON.stringify(entry)}`,
        ).toEqual(fresh.rows);
      }
    }
  } finally {
    live.close();
    await database.close();
    await other.close();
    store.close();
  }
}

describe("live results against fresh execution", () => {
  it.each([true, false])("agree after every write (incremental=%s)", async (incremental) => {
    await fc.assert(
      fc.asyncProperty(fc.array(write, { minLength: 10, maxLength: 30 }), (writes) =>
        check(writes, incremental),
      ),
      {
        seed: Number(process.env.MINNOW_SEED ?? 0x11fe),
        numRuns: Number(process.env.MINNOW_LIVE_RUNS ?? 12),
      },
    );
  });
});
