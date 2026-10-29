/**
 * Live queries against a fresh execution, over random write sequences.
 *
 * A live result is kept up to date in pieces: a commit window's changed keys are re-evaluated
 * and merged into the retained rows, aggregates are patched, zone proofs skip windows that
 * cannot matter, and a rekey moves rows and their references. Each of those is a shortcut that
 * must land exactly where running the statement again would. This property runs every kind of
 * write the engine has -- keyed and predicate writes, upserts, rekeys, transactions that commit
 * or roll back, writes from a second connection, compaction, collection, and index builds --
 * and after each one compares every subscription with a fresh query.
 *
 * Each statement has two subscribers. One records every `onChange` in a publication ledger,
 * which holds each delivery's `retained` map to the rows it claims to keep. The other rebuilds
 * its rows from `subscribePatches` alone, the way a worker-hosted consumer must. After each write
 * the ledger's latest rows, the rebuilt rows, and the fresh query must agree. The same property
 * runs in process, with incremental maintenance on and off, and across a worker boundary, where
 * the patches are the frames the worker posts. A declared reach fails the campaign when its
 * histories stop producing patched, moved, shrunk, or rekeyed deliveries, so the comparison
 * cannot quietly degrade into checking full re-runs only.
 *
 * `MINNOW_SEED=<n>` replays one seed; `MINNOW_LIVE_RUNS` widens each seed's run.
 */
import fc from "fast-check";
import { afterAll, describe, expect, it, vi } from "vitest";
import { MemoryBlockStore } from "../storage/memory.js";
import { LivePatchReplica, LivePublicationLedger } from "../testing/live-publication.js";
import type { LivePublicationState } from "../testing/live-publication.js";
import { createReach } from "../testing/reach.js";
import { seedsFor } from "../testing/seeds.js";
import { createBoundary } from "./client-audit-harness.js";
import { liveAggregateTestHooks } from "./live-aggregate.js";
import { MinnowDatabaseClient } from "./client.js";
import { MinnowDatabase } from "./database.js";
import type { LiveQueryDelivery } from "./live.js";
import type { LiveQueryPatch } from "./live-patch.js";
import { heavyTestTimeout } from "./storage-test-helpers.js";
import { exposeDatabase } from "./worker-server.js";
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
  // One group per key: a commit touches a few of many groups, so their output rows move within
  // the retained order instead of every group being evaluated and sorted again.
  "SELECT id, COUNT(*) AS n, SUM(x) AS s FROM a GROUP BY id ORDER BY s DESC NULLS LAST, id LIMIT 4",
  "SELECT x, COUNT(*) AS n FROM a GROUP BY x HAVING COUNT(*) >= 1 ORDER BY n DESC, x NULLS FIRST LIMIT 3 OFFSET 1",
  "SELECT g, id % 4 AS r, COUNT(*) AS n, SUM(x) AS s FROM a GROUP BY g, id % 4 ORDER BY g, r",
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
/** What a write needs from the connection it runs on, in process or across the worker boundary. */
interface Writer {
  execute(sql: string): Promise<unknown>;
  compactTable(table: string): Promise<unknown>;
}

/** The live query set either connection kind hands out. */
interface LiveSet {
  subscribe(
    sql: string,
    options: { onChange(result: QueryResult, delivery: LiveQueryDelivery): void },
  ): Promise<unknown>;
  subscribePatches(
    sql: string,
    options: { onPatch(patch: LiveQueryPatch): void },
  ): Promise<unknown>;
  refresh(): Promise<unknown>;
  close(): unknown;
}

async function apply(
  database: Writer,
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

type Mode = "incremental" | "re-run" | "worker";

/**
 * The states each mode's laws depend on. Re-running never patches, so it declares only that a
 * later delivery ran again; the worker path always maintains incrementally and posts patches.
 */
const DECLARED: Record<Mode, readonly LivePublicationState[]> = {
  incremental: ["patched", "moved", "replaced", "shrunk", "rerun", "keyChanges"],
  "re-run": ["rerun", "keyChanges"],
  worker: ["patched", "moved", "replaced", "shrunk", "rerun", "keyChanges"],
};

/** Maintained aggregates either move their touched groups' rows or rebuild; both must be judged. */
const AGGREGATE_REACH = ["aggregate-moved", "aggregate-rebuilt"] as const;

interface Opened {
  writer: Writer;
  live: LiveSet;
  close(): Promise<void>;
}

/** The connection the subscriptions live on: the database itself, or a client across a boundary. */
async function open(database: MinnowDatabase, mode: Mode): Promise<Opened> {
  if (mode !== "worker") {
    const live = database.liveQueries({ incremental: mode === "incremental" });
    return { writer: database, live, close: () => Promise.resolve(live.close()) };
  }
  const boundary = createBoundary();
  exposeDatabase(database, boundary.workerSide);
  const client = new MinnowDatabaseClient(boundary.clientSide);
  await client.ready();
  const live = client.liveQueries();
  return {
    writer: client,
    live,
    close: async () => {
      await live.close();
      await client.close();
    },
  };
}

async function check(
  writes: readonly Write[],
  mode: Mode,
  reach: ReturnType<typeof createReach<LivePublicationState>>,
  aggregates: ReturnType<typeof createReach<(typeof AGGREGATE_REACH)[number]>>,
): Promise<void> {
  reach.beginHistory();
  aggregates.beginHistory();
  const before = { ...liveAggregateTestHooks };
  const store = new MemoryBlockStore();
  const options = { autoCompact: false, autoCollect: false, rowsPerBlock: 4 } as const;
  const database = new MinnowDatabase(store, options);
  const other = new MinnowDatabase(store, options);
  let opened: Opened | undefined;
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
    opened = await open(database, mode);
    const subscribers = STATEMENTS.map((sql) => ({
      sql,
      ledger: new LivePublicationLedger(`${mode} ${sql}`, (state) => {
        if (DECLARED[mode].includes(state)) reach.reach(state);
      }),
      replica: new LivePatchReplica(`${mode} ${sql}`),
    }));
    for (const { sql, ledger, replica } of subscribers) {
      await opened.live.subscribe(sql, { onChange: ledger.change });
      await opened.live.subscribePatches(sql, { onPatch: replica.apply });
    }
    const indexed = { done: false };
    for (const [step, entry] of writes.entries()) {
      await apply(opened.writer, other, entry, indexed);
      await opened.live.refresh();
      for (const { sql, ledger, replica } of subscribers) {
        const at = `${mode}: ${sql} after step ${String(step)}: ${JSON.stringify(entry)}`;
        const fresh = await database.query(sql, { memoize: false });
        expect(ledger.violations, at).toEqual([]);
        expect(replica.violations, at).toEqual([]);
        expect(ledger.latest()?.rows, at).toEqual(fresh.rows);
        expect(replica.rows(), `${at} (rebuilt from patches)`).toEqual(fresh.rows);
      }
    }
    if (liveAggregateTestHooks.moves > before.moves) aggregates.reach("aggregate-moved");
    if (liveAggregateTestHooks.rebuilds > before.rebuilds) aggregates.reach("aggregate-rebuilt");
  } finally {
    await opened?.close();
    await database.close();
    await other.close();
    store.close();
  }
}

const RUNS = Number(process.env.MINNOW_LIVE_RUNS ?? 12);

describe("live results against fresh execution", () => {
  describe.each(["incremental", "re-run", "worker"] as const)("%s", (mode) => {
    const reach = createReach(`live-differential ${mode}`, DECLARED[mode]);
    // Re-running never maintains an aggregate, so it owes neither state.
    const aggregates = createReach(
      `live-differential ${mode} aggregates`,
      mode === "re-run" ? [] : AGGREGATE_REACH,
    );
    afterAll(() => {
      reach.assertReached();
      aggregates.assertReached();
    });
    it.each(seedsFor("live-differential", [0x11fe]))(
      "every delivery, patch, and final result agree (seed %i)",
      async (seed) => {
        await fc.assert(
          fc.asyncProperty(fc.array(write, { minLength: 10, maxLength: 30 }), (writes) =>
            check(writes, mode, reach, aggregates),
          ),
          { seed, numRuns: RUNS },
        );
      },
    );
  });
});
