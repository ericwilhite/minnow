/**
 * The SQL performance gate: a seeded dataset and query suite timed on the full MinnowDatabase
 * pipeline against native SQLite (node:sqlite) and PGlite (Wasm Postgres). Native SQLite is a
 * deliberately harsh baseline: the browser competitor is its Wasm build, which runs slower
 * than what is measured here.
 *
 * Each query's minnow/engine time ratio must stay at or below the checked-in threshold in
 * packages/core/perf-baseline.json. Thresholds pin the current ratios with headroom, separately
 * for each OS/architecture/Node-major profile, so the gate catches regressions rather than host
 * differences. Run with --update to add or rewrite the current runtime's thresholds after an
 * intentional change.
 */
import { readFileSync, writeFileSync, existsSync } from "node:fs";
import { DatabaseSync } from "node:sqlite";
import { isDeepStrictEqual } from "node:util";
import { MemoryBlockStore } from "../packages/core/src/storage/index.js";
import {
  MinnowDatabase,
  type VisibleSegment,
  type VisibleSegmentPageCursor,
} from "../packages/core/src/engine/database.js";
import { MinnowDialect } from "../packages/kysely/src/dialect.js";
import { Kysely } from "kysely";
import {
  PERFORMANCE_ENGINES,
  parsePerformanceBaseline,
  runtimePerformanceProfile,
  selectPerformanceThresholds,
  updatedPerformanceBaseline,
  type PerformanceEngine,
  type PerformanceThresholds,
} from "./lib/performance-baseline.mts";

async function allVisibleSegments(
  database: MinnowDatabase,
  tableName: string,
): Promise<VisibleSegment[]> {
  const records: VisibleSegment[] = [];
  let cursor: VisibleSegmentPageCursor | null = null;
  do {
    const page = await database.listVisibleSegmentPage(
      tableName,
      cursor === null ? {} : { cursor },
    );
    records.push(...page.records);
    cursor = page.nextCursor;
  } while (cursor !== null);
  return records;
}

const BASELINE_PATH = new URL("../packages/core/perf-baseline.json", import.meta.url);
const ROWS = 200_000;
const update = process.argv.includes("--update");
const profile = runtimePerformanceProfile();
const baselineFile = existsSync(BASELINE_PATH)
  ? parsePerformanceBaseline(JSON.parse(readFileSync(BASELINE_PATH, "utf8")), ROWS)
  : undefined;
// Reject an unknown runtime before spending minutes building and loading the benchmark corpus.
selectPerformanceThresholds(baselineFile, profile, [], update);
// Give V8 enough executions to tier allocation-heavy grouped/DISTINCT kernels before sampling.
// Two left that shape bimodal between fresh processes (about 17ms or 27ms) even though its
// steady-state cost was unchanged, turning the release gate into a coin flip.
const WARMUP = 5;
const RUNS = 9;
/**
 * Sub-millisecond queries are timed in batches rather than floored. Flooring both sides of the
 * ratio — the old approach — made every fast query gate as 0.5ms against 0.5ms, so a point
 * lookup could degrade sevenfold and still pass: exactly how a per-query catalog scan once
 * reached main unnoticed. Batching removes the noise instead of hiding it. Each sample runs the
 * query enough times to take at least this long, and the per-iteration cost is the total over
 * the count, which makes a 0.07ms query as measurable as a 7ms one and keeps the gate's
 * cross-engine ratio — the part that survives a change of machine — meaningful at every scale.
 * Read-only engines calibrate independently because normalized per-iteration timings remain
 * comparable and making a 7ms engine repeat as often as a 0.07ms engine only adds idle minutes.
 */
const TARGET_SAMPLE_MS = 25;
// Memo hits can complete in a few microseconds; 10k repeats is still a short sample but lets the
// fastest path reach TARGET_SAMPLE_MS instead of deriving a release decision from timer jitter.
const MAX_ITERATIONS = 10_000;
/** A tiny floor remains, against a divide-by-zero on an engine that answers instantly. */
const ratioOf = (minnowMs: number, engineMs: number): number =>
  Math.max(minnowMs, 0.0001) / Math.max(engineMs, 0.0001);

/**
 * How many times to repeat one query per timed sample. Measured once from a single run, so the
 * count reflects this machine. The optional target and cap let stateful mutations share a
 * bounded iteration count while read-only shapes give each engine the full-resolution sample.
 */
function iterationsFor(
  singleRunMs: number,
  targetSampleMs = TARGET_SAMPLE_MS,
  maximumIterations = MAX_ITERATIONS,
): number {
  if (singleRunMs >= targetSampleMs) return 1;
  return Math.min(
    maximumIterations,
    Math.max(1, Math.ceil(targetSampleMs / Math.max(singleRunMs, 0.001))),
  );
}

function mulberry32(seed: number): () => number {
  let state = seed;
  return () => {
    state |= 0;
    state = (state + 0x6d2b79f5) | 0;
    let t = Math.imul(state ^ (state >>> 15), 1 | state);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

const REGIONS = ["west", "east", "north", "south", "central", null] as const;
const LABELS = ["alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf", "hotel"];

interface Row {
  id: number;
  region: string | null;
  amount: number;
  active: boolean;
  joined: string | null;
  label: string;
}

function buildRows(): Row[] {
  const rng = mulberry32(0x9e17f);
  const rows: Row[] = [];
  for (let id = 1; id <= ROWS; id += 1) {
    rows.push({
      id,
      region: REGIONS[Math.floor(rng() * REGIONS.length)] ?? null,
      amount: Math.floor(rng() * 40_000) / 4,
      active: rng() < 0.5,
      joined:
        rng() < 0.1
          ? null
          : new Date(Date.UTC(2025, 0, 1) + Math.floor(rng() * 500 * 86_400_000)).toISOString(),
      label: `${LABELS[Math.floor(rng() * LABELS.length)] ?? "alpha"}-${String(Math.floor(rng() * 10_000))}`,
    });
  }
  return rows;
}

const DIMS = [
  { region: "west", label: "West Coast", rank: 1 },
  { region: "east", label: "East Coast", rank: 2 },
  { region: "north", label: "North", rank: 3 },
  { region: "south", label: "South", rank: 4 },
];

interface PerfQuery {
  readonly name: string;
  readonly sql: string;
  readonly params?: ReadonlyArray<number | string>;
  /** SQL only guarantees row order when the statement has an outer ORDER BY. */
  readonly ordered?: boolean;
  /** Serve Minnow's probe-validated result memo instead of re-executing (gates memo-hit latency). */
  readonly memoize?: boolean;
  /** Include Kysely's compilation and driver bridge instead of calling Minnow directly. */
  readonly kysely?: boolean;
}

const QUERIES: readonly PerfQuery[] = [
  { name: "count-star", sql: "SELECT COUNT(*) AS n FROM data" },
  {
    name: "filter-scan",
    sql: "SELECT id, amount FROM data WHERE amount > ? AND region = ?",
    params: [9000, "west"],
  },
  {
    name: "group-aggregate",
    sql: "SELECT region, COUNT(*) AS c, SUM(amount) AS s, AVG(amount) AS a FROM data GROUP BY region",
  },
  {
    name: "top-n",
    sql: "SELECT id, amount FROM data ORDER BY amount DESC, id LIMIT 100",
    ordered: true,
  },
  {
    name: "join-aggregate",
    sql: "SELECT m.label AS label, COUNT(*) AS c, SUM(d.amount) AS s FROM data d JOIN dims m ON m.region = d.region WHERE d.active = TRUE GROUP BY m.label",
  },
  {
    name: "like-scan",
    sql: "SELECT COUNT(*) AS n FROM data WHERE label LIKE 'delta-1%'",
  },
  {
    name: "window-running",
    sql: "SELECT id, SUM(amount) OVER (PARTITION BY region ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running FROM data WHERE id <= 20000",
  },
  {
    name: "distinct-aggregate",
    sql: "SELECT region, COUNT(DISTINCT label) AS labels FROM data GROUP BY region",
  },
  {
    name: "point-lookup",
    sql: "SELECT id, amount, label FROM data WHERE id = ?",
    params: [123_456],
  },
  {
    // The adapter is a shipped execution path, not just a type layer. This measures Kysely's
    // builder/compiler and Minnow driver bridge together against the equivalent direct SQL.
    name: "kysely-point-lookup",
    sql: "SELECT id, amount, label FROM data WHERE id = ?",
    params: [123_456],
    kysely: true,
  },
  {
    // Exact NUMERIC uses tagged string-domain arithmetic instead of Float64 kernels.
    name: "exact-numeric-filter",
    sql: "SELECT COUNT(*) AS n FROM exact_data WHERE amount + CAST(? AS NUMERIC) BETWEEN ? AND ?",
    params: [0.5, 2500.75, 5000.75],
  },
  {
    // A declared composite primary key exercises the public components while its hidden tuple
    // locator remains an internal row-addressing detail.
    name: "composite-key-lookup",
    sql: "SELECT payload FROM composite_data WHERE tenant = ? AND id = ?",
    params: ["tenant-7", 12_347],
  },
  {
    // Ids arrive in insertion order, so blocks are id-clustered and zone maps prune almost
    // every block; a pruning regression turns this into a full scan and trips the gate.
    name: "range-scan",
    sql: "SELECT COUNT(*) AS n, SUM(amount) AS s FROM data WHERE id BETWEEN ? AND ?",
    params: [150_000, 154_000],
  },
  {
    // A full sort with no limit, on a numeric key. Distinct from top-n, which the bounded
    // top-K sink answers without sorting: this one orders every row, which is what a window
    // function and an unpaged report both pay for.
    name: "sort-numeric",
    sql: "SELECT id, amount FROM data ORDER BY amount",
    ordered: true,
  },
  {
    // A page deep enough that a bounded top-K sink would retain most of what it scans. The
    // bound buys a memory guarantee — retention proportional to the limit rather than the table
    // — but past a tenth of the rows it costs more time than sorting everything once, so the
    // sink drops it and this shape pays a full scan and one sort. Gated so that trade stays
    // where it was measured instead of drifting.
    name: "deep-page",
    // Keep the benchmark's historical ordering explicit. PostgreSQL defaults nullable ASC keys
    // to NULLS LAST while SQLite defaults them to NULLS FIRST; the performance gate compares
    // ordered rows before it times anything, so an implicit dialect default is not a fair oracle.
    sql: "SELECT id, amount FROM data ORDER BY region NULLS FIRST, id LIMIT 100000",
    ordered: true,
  },
  {
    // The same point lookup, run while the catalog holds many more tables than the query
    // touches. Per-query work that scales with the schema rather than the data shows up here
    // and nowhere else: a catalog scan per query once made this five times slower than the
    // plain point lookup while every other case stayed flat.
    name: "catalog-point-lookup",
    sql: "SELECT id, amount, label FROM data WHERE id = ?",
    params: [123_456],
  },
  {
    // The same point lookup against a table one row has been deleted from. A mutation history
    // cannot be read as a plain append, and the replay that reads it used to cost the whole
    // table on every query — 236x this, for one deleted row out of 200,000.
    name: "delta-point-lookup",
    sql: "SELECT id, amount, label FROM data_mut WHERE id = ?",
    params: [123_456],
  },
  {
    // Cardinality over the same history: the delete's effect on the row count is known without
    // materializing a single column.
    name: "delta-count-star",
    sql: "SELECT COUNT(*) AS n FROM data_mut",
  },
  {
    // A full scan of that history, where every surviving row is copied past the deleted one.
    name: "delta-filter-scan",
    sql: "SELECT id, amount FROM data_mut WHERE amount > ? AND region = ?",
    params: [9000, "west"],
  },
  {
    // Same statement repeated unchanged: Minnow answers from the probe-validated memo while
    // the other engines re-execute. Gates the memo probe's own latency.
    name: "memo-hit-aggregate",
    sql: "SELECT region, COUNT(*) AS c, SUM(amount) AS s, AVG(amount) AS a FROM data GROUP BY region",
    memoize: true,
  },
];

function median(samples: number[]): number {
  const sorted = [...samples].sort((left, right) => left - right);
  return sorted[Math.floor(sorted.length / 2)] ?? 0;
}

/**
 * Makes the native SQLite result and Minnow's public row objects comparable without weakening
 * SQL semantics. Unordered statements compare as multisets; an outer ORDER BY compares in exact
 * row order. The gate refuses to time a read until this check passes, so an optimization cannot
 * improve its number by returning fewer, different, or misordered rows.
 */
function comparableRows(rows: ReadonlyArray<Record<string, unknown>>, ordered: boolean): string[] {
  const comparable = rows.map((row) =>
    JSON.stringify(
      Object.keys(row)
        .sort()
        .map((name) => [name, comparableValue(row[name])]),
    ),
  );
  return ordered ? comparable : comparable.sort();
}

function comparableValue(value: unknown): unknown {
  if (value instanceof Date) return value.toISOString();
  if (typeof value === "bigint") return Number(value);
  // JSON.stringify turns NaN into null and negative zero into zero; be explicit about both so a
  // future workload that reaches them fails rather than accidentally equating distinct values.
  if (typeof value === "number" && !Number.isFinite(value)) return String(value);
  if (Object.is(value, -0)) return "-0";
  return value;
}

function assertQueryEquivalent(query: PerfQuery, minnowResult: unknown, sqliteRows: unknown): void {
  const actual = (minnowResult as { rows?: Array<Record<string, unknown>> }).rows;
  if (!Array.isArray(actual) || !Array.isArray(sqliteRows)) {
    throw new Error(`${query.name}: performance correctness check did not receive row results`);
  }
  const actualRows = comparableRows(actual, query.ordered === true);
  const expectedRows = comparableRows(
    sqliteRows as Array<Record<string, unknown>>,
    query.ordered === true,
  );
  if (!isDeepStrictEqual(actualRows, expectedRows)) {
    throw new Error(
      `${query.name}: Minnow differs from SQLite before timing\n` +
        `expected ${JSON.stringify(expectedRows.slice(0, 5))}\n` +
        `received ${JSON.stringify(actualRows.slice(0, 5))}`,
    );
  }
}

/**
 * A shape's timing: the middle of the samples, and the ends.
 *
 * The threshold is sized from the spread rather than from a fixed multiplier, because how noisy a
 * shape is varies enormously between shapes. A 90ms sort is steady to a percent or two; a memo
 * hit answered in 0.01ms is at the edge of what the clock resolves and swings much further. One
 * global margin either has to be wide enough for the worst of those -- which is the blind spot
 * this gate is trying to close -- or it fails on noise, which is how a gate gets ignored.
 */
interface Timing {
  /** What the table reports: the typical cost. */
  readonly median: number;
  /** The slowest and fastest samples, which is what the threshold is built from. */
  readonly worst: number;
  readonly best: number;
  /** False for a shape that can only be measured once, and so has no spread to read. */
  readonly repeated: boolean;
}

/** Median per-iteration time over RUNS samples, each sample running the query `iterations` times. */
async function timeRepeated(
  run: () => Promise<unknown> | unknown,
  iterations: number,
): Promise<Timing> {
  const samples: number[] = [];
  for (let index = 0; index < RUNS; index += 1) {
    const started = performance.now();
    for (let repeat = 0; repeat < iterations; repeat += 1) await run();
    samples.push((performance.now() - started) / iterations);
  }
  return {
    median: median(samples),
    worst: Math.max(...samples),
    best: Math.min(...samples),
    repeated: true,
  };
}

/** Wraps a measurement that can only be taken once, so it flows through the same reporting. */
function onceOnly(ms: number): Timing {
  return { median: ms, worst: ms, best: ms, repeated: false };
}

/**
 * The threshold to record for a shape, from what its samples actually did.
 *
 * A repeated shape is pinned just past its own worst case: Minnow's slowest sample against the
 * comparison engine's fastest, which is the least favourable reading the run produced, plus a
 * little. A shape measured once has no spread to read, so it falls back to a fixed multiplier --
 * a real blind spot, kept explicit and kept to the two shapes that cannot be repeated.
 */
/**
 * How much headroom a recorded threshold gets, in the units of the ratio it pins.
 *
 * The old gate gave every shape 1.5x, which meant a 49% regression could re-baseline unnoticed on
 * a 90ms sort that never varies by more than a percent. Sizing purely from the observed spread
 * fixes that but overcorrects the other way: a memo hit measured at 0.01ms swings far enough
 * between samples that its own spread argues for *more* than 1.5x, which would make the gate
 * weaker than it was.
 *
 * So the spread chooses the margin and the bounds keep it honest. A steady shape is pinned at
 * FLOOR, well inside the old blanket figure. A noisy one gets more room, but never more than
 * CEILING -- which is the old figure, so no shape ends up gated more loosely than before.
 *
 * FLOOR is 1.25 rather than 1.15 because nine samples in one run understate what the fastest
 * shapes do between runs: a point lookup measured at 0.07ms against a 10ms engine puts all the
 * ratio's jitter on the small side of the division, and 1.15 tripped it about one run in three.
 */
const MARGIN_FLOOR = 1.25;
const MARGIN_CEILING = 1.5;
/** A measurement taken once has no spread to read, so it gets the ceiling and an explicit name. */
const ONCE_ONLY_MARGIN = MARGIN_CEILING;

function thresholdFor(minnow: Timing, engine: Timing): number {
  const ratio = ratioOf(minnow.median, engine.median);
  if (!minnow.repeated || !engine.repeated) return ratio * ONCE_ONLY_MARGIN;
  // Both sides contribute uncertainty to the ratio, so their spreads compound. The extra 5%
  // covers what nine samples in one run cannot see: the drift between one run and the next.
  const spread = (minnow.worst / minnow.best) * (engine.worst / engine.best) * 1.05;
  return ratio * Math.min(MARGIN_CEILING, Math.max(MARGIN_FLOOR, spread));
}

/**
 * Write shapes.
 *
 * The gate measured exactly one write -- bulk ingest -- which left the whole small-write path
 * ungated: a regression in point updates or keyed deletes could not move any number here. These
 * run against `data_mut`, whose rows all three engines already hold, and cycle the key so each
 * iteration does equivalent work on a different row rather than re-touching one hot row.
 */
interface PerfMutation {
  readonly name: string;
  readonly sql: string;
  /** Bound parameters for iteration `n`, so repeated samples stay equivalent but not identical. */
  readonly params: (n: number) => ReadonlyArray<number | string>;
}

/** Rows the cycling keys stay inside, well under the ingested range. */
const MUTATION_KEYS = 100_000;
/**
 * Stateful samples must write exactly the same amount of history in every process. Deriving this
 * count from one noisy warm-up made both the measured history and the final oracle vary between
 * runs. Twelve repeats put Minnow's fastest write sample safely above timer resolution while
 * keeping the slower comparison engines bounded.
 */
const MUTATION_ITERATIONS = 12;

const MUTATIONS: readonly PerfMutation[] = [
  {
    name: "point-update",
    sql: "UPDATE data_mut SET amount = amount + 1 WHERE id = ?",
    params: (n) => [(n % MUTATION_KEYS) + 1],
  },
  {
    name: "range-update",
    sql: "UPDATE data_mut SET amount = amount + 1 WHERE id BETWEEN ? AND ?",
    params: (n) => {
      const start = ((n * 100) % MUTATION_KEYS) + 1;
      return [start, start + 99];
    },
  },
  {
    name: "filtered-update",
    sql: "UPDATE data_mut SET label = ? WHERE region = ? AND id BETWEEN ? AND ?",
    params: (n) => {
      const start = ((n * 1_000) % MUTATION_KEYS) + 1;
      return [n % 2 === 0 ? "alpha" : "bravo", "west", start, start + 999];
    },
  },
];

const MUTATION_CHECK: PerfQuery = {
  name: "mutation-state",
  sql: "SELECT COUNT(*) AS n, SUM(amount) AS amount, SUM(CASE WHEN label = 'alpha' THEN 1 ELSE 0 END) AS alpha FROM data_mut",
};

interface BenchmarkDatabase {
  data: {
    id: number;
    region: string | null;
    amount: number;
    active: boolean;
    joined: Date | null;
    label: string;
  };
}

function minnowRunner(database: MinnowDatabase, query: PerfQuery): () => Promise<unknown> {
  if (query.kysely === true) {
    const id = Number(query.params?.[0] ?? 0);
    let repeat = 0;
    return async () => ({
      rows: await kysely
        .selectFrom("data")
        .select(["id", "amount", "label"])
        // Rotate through real rows so Minnow's result memo cannot turn this adapter measurement
        // into a memo-hit measurement after the first builder execution.
        .where("id", "=", id + (repeat++ % 1_000))
        .execute(),
    });
  }
  // By default the gate measures execution, not the probe-validated result memo (which would
  // answer every repeat sample from cache); memo-hit shapes opt in to measure the memo path.
  const options = {
    memoize: query.memoize ?? false,
    ...(query.params === undefined ? {} : { params: query.params as never }),
  };
  return () => database.query(query.sql, options);
}

async function warmUp(run: () => Promise<unknown> | unknown): Promise<number> {
  for (let index = 0; index < WARMUP; index += 1) await run();
  const started = performance.now();
  await run();
  return performance.now() - started;
}

/**
 * How many tables beyond the two the queries use. Enough that a per-query catalog scan is
 * unmistakable, small enough that creating them costs a fraction of the ingest.
 */
const CATALOG_TABLES = 100;

const rows = buildRows();

// Background compaction is off: the gate times the query and write paths, and a fold of the
// 200k-row table landing under a measurement would make a once-only shape such as bulk-delete
// swing threefold between runs for reasons that have nothing to do with the path under test.
// The mutation history the delta-* and write shapes read therefore stays as written.
const minnow = new MinnowDatabase(new MemoryBlockStore(), { autoCompact: false });
await minnow.createTable({
  name: "data",
  uniqueKey: "id",
  columns: [
    { name: "id", type: "number" },
    { name: "region", type: "string", nullable: true },
    { name: "amount", type: "number" },
    { name: "active", type: "boolean" },
    { name: "joined", type: "datetime", nullable: true },
    { name: "label", type: "string" },
  ],
});
/**
 * Wall time to load the 200k-row table, per engine, measured on each engine's load path as
 * configured here (Minnow insertBatch, SQLite one transaction, PGlite literal batches).
 * Single-sample by nature, but hundreds of milliseconds, so noise is proportionally small.
 */
const ingestMs = { minnow: 0, sqlite: 0, pglite: 0 };
{
  const started = performance.now();
  for (let start = 0; start < rows.length; start += 50_000) {
    await minnow.insertBatch(
      "data",
      rows.slice(start, start + 50_000).map((row) => ({
        ...row,
        joined: row.joined === null ? null : new Date(row.joined),
      })),
    );
  }
  ingestMs.minnow = performance.now() - started;
}
await minnow.createTable({
  name: "dims",
  columns: [
    { name: "region", type: "string" },
    { name: "label", type: "string" },
    { name: "rank", type: "number" },
  ],
});
await minnow.insertBatch("dims", DIMS);
const kysely = new Kysely<BenchmarkDatabase>({ dialect: new MinnowDialect({ driver: minnow }) });

const DOMAIN_ROWS = 20_000;
const domainRows = Array.from({ length: DOMAIN_ROWS }, (_, index) => {
  const id = index + 1;
  return {
    id,
    tenant: `tenant-${String(id % 20)}`,
    amount: `${String(id % 10_000)}.25`,
    payload: `payload-${String(id)}`,
  };
});
await minnow.execute(
  "CREATE TABLE exact_data (id INTEGER PRIMARY KEY, amount NUMERIC(30, 10) NOT NULL)",
);
await minnow.insertBatch(
  "exact_data",
  domainRows.map(({ id, amount }) => ({ id, amount })),
);
await minnow.execute(
  "CREATE TABLE composite_data (tenant TEXT NOT NULL, id INTEGER NOT NULL, payload TEXT NOT NULL, PRIMARY KEY (tenant, id))",
);
await minnow.insertBatch(
  "composite_data",
  domainRows.map(({ tenant, id, payload }) => ({ tenant, id, payload })),
);
// The same rows, minus one deleted row: every delta-* query reads this mutation history.
await minnow.createTable({
  name: "data_mut",
  uniqueKey: "id",
  columns: [
    { name: "id", type: "number" },
    { name: "region", type: "string", nullable: true },
    { name: "amount", type: "number" },
    { name: "active", type: "boolean" },
    { name: "joined", type: "datetime", nullable: true },
    { name: "label", type: "string" },
  ],
});
for (let start = 0; start < rows.length; start += 50_000) {
  await minnow.insertBatch(
    "data_mut",
    rows.slice(start, start + 50_000).map((row) => ({
      ...row,
      joined: row.joined === null ? null : new Date(row.joined),
    })),
  );
}
await minnow.deleteBatch("data_mut", { keys: [7] });

/**
 * A settled history: the same rows with a thousand point updates applied and the background
 * maintenance — folds and collection passes — allowed to finish. This is the steady state a
 * table open all day lives in, and SQLite's and PGlite's costs do not move with history, so the
 * ratio on the settled-* shapes is the engine's own cost of having been used: a regression here
 * is a read getting slower as a table is written to, or maintenance failing to keep up. The
 * settle itself is timed once, against the same updates applied to the other engines, so a fold
 * or a collection pass that grows expensive shows up as well.
 */
const SETTLED_UPDATES = 1_000;
const settledStore = new MemoryBlockStore();
const settled = new MinnowDatabase(settledStore);
await settled.createTable({
  name: "data_settled",
  uniqueKey: "id",
  columns: [
    { name: "id", type: "number" },
    { name: "region", type: "string", nullable: true },
    { name: "amount", type: "number" },
    { name: "active", type: "boolean" },
    { name: "joined", type: "datetime", nullable: true },
    { name: "label", type: "string" },
  ],
});
for (let start = 0; start < rows.length; start += 50_000) {
  await settled.insertBatch(
    "data_settled",
    rows.slice(start, start + 50_000).map((row) => ({
      ...row,
      joined: row.joined === null ? null : new Date(row.joined),
    })),
  );
}
const settledKey = (index: number): number => ((index * 7919) % MUTATION_KEYS) + 1;
const settleMs = { minnow: 0, sqlite: 0, pglite: 0 };
{
  const started = performance.now();
  for (let index = 0; index < SETTLED_UPDATES; index += 1) {
    await settled.execute("UPDATE data_settled SET amount = amount + 1 WHERE id = ?", [
      settledKey(index),
    ]);
  }
  // Wait for the background loops to finish: no active job, the table folded, and the store's
  // footprint no longer moving. A history that does not settle is a failure of the gate, not a
  // slow sample: maintenance has to keep up with a thousand updates.
  const footprint = async (visibleSegments: number): Promise<string> => {
    const stats = await settledStore.getStorageStats();
    return JSON.stringify([
      visibleSegments,
      stats.liveBlockCount + stats.obsoleteBlockCount,
      stats.manifestCount,
    ]);
  };
  let previous: string | undefined;
  let quiet = 0;
  const deadline = performance.now() + 60_000;
  while (quiet < 10) {
    if (performance.now() > deadline) {
      throw new Error(`data_settled did not settle within a minute: ${previous ?? "active"}`);
    }
    await new Promise((resolve) => setTimeout(resolve, 50));
    const activeCompaction = (await settled.listCompactionJobs()).some(
      (job) => job.state !== "published" && job.state !== "cancelled" && job.state !== "aborted",
    );
    const activeCollection = (await settled.listGarbageCollectionJobs()).some(
      (job) => job.state === "planned" || job.state === "running",
    );
    if (activeCompaction || activeCollection) {
      quiet = 0;
      continue;
    }
    const visibleSegments = (await allVisibleSegments(settled, "data_settled")).length;
    if (visibleSegments >= 32) {
      quiet = 0;
      continue;
    }
    const current = await footprint(visibleSegments);
    quiet = current === previous ? quiet + 1 : 0;
    previous = current;
  }
  settleMs.minnow = performance.now() - started;
}
const SETTLED_QUERIES: readonly PerfQuery[] = [
  {
    name: "settled-point-lookup",
    sql: "SELECT id, amount, label FROM data_settled WHERE id = ?",
    params: [123_457],
  },
  { name: "settled-count-star", sql: "SELECT COUNT(*) AS n FROM data_settled" },
  {
    name: "settled-filter-scan",
    sql: "SELECT id, amount FROM data_settled WHERE amount > ? AND region = ?",
    params: [9000, "west"],
  },
];
for (let index = 0; index < CATALOG_TABLES; index += 1) {
  await minnow.createTable({
    name: `spare_${String(index)}`,
    columns: [{ name: "id", type: "number" }],
  });
}

const sqlite = new DatabaseSync(":memory:");
sqlite.exec("PRAGMA case_sensitive_like = ON");
for (let index = 0; index < CATALOG_TABLES; index += 1) {
  sqlite.exec(`CREATE TABLE spare_${String(index)} ("id" INTEGER)`);
}
sqlite.exec(
  `CREATE TABLE data ("id" INTEGER, "region" TEXT, "amount" REAL, "active" INTEGER, "joined" TEXT, "label" TEXT)`,
);
sqlite.exec(`CREATE TABLE dims ("region" TEXT, "label" TEXT, "rank" REAL)`);
sqlite.exec(`CREATE TABLE exact_data ("id" INTEGER PRIMARY KEY, "amount" NUMERIC NOT NULL)`);
sqlite.exec(
  `CREATE TABLE composite_data ("tenant" TEXT NOT NULL, "id" INTEGER NOT NULL, "payload" TEXT NOT NULL, PRIMARY KEY ("tenant", "id"))`,
);
{
  const insert = sqlite.prepare("INSERT INTO data VALUES (?, ?, ?, ?, ?, ?)");
  const started = performance.now();
  sqlite.exec("BEGIN");
  for (const row of rows) {
    insert.run(row.id, row.region, row.amount, row.active ? 1 : 0, row.joined, row.label);
  }
  sqlite.exec("COMMIT");
  ingestMs.sqlite = performance.now() - started;
  const dim = sqlite.prepare("INSERT INTO dims VALUES (?, ?, ?)");
  for (const entry of DIMS) dim.run(entry.region, entry.label, entry.rank);
  const exact = sqlite.prepare("INSERT INTO exact_data VALUES (?, ?)");
  const composite = sqlite.prepare("INSERT INTO composite_data VALUES (?, ?, ?)");
  sqlite.exec("BEGIN");
  for (const row of domainRows) {
    exact.run(row.id, row.amount);
    composite.run(row.tenant, row.id, row.payload);
  }
  sqlite.exec("COMMIT");
  sqlite.exec(
    `CREATE TABLE data_mut ("id" INTEGER, "region" TEXT, "amount" REAL, "active" INTEGER, "joined" TEXT, "label" TEXT)`,
  );
  const mut = sqlite.prepare("INSERT INTO data_mut VALUES (?, ?, ?, ?, ?, ?)");
  sqlite.exec("BEGIN");
  for (const row of rows) {
    mut.run(row.id, row.region, row.amount, row.active ? 1 : 0, row.joined, row.label);
  }
  sqlite.exec("COMMIT");
  sqlite.exec("DELETE FROM data_mut WHERE id = 7");
  sqlite.exec(
    `CREATE TABLE data_settled ("id" INTEGER, "region" TEXT, "amount" REAL, "active" INTEGER, "joined" TEXT, "label" TEXT)`,
  );
  const settledInsert = sqlite.prepare("INSERT INTO data_settled VALUES (?, ?, ?, ?, ?, ?)");
  sqlite.exec("BEGIN");
  for (const row of rows) {
    settledInsert.run(row.id, row.region, row.amount, row.active ? 1 : 0, row.joined, row.label);
  }
  sqlite.exec("COMMIT");
  const settledUpdate = sqlite.prepare("UPDATE data_settled SET amount = amount + 1 WHERE id = ?");
  const settledStarted = performance.now();
  for (let index = 0; index < SETTLED_UPDATES; index += 1) settledUpdate.run(settledKey(index));
  settleMs.sqlite = performance.now() - settledStarted;
}

function sqlLiteral(value: unknown): string {
  if (value === null) return "NULL";
  if (typeof value === "string") return `'${value.replaceAll("'", "''")}'`;
  if (typeof value === "boolean") return value ? "TRUE" : "FALSE";
  return String(value);
}

function positionalToNumbered(sql: string): string {
  let index = 0;
  return sql.replace(/\?/g, () => `$${String((index += 1))}`);
}

console.log("loading pglite...");
const { PGlite } = await import("@electric-sql/pglite");
const pglite = await PGlite.create();
await pglite.exec(
  `CREATE TABLE data ("id" INTEGER, "region" TEXT, "amount" DOUBLE PRECISION, "active" BOOLEAN, "joined" TIMESTAMPTZ, "label" TEXT)`,
);
await pglite.exec(`CREATE TABLE dims ("region" TEXT, "label" TEXT, "rank" DOUBLE PRECISION)`);
await pglite.exec(
  `CREATE TABLE exact_data ("id" INTEGER PRIMARY KEY, "amount" NUMERIC(30, 10) NOT NULL)`,
);
await pglite.exec(
  `CREATE TABLE composite_data ("tenant" TEXT NOT NULL, "id" INTEGER NOT NULL, "payload" TEXT NOT NULL, PRIMARY KEY ("tenant", "id"))`,
);
for (let index = 0; index < CATALOG_TABLES; index += 1) {
  await pglite.exec(`CREATE TABLE spare_${String(index)} ("id" INTEGER)`);
}
{
  const started = performance.now();
  for (let start = 0; start < rows.length; start += 2000) {
    const batch = rows
      .slice(start, start + 2000)
      .map(
        (row) =>
          `(${[row.id, row.region, row.amount, row.active, row.joined, row.label]
            .map(sqlLiteral)
            .join(", ")})`,
      )
      .join(", ");
    await pglite.exec(`INSERT INTO data VALUES ${batch}`);
  }
  ingestMs.pglite = performance.now() - started;
}
await pglite.exec(
  `INSERT INTO dims VALUES ${DIMS.map((d) => `(${[d.region, d.label, d.rank].map(sqlLiteral).join(", ")})`).join(", ")}`,
);
for (let start = 0; start < domainRows.length; start += 2_000) {
  const batch = domainRows.slice(start, start + 2_000);
  await pglite.exec(
    `INSERT INTO exact_data VALUES ${batch
      .map((row) => `(${String(row.id)}, ${sqlLiteral(row.amount)})`)
      .join(", ")}`,
  );
  await pglite.exec(
    `INSERT INTO composite_data VALUES ${batch
      .map((row) => `(${sqlLiteral(row.tenant)}, ${String(row.id)}, ${sqlLiteral(row.payload)})`)
      .join(", ")}`,
  );
}
await pglite.exec(
  `CREATE TABLE data_mut ("id" INTEGER, "region" TEXT, "amount" DOUBLE PRECISION, "active" BOOLEAN, "joined" TIMESTAMPTZ, "label" TEXT)`,
);
for (let start = 0; start < rows.length; start += 2000) {
  const batch = rows
    .slice(start, start + 2000)
    .map(
      (row) =>
        `(${[row.id, row.region, row.amount, row.active, row.joined, row.label]
          .map(sqlLiteral)
          .join(", ")})`,
    )
    .join(", ");
  await pglite.exec(`INSERT INTO data_mut VALUES ${batch}`);
}
await pglite.exec("DELETE FROM data_mut WHERE id = 7");
await pglite.exec(
  `CREATE TABLE data_settled ("id" INTEGER, "region" TEXT, "amount" DOUBLE PRECISION, "active" BOOLEAN, "joined" TIMESTAMPTZ, "label" TEXT)`,
);
for (let start = 0; start < rows.length; start += 2000) {
  const batch = rows
    .slice(start, start + 2000)
    .map(
      (row) =>
        `(${[row.id, row.region, row.amount, row.active, row.joined, row.label]
          .map(sqlLiteral)
          .join(", ")})`,
    )
    .join(", ");
  await pglite.exec(`INSERT INTO data_settled VALUES ${batch}`);
}
{
  const started = performance.now();
  for (let index = 0; index < SETTLED_UPDATES; index += 1) {
    await pglite.query("UPDATE data_settled SET amount = amount + 1 WHERE id = $1", [
      settledKey(index),
    ]);
  }
  settleMs.pglite = performance.now() - started;
}

function pgliteRunner(query: PerfQuery): () => Promise<unknown> {
  const sql = positionalToNumbered(query.sql);
  const params = [...(query.params ?? [])];
  return () => pglite.query(sql, params);
}

type EngineName = PerformanceEngine;
const ENGINES: readonly EngineName[] = PERFORMANCE_ENGINES;

const workloadNames = [
  "bulk-ingest",
  ...QUERIES.map(({ name }) => name),
  ...MUTATIONS.map(({ name }) => name),
  "settle-after-updates",
  ...SETTLED_QUERIES.map(({ name }) => name),
  "bulk-delete",
];
const baseline = selectPerformanceThresholds(baselineFile, profile, workloadNames, update);

interface Result {
  name: string;
  minnow: Timing;
  engine: Record<EngineName, Timing>;
}

const results: Result[] = [
  {
    name: "bulk-ingest",
    minnow: onceOnly(ingestMs.minnow),
    engine: { sqlite: onceOnly(ingestMs.sqlite), pglite: onceOnly(ingestMs.pglite) },
  },
];
for (const query of QUERIES) {
  const statement = sqlite.prepare(query.sql);
  const sqliteParams = (query.params ?? []) as Array<number | string>;
  const runners = {
    minnow: minnowRunner(minnow, query),
    sqlite: () => statement.all(...sqliteParams),
    pglite: pgliteRunner(query),
  };
  assertQueryEquivalent(query, await runners.minnow(), runners.sqlite());
  const singleRuns = [
    await warmUp(runners.minnow),
    await warmUp(runners.sqlite),
    await warmUp(runners.pglite),
  ];
  const iterations = singleRuns.map((singleRun) => iterationsFor(singleRun));
  results.push({
    name: query.name,
    minnow: await timeRepeated(runners.minnow, iterations[0] ?? 1),
    engine: {
      sqlite: await timeRepeated(runners.sqlite, iterations[1] ?? 1),
      pglite: await timeRepeated(runners.pglite, iterations[2] ?? 1),
    },
  });
}

/**
 * Write shapes, timed the same way as the reads: one iteration count shared by all three engines
 * so the ratio compares like with like, and a cycling key so a repeated sample is equivalent work
 * rather than the same row over and over.
 */
for (const mutation of MUTATIONS) {
  const statement = sqlite.prepare(mutation.sql);
  const pgliteSql = positionalToNumbered(mutation.sql);
  // Each engine gets the same logical sequence. A shared counter made the three runners touch
  // disjoint keys, which looked equivalent in timing but prevented a meaningful state check.
  const counters = { minnow: 0, sqlite: 0, pglite: 0 };
  const runners = {
    minnow: () => minnow.execute(mutation.sql, mutation.params(counters.minnow++) as never),
    sqlite: () => statement.run(...(mutation.params(counters.sqlite++) as Array<number | string>)),
    pglite: () => pglite.query(pgliteSql, [...mutation.params(counters.pglite++)]),
  };
  await warmUp(runners.minnow);
  await warmUp(runners.sqlite);
  await warmUp(runners.pglite);
  results.push({
    name: mutation.name,
    minnow: await timeRepeated(runners.minnow, MUTATION_ITERATIONS),
    engine: {
      sqlite: await timeRepeated(runners.sqlite, MUTATION_ITERATIONS),
      pglite: await timeRepeated(runners.pglite, MUTATION_ITERATIONS),
    },
  });
  assertQueryEquivalent(
    MUTATION_CHECK,
    await minnow.query(MUTATION_CHECK.sql, { memoize: false }),
    sqlite.prepare(MUTATION_CHECK.sql).all(),
  );
}

/**
 * The settled shapes: the same read shapes over the history that was written to a thousand
 * times and then maintained, plus the settle itself, once.
 */
results.push({
  name: "settle-after-updates",
  minnow: onceOnly(settleMs.minnow),
  engine: { sqlite: onceOnly(settleMs.sqlite), pglite: onceOnly(settleMs.pglite) },
});
for (const query of SETTLED_QUERIES) {
  const statement = sqlite.prepare(query.sql);
  const sqliteParams = (query.params ?? []) as Array<number | string>;
  const runners = {
    minnow: minnowRunner(settled, query),
    sqlite: () => statement.all(...sqliteParams),
    pglite: pgliteRunner(query),
  };
  assertQueryEquivalent(query, await runners.minnow(), runners.sqlite());
  const singleRuns = [
    await warmUp(runners.minnow),
    await warmUp(runners.sqlite),
    await warmUp(runners.pglite),
  ];
  const iterations = singleRuns.map((singleRun) => iterationsFor(singleRun));
  results.push({
    name: query.name,
    minnow: await timeRepeated(runners.minnow, iterations[0] ?? 1),
    engine: {
      sqlite: await timeRepeated(runners.sqlite, iterations[1] ?? 1),
      pglite: await timeRepeated(runners.pglite, iterations[2] ?? 1),
    },
  });
}

/**
 * Bulk delete, measured once rather than repeated: a row can only be deleted the first time, so
 * there is no way to run this shape twice on the same data and have it mean the same thing. It
 * runs last, after every other measurement has taken its samples from the full table.
 */
{
  const first = MUTATION_KEYS + 1;
  const last = MUTATION_KEYS + 50_000;
  const deleteMs = { minnow: 0, sqlite: 0, pglite: 0 };
  let started = performance.now();
  await minnow.execute("DELETE FROM data_mut WHERE id BETWEEN ? AND ?", [first, last]);
  deleteMs.minnow = performance.now() - started;

  started = performance.now();
  sqlite.prepare("DELETE FROM data_mut WHERE id BETWEEN ? AND ?").run(first, last);
  deleteMs.sqlite = performance.now() - started;

  started = performance.now();
  await pglite.query("DELETE FROM data_mut WHERE id BETWEEN $1 AND $2", [first, last]);
  deleteMs.pglite = performance.now() - started;

  assertQueryEquivalent(
    MUTATION_CHECK,
    await minnow.query(MUTATION_CHECK.sql, { memoize: false }),
    sqlite.prepare(MUTATION_CHECK.sql).all(),
  );

  results.push({
    name: "bulk-delete",
    minnow: onceOnly(deleteMs.minnow),
    engine: { sqlite: onceOnly(deleteMs.sqlite), pglite: onceOnly(deleteMs.pglite) },
  });
}

sqlite.close();
await pglite.close();
await kysely.destroy();

console.log(`\nSQL performance gate — ${String(ROWS)} rows, median of ${String(RUNS)} runs`);
console.log("query                minnow(ms)      sqlite(ms)            pglite(ms)");
const versus = (minnowMs: number, engineMs: number): string => {
  const ratio = minnowMs / engineMs;
  return ratio <= 1 ? `${(1 / ratio).toFixed(1)}x faster` : `${ratio.toFixed(1)}x slower`;
};
const failures: string[] = [];
const newThresholds: PerformanceThresholds = {};
for (const result of results) {
  const minnowMs = result.minnow.median;
  const line: string[] = [result.name.padEnd(20), minnowMs.toFixed(2).padStart(11)];
  newThresholds[result.name] = {};
  for (const engine of ENGINES) {
    const engineMs = result.engine[engine].median;
    // The reported comparison is median against median -- the typical cost, which is what a
    // reader wants. The recorded threshold comes from the spread instead, so a shape is judged
    // against its own noise rather than against a number picked for the noisiest shape.
    const ratio = ratioOf(minnowMs, engineMs);
    // Significant digits rather than fixed decimals: a memo hit against a re-executing engine
    // is a ratio of 0.00015, which two decimals would round to a threshold of zero that nothing
    // can satisfy.
    newThresholds[result.name][engine] = Number(
      thresholdFor(result.minnow, result.engine[engine]).toPrecision(3),
    );
    const threshold = update ? undefined : baseline?.[result.name]?.[engine];
    const flag = threshold !== undefined && ratio > threshold ? "!" : " ";
    line.push(
      `${engineMs.toFixed(2).padStart(10)} ${versus(minnowMs, engineMs).padStart(11)}${flag}`,
    );
    if (threshold !== undefined && ratio > threshold) {
      failures.push(
        `${result.name} vs ${engine}: ratio ${ratio.toFixed(2)} exceeds threshold ${String(threshold)} ` +
          `(spread-calibrated threshold from this run: ${String(newThresholds[result.name]?.[engine])})`,
      );
    }
  }
  console.log(line.join(""));
}

if (update) {
  writeFileSync(
    BASELINE_PATH,
    `${JSON.stringify(
      updatedPerformanceBaseline(baselineFile, ROWS, profile, newThresholds),
      null,
      2,
    )}\n`,
  );
  console.log(`\nBaseline profile ${profile} updated.`);
}
if (failures.length > 0) {
  console.error(`\n${String(failures.length)} performance regression(s):`);
  for (const failure of failures) console.error(`  ${failure}`);
  process.exit(1);
}
console.log("\nNo performance regressions.");
