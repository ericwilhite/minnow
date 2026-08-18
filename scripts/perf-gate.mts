/**
 * The SQL performance gate: a seeded dataset and query suite timed on the full MinnowDatabase
 * pipeline against native SQLite (node:sqlite) and PGlite (Wasm Postgres). Native SQLite is a
 * deliberately harsh baseline: the browser competitor is its Wasm build, which runs slower
 * than what is measured here.
 *
 * Each query's minnow/engine time ratio must stay at or below the checked-in threshold in
 * packages/core/perf-baseline.json. Thresholds pin the current ratios with headroom, so the
 * gate catches regressions rather than machine differences; a missing baseline bootstraps one
 * from the current run. Run with --update to rewrite thresholds after intentional changes.
 */
import { readFileSync, writeFileSync, existsSync } from "node:fs";
import { DatabaseSync } from "node:sqlite";
import { MemoryBlockStore } from "../packages/core/src/storage/index.js";
import { MinnowDatabase } from "../packages/core/src/engine/database.js";

const BASELINE_PATH = new URL("../packages/core/perf-baseline.json", import.meta.url);
const ROWS = 200_000;
const WARMUP = 2;
const RUNS = 9;
/**
 * Sub-millisecond queries are timed in batches rather than floored. Flooring both sides of the
 * ratio — the old approach — made every fast query gate as 0.5ms against 0.5ms, so a point
 * lookup could degrade sevenfold and still pass: exactly how a per-query catalog scan once
 * reached main unnoticed. Batching removes the noise instead of hiding it. Each sample runs the
 * query enough times to take at least this long, and the per-iteration cost is the total over
 * the count, which makes a 0.07ms query as measurable as a 7ms one and keeps the gate's
 * cross-engine ratio — the part that survives a change of machine — meaningful at every scale.
 */
const TARGET_SAMPLE_MS = 25;
const MAX_ITERATIONS = 2_000;
/** A tiny floor remains, against a divide-by-zero on an engine that answers instantly. */
const ratioOf = (minnowMs: number, engineMs: number): number =>
  Math.max(minnowMs, 0.0001) / Math.max(engineMs, 0.0001);

/**
 * How many times to repeat one query per timed sample. Measured once from a single run, so the
 * count reflects this machine, and shared by all three engines for a given query so the ratio
 * compares like with like.
 */
function iterationsFor(singleRunMs: number): number {
  if (singleRunMs >= TARGET_SAMPLE_MS) return 1;
  return Math.min(
    MAX_ITERATIONS,
    Math.max(1, Math.ceil(TARGET_SAMPLE_MS / Math.max(singleRunMs, 0.001))),
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
  /** Serve Minnow's probe-validated result memo instead of re-executing (gates memo-hit latency). */
  readonly memoize?: boolean;
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
  },
  {
    // A page deep enough that the bounded top-K sink retains most of what it scans. The bound
    // buys a memory guarantee — retention stays proportional to the limit rather than the table
    // — and past roughly a quarter of the rows it costs more time than sorting everything once.
    // Gated so that trade stays where it was measured instead of drifting.
    name: "deep-page",
    sql: "SELECT id, amount FROM data ORDER BY region, id LIMIT 100000",
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
 */
const MARGIN_FLOOR = 1.15;
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
    sql: "UPDATE data_mut SET label = ? WHERE region = ? AND id < ?",
    params: (n) => ["alpha", "west", (n % 1_000) + 1],
  },
];

function minnowRunner(database: MinnowDatabase, query: PerfQuery): () => Promise<unknown> {
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

const minnow = new MinnowDatabase(new MemoryBlockStore());
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

function pgliteRunner(query: PerfQuery): () => Promise<unknown> {
  const sql = positionalToNumbered(query.sql);
  const params = [...(query.params ?? [])];
  return () => pglite.query(sql, params);
}

type EngineName = "sqlite" | "pglite";
const ENGINES: readonly EngineName[] = ["sqlite", "pglite"];

interface Baseline {
  rows: number;
  thresholds: Record<string, Partial<Record<EngineName, number>>>;
}

const update = process.argv.includes("--update");
const baseline: Baseline | undefined = existsSync(BASELINE_PATH)
  ? (JSON.parse(readFileSync(BASELINE_PATH, "utf8")) as Baseline)
  : undefined;

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
  // One iteration count per query, taken from the slowest engine's single run so that every
  // engine's sample clears the timer's resolution, and shared so the ratio compares like with
  // like.
  const singleRuns = [
    await warmUp(runners.minnow),
    await warmUp(runners.sqlite),
    await warmUp(runners.pglite),
  ];
  const iterations = iterationsFor(Math.max(...singleRuns));
  results.push({
    name: query.name,
    minnow: await timeRepeated(runners.minnow, iterations),
    engine: {
      sqlite: await timeRepeated(runners.sqlite, iterations),
      pglite: await timeRepeated(runners.pglite, iterations),
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
  let counter = 0;
  const runners = {
    minnow: () => minnow.execute(mutation.sql, mutation.params(counter++) as never),
    sqlite: () => statement.run(...(mutation.params(counter++) as Array<number | string>)),
    pglite: () => pglite.query(pgliteSql, [...mutation.params(counter++)]),
  };
  const singleRuns = [
    await warmUp(runners.minnow),
    await warmUp(runners.sqlite),
    await warmUp(runners.pglite),
  ];
  const iterations = iterationsFor(Math.max(...singleRuns));
  results.push({
    name: mutation.name,
    minnow: await timeRepeated(runners.minnow, iterations),
    engine: {
      sqlite: await timeRepeated(runners.sqlite, iterations),
      pglite: await timeRepeated(runners.pglite, iterations),
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

  results.push({
    name: "bulk-delete",
    minnow: onceOnly(deleteMs.minnow),
    engine: { sqlite: onceOnly(deleteMs.sqlite), pglite: onceOnly(deleteMs.pglite) },
  });
}

sqlite.close();
await pglite.close();

console.log(`\nSQL performance gate — ${String(ROWS)} rows, median of ${String(RUNS)} runs`);
console.log("query                minnow(ms)      sqlite(ms)            pglite(ms)");
const versus = (minnowMs: number, engineMs: number): string => {
  const ratio = minnowMs / engineMs;
  return ratio <= 1 ? `${(1 / ratio).toFixed(1)}x faster` : `${ratio.toFixed(1)}x slower`;
};
const failures: string[] = [];
const newThresholds: Baseline["thresholds"] = {};
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
    const threshold = baseline?.thresholds[result.name]?.[engine];
    const flag = threshold !== undefined && ratio > threshold ? "!" : " ";
    line.push(
      `${engineMs.toFixed(2).padStart(10)} ${versus(minnowMs, engineMs).padStart(11)}${flag}`,
    );
    if (threshold !== undefined && ratio > threshold) {
      failures.push(
        `${result.name} vs ${engine}: ratio ${ratio.toFixed(2)} exceeds threshold ${String(threshold)}`,
      );
    }
  }
  console.log(line.join(""));
}

if (baseline === undefined || update) {
  writeFileSync(
    BASELINE_PATH,
    `${JSON.stringify({ rows: ROWS, thresholds: newThresholds }, null, 2)}\n`,
  );
  console.log(
    baseline === undefined
      ? "\nBaseline bootstrapped — commit packages/core/perf-baseline.json."
      : "\nBaseline updated.",
  );
}
if (failures.length > 0) {
  console.error(`\n${String(failures.length)} performance regression(s):`);
  for (const failure of failures) console.error(`  ${failure}`);
  process.exit(1);
}
console.log("\nNo performance regressions.");
