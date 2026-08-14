/**
 * The SQL performance gate: a seeded dataset and query suite timed on the full MinnowDatabase
 * pipeline against three engines — native SQLite (node:sqlite), PGlite (Wasm Postgres), and
 * DuckDB (native vectorized OLAP). Native builds are a deliberately harsh baseline: the
 * browser competitors are the Wasm builds, which run slower than what is measured here.
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
const RUNS = 7;
/** Headroom multiplier applied to a measured ratio when writing a new baseline. */
const MARGIN = 1.5;

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
];

function median(samples: number[]): number {
  const sorted = [...samples].sort((left, right) => left - right);
  return sorted[Math.floor(sorted.length / 2)] ?? 0;
}

async function timeMinnow(database: MinnowDatabase, query: PerfQuery): Promise<number> {
  // The gate measures execution, not the probe-validated result memo (which would answer
  // every repeat sample from cache); real applications keep the default memoize: true.
  const options = {
    memoize: false as const,
    ...(query.params === undefined ? {} : { params: query.params as never }),
  };
  for (let index = 0; index < WARMUP; index += 1) await database.query(query.sql, options);
  const samples: number[] = [];
  for (let index = 0; index < RUNS; index += 1) {
    const started = performance.now();
    await database.query(query.sql, options);
    samples.push(performance.now() - started);
  }
  return median(samples);
}

function timeSqlite(database: DatabaseSync, query: PerfQuery): number {
  const statement = database.prepare(query.sql);
  const params = (query.params ?? []) as Array<number | string>;
  for (let index = 0; index < WARMUP; index += 1) statement.all(...params);
  const samples: number[] = [];
  for (let index = 0; index < RUNS; index += 1) {
    const started = performance.now();
    statement.all(...params);
    samples.push(performance.now() - started);
  }
  return median(samples);
}

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
for (let start = 0; start < rows.length; start += 50_000) {
  await minnow.insertBatch(
    "data",
    rows.slice(start, start + 50_000).map((row) => ({
      ...row,
      joined: row.joined === null ? null : new Date(row.joined),
    })),
  );
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

const sqlite = new DatabaseSync(":memory:");
sqlite.exec("PRAGMA case_sensitive_like = ON");
sqlite.exec(
  `CREATE TABLE data ("id" INTEGER, "region" TEXT, "amount" REAL, "active" INTEGER, "joined" TEXT, "label" TEXT)`,
);
sqlite.exec(`CREATE TABLE dims ("region" TEXT, "label" TEXT, "rank" REAL)`);
{
  const insert = sqlite.prepare("INSERT INTO data VALUES (?, ?, ?, ?, ?, ?)");
  sqlite.exec("BEGIN");
  for (const row of rows) {
    insert.run(row.id, row.region, row.amount, row.active ? 1 : 0, row.joined, row.label);
  }
  sqlite.exec("COMMIT");
  const dim = sqlite.prepare("INSERT INTO dims VALUES (?, ?, ?)");
  for (const entry of DIMS) dim.run(entry.region, entry.label, entry.rank);
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
await pglite.exec(
  `INSERT INTO dims VALUES ${DIMS.map((d) => `(${[d.region, d.label, d.rank].map(sqlLiteral).join(", ")})`).join(", ")}`,
);

console.log("loading duckdb...");
const { DuckDBInstance } = await import("@duckdb/node-api");
const duckdbInstance = await DuckDBInstance.create(":memory:");
const duckdb = await duckdbInstance.connect();
await duckdb.run(
  `CREATE TABLE data (id INTEGER, region TEXT, amount DOUBLE, active BOOLEAN, joined TIMESTAMPTZ, label TEXT)`,
);
await duckdb.run(`CREATE TABLE dims (region TEXT, label TEXT, rank DOUBLE)`);
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
  await duckdb.run(`INSERT INTO data VALUES ${batch}`);
}
await duckdb.run(
  `INSERT INTO dims VALUES ${DIMS.map((d) => `(${[d.region, d.label, d.rank].map(sqlLiteral).join(", ")})`).join(", ")}`,
);

async function timePglite(query: PerfQuery): Promise<number> {
  const sql = positionalToNumbered(query.sql);
  const params = [...(query.params ?? [])];
  for (let index = 0; index < WARMUP; index += 1) await pglite.query(sql, params);
  const samples: number[] = [];
  for (let index = 0; index < RUNS; index += 1) {
    const started = performance.now();
    await pglite.query(sql, params);
    samples.push(performance.now() - started);
  }
  return median(samples);
}

async function timeDuckdb(query: PerfQuery): Promise<number> {
  const params = [...(query.params ?? [])];
  const prepared = await duckdb.prepare(query.sql);
  const readAll = async (): Promise<void> => {
    prepared.bind(params as never);
    const reader = await prepared.runAndReadAll();
    reader.getRowObjects();
  };
  for (let index = 0; index < WARMUP; index += 1) await readAll();
  const samples: number[] = [];
  for (let index = 0; index < RUNS; index += 1) {
    const started = performance.now();
    await readAll();
    samples.push(performance.now() - started);
  }
  return median(samples);
}

type EngineName = "sqlite" | "pglite" | "duckdb";
const ENGINES: readonly EngineName[] = ["sqlite", "pglite", "duckdb"];

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
  minnowMs: number;
  engineMs: Record<EngineName, number>;
}

const results: Result[] = [];
for (const query of QUERIES) {
  const minnowMs = await timeMinnow(minnow, query);
  results.push({
    name: query.name,
    minnowMs,
    engineMs: {
      sqlite: timeSqlite(sqlite, query),
      pglite: await timePglite(query),
      duckdb: await timeDuckdb(query),
    },
  });
}
sqlite.close();
await pglite.close();
duckdb.closeSync();

console.log(`\nSQL performance gate — ${String(ROWS)} rows, median of ${String(RUNS)} runs`);
console.log(
  "query                minnow(ms)      sqlite(ms)            pglite(ms)            duckdb(ms)",
);
const versus = (minnowMs: number, engineMs: number): string => {
  const ratio = minnowMs / engineMs;
  return ratio <= 1 ? `${(1 / ratio).toFixed(1)}x faster` : `${ratio.toFixed(1)}x slower`;
};
const failures: string[] = [];
const newThresholds: Baseline["thresholds"] = {};
for (const result of results) {
  const line: string[] = [result.name.padEnd(20), result.minnowMs.toFixed(2).padStart(11)];
  newThresholds[result.name] = {};
  for (const engine of ENGINES) {
    const engineMs = result.engineMs[engine];
    const ratio = result.minnowMs / engineMs;
    newThresholds[result.name][engine] = Number((ratio * MARGIN).toFixed(2));
    const threshold = baseline?.thresholds[result.name]?.[engine];
    const flag = threshold !== undefined && ratio > threshold ? "!" : " ";
    line.push(
      `${engineMs.toFixed(2).padStart(10)} ${versus(result.minnowMs, engineMs).padStart(11)}${flag}`,
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
