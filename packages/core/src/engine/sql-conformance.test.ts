/**
 * Differential SQL conformance harness. A seeded generator produces a corpus of queries over
 * the supported SQL surface; every query executes through the full MinnowDatabase pipeline,
 * through the row executor, and through SQLite (the reference oracle, via node:sqlite — no
 * dependency added). The two Minnow paths must agree exactly; Minnow and SQLite must agree
 * after normalizing representation differences (booleans, dates, float rounding).
 *
 * The corpus deliberately avoids forms where the engines' documented semantics differ:
 * `/` (SQLite does integer division on integers), ROUND (half-away-from-zero vs half-even),
 * LIKE case-insensitivity (disabled via PRAGMA case_sensitive_like), and Minnow extensions
 * (MATCH/BM25, DATE_TRUNC, DATE literals). Everything else that both engines parse is fair
 * game, and a mismatch fails the suite with the offending statement.
 */
import { DatabaseSync } from "node:sqlite";
import { describe, expect, it } from "vitest";
import { MemoryBlockStore } from "../storage/index.js";
import { MinnowDatabase, type DatabaseRow } from "./database.js";
import {
  bindPlanParameters,
  compileQuery,
  executeRowQuery,
  type QueryResult,
  type QueryValue,
} from "./query.js";

// --- Deterministic fixture ----------------------------------------------------------------------

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

const REGIONS = ["west", "east", "north", "south", null] as const;
const LABELS = ["alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf", "hotel"];
const DATES = [
  "2025-11-05T08:30:00.000Z",
  "2025-12-30T00:00:00.000Z",
  "2026-01-02T03:04:05.000Z",
  "2026-02-01T12:00:00.000Z",
  "2026-03-15T23:59:59.000Z",
  null,
] as const;

interface FixtureRow {
  id: number;
  region: string | null;
  amount: number;
  active: boolean;
  joined: Date | null;
  label: string;
}

function buildFixture(): FixtureRow[] {
  const rng = mulberry32(0x5eed);
  const rows: FixtureRow[] = [];
  for (let id = 1; id <= 150; id += 1) {
    const region = REGIONS[Math.floor(rng() * REGIONS.length)] ?? null;
    const joined = DATES[Math.floor(rng() * DATES.length)] ?? null;
    rows.push({
      id,
      region,
      // Quarters keep every sum and difference exact in doubles, so aggregate comparisons
      // cannot drift on accumulation order.
      amount: Math.floor(rng() * 400) / 4,
      active: rng() < 0.5,
      joined: joined === null ? null : new Date(joined),
      label: LABELS[Math.floor(rng() * LABELS.length)] ?? "alpha",
    });
  }
  return rows;
}

const fixture = buildFixture();
const dims = [
  { region: "west", label: "West Coast", rank: 1 },
  { region: "east", label: "East Coast", rank: 2 },
  { region: "north", label: "North", rank: 3 },
  { region: "central", label: "Central", rank: 4 },
];

async function minnowFixture(): Promise<MinnowDatabase> {
  const database = new MinnowDatabase(new MemoryBlockStore(), { rowsPerBlock: 32 });
  await database.createTable({
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
  await database.insertBatch("data", fixture as unknown as DatabaseRow[]);
  await database.createTable({
    name: "dims",
    columns: [
      { name: "region", type: "string" },
      { name: "label", type: "string" },
      { name: "rank", type: "number" },
    ],
  });
  await database.insertBatch("dims", dims);
  return database;
}

const rowTables = new Map<string, DatabaseRow[]>([
  ["data", fixture as unknown as DatabaseRow[]],
  ["dims", dims],
]);

function sqliteFixture(): DatabaseSync {
  const database = new DatabaseSync(":memory:");
  database.exec("PRAGMA case_sensitive_like = ON");
  database.exec(
    `CREATE TABLE data ("id" INTEGER, "region" TEXT, "amount" REAL, "active" INTEGER, "joined" TEXT, "label" TEXT)`,
  );
  database.exec(`CREATE TABLE dims ("region" TEXT, "label" TEXT, "rank" REAL)`);
  const insert = database.prepare(`INSERT INTO data VALUES (?, ?, ?, ?, ?, ?)`);
  for (const row of fixture) {
    insert.run(
      row.id,
      row.region,
      row.amount,
      row.active ? 1 : 0,
      row.joined === null ? null : row.joined.toISOString(),
      row.label,
    );
  }
  const insertDim = database.prepare(`INSERT INTO dims VALUES (?, ?, ?)`);
  for (const dim of dims) insertDim.run(dim.region, dim.label, dim.rank);
  return database;
}

// --- Query corpus -------------------------------------------------------------------------------

type OracleName = "sqlite" | "pglite" | "duckdb";

interface Case {
  sql: string;
  params?: QueryValue[];
  /** Compare row order exactly; requires an ORDER BY ending in a unique key. */
  ordered: boolean;
  /** Oracles to skip, each for a documented semantic or dialect divergence. */
  skip?: readonly OracleName[];
}

type Template = (rng: () => number) => Case;

function pick<T>(rng: () => number, values: readonly T[]): T {
  const value = values[Math.floor(rng() * values.length)];
  if (value === undefined) throw new Error("empty pick pool");
  return value;
}

const comparisons = ["=", "!=", "<>", ">", ">=", "<", "<="] as const;
const someRegions = ["west", "east", "north", "south"] as const;
const patterns = ["a%", "%o", "%lt%", "_o%", "%l_a%", "echo"] as const;

const templates: Template[] = [
  (rng) => ({
    sql: `SELECT id, amount, amount * 2.0 + ${String(pick(rng, [1, 2.5, 10]))} AS scaled FROM data WHERE amount ${pick(rng, comparisons)} ? ORDER BY id`,
    params: [Math.floor(rng() * 400) / 4],
    ordered: true,
  }),
  (rng) => ({
    sql: `SELECT id, region FROM data WHERE (amount > ? AND active = ${rng() < 0.5 ? "TRUE" : "FALSE"}) OR (region = ? AND NOT amount >= ?) ORDER BY id`,
    params: [Math.floor(rng() * 300) / 4, pick(rng, someRegions), Math.floor(rng() * 300) / 4],
    ordered: true,
  }),
  (rng) => ({
    sql: `SELECT id, label FROM data WHERE region IN (?, ?) ORDER BY id`,
    params: [pick(rng, someRegions), pick(rng, someRegions)],
    ordered: true,
  }),
  (rng) => ({
    sql: `SELECT id FROM data WHERE region NOT IN (?, ?) ORDER BY id`,
    params: [pick(rng, someRegions), pick(rng, someRegions)],
    ordered: true,
  }),
  () => ({ sql: `SELECT id, amount FROM data WHERE region IS NULL ORDER BY id`, ordered: true }),
  (rng) => ({
    sql: `SELECT id FROM data WHERE region IS NOT NULL AND label LIKE '${pick(rng, patterns)}' ORDER BY id`,
    ordered: true,
  }),
  (rng) => ({
    sql: `SELECT id, amount FROM data WHERE amount BETWEEN ? AND ? ORDER BY id`,
    params: [Math.floor(rng() * 200) / 4, Math.floor(rng() * 200) / 4 + 50],
    ordered: true,
  }),
  (rng) => ({
    sql: `SELECT id, joined FROM data WHERE joined IS NOT NULL AND joined >= ? ORDER BY id`,
    params: [
      new Date(
        pick(
          rng,
          DATES.filter((date) => date !== null),
        ),
      ),
    ],
    ordered: true,
  }),
  (rng) => ({
    sql: `SELECT id, CASE WHEN amount >= ? THEN 'high' WHEN amount >= ? THEN 'mid' ELSE 'low' END AS tier, COALESCE(region, 'none') AS place FROM data ORDER BY id`,
    params: [60 + Math.floor(rng() * 20), 20 + Math.floor(rng() * 20)],
    ordered: true,
  }),
  (rng) => ({
    sql: `SELECT region, COUNT(*) AS c, SUM(amount) AS s, AVG(amount) AS a, MIN(amount) AS lo, MAX(amount) AS hi FROM data WHERE amount > ? GROUP BY region`,
    params: [Math.floor(rng() * 100) / 4],
    ordered: false,
  }),
  (rng) => ({
    sql: `SELECT region, COUNT(*) AS c FROM data GROUP BY region HAVING COUNT(*) > ${String(10 + Math.floor(rng() * 15))}`,
    ordered: false,
  }),
  () => ({ sql: `SELECT COUNT(DISTINCT region) AS regions FROM data`, ordered: false }),
  (rng) => ({
    sql: `SELECT COUNT(*) AS c, SUM(amount) AS s, MIN(joined) AS first_joined, MAX(joined) AS last_joined FROM data WHERE active = ${rng() < 0.5 ? "TRUE" : "FALSE"}`,
    ordered: false,
  }),
  (rng) => ({
    sql: `SELECT d.id AS id, m.label AS place FROM data d JOIN dims m ON d.region = m.region WHERE d.amount > ? ORDER BY d.id`,
    params: [Math.floor(rng() * 200) / 4],
    ordered: true,
  }),
  (rng) => ({
    sql: `SELECT d.id AS id, m.label AS place FROM data d LEFT JOIN dims m ON d.region = m.region WHERE d.id <= ${String(40 + Math.floor(rng() * 60))} ORDER BY d.id`,
    ordered: true,
  }),
  () => ({
    sql: `SELECT d.id AS id FROM data d LEFT JOIN dims m ON d.region = m.region WHERE m.label IS NULL ORDER BY d.id`,
    ordered: true,
  }),
  (rng) => {
    const op = pick(rng, ["UNION", "UNION ALL", "INTERSECT", "EXCEPT"] as const);
    return {
      sql: `SELECT region FROM data WHERE amount > ? ${op} SELECT region FROM dims`,
      params: [Math.floor(rng() * 300) / 4],
      ordered: false,
    };
  },
  (rng) => ({
    sql: `SELECT t.region AS region, t.c AS c FROM (SELECT region, COUNT(*) AS c FROM data GROUP BY region) t WHERE t.c > ${String(5 + Math.floor(rng() * 20))}`,
    ordered: false,
  }),
  (rng) => ({
    sql: `WITH busy AS (SELECT region, COUNT(*) AS c FROM data GROUP BY region) SELECT region, c FROM busy WHERE c > ${String(5 + Math.floor(rng() * 20))}`,
    ordered: false,
  }),
  () => ({
    sql: `SELECT id, amount FROM data WHERE amount > (SELECT AVG(amount) FROM data) ORDER BY id`,
    ordered: true,
  }),
  (rng) => ({
    sql: `SELECT id FROM data WHERE region IN (SELECT region FROM dims WHERE rank <= ${String(1 + Math.floor(rng() * 3))}) ORDER BY id`,
    ordered: true,
  }),
  (rng) => ({
    sql: `SELECT id FROM data WHERE EXISTS (SELECT 1 FROM dims WHERE rank > ${String(Math.floor(rng() * 6))}) ORDER BY id LIMIT 10`,
    ordered: true,
  }),
  () => ({
    sql: `SELECT id, region, ROW_NUMBER() OVER (PARTITION BY region ORDER BY id) AS rn FROM data`,
    ordered: false,
  }),
  () => ({
    sql: `SELECT id, RANK() OVER (ORDER BY amount) AS r, DENSE_RANK() OVER (ORDER BY amount) AS dr FROM data`,
    ordered: false,
  }),
  () => ({
    sql: `SELECT id, SUM(amount) OVER (PARTITION BY region) AS regional FROM data`,
    ordered: false,
  }),
  (rng) => ({
    sql: `SELECT id, region FROM data WHERE active = TRUE ORDER BY amount * -1.0, id LIMIT ${String(5 + Math.floor(rng() * 20))}`,
    ordered: true,
  }),
  (rng) => {
    const limit = 5 + Math.floor(rng() * 20);
    const offset = Math.floor(rng() * 30);
    return {
      sql: `SELECT id FROM data ORDER BY id LIMIT ${String(limit)} OFFSET ${String(offset)}`,
      ordered: true,
    };
  },
  () => ({ sql: `SELECT DISTINCT region FROM data`, ordered: false }),
  () => ({ sql: `SELECT DISTINCT * FROM dims`, ordered: false }),
  () => ({
    sql: `SELECT DISTINCT * FROM (SELECT region, active FROM data WHERE amount > 5) filtered`,
    ordered: false,
  }),
  () => ({
    // The recursive base seeds from a real table: the SQL surface has no FROM-less SELECT.
    sql: `WITH RECURSIVE n AS (SELECT MIN(rank) AS v FROM dims UNION ALL SELECT v + 1 AS v FROM n WHERE v < 20) SELECT v FROM n`,
    ordered: false,
  }),
  (rng) => ({
    sql: `SELECT region, active, COUNT(*) AS c FROM data GROUP BY region, active HAVING SUM(amount) > ?`,
    params: [Math.floor(rng() * 800) / 4],
    ordered: false,
  }),
  (rng) => ({
    sql: `SELECT d.id AS id FROM data d WHERE EXISTS (SELECT m.region FROM dims m WHERE m.region = d.region AND m.rank <= ?) ORDER BY d.id`,
    params: [1 + Math.floor(rng() * 4)],
    ordered: true,
  }),
  (rng) => ({
    sql: `SELECT d.id AS id FROM data d WHERE NOT EXISTS (SELECT m.region FROM dims m WHERE m.region = d.region AND m.rank <= ?) ORDER BY d.id`,
    params: [1 + Math.floor(rng() * 4)],
    ordered: true,
  }),
  () => ({
    sql: `SELECT d.id AS id FROM data d WHERE d.amount > (SELECT AVG(q.amount) FROM data q WHERE q.region = d.region) ORDER BY d.id`,
    ordered: true,
  }),
  (rng) => ({
    sql: `SELECT d.id AS id FROM data d WHERE (SELECT COUNT(*) FROM data q WHERE q.region = d.region AND q.amount > ?) >= ? ORDER BY d.id`,
    params: [Math.floor(rng() * 300) / 4, Math.floor(rng() * 12)],
    ordered: true,
  }),
  (rng) => ({
    sql: `SELECT d.id AS id FROM data d WHERE d.amount IN (SELECT q.amount FROM data q WHERE q.region = d.region AND q.active = ${rng() < 0.5 ? "TRUE" : "FALSE"}) ORDER BY d.id`,
    ordered: true,
  }),
  (rng) => ({
    sql: `SELECT d.id AS id FROM data d WHERE EXISTS (SELECT q.id FROM data q WHERE q.region = d.region AND q.label = d.label AND q.amount > ?) ORDER BY d.id`,
    params: [Math.floor(rng() * 300) / 4],
    ordered: true,
  }),
  () => ({
    sql: `SELECT id, label || '-' || COALESCE(region, 'none') AS tag FROM data ORDER BY id`,
    ordered: true,
  }),
  (rng) => ({
    sql: `SELECT id, UPPER(label) AS u, LOWER(label) AS l, LENGTH(label) AS n, SUBSTR(label, ?, 2) AS mid, TRIM(label) AS t, ABS(amount - ?) AS dist FROM data ORDER BY id`,
    params: [1 + Math.floor(rng() * 3), Math.floor(rng() * 200) / 4],
    ordered: true,
  }),
  (rng) => ({
    sql: `SELECT d.id AS id, m.rank AS r FROM data d CROSS JOIN dims m WHERE d.id <= ? ORDER BY id, r`,
    params: [10 + Math.floor(rng() * 30)],
    ordered: true,
  }),
  () => ({
    sql: `SELECT region, amount, id FROM data ORDER BY 2 DESC, 3`,
    ordered: true,
  }),
  () => ({
    sql: `SELECT id, CAST(amount AS INTEGER) AS whole, CAST(id AS TEXT) AS label, CAST('42.5' AS REAL) AS parsed FROM data ORDER BY id`,
    ordered: true,
    // PostgreSQL and DuckDB round float-to-integer casts; Minnow truncates, matching SQLite.
    skip: ["pglite", "duckdb"],
  }),
  (rng) => ({
    sql: `SELECT "id", "data"."amount" AS "amt" FROM "data" WHERE "amount" >= ? ORDER BY "id"`,
    params: [Math.floor(rng() * 300) / 4],
    ordered: true,
  }),
  () => ({
    sql: `SELECT id, region FROM data ORDER BY region, id`,
    ordered: true,
    // Default NULL ordering: Minnow matches SQLite (NULLs first ascending); PostgreSQL and
    // DuckDB default to NULLs last. Explicit NULLS FIRST/LAST agrees on all engines.
    skip: ["pglite", "duckdb"],
  }),
  (rng) => ({
    sql:
      rng() < 0.5
        ? `SELECT id, region FROM data ORDER BY region NULLS LAST, id`
        : `SELECT id, region FROM data ORDER BY region DESC NULLS FIRST, id`,
    ordered: true,
  }),
  () => ({
    sql: `SELECT d.id AS id, d.region AS dr, m.rank AS r FROM data d FULL JOIN dims m ON m.region = d.region`,
    ordered: false,
  }),
  () => ({
    sql: `SELECT id, LAG(amount) OVER (PARTITION BY region ORDER BY id) AS prev, LEAD(amount, 2, -1.0) OVER (ORDER BY id) AS nxt FROM data`,
    ordered: false,
  }),
  (rng) => ({
    sql: `SELECT id, SUM(amount) OVER (ORDER BY id ROWS BETWEEN ${String(1 + Math.floor(rng() * 4))} PRECEDING AND CURRENT ROW) AS windowed, MIN(amount) OVER (ORDER BY id ROWS BETWEEN 1 FOLLOWING AND 3 FOLLOWING) AS ahead FROM data`,
    ordered: false,
  }),
  () => ({
    sql: `SELECT id, COUNT(*) OVER (PARTITION BY region ORDER BY amount RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS peers FROM data`,
    ordered: false,
  }),
  () => ({
    sql: `SELECT region, SUM(DISTINCT amount) AS s FROM data GROUP BY region`,
    ordered: false,
  }),
  () => ({ sql: `SELECT AVG(DISTINCT amount) AS a FROM data`, ordered: false }),
  () => ({
    sql: `SELECT 1 + 1 AS two, UPPER('minnow') AS name, NULLIF(2, 2) AS n`,
    ordered: false,
  }),
  (rng) => ({
    sql: `SELECT id FROM data ORDER BY id LIMIT ? OFFSET ?`,
    params: [5 + Math.floor(rng() * 20), Math.floor(rng() * 20)],
    ordered: true,
  }),
  (rng) => ({
    sql: `SELECT id, id % ? AS m FROM data WHERE region IS DISTINCT FROM ? ORDER BY id`,
    params: [2 + Math.floor(rng() * 5), pick(rng, someRegions)],
    ordered: true,
  }),
  () => ({
    sql: `SELECT id FROM data WHERE region IS NOT DISTINCT FROM NULL ORDER BY id`,
    ordered: true,
  }),
  (rng) => ({
    sql: `SELECT id, NULLIF(region, 'west') AS n, FLOOR(amount) AS f, CEIL(amount) AS c, MOD(id, ?) AS m, POWER(2, id % 8) AS p, SQRT(id) AS s FROM data ORDER BY id`,
    params: [2 + Math.floor(rng() * 4)],
    ordered: true,
  }),
  (rng) => ({
    sql: `SELECT id, REPLACE(label, 'a', 'o') AS r, LTRIM(RTRIM('  ' || label || '  ')) AS t, INSTR(label, ?) AS i FROM data ORDER BY id`,
    params: [pick(rng, ["a", "o", "lt", "zz"] as const)],
    ordered: true,
    // PostgreSQL spells INSTR as strpos.
    skip: ["pglite"],
  }),
  (rng) => ({
    sql: `SELECT region, COUNT(*) FILTER (WHERE amount > ?) AS big, SUM(amount) FILTER (WHERE active = TRUE) AS active_total FROM data GROUP BY region`,
    params: [Math.floor(rng() * 300) / 4],
    ordered: false,
  }),
  () => ({
    sql: `SELECT id, FIRST_VALUE(amount) OVER (PARTITION BY region ORDER BY id) AS first_seen, LAST_VALUE(amount) OVER (PARTITION BY region ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS final FROM data`,
    ordered: false,
  }),
  (rng) => ({
    sql: `SELECT id, NTILE(${String(2 + Math.floor(rng() * 5))}) OVER (ORDER BY id) AS bucket FROM data`,
    ordered: false,
  }),
  () => ({
    sql: `SELECT id, PERCENT_RANK() OVER (PARTITION BY region ORDER BY amount) AS pr, CUME_DIST() OVER (PARTITION BY region ORDER BY amount) AS cd FROM data`,
    ordered: false,
  }),
  () => ({
    sql: `SELECT d.id AS id, (SELECT AVG(q.amount) FROM data q WHERE q.region = d.region) AS regional FROM data d ORDER BY d.id`,
    ordered: true,
  }),
  (rng) => ({
    sql: `SELECT id, SUM(amount) FILTER (WHERE label LIKE '${pick(rng, patterns)}') OVER (PARTITION BY region ORDER BY id) AS running FROM data`,
    ordered: false,
  }),
  () => ({
    sql: `SELECT id FROM data WHERE active IS TRUE ORDER BY id`,
    ordered: true,
  }),
  () => ({
    sql: `SELECT id FROM data WHERE active IS NOT TRUE ORDER BY id LIMIT 25`,
    ordered: true,
  }),
  (rng) => ({
    // Exercises the dictionary-level LIKE fast paths: prefix, suffix, containment, equality.
    sql: `SELECT COUNT(*) AS n FROM data WHERE label LIKE '${pick(rng, ["e%", "%o", "%lt%", "echo", "_o%"] as const)}'`,
    ordered: false,
  }),
  () => ({
    sql: `SELECT COUNT(*) AS n FROM data WHERE label LIKE 'a!%%' ESCAPE '!'`,
    ordered: false,
  }),
  () => ({
    sql: `SELECT v.column1 AS n, v.column2 AS tag FROM (VALUES (1, 'one'), (2, 'two'), (3, 'three')) v ORDER BY n`,
    ordered: true,
    // DuckDB names bare VALUES columns col0..colN; Minnow follows PostgreSQL/SQLite.
    skip: ["duckdb"],
  }),
  () => ({
    sql: `SELECT v.n AS n, v.tag AS tag FROM (VALUES (1, 'one'), (2, 'two')) AS v(n, tag) ORDER BY n`,
    ordered: true,
    // SQLite has no derived-table column alias lists; PostgreSQL and DuckDB check this one.
    skip: ["sqlite"],
  }),
  (rng) => ({
    sql: `SELECT d.id AS id, x.column2 AS tag FROM data d JOIN (VALUES ('west', 'W'), ('east', 'E')) x ON x.column1 = d.region WHERE d.id <= ? ORDER BY id`,
    params: [20 + Math.floor(rng() * 40)],
    ordered: true,
    // DuckDB names bare VALUES columns col0..colN; Minnow follows PostgreSQL/SQLite.
    skip: ["duckdb"],
  }),
  // The next three exercise features SQLite lacks; PGlite and DuckDB are the oracles.
  (rng) => ({
    sql: `SELECT id FROM data WHERE amount > ALL (SELECT rank + ? FROM dims) ORDER BY id`,
    params: [Math.floor(rng() * 200) / 4],
    ordered: true,
    skip: ["sqlite"],
  }),
  () => ({
    sql: `SELECT region FROM data INTERSECT ALL SELECT region FROM dims`,
    ordered: false,
    skip: ["sqlite"],
  }),
  () => ({
    sql: `SELECT region, SUM(amount) AS total FROM data WHERE region IS NOT NULL GROUP BY ROLLUP(region)`,
    ordered: false,
    skip: ["sqlite"],
  }),
];

function buildCorpus(): Case[] {
  const rng = mulberry32(0xc0ffee);
  const corpus: Case[] = [];
  for (let round = 0; round < 12; round += 1) {
    for (const template of templates) corpus.push(template(rng));
  }
  return corpus;
}

// --- Comparison ---------------------------------------------------------------------------------

function normalize(value: unknown): unknown {
  if (value === true) return 1;
  if (value === false) return 0;
  if (value instanceof Date) return value.toISOString();
  if (typeof value === "bigint") return normalize(Number(value));
  if (typeof value === "number") {
    if (Object.is(value, -0)) return 0;
    return Number(value.toFixed(9));
  }
  // DuckDB returns timestamp values as {micros} wrappers.
  if (typeof value === "object" && value !== null && "micros" in value) {
    return new Date(Number((value as { micros: bigint }).micros) / 1000).toISOString();
  }
  return value;
}

function rowKey(row: Record<string, unknown>): string {
  return JSON.stringify(
    Object.keys(row)
      .sort()
      .map((key) => [key, normalize(row[key])]),
  );
}

function resultKeys(rows: ReadonlyArray<Record<string, unknown>>, ordered: boolean): string[] {
  const keys = rows.map(rowKey);
  return ordered ? keys : [...keys].sort();
}

function diffSummary(label: string, left: string[], right: string[]): string {
  const firstDiff = left.findIndex((key, index) => key !== right[index]);
  const at = firstDiff === -1 ? Math.min(left.length, right.length) : firstDiff;
  return [
    `${label}: ${String(left.length)} vs ${String(right.length)} rows`,
    `  first difference at row ${String(at)}:`,
    `    minnow: ${left[at] ?? "(missing)"}`,
    `    oracle: ${right[at] ?? "(missing)"}`,
  ].join("\n");
}

function sqliteParams(params: QueryValue[] | undefined): Array<string | number | null> {
  return (params ?? []).map((value) => {
    if (value === null) return null;
    if (value === true) return 1;
    if (value === false) return 0;
    if (value instanceof Date) return value.toISOString();
    return value;
  });
}

// --- Oracles ------------------------------------------------------------------------------------

interface Oracle {
  readonly name: OracleName;
  execute(testCase: Case): Promise<Array<Record<string, unknown>>>;
  close(): Promise<void> | void;
}

function sqliteOracle(): Oracle {
  const database = sqliteFixture();
  return {
    name: "sqlite",
    execute: (testCase) =>
      Promise.resolve(
        database.prepare(testCase.sql).all(...sqliteParams(testCase.params)) as Array<
          Record<string, unknown>
        >,
      ),
    close: () => {
      database.close();
    },
  };
}

/** Rewrites `?` placeholders to PostgreSQL's `$n`; corpus strings never contain `?`. */
function positionalToNumbered(sql: string): string {
  let index = 0;
  return sql.replace(/\?/g, () => `$${String((index += 1))}`);
}

async function pgliteOracle(): Promise<Oracle> {
  const { PGlite } = await import("@electric-sql/pglite");
  const database = await PGlite.create();
  await database.exec(
    `CREATE TABLE data ("id" INTEGER, "region" TEXT, "amount" DOUBLE PRECISION, "active" BOOLEAN, "joined" TIMESTAMPTZ, "label" TEXT)`,
  );
  await database.exec(`CREATE TABLE dims ("region" TEXT, "label" TEXT, "rank" DOUBLE PRECISION)`);
  for (const row of fixture) {
    await database.query(`INSERT INTO data VALUES ($1, $2, $3, $4, $5, $6)`, [
      row.id,
      row.region,
      row.amount,
      row.active,
      row.joined,
      row.label,
    ]);
  }
  for (const dim of dims) {
    await database.query(`INSERT INTO dims VALUES ($1, $2, $3)`, [dim.region, dim.label, dim.rank]);
  }
  // int8 and numeric arrive as numbers, so counts and numeric-typed math compare directly.
  const parsers = { 20: (value: string) => Number(value), 1700: (value: string) => Number(value) };
  return {
    name: "pglite",
    execute: async (testCase) => {
      const result = await database.query(
        positionalToNumbered(testCase.sql),
        (testCase.params ?? []) as unknown[],
        { parsers },
      );
      return result.rows as Array<Record<string, unknown>>;
    },
    close: () => database.close(),
  };
}

async function duckdbOracle(): Promise<Oracle> {
  const { DuckDBInstance } = await import("@duckdb/node-api");
  const instance = await DuckDBInstance.create(":memory:");
  const connection = await instance.connect();
  await connection.run(
    `CREATE TABLE data (id INTEGER, region TEXT, amount DOUBLE, active BOOLEAN, joined TIMESTAMPTZ, label TEXT)`,
  );
  await connection.run(`CREATE TABLE dims (region TEXT, label TEXT, rank DOUBLE)`);
  const literal = (value: QueryValue): string => {
    if (value === null) return "NULL";
    if (value instanceof Date) return `'${value.toISOString()}'`;
    if (typeof value === "string") return `'${value.replaceAll("'", "''")}'`;
    if (typeof value === "boolean") return value ? "TRUE" : "FALSE";
    return String(value);
  };
  const dataRows = fixture
    .map(
      (row) =>
        `(${[row.id, row.region, row.amount, row.active, row.joined, row.label].map(literal).join(", ")})`,
    )
    .join(", ");
  await connection.run(`INSERT INTO data VALUES ${dataRows}`);
  const dimRows = dims
    .map((dim) => `(${[dim.region, dim.label, dim.rank].map(literal).join(", ")})`)
    .join(", ");
  await connection.run(`INSERT INTO dims VALUES ${dimRows}`);
  return {
    name: "duckdb",
    execute: async (testCase) => {
      const params = (testCase.params ?? []).map((value) =>
        value instanceof Date ? value.toISOString() : value,
      );
      const reader = await connection.runAndReadAll(testCase.sql, params);
      return reader.getRowObjects();
    },
    close: () => {
      connection.closeSync();
    },
  };
}

// --- The harness --------------------------------------------------------------------------------

describe("SQL conformance against SQLite, PGlite, and DuckDB", () => {
  it("agrees on the generated corpus across all execution paths and oracles", async () => {
    const corpus = buildCorpus();
    const database = await minnowFixture();
    const oracles: Oracle[] = [sqliteOracle(), await pgliteOracle(), await duckdbOracle()];
    const failures: string[] = [];
    try {
      for (const [index, testCase] of corpus.entries()) {
        const caseLabel = `#${String(index)} ${testCase.sql} :: ${JSON.stringify(testCase.params ?? [])}`;
        let vectorized: QueryResult;
        let rowExecutor: QueryResult;
        try {
          vectorized = await database.query(
            testCase.sql,
            testCase.params === undefined ? {} : { params: testCase.params },
          );
          rowExecutor = executeRowQuery(
            bindPlanParameters(compileQuery(testCase.sql), testCase.params),
            rowTables,
          );
        } catch (error) {
          failures.push(`${caseLabel}\n  minnow threw: ${String(error)}`);
          continue;
        }
        const vectorKeys = resultKeys(vectorized.rows, testCase.ordered);
        const rowKeys = resultKeys(rowExecutor.rows, testCase.ordered);
        if (vectorKeys.join("\n") !== rowKeys.join("\n")) {
          failures.push(
            `${caseLabel}\n${diffSummary("vectorized vs row executor", vectorKeys, rowKeys)}`,
          );
        }
        for (const oracle of oracles) {
          if (testCase.skip?.includes(oracle.name)) continue;
          let oracleRows: Array<Record<string, unknown>>;
          try {
            oracleRows = await oracle.execute(testCase);
          } catch (error) {
            failures.push(`${caseLabel}\n  ${oracle.name} threw: ${String(error)}`);
            continue;
          }
          const oracleKeys = resultKeys(oracleRows, testCase.ordered);
          if (vectorKeys.join("\n") !== oracleKeys.join("\n")) {
            failures.push(
              `${caseLabel}\n${diffSummary(`minnow vs ${oracle.name}`, vectorKeys, oracleKeys)}`,
            );
          }
        }
      }
    } finally {
      for (const oracle of oracles) await oracle.close();
    }
    expect(corpus.length).toBeGreaterThan(300);
    if (failures.length > 0) {
      expect.fail(
        `${String(failures.length)} of ${String(corpus.length)} conformance cases diverged:\n\n` +
          failures.slice(0, 10).join("\n\n"),
      );
    }
  }, 240_000);
});
