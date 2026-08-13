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

interface Case {
  sql: string;
  params?: QueryValue[];
  /** Compare row order exactly; requires an ORDER BY ending in a unique key. */
  ordered: boolean;
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
  if (typeof value === "number") {
    if (Object.is(value, -0)) return 0;
    return Number(value.toFixed(9));
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

// --- The harness --------------------------------------------------------------------------------

describe("SQL conformance against SQLite", () => {
  it("agrees on the generated corpus across all execution paths", async () => {
    const corpus = buildCorpus();
    const database = await minnowFixture();
    const oracle = sqliteFixture();
    const failures: string[] = [];
    try {
      for (const [index, testCase] of corpus.entries()) {
        const caseLabel = `#${String(index)} ${testCase.sql} :: ${JSON.stringify(testCase.params ?? [])}`;
        let vectorized: QueryResult;
        let rowExecutor: QueryResult;
        let oracleRows: Array<Record<string, unknown>>;
        try {
          vectorized = await database.query(
            testCase.sql,
            testCase.params === undefined ? {} : { params: testCase.params },
          );
          rowExecutor = executeRowQuery(
            bindPlanParameters(compileQuery(testCase.sql), testCase.params),
            rowTables,
          );
          oracleRows = oracle.prepare(testCase.sql).all(...sqliteParams(testCase.params));
        } catch (error) {
          failures.push(`${caseLabel}\n  threw: ${String(error)}`);
          continue;
        }
        const vectorKeys = resultKeys(vectorized.rows, testCase.ordered);
        const rowKeys = resultKeys(rowExecutor.rows, testCase.ordered);
        const oracleKeys = resultKeys(oracleRows, testCase.ordered);
        if (vectorKeys.join("\n") !== rowKeys.join("\n")) {
          failures.push(
            `${caseLabel}\n${diffSummary("vectorized vs row executor", vectorKeys, rowKeys)}`,
          );
        }
        if (vectorKeys.join("\n") !== oracleKeys.join("\n")) {
          failures.push(`${caseLabel}\n${diffSummary("minnow vs sqlite", vectorKeys, oracleKeys)}`);
        }
      }
    } finally {
      oracle.close();
    }
    expect(corpus.length).toBeGreaterThan(300);
    if (failures.length > 0) {
      expect.fail(
        `${String(failures.length)} of ${String(corpus.length)} conformance cases diverged:\n\n` +
          failures.slice(0, 8).join("\n\n"),
      );
    }
  }, 120_000);
});
