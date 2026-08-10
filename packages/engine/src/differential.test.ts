import { describe, expect, it } from "vitest";
import { type DatabaseRow } from "./database.js";
import { compileQuery, executeQuery, executeRowQuery, type QueryRow } from "./query.js";

/**
 * Seeded differential fuzzing: the columnar executor must agree with the row reference on a
 * randomized query corpus over randomized data, comparing results as column-equal multisets so
 * ordering ties cannot flake. Failures print the seed and SQL for exact reproduction.
 */

function mulberry32(seed: number): () => number {
  let state = seed >>> 0;
  return () => {
    state = (state + 0x6d2b79f5) >>> 0;
    let mixed = state;
    mixed = Math.imul(mixed ^ (mixed >>> 15), mixed | 1);
    mixed ^= mixed + Math.imul(mixed ^ (mixed >>> 7), mixed | 61);
    return ((mixed ^ (mixed >>> 14)) >>> 0) / 4294967296;
  };
}

interface Fuzzer {
  random(): number;
  pick<T>(values: readonly T[]): T;
  int(maximum: number): number;
}

function createFuzzer(seed: number): Fuzzer {
  const random = mulberry32(seed);
  return {
    random,
    pick: (values) => {
      const value = values[Math.floor(random() * values.length)];
      if (value === undefined) throw new Error("Empty pick");
      return value;
    },
    int: (maximum) => Math.floor(random() * maximum),
  };
}

const REGIONS = ["west", "east", "north", "south", null];
const LABELS = ["a", "b", "c", null];

function generateRows(fuzzer: Fuzzer, count: number): DatabaseRow[] {
  return Array.from({ length: count }, () => ({
    region: fuzzer.pick(REGIONS),
    label: fuzzer.pick(LABELS),
    amount: fuzzer.random() < 0.1 ? null : fuzzer.int(20) - 5,
    ratio: fuzzer.random() < 0.1 ? null : Math.round(fuzzer.random() * 100) / 10,
    joined: fuzzer.random() < 0.15 ? null : new Date(Date.UTC(2026, 0, 1 + fuzzer.int(60))),
  }));
}

function generateDims(fuzzer: Fuzzer): DatabaseRow[] {
  return REGIONS.filter((region) => region !== null)
    .filter(() => fuzzer.random() < 0.85)
    .map((region) => ({ region, weight: fuzzer.int(9) + 1 }));
}

const NUMERIC_COLUMNS = ["amount", "ratio"];
const ALL_COLUMNS = ["region", "label", "amount", "ratio", "joined"];
const COMPARISONS = ["=", "!=", ">", ">=", "<", "<="];

function generatePredicate(fuzzer: Fuzzer, qualifier: string): string {
  const kind = fuzzer.int(6);
  if (kind === 4) {
    const column = fuzzer.pick(ALL_COLUMNS);
    return `${qualifier}${column} IS ${fuzzer.random() < 0.5 ? "NOT " : ""}NULL`;
  }
  if (kind === 5) {
    const column = fuzzer.pick(NUMERIC_COLUMNS);
    const low = fuzzer.int(10) - 3;
    return `${qualifier}${column} BETWEEN ${String(low)} AND ${String(low + fuzzer.int(8))}`;
  }
  if (kind === 0) {
    const column = fuzzer.pick(NUMERIC_COLUMNS);
    return `${qualifier}${column} ${fuzzer.pick(COMPARISONS)} ${String(fuzzer.int(16) - 4)}`;
  }
  if (kind === 1) {
    return `${qualifier}region ${fuzzer.pick(["=", "!="])} '${fuzzer.pick(["west", "east", "nowhere"])}'`;
  }
  if (kind === 2) {
    const values = ["'west'", "'east'", "'north'", "NULL"]
      .filter(() => fuzzer.random() < 0.6)
      .join(", ");
    return `${qualifier}region ${fuzzer.pick(["IN", "NOT IN"])} (${values.length > 0 ? values : "'west'"})`;
  }
  return `${qualifier}joined ${fuzzer.pick([">=", "<"])} DATE '2026-01-${String(fuzzer.int(28) + 1).padStart(2, "0")}'`;
}

function generateSql(fuzzer: Fuzzer): string {
  const family = fuzzer.int(11);
  if (family === 10) {
    const grouped = fuzzer.random() < 0.5;
    return grouped
      ? `SELECT region, COUNT(DISTINCT ${fuzzer.pick(ALL_COLUMNS)}) AS distinct_values FROM rows${
          fuzzer.random() < 0.5 ? ` WHERE ${generatePredicate(fuzzer, "")}` : ""
        } GROUP BY region`
      : `SELECT COUNT(DISTINCT ${fuzzer.pick(ALL_COLUMNS)}) AS distinct_values FROM rows${
          fuzzer.random() < 0.5 ? ` WHERE ${generatePredicate(fuzzer, "")}` : ""
        }`;
  }
  const where = () =>
    fuzzer.random() < 0.7
      ? ` WHERE ${Array.from({ length: fuzzer.int(2) + 1 }, () => generatePredicate(fuzzer, "")).join(" AND ")}`
      : "";
  if (family === 0) {
    const columns = ALL_COLUMNS.filter(() => fuzzer.random() < 0.7);
    if (columns.length === 0) columns.push("region");
    return `SELECT ${columns.join(", ")} FROM rows${where()}`;
  }
  if (family === 1) {
    return `SELECT region, amount * 2 + 1 AS scaled, ROUND(ratio, 1) AS rounded FROM rows${where()} ORDER BY region, scaled, rounded LIMIT ${String(fuzzer.int(20) + 1)}`;
  }
  if (family === 2) {
    return `SELECT region, COUNT(*) AS count, SUM(amount) AS total, AVG(ratio) AS mean, MIN(joined) AS first, MAX(label) AS top FROM rows${where()} GROUP BY region`;
  }
  if (family === 3) {
    const having = fuzzer.pick([
      "COUNT(*) > 1",
      "SUM(amount) >= 0",
      "MIN(amount) > -3 AND COUNT(*) >= 1",
    ]);
    return `SELECT region, label, COUNT(*) AS count FROM rows${where()} GROUP BY region, label HAVING ${having}`;
  }
  if (family === 4) {
    return `SELECT DISTINCT region, label FROM rows${where()}`;
  }
  if (family === 5) {
    return `SELECT r.region, r.amount, d.weight FROM rows r ${fuzzer.pick(["JOIN", "LEFT JOIN"])} dims d ON d.region = r.region${
      fuzzer.random() < 0.5 ? ` WHERE ${generatePredicate(fuzzer, "r.")}` : ""
    }`;
  }
  if (family === 6) {
    return `WITH filtered AS (SELECT region, amount FROM rows${where()}) SELECT region, COUNT(*) AS count, SUM(amount) AS total FROM filtered GROUP BY region`;
  }
  if (family === 7) {
    return `SELECT t.region, t.total FROM (SELECT region, SUM(amount) AS total FROM rows GROUP BY region) t${
      fuzzer.random() < 0.5 ? " WHERE t.total >= 0" : ""
    }`;
  }
  if (family === 8) {
    return `SELECT region, amount FROM rows WHERE amount > ${String(fuzzer.int(6) - 2)} UNION${
      fuzzer.random() < 0.5 ? " ALL" : ""
    } SELECT region, amount FROM rows WHERE ratio > ${String(fuzzer.int(5))}`;
  }
  return `SELECT region, amount, ${fuzzer.pick(["ROW_NUMBER()", "RANK()", "DENSE_RANK()"])} OVER (PARTITION BY region ORDER BY amount ${fuzzer.pick(["ASC", "DESC"])}) AS position FROM rows${where()}`;
}

function canonicalRows(rows: readonly QueryRow[], columns: readonly string[]): string[] {
  return rows
    .map((row) =>
      JSON.stringify(
        columns.map((column) => {
          const value = row[column] ?? null;
          if (value === null) return null;
          if (value instanceof Date) return `date:${String(value.getTime())}`;
          if (typeof value === "number") {
            return Object.is(value, -0) ? "-0" : `number:${String(value)}`;
          }
          return `${typeof value}:${String(value)}`;
        }),
      ),
    )
    .sort();
}

describe("differential executor fuzzing", () => {
  for (const seed of [11, 47, 101, 211, 401]) {
    it(`agrees across 120 randomized queries for seed ${String(seed)}`, () => {
      const fuzzer = createFuzzer(seed);
      const tables = new Map([
        ["rows", generateRows(fuzzer, 120)],
        ["dims", generateDims(fuzzer)],
      ]);
      for (let index = 0; index < 120; index += 1) {
        const sql = generateSql(fuzzer);
        let plan;
        let rawPlan;
        try {
          plan = compileQuery(sql);
          rawPlan = compileQuery(sql, { optimize: false });
        } catch (error) {
          throw new Error(`seed ${String(seed)} #${String(index)} failed to compile: ${sql}`, {
            cause: error,
          });
        }
        // The optimized columnar execution must match the raw-plan row reference, so every
        // deterministic rewrite is inside the differential net.
        const columnar = executeQuery(plan, tables);
        const reference = executeRowQuery(rawPlan, tables);
        expect(columnar.columns, `seed ${String(seed)} #${String(index)}: ${sql}`).toEqual(
          reference.columns,
        );
        expect(
          canonicalRows(columnar.rows, columnar.columns),
          `seed ${String(seed)} #${String(index)}: ${sql}`,
        ).toEqual(canonicalRows(reference.rows, reference.columns));
      }
    });
  }
});
