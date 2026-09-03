/**
 * Differential SQL conformance harness. Every query here executes through the full
 * MinnowDatabase pipeline, through the row executor, and through SQLite and PGlite. The two
 * Minnow paths must agree exactly; Minnow and the independent engines must agree after
 * normalizing representation differences (booleans, dates, float rounding).
 *
 * Three layers, because each catches what the others cannot:
 *
 * 1. Seeded templates — hand-written shapes with randomized parameters, run over many rounds.
 * 2. Combination cases — generated from the axes a feature varies along (every aggregate ×
 *    DISTINCT × grouping × position; every window × grouped input × nesting; every join kind ×
 *    ON shape). Layer 1 tests what someone thought to write down, and every gap found so far was
 *    a *combination* nobody wrote down, which is what this layer exists to stop.
 * 3. The shipped feature matrix — every read-only feature the engine publicly claims, executed
 *    against both oracles. A claim nothing checks is how a wrong answer ships, so the
 *    classification test makes coverage mandatory: a supported feature is diffed here, owned by
 *    the mutation harness, or named in `matrixSkips` with the reason an oracle cannot judge it.
 *
 * Forms where the engines' documented semantics differ are skipped per oracle rather than
 * dropped: ROUND (half-away-from-zero vs half-even),
 * LIKE case-insensitivity (disabled via PRAGMA case_sensitive_like), and Minnow extensions
 * (MATCH/BM25). Both PostgreSQL sessions run in UTC, since Minnow reads every datetime as an
 * instant in UTC and a zone difference would be a difference in the question.
 */
import { DatabaseSync } from "node:sqlite";
import { describe, expect, it } from "vitest";
import rawPostgresProfile from "../../postgres-feature-profile.json";
import rawMatrix from "../../sql-feature-matrix.json";
import { MemoryBlockStore } from "../storage/index.js";
import { mulberry32, seedsFor } from "../testing/seeds.js";
import { positionalToNumbered } from "../testing/oracle.js";
import { MinnowDatabase, type DatabaseRow } from "./database.js";
import {
  annotatePlanIntegerDivision,
  bindPlanParameters,
  compileQuery,
  extendGroupByWithKeyDependents,
  foldIdentifierCase,
  executeRowQuery,
  transparentProjectionSource,
  type QueryResult,
  type QueryValue,
} from "./query.js";

// --- Deterministic fixture ----------------------------------------------------------------------

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

function buildFixture(seed: number): FixtureRow[] {
  const rng = mulberry32(seed);
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

const dims = [
  { region: "west", label: "West Coast", rank: 1 },
  { region: "east", label: "East Coast", rank: 2 },
  { region: "north", label: "North", rank: 3 },
  { region: "central", label: "Central", rank: 4 },
];

async function minnowFixture(fixture: readonly FixtureRow[]): Promise<MinnowDatabase> {
  const database = new MinnowDatabase(new MemoryBlockStore(), { rowsPerBlock: 32 });
  await database.createTable({
    name: "data",
    uniqueKey: "id",
    columns: [
      // INTEGER, as both oracles declare it: `id / 2` is integer division on all three.
      { name: "id", type: "number", integer: true },
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

/**
 * The row executor has no catalog, so it receives what catalog binding would have added: the
 * integer-division marks that `data.id` (an INTEGER on every engine) implies.
 */
function catalogBoundPlan(sql: string, params: QueryValue[] | undefined, optimize = true) {
  let plan = compileQuery(sql, optimize ? {} : { optimize: false });
  plan = foldIdentifierCase(plan, catalogColumnNames);
  annotatePlanIntegerDivision(plan, (table) => (table === "data" ? new Set(["id"]) : undefined));
  plan = extendGroupByWithKeyDependents(
    plan,
    (table) => (table === "data" ? ["id"] : undefined),
    (table) => catalogColumnNames.get(table),
  );
  return bindPlanParameters(plan, params);
}

const catalogColumnNames = new Map<string, readonly string[]>([
  ["data", ["id", "region", "amount", "active", "joined", "label"]],
  ["dims", ["region", "label", "rank"]],
]);

function rowTablesFor(fixture: readonly FixtureRow[]): Map<string, DatabaseRow[]> {
  return new Map<string, DatabaseRow[]>([
    ["data", fixture as unknown as DatabaseRow[]],
    ["dims", dims],
  ]);
}

function sqliteFixture(fixture: readonly FixtureRow[]): DatabaseSync {
  const database = new DatabaseSync(":memory:");
  database.exec("PRAGMA case_sensitive_like = ON");
  database.exec(
    `CREATE TABLE data ("id" INTEGER PRIMARY KEY, "region" TEXT, "amount" REAL, "active" INTEGER, "joined" TEXT, "label" TEXT)`,
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

type OracleName = "sqlite" | "pglite";

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
  // A disjunction of equalities on one column is normalized into an IN list. The rewrite is
  // only sound if it matches the oracle on a nullable column, where every non-matching row
  // evaluates to unknown rather than false.
  (rng) => ({
    sql: `SELECT id, region FROM data WHERE region = ? OR region = ? OR region = ? ORDER BY id`,
    params: [pick(rng, someRegions), pick(rng, someRegions), pick(rng, someRegions)],
    ordered: true,
  }),
  (rng) => ({
    sql: `SELECT id, amount FROM data WHERE amount = ? OR amount = ? ORDER BY id`,
    params: [Math.floor(rng() * 400) / 4, Math.floor(rng() * 400) / 4],
    ordered: true,
  }),
  // A NULL member makes the whole list unknown for non-matching rows; IN and OR must agree.
  (rng) => ({
    sql: `SELECT id, region FROM data WHERE region = ? OR region = NULL ORDER BY id`,
    params: [pick(rng, someRegions)],
    ordered: true,
  }),
  // Reversed operands, and a negated disjunction: NOT over the rewritten list must still
  // propagate unknown the way the original OR tree did.
  (rng) => ({
    sql: `SELECT id, region FROM data WHERE NOT (? = region OR ? = region) ORDER BY id`,
    params: [pick(rng, someRegions), pick(rng, someRegions)],
    ordered: true,
  }),
  // Mixed disjunctions must keep their original shape; these pin that the rewrite declines
  // rather than mis-firing across columns or past a non-equality branch.
  (rng) => ({
    sql: `SELECT id FROM data WHERE region = ? OR amount = ? ORDER BY id`,
    params: [pick(rng, someRegions), Math.floor(rng() * 400) / 4],
    ordered: true,
  }),
  (rng) => ({
    sql: `SELECT id FROM data WHERE amount = ? OR amount > ? ORDER BY id`,
    params: [Math.floor(rng() * 400) / 4, Math.floor(rng() * 400) / 4],
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
  // Conjunctive ON clauses. The equality in each becomes the hash key and the rest is a filter,
  // which is only sound for an inner join — so the same shapes run as LEFT JOINs too, where the
  // extra conjunct decides null-extension instead and the answers must still match.
  (rng) => ({
    sql: `SELECT d.id AS id, m.label AS place FROM data d JOIN dims m ON d.region = m.region AND m.rank > ? AND d.amount > 4 ORDER BY d.id`,
    params: [Math.floor(rng() * 3)],
    ordered: true,
  }),
  () => ({
    sql: `SELECT d.id AS id, m.label AS place FROM data d JOIN dims m ON m.region = d.region AND m.rank = d.id % 4 ORDER BY d.id`,
    ordered: true,
  }),
  (rng) => ({
    sql: `SELECT d.id AS id, m.label AS place FROM data d LEFT JOIN dims m ON d.region = m.region AND m.rank > ? ORDER BY d.id, place`,
    params: [Math.floor(rng() * 3)],
    ordered: true,
  }),
  () => ({
    sql: `SELECT d.id AS id, m.label AS place FROM data d LEFT JOIN dims m ON d.region = m.region AND d.amount > 4 ORDER BY d.id, place`,
    ordered: true,
  }),
  // A self-join whose ON pairs rows of one table: the market-basket shape, where losing the
  // equality means every row against every row.
  (rng) => ({
    sql: `SELECT a.region AS region, COUNT(*) AS pairs FROM data a JOIN data b ON b.region = a.region AND b.id > a.id AND b.amount >= ? GROUP BY a.region`,
    params: [Math.floor(rng() * 200) / 4],
    ordered: false,
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
    // PostgreSQL rounds float-to-integer casts, and so does Minnow; SQLite truncates.
    skip: ["sqlite"],
  }),
  (rng) => ({
    sql: `SELECT "id", "data"."amount" AS "amt" FROM "data" WHERE "amount" >= ? ORDER BY "id"`,
    params: [Math.floor(rng() * 300) / 4],
    ordered: true,
  }),
  () => ({
    sql: `SELECT id, region FROM data ORDER BY region, id`,
    ordered: true,
    // Default NULL ordering follows PostgreSQL; SQLite needs an explicit placement to agree.
    skip: ["sqlite"],
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
  // FULL JOIN ORDER BY once resolved qualified references against the UNION ALL desugar's
  // compound output, where the branch table aliases no longer exist ("Unknown table alias").
  () => ({
    sql: `SELECT d.id AS id, m.rank AS r FROM data d FULL JOIN dims m ON m.region = d.region ORDER BY d.id NULLS LAST, m.rank NULLS LAST`,
    ordered: true,
  }),
  () => ({
    sql: `SELECT d.id AS id, m.rank AS r FROM data d FULL JOIN dims m ON m.region = d.region ORDER BY d.id IS NULL, d.id NULLS LAST, m.rank NULLS LAST`,
    ordered: true,
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
  // Windows over a grouped block: SQL runs them after GROUP BY and HAVING, so they rank groups
  // and read aggregates. Every ORDER BY inside OVER(...) carries a unique tiebreak, or the rank
  // an engine hands to tied rows is its own business and the diff would be meaningless.
  () => ({
    sql: `SELECT region, label, SUM(amount) AS total, ROW_NUMBER() OVER (PARTITION BY region ORDER BY SUM(amount) DESC, label) AS rank, RANK() OVER (PARTITION BY region ORDER BY COUNT(*) DESC, label) AS by_rows FROM data GROUP BY region, label`,
    ordered: false,
  }),
  () => ({
    sql: `SELECT region, label, COUNT(*) AS n, SUM(SUM(amount)) OVER (PARTITION BY region) AS region_total FROM data GROUP BY region, label HAVING COUNT(*) > 1`,
    ordered: false,
  }),
  () => ({
    sql: `SELECT SUM(amount) AS total, ROW_NUMBER() OVER (ORDER BY SUM(amount)) AS only_row FROM data`,
    ordered: false,
  }),
  // Grouped by label rather than region: the tiebreak inside OVER(...) has to be non-nullable,
  // or the engines' opposite NULL ordering shifts every rank below it and the diff is noise.
  () => ({
    sql: `SELECT label, COUNT(DISTINCT region) AS regions, DENSE_RANK() OVER (ORDER BY COUNT(DISTINCT region) DESC, label) AS variety FROM data GROUP BY label`,
    ordered: false,
  }),
  () => ({
    sql: `SELECT region, SUM(DISTINCT amount) AS s FROM data GROUP BY region`,
    ordered: false,
  }),
  () => ({ sql: `SELECT AVG(DISTINCT amount) AS a FROM data`, ordered: false }),
  // Several DISTINCT aggregates in one select, beside plain ones: each keeps its own set of
  // values, so nothing here may fold into a shared deduplication.
  () => ({
    sql: `SELECT region, COUNT(DISTINCT amount) AS amounts, COUNT(DISTINCT label) AS labels, COUNT(DISTINCT active) AS flags, COUNT(*) AS rows_seen, SUM(amount) AS total, MIN(DISTINCT amount) AS lowest FROM data GROUP BY region`,
    ordered: false,
  }),
  // Nested inside arithmetic, which the top-level-only rule used to refuse outright.
  () => ({
    sql: `SELECT region, SUM(amount) / COUNT(DISTINCT label) AS per_label, COUNT(DISTINCT amount) + COUNT(DISTINCT label) AS spread FROM data GROUP BY region`,
    ordered: false,
    // Integer division: SQLite would floor SUM/COUNT where Minnow and PostgreSQL do not.
    skip: ["sqlite"],
  }),
  (rng) => ({
    sql: `SELECT region, COUNT(DISTINCT amount) AS amounts FROM data GROUP BY region HAVING COUNT(DISTINCT amount) > ? AND COUNT(*) > 1`,
    params: [Math.floor(rng() * 3)],
    ordered: false,
  }),
  () => ({
    sql: `SELECT COUNT(DISTINCT joined) AS days, COUNT(DISTINCT region) AS regions, MAX(DISTINCT amount) AS highest FROM data WHERE active`,
    ordered: false,
  }),
  () => ({
    sql: `SELECT COUNT(DISTINCT CASE WHEN amount > 5 THEN region END) AS busy_regions, COUNT(DISTINCT CASE WHEN active THEN label END) AS active_labels FROM data`,
    ordered: false,
  }),
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
  }),
  () => ({
    sql: `SELECT v.n AS n, v.tag AS tag FROM (VALUES (1, 'one'), (2, 'two')) AS v(n, tag) ORDER BY n`,
    ordered: true,
    // SQLite has no derived-table column alias lists; PGlite checks this one.
    skip: ["sqlite"],
  }),
  (rng) => ({
    sql: `SELECT d.id AS id, x.column2 AS tag FROM data d JOIN (VALUES ('west', 'W'), ('east', 'E')) x ON x.column1 = d.region WHERE d.id <= ? ORDER BY id`,
    params: [20 + Math.floor(rng() * 40)],
    ordered: true,
  }),
  // The next three exercise features SQLite lacks; PGlite is the oracle.
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
  // --- Matrix SQL surface, over the same seeded fixture ----------------------------------------
  (rng) => ({
    // E021-06/09/11 and T055 against PostgreSQL, whose spellings are the standard's.
    sql: `SELECT id, SUBSTRING(label FROM ${String(1 + Math.floor(rng() * 4))} FOR 3) AS part, POSITION('a' IN label) AS at, LPAD(label, 9, '.') AS padded, RPAD(label, 9, '.') AS tail, OVERLAY(label PLACING 'ZZ' FROM 2 FOR 2) AS masked, CHAR_LENGTH(label) AS width, OCTET_LENGTH(label) AS bytes FROM data ORDER BY id`,
    ordered: true,
    skip: ["sqlite"],
  }),
  (rng) => ({
    // A start below 1 and a window past the end are where the position-window rule shows. The
    // bounds are written into the text because PostgreSQL cannot infer a placeholder's type in
    // this position.
    sql: `SELECT id, SUBSTRING(label FROM ${String(Math.floor(rng() * 4))} FOR ${String(Math.floor(rng() * 12))}) AS part FROM data ORDER BY id`,
    ordered: true,
    skip: ["sqlite"],
  }),
  () => ({
    sql: `SELECT id, TRIM(LEADING 'a' FROM label) AS lead, TRIM(TRAILING 'o' FROM label) AS trail, TRIM(BOTH 'a' FROM label) AS both FROM data ORDER BY id`,
    ordered: true,
    skip: ["sqlite"],
  }),
  (rng) => ({
    // E051-07: over one source, where the wildcard's output names are the columns' own. With
    // several sources this engine prefixes them by alias, which no oracle does.
    sql: `SELECT d.* FROM data d WHERE d.amount >= ? ORDER BY id`,
    params: [Math.floor(rng() * 300) / 4],
    ordered: true,
  }),
  (rng) => ({
    // F041-07: the comma join.
    sql: `SELECT d.id AS id, m.rank AS r FROM data d, dims m WHERE m.region = d.region AND d.amount >= ? ORDER BY id, r`,
    params: [Math.floor(rng() * 300) / 4],
    ordered: true,
  }),
  () => ({
    sql: `SELECT d.id AS id, m.rank AS r FROM data d JOIN dims m USING (region) ORDER BY id`,
    ordered: true,
  }),
  () => ({
    sql: `SELECT d.id AS id FROM data d NATURAL JOIN dims m ORDER BY id`,
    ordered: true,
  }),
  (rng) => ({
    // F641: row comparisons and row IN, including the NULL region.
    sql: `SELECT id FROM data WHERE (region, amount) > (?, ?) ORDER BY id`,
    params: ["east", Math.floor(rng() * 300) / 4],
    ordered: true,
  }),
  () => ({
    sql: `SELECT id FROM data WHERE (region, active) IN (('west', TRUE), ('east', FALSE)) ORDER BY id`,
    ordered: true,
  }),
  () => ({
    sql: `SELECT id FROM data WHERE (region, label) IS NOT NULL ORDER BY id`,
    ordered: true,
    // SQLite rejects a row value in a null predicate.
    skip: ["sqlite"],
  }),
  (rng) => ({
    // T618 and T620: a named window reused by several functions.
    sql: `SELECT id, NTH_VALUE(amount, ${String(1 + Math.floor(rng() * 3))}) OVER w AS nth, FIRST_VALUE(amount) OVER w AS first, SUM(amount) OVER w AS running FROM data WINDOW w AS (PARTITION BY region ORDER BY id)`,
    ordered: false,
  }),
  (rng) => ({
    // T612: GROUPS frames and the exclusions, both of which SQLite and PostgreSQL implement.
    sql: `SELECT id, COUNT(*) OVER (ORDER BY region GROUPS BETWEEN ${String(1 + Math.floor(rng() * 2))} PRECEDING AND CURRENT ROW) AS peers, SUM(amount) OVER (ORDER BY region RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE GROUP) AS others, COUNT(*) OVER (ORDER BY region RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE TIES) AS untied FROM data WHERE region IS NOT NULL`,
    ordered: false,
  }),
  () => ({
    // T433 with several sets, the shape that caught a real bug: a literal-only select list.
    sql: `SELECT region, GROUPING(region) AS aggregated, COUNT(*) AS n FROM data WHERE region IS NOT NULL GROUP BY ROLLUP(region)`,
    ordered: false,
    skip: ["sqlite"],
  }),
  () => ({
    sql: `SELECT VAR_POP(amount) AS vp, VAR_SAMP(amount) AS vs, STDDEV_POP(amount) AS sp, EVERY(amount >= 0) AS all_positive, BOOL_OR(amount > 70) AS any_large FROM data`,
    ordered: false,
    skip: ["sqlite"],
  }),
  () => ({
    sql: `SELECT region, STDDEV_SAMP(amount) AS spread FROM data WHERE region IS NOT NULL GROUP BY region`,
    ordered: false,
    skip: ["sqlite"],
  }),
  (rng) => ({
    // F866 over a column with ties, ordered so the tie set is unambiguous.
    sql: `SELECT region FROM data WHERE region IS NOT NULL ORDER BY region DESC FETCH FIRST ${String(1 + Math.floor(rng() * 3))} ROWS WITH TIES`,
    ordered: true,
    skip: ["sqlite"],
  }),
  () => ({
    // E071-06 and T122: a set operation inside a derived table, under a nested WITH.
    sql: `SELECT s.id AS id FROM (WITH pool AS (SELECT id FROM data WHERE amount > 50 UNION SELECT id FROM data WHERE region = 'west') SELECT id FROM pool) s ORDER BY id`,
    ordered: true,
  }),
];

// --- Combination coverage -----------------------------------------------------------------------
//
// The templates above are written one at a time, which means they cover what somebody thought to
// write down. Everything that has gone wrong here was a *combination* nobody wrote down: a second
// DISTINCT aggregate in the same select, a window over a grouped block, a window inside an
// expression, an ON clause carrying more than its equality. So these are generated from the axes
// each feature varies along instead, and every crossing becomes a case. They are built once
// rather than per round, since the SQL does not vary with the seed.

const aggregates = ["COUNT", "SUM", "AVG", "MIN", "MAX"] as const;
const groupings = [
  { keys: "", select: "", by: "" },
  { keys: "region", select: "region, ", by: " GROUP BY region" },
  { keys: "region, label", select: "region, label, ", by: " GROUP BY region, label" },
];

/** Every aggregate, DISTINCT and not, in every position a select list can put one. */
function distinctCases(): Case[] {
  const cases: Case[] = [];
  for (const aggregate of aggregates) {
    for (const grouping of groupings) {
      const plain = `${aggregate}(amount)`;
      const distinct = `${aggregate}(DISTINCT amount)`;
      // Beside its own plain form, beside a second DISTINCT over another column, and beside
      // COUNT(*) — the shapes the old one-DISTINCT-per-select rule made impossible.
      cases.push({
        sql: `SELECT ${grouping.select}${distinct} AS d, ${plain} AS p, COUNT(DISTINCT label) AS labels, COUNT(*) AS n FROM data${grouping.by}`,
        ordered: false,
      });
      // Inside an expression rather than as the whole select item.
      cases.push({
        sql: `SELECT ${grouping.select}${distinct} + COUNT(DISTINCT label) AS combined, ${plain} - ${distinct} AS gap FROM data${grouping.by}`,
        ordered: false,
      });
      if (grouping.by !== "") {
        cases.push({
          sql: `SELECT ${grouping.select}${distinct} AS d FROM data${grouping.by} HAVING ${distinct} > 1 AND COUNT(*) > 2`,
          ordered: false,
        });
      }
    }
  }
  return cases;
}

/**
 * Every window function over grouped and ungrouped input, as the whole select item and inside an
 * expression. Each ORDER BY inside OVER(...) ends in a unique, non-null tiebreak: without one the
 * rank handed to tied rows is the engine's own business and a diff would mean nothing.
 */
function windowCases(): Case[] {
  const ranks = ["ROW_NUMBER()", "RANK()", "DENSE_RANK()", "PERCENT_RANK()", "CUME_DIST()"];
  const cases: Case[] = [];
  for (const rank of ranks) {
    cases.push({
      sql: `SELECT id, ${rank} OVER (ORDER BY amount, id) AS r FROM data`,
      ordered: false,
    });
    cases.push({
      sql: `SELECT region, SUM(amount) AS total, ${rank} OVER (ORDER BY SUM(amount), region) AS r FROM data WHERE region IS NOT NULL GROUP BY region`,
      ordered: false,
    });
  }
  for (const aggregate of aggregates) {
    cases.push({
      sql: `SELECT id, ${aggregate}(amount) OVER (PARTITION BY region ORDER BY amount, id) AS w FROM data`,
      ordered: false,
    });
    cases.push({
      sql: `SELECT region, ${aggregate}(amount) AS agg, ${aggregate}(${aggregate}(amount)) OVER (PARTITION BY region) AS nested FROM data WHERE region IS NOT NULL GROUP BY region, label`,
      ordered: false,
    });
  }
  // Windows inside expressions: arithmetic around one, two of them combined, one inside a CASE.
  cases.push(
    {
      sql: `SELECT id, amount - LAG(amount) OVER (ORDER BY amount, id) AS change FROM data`,
      ordered: false,
    },
    {
      sql: `SELECT id, ROW_NUMBER() OVER (ORDER BY amount, id) + LEAD(id, 1, 0) OVER (ORDER BY amount, id) AS mixed FROM data`,
      ordered: false,
    },
    {
      sql: `SELECT id, CASE WHEN ROW_NUMBER() OVER (ORDER BY amount, id) > 75 THEN 'late' ELSE 'early' END AS half FROM data`,
      ordered: false,
    },
    {
      sql: `SELECT id, 100.0 * amount / SUM(amount) OVER () AS share FROM data`,
      ordered: false,
    },
    {
      sql: `SELECT region, SUM(amount) AS total, 100.0 * SUM(amount) / SUM(SUM(amount)) OVER () AS pct FROM data WHERE region IS NOT NULL GROUP BY region`,
      ordered: false,
    },
  );
  return cases;
}

/** Both join kinds against every shape an ON clause takes, since only one of them keeps a key. */
function joinCases(): Case[] {
  const conditions = [
    "d.region = m.region",
    "d.region = m.region AND m.rank > 1",
    "d.region = m.region AND d.amount > 4",
    "d.region = m.region AND m.rank > 1 AND d.amount > 4",
    "d.region = m.region AND m.label > d.label",
    "m.region = d.region AND m.rank = d.id % 4",
    "d.amount > m.rank",
  ];
  return ["JOIN", "LEFT JOIN"].flatMap((kind) =>
    conditions.map((on) => ({
      sql: `SELECT d.id AS id, m.label AS place FROM data d ${kind} dims m ON ${on} WHERE d.id <= 60 ORDER BY d.id, place`,
      ordered: true,
    })),
  );
}

/** Datetime literals and calendar arithmetic, which only PostgreSQL spells the same way. */
function datetimeCases(): Case[] {
  const cases: Case[] = [
    { sql: `SELECT id FROM data WHERE joined >= DATE '2026-01-01' ORDER BY id`, ordered: true },
    {
      sql: `SELECT id FROM data WHERE joined >= TIMESTAMP '2026-01-02 03:04:05' ORDER BY id`,
      ordered: true,
    },
    {
      sql: `SELECT id FROM data WHERE joined BETWEEN TIMESTAMP '2025-12-01' AND TIMESTAMP '2026-02-01 12:00:00' ORDER BY id`,
      ordered: true,
    },
  ];
  for (const interval of ["1 month", "3 months", "1 year", "2 days", "36 hours", "90 minutes"]) {
    cases.push({
      sql: `SELECT id, joined + INTERVAL '${interval}' AS later, joined - INTERVAL '${interval}' AS earlier FROM data WHERE joined IS NOT NULL ORDER BY id`,
      ordered: true,
    });
  }
  // Month arithmetic has to clamp: 31 January plus a month is the end of February, not March 3.
  cases.push({
    sql: `SELECT TIMESTAMP '2026-01-31' + INTERVAL '1 month' AS clamped, TIMESTAMP '2024-02-29' + INTERVAL '1 year' AS leap FROM data LIMIT 1`,
    ordered: false,
  });
  // A zoneless string cast to TIMESTAMP is UTC, like the literal; `new Date("2026-01-02 03:04:05")`
  // would read the host's zone and answer differently on two machines.
  cases.push({
    sql: `SELECT CAST('2026-01-02 03:04:05' AS TIMESTAMP) AS spaced, CAST('2026-01-02' AS TIMESTAMP) AS midnight, CAST('2026-01-02T03:04:05.250Z' AS TIMESTAMP) AS iso FROM data LIMIT 1`,
    ordered: false,
  });
  // CURRENT_TIMESTAMP resolves once per statement, including when the ORDER BY hides a column
  // the select list does not carry: that desugar wraps the block, and the executor runs the
  // inner one. Every fixture date is in the past, so the answer is the fixture ordered by id.
  cases.push({
    sql: `SELECT id, region FROM data WHERE joined < CURRENT_TIMESTAMP ORDER BY amount, id`,
    ordered: true,
  });
  // SQLite has neither the literals nor INTERVAL; PostgreSQL checks every one of these.
  return cases.map((testCase) => ({ ...testCase, skip: ["sqlite"] as const }));
}

/** A CTE naming its own columns, including the recursive form where the step reads them back. */
function cteCases(): Case[] {
  return [
    {
      sql: `WITH totals(place, total) AS (SELECT region, SUM(amount) FROM data GROUP BY region) SELECT place, total FROM totals`,
      ordered: false,
    },
    {
      sql: `WITH RECURSIVE counter(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM counter WHERE n < 10) SELECT n FROM counter ORDER BY n`,
      ordered: true,
    },
    {
      sql: `WITH ranked(id, place, position) AS (SELECT id, region, ROW_NUMBER() OVER (ORDER BY amount, id) FROM data) SELECT position, place FROM ranked WHERE position <= 10 ORDER BY position`,
      ordered: true,
    },
  ];
}

/**
 * Crosses every spelling of a derived block's row window with every outer shape that reads the
 * block. An outer filter is pushed inside a plain derived table or CTE, which is wrong below a
 * window: only the literal LIMIT form was guarded, so under OFFSET, `ROWS`, and every placeholder
 * form the filter ran before the window. SQLite has no FETCH FIRST and requires LIMIT before
 * OFFSET, so those windows compare against PostgreSQL alone.
 */
function derivedWindowCases(): Case[] {
  const windows: ReadonlyArray<{ sql: string; params: QueryValue[]; skip?: OracleName[] }> = [
    { sql: "LIMIT 6", params: [] },
    { sql: "LIMIT ?", params: [6] },
    { sql: "LIMIT ? OFFSET ?", params: [6, 3] },
    { sql: "OFFSET 4", params: [], skip: ["sqlite"] },
    { sql: "OFFSET 4 ROWS", params: [], skip: ["sqlite"] },
    { sql: "OFFSET ?", params: [4], skip: ["sqlite"] },
    { sql: "FETCH FIRST ? ROWS ONLY", params: [5], skip: ["sqlite"] },
  ];
  const outers: ReadonlyArray<{
    sql: (source: string) => string;
    params: QueryValue[];
    ordered: boolean;
  }> = [
    {
      sql: (source) => `SELECT d.id FROM ${source} WHERE d.region = 'west'`,
      params: [],
      ordered: false,
    },
    {
      sql: (source) =>
        `SELECT d.id, o.label FROM ${source} JOIN data o ON o.id = d.id WHERE d.region = 'west'`,
      params: [],
      ordered: false,
    },
    {
      sql: (source) =>
        `SELECT d.id FROM ${source} WHERE d.region = 'west' ORDER BY d.id DESC LIMIT 2`,
      params: [],
      ordered: true,
    },
    // The outer window itself: a literal LIMIT over a placeholder OFFSET must not merge inward.
    {
      sql: (source) => `SELECT d.id FROM ${source} ORDER BY d.id LIMIT 2 OFFSET ?`,
      params: [1],
      ordered: true,
    },
  ];
  const cases: Case[] = [];
  for (const window of windows) {
    const block = `SELECT id, region FROM data ORDER BY id ${window.sql}`;
    const sources = [
      { prefix: "", source: `(${block}) d` },
      { prefix: `WITH d AS (${block}) `, source: "d" },
    ];
    for (const { prefix, source } of sources) {
      for (const outer of outers) {
        const params = [...window.params, ...outer.params];
        cases.push({
          sql: `${prefix}${outer.sql(source)}`,
          ordered: outer.ordered,
          ...(params.length === 0 ? {} : { params }),
          ...(window.skip === undefined ? {} : { skip: window.skip }),
        });
      }
    }
  }
  return cases;
}

/**
 * Crosses both wildcard spellings with bare, qualified, and quoted ORDER BY references. These
 * features are independently valid, but expansion used to erase the qualifier from `d.*` before
 * ORDER BY resolution. Keeping the crossing generated prevents either feature's isolated example
 * from claiming support while their ordinary composition is broken.
 */
function wildcardOrderCases(): Case[] {
  const projections = [
    { select: "*", from: "data", qualifiedOrder: "data.id" },
    { select: "data.*", from: "data", qualifiedOrder: "data.id" },
    { select: '"data".*', from: '"data"', qualifiedOrder: '"data"."id"' },
    { select: "d.*", from: "data d", qualifiedOrder: "d.id" },
    { select: '"d".*', from: 'data AS "d"', qualifiedOrder: '"d"."id"' },
  ] as const;
  return projections.flatMap(({ select, from, qualifiedOrder }) =>
    ["id", qualifiedOrder].map((order) => ({
      sql: `SELECT ${select} FROM ${from} ORDER BY ${order} DESC LIMIT 17`,
      ordered: true,
    })),
  );
}

/**
 * Crosses wildcard width with every lowering that consumes a concrete select shape. These were
 * once separate late executor special cases, so each feature worked alone while ordinary
 * compositions failed or produced an internally misaligned result.
 */
function wildcardShapeCases(): Case[] {
  return [
    {
      sql: `SELECT * FROM data ORDER BY amount * 2 DESC, id LIMIT 19`,
      ordered: true,
    },
    {
      sql: `SELECT data.* FROM data ORDER BY data.amount * 2 DESC, data.id LIMIT 19`,
      ordered: true,
    },
    { sql: `SELECT data.* FROM data ORDER BY 3 DESC, 1 LIMIT 19`, ordered: true },
    {
      sql: `SELECT DISTINCT data.* FROM data ORDER BY data.id DESC LIMIT 19`,
      ordered: true,
    },
    {
      sql: `SELECT data.*, ROW_NUMBER() OVER (ORDER BY amount, id) AS position FROM data ORDER BY id`,
      ordered: true,
    },
    {
      sql: `SELECT data.*, COUNT(*) OVER () AS total_rows FROM data ORDER BY id`,
      ordered: true,
    },
    {
      sql: `SELECT d.* FROM data d WHERE id <= 40 UNION SELECT e.* FROM data e WHERE id >= 120 ORDER BY 1`,
      ordered: true,
    },
    {
      sql: `SELECT d.* FROM data d WHERE id <= 20 UNION ALL SELECT e.* FROM data e WHERE id >= 140 ORDER BY 1`,
      ordered: true,
    },
    {
      sql: `SELECT d.* FROM data d WHERE id <= 80 INTERSECT SELECT e.* FROM data e WHERE id >= 60 ORDER BY 1`,
      ordered: true,
    },
    {
      sql: `SELECT d.* FROM data d WHERE id <= 80 EXCEPT SELECT e.* FROM data e WHERE id <= 20 ORDER BY 1`,
      ordered: true,
    },
    {
      sql: `WITH copied(ident, place, total, enabled, joined_at, title) AS (SELECT * FROM data) SELECT ident, title FROM copied ORDER BY ident LIMIT 19`,
      ordered: true,
    },
    {
      sql: `SELECT copied.ident, copied.title FROM (SELECT * FROM data) AS copied(ident, place, total, enabled, joined_at, title) ORDER BY copied.ident LIMIT 19`,
      ordered: true,
      skip: ["sqlite"],
    },
  ];
}

/**
 * A trailing ORDER BY, LIMIT, or OFFSET belongs to the whole set operation and names the first
 * member's output columns. The parser used to let the last member swallow the tail and resolve
 * it against its own select list, so an alias only the first member declared, a third member,
 * or an aggregate member all failed, and the two-member same-name case hid the whole class.
 */
function setOperationCases(): Case[] {
  return [
    { sql: `SELECT 1 AS a UNION SELECT 2 ORDER BY a DESC`, ordered: true },
    {
      sql: `SELECT id AS key FROM data WHERE id < 3 UNION SELECT id FROM data WHERE id < 5 ORDER BY key`,
      ordered: true,
    },
    {
      sql: `SELECT id FROM data WHERE id < 3 UNION SELECT id FROM data WHERE id < 5 UNION SELECT 9 ORDER BY id`,
      ordered: true,
    },
    {
      sql: `SELECT id FROM data WHERE id < 3 UNION ALL SELECT id FROM data WHERE id < 5 UNION ALL SELECT 9 ORDER BY id DESC LIMIT 4`,
      ordered: true,
    },
    {
      sql: `SELECT id FROM data WHERE id < 5 INTERSECT SELECT id FROM data WHERE id > 2 UNION SELECT 100 ORDER BY id`,
      ordered: true,
    },
    {
      sql: `SELECT id FROM data WHERE id < 5 EXCEPT SELECT id FROM data WHERE id = 2 EXCEPT SELECT 1 ORDER BY id`,
      ordered: true,
    },
    {
      sql: `SELECT id FROM data WHERE id < 6 INTERSECT SELECT id FROM data WHERE id > 2 ORDER BY id DESC`,
      ordered: true,
    },
    {
      sql: `SELECT COUNT(*) AS n FROM data UNION ALL SELECT COUNT(*) FROM dims ORDER BY n`,
      ordered: true,
    },
    {
      sql: `SELECT region, COUNT(*) AS n FROM data GROUP BY region UNION ALL SELECT region, COUNT(*) FROM dims GROUP BY region ORDER BY region NULLS LAST, n`,
      ordered: true,
    },
    {
      sql: `SELECT id, 'data' AS kind FROM data WHERE id < 3 UNION ALL SELECT rank, 'dim' FROM dims ORDER BY kind, id`,
      ordered: true,
    },
    {
      sql: `SELECT id FROM data WHERE id < 3 UNION SELECT id FROM data WHERE id < 5 ORDER BY id LIMIT 2 OFFSET 1`,
      ordered: true,
    },
    { sql: `VALUES (2), (1), (3) ORDER BY 1 DESC`, ordered: true, skip: ["sqlite"] },
    {
      sql: `SELECT id FROM data WHERE id < 3 UNION ALL VALUES (7), (8) ORDER BY 1 DESC`,
      ordered: true,
      skip: ["sqlite"],
    },
  ];
}

/**
 * Untyped string constants beside typed columns read in the column's type, as PostgreSQL types
 * an unknown-typed literal by its context. SQLite compares text against REAL and ISO text
 * lexically instead, so only the forms it happens to agree on include it.
 */
function coercionCases(): Case[] {
  return [
    { sql: `SELECT id FROM data WHERE joined >= '2026-01-01' ORDER BY id`, ordered: true },
    {
      sql: `SELECT id FROM data WHERE joined = '2026-01-02T03:04:05.000Z' ORDER BY id`,
      ordered: true,
    },
    {
      sql: `SELECT id FROM data WHERE joined BETWEEN '2025-12-01' AND '2026-02-01 12:00:00' ORDER BY id`,
      ordered: true,
      skip: ["sqlite"],
    },
    {
      sql: `SELECT id FROM data WHERE joined IN ('2026-01-02T03:04:05.000Z', '2025-12-30T00:00:00.000Z') ORDER BY id`,
      ordered: true,
    },
    {
      sql: `SELECT id FROM data WHERE joined > ? ORDER BY id`,
      params: ["2026-02-01"],
      ordered: true,
      skip: ["sqlite"],
    },
    // The untyped-constant reading also applies beside a datetime that is not a column: an
    // aggregate, a scalar subquery, the statement clock, or a COALESCE fallback.
    {
      sql: `SELECT region, COUNT(*) AS n FROM data GROUP BY region HAVING MAX(joined) > '2026-01-01' ORDER BY region NULLS LAST`,
      ordered: true,
    },
    {
      sql: `SELECT id FROM data WHERE joined IS NOT NULL AND (SELECT MAX(joined) FROM data) > '2026-01-01' ORDER BY id`,
      ordered: true,
    },
    {
      sql: `SELECT id, MAX(joined) > '2026-01-01' AS recent FROM data GROUP BY id ORDER BY id`,
      ordered: true,
      skip: ["sqlite"],
    },
    {
      sql: `SELECT id, CURRENT_DATE >= '2020-01-01' AS later FROM data ORDER BY id`,
      ordered: true,
    },
    {
      sql: `SELECT id FROM data WHERE COALESCE(joined, '2000-01-01') < '2026-01-01' ORDER BY id`,
      ordered: true,
      skip: ["sqlite"],
    },
    { sql: `SELECT id FROM data WHERE amount = '10' ORDER BY id`, ordered: true },
    { sql: `SELECT id FROM data WHERE amount > '90.5' ORDER BY id`, ordered: true },
    { sql: `SELECT id FROM data WHERE id IN ('1', '2', '3') ORDER BY id`, ordered: true },
    { sql: `SELECT id FROM data WHERE amount >= ? ORDER BY id`, params: ["95"], ordered: true },
    { sql: `SELECT id FROM data WHERE active = 't' ORDER BY id`, ordered: true, skip: ["sqlite"] },
    {
      sql: `SELECT id FROM data WHERE CAST(joined AS DATE) >= '2026-01-02' ORDER BY id`,
      ordered: true,
      skip: ["sqlite"],
    },
    {
      sql: `SELECT id, amount::INTEGER AS whole, -amount::INTEGER * 2 AS scaled, 'r-' || id || '/' || amount AS tag FROM data ORDER BY id`,
      ordered: true,
      skip: ["sqlite"],
    },
  ];
}

/** The table-driven PostgreSQL functions and operators, diffed against PGlite (SQLite lacks them). */
function functionCases(): Case[] {
  const cases: Case[] = [
    {
      // date(x) is the DATE cast spelled as a function; POW is POWER's other spelling.
      sql: `SELECT id, CAST(DATE(joined) AS TEXT) AS day, POW(2, id) AS doubled, POWER(id, 2) AS squared FROM data WHERE joined IS NOT NULL ORDER BY id`,
      ordered: true,
    },
    {
      sql: `SELECT id, CONCAT(region, '-', label, '-', amount) AS tag, CONCAT_WS('/', region, label) AS joined, LEFT(label, 2) AS l2, RIGHT(label, -2) AS rn2, REVERSE(label) AS rev, INITCAP(label || ' ' || label) AS cap, SPLIT_PART(label, 'l', 1) AS part, STRPOS(label, 'l') AS at, STARTS_WITH(label, 'a') AS starts, TRANSLATE(label, 'ao', 'AO') AS tr, ASCII(label) AS code, BTRIM(label, 'a') AS trimmed FROM data ORDER BY id`,
      ordered: true,
    },
    {
      sql: `SELECT id, MD5(label) AS digest, FORMAT('%s:%s', label, amount) AS formatted, REGEXP_REPLACE(label, '[aeiou]', '_', 'g') AS vowels, REGEXP_REPLACE(label, 'l+', 'L') AS first FROM data ORDER BY id`,
      ordered: true,
    },
    {
      sql: `SELECT id FROM data WHERE label ~ '^[a-c]' AND label !~* 'O$' ORDER BY id`,
      ordered: true,
    },
    { sql: `SELECT id, (label ~ 'a') || '!' AS tagged FROM data ORDER BY id`, ordered: true },
    {
      sql: `SELECT id FROM data WHERE region ~* '^W' OR label ~ ('l' || 'ta') ORDER BY id`,
      ordered: true,
    },
    {
      sql: `SELECT id, amount ^ 2 AS squared, 2 ^ 3 ^ 2 AS left_assoc, -2 ^ 2 AS negated, CAST(ROUND(CAST(EXP(1) AS NUMERIC), 6) AS DOUBLE PRECISION) AS e, CAST(ROUND(CAST(LN(amount + 1) AS NUMERIC), 6) AS DOUBLE PRECISION) AS ln, CAST(ROUND(CAST(LOG(amount + 1) AS NUMERIC), 6) AS DOUBLE PRECISION) AS lg, CAST(ROUND(LOG(2, CAST(amount + 1 AS NUMERIC)), 6) AS DOUBLE PRECISION) AS lg2, SIGN(amount - 50) AS sign, TRUNC(CAST(amount AS NUMERIC) / 7, 2) AS trunc, CBRT(27) AS cbrt, DIV(CAST(amount AS NUMERIC), 7) AS quotient, WIDTH_BUCKET(amount, 0, 100, 4) AS bucket FROM data ORDER BY id`,
      ordered: true,
    },
    {
      sql: `SELECT id, TO_CHAR(joined, 'YYYY-MM-DD HH24:MI:SS.MS') AS iso, TO_CHAR(joined, 'FMDay, DD FMMonth YYYY') AS spoken, TO_CHAR(joined, 'Dy Mon DD HH12:MI AM') AS clock, TO_CHAR(joined, 'IW DDD Q D J') AS calendar, TO_CHAR(joined, 'YY "week" IW') AS quoted FROM data WHERE joined IS NOT NULL ORDER BY id`,
      ordered: true,
    },
    {
      sql: `SELECT id, TO_CHAR(amount, '999.99') AS padded, TO_CHAR(amount, 'FM999.00') AS trimmed, TO_CHAR(-amount, '9999.9') AS negative, TO_CHAR(amount, '00009') AS zeros, TO_CHAR(amount * 1000, '9,999,999.99') AS grouped, TO_CHAR(amount, 'S999.99') AS signed, TO_CHAR(amount, '999.99MI') AS trailing, TO_CHAR(amount / 400, '9.99') AS fraction FROM data ORDER BY id`,
      ordered: true,
    },
    {
      sql: `SELECT TO_TIMESTAMP('2026-01-02 03:04:05', 'YYYY-MM-DD HH24:MI:SS') AS at, TO_TIMESTAMP('02/01/2026 03:04 PM', 'DD/MM/YYYY HH12:MI AM') AS pm, TO_TIMESTAMP(1767322800) AS epoch, MAKE_TIMESTAMP(2026, 1, 2, 3, 4, 5.5) AS made FROM data LIMIT 1`,
      ordered: false,
    },
    {
      sql: `SELECT id, DATE_PART('year', joined) AS y, DATE_PART('month', joined) AS m, EXTRACT(DOY FROM joined) AS doy, EXTRACT(ISODOW FROM joined) AS isodow, EXTRACT(ISOYEAR FROM joined) AS isoyear, EXTRACT(DECADE FROM joined) AS decade, EXTRACT(CENTURY FROM joined) AS century, EXTRACT(MILLISECONDS FROM joined) AS ms, EXTRACT(MICROSECONDS FROM joined) AS us, EXTRACT(YEAR FROM DATE '2026-03-04') AS from_date FROM data WHERE joined IS NOT NULL ORDER BY id`,
      ordered: true,
    },
    {
      sql: `SELECT id FROM data WHERE TO_CHAR(joined, 'YYYY-MM') = '2026-01' ORDER BY id`,
      ordered: true,
    },
    {
      sql: `SELECT TO_CHAR(joined, 'YYYY-MM') AS month, COUNT(*) AS n FROM data WHERE joined IS NOT NULL GROUP BY 1 ORDER BY 1`,
      ordered: true,
    },
    // AGE and TO_DATE agree on the value; PostgreSQL renders intervals and dates differently.
    {
      sql: `SELECT id, AGE(TIMESTAMP '2026-06-15 12:00:00', joined) > INTERVAL '3 months' AS quarter_old, TO_DATE('02/01/2026', 'DD/MM/YYYY') < joined AS after FROM data WHERE joined IS NOT NULL ORDER BY id`,
      ordered: true,
    },
  ];
  return cases.map((testCase) => ({ ...testCase, skip: ["sqlite"] as const }));
}

/**
 * Spellings PostgreSQL and SQLite both accept for one meaning — SELECT ALL, a column label
 * without AS, a GROUP BY that qualifies what the select list leaves bare (or the reverse), a
 * parenthesized join group, and LIMIT ALL — each crossed with the positions and neighbours it
 * can have. Every one was a rejection the upstream SQLLogicTest random corpus found, so the
 * crossings keep the fix from being narrower than the corpus.
 */
/**
 * Name resolution PostgreSQL performs from the catalog: unquoted identifiers fold to the unique
 * case-insensitive match when no exact name exists (output names keep the catalog spelling),
 * and a primary key in GROUP BY lets the row's other columns appear ungrouped.
 */
/**
 * PostgreSQL's JSON spellings — json_agg, json_build_object, to_json, row_to_json — and a table
 * alias used as a value, which is the row as a JSON object. These are the shapes Kysely's
 * jsonArrayFrom/jsonObjectFrom and Drizzle's relational queries emit. SQLite has none of them.
 */
function jsonSpellingCases(): Case[] {
  const pg = (sql: string): Case => ({ sql, ordered: true, skip: ["sqlite"] });
  return [
    pg(
      `SELECT region, JSON_AGG(amount ORDER BY amount) AS amounts, JSONB_AGG(label ORDER BY id) AS labels FROM data GROUP BY region ORDER BY region NULLS LAST`,
    ),
    pg(
      `SELECT id, JSON_BUILD_OBJECT('id', id, 'region', region, 'active', active) AS doc, JSONB_BUILD_OBJECT('amount', amount) AS amount_doc FROM data ORDER BY id`,
    ),
    pg(
      `SELECT id, TO_JSON(amount) AS amount_doc, TO_JSONB(region) AS region_doc, TO_JSON(active) AS active_doc FROM data ORDER BY id`,
    ),
    pg(
      `SELECT r.id, ROW_TO_JSON(r) AS row_doc FROM (SELECT id, region, amount FROM data) r ORDER BY r.id`,
    ),
    {
      sql: `SELECT JSON_AGG(r ORDER BY r.id) AS rows_doc FROM (SELECT id, amount FROM data WHERE id <= 3) r`,
      ordered: false,
      skip: ["sqlite"],
    },
    pg(
      `SELECT d.region, (SELECT COALESCE(JSON_AGG(agg), '[]') FROM (SELECT m.label, m.rank FROM dims m WHERE m.region = d.region ORDER BY m.rank) agg) AS dims FROM data d WHERE d.id <= 5 ORDER BY d.id`,
    ),
    pg(
      `SELECT d.id, (SELECT TO_JSON(obj) FROM (SELECT m.label, m.rank FROM dims m WHERE m.region = d.region ORDER BY m.rank LIMIT 1) obj) AS dim FROM data d WHERE d.id <= 5 ORDER BY d.id`,
    ),
    pg(
      `SELECT region, JSON_AGG(JSON_BUILD_OBJECT('id', id, 'amount', amount) ORDER BY id) AS items FROM data WHERE id <= 6 GROUP BY region ORDER BY region NULLS LAST`,
    ),
  ];
}

/** DISTINCT ON: the first row per key in ORDER BY order. SQLite has no such form. */
function distinctOnCases(): Case[] {
  const pg = (sql: string, ordered = true): Case => ({ sql, ordered, skip: ["sqlite"] });
  return [
    pg(
      `SELECT DISTINCT ON (region) region, id, amount FROM data ORDER BY region NULLS LAST, amount DESC, id`,
    ),
    pg(
      `SELECT DISTINCT ON (region, active) region, active, id FROM data ORDER BY region NULLS LAST, active, id`,
    ),
    pg(
      `SELECT DISTINCT ON (region) id, amount * 2 AS doubled FROM data ORDER BY region NULLS LAST, doubled DESC, id`,
    ),
    pg(
      `SELECT DISTINCT ON (d.region) d.region, d.id FROM data d WHERE d.amount > 20 ORDER BY d.region NULLS LAST, d.joined DESC NULLS LAST, d.id LIMIT 3`,
    ),
    pg(
      `SELECT DISTINCT ON (region) region, COUNT(*) AS n FROM data GROUP BY region, active ORDER BY region NULLS LAST, n DESC, active`,
    ),
    pg(`SELECT DISTINCT ON (region) region FROM data ORDER BY region NULLS LAST`),
  ];
}

function resolutionCases(): Case[] {
  return [
    { sql: `SELECT ID, Region, AMOUNT FROM DATA WHERE Id < 4 ORDER BY id`, ordered: true },
    { sql: `SELECT D.id, d.LABEL FROM Data AS D ORDER BY D.ID`, ordered: true },
    {
      sql: `SELECT d.Region, COUNT(*) AS n FROM DATA d GROUP BY D.region ORDER BY d.REGION NULLS LAST`,
      ordered: true,
    },
    {
      sql: `SELECT id, region, label, COUNT(*) AS n FROM data GROUP BY id ORDER BY id`,
      ordered: true,
    },
    {
      sql: `SELECT d.id, d.region, dm.label AS dim, SUM(d.amount) AS total FROM data d LEFT JOIN dims dm ON dm.region = d.region GROUP BY d.id, dm.label ORDER BY d.id`,
      ordered: true,
    },
    {
      sql: `SELECT d.id, d.amount FROM data d GROUP BY d.id HAVING d.amount > 50 ORDER BY d.amount DESC, d.id`,
      ordered: true,
    },
  ];
}

function spellingCases(): Case[] {
  const cases: Case[] = [];
  // SELECT [ALL | DISTINCT] × a label with AS, without AS, quoted either way, and a label after
  // a parenthesized or signed expression.
  for (const prefix of ["", "ALL ", "DISTINCT "]) {
    for (const label of ["AS total", "total", '"total"', 'AS "total"']) {
      cases.push({
        sql: `SELECT ${prefix}label, amount * 2 ${label} FROM data WHERE amount > 80 ORDER BY 1, 2`,
        ordered: true,
      });
    }
    cases.push({
      sql: `SELECT ${prefix}(amount) a, + amount b, - amount c, id n FROM data WHERE id <= 20 ORDER BY n`,
      ordered: true,
    });
    cases.push({
      sql: `SELECT ${prefix}COUNT(ALL amount) n, MIN(ALL label) first_label, MAX(region) last_region FROM data`,
      ordered: false,
    });
  }
  // A grouped column spelled bare on one side and qualified on the other, over a table named
  // by itself, with AS, and with a bare alias; alone, inside arithmetic, both spellings in one
  // expression, under HAVING and DISTINCT, and ordered by the other spelling. NULL regions stay
  // out of the sort keys, since the oracles place them differently.
  for (const { from, q } of [
    { from: "data", q: "data" },
    { from: "data AS d", q: "d" },
    { from: "data d", q: "d" },
  ]) {
    const shapes = [
      { select: "region", by: `${q}.region` },
      { select: `${q}.region`, by: "region" },
      { select: `${q}.region || '-' || region AS pair`, by: "region" },
      { select: `region || '-' || ${q}.region AS pair`, by: `${q}.region` },
      { select: `${q}.amount * 2 + amount AS triple`, by: "amount" },
      { select: `amount * 2 + ${q}.amount AS triple`, by: `${q}.amount` },
    ];
    for (const { select, by } of shapes) {
      cases.push({
        sql: `SELECT ${select}, COUNT(*) AS n, SUM(${q}.amount) AS total FROM ${from} WHERE region IS NOT NULL GROUP BY ${by} ORDER BY 1`,
        ordered: true,
      });
      cases.push({
        sql: `SELECT ${select} FROM ${from} GROUP BY ${by} HAVING COUNT(*) > 1`,
        ordered: false,
      });
    }
    cases.push({
      sql: `SELECT region, ${q}.label, COUNT(*) AS n FROM ${from} WHERE region IS NOT NULL GROUP BY ${q}.region, label ORDER BY region, ${q}.label`,
      ordered: true,
    });
    cases.push({
      sql: `SELECT DISTINCT ${q}.region FROM ${from} GROUP BY region, label`,
      ordered: false,
    });
    cases.push({
      sql: `SELECT region, MAX(amount) AS peak FROM ${from} WHERE region IS NOT NULL GROUP BY ${q}.region ORDER BY ${q}.region`,
      ordered: true,
    });
  }
  // Several sources: a bare name belongs to the one source that has it.
  cases.push({
    sql: "SELECT rank, d.active, COUNT(*) AS n FROM data d JOIN dims m ON m.region = d.region GROUP BY m.rank, active ORDER BY rank, d.active",
    ordered: true,
  });
  cases.push({
    sql: "SELECT m.rank + 1 AS next_rank, active FROM data d JOIN dims m ON m.region = d.region GROUP BY rank, d.active ORDER BY next_rank, active",
    ordered: true,
  });
  // A parenthesized join group as the whole FROM, nested, beside a comma, and as the operand
  // of CROSS JOIN and INNER JOIN.
  for (const from of [
    "( data d CROSS JOIN dims m )",
    "( data AS d CROSS JOIN dims AS m )",
    "( data d JOIN dims m ON m.region = d.region )",
    "( ( data d CROSS JOIN dims m ) )",
    "( data d CROSS JOIN dims m ) CROSS JOIN dims x",
    "dims x CROSS JOIN ( data d CROSS JOIN dims m )",
    "dims x, ( data d JOIN dims m ON m.region = d.region )",
    "dims x JOIN ( data d JOIN dims m ON m.region = d.region ) ON x.rank = m.rank",
  ]) {
    cases.push({
      sql: `SELECT COUNT(*) AS n, SUM(d.amount) AS total FROM ${from}`,
      ordered: false,
    });
    cases.push({
      sql: `SELECT d.id, m.label AS place FROM ${from} WHERE d.id <= 10 ORDER BY d.id, place`,
      ordered: true,
    });
  }
  // LIMIT ALL is PostgreSQL's spelling of no limit, alone, before OFFSET, and on a compound.
  for (const tail of ["LIMIT ALL", "LIMIT ALL OFFSET 3"]) {
    cases.push({
      sql: `SELECT id, amount FROM data WHERE amount > 60 ORDER BY id ${tail}`,
      ordered: true,
      skip: ["sqlite"],
    });
    cases.push({
      sql: `SELECT label FROM data WHERE amount > 90 UNION SELECT label FROM dims ORDER BY 1 ${tail}`,
      ordered: true,
      skip: ["sqlite"],
    });
  }
  return cases;
}

function combinationCases(): Case[] {
  return [
    ...spellingCases(),
    ...coercionCases(),
    ...functionCases(),
    ...distinctCases(),
    ...windowCases(),
    ...joinCases(),
    ...datetimeCases(),
    ...cteCases(),
    ...derivedWindowCases(),
    ...wildcardOrderCases(),
    ...wildcardShapeCases(),
    ...setOperationCases(),
  ];
}

function buildCorpus(seed: number): Case[] {
  const rng = mulberry32(seed);
  const corpus: Case[] = [];
  for (let round = 0; round < 12; round += 1) {
    for (const template of templates) corpus.push(template(rng));
  }
  corpus.push(...combinationCases());
  corpus.push(...divisionCases());
  corpus.push(...resolutionCases());
  corpus.push(...jsonSpellingCases());
  corpus.push(...distinctOnCases());
  return corpus;
}

/**
 * Division follows PostgreSQL's typing: `/` over two integers truncates toward zero, and a
 * float or NUMERIC operand makes it fractional. `id` is INTEGER on every engine, `amount` is a
 * double, and constants type by their spelling, so every operand pairing is diffed here: column
 * and constant, integer aggregate and count, CAST, CASE and COALESCE, a scalar subquery, a
 * derived table and CTE output, a bound parameter, and the WHERE and ORDER BY positions where
 * a truncated quotient selects different rows.
 */
function divisionCases(): Case[] {
  const ordered = (sql: string, params?: QueryValue[]): Case => ({
    sql,
    ordered: true,
    ...(params === undefined ? {} : { params }),
  });
  return [
    ordered(
      `SELECT 7 / 2 AS q, -7 / 2 AS nq, 7 / -2 AS qn, 1 / 3 AS third, 10 / 4 * 4 AS back, 7 / 2 * 1.5 AS mixed FROM data WHERE id = 1 ORDER BY id`,
    ),
    ordered(
      `SELECT id, id / 2 AS half, id / 3 AS third, -id / 2 AS neg, id / 2.0 AS exact, id / 4 * 4 AS floored FROM data ORDER BY id`,
    ),
    ordered(
      `SELECT id, amount / 2 AS half, amount / id AS ratio, id / amount AS inverse FROM data WHERE amount <> 0 ORDER BY id`,
    ),
    {
      // PostgreSQL rounds a float-to-integer cast and so does Minnow; SQLite truncates.
      sql: `SELECT id, CAST(amount AS INTEGER) / 2 AS half, CAST(id AS DOUBLE PRECISION) / 2 AS exact FROM data ORDER BY id`,
      ordered: true,
      skip: ["sqlite"],
    },
    ordered(`SELECT id FROM data WHERE id / 2 = 1 ORDER BY id`),
    ordered(`SELECT id FROM data WHERE id / 3 > 1 AND id / 2 * 2 = id ORDER BY id`),
    ordered(`SELECT id, id / 2 AS bucket FROM data ORDER BY id / 2 DESC, id`),
    ordered(
      `SELECT id / 2 AS bucket, COUNT(*) AS n, SUM(id) / COUNT(*) AS mean, SUM(id) / 2 AS half, MIN(id) / 2 AS low, MAX(id) / 2 AS high FROM data GROUP BY id / 2 ORDER BY bucket`,
    ),
    ordered(
      `SELECT COUNT(*) / 2 AS half_count, SUM(amount) / COUNT(*) AS mean_amount, AVG(id) / 2 AS half_mean FROM data ORDER BY 1`,
    ),
    ordered(
      `SELECT id, CASE WHEN active THEN id ELSE 0 END / 2 AS half, COALESCE(NULLIF(id, 3), 0) / 2 AS coalesced, ABS(-id) / 2 AS absolute FROM data ORDER BY id`,
    ),
    ordered(
      `SELECT id, (SELECT COUNT(*) FROM data) / 2 AS half_count, (SELECT SUM(id) FROM data) / 2 AS half_sum FROM data ORDER BY id`,
    ),
    ordered(`SELECT d.half FROM (SELECT id, id / 2 AS half FROM data) d ORDER BY d.half, d.id`),
    ordered(`SELECT d.total / 2 AS half FROM (SELECT SUM(id) AS total FROM data) d ORDER BY 1`),
    ordered(`SELECT d.id FROM (SELECT id, amount FROM data) d WHERE d.id / 2 = 2 ORDER BY d.id`),
    ordered(
      `WITH halves AS (SELECT id, id / 2 AS half FROM data) SELECT id, half FROM halves WHERE half > 1 ORDER BY id`,
    ),
    // PostgreSQL types a parameter beside an integer as an integer; node:sqlite binds every
    // JavaScript number as REAL, so its `/` is fractional and only PGlite judges these two.
    {
      sql: `SELECT id / ? AS q FROM data ORDER BY id`,
      params: [2],
      ordered: true,
      skip: ["sqlite"],
    },
    {
      sql: `SELECT id FROM data WHERE id / ? = ? ORDER BY id`,
      params: [3, 1],
      ordered: true,
      skip: ["sqlite"],
    },
    ordered(`SELECT id, id / 2 AS half FROM data UNION ALL SELECT 9, 9 / 2 ORDER BY id, half`),
    ordered(
      `SELECT id, id % 3 AS remainder, -id % 3 AS neg_remainder, id / 3 * 3 + id % 3 AS rebuilt FROM data ORDER BY id`,
    ),
  ];
}

// --- Seeds --------------------------------------------------------------------------------------

interface ConformanceRun {
  readonly fixtureSeed: number;
  readonly corpusSeed: number;
}

const DEFAULT_FIXTURE_SEED = 0x5eed;
const DEFAULT_CORPUS_SEED = 0xc0ffee;

/**
 * The runs this suite makes. The checked-in run has always drawn its fixture and its corpus from
 * two different seeds, and keeps both so the questions it asks do not change. Every other seed —
 * a `MINNOW_SEED` override or a regression recorded from a soak — drives the fixture and the
 * corpus together, because that is what the soak run that found it did: a replay that varied
 * only the corpus could miss a failure that needed the data too.
 */
function conformanceRuns(): ConformanceRun[] {
  return seedsFor("sql-conformance", [DEFAULT_CORPUS_SEED]).map((seed) => ({
    fixtureSeed: seed === DEFAULT_CORPUS_SEED ? DEFAULT_FIXTURE_SEED : seed,
    corpusSeed: seed,
  }));
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
  // JSON documents: Minnow returns text, PGlite parses json/jsonb into values, jsonb reorders
  // object keys (shorter first), and json keeps the producer's whitespace. All of those are one
  // document, so an object, an array, or text that parses as one compares in canonical form.
  if (typeof value === "object" && value !== null) return canonicalJson(value);
  if (typeof value === "string" && (value.startsWith("{") || value.startsWith("["))) {
    try {
      return canonicalJson(JSON.parse(value) as unknown);
    } catch {
      return value;
    }
  }
  return value;
}

function canonicalJson(value: unknown): string {
  const sorted = (node: unknown): unknown => {
    if (Array.isArray(node)) return node.map(sorted);
    if (node !== null && typeof node === "object") {
      return Object.fromEntries(
        Object.keys(node)
          .sort()
          .map((key) => [key, sorted((node as Record<string, unknown>)[key])]),
      );
    }
    return typeof node === "number" ? Number(node.toFixed(9)) : node;
  };
  return `json:${JSON.stringify(sorted(value))}`;
}

function rowKey(row: Record<string, unknown>): string {
  return JSON.stringify(
    Object.keys(row)
      .sort()
      .map((key) => [key, normalize(row[key])]),
  );
}

/**
 * Minnow returns exact NUMERIC as lossless text, while the PGlite oracle's `numeric` parser (OID
 * 1700, below) reads it as a number and SQLite has no decimal type at all. Columns the result
 * declares NUMERIC decode the same way before an oracle comparison, so numeric-typed math such
 * as TRUNC(CAST(amount AS NUMERIC) / 7, 2) compares by value; text columns stay text.
 */
function numericDecodedRows(result: QueryResult): Array<Record<string, unknown>> {
  const numericColumns = result.columns.filter(
    (_, index) => result.columnDomains[index]?.kind === "numeric",
  );
  // JSON columns decode the same way: Minnow returns the document text, PGlite parses json and
  // jsonb into values, so a scalar document (`TO_JSON(amount)` is `95`) compares by value.
  const jsonColumns = result.columns.filter(
    (_, index) => result.columnDomains[index]?.kind === "json",
  );
  if (numericColumns.length === 0 && jsonColumns.length === 0) return result.rows;
  return result.rows.map((row) => {
    const decoded: Record<string, unknown> = { ...row };
    for (const name of numericColumns) {
      const value = decoded[name];
      if (typeof value === "string") decoded[name] = Number(value);
    }
    for (const name of jsonColumns) {
      const value = decoded[name];
      if (typeof value === "string") decoded[name] = JSON.parse(value) as unknown;
    }
    return decoded;
  });
}

function resultKeys(rows: ReadonlyArray<Record<string, unknown>>, ordered: boolean): string[] {
  const keys = rows.map(rowKey);
  return ordered ? keys : [...keys].sort();
}

/** Result ordering may live under the projection that hides an ORDER BY-only expression. */
function hasResultOrder(plan: ReturnType<typeof compileQuery>): boolean {
  if (plan.orderBy.length > 0) return true;
  const wrapper = transparentProjectionSource(plan);
  if (wrapper !== undefined) return hasResultOrder(wrapper.inner);
  // Before FULL JOIN's internal wildcard has a schema, the ordinary transparent-wrapper helper
  // cannot prove its pass-through aliases. The marker belongs only to that ORDER BY lowering.
  return plan.base.alias === "(ordered)" && plan.base.derived !== undefined
    ? hasResultOrder(plan.base.derived)
    : false;
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
  execute(testCase: Case): Promise<{
    rows: Array<Record<string, unknown>>;
    columns: string[];
  }>;
  close(): Promise<void> | void;
}

function sqliteOracle(fixture: readonly FixtureRow[]): Oracle {
  const database = sqliteFixture(fixture);
  return {
    name: "sqlite",
    execute: (testCase) => {
      const statement = database.prepare(testCase.sql);
      const rows = statement.all(...sqliteParams(testCase.params)) as Array<
        Record<string, unknown>
      >;
      return Promise.resolve({ rows, columns: statement.columns().map(({ name }) => name) });
    },
    close: () => {
      database.close();
    },
  };
}

async function pgliteOracle(fixture: readonly FixtureRow[]): Promise<Oracle> {
  const { PGlite } = await import("@electric-sql/pglite");
  const database = await PGlite.create();
  // Minnow reads every datetime as an instant in UTC, including a TIMESTAMP literal written
  // without a zone. Left on the host's zone, PostgreSQL reads those literals locally and the
  // two disagree by the offset — a difference in the question, not in the answer.
  await database.exec(`SET TIME ZONE 'UTC'`);
  await database.exec(
    `CREATE TABLE data ("id" INTEGER PRIMARY KEY, "region" TEXT, "amount" DOUBLE PRECISION, "active" BOOLEAN, "joined" TIMESTAMPTZ, "label" TEXT)`,
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
  // int8 and numeric arrive as numbers, so counts and numeric-typed math compare directly, and a
  // timestamp without a zone (what `TIMESTAMP '…' + INTERVAL` returns) is read as UTC rather than
  // in whatever zone the machine running the tests happens to be in.
  const parsers = {
    20: (value: string) => Number(value),
    1700: (value: string) => Number(value),
    1114: (value: string) => new Date(`${value.replace(" ", "T")}Z`),
  };
  return {
    name: "pglite",
    execute: async (testCase) => {
      const result = await database.query(
        positionalToNumbered(testCase.sql),
        (testCase.params ?? []) as unknown[],
        { parsers },
      );
      return {
        rows: result.rows as Array<Record<string, unknown>>,
        columns: result.fields.map(({ name }) => name),
      };
    },
    close: () => database.close(),
  };
}

// --- Feature matrix coverage --------------------------------------------------------------------
//
// The corpus above is written by hand, so it only tests what somebody thought to write down. The
// shipped feature matrix is the other half: it is the list of SQL this engine claims to support,
// one example apiece, and until now nothing checked those claims against another database — a
// feature could be listed, execute, return the wrong answer, and pass. Every read-only claim now
// runs through both oracles, and the classification test below makes that non-optional: a new
// feature is either differentially checked here, checked by the mutation harness, or named in
// `matrixExemptions` with the reason it cannot be.

interface MatrixFeature {
  id: string;
  status: "supported" | "unsupported";
  example: string;
  setup?: string[];
  params?: QueryValue[];
}

const matrixFeatures = (rawMatrix as { features: MatrixFeature[] }).features;

type PostgresClassification =
  "compatible" | "different" | "extension" | "unsupported" | "inapplicable";

interface PostgresOverride {
  id: string;
  classification: PostgresClassification;
  verification?: "acceptance";
}

const postgresProfile = rawPostgresProfile as {
  defaults: {
    supported: PostgresClassification;
    unsupported: PostgresClassification;
  };
  overrides: PostgresOverride[];
};
const postgresOverrides = new Map(postgresProfile.overrides.map((entry) => [entry.id, entry]));

function postgresClassification(feature: MatrixFeature): PostgresClassification {
  const override = postgresOverrides.get(feature.id);
  if (override !== undefined) return override.classification;
  if (feature.status === "unsupported") return postgresProfile.defaults.unsupported;
  return postgresProfile.defaults.supported;
}

/** The fixture the matrix's examples are written against, mirrored from feature-matrix.test.ts. */
const matrixRows = [
  { region: "west", amount: 10, active: true, joined: new Date("2026-01-02T00:00:00.000Z") },
  { region: "west", amount: 6, active: false, joined: new Date("2025-12-30T00:00:00.000Z") },
  { region: "east", amount: 3, active: true, joined: new Date("2026-02-01T00:00:00.000Z") },
  { region: null, amount: 8, active: true, joined: null },
];
const matrixDims = [
  { region: "west", label: "West Coast", amount: 1 },
  { region: "north", label: "North", amount: 2 },
];

async function matrixMinnow(): Promise<MinnowDatabase> {
  const database = new MinnowDatabase(new MemoryBlockStore(), { rowsPerBlock: 8 });
  await database.createTable({
    name: "rows",
    columns: [
      { name: "region", type: "string", nullable: true },
      { name: "amount", type: "number" },
      { name: "active", type: "boolean" },
      { name: "joined", type: "datetime", nullable: true },
    ],
  });
  await database.insertBatch("rows", matrixRows);
  await database.createTable({
    name: "dims",
    columns: [
      { name: "region", type: "string" },
      { name: "label", type: "string" },
      { name: "amount", type: "number" },
    ],
  });
  await database.insertBatch("dims", matrixDims);
  return database;
}

function matrixSqlite(): Oracle {
  const database = new DatabaseSync(":memory:");
  database.exec("PRAGMA case_sensitive_like = ON");
  database.exec(
    `CREATE TABLE "rows" ("region" TEXT, "amount" REAL, "active" INTEGER, "joined" TEXT)`,
  );
  database.exec(`CREATE TABLE dims ("region" TEXT, "label" TEXT, "amount" REAL)`);
  const insert = database.prepare(`INSERT INTO "rows" VALUES (?, ?, ?, ?)`);
  for (const row of matrixRows) {
    insert.run(row.region, row.amount, row.active ? 1 : 0, row.joined?.toISOString() ?? null);
  }
  const insertDim = database.prepare(`INSERT INTO dims VALUES (?, ?, ?)`);
  for (const dim of matrixDims) insertDim.run(dim.region, dim.label, dim.amount);
  return {
    name: "sqlite",
    execute: (testCase) => {
      const statement = database.prepare(numberedToPositional(testCase.sql));
      const rows = statement.all(...sqliteParams(testCase.params)) as Array<
        Record<string, unknown>
      >;
      return Promise.resolve({ rows, columns: statement.columns().map(({ name }) => name) });
    },
    close: () => {
      database.close();
    },
  };
}

/** SQLite reads $1 as a named parameter; the matrix means it positionally, as PostgreSQL does. */
function numberedToPositional(sql: string): string {
  return sql.replace(/\$\d+/g, "?");
}

async function matrixPglite(): Promise<Oracle> {
  const { PGlite } = await import("@electric-sql/pglite");
  const database = await PGlite.create();
  // Every datetime in a Minnow database is an instant read as UTC. Left on the host's zone,
  // PostgreSQL truncates and extracts in local time and the two disagree by the offset.
  await database.exec(`SET TIME ZONE 'UTC'`);
  await database.exec(
    `CREATE TABLE "rows" ("region" TEXT, "amount" DOUBLE PRECISION, "active" BOOLEAN, "joined" TIMESTAMPTZ)`,
  );
  await database.exec(`CREATE TABLE dims ("region" TEXT, "label" TEXT, "amount" DOUBLE PRECISION)`);
  for (const row of matrixRows) {
    await database.query(`INSERT INTO "rows" VALUES ($1, $2, $3, $4)`, [
      row.region,
      row.amount,
      row.active,
      row.joined,
    ]);
  }
  for (const dim of matrixDims) {
    await database.query(`INSERT INTO dims VALUES ($1, $2, $3)`, [
      dim.region,
      dim.label,
      dim.amount,
    ]);
  }
  const parsers = {
    20: (value: string) => Number(value),
    1700: (value: string) => Number(value),
    1114: (value: string) => new Date(`${value.replace(" ", "T")}Z`),
  };
  return {
    name: "pglite",
    execute: async (testCase) => {
      const result = await database.query(
        positionalToNumbered(testCase.sql),
        (testCase.params ?? []) as unknown[],
        { parsers },
      );
      return {
        rows: result.rows as Array<Record<string, unknown>>,
        columns: result.fields.map(({ name }) => name),
      };
    },
    close: () => database.close(),
  };
}

/** Features the mutation harness owns: they change data rather than answer a question. */
function writesData(id: string): boolean {
  return (
    id.startsWith("mutation.") ||
    id.startsWith("ddl.") ||
    id.startsWith("trigger.") ||
    id.startsWith("transaction.")
  );
}

/**
 * Oracles that cannot be asked about a given feature, with the reason. Skipping one oracle still
 * leaves the other checking the claim, so most entries here cost nothing in coverage — SQLite has
 * no ILIKE, PostgreSQL has no two-argument ROUND on a float, and neither has Minnow's full-text
 * extensions. Every entry is a decision someone made and wrote down; the classification test below
 * fails for any supported feature that is neither executed nor listed.
 */
const matrixSkips = new Map<string, { oracles: readonly OracleName[]; reason: string }>([
  // --- PostgreSQL forms SQLite does not spell -----------------------------------------------
  ["function.char-length", { oracles: ["sqlite"], reason: "SQLite spells it LENGTH" }],
  ["limit.all", { oracles: ["sqlite"], reason: "SQLite has no LIMIT ALL" }],
  [
    "function.substring-from-for",
    { oracles: ["sqlite"], reason: "SQLite has no SUBSTRING(x FROM a FOR b) syntax" },
  ],
  [
    "function.trim-specification",
    { oracles: ["sqlite"], reason: "SQLite's TRIM takes no LEADING/TRAILING/BOTH" },
  ],
  [
    "function.trim-multi-character",
    {
      oracles: ["sqlite", "pglite"],
      reason:
        "The standard removes the trim string as a repeated unit; PostgreSQL removes any of its characters, and SQLite has no side keyword",
    },
  ],
  [
    "function.position",
    { oracles: ["sqlite"], reason: "SQLite spells it INSTR, arguments swapped" },
  ],
  ["function.pad", { oracles: ["sqlite"], reason: "SQLite has no LPAD/RPAD" }],
  ["function.overlay", { oracles: ["sqlite"], reason: "SQLite has no OVERLAY" }],
  [
    "from.column-alias-list",
    { oracles: ["sqlite"], reason: "SQLite has no column alias list on a table reference" },
  ],
  [
    "datetime.current-date",
    { oracles: ["sqlite"], reason: "SQLite has no DATE '…' literal to compare against" },
  ],
  [
    "datetime.current-timestamp",
    { oracles: ["sqlite"], reason: "SQLite has no TIMESTAMP '…' literal to compare against" },
  ],
  ["datetime.localtime", { oracles: ["sqlite"], reason: "SQLite spells it CURRENT_TIME" }],
  ["predicate.row-null", { oracles: ["sqlite"], reason: "SQLite rejects a row value in IS NULL" }],
  ["limit.with-ties", { oracles: ["sqlite"], reason: "SQLite has no FETCH FIRST clause" }],
  ["aggregate.grouping", { oracles: ["sqlite"], reason: "SQLite has neither ROLLUP nor GROUPING" }],
  [
    "aggregate.any-value",
    {
      oracles: ["sqlite", "pglite"],
      reason:
        "Which row of the group answers is implementation-dependent, so no two engines have to agree; SQLite has no ANY_VALUE either",
    },
  ],
  ["aggregate.variance", { oracles: ["sqlite"], reason: "SQLite has no VAR_POP" }],
  ["aggregate.stddev", { oracles: ["sqlite"], reason: "SQLite has no STDDEV_POP" }],
  ["aggregate.boolean", { oracles: ["sqlite"], reason: "SQLite has no EVERY" }],
  [
    "aggregate.json",
    {
      oracles: ["sqlite", "pglite"],
      reason:
        "SQLite spells it JSON_GROUP_ARRAY; PGlite returns a native JSON array while Minnow intentionally returns JSON text, and member order is unspecified without aggregate-local ORDER BY",
    },
  ],
  [
    "subquery.correlated-json-aggregate",
    {
      oracles: ["sqlite", "pglite"],
      reason:
        "SQLite lacks the SQL-standard JSON constructor spelling; PGlite returns native JSON while Minnow intentionally returns JSON text",
    },
  ],
  // --- SQL/JSON --------------------------------------------------------------------------
  ["json.value", { oracles: ["sqlite"], reason: "SQLite spells it json_extract" }],
  ["select.distinct-on", { oracles: ["sqlite"], reason: "SQLite has no DISTINCT ON" }],
  ["select.locking-clause", { oracles: ["sqlite"], reason: "SQLite has no FOR UPDATE" }],
  ["select.table-command", { oracles: ["sqlite"], reason: "SQLite has no TABLE command" }],
  [
    "literal.string-spellings",
    { oracles: ["sqlite"], reason: "SQLite has no E'…' or dollar-quoted strings" },
  ],
  ["predicate.like-operators", { oracles: ["sqlite"], reason: "SQLite has no ~~ operators" }],
  [
    "function.regexp-substring",
    { oracles: ["sqlite"], reason: "SQLite has no regex SUBSTRING, TO_HEX, or QUOTE_ functions" },
  ],
  ["json.agg-spellings", { oracles: ["sqlite"], reason: "SQLite spells it json_group_array" }],
  ["json.build-object", { oracles: ["sqlite"], reason: "SQLite spells it json_object" }],
  ["json.to-json", { oracles: ["sqlite"], reason: "SQLite spells it json_quote" }],
  [
    "json.row-reference",
    { oracles: ["sqlite"], reason: "SQLite has no row-valued alias or json_agg" },
  ],
  [
    "json.query",
    {
      oracles: ["sqlite", "pglite"],
      reason:
        "SQLite spells it json_extract; PostgreSQL renders JSON text with spaces after separators, which is a formatting difference rather than a value one",
    },
  ],
  ["json.exists", { oracles: ["sqlite"], reason: "SQLite spells it json_type IS NOT NULL" }],
  ["json.is-json", { oracles: ["sqlite"], reason: "SQLite spells it json_valid" }],
  [
    "json.object",
    {
      oracles: ["sqlite", "pglite"],
      reason:
        "SQLite has no KEY … VALUE spelling; PostgreSQL renders JSON text with spaces after separators",
    },
  ],
  [
    "json.array",
    { oracles: ["pglite"], reason: "PostgreSQL renders JSON text with spaces after separators" },
  ],
  [
    "json.arrow",
    {
      oracles: ["sqlite", "pglite"],
      reason:
        "PGlite decodes the json result natively while Minnow intentionally returns JSON text, and SQLite's CAST has no JSON type; the ->> forms below diff the same access against PGlite by value",
    },
  ],
  ["json.arrow-text", { oracles: ["sqlite"], reason: "SQLite's CAST has no JSON type" }],
  ["json.arrow-index", { oracles: ["sqlite"], reason: "SQLite's CAST has no JSON type" }],
  [
    "json.arrow-untyped",
    {
      oracles: ["pglite"],
      reason:
        "PostgreSQL cannot resolve -> over an untyped string literal, which is what this Minnow extension accepts; SQLite diffs it",
    },
  ],
  [
    "type.exact-numeric",
    {
      oracles: ["sqlite", "pglite"],
      reason:
        "Minnow returns exact decimals as strings; both oracle adapters decode this example as a number",
    },
  ],
  [
    "type.json-jsonb",
    {
      oracles: ["sqlite", "pglite"],
      reason:
        "Minnow returns JSON text, PGlite returns a native object, and SQLite does not have PostgreSQL JSONB casts",
    },
  ],
  ["type.uuid", { oracles: ["sqlite"], reason: "SQLite has no UUID type" }],
  [
    "type.interval",
    {
      oracles: ["sqlite", "pglite"],
      reason:
        "Minnow exposes its structured interval as canonical text; PGlite formats intervals differently and SQLite has no interval type",
    },
  ],
  ["from.lateral", { oracles: ["sqlite"], reason: "SQLite has no LATERAL source" }],
  ["from.lateral-non-equi", { oracles: ["sqlite"], reason: "SQLite has no LATERAL source" }],
  ["from.lateral-aggregate", { oracles: ["sqlite"], reason: "SQLite has no LATERAL source" }],
  ["from.lateral-limit", { oracles: ["sqlite"], reason: "SQLite has no LATERAL source" }],
  [
    "where.calendar-equality",
    { oracles: ["sqlite"], reason: "SQLite has no DATE_TRUNC or EXTRACT" },
  ],
  ["aggregate.string-agg", { oracles: ["sqlite"], reason: "SQLite spells it GROUP_CONCAT" }],
  ["json.table", { oracles: ["sqlite"], reason: "SQLite has no SQL/JSON JSON_TABLE syntax" }],
  ["predicate.similar-to", { oracles: ["sqlite"], reason: "SQLite has no SIMILAR TO" }],
  [
    "collation.explicit",
    { oracles: ["sqlite"], reason: "SQLite does not ship PostgreSQL's named C collation" },
  ],
  [
    "type.array",
    {
      oracles: ["sqlite", "pglite"],
      reason:
        "Minnow returns canonical JSON text, PostgreSQL returns a native array, and SQLite has no ARRAY constructor",
    },
  ],
  ["type.time", { oracles: ["sqlite"], reason: "SQLite has no TIME literal" }],
  [
    "type.date",
    {
      oracles: ["sqlite", "pglite"],
      reason:
        "SQLite has no native DATE type; PostgreSQL clients materialize DATE as midnight Date while Minnow preserves zoneless YYYY-MM-DD text",
    },
  ],

  ["predicate.match", { oracles: ["sqlite", "pglite"], reason: "MATCH is a Minnow extension" }],
  [
    "predicate.match-star",
    { oracles: ["sqlite", "pglite"], reason: "MATCH(*) is a Minnow extension" },
  ],
  [
    "predicate.match-parameter",
    { oracles: ["sqlite", "pglite"], reason: "MATCH is a Minnow extension" },
  ],
  ["function.bm25", { oracles: ["sqlite", "pglite"], reason: "BM25 is a Minnow extension" }],
  [
    "expression.date-trunc",
    { oracles: ["sqlite"], reason: "SQLite has no DATE_TRUNC; PostgreSQL still checks it" },
  ],
  [
    "expression.date-add",
    { oracles: ["sqlite"], reason: "SQLite spells interval arithmetic datetime(x, '+1 month')" },
  ],
  ["literal.date", { oracles: ["sqlite"], reason: "SQLite has no DATE '…' literal" }],
  ["literal.timestamp", { oracles: ["sqlite"], reason: "SQLite has no TIMESTAMP '…' literal" }],
  [
    "literal.scientific",
    {
      oracles: ["pglite"],
      reason:
        "PGlite renders every numeric constant as text while Minnow returns a Float64-representable value as a number",
    },
  ],
  [
    "expression.round",
    { oracles: ["pglite"], reason: "PostgreSQL's two-argument ROUND takes numeric, not float8" },
  ],
  ["expression.modulo", { oracles: ["pglite"], reason: "PostgreSQL has no % operator on float8" }],
  [
    "expression.cast",
    { oracles: ["sqlite"], reason: "SQLite renders a REAL cast to text as 10.0, PostgreSQL as 10" },
  ],
  [
    "function.numeric-core",
    {
      oracles: ["sqlite", "pglite"],
      reason: "one statement covering functions each oracle is missing a different one of",
    },
  ],
  ["function.string-extended", { oracles: ["pglite"], reason: "PostgreSQL spells INSTR strpos" }],
  ["function.extract", { oracles: ["sqlite"], reason: "SQLite has no EXTRACT(field FROM …)" }],
  ["limit.fetch-first", { oracles: ["sqlite"], reason: "SQLite has no FETCH FIRST" }],
  ["offset.standalone", { oracles: ["sqlite"], reason: "SQLite requires LIMIT before OFFSET" }],
  ["group-by.rollup", { oracles: ["sqlite"], reason: "SQLite has no ROLLUP" }],
  ["group-by.grouping-sets", { oracles: ["sqlite"], reason: "SQLite has no GROUPING SETS" }],
  ["predicate.boolean-test", { oracles: ["sqlite"], reason: "SQLite has no IS UNKNOWN" }],
  ["predicate.quantified", { oracles: ["sqlite"], reason: "SQLite has no ALL/ANY quantifiers" }],
  [
    "subquery.correlated-quantified",
    { oracles: ["sqlite"], reason: "SQLite has no ALL/ANY quantifiers" },
  ],
  ["predicate.ilike", { oracles: ["sqlite"], reason: "SQLite has no ILIKE" }],
  ["datetime.now", { oracles: ["sqlite"], reason: "SQLite has no now() function" }],
  [
    "function.string-postgres",
    { oracles: ["sqlite"], reason: "SQLite lacks LEFT, RIGHT, REVERSE, INITCAP, and the rest" },
  ],
  ["function.md5-format", { oracles: ["sqlite"], reason: "SQLite has no MD5 or FORMAT %I/%L" }],
  ["predicate.regex", { oracles: ["sqlite"], reason: "SQLite has no ~ operators" }],
  ["function.regexp-replace", { oracles: ["sqlite"], reason: "SQLite has no REGEXP_REPLACE" }],
  ["expression.power-operator", { oracles: ["sqlite"], reason: "SQLite has no ^ operator" }],
  [
    "function.math-extended",
    { oracles: ["sqlite"], reason: "SQLite lacks two-argument LOG, DIV, and WIDTH_BUCKET" },
  ],
  ["function.to-char-datetime", { oracles: ["sqlite"], reason: "SQLite has no TO_CHAR" }],
  ["function.to-char-numeric", { oracles: ["sqlite"], reason: "SQLite has no TO_CHAR" }],
  [
    "function.to-date-timestamp",
    {
      oracles: ["sqlite", "pglite"],
      reason:
        "SQLite has no TO_DATE; PostgreSQL clients materialize DATE as a midnight Date while Minnow preserves zoneless YYYY-MM-DD text (the corpus diffs TO_TIMESTAMP directly and TO_DATE through comparisons)",
    },
  ],
  [
    "function.make-date",
    {
      oracles: ["sqlite", "pglite"],
      reason:
        "SQLite has no MAKE_DATE; PostgreSQL clients materialize DATE as a midnight Date while Minnow preserves zoneless YYYY-MM-DD text (the corpus diffs the value through comparisons)",
    },
  ],
  [
    "function.age",
    {
      oracles: ["sqlite", "pglite"],
      reason:
        "SQLite has no AGE; PostgreSQL renders intervals as '1 mon 14 days 12:00:00' while Minnow returns its canonical months/days/usecs text (the unit tests pin the calendar arithmetic against PostgreSQL's answers)",
    },
  ],
  ["function.date-part", { oracles: ["sqlite"], reason: "SQLite has no DATE_PART or EXTRACT" }],
  ["expression.cast-postfix", { oracles: ["sqlite"], reason: "SQLite has no :: cast syntax" }],
  [
    "expression.concat-typed",
    { oracles: ["sqlite"], reason: "SQLite renders a REAL as 10.0 and a boolean as 1 in ||" },
  ],
  [
    "where.boolean-text",
    { oracles: ["sqlite"], reason: "SQLite stores booleans as integers, which 't' never equals" },
  ],
  [
    "select.values-ordered",
    { oracles: ["sqlite"], reason: "SQLite's VALUES takes no trailing ORDER BY or LIMIT" },
  ],
  ["where.between-symmetric", { oracles: ["sqlite"], reason: "SQLite has no BETWEEN SYMMETRIC" }],
  ["set.intersect-all", { oracles: ["sqlite"], reason: "SQLite has no INTERSECT ALL" }],
  ["set.except-all", { oracles: ["sqlite"], reason: "SQLite has no EXCEPT ALL" }],
]);

/**
 * The one failure an unoptimized plan is allowed to produce. `optimizePlan` does two jobs: it
 * rewrites plans for speed, and it decorrelates subqueries -- and decorrelation is lowering, not
 * optimization, so a correlated subquery has no form the executor can run until it happens. Those
 * plans reference an outer alias the inner block cannot resolve on its own, which is exactly this
 * message. Anything else means an optimization changed an answer, which is the bug being hunted.
 */
const REQUIRES_LOWERING = "Unknown table alias";

// --- The harness --------------------------------------------------------------------------------

describe("SQL conformance against SQLite and PGlite", () => {
  it.each(conformanceRuns())(
    "agrees on the generated corpus across all execution paths and oracles (fixture seed $fixtureSeed, corpus seed $corpusSeed)",
    async ({ fixtureSeed, corpusSeed }) => {
      const fixture = buildFixture(fixtureSeed);
      const corpus = buildCorpus(corpusSeed);
      const rowTables = rowTablesFor(fixture);
      const database = await minnowFixture(fixture);
      const oracles: Oracle[] = [sqliteOracle(fixture), await pgliteOracle(fixture)];
      const failures: string[] = [];
      let unoptimizedCompared = 0;
      let unoptimizedLowered = 0;
      try {
        for (const [index, testCase] of corpus.entries()) {
          const caseLabel = `#${String(index)} ${testCase.sql} :: ${JSON.stringify(testCase.params ?? [])}`;
          const hasTopLevelOrder = hasResultOrder(compileQuery(testCase.sql, { optimize: false }));
          if (testCase.ordered !== hasTopLevelOrder) {
            failures.push(
              `${caseLabel}\n  harness ordering flag is ${String(testCase.ordered)}, compiled outer ORDER BY is ${String(hasTopLevelOrder)}`,
            );
            continue;
          }
          let vectorized: QueryResult;
          let rowExecutor: QueryResult;
          try {
            vectorized = await database.query(
              testCase.sql,
              testCase.params === undefined ? {} : { params: testCase.params },
            );
            rowExecutor = executeRowQuery(
              catalogBoundPlan(testCase.sql, testCase.params),
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
          if (vectorized.columns.join(",") !== rowExecutor.columns.join(",")) {
            failures.push(
              `${caseLabel}\n  column order/names, vectorized vs row executor:\n` +
                `    vectorized: ${vectorized.columns.join(", ")}\n` +
                `    row: ${rowExecutor.columns.join(", ")}`,
            );
          }
          // The optimizer is otherwise inside the trusted base: both paths above compile through
          // it, so a rewrite that changes an answer consistently is invisible to their diff and
          // visible only to an oracle. Features no oracle can judge -- MATCH, BM25, and the rest of
          // matrixSkips -- would have nothing left to contradict them. Comparing against the
          // unoptimized plan is SQLite's disabled-optimization run: same question, no rewrites.
          //
          // Decorrelation is the exception, because it is lowering rather than optimization: a
          // correlated subquery has no executable form until it runs, so those plans legitimately
          // fail to execute unoptimized. That is allowed and counted, and any *other* failure is a
          // real one -- see the assertions below, which pin both the count and the reason.
          try {
            const unoptimized = executeRowQuery(
              catalogBoundPlan(testCase.sql, testCase.params, false),
              rowTables,
            );
            unoptimizedCompared += 1;
            const unoptimizedKeys = resultKeys(unoptimized.rows, testCase.ordered);
            if (rowKeys.join("\n") !== unoptimizedKeys.join("\n")) {
              failures.push(
                `${caseLabel}\n${diffSummary("optimized vs unoptimized plan", rowKeys, unoptimizedKeys)}`,
              );
            }
            if (rowExecutor.columns.join(",") !== unoptimized.columns.join(",")) {
              failures.push(
                `${caseLabel}\n  column order/names, optimized vs unoptimized plan:\n` +
                  `    optimized: ${rowExecutor.columns.join(", ")}\n` +
                  `    unoptimized: ${unoptimized.columns.join(", ")}`,
              );
            }
          } catch (error) {
            if (!String(error).includes(REQUIRES_LOWERING)) {
              failures.push(`${caseLabel}\n  unoptimized plan threw: ${String(error)}`);
            }
            unoptimizedLowered += 1;
          }
          for (const oracle of oracles) {
            if (testCase.skip?.includes(oracle.name)) continue;
            let oracleResult: { rows: Array<Record<string, unknown>>; columns: string[] };
            try {
              oracleResult = await oracle.execute(testCase);
            } catch (error) {
              failures.push(`${caseLabel}\n  ${oracle.name} threw: ${String(error)}`);
              continue;
            }
            const oracleKeys = resultKeys(oracleResult.rows, testCase.ordered);
            const minnowKeys = resultKeys(numericDecodedRows(vectorized), testCase.ordered);
            if (minnowKeys.join("\n") !== oracleKeys.join("\n")) {
              failures.push(
                `${caseLabel}\n${diffSummary(`minnow vs ${oracle.name}`, minnowKeys, oracleKeys)}`,
              );
            }
            // Row comparison sorts keys, so it cannot see output column order. Compare the
            // projected column lists directly: a regression that reorders or renames output
            // columns would otherwise pass silently.
            if (vectorized.columns.join(",") !== oracleResult.columns.join(",")) {
              failures.push(
                `${caseLabel}\n  column order/names vs ${oracle.name}:\n` +
                  `    minnow: ${vectorized.columns.join(", ")}\n` +
                  `    oracle: ${oracleResult.columns.join(", ")}`,
              );
            }
          }
        }
      } finally {
        for (const oracle of oracles) await oracle.close();
      }
      // Divergences are reported before the structural floors below, because a real disagreement
      // is the thing worth reading: a failing case also skips the rest of its own checks, which
      // would otherwise trip a floor and bury the diff that explains it.
      if (failures.length > 0) {
        expect.fail(
          `${String(failures.length)} of ${String(corpus.length)} conformance cases diverged:\n\n` +
            failures.slice(0, 10).join("\n\n"),
        );
      }
      // The floor is the combination layer: deleting it would otherwise quietly halve coverage.
      expect(corpus.length).toBeGreaterThan(1_000);
      expect(combinationCases().length).toBeGreaterThan(80);
      expect(wildcardOrderCases()).toHaveLength(10);
      expect(wildcardShapeCases()).toHaveLength(12);
      // The unoptimized comparison is only worth something while it actually runs. Every case that
      // got this far reached it, so pin both the total and the share that ran rather than lowered:
      // a change that routes cases into the lowering branch -- or one that makes the corpus
      // unrunnable without the optimizer -- fails here instead of quietly becoming a no-op.
      expect(unoptimizedCompared + unoptimizedLowered).toBe(corpus.length);
      expect(unoptimizedCompared / corpus.length).toBeGreaterThan(0.9);
    },
    240_000,
  );

  it("agrees with the oracles on every read-only feature the matrix claims", async () => {
    const covered = matrixFeatures.filter(
      (feature) =>
        feature.status === "supported" &&
        !writesData(feature.id) &&
        matrixSkips.get(feature.id)?.oracles.length !== 2,
    );
    const database = await matrixMinnow();
    const oracles: Oracle[] = [matrixSqlite(), await matrixPglite()];
    const failures: string[] = [];
    try {
      for (const feature of covered) {
        // The compiled outer tail distinguishes a result ORDER BY from one nested inside OVER.
        // PostgreSQL validates final ordering; SQLite has different default NULL placement, so
        // it remains a value-set oracle for these matrix examples.
        const ordered = hasResultOrder(compileQuery(feature.example, { optimize: false }));
        const testCase: Case = {
          sql: feature.example,
          ordered,
          ...(feature.params === undefined ? {} : { params: feature.params }),
        };
        let minnowResult: QueryResult;
        try {
          minnowResult = await database.query(
            testCase.sql,
            testCase.params === undefined ? {} : { params: testCase.params },
          );
        } catch (error) {
          failures.push(`${feature.id} :: ${feature.example}\n  minnow threw: ${String(error)}`);
          continue;
        }
        const skipped = matrixSkips.get(feature.id)?.oracles ?? [];
        for (const oracle of oracles) {
          if (skipped.includes(oracle.name)) continue;
          let oracleResult: { rows: Array<Record<string, unknown>>; columns: string[] };
          try {
            oracleResult = await oracle.execute(testCase);
          } catch (error) {
            failures.push(
              `${feature.id} :: ${feature.example}\n  ${oracle.name} threw: ${String(error)}`,
            );
            continue;
          }
          const compareOrder = ordered && oracle.name === "pglite";
          const minnowKeys = resultKeys(numericDecodedRows(minnowResult), compareOrder);
          const oracleKeys = resultKeys(oracleResult.rows, compareOrder);
          if (minnowKeys.join("\n") !== oracleKeys.join("\n")) {
            failures.push(
              `${feature.id} :: ${feature.example}\n${diffSummary(`minnow vs ${oracle.name}`, minnowKeys, oracleKeys)}`,
            );
          }
          if (minnowResult.columns.join(",") !== oracleResult.columns.join(",")) {
            failures.push(
              `${feature.id} :: ${feature.example}\n  column order/names vs ${oracle.name}:\n` +
                `    minnow: ${minnowResult.columns.join(", ")}\n` +
                `    oracle: ${oracleResult.columns.join(", ")}`,
            );
          }
        }
      }
    } finally {
      for (const oracle of oracles) await oracle.close();
    }
    expect(covered.length).toBeGreaterThan(80);
    if (failures.length > 0) {
      expect.fail(
        `${String(failures.length)} of ${String(covered.length)} matrix features diverged:\n\n` +
          failures.slice(0, 10).join("\n\n"),
      );
    }
  }, 240_000);

  it("pins every read claim that neither external oracle can represent", async () => {
    const expectedIds = [
      "aggregate.any-value",
      "aggregate.json",
      "function.age",
      "function.bm25",
      "function.make-date",
      "function.numeric-core",
      "function.to-date-timestamp",
      "function.trim-multi-character",
      "json.arrow",
      "json.object",
      "json.query",
      "predicate.match",
      "predicate.match-parameter",
      "predicate.match-star",
      "subquery.correlated-json-aggregate",
      "type.array",
      "type.date",
      "type.exact-numeric",
      "type.interval",
      "type.json-jsonb",
    ];
    const features = matrixFeatures.filter(
      (feature) =>
        feature.status === "supported" &&
        !writesData(feature.id) &&
        matrixSkips.get(feature.id)?.oracles.length === 2,
    );
    expect(features.map(({ id }) => id).sort()).toEqual(expectedIds);
    const database = await matrixMinnow();

    for (const feature of features) {
      const result = await database.query(
        feature.example,
        feature.params === undefined ? {} : { params: feature.params },
      );
      expect(result.columns, feature.id).toEqual(
        feature.id === "function.numeric-core"
          ? ["n", "g", "l", "f", "c", "m", "p", "s"]
          : feature.id === "function.bm25"
            ? ["region", "score"]
            : feature.id === "subquery.correlated-json-aggregate"
              ? ["region", "amounts"]
              : feature.id === "function.age"
                ? ["since", "so_far"]
                : feature.id === "function.make-date"
                  ? ["day", "at"]
                  : feature.id === "function.to-date-timestamp"
                    ? ["day", "at", "epoch"]
                    : [
                        {
                          "aggregate.any-value": "sample",
                          "aggregate.json": "regions",
                          "function.trim-multi-character": "trimmed",
                          "json.arrow": "element",
                          "json.object": "document",
                          "json.query": "a",
                          "predicate.match": "region",
                          "predicate.match-parameter": "region",
                          "predicate.match-star": "region",
                          "type.array": "pair",
                          "type.date": "day",
                          "type.exact-numeric": "amount",
                          "type.interval": "next_day",
                          "type.json-jsonb": "document",
                        }[feature.id] ?? "",
                      ],
      );

      if (feature.id === "function.trim-multi-character") {
        expect(resultKeys(result.rows, false), feature.id).toEqual(
          resultKeys([{ trimmed: "st" }, { trimmed: "st" }, { trimmed: "east" }], false),
        );
      } else if (feature.id === "aggregate.any-value") {
        expect(result.rows, feature.id).toHaveLength(1);
        expect([10, 6, 3, 8], feature.id).toContain(result.rows[0]?.sample);
      } else if (feature.id === "aggregate.json") {
        const regions = JSON.parse(String(result.rows[0]?.regions)) as Array<
          Record<string, unknown>
        >;
        expect(resultKeys(regions, false), feature.id).toEqual(
          resultKeys(
            [{ region: "west" }, { region: "west" }, { region: "east" }, { region: null }],
            false,
          ),
        );
      } else if (feature.id === "subquery.correlated-json-aggregate") {
        const normalized = result.rows.map((row) => ({
          region: row.region,
          amounts: row.amounts === null ? null : (JSON.parse(String(row.amounts)) as unknown),
        }));
        expect(normalized, feature.id).toEqual([
          { region: "west", amounts: [{ amount: 6 }, { amount: 10 }] },
          { region: "west", amounts: [{ amount: 6 }, { amount: 10 }] },
          { region: "east", amounts: [{ amount: 3 }] },
          { region: null, amounts: null },
        ]);
      } else if (feature.id === "json.query") {
        expect(JSON.parse(String(result.rows[0]?.a)), feature.id).toEqual([1, 2]);
      } else if (feature.id === "json.object") {
        expect(JSON.parse(String(result.rows[0]?.document)), feature.id).toEqual({
          a: 1,
          detail: { name: "Acme" },
        });
      } else if (feature.id === "json.arrow") {
        expect(JSON.parse(String(result.rows[0]?.element)), feature.id).toBe(6);
      } else if (feature.id === "type.exact-numeric") {
        expect(result.rows, feature.id).toEqual([{ amount: "1.25" }]);
      } else if (feature.id === "type.json-jsonb") {
        expect(JSON.parse(String(result.rows[0]?.document)), feature.id).toEqual({ a: 1 });
      } else if (feature.id === "type.interval") {
        expect(result.rows, feature.id).toEqual([
          { next_day: new Date("2026-01-03T00:00:00.000Z") },
          { next_day: new Date("2025-12-31T00:00:00.000Z") },
          { next_day: new Date("2026-02-02T00:00:00.000Z") },
          { next_day: null },
        ]);
      } else if (feature.id === "type.array") {
        expect(JSON.parse(String(result.rows[0]?.pair)), feature.id).toEqual([1, 2]);
      } else if (feature.id.startsWith("predicate.match")) {
        expect(result.rows, feature.id).toEqual([{ region: "west" }, { region: "west" }]);
      } else if (feature.id === "function.bm25") {
        expect(result.rows, feature.id).toHaveLength(2);
        for (const row of result.rows) {
          expect(row.region, feature.id).toBe("west");
          expect(row.score, feature.id).toEqual(expect.any(Number));
          expect(row.score as number, feature.id).toBeGreaterThan(0);
        }
      } else if (feature.id === "type.date") {
        expect(result.rows, feature.id).toEqual([{ day: "2026-08-26" }]);
      } else if (feature.id === "function.make-date") {
        expect(result.rows, feature.id).toEqual([
          { day: "2026-01-02", at: new Date("2026-01-02T03:04:05.500Z") },
        ]);
      } else if (feature.id === "function.to-date-timestamp") {
        expect(result.rows, feature.id).toEqual([
          {
            day: "2026-01-02",
            at: new Date("2026-01-02T15:04:00.000Z"),
            epoch: new Date("2026-01-02T03:00:00.000Z"),
          },
        ]);
      } else if (feature.id === "function.age") {
        // PostgreSQL's answers for these three fixture dates, rendered as Minnow's interval text:
        // years and months first, days borrowed from the earlier date's month, then the time.
        expect(
          resultKeys(
            result.rows.map(({ since }) => ({ since })),
            false,
          ),
          feature.id,
        ).toEqual(
          resultKeys(
            [
              { since: "2 mons 13 days 43200000000 usecs" },
              { since: "2 mons 16 days 43200000000 usecs" },
              { since: "1 mons 14 days 43200000000 usecs" },
            ],
            false,
          ),
        );
        for (const row of result.rows) {
          expect(String(row.so_far), feature.id).toMatch(/^\d+ mons \d+ days \d+ usecs$/);
        }
      } else if (feature.id === "function.numeric-core") {
        expect(result.rows, feature.id).toEqual([
          { n: 10, g: 10, l: 5, f: 10, c: 10, m: 2, p: 8, s: 4 },
          { n: 6, g: 6, l: 5, f: 6, c: 6, m: 2, p: 8, s: 4 },
          { n: null, g: 5, l: 3, f: 3, c: 3, m: 3, p: 8, s: 4 },
          { n: 8, g: 8, l: 5, f: 8, c: 8, m: 0, p: 8, s: 4 },
        ]);
      } else {
        expect.fail(`Missing self-check for ${feature.id}`);
      }
    }
  }, 120_000);

  it("renders declared-scale NUMERIC results exactly as PostgreSQL", async () => {
    // The generated corpus compares NUMERIC values numerically (its oracle parses them into
    // numbers), so it cannot see the rendered text. This case diffs the strings themselves:
    // PGlite's default decoder leaves `numeric` as PostgreSQL's rendered text, and a column
    // with a declared scale must display at exactly that scale on both engines. Write-time
    // rounding (half away from zero) and scientific-notation input are diffed the same way.
    const database = new MinnowDatabase(new MemoryBlockStore());
    const { PGlite } = await import("@electric-sql/pglite");
    const postgres = await PGlite.create();
    const failures: string[] = [];
    try {
      const ddl =
        "CREATE TABLE ledger (id INTEGER PRIMARY KEY, amount NUMERIC(10, 2), rate NUMERIC(8, 3))";
      const insert =
        "INSERT INTO ledger VALUES " +
        "(1, '1.50', '0.100'), (2, 7, 2), (3, '-3.1', '12.3456'), (4, '0.005', '1e2'), " +
        "(5, NULL, '0')";
      // A declared scale beyond division's significant-digit selection: the AVG quotient must
      // compute real digits to that scale rather than padding zeros over a shorter quotient.
      const fineDdl = "CREATE TABLE fine (id INTEGER PRIMARY KEY, v NUMERIC(30, 24))";
      const fineInsert = "INSERT INTO fine VALUES (1, 0), (2, 1), (3, 1)";
      await database.execute(ddl);
      await database.execute(insert);
      await database.execute(fineDdl);
      await database.execute(fineInsert);
      await postgres.exec(ddl);
      await postgres.exec(insert);
      await postgres.exec(fineDdl);
      await postgres.exec(fineInsert);
      const queries = [
        "SELECT id, amount, rate FROM ledger ORDER BY id",
        "SELECT SUM(amount) AS total, MIN(amount) AS low, MAX(rate) AS high FROM ledger",
        "SELECT COALESCE(amount, CAST(0 AS NUMERIC(10, 2))) AS amount FROM ledger WHERE id = 1",
        "SELECT CAST(amount AS NUMERIC(12, 4)) AS wide FROM ledger WHERE id = 1",
        // AVG over the declared scale, grouped and windowed: 24 true digits, not 20 padded.
        "SELECT AVG(v) AS mean FROM fine",
        "SELECT SUM(v) AS total FROM fine",
        "SELECT id, AVG(v) OVER () AS mean FROM fine ORDER BY id",
        // Division's result-scale selection at quotient weights above and below one, and its
        // half-away-from-zero rounding of the final digit.
        "SELECT CAST(2 AS NUMERIC) / 3 AS q",
        "SELECT CAST(1 AS NUMERIC) / 6 AS q",
        "SELECT CAST(1 AS NUMERIC) / 30000 AS q",
        "SELECT CAST(1 AS NUMERIC) / 300000 AS q",
        "SELECT CAST(200000000 AS NUMERIC) / 3 AS q",
        "SELECT CAST(20000 AS NUMERIC) / 3 AS q",
        "SELECT CAST(5 AS NUMERIC) / 0.0003 AS q",
        "SELECT CAST(-2 AS NUMERIC) / 3 AS q",
        "SELECT CAST(0.00005 AS NUMERIC) / 3 AS q",
        // Decimal constants carry their exact written digits, so constant arithmetic happens
        // in exact decimal space and quotient scales follow the written scales, as PostgreSQL
        // types bare constants NUMERIC. Integer constants beyond int8 are NUMERIC there too.
        "SELECT 1.000000000000000000000000 / 3 AS q",
        "SELECT -1.000000000000000000000000 / 6 AS q",
        "SELECT 1000000000000000000000000000000000000000 / 3 AS q",
        "SELECT 123456789.123456789123456789 * 2 AS q",
        "SELECT 0.30000000000000000000000004 - 0.2 AS q",
        "SELECT amount + 0.000000000000000000000001 AS q FROM ledger WHERE id = 1",
        "SELECT rate * 1.000000000000000000000001 AS q FROM ledger WHERE id = 3",
        // A scientific literal that stays exact expands at parse the way PostgreSQL expands
        // it, so the rendered digit strings agree.
        "SELECT 1e400 AS q",
        "SELECT 1.000000000000000001e3 AS q",
      ];
      for (const sql of queries) {
        const minnowRows = (await database.query(sql)).rows;
        const postgresRows = (await postgres.query(sql)).rows;
        if (JSON.stringify(minnowRows) !== JSON.stringify(postgresRows)) {
          failures.push(
            `${sql}\n  minnow:   ${JSON.stringify(minnowRows)}\n  postgres: ${JSON.stringify(postgresRows)}`,
          );
        }
      }
      const returning =
        "INSERT INTO ledger (id, amount, rate) VALUES (6, '9.9', 8) RETURNING amount, rate";
      const minnowReturned = (await database.execute(returning)) as {
        returnedRows?: Array<Record<string, unknown>>;
      };
      const postgresReturned = await postgres.query(returning);
      if (JSON.stringify(minnowReturned.returnedRows) !== JSON.stringify(postgresReturned.rows)) {
        failures.push(
          `${returning}\n  minnow:   ${JSON.stringify(minnowReturned.returnedRows)}\n` +
            `  postgres: ${JSON.stringify(postgresReturned.rows)}`,
        );
      }
    } finally {
      await postgres.close();
    }
    expect(failures).toEqual([]);
  }, 120_000);

  it("keeps ROUND and its numeric siblings exact over NUMERIC, rendered as PostgreSQL does", async () => {
    // ROUND over an exact NUMERIC once returned the engine's internal tag ("\u0000minnow-domain:
    // numeric:75.91") because the result column carried no NUMERIC domain, and a derived table
    // over it failed to build a number vector. This diffs the rendered text against PGlite for
    // every scalar that PostgreSQL types numeric-in, numeric-out — ROUND, TRUNC, ABS, FLOOR,
    // CEIL, MOD, SIGN, negation — across projection, aggregates, COALESCE, GROUP BY keys,
    // derived tables, UNION members, predicates, and window ordering.
    const database = new MinnowDatabase(new MemoryBlockStore());
    const { PGlite } = await import("@electric-sql/pglite");
    const postgres = await PGlite.create();
    const failures: string[] = [];
    try {
      const statements = [
        "CREATE TABLE o (id INTEGER PRIMARY KEY, total DOUBLE PRECISION)",
        "INSERT INTO o VALUES (1, 101.314), (2, 50.5)",
        "CREATE TABLE n (id INTEGER PRIMARY KEY, m NUMERIC(10, 2), g INTEGER)",
        "INSERT INTO n VALUES (1, '1.25', 1), (2, '2.35', 1), (3, '-3.45', 2), (4, NULL, 2), " +
          "(5, '12345.67', 3), (6, '7', 3)",
      ];
      for (const sql of statements) {
        await database.execute(sql);
        await postgres.exec(sql);
      }
      const queries = [
        // Doubles cast to NUMERIC, then rounded: the shape that first leaked the tag.
        "SELECT round(avg(total)::numeric, 2) AS a FROM o",
        "SELECT round(sum(total)::numeric, 2) AS a FROM o",
        "SELECT id, round(total::numeric, 1) AS a FROM o ORDER BY id",
        // A declared-scale column through every ROUND shape.
        "SELECT id, ROUND(m, 1) AS r FROM n ORDER BY id",
        "SELECT id, ROUND(m) AS r FROM n ORDER BY id",
        "SELECT id, ROUND(m, 3) AS r FROM n ORDER BY id",
        "SELECT id, ROUND(m, -1) AS r FROM n ORDER BY id",
        "SELECT ROUND(SUM(m), 1) AS r FROM n",
        "SELECT SUM(ROUND(m, 1)) AS r FROM n",
        "SELECT SUM(ROUND(m, 3)) AS r FROM n",
        "SELECT id, COALESCE(ROUND(m, 1), 0) AS r FROM n ORDER BY id",
        "SELECT ROUND(m, 1) AS k, COUNT(*) AS c FROM n GROUP BY ROUND(m, 1) ORDER BY k",
        "SELECT ROUND(m, 1) AS k FROM n GROUP BY ROUND(m, 1) HAVING ROUND(m, 1) > 2 ORDER BY k",
        "SELECT t.r FROM (SELECT ROUND(m, 1) AS r FROM n) t ORDER BY t.r",
        "SELECT t.r, COUNT(*) AS c FROM (SELECT ROUND(m, 1) AS r FROM n) t GROUP BY t.r ORDER BY t.r",
        "SELECT id, ROW_NUMBER() OVER (ORDER BY ROUND(m, 1)) AS rn FROM n ORDER BY id",
        "SELECT id, ROUND(m, 1) AS r FROM n ORDER BY ROUND(m, 1) DESC NULLS LAST",
        "SELECT id FROM n WHERE ROUND(m, 1) > 2 ORDER BY id",
        "SELECT id, ROUND(m, 1) + 1 AS r FROM n ORDER BY id",
        "SELECT id, ROUND(CAST(m AS NUMERIC(10, 3)), 2) AS r FROM n ORDER BY id",
        "SELECT ROUND(CAST(2.5 AS NUMERIC)) AS up, ROUND(CAST(-2.5 AS NUMERIC)) AS down",
        // Members of differing scale union as unconstrained NUMERIC; the combined column renders
        // canonically, so the row whose PostgreSQL text would keep a trailing zero stays out.
        "SELECT ROUND(m, 1) AS r FROM n WHERE id <> 6 UNION ALL SELECT ROUND(m, 2) FROM n WHERE id <> 6 ORDER BY r",
        // The rest of PostgreSQL's numeric-in, numeric-out scalar family.
        "SELECT id, TRUNC(m, 1) AS t, TRUNC(m) AS t0, TRUNC(m, 3) AS t3, TRUNC(m, -1) AS tn FROM n ORDER BY id",
        "SELECT id, ABS(m) AS a, FLOOR(m) AS f, CEIL(m) AS c, CEILING(m) AS c2 FROM n ORDER BY id",
        "SELECT id, MOD(m, 2) AS md, SIGN(m) AS s, -m AS neg FROM n ORDER BY id",
        "SELECT SUM(ABS(m)) AS a, MIN(FLOOR(m)) AS f, MAX(CEIL(m)) AS c FROM n",
        // A plain-number fallback beside NUMERIC values renders at its own scale.
        "SELECT id, COALESCE(m, 0) AS r FROM n ORDER BY id",
        "SELECT id, CASE WHEN m IS NULL THEN 0 ELSE m END AS r FROM n ORDER BY id",
        "SELECT id, GREATEST(m, 2) AS g, LEAST(m, 2) AS l FROM n ORDER BY id",
        "SELECT COALESCE(SUM(m), 0) AS s FROM n WHERE id > 100",
      ];
      for (const sql of queries) {
        let minnow: string;
        let expected: string;
        try {
          minnow = JSON.stringify((await database.query(sql)).rows);
        } catch (error) {
          minnow = `threw ${error instanceof Error ? error.message : String(error)}`;
        }
        try {
          expected = JSON.stringify((await postgres.query(sql)).rows);
        } catch (error) {
          expected = `threw ${error instanceof Error ? error.message : String(error)}`;
        }
        if (minnow !== expected) {
          failures.push(`${sql}\n  minnow:   ${minnow}\n  postgres: ${expected}`);
        }
      }
    } finally {
      await postgres.close();
    }
    expect(failures).toEqual([]);
  }, 120_000);

  it("leaves no supported feature unaccounted for", () => {
    const unclassified = matrixFeatures
      .filter((feature) => feature.status === "supported")
      .filter(
        (feature) =>
          !writesData(feature.id) &&
          matrixSkips.get(feature.id)?.oracles.length !== 2 &&
          !/^\s*(SELECT|WITH|VALUES|TABLE)\b/i.test(feature.example),
      )
      .map((feature) => feature.id);
    // A supported feature is checked against the oracles above, or owned by the mutation
    // harness, or exempted by name with a reason. Nothing else.
    expect(unclassified).toEqual([]);
    // And nothing may be exempted that does not exist, so the list cannot rot.
    const ids = new Set(matrixFeatures.map((feature) => feature.id));
    expect([...matrixSkips.keys()].filter((id) => !ids.has(id))).toEqual([]);

    // The PostgreSQL profile is executable, not a second prose list: every deterministic
    // compatible read reaches PGlite above. The sole exception is ANY_VALUE, whose chosen group
    // member is intentionally unspecified and has its own parser-acceptance check.
    const compatibleReads = matrixFeatures.filter(
      (feature) =>
        feature.status === "supported" &&
        !writesData(feature.id) &&
        postgresClassification(feature) === "compatible",
    );
    const compatiblePgliteSkips = compatibleReads
      .filter((feature) => matrixSkips.get(feature.id)?.oracles.includes("pglite") === true)
      .map(({ id }) => id);
    expect(compatiblePgliteSkips).toEqual(
      postgresProfile.overrides
        .filter(({ verification }) => verification === "acceptance")
        .map(({ id }) => id),
    );
  });
});
