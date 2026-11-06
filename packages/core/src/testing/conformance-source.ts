import type { QueryParameter } from "../engine/query.js";
import { mulberry32 } from "./seeds.js";
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

export interface FixtureRow {
  id: number;
  region: string | null;
  amount: number;
  active: boolean;
  /** TIMESTAMPTZ on every engine: an instant, whose text ends in `+00`. */
  joined: Date | null;
  label: string;
  /** TIMESTAMP (without time zone) on Minnow and PostgreSQL: its text carries no zone. */
  noted: Date | null;
}

const NOTED_START = Date.UTC(2026, 0, 1);

/**
 * `noted` for one row, derived from its id so the random draws that build every other column
 * (and the seeds that replay them) stay as they were: one hour, one second, and a quarter-second
 * step per id, so the text carries fractions, and NULL every fifth row.
 */
function notedFor(id: number): Date | null {
  return id % 5 === 0 ? null : new Date(NOTED_START + id * 3_601_250);
}

/** PostgreSQL's input text for a TIMESTAMP holding the same UTC wall clock as `value`. */
export function wallClockText(value: Date | null): string | null {
  return value === null ? null : value.toISOString().slice(0, 23).replace("T", " ");
}

export function buildFixture(seed: number): FixtureRow[] {
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
      noted: notedFor(id),
    });
  }
  return rows;
}

export const dims = [
  { region: "west", label: "West Coast", rank: 1 },
  { region: "east", label: "East Coast", rank: 2 },
  { region: "north", label: "North", rank: 3 },
  { region: "central", label: "Central", rank: 4 },
];

type OracleName = "sqlite" | "pglite";

/** A bound value: a scalar, or an array for a placeholder used as `= ANY($n)`. */
type CaseParameter = QueryParameter;

export interface Case {
  sql: string;
  params?: CaseParameter[];
  /** Compare row order exactly; requires an ORDER BY ending in a unique key. */
  ordered: boolean;
  /** Oracles to skip, each for a documented semantic or dialect divergence. */
  skip?: readonly OracleName[];
  /** Generator dimensions the case exercises (testing/reach.ts), counted when it agrees. */
  reach?: readonly string[];
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

export const templates: Template[] = [
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
  () => ({
    // Over an INTEGER column both sums and the count are integers, and the variance must still
    // divide exactly: PostgreSQL computes it in NUMERIC, never with integer `/`.
    sql: `SELECT VARIANCE(id) AS v, VAR_POP(id) AS vp, STDDEV(id) AS s, STDDEV_POP(id) AS sp FROM data WHERE id % 7 <> 0`,
    ordered: false,
    skip: ["sqlite"],
  }),
  () => ({
    sql: `SELECT region, VAR_SAMP(id) AS v, STDDEV_POP(id) AS sp FROM data WHERE region IS NOT NULL GROUP BY region`,
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
