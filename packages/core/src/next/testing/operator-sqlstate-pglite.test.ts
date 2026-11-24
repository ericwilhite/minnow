/**
 * Comparing values whose types have no comparison operator is "operator does not exist" (42883),
 * in every form a comparison takes, as in PostgreSQL.
 *
 * Lowering matched comparison operands with `commonType`, whose refusal is UNION and CASE result
 * matching's 42804 ("comparison types int4 and bool cannot be matched"): `SELECT v FROM t WHERE v
 * = true` was 42804 where PostgreSQL says 42883 "operator does not exist: integer = boolean".
 * Comparison operands now go through `comparisonType` (lower/coerce.ts), which names the operator.
 *
 * Law: PostgreSQL (PGlite) for every case: the SQLSTATE, and where it finds no operator, its
 * message ("operator does not exist: <left> <op> <right>").
 */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { afterAll, beforeAll, expect, it } from "vitest";
import { createReach, OPERATOR_SQLSTATE_REACH } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import { ReferenceUnsupportedError } from "./ir-reference.js";
import { LoweredDatabase } from "./lowering-harness.js";
import { oracleRows, type NextRows } from "./differential-adapter.js";
import { StorageCore } from "./storage-core.js";

const COLUMNS = {
  integer: "i",
  bigint: "g",
  boolean: "b",
  text: "s",
  date: "d",
  "timestamp without time zone": "ts",
  numeric: "n",
  "double precision": "f",
  interval: "iv",
  "character varying": "vc",
  character: "ch",
  smallint: "si",
  real: "r",
  "time without time zone": "tm",
  "timestamp with time zone": "tz",
  uuid: "u",
  json: "j",
  jsonb: "jb",
} as const;
type Type = keyof typeof COLUMNS;
const TYPES = Object.keys(COLUMNS) as Type[];
const SETUP = [
  "CREATE TABLE t (id integer PRIMARY KEY, i integer, g bigint, b boolean, s text, d date, ts timestamp, n numeric, f double precision, iv interval, vc varchar(10), ch char(5), si smallint, r real, tm time, tz timestamptz, u uuid, j json, jb jsonb)",
  "INSERT INTO t VALUES (1, 1, 1, true, '1', DATE '2020-01-01', TIMESTAMP '2020-01-01 00:00:00', 1, 1, NULL, '1', '1', 1, 1, NULL, NULL, NULL, NULL, NULL)",
  "INSERT INTO t VALUES (2, 2, 3, false, 'a', DATE '2020-01-02', TIMESTAMP '2020-01-02 12:00:00', 2.5, 2.5, NULL, 'a', 'a', 2, 2.5, NULL, NULL, NULL, NULL, NULL)",
  "INSERT INTO t VALUES (3, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)",
];

const OPERATORS = ["=", "<>", "<", "<=", ">", ">="] as const;
type Operator = (typeof OPERATORS)[number];
const FORMS = [
  "binary",
  "in list",
  "not in list",
  "between",
  "is distinct from",
  "is not distinct from",
  "any subquery",
  "all subquery",
  "in subquery",
  "case when",
  "nullif",
  // Neon's review of #63: forms the first generator lacked.
  "is true",
  "is not false",
  "any array",
  "all array",
  "not in subquery",
  "not between",
  "between symmetric",
  "join on",
  "check",
  "like",
  "ilike",
  "similar to",
] as const;
type Form = (typeof FORMS)[number];

/** One comparison of column `l` with columns `rs` in the given form (lists use all of `rs`). */
function statement(form: Form, operator: Operator, l: string, rs: readonly string[]): string {
  const r = rs[0] ?? l;
  switch (form) {
    case "binary":
      return `SELECT 1 FROM t WHERE ${l} ${operator} ${r}`;
    case "in list":
      return `SELECT id FROM t WHERE ${l} IN (${rs.join(", ")}) ORDER BY id`;
    case "not in list":
      return `SELECT id FROM t WHERE ${l} NOT IN (${rs.join(", ")}) ORDER BY id`;
    case "between":
      return `SELECT 1 FROM t WHERE ${l} BETWEEN ${r} AND ${r}`;
    case "is distinct from":
      return `SELECT 1 FROM t WHERE ${l} IS DISTINCT FROM ${r}`;
    case "is not distinct from":
      return `SELECT 1 FROM t WHERE ${l} IS NOT DISTINCT FROM ${r}`;
    case "any subquery":
      return `SELECT 1 FROM t WHERE ${l} ${operator} ANY (SELECT ${r} FROM t)`;
    case "all subquery":
      return `SELECT 1 FROM t WHERE ${l} ${operator} ALL (SELECT ${r} FROM t)`;
    case "in subquery":
      return `SELECT 1 FROM t WHERE ${l} IN (SELECT ${r} FROM t)`;
    case "case when":
      return `SELECT CASE ${l} WHEN ${r} THEN 1 END FROM t`;
    case "nullif":
      return `SELECT NULLIF(${l}, ${r}) FROM t`;
    case "is true":
      return `SELECT 1 FROM t WHERE ${l} IS TRUE`;
    case "is not false":
      return `SELECT 1 FROM t WHERE ${l} IS NOT FALSE`;
    case "any array":
      return `SELECT id FROM t WHERE ${l} = ANY (ARRAY[${rs.join(", ")}]) ORDER BY id`;
    case "all array":
      return `SELECT id FROM t WHERE ${l} <> ALL (ARRAY[${rs.join(", ")}]) ORDER BY id`;
    case "not in subquery":
      return `SELECT 1 FROM t WHERE ${l} NOT IN (SELECT ${r} FROM t)`;
    case "not between":
      return `SELECT 1 FROM t WHERE ${l} NOT BETWEEN ${r} AND ${rs[1] ?? r}`;
    case "between symmetric":
      return `SELECT 1 FROM t WHERE ${l} BETWEEN SYMMETRIC ${r} AND ${rs[1] ?? r}`;
    case "join on":
      return `SELECT 1 FROM t AS a JOIN t AS b ON ${qualify("a", l)} ${operator} ${qualify("b", r)}`;
    case "check":
      return `CREATE TABLE ck (x ${typeOf(l)}, y ${typeOf(r)}, CHECK (x ${operator} y))`;
    case "like":
      return `SELECT 1 FROM t WHERE ${l} LIKE ${r}`;
    case "ilike":
      return `SELECT 1 FROM t WHERE ${l} ILIKE ${r}`;
    case "similar to":
      return `SELECT 1 FROM t WHERE ${l} SIMILAR TO ${r}`;
  }
}

/** A column of `t` under an alias; an untyped left operand stays as written. */
function qualify(alias: string, operand: string): string {
  return /^[a-z]+$/u.test(operand) ? `${alias}.${operand}` : operand;
}

/** The declared type of a column of `t` (for CHECK's table); an untyped operand is text. */
function typeOf(column: string): string {
  const declared: Record<string, string> = { vc: "varchar(10)", ch: "char(5)" };
  const entry = Object.entries(COLUMNS).find(([, name]) => name === column);
  return declared[column] ?? entry?.[0] ?? "text";
}

/** Left operands that are not columns: PostgreSQL types them from the other side. */
const UNTYPED = ["'1'", "NULL"] as const;

interface Outcome {
  readonly state: string;
  readonly message: string;
  /** The rows, when it succeeded and rows were asked for. */
  readonly rows?: unknown[];
}

let pg: PGlite;
let next: LoweredDatabase;
/** The real engine (storage, executor) over the same rows: what lowering accepts it runs. */
let core: StorageCore;

beforeAll(async () => {
  pg = new PGlite();
  await pg.waitReady;
  next = new LoweredDatabase();
  core = await new StorageCore().open();
  for (const sql of SETUP) {
    await pg.exec(sql);
    next.execute(sql);
    // The executor cannot cast into varchar(n) / char(n) yet (the text kernels): NULL there.
    await core.execute(
      sql.replace(/'1', '1', (1|2), /u, "NULL, NULL, $1, ").replace(/'a', 'a', /u, "NULL, NULL, "),
    );
  }
}, 60_000);

afterAll(async () => {
  await pg.close();
  await core.close();
});

async function authority(sql: string): Promise<Outcome> {
  // A CREATE TABLE is checked and then undone: the next one reuses its name.
  const ddl = sql.startsWith("CREATE");
  if (ddl) await pg.exec("BEGIN");
  try {
    const result = await pg.query<Record<string, unknown>>(sql);
    return { state: "00000", message: "", rows: result.rows.map((row) => Object.values(row)) };
  } catch (error) {
    const e = error as { code?: string; message?: string };
    return { state: e.code ?? String(error), message: e.message ?? "" };
  } finally {
    if (ddl) await pg.exec("ROLLBACK");
  }
}

/** The statement on the real engine: its rows, or its SQLSTATE. */
async function executed(sql: string): Promise<Outcome> {
  try {
    const result = await core.execute(sql);
    if (result.kind !== "rows") return { state: "00000", message: "" };
    return {
      state: "00000",
      message: "",
      rows: oracleRows(result as NextRows).map((row) =>
        Object.values(row).map((v) => (typeof v === "bigint" ? Number(v) : v)),
      ),
    };
  } catch (error) {
    const e = error as { sqlState?: string; message?: string };
    return { state: e.sqlState ?? String(error), message: e.message ?? "" };
  }
}

function lowered(sql: string, rows: boolean): Outcome {
  try {
    next.lower(sql);
    if (!rows) return { state: "00000", message: "" };
    let values: Array<Record<string, unknown>>;
    try {
      values = next.execute(sql);
    } catch (error) {
      // The reference interpreter's own gaps (a cast it does not cover): lowering agreed, the
      // rows are not compared.
      if (error instanceof ReferenceUnsupportedError) return { state: "00000", message: "" };
      throw error;
    }
    return {
      state: "00000",
      message: "",
      rows: values.map((row) =>
        Object.values(row).map((v) => (typeof v === "bigint" ? Number(v) : v)),
      ),
    };
  } catch (error) {
    const e = error as { sqlState?: string; message?: string };
    return { state: e.sqlState ?? String(error), message: e.message ?? "" };
  }
}

/** Compares the SQLSTATE, the message where PostgreSQL finds no operator, and with `rows` the
 * rows when both succeed. */
async function compare(
  sql: string,
  reach?: ReturnType<typeof createReach>,
  rows = false,
): Promise<void> {
  const expected = await authority(sql);
  let actual = lowered(sql, rows);
  // A value PostgreSQL refuses while it analyses (an untyped '1' as a timestamptz, 22007),
  // next/ refuses when it runs the statement: the outcome is compared, not its stage.
  if (actual.state === "00000" && expected.state !== "00000" && sql.startsWith("SELECT")) {
    const run = await executed(sql);
    if (run.state !== "00000") {
      actual = run;
      reach?.reach("refused at execution");
    }
  }
  // Stated exception (not an operator question): where PostgreSQL refuses the untyped literal
  // '1' as input of another operand's type (22007, "invalid input syntax"), it does so while it
  // analyses; next/ checks such a literal only when it runs it as a cast from text, which its
  // executor does not run yet (0A000, "casts from text come with the text kernels", #58's
  // scope), and may meet another operand's error first. Which refusal is not compared; that
  // next/ refuses is (lowering or execution that accepts it would be wrong).
  if (
    expected.state.startsWith("22") &&
    expected.message.startsWith("invalid input syntax") &&
    sql.includes("'1'")
  ) {
    expect(actual.state, `${sql}: refused (PostgreSQL: ${expected.message})`).not.toBe("00000");
    reach?.reach("untyped input refused");
    return;
  }
  expect(actual.state, `${sql}: ${actual.message} (PostgreSQL: ${expected.message})`).toBe(
    expected.state,
  );
  if (expected.state === "42883" && expected.message.startsWith("operator does not exist"))
    expect(actual.message, sql).toBe(expected.message);
  if (rows && expected.state === "00000" && actual.rows !== undefined) {
    expect(actual.rows, `${sql}: rows`).toEqual(expected.rows);
    reach?.reach("rows compared");
  }
  if (expected.state === "42883") reach?.reach("no operator");
  else if (expected.state === "42804") reach?.reach("kept 42804");
  else if (expected.state === "00000") reach?.reach("compared");
  // What lowering accepts, the engine runs (an executor gap is 0A000, said so).
  if (expected.state === "00000" && sql.startsWith("SELECT")) {
    const run = await executed(sql);
    if (run.state !== "0A000") {
      expect(run.state, `${sql}: executed: ${run.message}`).toBe("00000");
      // Its varchar(n) / char(n) columns hold NULLs (above): rows compared without them.
      if (rows && run.rows !== undefined && !/\b(vc|ch)\b/u.test(sql))
        expect(run.rows, `${sql}: executed rows`).toEqual(expected.rows);
      reach?.reach("executed");
    }
  }
}

it("names the operator a comparison lacks, as PostgreSQL does", async () => {
  for (const sql of [
    "SELECT 1 FROM t WHERE i = true",
    "SELECT 1 FROM t WHERE b = 1",
    "SELECT 1 FROM t WHERE i != true",
    "SELECT 1 FROM t WHERE i IN (1, true)",
    "SELECT 1 FROM t WHERE i BETWEEN 1 AND true",
    "SELECT 1 FROM t WHERE i IS DISTINCT FROM true",
    "SELECT 1 FROM t WHERE i = ANY (SELECT b FROM t)",
    "SELECT 1 FROM t WHERE i < ALL (SELECT b FROM t)",
    "SELECT 1 FROM t WHERE i IN (SELECT b FROM t)",
    "SELECT CASE i WHEN true THEN 1 END FROM t",
    "SELECT NULLIF(i, true) FROM t",
    "SELECT 1 FROM t WHERE d = 1",
    "SELECT 1 FROM t WHERE ts > true",
    "SELECT 1 FROM t WHERE s = 1",
    "SELECT 1 FROM t WHERE i = s",
    "SELECT 1 FROM t WHERE n = b",
    "SELECT 1 FROM t WHERE (i, b) = (1, 1)",
  ])
    await compare(sql);
});

it("compares an IN list without a common type item by item, as PostgreSQL does", async () => {
  // PostgreSQL (transformAExprIn): one comparison in the list's common type, else each item on
  // its own, OR-ed (AND-ed for NOT IN) in list order.
  for (const sql of [
    "SELECT 1 IN (1, 1.5::float8, '1') AS r",
    "SELECT 'a'::text IN ('a', 1::text) AS r",
    "SELECT 1::int2 IN (1::int8, 1::numeric) AS r",
    "SELECT 1::int2 IN (2::int8, 1::numeric) AS r",
    "SELECT '1' IN (1, true) AS r",
    "SELECT '0' IN (1, false) AS r",
    "SELECT 't' NOT IN (1, false) AS r",
    "SELECT 1 IN (1, 1.5::float8, true) AS r",
    "SELECT 1 NOT IN (1, 1.5::float8, true) AS r",
    "SELECT 1 IN (1.5::float8, 'x'::text) AS r",
    "SELECT 1 IN (1::numeric, '2020-01-01'::date) AS r",
    "SELECT id FROM t WHERE d IN (ts, s) ORDER BY id",
    "SELECT id FROM t WHERE i IN (g, n, f) ORDER BY id",
    "SELECT id FROM t WHERE i NOT IN (g, n) ORDER BY id",
    "SELECT id FROM t WHERE vc IN (s, ch) ORDER BY id",
    "SELECT id FROM t WHERE vc = 1",
    "SELECT id FROM t WHERE ch = b",
  ])
    await compare(sql, undefined, true);
});

it("answers every form Neon's review of #63 named, as PostgreSQL does", async () => {
  for (const sql of [
    // P1: IS [NOT] TRUE/FALSE needs a boolean (42804), not an operator.
    "SELECT 1 FROM t WHERE i IS TRUE",
    "SELECT 1 FROM t WHERE i IS NOT TRUE",
    "SELECT 1 FROM t WHERE s IS FALSE",
    "SELECT 1 FROM t WHERE i IS NOT FALSE",
    "SELECT 1 FROM t WHERE b IS TRUE",
    "SELECT 1 FROM t WHERE i IS DISTINCT FROM b",
    // P1: = ANY / <> ALL over an ARRAY constructor build the array first (42804).
    "SELECT '1' = ANY (ARRAY[1, true])",
    "SELECT '1' <> ALL (ARRAY[1, true])",
    "SELECT NULL = ANY (ARRAY[1, true])",
    "SELECT 1 FROM t WHERE i = ANY (ARRAY[1, true])",
    "SELECT 1 FROM t WHERE i <> ALL (ARRAY[1, true])",
    "SELECT 1 FROM t WHERE i = ANY (ARRAY[true, false])",
    "SELECT 1 FROM t WHERE i = ANY (ARRAY[1, 2])",
    "SELECT '1' IN (1, true)",
    // P2: a parameter this expression fixed to another type (42P08).
    "SELECT 1 FROM t WHERE $1 IN (i, b)",
    "SELECT 1 FROM t WHERE $1 IN (1, true)",
    "SELECT 1 FROM t WHERE $1 NOT IN (1, true)",
    "SELECT 1 FROM t WHERE $1 = ($1 = 1)",
    "SELECT NULLIF($1, ($1 = 1)) FROM t",
    "SELECT 1 FROM t WHERE $1 IS DISTINCT FROM ($1 = 1)",
    "SELECT 1 FROM t WHERE b IN ($1, ($1 = 1))",
    "SELECT 1 FROM t WHERE $1 = i AND $1 = b",
    // P2: json has no comparison operator (42883 at lowering).
    "SELECT 1 FROM t WHERE j = j",
    "SELECT 1 FROM t WHERE j < j",
    "SELECT 1 FROM t WHERE j = jb",
    "SELECT 1 FROM t WHERE jb = j",
    "SELECT 1 FROM t WHERE j IS DISTINCT FROM j",
    "SELECT NULLIF(j, j) FROM t",
    // P2: LIKE, ILIKE and SIMILAR TO are operators (42883).
    "SELECT 1 FROM t WHERE i LIKE '1%'",
    "SELECT 1 FROM t WHERE s LIKE 1",
    "SELECT 1 FROM t WHERE i ILIKE '1%'",
    "SELECT 1 FROM t WHERE i NOT LIKE '1%'",
    "SELECT 1 FROM t WHERE i SIMILAR TO '1%'",
    "SELECT 1 FROM t WHERE i SIMILAR TO vc",
    // P2: NOT IN (subquery) is NOT (x = ANY (subquery)): its message names =.
    "SELECT 1 FROM t WHERE i NOT IN (SELECT b FROM t)",
    "SELECT 1 FROM t WHERE i NOT IN (SELECT b FROM t LIMIT 1)",
    "SELECT 1 FROM t WHERE i <> ALL (SELECT b FROM t)",
  ])
    await compare(sql);
});

it("answers every form Neon's re-check of #63 (a1f129e4) named, as PostgreSQL does", async () => {
  for (const sql of [
    // P1: against an untyped NULL, IS [NOT] DISTINCT FROM is IS [NOT] NULL (no operator).
    "SELECT id FROM t WHERE j IS DISTINCT FROM NULL ORDER BY id",
    "SELECT id FROM t WHERE j IS NOT DISTINCT FROM NULL ORDER BY id",
    "SELECT id FROM t WHERE NULL IS DISTINCT FROM j ORDER BY id",
    "SELECT id FROM t WHERE NULL IS NOT DISTINCT FROM j ORDER BY id",
    "SELECT id FROM t WHERE i IS DISTINCT FROM NULL ORDER BY id",
    "SELECT NULL IS DISTINCT FROM NULL",
    // ... but a typed NULL is an operand: json has no = (42883).
    "SELECT id FROM t WHERE j IS DISTINCT FROM NULL::json ORDER BY id",
    // P2: the ARRAY is built first: its element type, its items coerced to it.
    "SELECT id FROM t WHERE i = ANY (ARRAY['1', '2']) ORDER BY id",
    "SELECT id FROM t WHERE i <> ALL (ARRAY['1', '2']) ORDER BY id",
    "SELECT id FROM t WHERE i = ANY (ARRAY[NULL, NULL]) ORDER BY id",
    "SELECT id FROM t WHERE i = ANY (ARRAY[NULL, '1']) ORDER BY id",
    "SELECT id FROM t WHERE b = ANY (ARRAY['1', '2']) ORDER BY id",
    "SELECT id FROM t WHERE s = ANY (ARRAY[1, 'x']) ORDER BY id",
    "SELECT id FROM t WHERE i = ANY (ARRAY['x', true]) ORDER BY id",
    "SELECT id FROM t WHERE i = ANY (ARRAY[$1, ($1 = true)])",
    "SELECT id FROM t WHERE i = ANY (ARRAY[1, 1.5, true])",
    "SELECT id FROM t WHERE j = ANY (ARRAY[NULL, NULL])",
    "SELECT id FROM t WHERE i = ANY (ARRAY[1, '2']) ORDER BY id",
    "SELECT id FROM t WHERE s = ANY (ARRAY['a', NULL]) ORDER BY id",
    // P2: = ANY (ARRAY[...]) and IN (...) are one expression to GROUP BY (no AST mark leaks
    // into the query identity).
    "SELECT (i IN (1, 2)) FROM t GROUP BY (i = ANY (ARRAY[1, 2])) ORDER BY 1",
    "SELECT (i = ANY (ARRAY[1, 2])) FROM t GROUP BY (i IN (1, 2)) ORDER BY 1",
    // P3: json and jsonb in one ARRAY (42846, the first item's type kept).
    "SELECT id FROM t WHERE i = ANY (ARRAY[j, jb])",
    "SELECT id FROM t WHERE i = ANY (ARRAY[jb, j])",
    // P3: GREATEST and LEAST compare: json has no comparison function (42883).
    "SELECT GREATEST(j, j) FROM t",
    "SELECT LEAST(j, j) FROM t",
    "SELECT GREATEST(jb, jb) FROM t",
  ])
    await compare(sql, undefined, true);
});

it("keeps 42804 where PostgreSQL matches result types (UNION, CASE results)", async () => {
  for (const sql of [
    "SELECT i FROM t UNION SELECT b FROM t",
    "SELECT CASE WHEN i = 1 THEN 1 ELSE true END FROM t",
    "SELECT COALESCE(i, true) FROM t",
  ])
    await compare(sql);
});

for (const seed of seedsFor("next-operator-sqlstate", [0x6501, 0x6502]))
  it(`agrees with PostgreSQL on comparisons of every type pair (seed ${String(seed)})`, async () => {
    const reach = createReach("operator SQLSTATE", OPERATOR_SQLSTATE_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.constantFrom(...FORMS),
        fc.constantFrom(...OPERATORS),
        fc.oneof(
          {
            weight: 4,
            arbitrary: fc.constantFrom(...TYPES).map((type) => COLUMNS[type] as string),
          },
          { weight: 1, arbitrary: fc.constantFrom<string>(...UNTYPED) },
        ),
        // Right operands: columns, or an untyped literal (NULL or '1') on that side too.
        fc.array(
          fc.oneof(
            {
              weight: 6,
              arbitrary: fc.constantFrom(...TYPES).map((type) => COLUMNS[type] as string),
            },
            { weight: 1, arbitrary: fc.constantFrom<string>(...UNTYPED) },
          ),
          { minLength: 1, maxLength: 3 },
        ),
        async (form, operator, left, rights) => {
          reach.beginHistory();
          const list =
            form === "in list" ||
            form === "not in list" ||
            form === "any array" ||
            form === "all array";
          if (list && rights.length > 1) reach.reach("multi-item list");
          const untyped = (UNTYPED as readonly string[]).includes(left);
          // Divergence `simple-case-untyped-test`: PostgreSQL types an untyped CASE test as
          // text; next/'s front end desugars CASE x WHEN v before lowering sees the test.
          if (untyped && form === "case when") return;
          if (untyped) reach.reach("untyped left");
          if (rights.some((right) => (UNTYPED as readonly string[]).includes(right)))
            reach.reach("untyped right");
          await compare(statement(form, operator, left, rights), reach, list);
        },
      ),
      { seed, numRuns: 400 },
    );
    reach.assertReached();
  }, 120_000);
