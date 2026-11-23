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
} as const;
type Type = keyof typeof COLUMNS;
const TYPES = Object.keys(COLUMNS) as Type[];
const SETUP = [
  "CREATE TABLE t (id integer PRIMARY KEY, i integer, g bigint, b boolean, s text, d date, ts timestamp, n numeric, f double precision, iv interval, vc varchar(10), ch char(5))",
  "INSERT INTO t VALUES (1, 1, 1, true, '1', DATE '2020-01-01', TIMESTAMP '2020-01-01 00:00:00', 1, 1, NULL, '1', '1')",
  "INSERT INTO t VALUES (2, 2, 3, false, 'a', DATE '2020-01-02', TIMESTAMP '2020-01-02 12:00:00', 2.5, 2.5, NULL, 'a', 'a')",
  "INSERT INTO t VALUES (3, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)",
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
  }
}

interface Outcome {
  readonly state: string;
  readonly message: string;
  /** The rows, when it succeeded and rows were asked for. */
  readonly rows?: unknown[];
}

let pg: PGlite;
let next: LoweredDatabase;

beforeAll(async () => {
  pg = new PGlite();
  await pg.waitReady;
  next = new LoweredDatabase();
  for (const sql of SETUP) {
    await pg.exec(sql);
    next.execute(sql);
  }
}, 60_000);

afterAll(async () => {
  await pg.close();
});

async function authority(sql: string): Promise<Outcome> {
  try {
    const result = await pg.query<Record<string, unknown>>(sql);
    return { state: "00000", message: "", rows: result.rows.map((row) => Object.values(row)) };
  } catch (error) {
    const e = error as { code?: string; message?: string };
    return { state: e.code ?? String(error), message: e.message ?? "" };
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
  const actual = lowered(sql, rows);
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
  else if (expected.state === "00000") reach?.reach("compared");
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
        fc.constantFrom(...TYPES),
        fc.array(fc.constantFrom(...TYPES), { minLength: 1, maxLength: 3 }),
        async (form, operator, left, rights) => {
          reach.beginHistory();
          const list = form === "in list" || form === "not in list";
          if (list && rights.length > 1) reach.reach("multi-item list");
          await compare(
            statement(
              form,
              operator,
              COLUMNS[left],
              rights.map((type) => COLUMNS[type]),
            ),
            reach,
            list,
          );
        },
      ),
      { seed, numRuns: 200 },
    );
    reach.assertReached();
  }, 120_000);
