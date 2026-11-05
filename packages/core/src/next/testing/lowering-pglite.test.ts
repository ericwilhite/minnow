/**
 * Lowering against PostgreSQL itself (PGlite): every finding of review B6-p2a, each probe a case.
 * The same setup and statement run in PGlite and through lowering + the IR reference interpreter
 * (lowering-harness.ts); the outcome must match: the same SQLSTATE, or the same rows and result
 * types. PostgreSQL is the authority here, so this evidence shares no code with lowering.
 */
import { PGlite } from "@electric-sql/pglite";
import { NAME_RESOLUTION_SETUP, NAME_RESOLUTION_CASES } from "../../testing/conformance-source.js";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { LoweredDatabase } from "./lowering-harness.js";
import {
  check as checkIn,
  nextOutcome,
  pgOutcome as pgOutcomeIn,
  type Case,
} from "./pglite-oracle.js";

const check = (setup: readonly string[], cases: readonly Case[]) => checkIn(pg, setup, cases);
const pgOutcome = (sql: string) => pgOutcomeIn(pg, sql);

let pg: PGlite;

beforeAll(async () => {
  pg = new PGlite();
  await pg.waitReady;
}, 60_000);

afterAll(async () => {
  await pg.close();
});

/** PostgreSQL type OIDs → the base of our `sqlType` (typmods are not in a result's OID). */
const BASE = [
  "CREATE TABLE t (id integer PRIMARY KEY, v varchar(2), x bigint, g bigint GENERATED ALWAYS AS (x * 2) STORED)",
  "CREATE TABLE onlyg (g integer GENERATED ALWAYS AS (1) STORED)",
  "CREATE TABLE u (v varchar(2) PRIMARY KEY)",
  "CREATE TABLE f (id integer PRIMARY KEY, x double precision)",
  "CREATE SEQUENCE s",
  "INSERT INTO t (id, v, x) VALUES (1, 'ab', 5)",
  "INSERT INTO u VALUES ('ab')",
  "INSERT INTO f VALUES (1, 1.5)",
];

describe("lowering agrees with PostgreSQL on review B6-p2a's findings", () => {
  it("1: a VARCHAR key is never truncated into a false match", async () => {
    const next = await check(BASE, [
      { sql: "SELECT v FROM u WHERE v = 'abc'" },
      { sql: "SELECT v FROM u WHERE v = 'ab'" },
      { sql: "SELECT id FROM t WHERE v = 'abz'" },
    ]);
    // The point read still uses the key, with the untruncated constant.
    const lowered = next.lower("SELECT v FROM u WHERE v = 'abc'");
    expect(lowered.kind === "query" && lowered.pointRead).toBe(true);
  });

  it("2: constants keep their types until overloads are resolved", async () => {
    await check(BASE, [
      { sql: "SELECT 32768::smallint" },
      { sql: "SELECT 2147483647 + 1" },
      { sql: "SELECT 1::bigint", render: ["bigint"] },
      { sql: "SELECT 1.9::integer" },
      { sql: "SELECT 2147483648 AS a, CAST(2147483648 AS bigint) AS b" },
      { sql: "SELECT 2 + 3 * 4 AS n" },
      { sql: "INSERT INTO t (id, x) VALUES (2, 2147483647 + 1)" },
      { sql: "INSERT INTO t (id, x) VALUES (3, 32768::smallint)" },
    ]);
  });

  it("3: an assigned volatile value is evaluated once for the generated columns", async () => {
    await check(BASE, [
      { sql: "UPDATE t SET x = nextval('s') RETURNING x, g" },
      { sql: "SELECT id, x, g FROM t" },
    ]);
  });

  it("4: string assignments never truncate (22001)", async () => {
    await check(BASE, [
      { sql: "INSERT INTO t (id, v) VALUES (3, 'abc')" },
      { sql: "INSERT INTO u VALUES ('xyz')" },
      { sql: "UPDATE t SET v = 'xyz'" },
      { sql: "SELECT CAST('xyz' AS varchar(2)) AS c" },
    ]);
  });

  it("5: zero-argument functions resolve to their own result types", async () => {
    await check(BASE, [
      { sql: "SELECT random() >= 0 AS r, length(gen_random_uuid()::text) AS n" },
      { sql: "SELECT random() AS r, gen_random_uuid() AS u", typesOnly: true },
      { sql: "UPDATE f SET x = random() RETURNING x", typesOnly: true },
    ]);
  });

  it("6: generated columns are typed, recomputed by batch upserts, and immutable", async () => {
    const next = await check(BASE, [
      { sql: "INSERT INTO onlyg DEFAULT VALUES RETURNING *" },
      { sql: "CREATE TABLE bad (x double precision GENERATED ALWAYS AS (random()) STORED)" },
      { sql: "CREATE TABLE bad2 (x text GENERATED ALWAYS AS (random()) STORED)" },
      { sql: "CREATE TABLE bad3 (x timestamptz GENERATED ALWAYS AS (now()) STORED)" },
    ]);
    // A batch upsert of x recomputes g, as INSERT … ON CONFLICT DO UPDATE does in PostgreSQL.
    await pg.exec(
      "INSERT INTO t (id, x) VALUES (1, 7) ON CONFLICT (id) DO UPDATE SET x = EXCLUDED.x",
    );
    next.write({ table: "t", op: "upsert", names: ["id", "x"] }, [[1, 7]]);
    const expected = await pgOutcome("SELECT id, x, g FROM t ORDER BY id");
    expect(nextOutcome(next, "SELECT id, x, g FROM t ORDER BY id")).toEqual(expected);
  });

  it("7: an unqualified ON CONFLICT name is ambiguous (42702)", async () => {
    await check(BASE, [
      {
        sql: "INSERT INTO t (id, x) VALUES (1, 3) ON CONFLICT (id) DO UPDATE SET x = x + 1 RETURNING x",
      },
      {
        sql: "INSERT INTO t (id, x) VALUES (1, 3) ON CONFLICT (id) DO UPDATE SET x = t.x + EXCLUDED.x RETURNING x, g",
      },
    ]);
  });

  it("8: an explicit ::bigint renders bigint (R17.1)", async () => {
    await check(BASE, [
      {
        sql: "SELECT count(*)::bigint AS c, count(*) AS d, count(*) + 1 AS e FROM t",
        render: ["bigint", "number", "number"],
      },
      { sql: "SELECT sum(x) AS s FROM t", typesOnly: true, render: ["bigint"] },
    ]);
  });

  it("9: NULLIF types, float rounding and explicit casts follow PostgreSQL", async () => {
    await check(BASE, [
      { sql: "SELECT nullif(id, x) AS n FROM t" },
      { sql: "SELECT nullif(id, x) AS n FROM f" },
      {
        sql: "SELECT nullif(1::int4, 1.5::float8) AS a, nullif(1::int2, 2::int8) AS b, nullif(1.5::float8, 1::int4) AS d",
      },
      { sql: "SELECT nullif(1, 2.2) AS b", typesOnly: true },
      {
        sql: "SELECT 2.5::float8::int4 AS a, 3.5::float8::int4 AS b, (-2.5)::float8::int4 AS c, 2.5::numeric::int4 AS d",
      },
      { sql: "SELECT round(2.5::float8) AS a, round(3.5::float8) AS b" },
      { sql: "SELECT CAST(true AS date)" },
      { sql: "SELECT CAST(1 AS date)" },
      { sql: "SELECT CAST(1::int8 AS bool)" },
      { sql: "SELECT CAST(true AS int4) AS a, CAST(1 AS bool) AS b" },
      { sql: "SELECT '1' + '2'" },
      { sql: "SELECT '1' + 2 AS a, 1 + '2' AS b" },
    ]);
  });

  it("10: a composite IN over a NULL is FALSE in PostgreSQL; lowering does not lower it yet", async () => {
    // The mark-join law (ir.ts, R20.1) is the joins track's; lowering emits no mark join yet.
    expect(await pgOutcome("SELECT (1, 2) IN (SELECT 3, NULL::int) AS m")).toMatchObject({
      rows: [[false]],
    });
    expect(() =>
      new LoweredDatabase().lower("SELECT (1, 2) IN (SELECT 3, NULL::int) AS m"),
    ).toThrow();
  });

  it("TRUNCATE is its own statement; CREATE TABLE AS supports WITH [NO] DATA", async () => {
    await check(BASE, [
      { sql: "CREATE TABLE copy AS SELECT id, v FROM t WITH NO DATA" },
      { sql: "SELECT count(*) AS n FROM copy" },
      { sql: "CREATE TABLE copy2 AS SELECT id, v FROM t WITH DATA" },
      { sql: "SELECT id, v FROM copy2" },
      { sql: "TRUNCATE t" },
      { sql: "SELECT count(*) AS n FROM t" },
    ]);
  });
});

describe("name errors before grouping errors", () => {
  it("matches PostgreSQL messages for the same cases as the shipped engine", async () => {
    schema += 1;
    await pg.exec(`CREATE SCHEMA s${String(schema)}; SET search_path TO s${String(schema)}`);
    const next = new LoweredDatabase();
    for (const sql of NAME_RESOLUTION_SETUP) {
      await pg.exec(sql);
      next.execute(sql);
    }
    for (const sql of NAME_RESOLUTION_CASES) {
      let expected = "ok";
      try {
        await pg.query(sql);
      } catch (error) {
        expected = (error as Error).message;
      }
      let actual = "ok";
      try {
        next.lower(sql);
      } catch (error) {
        actual = (error as Error).message;
      }
      expect(actual, sql).toBe(expected);
    }
  });
});
