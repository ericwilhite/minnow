/** Typed inputs, sibling transitions, FILTER and transition-time errors use PG exactly. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { afterAll, beforeAll, expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import {
  createReach,
  JSON_AGGREGATE_TYPED_REACH,
  JSON_AGGREGATE_ERROR_REACH,
} from "../../testing/reach.js";
import { StorageCore } from "./storage-core.js";
import { oracleRows } from "./differential-adapter.js";
let pg: PGlite, core: StorageCore;
beforeAll(async () => {
  pg = new PGlite();
  await pg.waitReady;
  core = await new StorageCore().open();
}, 60000);
afterAll(async () => {
  await core.close();
  await pg.close();
});
const quote = (s: string) => "'" + s.replaceAll("'", "''") + "'";
const TYPES = ["int4", "numeric", "float4", "float8", "json", "jsonb"] as const;
function value(type: number, i: number): string {
  if (type === 0) return String(i - 9);
  if (type === 1) return quote(i % 3 ? `${String(i - 9)}.5000` : "9007199254740993.2500");
  if (type === 2)
    return quote(["35675512", "2097152.25", "-0", "1e-40", "Infinity", "1.25"][i % 6] ?? "0");
  if (type === 3)
    return quote(
      ["-24223037550190192", "-21473691544319048", "-0", "5e-324", "NaN", "0.01"][i % 6] ?? "0",
    );
  return quote(`{ "x":1.20,"x":${String(i)},"a":[null,3] }`);
}
it.each(seedsFor("next-json-aggregate-typed", [0xb70a, 0xb70b]))(
  "keeps typed values, FILTER and ordinary siblings exact (seed %i)",
  async (seed) => {
    const reach = createReach<string>("typed JSON aggregation", JSON_AGGREGATE_TYPED_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 0, max: 5 }),
        fc.integer({ min: 0, max: 24 }),
        fc.boolean(),
        fc.boolean(),
        async (type, n, desc, filter) => {
          reach.beginHistory();
          const rows = Array.from(
            { length: Math.max(1, n) },
            (_, i) =>
              `(${String(i)},${i % 7 === 0 ? "NULL" : value(type, i)}::${TYPES[type] ?? "int4"})`,
          ).join(",");
          await core.execute("DROP TABLE IF EXISTS typed_values");
          await core.execute(
            `CREATE TABLE typed_values(n INTEGER,v ${type >= 4 ? "text" : (TYPES[type] ?? "int4")})`,
          );
          await pg.exec("DROP TABLE IF EXISTS typed_values");
          await pg.exec(
            `CREATE TABLE typed_values(n INTEGER,v ${type >= 4 ? "text" : (TYPES[type] ?? "int4")})`,
          );
          // The write path binds the declared SQL type rather than relying on VALUES coercion.
          const entries = Array.from({ length: n }, (_, i) => ({
            n: i,
            v:
              i % 7 === 0
                ? null
                : type === 0
                  ? i - 9
                  : type === 1
                    ? i % 3
                      ? `${String(i - 9)}.5000`
                      : "9007199254740993.2500"
                    : type === 2 || type === 3
                      ? Number(value(type, i).slice(1, -1))
                      : value(type, i).slice(1, -1),
          }));
          if (entries.length) await core.writeBatch("insertBatch", "typed_values", entries);
          if (n) await pg.exec(`INSERT INTO typed_values VALUES ${rows}`);
          const arg = type >= 4 ? `v::${TYPES[type] ?? "json"}` : "v";
          const filtered = filter ? " FILTER(WHERE n%3<>0)" : "";
          const sql = `SELECT json_agg(${arg} ORDER BY n ${desc ? "DESC" : "ASC"})${filtered}::text a,jsonb_agg(${arg} ORDER BY n)${filtered}::text b,COUNT(*)::int n,SUM(n)::int s FROM typed_values`;
          const expected = (await pg.query(sql)).rows,
            actual = await core.execute(sql);
          if (!("batches" in actual)) throw new Error("Expected rows");
          expect(oracleRows(actual), sql).toEqual(expected);
          reach.reach(TYPES[type] ?? "int4");
          reach.reach(desc ? "descending" : "ascending");
          if (filter) reach.reach("FILTER");
          if (n === 0) reach.reach("empty group");
          reach.reach("ordinary siblings");
          reach.reach("exact text");
          if (type < 4) {
            const distinct = `SELECT json_agg(DISTINCT v ORDER BY v ${desc ? "DESC" : "ASC"})::text a FROM typed_values`;
            const want = (await pg.query(distinct)).rows,
              got = await core.execute(distinct);
            if (!("batches" in got)) throw new Error("Expected distinct rows");
            expect(oracleRows(got), distinct).toEqual(want);
            reach.reach("DISTINCT typed scalar");
          }
        },
      ),
      {
        seed,
        numRuns: 32,
        examples: [
          [0, 0, false, false],
          [1, 8, true, true],
          [2, 8, false, true],
          [3, 8, true, false],
          [4, 8, false, false],
          [5, 8, true, true],
        ],
      },
    );
    reach.assertReached();
  },
  120000,
);
async function outcome(sql: string, pgSide: boolean): Promise<unknown> {
  try {
    if (pgSide) return { rows: (await pg.query(sql)).rows };
    const r = await core.execute(sql);
    if (!("batches" in r)) throw new Error("Expected query");
    return { rows: oracleRows(r) };
  } catch (error) {
    if (
      error === null ||
      typeof error !== "object" ||
      (!("sqlState" in error) && !("code" in error))
    )
      throw error;
    return {
      state:
        "code" in error && typeof error.code === "string" && !error.code.startsWith("MINNOW")
          ? error.code
          : "sqlState" in error
            ? error.sqlState
            : "missing state",
    };
  }
}
it.each(seedsFor("next-json-aggregate-errors", [0xb70e, 0xb70f]))(
  "matches ordered NULL-key, duplicate and conversion precedence (seed %i)",
  async (seed) => {
    const reach = createReach<string>(
      "JSON aggregate transition errors",
      JSON_AGGREGATE_ERROR_REACH,
    );
    await fc.assert(
      fc.asyncProperty(
        fc.boolean(),
        fc.boolean(),
        fc.boolean(),
        fc.integer({ min: -1, max: 4 }),
        fc.integer({ min: -1, max: 4 }),
        fc.boolean(),
        fc.boolean(),
        async (binary, unique, strict, nullAt, badAt, desc, allNull) => {
          reach.beginHistory();
          const fn =
            (binary ? "jsonb_object_agg" : "json_object_agg") +
            (unique ? "_unique" : "") +
            (strict ? "_strict" : "");
          const rows = Array.from(
            { length: 5 },
            (_, i) =>
              `(${String(i)},${i === nullAt ? "NULL::text" : quote(String(i % 2))},${i === badAt ? quote('["\\u0000"]') + "::json" : allNull || i % 2 === 0 ? "NULL::json" : "'1'::json"})`,
          ).join(",");
          const sql = `SELECT ${fn}(k,v ORDER BY n ${desc ? "DESC" : "ASC"})::text j FROM(VALUES ${rows})t(n,k,v)`;
          expect(await outcome(sql, false), sql).toEqual(await outcome(sql, true));
          reach.reach(binary ? "JSONB" : "JSON");
          if (unique) reach.reach("unique");
          if (strict) reach.reach("strict");
          if (allNull && unique && strict && badAt === -1)
            reach.reach("duplicate omitted NULL values");
          if (nullAt >= 0) reach.reach("NULL key");
          if (badAt >= 0 && binary) reach.reach("value conversion");
          reach.reach(desc ? "descending" : "ascending");
          reach.reach("PG SQLSTATE");
        },
      ),
      {
        seed,
        numRuns: 32,
        examples: [
          [false, true, true, -1, -1, false, true],
          [true, true, true, -1, -1, false, true],
          [true, false, false, 0, 4, false, false],
          [true, false, false, 4, 0, false, false],
          [true, false, false, 0, 4, true, false],
        ],
      },
    );
    reach.assertReached();
  },
  120000,
);

it("keeps LIMIT demand, record provenance and empty correlated defaults", async () => {
  const cases = [
    "SELECT row_to_json(NULL)::text a,row_to_json(ROW(1),NULL)::text b",
    "SELECT row_to_json(x,true)::text a,to_json(x)::text b FROM(SELECT ROW(1,'汉',ROW(NULL,'x'))x)t",
    "SELECT g,json_object_agg(k,v ORDER BY n)::text j FROM(VALUES(0,'a',1,0),(1,NULL,2,1))t(g,k,v,n) GROUP BY g LIMIT 1",
    "SELECT j FROM(SELECT json_object_agg(NULL::text,n)::text j FROM generate_series(1,8)t(n))s LIMIT 0",
    "SELECT c.n,(SELECT COALESCE(json_agg(r ORDER BY r.id),'[]') FROM(SELECT o.id,o.v FROM(VALUES(1,'a'),(2,'b'))o(id,v) WHERE o.id=c.n)r)::text j FROM(VALUES(1),(2),(3))c(n) ORDER BY c.n",
  ];
  for (const sql of cases) {
    const expected = (await pg.query(sql)).rows,
      result = await core.execute(sql);
    if (!("batches" in result)) throw new Error("Expected JSON rows");
    expect(oracleRows(result), sql).toEqual(expected);
  }
});
