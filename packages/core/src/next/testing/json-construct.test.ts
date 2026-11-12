/** PostgreSQL wire text is the authority: JSON.parse would erase all spelling/spacing bugs. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { afterAll, beforeAll, expect, it, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import {
  createReach,
  JSON_CONSTRUCT_REACH,
  JSON_CONSTRUCT_NUMERIC_REACH,
  JSON_EXCEPT_RESULT_BOUND_REACH,
  JSON_CONSTRUCT_ARITY_REACH,
} from "../../testing/reach.js";
import { sqlLiteral } from "../exec/kernels/test-support.js";
import { Governor } from "../memory/governor.js";
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
const values = [
  "NULL::text",
  "TRUE",
  "FALSE",
  "'-9223372036854775808'::int8",
  "'9007199254740993.01000'::numeric",
  "'-0.00000'::numeric",
  "'NaN'::numeric",
  "'Infinity'::float8",
  "'-Infinity'::float4",
  "'-0'::float8",
  "'1e-5'::float8",
  "'1e15'::float8",
  "'1.23456789'::float4",
  '\'{ "a": 1.00, "a": 2 }\'::json',
  "'[1,null,2]'::jsonb",
  "DATE '2000-02-29'",
  "TIMESTAMP '2000-02-29 12:34:56.123456'",
];
async function compare(
  sql: string,
  executedSql = sql,
  params: ReadonlyArray<string | number> = [],
) {
  const authority = await pg.query(sql),
    actual = await core.execute(executedSql, params);
  if (!("batches" in actual)) throw new Error("Expected query");
  expect(oracleRows(actual), sql).toEqual(authority.rows);
}
it.each(seedsFor("next-json-construction", [0xb6d0, 0xb6d1]))(
  "preserves heterogeneous typed construction and exact text (seed %i)",
  async (seed) => {
    const reach = createReach<string>("JSON typed construction", JSON_CONSTRUCT_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 0, max: values.length - 1 }),
        fc.integer({ min: -999999, max: 999999 }),
        fc.integer({ min: 0, max: 15 }),
        fc.constantFrom("汉😃", '"\\\n\r\t\b\f\u0001', "duplicate", "/"),
        fc.integer({ min: 0, max: 3 }),
        async (index, coefficient, scale, key, shape) => {
          reach.beginHistory();
          const value = values[index] ?? "NULL",
            num = `'${String(coefficient)}.${"0".repeat(scale)}100'::numeric`,
            name =
              shape === 0
                ? "json_build_object"
                : shape === 1
                  ? "json_object"
                  : shape === 2
                    ? "json_build_array"
                    : "json_array";
          const argumentsSql =
            shape < 2
              ? `${quote(key)}${shape === 1 ? " VALUE " : ","}${value},${quote(key)}${shape === 1 ? " VALUE " : ","}${num},'text'${shape === 1 ? " VALUE " : ","}${quote(key)}`
              : `${value},${num},NULL,${quote(key)}`;
          const sql = `SELECT ${name}(${argumentsSql})::text j`;
          // Bind typed specials: general TEXT→NUMERIC/FLOAT execution remains an existing refusal.
          const special =
            index === 6
              ? "NaN"
              : index === 7
                ? Infinity
                : index === 8
                  ? -Infinity
                  : index === 9
                    ? -0
                    : index === 10
                      ? 1e-5
                      : index === 11
                        ? 1e15
                        : index === 12
                          ? Math.fround(1.23456789)
                          : undefined;
          const target =
            index === 6 ? "numeric" : index === 8 || index === 12 ? "float4" : "float8";
          await compare(
            sql,
            special === undefined ? sql : sql.replace(value, "$1::" + target),
            special === undefined ? [] : [special],
          );
          reach.reach(name);
          reach.reach("exact text");
          if (shape < 2) reach.reach("duplicate keys");
          if (index === 0) reach.reach("NULL handling");
          if (index === 4) reach.reach("int8 precision and numeric scale");
          if (index === 13 || index === 14) reach.reach("nested typed JSON");
          if (key.includes("汉")) reach.reach("UTF8");
          if (key.includes("\\")) reach.reach("escapes");
        },
      ),
      {
        seed,
        numRuns: 45,
        examples: Array.from(
          { length: values.length },
          (_, i) =>
            [i, -123, 5, i % 2 ? "汉😃" : '"\\\n\r\t\b\f\u0001', i % 4] as [
              number,
              number,
              number,
              string,
              number,
            ],
        ),
      },
    );
    reach.assertReached();
  },
  120000,
);
it.each([
  "SELECT json_build_object()::text j,json_build_array()::text a,json_array()::text s",
  "SELECT to_json('1.2300'::numeric)::text j",
  "SELECT json_build_object('x',json_build_array(1,NULL,'a'))::text j",
  "SELECT json_build_array(CASE WHEN TRUE THEN json_build_object('x',1) ELSE NULL END)::text j",
])("matches nesting/empty/one-value construction: %s", async (sql) => compare(sql));
it.each([
  "SELECT json_build_object(NULL,1)",
  "SELECT json_build_object(1)",
  "SELECT to_json(NULL)",
  "SELECT to_jsonb('unknown')",
  "SELECT json_build_object('{}'::json,1)",
])("matches runtime key SQLSTATE: %s", async (sql) => {
  let code: string | undefined;
  try {
    await pg.query(sql);
  } catch (e) {
    code = (e as { code: string }).code;
  }
  expect(code).toBeDefined();
  await expect(core.execute(sql)).rejects.toMatchObject({ sqlState: code });
});

it.each(seedsFor("next-json-construction-floats", [0xb6d4, 0xb6d5]))(
  "preserves PG float spelling and scalar keys (seed %i)",
  async (seed) => {
    await fc.assert(
      fc.asyncProperty(fc.double(), fc.float(), async (d, r) => {
        const sql = `SELECT json_build_array(${sqlLiteral("float8", d)},${sqlLiteral("float4", r)})::text j,json_build_object(${sqlLiteral("float8", d)},TRUE)::text k`;
        await compare(
          sql,
          "SELECT json_build_array($1::float8,$2::float4)::text j,json_build_object($1::float8,TRUE)::text k",
          [d, r],
        );
      }),
      {
        seed,
        numRuns: 64,
        examples: [
          [-0, -0],
          [NaN, NaN],
          [Infinity, Infinity],
          [-Infinity, -Infinity],
          [1e15, Math.fround(1e6)],
          [999999999999999.9, Math.fround(999999.9)],
          [1e-5, Math.fround(1e-5)],
        ],
      },
    );
  },
  120000,
);
it.each([
  "SELECT json_build_array(1,NULL)::text a,json_array(1,NULL)::text b",
  "SELECT jsonb_build_object('zz',1,'b',2,'zz',3)::text j",
  "SELECT jsonb_build_array(1,NULL,'x')::text j",
  "SELECT to_jsonb(1.2300)::text j",
  "SELECT json_object('a' VALUE '{}'::jsonb)::text j",
  "SELECT json_array('{}'::jsonb,NULL)::text j",
  "SELECT json_build_object(TRUE,1,17,2)::text j",
])("matches constructor identity: %s", async (sql) => compare(sql));

it("retains whole-row and array-type refusals until their typed consumers exist", async () => {
  await expect(core.execute("SELECT t FROM(SELECT 1 AS n)t")).rejects.toMatchObject({
    sqlState: "0A000",
  });
});

it.each(seedsFor("next-json-construction-numeric-wide", [0xb6d6, 0xb6d7]))(
  "keeps wide NUMERIC digits and display scale (seed %i)",
  async (seed) => {
    const reach = createReach<string>(
      "JSON constructor NUMERIC limits",
      JSON_CONSTRUCT_NUMERIC_REACH,
    );
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 0, max: 2000 }),
        fc.integer({ min: 0, max: 2000 }),
        fc.boolean(),
        async (digits, scale, negative) => {
          reach.beginHistory();
          const value =
            (negative ? "-" : "") + "1" + "2".repeat(digits) + "." + "0".repeat(scale) + "10";
          const sql = "SELECT json_build_object('n',$1::numeric)::text j";
          const expected = await pg.query(sql, [value]),
            actual = await core.execute(sql, [value]);
          if (!("batches" in actual)) throw new Error("Expected rows");
          expect(oracleRows(actual)).toEqual(expected.rows);
          reach.reach(negative ? "negative" : "positive");
          if (digits >= 2000) reach.reach("wide coefficient");
          if (digits === 131071) reach.reach("maximum integer digits");
          if (scale === 16381) reach.reach("maximum scale");
          reach.reach("exact text");
        },
      ),
      {
        seed,
        numRuns: 8,
        examples: [
          [2000, 2000, true],
          [0, 16381, false],
          [131071, 0, true],
        ],
      },
    );
    reach.assertReached();
  },
  120000,
);

it.each(seedsFor("next-json-construction-except-bound", [0xb6e0, 0xb6e1]))(
  "preserves large EXCEPT output across asymmetric JSON producers (seed %i)",
  async (seed) => {
    const reach = createReach<string>(
      "JSON retained EXCEPT output",
      JSON_EXCEPT_RESULT_BOUND_REACH,
    );
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 700000, max: 800000 }),
        fc.boolean(),
        async (n, binary) => {
          reach.beginHistory();
          const value = "\u0001".repeat(n),
            fn = binary ? "jsonb_build_object" : "json_build_object",
            sql = `SELECT ${fn}('v',$1::text)::text j EXCEPT SELECT ${fn}('v','small')::text j`;
          const governor = new Governor(64 << 20);
          const original = core.executor.prepare.bind(core.executor),
            prep = vi.spyOn(core.executor, "prepare").mockImplementation((plan) => {
              const p = original(plan);
              return { ...p, execute: (request) => p.execute({ ...request, governor }) };
            });
          try {
            const expected = (await pg.query<{ j: string }>(sql, [value])).rows,
              actual = await core.execute(sql, [value]);
            if (!("batches" in actual)) throw new Error("Expected rows");
            expect(oracleRows(actual)).toEqual(expected);
            expect(core.spill.bytesInUse).toBe(0);
            expect(governor.usedBytes).toBe(0);
          } finally {
            prep.mockRestore();
          }
          reach.reach(binary ? "JSONB" : "JSON");
          reach.reach("larger retained left");
          reach.reach("exact PostgreSQL");
          reach.reach("zero spill");
        },
      ),
      {
        seed,
        numRuns: 4,
        examples: [
          [700000, false],
          [700000, true],
        ],
      },
    );
    reach.assertReached();
  },
  120000,
);

it.each(seedsFor("next-json-construction-arity", [0xb6e2, 0xb6e3]))(
  "keeps PostgreSQL's build function arity boundary (seed %i)",
  async (seed) => {
    const reach = createReach<string>("JSON function arity", JSON_CONSTRUCT_ARITY_REACH);
    await fc.assert(
      fc.asyncProperty(fc.integer({ min: 96, max: 104 }), fc.boolean(), async (n, binary) => {
        reach.beginHistory();
        const sql = `SELECT ${binary ? "jsonb_build_array" : "json_build_array"}(${Array(n).fill("1").join(",")})::text j`;
        reach.reach(binary ? "JSONB" : "JSON");
        if (n === 100) reach.reach("at boundary");
        if (n <= 100) await compare(sql);
        else {
          await expect(pg.query(sql)).rejects.toMatchObject({ code: "54023" });
          await expect(core.execute(sql)).rejects.toMatchObject({ sqlState: "54023" });
          reach.reach("above boundary");
        }
        reach.reach("exact PostgreSQL");
      }),
      {
        seed,
        numRuns: 8,
        examples: [
          [100, false],
          [101, true],
        ],
      },
    );
    reach.assertReached();
  },
  120000,
);
