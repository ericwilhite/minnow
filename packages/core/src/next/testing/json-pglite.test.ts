/** JSON values retain PostgreSQL's wire bytes, rather than a JS parse/stringify approximation. */
import { PGlite } from "@electric-sql/pglite";
import { afterAll, beforeAll, expect, it } from "vitest";
import fc from "fast-check";
import { seedsFor } from "../../testing/seeds.js";
import {
  createReach,
  JSON_EXTRACTION_REACH,
  JSON_VALIDATION_REACH,
  JSON_MATERIALIZED_REACH,
  JSON_CURRENT_LEXICAL_REACH,
  JSON_KEY_TYPES_REACH,
  JSON_PARAMETER_ENVELOPE_REACH,
} from "../../testing/reach.js";
import { MinnowDatabase } from "../../engine/database.js";
import { MemoryBlockStore } from "../../storage/index.js";
import { currentJsonNumberText, QUERY_DIVERGENCES } from "./divergences.js";
import { StorageCore } from "./storage-core.js";
import { oracleRows } from "./differential-adapter.js";
let pg: PGlite, core: StorageCore;
beforeAll(async () => {
  pg = new PGlite();
  await pg.waitReady;
  core = await new StorageCore(32).open();
}, 60000);
afterAll(async () => {
  await core.close();
  await pg.close();
});
const quote = (s: string): string => "'" + s.replaceAll("'", "''") + "'";
it.each(seedsFor("next-json-extraction", [0xb640, 0xb641]))(
  "extracts raw JSON and decoded text exactly (seed %i)",
  async (seed) => {
    const reach = createReach<string>("JSON extraction", JSON_EXTRACTION_REACH);
    const examples: Array<[string, string | number | null, boolean, number]> = [
      [' { "n" : 9007199254740993, "n": 2.00 } ', "n", false, 0],
      ['{"n":9007199254740993}', "n", true, 0],
      ['{"n":1e+10000}', "n", false, 1],
      ['{"n":-0.000}', "n", true, 2],
      ['{"n":' + "[".repeat(128) + "0" + "]".repeat(128) + "}", "n", false, 0],
      ['{"n":1,"unused":"\\ud800"}', "n", false, 0],
      ['{"n":1,"unused":"\\u0000"}', "n", false, 0],
      ['{"n":null}', "n", false, 1],
      ['{"n":null}', "n", true, 0],
      ['{"a": 1, "\\u0061" : {" n": [ 1, 2 ]}}', "a", false, 2],
      ['["a",null,{"n": 1.00}]', -1, false, 0],
      ['["a",null,{"n": 1.00}]', 1, true, 1],
      ['{"n":"\\u0000"}', "n", true, 0],
      ['{"n":"\\ud800"}', "n", true, 2],
      ['["\\ud800",1]', 1, false, 0],
      ['["\\u0000",1]', 1, false, 1],
      ['["\\ud800",1]', 0, true, 2],
      ['"\\ud800"', 0, false, 0],
      ['{"n":"\\ud834\\udd1e\\n\\\\\\""}', "n", true, 0],
      ["[1,2]", "n", false, 0],
      ['{"n":1}', 0, false, 0],
      ["null", "n", true, 0],
      ['{"n":1}', null, false, 1],
      ["{}", "missing", true, 0],
    ];
    await fc.assert(
      fc.asyncProperty(
        fc.oneof(
          fc.constantFrom('["\\ud800",1]', '["\\u0000",1]'),
          fc
            .integer({ min: 1, max: 200 })
            .map((depth) => '{"n":' + "[".repeat(depth) + "0" + "]".repeat(depth) + "}"),
          fc
            .record({
              key: fc.constantFrom("n", "a", "'", "é", "中", "0"),
              value: fc.bigInt({ min: -999999999999999999999999n, max: 999999999999999999999999n }),
              scale: fc.integer({ min: 0, max: 6 }),
            })
            .map(
              ({ key, value, scale }) =>
                "{ " +
                JSON.stringify(key) +
                " : " +
                String(value) +
                (scale ? "." + "0".repeat(scale) : "") +
                ", " +
                JSON.stringify(key) +
                " : [ null, " +
                JSON.stringify(key) +
                " ] }",
            ),
          fc
            .array(fc.oneof(fc.integer(), fc.string({ maxLength: 30 }), fc.constant(null)), {
              maxLength: 12,
            })
            .map(JSON.stringify),
        ),
        fc.oneof(
          fc.constantFrom("n", "a", "'", "é", "中", "0", "missing"),
          fc.integer({ min: -15, max: 15 }),
          fc.constant(null),
        ),
        fc.boolean(),
        fc.integer({ min: 0, max: 2 }),
        async (document, key, text, scope) => {
          reach.beginHistory();
          const selector =
            key === null ? "NULL::text" : typeof key === "number" ? String(key) : quote(key);
          const expr = quote(document) + "::json " + (text ? "->>" : "->") + " " + selector;
          const sql =
            scope === 0
              ? "SELECT (" + expr + ")::text AS j"
              : scope === 1
                ? "SELECT j::text AS j FROM (SELECT " + expr + " AS j) q"
                : "WITH q AS (SELECT " + expr + " AS j) SELECT j::text AS j FROM q";
          let expected: unknown;
          try {
            expected = (await pg.query(sql)).rows;
          } catch (error) {
            const code = (error as { code?: string }).code;
            await expect(core.execute(sql)).rejects.toMatchObject({ sqlState: code });
            reach.reach("Unicode error");
            if (document.startsWith("[")) reach.reach("array Unicode");
            return;
          }
          const result = await core.execute(sql);
          if (!("batches" in result)) throw new Error("Expected JSON rows");
          expect(oracleRows(result), sql).toEqual(expected);
          reach.reach(text ? "decoded text" : "raw JSON");
          if (!text && key === "n" && document.startsWith('{"n":[['))
            reach.reach("nested raw output");
          reach.reach(["direct", "derived", "CTE"][scope] ?? "direct");
          if (key === null) reach.reach("NULL selector");
          if (typeof key === "number" && key < 0) reach.reach("negative index");
          if (document === '{"n":9007199254740993}') reach.reach("exact number text");
          if (document.includes("\\u0061")) reach.reach("duplicate escaped key");
          if (document.includes("null")) reach.reach("JSON null");
          if (document.includes('"missing"') || key === "missing") reach.reach("missing field");
        },
      ),
      { seed, numRuns: 80 + examples.length, examples },
    );
    reach.assertReached();
  },
  120000,
);

it("retains document bytes through stored, VALUES, view and CASE sources", async () => {
  await core.execute("CREATE TABLE json_source (id INTEGER PRIMARY KEY, doc TEXT)");
  await pg.exec("CREATE TABLE json_source (id INTEGER PRIMARY KEY, doc TEXT)");
  const docs = [' {"n" : 9007199254740993 } ', '{"n":"é\\n"}', '{"n":null}', null];
  await core.writeBatch(
    "insertBatch",
    "json_source",
    docs.map((doc, id) => ({ id, doc })),
  );
  for (let id = 0; id < docs.length; id++)
    await pg.query("INSERT INTO json_source VALUES ($1,$2)", [id, docs[id]]);
  await core.harness.path.recycleLog();
  await core.harness.path.quiesce();
  await core.execute("CREATE VIEW json_view AS SELECT id,doc::json AS j FROM json_source");
  await pg.exec("CREATE VIEW json_view AS SELECT id,doc::json AS j FROM json_source");
  const queries = [
    "SELECT id, doc::json ->> 'n' AS j FROM json_source ORDER BY id",
    "SELECT id,j ->> 'n' AS j FROM json_view ORDER BY id",
    "SELECT j ->> 'n' AS j FROM (VALUES ('{\"n\":2.00}'::json),('{\"n\":null}'::json)) q(j)",
    "SELECT id, (CASE WHEN id = 0 THEN doc::json ELSE '{\"n\":7}'::json END) ->> 'n' AS j FROM json_source ORDER BY id",
    "SELECT (CASE WHEN 1=1 THEN '{\"n\":7}'::json ELSE '{\"n\":\"\\ud800\"}'::json -> 'n' END)::text AS j",
    "SELECT id FROM json_source WHERE doc::json ->> 'n' IS NOT NULL ORDER BY id",
    "SELECT id, coalesce(doc::json, '{\"n\":9}'::json) ->> 'n' AS j FROM json_source ORDER BY id",
    "SELECT doc::json ->> $1::text AS j FROM json_source ORDER BY id",
    "SELECT doc::json ->> 'n' AS j FROM json_source LIMIT 0",
  ];
  for (const sql of queries) {
    const params = sql.includes("$1") ? ["n"] : [];
    const result = await core.execute(sql, params);
    if (!("batches" in result)) throw new Error("Expected JSON rows");
    expect(oracleRows(result), sql).toEqual((await pg.query(sql, params)).rows);
    if (sql.includes("WHERE doc::json")) {
      const prepared = core.prepare(sql);
      const ids = new Set<number>();
      const visit = (value: unknown): void => {
        if (value === null || typeof value !== "object" || ArrayBuffer.isView(value)) return;
        if ("id" in value && typeof value.id === "number" && "op" in value) ids.add(value.id);
        for (const child of Object.values(value)) visit(child);
      };
      visit(prepared.plan.root);
      for (const operator of core.lastProfile?.operators ?? [])
        expect(ids.has(operator.relId)).toBe(true);
    }
  }
  const invalid = "SELECT j::json ->> NULL::text AS j FROM (VALUES ('invalid'::text)) q(j)";
  await expect(pg.query(invalid)).rejects.toMatchObject({ code: "22P02" });
  await expect(core.execute(invalid)).rejects.toMatchObject({ sqlState: "22P02" });
  for (const sql of [
    "SELECT $1::text::json ->> 'n'",
    "SELECT j ->> 'n' FROM (SELECT $1::text::json AS j) q",
    "SELECT (doc || doc)::json ->> 'n' FROM json_source",
  ])
    expect(() => core.prepare(sql), sql).toThrow(
      expect.objectContaining({ sqlState: "0A000", stage: "execute", construct: "function" }),
    );
  expect(() => core.prepare("SELECT '{\"n\":1}'::jsonb ->> 'n'")).toThrow(
    expect.objectContaining({ sqlState: "0A000", stage: "execute", construct: "cast" }),
  );
}, 120000);

it.each(seedsFor("next-json-runtime-validation", [0xb644, 0xb645]))(
  "validates dynamic text as PG, without interpreting number spelling (seed %i)",
  async (seed) => {
    const reach = createReach<string>("JSON input validation", JSON_VALIDATION_REACH);
    const examples: Array<[number, number]> = [
      [0, 0],
      [1, -1],
      [2, 2],
      [3, 3],
      [4, 4],
      [5, 5],
    ];
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 0, max: 5 }),
        fc.integer({ min: -100000, max: 100000 }),
        async (shape, n) => {
          reach.beginHistory();
          const sources = [
            `[${String(n)},]`,
            `{"n":${String(n)},}`,
            `{"n":0${String(Math.abs(n))}}`,
            `{"n":${String(n)}e}`,
            `{"n":"\\q"}`,
            `{"n":${String(n)}.00}`,
          ];
          const doc = sources[shape];
          if (doc === undefined) throw new Error("Missing validation shape");
          const sql = `SELECT j::json ->> 'n' AS j FROM (VALUES (${quote(doc)}::text)) q(j)`;
          if (shape === 5) {
            const result = await core.execute(sql);
            if (!("batches" in result)) throw new Error("Expected rows");
            expect(oracleRows(result), sql).toEqual((await pg.query(sql)).rows);
          } else {
            await expect(pg.query(sql)).rejects.toMatchObject({ code: "22P02" });
            await expect(core.execute(sql)).rejects.toMatchObject({ sqlState: "22P02" });
          }
          reach.reach(
            ["array comma", "object comma", "leading zero", "exponent", "escape", "valid decimal"][
              shape
            ] ?? "",
          );
        },
      ),
      { seed, numRuns: 50 + examples.length, examples },
    );
    reach.assertReached();
  },
  120000,
);

it.each(seedsFor("next-json-materialized-refusal", [0xb646, 0xb647]))(
  "refuses front-end JSON materialization without exact source text (seed %i)",
  async (seed) => {
    const reach = createReach<string>("Materialized JSON boundary", JSON_MATERIALIZED_REACH);
    const examples: Array<[number, number, boolean]> = [
      [0, 2, false],
      [9007199254740991, 2, false],
      [-7, 4, true],
      [20, 1, true],
    ];
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: -100000, max: 100000 }),
        fc.integer({ min: 1, max: 6 }),
        fc.boolean(),
        async (value, scale, cte) => {
          reach.beginHistory();
          const number = String(value) + "." + "0".repeat(scale),
            document = '{"n":' + number + "}";
          const source = `JSON_TABLE(${quote('[{"v":' + document + "}]")},'$[*]' COLUMNS(v JSON PATH '$.v')) q`;
          const sql = cte
            ? `WITH src AS (SELECT v FROM ${source}) SELECT v ->> 'n' AS j FROM src`
            : `SELECT v ->> 'n' AS j FROM ${source}`;
          expect((await pg.query(sql)).rows).toEqual([{ j: number }]);
          let refusal: unknown;
          try {
            core.prepare(sql);
          } catch (error) {
            refusal = error;
          }
          expect(refusal).toMatchObject({
            name: "UnsupportedPlanError",
            stage: "lower",
            construct: "function",
            sqlState: "0A000",
          });
          if (!(refusal instanceof Error)) throw new Error("Expected a typed refusal");
          expect(refusal.message).toContain("source spelling");
          reach.reach(cte ? "CTE" : "direct");
          reach.reach("typed refusal");
          if (value < 0) reach.reach("negative");
          if (scale > 1) reach.reach("scale preserved by PG");
        },
      ),
      { seed, numRuns: 40 + examples.length, examples },
    );
    reach.assertReached();
  },
  120000,
);

it.each(seedsFor("next-json-current-lexical", [0xb64a, 0xb64b]))(
  "registers only the independently witnessed current-engine number normalization (seed %i)",
  async (seed) => {
    const current = new MinnowDatabase(new MemoryBlockStore(), {
      autoCompact: false,
      autoCollect: false,
    });
    const reach = createReach<string>("Current JSON lexical witness", JSON_CURRENT_LEXICAL_REACH);
    const examples: Array<[number, number]> = [
      [0, 1],
      [-7, 2],
      [42, 6],
    ];
    try {
      await fc.assert(
        fc.asyncProperty(
          fc.integer({ min: -100000, max: 100000 }),
          fc.integer({ min: 1, max: 6 }),
          async (value, scale) => {
            reach.beginHistory();
            const source = String(value) + "." + "0".repeat(scale);
            const sql = `SELECT ${quote('{"n":' + source + "}")}::json ->> 'n' AS j`;
            const native = await core.execute(sql);
            if (!("batches" in native)) throw new Error("Expected JSON rows");
            const authority = (await pg.query(sql)).rows;
            expect(oracleRows(native)).toEqual(authority);
            const canonical = (
              await pg.query<Record<string, unknown>>(`SELECT (${String(value)}::int4)::text AS j`)
            ).rows;
            const legacy = (await current.query(sql)).rows;
            expect(legacy).toEqual(canonical);
            expect(legacy).not.toEqual(authority);
            const plan = core.prepare(sql).plan;
            const id = currentJsonNumberText(plan, legacy, oracleRows(native), canonical);
            expect(QUERY_DIVERGENCES.find((entry) => entry.id === id)?.authority).toContain(
              "PostgreSQL",
            );
            expect(
              currentJsonNumberText(plan, [{ j: "wrong" }], oracleRows(native), canonical),
            ).toBeNull();
            expect(currentJsonNumberText(plan, legacy, [{ j: "wrong" }], canonical)).toBeNull();
            reach.reach(value === 0 ? "zero" : value < 0 ? "negative" : "positive");
            reach.reach("exact PG result");
            reach.reach("canonical PG witness");
            reach.reach("wrong value rejected");
          },
        ),
        { seed, numRuns: 40 + examples.length, examples },
      );
      reach.assertReached();
    } finally {
      await current.close();
    }
  },
  120000,
);

it.each(seedsFor("next-json-key-types", [0xb64e, 0xb64f]))(
  "keeps PG's text/int4 key overloads and error states (seed %i)",
  async (seed) => {
    const reach = createReach<string>("JSON key types", JSON_KEY_TYPES_REACH);
    const examples: Array<[number, boolean]> = [
      [0, false],
      [1, true],
      [2, false],
      [3, true],
      [4, false],
      [5, true],
    ];
    await fc.assert(
      fc.asyncProperty(fc.integer({ min: 0, max: 5 }), fc.boolean(), async (shape, text) => {
        reach.beginHistory();
        const key = ["true", "1.5", "1.0", "1::bigint", "1::smallint", "'n'::text"][shape];
        const sql = `SELECT ('["a",2,null]'::json ${text ? "->>" : "->"} ${String(key)})::text AS j`;
        if (shape < 4) {
          await expect(pg.query(sql)).rejects.toMatchObject({ code: "42883" });
          await expect(core.execute(sql)).rejects.toMatchObject({ sqlState: "42883" });
          reach.reach("42883");
        } else {
          const result = await core.execute(sql);
          if (!("batches" in result)) throw new Error("Expected rows");
          expect(oracleRows(result)).toEqual((await pg.query(sql)).rows);
        }
        reach.reach(
          ["boolean", "fractional", "numeric integral", "int8", "int2 widening", "text"][shape] ??
            "",
        );
        reach.reach(text ? "text extraction" : "raw extraction");
      }),
      { seed, numRuns: 40 + examples.length, examples },
    );
    reach.assertReached();
  },
  120000,
);

it.each(seedsFor("next-json-parameter-envelope", [0xb650, 0xb651]))(
  "keeps unbounded document parameters refused through projection aliases (seed %i)",
  async (seed) => {
    const reach = createReach<string>("JSON parameter provenance", JSON_PARAMETER_ENVELOPE_REACH);
    const examples: Array<[number, number, boolean]> = [
      [0, 1, false],
      [1, -7, false],
      [2, 3, true],
      [3, 4, false],
    ];
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 0, max: 3 }),
        fc.integer({ min: -100000, max: 100000 }),
        fc.boolean(),
        async (scope, value, isNull) => {
          reach.beginHistory();
          const source =
            scope === 0
              ? "SELECT $1::text::json ->> 'n' AS j"
              : scope === 1
                ? "SELECT j::json ->> 'n' AS j FROM (SELECT $1::text AS j) q"
                : scope === 2
                  ? "WITH q AS (SELECT $1::text AS j) SELECT j::json ->> 'n' AS j FROM q"
                  : "SELECT j::json ->> 'n' AS j FROM (VALUES ($1::text)) q(j)";
          const text = String(value) + ".00",
            document = isNull ? null : '{"n":' + text + "}";
          expect((await pg.query(source, [document])).rows).toEqual([{ j: isNull ? null : text }]);
          expect(() => core.prepare(source)).toThrow(
            expect.objectContaining({
              name: "UnsupportedPlanError",
              stage: "execute",
              construct: "function",
              sqlState: "0A000",
            }),
          );
          reach.reach(["direct", "derived text", "CTE text", "VALUES text"][scope] ?? "");
          reach.reach(isNull ? "SQL NULL" : "decimal spelling");
          reach.reach("typed bound refusal");
        },
      ),
      { seed, numRuns: 40 + examples.length, examples },
    );
    reach.assertReached();
  },
  120000,
);
