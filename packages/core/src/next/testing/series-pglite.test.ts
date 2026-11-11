/** The native SRF admits arguments at execution and streams without a hidden row/depth limit. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { beforeAll, afterAll, expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import {
  createReach,
  SERIES_SQL_REACH,
  SERIES_LARGE_REACH,
  SERIES_RENDER_REACH,
} from "../../testing/reach.js";
import { StorageCore } from "./storage-core.js";
import { oracleRows } from "./differential-adapter.js";
let pg: PGlite, core: StorageCore;
beforeAll(async () => {
  pg = new PGlite();
  await pg.waitReady;
  await pg.exec("SET TIME ZONE 'UTC'");
  core = await new StorageCore().open();
}, 60000);
afterAll(async () => {
  await core.close();
  await pg.close();
});
const ZERO_SERIES = "generate_series(1,5,0)";
const cases = [
  "generate_series(1,5)",
  "generate_series(5,1,-2)",
  "generate_series(5,1,1)",
  "generate_series(1,5,0)",
  "generate_series(NULL::int4,5,0)",
  "generate_series(1,NULL::int4,0)",
  "generate_series(1,5,NULL::int4)",
  "generate_series(NULL,5,1)",
  "generate_series(NULL,NULL)",
  "generate_series('1','5')",
  "generate_series('1','5',1)",
  "generate_series(2147483646,2147483647)",
  "generate_series(-2147483647,-2147483648,-1)",
  "generate_series(9223372036854775806::bigint,9223372036854775807::bigint,1)",
  "generate_series(-9223372036854775807::bigint,(-9223372036854775808)::bigint,-1)",
  "generate_series(1::smallint,5::smallint,1::smallint)",
  "generate_series(1::smallint,5::integer,1::smallint)",
  "generate_series(1,5::bigint,1)",
  "generate_series(TIMESTAMP '2024-01-31',TIMESTAMP '2024-05-31',INTERVAL '1 month')",
  "generate_series(TIMESTAMP '2024-05-31',TIMESTAMP '2024-01-01',INTERVAL '-1 month')",
  "generate_series(TIMESTAMP '2024-01-01 00:00:00.123456',TIMESTAMP '2024-01-01 00:00:00.123460',INTERVAL '1 microsecond')",
  "generate_series(TIMESTAMP '2024-01-01',TIMESTAMP '2024-01-02',INTERVAL '1 month -30 days')",
  "generate_series(NULL::timestamp,TIMESTAMP '2024-01-02',INTERVAL '1 month -30 days')",
] as const;
it.each(seedsFor("next-series-sql", [0xb666, 0xb667]))(
  "matches PG's typed arguments, rows and admission (seed %i)",
  async (seed) => {
    const reach = createReach<string>("Native series SQL", SERIES_SQL_REACH);
    const examples: Array<[number, number, number, number, number]> = cases.map((_, i) => [
      i,
      0,
      1,
      1,
      0,
    ]);
    examples.push([-1, -5, 5, 2, 1], [-1, 5, -5, -2, 2], [-1, 1, 5, 0, 0]);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: -1, max: cases.length - 1 }),
        fc.integer({ min: -20, max: 20 }),
        fc.integer({ min: -20, max: 20 }),
        fc.integer({ min: -5, max: 5 }),
        fc.integer({ min: 0, max: 2 }),
        async (index, start, stop, step, scope) => {
          reach.beginHistory();
          const source =
            index < 0
              ? `generate_series(${String(start)},${String(stop)},${String(step)})`
              : cases[index];
          if (source === undefined) throw new Error("Missing series source");
          const output =
            source.includes("TIMESTAMP") || source.includes("timestamp") ? "x::text AS x" : "x";
          const sql =
            scope === 0
              ? `SELECT ${output} FROM ${source} AS g(x)`
              : scope === 1
                ? `SELECT ${output} FROM(SELECT x FROM ${source} AS g(x)) q`
                : `WITH q AS(SELECT x FROM ${source} AS g(x)) SELECT ${output} FROM q`;
          const authority = `SELECT x::text AS x FROM ${source} AS g(x)`;
          let expected: unknown;
          try {
            expected = (await pg.query(authority)).rows;
          } catch (error) {
            await expect(core.execute(sql)).rejects.toMatchObject({
              sqlState: (error as { code: string }).code,
            });
            reach.reach(
              (error as { code: string }).code === "42725" ? "ambiguous overload" : "zero step",
            );
            return;
          }
          const result = await core.execute(sql);
          if (!("batches" in result)) throw new Error("Expected series rows");
          // Read integers as exact wire values; timestamp text goes through the real cast kernel.
          const rows = oracleRows(result).map((row) => {
            const x = row.x;
            if (
              x !== null &&
              typeof x !== "number" &&
              typeof x !== "bigint" &&
              typeof x !== "string"
            )
              throw new TypeError("Expected integer or timestamp text");
            return { x: x === null ? null : String(x) };
          });
          expect(rows, sql).toEqual(expected);
          reach.reach(["direct", "derived", "CTE"][scope] ?? "direct");
          if (rows.length === 0) reach.reach("empty");
          if (source.includes("NULL")) reach.reach("NULL argument");
          if (source.includes("::bigint")) reach.reach("int8");
          else if (!source.includes("TIMESTAMP") && !source.includes("timestamp"))
            reach.reach("int4");
          if (source.includes("smallint")) reach.reach("int2 widening");
          if (source.includes("TIMESTAMP")) {
            reach.reach("timestamp");
            if (source.includes("microsecond")) reach.reach("microsecond step");
            if (source.includes("month")) reach.reach("calendar step");
          }
          if (source.includes("2147483647") || source.includes("9223372036854775807"))
            reach.reach("last integer overflow stops");
        },
      ),
      { seed, numRuns: 55 + examples.length, examples },
    );
    reach.assertReached();
  },
  120000,
);
it.each(seedsFor("next-series-large", [0xb668, 0xb669]))(
  "streams beyond the shipped row cap and stops at demand (seed %i)",
  async (seed) => {
    const reach = createReach<string>("Large series", SERIES_LARGE_REACH);
    await fc.assert(
      fc.asyncProperty(fc.integer({ min: 100001, max: 200005 }), async (count) => {
        reach.beginHistory();
        const sql = `SELECT count(*) AS n FROM generate_series(1,${String(count)}) AS g(x)`;
        const expected = (
          await pg.query(`SELECT count(*)::text AS n FROM generate_series(1,${String(count)})`)
        ).rows;
        const result = await core.execute(sql);
        if (!("batches" in result)) throw new Error("Expected count");
        expect(oracleRows(result).map((row) => ({ n: String(row.n) }))).toEqual(expected);
        reach.reach("past shipped row cap");
        for (const source of ["generate_series(1,9223372036854775807::bigint)", ZERO_SERIES]) {
          const query = `SELECT x FROM ${source} AS g(x) LIMIT ${source === ZERO_SERIES ? "0" : "3"}`;
          const actual = await core.execute(query);
          if (!("batches" in actual)) throw new Error("Expected limited series");
          expect(
            oracleRows(actual).map((row) => {
              const x = row.x;
              if (typeof x !== "number" && typeof x !== "bigint")
                throw new TypeError("Expected integer");
              return { x: String(x) };
            }),
          ).toEqual(
            // FunctionScan materializes a FROM SRF in PG. A finite PG prefix witnesses the
            // integer stream's first three rows without allocating the huge complete range.
            (
              await pg.query(
                query
                  .replace("9223372036854775807::bigint", "3::bigint")
                  .replace("SELECT x", "SELECT x::text AS x"),
              )
            ).rows,
          );
          reach.reach(
            source === ZERO_SERIES ? "LIMIT 0 skips zero step" : "LIMIT stops huge range",
          );
        }
      }),
      { seed, numRuns: 4, examples: [[100001], [200005]] },
    );
    reach.assertReached();
  },
  120000,
);
it.each(seedsFor("next-series-rendering", [0xb676, 0xb677]))(
  "keeps R17.1 int8 rendering separate from widening (seed %i)",
  async (seed) => {
    const reach = createReach<string>("Series integer rendering", SERIES_RENDER_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 2147483648, max: 4294967295 }),
        fc.boolean(),
        fc.boolean(),
        async (start, explicit, derived) => {
          reach.beginHistory();
          const source = `SELECT x FROM generate_series(${String(start)}${explicit ? "::bigint" : ""},${String(start + 2)}) g(x)`;
          const sql = derived ? `SELECT x FROM(${source}) q` : source;
          const lowered = core.lowerer.lower(sql, core.context());
          if (lowered.kind !== "query") throw new Error("Expected series query");
          expect(lowered.columns[0]?.render).toBe(explicit ? "bigint" : "number");
          const actual = await core.execute(sql);
          if (!("batches" in actual)) throw new Error("Expected series rows");
          expect(
            oracleRows(actual).map((row) => {
              const x = row.x;
              if (typeof x !== "number" && typeof x !== "bigint")
                throw new TypeError("Expected integer");
              return { x: String(x) };
            }),
          ).toEqual((await pg.query(`SELECT x::text AS x FROM(${sql}) q`)).rows);
          reach.reach(explicit ? "explicit bigint" : "implicit int8");
          reach.reach(derived ? "derived" : "direct");
          reach.reach("PG exact");
        },
      ),
      {
        seed,
        numRuns: 20,
        examples: [
          [2147483648, false, false],
          [2147483648, true, false],
          [4294967295, false, true],
          [4294967295, true, true],
        ],
      },
    );
    reach.assertReached();
  },
  120000,
);
