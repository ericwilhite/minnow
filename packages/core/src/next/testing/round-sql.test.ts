/** SQL literal typing and binding around numeric ROUND, judged by PostgreSQL. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { afterAll, beforeAll, expect, it } from "vitest";
import { createReach, SIGNED_LITERAL_REACH } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import { StorageCore } from "./storage-core.js";
import { oracleRows } from "./differential-adapter.js";
let pg: PGlite, core: StorageCore;
beforeAll(async () => {
  pg = new PGlite();
  await pg.waitReady;
  core = await new StorageCore().open();
}, 60_000);
afterAll(async () => {
  await core.close();
  await pg.close();
});
const MAGNITUDES = [
  0n,
  1n,
  2147483647n,
  2147483648n,
  2147483649n,
  9223372036854775807n,
  9223372036854775808n,
  9223372036854775809n,
];
const FORMS = ["bare", "parenthesized", "multiply", "subtract", "double sign", "cast"] as const;
interface Shape {
  magnitude: bigint;
  form: number;
  decimal: boolean;
}
function source(shape: Shape): string {
  const text = shape.magnitude.toString() + (shape.decimal ? ".0" : "");
  switch (shape.form) {
    case 0:
      return `-${text}`;
    case 1:
      return `-(${text})`;
    case 2:
      return `(-1)*${text}`;
    case 3:
      return `0-${text}`;
    case 4:
      return `-(-${text})`;
    default:
      return `-(CAST(${(shape.magnitude % 9223372036854775807n).toString()}${shape.decimal ? ".0" : ""} AS BIGINT))`;
  }
}

it.each(seedsFor("next-signed-round-literals", [0xb609, 0xb60a]))(
  "matches PG signed literal types before ROUND resolution (seed %i)",
  async (seed) => {
    const reach = createReach<string>("signed numeric literals", SIGNED_LITERAL_REACH);
    const examples: Array<[Shape]> = [];
    for (const magnitude of MAGNITUDES)
      for (let form = 0; form < FORMS.length; form++)
        for (const decimal of [false, true]) examples.push([{ magnitude, form, decimal }]);
    await fc.assert(
      fc.asyncProperty(
        fc.record({
          magnitude: fc.bigInt({ min: 0n, max: 9223372036854775809n }),
          form: fc.integer({ min: 0, max: 5 }),
          decimal: fc.boolean(),
        }),
        async (shape) => {
          reach.beginHistory();
          const text = source(shape);
          const expectedType = (
            await pg.query<{ type: string }>(`SELECT pg_typeof(${text})::text AS type`)
          ).rows[0]?.type;
          const lowered = core.lowerer.lower(`SELECT ${text} AS v`, core.context());
          if (lowered.kind !== "query") throw new Error("Expected scalar query");
          expect(lowered.columns[0]?.sqlType, text).toBe(expectedType);
          // The plan-cache scanner must classify the same signed literal as PostgreSQL,
          // including both signed minima. Other expressions stay unlifted.
          const cacheSQL = `SELECT $1 = ${text} AS v`;
          const lifted = core.lowerer.parameterize(cacheSQL);
          if (shape.form === 0 && !shape.decimal && expectedType !== "numeric")
            expect(lifted, cacheSQL).not.toBeNull();
          if (lifted === null) reach.reach("unlifted expression");
          else {
            expect(lifted.types, cacheSQL).toEqual([lowered.plan.columns[0]?.type]);
            const family = core.lowerer.lowerParameterized(lifted, core.context());
            expect(family.kind, cacheSQL).toBe("parameterized");
            if (family.kind !== "parameterized") throw new Error("Expected cached family");
            const original = core.lowerer.lower(cacheSQL, core.context());
            if (original.kind !== "query") throw new Error("Expected original query");
            expect(family.statement.parameters, cacheSQL).toEqual(original.parameters);
            reach.reach(lifted.types[0]?.kind === "int32" ? "cached int4" : "cached int8");
            if (shape.magnitude === 2147483648n) reach.reach("cached int4 minimum");
            if (shape.magnitude === 9223372036854775808n) reach.reach("cached int8 minimum");
          }
          const form = FORMS[shape.form];
          if (form === undefined) throw new Error("Missing form");
          reach.reach(form);
          reach.reach(shape.decimal ? "decimal spelling" : "integer spelling");
          if (shape.magnitude >= 2147483647n && shape.magnitude <= 2147483649n)
            reach.reach("int4 boundary");
          if (shape.magnitude >= 9223372036854775807n) reach.reach("int8 boundary");
          const sql = `SELECT round($1::numeric,${text}) AS v`;
          let expected: Array<Record<string, unknown>>;
          try {
            expected = (await pg.query<Record<string, unknown>>(sql, ["1.25"])).rows;
          } catch (error) {
            const state = (error as { code: string }).code;
            expect(state).toBe("42883");
            await expect(core.execute(sql, ["1.25"]), text).rejects.toMatchObject({
              sqlState: state,
            });
            reach.reach("overload refusal");
            return;
          }
          const actual = await core.execute(sql, ["1.25"]);
          if (!("batches" in actual)) throw new Error("Expected ROUND rows");
          expect(oracleRows(actual), sql).toEqual(expected);
          reach.reach("ROUND rows");
        },
      ),
      { seed, numRuns: 112, examples },
    );
    reach.assertReached();
  },
  120_000,
);

it("binds SQL NaN and NULL and both int4 scale limits through the executor", async () => {
  for (const [value, scale] of [
    ["NaN", 2],
    ["NaN", null],
    [null, 2],
    ["Infinity", 2147483647],
    ["-Infinity", -2147483648],
    ["1.25", 2147483647],
    ["1.25", -2147483648],
  ] as const) {
    const sql = "SELECT round($1::numeric,$2::int) AS v";
    const expected = (await pg.query<Record<string, unknown>>(sql, [value, scale])).rows;
    const actual = await core.execute(sql, [value, scale]);
    if (!("batches" in actual)) throw new Error("Expected scalar rows");
    expect(oracleRows(actual)).toEqual(expected);
  }
});
