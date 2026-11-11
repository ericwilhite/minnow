/** EXTRACT returns exact PostgreSQL NUMERIC fields, not a float8 approximation. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { afterAll, beforeAll, expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, INTERVAL_EXTRACT_SQL_REACH } from "../../testing/reach.js";
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
const fields = [
  "epoch",
  "year",
  "month",
  "day",
  "hour",
  "minute",
  "second",
  "milliseconds",
  "microseconds",
  "decade",
  "century",
  "millennium",
  "quarter",
  "week",
  "dow",
  "nonesuch",
] as const;
it.each(seedsFor("next-interval-extract-sql", [0xb662, 0xb663]))(
  "keeps exact interval field coefficients, scale and errors (seed %i)",
  async (seed) => {
    const reach = createReach<string>("Interval extraction", INTERVAL_EXTRACT_SQL_REACH);
    const examples: Array<[number, number, number, number, boolean, number]> = fields.map(
      (_, i) => [i, 13, -2, -1234567, false, i % 3],
    );
    examples.push([0, 0, 0, 0, true, 0], [0, -13, 3, 9007199254740991, false, 1]);
    examples.push([12, -12, 0, 0, false, 0], [12, -3, 0, 0, false, 1], [13, 0, -8, 0, false, 0]);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 0, max: fields.length - 1 }),
        fc.integer({ min: -200, max: 200 }),
        fc.integer({ min: -100, max: 100 }),
        fc.integer({ min: -9000000000000, max: 9000000000000 }),
        fc.boolean(),
        fc.integer({ min: 0, max: 2 }),
        async (fieldIndex, months, days, micros, nil, scope) => {
          reach.beginHistory();
          const field = fields[fieldIndex] ?? "epoch";
          const literal = nil
            ? "NULL::interval"
            : `INTERVAL '${String(months)} months ${String(days)} days ${String(micros)} microseconds'`;
          const expr = `EXTRACT(${field} FROM ${literal})`;
          const sql =
            scope === 0
              ? `SELECT ${expr} AS v`
              : scope === 1
                ? `SELECT v AS v FROM(SELECT ${expr} AS v) q`
                : `WITH q AS(SELECT ${expr} AS v) SELECT v AS v FROM q`;
          let expected: unknown;
          try {
            expected = (await pg.query(sql)).rows;
          } catch (error) {
            await expect(core.execute(sql)).rejects.toMatchObject({
              sqlState: (error as { code: string }).code,
            });
            reach.reach(field === "dow" ? "unsupported field" : "unknown field");
            return;
          }
          const result = await core.execute(sql);
          if (!("batches" in result)) throw new Error("Expected extracted interval rows");
          expect(oracleRows(result), sql).toEqual(expected);
          if (nil) reach.reach("NULL");
          else {
            reach.reach(field);
            if (field === "quarter" && months < 0) reach.reach("negative quarter");
            if (months < 0 || days < 0 || micros < 0) reach.reach("negative fields");
            if (micros % 1000 !== 0) reach.reach("microsecond scale");
            if (Math.abs(micros) > 1000000000000000) reach.reach("large exact coefficient");
          }
          reach.reach(["direct", "derived", "CTE"][scope] ?? "direct");
        },
      ),
      { seed, numRuns: 70 + examples.length, examples },
    );
    reach.assertReached();
  },
  120000,
);
