/** JSONB numbers use PostgreSQL NUMERIC's display scale and range, not float8. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { afterAll, beforeAll, expect, it } from "vitest";
import { seedsFor } from "../../../testing/seeds.js";
import { createReach, JSONB_NUMBER_REACH } from "../../../testing/reach.js";
import { Slice, runToEnd } from "../slices.js";
import { JsonOutput } from "./json-output.js";
import { jsonbNumber } from "./jsonb-number.js";
let pg: PGlite;
beforeAll(async () => {
  pg = new PGlite();
  await pg.waitReady;
}, 60000);
afterAll(async () => {
  await pg.close();
});
it.each(seedsFor("next-jsonb-number", [0xb682, 0xb683]))(
  "keeps PG exact JSONB decimal bytes (seed %i)",
  async (seed) => {
    const reach = createReach<string>("JSONB number", JSONB_NUMBER_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: -999999999, max: 999999999 }),
        fc.integer({ min: 0, max: 20 }),
        fc.integer({ min: -35, max: 35 }),
        async (coefficient, zeros, exponent) => {
          reach.beginHistory();
          reach.reach(coefficient < 0 ? "negative" : coefficient > 0 ? "positive" : "zero");
          reach.reach(
            exponent < 0
              ? "negative exponent"
              : exponent > 0
                ? "positive exponent"
                : "zero exponent",
          );
          reach.reach(zeros >= 10 ? "long scale" : "short scale");
          const text = String(coefficient) + "." + "0".repeat(zeros) + "100e" + String(exponent);
          const expected = (await pg.query<{ t: string }>("SELECT $1::jsonb::text AS t", [text]))
            .rows[0]?.t;
          const bytes = new TextEncoder().encode(text),
            slice = new Slice(Infinity),
            counter = new JsonOutput(null, slice);
          runToEnd(jsonbNumber(bytes, 0, bytes.length, counter, slice));
          const output = new Uint8Array(counter.position),
            writer = new JsonOutput(output, slice);
          runToEnd(jsonbNumber(bytes, 0, bytes.length, writer, slice));
          expect(new TextDecoder().decode(output)).toBe(expected);
        },
      ),
      {
        seed,
        numRuns: 100,
        examples: [
          [0, 4, 0],
          [-12, 5, -2],
          [999999999, 20, 35],
        ],
      },
    );
    reach.assertReached();
  },
  120000,
);
