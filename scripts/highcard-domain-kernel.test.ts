/** Exact domain bounds and top-N order, against PG and the original hash kernel. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { afterAll, beforeAll, expect, it } from "vitest";
import { seedsFor } from "../packages/core/src/testing/seeds.js";
import { createReach } from "../packages/core/src/testing/reach.js";
import { highcard, highcardDomain, type LightInput } from "./lib/aggregation-light-kernels.mts";
let pg: PGlite;
beforeAll(async () => {
  pg = new PGlite();
  await pg.waitReady;
}, 60000);
afterAll(async () => {
  await pg.close();
});
it.each(seedsFor("highcard-domain-kernel", [0xb481, 0xb482]))(
  "certifies actual int32 bounds and retains the sparse fallback (seed %i)",
  async (seed) => {
    const reach = createReach("highcard direct kernel", [
      "empty",
      "compact",
      "negative domain",
      "int32 endpoints",
      "wide fallback",
      "top100",
      "negative zero",
      "holes omitted",
      "PG",
    ]);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 160, max: 320 }),
        fc.integer({ min: 0, max: 10000 }),
        fc.array(fc.integer({ min: -10000, max: 10000 }), {
          minLength: 16,
          maxLength: 16,
        }),
        async (n, shift, raw) => {
          reach.beginHistory();
          for (const mode of [
            "empty",
            "compact",
            "holes",
            "negative",
            "min",
            "max",
            "wide",
            "zero",
          ] as const) {
            const length = mode === "empty" ? 0 : n;
            const keys = Float64Array.from({ length }, (_, i) =>
              mode === "wide"
                ? i === 0
                  ? -2147483648
                  : i === 1
                    ? 2147483647
                    : i % 2
                      ? 2147483647 - i * 17
                      : -2147483648 + i * 17
                : mode === "holes"
                  ? shift + (i % 7) * 37
                  : mode === "zero"
                    ? 0
                    : mode === "min"
                      ? -2147483648 + ((i * 17) % 257)
                      : mode === "max"
                        ? 2147483647 - ((i * 17) % 257)
                        : (mode === "negative" ? -shift - 257 : shift) + ((i * 17) % 257),
            );
            const values = Float64Array.from({ length }, (_, i) =>
              mode === "zero" ? -0 : (raw[i % 16] ?? 0) / 8,
            );
            const input: LightInput = {
              rows: length,
              n: [keys, values],
              b: [],
              valid: new Uint8Array(0),
            };
            const expected = (
              await pg.query<{ k: string; n: string; s: string }>(
                length === 0
                  ? "SELECT k::text,count(*)::text AS n,sum(v)::text AS s FROM (SELECT 0::int4 AS k,0::float8 AS v WHERE false) AS inp GROUP BY k ORDER BY count(*) DESC,inp.k LIMIT 100"
                  : `WITH inp(ord,k,v) AS (VALUES ${Array.from({ length }, (_, i) => `(${String(i)},'${String(keys[i])}'::int4,'${Object.is(values[i], -0) ? "-0" : String(values[i])}'::float8)`).join(",")}) SELECT k::text,count(*)::text AS n,sum(v ORDER BY ord)::text AS s FROM inp GROUP BY k ORDER BY count(*) DESC,inp.k LIMIT 100`,
              )
            ).rows.map((r) => [Number(r.k), Number(r.n), Number(r.s)]);
            const got = highcardDomain(input);
            expect(got.domainStrategy).toBe(
              mode === "empty" ? "empty" : mode === "wide" ? "hash" : "direct",
            );
            expect(got.rows).toEqual(expected);
            expect(highcard(input).rows).toEqual(expected);
            if (mode === "empty") reach.reach("empty");
            if (mode === "compact") reach.reach("compact");
            if (mode === "holes") {
              expect(got.rows.length).toBe(7);
              reach.reach("holes omitted");
            }
            if (mode === "negative") reach.reach("negative domain");
            if (mode === "min" || mode === "max" || mode === "wide") reach.reach("int32 endpoints");
            if (mode === "wide") reach.reach("wide fallback");
            if (mode === "zero") {
              expect(Object.is(got.rows[0]?.[2], -0)).toBe(true);
              reach.reach("negative zero");
            }
            if (expected.length === 100) reach.reach("top100");
            reach.reach("PG");
          }
        },
      ),
      { seed, numRuns: 12 },
    );
    reach.assertReached();
  },
  60000,
);
