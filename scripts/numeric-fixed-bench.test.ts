/** Benchmark fixture is checked against PostgreSQL, independently of the plain loop. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { beforeAll, afterAll, expect, it } from "vitest";
import { fileURLToPath } from "node:url";
import { numericFixture } from "./lib/numeric-fixed-fixture.mts";
import createNumeric from "./next-numeric-fixed-worker.mts";
import { seedsFor } from "../packages/core/src/testing/seeds.js";
import { createReach, NUMERIC_BENCH_REACH } from "../packages/core/src/testing/reach.js";
let pg: PGlite;
beforeAll(async () => {
  pg = new PGlite();
  await pg.waitReady;
}, 60000);
afterAll(async () => {
  await pg.close();
});
it.each(seedsFor("next-numeric-bench", [0xb751, 0xb752]))(
  "verifies decoded fixture, native/plain work and complete volume (seed %i)",
  async (seed) => {
    const reach = createReach("NUMERIC benchmark", NUMERIC_BENCH_REACH);
    await fc.assert(
      fc.asyncProperty(fc.integer({ min: 1, max: 257 }), async (n) => {
        reach.beginHistory();
        const f = await numericFixture(n),
          tuples: string[] = [];
        for (const batch of f.source)
          for (let r = 0; r < batch.length; r++) {
            const code = batch.codes[r] ?? 0;
            const key = new TextDecoder().decode(
              batch.keys.bytes.subarray(batch.keys.offsets[code], batch.keys.offsets[code + 1]),
            );
            const values = batch.values.map((v) => `(${String(BigInt(v[r] ?? 0))}::numeric/100)`);
            tuples.push(`('${key.replace(/'/g, "''")}',${values.join(",")})`);
          }
        const query = await pg.query<{ g: string; a: string; b: string; c: string; d: string }>(
          `SELECT g,(sum(a)*100)::numeric(38,0)::text a,(sum(b)*100)::numeric(38,0)::text b,(min(c)*100)::numeric(38,0)::text c,(max(d)*100)::numeric(38,0)::text d FROM (VALUES ${tuples.join(",")}) v(g,a,b,c,d) GROUP BY g ORDER BY g COLLATE "C"`,
        );
        expect(f.expected).toEqual(
          query.rows.map((v) => [v.g, BigInt(v.a), BigInt(v.b), BigInt(v.c), BigInt(v.d)]),
        );
        let fingerprint: string | undefined;
        for (const plain of [false, true]) {
          const worker = await createNumeric({
            arm: "control",
            revision: "fixture-test",
            cohort: 0,
            workload: "agg_numeric_grouped",
            boundary: "native-component",
            signal: new AbortController().signal,
            data: { runtimeRoot: fileURLToPath(new URL("../", import.meta.url)), rows: n, plain },
          });
          try {
            const before = await worker.verify("agg_numeric_grouped");
            if (fingerprint !== undefined) expect(before.fingerprint).toBe(fingerprint);
            fingerprint = before.fingerprint;
            const run = await worker.run("agg_numeric_grouped", "native-component");
            expect(run.rows).toBe(before.rows);
            expect(run.detail).toEqual({
              completedLifetimes: 10,
              rowsPerLifetime: f.expected.length,
            });
            expect(await worker.verify("agg_numeric_grouped")).toEqual(before);
            reach.reach("full timed volume");
          } finally {
            await worker.close();
          }
        }
        reach.reach("native and plain exact");
        reach.reach("zero charges");
        if (n === 1) reach.reach("single row");
        if (n < 2048) reach.reach("partial batch");
        if (n > 2048) reach.reach("multiple batches");
        if (n > 6144) reach.reach("dictionary epochs");
      }),
      { seed, numRuns: 8, examples: [[1], [7], [2049], [6145]] },
    );
    reach.assertReached();
  },
  120000,
);
