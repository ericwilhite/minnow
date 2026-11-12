/** A copied NUMERIC sibling cannot force whole-row copies of an adoptable large JSON value. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { afterAll, beforeAll, expect, it, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, JSON_RESULT_MIXED_REACH } from "../../testing/reach.js";
import { Governor } from "../memory/governor.js";
import { StorageCore } from "./storage-core.js";
import { oracleRows } from "./differential-adapter.js";
let pg: PGlite;
beforeAll(async () => {
  pg = new PGlite();
  await pg.waitReady;
}, 60000);
afterAll(async () => {
  await pg.close();
});
it.each(seedsFor("next-json-result-mixed", [0xb6e6, 0xb6e7]))(
  "keeps wide/compact NUMERIC copies separate from JSON delivery (seed %i)",
  async (seed) => {
    const reach = createReach<string>("Mixed JSON result delivery", JSON_RESULT_MIXED_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 460000, max: 500000 }),
        fc.boolean(),
        fc.boolean(),
        async (length, wide, binary) => {
          reach.beginHistory();
          const core = await new StorageCore().open(),
            governor = new Governor(16 << 20),
            number = wide
              ? "900719925474099300000000000000000000000000000000000000000000000000000001.2300"
              : "1.2300",
            sql = `SELECT ${binary ? "jsonb_build_object" : "json_build_object"}('v',$1::text)::text j,'${number}'::numeric n`,
            value = "\u0001".repeat(length),
            expected = (await pg.query(sql, [value])).rows,
            original = core.executor.prepare.bind(core.executor),
            preparation = vi.spyOn(core.executor, "prepare").mockImplementation((plan) => {
              const prepared = original(plan);
              return {
                ...prepared,
                execute: (request) => prepared.execute({ ...request, governor }),
              };
            });
          try {
            const result = await core.execute(sql, [value]);
            if (!("batches" in result)) throw new Error("Expected rows");
            expect(oracleRows(result)).toEqual(expected);
            expect(governor.peakBytes).toBeLessThanOrEqual(governor.capBytes);
            expect(governor.usedBytes).toBe(0);
            expect(core.spill.bytesInUse).toBe(0);
            reach.reach(wide ? "wide JS coefficient copy" : "compact coefficient copy");
            reach.reach(binary ? "JSONB" : "JSON");
            reach.reach("fits whole cap");
            reach.reach("exact PostgreSQL");
            reach.reach("zero resources");
          } finally {
            preparation.mockRestore();
            await core.close();
          }
        },
      ),
      {
        seed,
        numRuns: 6,
        examples: [
          [500000, false, false],
          [500000, true, true],
          [460000, true, false],
        ],
      },
    );
    reach.assertReached();
  },
  120000,
);
