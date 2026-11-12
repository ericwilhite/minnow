import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../packages/core/src/testing/seeds.js";
import { createReach, QUERY_ROOT_VOLUME_REACH } from "../packages/core/src/testing/reach.js";
import { queryLifetimes } from "./lib/query-root-lifetimes.mts";
for (const seed of seedsFor("query-root-lifetime-volume", [0xb2f3, 0xb2f4]))
  it(`measures exactly the claimed complete work (${String(seed)})`, async () => {
    const reach = createReach("query root volume", QUERY_ROOT_VOLUME_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.oneof(fc.constant(1), fc.integer({ min: 2, max: 20 })),
        fc.oneof(fc.constant(0), fc.integer({ min: 1, max: 65536 })),
        async (repeats, rows) => {
          reach.beginHistory();
          let actualCalls = 0,
            actualRows = 0;
          const value = await queryLifetimes(repeats, rows, async () => {
            actualCalls++;
            actualRows += rows;
            return { rows };
          });
          expect(actualCalls).toBe(repeats);
          expect(actualRows).toBe(rows * repeats);
          expect(value).toEqual({
            rows: actualRows,
            detail: { completedLifetimes: actualCalls, rowsPerLifetime: rows },
          });
          reach.reach(repeats === 1 ? "one lifetime" : "many lifetimes");
          reach.reach(rows === 0 ? "empty result" : "nonempty result");
          reach.reach("exact callback work");
          await expect(
            queryLifetimes(repeats, rows, async () => ({ rows: rows + 1 })),
          ).rejects.toThrow("row count drift");
          reach.reach("volume drift refusal");
        },
      ),
      { seed, numRuns: 32 },
    );
    reach.assertReached();
  });
