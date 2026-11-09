/** Native retention witness while an early-stopped cursor remains live across cache eviction. */
import { execFileSync } from "node:child_process";
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../packages/core/src/testing/seeds.js";
import {
  createReach,
  WRITE_RANGE_NAVIGATION_LIFETIME_STATES,
} from "../packages/core/src/testing/reach.js";
import type { NavigationLifetimeCase } from "./write-navigation-lifetime-probe.mts";
const reach = createReach(
  "evicted navigation native lifetime",
  WRITE_RANGE_NAVIGATION_LIFETIME_STATES,
);
for (const seed of seedsFor("next-write-navigation-lifetime", [0xb3d2]))
  it(`does not root evicted payload from a live cursor (seed ${String(seed)})`, async () => {
    const all = [1024, 16384, 131072].flatMap((rows) =>
      (["desc", "bounded asc", "bounded desc"] as const).map((mode) => ({ rows, mode })),
    );
    await fc.assert(
      fc.asyncProperty(
        fc.shuffledSubarray(all, { minLength: all.length, maxLength: all.length }),
        async (plan) => {
          const data = JSON.parse(
            execFileSync(
              process.execPath,
              ["--expose-gc", "scripts/write-navigation-lifetime-probe.mts", JSON.stringify(plan)],
              {
                encoding: "utf8",
                timeout: 60000,
                maxBuffer: 1024 * 1024,
              },
            ),
          ) as Array<NavigationLifetimeCase & { witnesses: number; retainedBytes: number }>;
          expect(data).toHaveLength(9);
          for (const s of data) {
            expect(s.retainedBytes).toBe(0);
            expect(s.witnesses).toBeGreaterThanOrEqual(2);
            reach.beginHistory();
            reach.reach(s.mode);
            reach.reach(
              s.rows === 1024 ? "1024 keys" : s.rows === 16384 ? "16384 keys" : "131072 keys",
            );
          }
        },
      ),
      { seed, numRuns: 6, endOnFailure: true },
    );
    reach.assertReached();
  }, 120000);
