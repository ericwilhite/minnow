/** Borrow completion is a structural lifetime rule, not an elapsed-time assertion. */
import { execFileSync } from "node:child_process";
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../packages/core/src/testing/seeds.js";
import { createReach, ENCODED_PROJECT_BORROW_REACH } from "../packages/core/src/testing/reach.js";
import type { EncodedBorrowCase } from "./next-encoded-project-lifetime-probe.mts";

it.each(seedsFor("next-encoded-project-borrow", [0xb4e9, 0xb4ea]))(
  "releases expired input and binding metadata while projections remain live (seed %i)",
  async (seed) => {
    const reach = createReach("encoded projection borrow", ENCODED_PROJECT_BORROW_REACH),
      cases: EncodedBorrowCase[] = [1, 2, 4].flatMap((depth) =>
        [false, true].flatMap((blocked) =>
          [false, true].map((throwing) => ({ depth, blocked, throwing })),
        ),
      );
    await fc.assert(
      fc.asyncProperty(
        fc.shuffledSubarray(cases, { minLength: cases.length, maxLength: cases.length }),
        async (histories) => {
          const result = JSON.parse(
            execFileSync(
              process.execPath,
              [
                "--expose-gc",
                "--max-old-space-size=512",
                "scripts/next-encoded-project-lifetime-probe.mts",
                JSON.stringify(histories),
              ],
              { encoding: "utf8", timeout: 60_000, maxBuffer: 4 * 1024 * 1024 },
            ),
          ) as { cases: number; retainedInputBytes: number; retainedBindings: number };
          expect(result).toEqual({ cases: 12, retainedInputBytes: 0, retainedBindings: 0 });
          for (const history of histories) {
            reach.beginHistory();
            reach.reach(history.depth === 1 ? "single projection" : "nested projections");
            reach.reach(history.blocked ? "after readiness" : "synchronous readiness");
            reach.reach(history.throwing ? "receiver error" : "successful consumption");
            reach.reach("expired source collected");
            reach.reach("binding metadata collected");
          }
        },
      ),
      { seed, numRuns: 12, endOnFailure: true },
    );
    reach.assertReached();
  },
  120_000,
);
