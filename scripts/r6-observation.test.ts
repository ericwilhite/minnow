import fc from "fast-check";
import { expect, it } from "vitest";
import { r6Policy, validateLatencySample } from "../packages/core/src/next/testing/r6-policy.js";
import { seedsFor } from "../packages/core/src/testing/seeds.js";
import { createReach, R6_OBSERVATION_REACH } from "../packages/core/src/testing/reach.js";

for (const seed of seedsFor("r6-observation", [47142, 47143]))
  it(`separates observation from qualified acceptance without dropping invariants seed ${seed}`, () => {
    const reach = createReach("R6 observation", R6_OBSERVATION_REACH);
    fc.assert(
      fc.property(
        fc.integer({ min: 1, max: 10000 }),
        fc.integer({ min: 1, max: 100000 }),
        (count, p50) => {
          reach.beginHistory();
          for (const env of [
            {},
            { MINNOW_R6_MODE: "observe" },
            { MINNOW_R6_MODE: "observe", MINNOW_PERF_LANE: "1" },
          ]) {
            expect(r6Policy(env)).toEqual({
              enforce: false,
              status: "NOT ACCEPTED — observation only",
            });
            expect(() =>
              validateLatencySample({ count, p50, p95: p50 + 100000 }, count),
            ).not.toThrow();
          }
          reach.reach("over-target observation");
          for (const env of [
            { MINNOW_PERF_LANE: "1" },
            { MINNOW_R6_MODE: "accept" },
            { MINNOW_R6_MODE: "accept", MINNOW_R6_QUALIFIED: "0" },
          ])
            expect(() => r6Policy(env)).toThrow("NOT ACCEPTED");
          reach.reach("unqualified acceptance refused");
          expect(r6Policy({ MINNOW_R6_MODE: "accept", MINNOW_R6_QUALIFIED: "1" }).enforce).toBe(
            true,
          );
          reach.reach("qualified target judging");
          for (const sample of [
            { count: count + 1, p50, p95: p50 },
            ...[NaN, Infinity, -1].map((n) => ({ count, p50: n, p95: n })),
          ])
            expect(() => validateLatencySample(sample, count)).toThrow("invariant");
          reach.reach("invariants always enforced");
        },
      ),
      { seed, numRuns: 64 },
    );
    reach.assertReached();
  });

it("keeps scheduled observations separate from the explicitly blocked acceptance job", async () => {
  const { readFileSync } = await import("node:fs");
  const workflow = readFileSync(
    new URL("../.github/workflows/browser-performance.yml", import.meta.url),
    "utf8",
  );
  const owner = workflow.split("\n  owner:\n")[1]?.split("\n  owner-acceptance:\n");
  expect(owner?.[0]?.match(/MINNOW_R6_MODE: "observe"/g)).toHaveLength(2);
  expect(owner?.[0]).not.toContain("MINNOW_PERF_LANE");
  expect(owner?.[1]).toContain("R6 NOT ACCEPTED");
  expect(owner?.[1]).toContain("exit 1");
  expect(owner?.[1]).not.toContain("continue-on-error");
});
