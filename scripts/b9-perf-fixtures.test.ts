/** A fixture's ordinary writer must agree with every runtime allowed to restore it. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../packages/core/src/testing/seeds.js";
import { createReach, B9_PROBE_FIXTURE_REACH } from "../packages/core/src/testing/reach.js";
import { planFixtures, type RuntimeFormat } from "./b9-perf-fixtures.mts";

it.each(seedsFor("b9-probe-fixture-vocabulary", [0xb9f1, 0xb9f2]))(
  "shares compatible images and isolates incompatible development writers (seed %i)",
  (seed) => {
    const reach = createReach("B9 fixture vocabulary", B9_PROBE_FIXTURE_REACH);
    fc.assert(
      fc.property(fc.tuple(...[0, 1, 2].map(() => fc.integer({ min: 1, max: 3 }))), (profiles) => {
        reach.beginHistory();
        const arms = profiles.map((profile, i) => ({
            label: ["main", "candidate", "frozen"][i] ?? "missing",
            root: "/home/eric/.cache/b9-fixture-law/arm" + String(i),
            format: {
              fingerprint: String(profile).repeat(64),
              reader: profile,
              writer: profile,
              manifest: profile,
              envelope: 1,
            } satisfies RuntimeFormat,
          })),
          fixtures = planFixtures(arms, "/home/eric/.cache/b9-fixture-law/output");
        expect(fixtures).toHaveLength(new Set(profiles).size);
        const wide = planFixtures(arms, "/home/eric/.cache/b9-fixture-law/output", "W-1000"),
          star = planFixtures(arms, "/home/eric/.cache/b9-fixture-law/output", "S-0.1"),
          larger = planFixtures(arms, "/home/eric/.cache/b9-fixture-law/output", "S-333");
        expect(star).toEqual(
          planFixtures(arms, "/home/eric/.cache/b9-fixture-law/output", "S-0.1"),
        );
        for (const w of wide)
          for (const s of star) {
            expect(w.image).not.toBe(s.image);
            expect(w.imageIdentity).not.toBe(s.imageIdentity);
          }
        for (const small of star)
          for (const big of larger) {
            expect(small.image).not.toBe(big.image);
            expect(small.imageIdentity).not.toBe(big.imageIdentity);
          }
        reach.reach("logical datasets isolated");
        reach.reach("sizes isolated");
        const paths = new Set<string>();
        for (const fixture of fixtures) {
          expect(paths.has(fixture.image)).toBe(false);
          paths.add(fixture.image);
          const writer = arms.find((arm) => arm.root === fixture.writer);
          expect(writer?.format).toEqual(fixture.format);
          expect(fixture.readers).toContain(fixture.writer);
          for (const reader of fixture.readers)
            expect(arms.find((arm) => arm.root === reader)?.format).toEqual(fixture.format);
          if (fixture.readers.length > 1) reach.reach("shared");
        }
        if (fixtures.length > 1) reach.reach("separate");
        reach.reach("one writer per format");
      }),
      { seed, numRuns: 32 },
    );
    reach.assertReached();
  },
);

it("rejects logical workload names that could escape their output directory", () => {
  expect(() => planFixtures([], "/home/eric/.cache/b9-fixture-law/output", "../S1")).toThrow(
    "Bad logical workload identity",
  );
});
