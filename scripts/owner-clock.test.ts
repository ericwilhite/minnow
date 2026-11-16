import fc from "fast-check";
import { expect, it } from "vitest";
import { elapsedMicros } from "../packages/core/browser/next-owner/clock.js";
import { seedsFor } from "../packages/core/src/testing/seeds.js";
import { createReach, OWNER_CLOCK_REACH } from "../packages/core/src/testing/reach.js";
for (const seed of seedsFor("owner-clock", [0xb822, 0xb823]))
  it(`keeps exact browser tick boundaries without accepting over-target ticks seed ${String(seed)}`, () => {
    const reach = createReach("owner clock", OWNER_CLOCK_REACH);
    fc.assert(
      fc.property(
        fc.integer({ min: 1, max: 3_600_000 }),
        fc.constantFrom(5, 20, 100),
        fc.integer({ min: 1, max: 1000 }),
        (baseMs, stepUs, ticks) => {
          reach.beginHistory();
          const us = stepUs * ticks;
          expect(elapsedMicros(baseMs, baseMs + us / 1000)).toBe(us);
          reach.reach("clock tick exact");
          for (const target of [250, 400, 600, 5000]) {
            expect(elapsedMicros(baseMs, baseMs + target / 1000)).toBe(target);
            expect(elapsedMicros(baseMs, baseMs + (target + stepUs) / 1000)).toBeGreaterThan(
              target,
            );
          }
          reach.reach("over target remains over");
        },
      ),
      {
        seed,
        numRuns: 64,
        examples: [
          [1000, 20, 20],
          [3_600_000, 5, 80],
          [1024, 20, 20],
        ],
      },
    );
    // Actual Nov13 failure: a 400 us sample rejected as 400.00000000009095 us.
    expect((1024.4 - 1024) * 1000).toBeGreaterThan(400);
    expect(elapsedMicros(1024, 1024.4)).toBe(400);
    expect(elapsedMicros(1000, 1000.4)).toBe(400);
    expect(elapsedMicros(1024, 1024.400001)).toBeGreaterThan(400);
    reach.assertReached();
  });
