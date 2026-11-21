/**
 * A native step's minimum at the capacity boundary (writer floors steps 1-3, review of #34). A
 * native step protects exactly the step it runs: its live row and commit allocations and its
 * metadata step, never a row, commit or merge minimum (a commit, sequence frame or merge funds its
 * own window when it starts). Law: prepareNativeMinimum is served exactly when that need fits the
 * ordinary capacity (cap - reserve), whatever forecasts were retired before, and is refused with
 * 53200 at once otherwise. A row, commit or merge minimum in the native growth over-asks and
 * refuses a need that fits.
 */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, NATIVE_MINIMUM_BOUNDARY_REACH } from "../../testing/reach.js";
import { Governor, MemoryAdmissionError } from "../memory/governor.js";
import { WriteCredits } from "./credits.js";

const KiB = 1024;

it("serves a native minimum that fits exactly after idle forecasts were retired", async () => {
  // The reviewer's history: 4 MiB, every minimum 64 KiB, forecasts retired, then a native minimum
  // whose metadata headroom fills the ordinary capacity (no row, commit or merge minimum beside it).
  const governor = new Governor(4 << 20);
  const minimum = { rows: 64 * KiB, metadata: 64 * KiB, commits: 64 * KiB, merge: 64 * KiB };
  const credits = await WriteCredits.open(governor, minimum);
  try {
    credits.reclaimIdleForecasts();
    const headroom = governor.capBytes - governor.reserveBytes;
    await credits.prepareNativeMinimum(headroom);
    expect(credits.merge.held).toBe(0);
    expect(credits.metadata.held).toBe(headroom);
    expect(credits.rows.held).toBe(0);
    expect(credits.commits.held).toBe(0);
  } finally {
    credits.close();
  }
  expect(governor.usedBytes).toBe(0);
});

for (const seed of seedsFor("next-native-minimum-boundary", [50961, 50962]))
  it(`serves a native minimum exactly when it fits (${String(seed)})`, async () => {
    const reach = createReach("native minimum boundary", NATIVE_MINIMUM_BOUNDARY_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.constantFrom(4, 8, 16),
        fc.record({
          rows: fc.integer({ min: 4, max: 512 }),
          metadata: fc.integer({ min: 4, max: 256 }),
          commits: fc.integer({ min: 4, max: 512 }),
          merge: fc.integer({ min: 4, max: 512 }),
        }),
        fc.boolean(),
        fc.integer({ min: 0, max: 64 }),
        fc.integer({ min: 0, max: 64 }),
        fc.constantFrom(-KiB, 0, KiB),
        async (capMib, minimumKiB, deplete, liveRowsKiB, liveCommitsKiB, delta) => {
          reach.beginHistory();
          const governor = new Governor(capMib << 20);
          const minimum = {
            rows: minimumKiB.rows * KiB,
            metadata: minimumKiB.metadata * KiB,
            commits: minimumKiB.commits * KiB,
            merge: minimumKiB.merge * KiB,
          };
          const credits = await WriteCredits.open(governor, minimum);
          const liveRows = liveRowsKiB * KiB,
            liveCommits = liveCommitsKiB * KiB;
          let allocatedRows = 0,
            allocatedCommits = 0;
          try {
            // Live allocations the step keeps (they may exceed the minimum), then forecasts
            // retired or kept.
            if (liveRows > 0) {
              await credits.growTo(credits.rows, credits.rows.allocated + liveRows);
              expect(credits.rows.allocate(liveRows)).toBe(true);
              allocatedRows = liveRows;
              reach.reach("live row allocations");
            }
            if (liveCommits > 0) {
              await credits.growTo(credits.commits, credits.commits.allocated + liveCommits);
              expect(credits.commits.allocate(liveCommits)).toBe(true);
              allocatedCommits = liveCommits;
              reach.reach("live commit allocations");
            }
            if (deplete) {
              credits.reclaimIdleForecasts();
              reach.reach("depleted forecasts");
            }
            // The native need: live rows and commits and the metadata step. No minimums.
            const rows = liveRows,
              commits = liveCommits,
              ordinary = governor.capBytes - governor.reserveBytes;
            const headroom = ordinary - rows - commits + delta;
            if (headroom < 0) return;
            const need = rows + commits + headroom;
            let outcome: unknown = "served";
            try {
              await credits.prepareNativeMinimum(headroom);
            } catch (error) {
              outcome = error;
            }
            if (need <= ordinary) {
              expect(outcome, `need ${String(need)} fits ${String(ordinary)}`).toBe("served");
              expect(credits.metadata.held).toBeGreaterThanOrEqual(headroom);
              expect(credits.rows.held).toBeGreaterThanOrEqual(rows);
              expect(credits.commits.held).toBeGreaterThanOrEqual(commits);
              reach.reach(delta === 0 ? "fits exactly" : "fits below the boundary");
            } else {
              expect(outcome).toBeInstanceOf(MemoryAdmissionError);
              expect(outcome).toMatchObject({ sqlState: "53200" });
              reach.reach("refused above the boundary");
            }
            expect(governor.usedBytes).toBeLessThanOrEqual(governor.capBytes);
          } finally {
            if (allocatedRows > 0) credits.rows.free(allocatedRows);
            if (allocatedCommits > 0) credits.commits.free(allocatedCommits);
            credits.close();
          }
          expect(governor.usedBytes).toBe(0);
        },
      ),
      {
        seed,
        numRuns: 60,
        examples: [
          // The reviewer's static history, through the generator.
          [4, { rows: 64, metadata: 64, commits: 64, merge: 64 }, true, 0, 0, 0],
          [8, { rows: 512, metadata: 4, commits: 512, merge: 512 }, false, 64, 64, KiB],
        ],
      },
    );
    reach.assertReached();
  });
