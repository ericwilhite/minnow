/** A native merge prepays its own I/O; the legacy loan reserve is not counted twice. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, WRITE_PRIVATE_MERGE_REACH } from "../../testing/reach.js";
import { Governor, type Grant } from "../memory/governor.js";
import { WriteCredits } from "./credits.js";

for (const seed of seedsFor("next-write-private-merge-progress", [47233, 47234]))
  it(`admits the complete private merge through the whole cap (seed ${String(seed)})`, async () => {
    const reach = createReach("prepaid private merge", WRITE_PRIVATE_MERGE_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.constantFrom(8, 16, 32),
        fc.integer({ min: 1, max: 16 }),
        async (cap, units) => {
          reach.beginHistory();
          const governor = new Governor(cap * (1 << 20));
          const credits = await WriteCredits.open(governor, {
            rows: 0,
            metadata: 0,
            commits: 0,
            merge: 0,
          });
          const bytes =
            governor.capBytes - governor.reserveBytes + (units * governor.reserveBytes) / 16;
          let merge: Grant | undefined, buffer: Uint8Array | undefined;
          try {
            expect(bytes).toBeGreaterThan(governor.capBytes - governor.reserveBytes);
            expect(bytes).toBeLessThanOrEqual(governor.capBytes);
            reach.reach("complete step exceeds ordinary capacity");
            merge = await credits.acquireMerge("complete private I/O step", bytes);
            expect(merge.held).toBe(bytes);
            expect(merge.loaned).toBe(0);
            reach.reach("whole cap admitted without loan");
            expect(merge.allocate(bytes)).toBe(true);
            buffer = new Uint8Array(bytes).fill(29);
            expect(buffer.at(-1)).toBe(29);
            reach.reach("all private buffers funded");
            reach.reach(units === 16 ? "exact whole cap" : "below whole cap");
            expect(governor.peakBytes).toBeLessThanOrEqual(governor.capBytes);
          } finally {
            // eslint-disable-next-line no-useless-assignment -- Drop the private buffer before returning its credit.
            buffer = undefined;
            if (merge) {
              merge.free(merge.allocated);
              credits.releaseMerge(merge);
            }
            credits.close();
          }
          expect(governor.usedBytes).toBe(0);
          reach.reach("zero final charge");
        },
      ),
      {
        seed,
        numRuns: 15,
        examples: [
          [8, 16],
          [16, 1],
          [32, 8],
        ],
        endOnFailure: true,
      },
    );
    reach.assertReached();
  });
