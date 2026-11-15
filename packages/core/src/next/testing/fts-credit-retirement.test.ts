/** Provisional FTS minima retire idle credit, never live buffers or the overlapping next step. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, FTS_CREDIT_RETIREMENT_REACH } from "../../testing/reach.js";
import { Governor } from "../memory/governor.js";
import { WriteCredits } from "../write/credits.js";
for (const seed of seedsFor("next-fts-credit-retirement", [0xb863, 0xb864]))
  it(`returns cancelled forecasts while funding live bytes plus the next step ${String(seed)}`, async () => {
    const reach = createReach("FTS credit retirement", FTS_CREDIT_RETIREMENT_REACH);
    await fc.assert(
      fc.asyncProperty(fc.integer({ min: 1, max: 4096 }), async (n) => {
        reach.beginHistory();
        const KiB = 1024,
          governor = new Governor(4 * 1024 * KiB),
          minimum = { rows: 64 * KiB, metadata: 64 * KiB, commits: 64 * KiB, merge: 64 * KiB };
        const credits = await WriteCredits.open(governor, minimum);
        let holder: Awaited<ReturnType<Governor["admit"]>> | undefined;
        try {
          await credits.prepareMetadata(256 * KiB);
          expect(credits.metadata.allocate(n)).toBe(true);
          const live = new Uint8Array(n).fill(23);
          reach.reach("live buffer funded");
          const spare = governor.capBytes - governor.reserveBytes - governor.usedBytes;
          holder = await governor.admit({
            label: "FTS retirement competitor",
            minimumBytes: spare,
            spillable: true,
          });
          expect(holder.allocate(spare)).toBe(true);
          const retained = new Uint8Array(spare).fill(7);
          expect(holder.tryGrow(1)).toBe(false);
          reach.reach("all ordinary credit retained");
          const before = credits.metadata.held,
            freed = credits.retireMetadataHeadroom(minimum.metadata);
          expect(freed).toBeGreaterThan(0);
          expect(credits.metadata.held).toBe(before - freed);
          expect(credits.metadata.allocated).toBe(n);
          expect(credits.metadata.held, "live bytes and the next step coexist").toBe(
            n + minimum.metadata,
          );
          reach.reach("overlap funded");
          expect(live[0]).toBe(23);
          expect(retained[0]).toBe(7);
          reach.reach("owned bytes intact");
          expect(holder.tryGrow(freed)).toBe(true);
          reach.reach("idle credit returned");
          expect(credits.prepareMetadata(minimum.metadata)).toBeUndefined();
          credits.metadata.free(live.byteLength);
        } finally {
          holder?.close();
          credits.close();
        }
        expect(governor.usedBytes).toBe(0);
        reach.reach("all accounts returned");
      }),
      { seed, numRuns: 12 },
    );
    reach.assertReached();
  }, 60_000);
