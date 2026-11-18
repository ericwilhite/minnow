/** The complete fixed native decoder step belongs in ordinary admission, not a value loan. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, NATIVE_PREFIX_FIXED_STEP_REACH } from "../../testing/reach.js";
import { prefixFixture } from "./prefix-images.test-support.js";
import { valueAt } from "../exec/kernels/test-support.js";
for (const seed of seedsFor("next-native-prefix-fixed-step", [0xb643, 0xb644]))
  it(`prepays the complete fixed native source under a small reserve ${String(seed)}`, async () => {
    const reach = createReach("native fixed source step", NATIVE_PREFIX_FIXED_STEP_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.boolean(),
        fc.integer({ min: 0, max: 24000 }),
        fc.integer({ min: 0, max: 2 }),
        async (published, width, row) => {
          reach.beginHistory();
          const f = await prefixFixture(3, width, published, false, false, 64 * 1024 * 1024),
            gov = f.h.governor,
            competing = await gov.admit({
              label: "all competing credit",
              minimumBytes: gov.capBytes - gov.reserveBytes - gov.usedBytes,
              spillable: false,
            });
          expect(competing.allocate(competing.held)).toBe(true);
          try {
            const id = f.ids[row];
            if (id === undefined) throw new Error("Missing native source row");
            const ref = await f.reader.capture(f.tableId, id, f.stop.signal),
              lease = await f.images.readRowImages(ref, f.stop.signal);
            try {
              const key = lease.old?.columns[0];
              if (!key) throw new Error("Missing native OLD");
              expect(valueAt(key, 0)).toBe(row + 1);
            } finally {
              lease.release();
            }
            expect(f.source.loaned).toBe(0);
            expect(f.source.allocated).toBe(0);
            expect(gov.peakBytes).toBeLessThanOrEqual(gov.capBytes);
            reach.reach(published ? "published" : "resident");
            reach.reach(width > 0 ? "payload" : "empty payload");
            reach.reach("64MiB competing credit");
          } finally {
            competing.close();
            await f.close();
          }
          reach.reach("zero ownership");
        },
      ),
      {
        seed,
        numRuns: 6,
        examples: [
          [false, 0, 0],
          [true, 24000, 2],
        ],
      },
    );
    reach.assertReached();
  });
