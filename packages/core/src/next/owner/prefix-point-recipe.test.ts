/** A point recipe retains exactly its locator's existing owner-charged chunk,
 * including chunk1/2 of a large tail and publication during its bounded hint. */
import fc from "fast-check";
import { expect, it, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, NATIVE_PREFIX_POINT_RECIPE_REACH } from "../../testing/reach.js";
import { valueAt } from "../exec/kernels/test-support.js";
import { MemoryReads } from "../write/memory-read.js";
import { prefixFixture } from "./prefix-images.test-support.js";

for (const seed of seedsFor("next-native-prefix-point-recipe", [0xb733, 0xb734]))
  it(`retains the selected chunk across publication seed ${String(seed)}`, async () => {
    const reach = createReach("prefix point recipe", NATIVE_PREFIX_POINT_RECIPE_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 2050, max: 4100 }),
        fc.boolean(),
        async (count, publish) => {
          reach.beginHistory();
          const f = await prefixFixture(count, 4, false, true, false, 32 << 20, true, 8192);
          // eslint-disable-next-line @typescript-eslint/unbound-method -- Called with the original receiver below.
          const original = MemoryReads.prototype.hint;
          let published = false;
          const hints = vi.spyOn(MemoryReads.prototype, "hint").mockImplementation(async function (
            this: MemoryReads,
            request,
            context,
          ) {
            const hint = await original.call(this, request, context);
            if (publish && !published) {
              published = true;
              // Freeze committed storage directly: log recycling requires the writer slot
              // already owned by this statement. Native flush publication does not.
              const memtable = f.statement.tx.hooks.liveMemtable(f.tableId, 0).memtable;
              memtable.state = "frozen";
              memtable.measure();
              await f.path.quiesce();
              expect(f.path.stats().borrowedChunkBytes).toBeGreaterThan(0);
              reach.reach("owner-charged publication retention");
            }
            return hint;
          });
          try {
            for (const row of [2048, count - 1]) {
              const id = f.ids[row];
              if (id === undefined) throw new Error("missing point row");
              const ref = await f.reader.capture(f.tableId, id, f.stop.signal),
                lease = await f.images.readRowImages(ref, f.stop.signal);
              try {
                const key = lease.old?.columns[0],
                  payload = lease.old?.columns[2];
                if (!key || !payload) throw new Error("missing selected OLD");
                expect(valueAt(key, 0)).toBe(row + 1);
                expect(valueAt(payload, 0)).toBe(f.payloads[row]);
              } finally {
                lease.release();
              }
              expect(f.path.stats().borrowedChunkBytes).toBe(0);
            }
            reach.reach(publish ? "published" : "resident");
            reach.reach(count > 4096 ? "third chunk" : "second chunk");
            reach.reach("exact selected row");
            expect(f.h.governor.peakBytes).toBeLessThanOrEqual(32 << 20);
          } finally {
            hints.mockRestore();
            await f.close();
          }
          reach.reach("zero ownership");
        },
      ),
      {
        seed,
        numRuns: 4,
        examples: [
          [2050, false],
          [4100, true],
        ],
      },
    );
    reach.assertReached();
  }, 120000);
