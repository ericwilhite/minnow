/** Representation retries may change storage, never the logical statement prefix. */
import fc from "fast-check";
import { expect, it, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, NATIVE_PREFIX_MINIMUM_LIFETIME_REACH } from "../../testing/reach.js";
import { decimal, decimals, valueAt } from "../exec/kernels/test-support.js";
import { batch, ints, texts } from "../write/test-support.js";
import { prefixFixture } from "./prefix-images.test-support.js";

for (const seed of seedsFor("next-native-prefix-minimum-lifetime", [0xb731, 0xb732]))
  it(`fences prefix retries without holding input seed ${String(seed)}`, async () => {
    const reach = createReach("prefix minimum lifetime", NATIVE_PREFIX_MINIMUM_LIFETIME_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.constantFrom("mutate", "cancel", "publish"),
        fc.boolean(),
        async (mode, later) => {
          reach.beginHistory();
          const f = await prefixFixture(1, 80000, false, false, false, 32 << 20, true),
            readers = f.statement.tx.hooks.shared.readers,
            base = readers.count,
            append = vi.spyOn(f.images, "append"),
            reason = new Error("cancel prefix funding");
          let preparations = 0;
          f.setPrepare(async () => {
            expect(readers.count).toBe(base);
            expect(f.source.allocated).toBe(0);
            expect(f.source.loaned).toBe(0);
            preparations++;
            if (preparations !== (later ? 2 : 1)) return;
            if (later) expect(f.source.held).toBe(0);
            if (mode === "mutate")
              await f.statement
                .table(f.tableId)
                .insert(
                  batch(
                    ints([2]),
                    decimals(decimal(38, 0), [[2n, 0]], "wide", 0),
                    texts(["later"]),
                  ),
                  null,
                );
            else if (mode === "cancel") f.stop.abort(reason);
            else {
              // Freeze committed storage directly: log recycling requires the writer slot
              // already owned by this statement. Native flush publication does not.
              const memtable = f.statement.tx.hooks.liveMemtable(f.tableId, 0).memtable;
              memtable.state = "frozen";
              memtable.measure();
              await f.path.quiesce();
            }
          });
          try {
            const id = f.ids[0];
            if (id === undefined) throw new Error("missing prefix row");
            const capture = f.reader.capture(f.tableId, id, f.stop.signal);
            if (mode === "mutate") {
              await expect(capture).rejects.toThrow("locator-only after mutation begins");
              expect(append).not.toHaveBeenCalled();
              reach.reach("logical mutation rejected");
            } else if (mode === "cancel") {
              await expect(capture).rejects.toBe(reason);
              expect(append).not.toHaveBeenCalled();
              reach.reach("cancelled funding");
            } else {
              const ref = await capture,
                lease = await f.images.readRowImages(ref, f.stop.signal);
              try {
                const value = lease.old?.columns[2];
                if (!value) throw new Error("missing OLD payload");
                expect(valueAt(value, 0)).toBe(f.payloads[0]);
              } finally {
                lease.release();
              }
              reach.reach("publication retry preserves prefix");
            }
            reach.reach(later ? "wider funding boundary" : "initial funding boundary");
            expect(readers.count).toBe(base);
            expect(f.source.allocated).toBe(0);
            expect(f.source.loaned).toBe(0);
          } finally {
            append.mockRestore();
            await f.close();
          }
          reach.reach("zero ownership");
        },
      ),
      {
        seed,
        numRuns: 10,
        examples: [
          ["mutate", false],
          ["mutate", true],
          ["cancel", false],
          ["cancel", true],
          ["publish", false],
          ["publish", true],
        ],
      },
    );
    reach.assertReached();
  }, 120000);
