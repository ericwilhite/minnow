/** Invalid identity must be refused before copying; producer cleanup cannot mask format refusal. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, SNAPSHOT_INPUT_REACH } from "../../testing/reach.js";
import { SnapshotStreamReader, SnapshotStreamWriter, encodeSnapshotHeader } from "./snapshot.js";

class Budget {
  used = 0;
  allocations = 0;
  allocate(n: number): boolean {
    this.used += n;
    this.allocations++;
    return true;
  }
  free(n: number): void {
    this.used -= n;
    if (this.used < 0) throw new Error("double free");
  }
}
for (const seed of seedsFor("next-snapshot-input", [0xb25b, 0xb25c]))
  it(`preflights identity and preserves primary refusals seed ${String(seed)}`, async () => {
    const reach = createReach("snapshot input", SNAPSHOT_INPUT_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 1025, max: 8192 }),
        fc.integer({ min: 2, max: 65535 }),
        async (length, version) => {
          reach.beginHistory();
          for (const size of [0, 15, 17, length]) {
            const id = new Uint8Array(size),
              b = new Budget();
            let copies = 0,
              writes = 0;
            const copy = id.slice.bind(id);
            Object.defineProperty(id, "slice", {
              value: () => {
                copies++;
                return copy();
              },
            });
            await expect(
              SnapshotStreamWriter.open(
                { id, sourceLsn: 0 },
                {
                  write: () => {
                    writes++;
                  },
                },
                b,
              ),
            ).rejects.toMatchObject({ code: "MINNOW_SNAPSHOT_FORMAT" });
            expect(copies).toBe(0);
            expect(b.allocations).toBe(0);
            expect(b.used).toBe(0);
            expect(writes).toBe(0);
            reach.reach(size < 16 ? "short id" : "long id");
            reach.reach("no writes");
            reach.reach("zero charges");
          }
          for (const lsn of [-1, 0.5, NaN, Infinity, 2 ** 53]) {
            const b = new Budget();
            await expect(
              SnapshotStreamWriter.open(
                { id: new Uint8Array(16), sourceLsn: lsn },
                {
                  write: () => {
                    throw new Error("invalid header delivered");
                  },
                },
                b,
              ),
            ).rejects.toMatchObject({ code: "MINNOW_SNAPSHOT_FORMAT" });
            expect(b.allocations).toBe(0);
            expect(b.used).toBe(0);
            reach.reach("invalid lsn");
          }
          for (const legacy of [false, true])
            for (const asyncClose of [false, true]) {
              const bytes = legacy ? new TextEncoder().encode("MINSNAP1") : new Uint8Array(64);
              if (!legacy) {
                encodeSnapshotHeader({ id: new Uint8Array(16), sourceLsn: 0 }, bytes);
                new DataView(bytes.buffer).setUint16(4, version, true);
              }
              let pulled = false,
                returned = 0;
              const source: AsyncIterable<Uint8Array> = {
                [Symbol.asyncIterator]() {
                  return {
                    next: async () =>
                      pulled
                        ? { done: true, value: undefined }
                        : ((pulled = true), { done: false, value: bytes }),
                    return: () => {
                      returned++;
                      if (!asyncClose) throw new Error("producer cleanup failed");
                      return Promise.reject(new Error("producer cleanup failed"));
                    },
                  };
                },
              };
              const b = new Budget();
              await expect(SnapshotStreamReader.open(source, b)).rejects.toMatchObject({
                code: legacy ? "MINNOW_LEGACY_DATA" : "MINNOW_SNAPSHOT_FORMAT",
              });
              expect(returned).toBe(1);
              expect(b.used).toBe(0);
              reach.reach(legacy ? "legacy refusal" : "version refusal");
              reach.reach(asyncClose ? "async close failure" : "sync close failure");
            }
        },
      ),
      { seed, numRuns: 8 },
    );
    reach.assertReached();
  });
