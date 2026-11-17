/** Fixed ownership windows must survive large histories, holes, reuse and ordinal snapshots. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import {
  createReach,
  WRITER_INVENTORY_REACH,
  WRITER_INVENTORY_STORAGE_REACH,
} from "../../testing/reach.js";
import {
  MetadataJournal,
  MetadataWorkspace,
  METADATA_WORKSPACE_BYTES,
} from "./metadata-journal.js";
import { MetadataMutex } from "./metadata-mutex.js";
import { ScriptedMetadataPages } from "./metadata-test-support.js";
import { countingBudget } from "./space-test-support.js";
import type { ExtentWriter } from "./extents.js";
import { createWriterExtentView } from "./writer-view.js";
import { MAX_SLAB } from "./slabs.js";
import { openPairs } from "./metadata-extents.js";
import { disposeSnapshot } from "./extent-snapshot.js";
import { pairPhase } from "./metadata-work.js";
import { WriterInventory, writerInventoryBytes } from "./writer-inventory.js";
for (const seed of seedsFor("next-writer-inventory", [0xb93d, 0xb93e]))
  for (const count of [63, 64, 65, 32769])
    it(
      `bounds ${String(count)} sealed records seed ${String(seed)}`,
      async (context) => {
        const stop = new AbortController();
        let cleanup: (() => Promise<void>) | undefined;
        context.onTestFinished(async () => {
          stop.abort();
          await cleanup?.();
        });
        const reach = createReach("native sealed inventory", [
          ...WRITER_INVENTORY_REACH,
          WRITER_INVENTORY_STORAGE_REACH[count > 64 ? 1 : 0],
        ]);
        await fc.assert(
          fc.asyncProperty(fc.boolean(), async (reverse) => {
            stop.signal.throwIfAborted();
            reach.beginHistory();
            const memory = countingBudget(METADATA_WORKSPACE_BYTES),
              credit = countingBudget(writerInventoryBytes(64) + MAX_SLAB),
              workspace = MetadataWorkspace.create(memory),
              pages = new ScriptedMetadataPages(),
              journal = new MetadataJournal(pages, workspace),
              mutex = new MetadataMutex();
            expect(credit.allocate(writerInventoryBytes(64))).toBe(true);
            const inventory = new WriterInventory(journal, 64, {
              close: () => credit.free(writerInventoryBytes(64)),
            });
            const view = createWriterExtentView(
              {} as ExtentWriter,
              () => inventory.resident,
              () => inventory.count,
            );
            expect(credit.allocate(MAX_SLAB)).toBe(true);
            let slab: Uint8Array | undefined = new Uint8Array(MAX_SLAB),
              handle = 0,
              closing: Promise<void> | undefined;
            const close = () =>
              (closing ??= (async () => {
                await mutex.run(async () => {
                  try {
                    if (handle !== 0) await disposeSnapshot(journal, handle);
                  } finally {
                    handle = 0;
                    await inventory.closeInside();
                  }
                });
                slab = undefined;
                credit.free(MAX_SLAB);
                workspace.close();
                expect(inventory.residentSlots).toBe(0);
                expect(credit.used).toBe(0);
                expect(memory.used).toBe(0);
                expect(pages.native).toBe(0);
              })());
            cleanup = close;
            const expected: Array<{ offset: number; length: number; ordinal: number }> = [];
            try {
              for (let i = 0; i < count; i++) {
                stop.signal.throwIfAborted();
                if (inventory.needsFlush)
                  await mutex.run(() => inventory.flushInside({ signal: stop.signal }));
                const offset = (reverse ? count - i : i + 1) * 4096,
                  length = 1 + (i % 1024);
                inventory.append({ offset, length }, 4096);
                expect(inventory.residentSlots).toBeLessThanOrEqual(2 * inventory.capacity);
                expected.push({ offset, length, ordinal: i + 1 });
                if (i === 2) {
                  expect(view.slice()).toEqual(
                    expected.map(({ offset, length }) => ({ offset, length })),
                  );
                  view.forEach((extent, _index, receiver) => {
                    expect(receiver).toBe(view);
                    expect(Reflect.set(receiver, "length", 0)).toBe(false);
                    expect(Reflect.set(extent, "length", 999)).toBe(false);
                  });
                  expect(inventory.count).toBe(3);
                }
              }
              expect(inventory.count).toBe(count);
              expect(inventory.bytes).toBe(count * 4096);
              reach.reach("fixed record window");
              reach.reach("bounded hidden record arrays");
              for (const index of [0, Math.floor(count / 2), count - 1]) {
                const extent = expected[index];
                if (!extent) throw new Error("Missing generated sealed extent");
                await mutex.run(async () => {
                  const location = await inventory.findInside(extent, { signal: stop.signal });
                  expect(location).toBeDefined();
                  if (!location) throw new Error("Missing native ownership");
                  await inventory.clearInside(location, 4096);
                });
              }
              const deleted = new Set([0, Math.floor(count / 2), count - 1]);
              if (inventory.needsFlush)
                await mutex.run(() => inventory.flushInside({ signal: stop.signal }));
              const reused = expected[0];
              if (!reused) throw new Error("Missing reuse extent");
              inventory.append(reused, 4096);
              const live = expected.filter((_, i) => !deleted.has(i));
              live.push({ ...reused, ordinal: count + 1 });
              expect(inventory.count).toBe(live.length);
              reach.reach("discard and exact reuse");
              for (const since of [0, Math.floor(count / 2)]) {
                // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition -- timeout cleanup may retire this buffer
                if (!slab) throw new Error("Missing funded scratch");
                const io = { slab };
                const snapshot = await mutex.run(() =>
                  inventory.captureInside(io, since, { signal: stop.signal }),
                );
                handle = snapshot.handle;
                const oracle = live
                  .filter((e) => e.ordinal > since)
                  .sort((a, b) => a.offset - b.offset);
                expect(snapshot.count).toBe(oracle.length);
                await mutex.run(() =>
                  pairPhase(journal, io, "merge", async (slots) => {
                    const pairs = await openPairs(journal, handle, slots.left, true);
                    try {
                      let at = 0;
                      while (await pairs.next({ signal: stop.signal })) {
                        expect({ offset: pairs.offset, length: pairs.length }).toEqual({
                          offset: oracle[at]?.offset,
                          length: oracle[at]?.length,
                        });
                        at++;
                      }
                      expect(at).toBe(oracle.length);
                    } finally {
                      await pairs.close();
                    }
                  }),
                );
                await mutex.run(() => disposeSnapshot(journal, handle));
                handle = 0;
              }
              reach.reach("exact full and ordinal snapshots");
              if (count > 64) reach.reach("spilled ownership");
              else reach.reach("resident ownership");
            } finally {
              await close();
            }
            reach.reach("all ownership credit returned");
          }),
          { seed, numRuns: 2, examples: [[true]] },
        );
        reach.assertReached();
      },
      process.env.CI ? 120_000 : 40_000,
    );
