/** Ordinal cuts must read only a bounded overlap plus newly sealed ownership. */
import fc from "fast-check";
import { TaskClock } from "../task.js";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import {
  createReach,
  WRITER_INCREMENTAL_REACH,
  WRITER_FLUSH_CANCEL_REACH,
  WRITER_SMALL_CURSOR_REACH,
  WRITER_MIXED_CUT_REACH,
} from "../../testing/reach.js";
import {
  MetadataJournal,
  MetadataWorkspace,
  METADATA_WORKSPACE_BYTES,
  METADATA_PAYLOAD_BYTES,
} from "./metadata-journal.js";
import { ScriptedMetadataPages } from "./metadata-test-support.js";
import { countingBudget } from "./space-test-support.js";
import { MAX_SLAB } from "./slabs.js";
import { openPairs } from "./metadata-extents.js";
import { disposeSnapshot } from "./extent-snapshot.js";
import { pairPhase } from "./metadata-work.js";
import { METADATA_SMALL_BYTES } from "./contract.js";
import {
  makeRetirementInventory,
  RETIREMENT_OWNER_BYTES,
  RETIREMENT_CURSOR_BYTES,
} from "./retirement-inventory.js";
import { WriterInventory, writerInventoryBytes } from "./writer-inventory.js";

for (const seed of seedsFor("next-writer-incremental", [0xb945, 0xb946]))
  for (const stride of [17, 65])
    it(
      `resumes ${String(stride)}-record cuts seed ${String(seed)}`,
      async (context) => {
        const stop = new AbortController();
        let cleanup: (() => Promise<void>) | undefined;
        context.onTestFinished(async () => {
          stop.abort();
          await cleanup?.();
        });
        const reach = createReach("incremental sealed ownership", WRITER_INCREMENTAL_REACH);
        await fc.assert(
          fc.asyncProperty(
            fc.integer({ min: 64, max: 80 }),
            fc.boolean(),
            async (parts, reverse) => {
              stop.signal.throwIfAborted();
              reach.beginHistory();
              const memory = countingBudget(METADATA_WORKSPACE_BYTES),
                credit = countingBudget(writerInventoryBytes(64) + MAX_SLAB),
                workspace = MetadataWorkspace.create(memory),
                pages = new ScriptedMetadataPages(),
                journal = new MetadataJournal(pages, workspace);
              expect(credit.allocate(writerInventoryBytes(64) + MAX_SLAB)).toBe(true);
              const inventory = new WriterInventory(journal, 64, {
                close: () => credit.free(writerInventoryBytes(64)),
              });
              let slab: Uint8Array | undefined = new Uint8Array(MAX_SLAB),
                handle = 0,
                closing: Promise<void> | undefined;
              const close = () =>
                (closing ??= (async () => {
                  try {
                    if (handle !== 0) await disposeSnapshot(journal, handle);
                  } finally {
                    handle = 0;
                    await inventory.closeInside();
                    slab = undefined;
                    credit.free(MAX_SLAB);
                    workspace.close();
                  }
                  expect(credit.used).toBe(0);
                  expect(memory.used).toBe(0);
                  expect(pages.native).toBe(0);
                  expect(inventory.residentSlots).toBe(0);
                })());
              cleanup = close;
              const expected = (ordinal: number) => ({
                offset: (reverse ? parts * stride + 1 - ordinal : ordinal) * 4096,
                length: 1 + (ordinal % 1024),
              });
              let total = 0,
                reads = 0;
              const capture = async (since: number) => {
                stop.signal.throwIfAborted();
                if (slab === undefined) throw new Error("Missing funded capture scratch");
                const io = { slab },
                  before = pages.reads,
                  snapshot = await inventory.captureInside(io, since, { signal: stop.signal });
                reads += pages.reads - before;
                handle = snapshot.handle;
                const oracle = Array.from({ length: total - since }, (_, i) =>
                  expected(since + i + 1),
                ).sort((a, b) => a.offset - b.offset);
                expect(snapshot.count).toBe(oracle.length);
                await pairPhase(journal, io, "merge", async (slots) => {
                  const cursor = await openPairs(journal, handle, slots.left, true);
                  try {
                    let at = 0;
                    while (await cursor.next({ signal: stop.signal })) {
                      expect({ offset: cursor.offset, length: cursor.length }).toEqual(
                        oracle[at++],
                      );
                    }
                    expect(at).toBe(oracle.length);
                  } finally {
                    await cursor.close();
                  }
                });
                await disposeSnapshot(journal, handle);
                handle = 0;
              };
              try {
                for (let part = 0; part < parts; part++) {
                  const since = total;
                  for (let i = 0; i < stride; i++) {
                    stop.signal.throwIfAborted();
                    if (inventory.needsFlush) await inventory.flushInside({ signal: stop.signal });
                    inventory.append(expected(++total), 4096);
                    expect(inventory.residentSlots).toBeLessThanOrEqual(128);
                  }
                  await capture(since);
                }
                // Includes snapshot encoding/sort reads, not only the source walk. The fixed
                // allowance per cut permits one old chunk overlap; a head restart is quadratic.
                expect(reads).toBeLessThanOrEqual(32 * parts + 4 * Math.ceil(total / 64));
                reach.reach("linear capture IO");
                reach.reach("partial chunk and new chunks");
                reach.reach("bounded resident ownership");
                await capture(Math.floor(total / 3));
                reach.reach("older cut falls back exactly");
                await capture(0);
                reach.reach("full snapshot remains exact");
              } finally {
                await close();
              }
              reach.reach("all ownership returned");
            },
          ),
          { seed, numRuns: 2, examples: [[64, true]] },
        );
        reach.assertReached();
      },
      process.env.CI ? 120_000 : 40_000,
    );

for (const seed of seedsFor("next-writer-compact-flush", [0xb947, 0xb948]))
  it(`retries a cancelled compact ownership flush seed ${String(seed)}`, async () => {
    const reach = createReach("compact writer flush retry", WRITER_FLUSH_CANCEL_REACH);
    await fc.assert(
      fc.asyncProperty(fc.integer({ min: 3, max: 12 }), async (boundary) => {
        reach.beginHistory();
        const memory = countingBudget(METADATA_WORKSPACE_BYTES),
          workspace = MetadataWorkspace.create(memory),
          pages = new ScriptedMetadataPages(),
          journal = new MetadataJournal(pages, workspace),
          credit = countingBudget(writerInventoryBytes(64) + MAX_SLAB);
        expect(credit.allocate(writerInventoryBytes(64) + MAX_SLAB)).toBe(true);
        const inventory = new WriterInventory(journal, 64, {
            close: () => credit.free(writerInventoryBytes(64)),
          }),
          stop = new AbortController(),
          reason = new Error("cancel compact flush");
        let cancelled = false,
          largest = 0;
        let slab: Uint8Array | undefined = new Uint8Array(MAX_SLAB);
        try {
          for (let i = 1; i <= 64; i++) inventory.append({ offset: i * 4096, length: i }, 4096);
          pages.afterWrite = (_page, bytes) => {
            largest = Math.max(largest, bytes.length);
            if (!cancelled && pages.writes === boundary) {
              cancelled = true;
              stop.abort(reason);
            }
          };
          await expect(inventory.flushInside({ signal: stop.signal })).rejects.toBe(reason);
          expect(cancelled).toBe(true);
          reach.reach("cancel after scratch writes");
          expect(inventory.needsFlush).toBe(true);
          expect(inventory.resident).toHaveLength(64);
          reach.reach("resident ownership survives cancel");
          await inventory.flushInside();
          expect(largest).toBeLessThanOrEqual(METADATA_SMALL_BYTES);
          reach.reach("compact pages only");
          pages.afterWrite = null;
          inventory.append({ offset: 65 * 4096, length: 65 }, 4096);
          const captured = await inventory.captureInside({ slab });
          try {
            expect(captured.count).toBe(65);
            reach.reach("retry exact ownership");
          } finally {
            await disposeSnapshot(journal, captured.handle);
          }
        } finally {
          pages.afterWrite = null;
          await inventory.closeInside();
          // eslint-disable-next-line no-useless-assignment -- drop the owned scratch reference before refunding its charge
          slab = undefined;
          credit.free(MAX_SLAB);
          workspace.close();
        }
        expect(credit.used).toBe(0);
        expect(memory.used).toBe(0);
        expect(pages.native).toBe(0);
        reach.reach("all cancellation credit returned");
      }),
      { seed, numRuns: 3, examples: [[3], [8], [12]] },
    );
    reach.assertReached();
  });

for (const seed of seedsFor("next-writer-small-cursor", [0xb949, 0xb94a]))
  it(`bounds small descriptor cursor buffers seed ${String(seed)}`, async () => {
    const reach = createReach("writer descriptor cursor windows", WRITER_SMALL_CURSOR_REACH);
    await fc.assert(
      fc.asyncProperty(fc.integer({ min: 0, max: 8192 }), async (count) => {
        reach.beginHistory();
        const budget = countingBudget(RETIREMENT_OWNER_BYTES + RETIREMENT_CURSOR_BYTES);
        expect(budget.allocate(RETIREMENT_OWNER_BYTES)).toBe(true);
        let reads = 0;
        const inventory = makeRetirementInventory(
          {
            owner: {},
            read: (_handle, position, target) => {
              reads++;
              expect(target.byteLength).toBe(Math.min(METADATA_PAYLOAD_BYTES, count * 16));
              reach.reach(
                count * 16 < METADATA_PAYLOAD_BYTES ? "count bounded window" : "maximum window",
              );
              const perPage = METADATA_PAYLOAD_BYTES / 16,
                start = (position?.pages ?? 0) * perPage,
                take = Math.min(perPage, count - start),
                view = new DataView(target.buffer, target.byteOffset, target.byteLength);
              for (let i = 0; i < take; i++) {
                view.setFloat64(i * 16, (start + i + 1) * 4096, true);
                view.setFloat64(i * 16 + 8, 1, true);
              }
              const remaining = (count - start - take) * 16;
              return Promise.resolve({
                bytes: take * 16,
                position: {
                  next: remaining === 0 ? 0 : 2,
                  remaining,
                  pages: (position?.pages ?? 0) + 1,
                },
              });
            },
            dispose: () => Promise.resolve(),
            finished: () => undefined,
          },
          1,
          count,
          budget,
        );
        const cursor = inventory.cursor({ budget });
        try {
          let at = 0;
          while (await cursor.next()) {
            expect(cursor.offset).toBe(++at * 4096);
            expect(cursor.length).toBe(1);
          }
          expect(at).toBe(count);
          if (count === 0) {
            expect(reads).toBe(0);
            reach.reach("empty no read");
          }
        } finally {
          await cursor.close();
          await inventory.release();
        }
        expect(budget.used).toBe(0);
        reach.reach("all cursor credit returned");
      }),
      { seed, numRuns: 6, examples: [[0], [1], [64], [4096]] },
    );
    reach.assertReached();
  });

for (const seed of seedsFor("next-writer-mixed-cuts", [0xb951, 0xb952]))
  for (const resident of [false, true])
    it(`seeks after resident-only cuts ${String(resident)} seed ${String(seed)}`, async () => {
      const reach = createReach("mixed resident and journal cuts", WRITER_MIXED_CUT_REACH);
      await fc.assert(
        fc.asyncProperty(fc.integer({ min: 16, max: 24 }), async (parts) => {
          reach.beginHistory();
          const memory = countingBudget(METADATA_WORKSPACE_BYTES),
            workspace = MetadataWorkspace.create(memory),
            pages = new ScriptedMetadataPages(),
            journal = new MetadataJournal(pages, workspace),
            credit = countingBudget(writerInventoryBytes(64) + MAX_SLAB + 4096);
          expect(credit.allocate(writerInventoryBytes(64) + MAX_SLAB + 4096)).toBe(true);
          const inventory = new WriterInventory(journal, 64, {
            close: () => credit.free(writerInventoryBytes(64)),
          });
          let slab: Uint8Array | undefined = new Uint8Array(MAX_SLAB),
            handle = 0,
            total = 0;
          const append = async (n: number) => {
            for (let i = 0; i < n; i++) {
              if (inventory.needsFlush) await inventory.flushInside();
              inventory.append({ offset: ++total * 4096, length: 1 }, 4096);
            }
          };
          try {
            for (let part = 0; part < parts; part++) {
              const since = total;
              await append(64);
              if (resident) {
                let copied = inventory.copyTail(since, { clock: new TaskClock(() => 0) });
                expect(copied?.count).toBe(64);
                // Release the prepaid payload before the next capture.
                copied = undefined;
              } else {
                const result = await inventory.captureInside({ slab }, since);
                handle = result.handle;
                expect(result.count).toBe(64);
                await disposeSnapshot(journal, handle);
                handle = 0;
              }
            }
            reach.reach("many resident-only increasing cuts");
            const since = total;
            await append(65);
            const before = pages.reads,
              result = await inventory.captureInside({ slab }, since);
            handle = result.handle;
            expect(pages.reads - before).toBeLessThanOrEqual(32);
            reach.reach("next crossing cut has bounded old overlap");
            expect(result.count).toBe(65);
            await pairPhase(journal, { slab }, "merge", async (slots) => {
              const cursor = await openPairs(journal, handle, slots.left, true);
              let i = since;
              try {
                while (await cursor.next()) {
                  expect(cursor.offset).toBe(++i * 4096);
                  expect(cursor.length).toBe(1);
                }
                expect(i).toBe(total);
              } finally {
                await cursor.close();
              }
            });
            reach.reach("exact crossing suffix");
          } finally {
            if (handle !== 0) await disposeSnapshot(journal, handle);
            await inventory.closeInside();
            // eslint-disable-next-line no-useless-assignment -- drop scratch before refund
            slab = undefined;
            credit.free(MAX_SLAB + 4096);
            workspace.close();
          }
          expect(credit.used).toBe(0);
          expect(memory.used).toBe(0);
          expect(pages.native).toBe(0);
          reach.reach("all mixed-cut credit returned");
        }),
        { seed, numRuns: 2, examples: [[16]] },
      );
      reach.assertReached();
    });
