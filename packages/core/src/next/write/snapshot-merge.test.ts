/** Import merges borrow piece descriptors but page only one piece per logical input. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, SNAPSHOT_SORTED_MERGE_REACH } from "../../testing/reach.js";
import { Governor } from "../memory/governor.js";
import type { ExtentReader, ByteBudget } from "../storage/extents.js";
import { blobAddress } from "../storage/extents.js";
import { MemoryExtentSpace } from "../page/memory-extents.js";
import type { RunRef } from "../format/manifest.js";
import { encodeRun, runRef, RunReader } from "./runs.js";
import { mergeSortedRuns, DEFAULT_MERGE_POLICY } from "./merge.js";

for (const seed of seedsFor("next-snapshot-sorted-merge", [0xb28d, 0xb28e]))
  it(`pages continuation inputs under fixed working admission seed ${String(seed)}`, async () => {
    const reach = createReach("snapshot sorted merge", SNAPSHOT_SORTED_MERGE_REACH);
    await fc.assert(
      fc.asyncProperty(fc.integer({ min: 1, max: 50 }), async (salt) => {
        reach.beginHistory();
        for (const pieces of [1, 64])
          for (const cancel of [false, true]) {
            const governor = new Governor(16 << 20),
              input = await governor.admit({
                label: "fixture descriptors",
                minimumBytes: 1 << 20,
                spillable: false,
              }),
              work = await governor.admit({
                label: "fixed merge work",
                minimumBytes: 4 << 20,
                spillable: false,
              }),
              publication = await governor.admit({
                label: "output boundaries",
                minimumBytes: 2 << 20,
                spillable: false,
              });
            expect(input.allocate(2 * pieces * 1024)).toBe(true);
            const space = new MemoryExtentSpace({ cache: false }),
              source = space.beginWrite("piece fixture"),
              target = space.beginWrite("sorted output"),
              groups: RunRef[][] = [[], []],
              blooms = new Set<number>();
            let peak = 0,
              reads = 0,
              afterCancel = 0;
            const controller = new AbortController();
            const budget: ByteBudget = {
              allocate(n) {
                if (work.allocated + n > 4 << 20) return false;
                const ok = work.allocate(n);
                peak = Math.max(peak, work.allocated);
                return ok;
              },
              free(n) {
                work.free(n);
              },
            };
            let entered!: () => void, unblock!: () => void;
            const ready = new Promise<void>((resolve) => {
                entered = resolve;
              }),
              gate = new Promise<void>((resolve) => {
                unblock = resolve;
              });
            const blocked = pieces === 1 ? 1 : 12;
            const reader: ExtentReader = {
              peek(ref) {
                expect(blooms.has(blobAddress(ref))).toBe(false);
                return space.peek(ref);
              },
              async read(refs, options) {
                if (controller.signal.aborted) afterCancel++;
                if (cancel && ++reads === blocked) {
                  entered();
                  await gate;
                }
                for (const ref of refs) expect(blooms.has(blobAddress(ref))).toBe(false);
                return space.read(refs, options);
              },
            };
            let output: Awaited<ReturnType<typeof mergeSortedRuns>> | undefined;
            try {
              for (let lane = 0; lane < 2; lane++)
                for (let p = 0; p < pieces; p++) {
                  const encoded = encodeRun(
                    [0, 1].map((k) => {
                      const id = (p * 2 + k) * 2 + lane,
                        key = new Uint8Array(5);
                      key[0] = 1;
                      new DataView(key.buffer).setUint32(1, salt + Math.floor(id / 3), false);
                      return { key, entries: [{ rowId: id + 1, tombstone: false }] };
                    }),
                  );
                  if (encoded === null) throw new Error("fixture run");
                  try {
                    const written = await source.write(encoded.parts),
                      ref = runRef(encoded, written.blobs),
                      owned = {
                        ...ref,
                        minKey: ref.minKey.slice(),
                        maxKey: ref.maxKey.slice(),
                        ...(p ? { continues: true as const } : {}),
                      };
                    groups[lane]?.push(owned);
                    blooms.add(blobAddress(owned.bloom));
                  } finally {
                    encoded.release();
                  }
                }
              const merging = mergeSortedRuns({
                space: reader,
                writer: target,
                grant: budget,
                outputBudget: publication,
                runs: groups,
                level: 1,
                bottom: true,
                policy: { ...DEFAULT_MERGE_POLICY, splitEntries: 4 },
                stopped: () => controller.signal.aborted,
                signal: controller.signal,
              });
              void merging.catch(() => undefined);
              if (cancel) {
                await ready;
                controller.abort(new Error("cancel piece read"));
                unblock();
                await expect(merging).rejects.toThrow();
                expect(afterCancel).toBe(0);
                reach.reach("blocked cancel");
              } else {
                output = await merging;
                expect(output.entriesOut).toBe(pieces * 4);
                const rows: number[] = [];
                for (const ref of output.runs) {
                  const decoded = await RunReader.open(space, ref, work, work);
                  try {
                    await decoded.cursor(space, work, (_key, rowId, tombstone) => {
                      expect(tombstone).toBe(false);
                      rows.push(rowId);
                      return false;
                    });
                  } finally {
                    work.free(decoded.bytes);
                  }
                }
                expect(rows).toEqual(Array.from({ length: pieces * 4 }, (_, i) => i + 1));
                output.release();
                output = undefined;
                reach.reach("ordered complete output");
              }
              expect(work.allocated).toBe(0);
              expect(publication.allocated).toBe(0);
              expect(space.leased).toBe(0);
              expect(peak).toBeLessThanOrEqual(4 << 20);
              reach.reach(pieces === 1 ? "one piece" : "many pieces");
              reach.reach("no resident blooms");
              reach.reach("fixed working bound");
            } finally {
              unblock();
              output?.release();
              await target.abort();
              await source.abort();
              input.close();
              work.close();
              publication.close();
            }
            expect(governor.usedBytes).toBe(0);
          }
      }),
      { seed, numRuns: 4 },
    );
    reach.assertReached();
  }, 60000);
