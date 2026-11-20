/** Metadata-only lease/identity laws, using genuine encoded directories/checksums.
 * Native roots, restore and PostgreSQL rows are covered separately. No payload page
 * is demanded here, and these synthetic page descriptors are not a stored-data oracle.
 */
import fc from "fast-check";
import { expect, it, vi } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, DIRECTORY_CACHE_REACH } from "../../testing/reach.js";
import { Governor } from "../memory/governor.js";
import { DATA_ALIGN, StoreCorruptionError } from "../storage/contract.js";
import type { BlobRef } from "../storage/extents.js";
import type { ColumnChunk } from "../format/manifest.js";
import { PageEncoding } from "../format/manifest.js";
import * as directoryCode from "./directory.js";
import { type ChunkDirectory, encodeDirectory } from "./directory.js";
import { createDirectoryCache } from "./directory-cache.js";
import { ChunkReader } from "./reader.js";
import { ValueTag, PageCorruptionError } from "./format.js";
import { DistinctSketch, SKETCH_PRECISION } from "./sketch.js";
import { MemoryExtentSpace } from "./memory-extents.js";

const MODES = [
  "same blob",
  "extent length",
  "containing extent",
  "invalid extent",
  "reused address",
  "invalid body",
  "short chunk hit",
  "wrong type hit",
  "eviction",
  "once miss",
  "once hit",
  "cache refusal",
  "close while pinned",
  "late read after close",
  "new owner",
  "pending cold close",
  "overlapping cold opens",
] as const;
type Mode = (typeof MODES)[number];
async function store(space: MemoryExtentSpace, bytes: Uint8Array): Promise<BlobRef> {
  const writer = space.beginWrite("directory cache fixture");
  const ref = (await writer.write([bytes])).blobs[0];
  if (!ref) throw new Error("Missing written directory");
  return ref;
}
function body(count: number, salt: number, dense: boolean): ChunkDirectory {
  const sketch = new DistinctSketch(),
    slots = dense ? 1 << SKETCH_PRECISION : 3 + ((salt >>> 0) % 13);
  for (let at = 0; at < slots; at++) sketch.add((at << (32 - SKETCH_PRECISION)) | 1);
  const bytes = sketch.serialize();
  expect(bytes[1]).toBe(dense ? 0 : 1);
  return {
    tag: ValueTag.i32,
    rowCount: count,
    scale: 0,
    hasSpecials: false,
    pages: [
      {
        offset: 0,
        length: 256,
        firstRow: 0,
        rowCount: count,
        encoding: PageEncoding.plain,
        nullCount: 0,
        min: null,
        max: null,
        checksum: 0,
      },
    ],
    dictionary: null,
    sketch: bytes,
  };
}
function deferred() {
  let resolve!: () => void;
  const promise = new Promise<void>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}
it.each(seedsFor("next-directory-cache-identity", [0xb503, 0xb504]))(
  "keeps full identity, hit bounds and charged lifetimes (seed %i)",
  async (seed) => {
    const reach = createReach("directory-cache leases", DIRECTORY_CACHE_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.constantFrom(...MODES),
        fc.integer({ min: 1, max: 31 }),
        fc.integer(),
        fc.boolean(),
        async (mode, count, salt, dense) => {
          reach.beginHistory();
          const space = new MemoryExtentSpace({ cache: false }),
            governor = new Governor(8 << 20),
            cache = createDirectoryCache(governor),
            grant = await governor.admit({
              label: "directory-cache fixture",
              minimumBytes:
                mode === "cache refusal" ? governor.capBytes - governor.reserveBytes : 32768,
              spillable: false,
            }),
            padding = await store(space, new Uint8Array(DATA_ALIGN)),
            original = body(count, salt, dense),
            ref = await store(space, encodeDirectory(original)),
            chunk: ColumnChunk = {
              columnId: 1,
              type: { kind: "int32", bits: 32 },
              directory: ref,
              data: { extent: padding.extent, offset: 0, length: DATA_ALIGN },
              distinctSketch: null,
              aggregateSummary: null,
              statistics: { nullCount: 0, min: null, max: null, dictionarySize: null },
            },
            parse = vi.spyOn(directoryCode, "parseDirectory"),
            readers: ChunkReader[] = [];
          const open = async (source = chunk, once = false, budget = grant) => {
            const reader = await ChunkReader.open(space, source, {
              budget,
              directories: cache,
              ...(once ? { once: true } : {}),
            });
            readers.push(reader);
            return reader;
          };
          try {
            if (mode === "pending cold close" || mode === "overlapping cold opens") {
              const gates = Array.from({ length: mode === "pending cold close" ? 1 : 2 }, () => ({
                entered: deferred(),
                release: deferred(),
              }));
              const other =
                mode === "overlapping cold opens"
                  ? await governor.admit({
                      label: "overlapping directory holder",
                      minimumBytes: 32768,
                      spillable: false,
                    })
                  : grant;
              const holders = [grant, other];
              const read = space.read.bind(space);
              let started = 0;
              const pendingRead = vi
                .spyOn(space, "read")
                .mockImplementation(async (refs, options) => {
                  const gate = gates[started++];
                  if (!gate) throw new Error("Unexpected directory fixture read");
                  const lease = await read(refs, options);
                  // Hold a real, already funded IO lease: the open is in flight before close.
                  gate.entered.resolve();
                  await gate.release.promise;
                  return lease;
                });
              const pending = gates.map((_, at) => open(chunk, false, holders[at]));
              try {
                for (let at = 0; at < gates.length; at++) {
                  const gate = gates[at],
                    request = pending[at];
                  if (!gate || !request) throw new Error("Missing pending open");
                  const phase = await Promise.race([
                    gate.entered.promise.then(() => "entered"),
                    request.then(
                      () => "completed",
                      () => "rejected",
                    ),
                  ]);
                  expect(phase, "cold open must enter deferred IO").toBe("entered");
                }
                expect(parse.mock.calls.length).toBe(0);
                expect(cache.entryCount).toBe(0);
                expect(space.leased).toBe(ref.length * gates.length);
                expect(grant.allocated).toBe(ref.length);
                if (gates.length === 2) expect(other.allocated).toBe(ref.length);
                if (mode === "pending cold close") {
                  reach.reach("pending read owns funded bytes");
                  cache.close();
                  expect(grant.allocated).toBe(ref.length);
                  expect(space.leased).toBe(ref.length);
                  gates[0]?.release.resolve();
                  const completed = await pending[0];
                  if (!completed) throw new Error("Missing completed cold read");
                  expect(completed.directory).toEqual(original);
                  expect(parse.mock.calls.length).toBe(1);
                  expect(space.leased).toBe(0);
                  expect(cache.residentBytes).toBeGreaterThan(0);
                  expect(cache.evictableBytes).toBe(0);
                  completed.close();
                  expect(cache.entryCount).toBe(0);
                  expect(cache.residentBytes).toBe(0);
                  reach.reach("late unpin does not retain");
                } else {
                  reach.reach("overlapping reads own funded bytes");
                  const reverse = (salt & 1) !== 0,
                    firstIndex = reverse ? 1 : 0,
                    lastIndex = reverse ? 0 : 1;
                  gates[firstIndex]?.release.resolve();
                  const first = await pending[firstIndex];
                  if (!first) throw new Error("Missing first completion");
                  const metadata = holders[firstIndex]?.allocated ?? 0;
                  expect(holders[lastIndex]?.allocated).toBe(ref.length);
                  expect(metadata).toBeGreaterThan(cache.residentBytes);
                  expect(cache.residentBytes).toBeGreaterThan(0);
                  expect(cache.evictableBytes).toBe(0);
                  const closesBetween = (salt & 2) !== 0;
                  if (closesBetween) {
                    cache.close();
                    reach.reach("close between completions");
                  } else reach.reach("open through both completions");
                  gates[lastIndex]?.release.resolve();
                  const last = await pending[lastIndex];
                  if (!last) throw new Error("Missing second completion");
                  expect(first.directory, "overlapping opens share the admitted directory").toBe(
                    last.directory,
                  );
                  expect(first.directory).toEqual(original);
                  expect(parse.mock.calls.length).toBe(2);
                  expect(cache.entryCount).toBe(1);
                  expect(space.leased).toBe(0);
                  expect(grant.allocated).toBe(metadata);
                  expect(other.allocated).toBe(metadata);
                  reach.reach("shared directory after overlap");
                  reach.reach(reverse ? "reverse completion" : "forward completion");
                  const closeFirst = (salt & 4) !== 0 ? last : first,
                    closeLast = closeFirst === first ? last : first,
                    closedIndex = closeFirst === first ? firstIndex : lastIndex,
                    remainingIndex = closedIndex === 0 ? 1 : 0;
                  closeFirst.close();
                  expect(holders[closedIndex]?.allocated).toBe(0);
                  expect(holders[remainingIndex]?.allocated).toBe(metadata);
                  expect(cache.evictableBytes, "remaining reader owns its cache pin").toBe(0);
                  expect(cache.evict(Infinity)).toBe(0);
                  expect(cache.entryCount).toBe(1);
                  reach.reach("remaining reader keeps pin");
                  closeLast.close();
                  expect(grant.allocated).toBe(0);
                  expect(other.allocated).toBe(0);
                  if (!closesBetween) {
                    const retained = cache.residentBytes;
                    expect(retained).toBeGreaterThan(0);
                    expect(cache.evictableBytes).toBe(retained);
                    expect(cache.evict(Infinity)).toBe(retained);
                    reach.reach("overlap cache eviction returns bytes");
                  }
                  expect(cache.entryCount).toBe(0);
                  expect(cache.residentBytes).toBe(0);
                }
              } catch (error) {
                // Preserve a failed accounting assertion if a deliberately broken pin
                // design also throws during cleanup of its second holder.
                for (const gate of gates) gate.release.resolve();
                await Promise.allSettled(pending);
                for (const reader of readers) {
                  try {
                    reader.close();
                  } catch {
                    /* primary failure above owns the evidence */
                  }
                }
                throw error;
              } finally {
                for (const gate of gates) gate.release.resolve();
                await Promise.allSettled(pending);
                pendingRead.mockRestore();
                if (other !== grant) other.close();
              }
            } else if (mode === "once miss") {
              for (let pass = 0; pass < 3; pass++) {
                parse.mockClear();
                (await open(chunk, true)).close();
                expect(parse.mock.calls.length).toBe(1);
                expect(cache.entryCount).toBe(0);
              }
            } else if (mode === "invalid body") {
              const bad = original.sketch?.slice();
              if (!bad) throw new Error("Missing sketch");
              if (dense)
                bad[4 + ((salt >>> 0) % (1 << SKETCH_PRECISION))] = 24 + ((salt >>> 0) % 232);
              else bad.copyWithin(7, 4, 6);
              const invalid = await store(space, encodeDirectory({ ...original, sketch: bad }));
              await expect(open({ ...chunk, directory: invalid })).rejects.toBeInstanceOf(
                PageCorruptionError,
              );
              expect(parse.mock.calls.length).toBe(1);
              expect(cache.entryCount).toBe(0);
              reach.reach("invalid body refused on first read");
            } else {
              const first = await open();
              expect(first.rowCount).toBe(count);
              expect(parse.mock.calls.length).toBe(1);
              if (mode === "close while pinned") {
                cache.close();
                expect(cache.residentBytes).toBeGreaterThan(0);
                first.close();
                expect(cache.entryCount).toBe(0);
                expect(cache.residentBytes).toBe(0);
                reach.reach("late unpin does not retain");
              } else {
                first.close();
                parse.mockClear();
                if (mode === "cache refusal") {
                  expect(cache.entryCount).toBe(0);
                  (await open()).close();
                  expect(parse.mock.calls.length).toBe(1);
                } else {
                  expect(cache.residentBytes).toBeGreaterThan(0);
                  expect(cache.residentBytes).toBe(cache.evictableBytes);
                  if (mode === "same blob") {
                    (await open()).close();
                    expect(parse.mock.calls.length).toBe(0);
                  } else if (mode === "extent length" || mode === "containing extent") {
                    const changed =
                      mode === "extent length"
                        ? {
                            ...ref,
                            extent: { ...ref.extent, length: ref.extent.length + DATA_ALIGN },
                          }
                        : {
                            ...ref,
                            extent: {
                              offset: ref.extent.offset - DATA_ALIGN,
                              length: ref.extent.length + DATA_ALIGN,
                            },
                            offset: ref.offset + DATA_ALIGN,
                          };
                    expect(changed.extent.offset + changed.offset).toBe(
                      ref.extent.offset + ref.offset,
                    );
                    (await open({ ...chunk, directory: changed })).close();
                    expect(parse.mock.calls.length).toBe(1);
                    reach.reach("full containing identity checked");
                  } else if (mode === "invalid extent") {
                    await expect(
                      open({
                        ...chunk,
                        directory: {
                          ...ref,
                          extent: { ...ref.extent, length: ref.offset + ref.length - 1 },
                        },
                      }),
                    ).rejects.toBeInstanceOf(StoreCorruptionError);
                    expect(parse.mock.calls.length).toBe(0);
                    reach.reach("full containing identity checked");
                  } else if (mode === "short chunk hit" || mode === "wrong type hit") {
                    const changed =
                      mode === "short chunk hit"
                        ? { ...chunk, data: { ...chunk.data, length: 255 } }
                        : { ...chunk, type: { kind: "float64" } as const };
                    await expect(open(changed)).rejects.toBeInstanceOf(PageCorruptionError);
                    expect(parse.mock.calls.length).toBe(0);
                    expect(cache.evictableBytes).toBe(cache.residentBytes);
                    reach.reach("hit context rechecked");
                  } else if (mode === "reused address") {
                    space.allocator.release(ref.extent.offset, ref.extent.length);
                    space.evict();
                    const next = await store(space, encodeDirectory(body(count + 1, salt, dense)));
                    expect(next.extent.offset + next.offset).toBe(ref.extent.offset + ref.offset);
                    expect(next.checksum).not.toBe(ref.checksum);
                    const replacement = await open({ ...chunk, directory: next });
                    expect(replacement.rowCount).toBe(count + 1);
                    replacement.close();
                    expect(parse.mock.calls.length).toBe(1);
                    reach.reach("checksum identity checked");
                  } else if (mode === "eviction") {
                    const bytes = cache.residentBytes;
                    expect(cache.evict(Infinity)).toBe(bytes);
                    expect(cache.residentBytes).toBe(0);
                    (await open()).close();
                    expect(parse.mock.calls.length).toBe(1);
                    reach.reach("eviction revalidates");
                  } else if (mode === "once hit") {
                    const oldest = cache.oldestUnusedAt;
                    for (let pass = 0; pass < 3; pass++) (await open(chunk, true)).close();
                    expect(parse.mock.calls.length).toBe(0);
                    expect(cache.oldestUnusedAt).toBe(oldest);
                    reach.reach("once keeps eviction order");
                  } else if (mode === "late read after close") {
                    cache.close();
                    (await open()).close();
                    expect(parse.mock.calls.length).toBe(1);
                    expect(cache.entryCount).toBe(0);
                    reach.reach("late unpin does not retain");
                  } else {
                    cache.close();
                    const replacement = createDirectoryCache(governor);
                    try {
                      const reader = await ChunkReader.open(space, chunk, {
                        budget: grant,
                        directories: replacement,
                      });
                      readers.push(reader);
                      reader.close();
                      expect(parse.mock.calls.length).toBe(1);
                      expect(replacement).not.toBe(cache);
                    } finally {
                      replacement.close();
                    }
                    reach.reach("owner scope isolated");
                  }
                }
              }
            }
            reach.reach(mode);
            reach.reach(dense ? "dense sketch" : "sparse sketch");
            expect(grant.allocated).toBe(0);
          } finally {
            for (const reader of readers) reader.close();
            parse.mockClear();
            parse.mockRestore();
            cache.close();
            grant.close();
          }
          expect(space.leased).toBe(0);
          expect(cache.entryCount).toBe(0);
          expect(governor.usedBytes).toBe(0);
          reach.reach("all credit returned");
        },
      ),
      {
        seed,
        // Examples count toward numRuns: keep the old ten random histories after four new examples.
        numRuns: 44,
        examples: MODES.flatMap((mode, index): Array<[Mode, number, number, boolean]> => [
          [mode, 7, index, false],
          [mode, 17, index + 31, true],
        ]),
      },
    );
    reach.assertReached();
  },
  180_000,
);
