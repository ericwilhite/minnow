/** Immutable owner-private copies of root reachability. Resident state is a handle, not a set
 * of extents. Saved pairs include list-page allocations; a ticket may read them after the root
 * slot changes. This reconstructible scratch never becomes a recovery authority.
 */
import { pauseIfDue, type TaskWork } from "../task.js";
import { DATA_ALIGN, StoreCorruptionError } from "./contract.js";
import type { ByteBudget, ExtentRef } from "./extents.js";
import type { ExtentList } from "./extent-list.js";
import { MetadataJournal } from "./metadata-journal.js";
import { ExtentPairWriter, openPairs, sortExtents, spoolExtents } from "./metadata-extents.js";
import { pairPhase, type MetadataWork } from "./metadata-work.js";
import {
  NativeExtentList,
  NativeListPageCache,
  NATIVE_LIST_WINDOW_BYTES,
} from "./native-extent-list.js";
import { PageCache } from "./page-cache.js";
import {
  NativePhysicalExtents,
  NativeRootExtents,
  type ScalarExtents,
} from "./native-root-cursor.js";
import { BoundedFreeSpaceMap, MAX_RUN_ARENA_BYTES, runArenaBytes } from "./bounded-free-space.js";

async function check(work?: TaskWork) {
  const pending = pauseIfDue(work);
  if (pending !== undefined) await pending;
}

/** Descriptors are caller-owned and admitted. Payload pairs never materialize as JS objects. */
export async function saveNativeList(
  journal: MetadataJournal,
  cache: PageCache,
  list: ExtentList,
  dataLength: number,
  work?: TaskWork,
  io?: MetadataWork,
): Promise<number> {
  if (list.length === 0) return saveExtentInput(journal, [], true, dataLength, work, io);
  let physical = 0,
    orderedPhysical = 0,
    result = 0,
    failed = false;
  try {
    await journal.workspace.run("spool", async () => {
      physical = await spoolExtents(
        journal,
        (function* () {
          for (const page of list) yield page.blob.extent;
        })(),
        1,
        work,
      );
    });
    const compose = (scope: MetadataWork) =>
      pairPhase(journal, scope, "merge", async (slots) => {
        orderedPhysical = await sortExtents(journal, physical, "exact", work, slots);
        const native = new NativeExtentList(
          list,
          cache,
          scope.slab.subarray(0, NATIVE_LIST_WINDOW_BYTES),
          dataLength,
          work,
        );
        const pages = await openPairs(journal, orderedPhysical, slots.left, true);
        let writer: Awaited<ReturnType<MetadataJournal["writer"]>> | null = null,
          composeFailed = false;
        try {
          writer = await journal.writer(slots.output);
          const output = new ExtentPairWriter(writer, true, "refuse");
          let left = await native.next(),
            right = await pages.next(work),
            steps = 0;
          while (left || right) {
            if (steps++ % 128 === 0) await check(work);
            if (left && right && native.offset === pages.offset)
              throw new StoreCorruptionError(
                "Repeated private extent pair: payload names its own list-page allocation",
              );
            const takeNative = left && (!right || native.offset < pages.offset);
            const writing = output.append(
              takeNative ? native.offset : pages.offset,
              takeNative ? native.length : pages.length,
              work,
            );
            if (writing !== undefined) await writing;
            if (takeNative) {
              const pending = native.next();
              left = typeof pending === "boolean" ? pending : await pending;
            } else {
              const pending = pages.next(work);
              right = typeof pending === "boolean" ? pending : await pending;
            }
          }
          result = await writer.finish();
          writer = null;
          await check(work);
        } catch (error) {
          composeFailed = true;
          await writer?.abort().catch(() => undefined);
          throw error;
        } finally {
          native.close();
          await closePairs(pages, null, composeFailed);
        }
      });
    if (io === undefined) await cache.withReadSlab((slab) => compose({ slab }));
    else await compose(io);
    return result;
  } catch (error) {
    failed = true;
    if (result !== 0) await disposeSnapshot(journal, result).catch(() => undefined);
    throw error;
  } finally {
    await disposeAll(journal, [physical, orderedPhysical], failed);
  }
}

/** Generic/test callbacks use the same bounded disk sort when they cannot promise offset order. */
export async function saveExtentInput(
  journal: MetadataJournal,
  input: Iterable<ExtentRef> | AsyncIterable<ExtentRef>,
  ordered: boolean,
  dataLength: number,
  work?: TaskWork,
  io?: MetadataWork,
  deferSort = false,
): Promise<number> {
  let raw = 0,
    result = 0,
    failed = false;
  try {
    await journal.workspace.run("spool", async () => {
      const writer = await journal.writer(1),
        output = new ExtentPairWriter(writer, ordered, "exact");
      try {
        let count = 0;
        for await (const extent of input) {
          if (count++ % 128 === 0) await check(work);
          if (extent.offset + extent.length > dataLength)
            throw new StoreCorruptionError("Root extent is past data length");
          const writing = output.append(extent.offset, extent.length, work);
          if (writing !== undefined) await writing;
        }
        raw = await writer.finish();
        await check(work);
      } catch (error) {
        await writer.abort().catch(() => undefined);
        throw error;
      }
    });
    if (ordered || deferSort) {
      result = raw;
      raw = 0;
      return result;
    }
    await pairPhase(journal, io, "merge", async (slots) => {
      result = await sortExtents(journal, raw, "exact", work, slots);
    });
    return result;
  } catch (error) {
    failed = true;
    if (result !== 0) await disposeSnapshot(journal, result).catch(() => undefined);
    throw error;
  } finally {
    await disposeAll(journal, [raw], failed);
  }
}

async function disposeAll(journal: MetadataJournal, handles: readonly number[], failed: boolean) {
  let first: Error | undefined;
  for (const handle of handles)
    if (handle !== 0) {
      try {
        await disposeSnapshot(journal, handle);
      } catch (error) {
        first ??= error instanceof Error ? error : new Error(String(error));
      }
    }
  if (!failed && first !== undefined) throw first;
}

/** External workspace lock held, but no workspace phase active. */
export async function disposeSnapshot(journal: MetadataJournal, handle: number): Promise<void> {
  await journal.workspace.run("spool", () => journal.release(handle));
}

/** Linear union into an S-slab arena, then one open-time R admission with headroom. There is no
 * previous-only collection, per-extent charge, sorting of E, or retained backing-store overlap.
 */
export async function reconstructMap(
  journal: MetadataJournal,
  cache: PageCache,
  current: number,
  previous: number,
  dataLength: number,
  budget: ByteBudget,
  work?: TaskWork,
): Promise<BoundedFreeSpaceMap> {
  return (await reconstructState(journal, cache, current, previous, dataLength, budget, work)).map;
}

export async function reconstructState(
  journal: MetadataJournal,
  cache: PageCache,
  current: number,
  previous: number,
  dataLength: number,
  budget: ByteBudget,
  work?: TaskWork,
): Promise<{ map: BoundedFreeSpaceMap; previousOnlyBytes: number }> {
  return reconstructFrom(journal, cache, dataLength, budget, work, async () => {
    const a = await openPairs(journal, current, 1, true);
    try {
      return [a, await openPairs(journal, previous, 2, true)] as const;
    } catch (error) {
      await a.close().catch(() => undefined);
      throw error;
    }
  });
}

/** Startup reads native payloads once; only oversized physical-page directories need disk sort. */
export async function reconstructNativeState(
  journal: MetadataJournal,
  cache: PageCache,
  current: ExtentList,
  previous: ExtentList,
  dataLength: number,
  budget: ByteBudget,
  work?: TaskWork,
): Promise<{ map: BoundedFreeSpaceMap; previousOnlyBytes: number }> {
  let aPages = 0,
    bPages = 0,
    failed = false;
  const physical = async (list: ExtentList): Promise<number> => {
    if (list.length <= NATIVE_LIST_WINDOW_BYTES / 16) return 0;
    let raw = 0,
      failed = false;
    try {
      await journal.workspace.run("spool", async () => {
        raw = await spoolExtents(
          journal,
          (function* () {
            for (const page of list) yield page.blob.extent;
          })(),
          1,
          work,
        );
      });
      return await pairPhase(journal, undefined, "merge", (slots) =>
        sortExtents(journal, raw, "exact", work, slots),
      );
    } catch (error) {
      failed = true;
      throw error;
    } finally {
      await disposeAll(journal, [raw], failed);
    }
  };
  try {
    aPages = await physical(current);
    bPages = await physical(previous);
    return await reconstructFrom(
      journal,
      cache,
      dataLength,
      budget,
      work,
      async (slab) => {
        const physicalCursor = (list: ExtentList, handle: number, slot: number, at: number) =>
          handle === 0
            ? NativePhysicalExtents.open(
                list,
                slab.subarray(at, at + NATIVE_LIST_WINDOW_BYTES),
                dataLength,
                work,
              )
            : openPairs(journal, handle, slot, true);
        const shared = new NativeListPageCache(
          slab.subarray(0, NATIVE_LIST_WINDOW_BYTES),
          slab.subarray(NATIVE_LIST_WINDOW_BYTES, 2 * NATIVE_LIST_WINDOW_BYTES),
        );
        const a = await physicalCursor(current, aPages, 1, 640 * 1024);
        let b: ScalarExtents | null = null;
        try {
          b = await physicalCursor(previous, bPages, 2, 832 * 1024);
          return [
            new NativeRootExtents(
              new NativeExtentList(
                current,
                cache,
                slab.subarray(0, NATIVE_LIST_WINDOW_BYTES),
                dataLength,
                work,
                shared,
              ),
              a,
            ),
            new NativeRootExtents(
              new NativeExtentList(
                previous,
                cache,
                slab.subarray(NATIVE_LIST_WINDOW_BYTES, 2 * NATIVE_LIST_WINDOW_BYTES),
                dataLength,
                work,
                shared,
              ),
              b,
            ),
          ] as const;
        } catch (error) {
          await closePairs(a, b, true);
          throw error;
        }
      },
      "source",
    );
  } catch (error) {
    failed = true;
    throw error;
  } finally {
    await disposeAll(journal, [aPages, bPages], failed);
  }
}

async function reconstructFrom(
  journal: MetadataJournal,
  cache: PageCache,
  dataLength: number,
  budget: ByteBudget,
  work: TaskWork | undefined,
  cursors: (slab: Uint8Array) => Promise<readonly [ScalarExtents, ScalarExtents]>,
  phase: "merge" | "source" = "merge",
): Promise<{ map: BoundedFreeSpaceMap; previousOnlyBytes: number }> {
  const owned = { map: null as BoundedFreeSpaceMap | null };
  try {
    return await journal.workspace.run(phase, () =>
      cache.withReadSlab(async (slab) => {
        const temporary = BoundedFreeSpaceMap.borrow(
          DATA_ALIGN,
          slab.subarray(384 * 1024, 384 * 1024 + MAX_RUN_ARENA_BYTES),
        );
        temporary.startRebuild(Math.ceil(dataLength / DATA_ALIGN) * DATA_ALIGN);
        let a: ScalarExtents | null = null,
          b: ScalarExtents | null = null;
        let failed = false;
        try {
          [a, b] = await cursors(slab);
          let left = await a.next(work),
            right = await b.next(work),
            cursor = 0,
            gaps = 0,
            steps = 0,
            previousOnlyBytes = 0;
          while (left || right) {
            if (steps++ % 128 === 0) await check(work);
            let offset: number, length: number;
            if (left && (!right || a.offset < b.offset)) {
              offset = a.offset;
              length = a.length;
              {
                const next = a.next(work);
                left = typeof next === "boolean" ? next : await next;
              }
            } else if (right && (!left || b.offset < a.offset)) {
              offset = b.offset;
              length = b.length;
              previousOnlyBytes += Math.ceil(length / DATA_ALIGN) * DATA_ALIGN;
              {
                const next = b.next(work);
                right = typeof next === "boolean" ? next : await next;
              }
            } else {
              if (a.length !== b.length)
                throw new StoreCorruptionError(
                  `The current and previous roots name different extents at ${String(a.offset)}`,
                );
              offset = a.offset;
              length = a.length;
              {
                const next = a.next(work);
                left = typeof next === "boolean" ? next : await next;
              }
              {
                const next = b.next(work);
                right = typeof next === "boolean" ? next : await next;
              }
            }
            if (offset < cursor || offset + length > dataLength)
              throw new StoreCorruptionError("Kept roots overlap or extend past data length");
            if (offset > cursor) {
              temporary.releaseKnownAllocated(cursor, offset - cursor);
              gaps++;
            }
            cursor = offset + Math.ceil(length / DATA_ALIGN) * DATA_ALIGN;
          }
          if (cursor < temporary.end)
            temporary.releaseKnownAllocated(cursor, temporary.end - cursor);
          // Startup only: this cache is not exposed yet. Synchronous admission may evict encoded
          // cache entries, never this fixed slab; no governor/caller wait is permitted here.
          owned.map = BoundedFreeSpaceMap.create(DATA_ALIGN, runArenaBytes(gaps), budget);
          await owned.map.copyFrom(temporary, work);
          await check(work);
          return { map: owned.map, previousOnlyBytes };
        } catch (error) {
          failed = true;
          throw error;
        } finally {
          try {
            await closePairs(a, b, failed);
          } finally {
            temporary.close();
          }
        }
      }),
    );
  } catch (error) {
    owned.map?.close();
    throw error;
  }
}

async function closePairs(a: ScalarExtents | null, b: ScalarExtents | null, failed: boolean) {
  let first: Error | undefined;
  for (const pairs of [a, b]) {
    try {
      await pairs?.close();
    } catch (error) {
      first ??= error instanceof Error ? error : new Error(String(error));
    }
  }
  if (!failed && first !== undefined) throw first;
}
