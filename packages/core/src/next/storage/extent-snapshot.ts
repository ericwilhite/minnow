/** Immutable owner-private copies of root reachability. Resident state is a handle, not a set
 * of extents. Saved pairs include list-page allocations; a ticket may read them after the root
 * slot changes. This reconstructible scratch never becomes a recovery authority.
 */
import { pauseIfDue, type TaskWork } from "../task.js";
import { DATA_ALIGN, StoreCorruptionError } from "./contract.js";
import type { ByteBudget, ExtentRef } from "./extents.js";
import type { ExtentList } from "./extent-list.js";
import { MetadataJournal } from "./metadata-journal.js";
import {
  ExtentPairWriter,
  openPairs,
  mergeExtents,
  sortExtents,
  spoolExtents,
} from "./metadata-extents.js";
import { pairPhase, type MetadataWork } from "./metadata-work.js";
import { NativeExtentList, NATIVE_LIST_WINDOW_BYTES } from "./native-extent-list.js";
import { PageCache } from "./page-cache.js";
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
  let payload = 0,
    physical = 0,
    orderedPhysical = 0,
    result = 0,
    failed = false;
  try {
    const payloadPhase = async (slab: Uint8Array) => {
      const cursor = new NativeExtentList(
        list,
        cache,
        slab.subarray(0, NATIVE_LIST_WINDOW_BYTES),
        dataLength,
        work,
      );
      const writer = await journal.writer(1),
        output = new ExtentPairWriter(writer, true);
      try {
        for (;;) {
          const pending = cursor.next();
          if (!(typeof pending === "boolean" ? pending : await pending)) break;
          const writing = output.append(cursor.offset, cursor.length, work);
          if (writing !== undefined) await writing;
        }
        payload = await writer.finish();
        await check(work);
      } catch (error) {
        await writer.abort().catch(() => undefined);
        throw error;
      } finally {
        cursor.close();
      }
    };
    await journal.workspace.run("spool", () =>
      io === undefined ? cache.withReadSlab(payloadPhase) : payloadPhase(io.slab),
    );
    if (list.length === 0) {
      result = payload;
      payload = 0;
      return result;
    }
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
    await pairPhase(journal, io, "merge", async (slots) => {
      orderedPhysical = await sortExtents(journal, physical, "exact", work, slots);
      // A payload must not name its own list-page allocation, even with exactly equal coordinates.
      result = await mergeExtents(
        journal,
        payload,
        orderedPhysical,
        "union",
        work,
        "refuse",
        slots,
      );
    });
    return result;
  } catch (error) {
    failed = true;
    if (result !== 0) await disposeSnapshot(journal, result).catch(() => undefined);
    throw error;
  } finally {
    await disposeAll(journal, [payload, physical, orderedPhysical], failed);
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
  const owned = { map: null as BoundedFreeSpaceMap | null };
  try {
    return await journal.workspace.run("merge", () =>
      cache.withReadSlab(async (slab) => {
        const temporary = BoundedFreeSpaceMap.borrow(
          DATA_ALIGN,
          slab.subarray(384 * 1024, 384 * 1024 + MAX_RUN_ARENA_BYTES),
        );
        temporary.startRebuild(Math.ceil(dataLength / DATA_ALIGN) * DATA_ALIGN);
        const a = await openPairs(journal, current, 1, true);
        let b: Awaited<ReturnType<typeof openPairs>> | null = null;
        let failed = false;
        try {
          b = await openPairs(journal, previous, 2, true);
          let left = await a.next(work),
            right = await b.next(work),
            cursor = 0,
            gaps = 0,
            steps = 0;
          while (left || right) {
            if (steps++ % 128 === 0) await check(work);
            let offset: number, length: number;
            if (left && (!right || a.offset < b.offset)) {
              offset = a.offset;
              length = a.length;
              left = await a.next(work);
            } else if (right && (!left || b.offset < a.offset)) {
              offset = b.offset;
              length = b.length;
              right = await b.next(work);
            } else {
              if (a.length !== b.length)
                throw new StoreCorruptionError("Kept roots disagree on extent length");
              offset = a.offset;
              length = a.length;
              left = await a.next(work);
              right = await b.next(work);
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
          return owned.map;
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

async function closePairs(
  a: Awaited<ReturnType<typeof openPairs>>,
  b: Awaited<ReturnType<typeof openPairs>> | null,
  failed: boolean,
) {
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
