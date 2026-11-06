/**
 * Read coalescing: the one rule for turning many small range reads into few platform reads. The
 * file store's `readData` and the page cache's miss path both use it.
 *
 * A platform read costs a fixed ~15–80 µs warm and ~240–320 µs cold (storage probe, 16–32 KiB),
 * against ~1.5 µs per KiB of sequential bandwidth. So ranges are sorted and merged into one slab
 * while the gap between them is at most `MAX_SLAB_GAP` (64 KiB: about the cost of one extra call)
 * and the slab stays within `MAX_SLAB` (1 MiB). A 3-of-200-column scan therefore reads its
 * columns' extents and never the 197 others between them, which are far more than 64 KiB away.
 *
 * **Every slab is at most `MAX_SLAB` when every range is.** The extent space's ranges always are
 * (blobs are capped at `MAX_BLOB_BYTES` = `MAX_SLAB`, extents.ts), so its misses always fit its one
 * reused 1 MiB slab buffer. Only the session's raw `readData` may pass a longer range (the storage
 * kit reads a whole extent); such a range is read alone, as its own slab.
 */
import type { DataRange } from "./contract.js";

/** The largest gap bridged inside one slab (bytes read and discarded). */
export const MAX_SLAB_GAP = 64 * 1024;
/** The largest slab built by merging; a single longer range is read alone. */
export const MAX_SLAB = 1024 * 1024;

export interface Slab {
  readonly offset: number;
  readonly length: number;
  /** Indices into the planned ranges that this slab covers, in offset order. */
  readonly members: readonly number[];
}

/**
 * Groups `ranges` (any order; overlaps and duplicates allowed) into slabs in offset order. Every
 * range lies wholly inside exactly one slab.
 */
/**
 * `limit`: the largest slab (default `MAX_SLAB`, the shared read slab). A reader that owns each
 * slab's buffer (a one-pass read, page-cache.ts) may plan larger ones.
 */
export function planSlabs(ranges: readonly DataRange[], limit = MAX_SLAB): Slab[] {
  const order = ranges.map((range, index) => ({ range, index }));
  order.sort((a, b) => a.range.offset - b.range.offset);
  const slabs: Slab[] = [];
  let start = 0;
  let end = 0;
  let members: number[] = [];
  for (const { range, index } of order) {
    const { offset, length } = range;
    const rangeEnd = offset + length;
    if (members.length > 0) {
      const mergedEnd = Math.max(end, rangeEnd);
      // A range inside the slab costs nothing; otherwise bound the gap and the slab.
      if (rangeEnd <= end || (offset - end <= MAX_SLAB_GAP && mergedEnd - start <= limit)) {
        end = mergedEnd;
        members.push(index);
        continue;
      }
      slabs.push({ offset: start, length: end - start, members });
    }
    start = offset;
    end = rangeEnd;
    members = [index];
  }
  if (members.length > 0) slabs.push({ offset: start, length: end - start, members });
  return slabs;
}
