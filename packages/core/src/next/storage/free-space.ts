/**
 * Free-space tracking for one long-lived file: the data space (extents.ts) and the spill file use
 * it. It is memory only. The data space rebuilds it on open from the extents the durable root
 * reaches (`fromAllocated`); the spill file starts empty on every open.
 *
 * ## Model
 * - Space is `[0, end)`. Everything at or past `end` is free and unbounded: an allocation that
 *   fits no free run takes space at `end`, so allocation never fails (quota surfaces later, as a
 *   write error).
 * - Inside `[0, end)`, free space is a sorted list of disjoint, non-adjacent runs. Every offset and
 *   length is a multiple of `align`; a request is rounded up to it, and `free` rounds the same way,
 *   so `free(offset, n)` undoes `allocate(n)`.
 * - Placement is lowest-address first fit. It keeps live data low, so the free tail grows and can
 *   be returned to the platform (`end` shrinks when a run reaches it).
 * - Misuse is a `RangeError`: freeing space that is already free (a double retire), unaligned
 *   offsets, or space past `end`.
 *
 * Cost: allocation scans the runs (O(runs)); free and exact placement are O(log runs) plus the
 * array splice. Runs stay few because placement is first fit and frees coalesce.
 */
import { pauseIfDue, sortInSlices, type TaskWork } from "../task.js";

export class FreeSpaceMap {
  readonly align: number;
  /** Start of each free run, ascending. */
  readonly #starts: number[] = [];
  /** End (exclusive) of each free run. */
  readonly #ends: number[] = [];
  #end = 0;

  constructor(align: number) {
    if (!(Number.isSafeInteger(align) && align > 0)) throw new RangeError("Bad alignment");
    this.align = align;
  }

  /**
   * The map of a file of `length` bytes whose live extents are `allocated` (any order; duplicates
   * of one extent allowed, since several roots or groups may share it). Throws `RangeError` if two
   * different extents overlap, an extent is unaligned, or one runs past `length`: the caller
   * reports that as corruption.
   */
  static fromAllocated(
    align: number,
    length: number,
    allocated: Iterable<{ readonly offset: number; readonly length: number }>,
  ): FreeSpaceMap {
    const map = new FreeSpaceMap(align);
    const extents = [...allocated]
      .map(({ offset, length: bytes }) => ({ offset, end: offset + map.#round(bytes) }))
      .sort((a, b) => a.offset - b.offset || a.end - b.end);
    let cursor = 0;
    let previous: { offset: number; end: number } | null = null;
    for (const extent of extents) {
      map.#check(extent.offset);
      if (previous !== null && previous.offset === extent.offset && previous.end === extent.end)
        continue;
      if (extent.offset < cursor)
        throw new RangeError(`Extent at ${String(extent.offset)} overlaps another`);
      if (extent.end > map.#round(length))
        throw new RangeError(`Extent at ${String(extent.offset)} runs past the file`);
      if (extent.offset > cursor) map.#push(cursor, extent.offset);
      cursor = extent.end;
      previous = extent;
    }
    map.#end = cursor;
    return map;
  }

  /** Startup's task-sliced variant; cancellation is checked on every real resumption. */
  static async fromAllocatedAsync(
    align: number,
    length: number,
    allocated: Iterable<{ readonly offset: number; readonly length: number }>,
    work?: TaskWork,
  ): Promise<FreeSpaceMap> {
    const firstPause = pauseIfDue(work);
    if (firstPause !== undefined) await firstPause;
    const map = new FreeSpaceMap(align);
    const extents: Array<{ offset: number; end: number }> = [];
    for (const extent of allocated) {
      if (extents.length % 128 === 0) {
        const pending = pauseIfDue(work);
        if (pending !== undefined) await pending;
      }
      extents.push({ offset: extent.offset, end: extent.offset + map.#round(extent.length) });
    }
    await sortInSlices(extents, (a, b) => a.offset - b.offset || a.end - b.end, work);
    let cursor = 0;
    let previous: { offset: number; end: number } | null = null;
    for (let index = 0; index < extents.length; index++) {
      if (index % 128 === 0) {
        const pending = pauseIfDue(work);
        if (pending !== undefined) await pending;
      }
      const extent = extents[index];
      if (extent === undefined) continue;
      map.#check(extent.offset);
      if (previous !== null && previous.offset === extent.offset && previous.end === extent.end)
        continue;
      if (extent.offset < cursor)
        throw new RangeError(`Extent at ${String(extent.offset)} overlaps another`);
      if (extent.end > map.#round(length))
        throw new RangeError(`Extent at ${String(extent.offset)} runs past the file`);
      if (extent.offset > cursor) map.#push(cursor, extent.offset);
      cursor = extent.end;
      previous = extent;
    }
    map.#end = cursor;
    const pending = pauseIfDue(work);
    if (pending !== undefined) await pending;
    return map;
  }

  /** One pass over an ordered union; no materialized extent vector or comparison sort. */
  static async fromSortedAllocatedAsync(
    align: number,
    length: number,
    allocated: AsyncIterable<{ readonly offset: number; readonly length: number }>,
    work?: TaskWork,
  ): Promise<FreeSpaceMap> {
    const map = new FreeSpaceMap(align);
    let cursor = 0;
    let previous: { offset: number; end: number } | null = null;
    for await (const extent of allocated) {
      const pending = pauseIfDue(work);
      if (pending !== undefined) await pending;
      map.#check(extent.offset);
      const end = extent.offset + map.#round(extent.length);
      if (previous !== null && previous.offset === extent.offset && previous.end === end) continue;
      if (extent.offset < cursor) throw new RangeError("Ordered extents overlap or regress");
      if (end > map.#round(length)) throw new RangeError("Extent runs past the file");
      if (extent.offset > cursor) map.#push(cursor, extent.offset);
      cursor = end;
      previous = { offset: extent.offset, end };
    }
    map.#end = cursor;
    return map;
  }

  /** The allocator's high-water mark: bytes at or past it are free. */
  get end(): number {
    return this.#end;
  }

  /** Free bytes inside `[0, end)`. */
  get freeBytes(): number {
    let bytes = 0;
    for (let index = 0; index < this.#starts.length; index++)
      bytes += this.#endOf(index) - this.#startOf(index);
    return bytes;
  }

  get runCount(): number {
    return this.#starts.length;
  }

  /** The longest free run inside `[0, end)`, 0 if none. */
  get largestRun(): number {
    let largest = 0;
    for (let index = 0; index < this.#starts.length; index++)
      largest = Math.max(largest, this.#endOf(index) - this.#startOf(index));
    return largest;
  }

  /** Bytes `allocate(bytes)` takes. */
  sizeOf(bytes: number): number {
    return this.#round(bytes);
  }

  /** Takes `bytes` (rounded up) at the lowest free offset that fits, or at `end`. */
  allocate(bytes: number): number {
    const size = this.#round(bytes);
    for (let index = 0; index < this.#starts.length; index++) {
      const start = this.#startOf(index);
      if (this.#endOf(index) - start >= size) {
        this.#take(index, start, start + size);
        return start;
      }
    }
    const offset = this.#end;
    this.#end += size;
    return offset;
  }

  /**
   * Takes exactly `[offset, offset + bytes)` if all of it is free (the spill file extends its
   * previous extent this way, to keep a stream sequential). Returns whether it did.
   */
  allocateAt(offset: number, bytes: number): boolean {
    this.#check(offset);
    const end = offset + this.#round(bytes);
    if (offset >= this.#end) {
      if (offset > this.#end) this.#push(this.#end, offset);
      this.#end = end;
      return true;
    }
    const index = this.#runContaining(offset);
    if (index < 0) return false;
    // No run touches `end` (a free tail is dropped), so the range must fit inside this run.
    if (end > this.#endOf(index)) return false;
    this.#take(index, offset, end);
    return true;
  }

  /** Returns `[offset, offset + bytes)` (rounded up). Throws if any of it is already free. */
  free(offset: number, bytes: number): void {
    this.#check(offset);
    const end = offset + this.#round(bytes);
    if (end === offset) return;
    if (end > this.#end) throw new RangeError(`Free past the end at ${String(offset)}`);
    // The first run starting after `offset`; the run before it must end at or before `offset`.
    const next = this.#firstStartAfter(offset);
    const before = next - 1;
    if (before >= 0 && this.#endOf(before) > offset)
      throw new RangeError(`Double free at ${String(offset)}`);
    if (next < this.#starts.length && this.#startOf(next) < end)
      throw new RangeError(`Double free at ${String(offset)}`);
    const joinsBefore = before >= 0 && this.#ends[before] === offset;
    const joinsNext = next < this.#starts.length && this.#starts[next] === end;
    if (joinsBefore && joinsNext) {
      this.#ends[before] = this.#endOf(next);
      this.#starts.splice(next, 1);
      this.#ends.splice(next, 1);
    } else if (joinsBefore) this.#ends[before] = end;
    else if (joinsNext) this.#starts[next] = offset;
    else {
      this.#starts.splice(next, 0, offset);
      this.#ends.splice(next, 0, end);
    }
    this.#dropTail();
  }

  /** Whether all of `[offset, offset + bytes)` (rounded up) is allocated: below `end`, not free. */
  isAllocated(offset: number, bytes: number): boolean {
    const end = offset + this.#round(bytes);
    if (offset < 0 || end > this.#end) return false;
    // The last run starting at or before `offset` must end by it; the next must start at or after `end`.
    const next = this.#firstStartAfter(offset);
    if (next > 0 && this.#endOf(next - 1) > offset) return false;
    return next >= this.#starts.length || this.#startOf(next) >= end;
  }

  /** The free runs, ascending (tests and diagnostics). */
  runs(): Array<{ readonly offset: number; readonly length: number }> {
    return this.#starts.map((start, index) => ({
      offset: start,
      length: this.#endOf(index) - start,
    }));
  }

  #startOf(index: number): number {
    return this.#starts[index] ?? 0;
  }

  #endOf(index: number): number {
    return this.#ends[index] ?? 0;
  }

  #round(bytes: number): number {
    if (!(Number.isSafeInteger(bytes) && bytes >= 0)) throw new RangeError("Bad length");
    return Math.ceil(bytes / this.align) * this.align;
  }

  #check(offset: number): void {
    if (!(Number.isSafeInteger(offset) && offset >= 0 && offset % this.align === 0))
      throw new RangeError(`Unaligned offset ${String(offset)}`);
  }

  #push(start: number, end: number): void {
    this.#starts.push(start);
    this.#ends.push(end);
  }

  /** Index of the first run whose start is > offset (binary search). */
  #firstStartAfter(offset: number): number {
    let low = 0;
    let high = this.#starts.length;
    while (low < high) {
      const middle = (low + high) >>> 1;
      if (this.#startOf(middle) <= offset) low = middle + 1;
      else high = middle;
    }
    return low;
  }

  #runContaining(offset: number): number {
    const index = this.#firstStartAfter(offset) - 1;
    return index >= 0 && this.#endOf(index) > offset ? index : -1;
  }

  /** Removes `[start, end)` from run `index`, which contains it. */
  #take(index: number, start: number, end: number): void {
    const runStart = this.#startOf(index);
    const runEnd = this.#endOf(index);
    if (runStart === start && runEnd === end) {
      this.#starts.splice(index, 1);
      this.#ends.splice(index, 1);
    } else if (runStart === start) this.#starts[index] = end;
    else if (runEnd === end) this.#ends[index] = start;
    else {
      this.#ends[index] = start;
      this.#starts.splice(index + 1, 0, end);
      this.#ends.splice(index + 1, 0, runEnd);
    }
  }

  #dropTail(): void {
    const last = this.#starts.length - 1;
    if (last >= 0 && this.#ends[last] === this.#end) {
      this.#end = this.#startOf(last);
      this.#starts.pop();
      this.#ends.pop();
    }
  }
}
