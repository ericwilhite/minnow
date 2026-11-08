/** Fixed, prepaid run arena. Forgotten holes are never allocation or root-ownership proof.
 * allocate/allocateAt only consume tracked holes or contiguous EOF: they may run outside the
 * ownership-transition barrier. Releases require root/writer provenance under that barrier.
 * With releases blocked, concurrent reservations can only shrink the tracked free pool.
 */
import { pauseIfDue, type TaskWork } from "../task.js";
import {
  ReadDeniedError,
  type ByteBudget,
  type CacheMemory,
  type OpenSpaceOptions,
} from "./extents.js";

export const MIN_RUN_ARENA_BYTES = 32 * 1024;
export const MAX_RUN_ARENA_BYTES = 256 * 1024;
const HEADERS = 320;
const EMPTY = new Float64Array(0);
/** Two slots per initial run, capped before multiplying; no resize is needed at runtime. */
export function runArenaBytes(runCount: number): number {
  if (!Number.isSafeInteger(runCount) || runCount < 0) throw new RangeError("Bad run count");
  return Math.max(
    MIN_RUN_ARENA_BYTES,
    Math.min(MAX_RUN_ARENA_BYTES, HEADERS + Math.min(runCount, MAX_RUN_ARENA_BYTES) * 32),
  );
}
export class BoundedFreeSpaceMap {
  #pairs: Float64Array;
  #count = 0;
  #end = 0;
  #untracked = 0;
  #closed = false;
  #smallest = -1;
  #budget: ByteBudget | null;
  #memory: CacheMemory | null = null;
  static create(align: number, arenaBytes: number, budget: ByteBudget): BoundedFreeSpaceMap {
    if (!Number.isSafeInteger(align) || align <= 0) throw new RangeError("Bad alignment");
    if (
      !Number.isSafeInteger(arenaBytes) ||
      arenaBytes < MIN_RUN_ARENA_BYTES ||
      arenaBytes > MAX_RUN_ARENA_BYTES
    )
      throw new RangeError("Bad run arena size");
    if (!budget.allocate(arenaBytes)) throw new ReadDeniedError(arenaBytes);
    try {
      return new BoundedFreeSpaceMap(align, arenaBytes, budget);
    } catch (error) {
      budget.free(arenaBytes);
      throw error;
    }
  }
  private constructor(
    readonly align: number,
    readonly arenaBytes: number,
    budget: ByteBudget | null,
    backing?: Uint8Array,
  ) {
    this.#budget = budget;
    const length = 2 * Math.floor((arenaBytes - HEADERS) / 16);
    this.#pairs =
      backing === undefined
        ? new Float64Array(length)
        : new Float64Array(backing.buffer, backing.byteOffset, length);
  }
  /** Temporary startup builder, borrowing an exclusively held prepaid read slab. */
  static borrow(align: number, backing: Uint8Array): BoundedFreeSpaceMap {
    if (
      !Number.isSafeInteger(align) ||
      align <= 0 ||
      backing.byteOffset % 8 !== 0 ||
      backing.byteLength < MIN_RUN_ARENA_BYTES ||
      backing.byteLength > MAX_RUN_ARENA_BYTES
    )
      throw new RangeError("Invalid borrowed run arena");
    return new BoundedFreeSpaceMap(align, backing.byteLength, null, backing);
  }
  get bytes(): number {
    return this.#closed || (this.#budget === null && this.#memory === null) ? 0 : this.arenaBytes;
  }
  readonly evictableBytes = 0;
  evict(): number {
    return 0;
  }
  /** No publication admission: this open-time charge changes owner atomically. */
  transfer(register: OpenSpaceOptions["registerCache"]): void {
    if (this.#closed || this.#budget === null || this.#memory !== null)
      throw new TypeError("Arena has no transferable charge");
    const memory = register(this);
    const moved =
      memory.takeFrom === undefined
        ? memory.charge(this.arenaBytes)
        : memory.takeFrom(this.#budget, this.arenaBytes);
    if (!moved) {
      memory.unregister?.();
      throw new ReadDeniedError(this.arenaBytes);
    }
    if (memory.takeFrom === undefined) this.#budget.free(this.arenaBytes);
    this.#budget = null;
    this.#memory = memory;
  }
  /** One sequential rebuild; callers have validated every gap from the immutable root union. */
  startRebuild(end: number): void {
    this.#usable();
    this.#check(end);
    if (this.#end !== 0 || this.#count !== 0) throw new TypeError("Arena is not empty");
    this.#end = end;
  }
  /** Gap copying borrows the old paid slab, so no old/new retained backing overlap is needed. */
  async copyFrom(source: BoundedFreeSpaceMap, work?: TaskWork): Promise<void> {
    this.#usable();
    source.#usable();
    if (source.#count > this.capacity || this.#end !== 0 || this.#count !== 0)
      throw new RangeError("Cannot copy run arena");
    for (let at = 0; at < source.#count; at += 128) {
      const pending = pauseIfDue(work);
      if (pending !== undefined) await pending;
      this.#pairs.set(
        source.#pairs.subarray(at * 2, Math.min(source.#count, at + 128) * 2),
        at * 2,
      );
    }
    const pending = pauseIfDue(work);
    if (pending !== undefined) await pending;
    this.#count = source.#count;
    this.#end = source.#end;
    this.#untracked = source.#untracked;
    this.#smallest = -1;
  }
  get capacity(): number {
    return this.#pairs.length / 2;
  }
  get runCount(): number {
    return this.#count;
  }
  runOffset(index: number): number {
    if (!Number.isInteger(index) || index < 0 || index >= this.#count)
      throw new RangeError("Bad run index");
    return this.#first(index);
  }
  runLength(index: number): number {
    return this.#last(index) - this.runOffset(index);
  }
  get end(): number {
    return this.#end;
  }
  get untrackedFreeBytes(): number {
    return this.#untracked;
  }
  /** Tracked, reusable bytes only. Forgotten bytes are reported separately. */
  get freeBytes(): number {
    let bytes = 0;
    for (let i = 0; i < this.#count; i++) bytes += this.#last(i) - this.#first(i);
    return bytes;
  }
  get largestRun(): number {
    let bytes = 0;
    for (let i = 0; i < this.#count; i++) bytes = Math.max(bytes, this.#last(i) - this.#first(i));
    return bytes;
  }
  sizeOf(bytes: number): number {
    if (!Number.isSafeInteger(bytes) || bytes < 0) throw new RangeError("Bad length");
    const size = Math.ceil(bytes / this.align) * this.align;
    if (!Number.isSafeInteger(size)) throw new RangeError("Size exceeds safe coordinates");
    return size;
  }
  #check(offset: number): void {
    if (!Number.isSafeInteger(offset) || offset < 0 || offset % this.align !== 0)
      throw new RangeError("Bad offset");
  }
  #extentEnd(offset: number, bytes: number): number {
    this.#check(offset);
    const end = offset + this.sizeOf(bytes);
    if (!Number.isSafeInteger(end)) throw new RangeError("Range exceeds safe coordinates");
    return end;
  }
  #usable(): void {
    if (this.#closed) throw new TypeError("Run arena is closed");
  }
  #first(i: number): number {
    return this.#pairs[i * 2] ?? 0;
  }
  #last(i: number): number {
    return this.#pairs[i * 2 + 1] ?? 0;
  }
  #set(i: number, first: number, last: number): void {
    this.#pairs[i * 2] = first;
    this.#pairs[i * 2 + 1] = last;
    this.#smallest = -1;
  }
  #after(offset: number): number {
    let low = 0,
      high = this.#count;
    while (low < high) {
      const mid = (low + high) >>> 1;
      if (this.#first(mid) <= offset) low = mid + 1;
      else high = mid;
    }
    return low;
  }
  #remove(i: number): void {
    this.#pairs.copyWithin(i * 2, (i + 1) * 2, this.#count * 2);
    this.#count--;
    this.#smallest = -1;
  }
  #minimum(): number {
    if (this.#smallest >= 0) return this.#smallest;
    let at = 0;
    for (let i = 1; i < this.#count; i++)
      if (this.#last(i) - this.#first(i) < this.#last(at) - this.#first(at)) at = i;
    this.#smallest = at;
    return at;
  }
  #insert(first: number, last: number): void {
    if (first === last) return;
    if (this.#count === this.capacity) {
      const smallest = this.#minimum(),
        size = this.#last(smallest) - this.#first(smallest);
      if (last - first <= size) {
        this.#untracked += last - first;
        return;
      }
      this.#untracked += size;
      this.#remove(smallest);
    }
    const at = this.#after(first);
    this.#pairs.copyWithin((at + 1) * 2, at * 2, this.#count * 2);
    this.#set(at, first, last);
    this.#count++;
  }
  /** Lowest fit among tracked runs, otherwise fresh tail space. Never searches forgotten holes. */
  allocate(bytes: number): number {
    this.#usable();
    const size = this.sizeOf(bytes);
    for (let i = 0; i < this.#count; i++)
      if (this.#last(i) - this.#first(i) >= size) {
        const start = this.#first(i);
        this.#take(i, start, start + size);
        return start;
      }
    const start = this.#end;
    this.#end = this.#extentEnd(start, size);
    return start;
  }
  allocateAt(offset: number, bytes: number): boolean {
    this.#usable();
    const end = this.#extentEnd(offset, bytes);
    if (offset > this.#end) return false;
    if (offset === this.#end) {
      this.#end = end;
      return true;
    }
    const i = this.#after(offset) - 1;
    if (i < 0 || end > this.#last(i)) return false;
    this.#take(i, offset, end);
    return true;
  }
  #take(i: number, first: number, last: number): void {
    const start = this.#first(i),
      end = this.#last(i);
    this.#remove(i);
    this.#insert(start, first);
    this.#insert(last, end);
  }
  /** Caller must prove ownership under the space mutation barrier. Unknown holes cannot detect double frees. */
  releaseKnownAllocated(offset: number, bytes: number): void {
    this.#usable();
    const end = this.#extentEnd(offset, bytes);
    if (end === offset) return;
    if (end > this.#end) throw new RangeError("Free past end");
    let next = this.#after(offset),
      first = offset,
      last = end;
    if (next > 0 && this.#last(next - 1) > offset)
      throw new RangeError("Overlaps tracked free space");
    if (next < this.#count && this.#first(next) < end)
      throw new RangeError("Overlaps tracked free space");
    if (next > 0 && this.#last(next - 1) === offset) {
      first = this.#first(next - 1);
      this.#remove(next - 1);
      next--;
    }
    if (next < this.#count && this.#first(next) === end) {
      last = this.#last(next);
      this.#remove(next);
    }
    if (last === this.#end) {
      this.#end = first;
      return;
    }
    this.#insert(first, last);
    if (this.#count > 0 && this.#last(this.#count - 1) === this.#end) {
      this.#end = this.#first(this.#count - 1);
      this.#remove(this.#count - 1);
    }
  }
  /** A conservative classification. Unknown MUST NOT authorize retirement or root references. */
  allocationState(offset: number, bytes: number): "allocated" | "free" | "unknown" | "outside" {
    const end = this.#extentEnd(offset, bytes);
    if (end > this.#end) return "outside";
    const next = this.#after(offset);
    if (
      (next > 0 && this.#last(next - 1) > offset) ||
      (next < this.#count && this.#first(next) < end)
    )
      return "free";
    return this.#untracked === 0 ? "allocated" : "unknown";
  }
  /** Copies for diagnostics only; core iteration uses scalar pairs. */
  runs(): Array<{ offset: number; length: number }> {
    return Array.from({ length: this.#count }, (_, i) => ({
      offset: this.#first(i),
      length: this.#last(i) - this.#first(i),
    }));
  }
  close(): void {
    if (!this.#closed) {
      this.#closed = true;
      this.#pairs = EMPTY;
      this.#count = 0;
      this.#budget?.free(this.arenaBytes);
      this.#memory?.release(this.arenaBytes);
      this.#memory?.unregister?.();
      this.#budget = null;
      this.#memory = null;
    }
  }
}
