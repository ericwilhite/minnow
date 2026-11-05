/**
 * One builder's space (extents.ts `ExtentBuilder`): a logical extent of growing capacity, kept
 * contiguous where it can be and made contiguous at seal where it cannot (lead ruling R15.2). Pure
 * bookkeeping over a `FreeSpaceMap`; the builder does the I/O the plans describe.
 *
 * - `open` reserves the first segment (first fit).
 * - `extend(bytes)` grows the last segment in place when the space after it is free
 *   (`allocateAt`), else reserves a new segment anywhere. Logical positions never move: blob
 *   offsets are relative to the logical extent, and a write that crosses a segment boundary is
 *   split into one physical piece per segment (`pieces`).
 * - `seal(length)` of one segment trims its unused tail. Of several, it reserves one contiguous
 *   extent of `length` and returns the copies that move every segment's bytes into it; the old
 *   segments are released after the copies (`release`). That relocation reads and rewrites the
 *   extent once, through the space's 1 MiB write slab: no extra memory, ~2× the extent's I/O.
 *   Callers reserve from good estimates (page statistics) so it stays rare.
 */
import type { DataRange } from "./contract.js";
import type { FreeSpaceMap } from "./free-space.js";

interface Segment {
  readonly offset: number;
  /** Physical bytes reserved (a multiple of the alignment). */
  capacity: number;
  /** Logical position of the segment's first byte. */
  readonly start: number;
}

export interface SealPlan {
  /** The sealed extent (offset is aligned; length is the bytes written). */
  readonly extent: { readonly offset: number; readonly length: number };
  /** Copies to perform before `release`, in order (empty unless relocated). */
  readonly moves: ReadonlyArray<{
    readonly from: number;
    readonly to: number;
    readonly length: number;
  }>;
  /** Space to free once the moves are done (trimmed tails and old segments). */
  readonly release: readonly DataRange[];
}

export class ExtentReservation {
  readonly #map: FreeSpaceMap;
  readonly #segments: Segment[] = [];
  #state: "open" | "sealed" | "aborted" = "open";

  constructor(map: FreeSpaceMap, capacity: number) {
    if (!(Number.isSafeInteger(capacity) && capacity > 0)) throw new RangeError("Bad capacity");
    this.#map = map;
    const size = map.sizeOf(capacity);
    this.#segments.push({ offset: map.allocate(size), capacity: size, start: 0 });
  }

  /** Logical capacity: the sum of the segments. */
  get capacity(): number {
    const last = this.#last();
    return last.start + last.capacity;
  }

  /** Physical segments, in logical order (diagnostics and tests). */
  get segments(): ReadonlyArray<{ readonly offset: number; readonly capacity: number }> {
    return this.#segments.map(({ offset, capacity }) => ({ offset, capacity }));
  }

  /** Adds `bytes` of capacity. Returns true if it grew in place (no relocation at seal). */
  extend(bytes: number): boolean {
    this.#open();
    const size = this.#map.sizeOf(bytes);
    if (size === 0) return true;
    const last = this.#last();
    if (this.#map.allocateAt(last.offset + last.capacity, size)) {
      last.capacity += size;
      return true;
    }
    this.#segments.push({
      offset: this.#map.allocate(size),
      capacity: size,
      start: last.start + last.capacity,
    });
    return false;
  }

  /** The physical pieces of logical bytes `[position, position + length)`, in order. */
  pieces(position: number, length: number): DataRange[] {
    this.#open();
    if (position < 0 || length < 0 || position + length > this.capacity)
      throw new RangeError("Write outside the reserved capacity");
    const pieces: DataRange[] = [];
    let at = position;
    const end = position + length;
    for (const segment of this.#segments) {
      if (at >= end) break;
      const segmentEnd = segment.start + segment.capacity;
      if (at >= segmentEnd) continue;
      const take = Math.min(end, segmentEnd) - at;
      pieces.push({ offset: segment.offset + (at - segment.start), length: take });
      at += take;
    }
    return pieces;
  }

  /** Ends the reservation at `length` written bytes (> 0): see the module comment. */
  seal(length: number): SealPlan {
    this.#open();
    if (!(Number.isSafeInteger(length) && length > 0 && length <= this.capacity))
      throw new RangeError("Bad sealed length");
    this.#state = "sealed";
    const used = this.#map.sizeOf(length);
    const [first, ...rest] = this.#segments;
    if (first !== undefined && used <= first.capacity) {
      // Everything written fits the first segment: trim it, drop the unused overflow segments.
      const release = rest.map(({ offset, capacity }) => ({ offset, length: capacity }));
      if (first.capacity > used)
        release.unshift({ offset: first.offset + used, length: first.capacity - used });
      return { extent: { offset: first.offset, length }, moves: [], release };
    }
    const target = this.#map.allocate(used);
    const moves = this.#segments
      .filter((segment) => segment.start < length)
      .map((segment) => ({
        from: segment.offset,
        to: target + segment.start,
        length: Math.min(segment.capacity, length - segment.start),
      }));
    const release = this.#segments.map(({ offset, capacity }) => ({ offset, length: capacity }));
    return { extent: { offset: target, length }, moves, release };
  }

  /** Every segment, for the caller to free (nothing was published). */
  abort(): DataRange[] {
    this.#open();
    this.#state = "aborted";
    return this.#segments.map(({ offset, capacity }) => ({ offset, length: capacity }));
  }

  #last(): Segment {
    const last = this.#segments.at(-1);
    if (last === undefined) throw new Error("A reservation always has a segment");
    return last;
  }

  #open(): void {
    if (this.#state !== "open") throw new TypeError(`The reservation is ${this.#state}`);
  }
}
