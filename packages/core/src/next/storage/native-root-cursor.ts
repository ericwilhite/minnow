/** A root's payload pairs merged with its physical list-page allocations. Both inputs are
 * immutable. Physical-page sorting borrows a bounded S view; large page directories use the
 * existing external pair stream instead. No payload-extent vector is allocated.
 */
import { pauseIfDue, type TaskWork } from "../task.js";
import { DATA_ALIGN, StoreCorruptionError } from "./contract.js";
import type { ExtentList } from "./extent-list.js";
import { NativeExtentList } from "./native-extent-list.js";

export interface ScalarExtents {
  readonly offset: number;
  readonly length: number;
  next(work?: TaskWork): boolean | Promise<boolean>;
  close(): void | Promise<void>;
}
const EMPTY = new Float64Array(0);

/** Only list-page allocations are copied here, at 16 bytes per page, inside an existing slab. */
export class NativePhysicalExtents implements ScalarExtents {
  offset = 0;
  length = 0;
  #next = 0;
  #lastEnd = 0;
  #closed = false;
  private constructor(private pairs: Float64Array) {}
  static async open(
    list: ExtentList,
    scratch: Uint8Array,
    dataLength: number,
    work?: TaskWork,
  ): Promise<NativePhysicalExtents> {
    if (list.length > Math.floor(scratch.byteLength / 16) || scratch.byteOffset % 8 !== 0)
      throw new RangeError("Physical page directory exceeds its prepaid window");
    const pairs = new Float64Array(scratch.buffer, scratch.byteOffset, list.length * 2);
    const pause = async () => {
      const pending = pauseIfDue(work);
      if (pending !== undefined) await pending;
    };
    for (let at = 0; at < list.length; at++) {
      if (at % 128 === 0) await pause();
      const extent = list[at]?.blob.extent;
      if (
        extent === undefined ||
        !Number.isSafeInteger(extent.offset) ||
        extent.offset < 0 ||
        extent.offset % DATA_ALIGN !== 0 ||
        !Number.isSafeInteger(extent.length) ||
        extent.length <= 0 ||
        !Number.isSafeInteger(extent.offset + extent.length) ||
        extent.offset + extent.length > dataLength
      )
        throw new StoreCorruptionError("Invalid physical extent-list page");
      pairs[2 * at] = extent.offset;
      pairs[2 * at + 1] = extent.length;
    }
    const compare = (a: number, b: number) =>
      (pairs[2 * a] ?? 0) - (pairs[2 * b] ?? 0) ||
      (pairs[2 * a + 1] ?? 0) - (pairs[2 * b + 1] ?? 0);
    const swap = (a: number, b: number) => {
      const offset = pairs[2 * a] ?? 0,
        length = pairs[2 * a + 1] ?? 0;
      pairs[2 * a] = pairs[2 * b] ?? 0;
      pairs[2 * a + 1] = pairs[2 * b + 1] ?? 0;
      pairs[2 * b] = offset;
      pairs[2 * b + 1] = length;
    };
    let comparisons = 0;
    const sift = async (start: number, end: number) => {
      let parent = start;
      for (;;) {
        if (comparisons++ % 128 === 0) await pause();
        let child = 2 * parent + 1;
        if (child >= end) return;
        if (child + 1 < end && compare(child, child + 1) < 0) child++;
        if (compare(parent, child) >= 0) return;
        swap(parent, child);
        parent = child;
      }
    };
    for (let start = Math.floor(list.length / 2) - 1; start >= 0; start--)
      await sift(start, list.length);
    for (let end = list.length - 1; end > 0; end--) {
      swap(0, end);
      await sift(0, end);
    }
    await pause();
    return new NativePhysicalExtents(pairs);
  }
  next(): boolean {
    if (this.#closed) throw new TypeError("Physical extent cursor is closed");
    while (this.#next < this.pairs.length) {
      const offset = this.pairs[this.#next++] ?? 0;
      const length = this.pairs[this.#next++] ?? 0;
      if (this.#next > 2 && offset === this.offset) {
        if (length !== this.length)
          throw new StoreCorruptionError("List pages disagree on their shared allocation");
        continue;
      }
      if (offset < this.#lastEnd)
        throw new StoreCorruptionError("Physical list-page allocations overlap");
      this.offset = offset;
      this.length = length;
      this.#lastEnd = offset + Math.ceil(length / DATA_ALIGN) * DATA_ALIGN;
      return true;
    }
    return false;
  }
  close(): void {
    this.#closed = true;
    this.pairs = EMPTY;
  }
}

/** The fast path stays synchronous between payload-page reads and shared-clock pauses. */
export class NativeRootExtents implements ScalarExtents {
  offset = 0;
  length = 0;
  #started = false;
  #left = false;
  #right = false;
  #selected: 0 | 1 | 2 = 0;
  #closed = false;
  readonly #leftReady = (has: boolean) => {
    this.#left = has;
    return this.#choose();
  };
  readonly #rightReady = (has: boolean) => {
    this.#right = has;
    return this.#choose();
  };
  constructor(
    readonly payload: NativeExtentList,
    readonly physical: ScalarExtents,
  ) {}
  next(work?: TaskWork): boolean | Promise<boolean> {
    if (this.#closed) throw new TypeError("Native root cursor is closed");
    if (!this.#started) {
      this.#started = true;
      const pending = this.physical.next(work);
      return typeof pending === "boolean"
        ? this.#prime(pending)
        : pending.then((has) => this.#prime(has));
    }
    if (this.#selected === 0) return false;
    if (this.#selected === 1) {
      const pending = this.payload.next();
      return typeof pending === "boolean"
        ? this.#leftReady(pending)
        : pending.then(this.#leftReady);
    }
    const pending = this.physical.next(work);
    return typeof pending === "boolean"
      ? this.#rightReady(pending)
      : pending.then(this.#rightReady);
  }
  #prime(hasPhysical: boolean): boolean | Promise<boolean> {
    this.#right = hasPhysical;
    const pending = this.payload.next();
    return typeof pending === "boolean" ? this.#leftReady(pending) : pending.then(this.#leftReady);
  }

  #choose(): boolean {
    if (this.#left && this.#right && this.payload.offset === this.physical.offset)
      throw new StoreCorruptionError("Payload names its own list-page allocation");
    this.#selected =
      this.#left && (!this.#right || this.payload.offset < this.physical.offset)
        ? 1
        : this.#right
          ? 2
          : 0;
    if (this.#selected === 0) return false;
    const source = this.#selected === 1 ? this.payload : this.physical;
    this.offset = source.offset;
    this.length = source.length;
    return true;
  }
  close(): void | Promise<void> {
    if (this.#closed) return;
    this.#closed = true;
    this.payload.close();
    return this.physical.close();
  }
}
