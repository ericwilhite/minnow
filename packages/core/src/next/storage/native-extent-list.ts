/** Scalar decoder over an exclusively borrowed, prepaid read-slab window. No decoded extent
 * objects or whole-page copy is retained. The caller owns the immutable page descriptors.
 */
import { pauseIfDue, type TaskWork } from "../task.js";
import { xxHash32 } from "../hash/xxhash32.js";
import { DATA_ALIGN, StoreCorruptionError } from "./contract.js";
import { LIST_PAGE_EXTENTS, type ExtentList, type ExtentListPage } from "./extent-list.js";
import type { PageCache } from "./page-cache.js";

const HEADER = 16,
  ENTRY = 16;
export const NATIVE_LIST_WINDOW_BYTES = 192 * 1024;
const fail = (why: string): never => {
  throw new StoreCorruptionError(`Extent list: ${why}`);
};
/** Two borrowed windows, already charged by PageCache's read slab. Each cursor keeps its
 * own bytes while decoding; a hit copies from its peer before either window can be reused.
 * Reconstruction serializes reads. No extent-count-sized index or extra payload allocation.
 */
export class NativeListPageCache {
  #firstOffset = -1;
  #firstLength = 0;
  #secondOffset = -1;
  #secondLength = 0;
  constructor(
    readonly first: Uint8Array,
    readonly second: Uint8Array,
  ) {}
  async read(
    cache: PageCache,
    offset: number,
    target: Uint8Array,
    required: number,
  ): Promise<number> {
    const isFirst =
      target.buffer === this.first.buffer && target.byteOffset === this.first.byteOffset;
    const peer = isFirst ? this.second : this.first;
    const peerOffset = isFirst ? this.#secondOffset : this.#firstOffset;
    const peerLength = isFirst ? this.#secondLength : this.#firstLength;
    // A failed read must never advertise stale or partially overwritten bytes.
    if (isFirst) this.#firstLength = 0;
    else this.#secondLength = 0;
    let length = target.length;
    if (offset >= peerOffset && offset + required <= peerOffset + peerLength) {
      length = Math.min(length, peerOffset + peerLength - offset);
      target.set(peer.subarray(offset - peerOffset, offset - peerOffset + length));
    } else await cache.readRaw(offset, target);
    if (isFirst) {
      this.#firstOffset = offset;
      this.#firstLength = length;
    } else {
      this.#secondOffset = offset;
      this.#secondLength = length;
    }
    return length;
  }
}
export class NativeExtentList {
  offset = 0;
  length = 0;
  #index = 0;
  #entry = 0;
  #count = 0;
  #lastEnd = 0;
  #view: DataView | null = null;
  #windowOffset = -1;
  #windowLength = 0;
  #closed = false;
  constructor(
    readonly list: ExtentList,
    readonly cache: PageCache,
    readonly window: Uint8Array,
    readonly dataLength: number,
    readonly work?: TaskWork,
    readonly shared?: NativeListPageCache,
  ) {
    if (window.length !== NATIVE_LIST_WINDOW_BYTES)
      throw new RangeError("Wrong native list window");
  }
  /** Synchronous between page changes and real slice checks; callers need not await every pair. */
  next(): boolean | Promise<boolean> {
    if (this.#closed) throw new TypeError("Native list cursor is closed");
    if (this.#entry % 128 === 0) {
      const pending = pauseIfDue(this.work);
      if (pending !== undefined) return pending.then(() => this.#advance());
    }
    return this.#advance();
  }
  #advance(): boolean | Promise<boolean> {
    this.work?.signal?.throwIfAborted();
    if (this.#entry === this.#count) {
      if (this.#index === this.list.length) {
        this.#view = null;
        return false;
      }
      return this.#load();
    }
    const view = this.#view;
    if (view === null) return fail("missing page bytes");
    const at = HEADER + this.#entry++ * ENTRY;
    const offset = view.getFloat64(at, true),
      length = view.getFloat64(at + 8, true);
    const end = offset + length,
      alignedEnd = offset + Math.ceil(length / DATA_ALIGN) * DATA_ALIGN;
    if (
      !Number.isSafeInteger(offset) ||
      offset < 0 ||
      offset % DATA_ALIGN !== 0 ||
      !Number.isSafeInteger(length) ||
      length <= 0 ||
      !Number.isSafeInteger(end) ||
      !Number.isSafeInteger(alignedEnd) ||
      end > this.dataLength ||
      offset < this.#lastEnd
    )
      return fail("invalid, overlapping or descending extent");
    const page = this.list[this.#index - 1];
    if (page === undefined || (this.#entry === 1 && offset !== page.first))
      return fail("first offset disagrees with root");
    this.offset = offset;
    this.length = length;
    this.#lastEnd = alignedEnd;
    return true;
  }
  #physical(page: ExtentListPage): number {
    const blob = page.blob,
      extent = blob.extent;
    if (
      !Number.isSafeInteger(page.first) ||
      page.first < 0 ||
      page.first % DATA_ALIGN !== 0 ||
      !Number.isInteger(page.count) ||
      page.count < 1 ||
      page.count > LIST_PAGE_EXTENTS ||
      !Number.isSafeInteger(extent.offset) ||
      extent.offset < 0 ||
      extent.offset % DATA_ALIGN !== 0 ||
      !Number.isSafeInteger(extent.length) ||
      extent.length < 1 ||
      !Number.isSafeInteger(blob.offset) ||
      blob.offset < 0 ||
      blob.length !== HEADER + page.count * ENTRY ||
      blob.offset + blob.length > extent.length ||
      !Number.isSafeInteger(extent.offset + extent.length) ||
      extent.offset + extent.length > this.dataLength ||
      !Number.isInteger(blob.checksum) ||
      blob.checksum < 0 ||
      blob.checksum > 0xffffffff
    )
      return fail("invalid page descriptor");
    return extent.offset + blob.offset;
  }
  async #load(): Promise<boolean> {
    const page = this.list[this.#index];
    if (page === undefined) return fail("missing page descriptor");
    const offset = this.#physical(page),
      end = offset + page.blob.length;
    if (offset < this.#windowOffset || end > this.#windowOffset + this.#windowLength) {
      // Read up to two nearby payload pages per native call. Descriptors remain caller-owned.
      let readEnd = end;
      for (let at = this.#index + 1; at < Math.min(this.list.length, this.#index + 2); at++) {
        const following = this.list[at];
        if (following === undefined) return fail("missing following descriptor");
        const first = this.#physical(following),
          last = first + following.blob.length;
        if (first < readEnd || first - readEnd > 64 * 1024 || last - offset > this.window.length)
          break;
        readEnd = last;
      }
      this.#windowLength = 0;
      const target = this.window.subarray(0, readEnd - offset);
      if (this.shared === undefined) await this.cache.readRaw(offset, target);
      this.#windowLength =
        this.shared === undefined
          ? target.length
          : await this.shared.read(this.cache, offset, target, page.blob.length);
      this.#windowOffset = offset;
      const pending = pauseIfDue(this.work);
      if (pending !== undefined) await pending;
      this.work?.signal?.throwIfAborted();
    }
    const bytes = this.window.subarray(offset - this.#windowOffset, end - this.#windowOffset);
    if (xxHash32(bytes) !== page.blob.checksum) return fail("page checksum mismatch");
    const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
    if (
      view.getUint32(0, true) !== 0x314c584d ||
      view.getUint16(4, true) !== 1 ||
      view.getUint16(6, true) !== 0 ||
      view.getUint32(8, true) !== page.count ||
      view.getUint32(12, true) !== 0
    )
      return fail("page header disagrees with root");
    if (page.first < this.#lastEnd) return fail("page order overlaps previous page");
    this.#index++;
    this.#entry = 0;
    this.#count = page.count;
    this.#view = view;
    return this.#advance();
  }
  close(): void {
    this.#closed = true;
    this.#view = null;
  }
}
