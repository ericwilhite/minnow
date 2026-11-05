/**
 * An in-memory data space and extent allocator: the test and benchmark double for the store's
 * data space (storage/contract.ts) and the write path's free-space map, as spill/memory-spill.ts
 * is for spill. Not a production allocator: first fit over a sorted free list, in-place growth
 * only into free space directly after an extent.
 */
import type { BlobRef, ExtentRef } from "../format/manifest.js";
import { xxHash32 } from "../hash/xxhash32.js";
import type { DataRange } from "../storage/contract.js";
import { DATA_ALIGN, StoreCorruptionError } from "../storage/contract.js";
import {
  MAX_BLOB_BYTES,
  MAX_READ_BYTES,
  PageBudgetError,
  type PageBudget,
  type PageLease,
  type PageSpace,
} from "./reader.js";
import type { ExtentAllocator, ExtentStore } from "./writer.js";

const alignUp = (value: number): number => Math.ceil(value / DATA_ALIGN) * DATA_ALIGN;

export class MemoryDataSpace implements ExtentStore {
  bytes = new Uint8Array(1 << 20);
  length = 0;
  reads = 0;
  readBytes = 0;

  async writeData(offset: number, bytes: Uint8Array): Promise<void> {
    if (offset % DATA_ALIGN !== 0) throw new RangeError("Data writes start at DATA_ALIGN offsets");
    const end = offset + bytes.length;
    if (end > this.bytes.length) {
      let size = this.bytes.length * 2;
      while (size < end) size *= 2;
      const grown = new Uint8Array(size);
      grown.set(this.bytes.subarray(0, this.length));
      this.bytes = grown;
    }
    this.bytes.set(bytes, offset);
    this.length = Math.max(this.length, end);
  }

  async readData(ranges: readonly DataRange[]): Promise<Uint8Array[]> {
    this.reads += 1;
    return ranges.map(({ offset, length }) => {
      if (offset + length > this.length)
        throw new StoreCorruptionError("Read past the data length");
      this.readBytes += length;
      return this.bytes.subarray(offset, offset + length);
    });
  }
}

export class MemoryExtentAllocator implements ExtentAllocator {
  /** Free ranges, sorted by offset, aligned. */
  readonly free: Array<{ offset: number; length: number }> = [];
  end = 0;
  reserved = 0;

  reserve(length: number): ExtentRef {
    const size = alignUp(Math.max(1, length));
    const index = this.free.findIndex((range) => range.length >= size);
    let offset: number;
    if (index >= 0) {
      const range = this.free[index] ?? { offset: 0, length: 0 };
      offset = range.offset;
      if (range.length === size) this.free.splice(index, 1);
      else this.free[index] = { offset: range.offset + size, length: range.length - size };
    } else {
      offset = this.end;
      this.end += size;
    }
    this.reserved += size;
    return { offset, length: size };
  }

  extend(extent: ExtentRef, length: number): ExtentRef | null {
    const size = alignUp(length);
    const end = extent.offset + alignUp(extent.length);
    const extra = size - alignUp(extent.length);
    if (extra <= 0) return { offset: extent.offset, length: size };
    if (end === this.end) {
      this.end += extra;
      this.reserved += extra;
      return { offset: extent.offset, length: size };
    }
    const index = this.free.findIndex((range) => range.offset === end && range.length >= extra);
    if (index < 0) return null;
    const range = this.free[index] ?? { offset: 0, length: 0 };
    if (range.length === extra) this.free.splice(index, 1);
    else this.free[index] = { offset: range.offset + extra, length: range.length - extra };
    this.reserved += extra;
    return { offset: extent.offset, length: size };
  }

  trim(extent: ExtentRef, used: number): ExtentRef {
    const kept = alignUp(used);
    const held = alignUp(extent.length);
    if (held > kept) this.#free(extent.offset + kept, held - kept);
    return { offset: extent.offset, length: used };
  }

  release(extent: ExtentRef): void {
    this.#free(extent.offset, alignUp(extent.length));
  }

  #free(offset: number, length: number): void {
    if (length <= 0) return;
    this.reserved -= length;
    let index = this.free.findIndex((range) => range.offset > offset);
    if (index < 0) index = this.free.length;
    this.free.splice(index, 0, { offset, length });
    // Coalesce with the neighbours on either side.
    const next = this.free[index + 1];
    if (next?.offset === offset + length)
      this.free.splice(index, 2, { offset, length: length + next.length });
    const merged = this.free[index];
    const previous = this.free[index - 1];
    if (merged !== undefined && previous !== undefined) {
      if (previous.offset + previous.length === merged.offset)
        this.free.splice(index - 1, 2, {
          offset: previous.offset,
          length: previous.length + merged.length,
        });
    }
    const last = this.free[this.free.length - 1];
    if (last !== undefined && last.offset + last.length === this.end) {
      this.end = last.offset;
      this.free.pop();
    }
  }
}

/**
 * The extent space's reader over a `MemoryDataSpace` (storage/extents.ts semantics, for tests and
 * benchmarks): `read` verifies each blob's xxHash32 (`StoreCorruptionError` on a mismatch),
 * charges the lease to the caller's budget, refuses past `MAX_READ_BYTES`, and caches the
 * verified copy; `peek` returns a cached blob or null. `cache: false` makes every load a read.
 */
export class MemorySpace implements PageSpace {
  readonly #data: MemoryDataSpace;
  readonly #cache = new Map<string, Uint8Array>();
  readonly #caching: boolean;
  reads = 0;
  peeks = 0;
  hits = 0;
  /** Bytes leased and not yet released. */
  leased = 0;

  constructor(data: MemoryDataSpace, options: { readonly cache?: boolean } = {}) {
    this.#data = data;
    this.#caching = options.cache ?? true;
  }

  static #key(ref: BlobRef): string {
    return `${String(ref.offset)}:${String(ref.length)}:${String(ref.checksum)}`;
  }

  /** Drops every cached blob (a cold cache). */
  evict(): void {
    this.#cache.clear();
  }

  async read(
    refs: readonly BlobRef[],
    options: { readonly budget: PageBudget; readonly signal?: AbortSignal },
  ): Promise<PageLease> {
    const total = refs.reduce((sum, ref) => sum + ref.length, 0);
    if (total > MAX_READ_BYTES) throw new RangeError("A read asks for more than MAX_READ_BYTES");
    if (!options.budget.allocate(total)) throw new PageBudgetError(total);
    this.reads++;
    let bytes: Uint8Array[];
    try {
      bytes = refs.map((ref) => {
        const key = MemorySpace.#key(ref);
        const cached = this.#cache.get(key);
        if (cached !== undefined) return cached;
        if (ref.length > MAX_BLOB_BYTES)
          throw new RangeError("A blob is larger than MAX_BLOB_BYTES");
        if (ref.offset + ref.length > this.#data.length)
          throw new StoreCorruptionError("Read past the data length");
        const copy = this.#data.bytes.slice(ref.offset, ref.offset + ref.length);
        if (xxHash32(copy) !== ref.checksum)
          throw new StoreCorruptionError(`Checksum mismatch at ${String(ref.offset)}`);
        if (this.#caching) this.#cache.set(key, copy);
        return copy;
      });
    } catch (error) {
      options.budget.free(total);
      throw error;
    }
    this.leased += total;
    let released = false;
    return {
      bytes,
      release: () => {
        if (released) return;
        released = true;
        this.leased -= total;
        options.budget.free(total);
      },
    };
  }

  peek(ref: BlobRef): Uint8Array | null {
    this.peeks++;
    const found = this.#cache.get(MemorySpace.#key(ref)) ?? null;
    if (found !== null) this.hits++;
    return found;
  }
}
