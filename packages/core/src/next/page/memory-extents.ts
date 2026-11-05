/**
 * An in-memory extent space for the page suites and benchmarks: the storage track's
 * `ExtentReader`, `ExtentWriter` and `ExtentBuilder` (storage/extents.ts) over one growable byte
 * array, with the same observable rules, so the page writer and reader are tested against the
 * contract rather than a private allocator. Not a production space: no publish, pins or reclaim
 * generations (storage phase 2 implements those); first-fit allocation; no write slab.
 *
 * - `open(capacity)` reserves first fit; `extend` grows in place when the space after the
 *   reservation is free (true), else reserves a second segment and relocates at `seal` (false;
 *   `relocations` counts it).
 * - `append` places parts back to back, checksums each (xxHash32), and returns pending blobs with
 *   offsets relative to the extent; `seal` trims and returns `BlobRef`s carrying the extent.
 * - `read` checks each ref against its extent, verifies the checksum when bytes come from the
 *   array (`StoreCorruptionError`), refuses more than `MAX_READ_BYTES` (`RangeError`) and charges
 *   the lease to the budget (`ReadDeniedError`); verified copies are cached (unless
 *   `cache: false`) and `peek` serves them.
 */
import { xxHash32 } from "../hash/xxhash32.js";
import { DATA_ALIGN, StoreCorruptionError } from "../storage/contract.js";
import {
  MAX_BLOB_BYTES,
  MAX_READ_BYTES,
  ReadDeniedError,
  blobAddress,
  type BlobRef,
  type ByteBudget,
  type ExtentBuilder,
  type ExtentReader,
  type ExtentRef,
  type ExtentWriter,
  type PendingBlob,
  type ReadLease,
  type ReadOptions,
  type WriterState,
  type WrittenExtent,
} from "../storage/extents.js";

const alignUp = (value: number): number => Math.ceil(value / DATA_ALIGN) * DATA_ALIGN;

/** First fit over aligned runs; coalesces on free; the end grows on demand. */
class Allocator {
  readonly free: Array<{ offset: number; length: number }> = [];
  end = 0;
  reserved = 0;

  reserve(length: number): number {
    const size = alignUp(Math.max(1, length));
    const index = this.free.findIndex((range) => range.length >= size);
    this.reserved += size;
    if (index < 0) {
      const offset = this.end;
      this.end += size;
      return offset;
    }
    const range = this.free[index] ?? { offset: 0, length: 0 };
    if (range.length === size) this.free.splice(index, 1);
    else this.free[index] = { offset: range.offset + size, length: range.length - size };
    return range.offset;
  }

  /** Grows the run ending at `end` by `extra` aligned bytes, if they are free. */
  growAt(end: number, extra: number): boolean {
    const size = alignUp(extra);
    if (end === this.end) {
      this.end += size;
      this.reserved += size;
      return true;
    }
    const index = this.free.findIndex((range) => range.offset === end && range.length >= size);
    if (index < 0) return false;
    const range = this.free[index] ?? { offset: 0, length: 0 };
    if (range.length === size) this.free.splice(index, 1);
    else this.free[index] = { offset: range.offset + size, length: range.length - size };
    this.reserved += size;
    return true;
  }

  release(offset: number, length: number): void {
    const size = alignUp(length);
    if (size <= 0) return;
    this.reserved -= size;
    let index = this.free.findIndex((range) => range.offset > offset);
    if (index < 0) index = this.free.length;
    this.free.splice(index, 0, { offset, length: size });
    const next = this.free[index + 1];
    if (next?.offset === offset + size)
      this.free.splice(index, 2, { offset, length: size + next.length });
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

export interface MemoryExtentSpaceOptions {
  /** Cache verified blobs for `peek` (default true). */
  readonly cache?: boolean;
}

export class MemoryExtentSpace implements ExtentReader {
  bytes = new Uint8Array(1 << 20);
  readonly allocator = new Allocator();
  readonly #cache = new Map<string, Uint8Array>();
  readonly #caching: boolean;
  reads = 0;
  peeks = 0;
  hits = 0;
  relocations = 0;
  /** Bytes leased and not yet released. */
  leased = 0;

  constructor(options: MemoryExtentSpaceOptions = {}) {
    this.#caching = options.cache ?? true;
  }

  /** The same bytes behind another cache policy (a cold reader of one space). */
  view(options: MemoryExtentSpaceOptions): MemoryExtentSpace {
    const other = new MemoryExtentSpace(options);
    other.bytes = this.bytes;
    return other;
  }

  /** Drops every cached blob. */
  evict(): void {
    this.#cache.clear();
  }

  write(offset: number, bytes: Uint8Array): void {
    const end = offset + bytes.length;
    if (end > this.bytes.length) {
      let size = this.bytes.length * 2;
      while (size < end) size *= 2;
      const grown = new Uint8Array(size);
      grown.set(this.bytes);
      this.bytes = grown;
    }
    this.bytes.set(bytes, offset);
  }

  beginWrite(label: string): ExtentWriter {
    return new MemoryWriter(this, label);
  }

  static #key(ref: BlobRef): string {
    return `${String(blobAddress(ref))}:${String(ref.length)}:${String(ref.checksum)}`;
  }

  async read(refs: readonly BlobRef[], options: ReadOptions): Promise<ReadLease> {
    const total = refs.reduce((sum, ref) => sum + ref.length, 0);
    if (total > MAX_READ_BYTES) throw new RangeError("A read asks for more than MAX_READ_BYTES");
    for (const ref of refs)
      if (ref.offset < 0 || ref.offset + ref.length > ref.extent.length)
        throw new StoreCorruptionError("A blob reference lies outside its extent");
    if (!options.budget.allocate(total)) throw new ReadDeniedError(total);
    this.reads++;
    let bytes: Uint8Array[];
    try {
      bytes = refs.map((ref) => {
        const key = MemoryExtentSpace.#key(ref);
        const cached = this.#cache.get(key);
        if (cached !== undefined) return cached;
        const address = blobAddress(ref);
        const copy = this.bytes.slice(address, address + ref.length);
        if (copy.length !== ref.length || xxHash32(copy) !== ref.checksum)
          throw new StoreCorruptionError(`Checksum mismatch at ${String(address)}`);
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
    const found = this.#cache.get(MemoryExtentSpace.#key(ref)) ?? null;
    if (found !== null) this.hits++;
    return found;
  }
}

class MemoryWriter implements ExtentWriter {
  state: WriterState = "open";
  readonly extents: ExtentRef[] = [];
  readonly #builders = new Set<MemoryBuilder>();

  constructor(
    readonly space: MemoryExtentSpace,
    readonly label: string,
  ) {}

  get bytes(): number {
    let total = 0;
    for (const builder of this.#builders) total += builder.capacity;
    for (const extent of this.extents) total += alignUp(extent.length);
    return total;
  }

  open(capacity: number): ExtentBuilder {
    if (this.state !== "open") throw new TypeError(`Writer ${this.label} is ${this.state}`);
    if (!(capacity > 0)) throw new RangeError("An extent needs a positive capacity");
    const builder = new MemoryBuilder(this, capacity);
    this.#builders.add(builder);
    return builder;
  }

  sealed(builder: MemoryBuilder, extent: ExtentRef | null): void {
    this.#builders.delete(builder);
    if (extent !== null) this.extents.push(extent);
  }

  async write(parts: readonly Uint8Array[]): Promise<WrittenExtent> {
    if (parts.length === 0) throw new RangeError("write needs parts");
    const builder = this.open(
      Math.max(
        1,
        parts.reduce((sum, part) => sum + part.length, 0),
      ),
    );
    await builder.append(parts);
    const written = await builder.seal();
    if (written === null) throw new RangeError("write wrote nothing");
    return written;
  }

  async discard(extent: ExtentRef): Promise<void> {
    const index = this.extents.findIndex(
      (sealed) => sealed.offset === extent.offset && sealed.length === extent.length,
    );
    if (index < 0) throw new RangeError("discard of an extent this writer did not seal");
    this.extents.splice(index, 1);
    this.space.allocator.release(extent.offset, extent.length);
  }

  async abort(): Promise<void> {
    if (this.state === "aborted") return;
    if (this.state !== "open") throw new TypeError("abort after publishing began");
    for (const builder of this.#builders) builder.releaseAll();
    this.#builders.clear();
    for (const extent of this.extents.splice(0))
      this.space.allocator.release(extent.offset, extent.length);
    this.state = "aborted";
  }
}

interface Segment {
  readonly offset: number;
  capacity: number;
  /** Logical position of its first byte. */
  readonly start: number;
}

class MemoryBuilder implements ExtentBuilder {
  readonly #segments: Segment[] = [];
  readonly #pending: PendingBlob[] = [];
  capacity: number;
  length = 0;
  #sealed = false;

  constructor(
    readonly writer: MemoryWriter,
    capacity: number,
  ) {
    const size = alignUp(capacity);
    this.#segments.push({ offset: writer.space.allocator.reserve(size), capacity: size, start: 0 });
    this.capacity = size;
  }

  extend(bytes: number): boolean {
    if (this.#sealed) throw new TypeError("extend after seal");
    const size = alignUp(Math.max(1, bytes));
    const last = this.#segments[this.#segments.length - 1];
    if (last === undefined) throw new TypeError("extend of a released builder");
    this.capacity += size;
    if (this.writer.space.allocator.growAt(last.offset + last.capacity, size)) {
      last.capacity += size;
      return true;
    }
    this.#segments.push({
      offset: this.writer.space.allocator.reserve(size),
      capacity: size,
      start: last.start + last.capacity,
    });
    return false;
  }

  #place(bytes: Uint8Array, at: number): void {
    let done = 0;
    for (const segment of this.#segments) {
      const segmentEnd = segment.start + segment.capacity;
      const position = at + done;
      if (position >= segmentEnd || done === bytes.length) continue;
      const take = Math.min(bytes.length - done, segmentEnd - position);
      this.writer.space.write(
        segment.offset + position - segment.start,
        bytes.subarray(done, done + take),
      );
      done += take;
    }
  }

  async append(parts: readonly Uint8Array[]): Promise<PendingBlob[]> {
    if (this.#sealed) throw new TypeError("append after seal");
    const total = parts.reduce((sum, part) => sum + part.length, 0);
    for (const part of parts)
      if (part.length > MAX_BLOB_BYTES) throw new RangeError("A part exceeds MAX_BLOB_BYTES");
    if (this.length + total > this.capacity)
      throw new RangeError("append past the builder's capacity");
    const out: PendingBlob[] = [];
    for (const part of parts) {
      this.#place(part, this.length);
      const blob = { offset: this.length, length: part.length, checksum: xxHash32(part) };
      out.push(blob);
      this.#pending.push(blob);
      this.length += part.length;
    }
    return out;
  }

  releaseAll(): void {
    for (const segment of this.#segments.splice(0))
      this.writer.space.allocator.release(segment.offset, segment.capacity);
    this.#sealed = true;
  }

  async seal(): Promise<WrittenExtent | null> {
    if (this.#sealed) throw new TypeError("seal twice");
    const space = this.writer.space;
    if (this.length === 0) {
      this.releaseAll();
      this.writer.sealed(this, null);
      return null;
    }
    let offset: number;
    const first = this.#segments[0];
    if (first === undefined) throw new TypeError("seal of a released builder");
    if (this.#segments.length === 1) {
      offset = first.offset;
      const kept = alignUp(this.length);
      if (first.capacity > kept) space.allocator.release(offset + kept, first.capacity - kept);
    } else {
      // Relocate: one contiguous range, then free the segments.
      space.relocations++;
      offset = space.allocator.reserve(this.length);
      let copied = 0;
      for (const segment of this.#segments) {
        const take = Math.min(segment.capacity, this.length - copied);
        if (take > 0)
          space.write(offset + copied, space.bytes.slice(segment.offset, segment.offset + take));
        copied += take;
        space.allocator.release(segment.offset, segment.capacity);
      }
    }
    this.#segments.length = 0;
    this.#sealed = true;
    const extent: ExtentRef = { offset, length: this.length };
    this.writer.sealed(this, extent);
    return { extent, blobs: this.#pending.map((blob) => ({ extent, ...blob })) };
  }
}

/** A budget that admits everything (tests that do not check charging). */
export const UNLIMITED: ByteBudget = { allocate: () => true, free: () => undefined };
