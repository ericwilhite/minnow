/** Private paged spill ownership/free ranges. One prepaid cache and two I/O frames;
 * no managed directory, free-run vector or close-sized release array. Never recovered. */
import { xxHash32 } from "../hash/xxhash32.js";
import type { ByteBudget } from "../storage/extents.js";
import type { ScratchMetadata } from "../storage/contract.js";
import { SpillCorruptionError } from "./contract.js";

export const SCRATCH_CONTROL_BYTES = 20 * 1024;
export const SCRATCH_META_PAGE_BYTES = 4096;
export const SCRATCH_RELEASE_BATCH = 16;
const MAGIC = 0x524e534d,
  HEADER = 32,
  PAIRS = (SCRATCH_META_PAGE_BYTES - HEADER) / 16;
const valid = (n: number) => Number.isSafeInteger(n) && n >= 0;
const fail = () => new SpillCorruptionError("Malformed scratch allocator metadata");

export class RangeChain {
  head = 0;
  tail = 0;
  count = 0;
  lastOffset = 0;
  lastLength = 0;
  constructor(readonly owner: number) {}
}

export class ScratchRanges {
  #buffers: {
    frame: Uint8Array;
    temporary: Uint8Array;
    release: Uint8Array;
    view: DataView;
  } | null = null;
  #cached = 0;
  #dirty = false;
  #next = 1;
  #free = 0;
  #closed = false;
  readonly free: RangeChain;
  get frame(): Uint8Array {
    if (this.#buffers === null) throw new TypeError("Scratch allocator closed");
    return this.#buffers.frame;
  }
  get temporary(): Uint8Array {
    if (this.#buffers === null) throw new TypeError("Scratch allocator closed");
    return this.#buffers.temporary;
  }
  get releaseFrame(): Uint8Array {
    if (this.#buffers === null) throw new TypeError("Scratch allocator closed");
    return this.#buffers.release;
  }
  get #view(): DataView {
    if (this.#buffers === null) throw new TypeError("Scratch allocator closed");
    return this.#buffers.view;
  }
  readonly store: ScratchMetadata;
  constructor(
    openMetadata: () => ScratchMetadata,
    readonly budget: ByteBudget,
  ) {
    // Fixed minimum is admitted by the service owner before any operator starts spilling.
    if (!budget.allocate(SCRATCH_CONTROL_BYTES))
      throw new RangeError("Scratch allocator workspace must be prepaid by its owner");
    try {
      this.store = openMetadata();
      const frame = new Uint8Array(SCRATCH_META_PAGE_BYTES);
      this.#buffers = {
        frame,
        temporary: new Uint8Array(SCRATCH_META_PAGE_BYTES),
        release: new Uint8Array(SCRATCH_META_PAGE_BYTES),
        view: new DataView(frame.buffer),
      };
      this.free = new RangeChain(0);
    } catch (error) {
      budget.free(SCRATCH_CONTROL_BYTES);
      throw error;
    }
  }
  get residentEntries(): number {
    // All three page buffers are prepaid, including stale encoded slots and native response credit.
    return this.#closed ? 0 : 3 * PAIRS;
  }
  get count(): number {
    return this.#view.getUint32(24, true);
  }
  set count(n: number) {
    this.#view.setUint32(24, n, true);
    this.#dirty = true;
  }
  get next(): number {
    return this.#view.getFloat64(16, true);
  }
  set next(n: number) {
    this.#view.setFloat64(16, n, true);
    this.#dirty = true;
  }
  offset(i: number): number {
    return this.#view.getFloat64(HEADER + i * 16, true);
  }
  length(i: number): number {
    return this.#view.getFloat64(HEADER + i * 16 + 8, true);
  }
  pair(i: number, offset: number, length: number): void {
    this.#view.setFloat64(HEADER + i * 16, offset, true);
    this.#view.setFloat64(HEADER + i * 16 + 8, length, true);
    this.#dirty = true;
  }
  #check(bytes: Uint8Array, owner: number): void {
    const v = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength),
      crc = v.getUint32(28, true);
    v.setUint32(28, 0, true);
    const actual = xxHash32(bytes);
    v.setUint32(28, crc, true);
    if (
      v.getUint32(0, true) !== MAGIC ||
      v.getFloat64(8, true) !== owner ||
      v.getUint32(24, true) > PAIRS ||
      crc !== actual
    )
      throw fail();
    const next = v.getFloat64(16, true);
    if (!valid(next) || next >= this.#next) throw fail();
    for (let i = 0; i < v.getUint32(24, true); i++) {
      const a = v.getFloat64(HEADER + i * 16, true),
        n = v.getFloat64(HEADER + i * 16 + 8, true);
      if (!valid(a) || !valid(n) || n === 0 || a % 4096 !== 0 || n % 4096 !== 0 || !valid(a + n))
        throw fail();
    }
  }
  async #save(id: number, bytes: Uint8Array): Promise<void> {
    const v = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
    v.setUint32(28, 0, true);
    v.setUint32(28, xxHash32(bytes), true);
    await this.store.write(id - 1, bytes);
  }
  async flush(): Promise<void> {
    if (this.#cached !== 0 && this.#dirty) {
      await this.#save(this.#cached, this.frame);
      this.#dirty = false;
    }
  }
  async load(id: number, owner: number): Promise<void> {
    if (!valid(id) || id === 0 || id >= this.#next) throw fail();
    if (this.#cached !== id) {
      await this.flush();
      this.#cached = 0;
      await this.store.read(id - 1, this.frame);
      this.#check(this.frame, owner);
      this.#cached = id;
    } else if (this.#view.getFloat64(8, true) !== owner) throw fail();
  }
  async read(id: number, owner: number, target: Uint8Array): Promise<void> {
    await this.flush();
    if (!valid(id) || id === 0 || id >= this.#next) throw fail();
    await this.store.read(id - 1, target);
    this.#check(target, owner);
  }
  async #allocate(): Promise<number> {
    if (this.#free === 0) return this.#next++;
    const id = this.#free;
    await this.read(id, -1, this.temporary);
    this.#free = new DataView(this.temporary.buffer).getFloat64(16, true);
    return id;
  }
  async #initialize(id: number, owner: number, next = 0): Promise<void> {
    await this.flush();
    this.frame.fill(0);
    this.#view.setUint32(0, MAGIC, true);
    this.#view.setFloat64(8, owner, true);
    this.#view.setFloat64(16, next, true);
    this.#cached = id;
    this.#dirty = true;
  }
  async recycle(id: number): Promise<void> {
    if (this.#cached === id) {
      this.#cached = 0;
      this.#dirty = false;
    }
    this.temporary.fill(0);
    const v = new DataView(this.temporary.buffer);
    v.setUint32(0, MAGIC, true);
    v.setFloat64(8, -1, true);
    v.setFloat64(16, this.#free, true);
    await this.#save(id, this.temporary);
    this.#free = id;
  }
  async append(chain: RangeChain, offset: number, length: number): Promise<void> {
    if (chain.tail !== 0) {
      await this.load(chain.tail, chain.owner);
      if (this.count > 0 && this.offset(this.count - 1) + this.length(this.count - 1) === offset) {
        this.pair(
          this.count - 1,
          this.offset(this.count - 1),
          this.length(this.count - 1) + length,
        );
        chain.lastLength += length;
        return;
      }
    }
    if (chain.tail === 0 || this.count === PAIRS) {
      const id = await this.#allocate();
      if (chain.tail !== 0) {
        await this.load(chain.tail, chain.owner);
        this.next = id;
      } else chain.head = id;
      await this.#initialize(id, chain.owner);
      chain.tail = id;
    }
    this.pair(this.count, offset, length);
    this.count++;
    chain.count++;
    chain.lastOffset = offset;
    chain.lastLength = length;
  }
  async trim(chain: RangeChain, freed: number): Promise<void> {
    await this.load(chain.tail, chain.owner);
    const i = this.count - 1,
      n = this.length(i) - freed;
    if (n <= 0) throw fail();
    this.pair(i, this.offset(i), n);
    chain.lastLength -= freed;
  }
  async #remove(chain: RangeChain, id: number, previous: number, index: number): Promise<number> {
    await this.load(id, chain.owner);
    const next = this.next;
    this.frame.copyWithin(HEADER + index * 16, HEADER + (index + 1) * 16, HEADER + this.count * 16);
    this.count--;
    chain.count--;
    if (this.count !== 0) return id;
    if (previous === 0) chain.head = next;
    else {
      await this.load(previous, chain.owner);
      this.next = next;
    }
    if (chain.tail === id) chain.tail = previous;
    await this.recycle(id);
    return next;
  }
  /** Consume the beginning of a tracked run; exact placement only follows an allocated tail. */
  async take(bytes: number, at: number | null): Promise<number | null> {
    let id = this.free.head,
      previous = 0,
      visited = 0;
    while (id !== 0) {
      if (++visited >= this.#next) throw fail();
      await this.load(id, 0);
      for (let i = 0; i < this.count; i++) {
        const offset = this.offset(i),
          length = this.length(i);
        if ((at === null || at === offset) && length >= bytes) {
          if (length === bytes) await this.#remove(this.free, id, previous, i);
          else this.pair(i, offset + bytes, length - bytes);
          return offset;
        }
      }
      previous = id;
      id = this.next;
    }
    return null;
  }
  async #insert(id: number, index: number, offset: number, length: number): Promise<void> {
    const chain = this.free;
    if (id === 0) {
      await this.append(chain, offset, length);
      return;
    }
    await this.load(id, 0);
    if (this.count === PAIRS) {
      const newId = await this.#allocate();
      await this.load(id, 0);
      const half = PAIRS / 2,
        oldNext = this.next;
      this.temporary.fill(0);
      const v = new DataView(this.temporary.buffer);
      v.setUint32(0, MAGIC, true);
      v.setFloat64(8, 0, true);
      v.setFloat64(16, oldNext, true);
      v.setUint32(24, PAIRS - half, true);
      this.temporary.set(this.frame.subarray(HEADER + half * 16, HEADER + PAIRS * 16), HEADER);
      await this.#save(newId, this.temporary);
      this.count = half;
      this.next = newId;
      if (chain.tail === id) chain.tail = newId;
      if (index > half) {
        id = newId;
        index -= half;
        await this.load(id, 0);
      }
    }
    this.frame.copyWithin(HEADER + (index + 1) * 16, HEADER + index * 16, HEADER + this.count * 16);
    this.pair(index, offset, length);
    this.count++;
    chain.count++;
  }
  /** Canonical sorted/coalesced free ranges, paged rather than a resident FreeSpaceMap. */
  async add(offset: number, length: number): Promise<void> {
    const chain = this.free;
    if (chain.tail !== 0) {
      await this.load(chain.tail, 0);
      const i = this.count - 1,
        lastEnd = this.offset(i) + this.length(i);
      if (offset >= lastEnd) {
        await this.append(chain, offset, length);
        return;
      }
    }
    let id = chain.head,
      previous = 0,
      index = 0,
      visited = 0;
    while (id !== 0) {
      if (++visited >= this.#next * (PAIRS + 1)) throw fail();
      await this.load(id, 0);
      if (index === this.count) {
        previous = id;
        id = this.next;
        index = 0;
        continue;
      }
      const a = this.offset(index),
        n = this.length(index),
        end = offset + length;
      if (a + n < offset) {
        index++;
        continue;
      }
      if (a > end) break;
      if (a < end && a + n > offset) throw new SpillCorruptionError("Scratch range released twice");
      const mergedStart = Math.min(a, offset),
        mergedEnd = Math.max(a + n, end);
      offset = mergedStart;
      length = mergedEnd - mergedStart;
      const oldId = id;
      id = await this.#remove(chain, id, previous, index);
      if (id !== oldId) index = 0;
      if (id === 0) break;
      await this.load(id, 0);
      if (index > this.count) index = 0;
    }
    await this.#insert(id, index, offset, length);
  }
  async dropTail(end: number): Promise<number> {
    const chain = this.free;
    if (chain.tail === 0) return end;
    await this.load(chain.tail, 0);
    const i = this.count - 1;
    if (this.offset(i) + this.length(i) !== end) return end;
    const trimmed = this.offset(i);
    let previous = 0,
      id = chain.head,
      visited = 0;
    while (id !== chain.tail) {
      if (++visited >= this.#next) throw fail();
      await this.load(id, 0);
      previous = id;
      id = this.next;
      if (id === 0) throw fail();
    }
    await this.#remove(chain, id, previous, i);
    return trimmed;
  }
  async reset(): Promise<void> {
    this.#cached = 0;
    this.#dirty = false;
    this.#next = 1;
    this.#free = 0;
    this.free.head = 0;
    this.free.tail = 0;
    this.free.count = 0;
    this.free.lastLength = 0;
    await this.store.reset();
  }
  close(): void {
    if (!this.#closed) {
      this.#closed = true;
      this.#buffers = null;
      this.budget.free(SCRATCH_CONTROL_BYTES);
    }
  }
}
