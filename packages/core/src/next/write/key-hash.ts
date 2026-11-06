/**
 * The memtable's key-index delta (access/key-index.ts "Structure"): equality-key bytes →
 * entries `(rowId, seq, put | tombstone)`, newest first per key. Typed arrays only: an
 * open-addressing table of key slots over xxHash32, keys in one byte arena, entries in parallel
 * arrays linked per key. Nothing is allocated per key or entry once grown.
 *
 * - `add` puts an entry at the head of its key's chain.
 * - `pop` removes the newest entry of the newest key touched, in exact reverse order of `add`
 *   (transaction rollback walks its undo log backwards).
 * - `restamp` changes an entry's seq (commit turns pending seqs into the LSN).
 * - `sorted` lists keys in byte order with their entries in row-id order (a run's input).
 */
import { xxHash32 } from "../hash/xxhash32.js";
import { compareBytes } from "../vector/varlen.js";
import { charged, copyRange, UNCHARGED, type Charge } from "./bytes.js";

/** Bytes of a fresh delta's arrays (slots 64×4, 4 key arrays 16×4, arena 256, entries 16×(8+8+1+4+4)). */
const INITIAL_BYTES = 64 * 4 + 4 * 16 * 4 + 256 + 16 * 25;
const ENTRY_BYTES = 25;

export class KeyDelta {
  readonly #charge: Charge;
  /** Bytes charged for the delta's arrays. */
  chargedBytes = INITIAL_BYTES;

  /** `charge` pays for every array before it is allocated (the memtable's counter). */
  constructor(charge: Charge = UNCHARGED) {
    this.#charge = charge;
    charged(charge, INITIAL_BYTES);
  }

  #grow(bytes: number, freed: number): void {
    charged(this.#charge, bytes);
    this.chargedBytes += bytes - freed;
    // The old arrays are released after the copy (the caller replaces them right after).
    this.#charge.free(freed);
  }

  /** Returns every charged byte (the delta is dropped). */
  releaseCharge(): void {
    this.#charge.free(this.chargedBytes);
    this.chargedBytes = 0;
  }

  #slots: Int32Array = new Int32Array(64);
  #mask = 63;
  #hashes = new Int32Array(16);
  #keyStart = new Int32Array(16);
  #keyEnd = new Int32Array(16);
  #head = new Int32Array(16);
  #arena = new Uint8Array(256);
  #arenaUsed = 0;
  keyCount = 0;
  #rowIds = new Float64Array(16);
  #seqs = new Float64Array(16);
  #tombstones = new Uint8Array(16);
  #next = new Int32Array(16);
  #keyOf = new Int32Array(16);
  entryCount = 0;

  /** Bytes held (charged). */
  get bytes(): number {
    return this.chargedBytes;
  }

  /** Key slot of `key[start … end)`, or −1. */
  find(key: Uint8Array, start = 0, end = key.length): number {
    const hash = xxHash32(key, 0, start, end) | 0;
    let slot = hash & this.#mask;
    for (;;) {
      const entry = this.#slots[slot] ?? 0;
      if (entry === 0) return -1;
      const index = entry - 1;
      if (this.#hashes[index] === hash && this.#sameKey(index, key, start, end)) return index;
      slot = (slot + 1) & this.#mask;
    }
  }

  #sameKey(index: number, key: Uint8Array, start: number, end: number): boolean {
    const keyStart = this.#keyStart[index] ?? 0;
    const length = (this.#keyEnd[index] ?? 0) - keyStart;
    if (length !== end - start) return false;
    const arena = this.#arena;
    for (let k = 0; k < length; k++) if (arena[keyStart + k] !== key[start + k]) return false;
    return true;
  }

  /** The newest entry of key slot `keySlot`, or −1. */
  head(keySlot: number): number {
    return this.#head[keySlot] ?? -1;
  }

  nextEntry(entry: number): number {
    return this.#next[entry] ?? -1;
  }

  rowId(entry: number): number {
    return this.#rowIds[entry] ?? 0;
  }

  seq(entry: number): number {
    return this.#seqs[entry] ?? 0;
  }

  isTombstone(entry: number): boolean {
    return this.#tombstones[entry] === 1;
  }

  /**
   * The arena holding every key slot's bytes at `keyStart … keyEnd` (a later growth copies them
   * to a new arena; bytes already written never change).
   */
  keyArena(): Uint8Array {
    return this.#arena;
  }

  keyStart(keySlot: number): number {
    return this.#keyStart[keySlot] ?? 0;
  }

  keyEnd(keySlot: number): number {
    return this.#keyEnd[keySlot] ?? 0;
  }

  keyBytes(keySlot: number): Uint8Array {
    return this.#arena.subarray(this.#keyStart[keySlot] ?? 0, this.#keyEnd[keySlot] ?? 0);
  }

  /** Adds an entry for `key[start … end)` at the head of its chain; returns the entry index. */
  add(
    key: Uint8Array,
    start: number,
    end: number,
    rowId: number,
    seq: number,
    tombstone: boolean,
  ): number {
    let keySlot = this.find(key, start, end);
    if (keySlot < 0) keySlot = this.#insertKey(key, start, end);
    const entry = this.entryCount;
    if (entry >= this.#rowIds.length) this.#growEntries();
    this.#rowIds[entry] = rowId;
    this.#seqs[entry] = seq;
    this.#tombstones[entry] = tombstone ? 1 : 0;
    this.#next[entry] = this.#head[keySlot] ?? -1;
    this.#keyOf[entry] = keySlot;
    this.#head[keySlot] = entry;
    this.entryCount = entry + 1;
    return entry;
  }

  /** Removes the newest entry (`entryCount − 1`), which must be `entry`. */
  pop(entry: number): void {
    if (entry !== this.entryCount - 1)
      throw new RangeError("Key delta entries are removed newest first");
    const keySlot = this.#keyOf[entry] ?? 0;
    this.#head[keySlot] = this.#next[entry] ?? -1;
    this.entryCount = entry;
  }

  restamp(entry: number, seq: number): void {
    this.#seqs[entry] = seq;
  }

  /** Restamps entries `from … to − 1`. */
  restampRange(from: number, to: number, seq: number): void {
    this.#seqs.fill(seq, from, to);
  }

  /** The key slot an entry belongs to. */
  keyOf(entry: number): number {
    return this.#keyOf[entry] ?? 0;
  }

  #insertKey(key: Uint8Array, start: number, end: number): number {
    const index = this.keyCount;
    if (index >= this.#hashes.length) this.#growKeys();
    if ((index + 1) * 2 > this.#slots.length) this.#rehash(this.#slots.length * 2);
    const length = end - start;
    if (this.#arenaUsed + length > this.#arena.length) {
      let size = this.#arena.length * 2;
      while (size < this.#arenaUsed + length) size *= 2;
      this.#grow(size, this.#arena.byteLength);
      const grown = new Uint8Array(size);
      grown.set(this.#arena.subarray(0, this.#arenaUsed));
      this.#arena = grown;
    }
    copyRange(this.#arena, this.#arenaUsed, key, start, end);
    const hash = xxHash32(key, 0, start, end) | 0;
    this.#hashes[index] = hash;
    this.#keyStart[index] = this.#arenaUsed;
    this.#keyEnd[index] = this.#arenaUsed + length;
    this.#head[index] = -1;
    this.#arenaUsed += length;
    this.keyCount = index + 1;
    this.#place(index, hash);
    return index;
  }

  #place(index: number, hash: number): void {
    let slot = hash & this.#mask;
    while ((this.#slots[slot] ?? 0) !== 0) slot = (slot + 1) & this.#mask;
    this.#slots[slot] = index + 1;
  }

  #rehash(size: number): void {
    this.#grow(size * 4, this.#slots.byteLength);
    this.#slots = new Int32Array(size);
    this.#mask = size - 1;
    for (let index = 0; index < this.keyCount; index++)
      this.#place(index, this.#hashes[index] ?? 0);
  }

  #growKeys(): void {
    const size = this.#hashes.length * 2;
    this.#grow(4 * size * 4, 4 * this.#hashes.byteLength);
    const grow = (array: Int32Array) => {
      const grown = new Int32Array(size);
      grown.set(array);
      return grown;
    };
    this.#hashes = grow(this.#hashes);
    this.#keyStart = grow(this.#keyStart);
    this.#keyEnd = grow(this.#keyEnd);
    this.#head = grow(this.#head);
  }

  #growEntries(): void {
    const size = this.#rowIds.length * 2;
    this.#grow(size * ENTRY_BYTES, this.#rowIds.length * ENTRY_BYTES);
    const rowIds = new Float64Array(size);
    rowIds.set(this.#rowIds);
    this.#rowIds = rowIds;
    const seqs = new Float64Array(size);
    seqs.set(this.#seqs);
    this.#seqs = seqs;
    const tombstones = new Uint8Array(size);
    tombstones.set(this.#tombstones);
    this.#tombstones = tombstones;
    const next = new Int32Array(size);
    next.set(this.#next);
    this.#next = next;
    const keyOf = new Int32Array(size);
    keyOf.set(this.#keyOf);
    this.#keyOf = keyOf;
  }

  /**
   * `sorted` without the objects: each entry with `seq ≤ bound` (and `keep`, if given), in key-byte
   * order and, per key, (row id, put before tombstone) order, handed to `visit` as the key's range
   * in `arena` plus the entry's fields. Allocates only the key order (one Int32Array) and a reused
   * scratch for keys with several entries.
   */
  forEachSorted(
    bound: number,
    visit: (
      arena: Uint8Array,
      start: number,
      end: number,
      rowId: number,
      tombstone: boolean,
    ) => void,
    keep: ((rowId: number, tombstone: boolean) => boolean) | null = null,
  ): void {
    const arena = this.#arena;
    const starts = this.#keyStart;
    const ends = this.#keyEnd;
    const compareSlots = (a: number, b: number) =>
      compareBytes(arena, starts[a] ?? 0, ends[a] ?? 0, arena, starts[b] ?? 0, ends[b] ?? 0);
    const order = new Int32Array(this.keyCount);
    let ascending = true;
    for (let k = 0; k < this.keyCount; k++) {
      order[k] = k;
      if (ascending && k > 0 && compareSlots(k - 1, k) > 0) ascending = false;
    }
    if (!ascending) order.sort(compareSlots);
    const rowIds = this.#rowIds;
    const seqs = this.#seqs;
    const tombstones = this.#tombstones;
    const next = this.#next;
    let scratch = this.#scratch;
    for (const keySlot of order) {
      const start = starts[keySlot] ?? 0;
      const end = ends[keySlot] ?? 0;
      // One entry: the common case, no gathering.
      const head = this.#head[keySlot] ?? -1;
      if (head >= 0 && (next[head] ?? -1) < 0) {
        const rowId = rowIds[head] ?? 0;
        const tombstone = tombstones[head] === 1;
        if ((seqs[head] ?? 0) <= bound && (keep === null || keep(rowId, tombstone)))
          visit(arena, start, end, rowId, tombstone);
        continue;
      }
      let count = 0;
      for (let entry = head; entry >= 0; entry = next[entry] ?? -1) {
        if ((seqs[entry] ?? 0) > bound) continue;
        const rowId = rowIds[entry] ?? 0;
        const tombstone = tombstones[entry] === 1;
        if (keep !== null && !keep(rowId, tombstone)) continue;
        if (count >= scratch.length) {
          const grown = new Int32Array(scratch.length * 2);
          grown.set(scratch);
          scratch = this.#scratch = grown;
        }
        // Insertion by (row id, put before tombstone): chains are short.
        let at = count++;
        while (at > 0) {
          const prior = scratch[at - 1] ?? 0;
          const priorRow = rowIds[prior] ?? 0;
          if (
            priorRow < rowId ||
            (priorRow === rowId && (tombstones[prior] ?? 0) <= (tombstone ? 1 : 0))
          )
            break;
          scratch[at] = prior;
          at -= 1;
        }
        scratch[at] = entry;
      }
      for (let k = 0; k < count; k++) {
        const entry = scratch[k] ?? 0;
        visit(arena, start, end, rowIds[entry] ?? 0, tombstones[entry] === 1);
      }
    }
  }

  /**
   * The smallest and largest key this delta has ever held (key slots, cached until a key is
   * added; popped entries keep their keys, so the range may be wider: sound for skipping), or
   * null when it has no key. For deltas that no longer change (frozen, staged, published
   * memtables), it is computed once.
   */
  keyRange(): { readonly min: Uint8Array; readonly max: Uint8Array } | null {
    if (this.keyCount === 0) return null;
    if (this.#range !== null && this.#rangeKeys === this.keyCount) return this.#range;
    const arena = this.#arena;
    const starts = this.#keyStart;
    const ends = this.#keyEnd;
    let min = 0;
    let max = 0;
    for (let slot = 1; slot < this.keyCount; slot++) {
      const start = starts[slot] ?? 0;
      const end = ends[slot] ?? 0;
      if (compareBytes(arena, start, end, arena, starts[min] ?? 0, ends[min] ?? 0) < 0) min = slot;
      if (compareBytes(arena, start, end, arena, starts[max] ?? 0, ends[max] ?? 0) > 0) max = slot;
    }
    this.#range = { min: this.keyBytes(min).slice(), max: this.keyBytes(max).slice() };
    this.#rangeKeys = this.keyCount;
    return this.#range;
  }

  #range: { readonly min: Uint8Array; readonly max: Uint8Array } | null = null;
  #rangeKeys = 0;

  /** Scratch for one key's entries in `forEachSorted`. */
  #scratch = new Int32Array(16);

  /**
   * Key slots in key-byte order, each with its entries in (row id, put before tombstone) order,
   * keeping only entries with `seq ≤ bound`. Keys whose entries are all filtered out are skipped.
   */
  sorted(
    bound: number,
  ): Array<{ key: Uint8Array; entries: Array<{ rowId: number; tombstone: boolean }> }> {
    const arena = this.#arena;
    const starts = this.#keyStart;
    const ends = this.#keyEnd;
    const compareSlots = (a: number, b: number) =>
      compareBytes(arena, starts[a] ?? 0, ends[a] ?? 0, arena, starts[b] ?? 0, ends[b] ?? 0);
    const order = new Int32Array(this.keyCount);
    let ascending = true;
    for (let k = 0; k < this.keyCount; k++) {
      order[k] = k;
      if (ascending && k > 0 && compareSlots(k - 1, k) > 0) ascending = false;
    }
    // Keys inserted in order (serial ids, time keys) need no sort.
    if (!ascending) order.sort(compareSlots);
    const out: Array<{ key: Uint8Array; entries: Array<{ rowId: number; tombstone: boolean }> }> =
      [];
    for (const keySlot of order) {
      const entries: Array<{ rowId: number; tombstone: boolean }> = [];
      for (let entry = this.head(keySlot); entry >= 0; entry = this.nextEntry(entry))
        if (this.seq(entry) <= bound)
          entries.push({ rowId: this.rowId(entry), tombstone: this.isTombstone(entry) });
      if (entries.length === 0) continue;
      entries.sort((a, b) => a.rowId - b.rowId || Number(a.tombstone) - Number(b.tombstone));
      // A view: a frozen delta's arena no longer changes (the run is encoded before release).
      out.push({ key: this.keyBytes(keySlot), entries });
    }
    return out;
  }
}
