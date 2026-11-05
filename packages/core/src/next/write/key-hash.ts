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

export class KeyDelta {
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

  /** Bytes held (for memtable accounting). */
  get bytes(): number {
    return (
      this.#slots.byteLength +
      this.#hashes.byteLength * 4 +
      this.#arena.byteLength +
      this.#rowIds.byteLength * 2 +
      this.#tombstones.byteLength +
      this.#next.byteLength * 2
    );
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

  #insertKey(key: Uint8Array, start: number, end: number): number {
    const index = this.keyCount;
    if (index >= this.#hashes.length) this.#growKeys();
    if ((index + 1) * 2 > this.#slots.length) this.#rehash(this.#slots.length * 2);
    const length = end - start;
    if (this.#arenaUsed + length > this.#arena.length) {
      let size = this.#arena.length * 2;
      while (size < this.#arenaUsed + length) size *= 2;
      const grown = new Uint8Array(size);
      grown.set(this.#arena.subarray(0, this.#arenaUsed));
      this.#arena = grown;
    }
    this.#arena.set(key.subarray(start, end), this.#arenaUsed);
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
    this.#slots = new Int32Array(size);
    this.#mask = size - 1;
    for (let index = 0; index < this.keyCount; index++)
      this.#place(index, this.#hashes[index] ?? 0);
  }

  #growKeys(): void {
    const size = this.#hashes.length * 2;
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
      out.push({ key: this.keyBytes(keySlot).slice(), entries });
    }
    return out;
  }
}
