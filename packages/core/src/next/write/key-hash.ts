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
import type { MaybePromise } from "../access/key-index.js";
import { taskClock, yieldTask } from "../exec/pipeline.js";
import { xxHash32 } from "../hash/xxhash32.js";
import { compareBytes } from "../vector/varlen.js";
import { charged, copyRange, UNCHARGED, type Charge } from "./bytes.js";

type DeltaVisitor = (
  arena: Uint8Array,
  start: number,
  end: number,
  rowId: number,
  tombstone: boolean,
) => void;
type KeepEntry = (rowId: number, tombstone: boolean) => boolean;

/** Bytes of a fresh delta's arrays (slots 64×4, 4 key arrays 16×4, arena 256, entries 16×(8+8+1+4+4)). */
const INITIAL_BYTES = 64 * 4 + 4 * 16 * 4 + 256 + 16 * 25;
const ENTRY_BYTES = 25;
const NO_ENTRIES = new Int32Array(0);

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
   * Entries in key-byte order and, per key, (row id, put before tombstone) order.
   * Ordered chains are visited linearly; arbitrary chains use an in-place O(n log n) heapsort.
   * Large traversals share the worker task clock, and suspend only for a real task yield.
   * Scratch belongs to this traversal: another traversal can run while this one yields.
   */
  forEachSorted(
    bound: number,
    visit: DeltaVisitor,
    keep: KeepEntry | null = null,
    check: (() => void) | null = null,
  ): MaybePromise<void> {
    const work = this.#sortedWork(bound, visit, keep, check);
    const resume = (): MaybePromise<void> => {
      const next = work.next();
      return next.done ? undefined : next.value.then(resume);
    };
    return resume();
  }

  *#sortedWork(
    bound: number,
    visit: DeltaVisitor,
    keep: KeepEntry | null,
    check: (() => void) | null,
  ): Generator<Promise<void>, void, unknown> {
    yield* checkpoint(check);
    const arena = this.#arena;
    const starts = this.#keyStart;
    const ends = this.#keyEnd;
    const compareSlots = (a: number, b: number) =>
      compareBytes(arena, starts[a] ?? 0, ends[a] ?? 0, arena, starts[b] ?? 0, ends[b] ?? 0);
    const order = new Int32Array(this.keyCount);
    let ascending = true;
    for (let from = 0; from < this.keyCount; from += 128) {
      yield* checkpoint(check);
      ascending = fillKeyOrder(
        order,
        from,
        Math.min(from + 128, order.length),
        ascending,
        compareSlots,
      );
    }
    if (!ascending) yield* sortIndices(order, order.length, compareSlots, check);
    const rowIds = this.#rowIds;
    const seqs = this.#seqs;
    const tombstones = this.#tombstones;
    const next = this.#next;
    const compareEntries = (a: number, b: number) =>
      (rowIds[a] ?? 0) - (rowIds[b] ?? 0) || (tombstones[a] ?? 0) - (tombstones[b] ?? 0);
    let scratch: Int32Array = NO_ENTRIES;
    for (let slot = 0; slot < order.length;) {
      yield* checkpoint(check);
      const limit = Math.min(slot + 128, order.length);
      slot = this.#emitSingles(order, slot, limit, bound, visit, keep);
      if (slot === limit) continue;
      const keySlot = order[slot++] ?? 0;
      const start = starts[keySlot] ?? 0;
      const end = ends[keySlot] ?? 0;
      const head = this.#head[keySlot] ?? -1;
      if (scratch.length === 0) scratch = new Int32Array(16);
      let count = 0;
      let walked = 0;
      let forward = true;
      let reverse = true;
      for (let entry = head; entry >= 0; entry = next[entry] ?? -1) {
        if ((walked++ & 127) === 0) yield* checkpoint(check);
        if ((seqs[entry] ?? 0) > bound) continue;
        const rowId = rowIds[entry] ?? 0;
        const tombstone = tombstones[entry] === 1;
        if (keep !== null && !keep(rowId, tombstone)) continue;
        if (count >= scratch.length) {
          const grown: Int32Array = new Int32Array(scratch.length * 2);
          grown.set(scratch);
          scratch = grown;
        }
        if (count > 0) {
          const comparison = compareEntries(scratch[count - 1] ?? 0, entry);
          forward &&= comparison <= 0;
          reverse &&= comparison >= 0;
        }
        scratch[count++] = entry;
      }
      if (!forward && !reverse) yield* sortIndices(scratch, count, compareEntries, check);
      for (let from = 0; from < count; from += 128) {
        yield* checkpoint(check);
        emitEntries(
          scratch,
          from,
          Math.min(from + 128, count),
          reverse,
          count,
          rowIds,
          tombstones,
          arena,
          start,
          end,
          visit,
        );
      }
    }
  }

  /** Ordinary bounded loop: row emission stays outside the generator's suspension state. */
  #emitSingles(
    order: Int32Array,
    from: number,
    to: number,
    bound: number,
    visit: DeltaVisitor,
    keep: KeepEntry | null,
  ): number {
    const arena = this.#arena,
      starts = this.#keyStart,
      ends = this.#keyEnd,
      heads = this.#head,
      next = this.#next,
      seqs = this.#seqs,
      rowIds = this.#rowIds,
      tombstones = this.#tombstones;
    for (let at = from; at < to; at++) {
      const slot = order[at] ?? 0,
        head = heads[slot] ?? -1;
      if (head < 0) continue;
      if ((next[head] ?? -1) >= 0) return at;
      if ((seqs[head] ?? 0) > bound) continue;
      const rowId = rowIds[head] ?? 0,
        tombstone = tombstones[head] === 1;
      if (keep === null || keep(rowId, tombstone))
        visit(arena, starts[slot] ?? 0, ends[slot] ?? 0, rowId, tombstone);
    }
    return to;
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

/** Fill one bounded key-order batch without a suspension inside its hot loop. */
function fillKeyOrder(
  order: Int32Array,
  from: number,
  to: number,
  ascending: boolean,
  compare: (a: number, b: number) => number,
): boolean {
  for (let k = from; k < to; k++) {
    order[k] = k;
    if (ascending && k > 0 && compare(k - 1, k) > 0) ascending = false;
  }
  return ascending;
}

/** Emit one bounded chain batch; direction handles newest-first chains without sorting. */
function emitEntries(
  entries: Int32Array,
  from: number,
  to: number,
  reverse: boolean,
  count: number,
  rowIds: Float64Array,
  tombstones: Uint8Array,
  arena: Uint8Array,
  start: number,
  end: number,
  visit: DeltaVisitor,
): void {
  let at = reverse ? count - 1 - from : from;
  const step = reverse ? -1 : 1;
  for (let k = from; k < to; k++, at += step) {
    const entry = entries[at] ?? 0;
    visit(arena, start, end, rowIds[entry] ?? 0, tombstones[entry] === 1);
  }
}

/** A checkpoint never starts a clock; nested traversals spend the same worker task budget. */
function* checkpoint(check: (() => void) | null): Generator<Promise<void>, void, unknown> {
  check?.();
  if (taskClock.due()) {
    yield yieldTask();
    check?.();
  }
}

/** In-place heapsort, with no opaque native sort interval or auxiliary array. */
function* sortIndices(
  order: Int32Array,
  count: number,
  compare: (a: number, b: number) => number,
  check: (() => void) | null,
): Generator<Promise<void>, void, unknown> {
  let operations = 0;
  for (let start = count >>> 1, end = count; end > 1;) {
    if (start > 0) start--;
    else {
      end--;
      const first = order[0] ?? 0;
      order[0] = order[end] ?? 0;
      order[end] = first;
    }
    const value = order[start] ?? 0;
    let root = start;
    for (let child = root * 2 + 1; child < end; child = root * 2 + 1) {
      if ((operations++ & 127) === 0) yield* checkpoint(check);
      if (child + 1 < end && compare(order[child] ?? 0, order[child + 1] ?? 0) < 0) child++;
      if (compare(value, order[child] ?? 0) >= 0) break;
      order[root] = order[child] ?? 0;
      root = child;
    }
    order[root] = value;
    if ((operations++ & 127) === 0) yield* checkpoint(check);
  }
}
