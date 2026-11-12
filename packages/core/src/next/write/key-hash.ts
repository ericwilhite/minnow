/**
 * The memtable's key-index delta (access/key-index.ts "Structure"): equality-key bytes →
 * entries `(rowId, seq, put | tombstone)`, newest first per key. Typed arrays only: an
 * open-addressing table of key slots over xxHash32, keys in one byte arena, entries in parallel
 * arrays linked per key. A charged leaf directory maintains entry order on writes for lazy
 * ranges; its moves are bounded by the leaf size. Nothing is allocated per entry once grown.
 *
 * - `add` puts an entry at the head of its key's chain.
 * - `pop` removes the newest entry of the newest key touched, in exact reverse order of `add`
 *   (transaction rollback walks its undo log backwards).
 * - `restamp` changes an entry's seq (commit turns pending seqs into the LSN).
 * - `sorted` lists keys in byte order with their entries in row-id order (a run's input).
 */
import type { MaybePromise } from "../access/key-index.js";
import { taskClock, yieldTask } from "../exec/pipeline.js";
import { hashKey } from "./key-hashes.js";
import { xxHash32 } from "../hash/xxhash32.js";
import { compareBytes } from "../vector/varlen.js";
import { charged, copyRange, MemtableMemoryError, UNCHARGED, type Charge } from "./bytes.js";

import { OrderedEntries } from "./ordered-entries.js";

export type DeltaVisitor = ((
  arena: Uint8Array,
  start: number,
  end: number,
  rowId: number,
  tombstone: boolean,
  hash: number,
  bloomHash: number,
  single: boolean,
) => void) & {
  singles?: (
    data: SingleEntries,
    from: number,
    to: number,
    bound: number,
    keep: KeepEntry | null,
  ) => number;
};

/** Borrowed immutable arrays; the traversal and encodeDelta retain the delta's ownership. */
export interface SingleEntries {
  readonly arena: Uint8Array;
  readonly starts: Int32Array;
  readonly ends: Int32Array;
  readonly heads: Int32Array;
  readonly next: Int32Array;
  readonly seqs: Float64Array;
  readonly rowIds: Float64Array;
  readonly tombstones: Uint8Array;
  readonly hashes: Int32Array;
  readonly bloomHashes: Int32Array;
  readonly order: Int32Array | null;
}
type KeepEntry = (rowId: number, tombstone: boolean) => boolean;

/** Bytes of a fresh delta's arrays (slots 64×4, 5 key arrays 16×4, arena 256, entries 16×(8+8+1+4+4)). */
const INITIAL_BYTES = 64 * 4 + 5 * 16 * 4 + 256 + 16 * 25;
const ENTRY_BYTES = 25;
const NO_ENTRIES = new Int32Array(0);

/** Six big-endian bytes, zero padded, fit exactly in a JS integer. Ties still compare bytes. */
function orderPrefix(bytes: Uint8Array, start: number, end: number): number {
  let prefix = 0;
  for (let at = start; at < start + 6; at++)
    prefix = prefix * 256 + (at < end ? (bytes[at] ?? 0) : 0);
  return prefix;
}

export class KeyDelta {
  readonly #charge: Charge;
  /** Retained arrays/ranges plus the workspaces of active traversals. */
  chargedBytes = 0;
  #activeTraversals = 0;
  #releaseRequested = false;

  /** Every backing capacity is paid before its constructor runs. */
  constructor(charge: Charge = UNCHARGED) {
    this.#charge = charge;
    this.#reserve(INITIAL_BYTES);
    try {
      this.#slots = new Int32Array(64);
      this.#hashes = new Int32Array(16);
      this.#bloomHashes = new Int32Array(16);
      this.#keyStart = new Int32Array(16);
      this.#keyEnd = new Int32Array(16);
      this.#head = new Int32Array(16);
      this.#arena = new Uint8Array(256);
      this.#rowIds = new Float64Array(16);
      this.#seqs = new Float64Array(16);
      this.#tombstones = new Uint8Array(16);
      this.#next = new Int32Array(16);
      this.#keyOf = new Int32Array(16);
    } catch (error) {
      this.#releaseRetained();
      throw error;
    }
  }

  #reserve(bytes: number, charge: Charge = this.#charge): void {
    if (bytes === 0) return;
    charged(charge, bytes);
    this.chargedBytes += bytes;
  }

  #free(bytes: number, charge: Charge = this.#charge): void {
    if (bytes === 0) return;
    charge.free(bytes);
    this.chargedBytes -= bytes;
  }

  /** Idempotent. A suspended traversal retains the arrays until its last use settles. */
  releaseCharge(): void {
    if (this.#releaseRequested) return;
    this.#releaseRequested = true;
    if (this.#activeTraversals === 0) this.#releaseRetained();
  }

  #releaseRetained(): void {
    this.#free(this.chargedBytes);
  }

  #slots: Int32Array;
  #mask = 63;
  #hashes: Int32Array;
  #bloomHashes: Int32Array;
  readonly #computed = { first: 0, second: 0 };
  #keyStart: Int32Array;
  #keyEnd: Int32Array;
  #head: Int32Array;
  #arena: Uint8Array;
  #arenaUsed = 0;
  #maxKeyBytes = 0;
  get maxKeyBytes(): number {
    return this.#maxKeyBytes;
  }
  keyCount = 0;
  /** All allocated key slots, including those retained after rollback, are ascending. */
  #ascendingKeys = true;
  #lastPrefix: number | null = null;
  /** Entries below this bound are distinct ascending slots, entry id = key slot. */
  #ascendingEntryPrefix = 0;
  get ascendingEntryPrefix(): number {
    return this.#ascendingEntryPrefix;
  }
  get ascendingKeys(): boolean {
    return this.#ascendingKeys;
  }
  #rowIds: Float64Array;
  #seqs: Float64Array;
  #tombstones: Uint8Array;
  #next: Int32Array;
  #keyOf: Int32Array;
  entryCount = 0;

  /** Bytes held (charged). */
  get bytes(): number {
    return this.chargedBytes;
  }

  /** Key slot of `key[start … end)`, or −1. A supplied hash must be seed-0 XXH32 of this exact range. */
  find(key: Uint8Array, start = 0, end = key.length, hash = xxHash32(key, 0, start, end)): number {
    hash |= 0;
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

  #ordered: OrderedEntries | null = null;

  /** Maintained on writes: no first-page sort or traversal workspace. */
  ordered(): OrderedEntries | null {
    return this.#ordered;
  }

  /** A cursor retains the delta's paid arrays even if its owning view is released during I/O. */
  retain(): () => void {
    if (this.#releaseRequested) throw new RangeError("A released delta cannot be retained");
    this.#activeTraversals++;
    let live = true;
    return () => {
      if (!live) return;
      live = false;
      if (--this.#activeTraversals === 0 && this.#releaseRequested) this.#releaseRetained();
    };
  }

  #compareEntry(a: number, b: number): number {
    const ka = this.keyOf(a),
      kb = this.keyOf(b);
    return (
      compareBytes(
        this.#arena,
        this.keyStart(ka),
        this.keyEnd(ka),
        this.#arena,
        this.keyStart(kb),
        this.keyEnd(kb),
      ) ||
      this.rowId(a) - this.rowId(b) ||
      Number(this.isTombstone(a)) - Number(this.isTombstone(b)) ||
      a - b
    );
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
    hashKey(key, start, end, this.#computed);
    const hash = this.#computed.first | 0,
      bloomHash = this.#computed.second | 0;
    let keySlot = this.find(key, start, end, hash);
    const newKey = keySlot < 0;
    if (newKey) keySlot = this.#insertKey(key, start, end, hash, bloomHash);
    return this.#addEntry(keySlot, rowId, seq, tombstone, newKey);
  }

  /**
   * Validates a strictly ascending batch of five-byte keys (INT/DATE's normal equality form).
   * Complete unsigned payloads prove order; exactly one byte boundary check joins prior keys.
   * All validation precedes mutation. Other layouts, duplicates or disorder use ordinary add.
   */
  addOrderedBatch(
    bytes: Uint8Array,
    offsets: Int32Array,
    count: number,
    rowId: number,
    seq: number,
  ): boolean {
    if (!this.#ascendingKeys || count === 0) return false;
    const first = offsets[0] ?? 0;
    const tag = bytes[first];
    let previous = -1;
    for (let k = 0; k < count; k++) {
      const start = offsets[k] ?? 0,
        end = offsets[k + 1] ?? 0;
      if (end - start !== 5 || start < 0 || end > bytes.length || bytes[start] !== tag)
        return false;
      const word =
        (((bytes[start + 1] ?? 0) << 24) |
          ((bytes[start + 2] ?? 0) << 16) |
          ((bytes[start + 3] ?? 0) << 8) |
          (bytes[start + 4] ?? 0)) >>>
        0;
      if (word <= previous) return false;
      previous = word;
    }
    if (
      this.keyCount > 0 &&
      compareBytes(
        this.#arena,
        this.keyStart(this.keyCount - 1),
        this.keyEnd(this.keyCount - 1),
        bytes,
        first,
        offsets[1] ?? first,
      ) >= 0
    )
      return false;
    for (let k = 0; k < count; k++) {
      const start = offsets[k] ?? 0,
        end = offsets[k + 1] ?? 0;
      hashKey(bytes, start, end, this.#computed);
      const slot = this.#insertKey(
        bytes,
        start,
        end,
        this.#computed.first | 0,
        this.#computed.second | 0,
        true,
      );
      this.#addEntry(slot, rowId + k, seq, false, true);
    }
    return true;
  }

  #addEntry(
    keySlot: number,
    rowId: number,
    seq: number,
    tombstone: boolean,
    newKey: boolean,
  ): number {
    const entry = this.entryCount;
    if (entry >= this.#rowIds.length) this.#growEntries();
    this.#rowIds[entry] = rowId;
    this.#seqs[entry] = seq;
    this.#tombstones[entry] = tombstone ? 1 : 0;
    this.#next[entry] = this.#head[keySlot] ?? -1;
    this.#keyOf[entry] = keySlot;
    if (this.#ordered === null) {
      this.#reserve(64);
      this.#ordered = new OrderedEntries(
        {
          allocate: (bytes) => {
            this.#reserve(bytes);
            return true;
          },
          free: (bytes) => this.#free(bytes),
        },
        (a, b) => this.#compareEntry(a, b),
      );
    }
    // A fresh slot in an ascending key arena is greater than every previous entry's key.
    this.#ordered.add(entry, newKey && this.#ascendingKeys);
    if (entry === this.#ascendingEntryPrefix && newKey && keySlot === entry && this.#ascendingKeys)
      this.#ascendingEntryPrefix++;
    this.#head[keySlot] = entry;
    this.entryCount = entry + 1;
    return entry;
  }

  /** Removes the newest entry (`entryCount − 1`), which must be `entry`. */
  pop(entry: number): void {
    if (entry !== this.entryCount - 1)
      throw new RangeError("Key delta entries are removed newest first");
    this.#ascendingEntryPrefix = Math.min(this.#ascendingEntryPrefix, entry);
    this.#ordered?.remove(entry);
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

  /** Canonical hashes retained with immutable key bytes; popped entries retain their key slot. */
  keyHash(slot: number): number {
    return (this.#hashes[slot] ?? 0) >>> 0;
  }
  keyBloomHash(slot: number): number {
    return (this.#bloomHashes[slot] ?? 0) >>> 0;
  }

  #insertKey(
    key: Uint8Array,
    start: number,
    end: number,
    hash: number,
    bloomHash: number,
    ordered = false,
  ): number {
    const index = this.keyCount;
    if (index >= this.#hashes.length) this.#growKeys();
    if ((index + 1) * 2 > this.#slots.length) this.#rehash(this.#slots.length * 2);
    const length = end - start;
    if (this.#arenaUsed + length > this.#arena.length) {
      let size = this.#arena.length * 2;
      while (size < this.#arenaUsed + length) size *= 2;
      const freed = this.#arena.byteLength;
      this.#reserve(size);
      try {
        const grown = new Uint8Array(size);
        grown.set(this.#arena.subarray(0, this.#arenaUsed));
        this.#arena = grown;
      } catch (error) {
        this.#free(size);
        throw error;
      }
      this.#free(freed);
    }
    if (ordered) this.#lastPrefix = null;
    else if (this.#ascendingKeys) {
      const prefix = orderPrefix(key, start, end);
      const prior =
        this.#lastPrefix ??
        (index > 0
          ? orderPrefix(this.#arena, this.keyStart(index - 1), this.keyEnd(index - 1))
          : 0);
      if (
        index > 0 &&
        (prior > prefix ||
          (prior === prefix &&
            compareBytes(
              this.#arena,
              this.keyStart(index - 1),
              this.keyEnd(index - 1),
              key,
              start,
              end,
            ) > 0))
      )
        this.#ascendingKeys = false;
      this.#lastPrefix = prefix;
    }
    copyRange(this.#arena, this.#arenaUsed, key, start, end);
    this.#hashes[index] = hash;
    this.#bloomHashes[index] = bloomHash;
    this.#keyStart[index] = this.#arenaUsed;
    this.#keyEnd[index] = this.#arenaUsed + length;
    this.#head[index] = -1;
    this.#arenaUsed += length;
    if (length > this.#maxKeyBytes) this.#maxKeyBytes = length;
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
    const bytes = size * 4,
      freed = this.#slots.byteLength;
    this.#reserve(bytes);
    try {
      const grown = new Int32Array(size),
        mask = size - 1;
      for (let index = 0; index < this.keyCount; index++) {
        let slot = (this.#hashes[index] ?? 0) & mask;
        while ((grown[slot] ?? 0) !== 0) slot = (slot + 1) & mask;
        grown[slot] = index + 1;
      }
      this.#slots = grown;
      this.#mask = mask;
    } catch (error) {
      this.#free(bytes);
      throw error;
    }
    this.#free(freed);
  }

  #growKeys(): void {
    const size = this.#hashes.length * 2,
      bytes = 5 * size * 4,
      freed = 5 * this.#hashes.byteLength;
    this.#reserve(bytes);
    try {
      const grow = (array: Int32Array) => {
        const grown = new Int32Array(size);
        grown.set(array);
        return grown;
      };
      const hashes = grow(this.#hashes),
        bloomHashes = grow(this.#bloomHashes),
        starts = grow(this.#keyStart),
        ends = grow(this.#keyEnd),
        heads = grow(this.#head);
      this.#hashes = hashes;
      this.#bloomHashes = bloomHashes;
      this.#keyStart = starts;
      this.#keyEnd = ends;
      this.#head = heads;
    } catch (error) {
      this.#free(bytes);
      throw error;
    }
    this.#free(freed);
  }

  #growEntries(): void {
    const size = this.#rowIds.length * 2,
      bytes = size * ENTRY_BYTES,
      freed = this.#rowIds.length * ENTRY_BYTES;
    this.#reserve(bytes);
    try {
      const rowIds = new Float64Array(size);
      rowIds.set(this.#rowIds);
      const seqs = new Float64Array(size);
      seqs.set(this.#seqs);
      const tombstones = new Uint8Array(size);
      tombstones.set(this.#tombstones);
      const next = new Int32Array(size);
      next.set(this.#next);
      const keyOf = new Int32Array(size);
      keyOf.set(this.#keyOf);
      this.#rowIds = rowIds;
      this.#seqs = seqs;
      this.#tombstones = tombstones;
      this.#next = next;
      this.#keyOf = keyOf;
    } catch (error) {
      this.#free(bytes);
      throw error;
    }
    this.#free(freed);
  }

  /**
   * Entries in key-byte order and, per key, (row id, put before tombstone) order.
   * Proven ascending key slots need no permutation, comparison scan or order workspace.
   * Ordered chains are visited linearly; arbitrary chains use an in-place O(n log n) heapsort.
   * Large traversals share the worker task clock, and suspend only for a real task yield.
   * Scratch belongs to this traversal: another traversal can run while this one yields.
   * The caller keeps the input immutable until completion. `visit` borrows the key arena;
   * retaining it after completion requires retaining the delta's ownership too.
   */
  forEachSorted(
    bound: number,
    visit: DeltaVisitor,
    keep: KeepEntry | null = null,
    check: (() => void) | null = null,
    workspace: Charge = this.#charge,
  ): MaybePromise<void> {
    const work = this.#sortedWork(bound, visit, keep, check, workspace);
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
    workspace: Charge,
  ): Generator<Promise<void>, void, unknown> {
    if (this.#releaseRequested) throw new RangeError("A released key delta cannot be traversed");
    this.#activeTraversals++;
    let workspaceBytes = 0;
    try {
      yield* checkpoint(check);
      const arena = this.#arena;
      const starts = this.#keyStart;
      const ends = this.#keyEnd;
      const compareSlots = (a: number, b: number) =>
        compareBytes(arena, starts[a] ?? 0, ends[a] ?? 0, arena, starts[b] ?? 0, ends[b] ?? 0);
      const keyCount = this.keyCount;
      let order: Int32Array | null = null;
      if (!this.#ascendingKeys) {
        const orderBytes = keyCount * 4;
        this.#reserve(orderBytes, workspace);
        workspaceBytes += orderBytes;
        order = new Int32Array(keyCount);
        for (let from = 0; from < keyCount; from += 128) {
          yield* checkpoint(check);
          for (let k = from; k < Math.min(from + 128, keyCount); k++) order[k] = k;
        }
        yield* sortIndices(order, order.length, compareSlots, check);
      }
      const rowIds = this.#rowIds;
      const seqs = this.#seqs;
      const tombstones = this.#tombstones;
      const next = this.#next;
      const compareEntries = (a: number, b: number) =>
        (rowIds[a] ?? 0) - (rowIds[b] ?? 0) || (tombstones[a] ?? 0) - (tombstones[b] ?? 0);
      const singles: SingleEntries | null =
        visit.singles !== undefined
          ? {
              arena,
              starts,
              ends,
              heads: this.#head,
              next,
              seqs,
              rowIds,
              tombstones,
              hashes: this.#hashes,
              bloomHashes: this.#bloomHashes,
              order,
            }
          : null;
      let scratch: Int32Array = NO_ENTRIES;
      for (let slot = 0; slot < keyCount;) {
        yield* checkpoint(check);
        const limit = Math.min(slot + 128, keyCount);
        slot = this.#emitSingles(order, slot, limit, bound, visit, keep, singles);
        if (slot === limit) continue;
        const keySlot = order === null ? slot++ : (order[slot++] ?? 0);
        const start = starts[keySlot] ?? 0;
        const end = ends[keySlot] ?? 0;
        const head = this.#head[keySlot] ?? -1;
        if (scratch.length === 0) {
          this.#reserve(16 * 4, workspace);
          workspaceBytes += 16 * 4;
          scratch = new Int32Array(16);
        }
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
            const bytes = scratch.byteLength * 2;
            this.#reserve(bytes, workspace);
            workspaceBytes += bytes;
            const prior = scratch;
            const grown: Int32Array = new Int32Array(scratch.length * 2);
            grown.set(prior);
            scratch = grown;
            this.#free(prior.byteLength, workspace);
            workspaceBytes -= prior.byteLength;
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
            this.keyHash(keySlot),
            this.keyBloomHash(keySlot),
            visit,
          );
        }
      }
    } finally {
      this.#free(workspaceBytes, workspace);
      this.#finishTraversal();
    }
  }

  /** Re-read ownership after suspension; release may have been requested by another task. */
  #finishTraversal(): void {
    this.#activeTraversals--;
    if (this.#activeTraversals === 0 && this.#releaseRequested) this.#releaseRetained();
  }

  /** Ordinary bounded loop: row emission stays outside the generator's suspension state. */
  #emitSingles(
    order: Int32Array | null,
    from: number,
    to: number,
    bound: number,
    visit: DeltaVisitor,
    keep: KeepEntry | null,
    singles: SingleEntries | null,
  ): number {
    if (singles !== null && visit.singles !== undefined)
      return visit.singles(singles, from, to, bound, keep);
    const arena = this.#arena,
      starts = this.#keyStart,
      ends = this.#keyEnd,
      heads = this.#head,
      next = this.#next,
      seqs = this.#seqs,
      rowIds = this.#rowIds,
      tombstones = this.#tombstones;
    for (let at = from; at < to; at++) {
      const slot = order === null ? at : (order[at] ?? 0),
        head = heads[slot] ?? -1;
      if (head < 0) continue;
      if ((next[head] ?? -1) >= 0) return at;
      if ((seqs[head] ?? 0) > bound) continue;
      const rowId = rowIds[head] ?? 0,
        tombstone = tombstones[head] === 1;
      if (keep === null || keep(rowId, tombstone))
        visit(
          arena,
          starts[slot] ?? 0,
          ends[slot] ?? 0,
          rowId,
          tombstone,
          this.keyHash(slot),
          this.keyBloomHash(slot),
          true,
        );
    }
    return to;
  }

  /**
   * The smallest and largest key this delta has ever held (key slots, cached until a key is
   * added; popped entries keep their keys, so the range may be wider: sound for skipping), or
   * null when it has no key or cannot fund the optional copies. A nonempty delta with no
   * funded range cannot be pruned. Frozen deltas compute their funded range once.
   * Returned bytes are borrowed until the next cache attempt or `releaseCharge`. Current
   * callers compare them synchronously; a caller retaining a range must own its copy.
   */
  keyRange(): { readonly min: Uint8Array; readonly max: Uint8Array } | null {
    if (this.keyCount === 0) return null;
    if (this.#range !== null && this.#rangeKeys === this.keyCount) return this.#range;
    // Decline before walking keys when copies cannot fit. Otherwise repeated pressure would
    // turn this optional cache into a full key walk on every uniqueness check.
    const reserved = 2 * this.#maxKeyBytes;
    try {
      this.#reserve(reserved);
    } catch (error) {
      if (!(error instanceof MemtableMemoryError)) throw error;
      const prior = this.#rangeBytes;
      this.#range = null;
      this.#rangeKeys = 0;
      this.#rangeBytes = 0;
      this.#free(prior);
      return null;
    }
    const arena = this.#arena;
    const starts = this.#keyStart;
    const ends = this.#keyEnd;
    let min = 0;
    let max = 0;
    if (this.#ascendingKeys) max = this.keyCount - 1;
    for (let slot = 1; !this.#ascendingKeys && slot < this.keyCount; slot++) {
      const start = starts[slot] ?? 0;
      const end = ends[slot] ?? 0;
      if (compareBytes(arena, start, end, arena, starts[min] ?? 0, ends[min] ?? 0) < 0) min = slot;
      if (compareBytes(arena, start, end, arena, starts[max] ?? 0, ends[max] ?? 0) > 0) max = slot;
    }
    const minKey = this.keyBytes(min),
      maxKey = this.keyBytes(max);
    const bytes = minKey.byteLength + maxKey.byteLength;
    let range: { readonly min: Uint8Array; readonly max: Uint8Array };
    try {
      range = { min: new Uint8Array(minKey.byteLength), max: new Uint8Array(maxKey.byteLength) };
      range.min.set(minKey);
      range.max.set(maxKey);
    } catch (error) {
      this.#free(reserved);
      throw error;
    }
    const prior = this.#rangeBytes;
    this.#range = range;
    this.#rangeKeys = this.keyCount;
    this.#rangeBytes = bytes;
    this.#free(prior + reserved - bytes);
    return range;
  }

  #range: { readonly min: Uint8Array; readonly max: Uint8Array } | null = null;
  #rangeKeys = 0;
  #rangeBytes = 0;

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
  hash: number,
  bloomHash: number,
  visit: DeltaVisitor,
): void {
  let at = reverse ? count - 1 - from : from;
  const step = reverse ? -1 : 1;
  for (let k = from; k < to; k++, at += step) {
    const entry = entries[at] ?? 0;
    visit(arena, start, end, rowIds[entry] ?? 0, tombstones[entry] === 1, hash, bloomHash, false);
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
