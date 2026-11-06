/**
 * A byte space on IndexedDB (the backend's data, log and spill stores): records of at most
 * `IDB_RECORD_BYTES`, keyed by offset, kept disjoint, plus a companion index store
 * (offset → `{ offset, length }`). No path reads a record value to find records: values are up to
 * 256 KiB, and a Firefox cursor over them read each one (bulk writes fell to ~3–10 MB/s); index
 * values are 16 bytes.
 *
 * ## Who trusts what (review B1-p2d findings 1 and 2)
 * - **Writes** decide overlaps from the in-memory index (`IntervalIndex`). Only the owning session
 *   mutates it, inside its transactions, which run one at a time in call order (indexeddb.ts); a
 *   transaction that does not commit has its changes undone (`rollback`) before the next runs.
 * - **Reads never use the in-memory index.** A read selects its records from the persisted index
 *   inside its own transaction, so nothing that commits meanwhile (a release, another owner's
 *   split) can change the selection under it, and checks each record against its entry: a missing
 *   record, or one whose length differs from its entry, is `StoreCorruptionError`, never zeros.
 *   Gaps that no entry covers were never written (or were released) and read as zeros.
 * - **Open validates** the persisted index (safe integers, `0 < length ≤ IDB_RECORD_BYTES`, key
 *   equal to the entry's offset, sorted, no overlap) and that the record store's keys are exactly
 *   the index's: an orphan record or an entry without its record is `StoreCorruptionError`. It
 *   reads keys and 16-byte entries, never a record.
 *
 * ## Cost (finding 4)
 * The in-memory index is blocks of at most `INDEX_BLOCK` entries under a directory (the block
 * array, searched by each block's last end): insert and delete are O(log n + block), and removing
 * k consecutive entries is O(k + n / block). Its estimated size (`IntervalIndex.bytes`) is charged
 * to an optional memory registration in `CHARGE_STEP` granules; what the registration refuses is
 * counted as `overdraft` (the index must exist) and charged again as it shrinks or the registration
 * frees up.
 *
 * ## Clearing (finding 3)
 * `clear` deletes every record fully inside its range with one range delete per store, then trims
 * the at most two records straddling its ends (the only record reads on a write path): no step,
 * recursion or callback per record.
 */
import { StoreCorruptionError } from "./contract.js";
import type { CacheMemory } from "./extents.js";
import type { IdbDatabaseLike, IdbTransactionLike } from "./idb-like.js";

/** Records are at most this long (writes are split). */
export const IDB_RECORD_BYTES = 256 * 1024;
/** Index entries per block. */
export const INDEX_BLOCK = 512;
/**
 * Estimated bytes per index entry: a start and an end as doubles, plus V8's overhead, measured at
 * about 24 MiB for a million entries (scripts/idb-index-bench.mts).
 */
export const INDEX_ENTRY_BYTES = 24;
/** Estimated bytes per index block beyond its entries. */
export const INDEX_BLOCK_BYTES = 160;
/** The memory registration is charged and released in steps of this size. */
export const CHARGE_STEP = 64 * 1024;

interface StoredRecord {
  readonly offset: number;
  readonly bytes: ArrayBuffer;
}

interface IndexEntry {
  readonly offset: number;
  readonly length: number;
}

interface Block {
  starts: number[];
  ends: number[];
}

/** An entry of the in-memory index. */
export interface Interval {
  readonly start: number;
  readonly end: number;
}

const corrupt = (what: string) => new StoreCorruptionError(`IndexedDB byte space: ${what}`);

function isEntry(value: unknown): value is IndexEntry {
  if (typeof value !== "object" || value === null) return false;
  const { offset, length } = value as Record<string, unknown>;
  return (
    typeof offset === "number" &&
    typeof length === "number" &&
    Number.isSafeInteger(offset) &&
    Number.isSafeInteger(length) &&
    offset >= 0 &&
    length > 0 &&
    length <= IDB_RECORD_BYTES &&
    Number.isSafeInteger(offset + length)
  );
}

/** The first position in sorted `values` whose value is at least (or, `after`, above) `x`. */
function bound(values: readonly number[], x: number, after: boolean): number {
  let low = 0;
  let high = values.length;
  while (low < high) {
    const middle = (low + high) >>> 1;
    const value = values[middle] ?? 0;
    if (value < x || (after && value === x)) low = middle + 1;
    else high = middle;
  }
  return low;
}

/** Sorted, disjoint intervals in blocks (see "Cost"). */
export class IntervalIndex {
  #blocks: Block[] = [];
  #count = 0;
  readonly #memory: CacheMemory | null;
  /** Bytes the registration accepted, and bytes it refused (granules). */
  #charged = 0;
  #overdraft = 0;

  constructor(memory: CacheMemory | null = null) {
    this.#memory = memory;
  }

  /** Builds from validated entries sorted by offset. */
  static from(entries: readonly IndexEntry[], memory: CacheMemory | null): IntervalIndex {
    const index = new IntervalIndex(memory);
    // Exact-size arrays (`map`), not grown ones: no spare capacity resident per block.
    for (let at = 0; at < entries.length; at += INDEX_BLOCK) {
      const slice = entries.slice(at, at + INDEX_BLOCK);
      index.#blocks.push({
        starts: slice.map((entry) => entry.offset),
        ends: slice.map((entry) => entry.offset + entry.length),
      });
    }
    index.#count = entries.length;
    index.#settle();
    return index;
  }

  get count(): number {
    return this.#count;
  }

  get blocks(): number {
    return this.#blocks.length;
  }

  get end(): number {
    return this.#blocks.at(-1)?.ends.at(-1) ?? 0;
  }

  /** The estimated resident size. */
  get bytes(): number {
    return this.#count * INDEX_ENTRY_BYTES + this.#blocks.length * INDEX_BLOCK_BYTES;
  }

  /** Bytes charged to the memory registration, and bytes it refused. */
  get charge(): { readonly charged: number; readonly overdraft: number } {
    return { charged: this.#charged, overdraft: this.#overdraft };
  }

  /** Brings the charge to `bytes` rounded up to a granule. */
  #settle(): void {
    const memory = this.#memory;
    if (memory === null) return;
    const wanted = Math.ceil(this.bytes / CHARGE_STEP) * CHARGE_STEP;
    if (this.#charged > wanted) {
      memory.release(this.#charged - wanted);
      this.#charged = wanted;
    }
    // Charge what is missing (retrying any overdraft: the registration may have room now).
    const missing = wanted - this.#charged;
    if (missing > 0 && memory.charge(missing)) this.#charged = wanted;
    this.#overdraft = wanted - this.#charged;
  }

  /** Releases every charge (the session is closing); the index is empty afterwards. */
  release(): void {
    this.#blocks = [];
    this.#count = 0;
    if (this.#charged > 0) this.#memory?.release(this.#charged);
    this.#charged = 0;
    this.#overdraft = 0;
  }

  /** The first block whose last end is above `x` (whose entries may end after `x`). */
  #blockEndingAfter(x: number): number {
    let low = 0;
    let high = this.#blocks.length;
    while (low < high) {
      const middle = (low + high) >>> 1;
      if ((this.#blocks[middle]?.ends.at(-1) ?? 0) <= x) low = middle + 1;
      else high = middle;
    }
    return low;
  }

  /** Entries overlapping `[start, end)`, ascending. */
  overlapping(start: number, end: number): Interval[] {
    const out: Interval[] = [];
    for (let b = this.#blockEndingAfter(start); b < this.#blocks.length; b++) {
      const block = this.#blocks[b];
      if (block === undefined) break;
      for (let at = bound(block.ends, start, true); at < block.starts.length; at++) {
        const entryStart = block.starts[at] ?? 0;
        if (entryStart >= end) return out;
        out.push({ start: entryStart, end: block.ends[at] ?? 0 });
      }
    }
    return out;
  }

  /** Inserts `[start, end)`, which overlaps no entry (the caller cleared it). */
  insert(start: number, end: number): void {
    let b = this.#blockEndingAfter(start);
    if (b === this.#blocks.length) {
      const last = this.#blocks.at(-1);
      if (last === undefined || last.starts.length >= INDEX_BLOCK)
        this.#blocks.push({ starts: [], ends: [] });
      b = this.#blocks.length - 1;
    }
    const block = this.#blocks[b];
    if (block === undefined) return;
    const at = bound(block.starts, start, false);
    block.starts.splice(at, 0, start);
    block.ends.splice(at, 0, end);
    this.#count += 1;
    if (block.starts.length > INDEX_BLOCK) {
      const half = block.starts.length >>> 1;
      this.#blocks.splice(b + 1, 0, {
        starts: block.starts.splice(half),
        ends: block.ends.splice(half),
      });
    }
    this.#settle();
  }

  /** Removes every entry starting in `[first, last]` and returns them (for undo). */
  removeStarts(first: number, last: number): Interval[] {
    const removed: Interval[] = [];
    const from = this.#blockEndingAfter(first);
    let b = from;
    for (; b < this.#blocks.length; b++) {
      const block = this.#blocks[b];
      if (block === undefined || (block.starts[0] ?? Infinity) > last) break;
      const low = bound(block.starts, first, false);
      const high = bound(block.starts, last, true);
      for (let at = low; at < high; at++)
        removed.push({ start: block.starts[at] ?? 0, end: block.ends[at] ?? 0 });
      const size = block.starts.length;
      block.starts.splice(low, high - low);
      block.ends.splice(low, high - low);
      if (high < size) {
        b += 1;
        break;
      }
    }
    // The visited blocks are [from, b); the emptied ones are a contiguous run among them. Drop it
    // in one splice, then merge small survivors into a neighbour so blocks stay well filled.
    let empty = from;
    while (empty < b && (this.#blocks[empty]?.starts.length ?? 0) > 0) empty += 1;
    let kept = empty;
    while (kept < b && this.#blocks[kept]?.starts.length === 0) kept += 1;
    this.#blocks.splice(empty, kept - empty);
    this.#merge(empty);
    this.#merge(empty - 1);
    this.#count -= removed.length;
    this.#settle();
    return removed;
  }

  /** Merges block `b` and its successor when one is under a quarter full and both fit in one. */
  #merge(b: number): void {
    const block = this.#blocks[b];
    const next = this.#blocks[b + 1];
    if (block === undefined || next === undefined) return;
    const small = Math.min(block.starts.length, next.starts.length) < INDEX_BLOCK / 4;
    if (!small || block.starts.length + next.starts.length > INDEX_BLOCK) return;
    for (let at = 0; at < next.starts.length; at++) {
      block.starts.push(next.starts[at] ?? 0);
      block.ends.push(next.ends[at] ?? 0);
    }
    this.#blocks.splice(b + 1, 1);
  }

  /** Checks the structure (tests): sorted, disjoint, blocks non-empty and at most `INDEX_BLOCK`. */
  check(): void {
    let previous = -1;
    let count = 0;
    for (const block of this.#blocks) {
      if (block.starts.length === 0 || block.starts.length > INDEX_BLOCK)
        throw new Error(`index block of ${String(block.starts.length)} entries`);
      for (let at = 0; at < block.starts.length; at++) {
        const start = block.starts[at] ?? 0;
        const end = block.ends[at] ?? 0;
        if (start < previous || end <= start) throw new Error(`index entry ${String(start)}`);
        previous = end;
        count += 1;
      }
    }
    if (count !== this.#count) throw new Error("index count");
  }
}

export class ByteStore {
  readonly name: string;
  readonly indexName: string;
  readonly #index: IntervalIndex;
  /** Index changes of the transaction in progress, newest last. */
  #journal: Array<() => void> = [];

  private constructor(name: string, index: IntervalIndex) {
    this.name = name;
    this.indexName = `${name}-index`;
    this.#index = index;
  }

  /**
   * Loads and validates the index of byte space `name` (finding 2): the index store's keys and
   * entries, and the record store's keys (keys only). Any disagreement is `StoreCorruptionError`.
   */
  static async load(
    db: IdbDatabaseLike,
    name: string,
    memory: CacheMemory | null = null,
  ): Promise<ByteStore> {
    const indexName = `${name}-index`;
    let entries: unknown[] = [];
    let indexKeys: unknown[] = [];
    let recordKeys: unknown[] = [];
    await db.transaction([name, indexName], "readonly", "relaxed", (tx) => {
      tx.store(indexName).getAll({}, (values) => {
        entries = values;
      });
      tx.store(indexName).getAllKeys({}, (keys) => {
        indexKeys = keys;
      });
      tx.store(name).getAllKeys({}, (keys) => {
        recordKeys = keys;
      });
    });
    if (entries.length !== indexKeys.length) throw corrupt(`${indexName} changed while loading`);
    let previousEnd = 0;
    for (let at = 0; at < entries.length; at++) {
      const entry = entries[at];
      if (!isEntry(entry))
        throw corrupt(`entry ${String(indexKeys[at])} of ${indexName} is malformed`);
      if (indexKeys[at] !== entry.offset)
        throw corrupt(`entry ${String(entry.offset)} of ${indexName} is filed under another key`);
      if (entry.offset < previousEnd)
        throw corrupt(`entries of ${indexName} overlap at ${String(entry.offset)}`);
      previousEnd = entry.offset + entry.length;
    }
    const valid = entries as IndexEntry[];
    for (let at = 0; at < Math.max(recordKeys.length, valid.length); at++) {
      const record = recordKeys[at];
      const entry = valid[at]?.offset;
      if (record === entry) continue;
      throw corrupt(
        entry === undefined || (typeof record === "number" && record < entry)
          ? `record ${String(record)} of ${name} has no index entry`
          : `index entry ${String(entry)} of ${name} has no record`,
      );
    }
    return new ByteStore(name, IntervalIndex.from(valid, memory));
  }

  /** The end of the last record (the space's length). */
  get end(): number {
    return this.#index.end;
  }

  get records(): number {
    return this.#index.count;
  }

  get index(): IntervalIndex {
    return this.#index;
  }

  /** The transaction committed: its index changes stay. */
  commit(): void {
    this.#journal = [];
  }

  /** The transaction did not commit: undo its index changes. */
  rollback(): void {
    for (const undo of this.#journal.reverse()) undo();
    this.#journal = [];
  }

  /** Returns the index's memory charge (the session is closing). */
  close(): void {
    this.#index.release();
  }

  #put(tx: IdbTransactionLike, offset: number, bytes: ArrayBuffer): void {
    tx.store(this.name).put(offset, { offset, bytes } satisfies StoredRecord);
    tx.store(this.indexName).put(offset, { offset, length: bytes.byteLength } satisfies IndexEntry);
    this.#index.insert(offset, offset + bytes.byteLength);
    this.#journal.push(() => {
      this.#index.removeStarts(offset, offset);
    });
  }

  #removeStarts(first: number, last: number): void {
    const removed = this.#index.removeStarts(first, last);
    if (removed.length > 0)
      this.#journal.push(() => {
        for (const entry of removed) this.#index.insert(entry.start, entry.end);
      });
  }

  /** Removes `[start, end)`, trimming straddling records, then calls `next` (see "Clearing"). */
  clear(tx: IdbTransactionLike, start: number, end: number, next: () => void): void {
    const overlapping = end > start ? this.#index.overlapping(start, end) : [];
    let first = 0;
    let last = overlapping.length - 1;
    const straddling: Interval[] = [];
    const head = overlapping[0];
    if (head !== undefined && head.start < start) {
      straddling.push(head);
      first = 1;
    }
    const tail = overlapping[last];
    if (tail !== undefined && last >= first && tail.end > end) {
      straddling.push(tail);
      last -= 1;
    }
    const lower = overlapping[first];
    const upper = overlapping[last];
    if (lower !== undefined && upper !== undefined && last >= first) {
      const range = { lower: lower.start, upper: upper.start };
      tx.store(this.name).deleteRange(range);
      tx.store(this.indexName).deleteRange(range);
      this.#removeStarts(lower.start, upper.start);
    }
    if (straddling.length === 0) {
      next();
      return;
    }
    // Each straddling record keeps its parts outside the range. Both gets are issued before
    // either record is rewritten, and requests run in issue order, so each sees its record whole.
    let left = straddling.length;
    for (const record of straddling)
      tx.store(this.name).get(record.start, (value) => {
        const bytes = (value as StoredRecord | undefined)?.bytes;
        if (!(bytes instanceof ArrayBuffer) || bytes.byteLength !== record.end - record.start)
          throw corrupt(`record ${String(record.start)} of ${this.name} disagrees with its entry`);
        tx.store(this.name).delete(record.start);
        tx.store(this.indexName).delete(record.start);
        this.#removeStarts(record.start, record.start);
        if (record.start < start) this.#put(tx, record.start, bytes.slice(0, start - record.start));
        if (record.end > end) this.#put(tx, end, bytes.slice(end - record.start));
        left -= 1;
        if (left === 0) next();
      });
  }

  /** Writes `bytes` at `offset` (replacing what it overlaps), then calls `next`. */
  write(tx: IdbTransactionLike, offset: number, bytes: Uint8Array, next: () => void): void {
    if (bytes.length === 0) {
      next();
      return;
    }
    this.clear(tx, offset, offset + bytes.length, () => {
      // A standalone buffer of one record is stored as it is; anything else is cut into records.
      const whole =
        bytes.length <= IDB_RECORD_BYTES &&
        bytes.byteOffset === 0 &&
        bytes.byteLength === bytes.buffer.byteLength;
      if (whole) this.#put(tx, offset, bytes.buffer as ArrayBuffer);
      else
        for (let done = 0; done < bytes.length; done += IDB_RECORD_BYTES)
          this.#put(tx, offset + done, bytes.slice(done, done + IDB_RECORD_BYTES).buffer);
      next();
    });
  }

  /**
   * Fills `target` with the bytes at `offset`, then calls `then` (see "Who trusts what"). `tx`'s
   * scope must hold both stores. A missing or mismatched record aborts with `StoreCorruptionError`.
   */
  read(tx: IdbTransactionLike, offset: number, target: Uint8Array, then: () => void): void {
    target.fill(0);
    if (target.length === 0) {
      then();
      return;
    }
    const end = offset + target.length;
    const index = tx.store(this.indexName);
    index.last({ upper: offset, upperOpen: true }, (before) => {
      index.getAll({ lower: offset, upper: end, upperOpen: true }, (inside) => {
        const entries: IndexEntry[] = [];
        let previousEnd = -1;
        for (const entry of before === undefined ? inside : [before, ...inside]) {
          if (!isEntry(entry) || entry.offset < previousEnd)
            throw corrupt(`${this.indexName} is malformed near ${String(offset)}`);
          previousEnd = entry.offset + entry.length;
          if (previousEnd > offset) entries.push(entry);
        }
        let left = entries.length;
        if (left === 0) {
          then();
          return;
        }
        const records = tx.store(this.name);
        for (const entry of entries)
          records.get(entry.offset, (value) => {
            const found = value as StoredRecord | undefined;
            if (!(found?.bytes instanceof ArrayBuffer) || found.bytes.byteLength !== entry.length)
              throw corrupt(
                `record ${String(entry.offset)} of ${this.name} is missing or disagrees with its entry`,
              );
            const from = Math.max(offset, entry.offset);
            const to = Math.min(end, entry.offset + entry.length);
            target.set(new Uint8Array(found.bytes, from - entry.offset, to - from), from - offset);
            left -= 1;
            if (left === 0) then();
          });
      });
    });
  }

  /** Empties the space (and its index), then calls `next`. */
  reset(tx: IdbTransactionLike, next: () => void): void {
    tx.store(this.name).clear();
    tx.store(this.indexName).clear();
    this.#removeStarts(0, Number.MAX_SAFE_INTEGER);
    next();
  }
}
