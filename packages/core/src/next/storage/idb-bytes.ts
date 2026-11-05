/**
 * A byte space on IndexedDB (the backend's data, log and spill stores): records of at most
 * `IDB_RECORD_BYTES`, keyed by offset, kept disjoint, plus a companion index store (offset → length)
 * that is loaded into memory when the space is opened. Every overlap decision is made in memory,
 * so a write into free space is one `put` of the record and one of its index entry, and a read
 * `get`s exactly the records it needs.
 *
 * Why (increment 4, measured): finding the record before a write with a `prev` cursor makes
 * Firefox read that record's whole value, which took bulk writes from ~213 MB/s to ~3–10 MB/s; a
 * range delete and a second cursor cost Chromium about 3×. Records of 256 KiB were the fastest of
 * 32 KiB–1 MiB on both engines.
 *
 * ## Rules
 * - Operations run inside the caller's read-write transaction, in its request order, and update
 *   the in-memory index as they execute; `rollback` undoes those updates if the transaction does
 *   not commit, so memory always matches what is stored.
 * - A write first removes what it overlaps (deleting records inside its range, trimming the ones
 *   that straddle a bound: the only case that reads a value), then puts its records.
 * - Values are exact-size ArrayBuffers (never views, never Blobs). `write` stores a standalone
 *   buffer of one record as it is: callers pass a copy they will not touch again.
 */
import type { IdbDatabaseLike, IdbStoreLike, IdbTransactionLike } from "./idb-like.js";

/** Records are at most this long (writes are split). */
export const IDB_RECORD_BYTES = 256 * 1024;

interface StoredRecord {
  readonly offset: number;
  readonly bytes: ArrayBuffer;
}

interface IndexEntry {
  readonly offset: number;
  readonly length: number;
}

export class ByteStore {
  readonly name: string;
  readonly indexName: string;
  /** Record starts and ends, ascending (records are disjoint, so both orders agree). */
  #starts: number[] = [];
  #ends: number[] = [];
  /** Index changes of the transaction in progress, newest last. */
  #journal: Array<() => void> = [];

  private constructor(name: string) {
    this.name = name;
    this.indexName = `${name}-index`;
  }

  /** Loads the index of byte store `name` (one read of the small index store). */
  static async load(db: IdbDatabaseLike, name: string): Promise<ByteStore> {
    const store = new ByteStore(name);
    let entries: IndexEntry[] = [];
    await db.transaction([store.indexName], "readonly", "relaxed", (tx) => {
      tx.store(store.indexName).getAll({}, (values) => {
        entries = values as IndexEntry[];
      });
    });
    entries.sort((a, b) => a.offset - b.offset);
    for (const entry of entries) {
      store.#starts.push(entry.offset);
      store.#ends.push(entry.offset + entry.length);
    }
    return store;
  }

  /** The end of the last record (the space's length). */
  get end(): number {
    return this.#ends.at(-1) ?? 0;
  }

  get records(): number {
    return this.#starts.length;
  }

  /** Index of the first record ending after `start`. */
  #first(start: number): number {
    let low = 0;
    let high = this.#ends.length;
    while (low < high) {
      const middle = (low + high) >>> 1;
      if ((this.#ends[middle] ?? 0) <= start) low = middle + 1;
      else high = middle;
    }
    return low;
  }

  /** Records overlapping `[start, end)`, ascending. */
  #overlapping(start: number, end: number): Array<{ start: number; end: number }> {
    const out: Array<{ start: number; end: number }> = [];
    for (let index = this.#first(start); index < this.#starts.length; index++) {
      const recordStart = this.#starts[index] ?? 0;
      if (recordStart >= end) break;
      out.push({ start: recordStart, end: this.#ends[index] ?? 0 });
    }
    return out;
  }

  #insert(start: number, end: number): void {
    const index = this.#first(start);
    this.#starts.splice(index, 0, start);
    this.#ends.splice(index, 0, end);
    this.#journal.push(() => {
      this.#remove(start, false);
    });
  }

  #remove(start: number, journal = true): void {
    const index = this.#first(start);
    if (this.#starts[index] !== start) return;
    const end = this.#ends[index] ?? start;
    this.#starts.splice(index, 1);
    this.#ends.splice(index, 1);
    if (journal)
      this.#journal.push(() => {
        const at = this.#first(start);
        this.#starts.splice(at, 0, start);
        this.#ends.splice(at, 0, end);
      });
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

  #put(tx: IdbTransactionLike, offset: number, bytes: ArrayBuffer): void {
    tx.store(this.name).put(offset, { offset, bytes } satisfies StoredRecord);
    tx.store(this.indexName).put(offset, { offset, length: bytes.byteLength } satisfies IndexEntry);
    this.#insert(offset, offset + bytes.byteLength);
  }

  #delete(tx: IdbTransactionLike, offset: number): void {
    tx.store(this.name).delete(offset);
    tx.store(this.indexName).delete(offset);
    this.#remove(offset);
  }

  /** Removes `[start, end)`, trimming straddling records, then calls `next`. */
  clear(tx: IdbTransactionLike, start: number, end: number, next: () => void): void {
    const overlapping = end > start ? this.#overlapping(start, end) : [];
    const step = (index: number) => {
      const record = overlapping[index];
      if (record === undefined) {
        next();
        return;
      }
      if (record.start >= start && record.end <= end) {
        this.#delete(tx, record.start);
        step(index + 1);
        return;
      }
      // A straddling record: keep its parts outside the range (the only read on a write path).
      tx.store(this.name).get(record.start, (value) => {
        const bytes = (value as StoredRecord | undefined)?.bytes;
        this.#delete(tx, record.start);
        if (bytes !== undefined) {
          if (record.start < start)
            this.#put(tx, record.start, bytes.slice(0, start - record.start));
          if (record.end > end) this.#put(tx, end, bytes.slice(end - record.start));
        }
        step(index + 1);
      });
    };
    step(0);
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

  /** Fills `target` with the bytes at `offset` (gaps read as zeros), then calls `then`. */
  read(tx: IdbTransactionLike, offset: number, target: Uint8Array, then: () => void): void {
    target.fill(0);
    const end = offset + target.length;
    const overlapping = target.length > 0 ? this.#overlapping(offset, end) : [];
    if (overlapping.length === 0) {
      then();
      return;
    }
    let left = overlapping.length;
    const store: IdbStoreLike = tx.store(this.name);
    for (const record of overlapping)
      store.get(record.start, (value) => {
        const found = value as StoredRecord | undefined;
        if (found !== undefined) {
          const from = Math.max(offset, found.offset);
          const to = Math.min(end, found.offset + found.bytes.byteLength);
          if (to > from)
            target.set(new Uint8Array(found.bytes, from - found.offset, to - from), from - offset);
        }
        left -= 1;
        if (left === 0) then();
      });
  }

  /** Empties the space (and its index), then calls `next`. */
  reset(tx: IdbTransactionLike, next: () => void): void {
    tx.store(this.name).clear();
    tx.store(this.indexName).clear();
    const starts = this.#starts;
    const ends = this.#ends;
    this.#starts = [];
    this.#ends = [];
    this.#journal.push(() => {
      this.#starts = starts;
      this.#ends = ends;
    });
    next();
  }
}
