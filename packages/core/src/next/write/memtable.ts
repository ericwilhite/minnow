/**
 * A memtable: one table's rows in memory, at most 65,536 of them (one locator segment), in row-id
 * order, plus the key-index delta of every index and the deletions of its own rows.
 *
 * ## Visibility (access/snapshot.ts)
 * Rows are appended in commit order: committed rows, then the open transaction's rows statement by
 * statement. `marks` record, after each append, `(rowCount, seq)` with `seq` non-decreasing, so the
 * rows a view sees are a prefix: `visibleRows(bound)` is one binary search. Commit restamps the
 * open transaction's marks to its LSN; rollback truncates.
 *
 * ## Lifecycle
 * `live` (the table's newest memtable takes appends) → `frozen` (full; awaits its flush) →
 * `published` (its flush is in a durable root). Publishing frees the rows and deltas: readers still
 * on it are re-pointed (ruling R16.3) to the published group and runs, which `published` names,
 * protected by an extent-space pin the flush took during publication.
 */
import type { ColumnDef } from "../access/catalog.js";
import type { SpacePin } from "../access/space.js";
import type { GroupRef, GroupRoot, RunRef } from "../format/manifest.js";
import type { Vector } from "../vector/vector.js";
import { ColumnChunk, MEMORY_CHUNK_ROWS } from "./columns.js";
import { KeyDelta } from "./key-hash.js";
import { UNCHARGED, type Charge } from "./bytes.js";
import { need } from "./bytes.js";

export const MEMTABLE_ROWS = 65_536;
/** Memtable bytes that freeze it early (wide rows). */
export const MEMTABLE_BYTES = 8 * 1024 * 1024;

/** A memtable's memory: charged to the write path's grant before allocation, counted here. */
class Counter implements Charge {
  bytes = 0;
  constructor(readonly grant: Charge) {}
  allocate(bytes: number): boolean {
    if (!this.grant.allocate(bytes)) return false;
    this.bytes += bytes;
    return true;
  }
  free(bytes: number): void {
    this.grant.free(bytes);
    this.bytes -= bytes;
  }
}

export interface Published {
  readonly group: GroupRef;
  readonly groupRoot: GroupRoot;
  /** The run each index's delta became (absent: the delta was empty). */
  readonly runs: ReadonlyMap<number, RunRef>;
  /** Held for re-pointed readers; released when no reader can still reach this memtable. */
  readonly pin: SpacePin;
}

export class Memtable {
  readonly columns: readonly ColumnDef[];
  /** `chunks[c][k]`: column c, rows `k × MEMORY_CHUNK_ROWS …`. */
  chunks: ColumnChunk[][];
  rowCount = 0;
  readonly deltas: Map<number, KeyDelta>;
  /** Deleted positions of this memtable's rows → seq of the delete. */
  readonly deletions = new Map<number, number>();
  #markRows: number[] = [];
  #markSeqs: number[] = [];
  state: "live" | "frozen" | "published" = "live";
  published: Published | null = null;
  readonly #counter: Counter;
  /** Readers borrowing each chunk index (memory segments: one chunk at a time each). */
  readonly #borrows = new Map<number, number>();
  /** Chunks released while borrowed: charged until their last borrow ends. */
  readonly #lent = new Map<number, ColumnChunk[]>();

  /** `charge` is the write path's grant: every array is charged before it is allocated. */
  constructor(
    readonly tableId: number,
    readonly firstRowId: number,
    readonly schemaVersion: number,
    columns: readonly ColumnDef[],
    indexIds: readonly number[],
    charge: Charge = UNCHARGED,
    /** Rows before the memtable is full (tests shrink it: `EngineInput.tuning`). */
    readonly limit = MEMTABLE_ROWS,
  ) {
    this.columns = columns;
    this.chunks = columns.map(() => []);
    this.#counter = new Counter(charge);
    this.deltas = new Map(indexIds.map((id) => [id, new KeyDelta(this.#counter)]));
  }

  /** A new key delta for an index created while this memtable is live. */
  addDelta(indexId: number): KeyDelta {
    const delta = new KeyDelta(this.#counter);
    this.deltas.set(indexId, delta);
    return delta;
  }

  /** Removes an index's delta (a rolled back CREATE INDEX), returning its memory. */
  removeDelta(indexId: number): void {
    this.deltas.get(indexId)?.releaseCharge();
    this.deltas.delete(indexId);
  }

  /** A reader borrows chunk `chunk` (and gives back `previous`, −1 for none). */
  borrow(chunk: number, previous: number): void {
    if (chunk === previous) return;
    if (previous >= 0) this.unborrow(previous);
    this.#borrows.set(chunk, (this.#borrows.get(chunk) ?? 0) + 1);
  }

  unborrow(chunk: number): void {
    const left = (this.#borrows.get(chunk) ?? 0) - 1;
    if (left > 0) {
      this.#borrows.set(chunk, left);
      return;
    }
    this.#borrows.delete(chunk);
    const lent = this.#lent.get(chunk);
    if (lent !== undefined) {
      for (const stored of lent) stored.releaseCharge();
      this.#lent.delete(chunk);
    }
  }

  /** Bytes of released chunks still borrowed by readers. */
  get borrowedBytes(): number {
    let total = 0;
    for (const chunks of this.#lent.values())
      for (const stored of chunks) total += stored.chargedBytes;
    return total;
  }

  get capacity(): number {
    return this.limit - this.rowCount;
  }

  /** Bytes charged for rows, deltas and borrowed released chunks (exact, O(1)). */
  get bytes(): number {
    return this.#counter.bytes;
  }

  /** Kept for callers that measured before freezing; the count is always exact now. */
  measure(): number {
    return this.#counter.bytes;
  }

  get full(): boolean {
    return this.rowCount >= this.limit || this.bytes >= MEMTABLE_BYTES;
  }

  /**
   * Appends rows of `columns` (one vector per table column): `rows[from … from + count)` or
   * physical rows `from …`. The caller has checked capacity. Positions `rowCount …` are theirs.
   */
  append(columns: readonly Vector[], rows: Uint16Array | null, from: number, count: number): void {
    let done = 0;
    while (done < count) {
      const chunkIndex = Math.floor(this.rowCount / MEMORY_CHUNK_ROWS);
      const room = MEMORY_CHUNK_ROWS - (this.rowCount % MEMORY_CHUNK_ROWS);
      const take = Math.min(room, count - done);
      this.columns.forEach((column, c) => {
        const list = need(this.chunks[c]);
        let chunk = list[chunkIndex];
        if (chunk === undefined) {
          chunk = new ColumnChunk(column.type, this.#counter);
          list.push(chunk);
        }
        const vector = columns[c];
        if (vector === undefined) throw new RangeError("A batch lacks a column");
        chunk.append(vector, rows, from + done, from + done + take);
      });
      this.rowCount += take;
      done += take;
    }
  }

  /** Records that rows `0 … rowCount − 1` carry seqs ≤ `seq` (after an append under `seq`). */
  mark(seq: number): void {
    const last = this.#markSeqs.length - 1;
    if (last >= 0 && this.#markSeqs[last] === seq) this.#markRows[last] = this.rowCount;
    else {
      this.#markRows.push(this.rowCount);
      this.#markSeqs.push(seq);
    }
  }

  /** Rows a view with `bound` sees: a prefix. */
  /**
   * The smallest seq bound under which the first `rows` rows are visible: the seq of the first
   * mark covering them (0 for none), or Infinity while some of them are not marked. Read from the
   * marks, never searched for over the seq space (pending seqs sit near 2^52, where the midpoint
   * of a search over safe integers is no longer exact).
   */
  seqCovering(rows: number): number {
    if (rows <= 0) return 0;
    const counts = this.#markRows;
    let lo = 0;
    let hi = counts.length;
    while (lo < hi) {
      const mid = (lo + hi) >>> 1;
      if ((counts[mid] ?? 0) >= rows) hi = mid;
      else lo = mid + 1;
    }
    return lo < counts.length ? (this.#markSeqs[lo] ?? Infinity) : Infinity;
  }

  visibleRows(bound: number): number {
    const seqs = this.#markSeqs;
    let lo = 0;
    let hi = seqs.length;
    while (lo < hi) {
      const mid = (lo + hi) >>> 1;
      if ((seqs[mid] ?? 0) <= bound) lo = mid + 1;
      else hi = mid;
    }
    return lo === 0 ? 0 : (this.#markRows[lo - 1] ?? 0);
  }

  /** Every mark with a seq ≥ `from` takes `seq` (commit restamps pending marks to the LSN). */
  restamp(from: number, seq: number): void {
    for (let k = this.#markSeqs.length - 1; k >= 0; k--) {
      if ((this.#markSeqs[k] ?? 0) < from) break;
      this.#markSeqs[k] = seq;
    }
    // Merge equal neighbours so marks stay short.
    while (this.#markSeqs.length >= 2) {
      const n = this.#markSeqs.length;
      if (this.#markSeqs[n - 1] !== this.#markSeqs[n - 2]) break;
      this.#markRows[n - 2] = this.#markRows[n - 1] ?? 0;
      this.#markRows.pop();
      this.#markSeqs.pop();
    }
  }

  /** Drops rows from `rowCount` on and marks with seq ≥ `fromSeq` (a rollback). */
  truncate(rowCount: number, fromSeq: number): void {
    while (
      this.#markSeqs.length > 0 &&
      (this.#markSeqs[this.#markSeqs.length - 1] ?? 0) >= fromSeq
    ) {
      this.#markSeqs.pop();
      this.#markRows.pop();
    }
    if (rowCount >= this.rowCount) return;
    const keepChunks = Math.ceil(rowCount / MEMORY_CHUNK_ROWS);
    for (const list of this.chunks) {
      list.length = Math.min(list.length, keepChunks);
      const last = list[keepChunks - 1];
      if (last !== undefined) last.truncate(rowCount - (keepChunks - 1) * MEMORY_CHUNK_ROWS);
    }
    this.rowCount = rowCount;
  }

  /** Deleted positions under `bound` within `[0, rows)`. */
  deletedUnder(bound: number, rows: number): number[] {
    const out: number[] = [];
    for (const [position, seq] of this.deletions)
      if (seq <= bound && position < rows) out.push(position);
    return out;
  }

  /**
   * Frees the rows once published: re-pointed readers read them from the group. The key deltas
   * stay (sequence-qualified, cancelled pairs intact) for readers older than the publication,
   * until `dropDeltas` (ruling 2).
   */
  release(): void {
    const chunkCount = this.chunks[0]?.length ?? 0;
    for (let chunk = 0; chunk < chunkCount; chunk++) {
      const stored = this.chunks.map((list) => need(list[chunk]));
      if (this.#borrows.has(chunk)) this.#lent.set(chunk, stored);
      else for (const column of stored) column.releaseCharge();
    }
    this.chunks = this.columns.map(() => []);
  }

  /** Frees the key deltas once no reader older than the publication remains. */
  dropDeltas(): void {
    for (const delta of this.deltas.values()) delta.releaseCharge();
    this.deltas.clear();
    this.deltasDropped = true;
  }

  /** Returns every charged byte (the memtable is gone: dropped table, closed write path). */
  releaseAll(): void {
    for (const list of this.chunks) for (const stored of list) stored.releaseCharge();
    for (const chunks of this.#lent.values()) for (const stored of chunks) stored.releaseCharge();
    this.#lent.clear();
    for (const delta of this.deltas.values()) delta.releaseCharge();
    this.chunks = this.columns.map(() => []);
    this.deltas.clear();
    this.deltasDropped = true;
  }

  deltasDropped = false;
}
