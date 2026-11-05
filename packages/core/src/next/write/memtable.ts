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
import { need } from "./bytes.js";

export const MEMTABLE_ROWS = 65_536;
/** Memtable bytes that freeze it early (wide rows). */
export const MEMTABLE_BYTES = 8 * 1024 * 1024;
/** Rows between byte measurements of a live memtable. */
const BYTES_STEP = 256;

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

  constructor(
    readonly tableId: number,
    readonly firstRowId: number,
    readonly schemaVersion: number,
    columns: readonly ColumnDef[],
    indexIds: readonly number[],
  ) {
    this.columns = columns;
    this.chunks = columns.map(() => []);
    this.deltas = new Map(indexIds.map((id) => [id, new KeyDelta()]));
  }

  get capacity(): number {
    return MEMTABLE_ROWS - this.rowCount;
  }

  #bytes = 0;
  #bytesAt = -1;

  /**
   * Bytes held by rows and deltas: measured at most every `BYTES_STEP` appended rows (and on
   * demand once frozen), so the per-insert cost is O(1).
   */
  get bytes(): number {
    if (this.#bytesAt < 0 || (this.state === "live" && this.rowCount - this.#bytesAt >= BYTES_STEP))
      this.measure();
    return this.#bytes;
  }

  /** Measures the bytes now (freeze, rollback). */
  measure(): number {
    let total = 0;
    for (const column of this.chunks) for (const chunk of column) total += chunk.bytes;
    for (const delta of this.deltas.values()) total += delta.bytes;
    this.#bytes = total;
    this.#bytesAt = this.rowCount;
    return total;
  }

  get full(): boolean {
    return this.rowCount >= MEMTABLE_ROWS || this.bytes >= MEMTABLE_BYTES;
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
          chunk = new ColumnChunk(column.type);
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
    this.#bytesAt = -1;
  }

  /** Deleted positions under `bound` within `[0, rows)`. */
  deletedUnder(bound: number, rows: number): number[] {
    const out: number[] = [];
    for (const [position, seq] of this.deletions)
      if (seq <= bound && position < rows) out.push(position);
    return out;
  }

  /** Frees rows and deltas once published (re-pointed readers read the group and runs). */
  release(): void {
    this.chunks = this.columns.map(() => []);
    for (const id of this.deltas.keys()) this.deltas.set(id, new KeyDelta());
  }
}
