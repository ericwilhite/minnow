import type { StagedExtents } from "./staged-extents.js";
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
 *
 * ## Staged (a large transaction: format/replay.ts "Large transactions")
 * A frozen memtable of the open transaction's rows alone can be **staged**: its rows are written
 * as a group and its own index puts as runs (`staged`), a root lists them under the transaction's
 * staging id (`TableRoot.staged`), and the rows are freed (`published` names the group, so reads
 * re-point as above). The marks stay: the rows are still pending, visible by the same prefix rule,
 * and a staged run's entries are visible exactly when their row is (`visibleRowIdEnd`). Every
 * other delta entry (tombstones of older rows) stays in `deltas`, seq-qualified as before. A
 * rollback truncates the marks only (`rowCount` drops; `storedRows` keeps the group's count). At
 * commit the marks are restamped like any memtable's; a later flush folds the staged group and
 * runs into the table's directory instead of writing them again.
 * An emptied staging can still hold delta entries from before the rolled-back rows (including
 * committed tombstones). It stays frozen with an empty visible prefix until a rowless flush
 * publishes those entries; rollback must not remove that unrelated state.
 */
import type { ColumnDef } from "../access/catalog.js";
import type { SpacePin } from "../access/space.js";
import type { GroupRef, GroupRoot, RunRef } from "../format/manifest.js";
import type { Vector } from "../vector/vector.js";
import { ColumnChunk, MEMORY_CHUNK_ROWS } from "./columns.js";
import { layoutOf } from "../types.js";
import { KeyDelta } from "./key-hash.js";
import { charged, UNCHARGED, type Charge } from "./bytes.js";
import { need } from "./bytes.js";

export const MEMTABLE_ROWS = 65_536;
/** Memtable bytes that freeze it early (wide rows). */
export const MEMTABLE_BYTES = 8 * 1024 * 1024;

/** A memtable's memory: charged to the write path's grant before allocation, counted here. */
class Counter implements Charge {
  bytes = 0;
  constructor(
    readonly grant: Charge,
    readonly pool?: ArenaPool,
  ) {
    if (pool !== undefined && pool.charge !== grant)
      throw new TypeError("Arena pool belongs to another allocator");
  }
  reuseBytes(bytes: number): Uint8Array | undefined {
    const buffer = this.pool?.take(bytes);
    if (buffer !== undefined) this.bytes += buffer.byteLength;
    return buffer;
  }
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

/** Exact-capacity byte arenas. Active chunks and their source slots keep their normal lifetime. */
export class ArenaPool {
  readonly #entries: Map<number, Uint8Array[]>;
  readonly #oldest: Set<Uint8Array>;
  #bytes = 0;
  // Buffer/view headers and ownership entries, plus the bounded power-of-two buckets.
  static readonly overhead = 512 * 256 + 4096;
  constructor(
    readonly charge: Charge,
    readonly limit: number,
  ) {
    if (!charge.allocate(ArenaPool.overhead)) throw new Error("Missing arena-pool minimum");
    this.#entries = new Map();
    this.#oldest = new Set();
  }
  get bytes(): number {
    return this.#bytes;
  }
  take(bytes: number): Uint8Array | undefined {
    const entries = this.#entries.get(bytes);
    const buffer = entries?.pop();
    if (buffer === undefined) return;
    if (entries?.length === 0) this.#entries.delete(bytes);
    this.#oldest.delete(buffer);
    this.#bytes -= bytes;
    return buffer;
  }
  put(buffer: Uint8Array): boolean {
    const bytes = buffer.byteLength;
    if (
      bytes < 4096 ||
      bytes > this.limit ||
      (bytes & (bytes - 1)) !== 0 ||
      buffer.byteOffset !== 0 ||
      bytes !== buffer.buffer.byteLength
    )
      return false;
    if (this.#oldest.has(buffer)) throw new Error("Arena returned twice");
    while (this.#bytes + bytes > this.limit || this.#oldest.size >= 512) {
      const oldest = this.#oldest.values().next().value;
      if (oldest === undefined) break;
      this.#remove(oldest);
      this.charge.free(oldest.byteLength);
    }
    let entries = this.#entries.get(bytes);
    if (entries === undefined) this.#entries.set(bytes, (entries = []));
    entries.push(buffer);
    this.#oldest.add(buffer);
    this.#bytes += bytes;
    return true;
  }
  #remove(buffer: Uint8Array): void {
    const entries = this.#entries.get(buffer.byteLength);
    const at = entries?.indexOf(buffer) ?? -1;
    if (at >= 0) entries?.splice(at, 1);
    if (entries?.length === 0) this.#entries.delete(buffer.byteLength);
    this.#oldest.delete(buffer);
    this.#bytes -= buffer.byteLength;
  }
  clear(): void {
    for (const buffer of this.#oldest) this.charge.free(buffer.byteLength);
    this.#entries.clear();
    this.#oldest.clear();
    this.#bytes = 0;
  }
  close(): void {
    this.clear();
    this.charge.free(ArenaPool.overhead);
  }
}

/**
 * A group root read only when first needed (review B3-p2c-1 #2): a staged group recovered at open
 * names its root but does not read it, so an open walks no group root (R25).
 */
export class LazyGroupRoot {
  #value: GroupRoot | null = null;
  #loading: Promise<GroupRoot> | null = null;

  constructor(readonly read: () => Promise<GroupRoot>) {}

  get(): GroupRoot | Promise<GroupRoot> {
    if (this.#value !== null) return this.#value;
    this.#loading ??= this.read().then(
      (root) => (this.#value = root),
      (error: unknown) => {
        this.#loading = null; // a failed read is tried again by the next reader
        throw error;
      },
    );
    return this.#loading;
  }
}

/** A group root, decoded or still to be read. */
export type GroupRootRef = GroupRoot | LazyGroupRoot;

/** The decoded root of `ref`, reading it the first time when it is lazy. */
export function groupRootOf(ref: GroupRootRef): GroupRoot | Promise<GroupRoot> {
  return ref instanceof LazyGroupRoot ? ref.get() : ref;
}

/** A staged memtable's durable parts (rows and its own index puts). */
export interface StagedPart {
  /** The staging id the root lists them under. */
  readonly stagingId: number;
  readonly group: GroupRef;
  readonly groupRoot: GroupRootRef;
  /** Runs of the memtable's own puts per index, oldest first (replay may hold several). */
  readonly runs: ReadonlyMap<number, readonly RunRef[]>;
  /** Held while the staged group can be read through this memtable. */
  readonly pin: SpacePin;
  /** Every extent written for it (group, runs, tail bitmap): retired if the staging is dropped. */
  readonly extents: StagedExtents;
}

export interface Published {
  readonly group: GroupRef;
  readonly groupRoot: GroupRootRef;
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
  state: "live" | "frozen" | "staged" | "published" = "live";
  published: Published | null = null;
  /** Set once staged (see "Staged"); kept after a fold for its runs. */
  staged: StagedPart | null = null;
  /** Rows the staged group stores (≥ `rowCount`: a rollback truncates the marks only). */
  storedRows = 0;
  readonly #counter: Counter;
  #ownedColumnBytes = 0;
  /**
   * Sizing summaries for one-row reads (page/read-bounds.ts): per varlen column and chunk, the
   * largest and the summed value bytes (write/memory-output.ts memoryValueBytes) of every row ever
   * appended to that chunk, at `[column × chunks + chunk]`. Appends only raise them. A rollback that
   * keeps a chunk keeps them: its dropped rows were appended, so the summary still covers every row
   * the chunk holds. Only a dropped chunk resets them: a rollback past it, or publication
   * (`release`), which drops every chunk and frees the summaries with the rows, so re-pointed
   * readers size row by row. Rows that did not arrive through `append` (a recovered staged group)
   * leave the memtable without summaries (`valueSummary` answers null).
   */
  /** Column → summary index, maxima and sums; allocated (and charged) with the first row. */
  #summary: { columns: Int32Array; max: Float64Array; sum: Float64Array } | null = null;
  readonly #summaryChunks: number;
  readonly #summarySlots: number;
  #summaryBytes = 0;
  /** Rows `0 … #appendedRows − 1` all arrived through `append` (or none did, when it is 0). */
  #appendedRows = 0;
  #summarized = true;
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
    readonly pool?: ArenaPool,
    /** Recovery memoization can retire before this memtable or its readers do. */
    copyColumns = false,
    readonly pressureChanged?: () => void,
  ) {
    this.#counter = new Counter(charge, pool);
    if (copyColumns) {
      const bytes = 64 + columns.length * 8;
      charged(this.#counter, bytes);
      this.#ownedColumnBytes = bytes;
      this.columns = [...columns];
    } else this.columns = columns;
    this.chunks = this.columns.map(() => []);
    let varlen = 0;
    for (const column of this.columns) if (layoutOf(column.type) === "varlen") varlen++;
    this.#summaryChunks = Math.ceil(limit / MEMORY_CHUNK_ROWS);
    this.#summarySlots = varlen * this.#summaryChunks;
    // Nothing is allocated for summaries until the first row: an empty memtable allocates and
    // charges nothing for them.
    this.deltas = new Map(indexIds.map((id) => [id, new KeyDelta(this.#counter)]));
  }

  /**
   * The sizing summary of stored column `column` (index into `columns`) in chunk `chunk`: an upper
   * bound on the value bytes of any one row (`max`) and of all its rows together (`sum`), or null
   * when this memtable has none (rows that did not arrive through `append`). Fixed-width columns
   * have no value bytes.
   */
  valueSummary(column: number, chunk: number): { max: number; sum: number } | null {
    if (!this.#summarized || this.#appendedRows < this.rowCount) return null;
    if (column < 0 || column >= this.columns.length || chunk < 0 || chunk >= this.#summaryChunks)
      return null;
    if (this.#summarySlots === 0) return { max: 0, sum: 0 };
    const summary = this.#summary;
    if (summary === null) return null;
    const at = summary.columns[column] ?? -1;
    if (at < 0) return { max: 0, sum: 0 };
    const slot = at * this.#summaryChunks + chunk;
    return { max: summary.max[slot] ?? 0, sum: summary.sum[slot] ?? 0 };
  }

  #summarize(c: number, chunkIndex: number, chunk: ColumnChunk, from: number, to: number): void {
    const summary = this.#summary;
    const at = summary?.columns[c] ?? -1;
    if (summary === null || at < 0) return;
    const offsets = need(chunk.offsets);
    const slot = at * this.#summaryChunks + chunkIndex;
    let max = summary.max[slot] ?? 0,
      sum = summary.sum[slot] ?? 0;
    for (let k = from; k < to; k++) {
      const bytes = (offsets[k + 1] ?? 0) - (offsets[k] ?? 0);
      if (bytes > max) max = bytes;
      sum += bytes;
    }
    summary.max[slot] = max;
    summary.sum[slot] = sum;
  }

  /** A dropped chunk's rows are gone: its summaries start again with its next append. */
  #resetSummaries(chunkIndex: number): void {
    const summary = this.#summary;
    if (summary === null) return;
    for (let at = 0; at * this.#summaryChunks < summary.max.length; at++) {
      summary.max[at * this.#summaryChunks + chunkIndex] = 0;
      summary.sum[at * this.#summaryChunks + chunkIndex] = 0;
    }
  }

  #releaseChunk(chunk: ColumnChunk): void {
    const arena = chunk.arena;
    if (arena !== null && this.pool?.put(arena)) {
      this.#counter.bytes -= arena.byteLength;
      chunk.chargedBytes -= arena.byteLength;
      chunk.arena = null;
    }
    chunk.releaseCharge();
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
      for (const stored of lent) this.#releaseChunk(stored);
      this.#lent.delete(chunk);
      this.pressureChanged?.();
    }
  }

  /** Rollback can reuse an index while an older generation there is still borrowed. */
  #holdBorrowed(chunk: number, stored: ColumnChunk[]): void {
    const held = this.#lent.get(chunk);
    if (held === undefined) this.#lent.set(chunk, stored);
    else for (const column of stored) held.push(column);
  }

  /** Bytes of released chunks still borrowed by readers. */
  get borrowedBytes(): number {
    let total = 0;
    for (const chunks of this.#lent.values())
      for (const stored of chunks) total += stored.chargedBytes;
    return total;
  }

  /** One past the last row id this memtable holds or held (staged rows keep their ids). */
  get rowIdEnd(): number {
    return this.firstRowId + Math.max(this.rowCount, this.storedRows);
  }

  /**
   * One past the last row id a view with `bound` sees here: a staged run's entry is visible iff
   * its row id is below it (runs carry no seq). Infinity when every row is visible.
   */
  visibleRowIdEnd(bound: number): number {
    const visible = this.visibleRows(bound);
    return visible >= this.rowCount && this.storedRows <= this.rowCount
      ? Infinity
      : this.firstRowId + visible;
  }

  /** Replaces the key deltas (staging keeps only entries that are not its own puts). */
  replaceDeltas(deltas: ReadonlyMap<number, KeyDelta>): void {
    for (const delta of this.deltas.values()) delta.releaseCharge();
    this.deltas.clear();
    for (const [indexId, delta] of deltas) this.deltas.set(indexId, delta);
  }

  /** A key delta charged to this memtable (staging's residual deltas). */
  newDelta(): KeyDelta {
    return new KeyDelta(this.#counter);
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
    // Rows placed without `append` (recovery sets `rowCount`) have no summaries.
    if (this.#appendedRows !== this.rowCount) this.#summarized = false;
    if (this.#summarized && this.#summary === null && this.#summarySlots > 0 && count > 0) {
      // The column mapping and both buffers, charged before any of them exists.
      const bytes =
        this.columns.length * Int32Array.BYTES_PER_ELEMENT +
        2 * this.#summarySlots * Float64Array.BYTES_PER_ELEMENT;
      charged(this.#counter, bytes);
      this.#summaryBytes = bytes;
      const columns = new Int32Array(this.columns.length).fill(-1);
      let varlen = 0;
      this.columns.forEach((column, c) => {
        if (layoutOf(column.type) === "varlen") columns[c] = varlen++;
      });
      this.#summary = {
        columns,
        max: new Float64Array(this.#summarySlots),
        sum: new Float64Array(this.#summarySlots),
      };
    }
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
        const before = chunk.length;
        chunk.append(vector, rows, from + done, from + done + take);
        this.#summarize(c, chunkIndex, chunk, before, chunk.length);
      });
      this.rowCount += take;
      this.#appendedRows = this.rowCount;
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
    // Whole chunks past the kept rows return their memory (borrowed ones once their borrow ends).
    const chunkCount = this.chunks[0]?.length ?? 0;
    for (let chunk = keepChunks; chunk < chunkCount; chunk++) {
      const stored = this.chunks.map((list) => need(list[chunk]));
      if (this.#borrows.has(chunk)) this.#holdBorrowed(chunk, stored);
      else for (const column of stored) this.#releaseChunk(column);
      this.#resetSummaries(chunk);
    }
    for (const list of this.chunks) {
      list.length = Math.min(list.length, keepChunks);
      const last = list[keepChunks - 1];
      if (last !== undefined) last.truncate(rowCount - (keepChunks - 1) * MEMORY_CHUNK_ROWS);
    }
    this.rowCount = rowCount;
    if (this.#appendedRows > rowCount) this.#appendedRows = rowCount;
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
      if (this.#borrows.has(chunk)) this.#holdBorrowed(chunk, stored);
      else for (const column of stored) this.#releaseChunk(column);
    }
    this.chunks = this.columns.map(() => []);
    this.#dropSummaries();
  }

  /** Every chunk is gone (publication or teardown): so are its summaries and their charge. */
  #dropSummaries(): void {
    this.#summarized = false;
    this.#summary = null;
    if (this.#summaryBytes > 0) {
      this.#counter.free(this.#summaryBytes);
      this.#summaryBytes = 0;
    }
  }

  /** Frees the key deltas once no reader older than the publication remains. */
  dropDeltas(): void {
    for (const delta of this.deltas.values()) delta.releaseCharge();
    this.deltas.clear();
    this.deltasDropped = true;
  }

  /** Returns every charged byte (the memtable is gone: dropped table, closed write path). */
  releaseAll(): void {
    this.staged?.pin.release(); // idempotent
    for (const list of this.chunks) for (const stored of list) stored.releaseCharge();
    for (const chunks of this.#lent.values()) for (const stored of chunks) stored.releaseCharge();
    this.#lent.clear();
    for (const delta of this.deltas.values()) delta.releaseCharge();
    this.chunks = this.columns.map(() => []);
    this.deltas.clear();
    this.deltasDropped = true;
    if (this.#ownedColumnBytes > 0) {
      this.#counter.free(this.#ownedColumnBytes);
      this.#ownedColumnBytes = 0;
    }
    this.#dropSummaries();
  }

  deltasDropped = false;
}
