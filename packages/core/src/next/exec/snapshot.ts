/**
 * The executor's read port: what one query needs from the pinned snapshot it runs against.
 *
 * The write-path track (B3) owns snapshots, memtables, deletion state and the key index; the
 * executor cannot import `write/` (dependency rules), so it states the shape it consumes here and
 * B3's snapshot satisfies it (structurally, or through an adapter in `owner/`). Nothing in this
 * file is implemented by the executor.
 *
 * ## Lifetime
 * - The caller (the owner) pins the snapshot before `execute` and releases the pin only after the
 *   execution's `done` settles (executor.ts). The executor never calls `pin.release()`.
 * - Everything reachable from a `QuerySnapshot` is immutable while the pin is held: segment lists,
 *   row counts, statistics, live bitmaps, dictionaries and page bytes. Two reads of the same range
 *   return the same values.
 * - A read after the pin was released rejects with `SnapshotReleasedError` (exec/fetch.ts).
 *
 * ## What a statement sees
 * - The snapshot is taken at statement start. In a write transaction it includes the
 *   transaction's own earlier statements (as extra segments) and never the current statement's
 *   own writes, so `INSERT INTO t SELECT … FROM t` and `UPDATE t SET k = k + 1` read each row
 *   once (no Halloween problem).
 * - Deleted rows are already removed: every read returns a selection of live rows only.
 *
 * ## Memory
 * Every buffer a read fills is the caller's, allocated from the caller's grant. The snapshot's own
 * working memory for a read (encoded slabs, decode scratch) is charged to the grant passed in the
 * request and freed before the promise settles, except pages it keeps in the shared page cache,
 * which the governor accounts separately (memory/governor.ts rule 6). Dictionaries returned in a
 * `DictionaryVector` are cache-owned and immutable for the pin; the executor never charges them.
 *
 * ## Async
 * Reads are async (page I/O); everything else here is sync and cheap (in-memory metadata the pin
 * already holds). Directory metadata that is not resident yet is loaded by `openTable`.
 *
 * ## Errors
 * Storage corruption is the storage track's typed error, passed through unchanged. Cancellation:
 * a request's `signal` rejects the read with its reason between page reads; outputs are then
 * unspecified and nothing is retained.
 */
import type { Grant } from "../memory/governor.js";
import type { KeyBatch } from "../keys/key-encoding.js";
import type { VarlenBuilder } from "../vector/varlen.js";
import type { FlatVector, Selection, Vector } from "../vector/vector.js";
import type { RowFetcher, SnapshotPin } from "./fetch.js";

/**
 * Exact bounds in the column's stored encoding (int64 family as bigint, text as UTF-8 bytes,
 * possibly truncated with a conservative upper bound). Mirrors format/manifest.ts `StatValue`,
 * which `exec/` cannot import.
 */
export type BoundValue = boolean | number | bigint | Uint8Array;

/**
 * Statistics of one column over a segment or a page. Exact unless marked approximate. The
 * executor uses `min`/`max` to prune and to prove the int64 f64 fast path (vector/int64.ts
 * `provenSafe`), so they must be exact bounds of every live value or null.
 */
export interface ColumnBounds {
  readonly nullCount: number;
  readonly min: BoundValue | null;
  readonly max: BoundValue | null;
  /** Distinct entries of the segment's dictionary, when dictionary-encoded (exact). */
  readonly dictionarySize: number | null;
}

/** One page of one column: its row range inside the segment and its bounds. */
export interface PageSummary extends ColumnBounds {
  readonly firstRow: number;
  readonly rowCount: number;
}

export interface QuerySnapshot {
  readonly pin: SnapshotPin;
  /** The catalog version this snapshot reads; a plan lowered against another is stale. */
  readonly catalogVersion: number;
  /**
   * The table's view in this snapshot, loading its group directory if needed. Rejects with the
   * write path's unknown-table error if the table is absent (a stale plan, normally caught by
   * `catalogVersion` first).
   */
  openTable(tableId: number, signal?: AbortSignal): Promise<TableSnapshot>;
  /** Late materialization over this snapshot's locators (exec/fetch.ts). */
  readonly rows: RowFetcher;
}

export interface TableSnapshot {
  readonly tableId: number;
  readonly schemaVersion: number;
  /** Live rows in the snapshot (exact). */
  readonly liveRowCount: number;
  /**
   * The table's segments in row-id order: published groups, then frozen memtable views, then
   * this transaction's own staged rows. The index is the locator's segment (vector/locator.ts).
   */
  readonly segments: readonly SegmentSnapshot[];
  /** Probes an index by equality key (keyLookup, runtime filters on keys, upsert). */
  lookup(request: LookupRequest): Promise<LookupResult>;
}

export interface SegmentSnapshot {
  /** Row positions 0 … rowCount − 1 (≤ 65,536), including deleted ones. */
  readonly rowCount: number;
  readonly liveRowCount: number;
  /** Column bounds over the segment, or null when unknown (a memtable view may not track them). */
  bounds(columnId: number): ColumnBounds | null;
  /**
   * Page summaries of a column, in row order, covering 0 … rowCount − 1 without gaps; null when
   * the segment has no page structure (memtable views). Resident once `openTable` resolved.
   */
  pages(columnId: number): readonly PageSummary[] | null;
  /** Starts a reader over some of this segment's columns (one per scan and segment). */
  open(request: SegmentOpenRequest): SegmentReader;
}

export interface SegmentOpenRequest {
  /** Catalog column ids the reader will be asked for. */
  readonly columns: readonly number[];
  /**
   * Row ranges the scan will read, ascending (after page pruning); the reader coalesces their
   * pages into large reads and may prefetch the next range while the current one is consumed.
   */
  readonly ranges: ReadonlyArray<{ readonly firstRow: number; readonly rowCount: number }>;
  /** Charged with the reader's encoded slabs and scratch while it is open. */
  readonly grant: Grant;
  readonly signal?: AbortSignal;
}

/**
 * Reads row ranges of one segment, in ascending order. `close` frees everything it holds and is
 * idempotent; the scan closes it at the segment's end, on abort, or on error.
 */
export interface SegmentReader {
  /**
   * The live rows of `firstRow … firstRow + rowCount − 1` (≤ the batch capacity), written into
   * `selection` (positions relative to `firstRow`, ascending). Returns how many are live. Cheap:
   * reads only the deletion bitmap, never column pages.
   */
  live(firstRow: number, rowCount: number, selection: Selection): Promise<number>;
  /**
   * Decodes one column over `firstRow … firstRow + rowCount − 1` into the caller's buffers, or
   * only the slots `rows` names (late decode after a filter: other slots are unspecified).
   * Returns the vector to use:
   * - `out.flat`, filled (slots relative to `firstRow`);
   * - a dictionary vector over `out.codes` whose dictionary is the page's or segment's
   *   (immutable for the pin, with a stable `dictionaryId`): dictionary pages stay codes;
   * - a constant vector for a constant page.
   * Varlen data is appended through `out.varlen` (built on the caller's grant); if it answers
   * `full` or `denied`, the read returns `{ partial: k }` with slots before k complete.
   */
  read(request: ColumnReadRequest): Promise<Vector | { readonly partial: number }>;
  close(): void;
}

export interface ColumnReadRequest {
  readonly columnId: number;
  readonly firstRow: number;
  readonly rowCount: number;
  /** Only these slots are needed (ascending, relative to `firstRow`), or null for all. */
  readonly rows: Selection | null;
  readonly out: {
    readonly flat: FlatVector;
    readonly codes: Int32Array;
    readonly varlen: VarlenBuilder | null;
  };
}

/**
 * Index probes by equality key (keys/key-encoding.ts `encodeKeyBatch` in equality mode). Covers
 * published runs, the memtable delta and this transaction's staged rows, minus deleted rows.
 * - A unique index gives at most one locator per key; a non-unique one may give several, written
 *   as consecutive matches.
 * - Output: for match m, `probe[m]` is the key's index in `keys` and `locators[m]` its row (type
 *   `locator` of this table, this snapshot). Matches are ordered by probe index, then row id.
 * - `capacity` bounds the matches written; `matched` says how many were. When `nextKey` <
 *   `keys.count`, the caller calls again from it; one key's matches are never split across calls
 *   unless they alone exceed `capacity` (then `nextKey` stays and `fromMatch` continues inside it).
 */
export interface LookupRequest {
  readonly indexId: number;
  readonly keys: KeyBatch;
  readonly fromKey: number;
  /** Matches of `fromKey` already returned by the previous call (0 normally). */
  readonly fromMatch: number;
  readonly capacity: number;
  readonly probe: Uint16Array;
  readonly locators: Float64Array;
  readonly grant: Grant;
  readonly signal?: AbortSignal;
}

export interface LookupResult {
  readonly matched: number;
  /** The first key not fully answered (`keys.count` when done). */
  readonly nextKey: number;
  /** Matches of `nextKey` already written (non-zero only when one key overflowed `capacity`). */
  readonly nextMatch: number;
}
