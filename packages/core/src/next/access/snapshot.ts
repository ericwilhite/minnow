/**
 * Snapshots: what a reader sees, and for how long (owner: write path + key index, track B3).
 *
 * This is the executor's only read contract (ruling R1): it builds its scan operators and its
 * `RowFetcher` (exec/fetch.ts) over these segments and the format track's chunk readers. The
 * write path implements it; lowering reads the catalog; the owner manages pin lifetime. Nothing
 * here is durable.
 *
 * ## What a snapshot is
 * A `Snapshot` is a pinned, immutable view: one manifest root (held by an extent-space pin, so
 * nothing it reaches is reclaimed: B1's two-generation reclaim) plus the in-memory tail of every
 * table (memtables, deletions and key-index deltas) bounded by a **visibility bound** (below). It
 * is O(1) to pin: nothing is copied. Everything reachable from it reads the same until
 * `release()`.
 *
 * ## Visibility bound (`Snapshot.bound`)
 * Every in-memory change carries a sequence number `seq`; a change is visible iff
 * `seq ≤ bound` (`isVisible`). One comparison serves readers and the writer:
 * - A committed change has its transaction's LSN (1 … `MAX_LSN`).
 * - A change of the open write transaction (there is at most one: one writer at a time) has
 *   `pendingSeq(statement)`, which is above every LSN. It is restamped to the commit LSN when
 *   the transaction commits, and removed when it (or its statement, or a savepoint) rolls back.
 * - A **read snapshot** (`WritePath.pin`) has bound = an LSN, so it never sees pending changes.
 * - A **statement view** (`Statement.view`) of statement s has bound = `pendingSeq(s) − 1`: every
 *   committed change plus the transaction's changes from earlier statements, never its own. That
 *   is SQL's statement-level snapshot (and Halloween protection: an UPDATE never meets the rows it
 *   writes).
 * - Durable structures (groups, deletion bitmaps, index runs) in a pinned root are visible to
 *   every bound that can pin that root: a root checkpoints only LSNs ≤ the durable LSN at its
 *   publication, and no bound is below the durable LSN of its pin time (`WritePath.pin`). Rows a
 *   root holds as staged (format/replay.ts "Large transactions") are visible only once their
 *   `publishedRows` commit is (and, before that, to their own transaction's later statements).
 *
 * ## Segments and locators
 * `TableSnapshot.segments` lists every row the view can see, **in row-id order**, as segments of
 * at most 65,536 positions: durable groups (`kind: "group"`) and rows still in memory
 * (`kind: "memory"`). The open transaction's rows come last (a large transaction's rows may be
 * groups it wrote but has not committed). A locator (vector/locator.ts) is
 * `segment index × 65,536 + position`. A position names a stored row whether or not the view
 * considers it deleted: scans filter with `Segment.deletions()`; `fetch` reads whatever a locator
 * names.
 *
 * A memory segment of a statement view may hold positions past its `rowCount`: rows the current
 * statement wrote. Scans never see them; `fetch` serves locators that `TableWriter.conflicts`
 * returned for them (ON CONFLICT reads the row the statement inserted).
 *
 * ## Scanning (what the executor builds on)
 * - **Pruning.** `Segment.statistics(columnId)` and `Segment.pages(columnId)` give sound bounds:
 *   every stored value of the range (deleted rows included) lies within `[min, max]`, and
 *   `nullCount` counts its stored nulls exactly. `min`/`max` are exact for fixed-width types; text
 *   and bytes bounds may be truncated (min a prefix, max rounded up), still sound. Unknown is null,
 *   never a guess. So they can prune and prove the int64 f64 fast path (vector/int64.ts
 *   `provenSafe`). Approximate values (distinct sketches) are never here.
 * - **Groups.** Pages are read through `Snapshot.pages` (B1's verified cache) with the format
 *   track's chunk readers over `GroupSegment.chunk(columnId)`: dictionary pages decode to
 *   dictionary vectors (codes kept), and decoded dictionaries are charged to the query that
 *   decoded them (R5), never to the cache.
 * - **Memory.** `MemorySegment.chunk` returns a borrowed, zero-copy vector of one
 *   `MEMORY_CHUNK_ROWS`-row chunk (a dictionary vector where the memtable keeps codes).
 * - **Live rows.** `Segment.deletions()` gives the view's deletion vector; `liveRows` turns a
 *   position range into a selection. Deletion bitmaps of groups are blobs read through the cache.
 *
 * ## Memory segments: borrowing, retention, re-pointing (ruling R16.3)
 * - **Borrow.** A table snapshot borrows at most one chunk index per memory segment: the vectors
 *   `chunk()` returned for that chunk index (every column) stay valid until `chunk()` is called on
 *   the same segment with another chunk index, or the snapshot is released. The write path keeps
 *   their bytes and charges them to itself meanwhile (`WritePathStats.borrowedChunkBytes`).
 * - **Retention until publication.** A frozen memtable stays resident, charged to the write path,
 *   until its flush publishes; that is flush debt, bounded by `FROZEN_MEMTABLE_CAP_BYTES` with
 *   backpressure on writers, never by readers.
 * - **Re-point at publication.** When the flush's publish resolves, the write path, in the same
 *   synchronous step and before anything else can publish or retire, takes one extent-space pin
 *   on the publishing generation for the readers it re-points, and re-points every table snapshot
 *   still reading that memtable to the published group holding the same rows at the same
 *   positions. Then the memtable is freed (except each reader's one borrowed chunk, until its
 *   borrow ends). A re-pointed segment keeps its index, `rowCount`, deletions and summaries;
 *   its `chunk()` decodes into the caller's `out` through the page cache, leases charged to
 *   `TableReadOptions.budget`, and may return a Promise. That pin is released when the last
 *   re-pointed reader of that publication is released.
 * So a slow reader never blocks a writer and never pins more than one chunk per memory segment.
 *
 * ## Pin lifetime and ownership
 * - The owner pins one snapshot per query (or read transaction / snapshot scope), passes it to
 *   the executor, and releases it after the execution settles, or when the client's Web Lock is
 *   released. `release` is idempotent and synchronous. A statement view belongs to its statement
 *   and is released by `Statement.end()` / `rollback()`, never by the executor.
 * - **Release** (on close, on cancellation, on error alike: a cancelled execution settles, then
 *   the owner releases) ends every borrow of every table snapshot taken from it, drops its
 *   references to memtables, releases its extent-space pin and its share of any re-point pin, and
 *   frees nothing a newer snapshot still uses. A `chunk()` or index call in flight at release
 *   rejects with `SnapshotReleasedError` and releases its lease; vectors borrowed from it must not
 *   be used after release (the executor's aborted pipeline never touches them again).
 * - After release every method throws `SnapshotReleasedError` (defined here; exec/fetch.ts uses
 *   it). Locators die with the pin.
 * - Memory: a pin costs O(1) owner heap; what it retains is the write path's, reported as debt
 *   (`WritePathStats`), and bounded as above. Buffers it hands out are immutable: never write to
 *   them.
 */
import type { ColumnChunk, GroupRef, GroupRoot } from "../format/manifest.js";
import type { ColumnStatistics } from "../types.js";
import type { VarlenBuilder } from "../vector/varlen.js";
import type { FlatVector, Selection, Vector } from "../vector/vector.js";
import type { CatalogView, ColumnDef } from "./catalog.js";
import type { IndexReader, MaybePromise } from "./key-index.js";
import type { ByteBudget, SpaceReader } from "./space.js";

/** Largest LSN; LSNs are safe integers below the pending range. */
export const MAX_LSN = 2 ** 52 - 1;
/** Sequence numbers of the open write transaction's changes: `PENDING_BASE + statement`. */
export const PENDING_BASE = 2 ** 52;
/** Statements per transaction (pending sequence numbers stay safe integers). */
export const MAX_STATEMENTS = 2 ** 52 - 2;

/** Sequence number of a change made by statement `statement` (1, 2, …) of the open transaction. */
export function pendingSeq(statement: number): number {
  if (!(Number.isInteger(statement) && statement >= 1 && statement <= MAX_STATEMENTS))
    throw new RangeError("Statement ordinals run from 1");
  return PENDING_BASE + statement;
}

/** The bound of a read snapshot at `lsn` (sees committed changes with LSN ≤ lsn, nothing pending). */
export function readBound(lsn: number): number {
  if (!(Number.isInteger(lsn) && lsn >= 0 && lsn <= MAX_LSN)) throw new RangeError("Bad LSN");
  return lsn;
}

/** The bound of statement `statement`'s view: everything committed, earlier statements' changes. */
export function statementBound(statement: number): number {
  return pendingSeq(statement) - 1;
}

/** The visibility rule: a change with `seq` is visible under `bound`. */
export function isVisible(seq: number, bound: number): boolean {
  return seq <= bound;
}

export interface Snapshot {
  /** The pinned root's sequence (diagnostics). */
  readonly rootSequence: number;
  readonly released: boolean;
  /** The visibility bound (see the module comment). */
  readonly bound: number;
  /**
   * The newest commit this view can observe: `bound` for a read snapshot, the transaction's base
   * LSN for a statement view. Under strict durability nothing derived from this view reaches a
   * client before `WritePath.durable(observedLsn)` resolves (write-path.ts "Delivery").
   */
  readonly observedLsn: number;
  /** The catalog this view reads (a statement view sees its transaction's own DDL). */
  readonly catalog: CatalogView;
  /** Verified, cached reads of this pin's blobs (group roots, directories, pages, bitmaps). */
  readonly pages: SpaceReader;
  /**
   * One table as this view sees it. Synchronous when its group directory is cached (always for a
   * warm point read); otherwise reads it once. Throws `UnknownTableError` for a table the catalog
   * lacks. Each call returns a new table snapshot (its own borrows and budget).
   */
  table(tableId: number, options: TableReadOptions): MaybePromise<TableSnapshot>;
  /** An index of `tableId` (the primary key, a unique or a secondary index), read in this view. */
  index(tableId: number, indexId: number): IndexReader;
  /** Idempotent. Not allowed on a statement view (its statement releases it). */
  release(): void;
}

export interface TableReadOptions {
  /** The query's grant: charged for the page leases of re-pointed memory segments' reads. */
  readonly budget: ByteBudget;
}

export interface TableSnapshot {
  readonly tableId: number;
  /** The schema version this view reads the table under. */
  readonly schemaVersion: number;
  /** The catalog definition this view reads the table under. */
  readonly columns: readonly ColumnDef[];
  /** Rows visible and not deleted in this view, exactly. */
  readonly liveRowCount: number;
  /** In row-id order; `segments[i].index === i` is the locator segment. */
  readonly segments: readonly Segment[];
}

export type Segment = GroupSegment | MemorySegment;

/** One page (or memory chunk) of one column: its row range in the segment and its bounds. */
export interface PageSummary extends ColumnStatistics {
  readonly firstRow: number;
  readonly rowCount: number;
}

interface SegmentBase {
  /** Position of this segment in `TableSnapshot.segments` (the locator's segment number). */
  readonly index: number;
  /** Positions 0 … rowCount − 1 are the rows this view scans (≤ 65,536). */
  readonly rowCount: number;
  /** Positions not deleted in this view, exactly. */
  readonly liveRowCount: number;
  /**
   * The rows this view considers deleted, or null when none is. Synchronous for memory segments
   * and for groups whose bitmap is cached; otherwise reads the group's bitmap once. Immutable,
   * shared, charged to the cache (bitmaps) or the write path (memory deletions).
   */
  deletions(): MaybePromise<DeletionVector | null>;
  /**
   * Sound bounds of a column over the whole segment ("Scanning"), or null when unknown.
   * Groups: from the group root (`root()` must have resolved).
   */
  statistics(columnId: number): ColumnStatistics | null;
  /**
   * Bounds per page (groups: from the chunk's page directory, read once through the cache) or per
   * `MEMORY_CHUNK_ROWS` chunk (memory), in row order, covering 0 … rowCount − 1 without gaps. A
   * column the group predates is one summary of every row with `ColumnDef.missing`'s bounds.
   */
  pages(columnId: number): MaybePromise<readonly PageSummary[]>;
}

/** A durable (or the open transaction's staged) column group. */
export interface GroupSegment extends SegmentBase {
  readonly kind: "group";
  readonly group: GroupRef;
  /** True for rows of the open transaction written as a group (visible to it alone). */
  readonly staged: boolean;
  /** The group root (decoded once, cached, checksum verified when read). */
  root(): MaybePromise<GroupRoot>;
  /**
   * The chunk of `columnId` for the format track's chunk reader, or null when the group predates
   * the column (ALTER TABLE ADD COLUMN): every row then has `ColumnDef.missing`. Requires `root()`
   * to have resolved.
   */
  chunk(columnId: number): ColumnChunk | null;
}

/** Rows in memory, stored as immutable chunks of `MEMORY_CHUNK_ROWS` rows. */
export interface MemorySegment extends SegmentBase {
  readonly kind: "memory";
  /**
   * Chunk `chunk` (rows `chunk × MEMORY_CHUNK_ROWS …`, at most `rowCount`) of `columnId`, in its
   * exact physical type. While resident it returns a borrowed, immutable flat or dictionary vector
   * synchronously and ignores `out` (see "Borrow" for how long it stays valid). After a re-point
   * it decodes into `out` and may return a Promise; a varlen builder answering `full` or `denied`
   * gives `{ partial: k }` (slots before k complete).
   */
  chunk(columnId: number, chunk: number, out: ChunkOutput): MaybePromise<ChunkResult>;
}

/** Caller-owned buffers for a re-pointed memory chunk (capacity ≥ `MEMORY_CHUNK_ROWS`). */
export interface ChunkOutput {
  readonly flat: FlatVector;
  readonly codes: Int32Array;
  readonly varlen: VarlenBuilder | null;
}

export type ChunkResult = Vector | { readonly partial: number };

/** Memory segments store rows in chunks of this many rows (= `BATCH_ROWS`). */
export const MEMORY_CHUNK_ROWS = 2048;

/** Frozen memtable bytes awaiting their flush's publication before writers wait (two memtables). */
export const FROZEN_MEMTABLE_CAP_BYTES = 2 * 8 * 1024 * 1024;

/** The deleted positions of one segment, as one view sees them. Immutable; synchronous. */
export interface DeletionVector {
  readonly deletedCount: number;
  isDeleted(position: number): boolean;
  /**
   * Writes into `out` the rows of the batch covering positions `[from, from + length)` that are
   * live: batch row k is position `from + k`. Only rows of `input` (or every row when null) are
   * considered; `out.rows` must hold `length` entries. `out` may be `input` (filters in place).
   */
  liveRows(from: number, length: number, input: Selection | null, out: Selection): void;
}

export class UnknownTableError extends Error {
  readonly code = "MINNOW_UNKNOWN_TABLE";
  constructor(readonly tableId: number) {
    super(`Table ${String(tableId)} is not in this snapshot's catalog`);
    this.name = "UnknownTableError";
  }
}

/** Thrown by every method of a released snapshot, and by fetches through it (exec/fetch.ts). */
export class SnapshotReleasedError extends Error {
  readonly code = "MINNOW_SNAPSHOT_RELEASED";
  constructor() {
    super("The snapshot these locators belong to has been released");
    this.name = "SnapshotReleasedError";
  }
}
