/**
 * Snapshots: what a reader sees, and for how long (owner: write path + key index, track B3).
 *
 * The write path implements these; the executor (scans, `fetch`, `keyLookup`, joins), lowering
 * (catalog) and the owner (pin lifetime) consume them. Nothing here is durable.
 *
 * ## What a snapshot is
 * A `Snapshot` is a pinned, immutable view: one manifest root plus the in-memory tail of every
 * table (memtables, deletions and key-index deltas) bounded by a **visibility bound** (below). It
 * is O(1) to pin: nothing is copied. Everything reachable from it stays readable, unchanged,
 * until `release()`:
 * - the root's groups, deletion bitmaps and index runs (maintenance publishes new roots and
 *   reclaims only space no pinned root reaches: format/manifest.ts "Reclaim");
 * - the memtables of that moment, including ones frozen or flushed since (a pin keeps a frozen
 *   memtable alive; the write path counts that memory and the pin's age as debt).
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
 *   publication, and no bound is below the durable LSN of its pin time (`WritePath.pin`).
 *
 * ## Segments and locators
 * `TableSnapshot.segments` lists the table's rows as the snapshot sees them, in row-id order:
 * the root's groups (`kind: "group"`), then the memtables (`kind: "memory"`, oldest first, the
 * open transaction's rows last). A locator (vector/locator.ts) is `segment index × 65,536 +
 * position`. Each segment has at most 65,536 positions. A position names a stored row whether
 * or not the view considers it deleted: scans filter deletions (`Segment.deletions`), `fetch`
 * reads whatever a locator names.
 *
 * A memory segment of a statement view may hold positions past its `rowCount`: rows the current
 * statement wrote. Scans never see them; `fetch` serves locators that `TableWriter.conflicts`
 * returned for them (ON CONFLICT reads the row the statement inserted).
 *
 * ## Pin lifetime and ownership
 * - The owner pins one snapshot per query (or per read transaction / snapshot scope) and releases
 *   it when the query ends, fails or is cancelled, or when the client's Web Lock is released.
 *   `release` is idempotent and synchronous. A statement view belongs to its statement and is
 *   released by `Statement.end()` / `rollback()`, never by the executor.
 * - After release every method throws `SnapshotReleasedError` (exec/fetch.ts defines the same
 *   error class shape; the owner maps both). Locators die with the pin.
 * - `Snapshot` structurally satisfies exec/fetch.ts `SnapshotPin` (`rootSequence`, `released`,
 *   `release`), so a fetch request carries it as is.
 * - Memory: a pin costs O(1) owner heap. What it retains (frozen memtables, unreclaimable
 *   extents) is the write path's memory, charged to the write path's grant and reported as debt
 *   (`WritePathStats`); never charged to the query. Buffers it hands out (memory chunks, deletion
 *   vectors) are immutable and borrowed for the pin's life: never write to them.
 */
import type { ColumnChunk, GroupRef, GroupRoot } from "../format/manifest.js";
import type { FlatVector, Selection } from "../vector/vector.js";
import type { CatalogView, ColumnDef } from "./catalog.js";
import type { IndexReader } from "./key-index.js";

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
  /** The pinned root's sequence (diagnostics, and exec/fetch.ts `SnapshotPin`). */
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
  /** Throws `UnknownTableError` for a table the catalog lacks. */
  table(tableId: number): TableSnapshot;
  /** An index of `tableId` (the primary key, a unique or a secondary index), read in this view. */
  index(tableId: number, indexId: number): IndexReader;
  /** Idempotent. Not allowed on a statement view (its statement releases it). */
  release(): void;
}

export interface TableSnapshot {
  readonly tableId: number;
  /** The catalog definition this view reads the table under. */
  readonly columns: readonly ColumnDef[];
  /** Rows visible and not deleted in this view, exactly. */
  readonly liveRowCount: number;
  /** The table's segments in row-id order; `segments[i].index === i` is the locator segment. */
  readonly segments: readonly Segment[];
}

export type Segment = GroupSegment | MemorySegment;

interface SegmentBase {
  /** Position of this segment in `TableSnapshot.segments` (the locator's segment number). */
  readonly index: number;
  /** Positions 0 … rowCount − 1 are the rows this view scans (≤ 65,536). */
  readonly rowCount: number;
  /** Positions not deleted in this view, exactly. */
  readonly liveRowCount: number;
  /**
   * The rows this view considers deleted, or null when none is. Synchronous for memory segments
   * and for groups whose bitmap is cached; otherwise it reads (and verifies) the group's deletion
   * bitmap once. The result is immutable and shared; it is charged to the page cache, not the
   * caller.
   */
  deletions(): DeletionVector | null | Promise<DeletionVector | null>;
}

/** A durable column group of the pinned root. Its pages are read through `pages`. */
export interface GroupSegment extends SegmentBase {
  readonly kind: "group";
  readonly group: GroupRef;
  /** The group root (decoded once, cached, checksum verified when read). */
  root(): GroupRoot | Promise<GroupRoot>;
  /**
   * The chunk of `columnId`, or null when the group predates the column (ALTER TABLE ADD COLUMN):
   * every row then has the column's `ColumnDef.missing` value. Requires `root()` to have resolved.
   */
  chunk(columnId: number): ColumnChunk | null;
}

/**
 * Rows still in memory: a memtable (committed rows, maybe some of the open transaction's). Data
 * is stored as immutable chunks of `MEMORY_CHUNK_ROWS` rows, so batches are zero-copy views.
 */
export interface MemorySegment extends SegmentBase {
  readonly kind: "memory";
  /**
   * Rows `[chunk × MEMORY_CHUNK_ROWS, …)` of `columnId`: a borrowed flat vector of the chunk's
   * rows visible to this view (`length` = min(MEMORY_CHUNK_ROWS, rowCount − chunk start)), in
   * the column's exact physical type. Immutable and valid until the pin is released. (Memtable
   * rows are converted when an ALTER TABLE applies, so every column of the view's catalog exists.)
   */
  chunk(columnId: number, chunk: number): FlatVector;
}

/** Memory segments store rows in chunks of this many rows (= `BATCH_ROWS`). */
export const MEMORY_CHUNK_ROWS = 2048;

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

/** Thrown by every method of a released snapshot (same code as exec/fetch.ts). */
export class SnapshotReleasedError extends Error {
  readonly code = "MINNOW_SNAPSHOT_RELEASED";
  constructor() {
    super("The snapshot these locators belong to has been released");
    this.name = "SnapshotReleasedError";
  }
}
