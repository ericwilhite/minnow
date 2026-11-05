/**
 * The manifest root and the metadata it reaches, at the type level. Byte codecs belong to the
 * format track; these shapes and invariants are fixed.
 *
 * ## Shape
 *   ManifestRoot ─┬─ catalog (BlobRef), sequences, request ledger   ← global state at `globalLsn`
 *                 ├─ log incarnation (salt, firstLsn, rootSequence)
 *                 └─ tables[] ─ TableRoot ─┬─ groups (BlobRef → GroupRef[])
 *                                          │     └─ GroupRef.root (BlobRef → GroupRoot)
 *                                          │           └─ columns[] ─ ColumnChunk
 *                                          │                 ├─ data (ExtentRef: the chunk's pages)
 *                                          │                 └─ directory (BlobRef → PageEntry[])
 *                                          ├─ deletion bitmaps (per GroupRef)
 *                                          └─ keyIndex / secondary (IndexRoot → RunRef[])
 *
 * ## Invariants
 * - **Addresses.** Everything lives in the store's data space (storage/contract.ts). A `BlobRef`
 *   names a checksummed byte range there. A column chunk's pages are one contiguous extent
 *   (`ColumnChunk.data`); each `PageEntry.offset` is relative to it. A chunk never spans extents
 *   (a writer that cannot place it contiguously writes a smaller group).
 * - **Size.** The root is O(tables + ledger). A table root is O(1) plus its index runs; its group
 *   directory is O(groups); a group root is O(columns); a page directory is O(pages of one
 *   chunk). Nothing is O(rows). Planning and pruning read directories, never pages.
 * - **Verify once.** Every reference carries the xxHash32 of the bytes it names (pages: their
 *   PageEntry). A reader checks it when the bytes come from storage; cached bytes are trusted.
 * - **Immutability and sharing.** Everything a root reaches is immutable. A change writes new
 *   extents and publishes a new root (copy on write up the tree). A metadata-only rewrite (new
 *   statistics, a deletion bitmap, a renamed column) writes a new group root that references the
 *   old chunk extents unchanged; several roots and groups may reference one extent.
 * - **Reclaim.** An extent is free when neither the durable root nor any pinned snapshot's root
 *   reaches it. The owner computes free space on open by walking the root (crash orphans are
 *   simply unreachable: no job records) and maintains it in memory after. Freed space is reused
 *   in place; only a free tail is ever released to the platform.
 * - **Snapshots (C12).** A reader pins a root (and the memtable views of that moment) for the
 *   length of one query or read transaction; pins are in memory, never durable. A client's pins
 *   are dropped when its Web Lock is released (it vanished) or when it ends the read. There is no
 *   reading of arbitrary older versions: a root no pin holds is gone once a newer one is durable.
 * - **Groups.** A table's groups cover disjoint row-id ranges, in insertion (row-id) order. Row ids
 *   are never reused and never rewritten by maintenance; a merge keeps each row's id.
 * - **Checkpoints and replay** (replay.ts): global state (catalog, sequences, ledger) is as of
 *   `globalLsn`; each table's rows are as of its `flushedThroughLsn`. A root may switch to a new
 *   log salt only when `globalLsn` and every table's `flushedThroughLsn` reach the old log's last
 *   LSN; the new incarnation records the switching root's sequence (`log.rootSequence`).
 * - **Exactly once** (ledger.ts): `clients` holds the committed outcomes as of `globalLsn`.
 */
import type { ClientLedgerEntry } from "./ledger.js";
import type { LogIncarnation } from "./log-frame.js";
import type { ColumnStatistics, PhysicalType, StatValue } from "../types.js";

export type { ColumnStatistics, StatValue };

export const MANIFEST_FORMAT_VERSION = 1;

/** A byte range of the data space whose bytes carry their own checksums (pages). */
export interface ExtentRef {
  /** A multiple of `DATA_ALIGN`. */
  readonly offset: number;
  readonly length: number;
}

/** A checksummed byte range of the data space. */
export interface BlobRef {
  readonly offset: number;
  readonly length: number;
  /** xxHash32 (seed 0) of the referenced bytes. */
  readonly checksum: number;
}

export interface ManifestRoot {
  readonly formatVersion: number;
  /** Strictly increasing per publish. */
  readonly sequence: number;
  /** Epoch of the owner that published it (diagnostic; fencing is the store's). */
  readonly epoch: number;
  /** The log incarnation recovery replays. */
  readonly log: LogIncarnation;
  /** Global state below includes every transaction with LSN ≤ this. */
  readonly globalLsn: number;
  readonly catalogVersion: number;
  /** The catalog: tables (columns with PhysicalType, schema versions), constraints, views. */
  readonly catalog: BlobRef;
  /** Table ids below this that the catalog lacks were dropped; ids are never reused. */
  readonly nextTableId: number;
  /** Sequence state: each sequence's reserved-through value (values are handed out below it). */
  readonly sequences: ReadonlyArray<{
    readonly sequenceId: number;
    readonly reservedThrough: bigint;
  }>;
  readonly tables: readonly TableRoot[];
  readonly clients: readonly ClientLedgerEntry[];
}

export interface TableRoot {
  readonly tableId: number;
  /** The schema version its groups were written under (replay checks frames against it). */
  readonly schemaVersion: number;
  /** The next row id this table allocates (row ids start at 1). */
  readonly nextRowId: number;
  /** Every frame of this table with LSN ≤ this is in `groups`. */
  readonly flushedThroughLsn: number;
  readonly liveRowCount: number;
  /** → GroupRef[] in row-id order. */
  readonly groups: BlobRef;
  readonly groupCount: number;
  /**
   * Large transactions' rows published but not yet visible (replay.ts "Large transactions"): each
   * becomes visible when its `publishedRows` commit frame replays, and is folded into `groups`
   * (with `flushedThroughLsn` past the frame) by a later publish.
   */
  readonly staged: ReadonlyArray<{
    readonly transaction: number;
    readonly firstRowId: number;
    readonly rowCount: number;
    /** → GroupRef[] of the staged rows. */
    readonly groups: BlobRef;
  }>;
  readonly keyIndex: IndexRoot | null;
  readonly secondaryIndexes: readonly IndexRoot[];
  /** Mergeable table statistics (row count, distinct sketches, heavy hitters), or null. */
  readonly statistics: BlobRef | null;
}

export interface GroupRef {
  readonly groupId: number;
  readonly firstRowId: number;
  readonly lastRowId: number;
  /** Rows stored (≤ ~65,536), including deleted ones. */
  readonly rowCount: number;
  /** Rows not marked in `deletions`. */
  readonly liveRowCount: number;
  readonly root: BlobRef;
  /** Deletion bitmap over the group's row positions, or null when nothing is deleted. */
  readonly deletions: BlobRef | null;
}

export interface GroupRoot {
  readonly rowCount: number;
  /**
   * How a row position maps to its row id: dense (`firstRowId + position`) for a flushed group;
   * an ascending row-id column for a merged group with gaps.
   */
  readonly rowIds: { readonly kind: "dense" } | { readonly kind: "column"; readonly ref: BlobRef };
  /** One chunk per stored column, by catalog column id. */
  readonly columns: readonly ColumnChunk[];
}

export interface ColumnChunk {
  readonly columnId: number;
  readonly type: PhysicalType;
  /** The extent holding this chunk's pages, contiguously. */
  readonly data: ExtentRef;
  /** → PageEntry[] for this column of this group. */
  readonly directory: BlobRef;
  readonly statistics: ColumnStatistics;
  /** Mergeable distinct-count sketch (approximate; planning only), or null. */
  readonly distinctSketch: BlobRef | null;
}

export const PageEncoding = {
  plain: 1,
  constant: 2,
  bitmap: 3,
  /** Frame of reference + delta at u8/u16/u32 widths. */
  frameOfReference: 4,
  dictionary: 5,
  /** Plain or dictionary, then gzip (only when it saves ≥2× on a page ≥16 KiB). */
  gzip: 128,
} as const;

export interface PageEntry {
  /** Byte offset (relative to `ColumnChunk.data.offset`) and length of the page. */
  readonly offset: number;
  readonly length: number;
  /** Row positions this page covers within the group. */
  readonly firstRow: number;
  readonly rowCount: number;
  /** A `PageEncoding` value, possibly OR'd with `PageEncoding.gzip`. */
  readonly encoding: number;
  readonly nullCount: number;
  readonly min: StatValue | null;
  readonly max: StatValue | null;
  readonly checksum: number;
}

/** A key index or secondary index: leveled sorted runs of (key, rowId, seq). */
export interface IndexRoot {
  readonly indexId: number;
  readonly columnIds: readonly number[];
  readonly unique: boolean;
  readonly runs: readonly RunRef[];
}

export interface RunRef {
  readonly level: number;
  readonly entryCount: number;
  /**
   * Sorted entries: equality-key bytes (keys/key-encoding.ts), scalar types only. Runs of an
   * enum index are ordered by label bytes and serve equality lookups only (`indexKeyKind`).
   */
  readonly entries: BlobRef;
  /** Every Nth key with its entry offset, for binary search without reading the run. */
  readonly fences: BlobRef;
  readonly bloom: BlobRef;
  readonly minKey: Uint8Array;
  readonly maxKey: Uint8Array;
}
