/**
 * The manifest root and the metadata it reaches, at the type level. Byte codecs belong to the
 * format track; these shapes and invariants are fixed.
 *
 * ## Shape
 *   ManifestRoot ─┬─ catalog (BlobRef)
 *                 ├─ tables[] ─ TableRoot ─┬─ groups (BlobRef → GroupRef[])
 *                 │                       │     └─ GroupRef.root (BlobRef → GroupRoot)
 *                 │                       │           └─ columns[].directory (BlobRef → PageEntry[])
 *                 │                       ├─ deletion vectors (per GroupRef)
 *                 │                       └─ keyIndex / secondary (IndexRoot → RunRef[])
 *                 └─ clients (exactly-once ledger)
 *
 * ## Invariants
 * - **Size.** The root is O(tables). A table root is O(1) plus its index runs; its group directory
 *   is O(groups); a group root is O(columns); a page directory is O(pages of one column chunk).
 *   Nothing is O(rows). Planning and pruning read directories, never pages.
 * - **Verify once.** Every reference carries the xxHash32 of the bytes it names. A reader checks
 *   it when the bytes come from storage; cached bytes are trusted. Pages carry no checksum of
 *   their own (their PageEntry does).
 * - **Immutability.** Everything a root reaches is immutable. A change writes new packs and
 *   publishes a new root (copy on write up the tree).
 * - **Reachability.** Every pack a root reaches has id < `nextPackId`. On open, packs that the
 *   root does not reach are orphans of a crash and are reclaimed (no job records).
 * - **Groups.** A table's groups cover disjoint row-id ranges, in insertion (row-id) order. Row ids
 *   are never reused and never rewritten by maintenance; a merge keeps each row's id.
 * - **Log rule.** Recovery replays a frame of table T iff its salt is `log.salt` and its LSN is
 *   greater than `T.flushedThroughLsn`. A root may switch to a new salt (and the log be reset after
 *   it is durable) only when every table's `flushedThroughLsn` covers the old log's last LSN.
 * - **Exactly once.** A request (client id, sequence) is committed iff its frame is replayed or
 *   `clients` records a sequence ≥ it. Ledger entries outlive the log for `CLIENT_RETRY_WINDOW_MS`;
 *   clients stop retrying before that (the protocol enforces it).
 */
import type { PhysicalType } from "../types.js";
import type { PackId } from "../storage/contract.js";

export const MANIFEST_FORMAT_VERSION = 1;
/** How long a committed request stays deduplicated after its frame leaves the log. */
export const CLIENT_RETRY_WINDOW_MS = 10 * 60 * 1000;

/** A checksummed byte range of a pack. */
export interface BlobRef {
  readonly pack: PackId;
  readonly offset: number;
  readonly length: number;
  /** xxHash32 (seed 0) of the referenced bytes. */
  readonly checksum: number;
}

export interface ManifestRoot {
  readonly formatVersion: number;
  /** Strictly increasing per publish. */
  readonly sequence: number;
  /** Store epoch of the owner that published it (diagnostic; fencing is the store's). */
  readonly epoch: number;
  readonly nextPackId: PackId;
  /** The log incarnation recovery replays. */
  readonly log: { readonly salt: number; readonly firstLsn: number };
  readonly catalogVersion: number;
  /** The catalog: tables, columns (with PhysicalType), constraints, views, sequences. */
  readonly catalog: BlobRef;
  readonly tables: readonly TableRoot[];
  readonly clients: readonly ClientLedgerEntry[];
}

export interface TableRoot {
  readonly tableId: number;
  /** The next row id this table allocates (row ids start at 1). */
  readonly nextRowId: number;
  /** Every frame of this table with LSN ≤ this is in `groups`. */
  readonly flushedThroughLsn: number;
  readonly liveRowCount: number;
  /** → GroupRef[] in row-id order. */
  readonly groups: BlobRef;
  readonly groupCount: number;
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
  /** → PageEntry[] for this column of this group. */
  readonly directory: BlobRef;
  readonly statistics: ColumnStatistics;
}

/** Page and chunk statistics. Approximate fields never authorize pruning or overflow shortcuts. */
export interface ColumnStatistics {
  readonly nullCount: number;
  /**
   * Exact bounds in the type's stored encoding (int64 family as bigint; text as UTF-8 bytes in
   * byte order, possibly truncated with a conservative upper bound), or null if unknown.
   */
  readonly min: StatValue | null;
  readonly max: StatValue | null;
  /** Dictionary entries, when the chunk is dictionary-encoded. */
  readonly dictionarySize: number | null;
  /** Mergeable distinct-count sketch (approximate; planning only), or null. */
  readonly distinctSketch: BlobRef | null;
}

export type StatValue = boolean | number | bigint | Uint8Array;

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
  /** Byte offset and length within the column chunk's pack. */
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
  /** Sorted entries; memcomparable key bytes (DECISIONS.md C7). */
  readonly entries: BlobRef;
  /** Every Nth key with its entry offset, for binary search without reading the run. */
  readonly fences: BlobRef;
  readonly bloom: BlobRef;
  readonly minKey: Uint8Array;
  readonly maxKey: Uint8Array;
}

export interface ClientLedgerEntry {
  readonly clientId: Uint8Array;
  readonly lastSequence: number;
  readonly committedAtMs: number;
}
