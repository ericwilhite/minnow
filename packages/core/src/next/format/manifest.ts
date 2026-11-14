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
 *                                          │                 ├─ data (ChunkData: the chunk's pages)
 *                                          │                 └─ directory (BlobRef → PageEntry[])
 *                                          ├─ deletion bitmaps (per GroupRef)
 *                                          └─ keyIndex / secondary (IndexRoot → RunRef[])
 *
 * ## Invariants
 * - **Addresses.** Everything lives in the store's data space (storage/contract.ts), allocated in
 *   **extents** (`ExtentRef`: a `DATA_ALIGN`-aligned offset and the bytes written), the unit of
 *   allocation and reclaim. Every reference names the extent it lives in: a `BlobRef` is
 *   `{extent, offset, length, checksum}` with `offset` relative to `extent.offset` (ruling R15.1,
 *   storage's shape). So a reachability walk yields each reference's `extent` as it meets it and
 *   never infers an extent from an address.
 * - **Packing.** Small blobs (metadata blobs, page directories, sketches, the data of small column
 *   chunks) may share one 4 KiB-aligned extent; a large chunk's data has an extent of its own. An
 *   extent is reclaimed as a unit: it is free only when no blob in it is reachable from the current
 *   root, the previous root or a pin. Every reference into one extent carries the same `ExtentRef`.
 * - **Chunks.** A column chunk's pages lie contiguously in its data blob (`ColumnChunk.data`,
 *   inside one extent); each `PageEntry.offset` is relative to the start of that blob, so a page's
 *   data-space address is `blobAddress(data) + page.offset`. A chunk never spans extents (a writer
 *   that cannot place it contiguously writes a smaller group). Metadata blobs and pages are at most
 *   1 MiB (storage `MAX_BLOB_BYTES`); a chunk's data blob and an extent have no cap. The parts of
 *   a chunk's out-of-line values (page/large-value.ts) lie inside its data blob too, so they need
 *   no reference of their own: reaching the chunk reaches them, and they are reclaimed with it.
 * - **Size.** The root is O(tables + ledger). A table root is O(1) plus its index runs; its group
 *   directory is O(groups); a group root is O(columns); a page directory is O(pages of one
 *   chunk). Nothing is O(rows). Planning and pruning read directories, never pages.
 * - **Verify once.** Every reference carries the xxHash32 of the bytes it names (pages: their
 *   PageEntry). A reader checks it when the bytes come from storage; cached bytes are trusted.
 * - **Immutability and sharing.** Everything a root reaches is immutable. A change writes new
 *   extents and publishes a new root (copy on write up the tree). A metadata-only rewrite (new
 *   statistics, a deletion bitmap, a renamed column) writes a new group root that references the
 *   old chunk extents unchanged; several roots and groups may reference one extent.
 * - **Reclaim (two generations).** An extent is reusable only when neither the current durable root,
 *   nor the previous durable root (the other root slot), nor any pinned snapshot's root reaches
 *   it (storage/extents.ts, storage/reclaim.ts). Keeping the previous generation means a lost flush that rolls back to the previous root
 *   slot finds its extents intact instead of overwritten. At startup the owner reads both roots'
 *   extent lists (`extentList`, ruling R25; crash orphans are simply unlisted: no job records)
 *   and maintains free space in memory after. Freed space is reused in place; only a free tail is ever released to the platform.
 * - **Startup order** (one log scan): acquire the fence → open the store session → open the extent
 *   space (read the current and previous roots' extent lists from the store's root slots) → open the write path
 *   (a single log scan and replay, `publishedRows` staging included) → spill sweep → ready.
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
 *   `tables` agrees with the catalog at `globalLsn`: every table it has (one dropped later stays,
 *   as it was, since replay starts before the drop) with every index it has; a table root may
 *   also carry an empty root for its own index built after `globalLsn`. An index build no table
 *   checkpoint covers therefore holds `globalLsn` below it (a build on a table with no
 *   unpublished data has no entries, and the next root settles it). A later DROP only removes
 *   that obligation once `globalLsn` covers the DROP: an earlier catalog still needs the build.
 * - **Exactly once** (ledger.ts): `clients` holds the committed outcomes as of `globalLsn`.
 */
import { DURABLE_CODECS, DURABLE_FORMATS } from "../durable-format.js";
import type { RunKeyStorage } from "./key-pointer.js";
import type { RunEncoding } from "./encoding.js";
import type { ClientLedgerEntry } from "./ledger.js";
import type { LogIncarnation } from "./log-frame.js";
import type { ExtentList } from "../storage/extent-list.js";
import type { BlobRef } from "../storage/extents.js";
import type { ColumnStatistics, PhysicalType, StatValue } from "../types.js";

export type { ColumnStatistics, StatValue };

/**
 * 3: explicit run encoding (inline/overflow); all earlier rebuild roots were unpublished.
 * 2: the root names its extent list (ruling R25, `extentList`). Roots of format 1 were written
 * only by development builds (no supported writer: DECISIONS.md "R25") and are refused.
 */
export const MANIFEST_FORMAT_VERSION = DURABLE_FORMATS.manifest.version;

/**
 * Data-space addresses are the storage track's (storage/extents.ts, ruling R15.1): an `ExtentRef`
 * is the unit of allocation and reclaim; a `BlobRef` is a checksummed byte range inside one extent;
 * `blobAddress` is a blob's data-space address.
 */
export type { BlobRef, ExtentRef } from "../storage/extents.js";
export { blobAddress } from "../storage/extents.js";

/**
 * A column chunk's data blob ("Chunks"): where its pages lie, contiguously, inside one extent. A
 * `BlobRef` without a checksum of its own: each page carries its checksum in its `PageEntry`, a
 * reader reads pages (never the whole range), and the range may exceed the 1 MiB blob cap.
 */
export type ChunkData = Omit<BlobRef, "checksum">;

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
  /** MCRT catalog tree: ordered MCPG fragments of the versioned MCAT logical records. */
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
  /**
   * Ruling R25: the paged, offset-sorted list of every extent this root reaches except the ones
   * the list's pages live in (storage/extent-list.ts). Startup reads it, never the group roots;
   * the full walk (reachable.ts) is the audit's cross-check.
   */
  readonly extentList: ExtentList;
}

export interface TableRoot {
  readonly tableId: number;
  /** The schema version its groups were written under (replay checks frames against it). */
  readonly schemaVersion: number;
  /** The next row id this table allocates (row ids start at 1): past every row id it holds. */
  readonly nextRowId: number;
  /** Every frame of this table with LSN ≤ this is in `groups`. */
  readonly flushedThroughLsn: number;
  /** Σ `GroupRef.liveRowCount` over `groups` (staged rows are not counted). */
  readonly liveRowCount: number;
  /** → GroupRef[] in row-id order. */
  readonly groups: BlobRef;
  /** Groups in `groups`. The reachability walk checks both totals against the directory. */
  readonly groupCount: number;
  /**
   * Large transactions' rows published but not yet visible (replay.ts "Large transactions"): each
   * becomes visible when its `publishedRows` commit frame replays, and is folded into `groups`
   * (with `flushedThroughLsn` past the frame) by a later publish.
   */
  readonly staged: ReadonlyArray<{
    readonly transaction: number;
    /** No staged group starts before it. */
    readonly firstRowId: number;
    /** Σ `GroupRef.rowCount` over `groups`. */
    readonly rowCount: number;
    /** → GroupRef[] of the staged rows. */
    readonly groups: BlobRef;
    /**
     * The index runs built for the staged rows while staging (one entry per index of the table),
     * made visible, folded or dropped together with `groups`: replay never rebuilds index
     * entries from key columns.
     */
    readonly indexRuns: ReadonlyArray<{
      readonly indexId: number;
      readonly runs: readonly RunRef[];
    }>;
  }>;
  readonly keyIndex: IndexRoot | null;
  readonly secondaryIndexes: readonly IndexRoot[];
  /**
   * → `TableStatistics`: the table's planning statistics aggregated over every group in `groups`,
   * maintained at each publish, so an open reads one blob per table instead of every group root
   * (Gate A reopen). Null exactly when the table has no groups.
   */
  readonly statistics: BlobRef | null;
}

/**
 * Per-table planning statistics over the stored rows of every group (deleted rows included; sound
 * bounds, approximate counts). One entry per column some group stores, ascending column id.
 */
export interface TableStatistics {
  /** Groups summarized: the table's `groupCount` when written. */
  readonly groupCount: number;
  readonly columns: readonly ColumnTotals[];
}

export interface ColumnTotals {
  readonly columnId: number;
  /** Σ chunk null counts. */
  readonly nullCount: number;
  /** Σ non-null values stored. */
  readonly values: number;
  /** Σ chunk data bytes (average width = bytes / values). */
  readonly bytes: number;
  /** True when some group with values stored no bounds: `min`/`max` are then unknown. */
  readonly unbounded: boolean;
  /** The least and greatest bound over the groups (null when unknown or no values). */
  readonly min: StatValue | null;
  readonly max: StatValue | null;
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
  /** Raw LE u32 deletion words, inherited group-page version; exactly ceil(rowCount/32)*4
   * bytes with unused tail bits zero. Null when nothing is deleted. */
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
  /** The chunk's pages, contiguously, inside one extent (possibly shared: "Packing"). */
  readonly data: ChunkData;
  /** → PageEntry[] for this column of this group. */
  readonly directory: BlobRef;
  readonly statistics: ColumnStatistics;
  /** Mergeable distinct-count sketch (approximate; planning only), or null. */
  readonly distinctSketch: BlobRef | null;
}

export const PageEncoding = DURABLE_CODECS.page;

export interface PageEntry {
  /** Byte offset (relative to the start of the `ColumnChunk.data` blob) and length of the page. */
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
  /**
   * The out-of-line values of a plain varlen page (page/large-value.ts): the range, relative to
   * `ColumnChunk.data`, that the parts of its pointers tile exactly, in row order. It lies after
   * the previous page and before this one, so no value can alias another or overlap a page.
   * Absent when the page holds none.
   */
  readonly largeRegion?: { readonly offset: number; readonly length: number };
}

/** A key index or secondary index: leveled sorted runs of (key, rowId, seq). */
export interface IndexRoot {
  readonly indexId: number;
  readonly columnIds: readonly number[];
  readonly unique: boolean;
  readonly runs: readonly RunRef[];
}

export interface RunRef {
  /** Absent in memory means inline; always explicit on disk. */
  readonly encoding?: RunEncoding;
  readonly keyStorage?: RunKeyStorage;
  readonly level: number;
  /**
   * Present (true) on the second and later pieces of one merge output, which follow the first
   * piece in ascending key order (an output splits when it outgrows one run's block index, possibly
   * inside one key's entries). The pieces of one output are one sorted run; nothing else is.
   */
  readonly continues?: true;
  readonly entryCount: number;
  /**
   * Sorted entries: equality-key bytes (keys/key-encoding.ts), scalar types only. Runs of an
   * enum index are ordered by label bytes and serve equality lookups only (`indexKeyKind`).
   */
  readonly entries: BlobRef;
  /** Every Nth key with its entry offset, for binary search without reading the run. */
  readonly fences: BlobRef;
  readonly bloom: BlobRef;
  /** Full inline bounds, or bounded overflow prefixes. Prefix ties never prove disjointness;
   * keyStorage carries complete lengths/payload pointers for encoding2. */
  readonly minKey: Uint8Array;
  readonly maxKey: Uint8Array;
}
