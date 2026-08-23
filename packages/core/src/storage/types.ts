import type { DatabaseSnapshot, SnapshotLoadProgress } from "./snapshot.js";

export const storeNames = [
  "catalog",
  "manifests",
  "segments",
  "blocks",
  "transactions",
  "leases",
  "statistics",
  "temp",
  "gc",
] as const;

/** The manifest fields every commit publishes; `Manifest` adds the resolved block list. */
export interface ManifestSummary {
  version: number;
  previousVersion: number | null;
  createdAt: string;
  /**
   * Table IDs whose logical content this commit changed; empty means a logical no-change such as
   * compaction. Absent on manifests written before change tracking, which readers treat as
   * potentially changing every table.
   */
  changedTableIds?: string[];
  /** A pruned descriptor remains readable for commit reconciliation but cannot be pinned. */
  prunedAt?: string;
  /**
   * Commit-local maintenance hints, not persisted in manifest history. A ready full-text
   * column reports its durable delta-tail length so the committing engine can rebuild before
   * metadata grows with every later commit, even when nobody searches the column again.
   */
  ftsDeltaCounts?: Array<{ tableId: string; columnId: string; count: number }>;
}

export interface Manifest extends ManifestSummary {
  blockIds: string[];
}

/**
 * The stored manifest shape: a checkpoint carries the complete sorted block list; a delta
 * carries only this commit's added and removed ids plus its distance from the checkpoint below
 * it. Reads resolve a version by walking back to the nearest checkpoint and applying deltas
 * forward, so publishing a commit writes O(changed blocks) instead of rewriting every live
 * block id. Pruning first tombstones records; maintenance later deletes only the obsolete prefix
 * below the checkpoint that the earliest readable version needs.
 */
export interface StoredManifestRecord extends ManifestSummary {
  blockIds?: string[];
  addedBlockIds?: string[];
  removedBlockIds?: string[];
  /** Deltas since the checkpoint below; 0 on checkpoints. */
  deltaDepth?: number;
}

/** Every this-many commits the store writes a full checkpoint instead of a delta. */
export const MANIFEST_CHECKPOINT_INTERVAL = 32;

/** Applies one stored record to a running block set (checkpoint replaces, delta mutates). */
export function applyManifestRecord(blockIds: Set<string>, record: StoredManifestRecord): void {
  if (record.blockIds !== undefined) {
    blockIds.clear();
    for (const id of record.blockIds) blockIds.add(id);
    return;
  }
  for (const id of record.removedBlockIds ?? []) blockIds.delete(id);
  for (const id of record.addedBlockIds ?? []) blockIds.add(id);
}

export interface PublishManifestInput {
  changedTableIds?: readonly string[];
  expectedVersion: number | null;
  blockIds: readonly string[];
  createdAt?: string;
}

export const simpleDataTypes = ["boolean", "number", "string", "datetime"] as const;
export type SimpleDataType = (typeof simpleDataTypes)[number];

/**
 * Declarative write-time default. Plain structured-clone-safe data: the spec crosses the
 * worker postMessage boundary and persists in the catalog, so function defaults are
 * unrepresentable by design — the schema DSL carries those separately (`ColumnBuilder.defaultFn`)
 * and the typed facade fills them before a batch reaches the engine.
 */
export type ColumnDefault =
  | { kind: "now" }
  | { kind: "literal"; value: boolean | number | string }
  | { kind: "autoincrement" };

export interface TableColumnRecord {
  id: string;
  name: string;
  type: SimpleDataType;
  /**
   * SQL INTEGER/SMALLINT/BIGINT columns use the number physical type, but only accept exact
   * JavaScript safe integers. Absent for the public `number` type and SQL floating-point types.
   * Keeping the domain in catalog metadata prevents a declared integer from silently rounding
   * before it reaches storage while preserving the released Float64 block encoding.
   */
  integer?: true;
  nullable: boolean;
  /** Fills null-or-absent slots at insert time; never applied at read time. */
  defaultValue?: ColumnDefault;
  /**
   * What rows written before this column existed read as, instead of NULL.
   *
   * A column added by a migration has no blocks in older segments. Those rows would otherwise
   * read NULL forever, which is why adding a non-nullable column was impossible. Substituting
   * this value at read time makes the addition meaningful without rewriting a single stored
   * byte — the segments are untouched, and compaction folds the value in whenever it next
   * rewrites them. It is frozen when the column is added: a generator runs once, at migration
   * time, so every reader of a given row agrees.
   *
   * Spelled out rather than imported: storage sits below the engine, and NULL is the absence
   * this replaces, so it is not one of the options.
   */
  backfill?: boolean | number | string | Date;
  /**
   * String columns only: the closed set of values writes must draw from. Physically the column
   * stays a plain string column; the set is write-time validation metadata, so widening it (or
   * dropping it) is catalog-only while narrowing it is rejected by migration planning.
   */
  enumValues?: string[];
}

/**
 * The single authority on which enum declarations are legal, shared by the schema DSL's
 * `column.enum()` and the engine's `createTable`: at least one value, every value a non-empty
 * string, no duplicates. Returns a defensive copy.
 */
export function validateEnumValues(values: readonly string[], context: string): string[] {
  if (values.length === 0) {
    throw new TypeError(`An enum needs at least one value: ${context}`);
  }
  const seen = new Set<string>();
  for (const value of values) {
    if (typeof value !== "string" || value.length === 0) {
      throw new TypeError(`Enum values must be non-empty strings: ${context}`);
    }
    if (seen.has(value)) {
      throw new TypeError(`Duplicate enum value: ${context} has "${value}" twice`);
    }
    seen.add(value);
  }
  return [...values];
}

/**
 * Validates the catalog invariants owned by a table's column list. Storage adapters call this
 * for ordinary catalog writes and before restoring snapshots or checkpoints, so malformed
 * metadata cannot enter through a less common persistence path.
 */
export function validateTableColumns(columns: readonly TableColumnRecord[]): void {
  if (columns.length === 0) throw new TypeError("A table needs at least one column");
  for (const column of columns) {
    const integer: unknown = column.integer;
    if (integer !== undefined && (integer !== true || column.type !== "number")) {
      throw new TypeError(`Integer domain requires a number column: ${column.name}`);
    }
  }
  const ids = new Set(columns.map(({ id }) => id));
  const names = new Set(columns.map(({ name }) => name));
  if (ids.size !== columns.length || names.size !== columns.length) {
    throw new TypeError("Table columns must have unique IDs and names");
  }
}

/**
 * The single authority on which default declarations are legal, shared by the schema DSL's
 * `table()` and the engine's `createTable` so the two entry points (and the wire path between
 * them) can never drift: defaults require non-nullable columns, "now" is datetime-only,
 * auto-increment is the number unique key, and the unique key never defaults to a constant.
 */
export function validateColumnDefault(
  column: {
    name: string;
    type: SimpleDataType;
    integer?: true;
    nullable: boolean;
    isUniqueKey: boolean;
    enumValues?: readonly string[];
  },
  defaultValue: ColumnDefault,
): void {
  if (column.nullable) {
    throw new TypeError(`Defaults require a non-nullable column: ${column.name}`);
  }
  switch (defaultValue.kind) {
    case "now":
      if (column.type !== "datetime") {
        throw new TypeError(`Default now requires a datetime column: ${column.name}`);
      }
      return;
    case "autoincrement":
      if (column.type !== "number") {
        throw new TypeError(`Auto-increment requires a number column: ${column.name}`);
      }
      if (!column.isUniqueKey) {
        throw new TypeError(`Auto-increment requires the unique key column: ${column.name}`);
      }
      return;
    case "literal": {
      const value = defaultValue.value;
      if (column.type === "datetime") {
        throw new TypeError(`Datetime columns default with now, not a literal: ${column.name}`);
      }
      if (typeof value !== column.type) {
        throw new TypeError(`Default literal must be a ${column.type}: ${column.name}`);
      }
      if (typeof value === "number" && !Number.isFinite(value)) {
        throw new TypeError(`Default literal must be finite: ${column.name}`);
      }
      if (column.integer === true && !Number.isSafeInteger(value)) {
        throw new TypeError(`Default literal must be a safe integer: ${column.name}`);
      }
      if (
        column.enumValues !== undefined &&
        typeof value === "string" &&
        !column.enumValues.includes(value)
      ) {
        throw new TypeError(`Default must be one of the enum values: ${column.name}`);
      }
      if (column.isUniqueKey) {
        throw new TypeError(`Unique key cannot default to a constant: ${column.name}`);
      }
      return;
    }
    // The wire path can hand this untyped data (including specs from removed generator kinds).
    default:
      throw new TypeError(
        `Unknown default kind: ${String((defaultValue as { kind?: unknown }).kind)}`,
      );
  }
}

export type FtsColumnIndexState = "building" | "ready" | "invalid";

/**
 * One column's persisted full-text index declaration. The index is a pruning accelerator, never
 * ground truth: readers use it only in state "ready" with a matching tokenizer version, and the
 * scan re-verifies every candidate, so a stale or missing index costs speed, not correctness.
 */
export interface FtsColumnIndexRecord {
  storage: "fts-chunks-v1";
  tokenizerVersion: number;
  state: FtsColumnIndexState;
  /** Manifest version the base build covers; commit deltas above it merge at read time. */
  buildFromVersion: number;
}

export interface TableRecord {
  id: string;
  name: string;
  columns: TableColumnRecord[];
  uniqueKeyColumnId?: string;
  uniqueKeyLookupReady?: boolean;
  /** Full-text index state per column ID. Writers that see this emit commit deltas. */
  ftsColumns?: Record<string, FtsColumnIndexRecord>;
  /** AFTER triggers on this table, fired by the committing writer inside its transaction. */
  triggers?: TriggerRecord[];
  /**
   * Single-column FOREIGN KEY constraints (E141-04). The referenced column is the parent's
   * unique key, which is what the engine can probe for existence and what its keyed write paths
   * address rows by; a parent key never changes, so only ON DELETE has an action to take.
   */
  foreignKeys?: Array<{
    name: string;
    column: string;
    parentTable: string;
    parentColumn: string;
    onDelete: "restrict" | "cascade" | "set null";
  }>;
  /**
   * Row-level CHECK constraints (E141-06), each the text of a boolean expression over this
   * table's own columns. Text rather than a compiled form because the record crosses the worker
   * boundary and IndexedDB; the writer compiles it and evaluates it against every row it writes.
   */
  checks?: Array<{ name: string; sql: string }>;
  /**
   * True when `migrate()` created this table from a schema declaration, which makes the schema
   * authoritative over it: dropping the declaration may drop the table. A table created with
   * `CREATE TABLE`, or one written before this field existed, is absent-or-false and no
   * migration removes it — the same rule views follow, and it matters more here because a
   * table holds rows.
   */
  managed?: boolean;
  /**
   * A view rather than a table: the query text it stands for, and no segments of its own. The
   * `columns` are the query's inferred output schema, so a view answers the same catalog
   * questions a table does — what a reader can select, and of what type.
   */
  view?: {
    sql: string;
    /**
     * True when `migrate()` created this view from a schema declaration, which makes the schema
     * authoritative over it: dropping the declaration drops the view. A view created with
     * `CREATE VIEW`, or one written before this field existed, is absent-or-false and no
     * migration will remove it.
     */
    managed?: boolean;
  };
  createdAt: string;
  /** Compare-and-swap revision for catalog evolution; records written before it read as 0. */
  revision?: number;
}

/**
 * One AFTER trigger: catalog-persisted on its table record so the catalog epoch makes it
 * visible to every tab immediately, and executed by the committing writer inside the same
 * transaction as the triggering write — the write and its derivations publish atomically.
 */
export interface TriggerRecord {
  name: string;
  event: "insert" | "update" | "delete";
  /**
   * BEFORE and AFTER differ only in body staging order here: both fire in the committing
   * writer inside the triggering commit, so the pair exists for SQL portability, with
   * identical atomicity.
   */
  timing: "before" | "after";
  /** Body statements in order; each fires once per affected row. */
  statements: TriggerStatementRecord[];
  createdAt: string;
}

export interface TriggerStatementRecord {
  /** The body statement with every NEW.col / OLD.col reference rewritten to a placeholder. */
  sql: string;
  /** Placeholder bindings in order: which pseudo-row and column fills each parameter. */
  bindings: Array<{ source: "new" | "old"; column: string }>;
}

export class TableRecordConflictError extends Error {
  override readonly name = "TableRecordConflictError";

  constructor(
    readonly tableId: string,
    readonly expectedRevision: number,
    readonly actualRevision: number | null,
  ) {
    super(
      `Table ${tableId} changed: expected revision ${String(expectedRevision)}, found ${String(actualRevision)}`,
    );
  }
}

export type SegmentKind = "insert" | "upsert" | "update" | "delete" | "base";

/** Maps a contiguous segment-row run to its immutable hidden row IDs. */
export interface RowIdSpan {
  readonly rowStart: number;
  readonly rowCount: number;
  readonly rowIdStart: bigint;
}

export interface SegmentRecord {
  id: string;
  tableId: string;
  transactionId: string;
  rowCount: number;
  rowIdStart: bigint;
  rowIdEndExclusive: bigint;
  columnBlockIds: Record<string, string[]>;
  kind?: SegmentKind;
  keyColumnId?: string;
  /** Missing on legacy records, which are interpreted as level zero. */
  level?: number;
  /** Missing on legacy records, where commit order supplies the logical order. */
  logicalOrder?: number;
  /**
   * Staging position inside the owning transaction. Orders segments of one commit relative
   * to each other (an in-scope update must fold after the in-scope insert it patches);
   * missing on legacy records, which never shared a key within one commit.
   */
  commitOrdinal?: number;
  /** Missing on legacy insert/upsert records, which imply one contiguous row-ID span. */
  rowIdSpans?: readonly RowIdSpan[];
  /** Monotone policy ordinal for an immutable append-row-range level-two partition. */
  readonly partitionOrdinal?: number;
  createdAt: string;
}

export interface RowIdRange {
  start: bigint;
  endExclusive: bigint;
}

export type LeaseKind = "reader" | "backup";

export interface LeaseRecord {
  id: string;
  kind: LeaseKind;
  manifestVersion: number | null;
  ownerId: string;
  expiresAt: string;
  revision: number;
}

export const compactionJobStates = [
  "planned",
  "running",
  "ready",
  "published",
  "cancelled",
  "aborted",
] as const;
export type CompactionJobState = (typeof compactionJobStates)[number];

export interface CompactionJobCursor {
  sourceSegmentIndex: number;
  sourceBlockIndex: number;
}

export const compactionRewritePlanKinds = ["copy-v1", "rechunk-v1", "merge-v1"] as const;
export type CompactionRewritePlanKind = (typeof compactionRewritePlanKinds)[number];

export interface CopyCompactionRewritePlan {
  readonly kind: "copy-v1";
}

export interface RechunkCompactionSourceBlock {
  readonly blockId: string;
  readonly rowStart: number;
  readonly rowCount: number;
  /** Full persisted block byteLength, including the envelope and stored payload. */
  readonly storedBytes: number;
  /** Uncompressed encoded payload length from the immutable block header. */
  readonly encodedBytes: number;
  readonly checksum: number;
}

export interface RechunkCompactionSourceColumn {
  readonly columnId: string;
  readonly type: SimpleDataType;
  readonly sourceBlocks: readonly RechunkCompactionSourceBlock[];
}

export interface RechunkCompactionOutputWindow {
  readonly rowStart: number;
  readonly rowCount: number;
}

export const compactionOutputCompressions = ["raw", "gzip"] as const;
export type CompactionOutputCompression = (typeof compactionOutputCompressions)[number];

export interface RechunkCompactionRewritePlan {
  readonly kind: "rechunk-v1";
  readonly targetBlockBytes: number;
  readonly outputCompression: CompactionOutputCompression;
  readonly totalRows: number;
  readonly rowIdStart: bigint;
  readonly rowIdEndExclusive: bigint;
  readonly logicalOrder: number;
  readonly columns: readonly RechunkCompactionSourceColumn[];
  /** Shared row windows, emitted in output-window-major then column order. */
  readonly outputs: readonly RechunkCompactionOutputWindow[];
  /** Optional level-one publication layout; partitions tile the output without splitting windows. */
  readonly partitions?: readonly MergeOutputPartition[];
}

export interface MergeCompactionSourceBlock {
  readonly blockId: string;
  /** Row offset within the source segment column. */
  readonly rowStart: number;
  readonly rowCount: number;
  readonly storedBytes: number;
  readonly encodedBytes: number;
  readonly checksum: number;
}

export interface MergeCompactionSourceColumn {
  readonly columnId: string;
  readonly type: SimpleDataType;
  readonly sourceBlocks: readonly MergeCompactionSourceBlock[];
}

export interface MergeCompactionSourceSegment {
  readonly segmentId: string;
  readonly transactionId: string;
  readonly committedVersion: number;
  readonly kind: SegmentKind;
  readonly keyColumnId: string | null;
  readonly level: number;
  readonly logicalOrder: number;
  readonly rowCount: number;
  readonly rowIdStart: bigint;
  readonly rowIdEndExclusive: bigint;
  readonly rowIdSpans: readonly RowIdSpan[];
  readonly columns: readonly MergeCompactionSourceColumn[];
}

export interface MergeCompactionOutputSourceRange {
  /** Row offset within the canonical merged output. */
  readonly outputRowStart: number;
  readonly sourceBlockId: string;
  /** Row offset within sourceBlockId. */
  readonly sourceRowStart: number;
  readonly rowCount: number;
}

export interface MergeCompactionOutputColumn {
  readonly columnId: string;
  readonly type: SimpleDataType;
  readonly sourceRanges: readonly MergeCompactionOutputSourceRange[];
}

/**
 * One output segment of a partitioned physical rewrite: a contiguous run of the canonical
 * output, published as its own level-one partition under its own logical order.
 */
export interface MergeOutputPartition {
  /** Row offset within the canonical merged output. */
  readonly rowStart: number;
  readonly rowCount: number;
  /** The finite, non-negative published order; strictly increasing across partitions. */
  readonly logicalOrder: number;
}

/** An immutable logical replay result followed by a physical, output-driven rewrite. */
export interface MergeCompactionRewritePlan {
  readonly kind: "merge-v1";
  readonly targetBlockBytes: number;
  readonly outputCompression: CompactionOutputCompression;
  readonly keyColumnId: string;
  readonly totalRows: number;
  /** Bounding row-ID envelope; spans preserve gaps and output order. */
  readonly rowIdStart: bigint;
  readonly rowIdEndExclusive: bigint;
  readonly rowIdSpans: readonly RowIdSpan[];
  readonly logicalOrder: number;
  readonly sourceSegments: readonly MergeCompactionSourceSegment[];
  readonly columns: readonly MergeCompactionOutputColumn[];
  readonly outputs: readonly RechunkCompactionOutputWindow[];
  /**
   * How the output is split into published segments. Missing on plans that publish the whole
   * output as one segment under `logicalOrder`. When present, the partitions tile the output
   * contiguously, every output window lies inside one partition, and partition `i` publishes
   * as the job's output segment ID for `i === 0` and `${outputSegmentId}/${i}` after that.
   */
  readonly partitions?: readonly MergeOutputPartition[];
}

export type CompactionRewritePlan =
  CopyCompactionRewritePlan | RechunkCompactionRewritePlan | MergeCompactionRewritePlan;

/**
 * The next rechunk output to emit, ordered by output window and then column. A completed cursor
 * has outputIndex === outputs.length, columnIndex zero, and rowStart === totalRows.
 */
export interface CompactionOutputCursor {
  outputIndex: number;
  columnIndex: number;
  rowStart: number;
}

export interface CompactionJobRecord {
  id: string;
  tableId: string;
  sourceManifestVersion: number;
  sourceSegmentIds: string[];
  sourceBlockIds: string[];
  outputBlockIds: string[];
  cursor: CompactionJobCursor;
  processedRows: number;
  sourceStoredBytes: number;
  outputStoredBytes: number;
  logicalBytes: number;
  /** Missing on Phase 6A records, which normalize to copy-v1. */
  readonly rewritePlan?: CompactionRewritePlan;
  /** Null for copy-v1; points at the next output for rechunk-v1. */
  outputCursor?: CompactionOutputCursor | null;
  /** Immutable execution budget. Zero for copy-v1 jobs. */
  readonly memoryBudgetBytes?: number;
  /** Immutable planner estimate. Zero for copy-v1 jobs. */
  readonly minimumMemoryBytes?: number;
  /** Immutable stored bytes from newly promoted level-zero sources. Missing on legacy jobs. */
  readonly level0SourceStoredBytes?: number;
  /** Immutable stored bytes from the retained level-one anchor. Missing on legacy jobs. */
  readonly anchorSourceStoredBytes?: number;
  /** Output partition assigned by the append-row-range L2 policy. Missing on legacy jobs. */
  readonly outputPartitionOrdinal?: number;
  /** Immutable maximum compaction output bytes per newly promoted L0 byte. */
  readonly maxWriteAmplification?: number;
  /** Immutable exact ceiling for all stored output blocks produced by this job. */
  readonly maximumOutputStoredBytes?: number;
  /** Immutable conservative full-block upper bound for the planned output. */
  readonly plannedOutputStoredBytesUpperBound?: number;
  /**
   * Immutable stored bytes already written by cancelled or aborted attempts at these same
   * sources. The persisted ceiling is reduced by this amount, so attempts share one lifetime
   * write-amplification budget.
   */
  readonly priorAttemptOutputStoredBytes?: number;
  peakWorkingBytes?: number;
  outputLogicalBytes?: number;
  targetLevel: number;
  state: CompactionJobState;
  transactionId: string | null;
  outputSegmentId: string | null;
  publishedVersion: number | null;
  revision: number;
  createdAt: string;
  updatedAt: string;
  error?: string;
}

export interface CompactionJobRecordUpdate {
  outputBlockIds?: readonly string[];
  cursor?: CompactionJobCursor;
  processedRows?: number;
  sourceStoredBytes?: number;
  outputStoredBytes?: number;
  logicalBytes?: number;
  outputCursor?: CompactionOutputCursor | null;
  peakWorkingBytes?: number;
  outputLogicalBytes?: number;
  state?: CompactionJobState;
  transactionId?: string | null;
  outputSegmentId?: string | null;
  publishedVersion?: number | null;
  updatedAt: string;
  error?: string | null;
}

export class CompactionJobConflictError extends Error {
  override readonly name = "CompactionJobConflictError";

  constructor(
    readonly jobId: string,
    readonly expectedRevision: number,
    readonly actualRevision: number | null,
  ) {
    super(
      `Compaction job ${jobId} changed: expected revision ${String(expectedRevision)}, found ${String(actualRevision)}`,
    );
  }
}

export const garbageCollectionJobStates = ["planned", "running", "completed"] as const;
export type GarbageCollectionJobState = (typeof garbageCollectionJobStates)[number];

/**
 * How far a job has examined each of its candidate lists, in the order a step works through
 * them: manifests, then segments, then blocks, then transactions. Transactions come last so a
 * segment the same job reclaims has already released the transaction that wrote it.
 */
export interface GarbageCollectionCursor {
  manifestIndex: number;
  segmentIndex: number;
  blockIndex: number;
  transactionIndex: number;
}

export interface CreateGarbageCollectionJobInput {
  id: string;
  candidateManifestVersions: readonly number[];
  candidateSegmentIds: readonly string[];
  candidateBlockIds: readonly string[];
  /**
   * Transaction records the planner believes nothing needs any more: committed records below
   * the retained window with no segment owner, or aborted records after their pending artifacts
   * are gone. Optional, so a caller that only reclaims artifacts need not mention them. The
   * store decides for itself at step time.
   */
  candidateTransactionIds?: readonly string[];
  /** Fixed cutoff used to decide which persisted leases protect a manifest for this job. */
  leaseCutoff: string;
  createdAt: string;
}

export interface GarbageCollectionJobRecord {
  id: string;
  candidateManifestVersions: number[];
  candidateSegmentIds: string[];
  candidateBlockIds: string[];
  candidateTransactionIds: string[];
  cursor: GarbageCollectionCursor;
  prunedManifestCount: number;
  alreadyPrunedManifestCount: number;
  retainedManifestCount: number;
  missingManifestCount: number;
  reclaimedSegmentCount: number;
  retainedSegmentCount: number;
  missingSegmentCount: number;
  reclaimedBlockCount: number;
  retainedBlockCount: number;
  missingBlockCount: number;
  reclaimedBlockBytes: number;
  reclaimedTransactionCount: number;
  retainedTransactionCount: number;
  missingTransactionCount: number;
  state: GarbageCollectionJobState;
  revision: number;
  leaseCutoff: string;
  createdAt: string;
  updatedAt: string;
}

export interface RunGarbageCollectionStepInput {
  jobId: string;
  expectedRevision: number;
  maxItems: number;
  updatedAt: string;
}

export interface GarbageCollectionStepResult {
  job: GarbageCollectionJobRecord;
  prunedManifestVersions: number[];
  alreadyPrunedManifestVersions: number[];
  retainedManifestVersions: number[];
  missingManifestVersions: number[];
  reclaimedSegmentIds: string[];
  retainedSegmentIds: string[];
  missingSegmentIds: string[];
  reclaimedBlockIds: string[];
  retainedBlockIds: string[];
  missingBlockIds: string[];
  reclaimedBlockBytes: number;
  reclaimedTransactionIds: string[];
  retainedTransactionIds: string[];
  missingTransactionIds: string[];
}

export interface StoragePage<T, Cursor> {
  records: T[];
  nextCursor: Cursor | null;
}

export class GarbageCollectionJobConflictError extends Error {
  override readonly name = "GarbageCollectionJobConflictError";

  constructor(
    readonly jobId: string,
    readonly expectedRevision: number,
    readonly actualRevision: number | null,
  ) {
    super(
      `Garbage collection job ${jobId} changed: expected revision ${String(expectedRevision)}, found ${String(actualRevision)}`,
    );
  }
}

export class SnapshotManifestMissingError extends Error {
  override readonly name = "SnapshotManifestMissingError";

  constructor(readonly version: number) {
    super(`Snapshot manifest is unavailable: ${String(version)}`);
  }
}

export interface GarbageCollectionStepAccounting {
  examinedManifestCount: number;
  prunedManifestCount: number;
  alreadyPrunedManifestCount: number;
  retainedManifestCount: number;
  missingManifestCount: number;
  examinedSegmentCount: number;
  reclaimedSegmentCount: number;
  retainedSegmentCount: number;
  missingSegmentCount: number;
  examinedBlockCount: number;
  reclaimedBlockCount: number;
  retainedBlockCount: number;
  missingBlockCount: number;
  reclaimedBlockBytes: number;
  examinedTransactionCount: number;
  reclaimedTransactionCount: number;
  retainedTransactionCount: number;
  missingTransactionCount: number;
  updatedAt: string;
}

export class LeaseConflictError extends Error {
  override readonly name = "LeaseConflictError";

  constructor(
    readonly leaseId: string,
    readonly expectedRevision: number,
    readonly actualRevision: number | null,
  ) {
    super(
      `Lease ${leaseId} changed: expected revision ${String(expectedRevision)}, found ${String(actualRevision)}`,
    );
  }
}

export type TransactionStatus = "active" | "committed" | "aborted";

export interface TransactionRecord {
  id: string;
  snapshotVersion: number | null;
  pendingBlockIds: string[];
  pendingSegmentIds: string[];
  status: TransactionStatus;
  revision: number;
  startedAt: string;
  updatedAt: string;
  committedVersion: number | null;
}

export interface TransactionRecordUpdate {
  snapshotVersion?: number | null;
  pendingBlockIds?: readonly string[];
  pendingSegmentIds?: readonly string[];
  status?: TransactionStatus;
  updatedAt: string;
  committedVersion?: number | null;
}

export interface BeginTransactionInput {
  /** Record to create; the store stamps `snapshotVersion` with the current manifest version. */
  record: Omit<TransactionRecord, "snapshotVersion">;
  /** Reserve this many row ids for the table in the same atomic step. */
  reserveRowIds?: { tableId: string; count: number };
  /**
   * Reserve auto-increment values for the column in the same atomic step, first bumping the
   * counter to at least `atLeast`. `count` may be 0 for a pure bump past explicit values.
   */
  reserveAutoIncrement?: { tableId: string; columnId: string; count: number; atLeast?: bigint };
}

export interface BeginTransactionResult {
  record: TransactionRecord;
  rowIds?: RowIdRange;
  autoIncrementValues?: RowIdRange;
}

export interface StageTransactionArtifactsInput {
  transactionId: string;
  expectedRevision: number;
  blocks: readonly BlockWrite[];
  segments: readonly SegmentRecord[];
  updatedAt: string;
}

/**
 * The commit input carries only the change: added blocks are the transaction's journaled pending
 * blocks, removals are the superseded ids. The store derives the published manifest from its
 * stored base, so commit cost scales with the delta rather than the database's total block count.
 */
export interface CommitTransactionInput {
  transactionId: string;
  changedTableIds?: readonly string[];
  expectedTransactionRevision: number;
  expectedManifestVersion: number | null;
  removedBlockIds?: readonly string[];
  /**
   * Per-table unique-key changes, in operation order. Multi-entry commits come from atomic
   * write scopes; entries for the same table apply sequentially, so in-scope conflicts
   * (inserting one key twice) fail exactly like cross-commit conflicts.
   */
  uniqueKeyChanges?: readonly UniqueKeyChanges[];
  /** Per-table full-text deltas; at most one entry per table. */
  ftsChanges?: readonly FtsChanges[];
  committedAt: string;
}

/**
 * The single-shot write: stage these blocks and segments and commit them, in one atomic storage
 * transaction. Carries the same commit change as `CommitTransactionInput`; the artifacts become
 * the transaction's journaled pending ids on the way through.
 */
export interface WriteTransactionInput extends Omit<
  CommitTransactionInput,
  "transactionId" | "expectedTransactionRevision"
> {
  /**
   * Which transaction publishes. `{ id, expectedRevision }` names an active transaction begun
   * earlier (typically with `beginTransaction`, when the artifacts depend on a row-id or
   * auto-increment reservation); its journal is extended and its revision compare-and-swapped
   * (`TransactionRecordConflictError`). `{ record }` begins the transaction in the same step —
   * a fresh record (revision 0, empty journal) the store pins at `expectedManifestVersion` —
   * so a write that needed no reservation costs one round trip in total.
   */
  transaction:
    | { id: string; expectedRevision: number }
    | { record: Omit<TransactionRecord, "snapshotVersion"> };
  blocks: readonly BlockWrite[];
  segments: readonly SegmentRecord[];
}

export interface UniqueKeyChanges {
  tableId: string;
  keyTokens: readonly string[];
  requireAbsent: boolean;
  remove?: boolean;
}

/** One term's postings within a commit delta or base chunk: parallel rowId/tf arrays. */
export interface FtsPosting {
  term: string;
  rowIds: bigint[];
  tf: number[];
}

/** One indexed column's contribution from one commit: postings for the commit's new rows. */
export interface FtsColumnDelta {
  columnId: string;
  /** Term-sorted postings; rowIds ascending within each term. */
  postings: FtsPosting[];
  /** Total tokens the commit's rows contribute to this column — feeds exact BM25 statistics. */
  totalTokens: number;
}

/**
 * A commit's full-text index deltas, applied atomically with the manifest publish. The store
 * also closes the stale-writer race here: a commit that adds segments to a table whose record
 * indexes a column in state "building" or "ready" without carrying that column's delta flips
 * the column to "invalid" (self-healing rebuild) rather than rejecting the data commit.
 */
export interface FtsChanges {
  tableId: string;
  columns: readonly FtsColumnDelta[];
}

/** The per-term candidate row IDs a full-text index lookup returns, aligned with the query. */
export interface FtsCandidates {
  /** Per requested term: ascending unique row IDs whose indexed column contained the term. */
  rowIdsByTerm: bigint[][];
}

/**
 * Shared candidate-merge core for both stores: fetching chunks is store-specific, but the
 * term-match rule (exact, or prefix as a term range) and the sorted-unique row-id shape must
 * never drift between backends — pruning would silently differ per store.
 */
export function collectFtsCandidates(
  chunkLists: Iterable<readonly FtsPosting[]>,
  terms: ReadonlyArray<{ term: string; prefix: boolean }>,
): FtsCandidates {
  const sets = terms.map(() => new Set<bigint>());
  for (const postings of chunkLists) {
    for (const posting of postings) {
      for (let index = 0; index < terms.length; index += 1) {
        const term = terms[index];
        if (term === undefined) continue;
        const matches = term.prefix
          ? posting.term.startsWith(term.term)
          : posting.term === term.term;
        if (!matches) continue;
        const set = sets[index];
        if (set !== undefined) for (const rowId of posting.rowIds) set.add(rowId);
      }
    }
  }
  return {
    rowIdsByTerm: sets.map((set) =>
      [...set].sort((left, right) => (left < right ? -1 : left > right ? 1 : 0)),
    ),
  };
}

/**
 * Shared stale-writer policy for both stores' commit steps: a commit that adds segments to a
 * table with active full-text columns but no covering delta flips the uncovered columns to
 * "invalid" (the index self-heals through a rebuild; the data commit itself always proceeds).
 * Returns the updated record, or undefined when nothing changes.
 */
export function invalidateUncoveredFtsColumns(
  record: TableRecord,
  coveredColumnIds: ReadonlySet<string>,
): TableRecord | undefined {
  const ftsColumns = record.ftsColumns;
  if (ftsColumns === undefined) return undefined;
  let invalidated = false;
  const next: Record<string, FtsColumnIndexRecord> = {};
  for (const [columnId, state] of Object.entries(ftsColumns)) {
    if (state.state !== "invalid" && !coveredColumnIds.has(columnId)) {
      next[columnId] = { ...state, state: "invalid" };
      invalidated = true;
    } else {
      next[columnId] = { ...state };
    }
  }
  if (!invalidated) return undefined;
  return { ...record, ftsColumns: next, revision: (record.revision ?? 0) + 1 };
}

export class UniqueKeyConflictError extends Error {
  override readonly name = "UniqueKeyConflictError";

  constructor(
    readonly tableId: string,
    readonly keyToken: string,
  ) {
    super(`Unique key already exists in table ${tableId}`);
  }
}

export class WriteConflictError extends Error {
  override readonly name = "WriteConflictError";

  constructor(
    readonly expectedVersion: number | null,
    readonly actualVersion: number | null,
  ) {
    super(`Manifest changed: expected ${String(expectedVersion)}, found ${String(actualVersion)}`);
  }
}

export class TransactionRecordConflictError extends Error {
  override readonly name = "TransactionRecordConflictError";

  constructor(
    readonly transactionId: string,
    readonly expectedRevision: number,
    readonly actualRevision: number | null,
  ) {
    super(
      `Transaction ${transactionId} changed: expected revision ${String(expectedRevision)}, found ${String(actualRevision)}`,
    );
  }
}

export interface BlockWrite {
  id: string;
  bytes: Uint8Array;
}

export interface TempRunPage {
  ownerId: string;
  runId: string;
  pageIndex: number;
  bytes: Uint8Array;
}

export interface TempOwnerRecord {
  ownerId: string;
  expiresAt: string;
  revision: number;
}

export class TempOwnerConflictError extends Error {
  override readonly name = "TempOwnerConflictError";

  constructor(
    readonly ownerId: string,
    readonly expectedRevision: number,
    readonly actualRevision: number | null,
  ) {
    super(
      `Temp owner ${ownerId} changed: expected revision ${String(expectedRevision)}, found ${String(actualRevision)}`,
    );
  }
}

/**
 * One coherent read of everything query preparation needs before touching blocks: the
 * current manifest version, the named table records, every segment of the found tables,
 * and the transaction records those segments reference. Stores that can produce this in
 * one atomic read collapse the sequential per-record round trips a prepare would
 * otherwise issue.
 */
export interface QueryCatalogState {
  manifestVersion: number | null;
  /** Positional per requested name; undefined where the table does not exist. */
  tables: Array<TableRecord | undefined>;
  /** Segments of the found tables, sorted by id like listSegments. */
  segments: SegmentRecord[];
  /** Records for the segments' transaction ids; missing records are omitted. */
  transactions: TransactionRecord[];
  /**
   * The catalog epoch this state was read at, read in the same atomic storage transaction.
   * Present when the store maintains an epoch (see `getCatalogProbe`); callers may cache the
   * state and reuse it while a probe returns the same epoch.
   */
  catalogEpoch?: number;
}

/**
 * The two change counters a reader needs to know whether anything it may have cached is
 * still current, read together in one atomic storage transaction. `manifestVersion` moves on
 * every data commit. `catalogEpoch` moves on every catalog mutation — table creation, table
 * record updates (schema migration, full-text index stamps), and every manifest publish —
 * so an unchanged epoch proves cached catalog state is byte-identical to a fresh read.
 * Physical garbage collection does not move the epoch: it only deletes records that are
 * already invisible at every leased version, so cached state stays result-equivalent.
 */
export interface CatalogProbe {
  manifestVersion: number | null;
  catalogEpoch: number;
}

/**
 * Bulk payload storage: immutable, opaque byte blobs keyed by structured ids.
 *
 * Blocks are write-once. `addBlock`/`addBlocks` MUST reject an id that already exists, and a
 * batch containing any duplicate MUST write nothing at all. Ids contain `/` separators
 * (`table/<uuid>/segment/<uuid>/part/000001`) and sort lexically; treat them as opaque keys.
 * Reads MUST return bytes the caller may mutate freely (a fresh copy or freshly deserialized
 * buffer), and writes MUST NOT alias the caller's buffer — the engine may reuse it.
 *
 * Published blocks are retired by superseding them in a commit, never by `removeBlock`; the
 * lease-aware collector deletes the bytes once no reader can be pinned to them.
 */
export interface BlockPayloadStore {
  addBlock(id: string, bytes: Uint8Array): Promise<void>;
  /** All or nothing: an internal or existing duplicate id fails the whole batch unwritten. */
  addBlocks(blocks: readonly BlockWrite[]): Promise<void>;
  getBlock(id: string): Promise<Uint8Array | undefined>;
  /** Positional per requested id; undefined where a block does not exist. */
  getBlocks(ids: readonly string[]): Promise<Array<Uint8Array | undefined>>;
  /** Deleting a missing block is not an error. */
  removeBlock(id: string): Promise<void>;
  /** Every stored block id, sorted lexically. A cold path — tools and tests, not queries. */
  listBlockIds(): Promise<string[]>;
}

/**
 * The table catalog: schema records, the counters that keep writes collision-free, and
 * unique-key membership.
 *
 * Table mutations are compare-and-swap on the record's `revision` and MUST fail with
 * `TableRecordConflictError` — the exact exported class — on a mismatch. Catalog mutations
 * advance the catalog epoch (see `CatalogProbe`). Counter reservations (`reserveRowIds`,
 * `reserveAutoIncrement`) MUST be atomic and durable: two racing callers may never receive
 * overlapping ranges, across connections and across crashes. Reservations are never returned;
 * aborted transactions leave gaps.
 */
export interface CatalogStore {
  /** Fails on a duplicate id or name. Advances the catalog epoch. */
  addTable(record: TableRecord): Promise<void>;
  getTable(id: string): Promise<TableRecord | undefined>;
  getTableByName(name: string): Promise<TableRecord | undefined>;
  /** Sorted by table name. */
  listTables(): Promise<TableRecord[]>;
  /**
   * Replaces catalog metadata atomically. When `columns` removes a column, the same operation
   * must also discard that column's full-text catalog entry, base chunks, and commit deltas.
   */
  updateTable(
    id: string,
    expectedRevision: number,
    update: {
      columns?: TableColumnRecord[];
      /** Replaces the full-text index state map; null clears it. */
      ftsColumns?: Record<string, FtsColumnIndexRecord> | null;
      /** Replaces the trigger list; null clears it. */
      triggers?: TriggerRecord[] | null;
    },
  ): Promise<TableRecord>;
  /**
   * Removes a table's catalog record together with everything else keyed to it: its segments,
   * its full-text base chunks and commit deltas, its unique-key membership, and its row-id and
   * autoincrement counters. One step, so a crash cannot leave a segment pointing at a table
   * that no longer exists. Advances the catalog epoch, and fails with a
   * `TableRecordConflictError` on a revision mismatch, like `updateTable`.
   *
   * The table's blocks are the caller's business: they are retired by superseding them in a
   * commit, which leaves the bytes for the lease-aware collector rather than deleting data a
   * pinned reader may still be reading.
   */
  removeTable(id: string, expectedRevision: number): Promise<void>;
  reserveRowIds(tableId: string, count: number): Promise<RowIdRange>;
  /**
   * Atomically reserves `count` auto-increment values for the column, first bumping the
   * counter to at least `atLeast`. `count` may be 0 for a pure bump past explicit values.
   */
  reserveAutoIncrement(
    tableId: string,
    columnId: string,
    count: number,
    atLeast?: bigint,
  ): Promise<RowIdRange>;
  /** Which of the given key tokens already exist for the table, deduplicated and sorted. */
  getExistingUniqueKeys(tableId: string, keyTokens: readonly string[]): Promise<string[]>;
}

/**
 * Versions and visibility: manifests (the set of live block ids at each version), segments
 * (which blocks belong to which table and rows), and the transaction records that stage and
 * publish them.
 *
 * This is where the whole consistency story lives. `commitTransaction` is THE atomic step of
 * the database: in one durable, all-or-nothing action it validates the transaction record's
 * revision and active status, compare-and-swaps the current manifest version, publishes the
 * next manifest, finalizes the transaction's segments, applies unique-key changes (failing
 * with `UniqueKeyConflictError` on a `requireAbsent` violation), applies full-text deltas,
 * and flips the transaction record to committed. No intermediate state may ever be
 * observable, including after a crash at any moment. Version conflicts MUST be
 * `WriteConflictError` and revision conflicts `TransactionRecordConflictError` — the exact
 * exported classes; the engine's retry and rebase loops match on them, and the worker client
 * rehydrates them by name across the thread boundary.
 */
export interface TransactionStore {
  getCurrentManifest(): Promise<Manifest | undefined>;
  /** The current version alone, without materializing the manifest's block list. */
  getCurrentManifestVersion(): Promise<number | null>;
  getManifest(version: number): Promise<Manifest | undefined>;
  listManifests(): Promise<Manifest[]>;
  listManifestPage(
    afterVersion: number | null,
    limit: number,
  ): Promise<StoragePage<Manifest, number>>;
  /**
   * Publishes the next version directly from a full block-id list, compare-and-swapping on
   * `expectedVersion` (`WriteConflictError` on a mismatch). Every id must exist. The engine
   * commits through transactions instead; this is the lower-level tool underneath.
   */
  publishManifest(input: PublishManifestInput): Promise<Manifest>;
  /** Fails on a duplicate id; the record's snapshot version and pending ids must be valid. */
  createTransaction(record: TransactionRecord): Promise<void>;
  getTransaction(id: string): Promise<TransactionRecord | undefined>;
  /** Positional per requested id; undefined where a record does not exist. */
  getTransactions(ids: readonly string[]): Promise<Array<TransactionRecord | undefined>>;
  /** Sorted by startedAt, then id. */
  listTransactions(): Promise<TransactionRecord[]>;
  listTransactionPage(
    afterId: string | null,
    limit: number,
  ): Promise<StoragePage<TransactionRecord, string>>;
  /**
   * Compare-and-swap on `expectedRevision` (`TransactionRecordConflictError` on a mismatch).
   * Only active transactions may be updated, and only `commitTransaction` may mark one
   * committed.
   */
  updateTransaction(
    id: string,
    expectedRevision: number,
    update: TransactionRecordUpdate,
  ): Promise<TransactionRecord>;
  /** Publishes the next version; the summary omits the block list, which commits never need. */
  commitTransaction(input: CommitTransactionInput): Promise<ManifestSummary>;
  addSegment(record: SegmentRecord): Promise<void>;
  getSegment(id: string): Promise<SegmentRecord | undefined>;
  /** Sorted by id; `tableId` filters. */
  listSegments(tableId?: string): Promise<SegmentRecord[]>;
  /** Sorted by id, after the exclusive cursor; bounded for maintenance scans. */
  listSegmentPage(
    afterId: string | null,
    limit: number,
  ): Promise<StoragePage<SegmentRecord, string>>;
  removeSegment(id: string): Promise<void>;
}

/**
 * Reader pins. A lease is a stored record with an expiry that protects one manifest version
 * (and every block it references) from garbage collection while a reader may still be using
 * it. Renewals are compare-and-swap on `revision` and fail with `LeaseConflictError`; an
 * expired lease simply stops protecting — there is no callback, which is what makes dead
 * tabs safe.
 */
export interface LeaseStore {
  /** Fails on a duplicate id, or when the pinned version's manifest is unavailable. */
  createLease(record: LeaseRecord): Promise<void>;
  getLease(id: string): Promise<LeaseRecord | undefined>;
  /** Sorted by id. */
  listLeases(): Promise<LeaseRecord[]>;
  renewLease(id: string, expectedRevision: number, expiresAt: string): Promise<LeaseRecord>;
  /** True when removed; false (without removing) when the lease has not yet expired. */
  removeLeaseIfExpired(
    id: string,
    expectedRevision: number,
    expiresAtCutoff: string,
  ): Promise<boolean>;
  removeLease(id: string): Promise<void>;
}

/**
 * Background-maintenance bookkeeping: the resumable job records that let compaction and
 * garbage collection survive a tab being closed, throttled, or killed mid-step. Job updates
 * are compare-and-swap on `revision` and fail with `CompactionJobConflictError` /
 * `GarbageCollectionJobConflictError`.
 *
 * `runGarbageCollectionStep` does real deletion and MUST be atomic: it re-verifies lease and
 * transaction pins inside the same storage transaction that prunes manifests and deletes
 * segments and blocks, advancing the job's cursors so an interrupted collection resumes
 * rather than restarts. Reclaimed manifests are first tombstoned (`prunedAt`);
 * `removePrunedManifestRecords` may then delete only the old prefix no readable delta chain
 * needs.
 */
export interface MaintenanceStore {
  createCompactionJob(record: CompactionJobRecord): Promise<void>;
  getCompactionJob(id: string): Promise<CompactionJobRecord | undefined>;
  /** Sorted by createdAt, then id; `tableId` filters. */
  listCompactionJobs(tableId?: string): Promise<CompactionJobRecord[]>;
  listCompactionJobPage(
    afterId: string | null,
    limit: number,
  ): Promise<StoragePage<CompactionJobRecord, string>>;
  updateCompactionJob(
    id: string,
    expectedRevision: number,
    update: CompactionJobRecordUpdate,
  ): Promise<CompactionJobRecord>;
  /**
   * Resolves a job that may be racing its own publication: already-terminal jobs return
   * unchanged, a job whose transaction committed is marked published, anything else is
   * cancelled and its active transaction aborted — atomically.
   */
  cancelCompactionJob(
    id: string,
    expectedRevision: number,
    cancelledAt: string,
  ): Promise<CompactionJobRecord>;
  removeCompactionJob(id: string): Promise<void>;
  /** Validates candidate provenance against persisted records before accepting the job. */
  createGarbageCollectionJob(
    input: CreateGarbageCollectionJobInput,
  ): Promise<GarbageCollectionJobRecord>;
  getGarbageCollectionJob(id: string): Promise<GarbageCollectionJobRecord | undefined>;
  /** Sorted by createdAt, then id. */
  listGarbageCollectionJobs(): Promise<GarbageCollectionJobRecord[]>;
  runGarbageCollectionStep(
    input: RunGarbageCollectionStepInput,
  ): Promise<GarbageCollectionStepResult>;
  /**
   * Deletes obsolete tombstones only after their garbage blocks are gone, while retaining the
   * checkpoint prefix readable deltas need. A tombstone is the collector's durable discovery
   * record between bounded passes.
   */
  removePrunedManifestRecords(): Promise<number>;
  removeGarbageCollectionJob(id: string): Promise<void>;
}

/**
 * Full-text index persistence: per-column base chunks plus the per-commit deltas that
 * `commitTransaction` applies. The index is a pruning accelerator the scan re-verifies, so
 * losing a base costs a rebuild, never a wrong answer — which is why snapshots may restore
 * indexed columns as `invalid`.
 */
export interface FtsIndexStore {
  /** Removes every base chunk and commit delta owned by one column. */
  removeFtsColumn(tableId: string, columnId: string): Promise<void>;
  /**
   * Replaces one column's full-text base chunks (term-range partitioned, term-sorted within
   * each chunk) and deletes commit deltas the new base covers. The caller flips the catalog
   * state separately via updateTable; orphaned chunks from a lost race are overwritten by the
   * next build.
   */
  writeFtsBase(
    tableId: string,
    columnId: string,
    input: { coversVersion: number; chunks: FtsPosting[][]; totalTokens: number },
  ): Promise<void>;
  /**
   * Per-term candidate row IDs from the base chunks plus every commit delta at or below
   * `upToVersion`, with the column's merged token total for exact BM25 statistics. Prefix
   * terms match the term range [term, term + "\uffff"). Reports the merged delta-chunk count
   * so callers can schedule a rebuild when the tail grows, and the base's covered version —
   * a concurrent rebuild can publish a base ahead of a reader's snapshot, and a caller
   * needing snapshot-exact statistics must detect `coversVersion > upToVersion` and fall
   * back (candidates stay a safe superset either way).
   */
  readFtsCandidates(
    tableId: string,
    columnId: string,
    terms: ReadonlyArray<{ term: string; prefix: boolean }>,
    upToVersion: number,
  ): Promise<
    FtsCandidates & { deltaChunkCount: number; totalTokens: number; coversVersion: number }
  >;
}

/**
 * Query spill: scratch pages a bounded-memory query writes when it exceeds its budget, plus
 * the owner leases that let any connection reclaim a dead owner's pages. Pages carry no
 * durability requirement whatsoever — losing them costs a query, never data — but owner
 * records are real records with the usual compare-and-swap (`TempOwnerConflictError`).
 */
export interface TempSpillStore {
  putTempRunPage(page: TempRunPage): Promise<void>;
  /**
   * Optional: writes a batch of pages in one storage round trip. Callers fall back to
   * per-page writes when absent; implement it where per-call overhead is real (the IndexedDB
   * adapter pays one transaction per page otherwise).
   */
  putTempRunPages?(pages: readonly TempRunPage[]): Promise<void>;
  getTempRunPage(
    ownerId: string,
    runId: string,
    pageIndex: number,
  ): Promise<Uint8Array | undefined>;
  removeTempRun(ownerId: string, runId: string): Promise<void>;
  /** Removes the owner record and every page under the owner. */
  removeTempOwner(ownerId: string): Promise<void>;
  createTempOwner(record: TempOwnerRecord): Promise<void>;
  getTempOwner(ownerId: string): Promise<TempOwnerRecord | undefined>;
  renewTempOwner(
    ownerId: string,
    expectedRevision: number,
    expiresAt: string,
  ): Promise<TempOwnerRecord>;
  /** Sweeps pages too when it removes; owners found only via orphaned pages count as expired. */
  removeTempOwnerIfExpired(ownerId: string, expiresAtCutoff: string): Promise<boolean>;
  /** Owner ids from records and from orphaned pages alike, deduplicated, sorted, paged. */
  listTempOwnerIdsPage(
    afterOwnerId: string | null,
    limit: number,
  ): Promise<StoragePage<string, string>>;
}

/**
 * The complete storage contract: a database is `MinnowDatabase` plus one implementation of
 * this interface. The engine holds exactly one and talks to nothing else persistent, so
 * implementing it against a new substrate — React Native storage, an object store like R2,
 * the Node filesystem — yields a working database with no engine changes. The capability
 * interfaces above split the surface by concern; implement all of them (this type), and see
 * `/docs/storage/custom` for the guide and `runBlockStoreConformance` from
 * `@minnowdb/core/testing` for the referee.
 *
 * The rules every implementation must honor — the conformance kit checks each of them:
 *
 * - **Atomicity.** Every method is all-or-nothing, including after a crash at any moment.
 *   `commitTransaction` and `runGarbageCollectionStep` mutate several record families in one
 *   durable step. A method that resolves has happened; a method that rejects has not
 *   (observably) happened.
 * - **Conflicts are typed, by exact class.** Compare-and-swap failures throw the exported
 *   error classes (`WriteConflictError`, `TransactionRecordConflictError`,
 *   `TableRecordConflictError`, `LeaseConflictError`, `CompactionJobConflictError`,
 *   `GarbageCollectionJobConflictError`, `TempOwnerConflictError`, `UniqueKeyConflictError`,
 *   `SnapshotManifestMissingError`) — not subclasses, not wrappers. The engine's rebase loops
 *   match on them and the worker client rehydrates them by constructor name.
 * - **Platform errors pass through.** A quota refusal must escape as the browser's own
 *   `QuotaExceededError` `DOMException`, unwrapped, with everything committed beforehand
 *   intact and the same write succeeding once space frees — no reopen, no repair step.
 * - **Nothing is shared.** Returned records and bytes must be safe for the caller to mutate;
 *   received records and bytes must be copied or serialized before the call resolves.
 * - **Deterministic ordering.** List methods sort as documented on each capability interface;
 *   pagination cursors are stable under concurrent writes.
 * - **Optional means atomic.** The optional methods exist so an adapter that can do something
 *   in one atomic step may say so; callers trust a present method completely and fall back to
 *   the sequential calls when it is absent. Never implement one as the sequential calls in a
 *   trench coat.
 * - **Multiple connections are normal.** Several instances (tabs) may open one database.
 *   Readers must never block writers; competing writers must resolve through the typed
 *   conflicts. How is the adapter's business — storage transactions, a write-ahead log behind
 *   a leader, anything that keeps these rules true.
 * - **Records carry no adapter fields.** These types are the whole vocabulary between engine
 *   and store. Anything an adapter needs to remember about its own layout — key partitioning,
 *   format generations, file placements — lives in the adapter's own storage space, keyed
 *   however it likes, never as extra fields on the records it hands back.
 * - **Bigints are data.** Row ids, counters, and full-text posting ids are `bigint`; an
 *   adapter that serializes records needs an encoding for them.
 */
export interface BlockStore
  extends
    BlockPayloadStore,
    CatalogStore,
    TransactionStore,
    LeaseStore,
    MaintenanceStore,
    FtsIndexStore,
    TempSpillStore {
  /**
   * Optional: the current manifest version and catalog epoch in one atomic read. This is the
   * freshness probe: an unchanged pair proves any cached catalog state is still exactly what
   * a fresh read would return. Callers that find this absent must not cache catalog state.
   */
  getCatalogProbe?(): Promise<CatalogProbe>;
  /**
   * Optional: one atomic catalog read for query preparation. Implementations must return
   * the same records the individual getTableByName/listSegments/getTransactions calls
   * would; callers fall back to those calls when this is absent.
   */
  getQueryCatalogState?(tableNames: readonly string[]): Promise<QueryCatalogState>;
  /**
   * Optional: reads the current manifest version, creates the transaction record pinned to it,
   * and optionally reserves row ids, all in one atomic storage transaction — one round trip
   * instead of three. Callers fall back to the individual calls when this is absent.
   */
  beginTransaction?(input: BeginTransactionInput): Promise<BeginTransactionResult>;
  /**
   * Optional: stages blocks and segments and journals them on the transaction record in one
   * atomic storage transaction. Must be equivalent to addBlocks + addSegment(s) + one
   * updateTransaction appending the new ids, with no intermediate state observable after a
   * crash. Callers fall back to those calls when this is absent.
   */
  stageTransactionArtifacts?(input: StageTransactionArtifactsInput): Promise<TransactionRecord>;
  /**
   * Optional: the single-shot write — begin (or continue) a transaction, stage its blocks and
   * segments, and commit, all in one atomic storage transaction. Must be exactly equivalent to
   * `stageTransactionArtifacts` followed by `commitTransaction` (preceded by `createTransaction`
   * at `expectedManifestVersion` when the input carries a fresh record): the same validation,
   * the same typed conflicts (`WriteConflictError`, `TransactionRecordConflictError`,
   * `UniqueKeyConflictError`), the same finalized records afterwards — and nothing at all
   * written when any part refuses, including the fresh record. This is what lets a simple
   * write cost one durable storage commit instead of three; callers fall back to the sequence
   * when it is absent.
   */
  writeTransaction?(input: WriteTransactionInput): Promise<ManifestSummary>;
  /**
   * Optional: re-pins a lease to another manifest version and renews it, in one atomic step —
   * `createLease` at the new version plus `removeLease` of the old pin, as one round trip that
   * keeps the record and its id. Compare-and-swap on `expectedRevision` (`LeaseConflictError`);
   * the target version's manifest must be available (`SnapshotManifestMissingError`), and a
   * refused move leaves the lease exactly as it was. The engine uses it to carry its shared
   * reader pin forward after each commit; callers fall back to create + remove when absent.
   */
  moveLease?(
    id: string,
    expectedRevision: number,
    manifestVersion: number | null,
    expiresAt: string,
  ): Promise<LeaseRecord>;
  /**
   * Optional: one committed version copied out as a portable snapshot — see
   * `/docs/storage/snapshots` for what it carries, drops, and guarantees. A store without it
   * still works; `MinnowDatabase` reports the capability as missing rather than failing.
   */
  exportSnapshot?(): Promise<DatabaseSnapshot>;
  /**
   * Optional: loads a snapshot into this store, which must be empty. Pairs with
   * `exportSnapshot` — implement both or neither.
   */
  importSnapshot?(
    snapshot: DatabaseSnapshot,
    options?: { onProgress?: (progress: SnapshotLoadProgress) => void },
  ): Promise<void>;
  /**
   * Optional: what this database's data occupies in its substrate, in bytes — the number an
   * application shows a user next to the quota, and what the benchmarks report.
   */
  getLogicalStorageBytes?(): Promise<number>;
  /**
   * Releases whatever the connection holds (open handles, channels, timers) without flushing
   * or deleting anything. Synchronous; safe to call twice. Data durability must never depend
   * on close being called — tabs die without warning.
   */
  close(): void;
}

export function createManifest(input: PublishManifestInput): Manifest {
  return {
    version: input.expectedVersion === null ? 0 : input.expectedVersion + 1,
    previousVersion: input.expectedVersion,
    blockIds: [...new Set(input.blockIds)].sort(),
    createdAt: input.createdAt ?? new Date().toISOString(),
    ...(input.changedTableIds === undefined ? {} : { changedTableIds: [...input.changedTableIds] }),
  };
}

/**
 * Normalizes the additive L2 partition metadata while preserving legacy segment records verbatim.
 */
export function normalizeSegmentRecord(record: SegmentRecord): SegmentRecord {
  if (record.partitionOrdinal === undefined) return structuredClone(record);

  const partitionOrdinal = nonNegativeWholeNumber(
    record.partitionOrdinal,
    "Segment partition ordinal",
  );
  if (record.level !== 2) {
    throw new TypeError("A partitioned segment must have explicit level two");
  }
  const kind = record.kind ?? "insert";
  if (kind !== "insert" && kind !== "base") {
    throw new TypeError("A partitioned segment must be an insert or a merged base");
  }
  if (record.logicalOrder === undefined) {
    throw new TypeError("A partitioned segment requires an explicit logical order");
  }
  nonNegativeFiniteNumber(record.logicalOrder, "Segment logical order");
  const rowCount = positiveWholeNumber(record.rowCount, "Segment row count");
  if (kind === "insert") {
    // Append-row-range partition: one contiguous positive row-ID interval, no spans.
    if (record.rowIdSpans !== undefined) {
      throw new TypeError("A partitioned segment cannot contain row ID spans");
    }
    if (typeof record.rowIdStart !== "bigint" || record.rowIdStart <= 0n) {
      throw new RangeError("Segment row ID start must be a positive bigint");
    }
    if (
      typeof record.rowIdEndExclusive !== "bigint" ||
      record.rowIdEndExclusive !== record.rowIdStart + BigInt(rowCount)
    ) {
      throw new RangeError("A partitioned segment must have a contiguous positive row ID envelope");
    }
    return structuredClone({ ...record, partitionOrdinal });
  }
  // Keyed multi-range partition: a merged full-row base whose live rows keep their original
  // ids, described by positive, sorted, non-overlapping spans that sum to the row count.
  if (record.rowIdSpans === undefined || record.rowIdSpans.length === 0) {
    throw new TypeError("A merged partitioned segment requires row ID spans");
  }
  let spanRows = 0;
  let previousEnd = 0n;
  for (const span of record.rowIdSpans) {
    if (
      typeof span.rowIdStart !== "bigint" ||
      span.rowIdStart <= 0n ||
      !Number.isSafeInteger(span.rowCount) ||
      span.rowCount <= 0
    ) {
      throw new RangeError("A partitioned segment span must be a positive non-empty interval");
    }
    if (span.rowIdStart < previousEnd) {
      throw new RangeError("Partitioned segment spans must be sorted and non-overlapping");
    }
    previousEnd = span.rowIdStart + BigInt(span.rowCount);
    spanRows += span.rowCount;
  }
  if (spanRows !== rowCount) {
    throw new RangeError("Partitioned segment spans must cover exactly the row count");
  }
  return structuredClone({ ...record, partitionOrdinal });
}

export function updateTransactionRecord(
  record: TransactionRecord,
  update: TransactionRecordUpdate,
): TransactionRecord {
  return {
    ...record,
    ...(update.snapshotVersion === undefined ? {} : { snapshotVersion: update.snapshotVersion }),
    ...(update.pendingBlockIds === undefined
      ? {}
      : { pendingBlockIds: [...new Set(update.pendingBlockIds)].sort() }),
    ...(update.pendingSegmentIds === undefined
      ? {}
      : { pendingSegmentIds: [...new Set(update.pendingSegmentIds)].sort() }),
    ...(update.status === undefined ? {} : { status: update.status }),
    ...(update.committedVersion === undefined ? {} : { committedVersion: update.committedVersion }),
    updatedAt: update.updatedAt,
    revision: record.revision + 1,
  };
}

export function createGarbageCollectionJobRecord(
  input: CreateGarbageCollectionJobInput,
): GarbageCollectionJobRecord {
  const candidateManifestVersions = uniqueWholeNumbers(
    input.candidateManifestVersions,
    "Garbage collection candidate manifest version",
  );
  const candidateSegmentIds = uniqueIds(
    input.candidateSegmentIds,
    "Garbage collection candidate segment ID",
    true,
  );
  const candidateBlockIds = uniqueIds(
    input.candidateBlockIds,
    "Garbage collection candidate block ID",
    true,
  );
  const candidateTransactionIds = uniqueIds(
    input.candidateTransactionIds ?? [],
    "Garbage collection candidate transaction ID",
    true,
  );
  const createdAt = validTimestamp(input.createdAt, "Garbage collection creation timestamp");
  const complete =
    candidateManifestVersions.length === 0 &&
    candidateSegmentIds.length === 0 &&
    candidateBlockIds.length === 0 &&
    candidateTransactionIds.length === 0;
  return {
    id: nonEmptyString(input.id, "Garbage collection job ID"),
    candidateManifestVersions,
    candidateSegmentIds,
    candidateBlockIds,
    candidateTransactionIds,
    cursor: { manifestIndex: 0, segmentIndex: 0, blockIndex: 0, transactionIndex: 0 },
    prunedManifestCount: 0,
    alreadyPrunedManifestCount: 0,
    retainedManifestCount: 0,
    missingManifestCount: 0,
    reclaimedSegmentCount: 0,
    retainedSegmentCount: 0,
    missingSegmentCount: 0,
    reclaimedBlockCount: 0,
    retainedBlockCount: 0,
    missingBlockCount: 0,
    reclaimedBlockBytes: 0,
    reclaimedTransactionCount: 0,
    retainedTransactionCount: 0,
    missingTransactionCount: 0,
    state: complete ? "completed" : "planned",
    revision: 0,
    leaseCutoff: validTimestamp(input.leaseCutoff, "Garbage collection lease cutoff"),
    createdAt,
    updatedAt: createdAt,
  };
}

export function normalizeGarbageCollectionJobRecord(
  record: GarbageCollectionJobRecord,
): GarbageCollectionJobRecord {
  const legacy = record as Partial<GarbageCollectionJobRecord>;
  const candidateManifestVersions = uniqueWholeNumbers(
    record.candidateManifestVersions,
    "Garbage collection candidate manifest version",
  );
  const candidateSegmentIds = uniqueIds(
    record.candidateSegmentIds,
    "Garbage collection candidate segment ID",
    true,
  );
  const candidateBlockIds = uniqueIds(
    record.candidateBlockIds,
    "Garbage collection candidate block ID",
    true,
  );
  // These fields were added after durable jobs shipped. Missing values are the empty fourth
  // phase, which lets an old planned/running job resume after an upgrade without migration.
  const candidateTransactionIds = uniqueIds(
    legacy.candidateTransactionIds ?? [],
    "Garbage collection candidate transaction ID",
    true,
  );
  const cursor = normalizeGarbageCollectionCursor(record.cursor);
  const normalized: GarbageCollectionJobRecord = {
    ...record,
    id: nonEmptyString(record.id, "Garbage collection job ID"),
    candidateManifestVersions,
    candidateSegmentIds,
    candidateBlockIds,
    candidateTransactionIds,
    cursor,
    prunedManifestCount: nonNegativeWholeNumber(
      record.prunedManifestCount,
      "Garbage collection pruned manifest count",
    ),
    alreadyPrunedManifestCount: nonNegativeWholeNumber(
      record.alreadyPrunedManifestCount,
      "Garbage collection already-pruned manifest count",
    ),
    retainedManifestCount: nonNegativeWholeNumber(
      record.retainedManifestCount,
      "Garbage collection retained manifest count",
    ),
    missingManifestCount: nonNegativeWholeNumber(
      record.missingManifestCount,
      "Garbage collection missing manifest count",
    ),
    reclaimedSegmentCount: nonNegativeWholeNumber(
      record.reclaimedSegmentCount,
      "Garbage collection reclaimed segment count",
    ),
    retainedSegmentCount: nonNegativeWholeNumber(
      record.retainedSegmentCount,
      "Garbage collection retained segment count",
    ),
    missingSegmentCount: nonNegativeWholeNumber(
      record.missingSegmentCount,
      "Garbage collection missing segment count",
    ),
    reclaimedBlockCount: nonNegativeWholeNumber(
      record.reclaimedBlockCount,
      "Garbage collection reclaimed block count",
    ),
    retainedBlockCount: nonNegativeWholeNumber(
      record.retainedBlockCount,
      "Garbage collection retained block count",
    ),
    missingBlockCount: nonNegativeWholeNumber(
      record.missingBlockCount,
      "Garbage collection missing block count",
    ),
    reclaimedBlockBytes: nonNegativeWholeNumber(
      record.reclaimedBlockBytes,
      "Garbage collection reclaimed block bytes",
    ),
    reclaimedTransactionCount: nonNegativeWholeNumber(
      legacy.reclaimedTransactionCount ?? 0,
      "Garbage collection reclaimed transaction count",
    ),
    retainedTransactionCount: nonNegativeWholeNumber(
      legacy.retainedTransactionCount ?? 0,
      "Garbage collection retained transaction count",
    ),
    missingTransactionCount: nonNegativeWholeNumber(
      legacy.missingTransactionCount ?? 0,
      "Garbage collection missing transaction count",
    ),
    state: garbageCollectionJobState(record.state),
    revision: nonNegativeWholeNumber(record.revision, "Garbage collection job revision"),
    leaseCutoff: validTimestamp(record.leaseCutoff, "Garbage collection lease cutoff"),
    createdAt: validTimestamp(record.createdAt, "Garbage collection creation timestamp"),
    updatedAt: validTimestamp(record.updatedAt, "Garbage collection update timestamp"),
  };
  if (
    safeSum(
      [
        normalized.prunedManifestCount,
        normalized.alreadyPrunedManifestCount,
        normalized.retainedManifestCount,
        normalized.missingManifestCount,
      ],
      "Garbage collection examined manifest count",
    ) !== cursor.manifestIndex ||
    safeSum(
      [
        normalized.reclaimedSegmentCount,
        normalized.retainedSegmentCount,
        normalized.missingSegmentCount,
      ],
      "Garbage collection examined segment count",
    ) !== cursor.segmentIndex ||
    safeSum(
      [normalized.reclaimedBlockCount, normalized.retainedBlockCount, normalized.missingBlockCount],
      "Garbage collection examined block count",
    ) !== cursor.blockIndex ||
    safeSum(
      [
        normalized.reclaimedTransactionCount,
        normalized.retainedTransactionCount,
        normalized.missingTransactionCount,
      ],
      "Garbage collection examined transaction count",
    ) !== cursor.transactionIndex
  ) {
    throw new TypeError("Garbage collection cursor does not match its persisted accounting");
  }
  if (
    cursor.manifestIndex > candidateManifestVersions.length ||
    cursor.segmentIndex > candidateSegmentIds.length ||
    cursor.blockIndex > candidateBlockIds.length ||
    cursor.transactionIndex > candidateTransactionIds.length
  ) {
    throw new RangeError("Garbage collection cursor is outside its candidate selection");
  }
  const complete = garbageCollectionJobComplete(normalized);
  if ((normalized.state === "completed") !== complete) {
    throw new TypeError(
      complete
        ? "A finished garbage collection cursor requires completed state"
        : "A completed garbage collection job requires a finished cursor",
    );
  }
  if (
    normalized.state === "planned" &&
    (cursor.manifestIndex !== 0 ||
      cursor.segmentIndex !== 0 ||
      cursor.blockIndex !== 0 ||
      cursor.transactionIndex !== 0)
  ) {
    throw new TypeError("A planned garbage collection job cannot contain progress");
  }
  return structuredClone(normalized);
}

export function advanceGarbageCollectionJobRecord(
  record: GarbageCollectionJobRecord,
  accounting: GarbageCollectionStepAccounting,
): GarbageCollectionJobRecord {
  const current = normalizeGarbageCollectionJobRecord(record);
  if (current.state === "completed") return current;
  const increments = {
    examinedManifestCount: nonNegativeWholeNumber(
      accounting.examinedManifestCount,
      "Garbage collection examined manifest increment",
    ),
    prunedManifestCount: nonNegativeWholeNumber(
      accounting.prunedManifestCount,
      "Garbage collection pruned manifest increment",
    ),
    alreadyPrunedManifestCount: nonNegativeWholeNumber(
      accounting.alreadyPrunedManifestCount,
      "Garbage collection already-pruned manifest increment",
    ),
    retainedManifestCount: nonNegativeWholeNumber(
      accounting.retainedManifestCount,
      "Garbage collection retained manifest increment",
    ),
    missingManifestCount: nonNegativeWholeNumber(
      accounting.missingManifestCount,
      "Garbage collection missing manifest increment",
    ),
    examinedSegmentCount: nonNegativeWholeNumber(
      accounting.examinedSegmentCount,
      "Garbage collection examined segment increment",
    ),
    reclaimedSegmentCount: nonNegativeWholeNumber(
      accounting.reclaimedSegmentCount,
      "Garbage collection reclaimed segment increment",
    ),
    retainedSegmentCount: nonNegativeWholeNumber(
      accounting.retainedSegmentCount,
      "Garbage collection retained segment increment",
    ),
    missingSegmentCount: nonNegativeWholeNumber(
      accounting.missingSegmentCount,
      "Garbage collection missing segment increment",
    ),
    examinedBlockCount: nonNegativeWholeNumber(
      accounting.examinedBlockCount,
      "Garbage collection examined block increment",
    ),
    reclaimedBlockCount: nonNegativeWholeNumber(
      accounting.reclaimedBlockCount,
      "Garbage collection reclaimed block increment",
    ),
    retainedBlockCount: nonNegativeWholeNumber(
      accounting.retainedBlockCount,
      "Garbage collection retained block increment",
    ),
    missingBlockCount: nonNegativeWholeNumber(
      accounting.missingBlockCount,
      "Garbage collection missing block increment",
    ),
    reclaimedBlockBytes: nonNegativeWholeNumber(
      accounting.reclaimedBlockBytes,
      "Garbage collection reclaimed block byte increment",
    ),
    examinedTransactionCount: nonNegativeWholeNumber(
      accounting.examinedTransactionCount,
      "Garbage collection examined transaction increment",
    ),
    reclaimedTransactionCount: nonNegativeWholeNumber(
      accounting.reclaimedTransactionCount,
      "Garbage collection reclaimed transaction increment",
    ),
    retainedTransactionCount: nonNegativeWholeNumber(
      accounting.retainedTransactionCount,
      "Garbage collection retained transaction increment",
    ),
    missingTransactionCount: nonNegativeWholeNumber(
      accounting.missingTransactionCount,
      "Garbage collection missing transaction increment",
    ),
  };
  if (
    increments.examinedManifestCount !==
      safeSum(
        [
          increments.prunedManifestCount,
          increments.alreadyPrunedManifestCount,
          increments.retainedManifestCount,
          increments.missingManifestCount,
        ],
        "Garbage collection manifest increment",
      ) ||
    increments.examinedSegmentCount !==
      safeSum(
        [
          increments.reclaimedSegmentCount,
          increments.retainedSegmentCount,
          increments.missingSegmentCount,
        ],
        "Garbage collection segment increment",
      ) ||
    increments.examinedBlockCount !==
      safeSum(
        [
          increments.reclaimedBlockCount,
          increments.retainedBlockCount,
          increments.missingBlockCount,
        ],
        "Garbage collection block increment",
      ) ||
    increments.examinedTransactionCount !==
      safeSum(
        [
          increments.reclaimedTransactionCount,
          increments.retainedTransactionCount,
          increments.missingTransactionCount,
        ],
        "Garbage collection transaction increment",
      )
  ) {
    throw new TypeError("Garbage collection step accounting is incomplete");
  }
  const cursor: GarbageCollectionCursor = {
    manifestIndex: safeSum(
      [current.cursor.manifestIndex, increments.examinedManifestCount],
      "Garbage collection manifest cursor",
    ),
    segmentIndex: safeSum(
      [current.cursor.segmentIndex, increments.examinedSegmentCount],
      "Garbage collection segment cursor",
    ),
    blockIndex: safeSum(
      [current.cursor.blockIndex, increments.examinedBlockCount],
      "Garbage collection block cursor",
    ),
    transactionIndex: safeSum(
      [current.cursor.transactionIndex, increments.examinedTransactionCount],
      "Garbage collection transaction cursor",
    ),
  };
  const updated: GarbageCollectionJobRecord = {
    ...current,
    cursor,
    prunedManifestCount: safeSum(
      [current.prunedManifestCount, increments.prunedManifestCount],
      "Garbage collection pruned manifest count",
    ),
    alreadyPrunedManifestCount: safeSum(
      [current.alreadyPrunedManifestCount, increments.alreadyPrunedManifestCount],
      "Garbage collection already-pruned manifest count",
    ),
    retainedManifestCount: safeSum(
      [current.retainedManifestCount, increments.retainedManifestCount],
      "Garbage collection retained manifest count",
    ),
    missingManifestCount: safeSum(
      [current.missingManifestCount, increments.missingManifestCount],
      "Garbage collection missing manifest count",
    ),
    reclaimedSegmentCount: safeSum(
      [current.reclaimedSegmentCount, increments.reclaimedSegmentCount],
      "Garbage collection reclaimed segment count",
    ),
    retainedSegmentCount: safeSum(
      [current.retainedSegmentCount, increments.retainedSegmentCount],
      "Garbage collection retained segment count",
    ),
    missingSegmentCount: safeSum(
      [current.missingSegmentCount, increments.missingSegmentCount],
      "Garbage collection missing segment count",
    ),
    reclaimedBlockCount: safeSum(
      [current.reclaimedBlockCount, increments.reclaimedBlockCount],
      "Garbage collection reclaimed block count",
    ),
    retainedBlockCount: safeSum(
      [current.retainedBlockCount, increments.retainedBlockCount],
      "Garbage collection retained block count",
    ),
    missingBlockCount: safeSum(
      [current.missingBlockCount, increments.missingBlockCount],
      "Garbage collection missing block count",
    ),
    reclaimedBlockBytes: safeSum(
      [current.reclaimedBlockBytes, increments.reclaimedBlockBytes],
      "Garbage collection reclaimed block bytes",
    ),
    reclaimedTransactionCount: safeSum(
      [current.reclaimedTransactionCount, increments.reclaimedTransactionCount],
      "Garbage collection reclaimed transaction count",
    ),
    retainedTransactionCount: safeSum(
      [current.retainedTransactionCount, increments.retainedTransactionCount],
      "Garbage collection retained transaction count",
    ),
    missingTransactionCount: safeSum(
      [current.missingTransactionCount, increments.missingTransactionCount],
      "Garbage collection missing transaction count",
    ),
    state: "running",
    revision: current.revision + 1,
    updatedAt: validTimestamp(accounting.updatedAt, "Garbage collection update timestamp"),
  };
  if (garbageCollectionJobComplete(updated)) updated.state = "completed";
  return normalizeGarbageCollectionJobRecord(updated);
}

export function normalizeCompactionJobRecord(record: CompactionJobRecord): CompactionJobRecord {
  const error: unknown = record.error;
  if (error !== undefined && typeof error !== "string") {
    throw new TypeError("Compaction job error must be a string");
  }
  const rewritePlan = normalizeCompactionRewritePlan(record.rewritePlan);
  const logicalBytes = nonNegativeWholeNumber(record.logicalBytes, "Compaction logical bytes");
  const sourceStoredBytes = nonNegativeWholeNumber(
    record.sourceStoredBytes,
    "Compaction source stored bytes",
  );
  const outputStoredBytes = nonNegativeWholeNumber(
    record.outputStoredBytes,
    "Compaction output stored bytes",
  );
  const hasLevel0SourceStoredBytes = record.level0SourceStoredBytes !== undefined;
  const hasAnchorSourceStoredBytes = record.anchorSourceStoredBytes !== undefined;
  if (hasLevel0SourceStoredBytes !== hasAnchorSourceStoredBytes) {
    throw new TypeError("Compaction source-level byte accounting requires both stored byte fields");
  }
  const sourceLevelStoredBytes = hasLevel0SourceStoredBytes
    ? {
        level0SourceStoredBytes: positiveWholeNumber(
          record.level0SourceStoredBytes,
          "Compaction level-zero source stored bytes",
        ),
        anchorSourceStoredBytes: nonNegativeWholeNumber(
          record.anchorSourceStoredBytes,
          "Compaction anchor source stored bytes",
        ),
      }
    : undefined;
  if (
    sourceLevelStoredBytes !== undefined &&
    safeSum(
      [
        sourceLevelStoredBytes.level0SourceStoredBytes,
        sourceLevelStoredBytes.anchorSourceStoredBytes,
      ],
      "Compaction source-level stored bytes",
    ) !== sourceStoredBytes
  ) {
    throw new TypeError("Compaction source-level stored bytes must equal source stored bytes");
  }
  const level2PolicyValues = [
    record.outputPartitionOrdinal,
    record.maxWriteAmplification,
    record.maximumOutputStoredBytes,
    record.plannedOutputStoredBytesUpperBound,
  ];
  const level2PolicyFieldCount = level2PolicyValues.filter((value) => value !== undefined).length;
  if (level2PolicyFieldCount !== 0 && level2PolicyFieldCount !== level2PolicyValues.length) {
    throw new TypeError("Append-row-range L2 compaction policy fields must be present together");
  }
  if (record.priorAttemptOutputStoredBytes !== undefined && level2PolicyFieldCount === 0) {
    throw new TypeError(
      "Compaction prior-attempt accounting requires the L2 compaction policy fields",
    );
  }
  let level2Policy:
    | Pick<
        CompactionJobRecord,
        | "outputPartitionOrdinal"
        | "maxWriteAmplification"
        | "maximumOutputStoredBytes"
        | "plannedOutputStoredBytesUpperBound"
        | "priorAttemptOutputStoredBytes"
      >
    | undefined;
  if (level2PolicyFieldCount !== 0) {
    if (rewritePlan.kind !== "rechunk-v1" && rewritePlan.kind !== "merge-v1") {
      throw new TypeError("L2 compaction requires a rechunk or merge plan");
    }
    if (record.targetLevel !== 2) {
      throw new TypeError("Append-row-range L2 compaction must target level two");
    }
    // Append-row-range promotions consume pure level-zero prefixes; keyed merge promotions may
    // also fold a retained level-one anchor, whose bytes never count toward the L0 ceiling.
    if (
      rewritePlan.kind === "rechunk-v1" &&
      (sourceLevelStoredBytes?.level0SourceStoredBytes !== sourceStoredBytes ||
        sourceLevelStoredBytes.anchorSourceStoredBytes !== 0)
    ) {
      throw new TypeError("Append-row-range L2 compaction requires only level-zero source bytes");
    }
    if (rewritePlan.kind === "merge-v1" && sourceLevelStoredBytes === undefined) {
      throw new TypeError("Keyed L2 compaction requires source-level byte accounting");
    }
    const outputPartitionOrdinal = nonNegativeWholeNumber(
      record.outputPartitionOrdinal,
      "Compaction output partition ordinal",
    );
    const maxWriteAmplification = positiveFiniteNumber(
      record.maxWriteAmplification,
      "Compaction maximum write amplification",
    );
    const maximumOutputStoredBytes = positiveWholeNumber(
      record.maximumOutputStoredBytes,
      "Compaction maximum output stored bytes",
    );
    const plannedOutputStoredBytesUpperBound = positiveWholeNumber(
      record.plannedOutputStoredBytesUpperBound,
      "Compaction planned output stored byte upper bound",
    );
    const priorAttemptOutputStoredBytes =
      record.priorAttemptOutputStoredBytes === undefined
        ? undefined
        : nonNegativeWholeNumber(
            record.priorAttemptOutputStoredBytes,
            "Compaction prior-attempt output stored bytes",
          );
    const amplificationCeiling = floorWholeNumberProduct(
      sourceStoredBytes,
      maxWriteAmplification,
      "Compaction write amplification product",
    );
    // The persisted ceiling plus everything failed attempts already wrote must stay within
    // the amplification limit — attempts share one lifetime budget.
    if (maximumOutputStoredBytes + (priorAttemptOutputStoredBytes ?? 0) > amplificationCeiling) {
      throw new RangeError("Compaction output stored byte ceiling exceeds its amplification limit");
    }
    if (plannedOutputStoredBytesUpperBound > maximumOutputStoredBytes) {
      throw new RangeError("Compaction planned output exceeds its stored byte ceiling");
    }
    if (outputStoredBytes > plannedOutputStoredBytesUpperBound) {
      throw new RangeError("Compaction output stored bytes exceed their planned upper bound");
    }
    level2Policy = {
      outputPartitionOrdinal,
      maxWriteAmplification,
      maximumOutputStoredBytes,
      plannedOutputStoredBytesUpperBound,
      ...(priorAttemptOutputStoredBytes === undefined ? {} : { priorAttemptOutputStoredBytes }),
    };
  }
  const normalized: CompactionJobRecord = {
    ...record,
    id: nonEmptyString(record.id, "Compaction job ID"),
    tableId: nonEmptyString(record.tableId, "Compaction job table ID"),
    sourceManifestVersion: nonNegativeWholeNumber(
      record.sourceManifestVersion,
      "Compaction source manifest version",
    ),
    sourceSegmentIds:
      rewritePlan.kind === "copy-v1"
        ? uniqueIds(record.sourceSegmentIds, "Compaction source segment ID", false)
        : orderedUniqueIds(record.sourceSegmentIds, "Compaction source segment ID"),
    sourceBlockIds:
      rewritePlan.kind === "copy-v1"
        ? uniqueIds(record.sourceBlockIds, "Compaction source block ID", true)
        : orderedUniqueIds(record.sourceBlockIds, "Compaction source block ID").sort(),
    outputBlockIds:
      rewritePlan.kind === "copy-v1"
        ? uniqueIds(record.outputBlockIds, "Compaction output block ID", true)
        : orderedUniqueIds(record.outputBlockIds, "Compaction output block ID"),
    cursor: normalizeCompactionJobCursor(record.cursor),
    processedRows: nonNegativeWholeNumber(record.processedRows, "Compaction processed row count"),
    sourceStoredBytes,
    outputStoredBytes,
    logicalBytes,
    rewritePlan,
    outputCursor: normalizeCompactionOutputCursor(record.outputCursor, rewritePlan),
    memoryBudgetBytes: nonNegativeWholeNumber(
      record.memoryBudgetBytes ?? 0,
      "Compaction memory budget",
    ),
    minimumMemoryBytes: nonNegativeWholeNumber(
      record.minimumMemoryBytes ?? 0,
      "Compaction minimum memory",
    ),
    ...(sourceLevelStoredBytes ?? {}),
    ...(level2Policy ?? {}),
    peakWorkingBytes: nonNegativeWholeNumber(
      record.peakWorkingBytes ?? 0,
      "Compaction peak working bytes",
    ),
    outputLogicalBytes: nonNegativeWholeNumber(
      record.outputLogicalBytes ?? (rewritePlan.kind === "copy-v1" ? logicalBytes : 0),
      "Compaction output logical bytes",
    ),
    targetLevel: nonNegativeWholeNumber(record.targetLevel, "Compaction target level"),
    state: compactionJobState(record.state),
    transactionId: nullableId(record.transactionId, "Compaction transaction ID"),
    outputSegmentId: nullableId(record.outputSegmentId, "Compaction output segment ID"),
    publishedVersion:
      record.publishedVersion === null
        ? null
        : nonNegativeWholeNumber(record.publishedVersion, "Compaction published version"),
    revision: nonNegativeWholeNumber(record.revision, "Compaction job revision"),
    createdAt: nonEmptyString(record.createdAt, "Compaction creation timestamp"),
    updatedAt: nonEmptyString(record.updatedAt, "Compaction update timestamp"),
  };
  if (normalized.sourceSegmentIds.length === 0) {
    throw new TypeError("Compaction requires at least one source segment");
  }
  if (normalized.cursor.sourceSegmentIndex > normalized.sourceSegmentIds.length) {
    throw new RangeError("Compaction source segment cursor is outside the source selection");
  }
  if (
    normalized.cursor.sourceSegmentIndex === normalized.sourceSegmentIds.length &&
    normalized.cursor.sourceBlockIndex !== 0
  ) {
    throw new RangeError("A completed compaction cursor must start at block zero");
  }
  validateCompactionRewrite(normalized);
  validateCompactionJobState(normalized);
  return structuredClone(normalized);
}

export function updateCompactionJobRecord(
  record: CompactionJobRecord,
  update: CompactionJobRecordUpdate,
): CompactionJobRecord {
  const current = normalizeCompactionJobRecord(record);
  for (const field of [
    "rewritePlan",
    "memoryBudgetBytes",
    "minimumMemoryBytes",
    "level0SourceStoredBytes",
    "anchorSourceStoredBytes",
    "outputPartitionOrdinal",
    "maxWriteAmplification",
    "maximumOutputStoredBytes",
    "plannedOutputStoredBytesUpperBound",
    "priorAttemptOutputStoredBytes",
  ] as const) {
    if (Reflect.has(update, field)) {
      throw new TypeError(`Compaction ${field} is immutable`);
    }
  }
  if (isOutputDrivenCompactionPlan(current.rewritePlan)) {
    for (const field of ["cursor", "sourceStoredBytes", "logicalBytes"] as const) {
      if (Reflect.has(update, field)) {
        throw new TypeError(`Output-driven compaction ${field} is immutable`);
      }
    }
  }
  const mirrorCopyLogicalBytes =
    current.rewritePlan?.kind === "copy-v1" &&
    update.logicalBytes !== undefined &&
    update.outputLogicalBytes === undefined;
  const updated: CompactionJobRecord = {
    ...current,
    ...(update.outputBlockIds === undefined ? {} : { outputBlockIds: [...update.outputBlockIds] }),
    ...(update.cursor === undefined ? {} : { cursor: update.cursor }),
    ...(update.processedRows === undefined ? {} : { processedRows: update.processedRows }),
    ...(update.sourceStoredBytes === undefined
      ? {}
      : { sourceStoredBytes: update.sourceStoredBytes }),
    ...(update.outputStoredBytes === undefined
      ? {}
      : { outputStoredBytes: update.outputStoredBytes }),
    ...(update.logicalBytes === undefined ? {} : { logicalBytes: update.logicalBytes }),
    ...(update.outputCursor === undefined ? {} : { outputCursor: update.outputCursor }),
    ...(update.peakWorkingBytes === undefined ? {} : { peakWorkingBytes: update.peakWorkingBytes }),
    ...(update.outputLogicalBytes === undefined
      ? mirrorCopyLogicalBytes
        ? { outputLogicalBytes: update.logicalBytes }
        : {}
      : { outputLogicalBytes: update.outputLogicalBytes }),
    ...(update.state === undefined ? {} : { state: update.state }),
    ...(update.transactionId === undefined ? {} : { transactionId: update.transactionId }),
    ...(update.outputSegmentId === undefined ? {} : { outputSegmentId: update.outputSegmentId }),
    ...(update.publishedVersion === undefined ? {} : { publishedVersion: update.publishedVersion }),
    updatedAt: update.updatedAt,
    revision: current.revision + 1,
  };
  if (update.error === null) delete updated.error;
  else if (update.error !== undefined) updated.error = update.error;
  const normalized = normalizeCompactionJobRecord(updated);
  validateCompactionJobTransition(current.state, normalized.state);
  validateCompactionJobProgress(current, normalized);
  return normalized;
}

function validateCompactionJobState(record: CompactionJobRecord): void {
  const plan = record.rewritePlan ?? { kind: "copy-v1" };
  if (plan.kind === "merge-v1") {
    if (plan.totalRows === 0 && record.outputSegmentId !== null) {
      throw new TypeError("An empty merge compaction cannot have an output segment");
    }
    if (plan.totalRows > 0 && record.outputSegmentId === null) {
      throw new TypeError("A non-empty merge compaction requires an output segment ID");
    }
  }
  if (record.state === "cancelled" && record.error !== undefined) {
    throw new TypeError("A cancelled compaction cannot contain an error");
  }
  if (record.state === "planned") {
    const isCopy = record.rewritePlan?.kind === "copy-v1";
    const hasProgress =
      record.cursor.sourceSegmentIndex !== 0 ||
      record.cursor.sourceBlockIndex !== 0 ||
      record.outputBlockIds.length !== 0 ||
      record.processedRows !== 0 ||
      (isCopy && record.sourceStoredBytes !== 0) ||
      record.outputStoredBytes !== 0 ||
      (isCopy && record.logicalBytes !== 0) ||
      record.outputLogicalBytes !== 0 ||
      record.peakWorkingBytes !== 0;
    if (hasProgress || record.transactionId !== null || !isInitialOutputCursor(record)) {
      throw new TypeError("A planned compaction cannot contain transaction progress");
    }
  }
  if (record.state === "running" && record.transactionId === null) {
    throw new TypeError("A running compaction requires a transaction ID");
  }
  if (record.state === "ready" || record.state === "published") {
    if (
      record.transactionId === null ||
      (record.outputSegmentId === null && !(plan.kind === "merge-v1" && plan.totalRows === 0))
    ) {
      throw new TypeError(`${record.state} compaction requires its transaction and output segment`);
    }
    if (!hasCompletedCompactionCursor(record)) {
      throw new TypeError(`${record.state} compaction requires a completed cursor`);
    }
    if (record.outputBlockIds.length !== expectedCompactionOutputCount(record)) {
      throw new TypeError(`${record.state} compaction requires every output block`);
    }
    if (
      isOutputDrivenCompactionPlan(record.rewritePlan) &&
      expectedCompactionOutputCount(record) > 0 &&
      (record.peakWorkingBytes ?? 0) < (record.minimumMemoryBytes ?? 0)
    ) {
      throw new TypeError(`${record.state} compaction requires complete memory accounting`);
    }
  }
  if (record.state === "published") {
    if (record.publishedVersion === null) {
      throw new TypeError("A published compaction requires a manifest version");
    }
  } else if (record.publishedVersion !== null) {
    throw new TypeError("Only a published compaction can have a manifest version");
  }
}

function validateCompactionRewrite(record: CompactionJobRecord): void {
  const plan = record.rewritePlan ?? { kind: "copy-v1" };
  const outputLogicalBytes = record.outputLogicalBytes ?? 0;
  const memoryBudgetBytes = record.memoryBudgetBytes ?? 0;
  const minimumMemoryBytes = record.minimumMemoryBytes ?? 0;
  const peakWorkingBytes = record.peakWorkingBytes ?? 0;
  if (plan.kind === "copy-v1") {
    if (record.outputCursor !== null) {
      throw new TypeError("A copy compaction cannot have an output cursor");
    }
    if (memoryBudgetBytes !== 0 || minimumMemoryBytes !== 0 || peakWorkingBytes !== 0) {
      throw new TypeError("A copy compaction cannot have rechunk memory accounting");
    }
    if (outputLogicalBytes !== record.logicalBytes) {
      throw new TypeError("A copy compaction must preserve its logical byte count");
    }
    if (record.outputBlockIds.length > record.sourceBlockIds.length) {
      throw new RangeError(
        "Compaction output cannot contain more blocks than its source selection",
      );
    }
    return;
  }

  if (record.cursor.sourceSegmentIndex !== 0 || record.cursor.sourceBlockIndex !== 0) {
    throw new TypeError("An output-driven compaction does not use the source cursor");
  }
  const permitsZeroMinimum = plan.kind === "merge-v1" && plan.totalRows === 0;
  if (memoryBudgetBytes === 0 || (minimumMemoryBytes === 0 && !permitsZeroMinimum)) {
    throw new RangeError("An output-driven compaction requires a memory budget and minimum memory");
  }
  if (minimumMemoryBytes > memoryBudgetBytes) {
    throw new RangeError("Compaction minimum memory exceeds its memory budget");
  }
  if (peakWorkingBytes > memoryBudgetBytes) {
    throw new RangeError("Compaction peak working bytes exceed its memory budget");
  }

  const plannedBlocks =
    plan.kind === "rechunk-v1"
      ? plan.columns.flatMap((column) => column.sourceBlocks)
      : plan.sourceSegments.flatMap((segment) =>
          segment.columns.flatMap((column) => column.sourceBlocks),
        );
  const plannedBlockIds = plannedBlocks.map((block) => block.blockId);
  if (new Set(plannedBlockIds).size !== plannedBlockIds.length) {
    throw new TypeError("A planned source block can only appear once in its source layout");
  }
  const sortedPlannedIds = [...plannedBlockIds].sort();
  if (
    sortedPlannedIds.length !== record.sourceBlockIds.length ||
    sortedPlannedIds.some((id, index) => id !== record.sourceBlockIds[index])
  ) {
    throw new TypeError("The rewrite source layout must describe every selected source block");
  }
  const plannedStoredBytes = safeSum(
    plannedBlocks.map((block) => block.storedBytes),
    "Rechunk source stored bytes",
  );
  const plannedEncodedBytes = safeSum(
    plannedBlocks.map((block) => block.encodedBytes),
    "Rechunk source encoded bytes",
  );
  if (record.sourceStoredBytes !== plannedStoredBytes) {
    throw new TypeError("Source stored bytes must match the immutable rewrite layout");
  }
  if (record.logicalBytes !== plannedEncodedBytes) {
    throw new TypeError("Logical bytes must match the immutable rewrite layout");
  }

  if (plan.kind === "merge-v1") {
    if (
      plan.totalRows === 0 &&
      (record.outputBlockIds.length !== 0 ||
        record.outputStoredBytes !== 0 ||
        outputLogicalBytes !== 0 ||
        peakWorkingBytes !== 0 ||
        minimumMemoryBytes !== 0)
    ) {
      throw new TypeError("An empty merge compaction cannot contain physical output progress");
    }
    const plannedSegmentIds = plan.sourceSegments.map((segment) => segment.segmentId);
    if (
      plannedSegmentIds.length !== record.sourceSegmentIds.length ||
      plannedSegmentIds.some((id, index) => id !== record.sourceSegmentIds[index])
    ) {
      throw new TypeError(
        "Merge source layout must preserve every selected source segment in order",
      );
    }
  }

  const cursor = record.outputCursor;
  if (cursor === null || cursor === undefined) {
    throw new TypeError("An output-driven compaction requires an output cursor");
  }
  const completedOutputs = safeSum(
    [
      safeProduct(cursor.outputIndex, plan.columns.length, "Rechunk output cursor"),
      cursor.columnIndex,
    ],
    "Rechunk output cursor",
  );
  if (record.outputBlockIds.length !== completedOutputs) {
    throw new TypeError("Compaction output IDs must match the output cursor");
  }
  const expectedProcessedRows =
    cursor.outputIndex === plan.outputs.length
      ? plan.totalRows
      : (plan.outputs[cursor.outputIndex]?.rowStart ?? 0);
  if (record.processedRows !== expectedProcessedRows) {
    throw new TypeError("Processed rows must match completed output windows");
  }
}

function validateCompactionJobProgress(
  previous: CompactionJobRecord,
  next: CompactionJobRecord,
): void {
  for (const [label, previousValue, nextValue] of [
    ["processed rows", previous.processedRows, next.processedRows],
    ["source stored bytes", previous.sourceStoredBytes, next.sourceStoredBytes],
    ["output stored bytes", previous.outputStoredBytes, next.outputStoredBytes],
    ["logical bytes", previous.logicalBytes, next.logicalBytes],
    ["output logical bytes", previous.outputLogicalBytes ?? 0, next.outputLogicalBytes ?? 0],
    ["peak working bytes", previous.peakWorkingBytes ?? 0, next.peakWorkingBytes ?? 0],
  ] as const) {
    if (nextValue < previousValue) {
      throw new RangeError(`Compaction ${label} cannot decrease`);
    }
  }

  if (next.outputBlockIds.length < previous.outputBlockIds.length) {
    throw new TypeError("Compaction output block IDs cannot be removed");
  }
  if (isOutputDrivenCompactionPlan(previous.rewritePlan)) {
    if (previous.outputBlockIds.some((id, index) => next.outputBlockIds[index] !== id)) {
      throw new TypeError("Output block IDs are an append-only ordered checkpoint");
    }
    if (compactionOutputOrdinal(next) < compactionOutputOrdinal(previous)) {
      throw new RangeError("Output cursor cannot move backwards");
    }
    if (
      previous.rewritePlan.kind === "merge-v1" &&
      previous.outputSegmentId !== next.outputSegmentId
    ) {
      throw new TypeError("Merge output segment ID is immutable");
    }
  } else {
    const nextIds = new Set(next.outputBlockIds);
    if (previous.outputBlockIds.some((id) => !nextIds.has(id))) {
      throw new TypeError("Compaction output block IDs cannot be removed");
    }
    const previousCursor = previous.cursor;
    const nextCursor = next.cursor;
    if (
      nextCursor.sourceSegmentIndex < previousCursor.sourceSegmentIndex ||
      (nextCursor.sourceSegmentIndex === previousCursor.sourceSegmentIndex &&
        nextCursor.sourceBlockIndex < previousCursor.sourceBlockIndex)
    ) {
      throw new RangeError("Compaction source cursor cannot move backwards");
    }
  }
}

function isInitialOutputCursor(record: CompactionJobRecord): boolean {
  const plan = record.rewritePlan ?? { kind: "copy-v1" };
  if (plan.kind === "copy-v1") return record.outputCursor === null;
  const cursor = record.outputCursor;
  const initialRowStart = plan.outputs[0]?.rowStart ?? plan.totalRows;
  return (
    cursor?.outputIndex === 0 && cursor.columnIndex === 0 && cursor.rowStart === initialRowStart
  );
}

function hasCompletedCompactionCursor(record: CompactionJobRecord): boolean {
  const plan = record.rewritePlan ?? { kind: "copy-v1" };
  if (plan.kind === "copy-v1") {
    return record.cursor.sourceSegmentIndex === record.sourceSegmentIds.length;
  }
  const cursor = record.outputCursor;
  return (
    cursor?.outputIndex === plan.outputs.length &&
    cursor.columnIndex === 0 &&
    cursor.rowStart === plan.totalRows
  );
}

function expectedCompactionOutputCount(record: CompactionJobRecord): number {
  const plan = record.rewritePlan ?? { kind: "copy-v1" };
  return plan.kind === "copy-v1"
    ? record.sourceBlockIds.length
    : safeProduct(plan.outputs.length, plan.columns.length, "Rechunk output block count");
}

function compactionOutputOrdinal(record: CompactionJobRecord): number {
  const plan = record.rewritePlan;
  const cursor = record.outputCursor;
  if (!isOutputDrivenCompactionPlan(plan) || cursor === null || cursor === undefined) return 0;
  return safeSum(
    [
      safeProduct(cursor.outputIndex, plan.columns.length, "Rechunk output cursor"),
      cursor.columnIndex,
    ],
    "Rechunk output cursor",
  );
}

function isOutputDrivenCompactionPlan(
  plan: CompactionRewritePlan | undefined,
): plan is RechunkCompactionRewritePlan | MergeCompactionRewritePlan {
  return plan?.kind === "rechunk-v1" || plan?.kind === "merge-v1";
}

function validateCompactionJobTransition(
  previous: CompactionJobState,
  next: CompactionJobState,
): void {
  const allowed: Record<CompactionJobState, readonly CompactionJobState[]> = {
    planned: ["planned", "running", "cancelled", "aborted"],
    running: ["running", "ready", "published", "cancelled", "aborted"],
    ready: ["ready", "running", "published", "cancelled", "aborted"],
    published: ["published"],
    cancelled: ["cancelled"],
    aborted: ["aborted"],
  };
  if (!allowed[previous].includes(next)) {
    throw new TypeError(`Invalid compaction state transition: ${previous} to ${next}`);
  }
}

function normalizeCompactionJobCursor(value: unknown): CompactionJobCursor {
  if (typeof value !== "object" || value === null) {
    throw new TypeError("Compaction cursor must be an object");
  }
  return {
    sourceSegmentIndex: nonNegativeWholeNumber(
      Reflect.get(value, "sourceSegmentIndex"),
      "Compaction source segment cursor",
    ),
    sourceBlockIndex: nonNegativeWholeNumber(
      Reflect.get(value, "sourceBlockIndex"),
      "Compaction source block cursor",
    ),
  };
}

function normalizeCompactionRewritePlan(value: unknown): CompactionRewritePlan {
  if (value === undefined) return { kind: "copy-v1" };
  if (typeof value !== "object" || value === null) {
    throw new TypeError("Compaction rewrite plan must be an object");
  }
  const kind: unknown = Reflect.get(value, "kind");
  if (kind === "copy-v1") return { kind: "copy-v1" };
  if (kind === "merge-v1") return normalizeMergeCompactionRewritePlan(value);
  if (kind !== "rechunk-v1") {
    throw new TypeError(`Invalid compaction rewrite plan: ${String(kind)}`);
  }

  const totalRows = positiveWholeNumber(Reflect.get(value, "totalRows"), "Rechunk total row count");
  const rowIdStart = nonNegativeBigInt(Reflect.get(value, "rowIdStart"), "Rechunk row ID start");
  const rowIdEndExclusive = nonNegativeBigInt(
    Reflect.get(value, "rowIdEndExclusive"),
    "Rechunk row ID end",
  );
  if (rowIdEndExclusive - rowIdStart !== BigInt(totalRows)) {
    throw new RangeError("Rechunk row ID range must match its total row count");
  }
  const columnsValue: unknown = Reflect.get(value, "columns");
  if (!Array.isArray(columnsValue) || columnsValue.length === 0) {
    throw new TypeError("A rechunk plan requires at least one source column");
  }
  const columns = columnsValue.map((column, index) =>
    normalizeRechunkSourceColumn(column, totalRows, index),
  );
  const columnIds = columns.map((column) => column.columnId);
  if (new Set(columnIds).size !== columnIds.length) {
    throw new TypeError("A rechunk plan cannot contain duplicate source columns");
  }

  const outputsValue: unknown = Reflect.get(value, "outputs");
  if (!Array.isArray(outputsValue) || outputsValue.length === 0) {
    throw new TypeError("A rechunk plan requires at least one output window");
  }
  const outputs = outputsValue.map((output, index) => {
    if (typeof output !== "object" || output === null) {
      throw new TypeError(`Rechunk output window ${String(index)} must be an object`);
    }
    return {
      rowStart: nonNegativeWholeNumber(
        Reflect.get(output, "rowStart"),
        `Rechunk output window ${String(index)} row start`,
      ),
      rowCount: positiveUint32(
        Reflect.get(output, "rowCount"),
        `Rechunk output window ${String(index)} row count`,
      ),
    };
  });
  validateContiguousRows(outputs, totalRows, "Rechunk output windows");
  const partitionsValue: unknown = Reflect.get(value, "partitions");
  const partitions =
    partitionsValue === undefined
      ? undefined
      : normalizeMergeOutputPartitions(partitionsValue, totalRows, outputs);

  return {
    kind: "rechunk-v1",
    targetBlockBytes: positiveWholeNumber(
      Reflect.get(value, "targetBlockBytes"),
      "Rechunk target block bytes",
    ),
    outputCompression: compactionOutputCompression(Reflect.get(value, "outputCompression")),
    totalRows,
    rowIdStart,
    rowIdEndExclusive,
    logicalOrder: nonNegativeFiniteNumber(
      Reflect.get(value, "logicalOrder"),
      "Rechunk logical order",
    ),
    columns,
    outputs,
    ...(partitions === undefined ? {} : { partitions }),
  };
}

function normalizeRechunkSourceColumn(
  value: unknown,
  totalRows: number,
  columnIndex: number,
): RechunkCompactionSourceColumn {
  if (typeof value !== "object" || value === null) {
    throw new TypeError(`Rechunk source column ${String(columnIndex)} must be an object`);
  }
  const sourceBlocksValue: unknown = Reflect.get(value, "sourceBlocks");
  if (!Array.isArray(sourceBlocksValue) || sourceBlocksValue.length === 0) {
    throw new TypeError(`Rechunk source column ${String(columnIndex)} requires source blocks`);
  }
  const sourceBlocks = sourceBlocksValue.map((block, blockIndex) => {
    if (typeof block !== "object" || block === null) {
      throw new TypeError(
        `Rechunk source block ${String(columnIndex)}:${String(blockIndex)} must be an object`,
      );
    }
    return {
      blockId: nonEmptyString(
        Reflect.get(block, "blockId"),
        `Rechunk source block ${String(columnIndex)}:${String(blockIndex)} ID`,
      ),
      rowStart: nonNegativeWholeNumber(
        Reflect.get(block, "rowStart"),
        `Rechunk source block ${String(columnIndex)}:${String(blockIndex)} row start`,
      ),
      rowCount: positiveUint32(
        Reflect.get(block, "rowCount"),
        `Rechunk source block ${String(columnIndex)}:${String(blockIndex)} row count`,
      ),
      storedBytes: positiveWholeNumber(
        Reflect.get(block, "storedBytes"),
        `Rechunk source block ${String(columnIndex)}:${String(blockIndex)} stored bytes`,
      ),
      encodedBytes: nonNegativeWholeNumber(
        Reflect.get(block, "encodedBytes"),
        `Rechunk source block ${String(columnIndex)}:${String(blockIndex)} encoded bytes`,
      ),
      checksum: uint32(
        Reflect.get(block, "checksum"),
        `Rechunk source block ${String(columnIndex)}:${String(blockIndex)} checksum`,
      ),
    };
  });
  validateContiguousRows(
    sourceBlocks,
    totalRows,
    `Rechunk source column ${String(columnIndex)} blocks`,
  );
  return {
    columnId: nonEmptyString(
      Reflect.get(value, "columnId"),
      `Rechunk source column ${String(columnIndex)} ID`,
    ),
    type: simpleDataType(Reflect.get(value, "type")),
    sourceBlocks,
  };
}

function normalizeMergeCompactionRewritePlan(value: object): MergeCompactionRewritePlan {
  const totalRows = nonNegativeWholeNumber(
    Reflect.get(value, "totalRows"),
    "Merge total row count",
  );
  const rowIdStart = nonNegativeBigInt(Reflect.get(value, "rowIdStart"), "Merge row ID start");
  const rowIdEndExclusive = nonNegativeBigInt(
    Reflect.get(value, "rowIdEndExclusive"),
    "Merge row ID end",
  );
  const rowIdSpans = normalizeRowIdSpans(
    Reflect.get(value, "rowIdSpans"),
    totalRows,
    rowIdStart,
    rowIdEndExclusive,
    "Merge output row ID spans",
  );
  const keyColumnId = nonEmptyString(Reflect.get(value, "keyColumnId"), "Merge key column ID");

  const sourceSegmentsValue: unknown = Reflect.get(value, "sourceSegments");
  if (!Array.isArray(sourceSegmentsValue) || sourceSegmentsValue.length === 0) {
    throw new TypeError("A merge plan requires at least one source segment");
  }
  const sourceSegments = sourceSegmentsValue.map((segment, index) =>
    normalizeMergeSourceSegment(segment, index),
  );
  const sourceSegmentIds = sourceSegments.map((segment) => segment.segmentId);
  if (new Set(sourceSegmentIds).size !== sourceSegmentIds.length) {
    throw new TypeError("A merge source segment can only appear once");
  }
  for (let index = 1; index < sourceSegments.length; index += 1) {
    const previous = sourceSegments[index - 1];
    const current = sourceSegments[index];
    if (
      previous !== undefined &&
      current !== undefined &&
      compareMergeSourceSegments(previous, current) >= 0
    ) {
      throw new TypeError("Merge source segments must use canonical logical order");
    }
  }

  const sourceBlocks = new Map<
    string,
    { columnId: string; type: SimpleDataType; rowCount: number }
  >();
  for (const segment of sourceSegments) {
    for (const column of segment.columns) {
      for (const block of column.sourceBlocks) {
        if (sourceBlocks.has(block.blockId)) {
          throw new TypeError("A merge source block can only appear once");
        }
        sourceBlocks.set(block.blockId, {
          columnId: column.columnId,
          type: column.type,
          rowCount: block.rowCount,
        });
      }
    }
  }

  const columnsValue: unknown = Reflect.get(value, "columns");
  if (!Array.isArray(columnsValue) || columnsValue.length === 0) {
    throw new TypeError("A merge plan requires at least one output column");
  }
  const columns = columnsValue.map((column, index) =>
    normalizeMergeOutputColumn(column, totalRows, sourceBlocks, index),
  );
  const columnIds = columns.map((column) => column.columnId);
  if (new Set(columnIds).size !== columnIds.length) {
    throw new TypeError("A merge plan cannot contain duplicate output columns");
  }
  if (!columnIds.includes(keyColumnId)) {
    throw new TypeError("A merge plan must output its key column");
  }
  validateMergeSourceShapes(sourceSegments, columns, keyColumnId);

  const outputsValue: unknown = Reflect.get(value, "outputs");
  if (!Array.isArray(outputsValue)) throw new TypeError("Merge outputs must be an array");
  if ((totalRows === 0) !== (outputsValue.length === 0)) {
    throw new TypeError("Merge output windows must be empty exactly when no rows survive");
  }
  const outputs = outputsValue.map((output, index) => {
    if (typeof output !== "object" || output === null) {
      throw new TypeError(`Merge output window ${String(index)} must be an object`);
    }
    return {
      rowStart: nonNegativeWholeNumber(
        Reflect.get(output, "rowStart"),
        `Merge output window ${String(index)} row start`,
      ),
      rowCount: positiveUint32(
        Reflect.get(output, "rowCount"),
        `Merge output window ${String(index)} row count`,
      ),
    };
  });
  validateContiguousRows(outputs, totalRows, "Merge output windows");

  const logicalOrder = nonNegativeFiniteNumber(
    Reflect.get(value, "logicalOrder"),
    "Merge logical order",
  );
  if (logicalOrder !== sourceSegments[0]?.logicalOrder) {
    throw new TypeError("Merge logical order must match its earliest source segment");
  }
  const partitionsValue: unknown = Reflect.get(value, "partitions");
  const partitions =
    partitionsValue === undefined
      ? undefined
      : normalizeMergeOutputPartitions(partitionsValue, totalRows, outputs);

  return {
    kind: "merge-v1",
    targetBlockBytes: positiveWholeNumber(
      Reflect.get(value, "targetBlockBytes"),
      "Merge target block bytes",
    ),
    outputCompression: compactionOutputCompression(Reflect.get(value, "outputCompression")),
    keyColumnId,
    totalRows,
    rowIdStart,
    rowIdEndExclusive,
    rowIdSpans,
    logicalOrder,
    sourceSegments,
    columns,
    outputs,
    ...(partitions === undefined ? {} : { partitions }),
  };
}

/**
 * Output partitions tile the merged output, carry strictly increasing logical orders, and
 * never split an output window: a window's blocks belong to exactly one published segment.
 */
function normalizeMergeOutputPartitions(
  value: unknown,
  totalRows: number,
  outputs: readonly RechunkCompactionOutputWindow[],
): MergeOutputPartition[] {
  if (!Array.isArray(value)) throw new TypeError("Merge output partitions must be an array");
  if ((totalRows === 0) !== (value.length === 0)) {
    throw new TypeError("Merge output partitions must be empty exactly when no rows survive");
  }
  const partitions = value.map((partition, index) => {
    if (typeof partition !== "object" || partition === null) {
      throw new TypeError(`Merge output partition ${String(index)} must be an object`);
    }
    const label = `Merge output partition ${String(index)}`;
    return {
      rowStart: nonNegativeWholeNumber(Reflect.get(partition, "rowStart"), `${label} row start`),
      rowCount: positiveWholeNumber(Reflect.get(partition, "rowCount"), `${label} row count`),
      logicalOrder: nonNegativeFiniteNumber(
        Reflect.get(partition, "logicalOrder"),
        `${label} logical order`,
      ),
    };
  });
  validateContiguousRows(partitions, totalRows, "Merge output partitions");
  for (let index = 1; index < partitions.length; index += 1) {
    const previous = partitions[index - 1];
    const current = partitions[index];
    if (
      previous !== undefined &&
      current !== undefined &&
      current.logicalOrder <= previous.logicalOrder
    ) {
      throw new RangeError("Merge output partitions must carry strictly increasing logical orders");
    }
  }
  let partitionIndex = 0;
  for (const output of outputs) {
    let partition = partitions[partitionIndex];
    while (partition !== undefined && output.rowStart >= partition.rowStart + partition.rowCount) {
      partitionIndex += 1;
      partition = partitions[partitionIndex];
    }
    if (
      partition === undefined ||
      output.rowStart < partition.rowStart ||
      output.rowStart + output.rowCount > partition.rowStart + partition.rowCount
    ) {
      throw new RangeError("Merge output windows cannot straddle output partitions");
    }
  }
  return partitions;
}

function normalizeMergeSourceSegment(
  value: unknown,
  segmentIndex: number,
): MergeCompactionSourceSegment {
  if (typeof value !== "object" || value === null) {
    throw new TypeError(`Merge source segment ${String(segmentIndex)} must be an object`);
  }
  const label = `Merge source segment ${String(segmentIndex)}`;
  const kind = segmentKind(Reflect.get(value, "kind"));
  const rowCount = positiveWholeNumber(Reflect.get(value, "rowCount"), `${label} row count`);
  const rowIdStart = nonNegativeBigInt(Reflect.get(value, "rowIdStart"), `${label} row ID start`);
  const rowIdEndExclusive = nonNegativeBigInt(
    Reflect.get(value, "rowIdEndExclusive"),
    `${label} row ID end`,
  );
  let rowIdSpans: RowIdSpan[];
  if (kind === "insert" || kind === "upsert" || kind === "base") {
    rowIdSpans = normalizeRowIdSpans(
      Reflect.get(value, "rowIdSpans"),
      rowCount,
      rowIdStart,
      rowIdEndExclusive,
      `${label} row ID spans`,
    );
  } else {
    const spans: unknown = Reflect.get(value, "rowIdSpans");
    if (!Array.isArray(spans) || spans.length !== 0) {
      throw new TypeError(`${label} mutation markers cannot own row IDs`);
    }
    if (rowIdStart !== 0n || rowIdEndExclusive !== 0n) {
      throw new TypeError(`${label} mutation marker row ID envelope must be empty`);
    }
    rowIdSpans = [];
  }

  const columnsValue: unknown = Reflect.get(value, "columns");
  if (!Array.isArray(columnsValue) || columnsValue.length === 0) {
    throw new TypeError(`${label} requires at least one source column`);
  }
  const columns = columnsValue.map((column, columnIndex) =>
    normalizeMergeSourceColumn(column, rowCount, segmentIndex, columnIndex),
  );
  const columnIds = columns.map((column) => column.columnId);
  if (new Set(columnIds).size !== columnIds.length) {
    throw new TypeError(`${label} cannot contain duplicate source columns`);
  }

  return {
    segmentId: nonEmptyString(Reflect.get(value, "segmentId"), `${label} ID`),
    transactionId: nonEmptyString(Reflect.get(value, "transactionId"), `${label} transaction ID`),
    committedVersion: nonNegativeWholeNumber(
      Reflect.get(value, "committedVersion"),
      `${label} committed version`,
    ),
    kind,
    keyColumnId: nullableId(Reflect.get(value, "keyColumnId"), `${label} key column ID`),
    level: nonNegativeWholeNumber(Reflect.get(value, "level"), `${label} level`),
    logicalOrder: nonNegativeFiniteNumber(
      Reflect.get(value, "logicalOrder"),
      `${label} logical order`,
    ),
    rowCount,
    rowIdStart,
    rowIdEndExclusive,
    rowIdSpans,
    columns,
  };
}

function normalizeMergeSourceColumn(
  value: unknown,
  segmentRowCount: number,
  segmentIndex: number,
  columnIndex: number,
): MergeCompactionSourceColumn {
  const label = `Merge source column ${String(segmentIndex)}:${String(columnIndex)}`;
  if (typeof value !== "object" || value === null) {
    throw new TypeError(`${label} must be an object`);
  }
  const sourceBlocksValue: unknown = Reflect.get(value, "sourceBlocks");
  if (!Array.isArray(sourceBlocksValue) || sourceBlocksValue.length === 0) {
    throw new TypeError(`${label} requires source blocks`);
  }
  const sourceBlocks = sourceBlocksValue.map((block, blockIndex) => {
    if (typeof block !== "object" || block === null) {
      throw new TypeError(`${label} block ${String(blockIndex)} must be an object`);
    }
    return {
      blockId: nonEmptyString(Reflect.get(block, "blockId"), `${label} block ID`),
      rowStart: nonNegativeWholeNumber(Reflect.get(block, "rowStart"), `${label} block row start`),
      rowCount: positiveUint32(Reflect.get(block, "rowCount"), `${label} block row count`),
      storedBytes: positiveWholeNumber(
        Reflect.get(block, "storedBytes"),
        `${label} block stored bytes`,
      ),
      encodedBytes: nonNegativeWholeNumber(
        Reflect.get(block, "encodedBytes"),
        `${label} block encoded bytes`,
      ),
      checksum: uint32(Reflect.get(block, "checksum"), `${label} block checksum`),
    };
  });
  validateContiguousRows(sourceBlocks, segmentRowCount, `${label} blocks`);
  return {
    columnId: nonEmptyString(Reflect.get(value, "columnId"), `${label} ID`),
    type: simpleDataType(Reflect.get(value, "type")),
    sourceBlocks,
  };
}

function normalizeMergeOutputColumn(
  value: unknown,
  totalRows: number,
  sourceBlocks: ReadonlyMap<string, { columnId: string; type: SimpleDataType; rowCount: number }>,
  columnIndex: number,
): MergeCompactionOutputColumn {
  const label = `Merge output column ${String(columnIndex)}`;
  if (typeof value !== "object" || value === null) {
    throw new TypeError(`${label} must be an object`);
  }
  const columnId = nonEmptyString(Reflect.get(value, "columnId"), `${label} ID`);
  const type = simpleDataType(Reflect.get(value, "type"));
  const rangesValue: unknown = Reflect.get(value, "sourceRanges");
  if (!Array.isArray(rangesValue)) throw new TypeError(`${label} source ranges must be an array`);
  const sourceRanges = rangesValue.map((range, rangeIndex) => {
    if (typeof range !== "object" || range === null) {
      throw new TypeError(`${label} source range ${String(rangeIndex)} must be an object`);
    }
    const normalized: MergeCompactionOutputSourceRange = {
      outputRowStart: nonNegativeWholeNumber(
        Reflect.get(range, "outputRowStart"),
        `${label} source range output row start`,
      ),
      sourceBlockId: nonEmptyString(
        Reflect.get(range, "sourceBlockId"),
        `${label} source range block ID`,
      ),
      sourceRowStart: nonNegativeWholeNumber(
        Reflect.get(range, "sourceRowStart"),
        `${label} source range block row start`,
      ),
      rowCount: positiveUint32(Reflect.get(range, "rowCount"), `${label} source range row count`),
    };
    const source = sourceBlocks.get(normalized.sourceBlockId);
    if (source === undefined) throw new TypeError(`${label} references an unknown source block`);
    if (source.columnId !== columnId || source.type !== type) {
      throw new TypeError(`${label} source range has the wrong column or type`);
    }
    if (
      safeSum([normalized.sourceRowStart, normalized.rowCount], `${label} source range rows`) >
      source.rowCount
    ) {
      throw new RangeError(`${label} source range is outside its source block`);
    }
    return normalized;
  });
  let outputRowStart = 0;
  for (let index = 0; index < sourceRanges.length; index += 1) {
    const range = sourceRanges[index];
    if (range === undefined) continue;
    if (range.outputRowStart !== outputRowStart) {
      throw new RangeError(`${label} source ranges must cover output rows contiguously`);
    }
    const previous = sourceRanges[index - 1];
    if (
      previous?.sourceBlockId === range.sourceBlockId &&
      previous.sourceRowStart + previous.rowCount === range.sourceRowStart
    ) {
      throw new TypeError(`${label} contains adjacent source ranges that must be coalesced`);
    }
    outputRowStart = safeSum([outputRowStart, range.rowCount], `${label} output row count`);
  }
  if (outputRowStart !== totalRows) {
    throw new RangeError(`${label} source ranges must cover every merged output row`);
  }
  return { columnId, type, sourceRanges };
}

function validateMergeSourceShapes(
  sourceSegments: readonly MergeCompactionSourceSegment[],
  outputColumns: readonly MergeCompactionOutputColumn[],
  keyColumnId: string,
): void {
  const outputIds = outputColumns.map((column) => column.columnId);
  const outputTypes = new Map(outputColumns.map((column) => [column.columnId, column.type]));
  for (const segment of sourceSegments) {
    if (segment.keyColumnId !== keyColumnId) {
      throw new TypeError(`Merge source segment ${segment.segmentId} has the wrong key column`);
    }
    const sourceIds = segment.columns.map((column) => column.columnId);
    if (segment.columns.some((column) => outputTypes.get(column.columnId) !== column.type)) {
      throw new TypeError(
        `Merge source segment ${segment.segmentId} has an unknown column or type`,
      );
    }
    const canonicalIds = outputIds.filter((id) => sourceIds.includes(id));
    if (canonicalIds.some((id, index) => sourceIds[index] !== id)) {
      throw new TypeError(`Merge source segment ${segment.segmentId} columns are not canonical`);
    }
    if (segment.kind === "insert" || segment.kind === "upsert" || segment.kind === "base") {
      if (
        sourceIds.length !== outputIds.length ||
        sourceIds.some((id, index) => outputIds[index] !== id)
      ) {
        throw new TypeError(`Merge ${segment.kind} segment must contain every output column`);
      }
    } else if (segment.kind === "delete") {
      if (sourceIds.length !== 1 || sourceIds[0] !== keyColumnId) {
        throw new TypeError("A merge delete segment must contain only its key column");
      }
    } else if (
      sourceIds.length < 2 ||
      !sourceIds.includes(keyColumnId) ||
      sourceIds.every((id) => id === keyColumnId)
    ) {
      throw new TypeError("A merge update segment requires its key and a changed column");
    }
  }
}

function normalizeRowIdSpans(
  value: unknown,
  totalRows: number,
  rowIdStart: bigint,
  rowIdEndExclusive: bigint,
  label: string,
): RowIdSpan[] {
  if (!Array.isArray(value)) throw new TypeError(`${label} must be an array`);
  const spans = value.map((span, index) => {
    if (typeof span !== "object" || span === null) {
      throw new TypeError(`${label} ${String(index)} must be an object`);
    }
    return {
      rowStart: nonNegativeWholeNumber(
        Reflect.get(span, "rowStart"),
        `${label} ${String(index)} row start`,
      ),
      rowCount: positiveWholeNumber(
        Reflect.get(span, "rowCount"),
        `${label} ${String(index)} row count`,
      ),
      rowIdStart: nonNegativeBigInt(
        Reflect.get(span, "rowIdStart"),
        `${label} ${String(index)} row ID start`,
      ),
    };
  });
  let rowStart = 0;
  for (let index = 0; index < spans.length; index += 1) {
    const span = spans[index];
    if (span === undefined) continue;
    if (span.rowStart !== rowStart) throw new RangeError(`${label} must cover rows contiguously`);
    const previous = spans[index - 1];
    if (
      previous !== undefined &&
      previous.rowIdStart + BigInt(previous.rowCount) === span.rowIdStart
    ) {
      throw new TypeError(`${label} contains adjacent spans that must be coalesced`);
    }
    rowStart = safeSum([rowStart, span.rowCount], `${label} row count`);
  }
  if (rowStart !== totalRows) throw new RangeError(`${label} must cover every row`);
  if (spans.length === 0) {
    if (totalRows !== 0 || rowIdStart !== 0n || rowIdEndExclusive !== 0n) {
      throw new RangeError(`${label} has an invalid empty row ID envelope`);
    }
    return spans;
  }
  const intervals = spans
    .map((span) => ({ start: span.rowIdStart, end: span.rowIdStart + BigInt(span.rowCount) }))
    .sort((left, right) => (left.start < right.start ? -1 : left.start > right.start ? 1 : 0));
  for (let index = 1; index < intervals.length; index += 1) {
    const previous = intervals[index - 1];
    const current = intervals[index];
    if (previous !== undefined && current !== undefined && current.start < previous.end) {
      throw new RangeError(`${label} cannot contain overlapping row IDs`);
    }
  }
  const minimum = intervals[0]?.start;
  const maximum = intervals[intervals.length - 1]?.end;
  if (minimum !== rowIdStart || maximum !== rowIdEndExclusive) {
    throw new RangeError(`${label} must match its row ID envelope`);
  }
  return spans;
}

function compareMergeSourceSegments(
  left: MergeCompactionSourceSegment,
  right: MergeCompactionSourceSegment,
): number {
  return (
    left.logicalOrder - right.logicalOrder ||
    left.committedVersion - right.committedVersion ||
    left.segmentId.localeCompare(right.segmentId)
  );
}

function normalizeCompactionOutputCursor(
  value: unknown,
  plan: CompactionRewritePlan,
): CompactionOutputCursor | null {
  if (plan.kind === "copy-v1") {
    if (value !== undefined && value !== null) {
      throw new TypeError("A copy compaction cannot have an output cursor");
    }
    return null;
  }
  if (value === undefined) {
    if (plan.kind === "merge-v1") {
      throw new TypeError("A merge compaction requires an explicit output cursor");
    }
    return { outputIndex: 0, columnIndex: 0, rowStart: plan.outputs[0]?.rowStart ?? 0 };
  }
  if (typeof value !== "object" || value === null) {
    throw new TypeError("Rechunk output cursor must be an object");
  }
  const cursor: CompactionOutputCursor = {
    outputIndex: nonNegativeWholeNumber(
      Reflect.get(value, "outputIndex"),
      "Rechunk output cursor index",
    ),
    columnIndex: nonNegativeWholeNumber(
      Reflect.get(value, "columnIndex"),
      "Rechunk output cursor column index",
    ),
    rowStart: nonNegativeWholeNumber(
      Reflect.get(value, "rowStart"),
      "Rechunk output cursor row start",
    ),
  };
  if (cursor.outputIndex === plan.outputs.length) {
    if (cursor.columnIndex !== 0 || cursor.rowStart !== plan.totalRows) {
      throw new RangeError("A completed rechunk output cursor is not canonical");
    }
    return cursor;
  }
  const output = plan.outputs[cursor.outputIndex];
  if (output === undefined || cursor.columnIndex >= plan.columns.length) {
    throw new RangeError("Rechunk output cursor is outside the output plan");
  }
  if (cursor.rowStart !== output.rowStart) {
    throw new RangeError("Rechunk output cursor row start does not match its output window");
  }
  return cursor;
}

function validateContiguousRows(
  ranges: ReadonlyArray<{ rowStart: number; rowCount: number }>,
  totalRows: number,
  label: string,
): void {
  let rowStart = 0;
  for (const range of ranges) {
    if (range.rowStart !== rowStart) {
      throw new RangeError(`${label} must cover rows contiguously from zero`);
    }
    rowStart = safeSum([rowStart, range.rowCount], `${label} row count`);
  }
  if (rowStart !== totalRows) {
    throw new RangeError(`${label} must cover every planned row`);
  }
}

function compactionOutputCompression(value: unknown): CompactionOutputCompression {
  if (
    typeof value !== "string" ||
    !(compactionOutputCompressions as readonly string[]).includes(value)
  ) {
    throw new TypeError(`Invalid compaction output compression: ${String(value)}`);
  }
  return value as CompactionOutputCompression;
}

function simpleDataType(value: unknown): SimpleDataType {
  if (typeof value !== "string" || !(simpleDataTypes as readonly string[]).includes(value)) {
    throw new TypeError(`Invalid compaction column type: ${String(value)}`);
  }
  return value as SimpleDataType;
}

function segmentKind(value: unknown): SegmentKind {
  if (
    value !== "insert" &&
    value !== "upsert" &&
    value !== "update" &&
    value !== "delete" &&
    value !== "base"
  ) {
    throw new TypeError(`Invalid merge source segment kind: ${String(value)}`);
  }
  return value;
}

function compactionJobState(state: unknown): CompactionJobState {
  if (typeof state !== "string" || !(compactionJobStates as readonly string[]).includes(state)) {
    throw new TypeError(`Invalid compaction job state: ${String(state)}`);
  }
  return state as CompactionJobState;
}

function garbageCollectionJobState(state: unknown): GarbageCollectionJobState {
  if (
    typeof state !== "string" ||
    !(garbageCollectionJobStates as readonly string[]).includes(state)
  ) {
    throw new TypeError(`Invalid garbage collection job state: ${String(state)}`);
  }
  return state as GarbageCollectionJobState;
}

function normalizeGarbageCollectionCursor(value: unknown): GarbageCollectionCursor {
  if (typeof value !== "object" || value === null) {
    throw new TypeError("Garbage collection cursor must be an object");
  }
  return {
    manifestIndex: nonNegativeWholeNumber(
      Reflect.get(value, "manifestIndex"),
      "Garbage collection manifest cursor",
    ),
    segmentIndex: nonNegativeWholeNumber(
      Reflect.get(value, "segmentIndex"),
      "Garbage collection segment cursor",
    ),
    blockIndex: nonNegativeWholeNumber(
      Reflect.get(value, "blockIndex"),
      "Garbage collection block cursor",
    ),
    transactionIndex: nonNegativeWholeNumber(
      Reflect.get(value, "transactionIndex") ?? 0,
      "Garbage collection transaction cursor",
    ),
  };
}

function garbageCollectionJobComplete(record: GarbageCollectionJobRecord): boolean {
  return (
    record.cursor.manifestIndex === record.candidateManifestVersions.length &&
    record.cursor.segmentIndex === record.candidateSegmentIds.length &&
    record.cursor.blockIndex === record.candidateBlockIds.length &&
    record.cursor.transactionIndex === record.candidateTransactionIds.length
  );
}

function uniqueIds(ids: unknown, label: string, sort: boolean): string[] {
  if (!Array.isArray(ids)) throw new TypeError(`${label}s must be an array`);
  const unique = [...new Set(ids.map((id: unknown) => nonEmptyString(id, label)))];
  return sort ? unique.sort() : unique;
}

function orderedUniqueIds(ids: unknown, label: string): string[] {
  if (!Array.isArray(ids)) throw new TypeError(`${label}s must be an array`);
  const normalized = ids.map((id: unknown) => nonEmptyString(id, label));
  if (new Set(normalized).size !== normalized.length) {
    throw new TypeError(`${label}s cannot contain duplicates`);
  }
  return normalized;
}

function uniqueWholeNumbers(values: unknown, label: string): number[] {
  if (!Array.isArray(values)) throw new TypeError(`${label}s must be an array`);
  return [...new Set(values.map((value: unknown) => nonNegativeWholeNumber(value, label)))].sort(
    (left, right) => left - right,
  );
}

function nullableId(id: unknown, label: string): string | null {
  return id === null ? null : nonEmptyString(id, label);
}

function nonEmptyString(value: unknown, label: string): string {
  if (typeof value !== "string" || value.length === 0) {
    throw new TypeError(`${label} cannot be empty`);
  }
  return value;
}

function validTimestamp(value: unknown, label: string): string {
  const timestamp = nonEmptyString(value, label);
  if (!Number.isFinite(Date.parse(timestamp))) throw new TypeError(`${label} must be valid`);
  return timestamp;
}

function nonNegativeWholeNumber(value: unknown, label: string): number {
  if (typeof value !== "number" || !Number.isSafeInteger(value) || value < 0) {
    throw new RangeError(`${label} must be a non-negative whole number`);
  }
  return value;
}

function nonNegativeBigInt(value: unknown, label: string): bigint {
  if (typeof value !== "bigint" || value < 0n) {
    throw new RangeError(`${label} must be a non-negative bigint`);
  }
  return value;
}

function positiveWholeNumber(value: unknown, label: string): number {
  const normalized = nonNegativeWholeNumber(value, label);
  if (normalized === 0) throw new RangeError(`${label} must be positive`);
  return normalized;
}

function positiveFiniteNumber(value: unknown, label: string): number {
  if (typeof value !== "number" || !Number.isFinite(value) || value <= 0) {
    throw new RangeError(`${label} must be a positive finite number`);
  }
  return value;
}

function nonNegativeFiniteNumber(value: unknown, label: string): number {
  if (typeof value !== "number" || !Number.isFinite(value) || value < 0) {
    throw new RangeError(`${label} must be a non-negative finite number`);
  }
  return value;
}

function uint32(value: unknown, label: string): number {
  const normalized = nonNegativeWholeNumber(value, label);
  if (normalized > 0xffff_ffff) throw new RangeError(`${label} must fit in 32 bits`);
  return normalized;
}

function positiveUint32(value: unknown, label: string): number {
  const normalized = uint32(value, label);
  if (normalized === 0) throw new RangeError(`${label} must be positive`);
  return normalized;
}

function safeSum(values: readonly number[], label: string): number {
  let total = 0;
  for (const value of values) {
    total += value;
    if (!Number.isSafeInteger(total)) throw new RangeError(`${label} exceeds the safe range`);
  }
  return total;
}

function safeProduct(left: number, right: number, label: string): number {
  const product = left * right;
  if (!Number.isSafeInteger(product)) throw new RangeError(`${label} exceeds the safe range`);
  return product;
}

/** Floors an integer-times-double product without rounding the binary double upward. */
export function floorWholeNumberProduct(left: number, right: number, label: string): number {
  if (!Number.isSafeInteger(left) || left < 0 || !Number.isFinite(right) || right < 0) {
    throw new RangeError(`${label} exceeds the safe range`);
  }
  const bytes = new ArrayBuffer(8);
  const view = new DataView(bytes);
  view.setFloat64(0, right, false);
  const high = view.getUint32(0, false);
  const low = view.getUint32(4, false);
  const exponentBits = (high >>> 20) & 0x7ff;
  const fraction = (BigInt(high & 0x000f_ffff) << 32n) | BigInt(low);
  const significand = exponentBits === 0 ? fraction : (1n << 52n) | fraction;
  const binaryExponent = exponentBits === 0 ? -1074 : exponentBits - 1023 - 52;
  let product = BigInt(left) * significand;
  if (binaryExponent >= 0) product <<= BigInt(binaryExponent);
  else product >>= BigInt(-binaryExponent);
  if (product > BigInt(Number.MAX_SAFE_INTEGER)) {
    throw new RangeError(`${label} exceeds the safe range`);
  }
  return Number(product);
}
