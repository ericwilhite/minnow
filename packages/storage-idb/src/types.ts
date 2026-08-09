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

export interface Manifest {
  version: number;
  previousVersion: number | null;
  blockIds: string[];
  createdAt: string;
  /** A pruned descriptor remains readable for commit reconciliation but cannot be pinned. */
  prunedAt?: string;
}

export interface PublishManifestInput {
  expectedVersion: number | null;
  blockIds: readonly string[];
  createdAt?: string;
}

export const simpleDataTypes = ["boolean", "number", "string", "datetime"] as const;
export type SimpleDataType = (typeof simpleDataTypes)[number];

export interface TableColumnRecord {
  id: string;
  name: string;
  type: SimpleDataType;
  nullable: boolean;
}

export interface TableRecord {
  id: string;
  name: string;
  columns: TableColumnRecord[];
  uniqueKeyColumnId?: string;
  uniqueKeyLookupReady?: boolean;
  uniqueKeyStorage?: "chunks-v1";
  createdAt: string;
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

export const compactionOutputCompressions = ["raw", "rle", "gzip"] as const;
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

export interface GarbageCollectionCursor {
  manifestIndex: number;
  segmentIndex: number;
  blockIndex: number;
}

export interface CreateGarbageCollectionJobInput {
  id: string;
  candidateManifestVersions: readonly number[];
  candidateSegmentIds: readonly string[];
  candidateBlockIds: readonly string[];
  /** Fixed cutoff used to decide which persisted leases protect a manifest for this job. */
  leaseCutoff: string;
  createdAt: string;
}

export interface GarbageCollectionJobRecord {
  id: string;
  candidateManifestVersions: number[];
  candidateSegmentIds: string[];
  candidateBlockIds: string[];
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

export interface CommitTransactionInput {
  transactionId: string;
  expectedTransactionRevision: number;
  expectedManifestVersion: number | null;
  blockIds: readonly string[];
  removedBlockIds?: readonly string[];
  uniqueKeyChanges?: UniqueKeyChanges;
  committedAt: string;
}

export interface UniqueKeyChanges {
  tableId: string;
  keyTokens: readonly string[];
  requireAbsent: boolean;
  remove?: boolean;
  storageMode?: "chunks-v1";
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

export interface BlockStore {
  addBlock(id: string, bytes: Uint8Array): Promise<void>;
  addBlocks(blocks: readonly BlockWrite[]): Promise<void>;
  getBlock(id: string): Promise<Uint8Array | undefined>;
  getBlocks(ids: readonly string[]): Promise<Array<Uint8Array | undefined>>;
  removeBlock(id: string): Promise<void>;
  listBlockIds(): Promise<string[]>;
  addTable(record: TableRecord): Promise<void>;
  getTable(id: string): Promise<TableRecord | undefined>;
  getTableByName(name: string): Promise<TableRecord | undefined>;
  listTables(): Promise<TableRecord[]>;
  addSegment(record: SegmentRecord): Promise<void>;
  getSegment(id: string): Promise<SegmentRecord | undefined>;
  listSegments(tableId?: string): Promise<SegmentRecord[]>;
  removeSegment(id: string): Promise<void>;
  reserveRowIds(tableId: string, count: number): Promise<RowIdRange>;
  getExistingUniqueKeys(tableId: string, keyTokens: readonly string[]): Promise<string[]>;
  getCurrentManifest(): Promise<Manifest | undefined>;
  getManifest(version: number): Promise<Manifest | undefined>;
  listManifests(): Promise<Manifest[]>;
  publishManifest(input: PublishManifestInput): Promise<Manifest>;
  createTransaction(record: TransactionRecord): Promise<void>;
  getTransaction(id: string): Promise<TransactionRecord | undefined>;
  listTransactions(): Promise<TransactionRecord[]>;
  updateTransaction(
    id: string,
    expectedRevision: number,
    update: TransactionRecordUpdate,
  ): Promise<TransactionRecord>;
  commitTransaction(input: CommitTransactionInput): Promise<Manifest>;
  createLease(record: LeaseRecord): Promise<void>;
  getLease(id: string): Promise<LeaseRecord | undefined>;
  listLeases(): Promise<LeaseRecord[]>;
  renewLease(id: string, expectedRevision: number, expiresAt: string): Promise<LeaseRecord>;
  removeLeaseIfExpired(
    id: string,
    expectedRevision: number,
    expiresAtCutoff: string,
  ): Promise<boolean>;
  removeLease(id: string): Promise<void>;
  createCompactionJob(record: CompactionJobRecord): Promise<void>;
  getCompactionJob(id: string): Promise<CompactionJobRecord | undefined>;
  listCompactionJobs(tableId?: string): Promise<CompactionJobRecord[]>;
  updateCompactionJob(
    id: string,
    expectedRevision: number,
    update: CompactionJobRecordUpdate,
  ): Promise<CompactionJobRecord>;
  cancelCompactionJob(
    id: string,
    expectedRevision: number,
    cancelledAt: string,
  ): Promise<CompactionJobRecord>;
  removeCompactionJob(id: string): Promise<void>;
  createGarbageCollectionJob(
    input: CreateGarbageCollectionJobInput,
  ): Promise<GarbageCollectionJobRecord>;
  getGarbageCollectionJob(id: string): Promise<GarbageCollectionJobRecord | undefined>;
  listGarbageCollectionJobs(): Promise<GarbageCollectionJobRecord[]>;
  runGarbageCollectionStep(
    input: RunGarbageCollectionStepInput,
  ): Promise<GarbageCollectionStepResult>;
  removeGarbageCollectionJob(id: string): Promise<void>;
  close(): void;
}

export function createManifest(input: PublishManifestInput): Manifest {
  return {
    version: input.expectedVersion === null ? 0 : input.expectedVersion + 1,
    previousVersion: input.expectedVersion,
    blockIds: [...new Set(input.blockIds)].sort(),
    createdAt: input.createdAt ?? new Date().toISOString(),
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
  if ((record.kind ?? "insert") !== "insert") {
    throw new TypeError("A partitioned segment must be an insert");
  }
  if (record.logicalOrder === undefined) {
    throw new TypeError("A partitioned segment requires an explicit logical order");
  }
  nonNegativeWholeNumber(record.logicalOrder, "Segment logical order");
  if (record.rowIdSpans !== undefined) {
    throw new TypeError("A partitioned segment cannot contain row ID spans");
  }
  const rowCount = positiveWholeNumber(record.rowCount, "Segment row count");
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
  const createdAt = validTimestamp(input.createdAt, "Garbage collection creation timestamp");
  const complete =
    candidateManifestVersions.length === 0 &&
    candidateSegmentIds.length === 0 &&
    candidateBlockIds.length === 0;
  return {
    id: nonEmptyString(input.id, "Garbage collection job ID"),
    candidateManifestVersions,
    candidateSegmentIds,
    candidateBlockIds,
    cursor: { manifestIndex: 0, segmentIndex: 0, blockIndex: 0 },
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
  const cursor = normalizeGarbageCollectionCursor(record.cursor);
  const normalized: GarbageCollectionJobRecord = {
    ...record,
    id: nonEmptyString(record.id, "Garbage collection job ID"),
    candidateManifestVersions,
    candidateSegmentIds,
    candidateBlockIds,
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
    ) !== cursor.blockIndex
  ) {
    throw new TypeError("Garbage collection cursor does not match its persisted accounting");
  }
  if (
    cursor.manifestIndex > candidateManifestVersions.length ||
    cursor.segmentIndex > candidateSegmentIds.length ||
    cursor.blockIndex > candidateBlockIds.length
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
    (cursor.manifestIndex !== 0 || cursor.segmentIndex !== 0 || cursor.blockIndex !== 0)
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
  let level2Policy:
    | Pick<
        CompactionJobRecord,
        | "outputPartitionOrdinal"
        | "maxWriteAmplification"
        | "maximumOutputStoredBytes"
        | "plannedOutputStoredBytesUpperBound"
      >
    | undefined;
  if (level2PolicyFieldCount !== 0) {
    if (rewritePlan.kind !== "rechunk-v1") {
      throw new TypeError("Append-row-range L2 compaction requires a rechunk plan");
    }
    if (record.targetLevel !== 2) {
      throw new TypeError("Append-row-range L2 compaction must target level two");
    }
    if (
      sourceLevelStoredBytes?.level0SourceStoredBytes !== sourceStoredBytes ||
      sourceLevelStoredBytes.anchorSourceStoredBytes !== 0
    ) {
      throw new TypeError("Append-row-range L2 compaction requires only level-zero source bytes");
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
    const amplificationCeiling = floorWholeNumberProduct(
      sourceStoredBytes,
      maxWriteAmplification,
      "Compaction write amplification product",
    );
    if (maximumOutputStoredBytes > amplificationCeiling) {
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
    logicalOrder: nonNegativeWholeNumber(
      Reflect.get(value, "logicalOrder"),
      "Rechunk logical order",
    ),
    columns,
    outputs,
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

  const logicalOrder = nonNegativeWholeNumber(
    Reflect.get(value, "logicalOrder"),
    "Merge logical order",
  );
  if (logicalOrder !== sourceSegments[0]?.logicalOrder) {
    throw new TypeError("Merge logical order must match its earliest source segment");
  }

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
  };
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
    logicalOrder: nonNegativeWholeNumber(
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
  };
}

function garbageCollectionJobComplete(record: GarbageCollectionJobRecord): boolean {
  return (
    record.cursor.manifestIndex === record.candidateManifestVersions.length &&
    record.cursor.segmentIndex === record.candidateSegmentIds.length &&
    record.cursor.blockIndex === record.candidateBlockIds.length
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
