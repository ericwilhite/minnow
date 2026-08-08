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

export type SegmentKind = "insert" | "upsert" | "update" | "delete";

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

export const compactionRewritePlanKinds = ["copy-v1", "rechunk-v1"] as const;
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

export type CompactionRewritePlan = CopyCompactionRewritePlan | RechunkCompactionRewritePlan;

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

export function normalizeCompactionJobRecord(record: CompactionJobRecord): CompactionJobRecord {
  const error: unknown = record.error;
  if (error !== undefined && typeof error !== "string") {
    throw new TypeError("Compaction job error must be a string");
  }
  const rewritePlan = normalizeCompactionRewritePlan(record.rewritePlan);
  const logicalBytes = nonNegativeWholeNumber(record.logicalBytes, "Compaction logical bytes");
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
    sourceStoredBytes: nonNegativeWholeNumber(
      record.sourceStoredBytes,
      "Compaction source stored bytes",
    ),
    outputStoredBytes: nonNegativeWholeNumber(
      record.outputStoredBytes,
      "Compaction output stored bytes",
    ),
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
  for (const field of ["rewritePlan", "memoryBudgetBytes", "minimumMemoryBytes"] as const) {
    if (Reflect.has(update, field)) {
      throw new TypeError(`Compaction ${field} is immutable`);
    }
  }
  if (current.rewritePlan?.kind === "rechunk-v1") {
    for (const field of ["cursor", "sourceStoredBytes", "logicalBytes"] as const) {
      if (Reflect.has(update, field)) {
        throw new TypeError(`Rechunk compaction ${field} is immutable`);
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
    if (record.transactionId === null || record.outputSegmentId === null) {
      throw new TypeError(`${record.state} compaction requires transaction and output segment IDs`);
    }
    if (!hasCompletedCompactionCursor(record)) {
      throw new TypeError(`${record.state} compaction requires a completed cursor`);
    }
    if (record.outputBlockIds.length !== expectedCompactionOutputCount(record)) {
      throw new TypeError(`${record.state} compaction requires every output block`);
    }
    if (
      record.rewritePlan?.kind === "rechunk-v1" &&
      (record.peakWorkingBytes ?? 0) < (record.minimumMemoryBytes ?? 0)
    ) {
      throw new TypeError(`${record.state} rechunk compaction requires complete memory accounting`);
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
    throw new TypeError("A rechunk compaction does not use the source-driven cursor");
  }
  if (memoryBudgetBytes === 0 || minimumMemoryBytes === 0) {
    throw new RangeError("A rechunk compaction requires a memory budget and minimum memory");
  }
  if (minimumMemoryBytes > memoryBudgetBytes) {
    throw new RangeError("Compaction minimum memory exceeds its memory budget");
  }
  if (peakWorkingBytes > memoryBudgetBytes) {
    throw new RangeError("Compaction peak working bytes exceed its memory budget");
  }

  const plannedBlocks = plan.columns.flatMap((column) => column.sourceBlocks);
  const plannedBlockIds = plannedBlocks.map((block) => block.blockId);
  if (new Set(plannedBlockIds).size !== plannedBlockIds.length) {
    throw new TypeError("A rechunk source block can only appear once in its source layout");
  }
  const sortedPlannedIds = [...plannedBlockIds].sort();
  if (
    sortedPlannedIds.length !== record.sourceBlockIds.length ||
    sortedPlannedIds.some((id, index) => id !== record.sourceBlockIds[index])
  ) {
    throw new TypeError("Rechunk source layout must describe every selected source block");
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
    throw new TypeError("Rechunk source stored bytes must match its immutable source layout");
  }
  if (record.logicalBytes !== plannedEncodedBytes) {
    throw new TypeError("Rechunk logical bytes must match its immutable source layout");
  }

  const cursor = record.outputCursor;
  if (cursor === null || cursor === undefined) {
    throw new TypeError("A rechunk compaction requires an output cursor");
  }
  const completedOutputs = safeSum(
    [
      safeProduct(cursor.outputIndex, plan.columns.length, "Rechunk output cursor"),
      cursor.columnIndex,
    ],
    "Rechunk output cursor",
  );
  if (record.outputBlockIds.length !== completedOutputs) {
    throw new TypeError("Rechunk output IDs must match its output cursor");
  }
  const expectedProcessedRows =
    cursor.outputIndex === plan.outputs.length
      ? plan.totalRows
      : (plan.outputs[cursor.outputIndex]?.rowStart ?? 0);
  if (record.processedRows !== expectedProcessedRows) {
    throw new TypeError("Rechunk processed rows must match its completed output windows");
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
  if (previous.rewritePlan?.kind === "rechunk-v1") {
    if (previous.outputBlockIds.some((id, index) => next.outputBlockIds[index] !== id)) {
      throw new TypeError("Rechunk output block IDs are an append-only ordered checkpoint");
    }
    if (compactionOutputOrdinal(next) < compactionOutputOrdinal(previous)) {
      throw new RangeError("Rechunk output cursor cannot move backwards");
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
  return (
    cursor?.outputIndex === 0 &&
    cursor.columnIndex === 0 &&
    cursor.rowStart === plan.outputs[0]?.rowStart
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
  if (plan?.kind !== "rechunk-v1" || cursor === null || cursor === undefined) return 0;
  return safeSum(
    [
      safeProduct(cursor.outputIndex, plan.columns.length, "Rechunk output cursor"),
      cursor.columnIndex,
    ],
    "Rechunk output cursor",
  );
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
    throw new TypeError(`Invalid rechunk source column type: ${String(value)}`);
  }
  return value as SimpleDataType;
}

function compactionJobState(state: unknown): CompactionJobState {
  if (typeof state !== "string" || !(compactionJobStates as readonly string[]).includes(state)) {
    throw new TypeError(`Invalid compaction job state: ${String(state)}`);
  }
  return state as CompactionJobState;
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

function nullableId(id: unknown, label: string): string | null {
  return id === null ? null : nonEmptyString(id, label);
}

function nonEmptyString(value: unknown, label: string): string {
  if (typeof value !== "string" || value.length === 0) {
    throw new TypeError(`${label} cannot be empty`);
  }
  return value;
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
