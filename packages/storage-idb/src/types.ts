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

export const compactionJobStates = ["planned", "running", "ready", "published", "aborted"] as const;
export type CompactionJobState = (typeof compactionJobStates)[number];

export interface CompactionJobCursor {
  sourceSegmentIndex: number;
  sourceBlockIndex: number;
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
  const normalized: CompactionJobRecord = {
    ...record,
    id: nonEmptyString(record.id, "Compaction job ID"),
    tableId: nonEmptyString(record.tableId, "Compaction job table ID"),
    sourceManifestVersion: nonNegativeWholeNumber(
      record.sourceManifestVersion,
      "Compaction source manifest version",
    ),
    sourceSegmentIds: uniqueIds(record.sourceSegmentIds, "Compaction source segment ID", false),
    sourceBlockIds: uniqueIds(record.sourceBlockIds, "Compaction source block ID", true),
    outputBlockIds: uniqueIds(record.outputBlockIds, "Compaction output block ID", true),
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
    logicalBytes: nonNegativeWholeNumber(record.logicalBytes, "Compaction logical bytes"),
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
  if (normalized.outputBlockIds.length > normalized.sourceBlockIds.length) {
    throw new RangeError("Compaction output cannot contain more blocks than its source selection");
  }
  validateCompactionJobState(normalized);
  return structuredClone(normalized);
}

export function updateCompactionJobRecord(
  record: CompactionJobRecord,
  update: CompactionJobRecordUpdate,
): CompactionJobRecord {
  const updated: CompactionJobRecord = {
    ...record,
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
    ...(update.state === undefined ? {} : { state: update.state }),
    ...(update.transactionId === undefined ? {} : { transactionId: update.transactionId }),
    ...(update.outputSegmentId === undefined ? {} : { outputSegmentId: update.outputSegmentId }),
    ...(update.publishedVersion === undefined ? {} : { publishedVersion: update.publishedVersion }),
    updatedAt: update.updatedAt,
    revision: record.revision + 1,
  };
  if (update.error === null) delete updated.error;
  else if (update.error !== undefined) updated.error = update.error;
  const normalized = normalizeCompactionJobRecord(updated);
  validateCompactionJobTransition(record.state, normalized.state);
  return normalized;
}

function validateCompactionJobState(record: CompactionJobRecord): void {
  if (record.state === "planned") {
    const hasProgress =
      record.cursor.sourceSegmentIndex !== 0 ||
      record.cursor.sourceBlockIndex !== 0 ||
      record.outputBlockIds.length !== 0 ||
      record.processedRows !== 0 ||
      record.sourceStoredBytes !== 0 ||
      record.outputStoredBytes !== 0 ||
      record.logicalBytes !== 0;
    if (hasProgress || record.transactionId !== null) {
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
    if (record.cursor.sourceSegmentIndex !== record.sourceSegmentIds.length) {
      throw new TypeError(`${record.state} compaction requires a completed source cursor`);
    }
    if (record.outputBlockIds.length !== record.sourceBlockIds.length) {
      throw new TypeError(`${record.state} compaction requires every output block`);
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

function validateCompactionJobTransition(
  previous: CompactionJobState,
  next: CompactionJobState,
): void {
  const allowed: Record<CompactionJobState, readonly CompactionJobState[]> = {
    planned: ["planned", "running", "aborted"],
    running: ["running", "ready", "published", "aborted"],
    ready: ["ready", "running", "published", "aborted"],
    published: ["published"],
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
