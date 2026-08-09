import {
  buildPhysicalColumnFromRanges,
  decodeBlock,
  decodePhysicalBlock,
  encodeBlock,
  encodePhysicalBlock,
  getCompressionMemoryBound,
  inspectBlock,
  maximumPhysicalBlockByteLength,
  measurePhysicalColumnRanges,
  slicePhysicalColumn,
  type ColumnInput,
  type Compression,
  type DecodedColumn,
  type LogicalType,
  type PhysicalColumnRange,
  type ValidatedPhysicalColumn,
} from "@browserdatabase/block-format";
import {
  simpleDataTypes,
  floorWholeNumberProduct,
  type BlockStore,
  CompactionJobConflictError,
  type CompactionJobRecord,
  type CompactionJobState,
  GarbageCollectionJobConflictError,
  type GarbageCollectionJobRecord,
  type GarbageCollectionJobState,
  type MergeCompactionOutputColumn,
  type MergeCompactionOutputSourceRange,
  type MergeCompactionRewritePlan,
  type MergeCompactionSourceBlock,
  type MergeCompactionSourceColumn,
  type MergeCompactionSourceSegment,
  type RechunkCompactionOutputWindow,
  type RechunkCompactionRewritePlan,
  type RechunkCompactionSourceBlock,
  type RechunkCompactionSourceColumn,
  type RowIdSpan,
  type SegmentKind,
  type SegmentRecord,
  type SimpleDataType,
  type TableColumnRecord,
  type TableRecord,
  SnapshotManifestMissingError,
  TransactionRecordConflictError,
  UniqueKeyConflictError,
  WriteConflictError,
} from "@browserdatabase/storage-idb";
import {
  Snapshot,
  TransactionManager,
  type DatabaseTransaction,
  type LeasedSnapshot,
} from "@browserdatabase/transactions";
import {
  compileQuery,
  createPreparedColumnarQuery,
  referencedColumns,
  type PreparedQuery,
  type QueryResult,
} from "./query.js";
import { QueryMemoryContext } from "./memory.js";
import { createColumnarTable, type ColumnarColumnInput, type ColumnarTable } from "./vector.js";

const sizeTextEncoder = new TextEncoder();
const DEFAULT_COMPACTION_TARGET_BLOCK_BYTES = 2 * 1024 * 1024;
const DEFAULT_COMPACTION_MEMORY_BUDGET_BYTES = 32 * 1024 * 1024;
const DEFAULT_COMPACTION_MINIMUM_LEVEL_ZERO_SEGMENTS = 2;
const DEFAULT_COMPACTION_MAXIMUM_LEVEL_ZERO_SEGMENTS = 16;
const DEFAULT_COMPACTION_MAXIMUM_LEVEL_ZERO_STORED_BYTES = 64 * 1024 * 1024;
const DEFAULT_LEVEL_TWO_MAX_WRITE_AMPLIFICATION = 16;
const MAX_COMPACTION_TARGET_BLOCK_BYTES = 64 * 1024 * 1024;
const MAX_BLOCK_ENVELOPE_BYTES = 1024;
const INTERNAL_READ_LEASE_TTL_MS = 60_000;

type PhysicalCompactionRewritePlan = RechunkCompactionRewritePlan | MergeCompactionRewritePlan;

interface PhysicalCompactionSourceRange {
  readonly blockId: string;
  readonly outputRowStart: number;
  readonly sourceRowStart: number;
  readonly rowCount: number;
  readonly sourceBlockRowCount: number;
  readonly storedBytes: number;
  readonly encodedBytes: number;
  readonly checksum: number;
}

interface PhysicalCompactionSourceColumn {
  readonly columnId: string;
  readonly type: SimpleDataType;
  readonly sourceRanges: readonly PhysicalCompactionSourceRange[];
}

interface PhysicalCompactionLayout {
  readonly targetBlockBytes: number;
  readonly outputCompression: Compression;
  readonly totalRows: number;
  readonly columns: readonly PhysicalCompactionSourceColumn[];
  readonly outputs: readonly RechunkCompactionOutputWindow[];
}

interface MergeResolvedSource {
  readonly blockId: string;
  readonly sourceRowIndex: number;
}

interface MergeResolvedRow {
  readonly rowId: bigint;
  readonly sources: readonly MergeResolvedSource[];
}

export interface ColumnDefinition {
  name: string;
  type: SimpleDataType;
  nullable?: boolean;
}

export interface CreateTableInput {
  name: string;
  columns: readonly ColumnDefinition[];
  uniqueKey?: string;
}

export type BatchValue = boolean | number | string | Date | null;

export interface InsertBatchInput {
  columns: Readonly<Record<string, readonly BatchValue[]>>;
}

export interface InsertBatchResult {
  tableName: string;
  segmentId: string;
  rowCount: number;
  blockCount: number;
  storedBytes: number;
  version: number;
  metrics: WriteMetrics;
}

export interface UpsertBatchResult extends InsertBatchResult {
  insertedRowCount: number;
  updatedRowCount: number;
}

export interface UpdateBatchInput {
  keys: readonly BatchValue[];
  changes: Readonly<Record<string, readonly BatchValue[]>>;
}

export interface UpdateBatchResult {
  tableName: string;
  segmentId: string;
  requestedRowCount: number;
  updatedRowCount: number;
  changedColumns: string[];
  blockCount: number;
  storedBytes: number;
  version: number;
  metrics: WriteMetrics;
}

export interface WriteMetrics {
  logicalBytes: number;
  storedBytes: number;
  writeAmplification: number;
  encodeMs: number;
  stageMs: number;
  commitMs: number;
  totalMs: number;
  retries: number;
  rowsPerSecond: number;
}

export interface ReadTableOptions {
  version?: number;
  columns?: readonly string[];
}

export interface QueryOptions {
  readonly version?: number;
  /**
   * Budget for the documented modeled vector, row-index, group/result payload, and ordering buffers.
   * Boxed snapshot preparation, JavaScript container overhead, returned-result lifetime, and browser
   * allocator overhead are not included in this Phase 7B-B model.
   */
  readonly executionMemoryBudgetBytes?: number;
}

export interface DeleteBatchInput {
  keys: readonly BatchValue[];
}

export interface DeleteBatchResult {
  tableName: string;
  segmentId: string | null;
  requestedKeyCount: number;
  deletedRowCount: number;
  blockCount: number;
  storedBytes: number;
  version: number | null;
  metrics: WriteMetrics;
}

export interface CompactTableOptions {
  /** @deprecated Use minimumLevel0Segments. */
  minimumSegments?: number;
  /** Minimum L0 segments promoted; one also permits a direct L0 -> L2 promotion. */
  minimumLevel0Segments?: number;
  /** Target maximum L0 segments promoted by one job. Equal-order groups remain indivisible. */
  maxLevel0Segments?: number;
  /** Target maximum stored L0 bytes promoted by one job. The L1 anchor is excluded. */
  maxLevel0StoredBytes?: number;
  /** Number of immutable output blocks processed before yielding and checkpointing. */
  maxBlocksPerStep?: number;
  /** Output level. L2 is the append-only row-range partition policy. */
  targetLevel?: number;
  /** Hard L2 output-byte limit as a multiple of newly promoted L0 stored bytes. */
  maxWriteAmplification?: number;
  /** Estimated uncompressed physical bytes per output column block. */
  targetBlockBytes?: number;
  /** Compression used for rewritten output blocks. */
  outputCompression?: Compression;
  /** Upper bound for accounted JavaScript-owned executor buffers. */
  memoryBudgetBytes?: number;
}

export interface CompactTableStepOptions extends CompactTableOptions {
  maxBlocks?: number;
}

export type CompactionSkipReason =
  | "below-segment-threshold"
  | "contains-mutation-segments"
  | "non-contiguous-row-ids"
  | "unsupported-level-layout"
  | "write-amplification-budget";

export interface CompactTableResult {
  jobId?: string;
  tableName: string;
  compacted: boolean;
  skipReason?: CompactionSkipReason;
  sourceSegmentCount: number;
  sourceBlockCount: number;
  outputSegmentId: string | null;
  outputBlockCount: number;
  rowCount: number;
  sourceStoredBytes: number;
  outputStoredBytes: number;
  level0SourceStoredBytes?: number;
  anchorSourceStoredBytes?: number;
  compactionWriteAmplification?: number;
  outputPartitionOrdinal?: number;
  maxWriteAmplification?: number;
  maximumOutputStoredBytes?: number;
  plannedOutputStoredBytesUpperBound?: number;
  outputLogicalBytes?: number;
  targetBlockBytes?: number;
  outputCompression?: Compression;
  memoryBudgetBytes?: number;
  minimumMemoryBytes?: number;
  peakWorkingBytes?: number;
  supersededBlockCount: number;
  physicallyReclaimedBytes: 0;
  version: number | null;
  metrics: WriteMetrics | null;
}

export interface CompactionJobProgress {
  jobId: string | null;
  tableName: string;
  state: CompactionJobState | "skipped";
  processedRows: number;
  sourceSegmentCount: number;
  sourceBlockCount: number;
  outputBlockCount: number;
  level0SourceStoredBytes?: number;
  anchorSourceStoredBytes?: number;
  outputPartitionOrdinal?: number;
  maxWriteAmplification?: number;
  maximumOutputStoredBytes?: number;
  plannedOutputStoredBytesUpperBound?: number;
  memoryBudgetBytes?: number;
  minimumMemoryBytes?: number;
  peakWorkingBytes?: number;
  result: CompactTableResult | null;
}

export interface CancelCompactionJobResult {
  jobId: string;
  state: "cancelled" | "published" | "aborted";
  publishedVersion: number | null;
}

export interface CollectGarbageOptions {
  /** Maximum candidates examined and checkpointed by each durable reclamation step. */
  maxItemsPerStep?: number;
}

export interface CollectGarbageStepOptions {
  /** Maximum candidates examined and checkpointed by this durable reclamation step. */
  maxItems?: number;
}

export interface GarbageCollectionResult {
  jobId: string;
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
  physicallyReclaimedBytes: number;
}

export interface GarbageCollectionProgress {
  jobId: string;
  state: GarbageCollectionJobState;
  examinedManifestCount: number;
  examinedSegmentCount: number;
  examinedBlockCount: number;
  result: GarbageCollectionResult | null;
}

export interface TableDefinition {
  name: string;
  columns: ColumnDefinition[];
  uniqueKey?: string;
}

export type DatabaseRow = Record<string, Exclude<BatchValue, null> | null>;

export class UniqueConstraintError extends Error {
  override readonly name = "UniqueConstraintError";

  constructor(
    readonly tableName: string,
    readonly columnName: string,
    readonly value: Exclude<BatchValue, null>,
  ) {
    super(`Duplicate value for ${tableName}.${columnName}: ${formatValue(value)}`);
  }
}

export class MissingKeyError extends Error {
  override readonly name = "MissingKeyError";

  constructor(
    readonly tableName: string,
    readonly columnName: string,
    readonly value: Exclude<BatchValue, null>,
  ) {
    super(`Missing value for ${tableName}.${columnName}: ${formatValue(value)}`);
  }
}

export class CompactionMemoryBudgetError extends Error {
  override readonly name = "CompactionMemoryBudgetError";

  constructor(
    readonly budgetBytes: number,
    readonly minimumBytes: number,
  ) {
    super(
      `Compaction needs at least ${String(minimumBytes)} bytes of working memory; budget is ${String(budgetBytes)} bytes`,
    );
  }
}

export class CompactionWriteAmplificationError extends Error {
  override readonly name = "CompactionWriteAmplificationError";

  constructor(
    readonly outputBytes: number,
    readonly maximumOutputBytes: number,
  ) {
    super(
      `Compaction output would use ${String(outputBytes)} stored bytes; limit is ${String(maximumOutputBytes)} bytes`,
    );
  }
}

export class CompactionJobCancelledError extends Error {
  override readonly name = "CompactionJobCancelledError";

  constructor(readonly jobId: string) {
    super(`Compaction job cancelled: ${jobId}`);
  }
}

export interface BrowserDatabaseOptions {
  compression?: Compression;
  rowsPerBlock?: number;
  maxCommitRetries?: number;
  now?: () => Date;
  createId?: () => string;
}

export interface BufferedWriterOptions {
  mode?: "insert" | "upsert";
  maxRows?: number;
  maxBytes?: number;
  maxAgeMs?: number;
  onError?: (error: unknown) => void;
}

export type BufferedFlushResult = InsertBatchResult | UpsertBatchResult;

export interface LifecycleFlushRequester {
  requestFlush(): void;
}

export interface LifecycleDocumentTarget {
  readonly visibilityState: string;
  addEventListener(type: "visibilitychange", listener: () => void): void;
  removeEventListener(type: "visibilitychange", listener: () => void): void;
}

export interface LifecyclePageTarget {
  addEventListener(type: "pagehide", listener: () => void): void;
  removeEventListener(type: "pagehide", listener: () => void): void;
}

export interface LifecycleFlushOptions {
  document?: LifecycleDocumentTarget;
  page?: LifecyclePageTarget;
}

export interface VisibleSegment {
  id: string;
  rowCount: number;
  columnBlockIds: Readonly<Record<string, readonly string[]>>;
}

export class BrowserDatabase {
  readonly #transactions: TransactionManager;
  readonly #compression: Compression;
  readonly #rowsPerBlock: number;
  readonly #maxCommitRetries: number;
  readonly #now: () => Date;
  readonly #createId: () => string;
  readonly #internalLeaseOwnerId = `browserdatabase/${crypto.randomUUID()}`;
  #internalLeaseSequence = 0;

  constructor(
    private readonly store: BlockStore,
    options: BrowserDatabaseOptions = {},
  ) {
    this.#compression = options.compression ?? "raw";
    this.#rowsPerBlock = options.rowsPerBlock ?? 65_536;
    if (!Number.isSafeInteger(this.#rowsPerBlock) || this.#rowsPerBlock <= 0) {
      throw new RangeError("Rows per block must be a positive whole number");
    }
    this.#maxCommitRetries = options.maxCommitRetries ?? 8;
    if (!Number.isSafeInteger(this.#maxCommitRetries) || this.#maxCommitRetries < 0) {
      throw new RangeError("Commit retries must be a non-negative whole number");
    }
    this.#now = options.now ?? (() => new Date());
    this.#createId = options.createId ?? (() => crypto.randomUUID());
    this.#transactions = new TransactionManager(store, {
      now: this.#now,
      createId: this.#createId,
    });
  }

  async createTable(input: CreateTableInput): Promise<void> {
    const name = validateName(input.name, "Table");
    if (input.columns.length === 0) throw new TypeError("A table needs at least one column");
    const names = new Set<string>();
    const columns: TableColumnRecord[] = input.columns.map((column) => {
      const columnName = validateName(column.name, "Column");
      if (names.has(columnName)) throw new TypeError(`Duplicate column: ${columnName}`);
      names.add(columnName);
      if (!simpleDataTypes.includes(column.type)) {
        throw new TypeError(`Unsupported data type: ${column.type}`);
      }
      return {
        id: this.#createId(),
        name: columnName,
        type: column.type,
        nullable: column.nullable ?? false,
      };
    });
    const uniqueKeyColumn =
      input.uniqueKey === undefined
        ? undefined
        : columns.find((column) => column.name === input.uniqueKey);
    if (input.uniqueKey !== undefined && uniqueKeyColumn === undefined) {
      throw new TypeError(`Unique key column not found: ${input.uniqueKey}`);
    }
    if (uniqueKeyColumn?.nullable === true) {
      throw new TypeError(`Unique key cannot be nullable: ${uniqueKeyColumn.name}`);
    }
    await this.store.addTable({
      id: this.#createId(),
      name,
      columns,
      ...(uniqueKeyColumn === undefined ? {} : { uniqueKeyColumnId: uniqueKeyColumn.id }),
      ...(uniqueKeyColumn === undefined ? {} : { uniqueKeyLookupReady: true }),
      ...(uniqueKeyColumn === undefined ? {} : { uniqueKeyStorage: "chunks-v1" as const }),
      createdAt: this.#now().toISOString(),
    });
  }

  async listTables(): Promise<TableDefinition[]> {
    return (await this.store.listTables()).map((table) => {
      const uniqueKey = table.columns.find((column) => column.id === table.uniqueKeyColumnId)?.name;
      return {
        name: table.name,
        columns: table.columns.map((column) => ({
          name: column.name,
          type: column.type,
          nullable: column.nullable,
        })),
        ...(uniqueKey === undefined ? {} : { uniqueKey }),
      };
    });
  }

  async insertBatch(tableName: string, input: InsertBatchInput): Promise<InsertBatchResult> {
    const table = await this.#findTable(tableName);
    const rowCount = validateBatch(table, input);
    const keys = batchKeys(table, input);
    const result = await this.#writeBatch(table, input, "insert", keys);
    return {
      tableName: result.tableName,
      segmentId: result.segmentId,
      rowCount,
      blockCount: result.blockCount,
      storedBytes: result.storedBytes,
      version: result.version,
      metrics: result.metrics,
    };
  }

  async insert(
    tableName: string,
    row: Readonly<Record<string, BatchValue>>,
  ): Promise<InsertBatchResult> {
    return this.insertBatch(tableName, rowToBatch(row));
  }

  async upsertBatch(tableName: string, input: InsertBatchInput): Promise<UpsertBatchResult> {
    const table = await this.#findTable(tableName);
    const keyColumn = getUniqueKeyColumn(table);
    if (keyColumn === undefined) {
      throw new TypeError(`Table needs a unique key before it can be upserted: ${table.name}`);
    }
    const rowCount = validateBatch(table, input);
    const keys = batchKeys(table, input);
    if (keys === undefined) throw new Error("Unique key metadata is missing");
    const result = await this.#writeBatch(table, input, "upsert", keys);
    return { ...result, rowCount };
  }

  async upsert(
    tableName: string,
    row: Readonly<Record<string, BatchValue>>,
  ): Promise<UpsertBatchResult> {
    return this.upsertBatch(tableName, rowToBatch(row));
  }

  async updateBatch(tableName: string, input: UpdateBatchInput): Promise<UpdateBatchResult> {
    const table = await this.#findTable(tableName);
    const keyColumn = getUniqueKeyColumn(table);
    if (keyColumn === undefined) {
      throw new TypeError(`Table needs a unique key before rows can be updated: ${table.name}`);
    }
    const keys = validateUpdateBatch(table, keyColumn, input);
    return this.#writeUpdateBatch(table, keyColumn, input, keys);
  }

  async update(
    tableName: string,
    key: Exclude<BatchValue, null>,
    changes: Readonly<Record<string, BatchValue>>,
  ): Promise<UpdateBatchResult> {
    return this.updateBatch(tableName, {
      keys: [key],
      changes: Object.fromEntries(Object.entries(changes).map(([name, value]) => [name, [value]])),
    });
  }

  async deleteBatch(tableName: string, input: DeleteBatchInput): Promise<DeleteBatchResult> {
    const started = performance.now();
    const table = await this.#findTable(tableName);
    const keyColumn = getUniqueKeyColumn(table);
    if (keyColumn === undefined) {
      throw new TypeError(`Table needs a unique key before rows can be deleted: ${table.name}`);
    }
    if (input.keys.length === 0) throw new TypeError("A delete batch needs at least one key");
    const keys = new Map<string, Exclude<BatchValue, null>>();
    input.keys.forEach((value, index) => {
      validateValue(keyColumn, value, index);
      if (value === null) throw new TypeError(`Unique key cannot be null: ${keyColumn.name}`);
      const token = keyToken(keyColumn.type, value);
      if (keys.has(token))
        throw new TypeError(`Duplicate key in delete batch: ${formatValue(value)}`);
      keys.set(token, value);
    });
    const logicalBytes = estimateValuesBytes(input.keys);

    const transaction = await this.#transactions.begin();
    const segmentId = this.#createId();
    transaction.setUniqueKeyChanges({
      tableId: table.id,
      keyTokens: [...keys.keys()],
      requireAbsent: false,
      remove: true,
      ...(table.uniqueKeyStorage === undefined ? {} : { storageMode: table.uniqueKeyStorage }),
    });
    let deletedRowCount = 0;
    let storedBytes = 0;
    let blockCount = 0;
    let encodeMs = 0;
    let stageMs = 0;
    let commitMs = 0;
    let retries = 0;
    try {
      deletedRowCount = (
        await this.#existingKeyTokens(table, transaction.snapshotVersion, [...keys.keys()])
      ).size;
      if (deletedRowCount === 0) {
        await transaction.abort();
        return {
          tableName: table.name,
          segmentId: null,
          requestedKeyCount: keys.size,
          deletedRowCount: 0,
          blockCount: 0,
          storedBytes: 0,
          version: transaction.snapshotVersion,
          metrics: createWriteMetrics({
            logicalBytes,
            storedBytes: 0,
            encodeMs,
            stageMs,
            commitMs,
            totalMs: performance.now() - started,
            retries,
            rows: 0,
          }),
        };
      }

      const blockIds: string[] = [];
      const blockWrites: Array<{ id: string; bytes: Uint8Array }> = [];
      const values = [...keys.values()];
      for (let start = 0, part = 0; start < values.length; start += this.#rowsPerBlock, part += 1) {
        const slice = values.slice(start, Math.min(start + this.#rowsPerBlock, values.length));
        const encodeStarted = performance.now();
        const bytes = await encodeBlock(asColumnInput(keyColumn.type, slice), this.#compression);
        encodeMs += performance.now() - encodeStarted;
        const blockId = [
          "table",
          table.id,
          "segment",
          segmentId,
          "delete-key",
          keyColumn.id,
          "part",
          String(part).padStart(6, "0"),
        ].join("/");
        blockWrites.push({ id: blockId, bytes });
        blockIds.push(blockId);
        storedBytes += bytes.byteLength;
        blockCount += 1;
      }
      let stageStarted = performance.now();
      await transaction.stageBlocks(blockWrites);
      stageMs += performance.now() - stageStarted;
      stageStarted = performance.now();
      await transaction.stageSegment({
        id: segmentId,
        tableId: table.id,
        transactionId: transaction.id,
        rowCount: keys.size,
        rowIdStart: 0n,
        rowIdEndExclusive: 0n,
        columnBlockIds: { [keyColumn.id]: blockIds },
        kind: "delete",
        keyColumnId: keyColumn.id,
        level: 0,
        createdAt: this.#now().toISOString(),
      });
      stageMs += performance.now() - stageStarted;

      for (let attempt = 0; attempt <= this.#maxCommitRetries; attempt += 1) {
        const commitStarted = performance.now();
        try {
          const manifest = await transaction.commit();
          commitMs += performance.now() - commitStarted;
          return {
            tableName: table.name,
            segmentId,
            requestedKeyCount: keys.size,
            deletedRowCount,
            blockCount,
            storedBytes,
            version: manifest.version,
            metrics: createWriteMetrics({
              logicalBytes,
              storedBytes,
              encodeMs,
              stageMs,
              commitMs,
              totalMs: performance.now() - started,
              retries,
              rows: deletedRowCount,
            }),
          };
        } catch (error) {
          commitMs += performance.now() - commitStarted;
          if (!(error instanceof WriteConflictError) || attempt === this.#maxCommitRetries) {
            throw error;
          }
          retries += 1;
          const snapshot = await transaction.rebase();
          deletedRowCount = (
            await this.#existingKeyTokens(table, snapshot.version, [...keys.keys()])
          ).size;
        }
      }
      throw new Error("Commit retry limit was exceeded");
    } catch (error) {
      await transaction.abort();
      throw error;
    }
  }

  bufferedWriter(tableName: string, options: BufferedWriterOptions = {}): BufferedTableWriter {
    return new BufferedTableWriter(this, tableName, options);
  }

  async #writeUpdateBatch(
    table: TableRecord,
    keyColumn: TableColumnRecord,
    input: UpdateBatchInput,
    keys: Map<string, Exclude<BatchValue, null>>,
  ): Promise<UpdateBatchResult> {
    const started = performance.now();
    const logicalBytes =
      estimateValuesBytes(input.keys) +
      Object.values(input.changes).reduce(
        (total, values) => total + estimateValuesBytes(values),
        0,
      );
    const transaction = await this.#transactions.begin();
    const segmentId = this.#createId();
    const columnBlockIds: Record<string, string[]> = {};
    const changedColumns = Object.keys(input.changes).sort();
    let storedBytes = 0;
    let blockCount = 0;
    let encodeMs = 0;
    let stageMs = 0;
    let commitMs = 0;
    let retries = 0;

    try {
      await this.#assertKeysExist(table, keyColumn, transaction.snapshotVersion, keys);
      const columns = [keyColumn, ...changedColumns.map((name) => findColumn(table, name))];
      for (const column of columns) {
        const values = column.id === keyColumn.id ? input.keys : (input.changes[column.name] ?? []);
        const blockIds: string[] = [];
        const blockWrites: Array<{ id: string; bytes: Uint8Array }> = [];
        const encodeStarted = performance.now();
        for (
          let start = 0, part = 0;
          start < input.keys.length;
          start += this.#rowsPerBlock, part += 1
        ) {
          const slice = values.slice(
            start,
            Math.min(start + this.#rowsPerBlock, input.keys.length),
          );
          const bytes = await encodeBlock(asColumnInput(column.type, slice), this.#compression);
          const blockId = [
            "table",
            table.id,
            "segment",
            segmentId,
            "update-column",
            column.id,
            "part",
            String(part).padStart(6, "0"),
          ].join("/");
          blockWrites.push({ id: blockId, bytes });
          blockIds.push(blockId);
        }
        encodeMs += performance.now() - encodeStarted;
        columnBlockIds[column.id] = blockIds;
        storedBytes += sumBytes(blockWrites);
        blockCount += blockWrites.length;
        const blocksStageStarted = performance.now();
        await transaction.stageBlocks(blockWrites);
        stageMs += performance.now() - blocksStageStarted;
      }
      const segmentStageStarted = performance.now();
      await transaction.stageSegment({
        id: segmentId,
        tableId: table.id,
        transactionId: transaction.id,
        rowCount: input.keys.length,
        rowIdStart: 0n,
        rowIdEndExclusive: 0n,
        columnBlockIds,
        kind: "update",
        keyColumnId: keyColumn.id,
        level: 0,
        createdAt: this.#now().toISOString(),
      });
      stageMs += performance.now() - segmentStageStarted;

      for (let attempt = 0; attempt <= this.#maxCommitRetries; attempt += 1) {
        const commitStarted = performance.now();
        try {
          const manifest = await transaction.commit();
          commitMs += performance.now() - commitStarted;
          return {
            tableName: table.name,
            segmentId,
            requestedRowCount: input.keys.length,
            updatedRowCount: input.keys.length,
            changedColumns,
            blockCount,
            storedBytes,
            version: manifest.version,
            metrics: createWriteMetrics({
              logicalBytes,
              storedBytes,
              encodeMs,
              stageMs,
              commitMs,
              totalMs: performance.now() - started,
              retries,
              rows: input.keys.length,
            }),
          };
        } catch (error) {
          commitMs += performance.now() - commitStarted;
          if (!(error instanceof WriteConflictError) || attempt === this.#maxCommitRetries) {
            throw error;
          }
          retries += 1;
          const snapshot = await transaction.rebase();
          await this.#assertKeysExist(table, keyColumn, snapshot.version, keys);
        }
      }
      throw new Error("Commit retry limit was exceeded");
    } catch (error) {
      await transaction.abort();
      throw error;
    }
  }

  async #assertKeysExist(
    table: TableRecord,
    keyColumn: TableColumnRecord,
    version: number | null,
    keys: Map<string, Exclude<BatchValue, null>>,
  ): Promise<void> {
    const existing = await this.#existingKeyTokens(table, version, [...keys.keys()]);
    if (existing.size === keys.size) return;
    const missing = [...keys].find(([token]) => !existing.has(token));
    if (missing !== undefined) {
      throw new MissingKeyError(table.name, keyColumn.name, missing[1]);
    }
    throw new Error(`Unique-key lookup returned an inconsistent result: ${table.name}`);
  }

  async #writeBatch(
    table: TableRecord,
    input: InsertBatchInput,
    kind: "insert" | "upsert",
    keys: Map<string, Exclude<BatchValue, null>> | undefined,
  ): Promise<UpsertBatchResult> {
    const started = performance.now();
    const rowCount = input.columns[table.columns[0]?.name ?? ""]?.length ?? 0;
    const logicalBytes = estimateBatchBytes(input);
    const transaction = await this.#transactions.begin();
    if (keys !== undefined) {
      transaction.setUniqueKeyChanges({
        tableId: table.id,
        keyTokens: [...keys.keys()],
        requireAbsent: kind === "insert",
        ...(table.uniqueKeyStorage === undefined ? {} : { storageMode: table.uniqueKeyStorage }),
      });
    }
    const segmentId = this.#createId();
    const columnBlockIds: Record<string, string[]> = {};
    const batchBlockWrites: Array<{ id: string; bytes: Uint8Array }> = [];
    let storedBytes = 0;
    let blockCount = 0;
    let counts = { inserted: 0, updated: 0 };
    let encodeMs = 0;
    let stageMs = 0;
    let commitMs = 0;
    let retries = 0;

    try {
      counts =
        kind === "insert" && keys !== undefined
          ? { inserted: keys.size, updated: 0 }
          : await this.#classifyKeys(table, transaction.snapshotVersion, kind, keys);
      const rowIds = await this.store.reserveRowIds(table.id, rowCount);
      for (const column of table.columns) {
        const values = input.columns[column.name] ?? [];
        const blockIds: string[] = [];
        const blockWrites: Array<{ id: string; bytes: Uint8Array }> = [];
        const encodeStarted = performance.now();
        for (let start = 0, part = 0; start < rowCount; start += this.#rowsPerBlock, part += 1) {
          const slice = values.slice(start, Math.min(start + this.#rowsPerBlock, rowCount));
          const bytes = await encodeBlock(asColumnInput(column.type, slice), this.#compression);
          const blockId = [
            "table",
            table.id,
            "segment",
            segmentId,
            "column",
            column.id,
            "part",
            String(part).padStart(6, "0"),
          ].join("/");
          blockWrites.push({ id: blockId, bytes });
          blockIds.push(blockId);
        }
        encodeMs += performance.now() - encodeStarted;
        columnBlockIds[column.id] = blockIds;
        storedBytes += sumBytes(blockWrites);
        blockCount += blockWrites.length;
        batchBlockWrites.push(...blockWrites);
      }

      const blocksStageStarted = performance.now();
      await transaction.stageBlocks(batchBlockWrites);
      stageMs += performance.now() - blocksStageStarted;

      const segment: SegmentRecord = {
        id: segmentId,
        tableId: table.id,
        transactionId: transaction.id,
        rowCount,
        rowIdStart: rowIds.start,
        rowIdEndExclusive: rowIds.endExclusive,
        columnBlockIds,
        kind,
        ...(table.uniqueKeyColumnId === undefined ? {} : { keyColumnId: table.uniqueKeyColumnId }),
        level: 0,
        createdAt: this.#now().toISOString(),
      };
      const segmentStageStarted = performance.now();
      await transaction.stageSegment(segment);
      stageMs += performance.now() - segmentStageStarted;

      for (let attempt = 0; attempt <= this.#maxCommitRetries; attempt += 1) {
        const commitStarted = performance.now();
        try {
          const manifest = await transaction.commit();
          commitMs += performance.now() - commitStarted;
          return {
            tableName: table.name,
            segmentId,
            rowCount,
            blockCount,
            storedBytes,
            version: manifest.version,
            insertedRowCount: counts.inserted,
            updatedRowCount: counts.updated,
            metrics: createWriteMetrics({
              logicalBytes,
              storedBytes,
              encodeMs,
              stageMs,
              commitMs,
              totalMs: performance.now() - started,
              retries,
              rows: rowCount,
            }),
          };
        } catch (error) {
          commitMs += performance.now() - commitStarted;
          if (!(error instanceof WriteConflictError) || attempt === this.#maxCommitRetries) {
            throw error;
          }
          retries += 1;
          const snapshot = await transaction.rebase();
          counts =
            kind === "insert" && keys !== undefined
              ? { inserted: keys.size, updated: 0 }
              : await this.#classifyKeys(table, snapshot.version, kind, keys);
        }
      }
      throw new Error("Commit retry limit was exceeded");
    } catch (error) {
      await transaction.abort();
      if (error instanceof UniqueKeyConflictError && keys !== undefined) {
        const keyColumn = getUniqueKeyColumn(table);
        const value = keys.get(error.keyToken);
        if (keyColumn !== undefined && value !== undefined) {
          throw new UniqueConstraintError(table.name, keyColumn.name, value);
        }
      }
      throw error;
    }
  }

  async readTable(
    tableName: string,
    versionOrOptions?: number | ReadTableOptions,
  ): Promise<DatabaseRow[]> {
    const table = await this.#findTable(tableName);
    const options =
      typeof versionOrOptions === "number" ? { version: versionOrOptions } : versionOrOptions;
    const columns = resolveReadColumns(table, options?.columns);
    return this.#materializeTable(table, options?.version, columns);
  }

  /**
   * Compiles a read-only SELECT statement and materializes one stable snapshot.
   * Repeated execute() calls measure query execution without including storage I/O.
   */
  async prepareQuery(sql: string, options: QueryOptions = {}): Promise<PreparedQuery> {
    const plan = compileQuery(sql);
    const memory = new QueryMemoryContext(options.executionMemoryBudgetBytes);
    try {
      const tableNames = [plan.base.table, ...plan.joins.map((join) => join.table)];
      const uniqueTableNames = [...new Set(tableNames)];
      const tables = await Promise.all(uniqueTableNames.map((name) => this.#findTable(name)));
      const schemas = new Map(
        tables.map((table) => [table.name, table.columns.map(({ name }) => name)]),
      );
      const columns = referencedColumns(plan, schemas);
      const columnarTables = new Map<string, ColumnarTable>();
      await this.#withLeasedSnapshot(options.version, async (snapshot) => {
        for (const table of tables) {
          const requestedColumns = columns.get(table.name);
          columnarTables.set(
            table.name,
            await this.#materializeColumnarTableAtSnapshot(
              table,
              snapshot,
              resolveReadColumns(
                table,
                requestedColumns?.length === 0 ? [table.columns[0]?.name ?? ""] : requestedColumns,
              ),
            ),
          );
        }
      });
      return createPreparedColumnarQuery(plan, columnarTables, memory);
    } catch (error) {
      memory.close();
      throw error;
    }
  }

  /** Executes a read-only SELECT statement through the public query API. */
  async query(sql: string, options: QueryOptions = {}): Promise<QueryResult> {
    const prepared = await this.prepareQuery(sql, options);
    try {
      return prepared.execute();
    } finally {
      prepared.close();
    }
  }

  async listVisibleSegments(tableName: string, version?: number): Promise<VisibleSegment[]> {
    const table = await this.#findTable(tableName);
    return this.#withLeasedSnapshot(version, async (snapshot) =>
      (await this.#visibleSegmentRecords(table, snapshot)).map((segment) => ({
        id: segment.id,
        rowCount: segment.rowCount,
        columnBlockIds: structuredClone(segment.columnBlockIds),
      })),
    );
  }

  async compactTable(
    tableName: string,
    options: CompactTableOptions = {},
  ): Promise<CompactTableResult> {
    const maxBlocks = positiveWholeNumber(
      options.maxBlocksPerStep ?? 16,
      "Compaction blocks per step",
    );
    let progress = await this.compactTableStep(tableName, { ...options, maxBlocks });
    while (progress.result === null) {
      if (progress.jobId === null) throw new Error("Compaction progress lost its job ID");
      progress = await this.resumeCompactionJob(progress.jobId, { maxBlocks });
    }
    return progress.result;
  }

  /** Plans or advances one restart-safe physical compaction job. */
  async compactTableStep(
    tableName: string,
    options: CompactTableStepOptions = {},
  ): Promise<CompactionJobProgress> {
    const table = await this.#findTable(tableName);
    const active = (await this.store.listCompactionJobs(table.id)).find((job) =>
      isActiveCompactionState(job.state),
    );
    let job = active;
    if (job === undefined) {
      const planned = await this.#planCompaction(table, options);
      if ("compacted" in planned) return compactionSkippedProgress(planned);
      job = planned;
    }
    return this.#runCompactionJob(
      table,
      job,
      positiveWholeNumber(
        options.maxBlocks ?? options.maxBlocksPerStep ?? 1,
        "Compaction step block limit",
      ),
    );
  }

  /** Continues a persisted compaction job after a cooperative yield or restart. */
  async resumeCompactionJob(
    jobId: string,
    options: { maxBlocks?: number } = {},
  ): Promise<CompactionJobProgress> {
    const job = await this.store.getCompactionJob(jobId);
    if (job === undefined) throw new Error(`Compaction job not found: ${jobId}`);
    const table = await this.store.getTable(job.tableId);
    if (table === undefined) throw new Error(`Compaction table not found: ${job.tableId}`);
    return this.#runCompactionJob(
      table,
      job,
      positiveWholeNumber(options.maxBlocks ?? 1, "Compaction step block limit"),
    );
  }

  async listCompactionJobs(tableName?: string): Promise<CompactionJobRecord[]> {
    if (tableName === undefined) return this.store.listCompactionJobs();
    const table = await this.#findTable(tableName);
    return this.store.listCompactionJobs(table.id);
  }

  /** Atomically prevents an unpublished compaction job from committing its transaction. */
  async cancelCompactionJob(jobId: string): Promise<CancelCompactionJobResult> {
    for (;;) {
      const job = await this.store.getCompactionJob(jobId);
      if (job === undefined) throw new Error(`Compaction job not found: ${jobId}`);
      if (job.state === "cancelled" || job.state === "published" || job.state === "aborted") {
        return compactionCancellationResult(job);
      }
      try {
        return compactionCancellationResult(
          await this.store.cancelCompactionJob(job.id, job.revision, this.#now().toISOString()),
        );
      } catch (error) {
        if (error instanceof CompactionJobConflictError) continue;
        throw error;
      }
    }
  }

  /** Runs restart-safe lease-aware reclamation to completion in bounded durable steps. */
  async collectGarbage(options: CollectGarbageOptions = {}): Promise<GarbageCollectionResult> {
    const maxItems = positiveWholeNumber(
      options.maxItemsPerStep ?? 64,
      "Garbage collection items per step",
    );
    let progress = await this.collectGarbageStep({ maxItems });
    while (progress.result === null) {
      progress = await this.resumeGarbageCollectionJob(progress.jobId, { maxItems });
    }
    return progress.result;
  }

  /** Plans or advances one durable garbage-collection pass. */
  async collectGarbageStep(
    options: CollectGarbageStepOptions = {},
  ): Promise<GarbageCollectionProgress> {
    const active = (await this.store.listGarbageCollectionJobs()).find(
      (job) => job.state === "planned" || job.state === "running",
    );
    const job = active ?? (await this.#planGarbageCollection());
    return this.#runGarbageCollectionJob(
      job,
      positiveWholeNumber(options.maxItems ?? 1, "Garbage collection item limit"),
    );
  }

  /** Continues a persisted reclamation pass after a cooperative yield or restart. */
  async resumeGarbageCollectionJob(
    jobId: string,
    options: CollectGarbageStepOptions = {},
  ): Promise<GarbageCollectionProgress> {
    const job = await this.store.getGarbageCollectionJob(jobId);
    if (job === undefined) throw new Error(`Garbage collection job not found: ${jobId}`);
    return this.#runGarbageCollectionJob(
      job,
      positiveWholeNumber(options.maxItems ?? 1, "Garbage collection item limit"),
    );
  }

  async listGarbageCollectionJobs(): Promise<GarbageCollectionJobRecord[]> {
    return this.store.listGarbageCollectionJobs();
  }

  async #planGarbageCollection(): Promise<GarbageCollectionJobRecord> {
    const [current, manifests, transactions, compactionJobs] = await Promise.all([
      this.store.getCurrentManifest(),
      this.store.listManifests(),
      this.store.listTransactions(),
      this.store.listCompactionJobs(),
    ]);
    const historicalManifests = manifests.filter(
      (manifest) => manifest.version !== current?.version && manifest.prunedAt === undefined,
    );
    const terminalTransactions = transactions.filter((record) => record.status === "aborted");
    const terminalCompactionJobs = compactionJobs.filter(
      (job) => job.state === "published" || job.state === "cancelled" || job.state === "aborted",
    );
    const candidateBlockIds = new Set(historicalManifests.flatMap((manifest) => manifest.blockIds));
    const candidateSegmentIds = new Set<string>();
    for (const transaction of terminalTransactions) {
      transaction.pendingBlockIds.forEach((id) => candidateBlockIds.add(id));
      transaction.pendingSegmentIds.forEach((id) => candidateSegmentIds.add(id));
    }
    for (const job of terminalCompactionJobs) {
      job.sourceBlockIds.forEach((id) => candidateBlockIds.add(id));
      job.outputBlockIds.forEach((id) => candidateBlockIds.add(id));
      job.sourceSegmentIds.forEach((id) => candidateSegmentIds.add(id));
      if (job.outputSegmentId !== null) candidateSegmentIds.add(job.outputSegmentId);
    }
    const timestamp = this.#now().toISOString();
    return this.store.createGarbageCollectionJob({
      id: `garbage-collection/${this.#createId()}`,
      candidateManifestVersions: historicalManifests.map((manifest) => manifest.version),
      candidateSegmentIds: [...candidateSegmentIds],
      candidateBlockIds: [...candidateBlockIds],
      leaseCutoff: timestamp,
      createdAt: timestamp,
    });
  }

  async #runGarbageCollectionJob(
    initialJob: GarbageCollectionJobRecord,
    maxItems: number,
  ): Promise<GarbageCollectionProgress> {
    let job = initialJob;
    for (;;) {
      if (job.state === "completed") return garbageCollectionProgress(job);
      try {
        const step = await this.store.runGarbageCollectionStep({
          jobId: job.id,
          expectedRevision: job.revision,
          maxItems,
          updatedAt: this.#now().toISOString(),
        });
        return garbageCollectionProgress(step.job);
      } catch (error) {
        if (!(error instanceof GarbageCollectionJobConflictError)) throw error;
        const latest = await this.store.getGarbageCollectionJob(job.id);
        if (latest === undefined) throw new Error(`Garbage collection job not found: ${job.id}`);
        job = latest;
      }
    }
  }

  async #planCompaction(
    table: TableRecord,
    options: CompactTableOptions,
  ): Promise<CompactionJobRecord | CompactTableResult> {
    for (;;) {
      const manifest = await this.store.getCurrentManifest();
      const version = manifest?.version ?? null;
      try {
        return await this.#withLeasedSnapshot(version, (snapshot) =>
          this.#planCompactionAtSnapshot(table, options, version, snapshot),
        );
      } catch (error) {
        if (error instanceof SnapshotManifestMissingError) continue;
        throw error;
      }
    }
  }

  async #planCompactionAtSnapshot(
    table: TableRecord,
    options: CompactTableOptions,
    version: number | null,
    snapshot: LeasedSnapshot,
  ): Promise<CompactionJobRecord | CompactTableResult> {
    const legacyMinimumSegments: number | undefined = Reflect.get(options, "minimumSegments");
    if (
      options.minimumLevel0Segments !== undefined &&
      legacyMinimumSegments !== undefined &&
      options.minimumLevel0Segments !== legacyMinimumSegments
    ) {
      throw new RangeError("Compaction L0 segment thresholds conflict");
    }
    const minimumLevel0Segments = positiveWholeNumber(
      options.minimumLevel0Segments ??
        legacyMinimumSegments ??
        DEFAULT_COMPACTION_MINIMUM_LEVEL_ZERO_SEGMENTS,
      "Compaction minimum L0 segments",
    );
    const maxLevel0Segments = positiveWholeNumber(
      options.maxLevel0Segments ?? DEFAULT_COMPACTION_MAXIMUM_LEVEL_ZERO_SEGMENTS,
      "Compaction maximum L0 segments",
    );
    if (maxLevel0Segments < minimumLevel0Segments) {
      throw new RangeError("Compaction maximum L0 segments cannot be smaller than its minimum");
    }
    const maxLevel0StoredBytes = positiveWholeNumber(
      options.maxLevel0StoredBytes ?? DEFAULT_COMPACTION_MAXIMUM_LEVEL_ZERO_STORED_BYTES,
      "Compaction maximum L0 stored bytes",
    );
    const visibleSegments = await this.#visibleSegmentRecords(table, snapshot);
    const visibleBlockIds = uniqueSegmentBlockIds(visibleSegments);
    const inferredLevelTwoLayout = appendLevelTwoLayout(visibleSegments);
    const inferredTargetLevel =
      inferredLevelTwoLayout !== null && inferredLevelTwoLayout.levelTwoSegments.length > 0 ? 2 : 1;
    const targetLevel = positiveWholeNumber(
      options.targetLevel ?? inferredTargetLevel,
      "Compaction target level",
    );
    if (targetLevel !== 1 && targetLevel !== 2) {
      throw new RangeError("Compaction target level must be 1 or 2");
    }
    if (targetLevel === 1 && options.maxWriteAmplification !== undefined) {
      throw new RangeError("Compaction write amplification is only supported for L2 output");
    }
    const levelTwoMaxWriteAmplification =
      targetLevel === 2
        ? positiveFiniteNumber(
            options.maxWriteAmplification ?? DEFAULT_LEVEL_TWO_MAX_WRITE_AMPLIFICATION,
            "Compaction maximum write amplification",
          )
        : undefined;

    let anchor: SegmentRecord | undefined;
    let level0Segments: readonly SegmentRecord[];
    let effectiveMinimumLevel0Segments: number;
    let outputPartitionOrdinal: number | undefined;
    if (targetLevel === 1) {
      const firstLevel = visibleSegments[0]?.level ?? 0;
      const hasAnchor = firstLevel === 1;
      const level0Offset = hasAnchor ? 1 : 0;
      const supportedLevelLayout =
        firstLevel <= 1 &&
        visibleSegments.slice(level0Offset).every((segment) => (segment.level ?? 0) === 0);
      if (!supportedLevelLayout) {
        return compactTableSkipped(
          table.name,
          "unsupported-level-layout",
          visibleSegments,
          visibleBlockIds,
          version,
        );
      }
      anchor = hasAnchor ? visibleSegments[0] : undefined;
      level0Segments = visibleSegments.slice(level0Offset);
      effectiveMinimumLevel0Segments = hasAnchor
        ? minimumLevel0Segments
        : Math.max(2, minimumLevel0Segments);
    } else {
      if (
        table.uniqueKeyColumnId !== undefined ||
        visibleSegments.some(
          (segment) => (segment.kind ?? "insert") !== "insert" || segment.rowIdSpans !== undefined,
        )
      ) {
        return compactTableSkipped(
          table.name,
          "contains-mutation-segments",
          visibleSegments,
          visibleBlockIds,
          version,
        );
      }
      const layout = appendLevelTwoLayout(visibleSegments);
      if (layout === null) {
        return compactTableSkipped(
          table.name,
          "unsupported-level-layout",
          visibleSegments,
          visibleBlockIds,
          version,
        );
      }
      level0Segments = layout.level0Segments;
      effectiveMinimumLevel0Segments = minimumLevel0Segments;
      outputPartitionOrdinal = layout.levelTwoSegments.length;
    }
    if (level0Segments.length < effectiveMinimumLevel0Segments) {
      return compactTableSkipped(
        table.name,
        "below-segment-threshold",
        visibleSegments,
        visibleBlockIds,
        version,
      );
    }
    const selection = await this.#selectCompactionSources(
      anchor,
      level0Segments,
      effectiveMinimumLevel0Segments,
      maxLevel0Segments,
      maxLevel0StoredBytes,
      snapshot,
    );
    const sourceSegments = selection.sourceSegments;
    const sourceBlockIds = uniqueSegmentBlockIds(sourceSegments);
    const hasContiguousSourceRowIds = hasContiguousRowIds(sourceSegments);
    const hasPositiveSourceRowIds = (sourceSegments[0]?.rowIdStart ?? 0n) > 0n;
    const requiresMerge =
      targetLevel === 1 &&
      (sourceSegments.some(
        (segment) => (segment.kind ?? "insert") !== "insert" || segment.rowIdSpans !== undefined,
      ) ||
        (!hasContiguousSourceRowIds && table.uniqueKeyColumnId !== undefined));
    if (
      !requiresMerge &&
      (!hasContiguousSourceRowIds || (targetLevel === 2 && !hasPositiveSourceRowIds))
    ) {
      return compactTableSkipped(
        table.name,
        "non-contiguous-row-ids",
        sourceSegments,
        sourceBlockIds,
        version,
      );
    }
    if (version === null) throw new Error("Visible compaction segments require a manifest");

    const targetBlockBytes = positiveWholeNumber(
      options.targetBlockBytes ?? DEFAULT_COMPACTION_TARGET_BLOCK_BYTES,
      "Compaction target block bytes",
    );
    if (targetBlockBytes > MAX_COMPACTION_TARGET_BLOCK_BYTES) {
      throw new RangeError(
        `Compaction target block bytes cannot exceed ${String(MAX_COMPACTION_TARGET_BLOCK_BYTES)}`,
      );
    }
    const outputCompression = validateCompression(
      options.outputCompression ?? "gzip",
      "Compaction output compression",
    );
    if (
      getCompressionMemoryBound(outputCompression, targetBlockBytes).maximumOutputBytes >
      MAX_COMPACTION_TARGET_BLOCK_BYTES
    ) {
      throw new RangeError(
        `Compaction target block bytes exceed the ${outputCompression} worst-case format limit`,
      );
    }
    const memoryBudgetBytes = positiveWholeNumber(
      options.memoryBudgetBytes ?? DEFAULT_COMPACTION_MEMORY_BUDGET_BYTES,
      "Compaction memory budget",
    );
    const rewritePlan = requiresMerge
      ? await this.#createMergeCompactionPlan(
          table,
          sourceSegments,
          targetBlockBytes,
          outputCompression,
          memoryBudgetBytes,
          snapshot,
        )
      : await this.#createRechunkCompactionPlan(
          table,
          sourceSegments,
          targetBlockBytes,
          outputCompression,
          memoryBudgetBytes,
          snapshot,
        );
    const minimumMemoryBytes = compactionMinimumMemoryBytes(rewritePlan);
    if (minimumMemoryBytes > memoryBudgetBytes) {
      throw new CompactionMemoryBudgetError(memoryBudgetBytes, minimumMemoryBytes);
    }
    const sourceStoredBytes = physicalRewriteSourceStoredBytes(rewritePlan);
    if (
      selection.anchorSourceStoredBytes + selection.level0SourceStoredBytes !==
      sourceStoredBytes
    ) {
      throw new Error("Compaction source byte accounting differs from its rewrite plan");
    }

    let levelTwoBudget:
      | {
          outputPartitionOrdinal: number;
          maxWriteAmplification: number;
          maximumOutputStoredBytes: number;
          plannedOutputStoredBytesUpperBound: number;
        }
      | undefined;
    if (targetLevel === 2) {
      if (rewritePlan.kind !== "rechunk-v1" || outputPartitionOrdinal === undefined) {
        throw new Error("L2 compaction requires an append-only physical rewrite");
      }
      const maxWriteAmplification = levelTwoMaxWriteAmplification;
      if (maxWriteAmplification === undefined) {
        throw new Error("L2 compaction write-amplification policy is missing");
      }
      const maximumOutputStoredBytes = floorWholeNumberProduct(
        selection.level0SourceStoredBytes,
        maxWriteAmplification,
        "Compaction maximum output stored bytes",
      );
      const plannedOutputStoredBytesUpperBound =
        await this.#plannedPhysicalOutputStoredBytesUpperBound(rewritePlan, snapshot);
      levelTwoBudget = {
        outputPartitionOrdinal,
        maxWriteAmplification,
        maximumOutputStoredBytes,
        plannedOutputStoredBytesUpperBound,
      };
      if (plannedOutputStoredBytesUpperBound > maximumOutputStoredBytes) {
        return compactionWriteAmplificationSkipped({
          tableName: table.name,
          sourceSegments,
          sourceBlockIds,
          sourceStoredBytes,
          level0SourceStoredBytes: selection.level0SourceStoredBytes,
          outputPartitionOrdinal,
          maxWriteAmplification,
          maximumOutputStoredBytes,
          plannedOutputStoredBytesUpperBound,
          targetBlockBytes,
          outputCompression,
          memoryBudgetBytes,
          minimumMemoryBytes,
          version,
        });
      }
    }

    let id = ["compaction", table.id, "manifest", String(version)].join("/");
    const existing = await this.store.getCompactionJob(id);
    if (existing !== undefined && existing.state !== "aborted" && existing.state !== "cancelled") {
      return existing;
    }
    if (existing !== undefined) id = `${id}/retry/${this.#createId()}`;
    const timestamp = this.#now().toISOString();
    const job: CompactionJobRecord = {
      id,
      tableId: table.id,
      sourceManifestVersion: version,
      sourceSegmentIds: sourceSegments.map((segment) => segment.id),
      sourceBlockIds,
      outputBlockIds: [],
      cursor: { sourceSegmentIndex: 0, sourceBlockIndex: 0 },
      processedRows: 0,
      sourceStoredBytes,
      level0SourceStoredBytes: selection.level0SourceStoredBytes,
      anchorSourceStoredBytes: selection.anchorSourceStoredBytes,
      outputStoredBytes: 0,
      logicalBytes: physicalRewriteSourceEncodedBytes(rewritePlan),
      rewritePlan,
      outputCursor: {
        outputIndex: 0,
        columnIndex: 0,
        rowStart: rewritePlan.outputs[0]?.rowStart ?? 0,
      },
      memoryBudgetBytes,
      minimumMemoryBytes,
      ...(levelTwoBudget ?? {}),
      peakWorkingBytes: 0,
      outputLogicalBytes: 0,
      targetLevel,
      state: "planned",
      transactionId: null,
      outputSegmentId:
        rewritePlan.kind === "merge-v1" && rewritePlan.totalRows === 0
          ? null
          : `${id}/output-segment`,
      publishedVersion: null,
      revision: 0,
      createdAt: timestamp,
      updatedAt: timestamp,
    };
    await snapshot.renew(INTERNAL_READ_LEASE_TTL_MS);
    try {
      await this.store.createCompactionJob(job);
      return job;
    } catch (error) {
      const raced = await this.store.getCompactionJob(id);
      if (raced !== undefined) return raced;
      throw error;
    }
  }

  async #selectCompactionSources(
    anchor: SegmentRecord | undefined,
    level0Segments: readonly SegmentRecord[],
    minimumLevel0Segments: number,
    maxLevel0Segments: number,
    maxLevel0StoredBytes: number,
    snapshot: LeasedSnapshot,
  ): Promise<{
    sourceSegments: SegmentRecord[];
    level0SourceStoredBytes: number;
    anchorSourceStoredBytes: number;
  }> {
    const transactions = new Map(
      (await this.store.listTransactions()).map((record) => [record.id, record]),
    );
    const logicalOrder = (segment: SegmentRecord): number => {
      const owner = transactions.get(segment.transactionId);
      if (owner?.status !== "committed" || owner.committedVersion === null) {
        throw new Error(`Compaction source segment has no committed owner: ${segment.id}`);
      }
      return segment.logicalOrder ?? owner.committedVersion;
    };
    const seenBlockIds = new Set<string>();
    const measureStoredBytes = async (
      segments: readonly SegmentRecord[],
    ): Promise<{ storedBytes: number; blockIds: string[]; duplicateBlockId: string | null }> => {
      let total = 0;
      const blockIds: string[] = [];
      const measuredBlockIds = new Set<string>();
      let duplicateBlockId: string | null = null;
      for (const segment of segments) {
        for (const blockId of Object.values(segment.columnBlockIds).flat()) {
          if (seenBlockIds.has(blockId) || measuredBlockIds.has(blockId)) {
            duplicateBlockId ??= blockId;
          }
          measuredBlockIds.add(blockId);
          blockIds.push(blockId);
          await this.#renewInternalLeaseIfNeeded(snapshot);
          const bytes = await this.store.getBlock(blockId);
          if (bytes === undefined)
            throw new Error(`Compaction source block is missing: ${blockId}`);
          total = safeWholeNumberSum([total, bytes.byteLength], "Compaction selected stored bytes");
        }
      }
      return { storedBytes: total, blockIds, duplicateBlockId };
    };
    const acceptMeasurement = (measurement: {
      blockIds: readonly string[];
      duplicateBlockId: string | null;
    }): void => {
      if (measurement.duplicateBlockId !== null) {
        throw new Error(
          `Compaction source block is referenced more than once: ${measurement.duplicateBlockId}`,
        );
      }
      measurement.blockIds.forEach((blockId) => seenBlockIds.add(blockId));
    };

    let anchorSourceStoredBytes = 0;
    if (anchor !== undefined) {
      const anchorMeasurement = await measureStoredBytes([anchor]);
      acceptMeasurement(anchorMeasurement);
      anchorSourceStoredBytes = anchorMeasurement.storedBytes;
    }
    const selectedLevel0: SegmentRecord[] = [];
    let level0SourceStoredBytes = 0;
    for (let start = 0; start < level0Segments.length;) {
      const first = level0Segments[start];
      if (first === undefined) throw new Error("Compaction L0 source selection is unavailable");
      const order = logicalOrder(first);
      let end = start + 1;
      for (;;) {
        const next = level0Segments[end];
        if (next === undefined || logicalOrder(next) !== order) break;
        end += 1;
      }
      const group = level0Segments.slice(start, end);
      if (
        selectedLevel0.length >= minimumLevel0Segments &&
        selectedLevel0.length + group.length > maxLevel0Segments
      ) {
        break;
      }
      const groupMeasurement = await measureStoredBytes(group);
      if (
        selectedLevel0.length >= minimumLevel0Segments &&
        groupMeasurement.storedBytes > maxLevel0StoredBytes - level0SourceStoredBytes
      ) {
        break;
      }
      acceptMeasurement(groupMeasurement);
      selectedLevel0.push(...group);
      level0SourceStoredBytes = safeWholeNumberSum(
        [level0SourceStoredBytes, groupMeasurement.storedBytes],
        "Compaction selected L0 stored bytes",
      );
      start = end;
    }
    if (selectedLevel0.length < minimumLevel0Segments) {
      throw new Error("Compaction source selection did not satisfy its minimum L0 segment count");
    }
    return {
      sourceSegments: anchor === undefined ? selectedLevel0 : [anchor, ...selectedLevel0],
      level0SourceStoredBytes,
      anchorSourceStoredBytes,
    };
  }

  async #createRechunkCompactionPlan(
    table: TableRecord,
    sourceSegments: readonly SegmentRecord[],
    targetBlockBytes: number,
    outputCompression: Compression,
    memoryBudgetBytes: number,
    snapshot: LeasedSnapshot,
  ): Promise<RechunkCompactionRewritePlan> {
    const first = sourceSegments[0];
    const last = sourceSegments[sourceSegments.length - 1];
    if (first === undefined || last === undefined) {
      throw new Error("Compaction needs at least one source segment");
    }
    const totalRows = safeWholeNumberSum(
      sourceSegments.map((segment) => segment.rowCount),
      "Compaction source row count",
    );
    const columns: RechunkCompactionSourceColumn[] = [];
    let maximumEncodedBytesPerRow = 0;

    for (const column of table.columns) {
      const sourceBlocks: RechunkCompactionSourceBlock[] = [];
      let rowStart = 0;
      for (const segment of sourceSegments) {
        const segmentStart = rowStart;
        const blockIds = segment.columnBlockIds[column.id] ?? [];
        if (blockIds.length === 0) {
          throw new Error(`Compaction source column is missing blocks: ${column.name}`);
        }
        for (const blockId of blockIds) {
          await this.#renewInternalLeaseIfNeeded(snapshot);
          const bytes = await this.store.getBlock(blockId);
          if (bytes === undefined)
            throw new Error(`Compaction source block is missing: ${blockId}`);
          const description = inspectBlock(bytes);
          if (description.type !== column.type) {
            throw new Error(`Compaction source block type differs: ${blockId}`);
          }
          if (description.rowCount === 0) {
            throw new Error(`Compaction source block is empty: ${blockId}`);
          }
          sourceBlocks.push({
            blockId,
            rowStart,
            rowCount: description.rowCount,
            storedBytes: bytes.byteLength,
            encodedBytes: description.encodedLength,
            checksum: description.checksum,
          });
          maximumEncodedBytesPerRow = Math.max(
            maximumEncodedBytesPerRow,
            description.encodedLength / description.rowCount,
          );
          rowStart = safeWholeNumberSum(
            [rowStart, description.rowCount],
            "Compaction source block rows",
          );
        }
        if (rowStart - segmentStart !== segment.rowCount) {
          throw new Error(`Compaction source blocks do not cover segment ${segment.id}`);
        }
      }
      if (rowStart !== totalRows) {
        throw new Error(`Compaction source column does not cover every row: ${column.name}`);
      }
      columns.push({ columnId: column.id, type: column.type, sourceBlocks });
    }

    if (!Number.isFinite(maximumEncodedBytesPerRow) || maximumEncodedBytesPerRow <= 0) {
      throw new Error("Compaction could not estimate an output block size");
    }
    const rowsPerOutput = Math.max(
      1,
      Math.min(0xffff_ffff, Math.floor(targetBlockBytes / maximumEncodedBytesPerRow)),
    );
    const estimatedOutputs: RechunkCompactionOutputWindow[] = [];
    for (let rowStart = 0; rowStart < totalRows; rowStart += rowsPerOutput) {
      estimatedOutputs.push({
        rowStart,
        rowCount: Math.min(rowsPerOutput, totalRows - rowStart),
      });
    }
    const outputs = await this.#refinePhysicalOutputWindows(
      rechunkPhysicalColumns(columns),
      estimatedOutputs,
      targetBlockBytes,
      outputCompression,
      memoryBudgetBytes,
      snapshot,
    );

    return {
      kind: "rechunk-v1",
      targetBlockBytes,
      outputCompression,
      totalRows,
      rowIdStart: first.rowIdStart,
      rowIdEndExclusive: last.rowIdEndExclusive,
      logicalOrder: await this.#firstLogicalOrder(sourceSegments),
      columns,
      outputs,
    };
  }

  async #createMergeCompactionPlan(
    table: TableRecord,
    sourceSegments: readonly SegmentRecord[],
    targetBlockBytes: number,
    outputCompression: Compression,
    memoryBudgetBytes: number,
    snapshot: LeasedSnapshot,
  ): Promise<MergeCompactionRewritePlan> {
    const keyColumn = getUniqueKeyColumn(table);
    if (keyColumn === undefined) {
      throw new Error(`Mutation compaction requires a unique key: ${table.name}`);
    }
    const transactions = new Map(
      (await this.store.listTransactions()).map((record) => [record.id, record]),
    );
    const describedSegments: MergeCompactionSourceSegment[] = [];
    const sourceBlocksById = new Map<string, MergeCompactionSourceBlock>();

    for (const segment of sourceSegments) {
      const transaction = transactions.get(segment.transactionId);
      if (transaction?.status !== "committed" || transaction.committedVersion === null) {
        throw new Error(`Compaction source segment has no committed owner: ${segment.id}`);
      }
      const kind = segment.kind ?? "insert";
      const keyColumnId = segment.keyColumnId ?? table.uniqueKeyColumnId ?? null;
      if (keyColumnId !== keyColumn.id) {
        throw new Error(`Compaction source key differs from the table key: ${segment.id}`);
      }
      const unknownColumnId = Object.keys(segment.columnBlockIds).find(
        (columnId) => !table.columns.some((column) => column.id === columnId),
      );
      if (unknownColumnId !== undefined) {
        throw new Error(`Compaction source contains an unknown column: ${unknownColumnId}`);
      }
      const columns: MergeCompactionSourceColumn[] = [];
      for (const column of table.columns) {
        const blockIds = segment.columnBlockIds[column.id] ?? [];
        if (blockIds.length === 0) continue;
        const sourceBlocks: MergeCompactionSourceBlock[] = [];
        let rowStart = 0;
        for (const blockId of blockIds) {
          await this.#renewInternalLeaseIfNeeded(snapshot);
          const bytes = await this.store.getBlock(blockId);
          if (bytes === undefined) {
            throw new Error(`Compaction source block is missing: ${blockId}`);
          }
          const description = inspectBlock(bytes);
          if (description.type !== column.type || description.rowCount === 0) {
            throw new Error(`Compaction source block differs from table schema: ${blockId}`);
          }
          if (sourceBlocksById.has(blockId)) {
            throw new Error(`Compaction source block is referenced more than once: ${blockId}`);
          }
          const sourceBlock: MergeCompactionSourceBlock = {
            blockId,
            rowStart,
            rowCount: description.rowCount,
            storedBytes: bytes.byteLength,
            encodedBytes: description.encodedLength,
            checksum: description.checksum,
          };
          sourceBlocks.push(sourceBlock);
          sourceBlocksById.set(blockId, sourceBlock);
          rowStart = safeWholeNumberSum(
            [rowStart, description.rowCount],
            "Mutation compaction source rows",
          );
        }
        if (rowStart !== segment.rowCount) {
          throw new Error(`Compaction source blocks do not cover segment ${segment.id}`);
        }
        columns.push({ columnId: column.id, type: column.type, sourceBlocks });
      }
      validateMergeSegmentShape(table, segment.id, kind, columns, keyColumn.id);
      describedSegments.push({
        segmentId: segment.id,
        transactionId: segment.transactionId,
        committedVersion: transaction.committedVersion,
        kind,
        keyColumnId,
        level: segment.level ?? 0,
        logicalOrder: segment.logicalOrder ?? transaction.committedVersion,
        rowCount: segment.rowCount,
        rowIdStart: segment.rowIdStart,
        rowIdEndExclusive: segment.rowIdEndExclusive,
        rowIdSpans: mergeSourceRowIdSpans(segment, kind),
        columns,
      });
    }

    assertCanonicalMergeSourceOrder(describedSegments);
    const resolved = await this.#resolveMergeOutput(
      table,
      describedSegments,
      keyColumn,
      memoryBudgetBytes,
      snapshot,
    );
    const { columns, rowIdSpans, totalRows } = resolved;
    const rowIdEnvelope = rowIdSpanEnvelope(rowIdSpans);
    let outputs: RechunkCompactionOutputWindow[] = [];
    if (totalRows > 0) {
      let maximumEncodedBytesPerRow = 0;
      for (const column of columns) {
        for (const range of column.sourceRanges) {
          const source = sourceBlocksById.get(range.sourceBlockId);
          if (source === undefined) {
            throw new Error(
              `Mutation compaction source fingerprint is missing: ${range.sourceBlockId}`,
            );
          }
          maximumEncodedBytesPerRow = Math.max(
            maximumEncodedBytesPerRow,
            source.encodedBytes / source.rowCount,
          );
        }
      }
      if (!Number.isFinite(maximumEncodedBytesPerRow) || maximumEncodedBytesPerRow <= 0) {
        throw new Error("Compaction could not estimate an output block size");
      }
      const rowsPerOutput = Math.max(
        1,
        Math.min(0xffff_ffff, Math.floor(targetBlockBytes / maximumEncodedBytesPerRow)),
      );
      const estimatedOutputs: RechunkCompactionOutputWindow[] = [];
      for (let rowStart = 0; rowStart < totalRows; rowStart += rowsPerOutput) {
        estimatedOutputs.push({
          rowStart,
          rowCount: Math.min(rowsPerOutput, totalRows - rowStart),
        });
      }
      outputs = await this.#refinePhysicalOutputWindows(
        mergePhysicalColumns(columns, describedSegments),
        estimatedOutputs,
        targetBlockBytes,
        outputCompression,
        memoryBudgetBytes,
        snapshot,
      );
    }

    return {
      kind: "merge-v1",
      targetBlockBytes,
      outputCompression,
      keyColumnId: keyColumn.id,
      totalRows,
      rowIdStart: rowIdEnvelope.start,
      rowIdEndExclusive: rowIdEnvelope.endExclusive,
      rowIdSpans,
      logicalOrder: Math.min(...describedSegments.map((segment) => segment.logicalOrder)),
      sourceSegments: describedSegments,
      columns,
      outputs,
    };
  }

  async #resolveMergeOutput(
    table: TableRecord,
    segments: readonly MergeCompactionSourceSegment[],
    keyColumn: TableColumnRecord,
    memoryBudgetBytes: number,
    snapshot: LeasedSnapshot,
  ): Promise<{
    columns: MergeCompactionOutputColumn[];
    rowIdSpans: RowIdSpan[];
    totalRows: number;
  }> {
    const plannerMemoryBytes = mergePlannerMemoryBound(table, segments, keyColumn.id);
    if (plannerMemoryBytes > memoryBudgetBytes) {
      throw new CompactionMemoryBudgetError(memoryBudgetBytes, plannerMemoryBytes);
    }
    const columnIndexById = new Map(table.columns.map((column, index) => [column.id, index]));
    const rows: Array<MergeResolvedRow | undefined> = [];
    const rowIndexByKey = new Map<string, number>();
    for (const segment of segments) {
      if (segment.kind === "delete") {
        await this.#forEachMergeSourceKey(segment, keyColumn, snapshot, (value) => {
          const token = keyToken(keyColumn.type, value);
          const existingIndex = rowIndexByKey.get(token);
          if (existingIndex !== undefined) rows[existingIndex] = undefined;
          rowIndexByKey.delete(token);
        });
        continue;
      }
      if (segment.kind === "update") {
        const changedColumnIds = segment.columns
          .map((column) => column.columnId)
          .filter((columnId) => columnId !== keyColumn.id);
        await this.#forEachMergeSourceKey(segment, keyColumn, snapshot, (value, rowIndex) => {
          const token = keyToken(keyColumn.type, value);
          const existingIndex = rowIndexByKey.get(token);
          const existing = existingIndex === undefined ? undefined : rows[existingIndex];
          if (existingIndex === undefined || existing === undefined) {
            throw new Error(`Update segment references a missing key: ${segment.segmentId}`);
          }
          const sources = [...existing.sources];
          for (const columnId of changedColumnIds) {
            const columnIndex = columnIndexById.get(columnId);
            if (columnIndex === undefined) {
              throw new Error(`Mutation compaction column is missing: ${columnId}`);
            }
            sources[columnIndex] = mergeSourceAt(segment, columnId, rowIndex);
          }
          rows[existingIndex] = { rowId: existing.rowId, sources };
        });
        continue;
      }

      await this.#forEachMergeSourceKey(segment, keyColumn, snapshot, (value, rowIndex) => {
        const token = keyToken(keyColumn.type, value);
        const existingIndex = rowIndexByKey.get(token);
        const sources = table.columns.map((column) => mergeSourceAt(segment, column.id, rowIndex));
        if (segment.kind === "upsert" && existingIndex !== undefined) {
          const existing = rows[existingIndex];
          if (existing === undefined) {
            throw new Error(`Upsert segment references an invalid row slot: ${segment.segmentId}`);
          }
          rows[existingIndex] = { rowId: existing.rowId, sources };
          return;
        }
        if (existingIndex !== undefined) {
          throw new Error(`Insert segment contains a duplicate unique key: ${segment.segmentId}`);
        }
        rowIndexByKey.set(token, rows.length);
        rows.push({ rowId: rowIdAt(segment.rowIdSpans, rowIndex), sources });
      });
    }

    const visibleRows = rows.filter((row): row is MergeResolvedRow => row !== undefined);
    return {
      rowIdSpans: coalesceRowIdSpans(visibleRows.map((row) => row.rowId)),
      columns: table.columns.map((column, columnIndex) => ({
        columnId: column.id,
        type: column.type,
        sourceRanges: coalesceMergeOutputRanges(
          visibleRows.map((row) => {
            const source = row.sources[columnIndex];
            if (source === undefined) {
              throw new Error(`Mutation compaction row is missing column ${column.name}`);
            }
            return source;
          }),
        ),
      })),
      totalRows: visibleRows.length,
    };
  }

  async #forEachMergeSourceKey(
    segment: MergeCompactionSourceSegment,
    column: TableColumnRecord,
    snapshot: LeasedSnapshot,
    action: (value: BatchValue, rowIndex: number) => void,
  ): Promise<void> {
    const planned = segment.columns.find((candidate) => candidate.columnId === column.id);
    if (planned === undefined) {
      throw new Error(`Mutation segment has no key column: ${segment.segmentId}`);
    }
    let rowIndex = 0;
    for (const source of planned.sourceBlocks) {
      await this.#renewInternalLeaseIfNeeded(snapshot);
      const bytes = await this.store.getBlock(source.blockId);
      if (bytes === undefined)
        throw new Error(`Compaction source block is missing: ${source.blockId}`);
      const description = inspectBlock(bytes);
      if (
        bytes.byteLength !== source.storedBytes ||
        description.encodedLength !== source.encodedBytes ||
        description.checksum !== source.checksum ||
        description.rowCount !== source.rowCount ||
        description.type !== column.type
      ) {
        throw new Error(`Compaction source block differs from its plan: ${source.blockId}`);
      }
      for (const value of (await decodeBlock(bytes)).column.values) {
        action(value, rowIndex);
        rowIndex += 1;
      }
    }
    if (rowIndex !== segment.rowCount) {
      throw new Error(`Mutation segment key rows differ: ${segment.segmentId}`);
    }
  }

  async #refinePhysicalOutputWindows(
    columns: readonly PhysicalCompactionSourceColumn[],
    estimatedOutputs: readonly RechunkCompactionOutputWindow[],
    targetBlockBytes: number,
    outputCompression: Compression,
    memoryBudgetBytes: number,
    snapshot: LeasedSnapshot,
  ): Promise<RechunkCompactionOutputWindow[]> {
    const outputs: RechunkCompactionOutputWindow[] = [];
    for (const output of estimatedOutputs) {
      outputs.push(
        ...(await this.#refinePhysicalOutputWindow(
          columns,
          output,
          targetBlockBytes,
          outputCompression,
          memoryBudgetBytes,
          snapshot,
        )),
      );
    }
    return outputs;
  }

  async #refinePhysicalOutputWindow(
    columns: readonly PhysicalCompactionSourceColumn[],
    output: RechunkCompactionOutputWindow,
    targetBlockBytes: number,
    outputCompression: Compression,
    memoryBudgetBytes: number,
    snapshot: LeasedSnapshot,
  ): Promise<RechunkCompactionOutputWindow[]> {
    let splitReason: "target" | "format" | "memory" | null = null;
    const requiredMemoryBytes = Math.max(
      ...columns.map((column) => physicalOutputMemoryBound(column, output, outputCompression)),
    );
    if (requiredMemoryBytes > memoryBudgetBytes) {
      splitReason = "memory";
    } else {
      for (const column of columns) {
        let encodedByteLength: number;
        try {
          encodedByteLength = (
            await this.#measurePhysicalCompactionOutput(
              column,
              output,
              outputCompression,
              memoryBudgetBytes,
              snapshot,
            )
          ).encodedByteLength;
        } catch (error) {
          if (!isPhysicalColumnLimitError(error)) throw error;
          splitReason = "format";
          break;
        }
        if (
          getCompressionMemoryBound(outputCompression, encodedByteLength).maximumOutputBytes >
          MAX_COMPACTION_TARGET_BLOCK_BYTES
        ) {
          splitReason = "format";
          break;
        }
        if (encodedByteLength > targetBlockBytes) {
          splitReason = "target";
        }
      }
    }
    if (splitReason === null || (splitReason === "target" && output.rowCount === 1)) {
      return [output];
    }
    if (output.rowCount === 1) {
      if (splitReason === "memory") {
        throw new CompactionMemoryBudgetError(memoryBudgetBytes, requiredMemoryBytes);
      }
      throw new RangeError(
        `A single compaction row exceeds the ${outputCompression} block format limit`,
      );
    }

    const leftRowCount = Math.floor(output.rowCount / 2);
    const left = { rowStart: output.rowStart, rowCount: leftRowCount };
    const right = {
      rowStart: safeWholeNumberSum(
        [output.rowStart, leftRowCount],
        "Compaction split output row start",
      ),
      rowCount: output.rowCount - leftRowCount,
    };
    return [
      ...(await this.#refinePhysicalOutputWindow(
        columns,
        left,
        targetBlockBytes,
        outputCompression,
        memoryBudgetBytes,
        snapshot,
      )),
      ...(await this.#refinePhysicalOutputWindow(
        columns,
        right,
        targetBlockBytes,
        outputCompression,
        memoryBudgetBytes,
        snapshot,
      )),
    ];
  }

  async #measurePhysicalCompactionOutput(
    column: PhysicalCompactionSourceColumn,
    output: RechunkCompactionOutputWindow,
    outputCompression: Compression,
    memoryBudgetBytes: number,
    snapshot: LeasedSnapshot,
  ) {
    const loaded = await this.#loadPhysicalCompactionRanges(
      column,
      output,
      outputCompression,
      memoryBudgetBytes,
      snapshot,
    );
    return measurePhysicalColumnRanges(column.type, loaded.ranges);
  }

  async #plannedPhysicalOutputStoredBytesUpperBound(
    plan: PhysicalCompactionRewritePlan,
    snapshot: LeasedSnapshot,
  ): Promise<number> {
    const layout = physicalRewriteLayout(plan);
    let total = 0;
    for (const output of layout.outputs) {
      for (const column of layout.columns) {
        const measurement = await this.#measurePhysicalCompactionOutput(
          column,
          output,
          plan.outputCompression,
          Number.MAX_SAFE_INTEGER,
          snapshot,
        );
        total = safeWholeNumberSum(
          [
            total,
            maximumPhysicalBlockByteLength(
              measurement.encodedByteLength,
              measurement.metadata,
              plan.outputCompression,
            ),
          ],
          "Compaction planned output stored-byte upper bound",
        );
      }
    }
    return total;
  }

  async #runCompactionJob(
    table: TableRecord,
    initialJob: CompactionJobRecord,
    maxBlocks: number,
  ): Promise<CompactionJobProgress> {
    let job = (await this.store.getCompactionJob(initialJob.id)) ?? initialJob;
    if (job.state === "cancelled") throw new CompactionJobCancelledError(job.id);
    if (job.state === "aborted") throw new Error(job.error ?? `Compaction job aborted: ${job.id}`);
    if (job.state === "published") {
      return this.#publishedCompactionProgress(table, job);
    }
    const linkedTransaction =
      job.transactionId === null ? undefined : await this.store.getTransaction(job.transactionId);
    if (linkedTransaction?.status === "committed" && linkedTransaction.committedVersion !== null) {
      job = await this.#markCompactionPublished(job, linkedTransaction.committedVersion);
      return this.#publishedCompactionProgress(table, job);
    }
    const rewritePlan = job.rewritePlan ?? { kind: "copy-v1" as const };
    if (rewritePlan.kind !== "copy-v1") validatePhysicalTablePlan(table, rewritePlan);
    const sourceSegments =
      rewritePlan.kind === "copy-v1" ? await this.#loadCompactionSources(job) : [];

    let transaction: DatabaseTransaction;
    try {
      if (job.transactionId === null) {
        const linked = await this.#beginCompactionTransaction(job);
        if (linked.transaction === null) {
          return await this.#runCompactionJob(table, linked.job, maxBlocks);
        }
        ({ job, transaction } = linked);
      } else {
        if (linkedTransaction?.status !== "active") {
          const linked = await this.#beginCompactionTransaction(job);
          if (linked.transaction === null) {
            return await this.#runCompactionJob(table, linked.job, maxBlocks);
          }
          ({ job, transaction } = linked);
        } else {
          transaction = await this.#transactions.resume(job.transactionId);
          if (job.state !== "running") {
            job = await this.store.updateCompactionJob(job.id, job.revision, {
              state: "running",
              updatedAt: this.#now().toISOString(),
              error: null,
            });
          }
        }
      }
    } catch (error) {
      if ((await this.store.getCompactionJob(job.id))?.state === "cancelled") {
        throw new CompactionJobCancelledError(job.id);
      }
      throw error;
    }

    try {
      if (job.outputBlockIds.length > 0) {
        const pendingBlockIds = new Set(transaction.pendingBlockIds);
        const unjournaledOutputIds = job.outputBlockIds.filter((id) => !pendingBlockIds.has(id));
        if (unjournaledOutputIds.length > 0) {
          await transaction.stageExistingBlocks(unjournaledOutputIds);
        }
      }
      if (rewritePlan.kind !== "copy-v1") {
        job = await this.#advancePhysicalCompaction(job, transaction, rewritePlan, maxBlocks);
        if ((job.outputCursor?.outputIndex ?? 0) < rewritePlan.outputs.length) {
          return compactionProgress(table.name, job, null);
        }
      } else {
        let processedBlocks = 0;
        while (
          job.cursor.sourceSegmentIndex < sourceSegments.length &&
          processedBlocks < maxBlocks
        ) {
          const segmentIndex = job.cursor.sourceSegmentIndex;
          const segment = sourceSegments[segmentIndex];
          if (segment === undefined) throw new Error("Compaction source cursor is invalid");
          const entries = compactionBlockEntries(table, segment, segmentIndex, job.id);
          const entry = entries[job.cursor.sourceBlockIndex];
          if (entry === undefined) {
            job = await this.store.updateCompactionJob(job.id, job.revision, {
              cursor: { sourceSegmentIndex: segmentIndex + 1, sourceBlockIndex: 0 },
              processedRows: job.processedRows + segment.rowCount,
              updatedAt: this.#now().toISOString(),
            });
            continue;
          }

          const source = await this.store.getBlock(entry.sourceBlockId);
          if (source === undefined) {
            throw new Error(`A compaction source block is missing: ${entry.sourceBlockId}`);
          }
          const existing = await this.store.getBlock(entry.outputBlockId);
          if (existing === undefined) {
            await transaction.stageBlock(entry.outputBlockId, source);
          } else {
            if (!sameBytes(existing, source)) {
              throw new Error(`A resumed compaction block differs: ${entry.outputBlockId}`);
            }
            await transaction.stageExistingBlocks([entry.outputBlockId]);
          }

          const segmentComplete = job.cursor.sourceBlockIndex + 1 === entries.length;
          const description = inspectBlock(source);
          job = await this.store.updateCompactionJob(job.id, job.revision, {
            outputBlockIds: [...job.outputBlockIds, entry.outputBlockId],
            cursor: segmentComplete
              ? { sourceSegmentIndex: segmentIndex + 1, sourceBlockIndex: 0 }
              : {
                  sourceSegmentIndex: segmentIndex,
                  sourceBlockIndex: job.cursor.sourceBlockIndex + 1,
                },
            processedRows: job.processedRows + (segmentComplete ? segment.rowCount : 0),
            sourceStoredBytes: job.sourceStoredBytes + source.byteLength,
            outputStoredBytes: job.outputStoredBytes + source.byteLength,
            logicalBytes: job.logicalBytes + description.encodedLength,
            updatedAt: this.#now().toISOString(),
            error: null,
          });
          processedBlocks += 1;
        }

        if (job.cursor.sourceSegmentIndex < sourceSegments.length) {
          return compactionProgress(table.name, job, null);
        }
      }

      const expectedOutputIds =
        rewritePlan.kind !== "copy-v1"
          ? physicalOutputBlockIds(job.id, rewritePlan)
          : sourceSegments.flatMap((segment, segmentIndex) =>
              compactionBlockEntries(table, segment, segmentIndex, job.id).map(
                (entry) => entry.outputBlockId,
              ),
            );
      const pendingOutputIds = new Set(transaction.pendingBlockIds);
      const unjournaledOutputId = expectedOutputIds.find((id) => !pendingOutputIds.has(id));
      if (unjournaledOutputId !== undefined) {
        throw new Error(`Compaction output block was not journaled: ${unjournaledOutputId}`);
      }
      const outputSegmentId = job.outputSegmentId;
      const first = sourceSegments[0];
      const last = sourceSegments[sourceSegments.length - 1];
      if (rewritePlan.kind === "copy-v1" && (first === undefined || last === undefined)) {
        throw new Error("Compaction sources disappeared");
      }
      const outputRowCount =
        rewritePlan.kind === "copy-v1"
          ? sourceSegments.reduce((total, segment) => total + segment.rowCount, 0)
          : rewritePlan.totalRows;
      if (outputRowCount === 0) {
        if (outputSegmentId !== null) {
          throw new Error("An empty compaction cannot have an output segment");
        }
      } else {
        if (outputSegmentId === null) throw new Error("Compaction output segment ID is missing");
        const desiredOutputSegment: SegmentRecord = {
          id: outputSegmentId,
          tableId: table.id,
          transactionId: transaction.id,
          rowCount: outputRowCount,
          rowIdStart:
            rewritePlan.kind === "copy-v1" ? (first?.rowIdStart ?? 0n) : rewritePlan.rowIdStart,
          rowIdEndExclusive:
            rewritePlan.kind === "copy-v1"
              ? (last?.rowIdEndExclusive ?? 0n)
              : rewritePlan.rowIdEndExclusive,
          columnBlockIds:
            rewritePlan.kind === "copy-v1"
              ? compactionOutputColumns(table, sourceSegments, job.id)
              : physicalOutputColumns(job.id, rewritePlan),
          kind: rewritePlan.kind === "merge-v1" ? "base" : "insert",
          ...(table.uniqueKeyColumnId === undefined
            ? {}
            : { keyColumnId: table.uniqueKeyColumnId }),
          level: job.targetLevel,
          ...(job.outputPartitionOrdinal === undefined
            ? {}
            : { partitionOrdinal: job.outputPartitionOrdinal }),
          logicalOrder:
            rewritePlan.kind === "copy-v1"
              ? await this.#firstLogicalOrder(sourceSegments)
              : rewritePlan.logicalOrder,
          ...(rewritePlan.kind === "merge-v1"
            ? { rowIdSpans: structuredClone(rewritePlan.rowIdSpans) }
            : {}),
          createdAt: this.#now().toISOString(),
        };
        const outputSegment = await this.store.getSegment(outputSegmentId);
        if (outputSegment === undefined) {
          await transaction.stageSegment(desiredOutputSegment);
        } else if (outputSegment.transactionId === transaction.id) {
          if (!sameCompactionSegment(outputSegment, desiredOutputSegment)) {
            throw new Error(`A resumed compaction segment differs: ${outputSegmentId}`);
          }
          await transaction.stageExistingSegment(outputSegmentId);
        } else {
          const owner = await this.store.getTransaction(outputSegment.transactionId);
          if (
            (owner !== undefined && owner.status !== "aborted") ||
            !sameCompactionSegment(outputSegment, desiredOutputSegment)
          ) {
            throw new Error(`Compaction output segment cannot be adopted: ${outputSegmentId}`);
          }
          const visible = (await this.store.listManifests()).some((manifest) => {
            if (manifest.prunedAt !== undefined) return false;
            const blockIds = new Set(manifest.blockIds);
            return expectedOutputIds.every((id) => blockIds.has(id));
          });
          if (visible) {
            throw new Error(`Compaction output segment is already visible: ${outputSegmentId}`);
          }
          await this.store.removeSegment(outputSegmentId);
          await transaction.stageSegment(desiredOutputSegment);
        }
      }
      if (job.state !== "ready") {
        job = await this.store.updateCompactionJob(job.id, job.revision, {
          outputBlockIds: expectedOutputIds,
          state: "ready",
          updatedAt: this.#now().toISOString(),
          error: null,
        });
      }

      transaction.supersedeBlocks(job.sourceBlockIds);
      let manifest;
      try {
        manifest = await transaction.commit();
      } catch (error) {
        if (!(error instanceof WriteConflictError)) throw error;
        const current = await this.store.getCurrentManifest();
        const currentIds = new Set(current?.blockIds ?? []);
        if (job.sourceBlockIds.some((id) => !currentIds.has(id))) {
          if (transaction.status === "active") await transaction.abort();
          job = await this.#abortCompactionJob(
            job,
            "Compaction sources changed before publication",
          );
          throw new Error(job.error);
        }
        const rebased = await transaction.rebase();
        try {
          await this.#assertCompactionSnapshotOrder(job, rebased);
        } catch (error) {
          if (transaction.status === "active") await transaction.abort();
          job = await this.#abortCompactionJob(job, errorMessage(error));
          throw new Error(job.error);
        }
        const missingSourceId = job.sourceBlockIds.find((id) => !rebased.hasBlock(id));
        if (missingSourceId !== undefined) {
          if (transaction.status === "active") await transaction.abort();
          job = await this.#abortCompactionJob(
            job,
            `Compaction source is no longer visible: ${missingSourceId}`,
          );
          throw new Error(job.error);
        }
        transaction.supersedeBlocks(job.sourceBlockIds);
        manifest = await transaction.commit();
      }
      job = await this.#markCompactionPublished(job, manifest.version);
      return compactionProgress(
        table.name,
        job,
        this.#compactionResult(table, job, manifest.version),
      );
    } catch (error) {
      const latest = await this.store.getCompactionJob(job.id);
      if (latest?.state === "cancelled") {
        throw new CompactionJobCancelledError(job.id);
      }
      if (error instanceof CompactionWriteAmplificationError) {
        if (transaction.status === "active") await transaction.abort();
        if (latest !== undefined && latest.state !== "published" && latest.state !== "aborted") {
          try {
            await this.#abortCompactionJob(latest, error.message);
          } catch (updateError) {
            if (!(updateError instanceof CompactionJobConflictError)) throw updateError;
          }
        }
        throw error;
      }
      if (
        !(error instanceof CompactionJobConflictError) &&
        !(error instanceof TransactionRecordConflictError) &&
        latest?.revision === job.revision &&
        latest.state !== "published" &&
        latest.state !== "aborted"
      ) {
        try {
          await this.store.updateCompactionJob(latest.id, latest.revision, {
            updatedAt: this.#now().toISOString(),
            error: errorMessage(error),
          });
        } catch {
          // Another coordinator advanced the same persisted job.
        }
      }
      throw error;
    }
  }

  async #advancePhysicalCompaction(
    initialJob: CompactionJobRecord,
    transaction: DatabaseTransaction,
    plan: PhysicalCompactionRewritePlan,
    maxBlocks: number,
  ): Promise<CompactionJobRecord> {
    const layout = physicalRewriteLayout(plan);
    let job = initialJob;
    let processedBlocks = 0;
    while ((job.outputCursor?.outputIndex ?? 0) < layout.outputs.length) {
      if (processedBlocks >= maxBlocks) break;
      const cursor = job.outputCursor;
      if (cursor === null || cursor === undefined) {
        throw new Error("Physical compaction cursor is missing");
      }
      const output = layout.outputs[cursor.outputIndex];
      const column = layout.columns[cursor.columnIndex];
      if (output === undefined || column === undefined) {
        throw new Error("Physical compaction cursor is invalid");
      }
      const outputBlockId = physicalOutputBlockId(job.id, cursor.outputIndex, cursor.columnIndex);
      const built = await this.#buildPhysicalCompactionOutput(
        layout,
        column,
        output,
        job.memoryBudgetBytes ?? 0,
      );
      const existing = await this.store.getBlock(outputBlockId);
      let outputBytes: Uint8Array;
      if (existing === undefined) {
        outputBytes = await encodePhysicalBlock(built.physical, plan.outputCompression);
      } else {
        const decoded = await decodePhysicalBlock(existing);
        if (
          decoded.description.type !== column.type ||
          decoded.description.compression !== plan.outputCompression ||
          decoded.description.rowCount !== output.rowCount ||
          !sameBytes(decoded.column.bytes, built.physical.bytes)
        ) {
          throw new Error(`A resumed compaction block differs: ${outputBlockId}`);
        }
        outputBytes = existing;
      }
      const nextOutputStoredBytes = safeWholeNumberSum(
        [job.outputStoredBytes, outputBytes.byteLength],
        "Compaction output stored bytes",
      );
      const maximumOutputStoredBytes = job.maximumOutputStoredBytes;
      const plannedOutputStoredBytesUpperBound = job.plannedOutputStoredBytesUpperBound;
      if (
        (maximumOutputStoredBytes !== undefined &&
          nextOutputStoredBytes > maximumOutputStoredBytes) ||
        (plannedOutputStoredBytesUpperBound !== undefined &&
          nextOutputStoredBytes > plannedOutputStoredBytesUpperBound)
      ) {
        throw new CompactionWriteAmplificationError(
          nextOutputStoredBytes,
          Math.min(
            maximumOutputStoredBytes ?? Number.MAX_SAFE_INTEGER,
            plannedOutputStoredBytesUpperBound ?? Number.MAX_SAFE_INTEGER,
          ),
        );
      }
      if (existing === undefined) await transaction.stageBlock(outputBlockId, outputBytes);
      else await transaction.stageExistingBlocks([outputBlockId]);
      const description = inspectBlock(outputBytes);
      const nextColumnIndex = cursor.columnIndex + 1;
      const nextOutputIndex =
        nextColumnIndex === layout.columns.length ? cursor.outputIndex + 1 : cursor.outputIndex;
      const canonicalColumnIndex = nextColumnIndex === layout.columns.length ? 0 : nextColumnIndex;
      const nextRowStart =
        nextOutputIndex === layout.outputs.length
          ? layout.totalRows
          : (layout.outputs[nextOutputIndex]?.rowStart ?? output.rowStart);
      job = await this.store.updateCompactionJob(job.id, job.revision, {
        outputBlockIds: [...job.outputBlockIds, outputBlockId],
        outputCursor: {
          outputIndex: nextOutputIndex,
          columnIndex: canonicalColumnIndex,
          rowStart: nextRowStart,
        },
        processedRows: nextRowStart,
        outputStoredBytes: nextOutputStoredBytes,
        outputLogicalBytes: safeWholeNumberSum(
          [job.outputLogicalBytes ?? 0, description.encodedLength],
          "Compaction output logical bytes",
        ),
        peakWorkingBytes: Math.max(job.peakWorkingBytes ?? 0, built.peakWorkingBytes),
        updatedAt: this.#now().toISOString(),
        error: null,
      });
      processedBlocks += 1;
    }
    return job;
  }

  async #buildPhysicalCompactionOutput(
    plan: PhysicalCompactionLayout,
    column: PhysicalCompactionSourceColumn,
    output: RechunkCompactionOutputWindow,
    memoryBudgetBytes: number,
  ): Promise<{ physical: ValidatedPhysicalColumn; peakWorkingBytes: number }> {
    const loaded = await this.#loadPhysicalCompactionRanges(
      column,
      output,
      plan.outputCompression,
      memoryBudgetBytes,
    );
    const measurement = measurePhysicalColumnRanges(column.type, loaded.ranges);
    if (measurement.rowCount !== output.rowCount) {
      throw new Error("Compaction source ranges do not cover the planned output");
    }
    const physical = buildPhysicalColumnFromRanges(column.type, loaded.ranges);
    return { physical, peakWorkingBytes: loaded.peakWorkingBytes };
  }

  async #loadPhysicalCompactionRanges(
    column: PhysicalCompactionSourceColumn,
    output: RechunkCompactionOutputWindow,
    outputCompression: Compression,
    memoryBudgetBytes: number,
    snapshot?: LeasedSnapshot,
  ): Promise<{ ranges: PhysicalColumnRange[]; peakWorkingBytes: number }> {
    const memoryBound = physicalOutputMemoryBound(column, output, outputCompression);
    if (memoryBound > memoryBudgetBytes) {
      throw new CompactionMemoryBudgetError(memoryBudgetBytes, memoryBound);
    }
    const outputEnd = safeWholeNumberSum(
      [output.rowStart, output.rowCount],
      "Compaction output row range",
    );
    const ranges: PhysicalColumnRange[] = [];
    for (const sourceBlock of overlappingPhysicalSourceRanges(column, output)) {
      if (snapshot !== undefined) await this.#renewInternalLeaseIfNeeded(snapshot);
      const bytes = await this.store.getBlock(sourceBlock.blockId);
      if (bytes === undefined) {
        throw new Error(`A compaction source block is missing: ${sourceBlock.blockId}`);
      }
      const description = inspectBlock(bytes);
      if (
        bytes.byteLength !== sourceBlock.storedBytes ||
        description.encodedLength !== sourceBlock.encodedBytes ||
        description.checksum !== sourceBlock.checksum ||
        description.rowCount !== sourceBlock.sourceBlockRowCount ||
        description.type !== column.type
      ) {
        throw new Error(`A compaction source block differs from its plan: ${sourceBlock.blockId}`);
      }
      const decoded = await decodePhysicalBlock(bytes);
      const sourceEnd = safeWholeNumberSum(
        [sourceBlock.outputRowStart, sourceBlock.rowCount],
        "Compaction source row range",
      );
      const start =
        sourceBlock.sourceRowStart +
        Math.max(output.rowStart, sourceBlock.outputRowStart) -
        sourceBlock.outputRowStart;
      const end =
        sourceBlock.sourceRowStart + Math.min(outputEnd, sourceEnd) - sourceBlock.outputRowStart;
      const slice = slicePhysicalColumn(decoded.column, start, end);
      ranges.push({ column: slice, start: 0, end: slice.rowCount });
    }
    return { ranges, peakWorkingBytes: memoryBound };
  }

  async #loadCompactionSources(job: CompactionJobRecord): Promise<SegmentRecord[]> {
    const segments = await Promise.all(job.sourceSegmentIds.map((id) => this.store.getSegment(id)));
    const missingIndex = segments.findIndex((segment) => segment === undefined);
    if (missingIndex >= 0) {
      throw new Error(
        `Compaction source segment is missing: ${job.sourceSegmentIds[missingIndex] ?? ""}`,
      );
    }
    return segments as SegmentRecord[];
  }

  async #assertCompactionSnapshotOrder(
    job: CompactionJobRecord,
    snapshot: Snapshot,
  ): Promise<void> {
    const plan = job.rewritePlan ?? { kind: "copy-v1" as const };
    const sourceIds = new Set(job.sourceSegmentIds);
    const transactions = new Map(
      (await this.store.listTransactions()).map((record) => [record.id, record]),
    );
    const allVisibleSegments = (await this.store.listSegments()).filter((segment) =>
      Object.values(segment.columnBlockIds)
        .flat()
        .every((blockId) => snapshot.hasBlock(blockId)),
    );
    const visibleSegments = allVisibleSegments.filter((segment) => segment.tableId === job.tableId);
    const sourceBlockIds = new Set(job.sourceBlockIds);
    for (const segment of allVisibleSegments) {
      if (sourceIds.has(segment.id)) continue;
      if (
        Object.values(segment.columnBlockIds)
          .flat()
          .some((blockId) => sourceBlockIds.has(blockId))
      ) {
        throw new Error(`Concurrent segment shares a compaction source block: ${segment.id}`);
      }
    }
    if (job.outputPartitionOrdinal !== undefined) {
      if (plan.kind !== "rechunk-v1") {
        throw new Error("L2 compaction requires a rechunk rewrite plan");
      }
      await this.#assertLevelTwoSnapshotOrder(job, plan, snapshot, transactions);
      return;
    }
    let latestSource: Pick<
      MergeCompactionSourceSegment,
      "logicalOrder" | "committedVersion" | "segmentId"
    > | null = null;
    let outputLogicalOrder: number | null = null;
    if (plan.kind === "merge-v1") {
      const visibleById = new Map(visibleSegments.map((segment) => [segment.id, segment]));
      for (const planned of plan.sourceSegments) {
        const actual = visibleById.get(planned.segmentId);
        const owner = actual === undefined ? undefined : transactions.get(actual.transactionId);
        if (actual === undefined || !sameMergeSourceSegment(actual, owner, planned)) {
          throw new Error(`Compaction source segment differs from its plan: ${planned.segmentId}`);
        }
      }
      latestSource = plan.sourceSegments[plan.sourceSegments.length - 1] ?? null;
      outputLogicalOrder = plan.logicalOrder;
    } else {
      const sourceSegments = await this.#loadCompactionSources(job);
      for (const segment of sourceSegments) {
        const committedVersion = transactions.get(segment.transactionId)?.committedVersion;
        if (committedVersion === null || committedVersion === undefined) {
          throw new Error(`Compaction source segment has no committed owner: ${segment.id}`);
        }
        const tuple = {
          logicalOrder: segment.logicalOrder ?? committedVersion,
          committedVersion,
          segmentId: segment.id,
        };
        if (latestSource === null || compareMergeSourceOrder(latestSource, tuple) < 0) {
          latestSource = tuple;
        }
        outputLogicalOrder = Math.min(outputLogicalOrder ?? tuple.logicalOrder, tuple.logicalOrder);
      }
    }
    if (latestSource === null || outputLogicalOrder === null) {
      throw new Error("Compaction source order is unavailable");
    }
    for (const segment of visibleSegments) {
      if (sourceIds.has(segment.id)) continue;
      if ((segment.level ?? 0) !== 0) {
        throw new Error(`Concurrent segment has an unsupported compaction level: ${segment.id}`);
      }
      const committedVersion = transactions.get(segment.transactionId)?.committedVersion;
      if (committedVersion === null || committedVersion === undefined) {
        throw new Error(`Concurrent compaction segment has no committed owner: ${segment.id}`);
      }
      const logicalOrder = segment.logicalOrder ?? committedVersion;
      if (
        logicalOrder <= outputLogicalOrder ||
        compareMergeSourceOrder(latestSource, {
          logicalOrder,
          committedVersion,
          segmentId: segment.id,
        }) >= 0
      ) {
        throw new Error(`Concurrent segment would reorder compaction output: ${segment.id}`);
      }
    }
  }

  async #assertLevelTwoSnapshotOrder(
    job: CompactionJobRecord,
    plan: RechunkCompactionRewritePlan,
    snapshot: Snapshot,
    transactions: ReadonlyMap<string, { status: string; committedVersion: number | null }>,
  ): Promise<void> {
    const table = await this.store.getTable(job.tableId);
    if (table === undefined) throw new Error(`Compaction table is missing: ${job.tableId}`);
    const sourceManifest = await this.store.getManifest(job.sourceManifestVersion);
    if (sourceManifest === undefined || sourceManifest.prunedAt !== undefined) {
      throw new Error(
        `Compaction source manifest is unavailable: ${String(job.sourceManifestVersion)}`,
      );
    }
    const plannedSnapshot = new Snapshot(
      this.store,
      sourceManifest.version,
      sourceManifest.blockIds,
    );
    const plannedVisible = await this.#visibleSegmentRecords(table, plannedSnapshot);
    const plannedLayout = appendLevelTwoLayout(plannedVisible);
    if (
      plannedLayout === null ||
      plannedLayout.levelTwoSegments.length !== job.outputPartitionOrdinal
    ) {
      throw new Error("Compaction retained L2 prefix differs from its plan");
    }
    const plannedSources = plannedLayout.level0Segments.slice(0, job.sourceSegmentIds.length);
    if (
      plannedSources.length !== job.sourceSegmentIds.length ||
      plannedSources.some((segment, index) => segment.id !== job.sourceSegmentIds[index])
    ) {
      throw new Error("Compaction L2 sources are not the planned oldest L0 prefix");
    }
    for (const column of plan.columns) {
      const plannedBlockIds = column.sourceBlocks.map((block) => block.blockId);
      const descriptorBlockIds = plannedSources.flatMap(
        (segment) => segment.columnBlockIds[column.columnId] ?? [],
      );
      if (
        plannedBlockIds.length !== descriptorBlockIds.length ||
        plannedBlockIds.some((blockId, index) => blockId !== descriptorBlockIds[index])
      ) {
        throw new Error(`Compaction L2 source column differs from its plan: ${column.columnId}`);
      }
    }

    const currentVisible = await this.#visibleSegmentRecords(table, snapshot);
    const currentLayout = appendLevelTwoLayout(currentVisible);
    if (currentLayout?.levelTwoSegments.length !== job.outputPartitionOrdinal) {
      throw new Error("Concurrent segments violate the planned L2 layout");
    }
    const fixedPrefix = [...plannedLayout.retainedPrefix, ...plannedSources];
    if (currentVisible.length < fixedPrefix.length) {
      throw new Error("Compaction retained L2 prefix is no longer visible");
    }
    for (const [index, planned] of fixedPrefix.entries()) {
      const actual = currentVisible[index];
      if (
        actual?.id !== planned.id ||
        actual.transactionId !== planned.transactionId ||
        !sameCompactionSegment(actual, planned)
      ) {
        throw new Error(`Compaction retained segment differs from its plan: ${planned.id}`);
      }
    }
    const currentTail = currentVisible.slice(fixedPrefix.length);
    const earliestSource = sourceOrderTuple(plannedSources[0], transactions, "Compaction source");
    const latestSource = sourceOrderTuple(
      plannedSources[plannedSources.length - 1],
      transactions,
      "Compaction source",
    );
    const outputLogicalOrder = plan.logicalOrder;
    if (earliestSource.logicalOrder !== outputLogicalOrder) {
      throw new Error("Compaction L2 output order differs from its source prefix");
    }
    const latestRetained = plannedLayout.retainedPrefix.at(-1);
    if (
      latestRetained !== undefined &&
      compareMergeSourceOrder(
        sourceOrderTuple(latestRetained, transactions, "Retained compaction segment"),
        earliestSource,
      ) >= 0
    ) {
      throw new Error(`Retained compaction segment would reorder L2 output: ${latestRetained.id}`);
    }
    for (const segment of currentTail) {
      if (
        (segment.level ?? 0) !== 0 ||
        segment.partitionOrdinal !== undefined ||
        (segment.kind ?? "insert") !== "insert" ||
        segment.rowIdSpans !== undefined
      ) {
        throw new Error(`Concurrent segment has an unsupported compaction level: ${segment.id}`);
      }
      const tuple = sourceOrderTuple(segment, transactions, "Concurrent compaction segment");
      if (
        tuple.logicalOrder <= outputLogicalOrder ||
        compareMergeSourceOrder(latestSource, tuple) >= 0
      ) {
        throw new Error(`Concurrent segment would reorder compaction output: ${segment.id}`);
      }
    }
  }

  async #beginCompactionTransaction(job: CompactionJobRecord): Promise<{
    job: CompactionJobRecord;
    transaction: DatabaseTransaction | null;
  }> {
    const candidate = await this.#transactions.begin();
    const snapshot = await candidate.snapshot();
    try {
      await this.#assertCompactionSnapshotOrder(job, snapshot);
    } catch (error) {
      if (candidate.status === "active") await candidate.abort();
      const latest = await this.store.getCompactionJob(job.id);
      if (
        latest?.revision === job.revision &&
        latest.state !== "published" &&
        latest.state !== "cancelled" &&
        latest.state !== "aborted"
      ) {
        await this.#abortCompactionJob(latest, errorMessage(error));
      }
      throw error;
    }
    const missingSourceId = job.sourceBlockIds.find((id) => !snapshot.hasBlock(id));
    if (missingSourceId !== undefined) {
      if (candidate.status === "active") await candidate.abort();
      const latest = await this.store.getCompactionJob(job.id);
      if (latest !== undefined && latest.revision !== job.revision) {
        return { job: latest, transaction: null };
      }
      const reason = `Compaction source is no longer visible: ${missingSourceId}`;
      if (
        latest !== undefined &&
        latest.state !== "published" &&
        latest.state !== "aborted" &&
        latest.state !== "cancelled"
      ) {
        await this.#abortCompactionJob(latest, reason);
      }
      throw new Error(reason);
    }
    try {
      const linked = await this.store.updateCompactionJob(job.id, job.revision, {
        state: "running",
        transactionId: candidate.id,
        updatedAt: this.#now().toISOString(),
        error: null,
      });
      return { job: linked, transaction: candidate };
    } catch (error) {
      const latest = await this.store.getCompactionJob(job.id);
      if (latest?.transactionId === candidate.id) {
        return { job: latest, transaction: candidate };
      }
      if (candidate.status === "active") await candidate.abort();
      if (error instanceof CompactionJobConflictError && latest !== undefined) {
        return { job: latest, transaction: null };
      }
      throw error;
    }
  }

  async #markCompactionPublished(
    job: CompactionJobRecord,
    version: number,
  ): Promise<CompactionJobRecord> {
    if (job.state === "published") return job;
    try {
      return await this.store.updateCompactionJob(job.id, job.revision, {
        state: "published",
        publishedVersion: version,
        updatedAt: this.#now().toISOString(),
        error: null,
      });
    } catch (error) {
      const latest = await this.store.getCompactionJob(job.id);
      if (latest?.state === "published") return latest;
      if (latest?.transactionId === job.transactionId) {
        return this.store.updateCompactionJob(latest.id, latest.revision, {
          state: "published",
          publishedVersion: version,
          updatedAt: this.#now().toISOString(),
          error: null,
        });
      }
      throw error;
    }
  }

  async #abortCompactionJob(job: CompactionJobRecord, error: string): Promise<CompactionJobRecord> {
    return this.store.updateCompactionJob(job.id, job.revision, {
      state: "aborted",
      updatedAt: this.#now().toISOString(),
      error,
    });
  }

  async #publishedCompactionProgress(
    table: TableRecord,
    job: CompactionJobRecord,
  ): Promise<CompactionJobProgress> {
    const version = job.publishedVersion;
    if (version === null) {
      throw new Error(`Published compaction has no committed version: ${job.id}`);
    }
    return compactionProgress(table.name, job, this.#compactionResult(table, job, version));
  }

  #compactionResult(
    table: TableRecord,
    job: CompactionJobRecord,
    version: number,
  ): CompactTableResult {
    const rewritePlan = job.rewritePlan ?? { kind: "copy-v1" as const };
    const rowCount = rewritePlan.kind === "copy-v1" ? job.processedRows : rewritePlan.totalRows;
    const outputLogicalBytes = job.outputLogicalBytes ?? job.logicalBytes;
    const totalMs = Math.max(Date.parse(job.updatedAt) - Date.parse(job.createdAt), 0);
    return {
      jobId: job.id,
      tableName: table.name,
      compacted: true,
      sourceSegmentCount: job.sourceSegmentIds.length,
      sourceBlockCount: job.sourceBlockIds.length,
      outputSegmentId: job.outputSegmentId,
      outputBlockCount: job.outputBlockIds.length,
      rowCount,
      sourceStoredBytes: job.sourceStoredBytes,
      outputStoredBytes: job.outputStoredBytes,
      ...(job.level0SourceStoredBytes === undefined || job.anchorSourceStoredBytes === undefined
        ? {}
        : {
            level0SourceStoredBytes: job.level0SourceStoredBytes,
            anchorSourceStoredBytes: job.anchorSourceStoredBytes,
            compactionWriteAmplification: job.outputStoredBytes / job.level0SourceStoredBytes,
          }),
      ...(job.outputPartitionOrdinal === undefined
        ? {}
        : {
            outputPartitionOrdinal: job.outputPartitionOrdinal,
            maxWriteAmplification: job.maxWriteAmplification ?? 0,
            maximumOutputStoredBytes: job.maximumOutputStoredBytes ?? 0,
            plannedOutputStoredBytesUpperBound: job.plannedOutputStoredBytesUpperBound ?? 0,
          }),
      outputLogicalBytes,
      ...(rewritePlan.kind !== "copy-v1"
        ? {
            targetBlockBytes: rewritePlan.targetBlockBytes,
            outputCompression: rewritePlan.outputCompression,
            memoryBudgetBytes: job.memoryBudgetBytes ?? 0,
            minimumMemoryBytes: job.minimumMemoryBytes ?? 0,
            peakWorkingBytes: job.peakWorkingBytes ?? 0,
          }
        : {}),
      supersededBlockCount: job.sourceBlockIds.length,
      physicallyReclaimedBytes: 0,
      version,
      metrics: createWriteMetrics({
        logicalBytes: outputLogicalBytes,
        storedBytes: job.outputStoredBytes,
        encodeMs: 0,
        stageMs: totalMs,
        commitMs: 0,
        totalMs,
        retries: 0,
        rows: rowCount,
      }),
    };
  }

  async #firstLogicalOrder(sourceSegments: readonly SegmentRecord[]): Promise<number> {
    const transactions = new Map(
      (await this.store.listTransactions()).map((record) => [record.id, record]),
    );
    return Math.min(
      ...sourceSegments.map(
        (segment) =>
          segment.logicalOrder ?? transactions.get(segment.transactionId)?.committedVersion ?? 0,
      ),
    );
  }

  async #classifyKeys(
    table: TableRecord,
    version: number | null,
    kind: "insert" | "upsert",
    keys: Map<string, Exclude<BatchValue, null>> | undefined,
  ): Promise<{ inserted: number; updated: number }> {
    if (keys === undefined) return { inserted: 0, updated: 0 };
    const keyColumn = getUniqueKeyColumn(table);
    if (keyColumn === undefined) throw new Error("Unique key metadata is missing");
    const existing = await this.#existingKeyTokens(table, version, [...keys.keys()]);
    let inserted = 0;
    let updated = 0;
    for (const [token, value] of keys) {
      if (existing.has(token)) {
        if (kind === "insert") {
          throw new UniqueConstraintError(table.name, keyColumn.name, value);
        }
        updated += 1;
      } else {
        inserted += 1;
      }
    }
    return { inserted, updated };
  }

  async #existingKeyTokens(
    table: TableRecord,
    version: number | null,
    requestedTokens: readonly string[],
  ): Promise<Set<string>> {
    if (table.uniqueKeyLookupReady === true) {
      return new Set(await this.store.getExistingUniqueKeys(table.id, requestedTokens));
    }
    const keyColumn = getUniqueKeyColumn(table);
    if (keyColumn === undefined) throw new Error("Unique key metadata is missing");
    const requested = new Set(requestedTokens);
    return new Set(
      (await this.#materializeTable(table, version))
        .map((row) => keyToken(keyColumn.type, row[keyColumn.name] ?? null))
        .filter((token) => requested.has(token)),
    );
  }

  async #materializeTable(
    table: TableRecord,
    version?: number | null,
    projectedColumns: readonly TableColumnRecord[] = table.columns,
  ): Promise<DatabaseRow[]> {
    return this.#withLeasedSnapshot(version, (snapshot) =>
      this.#materializeTableAtSnapshot(table, snapshot, projectedColumns),
    );
  }

  async #materializeTableAtSnapshot(
    table: TableRecord,
    snapshot: LeasedSnapshot,
    projectedColumns: readonly TableColumnRecord[] = table.columns,
  ): Promise<DatabaseRow[]> {
    const segments = await this.#visibleSegmentRecords(table, snapshot);
    const keyColumn = getUniqueKeyColumn(table);
    const neededColumns = [
      ...projectedColumns,
      ...(keyColumn === undefined || projectedColumns.some((column) => column.id === keyColumn.id)
        ? []
        : [keyColumn]),
    ];
    const neededColumnIds = new Set(neededColumns.map((column) => column.id));
    const rows: Array<DatabaseRow | undefined> = [];
    const rowIndexByKey = new Map<string, number>();
    for (const segment of segments) {
      await this.#renewInternalLeaseIfNeeded(snapshot);
      const segmentBlockIds = Object.entries(segment.columnBlockIds)
        .filter(([columnId]) => neededColumnIds.has(columnId))
        .flatMap(([, ids]) => ids);
      const decodedColumns = await this.#loadDecodedBlocks(segmentBlockIds, snapshot);
      if (segment.kind === "delete") {
        if (keyColumn === undefined)
          throw new Error(`Delete segment has no unique key: ${segment.id}`);
        for (const value of this.#readSegmentColumn(keyColumn, segment, decodedColumns)) {
          const token = keyToken(keyColumn.type, value);
          const existingIndex = rowIndexByKey.get(token);
          if (existingIndex !== undefined) rows[existingIndex] = undefined;
          rowIndexByKey.delete(token);
        }
        continue;
      }
      if (segment.kind === "update") {
        if (keyColumn === undefined)
          throw new Error(`Update segment has no unique key: ${segment.id}`);
        const keyValues = this.#readSegmentColumn(keyColumn, segment, decodedColumns);
        const changedColumns = projectedColumns.filter(
          (column) =>
            column.id !== keyColumn.id && (segment.columnBlockIds[column.id]?.length ?? 0) > 0,
        );
        const changedValues = new Map(
          changedColumns.map(
            (column) =>
              [column.id, this.#readSegmentColumn(column, segment, decodedColumns)] as const,
          ),
        );
        keyValues.forEach((value, rowIndex) => {
          const token = keyToken(keyColumn.type, value);
          const existingIndex = rowIndexByKey.get(token);
          if (existingIndex === undefined) {
            throw new Error(`Update segment references a missing key: ${segment.id}`);
          }
          const existing = rows[existingIndex];
          if (existing === undefined) {
            throw new Error(`Update segment references a deleted row: ${segment.id}`);
          }
          for (const column of changedColumns) {
            existing[column.name] = changedValues.get(column.id)?.[rowIndex] ?? null;
          }
        });
        continue;
      }
      const segmentRows = this.#readSegment(neededColumns, segment, decodedColumns);
      for (const row of segmentRows) {
        if (segment.kind === "upsert") {
          if (keyColumn === undefined)
            throw new Error(`Upsert segment has no unique key: ${segment.id}`);
          const token = keyToken(keyColumn.type, row[keyColumn.name] ?? null);
          const existingIndex = rowIndexByKey.get(token);
          if (existingIndex === undefined) {
            rowIndexByKey.set(token, rows.length);
            rows.push(row);
          } else {
            rows[existingIndex] = row;
          }
        } else {
          if (keyColumn !== undefined) {
            const token = keyToken(keyColumn.type, row[keyColumn.name] ?? null);
            if (rowIndexByKey.has(token)) {
              throw new Error(`Stored table contains a duplicate unique key: ${table.name}`);
            }
            rowIndexByKey.set(token, rows.length);
          }
          rows.push(row);
        }
      }
    }
    const visibleRows = rows.filter((row): row is DatabaseRow => row !== undefined);
    if (neededColumns.length === projectedColumns.length) return visibleRows;
    return visibleRows.map((row) =>
      Object.fromEntries(projectedColumns.map((column) => [column.name, row[column.name] ?? null])),
    );
  }

  async #materializeColumnarTableAtSnapshot(
    table: TableRecord,
    snapshot: LeasedSnapshot,
    projectedColumns: readonly TableColumnRecord[],
  ): Promise<ColumnarTable> {
    const segments = await this.#visibleSegmentRecords(table, snapshot);
    const keyColumn = getUniqueKeyColumn(table);
    if (
      segments.every((segment) => {
        const kind = segment.kind ?? "insert";
        return kind === "insert" || kind === "base";
      })
    ) {
      const rowCount = segments.reduce((total, segment) => total + segment.rowCount, 0);
      const valuesByColumn = new Map(
        projectedColumns.map((column) => [column.id, new Array<BatchValue>(rowCount)] as const),
      );
      let outputRowStart = 0;
      for (const segment of segments) {
        await this.#renewInternalLeaseIfNeeded(snapshot);
        const segmentBlockIds = projectedColumns.flatMap(
          (column) => segment.columnBlockIds[column.id] ?? [],
        );
        const decodedColumns = await this.#loadDecodedBlocks(segmentBlockIds, snapshot);
        for (const column of projectedColumns) {
          const output = valuesByColumn.get(column.id);
          if (output === undefined) throw new Error(`Projected column is missing: ${column.name}`);
          const values = this.#readSegmentColumn(column, segment, decodedColumns);
          for (let row = 0; row < values.length; row += 1) {
            output[outputRowStart + row] = values[row] ?? null;
          }
        }
        outputRowStart += segment.rowCount;
      }
      const columns = new Map<string, ColumnarColumnInput>();
      for (const column of projectedColumns) {
        columns.set(column.name, {
          type: column.type,
          values: valuesByColumn.get(column.id) ?? [],
        });
      }
      const projectedKey =
        keyColumn !== undefined && projectedColumns.some((column) => column.id === keyColumn.id)
          ? keyColumn.name
          : undefined;
      return createColumnarTable(table.name, columns, projectedKey);
    }
    const neededColumns = [
      ...projectedColumns,
      ...(keyColumn === undefined || projectedColumns.some((column) => column.id === keyColumn.id)
        ? []
        : [keyColumn]),
    ];
    const valuesByColumn = new Map(
      neededColumns.map((column) => [column.id, [] as BatchValue[]] as const),
    );
    const alive: boolean[] = [];
    const rowIndexByKey = new Map<string, number>();

    for (const segment of segments) {
      await this.#renewInternalLeaseIfNeeded(snapshot);
      const segmentBlockIds = neededColumns.flatMap(
        (column) => segment.columnBlockIds[column.id] ?? [],
      );
      const decodedColumns = await this.#loadDecodedBlocks(segmentBlockIds, snapshot);
      if (segment.kind === "delete") {
        if (keyColumn === undefined)
          throw new Error(`Delete segment has no unique key: ${segment.id}`);
        for (const value of this.#readSegmentColumn(keyColumn, segment, decodedColumns)) {
          const token = keyToken(keyColumn.type, value);
          const existingIndex = rowIndexByKey.get(token);
          if (existingIndex !== undefined) alive[existingIndex] = false;
          rowIndexByKey.delete(token);
        }
        continue;
      }
      if (segment.kind === "update") {
        if (keyColumn === undefined)
          throw new Error(`Update segment has no unique key: ${segment.id}`);
        const keyValues = this.#readSegmentColumn(keyColumn, segment, decodedColumns);
        const changedColumns = projectedColumns.filter(
          (column) =>
            column.id !== keyColumn.id && (segment.columnBlockIds[column.id]?.length ?? 0) > 0,
        );
        const changedValues = new Map(
          changedColumns.map(
            (column) =>
              [column.id, this.#readSegmentColumn(column, segment, decodedColumns)] as const,
          ),
        );
        keyValues.forEach((value, rowIndex) => {
          const existingIndex = rowIndexByKey.get(keyToken(keyColumn.type, value));
          if (existingIndex === undefined || alive[existingIndex] !== true) {
            throw new Error(`Update segment references a missing key: ${segment.id}`);
          }
          for (const column of changedColumns) {
            const values = valuesByColumn.get(column.id);
            if (values === undefined)
              throw new Error(`Projected column is missing: ${column.name}`);
            values[existingIndex] = changedValues.get(column.id)?.[rowIndex] ?? null;
          }
        });
        continue;
      }

      const segmentValues = new Map(
        neededColumns.map(
          (column) =>
            [column.id, this.#readSegmentColumn(column, segment, decodedColumns)] as const,
        ),
      );
      for (let segmentRow = 0; segmentRow < segment.rowCount; segmentRow += 1) {
        let existingIndex: number | undefined;
        let token: string | undefined;
        if (keyColumn !== undefined) {
          const keyValue = segmentValues.get(keyColumn.id)?.[segmentRow] ?? null;
          token = keyToken(keyColumn.type, keyValue);
          existingIndex = rowIndexByKey.get(token);
        }
        if (segment.kind === "upsert" && existingIndex !== undefined) {
          for (const column of neededColumns) {
            const values = valuesByColumn.get(column.id);
            if (values === undefined)
              throw new Error(`Projected column is missing: ${column.name}`);
            values[existingIndex] = segmentValues.get(column.id)?.[segmentRow] ?? null;
          }
          alive[existingIndex] = true;
          continue;
        }
        if (existingIndex !== undefined) {
          throw new Error(`Stored table contains a duplicate unique key: ${table.name}`);
        }
        const outputIndex = alive.length;
        for (const column of neededColumns) {
          valuesByColumn.get(column.id)?.push(segmentValues.get(column.id)?.[segmentRow] ?? null);
        }
        alive.push(true);
        if (token !== undefined) rowIndexByKey.set(token, outputIndex);
      }
    }

    const hasDeletedRows = alive.some((visible) => !visible);
    const columns = new Map<string, ColumnarColumnInput>();
    for (const column of projectedColumns) {
      const sourceValues = valuesByColumn.get(column.id) ?? [];
      const values = hasDeletedRows
        ? sourceValues.filter((_value, rowIndex) => alive[rowIndex] === true)
        : sourceValues;
      columns.set(column.name, { type: column.type, values });
    }
    const projectedKey =
      keyColumn !== undefined && projectedColumns.some((column) => column.id === keyColumn.id)
        ? keyColumn.name
        : undefined;
    return createColumnarTable(table.name, columns, projectedKey);
  }

  #readSegment(
    columns: readonly TableColumnRecord[],
    segment: SegmentRecord,
    decodedColumns: ReadonlyMap<string, DecodedColumn>,
  ): DatabaseRow[] {
    const columnValues = new Map(
      columns.map(
        (column) => [column.id, this.#readSegmentColumn(column, segment, decodedColumns)] as const,
      ),
    );
    return Array.from({ length: segment.rowCount }, (_, rowIndex) =>
      Object.fromEntries(
        columns.map((column) => [column.name, columnValues.get(column.id)?.[rowIndex] ?? null]),
      ),
    );
  }

  #readSegmentColumn(
    column: TableColumnRecord,
    segment: SegmentRecord,
    decodedColumns: ReadonlyMap<string, DecodedColumn>,
  ): BatchValue[] {
    const blockIds = segment.columnBlockIds[column.id] ?? [];
    const values: BatchValue[] = [];
    for (const blockId of blockIds) {
      const decoded = decodedColumns.get(blockId);
      if (decoded === undefined) throw new Error(`Visible block is missing: ${blockId}`);
      if (decoded.type !== column.type) {
        throw new Error(`Column type mismatch: ${column.name}`);
      }
      for (const value of decoded.values) values.push(value);
    }
    if (values.length !== segment.rowCount) {
      throw new Error(`Column row count mismatch: ${column.name}`);
    }
    return values;
  }

  async #loadDecodedBlocks(
    blockIds: readonly string[],
    snapshot?: LeasedSnapshot,
  ): Promise<Map<string, DecodedColumn>> {
    if (blockIds.length === 0) return new Map();
    const decoded = new Map<string, DecodedColumn>();
    const decodeWindow = 16;
    for (let start = 0; start < blockIds.length; start += decodeWindow) {
      if (snapshot !== undefined) await this.#renewInternalLeaseIfNeeded(snapshot);
      const ids = blockIds.slice(start, start + decodeWindow);
      const blocks = await this.store.getBlocks(ids);
      const columns = await Promise.all(
        blocks.map(async (bytes, index) => {
          const blockId = ids[index] ?? "";
          if (bytes === undefined) throw new Error(`Visible block is missing: ${blockId}`);
          return [blockId, (await decodeBlock(bytes)).column] as const;
        }),
      );
      for (const [blockId, column] of columns) decoded.set(blockId, column);
    }
    return decoded;
  }

  async #visibleSegmentRecords(table: TableRecord, snapshot: Snapshot): Promise<SegmentRecord[]> {
    const transactions = new Map(
      (await this.store.listTransactions()).map((record) => [record.id, record]),
    );
    return (await this.store.listSegments(table.id))
      .filter((segment) =>
        Object.values(segment.columnBlockIds)
          .flat()
          .every((blockId) => snapshot.hasBlock(blockId)),
      )
      .sort((left, right) => {
        const leftVersion = transactions.get(left.transactionId)?.committedVersion ?? -1;
        const rightVersion = transactions.get(right.transactionId)?.committedVersion ?? -1;
        const leftOrder = left.logicalOrder ?? leftVersion;
        const rightOrder = right.logicalOrder ?? rightVersion;
        return (
          leftOrder - rightOrder || leftVersion - rightVersion || left.id.localeCompare(right.id)
        );
      });
  }

  async #withLeasedSnapshot<T>(
    version: number | null | undefined,
    action: (snapshot: LeasedSnapshot) => Promise<T>,
  ): Promise<T> {
    for (;;) {
      const lease = await this.#transactions.openLeasedSnapshot({
        id: `${this.#internalLeaseOwnerId}/${String(this.#internalLeaseSequence++)}`,
        ownerId: this.#internalLeaseOwnerId,
        ttlMs: INTERNAL_READ_LEASE_TTL_MS,
        ...(version === undefined ? {} : { version }),
      });
      try {
        return await action(lease);
      } catch (error) {
        if (version !== undefined || !(error instanceof SnapshotManifestMissingError)) throw error;
      } finally {
        await lease.release();
      }
    }
  }

  async #renewInternalLeaseIfNeeded(snapshot: LeasedSnapshot): Promise<void> {
    if (snapshot.expiresAt.getTime() - this.#now().getTime() > INTERNAL_READ_LEASE_TTL_MS / 2) {
      return;
    }
    await snapshot.renew(INTERNAL_READ_LEASE_TTL_MS);
  }

  async #findTable(name: string): Promise<TableRecord> {
    const table = await this.store.getTableByName(name);
    if (table === undefined) throw new Error(`Table not found: ${name}`);
    return table;
  }
}

export class BufferedTableWriter {
  readonly #mode: "insert" | "upsert";
  readonly #maxRows: number;
  readonly #maxBytes: number;
  readonly #maxAgeMs: number;
  readonly #onError: ((error: unknown) => void) | undefined;
  readonly #rows: Array<Readonly<Record<string, BatchValue>>> = [];
  #estimatedBytes = 0;
  #timer: ReturnType<typeof setTimeout> | undefined;
  #inFlight: Promise<BufferedFlushResult> | undefined;
  #closed = false;

  constructor(
    private readonly database: BrowserDatabase,
    private readonly tableName: string,
    options: BufferedWriterOptions = {},
  ) {
    this.#mode = options.mode ?? "insert";
    this.#maxRows = positiveWholeNumber(options.maxRows ?? 1_000, "Buffered row limit");
    this.#maxBytes = positiveWholeNumber(options.maxBytes ?? 1024 * 1024, "Buffered byte limit");
    this.#maxAgeMs = positiveWholeNumber(options.maxAgeMs ?? 1_000, "Buffered age limit");
    this.#onError = options.onError;
  }

  get pendingRowCount(): number {
    return this.#rows.length;
  }

  get estimatedBytes(): number {
    return this.#estimatedBytes;
  }

  async add(row: Readonly<Record<string, BatchValue>>): Promise<BufferedFlushResult | undefined> {
    this.#assertOpen();
    const copy = cloneRow(row);
    this.#rows.push(copy);
    this.#estimatedBytes += estimateRowBytes(copy);
    this.#scheduleAgeFlush();
    if (this.#rows.length >= this.#maxRows || this.#estimatedBytes >= this.#maxBytes) {
      return this.flush();
    }
    return undefined;
  }

  async flush(): Promise<BufferedFlushResult | undefined> {
    if (this.#inFlight !== undefined) return this.#inFlight;
    if (this.#rows.length === 0) return undefined;
    this.#clearTimer();
    const rows = this.#rows.splice(0);
    this.#estimatedBytes = 0;
    const batch = rowsToBatch(rows);
    const operation =
      this.#mode === "upsert"
        ? this.database.upsertBatch(this.tableName, batch)
        : this.database.insertBatch(this.tableName, batch);
    this.#inFlight = operation;
    try {
      return await operation;
    } catch (error) {
      this.#rows.unshift(...rows);
      this.#estimatedBytes = this.#rows.reduce((total, row) => total + estimateRowBytes(row), 0);
      throw error;
    } finally {
      this.#inFlight = undefined;
    }
  }

  requestFlush(): void {
    if (this.#closed) return;
    void this.#flushPending().catch((error: unknown) => this.#onError?.(error));
  }

  async close(): Promise<BufferedFlushResult | undefined> {
    if (this.#closed) return undefined;
    let result: BufferedFlushResult | undefined;
    while (this.#inFlight !== undefined || this.#rows.length > 0) {
      result = await this.flush();
      if (this.#inFlight !== undefined) await this.#inFlight;
    }
    this.#closed = true;
    this.#clearTimer();
    return result;
  }

  discard(): number {
    this.#assertOpen();
    const discarded = this.#rows.length;
    this.#rows.length = 0;
    this.#estimatedBytes = 0;
    this.#clearTimer();
    return discarded;
  }

  async #flushPending(): Promise<void> {
    while (this.#inFlight !== undefined || this.#rows.length > 0) {
      if (this.#inFlight !== undefined) await this.#inFlight;
      else await this.flush();
    }
  }

  #scheduleAgeFlush(): void {
    if (this.#timer !== undefined || this.#rows.length === 0 || this.#closed) return;
    this.#timer = setTimeout(() => {
      this.#timer = undefined;
      void this.flush().catch((error: unknown) => this.#onError?.(error));
    }, this.#maxAgeMs);
  }

  #clearTimer(): void {
    if (this.#timer === undefined) return;
    clearTimeout(this.#timer);
    this.#timer = undefined;
  }

  #assertOpen(): void {
    if (this.#closed) throw new Error("Buffered writer is closed");
  }
}

export function attachLifecycleFlush(
  requester: LifecycleFlushRequester,
  options: LifecycleFlushOptions = {},
): () => void {
  const documentTarget =
    options.document ?? (typeof document === "undefined" ? undefined : document);
  const pageTarget = options.page ?? (typeof window === "undefined" ? undefined : window);
  const onVisibilityChange = (): void => {
    if (documentTarget?.visibilityState === "hidden") requester.requestFlush();
  };
  const onPageHide = (): void => requester.requestFlush();
  documentTarget?.addEventListener("visibilitychange", onVisibilityChange);
  pageTarget?.addEventListener("pagehide", onPageHide);
  return () => {
    documentTarget?.removeEventListener("visibilitychange", onVisibilityChange);
    pageTarget?.removeEventListener("pagehide", onPageHide);
  };
}

function validateName(name: string, kind: string): string {
  const trimmed = name.trim();
  if (trimmed.length === 0) throw new TypeError(`${kind} name cannot be empty`);
  return trimmed;
}

function validateBatch(table: TableRecord, input: InsertBatchInput): number {
  const expected = new Set(table.columns.map((column) => column.name));
  for (const name of Object.keys(input.columns)) {
    if (!expected.has(name)) throw new TypeError(`Unknown column: ${name}`);
  }
  for (const column of table.columns) {
    if (!(column.name in input.columns)) throw new TypeError(`Missing column: ${column.name}`);
  }
  const first = table.columns[0];
  if (first === undefined) throw new Error("Table has no columns");
  const rowCount = input.columns[first.name]?.length ?? 0;
  if (rowCount === 0) throw new TypeError("A batch needs at least one row");
  table.columns.forEach((column) => {
    const values = input.columns[column.name] ?? [];
    if (values.length !== rowCount) throw new TypeError("All columns must have the same row count");
    values.forEach((value, index) => validateValue(column, value, index));
  });
  return rowCount;
}

function validateUpdateBatch(
  table: TableRecord,
  keyColumn: TableColumnRecord,
  input: UpdateBatchInput,
): Map<string, Exclude<BatchValue, null>> {
  if (input.keys.length === 0) throw new TypeError("An update batch needs at least one key");
  const changeNames = Object.keys(input.changes);
  if (changeNames.length === 0) throw new TypeError("An update batch needs at least one change");
  const keys = new Map<string, Exclude<BatchValue, null>>();
  input.keys.forEach((value, index) => {
    validateValue(keyColumn, value, index);
    if (value === null) throw new TypeError(`Unique key cannot be null: ${keyColumn.name}`);
    const token = keyToken(keyColumn.type, value);
    if (keys.has(token)) throw new TypeError(`Duplicate update key: ${formatValue(value)}`);
    keys.set(token, value);
  });
  for (const name of changeNames) {
    const column = table.columns.find((candidate) => candidate.name === name);
    if (column === undefined) throw new TypeError(`Unknown column: ${name}`);
    if (column.id === keyColumn.id) throw new TypeError(`Unique key cannot be updated: ${name}`);
    const values = input.changes[name] ?? [];
    if (values.length !== input.keys.length) {
      throw new TypeError("Every changed column must have the same row count as the keys");
    }
    values.forEach((value, index) => validateValue(column, value, index));
  }
  return keys;
}

function findColumn(table: TableRecord, name: string): TableColumnRecord {
  const column = table.columns.find((candidate) => candidate.name === name);
  if (column === undefined) throw new Error(`Column not found: ${name}`);
  return column;
}

function resolveReadColumns(
  table: TableRecord,
  names: readonly string[] | undefined,
): TableColumnRecord[] {
  if (names === undefined) return table.columns;
  if (names.length === 0) throw new TypeError("A projected read needs at least one column");
  const seen = new Set<string>();
  return names.map((name) => {
    if (seen.has(name)) throw new TypeError(`Duplicate projected column: ${name}`);
    seen.add(name);
    const column = table.columns.find((candidate) => candidate.name === name);
    if (column === undefined) throw new TypeError(`Unknown column: ${name}`);
    return column;
  });
}

function validateValue(column: TableColumnRecord, value: BatchValue, index: number): void {
  if (value === null) {
    if (!column.nullable) throw new TypeError(`${column.name}[${String(index)}] cannot be null`);
    return;
  }
  const valid =
    (column.type === "boolean" && typeof value === "boolean") ||
    (column.type === "number" && typeof value === "number" && Number.isFinite(value)) ||
    (column.type === "string" && typeof value === "string") ||
    (column.type === "datetime" && value instanceof Date && Number.isFinite(value.getTime()));
  if (!valid) {
    throw new TypeError(`${column.name}[${String(index)}] must be ${column.type}`);
  }
}

function getUniqueKeyColumn(table: TableRecord): TableColumnRecord | undefined {
  if (table.uniqueKeyColumnId === undefined) return undefined;
  return table.columns.find((column) => column.id === table.uniqueKeyColumnId);
}

function batchKeys(
  table: TableRecord,
  input: InsertBatchInput,
): Map<string, Exclude<BatchValue, null>> | undefined {
  const keyColumn = getUniqueKeyColumn(table);
  if (keyColumn === undefined) return undefined;
  const keys = new Map<string, Exclude<BatchValue, null>>();
  for (const value of input.columns[keyColumn.name] ?? []) {
    if (value === null) throw new TypeError(`Unique key cannot be null: ${keyColumn.name}`);
    const token = keyToken(keyColumn.type, value);
    if (keys.has(token)) {
      throw new UniqueConstraintError(table.name, keyColumn.name, value);
    }
    keys.set(token, value);
  }
  return keys;
}

function keyToken(type: SimpleDataType, value: BatchValue): string {
  if (value === null) throw new TypeError("Unique key cannot be null");
  switch (type) {
    case "boolean":
      if (typeof value !== "boolean") throw new TypeError("Invalid boolean unique key");
      return value ? "boolean:true" : "boolean:false";
    case "number":
      if (typeof value !== "number" || !Number.isFinite(value)) {
        throw new TypeError("Invalid number unique key");
      }
      return `number:${String(value)}`;
    case "string":
      if (typeof value !== "string") throw new TypeError("Invalid string unique key");
      return `string:${value}`;
    case "datetime":
      if (!(value instanceof Date) || !Number.isFinite(value.getTime())) {
        throw new TypeError("Invalid datetime unique key");
      }
      return `datetime:${String(value.getTime())}`;
  }
}

function formatValue(value: Exclude<BatchValue, null>): string {
  return value instanceof Date ? value.toISOString() : String(value);
}

function positiveWholeNumber(value: number, name: string): number {
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw new RangeError(`${name} must be a positive whole number`);
  }
  return value;
}

function positiveFiniteNumber(value: number, name: string): number {
  if (!Number.isFinite(value) || value <= 0) {
    throw new RangeError(`${name} must be a positive finite number`);
  }
  return value;
}

function validateCompression(value: unknown, name: string): Compression {
  if (value !== "raw" && value !== "rle" && value !== "gzip") {
    throw new TypeError(`${name} must be raw, rle, or gzip`);
  }
  return value;
}

function isPhysicalColumnLimitError(error: unknown): boolean {
  return (
    error instanceof RangeError && error.message === "Physical column exceeds maximum byte length"
  );
}

function safeWholeNumberSum(values: readonly number[], name: string): number {
  let total = 0;
  for (const value of values) {
    if (!Number.isSafeInteger(value) || value < 0 || total > Number.MAX_SAFE_INTEGER - value) {
      throw new RangeError(`${name} exceeds the safe integer range`);
    }
    total += value;
  }
  return total;
}

function safeWholeNumberProduct(left: number, right: number, name: string): number {
  if (
    !Number.isSafeInteger(left) ||
    left < 0 ||
    !Number.isSafeInteger(right) ||
    right < 0 ||
    (left !== 0 && right > Math.floor(Number.MAX_SAFE_INTEGER / left))
  ) {
    throw new RangeError(`${name} exceeds the safe integer range`);
  }
  return left * right;
}

function validateMergeSegmentShape(
  table: TableRecord,
  segmentId: string,
  kind: SegmentKind,
  columns: readonly MergeCompactionSourceColumn[],
  keyColumnId: string,
): void {
  const columnIds = columns.map((column) => column.columnId);
  const tableColumnIds = table.columns.map((column) => column.id);
  if (kind === "insert" || kind === "upsert" || kind === "base") {
    if (
      columnIds.length !== tableColumnIds.length ||
      columnIds.some((columnId, index) => columnId !== tableColumnIds[index])
    ) {
      throw new Error(`Full-row compaction source is missing columns: ${segmentId}`);
    }
    return;
  }
  if (kind === "delete") {
    if (columnIds.length !== 1 || columnIds[0] !== keyColumnId) {
      throw new Error(`Delete compaction source has invalid columns: ${segmentId}`);
    }
    return;
  }
  if (
    columnIds.length < 2 ||
    !columnIds.includes(keyColumnId) ||
    columnIds.every((columnId) => columnId === keyColumnId)
  ) {
    throw new Error(`Update compaction source has invalid columns: ${segmentId}`);
  }
}

function mergeSourceRowIdSpans(segment: SegmentRecord, kind: SegmentKind): RowIdSpan[] {
  if (kind === "update" || kind === "delete") {
    if (
      segment.rowIdStart !== 0n ||
      segment.rowIdEndExclusive !== 0n ||
      (segment.rowIdSpans?.length ?? 0) !== 0
    ) {
      throw new Error(`Mutation marker unexpectedly owns row IDs: ${segment.id}`);
    }
    return [];
  }
  const spans = segment.rowIdSpans?.map((span) => ({ ...span })) ?? [
    {
      rowStart: 0,
      rowCount: segment.rowCount,
      rowIdStart: segment.rowIdStart,
    },
  ];
  const envelope = rowIdSpanEnvelope(spans);
  let rowStart = 0;
  for (const [index, span] of spans.entries()) {
    if (span.rowStart !== rowStart || span.rowCount <= 0) {
      throw new Error(`Segment row ID spans are not contiguous: ${segment.id}`);
    }
    const previous = spans[index - 1];
    if (
      previous !== undefined &&
      previous.rowIdStart + BigInt(previous.rowCount) === span.rowIdStart
    ) {
      throw new Error(`Segment row ID spans are not coalesced: ${segment.id}`);
    }
    rowStart = safeWholeNumberSum([rowStart, span.rowCount], "Segment row ID span rows");
  }
  if (
    rowStart !== segment.rowCount ||
    envelope.start !== segment.rowIdStart ||
    envelope.endExclusive !== segment.rowIdEndExclusive
  ) {
    throw new Error(`Segment row ID spans differ from their envelope: ${segment.id}`);
  }
  const intervals = spans
    .map((span) => ({
      start: span.rowIdStart,
      end: span.rowIdStart + BigInt(span.rowCount),
    }))
    .sort((left, right) => (left.start < right.start ? -1 : left.start > right.start ? 1 : 0));
  for (let index = 1; index < intervals.length; index += 1) {
    const previous = intervals[index - 1];
    const current = intervals[index];
    if (previous !== undefined && current !== undefined && current.start < previous.end) {
      throw new Error(`Segment row IDs overlap: ${segment.id}`);
    }
  }
  return spans;
}

function assertCanonicalMergeSourceOrder(segments: readonly MergeCompactionSourceSegment[]): void {
  for (let index = 1; index < segments.length; index += 1) {
    const previous = segments[index - 1];
    const current = segments[index];
    if (
      previous !== undefined &&
      current !== undefined &&
      compareMergeSourceOrder(previous, current) >= 0
    ) {
      throw new Error("Mutation compaction sources are not in canonical logical order");
    }
  }
}

function compareMergeSourceOrder(
  left: Pick<MergeCompactionSourceSegment, "logicalOrder" | "committedVersion" | "segmentId">,
  right: Pick<MergeCompactionSourceSegment, "logicalOrder" | "committedVersion" | "segmentId">,
): number {
  return (
    left.logicalOrder - right.logicalOrder ||
    left.committedVersion - right.committedVersion ||
    left.segmentId.localeCompare(right.segmentId)
  );
}

function sourceOrderTuple(
  segment: SegmentRecord | undefined,
  transactions: ReadonlyMap<string, { status: string; committedVersion: number | null }>,
  label: string,
): Pick<MergeCompactionSourceSegment, "logicalOrder" | "committedVersion" | "segmentId"> {
  if (segment === undefined) throw new Error(`${label} is unavailable`);
  const owner = transactions.get(segment.transactionId);
  if (owner?.status !== "committed" || owner.committedVersion === null) {
    throw new Error(`${label} has no committed owner: ${segment.id}`);
  }
  return {
    logicalOrder: segment.logicalOrder ?? owner.committedVersion,
    committedVersion: owner.committedVersion,
    segmentId: segment.id,
  };
}

function mergePlannerMemoryBound(
  table: TableRecord,
  segments: readonly MergeCompactionSourceSegment[],
  keyColumnId: string,
): number {
  const candidateRows = safeWholeNumberSum(
    segments
      .filter(
        (segment) =>
          segment.kind === "insert" || segment.kind === "upsert" || segment.kind === "base",
      )
      .map((segment) => segment.rowCount),
    "Mutation compaction candidate rows",
  );
  const keyEncodedBytes = safeWholeNumberSum(
    segments.flatMap((segment) =>
      (segment.columns.find((column) => column.columnId === keyColumnId)?.sourceBlocks ?? []).map(
        (block) => block.encodedBytes,
      ),
    ),
    "Mutation compaction key bytes",
  );
  const rowMetadataBytes = safeWholeNumberProduct(
    candidateRows,
    safeWholeNumberSum(
      [256, safeWholeNumberProduct(table.columns.length, 256, "Mutation compaction row cells")],
      "Mutation compaction row metadata",
    ),
    "Mutation compaction row metadata",
  );
  return safeWholeNumberSum(
    [rowMetadataBytes, safeWholeNumberProduct(keyEncodedBytes, 4, "Mutation compaction keys")],
    "Mutation compaction planner memory",
  );
}

function mergeSourceAt(
  segment: MergeCompactionSourceSegment,
  columnId: string,
  rowIndex: number,
): MergeResolvedSource {
  const column = segment.columns.find((candidate) => candidate.columnId === columnId);
  const block = column === undefined ? undefined : rowRangeAt(column.sourceBlocks, rowIndex);
  if (block === undefined) {
    throw new Error(`Mutation source row is missing: ${segment.segmentId}:${columnId}`);
  }
  return { blockId: block.blockId, sourceRowIndex: rowIndex - block.rowStart };
}

function rowIdAt(spans: readonly RowIdSpan[], rowIndex: number): bigint {
  const span = rowRangeAt(spans, rowIndex);
  if (span === undefined) throw new Error(`Mutation source row ID is missing: ${String(rowIndex)}`);
  return span.rowIdStart + BigInt(rowIndex - span.rowStart);
}

function rowRangeAt<T extends { readonly rowStart: number; readonly rowCount: number }>(
  ranges: readonly T[],
  rowIndex: number,
): T | undefined {
  let low = 0;
  let high = ranges.length - 1;
  while (low <= high) {
    const middle = low + Math.floor((high - low) / 2);
    const range = ranges[middle];
    if (range === undefined) return undefined;
    if (rowIndex < range.rowStart) {
      high = middle - 1;
    } else if (rowIndex >= range.rowStart + range.rowCount) {
      low = middle + 1;
    } else {
      return range;
    }
  }
  return undefined;
}

function coalesceRowIdSpans(rowIds: readonly bigint[]): RowIdSpan[] {
  const spans: RowIdSpan[] = [];
  for (const [rowStart, rowId] of rowIds.entries()) {
    const previous = spans[spans.length - 1];
    if (previous !== undefined && previous.rowIdStart + BigInt(previous.rowCount) === rowId) {
      spans[spans.length - 1] = { ...previous, rowCount: previous.rowCount + 1 };
    } else {
      spans.push({ rowStart, rowCount: 1, rowIdStart: rowId });
    }
  }
  return spans;
}

function rowIdSpanEnvelope(spans: readonly RowIdSpan[]): {
  start: bigint;
  endExclusive: bigint;
} {
  if (spans.length === 0) return { start: 0n, endExclusive: 0n };
  let start = spans[0]?.rowIdStart ?? 0n;
  let endExclusive = start + BigInt(spans[0]?.rowCount ?? 0);
  for (const span of spans.slice(1)) {
    if (span.rowIdStart < start) start = span.rowIdStart;
    const spanEnd = span.rowIdStart + BigInt(span.rowCount);
    if (spanEnd > endExclusive) endExclusive = spanEnd;
  }
  return { start, endExclusive };
}

function coalesceMergeOutputRanges(
  sources: readonly MergeResolvedSource[],
): MergeCompactionOutputSourceRange[] {
  const ranges: MergeCompactionOutputSourceRange[] = [];
  for (const [outputRowStart, source] of sources.entries()) {
    const previous = ranges[ranges.length - 1];
    if (
      previous?.sourceBlockId === source.blockId &&
      previous.sourceRowStart + previous.rowCount === source.sourceRowIndex
    ) {
      ranges[ranges.length - 1] = { ...previous, rowCount: previous.rowCount + 1 };
    } else {
      ranges.push({
        outputRowStart,
        sourceBlockId: source.blockId,
        sourceRowStart: source.sourceRowIndex,
        rowCount: 1,
      });
    }
  }
  return ranges;
}

function validatePhysicalTablePlan(table: TableRecord, plan: PhysicalCompactionRewritePlan): void {
  if (
    table.columns.length !== plan.columns.length ||
    table.columns.some((column, index) => {
      const planned = plan.columns[index];
      return planned?.columnId !== column.id || planned.type !== column.type;
    })
  ) {
    throw new Error(`Compaction table schema changed after planning: ${table.name}`);
  }
  if (plan.kind === "merge-v1" && table.uniqueKeyColumnId !== plan.keyColumnId) {
    throw new Error(`Compaction table key changed after planning: ${table.name}`);
  }
}

function physicalRewriteSourceBlocks(
  plan: PhysicalCompactionRewritePlan,
): Array<RechunkCompactionSourceBlock | MergeCompactionSourceBlock> {
  return plan.kind === "rechunk-v1"
    ? plan.columns.flatMap((column) => [...column.sourceBlocks])
    : plan.sourceSegments.flatMap((segment) =>
        segment.columns.flatMap((column) => [...column.sourceBlocks]),
      );
}

function physicalRewriteSourceStoredBytes(plan: PhysicalCompactionRewritePlan): number {
  return safeWholeNumberSum(
    physicalRewriteSourceBlocks(plan).map((block) => block.storedBytes),
    "Compaction source stored bytes",
  );
}

function physicalRewriteSourceEncodedBytes(plan: PhysicalCompactionRewritePlan): number {
  return safeWholeNumberSum(
    physicalRewriteSourceBlocks(plan).map((block) => block.encodedBytes),
    "Compaction source encoded bytes",
  );
}

function rechunkPhysicalColumns(
  columns: readonly RechunkCompactionSourceColumn[],
): PhysicalCompactionSourceColumn[] {
  return columns.map((column) => ({
    columnId: column.columnId,
    type: column.type,
    sourceRanges: column.sourceBlocks.map((block) => ({
      blockId: block.blockId,
      outputRowStart: block.rowStart,
      sourceRowStart: 0,
      rowCount: block.rowCount,
      sourceBlockRowCount: block.rowCount,
      storedBytes: block.storedBytes,
      encodedBytes: block.encodedBytes,
      checksum: block.checksum,
    })),
  }));
}

function mergePhysicalColumns(
  columns: readonly MergeCompactionOutputColumn[],
  sourceSegments: readonly MergeCompactionSourceSegment[],
): PhysicalCompactionSourceColumn[] {
  const blocks = new Map(
    sourceSegments.flatMap((segment) =>
      segment.columns.flatMap((column) =>
        column.sourceBlocks.map((block) => [block.blockId, block] as const),
      ),
    ),
  );
  return columns.map((column) => ({
    columnId: column.columnId,
    type: column.type,
    sourceRanges: column.sourceRanges.map((range) => {
      const block = blocks.get(range.sourceBlockId);
      if (block === undefined) {
        throw new Error(
          `Mutation compaction source fingerprint is missing: ${range.sourceBlockId}`,
        );
      }
      return {
        blockId: block.blockId,
        outputRowStart: range.outputRowStart,
        sourceRowStart: range.sourceRowStart,
        rowCount: range.rowCount,
        sourceBlockRowCount: block.rowCount,
        storedBytes: block.storedBytes,
        encodedBytes: block.encodedBytes,
        checksum: block.checksum,
      };
    }),
  }));
}

function physicalRewriteLayout(plan: PhysicalCompactionRewritePlan): PhysicalCompactionLayout {
  return {
    targetBlockBytes: plan.targetBlockBytes,
    outputCompression: plan.outputCompression,
    totalRows: plan.totalRows,
    columns:
      plan.kind === "rechunk-v1"
        ? rechunkPhysicalColumns(plan.columns)
        : mergePhysicalColumns(plan.columns, plan.sourceSegments),
    outputs: plan.outputs,
  };
}

function overlappingPhysicalSourceRanges(
  column: PhysicalCompactionSourceColumn,
  output: RechunkCompactionOutputWindow,
): readonly PhysicalCompactionSourceRange[] {
  const outputEnd = safeWholeNumberSum(
    [output.rowStart, output.rowCount],
    "Compaction output row range",
  );
  return column.sourceRanges.filter((block) => {
    const blockEnd = safeWholeNumberSum(
      [block.outputRowStart, block.rowCount],
      "Compaction source row range",
    );
    return block.outputRowStart < outputEnd && blockEnd > output.rowStart;
  });
}

function physicalOutputMemoryBound(
  column: PhysicalCompactionSourceColumn,
  output: RechunkCompactionOutputWindow,
  compression: Compression,
): number {
  const sourceBlocks = overlappingPhysicalSourceRanges(column, output);
  let retainedDecodedBytes = 0;
  let decodePeakBytes = 0;
  for (const block of sourceBlocks) {
    const decodeBytes = safeWholeNumberSum(
      [
        retainedDecodedBytes,
        safeWholeNumberProduct(block.storedBytes, 2, "Compaction decode memory"),
        safeWholeNumberProduct(block.encodedBytes, 2, "Compaction decode memory"),
      ],
      "Compaction decode memory",
    );
    decodePeakBytes = Math.max(decodePeakBytes, decodeBytes);
    retainedDecodedBytes = safeWholeNumberSum(
      [retainedDecodedBytes, block.encodedBytes],
      "Compaction decoded source memory",
    );
  }
  const outputEncodedBytes = retainedDecodedBytes;
  const buildPeakBytes = safeWholeNumberSum(
    [retainedDecodedBytes, outputEncodedBytes],
    "Compaction physical build memory",
  );
  const compressionBound = getCompressionMemoryBound(compression, outputEncodedBytes);
  const maximumBlockBytes = safeWholeNumberSum(
    [MAX_BLOCK_ENVELOPE_BYTES, compressionBound.maximumOutputBytes],
    "Compaction encoded block memory",
  );
  const compressionPeakBytes = safeWholeNumberSum(
    [outputEncodedBytes, compressionBound.scratchBytes, compressionBound.maximumOutputBytes],
    "Compaction compression memory",
  );
  const envelopePeakBytes = safeWholeNumberSum(
    [outputEncodedBytes, compressionBound.maximumOutputBytes, maximumBlockBytes],
    "Compaction block envelope memory",
  );
  const reconciliationPeakBytes = safeWholeNumberSum(
    [
      safeWholeNumberProduct(outputEncodedBytes, 3, "Compaction reconciliation memory"),
      safeWholeNumberProduct(maximumBlockBytes, 2, "Compaction reconciliation memory"),
    ],
    "Compaction reconciliation memory",
  );
  return Math.max(
    1,
    decodePeakBytes,
    buildPeakBytes,
    compressionPeakBytes,
    envelopePeakBytes,
    reconciliationPeakBytes,
  );
}

function compactionMinimumMemoryBytes(plan: PhysicalCompactionRewritePlan): number {
  let minimumBytes = plan.outputs.length === 0 ? 0 : 1;
  const layout = physicalRewriteLayout(plan);
  for (const output of plan.outputs) {
    for (const column of layout.columns) {
      minimumBytes = Math.max(
        minimumBytes,
        physicalOutputMemoryBound(column, output, plan.outputCompression),
      );
    }
  }
  return minimumBytes;
}

function physicalOutputBlockId(jobId: string, outputIndex: number, columnIndex: number): string {
  return [
    jobId,
    "rewrite",
    "window",
    String(outputIndex).padStart(8, "0"),
    "column",
    String(columnIndex).padStart(8, "0"),
  ].join("/");
}

function physicalOutputBlockIds(jobId: string, plan: PhysicalCompactionRewritePlan): string[] {
  return plan.outputs.flatMap((_output, outputIndex) =>
    plan.columns.map((_column, columnIndex) =>
      physicalOutputBlockId(jobId, outputIndex, columnIndex),
    ),
  );
}

function physicalOutputColumns(
  jobId: string,
  plan: PhysicalCompactionRewritePlan,
): Record<string, string[]> {
  return Object.fromEntries(
    plan.columns.map((column, columnIndex) => [
      column.columnId,
      plan.outputs.map((_output, outputIndex) =>
        physicalOutputBlockId(jobId, outputIndex, columnIndex),
      ),
    ]),
  );
}

interface CompactionBlockEntry {
  columnId: string;
  sourceBlockId: string;
  outputBlockId: string;
}

function isActiveCompactionState(state: CompactionJobState): boolean {
  return state === "planned" || state === "running" || state === "ready";
}

function uniqueSegmentBlockIds(segments: readonly SegmentRecord[]): string[] {
  return [
    ...new Set(segments.flatMap((segment) => Object.values(segment.columnBlockIds).flat())),
  ].sort();
}

function compactionBlockEntries(
  table: TableRecord,
  segment: SegmentRecord,
  segmentIndex: number,
  jobId: string,
): CompactionBlockEntry[] {
  return table.columns.flatMap((column, columnIndex) =>
    (segment.columnBlockIds[column.id] ?? []).map((sourceBlockId, part) => ({
      columnId: column.id,
      sourceBlockId,
      outputBlockId: [
        jobId,
        "output",
        "segment",
        String(segmentIndex).padStart(6, "0"),
        "column",
        String(columnIndex).padStart(6, "0"),
        "part",
        String(part).padStart(6, "0"),
      ].join("/"),
    })),
  );
}

function compactionOutputColumns(
  table: TableRecord,
  sourceSegments: readonly SegmentRecord[],
  jobId: string,
): Record<string, string[]> {
  return Object.fromEntries(
    table.columns.map((column) => [
      column.id,
      sourceSegments.flatMap((segment, segmentIndex) =>
        compactionBlockEntries(table, segment, segmentIndex, jobId)
          .filter((entry) => entry.columnId === column.id)
          .map((entry) => entry.outputBlockId),
      ),
    ]),
  );
}

function compactionProgress(
  tableName: string,
  job: CompactionJobRecord,
  result: CompactTableResult | null,
): CompactionJobProgress {
  const rewritePlan = job.rewritePlan ?? { kind: "copy-v1" as const };
  return {
    jobId: job.id,
    tableName,
    state: job.state,
    processedRows: job.processedRows,
    sourceSegmentCount: job.sourceSegmentIds.length,
    sourceBlockCount: job.sourceBlockIds.length,
    outputBlockCount: job.outputBlockIds.length,
    ...(job.level0SourceStoredBytes === undefined || job.anchorSourceStoredBytes === undefined
      ? {}
      : {
          level0SourceStoredBytes: job.level0SourceStoredBytes,
          anchorSourceStoredBytes: job.anchorSourceStoredBytes,
        }),
    ...(job.outputPartitionOrdinal === undefined
      ? {}
      : {
          outputPartitionOrdinal: job.outputPartitionOrdinal,
          maxWriteAmplification: job.maxWriteAmplification ?? 0,
          maximumOutputStoredBytes: job.maximumOutputStoredBytes ?? 0,
          plannedOutputStoredBytesUpperBound: job.plannedOutputStoredBytesUpperBound ?? 0,
        }),
    ...(rewritePlan.kind !== "copy-v1"
      ? {
          memoryBudgetBytes: job.memoryBudgetBytes ?? 0,
          minimumMemoryBytes: job.minimumMemoryBytes ?? 0,
          peakWorkingBytes: job.peakWorkingBytes ?? 0,
        }
      : {}),
    result,
  };
}

function compactionSkippedProgress(result: CompactTableResult): CompactionJobProgress {
  return {
    jobId: null,
    tableName: result.tableName,
    state: "skipped",
    processedRows: 0,
    sourceSegmentCount: result.sourceSegmentCount,
    sourceBlockCount: result.sourceBlockCount,
    outputBlockCount: 0,
    result,
  };
}

function compactionCancellationResult(job: CompactionJobRecord): CancelCompactionJobResult {
  if (job.state !== "cancelled" && job.state !== "published" && job.state !== "aborted") {
    throw new Error(`Compaction cancellation did not reach a terminal state: ${job.id}`);
  }
  return {
    jobId: job.id,
    state: job.state,
    publishedVersion: job.publishedVersion,
  };
}

function garbageCollectionProgress(job: GarbageCollectionJobRecord): GarbageCollectionProgress {
  const result: GarbageCollectionResult | null =
    job.state === "completed"
      ? {
          jobId: job.id,
          prunedManifestCount: job.prunedManifestCount,
          alreadyPrunedManifestCount: job.alreadyPrunedManifestCount,
          retainedManifestCount: job.retainedManifestCount,
          missingManifestCount: job.missingManifestCount,
          reclaimedSegmentCount: job.reclaimedSegmentCount,
          retainedSegmentCount: job.retainedSegmentCount,
          missingSegmentCount: job.missingSegmentCount,
          reclaimedBlockCount: job.reclaimedBlockCount,
          retainedBlockCount: job.retainedBlockCount,
          missingBlockCount: job.missingBlockCount,
          physicallyReclaimedBytes: job.reclaimedBlockBytes,
        }
      : null;
  return {
    jobId: job.id,
    state: job.state,
    examinedManifestCount: job.cursor.manifestIndex,
    examinedSegmentCount: job.cursor.segmentIndex,
    examinedBlockCount: job.cursor.blockIndex,
    result,
  };
}

function sameBytes(left: Uint8Array, right: Uint8Array): boolean {
  return left.byteLength === right.byteLength && left.every((byte, index) => byte === right[index]);
}

function sameCompactionSegment(left: SegmentRecord, right: SegmentRecord): boolean {
  if (
    left.id !== right.id ||
    left.tableId !== right.tableId ||
    left.rowCount !== right.rowCount ||
    left.rowIdStart !== right.rowIdStart ||
    left.rowIdEndExclusive !== right.rowIdEndExclusive ||
    (left.kind ?? "insert") !== (right.kind ?? "insert") ||
    left.keyColumnId !== right.keyColumnId ||
    (left.level ?? 0) !== (right.level ?? 0) ||
    left.partitionOrdinal !== right.partitionOrdinal ||
    left.logicalOrder !== right.logicalOrder
  ) {
    return false;
  }
  const leftColumnIds = Object.keys(left.columnBlockIds).sort();
  const rightColumnIds = Object.keys(right.columnBlockIds).sort();
  if (
    leftColumnIds.length !== rightColumnIds.length ||
    leftColumnIds.some((id, index) => id !== rightColumnIds[index])
  ) {
    return false;
  }
  return (
    leftColumnIds.every((columnId) => {
      const leftIds = left.columnBlockIds[columnId] ?? [];
      const rightIds = right.columnBlockIds[columnId] ?? [];
      return (
        leftIds.length === rightIds.length && leftIds.every((id, index) => id === rightIds[index])
      );
    }) && sameRowIdSpans(left.rowIdSpans, right.rowIdSpans)
  );
}

function sameRowIdSpans(
  left: readonly RowIdSpan[] | undefined,
  right: readonly RowIdSpan[] | undefined,
): boolean {
  if (left === undefined || right === undefined) return left === right;
  return (
    left.length === right.length &&
    left.every((span, index) => {
      const other = right[index];
      return (
        span.rowStart === other?.rowStart &&
        span.rowCount === other.rowCount &&
        span.rowIdStart === other.rowIdStart
      );
    })
  );
}

function sameMergeSourceSegment(
  actual: SegmentRecord,
  owner: { status: string; committedVersion: number | null } | undefined,
  planned: MergeCompactionSourceSegment,
): boolean {
  if (
    owner?.status !== "committed" ||
    owner.committedVersion !== planned.committedVersion ||
    actual.id !== planned.segmentId ||
    actual.transactionId !== planned.transactionId ||
    (actual.kind ?? "insert") !== planned.kind ||
    (actual.keyColumnId ?? planned.keyColumnId) !== planned.keyColumnId ||
    (actual.level ?? 0) !== planned.level ||
    (actual.logicalOrder ?? owner.committedVersion) !== planned.logicalOrder ||
    actual.rowCount !== planned.rowCount ||
    actual.rowIdStart !== planned.rowIdStart ||
    actual.rowIdEndExclusive !== planned.rowIdEndExclusive
  ) {
    return false;
  }
  let spans: RowIdSpan[];
  try {
    spans = mergeSourceRowIdSpans(actual, planned.kind);
  } catch {
    return false;
  }
  if (!sameRowIdSpans(spans, planned.rowIdSpans)) return false;
  const actualColumnIds = new Set(Object.keys(actual.columnBlockIds));
  if (
    actualColumnIds.size !== planned.columns.length ||
    planned.columns.some((column) => !actualColumnIds.has(column.columnId))
  ) {
    return false;
  }
  return planned.columns.every((column) => {
    const actualIds = actual.columnBlockIds[column.columnId] ?? [];
    return (
      actualIds.length === column.sourceBlocks.length &&
      actualIds.every((id, index) => id === column.sourceBlocks[index]?.blockId)
    );
  });
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

function hasContiguousRowIds(segments: readonly SegmentRecord[]): boolean {
  return segments.every((segment, index) => {
    if (segment.rowIdEndExclusive - segment.rowIdStart !== BigInt(segment.rowCount)) return false;
    const previous = segments[index - 1];
    return previous === undefined || previous.rowIdEndExclusive === segment.rowIdStart;
  });
}

interface AppendLevelTwoLayout {
  retainedPrefix: readonly SegmentRecord[];
  levelTwoSegments: readonly SegmentRecord[];
  level0Segments: readonly SegmentRecord[];
}

function appendLevelTwoLayout(segments: readonly SegmentRecord[]): AppendLevelTwoLayout | null {
  let index = 0;
  const retainedPrefix: SegmentRecord[] = [];
  const levelTwoSegments: SegmentRecord[] = [];
  const first = segments[0];
  if (first !== undefined && (first.level ?? 0) === 1) {
    if (
      (first.kind ?? "insert") !== "insert" ||
      first.rowIdSpans !== undefined ||
      first.partitionOrdinal !== undefined ||
      (first.logicalOrder !== undefined &&
        (!Number.isSafeInteger(first.logicalOrder) || first.logicalOrder < 0))
    ) {
      return null;
    }
    retainedPrefix.push(first);
    index += 1;
  }
  for (;;) {
    const segment = segments[index];
    if (segment === undefined || (segment.level ?? 0) !== 2) break;
    if (
      (segment.kind ?? "insert") !== "insert" ||
      segment.rowIdSpans !== undefined ||
      segment.partitionOrdinal !== levelTwoSegments.length ||
      !Number.isSafeInteger(segment.logicalOrder) ||
      (segment.logicalOrder ?? -1) < 0 ||
      segment.rowIdEndExclusive - segment.rowIdStart !== BigInt(segment.rowCount)
    ) {
      return null;
    }
    retainedPrefix.push(segment);
    levelTwoSegments.push(segment);
    index += 1;
  }
  const level0Segments = segments.slice(index);
  if (
    level0Segments.some(
      (segment) =>
        (segment.level ?? 0) !== 0 ||
        segment.partitionOrdinal !== undefined ||
        (segment.kind ?? "insert") !== "insert" ||
        segment.rowIdSpans !== undefined ||
        (segment.logicalOrder !== undefined &&
          (!Number.isSafeInteger(segment.logicalOrder) || segment.logicalOrder < 0)),
    )
  ) {
    return null;
  }
  const rowIdIntervals = segments
    .map((segment) => ({
      start: segment.rowIdStart,
      end: segment.rowIdEndExclusive,
      expectedEnd: segment.rowIdStart + BigInt(segment.rowCount),
    }))
    .sort((left, right) => (left.start < right.start ? -1 : left.start > right.start ? 1 : 0));
  for (const [intervalIndex, interval] of rowIdIntervals.entries()) {
    if (interval.start <= 0n || interval.end !== interval.expectedEnd) return null;
    const previous = rowIdIntervals[intervalIndex - 1];
    if (previous !== undefined && interval.start < previous.end) return null;
  }
  return { retainedPrefix, levelTwoSegments, level0Segments };
}

function compactionWriteAmplificationSkipped(input: {
  tableName: string;
  sourceSegments: readonly SegmentRecord[];
  sourceBlockIds: readonly string[];
  sourceStoredBytes: number;
  level0SourceStoredBytes: number;
  outputPartitionOrdinal: number;
  maxWriteAmplification: number;
  maximumOutputStoredBytes: number;
  plannedOutputStoredBytesUpperBound: number;
  targetBlockBytes: number;
  outputCompression: Compression;
  memoryBudgetBytes: number;
  minimumMemoryBytes: number;
  version: number;
}): CompactTableResult {
  return {
    tableName: input.tableName,
    compacted: false,
    skipReason: "write-amplification-budget",
    sourceSegmentCount: input.sourceSegments.length,
    sourceBlockCount: input.sourceBlockIds.length,
    outputSegmentId: null,
    outputBlockCount: 0,
    rowCount: input.sourceSegments.reduce((total, segment) => total + segment.rowCount, 0),
    sourceStoredBytes: input.sourceStoredBytes,
    outputStoredBytes: 0,
    level0SourceStoredBytes: input.level0SourceStoredBytes,
    anchorSourceStoredBytes: 0,
    outputPartitionOrdinal: input.outputPartitionOrdinal,
    maxWriteAmplification: input.maxWriteAmplification,
    maximumOutputStoredBytes: input.maximumOutputStoredBytes,
    plannedOutputStoredBytesUpperBound: input.plannedOutputStoredBytesUpperBound,
    targetBlockBytes: input.targetBlockBytes,
    outputCompression: input.outputCompression,
    memoryBudgetBytes: input.memoryBudgetBytes,
    minimumMemoryBytes: input.minimumMemoryBytes,
    peakWorkingBytes: 0,
    supersededBlockCount: 0,
    physicallyReclaimedBytes: 0,
    version: input.version,
    metrics: null,
  };
}

function compactTableSkipped(
  tableName: string,
  skipReason: CompactionSkipReason,
  sourceSegments: readonly SegmentRecord[],
  sourceBlockIds: readonly string[],
  version: number | null,
): CompactTableResult {
  return {
    tableName,
    compacted: false,
    skipReason,
    sourceSegmentCount: sourceSegments.length,
    sourceBlockCount: sourceBlockIds.length,
    outputSegmentId: null,
    outputBlockCount: 0,
    rowCount: sourceSegments.reduce((total, segment) => total + segment.rowCount, 0),
    sourceStoredBytes: 0,
    outputStoredBytes: 0,
    supersededBlockCount: 0,
    physicallyReclaimedBytes: 0,
    version,
    metrics: null,
  };
}

function cloneRow(row: Readonly<Record<string, BatchValue>>): Readonly<Record<string, BatchValue>> {
  return Object.fromEntries(
    Object.entries(row).map(([name, value]) => [
      name,
      value instanceof Date ? new Date(value.getTime()) : value,
    ]),
  );
}

function estimateRowBytes(row: Readonly<Record<string, BatchValue>>): number {
  return estimateValuesBytes(Object.values(row));
}

function estimateBatchBytes(input: InsertBatchInput): number {
  return Object.values(input.columns).reduce(
    (total, values) => total + estimateValuesBytes(values),
    0,
  );
}

function estimateValuesBytes(values: readonly BatchValue[]): number {
  let bytes = 0;
  for (const value of values) {
    if (typeof value === "string") bytes += 4 + sizeTextEncoder.encode(value).byteLength;
    else if (typeof value === "number" || value instanceof Date) bytes += 8;
    else bytes += 1;
  }
  return bytes;
}

function sumBytes(blocks: ReadonlyArray<{ bytes: Uint8Array }>): number {
  return blocks.reduce((total, block) => total + block.bytes.byteLength, 0);
}

function createWriteMetrics(input: {
  logicalBytes: number;
  storedBytes: number;
  encodeMs: number;
  stageMs: number;
  commitMs: number;
  totalMs: number;
  retries: number;
  rows: number;
}): WriteMetrics {
  return {
    logicalBytes: input.logicalBytes,
    storedBytes: input.storedBytes,
    writeAmplification: input.logicalBytes === 0 ? 0 : input.storedBytes / input.logicalBytes,
    encodeMs: input.encodeMs,
    stageMs: input.stageMs,
    commitMs: input.commitMs,
    totalMs: input.totalMs,
    retries: input.retries,
    rowsPerSecond: input.rows / Math.max(input.totalMs / 1_000, 0.000_001),
  };
}

function rowsToBatch(rows: ReadonlyArray<Readonly<Record<string, BatchValue>>>): InsertBatchInput {
  const names = new Set(rows.flatMap((row) => Object.keys(row)));
  return {
    columns: Object.fromEntries(
      [...names].map((name) => [name, rows.map((row) => Reflect.get(row, name))]),
    ),
  };
}

function rowToBatch(row: Readonly<Record<string, BatchValue>>): InsertBatchInput {
  return {
    columns: Object.fromEntries(Object.entries(row).map(([name, value]) => [name, [value]])),
  };
}

function asColumnInput(type: LogicalType, values: readonly BatchValue[]): ColumnInput {
  switch (type) {
    case "boolean":
      return { type, values: values as ReadonlyArray<boolean | null> };
    case "number":
      return { type, values: values as ReadonlyArray<number | null> };
    case "string":
      return { type, values: values as ReadonlyArray<string | null> };
    case "datetime":
      return { type, values: values as ReadonlyArray<Date | null> };
  }
}
