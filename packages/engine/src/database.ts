import {
  buildPhysicalColumnFromRanges,
  decodeBlock,
  decodePhysicalBlock,
  encodeBlock,
  encodePhysicalBlock,
  getCompressionMemoryBound,
  inspectBlock,
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
  type BlockStore,
  CompactionJobConflictError,
  type CompactionJobRecord,
  type CompactionJobState,
  type RechunkCompactionOutputWindow,
  type RechunkCompactionRewritePlan,
  type RechunkCompactionSourceBlock,
  type RechunkCompactionSourceColumn,
  type SegmentRecord,
  type SimpleDataType,
  type TableColumnRecord,
  type TableRecord,
  TransactionRecordConflictError,
  UniqueKeyConflictError,
  WriteConflictError,
} from "@browserdatabase/storage-idb";
import { TransactionManager, type DatabaseTransaction } from "@browserdatabase/transactions";
import {
  compileQuery,
  createPreparedQuery,
  referencedColumns,
  type PreparedQuery,
  type QueryResult,
} from "./query.js";

const sizeTextEncoder = new TextEncoder();
const DEFAULT_COMPACTION_TARGET_BLOCK_BYTES = 2 * 1024 * 1024;
const DEFAULT_COMPACTION_MEMORY_BUDGET_BYTES = 32 * 1024 * 1024;
const MAX_COMPACTION_TARGET_BLOCK_BYTES = 64 * 1024 * 1024;
const MAX_BLOCK_ENVELOPE_BYTES = 1024;

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
  minimumSegments?: number;
  /** Number of immutable output blocks processed before yielding and checkpointing. */
  maxBlocksPerStep?: number;
  targetLevel?: number;
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
  "below-segment-threshold" | "contains-mutation-segments" | "non-contiguous-row-ids";

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
  memoryBudgetBytes?: number;
  minimumMemoryBytes?: number;
  peakWorkingBytes?: number;
  result: CompactTableResult | null;
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
  async prepareQuery(sql: string, options: { version?: number } = {}): Promise<PreparedQuery> {
    const plan = compileQuery(sql);
    const tableNames = [plan.base.table, ...plan.joins.map((join) => join.table)];
    const uniqueTableNames = [...new Set(tableNames)];
    const tables = await Promise.all(uniqueTableNames.map((name) => this.#findTable(name)));
    const schemas = new Map(
      tables.map((table) => [table.name, table.columns.map(({ name }) => name)]),
    );
    const columns = referencedColumns(plan, schemas);
    const snapshotVersion =
      options.version ?? (await this.store.getCurrentManifest())?.version ?? null;
    const rows = new Map<string, DatabaseRow[]>();
    for (const table of tables) {
      const requestedColumns = columns.get(table.name);
      rows.set(
        table.name,
        await this.#materializeTable(
          table,
          snapshotVersion,
          resolveReadColumns(
            table,
            requestedColumns?.length === 0 ? [table.columns[0]?.name ?? ""] : requestedColumns,
          ),
        ),
      );
    }
    return createPreparedQuery(plan, rows);
  }

  /** Executes a read-only SELECT statement through the public query API. */
  async query(sql: string, options: { version?: number } = {}): Promise<QueryResult> {
    const prepared = await this.prepareQuery(sql, options);
    try {
      return prepared.execute();
    } finally {
      prepared.close();
    }
  }

  async listVisibleSegments(tableName: string, version?: number): Promise<VisibleSegment[]> {
    const table = await this.#findTable(tableName);
    return (await this.#visibleSegmentRecords(table, version)).map((segment) => ({
      id: segment.id,
      rowCount: segment.rowCount,
      columnBlockIds: structuredClone(segment.columnBlockIds),
    }));
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

  /** Plans or advances one restart-safe append-only compaction job. */
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

  async #planCompaction(
    table: TableRecord,
    options: CompactTableOptions,
  ): Promise<CompactionJobRecord | CompactTableResult> {
    const minimumSegments = positiveWholeNumber(
      options.minimumSegments ?? 2,
      "Compaction segment threshold",
    );
    const targetLevel = positiveWholeNumber(options.targetLevel ?? 1, "Compaction target level");
    const manifest = await this.store.getCurrentManifest();
    const version = manifest?.version ?? null;
    const sourceSegments = await this.#visibleSegmentRecords(table, version);
    const sourceBlockIds = uniqueSegmentBlockIds(sourceSegments);
    if (sourceSegments.length < minimumSegments) {
      return compactTableSkipped(
        table.name,
        "below-segment-threshold",
        sourceSegments,
        sourceBlockIds,
        version,
      );
    }
    if (sourceSegments.some((segment) => (segment.kind ?? "insert") !== "insert")) {
      return compactTableSkipped(
        table.name,
        "contains-mutation-segments",
        sourceSegments,
        sourceBlockIds,
        version,
      );
    }
    if (!hasContiguousRowIds(sourceSegments)) {
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
    const rewritePlan = await this.#createRechunkCompactionPlan(
      table,
      sourceSegments,
      targetBlockBytes,
      outputCompression,
      memoryBudgetBytes,
    );
    const minimumMemoryBytes = compactionMinimumMemoryBytes(rewritePlan);
    if (minimumMemoryBytes > memoryBudgetBytes) {
      throw new CompactionMemoryBudgetError(memoryBudgetBytes, minimumMemoryBytes);
    }

    let id = ["compaction", table.id, "manifest", String(version)].join("/");
    const existing = await this.store.getCompactionJob(id);
    if (existing !== undefined && existing.state !== "aborted") return existing;
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
      sourceStoredBytes: rechunkSourceStoredBytes(rewritePlan),
      outputStoredBytes: 0,
      logicalBytes: rechunkSourceEncodedBytes(rewritePlan),
      rewritePlan,
      outputCursor: {
        outputIndex: 0,
        columnIndex: 0,
        rowStart: rewritePlan.outputs[0]?.rowStart ?? 0,
      },
      memoryBudgetBytes,
      minimumMemoryBytes,
      peakWorkingBytes: 0,
      outputLogicalBytes: 0,
      targetLevel,
      state: "planned",
      transactionId: null,
      outputSegmentId: `${id}/output-segment`,
      publishedVersion: null,
      revision: 0,
      createdAt: timestamp,
      updatedAt: timestamp,
    };
    try {
      await this.store.createCompactionJob(job);
      return job;
    } catch (error) {
      const raced = await this.store.getCompactionJob(id);
      if (raced !== undefined) return raced;
      throw error;
    }
  }

  async #createRechunkCompactionPlan(
    table: TableRecord,
    sourceSegments: readonly SegmentRecord[],
    targetBlockBytes: number,
    outputCompression: Compression,
    memoryBudgetBytes: number,
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
    const outputs = await this.#refineRechunkOutputWindows(
      columns,
      estimatedOutputs,
      targetBlockBytes,
      outputCompression,
      memoryBudgetBytes,
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

  async #refineRechunkOutputWindows(
    columns: readonly RechunkCompactionSourceColumn[],
    estimatedOutputs: readonly RechunkCompactionOutputWindow[],
    targetBlockBytes: number,
    outputCompression: Compression,
    memoryBudgetBytes: number,
  ): Promise<RechunkCompactionOutputWindow[]> {
    const outputs: RechunkCompactionOutputWindow[] = [];
    for (const output of estimatedOutputs) {
      outputs.push(
        ...(await this.#refineRechunkOutputWindow(
          columns,
          output,
          targetBlockBytes,
          outputCompression,
          memoryBudgetBytes,
        )),
      );
    }
    return outputs;
  }

  async #refineRechunkOutputWindow(
    columns: readonly RechunkCompactionSourceColumn[],
    output: RechunkCompactionOutputWindow,
    targetBlockBytes: number,
    outputCompression: Compression,
    memoryBudgetBytes: number,
  ): Promise<RechunkCompactionOutputWindow[]> {
    let splitReason: "target" | "format" | "memory" | null = null;
    const requiredMemoryBytes = Math.max(
      ...columns.map((column) => rechunkOutputMemoryBound(column, output, outputCompression)),
    );
    if (requiredMemoryBytes > memoryBudgetBytes) {
      splitReason = "memory";
    } else {
      for (const column of columns) {
        let encodedByteLength: number;
        try {
          encodedByteLength = (
            await this.#measureRechunkPhysicalOutput(
              column,
              output,
              outputCompression,
              memoryBudgetBytes,
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
      ...(await this.#refineRechunkOutputWindow(
        columns,
        left,
        targetBlockBytes,
        outputCompression,
        memoryBudgetBytes,
      )),
      ...(await this.#refineRechunkOutputWindow(
        columns,
        right,
        targetBlockBytes,
        outputCompression,
        memoryBudgetBytes,
      )),
    ];
  }

  async #measureRechunkPhysicalOutput(
    column: RechunkCompactionSourceColumn,
    output: RechunkCompactionOutputWindow,
    outputCompression: Compression,
    memoryBudgetBytes: number,
  ) {
    const loaded = await this.#loadRechunkPhysicalRanges(
      column,
      output,
      outputCompression,
      memoryBudgetBytes,
    );
    return measurePhysicalColumnRanges(column.type, loaded.ranges);
  }

  async #runCompactionJob(
    table: TableRecord,
    initialJob: CompactionJobRecord,
    maxBlocks: number,
  ): Promise<CompactionJobProgress> {
    let job = (await this.store.getCompactionJob(initialJob.id)) ?? initialJob;
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
    if (rewritePlan.kind === "rechunk-v1") validateRechunkTablePlan(table, rewritePlan);
    const sourceSegments =
      rewritePlan.kind === "copy-v1" ? await this.#loadCompactionSources(job) : [];

    let transaction: DatabaseTransaction;
    if (job.transactionId === null) {
      const linked = await this.#beginCompactionTransaction(job);
      if (linked.transaction === null) {
        return this.#runCompactionJob(table, linked.job, maxBlocks);
      }
      ({ job, transaction } = linked);
    } else {
      if (linkedTransaction?.status !== "active") {
        const linked = await this.#beginCompactionTransaction(job);
        if (linked.transaction === null) {
          return this.#runCompactionJob(table, linked.job, maxBlocks);
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

    try {
      if (job.outputBlockIds.length > 0) {
        const pendingBlockIds = new Set(transaction.pendingBlockIds);
        const unjournaledOutputIds = job.outputBlockIds.filter((id) => !pendingBlockIds.has(id));
        if (unjournaledOutputIds.length > 0) {
          await transaction.stageExistingBlocks(unjournaledOutputIds);
        }
      }
      if (rewritePlan.kind === "rechunk-v1") {
        job = await this.#advanceRechunkCompaction(job, transaction, rewritePlan, maxBlocks);
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
        rewritePlan.kind === "rechunk-v1"
          ? rechunkOutputBlockIds(job.id, rewritePlan)
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
      if (outputSegmentId === null) throw new Error("Compaction output segment ID is missing");
      const first = sourceSegments[0];
      const last = sourceSegments[sourceSegments.length - 1];
      if (rewritePlan.kind === "copy-v1" && (first === undefined || last === undefined)) {
        throw new Error("Compaction sources disappeared");
      }
      const desiredOutputSegment: SegmentRecord = {
        id: outputSegmentId,
        tableId: table.id,
        transactionId: transaction.id,
        rowCount:
          rewritePlan.kind === "rechunk-v1"
            ? rewritePlan.totalRows
            : sourceSegments.reduce((total, segment) => total + segment.rowCount, 0),
        rowIdStart:
          rewritePlan.kind === "rechunk-v1" ? rewritePlan.rowIdStart : (first?.rowIdStart ?? 0n),
        rowIdEndExclusive:
          rewritePlan.kind === "rechunk-v1"
            ? rewritePlan.rowIdEndExclusive
            : (last?.rowIdEndExclusive ?? 0n),
        columnBlockIds:
          rewritePlan.kind === "rechunk-v1"
            ? rechunkOutputColumns(job.id, rewritePlan)
            : compactionOutputColumns(table, sourceSegments, job.id),
        kind: "insert",
        ...(table.uniqueKeyColumnId === undefined ? {} : { keyColumnId: table.uniqueKeyColumnId }),
        level: job.targetLevel,
        logicalOrder:
          rewritePlan.kind === "rechunk-v1"
            ? rewritePlan.logicalOrder
            : await this.#firstLogicalOrder(sourceSegments),
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
          const blockIds = new Set(manifest.blockIds);
          return expectedOutputIds.every((id) => blockIds.has(id));
        });
        if (visible)
          throw new Error(`Compaction output segment is already visible: ${outputSegmentId}`);
        await this.store.removeSegment(outputSegmentId);
        await transaction.stageSegment(desiredOutputSegment);
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

  async #advanceRechunkCompaction(
    initialJob: CompactionJobRecord,
    transaction: DatabaseTransaction,
    plan: RechunkCompactionRewritePlan,
    maxBlocks: number,
  ): Promise<CompactionJobRecord> {
    let job = initialJob;
    let processedBlocks = 0;
    while ((job.outputCursor?.outputIndex ?? 0) < plan.outputs.length) {
      if (processedBlocks >= maxBlocks) break;
      const cursor = job.outputCursor;
      if (cursor === null || cursor === undefined) {
        throw new Error("Rechunk compaction cursor is missing");
      }
      const output = plan.outputs[cursor.outputIndex];
      const column = plan.columns[cursor.columnIndex];
      if (output === undefined || column === undefined) {
        throw new Error("Rechunk compaction cursor is invalid");
      }
      const outputBlockId = rechunkOutputBlockId(job.id, cursor.outputIndex, cursor.columnIndex);
      const built = await this.#buildRechunkPhysicalOutput(
        plan,
        column,
        output,
        job.memoryBudgetBytes ?? 0,
      );
      const existing = await this.store.getBlock(outputBlockId);
      let outputBytes: Uint8Array;
      if (existing === undefined) {
        outputBytes = await encodePhysicalBlock(built.physical, plan.outputCompression);
        await transaction.stageBlock(outputBlockId, outputBytes);
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
        await transaction.stageExistingBlocks([outputBlockId]);
      }
      const description = inspectBlock(outputBytes);
      const nextColumnIndex = cursor.columnIndex + 1;
      const nextOutputIndex =
        nextColumnIndex === plan.columns.length ? cursor.outputIndex + 1 : cursor.outputIndex;
      const canonicalColumnIndex = nextColumnIndex === plan.columns.length ? 0 : nextColumnIndex;
      const nextRowStart =
        nextOutputIndex === plan.outputs.length
          ? plan.totalRows
          : (plan.outputs[nextOutputIndex]?.rowStart ?? output.rowStart);
      job = await this.store.updateCompactionJob(job.id, job.revision, {
        outputBlockIds: [...job.outputBlockIds, outputBlockId],
        outputCursor: {
          outputIndex: nextOutputIndex,
          columnIndex: canonicalColumnIndex,
          rowStart: nextRowStart,
        },
        processedRows: nextRowStart,
        outputStoredBytes: safeWholeNumberSum(
          [job.outputStoredBytes, outputBytes.byteLength],
          "Compaction output stored bytes",
        ),
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

  async #buildRechunkPhysicalOutput(
    plan: RechunkCompactionRewritePlan,
    column: RechunkCompactionSourceColumn,
    output: RechunkCompactionOutputWindow,
    memoryBudgetBytes: number,
  ): Promise<{ physical: ValidatedPhysicalColumn; peakWorkingBytes: number }> {
    const loaded = await this.#loadRechunkPhysicalRanges(
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

  async #loadRechunkPhysicalRanges(
    column: RechunkCompactionSourceColumn,
    output: RechunkCompactionOutputWindow,
    outputCompression: Compression,
    memoryBudgetBytes: number,
  ): Promise<{ ranges: PhysicalColumnRange[]; peakWorkingBytes: number }> {
    const memoryBound = rechunkOutputMemoryBound(column, output, outputCompression);
    if (memoryBound > memoryBudgetBytes) {
      throw new CompactionMemoryBudgetError(memoryBudgetBytes, memoryBound);
    }
    const outputEnd = safeWholeNumberSum(
      [output.rowStart, output.rowCount],
      "Compaction output row range",
    );
    const ranges: PhysicalColumnRange[] = [];
    for (const sourceBlock of overlappingRechunkSourceBlocks(column, output)) {
      const bytes = await this.store.getBlock(sourceBlock.blockId);
      if (bytes === undefined) {
        throw new Error(`A compaction source block is missing: ${sourceBlock.blockId}`);
      }
      const description = inspectBlock(bytes);
      if (
        bytes.byteLength !== sourceBlock.storedBytes ||
        description.encodedLength !== sourceBlock.encodedBytes ||
        description.checksum !== sourceBlock.checksum ||
        description.rowCount !== sourceBlock.rowCount ||
        description.type !== column.type
      ) {
        throw new Error(`A compaction source block differs from its plan: ${sourceBlock.blockId}`);
      }
      const decoded = await decodePhysicalBlock(bytes);
      const sourceEnd = safeWholeNumberSum(
        [sourceBlock.rowStart, sourceBlock.rowCount],
        "Compaction source row range",
      );
      const start = Math.max(output.rowStart, sourceBlock.rowStart) - sourceBlock.rowStart;
      const end = Math.min(outputEnd, sourceEnd) - sourceBlock.rowStart;
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

  async #beginCompactionTransaction(job: CompactionJobRecord): Promise<{
    job: CompactionJobRecord;
    transaction: DatabaseTransaction | null;
  }> {
    const candidate = await this.#transactions.begin();
    const snapshot = await candidate.snapshot();
    const missingSourceId = job.sourceBlockIds.find((id) => !snapshot.hasBlock(id));
    if (missingSourceId !== undefined) {
      if (candidate.status === "active") await candidate.abort();
      const latest = await this.store.getCompactionJob(job.id);
      if (latest !== undefined && latest.revision !== job.revision) {
        return { job: latest, transaction: null };
      }
      const reason = `Compaction source is no longer visible: ${missingSourceId}`;
      if (latest !== undefined && latest.state !== "published" && latest.state !== "aborted") {
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
    const rowCount = rewritePlan.kind === "rechunk-v1" ? rewritePlan.totalRows : job.processedRows;
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
      outputLogicalBytes,
      ...(rewritePlan.kind === "rechunk-v1"
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
    const segments = await this.#visibleSegmentRecords(table, version);
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
      const segmentBlockIds = Object.entries(segment.columnBlockIds)
        .filter(([columnId]) => neededColumnIds.has(columnId))
        .flatMap(([, ids]) => ids);
      const decodedColumns = await this.#loadDecodedBlocks(segmentBlockIds);
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

  async #loadDecodedBlocks(blockIds: readonly string[]): Promise<Map<string, DecodedColumn>> {
    if (blockIds.length === 0) return new Map();
    const decoded = new Map<string, DecodedColumn>();
    const decodeWindow = 16;
    for (let start = 0; start < blockIds.length; start += decodeWindow) {
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

  async #visibleSegmentRecords(
    table: TableRecord,
    version?: number | null,
  ): Promise<SegmentRecord[]> {
    const snapshot = await this.#transactions.openSnapshot(version);
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

function validateRechunkTablePlan(table: TableRecord, plan: RechunkCompactionRewritePlan): void {
  if (
    table.columns.length !== plan.columns.length ||
    table.columns.some((column, index) => {
      const planned = plan.columns[index];
      return planned?.columnId !== column.id || planned.type !== column.type;
    })
  ) {
    throw new Error(`Compaction table schema changed after planning: ${table.name}`);
  }
}

function rechunkSourceStoredBytes(plan: RechunkCompactionRewritePlan): number {
  return safeWholeNumberSum(
    plan.columns.flatMap((column) => column.sourceBlocks.map((block) => block.storedBytes)),
    "Compaction source stored bytes",
  );
}

function rechunkSourceEncodedBytes(plan: RechunkCompactionRewritePlan): number {
  return safeWholeNumberSum(
    plan.columns.flatMap((column) => column.sourceBlocks.map((block) => block.encodedBytes)),
    "Compaction source encoded bytes",
  );
}

function overlappingRechunkSourceBlocks(
  column: RechunkCompactionSourceColumn,
  output: RechunkCompactionOutputWindow,
): readonly RechunkCompactionSourceBlock[] {
  const outputEnd = safeWholeNumberSum(
    [output.rowStart, output.rowCount],
    "Compaction output row range",
  );
  return column.sourceBlocks.filter((block) => {
    const blockEnd = safeWholeNumberSum(
      [block.rowStart, block.rowCount],
      "Compaction source row range",
    );
    return block.rowStart < outputEnd && blockEnd > output.rowStart;
  });
}

function rechunkOutputMemoryBound(
  column: RechunkCompactionSourceColumn,
  output: RechunkCompactionOutputWindow,
  compression: Compression,
): number {
  const sourceBlocks = overlappingRechunkSourceBlocks(column, output);
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

function compactionMinimumMemoryBytes(plan: RechunkCompactionRewritePlan): number {
  let minimumBytes = 1;
  for (const output of plan.outputs) {
    for (const column of plan.columns) {
      minimumBytes = Math.max(
        minimumBytes,
        rechunkOutputMemoryBound(column, output, plan.outputCompression),
      );
    }
  }
  return minimumBytes;
}

function rechunkOutputBlockId(jobId: string, outputIndex: number, columnIndex: number): string {
  return [
    jobId,
    "rewrite",
    "window",
    String(outputIndex).padStart(8, "0"),
    "column",
    String(columnIndex).padStart(8, "0"),
  ].join("/");
}

function rechunkOutputBlockIds(jobId: string, plan: RechunkCompactionRewritePlan): string[] {
  return plan.outputs.flatMap((_output, outputIndex) =>
    plan.columns.map((_column, columnIndex) =>
      rechunkOutputBlockId(jobId, outputIndex, columnIndex),
    ),
  );
}

function rechunkOutputColumns(
  jobId: string,
  plan: RechunkCompactionRewritePlan,
): Record<string, string[]> {
  return Object.fromEntries(
    plan.columns.map((column, columnIndex) => [
      column.columnId,
      plan.outputs.map((_output, outputIndex) =>
        rechunkOutputBlockId(jobId, outputIndex, columnIndex),
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
    ...(rewritePlan.kind === "rechunk-v1"
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
  return leftColumnIds.every((columnId) => {
    const leftIds = left.columnBlockIds[columnId] ?? [];
    const rightIds = right.columnBlockIds[columnId] ?? [];
    return (
      leftIds.length === rightIds.length && leftIds.every((id, index) => id === rightIds[index])
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
