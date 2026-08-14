import {
  toColumnarBatch,
  type BatchRow,
  type BatchValue,
  type ColumnarBatch,
  type InsertBatchInput,
} from "./batch.js";
import {
  CompactionJobCancelledError,
  CompactionMemoryBudgetError,
  CompactionWriteAmplificationError,
  MissingKeyError,
  SqlCompileError,
  UniqueConstraintError,
} from "./errors.js";
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
  type DecodedPhysicalBlock,
  type LogicalType,
  type PhysicalColumnRange,
  type ValidatedPhysicalColumn,
} from "../block-format/index.js";
import {
  fillColumnDefaults,
  patchAutoIncrementValues,
  type AutoIncrementFill,
  type FilledBatch,
} from "./defaults.js";
import {
  cachedQueryTerms,
  FTS_TOKENIZER_VERSION,
  renderDocumentValue,
  tokenize as ftsTokenize,
  type FtsStats,
} from "./fts.js";
import {
  simpleDataTypes,
  floorWholeNumberProduct,
  type BlockStore,
  type ColumnDefault,
  validateColumnDefault,
  validateEnumValues,
  type FtsColumnDelta,
  type FtsColumnIndexRecord,
  type FtsPosting,
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
  type QueryCatalogState,
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
  type TransactionRecord,
  SnapshotManifestMissingError,
  TableRecordConflictError,
  TransactionRecordConflictError,
  UniqueKeyConflictError,
  WriteConflictError,
} from "../storage/index.js";
import {
  Snapshot,
  TransactionManager,
  type DatabaseTransaction,
  type LeasedSnapshot,
} from "../transactions/index.js";
import {
  applyWindowFunctions,
  bindPlanParameters,
  bindStatementParameters,
  DUAL_TABLE,
  dualTableRows,
  blockHasSubqueries,
  combineUnionResults,
  compileQuery,
  hasAggregate,
  createRecursiveCteState,
  compileStatement,
  createPreparedColumnarQuery,
  evaluateRowExpression,
  expressionColumnNames,
  inferBlockSchema,
  referencedColumns,
  childExpressions,
  expandFtsColumns,
  forEachBlockExpression,
  planContainsFts,
  projectResultColumns,
  subqueryResolutionSteps,
  topLevelFtsMatchConjuncts,
  transparentProjectionSource,
  windowOutputType,
  type ComparisonOperator,
  type PredicateOperator,
  type CompiledQuery,
  type CompiledStatement,
  type Expression,
  type InsertValue,
  type PreparedQuery,
  type QueryResult,
  type QueryRow,
  type QueryValue,
  type SqlColumnSchema,
} from "./query.js";
import {
  QueryMemoryBudgetError,
  QueryMemoryContext,
  type QueryMemoryReservation,
} from "./memory.js";
import { LiveQuerySet, type LiveQuerySetOptions } from "./live.js";
import { renderPlan } from "./optimizer.js";
import {
  applyColumnSteps,
  planMigration,
  type AnyTable,
  type MigrationStep,
  type SchemaDefinition,
} from "./schema.js";
import {
  columnarTableFromRows,
  createColumnarTable,
  vectorValue,
  type ColumnarTable,
  type ColumnVector,
  type QuerySpillStore,
  type VectorWindow,
} from "./vector.js";

// The on-disk format is little-endian; the bulk Float64 copy reads platform order.
const PLATFORM_LITTLE_ENDIAN = new Uint8Array(new Uint32Array([1]).buffer)[0] === 1;
const vectorTextDecoder = new TextDecoder("utf-8", { fatal: true });
const NULL_STRING_VECTOR_CODE = 0xffffffff;
/** Delta-chunk tail length past which a search schedules a fold-by-rebuild of the base. */
const FTS_FOLD_DELTA_CHUNKS = 16;
const DEFAULT_COMPACTION_TARGET_BLOCK_BYTES = 2 * 1024 * 1024;
const DEFAULT_COMPACTION_MEMORY_BUDGET_BYTES = 32 * 1024 * 1024;
const DEFAULT_COMPACTION_MINIMUM_LEVEL_ZERO_SEGMENTS = 2;
const DEFAULT_COMPACTION_MAXIMUM_LEVEL_ZERO_SEGMENTS = 16;
const DEFAULT_COMPACTION_MAXIMUM_LEVEL_ZERO_STORED_BYTES = 64 * 1024 * 1024;
const DEFAULT_LEVEL_TWO_MAX_WRITE_AMPLIFICATION = 16;
const MAX_COMPACTION_TARGET_BLOCK_BYTES = 64 * 1024 * 1024;
const MAX_BLOCK_ENVELOPE_BYTES = 1024;
const INTERNAL_READ_LEASE_TTL_MS = 60_000;
/** Distinct SQL statements whose optimized plans stay cached; plans are a few KB each. */
const PLAN_CACHE_LIMIT = 512;

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
  readonly sources: MergeResolvedSource[];
}

interface SegmentVisibilityCatalog {
  readonly transactions: ReadonlyMap<string, { readonly committedVersion: number | null }>;
  readonly segmentsByTable: ReadonlyMap<string, readonly SegmentRecord[]>;
}

interface ZonePredicate {
  readonly column: TableColumnRecord;
  readonly operator: ComparisonOperator;
  readonly value: number;
}

interface SelectedAppendSegment {
  readonly segment: SegmentRecord;
  readonly blockIndexes: readonly number[];
  readonly rowCounts: readonly number[];
}

export interface ColumnDefinition {
  name: string;
  type: SimpleDataType;
  nullable?: boolean;
  /** Fills null-or-absent slots at insert time; never applied at read time. */
  defaultValue?: ColumnDefault;
  /** String columns only: the closed set of values writes must draw from. */
  enumValues?: readonly string[];
}

export interface CreateTableInput {
  name: string;
  columns: readonly ColumnDefinition[];
  uniqueKey?: string;
}

export type { BatchRow, BatchValue, ColumnarBatch, InsertBatchInput } from "./batch.js";

export interface InsertBatchResult {
  tableName: string;
  segmentId: string;
  rowCount: number;
  blockCount: number;
  storedBytes: number;
  version: number;
  metrics: WriteMetrics;
  /**
   * Full written vectors, input row order, for every column where the engine filled at least
   * one default or generated slot. Absent when the batch was written exactly as provided.
   */
  generatedColumns?: Record<string, BatchValue[]>;
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
   * Values for the statement's `?`/`$n` placeholders, in order. Required exactly when the
   * statement has placeholders; the compiled plan is cached on the SQL text and re-bound per
   * execution, so parameterized queries skip re-parsing.
   */
  readonly params?: readonly QueryValue[];
  /**
   * Budget for the documented modeled vector, row-index, group/result payload, and ordering buffers.
   * Boxed snapshot preparation, JavaScript container overhead, returned-result lifetime, and browser
   * allocator overhead are not included in this Phase 7B-B model.
   */
  readonly executionMemoryBudgetBytes?: number;
  /** Forces durable temp pages; with an explicit budget, spill otherwise retries only after exhaustion. */
  readonly spillToStorage?: boolean;
  /** Maximum rows encoded in each merged spill page. */
  readonly spillPageRows?: number;
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
  | "keys-outside-selected-sources"
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
  /** Stored bytes cancelled or aborted attempts at these sources already wrote. */
  priorAttemptOutputStoredBytes?: number;
  /** This attempt's output plus every prior failed attempt's, sharing one lifetime ceiling. */
  lifetimeOutputStoredBytes?: number;
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
  /** Maximum block/segment candidates copied into one durable planning record. */
  maxPlanningItems?: number;
}

export interface CollectGarbageStepOptions {
  /** Maximum candidates examined and checkpointed by this durable reclamation step. */
  maxItems?: number;
  /** Maximum block/segment candidates copied into a newly planned job. */
  maxPlanningItems?: number;
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

export {
  CompactionJobCancelledError,
  CompactionMemoryBudgetError,
  CompactionWriteAmplificationError,
  MissingKeyError,
  SqlCompileError,
  UniqueConstraintError,
};

export interface MinnowDatabaseOptions {
  compression?: Compression;
  rowsPerBlock?: number;
  maxCommitRetries?: number;
  now?: () => Date;
  createId?: () => string;
  /** Durable spill-owner lease lifetime; renewed while a spilling query runs. */
  spillOwnerLeaseMs?: number;
  /**
   * Retained bytes for the prepare cache: assembled append-table column vectors,
   * zone-pruned projections, and derived/subquery block results that let repeated query
   * preparation skip block reads, decompression, vector assembly, and deterministic
   * re-execution. Entries are keyed by the exact visible segment ids (plus predicate
   * values and the block plan where relevant), so a cached entry can never serve stale
   * data; writes simply stop matching and old entries age out of the byte-bounded LRU.
   * 0 disables the cache. Defaults to 64 MiB.
   */
  prepareCacheBytes?: number;
  /**
   * Visible-row threshold at which a full-text MATCH on an unindexed append-only column
   * schedules a background index build (fire-and-forget; correctness never waits on it).
   * Defaults to 4096.
   */
  ftsAutoIndexRows?: number;
}

export interface QuerySpillCleanupOptions {
  /** Maximum owners examined in this pass. */
  maxOwners?: number;
}

export interface QuerySpillCleanupResult {
  ownersExamined: number;
  ownersReclaimed: number;
  ownersRetained: number;
}

export type ExecuteResult =
  | { kind: "rows"; result: QueryResult }
  | { kind: "create-table"; table: string }
  | {
      kind: "insert";
      table: string;
      rowCount: number;
      /** Absent when the statement affected no rows (an INSERT ... SELECT of an empty set). */
      version?: number;
      returnedRows?: QueryRow[];
    }
  | { kind: "update"; table: string; rowCount: number; version?: number; returnedRows?: QueryRow[] }
  | {
      kind: "delete";
      table: string;
      rowCount: number;
      version?: number | null;
      returnedRows?: QueryRow[];
    };

/** Rejects an INSERT value that is still an unbound `?`/`$n` slot. */
function boundInsertValue(value: InsertValue): QueryValue {
  if (typeof value === "object" && value !== null && !(value instanceof Date)) {
    throw new TypeError("Statement has unbound placeholders; pass parameters when executing");
  }
  return value;
}

export interface RunStatementOptions {
  /**
   * Projects the affected rows back: column names, or "*" for every table column. Inserts echo
   * the written values; updates return post-update values; deletes return the rows as read.
   */
  readonly returning?: readonly string[] | "*";
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

export class MinnowDatabase {
  readonly #transactions: TransactionManager;
  readonly #compression: Compression;
  readonly #rowsPerBlock: number;
  readonly #maxCommitRetries: number;
  readonly #now: () => Date;
  readonly #spillOwnerLeaseMs: number;
  readonly #createId: () => string;
  readonly #internalLeaseOwnerId = `minnow/${crypto.randomUUID()}`;
  readonly #liveSets = new Set<LiveQuerySet>();
  #internalLeaseSequence = 0;
  readonly #prepareCacheLimitBytes: number;
  readonly #ftsAutoIndexRows: number;
  /** One background build attempt per (table, column) per session; misses just stay scans. */
  readonly #ftsBuildsInFlight = new Set<string>();
  /** Insertion order doubles as LRU order; hits re-insert their entry. */
  readonly #prepareCache = new Map<string, { payload: unknown; bytes: number }>();
  #prepareCacheUsedBytes = 0;
  /**
   * SQL text to optimized plan, LRU by insertion order. Compiled plans are never mutated after
   * optimization — subquery resolution and CTE expansion clone before rewriting and join
   * reordering spreads into fresh objects — so a hit shares the cached plan across executions
   * and skips tokenize/parse/optimize entirely.
   */
  readonly #planCache = new Map<string, CompiledQuery>();
  readonly #visibilityFingerprints = new WeakMap<SegmentVisibilityCatalog, Map<string, string>>();
  readonly #visibleSegmentsMemo = new WeakMap<
    SegmentVisibilityCatalog,
    Map<string, SegmentRecord[]>
  >();
  /** Per-snapshot postings reads: statistics and pruning share one result per (column, terms). */
  readonly #ftsCandidatesMemo = new WeakMap<Snapshot, Map<string, Promise<FtsCandidatesResult>>>();
  #sharedLease: SharedLeaseEntry | undefined;
  #sharedLeaseRenewal: Promise<void> | undefined;

  constructor(
    private readonly store: BlockStore,
    options: MinnowDatabaseOptions = {},
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
    this.#spillOwnerLeaseMs = options.spillOwnerLeaseMs ?? 60_000;
    if (!Number.isSafeInteger(this.#spillOwnerLeaseMs) || this.#spillOwnerLeaseMs <= 0) {
      throw new RangeError("Spill owner lease lifetime must be a positive whole number");
    }
    this.#prepareCacheLimitBytes = options.prepareCacheBytes ?? 64 * 1024 * 1024;
    if (!Number.isSafeInteger(this.#prepareCacheLimitBytes) || this.#prepareCacheLimitBytes < 0) {
      throw new RangeError("Prepare cache bytes must be a non-negative whole number");
    }
    this.#ftsAutoIndexRows = options.ftsAutoIndexRows ?? 4096;
    if (!Number.isSafeInteger(this.#ftsAutoIndexRows) || this.#ftsAutoIndexRows < 0) {
      throw new RangeError(
        "Full-text auto-index row threshold must be a non-negative whole number",
      );
    }
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
      if (column.enumValues !== undefined && column.type !== "string") {
        throw new TypeError(`Enum values require a string column: ${columnName}`);
      }
      return {
        id: this.#createId(),
        name: columnName,
        type: column.type,
        nullable: column.nullable ?? false,
        ...(column.defaultValue === undefined ? {} : { defaultValue: column.defaultValue }),
        ...(column.enumValues === undefined
          ? {}
          : { enumValues: validateEnumValues(column.enumValues, columnName) }),
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
    for (const column of columns) {
      if (column.defaultValue !== undefined) {
        validateColumnDefault(
          { ...column, isUniqueKey: column === uniqueKeyColumn },
          column.defaultValue,
        );
      }
    }
    await this.store.addTable({
      id: this.#createId(),
      name,
      columns,
      ...(uniqueKeyColumn === undefined ? {} : { uniqueKeyColumnId: uniqueKeyColumn.id }),
      ...(uniqueKeyColumn === undefined ? {} : { uniqueKeyLookupReady: true }),
      ...(uniqueKeyColumn === undefined ? {} : { uniqueKeyStorage: "chunks-v2" as const }),
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
          ...(column.defaultValue === undefined ? {} : { defaultValue: column.defaultValue }),
          ...(column.enumValues === undefined ? {} : { enumValues: [...column.enumValues] }),
        })),
        ...(uniqueKey === undefined ? {} : { uniqueKey }),
      };
    });
  }

  async insertBatch(tableName: string, input: InsertBatchInput): Promise<InsertBatchResult> {
    const table = await this.#findTable(tableName);
    const { batch, generated, autoIncrement, rowCount } = this.#fillDefaults(table, input);
    const keys =
      autoIncrement === undefined || autoIncrement.missingIndexes.length === 0
        ? batchKeys(table, batch)
        : undefined;
    const result = await this.#writeBatch(table, batch, "insert", keys, autoIncrement);
    collectAutoIncrementGenerated(batch, generated, autoIncrement);
    return {
      tableName: result.tableName,
      segmentId: result.segmentId,
      rowCount,
      blockCount: result.blockCount,
      storedBytes: result.storedBytes,
      version: result.version,
      metrics: result.metrics,
      ...(generated.size === 0 ? {} : { generatedColumns: Object.fromEntries(generated) }),
    };
  }

  /** Pivots, fills pure defaults, and validates — the shared front half of insert and upsert. */
  #fillDefaults(table: TableRecord, input: InsertBatchInput): FilledBatch & { rowCount: number } {
    const pivoted = toColumnarBatch(input);
    // The pivot stamps rowCount, so an all-default batch keeps its row count even after the
    // columnar form crossed the worker boundary.
    const filled = fillColumnDefaults(table, pivoted, this.#now, pivoted.rowCount);
    const rowCount = validateBatch(table, filled.batch, filled.autoIncrement?.column.name);
    return { ...filled, rowCount };
  }

  async insert(tableName: string, row: BatchRow): Promise<InsertBatchResult> {
    return this.insertBatch(tableName, [row]);
  }

  async upsertBatch(tableName: string, input: InsertBatchInput): Promise<UpsertBatchResult> {
    const table = await this.#findTable(tableName);
    const keyColumn = getUniqueKeyColumn(table);
    if (keyColumn === undefined) {
      throw new TypeError(`Table needs a unique key before it can be upserted: ${table.name}`);
    }
    const { batch, generated, autoIncrement, rowCount } = this.#fillDefaults(table, input);
    const deferred = autoIncrement !== undefined && autoIncrement.missingIndexes.length > 0;
    const keys = deferred ? undefined : batchKeys(table, batch);
    const result = await this.#writeBatch(table, batch, "upsert", keys, autoIncrement);
    collectAutoIncrementGenerated(batch, generated, autoIncrement);
    return {
      ...result,
      rowCount,
      ...(generated.size === 0 ? {} : { generatedColumns: Object.fromEntries(generated) }),
    };
  }

  async upsert(tableName: string, row: BatchRow): Promise<UpsertBatchResult> {
    return this.upsertBatch(tableName, [row]);
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
    let deletedRowCount: number;
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
          this.#notifyLiveCommit();
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
      const batchBlockWrites: Array<{ id: string; bytes: Uint8Array }> = [];
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
        batchBlockWrites.push(...blockWrites);
      }
      const stageStarted = performance.now();
      await transaction.stageArtifacts(batchBlockWrites, [
        {
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
        },
      ]);
      stageMs += performance.now() - stageStarted;

      for (let attempt = 0; attempt <= this.#maxCommitRetries; attempt += 1) {
        const commitStarted = performance.now();
        try {
          const manifest = await transaction.commit();
          commitMs += performance.now() - commitStarted;
          this.#notifyLiveCommit();
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
    input: ColumnarBatch,
    kind: "insert" | "upsert",
    keys: Map<string, Exclude<BatchValue, null>> | undefined,
    autoIncrement?: AutoIncrementFill,
  ): Promise<UpsertBatchResult> {
    const started = performance.now();
    const rowCount = input.columns[table.columns[0]?.name ?? ""]?.length ?? 0;
    const logicalBytes = estimateBatchBytes(input);
    const {
      transaction,
      rowIds: reservedRowIds,
      autoIncrementValues,
    } = await this.#transactions.beginWithReservation(
      { tableId: table.id, count: rowCount },
      autoIncrement === undefined
        ? undefined
        : {
            tableId: table.id,
            columnId: autoIncrement.column.id,
            count: autoIncrement.missingIndexes.length,
            atLeast: autoIncrement.atLeast,
          },
    );
    let resolvedKeys = keys;
    const segmentId = this.#createId();
    const columnBlockIds: Record<string, string[]> = {};
    const batchBlockWrites: Array<{ id: string; bytes: Uint8Array }> = [];
    let storedBytes = 0;
    let blockCount = 0;
    let counts: { inserted: number; updated: number };
    let encodeMs = 0;
    let stageMs = 0;
    let commitMs = 0;
    let retries = 0;

    try {
      if (autoIncrement !== undefined && autoIncrement.missingIndexes.length > 0) {
        // Generated values are assigned once here and survive commit-retry rebase unchanged,
        // exactly like reserved row ids; an abort burns the range and leaves a gap.
        const values =
          autoIncrementValues ??
          (await this.store.reserveAutoIncrement(
            table.id,
            autoIncrement.column.id,
            autoIncrement.missingIndexes.length,
            autoIncrement.atLeast,
          ));
        patchAutoIncrementValues(input, autoIncrement, values);
        // The pending column's nulls were exempted from pre-validation; the patched slots pass
        // the same validator as caller-supplied values, so every persisted slot is validated.
        const patched = input.columns[autoIncrement.column.name] ?? [];
        for (const rowIndex of autoIncrement.missingIndexes) {
          validateValue(autoIncrement.column, patched[rowIndex] ?? null, rowIndex);
        }
        resolvedKeys = batchKeys(table, input);
      }
      if (resolvedKeys !== undefined) {
        transaction.setUniqueKeyChanges({
          tableId: table.id,
          keyTokens: [...resolvedKeys.keys()],
          requireAbsent: kind === "insert",
          ...(table.uniqueKeyStorage === undefined ? {} : { storageMode: table.uniqueKeyStorage }),
        });
      }
      counts =
        kind === "insert" && resolvedKeys !== undefined
          ? { inserted: resolvedKeys.size, updated: 0 }
          : await this.#classifyKeys(table, transaction.snapshotVersion, kind, resolvedKeys);
      const rowIds = reservedRowIds ?? (await this.store.reserveRowIds(table.id, rowCount));
      // Insert batches maintain any active full-text index by tokenizing their own rows into a
      // commit delta. Other mutation kinds emit nothing on purpose: the store's stale-writer
      // rule then flips the index to "invalid" (keyed histories are unindexable) and the scan
      // path stays correct.
      if (kind === "insert") {
        const ftsDeltas = buildFtsColumnDeltas(table, input, rowIds.start);
        if (ftsDeltas.length > 0) {
          transaction.setFtsChanges({ tableId: table.id, columns: ftsDeltas });
        }
      }
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
      const stageStarted = performance.now();
      await transaction.stageArtifacts(batchBlockWrites, [segment]);
      stageMs += performance.now() - stageStarted;

      for (let attempt = 0; attempt <= this.#maxCommitRetries; attempt += 1) {
        const commitStarted = performance.now();
        try {
          const manifest = await transaction.commit();
          commitMs += performance.now() - commitStarted;
          this.#notifyLiveCommit();
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
            kind === "insert" && resolvedKeys !== undefined
              ? { inserted: resolvedKeys.size, updated: 0 }
              : await this.#classifyKeys(table, snapshot.version, kind, resolvedKeys);
        }
      }
      throw new Error("Commit retry limit was exceeded");
    } catch (error) {
      await transaction.abort();
      if (error instanceof UniqueKeyConflictError && resolvedKeys !== undefined) {
        const keyColumn = getUniqueKeyColumn(table);
        const value = resolvedKeys.get(error.keyToken);
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
    return this.#prepareCompiledPlan(
      bindPlanParameters(this.#compileCached(sql), options.params),
      options,
    );
  }

  #compileCached(sql: string): CompiledQuery {
    const cached = this.#planCache.get(sql);
    if (cached !== undefined) {
      this.#planCache.delete(sql);
      this.#planCache.set(sql, cached);
      return cached;
    }
    const plan = compileQuery(sql);
    this.#planCache.set(sql, plan);
    if (this.#planCache.size > PLAN_CACHE_LIMIT) {
      const oldest = this.#planCache.keys().next().value;
      if (oldest !== undefined) this.#planCache.delete(oldest);
    }
    return plan;
  }

  async #prepareCompiledPlan(
    plan: CompiledQuery,
    options: QueryOptions = {},
  ): Promise<PreparedQuery> {
    // The ORDER-BY-expression desugar's wrapper is projection-only: prepare the inner block
    // directly (no derived materialization) and project each result to the visible aliases,
    // so `.search()` costs the same whether or not the caller also selects the score.
    const wrapper = transparentProjectionSource(plan);
    if (wrapper !== undefined) {
      const prepared = await this.#prepareCompiledPlan(wrapper.inner, options);
      return {
        sql: prepared.sql,
        tables: prepared.tables,
        get memoryUsage() {
          return prepared.memoryUsage;
        },
        execute: () => projectResultColumns(prepared.execute(), wrapper.aliases),
        executeAsync: async (asyncOptions) =>
          projectResultColumns(await prepared.executeAsync(asyncOptions), wrapper.aliases),
        close: () => prepared.close(),
      };
    }
    const memory = new QueryMemoryContext(options.executionMemoryBudgetBytes);
    try {
      let columnarTables = new Map<string, ColumnarTable>();
      let resolvedPlan = plan;
      let ftsStats: Map<string, FtsStats> | undefined;
      const prepareAtSnapshot = async (
        snapshot: LeasedSnapshot,
        realTables: Map<string, TableRecord>,
        visibility: SegmentVisibilityCatalog,
      ): Promise<void> => {
        const typedSchemas = new Map<string, SqlColumnSchema[]>(
          [...realTables.values()].map((table) => [
            table.name,
            table.columns.map(({ name, type }) => ({ name, type })),
          ]),
        );
        // MATCH(*)/BM25(*) expand against the catalog first — copy-on-write, so the compile
        // cache's plan (and the parity tests) keep "*" — and subquery resolution then collects
        // its steps from the expanded plan so substitutions land in the object that executes.
        const expandedPlan = expandFtsColumns(plan, (tableName) =>
          searchableFtsColumns(realTables.get(tableName)),
        );
        const resolution = subqueryResolutionSteps(expandedPlan);
        for (const step of resolution.steps) {
          step.substitute(
            await this.#executeBlockCached(
              step.block,
              snapshot,
              visibility,
              memory,
              realTables,
              typedSchemas,
            ),
          );
        }
        resolvedPlan = resolution.plan;
        // Index-served BM25 statistics, computed against the same catalog snapshot the pruner
        // reads, so a pruned scoring scan always carries exact corpus numbers.
        ftsStats = await this.#ftsIndexStats(resolvedPlan, realTables, snapshot, visibility);
        columnarTables = await this.#prepareBlockInputs(
          resolvedPlan,
          snapshot,
          visibility,
          memory,
          realTables,
          typedSchemas,
        );
      };
      if (options.version !== undefined) {
        // Explicit time travel keeps the per-call lease and version-anchored reads.
        const realTables = await this.#findRealBlockTables(plan);
        await this.#withLeasedSnapshot(options.version, async (snapshot) => {
          const visibility = await this.#blockSegmentVisibility(realTables);
          await prepareAtSnapshot(snapshot, realTables, visibility);
        });
      } else {
        await this.#withSharedCatalogSnapshot(collectRealTableNames(plan), prepareAtSnapshot);
      }
      return createPreparedColumnarQuery(
        chooseJoinOrder(resolvedPlan, columnarTables),
        columnarTables,
        memory,
        ftsStats === undefined ? {} : { ftsStats },
      );
    } catch (error) {
      memory.close();
      throw error;
    }
  }

  /**
   * Reads the catalog state every prepare needs in one coherent store read, then runs the
   * action under a shared internal reader lease anchored to that state's manifest version.
   * The lease is reused across prepares at the same version, so steady-state preparation
   * costs one catalog read instead of per-record round trips plus lease create/release
   * write transactions. If the manifest is pruned between the read and the lease, the
   * state is re-read.
   */
  async #withSharedCatalogSnapshot<T>(
    names: readonly string[],
    action: (
      snapshot: LeasedSnapshot,
      realTables: Map<string, TableRecord>,
      visibility: SegmentVisibilityCatalog,
    ) => Promise<T>,
  ): Promise<T> {
    for (;;) {
      const state = await this.#queryCatalogState(names);
      const realTables = new Map<string, TableRecord>();
      names.forEach((name, index) => {
        const table = state.tables[index];
        if (table === undefined) throw new Error(`Table not found: ${name}`);
        realTables.set(table.name, table);
      });
      const segmentsByTable = new Map<string, SegmentRecord[]>();
      for (const segment of state.segments) {
        const tableSegments = segmentsByTable.get(segment.tableId) ?? [];
        tableSegments.push(segment);
        segmentsByTable.set(segment.tableId, tableSegments);
      }
      const visibility: SegmentVisibilityCatalog = {
        transactions: new Map(state.transactions.map((record) => [record.id, record] as const)),
        segmentsByTable,
      };
      const entry = await this.#acquireSharedLease(state.manifestVersion);
      if (entry === undefined) continue;
      try {
        return await action(entry.lease, realTables, visibility);
      } finally {
        this.#releaseSharedLease(entry);
      }
    }
  }

  /** One coherent catalog read via the store's batched method, or its sequential shape. */
  async #queryCatalogState(names: readonly string[]): Promise<QueryCatalogState> {
    const batched = this.store.getQueryCatalogState?.bind(this.store);
    if (batched !== undefined) return batched(names);
    // The version is read first so the segment listing is a superset of the version's
    // committed segments; blocks outside the leased manifest filter out during reads.
    const manifestVersion = await this.store.getCurrentManifestVersion();
    const tables = await Promise.all(names.map((name) => this.store.getTableByName(name)));
    const found = tables.filter((table): table is TableRecord => table !== undefined);
    const tableIds = new Set(found.map((table) => table.id));
    const only = found.length === 1 ? found[0] : undefined;
    const segments =
      only !== undefined
        ? await this.store.listSegments(only.id)
        : (await this.store.listSegments()).filter((segment) => tableIds.has(segment.tableId));
    const transactions = await this.#transactionRecordsForSegments(segments);
    return { manifestVersion, tables, segments, transactions };
  }

  /**
   * Reuses the shared internal reader lease when it targets the requested version and has
   * not expired; otherwise opens a fresh lease at that exact version and retires the old
   * one once its readers drain. Returns undefined when the version's manifest disappeared
   * between the catalog read and the lease, so the caller can re-read.
   */
  async #acquireSharedLease(version: number | null): Promise<SharedLeaseEntry | undefined> {
    const current = this.#sharedLease;
    if (
      current?.version === version &&
      current.lease.expiresAt.getTime() - this.#now().getTime() > 0
    ) {
      current.refCount += 1;
      try {
        await this.#renewInternalLeaseIfNeeded(current.lease);
        return current;
      } catch (error) {
        this.#releaseSharedLease(current);
        throw error;
      }
    }
    let lease: LeasedSnapshot;
    try {
      lease = await this.#transactions.openLeasedSnapshot({
        id: `${this.#internalLeaseOwnerId}/${String(this.#internalLeaseSequence++)}`,
        ownerId: this.#internalLeaseOwnerId,
        ttlMs: INTERNAL_READ_LEASE_TTL_MS,
        version,
      });
    } catch (error) {
      if (error instanceof SnapshotManifestMissingError) return undefined;
      throw error;
    }
    const entry: SharedLeaseEntry = { lease, version, refCount: 1 };
    const previous = this.#sharedLease;
    this.#sharedLease = entry;
    if (previous?.refCount === 0) {
      void previous.lease.release().catch(() => undefined);
    }
    return entry;
  }

  #releaseSharedLease(entry: SharedLeaseEntry): void {
    entry.refCount -= 1;
    if (entry.refCount === 0 && entry !== this.#sharedLease) {
      void entry.lease.release().catch(() => undefined);
    }
  }

  /** Collects every real table referenced by a block, its derived sources, or its subqueries. */
  async #findRealBlockTables(plan: CompiledQuery): Promise<Map<string, TableRecord>> {
    const names = collectRealTableNames(plan);
    const tables = await Promise.all(names.map((name) => this.#findTable(name)));
    return new Map(tables.map((table) => [table.name, table]));
  }

  async #blockSegmentVisibility(
    realTables: ReadonlyMap<string, TableRecord>,
  ): Promise<SegmentVisibilityCatalog> {
    const tableIds = new Set([...realTables.values()].map((table) => table.id));
    const onlyTable = realTables.size === 1 ? [...realTables.values()][0] : undefined;
    const segmentRecords =
      onlyTable !== undefined
        ? await this.store.listSegments(onlyTable.id)
        : await this.store.listSegments();
    const transactionRecords = await this.#transactionRecordsForSegments(segmentRecords);
    const segmentsByTable = new Map<string, SegmentRecord[]>();
    for (const segment of segmentRecords) {
      if (!tableIds.has(segment.tableId)) continue;
      const tableSegments = segmentsByTable.get(segment.tableId) ?? [];
      tableSegments.push(segment);
      segmentsByTable.set(segment.tableId, tableSegments);
    }
    return {
      transactions: new Map(transactionRecords.map((record) => [record.id, record] as const)),
      segmentsByTable,
    };
  }

  /**
   * Builds one block's columnar inputs: derived sources execute first at the same snapshot and
   * become typed in-memory tables under their synthetic names, then the block's real tables
   * materialize their referenced columns. Each block resolves its own inputs, so one real table
   * projected differently by two blocks never collides.
   */
  async #prepareBlockInputs(
    block: CompiledQuery,
    snapshot: LeasedSnapshot,
    visibility: SegmentVisibilityCatalog,
    memory: QueryMemoryContext,
    realTables: ReadonlyMap<string, TableRecord>,
    typedSchemas: Map<string, SqlColumnSchema[]>,
    extraInputs?: ReadonlyMap<string, ColumnarTable>,
  ): Promise<Map<string, ColumnarTable>> {
    const inputs = new Map<string, ColumnarTable>(extraInputs ?? []);
    const sources = [block.base, ...block.joins];
    for (const source of sources) {
      if (inputs.has(source.table)) continue;
      if (source.recursive !== undefined) {
        const { reference, base, step, all } = source.recursive;
        const { result: baseResult, schema } = await this.#executeBlockWithSchemaCached(
          base,
          snapshot,
          visibility,
          memory,
          realTables,
          typedSchemas,
        );
        typedSchemas.set(reference, schema);
        const state = createRecursiveCteState(baseResult, all);
        while (state.frontier.length > 0) {
          // Each iteration's inputs release with its own context, so only the accumulated rows
          // and the final columnar table hold memory across the fixpoint loop.
          const iterationMemory = memory.createChild();
          try {
            const frontierTable = derivedColumnarTable(
              reference,
              { columns: [...state.columns], rows: state.frontier },
              schema,
            );
            state.absorb(
              await this.#executeBlock(
                step,
                snapshot,
                visibility,
                iterationMemory,
                realTables,
                typedSchemas,
                new Map([[reference, frontierTable]]),
              ),
            );
          } finally {
            iterationMemory.close();
          }
        }
        typedSchemas.set(source.table, schema);
        inputs.set(
          source.table,
          derivedColumnarTable(
            source.table,
            { columns: [...state.columns], rows: state.rows },
            schema,
          ),
        );
        continue;
      }
      if (source.union !== undefined) {
        const results: QueryResult[] = [];
        let schema: SqlColumnSchema[] | undefined;
        for (const member of source.union.blocks) {
          const { result, schema: memberSchema } = await this.#executeBlockWithSchemaCached(
            member,
            snapshot,
            visibility,
            memory,
            realTables,
            typedSchemas,
          );
          if (schema === undefined) schema = memberSchema;
          else {
            if (memberSchema.length !== schema.length) {
              throw new TypeError("UNION members must select the same number of columns");
            }
            memberSchema.forEach((column, index) => {
              const expected = schema?.[index];
              if (expected !== undefined && column.type !== expected.type) {
                throw new TypeError(`UNION member column types must match: ${expected.name}`);
              }
            });
          }
          results.push(result);
        }
        const combined = combineUnionResults(results, source.union.ops);
        typedSchemas.set(source.table, schema ?? []);
        inputs.set(source.table, derivedColumnarTable(source.table, combined, schema ?? []));
        continue;
      }
      if (source.windowed !== undefined) {
        const { result: inner, schema: innerSchema } = await this.#executeBlockWithSchemaCached(
          source.windowed.block,
          snapshot,
          visibility,
          memory,
          realTables,
          typedSchemas,
        );
        const windowed = applyWindowFunctions(inner, source.windowed.windows);
        const schema = [
          ...innerSchema,
          ...source.windowed.windows.map((window) => ({
            name: window.alias,
            type: windowOutputType(window, innerSchema),
          })),
        ];
        typedSchemas.set(source.table, schema);
        inputs.set(source.table, derivedColumnarTable(source.table, windowed, schema));
        continue;
      }
      const derived = source.derived;
      if (derived === undefined) continue;
      const { result, schema } = await this.#executeBlockWithSchemaCached(
        derived,
        snapshot,
        visibility,
        memory,
        realTables,
        typedSchemas,
      );
      typedSchemas.set(source.table, schema);
      inputs.set(source.table, derivedColumnarTable(source.table, result, schema));
    }
    const nameSchemas = new Map(
      sources.map((source) => [
        source.table,
        (typedSchemas.get(source.table) ?? []).map(({ name }) => name),
      ]),
    );
    const columns = referencedColumns(block, nameSchemas);
    const onlyRealSource =
      sources.length === 1 && sources[0]?.derived === undefined ? block : undefined;
    for (const source of sources) {
      if (source.derived !== undefined || inputs.has(source.table)) continue;
      if (source.table === DUAL_TABLE) {
        inputs.set(DUAL_TABLE, columnarTableFromRows(DUAL_TABLE, dualTableRows()));
        continue;
      }
      const table = realTables.get(source.table);
      if (table === undefined) throw new TypeError(`Unknown table: ${source.table}`);
      const requestedColumns = columns.get(table.name) ?? [];
      inputs.set(
        table.name,
        await this.#materializeColumnarTableAtSnapshot(
          table,
          snapshot,
          requestedColumns.length === 0 ? [] : resolveReadColumns(table, requestedColumns),
          visibility,
          onlyRealSource,
        ),
      );
    }
    return inputs;
  }

  /** Executes a read-only SELECT statement through the public query API. */
  async query(sql: string, options: QueryOptions = {}): Promise<QueryResult> {
    const spillPageRows =
      options.spillPageRows === undefined
        ? undefined
        : positiveWholeNumber(options.spillPageRows, "Query spill page rows");
    const plan = bindPlanParameters(this.#compileCached(sql), options.params);
    if (this.#canStreamPlanShape(plan, options)) {
      const streamed = await this.#queryStreamed(plan, options, spillPageRows);
      if (streamed !== undefined) return streamed;
    } else {
      // An ORDER-BY-expression wrapper is a pure projection over the real query: stream the
      // inner block and project the hidden ordering column away, so the wrap never costs a
      // query its streaming eligibility.
      const wrapper = transparentProjectionSource(plan);
      if (wrapper !== undefined && this.#canStreamPlanShape(wrapper.inner, options)) {
        const streamed = await this.#queryStreamed(wrapper.inner, options, spillPageRows);
        if (streamed !== undefined) return projectResultColumns(streamed, wrapper.aliases);
      }
    }
    const prepared = await this.#prepareCompiledPlan(plan, options);
    try {
      const spill = options.spillToStorage ?? options.executionMemoryBudgetBytes !== undefined;
      if (!spill) return prepared.execute();
      if (options.spillToStorage !== true) {
        try {
          return prepared.execute();
        } catch (error) {
          if (!(error instanceof QueryMemoryBudgetError)) throw error;
        }
      }
      return await prepared.executeAsync({
        ...(spillPageRows === undefined ? {} : { spillPageRows }),
        spillStore: this.#leasedSpillStore(),
      });
    } finally {
      prepared.close();
    }
  }

  /**
   * Backs the executor's spill pages with durable owner leases: each owner registers a lease
   * before its first page write and renews it while pages are read or written, so an abandoned
   * owner becomes reclaimable only after its lease expires.
   */
  #leasedSpillStore(): QuerySpillStore {
    const leases = new Map<string, { revision: number; expiresAtMs: number }>();
    const ensureLease = async (ownerId: string): Promise<void> => {
      const nowMs = this.#now().getTime();
      const lease = leases.get(ownerId);
      if (lease === undefined) {
        const expiresAtMs = nowMs + this.#spillOwnerLeaseMs;
        await this.store.createTempOwner({
          ownerId,
          expiresAt: new Date(expiresAtMs).toISOString(),
          revision: 0,
        });
        leases.set(ownerId, { revision: 0, expiresAtMs });
        return;
      }
      if (nowMs < lease.expiresAtMs - this.#spillOwnerLeaseMs / 2) return;
      const expiresAtMs = nowMs + this.#spillOwnerLeaseMs;
      const renewed = await this.store.renewTempOwner(
        ownerId,
        lease.revision,
        new Date(expiresAtMs).toISOString(),
      );
      leases.set(ownerId, { revision: renewed.revision, expiresAtMs });
    };
    return {
      putPage: async (ownerId, runId, pageIndex, bytes) => {
        await ensureLease(ownerId);
        await this.store.putTempRunPage({ ownerId, runId, pageIndex, bytes });
      },
      getPage: async (ownerId, runId, pageIndex) => {
        await ensureLease(ownerId);
        return this.store.getTempRunPage(ownerId, runId, pageIndex);
      },
      removeRun: (ownerId, runId) => this.store.removeTempRun(ownerId, runId),
      removeOwner: async (ownerId) => {
        await this.store.removeTempOwner(ownerId);
        leases.delete(ownerId);
      },
    };
  }

  /**
   * Reclaims temp spill pages whose owner lease is expired or missing at a cutoff fixed when the
   * pass starts. Owners with an unexpired lease are retained; each owner is removed atomically
   * against a concurrent renewal.
   */
  async cleanupQuerySpill(
    options: QuerySpillCleanupOptions = {},
  ): Promise<QuerySpillCleanupResult> {
    const maxOwners =
      options.maxOwners === undefined
        ? Number.MAX_SAFE_INTEGER
        : positiveWholeNumber(options.maxOwners, "Spill cleanup owner limit");
    const cutoff = this.#now().toISOString();
    const result: QuerySpillCleanupResult = {
      ownersExamined: 0,
      ownersReclaimed: 0,
      ownersRetained: 0,
    };
    let afterOwnerId: string | null = null;
    while (result.ownersExamined < maxOwners) {
      const page = await this.store.listTempOwnerIdsPage(
        afterOwnerId,
        Math.min(64, maxOwners - result.ownersExamined),
      );
      for (const ownerId of page.records) {
        result.ownersExamined += 1;
        if (await this.store.removeTempOwnerIfExpired(ownerId, cutoff)) {
          result.ownersReclaimed += 1;
        } else {
          result.ownersRetained += 1;
        }
      }
      if (page.nextCursor === null) break;
      afterOwnerId = page.nextCursor;
    }
    return result;
  }

  /**
   * Creates a live-query set over this database. Local commits hint it directly, an optional
   * channel carries cross-tab hints, and an optional poll interval bounds staleness without any
   * hint; every hint path converges on the durable manifest version, so missed messages delay a
   * refresh but never produce a stale result. Subscriptions retain a result digest, not rows.
   */
  liveQueries(options: LiveQuerySetOptions = {}): LiveQuerySet {
    const set = new LiveQuerySet(
      {
        currentVersion: () => this.store.getCurrentManifestVersion(),
        manifestPage: (afterVersion, limit) => this.store.listManifestPage(afterVersion, limit),
        dependencyTableIds: async (query) => {
          const plan = typeof query === "string" ? this.#compileCached(query) : query.plan;
          const tables = await this.#findRealBlockTables(plan);
          return new Set([...tables.values()].map((record) => record.id));
        },
        execute: async (query) => {
          if (typeof query === "string") return this.query(query);
          const prepared = await this.#prepareCompiledPlan(query.plan);
          try {
            return prepared.execute();
          } finally {
            prepared.close();
          }
        },
      },
      {
        ...options,
        onClosed: () => {
          this.#liveSets.delete(set);
          options.onClosed?.();
        },
      },
    );
    this.#liveSets.add(set);
    return set;
  }

  #notifyLiveCommit(): void {
    for (const set of this.#liveSets) set.notifyLocalCommit();
  }

  /** Executes a built ORM query through the same preparation pipeline as compiled SQL. */
  async run<TRow>(query: {
    kind: "typed-query";
    plan: CompiledQuery;
    __row?: TRow;
  }): Promise<TRow[]> {
    const prepared = await this.#prepareCompiledPlan(query.plan);
    try {
      return prepared.execute().rows as TRow[];
    } finally {
      prepared.close();
    }
  }

  /**
   * Applies a schema definition to the catalog through metadata-only steps: creating missing
   * tables, adding nullable columns, renaming columns via their stable IDs, and widening
   * nullability. The pass is idempotent — re-running after a crash completes the remaining
   * steps — and every catalog alteration is one atomic compare-and-swap, so a concurrent
   * migrator fails explicitly with a conflict instead of interleaving.
   */
  async migrate(
    definition: SchemaDefinition<readonly AnyTable[]>,
  ): Promise<{ createdTables: string[]; alteredTables: string[]; steps: MigrationStep[] }> {
    // Table revisions also move under background full-text index activity (build stamps,
    // stale-writer invalidation), so a lost compare-and-swap no longer implies a concurrent
    // migrator. Migration is idempotent by construction — re-plan from fresh records and retry;
    // a genuinely concurrent migrator still fails explicitly once the retries are exhausted.
    for (let attempt = 0; ; attempt += 1) {
      try {
        return await this.#migrateOnce(definition);
      } catch (error) {
        if (!(error instanceof TableRecordConflictError) || attempt >= 2) throw error;
      }
    }
  }

  async #migrateOnce(
    definition: SchemaDefinition<readonly AnyTable[]>,
  ): Promise<{ createdTables: string[]; alteredTables: string[]; steps: MigrationStep[] }> {
    // One listTables pass drives planning and execution: a create step exists only because the
    // table was absent from this snapshot, and each altered table applies all of its steps in a
    // single compare-and-swap, so a migration costs one catalog write per changed table however
    // many tables or steps the schema carries. A concurrent creator or migrator still fails
    // explicitly through createTable's uniqueness check or the revision conflict.
    const records = await this.store.listTables();
    const recordsByName = new Map(records.map((record) => [record.name, record]));
    const plan = planMigration(records, definition);
    const createdTables: string[] = [];
    const alterationsByTable = new Map<string, MigrationStep[]>();
    for (const step of plan.steps) {
      if (step.kind === "create-table") {
        const entries = Object.entries(step.table.columns);
        const uniqueEntry = entries.find(([, columnDefinition]) => columnDefinition.isUnique);
        await this.createTable({
          name: step.table.name,
          ...(uniqueEntry === undefined ? {} : { uniqueKey: uniqueEntry[0] }),
          columns: entries.map(([name, columnDefinition]) => ({
            name,
            type: columnDefinition.type,
            ...(columnDefinition.isNullable ? { nullable: true } : {}),
            ...(columnDefinition.defaultSpec === undefined
              ? {}
              : { defaultValue: columnDefinition.defaultSpec }),
            ...(columnDefinition.enumValues === undefined
              ? {}
              : { enumValues: columnDefinition.enumValues }),
          })),
        });
        createdTables.push(step.table.name);
        continue;
      }
      const steps = alterationsByTable.get(step.tableName) ?? [];
      steps.push(step);
      alterationsByTable.set(step.tableName, steps);
    }
    const alteredTables: string[] = [];
    for (const [tableName, steps] of alterationsByTable) {
      const record = recordsByName.get(tableName);
      if (record === undefined) {
        throw new Error(`Migration target table is missing: ${tableName}`);
      }
      const columns = applyColumnSteps(record, steps, this.#createId);
      if (JSON.stringify(columns) === JSON.stringify(record.columns)) continue;
      await this.store.updateTable(record.id, record.revision ?? 0, { columns });
      alteredTables.push(tableName);
    }
    return { createdTables, alteredTables, steps: plan.steps };
  }

  /**
   * Renders the optimized logical plan for a SELECT statement plus the physical strategy notes
   * the prepared execution would choose, without executing it.
   */
  async explain(sql: string): Promise<string> {
    let plan = this.#compileCached(sql);
    // Explain reports on the plan the engine would execute, so MATCH(*)/BM25(*) expand against
    // the catalog exactly as preparation does (copy-on-write — the cache keeps "*").
    if (planContainsFts(plan)) {
      const realTables = await this.#findRealBlockTables(plan);
      plan = expandFtsColumns(plan, (tableName) => searchableFtsColumns(realTables.get(tableName)));
    }
    // The ORDER-BY-expression desugar's transparent wrapper executes as its inner block plus a
    // projection, so every execution-path note reports against the inner block.
    const reported = transparentProjectionSource(plan)?.inner ?? plan;
    const notes: string[] = [];
    if (this.#canStreamPlanShape(reported, { executionMemoryBudgetBytes: 1 })) {
      notes.push("eligible to stream the base scan through resident block windows under a budget");
    } else {
      notes.push("materializes inputs at preparation");
    }
    const table = reported.base.derived ?? reported.base.union ?? reported.base.windowed;
    if (table === undefined && reported.joins.length === 0) {
      const record = await this.#findTable(reported.base.table);
      if (zonePredicates(reported, record).length > 0) {
        notes.push("zone-map pruning applies to the unbudgeted scan");
      }
    }
    if (blockHasSubqueries(reported))
      notes.push("resolves uncorrelated subqueries at one snapshot");
    if (planContainsFts(reported)) {
      notes.push("full-text MATCH evaluates via per-dictionary term tables on the scan");
      if (table === undefined && reported.joins.length === 0) {
        const record = await this.#findTable(reported.base.table);
        const readyColumnIds = new Set(
          Object.entries(record.ftsColumns ?? {})
            .filter(
              ([, state]) =>
                state.state === "ready" && state.tokenizerVersion === FTS_TOKENIZER_VERSION,
            )
            .map(([columnId]) => columnId),
        );
        const columnsByName = new Map(
          record.columns.map((column) => [column.name, column] as const),
        );
        const scoringServed = [...this.#ftsBm25Nodes(reported).values()].every((node) =>
          node.columns.every((expression) => {
            if (expression.kind !== "column") return false;
            const column = columnsByName.get(
              expression.reference.split(".").at(-1) ?? expression.reference,
            );
            return column !== undefined && readyColumnIds.has(column.id);
          }),
        );
        const scoring = planContainsFts(reported, "bm25");
        if (readyColumnIds.size > 0 && (!scoring || scoringServed)) {
          notes.push("a ready full-text index prunes the base scan to candidate segments");
        }
        if (scoring) {
          notes.push(
            scoringServed
              ? "BM25 statistics come from the full-text index"
              : "BM25 scoring reads the full scan for corpus statistics; index pruning does not apply",
          );
        }
      } else if (planContainsFts(reported, "bm25")) {
        notes.push(
          "BM25 scoring reads the full scan for corpus statistics; index pruning does not apply",
        );
      }
    }
    return `${renderPlan(plan)}\n${notes.map((note) => `-- ${note}`).join("\n")}`;
  }

  /**
   * Executes one SQL statement. SELECT statements run through the read-only query pipeline;
   * INSERT ... VALUES maps onto a column batch insert, and UPDATE/DELETE on a unique-key table
   * read the matching keys at one snapshot and then apply the keyed mutation. The read and the
   * mutation are two steps, not one serializable transaction: a key changed by a competing writer
   * in between fails the statement explicitly rather than silently mutating other rows.
   */
  async execute(sql: string, params?: readonly QueryValue[]): Promise<ExecuteResult> {
    const statement = compileStatement(sql);
    if (statement.kind === "select") {
      return {
        kind: "rows",
        result: await this.query(statement.sql, params === undefined ? {} : { params }),
      };
    }
    return this.runStatement(bindStatementParameters(statement, params));
  }

  /**
   * ON CONFLICT (key) DO UPDATE SET with a column subset: rows whose key exists merge only the
   * assigned EXCLUDED columns through the keyed update path, and the rest insert. The existence
   * read and the writes are separate steps at one snapshot, like every SQL mutation here.
   */
  async #mergeConflictingInsertRows(
    statement: Extract<CompiledStatement, { kind: "insert" }>,
    options: RunStatementOptions,
  ): Promise<ExecuteResult> {
    const table = await this.#findTable(statement.table);
    const keyColumn = getUniqueKeyColumn(table);
    if (keyColumn === undefined || statement.onConflict?.column !== keyColumn.name) {
      throw new TypeError(
        `ON CONFLICT targets the table's unique key column: ${keyColumn?.name ?? "(none)"}`,
      );
    }
    const keyIndex = statement.columns.indexOf(keyColumn.name);
    if (keyIndex === -1) {
      // Without the key in the insert list no row can conflict; run as a plain insert.
      const { onConflict, ...plain } = statement;
      void onConflict;
      return this.runStatement(plain, options);
    }
    const assigned = statement.onConflict.columns ?? [];
    const keyToken = (value: QueryValue): string =>
      value instanceof Date ? `d${value.toISOString()}` : `${typeof value} ${String(value)}`;
    const existing = await this.#existingInsertKeys(table, keyColumn, statement);
    const freshRows: InsertValue[][] = [];
    const conflictingRows: InsertValue[][] = [];
    for (const row of statement.rows) {
      const key = boundInsertValue(row[keyIndex] ?? null);
      if (key !== null && existing.has(keyToken(key))) conflictingRows.push(row);
      else freshRows.push(row);
    }
    let updateVersion: number | undefined;
    if (conflictingRows.length > 0) {
      const changes: Record<string, BatchValue[]> = {};
      for (const name of assigned) {
        const columnIndex = statement.columns.indexOf(name);
        changes[name] = conflictingRows.map((row) => boundInsertValue(row[columnIndex] ?? null));
      }
      const keys = conflictingRows.map((row) => {
        const value = boundInsertValue(row[keyIndex] ?? null);
        if (value === null)
          throw new TypeError(`Unique key values must not be null: ${keyColumn.name}`);
        return value;
      });
      const result = await this.updateBatch(table.name, { keys, changes });
      updateVersion = result.version;
    }
    let insertResult: ExecuteResult | undefined;
    if (freshRows.length > 0) {
      const { onConflict, ...plain } = statement;
      void onConflict;
      insertResult = await this.runStatement({ ...plain, rows: freshRows }, options);
    }
    const insertVersion = insertResult?.kind === "insert" ? insertResult.version : undefined;
    const returning =
      options.returning === undefined
        ? undefined
        : await this.#mergedUpsertReturnedRows(
            table,
            keyColumn,
            statement,
            conflictingRows,
            insertResult,
            options.returning,
          );
    const version = insertVersion ?? updateVersion;
    return {
      kind: "insert",
      table: statement.table,
      rowCount: statement.rows.length,
      ...(version === undefined ? {} : { version }),
      ...(returning === undefined ? {} : { returnedRows: returning }),
    };
  }

  /** Post-merge RETURNING: updated rows read at the snapshot with assignments applied. */
  async #mergedUpsertReturnedRows(
    table: TableRecord,
    keyColumn: TableRecord["columns"][number],
    statement: Extract<CompiledStatement, { kind: "insert" }>,
    conflictingRows: readonly InsertValue[][],
    insertResult: ExecuteResult | undefined,
    returning: readonly string[] | "*",
  ): Promise<QueryRow[]> {
    const names = returning === "*" ? table.columns.map(({ name }) => name) : [...returning];
    for (const name of names) {
      if (!table.columns.some((column) => column.name === name)) {
        throw new TypeError(`RETURNING column does not exist: ${name}`);
      }
    }
    const keyIndex = statement.columns.indexOf(keyColumn.name);
    const assigned = new Set(statement.onConflict?.columns ?? []);
    const updatedByKey = new Map<QueryValue | string, InsertValue[]>();
    for (const row of conflictingRows) {
      const key = boundInsertValue(row[keyIndex] ?? null);
      updatedByKey.set(key instanceof Date ? key.toISOString() : key, row);
    }
    // One snapshot read serves every updated row's unassigned columns.
    const rowsByKey = new Map<QueryValue | string, QueryRow>();
    if (conflictingRows.length > 0) {
      const plan: CompiledQuery = {
        sql: "(on conflict returning)",
        base: { table: table.name, alias: table.name },
        joins: [],
        select: [...new Set([keyColumn.name, ...names])].map((name) => ({
          expression: { kind: "column", reference: name },
          alias: name,
        })),
        predicates: [
          {
            left: { kind: "column", reference: keyColumn.name },
            operator: "IN",
            right: {
              kind: "list",
              items: [...updatedByKey.keys()].map((value) => ({
                kind: "literal",
                value: typeof value === "string" || value === null ? value : (value as QueryValue),
              })),
            },
          },
        ],
        groupBy: [],
        having: [],
        orderBy: [],
      };
      const prepared = await this.#prepareCompiledPlan(plan);
      try {
        for (const row of prepared.execute().rows) {
          const key = row[keyColumn.name] ?? null;
          rowsByKey.set(key instanceof Date ? key.toISOString() : key, row);
        }
      } finally {
        prepared.close();
      }
    }
    const insertedEcho =
      insertResult?.kind === "insert" ? [...(insertResult.returnedRows ?? [])] : [];
    const output: QueryRow[] = [];
    for (const row of statement.rows) {
      const key = boundInsertValue(row[keyIndex] ?? null);
      const token = key instanceof Date ? key.toISOString() : key;
      const updated = updatedByKey.get(token);
      if (updated === undefined) {
        const echoed = insertedEcho.shift();
        if (echoed !== undefined) output.push(echoed);
        continue;
      }
      const snapshot = rowsByKey.get(token) ?? {};
      output.push(
        Object.fromEntries(
          names.map((name) => {
            if (assigned.has(name)) {
              const columnIndex = statement.columns.indexOf(name);
              return [name, boundInsertValue(updated[columnIndex] ?? null)];
            }
            if (name === keyColumn.name) return [name, key];
            return [name, snapshot[name] ?? null];
          }),
        ),
      );
    }
    return output;
  }

  /** Existing unique keys among an insert statement's rows, read at one snapshot. */
  async #existingInsertKeys(
    table: TableRecord,
    keyColumn: TableRecord["columns"][number],
    statement: Extract<CompiledStatement, { kind: "insert" }>,
  ): Promise<Set<string>> {
    const keyIndex = statement.columns.indexOf(keyColumn.name);
    const keys = statement.rows
      .map((row) => boundInsertValue(row[keyIndex] ?? null))
      .filter((value): value is Exclude<QueryValue, null> => value !== null);
    if (keys.length === 0) return new Set();
    const keyToken = (value: QueryValue): string =>
      value instanceof Date ? `d${value.toISOString()}` : `${typeof value} ${String(value)}`;
    const plan: CompiledQuery = {
      sql: "(on conflict existence)",
      base: { table: table.name, alias: table.name },
      joins: [],
      select: [{ expression: { kind: "column", reference: keyColumn.name }, alias: "key" }],
      predicates: [
        {
          left: { kind: "column", reference: keyColumn.name },
          operator: "IN",
          right: { kind: "list", items: keys.map((value) => ({ kind: "literal", value })) },
        },
      ],
      groupBy: [],
      having: [],
      orderBy: [],
    };
    const prepared = await this.#prepareCompiledPlan(plan);
    try {
      return new Set(prepared.execute().rows.map((row) => keyToken(row.key ?? null)));
    } finally {
      prepared.close();
    }
  }

  /** ON CONFLICT DO NOTHING: drops insert rows whose unique key already exists at a snapshot. */
  async #filterConflictingInsertRows(
    statement: Extract<CompiledStatement, { kind: "insert" }>,
  ): Promise<Extract<CompiledStatement, { kind: "insert" }>> {
    const table = await this.#findTable(statement.table);
    const keyColumn = getUniqueKeyColumn(table);
    if (keyColumn === undefined || statement.onConflict?.column !== keyColumn.name) {
      throw new TypeError(
        `ON CONFLICT targets the table's unique key column: ${keyColumn?.name ?? "(none)"}`,
      );
    }
    const keyIndex = statement.columns.indexOf(keyColumn.name);
    if (keyIndex === -1) return statement;
    const keys = statement.rows.map((row) => boundInsertValue(row[keyIndex] ?? null));
    const plan: CompiledQuery = {
      sql: "(on conflict do nothing)",
      base: { table: table.name, alias: table.name },
      joins: [],
      select: [{ expression: { kind: "column", reference: keyColumn.name }, alias: "key" }],
      predicates: [
        {
          left: { kind: "column", reference: keyColumn.name },
          operator: "IN",
          right: { kind: "list", items: keys.map((value) => ({ kind: "literal", value })) },
        },
      ],
      groupBy: [],
      having: [],
      orderBy: [],
    };
    const prepared = await this.#prepareCompiledPlan(plan);
    let existing: Set<QueryValue | string>;
    try {
      existing = new Set(
        prepared.execute().rows.map((row) => {
          const value = row.key ?? null;
          return value instanceof Date ? value.toISOString() : value;
        }),
      );
    } finally {
      prepared.close();
    }
    return {
      ...statement,
      rows: statement.rows.filter((row) => {
        const value = boundInsertValue(row[keyIndex] ?? null);
        return !existing.has(value instanceof Date ? value.toISOString() : value);
      }),
    };
  }

  /** Runs an INSERT ... SELECT source at one snapshot and materializes it as literal rows. */
  async #materializeInsertSelect(
    statement: Extract<CompiledStatement, { kind: "insert" }>,
  ): Promise<Extract<CompiledStatement, { kind: "insert" }>> {
    if (statement.query === undefined) return statement;
    const prepared = await this.#prepareCompiledPlan(statement.query);
    let result: QueryResult;
    try {
      result = prepared.execute();
    } finally {
      prepared.close();
    }
    const { query, ...rest } = statement;
    void query;
    return {
      ...rest,
      rows: result.rows.map((row) => result.columns.map((column) => row[column] ?? null)),
    };
  }

  /**
   * Executes an already-compiled statement — the typed mutation builders call this directly with
   * the same statement structures SQL parses into, sharing every validation and conflict rule.
   * With `returning`, the affected rows project back: inserts echo the written values, deletes
   * return the rows as read at the statement's snapshot, and updates return post-update values
   * (the snapshot row with assignments applied — the exact values the mutation wrote).
   */
  async runStatement(
    statement: CompiledStatement,
    options: RunStatementOptions = {},
  ): Promise<ExecuteResult> {
    if (statement.kind === "select") {
      return { kind: "rows", result: await this.query(statement.sql) };
    }
    if (statement.kind === "create-table") {
      await this.createTable({
        name: statement.table,
        columns: statement.columns,
        ...(statement.uniqueKey === undefined ? {} : { uniqueKey: statement.uniqueKey }),
      });
      return { kind: "create-table", table: statement.table };
    }
    // A RETURNING clause parsed from SQL text applies unless the caller overrides it.
    if (statement.returning !== undefined && options.returning === undefined) {
      options = { ...options, returning: statement.returning };
    }
    if (statement.kind === "insert" && statement.query !== undefined) {
      statement = await this.#materializeInsertSelect(statement);
    }
    if (statement.kind === "insert" && statement.onConflict?.action === "nothing") {
      statement = await this.#filterConflictingInsertRows(statement);
    }
    if (statement.kind === "insert" && statement.onConflict?.action === "update") {
      return this.#mergeConflictingInsertRows(statement, options);
    }
    if (statement.kind === "insert" && statement.rows.length === 0) {
      return {
        kind: "insert",
        table: statement.table,
        rowCount: 0,
        ...(options.returning === undefined ? {} : { returnedRows: [] }),
      };
    }
    if (statement.kind === "insert") {
      const table = await this.#findTable(statement.table);
      const viaUpsert = statement.onConflict?.action === "replace";
      if (viaUpsert) {
        const keyColumn = getUniqueKeyColumn(table);
        if (keyColumn === undefined || statement.onConflict?.column !== keyColumn.name) {
          throw new TypeError(
            `ON CONFLICT targets the table's unique key column: ${keyColumn?.name ?? "(none)"}`,
          );
        }
      }
      for (const name of statement.columns) {
        if (!table.columns.some((column) => column.name === name)) {
          throw new TypeError(`INSERT column does not exist: ${name}`);
        }
      }
      const columns: Record<string, BatchValue[]> = {};
      statement.columns.forEach((column, index) => {
        columns[column] = statement.rows.map((row) => boundInsertValue(row[index] ?? null));
      });
      // Unlisted columns pad with NULL: defaults fill their null slots engine-side, nullable
      // columns store NULL, and a non-nullable column without a default rejects the batch.
      for (const column of table.columns) {
        if (!(column.name in columns)) {
          columns[column.name] = statement.rows.map(() => null);
        }
      }
      const result = viaUpsert
        ? await this.upsertBatch(statement.table, { columns })
        : await this.insertBatch(statement.table, { columns });
      let returningColumns: readonly string[] | undefined;
      if (options.returning !== undefined) {
        returningColumns =
          options.returning === "*"
            ? table.columns.map((column) => column.name)
            : options.returning;
        for (const name of returningColumns) {
          if (!table.columns.some((column) => column.name === name)) {
            throw new TypeError(`RETURNING column does not exist: ${name}`);
          }
        }
      }
      const generated = result.generatedColumns ?? {};
      return {
        kind: "insert",
        table: statement.table,
        rowCount: result.rowCount,
        version: result.version,
        ...(returningColumns === undefined
          ? {}
          : {
              returnedRows: statement.rows.map((row, rowIndex) =>
                Object.fromEntries(
                  returningColumns.map((name) => [
                    name,
                    generated[name]?.[rowIndex] ??
                      boundInsertValue(row[statement.columns.indexOf(name)] ?? null),
                  ]),
                ),
              ),
            }),
      };
    }
    const table = await this.#findTable(statement.table);
    const keyColumn = getUniqueKeyColumn(table);
    if (keyColumn === undefined) {
      throw new TypeError(
        `${statement.kind === "update" ? "UPDATE" : "DELETE"} requires a table with a unique key: ${statement.table}`,
      );
    }
    const returningColumns =
      options.returning === undefined
        ? undefined
        : options.returning === "*"
          ? table.columns.map(({ name }) => name)
          : [...options.returning];
    const referenced = new Set([keyColumn.name, ...(returningColumns ?? [])]);
    if (statement.kind === "update") {
      for (const assignment of statement.assignments) {
        for (const column of expressionColumnNames(assignment.expression)) {
          referenced.add(column.split(".").at(-1) ?? column);
        }
      }
    }
    const plan: CompiledQuery = {
      sql: `(${statement.kind})`,
      base: { table: table.name, alias: table.name },
      joins: [],
      select: [...referenced].map((name) => ({
        expression: { kind: "column", reference: name },
        alias: name,
      })),
      predicates: statement.predicates,
      groupBy: [],
      having: [],
      orderBy: [],
    };
    const prepared = await this.#prepareCompiledPlan(plan);
    let rows: QueryResult["rows"];
    try {
      rows = prepared.execute().rows;
    } finally {
      prepared.close();
    }
    const keys = rows.map((row) => row[keyColumn.name]);
    if (keys.some((key) => key === null || key === undefined)) {
      throw new TypeError(`Unique key values must not be null: ${keyColumn.name}`);
    }
    if (statement.kind === "delete") {
      const returnedRows =
        returningColumns === undefined
          ? undefined
          : rows.map((row) =>
              Object.fromEntries(returningColumns.map((name) => [name, row[name] ?? null])),
            );
      if (keys.length === 0) {
        return {
          kind: "delete",
          table: table.name,
          rowCount: 0,
          ...(returnedRows === undefined ? {} : { returnedRows }),
        };
      }
      const result = await this.deleteBatch(table.name, { keys: keys as BatchValue[] });
      return {
        kind: "delete",
        table: table.name,
        rowCount: result.deletedRowCount,
        version: result.version,
        ...(returnedRows === undefined ? {} : { returnedRows }),
      };
    }
    if (keys.length === 0) {
      return {
        kind: "update",
        table: table.name,
        rowCount: 0,
        ...(returningColumns === undefined ? {} : { returnedRows: [] }),
      };
    }
    const changes: Record<string, Array<BatchValue | null>> = {};
    for (const assignment of statement.assignments) {
      changes[assignment.column] = rows.map((row) => {
        const value = evaluateRowExpression(assignment.expression, table.name, row);
        if (typeof value === "number" && !Number.isFinite(value)) {
          throw new TypeError(
            `UPDATE assignment produced a non-finite number: ${assignment.column}`,
          );
        }
        return value;
      });
    }
    const result = await this.updateBatch(table.name, {
      keys: keys as BatchValue[],
      changes,
    });
    const returnedRows =
      returningColumns === undefined
        ? undefined
        : rows.map((row, index) =>
            Object.fromEntries(
              returningColumns.map((name) => [
                name,
                name in changes ? (changes[name]?.[index] ?? null) : (row[name] ?? null),
              ]),
            ),
          );
    return {
      kind: "update",
      table: table.name,
      rowCount: result.updatedRowCount,
      version: result.version,
      ...(returnedRows === undefined ? {} : { returnedRows }),
    };
  }

  /**
   * A plan shape can stream its scan input: every asynchronous execution path, including both
   * spill paths, consumes scan rows batch-forward and copies values out, so a sliding resident
   * window can back the scan source while join build sides stay fully materialized. A self-join
   * is excluded because the base table would also be probed as a build side at arbitrary rows.
   * Streaming exists to honor an explicit budget; an unbudgeted or spill-disabled query keeps the
   * materialized path.
   */
  #canStreamPlanShape(plan: CompiledQuery, options: QueryOptions): boolean {
    return (
      options.executionMemoryBudgetBytes !== undefined &&
      options.spillToStorage !== false &&
      plan.base.table !== DUAL_TABLE &&
      plan.base.derived === undefined &&
      plan.base.union === undefined &&
      plan.base.windowed === undefined &&
      plan.base.recursive === undefined &&
      plan.joins.every((join) => join.derived === undefined && join.recursive === undefined) &&
      !plan.joins.some((join) => join.table === plan.base.table) &&
      !blockHasSubqueries(plan)
    );
  }

  /**
   * Executes a plan with a sliding block-aligned scan window over the base table instead of a
   * fully materialized scan input; join build sides are materialized at the same snapshot.
   * Returns undefined when the base table's visible shape is ineligible (keyed mutation replay),
   * so the caller falls back to the materialized path.
   */
  async #queryStreamed(
    plan: CompiledQuery,
    options: QueryOptions,
    spillPageRows: number | undefined,
  ): Promise<QueryResult | undefined> {
    const tableNames = [plan.base.table, ...plan.joins.map((join) => join.table)];
    const uniqueTableNames = [...new Set(tableNames)];
    const tables = await Promise.all(uniqueTableNames.map((name) => this.#findTable(name)));
    // Copy-on-write: expansion clones only full-text plans, leaving the compile cache's copy
    // untouched.
    plan = expandFtsColumns(plan, (tableName) =>
      searchableFtsColumns(tables.find((table) => table.name === tableName)),
    );
    const schemas = new Map(
      tables.map((table) => [table.name, table.columns.map(({ name }) => name)]),
    );
    const columns = referencedColumns(plan, schemas);
    const baseTable = tables.find((table) => table.name === plan.base.table);
    if (baseTable === undefined) throw new TypeError(`Unknown table: ${plan.base.table}`);
    const requestedBaseColumns = columns.get(baseTable.name) ?? [];
    const projectedBaseColumns =
      requestedBaseColumns.length === 0 ? [] : resolveReadColumns(baseTable, requestedBaseColumns);
    return this.#withLeasedSnapshot(options.version, async (snapshot) => {
      const tableIds = new Set(tables.map((table) => table.id));
      const segmentRecords =
        tables.length === 1
          ? await this.store.listSegments(baseTable.id)
          : await this.store.listSegments();
      const transactionRecords = await this.#transactionRecordsForSegments(segmentRecords);
      const segmentsByTable = new Map<string, SegmentRecord[]>();
      for (const segment of segmentRecords) {
        if (!tableIds.has(segment.tableId)) continue;
        const tableSegments = segmentsByTable.get(segment.tableId) ?? [];
        tableSegments.push(segment);
        segmentsByTable.set(segment.tableId, tableSegments);
      }
      const visibility: SegmentVisibilityCatalog = {
        transactions: new Map(transactionRecords.map((record) => [record.id, record] as const)),
        segmentsByTable,
      };
      // The fts invalidation flip commits atomically with its segments, so a record fetched
      // after the segment reads above cannot claim "ready" for data those segments miss.
      const freshBaseTable = planContainsFts(plan)
        ? ((await this.store.getTable(baseTable.id)) ?? baseTable)
        : baseTable;
      // Scoring needs corpus statistics: streamed windows cannot run the scan-side stats pass,
      // so a BM25 plan streams only when the index serves exact statistics; otherwise fall
      // back to the materialized path.
      const ftsStats = await this.#ftsIndexStats(
        plan,
        new Map(
          tables.map((record) => [
            record.name,
            record.id === freshBaseTable.id ? freshBaseTable : record,
          ]),
        ),
        snapshot,
        visibility,
      );
      if (planContainsFts(plan, "bm25") && ftsStats === undefined) return undefined;
      // The streamed scan honors the same index pruning as the materialized path; a budgeted
      // MATCH query must not silently pay the full scan just because it streams.
      const baseSegments = await this.#ftsPrunedSegments(
        freshBaseTable,
        await this.#visibleSegmentRecords(freshBaseTable, snapshot, visibility),
        plan,
        snapshot,
      );
      const baseView = this.#streamedViewFactory(
        baseTable,
        projectedBaseColumns,
        baseSegments,
        snapshot,
      );
      if (baseView === undefined) return undefined;

      // A single unordered/ungrouped inner equi-join whose build side is too big to materialize
      // executes as a partitioned hash join: each pass keeps one hash partition of the build
      // rows resident and re-streams both inputs, so memory stays bounded by partition size.
      const partitionedShape = partitionedJoinShape(plan);
      if (partitionedShape !== undefined && options.executionMemoryBudgetBytes !== undefined) {
        const buildTable = tables.find((table) => table.name === partitionedShape.buildTableName);
        if (buildTable !== undefined) {
          const requestedBuildColumns = columns.get(buildTable.name) ?? [];
          const projectedBuildColumns =
            requestedBuildColumns.length === 0
              ? []
              : resolveReadColumns(buildTable, requestedBuildColumns);
          if (
            projectedBuildColumns.some((column) => column.name === partitionedShape.buildKeyName)
          ) {
            const buildSegments = await this.#visibleSegmentRecords(
              buildTable,
              snapshot,
              visibility,
            );
            const buildView = this.#streamedViewFactory(
              buildTable,
              projectedBuildColumns,
              buildSegments,
              snapshot,
            );
            const budget = options.executionMemoryBudgetBytes;
            const estimate = estimatedColumnarBytes(buildSegments, projectedBuildColumns);
            if (buildView !== undefined && estimate > budget / 4) {
              return this.#runPartitionedJoin(
                plan,
                budget,
                estimate,
                baseView,
                buildView,
                buildTable.name,
                partitionedShape.buildKeyName,
              );
            }
          }
        }
      }

      const runStreamedExecution = async (attempt: {
        spill: boolean;
      }): Promise<QueryResult | undefined> => {
        const memory = new QueryMemoryContext(options.executionMemoryBudgetBytes);
        let prepared: PreparedQuery | undefined;
        try {
          const streamed = await baseView.create(memory);
          const inputTables = new Map<string, ColumnarTable>([[baseTable.name, streamed.table]]);
          for (const table of tables) {
            if (table.name === baseTable.name) continue;
            const requestedColumns = columns.get(table.name) ?? [];
            inputTables.set(
              table.name,
              await this.#materializeColumnarTableAtSnapshot(
                table,
                snapshot,
                requestedColumns.length === 0 ? [] : resolveReadColumns(table, requestedColumns),
                visibility,
              ),
            );
          }
          prepared = createPreparedColumnarQuery(
            plan,
            inputTables,
            memory,
            ftsStats === undefined ? {} : { ftsStats },
          );
          if (!attempt.spill) {
            try {
              return await prepared.executeAsync({ loadScanWindow: streamed.load });
            } catch (error) {
              if (error instanceof QueryMemoryBudgetError) return undefined;
              throw error;
            }
          }
          return await prepared.executeAsync({
            ...(spillPageRows === undefined ? {} : { spillPageRows }),
            spillStore: this.#leasedSpillStore(),
            loadScanWindow: streamed.load,
          });
        } finally {
          if (prepared === undefined) memory.close();
          else prepared.close();
        }
      };
      // Retained state is usually far smaller than the scan: a limited ORDER BY keeps only its
      // top rows and a grouped plan keeps one state per distinct group. Try the in-memory
      // execution before committing to a spill of every scanned row; only a genuine budget
      // overflow falls through. The streamed loader is forward-only, so the fallback rebuilds
      // the stream rather than rewinding it.
      const boundedStateShape =
        compiledPlanIsGrouped(plan) || (plan.limit !== undefined && plan.orderBy.length > 0);
      if (boundedStateShape) {
        const bounded = await runStreamedExecution({ spill: false });
        if (bounded !== undefined) return bounded;
      }
      return runStreamedExecution({ spill: true });
    });
  }

  /**
   * Checks a table's visible history for streamed-scan eligibility and returns a factory that
   * builds a fresh forward-only streamed view per attempt or pass. Append/base histories stream
   * directly; update/delete keyed histories stream through the mutation replay; histories with
   * upsert segments (which interleave new rows into slot order at their segment position) and
   * shapes whose blocks cannot back a block-driven loader return undefined so the caller keeps
   * the materialized path.
   */
  #streamedViewFactory(
    table: TableRecord,
    projectedColumns: readonly TableColumnRecord[],
    segments: readonly SegmentRecord[],
    snapshot: LeasedSnapshot,
  ):
    | {
        create: (memory: QueryMemoryContext) => Promise<{
          table: ColumnarTable;
          load: (start: number, length: number) => Promise<void>;
        }>;
      }
    | undefined {
    const scanSegments = segments.filter((segment) => {
      const kind = segment.kind ?? "insert";
      return kind === "insert" || kind === "base";
    });
    const mutationSegments = segments.filter(
      (segment) => segment.kind === "update" || segment.kind === "delete",
    );
    if (scanSegments.length + mutationSegments.length !== segments.length) return undefined;
    const keyColumn = getUniqueKeyColumn(table);
    if (mutationSegments.length > 0) {
      if (keyColumn === undefined) return undefined;
      // The key replay is block-driven over every segment, so the key column must have blocks
      // wherever rows exist; older histories without that shape keep the materialized path.
      const keyBlocksPresent = segments.every(
        (segment) =>
          segment.rowCount === 0 || (segment.columnBlockIds[keyColumn.id]?.length ?? 0) > 0,
      );
      if (!keyBlocksPresent) return undefined;
    }
    // A projected column absent from an older segment reads as NULL through the materialized
    // null-fill; the streaming loader is block-driven, so those plans keep the materialized path.
    const columnsPresent = projectedColumns.every((column) =>
      scanSegments.every((segment) => segment.columnBlockIds[column.id] !== undefined),
    );
    if (!columnsPresent) return undefined;
    const scanRowCount = scanSegments.reduce((total, segment) => total + segment.rowCount, 0);
    return {
      create: async (memory: QueryMemoryContext) =>
        mutationSegments.length > 0 && keyColumn !== undefined
          ? this.#createStreamedMutationTable(
              table,
              keyColumn,
              projectedColumns,
              segments,
              snapshot,
              memory,
            )
          : this.#createStreamedTable(
              table,
              projectedColumns,
              scanSegments,
              snapshot,
              scanRowCount,
              memory,
            ),
    };
  }

  /**
   * Executes a single unordered/ungrouped inner equi-join whose build side is too big to
   * materialize under the budget. Grace-style partition-by-rescan: pass i re-streams the build
   * table and keeps resident only rows whose join-key hash falls in partition i (about one
   * P-th of the build, reserved as usual), then runs the unchanged plan against a fresh
   * streamed base scan. Equal keys share a partition, so each inner match occurs in exactly
   * one pass and the union of pass results is the join; build rows with NULL or NaN keys can
   * never match and drop at partition time. Row order across passes is implementation-defined,
   * like any unordered query. The rescans trade time for bounded memory; the decoded-block
   * cache keeps repeat decodes cheap.
   */
  async #runPartitionedJoin(
    plan: CompiledQuery,
    budgetBytes: number,
    estimatedBuildBytes: number,
    baseView: {
      create: (memory: QueryMemoryContext) => Promise<{
        table: ColumnarTable;
        load: (start: number, length: number) => Promise<void>;
      }>;
    },
    buildView: {
      create: (memory: QueryMemoryContext) => Promise<{
        table: ColumnarTable;
        load: (start: number, length: number) => Promise<void>;
      }>;
    },
    buildTableName: string,
    buildKeyName: string,
  ): Promise<QueryResult> {
    // One partition should fit in an eighth of the budget: the resident partition briefly
    // exists twice (boxed rows, then their vectors) and shares the budget with both streamed
    // windows and the accumulating result.
    let partitions = 2;
    while (partitions < 64 && estimatedBuildBytes / partitions > budgetBytes / 8) partitions *= 2;
    const { limit, offset, ...strippedPlan } = plan;
    const wantedRows = (offset ?? 0) + (limit ?? Number.POSITIVE_INFINITY);
    const root = new QueryMemoryContext(budgetBytes);
    try {
      const combined: QueryRow[] = [];
      for (let partition = 0; partition < partitions; partition += 1) {
        if (combined.length >= wantedRows) break;
        const passMemory = root.createChild();
        let prepared: PreparedQuery | undefined;
        try {
          // The boxed rows live only until their vectors are built; a nested context releases
          // them so each partition is charged once while the pass executes.
          const collectMemory = passMemory.createChild();
          const buildScanMemory = collectMemory.createChild();
          const build = await buildView.create(buildScanMemory);
          let buildRows: DatabaseRow[] = [];
          const buildColumns = [...build.table.columns.entries()];
          // Small steps keep the collection windows a minor budget term next to the resident
          // partition itself.
          const step = 1_024;
          for (let start = 0; start < build.table.rowCount; start += step) {
            const length = Math.min(step, build.table.rowCount - start);
            await build.load(start, length);
            const keyVector = build.table.columns.get(buildKeyName);
            if (keyVector === undefined) {
              throw new Error(`Join key column is missing: ${buildTableName}.${buildKeyName}`);
            }
            for (let row = start; row < start + length; row += 1) {
              const key = columnVectorValueAt(keyVector, row);
              if (key === null || (typeof key === "number" && Number.isNaN(key))) continue;
              if (joinPartitionOf(key, partitions) !== partition) continue;
              const buildRow: DatabaseRow = {};
              for (const [name, vector] of buildColumns) {
                buildRow[name] = columnVectorValueAt(vector, row);
              }
              collectMemory.tally(
                estimateRowBytes(buildRow) + 2 * QUERY_ROW_OVERHEAD_BYTES,
                "Partitioned join build rows",
              );
              buildRows.push(buildRow);
            }
          }
          buildScanMemory.close();
          if (buildRows.length === 0) {
            collectMemory.close();
            continue;
          }
          const buildInput = columnarTableFromRows(buildTableName, buildRows);
          buildRows = [];
          const streamedBase = await baseView.create(passMemory);
          const inputTables = new Map<string, ColumnarTable>([
            [buildTableName, buildInput],
            [streamedBase.table.name, streamedBase.table],
          ]);
          prepared = createPreparedColumnarQuery(strippedPlan, inputTables, passMemory);
          collectMemory.close();
          const result = await prepared.executeAsync({ loadScanWindow: streamedBase.load });
          for (const row of result.rows) {
            if (combined.length >= wantedRows) break;
            root.tally(
              estimateRowBytes(row) + QUERY_ROW_OVERHEAD_BYTES,
              "Partitioned join result rows",
            );
            combined.push(row);
          }
        } finally {
          prepared?.close();
          passMemory.close();
        }
      }
      const startRow = offset ?? 0;
      const rows =
        startRow > 0 || limit !== undefined
          ? combined.slice(startRow, limit === undefined ? undefined : startRow + limit)
          : combined;
      return { columns: plan.select.map((item) => item.alias), rows };
    } finally {
      root.close();
    }
  }

  /**
   * Builds a single-table columnar view whose projected vectors hold one block-aligned resident
   * window at a time. Each load drops blocks that end before the requested range, decodes forward
   * as needed, reserves the replacement window before installing it, and releases the superseded
   * reservation afterward. The scan is forward-only; a backward request is a programming error.
   */
  #createStreamedTable(
    table: TableRecord,
    projectedColumns: readonly TableColumnRecord[],
    segments: readonly SegmentRecord[],
    snapshot: LeasedSnapshot,
    rowCount: number,
    memory: QueryMemoryContext,
  ): { table: ColumnarTable; load: (start: number, length: number) => Promise<void> } {
    interface ResidentBlock {
      column: ValidatedPhysicalColumn;
      startRow: number;
    }
    interface StreamedColumnState {
      column: TableColumnRecord;
      vector: ColumnVector;
      blockIds: readonly string[];
      nextBlockIndex: number;
      nextBlockStartRow: number;
      resident: ResidentBlock[];
      reservations: QueryMemoryReservation[];
    }
    const states: StreamedColumnState[] = projectedColumns.map((column) => ({
      column,
      vector: createStreamedColumnVector(column.type, rowCount),
      blockIds: segments.flatMap((segment) => segment.columnBlockIds[column.id] ?? []),
      nextBlockIndex: 0,
      nextBlockStartRow: 0,
      resident: [],
      reservations: [],
    }));
    const load = async (start: number, length: number): Promise<void> => {
      const end = Math.min(start + length, rowCount);
      for (const state of states) {
        const window = state.vector.window;
        if (window !== undefined && start >= window.start && end <= window.start + window.length) {
          continue;
        }
        if (window !== undefined && start < window.start) {
          throw new Error(`Streamed scan moved backward: ${table.name}.${state.column.name}`);
        }
        state.resident = state.resident.filter(
          (block) => block.startRow + block.column.rowCount > start,
        );
        while (state.nextBlockStartRow < end && state.nextBlockIndex < state.blockIds.length) {
          await this.#renewInternalLeaseIfNeeded(snapshot);
          const blockId = state.blockIds[state.nextBlockIndex] ?? "";
          const bytes = await this.store.getBlock(blockId);
          if (bytes === undefined) throw new Error(`Visible block is missing: ${blockId}`);
          const decoded = await decodePhysicalBlock(bytes);
          if (decoded.column.type !== state.column.type) {
            throw new Error(`Column type mismatch: ${state.column.name}`);
          }
          state.resident.push({ column: decoded.column, startRow: state.nextBlockStartRow });
          state.nextBlockIndex += 1;
          state.nextBlockStartRow += decoded.column.rowCount;
        }
        const first = state.resident[0];
        if (first === undefined || state.nextBlockStartRow < end) {
          throw new Error(`Column row count mismatch: ${state.column.name}`);
        }
        const windowStart = first.startRow;
        const windowRows = state.nextBlockStartRow - windowStart;
        const replacements: QueryMemoryReservation[] = [];
        try {
          installStreamedWindow(
            state.vector,
            state.resident,
            windowStart,
            windowRows,
            memory,
            `Streamed window ${state.column.name}`,
            replacements,
          );
        } catch (error) {
          for (const replacement of replacements) replacement.release();
          throw error;
        }
        for (const previous of state.reservations) previous.release();
        state.reservations = replacements;
      }
    };
    return {
      table: {
        name: table.name,
        rowCount,
        columns: new Map(states.map((state) => [state.column.name, state.vector])),
      },
      load,
    };
  }

  /**
   * Builds a streamed view of a keyed table whose visible history contains update and delete
   * segments (no upserts — those interleave new rows into slot order and keep the materialized
   * path). Mutation deltas are the small part of such a history, so they replay into resident
   * state — a dead-row bitmap over the base rows plus per-slot column patches referencing the
   * resident update vectors — while the base rows stream through the existing block-aligned
   * inner window. The outer view compacts dead rows and overlays patches per window, producing
   * exactly the materialized replay's rows in exactly its order.
   *
   * The replay tracks only mutation-touched key tokens, so its memory is bounded by the
   * mutation size, not the table; the duplicate-key corruption guard consequently only fires
   * for touched keys on this path.
   */
  async #createStreamedMutationTable(
    table: TableRecord,
    keyColumn: TableColumnRecord,
    projectedColumns: readonly TableColumnRecord[],
    baseSegments: readonly SegmentRecord[],
    snapshot: LeasedSnapshot,
    memory: QueryMemoryContext,
  ): Promise<{ table: ColumnarTable; load: (start: number, length: number) => Promise<void> }> {
    const scanSegments = baseSegments.filter((segment) => {
      const kind = segment.kind ?? "insert";
      return kind === "insert" || kind === "base";
    });

    // Phase A: materialize each mutation segment's key vector (and an update's changed
    // projected columns) — resident and reserved, bounded by the mutation history's size.
    const mutationKeyVectors = new Map<string, ColumnVector>();
    const mutationChangedVectors = new Map<string, Map<string, ColumnVector>>();
    const touched = new Set<string>();
    for (const segment of baseSegments) {
      const kind = segment.kind ?? "insert";
      if (kind !== "update" && kind !== "delete") continue;
      const keyVector = await this.#materializeAppendColumnVector(
        keyColumn,
        [segment],
        snapshot,
        segment.rowCount,
      );
      memory.reserve(columnVectorRetainedBytes(keyVector), "Streamed mutation replay");
      mutationKeyVectors.set(segment.id, keyVector);
      for (let row = 0; row < segment.rowCount; row += 1) {
        touched.add(columnVectorKeyToken(keyColumn.type, keyVector, row));
      }
      if (kind === "update") {
        const changed = new Map<string, ColumnVector>();
        for (const column of projectedColumns) {
          if (column.id === keyColumn.id) continue;
          if ((segment.columnBlockIds[column.id]?.length ?? 0) === 0) continue;
          const vector = await this.#materializeAppendColumnVector(
            column,
            [segment],
            snapshot,
            segment.rowCount,
          );
          memory.reserve(columnVectorRetainedBytes(vector), "Streamed mutation replay");
          changed.set(column.id, vector);
        }
        mutationChangedVectors.set(segment.id, changed);
      }
    }

    // Phase B: one bounded pass over the scan segments' key blocks — a single block resident
    // at a time — recording, per scan segment, the touched tokens and their absolute slots.
    const touchedByScanSegment = new Map<string, Array<{ token: string; slot: number }>>();
    let baseRows = 0;
    for (const segment of scanSegments) {
      const entries: Array<{ token: string; slot: number }> = [];
      touchedByScanSegment.set(segment.id, entries);
      let segmentRows = 0;
      for (const blockId of segment.columnBlockIds[keyColumn.id] ?? []) {
        await this.#renewInternalLeaseIfNeeded(snapshot);
        const bytes = await this.store.getBlock(blockId);
        if (bytes === undefined) throw new Error(`Visible block is missing: ${blockId}`);
        const decoded = await decodePhysicalBlock(bytes);
        if (decoded.column.type !== keyColumn.type) {
          throw new Error(`Column type mismatch: ${keyColumn.name}`);
        }
        const rows = decoded.column.rowCount;
        const validity = new Uint8Array(Math.ceil(rows / 8));
        const values =
          keyColumn.type === "boolean"
            ? new Uint8Array(rows)
            : keyColumn.type === "string"
              ? undefined
              : new Float64Array(rows);
        const codes = keyColumn.type === "string" ? new Uint32Array(rows) : undefined;
        codes?.fill(NULL_STRING_VECTOR_CODE);
        const builder = new StringDictionaryBuilder();
        appendPhysicalColumnToVector(decoded.column, 0, validity, values, codes, builder);
        const blockVector = (
          codes !== undefined
            ? {
                kind: "string",
                length: rows,
                validity,
                codes,
                dictionary: builder.dictionary,
              }
            : { kind: keyColumn.type, length: rows, validity, values }
        ) as ColumnVector;
        for (let row = 0; row < rows; row += 1) {
          const token = columnVectorKeyToken(keyColumn.type, blockVector, row);
          if (touched.has(token)) entries.push({ token, slot: baseRows + segmentRows + row });
        }
        segmentRows += rows;
      }
      if (segmentRows !== segment.rowCount) {
        throw new Error(`Column row count mismatch: ${keyColumn.name}`);
      }
      baseRows += segment.rowCount;
      memory.tally(entries.length * 64, "Streamed mutation replay");
    }

    // Replay in visible segment order, exactly like the materialized path: a delete unmaps the
    // key (a later insert re-adds at a new slot), an update requires a live mapped key and
    // patches per changed column with last-writer-wins.
    const dead = new Uint8Array(Math.ceil(baseRows / 8));
    memory.reserve(dead.byteLength, "Streamed mutation replay");
    const tokenSlot = new Map<string, number>();
    const patches = new Map<number, Map<string, { vector: ColumnVector; row: number }>>();
    let deadCount = 0;
    for (const segment of baseSegments) {
      const kind = segment.kind ?? "insert";
      if (kind === "insert" || kind === "base") {
        for (const entry of touchedByScanSegment.get(segment.id) ?? []) {
          if (tokenSlot.has(entry.token)) {
            throw new Error(`Stored table contains a duplicate unique key: ${table.name}`);
          }
          tokenSlot.set(entry.token, entry.slot);
        }
        continue;
      }
      const keyVector = mutationKeyVectors.get(segment.id);
      if (keyVector === undefined) {
        throw new Error(`Mutation segment key vector is missing: ${segment.id}`);
      }
      if (kind === "delete") {
        for (let row = 0; row < segment.rowCount; row += 1) {
          const token = columnVectorKeyToken(keyColumn.type, keyVector, row);
          const slot = tokenSlot.get(token);
          if (slot !== undefined && !bitmapHasValue(dead, slot)) {
            setBitmapValue(dead, slot);
            deadCount += 1;
            patches.delete(slot);
          }
          tokenSlot.delete(token);
        }
        continue;
      }
      const changed = mutationChangedVectors.get(segment.id) ?? new Map<string, ColumnVector>();
      for (let row = 0; row < segment.rowCount; row += 1) {
        const token = columnVectorKeyToken(keyColumn.type, keyVector, row);
        const slot = tokenSlot.get(token);
        if (slot === undefined) {
          throw new Error(`Update segment references a missing key: ${segment.id}`);
        }
        let slotPatches = patches.get(slot);
        if (slotPatches === undefined) {
          slotPatches = new Map();
          patches.set(slot, slotPatches);
        }
        for (const [columnId, vector] of changed) slotPatches.set(columnId, { vector, row });
      }
    }
    memory.tally(patches.size * 96 + tokenSlot.size * 64, "Streamed mutation replay");

    const outputRows = baseRows - deadCount;
    const inner = this.#createStreamedTable(
      table,
      projectedColumns,
      scanSegments,
      snapshot,
      baseRows,
      memory,
    );
    interface OuterColumnState {
      column: TableColumnRecord;
      vector: ColumnVector;
      reservations: QueryMemoryReservation[];
    }
    const states: OuterColumnState[] = projectedColumns.map((column) => ({
      column,
      vector: createStreamedColumnVector(column.type, outputRows),
      reservations: [],
    }));
    // Forward-only cursor: cursorOutput live rows exist strictly before base row cursorBase.
    let cursorOutput = 0;
    let cursorBase = 0;
    const load = async (start: number, length: number): Promise<void> => {
      const end = Math.min(start + length, outputRows);
      const window = states[0]?.vector.window;
      if (window !== undefined && start >= window.start && end <= window.start + window.length) {
        return;
      }
      if (window !== undefined && start < window.start) {
        throw new Error(`Streamed scan moved backward: ${table.name}`);
      }
      while (cursorOutput < start && cursorBase < baseRows) {
        if (!bitmapHasValue(dead, cursorBase)) cursorOutput += 1;
        cursorBase += 1;
      }
      const baseStart = cursorBase;
      let scanBase = cursorBase;
      let scanOutput = cursorOutput;
      const liveBaseRows: number[] = [];
      while (scanOutput < end && scanBase < baseRows) {
        if (!bitmapHasValue(dead, scanBase)) {
          liveBaseRows.push(scanBase);
          scanOutput += 1;
        }
        scanBase += 1;
      }
      if (scanOutput < end) throw new Error(`Column row count mismatch: ${table.name}`);
      if (scanBase > baseStart) await inner.load(baseStart, scanBase - baseStart);
      const windowRows = end - start;
      for (const state of states) {
        const innerVector = inner.table.columns.get(state.column.name);
        const innerWindow = innerVector?.window;
        if (innerVector === undefined || innerWindow === undefined) {
          throw new Error(`Streamed column is missing: ${state.column.name}`);
        }
        const validityBytes = Math.ceil(windowRows / 8);
        const typedBytes =
          validityBytes +
          (state.column.type === "boolean"
            ? windowRows
            : state.column.type === "string"
              ? windowRows * Uint32Array.BYTES_PER_ELEMENT
              : windowRows * Float64Array.BYTES_PER_ELEMENT);
        const replacements: QueryMemoryReservation[] = [];
        try {
          replacements.push(memory.reserve(typedBytes, `Streamed window ${state.column.name}`));
          const validity = new Uint8Array(validityBytes);
          const values =
            state.column.type === "boolean"
              ? new Uint8Array(windowRows)
              : state.column.type === "string"
                ? undefined
                : new Float64Array(windowRows);
          const codes = state.column.type === "string" ? new Uint32Array(windowRows) : undefined;
          codes?.fill(NULL_STRING_VECTOR_CODE);
          const dictionary: string[] = [];
          const dictionaryIndex = new Map<string, number>();
          const target = (
            codes !== undefined
              ? { kind: "string", length: windowRows, validity, codes, dictionary }
              : { kind: state.column.type, length: windowRows, validity, values }
          ) as ColumnVector;
          for (let index = 0; index < liveBaseRows.length; index += 1) {
            const baseRow = liveBaseRows[index] ?? 0;
            const patch = patches.get(baseRow)?.get(state.column.id);
            if (patch !== undefined) {
              copyColumnVectorValue(patch.vector, patch.row, target, index, dictionaryIndex);
            } else {
              copyColumnVectorValue(
                innerVector,
                baseRow - innerWindow.start,
                target,
                index,
                dictionaryIndex,
              );
            }
          }
          let dictionaryBytes = 0;
          for (const value of dictionary) dictionaryBytes += value.length;
          if (dictionaryBytes > 0) {
            replacements.push(
              memory.reserve(dictionaryBytes, `Streamed window ${state.column.name}`),
            );
          }
          const mutable = state.vector as unknown as MutableStreamedVectorFields;
          mutable.validity = validity;
          if (values !== undefined) mutable.values = values;
          if (codes !== undefined) {
            mutable.codes = codes;
            mutable.dictionary = dictionary;
          }
          mutable.window = { start, length: windowRows };
        } catch (error) {
          for (const replacement of replacements) replacement.release();
          throw error;
        }
        for (const previous of state.reservations) previous.release();
        state.reservations = replacements;
      }
      cursorOutput = end;
      cursorBase = scanBase;
    };
    return {
      table: {
        name: table.name,
        rowCount: outputRows,
        columns: new Map(states.map((state) => [state.column.name, state.vector])),
      },
      load,
    };
  }

  /** Prepares one block's inputs and executes it, returning the caller-owned result. */
  async #executeBlock(
    block: CompiledQuery,
    snapshot: LeasedSnapshot,
    visibility: SegmentVisibilityCatalog,
    memory: QueryMemoryContext,
    realTables: ReadonlyMap<string, TableRecord>,
    typedSchemas: Map<string, SqlColumnSchema[]>,
    extraInputs?: ReadonlyMap<string, ColumnarTable>,
  ): Promise<QueryResult> {
    const ftsStats = await this.#ftsIndexStats(block, realTables, snapshot, visibility);
    const inputs = await this.#prepareBlockInputs(
      block,
      snapshot,
      visibility,
      memory,
      realTables,
      typedSchemas,
      extraInputs,
    );
    const prepared = createPreparedColumnarQuery(
      block,
      inputs,
      memory.createChild(),
      ftsStats === undefined ? {} : { ftsStats },
    );
    try {
      return prepared.execute();
    } finally {
      prepared.close();
    }
  }

  /**
   * Executes a real-table-rooted block through the prepare cache: the result of a
   * derived table, union member, windowed inner block, recursive base, or scalar/IN
   * subquery is a pure function of its compiled plan and the visible segment ids of the
   * real tables it references, so an identical block over identical table states reuses
   * the previous result instead of re-reading and re-executing.
   */
  async #executeBlockCached(
    block: CompiledQuery,
    snapshot: LeasedSnapshot,
    visibility: SegmentVisibilityCatalog,
    memory: QueryMemoryContext,
    realTables: ReadonlyMap<string, TableRecord>,
    typedSchemas: Map<string, SqlColumnSchema[]>,
  ): Promise<QueryResult> {
    const key = await this.#blockResultCacheKey(block, snapshot, visibility, realTables);
    if (key !== undefined) {
      const cached = this.#cacheGet(key) as QueryResult | undefined;
      if (cached !== undefined) return cached;
    }
    const result = await this.#executeBlock(
      block,
      snapshot,
      visibility,
      memory,
      realTables,
      typedSchemas,
    );
    if (key !== undefined) this.#cachePut(key, result, queryResultRetainedBytes(result));
    return result;
  }

  /**
   * Like #executeBlockCached, but also carries the block's inferred output schema. The
   * schema must be captured at miss time: executing the block registers its nested
   * synthetic sources in typedSchemas, and a later cache hit skips that registration, so
   * re-inferring the schema at hit time would find those names missing.
   */
  async #executeBlockWithSchemaCached(
    block: CompiledQuery,
    snapshot: LeasedSnapshot,
    visibility: SegmentVisibilityCatalog,
    memory: QueryMemoryContext,
    realTables: ReadonlyMap<string, TableRecord>,
    typedSchemas: Map<string, SqlColumnSchema[]>,
  ): Promise<{ result: QueryResult; schema: SqlColumnSchema[] }> {
    const key = await this.#blockResultCacheKey(block, snapshot, visibility, realTables);
    const schemaKey = key === undefined ? undefined : `s${key}`;
    if (schemaKey !== undefined) {
      const cached = this.#cacheGet(schemaKey) as
        { result: QueryResult; schema: SqlColumnSchema[] } | undefined;
      if (cached !== undefined) return cached;
    }
    const result = await this.#executeBlock(
      block,
      snapshot,
      visibility,
      memory,
      realTables,
      typedSchemas,
    );
    const schema = inferBlockSchema(block, typedSchemas);
    const entry = { result, schema };
    if (schemaKey !== undefined) {
      this.#cachePut(schemaKey, entry, queryResultRetainedBytes(result) + 64);
    }
    return entry;
  }

  async #blockResultCacheKey(
    block: CompiledQuery,
    snapshot: LeasedSnapshot,
    visibility: SegmentVisibilityCatalog,
    realTables: ReadonlyMap<string, TableRecord>,
  ): Promise<string | undefined> {
    if (this.#prepareCacheLimitBytes === 0) return undefined;
    const parts: string[] = [];
    for (const name of collectRealTableNames(block).sort()) {
      const table = realTables.get(name);
      if (table === undefined) return undefined;
      parts.push(`${table.id}=${await this.#tableFingerprint(table, snapshot, visibility)}`);
    }
    try {
      return `blk\u0000${parts.join(";")}\u0000${JSON.stringify(block)}`;
    } catch {
      return undefined;
    }
  }

  /** The ordered visible segment ids of one table, memoized per visibility catalog. */
  async #tableFingerprint(
    table: TableRecord,
    snapshot: LeasedSnapshot,
    visibility: SegmentVisibilityCatalog,
  ): Promise<string> {
    let memo = this.#visibilityFingerprints.get(visibility);
    if (memo === undefined) {
      memo = new Map();
      this.#visibilityFingerprints.set(visibility, memo);
    }
    const existing = memo.get(table.id);
    if (existing !== undefined) return existing;
    const segments = await this.#visibleSegmentRecords(table, snapshot, visibility);
    const fingerprint = segments.map((segment) => segment.id).join(",");
    memo.set(table.id, fingerprint);
    return fingerprint;
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
    let progress = await this.collectGarbageStep({
      maxItems,
      ...(options.maxPlanningItems === undefined
        ? {}
        : { maxPlanningItems: options.maxPlanningItems }),
    });
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
    const job =
      active ??
      (await this.#planGarbageCollection(
        positiveWholeNumber(options.maxPlanningItems ?? 1_024, "Garbage collection planning limit"),
      ));
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

  async #planGarbageCollection(maxPlanningItems: number): Promise<GarbageCollectionJobRecord> {
    const current = await this.store.getCurrentManifest();
    const currentBlockIds = new Set(current?.blockIds ?? []);
    const candidateManifestVersions: number[] = [];
    const candidateBlockIds: string[] = [];
    const candidateSegmentIds: string[] = [];
    const candidateBlockIdSet = new Set<string>();
    const candidateSegmentIdSet = new Set<string>();
    const remaining = () =>
      maxPlanningItems - candidateBlockIds.length - candidateSegmentIds.length;
    const addBlocks = (ids: readonly string[]) => {
      for (const id of ids) {
        if (remaining() <= 0) break;
        if (candidateBlockIdSet.has(id)) continue;
        candidateBlockIdSet.add(id);
        candidateBlockIds.push(id);
      }
    };
    const addSegments = (ids: readonly string[]) => {
      for (const id of ids) {
        if (remaining() <= 0) break;
        if (candidateSegmentIdSet.has(id)) continue;
        candidateSegmentIdSet.add(id);
        candidateSegmentIds.push(id);
      }
    };
    let manifestCursor: number | null = null;
    do {
      const page = await this.store.listManifestPage(manifestCursor, 64);
      for (const manifest of page.records) {
        if (manifest.version === current?.version) continue;
        const existing = await this.#existingGarbageBlockCandidates(
          manifest.blockIds,
          currentBlockIds,
          remaining(),
        );
        if (manifest.prunedAt === undefined || existing.length > 0) {
          candidateManifestVersions.push(manifest.version);
          addBlocks(existing);
        }
        if (remaining() <= 0 || candidateManifestVersions.length === 64) break;
      }
      if (remaining() <= 0 || candidateManifestVersions.length === 64) break;
      manifestCursor = page.nextCursor;
    } while (manifestCursor !== null);

    if (remaining() > 0) {
      let transactionCursor: string | null = null;
      do {
        const page = await this.store.listTransactionPage(transactionCursor, 64);
        for (const transaction of page.records) {
          if (transaction.status !== "aborted") continue;
          addBlocks(
            await this.#existingGarbageBlockCandidates(
              transaction.pendingBlockIds,
              currentBlockIds,
              remaining(),
            ),
          );
          addSegments(
            await this.#existingGarbageSegmentCandidates(
              transaction.pendingSegmentIds,
              remaining(),
            ),
          );
          if (remaining() <= 0) break;
        }
        if (remaining() <= 0) break;
        transactionCursor = page.nextCursor;
      } while (transactionCursor !== null);
    }

    if (remaining() > 0) {
      let compactionCursor: string | null = null;
      do {
        const page = await this.store.listCompactionJobPage(compactionCursor, 64);
        for (const job of page.records) {
          if (job.state !== "published" && job.state !== "cancelled" && job.state !== "aborted") {
            continue;
          }
          addBlocks(
            await this.#existingGarbageBlockCandidates(
              [...job.sourceBlockIds, ...job.outputBlockIds],
              currentBlockIds,
              remaining(),
            ),
          );
          addSegments(
            await this.#existingGarbageSegmentCandidates(
              [
                ...job.sourceSegmentIds,
                ...(job.outputSegmentId === null ? [] : [job.outputSegmentId]),
              ],
              remaining(),
            ),
          );
          if (remaining() <= 0) break;
        }
        if (remaining() <= 0) break;
        compactionCursor = page.nextCursor;
      } while (compactionCursor !== null);
    }
    const timestamp = this.#now().toISOString();
    return this.store.createGarbageCollectionJob({
      id: `garbage-collection/${this.#createId()}`,
      candidateManifestVersions,
      candidateSegmentIds,
      candidateBlockIds,
      leaseCutoff: timestamp,
      createdAt: timestamp,
    });
  }

  async #existingGarbageBlockCandidates(
    ids: readonly string[],
    currentBlockIds: ReadonlySet<string>,
    limit: number,
  ): Promise<string[]> {
    const candidates: string[] = [];
    const seen = new Set<string>();
    for (let start = 0; start < ids.length && candidates.length < limit; start += 64) {
      const page = ids.slice(start, start + 64);
      const blocks = await this.store.getBlocks(page);
      for (let index = 0; index < page.length && candidates.length < limit; index += 1) {
        const id = page[index] ?? "";
        if (blocks[index] !== undefined && !currentBlockIds.has(id) && !seen.has(id)) {
          seen.add(id);
          candidates.push(id);
        }
      }
    }
    return candidates;
  }

  async #existingGarbageSegmentCandidates(
    ids: readonly string[],
    limit: number,
  ): Promise<string[]> {
    const candidates: string[] = [];
    const seen = new Set<string>();
    for (const id of ids) {
      if (candidates.length >= limit) break;
      if (seen.has(id) || (await this.store.getSegment(id)) === undefined) continue;
      seen.add(id);
      candidates.push(id);
    }
    return candidates;
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
        if (latest === undefined)
          throw new Error(`Garbage collection job not found: ${job.id}`, { cause: error });
        job = latest;
      }
    }
  }

  async #planCompaction(
    table: TableRecord,
    options: CompactTableOptions,
  ): Promise<CompactionJobRecord | CompactTableResult> {
    for (;;) {
      const version = await this.store.getCurrentManifestVersion();
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
    let keyedLevelTwo = false;
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
    } else if (
      table.uniqueKeyColumnId !== undefined ||
      visibleSegments.some(
        (segment) => (segment.kind ?? "insert") !== "insert" || segment.rowIdSpans !== undefined,
      )
    ) {
      // Keyed multi-range L2: merge (optional anchor + oldest level-zero prefix) into a new
      // span-carrying partition. Published partitions are never rewritten; mutation kinds
      // without a unique key cannot merge and keep the materialized skip.
      if (table.uniqueKeyColumnId === undefined) {
        return compactTableSkipped(
          table.name,
          "contains-mutation-segments",
          visibleSegments,
          visibleBlockIds,
          version,
        );
      }
      const layout = keyedLevelTwoLayout(visibleSegments);
      if (layout === null) {
        return compactTableSkipped(
          table.name,
          "unsupported-level-layout",
          visibleSegments,
          visibleBlockIds,
          version,
        );
      }
      anchor = layout.anchor;
      level0Segments = layout.level0Segments;
      effectiveMinimumLevel0Segments = minimumLevel0Segments;
      outputPartitionOrdinal = layout.levelTwoSegments.length;
      keyedLevelTwo = true;
    } else {
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
    // A keyed L2 promotion always merges: one uniform partition shape (a full-row base with
    // row-ID spans) regardless of whether the selected prefix happens to be pure inserts.
    const requiresMerge = keyedLevelTwo
      ? true
      : targetLevel === 1 &&
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
    let mergePlan: MergeCompactionRewritePlan | undefined;
    if (requiresMerge) {
      try {
        mergePlan = await this.#createMergeCompactionPlan(
          table,
          sourceSegments,
          targetBlockBytes,
          outputCompression,
          memoryBudgetBytes,
          snapshot,
        );
      } catch (error) {
        // A keyed L2 prefix whose mutations reference keys living in already-published
        // partitions cannot fold them without rewriting those partitions (key-range rewrite
        // remains future work); those deltas stay as replayable level-zero history.
        if (keyedLevelTwo && errorMessage(error).includes("references a missing key")) {
          return compactTableSkipped(
            table.name,
            "keys-outside-selected-sources",
            sourceSegments,
            sourceBlockIds,
            version,
          );
        }
        throw error;
      }
    }
    const rewritePlan =
      mergePlan ??
      (await this.#createRechunkCompactionPlan(
        table,
        sourceSegments,
        targetBlockBytes,
        outputCompression,
        memoryBudgetBytes,
        snapshot,
      ));
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

    const baseJobId = ["compaction", table.id, "manifest", String(version)].join("/");
    let levelTwoBudget:
      | {
          outputPartitionOrdinal: number;
          maxWriteAmplification: number;
          maximumOutputStoredBytes: number;
          plannedOutputStoredBytesUpperBound: number;
          priorAttemptOutputStoredBytes: number;
        }
      | undefined;
    if (targetLevel === 2) {
      if (outputPartitionOrdinal === undefined) {
        throw new Error("L2 compaction requires an append-only or merged physical rewrite");
      }
      const maxWriteAmplification = levelTwoMaxWriteAmplification;
      if (maxWriteAmplification === undefined) {
        throw new Error("L2 compaction write-amplification policy is missing");
      }
      // Attempts at promoting the same sources share one lifetime ceiling: bytes a cancelled
      // or aborted attempt already wrote for this manifest version reduce what the next
      // attempt may spend, so repeated failures cannot multiply the physical write budget.
      const priorAttemptOutputStoredBytes = (await this.store.listCompactionJobs(table.id))
        .filter(
          (candidate) =>
            (candidate.state === "cancelled" || candidate.state === "aborted") &&
            (candidate.id === baseJobId || candidate.id.startsWith(`${baseJobId}/retry/`)),
        )
        .reduce((total, candidate) => total + candidate.outputStoredBytes, 0);
      const maximumOutputStoredBytes = Math.max(
        0,
        floorWholeNumberProduct(
          selection.level0SourceStoredBytes,
          maxWriteAmplification,
          "Compaction maximum output stored bytes",
        ) - priorAttemptOutputStoredBytes,
      );
      const plannedOutputStoredBytesUpperBound =
        await this.#plannedPhysicalOutputStoredBytesUpperBound(rewritePlan, snapshot);
      levelTwoBudget = {
        outputPartitionOrdinal,
        maxWriteAmplification,
        maximumOutputStoredBytes,
        plannedOutputStoredBytesUpperBound,
        priorAttemptOutputStoredBytes,
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
          priorAttemptOutputStoredBytes,
          targetBlockBytes,
          outputCompression,
          memoryBudgetBytes,
          minimumMemoryBytes,
          version,
        });
      }
    }

    let id = baseJobId;
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
      (
        await this.#transactionRecordsForSegments([
          ...(anchor === undefined ? [] : [anchor]),
          ...level0Segments,
        ])
      ).map((record) => [record.id, record]),
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
      (await this.#transactionRecordsForSegments(sourceSegments)).map((record) => [
        record.id,
        record,
      ]),
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
          for (const columnId of changedColumnIds) {
            const columnIndex = columnIndexById.get(columnId);
            if (columnIndex === undefined) {
              throw new Error(`Mutation compaction column is missing: ${columnId}`);
            }
            existing.sources[columnIndex] = mergeSourceAt(segment, columnId, rowIndex);
          }
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

    const rowIdSpans: RowIdSpan[] = [];
    const sourceRangesByColumn = table.columns.map(() => [] as MergeCompactionOutputSourceRange[]);
    let totalRows = 0;
    for (const row of rows) {
      if (row === undefined) continue;
      appendRowIdSpan(rowIdSpans, totalRows, row.rowId);
      for (let columnIndex = 0; columnIndex < table.columns.length; columnIndex += 1) {
        const column = table.columns[columnIndex];
        const source = row.sources[columnIndex];
        if (column === undefined || source === undefined) {
          throw new Error("Mutation compaction row is missing a column source");
        }
        const sourceRanges = sourceRangesByColumn[columnIndex];
        if (sourceRanges === undefined) throw new Error("Mutation output column is missing");
        appendMergeOutputRange(sourceRanges, totalRows, source);
      }
      totalRows += 1;
    }
    return {
      rowIdSpans,
      columns: table.columns.map((column, columnIndex) => {
        const sourceRanges = sourceRangesByColumn[columnIndex];
        if (sourceRanges === undefined) throw new Error("Mutation output column is missing");
        return { columnId: column.id, type: column.type, sourceRanges };
      }),
      totalRows,
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
          const visible = await this.#unprunedManifestContainsAll(expectedOutputIds);
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
      transaction.markLogicallyUnchanged();
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
          throw new Error(job.error, { cause: error });
        }
        const rebased = await transaction.rebase();
        try {
          await this.#assertCompactionSnapshotOrder(job, rebased);
        } catch (error) {
          if (transaction.status === "active") await transaction.abort();
          job = await this.#abortCompactionJob(job, errorMessage(error));
          throw new Error(job.error, { cause: error });
        }
        const missingSourceId = job.sourceBlockIds.find((id) => !rebased.hasBlock(id));
        if (missingSourceId !== undefined) {
          if (transaction.status === "active") await transaction.abort();
          job = await this.#abortCompactionJob(
            job,
            `Compaction source is no longer visible: ${missingSourceId}`,
          );
          throw new Error(job.error, { cause: error });
        }
        transaction.supersedeBlocks(job.sourceBlockIds);
        transaction.markLogicallyUnchanged();
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
    const allVisibleSegments = (await this.store.listSegments()).filter((segment) =>
      Object.values(segment.columnBlockIds)
        .flat()
        .every((blockId) => snapshot.hasBlock(blockId)),
    );
    const plannedSourceSegments = (
      await Promise.all(job.sourceSegmentIds.map((id) => this.store.getSegment(id)))
    ).filter((segment): segment is SegmentRecord => segment !== undefined);
    const transactions = new Map(
      (
        await this.#transactionRecordsForSegments([...allVisibleSegments, ...plannedSourceSegments])
      ).map((record) => [record.id, record]),
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
      if (plan.kind === "rechunk-v1") {
        await this.#assertLevelTwoSnapshotOrder(job, plan, snapshot, transactions);
        return;
      }
      if (plan.kind !== "merge-v1") {
        throw new Error("L2 compaction requires a rechunk or merge rewrite plan");
      }
      // A keyed multi-range promotion falls through to the merge-source and ordering checks
      // below; the retained partitions are validated against the planned ordinal there.
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
    const retainedPartitionOrdinals: number[] = [];
    for (const segment of visibleSegments) {
      if (sourceIds.has(segment.id)) continue;
      // A keyed L2 promotion retains the already-published partitions untouched; they precede
      // the merge sources by construction and are validated against the planned ordinal below.
      if (job.outputPartitionOrdinal !== undefined && segment.partitionOrdinal !== undefined) {
        retainedPartitionOrdinals.push(segment.partitionOrdinal);
        continue;
      }
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
    if (job.outputPartitionOrdinal !== undefined) {
      retainedPartitionOrdinals.sort((left, right) => left - right);
      if (
        retainedPartitionOrdinals.length !== job.outputPartitionOrdinal ||
        retainedPartitionOrdinals.some((ordinal, index) => ordinal !== index)
      ) {
        throw new Error("Concurrent segments violate the planned L2 layout");
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
            priorAttemptOutputStoredBytes: job.priorAttemptOutputStoredBytes ?? 0,
            lifetimeOutputStoredBytes:
              (job.priorAttemptOutputStoredBytes ?? 0) + job.outputStoredBytes,
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
      (await this.#transactionRecordsForSegments(sourceSegments)).map((record) => [
        record.id,
        record,
      ]),
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

  /** Every projected column from cache, or undefined on any miss (misses fall through). */
  #cachedAppendColumns(
    table: TableRecord,
    projectedColumns: readonly TableColumnRecord[],
    fingerprint: string,
  ): Map<string, ColumnVector> | undefined {
    if (this.#prepareCacheLimitBytes === 0) return undefined;
    const columns = new Map<string, ColumnVector>();
    for (const column of projectedColumns) {
      const vector = this.#cachedColumnVector(table.id, column.id, fingerprint);
      if (vector === undefined) return undefined;
      columns.set(column.name, vector);
    }
    return columns;
  }

  #cacheGet(key: string): unknown {
    if (this.#prepareCacheLimitBytes === 0) return undefined;
    const entry = this.#prepareCache.get(key);
    if (entry === undefined) return undefined;
    this.#prepareCache.delete(key);
    this.#prepareCache.set(key, entry);
    return entry.payload;
  }

  /**
   * Caches one prepared artifact under the byte-bounded LRU. Payloads are shared
   * read-only across prepared queries; entries for superseded segment sets simply stop
   * matching and age out the same way evicted entries do.
   */
  #cachePut(key: string, payload: unknown, bytes: number): void {
    if (this.#prepareCacheLimitBytes === 0 || bytes > this.#prepareCacheLimitBytes) return;
    const existing = this.#prepareCache.get(key);
    if (existing !== undefined) {
      this.#prepareCache.delete(key);
      this.#prepareCacheUsedBytes -= existing.bytes;
    }
    this.#prepareCache.set(key, { payload, bytes });
    this.#prepareCacheUsedBytes += bytes;
    for (const [oldestKey, entry] of this.#prepareCache) {
      if (this.#prepareCacheUsedBytes <= this.#prepareCacheLimitBytes) break;
      if (oldestKey === key) continue;
      this.#prepareCache.delete(oldestKey);
      this.#prepareCacheUsedBytes -= entry.bytes;
    }
  }

  #cachedColumnVector(
    tableId: string,
    columnId: string,
    fingerprint: string,
  ): ColumnVector | undefined {
    return this.#cacheGet(`vec\u0000${tableId}\u0000${columnId}\u0000${fingerprint}`) as
      ColumnVector | undefined;
  }

  #storeColumnVector(
    tableId: string,
    columnId: string,
    fingerprint: string,
    vector: ColumnVector,
  ): void {
    this.#cachePut(
      `vec\u0000${tableId}\u0000${columnId}\u0000${fingerprint}`,
      vector,
      columnVectorRetainedBytes(vector),
    );
  }

  /**
   * Builds (or rebuilds) the persisted full-text pruning index for one column from the current
   * snapshot. Append-only tables only. The planner consults the index when the column is
   * "ready" with a matching tokenizer version and re-verifies every candidate row, so a stale,
   * missing, or lost index costs speed, never correctness. Searches schedule this lazily once
   * a table crosses the auto-index threshold; calling it directly is just a warm-up.
   */
  async buildFtsIndex(tableName: string, columnName: string): Promise<void> {
    const table = await this.#findTable(tableName);
    const column = table.columns.find((candidate) => candidate.name === columnName);
    if (column === undefined) throw new TypeError(`Unknown column: ${columnName}`);
    if (column.type === "boolean") {
      throw new TypeError(`Full-text indexes cannot cover boolean columns: ${columnName}`);
    }
    const stamp = (
      record: TableRecord,
      state: FtsColumnIndexRecord["state"],
      buildFromVersion: number,
    ): Promise<TableRecord> =>
      this.store.updateTable(record.id, record.revision ?? 0, {
        ftsColumns: {
          ...record.ftsColumns,
          [column.id]: {
            storage: "fts-chunks-v1",
            tokenizerVersion: FTS_TOKENIZER_VERSION,
            state,
            buildFromVersion,
          },
        },
      });
    // From this flip on, every writer that reads the record emits commit deltas; writers that
    // committed against the older record flip the column to "invalid" in the store, which the
    // ready-CAS below observes as a revision conflict and abandons the build.
    const marked = await stamp(table, "building", -1);
    await this.#withLeasedSnapshot(undefined, async (snapshot) => {
      const segments = await this.#visibleSegmentRecords(marked, snapshot);
      if (segments.some((segment) => (segment.kind ?? "insert") !== "insert")) {
        await stamp(marked, "invalid", -1).catch(() => undefined);
        throw new TypeError(
          `Full-text indexes support append-only tables; ${tableName} has keyed mutations`,
        );
      }
      const rowCount = segments.reduce((total, segment) => total + segment.rowCount, 0);
      const vector = await this.#materializeAppendColumnVector(
        column,
        segments,
        snapshot,
        rowCount,
      );
      const byTerm = new Map<string, { rowIds: bigint[]; tf: number[] }>();
      let totalTokens = 0;
      let offset = 0;
      for (const segment of segments) {
        for (let row = 0; row < segment.rowCount; row += 1) {
          totalTokens += addFtsDocument(
            byTerm,
            vectorValue(vector, offset + row),
            segment.rowIdStart + BigInt(row),
          );
        }
        offset += segment.rowCount;
      }
      const coversVersion = snapshot.version ?? -1;
      await this.store.writeFtsBase(table.id, column.id, {
        coversVersion,
        chunks: chunkFtsPostings(sortedFtsPostings(byTerm)),
        totalTokens,
      });
      const fresh = await this.store.getTable(table.id);
      const current = fresh?.ftsColumns?.[column.id];
      if (fresh !== undefined && current?.state === "building") {
        await this.store.updateTable(fresh.id, fresh.revision ?? 0, {
          ftsColumns: {
            ...fresh.ftsColumns,
            [column.id]: { ...current, state: "ready", buildFromVersion: coversVersion },
          },
        });
      }
    });
  }

  /**
   * Segment-level candidate pruning for a top-level MATCH conjunct backed by ready indexes.
   * Candidates come from per-term postings (union across the document's columns, intersection
   * across terms); segments whose row-id range contains no candidate cannot satisfy the
   * conjunct and drop before their blocks are read. The scan still evaluates MATCH on every
   * surviving row, so false positives are free and false negatives are impossible for
   * append-only histories (rows never change after their postings are written).
   */
  /**
   * The top block's BM25 nodes, deduplicated by their compiled signature — the same key the
   * executor's bind step uses, so served statistics land on exactly the nodes that asked.
   */
  #ftsBm25Nodes(plan: CompiledQuery): Map<string, { columns: Expression[]; query: string }> {
    const nodes = new Map<string, { columns: Expression[]; query: string }>();
    const visit = (expression: Expression): void => {
      if (expression.kind === "fts" && expression.op === "bm25" && expression.columns !== "*") {
        nodes.set(JSON.stringify(expression), {
          columns: expression.columns,
          query: expression.query,
        });
        return;
      }
      for (const child of childExpressions(expression)) visit(child);
    };
    forEachBlockExpression(plan, visit);
    return nodes;
  }

  /**
   * Whether the persisted index can serve exact BM25 statistics for every scoring node of the
   * plan. This single predicate is shared by the stats service and the segment pruner over the
   * same catalog snapshot, so "the scan was pruned" implies "statistics came from the index" —
   * the invariant that keeps scores identical whether or not an index exists.
   */
  #ftsIndexServesScoring(
    plan: CompiledQuery,
    table: TableRecord,
    segments: readonly SegmentRecord[],
  ): boolean {
    if (plan.joins.length > 0 || plan.base.table !== table.name) return false;
    if (!segments.every((segment) => (segment.kind ?? "insert") === "insert")) return false;
    const columnsByName = new Map(table.columns.map((column) => [column.name, column] as const));
    for (const node of this.#ftsBm25Nodes(plan).values()) {
      for (const expression of node.columns) {
        if (expression.kind !== "column") return false;
        const column = columnsByName.get(
          expression.reference.split(".").at(-1) ?? expression.reference,
        );
        const state = column === undefined ? undefined : table.ftsColumns?.[column.id];
        if (state?.state !== "ready" || state.tokenizerVersion !== FTS_TOKENIZER_VERSION) {
          return false;
        }
      }
    }
    return true;
  }

  /**
   * One postings read per (column, terms) per snapshot: the statistics service and the segment
   * pruner ask for the same candidates in the canonical search shape, and sharing the promise
   * both halves the index I/O and guarantees the two make identical freshness decisions.
   */
  #readFtsCandidatesMemoized(
    snapshot: Snapshot,
    tableId: string,
    columnId: string,
    terms: ReadonlyArray<{ term: string; prefix: boolean }>,
    upToVersion: number,
  ): Promise<FtsCandidatesResult> {
    let memo = this.#ftsCandidatesMemo.get(snapshot);
    if (memo === undefined) {
      memo = new Map();
      this.#ftsCandidatesMemo.set(snapshot, memo);
    }
    const key = `${tableId}\u0000${columnId}\u0000${String(upToVersion)}\u0000${terms
      .map((term) => `${term.term}${term.prefix ? "*" : ""}`)
      .join("\u0000")}`;
    const cached = memo.get(key);
    if (cached !== undefined) return cached;
    // Shared leases live across queries at one version; results are version-deterministic, but
    // candidate arrays can be large, so the memo sheds wholesale rather than growing unbounded.
    if (memo.size >= 64) memo.clear();
    const read = this.store.readFtsCandidates(tableId, columnId, terms, upToVersion);
    memo.set(key, read);
    return read;
  }

  /**
   * Exact BM25 corpus statistics from the postings index, keyed by node signature, or
   * undefined when any scoring node cannot be served (the scan then computes its own).
   * docCount is the visible row count (every row is a document); per-term document frequency
   * is the size of the union of the term's posting row ids across the document's columns; and
   * token totals merge from the base plus commit deltas. All exact for append-only histories —
   * including under time travel or a concurrent rebuild, because a base covering a version
   * ahead of this snapshot disqualifies the index here (and, through the shared reads, in the
   * pruner) rather than mixing corpora.
   */
  async #ftsIndexStats(
    plan: CompiledQuery,
    realTables: ReadonlyMap<string, TableRecord>,
    snapshot: LeasedSnapshot,
    visibility?: SegmentVisibilityCatalog,
  ): Promise<Map<string, FtsStats> | undefined> {
    if (!planContainsFts(plan, "bm25")) return undefined;
    const table = realTables.get(plan.base.table);
    if (table === undefined) return undefined;
    const nodes = this.#ftsBm25Nodes(plan);
    if (nodes.size === 0) return undefined;
    const segments = await this.#visibleSegmentRecords(table, snapshot, visibility);
    if (!this.#ftsIndexServesScoring(plan, table, segments)) return undefined;
    const docCount = segments.reduce((total, segment) => total + segment.rowCount, 0);
    const columnsByName = new Map(table.columns.map((column) => [column.name, column] as const));
    const upToVersion = snapshot.version ?? -1;
    const stats = new Map<string, FtsStats>();
    for (const [signature, node] of nodes) {
      const terms = cachedQueryTerms(node.query);
      const columns = node.columns.map((expression) =>
        expression.kind === "column"
          ? columnsByName.get(expression.reference.split(".").at(-1) ?? expression.reference)
          : undefined,
      );
      if (columns.some((column) => column === undefined)) return undefined;
      const perColumn = await Promise.all(
        (columns as TableColumnRecord[]).map((column) =>
          this.#readFtsCandidatesMemoized(snapshot, table.id, column.id, terms, upToVersion),
        ),
      );
      if (perColumn.some((result) => result.coversVersion > upToVersion)) return undefined;
      if (perColumn.some((result) => result.deltaChunkCount > FTS_FOLD_DELTA_CHUNKS)) {
        this.#scheduleFtsBuilds(table, columns as TableColumnRecord[], segments, true);
      }
      const dfByTerm = terms.map(
        (_, term) =>
          unionSortedRowIds(perColumn.map((result) => result.rowIdsByTerm[term] ?? [])).length,
      );
      stats.set(signature, {
        docCount,
        totalTokens: perColumn.reduce((total, result) => total + result.totalTokens, 0),
        dfByTerm,
      });
    }
    return stats;
  }

  async #ftsPrunedSegments(
    table: TableRecord,
    segments: SegmentRecord[],
    plan: CompiledQuery,
    snapshot: LeasedSnapshot,
  ): Promise<SegmentRecord[]> {
    if (plan.joins.length > 0 || plan.base.table !== table.name) return segments;
    const matches = topLevelFtsMatchConjuncts(plan);
    if (matches.length === 0) return segments;
    if (!segments.every((segment) => (segment.kind ?? "insert") === "insert")) return segments;
    // A scoring plan may only prune when the index serves its corpus statistics — otherwise
    // the scan-side stats pass would measure a shrunken corpus and scores would change the
    // moment an index appeared. The shared predicate keeps this in lockstep with the stats
    // service reading the same catalog snapshot.
    if (planContainsFts(plan, "bm25") && !this.#ftsIndexServesScoring(plan, table, segments)) {
      return segments;
    }
    const columnsByName = new Map(table.columns.map((column) => [column.name, column] as const));
    let surviving = segments;
    for (const match of matches) {
      const terms = cachedQueryTerms(match.query);
      if (terms.length === 0) continue;
      const columns = match.columns.flatMap((expression) =>
        expression.kind === "column"
          ? [columnsByName.get(expression.reference.split(".").at(-1) ?? expression.reference)]
          : [undefined],
      );
      if (columns.some((column) => column === undefined)) continue;
      const resolved = columns as TableColumnRecord[];
      const ready = resolved.every((column) => {
        const state = table.ftsColumns?.[column.id];
        return state?.state === "ready" && state.tokenizerVersion === FTS_TOKENIZER_VERSION;
      });
      if (!ready) {
        this.#scheduleFtsBuilds(table, resolved, segments);
        continue;
      }
      const upToVersion = snapshot.version ?? -1;
      const perColumn = await Promise.all(
        resolved.map((column) =>
          this.#readFtsCandidatesMemoized(snapshot, table.id, column.id, terms, upToVersion),
        ),
      );
      // A base rebuilt past this snapshot yields a candidate SUPERSET — safe for match-only
      // pruning, but a scoring plan's statistics would have bailed on the same shared reads,
      // so pruning must bail with them to keep "pruned ⟹ statistics served".
      if (
        planContainsFts(plan, "bm25") &&
        perColumn.some((result) => result.coversVersion > upToVersion)
      ) {
        return segments;
      }
      // A long delta tail re-merges on every read; folding is just a rebuild at a newer version.
      if (perColumn.some((result) => result.deltaChunkCount > FTS_FOLD_DELTA_CHUNKS)) {
        this.#scheduleFtsBuilds(table, resolved, segments, true);
      }
      // Document semantics over sorted candidate arrays: per term, union across the document's
      // columns; across terms, intersection. Both are linear merges, and the segment filter is
      // then one binary search per segment — no per-candidate scans.
      let candidates: bigint[] | undefined;
      for (let term = 0; term < terms.length; term += 1) {
        const union = unionSortedRowIds(perColumn.map((result) => result.rowIdsByTerm[term] ?? []));
        candidates = candidates === undefined ? union : intersectSortedRowIds(candidates, union);
        if (candidates.length === 0) break;
      }
      const candidateList = candidates ?? [];
      surviving =
        candidateList.length === 0
          ? []
          : surviving.filter((segment) =>
              sortedRowIdsIntersectRange(
                candidateList,
                segment.rowIdStart,
                segment.rowIdEndExclusive,
              ),
            );
      if (surviving.length === 0) break;
    }
    return surviving;
  }

  /**
   * Fire-and-forget lazy index builds. Keys release when the attempt settles, so a failed or
   * lost build (including a column durably stuck in "building" after a killed tab) retries on
   * the next search rather than wedging for the session.
   */
  #scheduleFtsBuilds(
    table: TableRecord,
    columns: readonly TableColumnRecord[],
    segments: readonly SegmentRecord[],
    rebuild = false,
  ): void {
    const rowCount = segments.reduce((total, segment) => total + segment.rowCount, 0);
    if (!rebuild && rowCount < this.#ftsAutoIndexRows) return;
    for (const column of columns) {
      const key = `${table.id}/${column.id}`;
      if (this.#ftsBuildsInFlight.has(key)) continue;
      this.#ftsBuildsInFlight.add(key);
      void this.buildFtsIndex(table.name, column.name)
        .catch(() => undefined)
        .finally(() => this.#ftsBuildsInFlight.delete(key));
    }
  }

  async #materializeColumnarTableAtSnapshot(
    table: TableRecord,
    snapshot: LeasedSnapshot,
    projectedColumns: readonly TableColumnRecord[],
    visibility?: SegmentVisibilityCatalog,
    plan?: CompiledQuery,
  ): Promise<ColumnarTable> {
    const visibleSegments = await this.#visibleSegmentRecords(table, snapshot, visibility);
    const segments =
      plan === undefined
        ? visibleSegments
        : await this.#ftsPrunedSegments(table, visibleSegments, plan, snapshot);
    const keyColumn = getUniqueKeyColumn(table);
    if (
      segments.every((segment) => {
        const kind = segment.kind ?? "insert";
        return kind === "insert" || kind === "base";
      })
    ) {
      const rowCount = segments.reduce((total, segment) => total + segment.rowCount, 0);
      if (projectedColumns.length === 0) {
        return { name: table.name, rowCount, columns: new Map() };
      }
      const projectedKey =
        keyColumn !== undefined && projectedColumns.some((column) => column.id === keyColumn.id)
          ? keyColumn.name
          : undefined;
      // Segment records are immutable, so the ordered visible segment ids exactly identify
      // a column's assembled vector; a fully cached projection needs no store reads at all.
      const fingerprint = segments.map((segment) => segment.id).join(",");
      const cachedColumns = this.#cachedAppendColumns(table, projectedColumns, fingerprint);
      if (cachedColumns !== undefined) {
        return {
          name: table.name,
          rowCount,
          columns: cachedColumns,
          ...(projectedKey ? { uniqueKey: projectedKey } : {}),
        };
      }
      // Zone-map pruning shrinks the scan the same way index pruning does, so a scoring plan
      // whose statistics come from the scan (no index serving it) must see the whole corpus.
      const zonePruningAllowed =
        plan === undefined ||
        !planContainsFts(plan, "bm25") ||
        this.#ftsIndexServesScoring(plan, table, visibleSegments);
      const predicates =
        plan === undefined || !zonePruningAllowed ? [] : zonePredicates(plan, table);
      if (predicates.length > 0) {
        const prunedKey =
          `prn\u0000${table.id}\u0000${fingerprint}\u0000` +
          `${projectedColumns.map((column) => column.id).join(",")}\u0000` +
          predicates
            .map(
              (predicate) =>
                `${predicate.column.id} ${predicate.operator} ${String(predicate.value)}`,
            )
            .join("&");
        const cachedPruned = this.#cacheGet(prunedKey) as ColumnarTable | undefined;
        if (cachedPruned !== undefined) return cachedPruned;
        const pruned = await this.#materializePrunedAppendTable(
          table,
          snapshot,
          projectedColumns,
          segments,
          predicates,
        );
        if (pruned !== undefined) {
          this.#cachePut(prunedKey, pruned, columnarTableRetainedBytes(pruned));
          return pruned;
        }
      }
      const columns = new Map<string, ColumnVector>();
      for (const column of projectedColumns) {
        let vector = this.#cachedColumnVector(table.id, column.id, fingerprint);
        if (vector === undefined) {
          vector = await this.#materializeAppendColumnVector(column, segments, snapshot, rowCount);
          this.#storeColumnVector(table.id, column.id, fingerprint, vector);
        }
        columns.set(column.name, vector);
      }
      return {
        name: table.name,
        rowCount,
        columns,
        ...(projectedKey ? { uniqueKey: projectedKey } : {}),
      };
    }
    const neededColumns = [
      ...projectedColumns,
      ...(keyColumn === undefined || projectedColumns.some((column) => column.id === keyColumn.id)
        ? []
        : [keyColumn]),
    ];
    const maximumRows = segments.reduce((total, segment) => {
      const kind = segment.kind ?? "insert";
      return (
        total + (kind === "insert" || kind === "base" || kind === "upsert" ? segment.rowCount : 0)
      );
    }, 0);
    const vectorsByColumn = new Map(
      neededColumns.map(
        (column) => [column.id, createEmptyColumnVector(column.type, maximumRows)] as const,
      ),
    );
    const dictionaryIndexes = new Map(
      neededColumns
        .filter((column) => column.type === "string")
        .map((column) => [column.id, new Map<string, number>()] as const),
    );
    const alive = new Uint8Array(maximumRows);
    const rowIndexByKey = new Map<string, number>();
    let slotCount = 0;

    for (const segment of segments) {
      await this.#renewInternalLeaseIfNeeded(snapshot);
      const segmentVectors = new Map<string, ColumnVector>();
      for (const column of neededColumns) {
        if ((segment.columnBlockIds[column.id]?.length ?? 0) === 0) continue;
        segmentVectors.set(
          column.id,
          await this.#materializeAppendColumnVector(column, [segment], snapshot, segment.rowCount),
        );
      }
      if (segment.kind === "delete") {
        if (keyColumn === undefined)
          throw new Error(`Delete segment has no unique key: ${segment.id}`);
        const keyVector = requiredColumnVector(segmentVectors, keyColumn, segment);
        for (let row = 0; row < segment.rowCount; row += 1) {
          const token = columnVectorKeyToken(keyColumn.type, keyVector, row);
          const existingIndex = rowIndexByKey.get(token);
          if (existingIndex !== undefined) alive[existingIndex] = 0;
          rowIndexByKey.delete(token);
        }
        continue;
      }
      if (segment.kind === "update") {
        if (keyColumn === undefined)
          throw new Error(`Update segment has no unique key: ${segment.id}`);
        const keyVector = requiredColumnVector(segmentVectors, keyColumn, segment);
        const changedColumns = projectedColumns.filter(
          (column) =>
            column.id !== keyColumn.id && (segment.columnBlockIds[column.id]?.length ?? 0) > 0,
        );
        for (let row = 0; row < segment.rowCount; row += 1) {
          const existingIndex = rowIndexByKey.get(
            columnVectorKeyToken(keyColumn.type, keyVector, row),
          );
          if (existingIndex === undefined || alive[existingIndex] !== 1) {
            throw new Error(`Update segment references a missing key: ${segment.id}`);
          }
          for (const column of changedColumns) {
            copyColumnVectorValue(
              requiredColumnVector(segmentVectors, column, segment),
              row,
              requiredColumnVector(vectorsByColumn, column, segment),
              existingIndex,
              dictionaryIndexes.get(column.id),
            );
          }
        }
        continue;
      }

      for (let segmentRow = 0; segmentRow < segment.rowCount; segmentRow += 1) {
        let existingIndex: number | undefined;
        let token: string | undefined;
        if (keyColumn !== undefined) {
          token = columnVectorKeyToken(
            keyColumn.type,
            requiredColumnVector(segmentVectors, keyColumn, segment),
            segmentRow,
          );
          existingIndex = rowIndexByKey.get(token);
        }
        if (segment.kind === "upsert" && existingIndex !== undefined) {
          for (const column of neededColumns) {
            // A column absent from this segment joined the catalog later; the replayed slot
            // keeps its NULL default. Older segments always replay before newer ones, so an
            // absent-column segment can never overwrite a value written with the column.
            if (segment.columnBlockIds[column.id] === undefined) continue;
            copyColumnVectorValue(
              requiredColumnVector(segmentVectors, column, segment),
              segmentRow,
              requiredColumnVector(vectorsByColumn, column, segment),
              existingIndex,
              dictionaryIndexes.get(column.id),
            );
          }
          alive[existingIndex] = 1;
          continue;
        }
        if (existingIndex !== undefined) {
          throw new Error(`Stored table contains a duplicate unique key: ${table.name}`);
        }
        const outputIndex = slotCount;
        for (const column of neededColumns) {
          if (segment.columnBlockIds[column.id] === undefined) continue;
          copyColumnVectorValue(
            requiredColumnVector(segmentVectors, column, segment),
            segmentRow,
            requiredColumnVector(vectorsByColumn, column, segment),
            outputIndex,
            dictionaryIndexes.get(column.id),
          );
        }
        alive[outputIndex] = 1;
        slotCount += 1;
        if (token !== undefined) rowIndexByKey.set(token, outputIndex);
      }
    }

    let visibleRowCount = 0;
    for (let row = 0; row < slotCount; row += 1) visibleRowCount += alive[row] ?? 0;
    if (projectedColumns.length === 0) {
      return { name: table.name, rowCount: visibleRowCount, columns: new Map() };
    }
    const columns = new Map<string, ColumnVector>();
    for (const column of projectedColumns) {
      const source = requiredColumnVector(vectorsByColumn, column);
      const output = createEmptyColumnVector(column.type, visibleRowCount);
      const outputDictionaryIndex =
        column.type === "string" ? new Map<string, number>() : undefined;
      let outputRow = 0;
      for (let sourceRow = 0; sourceRow < slotCount; sourceRow += 1) {
        if (alive[sourceRow] !== 1) continue;
        copyColumnVectorValue(source, sourceRow, output, outputRow, outputDictionaryIndex);
        outputRow += 1;
      }
      columns.set(column.name, output);
    }
    const projectedKey =
      keyColumn !== undefined && projectedColumns.some((column) => column.id === keyColumn.id)
        ? keyColumn.name
        : undefined;
    return {
      name: table.name,
      rowCount: visibleRowCount,
      columns,
      ...(projectedKey ? { uniqueKey: projectedKey } : {}),
    };
  }

  async #materializePrunedAppendTable(
    table: TableRecord,
    snapshot: LeasedSnapshot,
    projectedColumns: readonly TableColumnRecord[],
    segments: readonly SegmentRecord[],
    predicates: readonly ZonePredicate[],
  ): Promise<ColumnarTable | undefined> {
    const predicateColumns = [
      ...new Map(predicates.map((predicate) => [predicate.column.id, predicate.column])).values(),
    ];
    // Row-group alignment assumes every involved column stored blocks in every segment; a column
    // added after a segment was written reads as NULL through the full-scan path instead.
    const involvedColumns = [...projectedColumns, ...predicateColumns];
    if (
      involvedColumns.some((column) =>
        segments.some((segment) => segment.columnBlockIds[column.id] === undefined),
      )
    ) {
      return undefined;
    }
    const storedBlocks = new Map<string, Uint8Array>();
    const predicateBlockDescriptions = new Map<string, ReturnType<typeof inspectBlock>>();
    const predicateBlockIds = [
      ...new Set(
        segments.flatMap((segment) =>
          predicateColumns.flatMap((column) => segment.columnBlockIds[column.id] ?? []),
        ),
      ),
    ];
    for (let start = 0; start < predicateBlockIds.length; start += 16) {
      await this.#renewInternalLeaseIfNeeded(snapshot);
      const ids = predicateBlockIds.slice(start, start + 16);
      const blocks = await this.store.getBlocks(ids);
      for (let index = 0; index < ids.length; index += 1) {
        const id = ids[index] ?? "";
        const bytes = blocks[index];
        if (bytes === undefined) throw new Error(`Visible block is missing: ${id}`);
        storedBlocks.set(id, bytes);
        // Header and metadata only: zone-map elimination must not pay the decompress,
        // checksum, and payload-validation cost of blocks it is about to discard. The
        // surviving blocks are decoded (and fully verified) during materialization.
        predicateBlockDescriptions.set(id, inspectBlock(bytes));
      }
    }

    const selectedSegments: SelectedAppendSegment[] = [];
    for (const segment of segments) {
      const firstIds = segment.columnBlockIds[predicateColumns[0]?.id ?? ""] ?? [];
      if (
        predicateColumns.some(
          (column) => (segment.columnBlockIds[column.id]?.length ?? 0) !== firstIds.length,
        ) ||
        projectedColumns.some(
          (column) => (segment.columnBlockIds[column.id]?.length ?? 0) !== firstIds.length,
        )
      ) {
        return undefined;
      }
      const blockIndexes: number[] = [];
      const rowCounts: number[] = [];
      let segmentRows = 0;
      for (let blockIndex = 0; blockIndex < firstIds.length; blockIndex += 1) {
        let rowCount: number | undefined;
        let canMatch = true;
        for (const predicate of predicates) {
          const blockId = segment.columnBlockIds[predicate.column.id]?.[blockIndex] ?? "";
          const description = predicateBlockDescriptions.get(blockId);
          if (description === undefined) throw new Error(`Visible block is missing: ${blockId}`);
          if (description.type !== predicate.column.type) {
            throw new Error(`Column type mismatch: ${predicate.column.name}`);
          }
          if (rowCount !== undefined && description.rowCount !== rowCount) return undefined;
          rowCount = description.rowCount;
          if (!zoneMapCanMatch(description, predicate)) canMatch = false;
        }
        if (rowCount === undefined) return undefined;
        segmentRows += rowCount;
        if (canMatch) {
          blockIndexes.push(blockIndex);
          rowCounts.push(rowCount);
        }
      }
      if (segmentRows !== segment.rowCount) return undefined;
      if (blockIndexes.length > 0) selectedSegments.push({ segment, blockIndexes, rowCounts });
    }

    const expectedRows = new Map<string, number>();
    const candidateSegments = selectedSegments.map(({ segment, blockIndexes, rowCounts }) => {
      const columnBlockIds = Object.fromEntries(
        projectedColumns.map((column) => [
          column.id,
          blockIndexes.map((blockIndex, index) => {
            const blockId = segment.columnBlockIds[column.id]?.[blockIndex] ?? "";
            expectedRows.set(blockId, rowCounts[index] ?? 0);
            return blockId;
          }),
        ]),
      );
      return {
        ...segment,
        rowCount: rowCounts.reduce((total, count) => total + count, 0),
        columnBlockIds,
      };
    });
    const candidateRowCount = candidateSegments.reduce(
      (total, segment) => total + segment.rowCount,
      0,
    );
    const candidateVectors = new Map<string, ColumnVector>();
    for (const column of predicateColumns) {
      candidateVectors.set(
        column.id,
        await this.#materializeAppendColumnVector(
          column,
          candidateSegments,
          snapshot,
          candidateRowCount,
          storedBlocks,
          expectedRows,
        ),
      );
    }
    const selectedRows = new Uint32Array(candidateRowCount);
    let selectedRowCount = 0;
    for (let row = 0; row < candidateRowCount; row += 1) {
      if (
        predicates.every((predicate) =>
          vectorPredicateMatches(
            requiredColumnVector(candidateVectors, predicate.column),
            row,
            predicate,
          ),
        )
      ) {
        selectedRows[selectedRowCount] = row;
        selectedRowCount += 1;
      }
    }
    for (const column of projectedColumns) {
      if (candidateVectors.has(column.id)) continue;
      candidateVectors.set(
        column.id,
        selectedRowCount === 0
          ? createEmptyColumnVector(column.type, 0)
          : await this.#materializeAppendColumnVector(
              column,
              candidateSegments,
              snapshot,
              candidateRowCount,
              storedBlocks,
              expectedRows,
            ),
      );
    }
    const columns = new Map<string, ColumnVector>();
    for (const column of projectedColumns) {
      const candidate = requiredColumnVector(candidateVectors, column);
      if (selectedRowCount === candidateRowCount) {
        columns.set(column.name, candidate);
        continue;
      }
      const output = createEmptyColumnVector(column.type, selectedRowCount);
      const dictionaryIndex = column.type === "string" ? new Map<string, number>() : undefined;
      for (let outputRow = 0; outputRow < selectedRowCount; outputRow += 1) {
        copyColumnVectorValue(
          candidate,
          selectedRows[outputRow] ?? 0,
          output,
          outputRow,
          dictionaryIndex,
        );
      }
      columns.set(column.name, output);
    }
    const keyColumn = getUniqueKeyColumn(table);
    const projectedKey =
      keyColumn !== undefined && projectedColumns.some((column) => column.id === keyColumn.id)
        ? keyColumn.name
        : undefined;
    return {
      name: table.name,
      rowCount: selectedRowCount,
      columns,
      ...(projectedKey ? { uniqueKey: projectedKey } : {}),
    };
  }

  async #materializeAppendColumnVector(
    column: TableColumnRecord,
    segments: readonly SegmentRecord[],
    snapshot: LeasedSnapshot,
    rowCount: number,
    storedBlocks?: Map<string, Uint8Array>,
    expectedRows?: ReadonlyMap<string, number>,
  ): Promise<ColumnVector> {
    const validity = new Uint8Array(Math.ceil(rowCount / 8));
    const values =
      column.type === "boolean"
        ? new Uint8Array(rowCount)
        : column.type === "number" || column.type === "datetime"
          ? new Float64Array(rowCount)
          : undefined;
    const stringCodes = column.type === "string" ? new Uint32Array(rowCount) : undefined;
    stringCodes?.fill(NULL_STRING_VECTOR_CODE);
    const stringDictionary = new StringDictionaryBuilder();
    let outputRow = 0;

    for (const segment of segments) {
      const blockIds = segment.columnBlockIds[column.id];
      if (blockIds === undefined) {
        // The column joined the catalog after this segment was written; its rows read as NULL
        // through the preallocated validity default. A present-but-empty block list still fails
        // the row-count check below, preserving the corruption guard.
        outputRow += segment.rowCount;
        continue;
      }
      let segmentRows = 0;
      for (let start = 0; start < blockIds.length; start += 16) {
        await this.#renewInternalLeaseIfNeeded(snapshot);
        const ids = blockIds.slice(start, start + 16);
        // Block content is immutable, so the decoded physical form caches by id alone: after a
        // commit changes the visible segment set (which invalidates every assembled vector
        // fingerprint), the unchanged blocks skip the store fetch, decompression, checksum,
        // and payload validation, and re-preparation only pays vector assembly.
        const decodedBlocks = new Array<DecodedPhysicalBlock | undefined>(ids.length);
        const pendingIndexes: number[] = [];
        const fetchIds: string[] = [];
        for (let index = 0; index < ids.length; index += 1) {
          const id = ids[index] ?? "";
          const cached = this.#cacheGet(`dpb\u0000${id}`) as DecodedPhysicalBlock | undefined;
          if (cached !== undefined) {
            decodedBlocks[index] = cached;
            continue;
          }
          pendingIndexes.push(index);
          if (storedBlocks?.has(id) !== true) fetchIds.push(id);
        }
        const bytesById = storedBlocks ?? new Map<string, Uint8Array>();
        if (fetchIds.length > 0) {
          const fetched = await this.store.getBlocks(fetchIds);
          for (let index = 0; index < fetchIds.length; index += 1) {
            const id = fetchIds[index] ?? "";
            const bytes = fetched[index];
            if (bytes === undefined) throw new Error(`Visible block is missing: ${id}`);
            bytesById.set(id, bytes);
          }
        }
        for (const index of pendingIndexes) {
          const id = ids[index] ?? "";
          const bytes = bytesById.get(id);
          if (bytes === undefined) throw new Error(`Visible block is missing: ${id}`);
          const decoded = await decodePhysicalBlock(bytes);
          this.#cachePut(`dpb\u0000${id}`, decoded, bytes.byteLength);
          decodedBlocks[index] = decoded;
        }
        for (let index = 0; index < decodedBlocks.length; index += 1) {
          const decoded = decodedBlocks[index];
          if (decoded === undefined) {
            throw new Error(`Visible block is missing: ${ids[index] ?? ""}`);
          }
          const expected = expectedRows?.get(ids[index] ?? "");
          if (expected !== undefined && decoded.column.rowCount !== expected) {
            throw new Error(`Column block row count mismatch: ${column.name}`);
          }
          if (decoded.column.type !== column.type) {
            throw new Error(`Column type mismatch: ${column.name}`);
          }
          if (outputRow + decoded.column.rowCount > rowCount) {
            throw new Error(`Column row count mismatch: ${column.name}`);
          }
          appendPhysicalColumnToVector(
            decoded.column,
            outputRow,
            validity,
            values,
            stringCodes,
            stringDictionary,
          );
          outputRow += decoded.column.rowCount;
          segmentRows += decoded.column.rowCount;
        }
      }
      if (segmentRows !== segment.rowCount) {
        throw new Error(`Column row count mismatch: ${column.name}`);
      }
    }
    if (outputRow !== rowCount) throw new Error(`Column row count mismatch: ${column.name}`);
    if (column.type === "string") {
      return {
        kind: "string",
        length: rowCount,
        validity,
        codes: stringCodes ?? new Uint32Array(),
        dictionary: stringDictionary.dictionary,
      };
    }
    if (column.type === "boolean") {
      return {
        kind: "boolean",
        length: rowCount,
        validity,
        values: values as Uint8Array,
      };
    }
    return {
      kind: column.type,
      length: rowCount,
      validity,
      values: values as Float64Array,
    };
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
    const blockIds = segment.columnBlockIds[column.id];
    if (blockIds === undefined) {
      // The column joined the catalog after this segment was written; its rows read as NULL.
      return Array.from({ length: segment.rowCount }, () => null);
    }
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

  async #visibleSegmentRecords(
    table: TableRecord,
    snapshot: Snapshot,
    catalog?: SegmentVisibilityCatalog,
  ): Promise<SegmentRecord[]> {
    // One prepare asks for the same table's visible segments several times (statistics,
    // fingerprints, materialization); the visibility catalog is per-prepare state, so the
    // filtered-and-sorted result memoizes on it, like #visibilityFingerprints.
    if (catalog !== undefined) {
      let memo = this.#visibleSegmentsMemo.get(catalog);
      if (memo === undefined) {
        memo = new Map<string, SegmentRecord[]>();
        this.#visibleSegmentsMemo.set(catalog, memo);
      }
      const key = `${table.id}\u0000${String(snapshot.version ?? -1)}`;
      const cached = memo.get(key);
      if (cached !== undefined) return cached;
      const computed = await this.#visibleSegmentRecordsUncached(table, snapshot, catalog);
      memo.set(key, computed);
      return computed;
    }
    return this.#visibleSegmentRecordsUncached(table, snapshot, catalog);
  }

  async #visibleSegmentRecordsUncached(
    table: TableRecord,
    snapshot: Snapshot,
    catalog?: SegmentVisibilityCatalog,
  ): Promise<SegmentRecord[]> {
    const segments =
      catalog?.segmentsByTable.get(table.id) ?? (await this.store.listSegments(table.id));
    const transactions =
      catalog?.transactions ??
      new Map(
        (await this.#transactionRecordsForSegments(segments)).map((record) => [record.id, record]),
      );
    return segments
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

  async #transactionRecordsForSegments(
    segments: readonly SegmentRecord[],
  ): Promise<TransactionRecord[]> {
    const transactionIds = [...new Set(segments.map((segment) => segment.transactionId))];
    const records: TransactionRecord[] = [];
    for (let start = 0; start < transactionIds.length; start += 64) {
      const window = transactionIds.slice(start, start + 64);
      const loaded = await this.store.getTransactions(window);
      for (const record of loaded) if (record !== undefined) records.push(record);
    }
    return records;
  }

  async #unprunedManifestContainsAll(blockIds: readonly string[]): Promise<boolean> {
    let cursor: number | null = null;
    do {
      const page = await this.store.listManifestPage(cursor, 8);
      for (const manifest of page.records) {
        if (manifest.prunedAt !== undefined) continue;
        const visibleBlockIds = new Set(manifest.blockIds);
        if (blockIds.every((id) => visibleBlockIds.has(id))) return true;
      }
      cursor = page.nextCursor;
    } while (cursor !== null);
    return false;
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
    if (snapshot === this.#sharedLease?.lease) {
      // Overlapping prepares share one lease record; concurrent renewals would race the
      // lease's revision compare-and-swap, so they share one in-flight renewal instead.
      this.#sharedLeaseRenewal ??= snapshot
        .renew(INTERNAL_READ_LEASE_TTL_MS)
        .then(() => undefined)
        .finally(() => {
          this.#sharedLeaseRenewal = undefined;
        });
      await this.#sharedLeaseRenewal;
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
    private readonly database: MinnowDatabase,
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
    const operation =
      this.#mode === "upsert"
        ? this.database.upsertBatch(this.tableName, rows)
        : this.database.insertBatch(this.tableName, rows);
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
      void this.#flushPending().catch((error: unknown) => this.#onError?.(error));
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

/** True when a compiled block groups rows, either explicitly or through select aggregates. */
function compiledPlanIsGrouped(plan: CompiledQuery): boolean {
  return plan.groupBy.length > 0 || plan.select.some((item) => hasAggregate(item.expression));
}

function validateName(name: string, kind: string): string {
  const trimmed = name.trim();
  if (trimmed.length === 0) throw new TypeError(`${kind} name cannot be empty`);
  return trimmed;
}

function validateBatch(table: TableRecord, input: ColumnarBatch, pendingColumn?: string): number {
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
  for (const column of table.columns) {
    const values = input.columns[column.name] ?? [];
    if (values.length !== rowCount) throw new TypeError("All columns must have the same row count");
    // The pending auto-increment column keeps its null slots until the write path reserves a
    // range from storage, so nulls pass here; every explicit value still type-checks.
    const allowNull = column.name === pendingColumn;
    // An index loop rather than forEach: forEach skips holes in sparse arrays, which would let an
    // undefined value reach the block encoder and persist a corrupt block that only fails at read
    // time. Every slot is validated, holes included.
    for (let index = 0; index < values.length; index += 1) {
      const value = values[index] as BatchValue;
      if (allowNull && value === null) continue;
      validateValue(column, value, index);
    }
  }
  return rowCount;
}

/** Folds the storage-generated auto-increment vector into the generated-columns result map. */
function collectAutoIncrementGenerated(
  batch: ColumnarBatch,
  generated: Map<string, BatchValue[]>,
  autoIncrement: AutoIncrementFill | undefined,
): void {
  if (autoIncrement === undefined || autoIncrement.missingIndexes.length === 0) return;
  generated.set(autoIncrement.column.name, [...(batch.columns[autoIncrement.column.name] ?? [])]);
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
  if (
    column.enumValues !== undefined &&
    typeof value === "string" &&
    !column.enumValues.includes(value)
  ) {
    throw new TypeError(
      `${column.name}[${String(index)}] must be one of: ${column.enumValues.join(", ")}`,
    );
  }
}

/** Merges per-column sorted row-id arrays into one sorted, de-duplicated array. */
function unionSortedRowIds(arrays: ReadonlyArray<readonly bigint[]>): bigint[] {
  const nonEmpty = arrays.filter((array) => array.length > 0);
  if (nonEmpty.length === 0) return [];
  if (nonEmpty.length === 1) return [...(nonEmpty[0] ?? [])];
  // Pairwise linear merges: inputs are already sorted, so no comparison sort is needed.
  let merged: readonly bigint[] = nonEmpty[0] ?? [];
  for (let index = 1; index < nonEmpty.length; index += 1) {
    const other = nonEmpty[index] ?? [];
    const result: bigint[] = [];
    let leftIndex = 0;
    let rightIndex = 0;
    while (leftIndex < merged.length && rightIndex < other.length) {
      const left = merged[leftIndex] ?? 0n;
      const right = other[rightIndex] ?? 0n;
      const next = left <= right ? left : right;
      if (left <= right) leftIndex += 1;
      if (right <= left) rightIndex += 1;
      if (result.length === 0 || result[result.length - 1] !== next) result.push(next);
    }
    while (leftIndex < merged.length) {
      const value = merged[leftIndex] ?? 0n;
      if (result.length === 0 || result[result.length - 1] !== value) result.push(value);
      leftIndex += 1;
    }
    while (rightIndex < other.length) {
      const value = other[rightIndex] ?? 0n;
      if (result.length === 0 || result[result.length - 1] !== value) result.push(value);
      rightIndex += 1;
    }
    merged = result;
  }
  return merged as bigint[];
}

/** Two-pointer intersection of sorted row-id arrays. */
function intersectSortedRowIds(left: readonly bigint[], right: readonly bigint[]): bigint[] {
  const result: bigint[] = [];
  let leftIndex = 0;
  let rightIndex = 0;
  while (leftIndex < left.length && rightIndex < right.length) {
    const a = left[leftIndex] ?? 0n;
    const b = right[rightIndex] ?? 0n;
    if (a === b) {
      result.push(a);
      leftIndex += 1;
      rightIndex += 1;
    } else if (a < b) {
      leftIndex += 1;
    } else {
      rightIndex += 1;
    }
  }
  return result;
}

/** Whether any sorted row id falls inside [start, endExclusive) — one binary search. */
function sortedRowIdsIntersectRange(
  sorted: readonly bigint[],
  start: bigint,
  endExclusive: bigint,
): boolean {
  let low = 0;
  let high = sorted.length;
  while (low < high) {
    const middle = (low + high) >>> 1;
    if ((sorted[middle] ?? 0n) < start) low = middle + 1;
    else high = middle;
  }
  return low < sorted.length && (sorted[low] ?? 0n) < endExclusive;
}

/**
 * Tokenizes one cell into the term accumulator, tracking per-document term frequency, and
 * returns the cell's token count so producers can total the column's tokens for BM25 stats.
 */
function addFtsDocument(
  byTerm: Map<string, { rowIds: bigint[]; tf: number[] }>,
  value: BatchValue,
  rowId: bigint,
): number {
  const rendered = renderDocumentValue(value);
  if (rendered === undefined) return 0;
  const tokens = ftsTokenize(rendered);
  const counts = new Map<string, number>();
  for (const token of tokens) counts.set(token, (counts.get(token) ?? 0) + 1);
  for (const [term, tf] of counts) {
    const posting = byTerm.get(term) ?? { rowIds: [], tf: [] };
    posting.rowIds.push(rowId);
    posting.tf.push(tf);
    byTerm.set(term, posting);
  }
  return tokens.length;
}

function sortedFtsPostings(byTerm: Map<string, { rowIds: bigint[]; tf: number[] }>): FtsPosting[] {
  return [...byTerm.entries()]
    .sort(([left], [right]) => (left < right ? -1 : left > right ? 1 : 0))
    .map(([term, posting]) => ({ term, rowIds: posting.rowIds, tf: posting.tf }));
}

/** Term-range partitioning for the base chunks; 128 terms per chunk keeps records small. */
function chunkFtsPostings(postings: FtsPosting[], size = 128): FtsPosting[][] {
  const chunks: FtsPosting[][] = [];
  for (let start = 0; start < postings.length; start += size) {
    chunks.push(postings.slice(start, start + size));
  }
  return chunks;
}

/** Tokenizes an insert batch's values for every active full-text column into commit deltas. */
function buildFtsColumnDeltas(
  table: TableRecord,
  input: ColumnarBatch,
  rowIdStart: bigint,
): FtsColumnDelta[] {
  const active = Object.entries(table.ftsColumns ?? {}).filter(
    ([, record]) => record.state !== "invalid",
  );
  if (active.length === 0) return [];
  const columnsById = new Map(table.columns.map((column) => [column.id, column] as const));
  return active.flatMap(([columnId]) => {
    const column = columnsById.get(columnId);
    if (column === undefined) return [];
    const byTerm = new Map<string, { rowIds: bigint[]; tf: number[] }>();
    let totalTokens = 0;
    (input.columns[column.name] ?? []).forEach((value, index) => {
      totalTokens += addFtsDocument(byTerm, value, rowIdStart + BigInt(index));
    });
    return [{ columnId, postings: sortedFtsPostings(byTerm), totalTokens }];
  });
}

/** One column's postings read: candidates plus the freshness/fold metadata callers gate on. */
type FtsCandidatesResult = Awaited<ReturnType<BlockStore["readFtsCandidates"]>>;

/** The columns a MATCH(*) document draws from: everything except booleans. */
function searchableFtsColumns(table: TableRecord | undefined): readonly string[] | undefined {
  if (table === undefined) return undefined;
  return table.columns.filter((column) => column.type !== "boolean").map((column) => column.name);
}

function getUniqueKeyColumn(table: TableRecord): TableColumnRecord | undefined {
  if (table.uniqueKeyColumnId === undefined) return undefined;
  return table.columns.find((column) => column.id === table.uniqueKeyColumnId);
}

function batchKeys(
  table: TableRecord,
  input: ColumnarBatch,
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

const INITIAL_DICTIONARY_CAPACITY = 8;
const INITIAL_DICTIONARY_ARENA_BYTES = 256;

/**
 * Interns decoded strings by their raw UTF-8 bytes. The columns that grouping and filtering
 * target repeat the same byte ranges across millions of rows, so probing an owned byte arena
 * holds `TextDecoder` calls to one per distinct value rather than one per row. Entries live in
 * flat typed arrays so a high-cardinality column pays no per-value object overhead.
 */
class StringDictionaryBuilder {
  readonly dictionary: string[] = [];
  #buckets = filledInt32Array(INITIAL_DICTIONARY_CAPACITY);
  #next = filledInt32Array(INITIAL_DICTIONARY_CAPACITY);
  #hashes = new Uint32Array(INITIAL_DICTIONARY_CAPACITY);
  #offsets = new Uint32Array(INITIAL_DICTIONARY_CAPACITY);
  #lengths = new Uint32Array(INITIAL_DICTIONARY_CAPACITY);
  #arena = new Uint8Array(INITIAL_DICTIONARY_ARENA_BYTES);
  #arenaLength = 0;

  /** UTF-8 bytes held by the distinct values, tracked so callers skip a re-encoding pass. */
  get byteLength(): number {
    return this.#arenaLength;
  }

  codeForBytes(content: Uint8Array, start: number, end: number): number {
    const length = end - start;
    const hash = hashByteRange(content, start, end);
    let index = this.#buckets[hash & (this.#buckets.length - 1)] ?? -1;
    while (index >= 0) {
      if (
        this.#hashes[index] === hash &&
        this.#lengths[index] === length &&
        equalByteRange(this.#arena, this.#offsets[index] ?? 0, content, start, length)
      ) {
        return index;
      }
      index = this.#next[index] ?? -1;
    }
    return this.#insert(content, start, end, hash);
  }

  #insert(content: Uint8Array, start: number, end: number, hash: number): number {
    const code = this.dictionary.length;
    const length = end - start;
    this.#growEntries(code + 1);
    this.#growArena(this.#arenaLength + length);
    this.#arena.set(content.subarray(start, end), this.#arenaLength);
    this.#hashes[code] = hash;
    this.#offsets[code] = this.#arenaLength;
    this.#lengths[code] = length;
    this.#arenaLength += length;
    const bucket = hash & (this.#buckets.length - 1);
    this.#next[code] = this.#buckets[bucket] ?? -1;
    this.#buckets[bucket] = code;
    this.dictionary.push(vectorTextDecoder.decode(content.subarray(start, end)));
    return code;
  }

  #growEntries(required: number): void {
    if (required <= this.#hashes.length) return;
    let capacity = this.#hashes.length;
    while (capacity < required)
      capacity = safeWholeNumberProduct(capacity, 2, "String dictionary entries");
    const hashes = new Uint32Array(capacity);
    const offsets = new Uint32Array(capacity);
    const lengths = new Uint32Array(capacity);
    hashes.set(this.#hashes);
    offsets.set(this.#offsets);
    lengths.set(this.#lengths);
    this.#hashes = hashes;
    this.#offsets = offsets;
    this.#lengths = lengths;
    // Buckets track entry capacity, so chains stay short as the distinct-value count grows.
    this.#buckets = filledInt32Array(capacity);
    this.#next = filledInt32Array(capacity);
    for (let code = 0; code < this.dictionary.length; code += 1) {
      const bucket = (this.#hashes[code] ?? 0) & (capacity - 1);
      this.#next[code] = this.#buckets[bucket] ?? -1;
      this.#buckets[bucket] = code;
    }
  }

  #growArena(required: number): void {
    if (required <= this.#arena.byteLength) return;
    let capacity = this.#arena.byteLength;
    while (capacity < required)
      capacity = safeWholeNumberProduct(capacity, 2, "String dictionary arena");
    const arena = new Uint8Array(capacity);
    arena.set(this.#arena.subarray(0, this.#arenaLength));
    this.#arena = arena;
  }
}

function filledInt32Array(length: number): Int32Array {
  const array = new Int32Array(length);
  array.fill(-1);
  return array;
}

function hashByteRange(bytes: Uint8Array, start: number, end: number): number {
  let hash = 0x811c9dc5;
  for (let index = start; index < end; index += 1) {
    hash = Math.imul(hash ^ (bytes[index] ?? 0), 0x01000193) >>> 0;
  }
  return hash;
}

function equalByteRange(
  arena: Uint8Array,
  arenaOffset: number,
  content: Uint8Array,
  contentStart: number,
  length: number,
): boolean {
  for (let index = 0; index < length; index += 1) {
    if (arena[arenaOffset + index] !== content[contentStart + index]) return false;
  }
  return true;
}

/** Reusable copy arena that realigns misaligned float64 payloads for one bulk typed copy. */
let alignmentScratch = new ArrayBuffer(0);

function appendPhysicalColumnToVector(
  column: ValidatedPhysicalColumn,
  outputRowStart: number,
  outputValidity: Uint8Array,
  outputValues: Uint8Array | Float64Array | undefined,
  outputStringCodes: Uint32Array | undefined,
  stringDictionary: StringDictionaryBuilder,
): void {
  const sourceValidityLength = Math.ceil(column.rowCount / 8);
  const sourceValidity = column.bytes.subarray(0, sourceValidityLength);
  if (outputRowStart % 8 === 0) {
    // Byte-aligned append: the physical format zeroes every padding bit past rowCount (enforced
    // by validatePhysicalColumn), so copying whole bytes cannot leak stray validity into rows a
    // later block will own.
    outputValidity.set(sourceValidity, outputRowStart / 8);
  } else {
    for (let row = 0; row < column.rowCount; row += 1) {
      if (bitmapHasValue(sourceValidity, row)) {
        setBitmapValue(outputValidity, outputRowStart + row);
      }
    }
  }

  if (column.type === "boolean") {
    if (!(outputValues instanceof Uint8Array)) throw new Error("Boolean vector is missing");
    const sourceValues = column.bytes.subarray(sourceValidityLength);
    for (let row = 0; row < column.rowCount; row += 1) {
      if (bitmapHasValue(sourceValues, row)) outputValues[outputRowStart + row] = 1;
    }
    return;
  }
  if (column.type === "number" || column.type === "datetime") {
    if (!(outputValues instanceof Float64Array))
      throw new Error(`${column.type} vector is missing`);
    const valueOffset = column.bytes.byteOffset + sourceValidityLength;
    if (PLATFORM_LITTLE_ENDIAN) {
      // Values at invalid slots are never read (every consumer checks the validity bitmap
      // first), so bulk-copying them is safe. A misaligned payload — the common case, since the
      // validity prefix rarely ends on an 8-byte boundary — realigns through a reusable scratch
      // buffer: two memcpys still beat a per-row DataView loop by a wide margin.
      if (valueOffset % Float64Array.BYTES_PER_ELEMENT === 0) {
        outputValues.set(
          new Float64Array(column.bytes.buffer, valueOffset, column.rowCount),
          outputRowStart,
        );
        return;
      }
      const byteLength = column.rowCount * Float64Array.BYTES_PER_ELEMENT;
      if (alignmentScratch.byteLength < byteLength) {
        alignmentScratch = new ArrayBuffer(byteLength);
      }
      const scratchBytes = new Uint8Array(alignmentScratch, 0, byteLength);
      scratchBytes.set(new Uint8Array(column.bytes.buffer, valueOffset, byteLength));
      outputValues.set(new Float64Array(alignmentScratch, 0, column.rowCount), outputRowStart);
      return;
    }
    const sourceValues = new DataView(column.bytes.buffer, valueOffset, column.rowCount * 8);
    for (let row = 0; row < column.rowCount; row += 1) {
      if (bitmapHasValue(sourceValidity, row)) {
        outputValues[outputRowStart + row] = sourceValues.getFloat64(row * 8, true);
      }
    }
    return;
  }
  if (outputStringCodes === undefined) throw new Error("String vector is missing");
  const offsetsLength = (column.rowCount + 1) * 4;
  const offsets = new DataView(
    column.bytes.buffer,
    column.bytes.byteOffset + sourceValidityLength,
    offsetsLength,
  );
  const content = column.bytes.subarray(sourceValidityLength + offsetsLength);
  for (let row = 0; row < column.rowCount; row += 1) {
    if (!bitmapHasValue(sourceValidity, row)) continue;
    const start = offsets.getUint32(row * 4, true);
    const end = offsets.getUint32((row + 1) * 4, true);
    outputStringCodes[outputRowStart + row] = stringDictionary.codeForBytes(content, start, end);
  }
}

/**
 * A streamed vector starts with an empty resident window; its logical length is the full table so
 * bound plans see the true scan cardinality before the first load.
 */
function createStreamedColumnVector(type: SimpleDataType, length: number): ColumnVector {
  const window: VectorWindow = { start: 0, length: 0 };
  const validity = new Uint8Array(0);
  if (type === "boolean") {
    return { kind: type, length, validity, values: new Uint8Array(0), window };
  }
  if (type === "number" || type === "datetime") {
    return { kind: type, length, validity, values: new Float64Array(0), window };
  }
  return { kind: type, length, validity, codes: new Uint32Array(0), dictionary: [], window };
}

interface MutableStreamedVectorFields {
  validity: Uint8Array;
  window: VectorWindow;
  values?: Uint8Array | Float64Array;
  codes?: Uint32Array;
  dictionary?: string[];
}

/**
 * Replaces a streamed vector's resident window with freshly decoded rows. Fixed-width bytes are
 * reserved before allocation; per-window string dictionary bytes are measured during decoding and
 * reserved before the window is installed. The caller releases the superseded reservations.
 */
function installStreamedWindow(
  vector: ColumnVector,
  resident: ReadonlyArray<{ column: ValidatedPhysicalColumn; startRow: number }>,
  windowStart: number,
  windowRows: number,
  memory: QueryMemoryContext,
  label: string,
  reservations: QueryMemoryReservation[],
): void {
  const validityBytes = Math.ceil(windowRows / 8);
  const typedBytes =
    validityBytes +
    (vector.kind === "boolean"
      ? windowRows
      : vector.kind === "string"
        ? windowRows * Uint32Array.BYTES_PER_ELEMENT
        : windowRows * Float64Array.BYTES_PER_ELEMENT);
  reservations.push(memory.reserve(typedBytes, label));
  const validity = new Uint8Array(validityBytes);
  const values =
    vector.kind === "boolean"
      ? new Uint8Array(windowRows)
      : vector.kind === "number" || vector.kind === "datetime"
        ? new Float64Array(windowRows)
        : undefined;
  const codes = vector.kind === "string" ? new Uint32Array(windowRows) : undefined;
  codes?.fill(NULL_STRING_VECTOR_CODE);
  const builder = new StringDictionaryBuilder();
  for (const block of resident) {
    appendPhysicalColumnToVector(
      block.column,
      block.startRow - windowStart,
      validity,
      values,
      codes,
      builder,
    );
  }
  const dictionary = builder.dictionary;
  // The builder already retains each distinct value's UTF-8 bytes, so the window's dictionary
  // footprint is known without re-encoding every entry.
  if (dictionary.length > 0) reservations.push(memory.reserve(builder.byteLength, label));
  const mutable = vector as unknown as MutableStreamedVectorFields;
  mutable.validity = validity;
  if (values !== undefined) mutable.values = values;
  if (codes !== undefined) {
    mutable.codes = codes;
    mutable.dictionary = dictionary;
  }
  mutable.window = { start: windowStart, length: windowRows };
}

/**
 * Cost-based build-side selection using exact prepared row counts: a single inner equi-join
 * probes from the scanned base into a built index over the joined table, so when the joined
 * table is much larger than the base the two swap, building over the smaller input instead.
 * Left joins are asymmetric and wildcard selects expose source order, so both keep the written
 * order; multi-join reordering remains open.
 */
export function chooseJoinOrder(
  plan: CompiledQuery,
  inputs: ReadonlyMap<string, ColumnarTable>,
): CompiledQuery {
  const join = plan.joins[0];
  if (
    plan.joins.length !== 1 ||
    join?.kind !== "inner" ||
    join.on !== undefined ||
    plan.select[0]?.expression.kind === "wildcard"
  ) {
    return plan;
  }
  const baseRows = inputs.get(plan.base.table)?.rowCount ?? 0;
  const joinRows = inputs.get(join.table)?.rowCount ?? 0;
  if (joinRows <= baseRows * 2) return plan;
  const { kind, left, right, ...joinSource } = join;
  void kind;
  return {
    ...plan,
    base: joinSource,
    joins: [{ ...plan.base, kind: "inner", left, right }],
  };
}

/** Converts an executed derived-block result into a typed columnar input table. */
function derivedColumnarTable(
  name: string,
  result: QueryResult,
  schema: readonly SqlColumnSchema[],
): ColumnarTable {
  const columns = new Map(
    schema.map(({ name: columnName, type }) => [
      columnName,
      { type, values: result.rows.map((row) => row[columnName] ?? null) },
    ]),
  );
  return createColumnarTable(name, columns);
}

function createEmptyColumnVector(type: SimpleDataType, length: number): ColumnVector {
  const validity = new Uint8Array(Math.ceil(length / 8));
  if (type === "boolean") {
    return { kind: type, length, validity, values: new Uint8Array(length) };
  }
  if (type === "number" || type === "datetime") {
    return { kind: type, length, validity, values: new Float64Array(length) };
  }
  const codes = new Uint32Array(length);
  codes.fill(NULL_STRING_VECTOR_CODE);
  return { kind: type, length, validity, codes, dictionary: [] };
}

function requiredColumnVector(
  vectors: ReadonlyMap<string, ColumnVector>,
  column: TableColumnRecord,
  segment?: SegmentRecord,
): ColumnVector {
  const vector = vectors.get(column.id);
  if (vector === undefined) {
    throw new Error(
      segment === undefined
        ? `Projected column is missing: ${column.name}`
        : `Visible column is missing: ${segment.id}.${column.name}`,
    );
  }
  return vector;
}

function copyColumnVectorValue(
  source: ColumnVector,
  sourceRow: number,
  target: ColumnVector,
  targetRow: number,
  targetDictionaryIndex?: Map<string, number>,
): void {
  if (source.kind !== target.kind) throw new Error("Column vector type mismatch");
  if (!bitmapHasValue(source.validity, sourceRow)) {
    clearBitmapValue(target.validity, targetRow);
    if (target.kind === "string") target.codes[targetRow] = NULL_STRING_VECTOR_CODE;
    else target.values[targetRow] = 0;
    return;
  }
  setBitmapValue(target.validity, targetRow);
  if (source.kind === "boolean" && target.kind === "boolean") {
    target.values[targetRow] = source.values[sourceRow] ?? 0;
    return;
  }
  if (
    (source.kind === "number" || source.kind === "datetime") &&
    (target.kind === "number" || target.kind === "datetime")
  ) {
    target.values[targetRow] = source.values[sourceRow] ?? 0;
    return;
  }
  if (source.kind !== "string" || target.kind !== "string" || targetDictionaryIndex === undefined) {
    throw new Error("String vector dictionary is missing");
  }
  const sourceCode = source.codes[sourceRow] ?? NULL_STRING_VECTOR_CODE;
  const value = source.dictionary[sourceCode];
  if (value === undefined) throw new Error("String vector code is invalid");
  let targetCode = targetDictionaryIndex.get(value);
  if (targetCode === undefined) {
    targetCode = target.dictionary.length;
    (target.dictionary as string[]).push(value);
    targetDictionaryIndex.set(value, targetCode);
  }
  target.codes[targetRow] = targetCode;
}

/** Rough per-row bookkeeping overhead used when tallying retained row objects. */
const QUERY_ROW_OVERHEAD_BYTES = 16;

/** Reads one value from a (possibly windowed) column vector as a query value. */
function columnVectorValueAt(vector: ColumnVector, rowIndex: number): QueryValue {
  const window = vector.window;
  let slot = rowIndex;
  if (window !== undefined) {
    slot = rowIndex - window.start;
    if (slot < 0 || slot >= window.length) {
      throw new RangeError("Streamed vector row is outside the resident window");
    }
  }
  if (!bitmapHasValue(vector.validity, slot)) return null;
  if (vector.kind === "boolean") return vector.values[slot] === 1;
  if (vector.kind === "number") return vector.values[slot] ?? 0;
  if (vector.kind === "datetime") return new Date(vector.values[slot] ?? 0);
  const code = vector.codes[slot] ?? NULL_STRING_VECTOR_CODE;
  return code === NULL_STRING_VECTOR_CODE ? null : (vector.dictionary[code] ?? null);
}

const joinHashScratch = new DataView(new ArrayBuffer(8));

/**
 * Hash-partitions a join key so values the executor's equi-join treats as equal share a
 * partition: datetimes hash as their epoch milliseconds exactly like numbers (the executor
 * compares them coerced), -0 folds to 0, and strings hash their UTF-16 code units. The
 * partition count is a power of two.
 */
function joinPartitionOf(value: Exclude<QueryValue, null>, partitions: number): number {
  let hash = 0x811c9dc5;
  const mix = (word: number): void => {
    hash = Math.imul(hash ^ word, 0x01000193) >>> 0;
  };
  if (typeof value === "string") {
    for (let index = 0; index < value.length; index += 1) mix(value.charCodeAt(index));
    mix(0x53);
  } else if (typeof value === "boolean") {
    mix(value ? 1 : 0);
    mix(0x42);
  } else {
    const numeric = value instanceof Date ? value.getTime() : value;
    joinHashScratch.setFloat64(0, numeric === 0 ? 0 : numeric, true);
    mix(joinHashScratch.getUint32(0, true));
    mix(joinHashScratch.getUint32(4, true));
    mix(0x4e);
  }
  // Multiplicative mixing only propagates input variation toward high bits (small integers as
  // float64 vary in no low word bits at all), so a final avalanche folds the high bits back
  // down before the low-bit partition mask.
  hash ^= hash >>> 16;
  hash = Math.imul(hash, 0x85ebca6b) >>> 0;
  hash ^= hash >>> 13;
  hash = Math.imul(hash, 0xc2b2ae35) >>> 0;
  hash ^= hash >>> 16;
  return (hash >>> 0) & (partitions - 1);
}

/**
 * Detects the partitioned-join eligible shape: exactly one inner equi-join with no ON residue,
 * ungrouped and unordered (grouped joins need partial aggregation, ordered joins a final
 * re-sort pass), non-wildcard select, and both join sides written as qualified bare columns —
 * one resolving to the joined table (the build side) and one to the base.
 */
function partitionedJoinShape(
  plan: CompiledQuery,
): { buildTableName: string; buildKeyName: string } | undefined {
  const join = plan.joins[0];
  if (plan.joins.length !== 1 || join === undefined) return undefined;
  if (join.kind !== "inner" || join.on !== undefined) return undefined;
  if (compiledPlanIsGrouped(plan) || plan.orderBy.length > 0) return undefined;
  if (plan.select.some((item) => item.expression.kind === "wildcard")) return undefined;
  const buildNames = new Set([join.table, join.alias]);
  const baseNames = new Set([plan.base.table, plan.base.alias]);
  const sideOf = (expression: Expression): "build" | "base" | undefined => {
    if (expression.kind !== "column") return undefined;
    const parts = expression.reference.split(".");
    if (parts.length !== 2) return undefined;
    if (buildNames.has(parts[0] ?? "")) return "build";
    if (baseNames.has(parts[0] ?? "")) return "base";
    return undefined;
  };
  const leftSide = sideOf(join.left);
  const rightSide = sideOf(join.right);
  const buildExpression =
    leftSide === "build" && rightSide === "base"
      ? join.left
      : leftSide === "base" && rightSide === "build"
        ? join.right
        : undefined;
  if (buildExpression?.kind !== "column") return undefined;
  const buildKeyName = buildExpression.reference.split(".")[1] ?? "";
  if (buildKeyName.length === 0) return undefined;
  return { buildTableName: join.table, buildKeyName };
}

/** Crude resident-size guess used only to pick a strategy; reservations gate real memory. */
function estimatedColumnarBytes(
  segments: readonly SegmentRecord[],
  columns: readonly TableColumnRecord[],
): number {
  let rows = 0;
  for (const segment of segments) {
    const kind = segment.kind ?? "insert";
    if (kind === "insert" || kind === "base") rows += segment.rowCount;
  }
  let rowWidth = 0;
  for (const column of columns) rowWidth += column.type === "string" ? 32 : 8;
  return rows * Math.max(rowWidth, 1);
}

function columnVectorKeyToken(type: SimpleDataType, vector: ColumnVector, row: number): string {
  if (!bitmapHasValue(vector.validity, row)) return keyToken(type, null);
  if (type === "boolean" && vector.kind === "boolean") {
    return keyToken(type, vector.values[row] === 1);
  }
  if (type === "number" && vector.kind === "number") {
    return keyToken(type, vector.values[row] ?? Number.NaN);
  }
  if (type === "datetime" && vector.kind === "datetime") {
    return keyToken(type, new Date(vector.values[row] ?? Number.NaN));
  }
  if (type === "string" && vector.kind === "string") {
    return keyToken(type, vector.dictionary[vector.codes[row] ?? NULL_STRING_VECTOR_CODE] ?? null);
  }
  throw new Error("Column vector type mismatch");
}

function zonePredicates(plan: CompiledQuery, table: TableRecord): ZonePredicate[] {
  if (plan.joins.length > 0 || plan.base.table !== table.name) return [];
  const output: ZonePredicate[] = [];
  const resolveColumn = (reference: string): TableColumnRecord | undefined => {
    const parts = reference.split(".");
    if (parts.length === 2 && parts[0] !== plan.base.alias && parts[0] !== table.name) {
      return undefined;
    }
    const name = parts.length === 2 ? parts[1] : parts[0];
    const column = table.columns.find((candidate) => candidate.name === name);
    return column?.type === "number" || column?.type === "datetime" ? column : undefined;
  };
  const comparisons = new Set<ComparisonOperator>(["=", "!=", "<>", ">", ">=", "<", "<="]);
  const asComparison = (operator: PredicateOperator): ComparisonOperator | undefined =>
    comparisons.has(operator as ComparisonOperator) ? (operator as ComparisonOperator) : undefined;
  for (const predicate of plan.predicates) {
    // Zone-map pruning understands plain comparisons only; every other operator scans.
    const comparisonOperator = asComparison(predicate.operator);
    if (comparisonOperator === undefined) continue;
    const leftColumn =
      predicate.left.kind === "column" ? resolveColumn(predicate.left.reference) : undefined;
    const rightColumn =
      predicate.right.kind === "column" ? resolveColumn(predicate.right.reference) : undefined;
    const literal = leftColumn === undefined ? predicate.left : predicate.right;
    const column = leftColumn ?? rightColumn;
    if (column === undefined || literal.kind !== "literal") continue;
    const value =
      column.type === "datetime"
        ? literal.value instanceof Date
          ? literal.value.getTime()
          : undefined
        : typeof literal.value === "number"
          ? literal.value
          : undefined;
    if (value === undefined || !Number.isFinite(value)) continue;
    output.push({
      column,
      operator:
        leftColumn === undefined ? reverseComparison(comparisonOperator) : comparisonOperator,
      value,
    });
  }
  return output;
}

function reverseComparison(operator: ComparisonOperator): ComparisonOperator {
  if (operator === ">") return "<";
  if (operator === ">=") return "<=";
  if (operator === "<") return ">";
  if (operator === "<=") return ">=";
  return operator;
}

function zoneMapCanMatch(
  description: ReturnType<typeof inspectBlock>,
  predicate: ZonePredicate,
): boolean {
  if (description.nullCount === description.rowCount) return false;
  const zoneMap = description.metadata.zoneMap;
  if (zoneMap === undefined) return true;
  if (predicate.operator === "=") {
    return predicate.value >= zoneMap.min && predicate.value <= zoneMap.max;
  }
  if (predicate.operator === "!=" || predicate.operator === "<>") {
    return zoneMap.min !== predicate.value || zoneMap.max !== predicate.value;
  }
  if (predicate.operator === ">") return zoneMap.max > predicate.value;
  if (predicate.operator === ">=") return zoneMap.max >= predicate.value;
  if (predicate.operator === "<") return zoneMap.min < predicate.value;
  return zoneMap.min <= predicate.value;
}

function vectorPredicateMatches(
  vector: ColumnVector,
  row: number,
  predicate: ZonePredicate,
): boolean {
  if (!bitmapHasValue(vector.validity, row)) return false;
  if (
    (predicate.column.type !== "number" || vector.kind !== "number") &&
    (predicate.column.type !== "datetime" || vector.kind !== "datetime")
  ) {
    throw new Error("Predicate vector type mismatch");
  }
  const value = vector.values[row] ?? Number.NaN;
  if (predicate.operator === "=") return value === predicate.value;
  if (predicate.operator === "!=" || predicate.operator === "<>") return value !== predicate.value;
  if (predicate.operator === ">") return value > predicate.value;
  if (predicate.operator === ">=") return value >= predicate.value;
  if (predicate.operator === "<") return value < predicate.value;
  return value <= predicate.value;
}

function bitmapHasValue(bitmap: Uint8Array, index: number): boolean {
  return ((bitmap[index >>> 3] ?? 0) & (1 << (index & 7))) !== 0;
}

function setBitmapValue(bitmap: Uint8Array, index: number): void {
  const byte = index >>> 3;
  bitmap[byte] = (bitmap[byte] ?? 0) | (1 << (index & 7));
}

function clearBitmapValue(bitmap: Uint8Array, index: number): void {
  const byte = index >>> 3;
  bitmap[byte] = (bitmap[byte] ?? 0) & ~(1 << (index & 7));
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

function appendRowIdSpan(spans: RowIdSpan[], rowStart: number, rowId: bigint): void {
  const previous = spans[spans.length - 1];
  if (
    previous !== undefined &&
    previous.rowStart + previous.rowCount === rowStart &&
    previous.rowIdStart + BigInt(previous.rowCount) === rowId
  ) {
    spans[spans.length - 1] = { ...previous, rowCount: previous.rowCount + 1 };
  } else {
    spans.push({ rowStart, rowCount: 1, rowIdStart: rowId });
  }
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

function appendMergeOutputRange(
  ranges: MergeCompactionOutputSourceRange[],
  outputRowStart: number,
  source: MergeResolvedSource,
): void {
  const previous = ranges[ranges.length - 1];
  if (
    previous?.sourceBlockId === source.blockId &&
    previous.outputRowStart + previous.rowCount === outputRowStart &&
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
          priorAttemptOutputStoredBytes: job.priorAttemptOutputStoredBytes ?? 0,
          lifetimeOutputStoredBytes:
            (job.priorAttemptOutputStoredBytes ?? 0) + job.outputStoredBytes,
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

/** One shared internal reader lease, reused across prepares at the same manifest version. */
interface SharedLeaseEntry {
  lease: LeasedSnapshot;
  version: number | null;
  refCount: number;
}

/** Collects every real table name referenced by a block, its derived sources, or its subqueries. */
function collectRealTableNames(plan: CompiledQuery): string[] {
  const names = new Set<string>();
  const excluded = new Set<string>();
  const walkExpression = (expression: Expression): void => {
    if (expression.kind === "subquery" || expression.kind === "exists") {
      walk(expression.block);
      return;
    }
    if (expression.kind === "binary" || expression.kind === "condition") {
      walkExpression(expression.left);
      walkExpression(expression.right);
    } else if (expression.kind === "logical") {
      walkExpression(expression.left);
      walkExpression(expression.right);
    } else if (expression.kind === "not") walkExpression(expression.operand);
    else if (expression.kind === "case") {
      for (const branch of expression.branches) {
        walkExpression(branch.when);
        walkExpression(branch.then);
      }
      if (expression.otherwise !== undefined) walkExpression(expression.otherwise);
    } else if (expression.kind === "call") expression.arguments.forEach(walkExpression);
    else if (expression.kind === "list") expression.items.forEach(walkExpression);
  };
  const walk = (block: CompiledQuery): void => {
    for (const source of [block.base, ...block.joins]) {
      if (source.union !== undefined) source.union.blocks.forEach(walk);
      else if (source.windowed !== undefined) walk(source.windowed.block);
      else if (source.recursive !== undefined) {
        // The self-reference is bound per iteration, never loaded from storage.
        excluded.add(source.recursive.reference);
        walk(source.recursive.base);
        walk(source.recursive.step);
      } else if (source.derived === undefined) {
        if (source.table !== DUAL_TABLE) names.add(source.table);
      } else walk(source.derived);
    }
    for (const item of block.select) walkExpression(item.expression);
    block.groupBy.forEach(walkExpression);
    for (const join of block.joins) {
      if (join.on !== undefined) walkExpression(join.on);
    }
    for (const predicate of [...block.predicates, ...block.having]) {
      walkExpression(predicate.left);
      walkExpression(predicate.right);
    }
    for (const order of block.orderBy) walkExpression(order.expression);
  };
  walk(plan);
  for (const name of excluded) names.delete(name);
  return [...names];
}

/** Retained payload of a cached column vector: typed arrays plus dictionary characters. */
function columnVectorRetainedBytes(vector: ColumnVector): number {
  const base = vector.validity.byteLength;
  if (vector.kind === "string") {
    let dictionaryBytes = 0;
    for (const value of vector.dictionary) dictionaryBytes += 16 + value.length * 2;
    return base + vector.codes.byteLength + dictionaryBytes;
  }
  return base + vector.values.byteLength;
}

/** Retained payload of a cached columnar table: its vectors plus fixed overhead. */
function columnarTableRetainedBytes(table: ColumnarTable): number {
  let bytes = 128;
  for (const vector of table.columns.values()) bytes += columnVectorRetainedBytes(vector);
  return bytes;
}

/** Retained payload of a cached block result: column names plus estimated row values. */
function queryResultRetainedBytes(result: QueryResult): number {
  let bytes = 64;
  for (const column of result.columns) bytes += 16 + column.length * 2;
  for (const row of result.rows) bytes += estimateRowBytes(row);
  return bytes;
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

interface KeyedLevelTwoLayout {
  levelTwoSegments: readonly SegmentRecord[];
  anchor: SegmentRecord | undefined;
  level0Segments: readonly SegmentRecord[];
}

/**
 * Validates a keyed table's visible history for multi-range L2 promotion: existing partitions
 * (append-shaped inserts or merged bases carrying row-ID spans) with ordinals exactly 0..N-1,
 * then an optional single level-one anchor, then level-zero segments of any mutation kind. Every
 * row footprint — a partition's spans or interval, the anchor's, and each level-zero
 * insert/upsert interval — must be pairwise disjoint; update and delete deltas carry no
 * footprint. Returns null when the shape does not hold so the planner skips explicitly.
 */
function keyedLevelTwoLayout(segments: readonly SegmentRecord[]): KeyedLevelTwoLayout | null {
  let index = 0;
  const levelTwoSegments: SegmentRecord[] = [];
  for (;;) {
    const segment = segments[index];
    if (segment === undefined || (segment.level ?? 0) !== 2) break;
    const kind = segment.kind ?? "insert";
    if (
      segment.partitionOrdinal !== levelTwoSegments.length ||
      !Number.isSafeInteger(segment.logicalOrder) ||
      (segment.logicalOrder ?? -1) < 0 ||
      (kind !== "insert" && kind !== "base") ||
      (kind === "insert" && segment.rowIdSpans !== undefined) ||
      (kind === "base" && (segment.rowIdSpans?.length ?? 0) === 0)
    ) {
      return null;
    }
    levelTwoSegments.push(segment);
    index += 1;
  }
  let anchor: SegmentRecord | undefined;
  const maybeAnchor = segments[index];
  if (maybeAnchor !== undefined && (maybeAnchor.level ?? 0) === 1) {
    const kind = maybeAnchor.kind ?? "insert";
    if ((kind !== "insert" && kind !== "base") || maybeAnchor.partitionOrdinal !== undefined) {
      return null;
    }
    anchor = maybeAnchor;
    index += 1;
  }
  const level0Segments = segments.slice(index);
  if (
    level0Segments.some(
      (segment) => (segment.level ?? 0) !== 0 || segment.partitionOrdinal !== undefined,
    )
  ) {
    return null;
  }
  const intervals: Array<{ start: bigint; end: bigint }> = [];
  for (const segment of segments) {
    if (segment.rowIdSpans !== undefined) {
      for (const span of segment.rowIdSpans) {
        intervals.push({ start: span.rowIdStart, end: span.rowIdStart + BigInt(span.rowCount) });
      }
      continue;
    }
    if (segment.rowIdEndExclusive <= segment.rowIdStart) continue;
    if (segment.rowIdEndExclusive - segment.rowIdStart !== BigInt(segment.rowCount)) return null;
    intervals.push({ start: segment.rowIdStart, end: segment.rowIdEndExclusive });
  }
  intervals.sort((left, right) =>
    left.start < right.start ? -1 : left.start > right.start ? 1 : 0,
  );
  for (const [intervalIndex, interval] of intervals.entries()) {
    if (interval.start <= 0n) return null;
    const previous = intervals[intervalIndex - 1];
    if (previous !== undefined && interval.start < previous.end) return null;
  }
  return { levelTwoSegments, anchor, level0Segments };
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
  priorAttemptOutputStoredBytes: number;
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
    priorAttemptOutputStoredBytes: input.priorAttemptOutputStoredBytes,
    lifetimeOutputStoredBytes: input.priorAttemptOutputStoredBytes,
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

function estimateBatchBytes(input: ColumnarBatch): number {
  return Object.values(input.columns).reduce(
    (total, values) => total + estimateValuesBytes(values),
    0,
  );
}

function estimateValuesBytes(values: readonly BatchValue[]): number {
  let bytes = 0;
  for (const value of values) {
    // One byte per UTF-16 code unit approximates the UTF-8 payload (exact for ASCII) without
    // encoding the string just to measure it — this estimate feeds metrics and flush
    // thresholds, not the physical format.
    if (typeof value === "string") bytes += 4 + value.length;
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
