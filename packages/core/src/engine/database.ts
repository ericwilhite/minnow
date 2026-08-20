import {
  toColumnarBatch,
  type BatchRow,
  type BatchValue,
  type ColumnarBatch,
  type InsertBatchInput,
} from "./batch.js";
import { ArtifactCache } from "./artifact-cache.js";
import { BufferedTableWriter, type BufferedWriterOptions } from "./buffered-writer.js";
export {
  attachLifecycleFlush,
  BufferedTableWriter,
  type BufferedFlushResult,
  type BufferedWriterOptions,
  type LifecycleDocumentTarget,
  type LifecycleFlushOptions,
  type LifecycleFlushRequester,
  type LifecyclePageTarget,
} from "./buffered-writer.js";
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
  type CatalogProbe,
  type ManifestSummary,
  type MergeCompactionOutputColumn,
  type MergeCompactionRewritePlan,
  type MergeCompactionSourceBlock,
  type MergeCompactionSourceColumn,
  type MergeCompactionSourceSegment,
  type MergeOutputPartition,
  type QueryCatalogState,
  type RowIdRange,
  type RechunkCompactionOutputWindow,
  type RechunkCompactionRewritePlan,
  type RechunkCompactionSourceBlock,
  type RechunkCompactionSourceColumn,
  type RowIdSpan,
  type SegmentKind,
  type SegmentRecord,
  type SimpleDataType,
  decodeSnapshot,
  encodeSnapshot,
  type SnapshotExportProgress,
  type SnapshotLoadProgress,
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
  compileCheckExpression,
  compileQuery,
  hasAggregate,
  createRecursiveCteState,
  compileStatement,
  createPreparedColumnarQuery,
  evaluateJoinedRowExpression,
  evaluateRowExpression,
  expressionColumnNames,
  inferBlockSchema,
  referencedColumns,
  childExpressions,
  expandFtsColumns,
  expandNaturalJoins,
  expandSourceColumnAliases,
  expandViewSources,
  forEachBlockExpression,
  planContainsFts,
  planHasNaturalJoins,
  planHasSourceColumnAliases,
  planReadsViews,
  planReadsBeyondSingleScan,
  projectResultColumns,
  subqueryResolutionSteps,
  topLevelFtsMatchConjuncts,
  transparentProjectionSource,
  windowOutputType,
  type ComparisonOperator,
  type PredicateOperator,
  type CompiledQuery,
  type CompiledStatement,
  type ForeignKeyDefinition,
  type Expression,
  type InsertValue,
  type PreparedQuery,
  type QueryResult,
  type QueryRow,
  type QueryValue,
  type SqlColumnSchema,
} from "./query.js";
import {
  copyQueryResult,
  planMemoKey,
  queryResultMemoKey,
  queryResultRetainedBytes,
  RESULT_MEMO_MAX_BYTES,
} from "./query-cache.js";
import {
  QueryMemoryBudgetError,
  QueryMemoryContext,
  type QueryMemoryReservation,
} from "./memory.js";
import { LiveQuerySet, type LiveQueryInput, type LiveQuerySetOptions } from "./live.js";
import { chooseJoinOrder, renderPlan } from "./optimizer.js";
import { toCatalog, type Catalog } from "./catalog.js";
import {
  applyColumnSteps,
  declaredForeignKeys,
  isDestructiveStep,
  planMigration,
  type AnyTable,
  type AnyView,
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
const DEFAULT_COMPACTION_MAXIMUM_LEVEL_ZERO_SEGMENTS = 64;
const DEFAULT_COMPACTION_MAXIMUM_LEVEL_ZERO_STORED_BYTES = 64 * 1024 * 1024;
/**
 * Rows a keyed fold aims to keep in one level-one partition. A fold rewrites only the
 * partitions its deltas touch, so this bounds how much one touched key costs to absorb; the
 * table's partition count, and with it the per-query block count, grows as rows divided by it.
 */
const DEFAULT_COMPACTION_PARTITION_ROWS = 16_384;
const DEFAULT_LEVEL_TWO_MAX_WRITE_AMPLIFICATION = 16;
const MAX_COMPACTION_TARGET_BLOCK_BYTES = 64 * 1024 * 1024;
const MAX_BLOCK_ENVELOPE_BYTES = 1024;
const INTERNAL_READ_LEASE_TTL_MS = 60_000;
/** Live proof windows kept resident; a sweep uses one, concurrent sets a few. */
const LIVE_PROOF_CONTEXT_LIMIT = 4;
/** Distinct table-name sets whose catalog state stays resident; entries are tiny (records only). */
const CATALOG_STATE_CACHE_LIMIT = 64;
/** Blocks fetched per round trip when a streamed scan window needs more data. */
const STREAMED_SCAN_LOOKAHEAD_BLOCKS = 8;
/** Visible segments per table at which a scan or a commit schedules a compaction step. */
const AUTO_COMPACT_SCAN_SEGMENTS = 48;
/** Visible delete/update segments at which a scan or a commit schedules a compaction step. */
const AUTO_COMPACT_DELTA_SEGMENTS = 32;
/** Commits to one table between auto-compaction checks on the write path. */
const AUTO_COMPACT_COMMIT_CHECK_INTERVAL = 8;
/** Quiet time after a write burst before checking its final, sub-interval tail. */
const AUTO_COMPACT_IDLE_CHECK_MS = 25;
/** Commits between background collection passes; each prunes the manifests they wrote. */
const AUTO_COLLECT_COMMIT_INTERVAL = 64;
/**
 * Manifest versions background collection leaves readable behind the current one, and how
 * old one may be before it is collected regardless. A version is kept only while both hold: the
 * count serves a reader that names a version it was just handed, the age keeps a burst of
 * commits from pinning everything it superseded until the next burst — an idle tab reclaims
 * within a minute.
 */
const AUTO_COLLECT_RETAINED_VERSIONS = 64;
const AUTO_COLLECT_RETAINED_VERSION_MS = 60_000;
/** A commit this long after the last collection pass starts one, whatever the commit count. */
const AUTO_COLLECT_QUIET_MS = 60_000;
/** Candidates one background collection step examines before yielding to the event loop. */
const AUTO_COLLECT_STEP_ITEMS = 64;
/** Passes one background collection run makes before handing the rest to the next trigger. */
const AUTO_COLLECT_MAX_PASSES = 32;
/** Finished job records of each kind a background run leaves for inspection. */
const AUTO_COLLECT_RETAINED_JOB_RECORDS = 8;
/** Output blocks one background compaction step writes before yielding to the event loop. */
const AUTO_COMPACT_STEP_BLOCKS = 4;
/**
 * Level-zero segments one background fold may absorb. A fold rewrites every partition its
 * deltas touch, and a partition touched by several deltas is rewritten once, so absorbing
 * everything pending in one pass costs one rewrite of those partitions where the default would
 * cost several; the stored-bytes ceiling still bounds the pass.
 */
const AUTO_COMPACT_MAX_LEVEL_ZERO_SEGMENTS = 256;
/** Modeled retained bytes for one cached block description (header metadata, no payload). */
const ZONE_DESCRIPTION_CACHE_BYTES = 160;

/** Overlay logical order for a write scope's staged segments: after all committed data. */
const STAGED_OVERLAY_ORDER_BASE = 2 ** 52;

/**
 * Internal restart signal: a commit conflict invalidated staged trigger derivations — their
 * bodies and OLD/NEW images were read at the superseded snapshot, so a rebase would republish
 * stale values. The statement re-runs whole instead, re-reading everything fresh.
 */
class StaleTriggerDerivationsError extends Error {
  constructor(readonly conflict: WriteConflictError) {
    super("Trigger derivations were computed at a superseded snapshot");
  }
}
/** Distinct SQL statements whose optimized plans stay cached; plans are a few KB each. */
const PLAN_CACHE_LIMIT = 512;

/**
 * Below this ratio, gzip is not paying for itself: it costs the write a full compression pass
 * and every read a decompression pass, to save almost nothing. Measured on 129,000-row blocks,
 * the shapes worth compressing land far above it — the weakest, random UUID text, still reaches
 * 1.8x — while a column of unstructured doubles reaches 1.07x for 16ms a block.
 */
const GZIP_WORTHWHILE_RATIO = 1.2;

/**
 * How many blocks a column may be written raw before gzip is tried again. A column's shape is
 * usually stable across its blocks, which is what makes one observation worth reusing; this
 * bounds how long a wrong observation can persist if the data changes underneath it.
 */
const GZIP_REPROBE_BLOCKS = 32;
/** Failed per-column probes retained in one database session. */
const GZIP_VERDICT_CACHE_LIMIT = 256;

/**
 * Below this many logical bytes a block is written raw: the compression pass on the write and
 * the decompression pass on every read would cost more than the bytes they save, and a point
 * update's or delete's one-row block is the common case — it used to pay a CompressionStream
 * round trip to shrink a few dozen bytes.
 */
const GZIP_MINIMUM_INPUT_BYTES = 4 * 1024;
/** Bounds concurrent compression work without serializing independent column blocks. */
const WRITE_ENCODE_CONCURRENCY = 6;

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

/** A table's part of a live proof window: see `#liveProofContext`. */
interface LiveProofTable {
  readonly table: TableRecord;
  /** Segments whose transaction committed inside the window, in store order. */
  readonly windowSegments: readonly SegmentRecord[];
  /** The committed versions those segments account for. */
  readonly coveredVersions: ReadonlySet<number>;
}

/** What every live proof over one commit window shares: see `#liveProofContext`. */
interface LiveProofContext {
  readonly after: number | null;
  readonly until: number;
  /** Versions in (after, until] that recorded a change to each table. */
  readonly changedVersions: ReadonlyMap<string, ReadonlySet<number>>;
  readonly tables: Map<string, Promise<LiveProofTable | undefined>>;
}

interface SegmentVisibilityCatalog {
  readonly transactions: ReadonlyMap<string, { readonly committedVersion: number | null }>;
  readonly segmentsByTable: ReadonlyMap<string, readonly SegmentRecord[]>;
}

interface ZonePredicate {
  readonly column: TableColumnRecord;
  readonly operator: ComparisonOperator | "IN";
  /** The compared literal; for IN, the smallest list member. */
  readonly value: number;
  /** IN only: every distinct member, ascending, so a block test is one binary search. */
  readonly members?: Float64Array;
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
  /** What rows written before this column existed read as, instead of NULL. */
  backfill?: boolean | number | string | Date;
}

export interface MigrateOptions {
  /**
   * Allows steps that destroy data — dropping a column or a table. Off by default: a migration
   * runs when an application opens, with nobody to review it.
   */
  readonly allowDestructive?: boolean;
  /**
   * Treats the schema as the whole database, so a table it created and no longer declares is
   * dropped. Off by default: an application may migrate feature by feature, and each call
   * declaring only its own tables must not mean "drop the others". Needs `allowDestructive`
   * as well, since dropping a table destroys its rows.
   */
  readonly schemaOwnsDatabase?: boolean;
}

export interface CreateTableInput {
  name: string;
  columns: readonly ColumnDefinition[];
  /** Row conditions every written row must satisfy (E141-06); each is a boolean SQL expression. */
  checks?: ReadonlyArray<{ name: string; sql: string }>;
  /** Marks the table as created from a schema, which lets a later migration drop it. */
  managed?: boolean;
  /** Single-column references to another table's unique key (E141-04). */
  foreignKeys?: readonly ForeignKeyDefinition[];
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
  /** Encoding the batch into blocks. */
  encodeMs: number;
  /**
   * Staging the blocks and segments as a step of its own: trigger-derived rows, and stores
   * without a single-shot commit. On a store that commits a simple write in one storage
   * transaction the staging happens inside the commit, this is 0, and `commitMs` covers both.
   */
  stageMs: number;
  /** Committing — including the staging, on the single-shot path — and any rebase retries. */
  commitMs: number;
  totalMs: number;
  retries: number;
  rowsPerSecond: number;
}

export interface ReadTableOptions {
  version?: number;
  columns?: readonly string[];
}

/**
 * The scope handed to `snapshot()`: queries pinned to one manifest version, consistent with
 * each other for the lifetime of the callback.
 */
export interface SnapshotSession {
  /** The pinned manifest version; null only on a database with no commits yet. */
  readonly version: number | null;
  query(sql: string, options?: QueryOptions): Promise<QueryResult>;
}

/** Lifetime buffer pool counters; see MinnowDatabase.bufferPoolStats(). */
export interface BufferPoolStats {
  limitBytes: number;
  usedBytes: number;
  entries: number;
  hits: number;
  misses: number;
  evictions: number;
}

/** One staged mutation inside a write scope; the commit version arrives on the scope. */
export interface StagedWriteResult {
  tableName: string;
  segmentId: string | null;
  rowCount: number;
}

/**
 * The scope handed to `write()`: every mutation stages into one transaction and publishes
 * as one commit — all of it or none of it, in every tab. Reads observe the pre-scope snapshot
 * plus everything the scope has staged, so later statements (and trigger bodies) see earlier
 * ones; AFTER triggers fire per staged operation exactly as they do for standalone writes.
 *
 * A mutation that fails after registering part of its work ends the scope: the registration
 * cannot be undone in place, so later statements and the commit both reject even if the caller
 * caught the original error. A mutation that fails validation before registering anything
 * leaves the scope usable.
 */
export interface WriteSession {
  /**
   * Read-your-writes: the query observes the pre-scope snapshot PLUS everything this scope
   * has staged so far, ordered after all committed data — without publishing anything.
   */
  query(sql: string, options?: { params?: QueryOptions["params"] }): Promise<QueryResult>;
  insertBatch(tableName: string, input: InsertBatchInput): Promise<StagedWriteResult>;
  upsertBatch(tableName: string, input: InsertBatchInput): Promise<StagedWriteResult>;
  updateBatch(tableName: string, input: UpdateBatchInput): Promise<StagedWriteResult>;
  deleteBatch(tableName: string, input: DeleteBatchInput): Promise<StagedWriteResult>;
}

/** What one statement's execution cost, reported by the engine that ran it. */
export interface QueryExecutionStats {
  /**
   * Peak modeled execution memory for this statement, in bytes: the documented vector,
   * row-index, group/result payload, and ordering buffers, which is the same model
   * `executionMemoryBudgetBytes` bounds. Boxed snapshot preparation, JavaScript container
   * overhead, and allocator overhead are outside it. Not reported for a memo hit — nothing ran.
   */
  readonly peakMemoryBytes: number;
}

export interface QueryOptions {
  /**
   * Called once with what this execution cost, before the result is returned. Additive and
   * optional: the engine can report its own memory because it reserves before it allocates,
   * which is not something the storage layer or a caller could measure from outside.
   */
  readonly onStats?: (stats: QueryExecutionStats) => void;
  /**
   * false makes this statement compute its results instead of reusing any it has cached: the
   * probe-validated result memo, cached block results, and the columnar forms of derived and
   * windowed sources are all bypassed (the default true serves provably-fresh cached results
   * from each). Block and vector caches stay warm — those cache storage reads, not results.
   *
   * Useful for benchmarking execution itself, and for callers that re-run one statement in a
   * tight loop over changing external state. Note that replaying one statement over unchanging
   * data with the default on measures cache lookups, not query execution.
   */
  memoize?: boolean;
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
  /**
   * Target maximum rows per level-one partition a keyed fold publishes. A fold rewrites only
   * the partitions its deltas touch, so smaller partitions make folds cheaper and scans read
   * more blocks. Ignored by keyless tables and by L2 promotion.
   */
  partitionRows?: number;
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
  /** Every published output segment; a keyed fold publishes one per partition it wrote. */
  outputSegmentIds?: string[];
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
  /** Maximum block, segment, and transaction candidates copied into one durable planning record. */
  maxPlanningItems?: number;
  /**
   * Manifest versions below the current one to leave unpruned, newest first, so a reader that
   * names a recent version — `query({ version })`, `readTable(name, version)` — still finds it.
   * Defaults to 0: an explicit call reclaims everything no lease or job pins. Background
   * collection keeps 64.
   */
  retainRecentVersions?: number;
}

export interface CollectGarbageStepOptions {
  /** Maximum candidates examined and checkpointed by this durable reclamation step. */
  maxItems?: number;
  /** Maximum block, segment, and transaction candidates copied into a newly planned job. */
  maxPlanningItems?: number;
  /** See `CollectGarbageOptions.retainRecentVersions`; applies when this step plans a job. */
  retainRecentVersions?: number;
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
  reclaimedTransactionCount: number;
  retainedTransactionCount: number;
  missingTransactionCount: number;
  physicallyReclaimedBytes: number;
}

export interface GarbageCollectionProgress {
  jobId: string;
  state: GarbageCollectionJobState;
  examinedManifestCount: number;
  examinedSegmentCount: number;
  examinedBlockCount: number;
  examinedTransactionCount: number;
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
  /**
   * Block codec for newly written blocks; defaults to "gzip", which is also what compaction
   * rewrites to, so a table's blocks are encoded the same way however they got there.
   *
   * Measured over 200k rows on IndexedDB, gzip against raw: about half the stored bytes
   * (4.9 MiB vs 10.0 MiB) and a *faster* cold scan (13.3 ms vs 17.9 ms) — reading half the
   * bytes out of IndexedDB more than pays for decompressing them — at the cost of roughly
   * 2.2x on bulk ingest (413 ms vs 184 ms). Warm scans are identical either way, because the
   * buffer pool caches decoded blocks. Choose "raw" when ingest throughput matters more than
   * storage quota; the browser usually makes the opposite trade worth it.
   */
  compression?: Compression;
  rowsPerBlock?: number;
  maxCommitRetries?: number;
  now?: () => Date;
  createId?: () => string;
  /** Durable spill-owner lease lifetime; renewed while a spilling query runs. */
  spillOwnerLeaseMs?: number;
  /**
   * How long a statement-level transaction (`BEGIN` … `COMMIT`) may sit untouched before it
   * rolls itself back; 30 seconds by default. A scope nobody ever closes would otherwise hold
   * its staged blocks and transaction record against the collector forever.
   */
  transactionIdleTimeoutMs?: number;
  /**
   * Retained bytes for the block buffer pool: decoded physical blocks, their vectorized
   * per-block column forms, zone-map block descriptions, and derived/subquery block
   * results. Every entry is keyed by an immutable identity (a block id, or an exact
   * visible-segment-id fingerprint), so a cached entry can never serve stale data;
   * superseded entries simply stop being referenced and age out of the byte-bounded
   * LRU. 0 disables the pool. Defaults to 64 MiB.
   */
  bufferPoolBytes?: number;
  /**
   * Visible-row threshold at which a full-text MATCH on an unindexed append-only column
   * schedules a background index build (fire-and-forget; correctness never waits on it).
   * Defaults to 4096.
   */
  ftsAutoIndexRows?: number;
  /**
   * Background compaction: when a scan or a run of commits finds a table fragmented past 48
   * visible segments or carrying 32 delete/update deltas, a fold is planned and driven to
   * publication in small yielding steps, and re-planned while the table stays due — the same
   * self-maintenance pattern as the full-text auto index. Correctness never waits on it.
   * false disables, leaving `compactTable` to the caller.
   */
  autoCompact?: boolean;
  /**
   * Background collection: a garbage-collection pass after every background fold, and one
   * every 64 commits, each keeping the 64 most recent versions readable. Without it the
   * blocks a fold supersedes and the manifest every commit writes stay on disk until the
   * caller runs `collectGarbage`. Defaults to `autoCompact`; false leaves collection to the
   * caller.
   */
  autoCollect?: boolean;
  /**
   * Defaults for every compaction this database plans, background folds included. A call's
   * own `CompactTableOptions` override them.
   */
  compaction?: {
    /** Target maximum rows per level-one partition of a keyed table. Defaults to 16,384. */
    partitionRows?: number;
  };
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

/**
 * How deep a delete's referential actions may cascade. A chain of CASCADE keys can reach further
 * tables; the bound stops a cycle from recursing, the same way the trigger chain is bounded.
 */
const REFERENTIAL_CASCADES = 8;

/** One foreign key and the table that declares it, indexed by the parent it points at. */
interface ChildForeignKey {
  table: TableRecord;
  key: NonNullable<TableRecord["foreignKeys"]>[number];
}

/** What one catalog epoch says beyond the tables themselves; see `#catalogFacts`. */
interface CatalogFacts {
  /** View name to its query text. */
  views: ReadonlyMap<string, string>;
  /** Parent table name to the foreign keys referencing it. */
  childKeys: ReadonlyMap<string, readonly ChildForeignKey[]>;
}

/** Thrown into a held write scope to make it abort; never surfaces to a caller. */
class TransactionRollback extends Error {
  constructor() {
    super("Transaction rolled back");
    this.name = "TransactionRollback";
  }
}

/**
 * Whether a statement may run inside a statement-level transaction. Reads and row writes stage
 * into the scope; schema changes do not, because the catalog commits outside it and a rollback
 * could not take them back.
 */
function isTransactionalStatement(statement: CompiledStatement): boolean {
  return (
    statement.kind === "insert" ||
    statement.kind === "update" ||
    statement.kind === "delete" ||
    statement.kind === "select"
  );
}

export type ExecuteResult =
  | { kind: "rows"; result: QueryResult }
  | { kind: "create-table"; table: string }
  | { kind: "add-column"; table: string; column: string }
  | { kind: "drop-table"; table: string; dropped: boolean }
  | { kind: "merge"; table: string; rowCount: number; version?: number }
  | { kind: "transaction"; action: "begin" | "commit" | "rollback"; version?: number }
  | { kind: "create-view"; view: string }
  | { kind: "drop-view"; view: string; dropped: boolean }
  | { kind: "create-trigger"; table: string; name: string }
  | { kind: "drop-trigger"; name: string }
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

/** Quotes a catalog identifier for internally generated SQL. */
function quoteSqlIdentifier(identifier: string): string {
  return `"${identifier.replaceAll('"', '""')}"`;
}

export interface RunStatementOptions {
  /**
   * Projects the affected rows back: column names, or "*" for every table column. Inserts echo
   * the written values; updates return post-update values; deletes return the rows as read.
   */
  readonly returning?: readonly string[] | "*";
  /**
   * Where the statement's writes go. Absent means the database itself — one commit per
   * statement. A write scope routes them into that scope instead, which is how a statement-level
   * transaction makes several statements publish together.
   */
  readonly writer?: StatementWriter;
}

/**
 * A write scope seen as somewhere statements can run: the batch operations, plus the reads the
 * keyed mutation paths need — by SQL for subqueries, by plan for the rows an UPDATE or DELETE
 * is about to touch. Both reads observe the scope's own staged writes.
 */
export interface StatementWriter extends WriteSession {
  queryPlan(plan: CompiledQuery): Promise<QueryResult>;
}

export interface VisibleSegment {
  id: string;
  rowCount: number;
  columnBlockIds: Readonly<Record<string, readonly string[]>>;
}

/**
 * Snapshots are optional members of `BlockStore` — a store can be a complete database backend
 * without being able to copy itself out — so the database checks at the call and says plainly
 * when the capability is absent rather than failing as a missing property.
 */
function exportingStore(store: BlockStore): Required<Pick<BlockStore, "exportSnapshot">> {
  const exportSnapshot = store.exportSnapshot?.bind(store);
  if (exportSnapshot === undefined) {
    throw new Error("This database's block store cannot export snapshots");
  }
  return { exportSnapshot };
}

function importingStore(store: BlockStore): Required<Pick<BlockStore, "importSnapshot">> {
  const importSnapshot = store.importSnapshot?.bind(store);
  if (importSnapshot === undefined) {
    throw new Error("This database's block store cannot load snapshots");
  }
  return { importSnapshot };
}

export interface SnapshotExportOptions {
  /** Called as the export moves through its phases; see SnapshotExportProgress. */
  onProgress?: (progress: SnapshotExportProgress) => void;
}

export interface SnapshotImportOptions {
  /** Called as blocks land, for a progress bar over a multi-megabyte file. */
  onProgress?: (progress: SnapshotLoadProgress) => void;
}

export class MinnowDatabase {
  readonly #transactions: TransactionManager;
  /**
   * How long a statement-level transaction may sit untouched before it rolls itself back. The
   * bound is what makes a BEGIN a caller can walk away from safe: an abandoned scope holds
   * staged blocks and a transaction record the collector cannot reclaim while it is open.
   */
  readonly #transactionIdleTimeoutMs: number;
  /** What the catalog says, derived once per epoch; see `#catalogFacts`. */
  #catalogCache: { epoch: number; facts: CatalogFacts } | undefined;
  /** The scope a statement-level BEGIN opened, held until COMMIT, ROLLBACK, or the idle sweep. */
  #openTransaction:
    | {
        session: StatementWriter;
        settle: (outcome: "commit" | "rollback") => void;
        finished: Promise<{ version: number | null | undefined }>;
        timer: ReturnType<typeof setInterval>;
        /** Statements running right now: a transaction working is never idle, however slow. */
        busy: number;
        /** When the last statement started or finished, by the injected clock. */
        activeAt: number;
      }
    | undefined;
  readonly #compression: Compression;
  /** Per-column count since gzip last failed to repay itself; successful probes need no entry. */
  readonly #gzipVerdicts = new Map<string, number>();
  readonly #rowsPerBlock: number;
  readonly #maxCommitRetries: number;
  readonly #now: () => Date;
  readonly #spillOwnerLeaseMs: number;
  readonly #createId: () => string;
  readonly #internalLeaseOwnerId = `minnow/${crypto.randomUUID()}`;
  readonly #liveSets = new Set<LiveQuerySet>();
  /** Live proof inputs per commit window, keyed `after:until`; see #liveProofContext. */
  readonly #liveProofContexts = new Map<string, Promise<LiveProofContext>>();
  #internalLeaseSequence = 0;
  readonly #artifactCache: ArtifactCache;
  readonly #ftsAutoIndexRows: number;
  readonly #autoCompact: boolean;
  readonly #compactionPartitionRows: number;
  readonly #autoCollect: boolean;
  /** Data commits since the last background collection pass. */
  #commitsSinceCollection = 0;
  /**
   * The highest manifest version below which everything is known to be collected — pruned,
   * with no block left behind; a collection plan starts its walk there. In memory only: a
   * fresh instance walks the whole history once and learns it again.
   */
  #collectionWatermark: number | null = null;
  #autoCollectionInFlight = false;
  /** A trigger that arrived while a run was in flight; honoured when the run ends. */
  #autoCollectionRequested = false;
  #autoCollectionBackoffUntilCommit = 0;
  /** When the last background collection pass started, by the database clock. */
  #lastCollectionAt: number | undefined;
  /** The idle pass scheduled after the last commit; reset by the next commit. */
  #idleCollectionTimer: ReturnType<typeof setTimeout> | undefined;
  /** The garbage-collection step in flight, so steps run one at a time: see #serializedCollectionStep. */
  #collectionSteps: Promise<unknown> = Promise.resolve();
  /** One background build attempt per (table, column) per session; misses just stay scans. */
  readonly #ftsBuildsInFlight = new Set<string>();
  /** Tables with a fire-and-forget compaction step already running. */
  readonly #autoCompactionsInFlight = new Set<string>();
  /** Tables whose maintenance threshold was observed again while their fold was still running. */
  readonly #autoCompactionsRequested = new Set<string>();
  /** Changed tables awaiting the debounced check that closes a write burst. */
  readonly #idleCompactionTableIds = new Set<string>();
  #idleCompactionTimer: ReturnType<typeof setTimeout> | undefined;
  /** Tables whose drop is retiring data; prevents a new background fold from starting. */
  readonly #droppingTables = new Set<string>();
  /** Per table: the visible segment count a failed auto-compaction must see before retrying. */
  readonly #autoCompactionBackoff = new Map<string, number>();
  /** Data commits per table since its last write-path auto-compaction check. */
  readonly #commitsSinceCompactionCheck = new Map<string, number>();
  /** The compaction step in flight per table, so steps on one table run one at a time. */
  readonly #compactionSteps = new Map<string, Promise<unknown>>();
  /** The simple writes in flight, chained so they commit one after another: see #runWrite. */
  #writeChain: Promise<unknown> = Promise.resolve();
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
  /** An in-flight re-pin of the shared lease; acquirers wait for it, never join it. */
  #sharedLeaseMove: Promise<void> | undefined;
  /**
   * Catalog states keyed by requested table-name set, valid only at #catalogStateEpoch.
   * The (version, epoch) probe is the sole validity signal: a matched epoch proves a cached
   * entry is byte-identical to a fresh read, and a moved epoch clears every entry.
   */
  readonly #catalogStateCache = new Map<string, QueryCatalogState>();
  #catalogStateEpoch: number | undefined;

  constructor(
    private readonly store: BlockStore,
    options: MinnowDatabaseOptions = {},
  ) {
    this.#compression = options.compression ?? "gzip";
    // The block is the streamed scan's row group and the buffer pool's residency unit.
    // Measured curve (400k rows, streamed filter/group/like/top-n, 2026-08): throughput is
    // flat from ~16k rows per block and 2k-row blocks cost up to 66% on top-n; 65,536 sits
    // on the plateau while keeping eviction granularity moderate.
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
    this.#artifactCache = new ArtifactCache(options.bufferPoolBytes ?? 64 * 1024 * 1024);
    this.#ftsAutoIndexRows = options.ftsAutoIndexRows ?? 4096;
    this.#autoCompact = options.autoCompact ?? true;
    this.#autoCollect = options.autoCollect ?? this.#autoCompact;
    this.#compactionPartitionRows = positiveWholeNumber(
      options.compaction?.partitionRows ?? DEFAULT_COMPACTION_PARTITION_ROWS,
      "Compaction partition rows",
    );
    if (!Number.isSafeInteger(this.#ftsAutoIndexRows) || this.#ftsAutoIndexRows < 0) {
      throw new RangeError(
        "Full-text auto-index row threshold must be a non-negative whole number",
      );
    }
    this.#transactionIdleTimeoutMs = options.transactionIdleTimeoutMs ?? 30_000;
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
        ...(column.backfill === undefined ? {} : { backfill: column.backfill }),
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
    const foreignKeys = await Promise.all(
      (input.foreignKeys ?? []).map(async (key) => {
        const child = columns.find((column) => column.name === key.column);
        if (child === undefined) {
          throw new TypeError(
            `FOREIGN KEY ${key.name} names a column this table has no: ${key.column}`,
          );
        }
        // Self-references are allowed, and then the parent is this very table, which does not
        // exist yet — its own declaration is the authority on the key.
        const parent =
          key.parentTable === name ? undefined : await this.store.getTableByName(key.parentTable);
        if (key.parentTable !== name && parent === undefined) {
          throw new TypeError(
            `FOREIGN KEY ${key.name} references a table that does not exist: ${key.parentTable}`,
          );
        }
        const parentKey =
          parent === undefined
            ? columns.find((column) => column.name === input.uniqueKey)
            : getUniqueKeyColumn(parent);
        if (parentKey === undefined) {
          throw new TypeError(
            `FOREIGN KEY ${key.name} references a table with no unique key: ${key.parentTable}`,
          );
        }
        if (key.parentColumn !== undefined && key.parentColumn !== parentKey.name) {
          throw new TypeError(
            `FOREIGN KEY ${key.name} must reference ${key.parentTable}'s unique key ${parentKey.name}`,
          );
        }
        if (child.type !== parentKey.type) {
          throw new TypeError(
            `FOREIGN KEY ${key.name} compares ${child.type} with ${parentKey.type}`,
          );
        }
        if (key.onDelete === "set null" && !child.nullable) {
          throw new TypeError(`FOREIGN KEY ${key.name} cannot SET NULL a NOT NULL column`);
        }
        return {
          name: validateName(key.name, "Constraint"),
          column: key.column,
          parentTable: key.parentTable,
          parentColumn: parentKey.name,
          onDelete: key.onDelete,
        };
      }),
    );
    const checks = (input.checks ?? []).map((check) => {
      // Compiling here means a constraint the engine could never evaluate is refused at
      // definition rather than on the first write.
      compileCheckExpression(check.sql, check.name);
      return { name: validateName(check.name, "Constraint"), sql: check.sql };
    });
    await this.store.addTable({
      id: this.#createId(),
      name,
      columns,
      ...(foreignKeys.length === 0 ? {} : { foreignKeys }),
      ...(checks.length === 0 ? {} : { checks }),
      ...(input.managed === true ? { managed: true } : {}),
      ...(uniqueKeyColumn === undefined ? {} : { uniqueKeyColumnId: uniqueKeyColumn.id }),
      ...(uniqueKeyColumn === undefined ? {} : { uniqueKeyLookupReady: true }),
      createdAt: this.#now().toISOString(),
    });
  }

  /**
   * The published catalog: stable column IDs, key identity, constraints, triggers, and view
   * bodies. This is the introspection surface schema tooling builds on — `listTables()` answers
   * what a reader can select, this answers what a planner needs to diff.
   */
  async introspect(): Promise<Catalog> {
    return toCatalog(await this.store.listTables());
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

  /**
   * Re-runs a statement whose staged trigger derivations were invalidated by a commit
   * conflict: unlike the plain rebase-and-retry, a restart re-reads pre-images and re-runs
   * trigger bodies at the fresh state, so derivations can never publish stale values.
   */
  /**
   * Runs one simple write — insert, upsert, update, or delete — after every simple write this
   * database already has in flight, restarting it when its trigger derivations went stale.
   *
   * Commits are optimistic: a writer reads the manifest version, stages, and publishes only if
   * the version has not moved, rebasing and retrying otherwise up to `maxCommitRetries`.
   * Writers issued concurrently from one database used to all read the same version and spend
   * a retry per rival that landed first, so past `maxCommitRetries + 1` of them the rest failed
   * for nothing — contention this database need not create, and the queue does not. Writers in
   * other instances and other tabs still contend, and the retry loop is still what resolves
   * them. Write scopes are not queued: a scope's callback may issue a plain write of its own,
   * which must not wait on the scope that contains it.
   */
  async #runWrite<T>(run: () => Promise<T>): Promise<T> {
    const restarting = async (): Promise<T> => {
      for (let attempt = 0; ; attempt += 1) {
        try {
          return await run();
        } catch (error) {
          if (!(error instanceof StaleTriggerDerivationsError)) throw error;
          if (attempt >= this.#maxCommitRetries) throw error.conflict;
        }
      }
    };
    const previous = this.#writeChain;
    const current = previous.then(restarting, restarting);
    this.#writeChain = current;
    try {
      return await current;
    } finally {
      if (this.#writeChain === current) this.#writeChain = Promise.resolve();
    }
  }

  /**
   * Defines a view: a query the catalog answers to by name (F031-02). The stored record carries
   * the query's inferred output schema, so a view answers the same questions a table does —
   * what a reader can select, and of what type — and the devtools rail lists it beside them.
   * Reads expand it into the query it stands for; writes to it are refused.
   */
  async createView(
    name: string,
    sql: string,
    options: { orReplace?: boolean; managed?: boolean } = {},
  ): Promise<void> {
    const viewName = validateName(name, "View");
    const plan = compileQuery(sql);
    const existing = await this.store.getTableByName(viewName);
    if (existing !== undefined) {
      if (existing.view === undefined) throw new TypeError(`Table already exists: ${viewName}`);
      if (options.orReplace !== true) throw new TypeError(`View already exists: ${viewName}`);
    }
    // The schema is inferred against the catalog as it stands, where a view answers with its own
    // stored columns — so a view over a view types without expanding anything, and a body that
    // cannot be typed fails where it is written rather than on the first read.
    const schemas = new Map(
      (await this.store.listTables()).map((table) => [
        table.name,
        table.columns.map(({ name: column, type }) => ({ name: column, type })),
      ]),
    );
    const inferred = inferBlockSchema(plan, schemas);
    if (inferred.length === 0) throw new TypeError(`A view needs at least one column: ${viewName}`);
    if (existing !== undefined) await this.store.removeTable(existing.id, existing.revision ?? 0);
    await this.store.addTable({
      id: this.#createId(),
      name: viewName,
      columns: inferred.map((column) => ({
        id: this.#createId(),
        name: column.name,
        type: column.type,
        nullable: true,
      })),
      view: { sql, ...(options.managed === true ? { managed: true } : {}) },
      createdAt: this.#now().toISOString(),
    });
    this.#planCache.clear();
  }

  /** Removes a view. Returns whether one was dropped; `ifExists` makes a missing one no error. */
  async dropView(name: string, options: { ifExists?: boolean } = {}): Promise<boolean> {
    const record = await this.store.getTableByName(name);
    if (record?.view === undefined) {
      if (record !== undefined) throw new TypeError(`Not a view: ${name}`);
      if (options.ifExists === true) return false;
      throw new Error(`View not found: ${name}`);
    }
    await this.store.removeTable(record.id, record.revision ?? 0);
    this.#planCache.clear();
    return true;
  }

  /**
   * Drops a table: its rows, its catalog record, its full-text index, and its triggers. The
   * blocks are retired through an ordinary commit rather than deleted, so a reader pinned to an
   * older version keeps resolving the bytes it already holds and the lease-aware collector
   * reclaims them when nobody can reach them. What a pinned reader does lose is the table
   * itself — the catalog has one present tense, so a snapshot open across a drop sees the table
   * disappear rather than a frozen copy of it.
   *
   * Returns whether a table was dropped; with `ifExists`, a missing table is not an error.
   */
  async dropTable(tableName: string, options: { ifExists?: boolean } = {}): Promise<boolean> {
    const table = await this.store.getTableByName(tableName);
    if (table === undefined) {
      if (options.ifExists === true) return false;
      throw new Error(`Table not found: ${tableName}`);
    }
    if (table.view !== undefined) throw new TypeError(`${tableName} is a view; use DROP VIEW`);
    // Anything that would be left pointing at a table that is no longer there refuses the drop:
    // a view over it would fail on its next read, and a trigger body writing to it at every
    // firing. Both are worse outcomes than an error here, which is the rule a column rename
    // already follows.
    for (const owner of await this.store.listTables()) {
      if (owner.id === table.id) continue;
      if (owner.view !== undefined && viewReadsTable(owner.view.sql, table.name)) {
        throw new TypeError(`Cannot drop ${table.name}: view ${owner.name} reads it`);
      }
      for (const key of owner.foreignKeys ?? []) {
        if (key.parentTable === table.name) {
          throw new TypeError(
            `Cannot drop ${table.name}: foreign key ${key.name} on ${owner.name} references it`,
          );
        }
      }
      for (const trigger of owner.triggers ?? []) {
        const writesHere = trigger.statements.some((statement) => {
          const compiled = compileStatement(statement.sql);
          return (
            (compiled.kind === "insert" ||
              compiled.kind === "update" ||
              compiled.kind === "delete") &&
            compiled.table === table.name
          );
        });
        if (writesHere) {
          throw new TypeError(
            `Cannot drop ${table.name}: trigger ${trigger.name} on ${owner.name} writes to it`,
          );
        }
      }
    }
    this.#droppingTables.add(table.id);
    try {
      // Stop every fold already attached to the table before its catalog record disappears.
      // Otherwise an unpublished job can no longer resume or be cancelled by table name, and
      // its transaction and staged output become permanent roots.
      await this.#cancelTableCompactions(table.id);
      // Retiring the blocks is a commit like any other, and background compaction publishes
      // underneath it: a block this table owned a moment ago can already have been rewritten. The
      // list is therefore taken from the transaction's own snapshot — the manifest its commit will
      // be validated against — and a scope that loses the race simply runs again.
      for (let attempt = 0; ; attempt += 1) {
        const transaction = await this.#transactions.begin();
        try {
          const segments = await this.store.listSegments(table.id);
          const snapshot =
            transaction.snapshotVersion === null
              ? undefined
              : await this.store.getManifest(transaction.snapshotVersion);
          const live = new Set(snapshot?.blockIds ?? []);
          const blockIds = [
            ...new Set(segments.flatMap((segment) => Object.values(segment.columnBlockIds).flat())),
          ].filter((id) => live.has(id));
          transaction.markTableChanged(table.id);
          if (blockIds.length > 0) transaction.supersedeBlocks(blockIds);
          await transaction.commit();
          break;
        } catch (error) {
          await transaction.abort();
          if (!(error instanceof WriteConflictError) || attempt >= this.#maxCommitRetries)
            throw error;
        }
      }
      // The catalog goes last: until it does, the table is merely empty of live blocks, and a
      // crash in between leaves a table whose rows are gone rather than a segment pointing at a
      // table that is not there.
      // Catch a fold that was already between its scheduling check and job creation when the
      // drop began. The dropping marker prevents another one from starting after this point.
      await this.#cancelTableCompactions(table.id);
      await this.store.removeTable(table.id, table.revision ?? 0);
      for (const column of table.columns) this.#gzipVerdicts.delete(column.id);
      this.#autoCompactionBackoff.delete(table.id);
      this.#commitsSinceCompactionCheck.delete(table.id);
      this.#idleCompactionTableIds.delete(table.id);
      this.#planCache.clear();
      return true;
    } finally {
      this.#droppingTables.delete(table.id);
    }
  }

  async insertBatch(tableName: string, input: InsertBatchInput): Promise<InsertBatchResult> {
    const table = await this.#findTable(tableName);
    const { batch, generated, autoIncrement, rowCount } = this.#fillDefaults(table, input);
    await this.#assertForeignKeysPresent(
      table,
      (column) => batch.columns[column] ?? [],
      (sql, params) => this.query(sql, { params, memoize: false }),
    );
    const keys =
      autoIncrement === undefined || autoIncrement.missingIndexes.length === 0
        ? batchKeys(table, batch)
        : undefined;
    const result = await this.#runWrite(() =>
      this.#writeBatch(table, batch, "insert", keys, autoIncrement),
    );
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
    await this.#assertForeignKeysPresent(
      table,
      (column) => batch.columns[column] ?? [],
      (sql, params) => this.query(sql, { params, memoize: false }),
    );
    const deferred = autoIncrement !== undefined && autoIncrement.missingIndexes.length > 0;
    const keys = deferred ? undefined : batchKeys(table, batch);
    const result = await this.#runWrite(() =>
      this.#writeBatch(table, batch, "upsert", keys, autoIncrement),
    );
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
    await this.#assertForeignKeysPresent(
      table,
      (column) => input.changes[column] ?? [],
      (sql, params) => this.query(sql, { params, memoize: false }),
    );
    return this.#runWrite(() => this.#writeUpdateBatch(table, keyColumn, input, keys));
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
    const dependents = await this.#childForeignKeys(tableName);
    if (dependents.length === 0) {
      return this.#runWrite(() => this.#deleteBatchOnce(tableName, input));
    }
    // E141-04: the referential actions and the delete itself publish as one commit, so no tab
    // can observe a parent gone while its children still point at it.
    const table = await this.#findTable(tableName);
    const started = performance.now();
    const { result, version } = await this.write(async (session) => {
      await this.#applyReferentialActions(table, [...input.keys], session, REFERENTIAL_CASCADES);
      return session.deleteBatch(tableName, input);
    });
    return {
      tableName,
      segmentId: result.segmentId,
      requestedKeyCount: input.keys.length,
      deletedRowCount: result.rowCount,
      blockCount: 0,
      storedBytes: 0,
      version,
      metrics: {
        logicalBytes: 0,
        storedBytes: 0,
        writeAmplification: 0,
        encodeMs: 0,
        stageMs: 0,
        commitMs: 0,
        totalMs: performance.now() - started,
        retries: 0,
        rowsPerSecond: 0,
      },
    };
  }

  async #deleteBatchOnce(tableName: string, input: DeleteBatchInput): Promise<DeleteBatchResult> {
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

    // Deferred: the record is written only if something stages in two steps (trigger rows),
    // and otherwise rides the single-shot commit below — or never exists, for a no-op delete.
    const transaction = await this.#transactions.beginDeferred();
    const segmentId = this.#createId();
    transaction.setUniqueKeyChanges({
      tableId: table.id,
      keyTokens: [...keys.keys()],
      requireAbsent: false,
      remove: true,
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
        const bytes = await this.#encodeColumnBlock(
          keyColumn.id,
          asColumnInput(keyColumn.type, slice),
        );
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
      // Triggers fire once per row that actually exists: a requested key with no row deletes
      // nothing and must not fire a phantom all-null OLD image.
      const deletePreImages = (
        await this.#triggerPreImages(table, keyColumn, [...keys.values()], "delete")
      ).filter((row) => row !== undefined);
      const deleteValueAt = (
        source: "new" | "old",
        column: string,
        rowIndex: number,
      ): BatchValue => (source === "old" ? (deletePreImages[rowIndex]?.[column] ?? null) : null);
      await this.#stageTriggerDerivedInserts(
        transaction,
        table,
        "delete",
        deletePreImages.length,
        deleteValueAt,
        "before",
      );
      const segment: SegmentRecord = {
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
      };
      // AFTER triggers stage derived rows between the segment and the commit, which keeps the
      // two apart; without them the stage and the commit collapse into one storage write.
      const stagesAfter = firesAfterTriggers(table, "delete");
      const stageStarted = performance.now();
      if (stagesAfter) {
        await transaction.stageArtifacts(blockWrites, [segment]);
        await this.#stageTriggerDerivedInserts(
          transaction,
          table,
          "delete",
          deletePreImages.length,
          deleteValueAt,
          "after",
        );
      }
      stageMs += performance.now() - stageStarted;

      for (let attempt = 0; attempt <= this.#maxCommitRetries; attempt += 1) {
        const commitStarted = performance.now();
        try {
          const manifest = stagesAfter
            ? await transaction.commit()
            : await transaction.stageArtifactsAndCommit(blockWrites, [segment]);
          commitMs += performance.now() - commitStarted;
          this.#afterCommit(manifest);
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
          if (deletePreImages.length > 0) throw new StaleTriggerDerivationsError(error);
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
    // Deferred: the record rides the single-shot commit below unless trigger rows stage first.
    const transaction = await this.#transactions.beginDeferred();
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
          const bytes = await this.#encodeColumnBlock(column.id, asColumnInput(column.type, slice));
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
      const checks = table.checks ?? [];
      const preImages = await this.#triggerPreImages(
        table,
        keyColumn,
        input.keys.filter((key): key is Exclude<BatchValue, null> => key !== null),
        "update",
        undefined,
        checks.length > 0,
      );
      const updateValueAt = (
        source: "new" | "old",
        column: string,
        rowIndex: number,
      ): BatchValue => {
        if (source === "old") return preImages[rowIndex]?.[column] ?? null;
        const changed = input.changes[column];
        return changed === undefined
          ? (preImages[rowIndex]?.[column] ?? null)
          : (changed[rowIndex] ?? null);
      };
      // E141-06 on the write that changes a row rather than writes a whole one: the constraint
      // sees the post-image, the row as it will be once this update lands.
      if (checks.length > 0) {
        for (let rowIndex = 0; rowIndex < input.keys.length; rowIndex += 1) {
          if (preImages[rowIndex] === undefined) continue;
          const row: Record<string, BatchValue> = {};
          for (const column of table.columns) {
            row[column.name] = updateValueAt("new", column.name, rowIndex);
          }
          assertRowChecks(table, row, rowIndex);
        }
      }
      await this.#stageTriggerDerivedInserts(
        transaction,
        table,
        "update",
        input.keys.length,
        updateValueAt,
        "before",
      );
      const segment: SegmentRecord = {
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
      };
      // AFTER triggers stage derived rows between the segment and the commit, which keeps the
      // two apart; without them the stage and the commit collapse into one storage write.
      const stagesAfter = firesAfterTriggers(table, "update");
      const stageStarted = performance.now();
      if (stagesAfter) {
        await transaction.stageArtifacts(batchBlockWrites, [segment]);
        await this.#stageTriggerDerivedInserts(
          transaction,
          table,
          "update",
          input.keys.length,
          updateValueAt,
          "after",
        );
      }
      stageMs += performance.now() - stageStarted;

      for (let attempt = 0; attempt <= this.#maxCommitRetries; attempt += 1) {
        const commitStarted = performance.now();
        try {
          const manifest = stagesAfter
            ? await transaction.commit()
            : await transaction.stageArtifactsAndCommit(batchBlockWrites, [segment]);
          commitMs += performance.now() - commitStarted;
          this.#afterCommit(manifest);
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
          if (preImages.length > 0) throw new StaleTriggerDerivationsError(error);
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
      const encodeStarted = performance.now();
      for (
        let columnStart = 0;
        columnStart < table.columns.length;
        columnStart += WRITE_ENCODE_CONCURRENCY
      ) {
        const encodedColumns = await Promise.all(
          table.columns
            .slice(columnStart, columnStart + WRITE_ENCODE_CONCURRENCY)
            .map(async (column) => {
              const values = input.columns[column.name] ?? [];
              const blockIds: string[] = [];
              const blockWrites: Array<{ id: string; bytes: Uint8Array }> = [];
              for (
                let start = 0, part = 0;
                start < rowCount;
                start += this.#rowsPerBlock, part += 1
              ) {
                const slice = values.slice(start, Math.min(start + this.#rowsPerBlock, rowCount));
                const bytes = await this.#encodeColumnBlock(
                  column.id,
                  asColumnInput(column.type, slice),
                );
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
              return { columnId: column.id, blockIds, blockWrites };
            }),
        );
        // Promise.all preserves schema order, keeping metadata and staged writes deterministic
        // regardless of which native compressor finishes first.
        for (const encoded of encodedColumns) {
          columnBlockIds[encoded.columnId] = encoded.blockIds;
          storedBytes += sumBytes(encoded.blockWrites);
          blockCount += encoded.blockWrites.length;
          batchBlockWrites.push(...encoded.blockWrites);
        }
      }
      encodeMs += performance.now() - encodeStarted;

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
      const insertValueAt = (
        source: "new" | "old",
        column: string,
        rowIndex: number,
      ): BatchValue => (source === "new" ? (input.columns[column]?.[rowIndex] ?? null) : null);
      // Upserts fire per-row: INSERT triggers for fresh keys, UPDATE triggers (with the
      // replaced row as OLD) for conflicting ones — ON CONFLICT DO UPDATE semantics.
      const upsertKeyColumn = kind === "upsert" ? getUniqueKeyColumn(table) : undefined;
      const upsertFirings =
        upsertKeyColumn === undefined
          ? undefined
          : await this.#upsertTriggerFirings(table, upsertKeyColumn, input, rowCount);
      if (kind === "insert") {
        await this.#stageTriggerDerivedInserts(
          transaction,
          table,
          "insert",
          rowCount,
          insertValueAt,
          "before",
        );
      } else if (upsertFirings !== undefined) {
        await this.#stageUpsertTriggerFirings(transaction, table, input, upsertFirings, "before");
      }
      // AFTER triggers stage derived rows between the segment and the commit, which keeps the
      // two apart; without them the stage and the commit collapse into one storage write.
      const stagesAfter =
        kind === "insert"
          ? firesAfterTriggers(table, "insert")
          : upsertFirings !== undefined && firesAfterTriggers(table, "insert", "update");
      if (stagesAfter) {
        await transaction.stageArtifacts(batchBlockWrites, [segment]);
        if (kind === "insert") {
          await this.#stageTriggerDerivedInserts(
            transaction,
            table,
            "insert",
            rowCount,
            insertValueAt,
            "after",
          );
        } else if (upsertFirings !== undefined) {
          await this.#stageUpsertTriggerFirings(transaction, table, input, upsertFirings, "after");
        }
      }
      stageMs += performance.now() - stageStarted;

      for (let attempt = 0; attempt <= this.#maxCommitRetries; attempt += 1) {
        const commitStarted = performance.now();
        try {
          const manifest = stagesAfter
            ? await transaction.commit()
            : await transaction.stageArtifactsAndCommit(batchBlockWrites, [segment]);
          commitMs += performance.now() - commitStarted;
          this.#afterCommit(manifest);
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
          const stagedDerivations =
            kind === "insert"
              ? (table.triggers ?? []).some((trigger) => trigger.event === "insert")
              : upsertFirings !== undefined;
          if (stagedDerivations) throw new StaleTriggerDerivationsError(error);
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
   * Runs the callback against one pinned manifest version: every query inside the scope
   * observes the same committed state, however many commits land meanwhile. This is the
   * only pinning primitive — it cannot leak, because the pin is the scope. The version's
   * blocks stay protected by an internal reader lease for the scope's duration, renewed
   * while queries run; keep scopes short so garbage collection is not held back.
   *
   * On a database with no commits yet the session has no version to pin; its queries run
   * fresh, which is indistinguishable until the first commit.
   */
  async snapshot<T>(action: (session: SnapshotSession) => Promise<T>): Promise<T> {
    for (;;) {
      const version = await this.store.getCurrentManifestVersion();
      const entry = await this.#acquireSharedLease(version);
      if (entry === undefined) continue;
      try {
        const session: SnapshotSession = {
          version,
          query: (sql: string, options: QueryOptions = {}) =>
            this.query(sql, { ...options, ...(version === null ? {} : { version }) }),
        };
        return await action(session);
      } finally {
        this.#releaseSharedLease(entry);
      }
    }
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
    probe?: CatalogProbe,
  ): Promise<PreparedQuery> {
    // The ORDER-BY-expression desugar's wrapper is projection-only: prepare the inner block
    // directly (no derived materialization) and project each result to the visible aliases,
    // so `.search()` costs the same whether or not the caller also selects the score.
    const wrapper = transparentProjectionSource(plan);
    if (wrapper !== undefined) {
      const prepared = await this.#prepareCompiledPlan(wrapper.inner, options, probe);
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
          undefined,
          // memoize: false means "compute this statement's results" — that covers the
          // columnar forms of derived and windowed sources too, not just the result memo.
          options.memoize !== false,
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
        await this.#withSharedCatalogSnapshot(
          collectRealTableNames(plan),
          prepareAtSnapshot,
          probe,
        );
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
    probe?: CatalogProbe,
  ): Promise<T> {
    for (;;) {
      const state = await this.#cachedCatalogState(names, probe);
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
      if (entry === undefined) {
        // The manifest disappeared between the read and the lease, so the cached state is
        // anchored to a pruned version; drop it and re-read.
        this.#catalogStateCache.clear();
        this.#catalogStateEpoch = undefined;
        continue;
      }
      try {
        return await action(entry.lease, realTables, visibility);
      } finally {
        this.#releaseSharedLease(entry);
      }
    }
  }

  /**
   * The catalog freshness probe: one atomic (version, epoch) read decides whether the cached
   * catalog state is provably identical to a fresh read. An unchanged epoch means no table
   * record, manifest publish, or commit has landed anywhere — this tab or another — since the
   * cached state was read, so reuse is exact, not heuristic. Stores without a probe are never
   * cached. Entries key on the requested table-name set; a changed epoch clears them all.
   */
  async #cachedCatalogState(
    names: readonly string[],
    probe?: CatalogProbe,
  ): Promise<QueryCatalogState> {
    // A probe the caller read moments earlier in the same statement serves: a state read under
    // it is at least as fresh, and the cache is only consulted under its epoch.
    const read = this.store.getCatalogProbe?.bind(this.store);
    if (read === undefined) return this.#queryCatalogState(names);
    const { catalogEpoch } = probe ?? (await read());
    // Table names are only trimmed, never charset-restricted, so no join separator is
    // collision-free; JSON encoding is.
    const key = JSON.stringify(names);
    if (catalogEpoch === this.#catalogStateEpoch) {
      const cached = this.#catalogStateCache.get(key);
      if (cached !== undefined) {
        this.#catalogStateCache.delete(key);
        this.#catalogStateCache.set(key, cached);
        return cached;
      }
    }
    const state = await this.#queryCatalogState(names);
    // Only a state carrying its own epoch (read atomically with the records) can be cached;
    // the sequential fallback shape cannot prove which epoch it observed.
    if (state.catalogEpoch === undefined) return state;
    if (state.catalogEpoch !== this.#catalogStateEpoch) {
      this.#catalogStateCache.clear();
      this.#catalogStateEpoch = state.catalogEpoch;
    }
    this.#catalogStateCache.set(key, state);
    if (this.#catalogStateCache.size > CATALOG_STATE_CACHE_LIMIT) {
      const oldest = this.#catalogStateCache.keys().next().value;
      if (oldest !== undefined) this.#catalogStateCache.delete(oldest);
    }
    return state;
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
   * not expired. Otherwise the pin has to move: with no reader left on the old version the
   * one lease record is re-pinned in place (one storage write, instead of a create now and a
   * remove once the old one drains); while readers remain, a fresh lease opens at the exact
   * version and the old one retires as they finish. Returns undefined when the version's
   * manifest disappeared between the catalog read and the lease, so the caller can re-read.
   */
  async #acquireSharedLease(version: number | null): Promise<SharedLeaseEntry | undefined> {
    for (;;) {
      // A move in flight is closing the shared snapshot it re-pins; wait for it rather than
      // hand that snapshot out, then look again.
      if (this.#sharedLeaseMove !== undefined) {
        await this.#sharedLeaseMove;
        continue;
      }
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
      const options = {
        id: `${this.#internalLeaseOwnerId}/${String(this.#internalLeaseSequence++)}`,
        ownerId: this.#internalLeaseOwnerId,
        ttlMs: INTERNAL_READ_LEASE_TTL_MS,
        version,
      };
      let lease: LeasedSnapshot;
      try {
        if (current?.refCount === 0) {
          const move = this.#transactions.moveLeasedSnapshot(current.lease, options);
          this.#sharedLeaseMove = move.then(
            () => undefined,
            () => undefined,
          );
          try {
            lease = await move;
          } finally {
            this.#sharedLeaseMove = undefined;
          }
        } else {
          lease = await this.#transactions.openLeasedSnapshot(options);
        }
      } catch (error) {
        if (error instanceof SnapshotManifestMissingError) return undefined;
        throw error;
      }
      const entry: SharedLeaseEntry = { lease, version, refCount: 1 };
      const previous = this.#sharedLease;
      this.#sharedLease = entry;
      if (previous?.refCount === 0) {
        // Already closed when it was the one just moved; a remove otherwise.
        void previous.lease.release().catch(() => undefined);
      }
      return entry;
    }
  }

  /**
   * Drops the shared reader lease when nothing holds it and it has fallen behind the current
   * version. The lease outlives the query that took it so the next query at the same version
   * reuses it — but after a burst of writes and a fold, an idle database's last lease sits at a
   * pre-fold version and roots every block that version referenced, which is exactly what a
   * collection pass is trying to reclaim. The next query simply takes a fresh lease.
   */
  #releaseIdleSharedLease(): void {
    const current = this.#sharedLease;
    if (current?.refCount !== 0) return;
    this.#sharedLease = undefined;
    void current.lease.release().catch(() => undefined);
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
    cacheResults = true,
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
          cacheResults,
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
        // The windowed source's columnar form is a pure function of the inner block's cache
        // key plus the window spec, so a warm repeat skips the window pass and the
        // rows-to-columnar conversion entirely.
        const innerKey = await this.#blockResultCacheKey(
          source.windowed.block,
          snapshot,
          visibility,
          realTables,
          cacheResults,
        );
        const columnarKey =
          innerKey === undefined
            ? undefined
            : `ctw|${JSON.stringify(source.windowed.windows)}|${innerKey}`;
        if (columnarKey !== undefined) {
          const hit = this.#cacheGet(columnarKey) as
            { table: ColumnarTable; schema: SqlColumnSchema[] } | undefined;
          if (hit !== undefined) {
            typedSchemas.set(source.table, hit.schema);
            inputs.set(source.table, hit.table);
            continue;
          }
        }
        const { result: inner, schema: innerSchema } = await this.#executeBlockWithSchemaCached(
          source.windowed.block,
          snapshot,
          visibility,
          memory,
          realTables,
          typedSchemas,
          cacheResults,
        );
        const windowed = applyWindowFunctions(inner, source.windowed.windows, {
          copyRows: cacheResults,
        });
        const schema = [
          ...innerSchema,
          ...source.windowed.windows.map((window) => ({
            name: window.alias,
            type: windowOutputType(window, innerSchema),
          })),
        ];
        typedSchemas.set(source.table, schema);
        const table = derivedColumnarTable(source.table, windowed, schema);
        if (columnarKey !== undefined) {
          this.#cachePut(columnarKey, { table, schema }, derivedTableRetainedBytes(table));
        }
        inputs.set(source.table, table);
        continue;
      }
      const derived = source.derived;
      if (derived === undefined) continue;
      const derivedKey = await this.#blockResultCacheKey(
        derived,
        snapshot,
        visibility,
        realTables,
        cacheResults,
      );
      const columnarKey = derivedKey === undefined ? undefined : `ctd|${derivedKey}`;
      if (columnarKey !== undefined) {
        const hit = this.#cacheGet(columnarKey) as
          { table: ColumnarTable; schema: SqlColumnSchema[] } | undefined;
        if (hit !== undefined) {
          typedSchemas.set(source.table, hit.schema);
          inputs.set(source.table, hit.table);
          continue;
        }
      }
      const { result, schema } = await this.#executeBlockWithSchemaCached(
        derived,
        snapshot,
        visibility,
        memory,
        realTables,
        typedSchemas,
        cacheResults,
      );
      typedSchemas.set(source.table, schema);
      const derivedTable = derivedColumnarTable(source.table, result, schema);
      if (columnarKey !== undefined) {
        this.#cachePut(
          columnarKey,
          { table: derivedTable, schema },
          derivedTableRetainedBytes(derivedTable),
        );
      }
      inputs.set(source.table, derivedTable);
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
    const open = this.#openTransaction;
    if (open !== undefined) {
      // Read-your-writes: inside BEGIN … COMMIT a read sees the pre-scope snapshot plus what
      // this transaction has staged, and nothing another writer published in between.
      return this.#duringTransaction(open, () =>
        open.session.query(sql, options.params === undefined ? {} : { params: options.params }),
      );
    }
    const plan = bindPlanParameters(this.#compileCached(sql), options.params);
    // Result memoization is a pure cache over the freshness probe: the catalog epoch is part
    // of the key, so any commit or DDL changes the key and a hit can never be stale. Only
    // plain current-version queries memoize — explicit versions, budgets, and spill options
    // carry semantics of their own.
    const probe = this.store.getCatalogProbe?.bind(this.store);
    const memoizable =
      options.memoize !== false &&
      // A statement that reads the clock is not a function of the data, so the catalog epoch
      // cannot tell a fresh answer from a stale one.
      plan.usesStatementDatetime !== true &&
      options.version === undefined &&
      options.executionMemoryBudgetBytes === undefined &&
      options.spillToStorage === undefined &&
      options.spillPageRows === undefined &&
      probe !== undefined;
    if (probe === undefined || !memoizable) return this.#queryCompiled(plan, options);
    return this.#memoizedQuery(
      plan,
      `res ${queryResultMemoKey(sql, options.params ?? [])}`,
      options,
      probe,
    );
  }

  /**
   * The result memo: a pure cache over the freshness probe, keyed by the statement and the
   * catalog epoch it was answered at. The probe read before execution is handed down to the
   * execution itself — the view lookup and the catalog state would otherwise each probe again,
   * and on IndexedDB every probe is a read transaction, a floor under every small query.
   */
  async #memoizedQuery(
    plan: CompiledQuery,
    key: string,
    options: QueryOptions,
    probe: () => Promise<CatalogProbe>,
  ): Promise<QueryResult> {
    const before = await probe();
    const cached = this.#cacheGet(`${key}\u0001${String(before.catalogEpoch)}`) as
      QueryResult | undefined;
    if (cached !== undefined) return copyQueryResult(cached);
    const result = await this.#queryCompiled(plan, options, before);
    const bytes = queryResultRetainedBytes(result);
    if (bytes <= RESULT_MEMO_MAX_BYTES) {
      // Cache only when the epoch did not move during execution: the result is then exactly
      // that epoch's answer. A moved epoch simply skips the cache — never mis-files.
      const after = await probe();
      if (after.catalogEpoch === before.catalogEpoch) {
        this.#cachePut(
          `${key}\u0001${String(before.catalogEpoch)}`,
          copyQueryResult(result),
          bytes,
        );
      }
    }
    return result;
  }

  /**
   * Applies the rewrites that need the catalog rather than the statement alone: a view becomes
   * the query it stands for (F031-02), `FROM t AS y(a, b)` renames the table's columns
   * (E051-09), and a NATURAL join becomes the equality over the columns its sides share
   * (F401-01). Reads the catalog only for the statements that ask for one of them.
   */
  async #applyCatalogRewrites(plan: CompiledQuery, probe?: CatalogProbe): Promise<CompiledQuery> {
    const aliased = planHasSourceColumnAliases(plan);
    const natural = planHasNaturalJoins(plan);
    // Whether a name is a view cannot be read off the statement, so this is the one thing every
    // read has to ask the catalog. It asks by epoch — an O(1) probe the store already serves for
    // result memoization — and only re-reads the view set when the catalog has actually moved.
    // A database with no views therefore pays one probe, not a catalog scan per query.
    const { views } = await this.#catalogFacts(probe);
    let rewritten = plan;
    if (views.size > 0 && planReadsViews(plan, (name) => views.has(name))) {
      const bodies = new Map<string, CompiledQuery>();
      rewritten = expandViewSources(rewritten, (name) => {
        const sql = views.get(name);
        if (sql === undefined) return undefined;
        const cached = bodies.get(name);
        if (cached !== undefined) return cached;
        const compiled = this.#compileCached(sql);
        bodies.set(name, compiled);
        return compiled;
      });
    }
    if (!aliased && !natural) return rewritten;
    // These two need column *order*, which only the records carry; both are rare enough that
    // reading the catalog for them is fine.
    const columns = new Map(
      (await this.store.listTables()).map((table) => [
        table.name,
        table.columns.map(({ name }) => name),
      ]),
    );
    const columnsOf = (name: string): readonly string[] | undefined => columns.get(name);
    if (aliased) rewritten = expandSourceColumnAliases(rewritten, columnsOf);
    return natural ? expandNaturalJoins(rewritten, columnsOf) : rewritten;
  }

  /**
   * The two things the catalog knows that statements cannot: which names are views, and which
   * tables reference which. Both are consulted by every read or write of the relevant kind, so
   * the steady state has to be one epoch probe and no allocation; the facts are rebuilt only
   * when a catalog mutation — anywhere, including another tab — moves the epoch.
   */
  async #catalogFacts(probe?: CatalogProbe): Promise<CatalogFacts> {
    probe ??= await this.store.getCatalogProbe?.();
    const epoch = probe?.catalogEpoch;
    const cached = this.#catalogCache;
    if (cached !== undefined && epoch !== undefined && cached.epoch === epoch) return cached.facts;
    const views = new Map<string, string>();
    const childKeys = new Map<string, ChildForeignKey[]>();
    for (const table of await this.store.listTables()) {
      if (table.view !== undefined) views.set(table.name, table.view.sql);
      for (const key of table.foreignKeys ?? []) {
        const existing = childKeys.get(key.parentTable);
        if (existing === undefined) childKeys.set(key.parentTable, [{ table, key }]);
        else existing.push({ table, key });
      }
    }
    const facts: CatalogFacts = { views, childKeys };
    if (epoch !== undefined) this.#catalogCache = { epoch, facts };
    return facts;
  }

  /** Every foreign key pointing at one table, with the table that declares it. */
  async #childForeignKeys(parentName: string): Promise<readonly ChildForeignKey[]> {
    return (await this.#catalogFacts()).childKeys.get(parentName) ?? [];
  }

  /**
   * The one read pipeline: every compiled plan — SQL text, the typed builder, and live-query
   * re-runs — routes through the same streaming-first execution, so builder/SQL parity holds
   * for the execution path as well as the plan.
   */
  async #queryCompiled(
    plan: CompiledQuery,
    options: QueryOptions = {},
    probe?: CatalogProbe,
  ): Promise<QueryResult> {
    // One freshness probe per query: read here unless the caller already has one, and handed
    // to the view lookup and the catalog state below, which would otherwise probe again each.
    probe ??= await this.store.getCatalogProbe?.();
    plan = await this.#applyCatalogRewrites(plan, probe);
    const spillPageRows =
      options.spillPageRows === undefined
        ? undefined
        : positiveWholeNumber(options.spillPageRows, "Query spill page rows");
    if (this.#canStreamPlanShape(plan, options)) {
      const streamed = await this.#queryStreamed(plan, options, spillPageRows, probe);
      if (streamed !== undefined) return streamed;
    } else {
      // An ORDER-BY-expression wrapper is a pure projection over the real query: stream the
      // inner block and project the hidden ordering column away, so the wrap never costs a
      // query its streaming eligibility.
      const wrapper = transparentProjectionSource(plan);
      if (wrapper !== undefined && this.#canStreamPlanShape(wrapper.inner, options)) {
        const streamed = await this.#queryStreamed(wrapper.inner, options, spillPageRows, probe);
        if (streamed !== undefined) return projectResultColumns(streamed, wrapper.aliases);
      }
    }
    const prepared = await this.#prepareCompiledPlan(plan, options, probe);
    // Read the peak before close(): closing releases the context and zeroes what it tracked.
    const report = (result: QueryResult): QueryResult => {
      options.onStats?.({ peakMemoryBytes: prepared.memoryUsage.peakBytes });
      return result;
    };
    try {
      const spill = options.spillToStorage ?? options.executionMemoryBudgetBytes !== undefined;
      if (!spill) return report(prepared.execute());
      if (options.spillToStorage !== true) {
        try {
          return report(prepared.execute());
        } catch (error) {
          if (!(error instanceof QueryMemoryBudgetError)) throw error;
        }
      }
      return report(
        await prepared.executeAsync({
          ...(spillPageRows === undefined ? {} : { spillPageRows }),
          spillStore: this.#leasedSpillStore(),
        }),
      );
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
    const batched = this.store.putTempRunPages?.bind(this.store);
    return {
      putPage: async (ownerId, runId, pageIndex, bytes) => {
        await ensureLease(ownerId);
        await this.store.putTempRunPage({ ownerId, runId, pageIndex, bytes });
      },
      ...(batched === undefined
        ? {}
        : {
            putPages: async (pages) => {
              for (const owner of new Set(pages.map((page) => page.ownerId))) {
                await ensureLease(owner);
              }
              await batched(pages);
            },
          }),
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
          const compiled = typeof query === "string" ? this.#compileCached(query) : query.plan;
          // A live query over a view depends on the tables behind it, not on the view's name.
          const plan = await this.#applyCatalogRewrites(compiled);
          const tables = await this.#findRealBlockTables(plan);
          return new Set([...tables.values()].map((record) => record.id));
        },
        execute: async (query) =>
          typeof query === "string" ? this.query(query) : this.#queryCompiled(query.plan),
        changeCanAffect: (query, tableIds, after, until) =>
          this.#liveChangeCanAffect(query, tableIds, after, until),
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

  /**
   * The data-layer proof behind a live sweep's zone skips: whether the commits in
   * (after, until] to these tables can change this query's result. False only on proof —
   * every segment the window introduced to the table is a compaction rewrite or an insert
   * whose zone maps reject the query's predicates, and every version that changed the table
   * left a segment to inspect.
   *
   * Its inputs come from `#liveProofContext`, shared by every subscription in the sweep.
   */
  async #liveChangeCanAffect(
    query: LiveQueryInput,
    tableIds: readonly string[],
    after: number | null,
    until: number,
  ): Promise<boolean> {
    const plan = typeof query === "string" ? this.#compileCached(query) : query.plan;
    if (planContainsFts(plan)) return true;
    // Base-scan zone proofs are unsound when the plan reads the table anywhere else — a
    // subquery, EXISTS/IN, derived table, or set-operation branch can observe a row the base
    // predicates reject (e.g. `value > (SELECT AVG(value) FROM t)`).
    if (planReadsBeyondSingleScan(plan)) return true;
    const context = await this.#liveProofContext(after, until);
    for (const tableId of tableIds) {
      const entry = await this.#liveProofTable(context, tableId);
      if (entry === undefined) return true;
      const predicates = zonePredicates(plan, entry.table);
      for (const segment of entry.windowSegments) {
        const kind = segment.kind ?? "insert";
        // Compaction rewrites are visible-data-neutral by construction.
        if (kind === "base") continue;
        // Updates and deletes change existing rows; upserts can remove a row from a result
        // set by replacing values. Only pure inserts are append-only-safe to zone-check.
        if (kind !== "insert") return true;
        if (predicates.length === 0) return true;
        for (const predicate of predicates) {
          const blockIds = segment.columnBlockIds[predicate.column.id];
          if (blockIds === undefined) return true;
        }
        // The segment's new rows enter the result only if some row group passes every
        // predicate's zone check; block descriptions ride the same immutable-id cache the
        // streamed scan uses.
        const firstIds = segment.columnBlockIds[predicates[0]?.column.id ?? ""] ?? [];
        if (
          predicates.some(
            (predicate) =>
              (segment.columnBlockIds[predicate.column.id]?.length ?? 0) !== firstIds.length,
          )
        ) {
          return true;
        }
        // One batched read covers every uncached predicate block in the segment, instead of
        // a store round trip per block — hint processing after a wide insert stays cheap.
        const uncachedIds = [
          ...new Set(
            predicates.flatMap((predicate) =>
              (segment.columnBlockIds[predicate.column.id] ?? []).filter(
                (blockId) => this.#cacheGet(`zi ${blockId}`) === undefined,
              ),
            ),
          ),
        ];
        if (uncachedIds.length > 0) {
          const fetched = await this.store.getBlocks(uncachedIds);
          for (const [index, blockId] of uncachedIds.entries()) {
            const bytes = fetched[index];
            if (bytes === undefined) return true;
            this.#cachePut(`zi ${blockId}`, inspectBlock(bytes), ZONE_DESCRIPTION_CACHE_BYTES);
          }
        }
        for (let blockIndex = 0; blockIndex < firstIds.length; blockIndex += 1) {
          let canMatch = true;
          for (const predicate of predicates) {
            const blockId = segment.columnBlockIds[predicate.column.id]?.[blockIndex] ?? "";
            const description = this.#cacheGet(`zi ${blockId}`) as
              ReturnType<typeof inspectBlock> | undefined;
            if (description === undefined) return true;
            if (description.type !== predicate.column.type) return true;
            if (!zoneMapCanMatch(description, predicate)) {
              canMatch = false;
              break;
            }
          }
          if (canMatch) return true;
        }
      }
      // A version that changed this table but left no surviving segment to inspect (its
      // segments were compacted away and reclaimed) cannot be proven neutral.
      for (const version of context.changedVersions.get(tableId) ?? []) {
        if (!entry.coveredVersions.has(version)) return true;
      }
    }
    return false;
  }

  /**
   * The inputs every live proof over one commit window shares: the versions in (after, until]
   * that recorded a change to each table, and — filled in per table as proofs ask — the table
   * record, its segments committed in the window, and the versions those segments account
   * for. A sweep proves each subscription separately, and twenty subscriptions on one table
   * used to list its segments and transactions twenty times, a readonly transaction each on
   * IndexedDB and a cross-tab round trip each on an OPFS follower. A few recent windows stay
   * resident so concurrent sets sweeping different windows do not evict each other.
   */
  async #liveProofContext(after: number | null, until: number): Promise<LiveProofContext> {
    const key = `${String(after)}:${String(until)}`;
    const cached = this.#liveProofContexts.get(key);
    if (cached !== undefined) return cached;
    const context = (async (): Promise<LiveProofContext> => {
      // Proof requires every version to be accounted for by a surviving, inspected segment:
      // garbage collection deletes reclaimed segments outright, so "no segment in the window"
      // is absence of evidence, not evidence of neutrality.
      const changedVersions = new Map<string, Set<number>>();
      let cursor = after;
      pages: for (;;) {
        const page = await this.store.listManifestPage(cursor, 64);
        for (const manifest of page.records) {
          if (manifest.version > until) break pages;
          for (const tableId of manifest.changedTableIds ?? []) {
            const versions = changedVersions.get(tableId) ?? new Set<number>();
            versions.add(manifest.version);
            changedVersions.set(tableId, versions);
          }
          if (manifest.version === until) break pages;
        }
        if (page.nextCursor === null) break;
        cursor = page.nextCursor;
      }
      return { after, until, changedVersions, tables: new Map() };
    })();
    this.#liveProofContexts.set(key, context);
    if (this.#liveProofContexts.size > LIVE_PROOF_CONTEXT_LIMIT) {
      const oldest = this.#liveProofContexts.keys().next().value;
      if (oldest !== undefined) this.#liveProofContexts.delete(oldest);
    }
    return context;
  }

  #liveProofTable(context: LiveProofContext, tableId: string): Promise<LiveProofTable | undefined> {
    const cached = context.tables.get(tableId);
    if (cached !== undefined) return cached;
    const entry = (async (): Promise<LiveProofTable | undefined> => {
      const table = await this.store.getTable(tableId);
      if (table === undefined) return undefined;
      const segments = await this.store.listSegments(tableId);
      const transactions = new Map(
        (await this.#transactionRecordsForSegments(segments)).map(
          (record) => [record.id, record] as const,
        ),
      );
      const windowSegments: SegmentRecord[] = [];
      const coveredVersions = new Set<number>();
      for (const segment of segments) {
        const committed = transactions.get(segment.transactionId)?.committedVersion ?? null;
        if (committed === null || committed <= (context.after ?? -1) || committed > context.until) {
          continue;
        }
        coveredVersions.add(committed);
        windowSegments.push(segment);
      }
      return { table, windowSegments, coveredVersions };
    })();
    context.tables.set(tableId, entry);
    return entry;
  }

  /**
   * True when every manifest published in (after, until] changed no row in any table: the
   * explicitly empty `changedTableIds` that compaction publishes through
   * `markLogicallyUnchanged`. Such a commit is invisible to readers, so a write scope may
   * rebase across it rather than fail — it can invalidate neither what the scope read nor
   * what it staged, whichever tables those were.
   *
   * Everything else answers false, keeping the conflict. An *absent* `changedTableIds` is
   * unknown provenance rather than a claim of neutrality, and a window with a pruned or
   * missing version is absence of evidence rather than evidence of neutrality — the same
   * distinction `#liveChangeCanAffect` draws. Manifest versions are consecutive
   * (`createManifest` publishes `expectedVersion + 1`), so a gap is detectable.
   */
  async #commitsChangedNoData(after: number | null, until: number | null): Promise<boolean> {
    if (until === null) return false;
    let expected = after === null ? 0 : after + 1;
    let cursor = after;
    while (expected <= until) {
      const page = await this.store.listManifestPage(cursor, 64);
      if (page.records.length === 0) return false;
      for (const manifest of page.records) {
        if (expected > until) break;
        if (manifest.version !== expected) return false;
        if (manifest.changedTableIds?.length !== 0) return false;
        expected += 1;
      }
      if (page.nextCursor === null) break;
      cursor = page.nextCursor;
    }
    return expected > until;
  }

  /**
   * Read-triggered self-maintenance: a streamed scan that observes heavy fragmentation
   * schedules one incremental compaction step in the background, exactly like the full-text
   * auto index — fire-and-forget, never awaited by the read, one per table at a time.
   *
   * Deltas count separately from fragmentation. Folding them is what returns a table to the
   * plain append scan, and a table can carry enough of them to matter long before it has
   * forty-eight segments. A failed attempt (a merge plan that does not fit the compaction
   * memory budget is the usual one) doubles the count the table must reach before the next
   * one, so a table that cannot be compacted today costs one attempt, not one per query.
   */
  #maybeScheduleAutoCompaction(table: TableRecord, segments: readonly SegmentRecord[]): void {
    if (!this.#autoCompact) return;
    if (this.#droppingTables.has(table.id)) return;
    if (!autoCompactionDue(segments)) return;
    if (segments.length < (this.#autoCompactionBackoff.get(table.id) ?? 0)) return;
    if (this.#autoCompactionsInFlight.has(table.id)) {
      // A final burst can cross the threshold while the prior fold is still planning or
      // running. Remember it: otherwise no later commit or scan may arrive to trigger the fold
      // that the final state still needs.
      this.#autoCompactionsRequested.add(table.id);
      return;
    }
    this.#autoCompactionsInFlight.add(table.id);
    void this.#runAutoCompaction(table)
      .then((folded) => {
        if (folded) this.#autoCompactionBackoff.delete(table.id);
        else {
          this.#autoCompactionBackoff.set(
            table.id,
            Math.min(AUTO_COMPACT_MAX_LEVEL_ZERO_SEGMENTS, Math.max(2, segments.length * 2)),
          );
        }
      })
      .catch(() => {
        // Back off deterministic failures, but never beyond the maximum L0 prefix a fold can
        // consume. A transient conflict near the end of a burst must not strand hundreds of
        // segments waiting for a segment count the idle database can never reach.
        this.#autoCompactionBackoff.set(
          table.id,
          Math.min(AUTO_COMPACT_MAX_LEVEL_ZERO_SEGMENTS, Math.max(2, segments.length * 2)),
        );
      })
      .finally(() => {
        this.#autoCompactionsInFlight.delete(table.id);
        if (this.#autoCompactionsRequested.delete(table.id)) {
          void yieldToEventLoop().then(() => this.#checkAutoCompaction(table.id));
        }
      });
  }

  /**
   * Plans a compaction job and drives it to publication in small steps, yielding to the event
   * loop between them so queries and writes interleave with the maintenance; then, while the
   * table is still due, plans the next. A job that only advanced when the next scan happened
   * to trigger it would sit half-written in an idle tab, its output staged and its sources
   * still read on every query, and the deltas that landed while it ran would wait for a scan
   * that may never come. Returns whether anything was folded: a table compaction cannot help
   * (an unsupported layout, keys living in published partitions) must not be re-planned on
   * every trigger, so the caller backs it off as it would a failure.
   */
  async #runAutoCompaction(table: TableRecord): Promise<boolean> {
    const options = {
      maxBlocks: AUTO_COMPACT_STEP_BLOCKS,
      maxLevel0Segments: AUTO_COMPACT_MAX_LEVEL_ZERO_SEGMENTS,
    };
    let folded = false;
    for (;;) {
      if (this.#droppingTables.has(table.id)) return folded;
      let progress = await this.compactTableStep(table.name, options);
      while (progress.result === null) {
        if (progress.jobId === null) throw new Error("Compaction progress lost its job ID");
        await yieldToEventLoop();
        progress = await this.resumeCompactionJob(progress.jobId, options);
      }
      if (!progress.result.compacted) return folded;
      folded = true;
      // The fold's sources are garbage now; collect before planning the next fold.
      this.#maybeScheduleAutoCollection();
      await yieldToEventLoop();
      const current = await this.store.getTable(table.id);
      if (
        current === undefined ||
        !autoCompactionDue(await this.#currentVisibleSegments(current))
      ) {
        return folded;
      }
    }
  }

  /** The table's visible segments at the current manifest. */
  async #currentVisibleSegments(table: TableRecord): Promise<SegmentRecord[]> {
    // This is an optimistic metadata read, not a user snapshot: taking a durable reader lease
    // here would add a readwrite transaction to whichever foreground write happened to trigger
    // maintenance. Verify the manifest did not move while its segment records were loaded; the
    // current manifest itself cannot be pruned, so a matching version is the same stability proof
    // without persistent state. Do not chase a busy writer forever: this probe is only a hint,
    // periodic checks keep arriving during the burst, and the quiet-tail check gets a stable view
    // once it ends.
    let segments: SegmentRecord[] = [];
    for (let attempt = 0; attempt < 3; attempt += 1) {
      const manifest = await this.store.getCurrentManifest();
      segments = await this.#visibleSegmentRecords(
        table,
        new Snapshot(this.store, manifest?.version ?? null, manifest?.blockIds ?? []),
      );
      if ((await this.store.getCurrentManifestVersion()) === (manifest?.version ?? null)) {
        return segments;
      }
    }
    return segments;
  }

  /**
   * What every data commit shares: live sets learn of it, and the tables it changed count
   * toward their next write-path auto-compaction check.
   */
  #afterCommit(manifest: ManifestSummary): void {
    this.#notifyLiveCommit();
    this.#commitsSinceCollection += 1;
    const now = this.#now().getTime();
    if (
      this.#commitsSinceCollection >= AUTO_COLLECT_COMMIT_INTERVAL ||
      (this.#lastCollectionAt !== undefined &&
        now - this.#lastCollectionAt >= AUTO_COLLECT_QUIET_MS)
    ) {
      this.#maybeScheduleAutoCollection();
    }
    this.#armIdleCollection();
    if (!this.#autoCompact) return;
    for (const tableId of manifest.changedTableIds ?? []) {
      this.#idleCompactionTableIds.add(tableId);
      const commits = (this.#commitsSinceCompactionCheck.get(tableId) ?? 0) + 1;
      if (commits < AUTO_COMPACT_COMMIT_CHECK_INTERVAL) {
        this.#commitsSinceCompactionCheck.set(tableId, commits);
        continue;
      }
      this.#commitsSinceCompactionCheck.delete(tableId);
      void this.#checkAutoCompaction(tableId);
    }
    this.#armIdleCompactionCheck();
  }

  /**
   * Debounces the final write-path check for a burst. Sampling every few commits keeps the hot
   * path cheap, but the last one through seven commits can be the ones that cross a fold
   * threshold. Without this check an idle table can remain due forever because no later write or
   * scan arrives to notice it.
   */
  #armIdleCompactionCheck(): void {
    if (this.#idleCompactionTableIds.size === 0) return;
    if (this.#idleCompactionTimer !== undefined) clearTimeout(this.#idleCompactionTimer);
    const timer = setTimeout(() => {
      this.#idleCompactionTimer = undefined;
      const tableIds = [...this.#idleCompactionTableIds];
      this.#idleCompactionTableIds.clear();
      for (const tableId of tableIds) {
        this.#commitsSinceCompactionCheck.delete(tableId);
        void this.#checkAutoCompaction(tableId);
      }
    }, AUTO_COMPACT_IDLE_CHECK_MS);
    (timer as { unref?: () => void }).unref?.();
    this.#idleCompactionTimer = timer;
  }

  /**
   * The write-path auto-compaction check: the table's visible segments at the current manifest,
   * judged by the same thresholds a streamed scan applies. Without it a write-heavy phase with
   * no reads in between piles deltas up unfolded, and the next query pays for all of them at
   * once. Background maintenance never surfaces through a write; a failed check waits for the
   * next one.
   */
  async #checkAutoCompaction(tableId: string): Promise<void> {
    try {
      const table = await this.store.getTable(tableId);
      if (table === undefined) return;
      this.#maybeScheduleAutoCompaction(table, await this.#currentVisibleSegments(table));
    } catch {
      // Deliberately silent: the next commit or scan checks again.
    }
  }

  /**
   * Persists one AFTER trigger on its table record (compare-and-swap with retry, like
   * migration). Validation is CREATE-time so firing can trust the record: events bind only
   * the pseudo-rows they have (INSERT has NEW, DELETE has OLD, UPDATE both), binding columns
   * must exist on the trigger table, and body targets must exist, be keyless (v1 — the
   * commit's unique-key channel belongs to the primary write), differ from the trigger
   * table, and carry no triggers of their own: cascades are rejected, not silently skipped.
   */
  async #createTrigger(
    tableName: string,
    trigger: {
      name: string;
      event: "insert" | "update" | "delete";
      timing: "before" | "after";
      statements: Array<{
        sql: string;
        bindings: Array<{ source: "new" | "old"; column: string }>;
      }>;
    },
  ): Promise<void> {
    for (let attempt = 0; ; attempt += 1) {
      const table = await this.#findTable(tableName);
      const tables = await this.store.listTables();
      for (const record of tables) {
        if ((record.triggers ?? []).some((existing) => existing.name === trigger.name)) {
          throw new TypeError(`Trigger already exists: ${trigger.name}`);
        }
      }
      const columnNames = new Set(table.columns.map((column) => column.name));
      for (const statement of trigger.statements) {
        for (const binding of statement.bindings) {
          if (trigger.event === "insert" && binding.source === "old") {
            throw new TypeError("INSERT triggers have no OLD row");
          }
          if (trigger.event === "delete" && binding.source === "new") {
            throw new TypeError("DELETE triggers have no NEW row");
          }
          if (!columnNames.has(binding.column)) {
            throw new TypeError(`Unknown trigger column: ${binding.column}`);
          }
        }
        const compiled = compileStatement(statement.sql);
        if (
          compiled.kind !== "insert" &&
          compiled.kind !== "update" &&
          compiled.kind !== "delete"
        ) {
          throw new TypeError("Trigger bodies support INSERT, UPDATE, and DELETE statements");
        }
        const target = tables.find((record) => record.name === compiled.table);
        if (target === undefined) throw new TypeError(`Table not found: ${compiled.table}`);
        if (target.id === table.id) {
          throw new TypeError("A trigger cannot write to its own table");
        }
        if (compiled.kind === "insert" && target.uniqueKeyColumnId !== undefined) {
          throw new TypeError(`Trigger bodies insert into keyless tables only: ${compiled.table}`);
        }
        if (compiled.kind === "insert") {
          // Validate the body's column list now: a body that can never fire successfully
          // must fail CREATE, not every later write to the trigger's table.
          for (const name of compiled.columns) {
            if (!target.columns.some((column) => column.name === name)) {
              throw new TypeError(`Trigger body INSERT column does not exist: ${name}`);
            }
          }
          for (const column of target.columns) {
            if (
              !compiled.columns.includes(column.name) &&
              !column.nullable &&
              column.defaultValue === undefined
            ) {
              throw new TypeError(
                `Trigger body INSERT omits a non-nullable column without a default: ${column.name}`,
              );
            }
          }
        }
        if (compiled.kind !== "insert" && target.uniqueKeyColumnId === undefined) {
          throw new TypeError(
            `Trigger body ${compiled.kind.toUpperCase()}s need a keyed table: ${compiled.table}`,
          );
        }
        // Cascades are allowed one level deep; the runtime budget errors on deeper chains.
      }
      try {
        const createdAt = this.#now().toISOString();
        await this.store.updateTable(table.id, table.revision ?? 0, {
          triggers: [...(table.triggers ?? []), { ...trigger, createdAt }],
        });
        // The CAS guards only this table's record, so a concurrent create of the same name on
        // a different table can slip through the pre-check. Re-read and settle it the same
        // way on both sides — earliest createdAt wins, table id breaks ties — so exactly one
        // survives and the loser removes its own entry.
        const loser = (await this.store.listTables()).some(
          (record) =>
            record.id !== table.id &&
            (record.triggers ?? []).some(
              (existing) =>
                existing.name === trigger.name &&
                (existing.createdAt < createdAt ||
                  (existing.createdAt === createdAt && record.id < table.id)),
            ),
        );
        if (loser) {
          await this.#removeTriggerFrom(table.id, trigger.name);
          throw new TypeError(`Trigger already exists: ${trigger.name}`);
        }
        return;
      } catch (error) {
        if (!(error instanceof TableRecordConflictError) || attempt >= 2) throw error;
      }
    }
  }

  /** Removes one named trigger from one specific table record, retrying the record CAS. */
  async #removeTriggerFrom(tableId: string, name: string): Promise<void> {
    for (let attempt = 0; ; attempt += 1) {
      const record = await this.store.getTable(tableId);
      if (record === undefined) return;
      const remaining = (record.triggers ?? []).filter((trigger) => trigger.name !== name);
      if (remaining.length === (record.triggers ?? []).length) return;
      try {
        await this.store.updateTable(record.id, record.revision ?? 0, { triggers: remaining });
        return;
      } catch (error) {
        if (!(error instanceof TableRecordConflictError) || attempt >= 2) throw error;
      }
    }
  }

  async #dropTrigger(name: string): Promise<void> {
    for (let attempt = 0; ; attempt += 1) {
      const tables = await this.store.listTables();
      const owner = tables.find((record) =>
        (record.triggers ?? []).some((trigger) => trigger.name === name),
      );
      if (owner === undefined) throw new TypeError(`Trigger not found: ${name}`);
      try {
        await this.store.updateTable(owner.id, owner.revision ?? 0, {
          triggers: (owner.triggers ?? []).filter((trigger) => trigger.name !== name),
        });
        return;
      } catch (error) {
        if (!(error instanceof TableRecordConflictError) || attempt >= 2) throw error;
      }
    }
  }

  /**
   * The firing plan for an upsert batch: rows whose key already names a visible row (in the
   * read the caller provides — committed state, or the session overlay) fire as UPDATEs with
   * that row as OLD, matching ON CONFLICT DO UPDATE semantics; the rest fire as INSERTs. An
   * in-batch duplicate key fires as an UPDATE of the earlier occurrence. Returns undefined
   * when the table has no insert or update triggers.
   */
  async #upsertTriggerFirings(
    table: TableRecord,
    keyColumn: TableColumnRecord,
    batch: ColumnarBatch,
    rowCount: number,
    readRows?: (sql: string, params: QueryValue[]) => Promise<QueryResult>,
  ): Promise<
    | {
        inserts: number[];
        updates: number[];
        oldImages: Array<Record<string, BatchValue> | undefined>;
      }
    | undefined
  > {
    const fires = (table.triggers ?? []).some(
      (trigger) => trigger.event === "insert" || trigger.event === "update",
    );
    if (!fires || rowCount === 0) return undefined;
    const quote = (name: string): string => `"${name.replaceAll('"', '""')}"`;
    const keyValues = batch.columns[keyColumn.name] ?? [];
    const params = keyValues.filter((value): value is Exclude<BatchValue, null> => value !== null);
    const placeholders = params.map(() => "?").join(", ");
    const sql = `SELECT * FROM ${quote(table.name)} WHERE ${quote(keyColumn.name)} IN (${placeholders})`;
    const result =
      readRows === undefined
        ? await this.query(sql, { params, memoize: false })
        : await readRows(sql, params);
    const byToken = new Map(
      result.rows.map((row) => {
        const key = row[keyColumn.name];
        return [
          key === null || key === undefined ? "" : keyToken(keyColumn.type, key),
          row,
        ] as const;
      }),
    );
    const inserts: number[] = [];
    const updates: number[] = [];
    const oldImages: Array<Record<string, BatchValue> | undefined> = [];
    const seenInBatch = new Map<string, number>();
    for (let rowIndex = 0; rowIndex < rowCount; rowIndex += 1) {
      const key = keyValues[rowIndex] ?? null;
      const token = key === null ? "" : keyToken(keyColumn.type, key);
      const earlier = seenInBatch.get(token);
      const old =
        earlier === undefined
          ? byToken.get(token)
          : Object.fromEntries(
              table.columns.map((column) => [
                column.name,
                batch.columns[column.name]?.[earlier] ?? null,
              ]),
            );
      if (old === undefined) {
        inserts.push(rowIndex);
        oldImages.push(undefined);
      } else {
        updates.push(rowIndex);
        oldImages.push(old);
      }
      seenInBatch.set(token, rowIndex);
    }
    return { inserts, updates, oldImages };
  }

  /** Fires INSERT and UPDATE triggers for one upsert batch at one timing, per the plan. */
  async #stageUpsertTriggerFirings(
    transaction: DatabaseTransaction,
    table: TableRecord,
    batch: ColumnarBatch,
    firings: {
      inserts: number[];
      updates: number[];
      oldImages: Array<Record<string, BatchValue> | undefined>;
    },
    timing: "before" | "after",
    cascadeBudget = 1,
  ): Promise<void> {
    const batchValue = (column: string, rowIndex: number): BatchValue =>
      batch.columns[column]?.[rowIndex] ?? null;
    await this.#stageTriggerDerivedInserts(
      transaction,
      table,
      "insert",
      firings.inserts.length,
      (source, column, index) =>
        source === "new" ? batchValue(column, firings.inserts[index] ?? -1) : null,
      timing,
      cascadeBudget,
    );
    await this.#stageTriggerDerivedInserts(
      transaction,
      table,
      "update",
      firings.updates.length,
      (source, column, index) => {
        const rowIndex = firings.updates[index] ?? -1;
        if (source === "new") return batchValue(column, rowIndex);
        return firings.oldImages[rowIndex]?.[column] ?? null;
      },
      timing,
      cascadeBudget,
    );
  }

  /**
   * Pre-images for UPDATE/DELETE trigger firing, keyed to the input order; fetched only when
   * the table has matching triggers. OLD reflects the values observed at statement start
   * (snapshot semantics), which is also exactly what the mutation replay supersedes.
   */
  async #triggerPreImages(
    table: TableRecord,
    keyColumn: TableColumnRecord,
    keys: ReadonlyArray<Exclude<BatchValue, null>>,
    event: "update" | "delete",
    readRows?: (sql: string, params: QueryValue[]) => Promise<QueryResult>,
    /** Reads the rows even with no trigger to feed — CHECK constraints need the post-image. */
    force = false,
  ): Promise<Array<Record<string, BatchValue> | undefined>> {
    if (!force && !(table.triggers ?? []).some((trigger) => trigger.event === event)) return [];
    const quote = (name: string): string => `"${name.replaceAll('"', '""')}"`;
    const placeholders = keys.map(() => "?").join(", ");
    const preImageSql = `SELECT * FROM ${quote(table.name)} WHERE ${quote(keyColumn.name)} IN (${placeholders})`;
    const result =
      readRows === undefined
        ? await this.query(preImageSql, { params: [...keys] as QueryValue[], memoize: false })
        : await readRows(preImageSql, [...keys] as QueryValue[]);
    const byToken = new Map(
      result.rows.map((row) => {
        const key = row[keyColumn.name];
        return [
          key === null || key === undefined ? "" : keyToken(keyColumn.type, key),
          row,
        ] as const;
      }),
    );
    return keys.map((key) => byToken.get(keyToken(keyColumn.type, key)));
  }

  /**
   * Fires the table's AFTER triggers for one write: every body statement binds once per
   * affected row (NEW/OLD values as positional parameters), the bound rows accumulate into
   * one derived insert per statement, and each derived segment stages into the SAME
   * transaction as the triggering write — the write and its derivations publish atomically,
   * so a crashed tab loses both or neither and no tab can observe one without the other.
   */
  async #stageTriggerDerivedInserts(
    transaction: DatabaseTransaction,
    table: TableRecord,
    event: "insert" | "update" | "delete",
    rowCount: number,
    valueAt: (source: "new" | "old", column: string, rowIndex: number) => BatchValue,
    timing: "before" | "after" = "after",
    cascadeBudget = 1,
  ): Promise<void> {
    const triggers = (table.triggers ?? []).filter(
      (trigger) => trigger.event === event && trigger.timing === timing,
    );
    if (triggers.length === 0 || rowCount === 0) return;
    if (cascadeBudget < 0) {
      throw new Error(`Trigger cascade depth exceeded at table: ${table.name}`);
    }
    for (const trigger of triggers) {
      for (const statement of trigger.statements) {
        const compiled = compileStatement(statement.sql);
        if (compiled.kind === "insert") {
          await this.#applyTriggerInsertBody(
            transaction,
            compiled,
            statement.bindings,
            rowCount,
            valueAt,
            cascadeBudget,
          );
          continue;
        }
        if (compiled.kind !== "update" && compiled.kind !== "delete") continue;
        await this.#applyTriggerMutationBody(
          transaction,
          compiled,
          statement.bindings,
          rowCount,
          valueAt,
          cascadeBudget,
        );
      }
    }
  }

  /** One trigger body INSERT: rows bound per affected row, staged as one derived segment. */
  async #applyTriggerInsertBody(
    transaction: DatabaseTransaction,
    compiled: Extract<CompiledStatement, { kind: "insert" }>,
    bindings: ReadonlyArray<{ source: "new" | "old"; column: string }>,
    rowCount: number,
    valueAt: (source: "new" | "old", column: string, rowIndex: number) => BatchValue,
    cascadeBudget: number,
  ): Promise<void> {
    const derivedRows: BatchValue[][] = [];
    for (let rowIndex = 0; rowIndex < rowCount; rowIndex += 1) {
      const params = bindings.map((binding) => valueAt(binding.source, binding.column, rowIndex));
      const bound = bindStatementParameters(compiled, params);
      if (bound.kind !== "insert") continue;
      for (const row of bound.rows) {
        derivedRows.push(
          row.map((value) =>
            typeof value === "object" && value !== null && "parameter" in value ? null : value,
          ),
        );
      }
    }
    const target = await this.#findTable(compiled.table);
    if (target.uniqueKeyColumnId !== undefined) {
      throw new TypeError(`Trigger bodies insert into keyless tables only: ${target.name}`);
    }
    const columns: Record<string, BatchValue[]> = {};
    compiled.columns.forEach((name, columnIndex) => {
      columns[name] = derivedRows.map((row) => row[columnIndex] ?? null);
    });
    // Unlisted columns pad with NULL exactly like a top-level INSERT: defaults fill their
    // null slots, nullable columns store NULL. CREATE TRIGGER rejects bodies omitting a
    // non-nullable column without a default.
    for (const column of target.columns) {
      if (!(column.name in columns)) {
        columns[column.name] = derivedRows.map(() => null);
      }
    }
    const filled = fillColumnDefaults(
      target,
      toColumnarBatch({ columns }),
      this.#now,
      derivedRows.length,
    );
    validateBatch(target, filled.batch, filled.autoIncrement?.column.name);
    if (filled.autoIncrement !== undefined && filled.autoIncrement.missingIndexes.length > 0) {
      const values = await this.store.reserveAutoIncrement(
        target.id,
        filled.autoIncrement.column.id,
        filled.autoIncrement.missingIndexes.length,
        filled.autoIncrement.atLeast,
      );
      patchAutoIncrementValues(filled.batch, filled.autoIncrement, values);
    }
    const derivedValueAt = (source: "new" | "old", column: string, rowIndex: number): BatchValue =>
      source === "new" ? (filled.batch.columns[column]?.[rowIndex] ?? null) : null;
    // One cascade level: the derived insert fires the target's own INSERT triggers with a
    // spent budget, so their bodies must not need a further level.
    await this.#stageTriggerDerivedInserts(
      transaction,
      target,
      "insert",
      derivedRows.length,
      derivedValueAt,
      "before",
      cascadeBudget - 1,
    );
    await this.#stageDerivedInsert(transaction, target, filled.batch, derivedRows.length);
    await this.#stageTriggerDerivedInserts(
      transaction,
      target,
      "insert",
      derivedRows.length,
      derivedValueAt,
      "after",
      cascadeBudget - 1,
    );
  }

  /**
   * One trigger body UPDATE or DELETE against a keyed target: the statement binds per
   * affected row, its matched target rows resolve at statement-start state, and the merged
   * keys (plus computed assignments for updates) stage through the same session machinery
   * atomic write scopes use — inside the same commit, with the target's own triggers fired
   * on a spent cascade budget. A body that touches one target row from two triggering rows
   * in one firing is rejected: the reads cannot compound.
   */
  async #applyTriggerMutationBody(
    transaction: DatabaseTransaction,
    compiled: Extract<CompiledStatement, { kind: "update" | "delete" }>,
    bindings: ReadonlyArray<{ source: "new" | "old"; column: string }>,
    rowCount: number,
    valueAt: (source: "new" | "old", column: string, rowIndex: number) => BatchValue,
    cascadeBudget: number,
  ): Promise<void> {
    const target = await this.#findTable(compiled.table);
    const keyColumn = getUniqueKeyColumn(target);
    if (keyColumn === undefined) {
      throw new TypeError(
        `Trigger body ${compiled.kind.toUpperCase()}s need a keyed table: ${target.name}`,
      );
    }
    const mergedKeys: BatchValue[] = [];
    const seenKeys = new Set<string>();
    const mergedChanges: Record<string, BatchValue[]> = {};
    for (let rowIndex = 0; rowIndex < rowCount; rowIndex += 1) {
      const params = bindings.map((binding) => valueAt(binding.source, binding.column, rowIndex));
      const bound = bindStatementParameters(compiled, params);
      if (bound.kind !== "update" && bound.kind !== "delete") continue;
      const referenced = new Set<string>([keyColumn.name]);
      if (bound.kind === "update") {
        for (const assignment of bound.assignments) {
          for (const column of expressionColumnNames(assignment.expression)) {
            referenced.add(column.split(".").at(-1) ?? column);
          }
        }
      }
      const plan: CompiledQuery = {
        sql: `(trigger ${bound.kind})`,
        base: { table: target.name, alias: target.name },
        joins: [],
        select: [...referenced].map((name) => ({
          expression: { kind: "column", reference: name },
          alias: name,
        })),
        predicates: bound.predicates,
        groupBy: [],
        having: [],
        orderBy: [],
      };
      // The read goes through the transaction's staged overlay, exactly like a scope's
      // tx.query: the body must see the triggering write, earlier statements of the same
      // firing, and everything the enclosing scope staged — reading only committed state
      // silently loses those updates.
      const matched = (await this.#sessionQueryPlan(transaction, plan)).rows;
      for (const row of matched) {
        const key = row[keyColumn.name];
        if (key === null || key === undefined) {
          throw new TypeError(`Unique key values must not be null: ${keyColumn.name}`);
        }
        const token = keyToken(keyColumn.type, key);
        if (seenKeys.has(token)) {
          throw new TypeError(
            `A trigger body ${bound.kind} touched the same ${target.name} row twice in one firing`,
          );
        }
        seenKeys.add(token);
        mergedKeys.push(key);
        if (bound.kind === "update") {
          for (const assignment of bound.assignments) {
            const value = evaluateRowExpression(assignment.expression, target.name, row);
            if (typeof value === "number" && !Number.isFinite(value)) {
              throw new TypeError(
                `UPDATE assignment produced a non-finite number: ${assignment.column}`,
              );
            }
            (mergedChanges[assignment.column] ??= []).push(value);
          }
        }
      }
    }
    if (mergedKeys.length === 0) return;
    if (compiled.kind === "update") {
      await this.#sessionUpdate(
        transaction,
        target.name,
        { keys: mergedKeys, changes: mergedChanges },
        cascadeBudget - 1,
      );
    } else {
      await this.#sessionDelete(transaction, target.name, { keys: mergedKeys }, cascadeBudget - 1);
    }
  }

  /** Encodes and stages one derived insert segment into an already-open transaction. */
  async #stageDerivedInsert(
    transaction: DatabaseTransaction,
    table: TableRecord,
    batch: ColumnarBatch,
    rowCount: number,
  ): Promise<void> {
    await this.#stageInsertSegment(transaction, table, batch, rowCount, "insert");
  }

  /** Encodes and stages one insert/upsert segment into an already-open transaction. */
  async #stageInsertSegment(
    transaction: DatabaseTransaction,
    table: TableRecord,
    batch: ColumnarBatch,
    rowCount: number,
    kind: "insert" | "upsert",
    reservedRowIds?: RowIdRange,
  ): Promise<string> {
    const rowIds = reservedRowIds ?? (await this.store.reserveRowIds(table.id, rowCount));
    const segmentId = this.#createId();
    const columnBlockIds: Record<string, string[]> = {};
    const blockWrites: Array<{ id: string; bytes: Uint8Array }> = [];
    for (const column of table.columns) {
      const values = batch.columns[column.name] ?? [];
      const blockIds: string[] = [];
      for (let start = 0, part = 0; start < rowCount; start += this.#rowsPerBlock, part += 1) {
        const slice = values.slice(start, Math.min(start + this.#rowsPerBlock, rowCount));
        const bytes = await this.#encodeColumnBlock(column.id, asColumnInput(column.type, slice));
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
      columnBlockIds[column.id] = blockIds;
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
    await transaction.stageArtifacts(blockWrites, [segment]);
    return segmentId;
  }

  /**
   * Runs the callback against one shared write transaction: every staged mutation — across
   * any number of keyed or keyless tables, with their AFTER triggers — publishes as one
   * atomic commit. A concurrent *write* surfaces as a WriteConflictError from the scope
   * (nothing published); retry the whole scope. Background self-maintenance is not a
   * concurrent write: the scope rebases over data-neutral manifests and commits anyway, so
   * compaction landing mid-scope never fails it. An error thrown by the callback aborts the
   * scope with nothing published. A scope that stages nothing publishes nothing.
   */
  async write<T>(
    action: (session: WriteSession) => Promise<T>,
  ): Promise<{ result: T; version: number | null }> {
    return this.#openWriteScope((session) => action(session));
  }

  /**
   * `write()` with the scope's transaction handed to the callback as well, for the internal
   * callers that need to read through it by plan rather than by SQL text — the keyed mutation
   * paths a statement-level transaction routes through.
   */
  async #openWriteScope<T>(
    action: (session: WriteSession, transaction: DatabaseTransaction) => Promise<T>,
  ): Promise<{ result: T; version: number | null }> {
    const transaction = await this.#transactions.begin();
    let closed = false;
    let staged = 0;
    /**
     * A stage that throws *after* registering part of its work (key membership, full-text
     * deltas, derived segments, blocks) cannot be undone statement-by-statement, so the scope
     * is poisoned: even if the callback catches the error, the commit refuses rather than
     * publishing the fragment. A stage that fails validation before registering anything —
     * updating a missing key, say — leaves the scope clean and usable.
     */
    let poisoned: unknown;
    const open = (): void => {
      if (closed) throw new Error("The write scope has ended");
      if (poisoned !== undefined) {
        throw new Error("The write scope already failed and can only roll back", {
          cause: poisoned,
        });
      }
    };
    const guarded = async <T>(run: () => Promise<T>): Promise<T> => {
      const before = transaction.stagedWorkCount;
      try {
        return await run();
      } catch (error) {
        if (transaction.stagedWorkCount !== before) poisoned ??= error;
        throw error;
      }
    };
    const session: WriteSession = {
      query: async (sql, options) => {
        open();
        return this.#sessionQuery(transaction, sql, options);
      },
      insertBatch: async (tableName, input) => {
        open();
        staged += 1;
        return guarded(() => this.#sessionInsert(transaction, tableName, input, "insert"));
      },
      upsertBatch: async (tableName, input) => {
        open();
        staged += 1;
        return guarded(() => this.#sessionInsert(transaction, tableName, input, "upsert"));
      },
      updateBatch: async (tableName, input) => {
        open();
        staged += 1;
        return guarded(() => this.#sessionUpdate(transaction, tableName, input));
      },
      deleteBatch: async (tableName, input) => {
        open();
        staged += 1;
        return guarded(() => this.#sessionDelete(transaction, tableName, input));
      },
    };
    try {
      const result = await action(session, transaction);
      closed = true;
      if (poisoned !== undefined) {
        // The outer catch aborts the transaction.
        throw new Error(
          `The write scope failed mid-stage and was rolled back: ${
            poisoned instanceof Error ? poisoned.message : "staging failed"
          }`,
          { cause: poisoned },
        );
      }
      if (staged === 0) {
        await transaction.abort();
        return { result, version: await this.store.getCurrentManifestVersion() };
      }
      for (let attempt = 0; attempt <= this.#maxCommitRetries; attempt += 1) {
        try {
          const manifest = await transaction.commit();
          this.#afterCommit(manifest);
          return { result, version: manifest.version };
        } catch (error) {
          if (!(error instanceof WriteConflictError) || attempt === this.#maxCommitRetries) {
            throw error;
          }
          // Losing the version CAS to background self-maintenance is spurious: compaction
          // publishes a manifest that changed no row anywhere, so it cannot conflict with
          // what this scope read or staged. Rebase across those and commit again. The scope
          // still does not rebase over a commit that actually changed data — a concurrent
          // write loses the race explicitly, exactly as before.
          const from = transaction.snapshotVersion;
          const rebased = await transaction.rebase();
          if (!(await this.#commitsChangedNoData(from, rebased.version))) throw error;
        }
      }
      throw new Error("Commit retry limit was exceeded");
    } catch (error) {
      closed = true;
      await transaction.abort().catch(() => undefined);
      throw error;
    }
  }

  /**
   * A scope's read: committed state at the scope's lease plus this transaction's staged
   * segments overlaid as committed — ordered after every committed segment, in staging
   * order — with the staged blocks readable through a snapshot wrapper. Nothing about the
   * overlay leaves the call: caches key on committed identities only.
   */
  async #sessionQuery(
    transaction: DatabaseTransaction,
    sql: string,
    options: { params?: QueryOptions["params"] } = {},
  ): Promise<QueryResult> {
    const bound = bindPlanParameters(this.#compileCached(sql), options.params);
    // The same catalog rewrites a read outside a scope gets: a scope that could not see a view
    // would make the scope's reads a different language from everyone else's.
    const plan = await this.#applyCatalogRewrites(bound);
    return this.#sessionQueryPlan(transaction, plan);
  }

  /** #sessionQuery for an already-bound plan: trigger bodies resolve their reads through it. */
  async #sessionQueryPlan(
    transaction: DatabaseTransaction,
    plan: CompiledQuery,
  ): Promise<QueryResult> {
    const names = collectRealTableNames(plan);
    const tables = await Promise.all(names.map((name) => this.#findTable(name)));
    const realTables = new Map(tables.map((table) => [table.name, table] as const));
    const pendingIds = new Set(transaction.pendingSegmentIds);
    const pendingBlocks = new Set(transaction.pendingBlockIds);
    const ourRecord = await this.store.getTransaction(transaction.id);
    // Pinned to the scope's own snapshot, not the current manifest: the documented contract
    // is "the pre-scope snapshot plus everything this scope staged", and #assertKeysExist
    // already validates against transaction.snapshotVersion — reading the current version
    // here would let the two disagree mid-scope about a concurrent commit.
    return this.#withLeasedSnapshot(transaction.snapshotVersion, async (snapshot) => {
      const overlaySnapshot = {
        get version() {
          return snapshot.version;
        },
        get expiresAt() {
          return snapshot.expiresAt;
        },
        hasBlock: (blockId: string) => pendingBlocks.has(blockId) || snapshot.hasBlock(blockId),
        renew: (ttlMs: number) => snapshot.renew(ttlMs),
        release: () => snapshot.release(),
      } as unknown as LeasedSnapshot;
      const segmentsByTable = new Map<string, SegmentRecord[]>();
      const transactionRecords = new Map<string, TransactionRecord>();
      for (const table of tables) {
        const segments = await this.store.listSegments(table.id);
        // Staged segments sort after every committed one; commitOrdinal keeps their
        // staging order (the journal's id list is sorted, so it cannot).
        const doctored = segments.map((segment) =>
          pendingIds.has(segment.id)
            ? { ...segment, logicalOrder: STAGED_OVERLAY_ORDER_BASE + (segment.commitOrdinal ?? 0) }
            : segment,
        );
        segmentsByTable.set(table.id, doctored);
        for (const record of await this.#transactionRecordsForSegments(doctored)) {
          transactionRecords.set(record.id, record);
        }
      }
      if (ourRecord !== undefined) {
        transactionRecords.set(transaction.id, {
          ...ourRecord,
          status: "committed",
          committedVersion: STAGED_OVERLAY_ORDER_BASE,
        });
      }
      const visibility: SegmentVisibilityCatalog = {
        transactions: transactionRecords,
        segmentsByTable,
      };
      return this.#queryAtVisibility(plan, overlaySnapshot, visibility, realTables);
    });
  }

  /** Executes one plan at an explicit snapshot and visibility, covering every plan shape. */
  async #queryAtVisibility(
    plan: CompiledQuery,
    snapshot: LeasedSnapshot,
    visibility: SegmentVisibilityCatalog,
    realTables: ReadonlyMap<string, TableRecord>,
    cacheResults = true,
  ): Promise<QueryResult> {
    plan = expandFtsColumns(plan, (name) => searchableFtsColumns(realTables.get(name)));
    const typedSchemas = new Map<string, SqlColumnSchema[]>(
      [...realTables.values()].map((table) => [
        table.name,
        table.columns.map(({ name, type }) => ({ name, type })),
      ]),
    );
    const memory = new QueryMemoryContext();
    try {
      const resolution = subqueryResolutionSteps(plan);
      for (const step of resolution.steps) {
        step.substitute(
          await this.#executeBlock(
            step.block,
            snapshot,
            visibility,
            memory,
            realTables,
            typedSchemas,
            undefined,
            cacheResults,
          ),
        );
      }
      const wrapper = transparentProjectionSource(resolution.plan);
      if (wrapper !== undefined) {
        return projectResultColumns(
          await this.#executeBlock(
            wrapper.inner,
            snapshot,
            visibility,
            memory,
            realTables,
            typedSchemas,
            undefined,
            cacheResults,
          ),
          wrapper.aliases,
        );
      }
      return await this.#executeBlock(
        resolution.plan,
        snapshot,
        visibility,
        memory,
        realTables,
        typedSchemas,
        undefined,
        cacheResults,
      );
    } finally {
      memory.close();
    }
  }

  /** Net staged key membership per table, replayed from the transaction's entries. */
  #stagedKeyOverlay(
    transaction: DatabaseTransaction,
    tableId: string,
  ): { added: Set<string>; removed: Set<string> } {
    const added = new Set<string>();
    const removed = new Set<string>();
    for (const entry of transaction.accumulatedUniqueKeyChanges) {
      if (entry.tableId !== tableId) continue;
      for (const token of entry.keyTokens) {
        if (entry.remove === true) {
          added.delete(token);
          removed.add(token);
        } else {
          removed.delete(token);
          added.add(token);
        }
      }
    }
    return { added, removed };
  }

  async #sessionInsert(
    transaction: DatabaseTransaction,
    tableName: string,
    input: InsertBatchInput,
    kind: "insert" | "upsert",
    cascadeBudget = 1,
  ): Promise<StagedWriteResult> {
    const table = await this.#findTable(tableName);
    const { batch, autoIncrement, rowCount } = this.#fillDefaults(table, input);
    await this.#assertForeignKeysPresent(
      table,
      (column) => batch.columns[column] ?? [],
      (sql, params) => this.#sessionQuery(transaction, sql, { params }),
      transaction,
    );
    if (autoIncrement !== undefined && autoIncrement.missingIndexes.length > 0) {
      const values = await this.store.reserveAutoIncrement(
        table.id,
        autoIncrement.column.id,
        autoIncrement.missingIndexes.length,
        autoIncrement.atLeast,
      );
      patchAutoIncrementValues(batch, autoIncrement, values);
      const patched = batch.columns[autoIncrement.column.name] ?? [];
      for (const rowIndex of autoIncrement.missingIndexes) {
        validateValue(autoIncrement.column, patched[rowIndex] ?? null, rowIndex);
      }
    }
    const keys = batchKeys(table, batch);
    if (keys !== undefined) {
      transaction.setUniqueKeyChanges({
        tableId: table.id,
        keyTokens: [...keys.keys()],
        requireAbsent: kind === "insert",
      });
    }
    const rowIds = await this.store.reserveRowIds(table.id, rowCount);
    if (kind === "insert") {
      const ftsDeltas = buildFtsColumnDeltas(table, batch, rowIds.start);
      if (ftsDeltas.length > 0) {
        transaction.setFtsChanges({ tableId: table.id, columns: ftsDeltas });
      }
    }
    const insertValueAt = (source: "new" | "old", column: string, rowIndex: number): BatchValue =>
      source === "new" ? (batch.columns[column]?.[rowIndex] ?? null) : null;
    // Session upserts classify against the scope's own staged state, so a row inserted
    // earlier in the scope makes a later upsert of its key fire as an UPDATE.
    const sessionUpsertKeyColumn = kind === "upsert" ? getUniqueKeyColumn(table) : undefined;
    const sessionUpsertFirings =
      sessionUpsertKeyColumn === undefined
        ? undefined
        : await this.#upsertTriggerFirings(
            table,
            sessionUpsertKeyColumn,
            batch,
            rowCount,
            (sql, params) => this.#sessionQuery(transaction, sql, { params }),
          );
    if (kind === "insert") {
      await this.#stageTriggerDerivedInserts(
        transaction,
        table,
        "insert",
        rowCount,
        insertValueAt,
        "before",
        cascadeBudget,
      );
    } else if (sessionUpsertFirings !== undefined) {
      await this.#stageUpsertTriggerFirings(
        transaction,
        table,
        batch,
        sessionUpsertFirings,
        "before",
        cascadeBudget,
      );
    }
    const segmentId = await this.#stageInsertSegment(
      transaction,
      table,
      batch,
      rowCount,
      kind,
      rowIds,
    );
    if (kind === "insert") {
      await this.#stageTriggerDerivedInserts(
        transaction,
        table,
        "insert",
        rowCount,
        insertValueAt,
        "after",
        cascadeBudget,
      );
    } else if (sessionUpsertFirings !== undefined) {
      await this.#stageUpsertTriggerFirings(
        transaction,
        table,
        batch,
        sessionUpsertFirings,
        "after",
        cascadeBudget,
      );
    }
    return { tableName: table.name, segmentId, rowCount };
  }

  async #sessionUpdate(
    transaction: DatabaseTransaction,
    tableName: string,
    input: UpdateBatchInput,
    cascadeBudget = 1,
  ): Promise<StagedWriteResult> {
    const table = await this.#findTable(tableName);
    const keyColumn = getUniqueKeyColumn(table);
    if (keyColumn === undefined) {
      throw new TypeError(`Table needs a unique key before rows can be updated: ${table.name}`);
    }
    const keys = validateUpdateBatch(table, keyColumn, input);
    // Read-your-writes membership: keys staged by this scope pass, keys the scope removed
    // fail, and everything else checks against the committed snapshot as usual.
    const overlay = this.#stagedKeyOverlay(transaction, table.id);
    for (const [token, value] of keys) {
      if (overlay.removed.has(token)) {
        throw new MissingKeyError(table.name, keyColumn.name, value);
      }
    }
    const committedKeys = new Map([...keys].filter(([token]) => !overlay.added.has(token)));
    if (committedKeys.size > 0) {
      await this.#assertKeysExist(table, keyColumn, transaction.snapshotVersion, committedKeys);
    }
    await this.#assertForeignKeysPresent(
      table,
      (column) => input.changes[column] ?? [],
      (sql, params) => this.#sessionQuery(transaction, sql, { params }),
      transaction,
    );
    const sessionChecks = table.checks ?? [];
    const preImages = await this.#triggerPreImages(
      table,
      keyColumn,
      input.keys.filter((key): key is Exclude<BatchValue, null> => key !== null),
      "update",
      (preImageSql, params) => this.#sessionQuery(transaction, preImageSql, { params }),
      sessionChecks.length > 0,
    );
    const sessionUpdateValueAt = (
      source: "new" | "old",
      column: string,
      rowIndex: number,
    ): BatchValue => {
      if (source === "old") return preImages[rowIndex]?.[column] ?? null;
      const changed = input.changes[column];
      return changed === undefined
        ? (preImages[rowIndex]?.[column] ?? null)
        : (changed[rowIndex] ?? null);
    };
    // The same post-image rule as a standalone update, so a scope — and the partial upsert that
    // runs inside one — cannot write a row a direct UPDATE would refuse.
    if (sessionChecks.length > 0) {
      for (let rowIndex = 0; rowIndex < input.keys.length; rowIndex += 1) {
        if (preImages[rowIndex] === undefined) continue;
        const row: Record<string, BatchValue> = {};
        for (const column of table.columns) {
          row[column.name] = sessionUpdateValueAt("new", column.name, rowIndex);
        }
        assertRowChecks(table, row, rowIndex);
      }
    }
    await this.#stageTriggerDerivedInserts(
      transaction,
      table,
      "update",
      input.keys.length,
      sessionUpdateValueAt,
      "before",
      cascadeBudget,
    );
    const changedColumns = Object.keys(input.changes).sort();
    const columns = [keyColumn, ...changedColumns.map((name) => findColumn(table, name))];
    const segmentId = this.#createId();
    const columnBlockIds: Record<string, string[]> = {};
    const blockWrites: Array<{ id: string; bytes: Uint8Array }> = [];
    for (const column of columns) {
      const values = column.id === keyColumn.id ? input.keys : (input.changes[column.name] ?? []);
      const blockIds: string[] = [];
      for (
        let start = 0, part = 0;
        start < input.keys.length;
        start += this.#rowsPerBlock, part += 1
      ) {
        const slice = values.slice(start, Math.min(start + this.#rowsPerBlock, input.keys.length));
        const bytes = await this.#encodeColumnBlock(column.id, asColumnInput(column.type, slice));
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
      columnBlockIds[column.id] = blockIds;
    }
    await transaction.stageArtifacts(blockWrites, [
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
    await this.#stageTriggerDerivedInserts(
      transaction,
      table,
      "update",
      input.keys.length,
      sessionUpdateValueAt,
      "after",
      cascadeBudget,
    );
    return { tableName: table.name, segmentId, rowCount: input.keys.length };
  }

  async #sessionDelete(
    transaction: DatabaseTransaction,
    tableName: string,
    input: DeleteBatchInput,
    cascadeBudget = 1,
    referentialBudget = REFERENTIAL_CASCADES,
  ): Promise<StagedWriteResult> {
    const table = await this.#findTable(tableName);
    const keyColumn = getUniqueKeyColumn(table);
    if (keyColumn === undefined) {
      throw new TypeError(`Table needs a unique key before rows can be deleted: ${table.name}`);
    }
    // The dependents go first and in this same scope, so a cascade publishes with its cause.
    await this.#applyReferentialActions(
      table,
      [...input.keys],
      {
        query: (sql, options) => this.#sessionQuery(transaction, sql, options ?? {}),
        updateBatch: (name, update) =>
          this.#sessionUpdate(transaction, name, update, cascadeBudget),
        deleteBatch: (name, remove) =>
          this.#sessionDelete(transaction, name, remove, cascadeBudget, referentialBudget - 1),
      },
      referentialBudget,
    );
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
    transaction.setUniqueKeyChanges({
      tableId: table.id,
      keyTokens: [...keys.keys()],
      requireAbsent: false,
      remove: true,
    });
    // Fire only per existing row (session-visible state included): missing keys must not
    // produce phantom all-null OLD images.
    const preImages = (
      await this.#triggerPreImages(
        table,
        keyColumn,
        [...keys.values()],
        "delete",
        (preImageSql, params) => this.#sessionQuery(transaction, preImageSql, { params }),
      )
    ).filter((row) => row !== undefined);
    const sessionDeleteValueAt = (
      source: "new" | "old",
      column: string,
      rowIndex: number,
    ): BatchValue => (source === "old" ? (preImages[rowIndex]?.[column] ?? null) : null);
    await this.#stageTriggerDerivedInserts(
      transaction,
      table,
      "delete",
      preImages.length,
      sessionDeleteValueAt,
      "before",
      cascadeBudget,
    );
    const values = [...keys.values()];
    const segmentId = this.#createId();
    const blockIds: string[] = [];
    const blockWrites: Array<{ id: string; bytes: Uint8Array }> = [];
    for (let start = 0, part = 0; start < values.length; start += this.#rowsPerBlock, part += 1) {
      const slice = values.slice(start, Math.min(start + this.#rowsPerBlock, values.length));
      const bytes = await this.#encodeColumnBlock(
        keyColumn.id,
        asColumnInput(keyColumn.type, slice),
      );
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
    }
    await transaction.stageBlocks(blockWrites);
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
    await this.#stageTriggerDerivedInserts(
      transaction,
      table,
      "delete",
      preImages.length,
      sessionDeleteValueAt,
      "after",
      cascadeBudget,
    );
    return { tableName: table.name, segmentId, rowCount: keys.size };
  }

  #notifyLiveCommit(): void {
    for (const set of this.#liveSets) set.notifyLocalCommit();
  }

  /** Executes a built ORM query through the same streaming-first pipeline as compiled SQL. */
  async run<TRow>(query: {
    kind: "typed-query";
    plan: CompiledQuery;
    __row?: TRow;
  }): Promise<TRow[]> {
    const probe = this.store.getCatalogProbe?.bind(this.store);
    // The same memo a SQL query gets, keyed by the plan: a typed query is compiled once by the
    // builder and run many times, and it used to re-execute on every run.
    if (probe === undefined || query.plan.usesStatementDatetime === true) {
      return (await this.#queryCompiled(query.plan)).rows as TRow[];
    }
    return (await this.#memoizedQuery(query.plan, `typed ${planMemoKey(query.plan)}`, {}, probe))
      .rows as TRow[];
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
    options: MigrateOptions = {},
  ): Promise<{
    createdTables: string[];
    alteredTables: string[];
    droppedTables: string[];
    replacedViews: string[];
    droppedViews: string[];
    steps: MigrationStep[];
  }> {
    // Table revisions also move under background full-text index activity (build stamps,
    // stale-writer invalidation), so a lost compare-and-swap no longer implies a concurrent
    // migrator. Migration is idempotent by construction — re-plan from fresh records and retry;
    // a genuinely concurrent migrator still fails explicitly once the retries are exhausted.
    for (let attempt = 0; ; attempt += 1) {
      try {
        return await this.#migrateOnce(definition, options);
      } catch (error) {
        if (!(error instanceof TableRecordConflictError) || attempt >= 2) throw error;
      }
    }
  }

  /**
   * A declared view states the columns its author expects. The engine infers the real output
   * schema when it creates the view; if the two disagree the declaration is a lie that every
   * reader would inherit, so the migration fails here instead.
   */
  #assertViewMatchesDeclaration(record: TableRecord | undefined, step: { view: AnyView }): void {
    if (record === undefined) throw new Error(`View was not created: ${step.view.name}`);
    const declared = Object.entries(step.view.columns).map(([name, definition]) => ({
      name,
      type: definition.type,
    }));
    const inferred = record.columns.map(({ name, type }) => ({ name, type }));
    const render = (columns: Array<{ name: string; type: string }>): string =>
      columns.map(({ name, type }) => `${name} ${type}`).join(", ");
    if (render(declared) !== render(inferred)) {
      throw new TypeError(
        `View ${step.view.name} declares (${render(declared)}) but its query produces ` +
          `(${render(inferred)})`,
      );
    }
  }

  /**
   * Whether any visible row holds NULL in this column, proven from block headers alone.
   *
   * Every block records its own null count, and that field is covered by the envelope checksum
   * the format authenticates independently of the payload — the same guarantee zone-map pruning
   * already relies on. So tightening a column to NOT NULL costs one header read per block of that
   * column, not a scan of its values: bytes are fetched, but nothing is decompressed or decoded.
   *
   * A segment written before the column existed contributes NULLs for all its rows unless the
   * column carries a backfill, which is exactly what makes those rows non-null.
   */
  async #columnHoldsNull(table: TableRecord, columnId: string): Promise<boolean> {
    const record = table.columns.find((candidate) => candidate.id === columnId);
    if (record === undefined) return true;
    const snapshot = await this.#transactions.openSnapshot();
    const segments = await this.#visibleSegmentRecords(table, snapshot);
    for (const segment of segments) {
      if (segment.rowCount === 0) continue;
      const kind = segment.kind ?? "insert";
      // Only append-shaped histories are provable this way: a delete or update segment means a
      // row's live value is decided by replay, which headers cannot settle.
      if (kind !== "insert" && kind !== "base") return true;
      const blockIds = segment.columnBlockIds[columnId];
      if (blockIds === undefined || blockIds.length === 0) {
        if (record.backfill === undefined) return true;
        continue;
      }
      for (let start = 0; start < blockIds.length; start += 16) {
        const window = blockIds.slice(start, start + 16);
        const fetched = await this.store.getBlocks(window);
        for (const bytes of fetched) {
          if (bytes === undefined) return true;
          if (inspectBlock(bytes).nullCount > 0) return true;
        }
      }
    }
    return false;
  }

  /**
   * Bumps a column's auto-increment counter past every key already stored, so adopting the
   * generator on a populated table cannot mint an id that collides with one already written.
   *
   * The largest key comes from the numeric zone map each block carries in its header, so this
   * costs one header read per block of the key column — no decode, no scan. A block without a
   * zone map (or a non-numeric column) makes the maximum unknowable this way, and the migration
   * is refused rather than seeded from an incomplete picture.
   */
  async #seedAutoIncrement(table: TableRecord, columnName: string): Promise<void> {
    const column = table.columns.find(({ name }) => name === columnName);
    if (column === undefined) return;
    if (column.type !== "number") {
      throw new TypeError(`Auto-increment requires a number column: ${table.name}.${columnName}`);
    }
    const snapshot = await this.#transactions.openSnapshot();
    const segments = await this.#visibleSegmentRecords(table, snapshot);
    let largest = 0;
    for (const segment of segments) {
      if (segment.rowCount === 0) continue;
      const blockIds = segment.columnBlockIds[column.id] ?? [];
      if (blockIds.length === 0) continue;
      for (let start = 0; start < blockIds.length; start += 16) {
        const fetched = await this.store.getBlocks(blockIds.slice(start, start + 16));
        for (const bytes of fetched) {
          if (bytes === undefined) {
            throw new TypeError(
              `Auto-increment cannot be adopted for ${table.name}.${columnName}: a block is missing`,
            );
          }
          const zoneMap = inspectBlock(bytes).metadata.zoneMap;
          if (zoneMap === undefined) {
            throw new TypeError(
              `Auto-increment cannot be adopted for ${table.name}.${columnName}: a block carries ` +
                `no zone map, so the largest existing key is unknown without a scan`,
            );
          }
          if (zoneMap.max > largest) largest = zoneMap.max;
        }
      }
    }
    if (largest <= 0) return;
    await this.store.reserveAutoIncrement(table.id, column.id, 0, BigInt(Math.trunc(largest)) + 1n);
  }

  async #migrateOnce(
    definition: SchemaDefinition<readonly AnyTable[]>,
    options: MigrateOptions = {},
  ): Promise<{
    createdTables: string[];
    alteredTables: string[];
    droppedTables: string[];
    replacedViews: string[];
    droppedViews: string[];
    steps: MigrationStep[];
  }> {
    // One listTables pass drives planning and execution: a create step exists only because the
    // table was absent from this snapshot, and each altered table applies all of its steps in a
    // single compare-and-swap, so a migration costs one catalog write per changed table however
    // many tables or steps the schema carries. A concurrent creator or migrator still fails
    // explicitly through createTable's uniqueness check or the revision conflict.
    const records = await this.store.listTables();
    const recordsByName = new Map(records.map((record) => [record.name, record]));
    const plan = planMigration(toCatalog(records), definition, {
      ...(options.schemaOwnsDatabase === undefined
        ? {}
        : { schemaOwnsDatabase: options.schemaOwnsDatabase }),
    });
    // A migration runs when an application opens, with nobody to review it. A schema file that
    // drifted — a rename typed wrong, a branch checked out — would otherwise delete rows on
    // launch, so destroying anything is a decision the caller makes, not a default.
    if (options.allowDestructive !== true) {
      const destructive = plan.steps.filter(isDestructiveStep);
      if (destructive.length > 0) {
        const described = destructive
          .map((step) =>
            step.kind === "drop-table"
              ? `table ${step.tableName}`
              : `column ${step.tableName}.${step.columnName}`,
          )
          .join(", ");
        throw new TypeError(
          `This migration would destroy data: ${described}. Pass { allowDestructive: true } to ` +
            `apply it, or restore the declarations.`,
        );
      }
    }
    const createdTables: string[] = [];
    const replacedViews: string[] = [];
    const droppedViews: string[] = [];
    const droppedTables: string[] = [];
    const alterationsByTable = new Map<string, MigrationStep[]>();
    for (const step of plan.steps) {
      if (step.kind === "create-table") {
        const entries = Object.entries(step.table.columns);
        const uniqueEntry = entries.find(([, columnDefinition]) => columnDefinition.isUnique);
        // Constraints ride along with the columns: a table declared with a relation or a row
        // condition must reject the same writes whichever path created it. Dropping them here
        // is what made `migrate()` produce a weaker table than the equivalent SQL DDL.
        const foreignKeys = declaredForeignKeys(step.table);
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
            ...(columnDefinition.backfillValue === undefined
              ? {}
              : {
                  backfill:
                    typeof columnDefinition.backfillValue === "function"
                      ? columnDefinition.backfillValue()
                      : columnDefinition.backfillValue,
                }),
          })),
          ...(foreignKeys.length === 0 ? {} : { foreignKeys }),
          ...(step.table.checks.length === 0 ? {} : { checks: [...step.table.checks] }),
          managed: true,
        });
        createdTables.push(step.table.name);
        continue;
      }
      // Views carry no data, so they are applied directly rather than batched into a table's
      // atomic column rewrite. `orReplace` makes create and redefine one path.
      if (step.kind === "replace-view") {
        await this.createView(step.view.name, step.view.sql, {
          orReplace: true,
          managed: true,
        });
        this.#assertViewMatchesDeclaration(await this.store.getTableByName(step.view.name), step);
        replacedViews.push(step.view.name);
        continue;
      }
      if (step.kind === "drop-table") {
        await this.dropTable(step.tableName, { ifExists: true });
        droppedTables.push(step.tableName);
        continue;
      }
      if (step.kind === "drop-view") {
        await this.dropView(step.viewName, { ifExists: true });
        droppedViews.push(step.viewName);
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
      // Tightening to NOT NULL is the one step that has to be earned rather than declared: the
      // catalog cannot promise what the stored rows contain, so prove it from block headers
      // before the batched compare-and-swap makes it durable.
      for (const step of steps) {
        if (step.kind !== "tighten-nullable") continue;
        const target = record.columns.find(({ name }) => name === step.columnName);
        if (target === undefined) continue;
        if (await this.#columnHoldsNull(record, target.id)) {
          throw new TypeError(
            `Column cannot tighten to non-null: ${tableName}.${step.columnName} holds NULL in ` +
              `stored rows. Give it a value everywhere, or declare it nullable.`,
          );
        }
      }
      // Adopting auto-increment seeds the counter past the largest key already stored, so a
      // generated id can never collide with one already written.
      for (const step of steps) {
        if (step.kind !== "set-auto-increment" || !step.enabled) continue;
        await this.#seedAutoIncrement(record, step.columnName);
      }
      // A renamed column that a trigger references would silently bind NULL forever (NEW/OLD
      // bindings) or fail every firing (body target columns), so reject the rename instead.
      for (const step of steps) {
        if (step.kind !== "rename-column") continue;
        for (const owner of records) {
          for (const trigger of owner.triggers ?? []) {
            const referencesBinding =
              owner.id === record.id &&
              trigger.statements.some((statement) =>
                statement.bindings.some((binding) => binding.column === step.from),
              );
            const referencesBody = trigger.statements.some((statement) => {
              const compiled = compileStatement(statement.sql);
              if (compiled.kind === "insert") {
                return compiled.table === record.name && compiled.columns.includes(step.from);
              }
              if (compiled.kind !== "update" && compiled.kind !== "delete") return false;
              if (compiled.table !== record.name) return false;
              const referenced = new Set<string>();
              for (const predicate of compiled.predicates) {
                for (const side of [predicate.left, predicate.right]) {
                  if (side.kind === "column") {
                    referenced.add(side.reference.split(".").at(-1) ?? side.reference);
                  }
                }
              }
              if (compiled.kind === "update") {
                for (const assignment of compiled.assignments) {
                  referenced.add(assignment.column);
                  for (const column of expressionColumnNames(assignment.expression)) {
                    referenced.add(column.split(".").at(-1) ?? column);
                  }
                }
              }
              return referenced.has(step.from);
            });
            if (referencesBinding || referencesBody) {
              throw new TypeError(
                `Cannot rename ${record.name}.${step.from}: trigger ${trigger.name} references it`,
              );
            }
          }
        }
      }
      const columns = applyColumnSteps(record, steps, this.#createId);
      if (JSON.stringify(columns) === JSON.stringify(record.columns)) continue;
      await this.store.updateTable(record.id, record.revision ?? 0, { columns });
      alteredTables.push(tableName);
    }
    return {
      createdTables,
      alteredTables,
      droppedTables,
      replacedViews,
      droppedViews,
      steps: plan.steps,
    };
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
    if (this.#canStreamPlanShape(reported, {})) {
      notes.push("streams the base scan through resident block windows");
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
    if (statement.kind === "transaction") return this.#runTransactionStatement(statement.action);
    if (statement.kind === "select") {
      return {
        kind: "rows",
        result: await this.query(statement.sql, params === undefined ? {} : { params }),
      };
    }
    const open = this.#openTransaction;
    if (open !== undefined) {
      // Inside BEGIN … COMMIT every write stages into that one scope, so the statements publish
      // together or not at all. Schema changes are refused rather than silently auto-committed:
      // the catalog is not part of the scope, so a DDL statement inside one would land even if
      // the transaction rolled back.
      if (!isTransactionalStatement(statement)) {
        throw new TypeError(
          `${statement.kind.toUpperCase().replace("-", " ")} is not allowed inside a transaction`,
        );
      }
      return this.#duringTransaction(open, () =>
        this.runStatement(bindStatementParameters(statement, params), { writer: open.session }),
      );
    }
    return this.runStatement(bindStatementParameters(statement, params));
  }

  /**
   * E141-04 on the child side: every non-null value written into a referencing column must name
   * a row that exists in the parent. The probe is the parent's own unique-key membership — the
   * same index the keyed write paths address rows by — and only what that index cannot settle
   * is read through the writing transaction, so a child inserted next to its parent in one
   * scope still sees it.
   */
  async #assertForeignKeysPresent(
    table: TableRecord,
    rowsByColumn: (column: string) => readonly BatchValue[],
    read: (sql: string, params: QueryValue[]) => Promise<QueryResult>,
    transaction?: DatabaseTransaction,
  ): Promise<void> {
    for (const key of table.foreignKeys ?? []) {
      const values = rowsByColumn(key.column);
      const distinct = [...new Set(values.filter((value): value is QueryValue => value !== null))];
      if (distinct.length === 0) continue;
      // A NULL reference names no parent, which the standard leaves satisfied.
      const parent = await this.store.getTableByName(key.parentTable);
      if (parent === undefined) {
        throw new TypeError(
          `FOREIGN KEY ${key.name} references a missing table: ${key.parentTable}`,
        );
      }
      const wanted = [
        ...(await this.#foreignKeyValuesNeedingRead(parent, key, distinct, transaction)),
      ];
      if (wanted.length === 0) continue;
      const found = await read(
        `SELECT ${quoteSqlIdentifier(key.parentColumn)} AS parent_key FROM ${quoteSqlIdentifier(
          key.parentTable,
        )} WHERE ${quoteSqlIdentifier(key.parentColumn)} IN (${wanted.map(() => "?").join(", ")})`,
        wanted,
      );
      // Compared by the same token the keyed paths use, so 1 and '1' cannot pass for each other.
      const token = (value: QueryValue): string =>
        value instanceof Date ? `d${value.toISOString()}` : `${typeof value}:${String(value)}`;
      const present = new Set(
        found.rows.flatMap((row) => {
          const value = row.parent_key ?? null;
          return value === null ? [] : [token(value)];
        }),
      );
      const missing = wanted.find((value) => !present.has(token(value)));
      if (missing !== undefined) {
        throw new TypeError(
          `FOREIGN KEY ${key.name} has no ${key.parentTable} row with ${key.parentColumn} ${String(missing)}`,
        );
      }
    }
  }

  /**
   * Which referenced values a FOREIGN KEY probe still has to read rows for. A parent whose
   * persistent unique-key lookup is ready answers membership with point reads instead of a
   * scan, so the ordinary case — every referenced parent already committed — costs no table
   * read at all. Keys an open scope staged count as present, keys it removed stay unproven,
   * and anything the lookup cannot decide falls through to the caller's transactional read,
   * which is also what reports the violation.
   */
  async #foreignKeyValuesNeedingRead(
    parent: TableRecord,
    key: NonNullable<TableRecord["foreignKeys"]>[number],
    wanted: readonly QueryValue[],
    transaction: DatabaseTransaction | undefined,
  ): Promise<readonly QueryValue[]> {
    if (parent.uniqueKeyLookupReady !== true) return wanted;
    const parentKey = getUniqueKeyColumn(parent);
    if (parentKey?.name !== key.parentColumn) return wanted;
    const tokens = new Map<QueryValue, string>();
    for (const value of wanted) {
      // A value the key encoding rejects (wrong type for the parent key, a non-finite number)
      // is a violation the read reports with its own message rather than a thrown encoding error.
      const token = tryKeyToken(parentKey.type, value);
      if (token === undefined) return wanted;
      tokens.set(value, token);
    }
    const overlay =
      transaction === undefined ? undefined : this.#stagedKeyOverlay(transaction, parent.id);
    const probed = [...new Set(tokens.values())].filter(
      (token) => overlay?.added.has(token) !== true,
    );
    const present =
      probed.length === 0
        ? new Set<string>()
        : new Set(await this.store.getExistingUniqueKeys(parent.id, probed));
    return wanted.filter((value) => {
      const token = tokens.get(value) ?? "";
      if (overlay?.removed.has(token) === true) return true;
      if (overlay?.added.has(token) === true) return false;
      return !present.has(token);
    });
  }

  /**
   * E141-04 on the parent side: what a delete does to the rows referencing the parent it
   * removes. RESTRICT refuses, CASCADE deletes them, SET NULL clears the reference — each
   * applied inside the deleting transaction, so the parent and its dependents publish together.
   * A cascade that reaches another parent cascades again, bounded like a trigger chain.
   */
  async #applyReferentialActions(
    parent: TableRecord,
    keys: readonly QueryValue[],
    session: {
      query: (sql: string, options?: { params?: QueryOptions["params"] }) => Promise<QueryResult>;
      updateBatch: (table: string, input: UpdateBatchInput) => Promise<unknown>;
      deleteBatch: (table: string, input: DeleteBatchInput) => Promise<unknown>;
    },
    cascadeBudget: number,
  ): Promise<void> {
    if (keys.length === 0) return;
    const children = await this.#childForeignKeys(parent.name);
    if (children.length === 0) return;
    if (cascadeBudget < 0) {
      throw new Error(`Referential cascade depth exceeded at table: ${parent.name}`);
    }
    const placeholders = keys.map(() => "?").join(", ");
    for (const { table: child, key } of children) {
      const childKeyColumn = getUniqueKeyColumn(child);
      const selected =
        childKeyColumn === undefined
          ? quoteSqlIdentifier(key.column)
          : `${quoteSqlIdentifier(childKeyColumn.name)} AS child_key`;
      const affected = await session.query(
        `SELECT ${selected} FROM ${quoteSqlIdentifier(child.name)} WHERE ${quoteSqlIdentifier(
          key.column,
        )} IN (${placeholders})`,
        { params: [...keys] },
      );
      if (affected.rows.length === 0) continue;
      if (key.onDelete === "restrict") {
        throw new TypeError(
          `FOREIGN KEY ${key.name} still has ${String(affected.rows.length)} ${child.name} row(s) referencing ${parent.name}`,
        );
      }
      if (childKeyColumn === undefined) {
        throw new TypeError(
          `FOREIGN KEY ${key.name} needs a unique key on ${child.name} to ${key.onDelete}`,
        );
      }
      const childKeys = affected.rows
        .map((row) => row.child_key ?? null)
        .filter((value): value is Exclude<BatchValue, null> => value !== null);
      if (key.onDelete === "cascade") {
        await session.deleteBatch(child.name, { keys: childKeys });
        continue;
      }
      await session.updateBatch(child.name, {
        keys: childKeys,
        changes: { [key.column]: childKeys.map(() => null) },
      });
    }
  }

  /** Runs one statement inside an open transaction, holding off the idle sweep while it does. */
  async #duringTransaction<T>(
    state: { busy: number; activeAt: number },
    run: () => Promise<T>,
  ): Promise<T> {
    state.busy += 1;
    state.activeAt = this.#now().getTime();
    try {
      return await run();
    } finally {
      state.busy -= 1;
      state.activeAt = this.#now().getTime();
    }
  }

  /**
   * BEGIN / COMMIT / ROLLBACK (E151). The scope is the same one `write()` opens, held between
   * statements instead of around a callback: every write stages into it, reads see what it has
   * staged, COMMIT publishes, and ROLLBACK discards.
   *
   * An abandoned transaction is the hazard a statement-level BEGIN introduces — a lost reference
   * or a closed tab would otherwise hold a scope open forever — so one that goes untouched for
   * `transactionIdleTimeoutMs` rolls itself back, and closing the database rolls back whatever
   * is open.
   */
  async #runTransactionStatement(action: "begin" | "commit" | "rollback"): Promise<ExecuteResult> {
    if (action === "begin") {
      if (this.#openTransaction !== undefined) {
        throw new TypeError("A transaction is already open; COMMIT or ROLLBACK it first");
      }
      let start!: (writer: StatementWriter) => void;
      const opened = new Promise<StatementWriter>((resolve) => {
        start = resolve;
      });
      let settle!: (outcome: "commit" | "rollback") => void;
      const decided = new Promise<"commit" | "rollback">((resolve) => {
        settle = resolve;
      });
      const finished = this.#openWriteScope<null>(async (session, transaction) => {
        start({
          ...session,
          queryPlan: (plan) => this.#sessionQueryPlan(transaction, plan),
        });
        if ((await decided) === "rollback") throw new TransactionRollback();
        return null;
      });
      const session = await opened;
      const state = {
        session,
        settle,
        busy: 0,
        activeAt: this.#now().getTime(),
        finished: finished.then(
          ({ version }) => ({ version }),
          (error: unknown) => {
            if (error instanceof TransactionRollback) return { version: null };
            throw error;
          },
        ),
        timer: setInterval(() => {
          // Abandoned means nothing has run for the whole timeout — not that the timeout
          // elapsed. A statement in flight, however slow, and one that finished a moment ago
          // both count as activity; only a caller that walked away trips this.
          if (this.#openTransaction !== state || state.busy > 0) return;
          if (this.#now().getTime() - state.activeAt < this.#transactionIdleTimeoutMs) return;
          this.#openTransaction = undefined;
          clearInterval(state.timer);
          state.settle("rollback");
        }, this.#transactionIdleTimeoutMs),
      };
      // An idle sweep must never keep a process alive on its own; browsers have no unref.
      if (typeof state.timer === "object" && "unref" in state.timer) state.timer.unref();
      this.#openTransaction = state;
      return { kind: "transaction", action: "begin" };
    }
    const open = this.#openTransaction;
    if (open === undefined) {
      throw new TypeError(`${action.toUpperCase()} without an open transaction`);
    }
    this.#openTransaction = undefined;
    clearInterval(open.timer);
    open.settle(action === "commit" ? "commit" : "rollback");
    const { version } = await open.finished;
    return {
      kind: "transaction",
      action,
      ...(version === null || version === undefined ? {} : { version }),
    };
  }

  /**
   * ON CONFLICT (key) DO UPDATE SET with a column subset: rows whose key exists merge only the
   * assigned EXCLUDED columns through the keyed update path, and the rest insert. Classification,
   * update, insert, and RETURNING all run in one write scope, so any failure aborts the statement.
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
    for (const name of statement.columns) {
      if (!table.columns.some((column) => column.name === name)) {
        throw new TypeError(`INSERT column does not exist: ${name}`);
      }
    }
    const returningColumns =
      options.returning === undefined
        ? undefined
        : options.returning === "*"
          ? table.columns.map(({ name }) => name)
          : [...options.returning];
    for (const name of returningColumns ?? []) {
      if (!table.columns.some((column) => column.name === name)) {
        throw new TypeError(`RETURNING column does not exist: ${name}`);
      }
    }
    const assigned = statement.onConflict.columns ?? [];
    const keyToken = (value: QueryValue): string =>
      value instanceof Date ? `d${value.toISOString()}` : `${typeof value} ${String(value)}`;
    const { result: returnedRows, version } = await this.write(async (transaction) => {
      const existing = await this.#existingInsertKeys(table, keyColumn, statement, (sql, params) =>
        transaction.query(sql, { params }),
      );
      const freshRows: InsertValue[][] = [];
      const conflictingRows: InsertValue[][] = [];
      for (const row of statement.rows) {
        const key = boundInsertValue(row[keyIndex] ?? null);
        if (key !== null && existing.has(keyToken(key))) conflictingRows.push(row);
        else freshRows.push(row);
      }
      if (conflictingRows.length > 0) {
        const changes: Record<string, BatchValue[]> = {};
        for (const name of assigned) {
          const columnIndex = statement.columns.indexOf(name);
          changes[name] = conflictingRows.map((row) => boundInsertValue(row[columnIndex] ?? null));
        }
        const keys = conflictingRows.map((row) => {
          const value = boundInsertValue(row[keyIndex] ?? null);
          if (value === null) {
            throw new TypeError(`Unique key values must not be null: ${keyColumn.name}`);
          }
          return value;
        });
        await transaction.updateBatch(table.name, { keys, changes });
      }
      if (freshRows.length > 0) {
        const columns: Record<string, BatchValue[]> = {};
        statement.columns.forEach((column, index) => {
          columns[column] = freshRows.map((row) => boundInsertValue(row[index] ?? null));
        });
        for (const column of table.columns) {
          if (!(column.name in columns)) columns[column.name] = freshRows.map(() => null);
        }
        await transaction.insertBatch(table.name, { columns });
      }
      if (returningColumns === undefined) return undefined;
      const keys = statement.rows.map((row) => boundInsertValue(row[keyIndex] ?? null));
      const selectedColumns = [...new Set([keyColumn.name, ...returningColumns])];
      const selected = selectedColumns.map(quoteSqlIdentifier).join(", ");
      const result = await transaction.query(
        `SELECT ${selected} FROM ${quoteSqlIdentifier(table.name)} WHERE ${quoteSqlIdentifier(keyColumn.name)} IN (${keys.map(() => "?").join(", ")})`,
        { params: keys },
      );
      const rowsByKey = new Map(
        result.rows.map((row) => {
          const key = row[keyColumn.name] ?? null;
          return [keyToken(key), row] as const;
        }),
      );
      return keys.map((key) => {
        const row = rowsByKey.get(keyToken(key)) ?? {};
        return Object.fromEntries(returningColumns.map((name) => [name, row[name] ?? null]));
      });
    });
    return {
      kind: "insert",
      table: statement.table,
      rowCount: statement.rows.length,
      ...(version === null ? {} : { version }),
      ...(returnedRows === undefined ? {} : { returnedRows }),
    };
  }

  /** Existing unique keys among an insert statement's rows, read at one snapshot. */
  async #existingInsertKeys(
    table: TableRecord,
    keyColumn: TableRecord["columns"][number],
    statement: Extract<CompiledStatement, { kind: "insert" }>,
    query: (sql: string, params: readonly QueryValue[]) => Promise<QueryResult> = (sql, params) =>
      this.query(sql, { params }),
  ): Promise<Set<string>> {
    const keyIndex = statement.columns.indexOf(keyColumn.name);
    const keys = statement.rows
      .map((row) => boundInsertValue(row[keyIndex] ?? null))
      .filter((value): value is Exclude<QueryValue, null> => value !== null);
    if (keys.length === 0) return new Set();
    const keyToken = (value: QueryValue): string =>
      value instanceof Date ? `d${value.toISOString()}` : `${typeof value} ${String(value)}`;
    const result = await query(
      `SELECT ${quoteSqlIdentifier(keyColumn.name)} AS key FROM ${quoteSqlIdentifier(table.name)} WHERE ${quoteSqlIdentifier(keyColumn.name)} IN (${keys.map(() => "?").join(", ")})`,
      keys,
    );
    return new Set(result.rows.map((row) => keyToken(row.key ?? null)));
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
    // The streaming-first pipeline: a keyed IN list narrows to the blocks that can hold the
    // keys, where the prepared path materialized the table's columns first.
    const existing = new Set<QueryValue | string>(
      (await this.#queryCompiled(plan)).rows.map((row) => {
        const value = row.key ?? null;
        return value instanceof Date ? value.toISOString() : value;
      }),
    );
    return {
      ...statement,
      rows: statement.rows.filter((row) => {
        const value = boundInsertValue(row[keyIndex] ?? null);
        return !existing.has(value instanceof Date ? value.toISOString() : value);
      }),
    };
  }

  /** Runs an INSERT ... SELECT source at one snapshot and materializes it as literal rows. */
  /**
   * MERGE (F312). One pass over the source, joined to the target on the match condition, decides
   * each row's branch; the branches then apply as ordinary batched writes inside a single write
   * scope, so the whole statement publishes atomically and fires the same triggers the
   * equivalent INSERT, UPDATE, and DELETE would.
   *
   * The match condition has to equate the target's unique key with something from the source:
   * that is what the keyed write paths address rows by, and it is also why a source row can
   * match at most one target row — the standard's cardinality violation cannot arise here.
   */
  async #runMerge(
    statement: Extract<CompiledStatement, { kind: "merge" }>,
  ): Promise<ExecuteResult> {
    const target = await this.#findTable(statement.table);
    const keyColumn = getUniqueKeyColumn(target);
    if (keyColumn === undefined) {
      throw new TypeError(`MERGE requires a table with a unique key: ${target.name}`);
    }
    const sourceKey = mergeSourceKeyExpression(statement, keyColumn.name);
    // The source's key value per row, and the target row it matches (or none).
    const sourceSql = mergeSourceSql(statement);
    const { result: applied, version } = await this.write(async (session) => {
      const sourceRows = (await session.query(sourceSql)).rows;
      const keys = sourceRows.map((row) =>
        evaluateJoinedRowExpression(sourceKey, { [statement.source.alias]: row }),
      );
      const present = new Map<string, DatabaseRow>();
      const wanted = keys.filter((key): key is QueryValue => key !== null);
      if (wanted.length > 0) {
        const existing = await session.query(
          `SELECT * FROM ${quoteSqlIdentifier(target.name)} WHERE ${quoteSqlIdentifier(keyColumn.name)} IN (${wanted
            .map(() => "?")
            .join(", ")})`,
          { params: wanted },
        );
        for (const row of existing.rows) {
          const key = row[keyColumn.name];
          if (key !== null && key !== undefined) present.set(keyToken(keyColumn.type, key), row);
        }
      }
      const { updates, deletes, inserts } = classifyMergeRows({
        statement,
        target,
        keyColumn,
        sourceRows,
        keys,
        present,
      });
      // Applied in one order per kind, so a row can only be touched by the branch that claimed
      // it: updates, then deletes, then the fresh rows.
      let changedRows = 0;
      const assignedColumns = [
        ...new Set([...updates.values()].flatMap((entry) => Object.keys(entry.changes))),
      ];
      if (updates.size > 0) {
        const entries = [...updates.values()];
        const changes: Record<string, BatchValue[]> = {};
        for (const column of assignedColumns) {
          changes[column] = entries.map((entry) => entry.changes[column] ?? null);
        }
        const result = await session.updateBatch(target.name, {
          keys: entries.map((entry) => entry.key),
          changes,
        });
        changedRows += result.rowCount;
      }
      if (deletes.size > 0) {
        const result = await session.deleteBatch(target.name, {
          keys: [...deletes.values()] as Array<Exclude<BatchValue, null>>,
        });
        changedRows += result.rowCount;
      }
      if (inserts.length > 0) {
        const columns: Record<string, BatchValue[]> = {};
        for (const column of target.columns) {
          columns[column.name] = inserts.map((row) => row[column.name] ?? null);
        }
        const result = await session.insertBatch(target.name, { columns });
        changedRows += result.rowCount;
      }
      return changedRows;
    });
    return {
      kind: "merge",
      table: target.name,
      rowCount: applied,
      ...(version === null ? {} : { version }),
    };
  }

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
      if (statement.ifNotExists === true) {
        const existing = await this.store.getTableByName(statement.table);
        if (existing !== undefined) return { kind: "create-table", table: statement.table };
      }
      await this.createTable({
        name: statement.table,
        columns: statement.columns,
        ...(statement.checks === undefined ? {} : { checks: statement.checks }),
        ...(statement.foreignKeys === undefined ? {} : { foreignKeys: statement.foreignKeys }),
        ...(statement.uniqueKey === undefined ? {} : { uniqueKey: statement.uniqueKey }),
      });
      return { kind: "create-table", table: statement.table };
    }
    if (statement.kind === "create-table-as") {
      // T172: the query runs first, its output schema becomes the table's, and its rows are the
      // table's first insert — one statement, but the same createTable and insertBatch paths.
      const existing = await this.store.getTableByName(statement.table);
      if (existing !== undefined) {
        if (statement.ifNotExists === true) {
          return { kind: "create-table", table: statement.table };
        }
        throw new TypeError(`Table already exists: ${statement.table}`);
      }
      const records = await this.store.listTables();
      const schemas = new Map(
        records.map((record) => [
          record.name,
          record.columns.map(({ name, type }) => ({ name, type })),
        ]),
      );
      const columns = inferBlockSchema(statement.query, schemas).map(({ name, type }) => ({
        name,
        type,
        nullable: true,
      }));
      const result = await this.#queryCompiled(statement.query);
      await this.createTable({ name: statement.table, columns });
      if (result.rows.length > 0) {
        await this.insertBatch(statement.table, result.rows);
      }
      return { kind: "create-table", table: statement.table };
    }
    if (statement.kind === "add-column") {
      // F031-04. Existing rows have no value for the new column, so it is always nullable;
      // a DEFAULT fills the rows written afterwards.
      const added = statement.column;
      const record = await this.store.getTableByName(statement.table);
      if (record === undefined) throw new TypeError(`Unknown table: ${statement.table}`);
      if (added.nullable !== true) {
        throw new TypeError(
          "ALTER TABLE ADD COLUMN adds a nullable column; existing rows have no value for it",
        );
      }
      if (record.columns.some(({ name }) => name === added.name)) {
        throw new TypeError(`Column already exists: ${statement.table}.${added.name}`);
      }
      const columns = [
        ...structuredClone(record.columns),
        {
          id: this.#createId(),
          name: added.name,
          type: added.type,
          nullable: true,
          ...(added.defaultValue === undefined ? {} : { defaultValue: added.defaultValue }),
        },
      ];
      await this.store.updateTable(record.id, record.revision ?? 0, { columns });
      return { kind: "add-column", table: statement.table, column: added.name };
    }
    if (statement.kind === "merge") return this.#runMerge(statement);
    if (statement.kind === "create-view") {
      await this.createView(statement.view, statement.sql, {
        ...(statement.orReplace === true ? { orReplace: true } : {}),
      });
      return { kind: "create-view", view: statement.view };
    }
    if (statement.kind === "drop-view") {
      const dropped = await this.dropView(statement.view, {
        ...(statement.ifExists === true ? { ifExists: true } : {}),
      });
      return { kind: "drop-view", view: statement.view, dropped };
    }
    if (statement.kind === "drop-table") {
      const dropped = await this.dropTable(statement.table, {
        ...(statement.ifExists === true ? { ifExists: true } : {}),
      });
      return { kind: "drop-table", table: statement.table, dropped };
    }
    if (statement.kind === "create-trigger") {
      await this.#createTrigger(statement.table, statement.trigger);
      return { kind: "create-trigger", table: statement.table, name: statement.trigger.name };
    }
    if (statement.kind === "drop-trigger") {
      await this.#dropTrigger(statement.name);
      return { kind: "drop-trigger", name: statement.name };
    }
    if (statement.kind === "transaction") return this.#runTransactionStatement(statement.action);
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
      const writer = options.writer;
      // A scope stages and reports rows; a standalone write also publishes a version and any
      // values the engine generated. Both shapes answer here, and the narrow one has less to
      // report rather than something different.
      const result: StagedWriteResult & {
        version?: number;
        generatedColumns?: Record<string, BatchValue[]>;
      } = await (viaUpsert
        ? (writer?.upsertBatch(statement.table, { columns }) ??
          this.upsertBatch(statement.table, { columns }))
        : (writer?.insertBatch(statement.table, { columns }) ??
          this.insertBatch(statement.table, { columns })));
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
      const generated = "generatedColumns" in result ? (result.generatedColumns ?? {}) : {};
      return {
        kind: "insert",
        table: statement.table,
        rowCount: result.rowCount,
        // A staged write has no version yet: the scope it belongs to has not published.
        ...(result.version === undefined ? {} : { version: result.version }),
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
    let rows: QueryResult["rows"];
    if (options.writer !== undefined) {
      // Inside a scope the rows to touch are the ones the scope can see, which includes what it
      // has already staged — an UPDATE after an INSERT in the same transaction finds that row.
      rows = (await options.writer.queryPlan(plan)).rows;
    } else {
      // The same streaming-first pipeline a SELECT takes: its zone pruning and ascending-range
      // narrowing find the rows to touch, where the prepared path materialized the table's
      // columns first — most of a bulk delete's cost, at 200k rows.
      rows = (await this.#queryCompiled(plan)).rows;
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
      const deleted: { deletedRowCount?: number; rowCount?: number; version?: number | null } =
        options.writer === undefined
          ? await this.deleteBatch(table.name, { keys: keys as BatchValue[] })
          : await options.writer.deleteBatch(table.name, { keys: keys as BatchValue[] });
      return {
        kind: "delete",
        table: table.name,
        rowCount: deleted.deletedRowCount ?? deleted.rowCount ?? 0,
        ...(deleted.version === undefined || deleted.version === null
          ? {}
          : { version: deleted.version }),
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
    const updated: { updatedRowCount?: number; rowCount?: number; version?: number | null } =
      options.writer === undefined
        ? await this.updateBatch(table.name, { keys: keys as BatchValue[], changes })
        : await options.writer.updateBatch(table.name, { keys: keys as BatchValue[], changes });
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
      rowCount: updated.updatedRowCount ?? updated.rowCount ?? 0,
      ...(updated.version === undefined || updated.version === null
        ? {}
        : { version: updated.version }),
      ...(returnedRows === undefined ? {} : { returnedRows }),
    };
  }

  /**
   * A plan shape can stream its scan input: every asynchronous execution path, including both
   * spill paths, consumes scan rows batch-forward and copies values out, so a sliding resident
   * window can back the scan source while join build sides stay fully materialized. A self-join
   * is excluded because the base table would also be probed as a build side at arbitrary rows.
   * Streaming is the default execution: scan cost stays proportional to blocks actually read
   * through the buffer pool instead of a whole-table materialization per prepared snapshot.
   * The one excluded option combination is an explicit budget with spill disabled, where the
   * materialized path owns the memory-budget error contract.
   */
  #canStreamPlanShape(plan: CompiledQuery, options: QueryOptions): boolean {
    return (
      (options.executionMemoryBudgetBytes === undefined || options.spillToStorage !== false) &&
      // WITH TIES cannot stop at the limit: whether the next row belongs is only known after
      // reading it, so those plans take the materialized path that trims the ordered result.
      plan.limitWithTies !== true &&
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
    probe?: CatalogProbe,
  ): Promise<QueryResult | undefined> {
    const tableNames = [plan.base.table, ...plan.joins.map((join) => join.table)];
    const uniqueTableNames = [...new Set(tableNames)];
    if (options.version === undefined) {
      // The common path shares the probe-gated catalog state and the shared reader lease
      // with every other statement at the current version.
      return this.#withSharedCatalogSnapshot(
        uniqueTableNames,
        (snapshot, realTables, visibility) =>
          this.#queryStreamedAtSnapshot(
            plan,
            options,
            spillPageRows,
            snapshot,
            [...realTables.values()],
            visibility,
          ),
        probe,
      );
    }
    // Explicit time travel keeps the per-call lease and version-anchored reads.
    const tables = await Promise.all(uniqueTableNames.map((name) => this.#findTable(name)));
    return this.#withLeasedSnapshot(options.version, async (snapshot) => {
      const tableIds = new Set(tables.map((table) => table.id));
      const only = tables.length === 1 ? tables[0] : undefined;
      const segmentRecords =
        only !== undefined
          ? await this.store.listSegments(only.id)
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
      return this.#queryStreamedAtSnapshot(
        plan,
        options,
        spillPageRows,
        snapshot,
        tables,
        visibility,
      );
    });
  }

  async #queryStreamedAtSnapshot(
    plan: CompiledQuery,
    options: QueryOptions,
    spillPageRows: number | undefined,
    snapshot: LeasedSnapshot,
    tables: readonly TableRecord[],
    visibility: SegmentVisibilityCatalog,
  ): Promise<QueryResult | undefined> {
    // Copy-on-write: expansion clones only full-text plans, leaving the compile cache's copy
    // untouched.
    plan = expandFtsColumns(plan, (tableName) =>
      searchableFtsColumns(tables.find((table) => table.name === tableName)),
    );
    const schemas = new Map(
      tables.map((table) => [table.name, table.columns.map(({ name }) => name)]),
    );
    const columns = referencedColumns(plan, schemas);
    // Streaming must choose the scan/build orientation before creating its sliding base view.
    // Plain append histories have exact row counts in segment metadata; mutation histories keep
    // their written order because their visible cardinality requires replay to determine.
    const visibleByTable = new Map<string, SegmentRecord[]>();
    const rowCounts = new Map<string, { rowCount: number }>();
    for (const table of tables) {
      const segments = await this.#visibleSegmentRecords(table, snapshot, visibility);
      visibleByTable.set(table.name, segments);
      if (
        segments.every((segment) => {
          const kind = segment.kind ?? "insert";
          return kind === "insert" || kind === "base";
        })
      ) {
        rowCounts.set(table.name, {
          rowCount: segments.reduce((total, segment) => total + segment.rowCount, 0),
        });
      }
    }
    if (rowCounts.size === tables.length) plan = chooseJoinOrder(plan, rowCounts);
    const baseTable = tables.find((table) => table.name === plan.base.table);
    if (baseTable === undefined) throw new TypeError(`Unknown table: ${plan.base.table}`);
    const requestedBaseColumns = columns.get(baseTable.name) ?? [];
    const projectedBaseColumns =
      requestedBaseColumns.length === 0 ? [] : resolveReadColumns(baseTable, requestedBaseColumns);
    {
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
      const visibleBaseSegments =
        visibleByTable.get(freshBaseTable.name) ??
        (await this.#visibleSegmentRecords(freshBaseTable, snapshot, visibility));
      this.#maybeScheduleAutoCompaction(freshBaseTable, visibleBaseSegments);
      const baseSegments = await this.#ftsPrunedSegments(
        freshBaseTable,
        visibleBaseSegments,
        plan,
        snapshot,
      );
      // Zone-map elimination composes after index pruning: whole row groups whose statistics
      // reject the plan's predicates never stream at all.
      const zonePruned = await this.#zonePrunedStreamSegments(
        plan,
        freshBaseTable,
        projectedBaseColumns,
        baseSegments,
        snapshot,
      );
      const baseView = this.#streamedViewFactory(
        baseTable,
        projectedBaseColumns,
        zonePruned?.segments ?? baseSegments,
        snapshot,
        zonePruned?.storedBlocks,
        zonePruned !== undefined,
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
            const buildSegments =
              visibleByTable.get(buildTable.name) ??
              (await this.#visibleSegmentRecords(buildTable, snapshot, visibility));
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
              const streamedResult = await prepared.executeAsync({
                loadScanWindow: streamed.load,
              });
              options.onStats?.({ peakMemoryBytes: memory.usage.peakBytes });
              return streamedResult;
            } catch (error) {
              if (error instanceof QueryMemoryBudgetError) return undefined;
              throw error;
            }
          }
          const spilledResult = await prepared.executeAsync({
            ...(spillPageRows === undefined ? {} : { spillPageRows }),
            spillStore: this.#leasedSpillStore(),
            loadScanWindow: streamed.load,
          });
          options.onStats?.({ peakMemoryBytes: memory.usage.peakBytes });
          return spilledResult;
        } finally {
          if (prepared === undefined) memory.close();
          else prepared.close();
        }
      };
      // Without a budget or an explicit spill request there is nothing to spill for: the
      // scan streams in windows and retained state grows unbounded exactly like the
      // materialized path would.
      const spill = options.spillToStorage ?? options.executionMemoryBudgetBytes !== undefined;
      if (!spill) return runStreamedExecution({ spill: false });
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
    }
  }

  /**
   * Checks a table's visible history for streamed-scan eligibility and returns a factory that
   * builds a fresh forward-only streamed view per attempt or pass. Append/base histories stream
   * directly; update/delete keyed histories stream through the mutation replay; histories with
   * upsert segments (which interleave new rows into slot order at their segment position) and
   * shapes whose blocks cannot back a block-driven loader return undefined so the caller keeps
   * the materialized path.
   */
  /**
   * Zone-map row-group pruning for the streamed scan: header-only inspection of the
   * predicate columns' blocks selects the block indexes that can match, and the scan then
   * streams only those row groups — the same elimination the materialized pruned projection
   * performs, paid at scan time instead of cached under a mutable fingerprint. The inspected
   * bytes ride along so surviving predicate blocks decode without a second store fetch.
   * Returns undefined when the shape cannot prune (no zone predicates, a mutation history
   * whose slot arithmetic depends on every row group, scan-derived BM25 statistics, or
   * misaligned per-column block counts); the caller keeps the original segments.
   */
  async #zonePrunedStreamSegments(
    plan: CompiledQuery,
    table: TableRecord,
    projectedColumns: readonly TableColumnRecord[],
    segments: readonly SegmentRecord[],
    snapshot: LeasedSnapshot,
  ): Promise<{ segments: SegmentRecord[]; storedBlocks: Map<string, Uint8Array> } | undefined> {
    const mutations = segments.filter(
      (segment) => segment.kind === "delete" || segment.kind === "update",
    );
    const appends = segments.filter((segment) => {
      const kind = segment.kind ?? "insert";
      return kind === "insert" || kind === "base";
    });
    if (appends.length + mutations.length !== segments.length) return undefined;
    const predicates = zonePredicates(plan, table);
    if (predicates.length === 0) return undefined;
    if (planContainsFts(plan, "bm25") && !this.#ftsIndexServesScoring(plan, table, segments)) {
      return undefined;
    }
    const predicateColumns = [
      ...new Map(predicates.map((predicate) => [predicate.column.id, predicate.column])).values(),
    ];
    const keyColumn = getUniqueKeyColumn(table);
    if (mutations.length > 0) {
      // The replay addresses base rows by their slot in the streamed scan, so the key column
      // has to be pruned in lockstep with everything else the scan reads.
      if (keyColumn === undefined) return undefined;
      // An update can move a row's value into a predicate's range, which the base block's zone
      // map cannot know: eliminating that row group would drop a row the update makes match.
      // A delete only ever removes rows, so it composes with elimination unchanged.
      //
      // The key column is the exception, and it is the one that matters most. An update segment
      // always stores it — that is how the replay addresses the rows it patches — but it never
      // *changes* it, because updating a unique key is refused outright (see
      // validateUpdateBatch). Reading its presence as a hazard is what made a single updated row
      // demote every later keyed lookup to a full scan: `WHERE id = ?` has a predicate on the
      // key, every update segment carries the key, so pruning switched off and stayed off until
      // compaction. At 200k rows that is a point lookup at 7.6ms instead of 0.06ms, scaling with
      // the table from there.
      const hazardColumns = predicateColumns.filter((column) => column.id !== keyColumn.id);
      if (
        mutations.some(
          (segment) =>
            segment.kind === "update" &&
            hazardColumns.some((column) => (segment.columnBlockIds[column.id]?.length ?? 0) > 0),
        )
      ) {
        return undefined;
      }
    }
    const involvedColumns = [
      ...new Map(
        [
          ...projectedColumns,
          ...predicateColumns,
          ...(mutations.length > 0 && keyColumn !== undefined ? [keyColumn] : []),
        ].map((column) => [column.id, column]),
      ).values(),
    ];
    if (
      involvedColumns.some((column) =>
        appends.some((segment) => segment.columnBlockIds[column.id] === undefined),
      )
    ) {
      return undefined;
    }
    const storedBlocks = new Map<string, Uint8Array>();
    const descriptions = new Map<string, ReturnType<typeof inspectBlock>>();
    const predicateBlockIds = [
      ...new Set(
        appends.flatMap((segment) =>
          predicateColumns.flatMap((column) => segment.columnBlockIds[column.id] ?? []),
        ),
      ),
    ];
    // Block descriptions are immutable per id, like decoded blocks, so a repeated pruned
    // query pays zero store reads for elimination once its descriptions are resident.
    const missingIds = predicateBlockIds.filter((id) => {
      const cached = this.#cacheGet(`zi ${id}`) as ReturnType<typeof inspectBlock> | undefined;
      if (cached === undefined) return true;
      descriptions.set(id, cached);
      return false;
    });
    for (let start = 0; start < missingIds.length; start += 16) {
      await this.#renewInternalLeaseIfNeeded(snapshot);
      const ids = missingIds.slice(start, start + 16);
      const blocks = await this.store.getBlocks(ids);
      for (let index = 0; index < ids.length; index += 1) {
        const id = ids[index] ?? "";
        const bytes = blocks[index];
        if (bytes === undefined) throw new Error(`Visible block is missing: ${id}`);
        storedBlocks.set(id, bytes);
        // Header and metadata only: zone-map elimination must not pay the decompress,
        // checksum, and payload-validation cost of blocks it is about to discard.
        const description = inspectBlock(bytes);
        descriptions.set(id, description);
        this.#cachePut(`zi ${id}`, description, ZONE_DESCRIPTION_CACHE_BYTES);
      }
    }
    const prunedSegments: SegmentRecord[] = [];
    for (const segment of segments) {
      // Deltas ride through in place: they carry the replay's key markers and patches, they are
      // small, and their position among the appends is what makes the replay order meaningful.
      if (segment.kind === "delete" || segment.kind === "update") {
        prunedSegments.push(segment);
        continue;
      }
      const firstIds = segment.columnBlockIds[predicateColumns[0]?.id ?? ""] ?? [];
      if (
        involvedColumns.some(
          (column) => (segment.columnBlockIds[column.id]?.length ?? 0) !== firstIds.length,
        )
      ) {
        return undefined;
      }
      const blockIndexes: number[] = [];
      let segmentRows = 0;
      let prunedRows = 0;
      for (let blockIndex = 0; blockIndex < firstIds.length; blockIndex += 1) {
        let rowCount: number | undefined;
        let canMatch = true;
        for (const predicate of predicates) {
          const blockId = segment.columnBlockIds[predicate.column.id]?.[blockIndex] ?? "";
          const description = descriptions.get(blockId);
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
          prunedRows += rowCount;
        }
      }
      if (segmentRows !== segment.rowCount) return undefined;
      if (blockIndexes.length === 0) continue;
      prunedSegments.push({
        ...segment,
        rowCount: prunedRows,
        columnBlockIds: Object.fromEntries(
          involvedColumns.map((column) => [
            column.id,
            blockIndexes.map((blockIndex) => segment.columnBlockIds[column.id]?.[blockIndex] ?? ""),
          ]),
        ),
      });
    }
    // Elimination read every predicate block, but only the surviving ones are worth handing
    // to the scan; the rest are dropped here instead of riding along until the query ends.
    const survivingIds = new Set(
      prunedSegments.flatMap((segment) => Object.values(segment.columnBlockIds).flat()),
    );
    for (const id of [...storedBlocks.keys()]) {
      if (!survivingIds.has(id)) storedBlocks.delete(id);
    }
    return { segments: prunedSegments, storedBlocks };
  }

  #streamedViewFactory(
    table: TableRecord,
    projectedColumns: readonly TableColumnRecord[],
    segments: readonly SegmentRecord[],
    snapshot: LeasedSnapshot,
    storedBlocks?: Map<string, Uint8Array>,
    zonePruned = false,
  ):
    | {
        create: (memory: QueryMemoryContext) => Promise<{
          table: ColumnarTable;
          load: (start: number, length: number) => number | Promise<number>;
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
              zonePruned,
              storedBlocks,
            )
          : this.#createStreamedTable(
              table,
              projectedColumns,
              scanSegments,
              snapshot,
              scanRowCount,
              memory,
              storedBlocks,
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
        load: (start: number, length: number) => number | Promise<number>;
      }>;
    },
    buildView: {
      create: (memory: QueryMemoryContext) => Promise<{
        table: ColumnarTable;
        load: (start: number, length: number) => number | Promise<number>;
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
          // partition itself. The loader serves up to its current block boundary, so the
          // consumed length clamps to the returned resident end.
          const step = 1_024;
          for (let start = 0; start < build.table.rowCount;) {
            let length = Math.min(step, build.table.rowCount - start);
            const residentEnd = await build.load(start, length);
            if (typeof residentEnd === "number" && residentEnd > start) {
              length = Math.min(length, residentEnd - start);
            }
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
            start += length;
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
  /**
   * The vectorized form of one immutable block: validity, typed values, and for strings the
   * per-block dictionary with codes. Cached by block id beside the decoded physical form, so
   * a streamed scan installs whole blocks by reference — zero copies, zero re-decoding, and
   * dictionary objects that stay identical across queries, which keeps per-dictionary
   * expression caches (equality codes, LIKE match sets) hot between statements.
   */
  #blockColumnVector(blockId: string, decoded: DecodedPhysicalBlock): ColumnVector {
    const cached = this.#cacheGet(`dbv ${blockId}`) as ColumnVector | undefined;
    if (cached !== undefined) return cached;
    const column = decoded.column;
    const rows = column.rowCount;
    const validity = new Uint8Array(Math.ceil(rows / 8));
    const values =
      column.type === "boolean"
        ? new Uint8Array(rows)
        : column.type === "number" || column.type === "datetime"
          ? new Float64Array(rows)
          : undefined;
    const codes = column.type === "string" ? new Uint32Array(rows) : undefined;
    codes?.fill(NULL_STRING_VECTOR_CODE);
    const builder = new StringDictionaryBuilder();
    appendPhysicalColumnToVector(column, 0, validity, values, codes, builder);
    const vector = (
      codes !== undefined
        ? { kind: "string", length: rows, validity, codes, dictionary: builder.dictionary }
        : { kind: column.type, length: rows, validity, values }
    ) as ColumnVector;
    this.#cachePut(`dbv ${blockId}`, vector, blockVectorRetainedBytes(vector));
    return vector;
  }

  #createStreamedTable(
    table: TableRecord,
    projectedColumns: readonly TableColumnRecord[],
    segments: readonly SegmentRecord[],
    snapshot: LeasedSnapshot,
    rowCount: number,
    memory: QueryMemoryContext,
    storedBlocks?: Map<string, Uint8Array>,
  ): { table: ColumnarTable; load: (start: number, length: number) => number | Promise<number> } {
    interface StreamedColumnState {
      column: TableColumnRecord;
      vector: ColumnVector;
      blockIds: readonly string[];
      nextBlockIndex: number;
      nextBlockStartRow: number;
      resident: ResidentStreamBlock[];
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
    // The load contract: make a prefix of [start, start + length) resident and return its
    // exclusive end. Serving less than requested is normal — the window never extends past
    // the current block, so every install is a whole-block reference and a batch straddling
    // a block boundary is the caller's clamp to handle, not a stitched copy to build.
    // Synchronous fast path: when every column's resident window already contains the batch
    // start — every batch but the first per block — the loader answers without a promise, so
    // the scan loop stays allocation-free between slides.
    const load = (start: number, length: number): number | Promise<number> => {
      let syncEnd = rowCount;
      let allResident = states.length > 0;
      for (const state of states) {
        const window = state.vector.window;
        if (window !== undefined && start >= window.start && start < window.start + window.length) {
          syncEnd = Math.min(syncEnd, window.start + window.length);
          continue;
        }
        allResident = false;
        break;
      }
      if (allResident) return syncEnd;
      if (states.length === 0) return rowCount;
      return slide(start, length);
    };
    const slide = async (start: number, length: number): Promise<number> => {
      const end = Math.min(start + length, rowCount);
      let residentEnd = rowCount;
      for (const state of states) {
        const window = state.vector.window;
        if (window !== undefined && start >= window.start && start < window.start + window.length) {
          residentEnd = Math.min(residentEnd, window.start + window.length);
          continue;
        }
        if (window !== undefined && start < window.start) {
          throw new Error(`Streamed scan moved backward: ${table.name}.${state.column.name}`);
        }
        state.resident = state.resident.filter(
          (block) => block.startRow + block.vector.length > start,
        );
        while (state.nextBlockStartRow <= start && state.nextBlockIndex < state.blockIds.length) {
          // Fetch a short run of upcoming blocks in one round trip through the buffer pool.
          // The scan is forward-only over exactly these ids, so a decoded block beyond this
          // window is never wasted: it stays warm for the next load.
          const run = state.blockIds.slice(
            state.nextBlockIndex,
            state.nextBlockIndex + STREAMED_SCAN_LOOKAHEAD_BLOCKS,
          );
          const decodedRun = await this.#decodedBlocksThroughCache(run, snapshot, storedBlocks);
          for (let index = 0; index < decodedRun.length; index += 1) {
            const decoded = decodedRun[index];
            if (decoded === undefined) continue;
            if (decoded.column.type !== state.column.type) {
              throw new Error(`Column type mismatch: ${state.column.name}`);
            }
            state.resident.push({
              vector: this.#blockColumnVector(run[index] ?? "", decoded),
              startRow: state.nextBlockStartRow,
            });
            state.nextBlockIndex += 1;
            state.nextBlockStartRow += decoded.column.rowCount;
            if (state.nextBlockStartRow > start) break;
          }
        }
        state.resident = state.resident.filter(
          (block) => block.startRow + block.vector.length > start,
        );
        const first = state.resident[0];
        if (first === undefined || first.startRow > start) {
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
        residentEnd = Math.min(residentEnd, windowStart + windowRows);
      }
      if (residentEnd <= start && end > start) {
        throw new Error(`Streamed scan made no progress: ${table.name}`);
      }
      return residentEnd;
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
   * Block header/metadata descriptions, cached and fetched in one round trip: no decompress and
   * no payload validation. Descriptions are immutable per block id, so a repeated query pays
   * nothing — including the await, which is why the misses are batched rather than looped.
   */
  async #zoneDescriptions(
    blockIds: readonly string[],
    snapshot: LeasedSnapshot,
  ): Promise<Map<string, ReturnType<typeof inspectBlock>>> {
    const descriptions = new Map<string, ReturnType<typeof inspectBlock>>();
    const missing: string[] = [];
    for (const blockId of blockIds) {
      const cached = this.#cacheGet(`zi ${blockId}`) as ReturnType<typeof inspectBlock> | undefined;
      if (cached === undefined) missing.push(blockId);
      else descriptions.set(blockId, cached);
    }
    for (let start = 0; start < missing.length; start += 16) {
      await this.#renewInternalLeaseIfNeeded(snapshot);
      const ids = missing.slice(start, start + 16);
      const blocks = await this.store.getBlocks(ids);
      for (let index = 0; index < ids.length; index += 1) {
        const id = ids[index] ?? "";
        const bytes = blocks[index];
        if (bytes === undefined) throw new Error(`Visible block is missing: ${id}`);
        const description = inspectBlock(bytes);
        descriptions.set(id, description);
        this.#cachePut(`zi ${id}`, description, ZONE_DESCRIPTION_CACHE_BYTES);
      }
    }
    return descriptions;
  }

  /**
   * Builds a streamed view of a keyed table whose visible history contains update and delete
   * segments (no upserts — those interleave new rows into slot order and keep the materialized
   * path). Mutation deltas are the small part of such a history, so they replay into resident
   * state — a dead-row bitmap over the base rows plus per-slot column patches referencing the
   * resident update vectors — while the base rows stream through the block-aligned inner
   * window. The outer view compacts dead rows and overlays patches per window, producing
   * exactly the materialized replay's rows in exactly its order.
   *
   * The replay is a pure function of the visible segment set, so it is built once per commit
   * and shared by every query until the next one (`#streamedOverlayState`). The outer loader
   * serves whole inner windows: one whose rows nothing touched is installed by reference, the
   * difference between a copy per window and none; one with a dead or patched row is compacted
   * once, in runs rather than cells.
   *
   * The replay tracks only mutation-touched keys, so its memory is bounded by the mutation
   * size, not the table; the duplicate-key corruption guard consequently only fires for
   * touched keys on this path.
   */
  async #createStreamedMutationTable(
    table: TableRecord,
    keyColumn: TableColumnRecord,
    projectedColumns: readonly TableColumnRecord[],
    baseSegments: readonly SegmentRecord[],
    snapshot: LeasedSnapshot,
    memory: QueryMemoryContext,
    zonePruned = false,
    storedBlocks?: Map<string, Uint8Array>,
  ): Promise<{
    table: ColumnarTable;
    load: (start: number, length: number) => number | Promise<number>;
  }> {
    const scanSegments = baseSegments.filter((segment) => {
      const kind = segment.kind ?? "insert";
      return kind === "insert" || kind === "base";
    });
    const overlay = await this.#streamedOverlayState(
      table,
      keyColumn,
      baseSegments,
      scanSegments,
      snapshot,
      memory,
      zonePruned,
    );
    const { baseRows, dead, deadCount, patches, patchedSlots } = overlay;
    const hasPatches = patchedSlots.length > 0;
    const outputRows = baseRows - deadCount;
    const inner = this.#createStreamedTable(
      table,
      projectedColumns,
      scanSegments,
      snapshot,
      baseRows,
      memory,
      storedBlocks,
    );
    // Deltas that touch no row this scan reads — every one of them eliminated with its row
    // group, or aimed at keys this table no longer holds — leave the scan exactly as it was.
    if (deadCount === 0 && !hasPatches) return inner;
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
    const load = async (start: number, length: number): Promise<number> => {
      const end = Math.min(start + length, outputRows);
      // COUNT(*) and friends project nothing: the replay already knows how many rows survive,
      // so there is no window to build and no reason to walk the base rows to build it.
      if (states.length === 0) return end;
      const window = states[0]?.vector.window;
      if (window !== undefined && start >= window.start && start < window.start + window.length) {
        return window.start + window.length;
      }
      if (window !== undefined && start < window.start) {
        throw new Error(`Streamed scan moved backward: ${table.name}`);
      }
      // The cursor stops at the end of the window it last served, which can sit past a start
      // that falls before it. Rewinding costs one pass over the dead-row bitmap.
      if (cursorOutput > start) {
        cursorOutput = 0;
        cursorBase = 0;
      }
      while (cursorOutput < start && cursorBase < baseRows) {
        if (!bitmapHasValue(dead, cursorBase)) cursorOutput += 1;
        cursorBase += 1;
      }
      // Skip the dead rows in front of the first live one, so a window never starts dead.
      while (cursorBase < baseRows && bitmapHasValue(dead, cursorBase)) cursorBase += 1;
      if (cursorOutput !== start || cursorBase >= baseRows) {
        throw new Error(`Column row count mismatch: ${table.name}`);
      }
      const baseStart = cursorBase;
      // The inner loader serves whole blocks; the outer window covers the suffix of the inner
      // window from baseStart, however long, and the caller clamps to what it asked for.
      const innerEnd = await inner.load(baseStart, baseRows - baseStart);
      const baseEnd = typeof innerEnd === "number" ? Math.min(innerEnd, baseRows) : baseRows;
      if (baseEnd <= baseStart) throw new Error(`Column row count mismatch: ${table.name}`);
      const deadInWindow = bitmapCountRange(dead, baseStart, baseEnd);
      const patchedInWindow = hasPatches ? sortedCountRange(patchedSlots, baseStart, baseEnd) : 0;
      const liveRows = baseEnd - baseStart - deadInWindow;
      const untouched = deadInWindow === 0 && patchedInWindow === 0;
      const runs = untouched
        ? undefined
        : overlayWindowRuns(dead, patchedSlots, baseStart, baseEnd, patchedInWindow);
      interface OuterTarget {
        state: OuterColumnState;
        fields: MutableStreamedVectorFields;
        replacements: QueryMemoryReservation[];
      }
      const targets: OuterTarget[] = [];
      try {
        for (const state of states) {
          const innerVector = inner.table.columns.get(state.column.name);
          const innerWindow = innerVector?.window;
          if (innerVector === undefined || innerWindow === undefined) {
            throw new Error(`Streamed column is missing: ${state.column.name}`);
          }
          const offset = baseStart - innerWindow.start;
          if (offset < 0 || offset + (baseEnd - baseStart) > innerWindow.length) {
            throw new Error(`Column row count mismatch: ${state.column.name}`);
          }
          const replacements: QueryMemoryReservation[] = [];
          const fields =
            runs === undefined
              ? overlayWindowView(innerVector, offset, liveRows, memory, state.column, replacements)
              : overlayWindowCompacted(
                  innerVector,
                  innerWindow.start,
                  runs,
                  liveRows,
                  hasPatches ? patches : undefined,
                  state.column,
                  memory,
                  replacements,
                );
          fields.window = { start, length: liveRows };
          targets.push({ state, fields, replacements });
        }
        // Every fallible byte is reserved above; the installs below cannot throw, so a budget
        // overflow leaves every state's previous window and reservations intact.
        for (const { state, fields, replacements } of targets) {
          const mutable = state.vector as unknown as MutableStreamedVectorFields;
          mutable.validity = fields.validity;
          if (fields.values !== undefined) mutable.values = fields.values;
          if (fields.codes !== undefined) {
            mutable.codes = fields.codes;
            mutable.dictionary = fields.dictionary ?? [];
          }
          mutable.window = fields.window;
          for (const previous of state.reservations) previous.release();
          state.reservations = replacements;
        }
      } catch (error) {
        for (const entry of targets) {
          for (const replacement of entry.replacements) replacement.release();
        }
        throw error;
      }
      cursorOutput = start + liveRows;
      cursorBase = baseEnd;
      return start + liveRows;
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

  /**
   * The replayed mutation state for one visible segment set: which base rows are dead, and
   * which columns of which slots an update replaced. Cached under the segment ids in the
   * artifact LRU, because nothing about it changes between commits — before this, every query
   * over a table with so much as one deleted row rebuilt it, which made COUNT(*) on such a
   * table cost twenty times what it costs on a clean one. A cache hit is charged to the
   * query's memory as a tally, the same bytes a build reserves.
   */
  async #streamedOverlayState(
    table: TableRecord,
    keyColumn: TableColumnRecord,
    baseSegments: readonly SegmentRecord[],
    scanSegments: readonly SegmentRecord[],
    snapshot: LeasedSnapshot,
    memory: QueryMemoryContext,
    zonePruned: boolean,
  ): Promise<StreamedOverlayState> {
    // A zone-pruned scan keeps a segment's id with a subset of its blocks, and the slots the
    // replay addresses are the key blocks' rows in order — so the key blocks, not the segment
    // ids alone, are what identify the state.
    const key = [
      "overlay",
      table.id,
      zonePruned ? "pruned" : "full",
      baseSegments
        .map((segment) =>
          mutationSegmentKind(segment)
            ? segment.id
            : `${segment.id}:${(segment.columnBlockIds[keyColumn.id] ?? []).join("+")}`,
        )
        .join(","),
    ].join(" ");
    const cached = this.#cacheGet(key) as StreamedOverlayState | undefined;
    if (cached !== undefined) {
      memory.tally(cached.bytes, "Streamed mutation replay");
      return cached;
    }
    const state = await this.#buildStreamedOverlayState(
      table,
      keyColumn,
      baseSegments,
      scanSegments,
      snapshot,
      memory,
      zonePruned,
    );
    this.#cachePut(key, state, state.bytes);
    return state;
  }

  async #buildStreamedOverlayState(
    table: TableRecord,
    keyColumn: TableColumnRecord,
    baseSegments: readonly SegmentRecord[],
    scanSegments: readonly SegmentRecord[],
    snapshot: LeasedSnapshot,
    memory: QueryMemoryContext,
    zonePruned: boolean,
  ): Promise<StreamedOverlayState> {
    // Phase A: each mutation segment's key vector, and every column an update changed —
    // resident and reserved, bounded by the mutation history's size. All of an update's
    // columns, not only the ones this query projects: the state outlives the query. The
    // history's blocks come out of the buffer pool in one round trip, as block vectors: one
    // await per segment is what made a table with a few hundred deltas pay ten milliseconds
    // to rebuild this state after every commit.
    const deltaSegments = baseSegments.filter(mutationSegmentKind);
    const deltaBlockIds = new Set<string>();
    for (const segment of deltaSegments) {
      for (const column of table.columns) {
        for (const blockId of segment.columnBlockIds[column.id] ?? []) deltaBlockIds.add(blockId);
      }
    }
    const decodedDeltaBlocks = new Map<string, DecodedPhysicalBlock>();
    if (deltaBlockIds.size > 0) {
      const ids = [...deltaBlockIds];
      const decoded = await this.#decodedBlocksThroughCache(ids, snapshot);
      ids.forEach((id, index) => {
        const block = decoded[index];
        if (block !== undefined) decodedDeltaBlocks.set(id, block);
      });
    }
    const deltaVector = async (
      column: TableColumnRecord,
      segment: SegmentRecord,
    ): Promise<ColumnVector> => {
      const blockIds = segment.columnBlockIds[column.id] ?? [];
      const blockId = blockIds[0];
      if (blockIds.length !== 1 || blockId === undefined) {
        // A delta written in more than one block — a bulk update past rowsPerBlock — concatenates.
        return this.#materializeAppendColumnVector(column, [segment], snapshot, segment.rowCount);
      }
      const decoded = decodedDeltaBlocks.get(blockId);
      if (decoded === undefined) throw new Error(`Visible block is missing: ${blockId}`);
      if (decoded.column.type !== column.type) {
        throw new Error(`Column type mismatch: ${column.name}`);
      }
      const vector = this.#blockColumnVector(blockId, decoded);
      if (vector.length !== segment.rowCount) {
        throw new Error(`Column row count mismatch: ${column.name}`);
      }
      return vector;
    };
    const mutationKeyVectors = new Map<string, ColumnVector>();
    const mutationChangedVectors = new Map<string, Map<string, ColumnVector>>();
    // Keys as the primitives the vectors already hold. The replay used to build one string
    // token per row on both sides, which made a single deleted row cost an allocation and a
    // hash of every key in the table.
    const touched = new Set<OverlayKey>();
    let retainedBytes = 0;
    for (const segment of deltaSegments) {
      const kind = segment.kind ?? "insert";
      const keyVector = await deltaVector(keyColumn, segment);
      memory.reserve(columnVectorRetainedBytes(keyVector), "Streamed mutation replay");
      mutationKeyVectors.set(segment.id, keyVector);
      const readMutationKey = requiredColumnVectorKeyReader(keyVector);
      for (let row = 0; row < segment.rowCount; row += 1) {
        touched.add(readMutationKey(row));
      }
      if (kind === "update") {
        const changed = new Map<string, ColumnVector>();
        for (const column of table.columns) {
          if (column.id === keyColumn.id) continue;
          if ((segment.columnBlockIds[column.id]?.length ?? 0) === 0) continue;
          const vector = await deltaVector(column, segment);
          const bytes = columnVectorRetainedBytes(vector);
          memory.reserve(bytes, "Streamed mutation replay");
          retainedBytes += bytes;
          changed.set(column.id, vector);
        }
        mutationChangedVectors.set(segment.id, changed);
      }
    }

    // Phase B: one bounded pass over the scan segments' key blocks — a single block resident
    // at a time — recording, per scan segment, the touched keys and their absolute slots.
    const touchedByScanSegment = new Map<string, Array<{ key: OverlayKey; slot: number }>>();
    // A mutation history is small, and a unique key is usually written in order, so most key
    // blocks cannot hold any touched key at all. Their zone maps say so from the header alone,
    // which is what keeps one deleted row from costing a decode of every key block in the table.
    const touchedPredicate = touchedKeyPredicate(keyColumn, touched);
    const keyDescriptions =
      touchedPredicate === undefined
        ? new Map<string, ReturnType<typeof inspectBlock>>()
        : await this.#zoneDescriptions(
            scanSegments.flatMap((segment) => segment.columnBlockIds[keyColumn.id] ?? []),
            snapshot,
          );
    let baseRows = 0;
    for (const segment of scanSegments) {
      const entries: Array<{ key: OverlayKey; slot: number }> = [];
      touchedByScanSegment.set(segment.id, entries);
      let segmentRows = 0;
      for (const blockId of segment.columnBlockIds[keyColumn.id] ?? []) {
        const description = keyDescriptions.get(blockId);
        if (touchedPredicate !== undefined && description !== undefined) {
          if (description.type !== keyColumn.type) {
            throw new Error(`Column type mismatch: ${keyColumn.name}`);
          }
          if (!zoneMapCanMatch(description, touchedPredicate)) {
            segmentRows += description.rowCount;
            continue;
          }
        }
        const [decoded] = await this.#decodedBlocksThroughCache([blockId], snapshot);
        if (decoded === undefined) throw new Error(`Visible block is missing: ${blockId}`);
        if (decoded.column.type !== keyColumn.type) {
          throw new Error(`Column type mismatch: ${keyColumn.name}`);
        }
        // The block's vector form is what a scan of this table reads anyway, so it comes from
        // (and stays in) the buffer pool rather than being rebuilt for the replay.
        const blockVector = this.#blockColumnVector(blockId, decoded);
        const rows = blockVector.length;
        const readBlockKey = requiredColumnVectorKeyReader(blockVector);
        for (let row = 0; row < rows; row += 1) {
          const key = readBlockKey(row);
          if (touched.has(key)) entries.push({ key, slot: baseRows + segmentRows + row });
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
    const slotByKey = new Map<OverlayKey, number>();
    const patches = new Map<number, Map<string, OverlayPatch>>();
    let deadCount = 0;
    for (const segment of baseSegments) {
      const kind = segment.kind ?? "insert";
      if (kind === "insert" || kind === "base") {
        for (const entry of touchedByScanSegment.get(segment.id) ?? []) {
          if (slotByKey.has(entry.key)) {
            throw new Error(`Stored table contains a duplicate unique key: ${table.name}`);
          }
          slotByKey.set(entry.key, entry.slot);
        }
        continue;
      }
      const keyVector = mutationKeyVectors.get(segment.id);
      if (keyVector === undefined) {
        throw new Error(`Mutation segment key vector is missing: ${segment.id}`);
      }
      const readKey = requiredColumnVectorKeyReader(keyVector);
      if (kind === "delete") {
        for (let row = 0; row < segment.rowCount; row += 1) {
          const key = readKey(row);
          const slot = slotByKey.get(key);
          if (slot !== undefined && !bitmapHasValue(dead, slot)) {
            setBitmapValue(dead, slot);
            deadCount += 1;
            patches.delete(slot);
          }
          slotByKey.delete(key);
        }
        continue;
      }
      const changed = mutationChangedVectors.get(segment.id) ?? new Map<string, ColumnVector>();
      for (let row = 0; row < segment.rowCount; row += 1) {
        const slot = slotByKey.get(readKey(row));
        if (slot === undefined) {
          // Zone-map elimination only drops row groups this plan's predicates reject, and it
          // refuses to run at all when an update touches one of those predicate columns — so
          // an eliminated row's patch cannot change what the query returns.
          if (zonePruned) continue;
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
    let patchCells = 0;
    for (const slotPatches of patches.values()) patchCells += slotPatches.size;
    memory.tally(patches.size * 96 + patchCells * 48, "Streamed mutation replay");
    const patchedSlots = Uint32Array.from(patches.keys()).sort();
    return {
      baseRows,
      deadCount,
      dead,
      patches,
      patchedSlots,
      bytes:
        dead.byteLength +
        retainedBytes +
        patchedSlots.byteLength +
        patches.size * 96 +
        patchCells * 48,
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
    cacheResults = true,
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
      cacheResults,
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
    cacheResults = true,
  ): Promise<QueryResult> {
    const key = await this.#blockResultCacheKey(
      block,
      snapshot,
      visibility,
      realTables,
      cacheResults,
    );
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
      undefined,
      cacheResults,
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
    cacheResults = true,
  ): Promise<{ result: QueryResult; schema: SqlColumnSchema[] }> {
    const key = await this.#blockResultCacheKey(
      block,
      snapshot,
      visibility,
      realTables,
      cacheResults,
    );
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
      undefined,
      cacheResults,
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
    cacheResults = true,
  ): Promise<string | undefined> {
    // Every computed-result cache — whole-block results, their schemas, and the columnar
    // forms of derived and windowed sources — is keyed from here, and each one skips both
    // its read and its write when the key is undefined. Returning undefined is therefore
    // the single switch that makes a statement compute its results instead of reusing them.
    if (!cacheResults) return undefined;
    if (!this.#artifactCache.enabled) return undefined;
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

  /**
   * Copies the current committed version out as a portable snapshot file — the byte array
   * `../storage/snapshot.ts` describes, which `importSnapshot()` loads back into any store.
   *
   * The bytes are the unit here rather than the structured `DatabaseSnapshot` the store returns,
   * because that is what a caller does with a snapshot: write it to disk, hand it to a colleague,
   * ship it as an asset. Reach for `store.exportSnapshot()` directly when you want the records
   * instead. Either way the whole snapshot is materialized in memory, so a database of hundreds
   * of megabytes needs the headroom for a copy of itself.
   *
   * Stores differ in how they hold still while this runs: see the note on exporting safely in the
   * snapshots guide.
   */
  async exportSnapshot(options: SnapshotExportOptions = {}): Promise<Uint8Array> {
    // Announced before the work starts, because reading and encoding report nothing in between:
    // the encoded length is not known until the last block has been copied.
    options.onProgress?.({ phase: "reading", transferredBytes: 0, totalBytes: 0 });
    const bytes = await encodeSnapshot(await exportingStore(this.store).exportSnapshot());
    options.onProgress?.({
      phase: "done",
      transferredBytes: bytes.byteLength,
      totalBytes: bytes.byteLength,
    });
    return bytes;
  }

  /**
   * Loads a snapshot file into this database's store, which must be empty. Every block is
   * authenticated while decoding, so a corrupt file fails here rather than mid-query, and a store
   * that already holds a database throws rather than merging two histories.
   */
  async importSnapshot(bytes: Uint8Array, options: SnapshotImportOptions = {}): Promise<void> {
    // Asked before the file is decoded, so a store that could never load it fails immediately
    // rather than after parsing and authenticating hundreds of megabytes.
    const store = importingStore(this.store);
    const snapshot = await decodeSnapshot(bytes);
    await store.importSnapshot(snapshot, {
      ...(options.onProgress === undefined ? {} : { onProgress: options.onProgress }),
    });
    // This database is now looking at a catalog it has never seen. Both stores bump their catalog
    // epoch as they load, which is what expires the cached catalog state, but the compiled-plan
    // cache is keyed by statement text alone; clearing both is cheap and leaves nothing behind
    // from the database this one replaced.
    this.#planCache.clear();
    this.#catalogStateCache.clear();
    this.#catalogStateEpoch = undefined;
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
    return this.#serializedCompactionStep(table.id, async () => {
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
    });
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
    return this.#serializedCompactionStep(table.id, () =>
      this.#runCompactionJob(
        table,
        job,
        positiveWholeNumber(options.maxBlocks ?? 1, "Compaction step block limit"),
      ),
    );
  }

  /**
   * One compaction step at a time per table within this database: background compaction
   * drives a job in steps, and a caller stepping the same table explicitly must take turns with
   * it rather than advance the same job concurrently, which would write its output blocks
   * twice. Each step loads the job record fresh, so alternating drivers simply continue where
   * the other left off. Between instances and tabs the job's revision is the guard.
   */
  async #serializedCompactionStep<T>(tableId: string, step: () => Promise<T>): Promise<T> {
    const previous = this.#compactionSteps.get(tableId) ?? Promise.resolve();
    const run = previous.then(step, step);
    this.#compactionSteps.set(tableId, run);
    try {
      return await run;
    } finally {
      if (this.#compactionSteps.get(tableId) === run) this.#compactionSteps.delete(tableId);
    }
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

  async #cancelTableCompactions(tableId: string): Promise<void> {
    for (const job of await this.store.listCompactionJobs(tableId)) {
      if (job.state === "planned" || job.state === "running" || job.state === "ready") {
        await this.cancelCompactionJob(job.id);
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
      ...(options.retainRecentVersions === undefined
        ? {}
        : { retainRecentVersions: options.retainRecentVersions }),
    });
    while (progress.result === null) {
      progress = await this.resumeGarbageCollectionJob(progress.jobId, { maxItems });
    }
    await this.#pruneFinishedJobRecords();
    return progress.result;
  }

  /** Plans or advances one durable garbage-collection pass. */
  async collectGarbageStep(
    options: CollectGarbageStepOptions = {},
  ): Promise<GarbageCollectionProgress> {
    return this.#collectGarbageStep(options);
  }

  /** `collectGarbageStep`, with the age bound background collection adds to its retention. */
  async #collectGarbageStep(
    options: CollectGarbageStepOptions,
    retainedVersionMaxAgeMs = Number.POSITIVE_INFINITY,
  ): Promise<GarbageCollectionProgress> {
    return this.#serializedCollectionStep(async () => {
      const active = (await this.store.listGarbageCollectionJobs()).find(
        (job) => job.state === "planned" || job.state === "running",
      );
      const job =
        active ??
        (await this.#planGarbageCollection(
          positiveWholeNumber(
            options.maxPlanningItems ?? 1_024,
            "Garbage collection planning limit",
          ),
          nonNegativeWholeNumber(
            options.retainRecentVersions ?? 0,
            "Garbage collection retained versions",
          ),
          retainedVersionMaxAgeMs,
        ));
      return this.#runGarbageCollectionJob(
        job,
        positiveWholeNumber(options.maxItems ?? 1, "Garbage collection item limit"),
      );
    });
  }

  /** Continues a persisted reclamation pass after a cooperative yield or restart. */
  async resumeGarbageCollectionJob(
    jobId: string,
    options: CollectGarbageStepOptions = {},
  ): Promise<GarbageCollectionProgress> {
    const job = await this.store.getGarbageCollectionJob(jobId);
    if (job === undefined) throw new Error(`Garbage collection job not found: ${jobId}`);
    return this.#serializedCollectionStep(() =>
      this.#runGarbageCollectionJob(
        job,
        positiveWholeNumber(options.maxItems ?? 1, "Garbage collection item limit"),
      ),
    );
  }

  /**
   * One garbage-collection step at a time within this database, for the same reason
   * compaction steps take turns (`#serializedCompactionStep`): background collection drives
   * a job in steps, and a caller stepping collection explicitly continues the same job rather
   * than racing it.
   */
  async #serializedCollectionStep<T>(step: () => Promise<T>): Promise<T> {
    const previous = this.#collectionSteps;
    const run = previous.then(step, step);
    this.#collectionSteps = run;
    try {
      return await run;
    } finally {
      if (this.#collectionSteps === run) this.#collectionSteps = Promise.resolve();
    }
  }

  /**
   * Background collection: plans one pass and drives it to completion in yielding steps.
   * Runs after a background fold, whose superseded blocks are what a pass reclaims, and every
   * AUTO_COLLECT_COMMIT_INTERVAL commits, since every commit writes a manifest that stays on
   * disk until pruned. Keeps the most recent versions readable. Never surfaces through a
   * write or a scan; a failed pass backs off for an interval of commits.
   */
  #maybeScheduleAutoCollection(): void {
    if (!this.#autoCollect) return;
    if (this.#autoCollectionInFlight) {
      // A fold finishing or a quiet minute passing while a run is under way is a reason for
      // one more run once this one ends — a dropped trigger after the last commit of a burst
      // would otherwise leave the burst's leftovers until the next one.
      this.#autoCollectionRequested = true;
      return;
    }
    if (this.#commitsSinceCollection < this.#autoCollectionBackoffUntilCommit) return;
    this.#autoCollectionInFlight = true;
    this.#autoCollectionRequested = false;
    this.#commitsSinceCollection = 0;
    this.#lastCollectionAt = this.#now().getTime();
    void this.#runAutoCollection()
      .then(() => {
        this.#autoCollectionBackoffUntilCommit = 0;
      })
      .catch(() => {
        this.#autoCollectionBackoffUntilCommit = AUTO_COLLECT_COMMIT_INTERVAL * 2;
      })
      .finally(() => {
        this.#autoCollectionInFlight = false;
        if (this.#autoCollectionRequested) {
          this.#autoCollectionRequested = false;
          void yieldToEventLoop().then(() => {
            this.#maybeScheduleAutoCollection();
          });
        }
      });
  }

  /**
   * A pass a quiet period after the last commit, for a tab that stops writing: the retained
   * window's age bound lets that pass reclaim what the last burst superseded, which no commit
   * would otherwise arrive to trigger. Re-armed by every commit; unreferenced, so it never
   * keeps a process alive.
   */
  #armIdleCollection(): void {
    if (!this.#autoCollect) return;
    if (this.#idleCollectionTimer !== undefined) clearTimeout(this.#idleCollectionTimer);
    const timer = setTimeout(() => {
      this.#idleCollectionTimer = undefined;
      this.#maybeScheduleAutoCollection();
    }, AUTO_COLLECT_QUIET_MS);
    (timer as { unref?: () => void }).unref?.();
    this.#idleCollectionTimer = timer;
  }

  async #runAutoCollection(): Promise<void> {
    // One pass plans a bounded number of candidates, so a backlog — a burst of commits that
    // outran the passes between them — takes several. Keep passing while a pass still finds
    // something, up to a ceiling that keeps a pathological store from pinning the loop.
    for (let pass = 0; pass < AUTO_COLLECT_MAX_PASSES; pass += 1) {
      this.#releaseIdleSharedLease();
      let progress = await this.#collectGarbageStep(
        {
          maxItems: AUTO_COLLECT_STEP_ITEMS,
          retainRecentVersions: AUTO_COLLECT_RETAINED_VERSIONS,
        },
        AUTO_COLLECT_RETAINED_VERSION_MS,
      );
      while (progress.result === null) {
        await yieldToEventLoop();
        progress = await this.resumeGarbageCollectionJob(progress.jobId, {
          maxItems: AUTO_COLLECT_STEP_ITEMS,
        });
      }
      const result = progress.result;
      if (
        result.prunedManifestCount === 0 &&
        result.reclaimedBlockCount === 0 &&
        result.reclaimedSegmentCount === 0 &&
        result.reclaimedTransactionCount === 0
      ) {
        break;
      }
      await yieldToEventLoop();
    }
    await this.#pruneFinishedJobRecords();
  }

  /** Drops finished maintenance records while preserving the state needed for safe L2 retries. */
  async #pruneFinishedJobRecords(): Promise<void> {
    const newestFirst = <T extends { updatedAt: string }>(jobs: T[]): T[] =>
      jobs.sort((left, right) => right.updatedAt.localeCompare(left.updatedAt));
    const terminalCompactions = newestFirst(
      (await this.store.listCompactionJobs()).filter(
        (job) => job.state === "published" || job.state === "cancelled" || job.state === "aborted",
      ),
    );
    const retainedCompactionIds = new Set(
      terminalCompactions.slice(0, AUTO_COLLECT_RETAINED_JOB_RECORDS).map((job) => job.id),
    );
    // A retry persists the cumulative bytes written by earlier attempts. Keep only the newest
    // failure for each still-readable source snapshot; it carries the whole lifetime budget.
    // Once the source manifest is pruned, that exact retry can never be planned again.
    const retainedFailureBases = new Set<string>();
    const manifestReadable = new Map<number, boolean>();
    for (const job of terminalCompactions) {
      if (job.state !== "cancelled" && job.state !== "aborted") continue;
      const baseId = job.id.split("/retry/", 1)[0] ?? job.id;
      if (retainedFailureBases.has(baseId)) continue;
      let readable = manifestReadable.get(job.sourceManifestVersion);
      if (readable === undefined) {
        const manifest = await this.store.getManifest(job.sourceManifestVersion);
        readable = manifest !== undefined && manifest.prunedAt === undefined;
        manifestReadable.set(job.sourceManifestVersion, readable);
      }
      if (!readable) continue;
      retainedFailureBases.add(baseId);
      retainedCompactionIds.add(job.id);
    }
    for (const job of terminalCompactions) {
      if (!retainedCompactionIds.has(job.id)) await this.store.removeCompactionJob(job.id);
    }
    const collections = newestFirst(
      (await this.store.listGarbageCollectionJobs()).filter((job) => job.state === "completed"),
    );
    for (const job of collections.slice(AUTO_COLLECT_RETAINED_JOB_RECORDS)) {
      await this.store.removeGarbageCollectionJob(job.id);
    }
  }

  async listGarbageCollectionJobs(): Promise<GarbageCollectionJobRecord[]> {
    return this.store.listGarbageCollectionJobs();
  }

  async #planGarbageCollection(
    maxPlanningItems: number,
    retainRecentVersions: number,
    retainedVersionMaxAgeMs = Number.POSITIVE_INFINITY,
  ): Promise<GarbageCollectionJobRecord> {
    // Crashed readers and query spill owners are metadata roots too. Sweep their expired
    // records as part of every explicit or background collection so callers never need a
    // separate maintenance loop to keep either family bounded.
    await this.#transactions.removeExpiredLeases(this.#now());
    await this.cleanupQuerySpill();
    const current = await this.store.getCurrentManifest();
    const currentBlockIds = new Set(current?.blockIds ?? []);
    // Manifest versions are consecutive, so the retained window is a version floor; a version
    // inside it is still collected once it is older than the window's age.
    const retainAbove = (current?.version ?? 0) - retainRecentVersions;
    const retainAfter = this.#now().getTime() - retainedVersionMaxAgeMs;
    const retained = (manifest: { version: number; createdAt: string }): boolean =>
      manifest.version > retainAbove && Date.parse(manifest.createdAt) > retainAfter;
    const candidateManifestVersions: number[] = [];
    const candidateBlockIds: string[] = [];
    const candidateSegmentIds: string[] = [];
    const candidateTransactionIds: string[] = [];
    const candidateBlockIdSet = new Set<string>();
    const candidateSegmentIdSet = new Set<string>();
    const remaining = () =>
      maxPlanningItems -
      candidateBlockIds.length -
      candidateSegmentIds.length -
      candidateTransactionIds.length;
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
    // The walk starts past the prefix of history this database has already seen fully
    // collected — every manifest pruned and none of its blocks left — and extends that prefix
    // as it goes. A pruned manifest's blocks can only disappear, and a block it shares with a
    // later manifest is found through that manifest or the job that superseded it, so skipping
    // the dead prefix loses nothing; it is what keeps a pass proportional to the live history
    // rather than to everything the database ever committed.
    let manifestCursor: number | null = this.#collectionWatermark;
    let deadPrefixEnd = this.#collectionWatermark;
    let prefixContiguous = true;
    walk: do {
      const page = await this.store.listManifestPage(manifestCursor, 64);
      for (const manifest of page.records) {
        if (manifest.version === current?.version) break walk;
        if (retained(manifest)) {
          prefixContiguous = false;
          continue;
        }
        const existing = await this.#existingGarbageBlockCandidates(
          manifest.blockIds,
          currentBlockIds,
          remaining(),
        );
        if (manifest.prunedAt === undefined) {
          // Only an unpruned manifest is a pruning candidate: one already pruned would spend a
          // step's capacity confirming it, and with enough of them in front, the unpruned ones
          // behind them were never reached at all. Their leftover blocks still count.
          if (candidateManifestVersions.length < 64)
            candidateManifestVersions.push(manifest.version);
          prefixContiguous = false;
        } else if (prefixContiguous && existing.length === 0) {
          deadPrefixEnd = manifest.version;
        } else {
          prefixContiguous = false;
        }
        addBlocks(existing);
        if (remaining() <= 0) break walk;
      }
      manifestCursor = page.nextCursor;
    } while (manifestCursor !== null);
    this.#collectionWatermark = deadPrefixEnd;

    if (remaining() > 0) {
      const segmentOwnerIds = new Set(
        (await this.store.listSegments()).map((segment) => segment.transactionId),
      );
      const manifestEligibility = new Map<number, boolean>();
      const candidateManifestSet = new Set(candidateManifestVersions);
      let transactionCursor: string | null = null;
      do {
        const page = await this.store.listTransactionPage(transactionCursor, 64);
        for (const transaction of page.records) {
          if (transaction.status === "aborted") {
            const pendingBlocks = await this.#existingGarbageBlockCandidates(
              transaction.pendingBlockIds,
              currentBlockIds,
              remaining(),
            );
            addBlocks(pendingBlocks);
            const pendingSegments = await this.#existingGarbageSegmentCandidates(
              transaction.pendingSegmentIds,
              remaining(),
            );
            addSegments(pendingSegments);
            // The artifacts are deleted before transaction candidates within a job. A later
            // pass sees the empty journal and removes the aborted record itself.
            if (pendingBlocks.length === 0 && pendingSegments.length === 0 && remaining() > 0) {
              candidateTransactionIds.push(transaction.id);
            }
          } else if (
            transaction.status === "committed" &&
            transaction.committedVersion !== null &&
            !segmentOwnerIds.has(transaction.id) &&
            remaining() > 0
          ) {
            let eligible = manifestEligibility.get(transaction.committedVersion);
            if (eligible === undefined) {
              const manifest = await this.store.getManifest(transaction.committedVersion);
              eligible =
                manifest === undefined ||
                manifest.prunedAt !== undefined ||
                candidateManifestSet.has(transaction.committedVersion);
              manifestEligibility.set(transaction.committedVersion, eligible);
            }
            if (eligible) candidateTransactionIds.push(transaction.id);
          }
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
              [...job.sourceSegmentIds, ...compactionOutputSegmentIds(job)],
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
      candidateTransactionIds,
      leaseCutoff: timestamp,
      createdAt: timestamp,
    });
  }

  async #existingGarbageBlockCandidates(
    ids: readonly string[],
    currentBlockIds: ReadonlySet<string>,
    limit: number,
  ): Promise<string[]> {
    if (limit <= 0) return [];
    // A block the current manifest still carries is not garbage, whatever else references it,
    // and an unpruned manifest shares nearly all of its blocks with the current one. Deciding
    // that from the set first leaves the store lookups to the few blocks that might be gone —
    // reading every block of every manifest to find them made a planning pass cost the table
    // times the history.
    const possible: string[] = [];
    const seen = new Set<string>();
    for (const id of ids) {
      if (currentBlockIds.has(id) || seen.has(id)) continue;
      seen.add(id);
      possible.push(id);
    }
    const candidates: string[] = [];
    for (let start = 0; start < possible.length && candidates.length < limit; start += 64) {
      const page = possible.slice(start, start + 64);
      const blocks = await this.store.getBlocks(page);
      for (let index = 0; index < page.length && candidates.length < limit; index += 1) {
        const id = page[index] ?? "";
        if (blocks[index] !== undefined) candidates.push(id);
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
      if (job.state === "completed") {
        await this.store.removePrunedManifestRecords();
        return garbageCollectionProgress(job);
      }
      try {
        const step = await this.store.runGarbageCollectionStep({
          jobId: job.id,
          expectedRevision: job.revision,
          maxItems,
          updatedAt: this.#now().toISOString(),
        });
        if (step.job.state === "completed") await this.store.removePrunedManifestRecords();
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
    const partitionRows = positiveWholeNumber(
      options.partitionRows ?? this.#compactionPartitionRows,
      "Compaction partition rows",
    );

    let anchors: readonly SegmentRecord[] = [];
    let level0Segments: readonly SegmentRecord[];
    let effectiveMinimumLevel0Segments: number;
    let outputPartitionOrdinal: number | undefined;
    let keyedLevelTwo = false;
    let keyedLevelOne: KeyedLevelOneLayout | undefined;
    let keylessLevelOne: KeyedLevelOneLayout | undefined;
    if (targetLevel === 1 && table.uniqueKeyColumnId !== undefined) {
      // Keyed L1: a prefix of level-one partitions, then level-zero history. A fold rewrites
      // only the partitions the selected deltas touch (and the tail partition new rows join),
      // so which partitions it sources is decided after the level-zero selection below.
      const layout = keyedLevelOneLayout(visibleSegments);
      if (layout === null) {
        return compactTableSkipped(
          table.name,
          "unsupported-level-layout",
          visibleSegments,
          visibleBlockIds,
          version,
        );
      }
      keyedLevelOne = layout;
      level0Segments = layout.level0Segments;
      effectiveMinimumLevel0Segments =
        layout.partitions.length > 0 ? minimumLevel0Segments : Math.max(2, minimumLevel0Segments);
    } else if (targetLevel === 1) {
      const layout = keylessLevelOneLayout(visibleSegments);
      if (layout === null) {
        return compactTableSkipped(
          table.name,
          "unsupported-level-layout",
          visibleSegments,
          visibleBlockIds,
          version,
        );
      }
      keylessLevelOne = layout;
      level0Segments = layout.level0Segments;
      effectiveMinimumLevel0Segments =
        layout.partitions.length > 0 ? minimumLevel0Segments : Math.max(2, minimumLevel0Segments);
    } else if (
      table.uniqueKeyColumnId !== undefined ||
      visibleSegments.some(
        (segment) => (segment.kind ?? "insert") !== "insert" || segment.rowIdSpans !== undefined,
      )
    ) {
      // Keyed multi-range L2: merge (the level-one partitions + oldest level-zero prefix) into
      // a new span-carrying partition. Published partitions are never rewritten; mutation
      // kinds without a unique key cannot merge and keep the materialized skip.
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
      anchors = layout.anchors;
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
    if (version === null) throw new Error("Visible compaction segments require a manifest");
    const level0Selection = await this.#selectLevelZeroSources(
      level0Segments,
      effectiveMinimumLevel0Segments,
      maxLevel0Segments,
      maxLevel0StoredBytes,
      snapshot,
    );
    let partitioning: KeyedPartitioning | undefined;
    let rechunkPartitioning: RechunkPartitioning | undefined;
    if (keyedLevelOne !== undefined) {
      const keyColumn = getUniqueKeyColumn(table);
      if (keyColumn === undefined) {
        throw new Error(`Mutation compaction requires a unique key: ${table.name}`);
      }
      const touched = await this.#touchedPartitionIds(
        keyColumn,
        keyedLevelOne.partitions,
        level0Selection.segments,
        memoryBudgetBytes,
        snapshot,
      );
      // New rows join the last partition while it is small, or when it is being rewritten
      // anyway; otherwise they open a new partition behind it and it stays untouched.
      const last = keyedLevelOne.partitions[keyedLevelOne.partitions.length - 1];
      const bearsNewRows = level0Selection.segments.some((segment) =>
        mergeSourceBearsRows(segment.kind ?? "insert"),
      );
      const absorbsTail =
        last !== undefined &&
        bearsNewRows &&
        (touched.has(last.id) || last.rowCount < partitionRows);
      anchors = keyedLevelOne.partitions.filter(
        (partition) =>
          partition.rowCount > partitionRows ||
          touched.has(partition.id) ||
          (absorbsTail && partition.id === last.id),
      );
      partitioning = {
        partitions: keyedLevelOne.partitions,
        partitionRows,
        absorbsTail,
        nextLevelZeroOrder: level0Selection.nextLogicalOrder ?? version + 1,
      };
    } else if (keylessLevelOne !== undefined) {
      const last = keylessLevelOne.partitions.at(-1);
      // A partial tail is extended. An oversized legacy anchor is included once so this fold
      // heals it into bounded partitions; a full tail stays immutable and new rows start after it.
      const absorbsTail =
        last !== undefined && (last.rowCount < partitionRows || last.rowCount > partitionRows);
      anchors = absorbsTail ? [last] : [];
      rechunkPartitioning = {
        partitionRows,
        nextLevelZeroOrder: level0Selection.nextLogicalOrder ?? version + 1,
      };
    }
    const anchorMeasurement = await this.#measureCompactionSources(
      anchors,
      level0Selection.blockIds,
      snapshot,
    );
    const selection = {
      sourceSegments: [...anchors, ...level0Selection.segments],
      level0SourceStoredBytes: level0Selection.storedBytes,
      anchorSourceStoredBytes: anchorMeasurement.storedBytes,
    };
    const sourceSegments = selection.sourceSegments;
    const sourceBlockIds = uniqueSegmentBlockIds(sourceSegments);
    const hasContiguousSourceRowIds = hasContiguousRowIds(sourceSegments);
    const hasPositiveSourceRowIds = (sourceSegments[0]?.rowIdStart ?? 0n) > 0n;
    // A keyed fold always merges: one uniform partition shape (a full-row base with row-ID
    // spans, bounded by `partitionRows`) regardless of whether the selected prefix happens to
    // be pure inserts.
    const requiresMerge =
      keyedLevelTwo || keyedLevelOne !== undefined
        ? true
        : targetLevel === 1 &&
          sourceSegments.some(
            (segment) =>
              (segment.kind ?? "insert") !== "insert" || segment.rowIdSpans !== undefined,
          );
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
          partitioning,
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
        rechunkPartitioning,
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
        .reduce(
          (largest, candidate) =>
            Math.max(
              largest,
              safeWholeNumberSum(
                [candidate.priorAttemptOutputStoredBytes ?? 0, candidate.outputStoredBytes],
                "Compaction prior-attempt output stored bytes",
              ),
            ),
          0,
        );
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

  /**
   * Sums the stored bytes of the given segments' blocks, refusing a block that appears twice
   * among them or in `seenBlockIds` — a source block may only be superseded once.
   */
  async #measureCompactionSources(
    segments: readonly SegmentRecord[],
    seenBlockIds: ReadonlySet<string>,
    snapshot: LeasedSnapshot,
  ): Promise<{ storedBytes: number; blockIds: string[] }> {
    let total = 0;
    const blockIds: string[] = [];
    const measuredBlockIds = new Set<string>();
    for (const segment of segments) {
      for (const blockId of Object.values(segment.columnBlockIds).flat()) {
        if (seenBlockIds.has(blockId) || measuredBlockIds.has(blockId)) {
          throw new Error(`Compaction source block is referenced more than once: ${blockId}`);
        }
        measuredBlockIds.add(blockId);
        blockIds.push(blockId);
        await this.#renewInternalLeaseIfNeeded(snapshot);
        const bytes = await this.store.getBlock(blockId);
        if (bytes === undefined) throw new Error(`Compaction source block is missing: ${blockId}`);
        total = safeWholeNumberSum([total, bytes.byteLength], "Compaction selected stored bytes");
      }
    }
    return { storedBytes: total, blockIds };
  }

  /**
   * The oldest level-zero prefix one job promotes: whole equal-order groups, at least the
   * minimum, and past it no more than the segment and stored-byte ceilings allow. Also reports
   * the order of the first segment left behind, which bounds the orders a fold may publish.
   */
  async #selectLevelZeroSources(
    level0Segments: readonly SegmentRecord[],
    minimumLevel0Segments: number,
    maxLevel0Segments: number,
    maxLevel0StoredBytes: number,
    snapshot: LeasedSnapshot,
  ): Promise<{
    segments: SegmentRecord[];
    storedBytes: number;
    blockIds: Set<string>;
    nextLogicalOrder: number | null;
  }> {
    const transactions = new Map(
      (await this.#transactionRecordsForSegments(level0Segments)).map((record) => [
        record.id,
        record,
      ]),
    );
    const logicalOrder = (segment: SegmentRecord): number => {
      const owner = transactions.get(segment.transactionId);
      if (owner?.status !== "committed" || owner.committedVersion === null) {
        throw new Error(`Compaction source segment has no committed owner: ${segment.id}`);
      }
      return segment.logicalOrder ?? owner.committedVersion;
    };
    const seenBlockIds = new Set<string>();
    const selected: SegmentRecord[] = [];
    let storedBytes = 0;
    let start = 0;
    while (start < level0Segments.length) {
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
        selected.length >= minimumLevel0Segments &&
        selected.length + group.length > maxLevel0Segments
      ) {
        break;
      }
      const measurement = await this.#measureCompactionSources(group, seenBlockIds, snapshot);
      if (
        selected.length >= minimumLevel0Segments &&
        measurement.storedBytes > maxLevel0StoredBytes - storedBytes
      ) {
        break;
      }
      measurement.blockIds.forEach((blockId) => seenBlockIds.add(blockId));
      selected.push(...group);
      storedBytes = safeWholeNumberSum(
        [storedBytes, measurement.storedBytes],
        "Compaction selected L0 stored bytes",
      );
      start = end;
    }
    if (selected.length < minimumLevel0Segments) {
      throw new Error("Compaction source selection did not satisfy its minimum L0 segment count");
    }
    const next = level0Segments[start];
    return {
      segments: selected,
      storedBytes,
      blockIds: seenBlockIds,
      nextLogicalOrder: next === undefined ? null : logicalOrder(next),
    };
  }

  /**
   * Which level-one partitions the selected deltas reach into: those holding a key that some
   * delete, update, or upsert among them names. Inserts and upserts of new keys touch nothing;
   * their rows join the tail. Key blocks whose zone map rules every referenced key out are
   * skipped from the header, so a sorted key costs one decoded block per partition at most and
   * usually none; the referenced-key set is the same size the merge planner's is.
   */
  async #touchedPartitionIds(
    keyColumn: TableColumnRecord,
    partitions: readonly SegmentRecord[],
    level0Segments: readonly SegmentRecord[],
    memoryBudgetBytes: number,
    snapshot: LeasedSnapshot,
  ): Promise<Set<string>> {
    const touched = new Set<string>();
    if (partitions.length === 0) return touched;
    const deltas = level0Segments.filter((segment) =>
      mergeSourceReferencesKeys(segment.kind ?? "insert"),
    );
    if (deltas.length === 0) return touched;
    const referencedBytes = safeWholeNumberProduct(
      deltas.reduce((total, segment) => total + segment.rowCount, 0),
      MERGE_PLANNER_KEY_BYTES,
      "Compaction referenced keys",
    );
    if (referencedBytes > memoryBudgetBytes) {
      throw new CompactionMemoryBudgetError(memoryBudgetBytes, referencedBytes);
    }
    const referenced = new Set<OverlayKey>();
    for (const segment of deltas) {
      await this.#forEachSegmentKey(segment, keyColumn, snapshot, (value) => {
        referenced.add(overlayKeyOf(keyColumn.type, value));
      });
    }
    const predicate = touchedKeyPredicate(keyColumn, referenced);
    const descriptions =
      predicate === undefined
        ? new Map<string, ReturnType<typeof inspectBlock>>()
        : await this.#zoneDescriptions(
            partitions.flatMap((partition) => partition.columnBlockIds[keyColumn.id] ?? []),
            snapshot,
          );
    for (const partition of partitions) {
      const blockIds = partition.columnBlockIds[keyColumn.id] ?? [];
      if (blockIds.length === 0) {
        throw new Error(`Partition has no key column blocks: ${partition.id}`);
      }
      for (const blockId of blockIds) {
        const description = descriptions.get(blockId);
        if (
          predicate !== undefined &&
          description !== undefined &&
          !zoneMapCanMatch(description, predicate)
        ) {
          continue;
        }
        await this.#renewInternalLeaseIfNeeded(snapshot);
        const bytes = await this.store.getBlock(blockId);
        if (bytes === undefined) throw new Error(`Compaction source block is missing: ${blockId}`);
        const decoded = await decodeBlock(bytes);
        if (decoded.column.type !== keyColumn.type) {
          throw new Error(`Compaction source block differs from table schema: ${blockId}`);
        }
        if (
          decoded.column.values.some((value) => referenced.has(overlayKeyOf(keyColumn.type, value)))
        ) {
          touched.add(partition.id);
          break;
        }
      }
    }
    return touched;
  }

  /** Decodes a segment's key column in row order, one block resident at a time. */
  async #forEachSegmentKey(
    segment: SegmentRecord,
    keyColumn: TableColumnRecord,
    snapshot: LeasedSnapshot,
    action: (value: BatchValue, rowIndex: number) => void,
  ): Promise<void> {
    let rowIndex = 0;
    for (const blockId of segment.columnBlockIds[keyColumn.id] ?? []) {
      await this.#renewInternalLeaseIfNeeded(snapshot);
      const bytes = await this.store.getBlock(blockId);
      if (bytes === undefined) throw new Error(`Compaction source block is missing: ${blockId}`);
      const decoded = await decodeBlock(bytes);
      if (decoded.column.type !== keyColumn.type) {
        throw new Error(`Compaction source block differs from table schema: ${blockId}`);
      }
      for (const value of decoded.column.values) {
        action(value, rowIndex);
        rowIndex += 1;
      }
    }
    if (rowIndex !== segment.rowCount) {
      throw new Error(`Mutation segment key rows differ: ${segment.id}`);
    }
  }

  async #createRechunkCompactionPlan(
    table: TableRecord,
    sourceSegments: readonly SegmentRecord[],
    targetBlockBytes: number,
    outputCompression: Compression,
    memoryBudgetBytes: number,
    snapshot: LeasedSnapshot,
    partitioning?: RechunkPartitioning,
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
    const logicalOrder = await this.#firstLogicalOrder(sourceSegments);
    const partitions =
      partitioning === undefined
        ? undefined
        : planLinearOutputPartitions(
            totalRows,
            partitioning.partitionRows,
            logicalOrder,
            partitioning.nextLevelZeroOrder,
          );
    const estimatedOutputs: RechunkCompactionOutputWindow[] = [];
    const outputRegions = partitions ?? [{ rowStart: 0, rowCount: totalRows }];
    for (const region of outputRegions) {
      const regionEnd = region.rowStart + region.rowCount;
      for (let rowStart = region.rowStart; rowStart < regionEnd; rowStart += rowsPerOutput) {
        estimatedOutputs.push({
          rowStart,
          rowCount: Math.min(rowsPerOutput, regionEnd - rowStart),
        });
      }
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
      logicalOrder,
      columns,
      outputs,
      ...(partitions === undefined ? {} : { partitions }),
    };
  }

  /**
   * Plans a merge of the sources into one canonical output. With `partitioning`, the output
   * is also cut into level-one partitions: each rewritten source partition keeps its rows (and
   * its logical order) in place, new rows form the tail, and every run is chunked to at most
   * `partitionRows`, using fractional orders between unchanged neighbours.
   */
  async #createMergeCompactionPlan(
    table: TableRecord,
    sourceSegments: readonly SegmentRecord[],
    targetBlockBytes: number,
    outputCompression: Compression,
    memoryBudgetBytes: number,
    snapshot: LeasedSnapshot,
    partitioning?: KeyedPartitioning,
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
    const partitions =
      partitioning === undefined
        ? undefined
        : planOutputPartitions(
            partitioning,
            describedSegments,
            resolved.sourceOutputRowStarts,
            totalRows,
          );
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
      // Windows never straddle a partition: each partition's blocks are its own.
      const estimatedOutputs: RechunkCompactionOutputWindow[] = [];
      for (const region of partitions ?? [{ rowStart: 0, rowCount: totalRows }]) {
        const regionEnd = region.rowStart + region.rowCount;
        for (let rowStart = region.rowStart; rowStart < regionEnd; rowStart += rowsPerOutput) {
          estimatedOutputs.push({
            rowStart,
            rowCount: Math.min(rowsPerOutput, regionEnd - rowStart),
          });
        }
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
      ...(partitions === undefined ? {} : { partitions }),
    };
  }

  /**
   * Replays the source segments' mutations into one canonical output order, in memory that
   * scales with the deltas rather than with the table.
   *
   * Every row of every row-bearing source (base, insert, upsert) gets a slot, numbered in
   * canonical source order, and the output is the live slots in slot order. A row can only be
   * referenced later through its key, and only delete, update, and upsert sources reference
   * keys, so the first pass collects those keys — the touched set — and the replay then tracks
   * slots for touched keys alone. An untouched row can never be deleted, patched, or replaced:
   * it passes through as part of a run, one output range per source block rather than one per
   * row. Memory is O(delta rows + touched rows) plus two bytes per slot.
   *
   * The semantics are those of a per-row replay:
   * - delete: the key's live slot dies; a later insert of the key takes a new slot.
   * - update: the named columns of the key's live slot come from the update row; the key must
   *   be live.
   * - upsert: when the key is live, every column of that slot comes from the upsert row and the
   *   upsert row's own slot dies, so the row keeps its position and row ID; otherwise the
   *   upsert row is a new live row.
   * - insert/base: a new live row; a second live occurrence of a touched key is an error.
   */
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
    /** The output row at which each row-bearing source's surviving rows begin. */
    sourceOutputRowStarts: Map<string, number>;
  }> {
    const plannerMemoryBytes = mergePlannerMemoryBound(table, segments, keyColumn.id);
    if (plannerMemoryBytes > memoryBudgetBytes) {
      throw new CompactionMemoryBudgetError(memoryBudgetBytes, plannerMemoryBytes);
    }
    const columnIndexById = new Map(table.columns.map((column, index) => [column.id, index]));

    // Pass 1: the keys any delta references, and each delta's keys in row order.
    const touched = new Set<OverlayKey>();
    const deltaKeys = new Map<string, OverlayKey[]>();
    for (const segment of segments) {
      if (!mergeSourceReferencesKeys(segment.kind)) continue;
      const keys: OverlayKey[] = [];
      await this.#forEachMergeSourceKey(segment, keyColumn, snapshot, (value) => {
        const key = overlayKeyOf(keyColumn.type, value);
        keys.push(key);
        touched.add(key);
      });
      deltaKeys.set(segment.segmentId, keys);
    }

    // Pass 2: replay into slot state.
    let slotCount = 0;
    for (const segment of segments) {
      if (mergeSourceBearsRows(segment.kind)) slotCount += segment.rowCount;
    }
    const dead = new Uint8Array(slotCount);
    const patched = new Uint8Array(slotCount);
    const patches = new Map<number, Array<MergeResolvedSource | undefined>>();
    const liveSlotByKey = new Map<OverlayKey, number>();
    let slotBase = 0;
    for (const segment of segments) {
      if (segment.kind === "delete") {
        for (const key of deltaKeys.get(segment.segmentId) ?? []) {
          const slot = liveSlotByKey.get(key);
          if (slot === undefined) continue;
          dead[slot] = 1;
          patched[slot] = 0;
          patches.delete(slot);
          liveSlotByKey.delete(key);
        }
        continue;
      }
      if (segment.kind === "update") {
        const changedColumns = segment.columns
          .map((column) => column.columnId)
          .filter((columnId) => columnId !== keyColumn.id)
          .map((columnId) => {
            const columnIndex = columnIndexById.get(columnId);
            if (columnIndex === undefined) {
              throw new Error(`Mutation compaction column is missing: ${columnId}`);
            }
            return { columnId, columnIndex };
          });
        const keys = deltaKeys.get(segment.segmentId) ?? [];
        for (let rowIndex = 0; rowIndex < keys.length; rowIndex += 1) {
          const key = keys[rowIndex];
          const slot = key === undefined ? undefined : liveSlotByKey.get(key);
          if (slot === undefined) {
            throw new Error(`Update segment references a missing key: ${segment.segmentId}`);
          }
          let patch = patches.get(slot);
          if (patch === undefined) {
            patch = new Array<MergeResolvedSource | undefined>(table.columns.length).fill(
              undefined,
            );
            patches.set(slot, patch);
            patched[slot] = 1;
          }
          for (const { columnId, columnIndex } of changedColumns) {
            patch[columnIndex] = mergeSourceAt(segment, columnId, rowIndex);
          }
        }
        continue;
      }
      // A row-bearing source: base, insert, or upsert.
      const base = slotBase;
      const visit = (key: OverlayKey, rowIndex: number): void => {
        if (!touched.has(key)) return;
        const slot = base + rowIndex;
        const existing = liveSlotByKey.get(key);
        if (existing === undefined) {
          liveSlotByKey.set(key, slot);
          return;
        }
        if (segment.kind !== "upsert") {
          throw new Error(`Insert segment contains a duplicate unique key: ${segment.segmentId}`);
        }
        patches.set(
          existing,
          table.columns.map((column) => mergeSourceAt(segment, column.id, rowIndex)),
        );
        patched[existing] = 1;
        dead[slot] = 1;
      };
      const keys = deltaKeys.get(segment.segmentId);
      if (keys !== undefined) {
        keys.forEach(visit);
      } else if (touched.size > 0) {
        // With nothing referencing keys there is nothing to track: every row passes through.
        await this.#forEachMergeSourceKey(segment, keyColumn, snapshot, (value, rowIndex) => {
          visit(overlayKeyOf(keyColumn.type, value), rowIndex);
        });
      }
      slotBase += segment.rowCount;
    }
    touched.clear();
    liveSlotByKey.clear();
    deltaKeys.clear();

    // Pass 3: the live slots in slot order, as runs wherever nothing touched them.
    const output = new MergeOutputBuilder(table.columns);
    const sourceOutputRowStarts = new Map<string, number>();
    slotBase = 0;
    for (const segment of segments) {
      if (!mergeSourceBearsRows(segment.kind)) continue;
      sourceOutputRowStarts.set(segment.segmentId, output.totalRows);
      let runStart = -1;
      for (let rowIndex = 0; rowIndex < segment.rowCount; rowIndex += 1) {
        const slot = slotBase + rowIndex;
        if (dead[slot] === 1 || patched[slot] === 1) {
          if (runStart >= 0) {
            output.appendRun(segment, runStart, rowIndex - runStart);
            runStart = -1;
          }
          if (patched[slot] === 1) output.appendPatchedRow(segment, rowIndex, patches.get(slot));
          continue;
        }
        if (runStart < 0) runStart = rowIndex;
      }
      if (runStart >= 0) output.appendRun(segment, runStart, segment.rowCount - runStart);
      slotBase += segment.rowCount;
    }
    return { ...output.finish(), sourceOutputRowStarts };
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
        const createdAt = this.#now().toISOString();
        const desiredOutputSegments: SegmentRecord[] =
          rewritePlan.kind === "copy-v1"
            ? [
                {
                  id: outputSegmentId,
                  tableId: table.id,
                  transactionId: transaction.id,
                  rowCount: outputRowCount,
                  rowIdStart: first?.rowIdStart ?? 0n,
                  rowIdEndExclusive: last?.rowIdEndExclusive ?? 0n,
                  columnBlockIds: compactionOutputColumns(table, sourceSegments, job.id),
                  kind: "insert",
                  ...(table.uniqueKeyColumnId === undefined
                    ? {}
                    : { keyColumnId: table.uniqueKeyColumnId }),
                  level: job.targetLevel,
                  ...(job.outputPartitionOrdinal === undefined
                    ? {}
                    : { partitionOrdinal: job.outputPartitionOrdinal }),
                  logicalOrder: await this.#firstLogicalOrder(sourceSegments),
                  createdAt,
                },
              ]
            : compactionOutputSegments(table, job, rewritePlan, transaction.id, createdAt);
        for (const desiredOutputSegment of desiredOutputSegments) {
          await this.#stageCompactionOutputSegment(
            transaction,
            desiredOutputSegment,
            expectedOutputIds,
          );
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
      let manifest: ManifestSummary;
      for (;;) {
        let publicationConflict: WriteConflictError | undefined;
        try {
          manifest = await transaction.commit();
          break;
        } catch (error) {
          if (!(error instanceof WriteConflictError)) throw error;
          publicationConflict = error;
        }

        // Publication is logically neutral, so it may follow any number of concurrent data
        // commits while every source remains visible and in the same logical position. A single
        // retry is not sufficient: another tab (or this database's write queue) can win the
        // manifest CAS again between rebase and commit, leaving an otherwise complete job stuck
        // in `ready` after the last write. Keep rebasing until publication wins or a source
        // genuinely changes.
        const current = await this.store.getCurrentManifest();
        const currentIds = new Set(current?.blockIds ?? []);
        if (job.sourceBlockIds.some((id) => !currentIds.has(id))) {
          if (transaction.status === "active") await transaction.abort();
          job = await this.#abortCompactionJob(
            job,
            "Compaction sources changed before publication",
          );
          throw new Error(job.error, { cause: publicationConflict });
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
          throw new Error(job.error, { cause: publicationConflict });
        }
        transaction.supersedeBlocks(job.sourceBlockIds);
        transaction.markLogicallyUnchanged();
        await yieldToEventLoop();
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
        outputBytes = await this.#encodePreferredBlock(
          column.columnId,
          plan.outputCompression,
          built.physical.bytes.byteLength < GZIP_MINIMUM_INPUT_BYTES,
          (compression) => encodePhysicalBlock(built.physical, compression),
        );
      } else {
        const decoded = await decodePhysicalBlock(existing);
        if (
          decoded.description.type !== column.type ||
          (plan.outputCompression === "raw" && decoded.description.compression !== "raw") ||
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
    if (
      job.outputPartitionOrdinal === undefined &&
      job.targetLevel === 1 &&
      plan.kind !== "copy-v1" &&
      plan.partitions !== undefined
    ) {
      const table = await this.store.getTable(job.tableId);
      if (table === undefined) throw new Error(`Compaction table is missing: ${job.tableId}`);
      await this.#assertPartitionedLevelOneSnapshotOrder(
        job,
        plan,
        table,
        visibleSegments,
        transactions,
      );
      return;
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
        if (actual === undefined) {
          throw new Error(`Compaction source is no longer visible: ${planned.segmentId}`);
        }
        const owner = transactions.get(actual.transactionId);
        if (!sameMergeSourceSegment(actual, owner, planned)) {
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

  /**
   * The partitioned level-one rebase rule, shared by keyed merges and keyless rechunks. The
   * sources must be exactly as planned. Every partition the plan left alone must still be visible
   * and unchanged — they are read back from the planning snapshot's manifest, which the job
   * roots until it ends, so the check needs no record of its own. Every other visible segment
   * must be level-zero history committed after the latest source and ordered after every
   * partition the job publishes, so the output slots into the same place relative to the deltas
   * it did not absorb.
   */
  async #assertPartitionedLevelOneSnapshotOrder(
    job: CompactionJobRecord,
    plan: PhysicalCompactionRewritePlan,
    table: TableRecord,
    visibleSegments: readonly SegmentRecord[],
    transactions: ReadonlyMap<string, { status: string; committedVersion: number | null }>,
  ): Promise<void> {
    const sourceIds = new Set(job.sourceSegmentIds);
    const visibleById = new Map(visibleSegments.map((segment) => [segment.id, segment]));
    const sourceManifest = await this.store.getManifest(job.sourceManifestVersion);
    if (sourceManifest === undefined || sourceManifest.prunedAt !== undefined) {
      throw new Error(
        `Compaction source manifest is unavailable: ${String(job.sourceManifestVersion)}`,
      );
    }
    const plannedVisible = await this.#visibleSegmentRecords(
      table,
      new Snapshot(this.store, sourceManifest.version, sourceManifest.blockIds),
    );
    const plannedById = new Map(plannedVisible.map((segment) => [segment.id, segment]));
    const plannedLayout =
      table.uniqueKeyColumnId === undefined
        ? keylessLevelOneLayout(plannedVisible)
        : keyedLevelOneLayout(plannedVisible);
    if (plannedLayout === null) throw new Error("Compaction planned layout is no longer valid");

    let latestSource: Pick<
      MergeCompactionSourceSegment,
      "logicalOrder" | "committedVersion" | "segmentId"
    > | null = null;
    if (plan.kind === "merge-v1") {
      for (const planned of plan.sourceSegments) {
        const actual = visibleById.get(planned.segmentId);
        if (actual === undefined) {
          throw new Error(`Compaction source is no longer visible: ${planned.segmentId}`);
        }
        const owner = transactions.get(actual.transactionId);
        if (!sameMergeSourceSegment(actual, owner, planned)) {
          throw new Error(`Compaction source segment differs from its plan: ${planned.segmentId}`);
        }
      }
      latestSource = plan.sourceSegments[plan.sourceSegments.length - 1] ?? null;
    } else {
      for (const id of job.sourceSegmentIds) {
        const actual = visibleById.get(id);
        const planned = plannedById.get(id);
        if (actual === undefined) throw new Error(`Compaction source is no longer visible: ${id}`);
        if (
          actual.transactionId !== planned?.transactionId ||
          !sameCompactionSegment(actual, planned)
        ) {
          throw new Error(`Compaction source segment differs from its plan: ${id}`);
        }
        const tuple = sourceOrderTuple(actual, transactions, "Compaction source");
        if (latestSource === null || compareMergeSourceOrder(latestSource, tuple) < 0) {
          latestSource = tuple;
        }
      }
    }
    if (latestSource === null) throw new Error("Compaction source order is unavailable");
    const maxOutputOrder =
      plan.partitions === undefined
        ? plan.logicalOrder
        : Math.max(
            plan.logicalOrder,
            ...plan.partitions.map((partition) => partition.logicalOrder),
          );
    const retained = new Map(
      plannedLayout.partitions
        .filter((partition) => !sourceIds.has(partition.id))
        .map((partition) => [partition.id, partition]),
    );
    for (const segment of visibleSegments) {
      if (sourceIds.has(segment.id)) continue;
      if ((segment.level ?? 0) === 1) {
        const planned = retained.get(segment.id);
        if (
          planned?.transactionId !== segment.transactionId ||
          !sameCompactionSegment(segment, planned)
        ) {
          throw new Error(`Concurrent segment is not a retained partition: ${segment.id}`);
        }
        continue;
      }
      if ((segment.level ?? 0) !== 0) {
        throw new Error(`Concurrent segment has an unsupported compaction level: ${segment.id}`);
      }
      const tuple = sourceOrderTuple(segment, transactions, "Concurrent compaction segment");
      if (
        tuple.logicalOrder <= maxOutputOrder ||
        compareMergeSourceOrder(latestSource, tuple) >= 0
      ) {
        throw new Error(`Concurrent segment would reorder compaction output: ${segment.id}`);
      }
    }
    for (const id of retained.keys()) {
      if (!visibleById.has(id)) {
        throw new Error(`Retained compaction partition is no longer visible: ${id}`);
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

  /**
   * Stages one output segment, reconciling with what a previous attempt left: the same
   * segment staged by this transaction is reused, one left by an aborted transaction is
   * adopted when it matches and was never published, anything else is an error.
   */
  async #stageCompactionOutputSegment(
    transaction: DatabaseTransaction,
    desired: SegmentRecord,
    expectedOutputIds: readonly string[],
  ): Promise<void> {
    const existing = await this.store.getSegment(desired.id);
    if (existing === undefined) {
      await transaction.stageSegment(desired);
      return;
    }
    if (existing.transactionId === transaction.id) {
      if (!sameCompactionSegment(existing, desired)) {
        throw new Error(`A resumed compaction segment differs: ${desired.id}`);
      }
      await transaction.stageExistingSegment(desired.id);
      return;
    }
    const owner = await this.store.getTransaction(existing.transactionId);
    if (
      (owner !== undefined && owner.status !== "aborted") ||
      !sameCompactionSegment(existing, desired)
    ) {
      throw new Error(`Compaction output segment cannot be adopted: ${desired.id}`);
    }
    if (await this.#unprunedManifestContainsAll(expectedOutputIds)) {
      throw new Error(`Compaction output segment is already visible: ${desired.id}`);
    }
    await this.store.removeSegment(desired.id);
    await transaction.stageSegment(desired);
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
      outputSegmentIds: compactionOutputSegmentIds(job),
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

  #cacheGet(key: string): unknown {
    return this.#artifactCache.get(key);
  }

  /**
   * Buffer pool observability: byte budget and residency plus lifetime hit/miss/eviction
   * counters across every entry family (decoded blocks, block vectors, zone descriptions,
   * derived results, memoized results). Use it to size bufferPoolBytes from real workloads.
   */
  bufferPoolStats(): BufferPoolStats {
    return this.#artifactCache.stats();
  }

  /**
   * Caches one prepared artifact under the byte-bounded LRU. Payloads are shared
   * read-only across prepared queries; entries for superseded segment sets simply stop
   * matching and age out the same way evicted entries do.
   */
  #cachePut(key: string, payload: unknown, bytes: number): void {
    this.#artifactCache.put(key, payload, bytes);
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
      // Zone-map pruning shrinks the scan the same way index pruning does, so a scoring plan
      // whose statistics come from the scan (no index serving it) must see the whole corpus.
      const zonePruningAllowed =
        plan === undefined ||
        !planContainsFts(plan, "bm25") ||
        this.#ftsIndexServesScoring(plan, table, visibleSegments);
      const predicates =
        plan === undefined || !zonePruningAllowed ? [] : zonePredicates(plan, table);
      if (predicates.length > 0) {
        const pruned = await this.#materializePrunedAppendTable(
          table,
          snapshot,
          projectedColumns,
          segments,
          predicates,
        );
        if (pruned !== undefined) return pruned;
      }
      const columns = new Map<string, ColumnVector>();
      for (const column of projectedColumns) {
        columns.set(
          column.name,
          await this.#materializeAppendColumnVector(column, segments, snapshot, rowCount),
        );
      }
      return {
        name: table.name,
        rowCount,
        columns,
        ...(projectedKey ? { uniqueKey: projectedKey } : {}),
      };
    }
    if (
      keyColumn !== undefined &&
      segments.every((segment) => {
        const kind = segment.kind ?? "insert";
        return kind === "insert" || kind === "base" || kind === "delete" || kind === "update";
      })
    ) {
      const overlay = await this.#materializeOverlayTable(
        table,
        snapshot,
        projectedColumns,
        segments,
        keyColumn,
        plan,
      );
      if (overlay !== undefined) return overlay;
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
      neededColumns.map((column) => {
        const vector = createEmptyColumnVector(column.type, maximumRows);
        // Seeded, not filled per segment: replay overwrites a slot whenever a segment carries
        // the column, so what survives is exactly the rows no segment ever wrote it for.
        if (column.backfill !== undefined) {
          fillColumnVectorRange(vector, 0, maximumRows, column.backfill);
        }
        return [column.id, vector] as const;
      }),
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

  /**
   * An append scan of the base segments with the table's delete and update deltas applied on
   * top. The replay path below rebuilds every row of every segment through a keyed map because
   * one segment is not an append, so a single deleted row used to cost a full-table replay —
   * and cost it on every later query, since only compaction folds the delta away. This path
   * pays for the deltas instead: base columns are copied in runs, deleted keys mask rows out,
   * and updated keys patch the cells they changed.
   *
   * Returns undefined for any history it does not cover (a windowed vector, a null key, an
   * update whose key no live row carries), which falls through to the replay unchanged.
   */
  async #materializeOverlayTable(
    table: TableRecord,
    snapshot: LeasedSnapshot,
    projectedColumns: readonly TableColumnRecord[],
    segments: readonly SegmentRecord[],
    keyColumn: TableColumnRecord,
    plan: CompiledQuery | undefined,
  ): Promise<ColumnarTable | undefined> {
    const appends: SegmentRecord[] = [];
    const appendOrders: number[] = [];
    const deltas: Array<{ segment: SegmentRecord; order: number }> = [];
    segments.forEach((segment, order) => {
      const kind = segment.kind ?? "insert";
      if (kind === "insert" || kind === "base") {
        appends.push(segment);
        appendOrders.push(order);
        return;
      }
      deltas.push({ segment, order });
    });
    if (deltas.length === 0) return undefined;

    // The deltas are the small side: key markers, and for an update the columns it changed.
    const deletedAt = new Map<OverlayKey, number>();
    const updatesByKey = new Map<OverlayKey, OverlayUpdate[]>();
    const patchedColumnIds = new Set<string>();
    const allUpdates: OverlayUpdate[] = [];
    for (const { segment, order } of deltas) {
      await this.#renewInternalLeaseIfNeeded(snapshot);
      const keyVector = await this.#materializeAppendColumnVector(
        keyColumn,
        [segment],
        snapshot,
        segment.rowCount,
      );
      const readKey = columnVectorKeyReader(keyVector);
      if (readKey === undefined) return undefined;
      if (segment.kind === "delete") {
        for (let row = 0; row < segment.rowCount; row += 1) {
          const key = readKey(row);
          if (key === undefined) return undefined;
          deletedAt.set(key, order);
        }
        continue;
      }
      const changed = projectedColumns.filter(
        (column) =>
          column.id !== keyColumn.id && (segment.columnBlockIds[column.id]?.length ?? 0) > 0,
      );
      const vectors = new Map<string, ColumnVector>();
      for (const column of changed) {
        vectors.set(
          column.id,
          await this.#materializeAppendColumnVector(column, [segment], snapshot, segment.rowCount),
        );
        patchedColumnIds.add(column.id);
      }
      for (let row = 0; row < segment.rowCount; row += 1) {
        const key = readKey(row);
        if (key === undefined) return undefined;
        const update: OverlayUpdate = { order, row, vectors, applied: false };
        allUpdates.push(update);
        const existing = updatesByKey.get(key);
        if (existing === undefined) updatesByKey.set(key, [update]);
        else existing.push(update);
      }
    }

    const projectsKey = projectedColumns.some((column) => column.id === keyColumn.id);
    const neededColumns = projectsKey ? projectedColumns : [...projectedColumns, keyColumn];
    // Zone-map elimination drops whole row groups, so it can only compose with deltas that
    // cannot bring a pruned row back. Two conditions decide that, and they are independent.
    //
    // The first is ordering. A pruned scan reports no runs, so an update's position cannot be
    // compared against the segment a row came from -- but it does not need to be, as long as
    // every delta is newer than every base segment, because then every delta wins outright.
    //
    // The second is which columns the predicate reads. Zone maps are built from the base blocks,
    // so a predicate on a column an update rewrites is unsafe: the update could move a value into
    // the predicate's range, and the row group holding it would already have been eliminated. A
    // predicate on a column no update touches has no such hazard -- the unique key most of all,
    // which updates address rows *by* and never change.
    //
    // Requiring no updates at all conflated the two, and it is the write path that paid: a SQL
    // UPDATE resolves the rows it will touch by reading them back, so `UPDATE ... WHERE id = ?`
    // read every row of the table once any earlier update existed. Separating the conditions took
    // a 200k-row point update from 14.9ms to 2.4ms.
    const lastAppendOrder = appendOrders.at(-1) ?? -1;
    const deltasFollowAppends = deltas.every(({ order }) => order > lastAppendOrder);
    const candidates =
      plan === undefined || !deltasFollowAppends ? [] : zonePredicates(plan, table);
    // Dropping an unsafe predicate only prunes less, never more, so keeping the safe ones is
    // both correct and worth doing even when the two kinds are mixed.
    const predicates = candidates.filter((predicate) => !patchedColumnIds.has(predicate.column.id));
    let base: ColumnarTable | undefined;
    if (predicates.length > 0) {
      base = await this.#materializePrunedAppendTable(
        table,
        snapshot,
        neededColumns,
        appends,
        predicates,
      );
    }
    /** Whether the rows below are a pruned subset rather than every base row. */
    const prunedScan = base !== undefined;
    // Runs map an output row back to the segment that wrote it; a pruned scan reports no runs,
    // and orderFree is what makes that safe.
    const runs: Array<{ end: number; order: number }> = [];
    if (base === undefined) {
      const appendRowCount = appends.reduce((total, segment) => total + segment.rowCount, 0);
      const columns = new Map<string, ColumnVector>();
      for (const column of neededColumns) {
        columns.set(
          column.name,
          await this.#materializeAppendColumnVector(column, appends, snapshot, appendRowCount),
        );
      }
      base = { name: table.name, rowCount: appendRowCount, columns };
      let end = 0;
      appends.forEach((segment, index) => {
        end += segment.rowCount;
        runs.push({ end, order: appendOrders[index] ?? 0 });
      });
    }

    const rowCount = base.rowCount;
    const baseKeyVector = base.columns.get(keyColumn.name);
    if (baseKeyVector === undefined) return undefined;
    const readBaseKey = columnVectorKeyReader(baseKeyVector);
    if (readBaseKey === undefined) return undefined;
    const alive = new Uint8Array(rowCount).fill(1);
    const patches = new Map<number, OverlayUpdate[]>();
    let aliveCount = rowCount;
    let runIndex = 0;
    for (let row = 0; row < rowCount; row += 1) {
      while (runIndex < runs.length && row >= (runs[runIndex]?.end ?? 0)) runIndex += 1;
      const order = runs.length === 0 ? -1 : (runs[runIndex]?.order ?? -1);
      const key = readBaseKey(row);
      if (key === undefined) return undefined;
      const updates = updatesByKey.get(key);
      if (updates !== undefined) {
        const applicable = updates.filter((update) => update.order > order);
        for (const update of applicable) update.applied = true;
        if (applicable.length > 0) patches.set(row, applicable);
      }
      const deleted = deletedAt.get(key);
      if (deleted !== undefined && deleted > order) {
        alive[row] = 0;
        aliveCount -= 1;
      }
    }
    // An update whose key no base row carries means the replay's corruption check would fire,
    // or that a shape this path does not model let the row escape it. Either way, defer.
    //
    // A pruned scan is the one case where an unapplied update is expected rather than alarming:
    // its row was eliminated by a predicate on a column no update touches, so the row cannot
    // satisfy this query whatever the update did to it, and the patch is irrelevant here. The
    // check still applies in full to every unpruned scan, which is where a genuinely dangling
    // update would surface.
    if (!prunedScan && allUpdates.some((update) => !update.applied)) return undefined;

    const columns = new Map<string, ColumnVector>();
    for (const column of projectedColumns) {
      const source = base.columns.get(column.name);
      if (source === undefined) return undefined;
      if (aliveCount === rowCount && !patchedColumnIds.has(column.id)) {
        columns.set(column.name, source);
        continue;
      }
      columns.set(
        column.name,
        patchedColumnIds.has(column.id)
          ? patchColumnVector(source, column, alive, aliveCount, patches)
          : compactColumnVector(source, alive, aliveCount),
      );
    }
    return {
      name: table.name,
      rowCount: aliveCount,
      columns,
      ...(projectsKey ? { uniqueKey: keyColumn.name } : {}),
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

  /**
   * The block buffer pool: every read path decodes physical blocks through here. Block
   * content is immutable, so the decoded form caches by id alone and can never be stale —
   * a commit only ever introduces new ids, and superseded blocks simply stop being
   * referenced and age out of the byte-bounded LRU. Returns one decoded block per id, in
   * order; `storedBlocks` short-circuits the store fetch for bytes the caller already holds.
   */
  async #decodedBlocksThroughCache(
    ids: readonly string[],
    snapshot: LeasedSnapshot,
    storedBlocks?: Map<string, Uint8Array>,
  ): Promise<DecodedPhysicalBlock[]> {
    await this.#renewInternalLeaseIfNeeded(snapshot);
    const decodedBlocks = new Array<DecodedPhysicalBlock | undefined>(ids.length);
    const pendingIndexes: number[] = [];
    const fetchIds: string[] = [];
    for (let index = 0; index < ids.length; index += 1) {
      const id = ids[index] ?? "";
      const cached = this.#cacheGet(`dpb\0${id}`) as DecodedPhysicalBlock | undefined;
      if (cached !== undefined) {
        decodedBlocks[index] = cached;
        continue;
      }
      pendingIndexes.push(index);
      if (storedBlocks?.has(id) !== true) fetchIds.push(id);
    }
    // Fetched bytes live in a local map that dies with this call. Writing them into the
    // caller's `storedBlocks` would retain every block the scan ever touched for the whole
    // query — outside the buffer pool's budget and never charged to the memory context.
    const fetchedBytes = new Map<string, Uint8Array>();
    if (fetchIds.length > 0) {
      const fetched = await this.store.getBlocks(fetchIds);
      for (let index = 0; index < fetchIds.length; index += 1) {
        const id = fetchIds[index] ?? "";
        const bytes = fetched[index];
        if (bytes === undefined) throw new Error(`Visible block is missing: ${id}`);
        fetchedBytes.set(id, bytes);
      }
    }
    for (const index of pendingIndexes) {
      const id = ids[index] ?? "";
      const bytes = fetchedBytes.get(id) ?? storedBlocks?.get(id);
      if (bytes === undefined) throw new Error(`Visible block is missing: ${id}`);
      const decoded = await decodePhysicalBlock(bytes);
      this.#cachePut(`dpb\0${id}`, decoded, bytes.byteLength);
      decodedBlocks[index] = decoded;
    }
    return decodedBlocks.map((decoded, index) => {
      if (decoded === undefined) {
        throw new Error(`Visible block is missing: ${ids[index] ?? ""}`);
      }
      return decoded;
    });
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
        // The column joined the catalog after this segment was written. Its rows read as the
        // column's backfill, or as NULL through the preallocated validity default when it has
        // none. A present-but-empty block list still fails the row-count check below, preserving
        // the corruption guard.
        if (column.backfill !== undefined) {
          const backfill = column.backfill;
          const stringCode =
            stringCodes === undefined
              ? 0
              : dictionaryCodeForText(stringDictionary, String(backfill));
          const numeric = backfill instanceof Date ? backfill.getTime() : Number(backfill);
          for (let offset = 0; offset < segment.rowCount; offset += 1) {
            const index = outputRow + offset;
            setBitmapValue(validity, index);
            if (stringCodes !== undefined) stringCodes[index] = stringCode;
            else if (values instanceof Uint8Array) values[index] = backfill === true ? 1 : 0;
            else if (values !== undefined) values[index] = numeric;
          }
        }
        outputRow += segment.rowCount;
        continue;
      }
      let segmentRows = 0;
      for (let start = 0; start < blockIds.length; start += 16) {
        const ids = blockIds.slice(start, start + 16);
        // Immutable block content decodes through the buffer pool: unchanged blocks skip the
        // store fetch, decompression, checksum, and payload validation on every re-read.
        const decodedBlocks = await this.#decodedBlocksThroughCache(ids, snapshot, storedBlocks);
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
      // The column joined the catalog after this segment was written; its rows read as the
      // column's backfill, or NULL when it has none.
      const absent = column.backfill ?? null;
      return Array.from({ length: segment.rowCount }, () => absent);
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
          leftOrder - rightOrder ||
          leftVersion - rightVersion ||
          (left.commitOrdinal ?? 0) - (right.commitOrdinal ?? 0) ||
          left.id.localeCompare(right.id)
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

  /**
   * Encodes one column block, choosing its codec from what gzip achieved on this column's
   * previous block. Columns are homogeneous down their length, so one observation predicts the
   * next block; a column that gzip cannot compress is written raw until it is re-probed, and
   * the codec each block actually used is recorded in the block itself, so a wrong guess costs
   * bytes and never correctness.
   */
  async #encodeColumnBlock(columnId: string, input: ColumnInput): Promise<Uint8Array> {
    return this.#encodePreferredBlock(
      columnId,
      this.#compression,
      columnInputBytesBelow(input, GZIP_MINIMUM_INPUT_BYTES),
      (compression) => encodeBlock(input, compression),
    );
  }

  /**
   * Applies the same adaptive gzip rule to ordinary writes and compaction output. `gzip` is a
   * preference, not a promise: tiny inputs and probes that save less than 20% stay raw. Only a
   * failed verdict is cached, so successful columns do not leave one map entry behind forever.
   */
  async #encodePreferredBlock(
    columnId: string,
    preferred: Compression,
    belowMinimum: boolean,
    encode: (compression: Compression) => Promise<Uint8Array>,
  ): Promise<Uint8Array> {
    if (preferred !== "gzip") return encode(preferred);
    if (belowMinimum) return encode("raw");
    const verdict = this.#gzipVerdicts.get(columnId);
    if (verdict !== undefined) {
      if (verdict < GZIP_REPROBE_BLOCKS) {
        this.#gzipVerdicts.set(columnId, verdict + 1);
        return encode("raw");
      }
      this.#gzipVerdicts.delete(columnId);
    }
    const bytes = await encode("gzip");
    const description = inspectBlock(bytes);
    const worthwhile = description.encodedLength >= bytes.byteLength * GZIP_WORTHWHILE_RATIO;
    if (!worthwhile) {
      if (
        !this.#gzipVerdicts.has(columnId) &&
        this.#gzipVerdicts.size >= GZIP_VERDICT_CACHE_LIMIT
      ) {
        const oldest = this.#gzipVerdicts.keys().next().value;
        if (oldest !== undefined) this.#gzipVerdicts.delete(oldest);
      }
      this.#gzipVerdicts.set(columnId, 0);
      // Nothing was gained, so hand back the uncompressed form rather than make every read of
      // this block pay to inflate it.
      return encode("raw");
    }
    this.#gzipVerdicts.delete(columnId);
    return bytes;
  }

  async #findTable(name: string): Promise<TableRecord> {
    const table = await this.store.getTableByName(name);
    if (table === undefined) throw new Error(`Table not found: ${name}`);
    // Reads resolve a view into its query before reaching here, so a view arriving at this
    // point is a write, a DDL statement, or a path that forgot to rewrite: all of them errors.
    if (table.view !== undefined) throw new TypeError(`${name} is a view, not a table`);
    return table;
  }
}

/** What a MERGE decided for one source's rows, ready to apply as three batched writes. */
interface MergeWork {
  updates: Map<string, { key: Exclude<BatchValue, null>; changes: Record<string, BatchValue> }>;
  deletes: Map<string, QueryValue>;
  inserts: Array<Record<string, BatchValue>>;
}

/**
 * Chooses each source row's branch and evaluates what that branch would write (F312). Pure: it
 * reads the source rows and the target rows they matched and returns the work, so every decision
 * is made before any of it is applied — which is what lets the caller apply the result as three
 * batches instead of row by row.
 */
function classifyMergeRows(input: {
  statement: Extract<CompiledStatement, { kind: "merge" }>;
  target: TableRecord;
  keyColumn: TableColumnRecord;
  sourceRows: QueryResult["rows"];
  keys: readonly QueryValue[];
  present: ReadonlyMap<string, DatabaseRow>;
}): MergeWork {
  const { statement, target, keyColumn, sourceRows, keys, present } = input;
  const work: MergeWork = { updates: new Map(), deletes: new Map(), inserts: [] };
  const claimed = new Set<string>();
  sourceRows.forEach((sourceRow, index) => {
    const key = keys[index] ?? null;
    const token = key === null ? undefined : keyToken(keyColumn.type, key);
    const matched = token === undefined ? undefined : present.get(token);
    if (key !== null && token !== undefined && matched !== undefined) {
      // SQL:2023 15.8: two source rows may not both act on one target row. Applying the last one
      // silently would make the result depend on the order the source happened to produce.
      if (claimed.has(token)) {
        throw new TypeError(
          `MERGE matched ${target.name} row ${formatValue(key)} from more than one source row`,
        );
      }
      claimed.add(token);
    }
    const context = { [statement.source.alias]: sourceRow, [statement.alias]: matched };
    for (const branch of statement.branches) {
      // The first branch whose match state and condition both hold decides this row.
      if ((branch.when === "matched") !== (matched !== undefined)) continue;
      if (
        branch.condition !== undefined &&
        evaluateJoinedRowExpression(branch.condition, context) !== true
      ) {
        continue;
      }
      if (branch.action.kind === "delete") {
        if (key !== null && token !== undefined) {
          work.deletes.set(token, key);
          work.updates.delete(token);
        }
        return;
      }
      if (branch.action.kind === "update") {
        if (key === null || token === undefined) return;
        const changes: Record<string, BatchValue> = {};
        for (const assignment of branch.action.assignments) {
          if (assignment.column === keyColumn.name) {
            throw new TypeError(`MERGE cannot update the unique key: ${keyColumn.name}`);
          }
          changes[assignment.column] = evaluateJoinedRowExpression(assignment.expression, context);
        }
        work.updates.set(token, { key, changes });
        return;
      }
      const values = branch.action.values;
      const columns =
        branch.action.columns.length > 0
          ? branch.action.columns
          : target.columns.map(({ name }) => name);
      if (columns.length !== values.length) {
        throw new TypeError("MERGE INSERT values must match the table's columns");
      }
      const row: Record<string, BatchValue> = {};
      columns.forEach((column, position) => {
        const value = values[position];
        if (value !== undefined) row[column] = evaluateJoinedRowExpression(value, context);
      });
      work.inserts.push(row);
      return;
    }
  });
  return work;
}

/** Whether a view's body reads one table by name, at any depth of its query. */
function viewReadsTable(sql: string, table: string): boolean {
  try {
    return collectRealTableNames(compileQuery(sql)).includes(table);
  } catch {
    // A view whose body no longer compiles cannot be shown to depend on this table, and its own
    // next read will say so far more clearly than a refused drop would.
    return false;
  }
}

/**
 * The source-side expression a MERGE matches on. The condition has to be an equality naming the
 * target's unique key on one side, because that is the address the keyed write paths use; the
 * other side is what each source row contributes.
 */
function mergeSourceKeyExpression(
  statement: Extract<CompiledStatement, { kind: "merge" }>,
  keyColumn: string,
): Expression {
  const namesKey = (expression: Expression): boolean =>
    expression.kind === "column" &&
    (expression.reference === `${statement.alias}.${keyColumn}` ||
      expression.reference === keyColumn);
  const condition = statement.on;
  if (condition.kind === "condition" && condition.operator === "=") {
    if (namesKey(condition.left)) return condition.right;
    if (namesKey(condition.right)) return condition.left;
  }
  throw new TypeError(
    `MERGE ON must equate ${statement.alias}.${keyColumn} with a source value: the unique key is how rows are addressed`,
  );
}

/** The SELECT that materializes a MERGE's source rows under its correlation name. */
function mergeSourceSql(statement: Extract<CompiledStatement, { kind: "merge" }>): string {
  const alias = quoteSqlIdentifier(statement.source.alias);
  if (statement.source.table !== undefined) {
    return `SELECT * FROM ${quoteSqlIdentifier(statement.source.table)} AS ${alias}`;
  }
  const query = statement.source.sql;
  if (query === undefined) throw new TypeError("A MERGE source needs a table or a query");
  return `SELECT * FROM (${query}) AS ${alias}`;
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

/**
 * Compiled CHECK expressions, keyed by table id and catalog revision rather than by the record
 * object: the store hands out a fresh clone on every read, so an identity-keyed cache would miss
 * every time and recompile the constraints once per written batch. A changed revision compiles
 * again, which is what makes an altered constraint take effect.
 */
const compiledChecks = new Map<string, ReadonlyArray<{ name: string; expression: Expression }>>();
const COMPILED_CHECK_CACHE_LIMIT = 64;

function tableChecks(table: TableRecord): ReadonlyArray<{ name: string; expression: Expression }> {
  const key = `${table.id}/${String(table.revision ?? 0)}`;
  const cached = compiledChecks.get(key);
  if (cached !== undefined) return cached;
  const compiled = (table.checks ?? []).map((check) => ({
    name: check.name,
    expression: compileCheckExpression(check.sql, check.name),
  }));
  // Bounded and insertion-ordered: the oldest entry leaves, which is the table written least
  // recently. Constraint expressions are small, so the cap bounds the map, not the memory.
  if (compiledChecks.size >= COMPILED_CHECK_CACHE_LIMIT) {
    const oldest = compiledChecks.keys().next().value;
    if (oldest !== undefined) compiledChecks.delete(oldest);
  }
  compiledChecks.set(key, compiled);
  return compiled;
}

/**
 * Applies a table's CHECK constraints to one row (E141-06). A constraint fails only when it
 * evaluates to false: SQL's three-valued logic lets an unknown pass, which is why a NULL column
 * satisfies `CHECK (amount > 0)` unless the column is also NOT NULL.
 */
function assertRowChecks(table: TableRecord, row: Record<string, BatchValue>, index: number): void {
  for (const check of tableChecks(table)) {
    let value: unknown;
    try {
      // The row binds under the table's own name, so `CHECK (t.a > 0)` and `CHECK (a > 0)`
      // both resolve.
      value = evaluateRowExpression(check.expression, table.name, row);
    } catch (error) {
      throw new TypeError(
        `CHECK ${check.name} could not be evaluated for row ${String(index)}: ${
          error instanceof Error ? error.message : String(error)
        }`,
        { cause: error },
      );
    }
    if (value === false) {
      throw new TypeError(`CHECK ${check.name} failed for row ${String(index)} of ${table.name}`);
    }
  }
}

/** Reads one row out of a columnar batch, for the row-shaped checks. */
function batchRowAt(
  table: TableRecord,
  input: ColumnarBatch,
  index: number,
): Record<string, BatchValue> {
  const row: Record<string, BatchValue> = {};
  for (const column of table.columns)
    row[column.name] = input.columns[column.name]?.[index] ?? null;
  return row;
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
  // The constraints run last, over whole rows, once every column has been type-checked.
  if ((table.checks ?? []).length > 0) {
    for (let index = 0; index < rowCount; index += 1) {
      assertRowChecks(table, batchRowAt(table, input, index), index);
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

/**
 * Whether a column block's logical payload is under `limit` bytes: strings by length (two
 * bytes a code unit, stopping as soon as the limit is reached), everything else eight bytes a
 * value. An estimate, for the write path's codec choice — not an encoded size.
 */
function columnInputBytesBelow(input: ColumnInput, limit: number): boolean {
  if (input.type !== "string") return input.values.length * 8 < limit;
  let bytes = 0;
  for (const value of input.values) {
    bytes += 8 + (value === null ? 0 : value.length * 2);
    if (bytes >= limit) return false;
  }
  return true;
}

/** `keyToken` for values that may not encode: undefined instead of a thrown encoding error. */
function tryKeyToken(type: SimpleDataType, value: BatchValue): string | undefined {
  try {
    return keyToken(type, value);
  } catch {
    return undefined;
  }
}

/**
 * Whether a table's visible segments warrant a background fold: enough segments for a scan to
 * pay per-segment overhead, or enough deltas that every query replays a history. Counted in
 * segments, not rows — a handful of deltas costs little however many rows they hold, and
 * folding rewrites the table's anchor, so it is reserved for when the count has built up.
 */
/**
 * Whether a table's visible history warrants a background fold: enough level-zero segments to
 * fragment a scan, or enough deltas to cost one. Partitions compaction itself published
 * (level one and above) are the folded state, not fragmentation, and do not count — a large
 * keyed table is many partitions by design.
 */
function autoCompactionDue(segments: readonly SegmentRecord[]): boolean {
  let levelZero = 0;
  let deltas = 0;
  for (const segment of segments) {
    if ((segment.level ?? 0) === 0) levelZero += 1;
    const kind = segment.kind ?? "insert";
    if (kind !== "insert" && kind !== "base") deltas += 1;
  }
  return levelZero >= AUTO_COMPACT_SCAN_SEGMENTS || deltas >= AUTO_COMPACT_DELTA_SEGMENTS;
}

/** A macrotask boundary, so background work lets queued queries and writes run between steps. */
function yieldToEventLoop(): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, 0));
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

function nonNegativeWholeNumber(value: number, name: string): number {
  if (!Number.isSafeInteger(value) || value < 0) {
    throw new RangeError(`${name} must be a non-negative whole number`);
  }
  return value;
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

/** One decoded block resident in a streamed scan, in its vectorized (buffer pool) form. */
interface ResidentStreamBlock {
  vector: ColumnVector;
  startRow: number;
}

/**
 * Replaces a streamed vector's resident window. The common case — a batch-aligned window
 * covering exactly one block — installs the buffer pool's block vector by reference: no
 * copies, no re-decoding, and a dictionary object that stays identical across queries so
 * per-dictionary expression caches survive. A window straddling blocks (only where block
 * row counts are not batch-aligned) stitches the block vectors into one copied window.
 * Modeled bytes are reserved either way; the caller releases the superseded reservations.
 */
function installStreamedWindow(
  vector: ColumnVector,
  resident: readonly ResidentStreamBlock[],
  windowStart: number,
  windowRows: number,
  memory: QueryMemoryContext,
  label: string,
  reservations: QueryMemoryReservation[],
): void {
  const single = resident.length === 1 ? resident[0] : undefined;
  const mutable = vector as unknown as MutableStreamedVectorFields;
  if (single !== undefined) {
    const block = single.vector;
    if (single.startRow === windowStart && block.length === windowRows) {
      reservations.push(memory.reserve(blockVectorRetainedBytes(block), label));
      mutable.validity = block.validity;
      if (block.kind === "string") {
        mutable.codes = block.codes;
        mutable.dictionary = block.dictionary as string[];
      } else {
        mutable.values = block.values;
      }
      mutable.window = { start: windowStart, length: windowRows };
      return;
    }
  }
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
  const dictionary: string[] = [];
  const dictionaryIndex = new Map<string, number>();
  let dictionaryBytes = 0;
  for (const block of resident) {
    const offset = block.startRow - windowStart;
    const blockVector = block.vector;
    for (let row = 0; row < blockVector.length; row += 1) {
      if (bitmapHasValue(blockVector.validity, row)) setBitmapValue(validity, offset + row);
    }
    if (blockVector.kind === "string") {
      if (codes === undefined) throw new Error("String vector is missing");
      // Remap block codes through the merged window dictionary; the remap array makes the
      // per-row cost one lookup regardless of dictionary size.
      const remap = new Int32Array(blockVector.dictionary.length).fill(-1);
      for (let row = 0; row < blockVector.length; row += 1) {
        const code = blockVector.codes[row] ?? NULL_STRING_VECTOR_CODE;
        if (code === NULL_STRING_VECTOR_CODE) continue;
        let mapped = remap[code] ?? -1;
        if (mapped < 0) {
          const value = blockVector.dictionary[code] ?? "";
          let existing = dictionaryIndex.get(value);
          if (existing === undefined) {
            existing = dictionary.length;
            dictionary.push(value);
            dictionaryIndex.set(value, existing);
            dictionaryBytes += value.length;
          }
          remap[code] = existing;
          mapped = existing;
        }
        codes[offset + row] = mapped;
      }
    } else if (values !== undefined) {
      values.set(blockVector.values, offset);
    }
  }
  if (dictionary.length > 0) reservations.push(memory.reserve(dictionaryBytes, label));
  mutable.validity = validity;
  if (values !== undefined) mutable.values = values;
  if (codes !== undefined) {
    mutable.codes = codes;
    mutable.dictionary = dictionary;
  }
  mutable.window = { start: windowStart, length: windowRows };
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

/**
 * Writes a column's backfill across a run of slots, for rows whose segment predates the column.
 * Doing it here rather than at write time is what keeps adding a column metadata-only: the stored
 * segments never change, and compaction folds the value in the next time it rewrites them.
 */
function fillColumnVectorRange(
  vector: ColumnVector,
  start: number,
  count: number,
  value: boolean | number | string | Date,
): void {
  for (let offset = 0; offset < count; offset += 1) {
    setBitmapValue(vector.validity, start + offset);
  }
  if (vector.kind === "boolean") {
    vector.values.fill(value === true ? 1 : 0, start, start + count);
    return;
  }
  if (vector.kind === "number" || vector.kind === "datetime") {
    vector.values.fill(
      value instanceof Date ? value.getTime() : Number(value),
      start,
      start + count,
    );
    return;
  }
  const text = String(value);
  const dictionary = vector.dictionary as string[];
  let code = dictionary.indexOf(text);
  if (code === -1) code = dictionary.push(text) - 1;
  vector.codes.fill(code, start, start + count);
}

/** Interns one string in a builder-backed dictionary, for the append path's own arrays. */
function dictionaryCodeForText(builder: StringDictionaryBuilder, text: string): number {
  const bytes = new TextEncoder().encode(text);
  return builder.codeForBytes(bytes, 0, bytes.length);
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

/** A unique key read straight out of a vector: no per-row string token, no allocation. */
type OverlayKey = string | number | boolean;

/** One update segment row: which columns it changed, and whether a base row took it. */
interface OverlayUpdate {
  readonly order: number;
  readonly row: number;
  readonly vectors: ReadonlyMap<string, ColumnVector>;
  applied: boolean;
}

/** Whether a visible segment is a delete or update delta rather than appended rows. */
function mutationSegmentKind(segment: SegmentRecord): boolean {
  const kind = segment.kind ?? "insert";
  return kind !== "insert" && kind !== "base";
}

/** One update row standing in for a base row's column: the resident update vector and the row in it. */
interface OverlayPatch {
  readonly vector: ColumnVector;
  readonly row: number;
}

/**
 * The replayed mutation state of one visible segment set, shared by every query over it until
 * the next commit: see `#streamedOverlayState`.
 */
interface StreamedOverlayState {
  /** Rows in the scan (insert/base) segments, before deletes. */
  readonly baseRows: number;
  readonly deadCount: number;
  /** One bit per base row; set when a delete removed it. */
  readonly dead: Uint8Array;
  /** Per patched base row, the column IDs an update replaced. */
  readonly patches: ReadonlyMap<number, ReadonlyMap<string, OverlayPatch>>;
  /** The patched base rows in ascending order, for range counts. */
  readonly patchedSlots: Uint32Array;
  /** Modeled retained bytes, as charged to the buffer pool and tallied to each query. */
  readonly bytes: number;
}

/**
 * The shape of one outer overlay window over base rows `[from, to)`, as a flat list of steps
 * in base order: a pair `(start, length)` with a positive length is a run of live rows nothing
 * patched, copied as one slice; a pair `(row, 0)` is a single patched live row. Dead rows
 * appear in neither. Built once per window and applied to every projected column.
 */
type OverlayWindowSteps = number[];

const BYTE_POPCOUNT = new Uint8Array(256).map((_, byte) => {
  let count = 0;
  for (let value = byte; value !== 0; value &= value - 1) count += 1;
  return count;
});

/** Set bits in `bitmap` over bit indexes `[from, to)`. */
function bitmapCountRange(bitmap: Uint8Array, from: number, to: number): number {
  let count = 0;
  let index = from;
  while (index < to && (index & 7) !== 0) {
    if (bitmapHasValue(bitmap, index)) count += 1;
    index += 1;
  }
  while (index + 8 <= to) {
    count += BYTE_POPCOUNT[bitmap[index >>> 3] ?? 0] ?? 0;
    index += 8;
  }
  while (index < to) {
    if (bitmapHasValue(bitmap, index)) count += 1;
    index += 1;
  }
  return count;
}

/** The first index in ascending `sorted` whose value is at least `value`. */
function sortedLowerBound(sorted: Uint32Array, value: number): number {
  let low = 0;
  let high = sorted.length;
  while (low < high) {
    const middle = (low + high) >>> 1;
    if ((sorted[middle] ?? 0) < value) low = middle + 1;
    else high = middle;
  }
  return low;
}

/** Members of ascending `sorted` in `[from, to)`. */
function sortedCountRange(sorted: Uint32Array, from: number, to: number): number {
  return sortedLowerBound(sorted, to) - sortedLowerBound(sorted, from);
}

function overlayWindowRuns(
  dead: Uint8Array,
  patchedSlots: Uint32Array,
  from: number,
  to: number,
  patchedInWindow: number,
): OverlayWindowSteps {
  const steps: OverlayWindowSteps = [];
  let nextPatched = patchedInWindow > 0 ? sortedLowerBound(patchedSlots, from) : -1;
  let row = from;
  while (row < to) {
    // Dead rows, eight at a time where a whole byte is dead.
    if (bitmapHasValue(dead, row)) {
      row += 1;
      while (row < to && (row & 7) === 0 && dead[row >>> 3] === 0xff && row + 8 <= to) row += 8;
      while (row < to && bitmapHasValue(dead, row)) row += 1;
      continue;
    }
    const patchedRow = nextPatched >= 0 ? (patchedSlots[nextPatched] ?? to) : to;
    if (row === patchedRow) {
      steps.push(row, 0);
      row += 1;
      nextPatched += 1;
      if (nextPatched >= patchedSlots.length) nextPatched = -1;
      continue;
    }
    // A live run: up to the next patched row, the window end, or the next dead row — live
    // rows are consecutive except where a delete cut them, and whole live bytes skip in one.
    const limit = Math.min(to, patchedRow);
    const runStart = row;
    row += 1;
    while (row < limit && (row & 7) === 0 && row + 8 <= limit && dead[row >>> 3] === 0) row += 8;
    while (row < limit && !bitmapHasValue(dead, row)) row += 1;
    steps.push(runStart, row - runStart);
  }
  return steps;
}

/**
 * Copies `length` bits from `source` at bit `sourceStart` to `target` at bit `targetStart`,
 * whole bytes at a time once the target is byte-aligned: the target bytes it overwrites lie
 * entirely inside the copied range, so the target's other bits are left alone. This is what
 * makes a validity copy proportional to bytes rather than to cells.
 */
function copyBitRun(
  source: Uint8Array,
  sourceStart: number,
  target: Uint8Array,
  targetStart: number,
  length: number,
): void {
  let remaining = length;
  let from = sourceStart;
  let to = targetStart;
  while (remaining > 0 && (to & 7) !== 0) {
    if (bitmapHasValue(source, from)) setBitmapValue(target, to);
    from += 1;
    to += 1;
    remaining -= 1;
  }
  const shift = from & 7;
  if (shift === 0) {
    const bytes = remaining >>> 3;
    if (bytes > 0) {
      target.set(source.subarray(from >>> 3, (from >>> 3) + bytes), to >>> 3);
      from += bytes * 8;
      to += bytes * 8;
      remaining -= bytes * 8;
    }
  } else {
    while (remaining >= 8) {
      const sourceByte = from >>> 3;
      target[to >>> 3] =
        (((source[sourceByte] ?? 0) >>> shift) | ((source[sourceByte + 1] ?? 0) << (8 - shift))) &
        0xff;
      from += 8;
      to += 8;
      remaining -= 8;
    }
  }
  while (remaining > 0) {
    if (bitmapHasValue(source, from)) setBitmapValue(target, to);
    from += 1;
    to += 1;
    remaining -= 1;
  }
}

/**
 * An outer window that is the inner window's rows from `offset` on, by reference: typed-array
 * views over the resident block, and its dictionary as-is. Validity is a view too when the
 * offset falls on a byte, and otherwise the one small copy a bit offset forces.
 */
function overlayWindowView(
  inner: ColumnVector,
  offset: number,
  rows: number,
  memory: QueryMemoryContext,
  column: TableColumnRecord,
  reservations: QueryMemoryReservation[],
): MutableStreamedVectorFields {
  let validity: Uint8Array;
  if ((offset & 7) === 0) {
    validity = inner.validity.subarray(offset >>> 3, (offset >>> 3) + Math.ceil(rows / 8));
  } else {
    validity = new Uint8Array(Math.ceil(rows / 8));
    reservations.push(memory.reserve(validity.byteLength, `Streamed window ${column.name}`));
    copyBitRun(inner.validity, offset, validity, 0, rows);
  }
  const fields: MutableStreamedVectorFields = { validity, window: { start: 0, length: rows } };
  if (inner.kind === "string") {
    fields.codes = inner.codes.subarray(offset, offset + rows);
    fields.dictionary = inner.dictionary as string[];
  } else {
    fields.values = inner.values.subarray(offset, offset + rows);
  }
  return fields;
}

/**
 * An outer window compacted from an inner window: live runs copied as slices, patched rows
 * read from their update vectors. A string window shares the inner dictionary unless a patch
 * has to add to it, in which case it copies the dictionary first.
 */
function overlayWindowCompacted(
  inner: ColumnVector,
  innerWindowStart: number,
  steps: OverlayWindowSteps,
  rows: number,
  patches: ReadonlyMap<number, ReadonlyMap<string, OverlayPatch>> | undefined,
  column: TableColumnRecord,
  memory: QueryMemoryContext,
  reservations: QueryMemoryReservation[],
): MutableStreamedVectorFields {
  const validityBytes = Math.ceil(rows / 8);
  const typedBytes =
    validityBytes +
    (inner.kind === "boolean"
      ? rows
      : inner.kind === "string"
        ? rows * Uint32Array.BYTES_PER_ELEMENT
        : rows * Float64Array.BYTES_PER_ELEMENT);
  reservations.push(memory.reserve(typedBytes, `Streamed window ${column.name}`));
  const validity = new Uint8Array(validityBytes);
  const values =
    inner.kind === "boolean"
      ? new Uint8Array(rows)
      : inner.kind === "string"
        ? undefined
        : new Float64Array(rows);
  const codes = inner.kind === "string" ? new Uint32Array(rows) : undefined;
  codes?.fill(NULL_STRING_VECTOR_CODE);
  let dictionary = inner.kind === "string" ? (inner.dictionary as string[]) : undefined;
  let dictionaryIndex: Map<string, number> | undefined;
  let dictionaryCopied = false;
  const target = (
    codes !== undefined
      ? { kind: "string", length: rows, validity, codes, dictionary: dictionary ?? [] }
      : { kind: inner.kind, length: rows, validity, values }
  ) as ColumnVector;
  let out = 0;
  for (let index = 0; index < steps.length; index += 2) {
    const start = steps[index] ?? 0;
    const length = steps[index + 1] ?? 0;
    const patch = length === 0 ? patches?.get(start)?.get(column.id) : undefined;
    if (patch === undefined) {
      const count = Math.max(1, length);
      copyVectorSpan(inner, start - innerWindowStart, count, target, out);
      out += count;
      continue;
    }
    if (target.kind === "string" && !dictionaryCopied) {
      // A patch value may be new to this window's dictionary, and the inner's belongs to the
      // buffer pool: copy before the first append, and index the copy for the lookups.
      dictionary = [...(dictionary ?? [])];
      (target as unknown as { dictionary: string[] }).dictionary = dictionary;
      dictionaryIndex = new Map(dictionary.map((value, code) => [value, code]));
      dictionaryCopied = true;
    }
    copyColumnVectorValue(patch.vector, patch.row, target, out, dictionaryIndex);
    out += 1;
  }
  if (out !== rows) throw new Error(`Column row count mismatch: ${column.name}`);
  if (dictionaryCopied && dictionary !== undefined) {
    let dictionaryBytes = 0;
    for (const value of dictionary) dictionaryBytes += 16 + value.length * 2;
    reservations.push(memory.reserve(dictionaryBytes, `Streamed window ${column.name}`));
  }
  const fields: MutableStreamedVectorFields = { validity, window: { start: 0, length: rows } };
  if (codes !== undefined) {
    fields.codes = codes;
    fields.dictionary = dictionary ?? [];
  } else if (values !== undefined) {
    fields.values = values;
  }
  return fields;
}

/**
 * Reads a key column's values as primitives. Dictionary-coded strings resolve through the
 * dictionary the vector already holds, so a string key costs one array index and no encoding.
 * Undefined for a windowed vector or a null key — shapes the overlay refuses rather than guesses.
 */
function columnVectorKeyReader(
  vector: ColumnVector,
): ((row: number) => OverlayKey | undefined) | undefined {
  if (vector.window !== undefined) return undefined;
  const validity = vector.validity;
  if (vector.kind === "string") {
    const { codes, dictionary } = vector;
    return (row) =>
      bitmapHasValue(validity, row) ? dictionary[codes[row] ?? NULL_STRING_VECTOR_CODE] : undefined;
  }
  if (vector.kind === "boolean") {
    const values = vector.values;
    return (row) => (bitmapHasValue(validity, row) ? values[row] === 1 : undefined);
  }
  const values = vector.values;
  return (row) => (bitmapHasValue(validity, row) ? values[row] : undefined);
}

/** `columnVectorKeyReader` for a key column that must be readable: a null key is corruption. */
function requiredColumnVectorKeyReader(vector: ColumnVector): (row: number) => OverlayKey {
  const read = columnVectorKeyReader(vector);
  if (read === undefined) throw new Error("Unique key vector cannot be read as keys");
  return (row) => {
    const key = read(row);
    if (key === undefined) throw new TypeError("Unique key cannot be null");
    return key;
  };
}

/**
 * The touched keys as one IN zone predicate, or undefined when zone maps cannot judge them
 * (a string key has no numeric zone map, and an empty set has nothing to prune against).
 */
function touchedKeyPredicate(
  keyColumn: TableColumnRecord,
  touched: ReadonlySet<OverlayKey>,
): ZonePredicate | undefined {
  if (keyColumn.type !== "number" && keyColumn.type !== "datetime") return undefined;
  if (touched.size === 0) return undefined;
  const members = new Float64Array(touched.size);
  let index = 0;
  for (const key of touched) {
    if (typeof key !== "number") return undefined;
    members[index] = key;
    index += 1;
  }
  members.sort();
  return { column: keyColumn, operator: "IN", value: members[0] ?? 0, members };
}

/**
 * Copies a run of rows between vectors: values as one typed-array slice, validity as a bit run.
 * A string run needs `remap` unless both sides share a dictionary — the codes mean nothing on
 * their own. This is what keeps a copy proportional to bytes rather than to cells. The target
 * validity bits of the run must be clear beforehand, as a fresh window's are.
 */
function copyVectorSpan(
  source: ColumnVector,
  sourceStart: number,
  length: number,
  target: ColumnVector,
  targetStart: number,
  remap?: Uint32Array,
): void {
  if (source.kind === "string") {
    if (target.kind !== "string") throw new Error("Column vector type mismatch");
    if (remap === undefined) {
      target.codes.set(source.codes.subarray(sourceStart, sourceStart + length), targetStart);
    } else {
      for (let index = 0; index < length; index += 1) {
        if (!bitmapHasValue(source.validity, sourceStart + index)) continue;
        target.codes[targetStart + index] =
          remap[source.codes[sourceStart + index] ?? NULL_STRING_VECTOR_CODE] ??
          NULL_STRING_VECTOR_CODE;
      }
    }
  } else if (source.kind === "boolean") {
    if (target.kind !== "boolean") throw new Error("Column vector type mismatch");
    target.values.set(source.values.subarray(sourceStart, sourceStart + length), targetStart);
  } else {
    if (target.kind === "string" || target.kind === "boolean") {
      throw new Error("Column vector type mismatch");
    }
    target.values.set(source.values.subarray(sourceStart, sourceStart + length), targetStart);
  }
  copyBitRun(source.validity, sourceStart, target.validity, targetStart, length);
}

/**
 * The live rows of a vector, in order. Runs between deletions copy as typed-array slices and
 * the dictionary is shared, so dropping one row of a million does not rebuild the column.
 */
function compactColumnVector(
  source: ColumnVector,
  alive: Uint8Array,
  aliveCount: number,
): ColumnVector {
  const validity = new Uint8Array(Math.ceil(aliveCount / 8));
  const target: ColumnVector =
    source.kind === "string"
      ? {
          kind: "string",
          length: aliveCount,
          validity,
          codes: new Uint32Array(aliveCount).fill(NULL_STRING_VECTOR_CODE),
          dictionary: source.dictionary,
        }
      : source.kind === "boolean"
        ? { kind: "boolean", length: aliveCount, validity, values: new Uint8Array(aliveCount) }
        : { kind: source.kind, length: aliveCount, validity, values: new Float64Array(aliveCount) };
  let output = 0;
  let runStart = -1;
  for (let row = 0; row <= source.length; row += 1) {
    const live = row < source.length && alive[row] === 1;
    if (live) {
      if (runStart < 0) runStart = row;
      continue;
    }
    if (runStart < 0) continue;
    copyVectorSpan(source, runStart, row - runStart, target, output);
    output += row - runStart;
    runStart = -1;
  }
  return target;
}

/**
 * The live rows of a column an update changed: cell by cell, because a patched string value can
 * be one the source dictionary never held. Later updates win, in segment order.
 */
function patchColumnVector(
  source: ColumnVector,
  column: TableColumnRecord,
  alive: Uint8Array,
  aliveCount: number,
  patches: ReadonlyMap<number, readonly OverlayUpdate[]>,
): ColumnVector {
  const target = createEmptyColumnVector(column.type, aliveCount);
  const dictionaryIndex = column.type === "string" ? new Map<string, number>() : undefined;
  let output = 0;
  for (let row = 0; row < source.length; row += 1) {
    if (alive[row] !== 1) continue;
    let from = source;
    let fromRow = row;
    for (const update of patches.get(row) ?? []) {
      const vector = update.vectors.get(column.id);
      if (vector === undefined) continue;
      from = vector;
      fromRow = update.row;
    }
    copyColumnVectorValue(from, fromRow, target, output, dictionaryIndex);
    output += 1;
  }
  return target;
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
  const literalValue = (column: TableColumnRecord, expression: Expression): number | undefined => {
    if (expression.kind !== "literal") return undefined;
    const value =
      column.type === "datetime"
        ? expression.value instanceof Date
          ? expression.value.getTime()
          : undefined
        : typeof expression.value === "number"
          ? expression.value
          : undefined;
    return value === undefined || !Number.isFinite(value) ? undefined : value;
  };
  for (const predicate of plan.predicates) {
    // A literal list prunes like a comparison does: a block whose range holds no member of
    // the list cannot contain a matching row.
    if (predicate.operator === "IN" && predicate.left.kind === "column") {
      const column = resolveColumn(predicate.left.reference);
      if (column === undefined || predicate.right.kind !== "list") continue;
      const members = new Set<number>();
      let usable = true;
      for (const item of predicate.right.items) {
        // A NULL member matches nothing, so it narrows nothing either; anything that is not a
        // literal of the column's type leaves the list unprunable.
        if (item.kind === "literal" && item.value === null) continue;
        const value = literalValue(column, item);
        if (value === undefined) {
          usable = false;
          break;
        }
        members.add(value);
      }
      if (!usable || members.size === 0) continue;
      const sorted = Float64Array.from(members).sort();
      output.push({ column, operator: "IN", value: sorted[0] ?? 0, members: sorted });
      continue;
    }
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

/** Index of the first ascending member at or above `value`, or the member count. */
function firstMemberAtLeast(members: Float64Array, value: number): number {
  let low = 0;
  let high = members.length;
  while (low < high) {
    const middle = (low + high) >>> 1;
    if ((members[middle] ?? 0) < value) low = middle + 1;
    else high = middle;
  }
  return low;
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
  if (predicate.operator === "IN") {
    const members = predicate.members;
    if (members === undefined) return true;
    const first = firstMemberAtLeast(members, zoneMap.min);
    return first < members.length && (members[first] ?? 0) <= zoneMap.max;
  }
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
  if (predicate.operator === "IN") {
    const members = predicate.members;
    if (members === undefined) return true;
    const first = firstMemberAtLeast(members, value);
    return first < members.length && (members[first] ?? 0) === value;
  }
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
  if (value !== "raw" && value !== "gzip") {
    throw new TypeError(`${name} must be raw or gzip`);
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

/** Modeled bytes per referenced key the merge planner and the partition probe hold resident. */
const MERGE_PLANNER_KEY_BYTES = 96;

/** How a keyed level-one fold cuts its output into partitions. */
interface KeyedPartitioning {
  /** Every level-one partition of the table at planning, in visible order. */
  readonly partitions: readonly SegmentRecord[];
  /** Target maximum rows per published partition. */
  readonly partitionRows: number;
  /** Whether new rows join the last partition rather than opening a partition behind it. */
  readonly absorbsTail: boolean;
  /** Exclusive upper bound for a published partition's order: the first unselected level-zero segment's order, or the next manifest version. */
  readonly nextLevelZeroOrder: number;
}

interface RechunkPartitioning {
  readonly partitionRows: number;
  /** Exclusive upper bound: first unselected L0 order, or the next manifest version. */
  readonly nextLevelZeroOrder: number;
}

function planLinearOutputPartitions(
  totalRows: number,
  partitionRows: number,
  firstOrder: number,
  nextOrder: number,
): MergeOutputPartition[] {
  const count = Math.max(1, Math.ceil(totalRows / partitionRows));
  const orders = fractionalLogicalOrders(firstOrder, nextOrder, count);
  const partitions: MergeOutputPartition[] = [];
  for (let index = 0, rowStart = 0; index < count; index += 1) {
    const rowCount = Math.min(partitionRows, totalRows - rowStart);
    const logicalOrder = orders[index];
    if (rowCount <= 0 || logicalOrder === undefined) {
      throw new Error("Rechunk partition layout is incomplete");
    }
    partitions.push({ rowStart, rowCount, logicalOrder });
    rowStart += rowCount;
  }
  return partitions;
}

/**
 * Cuts the canonical merged output into the partitions a keyed fold publishes.
 *
 * Each rewritten source partition's surviving rows form one region that keeps the partition's
 * logical order, and so its place among the partitions the fold leaves alone. The rows of the
 * level-zero sources — the tail — form a region behind every existing partition, or extend
 * the last partition's region when the fold absorbs them into it. A region is then chunked to
 * at most `partitionRows` rows per published partition. The first chunk keeps the source
 * partition's order and every further chunk takes an evenly spaced fractional order before
 * the unchanged successor;
 * a fresh tail starts at its earliest source's order. Fractional orders make room independent
 * of adjacent commit versions, so every output is bounded by `partitionRows`. The order is
 * stable: a published partition sorts strictly between its neighbours and below every
 * level-zero segment, so a later fold rewrites it alone without moving a row.
 */
function planOutputPartitions(
  partitioning: KeyedPartitioning,
  sources: readonly MergeCompactionSourceSegment[],
  sourceOutputRowStarts: ReadonlyMap<string, number>,
  totalRows: number,
): MergeOutputPartition[] {
  const { partitions, partitionRows, absorbsTail, nextLevelZeroOrder } = partitioning;
  const startOf = (segmentId: string): number => {
    const start = sourceOutputRowStarts.get(segmentId);
    if (start === undefined) throw new Error(`Merge source has no output position: ${segmentId}`);
    return start;
  };
  const levelZeroRowSources = sources.filter(
    (source) => source.level === 0 && mergeSourceBearsRows(source.kind),
  );
  const tailStart =
    levelZeroRowSources.length === 0 ? totalRows : startOf(levelZeroRowSources[0]?.segmentId ?? "");
  const sourceIds = new Set(sources.map((source) => source.segmentId));
  const sourcedPartitionIndexes = partitions.flatMap((partition, index) =>
    sourceIds.has(partition.id) ? [index] : [],
  );

  interface Region {
    rowStart: number;
    rowCount: number;
    /** The order the first chunk keeps, or null for a fresh tail. */
    anchorOrder: number | null;
    /** Exclusive lower bound for the orders of chunks that take fresh integers. */
    roomStart: number;
    /** Exclusive upper bound for every chunk order. */
    roomEnd: number;
    /** The order a fresh tail starts at when it fits. */
    preferredOrder: number;
  }
  const regions: Region[] = [];
  for (const [position, index] of sourcedPartitionIndexes.entries()) {
    const partition = partitions[index];
    const order = partition?.logicalOrder;
    if (partition === undefined || order === undefined) {
      throw new Error("Partitioned merge source is not a level-one partition");
    }
    const nextSourced = sourcedPartitionIndexes[position + 1];
    const isLast = index === partitions.length - 1;
    const rowStart = startOf(partition.id);
    const rowEnd =
      isLast && absorbsTail
        ? totalRows
        : nextSourced === undefined
          ? tailStart
          : startOf(partitions[nextSourced]?.id ?? "");
    const successorOrder = partitions[index + 1]?.logicalOrder ?? nextLevelZeroOrder;
    regions.push({
      rowStart,
      rowCount: rowEnd - rowStart,
      anchorOrder: order,
      roomStart: order,
      roomEnd: successorOrder,
      preferredOrder: order,
    });
  }
  if (!(absorbsTail && partitions.length > 0)) {
    const lastOrder = partitions[partitions.length - 1]?.logicalOrder ?? -1;
    regions.push({
      rowStart: tailStart,
      rowCount: totalRows - tailStart,
      anchorOrder: null,
      roomStart: lastOrder,
      roomEnd: nextLevelZeroOrder,
      preferredOrder: Math.min(
        ...sources.filter((source) => source.level === 0).map((source) => source.logicalOrder),
      ),
    });
  }

  const output: MergeOutputPartition[] = [];
  for (const region of regions) {
    if (region.rowCount <= 0) continue;
    const chunks = Math.max(1, Math.ceil(region.rowCount / partitionRows));
    const firstOrder = region.anchorOrder ?? region.preferredOrder;
    if (
      !validLogicalOrder(firstOrder) ||
      firstOrder >= region.roomEnd ||
      (region.anchorOrder === null && firstOrder <= region.roomStart)
    ) {
      throw new Error("Partitioned merge has no logical-order interval for its output");
    }
    const logicalOrders = fractionalLogicalOrders(firstOrder, region.roomEnd, chunks);
    const baseRows = Math.floor(region.rowCount / chunks);
    const extraRows = region.rowCount % chunks;
    let rowStart = region.rowStart;
    for (let chunk = 0; chunk < chunks; chunk += 1) {
      const rowCount = baseRows + (chunk < extraRows ? 1 : 0);
      const logicalOrder = logicalOrders[chunk];
      if (logicalOrder === undefined) throw new Error("Partition logical order is unavailable");
      output.push({ rowStart, rowCount, logicalOrder });
      rowStart += rowCount;
    }
  }
  let coveredRows = 0;
  for (const [index, partition] of output.entries()) {
    const previous = output[index - 1];
    if (
      partition.rowStart !== coveredRows ||
      partition.rowCount <= 0 ||
      partition.logicalOrder >= nextLevelZeroOrder ||
      (previous !== undefined && previous.logicalOrder >= partition.logicalOrder)
    ) {
      throw new Error("Partitioned merge produced an invalid partition layout");
    }
    coveredRows += partition.rowCount;
  }
  if (coveredRows !== totalRows) {
    throw new Error("Partitioned merge partitions do not cover the merged output");
  }
  return output;
}

/** `count` increasing doubles in [first, upper), retaining `first` exactly. */
function fractionalLogicalOrders(first: number, upper: number, count: number): number[] {
  if (!validLogicalOrder(first) || !Number.isFinite(upper) || upper <= first || count < 1) {
    throw new Error("Partition logical-order interval is invalid");
  }
  const orders: number[] = [];
  for (let index = 0; index < count; index += 1) {
    const order = index === 0 ? first : first + ((upper - first) * index) / count;
    const previous = orders[index - 1];
    if (
      !validLogicalOrder(order) ||
      order >= upper ||
      (previous !== undefined && order <= previous)
    ) {
      throw new Error("Partition logical-order precision is exhausted");
    }
    orders.push(order);
  }
  return orders;
}

/**
 * The planner's working memory, as `#resolveMergeOutput` allocates it: two bytes per slot, the
 * touched-key set and live-slot map over the delta keys, one patch array per patched row, one
 * decoded key block at a time, and the output ranges themselves — which number the source
 * blocks plus one per patched cell, not one per row. Deliberately generous per element; this
 * bound is what a caller's `memoryBudgetBytes` is judged against, so it must not be optimistic.
 */
function mergePlannerMemoryBound(
  table: TableRecord,
  segments: readonly MergeCompactionSourceSegment[],
  keyColumnId: string,
): number {
  const SLOT_BYTES = 2;
  const KEY_BYTES = MERGE_PLANNER_KEY_BYTES;
  const PATCH_ROW_BYTES = 64;
  const PATCH_CELL_BYTES = 48;
  const RANGE_BYTES = 80;
  const DECODED_KEY_BLOCK_FACTOR = 4;
  let slotRows = 0;
  let deltaKeys = 0;
  let patchRows = 0;
  let sourceBlocks = 0;
  let largestKeyBlockBytes = 0;
  for (const segment of segments) {
    if (mergeSourceBearsRows(segment.kind)) slotRows += segment.rowCount;
    if (mergeSourceReferencesKeys(segment.kind)) deltaKeys += segment.rowCount;
    if (segment.kind === "update" || segment.kind === "upsert") patchRows += segment.rowCount;
    for (const column of segment.columns) {
      sourceBlocks += column.sourceBlocks.length;
      if (column.columnId !== keyColumnId) continue;
      for (const block of column.sourceBlocks) {
        largestKeyBlockBytes = Math.max(largestKeyBlockBytes, block.encodedBytes);
      }
    }
  }
  const columns = table.columns.length;
  return safeWholeNumberSum(
    [
      safeWholeNumberProduct(slotRows, SLOT_BYTES, "Mutation compaction slots"),
      safeWholeNumberProduct(deltaKeys, KEY_BYTES, "Mutation compaction keys"),
      safeWholeNumberProduct(
        patchRows,
        safeWholeNumberSum(
          [
            PATCH_ROW_BYTES,
            safeWholeNumberProduct(columns, PATCH_CELL_BYTES, "Mutation patch cells"),
          ],
          "Mutation compaction patch row",
        ),
        "Mutation compaction patches",
      ),
      safeWholeNumberProduct(
        safeWholeNumberSum(
          [sourceBlocks, safeWholeNumberProduct(patchRows, columns, "Mutation patched cells")],
          "Mutation compaction ranges",
        ),
        RANGE_BYTES,
        "Mutation compaction range bytes",
      ),
      safeWholeNumberProduct(
        largestKeyBlockBytes,
        DECODED_KEY_BLOCK_FACTOR,
        "Mutation compaction decoded key block",
      ),
    ],
    "Mutation compaction planner memory",
  );
}

/** Whether a source of this kind contributes rows to the merged output. */
function mergeSourceBearsRows(kind: SegmentKind): boolean {
  return kind === "insert" || kind === "upsert" || kind === "base";
}

/** Whether a source of this kind names existing rows by key. */
function mergeSourceReferencesKeys(kind: SegmentKind): boolean {
  return kind === "delete" || kind === "update" || kind === "upsert";
}

/**
 * A decoded key value as the primitive the overlay replay keys on (`OverlayKey`): equal keys
 * are equal primitives, and one table's key has one type, so nothing can collide.
 */
function overlayKeyOf(type: SimpleDataType, value: BatchValue): OverlayKey {
  if (value === null) throw new TypeError("Unique key cannot be null");
  switch (type) {
    case "boolean":
      if (typeof value !== "boolean") throw new TypeError("Invalid boolean unique key");
      return value;
    case "number":
      if (typeof value !== "number" || !Number.isFinite(value)) {
        throw new TypeError("Invalid number unique key");
      }
      return value;
    case "string":
      if (typeof value !== "string") throw new TypeError("Invalid string unique key");
      return value;
    case "datetime":
      if (!(value instanceof Date) || !Number.isFinite(value.getTime())) {
        throw new TypeError("Invalid datetime unique key");
      }
      return value.getTime();
  }
}

interface MutableRowIdSpan {
  rowStart: number;
  rowCount: number;
  rowIdStart: bigint;
}

interface MutableMergeOutputSourceRange {
  outputRowStart: number;
  sourceBlockId: string;
  sourceRowStart: number;
  rowCount: number;
}

/**
 * Accumulates the merged output as coalesced row-ID spans and per-column source ranges. A run
 * of untouched rows appends at most one range per source block it crosses, whatever its
 * length; a patched row appends one range per column. Adjacent ranges over the same block
 * merge in place, so the finished plan is proportional to blocks plus patched cells.
 */
class MergeOutputBuilder {
  readonly #columns: readonly TableColumnRecord[];
  readonly #rowIdSpans: MutableRowIdSpan[] = [];
  readonly #rangesByColumn: MutableMergeOutputSourceRange[][];
  readonly #blocksBySegment = new Map<
    string,
    ReadonlyArray<readonly MergeCompactionSourceBlock[]>
  >();
  #totalRows = 0;

  constructor(columns: readonly TableColumnRecord[]) {
    this.#columns = columns;
    this.#rangesByColumn = columns.map(() => []);
  }

  /** Output rows appended so far. */
  get totalRows(): number {
    return this.#totalRows;
  }

  /** Rows `[rowStart, rowStart + rowCount)` of a row-bearing source, unchanged. */
  appendRun(segment: MergeCompactionSourceSegment, rowStart: number, rowCount: number): void {
    if (rowCount <= 0) return;
    this.#appendRowIds(segment, rowStart, rowCount);
    const blocksByColumn = this.#sourceBlocks(segment);
    for (let columnIndex = 0; columnIndex < this.#columns.length; columnIndex += 1) {
      const blocks = blocksByColumn[columnIndex];
      const ranges = this.#rangesByColumn[columnIndex];
      if (blocks === undefined || ranges === undefined) {
        throw new Error("Mutation output column is missing");
      }
      let outputRow = this.#totalRows;
      let remaining = rowCount;
      let rowIndex = rowStart;
      while (remaining > 0) {
        const block = rowRangeAt(blocks, rowIndex);
        if (block === undefined) {
          throw new Error(`Mutation source row is missing: ${segment.segmentId}`);
        }
        const count = Math.min(remaining, block.rowStart + block.rowCount - rowIndex);
        appendMergeOutputRange(ranges, outputRow, block.blockId, rowIndex - block.rowStart, count);
        outputRow += count;
        rowIndex += count;
        remaining -= count;
      }
    }
    this.#totalRows += rowCount;
  }

  /** One row of a row-bearing source whose columns may come from later mutations. */
  appendPatchedRow(
    segment: MergeCompactionSourceSegment,
    rowIndex: number,
    patch: ReadonlyArray<MergeResolvedSource | undefined> | undefined,
  ): void {
    this.#appendRowIds(segment, rowIndex, 1);
    for (let columnIndex = 0; columnIndex < this.#columns.length; columnIndex += 1) {
      const column = this.#columns[columnIndex];
      const ranges = this.#rangesByColumn[columnIndex];
      if (column === undefined || ranges === undefined) {
        throw new Error("Mutation output column is missing");
      }
      const source = patch?.[columnIndex] ?? mergeSourceAt(segment, column.id, rowIndex);
      appendMergeOutputRange(ranges, this.#totalRows, source.blockId, source.sourceRowIndex, 1);
    }
    this.#totalRows += 1;
  }

  finish(): {
    columns: MergeCompactionOutputColumn[];
    rowIdSpans: RowIdSpan[];
    totalRows: number;
  } {
    return {
      rowIdSpans: this.#rowIdSpans,
      columns: this.#columns.map((column, columnIndex) => {
        const sourceRanges = this.#rangesByColumn[columnIndex];
        if (sourceRanges === undefined) throw new Error("Mutation output column is missing");
        return { columnId: column.id, type: column.type, sourceRanges };
      }),
      totalRows: this.#totalRows,
    };
  }

  #sourceBlocks(
    segment: MergeCompactionSourceSegment,
  ): ReadonlyArray<readonly MergeCompactionSourceBlock[]> {
    let blocks = this.#blocksBySegment.get(segment.segmentId);
    if (blocks === undefined) {
      blocks = this.#columns.map((column) => {
        const source = segment.columns.find((candidate) => candidate.columnId === column.id);
        if (source === undefined) {
          throw new Error(`Mutation source row is missing: ${segment.segmentId}:${column.id}`);
        }
        return source.sourceBlocks;
      });
      this.#blocksBySegment.set(segment.segmentId, blocks);
    }
    return blocks;
  }

  #appendRowIds(segment: MergeCompactionSourceSegment, rowStart: number, rowCount: number): void {
    let outputRow = this.#totalRows;
    let remaining = rowCount;
    let rowIndex = rowStart;
    while (remaining > 0) {
      const span = rowRangeAt(segment.rowIdSpans, rowIndex);
      if (span === undefined) {
        throw new Error(`Mutation source row ID is missing: ${String(rowIndex)}`);
      }
      const count = Math.min(remaining, span.rowStart + span.rowCount - rowIndex);
      appendRowIdSpan(
        this.#rowIdSpans,
        outputRow,
        span.rowIdStart + BigInt(rowIndex - span.rowStart),
        count,
      );
      outputRow += count;
      rowIndex += count;
      remaining -= count;
    }
  }
}

/** Appends `rowCount` consecutive row IDs from `rowId`, extending the last span when contiguous. */
function appendRowIdSpan(
  spans: MutableRowIdSpan[],
  rowStart: number,
  rowId: bigint,
  rowCount: number,
): void {
  const previous = spans[spans.length - 1];
  if (
    previous !== undefined &&
    previous.rowStart + previous.rowCount === rowStart &&
    previous.rowIdStart + BigInt(previous.rowCount) === rowId
  ) {
    previous.rowCount += rowCount;
  } else {
    spans.push({ rowStart, rowCount, rowIdStart: rowId });
  }
}

/** Appends `rowCount` output rows read from one source block, extending the last range when contiguous. */
function appendMergeOutputRange(
  ranges: MutableMergeOutputSourceRange[],
  outputRowStart: number,
  sourceBlockId: string,
  sourceRowStart: number,
  rowCount: number,
): void {
  const previous = ranges[ranges.length - 1];
  if (
    previous?.sourceBlockId === sourceBlockId &&
    previous.outputRowStart + previous.rowCount === outputRowStart &&
    previous.sourceRowStart + previous.rowCount === sourceRowStart
  ) {
    previous.rowCount += rowCount;
  } else {
    ranges.push({ outputRowStart, sourceBlockId, sourceRowStart, rowCount });
  }
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
  window: { readonly rowStart: number; readonly rowCount: number } = {
    rowStart: 0,
    rowCount: plan.totalRows,
  },
): Record<string, string[]> {
  const windowEnd = window.rowStart + window.rowCount;
  return Object.fromEntries(
    plan.columns.map((column, columnIndex) => [
      column.columnId,
      plan.outputs.flatMap((output, outputIndex) =>
        output.rowStart >= window.rowStart && output.rowStart + output.rowCount <= windowEnd
          ? [physicalOutputBlockId(jobId, outputIndex, columnIndex)]
          : [],
      ),
    ]),
  );
}

/** The segment ID partition `index` of a partitioned merge publishes under. */
function partitionOutputSegmentId(outputSegmentId: string, index: number): string {
  return index === 0 ? outputSegmentId : `${outputSegmentId}/${String(index)}`;
}

/** Every segment a job publishes: one per output partition, or the single output segment. */
function compactionOutputSegmentIds(job: CompactionJobRecord): string[] {
  if (job.outputSegmentId === null) return [];
  const plan = job.rewritePlan;
  if ((plan?.kind !== "merge-v1" && plan?.kind !== "rechunk-v1") || plan.partitions === undefined) {
    return [job.outputSegmentId];
  }
  const outputSegmentId = job.outputSegmentId;
  return plan.partitions.map((_partition, index) =>
    partitionOutputSegmentId(outputSegmentId, index),
  );
}

/**
 * The segments a physical compaction publishes, with the blocks of the windows each covers.
 * A partitioned rewrite publishes one level-one segment per planned partition. A merge carries
 * the slice of its row-ID spans; a rechunk carries the corresponding contiguous interval.
 */
function compactionOutputSegments(
  table: TableRecord,
  job: CompactionJobRecord,
  plan: PhysicalCompactionRewritePlan,
  transactionId: string,
  createdAt: string,
): SegmentRecord[] {
  const outputSegmentId = job.outputSegmentId;
  if (outputSegmentId === null) throw new Error("Compaction output segment ID is missing");
  const keyColumn =
    table.uniqueKeyColumnId === undefined ? {} : { keyColumnId: table.uniqueKeyColumnId };
  const partitionOrdinal =
    job.outputPartitionOrdinal === undefined
      ? {}
      : { partitionOrdinal: job.outputPartitionOrdinal };
  if (plan.partitions !== undefined) {
    return plan.partitions.map((partition, index) => {
      const rowIdSpans =
        plan.kind === "merge-v1"
          ? sliceRowIdSpans(plan.rowIdSpans, partition.rowStart, partition.rowCount)
          : undefined;
      const envelope =
        rowIdSpans === undefined
          ? {
              start: plan.rowIdStart + BigInt(partition.rowStart),
              endExclusive: plan.rowIdStart + BigInt(partition.rowStart + partition.rowCount),
            }
          : rowIdSpanEnvelope(rowIdSpans);
      return {
        id: partitionOutputSegmentId(outputSegmentId, index),
        tableId: table.id,
        transactionId,
        rowCount: partition.rowCount,
        rowIdStart: envelope.start,
        rowIdEndExclusive: envelope.endExclusive,
        columnBlockIds: physicalOutputColumns(job.id, plan, partition),
        kind: plan.kind === "merge-v1" ? "base" : "insert",
        ...keyColumn,
        level: job.targetLevel,
        ...partitionOrdinal,
        logicalOrder: partition.logicalOrder,
        ...(rowIdSpans === undefined ? {} : { rowIdSpans }),
        createdAt,
      };
    });
  }
  return [
    {
      id: outputSegmentId,
      tableId: table.id,
      transactionId,
      rowCount: plan.totalRows,
      rowIdStart: plan.rowIdStart,
      rowIdEndExclusive: plan.rowIdEndExclusive,
      columnBlockIds: physicalOutputColumns(job.id, plan),
      kind: plan.kind === "merge-v1" ? "base" : "insert",
      ...keyColumn,
      level: job.targetLevel,
      ...partitionOrdinal,
      logicalOrder: plan.logicalOrder,
      ...(plan.kind === "merge-v1" ? { rowIdSpans: structuredClone(plan.rowIdSpans) } : {}),
      createdAt,
    },
  ];
}

/** The spans of output rows `[rowStart, rowStart + rowCount)`, rebased to start at row zero. */
function sliceRowIdSpans(
  spans: readonly RowIdSpan[],
  rowStart: number,
  rowCount: number,
): RowIdSpan[] {
  const sliced: MutableRowIdSpan[] = [];
  const rowEnd = rowStart + rowCount;
  for (const span of spans) {
    const spanEnd = span.rowStart + span.rowCount;
    if (spanEnd <= rowStart || span.rowStart >= rowEnd) continue;
    const start = Math.max(span.rowStart, rowStart);
    const end = Math.min(spanEnd, rowEnd);
    appendRowIdSpan(
      sliced,
      start - rowStart,
      span.rowIdStart + BigInt(start - span.rowStart),
      end - start,
    );
  }
  return sliced;
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
          reclaimedTransactionCount: job.reclaimedTransactionCount,
          retainedTransactionCount: job.retainedTransactionCount,
          missingTransactionCount: job.missingTransactionCount,
          physicallyReclaimedBytes: job.reclaimedBlockBytes,
        }
      : null;
  return {
    jobId: job.id,
    state: job.state,
    examinedManifestCount: job.cursor.manifestIndex,
    examinedSegmentCount: job.cursor.segmentIndex,
    examinedBlockCount: job.cursor.blockIndex,
    examinedTransactionCount: job.cursor.transactionIndex,
    result,
  };
}

/** One shared internal reader lease, reused across prepares at the same manifest version. */
interface SharedLeaseEntry {
  lease: LeasedSnapshot;
  version: number | null;
  refCount: number;
}

/** Whether any of the table's triggers for these events fire AFTER the write, staging rows. */
function firesAfterTriggers(
  table: TableRecord,
  ...events: Array<"insert" | "update" | "delete">
): boolean {
  return (table.triggers ?? []).some(
    (trigger) => trigger.timing === "after" && events.includes(trigger.event),
  );
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

/** Retained payload of a cached derived columnar table: its vectors plus fixed overhead. */
function derivedTableRetainedBytes(table: ColumnarTable): number {
  let bytes = 128;
  for (const vector of table.columns.values()) bytes += columnVectorRetainedBytes(vector);
  return bytes;
}

/** Retained payload of a column vector: typed arrays plus dictionary characters. */
function columnVectorRetainedBytes(vector: ColumnVector): number {
  const base = vector.validity.byteLength;
  if (vector.kind === "string") {
    let dictionaryBytes = 0;
    for (const value of vector.dictionary) dictionaryBytes += 16 + value.length * 2;
    return base + vector.codes.byteLength + dictionaryBytes;
  }
  return base + vector.values.byteLength;
}

/**
 * Memoized retained size of a *block* vector. A block vector is built once per block id and
 * then shared by reference across every query that scans it, so its size never changes —
 * but a streamed scan re-reserves that size on every window install, and for a string column
 * the size walks the whole per-block dictionary. On a point lookup that dictionary walk cost
 * more than the lookup itself. Only pool-owned block vectors go through here: the streamed
 * vector they are installed into is a different object, and it is mutated.
 */
const blockVectorRetainedBytesMemo = new WeakMap<ColumnVector, number>();
function blockVectorRetainedBytes(vector: ColumnVector): number {
  const memoized = blockVectorRetainedBytesMemo.get(vector);
  if (memoized !== undefined) return memoized;
  const bytes = columnVectorRetainedBytes(vector);
  blockVectorRetainedBytesMemo.set(vector, bytes);
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

function validLogicalOrder(value: number | undefined): value is number {
  return value !== undefined && Number.isFinite(value) && value >= 0;
}

interface AppendLevelTwoLayout {
  retainedPrefix: readonly SegmentRecord[];
  levelTwoSegments: readonly SegmentRecord[];
  level0Segments: readonly SegmentRecord[];
}

function appendLevelTwoLayout(segments: readonly SegmentRecord[]): AppendLevelTwoLayout | null {
  const levelOneSegments = levelOnePartitionPrefix(segments);
  if (
    levelOneSegments === null ||
    levelOneSegments.some(
      (segment) => (segment.kind ?? "insert") !== "insert" || segment.rowIdSpans !== undefined,
    )
  ) {
    return null;
  }
  let index = levelOneSegments.length;
  const retainedPrefix: SegmentRecord[] = [];
  const levelTwoSegments: SegmentRecord[] = [];
  retainedPrefix.push(...levelOneSegments);
  for (;;) {
    const segment = segments[index];
    if (segment === undefined || (segment.level ?? 0) !== 2) break;
    if (
      (segment.kind ?? "insert") !== "insert" ||
      segment.rowIdSpans !== undefined ||
      segment.partitionOrdinal !== levelTwoSegments.length ||
      !validLogicalOrder(segment.logicalOrder) ||
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
        (segment.logicalOrder !== undefined && !validLogicalOrder(segment.logicalOrder)),
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

interface KeyedLevelOneLayout {
  /** The level-one partitions, in visible order: each a merged base or an append-shaped insert. */
  partitions: readonly SegmentRecord[];
  level0Segments: readonly SegmentRecord[];
}

/**
 * Validates a keyed table's visible history for a partitioned level-one fold: a prefix of
 * level-one partitions — merged bases carrying row-ID spans, or append-shaped inserts — each
 * with an explicit logical order, strictly increasing along the prefix; then level-zero
 * segments of any mutation kind. Every row footprint must be pairwise disjoint. Returns null
 * when the shape does not hold so the planner skips explicitly.
 */
function keyedLevelOneLayout(segments: readonly SegmentRecord[]): KeyedLevelOneLayout | null {
  const partitions = levelOnePartitionPrefix(segments);
  if (partitions === null) return null;
  const level0Segments = segments.slice(partitions.length);
  if (
    level0Segments.some(
      (segment) => (segment.level ?? 0) !== 0 || segment.partitionOrdinal !== undefined,
    )
  ) {
    return null;
  }
  if (!disjointRowIdFootprints(segments)) return null;
  return { partitions, level0Segments };
}

/** The append-only counterpart: bounded L1 partitions followed by contiguous insert deltas. */
function keylessLevelOneLayout(segments: readonly SegmentRecord[]): KeyedLevelOneLayout | null {
  const partitions = levelOnePartitionPrefix(segments);
  if (
    partitions === null ||
    partitions.some((segment) => (segment.kind ?? "insert") !== "insert")
  ) {
    return null;
  }
  const level0Segments = segments.slice(partitions.length);
  if (
    level0Segments.some(
      (segment) =>
        (segment.level ?? 0) !== 0 ||
        segment.partitionOrdinal !== undefined ||
        (segment.kind ?? "insert") !== "insert" ||
        segment.rowIdSpans !== undefined,
    ) ||
    !hasContiguousRowIds(segments)
  ) {
    return null;
  }
  return { partitions, level0Segments };
}

/**
 * The leading level-one segments, when they form a valid partition prefix: insert or base
 * kinds, no L2 ordinal, and explicit strictly increasing logical orders. Null otherwise.
 */
function levelOnePartitionPrefix(segments: readonly SegmentRecord[]): SegmentRecord[] | null {
  const partitions: SegmentRecord[] = [];
  for (const segment of segments) {
    if ((segment.level ?? 0) !== 1) break;
    const kind = segment.kind ?? "insert";
    const previousOrder = partitions[partitions.length - 1]?.logicalOrder ?? -1;
    if (
      (kind !== "insert" && kind !== "base") ||
      segment.partitionOrdinal !== undefined ||
      !validLogicalOrder(segment.logicalOrder) ||
      (segment.logicalOrder ?? -1) <= previousOrder
    ) {
      return null;
    }
    partitions.push(segment);
  }
  return partitions;
}

/**
 * Whether the segments' row footprints — spans where present, otherwise the contiguous
 * interval — are positive and pairwise disjoint. Update and delete deltas carry no footprint.
 */
function disjointRowIdFootprints(segments: readonly SegmentRecord[]): boolean {
  const intervals: Array<{ start: bigint; end: bigint }> = [];
  for (const segment of segments) {
    if (segment.rowIdSpans !== undefined) {
      for (const span of segment.rowIdSpans) {
        intervals.push({ start: span.rowIdStart, end: span.rowIdStart + BigInt(span.rowCount) });
      }
      continue;
    }
    if (segment.rowIdEndExclusive <= segment.rowIdStart) continue;
    if (segment.rowIdEndExclusive - segment.rowIdStart !== BigInt(segment.rowCount)) return false;
    intervals.push({ start: segment.rowIdStart, end: segment.rowIdEndExclusive });
  }
  intervals.sort((left, right) =>
    left.start < right.start ? -1 : left.start > right.start ? 1 : 0,
  );
  for (const [intervalIndex, interval] of intervals.entries()) {
    if (interval.start <= 0n) return false;
    const previous = intervals[intervalIndex - 1];
    if (previous !== undefined && interval.start < previous.end) return false;
  }
  return true;
}

interface KeyedLevelTwoLayout {
  levelTwoSegments: readonly SegmentRecord[];
  /** The level-one partitions a promotion folds along with the level-zero prefix. */
  anchors: readonly SegmentRecord[];
  level0Segments: readonly SegmentRecord[];
}

/**
 * Validates a keyed table's visible history for multi-range L2 promotion: existing partitions
 * (append-shaped inserts or merged bases carrying row-ID spans) with ordinals exactly 0..N-1,
 * then the level-one partitions, then level-zero segments of any mutation kind. Every row
 * footprint — a partition's spans or interval, the anchors', and each level-zero insert/upsert
 * interval — must be pairwise disjoint; update and delete deltas carry no footprint. Returns
 * null when the shape does not hold so the planner skips explicitly.
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
      !validLogicalOrder(segment.logicalOrder) ||
      (kind !== "insert" && kind !== "base") ||
      (kind === "insert" && segment.rowIdSpans !== undefined) ||
      (kind === "base" && (segment.rowIdSpans?.length ?? 0) === 0)
    ) {
      return null;
    }
    levelTwoSegments.push(segment);
    index += 1;
  }
  const anchors = levelOnePartitionPrefix(segments.slice(index));
  if (anchors === null) return null;
  index += anchors.length;
  const level0Segments = segments.slice(index);
  if (
    level0Segments.some(
      (segment) => (segment.level ?? 0) !== 0 || segment.partitionOrdinal !== undefined,
    )
  ) {
    return null;
  }
  if (!disjointRowIdFootprints(segments)) return null;
  return { levelTwoSegments, anchors, level0Segments };
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
