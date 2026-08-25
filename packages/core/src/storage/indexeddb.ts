import {
  type AbortTransactionIfExpiredInput,
  type AdoptAbortedSegmentInput,
  type BeginTransactionInput,
  type BeginTransactionResult,
  type BeginSnapshotFrameExportInput,
  type BeginSnapshotFrameImportInput,
  type BeginPostingBuildInput,
  type RenewPostingBuildInput,
  type AppendPostingBuildChunkInput,
  type FinishPostingBuildInput,
  type AbortPostingBuildInput,
  type ReadSnapshotExportFrameInput,
  type SnapshotFrameExportSession,
  type SnapshotFrameImportSession,
  type RenewSnapshotFrameImportInput,
  type AppendSnapshotImportFramesInput,
  type FinishSnapshotFrameImportInput,
  type SnapshotFrame,
  type SnapshotFrameFooter,
  type SnapshotFrameKind,
  type SnapshotFrameStreamHeader,
  type SnapshotCatalogItem,
  type SnapshotSegmentItem,
  type SnapshotTransactionItem,
  type SnapshotUniqueItem,
  type SnapshotPostingItem,
  type CloseSnapshotExportInput,
  type CancelSnapshotImportInput,
  type CommitTransactionInput,
  type DropTableColumnInput,
  type DropTableInput,
  type CompactionJobRecord,
  CompactionJobConflictError,
  assertCompactionOutputProvenance,
  compactionOutputSegmentIds,
  CompactionBacklogError,
  type CompactionJobRecordUpdate,
  createManifest,
  type CreateGarbageCollectionJobInput,
  createGarbageCollectionJobRecord,
  storeNames,
  advanceGarbageCollectionJobRecord as advanceGarbageCollectionJobRecordUnchecked,
  type BlockStore,
  type CatalogProbe,
  type CatalogMutationOptions,
  BlockReadBatchTooLargeError,
  type UniqueKeyChanges,
  type UniqueKeyBuildRecord,
  type BeginUniqueKeyBuildInput,
  type AppendUniqueKeyBuildChunkInput,
  type FinishUniqueKeyBuildInput,
  type RenewUniqueKeyBuildInput,
  type AbortUniqueKeyBuildInput,
  activePostingStorageColumnIds,
  assertTempRunPageBatchLimits,
  assertStorageBulkReadItems,
  assertTransactionArtifactBatchLimits,
  assertTransactionArtifactJournalLimits,
  boundedMaintenanceBatchItems,
  canonicalManifestChangedTableIds,
  catalogRecordRetainedBytes,
  manifestRecordRetainedReservationBytes,
  segmentRecordRetainedBytes,
  collectFtsPostings,
  ftsPostingQueryMatches,
  invalidateUncoveredFtsColumns,
  invalidateUncoveredSecondaryIndexes,
  type FtsCandidates,
  type FtsPostingQuery,
  type FtsPosting,
  type GarbageCollectionJobRecord,
  GarbageCollectionJobConflictError,
  type GarbageCollectionStepResult,
  type LeaseRecord,
  LeaseConflictError,
  LeaseExpiredError,
  LeaseOwnerConflictError,
  MAX_FTS_CANDIDATE_ROW_IDS,
  MAX_ACTIVE_UNIQUE_KEY_BUILDS,
  MAX_UNIQUE_KEY_BUILD_STAGED_BYTES,
  MAX_UNIQUE_KEY_BUILD_STAGED_BYTES_TOTAL,
  MAX_UNIQUE_KEY_BUILD_CHUNK_BYTES,
  MAX_UNIQUE_KEY_BUILD_TOKENS_PER_CHUNK,
  MAX_UNIQUE_KEY_BUILD_TTL_MS,
  MAX_FTS_BASE_CHUNKS,
  MAX_FTS_DELTA_CHUNKS,
  MAX_FTS_POSTINGS_PER_CHUNK,
  MAX_FTS_POSTING_ROW_IDS_PER_CHUNK,
  MAX_FTS_POSTING_TERM_CHARACTERS,
  MAX_FTS_TOKENS_PER_DOCUMENT,
  MAX_FTS_ORDERED_READ_BYTES,
  MAX_MANIFEST_BLOCK_PRESENCE_IDS,
  MAX_MANIFEST_RECORDS,
  MAX_MANIFEST_RETAINED_BYTES,
  MAX_BLOCK_READ_BATCH_BYTES,
  MAX_STORAGE_ID_CHARACTERS,
  MAX_LEASE_TTL_MS,
  MAX_ACTIVE_LEASES,
  MAX_ACTIVE_TRANSACTIONS,
  MAX_GLOBAL_STAGED_ARTIFACT_BYTES,
  MAX_GLOBAL_STAGED_BLOCKS,
  MAX_GLOBAL_STAGED_SEGMENTS,
  MAX_RETIRED_HISTORY_BYTES,
  MAX_PINNED_MANIFEST_VERSION_LAG,
  MAX_PINNED_RETIRED_BLOCKS,
  MAX_PINNED_RETIRED_BYTES,
  MAX_TERMINAL_TRANSACTION_RECORDS,
  MAX_ACTIVE_COMPACTION_JOBS,
  MAX_ACTIVE_GARBAGE_COLLECTION_JOBS,
  MAX_TERMINAL_COMPACTION_JOB_RECORDS,
  MAX_COMPLETED_GARBAGE_COLLECTION_JOB_RECORDS,
  MAX_CATALOG_RECORDS,
  MAX_CATALOG_RETAINED_BYTES,
  MAX_SEGMENT_RECORDS,
  MAX_SEGMENT_RETAINED_BYTES,
  MAX_ACTIVE_FTS_BASE_BUILDS,
  MAX_ACTIVE_SECONDARY_INDEX_BUILDS,
  MAX_ACCELERATOR_BUILD_STAGED_BYTES_TOTAL,
  MAX_ACCELERATOR_BUILD_STAGED_ENTRIES_TOTAL,
  MAX_POSTING_BUILD_TTL_MS,
  MAX_SNAPSHOT_SESSION_TTL_MS,
  MAX_SNAPSHOT_FRAME_BATCH_BYTES,
  MAX_SNAPSHOT_FRAME_BATCH_ITEMS,
  MAX_SNAPSHOT_METADATA_BATCH_BYTES,
  MAX_SNAPSHOT_METADATA_FRAME_BYTES,
  SNAPSHOT_FRAME_KINDS,
  MAX_LEVEL_ZERO_SEGMENTS,
  MAX_AUTO_INCREMENT_EXCLUSIVE_END,
  MAX_ROW_ID,
  MAX_ROW_ID_EXCLUSIVE_END,
  type Manifest,
  type ManifestBlockRecord,
  type ListManifestBlockPageInput,
  type ListRetiredManifestBlockPageInput,
  type ManifestBlockPage,
  type ManifestSummary,
  type StoredManifestRecord,
  type RenewTransactionInput,
  type RenewLeaseInput,
  type MoveLeaseInput,
  type RowIdRange,
  type RollbackTransactionArtifactsInput,
  type InterruptedSnapshotImport,
  type InterruptedSnapshotImportAbortResult,
  type StorageIntegrityReport,
  type StorageStats,
  StorageCorruptionError,
  StorageFormatVersionError,
  IndexedDbSchemaUpgradeBlockedError,
  StorageResourceLimitError,
  type RunGarbageCollectionStepInput,
  type UpdateGarbageCollectionPlanningInput,
  type SegmentRecord,
  type QueryCatalogState,
  SnapshotManifestMissingError,
  SnapshotImportConflictError,
  PostingBuildConflictError,
  type StageTransactionArtifactsInput,
  type StoragePage,
  type TableRecord,
  type TableRecordUpdate,
  TableInUseError,
  TableRecordConflictError,
  type TempOwnerRecord,
  TempOwnerConflictError,
  type RenewTempOwnerInput,
  MAX_TEMP_OWNER_TTL_MS,
  MAX_ACTIVE_TEMP_OWNERS,
  MAX_TEMP_RUNS_PER_OWNER,
  MAX_TEMP_PAGES_PER_OWNER,
  MAX_TEMP_RUNS_TOTAL,
  MAX_TEMP_PAGES_TOTAL,
  MAX_TEMP_BYTES_PER_OWNER,
  MAX_TEMP_BYTES_TOTAL,
  type TempRunPage,
  type TransactionRecord,
  TransactionRecordConflictError,
  type TransactionRecordUpdate,
  UniqueKeyConflictError,
  UniqueKeyBuildConflictError,
  UniqueIndexCoverageError,
  SchemaConflictError,
  normalizeCompactionJobRecord,
  normalizeGarbageCollectionJobRecord,
  normalizeSegmentRecord,
  updateCompactionJobRecord as updateCompactionJobRecordUnchecked,
  updateGarbageCollectionPlanningRecord,
  updateTransactionRecord as updateTransactionRecordUnchecked,
  validateColumnDefault,
  validateCatalogName,
  validateCanonicalManifestChangedTableIds,
  validateEnumValues,
  validateFtsOrderedReadLimits,
  validateFtsPostingQueries,
  validateStorageId,
  validateStorageDatabaseName,
  validateTableColumns,
  validateSecondaryIndexes,
  validateTableRecordBounds,
  secondaryIndexColumnIds,
  secondaryIndexWriteContractChanged,
  secondaryUniqueKeyNamespace,
  transactionCommitDeltaRetainedBytes,
  uniqueKeyBuildChunkRetainedBytes,
  WriteConflictError,
  type WriteTransactionInput,
} from "./types.js";
import { crc32, verifyStoredBlock } from "../block-format/index.js";
import { dateIsoString } from "../date-value.js";
import {
  decodeSnapshotMetadataItems,
  encodeSnapshotMetadataPage,
  extendSnapshotFrameStreamChecksum,
  prepareSnapshotFrameStreamHeader,
  snapshotFrameEnvelopeParts,
  snapshotFrameStreamHeaderIdentity,
} from "./snapshot-stream.js";

const SCHEMA_VERSION = 1;
const FIRST_STABLE_SCHEMA_VERSION = 1;
const CURRENT_MANIFEST_KEY = "manifest/current";
const MANIFEST_PRUNE_CLEANUP_KEY = "manifest/prune-cleanup";
const CATALOG_EPOCH_KEY = "catalog/epoch";
const SCHEMA_EPOCH_KEY = "catalog/schema-epoch";
const SNAPSHOT_EXPORT_KEY = "snapshot/export/active";
const SNAPSHOT_FRAME_IMPORT_KEY = "snapshot/frame-import";
const SNAPSHOT_FRAME_COMPLETED_KEY = "snapshot/frame-import/completed";
const SNAPSHOT_UNIQUE_STAGING_PREFIX = "snapshot/frame-import/unique/";
const SNAPSHOT_UNIQUE_OWNER_PREFIX = "snapshot/frame-import/unique-owner/";
const SNAPSHOT_POSTING_STAGING_PREFIX = "snapshot/frame-import/posting/";
const SNAPSHOT_POSTING_OWNER_PREFIX = "snapshot/frame-import/posting-owner/";
const SNAPSHOT_HEADER_STORE = "snapshotHeaders";
const indexedDbStoreNames = [...storeNames, SNAPSHOT_HEADER_STORE] as const;
const SEGMENT_TABLE_INDEX = "byTable";
const LEASE_EXPIRY_INDEX = "byExpiry";
const TRANSACTION_STATUS_INDEX = "byStatus";
const TEMP_OWNER_EXPIRY_INDEX = "byOwnerExpiry";
const TEMP_QUOTA = "quota";
const RESOURCE_LEDGER_KEY = "resource/global";
const CATALOG_RESOURCE_LEDGER_KEY = "resource/catalog";
const RECORD_RESOURCE_LEDGER_KEY = "resource/records";
const TRANSACTION_RESOURCE_LEDGER_PREFIX = "resource/transaction/";
const TABLE_ID_PREFIX = "table/id/";
const TABLE_NAME_PREFIX = "table/name/";
const SECONDARY_INDEX_NAME_PREFIX = "secondary-index/name/";
const TRIGGER_NAME_PREFIX = "trigger/name/";
const TRIGGER_ID_PREFIX = "trigger/id/";
const ROW_ID_PREFIX = "row-id/";
const AUTO_INCREMENT_PREFIX = "auto-increment/";
const BLOCK_METADATA_PREFIX = "block-metadata/";
const UNIQUE_KEY_CHUNK_INDEX = "unique-key-chunk-index";
const UNIQUE_KEY_CHUNK = "unique-key-chunk";
const UNIQUE_KEY_BUILD_PREFIX = "unique-key-build/";
const UNIQUE_KEY_BUILD_CHUNK = "unique-key-build-chunk";
const MANIFEST_BLOCK = "manifest-block";
const MANIFEST_BLOCK_ID_INDEX = "byManifestBlockId";
const UNIQUE_KEY_BUILD_ACTIVE_INDEX = "byUniqueKeyBuildActive";
const UNIQUE_KEY_BUILD_EXPIRY_INDEX = "byUniqueKeyBuildExpiry";
const UNIQUE_KEY_BUILD_CLEANUP_PAGE = 64;
/** Tail-version count at which a commit folds the ordered deltas into the lexical base. */
const UNIQUE_KEY_TAIL_CHUNK_LIMIT = 16;
const UNIQUE_KEY_BASE_PART = "unique-key-base-part";
/** Ordered UNIQUE base/tail leaves: at most 17 source leaves plus one output leaf stay live. */
const UNIQUE_KEY_MEMBERSHIP_PART_TOKENS = 2_048;
const UNIQUE_KEY_MEMBERSHIP_PART_RETAINED_BYTES = 2 * 1024 * 1024;
/**
 * Snapshot accelerator parts stay well below the 4 MiB metadata-frame ceiling. The retained
 * model is intentionally conservative, so one complete FTS generation and the canonical merge
 * result remain inside the ordered-read fuse even while both graphs briefly coexist.
 */
const SNAPSHOT_ACCELERATOR_PART_RETAINED_BYTES = 2 * 1024 * 1024;
/**
 * Tokens per folded base partition. Small enough that a keyed point mutation deserializes
 * one partition in about a millisecond; large enough that a multi-million-key table folds into
 * hundreds of records instead of millions.
 */
const FTS_BASE_INDEX_PREFIX = "fts-base-index/";
const FTS_BASE_PREFIX = "fts-base/";
const FTS_BASE_BUILD_PREFIX = "fts-base-build/";
const FTS_RETIREMENT_PREFIX = "fts-retirement/";
const CATALOG_FTS_BUILD_UPDATED_INDEX = "byFtsBuildUpdatedAt";
const CATALOG_FTS_BUILD_EXPIRY_INDEX = "byFtsBuildExpiry";
const CATALOG_FTS_RETIREMENT_UPDATED_INDEX = "byFtsRetirementUpdatedAt";
const FTS_BASE_BUILD_CLEANUP_PAGE = 64;
const FTS_CHUNK_PREFIX = "fts-chunk/";
const COMPACTION_JOB_KEY_PREFIX = "compaction-job/";
const GARBAGE_COLLECTION_JOB_KEY_PREFIX = "garbage-collection-job/";
const ACTIVE_COMPACTION_KEY_PREFIX = "maintenance/active-compaction/";
const ACTIVE_GARBAGE_COLLECTION_KEY = "maintenance/active-garbage-collection";
const MAINTENANCE_QUOTA_KEY = "maintenance/quota";
const storageTextEncoder = new TextEncoder();
const storageTextBuffer = new Uint8Array(1_024);

interface UniqueKeyChunk {
  addedTokens: string[];
  removedTokens: string[];
}

interface UniqueKeyBuildEnvelope {
  kind: "unique-key-build";
  record: UniqueKeyBuildRecord;
  cleanup: boolean;
  activeBuildState?: "active";
  activeExpiry?: [string, string];
  buildId: string;
  expiresAt: string;
}

interface CompactionJobEnvelope {
  kind: "compaction-job";
  record: CompactionJobRecord;
}

interface GarbageCollectionJobEnvelope {
  kind: "garbage-collection-job";
  record: GarbageCollectionJobRecord;
}

interface ActiveCompactionMarker {
  kind: "active-compaction";
  tableId: string;
  jobId: string;
}

interface ActiveGarbageCollectionMarker {
  kind: "active-garbage-collection";
  jobId: string;
}

interface MaintenanceQuotaRecord {
  activeCompactionJobs: number;
  terminalCompactionJobs: number;
  activeGarbageCollectionJobs: number;
  completedGarbageCollectionJobs: number;
}

interface SnapshotFrameExportMarker {
  kind: "snapshot-frame-export";
  sessionId: string;
  ownerId: string;
  manifestVersion: number;
  createdAt: string;
  expiresAt: string;
  revision: number;
  header: SnapshotFrameStreamHeader;
  metadataFrameCount: number;
  nextBlockIndex: number;
  lastBlockId: string | null;
}

interface SnapshotFrameImportMarker {
  kind: "snapshot-frame-import";
  identity: string;
  ownerId: string;
  version: number;
  createdAt: string;
  expiresAt: string;
  header: SnapshotFrameStreamHeader;
  nextSequence: number;
  stagedBytes: number;
  frameCount: number;
  itemCount: number;
  checksum: number;
  kindFrameCounts: number[];
  kindItemCounts: number[];
  kindStoredBytes: number[];
  replayCompleted: boolean;
}

interface CompletedSnapshotFrameImportRecord {
  kind: "snapshot-frame-import-completed";
  identity: string;
  version: number;
  createdAt: string;
  header: SnapshotFrameStreamHeader;
}

interface SnapshotBlockFrameRecord {
  kind: "snapshot-block-frame";
  sequence: number;
  blockId: string;
  byteLength: number;
  checksum: number;
}

interface SnapshotUniqueStagingRecord {
  kind: "snapshot-unique-staging";
  descriptor: Extract<SnapshotUniqueItem, { kind: "unique-generation" }>;
  nextOrdinal: number;
  tokenCount: number;
  lastToken: string | null;
}

interface SnapshotPostingStagingRecord {
  kind: "snapshot-posting-staging";
  descriptor: Extract<SnapshotPostingItem, { kind: "posting-generation" }>;
  nextOrdinal: number;
  totalTokens: number;
  lastTerm: string | null;
  boundaries: Array<{ first: string; last: string }>;
}

interface StoredBlockMetadata {
  byteLength: number;
  checksum: number;
}

interface TempGlobalQuotaRecord {
  ownerCount: number;
  runCount: number;
  pageCount: number;
  retainedBytes: number;
}

interface TempOwnerQuotaRecord {
  runCount: number;
  pageCount: number;
  retainedBytes: number;
}

interface TempRunQuotaRecord {
  pageCount: number;
  retainedBytes: number;
}

interface ResourceLedgerRecord {
  stagedBlockCount: number;
  stagedSegmentCount: number;
  stagedBytes: number;
  retiredHistoryBytes: number;
}

interface TransactionResourceLedgerRecord {
  blockCount: number;
  segmentCount: number;
  retainedBytes: number;
}

interface CatalogResourceLedgerRecord {
  recordCount: number;
  retainedBytes: number;
  checksum: number;
}

interface RecordResourceLedgerRecord {
  manifestCount: number;
  manifestBytes: number;
  segmentCount: number;
  segmentBytes: number;
  checksum: number;
}

type IndexedDbSchemaMigration = Readonly<{
  targetVersion: number;
  migrate(database: IDBDatabase, transaction: IDBTransaction): void;
}>;

/**
 * Release ratchet for the stable native format. Every future schema bump must add exactly one
 * ordered, transactional migration here before SCHEMA_VERSION changes. Keeping this registry
 * explicit prevents an accidental version bump from silently accepting an unmigrated stable
 * database. Schema 1 is the clean first release; there are no pre-contract schemas to support.
 */
const indexedDbSchemaMigrations: readonly IndexedDbSchemaMigration[] = [];

assertIndexedDbSchemaMigrationRegistry();

export interface IndexedDbBlockStoreOptions {
  name: string;
  durability?: IDBTransactionDurability;
  indexedDB?: IDBFactory;
  /**
   * Modeled bytes allowed for the optional complete unique-key membership cache. The cache
   * accelerates bulk loads but is never required for correctness; `0` disables it. Defaults to
   * 8 MiB so a table's row count cannot silently become an unbounded resident-memory cost.
   */
  uniqueKeyCacheBytes?: number;
}

const DEFAULT_UNIQUE_KEY_CACHE_BYTES = 8 * 1024 * 1024;
const TABLE_NAME_CACHE_LIMIT = 256;

export class IndexedDbBlockStore implements BlockStore {
  readonly liveQueryChannelName: string;
  readonly #db: IDBDatabase;
  readonly #durability: IDBTransactionDurability;
  readonly #uniqueKeyCacheBytes: number;
  /** Remembered name-to-id mappings; see getTableByName for why they need no invalidation. */
  readonly #tableIdsByName = new Map<string, string>();
  /**
   * Memoized unique-key state for the most recently written keyed table, mirroring
   * #manifestCache's validity rule: usable only while the next commit's expected manifest
   * version matches, so any commit from another instance or tab invalidates it. `present` is
   * the complete membership (folded base plus tail replay) and `chunks` are the unfolded tail
   * in commit order, so a valid cache lets a commit skip every unique-key read and lets a
   * fold rewrite the base without re-reading it. Bulk loads hit this on every batch.
   */
  #uniqueKeyCache:
    | {
        version: number;
        tableId: string;
        present: Set<string>;
        chunks: UniqueKeyChunk[];
        index: UniqueKeyChunkIndex;
      }
    | undefined;
  /**
   * The most recently resolved manifest block set, advanced in place as this instance commits.
   * Purely a memoization of immutable records: a miss (fresh instance, or another tab moved the
   * version) re-resolves from storage inside the committing transaction.
   */
  #manifestCache: { version: number; blockIds: Set<string> } | undefined;
  #snapshotPeakRetainedItems = 0;
  #snapshotPeakRetainedBytes = 0;
  #closed = false;

  private constructor(
    db: IDBDatabase,
    durability: IDBTransactionDurability,
    uniqueKeyCacheBytes: number,
  ) {
    this.#db = db;
    this.liveQueryChannelName = `minnowdb-live:indexeddb:${db.name}`;
    this.#durability = durability;
    this.#uniqueKeyCacheBytes = uniqueKeyCacheBytes;
    db.addEventListener("versionchange", () => {
      // Never leave a newer deployment blocked behind a forgotten connection in another tab.
      // Closing also makes every later method fail deterministically through #transaction.
      this.close();
    });
  }

  static async open(options: IndexedDbBlockStoreOptions): Promise<IndexedDbBlockStore> {
    validateStorageDatabaseName(options.name);
    const uniqueKeyCacheBytes = options.uniqueKeyCacheBytes ?? DEFAULT_UNIQUE_KEY_CACHE_BYTES;
    if (!Number.isSafeInteger(uniqueKeyCacheBytes) || uniqueKeyCacheBytes < 0) {
      throw new RangeError("Unique-key cache bytes must be a non-negative whole number");
    }
    const factory = options.indexedDB ?? getGlobalIndexedDb();
    if (factory === undefined) throw new Error("IndexedDB is unavailable");
    const request = factory.open(options.name, SCHEMA_VERSION);
    let abandoned = false;
    let upgradeError: Error | undefined;
    request.addEventListener("upgradeneeded", (event) => {
      if (abandoned) {
        request.transaction?.abort();
        return;
      }
      try {
        const upgrade = request.transaction;
        if (upgrade === null) throw new Error("IndexedDB schema transaction is missing");
        if (event.oldVersion === 0) {
          createCurrentIndexedDbSchema(request.result, upgrade);
          return;
        }
        applyIndexedDbSchemaMigrations(request.result, upgrade, event.oldVersion);
      } catch (error) {
        upgradeError = error instanceof Error ? error : new Error(String(error));
        request.transaction?.abort();
      }
    });
    let db: IDBDatabase;
    try {
      db = await openDatabaseRequest(request, (event) => {
        abandoned = true;
        return new IndexedDbSchemaUpgradeBlockedError(
          options.name,
          event.oldVersion,
          event.newVersion ?? SCHEMA_VERSION,
        );
      });
    } catch (error) {
      if (upgradeError !== undefined) throw upgradeError;
      if (isErrorNamed(error, "VersionError")) {
        const actualVersion = await readIndexedDbVersion(factory, options.name);
        throw new StorageFormatVersionError(
          "indexeddb",
          options.name,
          actualVersion,
          SCHEMA_VERSION,
          "newer",
        );
      }
      throw error;
    }
    const store = new IndexedDbBlockStore(db, options.durability ?? "strict", uniqueKeyCacheBytes);
    try {
      await validateCurrentIndexedDbSchema(db);
      await store.#validateCatalogResourceLedger();
      await store.#cleanupInterruptedFtsBaseBuildPage();
      await store.#cleanupFtsRetirementPage();
      await store.#cleanupExpiredUniqueKeyBuildPage(dateIsoString(new Date()));
      return store;
    } catch (error) {
      store.close();
      throw error;
    }
  }

  async getBlock(id: string): Promise<Uint8Array | undefined> {
    validateId(id, "Block ID");
    const transaction = this.#transaction("blocks", "readonly");
    const value: unknown = await requestResult<unknown>(transaction.objectStore("blocks").get(id));
    await transactionDone(transaction);
    return value === undefined ? undefined : asBytes(value, `blocks/${id}`);
  }

  async getBlocks(ids: readonly string[]): Promise<Array<Uint8Array | undefined>> {
    assertStorageBulkReadItems(ids, "Block read");
    for (const id of ids) validateId(id, "Block ID");
    const transaction = this.#transaction(["blocks", "catalog"], "readonly");
    try {
      const blocks = transaction.objectStore("blocks");
      const catalog = transaction.objectStore("catalog");
      const metadataValues = await Promise.all(
        ids.map((id) => requestResult<unknown>(catalog.get(blockMetadataKey(id)))),
      );
      const metadata = metadataValues.map((value, index) =>
        value === undefined ? undefined : asStoredBlockMetadata(value, ids[index] ?? ""),
      );
      const missingMetadataIndexes = metadata.flatMap((value, index) =>
        value === undefined ? [index] : [],
      );
      const missingMetadataKeys = await Promise.all(
        missingMetadataIndexes.map((index) => requestResult(blocks.getKey(ids[index] ?? ""))),
      );
      for (const [offset, key] of missingMetadataKeys.entries()) {
        if (key !== undefined) {
          const index = missingMetadataIndexes[offset] ?? -1;
          throw corruption(blockMetadataKey(ids[index] ?? ""), "block metadata is missing");
        }
      }
      let declaredBytes = 0;
      for (const entry of metadata) {
        declaredBytes += entry?.byteLength ?? 0;
        if (!Number.isSafeInteger(declaredBytes)) {
          throw corruption(BLOCK_METADATA_PREFIX, "block metadata byte total is unsafe");
        }
        if (declaredBytes > MAX_BLOCK_READ_BATCH_BYTES) {
          throw new BlockReadBatchTooLargeError(declaredBytes);
        }
      }
      const values = await Promise.all(
        ids.map((id, index) =>
          metadata[index] === undefined
            ? Promise.resolve(undefined)
            : requestResult<unknown>(blocks.get(id)),
        ),
      );
      const result = values.map((value, index) => {
        const id = ids[index] ?? "";
        const declared = metadata[index];
        if (declared === undefined) return undefined;
        if (value === undefined) throw corruption(`blocks/${id}`, "block payload is missing");
        const bytes = asBytes(value, `blocks/${id}`);
        if (bytes.byteLength !== declared.byteLength) {
          throw corruption(blockMetadataKey(id), "block byte length disagrees with its payload");
        }
        return bytes;
      });
      await transactionDone(transaction);
      return result;
    } catch (error) {
      abortIfActive(transaction);
      await ignoreAbort(transaction);
      throw error;
    }
  }

  async readManifestBlock(version: number | null, id: string): Promise<Uint8Array | undefined> {
    validateManifestMembershipInput(version, [id]);
    if (version === null) return undefined;
    const transaction = this.#transaction(["blocks", "catalog", "manifests"], "readonly");
    try {
      const manifests = transaction.objectStore("manifests");
      const manifestValue: unknown = await requestResult(manifests.get(version));
      if (manifestValue === undefined) {
        await transactionDone(transaction);
        return undefined;
      }
      const manifest = asStoredManifestRecord(manifestValue, version);
      if (manifest.prunedAt !== undefined) {
        await transactionDone(transaction);
        return undefined;
      }
      const [present] = await manifestBlockMembershipInTransaction(
        transaction.objectStore("catalog"),
        manifest.version,
        [id],
      );
      if (present !== true) {
        await transactionDone(transaction);
        return undefined;
      }
      const value: unknown = await requestResult(transaction.objectStore("blocks").get(id));
      if (value === undefined) {
        throw corruption(`blocks/${id}`, `readable manifest ${String(version)} block is missing`);
      }
      const bytes = asBytes(value, `blocks/${id}`);
      await transactionDone(transaction);
      return bytes;
    } catch (error) {
      abortIfActive(transaction);
      await ignoreAbort(transaction);
      throw error;
    }
  }

  async hasManifestBlocks(version: number | null, ids: readonly string[]): Promise<boolean[]> {
    validateManifestMembershipInput(version, ids);
    if (version === null || ids.length === 0) return ids.map(() => false);
    const transaction = this.#transaction(["catalog", "manifests"], "readonly");
    try {
      const manifests = transaction.objectStore("manifests");
      const manifestValue: unknown = await requestResult(manifests.get(version));
      if (manifestValue === undefined) {
        await transactionDone(transaction);
        return ids.map(() => false);
      }
      const manifest = asStoredManifestRecord(manifestValue, version);
      if (manifest.prunedAt !== undefined) {
        await transactionDone(transaction);
        return ids.map(() => false);
      }
      const result = await manifestBlockMembershipInTransaction(
        transaction.objectStore("catalog"),
        manifest.version,
        ids,
      );
      await transactionDone(transaction);
      return result;
    } catch (error) {
      abortIfActive(transaction);
      await ignoreAbort(transaction);
      throw error;
    }
  }

  async listManifestBlockPage(input: ListManifestBlockPageInput): Promise<ManifestBlockPage> {
    if (!Number.isSafeInteger(input.version) || input.version < 0) {
      throw new RangeError("Manifest block page version must be a non-negative safe integer");
    }
    if (input.afterBlockId !== null) validateId(input.afterBlockId, "Manifest block page cursor");
    validatePageLimit(input.limit);
    const transaction = this.#transaction("catalog", "readonly");
    try {
      const catalog = transaction.objectStore("catalog");
      const currentVersion = asOptionalManifestVersion(
        await requestResult<unknown>(catalog.get(CURRENT_MANIFEST_KEY)),
        CURRENT_MANIFEST_KEY,
      );
      if (currentVersion === undefined || input.version > currentVersion) {
        await transactionDone(transaction);
        return { records: [], nextCursor: null };
      }
      const records = await readManifestBlockPageFromCatalog(
        catalog,
        input.version,
        input.afterBlockId,
        input.limit + 1,
      );
      await transactionDone(transaction);
      const page = records.slice(0, input.limit);
      return {
        records: page.map(({ blockId, byteLength, checksum }) => ({
          blockId,
          byteLength,
          checksum,
        })),
        nextCursor: records.length > input.limit ? (page.at(-1)?.blockId ?? null) : null,
      };
    } catch (error) {
      abortIfActive(transaction);
      await ignoreAbort(transaction);
      throw error;
    }
  }

  async listRetiredManifestBlockPage(
    input: ListRetiredManifestBlockPageInput,
  ): Promise<ManifestBlockPage> {
    if (!Number.isSafeInteger(input.removedThroughVersion) || input.removedThroughVersion < 0) {
      throw new RangeError("Retired manifest block version must be a non-negative safe integer");
    }
    if (input.afterBlockId !== null) validateId(input.afterBlockId, "Retired block page cursor");
    validatePageLimit(input.limit);
    const transaction = this.#transaction("catalog", "readonly");
    try {
      const records = await readRetiredManifestBlockPageFromCatalog(
        transaction.objectStore("catalog"),
        input.removedThroughVersion,
        input.afterBlockId,
        input.limit + 1,
      );
      await transactionDone(transaction);
      const page = records.slice(0, input.limit);
      return {
        records: page.map(({ blockId, byteLength, checksum }) => ({
          blockId,
          byteLength,
          checksum,
        })),
        nextCursor: records.length > input.limit ? (page.at(-1)?.blockId ?? null) : null,
      };
    } catch (error) {
      abortIfActive(transaction);
      await ignoreAbort(transaction);
      throw error;
    }
  }

  async putTempRunPage(page: TempRunPage): Promise<void> {
    await this.putTempRunPages([page]);
  }

  async putTempRunPages(pages: readonly TempRunPage[]): Promise<void> {
    assertTempRunPageBatchLimits(pages);
    for (const page of pages) validateTempRunPage(page);
    if (pages.length === 0) return;
    // One transaction for the whole batch — the per-page path pays one commit each.
    const transaction = this.#transaction("temp", "readwrite");
    try {
      const store = transaction.objectStore("temp");
      await putTempRunPagesWithQuota(store, pages);
      await transactionDone(transaction);
    } catch (error) {
      abortIfActive(transaction);
      await ignoreAbort(transaction);
      throw error;
    }
  }

  async getTempRunPage(
    ownerId: string,
    runId: string,
    pageIndex: number,
  ): Promise<Uint8Array | undefined> {
    validateTempRunPageIdentity(ownerId, runId, pageIndex);
    const transaction = this.#transaction("temp", "readonly");
    const value: unknown = await requestResult(
      transaction.objectStore("temp").get(["run", ownerId, runId, pageIndex]),
    );
    await transactionDone(transaction);
    return value === undefined
      ? undefined
      : asBytes(value, `temp/${ownerId}/${runId}/${String(pageIndex)}`).slice();
  }

  async removeTempRun(ownerId: string, runId: string): Promise<void> {
    validateTempRunPageIdentity(ownerId, runId, 0);
    const transaction = this.#transaction("temp", "readwrite");
    try {
      const store = transaction.objectStore("temp");
      await removeTempRunWithQuota(store, ownerId, runId);
      await transactionDone(transaction);
    } catch (error) {
      abortIfActive(transaction);
      await ignoreAbort(transaction);
      throw error;
    }
  }

  async removeTempOwner(ownerId: string): Promise<void> {
    validateTempId(ownerId, "Temp run owner ID");
    const transaction = this.#transaction("temp", "readwrite");
    try {
      const store = transaction.objectStore("temp");
      await removeTempOwnerWithQuota(store, ownerId);
      await transactionDone(transaction);
    } catch (error) {
      abortIfActive(transaction);
      await ignoreAbort(transaction);
      throw error;
    }
  }

  async createTempOwner(record: TempOwnerRecord): Promise<void> {
    validateTempOwnerRecord(record);
    const transaction = this.#transaction("temp", "readwrite");
    try {
      const store = transaction.objectStore("temp");
      const existing = await requestResult(store.getKey(tempOwnerKey(record.ownerId)));
      if (existing !== undefined) throw new Error(`Temp owner already exists: ${record.ownerId}`);
      const global = asTempGlobalQuotaRecord(
        await requestResult<unknown>(store.get(tempGlobalQuotaKey())),
      );
      const ownerCount = incrementSafeInteger(global.ownerCount, "Active temp owner count");
      if (ownerCount > MAX_ACTIVE_TEMP_OWNERS) {
        throw new StorageResourceLimitError("temp owner", ownerCount, MAX_ACTIVE_TEMP_OWNERS);
      }
      store.add(structuredClone(record), tempOwnerKey(record.ownerId));
      store.add(emptyTempOwnerQuota(), tempOwnerQuotaKey(record.ownerId));
      store.put({ ...global, ownerCount }, tempGlobalQuotaKey());
      await transactionDone(transaction);
    } catch (error) {
      abortIfActive(transaction);
      await ignoreAbort(transaction);
      throw error;
    }
  }

  async getTempOwner(ownerId: string): Promise<TempOwnerRecord | undefined> {
    validateTempId(ownerId, "Temp run owner ID");
    const transaction = this.#transaction("temp", "readonly");
    const value: unknown = await requestResult(
      transaction.objectStore("temp").get(tempOwnerKey(ownerId)),
    );
    await transactionDone(transaction);
    return value === undefined ? undefined : asTempOwnerRecord(value);
  }

  async renewTempOwner(input: RenewTempOwnerInput): Promise<TempOwnerRecord> {
    validateTempId(input.ownerId, "Temp run owner ID");
    const cutoff = validateBoundedExpiration(
      input.expiresAtCutoff,
      input.expiresAt,
      "Temp owner",
      MAX_TEMP_OWNER_TTL_MS,
    );
    const transaction = this.#transaction("temp", "readwrite");
    const store = transaction.objectStore("temp");
    const value: unknown = await requestResult(store.get(tempOwnerKey(input.ownerId)));
    const record = value === undefined ? undefined : asTempOwnerRecord(value);
    if (record?.revision !== input.expectedRevision) {
      transaction.abort();
      await ignoreAbort(transaction);
      throw new TempOwnerConflictError(
        input.ownerId,
        input.expectedRevision,
        record?.revision ?? null,
      );
    }
    if (Date.parse(record.expiresAt) <= cutoff) {
      transaction.abort();
      await ignoreAbort(transaction);
      throw new Error(`Temp owner ${input.ownerId} is expired`);
    }
    const renewed = {
      ...record,
      expiresAt: input.expiresAt,
      revision: incrementSafeInteger(record.revision, "Temp owner revision"),
    };
    store.put(renewed, tempOwnerKey(input.ownerId));
    await transactionDone(transaction);
    return structuredClone(renewed);
  }

  async removeTempOwnerIfExpired(ownerId: string, expiresAtCutoff: string): Promise<boolean> {
    validateTempId(ownerId, "Temp run owner ID");
    const cutoff = Date.parse(expiresAtCutoff);
    if (!Number.isFinite(cutoff)) throw new TypeError("Temp owner expiry cutoff must be valid");
    const transaction = this.#transaction("temp", "readwrite");
    const store = transaction.objectStore("temp");
    const value: unknown = await requestResult(store.get(tempOwnerKey(ownerId)));
    if (value !== undefined) {
      const record = asTempOwnerRecord(value);
      const expiresAt = Date.parse(record.expiresAt);
      if (Number.isFinite(expiresAt) && expiresAt > cutoff) {
        await transactionDone(transaction);
        return false;
      }
    }
    await removeTempOwnerWithQuota(store, ownerId);
    await transactionDone(transaction);
    return true;
  }

  async listTempOwnerIdsPage(
    afterOwnerId: string | null,
    limit: number,
  ): Promise<StoragePage<string, string>> {
    validatePageLimit(limit);
    const transaction = this.#transaction("temp", "readonly");
    const store = transaction.objectStore("temp");
    const [recordOwnerIds, pageOwnerIds] = await Promise.all([
      readTempOwnerRecordIds(store, afterOwnerId, limit + 1),
      readTempPageOwnerIds(store, afterOwnerId, limit + 1),
    ]);
    await transactionDone(transaction);
    const union = [...new Set([...recordOwnerIds, ...pageOwnerIds])].sort();
    const records = union.slice(0, limit);
    return {
      records,
      nextCursor: union.length > limit ? (records.at(-1) ?? null) : null,
    };
  }

  async listExpiredTempOwnerPage(
    expiresAtCutoff: string,
    afterCursor: string | null,
    limit: number,
  ): Promise<StoragePage<string, string>> {
    validatePageLimit(limit);
    const cutoff = canonicalInputTimestamp(expiresAtCutoff, "Temp owner expiry cutoff");
    const after = decodeExpiryPageCursor(afterCursor, "Temp owner page");
    const transaction = this.#transaction("temp", "readonly");
    const owners = await readExpiredTempOwnerPage(
      transaction.objectStore("temp").index(TEMP_OWNER_EXPIRY_INDEX),
      cutoff,
      after,
      limit,
    );
    await transactionDone(transaction);
    return {
      records: owners.map((record) => record.ownerId),
      nextCursor:
        owners.length === limit
          ? encodeExpiryPageCursor(owners.at(-1)?.expiresAt ?? "", owners.at(-1)?.ownerId ?? "")
          : null,
    };
  }

  async addTable(record: TableRecord, options: CatalogMutationOptions = {}): Promise<void> {
    validateTableRecordBounds(record);
    const normalizedRecord = asIncomingTableRecord(
      structuredClone(record),
      `${TABLE_ID_PREFIX}${record.id}`,
    );
    const transaction = this.#transaction(["catalog", "statistics", "transactions"], "readwrite");
    try {
      const store = transaction.objectStore("catalog");
      await assertExpectedCatalogEpoch(
        store,
        normalizedRecord.id,
        normalizedRecord.revision,
        options.expectedCatalogEpoch,
      );
      const idKey = `${TABLE_ID_PREFIX}${normalizedRecord.id}`;
      const nameKey = `${TABLE_NAME_PREFIX}${normalizedRecord.name}`;
      const [existingId, existingName] = await Promise.all([
        requestResult(store.getKey(idKey)),
        requestResult(store.getKey(nameKey)),
      ]);
      if (existingId !== undefined || existingName !== undefined) {
        throw new Error(
          existingId !== undefined
            ? `Table already exists: ${record.id}`
            : `Table name already exists: ${record.name}`,
        );
      }
      await assertCatalogReservationAdmission(transaction, normalizedRecord);
      await assertTableForeignKeysInTransaction(store, normalizedRecord);
      await updateCatalogResourceLedger(
        transaction.objectStore("statistics"),
        undefined,
        normalizedRecord,
      );
      const indexNames = new Set<string>();
      for (const [indexId, index] of Object.entries(normalizedRecord.secondaryIndexes ?? {})) {
        const markerKey = `${SECONDARY_INDEX_NAME_PREFIX}${index.name}`;
        const marker = asOptionalSecondaryIndexNameMarker(
          await requestResult<unknown>(store.get(markerKey)),
          markerKey,
        );
        if (indexNames.has(index.name) || marker !== undefined) {
          throw new TypeError(`Index already exists: ${index.name}`);
        }
        indexNames.add(index.name);
        store.put({ tableId: normalizedRecord.id, indexId }, markerKey);
        if (index.uniqueEnforced === true) {
          store.put(
            { versions: [], hasBase: false } satisfies UniqueKeyChunkIndex,
            uniqueKeyChunkIndexKey(secondaryUniqueKeyNamespace(normalizedRecord.id, indexId)),
          );
        }
      }
      for (const trigger of normalizedRecord.triggers ?? []) {
        const nameKey = `${TRIGGER_NAME_PREFIX}${trigger.name}`;
        const idKey = `${TRIGGER_ID_PREFIX}${trigger.id}`;
        const [nameMarker, idMarker] = await Promise.all([
          requestResult<unknown>(store.get(nameKey)).then((value) =>
            asOptionalTriggerNameMarker(value, nameKey),
          ),
          requestResult<unknown>(store.get(idKey)).then((value) =>
            asOptionalTriggerIdMarker(value, idKey),
          ),
        ]);
        if (nameMarker !== undefined)
          throw new TypeError(`Trigger already exists: ${trigger.name}`);
        if (idMarker !== undefined) throw new TypeError(`Trigger ID already exists: ${trigger.id}`);
        store.put({ tableId: normalizedRecord.id, triggerId: trigger.id }, nameKey);
        store.put({ tableId: normalizedRecord.id, triggerName: trigger.name }, idKey);
      }
      // A declared primary/unique key owns a durable membership namespace from table creation.
      // Consequently, a missing index record can always be treated as corruption on the write hot
      // path; there is no ambiguous "brand-new and empty" state that could admit a duplicate.
      if (normalizedRecord.uniqueKeyColumnId !== undefined) {
        store.put(
          { versions: [], hasBase: false } satisfies UniqueKeyChunkIndex,
          uniqueKeyChunkIndexKey(normalizedRecord.id),
        );
      }
      store.add(normalizedRecord, idKey);
      store.add(normalizedRecord.id, nameKey);
      await bumpCatalogEpoch(store);
      await bumpSchemaEpoch(store);
      await transactionDone(transaction);
    } catch (error) {
      abortIfActive(transaction);
      await ignoreAbort(transaction);
      throw error;
    }
  }

  async getTable(id: string): Promise<TableRecord | undefined> {
    validateId(id, "Table ID");
    const transaction = this.#transaction("catalog", "readonly");
    const value: unknown = await requestResult(
      transaction.objectStore("catalog").get(`${TABLE_ID_PREFIX}${id}`),
    );
    await transactionDone(transaction);
    return value === undefined ? undefined : asTableRecord(value, `${TABLE_ID_PREFIX}${id}`);
  }

  async updateTable(
    id: string,
    expectedRevision: number,
    update: TableRecordUpdate,
  ): Promise<TableRecord> {
    validateId(id, "Table ID");
    if (update.columns !== undefined) validateTableColumns(update.columns);
    if (update.autoIncrementSeed !== undefined) {
      validateId(update.autoIncrementSeed.columnId, "Auto-increment seed column ID");
      validateAutoIncrementReservation(0, update.autoIncrementSeed.atLeast);
    }
    const transaction = this.#transaction(["catalog", "statistics"], "readwrite");
    try {
      const store = transaction.objectStore("catalog");
      const idKey = `${TABLE_ID_PREFIX}${id}`;
      const value: unknown = await requestResult(store.get(idKey));
      const record = value === undefined ? undefined : asTableRecord(value, idKey);
      const actualRevision = record === undefined ? null : record.revision;
      if (record === undefined || actualRevision !== expectedRevision) {
        throw new TableRecordConflictError(id, expectedRevision, actualRevision);
      }
      await assertExpectedCatalogEpoch(store, id, expectedRevision, update.expectedCatalogEpoch);
      const nextRevision = incrementSafeInteger(expectedRevision, "Table revision");
      if (update.expectedManifestVersion !== undefined) {
        const manifestValue: unknown = await requestResult(store.get(CURRENT_MANIFEST_KEY));
        const actualManifest =
          asOptionalManifestVersion(manifestValue, CURRENT_MANIFEST_KEY) ?? null;
        if (actualManifest !== update.expectedManifestVersion.value) {
          throw new WriteConflictError(update.expectedManifestVersion.value, actualManifest);
        }
      }
      if (update.uniqueKeySeed !== undefined) {
        const seen = new Set<string>();
        for (const token of update.uniqueKeySeed.keyTokens) {
          if (seen.has(token)) {
            throw new UniqueKeyConflictError(update.uniqueKeySeed.namespaceId, token);
          }
          seen.add(token);
        }
      }
      const {
        ftsColumns: previousFts,
        secondaryIndexes: previousSecondary,
        triggers: previousTriggers,
        view: previousView,
        ...base
      } = record;
      let nextFts = update.ftsColumns === undefined ? previousFts : update.ftsColumns;
      let nextSecondary =
        update.secondaryIndexes === undefined ? previousSecondary : update.secondaryIndexes;
      const retainedColumnIds =
        update.columns === undefined
          ? undefined
          : new Set(update.columns.map(({ id: columnId }) => columnId));
      if (nextFts !== null && nextFts !== undefined && retainedColumnIds !== undefined) {
        nextFts = Object.fromEntries(
          Object.entries(nextFts).filter(([columnId]) => retainedColumnIds.has(columnId)),
        );
        if (Object.keys(nextFts).length === 0) nextFts = null;
      }
      if (
        nextSecondary !== null &&
        nextSecondary !== undefined &&
        retainedColumnIds !== undefined
      ) {
        nextSecondary = Object.fromEntries(
          Object.entries(nextSecondary).filter(([, index]) =>
            secondaryIndexColumnIds(index).every((columnId) => retainedColumnIds.has(columnId)),
          ),
        );
        if (Object.keys(nextSecondary).length === 0) nextSecondary = null;
      }
      const indexNames = new Set<string>();
      for (const [indexId, index] of Object.entries(nextSecondary ?? {})) {
        const markerKey = `${SECONDARY_INDEX_NAME_PREFIX}${index.name}`;
        const marker = asOptionalSecondaryIndexNameMarker(
          await requestResult<unknown>(store.get(markerKey)),
          markerKey,
        );
        if (
          indexNames.has(index.name) ||
          (marker !== undefined && (marker.tableId !== id || marker.indexId !== indexId))
        ) {
          throw new TypeError(`Index already exists: ${index.name}`);
        }
        indexNames.add(index.name);
      }
      const nextTriggers = update.triggers === undefined ? previousTriggers : update.triggers;
      for (const trigger of nextTriggers ?? []) {
        const nameKey = `${TRIGGER_NAME_PREFIX}${trigger.name}`;
        const idKey = `${TRIGGER_ID_PREFIX}${trigger.id}`;
        const [nameMarker, idMarker] = await Promise.all([
          requestResult<unknown>(store.get(nameKey)).then((value) =>
            asOptionalTriggerNameMarker(value, nameKey),
          ),
          requestResult<unknown>(store.get(idKey)).then((value) =>
            asOptionalTriggerIdMarker(value, idKey),
          ),
        ]);
        if (
          nameMarker !== undefined &&
          (nameMarker.tableId !== id || nameMarker.triggerId !== trigger.id)
        ) {
          throw new TypeError(`Trigger already exists: ${trigger.name}`);
        }
        if (
          idMarker !== undefined &&
          (idMarker.tableId !== id || idMarker.triggerName !== trigger.name)
        ) {
          throw new TypeError(`Trigger ID already exists: ${trigger.id}`);
        }
      }
      const nextView = update.view === undefined ? previousView : update.view;
      const updated: TableRecord = {
        ...base,
        columns: update.columns === undefined ? record.columns : structuredClone(update.columns),
        ...(nextFts === null || nextFts === undefined
          ? {}
          : { ftsColumns: structuredClone(nextFts) }),
        ...(nextSecondary === null || nextSecondary === undefined
          ? {}
          : { secondaryIndexes: structuredClone(nextSecondary) }),
        ...(nextTriggers === null || nextTriggers === undefined
          ? {}
          : { triggers: structuredClone(nextTriggers) }),
        ...(nextView === null || nextView === undefined ? {} : { view: structuredClone(nextView) }),
        revision: nextRevision,
      };
      asIncomingTableRecord(updated, idKey);
      await updateCatalogResourceLedger(transaction.objectStore("statistics"), record, updated);
      for (const [indexId, index] of Object.entries(nextSecondary ?? {})) {
        store.put({ tableId: id, indexId }, `${SECONDARY_INDEX_NAME_PREFIX}${index.name}`);
      }
      for (const [indexId, index] of Object.entries(previousSecondary ?? {})) {
        const retained = nextSecondary?.[indexId];
        if (retained?.name === index.name) continue;
        const markerKey = `${SECONDARY_INDEX_NAME_PREFIX}${index.name}`;
        const marker = asOptionalSecondaryIndexNameMarker(
          await requestResult<unknown>(store.get(markerKey)),
          markerKey,
        );
        if (marker?.tableId === id && marker.indexId === indexId) store.delete(markerKey);
      }
      for (const trigger of nextTriggers ?? []) {
        store.put({ tableId: id, triggerId: trigger.id }, `${TRIGGER_NAME_PREFIX}${trigger.name}`);
        store.put({ tableId: id, triggerName: trigger.name }, `${TRIGGER_ID_PREFIX}${trigger.id}`);
      }
      for (const trigger of previousTriggers ?? []) {
        if (
          (nextTriggers ?? []).some(
            (candidate) => candidate.id === trigger.id && candidate.name === trigger.name,
          )
        ) {
          continue;
        }
        const nameKey = `${TRIGGER_NAME_PREFIX}${trigger.name}`;
        const idKey = `${TRIGGER_ID_PREFIX}${trigger.id}`;
        const [nameMarker, idMarker] = await Promise.all([
          requestResult<unknown>(store.get(nameKey)).then((value) =>
            asOptionalTriggerNameMarker(value, nameKey),
          ),
          requestResult<unknown>(store.get(idKey)).then((value) =>
            asOptionalTriggerIdMarker(value, idKey),
          ),
        ]);
        if (nameMarker?.tableId === id && nameMarker.triggerId === trigger.id) {
          store.delete(nameKey);
        }
        if (idMarker?.tableId === id && idMarker.triggerName === trigger.name) {
          store.delete(idKey);
        }
      }
      if (update.autoIncrementSeed !== undefined) {
        const { columnId, atLeast } = update.autoIncrementSeed;
        const column = updated.columns.find((candidate) => candidate.id === columnId);
        if (column?.defaultValue?.kind !== "autoincrement") {
          throw new TypeError(
            `Auto-increment seed has no declared column: ${updated.id}/${columnId}`,
          );
        }
        const counterKey = `${AUTO_INCREMENT_PREFIX}${updated.id}/${columnId}`;
        const current = asOptionalCounter(
          await requestResult<unknown>(store.get(counterKey)),
          counterKey,
        );
        store.put(maxBigInt(current ?? 1n, atLeast), counterKey);
      }
      if (update.uniqueKeySeed !== undefined) {
        const ownsSeed = Object.entries(updated.secondaryIndexes ?? {}).some(
          ([indexId, index]) =>
            index.unique === true &&
            index.uniqueEnforced === true &&
            index.state === "ready" &&
            secondaryUniqueKeyNamespace(updated.id, indexId) === update.uniqueKeySeed?.namespaceId,
        );
        if (!ownsSeed) {
          throw new TypeError("UNIQUE-index seed does not belong to a ready catalog index");
        }
      }
      for (const [indexId, index] of Object.entries(updated.secondaryIndexes ?? {})) {
        if (index.uniqueEnforced !== true) continue;
        const namespaceId = secondaryUniqueKeyNamespace(updated.id, indexId);
        if (update.uniqueKeySeed?.namespaceId === namespaceId) continue;
        const indexValue: unknown = await requestResult(
          store.get(uniqueKeyChunkIndexKey(namespaceId)),
        );
        if (indexValue === undefined) {
          throw corruption(
            `${UNIQUE_KEY_CHUNK_INDEX}/${namespaceId}`,
            "enforced UNIQUE index has no membership state",
          );
        }
        asUniqueKeyChunkIndex(indexValue);
      }
      if (retainedColumnIds !== undefined) {
        for (const column of record.columns) {
          if (!retainedColumnIds.has(column.id)) {
            await deleteFtsColumnRecords(store, record.id, column.id);
          }
        }
      }
      const retainedIndexStorage = new Set(
        Object.values(nextSecondary ?? {}).map((index) => index.storageColumnId),
      );
      for (const index of Object.values(previousSecondary ?? {})) {
        if (!retainedIndexStorage.has(index.storageColumnId)) {
          await deleteFtsColumnRecords(store, record.id, index.storageColumnId);
        }
      }
      for (const [indexId, previous] of Object.entries(previousSecondary ?? {})) {
        if (previous.unique === true && nextSecondary?.[indexId]?.unique !== true) {
          await replaceUniqueKeyMembership(
            store,
            secondaryUniqueKeyNamespace(record.id, indexId),
            [],
            false,
          );
        }
      }
      if (update.uniqueKeySeed !== undefined) {
        await replaceUniqueKeyMembership(
          store,
          update.uniqueKeySeed.namespaceId,
          update.uniqueKeySeed.keyTokens,
          true,
        );
      }
      store.put(structuredClone(updated), idKey);
      await bumpCatalogEpoch(store);
      if (
        update.columns !== undefined ||
        secondaryIndexWriteContractChanged(previousSecondary, nextSecondary) ||
        update.triggers !== undefined ||
        update.view !== undefined
      ) {
        await bumpSchemaEpoch(store);
      }
      await transactionDone(transaction);
      if (
        update.uniqueKeySeed !== undefined ||
        Object.entries(previousSecondary ?? {}).some(
          ([indexId, previous]) =>
            previous.unique === true && nextSecondary?.[indexId]?.unique !== true,
        )
      ) {
        this.#uniqueKeyCache = undefined;
      }
      return updated;
    } catch (error) {
      abortIfActive(transaction);
      await ignoreAbort(transaction);
      throw error;
    }
  }

  async beginUniqueKeyBuild(input: BeginUniqueKeyBuildInput): Promise<UniqueKeyBuildRecord> {
    validateUniqueKeyBuildIdentity(input);
    const createdAt = canonicalInputTimestamp(input.createdAt, "UNIQUE build creation time");
    const expiresAt = canonicalInputTimestamp(input.expiresAt, "UNIQUE build expiry");
    if (Date.parse(expiresAt) - Date.parse(createdAt) > MAX_UNIQUE_KEY_BUILD_TTL_MS) {
      throw new RangeError("UNIQUE build expiry exceeds its maximum lifetime");
    }
    await this.#cleanupUniqueKeyBuildIfExpired(input.buildId, createdAt);
    await this.#cleanupExpiredUniqueKeyBuildPage(createdAt);
    const transaction = this.#transaction("catalog", "readwrite");
    try {
      const catalog = transaction.objectStore("catalog");
      const key = uniqueKeyBuildKey(input.buildId);
      const existingValue: unknown = await requestResult(catalog.get(key));
      if (existingValue !== undefined) {
        const existing = asUniqueKeyBuildEnvelope(existingValue, key);
        if (
          !existing.cleanup &&
          existing.record.state === "active" &&
          existing.record.ownerId === input.ownerId &&
          existing.record.tableId === input.tableId &&
          existing.record.indexId === input.indexId &&
          existing.record.namespaceId === input.namespaceId &&
          existing.record.createdAt === createdAt &&
          existing.record.expiresAt === expiresAt
        ) {
          await transactionDone(transaction);
          return structuredClone(existing.record);
        }
        throw new UniqueKeyBuildConflictError(input.buildId, "another durable build exists");
      }
      const tableKey = `${TABLE_ID_PREFIX}${input.tableId}`;
      const tableValue: unknown = await requestResult(catalog.get(tableKey));
      const table = tableValue === undefined ? undefined : asTableRecord(tableValue, tableKey);
      const index = table?.secondaryIndexes?.[input.indexId];
      if (
        index?.unique !== true ||
        index.state !== "building" ||
        index.buildId !== input.buildId ||
        secondaryUniqueKeyNamespace(input.tableId, input.indexId) !== input.namespaceId
      ) {
        throw new UniqueKeyBuildConflictError(input.buildId, "catalog ownership changed");
      }
      const admission = await readUniqueKeyBuildAdmission(catalog);
      if (admission.activeBuilds >= MAX_ACTIVE_UNIQUE_KEY_BUILDS) {
        throw new StorageResourceLimitError(
          "unique-key build",
          admission.activeBuilds + 1,
          MAX_ACTIVE_UNIQUE_KEY_BUILDS,
        );
      }
      const record: UniqueKeyBuildRecord = {
        buildId: input.buildId,
        tableId: input.tableId,
        indexId: input.indexId,
        namespaceId: input.namespaceId,
        ownerId: input.ownerId,
        state: "active",
        nextOrdinal: 0,
        tokenCount: 0,
        retainedBytes: 0,
        expiresAt,
        createdAt,
        updatedAt: createdAt,
      };
      catalog.add(uniqueKeyBuildEnvelope(record), key);
      await transactionDone(transaction);
      return structuredClone(record);
    } catch (error) {
      abortIfActive(transaction);
      await ignoreAbort(transaction);
      throw error;
    }
  }

  async getUniqueKeyBuild(buildId: string): Promise<UniqueKeyBuildRecord | undefined> {
    validateId(buildId, "UNIQUE build ID");
    const transaction = this.#transaction("catalog", "readonly");
    const key = uniqueKeyBuildKey(buildId);
    const value: unknown = await requestResult(transaction.objectStore("catalog").get(key));
    await transactionDone(transaction);
    return value === undefined
      ? undefined
      : structuredClone(asUniqueKeyBuildEnvelope(value, key).record);
  }

  async renewUniqueKeyBuild(input: RenewUniqueKeyBuildInput): Promise<UniqueKeyBuildRecord> {
    validateId(input.buildId, "UNIQUE build ID");
    validateId(input.ownerId, "UNIQUE build owner ID");
    const cutoff = canonicalInputTimestamp(input.expiresAtCutoff, "UNIQUE build expiry cutoff");
    const expiresAt = canonicalInputTimestamp(input.expiresAt, "UNIQUE build expiry");
    const updatedAt = canonicalInputTimestamp(input.updatedAt, "UNIQUE build update time");
    if (
      Date.parse(expiresAt) <= Date.parse(cutoff) ||
      Date.parse(expiresAt) - Date.parse(cutoff) > MAX_UNIQUE_KEY_BUILD_TTL_MS
    ) {
      throw new RangeError("UNIQUE build renewal expiry is outside its bounded range");
    }
    const transaction = this.#transaction("catalog", "readwrite");
    try {
      const catalog = transaction.objectStore("catalog");
      const key = uniqueKeyBuildKey(input.buildId);
      const value: unknown = await requestResult(catalog.get(key));
      const envelope = value === undefined ? undefined : asUniqueKeyBuildEnvelope(value, key);
      if (
        envelope?.record.state !== "active" ||
        envelope.cleanup ||
        envelope.record.ownerId !== input.ownerId ||
        Date.parse(envelope.record.expiresAt) <= Date.parse(cutoff)
      ) {
        throw new UniqueKeyBuildConflictError(input.buildId, "ownership is absent or expired");
      }
      const record = { ...envelope.record, expiresAt, updatedAt };
      catalog.put(uniqueKeyBuildEnvelope(record), key);
      await transactionDone(transaction);
      return structuredClone(record);
    } catch (error) {
      abortIfActive(transaction);
      await ignoreAbort(transaction);
      throw error;
    }
  }

  async appendUniqueKeyBuildChunk(
    input: AppendUniqueKeyBuildChunkInput,
  ): Promise<UniqueKeyBuildRecord> {
    validateId(input.buildId, "UNIQUE build ID");
    validateId(input.ownerId, "UNIQUE build owner ID");
    if (!Number.isSafeInteger(input.ordinal) || input.ordinal < 0) {
      throw new RangeError("UNIQUE build ordinal must be a non-negative safe integer");
    }
    const retainedBytes = uniqueKeyBuildChunkRetainedBytes(input.keyTokens);
    const cutoff = canonicalInputTimestamp(input.expiresAtCutoff, "UNIQUE build expiry cutoff");
    const updatedAt = canonicalInputTimestamp(input.updatedAt, "UNIQUE build update time");
    const transaction = this.#transaction("catalog", "readwrite");
    try {
      const catalog = transaction.objectStore("catalog");
      const key = uniqueKeyBuildKey(input.buildId);
      const value: unknown = await requestResult(catalog.get(key));
      const envelope = value === undefined ? undefined : asUniqueKeyBuildEnvelope(value, key);
      if (
        envelope?.record.state !== "active" ||
        envelope.cleanup ||
        envelope.record.ownerId !== input.ownerId ||
        Date.parse(envelope.record.expiresAt) <= Date.parse(cutoff)
      ) {
        throw new UniqueKeyBuildConflictError(input.buildId, "ownership is absent or expired");
      }
      if (input.ordinal < envelope.record.nextOrdinal) {
        await assertUniqueKeyBuildChunkReplay(catalog, input);
        await transactionDone(transaction);
        return structuredClone(envelope.record);
      }
      if (input.ordinal !== envelope.record.nextOrdinal) {
        throw new UniqueKeyBuildConflictError(input.buildId, "chunk ordinal is not contiguous");
      }
      if (
        input.keyTokens.some(
          (token, index) => index > 0 && token <= (input.keyTokens[index - 1] ?? ""),
        )
      ) {
        throw new TypeError("UNIQUE build chunks must be in strict lexical order");
      }
      if (input.ordinal > 0) {
        const previousValue: unknown = await requestResult(
          catalog.get(uniqueKeyBuildChunkKey(input.buildId, input.ordinal - 1)),
        );
        if (!isRecord(previousValue) || typeof previousValue.lastToken !== "string") {
          throw corruption(
            uniqueKeyBuildKey(input.buildId),
            "previous UNIQUE build chunk is missing",
          );
        }
        if ((input.keyTokens[0] ?? "") <= previousValue.lastToken) {
          throw new TypeError("UNIQUE build chunks must be globally ordered");
        }
      }
      const nextRetainedBytes = safeByteSum(
        envelope.record.retainedBytes,
        retainedBytes,
        "UNIQUE build retained bytes",
      );
      if (nextRetainedBytes > MAX_UNIQUE_KEY_BUILD_STAGED_BYTES) {
        throw new StorageResourceLimitError(
          "unique-key build",
          nextRetainedBytes,
          MAX_UNIQUE_KEY_BUILD_STAGED_BYTES,
        );
      }
      const admission = await readUniqueKeyBuildAdmission(catalog);
      const totalRetainedBytes = safeByteSum(
        admission.retainedBytes,
        retainedBytes,
        "Global UNIQUE build retained bytes",
      );
      if (totalRetainedBytes > MAX_UNIQUE_KEY_BUILD_STAGED_BYTES_TOTAL) {
        throw new StorageResourceLimitError(
          "unique-key build",
          totalRetainedBytes,
          MAX_UNIQUE_KEY_BUILD_STAGED_BYTES_TOTAL,
        );
      }
      const parts = splitUniqueMembershipTokens(input.keyTokens);
      for (const part of parts) {
        catalog.add(part, uniqueKeyBasePartKey(input.buildId, part[0] ?? ""));
      }
      catalog.add(
        {
          tokenCount: input.keyTokens.length,
          retainedBytes,
          firstToken: input.keyTokens[0] ?? "",
          lastToken: input.keyTokens.at(-1) ?? "",
          partFirstTokens: parts.map((part) => part[0] ?? ""),
        },
        uniqueKeyBuildChunkKey(input.buildId, input.ordinal),
      );
      const record: UniqueKeyBuildRecord = {
        ...envelope.record,
        nextOrdinal: incrementSafeInteger(input.ordinal, "UNIQUE build ordinal"),
        tokenCount: safeByteSum(
          envelope.record.tokenCount,
          input.keyTokens.length,
          "UNIQUE build token count",
        ),
        retainedBytes: nextRetainedBytes,
        updatedAt,
      };
      catalog.put(uniqueKeyBuildEnvelope(record), key);
      await transactionDone(transaction);
      return structuredClone(record);
    } catch (error) {
      abortIfActive(transaction);
      await ignoreAbort(transaction);
      throw error;
    }
  }

  async finishUniqueKeyBuild(input: FinishUniqueKeyBuildInput): Promise<TableRecord> {
    validateId(input.buildId, "UNIQUE build ID");
    validateId(input.ownerId, "UNIQUE build owner ID");
    const cutoff = canonicalInputTimestamp(input.expiresAtCutoff, "UNIQUE build expiry cutoff");
    const completedAt = canonicalInputTimestamp(input.completedAt, "UNIQUE build completion time");
    if (
      !Number.isSafeInteger(input.expectedTableRevision) ||
      input.expectedTableRevision < 0 ||
      (input.expectedManifestVersion !== null &&
        (!Number.isSafeInteger(input.expectedManifestVersion) ||
          input.expectedManifestVersion < 0)) ||
      !Number.isSafeInteger(input.chunkCount) ||
      input.chunkCount < 0 ||
      !Number.isSafeInteger(input.coversVersion) ||
      input.coversVersion < -1
    ) {
      throw new TypeError("UNIQUE build completion metadata is invalid");
    }
    const transaction = this.#transaction(["catalog", "statistics"], "readwrite");
    try {
      const catalog = transaction.objectStore("catalog");
      const key = uniqueKeyBuildKey(input.buildId);
      const value: unknown = await requestResult(catalog.get(key));
      const envelope = value === undefined ? undefined : asUniqueKeyBuildEnvelope(value, key);
      if (envelope?.record.state === "completed") {
        const table = await readDeclaredTable(catalog, envelope.record.tableId);
        const index = table?.secondaryIndexes?.[envelope.record.indexId];
        const membershipValue: unknown = await requestResult(
          catalog.get(uniqueKeyChunkIndexKey(envelope.record.namespaceId)),
        );
        const membership =
          membershipValue === undefined ? undefined : asUniqueKeyChunkIndex(membershipValue);
        if (
          envelope.record.ownerId !== input.ownerId ||
          table === undefined ||
          index?.state !== "ready" ||
          index.uniqueEnforced !== true ||
          membership?.baseGenerationId !== input.buildId
        ) {
          throw new UniqueKeyBuildConflictError(input.buildId, "completed publication changed");
        }
        await transactionDone(transaction);
        return table;
      }
      if (
        envelope?.record.state !== "active" ||
        envelope.cleanup ||
        envelope.record.ownerId !== input.ownerId ||
        Date.parse(envelope.record.expiresAt) <= Date.parse(cutoff) ||
        envelope.record.nextOrdinal !== input.chunkCount
      ) {
        throw new UniqueKeyBuildConflictError(input.buildId, "build is incomplete or expired");
      }
      const currentValue: unknown = await requestResult(catalog.get(CURRENT_MANIFEST_KEY));
      const currentVersion = asOptionalManifestVersion(currentValue, CURRENT_MANIFEST_KEY) ?? null;
      if (currentVersion !== input.expectedManifestVersion) {
        throw new WriteConflictError(input.expectedManifestVersion, currentVersion);
      }
      const tableKey = `${TABLE_ID_PREFIX}${envelope.record.tableId}`;
      const tableValue: unknown = await requestResult(catalog.get(tableKey));
      const table = tableValue === undefined ? undefined : asTableRecord(tableValue, tableKey);
      if (table?.revision !== input.expectedTableRevision) {
        throw new TableRecordConflictError(
          envelope.record.tableId,
          input.expectedTableRevision,
          table?.revision ?? null,
        );
      }
      const currentIndex = table.secondaryIndexes?.[envelope.record.indexId];
      if (
        currentIndex?.state !== "building" ||
        currentIndex.unique !== true ||
        currentIndex.buildId !== input.buildId ||
        secondaryUniqueKeyNamespace(table.id, envelope.record.indexId) !==
          envelope.record.namespaceId
      ) {
        throw new UniqueKeyBuildConflictError(input.buildId, "catalog ownership changed");
      }
      const membershipKey = uniqueKeyChunkIndexKey(envelope.record.namespaceId);
      const membershipValue: unknown = await requestResult(catalog.get(membershipKey));
      if (membershipValue !== undefined) {
        const membership = asUniqueKeyChunkIndex(membershipValue);
        if (membership.hasBase || membership.versions.length > 0) {
          throw new UniqueKeyBuildConflictError(input.buildId, "membership is already populated");
        }
      }
      const { buildId: _buildId, ...readyIndex } = currentIndex;
      void _buildId;
      const updated: TableRecord = {
        ...table,
        secondaryIndexes: {
          ...table.secondaryIndexes,
          [envelope.record.indexId]: {
            ...readyIndex,
            state: "ready",
            uniqueEnforced: true,
          },
        },
        revision: incrementSafeInteger(table.revision, "Table revision"),
      };
      asIncomingTableRecord(updated, tableKey);
      await updateCatalogResourceLedger(transaction.objectStore("statistics"), table, updated);
      catalog.put(
        {
          versions: [],
          hasBase: true,
          baseGenerationId: input.buildId,
          tokenCount: envelope.record.tokenCount,
        } satisfies UniqueKeyChunkIndex,
        membershipKey,
      );
      catalog.put(updated, tableKey);
      const record: UniqueKeyBuildRecord = {
        ...envelope.record,
        state: "completed",
        retainedBytes: 0,
        updatedAt: completedAt,
        completedAt,
      };
      catalog.put(uniqueKeyBuildEnvelope(record), key);
      await bumpCatalogEpoch(catalog);
      await bumpSchemaEpoch(catalog);
      await transactionDone(transaction);
      this.#uniqueKeyCache = undefined;
      return structuredClone(updated);
    } catch (error) {
      abortIfActive(transaction);
      await ignoreAbort(transaction);
      throw error;
    }
  }

  async abortUniqueKeyBuild(input: AbortUniqueKeyBuildInput): Promise<boolean> {
    validateId(input.buildId, "UNIQUE build ID");
    validateId(input.ownerId, "UNIQUE build owner ID");
    const cutoff = canonicalInputTimestamp(input.expiresAtCutoff, "UNIQUE build expiry cutoff");
    const transaction = this.#transaction("catalog", "readwrite");
    try {
      const catalog = transaction.objectStore("catalog");
      const key = uniqueKeyBuildKey(input.buildId);
      const value: unknown = await requestResult(catalog.get(key));
      if (value === undefined) {
        await transactionDone(transaction);
        return false;
      }
      const envelope = asUniqueKeyBuildEnvelope(value, key);
      if (envelope.record.state !== "active") {
        throw new UniqueKeyBuildConflictError(input.buildId, "completed generation is published");
      }
      if (
        envelope.record.ownerId !== input.ownerId &&
        Date.parse(envelope.record.expiresAt) > Date.parse(cutoff)
      ) {
        throw new UniqueKeyBuildConflictError(input.buildId, "another live owner holds the build");
      }
      catalog.put(uniqueKeyBuildEnvelope(envelope.record, true), key);
      await transactionDone(transaction);
    } catch (error) {
      abortIfActive(transaction);
      await ignoreAbort(transaction);
      throw error;
    }
    await this.#cleanupUniqueKeyBuildArtifacts(input.buildId);
    return true;
  }

  async removeTable(
    id: string,
    expectedRevision: number,
    options: CatalogMutationOptions = {},
  ): Promise<void> {
    validateId(id, "Table ID");
    const transaction = this.#transaction(
      ["catalog", "gc", "segments", "statistics", "transactions"],
      "readwrite",
    );
    try {
      const catalog = transaction.objectStore("catalog");
      await assertExpectedCatalogEpoch(catalog, id, expectedRevision, options.expectedCatalogEpoch);
      const removal = await assertTableRemovalAllowed(transaction, id, expectedRevision);
      if (removal.segments.length > 0) {
        throw new Error(`Table ${id} has segments; use dropTable for data tables`);
      }
      await updateCatalogResourceLedger(
        transaction.objectStore("statistics"),
        removal.record,
        undefined,
      );
      await removeTableMetadataInTransaction(transaction, catalog, removal);
      await bumpCatalogEpoch(catalog);
      await bumpSchemaEpoch(catalog);
      await transactionDone(transaction);
      // The membership cache is keyed by manifest version, which a table drop does not move;
      // without this, a lookup after the drop would answer from the dead table's keys.
      if (
        this.#uniqueKeyCache?.tableId === id ||
        this.#uniqueKeyCache?.tableId.startsWith(`${id}\u0000secondary-index\u0000`) === true
      ) {
        this.#uniqueKeyCache = undefined;
      }
    } catch (error) {
      abortIfActive(transaction);
      await ignoreAbort(transaction);
      throw error;
    }
  }

  async dropTable(input: DropTableInput): Promise<ManifestSummary> {
    validateId(input.tableId, "Table ID");
    const transaction = this.#transaction(
      [
        "catalog",
        "gc",
        "leases",
        "manifests",
        "segments",
        "statistics",
        SNAPSHOT_HEADER_STORE,
        "transactions",
      ],
      "readwrite",
    );
    try {
      if (!Number.isFinite(Date.parse(input.committedAt))) {
        throw new TypeError("Table drop timestamp must be valid");
      }
      const catalog = transaction.objectStore("catalog");
      const actualCatalogEpoch = asCatalogEpoch(
        await requestResult<unknown>(catalog.get(CATALOG_EPOCH_KEY)),
      );
      if (actualCatalogEpoch !== input.expectedCatalogEpoch) {
        throw new TableRecordConflictError(
          input.tableId,
          input.expectedTableRevision,
          input.expectedTableRevision,
        );
      }
      const actualVersion =
        asOptionalManifestVersion(
          await requestResult<unknown>(catalog.get(CURRENT_MANIFEST_KEY)),
          CURRENT_MANIFEST_KEY,
        ) ?? null;
      if (actualVersion !== input.expectedManifestVersion) {
        throw new WriteConflictError(input.expectedManifestVersion, actualVersion);
      }
      const removal = await assertTableRemovalAllowed(
        transaction,
        input.tableId,
        input.expectedTableRevision,
      );
      await updateCatalogResourceLedger(
        transaction.objectStore("statistics"),
        removal.record,
        undefined,
      );
      const manifestStore = transaction.objectStore("manifests");
      let currentManifest: StoredManifestRecord | undefined;
      if (actualVersion !== null) {
        const value: unknown = await requestResult(manifestStore.get(actualVersion));
        if (value === undefined) throw new SnapshotManifestMissingError(actualVersion);
        currentManifest = asStoredManifestRecord(value, actualVersion);
        if (currentManifest.prunedAt !== undefined) {
          throw new SnapshotManifestMissingError(actualVersion);
        }
      }
      const tableBlockIds = new Set(removal.segments.flatMap(segmentBlockIds));
      const otherTableBlockIds = new Set<string>();
      await visitObjectStoreSequentially(transaction.objectStore("segments"), (value, key) => {
        if (typeof key !== "string") throw corruption("segments", "record key is invalid");
        const segment = asSegmentRecord(value);
        if (segment.id !== key) {
          throw corruption(`segments/${key}`, `record declares id ${segment.id}`);
        }
        if (segment.tableId !== input.tableId) {
          for (const blockId of segmentBlockIds(segment)) otherTableBlockIds.add(blockId);
        }
      });
      const retiredCandidates = [...tableBlockIds].filter((id) => !otherTableBlockIds.has(id));
      const newVersion =
        actualVersion === null ? 0 : incrementSafeInteger(actualVersion, "Manifest version");
      const retired = await retireManifestBlocksInTransaction(
        catalog,
        retiredCandidates,
        actualVersion,
        newVersion,
      );
      await updateRetiredHistoryLedger(transaction.objectStore("statistics"), retired.bytes);
      const manifest = createManifest({
        expectedVersion: actualVersion,
        liveBlockCount: (currentManifest?.liveBlockCount ?? 0) - retired.count,
        liveBlockBytes: (currentManifest?.liveBlockBytes ?? 0) - retired.bytes,
        changedTableIds: [input.tableId],
        createdAt: input.committedAt,
      });
      await updateRecordResourceLedger(transaction.objectStore("statistics"), {
        manifests: [{ next: manifest }],
        segments: removal.segments.map((segment) => ({ previous: segment })),
      });
      await assertPinnedHistoryAdmission(transaction, {
        cutoff: input.committedAt,
        currentVersion: manifest.version,
        prospectiveRemovedBlockIds: new Set(retired.ids),
      });
      await removeTableMetadataInTransaction(transaction, catalog, removal);
      manifestStore.add(manifest satisfies StoredManifestRecord, manifest.version);
      catalog.put(manifest.version, CURRENT_MANIFEST_KEY);
      // A completed import header is retained only to reconcile a lost acknowledgement from
      // that exact publication. Once a later manifest lands it can never be replayed safely,
      // and retaining its (bounded but potentially large) canonical header would be unbounded
      // database-lifetime overhead. Delete it in the same transaction as the version advance.
      catalog.delete(SNAPSHOT_FRAME_COMPLETED_KEY);
      await deleteSnapshotFrameRecords(transaction.objectStore(SNAPSHOT_HEADER_STORE), "import");
      await bumpCatalogEpoch(catalog);
      await bumpSchemaEpoch(catalog);
      await transactionDone(transaction);
      this.#manifestCache = undefined;
      if (
        this.#uniqueKeyCache?.tableId === input.tableId ||
        this.#uniqueKeyCache?.tableId.startsWith(`${input.tableId}\u0000secondary-index\u0000`) ===
          true
      ) {
        this.#uniqueKeyCache = undefined;
      } else if (this.#uniqueKeyCache !== undefined) {
        this.#uniqueKeyCache.version = manifest.version;
      }
      return manifest;
    } catch (error) {
      abortIfActive(transaction);
      await ignoreAbort(transaction);
      throw error;
    }
  }

  async dropTableColumn(input: DropTableColumnInput): Promise<ManifestSummary> {
    validateId(input.tableId, "Table ID");
    validateId(input.columnId, "Column ID");
    const transaction = this.#transaction(
      [
        "catalog",
        "gc",
        "leases",
        "manifests",
        "segments",
        "statistics",
        SNAPSHOT_HEADER_STORE,
        "transactions",
      ],
      "readwrite",
    );
    try {
      if (!Number.isFinite(Date.parse(input.committedAt))) {
        throw new TypeError("Column drop timestamp must be valid");
      }
      const catalog = transaction.objectStore("catalog");
      const actualCatalogEpoch = asCatalogEpoch(
        await requestResult<unknown>(catalog.get(CATALOG_EPOCH_KEY)),
      );
      if (actualCatalogEpoch !== input.expectedCatalogEpoch) {
        throw new TableRecordConflictError(
          input.tableId,
          input.expectedTableRevision,
          input.expectedTableRevision,
        );
      }
      const actualVersion =
        asOptionalManifestVersion(
          await requestResult<unknown>(catalog.get(CURRENT_MANIFEST_KEY)),
          CURRENT_MANIFEST_KEY,
        ) ?? null;
      if (actualVersion !== input.expectedManifestVersion) {
        throw new WriteConflictError(input.expectedManifestVersion, actualVersion);
      }
      const tableState = await assertTableRemovalAllowed(
        transaction,
        input.tableId,
        input.expectedTableRevision,
      );
      const { record: table } = tableState;
      const column = table.columns.find((candidate) => candidate.id === input.columnId);
      if (column === undefined) {
        throw new Error(`Column does not exist: ${input.tableId}/${input.columnId}`);
      }
      if (table.columns.length === 1) throw new Error("Cannot drop the last table column");
      if (
        table.uniqueKeyColumnId === input.columnId ||
        table.primaryKeyColumnIds?.includes(input.columnId) === true
      ) {
        throw new Error(`Cannot drop key column: ${input.columnId}`);
      }
      const dependentIndex = Object.values(table.secondaryIndexes ?? {}).find((index) =>
        secondaryIndexColumnIds(index).includes(input.columnId),
      );
      if (dependentIndex !== undefined) {
        throw new Error(`Cannot drop column used by secondary index: ${dependentIndex.name}`);
      }
      if (table.view !== undefined) throw new Error("Cannot drop a column from a view");

      const manifestStore = transaction.objectStore("manifests");
      let currentManifest: StoredManifestRecord | undefined;
      if (actualVersion !== null) {
        const value: unknown = await requestResult(manifestStore.get(actualVersion));
        if (value === undefined) throw new SnapshotManifestMissingError(actualVersion);
        currentManifest = asStoredManifestRecord(value, actualVersion);
        if (currentManifest.prunedAt !== undefined) {
          throw new SnapshotManifestMissingError(actualVersion);
        }
      }
      const candidateBlockIds = new Set<string>();
      for (const segment of tableState.segments) {
        for (const id of segment.columnBlockIds[input.columnId] ?? []) candidateBlockIds.add(id);
      }
      const remainingBlockIds = new Set<string>();
      const segmentStore = transaction.objectStore("segments");
      await visitObjectStoreSequentially(segmentStore, (segmentValue, key) => {
        if (typeof key !== "string") throw corruption("segments", "record key is invalid");
        const segment = asSegmentRecord(segmentValue);
        if (segment.id !== key) {
          throw corruption(`segments/${key}`, `record declares id ${segment.id}`);
        }
        for (const [columnId, blockIds] of Object.entries(segment.columnBlockIds)) {
          if (segment.tableId === input.tableId && columnId === input.columnId) continue;
          for (const id of blockIds) remainingBlockIds.add(id);
        }
      });
      const retiredCandidates = [...candidateBlockIds].filter((id) => !remainingBlockIds.has(id));
      const newVersion =
        actualVersion === null ? 0 : incrementSafeInteger(actualVersion, "Manifest version");
      const retired = await retireManifestBlocksInTransaction(
        catalog,
        retiredCandidates,
        actualVersion,
        newVersion,
      );
      await updateRetiredHistoryLedger(transaction.objectStore("statistics"), retired.bytes);
      const manifest = createManifest({
        expectedVersion: actualVersion,
        liveBlockCount: (currentManifest?.liveBlockCount ?? 0) - retired.count,
        liveBlockBytes: (currentManifest?.liveBlockBytes ?? 0) - retired.bytes,
        changedTableIds: [input.tableId],
        createdAt: input.committedAt,
      });
      await assertPinnedHistoryAdmission(transaction, {
        cutoff: input.committedAt,
        currentVersion: manifest.version,
        prospectiveRemovedBlockIds: new Set(retired.ids),
      });
      const { ftsColumns: previousFts, revision: _previousRevision, ...tableBase } = table;
      void _previousRevision;
      const nextFts = Object.fromEntries(
        Object.entries(previousFts ?? {}).filter(([columnId]) => columnId !== input.columnId),
      );
      const updatedTable: TableRecord = {
        ...tableBase,
        columns: table.columns.filter((candidate) => candidate.id !== input.columnId),
        ...(Object.keys(nextFts).length === 0 ? {} : { ftsColumns: nextFts }),
        revision: incrementSafeInteger(table.revision, "Table revision"),
      };
      asIncomingTableRecord(updatedTable, `${TABLE_ID_PREFIX}${table.id}`);
      await updateCatalogResourceLedger(transaction.objectStore("statistics"), table, updatedTable);
      const segmentChanges = tableState.segments.flatMap((segment) => {
        if (segment.columnBlockIds[input.columnId] === undefined) return [];
        const next = asSegmentRecord({
          ...segment,
          columnBlockIds: Object.fromEntries(
            Object.entries(segment.columnBlockIds).filter(
              ([columnId]) => columnId !== input.columnId,
            ),
          ),
        });
        return [{ previous: segment, next }];
      });
      await updateRecordResourceLedger(transaction.objectStore("statistics"), {
        manifests: [{ next: manifest }],
        segments: segmentChanges,
      });
      for (const { next } of segmentChanges) segmentStore.put(next, next.id);
      catalog.put(updatedTable, `${TABLE_ID_PREFIX}${table.id}`);
      catalog.delete(`${AUTO_INCREMENT_PREFIX}${table.id}/${input.columnId}`);
      await deleteFtsColumnRecords(catalog, table.id, input.columnId);
      manifestStore.add(manifest satisfies StoredManifestRecord, manifest.version);
      catalog.put(manifest.version, CURRENT_MANIFEST_KEY);
      catalog.delete(SNAPSHOT_FRAME_COMPLETED_KEY);
      await deleteSnapshotFrameRecords(transaction.objectStore(SNAPSHOT_HEADER_STORE), "import");
      await bumpCatalogEpoch(catalog);
      await bumpSchemaEpoch(catalog);
      await transactionDone(transaction);
      this.#manifestCache = undefined;
      if (this.#uniqueKeyCache !== undefined) this.#uniqueKeyCache.version = manifest.version;
      return manifest;
    } catch (error) {
      abortIfActive(transaction);
      await ignoreAbort(transaction);
      throw error;
    }
  }

  async writeFtsBase(
    tableId: string,
    columnId: string,
    input: { coversVersion: number; chunks: FtsPosting[][]; totalTokens: number },
  ): Promise<void> {
    validateId(tableId, "Table ID");
    validateId(columnId, "Column ID");
    if (
      !Number.isSafeInteger(input.coversVersion) ||
      input.coversVersion < -1 ||
      !Number.isSafeInteger(input.totalTokens) ||
      input.totalTokens < 0 ||
      input.chunks.length > MAX_FTS_BASE_CHUNKS
    ) {
      throw new TypeError("Full-text base metadata is invalid");
    }
    let tokenCount = 0;
    input.chunks.forEach((chunk, ordinal) => {
      const decoded = decodeFtsPostingChunk(chunk);
      if (decoded === undefined || decoded.length === 0) {
        throw new TypeError(`Full-text base chunk is invalid: ${String(ordinal)}`);
      }
      tokenCount = safeByteSum(tokenCount, ftsPostingTokenCount(decoded), "Full-text token count");
    });
    if (input.totalTokens !== tokenCount) {
      throw new TypeError("Full-text base total token count is invalid");
    }
    const buildId = crypto.randomUUID();
    const ownerId = `posting-build/${crypto.randomUUID()}`;
    const createdAt = dateIsoString(new Date());
    const expiresAt = dateIsoString(new Date(Date.parse(createdAt) + MAX_POSTING_BUILD_TTL_MS));
    await this.beginFtsBaseBuild({ tableId, columnId, buildId, ownerId, createdAt, expiresAt });
    try {
      for (const [ordinal, chunk] of input.chunks.entries()) {
        await this.writeFtsBaseBuildChunk({
          tableId,
          columnId,
          buildId,
          ownerId,
          expiresAtCutoff: createdAt,
          expiresAt,
          updatedAt: createdAt,
          ordinal,
          chunk,
        });
      }
      await this.finishFtsBaseBuild({
        tableId,
        columnId,
        buildId,
        ownerId,
        expiresAtCutoff: createdAt,
        coversVersion: input.coversVersion,
        chunkCount: input.chunks.length,
        totalTokens: input.totalTokens,
        completedAt: createdAt,
      });
    } catch (error) {
      await this.abortFtsBaseBuild({
        tableId,
        columnId,
        buildId,
        ownerId,
        expiresAtCutoff: createdAt,
      });
      throw error;
    }
  }

  async beginFtsBaseBuild(input: BeginPostingBuildInput): Promise<void> {
    const { tableId, columnId, buildId, ownerId } = input;
    validateId(tableId, "Table ID");
    validateId(columnId, "Column ID");
    validateId(buildId, "Full-text base build ID");
    validateId(ownerId, "Postings build owner ID");
    validateBoundedExpiration(
      input.createdAt,
      input.expiresAt,
      "Postings build",
      MAX_POSTING_BUILD_TTL_MS,
    );
    await this.#cleanupInterruptedFtsBaseBuildPage(input.createdAt);
    await this.#cleanupFtsRetirementFully(tableId, columnId);
    const probe = this.#transaction("catalog", "readonly");
    const markerKey = ftsBaseBuildKey(tableId, columnId);
    const existing = asOptionalFtsBaseBuildMarker(
      await requestResult<unknown>(probe.objectStore("catalog").get(markerKey)),
      markerKey,
    );
    await transactionDone(probe);
    if (
      existing?.buildId === buildId &&
      existing.ownerId === ownerId &&
      existing.createdAt === input.createdAt &&
      existing.expiresAt === input.expiresAt
    ) {
      return;
    }
    if (
      existing !== undefined &&
      (Date.parse(existing.expiresAt) > Date.parse(input.createdAt) ||
        !(await this.#deleteFtsBaseBuildFully(
          tableId,
          columnId,
          existing.buildId,
          Date.parse(existing.updatedAt),
        )))
    ) {
      throw new Error(`Postings base build is owned by another caller: ${buildId}`);
    }
    const transaction = this.#transaction("catalog", "readwrite");
    const store = transaction.objectStore("catalog");
    const tableKey = `${TABLE_ID_PREFIX}${tableId}`;
    const tableValue: unknown = await requestResult(store.get(tableKey));
    const table = tableValue === undefined ? undefined : asTableRecord(tableValue, tableKey);
    if (table === undefined || !activePostingStorageColumnIds(table).has(columnId)) {
      transaction.abort();
      await ignoreAbort(transaction);
      throw new Error(`Postings index is no longer active: ${tableId}/${columnId}`);
    }
    const ownerKind = Object.values(table.secondaryIndexes ?? {}).some(
      (index) => index.storageColumnId === columnId,
    )
      ? "secondary-index"
      : "fts-column";
    const admission = await readPostingBuildAdmission(store);
    const activeForKind =
      ownerKind === "secondary-index" ? admission.secondaryBuilds : admission.ftsBuilds;
    const activeLimit =
      ownerKind === "secondary-index"
        ? MAX_ACTIVE_SECONDARY_INDEX_BUILDS
        : MAX_ACTIVE_FTS_BASE_BUILDS;
    if (activeForKind >= activeLimit) {
      throw new StorageResourceLimitError(
        ownerKind === "secondary-index" ? "secondary-index build" : "full-text build",
        activeForKind + 1,
        activeLimit,
      );
    }
    if ((await requestResult(store.getKey(markerKey))) !== undefined) {
      transaction.abort();
      await ignoreAbort(transaction);
      throw new Error(`Full-text base build changed: ${buildId}`);
    }
    store.put(
      {
        buildId,
        ownerId,
        ownerKind,
        createdAt: input.createdAt,
        expiresAt: input.expiresAt,
        ftsBuildExpiry: input.expiresAt,
        boundaries: [],
        totalTokens: 0,
        retainedBytes: 0,
        retainedEntries: 0,
        secondaryCompatible: true,
        updatedAt: input.createdAt,
        cleanupIndex: 0,
      },
      markerKey,
    );
    await transactionDone(transaction);
  }

  async renewFtsBaseBuild(input: RenewPostingBuildInput): Promise<void> {
    validatePostingBuildOwnerInput(input);
    const transaction = this.#transaction("catalog", "readwrite");
    try {
      const catalog = transaction.objectStore("catalog");
      const key = ftsBaseBuildKey(input.tableId, input.columnId);
      const marker = asOptionalFtsBaseBuildMarker(
        await requestResult<unknown>(catalog.get(key)),
        key,
      );
      assertLivePostingBuildOwner(marker, input);
      catalog.put(
        {
          ...marker,
          expiresAt: input.expiresAt,
          ftsBuildExpiry: input.expiresAt,
          updatedAt: input.updatedAt,
        },
        key,
      );
      await transactionDone(transaction);
    } catch (error) {
      abortIfActive(transaction);
      await ignoreAbort(transaction);
      throw error;
    }
  }

  async writeFtsBaseBuildChunk(input: AppendPostingBuildChunkInput): Promise<void> {
    const { tableId, columnId, buildId, ordinal } = input;
    const chunk = [...input.chunk];
    validateId(tableId, "Table ID");
    validateId(columnId, "Column ID");
    validateId(buildId, "Full-text base build ID");
    validatePostingBuildOwnerInput(input);
    if (!Number.isSafeInteger(ordinal) || ordinal < 0 || ordinal >= MAX_FTS_BASE_CHUNKS) {
      throw new RangeError("Full-text base chunk ordinal is outside its bounded range");
    }
    const decodedChunk = decodeFtsPostingChunk(chunk);
    if (decodedChunk === undefined || decodedChunk.length === 0) {
      throw new TypeError(`Full-text base chunk is invalid: ${String(ordinal)}`);
    }
    const transaction = this.#transaction("catalog", "readwrite");
    const store = transaction.objectStore("catalog");
    const markerKey = ftsBaseBuildKey(tableId, columnId);
    const marker = asOptionalFtsBaseBuildMarker(
      await requestResult<unknown>(store.get(markerKey)),
      markerKey,
    );
    assertLivePostingBuildOwner(marker, input);
    if (marker.cleanupIndex !== 0) {
      transaction.abort();
      await ignoreAbort(transaction);
      throw new Error(`Full-text base build changed: ${buildId}`);
    }
    const chunkKey = `${ftsBaseChunkPrefix(tableId, columnId, buildId)}${String(ordinal).padStart(6, "0")}`;
    if (ordinal < marker.boundaries.length) {
      const stored = decodeFtsPostingChunk(await requestResult<unknown>(store.get(chunkKey)));
      if (
        stored === undefined ||
        !sameFtsPostingChunk(stored, decodedChunk) ||
        !ftsChunkMatchesBoundary(stored, marker.boundaries[ordinal])
      ) {
        throw new PostingBuildConflictError(buildId, input.ownerId, "chunk replay changed");
      }
      store.put(
        {
          ...marker,
          expiresAt: input.expiresAt,
          ftsBuildExpiry: input.expiresAt,
          updatedAt: input.updatedAt,
        },
        markerKey,
      );
      await transactionDone(transaction);
      return;
    }
    if (ordinal > marker.boundaries.length) {
      transaction.abort();
      await ignoreAbort(transaction);
      throw new Error(`Full-text base chunk is out of order: ${String(ordinal)}`);
    }
    const retained = ftsPostingChunkRetainedBounds(decodedChunk);
    const retainedEntries = decodedChunk.reduce(
      (total, posting) => safeByteSum(total, 1 + posting.rowIds.length, "Postings entries"),
      0,
    );
    const admission = await readPostingBuildAdmission(store);
    const nextGlobalBytes = safeByteSum(
      admission.retainedBytes,
      retained.bytes,
      "Accelerator build retained bytes",
    );
    const nextGlobalEntries = safeByteSum(
      admission.retainedEntries,
      retainedEntries,
      "Accelerator build retained entries",
    );
    if (nextGlobalBytes > MAX_ACCELERATOR_BUILD_STAGED_BYTES_TOTAL) {
      throw new StorageResourceLimitError(
        "accelerator build byte",
        nextGlobalBytes,
        MAX_ACCELERATOR_BUILD_STAGED_BYTES_TOTAL,
      );
    }
    if (nextGlobalEntries > MAX_ACCELERATOR_BUILD_STAGED_ENTRIES_TOTAL) {
      throw new StorageResourceLimitError(
        "accelerator build entry",
        nextGlobalEntries,
        MAX_ACCELERATOR_BUILD_STAGED_ENTRIES_TOTAL,
      );
    }
    store.put(decodedChunk, chunkKey);
    store.put(
      {
        buildId,
        ownerId: marker.ownerId,
        ownerKind: marker.ownerKind,
        createdAt: marker.createdAt,
        expiresAt: input.expiresAt,
        ftsBuildExpiry: input.expiresAt,
        updatedAt: input.updatedAt,
        cleanupIndex: 0,
        boundaries: [
          ...marker.boundaries,
          {
            first: decodedChunk[0]?.term ?? "",
            last: decodedChunk[decodedChunk.length - 1]?.term ?? "",
          },
        ],
        totalTokens: safeByteSum(
          marker.totalTokens,
          ftsPostingTokenCount(decodedChunk),
          "Full-text build token count",
        ),
        retainedBytes: safeByteSum(
          marker.retainedBytes,
          retained.bytes,
          "Postings build retained bytes",
        ),
        retainedEntries: safeByteSum(
          marker.retainedEntries,
          retainedEntries,
          "Postings build retained entries",
        ),
        secondaryCompatible:
          marker.secondaryCompatible &&
          decodedChunk.every((posting) => posting.tf.every((frequency) => frequency === 1)),
      },
      markerKey,
    );
    await transactionDone(transaction);
  }

  async finishFtsBaseBuild(input: FinishPostingBuildInput): Promise<void> {
    const { tableId, columnId, buildId } = input;
    validateId(tableId, "Table ID");
    validateId(columnId, "Column ID");
    validateId(buildId, "Full-text base build ID");
    validateId(input.ownerId, "Postings build owner ID");
    canonicalInputTimestamp(input.expiresAtCutoff, "Postings build expiry cutoff");
    canonicalInputTimestamp(input.completedAt, "Postings build completion time");
    if (
      !Number.isSafeInteger(input.coversVersion) ||
      input.coversVersion < -1 ||
      !Number.isSafeInteger(input.chunkCount) ||
      input.chunkCount < 0 ||
      input.chunkCount > MAX_FTS_BASE_CHUNKS ||
      !Number.isSafeInteger(input.totalTokens) ||
      input.totalTokens < 0
    ) {
      throw new TypeError("Full-text base completion metadata is invalid");
    }
    await this.#cleanupFtsRetirementFully(tableId, columnId);
    const transaction = this.#transaction("catalog", "readwrite");
    const store = transaction.objectStore("catalog");
    const markerKey = ftsBaseBuildKey(tableId, columnId);
    const [markerValue, previousValue, deltaIndexValue, tableValue] = await Promise.all([
      requestResult<unknown>(store.get(markerKey)),
      requestResult<unknown>(store.get(`${FTS_BASE_INDEX_PREFIX}${tableId}/${columnId}`)),
      requestResult<unknown>(store.get(ftsChunkIndexKey(tableId, columnId))),
      requestResult<unknown>(store.get(`${TABLE_ID_PREFIX}${tableId}`)),
    ]);
    const marker = asOptionalFtsBaseBuildMarker(markerValue, markerKey);
    const previousKey = `${FTS_BASE_INDEX_PREFIX}${tableId}/${columnId}`;
    const previous = asOptionalFtsBaseToc(previousValue, previousKey);
    const deltaIndexKey = ftsChunkIndexKey(tableId, columnId);
    const deltaIndex = asOptionalFtsDeltaIndex(deltaIndexValue, deltaIndexKey);
    const tableKey = `${TABLE_ID_PREFIX}${tableId}`;
    const table = tableValue === undefined ? undefined : asTableRecord(tableValue, tableKey);
    if (
      marker?.buildId !== buildId ||
      marker.ownerId !== input.ownerId ||
      Date.parse(marker.expiresAt) <= Date.parse(input.expiresAtCutoff) ||
      marker.cleanupIndex !== 0 ||
      marker.boundaries.length !== input.chunkCount
    ) {
      transaction.abort();
      await ignoreAbort(transaction);
      throw new Error(`Full-text base build is incomplete: ${buildId}`);
    }
    if (table === undefined || !activePostingStorageColumnIds(table).has(columnId)) {
      await transactionDone(transaction);
      await this.#deleteFtsBaseBuildFully(tableId, columnId, buildId);
      return;
    }
    const secondaryOwner = Object.values(table.secondaryIndexes ?? {}).some(
      (index) => index.storageColumnId === columnId,
    );
    if (
      marker.totalTokens !== input.totalTokens ||
      (secondaryOwner && !marker.secondaryCompatible)
    ) {
      throw new TypeError("Full-text base total token count does not match its postings");
    }
    const retiredGenerations: FtsRetirementMarker["generations"] =
      previous === undefined
        ? []
        : [
            {
              generation: previous.generation,
              chunkCount: previous.boundaries.length,
              cleanupIndex: 0,
            },
          ];
    store.put(
      {
        coversVersion: input.coversVersion,
        boundaries: marker.boundaries,
        totalTokens: input.totalTokens,
        generation: buildId,
      } satisfies FtsBaseToc,
      `${FTS_BASE_INDEX_PREFIX}${tableId}/${columnId}`,
    );
    const surviving: number[] = [];
    const retiredVersions: number[] = [];
    for (const version of deltaIndex?.versions ?? []) {
      if (version <= input.coversVersion) retiredVersions.push(version);
      else surviving.push(version);
    }
    await stageFtsRetirement(store, tableId, columnId, retiredGenerations, retiredVersions);
    store.put({ versions: surviving }, ftsChunkIndexKey(tableId, columnId));
    store.delete(markerKey);
    await transactionDone(transaction);
  }

  async abortFtsBaseBuild(input: AbortPostingBuildInput): Promise<void> {
    const { tableId, columnId, buildId } = input;
    validateId(tableId, "Table ID");
    validateId(columnId, "Column ID");
    validateId(buildId, "Full-text base build ID");
    validateId(input.ownerId, "Postings build owner ID");
    const cutoff = canonicalInputTimestamp(input.expiresAtCutoff, "Postings build expiry cutoff");
    const probe = this.#transaction("catalog", "readonly");
    const key = ftsBaseBuildKey(tableId, columnId);
    const marker = asOptionalFtsBaseBuildMarker(
      await requestResult<unknown>(probe.objectStore("catalog").get(key)),
      key,
    );
    await transactionDone(probe);
    if (marker === undefined) return;
    if (marker.buildId !== buildId) throw new Error(`Postings base build changed: ${buildId}`);
    if (marker.ownerId !== input.ownerId && Date.parse(marker.expiresAt) > Date.parse(cutoff)) {
      throw new Error(`Postings base build is owned by another caller: ${buildId}`);
    }
    await this.#deleteFtsBaseBuildFully(tableId, columnId, buildId, Date.parse(marker.updatedAt));
  }

  async removeFtsColumn(tableId: string, columnId: string): Promise<void> {
    validateId(tableId, "Table ID");
    validateId(columnId, "Column ID");
    const transaction = this.#transaction("catalog", "readwrite");
    const store = transaction.objectStore("catalog");
    const tableKey = `${TABLE_ID_PREFIX}${tableId}`;
    const tableValue: unknown = await requestResult(store.get(tableKey));
    const table = tableValue === undefined ? undefined : asTableRecord(tableValue, tableKey);
    if (table !== undefined && activePostingStorageColumnIds(table).has(columnId)) {
      transaction.abort();
      await ignoreAbort(transaction);
      throw new Error(`Postings index is still active: ${tableId}/${columnId}`);
    }
    await deleteFtsColumnRecords(store, tableId, columnId);
    await transactionDone(transaction);
    await this.#cleanupFtsRetirementFully(tableId, columnId);
  }

  async readFtsCandidates(
    tableId: string,
    columnId: string,
    terms: readonly FtsPostingQuery[],
    upToVersion: number,
    maxRowIds = MAX_FTS_CANDIDATE_ROW_IDS,
  ): Promise<
    FtsCandidates & {
      deltaChunkCount: number;
      totalTokens: number;
      coversVersion: number;
      hasBase: boolean;
    }
  > {
    validateId(tableId, "Table ID");
    validateId(columnId, "Column ID");
    validateFtsPostingQueries(terms);
    if (
      !Number.isSafeInteger(maxRowIds) ||
      maxRowIds < 1 ||
      maxRowIds > MAX_FTS_CANDIDATE_ROW_IDS
    ) {
      throw new RangeError(
        `Full-text candidate limit must be between 1 and ${String(MAX_FTS_CANDIDATE_ROW_IDS)}`,
      );
    }
    const transaction = this.#transaction("catalog", "readonly");
    const store = transaction.objectStore("catalog");
    const [rawToc, rawDeltaIndex] = await Promise.all([
      requestResult<unknown>(store.get(`${FTS_BASE_INDEX_PREFIX}${tableId}/${columnId}`)),
      requestResult<unknown>(store.get(ftsChunkIndexKey(tableId, columnId))),
    ]);
    const toc = decodeFtsBaseToc(rawToc);
    const deltaIndex = decodeFtsDeltaIndex(rawDeltaIndex);
    const metadataComplete =
      (rawToc === undefined || toc !== undefined) &&
      (rawDeltaIndex === undefined || deltaIndex !== undefined) &&
      (toc === undefined || deltaIndex !== undefined);
    const chunkPrefix = ftsBaseChunkPrefix(tableId, columnId, toc?.generation);
    const wantedOrdinals = (toc?.boundaries ?? []).flatMap((boundary, ordinal) =>
      terms.some((query) => {
        const lower = "term" in query ? query.term : query.lower;
        const upper =
          "term" in query ? (query.prefix ? `${query.term}￿` : query.term) : query.upper;
        return (
          (lower === undefined || lower <= boundary.last) &&
          (upper === undefined || upper >= boundary.first)
        );
      })
        ? [ordinal]
        : [],
    );
    const coversVersion = toc?.coversVersion ?? -1;
    const wantedVersions = (deltaIndex?.versions ?? []).filter(
      (version) => version > coversVersion && version <= upToVersion,
    );
    const candidateSets = terms.map(() => new Set<bigint>());
    let retainedRowIds = 0;
    let overflow = false;
    let complete = metadataComplete;
    let deltaChunkCount = 0;
    let totalTokens = toc?.totalTokens ?? 0;
    const consume = (postings: readonly FtsPosting[]): boolean => {
      for (const posting of postings) {
        for (let index = 0; index < terms.length; index += 1) {
          const query = terms[index];
          const set = candidateSets[index];
          if (
            query === undefined ||
            set === undefined ||
            !ftsPostingQueryMatches(posting.term, query)
          ) {
            continue;
          }
          for (const rowId of posting.rowIds) {
            if (set.has(rowId)) continue;
            if (retainedRowIds === maxRowIds) {
              return true;
            }
            set.add(rowId);
            retainedRowIds += 1;
          }
        }
      }
      return false;
    };
    for (const ordinal of wantedOrdinals) {
      const value: unknown = await requestResult(
        store.get(`${chunkPrefix}${String(ordinal).padStart(6, "0")}`),
      );
      const chunk = decodeFtsPostingChunk(value);
      if (chunk === undefined || !ftsChunkMatchesBoundary(chunk, toc?.boundaries[ordinal])) {
        complete = false;
      } else if (consume(chunk)) {
        overflow = true;
        break;
      }
    }
    if (!overflow) {
      for (const version of wantedVersions) {
        const value: unknown = await requestResult(
          store.get(ftsChunkKey(tableId, columnId, version)),
        );
        const chunk = decodeFtsDeltaChunk(value);
        if (chunk === undefined) {
          complete = false;
        } else {
          deltaChunkCount += 1;
          totalTokens = safeByteSum(totalTokens, chunk.totalTokens, "Full-text token count");
          if (consume(chunk.postings)) {
            overflow = true;
            break;
          }
        }
      }
    }
    await transactionDone(transaction);
    return {
      rowIdsByTerm: overflow
        ? terms.map(() => [])
        : candidateSets.map((set) =>
            [...set].sort((left, right) => (left < right ? -1 : left > right ? 1 : 0)),
          ),
      overflow,
      deltaChunkCount,
      totalTokens,
      coversVersion,
      // Incomplete accelerator state is a cache miss, never a partial pruning answer. Every
      // engine path already treats hasBase=false as a full-scan/rebuild signal.
      hasBase: toc !== undefined && complete && !overflow,
    };
  }

  async readFtsPostings(
    tableId: string,
    columnId: string,
    upToVersion: number,
    maxRowIds = MAX_FTS_CANDIDATE_ROW_IDS,
    maxRetainedBytes = MAX_FTS_ORDERED_READ_BYTES,
  ) {
    validateId(tableId, "Table ID");
    validateId(columnId, "Column ID");
    if (!Number.isSafeInteger(upToVersion) || upToVersion < 0) {
      throw new RangeError("Full-text snapshot version must be a non-negative safe integer");
    }
    validateFtsOrderedReadLimits(maxRowIds, maxRetainedBytes);
    const transaction = this.#transaction("catalog", "readonly");
    const store = transaction.objectStore("catalog");
    const [rawToc, rawDeltaIndex] = await Promise.all([
      requestResult<unknown>(store.get(`${FTS_BASE_INDEX_PREFIX}${tableId}/${columnId}`)),
      requestResult<unknown>(store.get(ftsChunkIndexKey(tableId, columnId))),
    ]);
    const toc = decodeFtsBaseToc(rawToc);
    const deltaIndex = decodeFtsDeltaIndex(rawDeltaIndex);
    const metadataComplete =
      (rawToc === undefined || toc !== undefined) &&
      (rawDeltaIndex === undefined || deltaIndex !== undefined) &&
      (toc === undefined || deltaIndex !== undefined);
    const coversVersion = toc?.coversVersion ?? -1;
    const prefix = ftsBaseChunkPrefix(tableId, columnId, toc?.generation);
    const versions = (deltaIndex?.versions ?? []).filter(
      (version) => version > coversVersion && version <= upToVersion,
    );
    const chunks: FtsPosting[][] = [];
    let retainedRowIds = 0;
    let retainedBytes = 0;
    let overflow = false;
    let complete = metadataComplete;
    let deltaChunkCount = 0;
    const retain = (chunk: FtsPosting[]): boolean => {
      const bounds = ftsPostingChunkRetainedBounds(chunk);
      if (
        retainedRowIds > maxRowIds - bounds.rowIds ||
        retainedBytes > maxRetainedBytes - bounds.bytes
      ) {
        return false;
      }
      retainedRowIds += bounds.rowIds;
      retainedBytes += bounds.bytes;
      chunks.push(chunk);
      return true;
    };
    for (let ordinal = 0; ordinal < (toc?.boundaries.length ?? 0); ordinal += 1) {
      const value: unknown = await requestResult(
        store.get(`${prefix}${String(ordinal).padStart(6, "0")}`),
      );
      const chunk = decodeFtsPostingChunk(value);
      if (chunk === undefined || !ftsChunkMatchesBoundary(chunk, toc?.boundaries[ordinal])) {
        complete = false;
      } else if (!retain(chunk)) {
        overflow = true;
        break;
      }
    }
    if (!overflow) {
      for (const version of versions) {
        const value: unknown = await requestResult(
          store.get(ftsChunkKey(tableId, columnId, version)),
        );
        const chunk = decodeFtsDeltaChunk(value);
        if (chunk === undefined) {
          complete = false;
        } else {
          deltaChunkCount += 1;
          if (!retain(chunk.postings)) {
            overflow = true;
            break;
          }
        }
      }
    }
    await transactionDone(transaction);
    return {
      postings: overflow ? [] : collectFtsPostings(chunks),
      overflow,
      deltaChunkCount,
      coversVersion,
      hasBase: toc !== undefined && complete && !overflow,
    };
  }

  /**
   * Resolving a name means two reads — name to id, then id to record — and the second cannot
   * start until the first lands, so the write path pays two serialized round trips for one
   * lookup. A remembered id collapses that to one read, and needs no invalidation: the record
   * carries its own name, so a mapping that has gone stale fails the check below and falls
   * back to the full resolution. Table ids are unique per creation, so a dropped and recreated
   * table cannot be mistaken for its predecessor.
   */
  async getTableByName(name: string): Promise<TableRecord | undefined> {
    validateCatalogName(name, "Table name");
    const transaction = this.#transaction("catalog", "readonly");
    const store = transaction.objectStore("catalog");
    const rememberedId = this.#tableIdsByName.get(name);
    if (rememberedId !== undefined) {
      const cached: unknown = await requestResult(store.get(`${TABLE_ID_PREFIX}${rememberedId}`));
      if (cached !== undefined) {
        const record = asTableRecord(cached, `${TABLE_ID_PREFIX}${rememberedId}`);
        if (record.name === name) {
          this.#tableIdsByName.delete(name);
          this.#tableIdsByName.set(name, rememberedId);
          await transactionDone(transaction);
          return record;
        }
      }
      this.#tableIdsByName.delete(name);
    }
    const rawId: unknown = await requestResult(store.get(`${TABLE_NAME_PREFIX}${name}`));
    if (rawId !== undefined && !isStorageId(rawId)) {
      throw corruption(`${TABLE_NAME_PREFIX}${name}`, "table name pointer is invalid");
    }
    const id = typeof rawId === "string" ? rawId : undefined;
    const value: unknown =
      id === undefined ? undefined : await requestResult(store.get(`${TABLE_ID_PREFIX}${id}`));
    await transactionDone(transaction);
    if (value === undefined) return undefined;
    const record = asTableRecord(value, `${TABLE_ID_PREFIX}${id ?? ""}`);
    if (record.name !== name || record.id !== id) {
      throw corruption(`${TABLE_NAME_PREFIX}${name}`, "does not match its table record");
    }
    this.#tableIdsByName.set(name, record.id);
    if (this.#tableIdsByName.size > TABLE_NAME_CACHE_LIMIT) {
      const oldest = this.#tableIdsByName.keys().next().value;
      if (oldest !== undefined) this.#tableIdsByName.delete(oldest);
    }
    return record;
  }

  async listTables(): Promise<TableRecord[]> {
    const transaction = this.#transaction("catalog", "readonly");
    const store = transaction.objectStore("catalog");
    const records: TableRecord[] = [];
    await visitObjectStoreSequentially(store, (value, key) => {
      if (typeof key === "string" && key.startsWith(TABLE_ID_PREFIX)) {
        records.push(asTableRecord(value, key));
      }
    });
    await transactionDone(transaction);
    return records.sort((left, right) => left.name.localeCompare(right.name));
  }

  async getSegment(id: string): Promise<SegmentRecord | undefined> {
    validateId(id, "Segment ID");
    const transaction = this.#transaction("segments", "readonly");
    const value: unknown = await requestResult(transaction.objectStore("segments").get(id));
    await transactionDone(transaction);
    return value === undefined ? undefined : asSegmentRecord(value);
  }

  async listSegmentPage(afterId: string | null, limit: number) {
    validatePageLimit(limit);
    const transaction = this.#transaction("segments", "readonly");
    const records = await readCursorPage(
      transaction.objectStore("segments"),
      limit,
      asSegmentRecord,
      (key) => typeof key === "string" && (afterId === null || key > afterId),
      afterId ?? undefined,
    );
    await transactionDone(transaction);
    return { records, nextCursor: records.length === limit ? (records.at(-1)?.id ?? null) : null };
  }

  async listTableSegmentPage(tableId: string, afterId: string | null, limit: number) {
    validateId(tableId, "Table ID");
    if (afterId !== null) validateId(afterId, "Segment cursor");
    validatePageLimit(limit);
    const transaction = this.#transaction("segments", "readonly");
    try {
      const records = await readTableSegmentPage(
        transaction.objectStore("segments").index(SEGMENT_TABLE_INDEX),
        tableId,
        afterId,
        limit,
      );
      await transactionDone(transaction);
      return {
        records,
        nextCursor: records.length === limit ? (records.at(-1)?.id ?? null) : null,
      };
    } catch (error) {
      abortIfActive(transaction);
      await ignoreAbort(transaction);
      throw error;
    }
  }

  async removeAbortedSegment(segmentId: string, expectedTransactionId: string): Promise<boolean> {
    validateId(segmentId, "Segment ID");
    validateId(expectedTransactionId, "Transaction ID");
    const transaction = this.#transaction(
      ["catalog", "gc", "manifests", "segments", "statistics", "transactions"],
      "readwrite",
    );
    try {
      const segmentStore = transaction.objectStore("segments");
      const value: unknown = await requestResult(segmentStore.get(segmentId));
      if (value === undefined) {
        await transactionDone(transaction);
        return false;
      }
      const segment = asSegmentRecord(value);
      if (segment.id !== segmentId || segment.transactionId !== expectedTransactionId) {
        throw new Error(
          `Segment ${segmentId} is not owned by transaction ${expectedTransactionId}`,
        );
      }
      const transactionStore = transaction.objectStore("transactions");
      const ownerValue: unknown = await requestResult(transactionStore.get(expectedTransactionId));
      if (ownerValue === undefined) {
        throw corruption(
          `segments/${segmentId}`,
          `owning transaction ${expectedTransactionId} is missing`,
        );
      }
      const owner = asTransactionRecord(ownerValue, expectedTransactionId);
      if (owner.status !== "aborted") {
        throw new Error(`Segment ${segmentId} owner must be aborted; found ${owner.status}`);
      }
      if (!owner.pendingSegmentIds.includes(segmentId)) {
        throw corruption(
          `transactions/${owner.id}`,
          `owned segment ${segmentId} is absent from its journal`,
        );
      }
      const published = await findReadableManifestBlock(transaction, segmentBlockIds(segment));
      if (published !== undefined) {
        throw new Error(
          `Segment ${segmentId} is published by manifest ${String(published.version)}`,
        );
      }
      await visitObjectStoreSequentially(transactionStore, (candidateValue, key) => {
        if (typeof key !== "string") throw corruption("transactions", "record key is invalid");
        const candidate = asTransactionRecord(candidateValue, key);
        if (candidate.status === "active" && candidate.pendingSegmentIds.includes(segmentId)) {
          throw new Error(
            `Segment ${segmentId} is referenced by active transaction ${candidate.id}`,
          );
        }
      });
      await visitObjectStoreSequentially(transaction.objectStore("gc"), (jobValue, key) => {
        const job = asCompactionJobAtMaintenanceKey(jobValue, key);
        if (
          job !== undefined &&
          !isTerminalCompactionJob(job) &&
          (job.sourceSegmentIds.includes(segmentId) ||
            compactionOutputSegmentIds(job).includes(segmentId))
        ) {
          throw new Error(`Segment ${segmentId} is referenced by compaction job ${job.id}`);
        }
      });
      const retainedSegmentIds = owner.pendingSegmentIds.filter((id) => id !== segmentId);
      const rebasedSegments = (
        await Promise.all(
          retainedSegmentIds.map(async (id, commitOrdinal) => {
            const retainedValue: unknown = await requestResult(segmentStore.get(id));
            if (retainedValue === undefined) {
              throw corruption(`transactions/${owner.id}`, `pending segment ${id} is missing`);
            }
            const retained = asSegmentRecord(retainedValue);
            if (retained.transactionId !== owner.id) {
              throw corruption(
                `transactions/${owner.id}`,
                `pending segment ${id} belongs to another transaction`,
              );
            }
            return retained.commitOrdinal === commitOrdinal
              ? undefined
              : { previous: retained, next: { ...retained, commitOrdinal } };
          }),
        )
      ).filter(
        (entry): entry is { previous: SegmentRecord; next: SegmentRecord } => entry !== undefined,
      );
      const updatedOwner: TransactionRecord = {
        ...owner,
        pendingSegmentIds: retainedSegmentIds,
        revision: incrementSafeInteger(owner.revision, "Transaction revision"),
      };
      await updateTransactionResourceLedger(transaction.objectStore("statistics"), owner.id, {
        blockCount: 0,
        segmentCount: -1,
        retainedBytes: 0,
      });
      await updateRecordResourceLedger(transaction.objectStore("statistics"), {
        segments: [{ previous: segment }, ...rebasedSegments],
      });
      segmentStore.delete(segmentId);
      for (const { next } of rebasedSegments) segmentStore.put(next, next.id);
      transactionStore.put(updatedOwner, owner.id);
      await transactionDone(transaction);
      return true;
    } catch (error) {
      abortIfActive(transaction);
      await ignoreAbort(transaction);
      throw error;
    }
  }

  async adoptAbortedSegment(input: AdoptAbortedSegmentInput): Promise<TransactionRecord> {
    validateId(input.segment.id, "Segment ID");
    validateId(input.expectedAbortedTransactionId, "Aborted transaction ID");
    validateId(input.replacementTransactionId, "Replacement transaction ID");
    validateId(input.compactionJobId, "Compaction job ID");
    const desired = asSegmentRecord(input.segment);
    if (
      desired.transactionId !== input.replacementTransactionId ||
      input.expectedAbortedTransactionId === input.replacementTransactionId
    ) {
      throw new TypeError("Segment adoption transaction identities are invalid");
    }
    canonicalInputTimestamp(input.updatedAt, "Segment adoption timestamp");
    const transaction = this.#transaction(
      ["blocks", "catalog", "gc", "manifests", "segments", "statistics", "transactions"],
      "readwrite",
    );
    try {
      const segmentStore = transaction.objectStore("segments");
      const transactionStore = transaction.objectStore("transactions");
      const [storedValue, abortedValue, replacementValue] = await Promise.all([
        requestResult<unknown>(segmentStore.get(desired.id)),
        requestResult<unknown>(transactionStore.get(input.expectedAbortedTransactionId)),
        requestResult<unknown>(transactionStore.get(input.replacementTransactionId)),
      ]);
      if (storedValue === undefined) throw new Error(`Segment does not exist: ${desired.id}`);
      const stored = asSegmentRecord(storedValue);
      const {
        transactionId: _storedOwner,
        commitOrdinal: _storedOrdinal,
        ...storedContent
      } = stored;
      const {
        transactionId: _desiredOwner,
        commitOrdinal: _desiredOrdinal,
        ...desiredContent
      } = desired;
      void _storedOwner;
      void _storedOrdinal;
      void _desiredOwner;
      void _desiredOrdinal;
      if (
        stored.transactionId !== input.expectedAbortedTransactionId ||
        !sameStructuredValue(storedContent, desiredContent)
      ) {
        throw new Error(`Segment ${desired.id} does not match the immutable adoption record`);
      }
      const aborted =
        abortedValue === undefined
          ? undefined
          : asTransactionRecord(abortedValue, input.expectedAbortedTransactionId);
      if (aborted?.revision !== input.expectedAbortedTransactionRevision) {
        throw new TransactionRecordConflictError(
          input.expectedAbortedTransactionId,
          input.expectedAbortedTransactionRevision,
          aborted?.revision ?? null,
        );
      }
      const replacement =
        replacementValue === undefined
          ? undefined
          : asTransactionRecord(replacementValue, input.replacementTransactionId);
      if (replacement?.revision !== input.expectedReplacementTransactionRevision) {
        throw new TransactionRecordConflictError(
          input.replacementTransactionId,
          input.expectedReplacementTransactionRevision,
          replacement?.revision ?? null,
        );
      }
      if (aborted.status !== "aborted" || !aborted.pendingSegmentIds.includes(desired.id)) {
        throw new Error(
          `Segment ${desired.id} is not journaled by the expected aborted transaction`,
        );
      }
      if (replacement.status !== "active") {
        throw new Error(`Segment ${desired.id} replacement transaction is not active`);
      }
      if (desired.commitOrdinal !== replacement.pendingSegmentIds.length) {
        throw new Error(`Replacement segment commit ordinal is not the next journal ordinal`);
      }
      if (replacement.pendingSegmentIds.includes(desired.id)) {
        throw new Error(
          `Segment ${desired.id} is already journaled by the replacement transaction`,
        );
      }
      const availableBlocks = new Set(replacement.pendingBlockIds);
      if (replacement.snapshotVersion !== null) {
        const manifestValue: unknown = await requestResult(
          transaction.objectStore("manifests").get(replacement.snapshotVersion),
        );
        if (manifestValue === undefined) {
          throw new Error(
            `Replacement transaction snapshot is missing: ${String(replacement.snapshotVersion)}`,
          );
        }
        const snapshotBlocks = await resolveManifestBlockSetInTransaction(
          transaction.objectStore("catalog"),
          asStoredManifestRecord(manifestValue, replacement.snapshotVersion).version,
        );
        for (const id of snapshotBlocks) availableBlocks.add(id);
      }
      const unavailableBlock = segmentBlockIds(desired).find((id) => !availableBlocks.has(id));
      if (unavailableBlock !== undefined) {
        throw new Error(
          `Replacement segment ${desired.id} references unavailable block ${unavailableBlock}`,
        );
      }
      const missingBlock = (
        await Promise.all(
          segmentBlockIds(desired).map((id) =>
            requestResult(transaction.objectStore("blocks").getKey(id)),
          ),
        )
      ).findIndex((key) => key === undefined);
      if (missingBlock >= 0) {
        throw new Error(
          `Replacement segment references missing block: ${segmentBlockIds(desired)[missingBlock] ?? ""}`,
        );
      }
      await assertSegmentTargetsCurrentTable(transaction.objectStore("catalog"), desired);

      if ((await findReadableManifestBlock(transaction, segmentBlockIds(stored))) !== undefined) {
        throw new Error(`Segment ${desired.id} is still reachable from a readable manifest`);
      }
      await visitObjectStoreSequentially(transactionStore, (candidateValue, key) => {
        if (typeof key !== "string") throw corruption("transactions", "record key is invalid");
        const candidate = asTransactionRecord(candidateValue, key);
        if (
          candidate.id !== aborted.id &&
          candidate.id !== replacement.id &&
          candidate.status === "active" &&
          candidate.pendingSegmentIds.includes(desired.id)
        ) {
          throw new Error(
            `Segment ${desired.id} is referenced by active transaction ${candidate.id}`,
          );
        }
      });
      const linkedJobIds = new Set<string>();
      await visitObjectStoreSequentially(transaction.objectStore("gc"), (jobValue, key) => {
        const job = asCompactionJobAtMaintenanceKey(jobValue, key);
        if (job === undefined || isTerminalCompactionJob(job)) return;
        const references =
          job.sourceSegmentIds.includes(desired.id) ||
          compactionOutputSegmentIds(job).includes(desired.id);
        if (!references) return;
        if (
          job.id !== input.compactionJobId ||
          !compactionOutputSegmentIds(job).includes(desired.id) ||
          job.sourceSegmentIds.includes(desired.id) ||
          job.transactionId !== replacement.id
        ) {
          throw new Error(
            `Segment ${desired.id} is referenced by foreign compaction job ${job.id}`,
          );
        }
        linkedJobIds.add(job.id);
      });
      if (!linkedJobIds.has(input.compactionJobId)) {
        throw new Error(`Segment ${desired.id} has no matching replacement compaction job`);
      }

      const abortedSegmentIds = aborted.pendingSegmentIds.filter((id) => id !== desired.id);
      const rebasedAbortedSegments = (
        await Promise.all(
          abortedSegmentIds.map(async (id, commitOrdinal) => {
            const value: unknown = await requestResult(segmentStore.get(id));
            if (value === undefined) {
              throw corruption(`transactions/${aborted.id}`, `pending segment ${id} is missing`);
            }
            const segment = asSegmentRecord(value);
            if (segment.transactionId !== aborted.id) {
              throw corruption(
                `transactions/${aborted.id}`,
                `pending segment ${id} belongs to another transaction`,
              );
            }
            return segment.commitOrdinal === commitOrdinal
              ? undefined
              : { previous: segment, next: { ...segment, commitOrdinal } };
          }),
        )
      ).filter(
        (entry): entry is { previous: SegmentRecord; next: SegmentRecord } => entry !== undefined,
      );
      const updatedAborted: TransactionRecord = {
        ...aborted,
        pendingSegmentIds: abortedSegmentIds,
        revision: incrementSafeInteger(aborted.revision, "Transaction revision"),
        updatedAt: input.updatedAt,
      };
      const updatedReplacement: TransactionRecord = {
        ...replacement,
        pendingSegmentIds: [...replacement.pendingSegmentIds, desired.id],
        revision: incrementSafeInteger(replacement.revision, "Transaction revision"),
        updatedAt: input.updatedAt,
      };
      assertTransactionArtifactJournalLimits(
        updatedReplacement.pendingBlockIds,
        updatedReplacement.pendingSegmentIds,
      );
      const statistics = transaction.objectStore("statistics");
      await updateTransactionResourceLedger(statistics, aborted.id, {
        blockCount: 0,
        segmentCount: -1,
        retainedBytes: 0,
      });
      await updateTransactionResourceLedger(statistics, replacement.id, {
        blockCount: 0,
        segmentCount: 1,
        retainedBytes: 0,
      });
      await updateRecordResourceLedger(statistics, {
        segments: [{ previous: stored, next: desired }, ...rebasedAbortedSegments],
      });
      segmentStore.put(desired, desired.id);
      for (const { next } of rebasedAbortedSegments) segmentStore.put(next, next.id);
      transactionStore.put(updatedAborted, updatedAborted.id);
      transactionStore.put(updatedReplacement, updatedReplacement.id);
      await transactionDone(transaction);
      return structuredClone(updatedReplacement);
    } catch (error) {
      abortIfActive(transaction);
      await ignoreAbort(transaction);
      throw error;
    }
  }

  async reserveRowIds(tableId: string, count: number): Promise<RowIdRange> {
    validateId(tableId, "Table ID");
    validateCount(count);
    const transaction = this.#transaction("catalog", "readwrite");
    try {
      const store = transaction.objectStore("catalog");
      if ((await readDeclaredTable(store, tableId)) === undefined) {
        throw new Error(`Row ID reservation has no table: ${tableId}`);
      }
      const key = `${ROW_ID_PREFIX}${tableId}`;
      const current = asOptionalCounter(await requestResult<unknown>(store.get(key)), key);
      const start = current ?? 1n;
      const endExclusive = start + BigInt(count);
      assertCounterEndInRange(endExclusive, MAX_ROW_ID_EXCLUSIVE_END, "Row ID reservation");
      store.put(endExclusive, key);
      await transactionDone(transaction);
      return { start, endExclusive };
    } catch (error) {
      abortIfActive(transaction);
      await ignoreAbort(transaction);
      throw error;
    }
  }

  async reserveAutoIncrement(
    tableId: string,
    columnId: string,
    count: number,
    atLeast?: bigint,
  ): Promise<RowIdRange> {
    validateId(tableId, "Table ID");
    validateId(columnId, "Column ID");
    validateAutoIncrementReservation(count, atLeast);
    const transaction = this.#transaction("catalog", "readwrite");
    try {
      const store = transaction.objectStore("catalog");
      await assertDeclaredAutoIncrementColumn(store, tableId, columnId);
      const key = `${AUTO_INCREMENT_PREFIX}${tableId}/${columnId}`;
      const current = asOptionalCounter(await requestResult<unknown>(store.get(key)), key);
      const start = maxBigInt(current ?? 1n, atLeast ?? 1n);
      const endExclusive = start + BigInt(count);
      assertCounterEndInRange(
        endExclusive,
        MAX_AUTO_INCREMENT_EXCLUSIVE_END,
        "Auto-increment reservation",
      );
      store.put(endExclusive, key);
      await transactionDone(transaction);
      return { start, endExclusive };
    } catch (error) {
      abortIfActive(transaction);
      await ignoreAbort(transaction);
      throw error;
    }
  }

  async getExistingUniqueKeys(tableId: string, keyTokens: readonly string[]): Promise<string[]> {
    validateId(tableId, "Table ID");
    assertStorageBulkReadItems(keyTokens, "Unique-key read");
    const tokens = [...new Set(keyTokens)];
    const transaction = this.#transaction("catalog", "readonly");
    const store = transaction.objectStore("catalog");
    const versionValue: unknown = await requestResult(store.get(CURRENT_MANIFEST_KEY));
    const version = asOptionalManifestVersion(versionValue, CURRENT_MANIFEST_KEY);
    const cache = this.#uniqueKeyCache;
    if (cache?.tableId === tableId && cache.version === version) {
      await transactionDone(transaction);
      return tokens.filter((token) => cache.present.has(token)).sort();
    }
    const rawIndex: unknown = await requestResult(store.get(uniqueKeyChunkIndexKey(tableId)));
    if (rawIndex === undefined && !(await uniqueKeyNamespaceIsActive(store, tableId))) {
      await transactionDone(transaction);
      return [];
    }
    const { existing } = await readChunkedUniqueKeys(store, tableId, tokens, { value: rawIndex });
    await transactionDone(transaction);
    return [...existing].sort();
  }

  async getCurrentManifestVersion(): Promise<number | null> {
    const transaction = this.#transaction("catalog", "readonly");
    const value: unknown = await requestResult(
      transaction.objectStore("catalog").get(CURRENT_MANIFEST_KEY),
    );
    await transactionDone(transaction);
    return asOptionalManifestVersion(value, CURRENT_MANIFEST_KEY) ?? null;
  }

  /**
   * The freshness probe runs once per query, so its latency is a floor under every read the
   * database serves. Both values are final the moment their reads land: a read-only
   * transaction has nothing left to commit, and waiting for its `complete` event only costs
   * another turn of the event loop — measured at roughly 40% of the probe in Chromium — while
   * changing nothing about what was read. The reads still race the transaction's failure
   * events, so an abort surfaces as a rejection here instead of hanging.
   */
  async getCatalogProbe(): Promise<CatalogProbe> {
    const transaction = this.#transaction("catalog", "readonly");
    const catalog = transaction.objectStore("catalog");
    const [versionValue, epochValue, schemaEpochValue] = await Promise.race([
      Promise.all([
        requestResult<unknown>(catalog.get(CURRENT_MANIFEST_KEY)),
        requestResult<unknown>(catalog.get(CATALOG_EPOCH_KEY)),
        requestResult<unknown>(catalog.get(SCHEMA_EPOCH_KEY)),
      ]),
      transactionFailure(transaction),
    ]);
    return {
      manifestVersion: asOptionalManifestVersion(versionValue, CURRENT_MANIFEST_KEY) ?? null,
      catalogEpoch: asCatalogEpoch(epochValue),
      schemaEpoch: asSchemaEpoch(schemaEpochValue),
    };
  }

  async getQueryCatalogState(names: readonly string[]): Promise<QueryCatalogState> {
    assertStorageBulkReadItems(names, "Query catalog table batch");
    for (const name of names) validateCatalogName(name, "Table name");
    const transaction = this.#transaction(["catalog", "segments", "transactions"], "readonly");
    try {
      const catalog = transaction.objectStore("catalog");
      const [versionValue, epochValue, ...rawIds] = await Promise.all([
        requestResult<unknown>(catalog.get(CURRENT_MANIFEST_KEY)),
        requestResult<unknown>(catalog.get(CATALOG_EPOCH_KEY)),
        ...names.map((name) => requestResult<unknown>(catalog.get(`${TABLE_NAME_PREFIX}${name}`))),
      ]);
      const ids = rawIds.map((rawId, index) => {
        if (rawId !== undefined && !isStorageId(rawId)) {
          throw corruption(
            `${TABLE_NAME_PREFIX}${names[index] ?? ""}`,
            "table name pointer is invalid",
          );
        }
        return typeof rawId === "string" ? rawId : undefined;
      });
      const rawTables = await Promise.all(
        ids.map((id) =>
          id === undefined
            ? Promise.resolve(undefined)
            : requestResult<unknown>(catalog.get(`${TABLE_ID_PREFIX}${id}`)),
        ),
      );
      const tables = rawTables.map((value, index) => {
        const id = ids[index];
        if (value === undefined || id === undefined) return undefined;
        const record = asTableRecord(value, `${TABLE_ID_PREFIX}${id}`);
        if (record.id !== id || record.name !== names[index]) {
          throw corruption(`${TABLE_NAME_PREFIX}${names[index] ?? ""}`, "pointer mismatch");
        }
        return record;
      });
      const segmentIndex = transaction.objectStore("segments").index(SEGMENT_TABLE_INDEX);
      const segments: SegmentRecord[] = [];
      for (const table of tables) {
        if (table === undefined) continue;
        let cursor: string | null = null;
        do {
          const page = await readTableSegmentPage(segmentIndex, table.id, cursor, 1_024);
          segments.push(...page);
          cursor = page.length === 1_024 ? (page.at(-1)?.id ?? null) : null;
        } while (cursor !== null);
      }
      segments.sort((left, right) => left.id.localeCompare(right.id));
      const transactionIds = [...new Set(segments.map((segment) => segment.transactionId))];
      const transactionStore = transaction.objectStore("transactions");
      const transactions: TransactionRecord[] = [];
      for (let start = 0; start < transactionIds.length; start += 64) {
        const window = transactionIds.slice(start, start + 64);
        const values = await Promise.all(
          window.map((id) => requestResult<unknown>(transactionStore.get(id))),
        );
        values.forEach((value, index) => {
          const id = window[index];
          if (value !== undefined && id !== undefined) {
            transactions.push(asTransactionRecord(value, id));
          }
        });
      }
      await transactionDone(transaction);
      return {
        manifestVersion: asOptionalManifestVersion(versionValue, CURRENT_MANIFEST_KEY) ?? null,
        catalogEpoch: asCatalogEpoch(epochValue),
        tables,
        segments,
        transactions,
      };
    } catch (error) {
      abortIfActive(transaction);
      await ignoreAbort(transaction);
      throw error;
    }
  }

  async getCurrentManifest(): Promise<Manifest | undefined> {
    const transaction = this.#transaction(["catalog", "manifests"], "readonly");
    const manifestStore = transaction.objectStore("manifests");
    const versionValue = await requestResult<unknown>(
      transaction.objectStore("catalog").get(CURRENT_MANIFEST_KEY),
    );
    const version = asOptionalManifestVersion(versionValue, CURRENT_MANIFEST_KEY);
    const value: unknown =
      version === undefined ? undefined : await requestResult(manifestStore.get(version));
    const manifest =
      value === undefined
        ? undefined
        : await resolveManifestInTransaction(asStoredManifestRecord(value, version));
    await transactionDone(transaction);
    if (version !== undefined && manifest === undefined)
      throw corruption(CURRENT_MANIFEST_KEY, `points to missing manifest ${String(version)}`);
    return manifest;
  }

  async getManifest(version: number): Promise<Manifest | undefined> {
    const transaction = this.#transaction("manifests", "readonly");
    const manifestStore = transaction.objectStore("manifests");
    const value: unknown = await requestResult<unknown>(manifestStore.get(version));
    const manifest =
      value === undefined
        ? undefined
        : await resolveManifestInTransaction(asStoredManifestRecord(value, version));
    await transactionDone(transaction);
    return manifest;
  }

  async listManifestPage(afterVersion: number | null, limit: number) {
    validatePageLimit(limit);
    const transaction = this.#transaction("manifests", "readonly");
    const manifestStore = transaction.objectStore("manifests");
    const stored = await readCursorPage(
      manifestStore,
      limit,
      (value, key) => {
        if (typeof key !== "number" || !Number.isSafeInteger(key) || key < 0) {
          throw corruption("manifests", "record key is invalid");
        }
        return asStoredManifestRecord(value, key);
      },
      (key) => typeof key === "number" && (afterVersion === null || key > afterVersion),
      afterVersion ?? undefined,
    );
    const records = stored.map(manifestView);
    await transactionDone(transaction);
    return {
      records,
      nextCursor: records.length === limit ? (records.at(-1)?.version ?? null) : null,
    };
  }

  async beginTransaction(input: BeginTransactionInput): Promise<BeginTransactionResult> {
    if (input.record.pendingBlockIds.length > 0 || input.record.pendingSegmentIds.length > 0) {
      throw new TypeError("A fresh transaction cannot begin with pending artifacts");
    }
    if (
      input.record.pendingTable !== undefined ||
      input.record.pendingTableNextRowId !== undefined ||
      input.record.catalogEpochGuard !== undefined ||
      (input.record as TransactionRecord).schemaEpochGuard !== undefined
    ) {
      throw new TypeError("Storage-owned transaction state cannot be supplied at begin");
    }
    const pending =
      input.pendingTable === undefined
        ? undefined
        : {
            record: asIncomingTableRecord(
              structuredClone(input.pendingTable.record),
              `transactions/${input.record.id}/pendingTable`,
            ),
            nextRowId: input.pendingTable.nextRowId,
            expectedCatalogEpoch: input.pendingTable.expectedCatalogEpoch,
          };
    if (pending !== undefined) {
      if (
        typeof pending.nextRowId !== "bigint" ||
        pending.nextRowId < 1n ||
        pending.nextRowId > MAX_ROW_ID_EXCLUSIVE_END
      ) {
        throw new RangeError("Pending table next row ID is invalid");
      }
      if (!Number.isSafeInteger(pending.expectedCatalogEpoch) || pending.expectedCatalogEpoch < 0) {
        throw new TypeError("Pending table catalog epoch is invalid");
      }
    }
    const transaction = this.#transaction(
      ["transactions", "manifests", "catalog", "leases", "statistics"],
      "readwrite",
    );
    try {
      const catalog = transaction.objectStore("catalog");
      const current = asOptionalManifestVersion(
        await requestResult<unknown>(catalog.get(CURRENT_MANIFEST_KEY)),
        CURRENT_MANIFEST_KEY,
      );
      const schemaEpoch = asSchemaEpoch(
        await requestResult<unknown>(catalog.get(SCHEMA_EPOCH_KEY)),
      );
      const record: TransactionRecord = {
        ...structuredClone(input.record),
        snapshotVersion: current ?? null,
        schemaEpochGuard: schemaEpoch,
        ...(pending === undefined
          ? {}
          : {
              pendingTable: pending.record,
              pendingTableNextRowId: pending.nextRowId,
              catalogEpochGuard: pending.expectedCatalogEpoch,
            }),
      };
      validateNewTransactionLifetime(record);
      const normalized = asTransactionRecord(record, record.id);
      await assertSnapshotAvailableInTransaction(transaction, normalized.snapshotVersion);
      await assertActiveTransactionAdmission(transaction.objectStore("transactions"));
      if (pending !== undefined) {
        const epoch = asCatalogEpoch(await requestResult<unknown>(catalog.get(CATALOG_EPOCH_KEY)));
        if (epoch !== pending.expectedCatalogEpoch) {
          throw new TableRecordConflictError(pending.record.id, 0, null);
        }
        const [existingId, existingName] = await Promise.all([
          requestResult(catalog.getKey(`${TABLE_ID_PREFIX}${pending.record.id}`)),
          requestResult(catalog.getKey(`${TABLE_NAME_PREFIX}${pending.record.name}`)),
        ]);
        if (existingId !== undefined || existingName !== undefined) {
          throw new Error(`Table already exists: ${pending.record.name}`);
        }
        await assertCatalogReservationAdmission(transaction, pending.record);
        await assertTableForeignKeysInTransaction(catalog, pending.record);
      }
      await assertPinnedHistoryAdmission(transaction, {
        cutoff: normalized.updatedAt,
        currentVersion: current ?? null,
        replacementTransaction: normalized,
      });
      transaction.objectStore("transactions").add(normalized, normalized.id);
      let rowIds: RowIdRange | undefined;
      if (input.reserveRowIds !== undefined) {
        validateCount(input.reserveRowIds.count);
        if ((await readDeclaredTable(catalog, input.reserveRowIds.tableId)) === undefined) {
          throw new Error(`Row ID reservation has no table: ${input.reserveRowIds.tableId}`);
        }
        const key = `${ROW_ID_PREFIX}${input.reserveRowIds.tableId}`;
        const currentRowId = asOptionalCounter(await requestResult<unknown>(catalog.get(key)), key);
        const start = currentRowId ?? 1n;
        const endExclusive = start + BigInt(input.reserveRowIds.count);
        assertCounterEndInRange(endExclusive, MAX_ROW_ID_EXCLUSIVE_END, "Row ID reservation");
        catalog.put(endExclusive, key);
        rowIds = { start, endExclusive };
      }
      let autoIncrementValues: RowIdRange | undefined;
      if (input.reserveAutoIncrement !== undefined) {
        const { tableId, columnId, count, atLeast } = input.reserveAutoIncrement;
        validateAutoIncrementReservation(count, atLeast);
        await assertDeclaredAutoIncrementColumn(catalog, tableId, columnId);
        const key = `${AUTO_INCREMENT_PREFIX}${tableId}/${columnId}`;
        const current = asOptionalCounter(await requestResult<unknown>(catalog.get(key)), key);
        const start = maxBigInt(current ?? 1n, atLeast ?? 1n);
        const endExclusive = start + BigInt(count);
        assertCounterEndInRange(
          endExclusive,
          MAX_AUTO_INCREMENT_EXCLUSIVE_END,
          "Auto-increment reservation",
        );
        catalog.put(endExclusive, key);
        autoIncrementValues = { start, endExclusive };
      }
      await transactionDone(transaction);
      return {
        record: structuredClone(normalized),
        ...(rowIds === undefined ? {} : { rowIds }),
        ...(autoIncrementValues === undefined ? {} : { autoIncrementValues }),
      };
    } catch (error) {
      abortIfActive(transaction);
      await ignoreAbort(transaction);
      throw error;
    }
  }

  async createTransaction(record: TransactionRecord): Promise<void> {
    validateNewTransactionLifetime(record);
    if (record.pendingBlockIds.length > 0 || record.pendingSegmentIds.length > 0) {
      throw new TypeError("A fresh transaction cannot begin with pending artifacts");
    }
    if (
      record.pendingTable !== undefined ||
      record.pendingTableNextRowId !== undefined ||
      record.catalogEpochGuard !== undefined
    ) {
      throw new TypeError("Pending tables must be reserved through beginTransaction");
    }
    const transaction = this.#transaction(
      ["transactions", "manifests", "blocks", "catalog", "segments", "statistics", "leases"],
      "readwrite",
    );
    try {
      const catalog = transaction.objectStore("catalog");
      const schemaEpoch = asSchemaEpoch(
        await requestResult<unknown>(catalog.get(SCHEMA_EPOCH_KEY)),
      );
      const normalized = asTransactionRecord(
        {
          ...structuredClone(record),
          ...(record.status === "active" && record.schemaEpochGuard === undefined
            ? { schemaEpochGuard: schemaEpoch }
            : {}),
        },
        record.id,
      );
      if (normalized.schemaEpochGuard !== schemaEpoch) {
        throw new SchemaConflictError(normalized.schemaEpochGuard ?? -1, schemaEpoch);
      }
      await assertSnapshotAvailableInTransaction(transaction, normalized.snapshotVersion);
      await assertPendingArtifactsAvailableInTransaction(transaction, normalized, true, true, true);
      await assertActiveTransactionAdmission(transaction.objectStore("transactions"));
      const currentVersion =
        asOptionalManifestVersion(
          await requestResult<unknown>(catalog.get(CURRENT_MANIFEST_KEY)),
          CURRENT_MANIFEST_KEY,
        ) ?? null;
      await assertPinnedHistoryAdmission(transaction, {
        cutoff: normalized.updatedAt,
        currentVersion,
        replacementTransaction: normalized,
      });
      if (normalized.pendingBlockIds.length > 0 || normalized.pendingSegmentIds.length > 0) {
        let retainedBytes = 0;
        for (const id of normalized.pendingBlockIds) {
          retainedBytes = safeByteSum(
            retainedBytes,
            asStoredBlockMetadata(
              await requestResult<unknown>(
                transaction.objectStore("catalog").get(blockMetadataKey(id)),
              ),
              id,
            ).byteLength,
            "Transaction staged bytes",
          );
        }
        await updateTransactionResourceLedger(
          transaction.objectStore("statistics"),
          normalized.id,
          {
            blockCount: normalized.pendingBlockIds.length,
            segmentCount: normalized.pendingSegmentIds.length,
            retainedBytes,
          },
        );
      }
      transaction.objectStore("transactions").add(normalized, normalized.id);
      await transactionDone(transaction);
    } catch (error) {
      abortIfActive(transaction);
      await ignoreAbort(transaction);
      throw error;
    }
  }

  async getTransaction(id: string): Promise<TransactionRecord | undefined> {
    validateId(id, "Transaction ID");
    const transaction = this.#transaction("transactions", "readonly");
    const value: unknown = await requestResult<unknown>(
      transaction.objectStore("transactions").get(id),
    );
    await transactionDone(transaction);
    return value === undefined ? undefined : asTransactionRecord(value, id);
  }

  async getTransactions(ids: readonly string[]): Promise<Array<TransactionRecord | undefined>> {
    assertStorageBulkReadItems(ids, "Transaction read");
    for (const id of ids) validateId(id, "Transaction ID");
    const transaction = this.#transaction("transactions", "readonly");
    const store = transaction.objectStore("transactions");
    const values = await Promise.all(ids.map((id) => requestResult<unknown>(store.get(id))));
    await transactionDone(transaction);
    return values.map((value, index) =>
      value === undefined ? undefined : asTransactionRecord(value, ids[index]),
    );
  }

  async listTransactionPage(afterId: string | null, limit: number) {
    validatePageLimit(limit);
    const transaction = this.#transaction("transactions", "readonly");
    const records = await readCursorPage(
      transaction.objectStore("transactions"),
      limit,
      (value, key) => {
        if (typeof key !== "string") throw corruption("transactions", "record key is invalid");
        return asTransactionRecord(value, key);
      },
      (key) => typeof key === "string" && (afterId === null || key > afterId),
      afterId ?? undefined,
    );
    await transactionDone(transaction);
    return { records, nextCursor: records.length === limit ? (records.at(-1)?.id ?? null) : null };
  }

  async updateTransaction(
    id: string,
    expectedRevision: number,
    update: TransactionRecordUpdate,
  ): Promise<TransactionRecord> {
    validateId(id, "Transaction ID");
    const transaction = this.#transaction(
      ["transactions", "manifests", "blocks", "catalog", "segments", "statistics"],
      "readwrite",
    );
    const store = transaction.objectStore("transactions");
    const value: unknown = await requestResult<unknown>(store.get(id));
    const current = value === undefined ? undefined : asTransactionRecord(value, id);
    if (current?.revision !== expectedRevision) {
      transaction.abort();
      await ignoreAbort(transaction);
      throw new TransactionRecordConflictError(id, expectedRevision, current?.revision ?? null);
    }
    try {
      assertGenericTransactionUpdateAllowed(current, update);
      const updated = updateTransactionRecord(current, update);
      if (update.snapshotVersion !== undefined) {
        await assertSnapshotAvailableInTransaction(transaction, updated.snapshotVersion);
      }
      await assertPendingArtifactsAvailableInTransaction(
        transaction,
        updated,
        update.pendingBlockIds !== undefined,
        update.pendingSegmentIds !== undefined,
        update.pendingSegmentIds !== undefined,
      );
      const addedBlocks = updated.pendingBlockIds.filter(
        (blockId) => !current.pendingBlockIds.includes(blockId),
      );
      const addedSegments = updated.pendingSegmentIds.filter(
        (segmentId) => !current.pendingSegmentIds.includes(segmentId),
      );
      if (
        (update.pendingBlockIds !== undefined &&
          current.pendingBlockIds.some((blockId) => !updated.pendingBlockIds.includes(blockId))) ||
        (update.pendingSegmentIds !== undefined &&
          current.pendingSegmentIds.some(
            (segmentId) => !updated.pendingSegmentIds.includes(segmentId),
          ))
      ) {
        throw new TypeError("Use rollbackTransactionArtifacts to remove journaled artifacts");
      }
      if (addedBlocks.length > 0 || addedSegments.length > 0) {
        let retainedBytes = 0;
        for (const id of addedBlocks) {
          retainedBytes = safeByteSum(
            retainedBytes,
            asStoredBlockMetadata(
              await requestResult<unknown>(
                transaction.objectStore("catalog").get(blockMetadataKey(id)),
              ),
              id,
            ).byteLength,
            "Staged existing block bytes",
          );
        }
        await updateTransactionResourceLedger(transaction.objectStore("statistics"), current.id, {
          blockCount: addedBlocks.length,
          segmentCount: addedSegments.length,
          retainedBytes,
        });
      }
      store.put(updated, id);
      await transactionDone(transaction);
      return structuredClone(updated);
    } catch (error) {
      abortIfActive(transaction);
      await ignoreAbort(transaction);
      throw error;
    }
  }

  async renewTransaction(input: RenewTransactionInput): Promise<boolean> {
    validateId(input.transactionId, "Transaction ID");
    validateId(input.ownerId, "Transaction owner ID");
    const cutoff = validateBoundedLeaseExpiration(
      input.expiresAtCutoff,
      input.expiresAt,
      "Transaction",
    );
    const transaction = this.#transaction(["transactions", "catalog", "leases"], "readwrite");
    try {
      const store = transaction.objectStore("transactions");
      const value: unknown = await requestResult(store.get(input.transactionId));
      if (value === undefined) {
        await transactionDone(transaction);
        return false;
      }
      const record = asTransactionRecord(value, input.transactionId);
      if (
        record.status !== "active" ||
        record.ownerId !== input.ownerId ||
        Date.parse(record.expiresAt) <= cutoff
      ) {
        await transactionDone(transaction);
        return false;
      }
      const renewed = { ...record, expiresAt: input.expiresAt };
      const currentVersion =
        asOptionalManifestVersion(
          await requestResult<unknown>(
            transaction.objectStore("catalog").get(CURRENT_MANIFEST_KEY),
          ),
          CURRENT_MANIFEST_KEY,
        ) ?? null;
      await assertPinnedHistoryAdmission(transaction, {
        cutoff: input.expiresAtCutoff,
        currentVersion,
        replacementTransaction: renewed,
        excludeTransactionId: record.id,
      });
      store.put(renewed, input.transactionId);
      await transactionDone(transaction);
      return true;
    } catch (error) {
      abortIfActive(transaction);
      await ignoreAbort(transaction);
      throw error;
    }
  }

  async abortTransactionIfExpired(
    input: AbortTransactionIfExpiredInput,
  ): Promise<TransactionRecord | undefined> {
    validateId(input.transactionId, "Transaction ID");
    validateId(input.expectedOwnerId, "Transaction owner ID");
    const cutoff = Date.parse(input.expiresAtCutoff);
    if (!Number.isFinite(cutoff)) {
      throw new TypeError("Transaction expiry cutoff must be valid");
    }
    if (!Number.isFinite(Date.parse(input.updatedAt))) {
      throw new TypeError("Transaction update timestamp must be valid");
    }
    const transaction = this.#transaction("transactions", "readwrite");
    try {
      const store = transaction.objectStore("transactions");
      const value: unknown = await requestResult(store.get(input.transactionId));
      if (value === undefined) {
        await transactionDone(transaction);
        return undefined;
      }
      const current = asTransactionRecord(value, input.transactionId);
      if (
        current.status !== "active" ||
        current.ownerId !== input.expectedOwnerId ||
        Date.parse(current.expiresAt) > cutoff
      ) {
        await transactionDone(transaction);
        return undefined;
      }
      const updated = updateTransactionRecord(current, {
        status: "aborted",
        updatedAt: input.updatedAt,
        committedVersion: null,
      });
      await assertTerminalTransactionAdmission(store);
      store.put(updated, updated.id);
      await transactionDone(transaction);
      return structuredClone(updated);
    } catch (error) {
      abortIfActive(transaction);
      await ignoreAbort(transaction);
      throw error;
    }
  }

  async stageTransactionArtifacts(
    input: StageTransactionArtifactsInput,
  ): Promise<TransactionRecord> {
    assertTransactionArtifactBatchLimits(input.blocks, input.segments);
    validateId(input.transactionId, "Transaction ID");
    const transaction = this.#transaction(
      ["blocks", "catalog", "segments", "statistics", "transactions"],
      "readwrite",
    );
    try {
      const transactionStore = transaction.objectStore("transactions");
      const value: unknown = await requestResult<unknown>(
        transactionStore.get(input.transactionId),
      );
      const current =
        value === undefined ? undefined : asTransactionRecord(value, input.transactionId);
      if (current?.revision !== input.expectedRevision || current.status !== "active") {
        throw new TransactionRecordConflictError(
          input.transactionId,
          input.expectedRevision,
          current?.revision ?? null,
        );
      }
      const ids = new Set<string>();
      for (const block of input.blocks) {
        validateId(block.id, "Block ID");
        assertUnsharedBytes(block.bytes, "Block bytes");
        if (ids.has(block.id)) throw new Error(`Block already exists: ${block.id}`);
        ids.add(block.id);
      }
      const segmentIds = new Set<string>();
      const normalizedSegments: SegmentRecord[] = [];
      for (const segment of input.segments) {
        const normalized = normalizeSegmentRecord(segment);
        asSegmentRecord(normalized);
        assertIncomingSegmentHasColumns(normalized);
        if (segmentIds.has(normalized.id)) {
          throw new Error(`Segment already exists: ${normalized.id}`);
        }
        if (normalized.transactionId !== input.transactionId) {
          throw new Error(`Segment ${normalized.id} belongs to another transaction`);
        }
        const expectedCommitOrdinal = current.pendingSegmentIds.length + normalizedSegments.length;
        if (normalized.commitOrdinal !== expectedCommitOrdinal) {
          throw new TypeError(
            `Segment ${normalized.id} commit ordinal must be ${String(expectedCommitOrdinal)}`,
          );
        }
        await assertSegmentTargetsCurrentTable(
          transaction.objectStore("catalog"),
          normalized,
          current.pendingTable,
        );
        segmentIds.add(normalized.id);
        normalizedSegments.push(normalized);
      }
      const update: TransactionRecordUpdate = {
        pendingBlockIds: [...current.pendingBlockIds, ...input.blocks.map((block) => block.id)],
        pendingSegmentIds: [
          ...current.pendingSegmentIds,
          ...normalizedSegments.map((segment) => segment.id),
        ],
        updatedAt: input.updatedAt,
      };
      if (current.pendingTable !== undefined) {
        let nextRowId = current.pendingTableNextRowId;
        if (nextRowId === undefined) throw corruption("transactions", "pending row state missing");
        for (const segment of normalizedSegments) {
          if (
            segment.tableId !== current.pendingTable.id ||
            segment.level !== 0 ||
            segment.kind !== "insert" ||
            segment.rowIdStart !== nextRowId
          ) {
            throw new TypeError(
              `Pending table segment ${segment.id} does not continue its row-ID allocation`,
            );
          }
          nextRowId = segment.rowIdEndExclusive;
        }
        update.pendingTableNextRowId = nextRowId;
      }
      assertGenericTransactionUpdateAllowed(current, update);
      const updated = updateTransactionRecord(current, update);
      assertTransactionArtifactJournalLimits(updated.pendingBlockIds, updated.pendingSegmentIds);
      const updatedBlockIds = new Set(updated.pendingBlockIds);
      for (const segment of normalizedSegments) {
        const unjournaledBlock = segmentBlockIds(segment).find((id) => !updatedBlockIds.has(id));
        if (unjournaledBlock !== undefined) {
          throw new Error(
            `Segment ${segment.id} references block absent from the transaction journal: ${unjournaledBlock}`,
          );
        }
      }
      // Only previously journaled artifacts need existence probes: the ones added below commit
      // or fail atomically with the journal update itself.
      await assertPendingArtifactsAvailableInTransaction(transaction, current);
      await updateTransactionResourceLedger(
        transaction.objectStore("statistics"),
        input.transactionId,
        {
          blockCount: input.blocks.length,
          segmentCount: normalizedSegments.length,
          retainedBytes: input.blocks.reduce(
            (total, block) => safeByteSum(total, block.bytes.byteLength, "Staged block bytes"),
            0,
          ),
        },
      );
      await updateRecordResourceLedger(transaction.objectStore("statistics"), {
        segments: normalizedSegments.map((segment) => ({ next: segment })),
      });
      const blockStore = transaction.objectStore("blocks");
      const catalog = transaction.objectStore("catalog");
      for (const block of input.blocks) {
        const bytes = compactStructuredCloneBytes(block.bytes, "Block bytes");
        blockStore.add(bytes, block.id);
        catalog.add(
          { byteLength: bytes.byteLength, checksum: crc32(bytes) } satisfies StoredBlockMetadata,
          blockMetadataKey(block.id),
        );
      }
      const segmentStore = transaction.objectStore("segments");
      for (const segment of normalizedSegments) {
        segmentStore.add(segment, segment.id);
      }
      transactionStore.put(updated, input.transactionId);
      await transactionDone(transaction);
      return structuredClone(updated);
    } catch (error) {
      abortIfActive(transaction);
      await ignoreAbort(transaction);
      throw error;
    }
  }

  async rollbackTransactionArtifacts(
    input: RollbackTransactionArtifactsInput,
  ): Promise<TransactionRecord> {
    validateId(input.transactionId, "Transaction ID");
    for (const id of [...input.pendingBlockIds, ...input.removeBlockIds]) {
      validateId(id, "Block ID");
    }
    for (const id of [...input.pendingSegmentIds, ...input.removeSegmentIds]) {
      validateId(id, "Segment ID");
    }
    const transaction = this.#transaction(
      ["blocks", "catalog", "gc", "manifests", "segments", "statistics", "transactions"],
      "readwrite",
    );
    try {
      const transactionStore = transaction.objectStore("transactions");
      const value: unknown = await requestResult(transactionStore.get(input.transactionId));
      const current =
        value === undefined ? undefined : asTransactionRecord(value, input.transactionId);
      if (current?.revision !== input.expectedRevision || current.status !== "active") {
        throw new TransactionRecordConflictError(
          input.transactionId,
          input.expectedRevision,
          current?.revision ?? null,
        );
      }
      const blockPartition = validateArtifactPartition(
        input.pendingBlockIds,
        input.removeBlockIds,
        current.pendingBlockIds,
      );
      const segmentPartition = validateArtifactPartition(
        input.pendingSegmentIds,
        input.removeSegmentIds,
        current.pendingSegmentIds,
      );
      if (!blockPartition || !segmentPartition) {
        throw new TypeError(
          "Savepoint rollback artifacts do not reconstruct the transaction journal",
        );
      }
      const removedBlocks = new Set(input.removeBlockIds);
      const removedSegments = new Set(input.removeSegmentIds);
      const blockStore = transaction.objectStore("blocks");
      let removedBlockBytes = 0;
      for (const id of input.removeBlockIds) {
        if ((await requestResult(blockStore.getKey(id))) === undefined) {
          throw corruption(`blocks/${id}`, "rollback artifact is missing");
        }
        removedBlockBytes = safeByteSum(
          removedBlockBytes,
          asStoredBlockMetadata(
            await requestResult<unknown>(
              transaction.objectStore("catalog").get(blockMetadataKey(id)),
            ),
            id,
          ).byteLength,
          "Rollback artifact bytes",
        );
      }
      const publishedBlock = await findReadableManifestBlock(transaction, input.removeBlockIds);
      if (publishedBlock !== undefined) {
        throw corruption(
          `transactions/${input.transactionId}`,
          `rollback journal names block ${publishedBlock.blockId} reachable by manifest ${String(publishedBlock.version)}`,
        );
      }
      await visitObjectStoreSequentially(transactionStore, (recordValue, key) => {
        if (typeof key !== "string") throw corruption("transactions", "record key is invalid");
        if (key === input.transactionId) return;
        const record = asTransactionRecord(recordValue, key);
        const shared = record.pendingBlockIds.find((id) => removedBlocks.has(id));
        const sharedSegment = record.pendingSegmentIds.find((id) => removedSegments.has(id));
        if (shared !== undefined || sharedSegment !== undefined) {
          throw corruption(
            `transactions/${input.transactionId}`,
            `rollback artifact ${shared ?? sharedSegment ?? "unknown"} is journaled by transaction ${record.id}`,
          );
        }
      });
      const foundRemovedSegments = new Set<string>();
      const removedSegmentRecords: SegmentRecord[] = [];
      const segmentStore = transaction.objectStore("segments");
      await visitObjectStoreSequentially(segmentStore, (segmentValue, key) => {
        if (typeof key !== "string") throw corruption("segments", "record key is invalid");
        const segment = asSegmentRecord(segmentValue);
        if (segment.id !== key) {
          throw corruption(`segments/${key}`, `record declares id ${segment.id}`);
        }
        if (removedSegments.has(key)) {
          if (segment.transactionId !== input.transactionId) {
            throw corruption(
              `transactions/${input.transactionId}`,
              `rollback journal names segment ${key} owned by ${segment.transactionId}`,
            );
          }
          foundRemovedSegments.add(key);
          removedSegmentRecords.push(segment);
          return;
        }
        const referenced = Object.values(segment.columnBlockIds)
          .flat()
          .find((id) => removedBlocks.has(id));
        if (referenced !== undefined) {
          throw corruption(
            `transactions/${input.transactionId}`,
            `rollback block ${referenced} is referenced by retained segment ${key}`,
          );
        }
      });
      const missingSegment = input.removeSegmentIds.find((id) => !foundRemovedSegments.has(id));
      if (missingSegment !== undefined) {
        throw corruption(`segments/${missingSegment}`, "rollback artifact is missing");
      }
      await visitObjectStoreSequentially(transaction.objectStore("gc"), (jobValue, key) => {
        if (typeof key !== "string") throw corruption("gc", "record key is invalid");
        if (key.startsWith(COMPACTION_JOB_KEY_PREFIX)) {
          const job = asCompactionJobEnvelope(jobValue);
          const block = [...job.sourceBlockIds, ...job.outputBlockIds].find((id) =>
            removedBlocks.has(id),
          );
          const segment = [...job.sourceSegmentIds, ...compactionOutputSegmentIds(job)].find((id) =>
            removedSegments.has(id),
          );
          if (block !== undefined || segment !== undefined) {
            throw corruption(
              `transactions/${input.transactionId}`,
              `rollback artifact is referenced by compaction job ${job.id}: ${block ?? segment ?? "unknown"}`,
            );
          }
          return;
        }
        if (key.startsWith(GARBAGE_COLLECTION_JOB_KEY_PREFIX)) {
          const job = asGarbageCollectionJobEnvelope(jobValue);
          const block = job.candidateBlockIds.find((id) => removedBlocks.has(id));
          const segment = job.candidateSegmentIds.find((id) => removedSegments.has(id));
          if (block !== undefined || segment !== undefined) {
            throw corruption(
              `transactions/${input.transactionId}`,
              `rollback artifact is referenced by garbage collection job ${job.id}: ${block ?? segment ?? "unknown"}`,
            );
          }
          return;
        }
        asCompactionJobAtMaintenanceKey(jobValue, key);
      });
      let pendingTableNextRowId: bigint | undefined;
      if (current.pendingTable !== undefined) {
        const retained = await Promise.all(
          input.pendingSegmentIds.map((id) => requestResult<unknown>(segmentStore.get(id))),
        );
        const retainedSegments = retained
          .map((value) => (value === undefined ? undefined : asSegmentRecord(value)))
          .filter(
            (segment): segment is SegmentRecord => segment?.tableId === current.pendingTable?.id,
          );
        pendingTableNextRowId = 1n;
        for (const segment of retainedSegments) {
          if (segment.kind !== "insert" || segment.rowIdStart !== pendingTableNextRowId) {
            throw corruption("transactions", "pending table row IDs are discontinuous");
          }
          pendingTableNextRowId = segment.rowIdEndExclusive;
        }
      }
      const updated = updateTransactionRecord(current, {
        pendingBlockIds: input.pendingBlockIds,
        pendingSegmentIds: input.pendingSegmentIds,
        updatedAt: input.updatedAt,
        ...(pendingTableNextRowId === undefined ? {} : { pendingTableNextRowId }),
      });
      await updateTransactionResourceLedger(
        transaction.objectStore("statistics"),
        input.transactionId,
        {
          blockCount: -input.removeBlockIds.length,
          segmentCount: -input.removeSegmentIds.length,
          retainedBytes: -removedBlockBytes,
        },
      );
      await updateRecordResourceLedger(transaction.objectStore("statistics"), {
        segments: removedSegmentRecords.map((segment) => ({ previous: segment })),
      });
      for (const id of input.removeBlockIds) blockStore.delete(id);
      for (const id of input.removeBlockIds) {
        transaction.objectStore("catalog").delete(blockMetadataKey(id));
      }
      for (const id of input.removeSegmentIds) segmentStore.delete(id);
      transactionStore.put(updated, input.transactionId);
      await transactionDone(transaction);
      return structuredClone(updated);
    } catch (error) {
      abortIfActive(transaction);
      await ignoreAbort(transaction);
      throw error;
    }
  }

  async commitTransaction(input: CommitTransactionInput): Promise<ManifestSummary> {
    validateId(input.transactionId, "Transaction ID");
    if (input.compactionJobId !== undefined) {
      validateId(input.compactionJobId, "Compaction job ID");
    }
    validateCommitFtsChanges(input.ftsChanges);
    transactionCommitDeltaRetainedBytes(input.uniqueKeyChanges ?? [], input.ftsChanges ?? []);
    const storeNames = [
      "blocks",
      "catalog",
      "leases",
      "manifests",
      "statistics",
      "transactions",
      "segments",
      SNAPSHOT_HEADER_STORE,
    ];
    if ((input.removedBlockIds?.length ?? 0) > 0) storeNames.push("gc");
    const transaction = this.#transaction(storeNames, "readwrite");
    try {
      const record = await readActiveTransactionRecord(
        transaction,
        input.transactionId,
        input.expectedTransactionRevision,
      );
      const outcome = await this.#commitInTransaction(transaction, record, input);
      await transactionDone(transaction);
      outcome.settle();
      return outcome.manifest;
    } catch (error) {
      abortIfActive(transaction);
      await ignoreAbort(transaction);
      throw error;
    }
  }

  async writeTransaction(input: WriteTransactionInput): Promise<ManifestSummary> {
    const transactionId =
      "record" in input.transaction ? input.transaction.record.id : input.transaction.id;
    validateId(transactionId, "Transaction ID");
    if (input.compactionJobId !== undefined) {
      validateId(input.compactionJobId, "Compaction job ID");
    }
    validateCommitFtsChanges(input.ftsChanges);
    transactionCommitDeltaRetainedBytes(input.uniqueKeyChanges ?? [], input.ftsChanges ?? []);
    assertTransactionArtifactBatchLimits(input.blocks, input.segments);
    const blockIds = new Set<string>();
    for (const block of input.blocks) {
      validateId(block.id, "Block ID");
      assertUnsharedBytes(block.bytes, "Block bytes");
      if (blockIds.has(block.id)) throw new Error(`Block already exists: ${block.id}`);
      blockIds.add(block.id);
    }
    const segmentIds = new Set<string>();
    const normalizedSegments = input.segments.map((segment) => {
      const normalized = normalizeSegmentRecord(segment);
      asSegmentRecord(normalized);
      assertIncomingSegmentHasColumns(normalized);
      if (segmentIds.has(normalized.id)) {
        throw new Error(`Segment already exists: ${normalized.id}`);
      }
      if (normalized.transactionId !== transactionId) {
        throw new TypeError(`Segment ${normalized.id} belongs to another transaction`);
      }
      segmentIds.add(normalized.id);
      return normalized;
    });
    if (
      "record" in input.transaction &&
      (input.transaction.record.pendingBlockIds.length > 0 ||
        input.transaction.record.pendingSegmentIds.length > 0)
    ) {
      throw new TypeError("A fresh transaction cannot begin with pending artifacts");
    }
    const storeNames = [
      "blocks",
      "catalog",
      "leases",
      "manifests",
      "statistics",
      "transactions",
      "segments",
      SNAPSHOT_HEADER_STORE,
    ];
    if ((input.removedBlockIds?.length ?? 0) > 0) storeNames.push("gc");
    const transaction = this.#transaction(storeNames, "readwrite");
    try {
      const transactionStore = transaction.objectStore("transactions");
      let base: TransactionRecord;
      if ("record" in input.transaction) {
        const id = input.transaction.record.id;
        validateNewTransactionLifetime({
          ...input.transaction.record,
          snapshotVersion: input.expectedManifestVersion,
        });
        if ((await requestResult(transactionStore.getKey(id))) !== undefined) {
          throw new Error(`Transaction already exists: ${id}`);
        }
        // The fresh record pins the version the caller prepared against, so the commit below
        // is the same compare-and-swap a begun transaction would make; a moved manifest is a
        // WriteConflictError with nothing written, record included.
        const current = asOptionalManifestVersion(
          await requestResult<unknown>(
            transaction.objectStore("catalog").get(CURRENT_MANIFEST_KEY),
          ),
          CURRENT_MANIFEST_KEY,
        );
        const actualVersion = current ?? null;
        if (actualVersion !== input.expectedManifestVersion) {
          throw new WriteConflictError(input.expectedManifestVersion, actualVersion);
        }
        base = asTransactionRecord(
          {
            ...structuredClone(input.transaction.record),
            snapshotVersion: actualVersion,
          },
          id,
        );
      } else {
        base = await readActiveTransactionRecord(
          transaction,
          input.transaction.id,
          input.transaction.expectedRevision,
        );
      }
      for (const segment of normalizedSegments) {
        await assertSegmentTargetsCurrentTable(
          transaction.objectStore("catalog"),
          segment,
          base.pendingTable,
        );
      }
      normalizedSegments.forEach((segment, index) => {
        const expectedCommitOrdinal = base.pendingSegmentIds.length + index;
        if (segment.commitOrdinal !== expectedCommitOrdinal) {
          throw new TypeError(
            `Segment ${segment.id} commit ordinal must be ${String(expectedCommitOrdinal)}`,
          );
        }
      });
      const stageUpdate: TransactionRecordUpdate = {
        pendingBlockIds: [...base.pendingBlockIds, ...blockIds],
        pendingSegmentIds: [
          ...base.pendingSegmentIds,
          ...normalizedSegments.map((segment) => segment.id),
        ],
        updatedAt: input.committedAt,
      };
      if (base.pendingTable !== undefined) {
        let nextRowId = base.pendingTableNextRowId;
        if (nextRowId === undefined) {
          throw corruption(`transactions/${base.id}`, "pending table row-ID state is missing");
        }
        for (const segment of normalizedSegments) {
          if (
            segment.tableId !== base.pendingTable.id ||
            segment.level !== 0 ||
            segment.kind !== "insert" ||
            segment.rowIdStart !== nextRowId
          ) {
            throw new TypeError(
              `Pending table segment ${segment.id} does not continue its row-ID allocation`,
            );
          }
          nextRowId = segment.rowIdEndExclusive;
        }
        stageUpdate.pendingTableNextRowId = nextRowId;
      }
      const staged = updateTransactionRecord(base, stageUpdate);
      assertTransactionArtifactJournalLimits(staged.pendingBlockIds, staged.pendingSegmentIds);
      // Only previously journaled artifacts need existence probes; the ones added below commit
      // or fail atomically with everything else.
      await assertPendingArtifactsAvailableInTransaction(transaction, base);
      await updateRecordResourceLedger(transaction.objectStore("statistics"), {
        segments: normalizedSegments.map((segment) => ({ next: segment })),
      });
      const blockStore = transaction.objectStore("blocks");
      const catalog = transaction.objectStore("catalog");
      for (const block of input.blocks) {
        const bytes = compactStructuredCloneBytes(block.bytes, "Block bytes");
        blockStore.add(bytes, block.id);
        catalog.add(
          { byteLength: bytes.byteLength, checksum: crc32(bytes) } satisfies StoredBlockMetadata,
          blockMetadataKey(block.id),
        );
      }
      const segmentStore = transaction.objectStore("segments");
      for (const segment of normalizedSegments) {
        segmentStore.add(segment, segment.id);
      }
      const outcome = await this.#commitInTransaction(transaction, staged, input);
      await transactionDone(transaction);
      outcome.settle();
      return outcome.manifest;
    } catch (error) {
      abortIfActive(transaction);
      await ignoreAbort(transaction);
      throw error;
    }
  }

  /**
   * The commit proper, inside an open readwrite transaction over every commit store, against a
   * record the caller already resolved as active at the expected revision — the journaled
   * record for `commitTransaction`, the freshly staged one for `writeTransaction`. Throws to
   * refuse (the caller aborts the storage transaction); returns the summary plus the cache
   * advances to apply once the transaction has proven durable.
   */
  async #commitInTransaction(
    transaction: IDBTransaction,
    record: TransactionRecord,
    input: Omit<CommitTransactionInput, "transactionId" | "expectedTransactionRevision">,
  ): Promise<{ manifest: ManifestSummary; settle: () => void }> {
    incrementSafeInteger(record.revision, "Transaction revision");
    const transactionStore = transaction.objectStore("transactions");
    const catalog = transaction.objectStore("catalog");
    const schemaEpoch = asSchemaEpoch(await requestResult<unknown>(catalog.get(SCHEMA_EPOCH_KEY)));
    if (record.schemaEpochGuard !== schemaEpoch) {
      throw new SchemaConflictError(record.schemaEpochGuard ?? -1, schemaEpoch);
    }
    const current = asOptionalManifestVersion(
      await requestResult<unknown>(catalog.get(CURRENT_MANIFEST_KEY)),
      CURRENT_MANIFEST_KEY,
    );
    const actualVersion = current ?? null;
    if (actualVersion !== input.expectedManifestVersion) {
      throw new WriteConflictError(input.expectedManifestVersion, actualVersion);
    }

    if (record.snapshotVersion !== input.expectedManifestVersion) {
      throw new Error("Transaction snapshot does not match the expected manifest");
    }
    const pendingTable = record.pendingTable;
    if (pendingTable !== undefined) {
      const epoch = asCatalogEpoch(await requestResult<unknown>(catalog.get(CATALOG_EPOCH_KEY)));
      if (epoch !== record.catalogEpochGuard) {
        throw new TableRecordConflictError(
          pendingTable.id,
          pendingTable.revision,
          (await readDeclaredTable(catalog, pendingTable.id))?.revision ?? null,
        );
      }
      const [existingId, existingName] = await Promise.all([
        requestResult(catalog.getKey(`${TABLE_ID_PREFIX}${pendingTable.id}`)),
        requestResult(catalog.getKey(`${TABLE_NAME_PREFIX}${pendingTable.name}`)),
      ]);
      if (existingId !== undefined || existingName !== undefined) {
        throw new Error(`Table already exists: ${pendingTable.name}`);
      }
      await assertCatalogReservationAdmission(transaction, pendingTable, record.id);
      await assertTableForeignKeysInTransaction(catalog, pendingTable);
      if (record.pendingTableNextRowId === undefined) {
        throw corruption(`transactions/${record.id}`, "pending table row-ID state is missing");
      }
    }

    const manifestStore = transaction.objectStore("manifests");
    const baseRecord =
      input.expectedManifestVersion === null
        ? undefined
        : await (async () => {
            const value: unknown = await requestResult(
              manifestStore.get(input.expectedManifestVersion ?? -1),
            );
            return value === undefined
              ? undefined
              : asStoredManifestRecord(value, input.expectedManifestVersion ?? undefined);
          })();
    if (input.expectedManifestVersion !== null && baseRecord === undefined) {
      throw new Error(`Snapshot manifest is missing: ${String(input.expectedManifestVersion)}`);
    }
    const declaredRemovedBlockIds = input.removedBlockIds ?? [];
    const removedBlockIds = [...new Set(declaredRemovedBlockIds)].sort();
    if (removedBlockIds.length !== declaredRemovedBlockIds.length) {
      throw new TypeError("Compaction retirement block IDs must be unique");
    }
    if (removedBlockIds.length === 0 && input.compactionJobId !== undefined) {
      throw new TypeError("A compaction job cannot be linked without retired blocks");
    }
    if (removedBlockIds.length > 0 && input.compactionJobId === undefined) {
      throw new TypeError("Block retirement requires a compaction job");
    }
    const pendingRemovedBlock = removedBlockIds.find((id) => record.pendingBlockIds.includes(id));
    if (pendingRemovedBlock !== undefined) {
      throw new Error(`Cannot supersede a pending block: ${pendingRemovedBlock}`);
    }
    let baseBlockIdSet: Set<string> | undefined;
    const removedMembership = await manifestBlockMembershipInTransaction(
      catalog,
      input.expectedManifestVersion,
      removedBlockIds,
    );
    const invalidRemovedIndex = removedMembership.findIndex((present) => !present);
    if (invalidRemovedIndex >= 0) {
      throw new Error(
        `Cannot supersede a block outside the transaction snapshot: ${removedBlockIds[invalidRemovedIndex] ?? ""}`,
      );
    }
    if ((input.levelZeroSegmentLimits?.length ?? 0) > 0) {
      baseBlockIdSet = await resolveManifestBlockSetInTransaction(
        catalog,
        input.expectedManifestVersion,
      );
    }
    if (removedBlockIds.length > 0) {
      await assertCompactionRetirementInTransaction(transaction, record, input, removedBlockIds);
    }

    const blockStore = transaction.objectStore("blocks");
    const storedBlockKeys = await Promise.all(
      record.pendingBlockIds.map((id) => requestResult(blockStore.getKey(id))),
    );
    const missingBlockIndex = storedBlockKeys.findIndex((key) => key === undefined);
    if (missingBlockIndex >= 0) {
      throw new Error(
        `Manifest references missing block: ${record.pendingBlockIds[missingBlockIndex] ?? ""}`,
      );
    }

    const segmentStore = transaction.objectStore("segments");
    const pendingSegmentValues: unknown[] = await Promise.all(
      record.pendingSegmentIds.map((id) => requestResult(segmentStore.get(id))),
    );
    const missingSegmentIndex = pendingSegmentValues.findIndex((value) => value === undefined);
    if (missingSegmentIndex >= 0) {
      throw new Error(
        `Transaction references missing segment: ${record.pendingSegmentIds[missingSegmentIndex] ?? ""}`,
      );
    }
    const pendingSegments = pendingSegmentValues.map(asSegmentRecord);
    const foreignSegment = pendingSegments.find((segment) => segment.transactionId !== record.id);
    if (foreignSegment !== undefined) {
      throw new Error(`Segment ${foreignSegment.id} belongs to another transaction`);
    }
    await assertCompactionOutputSegmentsInTransaction(
      transaction,
      record,
      input.compactionJobId,
      pendingSegments,
    );
    await assertLevelZeroSegmentLimits(
      transaction,
      input.levelZeroSegmentLimits ?? [],
      pendingSegments,
      baseBlockIdSet ?? new Set(),
      pendingTable,
    );

    // The single-entry path below is the hot bulk-load path. Multi-entry commits (write scopes
    // and tables with secondary UNIQUE indexes) replay per-namespace changes further down; they
    // retain one complete membership cache when its byte budget allows it.
    const uniqueKeyEntries = input.uniqueKeyChanges ?? [];
    const coveredUniqueNamespaces = new Set(uniqueKeyEntries.map((entry) => entry.tableId));
    const changedTableIds = new Set([
      ...(input.changedTableIds ?? []),
      ...pendingSegments.map((segment) => segment.tableId),
    ]);
    for (const tableId of changedTableIds) {
      const value: unknown = await requestResult(catalog.get(`${TABLE_ID_PREFIX}${tableId}`));
      const changedTable =
        value === undefined
          ? pendingTable?.id === tableId
            ? pendingTable
            : undefined
          : asTableRecord(value, `${TABLE_ID_PREFIX}${tableId}`);
      if (changedTable === undefined) continue;
      for (const [indexId, index] of Object.entries(changedTable.secondaryIndexes ?? {})) {
        if (
          index.uniqueEnforced === true &&
          !coveredUniqueNamespaces.has(secondaryUniqueKeyNamespace(tableId, indexId))
        ) {
          transaction.abort();
          await ignoreAbort(transaction);
          throw new UniqueIndexCoverageError(tableId, index.name);
        }
      }
    }
    const uniqueKeyChanges = uniqueKeyEntries.length === 1 ? uniqueKeyEntries[0] : undefined;
    const keyCache = this.#uniqueKeyCache;
    const keyCacheVersionValid = keyCache?.version === input.expectedManifestVersion;
    const keyCacheValid =
      uniqueKeyChanges !== undefined &&
      keyCacheVersionValid &&
      keyCache.tableId === uniqueKeyChanges.tableId;
    let keyState:
      | {
          existing: Set<string>;
          index: UniqueKeyChunkIndex;
          chunks: UniqueKeyChunk[];
          fullPresent?: Set<string>;
        }
      | undefined;
    if (uniqueKeyChanges !== undefined) {
      let conflictToken: string | undefined;
      if (keyCacheValid) {
        // Steady-state bulk load: membership answered entirely from memory, zero reads.
        const existing = new Set<string>();
        for (const token of uniqueKeyChanges.keyTokens) {
          if (keyCache.present.has(token)) existing.add(token);
        }
        keyState = {
          existing,
          index: keyCache.index,
          chunks: keyCache.chunks,
          fullPresent: keyCache.present,
        };
      } else {
        keyState = await readChunkedUniqueKeys(
          catalog,
          uniqueKeyChanges.tableId,
          uniqueKeyChanges.keyTokens,
        );
      }
      if (uniqueKeyChanges.requireAbsent) {
        conflictToken = uniqueKeyChanges.keyTokens.find((token) => keyState?.existing.has(token));
      }
      if (conflictToken !== undefined) {
        throw new UniqueKeyConflictError(uniqueKeyChanges.tableId, conflictToken);
      }
    }
    /**
     * Multi-entry key work, resolved before the manifest writes: per table, read the union
     * of touched tokens once, then replay the entries in operation order over a working set
     * so in-scope conflicts fail exactly like cross-commit conflicts. The net add/remove
     * per table applies after the manifest version is known.
     */
    const multiKeyWork: Array<{
      tableId: string;
      addedTokens: string[];
      removedTokens: string[];
      index: UniqueKeyChunkIndex;
      chunks: UniqueKeyChunk[];
      fullPresent?: Set<string>;
      usedCache: boolean;
    }> = [];
    if (uniqueKeyEntries.length > 1) {
      const byTable = new Map<string, UniqueKeyChanges[]>();
      for (const entry of uniqueKeyEntries) {
        const list = byTable.get(entry.tableId) ?? [];
        list.push(entry);
        byTable.set(entry.tableId, list);
      }
      for (const [tableId, entries] of byTable) {
        const unionTokens = [...new Set(entries.flatMap((entry) => entry.keyTokens))];
        const usesCache = keyCacheVersionValid && keyCache.tableId === tableId;
        const state = usesCache
          ? {
              existing: new Set(unionTokens.filter((token) => keyCache.present.has(token))),
              index: keyCache.index,
              chunks: keyCache.chunks,
              fullPresent: keyCache.present,
            }
          : await readChunkedUniqueKeys(catalog, tableId, unionTokens);
        const present = state.existing;
        const original = new Set(present);
        for (const entry of entries) {
          for (const token of entry.keyTokens) {
            if (entry.remove === true) {
              present.delete(token);
              continue;
            }
            if (entry.requireAbsent && present.has(token)) {
              throw new UniqueKeyConflictError(tableId, token);
            }
            present.add(token);
          }
        }
        multiKeyWork.push({
          tableId,
          addedTokens: unionTokens.filter((token) => present.has(token) && !original.has(token)),
          removedTokens: unionTokens.filter((token) => !present.has(token) && original.has(token)),
          index: state.index,
          chunks: state.chunks,
          ...(state.fullPresent === undefined ? {} : { fullPresent: state.fullPresent }),
          usedCache: usesCache,
        });
      }
    }

    const journalBlockIds = [...record.pendingBlockIds];
    const journalMembership = await manifestBlockMembershipInTransaction(
      catalog,
      input.expectedManifestVersion,
      journalBlockIds,
    );
    const addedBlockIds = journalBlockIds.filter((_, index) => journalMembership[index] !== true);
    const [addedMetadata, removedMetadata] = await Promise.all([
      Promise.all(
        addedBlockIds.map((id) =>
          requestResult<unknown>(catalog.get(blockMetadataKey(id))).then((value) =>
            asStoredBlockMetadata(value, id),
          ),
        ),
      ),
      Promise.all(
        removedBlockIds.map((id) =>
          requestResult<unknown>(catalog.get(blockMetadataKey(id))).then((value) =>
            asStoredBlockMetadata(value, id),
          ),
        ),
      ),
    ]);
    const addedBytes = addedMetadata.reduce(
      (total, entry) => safeByteSum(total, entry.byteLength, "Manifest added bytes"),
      0,
    );
    const removedBytes = removedMetadata.reduce(
      (total, entry) => safeByteSum(total, entry.byteLength, "Manifest removed bytes"),
      0,
    );
    const liveBlockCount =
      (baseRecord?.liveBlockCount ?? 0) + addedBlockIds.length - removedBlockIds.length;
    const liveBlockBytes = (baseRecord?.liveBlockBytes ?? 0) + addedBytes - removedBytes;
    if (liveBlockCount < 0 || liveBlockBytes < 0) {
      throw corruption("manifests", "live block summary underflowed");
    }
    // A staged segment proves a logical table change even if an ordinary caller supplies an
    // empty or incomplete hint. Only a compaction whose job/source/output provenance was
    // validated above may suppress those physical rewrite segments from the logical change
    // list. Pending-table publication is always a logical catalog/data change.
    const logicallyChangedTableIds = new Set(input.changedTableIds ?? []);
    if (input.compactionJobId === undefined) {
      for (const segment of pendingSegments) logicallyChangedTableIds.add(segment.tableId);
    }
    if (pendingTable !== undefined) logicallyChangedTableIds.add(pendingTable.id);
    const summary: ManifestSummary = {
      version:
        input.expectedManifestVersion === null
          ? 0
          : incrementSafeInteger(input.expectedManifestVersion, "Manifest version"),
      previousVersion: input.expectedManifestVersion,
      createdAt: input.committedAt,
      liveBlockCount,
      liveBlockBytes,
      changedTableIds: canonicalManifestChangedTableIds([...logicallyChangedTableIds]),
    };
    const manifest = summary;
    const committedSegmentChanges = pendingSegments.map((segment) => {
      const next: SegmentRecord = {
        ...segment,
        level: segment.level,
        logicalOrder: segment.level === 0 ? manifest.version : segment.logicalOrder,
      };
      return { previous: segment, next };
    });
    if (pendingTable !== undefined) {
      await updateCatalogResourceLedger(
        transaction.objectStore("statistics"),
        undefined,
        pendingTable,
      );
    }
    await updateRecordResourceLedger(transaction.objectStore("statistics"), {
      manifests: [{ next: manifest }],
      segments: committedSegmentChanges,
    });
    await assertPinnedHistoryAdmission(transaction, {
      cutoff: input.committedAt,
      currentVersion: manifest.version,
      excludeTransactionId: record.id,
      prospectiveRemovedBlockIds: new Set(removedBlockIds),
    });
    const committed = updateTransactionRecord(record, {
      status: "committed",
      committedVersion: manifest.version,
      updatedAt: input.committedAt,
    });
    await assertTerminalTransactionAdmission(transactionStore);
    await updateRetiredHistoryLedger(transaction.objectStore("statistics"), removedBytes);
    await publishManifestBlockDeltaInTransaction(
      catalog,
      manifest.version,
      addedBlockIds,
      addedMetadata,
      removedBlockIds,
    );
    manifestStore.add(manifest satisfies StoredManifestRecord, manifest.version);
    catalog.put(manifest.version, CURRENT_MANIFEST_KEY);
    catalog.delete(SNAPSHOT_FRAME_COMPLETED_KEY);
    await deleteSnapshotFrameRecords(transaction.objectStore(SNAPSHOT_HEADER_STORE), "import");
    if (pendingTable !== undefined) {
      await installPendingTableCatalogRecords(catalog, pendingTable);
      catalog.put(record.pendingTableNextRowId ?? 1n, `${ROW_ID_PREFIX}${pendingTable.id}`);
      await bumpSchemaEpoch(catalog);
    }
    await bumpCatalogEpoch(catalog);
    for (const { next: segment } of committedSegmentChanges) {
      segmentStore.put(segment, segment.id);
    }
    // How the unique-key cache advances once this commit proves durable: `keep` carries the
    // valid cache through with its membership delta applied; `replace` installs newly resolved
    // full knowledge; `drop` forgets a same-table cache we could not keep truthful.
    let keyCachePlan:
      | { action: "keep"; chunk: UniqueKeyChunk; index: UniqueKeyChunkIndex }
      | {
          action: "replace";
          present: Set<string>;
          chunks: UniqueKeyChunk[];
          index: UniqueKeyChunkIndex;
        }
      | { action: "drop" }
      | undefined;
    if (uniqueKeyChanges !== undefined) {
      {
        const existing = keyState?.existing ?? new Set<string>();
        const chunk: UniqueKeyChunk = {
          addedTokens:
            uniqueKeyChanges.remove === true
              ? []
              : uniqueKeyChanges.keyTokens.filter((token) => !existing.has(token)),
          removedTokens:
            uniqueKeyChanges.remove === true
              ? uniqueKeyChanges.keyTokens.filter((token) => existing.has(token))
              : [],
        };
        if (chunk.addedTokens.length > 0 || chunk.removedTokens.length > 0) {
          const tableId = uniqueKeyChanges.tableId;
          const index = keyState?.index ?? { versions: [], hasBase: false };
          // A partitioned point read proves only the requested tokens. In particular, an empty
          // `chunks` array does not mean a base-less namespace is empty: it means the reader did
          // not retain every durable tail part. Seed a complete-membership cache only when the
          // read explicitly proved completeness (currently an actually empty namespace).
          const priorPresent = keyState?.fullPresent;
          if (index.versions.length + 1 > UNIQUE_KEY_TAIL_CHUNK_LIMIT) {
            const nextIndex = await foldUniqueMembershipGeneration(
              catalog,
              tableId,
              index,
              manifest.version,
              chunk,
            );
            let cachePresent = priorPresent;
            if (cachePresent !== undefined) {
              if (cachePresent === keyState?.fullPresent) cachePresent = new Set(cachePresent);
              applyChunk(cachePresent, chunk);
            }
            keyCachePlan =
              cachePresent === undefined
                ? { action: "drop" }
                : { action: "replace", present: cachePresent, chunks: [], index: nextIndex };
          } else {
            const chunkParts = writeUniqueKeyTailParts(catalog, tableId, manifest.version, chunk);
            const nextIndex: UniqueKeyChunkIndex = {
              versions: [...index.versions, manifest.version],
              hasBase: index.hasBase,
              // The base is untouched here, so its recorded size still describes it. Dropping
              // it would cost the next fold its incremental path.
              ...uniqueKeyBaseIndexFields(index),
            };
            catalog.put(nextIndex, uniqueKeyChunkIndexKey(tableId));
            if (keyCacheValid) {
              keyCachePlan = { action: "keep", chunk, index: nextIndex };
            } else if (keyState?.fullPresent !== undefined) {
              const nextPresent = new Set(keyState.fullPresent);
              applyChunk(nextPresent, chunk);
              keyCachePlan = {
                action: "replace",
                present: nextPresent,
                chunks: [...keyState.chunks, ...chunkParts],
                index: nextIndex,
              };
            } else if (priorPresent !== undefined) {
              applyChunk(priorPresent, chunk);
              keyCachePlan = {
                action: "replace",
                present: priorPresent,
                chunks: [...(keyState?.chunks ?? []), ...chunkParts],
                index: nextIndex,
              };
            } else {
              keyCachePlan = { action: "drop" };
            }
          }
        }
      }
    }
    let multiKeyCachePlan:
      | {
          action: "advance";
          tableId: string;
          chunk?: UniqueKeyChunk;
          chunks: UniqueKeyChunk[];
          index: UniqueKeyChunkIndex;
        }
      | {
          action: "replace";
          tableId: string;
          present: Set<string>;
          chunks: UniqueKeyChunk[];
          index: UniqueKeyChunkIndex;
        }
      | undefined;
    const rememberMultiKeyState = (
      work: (typeof multiKeyWork)[number],
      state: { chunks: UniqueKeyChunk[]; index: UniqueKeyChunkIndex },
      chunk?: UniqueKeyChunk,
      replacement?: Set<string>,
    ): void => {
      if (work.usedCache) {
        multiKeyCachePlan =
          replacement === undefined
            ? { action: "advance", tableId: work.tableId, ...state, ...(chunk ? { chunk } : {}) }
            : { action: "replace", tableId: work.tableId, present: replacement, ...state };
        return;
      }
      if (multiKeyCachePlan !== undefined || work.fullPresent === undefined) return;
      const present = replacement ?? new Set(work.fullPresent);
      if (replacement === undefined && chunk !== undefined) applyChunk(present, chunk);
      multiKeyCachePlan = { action: "replace", tableId: work.tableId, present, ...state };
    };
    for (const work of multiKeyWork) {
      if (work.addedTokens.length === 0 && work.removedTokens.length === 0) {
        rememberMultiKeyState(work, { chunks: work.chunks, index: work.index });
        continue;
      }
      const chunk: UniqueKeyChunk = {
        addedTokens: work.addedTokens,
        removedTokens: work.removedTokens,
      };
      if (work.index.versions.length + 1 <= UNIQUE_KEY_TAIL_CHUNK_LIMIT) {
        const nextIndex: UniqueKeyChunkIndex = {
          versions: [...work.index.versions, manifest.version],
          hasBase: work.index.hasBase,
          ...uniqueKeyBaseIndexFields(work.index),
        };
        const chunkParts = writeUniqueKeyTailParts(catalog, work.tableId, manifest.version, chunk);
        // Appending a tail chunk leaves the base alone, so its recorded size still holds.
        catalog.put(nextIndex, uniqueKeyChunkIndexKey(work.tableId));
        rememberMultiKeyState(
          work,
          { chunks: [...work.chunks, ...chunkParts], index: nextIndex },
          chunk,
        );
        continue;
      }

      const nextIndex = await foldUniqueMembershipGeneration(
        catalog,
        work.tableId,
        work.index,
        manifest.version,
        chunk,
      );
      let present = work.fullPresent;
      if (present !== undefined) {
        if (work.usedCache) present = new Set(present);
        applyChunk(present, chunk);
      }
      rememberMultiKeyState(work, { chunks: [], index: nextIndex }, undefined, present);
    }
    // Full-text deltas apply atomically with the publish; a stale writer (one that committed
    // segments to an indexed table without deltas) flips the affected columns to "invalid"
    // instead of failing the data commit — the index self-heals through a rebuild.
    const changedFtsTableIds = new Set(pendingSegments.map((segment) => segment.tableId));
    const scalarChangedTableIds = new Set(manifest.changedTableIds);
    for (const tableId of changedFtsTableIds) {
      const tableValue: unknown = await requestResult(catalog.get(`${TABLE_ID_PREFIX}${tableId}`));
      if (tableValue === undefined) continue;
      const forTable = (input.ftsChanges ?? []).find((entry) => entry.tableId === tableId);
      const covered = new Set(forTable?.columns.map((column) => column.columnId) ?? []);
      const table = asTableRecord(tableValue, `${TABLE_ID_PREFIX}${tableId}`);
      const invalidated = invalidateUncoveredFtsColumns(table, covered);
      const withFts = invalidated ?? table;
      const withSecondary = scalarChangedTableIds.has(tableId)
        ? invalidateUncoveredSecondaryIndexes(withFts, covered)
        : undefined;
      if (withSecondary !== undefined || invalidated !== undefined) {
        const replacement = withSecondary ?? invalidated;
        asTableRecord(replacement, `${TABLE_ID_PREFIX}${tableId}`);
        await updateCatalogResourceLedger(
          transaction.objectStore("statistics"),
          table,
          replacement,
        );
        catalog.put(structuredClone(replacement), `${TABLE_ID_PREFIX}${tableId}`);
      }
    }
    const ftsDeltaCounts: NonNullable<ManifestSummary["ftsDeltaCounts"]> = [];
    for (const ftsEntry of input.ftsChanges ?? []) {
      const tableValue: unknown = await requestResult(
        catalog.get(`${TABLE_ID_PREFIX}${ftsEntry.tableId}`),
      );
      if (tableValue === undefined) continue;
      const table = asTableRecord(tableValue, `${TABLE_ID_PREFIX}${ftsEntry.tableId}`);
      const active = activePostingStorageColumnIds(table);
      for (const column of ftsEntry.columns) {
        if (!active.has(column.columnId)) continue;
        const postings = decodeFtsPostingChunk(column.postings);
        if (
          postings === undefined ||
          !Number.isSafeInteger(column.totalTokens) ||
          column.totalTokens < 0
        ) {
          throw new TypeError(`Full-text delta is invalid: ${ftsEntry.tableId}/${column.columnId}`);
        }
        const secondaryOwner = Object.values(table.secondaryIndexes ?? {}).some(
          (index) => index.storageColumnId === column.columnId,
        );
        if (
          ftsPostingTokenCount(postings) !== column.totalTokens ||
          (secondaryOwner &&
            postings.some((posting) => posting.tf.some((frequency) => frequency !== 1)))
        ) {
          throw new TypeError(
            `Full-text delta token count is invalid: ${ftsEntry.tableId}/${column.columnId}`,
          );
        }
        const indexKey = ftsChunkIndexKey(ftsEntry.tableId, column.columnId);
        const rawChunkIndex: unknown = await requestResult(catalog.get(indexKey));
        const chunkIndex = decodeFtsDeltaIndex(rawChunkIndex);
        if (
          (rawChunkIndex !== undefined && chunkIndex === undefined) ||
          (chunkIndex?.versions.length ?? 0) >= MAX_FTS_DELTA_CHUNKS
        ) {
          for (const version of chunkIndex?.versions ?? []) {
            catalog.delete(ftsChunkKey(ftsEntry.tableId, column.columnId, version));
          }
          catalog.delete(indexKey);
          const currentTableValue: unknown = await requestResult(
            catalog.get(`${TABLE_ID_PREFIX}${ftsEntry.tableId}`),
          );
          if (currentTableValue !== undefined) {
            const currentTable = asTableRecord(
              currentTableValue,
              `${TABLE_ID_PREFIX}${ftsEntry.tableId}`,
            );
            const covered = activePostingStorageColumnIds(currentTable);
            covered.delete(column.columnId);
            const invalidated = invalidateUncoveredFtsColumns(currentTable, covered);
            if (invalidated !== undefined) {
              await updateCatalogResourceLedger(
                transaction.objectStore("statistics"),
                currentTable,
                invalidated,
              );
              catalog.put(invalidated, `${TABLE_ID_PREFIX}${ftsEntry.tableId}`);
            }
          }
          ftsDeltaCounts.push({
            tableId: ftsEntry.tableId,
            columnId: column.columnId,
            count: 0,
          });
          continue;
        }
        catalog.put(
          structuredClone({
            postings,
            totalTokens: column.totalTokens,
          } satisfies FtsDeltaChunk),
          ftsChunkKey(ftsEntry.tableId, column.columnId, manifest.version),
        );
        const versions = [...(chunkIndex?.versions ?? []), manifest.version];
        catalog.put({ versions }, indexKey);
        ftsDeltaCounts.push({
          tableId: ftsEntry.tableId,
          columnId: column.columnId,
          count: versions.length,
        });
      }
    }
    if (ftsDeltaCounts.length > 0) manifest.ftsDeltaCounts = ftsDeltaCounts;
    await clearTransactionResourceLedger(transaction.objectStore("statistics"), committed.id);
    transactionStore.put(committed, committed.id);
    const settle = (): void => {
      if (pendingTable !== undefined) {
        this.#tableIdsByName.set(pendingTable.name, pendingTable.id);
      }
      if (uniqueKeyEntries.length > 1) {
        if (multiKeyCachePlan?.action === "advance") {
          const retained = this.#uniqueKeyCache;
          if (
            retained?.tableId === multiKeyCachePlan.tableId &&
            retained.version === input.expectedManifestVersion
          ) {
            if (multiKeyCachePlan.chunk !== undefined) {
              applyChunk(retained.present, multiKeyCachePlan.chunk);
            }
            retained.chunks = multiKeyCachePlan.chunks;
            retained.index = multiKeyCachePlan.index;
            retained.version = manifest.version;
            if (
              uniqueKeyCacheRetainedBytes(retained.present, retained.chunks) >
              this.#uniqueKeyCacheBytes
            ) {
              this.#uniqueKeyCache = undefined;
            }
          } else {
            this.#uniqueKeyCache = undefined;
          }
        } else if (multiKeyCachePlan?.action === "replace") {
          this.#uniqueKeyCache =
            uniqueKeyCacheRetainedBytes(multiKeyCachePlan.present, multiKeyCachePlan.chunks) <=
            this.#uniqueKeyCacheBytes
              ? {
                  version: manifest.version,
                  tableId: multiKeyCachePlan.tableId,
                  present: multiKeyCachePlan.present,
                  chunks: multiKeyCachePlan.chunks,
                  index: multiKeyCachePlan.index,
                }
              : undefined;
        } else {
          this.#uniqueKeyCache = undefined;
        }
      }
      // The unique-key cache follows the same rule as the manifest cache below: advance it only
      // after the durable commit. Commits from this instance cannot silently change another
      // table's key records, so a cache for a different table just moves to the new version.
      if (uniqueKeyChanges !== undefined && keyCachePlan !== undefined) {
        if (keyCachePlan.action === "keep" && keyCacheValid) {
          // In-place delta on the live cache: no copies of a multi-million-token set per batch.
          applyChunk(keyCache.present, keyCachePlan.chunk);
          keyCache.chunks.push(keyCachePlan.chunk);
          keyCache.index = keyCachePlan.index;
          keyCache.version = manifest.version;
          if (
            uniqueKeyCacheRetainedBytes(keyCache.present, keyCache.chunks) >
            this.#uniqueKeyCacheBytes
          ) {
            this.#uniqueKeyCache = undefined;
          }
        } else if (keyCachePlan.action === "replace") {
          this.#uniqueKeyCache =
            uniqueKeyCacheRetainedBytes(keyCachePlan.present, keyCachePlan.chunks) <=
            this.#uniqueKeyCacheBytes
              ? {
                  version: manifest.version,
                  tableId: uniqueKeyChanges.tableId,
                  present: keyCachePlan.present,
                  chunks: keyCachePlan.chunks,
                  index: keyCachePlan.index,
                }
              : undefined;
        } else if (this.#uniqueKeyCache?.tableId === uniqueKeyChanges.tableId) {
          this.#uniqueKeyCache = undefined;
        } else if (this.#uniqueKeyCache !== undefined) {
          this.#uniqueKeyCache.version = manifest.version;
        }
      } else if (
        uniqueKeyChanges !== undefined &&
        this.#uniqueKeyCache?.tableId === uniqueKeyChanges.tableId
      ) {
        // A no-op key write against the cached table: nothing changed on disk, so the cache
        // just moves to the new version.
        this.#uniqueKeyCache.version = manifest.version;
      } else if (this.#uniqueKeyCache !== undefined) {
        this.#uniqueKeyCache.version = manifest.version;
      }
      this.#manifestCache = undefined;
    };
    return { manifest, settle };
  }

  async createLease(record: LeaseRecord): Promise<void> {
    validateBoundedLeaseExpiration(record.createdAt, record.expiresAt, "Lease");
    const normalized = asLeaseRecord(record, record.id);
    if (normalized.kind === "backup") {
      throw new TypeError("Backup leases are created by beginSnapshotFrameExport");
    }
    const transaction = this.#transaction(
      ["blocks", "catalog", "leases", "manifests", "transactions"],
      "readwrite",
    );
    try {
      await assertSnapshotAvailableInTransaction(transaction, normalized.manifestVersion);
      const leaseStore = transaction.objectStore("leases");
      await deleteExpiredReaderLeasePage(leaseStore, normalized.createdAt, 64);
      const leaseCount = await requestResult<number>(leaseStore.count());
      if (leaseCount >= MAX_ACTIVE_LEASES) {
        throw new StorageResourceLimitError("lease", leaseCount + 1, MAX_ACTIVE_LEASES);
      }
      const currentVersion =
        asOptionalManifestVersion(
          await requestResult<unknown>(
            transaction.objectStore("catalog").get(CURRENT_MANIFEST_KEY),
          ),
          CURRENT_MANIFEST_KEY,
        ) ?? null;
      await assertPinnedHistoryAdmission(transaction, {
        cutoff: normalized.createdAt,
        currentVersion,
        replacementLease: normalized,
      });
      leaseStore.add(normalized, normalized.id);
      await transactionDone(transaction);
    } catch (error) {
      abortIfActive(transaction);
      await ignoreAbort(transaction);
      throw error;
    }
  }

  async getLease(id: string): Promise<LeaseRecord | undefined> {
    validateId(id, "Lease ID");
    const transaction = this.#transaction("leases", "readonly");
    const value: unknown = await requestResult(transaction.objectStore("leases").get(id));
    await transactionDone(transaction);
    return value === undefined ? undefined : asLeaseRecord(value, id);
  }

  async listLeases(): Promise<LeaseRecord[]> {
    const transaction = this.#transaction("leases", "readonly");
    const values: LeaseRecord[] = [];
    await visitObjectStoreSequentially(transaction.objectStore("leases"), (value, key) => {
      if (typeof key !== "string") throw corruption("leases", "record key is invalid");
      values.push(asLeaseRecord(value, key));
    });
    await transactionDone(transaction);
    return values.sort((left, right) => left.id.localeCompare(right.id));
  }

  async listExpiredLeasePage(expiresAtCutoff: string, afterCursor: string | null, limit: number) {
    validatePageLimit(limit);
    if (!Number.isFinite(Date.parse(expiresAtCutoff))) {
      throw new TypeError("Lease expiry cutoff must be valid");
    }
    const after = decodeExpiryPageCursor(afterCursor, "Lease page");
    const transaction = this.#transaction("leases", "readonly");
    const records = await readExpiredLeasePage(
      transaction.objectStore("leases").index(LEASE_EXPIRY_INDEX),
      expiresAtCutoff,
      after,
      limit,
    );
    await transactionDone(transaction);
    return {
      records,
      nextCursor:
        records.length === limit
          ? encodeExpiryPageCursor(records.at(-1)?.expiresAt ?? "", records.at(-1)?.id ?? "")
          : null,
    };
  }

  async renewLease(input: RenewLeaseInput): Promise<LeaseRecord> {
    validateId(input.id, "Lease ID");
    const cutoff = validateBoundedLeaseExpiration(input.expiresAtCutoff, input.expiresAt, "Lease");
    const transaction = this.#transaction(
      ["blocks", "catalog", "leases", "manifests", "transactions"],
      "readwrite",
    );
    const store = transaction.objectStore("leases");
    const value: unknown = await requestResult(store.get(input.id));
    const record = value === undefined ? undefined : asLeaseRecord(value, input.id);
    if (record?.revision !== input.expectedRevision) {
      transaction.abort();
      await ignoreAbort(transaction);
      throw new LeaseConflictError(input.id, input.expectedRevision, record?.revision ?? null);
    }
    try {
      if (Date.parse(record.expiresAt) <= cutoff) {
        throw new LeaseExpiredError(input.id, record.expiresAt, input.expiresAtCutoff);
      }
      await assertSnapshotAvailableInTransaction(transaction, record.manifestVersion);
      const renewed = {
        ...record,
        expiresAt: input.expiresAt,
        revision: incrementSafeInteger(record.revision, "Lease revision"),
      };
      const currentVersion =
        asOptionalManifestVersion(
          await requestResult<unknown>(
            transaction.objectStore("catalog").get(CURRENT_MANIFEST_KEY),
          ),
          CURRENT_MANIFEST_KEY,
        ) ?? null;
      await assertPinnedHistoryAdmission(transaction, {
        cutoff: input.expiresAtCutoff,
        currentVersion,
        replacementLease: renewed,
        excludeLeaseId: record.id,
      });
      if (record.kind === "backup") {
        const catalog = transaction.objectStore("catalog");
        const markerValue = await requestResult<unknown>(catalog.get(SNAPSHOT_EXPORT_KEY));
        if (markerValue === undefined) {
          throw corruption(SNAPSHOT_EXPORT_KEY, "backup lease has no active export marker");
        }
        const marker = asSnapshotFrameExportMarker(markerValue);
        assertSnapshotFrameExportLease(marker, record);
        catalog.put(
          { ...marker, expiresAt: renewed.expiresAt, revision: renewed.revision },
          SNAPSHOT_EXPORT_KEY,
        );
      }
      store.put(renewed, input.id);
      await transactionDone(transaction);
      return structuredClone(renewed);
    } catch (error) {
      abortIfActive(transaction);
      await ignoreAbort(transaction);
      throw error;
    }
  }

  async moveLease(input: MoveLeaseInput): Promise<LeaseRecord> {
    validateId(input.id, "Lease ID");
    const cutoff = validateBoundedLeaseExpiration(input.expiresAtCutoff, input.expiresAt, "Lease");
    const transaction = this.#transaction(
      ["leases", "manifests", "blocks", "catalog", "transactions"],
      "readwrite",
    );
    const store = transaction.objectStore("leases");
    const value: unknown = await requestResult(store.get(input.id));
    const record = value === undefined ? undefined : asLeaseRecord(value, input.id);
    if (record?.revision !== input.expectedRevision) {
      transaction.abort();
      await ignoreAbort(transaction);
      throw new LeaseConflictError(input.id, input.expectedRevision, record?.revision ?? null);
    }
    try {
      if (Date.parse(record.expiresAt) <= cutoff) {
        throw new LeaseExpiredError(input.id, record.expiresAt, input.expiresAtCutoff);
      }
      if (record.kind === "backup") {
        throw new Error("Snapshot export leases cannot move to another manifest");
      }
      await assertSnapshotAvailableInTransaction(transaction, input.manifestVersion);
      const moved = {
        ...record,
        manifestVersion: input.manifestVersion,
        expiresAt: input.expiresAt,
        revision: incrementSafeInteger(record.revision, "Lease revision"),
      };
      const currentVersion =
        asOptionalManifestVersion(
          await requestResult<unknown>(
            transaction.objectStore("catalog").get(CURRENT_MANIFEST_KEY),
          ),
          CURRENT_MANIFEST_KEY,
        ) ?? null;
      await assertPinnedHistoryAdmission(transaction, {
        cutoff: input.expiresAtCutoff,
        currentVersion,
        replacementLease: moved,
        excludeLeaseId: record.id,
      });
      store.put(moved, input.id);
      await transactionDone(transaction);
      return structuredClone(moved);
    } catch (error) {
      abortIfActive(transaction);
      await ignoreAbort(transaction);
      throw error;
    }
  }

  async removeLeaseIfExpired(
    id: string,
    expectedRevision: number,
    expiresAtCutoff: string,
  ): Promise<boolean> {
    validateId(id, "Lease ID");
    const cutoff = Date.parse(expiresAtCutoff);
    if (!Number.isFinite(cutoff)) throw new TypeError("Lease expiry cutoff must be valid");
    const transaction = this.#transaction(
      ["catalog", "leases", SNAPSHOT_HEADER_STORE],
      "readwrite",
    );
    const store = transaction.objectStore("leases");
    const value: unknown = await requestResult(store.get(id));
    const record = value === undefined ? undefined : asLeaseRecord(value, id);
    if (record?.revision !== expectedRevision) {
      transaction.abort();
      await ignoreAbort(transaction);
      throw new LeaseConflictError(id, expectedRevision, record?.revision ?? null);
    }
    const expiresAt = Date.parse(record.expiresAt);
    if (!Number.isFinite(expiresAt) || expiresAt > cutoff) {
      await transactionDone(transaction);
      return false;
    }
    if (record.kind === "backup") {
      const catalog = transaction.objectStore("catalog");
      const markerValue = await requestResult<unknown>(catalog.get(SNAPSHOT_EXPORT_KEY));
      if (markerValue === undefined) {
        throw corruption(SNAPSHOT_EXPORT_KEY, "backup lease has no active export marker");
      }
      const marker = asSnapshotFrameExportMarker(markerValue);
      assertSnapshotFrameExportLease(marker, record);
      catalog.delete(SNAPSHOT_EXPORT_KEY);
      await deleteSnapshotFrameRecords(transaction.objectStore(SNAPSHOT_HEADER_STORE), "export");
    }
    store.delete(id);
    await transactionDone(transaction);
    return true;
  }

  async removeLease(input: { id: string; ownerId: string }): Promise<boolean> {
    validateId(input.id, "Lease ID");
    validateId(input.ownerId, "Lease owner ID");
    const transaction = this.#transaction(
      ["catalog", "leases", SNAPSHOT_HEADER_STORE],
      "readwrite",
    );
    const store = transaction.objectStore("leases");
    const value: unknown = await requestResult(store.get(input.id));
    if (value === undefined) {
      await transactionDone(transaction);
      return false;
    }
    const lease = asLeaseRecord(value, input.id);
    if (lease.ownerId !== input.ownerId) {
      transaction.abort();
      await ignoreAbort(transaction);
      throw new LeaseOwnerConflictError(input.id, input.ownerId, lease.ownerId);
    }
    if (lease.kind === "backup") {
      const catalog = transaction.objectStore("catalog");
      const markerValue = await requestResult<unknown>(catalog.get(SNAPSHOT_EXPORT_KEY));
      if (markerValue === undefined) {
        throw corruption(SNAPSHOT_EXPORT_KEY, "backup lease has no active export marker");
      }
      const marker = asSnapshotFrameExportMarker(markerValue);
      assertSnapshotFrameExportLease(marker, lease);
      catalog.delete(SNAPSHOT_EXPORT_KEY);
      await deleteSnapshotFrameRecords(transaction.objectStore(SNAPSHOT_HEADER_STORE), "export");
    }
    store.delete(input.id);
    await transactionDone(transaction);
    return true;
  }

  async createCompactionJob(record: CompactionJobRecord): Promise<void> {
    const normalized = normalizeCompactionJobRecord(record);
    const transaction = this.#transaction(
      ["blocks", "catalog", "gc", "manifests", "segments", "transactions"],
      "readwrite",
    );
    const store = transaction.objectStore("gc");
    const key = compactionJobKey(normalized.id);
    if ((await requestResult(store.getKey(key))) !== undefined) {
      transaction.abort();
      await ignoreAbort(transaction);
      throw new Error(`Compaction job already exists: ${normalized.id}`);
    }
    await assertCompactionJobReferences(transaction, normalized);
    const quota = await readMaintenanceQuota(store);
    if (!isTerminalCompactionJob(normalized)) {
      const activeCompactionJobs = checkedQuotaIncrement(
        quota.activeCompactionJobs,
        "Active compaction job count",
      );
      assertMaintenanceLimit("compaction job", activeCompactionJobs, MAX_ACTIVE_COMPACTION_JOBS);
      const markerKey = activeCompactionKey(normalized.tableId);
      const markerValue: unknown = await requestResult(store.get(markerKey));
      if (markerValue !== undefined) {
        const marker = asActiveCompactionMarker(markerValue, normalized.tableId);
        throw new Error(
          `Compaction job ${marker.jobId} is already active for table ${normalized.tableId}`,
        );
      }
      store.add(
        { kind: "active-compaction", tableId: normalized.tableId, jobId: normalized.id },
        markerKey,
      );
      store.put({ ...quota, activeCompactionJobs }, MAINTENANCE_QUOTA_KEY);
    } else {
      const terminalCompactionJobs = checkedQuotaIncrement(
        quota.terminalCompactionJobs,
        "Terminal compaction job count",
      );
      assertMaintenanceLimit(
        "terminal compaction job",
        terminalCompactionJobs,
        MAX_TERMINAL_COMPACTION_JOB_RECORDS,
      );
      store.put({ ...quota, terminalCompactionJobs }, MAINTENANCE_QUOTA_KEY);
    }
    store.add(compactionJobEnvelope(normalized), key);
    await transactionDone(transaction);
  }

  async getCompactionJob(id: string): Promise<CompactionJobRecord | undefined> {
    validateId(id, "Compaction job ID");
    const transaction = this.#transaction("gc", "readonly");
    const value: unknown = await requestResult(
      transaction.objectStore("gc").get(compactionJobKey(id)),
    );
    await transactionDone(transaction);
    return value === undefined ? undefined : asCompactionJobEnvelope(value);
  }

  async listCompactionJobs(tableId?: string): Promise<CompactionJobRecord[]> {
    if (tableId !== undefined) validateId(tableId, "Table ID");
    const transaction = this.#transaction("gc", "readonly");
    const records: CompactionJobRecord[] = [];
    await visitObjectStoreSequentially(transaction.objectStore("gc"), (value, key) => {
      if (typeof key !== "string") throw corruption("gc", "record key is invalid");
      if (!key.startsWith(COMPACTION_JOB_KEY_PREFIX)) return;
      const record = asCompactionJobEnvelope(value);
      if (key !== compactionJobKey(record.id)) {
        throw corruption(`gc/${key}`, `record declares id ${record.id}`);
      }
      if (tableId === undefined || record.tableId === tableId) records.push(record);
    });
    await transactionDone(transaction);
    return records.sort(
      (left, right) =>
        left.createdAt.localeCompare(right.createdAt) || left.id.localeCompare(right.id),
    );
  }

  async listCompactionJobPage(afterId: string | null, limit: number) {
    validatePageLimit(limit);
    const transaction = this.#transaction("gc", "readonly");
    const records = await readCursorPage(
      transaction.objectStore("gc"),
      limit,
      (value) => asCompactionJobEnvelope(value),
      (key) =>
        typeof key === "string" &&
        key.startsWith(COMPACTION_JOB_KEY_PREFIX) &&
        (afterId === null || key > compactionJobKey(afterId)),
      afterId === null ? COMPACTION_JOB_KEY_PREFIX : compactionJobKey(afterId),
    );
    await transactionDone(transaction);
    return { records, nextCursor: records.length === limit ? (records.at(-1)?.id ?? null) : null };
  }

  async updateCompactionJob(
    id: string,
    expectedRevision: number,
    update: CompactionJobRecordUpdate,
  ): Promise<CompactionJobRecord> {
    validateId(id, "Compaction job ID");
    if (update.state === "cancelled") {
      throw new TypeError("Use cancelCompactionJob to cancel a compaction job");
    }
    const transaction = this.#transaction(
      ["blocks", "catalog", "gc", "manifests", "segments", "transactions"],
      "readwrite",
    );
    const store = transaction.objectStore("gc");
    const key = compactionJobKey(id);
    const value: unknown = await requestResult(store.get(key));
    const current = value === undefined ? undefined : asCompactionJobEnvelope(value);
    if (current?.revision !== expectedRevision) {
      transaction.abort();
      await ignoreAbort(transaction);
      throw new CompactionJobConflictError(id, expectedRevision, current?.revision ?? null);
    }
    if (!isTerminalCompactionJob(current)) await assertActiveCompactionMarker(store, current);
    const updated = updateCompactionJobRecord(current, update);
    await assertCompactionJobReferences(transaction, updated);
    if (!isTerminalCompactionJob(current) && isTerminalCompactionJob(updated)) {
      const quota = await readMaintenanceQuota(store);
      const terminalCompactionJobs = checkedQuotaIncrement(
        quota.terminalCompactionJobs,
        "Terminal compaction job count",
      );
      assertMaintenanceLimit(
        "terminal compaction job",
        terminalCompactionJobs,
        MAX_TERMINAL_COMPACTION_JOB_RECORDS,
      );
      store.put(
        {
          ...quota,
          activeCompactionJobs: checkedQuotaDecrement(
            quota.activeCompactionJobs,
            "active compaction job count",
          ),
          terminalCompactionJobs,
        },
        MAINTENANCE_QUOTA_KEY,
      );
      store.delete(activeCompactionKey(current.tableId));
    }
    store.put(compactionJobEnvelope(updated), key);
    await transactionDone(transaction);
    return structuredClone(updated);
  }

  async cancelCompactionJob(
    id: string,
    expectedRevision: number,
    cancelledAt: string,
  ): Promise<CompactionJobRecord> {
    validateId(id, "Compaction job ID");
    const transaction = this.#transaction(["gc", "transactions"], "readwrite");
    const jobStore = transaction.objectStore("gc");
    const transactionStore = transaction.objectStore("transactions");
    const key = compactionJobKey(id);
    const value: unknown = await requestResult(jobStore.get(key));
    const current = value === undefined ? undefined : asCompactionJobEnvelope(value);
    if (current?.revision !== expectedRevision) {
      transaction.abort();
      await ignoreAbort(transaction);
      throw new CompactionJobConflictError(id, expectedRevision, current?.revision ?? null);
    }
    if (isTerminalCompactionJob(current)) {
      await transactionDone(transaction);
      return structuredClone(current);
    }
    await assertActiveCompactionMarker(jobStore, current);
    incrementSafeInteger(current.revision, "Compaction job revision");

    const transactionValue: unknown =
      current.transactionId === null
        ? undefined
        : await requestResult(transactionStore.get(current.transactionId));
    const linkedTransaction =
      transactionValue === undefined ? undefined : asTransactionRecord(transactionValue);
    let updated: CompactionJobRecord;
    let updatedLinkedTransaction: TransactionRecord | undefined;
    try {
      if (linkedTransaction?.status === "committed") {
        if (linkedTransaction.committedVersion === null) {
          throw new Error(`Committed transaction has no manifest version: ${linkedTransaction.id}`);
        }
        updated = updateCompactionJobRecord(current, {
          state: "published",
          publishedVersion: linkedTransaction.committedVersion,
          updatedAt: cancelledAt,
          error: null,
        });
      } else {
        updated = updateCompactionJobRecord(current, {
          state: "cancelled",
          updatedAt: cancelledAt,
          error: null,
        });
        if (linkedTransaction?.status === "active") {
          await assertTerminalTransactionAdmission(transactionStore);
          updatedLinkedTransaction = updateTransactionRecord(linkedTransaction, {
            status: "aborted",
            updatedAt: cancelledAt,
            committedVersion: null,
          });
        }
      }
    } catch (error) {
      transaction.abort();
      await ignoreAbort(transaction);
      throw error;
    }
    const quota = await readMaintenanceQuota(jobStore);
    const terminalCompactionJobs = checkedQuotaIncrement(
      quota.terminalCompactionJobs,
      "Terminal compaction job count",
    );
    assertMaintenanceLimit(
      "terminal compaction job",
      terminalCompactionJobs,
      MAX_TERMINAL_COMPACTION_JOB_RECORDS,
    );
    if (updatedLinkedTransaction !== undefined) {
      transactionStore.put(updatedLinkedTransaction, updatedLinkedTransaction.id);
    }
    jobStore.put(compactionJobEnvelope(updated), key);
    jobStore.put(
      {
        ...quota,
        activeCompactionJobs: checkedQuotaDecrement(
          quota.activeCompactionJobs,
          "active compaction job count",
        ),
        terminalCompactionJobs,
      },
      MAINTENANCE_QUOTA_KEY,
    );
    jobStore.delete(activeCompactionKey(current.tableId));
    await transactionDone(transaction);
    return structuredClone(updated);
  }

  async removeCompactionJob(id: string): Promise<boolean> {
    validateId(id, "Compaction job ID");
    const transaction = this.#transaction(
      ["blocks", "catalog", "gc", "manifests", "segments", "transactions"],
      "readwrite",
    );
    try {
      const store = transaction.objectStore("gc");
      const key = compactionJobKey(id);
      const value: unknown = await requestResult(store.get(key));
      if (value === undefined) {
        await transactionDone(transaction);
        return false;
      }
      const job = asCompactionJobEnvelope(value);
      if (job.id !== id) throw corruption(`gc/${key}`, `record declares id ${job.id}`);
      if (!isTerminalCompactionJob(job)) {
        throw new Error(`Compaction job ${id} is not terminal`);
      }
      if (!(await compactionJobRemovalPreservesProvenance(transaction, job))) {
        await transactionDone(transaction);
        return false;
      }
      const quota = await readMaintenanceQuota(store);
      store.put(
        {
          ...quota,
          terminalCompactionJobs: checkedQuotaDecrement(
            quota.terminalCompactionJobs,
            "terminal compaction job count",
          ),
        },
        MAINTENANCE_QUOTA_KEY,
      );
      store.delete(key);
      await transactionDone(transaction);
      return true;
    } catch (error) {
      abortIfActive(transaction);
      await ignoreAbort(transaction);
      throw error;
    }
  }

  async createGarbageCollectionJob(
    input: CreateGarbageCollectionJobInput,
  ): Promise<GarbageCollectionJobRecord> {
    const record = createGarbageCollectionJobRecord(input);
    const transaction = this.#transaction(
      ["catalog", "gc", "manifests", "segments", "transactions"],
      "readwrite",
    );
    const gcStore = transaction.objectStore("gc");
    const key = garbageCollectionJobKey(record.id);
    try {
      if ((await requestResult(gcStore.getKey(key))) !== undefined) {
        throw new Error(`Garbage collection job already exists: ${record.id}`);
      }
      const quota = await readMaintenanceQuota(gcStore);
      let activeGarbageCollectionJobs: number | undefined;
      if (record.state !== "completed") {
        activeGarbageCollectionJobs = checkedQuotaIncrement(
          quota.activeGarbageCollectionJobs,
          "Active garbage collection job count",
        );
        assertMaintenanceLimit(
          "garbage collection job",
          activeGarbageCollectionJobs,
          MAX_ACTIVE_GARBAGE_COLLECTION_JOBS,
        );
        const markerValue: unknown = await requestResult(
          gcStore.get(ACTIVE_GARBAGE_COLLECTION_KEY),
        );
        if (markerValue !== undefined) {
          const marker = asActiveGarbageCollectionMarker(markerValue);
          throw new Error(`Garbage collection job ${marker.jobId} is already active`);
        }
      }
      await assertGarbageCollectionCandidateProvenanceInTransaction(transaction, record);
      if (record.state !== "completed") {
        if (activeGarbageCollectionJobs === undefined) {
          throw new Error("Garbage collection admission state is inconsistent");
        }
        gcStore.add(
          { kind: "active-garbage-collection", jobId: record.id },
          ACTIVE_GARBAGE_COLLECTION_KEY,
        );
        gcStore.put({ ...quota, activeGarbageCollectionJobs }, MAINTENANCE_QUOTA_KEY);
      } else {
        const completedGarbageCollectionJobs = checkedQuotaIncrement(
          quota.completedGarbageCollectionJobs,
          "Completed garbage collection job count",
        );
        assertMaintenanceLimit(
          "completed garbage collection job",
          completedGarbageCollectionJobs,
          MAX_COMPLETED_GARBAGE_COLLECTION_JOB_RECORDS,
        );
        gcStore.put({ ...quota, completedGarbageCollectionJobs }, MAINTENANCE_QUOTA_KEY);
      }
      gcStore.add(garbageCollectionJobEnvelope(record), key);
      await transactionDone(transaction);
      return structuredClone(record);
    } catch (error) {
      abortIfActive(transaction);
      await ignoreAbort(transaction);
      throw error;
    }
  }

  async updateGarbageCollectionPlanning(
    input: UpdateGarbageCollectionPlanningInput,
  ): Promise<GarbageCollectionJobRecord> {
    const transaction = this.#transaction(
      ["catalog", "gc", "manifests", "segments", "transactions"],
      "readwrite",
    );
    const gcStore = transaction.objectStore("gc");
    const key = garbageCollectionJobKey(input.jobId);
    try {
      const value: unknown = await requestResult(gcStore.get(key));
      const current = value === undefined ? undefined : asGarbageCollectionJobEnvelope(value);
      if (current?.revision !== input.expectedRevision) {
        throw new GarbageCollectionJobConflictError(
          input.jobId,
          input.expectedRevision,
          current?.revision ?? null,
        );
      }
      if (current.state !== "completed") {
        await assertActiveGarbageCollectionMarker(gcStore, current);
      }
      const updated = updateGarbageCollectionPlanningRecord(current, input);
      await assertGarbageCollectionCandidateProvenanceInTransaction(transaction, updated);
      gcStore.put(garbageCollectionJobEnvelope(updated), key);
      await transactionDone(transaction);
      return structuredClone(updated);
    } catch (error) {
      abortIfActive(transaction);
      await ignoreAbort(transaction);
      throw error;
    }
  }

  async getGarbageCollectionJob(id: string): Promise<GarbageCollectionJobRecord | undefined> {
    validateId(id, "Garbage collection job ID");
    const transaction = this.#transaction("gc", "readonly");
    const value: unknown = await requestResult(
      transaction.objectStore("gc").get(garbageCollectionJobKey(id)),
    );
    await transactionDone(transaction);
    return value === undefined ? undefined : asGarbageCollectionJobEnvelope(value);
  }

  async listGarbageCollectionJobs(): Promise<GarbageCollectionJobRecord[]> {
    const transaction = this.#transaction("gc", "readonly");
    const records: GarbageCollectionJobRecord[] = [];
    await visitObjectStoreSequentially(transaction.objectStore("gc"), (value, key) => {
      if (typeof key !== "string") throw corruption("gc", "record key is invalid");
      if (!key.startsWith(GARBAGE_COLLECTION_JOB_KEY_PREFIX)) return;
      const record = asGarbageCollectionJobEnvelope(value);
      if (key !== garbageCollectionJobKey(record.id)) {
        throw corruption(`gc/${key}`, `record declares id ${record.id}`);
      }
      records.push(record);
    });
    await transactionDone(transaction);
    return records.sort(
      (left, right) =>
        left.createdAt.localeCompare(right.createdAt) || left.id.localeCompare(right.id),
    );
  }

  async listGarbageCollectionJobPage(afterId: string | null, limit: number) {
    validatePageLimit(limit);
    const transaction = this.#transaction("gc", "readonly");
    const records = await readCursorPage(
      transaction.objectStore("gc"),
      limit,
      (value) => asGarbageCollectionJobEnvelope(value),
      (key) =>
        typeof key === "string" &&
        key.startsWith(GARBAGE_COLLECTION_JOB_KEY_PREFIX) &&
        (afterId === null || key > garbageCollectionJobKey(afterId)),
      afterId === null ? GARBAGE_COLLECTION_JOB_KEY_PREFIX : garbageCollectionJobKey(afterId),
    );
    await transactionDone(transaction);
    return { records, nextCursor: records.length === limit ? (records.at(-1)?.id ?? null) : null };
  }

  async runGarbageCollectionStep(
    input: RunGarbageCollectionStepInput,
  ): Promise<GarbageCollectionStepResult> {
    validateGarbageCollectionStepInput(input);
    const transaction = this.#transaction(
      ["gc", "blocks", "segments", "catalog", "manifests", "transactions", "leases", "statistics"],
      "readwrite",
    );
    const gcStore = transaction.objectStore("gc");
    const key = garbageCollectionJobKey(input.jobId);
    try {
      const value: unknown = await requestResult(gcStore.get(key));
      const current = value === undefined ? undefined : asGarbageCollectionJobEnvelope(value);
      if (current?.revision !== input.expectedRevision) {
        throw new GarbageCollectionJobConflictError(
          input.jobId,
          input.expectedRevision,
          current?.revision ?? null,
        );
      }
      if (current.state === "completed") {
        await transactionDone(transaction);
        return emptyGarbageCollectionStep(current);
      }
      await assertActiveGarbageCollectionMarker(gcStore, current);
      incrementSafeInteger(current.revision, "Garbage collection job revision");

      const catalog = transaction.objectStore("catalog");
      const manifestStore = transaction.objectStore("manifests");
      const segmentStore = transaction.objectStore("segments");
      const blockStore = transaction.objectStore("blocks");
      const transactionStore = transaction.objectStore("transactions");
      const currentVersionValue: unknown = await requestResult(catalog.get(CURRENT_MANIFEST_KEY));
      const currentVersion =
        asOptionalManifestVersion(currentVersionValue, CURRENT_MANIFEST_KEY) ?? null;
      const leaseCutoff = Date.parse(current.leaseCutoff);
      await assertGarbageCollectionPinsAvailableInTransaction(
        transaction,
        currentVersion,
        leaseCutoff,
      );

      const prunedManifestVersions: number[] = [];
      const alreadyPrunedManifestVersions: number[] = [];
      const retainedManifestVersions: number[] = [];
      const missingManifestVersions: number[] = [];
      const manifestResourceChanges: Array<{
        previous?: StoredManifestRecord;
        next?: StoredManifestRecord;
      }> = [];
      const reclaimedSegmentIds: string[] = [];
      const reclaimedSegmentRecords: SegmentRecord[] = [];
      const retainedSegmentIds: string[] = [];
      const missingSegmentIds: string[] = [];
      const reclaimedBlockIds: string[] = [];
      const retainedBlockIds: string[] = [];
      const missingBlockIds: string[] = [];
      const reclaimedTransactionIds: string[] = [];
      const retainedTransactionIds: string[] = [];
      const missingTransactionIds: string[] = [];
      let reclaimedBlockBytes = 0;
      let remaining = input.maxItems;
      let manifestIndex = current.cursor.manifestIndex;
      while (remaining > 0 && manifestIndex < current.candidateManifestVersions.length) {
        const version = current.candidateManifestVersions[manifestIndex];
        if (version === undefined) throw new Error("Garbage collection manifest cursor is invalid");
        const manifestValue: unknown = await requestResult(manifestStore.get(version));
        if (manifestValue === undefined) missingManifestVersions.push(version);
        else if (asStoredManifestRecord(manifestValue, version).prunedAt !== undefined) {
          alreadyPrunedManifestVersions.push(version);
        } else if (
          await isManifestVersionPinnedInTransaction(
            transaction,
            version,
            currentVersion,
            leaseCutoff,
          )
        )
          retainedManifestVersions.push(version);
        else {
          // The tombstone keeps the record's full content (checkpoint list or delta) so chains
          // above it keep resolving; it only stops counting as a reachability root.
          const previous = asStoredManifestRecord(manifestValue, version);
          const next = { ...previous, prunedAt: input.updatedAt };
          manifestResourceChanges.push({ previous, next });
          manifestStore.put(next, version);
          prunedManifestVersions.push(version);
        }
        manifestIndex += 1;
        remaining -= 1;
      }

      const prunedManifestVersionSet = new Set(prunedManifestVersions);
      await assertRemainingManifestRecordsAvailable(transaction, prunedManifestVersionSet);
      let segmentIndex = current.cursor.segmentIndex;
      const segmentIdsToExamine =
        manifestIndex === current.candidateManifestVersions.length
          ? current.candidateSegmentIds.slice(segmentIndex, segmentIndex + remaining)
          : [];
      const blockCapacity = Math.max(0, remaining - segmentIdsToExamine.length);
      const blockIdsToExamine =
        manifestIndex === current.candidateManifestVersions.length &&
        segmentIndex + segmentIdsToExamine.length === current.candidateSegmentIds.length
          ? current.candidateBlockIds.slice(
              current.cursor.blockIndex,
              current.cursor.blockIndex + blockCapacity,
            )
          : [];
      const roots = await collectBoundedPhysicalRootsInTransaction(
        transaction,
        segmentIdsToExamine,
        blockIdsToExamine,
        prunedManifestVersionSet,
      );
      while (
        remaining > 0 &&
        manifestIndex === current.candidateManifestVersions.length &&
        segmentIndex < current.candidateSegmentIds.length
      ) {
        const id = current.candidateSegmentIds[segmentIndex];
        if (id === undefined) throw new Error("Garbage collection segment cursor is invalid");
        const segmentValue: unknown = await requestResult(segmentStore.get(id));
        if (segmentValue === undefined) missingSegmentIds.push(id);
        else if (roots.segmentIds.has(id)) retainedSegmentIds.push(id);
        else {
          reclaimedSegmentRecords.push(asSegmentRecord(segmentValue));
          segmentStore.delete(id);
          reclaimedSegmentIds.push(id);
        }
        segmentIndex += 1;
        remaining -= 1;
      }

      if (manifestResourceChanges.length > 0 || reclaimedSegmentRecords.length > 0) {
        await updateRecordResourceLedger(transaction.objectStore("statistics"), {
          manifests: manifestResourceChanges,
          segments: reclaimedSegmentRecords.map((segment) => ({ previous: segment })),
        });
      }

      let blockIndex = current.cursor.blockIndex;
      let transactionIndex = current.cursor.transactionIndex;
      if (
        remaining > 0 &&
        manifestIndex === current.candidateManifestVersions.length &&
        segmentIndex === current.candidateSegmentIds.length &&
        blockIndex === current.candidateBlockIds.length
      ) {
        const candidates = new Set(current.candidateTransactionIds.slice(transactionIndex));
        const segmentOwners = new Set<string>();
        await visitObjectStoreSequentially(segmentStore, (value) => {
          const owner = asSegmentRecord(value).transactionId;
          if (candidates.has(owner)) segmentOwners.add(owner);
        });
        const unfinishedCompactionOwners = new Set<string>();
        await visitObjectStoreSequentially(gcStore, (value, key) => {
          const job = asCompactionJobAtMaintenanceKey(value, key);
          if (job === undefined) return;
          if (
            !isTerminalCompactionJob(job) &&
            job.transactionId !== null &&
            candidates.has(job.transactionId)
          ) {
            unfinishedCompactionOwners.add(job.transactionId);
          }
        });
        while (remaining > 0 && transactionIndex < current.candidateTransactionIds.length) {
          const id = current.candidateTransactionIds[transactionIndex];
          if (id === undefined) {
            throw new Error("Garbage collection transaction cursor is invalid");
          }
          const transactionValue: unknown = await requestResult(transactionStore.get(id));
          const record =
            transactionValue === undefined ? undefined : asTransactionRecord(transactionValue, id);
          const manifestValue: unknown =
            record?.committedVersion === null || record?.committedVersion === undefined
              ? undefined
              : await requestResult(manifestStore.get(record.committedVersion));
          const manifest =
            manifestValue === undefined
              ? undefined
              : asStoredManifestRecord(manifestValue, record?.committedVersion ?? undefined);
          if (record === undefined) missingTransactionIds.push(id);
          else if (
            unfinishedCompactionOwners.has(id) ||
            (record.status === "committed"
              ? (manifest !== undefined && manifest.prunedAt === undefined) || segmentOwners.has(id)
              : record.status === "aborted"
                ? (await anyObjectStoreKeyExists(blockStore, record.pendingBlockIds)) ||
                  (await anyObjectStoreKeyExists(segmentStore, record.pendingSegmentIds))
                : true)
          ) {
            retainedTransactionIds.push(id);
          } else {
            await clearTransactionResourceLedger(transaction.objectStore("statistics"), id);
            transactionStore.delete(id);
            reclaimedTransactionIds.push(id);
          }
          transactionIndex += 1;
          remaining -= 1;
        }
      }

      while (
        remaining > 0 &&
        manifestIndex === current.candidateManifestVersions.length &&
        segmentIndex === current.candidateSegmentIds.length &&
        blockIndex < current.candidateBlockIds.length
      ) {
        const id = current.candidateBlockIds[blockIndex];
        if (id === undefined) throw new Error("Garbage collection block cursor is invalid");
        const blockValue: unknown = await requestResult(blockStore.get(id));
        if (blockValue === undefined) missingBlockIds.push(id);
        else if (roots.blockIds.has(id)) retainedBlockIds.push(id);
        else {
          const bytes = asBytes(blockValue, `blocks/${id}`);
          const provenanceValue: unknown = await requestResult(
            transaction.objectStore("catalog").get(manifestBlockKey(id)),
          );
          if (provenanceValue !== undefined) {
            const provenance = asManifestBlockRecord(provenanceValue, id);
            if (provenance.removedVersion === null) {
              throw corruption(`${MANIFEST_BLOCK}/${id}`, "collector selected a live block");
            }
            await updateRetiredHistoryLedger(
              transaction.objectStore("statistics"),
              -provenance.byteLength,
            );
          }
          blockStore.delete(id);
          transaction.objectStore("catalog").delete(blockMetadataKey(id));
          transaction.objectStore("catalog").delete(manifestBlockKey(id));
          reclaimedBlockIds.push(id);
          reclaimedBlockBytes = safeStorageSum(reclaimedBlockBytes, bytes.byteLength);
        }
        blockIndex += 1;
        remaining -= 1;
      }

      const updated = advanceGarbageCollectionJobRecord(current, {
        examinedManifestCount:
          prunedManifestVersions.length +
          alreadyPrunedManifestVersions.length +
          retainedManifestVersions.length +
          missingManifestVersions.length,
        prunedManifestCount: prunedManifestVersions.length,
        alreadyPrunedManifestCount: alreadyPrunedManifestVersions.length,
        retainedManifestCount: retainedManifestVersions.length,
        missingManifestCount: missingManifestVersions.length,
        examinedSegmentCount:
          reclaimedSegmentIds.length + retainedSegmentIds.length + missingSegmentIds.length,
        reclaimedSegmentCount: reclaimedSegmentIds.length,
        retainedSegmentCount: retainedSegmentIds.length,
        missingSegmentCount: missingSegmentIds.length,
        examinedBlockCount:
          reclaimedBlockIds.length + retainedBlockIds.length + missingBlockIds.length,
        reclaimedBlockCount: reclaimedBlockIds.length,
        retainedBlockCount: retainedBlockIds.length,
        missingBlockCount: missingBlockIds.length,
        reclaimedBlockBytes,
        examinedTransactionCount:
          reclaimedTransactionIds.length +
          retainedTransactionIds.length +
          missingTransactionIds.length,
        reclaimedTransactionCount: reclaimedTransactionIds.length,
        retainedTransactionCount: retainedTransactionIds.length,
        missingTransactionCount: missingTransactionIds.length,
        updatedAt: input.updatedAt,
      });
      gcStore.put(garbageCollectionJobEnvelope(updated), key);
      if (updated.state === "completed") {
        const quota = await readMaintenanceQuota(gcStore);
        const completedGarbageCollectionJobs = checkedQuotaIncrement(
          quota.completedGarbageCollectionJobs,
          "Completed garbage collection job count",
        );
        assertMaintenanceLimit(
          "completed garbage collection job",
          completedGarbageCollectionJobs,
          MAX_COMPLETED_GARBAGE_COLLECTION_JOB_RECORDS,
        );
        gcStore.put(
          {
            ...quota,
            activeGarbageCollectionJobs: checkedQuotaDecrement(
              quota.activeGarbageCollectionJobs,
              "active garbage collection job count",
            ),
            completedGarbageCollectionJobs,
          },
          MAINTENANCE_QUOTA_KEY,
        );
        await requestResult(gcStore.delete(ACTIVE_GARBAGE_COLLECTION_KEY));
      }
      await transactionDone(transaction);
      return {
        job: structuredClone(updated),
        prunedManifestVersions,
        alreadyPrunedManifestVersions,
        retainedManifestVersions,
        missingManifestVersions,
        reclaimedSegmentIds,
        retainedSegmentIds,
        missingSegmentIds,
        reclaimedBlockIds,
        retainedBlockIds,
        missingBlockIds,
        reclaimedBlockBytes,
        reclaimedTransactionIds,
        retainedTransactionIds,
        missingTransactionIds,
      };
    } catch (error) {
      abortIfActive(transaction);
      await ignoreAbort(transaction);
      throw error;
    }
  }

  async removePrunedManifestRecords(maxItems: number): Promise<number> {
    boundedMaintenanceBatchItems(maxItems, "Pruned manifest removal limit");
    const transaction = this.#transaction(["catalog", "manifests", "statistics"], "readwrite");
    const store = transaction.objectStore("manifests");
    const catalog = transaction.objectStore("catalog");
    try {
      const persistedState: unknown = await requestResult(catalog.get(MANIFEST_PRUNE_CLEANUP_KEY));
      let state = asManifestPruneCleanupState(persistedState);
      let budget = maxItems;
      let removed = 0;
      while (budget > 0) {
        if (state.phase === "scan") {
          const scan = await scanManifestPruneBoundary(store, state.afterVersion, budget);
          budget -= scan.visited;
          if (scan.safeBelow !== undefined) {
            state = { phase: "delete", safeBelow: scan.safeBelow, beforeVersion: scan.safeBelow };
            catalog.put(state, MANIFEST_PRUNE_CLEANUP_KEY);
            if (scan.safeBelow === 0) {
              catalog.delete(MANIFEST_PRUNE_CLEANUP_KEY);
              break;
            }
            continue;
          }
          if (scan.reachedEnd) {
            const currentVersion = asOptionalManifestVersion(
              await requestResult<unknown>(catalog.get(CURRENT_MANIFEST_KEY)),
              CURRENT_MANIFEST_KEY,
            );
            if (currentVersion === undefined && (await requestResult(store.count())) === 0) {
              // A never-published database has no readable manifest by definition. Cleanup is
              // already complete; this is not the corrupt "history exists but no current
              // pointer" state that must continue to fail closed.
              catalog.delete(MANIFEST_PRUNE_CLEANUP_KEY);
              break;
            }
            throw corruption("manifests", "no readable manifest remains");
          }
          state = { phase: "scan", afterVersion: scan.afterVersion };
          catalog.put(state, MANIFEST_PRUNE_CLEANUP_KEY);
          break;
        }
        const deletion = await deletePrunedManifestPage(
          store,
          state.safeBelow,
          state.beforeVersion,
          budget,
        );
        if (deletion.removedRecords.length > 0) {
          await updateRecordResourceLedger(transaction.objectStore("statistics"), {
            manifests: deletion.removedRecords.map((record) => ({ previous: record })),
          });
        }
        budget -= deletion.visited;
        removed += deletion.removed;
        if (deletion.reachedEnd) {
          catalog.delete(MANIFEST_PRUNE_CLEANUP_KEY);
          break;
        }
        state = {
          phase: "delete",
          safeBelow: state.safeBelow,
          beforeVersion: deletion.beforeVersion,
        };
        catalog.put(state, MANIFEST_PRUNE_CLEANUP_KEY);
        break;
      }
      await transactionDone(transaction);
      return removed;
    } catch (error) {
      abortIfActive(transaction);
      await ignoreAbort(transaction);
      throw error;
    }
  }

  async removeGarbageCollectionJob(id: string): Promise<void> {
    validateId(id, "Garbage collection job ID");
    const transaction = this.#transaction("gc", "readwrite");
    const store = transaction.objectStore("gc");
    const key = garbageCollectionJobKey(id);
    const value: unknown = await requestResult(store.get(key));
    if (value !== undefined) {
      const job = asGarbageCollectionJobEnvelope(value);
      if (job.id !== id) throw corruption(`gc/${key}`, `record declares id ${job.id}`);
      if (job.state !== "completed") {
        transaction.abort();
        await ignoreAbort(transaction);
        throw new Error(`Garbage collection job ${id} is not completed`);
      }
      const quota = await readMaintenanceQuota(store);
      store.put(
        {
          ...quota,
          completedGarbageCollectionJobs: checkedQuotaDecrement(
            quota.completedGarbageCollectionJobs,
            "completed garbage collection job count",
          ),
        },
        MAINTENANCE_QUOTA_KEY,
      );
      store.delete(key);
    }
    await transactionDone(transaction);
  }

  async getLogicalStorageBytes(): Promise<number> {
    const transaction = this.#transaction([...indexedDbStoreNames], "readonly");
    let total = 0;
    for (const storeName of indexedDbStoreNames) {
      total = safeByteSum(
        total,
        await logicalObjectStoreBytes(transaction.objectStore(storeName)),
        "Logical storage bytes",
      );
    }
    await transactionDone(transaction);
    return total;
  }

  async inspectInterruptedImport(): Promise<InterruptedSnapshotImport | null> {
    const transaction = this.#transaction("catalog", "readonly");
    const catalog = transaction.objectStore("catalog");
    const [currentValue, frameMarkerValue] = await Promise.all([
      requestResult<unknown>(catalog.get(CURRENT_MANIFEST_KEY)),
      requestResult<unknown>(catalog.get(SNAPSHOT_FRAME_IMPORT_KEY)),
    ]);
    const current = asOptionalManifestVersion(currentValue, CURRENT_MANIFEST_KEY);
    if (frameMarkerValue === undefined) {
      await transactionDone(transaction);
      return null;
    }
    const marker = asSnapshotFrameImportMarker(frameMarkerValue);
    if (current !== undefined && !marker.replayCompleted) {
      throw corruption(SNAPSHOT_FRAME_IMPORT_KEY, "journal exists beside a published database");
    }
    await transactionDone(transaction);
    return {
      identity: marker.identity,
      version: marker.version,
      createdAt: marker.createdAt,
      stagedBlockCount: marker.kindItemCounts[SNAPSHOT_FRAME_KINDS.indexOf("block")] ?? 0,
      stagedBytes: marker.stagedBytes,
    };
  }

  async abortInterruptedImport(identity: string): Promise<InterruptedSnapshotImportAbortResult> {
    validateId(identity, "Snapshot import identity");
    const transaction = this.#transaction([...indexedDbStoreNames], "readwrite", {
      allowSnapshotImport: true,
    });
    try {
      const catalog = transaction.objectStore("catalog");
      const [currentValue, frameMarkerValue] = await Promise.all([
        requestResult<unknown>(catalog.get(CURRENT_MANIFEST_KEY)),
        requestResult<unknown>(catalog.get(SNAPSHOT_FRAME_IMPORT_KEY)),
      ]);
      if (asOptionalManifestVersion(currentValue, CURRENT_MANIFEST_KEY) !== undefined) {
        throw new Error("A published database cannot be discarded as an interrupted import");
      }
      if (frameMarkerValue === undefined) {
        throw new Error("There is no interrupted snapshot import");
      }
      const marker = asSnapshotFrameImportMarker(frameMarkerValue);
      if (marker.identity !== identity) {
        throw new Error("Snapshot import identity does not match the interrupted import");
      }
      if (marker.replayCompleted) {
        throw new Error("A completed replay is not an interrupted unpublished import");
      }
      for (const name of indexedDbStoreNames) transaction.objectStore(name).clear();
      transaction.objectStore("gc").add(emptyMaintenanceQuota(), MAINTENANCE_QUOTA_KEY);
      transaction.objectStore("statistics").add(emptyResourceLedger(), RESOURCE_LEDGER_KEY);
      transaction
        .objectStore("statistics")
        .add(emptyCatalogResourceLedger(), CATALOG_RESOURCE_LEDGER_KEY);
      transaction
        .objectStore("statistics")
        .add(emptyRecordResourceLedger(), RECORD_RESOURCE_LEDGER_KEY);
      await transactionDone(transaction);
      this.#tableIdsByName.clear();
      this.#uniqueKeyCache = undefined;
      this.#manifestCache = undefined;
      return {
        identity,
        removedBlockCount: marker.kindItemCounts[SNAPSHOT_FRAME_KINDS.indexOf("block")] ?? 0,
        removedBytes: marker.stagedBytes,
      };
    } catch (error) {
      abortIfActive(transaction);
      await ignoreAbort(transaction);
      throw error;
    }
  }

  async getStorageStats(): Promise<StorageStats> {
    const transaction = this.#transaction([...indexedDbStoreNames], "readonly");
    const [manifestCount, transactionCount, segmentCount, frameMarkerValue, currentValue] =
      await Promise.all([
        requestResult<number>(transaction.objectStore("manifests").count()),
        requestResult<number>(transaction.objectStore("transactions").count()),
        requestResult<number>(transaction.objectStore("segments").count()),
        requestResult<unknown>(transaction.objectStore("catalog").get(SNAPSHOT_FRAME_IMPORT_KEY)),
        requestResult<unknown>(transaction.objectStore("catalog").get(CURRENT_MANIFEST_KEY)),
      ]);
    const currentVersion = asOptionalManifestVersion(currentValue, CURRENT_MANIFEST_KEY);
    const currentManifest =
      currentVersion === undefined
        ? undefined
        : asStoredManifestRecord(
            await requestResult<unknown>(transaction.objectStore("manifests").get(currentVersion)),
            currentVersion,
          );
    let storedBlockBytes = 0;
    let storedBlockCount = 0;
    let blockLogicalBytes = 0;
    await visitObjectStoreSequentially(transaction.objectStore("blocks"), (value, id) => {
      if (typeof id !== "string") throw corruption("blocks", "block key is invalid");
      const bytes = asBytes(value, `blocks/${id}`);
      storedBlockCount = incrementSafeInteger(storedBlockCount, "Stored block count");
      storedBlockBytes = safeByteSum(storedBlockBytes, bytes.byteLength, "Stored block bytes");
      blockLogicalBytes = safeByteSum(
        blockLogicalBytes,
        safeByteSum(logicalStoredBytes(id), logicalStoredBytes(value), "Logical block bytes"),
        "Logical block bytes",
      );
    });
    let liveBlockCount = currentManifest?.liveBlockCount ?? 0;
    let liveBlockBytes = currentManifest?.liveBlockBytes ?? 0;
    let obsoleteBlockCount = storedBlockCount - liveBlockCount;
    let obsoleteBlockBytes = storedBlockBytes - liveBlockBytes;
    if (obsoleteBlockCount < 0 || obsoleteBlockBytes < 0) {
      throw corruption("manifests", "current live-block summary exceeds stored payloads");
    }
    const tempLogicalBytes = await logicalObjectStoreBytes(transaction.objectStore("temp"));
    const snapshotHeaderLogicalBytes = await logicalObjectStoreBytes(
      transaction.objectStore(SNAPSHOT_HEADER_STORE),
    );
    let temporaryBytes = safeByteSum(
      tempLogicalBytes,
      snapshotHeaderLogicalBytes,
      "Temporary bytes",
    );
    if (frameMarkerValue !== undefined) {
      const marker = asSnapshotFrameImportMarker(frameMarkerValue);
      if (!marker.replayCompleted) {
        temporaryBytes = safeByteSum(
          temporaryBytes,
          safeByteSum(liveBlockBytes, obsoleteBlockBytes, "Temporary block bytes"),
          "Temporary bytes",
        );
        liveBlockBytes = 0;
        obsoleteBlockBytes = 0;
        liveBlockCount = 0;
        obsoleteBlockCount = 0;
      }
    }
    let logicalBytes = safeByteSum(
      blockLogicalBytes,
      safeByteSum(tempLogicalBytes, snapshotHeaderLogicalBytes, "Logical temporary bytes"),
      "Logical storage bytes",
    );
    for (const name of indexedDbStoreNames) {
      if (name === "blocks" || name === "temp" || name === SNAPSHOT_HEADER_STORE) continue;
      logicalBytes = safeByteSum(
        logicalBytes,
        await logicalObjectStoreBytes(transaction.objectStore(name)),
        "Logical storage bytes",
      );
    }
    await transactionDone(transaction);
    return {
      backend: "indexeddb",
      logicalBytes,
      physicalBytes: null,
      liveBlockBytes,
      obsoleteBlockBytes,
      liveBlockCount,
      obsoleteBlockCount,
      temporaryBytes,
      walBytes: null,
      checkpointBytes: null,
      orphanBytes: null,
      manifestCount,
      transactionCount,
      segmentCount,
    };
  }

  async checkIntegrity(
    options: { mode?: "metadata" | "full"; maxIssues?: number } = {},
  ): Promise<StorageIntegrityReport> {
    const requestedMode: unknown = options.mode ?? "metadata";
    if (requestedMode !== "metadata" && requestedMode !== "full") {
      throw new TypeError("Integrity mode is invalid");
    }
    const mode = requestedMode;
    const maxIssues = options.maxIssues ?? 100;
    if (!Number.isSafeInteger(maxIssues) || maxIssues < 0) {
      throw new RangeError("Integrity maxIssues must be a non-negative whole number");
    }
    const transaction = this.#transaction([...indexedDbStoreNames], "readonly");
    const issues: Array<{ code: string; location: string; message: string }> = [];
    let issueCount = 0;
    let checkedRecords = 0;
    let checkedBlocks = 0;
    let checkedBytes = 0;
    const issue = (code: string, location: string, error: unknown): void => {
      issueCount += 1;
      if (issues.length >= maxIssues) return;
      issues.push({ code, location, message: integrityErrorMessage(error) });
    };
    const catalog = transaction.objectStore("catalog");
    const [currentValue, epochValue, schemaEpochValue] = await Promise.all([
      requestResult<unknown>(catalog.get(CURRENT_MANIFEST_KEY)),
      requestResult<unknown>(catalog.get(CATALOG_EPOCH_KEY)),
      requestResult<unknown>(catalog.get(SCHEMA_EPOCH_KEY)),
    ]);
    let currentVersion: number | undefined;
    try {
      currentVersion = asOptionalManifestVersion(currentValue, CURRENT_MANIFEST_KEY);
    } catch (error) {
      issue("invalid-current-manifest", CURRENT_MANIFEST_KEY, error);
    }
    let catalogEpoch: number | undefined;
    let schemaEpoch: number | undefined;
    try {
      catalogEpoch = asCatalogEpoch(epochValue);
    } catch (error) {
      issue("invalid-catalog-epoch", CATALOG_EPOCH_KEY, error);
    }
    try {
      schemaEpoch = asSchemaEpoch(schemaEpochValue);
    } catch (error) {
      issue("invalid-schema-epoch", SCHEMA_EPOCH_KEY, error);
    }
    if (catalogEpoch !== undefined && schemaEpoch !== undefined && schemaEpoch > catalogEpoch) {
      issue("schema-epoch-ahead", SCHEMA_EPOCH_KEY, "schema epoch exceeds catalog epoch");
    }

    const tablesById = new Map<string, TableRecord>();
    const tableNames = new Map<string, string>();
    const secondaryIndexNames = new Map<string, { tableId: string; indexId: string }>();
    const triggerNames = new Map<string, { tableId: string; triggerId: string }>();
    const triggerIds = new Map<string, { tableId: string; triggerName: string }>();
    let snapshotImportMarker: SnapshotFrameImportMarker | undefined;
    let completedSnapshotImport: CompletedSnapshotFrameImportRecord | undefined;
    let snapshotExportMarker: SnapshotFrameExportMarker | undefined;
    await visitObjectStoreSequentially(catalog, async (value, key) => {
      checkedRecords += 1;
      try {
        if (typeof key === "string") {
          if (key.startsWith(TABLE_ID_PREFIX)) {
            const record = asTableRecord(value, key);
            if (key !== `${TABLE_ID_PREFIX}${record.id}`) {
              throw corruption(key, "table key and id differ");
            }
            tablesById.set(record.id, record);
          } else if (key.startsWith(TABLE_NAME_PREFIX)) {
            const name = key.slice(TABLE_NAME_PREFIX.length);
            tableNames.set(name, nonEmptyStoredString(value, key));
          } else if (key.startsWith(SECONDARY_INDEX_NAME_PREFIX)) {
            const name = key.slice(SECONDARY_INDEX_NAME_PREFIX.length);
            if (name.length === 0) throw corruption(key, "index name is empty");
            const marker = asOptionalSecondaryIndexNameMarker(value, key);
            if (marker === undefined) throw corruption(key, "index marker is missing");
            secondaryIndexNames.set(name, marker);
          } else if (key.startsWith(TRIGGER_NAME_PREFIX)) {
            const name = key.slice(TRIGGER_NAME_PREFIX.length);
            storedCatalogName(name, key);
            const marker = asOptionalTriggerNameMarker(value, key);
            if (marker === undefined) throw corruption(key, "trigger name marker is missing");
            triggerNames.set(name, marker);
          } else if (key.startsWith(TRIGGER_ID_PREFIX)) {
            const id = key.slice(TRIGGER_ID_PREFIX.length);
            nonEmptyStoredString(id, key);
            const marker = asOptionalTriggerIdMarker(value, key);
            if (marker === undefined) throw corruption(key, "trigger ID marker is missing");
            triggerIds.set(id, marker);
          } else if (key === CURRENT_MANIFEST_KEY) {
            asOptionalManifestVersion(value, key);
          } else if (key === CATALOG_EPOCH_KEY) {
            asCatalogEpoch(value);
          } else if (key === SCHEMA_EPOCH_KEY) {
            asSchemaEpoch(value);
          } else if (key === SNAPSHOT_FRAME_IMPORT_KEY) {
            snapshotImportMarker = asSnapshotFrameImportMarker(value);
          } else if (key === SNAPSHOT_FRAME_COMPLETED_KEY) {
            completedSnapshotImport = asOptionalCompletedSnapshotFrameImportRecord(value);
          } else if (key === SNAPSHOT_EXPORT_KEY) {
            snapshotExportMarker = asSnapshotFrameExportMarker(value);
          } else if (key.startsWith(BLOCK_METADATA_PREFIX)) {
            const blockId = key.slice(BLOCK_METADATA_PREFIX.length);
            if (blockId.length === 0) throw corruption(key, "block ID is missing");
            asStoredBlockMetadata(value, blockId);
            if (
              (await requestResult(transaction.objectStore("blocks").getKey(blockId))) === undefined
            ) {
              throw corruption(key, "block payload is missing");
            }
          } else if (key.startsWith(ROW_ID_PREFIX)) {
            if (key.length === ROW_ID_PREFIX.length) throw corruption(key, "table id is missing");
            asOptionalCounter(value, key);
            const tableId = key.slice(ROW_ID_PREFIX.length);
            const tableValue: unknown = await requestResult(
              catalog.get(`${TABLE_ID_PREFIX}${tableId}`),
            );
            if (tableValue === undefined) {
              issue("orphan-row-id-counter", key, "counter has no catalog table owner");
              return;
            }
            asTableRecord(tableValue, `${TABLE_ID_PREFIX}${tableId}`);
          } else if (key.startsWith(AUTO_INCREMENT_PREFIX)) {
            if (key.length === AUTO_INCREMENT_PREFIX.length) {
              throw corruption(key, "table and column ids are missing");
            }
            asOptionalCounter(value, key);
            if (!(await autoIncrementCounterHasCatalogOwner(catalog, key))) {
              issue(
                "orphan-auto-increment-counter",
                key,
                "counter has no auto-increment column owner",
              );
              return;
            }
          } else if (key.startsWith(FTS_BASE_INDEX_PREFIX)) {
            const toc = decodeFtsBaseToc(value);
            if (toc === undefined) throw corruption(key, "postings table of contents is invalid");
            const identity = key.slice(FTS_BASE_INDEX_PREFIX.length);
            if (!(await postingIdentityHasCatalogOwner(catalog, identity))) {
              throw corruption(key, "postings table of contents has no catalog owner");
            }
            const prefix = ftsBaseChunkPrefixFromIdentity(identity, toc.generation);
            for (let ordinal = 0; ordinal < toc.boundaries.length; ordinal += 1) {
              const chunkKey = `${prefix}${String(ordinal).padStart(6, "0")}`;
              const chunk = decodeFtsPostingChunk(
                await requestResult<unknown>(catalog.get(chunkKey)),
              );
              if (chunk === undefined || !ftsChunkMatchesBoundary(chunk, toc.boundaries[ordinal])) {
                throw corruption(chunkKey, "postings base chunk is missing or invalid");
              }
            }
          } else if (key.startsWith(FTS_BASE_BUILD_PREFIX)) {
            const marker = asOptionalFtsBaseBuildMarker(value, key);
            if (marker === undefined) throw corruption(key, "postings build marker is missing");
            const identity = key.slice(FTS_BASE_BUILD_PREFIX.length);
            if (!(await postingIdentityHasCatalogOwner(catalog, identity))) {
              throw corruption(key, "postings build has no catalog owner");
            }
            const prefix = ftsBaseChunkPrefixFromIdentity(identity, marker.buildId);
            for (
              let ordinal = marker.cleanupIndex;
              ordinal < marker.boundaries.length;
              ordinal += 1
            ) {
              const chunkKey = `${prefix}${String(ordinal).padStart(6, "0")}`;
              const chunk = decodeFtsPostingChunk(
                await requestResult<unknown>(catalog.get(chunkKey)),
              );
              if (
                chunk === undefined ||
                !ftsChunkMatchesBoundary(chunk, marker.boundaries[ordinal])
              ) {
                throw corruption(chunkKey, "staged postings chunk is missing or invalid");
              }
            }
          } else if (key.startsWith(FTS_RETIREMENT_PREFIX)) {
            const marker = asOptionalFtsRetirementMarker(value, key);
            if (marker === undefined)
              throw corruption(key, "postings retirement marker is missing");
          } else if (key.startsWith(UNIQUE_KEY_BUILD_PREFIX)) {
            const envelope = asUniqueKeyBuildEnvelope(value, key);
            if (key !== uniqueKeyBuildKey(envelope.record.buildId)) {
              throw corruption(key, "UNIQUE build key and record differ");
            }
          } else if (key.startsWith(FTS_BASE_PREFIX)) {
            const chunk = decodeFtsPostingChunk(value);
            if (chunk === undefined) throw corruption(key, "postings base chunk is invalid");
            if (!(await ftsBaseChunkHasProvenance(catalog, key))) {
              throw corruption(key, "postings base chunk has no table of contents or build marker");
            }
          } else if (key.startsWith(`${FTS_CHUNK_PREFIX}index/`)) {
            const index = decodeFtsDeltaIndex(value);
            if (index === undefined) throw corruption(key, "postings delta index is invalid");
            const identity = key.slice(`${FTS_CHUNK_PREFIX}index/`.length);
            if (!(await postingIdentityHasCatalogOwner(catalog, identity))) {
              throw corruption(key, "postings delta index has no catalog owner");
            }
            for (const version of index.versions) {
              const chunkKey = `${FTS_CHUNK_PREFIX}${identity}/${String(version)}`;
              if (
                decodeFtsDeltaChunk(await requestResult<unknown>(catalog.get(chunkKey))) ===
                undefined
              ) {
                throw corruption(chunkKey, "postings delta chunk is missing or invalid");
              }
            }
          } else if (key.startsWith(FTS_CHUNK_PREFIX)) {
            const chunk = decodeFtsDeltaChunk(value);
            if (chunk === undefined) throw corruption(key, "postings delta chunk is invalid");
            if (!(await ftsDeltaChunkHasProvenance(catalog, key))) {
              throw corruption(key, "postings delta chunk has no index or retirement marker");
            }
          }
          return;
        }
        if (!Array.isArray(key) || typeof key[0] !== "string" || typeof key[1] !== "string") {
          throw corruption("catalog", "structured catalog key is invalid");
        }
        const [kind, namespaceId, ordinal, boundary] = key;
        if (kind === MANIFEST_BLOCK) {
          if (key.length !== 2) {
            throw corruption(`${MANIFEST_BLOCK}/${namespaceId}`, "key shape is invalid");
          }
          asManifestBlockRecord(value, namespaceId);
        } else if (kind === UNIQUE_KEY_BUILD_CHUNK) {
          if (
            key.length !== 3 ||
            !isStorageId(namespaceId) ||
            !Number.isSafeInteger(ordinal) ||
            (ordinal as number) < 0 ||
            !isRecord(value) ||
            !hasOnlyKnownFields(value, [
              "tokenCount",
              "retainedBytes",
              "firstToken",
              "lastToken",
              "partFirstTokens",
            ]) ||
            !isBoundedCursor(value.tokenCount, MAX_UNIQUE_KEY_BUILD_TOKENS_PER_CHUNK) ||
            !isBoundedCursor(value.retainedBytes, MAX_UNIQUE_KEY_BUILD_CHUNK_BYTES) ||
            typeof value.firstToken !== "string" ||
            typeof value.lastToken !== "string" ||
            !Array.isArray(value.partFirstTokens) ||
            !value.partFirstTokens.every((token) => typeof token === "string")
          ) {
            throw corruption(`${UNIQUE_KEY_BUILD_CHUNK}/${namespaceId}`, "chunk is invalid");
          }
        } else if (kind === UNIQUE_KEY_CHUNK_INDEX) {
          if (key.length !== 2) {
            throw corruption(`${UNIQUE_KEY_CHUNK_INDEX}/${namespaceId}`, "key shape is invalid");
          }
          asUniqueKeyChunkIndex(value);
          if (!(await uniqueKeyNamespaceIsActive(catalog, namespaceId))) {
            throw corruption(namespaceId, "membership index has no catalog owner");
          }
        } else if (kind === UNIQUE_KEY_BASE_PART) {
          if (key.length !== 3 || typeof ordinal !== "string") {
            throw corruption(`${UNIQUE_KEY_BASE_PART}/${namespaceId}`, "key shape is invalid");
          }
          const tokens = asBasePartition(value);
          if (tokens[0] !== ordinal) {
            throw corruption(`${UNIQUE_KEY_BASE_PART}/${namespaceId}`, "boundary differs from key");
          }
        } else if (kind === UNIQUE_KEY_CHUNK) {
          if (key.length !== 4 || typeof boundary !== "string") {
            throw corruption(`${UNIQUE_KEY_CHUNK}/${namespaceId}`, "key shape is invalid");
          }
          if (!Number.isSafeInteger(ordinal) || (ordinal as number) < 0) {
            throw corruption(`${UNIQUE_KEY_CHUNK}/${namespaceId}`, "version is invalid");
          }
          const chunk = asUniqueKeyChunk(value);
          if (uniqueChunkFirstToken(chunk) !== boundary) {
            throw corruption(`${UNIQUE_KEY_CHUNK}/${namespaceId}`, "boundary differs from key");
          }
          const rawIndex: unknown = await requestResult(
            catalog.get(uniqueKeyChunkIndexKey(namespaceId)),
          );
          const index = rawIndex === undefined ? undefined : asUniqueKeyChunkIndex(rawIndex);
          if (!index?.versions.includes(ordinal as number)) {
            throw corruption(`${UNIQUE_KEY_CHUNK}/${namespaceId}`, "tail is orphaned");
          }
        } else throw corruption(kind, "structured catalog record kind is unknown");
      } catch (error) {
        issue("invalid-catalog-record", storageKeyLocation(key), error);
      }
    });

    if (snapshotImportMarker !== undefined) {
      if (!snapshotImportMarker.replayCompleted && currentVersion !== undefined) {
        issue(
          "published-snapshot-import",
          SNAPSHOT_FRAME_IMPORT_KEY,
          "live import journal exists beside a published database",
        );
      }
      if (snapshotImportMarker.replayCompleted && currentVersion !== snapshotImportMarker.version) {
        issue(
          "stale-snapshot-replay",
          SNAPSHOT_FRAME_IMPORT_KEY,
          "completed replay does not match the current manifest",
        );
      }
    }
    if (
      completedSnapshotImport !== undefined &&
      currentVersion !== completedSnapshotImport.version
    ) {
      issue(
        "stale-completed-snapshot-import",
        SNAPSHOT_FRAME_COMPLETED_KEY,
        "completed import does not describe the current manifest",
      );
    }
    let snapshotFrameRecords = 0;
    await visitObjectStoreSequentially(
      transaction.objectStore(SNAPSHOT_HEADER_STORE),
      (value, key) => {
        checkedRecords += 1;
        try {
          if (
            !Array.isArray(key) ||
            key.length !== 3 ||
            (key[0] !== "export" && key[0] !== "import") ||
            typeof key[1] !== "string" ||
            !Number.isSafeInteger(key[2]) ||
            (key[2] as number) < 0
          ) {
            throw corruption(SNAPSHOT_HEADER_STORE, "snapshot frame key is invalid");
          }
          const [direction, identity, rawSequence] = key;
          const sequence = rawSequence as number;
          if (direction === "export") {
            const header =
              identity === snapshotExportMarker?.sessionId &&
              sequence < snapshotExportMarker.metadataFrameCount
                ? snapshotExportMarker.header
                : undefined;
            if (header === undefined) {
              throw corruption(SNAPSHOT_HEADER_STORE, "snapshot export frame is orphaned");
            }
            const frame = asSnapshotFrame(value, sequence);
            if (frame.kind !== snapshotFrameKindAtSequence(header, sequence)) {
              throw corruption(SNAPSHOT_HEADER_STORE, "snapshot export frame kind is invalid");
            }
          } else {
            const header =
              identity === snapshotImportMarker?.identity &&
              sequence < snapshotImportMarker.nextSequence
                ? snapshotImportMarker.header
                : undefined;
            if (header === undefined) {
              throw corruption(SNAPSHOT_HEADER_STORE, "snapshot import frame is orphaned");
            }
            const kind = snapshotFrameKindAtSequence(header, sequence);
            if (kind === "block") asSnapshotBlockFrameRecord(value, sequence);
            else {
              const frame = asSnapshotFrame(value, sequence);
              if (frame.kind !== kind || crc32(frame.payload) !== frame.checksum) {
                throw corruption(SNAPSHOT_HEADER_STORE, "snapshot import frame is invalid");
              }
            }
          }
          snapshotFrameRecords += 1;
        } catch (error) {
          issue("invalid-snapshot-frame", storageKeyLocation(key), error);
        }
      },
    );
    const expectedSnapshotFrameRecords =
      (snapshotExportMarker?.metadataFrameCount ?? 0) + (snapshotImportMarker?.nextSequence ?? 0);
    if (snapshotFrameRecords !== expectedSnapshotFrameRecords) {
      issue(
        "incomplete-snapshot-frames",
        SNAPSHOT_HEADER_STORE,
        "snapshot frame record count differs from durable progress",
      );
    }

    for (const [name, id] of tableNames) {
      const table = tablesById.get(id);
      if (table?.name !== name) {
        issue("broken-table-name", `${TABLE_NAME_PREFIX}${name}`, "pointer does not match a table");
      }
    }
    const declaredTriggerNames = new Map<string, { tableId: string; triggerId: string }>();
    const declaredTriggerIds = new Map<string, { tableId: string; triggerName: string }>();
    for (const table of tablesById.values()) {
      if (tableNames.get(table.name) !== table.id) {
        issue(
          "missing-table-name",
          `${TABLE_ID_PREFIX}${table.id}`,
          "reverse name pointer is missing",
        );
      }
      const namespaces = [
        ...(table.uniqueKeyColumnId === undefined ? [] : [table.id]),
        ...Object.entries(table.secondaryIndexes ?? {}).flatMap(([indexId, index]) =>
          index.uniqueEnforced === true ? [secondaryUniqueKeyNamespace(table.id, indexId)] : [],
        ),
      ];
      for (const namespaceId of namespaces) {
        const rawIndex: unknown = await requestResult(
          catalog.get(uniqueKeyChunkIndexKey(namespaceId)),
        );
        if (rawIndex === undefined) {
          issue("missing-unique-membership", namespaceId, "membership index is missing");
          continue;
        }
        const index = asUniqueKeyChunkIndex(rawIndex);
        if (index.hasBase) {
          if (index.baseGenerationId !== undefined) {
            try {
              await validateUniqueKeyGenerationTokenCount(
                catalog,
                index.baseGenerationId,
                index.tokenCount ?? 0,
              );
            } catch (error) {
              issue("unique-token-count", namespaceId, error);
            }
          }
        }
        for (const version of index.versions) {
          const tails = await readUniqueKeyTailParts(catalog, namespaceId, version);
          if (tails.length === 0) {
            issue("missing-unique-tail", namespaceId, `tail ${String(version)} is missing`);
          }
        }
      }
      for (const [indexId, index] of Object.entries(table.secondaryIndexes ?? {})) {
        const marker = secondaryIndexNames.get(index.name);
        if (marker?.tableId !== table.id || marker.indexId !== indexId) {
          issue(
            "missing-secondary-index-name",
            `${SECONDARY_INDEX_NAME_PREFIX}${index.name}`,
            "reverse index-name pointer is missing or mismatched",
          );
        }
      }
      for (const trigger of table.triggers ?? []) {
        const existingName = declaredTriggerNames.get(trigger.name);
        if (existingName !== undefined) {
          issue(
            "duplicate-trigger-name",
            `${TABLE_ID_PREFIX}${table.id}`,
            `trigger name ${trigger.name} is also owned by table ${existingName.tableId}`,
          );
        } else {
          declaredTriggerNames.set(trigger.name, { tableId: table.id, triggerId: trigger.id });
        }
        const existingId = declaredTriggerIds.get(trigger.id);
        if (existingId !== undefined) {
          issue(
            "duplicate-trigger-id",
            `${TABLE_ID_PREFIX}${table.id}`,
            `trigger ID ${trigger.id} is also owned by table ${existingId.tableId}`,
          );
        } else {
          declaredTriggerIds.set(trigger.id, { tableId: table.id, triggerName: trigger.name });
        }
        const nameMarker = triggerNames.get(trigger.name);
        if (nameMarker?.tableId !== table.id || nameMarker.triggerId !== trigger.id) {
          issue(
            "missing-trigger-name",
            `${TRIGGER_NAME_PREFIX}${trigger.name}`,
            "reverse trigger-name pointer is missing or mismatched",
          );
        }
        const idMarker = triggerIds.get(trigger.id);
        if (idMarker?.tableId !== table.id || idMarker.triggerName !== trigger.name) {
          issue(
            "missing-trigger-id",
            `${TRIGGER_ID_PREFIX}${trigger.id}`,
            "reverse trigger-ID pointer is missing or mismatched",
          );
        }
      }
      for (const storageColumnId of activePostingStorageColumnIds(table)) {
        const ready =
          table.ftsColumns?.[storageColumnId]?.state === "ready" ||
          Object.values(table.secondaryIndexes ?? {}).some(
            (index) => index.storageColumnId === storageColumnId && index.state === "ready",
          );
        if (ready) {
          const identity = `${table.id}/${storageColumnId}`;
          const toc = decodeFtsBaseToc(
            await requestResult<unknown>(catalog.get(`${FTS_BASE_INDEX_PREFIX}${identity}`)),
          );
          if (toc === undefined) {
            issue("missing-postings-toc", identity, "ready postings index has no base");
          }
          const delta = decodeFtsDeltaIndex(
            await requestResult<unknown>(catalog.get(`${FTS_CHUNK_PREFIX}index/${identity}`)),
          );
          if (delta === undefined) {
            issue("missing-postings-delta-index", identity, "postings delta index is missing");
          }
        }
      }
    }

    for (const [name, marker] of secondaryIndexNames) {
      const index = tablesById.get(marker.tableId)?.secondaryIndexes?.[marker.indexId];
      if (index?.name !== name) {
        issue(
          "broken-secondary-index-name",
          `${SECONDARY_INDEX_NAME_PREFIX}${name}`,
          "pointer does not match a catalog index",
        );
      }
    }
    for (const [name, marker] of triggerNames) {
      const trigger = tablesById
        .get(marker.tableId)
        ?.triggers?.find((candidate) => candidate.id === marker.triggerId);
      if (trigger?.name !== name) {
        issue(
          "broken-trigger-name",
          `${TRIGGER_NAME_PREFIX}${name}`,
          "pointer does not match a catalog trigger",
        );
      }
    }
    for (const [id, marker] of triggerIds) {
      const trigger = tablesById
        .get(marker.tableId)
        ?.triggers?.find((candidate) => candidate.name === marker.triggerName);
      if (trigger?.id !== id) {
        issue(
          "broken-trigger-id",
          `${TRIGGER_ID_PREFIX}${id}`,
          "pointer does not match a catalog trigger",
        );
      }
    }
    let currentManifestFound = currentVersion === undefined;
    let previousManifest: StoredManifestRecord | undefined;
    let currentManifestRecord: StoredManifestRecord | undefined;
    let observedManifestCount = 0;
    let observedManifestBytes = 0;
    await visitObjectStoreSequentially(transaction.objectStore("manifests"), (value, key) => {
      checkedRecords += 1;
      observedManifestCount = incrementSafeInteger(
        observedManifestCount,
        "Integrity manifest count",
      );
      try {
        if (typeof key !== "number" || !Number.isSafeInteger(key) || key < 0) {
          throw corruption("manifests", "record key is invalid");
        }
        const record = asStoredManifestRecord(value, key);
        observedManifestBytes = safeByteSum(
          observedManifestBytes,
          manifestRecordRetainedReservationBytes(record),
          "Integrity manifest bytes",
        );
        if (
          record.previousVersion !== null &&
          record.previousVersion !== previousManifest?.version
        ) {
          throw corruption(`manifests/${String(key)}`, "manifest predecessor is unavailable");
        }
        previousManifest = record;
        if (key === currentVersion) {
          currentManifestFound = true;
          if (record.prunedAt !== undefined) {
            throw corruption(CURRENT_MANIFEST_KEY, "current manifest is pruned");
          }
          currentManifestRecord = record;
        }
      } catch (error) {
        issue("invalid-manifest", storageKeyLocation(key), error);
      }
    });
    if (!currentManifestFound) {
      issue("missing-current-manifest", CURRENT_MANIFEST_KEY, "current manifest is missing");
    }
    if (currentManifestRecord !== undefined) {
      let liveBytes = 0;
      let liveCount = 0;
      try {
        await visitManifestBlockRecords(catalog, async (record) => {
          checkedRecords += 1;
          if (!manifestBlockVisibleAt(record, currentManifestRecord?.version ?? -1)) return;
          liveCount = incrementSafeInteger(liveCount, "Integrity live block count");
          liveBytes = safeByteSum(liveBytes, record.byteLength, "Integrity live block bytes");
          if (
            (await requestResult(transaction.objectStore("blocks").getKey(record.blockId))) ===
            undefined
          ) {
            issue(
              "missing-live-block",
              `blocks/${record.blockId}`,
              "current manifest references a missing block",
            );
          }
        });
        if (
          liveCount !== currentManifestRecord.liveBlockCount ||
          liveBytes !== currentManifestRecord.liveBlockBytes
        ) {
          issue(
            "manifest-summary-mismatch",
            `manifests/${String(currentManifestRecord.version)}`,
            "live block count or byte total disagrees with provenance",
          );
        }
      } catch (error) {
        issue("invalid-manifest-block", MANIFEST_BLOCK, error);
      }
    }

    const segmentStore = transaction.objectStore("segments");
    const transactionStore = transaction.objectStore("transactions");
    const blockStore = transaction.objectStore("blocks");
    const manifestStore = transaction.objectStore("manifests");
    let observedSegmentCount = 0;
    let observedSegmentBytes = 0;
    await visitObjectStoreSequentially(segmentStore, async (value, key) => {
      checkedRecords += 1;
      observedSegmentCount = incrementSafeInteger(observedSegmentCount, "Integrity segment count");
      try {
        if (typeof key !== "string") throw corruption("segments", "record key is invalid");
        const segment = asSegmentRecord(value);
        observedSegmentBytes = safeByteSum(
          observedSegmentBytes,
          segmentRecordRetainedBytes(segment),
          "Integrity segment bytes",
        );
        if (segment.id !== key) throw corruption(`segments/${key}`, "record id differs from key");
        const ownerValue: unknown = await requestResult(
          transactionStore.get(segment.transactionId),
        );
        if (ownerValue === undefined) {
          throw corruption(
            `segments/${key}`,
            `owning transaction ${segment.transactionId} is missing`,
          );
        }
        const owner = asTransactionRecord(ownerValue, segment.transactionId);
        const segmentTable =
          tablesById.get(segment.tableId) ??
          (owner.status === "active" && owner.pendingTable?.id === segment.tableId
            ? owner.pendingTable
            : undefined);
        if (segmentTable === undefined) {
          throw corruption(`segments/${key}`, `table ${segment.tableId} is missing`);
        }
        if (!tablesById.has(segment.tableId)) {
          if (!owner.pendingSegmentIds.includes(segment.id)) {
            throw corruption(`segments/${key}`, "pending table owner does not journal segment");
          }
          const columns = new Set(segmentTable.columns.map((column) => column.id));
          if (Object.keys(segment.columnBlockIds).some((columnId) => !columns.has(columnId))) {
            throw corruption(`segments/${key}`, "pending table segment has an unknown column");
          }
        }
        for (const blockId of Object.values(segment.columnBlockIds).flat()) {
          if ((await requestResult(blockStore.getKey(blockId))) === undefined) {
            throw corruption(`segments/${key}`, `referenced block ${blockId} is missing`);
          }
        }
      } catch (error) {
        issue("invalid-segment", storageKeyLocation(key), error);
      }
    });
    const integrityPendingTables: TableRecord[] = [];
    await visitObjectStoreSequentially(transactionStore, async (value, key) => {
      checkedRecords += 1;
      try {
        if (typeof key !== "string") throw corruption("transactions", "record key is invalid");
        const record = asTransactionRecord(value, key);
        for (const [commitOrdinal, segmentId] of record.pendingSegmentIds.entries()) {
          const segmentValue: unknown = await requestResult(segmentStore.get(segmentId));
          if (segmentValue === undefined) {
            if (record.status === "active") {
              throw corruption(`transactions/${key}`, `pending segment ${segmentId} is missing`);
            }
            continue;
          }
          const segment = asSegmentRecord(segmentValue);
          if (segment.transactionId !== record.id) {
            throw corruption(
              `transactions/${key}`,
              `pending segment ${segmentId} belongs to another transaction`,
            );
          }
          if (segment.commitOrdinal !== commitOrdinal) {
            throw corruption(
              `transactions/${key}`,
              `pending segment ${segmentId} has a noncanonical commit ordinal`,
            );
          }
        }
        if (record.status === "active") {
          if (record.pendingTable !== undefined) integrityPendingTables.push(record.pendingTable);
          for (const blockId of record.pendingBlockIds) {
            if ((await requestResult(blockStore.getKey(blockId))) === undefined) {
              throw corruption(`transactions/${key}`, `pending block ${blockId} is missing`);
            }
            const provenanceValue: unknown = await requestResult(
              catalog.get(manifestBlockKey(blockId)),
            );
            const alreadyLive =
              provenanceValue !== undefined &&
              currentVersion !== undefined &&
              manifestBlockVisibleAt(
                asManifestBlockRecord(provenanceValue, blockId),
                currentVersion,
              );
            if (alreadyLive) {
              throw corruption(
                `transactions/${key}`,
                `pending block ${blockId} is already live in the current manifest`,
              );
            }
            const journaledByAnotherActiveTransaction = await visitObjectStoreSequentially(
              transactionStore,
              (candidateValue, candidateKey) => {
                if (typeof candidateKey !== "string") {
                  throw corruption("transactions", "record key is invalid");
                }
                const candidate = asTransactionRecord(candidateValue, candidateKey);
                return (
                  candidate.id !== record.id &&
                  candidate.status === "active" &&
                  candidate.pendingBlockIds.includes(blockId)
                );
              },
            );
            if (journaledByAnotherActiveTransaction) {
              throw corruption(
                `transactions/${key}`,
                `pending block ${blockId} is journaled by another active transaction`,
              );
            }
            const ownedByAnotherTransaction = await visitObjectStoreSequentially(
              segmentStore,
              (segmentValue, segmentKey) => {
                if (typeof segmentKey !== "string") {
                  throw corruption("segments", "record key is invalid");
                }
                const segment = asSegmentRecord(segmentValue);
                if (segment.id !== segmentKey) {
                  throw corruption(`segments/${segmentKey}`, "record id differs from key");
                }
                return (
                  segment.transactionId !== record.id && segmentBlockIds(segment).includes(blockId)
                );
              },
            );
            if (ownedByAnotherTransaction) {
              throw corruption(
                `transactions/${key}`,
                `pending block ${blockId} belongs to another transaction's segment`,
              );
            }
          }
          for (const segmentId of record.pendingSegmentIds) {
            const segmentValue: unknown = await requestResult(segmentStore.get(segmentId));
            if (segmentValue === undefined) {
              throw corruption(`transactions/${key}`, `pending segment ${segmentId} is missing`);
            }
            if (asSegmentRecord(segmentValue).transactionId !== record.id) {
              throw corruption(
                `transactions/${key}`,
                `pending segment ${segmentId} belongs to another transaction`,
              );
            }
          }
          if (record.pendingTable !== undefined) {
            let nextRowId = 1n;
            for (const segmentId of record.pendingSegmentIds) {
              const segment = asSegmentRecord(await requestResult(segmentStore.get(segmentId)));
              if (segment.tableId !== record.pendingTable.id) continue;
              if (
                segment.kind !== "insert" ||
                segment.rowIdStart !== nextRowId ||
                segment.rowIdEndExclusive <= segment.rowIdStart
              ) {
                throw corruption(`transactions/${key}`, "pending table row ranges are invalid");
              }
              nextRowId = segment.rowIdEndExclusive;
            }
            if (record.pendingTableNextRowId !== nextRowId) {
              throw corruption(`transactions/${key}`, "pending table row counter is invalid");
            }
          }
          await visitObjectStoreSequentially(segmentStore, (segmentValue, segmentKey) => {
            if (typeof segmentKey !== "string") {
              throw corruption("segments", "record key is invalid");
            }
            const segment = asSegmentRecord(segmentValue);
            if (
              segment.transactionId === record.id &&
              !record.pendingSegmentIds.includes(segment.id)
            ) {
              throw corruption(
                `transactions/${key}`,
                `owned segment ${segment.id} is absent from the active journal`,
              );
            }
          });
          if (record.snapshotVersion !== null) {
            const snapshotValue: unknown = await requestResult(
              manifestStore.get(record.snapshotVersion),
            );
            if (
              snapshotValue === undefined ||
              asStoredManifestRecord(snapshotValue, record.snapshotVersion).prunedAt !== undefined
            ) {
              throw corruption(`transactions/${key}`, "active snapshot is unavailable");
            }
          }
        }
      } catch (error) {
        issue("invalid-transaction", storageKeyLocation(key), error);
      }
    });
    try {
      const combinedTables = [...tablesById.values(), ...integrityPendingTables];
      const ids = new Set<string>();
      const names = new Set<string>();
      let combinedBytes = 0;
      for (const table of combinedTables) {
        if (ids.has(table.id)) throw new Error(`Duplicate table id: ${table.id}`);
        if (names.has(table.name)) throw new Error(`Duplicate table name: ${table.name}`);
        ids.add(table.id);
        names.add(table.name);
        combinedBytes = safeByteSum(
          combinedBytes,
          catalogRecordRetainedBytes(table),
          "Integrity catalog bytes",
        );
        await assertTableForeignKeysInTransaction(catalog, table);
      }
      if (combinedTables.length > MAX_CATALOG_RECORDS) {
        throw new Error("Published and pending catalog count exceeds its limit");
      }
      if (combinedBytes > MAX_CATALOG_RETAINED_BYTES) {
        throw new Error("Published and pending catalog bytes exceed their limit");
      }
    } catch (error) {
      issue("invalid-pending-catalog", "transactions", error);
    }
    const leaseIds = new Set<string>();
    await visitObjectStoreSequentially(transaction.objectStore("leases"), async (value, key) => {
      checkedRecords += 1;
      try {
        if (typeof key !== "string") throw corruption("leases", "record key is invalid");
        const lease = asLeaseRecord(value, key);
        leaseIds.add(key);
        if (lease.kind === "backup") {
          if (snapshotExportMarker === undefined) {
            throw corruption(`leases/${key}`, "backup lease has no active export marker");
          }
          assertSnapshotFrameExportLease(snapshotExportMarker, lease);
        }
        if (lease.manifestVersion !== null) {
          const manifestValue: unknown = await requestResult(
            manifestStore.get(lease.manifestVersion),
          );
          if (
            manifestValue === undefined ||
            asStoredManifestRecord(manifestValue, lease.manifestVersion).prunedAt !== undefined
          ) {
            throw corruption(`leases/${key}`, "pinned manifest is unavailable");
          }
        }
      } catch (error) {
        issue("invalid-lease", storageKeyLocation(key), error);
      }
    });
    if (snapshotExportMarker !== undefined && !leaseIds.has(snapshotExportMarker.sessionId)) {
      issue(
        "missing-snapshot-export-lease",
        SNAPSHOT_EXPORT_KEY,
        "active export marker has no lease",
      );
    }
    const integrityCompactionJobs = new Map<string, CompactionJobRecord>();
    const integrityGarbageCollectionJobs = new Map<string, GarbageCollectionJobRecord>();
    const integrityActiveCompactions = new Map<string, ActiveCompactionMarker>();
    let integrityActiveGarbageCollection: ActiveGarbageCollectionMarker | undefined;
    let integrityMaintenanceQuota: MaintenanceQuotaRecord | undefined;
    await visitObjectStoreSequentially(transaction.objectStore("gc"), async (value, key) => {
      checkedRecords += 1;
      try {
        if (typeof key !== "string") throw corruption("gc", "record key is invalid");
        if (key.startsWith(COMPACTION_JOB_KEY_PREFIX)) {
          const job = asCompactionJobEnvelope(value);
          if (key !== compactionJobKey(job.id)) {
            throw corruption(`gc/${key}`, `record declares id ${job.id}`);
          }
          integrityCompactionJobs.set(job.id, job);
          if (!isTerminalCompactionJob(job)) {
            const sourceManifest: unknown = await requestResult(
              manifestStore.get(job.sourceManifestVersion),
            );
            if (
              sourceManifest === undefined ||
              asStoredManifestRecord(sourceManifest, job.sourceManifestVersion).prunedAt !==
                undefined
            ) {
              throw corruption(`gc/${key}`, "source manifest is unavailable");
            }
            if (!tablesById.has(job.tableId)) {
              throw corruption(`gc/${key}`, `table ${job.tableId} is missing`);
            }
            for (const id of [...job.sourceBlockIds, ...job.outputBlockIds]) {
              if ((await requestResult(blockStore.getKey(id))) === undefined) {
                throw corruption(`gc/${key}`, `referenced block ${id} is missing`);
              }
            }
            for (const id of job.sourceSegmentIds) {
              if ((await requestResult(segmentStore.getKey(id))) === undefined) {
                throw corruption(`gc/${key}`, `source segment ${id} is missing`);
              }
            }
            if (job.transactionId !== null) {
              const transactionValue: unknown = await requestResult(
                transactionStore.get(job.transactionId),
              );
              if (transactionValue === undefined) {
                throw corruption(`gc/${key}`, "linked transaction is missing");
              }
              const owner = asTransactionRecord(transactionValue, job.transactionId);
              if (job.state === "ready" || owner.pendingSegmentIds.length > 0) {
                const table = tablesById.get(job.tableId);
                if (table === undefined) throw corruption(`gc/${key}`, "table is missing");
                const sourceSegments = await Promise.all(
                  job.sourceSegmentIds.map(async (id) => {
                    const source: unknown = await requestResult(segmentStore.get(id));
                    if (source === undefined) {
                      throw corruption(`gc/${key}`, `source segment ${id} is missing`);
                    }
                    return asSegmentRecord(source);
                  }),
                );
                const outputSegments = await Promise.all(
                  owner.pendingSegmentIds.map(async (id) => {
                    const output: unknown = await requestResult(segmentStore.get(id));
                    if (output === undefined) {
                      throw corruption(`gc/${key}`, `output segment ${id} is missing`);
                    }
                    return asSegmentRecord(output);
                  }),
                );
                assertCompactionOutputProvenance(
                  job,
                  table,
                  owner,
                  sourceSegments,
                  outputSegments,
                  { allowOutputPrefix: job.state === "running" },
                );
              }
            }
          }
        } else if (key.startsWith(GARBAGE_COLLECTION_JOB_KEY_PREFIX)) {
          const job = asGarbageCollectionJobEnvelope(value);
          if (key !== garbageCollectionJobKey(job.id)) {
            throw corruption(`gc/${key}`, `record declares id ${job.id}`);
          }
          integrityGarbageCollectionJobs.set(job.id, job);
          for (const version of job.candidateManifestVersions) {
            const candidate: unknown = await requestResult(manifestStore.get(version));
            if (candidate !== undefined) asStoredManifestRecord(candidate, version);
          }
          for (const id of job.candidateSegmentIds) {
            const candidate: unknown = await requestResult(segmentStore.get(id));
            if (candidate !== undefined && asSegmentRecord(candidate).id !== id) {
              throw corruption(`gc/${key}`, `candidate segment ${id} declares another id`);
            }
          }
          for (const id of job.candidateBlockIds) {
            const candidate: unknown = await requestResult(blockStore.get(id));
            if (candidate !== undefined) asBytes(candidate, `blocks/${id}`);
          }
          for (const id of job.candidateTransactionIds) {
            const candidate: unknown = await requestResult(transactionStore.get(id));
            if (candidate !== undefined) asTransactionRecord(candidate, id);
          }
        } else if (key.startsWith(ACTIVE_COMPACTION_KEY_PREFIX)) {
          let tableId: string;
          try {
            tableId = decodeURIComponent(key.slice(ACTIVE_COMPACTION_KEY_PREFIX.length));
          } catch {
            throw corruption(`gc/${key}`, "active compaction marker key is invalid");
          }
          if (tableId.length === 0 || activeCompactionKey(tableId) !== key) {
            throw corruption(`gc/${key}`, "active compaction marker key is invalid");
          }
          integrityActiveCompactions.set(tableId, asActiveCompactionMarker(value, tableId));
        } else if (key === ACTIVE_GARBAGE_COLLECTION_KEY) {
          integrityActiveGarbageCollection = asActiveGarbageCollectionMarker(value);
        } else if (key === MAINTENANCE_QUOTA_KEY) {
          integrityMaintenanceQuota = asMaintenanceQuota(value);
        } else throw corruption(`gc/${key}`, "job kind is unknown");
      } catch (error) {
        issue("invalid-maintenance-record", storageKeyLocation(key), error);
      }
    });
    for (const job of integrityCompactionJobs.values()) {
      const marker = integrityActiveCompactions.get(job.tableId);
      if (!isTerminalCompactionJob(job) && marker?.jobId !== job.id) {
        issue(
          "missing-active-compaction-marker",
          `gc/${compactionJobKey(job.id)}`,
          "nonterminal compaction job has no exact active marker",
        );
      }
      if (isTerminalCompactionJob(job) && marker?.jobId === job.id) {
        issue(
          "terminal-active-compaction-marker",
          `gc/${activeCompactionKey(job.tableId)}`,
          "terminal compaction job still owns the active marker",
        );
      }
    }
    const observedMaintenanceQuota: MaintenanceQuotaRecord = {
      activeCompactionJobs: [...integrityCompactionJobs.values()].filter(
        (job) => !isTerminalCompactionJob(job),
      ).length,
      terminalCompactionJobs: [...integrityCompactionJobs.values()].filter(isTerminalCompactionJob)
        .length,
      activeGarbageCollectionJobs: [...integrityGarbageCollectionJobs.values()].filter(
        (job) => job.state !== "completed",
      ).length,
      completedGarbageCollectionJobs: [...integrityGarbageCollectionJobs.values()].filter(
        (job) => job.state === "completed",
      ).length,
    };
    if (
      integrityMaintenanceQuota === undefined ||
      Object.entries(observedMaintenanceQuota).some(
        ([field, count]) =>
          integrityMaintenanceQuota?.[field as keyof MaintenanceQuotaRecord] !== count,
      )
    ) {
      issue(
        "maintenance-quota-mismatch",
        `gc/${MAINTENANCE_QUOTA_KEY}`,
        "maintenance quota disagrees with durable job records",
      );
    }
    for (const [tableId, marker] of integrityActiveCompactions) {
      const job = integrityCompactionJobs.get(marker.jobId);
      if (job?.tableId !== tableId || isTerminalCompactionJob(job)) {
        issue(
          "orphan-active-compaction-marker",
          `gc/${activeCompactionKey(tableId)}`,
          "active compaction marker does not name a nonterminal job for its table",
        );
      }
    }
    for (const job of integrityGarbageCollectionJobs.values()) {
      if (job.state !== "completed" && integrityActiveGarbageCollection?.jobId !== job.id) {
        issue(
          "missing-active-garbage-collection-marker",
          `gc/${garbageCollectionJobKey(job.id)}`,
          "nonterminal garbage collection job has no exact active marker",
        );
      }
      if (job.state === "completed" && integrityActiveGarbageCollection?.jobId === job.id) {
        issue(
          "completed-active-garbage-collection-marker",
          `gc/${ACTIVE_GARBAGE_COLLECTION_KEY}`,
          "completed garbage collection job still owns the active marker",
        );
      }
    }
    if (integrityActiveGarbageCollection !== undefined) {
      const job = integrityGarbageCollectionJobs.get(integrityActiveGarbageCollection.jobId);
      if (job === undefined || job.state === "completed") {
        issue(
          "orphan-active-garbage-collection-marker",
          `gc/${ACTIVE_GARBAGE_COLLECTION_KEY}`,
          "active garbage collection marker does not name a nonterminal job",
        );
      }
    }
    const tempStore = transaction.objectStore("temp");
    let persistedTempGlobal: TempGlobalQuotaRecord | undefined;
    let observedOwnerCount = 0;
    let observedRunCount = 0;
    let observedPageCount = 0;
    let observedBytes = 0;
    let runOwnerId: string | undefined;
    let runId: string | undefined;
    let runPageCount = 0;
    let runBytes = 0;
    let ownerRunCount = 0;
    let ownerPageCount = 0;
    let ownerBytes = 0;
    const finishObservedOwner = async (): Promise<void> => {
      if (runOwnerId === undefined) return;
      const persisted = asTempOwnerQuotaRecord(
        await requestResult<unknown>(tempStore.get(tempOwnerQuotaKey(runOwnerId))),
        runOwnerId,
      );
      if (
        persisted.runCount !== ownerRunCount ||
        persisted.pageCount !== ownerPageCount ||
        persisted.retainedBytes !== ownerBytes
      ) {
        issue("temp-quota-mismatch", `temp/${runOwnerId}`, "owner quota disagrees with its pages");
      }
      ownerRunCount = 0;
      ownerPageCount = 0;
      ownerBytes = 0;
    };
    const finishObservedRun = async (): Promise<void> => {
      if (runOwnerId === undefined || runId === undefined) return;
      const persisted = asTempRunQuotaRecord(
        await requestResult<unknown>(tempStore.get(tempRunQuotaKey(runOwnerId, runId))),
        runOwnerId,
        runId,
      );
      if (persisted?.pageCount !== runPageCount || persisted.retainedBytes !== runBytes) {
        issue(
          "temp-quota-mismatch",
          `temp/${runOwnerId}\u0000${runId}`,
          "run quota disagrees with its pages",
        );
      }
      ownerRunCount = incrementSafeInteger(ownerRunCount, "Integrity temp run count");
      ownerPageCount = safeByteSum(ownerPageCount, runPageCount, "Integrity owner pages");
      ownerBytes = safeByteSum(ownerBytes, runBytes, "Integrity owner temp bytes");
      observedRunCount = incrementSafeInteger(observedRunCount, "Integrity temp run count");
      runPageCount = 0;
      runBytes = 0;
    };
    await visitObjectStoreSequentially(tempStore, async (value, key) => {
      checkedRecords += 1;
      try {
        if (!Array.isArray(key) || key[0] === undefined) {
          throw corruption("temp", "record key is invalid");
        }
        if (key[0] === "owner") {
          const owner = asTempOwnerRecord(value);
          if (key.length !== 2 || key[1] !== owner.ownerId) {
            throw corruption("temp", "owner key is inconsistent");
          }
          observedOwnerCount = incrementSafeInteger(
            observedOwnerCount,
            "Integrity temp owner count",
          );
          asTempOwnerQuotaRecord(
            await requestResult<unknown>(tempStore.get(tempOwnerQuotaKey(owner.ownerId))),
            owner.ownerId,
          );
        } else if (key[0] === "run") {
          if (
            key.length !== 4 ||
            !isStorageId(key[1]) ||
            !isStorageId(key[2]) ||
            !Number.isSafeInteger(key[3]) ||
            (key[3] as number) < 0
          ) {
            throw corruption("temp", "run page key is invalid");
          }
          const bytes = asBytes(value, `temp/${storageKeyLocation(key)}`);
          if (runOwnerId !== key[1] || runId !== key[2]) {
            const previousOwnerId = runOwnerId;
            await finishObservedRun();
            if (previousOwnerId !== undefined && previousOwnerId !== key[1]) {
              await finishObservedOwner();
            }
            runOwnerId = key[1];
            runId = key[2];
            if ((await requestResult(tempStore.getKey(tempOwnerKey(runOwnerId)))) === undefined) {
              issue("orphan-temp-run", `temp/${runOwnerId}`, "run pages have no owner record");
            }
          }
          runPageCount = incrementSafeInteger(runPageCount, "Integrity temp page count");
          runBytes = safeByteSum(runBytes, bytes.byteLength, "Integrity temp run bytes");
          observedPageCount = incrementSafeInteger(observedPageCount, "Integrity temp page count");
          observedBytes = safeByteSum(observedBytes, bytes.byteLength, "Integrity temp bytes");
        } else if (key[0] === TEMP_QUOTA && key[1] === "global" && key.length === 2) {
          persistedTempGlobal = asTempGlobalQuotaRecord(value);
        } else if (
          key[0] === TEMP_QUOTA &&
          key[1] === "owner" &&
          key.length === 3 &&
          isStorageId(key[2])
        ) {
          asTempOwnerQuotaRecord(value, key[2]);
          if ((await requestResult(tempStore.getKey(tempOwnerKey(key[2])))) === undefined) {
            throw corruption("temp", "owner quota has no owner record");
          }
        } else if (
          key[0] === TEMP_QUOTA &&
          key[1] === "run" &&
          key.length === 4 &&
          isStorageId(key[2]) &&
          isStorageId(key[3])
        ) {
          const run = asTempRunQuotaRecord(value, key[2], key[3]);
          if (run === undefined) throw corruption("temp", "run quota is missing");
          if (!(await tempRunHasAnyPage(tempStore, key[2], key[3]))) {
            throw corruption("temp", "run quota has no pages");
          }
        } else throw corruption("temp", "record kind is unknown");
      } catch (error) {
        issue("invalid-temp-record", storageKeyLocation(key), error);
      }
    });
    await finishObservedRun();
    await finishObservedOwner();
    const expectedGlobal: TempGlobalQuotaRecord = {
      ownerCount: observedOwnerCount,
      runCount: observedRunCount,
      pageCount: observedPageCount,
      retainedBytes: observedBytes,
    };
    if (
      persistedTempGlobal !== undefined &&
      (persistedTempGlobal.ownerCount !== expectedGlobal.ownerCount ||
        persistedTempGlobal.runCount !== expectedGlobal.runCount ||
        persistedTempGlobal.pageCount !== expectedGlobal.pageCount ||
        persistedTempGlobal.retainedBytes !== expectedGlobal.retainedBytes)
    ) {
      issue("temp-quota-mismatch", "temp/quota", "global quota disagrees with temp records");
    }
    let persistedResourceLedger: ResourceLedgerRecord | undefined;
    let persistedCatalogResourceLedger: CatalogResourceLedgerRecord | undefined;
    let persistedRecordResourceLedger: RecordResourceLedgerRecord | undefined;
    let stagedBlockCount = 0;
    let stagedSegmentCount = 0;
    let stagedBytes = 0;
    await visitObjectStoreSequentially(
      transaction.objectStore("statistics"),
      async (value, key) => {
        checkedRecords += 1;
        try {
          if (key === RESOURCE_LEDGER_KEY) {
            persistedResourceLedger = asResourceLedger(value);
            return;
          }
          if (key === CATALOG_RESOURCE_LEDGER_KEY) {
            persistedCatalogResourceLedger = asCatalogResourceLedger(value);
            return;
          }
          if (key === RECORD_RESOURCE_LEDGER_KEY) {
            persistedRecordResourceLedger = asRecordResourceLedger(value);
            return;
          }
          if (typeof key !== "string" || !key.startsWith(TRANSACTION_RESOURCE_LEDGER_PREFIX)) {
            throw corruption(
              `statistics/${storageKeyLocation(key)}`,
              "resource ledger key is unknown",
            );
          }
          const transactionId = decodeURIComponent(
            key.slice(TRANSACTION_RESOURCE_LEDGER_PREFIX.length),
          );
          const ledger = asOptionalTransactionResourceLedger(value, transactionId);
          if (ledger === undefined)
            throw corruption(`statistics/${key}`, "transaction ledger is missing");
          const transactionValue: unknown = await requestResult(
            transactionStore.get(transactionId),
          );
          if (transactionValue === undefined) {
            throw corruption(`statistics/${key}`, "transaction resource ledger has no transaction");
          }
          const owner = asTransactionRecord(transactionValue, transactionId);
          if (
            owner.status === "committed" ||
            ledger.blockCount !== owner.pendingBlockIds.length ||
            ledger.segmentCount !== owner.pendingSegmentIds.length
          ) {
            throw corruption(
              `statistics/${key}`,
              "transaction resource ledger disagrees with its journal",
            );
          }
          stagedBlockCount = safeByteSum(
            stagedBlockCount,
            ledger.blockCount,
            "Integrity staged blocks",
          );
          stagedSegmentCount = safeByteSum(
            stagedSegmentCount,
            ledger.segmentCount,
            "Integrity staged segments",
          );
          stagedBytes = safeByteSum(stagedBytes, ledger.retainedBytes, "Integrity staged bytes");
        } catch (error) {
          issue("invalid-resource-ledger", storageKeyLocation(key), error);
        }
      },
    );
    const resourceLedgerMatches =
      persistedResourceLedger?.stagedBlockCount === stagedBlockCount &&
      persistedResourceLedger.stagedSegmentCount === stagedSegmentCount &&
      persistedResourceLedger.stagedBytes === stagedBytes;
    if (!resourceLedgerMatches) {
      issue(
        "resource-ledger-mismatch",
        `statistics/${RESOURCE_LEDGER_KEY}`,
        "global staged-artifact ledger disagrees with transaction ledgers",
      );
    }
    let observedCatalogRetainedBytes = 0;
    for (const table of tablesById.values()) {
      observedCatalogRetainedBytes = safeByteSum(
        observedCatalogRetainedBytes,
        catalogRecordRetainedBytes(table),
        "Integrity catalog retained bytes",
      );
    }
    const observedCatalogLedger = withCatalogResourceLedgerChecksum({
      recordCount: tablesById.size,
      retainedBytes: observedCatalogRetainedBytes,
    });
    if (
      persistedCatalogResourceLedger?.recordCount !== observedCatalogLedger.recordCount ||
      persistedCatalogResourceLedger.retainedBytes !== observedCatalogLedger.retainedBytes ||
      persistedCatalogResourceLedger.checksum !== observedCatalogLedger.checksum
    ) {
      issue(
        "catalog-resource-ledger-mismatch",
        `statistics/${CATALOG_RESOURCE_LEDGER_KEY}`,
        "catalog resource ledger disagrees with table records",
      );
    }
    if (
      persistedRecordResourceLedger?.manifestCount !== observedManifestCount ||
      persistedRecordResourceLedger.manifestBytes !== observedManifestBytes ||
      persistedRecordResourceLedger.segmentCount !== observedSegmentCount ||
      persistedRecordResourceLedger.segmentBytes !== observedSegmentBytes
    ) {
      issue(
        "record-resource-ledger-mismatch",
        `statistics/${RECORD_RESOURCE_LEDGER_KEY}`,
        "manifest/segment resource ledger disagrees with durable records",
      );
    }
    let observedRetiredHistoryBytes = 0;
    await visitManifestBlockRecords(catalog, async (record) => {
      if (
        record.removedVersion !== null &&
        (await requestResult(blockStore.getKey(record.blockId))) !== undefined
      ) {
        observedRetiredHistoryBytes = safeByteSum(
          observedRetiredHistoryBytes,
          record.byteLength,
          "Integrity retired history bytes",
        );
      }
      return undefined;
    });
    if (persistedResourceLedger?.retiredHistoryBytes !== observedRetiredHistoryBytes) {
      issue(
        "resource-ledger-mismatch",
        `statistics/${RESOURCE_LEDGER_KEY}`,
        "retired-history byte ledger disagrees with manifest provenance",
      );
    }

    await visitObjectStoreSequentially(transaction.objectStore("blocks"), async (value, key) => {
      if (typeof key !== "string" || key.length === 0) {
        issue("invalid-block-key", storageKeyLocation(key), "block key is invalid");
        return;
      }
      try {
        const bytes = asBytes(value, `blocks/${key}`);
        const metadataValue: unknown = await requestResult(catalog.get(blockMetadataKey(key)));
        if (metadataValue === undefined) {
          throw corruption(blockMetadataKey(key), "block metadata is missing");
        }
        const metadata = asStoredBlockMetadata(metadataValue, key);
        if (metadata.byteLength !== bytes.byteLength) {
          throw corruption(blockMetadataKey(key), "block byte length disagrees with its payload");
        }
        if (crc32(bytes) !== metadata.checksum) {
          throw corruption(blockMetadataKey(key), "block checksum disagrees with its payload");
        }
        if (mode === "full") {
          checkedBlocks += 1;
          checkedBytes = safeByteSum(checkedBytes, bytes.byteLength, "Integrity checked bytes");
          verifyStoredBlock(bytes);
        }
      } catch (error) {
        issue("invalid-block", `blocks/${key}`, error);
      }
    });
    await transactionDone(transaction);
    return {
      mode,
      ok: issueCount === 0,
      checkedRecords,
      checkedBlocks,
      checkedBytes,
      issueCount,
      issues,
    };
  }

  /** Captures one immutable, framed view and its durable GC pin without reading payload bytes. */
  async beginSnapshotFrameExport(
    input: BeginSnapshotFrameExportInput,
  ): Promise<SnapshotFrameExportSession> {
    validateId(input.ownerId, "Snapshot export owner ID");
    const createdAtCutoff = validateBoundedExpiration(
      input.createdAt,
      input.expiresAt,
      "Snapshot export",
      MAX_SNAPSHOT_SESSION_TTL_MS,
    );
    const sessionId = `snapshot-export/${crypto.randomUUID()}`;
    const transaction = this.#transaction(
      ["catalog", "leases", "manifests", "segments", "transactions", SNAPSHOT_HEADER_STORE],
      "readwrite",
    );
    try {
      const catalog = transaction.objectStore("catalog");
      const leases = transaction.objectStore("leases");
      const frameStore = transaction.objectStore(SNAPSHOT_HEADER_STORE);
      const markerValue: unknown = await requestResult(catalog.get(SNAPSHOT_EXPORT_KEY));
      if (markerValue !== undefined) {
        const marker = asSnapshotFrameExportMarker(markerValue);
        const leaseValue: unknown = await requestResult(leases.get(marker.sessionId));
        if (leaseValue === undefined) {
          throw corruption(SNAPSHOT_EXPORT_KEY, "active export marker has no lease");
        }
        const lease = asLeaseRecord(leaseValue, marker.sessionId);
        assertSnapshotFrameExportLease(marker, lease);
        if (Date.parse(marker.expiresAt) > createdAtCutoff) {
          throw new Error(`Snapshot export is already active: ${marker.sessionId}`);
        }
        leases.delete(marker.sessionId);
        catalog.delete(SNAPSHOT_EXPORT_KEY);
        frameStore.clear();
      }
      const version = asOptionalManifestVersion(
        await requestResult<unknown>(catalog.get(CURRENT_MANIFEST_KEY)),
        CURRENT_MANIFEST_KEY,
      );
      if (version === undefined) throw new Error("There is no committed version to snapshot");
      const manifestValue: unknown = await requestResult(
        transaction.objectStore("manifests").get(version),
      );
      if (manifestValue === undefined) {
        throw corruption(CURRENT_MANIFEST_KEY, `points to missing manifest ${String(version)}`);
      }
      const manifest = asStoredManifestRecord(manifestValue, version);
      this.#snapshotPeakRetainedItems = 0;
      this.#snapshotPeakRetainedBytes = 0;
      const { summaries, frameCount: metadataFrameCount } =
        await writeSnapshotMetadataFramesInTransaction({
          transaction,
          manifest,
          version,
          direction: "export",
          identity: sessionId,
          observeRetainedItems: (count) => {
            this.#snapshotPeakRetainedItems = Math.max(this.#snapshotPeakRetainedItems, count);
          },
          observeRetainedBytes: (bytes) => {
            this.#snapshotPeakRetainedBytes = Math.max(this.#snapshotPeakRetainedBytes, bytes);
          },
        });
      summaries.block = {
        frameCount: manifest.liveBlockCount,
        itemCount: manifest.liveBlockCount,
        storedBytes: manifest.liveBlockBytes,
      };
      const header = prepareSnapshotFrameStreamHeader({
        formatVersion: 1,
        databaseVersion: version,
        createdAt: input.createdAt,
        kinds: summaries,
      });
      const lease: LeaseRecord = {
        id: sessionId,
        kind: "backup",
        manifestVersion: version,
        ownerId: input.ownerId,
        createdAt: input.createdAt,
        expiresAt: input.expiresAt,
        revision: 0,
      };
      await assertPinnedHistoryAdmission(transaction, {
        cutoff: input.createdAt,
        currentVersion: version,
        replacementLease: lease,
      });
      const marker: SnapshotFrameExportMarker = {
        kind: "snapshot-frame-export",
        sessionId,
        ownerId: input.ownerId,
        manifestVersion: version,
        createdAt: input.createdAt,
        expiresAt: input.expiresAt,
        revision: 0,
        header,
        metadataFrameCount,
        nextBlockIndex: 0,
        lastBlockId: null,
      };
      leases.add(lease, sessionId);
      catalog.add(marker, SNAPSHOT_EXPORT_KEY);
      await transactionDone(transaction);
      return { sessionId, ownerId: input.ownerId, expiresAt: input.expiresAt, header };
    } catch (error) {
      abortIfActive(transaction);
      await ignoreAbort(transaction);
      throw error;
    }
  }

  /** Reads one frozen metadata page or one live-at-version payload and renews the pin atomically. */
  async readSnapshotExportFrame(
    input: ReadSnapshotExportFrameInput,
  ): Promise<SnapshotFrame | undefined> {
    validateId(input.sessionId, "Snapshot export session ID");
    validateId(input.ownerId, "Snapshot export owner ID");
    if (!Number.isSafeInteger(input.sequence) || input.sequence < 0) {
      throw new RangeError("Snapshot frame sequence must be a non-negative safe integer");
    }
    const cutoffAt = canonicalInputTimestamp(
      input.expiresAtCutoff,
      "Snapshot export expiry cutoff",
    );
    validateBoundedExpiration(
      input.expiresAtCutoff,
      input.expiresAt,
      "Snapshot export renewal",
      MAX_SNAPSHOT_SESSION_TTL_MS,
    );
    const transaction = this.#transaction(
      ["blocks", "catalog", "leases", SNAPSHOT_HEADER_STORE],
      "readwrite",
    );
    try {
      const catalog = transaction.objectStore("catalog");
      const leases = transaction.objectStore("leases");
      const marker = asSnapshotFrameExportMarker(
        await requestResult<unknown>(catalog.get(SNAPSHOT_EXPORT_KEY)),
      );
      if (marker.sessionId !== input.sessionId || marker.ownerId !== input.ownerId) {
        throw new LeaseOwnerConflictError(input.sessionId, input.ownerId, marker.ownerId);
      }
      const lease = asLeaseRecord(
        await requestResult<unknown>(leases.get(input.sessionId)),
        input.sessionId,
      );
      assertSnapshotFrameExportLease(marker, lease);
      if (Date.parse(lease.expiresAt) <= Date.parse(cutoffAt)) {
        throw new LeaseExpiredError(input.sessionId, lease.expiresAt, cutoffAt);
      }
      const totalFrames = snapshotHeaderFrameCount(marker.header);
      let frame: SnapshotFrame | undefined;
      let updatedMarker = marker;
      if (input.sequence < marker.metadataFrameCount) {
        frame = asSnapshotFrame(
          await requestResult<unknown>(
            transaction
              .objectStore(SNAPSHOT_HEADER_STORE)
              .get(snapshotFrameKey("export", marker.sessionId, input.sequence)),
          ),
          input.sequence,
        );
      } else if (input.sequence < totalFrames) {
        const blockIndex = input.sequence - marker.metadataFrameCount;
        let blockRecord: ManifestBlockRecord;
        if (blockIndex === marker.nextBlockIndex) {
          const next = await nextManifestBlockRecord(
            catalog.index(MANIFEST_BLOCK_ID_INDEX),
            marker.manifestVersion,
            marker.lastBlockId,
          );
          if (next === undefined) {
            throw corruption(SNAPSHOT_EXPORT_KEY, "manifest block count exceeds provenance");
          }
          blockRecord = next;
          updatedMarker = {
            ...marker,
            nextBlockIndex: incrementSafeInteger(blockIndex, "Snapshot block cursor"),
            lastBlockId: blockRecord.blockId,
          };
        } else if (blockIndex + 1 === marker.nextBlockIndex && marker.lastBlockId !== null) {
          blockRecord = asManifestBlockRecord(
            await requestResult<unknown>(catalog.get(manifestBlockKey(marker.lastBlockId))),
            marker.lastBlockId,
          );
          if (!manifestBlockVisibleAt(blockRecord, marker.manifestVersion)) {
            throw corruption(SNAPSHOT_EXPORT_KEY, "replayed block is outside the pinned manifest");
          }
        } else {
          throw new RangeError("Snapshot block frames must be read contiguously");
        }
        const value: unknown = await requestResult(
          transaction.objectStore("blocks").get(blockRecord.blockId),
        );
        if (value === undefined) {
          throw corruption(`blocks/${blockRecord.blockId}`, "pinned block is missing");
        }
        const payload = asBytes(value, `blocks/${blockRecord.blockId}`);
        const metadata = asStoredBlockMetadata(
          await requestResult<unknown>(catalog.get(blockMetadataKey(blockRecord.blockId))),
          blockRecord.blockId,
        );
        if (
          payload.byteLength !== blockRecord.byteLength ||
          payload.byteLength !== metadata.byteLength ||
          crc32(payload) !== blockRecord.checksum ||
          metadata.checksum !== blockRecord.checksum
        ) {
          throw corruption(
            `blocks/${blockRecord.blockId}`,
            "pinned block metadata is inconsistent",
          );
        }
        frame = {
          sequence: input.sequence,
          kind: "block",
          itemCount: 1,
          key: blockRecord.blockId,
          payload,
          checksum: blockRecord.checksum,
        };
      }
      const revision = incrementSafeInteger(lease.revision, "Snapshot export revision");
      leases.put({ ...lease, expiresAt: input.expiresAt, revision }, input.sessionId);
      catalog.put({ ...updatedMarker, expiresAt: input.expiresAt, revision }, SNAPSHOT_EXPORT_KEY);
      await transactionDone(transaction);
      return frame;
    } catch (error) {
      abortIfActive(transaction);
      await ignoreAbort(transaction);
      throw error;
    }
  }

  async closeSnapshotFrameExport(input: CloseSnapshotExportInput): Promise<boolean> {
    validateId(input.sessionId, "Snapshot export session ID");
    validateId(input.ownerId, "Snapshot export owner ID");
    const transaction = this.#transaction(
      ["catalog", "leases", SNAPSHOT_HEADER_STORE],
      "readwrite",
    );
    try {
      const catalog = transaction.objectStore("catalog");
      const raw = await requestResult<unknown>(catalog.get(SNAPSHOT_EXPORT_KEY));
      if (raw === undefined) {
        await transactionDone(transaction);
        return false;
      }
      const marker = asSnapshotFrameExportMarker(raw);
      if (marker.sessionId !== input.sessionId || marker.ownerId !== input.ownerId) {
        throw new LeaseOwnerConflictError(input.sessionId, input.ownerId, marker.ownerId);
      }
      const leases = transaction.objectStore("leases");
      const lease = asLeaseRecord(
        await requestResult<unknown>(leases.get(input.sessionId)),
        input.sessionId,
      );
      assertSnapshotFrameExportLease(marker, lease);
      leases.delete(input.sessionId);
      catalog.delete(SNAPSHOT_EXPORT_KEY);
      await deleteSnapshotFrameRecords(transaction.objectStore(SNAPSHOT_HEADER_STORE), "export");
      await transactionDone(transaction);
      return true;
    } catch (error) {
      abortIfActive(transaction);
      await ignoreAbort(transaction);
      throw error;
    }
  }

  async beginSnapshotFrameImport(
    input: BeginSnapshotFrameImportInput,
  ): Promise<SnapshotFrameImportSession> {
    validateId(input.identity, "Snapshot identity");
    validateId(input.ownerId, "Snapshot import owner ID");
    validateBoundedExpiration(
      input.createdAt,
      input.expiresAt,
      "Snapshot import",
      MAX_SNAPSHOT_SESSION_TTL_MS,
    );
    const header = prepareSnapshotFrameStreamHeader(input.header);
    if (snapshotFrameStreamHeaderIdentity(header) !== input.identity) {
      throw new SnapshotImportConflictError(
        input.identity,
        input.ownerId,
        "header identity differs",
      );
    }
    const transaction = this.#transaction(
      [
        "blocks",
        "catalog",
        "manifests",
        "segments",
        "statistics",
        "transactions",
        SNAPSHOT_HEADER_STORE,
      ],
      "readwrite",
      { allowSnapshotImport: true },
    );
    try {
      const catalog = transaction.objectStore("catalog");
      const currentVersion = asOptionalManifestVersion(
        await requestResult<unknown>(catalog.get(CURRENT_MANIFEST_KEY)),
        CURRENT_MANIFEST_KEY,
      );
      const completed = asOptionalCompletedSnapshotFrameImportRecord(
        await requestResult<unknown>(catalog.get(SNAPSHOT_FRAME_COMPLETED_KEY)),
      );
      if (currentVersion !== undefined) {
        if (
          completed?.identity === input.identity &&
          completed.version === currentVersion &&
          sameSnapshotFrameHeader(completed.header, header)
        ) {
          const manifestValue = await requestResult<unknown>(
            transaction.objectStore("manifests").get(currentVersion),
          );
          if (manifestValue === undefined) {
            throw corruption(CURRENT_MANIFEST_KEY, "completed replay manifest is missing");
          }
          const manifest = asStoredManifestRecord(manifestValue, currentVersion);
          this.#snapshotPeakRetainedItems = 0;
          this.#snapshotPeakRetainedBytes = 0;
          const frameStore = transaction.objectStore(SNAPSHOT_HEADER_STORE);
          await deleteSnapshotFrameRecords(frameStore, "import");
          const { frameCount } = await writeSnapshotMetadataFramesInTransaction({
            transaction,
            manifest,
            version: currentVersion,
            direction: "import",
            identity: input.identity,
            observeRetainedItems: (count) => {
              this.#snapshotPeakRetainedItems = Math.max(this.#snapshotPeakRetainedItems, count);
            },
            observeRetainedBytes: (bytes) => {
              this.#snapshotPeakRetainedBytes = Math.max(this.#snapshotPeakRetainedBytes, bytes);
            },
          });
          const metadataFrameCount = SNAPSHOT_FRAME_KINDS.filter((kind) => kind !== "block").reduce(
            (total, kind) => total + header.kinds[kind].frameCount,
            0,
          );
          if (frameCount !== metadataFrameCount) {
            throw new SnapshotImportConflictError(
              input.identity,
              input.ownerId,
              "completed database metadata has advanced",
            );
          }
          const replay: SnapshotFrameImportMarker = {
            kind: "snapshot-frame-import",
            identity: input.identity,
            ownerId: input.ownerId,
            version: currentVersion,
            createdAt: input.createdAt,
            expiresAt: input.expiresAt,
            header,
            nextSequence: 0,
            stagedBytes: 0,
            frameCount: 0,
            itemCount: 0,
            checksum: 0,
            kindFrameCounts: SNAPSHOT_FRAME_KINDS.map(() => 0),
            kindItemCounts: SNAPSHOT_FRAME_KINDS.map(() => 0),
            kindStoredBytes: SNAPSHOT_FRAME_KINDS.map(() => 0),
            replayCompleted: true,
          };
          catalog.put(replay, SNAPSHOT_FRAME_IMPORT_KEY);
          await transactionDone(transaction);
          return snapshotFrameImportSession(replay);
        }
        throw new Error("This store already holds a database");
      }
      const rawMarker = await requestResult<unknown>(catalog.get(SNAPSHOT_FRAME_IMPORT_KEY));
      if (rawMarker !== undefined) {
        const existing = asSnapshotFrameImportMarker(rawMarker);
        if (
          existing.identity === input.identity &&
          sameSnapshotFrameHeader(existing.header, header)
        ) {
          if (Date.parse(existing.expiresAt) > Date.parse(input.createdAt)) {
            if (existing.ownerId !== input.ownerId) {
              throw new SnapshotImportConflictError(
                input.identity,
                existing.ownerId,
                "session is owned by another caller",
              );
            }
            await transactionDone(transaction);
            return snapshotFrameImportSession(existing);
          }
          const adopted = { ...existing, ownerId: input.ownerId, expiresAt: input.expiresAt };
          catalog.put(adopted, SNAPSHOT_FRAME_IMPORT_KEY);
          await transactionDone(transaction);
          return snapshotFrameImportSession(adopted);
        }
        if (Date.parse(existing.expiresAt) > Date.parse(input.createdAt)) {
          throw new SnapshotImportConflictError(
            input.identity,
            existing.ownerId,
            "a different import is active",
          );
        }
        // A different expired stream cannot inherit any durable prefix from its predecessor.
        // Clearing and reseeding every import-owned store in this one transaction prevents a
        // crash or quota failure from publishing a hybrid of two snapshots.
        transaction.objectStore("blocks").clear();
        catalog.clear();
        transaction.objectStore("manifests").clear();
        transaction.objectStore("segments").clear();
        transaction.objectStore("transactions").clear();
        transaction.objectStore(SNAPSHOT_HEADER_STORE).clear();
        const statistics = transaction.objectStore("statistics");
        statistics.clear();
        statistics.put(emptyResourceLedger(), RESOURCE_LEDGER_KEY);
        statistics.put(emptyCatalogResourceLedger(), CATALOG_RESOURCE_LEDGER_KEY);
        statistics.put(emptyRecordResourceLedger(), RECORD_RESOURCE_LEDGER_KEY);
      }
      const marker: SnapshotFrameImportMarker = {
        kind: "snapshot-frame-import",
        identity: input.identity,
        ownerId: input.ownerId,
        version: header.databaseVersion,
        createdAt: input.createdAt,
        expiresAt: input.expiresAt,
        header,
        nextSequence: 0,
        stagedBytes: 0,
        frameCount: 0,
        itemCount: 0,
        checksum: 0,
        kindFrameCounts: SNAPSHOT_FRAME_KINDS.map(() => 0),
        kindItemCounts: SNAPSHOT_FRAME_KINDS.map(() => 0),
        kindStoredBytes: SNAPSHOT_FRAME_KINDS.map(() => 0),
        replayCompleted: false,
      };
      catalog.put(marker, SNAPSHOT_FRAME_IMPORT_KEY);
      await transactionDone(transaction);
      return snapshotFrameImportSession(marker);
    } catch (error) {
      abortIfActive(transaction);
      await ignoreAbort(transaction);
      throw error;
    }
  }

  async renewSnapshotFrameImport(
    input: RenewSnapshotFrameImportInput,
  ): Promise<SnapshotFrameImportSession> {
    validateId(input.identity, "Snapshot identity");
    validateId(input.ownerId, "Snapshot import owner ID");
    validateBoundedExpiration(
      input.expiresAtCutoff,
      input.expiresAt,
      "Snapshot import renewal",
      MAX_SNAPSHOT_SESSION_TTL_MS,
    );
    const cutoff = Date.parse(canonicalInputTimestamp(input.expiresAtCutoff, "Snapshot cutoff"));
    const transaction = this.#transaction("catalog", "readwrite", { allowSnapshotImport: true });
    try {
      const catalog = transaction.objectStore("catalog");
      const marker = requireSnapshotFrameImportMarker(
        await requestResult<unknown>(catalog.get(SNAPSHOT_FRAME_IMPORT_KEY)),
        input.identity,
        input.ownerId,
        cutoff,
      );
      const renewed = { ...marker, expiresAt: input.expiresAt };
      catalog.put(renewed, SNAPSHOT_FRAME_IMPORT_KEY);
      await transactionDone(transaction);
      return snapshotFrameImportSession(renewed);
    } catch (error) {
      abortIfActive(transaction);
      await ignoreAbort(transaction);
      throw error;
    }
  }

  async appendSnapshotImportFrames(
    input: AppendSnapshotImportFramesInput,
  ): Promise<SnapshotFrameImportSession> {
    validateSnapshotFrameBatch(input.frames);
    validateId(input.identity, "Snapshot identity");
    validateId(input.ownerId, "Snapshot import owner ID");
    validateBoundedExpiration(
      input.expiresAtCutoff,
      input.expiresAt,
      "Snapshot import renewal",
      MAX_SNAPSHOT_SESSION_TTL_MS,
    );
    const cutoff = Date.parse(canonicalInputTimestamp(input.expiresAtCutoff, "Snapshot cutoff"));
    const stores = [
      "blocks",
      "catalog",
      "segments",
      "statistics",
      "transactions",
      SNAPSHOT_HEADER_STORE,
    ];
    const transaction = this.#transaction(stores, "readwrite", { allowSnapshotImport: true });
    try {
      const catalog = transaction.objectStore("catalog");
      const frameStore = transaction.objectStore(SNAPSHOT_HEADER_STORE);
      let marker = requireSnapshotFrameImportMarker(
        await requestResult<unknown>(catalog.get(SNAPSHOT_FRAME_IMPORT_KEY)),
        input.identity,
        input.ownerId,
        cutoff,
      );
      for (const candidate of input.frames) {
        const frame = prepareSnapshotFrame(candidate, marker.header);
        if (frame.sequence < marker.nextSequence) {
          await assertSnapshotImportFrameReplay(
            transaction.objectStore("blocks"),
            catalog,
            frameStore,
            marker,
            frame,
          );
          continue;
        }
        if (frame.sequence !== marker.nextSequence) {
          throw new RangeError("Snapshot import frames must be contiguous");
        }
        const kindIndex = SNAPSHOT_FRAME_KINDS.indexOf(frame.kind);
        const expected = marker.header.kinds[frame.kind];
        const kindFrameCount = incrementSafeInteger(
          marker.kindFrameCounts[kindIndex] ?? 0,
          "Snapshot kind frame count",
        );
        const kindItemCount = safeByteSum(
          marker.kindItemCounts[kindIndex] ?? 0,
          frame.itemCount,
          "Snapshot kind item count",
        );
        const kindStoredBytes = safeByteSum(
          marker.kindStoredBytes[kindIndex] ?? 0,
          frame.payload.byteLength,
          "Snapshot kind stored bytes",
        );
        if (
          kindFrameCount > expected.frameCount ||
          kindItemCount > expected.itemCount ||
          kindStoredBytes > expected.storedBytes
        ) {
          throw new RangeError(`Snapshot ${frame.kind} frames exceed the header`);
        }
        const earlierIncomplete = marker.kindFrameCounts.some(
          (count, index) =>
            index < kindIndex &&
            count !== marker.header.kinds[SNAPSHOT_FRAME_KINDS[index] ?? "block"].frameCount,
        );
        const laterStarted = marker.kindFrameCounts.some(
          (count, index) => index > kindIndex && count > 0,
        );
        if (earlierIncomplete || laterStarted) {
          throw new TypeError("Snapshot frame kinds are not canonical");
        }
        if (frame.kind === "block") {
          const id = frame.key ?? "";
          const existing = await requestResult<unknown>(transaction.objectStore("blocks").get(id));
          if (
            existing !== undefined &&
            !sameBytes(asBytes(existing, `blocks/${id}`), frame.payload)
          ) {
            throw new SnapshotImportConflictError(
              input.identity,
              input.ownerId,
              "block replay differs",
            );
          }
          if (marker.replayCompleted && existing === undefined) {
            throw new SnapshotImportConflictError(
              input.identity,
              input.ownerId,
              "completed block is missing",
            );
          }
          if (!marker.replayCompleted && existing === undefined) {
            transaction.objectStore("blocks").add(frame.payload.slice(), id);
          }
          const metadataValue = await requestResult<unknown>(catalog.get(blockMetadataKey(id)));
          if (metadataValue === undefined) {
            if (marker.replayCompleted) {
              throw corruption(blockMetadataKey(id), "completed block metadata is missing");
            }
            catalog.add(
              {
                byteLength: frame.payload.byteLength,
                checksum: frame.checksum,
              } satisfies StoredBlockMetadata,
              blockMetadataKey(id),
            );
            catalog.add(
              {
                blockId: id,
                byteLength: frame.payload.byteLength,
                checksum: frame.checksum,
                addedVersion: marker.version,
                removedVersion: null,
              } satisfies ManifestBlockRecord,
              manifestBlockKey(id),
            );
          } else {
            const metadata = asStoredBlockMetadata(metadataValue, id);
            if (
              metadata.byteLength !== frame.payload.byteLength ||
              metadata.checksum !== frame.checksum
            ) {
              throw corruption(blockMetadataKey(id), "staged block metadata differs");
            }
            const provenance = asManifestBlockRecord(
              await requestResult<unknown>(catalog.get(manifestBlockKey(id))),
              id,
            );
            if (
              provenance.byteLength !== frame.payload.byteLength ||
              provenance.checksum !== frame.checksum ||
              !manifestBlockVisibleAt(provenance, marker.version)
            ) {
              throw corruption(
                storageKeyLocation(manifestBlockKey(id)),
                "staged block provenance differs",
              );
            }
          }
          const frameKey = snapshotFrameKey("import", marker.identity, frame.sequence);
          if (marker.replayCompleted) {
            frameStore.put(snapshotBlockFrameRecord(frame), frameKey);
          } else {
            frameStore.add(snapshotBlockFrameRecord(frame), frameKey);
          }
        } else if (marker.replayCompleted) {
          const stored = asSnapshotFrame(
            await requestResult<unknown>(
              frameStore.get(snapshotFrameKey("import", marker.identity, frame.sequence)),
            ),
            frame.sequence,
          );
          if (!sameSnapshotFrame(stored, frame)) {
            throw new SnapshotImportConflictError(
              input.identity,
              input.ownerId,
              `replayed frame ${String(frame.sequence)} differs`,
            );
          }
        } else {
          await stageSnapshotMetadataFrame(transaction, frame);
          frameStore.add(frame, snapshotFrameKey("import", marker.identity, frame.sequence));
        }
        const frameCounts = [...marker.kindFrameCounts];
        const itemCounts = [...marker.kindItemCounts];
        const storedBytes = [...marker.kindStoredBytes];
        frameCounts[kindIndex] = kindFrameCount;
        itemCounts[kindIndex] = kindItemCount;
        storedBytes[kindIndex] = kindStoredBytes;
        marker = {
          ...marker,
          expiresAt: input.expiresAt,
          nextSequence: incrementSafeInteger(frame.sequence, "Snapshot frame sequence"),
          stagedBytes: safeByteSum(
            marker.stagedBytes,
            frame.payload.byteLength,
            "Snapshot staged bytes",
          ),
          frameCount: incrementSafeInteger(marker.frameCount, "Snapshot frame count"),
          itemCount: safeByteSum(marker.itemCount, frame.itemCount, "Snapshot item count"),
          checksum: extendSnapshotFrameStreamChecksum(
            marker.checksum,
            snapshotFrameEnvelopeParts(frame),
          ),
          kindFrameCounts: frameCounts,
          kindItemCounts: itemCounts,
          kindStoredBytes: storedBytes,
        };
      }
      catalog.put(marker, SNAPSHOT_FRAME_IMPORT_KEY);
      await transactionDone(transaction);
      return snapshotFrameImportSession(marker);
    } catch (error) {
      abortIfActive(transaction);
      await ignoreAbort(transaction);
      throw error;
    }
  }

  async finishSnapshotFrameImport(input: FinishSnapshotFrameImportInput): Promise<void> {
    validateId(input.identity, "Snapshot identity");
    validateId(input.ownerId, "Snapshot import owner ID");
    const cutoff = Date.parse(canonicalInputTimestamp(input.expiresAtCutoff, "Snapshot cutoff"));
    const transaction = this.#transaction([...indexedDbStoreNames], "readwrite", {
      allowSnapshotImport: true,
    });
    try {
      const catalog = transaction.objectStore("catalog");
      const marker = requireSnapshotFrameImportMarker(
        await requestResult<unknown>(catalog.get(SNAPSHOT_FRAME_IMPORT_KEY)),
        input.identity,
        input.ownerId,
        cutoff,
      );
      validateSnapshotFrameFooter(marker, input.footer);
      if (marker.replayCompleted) {
        const currentVersion = asOptionalManifestVersion(
          await requestResult<unknown>(catalog.get(CURRENT_MANIFEST_KEY)),
          CURRENT_MANIFEST_KEY,
        );
        if (currentVersion !== marker.version) {
          throw new SnapshotImportConflictError(
            marker.identity,
            marker.ownerId,
            "completed database has advanced",
          );
        }
        catalog.delete(SNAPSHOT_FRAME_IMPORT_KEY);
        await deleteSnapshotFrameRecords(transaction.objectStore(SNAPSHOT_HEADER_STORE), "import");
        await transactionDone(transaction);
        return;
      }
      await validateAndPromoteStagedSnapshot(transaction, marker);
      const manifest: StoredManifestRecord = {
        version: marker.version,
        previousVersion: null,
        liveBlockCount: marker.header.kinds.block.itemCount,
        liveBlockBytes: marker.header.kinds.block.storedBytes,
        changedTableIds: [],
        createdAt: marker.header.createdAt,
      };
      await updateRecordResourceLedger(transaction.objectStore("statistics"), {
        manifests: [{ next: manifest }],
      });
      transaction.objectStore("manifests").add(manifest, marker.version);
      catalog.put(marker.version, CURRENT_MANIFEST_KEY);
      catalog.put(0, CATALOG_EPOCH_KEY);
      catalog.put(0, SCHEMA_EPOCH_KEY);
      catalog.put(
        {
          kind: "snapshot-frame-import-completed",
          identity: marker.identity,
          version: marker.version,
          createdAt: marker.createdAt,
          header: marker.header,
        } satisfies CompletedSnapshotFrameImportRecord,
        SNAPSHOT_FRAME_COMPLETED_KEY,
      );
      catalog.delete(SNAPSHOT_FRAME_IMPORT_KEY);
      await deleteSnapshotFrameRecords(transaction.objectStore(SNAPSHOT_HEADER_STORE), "import");
      await transactionDone(transaction);
      this.#tableIdsByName.clear();
      this.#uniqueKeyCache = undefined;
      this.#manifestCache = undefined;
    } catch (error) {
      abortIfActive(transaction);
      await ignoreAbort(transaction);
      throw error;
    }
  }

  async cancelSnapshotFrameImport(
    input: CancelSnapshotImportInput,
  ): Promise<InterruptedSnapshotImportAbortResult> {
    validateId(input.identity, "Snapshot identity");
    validateId(input.ownerId, "Snapshot import owner ID");
    const transaction = this.#transaction([...indexedDbStoreNames], "readwrite", {
      allowSnapshotImport: true,
    });
    try {
      const catalog = transaction.objectStore("catalog");
      const raw = await requestResult<unknown>(catalog.get(SNAPSHOT_FRAME_IMPORT_KEY));
      if (raw === undefined) {
        throw new SnapshotImportConflictError(input.identity, input.ownerId, "session is missing");
      }
      const marker = asSnapshotFrameImportMarker(raw);
      if (marker.identity !== input.identity || marker.ownerId !== input.ownerId) {
        throw new SnapshotImportConflictError(
          input.identity,
          marker.ownerId,
          "session is owned by another caller",
        );
      }
      if (marker.replayCompleted) {
        catalog.delete(SNAPSHOT_FRAME_IMPORT_KEY);
        await deleteSnapshotFrameRecords(transaction.objectStore(SNAPSHOT_HEADER_STORE), "import");
        await transactionDone(transaction);
        return { identity: marker.identity, removedBlockCount: 0, removedBytes: 0 };
      }
      for (const name of indexedDbStoreNames) transaction.objectStore(name).clear();
      transaction.objectStore("gc").add(emptyMaintenanceQuota(), MAINTENANCE_QUOTA_KEY);
      transaction.objectStore("statistics").add(emptyResourceLedger(), RESOURCE_LEDGER_KEY);
      transaction
        .objectStore("statistics")
        .add(emptyCatalogResourceLedger(), CATALOG_RESOURCE_LEDGER_KEY);
      transaction
        .objectStore("statistics")
        .add(emptyRecordResourceLedger(), RECORD_RESOURCE_LEDGER_KEY);
      await transactionDone(transaction);
      this.#tableIdsByName.clear();
      this.#uniqueKeyCache = undefined;
      this.#manifestCache = undefined;
      return {
        identity: marker.identity,
        removedBlockCount: marker.kindItemCounts[SNAPSHOT_FRAME_KINDS.indexOf("block")] ?? 0,
        removedBytes: marker.kindStoredBytes[SNAPSHOT_FRAME_KINDS.indexOf("block")] ?? 0,
      };
    } catch (error) {
      abortIfActive(transaction);
      await ignoreAbort(transaction);
      throw error;
    }
  }

  close(): void {
    if (this.#closed) return;
    this.#closed = true;
    // close() is a memory boundary too: callers often retain their service graph after tearing
    // down a database, so do not leave table names, a manifest, or unique-key membership hanging
    // from the closed adapter.
    this.#tableIdsByName.clear();
    this.#uniqueKeyCache = undefined;
    this.#manifestCache = undefined;
    this.#db.close();
  }

  /** Test-only counters that make the adapter's retained-state bounds regression-testable. */
  _residentStateForTests(): {
    tableNameCacheEntries: number;
    uniqueKeyCacheEntries: number;
    uniqueKeyCacheBytes: number;
    uniqueKeyCacheLimitBytes: number;
    manifestCacheBlockIds: number;
    snapshotPeakRetainedItems: number;
    snapshotPeakRetainedBytes: number;
  } {
    const uniqueKeyCache = this.#uniqueKeyCache;
    return {
      tableNameCacheEntries: this.#tableIdsByName.size,
      uniqueKeyCacheEntries: uniqueKeyCache?.present.size ?? 0,
      uniqueKeyCacheBytes:
        uniqueKeyCache === undefined
          ? 0
          : uniqueKeyCacheRetainedBytes(uniqueKeyCache.present, uniqueKeyCache.chunks),
      uniqueKeyCacheLimitBytes: this.#uniqueKeyCacheBytes,
      manifestCacheBlockIds: this.#manifestCache?.blockIds.size ?? 0,
      snapshotPeakRetainedItems: this.#snapshotPeakRetainedItems,
      snapshotPeakRetainedBytes: this.#snapshotPeakRetainedBytes,
    };
  }

  async #deleteFtsBaseBuildFully(
    tableId: string,
    columnId: string,
    expectedBuildId?: string,
    updatedAtCutoff?: number,
  ): Promise<boolean> {
    for (;;) {
      const transaction = this.#transaction("catalog", "readwrite", {
        allowSnapshotImport: true,
      });
      try {
        const catalog = transaction.objectStore("catalog");
        const key = ftsBaseBuildKey(tableId, columnId);
        const marker = asOptionalFtsBaseBuildMarker(
          await requestResult<unknown>(catalog.get(key)),
          key,
        );
        if (marker === undefined) {
          await transactionDone(transaction);
          return true;
        }
        if (
          (expectedBuildId !== undefined && marker.buildId !== expectedBuildId) ||
          (updatedAtCutoff !== undefined && Date.parse(marker.updatedAt) > updatedAtCutoff)
        ) {
          await transactionDone(transaction);
          return false;
        }
        const end = Math.min(
          marker.cleanupIndex + FTS_BASE_BUILD_CLEANUP_PAGE,
          marker.boundaries.length,
        );
        const prefix = ftsBaseChunkPrefix(tableId, columnId, marker.buildId);
        for (let ordinal = marker.cleanupIndex; ordinal < end; ordinal += 1) {
          catalog.delete(`${prefix}${String(ordinal).padStart(6, "0")}`);
        }
        if (end === marker.boundaries.length) {
          catalog.delete(key);
        } else {
          catalog.put({ ...marker, cleanupIndex: end }, key);
        }
        await transactionDone(transaction);
        if (end === marker.boundaries.length) return true;
      } catch (error) {
        abortIfActive(transaction);
        await ignoreAbort(transaction);
        throw error;
      }
    }
  }

  async #cleanupFtsRetirementFully(tableId: string, columnId: string): Promise<void> {
    for (;;) {
      const transaction = this.#transaction("catalog", "readwrite", {
        allowSnapshotImport: true,
      });
      try {
        const complete = await deleteFtsRetirementPage(
          transaction.objectStore("catalog"),
          `${tableId}/${columnId}`,
        );
        await transactionDone(transaction);
        if (complete) return;
      } catch (error) {
        abortIfActive(transaction);
        await ignoreAbort(transaction);
        throw error;
      }
    }
  }

  async #cleanupExpiredUniqueKeyBuildPage(expiresAtCutoff: string): Promise<void> {
    const probe = this.#transaction("catalog", "readonly", { allowSnapshotImport: true });
    const buildId = await firstExpiredUniqueKeyBuild(
      probe.objectStore("catalog").index(UNIQUE_KEY_BUILD_EXPIRY_INDEX),
      expiresAtCutoff,
    );
    await transactionDone(probe);
    if (buildId !== undefined) await this.#cleanupUniqueKeyBuildIfExpired(buildId, expiresAtCutoff);
  }

  async #cleanupUniqueKeyBuildIfExpired(buildId: string, expiresAtCutoff: string): Promise<void> {
    const transaction = this.#transaction("catalog", "readwrite", {
      allowSnapshotImport: true,
    });
    let cleanup = false;
    try {
      const catalog = transaction.objectStore("catalog");
      const key = uniqueKeyBuildKey(buildId);
      const value: unknown = await requestResult(catalog.get(key));
      if (value !== undefined) {
        const envelope = asUniqueKeyBuildEnvelope(value, key);
        cleanup =
          envelope.record.state === "active" &&
          (envelope.cleanup || envelope.record.expiresAt <= expiresAtCutoff);
        if (cleanup && !envelope.cleanup) {
          catalog.put(uniqueKeyBuildEnvelope(envelope.record, true), key);
        }
      }
      await transactionDone(transaction);
    } catch (error) {
      abortIfActive(transaction);
      await ignoreAbort(transaction);
      throw error;
    }
    if (cleanup) await this.#cleanupUniqueKeyBuildArtifacts(buildId);
  }

  async #cleanupUniqueKeyBuildArtifacts(buildId: string): Promise<void> {
    for (;;) {
      const transaction = this.#transaction("catalog", "readwrite", {
        allowSnapshotImport: true,
      });
      try {
        const catalog = transaction.objectStore("catalog");
        const key = uniqueKeyBuildKey(buildId);
        const value: unknown = await requestResult(catalog.get(key));
        if (value === undefined) {
          await transactionDone(transaction);
          return;
        }
        const envelope = asUniqueKeyBuildEnvelope(value, key);
        if (envelope.record.state !== "active" || !envelope.cleanup) {
          await transactionDone(transaction);
          return;
        }
        const complete = await deleteUniqueKeyBuildArtifactsPage(catalog, buildId);
        if (complete) catalog.delete(key);
        await transactionDone(transaction);
        if (complete) return;
      } catch (error) {
        abortIfActive(transaction);
        await ignoreAbort(transaction);
        throw error;
      }
    }
  }

  /** Fail closed before exposing a connection whose durable catalog accounting has drifted. */
  async #validateCatalogResourceLedger(): Promise<void> {
    const transaction = this.#transaction(
      ["catalog", "manifests", "segments", "statistics", "transactions"],
      "readonly",
      {
        allowSnapshotImport: true,
      },
    );
    const catalog = transaction.objectStore("catalog");
    const observedCatalog = await readCatalogResourceLedger(catalog);
    const persisted = asCatalogResourceLedger(
      await requestResult<unknown>(
        transaction.objectStore("statistics").get(CATALOG_RESOURCE_LEDGER_KEY),
      ),
    );
    if (
      persisted.recordCount !== observedCatalog.recordCount ||
      persisted.retainedBytes !== observedCatalog.retainedBytes ||
      persisted.checksum !== observedCatalog.checksum
    ) {
      throw corruption(
        `statistics/${CATALOG_RESOURCE_LEDGER_KEY}`,
        "catalog resource ledger disagrees with table records",
      );
    }
    const publishedTables: TableRecord[] = [];
    const triggerNameMarkers = new Map<string, { tableId: string; triggerId: string }>();
    const triggerIdMarkers = new Map<string, { tableId: string; triggerName: string }>();
    await visitObjectStoreSequentially(catalog, (value, key) => {
      if (typeof key !== "string") return;
      if (key.startsWith(TABLE_ID_PREFIX)) {
        publishedTables.push(asTableRecord(value, key));
      } else if (key.startsWith(TRIGGER_NAME_PREFIX)) {
        const name = key.slice(TRIGGER_NAME_PREFIX.length);
        storedCatalogName(name, key);
        const marker = asOptionalTriggerNameMarker(value, key);
        if (marker === undefined) throw corruption(key, "trigger name marker is missing");
        triggerNameMarkers.set(name, marker);
      } else if (key.startsWith(TRIGGER_ID_PREFIX)) {
        const id = key.slice(TRIGGER_ID_PREFIX.length);
        nonEmptyStoredString(id, key);
        const marker = asOptionalTriggerIdMarker(value, key);
        if (marker === undefined) throw corruption(key, "trigger ID marker is missing");
        triggerIdMarkers.set(id, marker);
      }
    });
    const pending = await pendingCatalogReservations(transaction.objectStore("transactions"));
    const combinedTables = [...publishedTables, ...pending.records];
    const combinedBytes = observedCatalog.retainedBytes + pending.retainedBytes;
    if (combinedTables.length > MAX_CATALOG_RECORDS) {
      throw corruption(TABLE_ID_PREFIX, "published and pending catalog count exceeds its limit");
    }
    if (combinedBytes > MAX_CATALOG_RETAINED_BYTES) {
      throw corruption(TABLE_ID_PREFIX, "published and pending catalog bytes exceed their limit");
    }
    const ids = new Set<string>();
    const names = new Set<string>();
    const triggerNames = new Set<string>();
    const triggerIds = new Set<string>();
    const publishedById = new Map(publishedTables.map((table) => [table.id, table] as const));
    for (const table of combinedTables) {
      if (ids.has(table.id)) throw corruption(TABLE_ID_PREFIX, `duplicate table id ${table.id}`);
      if (names.has(table.name)) {
        throw corruption(TABLE_NAME_PREFIX, `duplicate table name ${table.name}`);
      }
      ids.add(table.id);
      names.add(table.name);
      for (const trigger of table.triggers ?? []) {
        if (triggerNames.has(trigger.name)) {
          throw corruption(TRIGGER_NAME_PREFIX, `duplicate trigger name ${trigger.name}`);
        }
        if (triggerIds.has(trigger.id)) {
          throw corruption(TRIGGER_ID_PREFIX, `duplicate trigger ID ${trigger.id}`);
        }
        triggerNames.add(trigger.name);
        triggerIds.add(trigger.id);
        if (publishedById.has(table.id)) {
          const nameMarker = triggerNameMarkers.get(trigger.name);
          const idMarker = triggerIdMarkers.get(trigger.id);
          if (nameMarker?.tableId !== table.id || nameMarker.triggerId !== trigger.id) {
            throw corruption(
              `${TRIGGER_NAME_PREFIX}${trigger.name}`,
              "trigger name marker is missing or mismatched",
            );
          }
          if (idMarker?.tableId !== table.id || idMarker.triggerName !== trigger.name) {
            throw corruption(
              `${TRIGGER_ID_PREFIX}${trigger.id}`,
              "trigger ID marker is missing or mismatched",
            );
          }
        }
      }
      await assertTableForeignKeysInTransaction(catalog, table);
    }
    for (const [name, marker] of triggerNameMarkers) {
      const trigger = publishedById
        .get(marker.tableId)
        ?.triggers?.find((candidate) => candidate.id === marker.triggerId);
      if (trigger?.name !== name) {
        throw corruption(`${TRIGGER_NAME_PREFIX}${name}`, "trigger name marker is orphaned");
      }
    }
    for (const [id, marker] of triggerIdMarkers) {
      const trigger = publishedById
        .get(marker.tableId)
        ?.triggers?.find((candidate) => candidate.name === marker.triggerName);
      if (trigger?.id !== id) {
        throw corruption(`${TRIGGER_ID_PREFIX}${id}`, "trigger ID marker is orphaned");
      }
    }
    const recordLedger = asRecordResourceLedger(
      await requestResult<unknown>(
        transaction.objectStore("statistics").get(RECORD_RESOURCE_LEDGER_KEY),
      ),
    );
    const [manifestCount, segmentCount] = await Promise.all([
      requestResult<number>(transaction.objectStore("manifests").count()),
      requestResult<number>(transaction.objectStore("segments").count()),
    ]);
    if (
      recordLedger.manifestCount !== manifestCount ||
      recordLedger.segmentCount !== segmentCount
    ) {
      throw corruption(
        `statistics/${RECORD_RESOURCE_LEDGER_KEY}`,
        "record count ledger disagrees with manifest or segment records",
      );
    }
    await transactionDone(transaction);
  }

  /** Reclaims at most one bounded page from an expired postings build on open/build activity. */
  async #cleanupInterruptedFtsBaseBuildPage(
    expiresAtCutoff = dateIsoString(new Date()),
  ): Promise<void> {
    const cutoff = Date.parse(expiresAtCutoff);
    if (!Number.isFinite(cutoff)) throw new TypeError("Postings build expiry cutoff is invalid");
    const probe = this.#transaction("catalog", "readonly", { allowSnapshotImport: true });
    const candidate = await firstFtsBaseBuildByExpiry(
      probe.objectStore("catalog").index(CATALOG_FTS_BUILD_EXPIRY_INDEX),
    );
    await transactionDone(probe);
    if (candidate === undefined || Date.parse(candidate.marker.expiresAt) > cutoff) return;

    const transaction = this.#transaction("catalog", "readwrite", { allowSnapshotImport: true });
    try {
      const catalog = transaction.objectStore("catalog");
      const marker = asOptionalFtsBaseBuildMarker(
        await requestResult<unknown>(catalog.get(candidate.key)),
        candidate.key,
      );
      if (marker === undefined || Date.parse(marker.expiresAt) > cutoff) {
        await transactionDone(transaction);
        return;
      }
      const end = Math.min(
        marker.cleanupIndex + FTS_BASE_BUILD_CLEANUP_PAGE,
        marker.boundaries.length,
      );
      const identity = candidate.key.slice(FTS_BASE_BUILD_PREFIX.length);
      const prefix = ftsBaseChunkPrefixFromIdentity(identity, marker.buildId);
      for (let ordinal = marker.cleanupIndex; ordinal < end; ordinal += 1) {
        catalog.delete(`${prefix}${String(ordinal).padStart(6, "0")}`);
      }
      if (end === marker.boundaries.length) {
        catalog.delete(candidate.key);
      } else {
        catalog.put({ ...marker, cleanupIndex: end }, candidate.key);
      }
      await transactionDone(transaction);
    } catch (error) {
      abortIfActive(transaction);
      await ignoreAbort(transaction);
      throw error;
    }
  }

  /** Reclaims one bounded retirement page; publication never waits on old generation deletion. */
  async #cleanupFtsRetirementPage(): Promise<void> {
    const probe = this.#transaction("catalog", "readonly", { allowSnapshotImport: true });
    const candidate = await firstFtsRetirementByUpdatedAt(
      probe.objectStore("catalog").index(CATALOG_FTS_RETIREMENT_UPDATED_INDEX),
    );
    await transactionDone(probe);
    if (candidate === undefined) return;

    const transaction = this.#transaction("catalog", "readwrite", { allowSnapshotImport: true });
    try {
      const identity = candidate.key.slice(FTS_RETIREMENT_PREFIX.length);
      await deleteFtsRetirementPage(transaction.objectStore("catalog"), identity);
      await transactionDone(transaction);
    } catch (error) {
      abortIfActive(transaction);
      await ignoreAbort(transaction);
      throw error;
    }
  }

  #transaction(
    stores: string | string[],
    mode: IDBTransactionMode,
    options: { allowSnapshotImport?: boolean } = {},
  ): IDBTransaction {
    if (this.#closed) throw new Error("This IndexedDB store connection is closed");
    const transaction =
      mode === "readwrite"
        ? this.#db.transaction(stores, mode, { durability: this.#durability })
        : this.#db.transaction(stores, mode);
    const storeList = typeof stores === "string" ? [stores] : stores;
    if (mode === "readwrite" && !options.allowSnapshotImport && storeList.includes("catalog")) {
      // Queue this read before the caller can enqueue its writes. The import-preparation
      // transaction covers every object store, so it cannot publish the marker until any older
      // writer has finished; once the marker exists, aborting catalog writes prevents every
      // ordinary logical-state publication until the import either resumes or is discarded.
      const catalog = transaction.objectStore("catalog");
      const marker = catalog.get(SNAPSHOT_FRAME_IMPORT_KEY);
      marker.addEventListener(
        "success",
        () => {
          if (marker.result !== undefined) abortIfActive(transaction);
        },
        { once: true },
      );
    }
    return transaction;
  }
}

function assertIndexedDbSchemaMigrationRegistry(): void {
  const expectedCount = SCHEMA_VERSION - FIRST_STABLE_SCHEMA_VERSION;
  if (indexedDbSchemaMigrations.length !== expectedCount) {
    throw new Error(
      `IndexedDB schema version ${String(SCHEMA_VERSION)} requires exactly ${String(expectedCount)} ` +
        `ordered migration${expectedCount === 1 ? "" : "s"} from stable version ` +
        String(FIRST_STABLE_SCHEMA_VERSION),
    );
  }
  for (const [index, migration] of indexedDbSchemaMigrations.entries()) {
    const expectedTarget = FIRST_STABLE_SCHEMA_VERSION + index + 1;
    if (migration.targetVersion !== expectedTarget) {
      throw new Error(
        `IndexedDB schema migration ${String(index)} targets version ` +
          `${String(migration.targetVersion)}; expected ${String(expectedTarget)}`,
      );
    }
  }
}

function createCurrentIndexedDbSchema(database: IDBDatabase, upgrade: IDBTransaction): void {
  for (const storeName of storeNames) database.createObjectStore(storeName);
  database.createObjectStore(SNAPSHOT_HEADER_STORE);
  upgrade.objectStore("segments").createIndex(SEGMENT_TABLE_INDEX, "tableId");
  upgrade.objectStore("leases").createIndex(LEASE_EXPIRY_INDEX, ["expiresAt", "id"]);
  upgrade.objectStore("transactions").createIndex(TRANSACTION_STATUS_INDEX, "status");
  upgrade.objectStore("temp").createIndex(TEMP_OWNER_EXPIRY_INDEX, ["expiresAt", "ownerId"]);
  upgrade.objectStore("catalog").createIndex(CATALOG_FTS_BUILD_UPDATED_INDEX, "updatedAt");
  upgrade.objectStore("catalog").createIndex(CATALOG_FTS_BUILD_EXPIRY_INDEX, "ftsBuildExpiry");
  upgrade
    .objectStore("catalog")
    .createIndex(CATALOG_FTS_RETIREMENT_UPDATED_INDEX, "retirementUpdatedAt");
  upgrade.objectStore("catalog").createIndex(UNIQUE_KEY_BUILD_ACTIVE_INDEX, "activeBuildState");
  upgrade.objectStore("catalog").createIndex(UNIQUE_KEY_BUILD_EXPIRY_INDEX, "activeExpiry");
  upgrade.objectStore("catalog").createIndex(MANIFEST_BLOCK_ID_INDEX, "blockId", {
    unique: true,
  });
  upgrade.objectStore("gc").add(emptyMaintenanceQuota(), MAINTENANCE_QUOTA_KEY);
  upgrade.objectStore("statistics").add(emptyResourceLedger(), RESOURCE_LEDGER_KEY);
  upgrade.objectStore("statistics").add(emptyCatalogResourceLedger(), CATALOG_RESOURCE_LEDGER_KEY);
  upgrade.objectStore("statistics").add(emptyRecordResourceLedger(), RECORD_RESOURCE_LEDGER_KEY);
}

function applyIndexedDbSchemaMigrations(
  database: IDBDatabase,
  upgrade: IDBTransaction,
  oldVersion: number,
): void {
  for (const migration of indexedDbSchemaMigrations) {
    if (migration.targetVersion > oldVersion) migration.migrate(database, upgrade);
  }
}

interface IndexedDbIndexSchema {
  readonly name: string;
  readonly keyPath: string | readonly string[];
  readonly unique?: boolean;
}

const indexedDbIndexSchema: Readonly<Record<string, readonly IndexedDbIndexSchema[]>> = {
  segments: [{ name: SEGMENT_TABLE_INDEX, keyPath: "tableId" }],
  leases: [{ name: LEASE_EXPIRY_INDEX, keyPath: ["expiresAt", "id"] }],
  transactions: [{ name: TRANSACTION_STATUS_INDEX, keyPath: "status" }],
  temp: [{ name: TEMP_OWNER_EXPIRY_INDEX, keyPath: ["expiresAt", "ownerId"] }],
  catalog: [
    { name: CATALOG_FTS_BUILD_UPDATED_INDEX, keyPath: "updatedAt" },
    { name: CATALOG_FTS_BUILD_EXPIRY_INDEX, keyPath: "ftsBuildExpiry" },
    { name: CATALOG_FTS_RETIREMENT_UPDATED_INDEX, keyPath: "retirementUpdatedAt" },
    { name: UNIQUE_KEY_BUILD_ACTIVE_INDEX, keyPath: "activeBuildState" },
    { name: UNIQUE_KEY_BUILD_EXPIRY_INDEX, keyPath: "activeExpiry" },
    { name: MANIFEST_BLOCK_ID_INDEX, keyPath: "blockId", unique: true },
  ],
};

async function validateCurrentIndexedDbSchema(database: IDBDatabase): Promise<void> {
  const actualStoreNames = Array.from(database.objectStoreNames).sort();
  const expectedStoreNames = [...indexedDbStoreNames].sort();
  if (!stringArraysEqual(actualStoreNames, expectedStoreNames)) {
    throw corruption(
      "schema",
      `object stores are ${actualStoreNames.join(", ")}; expected ${expectedStoreNames.join(", ")}`,
    );
  }
  let transaction: IDBTransaction | undefined;
  try {
    transaction = database.transaction([...indexedDbStoreNames], "readonly");
    for (const storeName of indexedDbStoreNames) {
      const store = transaction.objectStore(storeName);
      if (store.keyPath !== null || store.autoIncrement) {
        throw corruption("schema", `${storeName} must use out-of-line, non-incrementing keys`);
      }
      const expectedIndexes = indexedDbIndexSchema[storeName] ?? [];
      const actualIndexNames = Array.from(store.indexNames).sort();
      const expectedIndexNames = expectedIndexes.map(({ name }) => name).sort();
      if (!stringArraysEqual(actualIndexNames, expectedIndexNames)) {
        throw corruption(
          "schema",
          `${storeName} indexes are ${actualIndexNames.join(", ")}; expected ` +
            expectedIndexNames.join(", "),
        );
      }
      for (const expected of expectedIndexes) {
        const index = store.index(expected.name);
        if (
          !keyPathsEqual(index.keyPath, expected.keyPath) ||
          index.unique !== (expected.unique ?? false) ||
          index.multiEntry
        ) {
          throw corruption("schema", `${storeName}/${expected.name} definition is incompatible`);
        }
      }
    }
    await transactionDone(transaction);
  } catch (error) {
    if (transaction !== undefined) abortIfActive(transaction);
    if (error instanceof StorageCorruptionError) throw error;
    throw corruption("schema", integrityErrorMessage(error));
  }
}

function stringArraysEqual(left: readonly string[], right: readonly string[]): boolean {
  return left.length === right.length && left.every((value, index) => value === right[index]);
}

function keyPathsEqual(actual: string | string[], expected: string | readonly string[]): boolean {
  if (typeof actual === "string" || typeof expected === "string") return actual === expected;
  return stringArraysEqual(actual, expected);
}

function isErrorNamed(error: unknown, name: string): boolean {
  return (
    (error instanceof Error && error.name === name) ||
    (typeof error === "object" && error !== null && Reflect.get(error, "name") === name)
  );
}

async function readIndexedDbVersion(factory: IDBFactory, name: string): Promise<number | null> {
  try {
    const databases = await factory.databases();
    const version = databases.find((database) => database.name === name)?.version;
    return version ?? null;
  } catch {
    // databases() may be unavailable or denied even when open() is usable. VersionError still
    // proves the direction; null represents an exact version that could not be observed safely.
    return null;
  }
}

function openDatabaseRequest(
  request: IDBOpenDBRequest,
  blockedError: (event: IDBVersionChangeEvent) => Error,
): Promise<IDBDatabase> {
  return new Promise((resolve, reject) => {
    let settled = false;
    request.addEventListener(
      "success",
      () => {
        if (settled) {
          request.result.close();
          return;
        }
        settled = true;
        resolve(request.result);
      },
      { once: true },
    );
    request.addEventListener(
      "error",
      () => {
        if (settled) return;
        settled = true;
        reject(request.error ?? new Error("IndexedDB open failed"));
      },
      { once: true },
    );
    request.addEventListener(
      "blocked",
      (event) => {
        if (settled) return;
        settled = true;
        reject(blockedError(event));
      },
      { once: true },
    );
  });
}

function requestResult<T>(request: IDBRequest<T>): Promise<T> {
  return new Promise((resolve, reject) => {
    request.addEventListener("success", () => resolve(request.result), { once: true });
    request.addEventListener(
      "error",
      () => reject(request.error ?? new Error("IndexedDB request failed")),
      {
        once: true,
      },
    );
  });
}

function readCursorPage<T>(
  store: IDBObjectStore,
  limit: number,
  decode: (value: unknown, key: IDBValidKey) => T,
  acceptKey: (key: IDBValidKey) => boolean,
  seekKey?: string | number,
): Promise<T[]> {
  return new Promise((resolve, reject) => {
    const records: T[] = [];
    const request = store.openCursor();
    let seekPending = seekKey !== undefined;
    request.onerror = () => reject(request.error ?? new Error("IndexedDB cursor failed"));
    request.onsuccess = () => {
      const cursor = request.result;
      if (cursor === null || records.length === limit) {
        resolve(records);
        return;
      }
      try {
        if (seekPending) {
          seekPending = false;
          if (cursor.key < (seekKey ?? cursor.key)) {
            cursor.continue(seekKey);
            return;
          }
        }
        if (acceptKey(cursor.key)) records.push(decode(cursor.value, cursor.key));
        if (records.length === limit) resolve(records);
        else cursor.continue();
      } catch (error) {
        reject(error instanceof Error ? error : new Error(String(error)));
      }
    };
  });
}

function encodeExpiryPageCursor(expiresAt: string, id: string): string {
  return JSON.stringify([expiresAt, id]);
}

function decodeExpiryPageCursor(
  cursor: string | null,
  label: string,
): readonly [string, string] | null {
  if (cursor === null) return null;
  let value: unknown;
  try {
    value = JSON.parse(cursor);
  } catch {
    throw new TypeError(`${label} cursor is invalid`);
  }
  if (
    !Array.isArray(value) ||
    value.length !== 2 ||
    typeof value[0] !== "string" ||
    typeof value[1] !== "string" ||
    value[1].length === 0 ||
    new Date(Date.parse(value[0])).toISOString() !== value[0]
  ) {
    throw new TypeError(`${label} cursor is invalid`);
  }
  return [value[0], value[1]];
}

function compareLeaseIndexKeys(
  left: readonly [string, string],
  right: readonly [string, string],
): number {
  return left[0] < right[0]
    ? -1
    : left[0] > right[0]
      ? 1
      : left[1] < right[1]
        ? -1
        : left[1] > right[1]
          ? 1
          : 0;
}

function asExpiryIndexKey(value: IDBValidKey, location: string): readonly [string, string] {
  if (
    !Array.isArray(value) ||
    value.length !== 2 ||
    typeof value[0] !== "string" ||
    typeof value[1] !== "string"
  ) {
    throw corruption(location, "index key is invalid");
  }
  return [value[0], value[1]];
}

function readExpiredLeasePage(
  index: IDBIndex,
  expiresAtCutoff: string,
  after: readonly [string, string] | null,
  limit: number,
): Promise<LeaseRecord[]> {
  return new Promise((resolve, reject) => {
    const records: LeaseRecord[] = [];
    const request = index.openCursor();
    let seekPending = after !== null;
    request.onerror = () => reject(request.error ?? new Error("IndexedDB lease cursor failed"));
    request.onsuccess = () => {
      const cursor = request.result;
      if (cursor === null || records.length === limit) {
        resolve(records);
        return;
      }
      try {
        const key = asExpiryIndexKey(cursor.key, "leases/byExpiry");
        if (seekPending && after !== null) {
          const comparison = compareLeaseIndexKeys(key, after);
          seekPending = false;
          if (comparison < 0) {
            cursor.continue([...after]);
            return;
          }
          if (comparison === 0) {
            cursor.continue();
            return;
          }
        }
        if (key[0] > expiresAtCutoff) {
          resolve(records);
          return;
        }
        const record = asLeaseRecord(cursor.value);
        if (
          record.expiresAt !== key[0] ||
          record.id !== key[1] ||
          cursor.primaryKey !== record.id
        ) {
          throw corruption(`leases/${record.id}`, "expiry index does not match its record");
        }
        records.push(record);
        if (records.length === limit) resolve(records);
        else cursor.continue();
      } catch (error) {
        reject(error instanceof Error ? error : new Error(String(error)));
      }
    };
  });
}

function readExpiredTempOwnerPage(
  index: IDBIndex,
  expiresAtCutoff: string,
  after: readonly [string, string] | null,
  limit: number,
): Promise<TempOwnerRecord[]> {
  return new Promise((resolve, reject) => {
    const records: TempOwnerRecord[] = [];
    const request = index.openCursor();
    let seekPending = after !== null;
    request.onerror = () => reject(request.error ?? new Error("IndexedDB temp cursor failed"));
    request.onsuccess = () => {
      const cursor = request.result;
      if (cursor === null || records.length === limit) {
        resolve(records);
        return;
      }
      try {
        const key = asExpiryIndexKey(cursor.key, "temp/byOwnerExpiry");
        if (seekPending && after !== null) {
          const comparison = compareLeaseIndexKeys(key, after);
          seekPending = false;
          if (comparison < 0) {
            cursor.continue([...after]);
            return;
          }
          if (comparison === 0) {
            cursor.continue();
            return;
          }
        }
        if (key[0] > expiresAtCutoff) {
          resolve(records);
          return;
        }
        const record = asTempOwnerRecord(cursor.value);
        if (
          record.expiresAt !== key[0] ||
          record.ownerId !== key[1] ||
          !Array.isArray(cursor.primaryKey) ||
          cursor.primaryKey[0] !== "owner" ||
          cursor.primaryKey[1] !== record.ownerId
        ) {
          throw corruption(
            `temp/owner/${record.ownerId}`,
            "expiry index does not match its record",
          );
        }
        records.push(record);
        if (records.length === limit) resolve(records);
        else cursor.continue();
      } catch (error) {
        reject(error instanceof Error ? error : new Error(String(error)));
      }
    };
  });
}

/**
 * Advances the catalog epoch inside an already-open readwrite transaction on the catalog
 * store. Every catalog mutation calls this in its own transaction, so the epoch read by
 * `getCatalogProbe` is an atomic, monotonic proof of catalog identity: an unchanged epoch
 * means no table record, manifest publish, or commit has landed since the epoch was read.
 */
async function bumpCatalogEpoch(catalog: IDBObjectStore): Promise<void> {
  const value: unknown = await requestResult(catalog.get(CATALOG_EPOCH_KEY));
  let next: number;
  try {
    next = incrementSafeInteger(asCatalogEpoch(value), "Catalog epoch");
  } catch (error) {
    abortIfActive(catalog.transaction);
    throw error;
  }
  catalog.put(next, CATALOG_EPOCH_KEY);
}

/** Advances the DDL-only guard used to reject artifacts prepared against an old schema. */
async function bumpSchemaEpoch(catalog: IDBObjectStore): Promise<void> {
  const value: unknown = await requestResult(catalog.get(SCHEMA_EPOCH_KEY));
  let next: number;
  try {
    next = incrementSafeInteger(asSchemaEpoch(value), "Schema epoch");
  } catch (error) {
    abortIfActive(catalog.transaction);
    throw error;
  }
  catalog.put(next, SCHEMA_EPOCH_KEY);
}

async function assertExpectedCatalogEpoch(
  catalog: IDBObjectStore,
  tableId: string,
  expectedRevision: number,
  expectedCatalogEpoch: number | undefined,
): Promise<void> {
  if (expectedCatalogEpoch === undefined) return;
  if (!Number.isSafeInteger(expectedCatalogEpoch) || expectedCatalogEpoch < 0) {
    throw new TypeError("Expected catalog epoch is invalid");
  }
  const actualCatalogEpoch = asCatalogEpoch(
    await requestResult<unknown>(catalog.get(CATALOG_EPOCH_KEY)),
  );
  if (actualCatalogEpoch === expectedCatalogEpoch) return;
  const tableValue: unknown = await requestResult(catalog.get(`${TABLE_ID_PREFIX}${tableId}`));
  const actualRevision =
    tableValue === undefined
      ? null
      : asTableRecord(tableValue, `${TABLE_ID_PREFIX}${tableId}`).revision;
  throw new TableRecordConflictError(tableId, expectedRevision, actualRevision);
}

function visitObjectStoreSequentially(
  store: IDBObjectStore,
  visit: (value: unknown, key: IDBValidKey) => unknown,
  direction: IDBCursorDirection = "next",
): Promise<boolean> {
  return new Promise((resolve, reject) => {
    const request = store.openCursor(null, direction);
    request.onerror = () => reject(request.error ?? new Error("IndexedDB cursor failed"));
    request.onsuccess = () => {
      const cursor = request.result;
      if (cursor === null) {
        resolve(false);
        return;
      }
      let result: unknown;
      try {
        result = visit(cursor.value, cursor.key);
      } catch (error) {
        reject(error instanceof Error ? error : new Error(String(error)));
        return;
      }
      if (typeof (result as { then?: unknown } | null)?.then !== "function") {
        if (result === true) resolve(true);
        else cursor.continue();
        return;
      }
      Promise.resolve(result).then(
        (stop) => {
          if (stop === true) resolve(true);
          else cursor.continue();
        },
        (error: unknown) => reject(error instanceof Error ? error : new Error(String(error))),
      );
    };
  });
}

/** Visits one exact secondary-index partition and reports the primary record key. */
function visitIndexPartitionSequentially(
  index: IDBIndex,
  key: IDBValidKey,
  visit: (value: unknown, primaryKey: IDBValidKey) => unknown,
): Promise<boolean> {
  return new Promise((resolve, reject) => {
    const request = index.openCursor(key);
    request.onerror = () => reject(request.error ?? new Error("IndexedDB cursor failed"));
    request.onsuccess = () => {
      const cursor = request.result;
      if (cursor === null) {
        resolve(false);
        return;
      }
      let result: unknown;
      try {
        result = visit(cursor.value, cursor.primaryKey);
      } catch (error) {
        reject(error instanceof Error ? error : new Error(String(error)));
        return;
      }
      if (typeof (result as { then?: unknown } | null)?.then !== "function") {
        if (result === true) resolve(true);
        else cursor.continue();
        return;
      }
      Promise.resolve(result).then((stop) => {
        if (stop === true) resolve(true);
        else cursor.continue();
      }, reject);
    };
  });
}

function readTableSegmentsForRemoval(index: IDBIndex, tableId: string): Promise<SegmentRecord[]> {
  return new Promise((resolve, reject) => {
    const records: SegmentRecord[] = [];
    const request = index.openCursor(tableId);
    request.onerror = () => reject(request.error ?? new Error("IndexedDB cursor failed"));
    request.onsuccess = () => {
      const cursor = request.result;
      if (cursor === null) {
        records.sort((left, right) => left.id.localeCompare(right.id));
        resolve(records);
        return;
      }
      try {
        const record = asSegmentRecord(cursor.value);
        if (
          typeof cursor.primaryKey !== "string" ||
          record.id !== cursor.primaryKey ||
          record.tableId !== tableId
        ) {
          throw corruption(
            `segments/${storageKeyLocation(cursor.primaryKey)}`,
            "index entry is inconsistent",
          );
        }
        records.push(record);
        cursor.continue();
      } catch (error) {
        reject(error instanceof Error ? error : new Error(String(error)));
      }
    };
  });
}

async function assertTableRemovalAllowed(
  transaction: IDBTransaction,
  tableId: string,
  expectedRevision: number,
): Promise<{ record: TableRecord; segments: SegmentRecord[] }> {
  const catalog = transaction.objectStore("catalog");
  const tableKey = `${TABLE_ID_PREFIX}${tableId}`;
  const value: unknown = await requestResult(catalog.get(tableKey));
  const record = value === undefined ? undefined : asTableRecord(value, tableKey);
  const actualRevision = record?.revision ?? null;
  if (record === undefined || actualRevision !== expectedRevision) {
    throw new TableRecordConflictError(tableId, expectedRevision, actualRevision);
  }
  await visitObjectStoreSequentially(catalog, (candidateValue, key) => {
    if (typeof key !== "string" || !key.startsWith(TABLE_ID_PREFIX) || key === tableKey) return;
    const candidate = asTableRecord(candidateValue, key);
    const foreignKey = (candidate.foreignKeys ?? []).find(
      (constraint) => constraint.parentTable === record.name,
    );
    if (foreignKey !== undefined) {
      throw new TableInUseError(tableId, "foreign key", `${candidate.id}/${foreignKey.name}`);
    }
  });
  const segments = await readTableSegmentsForRemoval(
    transaction.objectStore("segments").index(SEGMENT_TABLE_INDEX),
    tableId,
  );
  const transactionStore = transaction.objectStore("transactions");
  await visitObjectStoreSequentially(transactionStore, (candidateValue, key) => {
    const candidate = asTransactionRecord(candidateValue, storageKeyLocation(key));
    const pendingTable = candidate.status === "active" ? candidate.pendingTable : undefined;
    const foreignKey = (pendingTable?.foreignKeys ?? []).find(
      (constraint) => constraint.parentTable === record.name,
    );
    if (foreignKey !== undefined) {
      throw new TableInUseError(
        tableId,
        "pending foreign key",
        `${candidate.id}/${foreignKey.name}`,
      );
    }
  });
  const owners = new Map<string, TransactionRecord>();
  const activeTransactionIds = new Set<string>();
  for (const segment of segments) {
    let owner = owners.get(segment.transactionId);
    if (owner === undefined) {
      const ownerValue: unknown = await requestResult(transactionStore.get(segment.transactionId));
      if (ownerValue === undefined) {
        throw corruption(
          `segments/${segment.id}`,
          `references missing transaction ${segment.transactionId}`,
        );
      }
      owner = asTransactionRecord(ownerValue, segment.transactionId);
      owners.set(segment.transactionId, owner);
    }
    if (owner.status === "active") activeTransactionIds.add(owner.id);
  }
  const activeTransactionId = [...activeTransactionIds].sort()[0];
  if (activeTransactionId !== undefined) {
    throw new TableInUseError(tableId, "transaction", activeTransactionId);
  }
  const activeCompactionJobIds: string[] = [];
  await visitObjectStoreSequentially(transaction.objectStore("gc"), (jobValue, key) => {
    const job = asCompactionJobAtMaintenanceKey(jobValue, key);
    if (job?.tableId === tableId && !isTerminalCompactionJob(job)) {
      activeCompactionJobIds.push(job.id);
    }
  });
  const activeCompactionJobId = activeCompactionJobIds.sort()[0];
  if (activeCompactionJobId !== undefined) {
    throw new TableInUseError(tableId, "compaction job", activeCompactionJobId);
  }
  return { record, segments };
}

async function removeTableMetadataInTransaction(
  transaction: IDBTransaction,
  catalog: IDBObjectStore,
  removal: { record: TableRecord; segments: readonly SegmentRecord[] },
): Promise<void> {
  const { record } = removal;
  catalog.delete(`${TABLE_ID_PREFIX}${record.id}`);
  catalog.delete(`${TABLE_NAME_PREFIX}${record.name}`);
  catalog.delete(`${ROW_ID_PREFIX}${record.id}`);
  for (const [indexId, index] of Object.entries(record.secondaryIndexes ?? {})) {
    const markerKey = `${SECONDARY_INDEX_NAME_PREFIX}${index.name}`;
    const marker = asOptionalSecondaryIndexNameMarker(
      await requestResult<unknown>(catalog.get(markerKey)),
      markerKey,
    );
    if (marker?.tableId === record.id && marker.indexId === indexId) catalog.delete(markerKey);
    if (index.unique === true) {
      await replaceUniqueKeyMembership(
        catalog,
        secondaryUniqueKeyNamespace(record.id, indexId),
        [],
        false,
      );
    }
  }
  for (const trigger of record.triggers ?? []) {
    const nameKey = `${TRIGGER_NAME_PREFIX}${trigger.name}`;
    const idKey = `${TRIGGER_ID_PREFIX}${trigger.id}`;
    const [nameMarker, idMarker] = await Promise.all([
      requestResult<unknown>(catalog.get(nameKey)).then((value) =>
        asOptionalTriggerNameMarker(value, nameKey),
      ),
      requestResult<unknown>(catalog.get(idKey)).then((value) =>
        asOptionalTriggerIdMarker(value, idKey),
      ),
    ]);
    if (nameMarker?.tableId === record.id && nameMarker.triggerId === trigger.id) {
      catalog.delete(nameKey);
    }
    if (idMarker?.tableId === record.id && idMarker.triggerName === trigger.name) {
      catalog.delete(idKey);
    }
  }
  const ownedUniqueBuildIds: string[] = [];
  await visitObjectStoreSequentially(catalog, (value, key) => {
    if (typeof key !== "string" || !key.startsWith(UNIQUE_KEY_BUILD_PREFIX)) return;
    const envelope = asUniqueKeyBuildEnvelope(value, key);
    if (envelope.record.tableId === record.id) ownedUniqueBuildIds.push(envelope.record.buildId);
  });
  for (const buildId of ownedUniqueBuildIds) {
    await deleteUniqueKeyBuildArtifactsInTransaction(catalog, buildId);
    catalog.delete(uniqueKeyBuildKey(buildId));
  }
  const ownedStringPrefixes = [
    `${AUTO_INCREMENT_PREFIX}${record.id}/`,
    `${FTS_BASE_INDEX_PREFIX}${record.id}/`,
    `${FTS_BASE_PREFIX}${record.id}/`,
    `${FTS_BASE_BUILD_PREFIX}${record.id}/`,
    `${FTS_CHUNK_PREFIX}${record.id}/`,
  ];
  const ownedArrayKinds = new Set([UNIQUE_KEY_CHUNK_INDEX, UNIQUE_KEY_CHUNK, UNIQUE_KEY_BASE_PART]);
  await visitObjectStoreSequentially(catalog, (_value, key) => {
    if (typeof key === "string") {
      if (ownedStringPrefixes.some((prefix) => key.startsWith(prefix))) catalog.delete(key);
      return;
    }
    if (!Array.isArray(key)) return;
    const [kind, owner] = key as unknown[];
    if (typeof kind === "string" && ownedArrayKinds.has(kind) && owner === record.id) {
      catalog.delete(key);
    }
  });
  const segmentStore = transaction.objectStore("segments");
  for (const segment of removal.segments) segmentStore.delete(segment.id);
}

function deleteUniqueKeyBuildArtifactsInTransaction(
  catalog: IDBObjectStore,
  buildId: string,
): Promise<void> {
  return new Promise((resolve, reject) => {
    let phase: "base" | "chunks" = "base";
    const request = catalog.openCursor();
    request.onerror = () => reject(request.error ?? new Error("IndexedDB cursor failed"));
    request.onsuccess = () => {
      const cursor = request.result;
      if (cursor === null) {
        resolve();
        return;
      }
      const kind = phase === "base" ? UNIQUE_KEY_BASE_PART : UNIQUE_KEY_BUILD_CHUNK;
      const comparison = compareStructuredPrefix(cursor.key, kind, buildId);
      if (comparison < 0) {
        cursor.continue([kind, buildId]);
        return;
      }
      if (comparison > 0) {
        if (phase === "base") {
          phase = "chunks";
          const chunkComparison = compareStructuredPrefix(
            cursor.key,
            UNIQUE_KEY_BUILD_CHUNK,
            buildId,
          );
          if (chunkComparison < 0) {
            cursor.continue([UNIQUE_KEY_BUILD_CHUNK, buildId]);
            return;
          }
          if (chunkComparison > 0) {
            resolve();
            return;
          }
        } else {
          resolve();
          return;
        }
      }
      cursor.delete();
      cursor.continue();
    };
  });
}

/**
 * Rejects if a transaction aborts or fails, and never resolves otherwise. Lets a caller that
 * does not wait for `complete` still surface the failure it would otherwise have missed.
 */
function transactionFailure(transaction: IDBTransaction): Promise<never> {
  return new Promise((_resolve, reject) => {
    transaction.addEventListener(
      "abort",
      () => reject(transaction.error ?? new Error("Transaction aborted")),
      { once: true },
    );
    transaction.addEventListener(
      "error",
      () => reject(transaction.error ?? new Error("Transaction failed")),
      { once: true },
    );
  });
}

function transactionDone(transaction: IDBTransaction): Promise<void> {
  return new Promise((resolve, reject) => {
    transaction.addEventListener("complete", () => resolve(), { once: true });
    transaction.addEventListener(
      "abort",
      () => reject(transaction.error ?? new Error("Transaction aborted")),
      {
        once: true,
      },
    );
    transaction.addEventListener(
      "error",
      () => reject(transaction.error ?? new Error("Transaction failed")),
      {
        once: true,
      },
    );
  });
}

async function ignoreAbort(transaction: IDBTransaction): Promise<void> {
  try {
    await transactionDone(transaction);
  } catch {
    // The caller reports the more useful domain error.
  }
}

function corruption(location: string, message: string): StorageCorruptionError {
  return new StorageCorruptionError("indexeddb", location, message);
}

function assertKnownFields(value: object, allowed: readonly string[], location: string): void {
  const known = new Set(allowed);
  const unknown = Object.keys(value).find((key) => !known.has(key));
  if (unknown !== undefined) throw corruption(location, `field is unknown: ${unknown}`);
}

function hasOnlyKnownFields(value: Record<string, unknown>, allowed: readonly string[]): boolean {
  const known = new Set(allowed);
  return Object.keys(value).every((key) => known.has(key));
}

function integrityErrorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function validStoredTimestamp(value: unknown, location: string): string {
  if (typeof value !== "string" || !Number.isFinite(Date.parse(value))) {
    throw corruption(location, "timestamp is invalid");
  }
  return value;
}

function canonicalStoredTimestamp(value: unknown, location: string): string {
  const timestamp = validStoredTimestamp(value, location);
  if (new Date(Date.parse(timestamp)).toISOString() !== timestamp) {
    throw corruption(location, "timestamp is not canonical UTC ISO-8601");
  }
  return timestamp;
}

function asOptionalManifestVersion(value: unknown, location: string): number | undefined {
  if (value === undefined) return undefined;
  if (!Number.isSafeInteger(value) || (value as number) < 0) {
    throw corruption(location, "manifest pointer is invalid");
  }
  return value as number;
}

function asCatalogEpoch(value: unknown): number {
  if (value === undefined) return 0;
  if (!Number.isSafeInteger(value) || (value as number) < 0) {
    throw corruption(CATALOG_EPOCH_KEY, "catalog epoch is invalid");
  }
  return value as number;
}

function asSchemaEpoch(value: unknown): number {
  if (value === undefined) return 0;
  if (!Number.isSafeInteger(value) || (value as number) < 0) {
    throw corruption(SCHEMA_EPOCH_KEY, "schema epoch is invalid");
  }
  return value as number;
}

function asOptionalCounter(value: unknown, location: string): bigint | undefined {
  if (value === undefined) return undefined;
  const maximum = location.startsWith(AUTO_INCREMENT_PREFIX)
    ? MAX_AUTO_INCREMENT_EXCLUSIVE_END
    : MAX_ROW_ID_EXCLUSIVE_END;
  if (typeof value !== "bigint" || value < 1n || value > maximum) {
    throw corruption(location, "counter is outside its numeric range");
  }
  return value;
}

function storageKeyLocation(key: IDBValidKey): string {
  if (typeof key === "string" || typeof key === "number") return String(key);
  if (key instanceof Date) return key.toISOString();
  if (Array.isArray(key)) return `[${key.map(storageKeyLocation).join(",")}]`;
  if (key instanceof ArrayBuffer) return `binary:${String(key.byteLength)}`;
  return `binary:${String(key.byteLength)}`;
}

function asOptionalSecondaryIndexNameMarker(
  value: unknown,
  location: string,
): { tableId: string; indexId: string } | undefined {
  if (value === undefined) return undefined;
  if (!isRecord(value)) throw corruption(location, "secondary-index name marker is invalid");
  assertKnownFields(value, ["tableId", "indexId"], location);
  return {
    tableId: nonEmptyStoredString(value.tableId, `${location}/tableId`),
    indexId: nonEmptyStoredString(value.indexId, `${location}/indexId`),
  };
}

function asOptionalTriggerNameMarker(
  value: unknown,
  location: string,
): { tableId: string; triggerId: string } | undefined {
  if (value === undefined) return undefined;
  if (!isRecord(value)) throw corruption(location, "trigger name marker is invalid");
  assertKnownFields(value, ["tableId", "triggerId"], location);
  return {
    tableId: nonEmptyStoredString(value.tableId, `${location}/tableId`),
    triggerId: nonEmptyStoredString(value.triggerId, `${location}/triggerId`),
  };
}

function asOptionalTriggerIdMarker(
  value: unknown,
  location: string,
): { tableId: string; triggerName: string } | undefined {
  if (value === undefined) return undefined;
  if (!isRecord(value)) throw corruption(location, "trigger ID marker is invalid");
  assertKnownFields(value, ["tableId", "triggerName"], location);
  return {
    tableId: nonEmptyStoredString(value.tableId, `${location}/tableId`),
    triggerName: storedCatalogName(value.triggerName, `${location}/triggerName`),
  };
}

function nonEmptyStoredString(value: unknown, location: string): string {
  if (!isStorageId(value)) {
    throw corruption(
      location,
      `value must contain 1-${String(MAX_STORAGE_ID_CHARACTERS)} characters`,
    );
  }
  return value;
}

function storedCatalogName(value: unknown, location: string): string {
  try {
    return validateCatalogName(value, location);
  } catch {
    throw corruption(location, "catalog name is invalid");
  }
}

function nonNegativeStoredInteger(value: unknown, location: string): number {
  if (!Number.isSafeInteger(value) || (value as number) < 0) {
    throw corruption(location, "value must be a non-negative whole number");
  }
  return value as number;
}

function asNullableStoredVersion(value: unknown, location: string): number | null {
  if (value === null) return null;
  return nonNegativeStoredInteger(value, location);
}

function requiredUniqueStringArray(value: unknown, location: string): string[] {
  if (!Array.isArray(value) || !value.every(isStorageId) || new Set(value).size !== value.length) {
    throw corruption(location, "identifier list is invalid");
  }
  return [...value];
}

function validateArtifactPartition(
  retained: readonly string[],
  removed: readonly string[],
  current: readonly string[],
): boolean {
  if (retained.length + removed.length !== current.length) return false;
  return (
    retained.every((id, index) => id.length > 0 && current[index] === id) &&
    removed.every((id, index) => id.length > 0 && current[retained.length + index] === id)
  );
}

function asBytes(value: unknown, location = "blocks"): Uint8Array {
  // Every IndexedDB get deserializes a fresh, unshared value, so the bytes return without a
  // defensive copy; wrapping an ArrayBuffer in a view is also zero-copy.
  if (value instanceof Uint8Array) {
    if (isSharedBytes(value)) throw corruption(location, "stored payload uses shared memory");
    return value;
  }
  if (value instanceof ArrayBuffer) return new Uint8Array(value);
  throw corruption(location, "stored payload is not binary data");
}

function compactStructuredCloneBytes(bytes: Uint8Array, label: string): Uint8Array {
  assertUnsharedBytes(bytes, label);
  return bytes.byteOffset === 0 && bytes.byteLength === bytes.buffer.byteLength
    ? bytes
    : bytes.slice();
}

function assertUnsharedBytes(value: unknown, label: string): asserts value is Uint8Array {
  if (!(value instanceof Uint8Array) || isSharedBytes(value)) {
    throw new TypeError(`${label} must be a Uint8Array backed by unshared memory`);
  }
}

function isSharedBytes(bytes: Uint8Array): boolean {
  return typeof SharedArrayBuffer !== "undefined" && bytes.buffer instanceof SharedArrayBuffer;
}

function asStoredManifestRecord(value: unknown, expectedVersion?: number): StoredManifestRecord {
  if (!isRecord(value)) throw corruption("manifests", "record is not an object");
  assertKnownFields(
    value,
    [
      "version",
      "previousVersion",
      "createdAt",
      "liveBlockCount",
      "liveBlockBytes",
      "changedTableIds",
      "prunedAt",
    ],
    "manifests",
  );
  const version = value.version;
  const previousVersion = value.previousVersion;
  if (!Number.isSafeInteger(version) || (version as number) < 0) {
    throw corruption("manifests", "version is invalid");
  }
  if (expectedVersion !== undefined && version !== expectedVersion) {
    throw corruption(
      `manifests/${String(expectedVersion)}`,
      `record declares version ${String(version)}`,
    );
  }
  const manifestVersion = version as number;
  if (
    previousVersion !== null &&
    (!Number.isSafeInteger(previousVersion) || (previousVersion as number) < 0)
  ) {
    throw corruption(`manifests/${String(version)}`, "previous version is invalid");
  }
  const manifestPreviousVersion = previousVersion as number | null;
  if (manifestPreviousVersion !== null && manifestPreviousVersion !== manifestVersion - 1) {
    throw corruption(
      `manifests/${String(version)}`,
      "previous version is not the immediate predecessor",
    );
  }
  const createdAt = validStoredTimestamp(value.createdAt, `manifests/${String(version)}/createdAt`);
  const prunedAt =
    value.prunedAt === undefined
      ? undefined
      : validStoredTimestamp(value.prunedAt, `manifests/${String(version)}/prunedAt`);
  if (!Array.isArray(value.changedTableIds)) {
    throw corruption(`manifests/${String(version)}`, "changed table IDs are missing");
  }
  let changedTableIds: string[];
  try {
    changedTableIds = validateCanonicalManifestChangedTableIds(value.changedTableIds);
  } catch (error) {
    throw corruption(
      `manifests/${String(version)}/changedTableIds`,
      error instanceof Error ? error.message : "changed table IDs are invalid",
    );
  }
  if (
    !Number.isSafeInteger(value.liveBlockCount) ||
    (value.liveBlockCount as number) < 0 ||
    !Number.isSafeInteger(value.liveBlockBytes) ||
    (value.liveBlockBytes as number) < 0
  ) {
    throw corruption(`manifests/${String(version)}`, "live block summary is invalid");
  }
  return {
    version: manifestVersion,
    previousVersion: manifestPreviousVersion,
    createdAt,
    liveBlockCount: value.liveBlockCount as number,
    liveBlockBytes: value.liveBlockBytes as number,
    changedTableIds,
    ...(prunedAt === undefined ? {} : { prunedAt }),
  };
}

function manifestBlockKey(blockId: string): IDBValidKey {
  return [MANIFEST_BLOCK, blockId];
}

function asManifestBlockRecord(value: unknown, expectedBlockId?: string): ManifestBlockRecord {
  const location =
    expectedBlockId === undefined ? MANIFEST_BLOCK : `${MANIFEST_BLOCK}/${expectedBlockId}`;
  if (!isRecord(value)) throw corruption(location, "record is not an object");
  assertKnownFields(
    value,
    ["blockId", "byteLength", "checksum", "addedVersion", "removedVersion"],
    location,
  );
  if (
    !isStorageId(value.blockId) ||
    (expectedBlockId !== undefined && value.blockId !== expectedBlockId) ||
    !isBoundedCursor(value.byteLength, MAX_BLOCK_READ_BATCH_BYTES) ||
    value.byteLength === 0 ||
    !isUint32(value.checksum) ||
    !isBoundedCursor(value.addedVersion, Number.MAX_SAFE_INTEGER) ||
    (value.removedVersion !== null &&
      (!isBoundedCursor(value.removedVersion, Number.MAX_SAFE_INTEGER) ||
        value.removedVersion <= value.addedVersion))
  ) {
    throw corruption(location, "manifest block provenance is invalid");
  }
  return {
    blockId: value.blockId,
    byteLength: value.byteLength,
    checksum: value.checksum,
    addedVersion: value.addedVersion,
    removedVersion: value.removedVersion,
  };
}

function manifestBlockVisibleAt(record: ManifestBlockRecord, version: number): boolean {
  return (
    record.addedVersion <= version &&
    (record.removedVersion === null || version < record.removedVersion)
  );
}

function firstOverlappingManifestVersion(
  record: ManifestBlockRecord,
  sortedVersions: readonly number[],
): number | undefined {
  let low = 0;
  let high = sortedVersions.length;
  while (low < high) {
    const middle = (low + high) >>> 1;
    if ((sortedVersions[middle] ?? Number.MAX_SAFE_INTEGER) < record.addedVersion) low = middle + 1;
    else high = middle;
  }
  const version = sortedVersions[low];
  return version !== undefined && manifestBlockVisibleAt(record, version) ? version : undefined;
}

function manifestBlockRecordOverlapsVersions(
  record: ManifestBlockRecord,
  sortedVersions: readonly number[],
): boolean {
  return firstOverlappingManifestVersion(record, sortedVersions) !== undefined;
}

type ManifestPruneCleanupState =
  | { phase: "scan"; afterVersion: number | null }
  | { phase: "delete"; safeBelow: number; beforeVersion: number };

function asManifestPruneCleanupState(value: unknown): ManifestPruneCleanupState {
  if (value === undefined) return { phase: "scan", afterVersion: null };
  if (!isRecord(value)) {
    throw corruption(MANIFEST_PRUNE_CLEANUP_KEY, "record is not an object");
  }
  if (value.phase === "scan") {
    assertKnownFields(value, ["phase", "afterVersion"], MANIFEST_PRUNE_CLEANUP_KEY);
    if (
      value.afterVersion !== null &&
      (!Number.isSafeInteger(value.afterVersion) || (value.afterVersion as number) < 0)
    ) {
      throw corruption(MANIFEST_PRUNE_CLEANUP_KEY, "scan cursor is invalid");
    }
    return { phase: "scan", afterVersion: value.afterVersion as number | null };
  }
  if (value.phase === "delete") {
    assertKnownFields(value, ["phase", "safeBelow", "beforeVersion"], MANIFEST_PRUNE_CLEANUP_KEY);
    if (
      !Number.isSafeInteger(value.safeBelow) ||
      (value.safeBelow as number) < 0 ||
      !Number.isSafeInteger(value.beforeVersion) ||
      (value.beforeVersion as number) < 0 ||
      (value.beforeVersion as number) > (value.safeBelow as number)
    ) {
      throw corruption(MANIFEST_PRUNE_CLEANUP_KEY, "delete cursor is invalid");
    }
    return {
      phase: "delete",
      safeBelow: value.safeBelow as number,
      beforeVersion: value.beforeVersion as number,
    };
  }
  throw corruption(MANIFEST_PRUNE_CLEANUP_KEY, "phase is invalid");
}

function scanManifestPruneBoundary(
  store: IDBObjectStore,
  afterVersion: number | null,
  maxItems: number,
): Promise<{
  visited: number;
  afterVersion: number | null;
  safeBelow?: number;
  reachedEnd: boolean;
}> {
  return new Promise((resolve, reject) => {
    let visited = 0;
    let latest = afterVersion;
    let seekPending = afterVersion !== null;
    const request = store.openCursor();
    request.onerror = () => reject(request.error ?? new Error("IndexedDB manifest cursor failed"));
    request.onsuccess = () => {
      const cursor = request.result;
      if (cursor === null) {
        resolve({ visited, afterVersion: latest, reachedEnd: true });
        return;
      }
      try {
        if (typeof cursor.key !== "number" || !Number.isSafeInteger(cursor.key) || cursor.key < 0) {
          throw corruption("manifests", "record key is invalid");
        }
        if (seekPending && afterVersion !== null) {
          seekPending = false;
          if (cursor.key < afterVersion) {
            cursor.continue(afterVersion);
            return;
          }
          if (cursor.key === afterVersion) {
            cursor.continue();
            return;
          }
        }
        const record = asStoredManifestRecord(cursor.value, cursor.key);
        visited += 1;
        latest = record.version;
        if (record.prunedAt === undefined) {
          resolve({
            visited,
            afterVersion: latest,
            safeBelow: record.version,
            reachedEnd: false,
          });
          return;
        }
        if (visited === maxItems) {
          resolve({ visited, afterVersion: latest, reachedEnd: false });
          return;
        }
        cursor.continue();
      } catch (error) {
        reject(error instanceof Error ? error : new Error(String(error)));
      }
    };
  });
}

function deletePrunedManifestPage(
  store: IDBObjectStore,
  safeBelow: number,
  beforeVersion: number,
  maxItems: number,
): Promise<{
  visited: number;
  removed: number;
  removedRecords: StoredManifestRecord[];
  beforeVersion: number;
  reachedEnd: boolean;
}> {
  return new Promise((resolve, reject) => {
    let visited = 0;
    let removed = 0;
    const removedRecords: StoredManifestRecord[] = [];
    let before = beforeVersion;
    let seekPending = true;
    const request = store.openCursor(null, "prev");
    request.onerror = () => reject(request.error ?? new Error("IndexedDB manifest cursor failed"));
    request.onsuccess = () => {
      const cursor = request.result;
      if (cursor === null) {
        resolve({ visited, removed, removedRecords, beforeVersion: before, reachedEnd: true });
        return;
      }
      try {
        if (typeof cursor.key !== "number" || !Number.isSafeInteger(cursor.key) || cursor.key < 0) {
          throw corruption("manifests", "record key is invalid");
        }
        if (seekPending) {
          seekPending = false;
          if (cursor.key >= beforeVersion) {
            if (beforeVersion === 0) {
              resolve({
                visited,
                removed,
                removedRecords,
                beforeVersion: 0,
                reachedEnd: true,
              });
            } else {
              cursor.continue(beforeVersion - 1);
            }
            return;
          }
        }
        const record = asStoredManifestRecord(cursor.value, cursor.key);
        if (record.version >= safeBelow || record.prunedAt === undefined) {
          throw corruption(
            `manifests/${String(record.version)}`,
            "prune cleanup crossed its safe tombstone prefix",
          );
        }
        visited += 1;
        before = record.version;
        cursor.delete();
        removed += 1;
        removedRecords.push(record);
        if (visited === maxItems) {
          resolve({ visited, removed, removedRecords, beforeVersion: before, reachedEnd: false });
        } else {
          cursor.continue();
        }
      } catch (error) {
        reject(error instanceof Error ? error : new Error(String(error)));
      }
    };
  });
}

function emptySnapshotKindSummaries(): Record<
  SnapshotFrameKind,
  { frameCount: number; itemCount: number; storedBytes: number }
> {
  return Object.fromEntries(
    SNAPSHOT_FRAME_KINDS.map((kind) => [kind, { frameCount: 0, itemCount: 0, storedBytes: 0 }]),
  ) as Record<SnapshotFrameKind, { frameCount: number; itemCount: number; storedBytes: number }>;
}

interface IndexedDbSnapshotFtsIndex {
  columnId: string;
  coversVersion: number;
  totalTokens: number;
  chunkCount: number;
  generationId: string;
}

interface IndexedDbSnapshotUniqueMembership {
  namespaceId: string;
  indexId: string | null;
  generationId: string;
  chunkCount: number;
  tokenCount: number;
}

interface IndexedDbSnapshotTable {
  record: TableRecord;
  nextRowId: bigint;
  autoIncrement: Array<{ columnId: string; next: bigint }>;
  uniqueMemberships: IndexedDbSnapshotUniqueMembership[];
  fts: IndexedDbSnapshotFtsIndex[];
}

function snapshotFrameKey(
  direction: "export" | "import",
  identity: string,
  sequence: number,
): IDBValidKey {
  return [direction, identity, sequence];
}

function deleteSnapshotFrameRecords(
  store: IDBObjectStore,
  direction: "export" | "import",
): Promise<void> {
  return new Promise((resolve, reject) => {
    const request = store.openCursor();
    request.onerror = () => reject(request.error ?? new Error("Snapshot frame cleanup failed"));
    request.onsuccess = () => {
      const cursor = request.result;
      if (cursor === null) {
        resolve();
        return;
      }
      if (Array.isArray(cursor.key) && cursor.key[0] === direction) cursor.delete();
      cursor.continue();
    };
  });
}

function snapshotHeaderFrameCount(header: SnapshotFrameStreamHeader): number {
  return SNAPSHOT_FRAME_KINDS.reduce(
    (total, kind) => safeByteSum(total, header.kinds[kind].frameCount, "Snapshot frame count"),
    0,
  );
}

function snapshotHeaderStoredBytes(header: SnapshotFrameStreamHeader): number {
  return SNAPSHOT_FRAME_KINDS.reduce(
    (total, kind) => safeByteSum(total, header.kinds[kind].storedBytes, "Snapshot stored bytes"),
    0,
  );
}

function asSnapshotFrame(value: unknown, expectedSequence: number): SnapshotFrame {
  if (!isRecord(value))
    throw corruption(SNAPSHOT_HEADER_STORE, "snapshot frame is missing or invalid");
  assertKnownFields(
    value,
    ["sequence", "kind", "itemCount", "key", "payload", "checksum"],
    SNAPSHOT_HEADER_STORE,
  );
  if (
    value.sequence !== expectedSequence ||
    !SNAPSHOT_FRAME_KINDS.includes(value.kind as SnapshotFrameKind) ||
    !Number.isSafeInteger(value.itemCount) ||
    (value.itemCount as number) < 1 ||
    (value.key !== null && !isStorageId(value.key)) ||
    !(value.payload instanceof Uint8Array) ||
    !Number.isSafeInteger(value.checksum) ||
    (value.checksum as number) < 0 ||
    (value.checksum as number) > 0xffff_ffff
  ) {
    throw corruption(SNAPSHOT_HEADER_STORE, "snapshot frame is invalid");
  }
  return {
    sequence: expectedSequence,
    kind: value.kind as SnapshotFrameKind,
    itemCount: value.itemCount as number,
    key: value.key,
    payload: value.payload,
    checksum: value.checksum as number,
  };
}

function prepareSnapshotFrame(
  value: SnapshotFrame,
  header: SnapshotFrameStreamHeader,
): SnapshotFrame {
  const frame = asSnapshotFrame(value, value.sequence);
  if (frame.sequence >= snapshotHeaderFrameCount(header)) {
    throw new RangeError("Snapshot frame sequence exceeds the header");
  }
  if (crc32(frame.payload) !== frame.checksum) throw new Error("Snapshot frame checksum mismatch");
  if (frame.kind === "block") {
    if (frame.key === null || frame.itemCount !== 1) {
      throw new TypeError("Snapshot block frame key/count is invalid");
    }
    try {
      verifyStoredBlock(frame.payload);
    } catch (error) {
      throw new TypeError(error instanceof Error ? error.message : "Snapshot block is invalid", {
        cause: error,
      });
    }
  } else {
    if (frame.key !== null) throw new TypeError("Snapshot metadata frame cannot have a key");
    if (decodeSnapshotMetadataItems(frame.kind, frame.payload).length !== frame.itemCount) {
      throw new TypeError("Snapshot metadata item count differs from its payload");
    }
  }
  return frame;
}

function validateSnapshotFrameBatch(frames: readonly SnapshotFrame[]): void {
  if (frames.length > MAX_SNAPSHOT_FRAME_BATCH_ITEMS) {
    throw new RangeError("Snapshot frame batch has too many frames");
  }
  let totalBytes = 0;
  let metadataBytes = 0;
  for (const frame of frames) {
    if (!(frame.payload instanceof Uint8Array))
      throw new TypeError("Snapshot frame payload must be bytes");
    totalBytes = safeByteSum(totalBytes, frame.payload.byteLength, "Snapshot frame batch bytes");
    if (frame.kind !== "block") {
      metadataBytes = safeByteSum(
        metadataBytes,
        frame.payload.byteLength,
        "Snapshot metadata batch bytes",
      );
    }
  }
  if (totalBytes > MAX_SNAPSHOT_FRAME_BATCH_BYTES)
    throw new RangeError("Snapshot frame batch is too large");
  if (metadataBytes > MAX_SNAPSHOT_METADATA_BATCH_BYTES) {
    throw new RangeError("Snapshot metadata batch is too large");
  }
}

function sameSnapshotFrame(left: SnapshotFrame, right: SnapshotFrame): boolean {
  return (
    left.sequence === right.sequence &&
    left.kind === right.kind &&
    left.itemCount === right.itemCount &&
    left.key === right.key &&
    left.checksum === right.checksum &&
    sameBytes(left.payload, right.payload)
  );
}

function snapshotBlockFrameRecord(frame: SnapshotFrame): SnapshotBlockFrameRecord {
  if (frame.kind !== "block" || frame.key === null) {
    throw new TypeError("Snapshot block frame descriptor is invalid");
  }
  return {
    kind: "snapshot-block-frame",
    sequence: frame.sequence,
    blockId: frame.key,
    byteLength: frame.payload.byteLength,
    checksum: frame.checksum,
  };
}

function asSnapshotBlockFrameRecord(value: unknown, sequence: number): SnapshotBlockFrameRecord {
  if (!isRecord(value))
    throw corruption(SNAPSHOT_HEADER_STORE, "snapshot block descriptor is missing");
  assertKnownFields(
    value,
    ["kind", "sequence", "blockId", "byteLength", "checksum"],
    SNAPSHOT_HEADER_STORE,
  );
  if (
    value.kind !== "snapshot-block-frame" ||
    value.sequence !== sequence ||
    !isStorageId(value.blockId) ||
    !Number.isSafeInteger(value.byteLength) ||
    (value.byteLength as number) < 1 ||
    !Number.isSafeInteger(value.checksum) ||
    (value.checksum as number) < 0 ||
    (value.checksum as number) > 0xffff_ffff
  ) {
    throw corruption(SNAPSHOT_HEADER_STORE, "snapshot block descriptor is invalid");
  }
  return {
    kind: "snapshot-block-frame",
    sequence,
    blockId: value.blockId,
    byteLength: value.byteLength as number,
    checksum: value.checksum as number,
  };
}

async function assertSnapshotImportFrameReplay(
  blocks: IDBObjectStore,
  catalog: IDBObjectStore,
  frameStore: IDBObjectStore,
  marker: SnapshotFrameImportMarker,
  frame: SnapshotFrame,
): Promise<void> {
  const storedValue = await requestResult<unknown>(
    frameStore.get(snapshotFrameKey("import", marker.identity, frame.sequence)),
  );
  if (frame.kind !== "block") {
    const stored = asSnapshotFrame(storedValue, frame.sequence);
    if (sameSnapshotFrame(stored, frame)) return;
  } else {
    const descriptor = asSnapshotBlockFrameRecord(storedValue, frame.sequence);
    const id = frame.key ?? "";
    const value = await requestResult<unknown>(blocks.get(id));
    const metadataValue = await requestResult<unknown>(catalog.get(blockMetadataKey(id)));
    const provenanceValue = await requestResult<unknown>(catalog.get(manifestBlockKey(id)));
    if (value !== undefined && metadataValue !== undefined && provenanceValue !== undefined) {
      const bytes = asBytes(value, `blocks/${id}`);
      const metadata = asStoredBlockMetadata(metadataValue, id);
      const provenance = asManifestBlockRecord(provenanceValue, id);
      if (
        descriptor.blockId === id &&
        descriptor.byteLength === frame.payload.byteLength &&
        descriptor.checksum === frame.checksum &&
        metadata.byteLength === frame.payload.byteLength &&
        metadata.checksum === frame.checksum &&
        provenance.byteLength === frame.payload.byteLength &&
        provenance.checksum === frame.checksum &&
        manifestBlockVisibleAt(provenance, marker.version) &&
        sameBytes(bytes, frame.payload)
      ) {
        return;
      }
    }
  }
  throw new SnapshotImportConflictError(
    marker.identity,
    marker.ownerId,
    `replayed frame ${String(frame.sequence)} differs`,
  );
}

async function nextManifestBlockRecord(
  index: IDBIndex,
  version: number,
  afterBlockId: string | null,
): Promise<ManifestBlockRecord | undefined> {
  return new Promise((resolve, reject) => {
    const request = index.openCursor();
    let sought = afterBlockId === null;
    request.addEventListener(
      "error",
      () => reject(request.error ?? new Error("Manifest block cursor failed")),
      { once: true },
    );
    request.addEventListener("success", () => {
      const cursor = request.result;
      if (cursor === null) {
        resolve(undefined);
        return;
      }
      try {
        if (!sought) {
          if (typeof cursor.key === "string" && cursor.key <= (afterBlockId ?? "")) {
            if (cursor.key < (afterBlockId ?? "")) cursor.continue(afterBlockId ?? undefined);
            else {
              sought = true;
              cursor.continue();
            }
            return;
          }
          sought = true;
        }
        sought = true;
        if (
          Array.isArray(cursor.primaryKey) &&
          cursor.primaryKey[0] === MANIFEST_BLOCK &&
          typeof cursor.primaryKey[1] === "string"
        ) {
          const record = asManifestBlockRecord(cursor.value, cursor.primaryKey[1]);
          if (manifestBlockVisibleAt(record, version)) {
            resolve(record);
            return;
          }
        }
        cursor.continue();
      } catch (error) {
        reject(error instanceof Error ? error : new Error(String(error)));
      }
    });
  });
}

function asSnapshotFrameExportMarker(value: unknown): SnapshotFrameExportMarker {
  if (!isRecord(value)) throw corruption(SNAPSHOT_EXPORT_KEY, "active export marker is invalid");
  assertKnownFields(
    value,
    [
      "kind",
      "sessionId",
      "ownerId",
      "manifestVersion",
      "createdAt",
      "expiresAt",
      "revision",
      "header",
      "metadataFrameCount",
      "nextBlockIndex",
      "lastBlockId",
    ],
    SNAPSHOT_EXPORT_KEY,
  );
  if (
    value.kind !== "snapshot-frame-export" ||
    !isStorageId(value.sessionId) ||
    !isStorageId(value.ownerId) ||
    !Number.isSafeInteger(value.manifestVersion) ||
    (value.manifestVersion as number) < 0 ||
    !Number.isSafeInteger(value.revision) ||
    (value.revision as number) < 0 ||
    !Number.isSafeInteger(value.metadataFrameCount) ||
    (value.metadataFrameCount as number) < 0 ||
    !Number.isSafeInteger(value.nextBlockIndex) ||
    (value.nextBlockIndex as number) < 0 ||
    (value.lastBlockId !== null && !isStorageId(value.lastBlockId))
  ) {
    throw corruption(SNAPSHOT_EXPORT_KEY, "active export marker is invalid");
  }
  const createdAt = canonicalStoredTimestamp(value.createdAt, `${SNAPSHOT_EXPORT_KEY}/createdAt`);
  const expiresAt = canonicalStoredTimestamp(value.expiresAt, `${SNAPSHOT_EXPORT_KEY}/expiresAt`);
  let header: SnapshotFrameStreamHeader;
  try {
    header = prepareSnapshotFrameStreamHeader(value.header as SnapshotFrameStreamHeader);
  } catch (error) {
    throw corruption(
      SNAPSHOT_EXPORT_KEY,
      error instanceof Error ? error.message : "snapshot header is invalid",
    );
  }
  const metadataFrameCount = SNAPSHOT_FRAME_KINDS.filter((kind) => kind !== "block").reduce(
    (total, kind) => safeByteSum(total, header.kinds[kind].frameCount, "Snapshot metadata frames"),
    0,
  );
  if (
    value.metadataFrameCount !== metadataFrameCount ||
    (value.nextBlockIndex as number) > header.kinds.block.frameCount ||
    ((value.nextBlockIndex as number) === 0) !== (value.lastBlockId === null) ||
    Date.parse(expiresAt) <= Date.parse(createdAt)
  ) {
    throw corruption(SNAPSHOT_EXPORT_KEY, "active export marker progress is invalid");
  }
  return {
    kind: "snapshot-frame-export",
    sessionId: value.sessionId,
    ownerId: value.ownerId,
    manifestVersion: value.manifestVersion as number,
    createdAt,
    expiresAt,
    revision: value.revision as number,
    header,
    metadataFrameCount,
    nextBlockIndex: value.nextBlockIndex as number,
    lastBlockId: value.lastBlockId,
  };
}

function assertSnapshotFrameExportLease(
  marker: SnapshotFrameExportMarker,
  lease: LeaseRecord,
): void {
  if (
    lease.kind !== "backup" ||
    lease.id !== marker.sessionId ||
    lease.ownerId !== marker.ownerId ||
    lease.manifestVersion !== marker.manifestVersion ||
    lease.createdAt !== marker.createdAt ||
    lease.expiresAt !== marker.expiresAt ||
    lease.revision !== marker.revision
  ) {
    throw corruption(SNAPSHOT_EXPORT_KEY, "active export marker and lease disagree");
  }
}

function asSnapshotFrameImportMarker(value: unknown): SnapshotFrameImportMarker {
  if (!isRecord(value))
    throw corruption(SNAPSHOT_FRAME_IMPORT_KEY, "snapshot import marker is invalid");
  assertKnownFields(
    value,
    [
      "kind",
      "identity",
      "ownerId",
      "version",
      "createdAt",
      "expiresAt",
      "header",
      "nextSequence",
      "stagedBytes",
      "frameCount",
      "itemCount",
      "checksum",
      "kindFrameCounts",
      "kindItemCounts",
      "kindStoredBytes",
      "replayCompleted",
    ],
    SNAPSHOT_FRAME_IMPORT_KEY,
  );
  const arrays = [value.kindFrameCounts, value.kindItemCounts, value.kindStoredBytes];
  if (
    value.kind !== "snapshot-frame-import" ||
    !isStorageId(value.identity) ||
    !isStorageId(value.ownerId) ||
    !Number.isSafeInteger(value.version) ||
    (value.version as number) < 0 ||
    !Number.isSafeInteger(value.nextSequence) ||
    (value.nextSequence as number) < 0 ||
    !Number.isSafeInteger(value.stagedBytes) ||
    (value.stagedBytes as number) < 0 ||
    !Number.isSafeInteger(value.frameCount) ||
    (value.frameCount as number) < 0 ||
    !Number.isSafeInteger(value.itemCount) ||
    (value.itemCount as number) < 0 ||
    !Number.isSafeInteger(value.checksum) ||
    (value.checksum as number) < 0 ||
    (value.checksum as number) > 0xffff_ffff ||
    typeof value.replayCompleted !== "boolean" ||
    arrays.some(
      (array) =>
        !Array.isArray(array) ||
        array.length !== SNAPSHOT_FRAME_KINDS.length ||
        array.some((entry) => !Number.isSafeInteger(entry) || (entry as number) < 0),
    )
  ) {
    throw corruption(SNAPSHOT_FRAME_IMPORT_KEY, "snapshot import marker is invalid");
  }
  const createdAt = canonicalStoredTimestamp(
    value.createdAt,
    `${SNAPSHOT_FRAME_IMPORT_KEY}/createdAt`,
  );
  const expiresAt = canonicalStoredTimestamp(
    value.expiresAt,
    `${SNAPSHOT_FRAME_IMPORT_KEY}/expiresAt`,
  );
  let header: SnapshotFrameStreamHeader;
  try {
    header = prepareSnapshotFrameStreamHeader(value.header as SnapshotFrameStreamHeader);
  } catch (error) {
    throw corruption(
      SNAPSHOT_FRAME_IMPORT_KEY,
      error instanceof Error ? error.message : "snapshot header is invalid",
    );
  }
  if (
    header.databaseVersion !== value.version ||
    value.nextSequence !== value.frameCount ||
    (value.nextSequence as number) > snapshotHeaderFrameCount(header) ||
    (value.stagedBytes as number) > snapshotHeaderStoredBytes(header) ||
    Date.parse(expiresAt) <= Date.parse(createdAt)
  ) {
    throw corruption(SNAPSHOT_FRAME_IMPORT_KEY, "snapshot import progress is inconsistent");
  }
  return {
    kind: "snapshot-frame-import",
    identity: value.identity,
    ownerId: value.ownerId,
    version: value.version,
    createdAt,
    expiresAt,
    header,
    nextSequence: value.nextSequence as number,
    stagedBytes: value.stagedBytes as number,
    frameCount: value.frameCount as number,
    itemCount: value.itemCount as number,
    checksum: value.checksum as number,
    kindFrameCounts: value.kindFrameCounts as number[],
    kindItemCounts: value.kindItemCounts as number[],
    kindStoredBytes: value.kindStoredBytes as number[],
    replayCompleted: value.replayCompleted,
  };
}

function requireSnapshotFrameImportMarker(
  value: unknown,
  identity: string,
  ownerId: string,
  cutoff: number,
): SnapshotFrameImportMarker {
  if (value === undefined) {
    throw new SnapshotImportConflictError(identity, ownerId, "session is missing");
  }
  const marker = asSnapshotFrameImportMarker(value);
  if (marker.identity !== identity || marker.ownerId !== ownerId) {
    throw new SnapshotImportConflictError(
      identity,
      marker.ownerId,
      "session is owned by another caller",
    );
  }
  if (Date.parse(marker.expiresAt) <= cutoff) {
    throw new SnapshotImportConflictError(identity, ownerId, "session is expired");
  }
  return marker;
}

function snapshotFrameImportSession(marker: SnapshotFrameImportMarker): SnapshotFrameImportSession {
  return {
    identity: marker.identity,
    ownerId: marker.ownerId,
    version: marker.version,
    createdAt: marker.createdAt,
    expiresAt: marker.expiresAt,
    nextSequence: marker.nextSequence,
    stagedBytes: marker.stagedBytes,
  };
}

function asOptionalCompletedSnapshotFrameImportRecord(
  value: unknown,
): CompletedSnapshotFrameImportRecord | undefined {
  if (value === undefined) return undefined;
  if (!isRecord(value))
    throw corruption(SNAPSHOT_FRAME_COMPLETED_KEY, "completed import is invalid");
  assertKnownFields(
    value,
    ["kind", "identity", "version", "createdAt", "header"],
    SNAPSHOT_FRAME_COMPLETED_KEY,
  );
  if (
    value.kind !== "snapshot-frame-import-completed" ||
    !isStorageId(value.identity) ||
    !Number.isSafeInteger(value.version) ||
    (value.version as number) < 0
  ) {
    throw corruption(SNAPSHOT_FRAME_COMPLETED_KEY, "completed import is invalid");
  }
  let header: SnapshotFrameStreamHeader;
  try {
    header = prepareSnapshotFrameStreamHeader(value.header as SnapshotFrameStreamHeader);
  } catch (error) {
    throw corruption(
      SNAPSHOT_FRAME_COMPLETED_KEY,
      error instanceof Error ? error.message : "completed import header is invalid",
    );
  }
  if (header.databaseVersion !== value.version) {
    throw corruption(SNAPSHOT_FRAME_COMPLETED_KEY, "completed import version is inconsistent");
  }
  return {
    kind: "snapshot-frame-import-completed",
    identity: value.identity,
    version: value.version,
    createdAt: canonicalStoredTimestamp(
      value.createdAt,
      `${SNAPSHOT_FRAME_COMPLETED_KEY}/createdAt`,
    ),
    header,
  };
}

function sameSnapshotFrameHeader(
  left: SnapshotFrameStreamHeader,
  right: SnapshotFrameStreamHeader,
): boolean {
  return snapshotFrameStreamHeaderIdentity(left) === snapshotFrameStreamHeaderIdentity(right);
}

function validateSnapshotFrameFooter(
  marker: SnapshotFrameImportMarker,
  footer: SnapshotFrameFooter,
): void {
  if (
    !Number.isSafeInteger(footer.frameCount) ||
    !Number.isSafeInteger(footer.itemCount) ||
    !Number.isSafeInteger(footer.storedBytes) ||
    !Number.isSafeInteger(footer.checksum) ||
    footer.frameCount !== marker.frameCount ||
    footer.itemCount !== marker.itemCount ||
    footer.storedBytes !== marker.stagedBytes ||
    footer.checksum !== marker.checksum ||
    marker.nextSequence !== snapshotHeaderFrameCount(marker.header) ||
    marker.stagedBytes !== snapshotHeaderStoredBytes(marker.header)
  ) {
    throw new SnapshotImportConflictError(
      marker.identity,
      marker.ownerId,
      "footer differs from staged frames",
    );
  }
  SNAPSHOT_FRAME_KINDS.forEach((kind, index) => {
    const expected = marker.header.kinds[kind];
    if (
      marker.kindFrameCounts[index] !== expected.frameCount ||
      marker.kindItemCounts[index] !== expected.itemCount ||
      marker.kindStoredBytes[index] !== expected.storedBytes
    ) {
      throw new SnapshotImportConflictError(
        marker.identity,
        marker.ownerId,
        `${kind} summary is incomplete`,
      );
    }
  });
}

function snapshotFrameKindAtSequence(
  header: SnapshotFrameStreamHeader,
  sequence: number,
): SnapshotFrameKind {
  let offset = 0;
  for (const kind of SNAPSHOT_FRAME_KINDS) {
    offset += header.kinds[kind].frameCount;
    if (sequence < offset) return kind;
  }
  throw new RangeError("Snapshot frame sequence exceeds the header");
}

function snapshotUniqueStagingKey(generationId: string): string {
  return `${SNAPSHOT_UNIQUE_STAGING_PREFIX}${encodeURIComponent(generationId)}`;
}

function snapshotPostingStagingKey(generationId: string): string {
  return `${SNAPSHOT_POSTING_STAGING_PREFIX}${encodeURIComponent(generationId)}`;
}

function snapshotUniqueOwnerKey(namespaceId: string): string {
  return `${SNAPSHOT_UNIQUE_OWNER_PREFIX}${encodeURIComponent(namespaceId)}`;
}

function snapshotPostingOwnerPointerKey(tableId: string, storageColumnId: string): string {
  return `${SNAPSHOT_POSTING_OWNER_PREFIX}${encodeURIComponent(snapshotPostingOwnerKey(tableId, storageColumnId))}`;
}

function asSnapshotUniqueStagingRecord(
  value: unknown,
  generationId: string,
): SnapshotUniqueStagingRecord {
  const location = snapshotUniqueStagingKey(generationId);
  if (!isRecord(value)) throw corruption(location, "snapshot UNIQUE staging record is invalid");
  assertKnownFields(
    value,
    ["kind", "descriptor", "nextOrdinal", "tokenCount", "lastToken"],
    location,
  );
  if (
    value.kind !== "snapshot-unique-staging" ||
    !Number.isSafeInteger(value.nextOrdinal) ||
    (value.nextOrdinal as number) < 0 ||
    !Number.isSafeInteger(value.tokenCount) ||
    (value.tokenCount as number) < 0 ||
    (value.lastToken !== null && typeof value.lastToken !== "string")
  ) {
    throw corruption(location, "snapshot UNIQUE staging record is invalid");
  }
  const descriptor = value.descriptor as SnapshotUniqueItem;
  if (descriptor.kind !== "unique-generation" || descriptor.generationId !== generationId) {
    throw corruption(location, "snapshot UNIQUE staging descriptor is invalid");
  }
  return {
    kind: "snapshot-unique-staging",
    descriptor,
    nextOrdinal: value.nextOrdinal as number,
    tokenCount: value.tokenCount as number,
    lastToken: value.lastToken,
  };
}

function asSnapshotPostingStagingRecord(
  value: unknown,
  generationId: string,
): SnapshotPostingStagingRecord {
  const location = snapshotPostingStagingKey(generationId);
  if (!isRecord(value)) throw corruption(location, "snapshot posting staging record is invalid");
  assertKnownFields(
    value,
    ["kind", "descriptor", "nextOrdinal", "totalTokens", "lastTerm", "boundaries"],
    location,
  );
  if (
    value.kind !== "snapshot-posting-staging" ||
    !Number.isSafeInteger(value.nextOrdinal) ||
    (value.nextOrdinal as number) < 0 ||
    !Number.isSafeInteger(value.totalTokens) ||
    (value.totalTokens as number) < 0 ||
    (value.lastTerm !== null && typeof value.lastTerm !== "string") ||
    !Array.isArray(value.boundaries)
  ) {
    throw corruption(location, "snapshot posting staging record is invalid");
  }
  const descriptor = value.descriptor as SnapshotPostingItem;
  if (descriptor.kind !== "posting-generation" || descriptor.generationId !== generationId) {
    throw corruption(location, "snapshot posting staging descriptor is invalid");
  }
  const boundaries = value.boundaries.map((boundary) => {
    if (
      !isRecord(boundary) ||
      typeof boundary.first !== "string" ||
      typeof boundary.last !== "string" ||
      Object.keys(boundary).some((field) => field !== "first" && field !== "last")
    ) {
      throw corruption(location, "snapshot posting boundary is invalid");
    }
    return { first: boundary.first, last: boundary.last };
  });
  return {
    kind: "snapshot-posting-staging",
    descriptor,
    nextOrdinal: value.nextOrdinal as number,
    totalTokens: value.totalTokens as number,
    lastTerm: value.lastTerm,
    boundaries,
  };
}

async function stageSnapshotMetadataFrame(
  transaction: IDBTransaction,
  frame: SnapshotFrame,
): Promise<void> {
  if (frame.kind === "block") throw new TypeError("Block frame is not snapshot metadata");
  const items = decodeSnapshotMetadataItems(frame.kind, frame.payload);
  if (items.length !== 1 || frame.itemCount !== 1) {
    throw new TypeError("Snapshot v1 metadata frame must contain exactly one item");
  }
  const item = items[0];
  if (item === undefined) throw new TypeError("Snapshot metadata frame is empty");
  const catalog = transaction.objectStore("catalog");
  if (item.kind === "table") {
    const tableKey = `${TABLE_ID_PREFIX}${item.record.id}`;
    const record = asIncomingTableRecord(item.record, tableKey);
    await updateCatalogResourceLedger(transaction.objectStore("statistics"), undefined, record);
    catalog.add(record, tableKey);
    catalog.add(record.id, `${TABLE_NAME_PREFIX}${record.name}`);
    catalog.put(item.nextRowId, `${ROW_ID_PREFIX}${record.id}`);
    for (const entry of item.autoIncrement) {
      catalog.put(entry.next, `${AUTO_INCREMENT_PREFIX}${record.id}/${entry.columnId}`);
    }
    for (const [indexId, index] of Object.entries(record.secondaryIndexes ?? {})) {
      catalog.add({ tableId: record.id, indexId }, `${SECONDARY_INDEX_NAME_PREFIX}${index.name}`);
    }
    for (const trigger of record.triggers ?? []) {
      catalog.add(
        { tableId: record.id, triggerId: trigger.id },
        `${TRIGGER_NAME_PREFIX}${trigger.name}`,
      );
      catalog.add(
        { tableId: record.id, triggerName: trigger.name },
        `${TRIGGER_ID_PREFIX}${trigger.id}`,
      );
    }
    return;
  }
  if (item.kind === "segment") {
    const record = asSegmentRecord(item.record);
    await updateRecordResourceLedger(transaction.objectStore("statistics"), {
      segments: [{ next: record }],
    });
    transaction.objectStore("segments").add(record, record.id);
    return;
  }
  if (item.kind === "transaction") {
    const record = asTransactionRecord(item.record, item.record.id);
    if (record.status === "active" && record.pendingTable !== undefined) {
      await assertCatalogReservationAdmission(transaction, record.pendingTable);
      await assertTableForeignKeysInTransaction(catalog, record.pendingTable);
    }
    transaction.objectStore("transactions").add(record, record.id);
    return;
  }
  if (item.kind === "unique-generation") {
    const tableValue: unknown = await requestResult(
      catalog.get(`${TABLE_ID_PREFIX}${item.tableId}`),
    );
    if (tableValue === undefined) throw new TypeError("Snapshot UNIQUE owner table is missing");
    const table = asTableRecord(tableValue);
    const expectedNamespace =
      item.indexId === null
        ? table.uniqueKeyColumnId === undefined
          ? undefined
          : table.id
        : table.secondaryIndexes?.[item.indexId]?.uniqueEnforced === true
          ? secondaryUniqueKeyNamespace(table.id, item.indexId)
          : undefined;
    if (expectedNamespace !== item.namespaceId) {
      throw new TypeError("Snapshot UNIQUE descriptor has the wrong catalog owner");
    }
    catalog.add(
      {
        kind: "snapshot-unique-staging",
        descriptor: item,
        nextOrdinal: 0,
        tokenCount: 0,
        lastToken: null,
      } satisfies SnapshotUniqueStagingRecord,
      snapshotUniqueStagingKey(item.generationId),
    );
    catalog.add(item.generationId, snapshotUniqueOwnerKey(item.namespaceId));
    return;
  }
  if (item.kind === "unique-chunk") {
    const key = snapshotUniqueStagingKey(item.generationId);
    const staging = asSnapshotUniqueStagingRecord(
      await requestResult<unknown>(catalog.get(key)),
      item.generationId,
    );
    if (
      staging.descriptor.namespaceId !== item.namespaceId ||
      item.ordinal !== staging.nextOrdinal ||
      (staging.lastToken !== null && (item.keyTokens[0] ?? "") <= staging.lastToken)
    ) {
      throw new TypeError("Snapshot UNIQUE chunks are not contiguous and globally ordered");
    }
    for (const part of splitUniqueMembershipTokens(item.keyTokens)) {
      catalog.add(part, uniqueKeyBasePartKey(item.generationId, part[0] ?? ""));
    }
    catalog.put(
      {
        ...staging,
        nextOrdinal: incrementSafeInteger(staging.nextOrdinal, "Snapshot UNIQUE ordinal"),
        tokenCount: safeByteSum(
          staging.tokenCount,
          item.keyTokens.length,
          "Snapshot UNIQUE token count",
        ),
        lastToken: item.keyTokens.at(-1) ?? staging.lastToken,
      },
      key,
    );
    return;
  }
  if (item.kind === "posting-generation") {
    const tableValue: unknown = await requestResult(
      catalog.get(`${TABLE_ID_PREFIX}${item.tableId}`),
    );
    if (tableValue === undefined) throw new TypeError("Snapshot posting owner table is missing");
    const table = asTableRecord(tableValue);
    const owned =
      item.ownerKind === "fts-column"
        ? table.ftsColumns?.[item.ownerId]?.state === "ready" &&
          item.storageColumnId === item.ownerId
        : table.secondaryIndexes?.[item.ownerId]?.state === "ready" &&
          table.secondaryIndexes[item.ownerId]?.storageColumnId === item.storageColumnId;
    if (!owned) throw new TypeError("Snapshot posting descriptor has no ready catalog owner");
    catalog.add(
      {
        kind: "snapshot-posting-staging",
        descriptor: item,
        nextOrdinal: 0,
        totalTokens: 0,
        lastTerm: null,
        boundaries: [],
      } satisfies SnapshotPostingStagingRecord,
      snapshotPostingStagingKey(item.generationId),
    );
    catalog.add(
      item.generationId,
      snapshotPostingOwnerPointerKey(item.tableId, item.storageColumnId),
    );
    return;
  }
  const key = snapshotPostingStagingKey(item.generationId);
  const staging = asSnapshotPostingStagingRecord(
    await requestResult<unknown>(catalog.get(key)),
    item.generationId,
  );
  if (
    staging.descriptor.storageColumnId !== item.storageColumnId ||
    item.ordinal !== staging.nextOrdinal ||
    (staging.lastTerm !== null && (item.postings[0]?.term ?? "") <= staging.lastTerm)
  ) {
    throw new TypeError("Snapshot posting chunks are not contiguous and globally ordered");
  }
  const tableId = staging.descriptor.tableId;
  const prefix = ftsBaseChunkPrefix(tableId, item.storageColumnId, item.generationId);
  catalog.add(
    item.postings.map((posting) => structuredClone(posting)),
    `${prefix}${String(item.ordinal).padStart(6, "0")}`,
  );
  const chunkTokens = ftsPostingTokenCount(item.postings);
  catalog.put(
    {
      ...staging,
      nextOrdinal: incrementSafeInteger(staging.nextOrdinal, "Snapshot posting ordinal"),
      totalTokens: safeByteSum(staging.totalTokens, chunkTokens, "Snapshot posting token count"),
      lastTerm: item.postings.at(-1)?.term ?? staging.lastTerm,
      boundaries: [
        ...staging.boundaries,
        { first: item.postings[0]?.term ?? "", last: item.postings.at(-1)?.term ?? "" },
      ],
    },
    key,
  );
}

async function readSnapshotGenerationId(
  catalog: IDBObjectStore,
  key: string,
  label: string,
): Promise<string> {
  const value: unknown = await requestResult(catalog.get(key));
  if (!isStorageId(value))
    throw corruption(key, `${label} generation pointer is missing or invalid`);
  return value;
}

async function validateAndPromoteStagedSnapshot(
  transaction: IDBTransaction,
  marker: SnapshotFrameImportMarker,
): Promise<void> {
  const catalog = transaction.objectStore("catalog");
  const segments = transaction.objectStore("segments");
  const transactions = transaction.objectStore("transactions");
  const blocks = transaction.objectStore("blocks");
  await visitObjectStoreSequentially(catalog, async (value, key) => {
    if (typeof key !== "string" || !key.startsWith(TABLE_ID_PREFIX)) return;
    const table = asTableRecord(value, key);
    const nameValue: unknown = await requestResult(
      catalog.get(`${TABLE_NAME_PREFIX}${table.name}`),
    );
    if (nameValue !== table.id) throw corruption(key, "snapshot table name marker is missing");
    if (
      asOptionalCounter(
        await requestResult<unknown>(catalog.get(`${ROW_ID_PREFIX}${table.id}`)),
        `${ROW_ID_PREFIX}${table.id}`,
      ) === undefined
    ) {
      throw corruption(key, "snapshot table row counter is missing");
    }
    for (const column of table.columns) {
      if (column.defaultValue?.kind !== "autoincrement") continue;
      const counterKey = `${AUTO_INCREMENT_PREFIX}${table.id}/${column.id}`;
      if (
        asOptionalCounter(await requestResult<unknown>(catalog.get(counterKey)), counterKey) ===
        undefined
      ) {
        throw corruption(key, `snapshot auto-increment counter is missing for ${column.id}`);
      }
    }
    if (table.uniqueKeyColumnId !== undefined) {
      const generationId = await readSnapshotGenerationId(
        catalog,
        snapshotUniqueOwnerKey(table.id),
        "UNIQUE",
      );
      asSnapshotUniqueStagingRecord(
        await requestResult<unknown>(catalog.get(snapshotUniqueStagingKey(generationId))),
        generationId,
      );
    }
    for (const [indexId, index] of Object.entries(table.secondaryIndexes ?? {})) {
      if (index.uniqueEnforced === true) {
        const namespaceId = secondaryUniqueKeyNamespace(table.id, indexId);
        const generationId = await readSnapshotGenerationId(
          catalog,
          snapshotUniqueOwnerKey(namespaceId),
          "secondary UNIQUE",
        );
        asSnapshotUniqueStagingRecord(
          await requestResult<unknown>(catalog.get(snapshotUniqueStagingKey(generationId))),
          generationId,
        );
      }
      if (index.state === "ready") {
        const generationId = await readSnapshotGenerationId(
          catalog,
          snapshotPostingOwnerPointerKey(table.id, index.storageColumnId),
          "secondary posting",
        );
        asSnapshotPostingStagingRecord(
          await requestResult<unknown>(catalog.get(snapshotPostingStagingKey(generationId))),
          generationId,
        );
      }
    }
    for (const [columnId, state] of Object.entries(table.ftsColumns ?? {})) {
      if (state.state !== "ready") continue;
      const generationId = await readSnapshotGenerationId(
        catalog,
        snapshotPostingOwnerPointerKey(table.id, columnId),
        "full-text posting",
      );
      asSnapshotPostingStagingRecord(
        await requestResult<unknown>(catalog.get(snapshotPostingStagingKey(generationId))),
        generationId,
      );
    }
  });
  await visitObjectStoreSequentially(transactions, (value, key) => {
    if (typeof key !== "string")
      throw corruption("transactions", "snapshot transaction key is invalid");
    const record = asTransactionRecord(value, key);
    if (
      record.status !== "committed" ||
      record.committedVersion === null ||
      record.committedVersion > marker.version ||
      record.pendingBlockIds.length !== 0 ||
      record.pendingSegmentIds.length !== 0
    ) {
      throw corruption(
        `transactions/${key}`,
        "snapshot transaction is not canonical committed history",
      );
    }
  });
  await visitObjectStoreSequentially(segments, async (value, key) => {
    if (typeof key !== "string") throw corruption("segments", "snapshot segment key is invalid");
    const segment = asSegmentRecord(value);
    if (segment.id !== key)
      throw corruption(`segments/${key}`, "snapshot segment id differs from key");
    if (
      (await requestResult(catalog.getKey(`${TABLE_ID_PREFIX}${segment.tableId}`))) === undefined
    ) {
      throw corruption(`segments/${key}`, "snapshot segment table is missing");
    }
    const ownerValue: unknown = await requestResult(transactions.get(segment.transactionId));
    if (ownerValue === undefined)
      throw corruption(`segments/${key}`, "snapshot segment owner is missing");
    asTransactionRecord(ownerValue, segment.transactionId);
    for (const blockId of segmentBlockIds(segment)) {
      const provenance = asManifestBlockRecord(
        await requestResult<unknown>(catalog.get(manifestBlockKey(blockId))),
        blockId,
      );
      if (!manifestBlockVisibleAt(provenance, marker.version)) {
        throw corruption(`segments/${key}`, `snapshot segment block ${blockId} is not live`);
      }
    }
  });
  let liveBlockCount = 0;
  let liveBlockBytes = 0;
  await visitManifestBlockRecords(catalog, async (record) => {
    if (!manifestBlockVisibleAt(record, marker.version)) return undefined;
    const payloadValue: unknown = await requestResult(blocks.get(record.blockId));
    if (payloadValue === undefined)
      throw corruption(`blocks/${record.blockId}`, "snapshot block is missing");
    const payload = asBytes(payloadValue, `blocks/${record.blockId}`);
    const metadata = asStoredBlockMetadata(
      await requestResult<unknown>(catalog.get(blockMetadataKey(record.blockId))),
      record.blockId,
    );
    if (
      payload.byteLength !== record.byteLength ||
      metadata.byteLength !== record.byteLength ||
      metadata.checksum !== record.checksum ||
      crc32(payload) !== record.checksum
    ) {
      throw corruption(`blocks/${record.blockId}`, "snapshot block metadata is inconsistent");
    }
    liveBlockCount = incrementSafeInteger(liveBlockCount, "Snapshot live block count");
    liveBlockBytes = safeByteSum(liveBlockBytes, record.byteLength, "Snapshot live block bytes");
    return undefined;
  });
  if (
    liveBlockCount !== marker.header.kinds.block.itemCount ||
    liveBlockBytes !== marker.header.kinds.block.storedBytes
  ) {
    throw corruption(
      SNAPSHOT_FRAME_IMPORT_KEY,
      "snapshot block provenance disagrees with the header",
    );
  }
  await visitObjectStoreSequentially(catalog, (value, key) => {
    if (typeof key !== "string") return;
    if (key.startsWith(SNAPSHOT_UNIQUE_STAGING_PREFIX)) {
      const generationId = decodeURIComponent(key.slice(SNAPSHOT_UNIQUE_STAGING_PREFIX.length));
      const staging = asSnapshotUniqueStagingRecord(value, generationId);
      if (
        staging.nextOrdinal !== staging.descriptor.chunkCount ||
        staging.tokenCount !== staging.descriptor.tokenCount
      ) {
        throw corruption(key, "snapshot UNIQUE generation is incomplete");
      }
      catalog.put(
        {
          versions: [],
          hasBase: true,
          baseGenerationId: generationId,
          tokenCount: staging.tokenCount,
        } satisfies UniqueKeyChunkIndex,
        uniqueKeyChunkIndexKey(staging.descriptor.namespaceId),
      );
      catalog.delete(snapshotUniqueOwnerKey(staging.descriptor.namespaceId));
      catalog.delete(key);
      return;
    }
    if (key.startsWith(SNAPSHOT_POSTING_STAGING_PREFIX)) {
      const generationId = decodeURIComponent(key.slice(SNAPSHOT_POSTING_STAGING_PREFIX.length));
      const staging = asSnapshotPostingStagingRecord(value, generationId);
      if (
        staging.nextOrdinal !== staging.descriptor.chunkCount ||
        staging.totalTokens !== staging.descriptor.totalTokens ||
        staging.boundaries.length !== staging.descriptor.chunkCount
      ) {
        throw corruption(key, "snapshot posting generation is incomplete");
      }
      const identity = `${staging.descriptor.tableId}/${staging.descriptor.storageColumnId}`;
      catalog.put(
        {
          coversVersion: staging.descriptor.coversVersion,
          boundaries: staging.boundaries,
          totalTokens: staging.totalTokens,
          generation: generationId,
        },
        `${FTS_BASE_INDEX_PREFIX}${identity}`,
      );
      catalog.put({ versions: [] }, `${FTS_CHUNK_PREFIX}index/${identity}`);
      catalog.delete(
        snapshotPostingOwnerPointerKey(
          staging.descriptor.tableId,
          staging.descriptor.storageColumnId,
        ),
      );
      catalog.delete(key);
    }
  });
}

async function readSnapshotPostingGeneration(
  catalog: IDBObjectStore,
  tableId: string,
  columnId: string,
  version: number,
  observeRetainedBytes: (bytes: number) => void = () => undefined,
): Promise<{ generationId: string; chunks: FtsPosting[][]; totalTokens: number } | undefined> {
  const identity = `${tableId}/${columnId}`;
  const toc = decodeFtsBaseToc(
    await requestResult<unknown>(catalog.get(`${FTS_BASE_INDEX_PREFIX}${identity}`)),
  );
  const deltaIndex = decodeFtsDeltaIndex(
    await requestResult<unknown>(catalog.get(`${FTS_CHUNK_PREFIX}index/${identity}`)),
  );
  if (toc === undefined || deltaIndex === undefined || toc.coversVersion > version)
    return undefined;
  const chunks: FtsPosting[][] = [];
  let retainedBytes = 0;
  let totalTokens = 0;
  const retain = (chunk: FtsPosting[]): boolean => {
    const bounds = ftsPostingChunkRetainedBounds(chunk);
    if (bounds.bytes > MAX_FTS_ORDERED_READ_BYTES - retainedBytes) return false;
    retainedBytes += bounds.bytes;
    observeRetainedBytes(retainedBytes);
    chunks.push(chunk);
    return true;
  };
  const prefix = ftsBaseChunkPrefix(tableId, columnId, toc.generation);
  for (let ordinal = 0; ordinal < toc.boundaries.length; ordinal += 1) {
    const chunk = decodeFtsPostingChunk(
      await requestResult<unknown>(catalog.get(`${prefix}${String(ordinal).padStart(6, "0")}`)),
    );
    if (chunk === undefined || !ftsChunkMatchesBoundary(chunk, toc.boundaries[ordinal])) {
      return undefined;
    }
    if (!retain(chunk)) return undefined;
    totalTokens = safeByteSum(
      totalTokens,
      ftsPostingTokenCount(chunk),
      "Snapshot posting token count",
    );
  }
  for (const deltaVersion of deltaIndex.versions) {
    if (deltaVersion <= toc.coversVersion || deltaVersion > version) continue;
    const delta = decodeFtsDeltaChunk(
      await requestResult<unknown>(catalog.get(ftsChunkKey(tableId, columnId, deltaVersion))),
    );
    if (delta === undefined) return undefined;
    if (delta.postings.length > 0 && !retain(delta.postings)) return undefined;
    totalTokens = safeByteSum(totalTokens, delta.totalTokens, "Snapshot posting token count");
  }
  const merged = collectFtsPostings(chunks);
  if (ftsPostingChunkRetainedBounds(merged).bytes > MAX_FTS_ORDERED_READ_BYTES) {
    return undefined;
  }
  const orderedChunks: FtsPosting[][] = [];
  let current: FtsPosting[] = [];
  let currentBytes = 0;
  let currentRowIds = 0;
  for (const posting of merged) {
    const bounds = ftsPostingChunkRetainedBounds([posting]);
    if (
      bounds.bytes > SNAPSHOT_ACCELERATOR_PART_RETAINED_BYTES ||
      bounds.rowIds > MAX_FTS_POSTING_ROW_IDS_PER_CHUNK
    ) {
      // Snapshot v1 does not split one term across frames because restore requires strict global
      // term order. Omitting the whole accelerator is safe: the catalog copy is marked invalid.
      return undefined;
    }
    if (
      current.length === MAX_FTS_POSTINGS_PER_CHUNK ||
      currentBytes > SNAPSHOT_ACCELERATOR_PART_RETAINED_BYTES - bounds.bytes ||
      currentRowIds > MAX_FTS_POSTING_ROW_IDS_PER_CHUNK - bounds.rowIds
    ) {
      orderedChunks.push(current);
      current = [];
      currentBytes = 0;
      currentRowIds = 0;
    }
    current.push(posting);
    currentBytes += bounds.bytes;
    currentRowIds += bounds.rowIds;
  }
  if (current.length > 0) orderedChunks.push(current);
  const mergedTotalTokens = ftsPostingTokenCount(merged);
  if (mergedTotalTokens !== totalTokens) {
    // Overlapping row-window bases may repeat the same logical posting. The merged generation is
    // authoritative for the exported candidate index, so its exact frequencies define the new
    // generation's total rather than the sum of overlapping physical windows.
    totalTokens = mergedTotalTokens;
  }
  return { generationId: toc.generation, chunks: orderedChunks, totalTokens };
}

function snapshotPostingOwnerKey(tableId: string, storageColumnId: string): string {
  return `${String(tableId.length)}:${tableId}${storageColumnId}`;
}

async function planSnapshotUniqueMembership(
  catalog: IDBObjectStore,
  namespaceId: string,
  indexId: string | null,
  observeRetainedBytes: (bytes: number) => void,
): Promise<IndexedDbSnapshotUniqueMembership> {
  let chunkCount = 0;
  let chunkTokens = 0;
  let chunkBytes = 0;
  const result = await visitCanonicalUniqueKeyTokens(
    catalog,
    namespaceId,
    (token, retainedSourceBytes) => {
      const tokenBytes = uniqueMembershipTokenRetainedBytes(token);
      if (
        chunkTokens === UNIQUE_KEY_MEMBERSHIP_PART_TOKENS ||
        chunkBytes > UNIQUE_KEY_MEMBERSHIP_PART_RETAINED_BYTES - tokenBytes
      ) {
        chunkCount = incrementSafeInteger(chunkCount, "Snapshot UNIQUE chunk count");
        chunkTokens = 0;
        chunkBytes = 0;
      }
      chunkTokens += 1;
      chunkBytes += tokenBytes;
      observeRetainedBytes(safeByteSum(retainedSourceBytes, chunkBytes, "Snapshot UNIQUE bytes"));
    },
  );
  if (chunkTokens > 0) {
    chunkCount = incrementSafeInteger(chunkCount, "Snapshot UNIQUE chunk count");
  }
  return {
    namespaceId,
    indexId,
    generationId: result.generationId,
    chunkCount,
    tokenCount: result.tokenCount,
  };
}

async function readSnapshotTableMetadata(
  catalog: IDBObjectStore,
  record: TableRecord,
  version: number,
  observeRetainedBytes: (bytes: number) => void,
): Promise<IndexedDbSnapshotTable> {
  const [rowIdValue, ...autoIncrementValues] = await Promise.all([
    requestResult<unknown>(catalog.get(`${ROW_ID_PREFIX}${record.id}`)),
    ...record.columns.map((column) =>
      requestResult<unknown>(catalog.get(`${AUTO_INCREMENT_PREFIX}${record.id}/${column.id}`)),
    ),
  ]);
  const uniqueMemberships: IndexedDbSnapshotUniqueMembership[] = [];
  if (record.uniqueKeyColumnId !== undefined) {
    uniqueMemberships.push(
      await planSnapshotUniqueMembership(catalog, record.id, null, observeRetainedBytes),
    );
  }
  for (const [indexId, index] of Object.entries(record.secondaryIndexes ?? {})) {
    if (index.uniqueEnforced !== true) continue;
    const namespaceId = secondaryUniqueKeyNamespace(record.id, indexId);
    uniqueMemberships.push(
      await planSnapshotUniqueMembership(catalog, namespaceId, indexId, observeRetainedBytes),
    );
  }

  const fts: IndexedDbSnapshotFtsIndex[] = [];
  const ftsColumns = { ...(record.ftsColumns ?? {}) };
  for (const [columnId, state] of Object.entries(ftsColumns)) {
    const generation =
      state.state === "ready"
        ? await readSnapshotPostingGeneration(
            catalog,
            record.id,
            columnId,
            version,
            observeRetainedBytes,
          )
        : undefined;
    if (generation === undefined) {
      ftsColumns[columnId] = { ...state, state: "invalid" };
      continue;
    }
    fts.push({
      columnId,
      coversVersion: version,
      totalTokens: generation.totalTokens,
      chunkCount: generation.chunks.length,
      generationId: generation.generationId,
    });
  }
  const secondaryIndexes = { ...(record.secondaryIndexes ?? {}) };
  for (const [indexId, state] of Object.entries(secondaryIndexes)) {
    const storageColumnId = state.storageColumnId;
    const generation =
      state.state === "ready"
        ? await readSnapshotPostingGeneration(
            catalog,
            record.id,
            storageColumnId,
            version,
            observeRetainedBytes,
          )
        : undefined;
    if (generation === undefined) {
      const { buildId: _abandonedBuild, ...invalid } = state;
      void _abandonedBuild;
      secondaryIndexes[indexId] = { ...invalid, state: "invalid" };
      continue;
    }
    fts.push({
      columnId: storageColumnId,
      coversVersion: version,
      totalTokens: generation.totalTokens,
      chunkCount: generation.chunks.length,
      generationId: generation.generationId,
    });
  }
  return {
    record: {
      ...record,
      ...(Object.keys(ftsColumns).length === 0 ? {} : { ftsColumns }),
      ...(Object.keys(secondaryIndexes).length === 0 ? {} : { secondaryIndexes }),
    },
    nextRowId: asOptionalCounter(rowIdValue, `${ROW_ID_PREFIX}${record.id}`) ?? 1n,
    autoIncrement: record.columns.flatMap((column, index) => {
      const next = asOptionalCounter(
        autoIncrementValues[index],
        `${AUTO_INCREMENT_PREFIX}${record.id}/${column.id}`,
      );
      return next === undefined ? [] : [{ columnId: column.id, next }];
    }),
    uniqueMemberships,
    fts,
  };
}

function snapshotTableRetainedItems(table: IndexedDbSnapshotTable): number {
  let count = 1 + table.autoIncrement.length + table.uniqueMemberships.length;
  count += table.fts.length;
  return count;
}

async function writeSnapshotMetadataFramesInTransaction(input: {
  transaction: IDBTransaction;
  manifest: Manifest;
  version: number;
  direction: "export" | "import";
  identity: string;
  observeRetainedItems: (count: number) => void;
  observeRetainedBytes: (bytes: number) => void;
}): Promise<{
  summaries: Record<
    SnapshotFrameKind,
    { frameCount: number; itemCount: number; storedBytes: number }
  >;
  frameCount: number;
}> {
  const catalog = input.transaction.objectStore("catalog");
  const frameStore = input.transaction.objectStore(SNAPSHOT_HEADER_STORE);
  const summaries = emptySnapshotKindSummaries();
  let sequence = 0;
  const writeItem = (kind: Exclude<SnapshotFrameKind, "block">, item: unknown): void => {
    const payload = encodeSnapshotMetadataPage([item]);
    if (payload.byteLength > MAX_SNAPSHOT_METADATA_FRAME_BYTES) {
      throw new RangeError(`${kind} item is too large for one snapshot frame`);
    }
    const decoded = decodeSnapshotMetadataItems(kind, payload);
    if (decoded.length !== 1) throw new Error("Snapshot metadata codec lost an item");
    const frame: SnapshotFrame = {
      sequence,
      kind,
      itemCount: 1,
      key: null,
      payload,
      checksum: crc32(payload),
    };
    frameStore.add(frame, snapshotFrameKey(input.direction, input.identity, sequence));
    const summary = summaries[kind];
    summary.frameCount = incrementSafeInteger(summary.frameCount, "Snapshot frame count");
    summary.itemCount = incrementSafeInteger(summary.itemCount, "Snapshot item count");
    summary.storedBytes = safeByteSum(
      summary.storedBytes,
      payload.byteLength,
      "Snapshot stored bytes",
    );
    sequence = incrementSafeInteger(sequence, "Snapshot frame sequence");
  };
  const visitTables = async (
    visit: (table: IndexedDbSnapshotTable) => void | Promise<void>,
  ): Promise<void> => {
    await visitObjectStoreSequentially(catalog, async (value, key) => {
      if (typeof key !== "string" || !key.startsWith(TABLE_ID_PREFIX)) return;
      const table = await readSnapshotTableMetadata(
        catalog,
        asTableRecord(value, key),
        input.version,
        input.observeRetainedBytes,
      );
      input.observeRetainedItems(snapshotTableRetainedItems(table));
      await visit(table);
    });
  };

  await visitTables(async (table) => {
    writeItem("catalog-page", {
      kind: "table",
      record: table.record,
      nextRowId: table.nextRowId,
      autoIncrement: [...table.autoIncrement].sort((left, right) =>
        left.columnId.localeCompare(right.columnId),
      ),
    } satisfies SnapshotCatalogItem);
  });
  await visitObjectStoreSequentially(input.transaction.objectStore("segments"), async (value) => {
    const record = asSegmentRecord(value);
    const ownerValue: unknown = await requestResult(
      input.transaction.objectStore("transactions").get(record.transactionId),
    );
    if (ownerValue === undefined) return;
    const owner = asTransactionRecord(ownerValue, record.transactionId);
    if (
      owner.status !== "committed" ||
      owner.committedVersion === null ||
      owner.committedVersion > input.version
    ) {
      return;
    }
    for (const blockId of segmentBlockIds(record)) {
      const provenanceValue: unknown = await requestResult(catalog.get(manifestBlockKey(blockId)));
      if (
        provenanceValue === undefined ||
        !manifestBlockVisibleAt(asManifestBlockRecord(provenanceValue, blockId), input.version)
      ) {
        return;
      }
    }
    input.observeRetainedItems(1);
    writeItem("segment-page", { kind: "segment", record } satisfies SnapshotSegmentItem);
  });
  await visitObjectStoreSequentially(
    input.transaction.objectStore("transactions"),
    (value, key) => {
      if (typeof key !== "string") throw corruption("transactions", "record key is invalid");
      const record = asTransactionRecord(value, key);
      if (
        record.status !== "committed" ||
        record.committedVersion === null ||
        record.committedVersion > input.version
      ) {
        return;
      }
      input.observeRetainedItems(1);
      writeItem("transaction-page", {
        kind: "transaction",
        record: { ...record, pendingBlockIds: [], pendingSegmentIds: [] },
      } satisfies SnapshotTransactionItem);
    },
  );
  await visitTables(async (table) => {
    const memberships = [...table.uniqueMemberships].sort((left, right) =>
      left.namespaceId.localeCompare(right.namespaceId),
    );
    for (const membership of memberships) {
      writeItem("unique-page", {
        kind: "unique-generation",
        tableId: table.record.id,
        indexId: membership.indexId,
        namespaceId: membership.namespaceId,
        generationId: membership.generationId,
        chunkCount: membership.chunkCount,
        tokenCount: membership.tokenCount,
      } satisfies SnapshotUniqueItem);
      let ordinal = 0;
      let keyTokens: string[] = [];
      let retainedBytes = 0;
      const flush = (): void => {
        if (keyTokens.length === 0) return;
        writeItem("unique-page", {
          kind: "unique-chunk",
          namespaceId: membership.namespaceId,
          generationId: membership.generationId,
          ordinal,
          keyTokens,
        } satisfies SnapshotUniqueItem);
        ordinal = incrementSafeInteger(ordinal, "Snapshot UNIQUE ordinal");
        keyTokens = [];
        retainedBytes = 0;
      };
      await visitCanonicalUniqueKeyTokens(
        catalog,
        membership.namespaceId,
        (token, retainedSourceBytes) => {
          const tokenBytes = uniqueMembershipTokenRetainedBytes(token);
          if (
            keyTokens.length === UNIQUE_KEY_MEMBERSHIP_PART_TOKENS ||
            retainedBytes > UNIQUE_KEY_MEMBERSHIP_PART_RETAINED_BYTES - tokenBytes
          ) {
            flush();
          }
          keyTokens.push(token);
          retainedBytes += tokenBytes;
          input.observeRetainedBytes(
            safeByteSum(retainedSourceBytes, retainedBytes, "Snapshot UNIQUE retained bytes"),
          );
        },
      );
      flush();
      if (ordinal !== membership.chunkCount) {
        throw corruption(membership.namespaceId, "UNIQUE snapshot plan changed during export");
      }
    }
  });
  await visitTables(async (table) => {
    const generations = [...table.fts].sort((left, right) =>
      left.columnId.localeCompare(right.columnId),
    );
    for (const planned of generations) {
      const generation = await readSnapshotPostingGeneration(
        catalog,
        table.record.id,
        planned.columnId,
        input.version,
        input.observeRetainedBytes,
      );
      if (
        generation?.generationId !== planned.generationId ||
        generation.totalTokens !== planned.totalTokens ||
        generation.chunks.length !== planned.chunkCount
      ) {
        throw corruption(planned.columnId, "posting snapshot plan changed during export");
      }
      const secondary = Object.entries(table.record.secondaryIndexes ?? {}).find(
        ([, index]) => index.storageColumnId === planned.columnId,
      );
      const ownerKind = secondary === undefined ? "fts-column" : "secondary-index";
      const ownerId = secondary?.[0] ?? planned.columnId;
      const generationId = generation.generationId;
      writeItem("posting-page", {
        kind: "posting-generation",
        tableId: table.record.id,
        ownerKind,
        ownerId,
        storageColumnId: planned.columnId,
        generationId,
        coversVersion: planned.coversVersion,
        chunkCount: generation.chunks.length,
        totalTokens: generation.totalTokens,
      } satisfies SnapshotPostingItem);
      generation.chunks.forEach((postings, ordinal) => {
        writeItem("posting-page", {
          kind: "posting-chunk",
          storageColumnId: planned.columnId,
          generationId,
          ordinal,
          postings,
        } satisfies SnapshotPostingItem);
      });
    }
  });
  void input.manifest;
  return { summaries, frameCount: sequence };
}

function blockMetadataKey(id: string): string {
  return `${BLOCK_METADATA_PREFIX}${id}`;
}

function asStoredBlockMetadata(value: unknown, id: string): StoredBlockMetadata {
  const location = blockMetadataKey(id);
  if (!isRecord(value)) throw corruption(location, "block metadata is missing or invalid");
  assertKnownFields(value, ["byteLength", "checksum"], location);
  if (
    !isBoundedCursor(value.byteLength, MAX_BLOCK_READ_BATCH_BYTES) ||
    value.byteLength === 0 ||
    !isUint32(value.checksum)
  ) {
    throw corruption(location, "block metadata is invalid");
  }
  return { byteLength: value.byteLength, checksum: value.checksum };
}

function sameBytes(left: Uint8Array, right: Uint8Array): boolean {
  if (left.byteLength !== right.byteLength) return false;
  for (let index = 0; index < left.byteLength; index += 1) {
    if (left[index] !== right[index]) return false;
  }
  return true;
}

function sameStructuredValue(left: unknown, right: unknown): boolean {
  if (Object.is(left, right)) return true;
  if (typeof left !== "object" || left === null || typeof right !== "object" || right === null) {
    return false;
  }
  if (Array.isArray(left) || Array.isArray(right)) {
    return (
      Array.isArray(left) &&
      Array.isArray(right) &&
      left.length === right.length &&
      left.every((value, index) => sameStructuredValue(value, right[index]))
    );
  }
  const leftRecord = left as Record<string, unknown>;
  const rightRecord = right as Record<string, unknown>;
  const leftKeys = Object.keys(leftRecord).sort();
  const rightKeys = Object.keys(rightRecord).sort();
  return (
    leftKeys.length === rightKeys.length &&
    leftKeys.every(
      (key, index) =>
        key === rightKeys[index] && sameStructuredValue(leftRecord[key], rightRecord[key]),
    )
  );
}

function isUint32(value: unknown): value is number {
  return Number.isSafeInteger(value) && (value as number) >= 0 && (value as number) <= 0xffffffff;
}

function isBoundedCursor(value: unknown, maximum: number): value is number {
  return Number.isSafeInteger(value) && (value as number) >= 0 && (value as number) <= maximum;
}

async function anyObjectStoreKeyExists(
  store: IDBObjectStore,
  ids: readonly string[],
): Promise<boolean> {
  for (const id of ids) {
    if ((await requestResult(store.getKey(id))) !== undefined) return true;
  }
  return false;
}

/** Returns the bounded public view of one canonical stored manifest summary. */
function manifestView(record: StoredManifestRecord): Manifest {
  return structuredClone(record);
}

/** Cold-path materialization used only by maintenance and integrity operations. */
async function resolveManifestBlockSetInTransaction(
  catalog: IDBObjectStore,
  version: number | null,
): Promise<Set<string>> {
  const blockIds = new Set<string>();
  if (version === null) return blockIds;
  await visitManifestBlockRecords(catalog, (record) => {
    if (manifestBlockVisibleAt(record, version)) blockIds.add(record.blockId);
    return undefined;
  });
  return blockIds;
}

function visitManifestBlockRecords(
  catalog: IDBObjectStore,
  visit: (record: ManifestBlockRecord) => boolean | undefined | Promise<boolean> | Promise<void>,
): Promise<void> {
  return new Promise((resolve, reject) => {
    const request = catalog.openCursor();
    let sought = false;
    request.onerror = () => reject(request.error ?? new Error("IndexedDB cursor failed"));
    request.onsuccess = () => {
      const cursor = request.result;
      if (cursor === null) {
        resolve();
        return;
      }
      try {
        if (!sought) {
          sought = true;
          if (compareStructuredKind(cursor.key, MANIFEST_BLOCK) < 0) {
            cursor.continue([MANIFEST_BLOCK]);
            return;
          }
        }
        if (compareStructuredKind(cursor.key, MANIFEST_BLOCK) !== 0) {
          resolve();
          return;
        }
        const key = cursor.key;
        if (!Array.isArray(key) || key.length !== 2 || typeof key[1] !== "string") {
          throw corruption(MANIFEST_BLOCK, "record key is invalid");
        }
        const record = asManifestBlockRecord(cursor.value, key[1]);
        Promise.resolve(visit(record)).then((result) => {
          if (result === true) resolve();
          else cursor.continue();
        }, reject);
      } catch (error) {
        reject(error instanceof Error ? error : new Error(String(error)));
      }
    };
  });
}

async function resolveManifestInTransaction(record: StoredManifestRecord): Promise<Manifest> {
  return manifestView(record);
}

function validateManifestMembershipInput(version: number | null, ids: readonly string[]): void {
  if (version !== null && (!Number.isSafeInteger(version) || version < 0)) {
    throw new RangeError("Manifest version must be null or a non-negative safe integer");
  }
  if (ids.length > MAX_MANIFEST_BLOCK_PRESENCE_IDS) {
    throw new RangeError(
      `Manifest block membership exceeds ${String(MAX_MANIFEST_BLOCK_PRESENCE_IDS)} IDs`,
    );
  }
  for (const id of ids) validateId(id, "Manifest block ID");
}

async function manifestBlockMembershipInTransaction(
  catalog: IDBObjectStore,
  version: number | null,
  ids: readonly string[],
): Promise<boolean[]> {
  if (version === null) return ids.map(() => false);
  const values = await Promise.all(
    ids.map((id) => requestResult<unknown>(catalog.get(manifestBlockKey(id)))),
  );
  return values.map((value, index) => {
    if (value === undefined) return false;
    return manifestBlockVisibleAt(asManifestBlockRecord(value, ids[index]), version);
  });
}

async function findReadableManifestBlock(
  transaction: IDBTransaction,
  ids: readonly string[],
  excludedVersions: ReadonlySet<number> = new Set(),
): Promise<{ blockId: string; version: number } | undefined> {
  if (ids.length === 0) return undefined;
  const catalog = transaction.objectStore("catalog");
  const values = await Promise.all(
    ids.map((id) => requestResult<unknown>(catalog.get(manifestBlockKey(id)))),
  );
  const records = values.flatMap((value, index) =>
    value === undefined ? [] : [asManifestBlockRecord(value, ids[index])],
  );
  if (records.length === 0) return undefined;
  let found: { blockId: string; version: number } | undefined;
  await visitObjectStoreSequentially(transaction.objectStore("manifests"), (value, key) => {
    if (typeof key !== "number" || !Number.isSafeInteger(key) || key < 0) {
      throw corruption("manifests", "record key is invalid");
    }
    const manifest = asStoredManifestRecord(value, key);
    if (manifest.prunedAt !== undefined || excludedVersions.has(manifest.version)) return;
    const record = records.find((candidate) => manifestBlockVisibleAt(candidate, manifest.version));
    if (record === undefined) return;
    found = { blockId: record.blockId, version: manifest.version };
    return true;
  });
  return found;
}

function asTransactionRecord(value: unknown, expectedId?: string): TransactionRecord {
  if (!isRecord(value)) throw corruption("transactions", "record is not an object");
  assertKnownFields(
    value,
    [
      "id",
      "ownerId",
      "expiresAt",
      "snapshotVersion",
      "pendingBlockIds",
      "pendingSegmentIds",
      "status",
      "revision",
      "startedAt",
      "updatedAt",
      "committedVersion",
      "schemaEpochGuard",
      "pendingTable",
      "pendingTableNextRowId",
      "catalogEpochGuard",
    ],
    "transactions",
  );
  const id = nonEmptyStoredString(value.id, "transactions/id");
  if (expectedId !== undefined && id !== expectedId) {
    throw corruption(`transactions/${expectedId}`, `record declares id ${id}`);
  }
  const snapshotVersion = asNullableStoredVersion(
    value.snapshotVersion,
    `transactions/${id}/snapshotVersion`,
  );
  const committedVersion = asNullableStoredVersion(
    value.committedVersion,
    `transactions/${id}/committedVersion`,
  );
  const pendingBlockIds = requiredUniqueStringArray(
    value.pendingBlockIds,
    `transactions/${id}/pendingBlockIds`,
  );
  const pendingSegmentIds = requiredUniqueStringArray(
    value.pendingSegmentIds,
    `transactions/${id}/pendingSegmentIds`,
  );
  if (!(["active", "committed", "aborted"] as unknown[]).includes(value.status)) {
    throw corruption(`transactions/${id}/status`, "status is invalid");
  }
  const status = value.status as TransactionRecord["status"];
  const revision = nonNegativeStoredInteger(value.revision, `transactions/${id}/revision`);
  const ownerId = nonEmptyStoredString(value.ownerId, `transactions/${id}/ownerId`);
  const expiresAt = validStoredTimestamp(value.expiresAt, `transactions/${id}/expiresAt`);
  const startedAt = validStoredTimestamp(value.startedAt, `transactions/${id}/startedAt`);
  const updatedAt = validStoredTimestamp(value.updatedAt, `transactions/${id}/updatedAt`);
  if (Date.parse(updatedAt) < Date.parse(startedAt)) {
    throw corruption(`transactions/${id}`, "update precedes creation");
  }
  if (
    (status === "committed" && committedVersion === null) ||
    (status !== "committed" && committedVersion !== null)
  ) {
    throw corruption(`transactions/${id}`, "status fields are inconsistent");
  }
  const pendingTable =
    value.pendingTable === undefined
      ? undefined
      : asTableRecord(value.pendingTable, `transactions/${id}/pendingTable`);
  const pendingTableNextRowId =
    value.pendingTableNextRowId === undefined
      ? undefined
      : asOptionalCounter(value.pendingTableNextRowId, `${ROW_ID_PREFIX}${pendingTable?.id ?? id}`);
  const catalogEpochGuard =
    value.catalogEpochGuard === undefined
      ? undefined
      : nonNegativeStoredInteger(value.catalogEpochGuard, `transactions/${id}/catalogEpochGuard`);
  const schemaEpochGuard =
    value.schemaEpochGuard === undefined
      ? undefined
      : nonNegativeStoredInteger(value.schemaEpochGuard, `transactions/${id}/schemaEpochGuard`);
  if (
    (status === "active" && schemaEpochGuard === undefined) ||
    (status !== "active" && schemaEpochGuard !== undefined)
  ) {
    throw corruption(`transactions/${id}`, "schema epoch guard is inconsistent with status");
  }
  if (
    (pendingTable === undefined
      ? pendingTableNextRowId !== undefined || catalogEpochGuard !== undefined
      : pendingTableNextRowId === undefined || catalogEpochGuard === undefined) ||
    (pendingTable !== undefined && status !== "active")
  ) {
    throw corruption(`transactions/${id}`, "pending-table fields are inconsistent");
  }
  const base = {
    id,
    ownerId,
    expiresAt,
    snapshotVersion,
    pendingBlockIds,
    pendingSegmentIds,
    status,
    revision,
    startedAt,
    updatedAt,
    committedVersion,
    ...(schemaEpochGuard === undefined ? {} : { schemaEpochGuard }),
  };
  if (pendingTable === undefined) return base;
  if (pendingTableNextRowId === undefined || catalogEpochGuard === undefined) {
    throw corruption(`transactions/${id}`, "pending-table fields are incomplete");
  }
  return { ...base, pendingTable, pendingTableNextRowId, catalogEpochGuard };
}

function asTableRecord(value: unknown, location = "catalog/table"): TableRecord {
  if (!isRecord(value)) throw corruption(location, "table record is not an object");
  assertKnownFields(
    value,
    [
      "id",
      "name",
      "columns",
      "uniqueKeyColumnId",
      "primaryKeyColumnIds",
      "uniqueKeyLookupReady",
      "ftsColumns",
      "secondaryIndexes",
      "triggers",
      "foreignKeys",
      "checks",
      "managed",
      "view",
      "enumType",
      "sequence",
      "createdAt",
      "revision",
    ],
    location,
  );
  const record = structuredClone(value) as unknown as TableRecord;
  const id = nonEmptyStoredString(value.id, `${location}/id`);
  storedCatalogName(value.name, `${location}/name`);
  if (!Array.isArray(value.columns)) throw corruption(location, "columns are invalid");
  validStoredTimestamp(value.createdAt, `${location}/createdAt`);
  nonNegativeStoredInteger(value.revision, `${location}/revision`);
  for (const [index, column] of value.columns.entries()) {
    if (isRecord(column)) {
      assertKnownFields(
        column,
        [
          "id",
          "name",
          "type",
          "integer",
          "sqlDomain",
          "nullable",
          "defaultValue",
          "backfill",
          "enumValues",
          "hidden",
        ],
        `${location}/columns/${String(index)}`,
      );
      if (isRecord(column.sqlDomain)) {
        const domainFields =
          column.sqlDomain.kind === "numeric"
            ? ["kind", "precision", "scale"]
            : column.sqlDomain.kind === "array"
              ? ["kind", "element"]
              : column.sqlDomain.kind === "enum"
                ? ["kind", "name", "values"]
                : ["kind"];
        assertKnownFields(
          column.sqlDomain,
          domainFields,
          `${location}/columns/${String(index)}/sqlDomain`,
        );
      }
      if (isRecord(column.defaultValue)) {
        const defaultFields =
          column.defaultValue.kind === "literal"
            ? ["kind", "value"]
            : column.defaultValue.kind === "expression"
              ? ["kind", "sql"]
              : ["kind"];
        assertKnownFields(
          column.defaultValue,
          defaultFields,
          `${location}/columns/${String(index)}/defaultValue`,
        );
      }
    }
    if (
      !isRecord(column) ||
      !isStorageId(column.id) ||
      !isCatalogName(column.name) ||
      !["boolean", "number", "string", "datetime"].includes(String(column.type)) ||
      typeof column.nullable !== "boolean" ||
      (column.integer !== undefined && column.integer !== true) ||
      (column.hidden !== undefined && column.hidden !== true) ||
      (column.enumValues !== undefined && !isStringArray(column.enumValues))
    ) {
      throw corruption(`${location}/columns/${String(index)}`, "column metadata is invalid");
    }
  }
  try {
    validateTableColumns(record.columns);
    validateSecondaryIndexes(record);
  } catch (error) {
    throw corruption(
      location,
      error instanceof Error ? error.message : "table metadata is invalid",
    );
  }
  const columnIds = new Set(record.columns.map((column) => column.id));
  const columnNames = new Set(record.columns.map((column) => column.name));
  for (const column of record.columns) {
    const context = {
      name: column.name,
      type: column.type,
      ...(column.integer === undefined ? {} : { integer: column.integer }),
      ...(column.sqlDomain === undefined ? {} : { sqlDomain: column.sqlDomain }),
      nullable: column.nullable,
      isUniqueKey: record.uniqueKeyColumnId === column.id,
      ...(column.enumValues === undefined ? {} : { enumValues: column.enumValues }),
    };
    try {
      if (column.enumValues !== undefined) validateEnumValues(column.enumValues, column.name);
      if (column.defaultValue !== undefined) validateColumnDefault(context, column.defaultValue);
      if (column.backfill !== undefined) {
        validateColumnDefault(context, { kind: "literal", value: column.backfill });
      }
    } catch (error) {
      throw corruption(
        location,
        error instanceof Error ? error.message : `column metadata is invalid: ${column.name}`,
      );
    }
  }
  if (
    (record.uniqueKeyColumnId !== undefined &&
      (!isStorageId(record.uniqueKeyColumnId) || !columnIds.has(record.uniqueKeyColumnId))) ||
    (record.uniqueKeyLookupReady !== undefined &&
      typeof record.uniqueKeyLookupReady !== "boolean") ||
    (record.primaryKeyColumnIds !== undefined &&
      (!isStorageIdArray(record.primaryKeyColumnIds) ||
        record.primaryKeyColumnIds.length === 0 ||
        new Set(record.primaryKeyColumnIds).size !== record.primaryKeyColumnIds.length ||
        record.primaryKeyColumnIds.some((columnId) => !columnIds.has(columnId))))
  ) {
    throw corruption(location, "primary/unique key metadata is invalid");
  }
  const rawFtsColumns = value.ftsColumns;
  if (rawFtsColumns !== undefined) {
    if (!isRecord(rawFtsColumns)) throw corruption(location, "FTS catalog is invalid");
    for (const [columnId, state] of Object.entries(rawFtsColumns)) {
      const column = record.columns.find((candidate) => candidate.id === columnId);
      if (isRecord(state)) {
        assertKnownFields(
          state,
          ["storage", "tokenizerVersion", "state", "buildFromVersion"],
          `${location}/ftsColumns/${columnId}`,
        );
      }
      if (
        !isStorageId(columnId) ||
        column?.type !== "string" ||
        !isRecord(state) ||
        state.storage !== "fts-chunks-v1" ||
        typeof state.tokenizerVersion !== "number" ||
        !Number.isSafeInteger(state.tokenizerVersion) ||
        state.tokenizerVersion < 0 ||
        !["building", "ready", "invalid"].includes(String(state.state)) ||
        typeof state.buildFromVersion !== "number" ||
        !Number.isSafeInteger(state.buildFromVersion) ||
        state.buildFromVersion < -1
      ) {
        throw corruption(location, `FTS state is invalid: ${columnId}`);
      }
    }
  }
  const rawSecondaryIndexes = value.secondaryIndexes;
  if (rawSecondaryIndexes !== undefined) {
    if (!isRecord(rawSecondaryIndexes)) {
      throw corruption(location, "secondary-index catalog is invalid");
    }
    for (const [indexId, index] of Object.entries(rawSecondaryIndexes)) {
      if (isRecord(index)) {
        assertKnownFields(
          index,
          [
            "name",
            "columnId",
            "columnIds",
            "directions",
            "unique",
            "uniqueEnforced",
            "termEncoding",
            "storage",
            "storageColumnId",
            "locator",
            "state",
            "buildId",
            "buildFromVersion",
          ],
          `${location}/secondaryIndexes/${indexId}`,
        );
      }
      if (
        !isStorageId(indexId) ||
        !isRecord(index) ||
        !isCatalogName(index.name) ||
        !isStorageId(index.columnId) ||
        !isStorageId(index.storageColumnId) ||
        (index.columnIds !== undefined && !isStorageIdArray(index.columnIds)) ||
        (index.directions !== undefined &&
          (!Array.isArray(index.directions) ||
            index.directions.some((direction) => direction !== "asc" && direction !== "desc"))) ||
        (index.unique !== undefined && index.unique !== true) ||
        (index.uniqueEnforced !== undefined && index.uniqueEnforced !== true) ||
        (index.buildId !== undefined && !isStorageId(index.buildId))
      ) {
        throw corruption(location, `secondary-index state is invalid: ${indexId}`);
      }
    }
  }
  if (record.view !== undefined) {
    if (isRecord(record.view))
      assertKnownFields(record.view, ["sql", "managed"], `${location}/view`);
    if (
      !isRecord(record.view) ||
      typeof record.view.sql !== "string" ||
      record.view.sql.length === 0 ||
      typeof record.view.managed !== "boolean"
    ) {
      throw corruption(location, "view metadata is invalid");
    }
  }
  if (typeof record.managed !== "boolean") {
    throw corruption(location, "managed flag is invalid");
  }
  if (record.checks !== undefined) {
    if (
      !Array.isArray(record.checks) ||
      record.checks.some(
        (check) =>
          !isRecord(check) ||
          !isCatalogName(check.name) ||
          typeof check.sql !== "string" ||
          check.sql.length === 0,
      )
    ) {
      throw corruption(location, "CHECK metadata is invalid");
    }
    record.checks.forEach((check, index) => {
      if (isRecord(check))
        assertKnownFields(check, ["name", "sql"], `${location}/checks/${String(index)}`);
    });
  }
  const constraintNames = new Set<string>();
  const foreignKeyConstraints = Array.isArray(record.foreignKeys) ? record.foreignKeys : [];
  for (const constraint of [...foreignKeyConstraints, ...(record.checks ?? [])]) {
    if (!isRecord(constraint) || typeof constraint.name !== "string") continue;
    if (constraintNames.has(constraint.name)) {
      throw corruption(location, `constraint name is duplicated: ${constraint.name}`);
    }
    constraintNames.add(constraint.name);
  }
  const rawForeignKeys = value.foreignKeys;
  if (rawForeignKeys !== undefined) {
    if (!Array.isArray(rawForeignKeys)) {
      throw corruption(location, "foreign-key metadata is invalid");
    }
    for (const [index, foreignKey] of rawForeignKeys.entries()) {
      if (isRecord(foreignKey)) {
        assertKnownFields(
          foreignKey,
          ["name", "columns", "parentTable", "parentColumns", "onDelete"],
          `${location}/foreignKeys/${String(index)}`,
        );
      }
      if (
        !isRecord(foreignKey) ||
        !isCatalogName(foreignKey.name) ||
        !isCatalogNameArray(foreignKey.columns) ||
        foreignKey.columns.length === 0 ||
        foreignKey.columns.some((columnName) => !columnNames.has(columnName)) ||
        !isCatalogName(foreignKey.parentTable) ||
        !isCatalogNameArray(foreignKey.parentColumns) ||
        foreignKey.parentColumns.length === 0 ||
        foreignKey.columns.length !== foreignKey.parentColumns.length ||
        !["restrict", "cascade", "set null"].includes(String(foreignKey.onDelete)) ||
        foreignKey.parentColumns.some((columnName) => !isCatalogName(columnName))
      ) {
        throw corruption(
          `${location}/foreignKeys/${String(index)}`,
          "foreign-key metadata is invalid",
        );
      }
    }
  }
  const rawTriggers = value.triggers;
  if (rawTriggers !== undefined) {
    if (!Array.isArray(rawTriggers)) throw corruption(location, "trigger metadata is invalid");
    const triggerIds = new Set<string>();
    const triggerNames = new Set<string>();
    for (const [triggerIndex, trigger] of rawTriggers.entries()) {
      if (isRecord(trigger)) {
        assertKnownFields(
          trigger,
          ["id", "name", "event", "timing", "statements", "createdAt"],
          `${location}/triggers/${String(triggerIndex)}`,
        );
      }
      if (
        !isRecord(trigger) ||
        !isStorageId(trigger.id) ||
        triggerIds.has(trigger.id) ||
        !isCatalogName(trigger.name) ||
        triggerNames.has(trigger.name) ||
        !["insert", "update", "delete"].includes(String(trigger.event)) ||
        !["before", "after"].includes(String(trigger.timing)) ||
        typeof trigger.createdAt !== "string" ||
        !Number.isFinite(Date.parse(trigger.createdAt)) ||
        !Array.isArray(trigger.statements)
      ) {
        throw corruption(`${location}/triggers/${String(triggerIndex)}`, "trigger is invalid");
      }
      triggerIds.add(trigger.id);
      triggerNames.add(trigger.name);
      for (const [statementIndex, statement] of trigger.statements.entries()) {
        if (isRecord(statement)) {
          assertKnownFields(
            statement,
            ["sql", "bindings"],
            `${location}/triggers/${String(triggerIndex)}/statements/${String(statementIndex)}`,
          );
          if (Array.isArray(statement.bindings)) {
            statement.bindings.forEach((binding, bindingIndex) => {
              if (isRecord(binding)) {
                assertKnownFields(
                  binding,
                  ["source", "column"],
                  `${location}/triggers/${String(triggerIndex)}/statements/${String(statementIndex)}/bindings/${String(bindingIndex)}`,
                );
              }
            });
          }
        }
        if (
          !isRecord(statement) ||
          typeof statement.sql !== "string" ||
          statement.sql.length === 0 ||
          !Array.isArray(statement.bindings) ||
          statement.bindings.some(
            (binding) =>
              !isRecord(binding) ||
              (binding.source !== "new" && binding.source !== "old") ||
              !isCatalogName(binding.column) ||
              !columnNames.has(binding.column),
          )
        ) {
          throw corruption(
            `${location}/triggers/${String(triggerIndex)}/statements/${String(statementIndex)}`,
            "trigger statement is invalid",
          );
        }
      }
    }
  }
  if (
    [record.view, record.enumType, record.sequence].filter((entry) => entry !== undefined).length >
    1
  ) {
    throw corruption(location, "catalog object kinds are mutually exclusive");
  }
  if (record.enumType !== undefined) {
    if (isRecord(record.enumType)) {
      assertKnownFields(record.enumType, ["name", "values"], `${location}/enumType`);
    }
    if (
      !isRecord(record.enumType) ||
      !isCatalogName(record.enumType.name) ||
      !isStringArray(record.enumType.values) ||
      record.enumType.values.length === 0 ||
      new Set(record.enumType.values).size !== record.enumType.values.length
    ) {
      throw corruption(location, "enum metadata is invalid");
    }
  }
  if (record.sequence !== undefined) {
    if (isRecord(record.sequence)) {
      assertKnownFields(record.sequence, ["name", "start", "columnId"], `${location}/sequence`);
    }
    if (
      !isRecord(record.sequence) ||
      !isCatalogName(record.sequence.name) ||
      !Number.isSafeInteger(record.sequence.start) ||
      !isStorageId(record.sequence.columnId) ||
      !columnIds.has(record.sequence.columnId)
    ) {
      throw corruption(location, "sequence metadata is invalid");
    }
  }
  if (record.id !== id) throw corruption(location, "table id is invalid");
  return record;
}

function asIncomingTableRecord(value: unknown, location: string): TableRecord {
  try {
    return asTableRecord(value, location);
  } catch (error) {
    if (error instanceof StorageCorruptionError)
      throw new TypeError(error.message, { cause: error });
    throw error;
  }
}

function asSegmentRecord(value: unknown): SegmentRecord {
  if (!isRecord(value)) throw corruption("segments", "record is not an object");
  assertKnownFields(
    value,
    [
      "id",
      "tableId",
      "transactionId",
      "rowCount",
      "rowIdStart",
      "rowIdEndExclusive",
      "columnBlockIds",
      "kind",
      "keyColumnId",
      "level",
      "logicalOrder",
      "commitOrdinal",
      "rowIdSpans",
      "partitionOrdinal",
      "createdAt",
    ],
    "segments",
  );
  const id = nonEmptyStoredString(value.id, "segments/id");
  const location = `segments/${id}`;
  nonEmptyStoredString(value.tableId, `${location}/tableId`);
  nonEmptyStoredString(value.transactionId, `${location}/transactionId`);
  if (!Number.isSafeInteger(value.rowCount) || (value.rowCount as number) <= 0) {
    throw corruption(location, "row count is invalid");
  }
  if (typeof value.rowIdStart !== "bigint" || typeof value.rowIdEndExclusive !== "bigint") {
    throw corruption(location, "row ID envelope is invalid");
  }
  if (value.rowIdStart > MAX_ROW_ID || value.rowIdEndExclusive > MAX_ROW_ID_EXCLUSIVE_END) {
    throw corruption(location, "row ID envelope exceeds uint64 storage");
  }
  if (!isRecord(value.columnBlockIds)) {
    throw corruption(location, "column block map is invalid");
  }
  for (const [columnId, blockIds] of Object.entries(value.columnBlockIds)) {
    if (
      !isStorageId(columnId) ||
      !isStringArray(blockIds) ||
      blockIds.length === 0 ||
      blockIds.some((blockId) => !isStorageId(blockId)) ||
      new Set(blockIds).size !== blockIds.length
    ) {
      throw corruption(location, `column block list is invalid: ${columnId}`);
    }
  }
  if (
    value.kind !== "insert" &&
    value.kind !== "upsert" &&
    value.kind !== "update" &&
    value.kind !== "delete" &&
    value.kind !== "base"
  ) {
    throw corruption(location, "kind is invalid");
  }
  if (value.keyColumnId !== undefined && !isStorageId(value.keyColumnId)) {
    throw corruption(location, "key column is invalid");
  }
  if (
    !Number.isSafeInteger(value.level) ||
    (value.level as number) < 0 ||
    (value.level as number) > 2
  ) {
    throw corruption(location, "level is invalid");
  }
  if (!Number.isSafeInteger(value.commitOrdinal) || (value.commitOrdinal as number) < 0) {
    throw corruption(location, "commitOrdinal is invalid");
  }
  if (
    value.partitionOrdinal !== undefined &&
    (!Number.isSafeInteger(value.partitionOrdinal) || (value.partitionOrdinal as number) < 0)
  ) {
    throw corruption(location, "partitionOrdinal is invalid");
  }
  if (
    typeof value.logicalOrder !== "number" ||
    !Number.isFinite(value.logicalOrder) ||
    value.logicalOrder < 0
  ) {
    throw corruption(location, "logical order is invalid");
  }
  validStoredTimestamp(value.createdAt, `${location}/createdAt`);
  const zeroKeyedEnvelope =
    value.rowIdStart === 0n &&
    value.rowIdEndExclusive === 0n &&
    (value.kind === "update" || value.kind === "delete") &&
    typeof value.keyColumnId === "string" &&
    value.keyColumnId.length > 0;
  if (!Array.isArray(value.rowIdSpans)) {
    throw corruption(location, "row ID spans are invalid");
  }
  if (zeroKeyedEnvelope && value.rowIdSpans.length > 0) {
    throw corruption(location, "keyed mutation cannot carry row ID spans");
  }
  if (value.rowIdSpans.length > 0) {
    let rows = 0;
    let expectedRowStart = 0;
    let minimumRowId: bigint | undefined;
    let maximumRowIdEnd = 0n;
    const intervals: Array<{ start: bigint; end: bigint }> = [];
    for (const span of value.rowIdSpans) {
      if (isRecord(span)) {
        assertKnownFields(span, ["rowStart", "rowCount", "rowIdStart"], `${location}/rowIdSpans`);
      }
      if (
        !isRecord(span) ||
        !Number.isSafeInteger(span.rowStart) ||
        span.rowStart !== expectedRowStart ||
        typeof span.rowIdStart !== "bigint" ||
        span.rowIdStart <= 0n ||
        span.rowIdStart > MAX_ROW_ID ||
        !Number.isSafeInteger(span.rowCount) ||
        (span.rowCount as number) <= 0
      ) {
        throw corruption(location, "row ID spans are invalid");
      }
      const end = span.rowIdStart + BigInt(span.rowCount as number);
      if (end > MAX_ROW_ID_EXCLUSIVE_END) {
        throw corruption(location, "row ID span exceeds uint64 storage");
      }
      intervals.push({ start: span.rowIdStart, end });
      minimumRowId =
        minimumRowId === undefined || span.rowIdStart < minimumRowId
          ? span.rowIdStart
          : minimumRowId;
      if (end > maximumRowIdEnd) maximumRowIdEnd = end;
      expectedRowStart += span.rowCount as number;
      rows += span.rowCount as number;
    }
    intervals.sort((left, right) =>
      left.start < right.start ? -1 : left.start > right.start ? 1 : 0,
    );
    if (
      intervals.some(
        (interval, index) => index > 0 && interval.start < (intervals[index - 1]?.end ?? 0n),
      )
    ) {
      throw corruption(location, "row ID spans overlap");
    }
    if (
      rows !== value.rowCount ||
      minimumRowId !== value.rowIdStart ||
      maximumRowIdEnd !== value.rowIdEndExclusive
    ) {
      throw corruption(location, "row ID spans do not match the segment envelope");
    }
  } else if (
    !zeroKeyedEnvelope &&
    (value.rowIdStart <= 0n ||
      value.rowIdEndExclusive !== value.rowIdStart + BigInt(value.rowCount as number))
  ) {
    throw corruption(location, "row ID envelope does not match row count");
  }
  try {
    return normalizeSegmentRecord(value as unknown as SegmentRecord);
  } catch (error) {
    throw corruption(
      "segments",
      error instanceof Error ? error.message : "segment record is invalid",
    );
  }
}

function asLeaseRecord(value: unknown, expectedId?: string): LeaseRecord {
  if (!isRecord(value)) throw corruption("leases", "record is not an object");
  assertKnownFields(
    value,
    ["id", "kind", "manifestVersion", "ownerId", "createdAt", "expiresAt", "revision"],
    "leases",
  );
  const id = nonEmptyStoredString(value.id, "leases/id");
  if (expectedId !== undefined && id !== expectedId) {
    throw corruption(`leases/${expectedId}`, `record declares id ${id}`);
  }
  if (value.kind !== "reader" && value.kind !== "backup") {
    throw corruption(`leases/${id}`, "kind is invalid");
  }
  return {
    id,
    kind: value.kind,
    manifestVersion: asNullableStoredVersion(value.manifestVersion, `leases/${id}/manifestVersion`),
    ownerId: nonEmptyStoredString(value.ownerId, `leases/${id}/ownerId`),
    createdAt: canonicalStoredTimestamp(value.createdAt, `leases/${id}/createdAt`),
    expiresAt: canonicalStoredTimestamp(value.expiresAt, `leases/${id}/expiresAt`),
    revision: nonNegativeStoredInteger(value.revision, `leases/${id}/revision`),
  };
}

/**
 * The transaction record a commit needs: present, active, and at the expected revision —
 * otherwise a `TransactionRecordConflictError`, the caller's storage transaction still open for
 * it to abort.
 */
async function readActiveTransactionRecord(
  transaction: IDBTransaction,
  id: string,
  expectedRevision: number,
): Promise<TransactionRecord> {
  const value: unknown = await requestResult<unknown>(
    transaction.objectStore("transactions").get(id),
  );
  const record = value === undefined ? undefined : asTransactionRecord(value);
  if (record?.revision !== expectedRevision || record.status !== "active") {
    throw new TransactionRecordConflictError(id, expectedRevision, record?.revision ?? null);
  }
  return record;
}

async function assertSnapshotAvailableInTransaction(
  transaction: IDBTransaction,
  version: number | null,
): Promise<void> {
  if (version === null) return;
  const manifestStore = transaction.objectStore("manifests");
  const value: unknown = await requestResult(manifestStore.get(version));
  if (value === undefined) throw new SnapshotManifestMissingError(version);
  const record = asStoredManifestRecord(value, version);
  if (record.prunedAt !== undefined) throw new SnapshotManifestMissingError(version);
}

async function assertCompactionJobReferences(
  transaction: IDBTransaction,
  job: CompactionJobRecord,
): Promise<void> {
  const catalog = transaction.objectStore("catalog");
  if ((await readDeclaredTable(catalog, job.tableId)) === undefined) {
    throw new Error(`Compaction job ${job.id} references missing table: ${job.tableId}`);
  }
  await assertSnapshotAvailableInTransaction(transaction, job.sourceManifestVersion);
  const manifestStore = transaction.objectStore("manifests");
  const sourceValue: unknown = await requestResult(manifestStore.get(job.sourceManifestVersion));
  if (sourceValue === undefined) throw new SnapshotManifestMissingError(job.sourceManifestVersion);
  const sourceBlocks = await resolveManifestBlockSetInTransaction(
    transaction.objectStore("catalog"),
    asStoredManifestRecord(sourceValue, job.sourceManifestVersion).version,
  );
  const declaredSourceBlocks = new Set(job.sourceBlockIds);
  const blockStore = transaction.objectStore("blocks");
  for (const blockId of job.sourceBlockIds) {
    if (!sourceBlocks.has(blockId)) {
      throw new Error(`Compaction job ${job.id} source block is not in its manifest: ${blockId}`);
    }
    if ((await requestResult(blockStore.getKey(blockId))) === undefined) {
      throw new Error(`Compaction job ${job.id} references missing source block: ${blockId}`);
    }
  }
  const segmentStore = transaction.objectStore("segments");
  for (const segmentId of job.sourceSegmentIds) {
    const segmentValue: unknown = await requestResult(segmentStore.get(segmentId));
    if (segmentValue === undefined) {
      throw new Error(`Compaction job ${job.id} references missing source segment: ${segmentId}`);
    }
    const segment = asSegmentRecord(segmentValue);
    if (segment.id !== segmentId || segment.tableId !== job.tableId) {
      throw new Error(
        `Compaction job ${job.id} source segment belongs to another table: ${segmentId}`,
      );
    }
    for (const blockId of segmentBlockIds(segment)) {
      if (!declaredSourceBlocks.has(blockId)) {
        throw new Error(
          `Compaction job ${job.id} source segment block is absent from its source list: ${blockId}`,
        );
      }
    }
  }
  for (const blockId of job.outputBlockIds) {
    if ((await requestResult(blockStore.getKey(blockId))) === undefined) {
      throw new Error(`Compaction job ${job.id} references missing output block: ${blockId}`);
    }
  }
  if (job.transactionId !== null) {
    const value: unknown = await requestResult(
      transaction.objectStore("transactions").get(job.transactionId),
    );
    if (value === undefined) {
      throw new Error(
        `Compaction job ${job.id} references missing transaction: ${job.transactionId}`,
      );
    }
    asTransactionRecord(value, job.transactionId);
  }
  if (job.state === "ready" || job.state === "published") {
    for (const outputId of compactionOutputSegmentIds(job)) {
      const value: unknown = await requestResult(segmentStore.get(outputId));
      if (value === undefined) {
        throw new Error(`Compaction job ${job.id} references missing output segment: ${outputId}`);
      }
      const output = asSegmentRecord(value);
      if (output.tableId !== job.tableId || output.transactionId !== job.transactionId) {
        throw new Error(`Compaction job ${job.id} output segment ownership is invalid`);
      }
    }
  }
  if (job.publishedVersion !== null) {
    await assertSnapshotAvailableInTransaction(transaction, job.publishedVersion);
  }
}

async function assertCompactionRetirementInTransaction(
  transaction: IDBTransaction,
  committingTransaction: TransactionRecord,
  input: Omit<CommitTransactionInput, "transactionId" | "expectedTransactionRevision">,
  removedBlockIds: readonly string[],
): Promise<void> {
  const jobId = input.compactionJobId;
  if (jobId === undefined || jobId.length === 0) {
    throw new TypeError("Block retirement requires a compaction job");
  }
  const maintenanceStore = transaction.objectStore("gc");
  const key = compactionJobKey(jobId);
  const value: unknown = await requestResult(maintenanceStore.get(key));
  if (value === undefined) throw new Error(`Compaction job does not exist: ${jobId}`);
  const job = asCompactionJobEnvelope(value);
  if (job.id !== jobId) throw corruption(`gc/${key}`, `record declares id ${job.id}`);
  if (job.state !== "ready") {
    throw new Error(`Compaction job ${jobId} is not ready`);
  }
  if (job.transactionId !== committingTransaction.id) {
    throw new Error(`Compaction job ${jobId} belongs to another transaction`);
  }
  if ((await readDeclaredTable(transaction.objectStore("catalog"), job.tableId)) === undefined) {
    throw new Error(`Compaction job ${jobId} references missing table: ${job.tableId}`);
  }

  const retired = new Set(removedBlockIds);
  const declaredSourceBlocks = new Set(job.sourceBlockIds);
  if (
    declaredSourceBlocks.size !== job.sourceBlockIds.length ||
    declaredSourceBlocks.size !== retired.size ||
    [...declaredSourceBlocks].some((id) => !retired.has(id))
  ) {
    throw new Error(`Compaction job ${jobId} source blocks do not match the retirement`);
  }
  const declaredSourceSegments = new Set(job.sourceSegmentIds);
  if (declaredSourceSegments.size !== job.sourceSegmentIds.length) {
    throw corruption(`gc/${key}`, "source segment IDs contain duplicates");
  }

  const segmentStore = transaction.objectStore("segments");
  const sourceReferenceOwner = new Map<string, string>();
  for (const segmentId of job.sourceSegmentIds) {
    const segmentValue: unknown = await requestResult(segmentStore.get(segmentId));
    if (segmentValue === undefined) {
      throw new Error(`Compaction job ${jobId} references missing source segment: ${segmentId}`);
    }
    const segment = asSegmentRecord(segmentValue);
    if (segment.id !== segmentId) {
      throw corruption(`segments/${segmentId}`, `record declares id ${segment.id}`);
    }
    if (segment.tableId !== job.tableId) {
      throw new Error(
        `Compaction job ${jobId} source segment belongs to another table: ${segmentId}`,
      );
    }
    for (const blockId of segmentBlockIds(segment)) {
      if (!declaredSourceBlocks.has(blockId)) {
        throw new Error(
          `Compaction job ${jobId} source segment references undeclared block: ${blockId}`,
        );
      }
      const previousOwner = sourceReferenceOwner.get(blockId);
      if (previousOwner !== undefined) {
        throw new Error(
          `Compaction job ${jobId} source block ${blockId} is aliased by segments ${previousOwner} and ${segmentId}`,
        );
      }
      sourceReferenceOwner.set(blockId, segmentId);
    }
  }
  const missingSourceReference = job.sourceBlockIds.find(
    (blockId) => !sourceReferenceOwner.has(blockId),
  );
  if (missingSourceReference !== undefined) {
    throw new Error(
      `Compaction job ${jobId} source block has no source segment: ${missingSourceReference}`,
    );
  }

  await visitObjectStoreSequentially(segmentStore, (segmentValue, segmentKey) => {
    if (typeof segmentKey !== "string") throw corruption("segments", "record key is invalid");
    const segment = asSegmentRecord(segmentValue);
    if (segment.id !== segmentKey) {
      throw corruption(`segments/${segmentKey}`, `record declares id ${segment.id}`);
    }
    if (declaredSourceSegments.has(segment.id)) return;
    const alias = segmentBlockIds(segment).find((blockId) => retired.has(blockId));
    if (alias !== undefined) {
      throw new Error(
        `Compaction job ${jobId} cannot retire block ${alias}; segment ${segment.id} also references it`,
      );
    }
  });

  const unjournaledOutput = compactionOutputSegmentIds(job).find(
    (id) => !committingTransaction.pendingSegmentIds.includes(id),
  );
  if (unjournaledOutput !== undefined) {
    throw new Error(
      `Compaction job ${jobId} output segment is not journaled by its transaction: ${unjournaledOutput}`,
    );
  }
  const journaledBlocks = new Set(committingTransaction.pendingBlockIds);
  const missingOutputBlock = job.outputBlockIds.find((blockId) => !journaledBlocks.has(blockId));
  if (missingOutputBlock !== undefined) {
    throw new Error(
      `Compaction job ${jobId} output block is not journaled by its transaction: ${missingOutputBlock}`,
    );
  }
}

async function assertCompactionOutputSegmentsInTransaction(
  transaction: IDBTransaction,
  committingTransaction: TransactionRecord,
  compactionJobId: string | undefined,
  pendingSegments: readonly SegmentRecord[],
): Promise<void> {
  if (compactionJobId === undefined) {
    const nonLevelZero = pendingSegments.find((segment) => segment.level > 0);
    if (nonLevelZero !== undefined) {
      throw new Error("Non-level-zero segments require a ready compaction job");
    }
    return;
  }
  const key = compactionJobKey(compactionJobId);
  const value: unknown = await requestResult(transaction.objectStore("gc").get(key));
  if (value === undefined) throw new Error(`Compaction job does not exist: ${compactionJobId}`);
  const job = asCompactionJobEnvelope(value);
  if (job.id !== compactionJobId) throw corruption(`gc/${key}`, `record declares id ${job.id}`);
  if (job.state !== "ready") throw new Error(`Compaction job ${compactionJobId} is not ready`);

  const table = await readDeclaredTable(transaction.objectStore("catalog"), job.tableId);
  if (table === undefined) throw new Error(`Compaction job ${job.id} has no table`);
  const sourceSegments = await Promise.all(
    job.sourceSegmentIds.map(async (id) => {
      const source: unknown = await requestResult(transaction.objectStore("segments").get(id));
      if (source === undefined) throw new Error(`Compaction source segment is missing: ${id}`);
      return asSegmentRecord(source);
    }),
  );
  assertCompactionOutputProvenance(
    job,
    table,
    committingTransaction,
    sourceSegments,
    pendingSegments,
  );
}

async function assertLevelZeroSegmentLimits(
  transaction: IDBTransaction,
  limits: ReadonlyArray<{ tableId: string; limit: number }>,
  pendingSegments: readonly SegmentRecord[],
  currentBlockIds: ReadonlySet<string>,
  pendingTable?: TableRecord,
): Promise<void> {
  const pendingTables = new Set(
    pendingSegments.filter((segment) => segment.level === 0).map((segment) => segment.tableId),
  );
  if (limits.length !== pendingTables.size) {
    throw new TypeError("Level-zero segment limits must exactly cover pending level-zero tables");
  }
  const seenTables = new Set<string>();
  const catalog = transaction.objectStore("catalog");
  const segmentIndex = transaction.objectStore("segments").index(SEGMENT_TABLE_INDEX);
  for (const entry of limits) {
    if (entry.tableId.length === 0) throw new TypeError("Level-zero table ID cannot be empty");
    if (
      !Number.isSafeInteger(entry.limit) ||
      entry.limit <= 0 ||
      entry.limit > MAX_LEVEL_ZERO_SEGMENTS
    ) {
      throw new RangeError(
        `Level-zero segment limit must be between 1 and ${String(MAX_LEVEL_ZERO_SEGMENTS)}`,
      );
    }
    if (seenTables.has(entry.tableId)) {
      throw new TypeError(`Level-zero segment limit is duplicated: ${entry.tableId}`);
    }
    seenTables.add(entry.tableId);
    if (!pendingTables.has(entry.tableId)) {
      throw new TypeError(`Level-zero segment limit has no pending table: ${entry.tableId}`);
    }
    const table =
      (await readDeclaredTable(catalog, entry.tableId)) ??
      (pendingTable?.id === entry.tableId ? pendingTable : undefined);
    if (table === undefined) {
      throw new Error(`Level-zero segment limit references missing table: ${entry.tableId}`);
    }
    const existing = await countVisibleLevelZeroSegments(
      segmentIndex,
      transaction.objectStore("transactions"),
      entry.tableId,
      currentBlockIds,
    );
    const added = pendingSegments.reduce(
      (count, segment) =>
        count + (segment.tableId === entry.tableId && segment.level === 0 ? 1 : 0),
      0,
    );
    const count = existing + added;
    if (count > entry.limit) {
      throw new CompactionBacklogError(table.name, count, entry.limit);
    }
  }
}

function countVisibleLevelZeroSegments(
  index: IDBIndex,
  transactions: IDBObjectStore,
  tableId: string,
  currentBlockIds: ReadonlySet<string>,
): Promise<number> {
  return new Promise((resolve, reject) => {
    const statuses = new Map<string, TransactionRecord["status"]>();
    let count = 0;
    const request = index.openCursor(tableId);
    request.onerror = () => reject(request.error ?? new Error("IndexedDB segment cursor failed"));
    request.onsuccess = () => {
      const cursor = request.result;
      if (cursor === null) {
        resolve(count);
        return;
      }
      let segment: SegmentRecord;
      try {
        segment = asSegmentRecord(cursor.value);
        if (segment.id !== cursor.primaryKey || segment.tableId !== tableId) {
          throw corruption(`segments/${segment.id}`, "table index does not match its record");
        }
        if (segment.level !== 0) {
          cursor.continue();
          return;
        }
        const blockIds = segmentBlockIds(segment);
        const visibleBlockCount = blockIds.filter((id) => currentBlockIds.has(id)).length;
        if (visibleBlockCount > 0 && visibleBlockCount < blockIds.length) {
          throw corruption(`segments/${segment.id}`, "only part of the segment is manifest-live");
        }
        if (blockIds.length > 0 && visibleBlockCount === 0) {
          cursor.continue();
          return;
        }
      } catch (error) {
        reject(error instanceof Error ? error : new Error(String(error)));
        return;
      }
      const knownStatus = statuses.get(segment.transactionId);
      if (knownStatus !== undefined) {
        if (knownStatus === "committed") count += 1;
        cursor.continue();
        return;
      }
      const ownerRequest = transactions.get(segment.transactionId);
      ownerRequest.onerror = () =>
        reject(ownerRequest.error ?? new Error("IndexedDB transaction lookup failed"));
      ownerRequest.onsuccess = () => {
        try {
          if (ownerRequest.result === undefined) {
            throw corruption(
              `segments/${segment.id}`,
              `owning transaction ${segment.transactionId} is missing`,
            );
          }
          const owner = asTransactionRecord(ownerRequest.result, segment.transactionId);
          statuses.set(owner.id, owner.status);
          if (owner.status === "committed") count += 1;
          cursor.continue();
        } catch (error) {
          reject(error instanceof Error ? error : new Error(String(error)));
        }
      };
    };
  });
}

async function compactionJobRemovalPreservesProvenance(
  transaction: IDBTransaction,
  removedJob: CompactionJobRecord,
): Promise<boolean> {
  const rootedBlocks = new Set<string>();
  const rootedSegments = new Set<string>();
  const readableVersions: number[] = [];
  await visitObjectStoreSequentially(transaction.objectStore("manifests"), (value, key) => {
    if (typeof key !== "number" || !Number.isSafeInteger(key) || key < 0) {
      throw corruption("manifests", "record key is invalid");
    }
    const manifest = asStoredManifestRecord(value, key);
    if (manifest.prunedAt === undefined) readableVersions.push(manifest.version);
  });
  await visitManifestBlockRecords(transaction.objectStore("catalog"), (record) => {
    if (manifestBlockRecordOverlapsVersions(record, readableVersions)) {
      rootedBlocks.add(record.blockId);
    }
    return undefined;
  });
  await visitObjectStoreSequentially(transaction.objectStore("segments"), (value, key) => {
    if (typeof key !== "string") throw corruption("segments", "record key is invalid");
    const segment = asSegmentRecord(value);
    if (segment.id !== key) throw corruption(`segments/${key}`, `record declares id ${segment.id}`);
    for (const blockId of segmentBlockIds(segment)) rootedBlocks.add(blockId);
  });
  await visitObjectStoreSequentially(transaction.objectStore("transactions"), (value, key) => {
    if (typeof key !== "string") throw corruption("transactions", "record key is invalid");
    const record = asTransactionRecord(value, key);
    for (const blockId of record.pendingBlockIds) rootedBlocks.add(blockId);
    for (const segmentId of record.pendingSegmentIds) rootedSegments.add(segmentId);
  });
  await visitObjectStoreSequentially(transaction.objectStore("gc"), (value, key) => {
    if (typeof key === "string" && key.startsWith(GARBAGE_COLLECTION_JOB_KEY_PREFIX)) {
      const job = asGarbageCollectionJobEnvelope(value);
      for (const blockId of job.candidateBlockIds) rootedBlocks.add(blockId);
      for (const segmentId of job.candidateSegmentIds) rootedSegments.add(segmentId);
      return;
    }
    const job = asCompactionJobAtMaintenanceKey(value, key);
    if (job === undefined || job.id === removedJob.id) return;
    for (const blockId of [...job.sourceBlockIds, ...job.outputBlockIds]) {
      rootedBlocks.add(blockId);
    }
    for (const segmentId of [...job.sourceSegmentIds, ...compactionOutputSegmentIds(job)]) {
      rootedSegments.add(segmentId);
    }
  });
  const blockStore = transaction.objectStore("blocks");
  for (const blockId of [...removedJob.sourceBlockIds, ...removedJob.outputBlockIds]) {
    if (
      (await requestResult(blockStore.getKey(blockId))) !== undefined &&
      !rootedBlocks.has(blockId)
    ) {
      return false;
    }
  }
  const segmentStore = transaction.objectStore("segments");
  for (const segmentId of [
    ...removedJob.sourceSegmentIds,
    ...compactionOutputSegmentIds(removedJob),
  ]) {
    if (
      (await requestResult(segmentStore.getKey(segmentId))) !== undefined &&
      !rootedSegments.has(segmentId)
    ) {
      return false;
    }
  }
  return true;
}

async function assertGarbageCollectionPinsAvailableInTransaction(
  transaction: IDBTransaction,
  currentVersion: number | null,
  leaseCutoff: number,
): Promise<void> {
  await assertSnapshotAvailableInTransaction(transaction, currentVersion);
  await visitObjectStoreSequentially(transaction.objectStore("transactions"), async (value) => {
    const record = asTransactionRecord(value);
    if (record.status === "active") {
      await assertSnapshotAvailableInTransaction(transaction, record.snapshotVersion);
    }
  });
  await visitObjectStoreSequentially(transaction.objectStore("leases"), async (value) => {
    const lease = asLeaseRecord(value);
    const expiresAt = Date.parse(lease.expiresAt);
    if (!Number.isFinite(expiresAt) || expiresAt > leaseCutoff) {
      await assertSnapshotAvailableInTransaction(transaction, lease.manifestVersion);
    }
  });
  await visitObjectStoreSequentially(transaction.objectStore("gc"), async (value, key) => {
    const job = asCompactionJobAtMaintenanceKey(value, key);
    if (job === undefined) return;
    if (isTerminalCompactionJob(job)) return;
    await assertSnapshotAvailableInTransaction(transaction, job.sourceManifestVersion);
    if (job.transactionId === null) return;
    const transactionValue: unknown = await requestResult(
      transaction.objectStore("transactions").get(job.transactionId),
    );
    if (transactionValue === undefined) return;
    const linkedTransaction = asTransactionRecord(transactionValue);
    if (linkedTransaction.status !== "committed") return;
    if (linkedTransaction.committedVersion === null) {
      throw new Error(`Committed transaction has no manifest version: ${linkedTransaction.id}`);
    }
    await assertSnapshotAvailableInTransaction(transaction, linkedTransaction.committedVersion);
  });
}

async function assertPendingArtifactsAvailableInTransaction(
  transaction: IDBTransaction,
  record: TransactionRecord,
  validateBlocks = true,
  validateSegments = true,
  validateCurrentTable = false,
): Promise<void> {
  const [blockKeys, segmentValues] = await Promise.all([
    validateBlocks
      ? Promise.all(
          record.pendingBlockIds.map((id) =>
            requestResult(transaction.objectStore("blocks").getKey(id)),
          ),
        )
      : Promise.resolve([]),
    validateSegments
      ? Promise.all(
          record.pendingSegmentIds.map((id) =>
            requestResult<unknown>(transaction.objectStore("segments").get(id)),
          ),
        )
      : Promise.resolve([]),
  ]);
  const missingBlockIndex = blockKeys.findIndex((key) => key === undefined);
  if (missingBlockIndex >= 0) {
    throw new Error(
      `Transaction references missing pending block: ${record.pendingBlockIds[missingBlockIndex] ?? ""}`,
    );
  }
  const missingSegmentIndex = segmentValues.findIndex((value) => value === undefined);
  if (missingSegmentIndex >= 0) {
    throw new Error(
      `Transaction references missing pending segment: ${record.pendingSegmentIds[missingSegmentIndex] ?? ""}`,
    );
  }
  if (validateSegments) {
    const pendingBlocks = new Set(record.pendingBlockIds);
    const segments: SegmentRecord[] = [];
    for (const [index, value] of segmentValues.entries()) {
      const expectedId = record.pendingSegmentIds[index] ?? "";
      const segment = asSegmentRecord(value);
      if (segment.id !== expectedId || segment.transactionId !== record.id) {
        throw corruption(
          `transactions/${record.id}`,
          `pending segment ${expectedId} has invalid ownership or identity`,
        );
      }
      if (segment.commitOrdinal !== index) {
        throw corruption(
          `transactions/${record.id}`,
          `pending segment ${expectedId} has commit ordinal ${String(segment.commitOrdinal)}; expected ${String(index)}`,
        );
      }
      segments.push(segment);
      const unjournaledBlock = segmentBlockIds(segment).find((id) => !pendingBlocks.has(id));
      if (unjournaledBlock !== undefined) {
        throw corruption(
          `transactions/${record.id}`,
          `pending segment ${expectedId} references unjournaled block ${unjournaledBlock}`,
        );
      }
      if (validateCurrentTable) {
        await assertSegmentTargetsCurrentTable(
          transaction.objectStore("catalog"),
          segment,
          record.pendingTable,
        );
      }
    }
    if (record.pendingTable !== undefined) {
      let nextRowId = 1n;
      for (const segment of segments) {
        if (
          segment.tableId !== record.pendingTable.id ||
          segment.level !== 0 ||
          segment.kind !== "insert" ||
          segment.rowIdStart !== nextRowId
        ) {
          throw corruption(`transactions/${record.id}`, "pending-table journal is not contiguous");
        }
        nextRowId = segment.rowIdEndExclusive;
      }
      if (record.pendingTableNextRowId !== nextRowId) {
        throw corruption(`transactions/${record.id}`, "pending row counter disagrees with journal");
      }
    }
  }
}

/**
 * After pruning, every remaining manifest must still be fully readable. One ascending pass
 * resolves the whole chain with a single running set (pruned records still contribute their
 * deltas), unions the blocks referenced by any remaining manifest, and verifies each referenced
 * block once instead of once per manifest.
 */
async function assertRemainingManifestRecordsAvailable(
  transaction: IDBTransaction,
  newlyPrunedVersions: ReadonlySet<number>,
): Promise<void> {
  const readableVersions: number[] = [];
  await visitObjectStoreSequentially(transaction.objectStore("manifests"), (value, key) => {
    if (typeof key !== "number") throw corruption("manifests", "record key is invalid");
    const record = asStoredManifestRecord(value, key);
    if (record.prunedAt !== undefined || newlyPrunedVersions.has(record.version)) return;
    readableVersions.push(record.version);
  });
  const referenced = new Map<string, number>();
  await visitManifestBlockRecords(transaction.objectStore("catalog"), (record) => {
    const version = firstOverlappingManifestVersion(record, readableVersions);
    if (version !== undefined) referenced.set(record.blockId, version);
    return undefined;
  });
  const blockStore = transaction.objectStore("blocks");
  for (const [id, version] of referenced) {
    if ((await requestResult(blockStore.getKey(id))) === undefined) {
      throw new SnapshotManifestMissingError(version);
    }
  }
}

function compactionJobKey(id: string): string {
  return `${COMPACTION_JOB_KEY_PREFIX}${id}`;
}

function garbageCollectionJobKey(id: string): string {
  return `${GARBAGE_COLLECTION_JOB_KEY_PREFIX}${id}`;
}

function activeCompactionKey(tableId: string): string {
  return `${ACTIVE_COMPACTION_KEY_PREFIX}${encodeURIComponent(tableId)}`;
}

function emptyMaintenanceQuota(): MaintenanceQuotaRecord {
  return {
    activeCompactionJobs: 0,
    terminalCompactionJobs: 0,
    activeGarbageCollectionJobs: 0,
    completedGarbageCollectionJobs: 0,
  };
}

function asMaintenanceQuota(value: unknown): MaintenanceQuotaRecord {
  const location = `gc/${MAINTENANCE_QUOTA_KEY}`;
  if (!isRecord(value))
    throw corruption(location, "maintenance quota record is missing or invalid");
  const fields = [
    "activeCompactionJobs",
    "terminalCompactionJobs",
    "activeGarbageCollectionJobs",
    "completedGarbageCollectionJobs",
  ] as const;
  assertKnownFields(value, fields, location);
  for (const field of fields) {
    if (!Number.isSafeInteger(value[field]) || (value[field] as number) < 0) {
      throw corruption(location, `maintenance quota ${field} is invalid`);
    }
  }
  return value as unknown as MaintenanceQuotaRecord;
}

async function readMaintenanceQuota(store: IDBObjectStore): Promise<MaintenanceQuotaRecord> {
  return asMaintenanceQuota(await requestResult<unknown>(store.get(MAINTENANCE_QUOTA_KEY)));
}

function checkedQuotaIncrement(value: number, label: string): number {
  return incrementSafeInteger(value, label);
}

function checkedQuotaDecrement(value: number, label: string): number {
  if (value <= 0) throw corruption(`gc/${MAINTENANCE_QUOTA_KEY}`, `${label} underflowed`);
  return value - 1;
}

function assertMaintenanceLimit(
  resource:
    | "compaction job"
    | "terminal compaction job"
    | "garbage collection job"
    | "completed garbage collection job",
  count: number,
  limit: number,
): void {
  if (count > limit) throw new StorageResourceLimitError(resource, count, limit);
}

function asActiveCompactionMarker(value: unknown, expectedTableId: string): ActiveCompactionMarker {
  const location = activeCompactionKey(expectedTableId);
  if (!isRecord(value)) throw corruption(`gc/${location}`, "active compaction marker is invalid");
  assertKnownFields(value, ["kind", "tableId", "jobId"], `gc/${location}`);
  if (
    value.kind !== "active-compaction" ||
    value.tableId !== expectedTableId ||
    typeof value.jobId !== "string" ||
    value.jobId.length === 0
  ) {
    throw corruption(`gc/${location}`, "active compaction marker is invalid");
  }
  return { kind: "active-compaction", tableId: expectedTableId, jobId: value.jobId };
}

function asActiveGarbageCollectionMarker(value: unknown): ActiveGarbageCollectionMarker {
  if (!isRecord(value)) {
    throw corruption(`gc/${ACTIVE_GARBAGE_COLLECTION_KEY}`, "active GC marker is invalid");
  }
  assertKnownFields(value, ["kind", "jobId"], `gc/${ACTIVE_GARBAGE_COLLECTION_KEY}`);
  if (
    value.kind !== "active-garbage-collection" ||
    typeof value.jobId !== "string" ||
    value.jobId.length === 0
  ) {
    throw corruption(`gc/${ACTIVE_GARBAGE_COLLECTION_KEY}`, "active GC marker is invalid");
  }
  return { kind: "active-garbage-collection", jobId: value.jobId };
}

async function assertActiveCompactionMarker(
  store: IDBObjectStore,
  record: CompactionJobRecord,
): Promise<void> {
  const marker = asActiveCompactionMarker(
    await requestResult<unknown>(store.get(activeCompactionKey(record.tableId))),
    record.tableId,
  );
  if (marker.jobId !== record.id) {
    throw corruption(
      `gc/${activeCompactionKey(record.tableId)}`,
      `marker names job ${marker.jobId} instead of ${record.id}`,
    );
  }
}

async function assertActiveGarbageCollectionMarker(
  store: IDBObjectStore,
  record: GarbageCollectionJobRecord,
): Promise<void> {
  const marker = asActiveGarbageCollectionMarker(
    await requestResult<unknown>(store.get(ACTIVE_GARBAGE_COLLECTION_KEY)),
  );
  if (marker.jobId !== record.id) {
    throw corruption(
      `gc/${ACTIVE_GARBAGE_COLLECTION_KEY}`,
      `marker names job ${marker.jobId} instead of ${record.id}`,
    );
  }
}

function compactionJobEnvelope(record: CompactionJobRecord): CompactionJobEnvelope {
  const envelope = { kind: "compaction-job", record: structuredClone(record) } as const;
  asCompactionJobEnvelope(envelope);
  return envelope;
}

function isCompactionJobEnvelope(value: unknown): value is CompactionJobEnvelope {
  return (
    typeof value === "object" &&
    value !== null &&
    Reflect.get(value, "kind") === "compaction-job" &&
    typeof Reflect.get(value, "record") === "object" &&
    Reflect.get(value, "record") !== null
  );
}

function assertKnownRecordArrayFields(
  value: unknown,
  fields: readonly string[],
  location: string,
  nested?: (record: Record<string, unknown>, location: string) => void,
): void {
  if (!Array.isArray(value)) return;
  value.forEach((candidate, index) => {
    if (!isRecord(candidate)) return;
    const candidateLocation = `${location}/${String(index)}`;
    assertKnownFields(candidate, fields, candidateLocation);
    nested?.(candidate, candidateLocation);
  });
}

function assertKnownCompactionJobNestedFields(
  record: Record<string, unknown>,
  location: string,
): void {
  if (isRecord(record.cursor)) {
    assertKnownFields(
      record.cursor,
      ["sourceSegmentIndex", "sourceBlockIndex"],
      `${location}/cursor`,
    );
  }
  if (isRecord(record.outputCursor)) {
    assertKnownFields(
      record.outputCursor,
      ["outputIndex", "columnIndex", "rowStart"],
      `${location}/outputCursor`,
    );
  }
  const plan = record.rewritePlan;
  if (!isRecord(plan)) return;
  const planLocation = `${location}/rewritePlan`;
  if (plan.kind === "copy-v1") {
    assertKnownFields(plan, ["kind"], planLocation);
    return;
  }
  const sourceBlockFields = [
    "blockId",
    "rowStart",
    "rowCount",
    "storedBytes",
    "encodedBytes",
    "checksum",
  ];
  const partitionFields = ["rowStart", "rowCount", "logicalOrder"];
  const outputFields = ["rowStart", "rowCount"];
  if (plan.kind === "rechunk-v1") {
    assertKnownFields(
      plan,
      [
        "kind",
        "targetBlockBytes",
        "outputCompression",
        "totalRows",
        "rowIdStart",
        "rowIdEndExclusive",
        "logicalOrder",
        "columns",
        "outputs",
        "partitions",
      ],
      planLocation,
    );
    assertKnownRecordArrayFields(
      plan.columns,
      ["columnId", "type", "sourceBlocks"],
      `${planLocation}/columns`,
      (column, columnLocation) =>
        assertKnownRecordArrayFields(
          column.sourceBlocks,
          sourceBlockFields,
          `${columnLocation}/sourceBlocks`,
        ),
    );
    assertKnownRecordArrayFields(plan.outputs, outputFields, `${planLocation}/outputs`);
    assertKnownRecordArrayFields(plan.partitions, partitionFields, `${planLocation}/partitions`);
    return;
  }
  if (plan.kind !== "merge-v1") return;
  assertKnownFields(
    plan,
    [
      "kind",
      "targetBlockBytes",
      "outputCompression",
      "keyColumnId",
      "totalRows",
      "rowIdStart",
      "rowIdEndExclusive",
      "rowIdSpans",
      "logicalOrder",
      "sourceSegments",
      "columns",
      "outputs",
      "partitions",
    ],
    planLocation,
  );
  assertKnownRecordArrayFields(
    plan.rowIdSpans,
    ["rowStart", "rowCount", "rowIdStart"],
    `${planLocation}/rowIdSpans`,
  );
  assertKnownRecordArrayFields(
    plan.sourceSegments,
    [
      "segmentId",
      "transactionId",
      "committedVersion",
      "kind",
      "keyColumnId",
      "level",
      "logicalOrder",
      "rowCount",
      "rowIdStart",
      "rowIdEndExclusive",
      "rowIdSpans",
      "columns",
    ],
    `${planLocation}/sourceSegments`,
    (segment, segmentLocation) => {
      assertKnownRecordArrayFields(
        segment.rowIdSpans,
        ["rowStart", "rowCount", "rowIdStart"],
        `${segmentLocation}/rowIdSpans`,
      );
      assertKnownRecordArrayFields(
        segment.columns,
        ["columnId", "type", "sourceBlocks"],
        `${segmentLocation}/columns`,
        (column, columnLocation) =>
          assertKnownRecordArrayFields(
            column.sourceBlocks,
            sourceBlockFields,
            `${columnLocation}/sourceBlocks`,
          ),
      );
    },
  );
  assertKnownRecordArrayFields(
    plan.columns,
    ["columnId", "type", "sourceRanges"],
    `${planLocation}/columns`,
    (column, columnLocation) =>
      assertKnownRecordArrayFields(
        column.sourceRanges,
        ["outputRowStart", "sourceBlockId", "sourceRowStart", "rowCount"],
        `${columnLocation}/sourceRanges`,
      ),
  );
  assertKnownRecordArrayFields(plan.outputs, outputFields, `${planLocation}/outputs`);
  assertKnownRecordArrayFields(plan.partitions, partitionFields, `${planLocation}/partitions`);
}

function asCompactionJobEnvelope(value: unknown): CompactionJobRecord {
  if (!isCompactionJobEnvelope(value)) throw corruption("gc", "compaction job is invalid");
  assertKnownFields(value, ["kind", "record"], "gc/compaction-envelope");
  assertKnownFields(
    value.record,
    [
      "id",
      "tableId",
      "sourceManifestVersion",
      "sourceSegmentIds",
      "sourceBlockIds",
      "outputBlockIds",
      "cursor",
      "processedRows",
      "sourceStoredBytes",
      "outputStoredBytes",
      "logicalBytes",
      "rewritePlan",
      "outputCursor",
      "memoryBudgetBytes",
      "minimumMemoryBytes",
      "level0SourceStoredBytes",
      "anchorSourceStoredBytes",
      "outputPartitionOrdinal",
      "maxWriteAmplification",
      "maximumOutputStoredBytes",
      "plannedOutputStoredBytesUpperBound",
      "priorAttemptOutputStoredBytes",
      "peakWorkingBytes",
      "outputLogicalBytes",
      "targetLevel",
      "state",
      "transactionId",
      "outputSegmentId",
      "publishedVersion",
      "revision",
      "createdAt",
      "updatedAt",
      "error",
    ],
    "gc/compaction-job",
  );
  try {
    const normalized = normalizeCompactionJobRecord(structuredClone(value.record));
    assertKnownCompactionJobNestedFields(
      value.record as unknown as Record<string, unknown>,
      `gc/compaction-job/${normalized.id}`,
    );
    return normalized;
  } catch (error) {
    throw corruption("gc", error instanceof Error ? error.message : "compaction job is invalid");
  }
}

function garbageCollectionJobEnvelope(
  record: GarbageCollectionJobRecord,
): GarbageCollectionJobEnvelope {
  const envelope = { kind: "garbage-collection-job", record: structuredClone(record) } as const;
  asGarbageCollectionJobEnvelope(envelope);
  return envelope;
}

function isGarbageCollectionJobEnvelope(value: unknown): value is GarbageCollectionJobEnvelope {
  return (
    typeof value === "object" &&
    value !== null &&
    Reflect.get(value, "kind") === "garbage-collection-job" &&
    typeof Reflect.get(value, "record") === "object" &&
    Reflect.get(value, "record") !== null
  );
}

function asGarbageCollectionJobEnvelope(value: unknown): GarbageCollectionJobRecord {
  if (!isGarbageCollectionJobEnvelope(value)) {
    throw corruption("gc", "garbage collection job is invalid");
  }
  assertKnownFields(value, ["kind", "record"], "gc/garbage-collection-envelope");
  assertKnownFields(
    value.record,
    [
      "id",
      "candidateManifestVersions",
      "candidateSegmentIds",
      "candidateBlockIds",
      "candidateTransactionIds",
      "cursor",
      "prunedManifestCount",
      "alreadyPrunedManifestCount",
      "retainedManifestCount",
      "missingManifestCount",
      "reclaimedSegmentCount",
      "retainedSegmentCount",
      "missingSegmentCount",
      "reclaimedBlockCount",
      "retainedBlockCount",
      "missingBlockCount",
      "reclaimedBlockBytes",
      "reclaimedTransactionCount",
      "retainedTransactionCount",
      "missingTransactionCount",
      "state",
      "revision",
      "leaseCutoff",
      "createdAt",
      "updatedAt",
      "discovery",
    ],
    "gc/garbage-collection-job",
  );
  if (isRecord(value.record.cursor)) {
    assertKnownFields(
      value.record.cursor,
      ["manifestIndex", "segmentIndex", "blockIndex", "transactionIndex"],
      "gc/garbage-collection-job/cursor",
    );
  }
  if (isRecord(value.record.discovery)) {
    assertKnownFields(
      value.record.discovery,
      [
        "phase",
        "currentManifestVersion",
        "retainAboveVersion",
        "retainAfter",
        "maxPlanningItems",
        "manifestCursor",
        "segmentCursor",
        "transactionCursor",
        "compactionCursor",
        "visitedRecords",
        "resumePhase",
        "artifactCursor",
        "postManifestPhase",
      ],
      "gc/garbage-collection-job/discovery",
    );
  }
  try {
    return normalizeGarbageCollectionJobRecord(structuredClone(value.record));
  } catch (error) {
    throw corruption(
      "gc",
      error instanceof Error ? error.message : "garbage collection job is invalid",
    );
  }
}

function asCompactionJobAtMaintenanceKey(
  value: unknown,
  key: IDBValidKey,
): CompactionJobRecord | undefined {
  if (typeof key !== "string") throw corruption("gc", "record key is invalid");
  if (key.startsWith(COMPACTION_JOB_KEY_PREFIX)) {
    const record = asCompactionJobEnvelope(value);
    if (key !== compactionJobKey(record.id)) {
      throw corruption(`gc/${key}`, `record declares id ${record.id}`);
    }
    return record;
  }
  if (key.startsWith(GARBAGE_COLLECTION_JOB_KEY_PREFIX)) {
    const record = asGarbageCollectionJobEnvelope(value);
    if (key !== garbageCollectionJobKey(record.id)) {
      throw corruption(`gc/${key}`, `record declares id ${record.id}`);
    }
    return undefined;
  }
  if (key.startsWith(ACTIVE_COMPACTION_KEY_PREFIX)) {
    let tableId: string;
    try {
      tableId = decodeURIComponent(key.slice(ACTIVE_COMPACTION_KEY_PREFIX.length));
    } catch {
      throw corruption(`gc/${key}`, "active compaction marker key is invalid");
    }
    if (tableId.length === 0 || activeCompactionKey(tableId) !== key) {
      throw corruption(`gc/${key}`, "active compaction marker key is invalid");
    }
    asActiveCompactionMarker(value, tableId);
    return undefined;
  }
  if (key === ACTIVE_GARBAGE_COLLECTION_KEY) {
    asActiveGarbageCollectionMarker(value);
    return undefined;
  }
  if (key === MAINTENANCE_QUOTA_KEY) {
    asMaintenanceQuota(value);
    return undefined;
  }
  throw corruption(`gc/${key}`, "job kind is unknown");
}

function isTerminalCompactionJob(record: CompactionJobRecord): boolean {
  return record.state === "published" || record.state === "cancelled" || record.state === "aborted";
}

function assertGenericTransactionUpdateAllowed(
  record: TransactionRecord,
  update: TransactionRecordUpdate,
): void {
  if (record.status !== "active") {
    throw new TypeError(`Only active transactions can be updated; found ${record.status}`);
  }
  if (update.status === "committed") {
    throw new TypeError("Use commitTransaction to commit a transaction");
  }
  if (Reflect.has(update, "committedVersion")) {
    throw new TypeError("Only commitTransaction can set a committed transaction version");
  }
}

async function assertGarbageCollectionCandidateProvenanceInTransaction(
  transaction: IDBTransaction,
  job: GarbageCollectionJobRecord,
): Promise<void> {
  const manifestStore = transaction.objectStore("manifests");
  for (const version of job.candidateManifestVersions) {
    const value: unknown = await requestResult(manifestStore.get(version));
    if (value === undefined) {
      throw new Error(`Garbage collection candidate manifest is missing: ${String(version)}`);
    }
  }
  for (const id of job.candidateTransactionIds) {
    const value: unknown = await requestResult(transaction.objectStore("transactions").get(id));
    const record = value === undefined ? undefined : asTransactionRecord(value);
    if (
      record === undefined ||
      (record.status !== "aborted" &&
        (record.status !== "committed" || record.committedVersion === null))
    ) {
      throw new Error(`Garbage collection transaction candidate is not terminal: ${id}`);
    }
  }
  const manifestProvenBlockIds = new Set<string>();
  const manifestValues = await Promise.all(
    job.candidateBlockIds.map((id) =>
      requestResult<unknown>(transaction.objectStore("catalog").get(manifestBlockKey(id))),
    ),
  );
  for (const [index, value] of manifestValues.entries()) {
    if (value !== undefined) {
      const id = job.candidateBlockIds[index] ?? "";
      asManifestBlockRecord(value, id);
      manifestProvenBlockIds.add(id);
    }
  }
  const blockHasProvenance = async (id: string): Promise<boolean> => {
    if (manifestProvenBlockIds.has(id)) return true;
    const transactionProven = await visitObjectStoreSequentially(
      transaction.objectStore("transactions"),
      (value) => {
        const record = asTransactionRecord(value);
        return record.status === "aborted" && record.pendingBlockIds.includes(id);
      },
    );
    if (transactionProven) return true;
    return visitObjectStoreSequentially(transaction.objectStore("gc"), (value, key) => {
      const record = asCompactionJobAtMaintenanceKey(value, key);
      if (record === undefined) return false;
      return (
        isTerminalCompactionJob(record) &&
        (record.sourceBlockIds.includes(id) || record.outputBlockIds.includes(id))
      );
    });
  };
  for (const id of job.candidateBlockIds) {
    if (await blockHasProvenance(id)) continue;
    throw new Error(`Garbage collection block candidate has no persisted provenance: ${id}`);
  }
  for (const id of job.candidateSegmentIds) {
    const segmentValue: unknown = await requestResult(transaction.objectStore("segments").get(id));
    if (segmentValue !== undefined) continue;
    throw new Error(`Garbage collection segment candidate has no persisted provenance: ${id}`);
  }
}

async function isManifestVersionPinnedInTransaction(
  transaction: IDBTransaction,
  version: number,
  currentVersion: number | null,
  leaseCutoff: number,
): Promise<boolean> {
  if (currentVersion === version) return true;
  const transactionPinned = await visitObjectStoreSequentially(
    transaction.objectStore("transactions"),
    (value) => {
      const record = asTransactionRecord(value);
      return record.status === "active" && record.snapshotVersion === version;
    },
  );
  if (transactionPinned) return true;
  const leasePinned = await visitObjectStoreSequentially(
    transaction.objectStore("leases"),
    (value) => {
      const lease = asLeaseRecord(value);
      const expiresAt = Date.parse(lease.expiresAt);
      return (
        lease.manifestVersion === version &&
        (!Number.isFinite(expiresAt) || expiresAt > leaseCutoff)
      );
    },
  );
  if (leasePinned) return true;
  return visitObjectStoreSequentially(transaction.objectStore("gc"), async (value, key) => {
    const job = asCompactionJobAtMaintenanceKey(value, key);
    if (job === undefined) return false;
    if (isTerminalCompactionJob(job)) return false;
    if (job.sourceManifestVersion === version) return true;
    if (job.transactionId === null) return false;
    const transactionValue: unknown = await requestResult(
      transaction.objectStore("transactions").get(job.transactionId),
    );
    if (transactionValue === undefined) return false;
    const linkedTransaction = asTransactionRecord(transactionValue);
    if (linkedTransaction.status !== "committed") return false;
    if (linkedTransaction.committedVersion === null) {
      throw new Error(`Committed transaction has no manifest version: ${linkedTransaction.id}`);
    }
    return linkedTransaction.committedVersion === version;
  });
}

async function collectBoundedPhysicalRootsInTransaction(
  transaction: IDBTransaction,
  candidateSegmentIds: readonly string[],
  candidateBlockIds: readonly string[],
  newlyPrunedVersions: ReadonlySet<number>,
): Promise<{ blockIds: Set<string>; segmentIds: Set<string> }> {
  const candidateSegments = new Set(candidateSegmentIds);
  const candidateBlocks = new Set(candidateBlockIds);
  const directSegmentRoots = new Set<string>();
  await visitObjectStoreSequentially(transaction.objectStore("transactions"), (value) => {
    const record = asTransactionRecord(value);
    if (record.status !== "active") return;
    for (const id of record.pendingSegmentIds)
      if (candidateSegments.has(id)) directSegmentRoots.add(id);
  });
  await visitObjectStoreSequentially(transaction.objectStore("gc"), (value, key) => {
    const job = asCompactionJobAtMaintenanceKey(value, key);
    if (job === undefined) return;
    if (isTerminalCompactionJob(job)) return;
    for (const id of job.sourceSegmentIds)
      if (candidateSegments.has(id)) directSegmentRoots.add(id);
    for (const outputId of compactionOutputSegmentIds(job)) {
      if (candidateSegments.has(outputId)) directSegmentRoots.add(outputId);
    }
  });

  // A dependency fan-out must never become a permanent root. Probe one id at a time after a
  // small bounded cache fills: this trades extra IndexedDB cursor work for fixed working memory
  // on unusually wide segments while preserving exact reachability.
  const directBlockRootCache = new Map<string, boolean>();
  const isDirectBlockRoot = async (id: string): Promise<boolean> => {
    const cached = directBlockRootCache.get(id);
    if (cached !== undefined) return cached;
    let rooted = await visitObjectStoreSequentially(
      transaction.objectStore("transactions"),
      (value) => {
        const record = asTransactionRecord(value);
        return record.status === "active" && record.pendingBlockIds.includes(id);
      },
    );
    if (!rooted) {
      rooted = await visitObjectStoreSequentially(transaction.objectStore("gc"), (value, key) => {
        const job = asCompactionJobAtMaintenanceKey(value, key);
        if (job === undefined) return false;
        return (
          !isTerminalCompactionJob(job) &&
          (job.sourceBlockIds.includes(id) || job.outputBlockIds.includes(id))
        );
      });
    }
    if (!rooted) {
      rooted =
        (await findReadableManifestBlock(transaction, [id], newlyPrunedVersions)) !== undefined;
    }
    if (directBlockRootCache.size < 4_096) directBlockRootCache.set(id, rooted);
    return rooted;
  };

  const rootedBlockIds = new Set<string>();
  const rootedSegmentIds = new Set<string>();
  const segmentStore = transaction.objectStore("segments");
  const transactionStore = transaction.objectStore("transactions");
  for (const id of candidateSegmentIds) {
    const value: unknown = await requestResult(segmentStore.get(id));
    if (value === undefined) continue;
    const segment = asSegmentRecord(value);
    const ids = segmentBlockIds(segment);
    const ownerValue: unknown = await requestResult(transactionStore.get(segment.transactionId));
    const owner = ownerValue === undefined ? undefined : asTransactionRecord(ownerValue);
    let allBlocksRooted = ids.length > 0;
    for (const blockId of ids) {
      if (await isDirectBlockRoot(blockId)) continue;
      allBlocksRooted = false;
      break;
    }
    if (directSegmentRoots.has(segment.id) || owner?.status === "active" || allBlocksRooted) {
      rootedSegmentIds.add(segment.id);
      for (const blockId of ids) if (candidateBlocks.has(blockId)) rootedBlockIds.add(blockId);
    }
  }
  for (const id of candidateBlockIds) if (await isDirectBlockRoot(id)) rootedBlockIds.add(id);
  for (const id of directSegmentRoots) rootedSegmentIds.add(id);
  return { blockIds: rootedBlockIds, segmentIds: rootedSegmentIds };
}

function segmentBlockIds(segment: SegmentRecord): string[] {
  return [...new Set(Object.values(segment.columnBlockIds).flat())];
}

function validateGarbageCollectionStepInput(input: RunGarbageCollectionStepInput): void {
  if (input.jobId.length === 0) throw new TypeError("Garbage collection job ID cannot be empty");
  if (!Number.isSafeInteger(input.expectedRevision) || input.expectedRevision < 0) {
    throw new RangeError(
      "Garbage collection expected revision must be a non-negative whole number",
    );
  }
  boundedMaintenanceBatchItems(input.maxItems, "Garbage collection item limit");
  if (input.updatedAt.length === 0 || !Number.isFinite(Date.parse(input.updatedAt))) {
    throw new TypeError("Garbage collection update timestamp must be valid");
  }
}

function validateCommitFtsChanges(changes: CommitTransactionInput["ftsChanges"]): void {
  const entries = changes ?? [];
  assertStorageBulkReadItems(entries, "Full-text commit table deltas");
  const tableIds = new Set<string>();
  for (const entry of entries) {
    validateId(entry.tableId, "Full-text delta table ID");
    if (tableIds.has(entry.tableId)) {
      throw new TypeError(`Full-text commit table is duplicated: ${entry.tableId}`);
    }
    tableIds.add(entry.tableId);
    assertStorageBulkReadItems(entry.columns, "Full-text commit column deltas");
    const columnIds = new Set<string>();
    for (const column of entry.columns) {
      validateId(column.columnId, "Full-text delta column ID");
      if (columnIds.has(column.columnId)) {
        throw new TypeError(`Full-text commit column is duplicated: ${column.columnId}`);
      }
      columnIds.add(column.columnId);
      const postings = decodeFtsPostingChunk(column.postings);
      if (
        postings === undefined ||
        !Number.isSafeInteger(column.totalTokens) ||
        column.totalTokens < 0 ||
        ftsPostingTokenCount(postings) !== column.totalTokens
      ) {
        throw new TypeError(`Full-text delta is invalid: ${entry.tableId}/${column.columnId}`);
      }
    }
  }
}

function validateUniqueKeyBuildIdentity(input: BeginUniqueKeyBuildInput): void {
  validateId(input.buildId, "UNIQUE build ID");
  validateId(input.tableId, "UNIQUE build table ID");
  validateId(input.indexId, "UNIQUE build index ID");
  validateId(input.namespaceId, "UNIQUE build namespace ID");
  validateId(input.ownerId, "UNIQUE build owner ID");
}

function validatePageLimit(limit: number): void {
  boundedMaintenanceBatchItems(limit, "Storage page limit");
}

function validateBoundedLeaseExpiration(
  cutoffAt: string,
  expiresAt: string,
  label: string,
): number {
  return validateBoundedExpiration(cutoffAt, expiresAt, label, MAX_LEASE_TTL_MS);
}

function canonicalInputTimestamp(value: string, label: string): string {
  const timestamp = Date.parse(value);
  if (!Number.isFinite(timestamp) || new Date(timestamp).toISOString() !== value) {
    throw new TypeError(`${label} must be canonical UTC ISO-8601`);
  }
  return value;
}

function validateBoundedExpiration(
  cutoffAt: string,
  expiresAt: string,
  label: string,
  maximumTtlMs: number,
): number {
  const cutoff = Date.parse(cutoffAt);
  const expiry = Date.parse(expiresAt);
  if (!Number.isFinite(cutoff) || !Number.isFinite(expiry)) {
    throw new TypeError(`${label} timestamps must be valid`);
  }
  if (new Date(cutoff).toISOString() !== cutoffAt || new Date(expiry).toISOString() !== expiresAt) {
    throw new TypeError(`${label} timestamps must be canonical UTC ISO-8601`);
  }
  if (expiry <= cutoff) throw new RangeError(`${label} expiration must be after its cutoff`);
  if (expiry - cutoff > maximumTtlMs) {
    throw new RangeError(`${label} expiration exceeds the maximum TTL`);
  }
  return cutoff;
}

function validateNewTransactionLifetime(record: TransactionRecord): void {
  validateBoundedLeaseExpiration(record.startedAt, record.expiresAt, "Transaction");
}

function emptyGarbageCollectionStep(job: GarbageCollectionJobRecord): GarbageCollectionStepResult {
  return {
    job: structuredClone(job),
    prunedManifestVersions: [],
    alreadyPrunedManifestVersions: [],
    retainedManifestVersions: [],
    missingManifestVersions: [],
    reclaimedSegmentIds: [],
    retainedSegmentIds: [],
    missingSegmentIds: [],
    reclaimedBlockIds: [],
    retainedBlockIds: [],
    missingBlockIds: [],
    reclaimedBlockBytes: 0,
    reclaimedTransactionIds: [],
    retainedTransactionIds: [],
    missingTransactionIds: [],
  };
}

function safeStorageSum(left: number, right: number): number {
  return safeByteSum(left, right, "Garbage collection reclaimed block bytes");
}

function safeByteSum(left: number, right: number, context: string): number {
  const total = left + right;
  if (!Number.isSafeInteger(total)) throw new RangeError(`${context} exceed the safe range`);
  return total;
}

function incrementSafeInteger(value: number, label: string): number {
  if (!Number.isSafeInteger(value) || value >= Number.MAX_SAFE_INTEGER) {
    throw new RangeError(`${label} cannot advance beyond the safe integer range`);
  }
  return value + 1;
}

function updateTransactionRecord(
  record: TransactionRecord,
  update: TransactionRecordUpdate,
): TransactionRecord {
  incrementSafeInteger(record.revision, "Transaction revision");
  return updateTransactionRecordUnchecked(record, update);
}

function updateCompactionJobRecord(
  record: CompactionJobRecord,
  update: CompactionJobRecordUpdate,
): CompactionJobRecord {
  incrementSafeInteger(record.revision, "Compaction job revision");
  return updateCompactionJobRecordUnchecked(record, update);
}

function advanceGarbageCollectionJobRecord(
  record: GarbageCollectionJobRecord,
  accounting: Parameters<typeof advanceGarbageCollectionJobRecordUnchecked>[1],
): GarbageCollectionJobRecord {
  incrementSafeInteger(record.revision, "Garbage collection job revision");
  return advanceGarbageCollectionJobRecordUnchecked(record, accounting);
}

function abortIfActive(transaction: IDBTransaction): void {
  try {
    transaction.abort();
  } catch {
    // An already completed or aborted transaction needs no further action.
  }
}

function validateCount(count: number): void {
  if (!Number.isSafeInteger(count) || count <= 0) {
    throw new RangeError("Row ID reservation count must be a positive whole number");
  }
}

interface FtsBaseToc {
  coversVersion: number;
  boundaries: Array<{ first: string; last: string }>;
  totalTokens: number;
  generation: string;
}

interface FtsBaseBuildMarker {
  buildId: string;
  ownerId: string;
  ownerKind: "fts-column" | "secondary-index";
  createdAt: string;
  expiresAt: string;
  ftsBuildExpiry: string;
  boundaries: Array<{ first: string; last: string }>;
  totalTokens: number;
  retainedBytes: number;
  retainedEntries: number;
  secondaryCompatible: boolean;
  updatedAt: string;
  cleanupIndex: number;
}

interface FtsRetirementMarker {
  kind: "fts-retirement";
  retirementUpdatedAt: string;
  generations: Array<{ generation: string; chunkCount: number; cleanupIndex: number }>;
  deltaVersions: number[];
  deltaCleanupIndex: number;
}

interface FtsDeltaChunk {
  postings: FtsPosting[];
  totalTokens: number;
}

/**
 * Accelerator decoders are intentionally non-throwing: an invalid record makes the whole index
 * incomplete, which the public read reports as hasBase=false so SQL scans authoritative rows.
 * Catalog/manifests/uniqueness use the opposite policy and throw typed corruption errors.
 */
function decodeFtsBaseToc(value: unknown): FtsBaseToc | undefined {
  if (value === undefined) return undefined;
  if (!isRecord(value)) return undefined;
  if (
    !hasOnlyKnownFields(value, ["coversVersion", "totalTokens", "boundaries", "generation"]) ||
    !Number.isSafeInteger(value.coversVersion) ||
    (value.coversVersion as number) < -1 ||
    !isBoundedCursor(value.totalTokens, Number.MAX_SAFE_INTEGER) ||
    !isStorageId(value.generation) ||
    !Array.isArray(value.boundaries) ||
    value.boundaries.length > MAX_FTS_BASE_CHUNKS
  ) {
    return undefined;
  }
  const boundaries: Array<{ first: string; last: string }> = [];
  for (const candidate of value.boundaries) {
    if (
      !isRecord(candidate) ||
      !hasOnlyKnownFields(candidate, ["first", "last"]) ||
      typeof candidate.first !== "string" ||
      candidate.first.length === 0 ||
      candidate.first.length > MAX_FTS_POSTING_TERM_CHARACTERS ||
      typeof candidate.last !== "string" ||
      candidate.last.length === 0 ||
      candidate.last.length > MAX_FTS_POSTING_TERM_CHARACTERS ||
      candidate.first > candidate.last
    ) {
      return undefined;
    }
    boundaries.push({ first: candidate.first, last: candidate.last });
  }
  return {
    coversVersion: value.coversVersion as number,
    totalTokens: value.totalTokens,
    boundaries,
    generation: value.generation,
  };
}

function decodeFtsDeltaIndex(value: unknown): { versions: number[] } | undefined {
  if (value === undefined) return undefined;
  if (
    !isRecord(value) ||
    !hasOnlyKnownFields(value, ["versions"]) ||
    !Array.isArray(value.versions) ||
    value.versions.length > MAX_FTS_DELTA_CHUNKS
  ) {
    return undefined;
  }
  const versions = value.versions;
  if (
    !versions.every((entry) => Number.isSafeInteger(entry) && entry >= 0) ||
    new Set(versions).size !== versions.length ||
    versions.some((entry, index) => index > 0 && entry <= (versions[index - 1] ?? -1))
  ) {
    return undefined;
  }
  return { versions: [...(versions as number[])] };
}

function decodeFtsPostingChunk(value: unknown): FtsPosting[] | undefined {
  if (!Array.isArray(value) || value.length > MAX_FTS_POSTINGS_PER_CHUNK) return undefined;
  const postings: FtsPosting[] = [];
  let previousTerm: string | undefined;
  let retainedRowIds = 0;
  for (const candidate of value) {
    if (!isRecord(candidate) || !hasOnlyKnownFields(candidate, ["term", "rowIds", "tf"])) {
      return undefined;
    }
    const rowIds = candidate.rowIds;
    const termFrequencies = candidate.tf;
    if (
      typeof candidate.term !== "string" ||
      candidate.term.length === 0 ||
      candidate.term.length > MAX_FTS_POSTING_TERM_CHARACTERS ||
      (previousTerm !== undefined && candidate.term <= previousTerm) ||
      !Array.isArray(rowIds) ||
      !rowIds.every((rowId) => typeof rowId === "bigint" && rowId > 0n && rowId <= MAX_ROW_ID) ||
      !Array.isArray(termFrequencies) ||
      !termFrequencies.every(
        (tf) => Number.isSafeInteger(tf) && tf > 0 && tf <= MAX_FTS_TOKENS_PER_DOCUMENT,
      ) ||
      rowIds.length !== termFrequencies.length ||
      rowIds.some((rowId, index) => index > 0 && rowId <= (rowIds[index - 1] as bigint))
    ) {
      return undefined;
    }
    retainedRowIds += rowIds.length;
    if (retainedRowIds > MAX_FTS_POSTING_ROW_IDS_PER_CHUNK) return undefined;
    postings.push({
      term: candidate.term,
      rowIds: [...(rowIds as bigint[])],
      tf: [...(termFrequencies as number[])],
    });
    previousTerm = candidate.term;
  }
  return postings;
}

/**
 * Conservative peak-retention model for an ordered read: the decoded chunk, the merge map, and
 * the returned arrays can briefly coexist. Candidate reads do not retain chunks and use their
 * exact distinct-row ceiling instead.
 */
function ftsPostingChunkRetainedBounds(postings: readonly FtsPosting[]): {
  rowIds: number;
  bytes: number;
} {
  let rowIds = 0;
  let bytes = 0;
  for (const posting of postings) {
    rowIds = safeByteSum(rowIds, posting.rowIds.length, "Full-text retained row IDs");
    bytes = safeByteSum(
      bytes,
      posting.term.length * 4 + posting.rowIds.length * 64 + 64,
      "Full-text retained bytes",
    );
  }
  return { rowIds, bytes };
}

function ftsPostingTokenCount(postings: readonly FtsPosting[]): number {
  let total = 0;
  for (const posting of postings) {
    for (const frequency of posting.tf) {
      total = safeByteSum(total, frequency, "Full-text token count");
    }
  }
  return total;
}

function asOptionalFtsBaseToc(value: unknown, location: string): FtsBaseToc | undefined {
  if (value === undefined) return undefined;
  const decoded = decodeFtsBaseToc(value);
  if (decoded === undefined) throw corruption(location, "postings table of contents is invalid");
  return decoded;
}

function asOptionalFtsDeltaIndex(
  value: unknown,
  location: string,
): { versions: number[] } | undefined {
  if (value === undefined) return undefined;
  const decoded = decodeFtsDeltaIndex(value);
  if (decoded === undefined) throw corruption(location, "postings delta index is invalid");
  return decoded;
}

function decodeFtsBaseBuildMarker(value: unknown): FtsBaseBuildMarker | undefined {
  if (
    !isRecord(value) ||
    !hasOnlyKnownFields(value, [
      "buildId",
      "ownerId",
      "ownerKind",
      "createdAt",
      "expiresAt",
      "ftsBuildExpiry",
      "boundaries",
      "totalTokens",
      "retainedBytes",
      "retainedEntries",
      "secondaryCompatible",
      "updatedAt",
      "cleanupIndex",
    ]) ||
    !isStorageId(value.buildId) ||
    !isStorageId(value.ownerId) ||
    (value.ownerKind !== "fts-column" && value.ownerKind !== "secondary-index") ||
    typeof value.createdAt !== "string" ||
    typeof value.expiresAt !== "string" ||
    value.ftsBuildExpiry !== value.expiresAt ||
    !isBoundedCursor(value.totalTokens, Number.MAX_SAFE_INTEGER) ||
    !isBoundedCursor(value.retainedBytes, MAX_ACCELERATOR_BUILD_STAGED_BYTES_TOTAL) ||
    !isBoundedCursor(value.retainedEntries, MAX_ACCELERATOR_BUILD_STAGED_ENTRIES_TOTAL) ||
    typeof value.secondaryCompatible !== "boolean" ||
    typeof value.updatedAt !== "string" ||
    !isBoundedCursor(value.cleanupIndex, Number.MAX_SAFE_INTEGER)
  ) {
    return undefined;
  }
  const decoded = decodeFtsBaseToc({
    coversVersion: 0,
    totalTokens: 0,
    boundaries: value.boundaries,
    generation: value.buildId,
  });
  if (decoded === undefined || value.cleanupIndex > decoded.boundaries.length) return undefined;
  const updatedAt = Date.parse(value.updatedAt);
  const createdAt = Date.parse(value.createdAt);
  const expiresAt = Date.parse(value.expiresAt);
  if (
    !Number.isFinite(updatedAt) ||
    new Date(updatedAt).toISOString() !== value.updatedAt ||
    !Number.isFinite(createdAt) ||
    new Date(createdAt).toISOString() !== value.createdAt ||
    !Number.isFinite(expiresAt) ||
    new Date(expiresAt).toISOString() !== value.expiresAt ||
    expiresAt <= createdAt
  ) {
    return undefined;
  }
  return {
    buildId: value.buildId,
    ownerId: value.ownerId,
    ownerKind: value.ownerKind,
    createdAt: value.createdAt,
    expiresAt: value.expiresAt,
    ftsBuildExpiry: value.expiresAt,
    boundaries: decoded.boundaries,
    totalTokens: value.totalTokens,
    retainedBytes: value.retainedBytes,
    retainedEntries: value.retainedEntries,
    secondaryCompatible: value.secondaryCompatible,
    updatedAt: value.updatedAt,
    cleanupIndex: value.cleanupIndex,
  };
}

function asOptionalFtsBaseBuildMarker(
  value: unknown,
  location: string,
): FtsBaseBuildMarker | undefined {
  if (value === undefined) return undefined;
  const decoded = decodeFtsBaseBuildMarker(value);
  if (decoded === undefined) throw corruption(location, "postings build marker is invalid");
  return decoded;
}

function validatePostingBuildOwnerInput(input: RenewPostingBuildInput): void {
  validateId(input.tableId, "Table ID");
  validateId(input.columnId, "Column ID");
  validateId(input.buildId, "Postings build ID");
  validateId(input.ownerId, "Postings build owner ID");
  validateBoundedExpiration(
    input.expiresAtCutoff,
    input.expiresAt,
    "Postings build renewal",
    MAX_POSTING_BUILD_TTL_MS,
  );
  canonicalInputTimestamp(input.updatedAt, "Postings build update time");
}

function assertLivePostingBuildOwner(
  marker: FtsBaseBuildMarker | undefined,
  input: RenewPostingBuildInput,
): asserts marker is FtsBaseBuildMarker {
  if (
    marker?.buildId !== input.buildId ||
    marker.ownerId !== input.ownerId ||
    Date.parse(marker.expiresAt) <= Date.parse(input.expiresAtCutoff)
  ) {
    throw new Error(`Postings base build ownership is absent or expired: ${input.buildId}`);
  }
}

function decodeFtsRetirementMarker(value: unknown): FtsRetirementMarker | undefined {
  if (
    !isRecord(value) ||
    !hasOnlyKnownFields(value, [
      "kind",
      "retirementUpdatedAt",
      "generations",
      "deltaVersions",
      "deltaCleanupIndex",
    ]) ||
    value.kind !== "fts-retirement" ||
    typeof value.retirementUpdatedAt !== "string" ||
    !Array.isArray(value.generations) ||
    value.generations.length > 3 ||
    !Array.isArray(value.deltaVersions) ||
    value.deltaVersions.length > MAX_FTS_DELTA_CHUNKS ||
    !isBoundedCursor(value.deltaCleanupIndex, value.deltaVersions.length)
  ) {
    return undefined;
  }
  const updatedAt = Date.parse(value.retirementUpdatedAt);
  if (
    !Number.isFinite(updatedAt) ||
    new Date(updatedAt).toISOString() !== value.retirementUpdatedAt
  ) {
    return undefined;
  }
  const generations: FtsRetirementMarker["generations"] = [];
  const generationIds = new Set<string>();
  for (const entry of value.generations) {
    if (
      !isRecord(entry) ||
      !hasOnlyKnownFields(entry, ["generation", "chunkCount", "cleanupIndex"]) ||
      !isStorageId(entry.generation) ||
      !Number.isSafeInteger(entry.chunkCount) ||
      (entry.chunkCount as number) < 0 ||
      (entry.chunkCount as number) > MAX_FTS_BASE_CHUNKS ||
      !isBoundedCursor(entry.cleanupIndex, entry.chunkCount as number) ||
      generationIds.has(entry.generation)
    ) {
      return undefined;
    }
    generationIds.add(entry.generation);
    generations.push({
      generation: entry.generation,
      chunkCount: entry.chunkCount as number,
      cleanupIndex: entry.cleanupIndex,
    });
  }
  const deltaVersions = value.deltaVersions;
  if (
    !deltaVersions.every((entry) => Number.isSafeInteger(entry) && entry >= 0) ||
    new Set(deltaVersions).size !== deltaVersions.length ||
    deltaVersions.some((entry, index) => index > 0 && entry <= (deltaVersions[index - 1] ?? -1))
  ) {
    return undefined;
  }
  return {
    kind: "fts-retirement",
    retirementUpdatedAt: value.retirementUpdatedAt,
    generations,
    deltaVersions: [...(deltaVersions as number[])],
    deltaCleanupIndex: value.deltaCleanupIndex,
  };
}

function asOptionalFtsRetirementMarker(
  value: unknown,
  location: string,
): FtsRetirementMarker | undefined {
  if (value === undefined) return undefined;
  const decoded = decodeFtsRetirementMarker(value);
  if (decoded === undefined) throw corruption(location, "postings retirement marker is invalid");
  return decoded;
}

function decodeFtsDeltaChunk(value: unknown): FtsDeltaChunk | undefined {
  if (
    !isRecord(value) ||
    !hasOnlyKnownFields(value, ["postings", "totalTokens"]) ||
    !Number.isSafeInteger(value.totalTokens) ||
    (value.totalTokens as number) < 0
  ) {
    return undefined;
  }
  const postings = decodeFtsPostingChunk(value.postings);
  return postings === undefined
    ? undefined
    : { postings, totalTokens: value.totalTokens as number };
}

function ftsChunkMatchesBoundary(
  chunk: readonly FtsPosting[],
  boundary: { first: string; last: string } | undefined,
): boolean {
  if (boundary === undefined) return false;
  return (
    (chunk[0]?.term ?? "") === boundary.first &&
    (chunk[chunk.length - 1]?.term ?? "") === boundary.last
  );
}

function sameFtsPostingChunk(left: readonly FtsPosting[], right: readonly FtsPosting[]): boolean {
  return (
    left.length === right.length &&
    left.every((posting, index) => {
      const candidate = right[index];
      if (candidate === undefined) return false;
      return (
        posting.term === candidate.term &&
        posting.rowIds.length === candidate.rowIds.length &&
        posting.rowIds.every((rowId, rowIndex) => rowId === candidate.rowIds[rowIndex]) &&
        posting.tf.length === candidate.tf.length &&
        posting.tf.every((frequency, tfIndex) => frequency === candidate.tf[tfIndex])
      );
    })
  );
}

function ftsChunkKey(tableId: string, columnId: string, version: number): string {
  return `${FTS_CHUNK_PREFIX}${tableId}/${columnId}/${String(version)}`;
}

function ftsChunkIndexKey(tableId: string, columnId: string): string {
  return `${FTS_CHUNK_PREFIX}index/${tableId}/${columnId}`;
}

function ftsBaseBuildKey(tableId: string, columnId: string): string {
  return `${FTS_BASE_BUILD_PREFIX}${tableId}/${columnId}`;
}

function ftsRetirementKey(tableId: string, columnId: string): string {
  return `${FTS_RETIREMENT_PREFIX}${tableId}/${columnId}`;
}

function ftsBaseChunkPrefix(tableId: string, columnId: string, generation?: string): string {
  return generation === undefined
    ? `${FTS_BASE_PREFIX}${tableId}/${columnId}/`
    : `${FTS_BASE_PREFIX}${tableId}/${columnId}/generation/${encodeURIComponent(generation)}/`;
}

function ftsBaseChunkPrefixFromIdentity(identity: string, generation?: string): string {
  return generation === undefined
    ? `${FTS_BASE_PREFIX}${identity}/`
    : `${FTS_BASE_PREFIX}${identity}/generation/${encodeURIComponent(generation)}/`;
}

async function ftsBaseChunkHasProvenance(catalog: IDBObjectStore, key: string): Promise<boolean> {
  const suffix = key.slice(FTS_BASE_PREFIX.length);
  const generationSeparator = suffix.lastIndexOf("/generation/");
  const ordinalSeparator = suffix.lastIndexOf("/");
  if (generationSeparator <= 0 || ordinalSeparator <= generationSeparator + 12) return false;
  const identity = suffix.slice(0, generationSeparator);
  let generation: string;
  try {
    generation = decodeURIComponent(
      suffix.slice(generationSeparator + "/generation/".length, ordinalSeparator),
    );
  } catch {
    return false;
  }
  if (!isStorageId(generation)) return false;
  const ordinal = Number(suffix.slice(ordinalSeparator + 1));
  if (!Number.isSafeInteger(ordinal) || ordinal < 0) return false;
  const [tocValue, buildValue, retirementValue] = await Promise.all([
    requestResult<unknown>(catalog.get(`${FTS_BASE_INDEX_PREFIX}${identity}`)),
    requestResult<unknown>(catalog.get(`${FTS_BASE_BUILD_PREFIX}${identity}`)),
    requestResult<unknown>(catalog.get(`${FTS_RETIREMENT_PREFIX}${identity}`)),
  ]);
  const toc = decodeFtsBaseToc(tocValue);
  if (toc?.generation === generation && ordinal < toc.boundaries.length) return true;
  const build = decodeFtsBaseBuildMarker(buildValue);
  if (
    build?.buildId === generation &&
    ordinal >= build.cleanupIndex &&
    ordinal < build.boundaries.length
  ) {
    return true;
  }
  const retirement = decodeFtsRetirementMarker(retirementValue);
  return (
    retirement?.generations.some(
      (entry) =>
        entry.generation === generation &&
        ordinal >= entry.cleanupIndex &&
        ordinal < entry.chunkCount,
    ) ?? false
  );
}

function postingIdentityHasCatalogOwner(
  catalog: IDBObjectStore,
  identity: string,
): Promise<boolean> {
  return visitObjectStoreSequentially(catalog, (value, key) => {
    if (typeof key !== "string" || !key.startsWith(TABLE_ID_PREFIX)) return false;
    const table = asTableRecord(value, key);
    return [...activePostingStorageColumnIds(table)].some(
      (columnId) => `${table.id}/${columnId}` === identity,
    );
  });
}

function autoIncrementCounterHasCatalogOwner(
  catalog: IDBObjectStore,
  counterKey: string,
): Promise<boolean> {
  return visitObjectStoreSequentially(catalog, (value, key) => {
    if (typeof key !== "string" || !key.startsWith(TABLE_ID_PREFIX)) return false;
    const table = asTableRecord(value, key);
    return table.columns.some(
      (column) =>
        column.defaultValue?.kind === "autoincrement" &&
        `${AUTO_INCREMENT_PREFIX}${table.id}/${column.id}` === counterKey,
    );
  });
}

async function ftsDeltaChunkHasProvenance(catalog: IDBObjectStore, key: string): Promise<boolean> {
  const suffix = key.slice(FTS_CHUNK_PREFIX.length);
  const separator = suffix.lastIndexOf("/");
  if (separator <= 0) return false;
  const identity = suffix.slice(0, separator);
  const version = Number(suffix.slice(separator + 1));
  if (!Number.isSafeInteger(version) || version < 0) return false;
  const [indexValue, retirementValue] = await Promise.all([
    requestResult<unknown>(catalog.get(`${FTS_CHUNK_PREFIX}index/${identity}`)),
    requestResult<unknown>(catalog.get(`${FTS_RETIREMENT_PREFIX}${identity}`)),
  ]);
  if (decodeFtsDeltaIndex(indexValue)?.versions.includes(version) === true) return true;
  const retirement = decodeFtsRetirementMarker(retirementValue);
  return retirement?.deltaVersions.slice(retirement.deltaCleanupIndex).includes(version) === true;
}

function firstFtsBaseBuildByExpiry(
  index: IDBIndex,
): Promise<{ key: string; marker: FtsBaseBuildMarker } | undefined> {
  return new Promise((resolve, reject) => {
    const request = index.openCursor();
    request.onerror = () => reject(request.error ?? new Error("IndexedDB cursor failed"));
    request.onsuccess = () => {
      const cursor = request.result;
      if (cursor === null) {
        resolve(undefined);
        return;
      }
      if (
        typeof cursor.primaryKey !== "string" ||
        !cursor.primaryKey.startsWith(FTS_BASE_BUILD_PREFIX)
      ) {
        reject(corruption("catalog", "unexpected record is present in the FTS-build index"));
      } else {
        const marker = asOptionalFtsBaseBuildMarker(cursor.value, cursor.primaryKey);
        if (marker === undefined) {
          reject(corruption(cursor.primaryKey, "postings build marker is missing"));
        } else {
          resolve({ key: cursor.primaryKey, marker });
        }
      }
    };
  });
}

function readPostingBuildAdmission(store: IDBObjectStore): Promise<{
  ftsBuilds: number;
  secondaryBuilds: number;
  retainedBytes: number;
  retainedEntries: number;
}> {
  return new Promise((resolve, reject) => {
    let ftsBuilds = 0;
    let secondaryBuilds = 0;
    let retainedBytes = 0;
    let retainedEntries = 0;
    const request = store.index(CATALOG_FTS_BUILD_EXPIRY_INDEX).openCursor();
    request.onerror = () => reject(request.error ?? new Error("IndexedDB cursor failed"));
    request.onsuccess = () => {
      const cursor = request.result;
      if (cursor === null) {
        resolve({ ftsBuilds, secondaryBuilds, retainedBytes, retainedEntries });
        return;
      }
      if (
        typeof cursor.primaryKey !== "string" ||
        !cursor.primaryKey.startsWith(FTS_BASE_BUILD_PREFIX)
      ) {
        reject(corruption("catalog", "unexpected record is present in the postings-build index"));
        return;
      }
      const marker = asOptionalFtsBaseBuildMarker(cursor.value, cursor.primaryKey);
      if (marker === undefined) {
        reject(corruption(cursor.primaryKey, "postings build marker is missing"));
        return;
      }
      if (marker.ownerKind === "secondary-index") secondaryBuilds += 1;
      else ftsBuilds += 1;
      retainedBytes = safeByteSum(
        retainedBytes,
        marker.retainedBytes,
        "Accelerator build retained bytes",
      );
      retainedEntries = safeByteSum(
        retainedEntries,
        marker.retainedEntries,
        "Accelerator build retained entries",
      );
      cursor.continue();
    };
  });
}

function firstFtsRetirementByUpdatedAt(
  index: IDBIndex,
): Promise<{ key: string; marker: FtsRetirementMarker } | undefined> {
  return new Promise((resolve, reject) => {
    const request = index.openCursor();
    request.onerror = () => reject(request.error ?? new Error("IndexedDB cursor failed"));
    request.onsuccess = () => {
      const cursor = request.result;
      if (cursor === null) {
        resolve(undefined);
        return;
      }
      if (
        typeof cursor.primaryKey !== "string" ||
        !cursor.primaryKey.startsWith(FTS_RETIREMENT_PREFIX)
      ) {
        reject(corruption("catalog", "unexpected record is present in the FTS-retirement index"));
      } else {
        const marker = asOptionalFtsRetirementMarker(cursor.value, cursor.primaryKey);
        if (marker === undefined) {
          reject(corruption(cursor.primaryKey, "postings retirement marker is missing"));
        } else {
          resolve({ key: cursor.primaryKey, marker });
        }
      }
    };
  });
}

async function stageFtsRetirement(
  store: IDBObjectStore,
  tableId: string,
  columnId: string,
  generations: ReadonlyArray<FtsRetirementMarker["generations"][number]>,
  deltaVersions: readonly number[],
): Promise<void> {
  const key = ftsRetirementKey(tableId, columnId);
  const current = asOptionalFtsRetirementMarker(await requestResult<unknown>(store.get(key)), key);
  const byGeneration = new Map(
    (current?.generations ?? []).map((entry) => [entry.generation, entry] as const),
  );
  for (const entry of generations) {
    const existing = byGeneration.get(entry.generation);
    if (
      existing !== undefined &&
      (existing.chunkCount !== entry.chunkCount || existing.cleanupIndex !== entry.cleanupIndex)
    ) {
      throw corruption(key, `conflicting retirement for generation ${entry.generation}`);
    }
    byGeneration.set(entry.generation, entry);
  }
  const mergedGenerations = [...byGeneration.values()];
  if (mergedGenerations.length > 3) {
    throw corruption(key, "too many pending postings generations");
  }
  const mergedVersions = [...new Set([...(current?.deltaVersions ?? []), ...deltaVersions])].sort(
    (left, right) => left - right,
  );
  if (mergedVersions.length > MAX_FTS_DELTA_CHUNKS) {
    throw corruption(key, "too many pending postings deltas");
  }
  if (mergedGenerations.length === 0 && mergedVersions.length === 0) return;
  store.put(
    {
      kind: "fts-retirement",
      retirementUpdatedAt: dateIsoString(new Date()),
      generations: mergedGenerations,
      deltaVersions: mergedVersions,
      deltaCleanupIndex: current?.deltaCleanupIndex ?? 0,
    } satisfies FtsRetirementMarker,
    key,
  );
}

async function deleteFtsRetirementPage(store: IDBObjectStore, identity: string): Promise<boolean> {
  const key = `${FTS_RETIREMENT_PREFIX}${identity}`;
  const marker = asOptionalFtsRetirementMarker(await requestResult<unknown>(store.get(key)), key);
  if (marker === undefined) return true;
  let remaining = FTS_BASE_BUILD_CLEANUP_PAGE;
  const generations = marker.generations.map((entry) => ({ ...entry }));
  for (const entry of generations) {
    if (remaining === 0 || entry.cleanupIndex === entry.chunkCount) continue;
    const end = Math.min(entry.cleanupIndex + remaining, entry.chunkCount);
    const prefix = ftsBaseChunkPrefixFromIdentity(identity, entry.generation);
    for (let ordinal = entry.cleanupIndex; ordinal < end; ordinal += 1) {
      store.delete(`${prefix}${String(ordinal).padStart(6, "0")}`);
    }
    remaining -= end - entry.cleanupIndex;
    entry.cleanupIndex = end;
  }
  let deltaCleanupIndex = marker.deltaCleanupIndex;
  const deltaEnd = Math.min(deltaCleanupIndex + remaining, marker.deltaVersions.length);
  for (; deltaCleanupIndex < deltaEnd; deltaCleanupIndex += 1) {
    store.delete(
      `${FTS_CHUNK_PREFIX}${identity}/${String(marker.deltaVersions[deltaCleanupIndex] ?? -1)}`,
    );
  }
  const complete =
    generations.every((entry) => entry.cleanupIndex === entry.chunkCount) &&
    deltaCleanupIndex === marker.deltaVersions.length;
  if (complete) {
    store.delete(key);
  } else {
    store.put(
      {
        ...marker,
        retirementUpdatedAt: dateIsoString(new Date()),
        generations,
        deltaCleanupIndex,
      },
      key,
    );
  }
  return complete;
}

async function deleteFtsColumnRecords(
  store: IDBObjectStore,
  tableId: string,
  columnId: string,
): Promise<void> {
  const tocKey = `${FTS_BASE_INDEX_PREFIX}${tableId}/${columnId}`;
  const deltaIndexKey = ftsChunkIndexKey(tableId, columnId);
  const markerKey = ftsBaseBuildKey(tableId, columnId);
  const [tocValue, deltaIndexValue, markerValue] = await Promise.all([
    requestResult<unknown>(store.get(tocKey)),
    requestResult<unknown>(store.get(deltaIndexKey)),
    requestResult<unknown>(store.get(markerKey)),
  ]);
  const toc = asOptionalFtsBaseToc(tocValue, tocKey);
  const deltaIndex = asOptionalFtsDeltaIndex(deltaIndexValue, deltaIndexKey);
  const marker = asOptionalFtsBaseBuildMarker(markerValue, markerKey);
  const generations: FtsRetirementMarker["generations"] = [];
  if (toc !== undefined) {
    generations.push({
      generation: toc.generation,
      chunkCount: toc.boundaries.length,
      cleanupIndex: 0,
    });
  }
  if (marker !== undefined) {
    generations.push({
      generation: marker.buildId,
      chunkCount: marker.boundaries.length,
      cleanupIndex: marker.cleanupIndex,
    });
  }
  await stageFtsRetirement(store, tableId, columnId, generations, deltaIndex?.versions ?? []);
  store.delete(tocKey);
  store.delete(deltaIndexKey);
  store.delete(markerKey);
}

function validateAutoIncrementReservation(count: number, atLeast: bigint | undefined): void {
  if (!Number.isSafeInteger(count) || count < 0) {
    throw new RangeError("Auto-increment reservation count must be a non-negative whole number");
  }
  if (atLeast !== undefined && typeof atLeast !== "bigint") {
    throw new TypeError("Auto-increment bump target must be a bigint");
  }
  if (atLeast !== undefined && atLeast < 1n) {
    throw new RangeError("Auto-increment bump target must be at least 1");
  }
  if (atLeast !== undefined && atLeast > MAX_AUTO_INCREMENT_EXCLUSIVE_END) {
    throw new RangeError("Auto-increment bump target is outside the safe integer range");
  }
}

function assertCounterEndInRange(endExclusive: bigint, maximum: bigint, label: string): void {
  if (endExclusive > maximum) throw new RangeError(`${label} exceeds its numeric range`);
}

function maxBigInt(a: bigint, b: bigint): bigint {
  return a > b ? a : b;
}

function validateTempRunPage(page: TempRunPage): void {
  validateTempRunPageIdentity(page.ownerId, page.runId, page.pageIndex);
  assertUnsharedBytes(page.bytes, "Temp run page bytes");
  if (page.bytes.byteLength === 0) throw new RangeError("Temp run page bytes cannot be empty");
}

function emptyResourceLedger(): ResourceLedgerRecord {
  return { stagedBlockCount: 0, stagedSegmentCount: 0, stagedBytes: 0, retiredHistoryBytes: 0 };
}

function emptyCatalogResourceLedger(): CatalogResourceLedgerRecord {
  return withCatalogResourceLedgerChecksum({ recordCount: 0, retainedBytes: 0 });
}

function emptyRecordResourceLedger(): RecordResourceLedgerRecord {
  return withRecordResourceLedgerChecksum({
    manifestCount: 0,
    manifestBytes: 0,
    segmentCount: 0,
    segmentBytes: 0,
  });
}

function asCatalogResourceLedger(value: unknown): CatalogResourceLedgerRecord {
  const record = asBoundedLedgerCounts<CatalogResourceLedgerRecord>(
    value,
    ["recordCount", "retainedBytes", "checksum"],
    `statistics/${CATALOG_RESOURCE_LEDGER_KEY}`,
  );
  if (
    record.recordCount > MAX_CATALOG_RECORDS ||
    record.retainedBytes > MAX_CATALOG_RETAINED_BYTES
  ) {
    throw corruption(
      `statistics/${CATALOG_RESOURCE_LEDGER_KEY}`,
      "catalog resource ledger exceeds its hard limit",
    );
  }
  if (record.checksum !== catalogResourceLedgerChecksum(record)) {
    throw corruption(
      `statistics/${CATALOG_RESOURCE_LEDGER_KEY}`,
      "catalog resource ledger checksum is invalid",
    );
  }
  return record;
}

function catalogResourceLedgerChecksum(
  record: Omit<CatalogResourceLedgerRecord, "checksum">,
): number {
  return crc32(
    new TextEncoder().encode(`${String(record.recordCount)}:${String(record.retainedBytes)}`),
  );
}

function withCatalogResourceLedgerChecksum(
  record: Omit<CatalogResourceLedgerRecord, "checksum">,
): CatalogResourceLedgerRecord {
  return { ...record, checksum: catalogResourceLedgerChecksum(record) };
}

function asRecordResourceLedger(value: unknown): RecordResourceLedgerRecord {
  const record = asBoundedLedgerCounts<RecordResourceLedgerRecord>(
    value,
    ["manifestCount", "manifestBytes", "segmentCount", "segmentBytes", "checksum"],
    `statistics/${RECORD_RESOURCE_LEDGER_KEY}`,
  );
  const expected = recordResourceLedgerChecksum(record);
  if (record.checksum !== expected) {
    throw corruption(
      `statistics/${RECORD_RESOURCE_LEDGER_KEY}`,
      "record resource ledger checksum is invalid",
    );
  }
  if (
    record.manifestCount > MAX_MANIFEST_RECORDS ||
    record.manifestBytes > MAX_MANIFEST_RETAINED_BYTES ||
    record.segmentCount > MAX_SEGMENT_RECORDS ||
    record.segmentBytes > MAX_SEGMENT_RETAINED_BYTES
  ) {
    throw corruption(
      `statistics/${RECORD_RESOURCE_LEDGER_KEY}`,
      "record resource ledger exceeds its hard limit",
    );
  }
  return record;
}

function recordResourceLedgerChecksum(
  record: Omit<RecordResourceLedgerRecord, "checksum">,
): number {
  return crc32(
    new TextEncoder().encode(
      `${String(record.manifestCount)}:${String(record.manifestBytes)}:${String(record.segmentCount)}:${String(record.segmentBytes)}`,
    ),
  );
}

function withRecordResourceLedgerChecksum(
  record: Omit<RecordResourceLedgerRecord, "checksum">,
): RecordResourceLedgerRecord {
  return { ...record, checksum: recordResourceLedgerChecksum(record) };
}

async function updateRecordResourceLedger(
  store: IDBObjectStore,
  changes: {
    manifests?: ReadonlyArray<{
      previous?: StoredManifestRecord;
      next?: StoredManifestRecord;
    }>;
    segments?: ReadonlyArray<{ previous?: SegmentRecord; next?: SegmentRecord }>;
  },
): Promise<void> {
  const current = asRecordResourceLedger(
    await requestResult<unknown>(store.get(RECORD_RESOURCE_LEDGER_KEY)),
  );
  let manifestCount = current.manifestCount;
  let manifestBytes = current.manifestBytes;
  const manifestVersions = new Set<number>();
  for (const change of changes.manifests ?? []) {
    const version = change.previous?.version ?? change.next?.version;
    if (version === undefined || manifestVersions.has(version)) {
      throw new TypeError("Manifest resource changes must have distinct versions");
    }
    if (
      change.previous !== undefined &&
      change.next !== undefined &&
      change.previous.version !== change.next.version
    ) {
      throw new TypeError("Manifest resource replacement changed its version");
    }
    manifestVersions.add(version);
    if (change.previous !== undefined) {
      manifestCount -= 1;
      manifestBytes -= manifestRecordRetainedReservationBytes(change.previous);
    }
    if (change.next !== undefined) {
      manifestCount += 1;
      manifestBytes += manifestRecordRetainedReservationBytes(change.next);
    }
  }
  let segmentCount = current.segmentCount;
  let segmentBytes = current.segmentBytes;
  const segmentIds = new Set<string>();
  for (const change of changes.segments ?? []) {
    const id = change.previous?.id ?? change.next?.id;
    if (id === undefined || segmentIds.has(id)) {
      throw new TypeError("Segment resource changes must have distinct IDs");
    }
    if (
      change.previous !== undefined &&
      change.next !== undefined &&
      change.previous.id !== change.next.id
    ) {
      throw new TypeError("Segment resource replacement changed its ID");
    }
    segmentIds.add(id);
    if (change.previous !== undefined) {
      segmentCount -= 1;
      segmentBytes -= segmentRecordRetainedBytes(change.previous);
    }
    if (change.next !== undefined) {
      segmentCount += 1;
      segmentBytes += segmentRecordRetainedBytes(change.next);
    }
  }
  const nextWithoutChecksum = { manifestCount, manifestBytes, segmentCount, segmentBytes };
  for (const [field, value] of Object.entries(nextWithoutChecksum)) {
    if (!Number.isSafeInteger(value) || value < 0) {
      throw corruption(
        `statistics/${RECORD_RESOURCE_LEDGER_KEY}`,
        `record resource ledger ${field} overflowed`,
      );
    }
  }
  if (manifestCount > MAX_MANIFEST_RECORDS) {
    throw new StorageResourceLimitError("manifest record", manifestCount, MAX_MANIFEST_RECORDS);
  }
  if (manifestBytes > MAX_MANIFEST_RETAINED_BYTES) {
    throw new StorageResourceLimitError(
      "manifest byte",
      manifestBytes,
      MAX_MANIFEST_RETAINED_BYTES,
    );
  }
  if (segmentCount > MAX_SEGMENT_RECORDS) {
    throw new StorageResourceLimitError("segment record", segmentCount, MAX_SEGMENT_RECORDS);
  }
  if (segmentBytes > MAX_SEGMENT_RETAINED_BYTES) {
    throw new StorageResourceLimitError("segment byte", segmentBytes, MAX_SEGMENT_RETAINED_BYTES);
  }
  store.put(withRecordResourceLedgerChecksum(nextWithoutChecksum), RECORD_RESOURCE_LEDGER_KEY);
}

async function updateCatalogResourceLedger(
  store: IDBObjectStore,
  previous: TableRecord | undefined,
  next: TableRecord | undefined,
): Promise<void> {
  const current = asCatalogResourceLedger(
    await requestResult<unknown>(store.get(CATALOG_RESOURCE_LEDGER_KEY)),
  );
  const previousBytes = previous === undefined ? 0 : catalogRecordRetainedBytes(previous);
  const nextBytes = next === undefined ? 0 : catalogRecordRetainedBytes(next);
  const recordCount =
    current.recordCount - (previous === undefined ? 0 : 1) + (next === undefined ? 0 : 1);
  const retainedBytes = current.retainedBytes - previousBytes + nextBytes;
  if (
    !Number.isSafeInteger(recordCount) ||
    recordCount < 0 ||
    !Number.isSafeInteger(retainedBytes) ||
    retainedBytes < 0
  ) {
    throw corruption(`statistics/${CATALOG_RESOURCE_LEDGER_KEY}`, "catalog byte ledger overflowed");
  }
  if (recordCount > MAX_CATALOG_RECORDS) {
    throw new StorageResourceLimitError("catalog record", recordCount, MAX_CATALOG_RECORDS);
  }
  if (retainedBytes > MAX_CATALOG_RETAINED_BYTES) {
    throw new StorageResourceLimitError("catalog byte", retainedBytes, MAX_CATALOG_RETAINED_BYTES);
  }
  store.put(
    withCatalogResourceLedgerChecksum({ recordCount, retainedBytes }),
    CATALOG_RESOURCE_LEDGER_KEY,
  );
}

async function pendingCatalogReservations(
  store: IDBObjectStore,
  excludeTransactionId?: string,
): Promise<{ records: TableRecord[]; retainedBytes: number }> {
  const records: TableRecord[] = [];
  let retainedBytes = 0;
  await visitObjectStoreSequentially(store, (value, key) => {
    if (typeof key !== "string") throw corruption("transactions", "record key is invalid");
    const transaction = asTransactionRecord(value, key);
    if (
      transaction.id === excludeTransactionId ||
      transaction.status !== "active" ||
      transaction.pendingTable === undefined
    ) {
      return;
    }
    records.push(transaction.pendingTable);
    retainedBytes = safeByteSum(
      retainedBytes,
      catalogRecordRetainedBytes(transaction.pendingTable),
      "Pending catalog retained bytes",
    );
  });
  return { records, retainedBytes };
}

async function assertCatalogReservationAdmission(
  transaction: IDBTransaction,
  record: TableRecord,
  excludeTransactionId?: string,
): Promise<void> {
  const published = asCatalogResourceLedger(
    await requestResult<unknown>(
      transaction.objectStore("statistics").get(CATALOG_RESOURCE_LEDGER_KEY),
    ),
  );
  const pending = await pendingCatalogReservations(
    transaction.objectStore("transactions"),
    excludeTransactionId,
  );
  if (pending.records.some((candidate) => candidate.id === record.id)) {
    throw new Error(`Table already exists: ${record.id}`);
  }
  if (pending.records.some((candidate) => candidate.name === record.name)) {
    throw new Error(`Table name already exists: ${record.name}`);
  }
  const catalog = transaction.objectStore("catalog");
  for (const trigger of record.triggers ?? []) {
    const nameKey = `${TRIGGER_NAME_PREFIX}${trigger.name}`;
    const idKey = `${TRIGGER_ID_PREFIX}${trigger.id}`;
    const [nameMarker, idMarker] = await Promise.all([
      requestResult<unknown>(catalog.get(nameKey)).then((value) =>
        asOptionalTriggerNameMarker(value, nameKey),
      ),
      requestResult<unknown>(catalog.get(idKey)).then((value) =>
        asOptionalTriggerIdMarker(value, idKey),
      ),
    ]);
    if (nameMarker !== undefined) throw new TypeError(`Trigger already exists: ${trigger.name}`);
    if (idMarker !== undefined) throw new TypeError(`Trigger ID already exists: ${trigger.id}`);
    for (const candidate of pending.records) {
      for (const pendingTrigger of candidate.triggers ?? []) {
        if (pendingTrigger.name === trigger.name) {
          throw new TypeError(`Trigger already exists: ${trigger.name}`);
        }
        if (pendingTrigger.id === trigger.id) {
          throw new TypeError(`Trigger ID already exists: ${trigger.id}`);
        }
      }
    }
  }
  const recordCount = published.recordCount + pending.records.length + 1;
  const retainedBytes =
    published.retainedBytes + pending.retainedBytes + catalogRecordRetainedBytes(record);
  if (recordCount > MAX_CATALOG_RECORDS) {
    throw new StorageResourceLimitError("catalog record", recordCount, MAX_CATALOG_RECORDS);
  }
  if (retainedBytes > MAX_CATALOG_RETAINED_BYTES) {
    throw new StorageResourceLimitError("catalog byte", retainedBytes, MAX_CATALOG_RETAINED_BYTES);
  }
}

async function assertTableForeignKeysInTransaction(
  catalog: IDBObjectStore,
  record: TableRecord,
): Promise<void> {
  for (const key of record.foreignKeys ?? []) {
    const parent =
      key.parentTable === record.name
        ? record
        : await readDeclaredTableByName(catalog, key.parentTable);
    if (parent === undefined) {
      throw new TypeError(`FOREIGN KEY ${key.name} references a missing table: ${key.parentTable}`);
    }
    const addressIds = parent.primaryKeyColumnIds?.length
      ? parent.primaryKeyColumnIds
      : parent.uniqueKeyColumnId === undefined
        ? []
        : [parent.uniqueKeyColumnId];
    const addressNames = addressIds.map(
      (id) => parent.columns.find((column) => column.id === id)?.name ?? "",
    );
    if (
      addressNames.length !== key.parentColumns.length ||
      addressNames.some((name, index) => name !== key.parentColumns[index])
    ) {
      throw new TypeError(
        `FOREIGN KEY ${key.name} must reference the parent primary or unique key`,
      );
    }
  }
}

async function updateRetiredHistoryLedger(
  store: IDBObjectStore,
  deltaBytes: number,
): Promise<void> {
  const current = asResourceLedger(await requestResult<unknown>(store.get(RESOURCE_LEDGER_KEY)));
  const retiredHistoryBytes = current.retiredHistoryBytes + deltaBytes;
  if (!Number.isSafeInteger(retiredHistoryBytes) || retiredHistoryBytes < 0) {
    throw corruption(`statistics/${RESOURCE_LEDGER_KEY}`, "retired history byte ledger overflowed");
  }
  if (retiredHistoryBytes > MAX_RETIRED_HISTORY_BYTES) {
    throw new StorageResourceLimitError(
      "retired history byte",
      retiredHistoryBytes,
      MAX_RETIRED_HISTORY_BYTES,
    );
  }
  store.put({ ...current, retiredHistoryBytes }, RESOURCE_LEDGER_KEY);
}

function transactionResourceLedgerKey(transactionId: string): string {
  return `${TRANSACTION_RESOURCE_LEDGER_PREFIX}${encodeURIComponent(transactionId)}`;
}

function asBoundedLedgerCounts<T extends object>(
  value: unknown,
  fields: ReadonlyArray<keyof T & string>,
  location: string,
): T {
  if (!isRecord(value)) throw corruption(location, "resource ledger record is missing or invalid");
  assertKnownFields(value, fields, location);
  for (const field of fields) {
    const count = value[field];
    if (!Number.isSafeInteger(count) || (count as number) < 0) {
      throw corruption(location, `resource ledger ${field} is invalid`);
    }
  }
  return value as T;
}

function asResourceLedger(value: unknown): ResourceLedgerRecord {
  return asBoundedLedgerCounts<ResourceLedgerRecord>(
    value,
    ["stagedBlockCount", "stagedSegmentCount", "stagedBytes", "retiredHistoryBytes"],
    `statistics/${RESOURCE_LEDGER_KEY}`,
  );
}

function asOptionalTransactionResourceLedger(
  value: unknown,
  transactionId: string,
): TransactionResourceLedgerRecord | undefined {
  if (value === undefined) return undefined;
  return asBoundedLedgerCounts<TransactionResourceLedgerRecord>(
    value,
    ["blockCount", "segmentCount", "retainedBytes"],
    `statistics/${transactionResourceLedgerKey(transactionId)}`,
  );
}

async function updateTransactionResourceLedger(
  store: IDBObjectStore,
  transactionId: string,
  delta: { blockCount: number; segmentCount: number; retainedBytes: number },
): Promise<void> {
  const global = asResourceLedger(await requestResult<unknown>(store.get(RESOURCE_LEDGER_KEY)));
  const key = transactionResourceLedgerKey(transactionId);
  const current = asOptionalTransactionResourceLedger(
    await requestResult<unknown>(store.get(key)),
    transactionId,
  ) ?? { blockCount: 0, segmentCount: 0, retainedBytes: 0 };
  const next = {
    blockCount: current.blockCount + delta.blockCount,
    segmentCount: current.segmentCount + delta.segmentCount,
    retainedBytes: current.retainedBytes + delta.retainedBytes,
  };
  const nextGlobal: ResourceLedgerRecord = {
    ...global,
    stagedBlockCount: global.stagedBlockCount + delta.blockCount,
    stagedSegmentCount: global.stagedSegmentCount + delta.segmentCount,
    stagedBytes: global.stagedBytes + delta.retainedBytes,
  };
  for (const [field, value] of Object.entries({ ...next, ...nextGlobal })) {
    if (!Number.isSafeInteger(value) || value < 0) {
      throw corruption(`statistics/${RESOURCE_LEDGER_KEY}`, `resource ledger ${field} overflowed`);
    }
  }
  if (nextGlobal.stagedBlockCount > MAX_GLOBAL_STAGED_BLOCKS) {
    throw new StorageResourceLimitError(
      "staged block",
      nextGlobal.stagedBlockCount,
      MAX_GLOBAL_STAGED_BLOCKS,
    );
  }
  if (nextGlobal.stagedSegmentCount > MAX_GLOBAL_STAGED_SEGMENTS) {
    throw new StorageResourceLimitError(
      "staged segment",
      nextGlobal.stagedSegmentCount,
      MAX_GLOBAL_STAGED_SEGMENTS,
    );
  }
  if (nextGlobal.stagedBytes > MAX_GLOBAL_STAGED_ARTIFACT_BYTES) {
    throw new StorageResourceLimitError(
      "staged artifact byte",
      nextGlobal.stagedBytes,
      MAX_GLOBAL_STAGED_ARTIFACT_BYTES,
    );
  }
  store.put(nextGlobal, RESOURCE_LEDGER_KEY);
  if (next.blockCount === 0 && next.segmentCount === 0 && next.retainedBytes === 0)
    store.delete(key);
  else store.put(next, key);
}

async function clearTransactionResourceLedger(
  store: IDBObjectStore,
  transactionId: string,
): Promise<void> {
  const key = transactionResourceLedgerKey(transactionId);
  const current = asOptionalTransactionResourceLedger(
    await requestResult<unknown>(store.get(key)),
    transactionId,
  );
  if (current === undefined) return;
  await updateTransactionResourceLedger(store, transactionId, {
    blockCount: -current.blockCount,
    segmentCount: -current.segmentCount,
    retainedBytes: -current.retainedBytes,
  });
}

async function assertActiveTransactionAdmission(store: IDBObjectStore): Promise<void> {
  const index = store.index(TRANSACTION_STATUS_INDEX);
  // Only active records consume this quota. Counting the terminal partitions and the whole
  // store made every begin O(database-lifetime transaction count), turning a concurrent run
  // into quadratic IndexedDB cursor work. The active partition is itself hard-bounded.
  const active = await requestResult<number>(index.count("active"));
  if (active >= MAX_ACTIVE_TRANSACTIONS) {
    throw new StorageResourceLimitError("transaction", active + 1, MAX_ACTIVE_TRANSACTIONS);
  }
}

async function assertTerminalTransactionAdmission(store: IDBObjectStore): Promise<void> {
  const index = store.index(TRANSACTION_STATUS_INDEX);
  const [committed, aborted] = await Promise.all([
    requestResult<number>(index.count("committed")),
    requestResult<number>(index.count("aborted")),
  ]);
  const terminal = committed + aborted;
  if (!Number.isSafeInteger(terminal)) {
    throw corruption("transactions", "terminal record count is unsafe");
  }
  if (terminal >= MAX_TERMINAL_TRANSACTION_RECORDS) {
    throw new StorageResourceLimitError(
      "terminal transaction",
      terminal + 1,
      MAX_TERMINAL_TRANSACTION_RECORDS,
    );
  }
}

/** Sweep at most one maintenance page before lease admission. Backup leases are paired with the
 * durable export marker and are reclaimed by the export singleton path, so this generic reader
 * admission sweep deliberately removes reader leases only. */
function deleteExpiredReaderLeasePage(
  store: IDBObjectStore,
  expiresAtCutoff: string,
  limit: number,
): Promise<number> {
  const cutoff = Date.parse(expiresAtCutoff);
  return new Promise((resolve, reject) => {
    const request = store.index(LEASE_EXPIRY_INDEX).openCursor();
    let visited = 0;
    let removed = 0;
    request.onerror = () => reject(request.error ?? new Error("IndexedDB cursor failed"));
    request.onsuccess = () => {
      const cursor = request.result;
      if (cursor === null || visited >= limit) {
        resolve(removed);
        return;
      }
      if (typeof cursor.primaryKey !== "string") {
        reject(corruption("leases", "record key is invalid"));
        return;
      }
      const record = asLeaseRecord(cursor.value, cursor.primaryKey);
      if (Date.parse(record.expiresAt) > cutoff) {
        resolve(removed);
        return;
      }
      visited += 1;
      if (record.kind === "reader") {
        cursor.delete();
        removed += 1;
      }
      cursor.continue();
    };
  });
}

async function assertPinnedHistoryAdmission(
  transaction: IDBTransaction,
  input: {
    cutoff: string;
    currentVersion: number | null;
    replacementLease?: LeaseRecord;
    replacementTransaction?: TransactionRecord;
    excludeLeaseId?: string;
    excludeTransactionId?: string;
    prospectiveRemovedBlockIds?: ReadonlySet<string>;
  },
): Promise<void> {
  const cutoff = Date.parse(input.cutoff);
  if (!Number.isFinite(cutoff)) throw new TypeError("Pinned-history cutoff must be valid");
  const pinnedVersions: number[] = [];
  if (transaction.objectStoreNames.contains("leases")) {
    await visitObjectStoreSequentially(transaction.objectStore("leases"), (value, key) => {
      if (typeof key !== "string") throw corruption("leases", "record key is invalid");
      if (key === input.excludeLeaseId) return;
      const lease = asLeaseRecord(value, key);
      if (lease.manifestVersion !== null && Date.parse(lease.expiresAt) > cutoff) {
        pinnedVersions.push(lease.manifestVersion);
      }
    });
  }
  const replacement = input.replacementLease;
  if (
    replacement?.manifestVersion !== null &&
    replacement?.manifestVersion !== undefined &&
    Date.parse(replacement.expiresAt) > cutoff
  ) {
    pinnedVersions.push(replacement.manifestVersion);
  }
  const replacementTransaction = input.replacementTransaction;
  if (
    replacementTransaction?.status === "active" &&
    replacementTransaction.snapshotVersion !== null &&
    Date.parse(replacementTransaction.expiresAt) > cutoff
  ) {
    pinnedVersions.push(replacementTransaction.snapshotVersion);
  }
  if (transaction.objectStoreNames.contains("transactions")) {
    const transactions = transaction.objectStore("transactions");
    // Terminal journals can grow to the durable hard limit before collection. They cannot pin
    // history, so walk only the bounded active partition instead of rescanning every historical
    // transaction on each begin and commit.
    await visitIndexPartitionSequentially(
      transactions.index(TRANSACTION_STATUS_INDEX),
      "active",
      (value, key) => {
        if (typeof key !== "string") throw corruption("transactions", "record key is invalid");
        if (key === input.excludeTransactionId) return;
        const record = asTransactionRecord(value, key);
        if (record.status !== "active") {
          throw corruption(`transactions/${key}`, "status index does not match its record");
        }
        if (record.snapshotVersion !== null && Date.parse(record.expiresAt) > cutoff) {
          pinnedVersions.push(record.snapshotVersion);
        }
      },
    );
  }
  if (input.currentVersion !== null && pinnedVersions.length > 0) {
    const oldest = Math.min(...pinnedVersions);
    const lag = input.currentVersion - oldest;
    if (lag > MAX_PINNED_MANIFEST_VERSION_LAG) {
      throw new StorageResourceLimitError(
        "pinned manifest version lag",
        lag,
        MAX_PINNED_MANIFEST_VERSION_LAG,
      );
    }
    if (
      pinnedVersions.every((version) => version === input.currentVersion) &&
      (input.prospectiveRemovedBlockIds?.size ?? 0) === 0
    ) {
      return;
    }
  }
  if (pinnedVersions.length === 0) return;
  let pinnedBlockCount = 0;
  let pinnedBytes = 0;
  await visitManifestBlockRecords(transaction.objectStore("catalog"), (record) => {
    const removedVersion = input.prospectiveRemovedBlockIds?.has(record.blockId)
      ? input.currentVersion
      : record.removedVersion;
    if (removedVersion === null) return undefined;
    const pinned = pinnedVersions.some(
      (version) => record.addedVersion <= version && version < removedVersion,
    );
    if (!pinned) return undefined;
    pinnedBlockCount = incrementSafeInteger(pinnedBlockCount, "Pinned retired block count");
    pinnedBytes = safeByteSum(pinnedBytes, record.byteLength, "Pinned retired block bytes");
    if (pinnedBlockCount > MAX_PINNED_RETIRED_BLOCKS) {
      throw new StorageResourceLimitError(
        "pinned retired block",
        pinnedBlockCount,
        MAX_PINNED_RETIRED_BLOCKS,
      );
    }
    if (pinnedBytes > MAX_PINNED_RETIRED_BYTES) {
      throw new StorageResourceLimitError(
        "pinned retired byte",
        pinnedBytes,
        MAX_PINNED_RETIRED_BYTES,
      );
    }
    return undefined;
  });
}

/** Recomputes the table-only catalog ledger through a native lexical prefix cursor. */
function readCatalogResourceLedger(store: IDBObjectStore): Promise<CatalogResourceLedgerRecord> {
  return new Promise((resolve, reject) => {
    const request = store.openCursor();
    let positioned = false;
    let recordCount = 0;
    let retainedBytes = 0;
    request.onerror = () => reject(request.error ?? new Error("IndexedDB cursor failed"));
    request.onsuccess = () => {
      const cursor = request.result;
      if (cursor === null) {
        resolve(withCatalogResourceLedgerChecksum({ recordCount, retainedBytes }));
        return;
      }
      const key = cursor.key;
      if (!positioned) {
        positioned = true;
        if (typeof key !== "string" || !key.startsWith(TABLE_ID_PREFIX)) {
          cursor.continue(TABLE_ID_PREFIX);
          return;
        }
      }
      if (typeof key !== "string" || !key.startsWith(TABLE_ID_PREFIX)) {
        resolve(withCatalogResourceLedgerChecksum({ recordCount, retainedBytes }));
        return;
      }
      try {
        recordCount = incrementSafeInteger(recordCount, "Catalog record count");
        if (recordCount > MAX_CATALOG_RECORDS) {
          throw corruption(TABLE_ID_PREFIX, "catalog record count exceeds its hard limit");
        }
        retainedBytes = safeByteSum(
          retainedBytes,
          catalogRecordRetainedBytes(asTableRecord(cursor.value, key)),
          "Catalog retained bytes",
        );
        if (retainedBytes > MAX_CATALOG_RETAINED_BYTES) {
          throw corruption(TABLE_ID_PREFIX, "catalog bytes exceed their hard limit");
        }
        cursor.continue();
      } catch (error) {
        reject(error instanceof Error ? error : new Error(String(error)));
      }
    };
  });
}

function validateTempRunPageIdentity(ownerId: string, runId: string, pageIndex: number): void {
  validateTempId(ownerId, "Temp run owner ID");
  validateTempId(runId, "Temp run ID");
  if (!Number.isSafeInteger(pageIndex) || pageIndex < 0) {
    throw new RangeError("Temp run page index must be a non-negative whole number");
  }
}

function validateId(value: unknown, label: string): asserts value is string {
  validateStorageId(value, label);
}

function isStorageId(value: unknown): value is string {
  try {
    validateStorageId(value);
    return true;
  } catch {
    return false;
  }
}

function isStorageIdArray(value: unknown): value is string[] {
  return Array.isArray(value) && value.every(isStorageId);
}

function isCatalogName(value: unknown): value is string {
  try {
    validateCatalogName(value);
    return true;
  } catch {
    return false;
  }
}

function isCatalogNameArray(value: unknown): value is string[] {
  return Array.isArray(value) && value.every(isCatalogName);
}

async function readDeclaredTable(
  catalog: IDBObjectStore,
  tableId: string,
): Promise<TableRecord | undefined> {
  validateId(tableId, "Table ID");
  const key = `${TABLE_ID_PREFIX}${tableId}`;
  const value: unknown = await requestResult(catalog.get(key));
  if (value === undefined) return undefined;
  const table = asTableRecord(value, key);
  if (table.id !== tableId) throw corruption(key, "table ID does not match its catalog key");
  return table;
}

async function readDeclaredTableByName(
  catalog: IDBObjectStore,
  name: string,
): Promise<TableRecord | undefined> {
  validateCatalogName(name, "Table name");
  const pointerKey = `${TABLE_NAME_PREFIX}${name}`;
  const rawId: unknown = await requestResult(catalog.get(pointerKey));
  if (rawId === undefined) return undefined;
  if (!isStorageId(rawId)) throw corruption(pointerKey, "table name pointer is invalid");
  const table = await readDeclaredTable(catalog, rawId);
  if (table?.name !== name) {
    throw corruption(pointerKey, "does not match its table record");
  }
  return table;
}

async function installPendingTableCatalogRecords(
  catalog: IDBObjectStore,
  record: TableRecord,
): Promise<void> {
  const indexNames = new Set<string>();
  for (const [indexId, index] of Object.entries(record.secondaryIndexes ?? {})) {
    const markerKey = `${SECONDARY_INDEX_NAME_PREFIX}${index.name}`;
    if (
      indexNames.has(index.name) ||
      (await requestResult<unknown>(catalog.get(markerKey))) !== undefined
    ) {
      throw new TypeError(`Index already exists: ${index.name}`);
    }
    indexNames.add(index.name);
    catalog.put({ tableId: record.id, indexId }, markerKey);
    if (index.uniqueEnforced === true) {
      catalog.put(
        { versions: [], hasBase: false } satisfies UniqueKeyChunkIndex,
        uniqueKeyChunkIndexKey(secondaryUniqueKeyNamespace(record.id, indexId)),
      );
    }
  }
  for (const trigger of record.triggers ?? []) {
    const nameKey = `${TRIGGER_NAME_PREFIX}${trigger.name}`;
    const idKey = `${TRIGGER_ID_PREFIX}${trigger.id}`;
    const [nameMarker, idMarker] = await Promise.all([
      requestResult<unknown>(catalog.get(nameKey)).then((value) =>
        asOptionalTriggerNameMarker(value, nameKey),
      ),
      requestResult<unknown>(catalog.get(idKey)).then((value) =>
        asOptionalTriggerIdMarker(value, idKey),
      ),
    ]);
    if (nameMarker !== undefined) throw new TypeError(`Trigger already exists: ${trigger.name}`);
    if (idMarker !== undefined) throw new TypeError(`Trigger ID already exists: ${trigger.id}`);
    catalog.put({ tableId: record.id, triggerId: trigger.id }, nameKey);
    catalog.put({ tableId: record.id, triggerName: trigger.name }, idKey);
  }
  if (record.uniqueKeyColumnId !== undefined) {
    catalog.put(
      { versions: [], hasBase: false } satisfies UniqueKeyChunkIndex,
      uniqueKeyChunkIndexKey(record.id),
    );
  }
  catalog.add(structuredClone(record), `${TABLE_ID_PREFIX}${record.id}`);
  catalog.add(record.id, `${TABLE_NAME_PREFIX}${record.name}`);
}

async function assertSegmentTargetsCurrentTable(
  catalog: IDBObjectStore,
  segment: SegmentRecord,
  pendingTable?: TableRecord,
): Promise<void> {
  const table =
    (await readDeclaredTable(catalog, segment.tableId)) ??
    (pendingTable?.id === segment.tableId ? pendingTable : undefined);
  if (table === undefined) throw new Error(`Segment ${segment.id} references missing table`);
  const columnIds = new Set(table.columns.map((column) => column.id));
  for (const columnId of Object.keys(segment.columnBlockIds)) {
    if (!columnIds.has(columnId)) {
      throw new Error(`Segment ${segment.id} references unknown column: ${columnId}`);
    }
  }
}

function assertIncomingSegmentHasColumns(segment: SegmentRecord): void {
  if (Object.keys(segment.columnBlockIds).length === 0) {
    throw new TypeError(`Segment ${segment.id} must contain at least one column block mapping`);
  }
}

async function assertDeclaredAutoIncrementColumn(
  catalog: IDBObjectStore,
  tableId: string,
  columnId: string,
): Promise<void> {
  validateId(tableId, "Table ID");
  validateId(columnId, "Column ID");
  const column = (await readDeclaredTable(catalog, tableId))?.columns.find(
    (entry) => entry.id === columnId,
  );
  if (column?.defaultValue?.kind !== "autoincrement") {
    throw new Error(`Auto-increment reservation has no declared column: ${tableId}/${columnId}`);
  }
}

function validateTempId(id: string, label: string): void {
  validateId(id, label);
}

function tempRunPageKey(page: TempRunPage): IDBValidKey {
  return ["run", page.ownerId, page.runId, page.pageIndex];
}

function tempGlobalQuotaKey(): IDBValidKey {
  return [TEMP_QUOTA, "global"];
}

function tempOwnerQuotaKey(ownerId: string): IDBValidKey {
  return [TEMP_QUOTA, "owner", ownerId];
}

function tempRunQuotaKey(ownerId: string, runId: string): IDBValidKey {
  return [TEMP_QUOTA, "run", ownerId, runId];
}

function emptyTempGlobalQuota(): TempGlobalQuotaRecord {
  return { ownerCount: 0, runCount: 0, pageCount: 0, retainedBytes: 0 };
}

function emptyTempOwnerQuota(): TempOwnerQuotaRecord {
  return { runCount: 0, pageCount: 0, retainedBytes: 0 };
}

function asTempQuotaCounts(
  value: unknown,
  fields: readonly string[],
  location: string,
): Record<string, number> {
  if (!isRecord(value)) throw corruption(location, "temp quota record is missing or invalid");
  assertKnownFields(value, fields, location);
  for (const field of fields) {
    const count = value[field];
    if (!Number.isSafeInteger(count) || (count as number) < 0) {
      throw corruption(location, `temp quota ${field} is invalid`);
    }
  }
  return value as Record<string, number>;
}

function asTempGlobalQuotaRecord(value: unknown): TempGlobalQuotaRecord {
  if (value === undefined) return emptyTempGlobalQuota();
  return asTempQuotaCounts(
    value,
    ["ownerCount", "runCount", "pageCount", "retainedBytes"],
    "temp quota/global",
  ) as unknown as TempGlobalQuotaRecord;
}

function asTempOwnerQuotaRecord(value: unknown, ownerId: string): TempOwnerQuotaRecord {
  return asTempQuotaCounts(
    value,
    ["runCount", "pageCount", "retainedBytes"],
    `temp quota/owner/${ownerId}`,
  ) as unknown as TempOwnerQuotaRecord;
}

function asTempRunQuotaRecord(
  value: unknown,
  ownerId: string,
  runId: string,
): TempRunQuotaRecord | undefined {
  if (value === undefined) return undefined;
  return asTempQuotaCounts(
    value,
    ["pageCount", "retainedBytes"],
    `temp quota/run/${ownerId}/${runId}`,
  ) as unknown as TempRunQuotaRecord;
}

async function putTempRunPagesWithQuota(
  store: IDBObjectStore,
  pages: readonly TempRunPage[],
): Promise<void> {
  const uniqueKeys = new Set<string>();
  const owners = new Map<
    string,
    { record: TempOwnerRecord; quota: TempOwnerQuotaRecord; runs: Map<string, TempRunQuotaRecord> }
  >();
  const global = asTempGlobalQuotaRecord(
    await requestResult<unknown>(store.get(tempGlobalQuotaKey())),
  );
  const nextGlobal = { ...global };
  for (const page of pages) {
    const identity = `${page.ownerId}\u0000${page.runId}\u0000${String(page.pageIndex)}`;
    if (uniqueKeys.has(identity)) throw new TypeError("Temp page batch repeats a page identity");
    uniqueKeys.add(identity);
    let owner = owners.get(page.ownerId);
    if (owner === undefined) {
      const ownerValue: unknown = await requestResult(store.get(tempOwnerKey(page.ownerId)));
      if (ownerValue === undefined) {
        throw new Error(`Temp run page has no owner: ${page.ownerId}`);
      }
      owner = {
        record: asTempOwnerRecord(ownerValue),
        quota: asTempOwnerQuotaRecord(
          await requestResult<unknown>(store.get(tempOwnerQuotaKey(page.ownerId))),
          page.ownerId,
        ),
        runs: new Map(),
      };
      owners.set(page.ownerId, owner);
    }
    let run = owner.runs.get(page.runId);
    if (run === undefined) {
      const stored = asTempRunQuotaRecord(
        await requestResult<unknown>(store.get(tempRunQuotaKey(page.ownerId, page.runId))),
        page.ownerId,
        page.runId,
      );
      run = stored ?? { pageCount: 0, retainedBytes: 0 };
      owner.runs.set(page.runId, run);
      if (stored === undefined) {
        owner.quota = {
          ...owner.quota,
          runCount: incrementSafeInteger(owner.quota.runCount, "Temp owner run count"),
        };
        nextGlobal.runCount = incrementSafeInteger(nextGlobal.runCount, "Global temp run count");
      }
    }
    const existingValue: unknown = await requestResult(store.get(tempRunPageKey(page)));
    const existingBytes =
      existingValue === undefined
        ? 0
        : asBytes(existingValue, `temp/${page.ownerId}/${page.runId}/${String(page.pageIndex)}`)
            .byteLength;
    const pageDelta = existingValue === undefined ? 1 : 0;
    const byteDelta = page.bytes.byteLength - existingBytes;
    run.pageCount += pageDelta;
    run.retainedBytes += byteDelta;
    owner.quota.pageCount += pageDelta;
    owner.quota.retainedBytes += byteDelta;
    nextGlobal.pageCount += pageDelta;
    nextGlobal.retainedBytes += byteDelta;
  }
  for (const [ownerId, owner] of owners) {
    if (owner.quota.runCount > MAX_TEMP_RUNS_PER_OWNER) {
      throw new StorageResourceLimitError(
        "temp run",
        owner.quota.runCount,
        MAX_TEMP_RUNS_PER_OWNER,
      );
    }
    if (owner.quota.pageCount > MAX_TEMP_PAGES_PER_OWNER) {
      throw new StorageResourceLimitError(
        "temp page",
        owner.quota.pageCount,
        MAX_TEMP_PAGES_PER_OWNER,
      );
    }
    if (owner.quota.retainedBytes > MAX_TEMP_BYTES_PER_OWNER) {
      throw new StorageResourceLimitError(
        "temp owner byte",
        owner.quota.retainedBytes,
        MAX_TEMP_BYTES_PER_OWNER,
      );
    }
    store.put(owner.quota, tempOwnerQuotaKey(ownerId));
    for (const [runId, run] of owner.runs) {
      store.put(run, tempRunQuotaKey(ownerId, runId));
    }
  }
  if (nextGlobal.runCount > MAX_TEMP_RUNS_TOTAL) {
    throw new StorageResourceLimitError(
      "temporary run total",
      nextGlobal.runCount,
      MAX_TEMP_RUNS_TOTAL,
    );
  }
  if (nextGlobal.pageCount > MAX_TEMP_PAGES_TOTAL) {
    throw new StorageResourceLimitError(
      "temporary page total",
      nextGlobal.pageCount,
      MAX_TEMP_PAGES_TOTAL,
    );
  }
  if (nextGlobal.retainedBytes > MAX_TEMP_BYTES_TOTAL) {
    throw new StorageResourceLimitError(
      "temporary byte",
      nextGlobal.retainedBytes,
      MAX_TEMP_BYTES_TOTAL,
    );
  }
  for (const page of pages) {
    store.put(compactStructuredCloneBytes(page.bytes, "Temp run page bytes"), tempRunPageKey(page));
  }
  store.put(nextGlobal, tempGlobalQuotaKey());
}

async function removeTempRunWithQuota(
  store: IDBObjectStore,
  ownerId: string,
  runId: string,
): Promise<void> {
  const run = asTempRunQuotaRecord(
    await requestResult<unknown>(store.get(tempRunQuotaKey(ownerId, runId))),
    ownerId,
    runId,
  );
  if (run === undefined) return;
  const owner = asTempOwnerQuotaRecord(
    await requestResult<unknown>(store.get(tempOwnerQuotaKey(ownerId))),
    ownerId,
  );
  const global = asTempGlobalQuotaRecord(
    await requestResult<unknown>(store.get(tempGlobalQuotaKey())),
  );
  await deleteTempRunPagePrefix(store, ownerId, runId);
  store.delete(tempRunQuotaKey(ownerId, runId));
  store.put(
    {
      runCount: owner.runCount - 1,
      pageCount: owner.pageCount - run.pageCount,
      retainedBytes: owner.retainedBytes - run.retainedBytes,
    },
    tempOwnerQuotaKey(ownerId),
  );
  store.put(
    {
      ...global,
      runCount: global.runCount - 1,
      pageCount: global.pageCount - run.pageCount,
      retainedBytes: global.retainedBytes - run.retainedBytes,
    },
    tempGlobalQuotaKey(),
  );
}

async function removeTempOwnerWithQuota(store: IDBObjectStore, ownerId: string): Promise<void> {
  const ownerValue: unknown = await requestResult(store.get(tempOwnerKey(ownerId)));
  if (ownerValue === undefined) return;
  asTempOwnerRecord(ownerValue);
  const owner = asTempOwnerQuotaRecord(
    await requestResult<unknown>(store.get(tempOwnerQuotaKey(ownerId))),
    ownerId,
  );
  const global = asTempGlobalQuotaRecord(
    await requestResult<unknown>(store.get(tempGlobalQuotaKey())),
  );
  await deleteTempRunPagePrefix(store, ownerId);
  await deleteTempRunQuotaPrefix(store, ownerId);
  store.delete(tempOwnerKey(ownerId));
  store.delete(tempOwnerQuotaKey(ownerId));
  store.put(
    {
      ownerCount: global.ownerCount - 1,
      runCount: global.runCount - owner.runCount,
      pageCount: global.pageCount - owner.pageCount,
      retainedBytes: global.retainedBytes - owner.retainedBytes,
    },
    tempGlobalQuotaKey(),
  );
}

/** Deletes only one compound-key prefix. The first cursor event jumps to the lower bound, so
 * cleanup cost is proportional to the selected owner's pages rather than the shared temp store. */
function deleteTempRunPagePrefix(
  store: IDBObjectStore,
  ownerId: string,
  runId?: string,
): Promise<void> {
  return new Promise((resolve, reject) => {
    const request = store.openCursor();
    request.onerror = () => reject(request.error ?? new Error("IndexedDB cursor failed"));
    request.onsuccess = () => {
      const cursor = request.result;
      if (cursor === null) {
        resolve();
        return;
      }
      const key = cursor.key;
      if (!Array.isArray(key) || (key[0] !== "owner" && key[0] !== "run")) {
        reject(corruption(`temp/${storageKeyLocation(key)}`, "record key is invalid"));
        return;
      }
      if (key[0] === "owner") {
        cursor.continue(runId === undefined ? ["run", ownerId] : ["run", ownerId, runId]);
        return;
      }
      if (typeof key[1] !== "string" || typeof key[2] !== "string") {
        reject(corruption(`temp/${storageKeyLocation(key)}`, "run page key is invalid"));
        return;
      }
      const ownerComparison = key[1] < ownerId ? -1 : key[1] > ownerId ? 1 : 0;
      const runComparison = runId === undefined ? 0 : key[2] < runId ? -1 : key[2] > runId ? 1 : 0;
      if (ownerComparison < 0 || (ownerComparison === 0 && runComparison < 0)) {
        cursor.continue(runId === undefined ? ["run", ownerId] : ["run", ownerId, runId]);
        return;
      }
      if (ownerComparison > 0 || (runId !== undefined && runComparison > 0)) {
        resolve();
        return;
      }
      cursor.delete();
      cursor.continue();
    };
  });
}

function tempRunHasAnyPage(
  store: IDBObjectStore,
  ownerId: string,
  runId: string,
): Promise<boolean> {
  return new Promise((resolve, reject) => {
    const request = store.openKeyCursor();
    let positioned = false;
    request.onerror = () => reject(request.error ?? new Error("IndexedDB cursor failed"));
    request.onsuccess = () => {
      const cursor = request.result;
      if (cursor === null) {
        resolve(false);
        return;
      }
      if (!positioned) {
        positioned = true;
        if (
          !Array.isArray(cursor.key) ||
          cursor.key[0] !== "run" ||
          cursor.key[1] !== ownerId ||
          cursor.key[2] !== runId
        ) {
          cursor.continue(["run", ownerId, runId]);
          return;
        }
      }
      resolve(
        Array.isArray(cursor.key) &&
          cursor.key[0] === "run" &&
          cursor.key[1] === ownerId &&
          cursor.key[2] === runId,
      );
    };
  });
}

function deleteTempRunQuotaPrefix(store: IDBObjectStore, ownerId: string): Promise<void> {
  return new Promise((resolve, reject) => {
    const request = store.openCursor();
    let positioned = false;
    request.onerror = () => reject(request.error ?? new Error("IndexedDB cursor failed"));
    request.onsuccess = () => {
      const cursor = request.result;
      if (cursor === null) {
        resolve();
        return;
      }
      const key = cursor.key;
      if (!positioned) {
        positioned = true;
        if (
          !Array.isArray(key) ||
          key[0] !== TEMP_QUOTA ||
          key[1] !== "run" ||
          key[2] !== ownerId
        ) {
          cursor.continue([TEMP_QUOTA, "run", ownerId]);
          return;
        }
      }
      if (!Array.isArray(key) || key[0] !== TEMP_QUOTA || key[1] !== "run" || key[2] !== ownerId) {
        resolve();
        return;
      }
      cursor.delete();
      cursor.continue();
    };
  });
}

function readTableSegmentPage(
  index: IDBIndex,
  tableId: string,
  afterId: string | null,
  limit: number,
): Promise<SegmentRecord[]> {
  return new Promise((resolve, reject) => {
    const records: SegmentRecord[] = [];
    const request = index.openCursor(tableId);
    request.onerror = () =>
      reject(request.error ?? new Error("IndexedDB table segment cursor failed"));
    request.onsuccess = () => {
      const cursor = request.result;
      if (cursor === null) {
        resolve(records);
        return;
      }
      try {
        if (typeof cursor.primaryKey !== "string") {
          throw corruption("segments", "table segment index key is invalid");
        }
        if (afterId !== null && cursor.primaryKey <= afterId) {
          cursor.continue();
          return;
        }
        const record = asSegmentRecord(cursor.value);
        if (record.id !== cursor.primaryKey || record.tableId !== tableId) {
          throw corruption(`segments/${record.id}`, "table segment index is inconsistent");
        }
        records.push(record);
        if (records.length === limit) resolve(records);
        else cursor.continue();
      } catch (error) {
        reject(error instanceof Error ? error : new Error(String(error)));
      }
    };
  });
}

function tempOwnerKey(ownerId: string): IDBValidKey {
  return ["owner", ownerId];
}

function validateTempOwnerRecord(record: TempOwnerRecord): void {
  validateTempId(record.ownerId, "Temp run owner ID");
  validateBoundedExpiration(
    record.createdAt,
    record.expiresAt,
    "Temp owner",
    MAX_TEMP_OWNER_TTL_MS,
  );
  if (record.revision !== 0) {
    throw new RangeError("Temp owner record must be created at revision zero");
  }
}

function asTempOwnerRecord(value: unknown): TempOwnerRecord {
  if (!isRecord(value)) throw corruption("temp/owner", "record is not an object");
  assertKnownFields(value, ["ownerId", "createdAt", "expiresAt", "revision"], "temp/owner");
  return {
    ownerId: nonEmptyStoredString(value.ownerId, "temp/owner/id"),
    createdAt: canonicalStoredTimestamp(value.createdAt, "temp/owner/createdAt"),
    expiresAt: canonicalStoredTimestamp(value.expiresAt, "temp/owner/expiresAt"),
    revision: nonNegativeStoredInteger(value.revision, "temp/owner/revision"),
  };
}

// Both scans run without IDBKeyRange (absent from the injected test factory environment): owner
// keys ["owner", id] sort before page keys ["run", id, runId, index], and an empty array sorts
// after every string, so ["run", id, []] jumps past one owner's remaining pages.
function readTempOwnerRecordIds(
  store: IDBObjectStore,
  afterOwnerId: string | null,
  max: number,
): Promise<string[]> {
  return new Promise((resolve, reject) => {
    const ownerIds: string[] = [];
    const request = store.openKeyCursor();
    request.onerror = () => reject(request.error ?? new Error("IndexedDB cursor failed"));
    request.onsuccess = () => {
      const cursor = request.result;
      if (cursor === null || ownerIds.length >= max) {
        resolve(ownerIds);
        return;
      }
      const key = cursor.key;
      if (!Array.isArray(key) || key[0] !== "owner" || typeof key[1] !== "string") {
        resolve(ownerIds);
        return;
      }
      if (afterOwnerId !== null && key[1] < afterOwnerId) {
        cursor.continue(["owner", afterOwnerId]);
        return;
      }
      if (afterOwnerId !== null && key[1] === afterOwnerId) {
        cursor.continue();
        return;
      }
      ownerIds.push(key[1]);
      if (ownerIds.length >= max) resolve(ownerIds);
      else cursor.continue();
    };
  });
}

function readTempPageOwnerIds(
  store: IDBObjectStore,
  afterOwnerId: string | null,
  max: number,
): Promise<string[]> {
  return new Promise((resolve, reject) => {
    const ownerIds: string[] = [];
    const request = store.openKeyCursor();
    request.onerror = () => reject(request.error ?? new Error("IndexedDB cursor failed"));
    request.onsuccess = () => {
      const cursor = request.result;
      if (cursor === null || ownerIds.length >= max) {
        resolve(ownerIds);
        return;
      }
      const key = cursor.key;
      if (Array.isArray(key) && key[0] === "owner") {
        cursor.continue(["run"]);
        return;
      }
      if (!Array.isArray(key) || key[0] !== "run" || typeof key[1] !== "string") {
        cursor.continue();
        return;
      }
      if (afterOwnerId !== null && key[1] <= afterOwnerId) {
        cursor.continue(["run", afterOwnerId, []]);
        return;
      }
      ownerIds.push(key[1]);
      if (ownerIds.length >= max) {
        resolve(ownerIds);
        return;
      }
      cursor.continue(["run", key[1], []]);
    };
  });
}

function uniqueKeyChunkIndexKey(tableId: string): IDBValidKey {
  return [UNIQUE_KEY_CHUNK_INDEX, tableId];
}

function uniqueKeyChunkKey(tableId: string, version: number, firstToken: string): IDBValidKey {
  return [UNIQUE_KEY_CHUNK, tableId, version, firstToken];
}

function uniqueKeyBasePartKey(generationId: string, firstToken: string): IDBValidKey {
  return [UNIQUE_KEY_BASE_PART, generationId, firstToken];
}

function uniqueKeyBuildKey(buildId: string): string {
  return `${UNIQUE_KEY_BUILD_PREFIX}${buildId}`;
}

function uniqueKeyBuildChunkKey(buildId: string, ordinal: number): IDBValidKey {
  return [UNIQUE_KEY_BUILD_CHUNK, buildId, ordinal];
}

function asUniqueKeyBuildEnvelope(value: unknown, location: string): UniqueKeyBuildEnvelope {
  if (!isRecord(value)) throw corruption(location, "UNIQUE build record is invalid");
  assertKnownFields(
    value,
    ["kind", "record", "cleanup", "activeBuildState", "activeExpiry", "buildId", "expiresAt"],
    location,
  );
  if (
    value.kind !== "unique-key-build" ||
    typeof value.cleanup !== "boolean" ||
    !isStorageId(value.buildId) ||
    typeof value.expiresAt !== "string" ||
    !isRecord(value.record)
  ) {
    throw corruption(location, "UNIQUE build envelope is invalid");
  }
  const recordValue = value.record;
  assertKnownFields(
    recordValue,
    [
      "buildId",
      "tableId",
      "indexId",
      "namespaceId",
      "ownerId",
      "state",
      "nextOrdinal",
      "tokenCount",
      "retainedBytes",
      "expiresAt",
      "createdAt",
      "updatedAt",
      "completedAt",
    ],
    `${location}/record`,
  );
  if (
    !isStorageId(recordValue.buildId) ||
    !isStorageId(recordValue.tableId) ||
    !isStorageId(recordValue.indexId) ||
    !isStorageId(recordValue.namespaceId) ||
    !isStorageId(recordValue.ownerId) ||
    (recordValue.state !== "active" && recordValue.state !== "completed") ||
    !isBoundedCursor(recordValue.nextOrdinal, Number.MAX_SAFE_INTEGER) ||
    !isBoundedCursor(recordValue.tokenCount, Number.MAX_SAFE_INTEGER) ||
    !isBoundedCursor(recordValue.retainedBytes, MAX_UNIQUE_KEY_BUILD_STAGED_BYTES) ||
    typeof recordValue.expiresAt !== "string" ||
    typeof recordValue.createdAt !== "string" ||
    typeof recordValue.updatedAt !== "string" ||
    (recordValue.completedAt !== undefined && typeof recordValue.completedAt !== "string")
  ) {
    throw corruption(location, "UNIQUE build record fields are invalid");
  }
  validStoredTimestamp(recordValue.expiresAt, `${location}/expiresAt`);
  validStoredTimestamp(recordValue.createdAt, `${location}/createdAt`);
  validStoredTimestamp(recordValue.updatedAt, `${location}/updatedAt`);
  if (recordValue.completedAt !== undefined) {
    validStoredTimestamp(recordValue.completedAt, `${location}/completedAt`);
  }
  const record = structuredClone(recordValue) as unknown as UniqueKeyBuildRecord;
  const active = record.state === "active" && !value.cleanup;
  if (
    value.buildId !== record.buildId ||
    value.expiresAt !== record.expiresAt ||
    (active
      ? value.activeBuildState !== "active" ||
        !Array.isArray(value.activeExpiry) ||
        value.activeExpiry.length !== 2 ||
        value.activeExpiry[0] !== record.expiresAt ||
        value.activeExpiry[1] !== record.buildId
      : value.activeBuildState !== undefined || value.activeExpiry !== undefined) ||
    (record.state === "active"
      ? record.completedAt !== undefined
      : record.completedAt === undefined || record.retainedBytes !== 0)
  ) {
    throw corruption(location, "UNIQUE build envelope state is inconsistent");
  }
  return {
    kind: "unique-key-build",
    record,
    cleanup: value.cleanup,
    ...(active
      ? {
          activeBuildState: "active" as const,
          activeExpiry: [record.expiresAt, record.buildId] as [string, string],
        }
      : {}),
    buildId: record.buildId,
    expiresAt: record.expiresAt,
  };
}

function readUniqueKeyBuildAdmission(
  catalog: IDBObjectStore,
): Promise<{ activeBuilds: number; retainedBytes: number }> {
  return new Promise((resolve, reject) => {
    let activeBuilds = 0;
    let retainedBytes = 0;
    const request = catalog.index(UNIQUE_KEY_BUILD_ACTIVE_INDEX).openCursor("active");
    request.onerror = () => reject(request.error ?? new Error("IndexedDB cursor failed"));
    request.onsuccess = () => {
      const cursor = request.result;
      if (cursor === null) {
        resolve({ activeBuilds, retainedBytes });
        return;
      }
      try {
        const envelope = asUniqueKeyBuildEnvelope(
          cursor.value,
          typeof cursor.primaryKey === "string" ? cursor.primaryKey : "catalog/UNIQUE-build",
        );
        if (envelope.record.state !== "active" || envelope.cleanup) {
          throw corruption("catalog/UNIQUE-build", "active-build index is inconsistent");
        }
        activeBuilds += 1;
        retainedBytes = safeByteSum(
          retainedBytes,
          envelope.record.retainedBytes,
          "Global UNIQUE build retained bytes",
        );
        cursor.continue();
      } catch (error) {
        reject(error instanceof Error ? error : new Error(String(error)));
      }
    };
  });
}

function firstExpiredUniqueKeyBuild(index: IDBIndex, cutoff: string): Promise<string | undefined> {
  return new Promise((resolve, reject) => {
    const request = index.openCursor();
    request.onerror = () => reject(request.error ?? new Error("IndexedDB cursor failed"));
    request.onsuccess = () => {
      const cursor = request.result;
      if (cursor === null) {
        resolve(undefined);
        return;
      }
      try {
        const location =
          typeof cursor.primaryKey === "string" ? cursor.primaryKey : "catalog/UNIQUE-build";
        const envelope = asUniqueKeyBuildEnvelope(cursor.value, location);
        if (envelope.record.expiresAt > cutoff) resolve(undefined);
        else resolve(envelope.record.buildId);
      } catch (error) {
        reject(error instanceof Error ? error : new Error(String(error)));
      }
    };
  });
}

async function assertUniqueKeyBuildChunkReplay(
  catalog: IDBObjectStore,
  input: AppendUniqueKeyBuildChunkInput,
): Promise<void> {
  const value: unknown = await requestResult(
    catalog.get(uniqueKeyBuildChunkKey(input.buildId, input.ordinal)),
  );
  if (
    !isRecord(value) ||
    !hasOnlyKnownFields(value, [
      "tokenCount",
      "retainedBytes",
      "firstToken",
      "lastToken",
      "partFirstTokens",
    ]) ||
    value.tokenCount !== input.keyTokens.length ||
    value.retainedBytes !== uniqueKeyBuildChunkRetainedBytes(input.keyTokens) ||
    value.firstToken !== input.keyTokens[0] ||
    value.lastToken !== input.keyTokens.at(-1) ||
    !Array.isArray(value.partFirstTokens) ||
    !value.partFirstTokens.every((token) => typeof token === "string")
  ) {
    throw new UniqueKeyBuildConflictError(input.buildId, "replayed chunk metadata differs");
  }
  const storedParts = await Promise.all(
    value.partFirstTokens.map((firstToken) =>
      requestResult<unknown>(catalog.get(uniqueKeyBasePartKey(input.buildId, firstToken))),
    ),
  );
  const storedTokens = storedParts.flatMap((part) => asBasePartition(part));
  if (
    storedTokens.length !== input.keyTokens.length ||
    storedTokens.some((token, index) => token !== input.keyTokens[index])
  ) {
    throw new UniqueKeyBuildConflictError(input.buildId, "replayed chunk tokens differ");
  }
}

function deleteUniqueKeyBuildArtifactsPage(
  catalog: IDBObjectStore,
  buildId: string,
): Promise<boolean> {
  return new Promise((resolve, reject) => {
    let phase: "base" | "chunks" = "base";
    let deleted = 0;
    const request = catalog.openCursor();
    request.onerror = () => reject(request.error ?? new Error("IndexedDB cursor failed"));
    request.onsuccess = () => {
      const cursor = request.result;
      if (cursor === null) {
        resolve(true);
        return;
      }
      const kind = phase === "base" ? UNIQUE_KEY_BASE_PART : UNIQUE_KEY_BUILD_CHUNK;
      const key = cursor.key;
      const comparison = compareStructuredPrefix(key, kind, buildId);
      if (comparison < 0) {
        cursor.continue([kind, buildId]);
        return;
      }
      if (comparison > 0) {
        if (phase === "base") {
          phase = "chunks";
          const chunkComparison = compareStructuredPrefix(key, UNIQUE_KEY_BUILD_CHUNK, buildId);
          if (chunkComparison < 0) {
            cursor.continue([UNIQUE_KEY_BUILD_CHUNK, buildId]);
            return;
          }
          if (chunkComparison > 0) {
            resolve(true);
            return;
          }
        } else {
          resolve(true);
          return;
        }
      }
      cursor.delete();
      deleted += 1;
      if (deleted === UNIQUE_KEY_BUILD_CLEANUP_PAGE) {
        resolve(false);
        return;
      }
      cursor.continue();
    };
  });
}

function compareStructuredPrefix(key: IDBValidKey, kind: string, id: string): -1 | 0 | 1 {
  if (!Array.isArray(key)) return -1;
  const keyKind = key[0];
  if (typeof keyKind !== "string") return 1;
  if (keyKind < kind) return -1;
  if (keyKind > kind) return 1;
  const keyId = key[1];
  if (typeof keyId !== "string") return 1;
  if (keyId < id) return -1;
  return keyId > id ? 1 : 0;
}

function compareStructuredKeyPrefix(
  key: IDBValidKey,
  prefix: ReadonlyArray<string | number>,
): -1 | 0 | 1 {
  if (!Array.isArray(key)) return -1;
  for (let index = 0; index < prefix.length; index += 1) {
    const left = key[index];
    const right = prefix[index];
    if (typeof left === "string" && typeof right === "string") {
      if (left < right) return -1;
      if (left > right) return 1;
    } else if (typeof left === "number" && typeof right === "number") {
      if (left < right) return -1;
      if (left > right) return 1;
    } else {
      return 1;
    }
  }
  return 0;
}

function compareSimpleStructuredKey(
  left: IDBValidKey,
  right: ReadonlyArray<string | number>,
): -1 | 0 | 1 {
  if (!Array.isArray(left)) return -1;
  const length = Math.min(left.length, right.length);
  for (let index = 0; index < length; index += 1) {
    const value = left[index];
    const expected = right[index];
    if (expected === undefined || typeof value !== typeof expected) return 1;
    if ((value as string | number) < expected) return -1;
    if ((value as string | number) > expected) return 1;
  }
  if (left.length < right.length) return -1;
  return left.length > right.length ? 1 : 0;
}

function compareStructuredKind(key: IDBValidKey, kind: string): -1 | 0 | 1 {
  if (!Array.isArray(key)) return -1;
  const keyKind = key[0];
  if (typeof keyKind !== "string") return 1;
  if (keyKind < kind) return -1;
  return keyKind > kind ? 1 : 0;
}

function readManifestBlockPageFromCatalog(
  catalog: IDBObjectStore,
  version: number,
  afterBlockId: string | null,
  limit: number,
): Promise<ManifestBlockRecord[]> {
  return new Promise((resolve, reject) => {
    const records: ManifestBlockRecord[] = [];
    const request = catalog.openCursor();
    let sought = false;
    request.onerror = () => reject(request.error ?? new Error("IndexedDB cursor failed"));
    request.onsuccess = () => {
      const cursor = request.result;
      if (cursor === null || records.length === limit) {
        resolve(records);
        return;
      }
      try {
        if (!sought) {
          sought = true;
          const seekKey: IDBValidKey =
            afterBlockId === null ? [MANIFEST_BLOCK] : [MANIFEST_BLOCK, afterBlockId];
          if (compareStructuredKind(cursor.key, MANIFEST_BLOCK) < 0) {
            cursor.continue(seekKey);
            return;
          }
        }
        if (compareStructuredKind(cursor.key, MANIFEST_BLOCK) !== 0) {
          resolve(records);
          return;
        }
        const key = cursor.key;
        if (!Array.isArray(key) || key.length !== 2 || typeof key[1] !== "string") {
          throw corruption(MANIFEST_BLOCK, "record key is invalid");
        }
        if (afterBlockId !== null && key[1] <= afterBlockId) {
          cursor.continue();
          return;
        }
        const record = asManifestBlockRecord(cursor.value, key[1]);
        if (manifestBlockVisibleAt(record, version)) records.push(record);
        if (records.length === limit) resolve(records);
        else cursor.continue();
      } catch (error) {
        reject(error instanceof Error ? error : new Error(String(error)));
      }
    };
  });
}

function readRetiredManifestBlockPageFromCatalog(
  catalog: IDBObjectStore,
  removedThroughVersion: number,
  afterBlockId: string | null,
  limit: number,
): Promise<ManifestBlockRecord[]> {
  return new Promise((resolve, reject) => {
    const records: ManifestBlockRecord[] = [];
    const request = catalog.openCursor();
    let sought = false;
    request.onerror = () => reject(request.error ?? new Error("IndexedDB cursor failed"));
    request.onsuccess = () => {
      const cursor = request.result;
      if (cursor === null || records.length === limit) {
        resolve(records);
        return;
      }
      try {
        if (!sought) {
          sought = true;
          const seekKey: IDBValidKey =
            afterBlockId === null ? [MANIFEST_BLOCK] : [MANIFEST_BLOCK, afterBlockId];
          if (compareStructuredKind(cursor.key, MANIFEST_BLOCK) < 0) {
            cursor.continue(seekKey);
            return;
          }
        }
        if (compareStructuredKind(cursor.key, MANIFEST_BLOCK) !== 0) {
          resolve(records);
          return;
        }
        const key = cursor.key;
        if (!Array.isArray(key) || key.length !== 2 || typeof key[1] !== "string") {
          throw corruption(MANIFEST_BLOCK, "record key is invalid");
        }
        if (afterBlockId !== null && key[1] <= afterBlockId) {
          cursor.continue();
          return;
        }
        const record = asManifestBlockRecord(cursor.value, key[1]);
        if (record.removedVersion !== null && record.removedVersion <= removedThroughVersion) {
          records.push(record);
        }
        if (records.length === limit) resolve(records);
        else cursor.continue();
      } catch (error) {
        reject(error instanceof Error ? error : new Error(String(error)));
      }
    };
  });
}

async function publishManifestBlockDeltaInTransaction(
  catalog: IDBObjectStore,
  version: number,
  addedBlockIds: readonly string[],
  addedMetadata: readonly StoredBlockMetadata[],
  removedBlockIds: readonly string[],
): Promise<void> {
  if (addedBlockIds.length !== addedMetadata.length) {
    throw new TypeError("Manifest additions and block metadata disagree");
  }
  const previousVersion = version - 1;
  const existingAdds = await Promise.all(
    addedBlockIds.map((id) => requestResult<unknown>(catalog.get(manifestBlockKey(id)))),
  );
  const existingRemovals = await Promise.all(
    removedBlockIds.map((id) => requestResult<unknown>(catalog.get(manifestBlockKey(id)))),
  );
  for (const [index, value] of existingAdds.entries()) {
    const id = addedBlockIds[index] ?? "";
    if (value !== undefined) {
      asManifestBlockRecord(value, id);
      throw corruption(`${MANIFEST_BLOCK}/${id}`, "retired block ID was reused");
    }
  }
  const removalRecords = existingRemovals.map((value, index) => {
    const id = removedBlockIds[index] ?? "";
    if (value === undefined) {
      throw corruption(`${MANIFEST_BLOCK}/${id}`, "retired block has no provenance");
    }
    const record = asManifestBlockRecord(value, id);
    if (record.removedVersion !== null || !manifestBlockVisibleAt(record, previousVersion)) {
      throw corruption(`${MANIFEST_BLOCK}/${id}`, "retired block is not live");
    }
    return record;
  });
  for (const [index, id] of addedBlockIds.entries()) {
    const metadata = addedMetadata[index];
    if (metadata === undefined) throw new TypeError("Manifest block metadata is missing");
    catalog.add(
      {
        blockId: id,
        byteLength: metadata.byteLength,
        checksum: metadata.checksum,
        addedVersion: version,
        removedVersion: null,
      } satisfies ManifestBlockRecord,
      manifestBlockKey(id),
    );
  }
  for (const record of removalRecords) {
    catalog.put(
      { ...record, removedVersion: version } satisfies ManifestBlockRecord,
      manifestBlockKey(record.blockId),
    );
  }
}

async function retireManifestBlocksInTransaction(
  catalog: IDBObjectStore,
  candidateIds: readonly string[],
  currentVersion: number | null,
  newVersion: number,
): Promise<{ count: number; bytes: number; ids: string[] }> {
  if (currentVersion === null) return { count: 0, bytes: 0, ids: [] };
  const values = await Promise.all(
    candidateIds.map((id) => requestResult<unknown>(catalog.get(manifestBlockKey(id)))),
  );
  let count = 0;
  let bytes = 0;
  const ids: string[] = [];
  for (const [index, value] of values.entries()) {
    const id = candidateIds[index] ?? "";
    if (value === undefined) continue;
    const record = asManifestBlockRecord(value, id);
    if (!manifestBlockVisibleAt(record, currentVersion)) continue;
    if (record.removedVersion !== null) {
      throw corruption(`${MANIFEST_BLOCK}/${id}`, "visible block is already retired");
    }
    count += 1;
    ids.push(id);
    bytes = safeByteSum(bytes, record.byteLength, "Retired manifest block bytes");
    catalog.put(
      { ...record, removedVersion: newVersion } satisfies ManifestBlockRecord,
      manifestBlockKey(id),
    );
  }
  return { count, bytes, ids };
}

function uniqueKeyBuildEnvelope(
  record: UniqueKeyBuildRecord,
  cleanup = false,
): UniqueKeyBuildEnvelope {
  const active = record.state === "active" && !cleanup;
  return {
    kind: "unique-key-build",
    record: structuredClone(record),
    cleanup,
    ...(active
      ? {
          activeBuildState: "active" as const,
          activeExpiry: [record.expiresAt, record.buildId] as [string, string],
        }
      : {}),
    buildId: record.buildId,
    expiresAt: record.expiresAt,
  };
}

/** Replaces or removes one complete unique-membership namespace inside the caller's transaction. */
async function replaceUniqueKeyMembership(
  store: IDBObjectStore,
  namespaceId: string,
  keyTokens: readonly string[] | Set<string>,
  retainEmpty: boolean,
): Promise<void> {
  const raw = await requestResult<unknown>(store.get(uniqueKeyChunkIndexKey(namespaceId)));
  const previous = raw === undefined ? undefined : asUniqueKeyChunkIndex(raw);
  if (previous?.baseGenerationId !== undefined) {
    await deleteUniqueKeyPartPrefix(store, [UNIQUE_KEY_BASE_PART, previous.baseGenerationId]);
  }
  for (const version of previous?.versions ?? []) {
    await deleteUniqueKeyPartPrefix(store, [UNIQUE_KEY_CHUNK, namespaceId, version]);
  }
  const tokenCount = keyTokens instanceof Set ? keyTokens.size : keyTokens.length;
  if (!retainEmpty && tokenCount === 0) {
    store.delete(uniqueKeyChunkIndexKey(namespaceId));
    return;
  }
  if (tokenCount === 0) {
    store.put(
      { versions: [], hasBase: false } satisfies UniqueKeyChunkIndex,
      uniqueKeyChunkIndexKey(namespaceId),
    );
    return;
  }
  const generationId = `unique-base/${crypto.randomUUID()}`;
  const sortedTokens = [...keyTokens].sort();
  writeUniqueKeyBaseParts(store, generationId, sortedTokens);
  store.put(
    { versions: [], hasBase: true, baseGenerationId: generationId, tokenCount },
    uniqueKeyChunkIndexKey(namespaceId),
  );
}

/**
 * Resolves which of the requested tokens exist: point probes against the folded base records,
 * then the bounded chunk tail applied in commit order (an add in a later chunk revives a token
 * a base fold removed, a removal hides a base token). The full tail chunks return with the
 * answer so a folding commit can replay them without re-reading.
 */
interface UniqueKeyChunkIndex {
  versions: number[];
  /** True once a fold has written an immutable, lexically chunked base generation. */
  hasBase: boolean;
  tokenCount?: number;
  /** Ordered base-part generation; required whenever `hasBase` is true. */
  baseGenerationId?: string;
}

function asUniqueKeyChunkIndex(value: unknown): UniqueKeyChunkIndex {
  if (!isRecord(value)) {
    throw corruption(UNIQUE_KEY_CHUNK_INDEX, "membership index is missing or invalid");
  }
  assertKnownFields(
    value,
    ["versions", "hasBase", "tokenCount", "baseGenerationId"],
    UNIQUE_KEY_CHUNK_INDEX,
  );
  const versions = value.versions;
  if (
    !Array.isArray(versions) ||
    !versions.every((entry) => Number.isSafeInteger(entry) && entry >= 0) ||
    new Set(versions).size !== versions.length ||
    versions.some((entry, index) => index > 0 && entry <= (versions[index - 1] ?? -1)) ||
    versions.length > UNIQUE_KEY_TAIL_CHUNK_LIMIT
  ) {
    throw corruption(UNIQUE_KEY_CHUNK_INDEX, "tail versions are not canonical");
  }
  if (typeof value.hasBase !== "boolean") {
    throw corruption(UNIQUE_KEY_CHUNK_INDEX, "base-presence flag is invalid");
  }
  const baseGenerationId = isStorageId(value.baseGenerationId) ? value.baseGenerationId : undefined;
  let baseFields: Pick<UniqueKeyChunkIndex, "baseGenerationId" | "tokenCount"> = {};
  if (value.hasBase) {
    if (
      baseGenerationId === undefined ||
      !Number.isSafeInteger(value.tokenCount) ||
      (value.tokenCount as number) < 0
    ) {
      throw corruption(UNIQUE_KEY_CHUNK_INDEX, "ordered base metadata is invalid");
    }
    baseFields = { baseGenerationId, tokenCount: value.tokenCount as number };
  } else if (value.tokenCount !== undefined || value.baseGenerationId !== undefined) {
    throw corruption(UNIQUE_KEY_CHUNK_INDEX, "base metadata exists without a base");
  }
  return {
    versions: [...(versions as number[])],
    hasBase: value.hasBase,
    ...baseFields,
  };
}

function uniqueKeyBaseIndexFields(
  index: UniqueKeyChunkIndex,
): Pick<UniqueKeyChunkIndex, "tokenCount" | "baseGenerationId"> {
  return {
    ...(index.tokenCount === undefined ? {} : { tokenCount: index.tokenCount }),
    ...(index.baseGenerationId === undefined ? {} : { baseGenerationId: index.baseGenerationId }),
  };
}

/** Applies one tail chunk's membership delta in place, in the order commits recorded it. */
function applyChunk(present: Set<string>, chunk: UniqueKeyChunk): void {
  for (const token of chunk.addedTokens) present.add(token);
  for (const token of chunk.removedTokens) present.delete(token);
}

/**
 * Conservative retained-size model for the optional complete-membership cache. Strings are the
 * dominant payload; the fixed allowance covers Set buckets, array slots, and chunk objects.
 * This is deliberately not the on-disk size — it models the JavaScript graph we keep alive.
 */
function uniqueKeyCacheRetainedBytes(
  present: ReadonlySet<string>,
  chunks: readonly UniqueKeyChunk[],
): number {
  let bytes = 256;
  for (const token of present) bytes += 32 + token.length * 2;
  for (const chunk of chunks) {
    bytes += 64;
    // Count the strings again even when an added token is also in `present`. Engines may share
    // the string allocation, but a budget must not depend on that implementation detail; removed
    // tokens are retained only by these arrays and can be arbitrarily long.
    for (const token of chunk.addedTokens) bytes += 8 + token.length * 2;
    for (const token of chunk.removedTokens) bytes += 8 + token.length * 2;
  }
  return bytes;
}

function asBasePartition(value: unknown): string[] {
  if (
    !isStringArray(value) ||
    value.some((token) => token.length === 0) ||
    value.length === 0 ||
    value.length > UNIQUE_KEY_MEMBERSHIP_PART_TOKENS ||
    value.some((token, index) => index > 0 && token <= (value[index - 1] ?? ""))
  ) {
    throw corruption(UNIQUE_KEY_BASE_PART, "ordered base part is invalid");
  }
  let retainedBytes = 0;
  for (const token of value) {
    retainedBytes = safeByteSum(
      retainedBytes,
      16 + token.length * 2,
      "UNIQUE membership part bytes",
    );
    if (retainedBytes > UNIQUE_KEY_MEMBERSHIP_PART_RETAINED_BYTES) {
      throw corruption(UNIQUE_KEY_BASE_PART, "ordered base part exceeds its byte limit");
    }
  }
  return [...value];
}

function uniqueMembershipTokenRetainedBytes(token: string): number {
  // Reuse the public mutation validator so durable base/tail records accept exactly the same
  // canonical token domain as streamed UNIQUE builds.
  return uniqueKeyBuildChunkRetainedBytes([token]);
}

function splitUniqueMembershipTokens(tokens: readonly string[]): string[][] {
  const parts: string[][] = [];
  let part: string[] = [];
  let retainedBytes = 0;
  for (const token of tokens) {
    const tokenBytes = uniqueMembershipTokenRetainedBytes(token);
    if (
      part.length === UNIQUE_KEY_MEMBERSHIP_PART_TOKENS ||
      retainedBytes > UNIQUE_KEY_MEMBERSHIP_PART_RETAINED_BYTES - tokenBytes
    ) {
      parts.push(part);
      part = [];
      retainedBytes = 0;
    }
    part.push(token);
    retainedBytes += tokenBytes;
  }
  if (part.length > 0) parts.push(part);
  return parts;
}

function canonicalUniqueKeyChunk(chunk: UniqueKeyChunk): UniqueKeyChunk {
  const states = new Map<string, boolean>();
  for (const token of chunk.addedTokens) {
    uniqueMembershipTokenRetainedBytes(token);
    if (states.has(token)) throw new TypeError("UNIQUE tail chunk repeats a token");
    states.set(token, true);
  }
  for (const token of chunk.removedTokens) {
    uniqueMembershipTokenRetainedBytes(token);
    if (states.has(token)) throw new TypeError("UNIQUE tail chunk repeats a token");
    states.set(token, false);
  }
  const entries = [...states].sort(([left], [right]) => (left < right ? -1 : left > right ? 1 : 0));
  return {
    addedTokens: entries.flatMap(([token, present]) => (present ? [token] : [])),
    removedTokens: entries.flatMap(([token, present]) => (present ? [] : [token])),
  };
}

function splitUniqueKeyChunk(chunk: UniqueKeyChunk): UniqueKeyChunk[] {
  const canonical = canonicalUniqueKeyChunk(chunk);
  const states = [
    ...canonical.addedTokens.map((token) => [token, true] as const),
    ...canonical.removedTokens.map((token) => [token, false] as const),
  ].sort(([left], [right]) => (left < right ? -1 : left > right ? 1 : 0));
  const parts: UniqueKeyChunk[] = [];
  let entries: Array<readonly [string, boolean]> = [];
  let retainedBytes = 0;
  for (const entry of states) {
    const tokenBytes = uniqueMembershipTokenRetainedBytes(entry[0]);
    if (
      entries.length === UNIQUE_KEY_MEMBERSHIP_PART_TOKENS ||
      retainedBytes > UNIQUE_KEY_MEMBERSHIP_PART_RETAINED_BYTES - tokenBytes
    ) {
      parts.push({
        addedTokens: entries.flatMap(([token, present]) => (present ? [token] : [])),
        removedTokens: entries.flatMap(([token, present]) => (present ? [] : [token])),
      });
      entries = [];
      retainedBytes = 0;
    }
    entries.push(entry);
    retainedBytes += tokenBytes;
  }
  if (entries.length > 0) {
    parts.push({
      addedTokens: entries.flatMap(([token, present]) => (present ? [token] : [])),
      removedTokens: entries.flatMap(([token, present]) => (present ? [] : [token])),
    });
  }
  return parts;
}

function uniqueChunkFirstToken(chunk: UniqueKeyChunk): string {
  const added = chunk.addedTokens[0];
  const removed = chunk.removedTokens[0];
  if (added === undefined) return removed ?? "";
  if (removed === undefined) return added;
  return added < removed ? added : removed;
}

function writeUniqueKeyTailParts(
  store: IDBObjectStore,
  namespaceId: string,
  version: number,
  chunk: UniqueKeyChunk,
): UniqueKeyChunk[] {
  const parts = splitUniqueKeyChunk(chunk);
  for (const part of parts) {
    store.put(part, uniqueKeyChunkKey(namespaceId, version, uniqueChunkFirstToken(part)));
  }
  return parts;
}

function writeUniqueKeyBaseParts(
  store: IDBObjectStore,
  generationId: string,
  sortedTokens: readonly string[],
): void {
  for (const part of splitUniqueMembershipTokens(sortedTokens)) {
    store.put(part, uniqueKeyBasePartKey(generationId, part[0] ?? ""));
  }
}

function deleteUniqueKeyPartPrefix(
  store: IDBObjectStore,
  prefix: ReadonlyArray<string | number>,
): Promise<void> {
  return new Promise((resolve, reject) => {
    const request = store.openCursor();
    let positioned = false;
    request.onerror = () => reject(request.error ?? new Error("IndexedDB cursor failed"));
    request.onsuccess = () => {
      const cursor = request.result;
      if (cursor === null) {
        resolve();
        return;
      }
      if (!positioned) {
        positioned = true;
        if (compareStructuredKeyPrefix(cursor.key, prefix) < 0) {
          cursor.continue([...prefix]);
          return;
        }
      }
      if (compareStructuredKeyPrefix(cursor.key, prefix) !== 0) {
        resolve();
        return;
      }
      cursor.delete();
      cursor.continue();
    };
  });
}

function readUniqueKeyTailParts(
  store: IDBObjectStore,
  namespaceId: string,
  version: number,
): Promise<UniqueKeyChunk[]> {
  return new Promise((resolve, reject) => {
    const chunks: UniqueKeyChunk[] = [];
    const request = store.openCursor();
    let positioned = false;
    let previousLast: string | undefined;
    request.onerror = () => reject(request.error ?? new Error("IndexedDB cursor failed"));
    request.onsuccess = () => {
      const cursor = request.result;
      if (cursor === null) {
        resolve(chunks);
        return;
      }
      const prefix = [UNIQUE_KEY_CHUNK, namespaceId, version] as const;
      if (!positioned) {
        positioned = true;
        if (compareStructuredKeyPrefix(cursor.key, prefix) < 0) {
          cursor.continue([...prefix]);
          return;
        }
      }
      if (compareStructuredKeyPrefix(cursor.key, prefix) !== 0) {
        resolve(chunks);
        return;
      }
      try {
        const chunk = asUniqueKeyChunk(cursor.value);
        const first = uniqueChunkFirstToken(chunk);
        const key = cursor.key;
        if (!Array.isArray(key) || key.length !== 4 || key[3] !== first) {
          throw corruption(UNIQUE_KEY_CHUNK, "tail part boundary differs from its key");
        }
        const addedLast = chunk.addedTokens.at(-1);
        const removedLast = chunk.removedTokens.at(-1);
        const last =
          addedLast === undefined
            ? (removedLast ?? "")
            : removedLast === undefined || addedLast > removedLast
              ? addedLast
              : removedLast;
        if (previousLast !== undefined && first <= previousLast) {
          throw corruption(UNIQUE_KEY_CHUNK, "tail parts overlap or are out of order");
        }
        previousLast = last;
        chunks.push(chunk);
        cursor.continue();
      } catch (error) {
        reject(error instanceof Error ? error : new Error(String(error)));
      }
    };
  });
}

class UniqueMembershipPartSource {
  #cursor: IDBCursorWithValue | null = null;
  #baseTokens: string[] = [];
  #addedTokens: string[] = [];
  #removedTokens: string[] = [];
  #basePosition = 0;
  #addedPosition = 0;
  #removedPosition = 0;
  #previousLast: string | undefined;
  #done = false;
  #request: IDBRequest<IDBCursorWithValue | null> | undefined;

  token: string | undefined;
  present = false;
  retainedBytes = 0;
  partCount = 0;

  constructor(
    readonly store: IDBObjectStore,
    readonly prefix: ReadonlyArray<string | number>,
    readonly version: number,
    readonly kind: "base" | "tail",
  ) {}

  async initialize(): Promise<void> {
    this.#request = this.store.openCursor();
    await this.#readCursor(true);
    this.#selectToken();
  }

  async advance(): Promise<void> {
    if (this.token === undefined) return;
    if (this.kind === "base") this.#basePosition += 1;
    else if (this.addedTokens[this.#addedPosition] === this.token) this.#addedPosition += 1;
    else this.#removedPosition += 1;
    if (this.#partExhausted()) await this.#readCursor(false);
    this.#selectToken();
  }

  get addedTokens(): readonly string[] {
    return this.#addedTokens;
  }

  async #nextRequest(): Promise<IDBCursorWithValue | null> {
    const request = this.#request;
    if (request === undefined) throw new Error("UNIQUE membership source is not initialized");
    return new Promise((resolve, reject) => {
      request.onerror = () => reject(request.error ?? new Error("IndexedDB cursor failed"));
      request.onsuccess = () => resolve(request.result);
    });
  }

  async #readCursor(initial: boolean): Promise<void> {
    if (this.#done) return;
    if (!initial) this.#cursor?.continue();
    let cursor = await this.#nextRequest();
    if (initial && cursor !== null && compareStructuredKeyPrefix(cursor.key, this.prefix) < 0) {
      cursor.continue([...this.prefix]);
      cursor = await this.#nextRequest();
    }
    if (cursor === null || compareStructuredKeyPrefix(cursor.key, this.prefix) !== 0) {
      this.#done = true;
      this.#cursor = null;
      this.#baseTokens = [];
      this.#addedTokens = [];
      this.#removedTokens = [];
      this.retainedBytes = 0;
      return;
    }
    this.#cursor = cursor;
    this.partCount = incrementSafeInteger(this.partCount, "UNIQUE membership part count");
    if (this.kind === "base") {
      this.#baseTokens = asBasePartition(cursor.value);
      this.#addedTokens = [];
      this.#removedTokens = [];
      this.#basePosition = 0;
    } else {
      const chunk = asUniqueKeyChunk(cursor.value);
      this.#baseTokens = [];
      this.#addedTokens = chunk.addedTokens;
      this.#removedTokens = chunk.removedTokens;
      this.#addedPosition = 0;
      this.#removedPosition = 0;
    }
    const first =
      this.kind === "base"
        ? (this.#baseTokens[0] ?? "")
        : uniqueChunkFirstToken({
            addedTokens: this.#addedTokens,
            removedTokens: this.#removedTokens,
          });
    const key = cursor.key;
    if (!Array.isArray(key) || key.at(-1) !== first) {
      throw corruption(UNIQUE_KEY_CHUNK, "ordered membership boundary differs from its key");
    }
    if (this.#previousLast !== undefined && first <= this.#previousLast) {
      throw corruption(UNIQUE_KEY_CHUNK, "ordered membership parts overlap");
    }
    const addedLast = this.#addedTokens.at(-1);
    const removedLast = this.#removedTokens.at(-1);
    this.#previousLast =
      this.kind === "base"
        ? this.#baseTokens.at(-1)
        : addedLast === undefined
          ? removedLast
          : removedLast === undefined || addedLast > removedLast
            ? addedLast
            : removedLast;
    this.retainedBytes = 0;
    for (const token of this.#baseTokens) {
      this.retainedBytes = safeByteSum(
        this.retainedBytes,
        uniqueMembershipTokenRetainedBytes(token),
        "UNIQUE part bytes",
      );
    }
    for (const token of this.#addedTokens) {
      this.retainedBytes = safeByteSum(
        this.retainedBytes,
        uniqueMembershipTokenRetainedBytes(token),
        "UNIQUE part bytes",
      );
    }
    for (const token of this.#removedTokens) {
      this.retainedBytes = safeByteSum(
        this.retainedBytes,
        uniqueMembershipTokenRetainedBytes(token),
        "UNIQUE part bytes",
      );
    }
  }

  #partExhausted(): boolean {
    return this.kind === "base"
      ? this.#basePosition >= this.#baseTokens.length
      : this.#addedPosition >= this.#addedTokens.length &&
          this.#removedPosition >= this.#removedTokens.length;
  }

  #selectToken(): void {
    if (this.#done || this.#partExhausted()) {
      this.token = undefined;
      return;
    }
    if (this.kind === "base") {
      this.token = this.#baseTokens[this.#basePosition];
      this.present = true;
      return;
    }
    const added = this.#addedTokens[this.#addedPosition];
    const removed = this.#removedTokens[this.#removedPosition];
    if (removed === undefined || (added !== undefined && added < removed)) {
      this.token = added;
      this.present = true;
    } else {
      this.token = removed;
      this.present = false;
    }
  }
}

async function visitCanonicalUniqueKeyTokens(
  store: IDBObjectStore,
  namespaceId: string,
  visit: (token: string, retainedSourceBytes: number) => void,
  suppliedIndex?: UniqueKeyChunkIndex,
): Promise<{ tokenCount: number; generationId: string }> {
  const index =
    suppliedIndex ??
    asUniqueKeyChunkIndex(
      await requestResult<unknown>(store.get(uniqueKeyChunkIndexKey(namespaceId))),
    );
  const generationId = index.baseGenerationId ?? namespaceId;
  const sources: UniqueMembershipPartSource[] = [];
  if (index.hasBase) {
    sources.push(
      new UniqueMembershipPartSource(store, [UNIQUE_KEY_BASE_PART, generationId], -1, "base"),
    );
  }
  for (const version of index.versions) {
    sources.push(
      new UniqueMembershipPartSource(
        store,
        [UNIQUE_KEY_CHUNK, namespaceId, version],
        version,
        "tail",
      ),
    );
  }
  // Initialize and advance sources serially. That prevents two decoded parts per source from
  // coexisting while a cursor request resolves, so the modeled peak is exactly one bounded part
  // for each of the base + at most sixteen tail sources.
  for (const source of sources) await source.initialize();
  const missingSource = sources.find((source) => source.partCount === 0);
  if (missingSource !== undefined) {
    throw corruption(
      missingSource.kind === "base" ? UNIQUE_KEY_BASE_PART : `${UNIQUE_KEY_CHUNK}/${namespaceId}`,
      `${missingSource.kind} membership source has no parts`,
    );
  }
  let tokenCount = 0;
  for (;;) {
    let token: string | undefined;
    for (const source of sources) {
      if (source.token !== undefined && (token === undefined || source.token < token)) {
        token = source.token;
      }
    }
    if (token === undefined) break;
    let winner: UniqueMembershipPartSource | undefined;
    const matching: UniqueMembershipPartSource[] = [];
    for (const source of sources) {
      if (source.token !== token) continue;
      matching.push(source);
      if (winner === undefined || source.version > winner.version) winner = source;
    }
    if (winner?.present === true) {
      tokenCount = incrementSafeInteger(tokenCount, "Snapshot UNIQUE token count");
      visit(
        token,
        sources.reduce(
          (total, source) =>
            safeByteSum(total, source.retainedBytes, "Snapshot UNIQUE source bytes"),
          0,
        ),
      );
    }
    for (const source of matching) await source.advance();
  }
  return { tokenCount, generationId };
}

/**
 * Folds base + sixteen durable tails + this commit into a new immutable generation. The k-way
 * reader retains one 2 MiB part per source and the writer retains one 2 MiB output part, for a
 * fixed conservative 36 MiB peak regardless of namespace cardinality.
 */
async function foldUniqueMembershipGeneration(
  store: IDBObjectStore,
  namespaceId: string,
  index: UniqueKeyChunkIndex,
  version: number,
  pending: UniqueKeyChunk,
): Promise<UniqueKeyChunkIndex> {
  writeUniqueKeyTailParts(store, namespaceId, version, pending);
  const inputIndex: UniqueKeyChunkIndex = {
    versions: [...index.versions, version],
    hasBase: index.hasBase,
    ...uniqueKeyBaseIndexFields(index),
  };
  const generationId = `unique-base/${crypto.randomUUID()}`;
  let output: string[] = [];
  let outputBytes = 0;
  const flush = (): void => {
    if (output.length === 0) return;
    store.put(output, uniqueKeyBasePartKey(generationId, output[0] ?? ""));
    output = [];
    outputBytes = 0;
  };
  const result = await visitCanonicalUniqueKeyTokens(
    store,
    namespaceId,
    (token) => {
      const tokenBytes = uniqueMembershipTokenRetainedBytes(token);
      if (
        output.length === UNIQUE_KEY_MEMBERSHIP_PART_TOKENS ||
        outputBytes > UNIQUE_KEY_MEMBERSHIP_PART_RETAINED_BYTES - tokenBytes
      ) {
        flush();
      }
      output.push(token);
      outputBytes += tokenBytes;
    },
    inputIndex,
  );
  flush();

  if (index.baseGenerationId !== undefined) {
    await deleteUniqueKeyPartPrefix(store, [UNIQUE_KEY_BASE_PART, index.baseGenerationId]);
  }
  for (const tailVersion of inputIndex.versions) {
    await deleteUniqueKeyPartPrefix(store, [UNIQUE_KEY_CHUNK, namespaceId, tailVersion]);
  }
  const folded: UniqueKeyChunkIndex =
    result.tokenCount === 0
      ? { versions: [], hasBase: false }
      : {
          versions: [],
          hasBase: true,
          baseGenerationId: generationId,
          tokenCount: result.tokenCount,
        };
  store.put(folded, uniqueKeyChunkIndexKey(namespaceId));
  return folded;
}

function validateUniqueKeyGenerationTokenCount(
  store: IDBObjectStore,
  generationId: string,
  expectedCount: number,
): Promise<void> {
  return new Promise((resolve, reject) => {
    const request = store.openCursor();
    let positioned = false;
    let count = 0;
    let previousLast: string | undefined;
    request.onerror = () => reject(request.error ?? new Error("IndexedDB cursor failed"));
    request.onsuccess = () => {
      const cursor = request.result;
      if (cursor === null) {
        if (count === expectedCount) resolve();
        else reject(corruption(generationId, "UNIQUE generation token count is inconsistent"));
        return;
      }
      if (!positioned) {
        positioned = true;
        if (compareStructuredPrefix(cursor.key, UNIQUE_KEY_BASE_PART, generationId) < 0) {
          cursor.continue([UNIQUE_KEY_BASE_PART, generationId]);
          return;
        }
      }
      if (compareStructuredPrefix(cursor.key, UNIQUE_KEY_BASE_PART, generationId) !== 0) {
        if (count === expectedCount) resolve();
        else reject(corruption(generationId, "UNIQUE generation token count is inconsistent"));
        return;
      }
      const part = asBasePartition(cursor.value);
      const first = part[0] ?? "";
      if (!Array.isArray(cursor.key) || cursor.key.length !== 3 || cursor.key[2] !== first) {
        reject(corruption(generationId, "UNIQUE base boundary differs from its key"));
        return;
      }
      if (previousLast !== undefined && first <= previousLast) {
        reject(corruption(generationId, "UNIQUE base parts overlap or are out of order"));
        return;
      }
      previousLast = part.at(-1);
      count = safeByteSum(count, part.length, "UNIQUE generation token count");
      cursor.continue();
    };
  });
}

/**
 * Applies one ordered base/tail source to only the requested tokens. A single reverse cursor
 * walks predecessor boundaries in descending token order, so the operation retains one decoded
 * 2 MiB part and issues at most one request per distinct touched part rather than loading the
 * source's complete generation.
 */
function applyUniqueMembershipSourceToRequested(
  store: IDBObjectStore,
  prefix: ReadonlyArray<string | number>,
  kind: "base" | "tail",
  requestedDescending: readonly string[],
  existing: Set<string>,
): Promise<void> {
  if (requestedDescending.length === 0) return Promise.resolve();
  return new Promise((resolve, reject) => {
    const request = store.openCursor(null, "prev");
    let position = 0;
    let positioned = false;
    request.onerror = () => reject(request.error ?? new Error("IndexedDB cursor failed"));
    request.onsuccess = () => {
      const cursor = request.result;
      const token = requestedDescending[position];
      if (cursor === null || token === undefined) {
        resolve();
        return;
      }
      try {
        const target = [...prefix, token];
        if (!positioned) {
          positioned = true;
          if (compareSimpleStructuredKey(cursor.key, target) > 0) {
            cursor.continue(target);
            return;
          }
        }
        if (compareStructuredKeyPrefix(cursor.key, prefix) !== 0) {
          // The reverse seek landed before this source, so every remaining token precedes its
          // first boundary and is unaffected by it.
          resolve();
          return;
        }
        const key = cursor.key;
        const base = kind === "base" ? asBasePartition(cursor.value) : undefined;
        const tail = kind === "tail" ? asUniqueKeyChunk(cursor.value) : undefined;
        const first = base?.[0] ?? (tail === undefined ? "" : uniqueChunkFirstToken(tail));
        if (!Array.isArray(key) || key.length !== prefix.length + 1 || key.at(-1) !== first) {
          throw corruption(
            kind === "base" ? UNIQUE_KEY_BASE_PART : UNIQUE_KEY_CHUNK,
            "ordered membership boundary differs from its key",
          );
        }
        const baseLast = base?.at(-1);
        const addedLast = tail?.addedTokens.at(-1);
        const removedLast = tail?.removedTokens.at(-1);
        const last =
          baseLast ??
          (addedLast === undefined
            ? removedLast
            : removedLast === undefined || addedLast > removedLast
              ? addedLast
              : removedLast);
        while (position < requestedDescending.length) {
          const candidate = requestedDescending[position];
          if (candidate === undefined || candidate < first) break;
          if (last !== undefined && candidate <= last) {
            if (
              base?.includes(candidate) === true ||
              tail?.addedTokens.includes(candidate) === true
            ) {
              existing.add(candidate);
            } else if (tail?.removedTokens.includes(candidate) === true) {
              existing.delete(candidate);
            }
          }
          position += 1;
        }
        const next = requestedDescending[position];
        if (next === undefined) resolve();
        else cursor.continue([...prefix, next]);
      } catch (error) {
        reject(error instanceof Error ? error : new Error(String(error)));
      }
    };
  });
}

/** Fail closed if a durable membership index points at an absent first part. */
function assertUniqueMembershipSourceExists(
  store: IDBObjectStore,
  prefix: ReadonlyArray<string | number>,
  kind: "base" | "tail",
): Promise<void> {
  return new Promise((resolve, reject) => {
    const request = store.openCursor();
    let positioned = false;
    request.onerror = () => reject(request.error ?? new Error("IndexedDB cursor failed"));
    request.onsuccess = () => {
      const cursor = request.result;
      if (cursor === null) {
        reject(
          corruption(
            kind === "base" ? UNIQUE_KEY_BASE_PART : UNIQUE_KEY_CHUNK,
            "membership source has no parts",
          ),
        );
        return;
      }
      try {
        if (!positioned && compareStructuredKeyPrefix(cursor.key, prefix) < 0) {
          positioned = true;
          cursor.continue([...prefix]);
          return;
        }
        if (compareStructuredKeyPrefix(cursor.key, prefix) !== 0) {
          throw corruption(
            kind === "base" ? UNIQUE_KEY_BASE_PART : UNIQUE_KEY_CHUNK,
            "membership source has no parts",
          );
        }
        const part =
          kind === "base" ? asBasePartition(cursor.value) : asUniqueKeyChunk(cursor.value);
        const first = Array.isArray(part) ? part[0] : uniqueChunkFirstToken(part);
        if (!Array.isArray(cursor.key) || cursor.key.at(-1) !== first) {
          throw corruption(
            kind === "base" ? UNIQUE_KEY_BASE_PART : UNIQUE_KEY_CHUNK,
            "membership boundary differs from its key",
          );
        }
        resolve();
      } catch (error) {
        reject(error instanceof Error ? error : new Error(String(error)));
      }
    };
  });
}

async function readChunkedUniqueKeys(
  store: IDBObjectStore,
  tableId: string,
  requestedTokens: readonly string[],
  suppliedIndex?: { value: unknown },
): Promise<{
  existing: Set<string>;
  index: UniqueKeyChunkIndex;
  /** Full retained tail chunks, empty for the partitioned point-read path. */
  chunks: UniqueKeyChunk[];
  /** The complete membership (base plus tail replay), when this read resolved all of it. */
  fullPresent?: Set<string>;
}> {
  const rawIndex: unknown =
    suppliedIndex === undefined
      ? await requestResult(store.get(uniqueKeyChunkIndexKey(tableId)))
      : suppliedIndex.value;
  if (rawIndex === undefined) {
    throw corruption(`${UNIQUE_KEY_CHUNK_INDEX}/${tableId}`, "membership index is missing");
  }
  const index = asUniqueKeyChunkIndex(rawIndex);
  const requested = new Set(requestedTokens);
  const requestedDescending = [...requested].sort((left, right) =>
    left < right ? 1 : left > right ? -1 : 0,
  );
  const existing = new Set<string>();
  if (index.hasBase) {
    if (index.baseGenerationId === undefined) {
      throw corruption(
        `${UNIQUE_KEY_CHUNK_INDEX}/${tableId}`,
        "ordered base generation is missing",
      );
    }
    const prefix = [UNIQUE_KEY_BASE_PART, index.baseGenerationId] as const;
    await assertUniqueMembershipSourceExists(store, prefix, "base");
    await applyUniqueMembershipSourceToRequested(
      store,
      prefix,
      "base",
      requestedDescending,
      existing,
    );
  }
  for (const version of index.versions) {
    const prefix = [UNIQUE_KEY_CHUNK, tableId, version] as const;
    await assertUniqueMembershipSourceExists(store, prefix, "tail");
    await applyUniqueMembershipSourceToRequested(
      store,
      prefix,
      "tail",
      requestedDescending,
      existing,
    );
  }
  return {
    existing,
    index,
    chunks: [],
    ...(!index.hasBase && index.versions.length === 0 ? { fullPresent: new Set<string>() } : {}),
  };
}

async function uniqueKeyNamespaceIsActive(
  store: IDBObjectStore,
  namespaceId: string,
): Promise<boolean> {
  const separator = "\u0000secondary-index\u0000";
  const separatorIndex = namespaceId.lastIndexOf(separator);
  const tableId = separatorIndex < 0 ? namespaceId : namespaceId.slice(0, separatorIndex);
  const tableKey = `${TABLE_ID_PREFIX}${tableId}`;
  const value: unknown = await requestResult(store.get(tableKey));
  if (value === undefined) return false;
  const table = asTableRecord(value, tableKey);
  if (separatorIndex < 0) return table.uniqueKeyColumnId !== undefined;
  const indexId = namespaceId.slice(separatorIndex + separator.length);
  const index = table.secondaryIndexes?.[indexId];
  return index?.unique === true;
}

function asUniqueKeyChunk(value: unknown): UniqueKeyChunk {
  if (typeof value !== "object" || value === null) {
    throw corruption(UNIQUE_KEY_CHUNK, "tail chunk is missing or invalid");
  }
  const record = value as Record<string, unknown>;
  assertKnownFields(record, ["addedTokens", "removedTokens"], UNIQUE_KEY_CHUNK);
  const addedTokens = record.addedTokens;
  const removedTokens = record.removedTokens;
  if (!isStringArray(addedTokens) || !isStringArray(removedTokens)) {
    throw corruption(UNIQUE_KEY_CHUNK, "tail chunk is invalid");
  }
  if (
    new Set(addedTokens).size !== addedTokens.length ||
    new Set(removedTokens).size !== removedTokens.length ||
    addedTokens.some((token) => removedTokens.includes(token)) ||
    addedTokens.some((token, index) => index > 0 && token <= (addedTokens[index - 1] ?? "")) ||
    removedTokens.some((token, index) => index > 0 && token <= (removedTokens[index - 1] ?? "")) ||
    addedTokens.length + removedTokens.length > UNIQUE_KEY_MEMBERSHIP_PART_TOKENS
  ) {
    throw corruption(UNIQUE_KEY_CHUNK, "tail chunk is not canonical");
  }
  let retainedBytes = 0;
  for (const token of [...addedTokens, ...removedTokens]) {
    try {
      retainedBytes = safeByteSum(
        retainedBytes,
        uniqueMembershipTokenRetainedBytes(token),
        "UNIQUE tail part bytes",
      );
    } catch (error) {
      throw corruption(UNIQUE_KEY_CHUNK, error instanceof Error ? error.message : String(error));
    }
    if (retainedBytes > UNIQUE_KEY_MEMBERSHIP_PART_RETAINED_BYTES) {
      throw corruption(UNIQUE_KEY_CHUNK, "tail chunk exceeds its byte limit");
    }
  }
  return { addedTokens, removedTokens };
}

function getGlobalIndexedDb(): IDBFactory | undefined {
  return "indexedDB" in globalThis ? globalThis.indexedDB : undefined;
}

function logicalStoredBytes(value: unknown, seen = new Set<object>()): number {
  if (value === null || value === undefined) return 0;
  if (typeof value === "string") return utf8Bytes(value);
  if (typeof value === "number" || typeof value === "bigint") return 8;
  if (typeof value === "boolean") return 1;
  if (value instanceof Date) return 8;
  if (value instanceof ArrayBuffer) return value.byteLength;
  if (ArrayBuffer.isView(value)) return value.byteLength;
  if (typeof value !== "object" || seen.has(value)) return 0;
  seen.add(value);
  if (Array.isArray(value)) {
    return value.reduce<number>(
      (bytes, entry) => safeByteSum(bytes, logicalStoredBytes(entry, seen), "Logical stored bytes"),
      0,
    );
  }
  return Object.entries(value).reduce(
    (bytes, [key, entry]) =>
      safeByteSum(
        safeByteSum(bytes, logicalStoredBytes(key, seen), "Logical stored bytes"),
        logicalStoredBytes(entry, seen),
        "Logical stored bytes",
      ),
    0,
  );
}

function isStringArray(value: unknown): value is string[] {
  return Array.isArray(value) && value.every((entry: unknown) => typeof entry === "string");
}

function logicalObjectStoreBytes(store: IDBObjectStore): Promise<number> {
  return new Promise((resolve, reject) => {
    let total = 0;
    const request = store.openCursor();
    request.addEventListener("success", () => {
      const cursor = request.result;
      if (cursor === null) {
        resolve(total);
        return;
      }
      total = safeByteSum(
        safeByteSum(total, logicalStoredBytes(cursor.key), "Logical object-store bytes"),
        logicalStoredBytes(cursor.value),
        "Logical object-store bytes",
      );
      cursor.continue();
    });
    request.addEventListener(
      "error",
      () => reject(request.error ?? new Error("IndexedDB size cursor failed")),
      { once: true },
    );
  });
}

function utf8Bytes(value: string): number {
  if (value.length * 3 <= storageTextBuffer.byteLength) {
    return storageTextEncoder.encodeInto(value, storageTextBuffer).written;
  }
  return storageTextEncoder.encode(value).byteLength;
}
