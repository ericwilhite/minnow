import {
  type BeginTransactionInput,
  type BeginTransactionResult,
  type CloseSnapshotExportInput,
  type CancelSnapshotImportInput,
  type InterruptedSnapshotImportAbortResult,
  type AbortTransactionIfExpiredInput,
  type AdoptAbortedSegmentInput,
  type RenewTransactionInput,
  type CommitTransactionInput,
  type CompactionJobRecord,
  type CompactionJobRecordUpdate,
  type CreateGarbageCollectionJobInput,
  type UpdateGarbageCollectionPlanningInput,
  type DropTableColumnInput,
  type DropTableInput,
  type FtsCandidates,
  type FtsPostingQuery,
  type FtsPosting,
  type GarbageCollectionJobRecord,
  type GarbageCollectionStepResult,
  type LeaseRecord,
  type MoveLeaseInput,
  type ManifestSummary,
  type ManifestBlockPage,
  type ListManifestBlockPageInput,
  type ListRetiredManifestBlockPageInput,
  type RollbackTransactionArtifactsInput,
  type RenewLeaseInput,
  type RenewTempOwnerInput,
  type RunGarbageCollectionStepInput,
  type SegmentRecord,
  type StageTransactionArtifactsInput,
  type StoragePage,
  type StorageIntegrityMode,
  type StorageIntegrityReport,
  type StorageStats,
  type CatalogMutationOptions,
  type TableRecord,
  type TableRecordUpdate,
  type TempOwnerRecord,
  type TempRunPage,
  type TransactionRecord,
  type TransactionRecordUpdate,
  type WriteTransactionInput,
  type BeginUniqueKeyBuildInput,
  type RenewUniqueKeyBuildInput,
  type AppendUniqueKeyBuildChunkInput,
  type FinishUniqueKeyBuildInput,
  type AbortUniqueKeyBuildInput,
  type BeginPostingBuildInput,
  type RenewPostingBuildInput,
  type AppendPostingBuildChunkInput,
  type FinishPostingBuildInput,
  type AbortPostingBuildInput,
  type BeginSnapshotFrameExportInput,
  type BeginSnapshotFrameImportInput,
  type ReadSnapshotExportFrameInput,
  type RenewSnapshotFrameImportInput,
  type AppendSnapshotImportFramesInput,
  type FinishSnapshotFrameImportInput,
  type SnapshotFrame,
  type SnapshotFrameExportSession,
  type SnapshotFrameImportSession,
  type SnapshotFrameKind,
  type SnapshotFrameStreamHeader,
  type SnapshotKindSummary,
  type SnapshotMetadataItem,
  type SnapshotPostingItem,
  activePostingStorageColumnIds,
  assertStorageBulkReadItems,
  assertTempRunPageBatchLimits,
  BlockReadBatchTooLargeError,
  collectFtsPostingsBounded,
  ftsPostingQueryMatches,
  MAX_BLOCK_READ_BATCH_BYTES,
  MAX_FTS_BASE_CHUNKS,
  MAX_POSTING_BUILD_TTL_MS,
  MAX_FTS_POSTING_TERM_CHARACTERS,
  MAX_MANIFEST_BLOCK_PRESENCE_IDS,
  MAX_STORAGE_ID_CHARACTERS,
  MAX_TEMP_RUN_PAGE_BYTES,
  MAX_TEMP_RUN_PAGES_PER_BATCH,
  MAX_TEMP_RUN_BATCH_BYTES,
  MAX_TEMP_RUNS_PER_OWNER,
  MAX_TEMP_PAGES_PER_OWNER,
  MAX_TEMP_RUNS_TOTAL,
  MAX_TEMP_PAGES_TOTAL,
  MAX_TEMP_BYTES_PER_OWNER,
  MAX_TEMP_BYTES_TOTAL,
  MAX_ACTIVE_FTS_BASE_BUILDS,
  MAX_ACTIVE_SECONDARY_INDEX_BUILDS,
  MAX_ACCELERATOR_BUILD_STAGED_BYTES_TOTAL,
  MAX_ACCELERATOR_BUILD_STAGED_ENTRIES_TOTAL,
  MAX_FTS_CANDIDATE_ROW_IDS,
  MAX_FTS_ORDERED_READ_BYTES,
  MAX_SNAPSHOT_SESSION_TTL_MS,
  MAX_SNAPSHOT_FRAME_BATCH_BYTES,
  MAX_SNAPSHOT_FRAME_BATCH_ITEMS,
  MAX_SNAPSHOT_METADATA_BATCH_BYTES,
  SNAPSHOT_FRAME_KINDS,
  MAX_TRANSACTION_STAGE_BLOCKS,
  MAX_TRANSACTION_STAGE_SEGMENTS,
  SnapshotImportConflictError,
  PostingBuildConflictError,
  StorageResourceLimitError,
  StorageCorruptionError,
  StorageFormatVersionError,
  validateFtsPostingQueries,
  validateFtsOrderedReadLimits,
  uniqueKeyBuildChunkRetainedBytes,
} from "../types.js";
import { dateIsoString } from "../../date-value.js";
import {
  decodeSnapshotMetadataItems,
  encodeSnapshotMetadataPage,
  extendSnapshotFrameStreamChecksum,
  prepareSnapshotFrameStreamHeader,
  snapshotFrameEnvelopeParts,
  snapshotFrameStreamHeaderIdentity,
} from "../snapshot-stream.js";
import {
  RecordCore,
  type RecordCoreState,
  validateBeginTransactionInput,
  validateBlockWriteBytes,
  validateFtsBaseInput,
  validateFtsPostingChunks,
  validateId,
  validateTempRunPage,
  validateTempRunPageIdentity,
} from "../toolkit/record-core.js";
import { OpfsTree, encodeSegment, decodeSegment } from "./files.js";
import {
  SnapshotFrameLedger,
  snapshotLedgerPath,
  type SnapshotLedgerRead,
} from "./snapshot-ledger.js";
import {
  decodePostingChunk,
  decodeSyncCheckpoint,
  encodePostingChunk,
  encodeSyncCheckpoint,
} from "../toolkit/wire.js";
import { WalWriter, iterateWalFrames } from "../toolkit/wal.js";
import {
  ExtentPool,
  assertValidExtentMeta,
  assertValidPlacement,
  extentPath,
  type ExtentBatchMark,
  type ExtentMeta,
  type Placement,
} from "../toolkit/extents.js";
import { readFully, writeFully } from "../toolkit/sync-file.js";
import { MAX_STORED_BLOCK_BYTE_LENGTH, verifyStoredBlock } from "../../block-format/index.js";
import { crc32 } from "../../block-format/checksum.js";

/** Checkpoint when the WAL passes either bound; both trade replay time against pause time. */
const CHECKPOINT_WAL_BYTES = 4 * 1024 * 1024;
const CHECKPOINT_ENTRIES = 1024;
/** A failed checkpoint may defer compaction, but the recovery log itself stays bounded. */
export const MAX_OPFS_WAL_BYTES = 256 * 1024 * 1024;
export const MAX_OPFS_CHECKPOINT_BYTES = 256 * 1024 * 1024;
/** Failed physical reclamation cannot permit byte-growing work forever. */
const MAX_OPFS_CLEANUP_DEBT_BYTES = 64 * 1024 * 1024;
/** Decoded full-text base cache: bounded primarily by modeled retained heap, count secondarily. */
const FTS_CHUNK_CACHE_BYTES = 16 * 1024 * 1024;
const FTS_CHUNK_CACHE_SIZE = 64;
/** Keeps relocation WAL frames bounded even when an extent contains many tiny payloads. */
const MAX_RELOCATION_PAYLOADS = 256;
/** Canonical framed snapshots retain one bounded posting chunk per semantic metadata frame. */
const SNAPSHOT_POSTING_TERMS_PER_CHUNK = 128;
/** Model filesystem metadata so persistent refusal cannot grow unlimited zero-byte scratch files. */
const TEMP_FILE_CLEANUP_DEBT_FLOOR_BYTES = 64 * 1024;

interface IdPlacement {
  id: string;
  placement: Placement;
}

interface BlockRelocation {
  id: string;
  from: Placement;
  placement: Placement;
}

interface FtsChunkRelocation {
  key: string;
  ordinal: number;
  from: Placement;
  placement: Placement;
}

interface FtsBuildChunkRelocation extends FtsChunkRelocation {
  buildId: string;
}

interface FtsBasePointer {
  coversVersion: number;
  totalTokens: number;
  chunks: Placement[];
  chunkBounds: Array<{ first: string; last: string }>;
}

interface FtsBaseBuildPointer {
  buildId: string;
  ownerId: string;
  ownerKind: "fts-column" | "secondary-index";
  createdAt: string;
  expiresAt: string;
  updatedAt: string;
  chunks: Placement[];
  chunkBounds: Array<{ first: string; last: string }>;
  totalTokens: number;
  retainedBytes: number;
  retainedEntries: number;
}

interface TempPageLedgerRecord {
  ownerId: string;
  runId: string;
  pageIndex: number;
  length: number;
}

interface TempOwnerUsage {
  runs: Map<string, number>;
  pages: number;
  bytes: number;
}

type ExtentPayloadRef =
  | { kind: "block"; id: string }
  | { kind: "fts"; key: string; ordinal: number }
  | { kind: "fts-build"; key: string; buildId: string; ordinal: number };

interface SnapshotFrameExportState {
  sessionId: string;
  ownerId: string;
  createdAt: string;
  expiresAt: string;
  leaseRevision: number;
  manifestVersion: number;
  header: SnapshotFrameStreamHeader;
  ledgerId: string;
  ledgerLength: number;
  metadataFrameCount: number;
  nextSequence: number;
  metadataOffset: number;
  blockCursor: string | null;
  lastSequence: number | null;
  lastMetadataOffset: number | null;
  lastBlockId: string | null;
}

interface SnapshotFrameObservedState {
  frameCount: number;
  itemCount: number;
  storedBytes: number;
}

interface SnapshotFrameImportState {
  identity: string;
  ownerId: string;
  version: number;
  createdAt: string;
  expiresAt: string;
  header: SnapshotFrameStreamHeader;
  ledgerId: string;
  ledgerLength: number;
  nextSequence: number;
  stagedBytes: number;
  blockCount: number;
  blockBytes: number;
  checksum: number;
  currentKindIndex: number;
  observed: Record<SnapshotFrameKind, SnapshotFrameObservedState>;
  lastBatchStartSequence: number | null;
  lastBatchOffset: number | null;
  lastBatchFrameCount: number;
  completedReplay: boolean;
}

interface CompletedSnapshotFrameImportState {
  identity: string;
  version: number;
  header: SnapshotFrameStreamHeader;
  ledgerId: string;
  ledgerLength: number;
}

interface CheckpointState {
  formatVersion: 1;
  generation: number;
  lastSeq: number;
  core: RecordCoreState;
  blockIndex: Array<readonly [string, Placement]>;
  ftsBases: Array<readonly [string, FtsBasePointer]>;
  ftsBuilds: Array<readonly [string, FtsBaseBuildPointer]>;
  tempPages: TempPageLedgerRecord[];
  snapshotFrameExport?: SnapshotFrameExportState;
  snapshotFrameImport?: SnapshotFrameImportState;
  completedSnapshotFrameImport?: CompletedSnapshotFrameImportState;
  extents: ExtentMeta;
}

type WalEntryBody =
  | {
      op: "relocatePayloads";
      blocks: BlockRelocation[];
      ftsChunks: FtsChunkRelocation[];
      ftsBuildChunks: FtsBuildChunkRelocation[];
    }
  | {
      op: "addTable";
      record: TableRecord;
      expectedCatalogEpoch: number | null;
    }
  | {
      op: "updateTable";
      id: string;
      expectedRevision: number;
      update: TableRecordUpdate;
    }
  | {
      op: "removeTable";
      id: string;
      expectedRevision: number;
      expectedCatalogEpoch: number | null;
    }
  | { op: "dropTable"; input: DropTableInput }
  | { op: "dropTableColumn"; input: DropTableColumnInput }
  | { op: "removeFtsColumn"; tableId: string; columnId: string }
  | { op: "removeAbortedSegment"; id: string; expectedTransactionId: string }
  | { op: "adoptAbortedSegment"; input: AdoptAbortedSegmentInput }
  | { op: "reserveRowIds"; tableId: string; count: number }
  | { op: "beginUniqueKeyBuild"; input: BeginUniqueKeyBuildInput }
  | { op: "renewUniqueKeyBuild"; input: RenewUniqueKeyBuildInput }
  | { op: "appendUniqueKeyBuildChunk"; input: AppendUniqueKeyBuildChunkInput }
  | { op: "finishUniqueKeyBuild"; input: FinishUniqueKeyBuildInput }
  | { op: "abortUniqueKeyBuild"; input: AbortUniqueKeyBuildInput }
  | {
      op: "reserveAutoIncrement";
      tableId: string;
      columnId: string;
      count: number;
      atLeast?: bigint;
    }
  | { op: "beginTransaction"; input: BeginTransactionInput }
  | { op: "createTransaction"; record: TransactionRecord }
  | { op: "renewTransaction"; input: RenewTransactionInput }
  | { op: "abortTransactionIfExpired"; input: AbortTransactionIfExpiredInput }
  | {
      op: "updateTransaction";
      id: string;
      expectedRevision: number;
      update: TransactionRecordUpdate;
    }
  | {
      op: "stageTransactionArtifacts";
      transactionId: string;
      expectedRevision: number;
      blocks: IdPlacement[];
      segments: SegmentRecord[];
      updatedAt: string;
    }
  | { op: "rollbackTransactionArtifacts"; input: RollbackTransactionArtifactsInput }
  | { op: "commitTransaction"; input: CommitTransactionInput }
  | {
      op: "writeTransaction";
      input: Omit<WriteTransactionInput, "blocks">;
      blocks: IdPlacement[];
    }
  | { op: "createLease"; record: LeaseRecord }
  | { op: "renewLease"; input: RenewLeaseInput }
  | { op: "moveLease"; input: MoveLeaseInput }
  | { op: "removeLeaseIfExpired"; id: string; expectedRevision: number; expiresAtCutoff: string }
  | { op: "removeLease"; input: { id: string; ownerId: string } }
  | { op: "createCompactionJob"; record: CompactionJobRecord }
  | {
      op: "updateCompactionJob";
      id: string;
      expectedRevision: number;
      update: CompactionJobRecordUpdate;
    }
  | { op: "cancelCompactionJob"; id: string; expectedRevision: number; cancelledAt: string }
  | { op: "removeCompactionJob"; id: string }
  | { op: "createGarbageCollectionJob"; input: CreateGarbageCollectionJobInput }
  | { op: "updateGarbageCollectionPlanning"; input: UpdateGarbageCollectionPlanningInput }
  | {
      op: "garbageCollectionStep";
      effect: {
        job: GarbageCollectionJobRecord;
        prunedManifestVersions: number[];
        reclaimedSegmentIds: string[];
        reclaimedBlockIds: string[];
        reclaimedTransactionIds: string[];
        updatedAt: string;
      };
    }
  | { op: "removeGarbageCollectionJob"; id: string }
  | { op: "removePrunedManifestRecords"; maxItems: number }
  | { op: "createTempOwner"; record: TempOwnerRecord }
  | { op: "renewTempOwner"; input: RenewTempOwnerInput }
  | { op: "removeTempOwnerIfExpired"; ownerId: string; expiresAtCutoff: string }
  | { op: "removeTempOwner"; ownerId: string }
  | { op: "reserveTempRunPages"; pages: TempPageLedgerRecord[] }
  | {
      op: "restoreTempRunPages";
      pages: Array<Omit<TempPageLedgerRecord, "length"> & { length: number | null }>;
    }
  | { op: "removeTempRun"; ownerId: string; runId: string }
  | { op: "writeFtsBase"; tableId: string; columnId: string; pointer: FtsBasePointer }
  | { op: "beginFtsBaseBuild"; input: BeginPostingBuildInput }
  | { op: "renewFtsBaseBuild"; input: RenewPostingBuildInput }
  | {
      op: "writeFtsBaseBuildChunk";
      input: Omit<AppendPostingBuildChunkInput, "chunk">;
      placement: Placement;
      bounds: { first: string; last: string };
      totalTokens: number;
      retainedEntries: number;
    }
  | { op: "finishFtsBaseBuild"; input: FinishPostingBuildInput }
  | { op: "abortFtsBaseBuild"; input: AbortPostingBuildInput }
  | { op: "beginSnapshotFrameExport"; state: SnapshotFrameExportState }
  | {
      op: "advanceSnapshotFrameExport";
      input: ReadSnapshotExportFrameInput;
      expectedLeaseRevision: number;
      next: Pick<
        SnapshotFrameExportState,
        | "nextSequence"
        | "metadataOffset"
        | "blockCursor"
        | "lastSequence"
        | "lastMetadataOffset"
        | "lastBlockId"
      >;
    }
  | { op: "closeSnapshotFrameExport"; input: CloseSnapshotExportInput }
  | { op: "beginSnapshotFrameImport"; state: SnapshotFrameImportState }
  | { op: "renewSnapshotFrameImport"; input: RenewSnapshotFrameImportInput }
  | {
      op: "appendSnapshotImportFrames";
      input: RenewSnapshotFrameImportInput;
      state: Omit<SnapshotFrameImportState, "header">;
      blockPlacements: IdPlacement[];
      replay: boolean;
    }
  | { op: "finishSnapshotFrameImport"; input: FinishSnapshotFrameImportInput }
  | { op: "cancelSnapshotFrameImport"; input: CancelSnapshotImportInput };

type WalEntry = WalEntryBody & { seq: number };

const CORE_READ_METHODS = [
  "getTempOwner",
  "getTable",
  "getTableByName",
  "listTables",
  "getSegment",
  "listSegmentPage",
  "listTableSegmentPage",
  "getExistingUniqueKeys",
  "getUniqueKeyBuild",
  "getCurrentManifestVersion",
  "getCatalogProbe",
  "getCurrentManifest",
  "getManifest",
  "listManifestPage",
  "listManifestBlockPage",
  "listRetiredManifestBlockPage",
  "getTransaction",
  "getTransactions",
  "listTransactionPage",
  "getLease",
  "listLeases",
  "listExpiredLeasePage",
  "getCompactionJob",
  "listCompactionJobs",
  "listCompactionJobPage",
  "getGarbageCollectionJob",
  "listGarbageCollectionJobs",
  "listGarbageCollectionJobPage",
] as const;
type CoreReadMethod = (typeof CORE_READ_METHODS)[number];

const LOGGED_BODY_BUILDERS = {
  createTempOwner: ([record]: unknown[]) => ({ op: "createTempOwner", record }) as WalEntryBody,
  renewTempOwner: ([input]: unknown[]) => ({ op: "renewTempOwner", input }) as WalEntryBody,
  removeAbortedSegment: ([id, expectedTransactionId]: unknown[]) =>
    ({ op: "removeAbortedSegment", id, expectedTransactionId }) as WalEntryBody,
  adoptAbortedSegment: ([input]: unknown[]) =>
    ({ op: "adoptAbortedSegment", input }) as WalEntryBody,
  reserveRowIds: ([tableId, count]: unknown[]) =>
    ({ op: "reserveRowIds", tableId, count }) as WalEntryBody,
  beginUniqueKeyBuild: ([input]: unknown[]) =>
    ({ op: "beginUniqueKeyBuild", input }) as WalEntryBody,
  renewUniqueKeyBuild: ([input]: unknown[]) =>
    ({ op: "renewUniqueKeyBuild", input }) as WalEntryBody,
  appendUniqueKeyBuildChunk: ([input]: unknown[]) =>
    ({ op: "appendUniqueKeyBuildChunk", input }) as WalEntryBody,
  finishUniqueKeyBuild: ([input]: unknown[]) =>
    ({ op: "finishUniqueKeyBuild", input }) as WalEntryBody,
  abortUniqueKeyBuild: ([input]: unknown[]) =>
    ({ op: "abortUniqueKeyBuild", input }) as WalEntryBody,
  reserveAutoIncrement: ([tableId, columnId, count, atLeast]: unknown[]) =>
    ({ op: "reserveAutoIncrement", tableId, columnId, count, atLeast }) as WalEntryBody,
  createTransaction: ([record]: unknown[]) => ({ op: "createTransaction", record }) as WalEntryBody,
  renewTransaction: ([input]: unknown[]) => ({ op: "renewTransaction", input }) as WalEntryBody,
  abortTransactionIfExpired: ([input]: unknown[]) =>
    ({ op: "abortTransactionIfExpired", input }) as WalEntryBody,
  updateTransaction: ([id, expectedRevision, update]: unknown[]) =>
    ({ op: "updateTransaction", id, expectedRevision, update }) as WalEntryBody,
  commitTransaction: ([input]: unknown[]) => ({ op: "commitTransaction", input }) as WalEntryBody,
  createLease: ([record]: unknown[]) => ({ op: "createLease", record }) as WalEntryBody,
  renewLease: ([input]: unknown[]) => ({ op: "renewLease", input }) as WalEntryBody,
  moveLease: ([input]: unknown[]) => ({ op: "moveLease", input }) as WalEntryBody,
  removeLeaseIfExpired: ([id, expectedRevision, expiresAtCutoff]: unknown[]) =>
    ({ op: "removeLeaseIfExpired", id, expectedRevision, expiresAtCutoff }) as WalEntryBody,
  removeLease: ([input]: unknown[]) => ({ op: "removeLease", input }) as WalEntryBody,
  createCompactionJob: ([record]: unknown[]) =>
    ({ op: "createCompactionJob", record }) as WalEntryBody,
  updateCompactionJob: ([id, expectedRevision, update]: unknown[]) =>
    ({ op: "updateCompactionJob", id, expectedRevision, update }) as WalEntryBody,
  cancelCompactionJob: ([id, expectedRevision, cancelledAt]: unknown[]) =>
    ({ op: "cancelCompactionJob", id, expectedRevision, cancelledAt }) as WalEntryBody,
  createGarbageCollectionJob: ([input]: unknown[]) =>
    ({ op: "createGarbageCollectionJob", input }) as WalEntryBody,
  updateGarbageCollectionPlanning: ([input]: unknown[]) =>
    ({ op: "updateGarbageCollectionPlanning", input }) as WalEntryBody,
  removeGarbageCollectionJob: ([id]: unknown[]) =>
    ({ op: "removeGarbageCollectionJob", id }) as WalEntryBody,
  removePrunedManifestRecords: ([maxItems]: unknown[]) =>
    ({ op: "removePrunedManifestRecords", maxItems }) as WalEntryBody,
} as const;
type LoggedMethod = keyof typeof LOGGED_BODY_BUILDERS;

/**
 * The leader: the one instance holding the database's write-ahead log handle, and with it
 * every other handle — checkpoint slots, the tail extent, a small cache of sealed extents.
 *
 * Because the handles are held, every operation's storage cost is a synchronous write or read
 * measured in microseconds, and because leadership is exclusive by the browser's own file
 * lock, the leader's in-memory `RecordCore` is always provably current — there are no
 * freshness probes anywhere. An operation is: validate and mutate the core (synchronously),
 * append one checksummed WAL frame (synchronously), acknowledge. The only awaits sit before
 * that critical section (extent appends that may seal, temp-file IO), so no observer ever
 * sees state mid-mutation.
 *
 * Checkpointing is also synchronous — dump, write to the older of two fixed slots, flush,
 * reset the WAL — which is what makes a single WAL file safe: the flush precedes the reset,
 * and a crash mid-checkpoint corrupts only the slot being written while the other slot plus
 * the un-reset WAL still reconstruct everything.
 */
export class OpfsLeader {
  readonly #tree: OpfsTree;
  readonly #strict: boolean;
  readonly #core: RecordCore;
  readonly #blockIndex = new Map<string, Placement>();
  readonly #ftsBases = new Map<string, FtsBasePointer>();
  readonly #ftsBuilds = new Map<string, FtsBaseBuildPointer>();
  #activeFtsBuildCount = 0;
  #activeSecondaryBuildCount = 0;
  #acceleratorBuildStagedBytes = 0;
  #acceleratorBuildStagedEntries = 0;
  readonly #extentPayloadRefs = new Map<number, Map<string, ExtentPayloadRef>>();
  readonly #tempPages = new Map<string, TempPageLedgerRecord>();
  readonly #tempOwnerUsage = new Map<string, TempOwnerUsage>();
  #tempRunCount = 0;
  #tempPageBytes = 0;
  #tempLedgerNeedsCheckpoint = false;
  readonly #ftsChunkCache = new Map<string, { chunk: FtsPosting[]; modeledBytes: number }>();
  #ftsChunkCacheBytes = 0;
  #snapshotFrameExport: SnapshotFrameExportState | undefined;
  #snapshotFrameExportLedger: SnapshotFrameLedger | undefined;
  #preparedSnapshotFrameExportLedger: SnapshotFrameLedger | undefined;
  #snapshotFrameImport: SnapshotFrameImportState | undefined;
  #snapshotFrameImportLedger: SnapshotFrameLedger | undefined;
  #preparedSnapshotFrameImportLedger: SnapshotFrameLedger | undefined;
  #completedSnapshotFrameImport: CompletedSnapshotFrameImportState | undefined;
  readonly #staleSnapshotFrameLedgers = new Map<
    string,
    { kind: "export" | "import"; id: string }
  >();
  #extents: ExtentPool | undefined;
  #wal: WalWriter;
  /** The pool exists from the moment recovery succeeds; before that, nothing may touch it. */
  get #pool(): ExtentPool {
    const pool = this.#extents;
    if (pool === undefined) throw new Error("The OPFS leader has not recovered yet");
    return pool;
  }
  readonly #walHandle: FileSystemSyncAccessHandle;
  readonly #slots: [FileSystemSyncAccessHandle, FileSystemSyncAccessHandle];
  /** The slot holding the newest checkpoint; the next checkpoint writes to the other. */
  #newestSlot = 0;
  #seq = 0;
  #entriesSinceCheckpoint = 0;
  #permissivePhysical = false;
  #poisoned = false;
  #closed = false;
  #chain: Promise<unknown> = Promise.resolve();
  readonly #checkpointEntries: number;
  readonly #cleanupLimitBytes: number;
  #lastCheckpointBytes = 0;
  #checkpointScheduled = false;
  #checkpointGeneration = 0;
  #checkpointFailures = 0;
  #lastCheckpointError: unknown;
  #checkpointRetryAtEntries = 0;
  #cleanupFailures = 0;
  #lastCleanupError: unknown;
  #cleanupRetryScheduled = false;
  #cleanupDebtBytes = 0;

  private constructor(
    tree: OpfsTree,
    strict: boolean,
    walHandle: FileSystemSyncAccessHandle,
    slots: [FileSystemSyncAccessHandle, FileSystemSyncAccessHandle],
    checkpointEntries?: number,
    cleanupLimitBytes?: number,
  ) {
    this.#checkpointEntries = checkpointEntries ?? CHECKPOINT_ENTRIES;
    this.#cleanupLimitBytes = cleanupLimitBytes ?? MAX_OPFS_CLEANUP_DEBT_BYTES;
    if (!Number.isSafeInteger(this.#cleanupLimitBytes) || this.#cleanupLimitBytes < 1) {
      throw new TypeError("OPFS cleanup limit must be a positive safe integer");
    }
    this.#tree = tree;
    this.#strict = strict;
    this.#walHandle = walHandle;
    this.#slots = slots;
    this.#wal = new WalWriter(walHandle, 0);
    this.#core = new RecordCore({
      hasBlock: (id) => this.#permissivePhysical || this.#blockIndex.has(id),
      blockByteLength: (id) =>
        this.#blockIndex.get(id)?.length ?? (this.#permissivePhysical ? 0 : undefined),
      blockChecksum: (id) =>
        this.#blockIndex.get(id)?.checksum ?? (this.#permissivePhysical ? 0 : undefined),
    });
  }

  /** Builds a leader from held handles: newest valid checkpoint slot plus the WAL tail. */
  static async recover(
    tree: OpfsTree,
    strict: boolean,
    handles: {
      wal: FileSystemSyncAccessHandle;
      slotA: FileSystemSyncAccessHandle;
      slotB: FileSystemSyncAccessHandle;
    },
    checkpointEntries?: number,
    cleanupLimitBytes?: number,
  ): Promise<OpfsLeader> {
    const leader = new OpfsLeader(
      tree,
      strict,
      handles.wal,
      [handles.slotA, handles.slotB],
      checkpointEntries,
      cleanupLimitBytes,
    );
    try {
      await leader.#loadFromDisk();
    } catch (error) {
      // A failed recovery must release every handle it acquired, or the browser's file lock
      // outlives the attempt and no connection can ever lead this database again.
      leader.crash();
      if (
        error instanceof StorageCorruptionError ||
        error instanceof StorageFormatVersionError ||
        error instanceof DOMException
      ) {
        throw error;
      }
      throw corruption("recovery", error instanceof Error ? error.message : String(error), error);
    }
    return leader;
  }

  async #loadFromDisk(): Promise<void> {
    const decodedSlots = this.#slots.map((slot, index) => {
      const size = slot.getSize();
      if (size === 0) return { index, size, state: undefined };
      if (size > MAX_OPFS_CHECKPOINT_BYTES) {
        return {
          index,
          size,
          state: undefined,
          error: new Error(
            `Checkpoint slot ${String(index)} exceeds ${String(MAX_OPFS_CHECKPOINT_BYTES)} bytes`,
          ),
        };
      }
      const bytes = new Uint8Array(size);
      readFully(slot, bytes, 0, `reading checkpoint slot ${String(index)}`);
      try {
        const decoded = decodeSyncCheckpoint(bytes);
        return {
          index,
          size,
          bytes,
          state: decoded === undefined ? undefined : validateCheckpointState(decoded),
        };
      } catch (error) {
        return { index, size, state: undefined, error };
      }
    });
    for (const slot of decodedSlots) {
      if ("error" in slot && slot.error instanceof StorageFormatVersionError) throw slot.error;
    }
    const validSlots = decodedSlots
      .filter((slot): slot is typeof slot & { state: CheckpointState } => slot.state !== undefined)
      .sort(
        (left, right) =>
          right.state.generation - left.state.generation ||
          right.state.lastSeq - left.state.lastSeq,
      );
    const walSize = this.#walHandle.getSize();
    if (walSize > MAX_OPFS_WAL_BYTES) {
      throw new Error(
        `OPFS WAL exceeds its ${String(MAX_OPFS_WAL_BYTES)} byte recovery limit: ${String(walSize)}`,
      );
    }
    if (validSlots.length === 0 && decodedSlots.some(({ size }) => size > 0) && walSize === 0) {
      throw new Error(
        "Every OPFS checkpoint copy is corrupt; refusing to open as an empty database",
      );
    }
    const selected = validSlots[0];
    const checkpoint = selected?.state;
    const firstCopy = validSlots[0]?.state;
    const secondCopy = validSlots[1]?.state;
    if (firstCopy !== undefined && secondCopy !== undefined) {
      if (firstCopy.generation > secondCopy.generation && firstCopy.lastSeq < secondCopy.lastSeq) {
        throw new Error("OPFS checkpoint generation moves its WAL sequence backwards");
      }
      if (
        firstCopy.generation === secondCopy.generation &&
        !equalBytes(validSlots[0]?.bytes ?? EMPTY_BYTES, validSlots[1]?.bytes ?? EMPTY_BYTES)
      ) {
        throw new Error("OPFS checkpoint copies disagree within the same generation");
      }
    }
    this.#newestSlot = selected?.index ?? 0;
    if (
      walSize === 0 &&
      validSlots.length === 2 &&
      (validSlots[0]?.state.generation !== validSlots[1]?.state.generation ||
        validSlots[0]?.state.lastSeq !== validSlots[1]?.state.lastSeq)
    ) {
      throw new Error(
        "OPFS checkpoint copies disagree without a WAL bridge; refusing a silent rollback",
      );
    }
    if (checkpoint !== undefined) await validateCheckpointPhysical(checkpoint, this.#tree);

    this.#blockIndex.clear();
    this.#ftsBases.clear();
    this.#ftsBuilds.clear();
    this.#activeFtsBuildCount = 0;
    this.#activeSecondaryBuildCount = 0;
    this.#acceleratorBuildStagedBytes = 0;
    this.#acceleratorBuildStagedEntries = 0;
    this.#extentPayloadRefs.clear();
    this.#tempPages.clear();
    this.#tempOwnerUsage.clear();
    this.#tempRunCount = 0;
    this.#tempPageBytes = 0;
    this.#tempLedgerNeedsCheckpoint = false;
    this.#ftsChunkCache.clear();
    this.#ftsChunkCacheBytes = 0;
    this.#extents?.close();
    this.#extents = await ExtentPool.open(this.#tree, checkpoint?.extents);
    this.#snapshotFrameExportLedger?.close();
    this.#snapshotFrameImportLedger?.close();
    this.#snapshotFrameExport = checkpoint?.snapshotFrameExport;
    this.#snapshotFrameImport = checkpoint?.snapshotFrameImport;
    this.#completedSnapshotFrameImport = checkpoint?.completedSnapshotFrameImport;
    this.#snapshotFrameExportLedger =
      this.#snapshotFrameExport === undefined
        ? undefined
        : await SnapshotFrameLedger.open(
            this.#tree,
            "export",
            this.#snapshotFrameExport.ledgerId,
            this.#snapshotFrameExport.ledgerLength,
            false,
          );
    const importLedgerState = this.#snapshotFrameImport ?? this.#completedSnapshotFrameImport;
    this.#snapshotFrameImportLedger =
      importLedgerState === undefined
        ? undefined
        : await SnapshotFrameLedger.open(
            this.#tree,
            "import",
            importLedgerState.ledgerId,
            importLedgerState.ledgerLength,
            false,
          );
    if (
      checkpoint !== undefined &&
      this.#snapshotFrameImport !== undefined &&
      !this.#snapshotFrameImport.completedReplay
    ) {
      const ledger = this.#snapshotFrameImportLedger;
      if (ledger === undefined) throw new Error("Snapshot frame import ledger is missing");
      const stagedPlacements = [...ledger.records(this.#snapshotFrameImport.ledgerLength)].flatMap(
        ({ record }) => (record.placement === undefined ? [] : [record.placement]),
      );
      validateLivePlacementAccounting(
        [...allCheckpointPlacements(checkpoint), ...stagedPlacements],
        checkpoint.extents,
      );
    }
    if (checkpoint === undefined) {
      this.#core.load(EMPTY_CORE_STATE());
      this.#seq = 0;
    } else {
      for (const [id, placement] of checkpoint.blockIndex) this.#setBlockPlacement(id, placement);
      this.#core.load(checkpoint.core);
      for (const [key, pointer] of checkpoint.ftsBases) this.#setFtsBasePointer(key, pointer);
      for (const [key, pointer] of checkpoint.ftsBuilds) this.#setFtsBuildPointer(key, pointer);
      await this.#validateStagedPostingBuilds();
      this.#applyTempPageUpdates(checkpoint.tempPages);
      this.#seq = checkpoint.lastSeq;
      this.#checkpointGeneration = checkpoint.generation;
    }

    const newest = this.#slots[this.#newestSlot];
    this.#lastCheckpointBytes = newest === undefined ? 0 : newest.getSize();
    let applied = 0;
    let recoveryEndOffset = 0;
    let previousWalSeq: number | undefined;
    let tailExtent = checkpoint?.extents.tailExtentId ?? 0;
    let tailEnd = checkpoint?.extents.tailOffset ?? 0;
    let nextExtent = checkpoint?.extents.nextExtentId ?? 1;
    for (const { payload, frameEnd } of iterateWalFrames(this.#walHandle)) {
      const entry = validateWalEntry(payload);
      if (previousWalSeq === undefined) {
        if (checkpoint === undefined && entry.seq !== 1) {
          throw new Error(`OPFS WAL starts at sequence ${String(entry.seq)} instead of 1`);
        }
        if (
          checkpoint !== undefined &&
          entry.seq > this.#seq &&
          !isSafeSuccessor(this.#seq, entry.seq)
        ) {
          throw new Error(
            `OPFS WAL sequence gap after checkpoint ${String(this.#seq)}: ${String(entry.seq)}`,
          );
        }
      } else if (!isSafeSuccessor(previousWalSeq, entry.seq)) {
        throw new Error(
          `OPFS WAL sequence is not consecutive: ${String(previousWalSeq)} then ${String(entry.seq)}`,
        );
      }
      previousWalSeq = entry.seq;
      if (entry.seq <= this.#seq) {
        recoveryEndOffset = frameEnd;
        continue; // Covered by the checkpoint; reset had not run yet.
      }
      if (!isSafeSuccessor(this.#seq, entry.seq)) {
        throw new Error(`OPFS WAL sequence gap after ${String(this.#seq)}: ${String(entry.seq)}`);
      }
      const placementCursorBeforeFrame = { tailExtent, tailEnd };
      for (const placement of placementsOf(entry)) {
        assertValidPlacement(placement);
        if (placement.extent === tailExtent) {
          if (placement.offset !== tailEnd) {
            throw new Error(
              `OPFS WAL placement is not append-only at sequence ${String(entry.seq)}: ` +
                `${String(placement.extent)}:${String(placement.offset)} after ${String(tailEnd)}`,
            );
          }
        } else {
          if (placement.extent !== nextExtent || placement.offset !== 0) {
            throw new Error(
              `OPFS WAL placement skips or reuses an extent at sequence ${String(entry.seq)}: ` +
                `${String(placement.extent)}:${String(placement.offset)}, expected ${String(nextExtent)}:0`,
            );
          }
          tailExtent = placement.extent;
          nextExtent = safeSuccessor(placement.extent, "OPFS extent ID");
          tailEnd = 0;
        }
        tailEnd += placement.length;
      }
      try {
        await this.#verifyEntryPayloads(entry);
      } catch (error) {
        if (this.#strict) {
          throw new Error(
            `Strict OPFS recovery found an invalid stored payload at WAL sequence ` +
              String(entry.seq),
            { cause: error },
          );
        }
        tailExtent = placementCursorBeforeFrame.tailExtent;
        tailEnd = placementCursorBeforeFrame.tailEnd;
        break;
      }
      applied += 1;
      this.#applyReplayed(entry);
      for (const placement of placementsOf(entry)) {
        this.#pool.restorePlacement(placement);
      }
      this.#seq = entry.seq;
      recoveryEndOffset = frameEnd;
    }
    this.#snapshotFrameExportLedger?.truncate(this.#snapshotFrameExportLedger.byteLength);
    this.#snapshotFrameImportLedger?.truncate(this.#snapshotFrameImportLedger.byteLength);
    if (tailExtent !== this.#pool.tailExtentId) {
      await this.#pool.adoptTail(tailExtent, tailEnd);
    }
    // A checksum-valid WAL frame can still reference extent bytes that a relaxed-durability
    // power loss did not persist. Discard that frame and every dependent successor; future
    // appends overwrite the inconsistent suffix from the last fully verified boundary.
    this.#wal = new WalWriter(this.#walHandle, recoveryEndOffset);
    this.#entriesSinceCheckpoint = applied;
    this.#poisoned = false;

    // WAL replay is an in-memory transaction until this point. Re-load its resolved record
    // state through RecordCore's exhaustive runtime/cross-record validator before any physical
    // reclamation or external request can observe it. This catches malformed nested WAL bodies
    // that a mutation's narrow hot-path validator could otherwise ignore, without cloning the
    // complete catalog once per frame (quadratic recovery on long logs).
    const replayValidator = new RecordCore({
      hasBlock: (id) => this.#blockIndex.has(id),
      blockByteLength: (id) => this.#blockIndex.get(id)?.length,
      blockChecksum: (id) => this.#blockIndex.get(id)?.checksum,
    });
    replayValidator.load(this.#core.dump());

    await this.#validateRecoveredSnapshotFrameSessions();

    const recoveryCutoff = dateIsoString(new Date());
    this.#core.validatePinnedRetiredLimits(recoveryCutoff);
    if (
      this.#snapshotFrameExport !== undefined &&
      Date.parse(this.#snapshotFrameExport.expiresAt) <= Date.parse(recoveryCutoff)
    ) {
      const session = this.#snapshotFrameExport;
      this.#core.removeLeaseIfExpired(
        snapshotFrameExportLeaseId(session.sessionId),
        session.leaseRevision,
        recoveryCutoff,
      );
      this.#snapshotFrameExportLedger?.close();
      this.#snapshotFrameExportLedger = undefined;
      this.#snapshotFrameExport = undefined;
      this.#queueSnapshotFrameLedgerCleanup("export", session.ledgerId);
    }
    if (
      this.#snapshotFrameImport !== undefined &&
      Date.parse(this.#snapshotFrameImport.expiresAt) <= Date.parse(recoveryCutoff)
    ) {
      const session = this.#snapshotFrameImport;
      if (!session.completedReplay) {
        this.#releasePlacements(this.#snapshotFrameImportPlacements(session));
      }
      this.#snapshotFrameImportLedger?.close();
      this.#snapshotFrameImportLedger = undefined;
      this.#snapshotFrameImport = undefined;
      if (!session.completedReplay) {
        this.#queueSnapshotFrameLedgerCleanup("import", session.ledgerId);
      }
    }

    validateLivePlacementAccounting(
      uniquePlacements([
        ...this.#blockIndex.values(),
        ...[...this.#ftsBases.values()].flatMap((pointer) => pointer.chunks),
        ...[...this.#ftsBuilds.values()].flatMap((pointer) => pointer.chunks),
        ...(this.#snapshotFrameImport === undefined || this.#snapshotFrameImport.completedReplay
          ? []
          : [...this.#snapshotFrameImportPlacements(this.#snapshotFrameImport)]),
      ]),
      this.#pool.meta(),
    );

    // Physical recovery cleanup is reclamation, not logical recovery. A platform can refuse a
    // delete/truncate while every published record and payload remains readable. Keep serving
    // that durable state, expose the debt, and let the ordinary bounded-maintenance path retry
    // it before accepting more payload growth.
    await this.#postCommitCleanup(() => this.#cleanupRecoveryArtifacts(recoveryEndOffset));
  }

  // ---------------------------------------------------------------------------------------
  // The logged-operation spine.
  // ---------------------------------------------------------------------------------------

  #run<T>(work: () => Promise<T> | T): Promise<T> {
    const result = this.#chain.then(async () => {
      if (this.#closed) throw new Error("This OPFS store connection is closed");
      if (this.#poisoned) await this.#loadFromDisk();
      return work();
    });
    this.#chain = result.catch(() => undefined);
    return result;
  }

  /**
   * Reads answer from memory without the queue — but never from a poisoned instance, whose
   * memory ran ahead of a write the disk refused. The fast path is one boolean check; the
   * poisoned path rides the queue, which reloads from the held handles first.
   */
  async #healthy(): Promise<void> {
    if (!this.#poisoned) return;
    await this.#run(() => undefined);
  }

  /**
   * The critical section: apply to the core and append the frame in one synchronous run.
   * A validation throw leaves the core untouched (validate-then-mutate bodies); a WAL write
   * failure after mutation poisons the leader, which reloads from its own handles before the
   * next operation, and the original error — quota, most importantly — escapes unwrapped.
   */
  #logged(body: WalEntryBody): unknown {
    if (this.#wal.byteLength >= MAX_OPFS_WAL_BYTES - 64 * 1024 * 1024) {
      // Keep one maximum-sized frame of headroom. A checkpoint refusal happens before the
      // record state mutates, applying bounded backpressure instead of growing forever.
      this.checkpointNow();
    }
    // Strict durability is per published frame, not per payload. A batch may append many
    // blocks into the tail; flush that dirty tail once after all writes complete and before
    // either the in-memory mutation or the WAL frame can publish them. Sealing already flushes
    // each completed extent, so multi-extent batches pay the irreducible one flush per file.
    if (this.#strict) {
      try {
        this.#pool.flush();
      } catch (error) {
        // Appends have advanced the pool's in-memory tail/accounting but nothing has been
        // published. Reload before any later operation so dead batch bytes become overwriteable
        // space exactly as they do after a refused WAL append.
        this.#poisoned = true;
        throw error;
      }
    }
    // Refuse before the logical mutation. Once the sequence space is exhausted there is no
    // canonical WAL frame that can durably acknowledge another state transition.
    safeSuccessor(this.#seq, "OPFS WAL sequence");
    safeSuccessor(this.#entriesSinceCheckpoint, "OPFS checkpoint entry count");
    const result = this.#applyBody(body);
    this.#clearCompletedSnapshotImportIfAdvanced();
    this.#appendFrame(body);
    return result;
  }

  #appendFrame(body: WalEntryBody): void {
    try {
      const nextSeq = safeSuccessor(this.#seq, "OPFS WAL sequence");
      this.#wal.append({ seq: nextSeq, ...body }, this.#strict);
      this.#seq = nextSeq;
      this.#entriesSinceCheckpoint = safeSuccessor(
        this.#entriesSinceCheckpoint,
        "OPFS checkpoint entry count",
      );
    } catch (error) {
      this.#poisoned = true;
      throw error;
    }
    // The checkpoint runs as its own queued step: the operation that trips the threshold is
    // already durable and should not absorb the pause — or a checkpoint failure. A failed
    // checkpoint (quota, most plausibly) leaves the WAL covering everything and retries on a
    // later trigger; the operations themselves stay correct throughout.
    if (
      this.#checkpointDue() &&
      this.#entriesSinceCheckpoint >= this.#checkpointRetryAtEntries &&
      !this.#checkpointScheduled
    ) {
      this.#checkpointScheduled = true;
      void this.#run(() => {
        this.#checkpointScheduled = false;
        if (this.#checkpointDue()) {
          this.checkpointNow();
        }
      }).catch(() => undefined);
    }
  }

  /**
   * Due when the WAL outgrows the larger of the floor and the previous checkpoint's own size —
   * so checkpointing amortizes to O(bytes written) instead of re-serializing the whole state
   * every fixed interval during a bulk load — or when the frame count alone would make a
   * replay slow.
   */
  #checkpointDue(): boolean {
    return (
      this.#wal.byteLength >= Math.max(CHECKPOINT_WAL_BYTES, this.#lastCheckpointBytes) ||
      this.#entriesSinceCheckpoint >= this.#checkpointEntries
    );
  }

  #applyBody(body: WalEntryBody): unknown {
    switch (body.op) {
      case "relocatePayloads": {
        // Validate the complete move before changing an index: a stale relocation must leave
        // both indexes and live-byte accounting untouched.
        for (const move of body.blocks) {
          if (!samePlacement(this.#blockIndex.get(move.id), move.from)) {
            throw new Error(`OPFS block moved before relocation: ${move.id}`);
          }
        }
        for (const move of body.ftsChunks) {
          if (!samePlacement(this.#ftsBases.get(move.key)?.chunks[move.ordinal], move.from)) {
            throw new Error(`OPFS full-text chunk moved before relocation: ${move.key}`);
          }
        }
        for (const move of body.ftsBuildChunks) {
          const build = this.#ftsBuilds.get(move.key);
          if (
            build?.buildId !== move.buildId ||
            !samePlacement(build.chunks[move.ordinal], move.from)
          ) {
            throw new Error(`OPFS staged postings chunk moved before relocation: ${move.key}`);
          }
        }
        for (const move of body.blocks) this.#setBlockPlacement(move.id, move.placement);
        for (const move of body.ftsChunks) {
          const pointer = this.#ftsBases.get(move.key);
          if (pointer === undefined) throw new Error(`OPFS full-text base is missing: ${move.key}`);
          this.#untrackPayload({ kind: "fts", key: move.key, ordinal: move.ordinal }, move.from);
          pointer.chunks[move.ordinal] = move.placement;
          this.#trackPayload({ kind: "fts", key: move.key, ordinal: move.ordinal }, move.placement);
          this.#dropFtsChunkCache(move.key);
        }
        for (const move of body.ftsBuildChunks) {
          const build = this.#ftsBuilds.get(move.key);
          if (build === undefined) {
            throw new Error(`OPFS staged postings build is missing: ${move.key}`);
          }
          this.#untrackPayload(
            {
              kind: "fts-build",
              key: move.key,
              buildId: move.buildId,
              ordinal: move.ordinal,
            },
            move.from,
          );
          build.chunks[move.ordinal] = move.placement;
          this.#trackPayload(
            {
              kind: "fts-build",
              key: move.key,
              buildId: move.buildId,
              ordinal: move.ordinal,
            },
            move.placement,
          );
        }
        this.#releasePlacements([
          ...body.blocks.map((move) => move.from),
          ...body.ftsChunks.map((move) => move.from),
          ...body.ftsBuildChunks.map((move) => move.from),
        ]);
        return undefined;
      }
      case "addTable":
        return this.#core.addTable(
          body.record,
          body.expectedCatalogEpoch === null
            ? {}
            : { expectedCatalogEpoch: body.expectedCatalogEpoch },
        );
      case "updateTable": {
        const before = this.#core.getTable(body.id);
        const updated = this.#core.updateTable(body.id, body.expectedRevision, body.update);
        if (before !== undefined) {
          const beforeStorageIds = new Set([
            ...Object.keys(before.ftsColumns ?? {}),
            ...Object.values(before.secondaryIndexes ?? {}).map((index) => index.storageColumnId),
          ]);
          const retainedStorageIds = new Set([
            ...Object.keys(updated.ftsColumns ?? {}),
            ...Object.values(updated.secondaryIndexes ?? {}).map((index) => index.storageColumnId),
          ]);
          for (const storageColumnId of beforeStorageIds) {
            if (retainedStorageIds.has(storageColumnId)) continue;
            const key = postingStorageKey(before.id, storageColumnId);
            const pointer = this.#ftsBases.get(key);
            if (pointer !== undefined) {
              this.#deleteFtsBasePointer(key);
              this.#dropFtsChunkCache(key);
              this.#releasePlacements(pointer.chunks);
            }
            const build = this.#ftsBuilds.get(key);
            if (build !== undefined) {
              this.#deleteFtsBuildPointer(key);
              this.#releasePlacements(build.chunks);
            }
          }
        }
        return updated;
      }
      case "removeTable": {
        this.#core.removeTable(
          body.id,
          body.expectedRevision,
          body.expectedCatalogEpoch === null
            ? {}
            : { expectedCatalogEpoch: body.expectedCatalogEpoch },
        );
        for (const [key, pointer] of [...this.#ftsBases]) {
          if (parsePostingStorageKey(key, "postings base")[0] !== body.id) continue;
          this.#deleteFtsBasePointer(key);
          this.#dropFtsChunkCache(key);
          this.#releasePlacements(pointer.chunks);
        }
        for (const [key, build] of [...this.#ftsBuilds]) {
          if (parsePostingStorageKey(key, "postings build")[0] !== body.id) continue;
          this.#deleteFtsBuildPointer(key);
          this.#releasePlacements(build.chunks);
        }
        return undefined;
      }
      case "dropTable": {
        const summary = this.#core.dropTable(body.input);
        for (const [key, pointer] of [...this.#ftsBases]) {
          if (parsePostingStorageKey(key, "postings base")[0] !== body.input.tableId) continue;
          this.#deleteFtsBasePointer(key);
          this.#dropFtsChunkCache(key);
          this.#releasePlacements(pointer.chunks);
        }
        for (const [key, build] of [...this.#ftsBuilds]) {
          if (parsePostingStorageKey(key, "postings build")[0] !== body.input.tableId) continue;
          this.#deleteFtsBuildPointer(key);
          this.#releasePlacements(build.chunks);
        }
        return summary;
      }
      case "dropTableColumn": {
        const summary = this.#core.dropTableColumn(body.input);
        const key = postingStorageKey(body.input.tableId, body.input.columnId);
        const pointer = this.#ftsBases.get(key);
        if (pointer !== undefined) {
          this.#deleteFtsBasePointer(key);
          this.#dropFtsChunkCache(key);
          this.#releasePlacements(pointer.chunks);
        }
        const build = this.#ftsBuilds.get(key);
        if (build !== undefined) {
          this.#deleteFtsBuildPointer(key);
          this.#releasePlacements(build.chunks);
        }
        return summary;
      }
      case "removeFtsColumn": {
        this.#core.removeFtsColumn(body.tableId, body.columnId);
        const key = postingStorageKey(body.tableId, body.columnId);
        const pointer = this.#ftsBases.get(key);
        if (pointer !== undefined) {
          this.#deleteFtsBasePointer(key);
          this.#dropFtsChunkCache(key);
          this.#releasePlacements(pointer.chunks);
        }
        const build = this.#ftsBuilds.get(key);
        if (build !== undefined) {
          this.#deleteFtsBuildPointer(key);
          this.#releasePlacements(build.chunks);
        }
        return undefined;
      }
      case "removeAbortedSegment":
        return this.#core.removeAbortedSegment(body.id, body.expectedTransactionId);
      case "adoptAbortedSegment":
        return this.#core.adoptAbortedSegment(body.input);
      case "reserveRowIds":
        return this.#core.reserveRowIds(body.tableId, body.count);
      case "beginUniqueKeyBuild":
        return this.#core.beginUniqueKeyBuild(body.input);
      case "renewUniqueKeyBuild":
        return this.#core.renewUniqueKeyBuild(body.input);
      case "appendUniqueKeyBuildChunk": {
        const current = this.#core.getUniqueKeyBuild(body.input.buildId);
        if (current?.state === "active" && body.input.ordinal === current.nextOrdinal) {
          this.#assertUniqueBuildUsage(
            uniqueKeyBuildChunkRetainedBytes(body.input.keyTokens),
            body.input.keyTokens.length,
          );
        }
        return this.#core.appendUniqueKeyBuildChunk(body.input);
      }
      case "finishUniqueKeyBuild":
        return this.#core.finishUniqueKeyBuild(body.input);
      case "abortUniqueKeyBuild":
        return this.#core.abortUniqueKeyBuild(body.input);
      case "reserveAutoIncrement":
        return this.#core.reserveAutoIncrement(
          body.tableId,
          body.columnId,
          body.count,
          body.atLeast,
        );
      case "beginTransaction":
        return this.#core.beginTransaction(body.input);
      case "createTransaction":
        return this.#core.createTransaction(body.record);
      case "renewTransaction":
        return this.#core.renewTransaction(body.input);
      case "abortTransactionIfExpired":
        return this.#core.abortTransactionIfExpired(body.input);
      case "updateTransaction":
        return this.#core.updateTransaction(body.id, body.expectedRevision, body.update);
      case "stageTransactionArtifacts": {
        const updated = this.#core.stageTransactionArtifacts(
          {
            transactionId: body.transactionId,
            expectedRevision: body.expectedRevision,
            blocks: body.blocks.map(({ id }) => ({ id, bytes: EMPTY_BYTES })),
            segments: body.segments,
            updatedAt: body.updatedAt,
          },
          {
            blocksPrevalidated: true,
            blockByteLengths: new Map(
              body.blocks.map(({ id, placement }) => [id, placement.length]),
            ),
          },
        );
        for (const { id, placement } of body.blocks) this.#setBlockPlacement(id, placement);
        return updated;
      }
      case "rollbackTransactionArtifacts": {
        const updated = this.#core.rollbackTransactionArtifacts(body.input);
        this.#releaseReclaimedBlocks(body.input.removeBlockIds);
        return updated;
      }
      case "commitTransaction":
        return this.#core.commitTransaction(body.input);
      case "writeTransaction": {
        // The core counts this step's own blocks as present, so the index is set only once the
        // whole write has been accepted — a refusal leaves no stray placements behind.
        const summary = this.#core.writeTransaction(
          { ...body.input, blocks: body.blocks.map(({ id }) => ({ id, bytes: EMPTY_BYTES })) },
          {
            blocksPrevalidated: true,
            blockByteLengths: new Map(
              body.blocks.map(({ id, placement }) => [id, placement.length]),
            ),
            blockChecksums: new Map(
              body.blocks.map(({ id, placement }) => [id, placement.checksum]),
            ),
          },
        );
        for (const { id, placement } of body.blocks) this.#setBlockPlacement(id, placement);
        return summary;
      }
      case "createLease":
        return this.#core.createLease(body.record);
      case "renewLease":
        return this.#core.renewLease(body.input);
      case "moveLease":
        return this.#core.moveLease(body.input);
      case "removeLeaseIfExpired":
        return this.#core.removeLeaseIfExpired(
          body.id,
          body.expectedRevision,
          body.expiresAtCutoff,
        );
      case "removeLease":
        return this.#core.removeLease(body.input);
      case "createCompactionJob":
        return this.#core.createCompactionJob(body.record);
      case "updateCompactionJob":
        return this.#core.updateCompactionJob(body.id, body.expectedRevision, body.update);
      case "cancelCompactionJob":
        return this.#core.cancelCompactionJob(body.id, body.expectedRevision, body.cancelledAt);
      case "removeCompactionJob":
        return this.#core.removeCompactionJob(body.id);
      case "createGarbageCollectionJob":
        return this.#core.createGarbageCollectionJob(body.input);
      case "updateGarbageCollectionPlanning":
        return this.#core.updateGarbageCollectionPlanning(body.input);
      case "garbageCollectionStep": {
        this.#core.applyGarbageCollectionEffect(body.effect);
        this.#releaseReclaimedBlocks(body.effect.reclaimedBlockIds);
        return undefined;
      }
      case "removeGarbageCollectionJob":
        return this.#core.removeGarbageCollectionJob(body.id);
      case "removePrunedManifestRecords":
        return this.#core.removePrunedManifestRecords(body.maxItems);
      case "createTempOwner":
        return this.#core.createTempOwner(body.record);
      case "renewTempOwner":
        return this.#core.renewTempOwner(body.input);
      case "removeTempOwnerIfExpired":
        if (!this.#core.removeTempOwnerIfExpired(body.ownerId, body.expiresAtCutoff)) return false;
        this.#removeTempLedgerOwner(body.ownerId);
        return true;
      case "removeTempOwner":
        this.#core.removeTempOwner(body.ownerId);
        this.#removeTempLedgerOwner(body.ownerId);
        return undefined;
      case "reserveTempRunPages":
        this.#applyTempPageUpdates(body.pages);
        return undefined;
      case "restoreTempRunPages":
        this.#applyTempPageUpdates(body.pages);
        return undefined;
      case "removeTempRun":
        this.#removeTempLedgerRun(body.ownerId, body.runId);
        return undefined;
      case "writeFtsBase": {
        const table = this.#core.getTable(body.tableId);
        if (table === undefined || !activePostingStorageColumnIds(table).has(body.columnId)) {
          throw new Error(`Postings index is no longer active: ${body.tableId}/${body.columnId}`);
        }
        const key = postingStorageKey(body.tableId, body.columnId);
        const build = this.#ftsBuilds.get(key);
        if (build !== undefined) {
          this.#deleteFtsBuildPointer(key);
          this.#releasePlacements(build.chunks);
        }
        const previous = this.#ftsBases.get(key);
        if (previous !== undefined) {
          this.#deleteFtsBasePointer(key);
          this.#releasePlacements(previous.chunks);
          this.#dropFtsChunkCache(key);
        }
        this.#setFtsBasePointer(key, structuredClone(body.pointer));
        this.#core.pruneFtsDeltas(body.tableId, body.columnId, body.pointer.coversVersion);
        return undefined;
      }
      case "beginFtsBaseBuild": {
        const { input } = body;
        validatePostingBuildBegin(input);
        const table = this.#core.getTable(input.tableId);
        if (table === undefined || !activePostingStorageColumnIds(table).has(input.columnId)) {
          throw new Error(`Postings index is no longer active: ${input.tableId}/${input.columnId}`);
        }
        const key = postingStorageKey(input.tableId, input.columnId);
        const ownerKind = this.#postingBuildOwnerKind(table, input.columnId);
        const previous = this.#ftsBuilds.get(key);
        if (previous !== undefined) {
          if (
            previous.buildId === input.buildId &&
            previous.ownerId === input.ownerId &&
            previous.createdAt === input.createdAt &&
            previous.expiresAt === input.expiresAt
          ) {
            return undefined;
          }
          if (Date.parse(previous.expiresAt) > Date.parse(input.createdAt)) {
            throw new PostingBuildConflictError(
              input.buildId,
              input.ownerId,
              "another live build exists",
            );
          }
        }
        this.#replaceExpiredPostingBuilds(key, input.createdAt, {
          buildId: input.buildId,
          ownerId: input.ownerId,
          ownerKind,
          createdAt: input.createdAt,
          expiresAt: input.expiresAt,
          updatedAt: input.createdAt,
          chunks: [],
          chunkBounds: [],
          totalTokens: 0,
          retainedBytes: 0,
          retainedEntries: 0,
        });
        return undefined;
      }
      case "renewFtsBaseBuild": {
        const build = this.#requireLivePostingBuild(body.input);
        build.expiresAt = laterTimestamp(build.expiresAt, body.input.expiresAt);
        build.updatedAt = body.input.updatedAt;
        return undefined;
      }
      case "writeFtsBaseBuildChunk": {
        const build = this.#requireLivePostingBuild(body.input);
        if (body.input.ordinal !== build.chunks.length) {
          throw new Error(`Full-text base chunk is out of order: ${String(body.input.ordinal)}`);
        }
        const next: FtsBaseBuildPointer = {
          ...build,
          chunks: [...build.chunks, body.placement],
          chunkBounds: [...build.chunkBounds, body.bounds],
          totalTokens: safeByteSum(
            build.totalTokens,
            body.totalTokens,
            "Full-text base build token count",
          ),
          retainedBytes: safeByteSum(
            build.retainedBytes,
            body.placement.length,
            "Accelerator build staged bytes",
          ),
          retainedEntries: safeByteSum(
            build.retainedEntries,
            body.retainedEntries,
            "Accelerator build staged entries",
          ),
          expiresAt: laterTimestamp(build.expiresAt, body.input.expiresAt),
          updatedAt: body.input.updatedAt,
        };
        this.#setFtsBuildPointer(postingStorageKey(body.input.tableId, body.input.columnId), next);
        return undefined;
      }
      case "finishFtsBaseBuild": {
        const input = body.input;
        const build = this.#requireLivePostingBuild(input);
        const key = postingStorageKey(input.tableId, input.columnId);
        if (build.chunks.length !== input.chunkCount || build.totalTokens !== input.totalTokens) {
          throw new Error(`Full-text base build is incomplete: ${input.buildId}`);
        }
        const table = this.#core.getTable(input.tableId);
        if (table === undefined || !activePostingStorageColumnIds(table).has(input.columnId)) {
          this.#deleteFtsBuildPointer(key);
          this.#releasePlacements(build.chunks);
          return undefined;
        }
        const previous = this.#ftsBases.get(key);
        if (previous !== undefined) {
          this.#deleteFtsBasePointer(key);
          this.#releasePlacements(previous.chunks);
          this.#dropFtsChunkCache(key);
        }
        this.#deleteFtsBuildPointer(key);
        this.#setFtsBasePointer(key, {
          coversVersion: input.coversVersion,
          totalTokens: input.totalTokens,
          chunks: build.chunks,
          chunkBounds: build.chunkBounds,
        });
        this.#core.pruneFtsDeltas(input.tableId, input.columnId, input.coversVersion);
        return undefined;
      }
      case "abortFtsBaseBuild": {
        const input = body.input;
        validatePostingBuildAbort(input);
        const key = postingStorageKey(input.tableId, input.columnId);
        const build = this.#ftsBuilds.get(key);
        if (build === undefined) return undefined;
        if (
          build.buildId !== input.buildId ||
          (build.ownerId !== input.ownerId &&
            Date.parse(build.expiresAt) > Date.parse(input.expiresAtCutoff))
        ) {
          throw new PostingBuildConflictError(input.buildId, input.ownerId, "ownership changed");
        }
        this.#deleteFtsBuildPointer(key);
        this.#releasePlacements(build.chunks);
        return undefined;
      }
      case "beginSnapshotFrameExport": {
        const previous = this.#snapshotFrameExport;
        if (previous !== undefined) {
          if (Date.parse(previous.expiresAt) > Date.parse(body.state.createdAt)) {
            throw new Error(`Snapshot frame export is already active: ${previous.sessionId}`);
          }
          this.#core.removeLease({
            id: snapshotFrameExportLeaseId(previous.sessionId),
            ownerId: previous.ownerId,
          });
        }
        this.#core.createLease({
          id: snapshotFrameExportLeaseId(body.state.sessionId),
          ownerId: body.state.ownerId,
          kind: "backup",
          manifestVersion: body.state.manifestVersion,
          createdAt: body.state.createdAt,
          expiresAt: body.state.expiresAt,
          revision: 0,
        });
        this.#snapshotFrameExport = structuredClone(body.state);
        if (this.#preparedSnapshotFrameExportLedger !== undefined) {
          this.#snapshotFrameExportLedger?.close();
          this.#snapshotFrameExportLedger = this.#preparedSnapshotFrameExportLedger;
          this.#preparedSnapshotFrameExportLedger = undefined;
        }
        return snapshotFrameExportSession(body.state);
      }
      case "advanceSnapshotFrameExport": {
        const session = this.#requireSnapshotFrameExport(body.input.sessionId, body.input.ownerId);
        validateSnapshotFrameExportAdvance(session, body.input.sequence, body.next);
        const lease = this.#core.renewLease({
          id: snapshotFrameExportLeaseId(session.sessionId),
          expectedRevision: body.expectedLeaseRevision,
          expiresAtCutoff: body.input.expiresAtCutoff,
          expiresAt: body.input.expiresAt,
        });
        Object.assign(session, structuredClone(body.next), {
          expiresAt: lease.expiresAt,
          leaseRevision: lease.revision,
        });
        return undefined;
      }
      case "closeSnapshotFrameExport": {
        const session = this.#snapshotFrameExport;
        if (session === undefined) return false;
        if (session.sessionId !== body.input.sessionId || session.ownerId !== body.input.ownerId) {
          throw new Error(`Snapshot frame export ownership changed: ${body.input.sessionId}`);
        }
        this.#core.removeLease({
          id: snapshotFrameExportLeaseId(session.sessionId),
          ownerId: session.ownerId,
        });
        this.#snapshotFrameExport = undefined;
        return true;
      }
      case "beginSnapshotFrameImport": {
        const previous = this.#snapshotFrameImport;
        if (
          previous !== undefined &&
          !previous.completedReplay &&
          (previous.ledgerId !== body.state.ledgerId || body.state.completedReplay)
        ) {
          this.#releasePlacements(this.#snapshotFrameImportPlacements(previous));
        }
        this.#snapshotFrameImport = structuredClone(body.state);
        if (this.#preparedSnapshotFrameImportLedger !== undefined) {
          this.#snapshotFrameImportLedger?.close();
          this.#snapshotFrameImportLedger = this.#preparedSnapshotFrameImportLedger;
          this.#preparedSnapshotFrameImportLedger = undefined;
        }
        return snapshotFrameImportSession(body.state);
      }
      case "renewSnapshotFrameImport": {
        const session = this.#requireSnapshotFrameImport(body.input);
        if (Date.parse(body.input.expiresAt) > Date.parse(session.expiresAt)) {
          session.expiresAt = body.input.expiresAt;
        }
        return snapshotFrameImportSession(session);
      }
      case "appendSnapshotImportFrames": {
        const current = this.#requireSnapshotFrameImport(body.input);
        if (current.completedReplay !== body.replay) {
          throw new Error("Snapshot frame import replay mode changed");
        }
        const next = { ...structuredClone(body.state), header: current.header };
        validateSnapshotFrameImportState(next);
        validateSnapshotFrameImportTransition(current, next, body.blockPlacements);
        this.#snapshotFrameImport = next;
        return snapshotFrameImportSession(next);
      }
      case "finishSnapshotFrameImport": {
        const session = this.#requireSnapshotFrameImport(body.input);
        validateSnapshotFrameFooter(session, body.input.footer);
        if (!session.completedReplay) this.#promoteSnapshotFrameImport(session);
        else if (this.#core.getCurrentManifestVersion() !== session.version) {
          throw new SnapshotImportConflictError(
            session.identity,
            session.ownerId,
            "completed snapshot database advanced",
          );
        }
        this.#completedSnapshotFrameImport = {
          identity: session.identity,
          version: session.version,
          header: session.header,
          ledgerId: session.ledgerId,
          ledgerLength: session.ledgerLength,
        };
        this.#snapshotFrameImport = undefined;
        return undefined;
      }
      case "cancelSnapshotFrameImport": {
        const session = this.#snapshotFrameImport;
        if (session?.identity !== body.input.identity) {
          return { identity: body.input.identity, removedBlockCount: 0, removedBytes: 0 };
        }
        if (session.ownerId !== body.input.ownerId) {
          throw new Error(`Snapshot frame import ownership changed: ${body.input.identity}`);
        }
        if (!session.completedReplay) {
          this.#releasePlacements(this.#snapshotFrameImportPlacements(session));
        }
        const result = {
          identity: session.identity,
          removedBlockCount: session.completedReplay ? 0 : session.blockCount,
          removedBytes: session.completedReplay ? 0 : session.blockBytes,
        };
        this.#snapshotFrameImport = undefined;
        return result;
      }
      default:
        throw new Error(`Unsupported OPFS WAL operation: ${String((body as { op?: unknown }).op)}`);
    }
  }

  #requireLivePostingBuild(input: {
    tableId: string;
    columnId: string;
    buildId: string;
    ownerId: string;
    expiresAtCutoff: string;
  }): FtsBaseBuildPointer {
    const build = this.#ftsBuilds.get(postingStorageKey(input.tableId, input.columnId));
    if (
      build?.buildId !== input.buildId ||
      build.ownerId !== input.ownerId ||
      Date.parse(build.expiresAt) <= Date.parse(input.expiresAtCutoff)
    ) {
      throw new PostingBuildConflictError(
        input.buildId,
        input.ownerId,
        "ownership changed or expired",
      );
    }
    return build;
  }

  async #validateStagedPostingBuilds(): Promise<void> {
    for (const [key, build] of this.#ftsBuilds) {
      const [tableId, columnId] = parsePostingStorageKey(key, "staged full-text base");
      const table = this.#core.getTable(tableId);
      if (
        table === undefined ||
        !activePostingStorageColumnIds(table).has(columnId) ||
        this.#postingBuildOwnerKind(table, columnId) !== build.ownerKind
      ) {
        throw new Error(`Staged postings build has invalid catalog ownership: ${key}`);
      }
      let retainedBytes = 0;
      let retainedEntries = 0;
      let totalTokens = 0;
      for (const [ordinal, placement] of build.chunks.entries()) {
        retainedBytes = safeByteSum(
          retainedBytes,
          placement.length,
          "Staged postings retained bytes",
        );
        const chunk = verifyFtsChunkBytes(
          await this.#pool.readVerified(placement),
          build.chunkBounds[ordinal],
          `Staged postings ${key} chunk ${String(ordinal)}`,
        );
        for (const posting of chunk) {
          retainedEntries = safeByteSum(
            retainedEntries,
            posting.rowIds.length,
            "Staged postings retained entries",
          );
          for (const frequency of posting.tf) {
            totalTokens = safeByteSum(totalTokens, frequency, "Staged postings token count");
          }
        }
      }
      if (
        retainedBytes !== build.retainedBytes ||
        retainedEntries !== build.retainedEntries ||
        totalTokens !== build.totalTokens
      ) {
        throw new Error(`Staged postings accounting is corrupt: ${key}`);
      }
    }
  }

  #postingBuildOwnerKind(
    table: TableRecord,
    storageColumnId: string,
  ): "fts-column" | "secondary-index" {
    if (table.ftsColumns?.[storageColumnId] !== undefined) return "fts-column";
    for (const index of Object.values(table.secondaryIndexes ?? {})) {
      if (index.storageColumnId === storageColumnId) return "secondary-index";
    }
    throw new Error(`Postings index is no longer active: ${table.id}/${storageColumnId}`);
  }

  #replaceExpiredPostingBuilds(key: string, cutoff: string, pointer: FtsBaseBuildPointer): void {
    const removals = new Map<string, FtsBaseBuildPointer>();
    const sameKey = this.#ftsBuilds.get(key);
    if (sameKey !== undefined) removals.set(key, sameKey);
    let removedForKind = Number(sameKey?.ownerKind === pointer.ownerKind);
    const count =
      pointer.ownerKind === "fts-column"
        ? this.#activeFtsBuildCount
        : this.#activeSecondaryBuildCount;
    const limit =
      pointer.ownerKind === "fts-column"
        ? MAX_ACTIVE_FTS_BASE_BUILDS
        : MAX_ACTIVE_SECONDARY_INDEX_BUILDS;
    if (count - removedForKind + 1 > limit) {
      for (const [candidateKey, build] of this.#ftsBuilds) {
        if (
          removals.has(candidateKey) ||
          build.ownerKind !== pointer.ownerKind ||
          build.expiresAt > cutoff
        ) {
          continue;
        }
        removals.set(candidateKey, build);
        removedForKind += 1;
        if (removals.size === 64 || count - removedForKind + 1 <= limit) break;
      }
    }
    const projectedFts =
      this.#activeFtsBuildCount -
      [...removals.values()].filter((build) => build.ownerKind === "fts-column").length +
      Number(pointer.ownerKind === "fts-column");
    const projectedSecondary =
      this.#activeSecondaryBuildCount -
      [...removals.values()].filter((build) => build.ownerKind === "secondary-index").length +
      Number(pointer.ownerKind === "secondary-index");
    if (projectedFts > MAX_ACTIVE_FTS_BASE_BUILDS) {
      throw new StorageResourceLimitError(
        "full-text build",
        projectedFts,
        MAX_ACTIVE_FTS_BASE_BUILDS,
      );
    }
    if (projectedSecondary > MAX_ACTIVE_SECONDARY_INDEX_BUILDS) {
      throw new StorageResourceLimitError(
        "secondary-index build",
        projectedSecondary,
        MAX_ACTIVE_SECONDARY_INDEX_BUILDS,
      );
    }
    const removedBytes = [...removals.values()].reduce(
      (sum, build) => safeByteSum(sum, build.retainedBytes, "Accelerator build staged bytes"),
      0,
    );
    const removedEntries = [...removals.values()].reduce(
      (sum, build) => safeByteSum(sum, build.retainedEntries, "Accelerator build staged entries"),
      0,
    );
    const unique = this.#core.uniqueKeyBuildStagedUsage();
    const projectedBytes = safeByteSum(
      this.#acceleratorBuildStagedBytes - removedBytes,
      safeByteSum(pointer.retainedBytes, unique.bytes, "Accelerator build staged bytes"),
      "Accelerator build staged bytes",
    );
    const projectedEntries = safeByteSum(
      this.#acceleratorBuildStagedEntries - removedEntries,
      safeByteSum(pointer.retainedEntries, unique.entries, "Accelerator build staged entries"),
      "Accelerator build staged entries",
    );
    if (projectedBytes > MAX_ACCELERATOR_BUILD_STAGED_BYTES_TOTAL) {
      throw new StorageResourceLimitError(
        "accelerator build byte",
        projectedBytes,
        MAX_ACCELERATOR_BUILD_STAGED_BYTES_TOTAL,
      );
    }
    if (projectedEntries > MAX_ACCELERATOR_BUILD_STAGED_ENTRIES_TOTAL) {
      throw new StorageResourceLimitError(
        "accelerator build entry",
        projectedEntries,
        MAX_ACCELERATOR_BUILD_STAGED_ENTRIES_TOTAL,
      );
    }
    for (const [candidateKey, build] of removals) {
      this.#deleteFtsBuildPointer(candidateKey);
      this.#releasePlacements(build.chunks);
    }
    this.#setFtsBuildPointer(key, pointer);
  }

  #assertFtsBuildUsage(
    ownerKind: FtsBaseBuildPointer["ownerKind"],
    byteDelta: number,
    entryDelta: number,
    replacing?: FtsBaseBuildPointer,
  ): void {
    const nextFts =
      this.#activeFtsBuildCount -
      Number(replacing?.ownerKind === "fts-column") +
      Number(ownerKind === "fts-column");
    const nextSecondary =
      this.#activeSecondaryBuildCount -
      Number(replacing?.ownerKind === "secondary-index") +
      Number(ownerKind === "secondary-index");
    if (nextFts > MAX_ACTIVE_FTS_BASE_BUILDS) {
      throw new StorageResourceLimitError("full-text build", nextFts, MAX_ACTIVE_FTS_BASE_BUILDS);
    }
    if (nextSecondary > MAX_ACTIVE_SECONDARY_INDEX_BUILDS) {
      throw new StorageResourceLimitError(
        "secondary-index build",
        nextSecondary,
        MAX_ACTIVE_SECONDARY_INDEX_BUILDS,
      );
    }
    const unique = this.#core.uniqueKeyBuildStagedUsage();
    const previousBytes = replacing?.retainedBytes ?? 0;
    const previousEntries = replacing?.retainedEntries ?? 0;
    const baseBytes = this.#acceleratorBuildStagedBytes - previousBytes;
    const baseEntries = this.#acceleratorBuildStagedEntries - previousEntries;
    if (baseBytes < 0 || baseEntries < 0) {
      throw new Error("Accelerator build staged accounting underflow");
    }
    const bytes = safeByteSum(
      safeByteSum(baseBytes, byteDelta, "Accelerator build staged bytes"),
      unique.bytes,
      "Accelerator build staged bytes",
    );
    const entries = safeByteSum(
      safeByteSum(baseEntries, entryDelta, "Accelerator build staged entries"),
      unique.entries,
      "Accelerator build staged entries",
    );
    if (bytes > MAX_ACCELERATOR_BUILD_STAGED_BYTES_TOTAL) {
      throw new StorageResourceLimitError(
        "accelerator build byte",
        bytes,
        MAX_ACCELERATOR_BUILD_STAGED_BYTES_TOTAL,
      );
    }
    if (entries > MAX_ACCELERATOR_BUILD_STAGED_ENTRIES_TOTAL) {
      throw new StorageResourceLimitError(
        "accelerator build entry",
        entries,
        MAX_ACCELERATOR_BUILD_STAGED_ENTRIES_TOTAL,
      );
    }
  }

  #assertUniqueBuildUsage(byteDelta: number, entryDelta: number): void {
    const unique = this.#core.uniqueKeyBuildStagedUsage();
    const bytes = safeByteSum(
      this.#acceleratorBuildStagedBytes,
      safeByteSum(unique.bytes, byteDelta, "Accelerator build staged bytes"),
      "Accelerator build staged bytes",
    );
    const entries = safeByteSum(
      this.#acceleratorBuildStagedEntries,
      safeByteSum(unique.entries, entryDelta, "Accelerator build staged entries"),
      "Accelerator build staged entries",
    );
    if (bytes > MAX_ACCELERATOR_BUILD_STAGED_BYTES_TOTAL) {
      throw new StorageResourceLimitError(
        "accelerator build byte",
        bytes,
        MAX_ACCELERATOR_BUILD_STAGED_BYTES_TOTAL,
      );
    }
    if (entries > MAX_ACCELERATOR_BUILD_STAGED_ENTRIES_TOTAL) {
      throw new StorageResourceLimitError(
        "accelerator build entry",
        entries,
        MAX_ACCELERATOR_BUILD_STAGED_ENTRIES_TOTAL,
      );
    }
  }

  #applyReplayed(entry: WalEntry): void {
    this.#permissivePhysical = true;
    try {
      const { seq: _seq, ...body } = entry;
      void _seq;
      this.#applyBody(body);
      this.#clearCompletedSnapshotImportIfAdvanced();
    } finally {
      this.#permissivePhysical = false;
    }
  }

  #clearCompletedSnapshotImportIfAdvanced(): void {
    const frameCompleted = this.#completedSnapshotFrameImport;
    if (
      frameCompleted !== undefined &&
      this.#core.getCurrentManifestVersion() !== frameCompleted.version
    ) {
      this.#completedSnapshotFrameImport = undefined;
      if (
        this.#snapshotFrameImport?.completedReplay !== true ||
        this.#snapshotFrameImport.ledgerId !== frameCompleted.ledgerId
      ) {
        this.#queueSnapshotFrameLedgerCleanup("import", frameCompleted.ledgerId);
      }
    }
  }

  #queueSnapshotFrameLedgerCleanup(kind: "export" | "import", id: string): void {
    this.#staleSnapshotFrameLedgers.set(`${kind}/${id}`, { kind, id });
  }

  async #verifyEntryPayloads(entry: WalEntry): Promise<void> {
    const placements = placementsOf(entry);
    for (const placement of placements) {
      try {
        await this.#pool.readVerified(placement);
      } catch (error) {
        throw new Error(
          `WAL sequence ${String(entry.seq)} references an invalid extent placement ` +
            `${String(placement.extent)}:${String(placement.offset)}+` +
            String(placement.length),
          { cause: error },
        );
      }
    }
    if (entry.op === "writeFtsBase") {
      for (const [ordinal, placement] of entry.pointer.chunks.entries()) {
        verifyFtsChunkBytes(
          await this.#pool.readVerified(placement),
          entry.pointer.chunkBounds[ordinal],
          `WAL full-text base chunk ${String(ordinal)}`,
        );
      }
    } else if (entry.op === "writeFtsBaseBuildChunk") {
      const chunk = verifyFtsChunkBytes(
        await this.#pool.readVerified(entry.placement),
        entry.bounds,
        `WAL staged full-text chunk ${String(entry.input.ordinal)}`,
      );
      const retainedEntries = chunk.reduce(
        (total, posting) =>
          safeByteSum(total, posting.rowIds.length, "WAL staged postings entry count"),
        0,
      );
      const totalTokens = chunk.reduce(
        (total, posting) =>
          posting.tf.reduce(
            (sum, frequency) => safeByteSum(sum, frequency, "WAL staged postings token count"),
            total,
          ),
        0,
      );
      if (retainedEntries !== entry.retainedEntries || totalTokens !== entry.totalTokens) {
        throw new Error("WAL staged postings accounting does not match its payload");
      }
    } else if (entry.op === "beginSnapshotFrameExport") {
      this.#preparedSnapshotFrameExportLedger?.close();
      this.#preparedSnapshotFrameExportLedger = await SnapshotFrameLedger.open(
        this.#tree,
        "export",
        entry.state.ledgerId,
        entry.state.ledgerLength,
        false,
      );
      let sequence = 0;
      for (const { record } of this.#preparedSnapshotFrameExportLedger.records(
        entry.state.ledgerLength,
      )) {
        if (
          record.sequence !== sequence ||
          record.kind === "block" ||
          record.payload === undefined
        ) {
          throw new Error("Snapshot export WAL ledger is not canonical");
        }
        const items = decodeSnapshotMetadataItems(record.kind, record.payload);
        if (items.length !== record.itemCount) {
          throw new Error("Snapshot export WAL ledger item count changed");
        }
        sequence = safeSuccessor(sequence, "Snapshot export ledger sequence");
      }
      if (sequence !== entry.state.metadataFrameCount) {
        throw new Error("Snapshot export WAL ledger frame count changed");
      }
    } else if (entry.op === "beginSnapshotFrameImport") {
      this.#preparedSnapshotFrameImportLedger?.close();
      this.#preparedSnapshotFrameImportLedger = await SnapshotFrameLedger.open(
        this.#tree,
        "import",
        entry.state.ledgerId,
        entry.state.ledgerLength,
        false,
      );
    } else if (entry.op === "appendSnapshotImportFrames") {
      const current = this.#snapshotFrameImport;
      const ledger = this.#requireSnapshotFrameImportLedger();
      if (current === undefined) throw new Error("Snapshot frame WAL append has no session");
      const nextState = { ...entry.state, header: current.header };
      validateSnapshotFrameImportState(nextState);
      if (!entry.replay) ledger.adoptLength(nextState.ledgerLength);
      const records = [...ledger.records(nextState.ledgerLength)].filter(
        ({ record }) =>
          record.sequence >= current.nextSequence && record.sequence < nextState.nextSequence,
      );
      const frames: SnapshotFrame[] = [];
      for (const { record } of records) {
        const payload =
          record.payload ??
          (record.placement === undefined
            ? undefined
            : await this.#pool.readVerified(record.placement));
        frames.push(frameFromSnapshotLedger({ record, offset: 0, nextOffset: 0 }, payload));
      }
      const expected = advanceSnapshotFrameImportState(current, frames, entry.input.expiresAt);
      expected.ledgerLength = nextState.ledgerLength;
      if (
        JSON.stringify(durableSnapshotFrameImportState(expected)) !== JSON.stringify(entry.state)
      ) {
        throw new Error("Snapshot frame WAL state disagrees with its durable ledger");
      }
    }
  }

  #releaseReclaimedBlocks(ids: readonly string[]): void {
    const placements: Placement[] = [];
    for (const id of ids) {
      const placement = this.#deleteBlockPlacement(id);
      if (placement !== undefined) placements.push(placement);
    }
    this.#releasePlacements(placements);
  }

  #payloadRefKey(ref: ExtentPayloadRef): string {
    switch (ref.kind) {
      case "block":
        return JSON.stringify(["b", ref.id]);
      case "fts":
        return JSON.stringify(["f", ref.key, ref.ordinal]);
      case "fts-build":
        return JSON.stringify(["s", ref.key, ref.buildId, ref.ordinal]);
    }
  }

  #trackPayload(ref: ExtentPayloadRef, placement: Placement): void {
    let refs = this.#extentPayloadRefs.get(placement.extent);
    if (refs === undefined) {
      refs = new Map();
      this.#extentPayloadRefs.set(placement.extent, refs);
    }
    const key = this.#payloadRefKey(ref);
    if (refs.has(key)) throw new Error(`Duplicate OPFS extent payload reference: ${key}`);
    refs.set(key, ref);
  }

  #untrackPayload(ref: ExtentPayloadRef, placement: Placement): void {
    const refs = this.#extentPayloadRefs.get(placement.extent);
    if (!refs?.delete(this.#payloadRefKey(ref))) {
      throw new Error(`Missing OPFS extent payload reference: ${this.#payloadRefKey(ref)}`);
    }
    if (refs.size === 0) this.#extentPayloadRefs.delete(placement.extent);
  }

  #setBlockPlacement(id: string, placement: Placement): void {
    const previous = this.#blockIndex.get(id);
    if (previous !== undefined) this.#untrackPayload({ kind: "block", id }, previous);
    this.#blockIndex.set(id, placement);
    this.#trackPayload({ kind: "block", id }, placement);
  }

  #deleteBlockPlacement(id: string): Placement | undefined {
    const previous = this.#blockIndex.get(id);
    if (previous === undefined) return undefined;
    this.#untrackPayload({ kind: "block", id }, previous);
    this.#blockIndex.delete(id);
    return previous;
  }

  #setFtsBasePointer(key: string, pointer: FtsBasePointer): void {
    this.#deleteFtsBasePointer(key);
    this.#ftsBases.set(key, pointer);
    pointer.chunks.forEach((placement, ordinal) =>
      this.#trackPayload({ kind: "fts", key, ordinal }, placement),
    );
  }

  #deleteFtsBasePointer(key: string): FtsBasePointer | undefined {
    const previous = this.#ftsBases.get(key);
    if (previous === undefined) return undefined;
    previous.chunks.forEach((placement, ordinal) =>
      this.#untrackPayload({ kind: "fts", key, ordinal }, placement),
    );
    this.#ftsBases.delete(key);
    return previous;
  }

  #setFtsBuildPointer(key: string, pointer: FtsBaseBuildPointer): void {
    const previous = this.#ftsBuilds.get(key);
    this.#assertFtsBuildUsage(
      pointer.ownerKind,
      pointer.retainedBytes,
      pointer.retainedEntries,
      previous,
    );
    if (previous !== undefined) {
      previous.chunks.forEach((placement, ordinal) =>
        this.#untrackPayload(
          { kind: "fts-build", key, buildId: previous.buildId, ordinal },
          placement,
        ),
      );
      this.#activeFtsBuildCount -= Number(previous.ownerKind === "fts-column");
      this.#activeSecondaryBuildCount -= Number(previous.ownerKind === "secondary-index");
      this.#acceleratorBuildStagedBytes -= previous.retainedBytes;
      this.#acceleratorBuildStagedEntries -= previous.retainedEntries;
    }
    this.#ftsBuilds.set(key, pointer);
    this.#activeFtsBuildCount += Number(pointer.ownerKind === "fts-column");
    this.#activeSecondaryBuildCount += Number(pointer.ownerKind === "secondary-index");
    this.#acceleratorBuildStagedBytes = safeByteSum(
      this.#acceleratorBuildStagedBytes,
      pointer.retainedBytes,
      "Accelerator build staged bytes",
    );
    this.#acceleratorBuildStagedEntries = safeByteSum(
      this.#acceleratorBuildStagedEntries,
      pointer.retainedEntries,
      "Accelerator build staged entries",
    );
    pointer.chunks.forEach((placement, ordinal) =>
      this.#trackPayload({ kind: "fts-build", key, buildId: pointer.buildId, ordinal }, placement),
    );
  }

  #deleteFtsBuildPointer(key: string): FtsBaseBuildPointer | undefined {
    const previous = this.#ftsBuilds.get(key);
    if (previous === undefined) return undefined;
    previous.chunks.forEach((placement, ordinal) =>
      this.#untrackPayload(
        { kind: "fts-build", key, buildId: previous.buildId, ordinal },
        placement,
      ),
    );
    this.#ftsBuilds.delete(key);
    this.#activeFtsBuildCount -= Number(previous.ownerKind === "fts-column");
    this.#activeSecondaryBuildCount -= Number(previous.ownerKind === "secondary-index");
    this.#acceleratorBuildStagedBytes -= previous.retainedBytes;
    this.#acceleratorBuildStagedEntries -= previous.retainedEntries;
    if (
      this.#activeFtsBuildCount < 0 ||
      this.#activeSecondaryBuildCount < 0 ||
      this.#acceleratorBuildStagedBytes < 0 ||
      this.#acceleratorBuildStagedEntries < 0
    ) {
      throw new Error("Accelerator build staged accounting underflow");
    }
    return previous;
  }

  #tempPageKey(ownerId: string, runId: string, pageIndex: number): string {
    return JSON.stringify([ownerId, runId, pageIndex]);
  }

  #applyTempPageUpdates(
    updates: ReadonlyArray<Omit<TempPageLedgerRecord, "length"> & { length: number | null }>,
  ): void {
    const affected = new Map<string, TempOwnerUsage>();
    const usageFor = (ownerId: string): TempOwnerUsage => {
      let usage = affected.get(ownerId);
      if (usage === undefined) {
        const current = this.#tempOwnerUsage.get(ownerId);
        usage = {
          runs: new Map(current?.runs ?? []),
          pages: current?.pages ?? 0,
          bytes: current?.bytes ?? 0,
        };
        affected.set(ownerId, usage);
      }
      return usage;
    };
    let pages = this.#tempPages.size;
    let bytes = this.#tempPageBytes;
    let runs = this.#tempRunCount;
    const seen = new Set<string>();
    for (const update of updates) {
      validateTempPageLedgerRecord(update);
      const key = this.#tempPageKey(update.ownerId, update.runId, update.pageIndex);
      if (seen.has(key)) throw new Error("Temp page ledger update repeats a page");
      seen.add(key);
      const previous = this.#tempPages.get(key);
      const usage = usageFor(update.ownerId);
      if (update.length === null) {
        if (previous === undefined) continue;
        pages -= 1;
        bytes -= previous.length;
        usage.pages -= 1;
        usage.bytes -= previous.length;
        const runPages = (usage.runs.get(update.runId) ?? 0) - 1;
        if (runPages === 0) {
          usage.runs.delete(update.runId);
          runs -= 1;
        } else usage.runs.set(update.runId, runPages);
        continue;
      }
      if (this.#core.getTempOwner(update.ownerId) === undefined) {
        throw new Error(`Temp page has no owner: ${update.ownerId}`);
      }
      if (previous === undefined) {
        pages += 1;
        bytes += update.length;
        usage.pages += 1;
        usage.bytes += update.length;
        const runPages = usage.runs.get(update.runId) ?? 0;
        if (runPages === 0) runs += 1;
        usage.runs.set(update.runId, runPages + 1);
      } else {
        bytes += update.length - previous.length;
        usage.bytes += update.length - previous.length;
      }
    }
    for (const [ownerId, usage] of affected) {
      if (usage.runs.size > MAX_TEMP_RUNS_PER_OWNER) {
        throw new StorageResourceLimitError("temp run", usage.runs.size, MAX_TEMP_RUNS_PER_OWNER);
      }
      if (usage.pages > MAX_TEMP_PAGES_PER_OWNER) {
        throw new StorageResourceLimitError("temp page", usage.pages, MAX_TEMP_PAGES_PER_OWNER);
      }
      if (usage.bytes > MAX_TEMP_BYTES_PER_OWNER) {
        throw new StorageResourceLimitError(
          "temp owner byte",
          usage.bytes,
          MAX_TEMP_BYTES_PER_OWNER,
        );
      }
      if (usage.pages < 0 || usage.bytes < 0) {
        throw new Error(`Temp page ledger underflow for owner: ${ownerId}`);
      }
    }
    if (runs > MAX_TEMP_RUNS_TOTAL) {
      throw new StorageResourceLimitError("temporary run total", runs, MAX_TEMP_RUNS_TOTAL);
    }
    if (pages > MAX_TEMP_PAGES_TOTAL) {
      throw new StorageResourceLimitError("temporary page total", pages, MAX_TEMP_PAGES_TOTAL);
    }
    if (bytes > MAX_TEMP_BYTES_TOTAL) {
      throw new StorageResourceLimitError("temporary byte", bytes, MAX_TEMP_BYTES_TOTAL);
    }
    if (runs < 0 || pages < 0 || bytes < 0) throw new Error("Temp page ledger underflow");
    for (const update of updates) {
      const key = this.#tempPageKey(update.ownerId, update.runId, update.pageIndex);
      if (update.length === null) this.#tempPages.delete(key);
      else this.#tempPages.set(key, { ...update, length: update.length });
    }
    for (const [ownerId, usage] of affected) {
      if (usage.pages === 0) this.#tempOwnerUsage.delete(ownerId);
      else this.#tempOwnerUsage.set(ownerId, usage);
    }
    this.#tempRunCount = runs;
    this.#tempPageBytes = bytes;
  }

  #removeTempLedgerRun(ownerId: string, runId: string): void {
    const updates = [...this.#tempPages.values()]
      .filter((record) => record.ownerId === ownerId && record.runId === runId)
      .map((record) => ({ ...record, length: null }));
    this.#applyTempPageUpdates(updates);
  }

  #removeTempLedgerOwner(ownerId: string): void {
    const updates = [...this.#tempPages.values()]
      .filter((record) => record.ownerId === ownerId)
      .map((record) => ({ ...record, length: null }));
    this.#applyTempPageUpdates(updates);
  }

  #deleteTempLedgerPage(record: TempPageLedgerRecord): void {
    const key = this.#tempPageKey(record.ownerId, record.runId, record.pageIndex);
    if (!this.#tempPages.delete(key)) return;
    const usage = this.#tempOwnerUsage.get(record.ownerId);
    if (usage === undefined) throw new Error(`Temp page usage is missing: ${record.ownerId}`);
    usage.pages -= 1;
    usage.bytes -= record.length;
    this.#tempPageBytes -= record.length;
    const runPages = (usage.runs.get(record.runId) ?? 0) - 1;
    if (runPages < 0 || usage.pages < 0 || usage.bytes < 0 || this.#tempPageBytes < 0) {
      throw new Error(`Temp page ledger underflow for owner: ${record.ownerId}`);
    }
    if (runPages === 0) {
      usage.runs.delete(record.runId);
      this.#tempRunCount -= 1;
    } else {
      usage.runs.set(record.runId, runPages);
    }
    if (usage.pages === 0) this.#tempOwnerUsage.delete(record.ownerId);
  }

  #matchingTempPageRecord(path: readonly string[], size: number): TempPageLedgerRecord | undefined {
    const identity = parseTempPageFilePath(path);
    if (identity === undefined) return undefined;
    const record = this.#tempPages.get(
      this.#tempPageKey(identity.ownerId, identity.runId, identity.pageIndex),
    );
    return record?.length === size ? record : undefined;
  }

  #releasePlacements(placements: Iterable<Placement>): number[] {
    try {
      return this.#pool.release(placements);
    } catch (error) {
      // An accounting refusal means in-memory mutation must not serve another read. Recovery
      // revalidates exact placement accounting before the next operation can continue.
      this.#poisoned = true;
      throw error;
    }
  }

  /**
   * Fully synchronous: publish the same generation to both slots, flush each, then reset.
   * Therefore corruption of either post-success copy cannot expose an older database.
   */
  checkpointNow(): void {
    try {
      this.#checkpointNowUnchecked();
      this.#checkpointFailures = 0;
      this.#lastCheckpointError = undefined;
      this.#checkpointRetryAtEntries = 0;
    } catch (error) {
      this.#checkpointFailures += 1;
      this.#lastCheckpointError = error;
      const retryEntries = 2 ** Math.min(this.#checkpointFailures, 10);
      this.#checkpointRetryAtEntries = this.#entriesSinceCheckpoint + retryEntries;
      throw error;
    }
  }

  #checkpointNowUnchecked(): void {
    const generation = safeSuccessor(this.#checkpointGeneration, "OPFS checkpoint generation");
    // In relaxed mode, appends deliberately avoid per-operation flushes. Make every extent
    // referenced by this checkpoint durable before publishing and flushing the checkpoint;
    // only then is it safe to reset the WAL.
    if (!this.#strict) this.#pool.flush();
    this.#snapshotFrameExportLedger?.flush();
    this.#snapshotFrameImportLedger?.flush();
    const state: CheckpointState = {
      formatVersion: 1,
      generation,
      lastSeq: this.#seq,
      core: this.#core.dump(),
      blockIndex: [...this.#blockIndex.entries()],
      ftsBases: [...this.#ftsBases.entries()],
      ftsBuilds: [...this.#ftsBuilds.entries()],
      tempPages: [...this.#tempPages.values()],
      ...(this.#snapshotFrameExport === undefined
        ? {}
        : { snapshotFrameExport: structuredClone(this.#snapshotFrameExport) }),
      ...(this.#snapshotFrameImport === undefined
        ? {}
        : { snapshotFrameImport: structuredClone(this.#snapshotFrameImport) }),
      ...(this.#completedSnapshotFrameImport === undefined
        ? {}
        : {
            completedSnapshotFrameImport: structuredClone(this.#completedSnapshotFrameImport),
          }),
      extents: this.#pool.meta(),
    };
    const bytes = encodeSyncCheckpoint(state);
    if (bytes.byteLength > MAX_OPFS_CHECKPOINT_BYTES) {
      throw new Error(
        `OPFS checkpoint exceeds its ${String(MAX_OPFS_CHECKPOINT_BYTES)} byte limit: ` +
          String(bytes.byteLength),
      );
    }
    const slotIndex = this.#newestSlot === 0 ? 1 : 0;
    const mirrorIndex = slotIndex === 0 ? 1 : 0;
    const writeSlot = (index: number): void => {
      const slot = this.#slots[index];
      if (slot === undefined) throw new Error(`Missing OPFS checkpoint slot ${String(index)}`);
      slot.truncate(0);
      writeFully(slot, bytes, 0, `writing checkpoint slot ${String(index)}`);
      slot.flush();
    };
    writeSlot(slotIndex);
    this.#newestSlot = slotIndex;
    this.#checkpointGeneration = state.generation;
    // WAL remains intact until the redundant copy is equally durable.
    writeSlot(mirrorIndex);
    this.#wal.reset();
    this.#entriesSinceCheckpoint = 0;
    this.#lastCheckpointBytes = bytes.byteLength;
  }

  #checkpointBeforeDeletingWalPayload(): void {
    // Snapshot ledgers are payloads of begin/append WAL frames. A later close, cancel, or
    // takeover makes them unreachable from current state, but recovery still verifies the old
    // frames before replay. Reset that history behind a redundant checkpoint before unlinking
    // the last copy; a checkpoint refusal deliberately leaves cleanup debt instead.
    if (this.#wal.byteLength > 0) this.checkpointNow();
  }

  async #deleteDrainedExtents(): Promise<void> {
    const deletable = this.#releasePlacements([]);
    if (deletable.length > 0 && this.#wal.byteLength > 0) {
      // Older WAL frames may still name payloads that a later GC/relocation frame retired.
      // Recovery verifies frame payloads before replay, so deleting their last physical extent
      // is safe only after a redundant checkpoint makes the resolved indexes authoritative and
      // resets that history. A checkpoint refusal leaves the files in place as cleanup debt.
      this.checkpointNow();
    }
    for (const id of deletable) {
      await this.#pool.deleteExtent(id);
    }
  }

  async #cleanupRecoveryArtifacts(walEndOffset = this.#wal.byteLength): Promise<void> {
    let firstError: unknown;
    const attempt = async (work: () => Promise<void> | void): Promise<void> => {
      try {
        await work();
      } catch (error) {
        firstError ??= error;
      }
    };
    await attempt(() => {
      if (this.#walHandle.getSize() > walEndOffset) this.#walHandle.truncate(walEndOffset);
    });
    await attempt(() => this.#deleteDrainedExtents());
    await attempt(() => this.#pool.truncateTailToPublishedOffset());
    await attempt(() => this.#deleteUnknownExtentFiles());
    await attempt(() => this.#deleteUnknownSnapshotFrameLedgers());
    await attempt(async () => {
      if (await this.#reconcileRecoveredTempPages()) this.#tempLedgerNeedsCheckpoint = true;
      if (this.#tempLedgerNeedsCheckpoint) {
        this.checkpointNow();
        this.#tempLedgerNeedsCheckpoint = false;
      }
    });
    await attempt(() => this.#deleteUnknownTempPageFiles());
    for (const [key, ledger] of this.#staleSnapshotFrameLedgers) {
      await attempt(async () => {
        this.#checkpointBeforeDeletingWalPayload();
        await this.#tree.deleteFile(snapshotLedgerPath(ledger.kind, ledger.id));
        this.#staleSnapshotFrameLedgers.delete(key);
      });
    }
    if (firstError !== undefined) {
      throw firstError instanceof Error
        ? firstError
        : new Error(errorMessage(firstError) ?? "OPFS recovery cleanup failed");
    }
  }

  /**
   * Physical reclamation follows the durable logical mutation. It must never turn an already
   * committed operation into a rejected promise: callers could retry under the false belief
   * that nothing happened. Record the debt, retry once on the operation queue, and expose any
   * persistent refusal through maintenance health and integrity diagnostics.
   */
  async #postCommitCleanup(work: () => Promise<void>): Promise<void> {
    try {
      await work();
      this.#cleanupDebtBytes = await this.#measureCleanupDebtBytes();
      this.#cleanupFailures = 0;
      this.#lastCleanupError = undefined;
      return;
    } catch (error) {
      this.#cleanupFailures += 1;
      this.#lastCleanupError = error;
      await this.#refreshCleanupDebtAfterFailure();
    }
    if (this.#cleanupRetryScheduled || this.#closed) return;
    this.#cleanupRetryScheduled = true;
    void this.#run(async () => {
      this.#cleanupRetryScheduled = false;
      try {
        await work();
        this.#cleanupDebtBytes = await this.#measureCleanupDebtBytes();
        this.#cleanupFailures = 0;
        this.#lastCleanupError = undefined;
      } catch (error) {
        this.#cleanupFailures += 1;
        this.#lastCleanupError = error;
        await this.#refreshCleanupDebtAfterFailure();
        // A later mutation that creates cleanup work retries again. Do not spin forever on a
        // persistent platform refusal or on corruption that needs operator attention.
      }
    }).catch(() => undefined);
  }

  async #refreshCleanupDebtAfterFailure(): Promise<void> {
    try {
      this.#cleanupDebtBytes = await this.#measureCleanupDebtBytes();
    } catch (error) {
      this.#cleanupDebtBytes = Math.max(this.#cleanupDebtBytes, this.#cleanupLimitBytes);
      this.#lastCleanupError = error;
    }
  }

  async #measureCleanupDebtBytes(): Promise<number> {
    const meta = this.#pool.meta();
    const sizes = await this.#pool.physicalByteLengths();
    let debt = 0;
    for (const [id, liveBytes] of meta.liveBytes) {
      const physicalBytes = sizes.get(id) ?? 0;
      // Extents below half occupancy are actionable cleanup work. Denser sealed extents have a
      // hard <2x physical/live bound and are deliberately left alone to avoid write amplification.
      if (id !== meta.tailExtentId && liveBytes * 2 < physicalBytes) {
        debt += physicalBytes - liveBytes;
      }
    }
    const known = new Set(meta.liveBytes.map(([id]) => extentPath(id).at(-1) ?? ""));
    known.add(extentPath(meta.tailExtentId).at(-1) ?? "");
    for await (const file of this.#tree.walkFiles(["extents"])) {
      if (file.path.length !== 1 || !known.has(file.path[0] ?? "")) debt += file.size;
    }
    const tailSize = sizes.get(meta.tailExtentId) ?? meta.tailOffset;
    debt += Math.max(0, tailSize - meta.tailOffset);
    debt += Math.max(0, this.#walHandle.getSize() - this.#wal.byteLength);
    const knownSnapshotLedgers = new Set<string>();
    if (this.#snapshotFrameExport !== undefined) {
      knownSnapshotLedgers.add(
        snapshotLedgerPath("export", this.#snapshotFrameExport.ledgerId).join("/"),
      );
    }
    const imported = this.#snapshotFrameImport ?? this.#completedSnapshotFrameImport;
    if (imported !== undefined) {
      knownSnapshotLedgers.add(snapshotLedgerPath("import", imported.ledgerId).join("/"));
    }
    for await (const file of this.#tree.walkFiles(["snapshots-v1"])) {
      if (!knownSnapshotLedgers.has(["snapshots-v1", ...file.path].join("/"))) {
        debt += file.size;
      }
    }
    for await (const file of this.#tree.walkFiles(["temp"])) {
      if (this.#matchingTempPageRecord(file.path, file.size) === undefined) {
        debt += Math.max(file.size, TEMP_FILE_CLEANUP_DEBT_FLOOR_BYTES);
      }
    }
    return debt;
  }

  /** Retry existing debt before accepting another operation that can grow extent files. */
  async #preparePayloadGrowth(): Promise<void> {
    if (this.#cleanupFailures === 0 && this.#cleanupDebtBytes < this.#cleanupLimitBytes) return;
    try {
      await this.#cleanupRecoveryArtifacts();
      await this.#compactFragmentedExtents();
      this.#cleanupDebtBytes = await this.#measureCleanupDebtBytes();
      if (this.#cleanupDebtBytes === 0) {
        this.#cleanupFailures = 0;
        this.#lastCleanupError = undefined;
      }
    } catch (error) {
      this.#cleanupFailures += 1;
      this.#lastCleanupError = error;
      await this.#refreshCleanupDebtAfterFailure();
    }
    if (this.#cleanupDebtBytes >= this.#cleanupLimitBytes) {
      throw new DOMException(
        `OPFS physical cleanup debt ${String(this.#cleanupDebtBytes)} bytes reached its ` +
          `${String(this.#cleanupLimitBytes)} byte safety limit`,
        "QuotaExceededError",
      );
    }
  }

  async #deleteUnknownExtentFiles(): Promise<void> {
    const meta = this.#pool.meta();
    const known = new Set(meta.liveBytes.map(([id]) => extentPath(id).at(-1) ?? ""));
    known.add(extentPath(meta.tailExtentId).at(-1) ?? "");
    for await (const file of this.#tree.walkFiles(["extents"])) {
      if (file.path.length === 1 && known.has(file.path[0] ?? "")) continue;
      await this.#tree.deleteFile(["extents", ...file.path]);
    }
  }

  async #deleteUnknownSnapshotFrameLedgers(): Promise<void> {
    const known = new Set<string>();
    if (this.#snapshotFrameExport !== undefined) {
      known.add(snapshotLedgerPath("export", this.#snapshotFrameExport.ledgerId).join("/"));
    }
    const imported = this.#snapshotFrameImport ?? this.#completedSnapshotFrameImport;
    if (imported !== undefined) {
      known.add(snapshotLedgerPath("import", imported.ledgerId).join("/"));
    }
    for await (const file of this.#tree.walkFiles(["snapshots-v1"])) {
      const path = ["snapshots-v1", ...file.path];
      if (known.has(path.join("/"))) continue;
      this.#checkpointBeforeDeletingWalPayload();
      await this.#tree.deleteFile(path);
    }
  }

  /**
   * Scratch files are written after their reservation frame. A crash may therefore leave a
   * reservation without bytes. Drop only those missing/mismatched reservations after a complete
   * streamed directory pass; an interrupted scan cannot accidentally forget live scratch data.
   */
  async #reconcileRecoveredTempPages(): Promise<boolean> {
    const seen = Symbol("seen temp page during recovery");
    type SeenRecord = TempPageLedgerRecord & { [seen]?: true };
    for await (const file of this.#tree.walkFiles(["temp"])) {
      const record = this.#matchingTempPageRecord(file.path, file.size) as SeenRecord | undefined;
      if (record !== undefined) Object.defineProperty(record, seen, { value: true });
    }
    let changed = false;
    for (const record of this.#tempPages.values() as MapIterator<SeenRecord>) {
      if (record[seen] === true) continue;
      this.#deleteTempLedgerPage(record);
      changed = true;
    }
    return changed;
  }

  async #deleteUnknownTempPageFiles(): Promise<void> {
    for await (const file of this.#tree.walkFiles(["temp"])) {
      if (this.#matchingTempPageRecord(file.path, file.size) !== undefined) continue;
      await this.#tree.deleteFile(["temp", ...file.path]);
    }
  }

  async #rollbackUnpublishedBatch(mark: ExtentBatchMark, originalError: unknown): Promise<never> {
    try {
      await this.#pool.rollbackBatch(mark);
    } catch (rollbackError) {
      this.#poisoned = true;
      throw new Error(
        `Failed to roll back unpublished OPFS extent bytes after: ${String(originalError)}`,
        {
          cause: rollbackError,
        },
      );
    }
    throw originalError;
  }

  #beginUnpublishedBatch(): ExtentBatchMark {
    // Checkpoint before appending: a checkpoint taken after the pool's live-byte counters move
    // but before the WAL publishes their index pointers would persist an inconsistent state.
    if (this.#wal.byteLength >= MAX_OPFS_WAL_BYTES - 64 * 1024 * 1024) this.checkpointNow();
    return this.#pool.markBatch();
  }

  /** Re-packs every sealed extent below 50% occupancy; each source file is at most 8 MiB. */
  async #compactFragmentedExtents(): Promise<void> {
    // Import placements are durable but intentionally absent from the public block index. Keep
    // their extent coordinates stable until finish or abort publishes/releases the whole set.
    if (
      this.#snapshotFrameExport !== undefined ||
      this.#snapshotFrameImport !== undefined ||
      this.#completedSnapshotFrameImport !== undefined
    ) {
      return;
    }
    for (;;) {
      const extent = await this.#pool.fragmentedExtentId();
      if (extent === undefined) return;
      const blockSources: Array<{ id: string; from: Placement }> = [];
      const ftsSources: Array<{ key: string; ordinal: number; from: Placement }> = [];
      const ftsBuildSources: Array<{
        key: string;
        buildId: string;
        ordinal: number;
        from: Placement;
      }> = [];
      const refs = this.#extentPayloadRefs.get(extent);
      if (refs !== undefined) {
        let selected = 0;
        for (const ref of refs.values()) {
          if (selected === MAX_RELOCATION_PAYLOADS) break;
          switch (ref.kind) {
            case "block": {
              const from = this.#blockIndex.get(ref.id);
              if (from?.extent !== extent) {
                throw new Error(`OPFS reverse extent index is stale for block ${ref.id}`);
              }
              blockSources.push({ id: ref.id, from });
              break;
            }
            case "fts": {
              const from = this.#ftsBases.get(ref.key)?.chunks[ref.ordinal];
              if (from?.extent !== extent) {
                throw new Error(`OPFS reverse extent index is stale for postings ${ref.key}`);
              }
              ftsSources.push({ key: ref.key, ordinal: ref.ordinal, from });
              break;
            }
            case "fts-build": {
              const build = this.#ftsBuilds.get(ref.key);
              const from = build?.chunks[ref.ordinal];
              if (build?.buildId !== ref.buildId || from?.extent !== extent) {
                throw new Error(
                  `OPFS reverse extent index is stale for staged postings ${ref.key}`,
                );
              }
              ftsBuildSources.push({
                key: ref.key,
                buildId: ref.buildId,
                ordinal: ref.ordinal,
                from,
              });
              break;
            }
          }
          selected += 1;
        }
      }
      if (blockSources.length === 0 && ftsSources.length === 0 && ftsBuildSources.length === 0) {
        throw new Error(`OPFS extent ${String(extent)} has live bytes but no indexed payload`);
      }
      const blocks: BlockRelocation[] = [];
      const ftsChunks: FtsChunkRelocation[] = [];
      const ftsBuildChunks: FtsBuildChunkRelocation[] = [];
      const mark = this.#beginUnpublishedBatch();
      try {
        for (const source of blockSources) {
          const bytes = await this.#pool.readVerified(source.from);
          verifyStoredBlock(bytes);
          const placement = await this.#pool.append(bytes, false);
          blocks.push({ ...source, placement });
        }
        for (const source of ftsSources) {
          const bytes = await this.#pool.readVerified(source.from);
          const pointer = this.#ftsBases.get(source.key);
          verifyFtsChunkBytes(
            bytes,
            pointer?.chunkBounds[source.ordinal],
            `full-text base ${source.key}/${String(source.ordinal)}`,
          );
          const placement = await this.#pool.append(bytes, false);
          ftsChunks.push({ ...source, placement });
        }
        for (const source of ftsBuildSources) {
          const bytes = await this.#pool.readVerified(source.from);
          const build = this.#ftsBuilds.get(source.key);
          verifyFtsChunkBytes(
            bytes,
            build?.chunkBounds[source.ordinal],
            `staged full-text base ${source.key}/${String(source.ordinal)}`,
          );
          const placement = await this.#pool.append(bytes, false);
          ftsBuildChunks.push({ ...source, placement });
        }
        this.#logged({ op: "relocatePayloads", blocks, ftsChunks, ftsBuildChunks });
        this.#pool.commitBatch(mark);
      } catch (error) {
        return this.#rollbackUnpublishedBatch(mark, error);
      }
      await this.#deleteDrainedExtents();
    }
  }

  // ---------------------------------------------------------------------------------------
  // Blocks.
  // ---------------------------------------------------------------------------------------

  async getBlock(id: string): Promise<Uint8Array | undefined> {
    validateId(id);
    await this.#healthy();
    const placement = this.#blockIndex.get(id);
    if (placement === undefined) return undefined;
    return this.#pool.read(placement);
  }

  async getBlocks(ids: readonly string[]): Promise<Array<Uint8Array | undefined>> {
    assertStorageBulkReadItems(ids, "Block read");
    for (const id of ids) validateId(id);
    await this.#healthy();
    const placements = ids.map((id) => this.#blockIndex.get(id));
    assertBlockReadBatchByteLimit(placements);
    // `read` takes its reader reservation synchronously. Starting the whole bounded batch in
    // one turn prevents relocation/deletion from invalidating any captured placement.
    return Promise.all(
      placements.map(async (placement) =>
        placement === undefined ? undefined : await this.#pool.read(placement),
      ),
    );
  }

  async readManifestBlock(version: number | null, id: string): Promise<Uint8Array | undefined> {
    await this.#healthy();
    if (this.#core.hasManifestBlocks(version, [id])[0] !== true) return undefined;
    const placement = this.#blockIndex.get(id);
    if (placement === undefined) {
      throw corruption(
        `blocks/${id}`,
        `readable manifest ${String(version)} references a missing payload`,
      );
    }
    // `read()` acquires an extent reader reservation synchronously before its first await.
    // Relocation may swap the index after this point, but deletion waits for that reservation,
    // so the immutable bytes selected together with membership remain readable to completion.
    try {
      return await this.#pool.read(placement);
    } catch (error) {
      throw corruption(
        `blocks/${id}`,
        `readable manifest ${String(version)} references an unreadable payload`,
        error,
      );
    }
  }

  async hasManifestBlocks(version: number | null, ids: readonly string[]): Promise<boolean[]> {
    await this.#healthy();
    if (ids.length > MAX_MANIFEST_BLOCK_PRESENCE_IDS) {
      throw new RangeError(
        `Manifest block presence accepts at most ${String(MAX_MANIFEST_BLOCK_PRESENCE_IDS)} ids`,
      );
    }
    return this.#core.hasManifestBlocks(version, ids);
  }

  async listManifestBlockPage(input: ListManifestBlockPageInput): Promise<ManifestBlockPage> {
    await this.#healthy();
    return this.#core.listManifestBlockPage(input);
  }

  async listRetiredManifestBlockPage(
    input: ListRetiredManifestBlockPageInput,
  ): Promise<ManifestBlockPage> {
    await this.#healthy();
    return this.#core.listRetiredManifestBlockPage(input);
  }

  // ---------------------------------------------------------------------------------------
  // Temp spill pages: leader-accounted so many tabs share one exact disk quota.
  // ---------------------------------------------------------------------------------------

  async putTempRunPage(page: TempRunPage): Promise<void> {
    await this.putTempRunPages([page]);
  }

  async putTempRunPages(pages: readonly TempRunPage[]): Promise<void> {
    assertTempRunPageBatchLimits(pages);
    for (const page of pages) validateTempRunPage(page);
    await this.#run(async () => {
      await this.#preparePayloadGrowth();
      const previous = pages.map((page) => ({
        ownerId: page.ownerId,
        runId: page.runId,
        pageIndex: page.pageIndex,
        length:
          this.#tempPages.get(this.#tempPageKey(page.ownerId, page.runId, page.pageIndex))
            ?.length ?? null,
      }));
      this.#logged({
        op: "reserveTempRunPages",
        pages: pages.map((page) => ({
          ownerId: page.ownerId,
          runId: page.runId,
          pageIndex: page.pageIndex,
          length: page.bytes.byteLength,
        })),
      });
      try {
        for (const page of pages) {
          await this.#tree.writeFile(
            tempPagePath(page.ownerId, page.runId, page.pageIndex),
            page.bytes,
          );
        }
      } catch (error) {
        try {
          this.#logged({ op: "restoreTempRunPages", pages: previous });
        } catch (rollbackError) {
          this.#poisoned = true;
          throw new Error("Failed to roll back a refused OPFS temp-page reservation", {
            cause: rollbackError,
          });
        }
        throw error;
      }
    });
  }

  async getTempRunPage(
    ownerId: string,
    runId: string,
    pageIndex: number,
  ): Promise<Uint8Array | undefined> {
    validateTempRunPageIdentity(ownerId, runId, pageIndex);
    return this.#run(async () => {
      const record = this.#tempPages.get(this.#tempPageKey(ownerId, runId, pageIndex));
      if (record === undefined) return undefined;
      const bytes = await this.#tree.readFile(tempPagePath(ownerId, runId, pageIndex), {
        maxBytes: record.length,
      });
      if (bytes === undefined) return undefined;
      if (bytes.byteLength !== record.length) {
        throw corruption(
          `temp/${ownerId}/${runId}/${String(pageIndex)}`,
          `ledger length ${String(record.length)} does not match ${String(bytes.byteLength)}`,
        );
      }
      return bytes;
    });
  }

  async removeTempRun(ownerId: string, runId: string): Promise<void> {
    validateTempRunPageIdentity(ownerId, runId, 0);
    await this.#run(() => this.#logged({ op: "removeTempRun", ownerId, runId }));
    await this.#postCommitCleanup(() =>
      this.#tree.deleteTree(["temp", encodeSegment(ownerId), encodeSegment(runId)]),
    );
  }

  async removeTempOwner(ownerId: string): Promise<void> {
    validateId(ownerId);
    await this.#run(() => this.#logged({ op: "removeTempOwner", ownerId }));
    await this.#postCommitCleanup(() => this.#tree.deleteTree(["temp", encodeSegment(ownerId)]));
  }

  async removeCompactionJob(id: string): Promise<boolean> {
    return this.#run(() => {
      if (!this.#core.canRemoveCompactionJob(id)) return false;
      return this.#logged({ op: "removeCompactionJob", id }) as boolean;
    });
  }

  /** @internal Shared implementation installed for direct RecordCore reads below. */
  async _readCoreGenerated(method: CoreReadMethod, args: unknown[]): Promise<unknown> {
    await this.#healthy();
    const read = Reflect.get(this.#core, method) as (...call: unknown[]) => unknown;
    return Reflect.apply(read, this.#core, args);
  }

  /** @internal Shared implementation installed for ordinary one-frame mutations below. */
  _loggedGenerated(method: LoggedMethod, args: unknown[]): Promise<unknown> {
    return this.#run(() => this.#logged(LOGGED_BODY_BUILDERS[method](args)));
  }

  async removeTempOwnerIfExpired(ownerId: string, expiresAtCutoff: string): Promise<boolean> {
    const removed = await this.#run(
      () => this.#logged({ op: "removeTempOwnerIfExpired", ownerId, expiresAtCutoff }) as boolean,
    );
    if (removed) {
      await this.#postCommitCleanup(() => this.#tree.deleteTree(["temp", encodeSegment(ownerId)]));
    }
    return removed;
  }

  async listTempOwnerIdsPage(
    afterOwnerId: string | null,
    limit: number,
  ): Promise<StoragePage<string, string>> {
    await this.#healthy();
    // Validate the cursor/limit before walking the physical namespace, then retain only the
    // smallest page of orphan owner names. Directory iteration may be arbitrarily large but
    // this method's heap is always O(limit).
    this.#core.listTempOwnerIdsPage(afterOwnerId, limit, []);
    const physicalOwnerIds: string[] = [];
    for await (const name of this.#tree.iterateNames(["temp"])) {
      const ownerId = decodeSegment(name);
      if (afterOwnerId !== null && ownerId <= afterOwnerId) continue;
      const index = lowerBoundStrings(physicalOwnerIds, ownerId);
      if (physicalOwnerIds[index] === ownerId) continue;
      physicalOwnerIds.splice(index, 0, ownerId);
      if (physicalOwnerIds.length > limit) physicalOwnerIds.pop();
    }
    return this.#core.listTempOwnerIdsPage(afterOwnerId, limit, physicalOwnerIds);
  }

  async listExpiredTempOwnerPage(
    expiresAtCutoff: string,
    afterCursor: string | null,
    limit: number,
  ): Promise<StoragePage<string, string>> {
    await this.#healthy();
    return this.#core.listExpiredTempOwnerPage(expiresAtCutoff, afterCursor, limit);
  }

  // ---------------------------------------------------------------------------------------
  // Logged record mutations.
  // ---------------------------------------------------------------------------------------

  async addTable(record: TableRecord, options: CatalogMutationOptions = {}): Promise<void> {
    await this.#run(() =>
      this.#logged({
        op: "addTable",
        record,
        expectedCatalogEpoch: options.expectedCatalogEpoch ?? null,
      }),
    );
  }

  async updateTable(
    id: string,
    expectedRevision: number,
    update: TableRecordUpdate,
  ): Promise<TableRecord> {
    return this.#run(async () => {
      const updated = this.#logged({
        op: "updateTable",
        id,
        expectedRevision,
        update,
      }) as TableRecord;
      if (update.columns !== undefined) {
        // DROP COLUMN may have released full-text chunks. Reclaim their extents before this
        // mutation resolves so repeated schema evolution cannot grow the live process's disk.
        await this.#postCommitCleanup(async () => {
          await this.#deleteDrainedExtents();
          await this.#compactFragmentedExtents();
        });
      }
      return updated;
    });
  }

  async removeTable(
    id: string,
    expectedRevision: number,
    options: CatalogMutationOptions = {},
  ): Promise<void> {
    await this.#run(async () => {
      this.#logged({
        op: "removeTable",
        id,
        expectedRevision,
        expectedCatalogEpoch: options.expectedCatalogEpoch ?? null,
      });
      await this.#postCommitCleanup(async () => {
        await this.#deleteDrainedExtents();
        await this.#compactFragmentedExtents();
      });
    });
  }

  async dropTable(input: DropTableInput): Promise<ManifestSummary> {
    return this.#run(async () => {
      const summary = this.#logged({ op: "dropTable", input }) as ManifestSummary;
      await this.#postCommitCleanup(async () => {
        await this.#deleteDrainedExtents();
        await this.#compactFragmentedExtents();
      });
      return summary;
    });
  }

  async dropTableColumn(input: DropTableColumnInput): Promise<ManifestSummary> {
    return this.#run(async () => {
      const summary = this.#logged({ op: "dropTableColumn", input }) as ManifestSummary;
      await this.#postCommitCleanup(async () => {
        await this.#deleteDrainedExtents();
        await this.#compactFragmentedExtents();
      });
      return summary;
    });
  }

  async removeFtsColumn(tableId: string, columnId: string): Promise<void> {
    await this.#run(async () => {
      this.#logged({ op: "removeFtsColumn", tableId, columnId });
      await this.#postCommitCleanup(async () => {
        await this.#deleteDrainedExtents();
        await this.#compactFragmentedExtents();
      });
    });
  }

  async beginTransaction(input: BeginTransactionInput): Promise<BeginTransactionResult> {
    validateBeginTransactionInput(input);
    return this.#run(
      () => this.#logged({ op: "beginTransaction", input }) as BeginTransactionResult,
    );
  }

  async stageTransactionArtifacts(
    input: StageTransactionArtifactsInput,
  ): Promise<TransactionRecord> {
    return this.#run(async () => {
      this.#core.preflightTransactionArtifactStage(input);
      const ids = new Set<string>();
      for (const block of input.blocks) {
        validateId(block.id);
        validateBlockWriteBytes(block.bytes);
        if (ids.has(block.id) || this.#blockIndex.has(block.id)) {
          throw new Error(`Block already exists: ${block.id}`);
        }
        ids.add(block.id);
      }
      const blocks: IdPlacement[] = [];
      await this.#preparePayloadGrowth();
      const mark = this.#beginUnpublishedBatch();
      try {
        for (const block of input.blocks) {
          blocks.push({
            id: block.id,
            placement: await this.#pool.append(block.bytes, false),
          });
        }
        const updated = this.#logged({
          op: "stageTransactionArtifacts",
          transactionId: input.transactionId,
          expectedRevision: input.expectedRevision,
          blocks,
          segments: [...input.segments],
          updatedAt: input.updatedAt,
        }) as TransactionRecord;
        this.#pool.commitBatch(mark);
        return updated;
      } catch (error) {
        return this.#rollbackUnpublishedBatch(mark, error);
      }
    });
  }

  async rollbackTransactionArtifacts(
    input: RollbackTransactionArtifactsInput,
  ): Promise<TransactionRecord> {
    return this.#run(async () => {
      const updated = this.#logged({
        op: "rollbackTransactionArtifacts",
        input,
      }) as TransactionRecord;
      await this.#postCommitCleanup(() => this.#deleteDrainedExtents());
      return updated;
    });
  }

  async writeTransaction(input: WriteTransactionInput): Promise<ManifestSummary> {
    return this.#run(async () => {
      this.#core.preflightWriteTransaction(input);
      const ids = new Set<string>();
      for (const block of input.blocks) {
        validateId(block.id);
        validateBlockWriteBytes(block.bytes);
        if (ids.has(block.id) || this.#blockIndex.has(block.id)) {
          throw new Error(`Block already exists: ${block.id}`);
        }
        ids.add(block.id);
      }
      const { blocks: bytes, ...rest } = input;
      const blocks: IdPlacement[] = [];
      await this.#preparePayloadGrowth();
      const mark = this.#beginUnpublishedBatch();
      try {
        for (const block of bytes) {
          blocks.push({
            id: block.id,
            placement: await this.#pool.append(block.bytes, false),
          });
        }
        const summary = this.#logged({
          op: "writeTransaction",
          input: { ...rest, segments: [...input.segments] },
          blocks,
        }) as ManifestSummary;
        this.#pool.commitBatch(mark);
        return summary;
      } catch (error) {
        return this.#rollbackUnpublishedBatch(mark, error);
      }
    });
  }

  async runGarbageCollectionStep(
    input: RunGarbageCollectionStepInput,
  ): Promise<GarbageCollectionStepResult> {
    return this.#run(async () => {
      // Computed against provably-current state, logged as the resolved effect so replay
      // needs no knowledge of physical placement.
      const step = this.#core.runGarbageCollectionStep(input);
      this.#releaseReclaimedBlocks(step.reclaimedBlockIds);
      this.#appendFrame({
        op: "garbageCollectionStep",
        effect: {
          job: step.job,
          prunedManifestVersions: [...step.prunedManifestVersions],
          reclaimedSegmentIds: [...step.reclaimedSegmentIds],
          reclaimedBlockIds: [...step.reclaimedBlockIds],
          reclaimedTransactionIds: [...step.reclaimedTransactionIds],
          updatedAt: input.updatedAt,
        },
      });
      await this.#postCommitCleanup(async () => {
        await this.#deleteDrainedExtents();
        if (step.job.state === "completed") await this.#compactFragmentedExtents();
      });
      return step;
    });
  }

  // ---------------------------------------------------------------------------------------
  // Full-text bases: immutable chunks in extents plus a logged pointer.
  // ---------------------------------------------------------------------------------------

  async writeFtsBase(
    tableId: string,
    columnId: string,
    input: { coversVersion: number; chunks: FtsPosting[][]; totalTokens: number },
  ): Promise<void> {
    await this.#run(async () => {
      validateId(tableId);
      validateId(columnId);
      const table = this.#core.getTable(tableId);
      if (table === undefined || !activePostingStorageColumnIds(table).has(columnId)) {
        throw new Error(`Postings index is no longer active: ${tableId}/${columnId}`);
      }
      validateFtsBaseInput(input, "Full-text base");
      const chunks: Placement[] = [];
      await this.#preparePayloadGrowth();
      const mark = this.#beginUnpublishedBatch();
      try {
        for (const chunk of input.chunks) {
          chunks.push(await this.#pool.append(encodeFtsChunk(chunk), false));
        }
        const pointer: FtsBasePointer = {
          coversVersion: input.coversVersion,
          totalTokens: input.totalTokens,
          chunks,
          chunkBounds: input.chunks.map((chunk) => ({
            first: chunk[0]?.term ?? "",
            last: chunk[chunk.length - 1]?.term ?? "",
          })),
        };
        this.#logged({ op: "writeFtsBase", tableId, columnId, pointer });
        this.#pool.commitBatch(mark);
      } catch (error) {
        return this.#rollbackUnpublishedBatch(mark, error);
      }
      await this.#postCommitCleanup(async () => {
        await this.#deleteDrainedExtents();
        await this.#compactFragmentedExtents();
      });
    });
  }

  async beginFtsBaseBuild(input: BeginPostingBuildInput): Promise<void> {
    await this.#run(() => {
      validatePostingBuildBegin(input);
      this.#logged({ op: "beginFtsBaseBuild", input });
    });
  }

  async renewFtsBaseBuild(input: RenewPostingBuildInput): Promise<void> {
    await this.#run(() => {
      validatePostingBuildRenewal(input);
      this.#logged({ op: "renewFtsBaseBuild", input });
    });
  }

  async writeFtsBaseBuildChunk(input: AppendPostingBuildChunkInput): Promise<void> {
    await this.#run(async () => {
      validatePostingBuildAppend(input, true);
      const build = this.#requireLivePostingBuild(input);
      if (input.ordinal > build.chunks.length) {
        throw new Error(`Full-text base chunk is out of order: ${String(input.ordinal)}`);
      }
      if (input.ordinal >= MAX_FTS_BASE_CHUNKS) {
        throw new RangeError("Full-text base build exceeds the chunk-count limit");
      }
      const chunk = [...input.chunk];
      const totalTokens = validateFtsPostingChunks([chunk], "Full-text base build");
      const retainedEntries = chunk.reduce(
        (total, posting) =>
          safeByteSum(total, posting.rowIds.length, "Accelerator build staged entries"),
        0,
      );
      const encodedChunk = encodeFtsChunk(chunk);
      if (input.ordinal < build.chunks.length) {
        const placement = build.chunks[input.ordinal];
        if (
          placement === undefined ||
          !equalBytes(await this.#pool.readVerified(placement), encodedChunk)
        ) {
          throw new PostingBuildConflictError(
            input.buildId,
            input.ownerId,
            `chunk ${String(input.ordinal)} replay changed`,
          );
        }
        return;
      }
      this.#assertFtsBuildUsage(
        build.ownerKind,
        safeByteSum(build.retainedBytes, encodedChunk.length, "Accelerator build staged bytes"),
        safeByteSum(build.retainedEntries, retainedEntries, "Accelerator build staged entries"),
        build,
      );
      await this.#preparePayloadGrowth();
      const mark = this.#beginUnpublishedBatch();
      try {
        const placement = await this.#pool.append(encodedChunk, false);
        const { chunk: _chunk, ...durableInput } = input;
        void _chunk;
        this.#logged({
          op: "writeFtsBaseBuildChunk",
          input: durableInput,
          placement,
          bounds: { first: chunk[0]?.term ?? "", last: chunk[chunk.length - 1]?.term ?? "" },
          totalTokens,
          retainedEntries,
        });
        this.#pool.commitBatch(mark);
      } catch (error) {
        await this.#rollbackUnpublishedBatch(mark, error);
      }
    });
  }

  async finishFtsBaseBuild(input: FinishPostingBuildInput): Promise<void> {
    await this.#run(async () => {
      validatePostingBuildFinish(input);
      this.#logged({ op: "finishFtsBaseBuild", input });
      await this.#postCommitCleanup(async () => {
        await this.#deleteDrainedExtents();
        await this.#compactFragmentedExtents();
      });
    });
  }

  async abortFtsBaseBuild(input: AbortPostingBuildInput): Promise<void> {
    await this.#run(async () => {
      validatePostingBuildAbort(input);
      this.#logged({ op: "abortFtsBaseBuild", input });
      await this.#postCommitCleanup(() => this.#deleteDrainedExtents());
    });
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
    validateId(tableId);
    validateId(columnId);
    validateFtsPostingQueries(terms);
    if (!Number.isSafeInteger(upToVersion) || upToVersion < -1) {
      throw new RangeError("Full-text query version must be a safe integer at least -1");
    }
    validateFtsCandidateLimit(maxRowIds);
    return this.#run(async () => {
      // Unlike immutable table blocks, derived-index extents are not protected by reader leases:
      // DROP INDEX may reclaim them immediately. Hold the leader queue until every selected
      // chunk is copied so a concurrent drop yields either the complete old base or no base,
      // never a missing file halfway through a query.
      const key = postingStorageKey(tableId, columnId);
      const pointer = this.#ftsBases.get(key);
      const coversVersion = pointer?.coversVersion ?? -1;
      const deltas = this.#core.readFtsDeltas(tableId, columnId, coversVersion, upToVersion);
      const candidates = await this.#collectFtsCandidatesBounded(
        key,
        pointer,
        deltas.chunkLists,
        terms,
        maxRowIds,
      );
      return {
        ...candidates,
        deltaChunkCount: deltas.deltaChunkCount,
        totalTokens: (pointer?.totalTokens ?? 0) + deltas.deltaTokens,
        coversVersion,
        hasBase: pointer !== undefined,
      };
    });
  }

  async #collectFtsCandidatesBounded(
    key: string,
    pointer: FtsBasePointer | undefined,
    deltaChunks: ReadonlyArray<readonly FtsPosting[]>,
    terms: readonly FtsPostingQuery[],
    maxRowIds: number,
  ): Promise<FtsCandidates> {
    const sets = terms.map(() => new Set<bigint>());
    let retainedRowIds = 0;
    const addChunk = (postings: readonly FtsPosting[]): boolean => {
      for (const posting of postings) {
        for (let index = 0; index < terms.length; index += 1) {
          const query = terms[index];
          const set = sets[index];
          if (
            query === undefined ||
            set === undefined ||
            !ftsPostingQueryMatches(posting.term, query)
          ) {
            continue;
          }
          for (const rowId of posting.rowIds) {
            if (set.has(rowId)) continue;
            if (retainedRowIds === maxRowIds) return false;
            set.add(rowId);
            retainedRowIds += 1;
          }
        }
      }
      return true;
    };
    if (pointer !== undefined) {
      const ordinals = selectFtsChunks(pointer.chunkBounds, terms);
      for (const ordinal of ordinals) {
        if (!addChunk(await this.#loadFtsChunk(key, pointer, ordinal))) {
          return { rowIdsByTerm: terms.map(() => []), overflow: true };
        }
      }
    }
    for (const chunk of deltaChunks) {
      if (!addChunk(chunk)) return { rowIdsByTerm: terms.map(() => []), overflow: true };
    }
    return {
      rowIdsByTerm: sets.map((set) => [...set].sort((left, right) => (left < right ? -1 : 1))),
      overflow: false,
    };
  }

  async readFtsPostings(
    tableId: string,
    columnId: string,
    upToVersion: number,
    maxRowIds = MAX_FTS_CANDIDATE_ROW_IDS,
    maxRetainedBytes = MAX_FTS_ORDERED_READ_BYTES,
  ) {
    validateId(tableId);
    validateId(columnId);
    if (!Number.isSafeInteger(upToVersion) || upToVersion < -1) {
      throw new RangeError("Full-text query version must be a safe integer at least -1");
    }
    validateFtsOrderedReadLimits(maxRowIds, maxRetainedBytes);
    return this.#run(async () => {
      const key = postingStorageKey(tableId, columnId);
      const pointer = this.#ftsBases.get(key);
      const coversVersion = pointer?.coversVersion ?? -1;
      const deltas = this.#core.readFtsDeltas(tableId, columnId, coversVersion, upToVersion);
      const baseChunks =
        pointer === undefined
          ? []
          : await this.#loadFtsChunksBounded(
              key,
              pointer,
              pointer.chunks.map((_, ordinal) => ordinal),
              maxRetainedBytes,
            );
      if (baseChunks === undefined) {
        return {
          postings: [],
          overflow: true,
          deltaChunkCount: deltas.deltaChunkCount,
          coversVersion,
          hasBase: pointer !== undefined,
        };
      }
      const result = collectFtsPostingsBounded(
        [...baseChunks, ...deltas.chunkLists],
        maxRowIds,
        maxRetainedBytes,
      );
      return {
        ...result,
        deltaChunkCount: deltas.deltaChunkCount,
        coversVersion,
        hasBase: pointer !== undefined,
      };
    });
  }

  async #loadFtsChunksBounded(
    key: string,
    pointer: FtsBasePointer,
    ordinals: readonly number[],
    maxModeledBytes: number,
  ): Promise<FtsPosting[][] | undefined> {
    const chunks: FtsPosting[][] = [];
    let modeledBytes = 0;
    for (const ordinal of ordinals) {
      const chunk = await this.#loadFtsChunk(key, pointer, ordinal);
      modeledBytes += modeledFtsChunkBytes(chunk);
      if (!Number.isSafeInteger(modeledBytes) || modeledBytes > maxModeledBytes) return undefined;
      chunks.push(chunk);
    }
    return chunks;
  }

  async #loadFtsChunk(
    key: string,
    pointer: FtsBasePointer,
    ordinal: number,
  ): Promise<FtsPosting[]> {
    const placement = pointer.chunks[ordinal];
    if (placement === undefined) return [];
    const cacheKey = `${key}/${String(placement.extent)}/${String(placement.offset)}`;
    const cached = this.#ftsChunkCache.get(cacheKey);
    if (cached !== undefined) {
      this.#ftsChunkCache.delete(cacheKey);
      this.#ftsChunkCache.set(cacheKey, cached);
      return cached.chunk;
    }
    const chunk = verifyFtsChunkBytes(
      await this.#pool.readVerified(placement),
      pointer.chunkBounds[ordinal],
      `Full-text base ${key} chunk ${String(ordinal)}`,
    );
    const modeledBytes = modeledFtsChunkBytes(chunk);
    if (modeledBytes <= FTS_CHUNK_CACHE_BYTES) {
      this.#ftsChunkCache.set(cacheKey, { chunk, modeledBytes });
      this.#ftsChunkCacheBytes += modeledBytes;
    }
    while (
      this.#ftsChunkCache.size > FTS_CHUNK_CACHE_SIZE ||
      this.#ftsChunkCacheBytes > FTS_CHUNK_CACHE_BYTES
    ) {
      const [oldest] = this.#ftsChunkCache.keys();
      if (oldest === undefined) break;
      const removed = this.#ftsChunkCache.get(oldest);
      this.#ftsChunkCache.delete(oldest);
      this.#ftsChunkCacheBytes -= removed?.modeledBytes ?? 0;
    }
    return chunk;
  }

  #dropFtsChunkCache(keyPrefix: string): void {
    for (const key of [...this.#ftsChunkCache.keys()]) {
      if (!key.startsWith(`${keyPrefix}/`)) continue;
      const removed = this.#ftsChunkCache.get(key);
      this.#ftsChunkCache.delete(key);
      this.#ftsChunkCacheBytes -= removed?.modeledBytes ?? 0;
    }
  }

  // ---------------------------------------------------------------------------------------
  // Snapshots, sizing, lifecycle.
  // ---------------------------------------------------------------------------------------

  /**
   * Streams exact ready posting generations from OPFS extents into the framed snapshot ledger.
   * Large generations that exceed the public bounded ordered-read fuse are omitted deliberately;
   * restore then marks that accelerator invalid and rebuilds it instead of trusting partial data.
   */
  async *#snapshotFramePostingItems(version: number): AsyncGenerator<SnapshotPostingItem> {
    const owners: Array<{
      tableId: string;
      ownerKind: "fts-column" | "secondary-index";
      ownerId: string;
      storageColumnId: string;
    }> = [];
    for (const table of this.#core.listTables()) {
      for (const [columnId, state] of Object.entries(table.ftsColumns ?? {})) {
        if (state.state === "ready") {
          owners.push({
            tableId: table.id,
            ownerKind: "fts-column",
            ownerId: columnId,
            storageColumnId: columnId,
          });
        }
      }
      for (const [indexId, index] of Object.entries(table.secondaryIndexes ?? {})) {
        if (index.state === "ready") {
          owners.push({
            tableId: table.id,
            ownerKind: "secondary-index",
            ownerId: indexId,
            storageColumnId: index.storageColumnId,
          });
        }
      }
    }
    owners.sort(
      (left, right) =>
        left.tableId.localeCompare(right.tableId) ||
        left.storageColumnId.localeCompare(right.storageColumnId) ||
        left.ownerId.localeCompare(right.ownerId),
    );
    for (const [ownerOrdinal, owner] of owners.entries()) {
      const key = postingStorageKey(owner.tableId, owner.storageColumnId);
      const pointer = this.#ftsBases.get(key);
      const coversVersion = pointer?.coversVersion ?? -1;
      const deltas = this.#core.readFtsDeltas(
        owner.tableId,
        owner.storageColumnId,
        coversVersion,
        version,
      );
      const baseChunks =
        pointer === undefined
          ? []
          : await this.#loadFtsChunksBounded(
              key,
              pointer,
              pointer.chunks.map((_, ordinal) => ordinal),
              MAX_FTS_ORDERED_READ_BYTES,
            );
      if (baseChunks === undefined) continue;
      const collected = collectFtsPostingsBounded(
        [...baseChunks, ...deltas.chunkLists],
        MAX_FTS_CANDIDATE_ROW_IDS,
        MAX_FTS_ORDERED_READ_BYTES,
      );
      if (collected.overflow) continue;
      const postings = collected.postings.map((posting) =>
        owner.ownerKind === "secondary-index"
          ? { ...posting, tf: posting.rowIds.map(() => 1) }
          : posting,
      );
      let totalTokens = 0;
      for (const posting of postings) {
        for (const frequency of posting.tf) {
          totalTokens += frequency;
          if (!Number.isSafeInteger(totalTokens)) {
            throw new RangeError("Snapshot posting token total exceeds the safe integer range");
          }
        }
      }
      const generationId = `snapshot-p-${String(version)}-${String(ownerOrdinal)}`;
      yield {
        kind: "posting-generation",
        ...owner,
        generationId,
        coversVersion: version,
        chunkCount: Math.ceil(postings.length / SNAPSHOT_POSTING_TERMS_PER_CHUNK),
        totalTokens,
      };
      for (let offset = 0, ordinal = 0; offset < postings.length; ordinal += 1) {
        const chunk = postings.slice(offset, offset + SNAPSHOT_POSTING_TERMS_PER_CHUNK);
        offset += chunk.length;
        yield {
          kind: "posting-chunk",
          storageColumnId: owner.storageColumnId,
          generationId,
          ordinal,
          postings: chunk,
        };
      }
    }
  }

  async beginSnapshotFrameExport(
    input: BeginSnapshotFrameExportInput,
  ): Promise<SnapshotFrameExportSession> {
    validateId(input.ownerId);
    validateSnapshotLifetime(input.createdAt, input.expiresAt, "Snapshot frame export");
    return this.#run(async () => {
      const existing = this.#snapshotFrameExport;
      if (existing !== undefined && Date.parse(existing.expiresAt) > Date.parse(input.createdAt)) {
        throw new Error(`Snapshot frame export is already active: ${existing.sessionId}`);
      }
      const manifest = this.#core.snapshotFrameManifest();
      const sessionId = crypto.randomUUID();
      const ledgerId = sessionId;
      const ledger = await SnapshotFrameLedger.open(this.#tree, "export", ledgerId, 0, true);
      const summaries = emptySnapshotFrameSummaries();
      let sequence = 0;
      try {
        for (const item of this.#core.snapshotFrameMetadataItems()) {
          sequence = appendSnapshotMetadataItems(
            ledger,
            snapshotMetadataFrameKind(item),
            [item],
            sequence,
            summaries,
          );
        }
        for await (const item of this.#snapshotFramePostingItems(manifest.version)) {
          sequence = appendSnapshotMetadataItems(
            ledger,
            "posting-page",
            [item],
            sequence,
            summaries,
          );
        }
        summaries.block = {
          frameCount: manifest.liveBlockCount,
          itemCount: manifest.liveBlockCount,
          storedBytes: manifest.liveBlockBytes,
        };
        const header = prepareSnapshotFrameStreamHeader({
          formatVersion: 1,
          databaseVersion: manifest.version,
          createdAt: input.createdAt,
          kinds: summaries,
        });
        ledger.flush();
        const state: SnapshotFrameExportState = {
          sessionId,
          ownerId: input.ownerId,
          createdAt: input.createdAt,
          expiresAt: input.expiresAt,
          leaseRevision: 0,
          manifestVersion: manifest.version,
          header,
          ledgerId,
          ledgerLength: ledger.byteLength,
          metadataFrameCount: sequence,
          nextSequence: 0,
          metadataOffset: 0,
          blockCursor: null,
          lastSequence: null,
          lastMetadataOffset: null,
          lastBlockId: null,
        };
        const previousLedgerId = existing?.ledgerId;
        const result = this.#logged({
          op: "beginSnapshotFrameExport",
          state,
        }) as SnapshotFrameExportSession;
        this.#snapshotFrameExportLedger?.close();
        this.#snapshotFrameExportLedger = ledger;
        if (previousLedgerId !== undefined) {
          await this.#postCommitCleanup(async () => {
            this.#checkpointBeforeDeletingWalPayload();
            await this.#tree.deleteFile(snapshotLedgerPath("export", previousLedgerId));
          });
        }
        return result;
      } catch (error) {
        ledger.close();
        await this.#tree.deleteFile(snapshotLedgerPath("export", ledgerId)).catch(() => false);
        throw error;
      }
    });
  }

  async readSnapshotExportFrame(
    input: ReadSnapshotExportFrameInput,
  ): Promise<SnapshotFrame | undefined> {
    validateSnapshotRenewal(input, "Snapshot frame export");
    if (!Number.isSafeInteger(input.sequence) || input.sequence < 0) {
      throw new RangeError("Snapshot export frame sequence is invalid");
    }
    return this.#run(async () => {
      const session = this.#requireSnapshotFrameExport(input.sessionId, input.ownerId);
      const expectedFrames = snapshotHeaderFrameCount(session.header);
      if (input.sequence >= expectedFrames) return undefined;
      if (input.sequence !== session.nextSequence && input.sequence !== session.lastSequence) {
        throw new RangeError("Snapshot export frame request is not contiguous");
      }
      const replay = input.sequence === session.lastSequence;
      let frame: SnapshotFrame;
      let next: Pick<
        SnapshotFrameExportState,
        | "nextSequence"
        | "metadataOffset"
        | "blockCursor"
        | "lastSequence"
        | "lastMetadataOffset"
        | "lastBlockId"
      >;
      if (input.sequence < session.metadataFrameCount) {
        const offset = replay ? session.lastMetadataOffset : session.metadataOffset;
        if (offset === null) throw new Error("Snapshot metadata replay cursor is missing");
        const read = this.#requireSnapshotFrameExportLedger().read(offset, session.ledgerLength);
        frame = frameFromSnapshotLedger(read, undefined);
        if (frame.sequence !== input.sequence) {
          throw new StorageCorruptionError(
            "opfs",
            `snapshot-export/${session.sessionId}`,
            "metadata frame sequence disagrees with its durable ledger",
          );
        }
        next = replay
          ? snapshotFrameExportCursor(session)
          : {
              nextSequence: safeSuccessor(session.nextSequence, "Snapshot export sequence"),
              metadataOffset: read.nextOffset,
              blockCursor: session.blockCursor,
              lastSequence: input.sequence,
              lastMetadataOffset: offset,
              lastBlockId: null,
            };
      } else {
        const blockId = replay ? session.lastBlockId : this.#nextSnapshotExportBlockId(session);
        if (blockId === null) {
          throw new StorageCorruptionError(
            "opfs",
            `snapshot-export/${session.sessionId}`,
            "manifest block count exceeds its provenance page",
          );
        }
        const placement = this.#blockIndex.get(blockId);
        if (placement === undefined) {
          throw new StorageCorruptionError(
            "opfs",
            `block/${blockId}`,
            "snapshot manifest member has no physical placement",
          );
        }
        const bytes = await this.#pool.readVerified(placement);
        frame = {
          sequence: input.sequence,
          kind: "block",
          itemCount: 1,
          key: blockId,
          payload: bytes,
          checksum: placement.checksum,
        };
        snapshotFrameEnvelopeParts(frame);
        next = replay
          ? snapshotFrameExportCursor(session)
          : {
              nextSequence: safeSuccessor(session.nextSequence, "Snapshot export sequence"),
              metadataOffset: session.metadataOffset,
              blockCursor: blockId,
              lastSequence: input.sequence,
              lastMetadataOffset: null,
              lastBlockId: blockId,
            };
      }
      this.#logged({
        op: "advanceSnapshotFrameExport",
        input,
        expectedLeaseRevision: session.leaseRevision,
        next,
      });
      return frame;
    });
  }

  async closeSnapshotFrameExport(input: CloseSnapshotExportInput): Promise<boolean> {
    validateId(input.sessionId);
    validateId(input.ownerId);
    return this.#run(async () => {
      const session = this.#snapshotFrameExport;
      const closed = this.#logged({ op: "closeSnapshotFrameExport", input }) as boolean;
      if (!closed || session === undefined) return false;
      this.#snapshotFrameExportLedger?.close();
      this.#snapshotFrameExportLedger = undefined;
      await this.#postCommitCleanup(async () => {
        this.#checkpointBeforeDeletingWalPayload();
        await this.#tree.deleteFile(snapshotLedgerPath("export", session.ledgerId));
      });
      return true;
    });
  }

  async beginSnapshotFrameImport(
    input: BeginSnapshotFrameImportInput,
  ): Promise<SnapshotFrameImportSession> {
    validateId(input.identity);
    validateId(input.ownerId);
    validateSnapshotLifetime(input.createdAt, input.expiresAt, "Snapshot frame import");
    const header = prepareSnapshotFrameStreamHeader(input.header);
    if (input.identity !== snapshotFrameStreamHeaderIdentity(header)) {
      throw new SnapshotImportConflictError(
        input.identity,
        input.ownerId,
        "header identity changed",
      );
    }
    return this.#run(async () => {
      const current = this.#snapshotFrameImport;
      if (current !== undefined) {
        const same =
          current.identity === input.identity && equalSnapshotFrameHeaders(current.header, header);
        if (
          same &&
          current.ownerId === input.ownerId &&
          Date.parse(current.expiresAt) > Date.parse(input.createdAt)
        ) {
          return snapshotFrameImportSession(current);
        }
        if (Date.parse(current.expiresAt) > Date.parse(input.createdAt)) {
          throw new SnapshotImportConflictError(
            input.identity,
            input.ownerId,
            "another owner is active",
          );
        }
        if (same) {
          const adopted = { ...current, ownerId: input.ownerId, expiresAt: input.expiresAt };
          return this.#logged({
            op: "beginSnapshotFrameImport",
            state: adopted,
          }) as SnapshotFrameImportSession;
        }
      }
      const completed = this.#completedSnapshotFrameImport;
      if (completed?.identity === input.identity) {
        if (!equalSnapshotFrameHeaders(completed.header, header)) {
          throw new SnapshotImportConflictError(
            input.identity,
            input.ownerId,
            "completed header differs",
          );
        }
        if (this.#core.getCurrentManifestVersion() !== completed.version) {
          throw new SnapshotImportConflictError(
            input.identity,
            input.ownerId,
            `completed at version ${String(completed.version)}, but the database advanced`,
          );
        }
        const replay = newSnapshotFrameImportState(input, header, completed.ledgerId, true);
        replay.ledgerLength = completed.ledgerLength;
        return this.#logged({
          op: "beginSnapshotFrameImport",
          state: replay,
        }) as SnapshotFrameImportSession;
      }
      if (this.#core.getCurrentManifestVersion() !== null || this.#core.listTables().length > 0) {
        throw new Error("This store already holds a database");
      }
      const ledgerId = crypto.randomUUID();
      const ledger = await SnapshotFrameLedger.open(this.#tree, "import", ledgerId, 0, true);
      const state = newSnapshotFrameImportState(input, header, ledgerId, false);
      const previous = current;
      try {
        const result = this.#logged({
          op: "beginSnapshotFrameImport",
          state,
        }) as SnapshotFrameImportSession;
        this.#snapshotFrameImportLedger?.close();
        this.#snapshotFrameImportLedger = ledger;
        if (previous !== undefined && !previous.completedReplay) {
          await this.#postCommitCleanup(async () => {
            this.#checkpointBeforeDeletingWalPayload();
            await this.#tree.deleteFile(snapshotLedgerPath("import", previous.ledgerId));
          });
        }
        return result;
      } catch (error) {
        ledger.close();
        await this.#tree.deleteFile(snapshotLedgerPath("import", ledgerId)).catch(() => false);
        throw error;
      }
    });
  }

  async renewSnapshotFrameImport(
    input: RenewSnapshotFrameImportInput,
  ): Promise<SnapshotFrameImportSession> {
    validateSnapshotRenewal(input, "Snapshot frame import");
    return this.#run(
      () => this.#logged({ op: "renewSnapshotFrameImport", input }) as SnapshotFrameImportSession,
    );
  }

  async appendSnapshotImportFrames(
    input: AppendSnapshotImportFramesInput,
  ): Promise<SnapshotFrameImportSession> {
    validateSnapshotRenewal(input, "Snapshot frame import");
    validateSnapshotFrameBatch(input.frames);
    return this.#run(async () => {
      const session = this.#requireSnapshotFrameImport(input);
      const renewal: RenewSnapshotFrameImportInput = {
        identity: input.identity,
        ownerId: input.ownerId,
        expiresAtCutoff: input.expiresAtCutoff,
        expiresAt: input.expiresAt,
      };
      const first = input.frames[0];
      if (first === undefined) throw new RangeError("Snapshot frame batch is empty");
      if (first.sequence === session.lastBatchStartSequence) {
        await this.#compareSnapshotFrameReplay(session, input.frames);
        return this.#logged({
          op: "renewSnapshotFrameImport",
          input: renewal,
        }) as SnapshotFrameImportSession;
      }
      if (first.sequence !== session.nextSequence) {
        throw new RangeError("Snapshot import frame batch is not contiguous");
      }
      const ledger = this.#requireSnapshotFrameImportLedger();
      const before = ledger.byteLength;
      const next = advanceSnapshotFrameImportState(session, input.frames, input.expiresAt);
      if (session.completedReplay) {
        await this.#compareSnapshotFrameReplay(session, input.frames);
        return this.#logged({
          op: "appendSnapshotImportFrames",
          input: renewal,
          state: durableSnapshotFrameImportState(next),
          blockPlacements: [],
          replay: true,
        }) as SnapshotFrameImportSession;
      }
      await this.#preparePayloadGrowth();
      const mark = this.#beginUnpublishedBatch();
      const blockPlacements: IdPlacement[] = [];
      try {
        for (const frame of input.frames) {
          if (frame.kind === "block") {
            verifyStoredBlock(frame.payload);
            const placement = await this.#pool.append(frame.payload, false);
            if (placement.checksum !== frame.checksum) {
              throw new Error(
                `Snapshot block checksum disagrees with its bytes: ${String(frame.key)}`,
              );
            }
            if (frame.key === null) throw new Error("Snapshot block frame has no block ID");
            ledger.append(frame, placement);
            blockPlacements.push({ id: frame.key, placement });
          } else {
            ledger.append(frame);
          }
        }
        ledger.flush();
        next.ledgerLength = ledger.byteLength;
        const result = this.#logged({
          op: "appendSnapshotImportFrames",
          input: renewal,
          state: durableSnapshotFrameImportState(next),
          blockPlacements,
          replay: false,
        }) as SnapshotFrameImportSession;
        this.#pool.commitBatch(mark);
        return result;
      } catch (error) {
        ledger.truncate(before);
        await this.#pool.rollbackBatch(mark);
        throw error;
      }
    });
  }

  async finishSnapshotFrameImport(input: FinishSnapshotFrameImportInput): Promise<void> {
    validateId(input.identity);
    validateId(input.ownerId);
    requireTimestamp(input.expiresAtCutoff, "snapshot frame import cutoff");
    await this.#run(() => {
      this.#logged({ op: "finishSnapshotFrameImport", input });
    });
  }

  async cancelSnapshotFrameImport(
    input: CancelSnapshotImportInput,
  ): Promise<InterruptedSnapshotImportAbortResult> {
    validateId(input.identity);
    validateId(input.ownerId);
    return this.#run(async () => {
      const session = this.#snapshotFrameImport;
      const result = this.#logged({
        op: "cancelSnapshotFrameImport",
        input,
      }) as InterruptedSnapshotImportAbortResult;
      if (session !== undefined && !session.completedReplay) {
        this.#snapshotFrameImportLedger?.close();
        this.#snapshotFrameImportLedger = undefined;
        await this.#postCommitCleanup(async () => {
          this.#checkpointBeforeDeletingWalPayload();
          await this.#tree.deleteFile(snapshotLedgerPath("import", session.ledgerId));
          await this.#deleteDrainedExtents();
        });
      }
      return result;
    });
  }

  async #validateRecoveredSnapshotFrameSessions(): Promise<void> {
    const exported = this.#snapshotFrameExport;
    if (exported !== undefined) {
      const lease = this.#core.getLease(snapshotFrameExportLeaseId(exported.sessionId));
      if (
        lease?.ownerId !== exported.ownerId ||
        lease.kind !== "backup" ||
        lease.manifestVersion !== exported.manifestVersion ||
        lease.createdAt !== exported.createdAt ||
        lease.expiresAt !== exported.expiresAt ||
        lease.revision !== exported.leaseRevision
      ) {
        throw new Error("Snapshot frame export disagrees with its durable manifest lease");
      }
      const ledger = this.#requireSnapshotFrameExportLedger();
      const observed = emptySnapshotFrameSummaries();
      let sequence = 0;
      let endOffset = 0;
      for (const read of ledger.records(exported.ledgerLength)) {
        const { record } = read;
        if (
          record.sequence !== sequence ||
          record.kind === "block" ||
          record.key !== null ||
          record.payload === undefined
        ) {
          throw new Error("Snapshot frame export metadata ledger is not canonical");
        }
        const items = decodeSnapshotMetadataItems(record.kind, record.payload);
        if (items.length !== record.itemCount) {
          throw new Error("Snapshot frame export metadata item count changed");
        }
        const summary = observed[record.kind];
        observed[record.kind] = {
          frameCount: safeByteSum(summary.frameCount, 1, "Snapshot export frame count"),
          itemCount: safeByteSum(summary.itemCount, items.length, "Snapshot export item count"),
          storedBytes: safeByteSum(
            summary.storedBytes,
            record.payloadLength,
            "Snapshot export metadata bytes",
          ),
        };
        sequence = safeSuccessor(sequence, "Snapshot export metadata sequence");
        endOffset = read.nextOffset;
      }
      if (sequence !== exported.metadataFrameCount || endOffset !== exported.ledgerLength) {
        throw new Error("Snapshot frame export ledger boundary changed");
      }
      for (const kind of SNAPSHOT_FRAME_KINDS) {
        if (kind === "block") continue;
        if (JSON.stringify(observed[kind]) !== JSON.stringify(exported.header.kinds[kind])) {
          throw new Error(`Snapshot frame export ${kind} summary changed`);
        }
      }
    }

    const imported = this.#snapshotFrameImport;
    if (imported === undefined) return;
    if (imported.completedReplay) {
      const completed = this.#completedSnapshotFrameImport;
      if (
        completed?.identity !== imported.identity ||
        completed.ledgerId !== imported.ledgerId ||
        completed.ledgerLength !== imported.ledgerLength ||
        completed.version !== imported.version ||
        !equalSnapshotFrameHeaders(completed.header, imported.header) ||
        this.#core.getCurrentManifestVersion() !== imported.version
      ) {
        throw new Error("Completed snapshot frame replay marker is inconsistent");
      }
      return;
    }
    if (this.#core.getCurrentManifestVersion() !== null || this.#core.listTables().length > 0) {
      throw new Error("A pending snapshot frame import exists beside a published database");
    }
    const ledger = this.#requireSnapshotFrameImportLedger();
    const expected: SnapshotFrameImportState = {
      ...structuredClone(imported),
      ledgerLength: 0,
      nextSequence: 0,
      stagedBytes: 0,
      blockCount: 0,
      blockBytes: 0,
      checksum: 0,
      currentKindIndex: 0,
      observed: emptySnapshotFrameSummaries(),
      lastBatchStartSequence: null,
      lastBatchOffset: null,
      lastBatchFrameCount: 0,
    };
    for (const read of ledger.records(imported.ledgerLength)) {
      const payload =
        read.record.payload ??
        (read.record.placement === undefined
          ? undefined
          : await this.#pool.readVerified(read.record.placement));
      const frame = frameFromSnapshotLedger(read, payload);
      if (frame.kind === "block") verifyStoredBlock(frame.payload);
      const advanced = advanceSnapshotFrameImportState(expected, [frame], imported.expiresAt);
      advanced.ledgerLength = read.nextOffset;
      Object.assign(expected, advanced);
    }
    for (const field of [
      "ledgerLength",
      "nextSequence",
      "stagedBytes",
      "blockCount",
      "blockBytes",
      "checksum",
      "currentKindIndex",
    ] as const) {
      if (expected[field] !== imported[field]) {
        throw new Error(`Snapshot frame import ${field} disagrees with its ledger`);
      }
    }
    if (JSON.stringify(expected.observed) !== JSON.stringify(imported.observed)) {
      throw new Error("Snapshot frame import summaries disagree with its ledger");
    }
    validateSnapshotFrameImportLastBatch(imported, ledger);
  }

  #requireSnapshotFrameExport(sessionId: string, ownerId: string): SnapshotFrameExportState {
    const session = this.#snapshotFrameExport;
    if (session?.sessionId !== sessionId || session.ownerId !== ownerId) {
      throw new Error(`Snapshot frame export ownership changed: ${sessionId}`);
    }
    return session;
  }

  #requireSnapshotFrameExportLedger(): SnapshotFrameLedger {
    const ledger = this.#snapshotFrameExportLedger;
    if (ledger === undefined) throw new Error("Snapshot frame export ledger is not open");
    return ledger;
  }

  #nextSnapshotExportBlockId(session: SnapshotFrameExportState): string | null {
    const page = this.#core.listManifestBlockPage({
      version: session.manifestVersion,
      afterBlockId: session.blockCursor,
      limit: 1,
    });
    return page.records[0]?.blockId ?? null;
  }

  #requireSnapshotFrameImport(
    input: Pick<RenewSnapshotFrameImportInput, "identity" | "ownerId" | "expiresAtCutoff">,
  ): SnapshotFrameImportState {
    const session = this.#snapshotFrameImport;
    if (session?.identity !== input.identity || session.ownerId !== input.ownerId) {
      throw new SnapshotImportConflictError(input.identity, input.ownerId, "ownership changed");
    }
    if (Date.parse(session.expiresAt) <= Date.parse(input.expiresAtCutoff)) {
      throw new SnapshotImportConflictError(input.identity, input.ownerId, "ownership expired");
    }
    return session;
  }

  #requireSnapshotFrameImportLedger(): SnapshotFrameLedger {
    const ledger = this.#snapshotFrameImportLedger;
    if (ledger === undefined) throw new Error("Snapshot frame import ledger is not open");
    return ledger;
  }

  *#snapshotFrameImportPlacements(session: SnapshotFrameImportState): IterableIterator<Placement> {
    const ledger = this.#requireSnapshotFrameImportLedger();
    for (const { record } of ledger.records(session.ledgerLength)) {
      if (record.placement !== undefined) yield record.placement;
    }
  }

  async #compareSnapshotFrameReplay(
    session: SnapshotFrameImportState,
    frames: readonly SnapshotFrame[],
  ): Promise<void> {
    const ledger = this.#requireSnapshotFrameImportLedger();
    const first = frames[0];
    if (first === undefined) throw new RangeError("Snapshot replay batch is empty");
    const persisted = [...ledger.records(session.ledgerLength)].filter(
      ({ record }) =>
        record.sequence >= first.sequence && record.sequence < first.sequence + frames.length,
    );
    if (persisted.length !== frames.length) {
      throw new SnapshotImportConflictError(
        session.identity,
        session.ownerId,
        "replayed frame range was not previously staged",
      );
    }
    for (let index = 0; index < frames.length; index += 1) {
      const frame = frames[index];
      const stored = persisted[index]?.record;
      if (frame === undefined || stored === undefined) {
        throw new SnapshotImportConflictError(
          session.identity,
          session.ownerId,
          "replayed frame changed",
        );
      }
      if (
        frame.sequence !== stored.sequence ||
        frame.kind !== stored.kind ||
        frame.itemCount !== stored.itemCount ||
        frame.key !== stored.key ||
        frame.payload.byteLength !== stored.payloadLength ||
        frame.checksum !== stored.checksum
      ) {
        throw new SnapshotImportConflictError(
          session.identity,
          session.ownerId,
          "replayed frame changed",
        );
      }
      const bytes =
        stored.payload ??
        (stored.placement === undefined
          ? undefined
          : await this.#pool.readVerified(stored.placement));
      if (bytes === undefined || !equalBytes(bytes, frame.payload)) {
        throw new SnapshotImportConflictError(
          session.identity,
          session.ownerId,
          "replayed frame bytes changed",
        );
      }
    }
  }

  #promoteSnapshotFrameImport(session: SnapshotFrameImportState): void {
    const ledger = this.#requireSnapshotFrameImportLedger();
    const added: string[] = [];
    const metadata = function* (): IterableIterator<SnapshotMetadataItem> {
      for (const { record } of ledger.records(session.ledgerLength)) {
        if (record.kind === "block") continue;
        if (record.payload === undefined) throw new Error("Snapshot metadata frame has no payload");
        yield* decodeSnapshotMetadataItems(record.kind, record.payload);
      }
    };
    const blocks = function* (): IterableIterator<{
      blockId: string;
      byteLength: number;
      checksum: number;
    }> {
      for (const { record } of ledger.records(session.ledgerLength)) {
        if (record.kind !== "block") continue;
        if (record.key === null || record.placement === undefined) {
          throw new Error("Snapshot block frame has no durable placement");
        }
        yield {
          blockId: record.key,
          byteLength: record.payloadLength,
          checksum: record.checksum,
        };
      }
    };
    try {
      for (const { record } of ledger.records(session.ledgerLength)) {
        if (record.kind !== "block" || record.key === null || record.placement === undefined)
          continue;
        if (this.#blockIndex.has(record.key))
          throw new Error(`Snapshot repeats block: ${record.key}`);
        this.#setBlockPlacement(record.key, record.placement);
        added.push(record.key);
      }
      this.#core.loadSnapshotFrameItems(session.header, metadata(), blocks());
    } catch (error) {
      for (const id of added) this.#deleteBlockPlacement(id);
      throw error;
    }
  }

  /** Live payload bytes plus the control files — what this database logically occupies. */
  async getLogicalStorageBytes(): Promise<number> {
    let total = this.#wal.byteLength;
    const slot = this.#slots[this.#newestSlot];
    if (slot !== undefined) total += slot.getSize();
    for (const [, bytes] of this.#pool.meta().liveBytes) total += bytes;
    for await (const { size } of this.#tree.walkFiles(["temp"])) total += size;
    return total;
  }

  async getStorageStats(): Promise<StorageStats> {
    return this.#run(() => this.#storageStatsUnqueued());
  }

  async checkIntegrity(
    options: { mode?: StorageIntegrityMode; maxIssues?: number } = {},
  ): Promise<StorageIntegrityReport> {
    return this.#run(async () => {
      const mode = options.mode ?? "metadata";
      const maxIssues = options.maxIssues ?? 100;
      if (!Number.isSafeInteger(maxIssues) || maxIssues < 0) {
        throw new TypeError("Integrity maxIssues must be a non-negative safe integer");
      }
      const issues: Array<{ code: string; location: string; message: string }> = [];
      let issueCount = 0;
      const issue = (code: string, location: string, message: string): void => {
        issueCount += 1;
        if (issues.length < maxIssues) issues.push({ code, location, message });
      };
      const core = this.#core.storageRecordStats();
      const checkedRecords =
        core.manifestCount +
        core.transactionCount +
        core.tableCount +
        core.segmentCount +
        core.leaseCount +
        core.compactionJobCount +
        core.garbageCollectionJobCount +
        core.rowCounterCount +
        core.autoIncrementCounterCount +
        core.ftsBaseCount +
        core.ftsDeltaCount +
        core.uniqueNamespaceCount +
        core.uniqueKeyBuildCount +
        core.tempOwnerCount +
        this.#tempPages.size;
      try {
        validateLivePlacementAccounting(
          uniquePlacements([
            ...this.#blockIndex.values(),
            ...[...this.#ftsBases.values()].flatMap((pointer) => pointer.chunks),
            ...[...this.#ftsBuilds.values()].flatMap((pointer) => pointer.chunks),
            ...(this.#snapshotFrameImport === undefined || this.#snapshotFrameImport.completedReplay
              ? []
              : [...this.#snapshotFrameImportPlacements(this.#snapshotFrameImport)]),
          ]),
          this.#pool.meta(),
        );
      } catch (error) {
        issue(
          "extent-accounting",
          "extents",
          error instanceof Error ? error.message : String(error),
        );
      }

      let checkedBlocks = 0;
      let checkedBytes = 0;
      for (const [label, ledger, length] of [
        [
          "snapshot-frame-export/ledger",
          this.#snapshotFrameExportLedger,
          this.#snapshotFrameExport?.ledgerLength,
        ],
        [
          "snapshot-frame-import/ledger",
          this.#snapshotFrameImportLedger,
          (this.#snapshotFrameImport ?? this.#completedSnapshotFrameImport)?.ledgerLength,
        ],
      ] as const) {
        if (ledger === undefined || length === undefined) continue;
        try {
          for (const { record } of ledger.records(length)) {
            checkedBytes += record.payload === undefined ? 0 : record.payloadLength;
            if (record.payload !== undefined) {
              if (record.kind === "block")
                throw new Error("Snapshot block is inline in its ledger");
              decodeSnapshotMetadataItems(record.kind, record.payload);
            }
          }
        } catch (error) {
          issue(
            "snapshot-ledger-corruption",
            label,
            error instanceof Error ? error.message : String(error),
          );
        }
      }

      for (const record of this.#tempPages.values()) {
        const location = `temp/${record.ownerId}/${record.runId}/${String(record.pageIndex)}`;
        try {
          const size = await this.#tree.fileSize(
            tempPagePath(record.ownerId, record.runId, record.pageIndex),
          );
          if (size === undefined) throw new Error("reserved temp page file is missing");
          if (size !== record.length) {
            throw new Error(
              `reserved length ${String(record.length)} does not match file length ${String(size)}`,
            );
          }
          checkedBytes += size;
        } catch (error) {
          issue(
            "temp-page-corruption",
            location,
            error instanceof Error ? error.message : String(error),
          );
        }
      }
      for await (const file of this.#tree.walkFiles(["temp"])) {
        if (this.#matchingTempPageRecord(file.path, file.size) !== undefined) continue;
        issue("temp-page-orphan", `temp/${file.path.join("/")}`, "unreserved temp page file");
      }

      type IntegrityPayload =
        | { location: string; placement: Placement; kind: "block" }
        | {
            location: string;
            placement: Placement;
            kind: "fts";
            bounds: { first: string; last: string } | undefined;
          };
      const payloads = (function* (leader: OpfsLeader): IterableIterator<IntegrityPayload> {
        for (const [id, placement] of leader.#blockIndex) {
          yield { location: `blocks/${id}`, placement, kind: "block" };
        }
        for (const [key, pointer] of leader.#ftsBases) {
          for (const [ordinal, placement] of pointer.chunks.entries()) {
            yield {
              location: `fts/${key}/${String(ordinal)}`,
              placement,
              kind: "fts",
              bounds: pointer.chunkBounds[ordinal],
            };
          }
        }
        for (const [key, pointer] of leader.#ftsBuilds) {
          for (const [ordinal, placement] of pointer.chunks.entries()) {
            yield {
              location: `fts-build/${key}/${String(ordinal)}`,
              placement,
              kind: "fts",
              bounds: pointer.chunkBounds[ordinal],
            };
          }
        }
        if (
          leader.#snapshotFrameImport?.completedReplay === false &&
          leader.#snapshotFrameImportLedger !== undefined
        ) {
          for (const { record } of leader.#snapshotFrameImportLedger.records(
            leader.#snapshotFrameImport.ledgerLength,
          )) {
            if (record.placement !== undefined) {
              yield {
                location: `snapshot-frame-import/blocks/${String(record.key)}`,
                placement: record.placement,
                kind: "block",
              };
            }
          }
        }
      })(this);
      const workers = Array.from({ length: 8 }, async () => {
        for (;;) {
          const next = payloads.next();
          if (next.done) return;
          const payload = next.value;
          if (payload.kind === "block") checkedBlocks += 1;
          checkedBytes += payload.placement.length;
          try {
            if (mode === "full") {
              const bytes = await this.#pool.readVerified(payload.placement);
              if (payload.kind === "block") verifyStoredBlock(bytes);
              else {
                verifyFtsChunkBytes(bytes, payload.bounds, payload.location);
              }
            } else if (!(await this.#pool.contains(payload.placement))) {
              throw new Error("placement is missing or exceeds its extent file");
            }
          } catch (error) {
            issue(
              mode === "full" ? "payload-checksum" : "payload-missing",
              payload.location,
              error instanceof Error ? error.message : String(error),
            );
          }
        }
      });
      await Promise.all(workers);

      const stats = await this.#storageStatsUnqueued();
      if ((stats.orphanBytes ?? 0) > 0) {
        issue("orphan-bytes", "extents", `${String(stats.orphanBytes)} unreachable physical bytes`);
      }
      if (this.#checkpointFailures > 0) {
        issue(
          "checkpoint-degraded",
          "wal",
          `${String(this.#checkpointFailures)} consecutive checkpoint failures`,
        );
      }
      if (this.#cleanupFailures > 0) {
        issue(
          "cleanup-degraded",
          "extents",
          `${String(this.#cleanupFailures)} consecutive physical cleanup failures`,
        );
      }
      return {
        mode,
        ok: issueCount === 0,
        checkedRecords,
        checkedBlocks,
        checkedBytes,
        issueCount,
        issues,
      };
    });
  }

  async #storageStatsUnqueued(): Promise<StorageStats> {
    // `checkIntegrity` already owns the operation queue, so both public maintenance methods
    // share this unqueued implementation.
    const meta = this.#pool.meta();
    const knownExtentSizes = await this.#pool.physicalByteLengths();
    const known = new Set(meta.liveBytes.map(([id]) => extentPath(id)[1] ?? ""));
    known.add(extentPath(meta.tailExtentId)[1] ?? "");
    const importedSnapshotFrame = this.#snapshotFrameImport ?? this.#completedSnapshotFrameImport;
    const liveSnapshotLedgerPaths = new Set(
      [
        this.#snapshotFrameExport === undefined
          ? undefined
          : snapshotLedgerPath("export", this.#snapshotFrameExport.ledgerId).join("/"),
        importedSnapshotFrame === undefined
          ? undefined
          : snapshotLedgerPath("import", importedSnapshotFrame.ledgerId).join("/"),
      ].filter((path): path is string => path !== undefined),
    );
    let unknownExtentBytes = 0;
    let temporaryFileBytes = 0;
    let unknownTempFileBytes = 0;
    let unknownTempFileDebtBytes = 0;
    let snapshotLedgerBytes = 0;
    let unknownSnapshotLedgerBytes = 0;
    let otherPhysicalBytes = 0;
    for await (const file of this.#tree.walkFiles([])) {
      const family = file.path[0];
      if (family === "extents") {
        if (file.path.length !== 2 || !known.has(file.path[1] ?? "")) {
          unknownExtentBytes += file.size;
        }
        continue;
      }
      if (family === "temp") {
        if (this.#matchingTempPageRecord(file.path.slice(1), file.size) === undefined) {
          unknownTempFileBytes += file.size;
          unknownTempFileDebtBytes += Math.max(file.size, TEMP_FILE_CLEANUP_DEBT_FLOOR_BYTES);
        } else temporaryFileBytes += file.size;
      }
      if (family === "snapshots-v1") {
        if (liveSnapshotLedgerPaths.has(file.path.join("/"))) snapshotLedgerBytes += file.size;
        else unknownSnapshotLedgerBytes += file.size;
      }
      if (!(
        file.path.length === 1 && ["wal", "checkpoint-a", "checkpoint-b"].includes(family ?? "")
      )) {
        otherPhysicalBytes += file.size;
      }
    }
    const knownExtentPhysicalBytes = [...knownExtentSizes.values()].reduce(
      (total, size) => total + size,
      0,
    );
    const tailPhysicalBytes = knownExtentSizes.get(meta.tailExtentId) ?? 0;
    const unpublishedTailBytes = Math.max(0, tailPhysicalBytes - meta.tailOffset);
    const drainedExtentBytes = meta.liveBytes.reduce(
      (total, [id, liveBytes]) =>
        id !== meta.tailExtentId && liveBytes === 0
          ? total + (knownExtentSizes.get(id) ?? 0)
          : total,
      0,
    );
    const reclaimableFragmentBytes = meta.liveBytes.reduce((total, [id, liveBytes]) => {
      const physicalBytes = knownExtentSizes.get(id) ?? 0;
      return id !== meta.tailExtentId && liveBytes * 2 < physicalBytes
        ? total + physicalBytes - liveBytes
        : total;
    }, 0);
    let orphanBytes = unknownExtentBytes + drainedExtentBytes;
    orphanBytes += unpublishedTailBytes;
    orphanBytes += unknownTempFileBytes;
    this.#cleanupDebtBytes =
      unknownExtentBytes +
      unpublishedTailBytes +
      reclaimableFragmentBytes +
      unknownTempFileDebtBytes;
    const core = this.#core.storageRecordStats();
    const liveBytes = meta.liveBytes.reduce((total, [, bytes]) => total + bytes, 0);
    const temporaryBytes =
      temporaryFileBytes +
      snapshotLedgerBytes +
      (this.#snapshotFrameImport?.completedReplay === false
        ? this.#snapshotFrameImport.blockBytes
        : 0);
    const checkpointBytes = this.#slots.reduce((total, slot) => total + slot.getSize(), 0);
    const physicalWalBytes = this.#walHandle.getSize();
    const staleWalBytes = Math.max(0, physicalWalBytes - this.#wal.byteLength);
    orphanBytes += staleWalBytes;
    orphanBytes += unknownSnapshotLedgerBytes;
    this.#cleanupDebtBytes += staleWalBytes + unknownSnapshotLedgerBytes;
    return {
      backend: "opfs",
      logicalBytes:
        this.#wal.byteLength +
        Math.max(...this.#slots.map((slot) => slot.getSize())) +
        liveBytes +
        temporaryFileBytes,
      physicalBytes:
        otherPhysicalBytes +
        physicalWalBytes +
        checkpointBytes +
        knownExtentPhysicalBytes +
        unknownExtentBytes,
      liveBlockBytes: core.liveBlockBytes,
      obsoleteBlockBytes: Math.max(
        0,
        [...this.#blockIndex.values()].reduce((total, placement) => total + placement.length, 0) -
          core.liveBlockBytes,
      ),
      liveBlockCount: core.liveBlockCount,
      obsoleteBlockCount: Math.max(0, this.#blockIndex.size - core.liveBlockCount),
      temporaryBytes,
      walBytes: physicalWalBytes,
      checkpointBytes,
      orphanBytes,
      manifestCount: core.manifestCount,
      transactionCount: core.transactionCount,
      segmentCount: core.segmentCount,
      maintenance: {
        degraded: this.#checkpointFailures > 0 || this.#cleanupFailures > 0,
        consecutiveFailures: Math.max(this.#checkpointFailures, this.#cleanupFailures),
        lastError:
          errorMessage(this.#lastCleanupError) ?? errorMessage(this.#lastCheckpointError) ?? null,
        walLimitBytes: MAX_OPFS_WAL_BYTES,
        cleanupDebtBytes: this.#cleanupDebtBytes,
        cleanupLimitBytes: this.#cleanupLimitBytes,
      },
    };
  }

  /** Graceful: flush, checkpoint (so the next leader's takeover is instant), release. */
  async shutdown(): Promise<void> {
    await this.#run(() => {
      this.checkpointNow();
      this.#closed = true;
    }).catch(() => {
      // Shutting down a poisoned leader still releases its handles below.
      this.#closed = true;
    });
    this.#walHandle.close();
    for (const slot of this.#slots) slot.close();
    this.#snapshotFrameExportLedger?.close();
    this.#snapshotFrameImportLedger?.close();
    this.#preparedSnapshotFrameExportLedger?.close();
    this.#preparedSnapshotFrameImportLedger?.close();
    this.#extents?.close();
  }

  /** Test-only: what tab death does — locks release, nothing is flushed or checkpointed. */
  crash(): void {
    this.#closed = true;
    this.#walHandle.close();
    for (const slot of this.#slots) slot.close();
    this.#snapshotFrameExportLedger?.close();
    this.#snapshotFrameImportLedger?.close();
    this.#preparedSnapshotFrameExportLedger?.close();
    this.#preparedSnapshotFrameImportLedger?.close();
    this.#extents?.close();
  }
}

/** @internal Exact pre-read aggregate limit, exported only for the no-allocation regression. */
export function assertBlockReadBatchByteLimit(
  placements: ReadonlyArray<{ length: number } | undefined>,
): void {
  let requestedBytes = 0;
  for (const placement of placements) {
    requestedBytes += placement?.length ?? 0;
    if (!Number.isSafeInteger(requestedBytes) || requestedBytes > MAX_BLOCK_READ_BATCH_BYTES) {
      throw new BlockReadBatchTooLargeError(requestedBytes);
    }
  }
}

for (const method of CORE_READ_METHODS) {
  Object.defineProperty(OpfsLeader.prototype, method, {
    configurable: true,
    value(this: OpfsLeader, ...args: unknown[]) {
      return this._readCoreGenerated(method, args);
    },
  });
}
for (const method of Object.keys(LOGGED_BODY_BUILDERS) as LoggedMethod[]) {
  Object.defineProperty(OpfsLeader.prototype, method, {
    configurable: true,
    value(this: OpfsLeader, ...args: unknown[]) {
      return this._loggedGenerated(method, args);
    },
  });
}

const WAL_BODY_KEYS = {
  relocatePayloads: ["blocks", "ftsChunks", "ftsBuildChunks"],
  addTable: ["record", "expectedCatalogEpoch"],
  updateTable: ["id", "expectedRevision", "update"],
  removeTable: ["id", "expectedRevision", "expectedCatalogEpoch"],
  dropTable: ["input"],
  dropTableColumn: ["input"],
  removeFtsColumn: ["tableId", "columnId"],
  removeAbortedSegment: ["id", "expectedTransactionId"],
  adoptAbortedSegment: ["input"],
  reserveRowIds: ["tableId", "count"],
  beginUniqueKeyBuild: ["input"],
  renewUniqueKeyBuild: ["input"],
  appendUniqueKeyBuildChunk: ["input"],
  finishUniqueKeyBuild: ["input"],
  abortUniqueKeyBuild: ["input"],
  reserveAutoIncrement: ["tableId", "columnId", "count", "atLeast"],
  beginTransaction: ["input"],
  createTransaction: ["record"],
  renewTransaction: ["input"],
  abortTransactionIfExpired: ["input"],
  updateTransaction: ["id", "expectedRevision", "update"],
  stageTransactionArtifacts: [
    "transactionId",
    "expectedRevision",
    "blocks",
    "segments",
    "updatedAt",
  ],
  rollbackTransactionArtifacts: ["input"],
  commitTransaction: ["input"],
  writeTransaction: ["input", "blocks"],
  createLease: ["record"],
  renewLease: ["input"],
  moveLease: ["input"],
  removeLeaseIfExpired: ["id", "expectedRevision", "expiresAtCutoff"],
  removeLease: ["input"],
  createCompactionJob: ["record"],
  updateCompactionJob: ["id", "expectedRevision", "update"],
  cancelCompactionJob: ["id", "expectedRevision", "cancelledAt"],
  removeCompactionJob: ["id"],
  createGarbageCollectionJob: ["input"],
  updateGarbageCollectionPlanning: ["input"],
  garbageCollectionStep: ["effect"],
  removeGarbageCollectionJob: ["id"],
  removePrunedManifestRecords: ["maxItems"],
  createTempOwner: ["record"],
  renewTempOwner: ["input"],
  removeTempOwnerIfExpired: ["ownerId", "expiresAtCutoff"],
  removeTempOwner: ["ownerId"],
  reserveTempRunPages: ["pages"],
  restoreTempRunPages: ["pages"],
  removeTempRun: ["ownerId", "runId"],
  writeFtsBase: ["tableId", "columnId", "pointer"],
  beginFtsBaseBuild: ["input"],
  renewFtsBaseBuild: ["input"],
  writeFtsBaseBuildChunk: ["input", "placement", "bounds", "totalTokens", "retainedEntries"],
  finishFtsBaseBuild: ["input"],
  abortFtsBaseBuild: ["input"],
  beginSnapshotFrameExport: ["state"],
  advanceSnapshotFrameExport: ["input", "expectedLeaseRevision", "next"],
  closeSnapshotFrameExport: ["input"],
  beginSnapshotFrameImport: ["state"],
  renewSnapshotFrameImport: ["input"],
  appendSnapshotImportFrames: ["input", "state", "blockPlacements", "replay"],
  finishSnapshotFrameImport: ["input"],
  cancelSnapshotFrameImport: ["input"],
} as const satisfies Record<WalEntryBody["op"], readonly string[]>;

function validateWalEntry(value: unknown): WalEntry {
  if (!isRecord(value)) throw new Error("OPFS WAL entry is not an object");
  if (!Number.isSafeInteger(value.seq) || (value.seq as number) < 1) {
    throw new Error(`OPFS WAL entry has an invalid sequence: ${String(value.seq)}`);
  }
  if (typeof value.op !== "string") {
    throw new Error(`Unsupported OPFS WAL operation: ${String(value.op)}`);
  }
  if (!Object.hasOwn(WAL_BODY_KEYS, value.op)) {
    throw new Error(`Unsupported OPFS WAL operation: ${value.op}`);
  }
  const bodyKeys = WAL_BODY_KEYS[value.op as keyof typeof WAL_BODY_KEYS];
  assertWalKeys(value, bodyKeys);
  const entry = value as unknown as WalEntry;
  switch (entry.op) {
    case "addTable":
    case "createTransaction":
    case "createLease":
    case "createCompactionJob":
    case "createTempOwner":
      requireRecord(entry.record, `${entry.op} record`);
      if (entry.op === "addTable" && entry.expectedCatalogEpoch !== null) {
        requireNonNegativeInteger(entry.expectedCatalogEpoch, "addTable expectedCatalogEpoch");
      }
      break;
    case "updateTable":
    case "updateTransaction":
    case "updateCompactionJob":
      requireRecord(entry.update, `${entry.op} update`);
      break;
    case "removeAbortedSegment":
      requireStorageId(entry.expectedTransactionId, "removeAbortedSegment transaction id");
      break;
    case "removeTable":
      if (entry.expectedCatalogEpoch !== null) {
        requireNonNegativeInteger(entry.expectedCatalogEpoch, "removeTable expectedCatalogEpoch");
      }
      break;
    case "reserveRowIds":
      requirePositiveInteger(entry.count, "reserveRowIds count");
      break;
    case "beginUniqueKeyBuild":
    case "renewUniqueKeyBuild":
    case "appendUniqueKeyBuildChunk":
    case "finishUniqueKeyBuild":
    case "abortUniqueKeyBuild":
      requireRecord(entry.input, `${entry.op} input`);
      break;
    case "moveLease":
      requireRecord(entry.input, "moveLease input");
      if (entry.input.manifestVersion !== null) {
        requireNonNegativeInteger(entry.input.manifestVersion, "moveLease manifestVersion");
      }
      break;
    case "renewTransaction":
      requireRecord(entry.input, "renewTransaction input");
      requireTimestamp(entry.input.expiresAtCutoff, "renewTransaction cutoff");
      break;
    case "rollbackTransactionArtifacts":
      requireRecord(entry.input, "rollbackTransactionArtifacts input");
      requireNonNegativeInteger(
        entry.input.expectedRevision,
        "rollbackTransactionArtifacts revision",
      );
      break;
    case "garbageCollectionStep":
      requireRecord(entry.effect, "garbageCollectionStep effect");
      assertExactRecordKeys(
        entry.effect,
        [
          "job",
          "prunedManifestVersions",
          "reclaimedSegmentIds",
          "reclaimedBlockIds",
          "reclaimedTransactionIds",
          "updatedAt",
        ],
        "garbageCollectionStep effect",
      );
      break;
    case "relocatePayloads": {
      validateRelocations(entry.blocks, "block relocations", true);
      validateRelocations(entry.ftsChunks, "full-text relocations", false);
      validateRelocations(entry.ftsBuildChunks, "staged full-text relocations", false);
      break;
    }
    case "stageTransactionArtifacts":
      validateIdPlacements(entry.blocks, "stageTransactionArtifacts blocks");
      if (entry.blocks.length > MAX_TRANSACTION_STAGE_BLOCKS) {
        throw new Error("Invalid stageTransactionArtifacts block count");
      }
      if (!Array.isArray(entry.segments))
        throw new Error("Invalid stageTransactionArtifacts segments");
      if (entry.segments.length > MAX_TRANSACTION_STAGE_SEGMENTS) {
        throw new Error("Invalid stageTransactionArtifacts segment count");
      }
      break;
    case "writeTransaction":
      requireRecord(entry.input, "writeTransaction input");
      validateIdPlacements(entry.blocks, "writeTransaction blocks");
      if (entry.blocks.length > MAX_TRANSACTION_STAGE_BLOCKS) {
        throw new Error("Invalid writeTransaction block count");
      }
      if (
        !Array.isArray(entry.input.segments) ||
        entry.input.segments.length > MAX_TRANSACTION_STAGE_SEGMENTS
      ) {
        throw new Error("Invalid writeTransaction segment count");
      }
      break;
    case "writeFtsBase":
      requireStorageId(entry.tableId, "writeFtsBase tableId");
      requireStorageId(entry.columnId, "writeFtsBase columnId");
      validateRuntimePointer(entry.pointer, "writeFtsBase pointer");
      break;
    case "reserveTempRunPages":
    case "restoreTempRunPages":
      if (!Array.isArray(entry.pages)) throw new Error(`Invalid ${entry.op} pages`);
      if (entry.pages.length > MAX_TEMP_RUN_PAGES_PER_BATCH) {
        throw new Error(`Invalid ${entry.op} page count`);
      }
      for (const page of entry.pages) validateTempPageLedgerRecord(page);
      if (entry.op === "reserveTempRunPages") {
        let bytes = 0;
        for (const page of entry.pages) {
          const length: unknown = Reflect.get(page, "length");
          if (length === null) throw new Error("Temp page reservation length is missing");
          bytes += length as number;
        }
        if (bytes > MAX_TEMP_RUN_BATCH_BYTES) {
          throw new Error("Temp page reservation byte count is invalid");
        }
      }
      break;
    case "removeTempRun":
      requireStorageId(entry.ownerId, "removeTempRun ownerId");
      requireStorageId(entry.runId, "removeTempRun runId");
      break;
    case "beginFtsBaseBuild":
      validatePostingBuildBegin(entry.input);
      break;
    case "renewFtsBaseBuild":
      validatePostingBuildRenewal(entry.input);
      break;
    case "writeFtsBaseBuildChunk":
      validatePostingBuildAppend(entry.input, false);
      requireNonNegativeInteger(entry.input.ordinal, "full-text build ordinal");
      if (entry.input.ordinal >= MAX_FTS_BASE_CHUNKS) {
        throw new Error("Full-text build ordinal exceeds the chunk-count limit");
      }
      assertValidPlacement(entry.placement);
      requireNonNegativeInteger(entry.totalTokens, "full-text build token count");
      requireNonNegativeInteger(entry.retainedEntries, "full-text build entry count");
      if (entry.retainedEntries > MAX_ACCELERATOR_BUILD_STAGED_ENTRIES_TOTAL) {
        throw new Error("Full-text build entry count exceeds the staged limit");
      }
      if (
        !isRecord(entry.bounds) ||
        typeof entry.bounds.first !== "string" ||
        typeof entry.bounds.last !== "string"
      ) {
        throw new Error("Invalid full-text build chunk bounds");
      }
      break;
    case "finishFtsBaseBuild":
      validatePostingBuildFinish(entry.input);
      break;
    case "abortFtsBaseBuild":
      validatePostingBuildAbort(entry.input);
      break;
    case "beginSnapshotFrameExport":
      validateSnapshotFrameExportState(entry.state);
      break;
    case "advanceSnapshotFrameExport":
      validateSnapshotRenewal(entry.input, "Snapshot frame export");
      requireNonNegativeInteger(entry.input.sequence, "snapshot frame export sequence");
      requireNonNegativeInteger(
        entry.expectedLeaseRevision,
        "snapshot frame export lease revision",
      );
      requireRecord(entry.next, "snapshot frame export cursor");
      break;
    case "closeSnapshotFrameExport":
      requireRecord(entry.input, "closeSnapshotFrameExport input");
      requireStorageId(entry.input.sessionId, "closeSnapshotFrameExport session id");
      requireStorageId(entry.input.ownerId, "closeSnapshotFrameExport owner id");
      break;
    case "beginSnapshotFrameImport":
      validateSnapshotFrameImportState(entry.state);
      break;
    case "renewSnapshotFrameImport":
      validateSnapshotRenewal(entry.input, "Snapshot frame import");
      break;
    case "appendSnapshotImportFrames":
      validateSnapshotRenewal(entry.input, "Snapshot frame import");
      requireRecord(entry.state, "snapshot frame import state");
      if (typeof entry.replay !== "boolean") throw new Error("Invalid snapshot frame replay mode");
      validateIdPlacements(entry.blockPlacements, "snapshot frame block placements");
      break;
    case "finishSnapshotFrameImport":
      requireRecord(entry.input, "finishSnapshotFrameImport input");
      requireStorageId(entry.input.identity, "finishSnapshotFrameImport identity");
      requireStorageId(entry.input.ownerId, "finishSnapshotFrameImport owner id");
      requireTimestamp(entry.input.expiresAtCutoff, "finishSnapshotFrameImport cutoff");
      requireRecord(entry.input.footer, "finishSnapshotFrameImport footer");
      break;
    case "cancelSnapshotFrameImport":
      requireRecord(entry.input, "cancelSnapshotFrameImport input");
      assertExactRecordKeys(
        entry.input,
        ["identity", "ownerId"],
        "cancelSnapshotFrameImport input",
      );
      requireStorageId(entry.input.identity, "cancelSnapshotFrameImport identity");
      requireStorageId(entry.input.ownerId, "cancelSnapshotFrameImport owner id");
      break;
    default:
      break;
  }
  // Placement-bearing operations are validated before any replay mutation. Other operations
  // flow through RecordCore's validate-then-mutate methods after their discriminator check.
  for (const placement of placementsOf(entry)) assertValidPlacement(placement);
  return entry;
}

function validateIdPlacements(value: unknown, label: string): asserts value is IdPlacement[] {
  if (!Array.isArray(value)) throw new Error(`Invalid ${label}`);
  const ids = new Set<string>();
  for (const item of value) {
    if (!isRecord(item) || typeof item.id !== "string" || ids.has(item.id)) {
      throw new Error(`Invalid or duplicate ${label}`);
    }
    validateId(item.id);
    ids.add(item.id);
    assertValidPlacement(item.placement as Placement);
  }
}

function assertWalKeys(value: Record<string, unknown>, bodyKeys: readonly string[]): void {
  const allowed = new Set(["seq", "op", ...bodyKeys]);
  for (const key of Object.keys(value)) {
    if (!allowed.has(key)) throw new Error(`Unexpected ${String(value.op)} WAL field: ${key}`);
  }
}

function assertExactRecordKeys(
  value: Record<string, unknown>,
  keys: readonly string[],
  label: string,
): void {
  const expected = new Set(keys);
  const actual = Object.keys(value);
  if (actual.length !== expected.size || actual.some((key) => !expected.has(key))) {
    throw new Error(`Invalid ${label} fields`);
  }
}

function validateRelocations(value: unknown, label: string, blocks: boolean): void {
  if (!Array.isArray(value)) throw new Error(`Invalid ${label}`);
  for (const item of value) {
    if (!isRecord(item)) throw new Error(`Invalid ${label} entry`);
    if (blocks) requireStorageId(item.id, `${label} identity`);
    else requireStorageKey(item.key, `${label} identity`);
    if (!blocks) requireNonNegativeInteger(item.ordinal, `${label} ordinal`);
    assertValidPlacement(item.from as Placement);
    assertValidPlacement(item.placement as Placement);
  }
}

function requireTimestamp(value: unknown, label: string): asserts value is string {
  requireString(value, label);
  const milliseconds = Date.parse(value);
  if (!Number.isFinite(milliseconds) || dateIsoString(new Date(milliseconds)) !== value) {
    throw new Error(`Invalid ${label}`);
  }
}

function validateRuntimePointer(value: unknown, label: string): void {
  if (!isRecord(value) || !Array.isArray(value.chunks) || !Array.isArray(value.chunkBounds)) {
    throw new Error(`Invalid ${label}`);
  }
  if (value.chunks.length !== value.chunkBounds.length) throw new Error(`Invalid ${label} bounds`);
  if (value.chunks.length > MAX_FTS_BASE_CHUNKS) {
    throw new Error(`Invalid ${label} chunk count`);
  }
  for (const placement of value.chunks) assertValidPlacement(placement as Placement);
  requireCoverageVersion(value.coversVersion, `${label} coversVersion`);
  requireNonNegativeInteger(value.totalTokens, `${label} totalTokens`);
  for (const bounds of value.chunkBounds) {
    validateChunkBounds(bounds, label);
  }
}

function validateChunkBounds(value: unknown, label: string): void {
  if (
    !isRecord(value) ||
    typeof value.first !== "string" ||
    value.first.length === 0 ||
    value.first.length > MAX_FTS_POSTING_TERM_CHARACTERS ||
    typeof value.last !== "string" ||
    value.last.length === 0 ||
    value.last.length > MAX_FTS_POSTING_TERM_CHARACTERS ||
    value.first > value.last
  ) {
    throw new Error(`Invalid ${label} chunk bounds`);
  }
}

function requireString(value: unknown, label: string): asserts value is string {
  if (typeof value !== "string" || value.length === 0) throw new Error(`Invalid ${label}`);
}

function requireStorageId(value: unknown, label: string): asserts value is string {
  requireString(value, label);
  if (value.length > MAX_STORAGE_ID_CHARACTERS) throw new Error(`Invalid ${label}`);
}

function requireStorageKey(value: unknown, label: string): asserts value is string {
  requireString(value, label);
  if (value.length > MAX_STORAGE_ID_CHARACTERS * 2 + 1) throw new Error(`Invalid ${label}`);
}

function requireRecord(value: unknown, label: string): asserts value is Record<string, unknown> {
  if (!isRecord(value)) throw new Error(`Invalid ${label}`);
}

function requireNonNegativeInteger(value: unknown, label: string): asserts value is number {
  if (!isNonNegativeSafeInteger(value)) throw new Error(`Invalid ${label}`);
}

function requireCoverageVersion(value: unknown, label: string): asserts value is number {
  if (!Number.isSafeInteger(value) || (value as number) < -1) throw new Error(`Invalid ${label}`);
}

function validateFtsCandidateLimit(value: number): void {
  if (!Number.isSafeInteger(value) || value < 1 || value > MAX_FTS_CANDIDATE_ROW_IDS) {
    throw new RangeError(
      `Full-text candidate limit must be between 1 and ${String(MAX_FTS_CANDIDATE_ROW_IDS)}`,
    );
  }
}

function requirePositiveInteger(value: unknown, label: string): asserts value is number {
  if (!Number.isSafeInteger(value) || (value as number) < 1) throw new Error(`Invalid ${label}`);
}

const POSTING_BUILD_IDENTITY_FIELDS = ["tableId", "columnId", "buildId", "ownerId"] as const;

function validatePostingBuildIdentity(value: Record<string, unknown>, label: string): void {
  for (const field of POSTING_BUILD_IDENTITY_FIELDS) {
    requireStorageId(value[field], `${label} ${field}`);
  }
}

function validatePostingBuildBegin(value: unknown): asserts value is BeginPostingBuildInput {
  requireRecord(value, "posting build begin input");
  assertExactRecordKeys(
    value,
    [...POSTING_BUILD_IDENTITY_FIELDS, "createdAt", "expiresAt"],
    "posting build begin input",
  );
  validatePostingBuildIdentity(value, "posting build begin");
  requireTimestamp(value.createdAt, "posting build creation time");
  requireTimestamp(value.expiresAt, "posting build expiration");
  const lifetime = Date.parse(value.expiresAt) - Date.parse(value.createdAt);
  if (lifetime <= 0 || lifetime > MAX_POSTING_BUILD_TTL_MS) {
    throw new RangeError("Posting build expiration interval is invalid");
  }
}

function validatePostingBuildRenewal(value: unknown): asserts value is RenewPostingBuildInput {
  requireRecord(value, "posting build renewal input");
  assertExactRecordKeys(
    value,
    [...POSTING_BUILD_IDENTITY_FIELDS, "expiresAtCutoff", "expiresAt", "updatedAt"],
    "posting build renewal input",
  );
  validatePostingBuildIdentity(value, "posting build renewal");
  requireTimestamp(value.expiresAtCutoff, "posting build expiration cutoff");
  requireTimestamp(value.expiresAt, "posting build expiration");
  requireTimestamp(value.updatedAt, "posting build update time");
  const lifetime = Date.parse(value.expiresAt) - Date.parse(value.expiresAtCutoff);
  if (lifetime <= 0 || lifetime > MAX_POSTING_BUILD_TTL_MS) {
    throw new RangeError("Posting build renewal interval is invalid");
  }
}

function validatePostingBuildAppend(
  value: unknown,
  includeChunk: boolean,
): asserts value is AppendPostingBuildChunkInput {
  requireRecord(value, "posting build append input");
  assertExactRecordKeys(
    value,
    [
      ...POSTING_BUILD_IDENTITY_FIELDS,
      "expiresAtCutoff",
      "expiresAt",
      "updatedAt",
      "ordinal",
      ...(includeChunk ? ["chunk"] : []),
    ],
    "posting build append input",
  );
  const renewal = Object.fromEntries(
    Object.entries(value).filter(([key]) => key !== "ordinal" && key !== "chunk"),
  );
  validatePostingBuildRenewal(renewal);
  requireNonNegativeInteger(value.ordinal, "posting build chunk ordinal");
  if (includeChunk && !Array.isArray(value.chunk)) {
    throw new TypeError("Posting build chunk must be an array");
  }
}

function validatePostingBuildFinish(value: unknown): asserts value is FinishPostingBuildInput {
  requireRecord(value, "posting build finish input");
  assertExactRecordKeys(
    value,
    [
      ...POSTING_BUILD_IDENTITY_FIELDS,
      "expiresAtCutoff",
      "coversVersion",
      "chunkCount",
      "totalTokens",
      "completedAt",
    ],
    "posting build finish input",
  );
  validatePostingBuildIdentity(value, "posting build finish");
  requireTimestamp(value.expiresAtCutoff, "posting build finish cutoff");
  requireTimestamp(value.completedAt, "posting build completion time");
  requireCoverageVersion(value.coversVersion, "posting build coverage version");
  requireNonNegativeInteger(value.chunkCount, "posting build chunk count");
  requireNonNegativeInteger(value.totalTokens, "posting build token count");
  if (value.chunkCount > MAX_FTS_BASE_CHUNKS) {
    throw new RangeError("Posting build exceeds the chunk-count limit");
  }
}

function validatePostingBuildAbort(value: unknown): asserts value is AbortPostingBuildInput {
  requireRecord(value, "posting build abort input");
  assertExactRecordKeys(
    value,
    [...POSTING_BUILD_IDENTITY_FIELDS, "expiresAtCutoff"],
    "posting build abort input",
  );
  validatePostingBuildIdentity(value, "posting build abort");
  requireTimestamp(value.expiresAtCutoff, "posting build abort cutoff");
}

function laterTimestamp(left: string, right: string): string {
  return Date.parse(left) >= Date.parse(right) ? left : right;
}

function validateCheckpointState(value: unknown): CheckpointState {
  if (!isRecord(value)) throw new Error("OPFS checkpoint is not an object");
  if (value.formatVersion !== 1) {
    if (typeof value.formatVersion !== "number" || !Number.isSafeInteger(value.formatVersion)) {
      throw new Error("Invalid OPFS checkpoint format version");
    }
    throw new StorageFormatVersionError(
      "opfs",
      "checkpoint/state",
      value.formatVersion,
      1,
      value.formatVersion < 1 ? "older" : "newer",
    );
  }
  if (!isNonNegativeSafeInteger(value.lastSeq)) {
    throw new Error(`Invalid OPFS checkpoint sequence: ${String(value.lastSeq)}`);
  }
  if (!Number.isSafeInteger(value.generation) || (value.generation as number) < 1) {
    throw new Error(`Invalid OPFS checkpoint generation: ${String(value.generation)}`);
  }
  if (!Array.isArray(value.blockIndex) || !Array.isArray(value.ftsBases)) {
    throw new Error("Invalid OPFS checkpoint payload indexes");
  }
  if (!Array.isArray(value.ftsBuilds)) {
    throw new Error("Invalid OPFS checkpoint staged full-text index");
  }
  if (!Array.isArray(value.tempPages)) {
    throw new Error("Invalid OPFS checkpoint temp-page ledger");
  }
  assertExactRecordKeys(
    value,
    [
      "formatVersion",
      "generation",
      "lastSeq",
      "core",
      "blockIndex",
      "ftsBases",
      "ftsBuilds",
      "tempPages",
      ...(value.snapshotFrameExport === undefined ? [] : ["snapshotFrameExport"]),
      ...(value.snapshotFrameImport === undefined ? [] : ["snapshotFrameImport"]),
      ...(value.completedSnapshotFrameImport === undefined ? [] : ["completedSnapshotFrameImport"]),
      "extents",
    ],
    "OPFS checkpoint",
  );
  assertValidExtentMeta(value.extents as ExtentMeta);

  const blockIds = new Set<string>();
  for (const entry of value.blockIndex) {
    if (!Array.isArray(entry) || entry.length !== 2 || typeof entry[0] !== "string") {
      throw new Error("Invalid OPFS checkpoint block-index entry");
    }
    if (blockIds.has(entry[0])) throw new Error(`Duplicate OPFS checkpoint block id: ${entry[0]}`);
    validateId(entry[0]);
    blockIds.add(entry[0]);
    assertValidPlacement(entry[1] as Placement);
  }
  validatePointerEntries(value.ftsBases, "full-text base");
  validatePointerEntries(value.ftsBuilds, "staged full-text base");
  for (const page of value.tempPages) {
    validateTempPageLedgerRecord(page);
    if (page.length === null) throw new Error("OPFS checkpoint temp page length is missing");
  }
  if (value.snapshotFrameExport !== undefined) {
    validateSnapshotFrameExportState(value.snapshotFrameExport);
  }
  if (value.snapshotFrameImport !== undefined) {
    validateSnapshotFrameImportState(value.snapshotFrameImport);
  }
  if (value.completedSnapshotFrameImport !== undefined) {
    validateCompletedSnapshotFrameImportState(value.completedSnapshotFrameImport);
  }

  const state = value as unknown as CheckpointState;
  if (state.snapshotFrameImport === undefined || state.snapshotFrameImport.completedReplay) {
    validateLivePlacementAccounting(allCheckpointPlacements(state), state.extents);
  }
  return state;
}

function validateTempPageLedgerRecord(
  value: unknown,
): asserts value is Omit<TempPageLedgerRecord, "length"> & { length: number | null } {
  requireRecord(value, "temp page ledger record");
  assertExactRecordKeys(
    value,
    ["ownerId", "runId", "pageIndex", "length"],
    "temp page ledger record",
  );
  requireStorageId(value.ownerId, "temp page owner id");
  requireStorageId(value.runId, "temp page run id");
  requireNonNegativeInteger(value.pageIndex, "temp page index");
  if (value.length !== null) {
    requireNonNegativeInteger(value.length, "temp page byte length");
    if (value.length > MAX_TEMP_RUN_PAGE_BYTES) {
      throw new Error("Invalid temp page byte length");
    }
  }
}

function validateCompletedSnapshotFrameImportState(
  value: unknown,
): asserts value is CompletedSnapshotFrameImportState {
  requireRecord(value, "completed snapshot frame import state");
  assertExactRecordKeys(
    value,
    ["identity", "version", "header", "ledgerId", "ledgerLength"],
    "completed snapshot frame import state",
  );
  requireStorageId(value.identity, "completed snapshot frame import identity");
  requireStorageId(value.ledgerId, "completed snapshot frame import ledger id");
  requireNonNegativeInteger(value.version, "completed snapshot frame import version");
  requireNonNegativeInteger(value.ledgerLength, "completed snapshot frame import ledger length");
  const header = prepareSnapshotFrameStreamHeader(value.header as SnapshotFrameStreamHeader);
  if (header.databaseVersion !== value.version) {
    throw new Error("Completed snapshot frame import header version changed");
  }
}

function validatePointerEntries(entries: unknown[], label: string): void {
  const keys = new Set<string>();
  for (const entry of entries) {
    if (!Array.isArray(entry) || entry.length !== 2 || typeof entry[0] !== "string") {
      throw new Error(`Invalid OPFS checkpoint ${label} entry`);
    }
    if (keys.has(entry[0])) throw new Error(`Duplicate OPFS checkpoint ${label}: ${entry[0]}`);
    validatePostingStorageKey(entry[0], label);
    keys.add(entry[0]);
    const pointer: unknown = entry[1];
    if (
      !isRecord(pointer) ||
      !Array.isArray(pointer.chunks) ||
      !Array.isArray(pointer.chunkBounds)
    ) {
      throw new Error(`Invalid OPFS checkpoint ${label} pointer: ${entry[0]}`);
    }
    if (pointer.chunks.length !== pointer.chunkBounds.length) {
      throw new Error(`OPFS checkpoint ${label} bounds do not match its chunks: ${entry[0]}`);
    }
    if (pointer.chunks.length > MAX_FTS_BASE_CHUNKS) {
      throw new Error(`OPFS checkpoint ${label} exceeds the chunk-count limit: ${entry[0]}`);
    }
    for (const placement of pointer.chunks) assertValidPlacement(placement as Placement);
    for (const bounds of pointer.chunkBounds) validateChunkBounds(bounds, label);
    if (label === "full-text base") {
      assertExactRecordKeys(
        pointer,
        ["coversVersion", "totalTokens", "chunks", "chunkBounds"],
        `${label} pointer`,
      );
      requireCoverageVersion(pointer.coversVersion, `${label} coversVersion`);
      requireNonNegativeInteger(pointer.totalTokens, `${label} totalTokens`);
    } else {
      assertExactRecordKeys(
        pointer,
        [
          "buildId",
          "ownerId",
          "ownerKind",
          "createdAt",
          "expiresAt",
          "updatedAt",
          "chunks",
          "chunkBounds",
          "totalTokens",
          "retainedBytes",
          "retainedEntries",
        ],
        `${label} pointer`,
      );
      requireStorageId(pointer.buildId, `${label} buildId`);
      requireStorageId(pointer.ownerId, `${label} ownerId`);
      if (pointer.ownerKind !== "fts-column" && pointer.ownerKind !== "secondary-index") {
        throw new Error(`Invalid ${label} owner kind`);
      }
      requireTimestamp(pointer.createdAt, `${label} createdAt`);
      requireTimestamp(pointer.expiresAt, `${label} expiresAt`);
      requireTimestamp(pointer.updatedAt, `${label} updatedAt`);
      const lifetime = Date.parse(pointer.expiresAt) - Date.parse(pointer.createdAt);
      if (lifetime <= 0 || lifetime > MAX_POSTING_BUILD_TTL_MS) {
        throw new RangeError(`${label} expiration interval is invalid`);
      }
      requireNonNegativeInteger(pointer.totalTokens, `${label} totalTokens`);
      requireNonNegativeInteger(pointer.retainedBytes, `${label} retainedBytes`);
      requireNonNegativeInteger(pointer.retainedEntries, `${label} retainedEntries`);
      let placementBytes = 0;
      for (const value of pointer.chunks) {
        const placement = value as Placement;
        placementBytes = safeByteSum(
          placementBytes,
          placement.length,
          `${label} retained placement bytes`,
        );
      }
      if (placementBytes !== pointer.retainedBytes) {
        throw new Error(`${label} retained bytes do not match its chunks`);
      }
    }
  }
}

function allCheckpointPlacements(state: CheckpointState): Placement[] {
  const placements = [
    ...state.blockIndex.map(([, placement]) => placement),
    ...state.ftsBases.flatMap(([, pointer]) => pointer.chunks),
    ...state.ftsBuilds.flatMap(([, pointer]) => pointer.chunks),
  ];
  return [...new Map(placements.map((placement) => [placementKey(placement), placement])).values()];
}

function validateLivePlacementAccounting(placements: readonly Placement[], meta: ExtentMeta): void {
  assertValidExtentMeta(meta);
  const sums = new Map<number, number>();
  const ranges = new Map<number, Array<{ start: number; end: number }>>();
  for (const placement of placements) {
    assertValidPlacement(placement);
    if (placement.extent >= meta.nextExtentId) {
      throw new Error(`OPFS placement references future extent ${String(placement.extent)}`);
    }
    const end = placement.offset + placement.length;
    if (placement.extent === meta.tailExtentId && end > meta.tailOffset) {
      throw new Error(
        `OPFS placement exceeds checkpointed tail: ${String(placement.offset)}+` +
          `${String(placement.length)} > ${String(meta.tailOffset)}`,
      );
    }
    sums.set(placement.extent, (sums.get(placement.extent) ?? 0) + placement.length);
    if (placement.length > 0) {
      const extentRanges = ranges.get(placement.extent) ?? [];
      extentRanges.push({ start: placement.offset, end });
      ranges.set(placement.extent, extentRanges);
    }
  }
  for (const [id, extentRanges] of ranges) {
    extentRanges.sort((left, right) => left.start - right.start || left.end - right.end);
    for (let index = 1; index < extentRanges.length; index += 1) {
      const previous = extentRanges[index - 1];
      const current = extentRanges[index];
      if (previous !== undefined && current !== undefined && current.start < previous.end) {
        throw new Error(`OPFS checkpoint has overlapping placements in extent ${String(id)}`);
      }
    }
  }
  const recorded = new Map(meta.liveBytes);
  const ids = new Set([...sums.keys(), ...recorded.keys()]);
  for (const id of ids) {
    if ((sums.get(id) ?? 0) !== (recorded.get(id) ?? 0)) {
      throw new Error(
        `OPFS extent ${String(id)} live-byte mismatch: placements ${String(sums.get(id) ?? 0)}, ` +
          `checkpoint ${String(recorded.get(id) ?? 0)}`,
      );
    }
  }
}

async function validateCheckpointPhysical(state: CheckpointState, tree: OpfsTree): Promise<void> {
  const files = new Map<string, number>();
  for await (const { path, size } of tree.walkFiles(["extents"])) {
    if (path.length === 1) files.set(path[0] ?? "", size);
  }
  for (const placement of allCheckpointPlacements(state)) {
    const name = extentPath(placement.extent).at(-1) ?? "";
    const size = files.get(name);
    if (size === undefined || placement.offset + placement.length > size) {
      throw new Error(
        `OPFS checkpoint placement exceeds or is missing its extent: ${String(placement.extent)}:` +
          `${String(placement.offset)}+${String(placement.length)}`,
      );
    }
  }
  const ledgers: Array<{
    kind: "export" | "import";
    id: string;
    length: number;
    includePlacements: boolean;
  }> = [];
  if (state.snapshotFrameExport !== undefined) {
    ledgers.push({
      kind: "export",
      id: state.snapshotFrameExport.ledgerId,
      length: state.snapshotFrameExport.ledgerLength,
      includePlacements: false,
    });
  }
  const importState = state.snapshotFrameImport ?? state.completedSnapshotFrameImport;
  if (importState !== undefined) {
    ledgers.push({
      kind: "import",
      id: importState.ledgerId,
      length: importState.ledgerLength,
      includePlacements:
        state.snapshotFrameImport !== undefined && !state.snapshotFrameImport.completedReplay,
    });
  }
  for (const descriptor of ledgers) {
    const ledger = await SnapshotFrameLedger.open(
      tree,
      descriptor.kind,
      descriptor.id,
      descriptor.length,
      false,
    );
    try {
      for (const { record } of ledger.records(descriptor.length)) {
        if (!descriptor.includePlacements || record.placement === undefined) continue;
        const name = extentPath(record.placement.extent).at(-1) ?? "";
        const size = files.get(name);
        if (size === undefined || record.placement.offset + record.placement.length > size) {
          throw new Error("Snapshot frame ledger placement exceeds or is missing its extent");
        }
      }
    } finally {
      ledger.close();
    }
  }
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function safeSuccessor(value: number, label: string): number {
  if (!Number.isSafeInteger(value) || value < 0 || value >= Number.MAX_SAFE_INTEGER) {
    throw new RangeError(`${label} cannot exceed the safe integer range`);
  }
  return value + 1;
}

function isSafeSuccessor(previous: number, next: number): boolean {
  return previous < Number.MAX_SAFE_INTEGER && next === previous + 1;
}

function isNonNegativeSafeInteger(value: unknown): value is number {
  return Number.isSafeInteger(value) && (value as number) >= 0;
}

function equalBytes(left: Uint8Array, right: Uint8Array): boolean {
  if (left.byteLength !== right.byteLength) return false;
  for (let index = 0; index < left.byteLength; index += 1) {
    if (left[index] !== right[index]) return false;
  }
  return true;
}

function lowerBoundStrings(values: readonly string[], target: string): number {
  let low = 0;
  let high = values.length;
  while (low < high) {
    const middle = low + ((high - low) >> 1);
    if ((values[middle] ?? "") < target) low = middle + 1;
    else high = middle;
  }
  return low;
}

function safeByteSum(left: number, right: number, label: string): number {
  const total = left + right;
  if (
    !Number.isSafeInteger(left) ||
    !Number.isSafeInteger(right) ||
    left < 0 ||
    right < 0 ||
    !Number.isSafeInteger(total)
  ) {
    throw new RangeError(`${label} exceeds the safe integer range`);
  }
  return total;
}

function postingStorageKey(tableId: string, columnId: string): string {
  return JSON.stringify([tableId, columnId]);
}

function parsePostingStorageKey(value: string, label: string): readonly [string, string] {
  let decoded: unknown;
  try {
    decoded = JSON.parse(value);
  } catch {
    throw new Error(`Invalid ${label} key`);
  }
  if (
    !Array.isArray(decoded) ||
    decoded.length !== 2 ||
    typeof decoded[0] !== "string" ||
    typeof decoded[1] !== "string"
  ) {
    throw new Error(`Invalid ${label} key`);
  }
  requireStorageId(decoded[0], `${label} table id`);
  requireStorageId(decoded[1], `${label} column id`);
  if (postingStorageKey(decoded[0], decoded[1]) !== value) {
    throw new Error(`Non-canonical ${label} key`);
  }
  return [decoded[0], decoded[1]];
}

function validatePostingStorageKey(value: string, label: string): void {
  parsePostingStorageKey(value, label);
}

function validateSnapshotFrameExportState(
  value: unknown,
): asserts value is SnapshotFrameExportState {
  requireRecord(value, "snapshot frame export state");
  assertExactRecordKeys(
    value,
    [
      "sessionId",
      "ownerId",
      "createdAt",
      "expiresAt",
      "leaseRevision",
      "manifestVersion",
      "header",
      "ledgerId",
      "ledgerLength",
      "metadataFrameCount",
      "nextSequence",
      "metadataOffset",
      "blockCursor",
      "lastSequence",
      "lastMetadataOffset",
      "lastBlockId",
    ],
    "snapshot frame export state",
  );
  requireStorageId(value.sessionId, "snapshot frame export session id");
  requireStorageId(value.ownerId, "snapshot frame export owner id");
  requireStorageId(value.ledgerId, "snapshot frame export ledger id");
  validateSnapshotStateLifetime(
    value.createdAt as string,
    value.expiresAt as string,
    "Snapshot frame export",
  );
  requireNonNegativeInteger(value.leaseRevision, "snapshot frame export lease revision");
  requireNonNegativeInteger(value.manifestVersion, "snapshot frame export manifest version");
  requireNonNegativeInteger(value.ledgerLength, "snapshot frame export ledger length");
  requireNonNegativeInteger(value.metadataFrameCount, "snapshot frame export metadata frame count");
  requireNonNegativeInteger(value.nextSequence, "snapshot frame export next sequence");
  requireNonNegativeInteger(value.metadataOffset, "snapshot frame export metadata offset");
  for (const [field, item] of [
    ["lastSequence", value.lastSequence],
    ["lastMetadataOffset", value.lastMetadataOffset],
  ] as const) {
    if (item !== null) requireNonNegativeInteger(item, `snapshot frame export ${field}`);
  }
  if (value.blockCursor !== null)
    requireStorageId(value.blockCursor, "snapshot frame export block cursor");
  if (value.lastBlockId !== null)
    requireStorageId(value.lastBlockId, "snapshot frame export last block id");
  const header = prepareSnapshotFrameStreamHeader(value.header as SnapshotFrameStreamHeader);
  if (header.databaseVersion !== value.manifestVersion) {
    throw new Error("Snapshot frame export header version changed");
  }
  const metadataFrameCount = SNAPSHOT_FRAME_KINDS.reduce(
    (total, kind) =>
      kind === "block"
        ? total
        : safeByteSum(total, header.kinds[kind].frameCount, "Snapshot metadata frame count"),
    0,
  );
  const totalFrameCount = safeByteSum(
    metadataFrameCount,
    header.kinds.block.frameCount,
    "Snapshot frame count",
  );
  if (
    value.metadataFrameCount !== metadataFrameCount ||
    value.nextSequence > totalFrameCount ||
    value.metadataOffset > value.ledgerLength ||
    (value.nextSequence <= metadataFrameCount && value.blockCursor !== null) ||
    (value.nextSequence === 0
      ? value.lastSequence !== null ||
        value.lastMetadataOffset !== null ||
        value.lastBlockId !== null
      : value.lastSequence !== value.nextSequence - 1)
  ) {
    throw new Error("Snapshot frame export cursor is inconsistent");
  }
}

function validateSnapshotFrameImportState(
  value: unknown,
): asserts value is SnapshotFrameImportState {
  requireRecord(value, "snapshot frame import state");
  assertExactRecordKeys(
    value,
    [
      "identity",
      "ownerId",
      "version",
      "createdAt",
      "expiresAt",
      "header",
      "ledgerId",
      "ledgerLength",
      "nextSequence",
      "stagedBytes",
      "blockCount",
      "blockBytes",
      "checksum",
      "currentKindIndex",
      "observed",
      "lastBatchStartSequence",
      "lastBatchOffset",
      "lastBatchFrameCount",
      "completedReplay",
    ],
    "snapshot frame import state",
  );
  requireStorageId(value.identity, "snapshot frame import identity");
  requireStorageId(value.ownerId, "snapshot frame import owner id");
  requireStorageId(value.ledgerId, "snapshot frame import ledger id");
  validateSnapshotStateLifetime(
    value.createdAt as string,
    value.expiresAt as string,
    "Snapshot frame import",
  );
  for (const field of [
    "version",
    "ledgerLength",
    "nextSequence",
    "stagedBytes",
    "blockCount",
    "blockBytes",
    "checksum",
    "currentKindIndex",
    "lastBatchFrameCount",
  ] as const) {
    requireNonNegativeInteger(value[field], `snapshot frame import ${field}`);
  }
  if (
    typeof value.checksum !== "number" ||
    typeof value.currentKindIndex !== "number" ||
    value.checksum > 0xffff_ffff ||
    value.currentKindIndex >= SNAPSHOT_FRAME_KINDS.length
  ) {
    throw new Error("Snapshot frame import checksum or kind cursor is invalid");
  }
  for (const field of ["lastBatchStartSequence", "lastBatchOffset"] as const) {
    if (value[field] !== null) {
      requireNonNegativeInteger(value[field], `snapshot frame import ${field}`);
    }
  }
  if (typeof value.completedReplay !== "boolean") {
    throw new Error("Snapshot frame import replay mode is invalid");
  }
  const header = prepareSnapshotFrameStreamHeader(value.header as SnapshotFrameStreamHeader);
  if (header.databaseVersion !== value.version) {
    throw new Error("Snapshot frame import header version changed");
  }
  requireRecord(value.observed, "snapshot frame import observed summaries");
  assertExactRecordKeys(value.observed, SNAPSHOT_FRAME_KINDS, "snapshot observed summaries");
  for (const kind of SNAPSHOT_FRAME_KINDS) {
    const summary = value.observed[kind];
    requireRecord(summary, `snapshot ${kind} observed summary`);
    assertExactRecordKeys(
      summary,
      ["frameCount", "itemCount", "storedBytes"],
      `snapshot ${kind} observed summary`,
    );
    requireNonNegativeInteger(summary.frameCount, `snapshot ${kind} observed frame count`);
    requireNonNegativeInteger(summary.itemCount, `snapshot ${kind} observed item count`);
    requireNonNegativeInteger(summary.storedBytes, `snapshot ${kind} observed stored bytes`);
    const expected = header.kinds[kind];
    if (
      summary.frameCount > expected.frameCount ||
      summary.itemCount > expected.itemCount ||
      summary.storedBytes > expected.storedBytes
    ) {
      throw new Error(`Snapshot ${kind} observed summary exceeds its header`);
    }
  }
  if ((value.nextSequence as number) > snapshotHeaderFrameCount(header)) {
    throw new Error("Snapshot frame import sequence exceeds its header");
  }
}

function validateSnapshotLifetime(createdAt: string, expiresAt: string, label: string): void {
  requireTimestamp(createdAt, `${label} creation time`);
  requireTimestamp(expiresAt, `${label} expiration`);
  const lifetime = Date.parse(expiresAt) - Date.parse(createdAt);
  if (lifetime <= 0 || lifetime > MAX_SNAPSHOT_SESSION_TTL_MS) {
    throw new RangeError(`${label} expiration interval is invalid`);
  }
}

/**
 * A durable state keeps the session's original creation time while a valid renewal moves its
 * expiry forward. The renewal WAL input carries the bounded cutoff-to-expiry interval, so state
 * validation must not mistake a session older than one TTL for one lease longer than the TTL.
 */
function validateSnapshotStateLifetime(createdAt: string, expiresAt: string, label: string): void {
  requireTimestamp(createdAt, `${label} creation time`);
  requireTimestamp(expiresAt, `${label} expiration`);
  if (Date.parse(expiresAt) <= Date.parse(createdAt)) {
    throw new RangeError(`${label} expiration interval is invalid`);
  }
}

function validateSnapshotRenewal(
  input: {
    ownerId: string;
    expiresAtCutoff: string;
    expiresAt: string;
    identity?: string;
    sessionId?: string;
  },
  label: string,
): void {
  validateId(input.ownerId);
  if (input.identity !== undefined) validateId(input.identity);
  if (input.sessionId !== undefined) validateId(input.sessionId);
  validateSnapshotLifetime(input.expiresAtCutoff, input.expiresAt, label);
}

function snapshotFrameExportLeaseId(sessionId: string): string {
  return `snapshot-frame-export/${sessionId}`;
}

function snapshotFrameExportSession(state: SnapshotFrameExportState): SnapshotFrameExportSession {
  return {
    sessionId: state.sessionId,
    ownerId: state.ownerId,
    expiresAt: state.expiresAt,
    header: structuredClone(state.header),
  };
}

function snapshotFrameImportSession(state: SnapshotFrameImportState): SnapshotFrameImportSession {
  return {
    identity: state.identity,
    ownerId: state.ownerId,
    version: state.version,
    createdAt: state.createdAt,
    expiresAt: state.expiresAt,
    nextSequence: state.nextSequence,
    stagedBytes: state.stagedBytes,
  };
}

function emptySnapshotFrameSummaries(): Record<SnapshotFrameKind, SnapshotKindSummary> {
  return Object.fromEntries(
    SNAPSHOT_FRAME_KINDS.map((kind) => [kind, { frameCount: 0, itemCount: 0, storedBytes: 0 }]),
  ) as Record<SnapshotFrameKind, SnapshotKindSummary>;
}

function snapshotMetadataFrameKind(item: SnapshotMetadataItem): SnapshotFrameKind {
  switch (item.kind) {
    case "table":
      return "catalog-page";
    case "segment":
      return "segment-page";
    case "transaction":
      return "transaction-page";
    case "unique-generation":
    case "unique-chunk":
      return "unique-page";
    case "posting-generation":
    case "posting-chunk":
      return "posting-page";
  }
}

function appendSnapshotMetadataItems(
  ledger: SnapshotFrameLedger,
  kind: SnapshotFrameKind,
  items: readonly SnapshotMetadataItem[],
  sequence: number,
  summaries: Record<SnapshotFrameKind, SnapshotKindSummary>,
): number {
  if (kind === "block") throw new TypeError("Block frames cannot contain metadata items");
  if (items.length !== 1) throw new TypeError("Snapshot metadata frames contain exactly one item");
  const payload = encodeSnapshotMetadataPage(items);
  const frame: SnapshotFrame = {
    sequence,
    kind,
    itemCount: items.length,
    key: null,
    payload,
    checksum: crc32(payload),
  };
  ledger.append(frame);
  const summary = summaries[kind];
  summaries[kind] = {
    frameCount: safeByteSum(summary.frameCount, 1, `Snapshot ${kind} frame count`),
    itemCount: safeByteSum(summary.itemCount, items.length, `Snapshot ${kind} item count`),
    storedBytes: safeByteSum(
      summary.storedBytes,
      payload.byteLength,
      `Snapshot ${kind} stored bytes`,
    ),
  };
  return safeSuccessor(sequence, "Snapshot frame sequence");
}

function snapshotHeaderFrameCount(header: SnapshotFrameStreamHeader): number {
  return SNAPSHOT_FRAME_KINDS.reduce(
    (total, kind) => safeByteSum(total, header.kinds[kind].frameCount, "Snapshot frame count"),
    0,
  );
}

function frameFromSnapshotLedger(
  read: SnapshotLedgerRead,
  payload: Uint8Array | undefined,
): SnapshotFrame {
  const record = read.record;
  const bytes = record.payload ?? payload;
  if (bytes === undefined) throw new Error("Snapshot ledger frame payload is unavailable");
  const frame: SnapshotFrame = {
    sequence: record.sequence,
    kind: record.kind,
    itemCount: record.itemCount,
    key: record.key,
    payload: bytes,
    checksum: record.checksum,
  };
  snapshotFrameEnvelopeParts(frame);
  return frame;
}

function snapshotFrameExportCursor(
  state: SnapshotFrameExportState,
): Pick<
  SnapshotFrameExportState,
  | "nextSequence"
  | "metadataOffset"
  | "blockCursor"
  | "lastSequence"
  | "lastMetadataOffset"
  | "lastBlockId"
> {
  return {
    nextSequence: state.nextSequence,
    metadataOffset: state.metadataOffset,
    blockCursor: state.blockCursor,
    lastSequence: state.lastSequence,
    lastMetadataOffset: state.lastMetadataOffset,
    lastBlockId: state.lastBlockId,
  };
}

function validateSnapshotFrameExportAdvance(
  current: SnapshotFrameExportState,
  requestedSequence: number,
  next: Pick<
    SnapshotFrameExportState,
    | "nextSequence"
    | "metadataOffset"
    | "blockCursor"
    | "lastSequence"
    | "lastMetadataOffset"
    | "lastBlockId"
  >,
): void {
  const replay = requestedSequence === current.lastSequence;
  if (replay) {
    if (JSON.stringify(next) !== JSON.stringify(snapshotFrameExportCursor(current))) {
      throw new Error("Snapshot export replay changed its cursor");
    }
    return;
  }
  if (
    requestedSequence !== current.nextSequence ||
    next.nextSequence !== safeSuccessor(current.nextSequence, "Snapshot export sequence") ||
    next.lastSequence !== requestedSequence
  ) {
    throw new Error("Snapshot export cursor is not the next valid state");
  }
  if (requestedSequence < current.metadataFrameCount) {
    if (
      next.lastMetadataOffset !== current.metadataOffset ||
      next.metadataOffset <= current.metadataOffset ||
      next.blockCursor !== current.blockCursor ||
      next.lastBlockId !== null
    ) {
      throw new Error("Snapshot metadata cursor is invalid");
    }
  } else if (
    next.metadataOffset !== current.metadataOffset ||
    next.lastMetadataOffset !== null ||
    next.lastBlockId === null ||
    next.blockCursor !== next.lastBlockId ||
    (current.blockCursor !== null && next.blockCursor <= current.blockCursor)
  ) {
    throw new Error("Snapshot block cursor is invalid");
  }
}

function equalSnapshotFrameHeaders(
  left: SnapshotFrameStreamHeader,
  right: SnapshotFrameStreamHeader,
): boolean {
  return JSON.stringify(left) === JSON.stringify(right);
}

function newSnapshotFrameImportState(
  input: BeginSnapshotFrameImportInput,
  header: SnapshotFrameStreamHeader,
  ledgerId: string,
  completedReplay: boolean,
): SnapshotFrameImportState {
  return {
    identity: input.identity,
    ownerId: input.ownerId,
    version: header.databaseVersion,
    createdAt: input.createdAt,
    expiresAt: input.expiresAt,
    header,
    ledgerId,
    ledgerLength: 0,
    nextSequence: 0,
    stagedBytes: 0,
    blockCount: 0,
    blockBytes: 0,
    checksum: 0,
    currentKindIndex: 0,
    observed: emptySnapshotFrameSummaries(),
    lastBatchStartSequence: null,
    lastBatchOffset: null,
    lastBatchFrameCount: 0,
    completedReplay,
  };
}

function validateSnapshotFrameBatch(frames: readonly SnapshotFrame[]): void {
  if (frames.length < 1 || frames.length > MAX_SNAPSHOT_FRAME_BATCH_ITEMS) {
    throw new RangeError("Snapshot frame batch is empty or too large");
  }
  let bytes = 0;
  let metadataBytes = 0;
  let blockCount = 0;
  for (const frame of frames) {
    const parts = snapshotFrameEnvelopeParts(frame);
    if (crc32(frame.payload) !== frame.checksum)
      throw new Error("Snapshot frame checksum mismatch");
    bytes = safeByteSum(bytes, frame.payload.byteLength, "Snapshot frame batch bytes");
    if (frame.kind === "block") blockCount += 1;
    else {
      metadataBytes = safeByteSum(
        metadataBytes,
        frame.payload.byteLength,
        "Snapshot metadata batch bytes",
      );
      const items = decodeSnapshotMetadataItems(frame.kind, frame.payload);
      if (items.length !== frame.itemCount) {
        throw new Error("Snapshot frame item count disagrees with its payload");
      }
    }
    void parts;
  }
  if (
    bytes > MAX_SNAPSHOT_FRAME_BATCH_BYTES ||
    metadataBytes > MAX_SNAPSHOT_METADATA_BATCH_BYTES ||
    blockCount > 1
  ) {
    throw new RangeError("Snapshot frame batch exceeds its byte or block limit");
  }
}

function advanceSnapshotFrameImportState(
  current: SnapshotFrameImportState,
  frames: readonly SnapshotFrame[],
  expiresAt: string,
): SnapshotFrameImportState {
  const observed = structuredClone(current.observed);
  let sequence = current.nextSequence;
  let stagedBytes = current.stagedBytes;
  let blockCount = current.blockCount;
  let blockBytes = current.blockBytes;
  let checksum = current.checksum;
  let kindIndex = current.currentKindIndex;
  for (const frame of frames) {
    if (frame.sequence !== sequence)
      throw new Error("Snapshot import frame sequence is not contiguous");
    const nextKindIndex = SNAPSHOT_FRAME_KINDS.indexOf(frame.kind);
    if (nextKindIndex < kindIndex) throw new Error("Snapshot import frame kinds are out of order");
    kindIndex = nextKindIndex;
    const summary = observed[frame.kind];
    const nextSummary = {
      frameCount: safeByteSum(summary.frameCount, 1, "Snapshot observed frame count"),
      itemCount: safeByteSum(summary.itemCount, frame.itemCount, "Snapshot observed item count"),
      storedBytes: safeByteSum(
        summary.storedBytes,
        frame.payload.byteLength,
        "Snapshot observed stored bytes",
      ),
    };
    const expected = current.header.kinds[frame.kind];
    if (
      nextSummary.frameCount > expected.frameCount ||
      nextSummary.itemCount > expected.itemCount ||
      nextSummary.storedBytes > expected.storedBytes
    ) {
      throw new Error(`Snapshot ${frame.kind} frames exceed the declared summary`);
    }
    observed[frame.kind] = nextSummary;
    checksum = extendSnapshotFrameStreamChecksum(checksum, snapshotFrameEnvelopeParts(frame));
    stagedBytes = safeByteSum(stagedBytes, frame.payload.byteLength, "Snapshot staged bytes");
    if (frame.kind === "block") {
      blockCount = safeByteSum(blockCount, 1, "Snapshot staged block count");
      blockBytes = safeByteSum(blockBytes, frame.payload.byteLength, "Snapshot staged block bytes");
    }
    sequence = safeSuccessor(sequence, "Snapshot import sequence");
  }
  return {
    ...current,
    expiresAt:
      Date.parse(expiresAt) > Date.parse(current.expiresAt) ? expiresAt : current.expiresAt,
    nextSequence: sequence,
    stagedBytes,
    blockCount,
    blockBytes,
    checksum,
    currentKindIndex: kindIndex,
    observed,
    lastBatchStartSequence: frames[0]?.sequence ?? null,
    lastBatchOffset: current.ledgerLength,
    lastBatchFrameCount: frames.length,
  };
}

function durableSnapshotFrameImportState(
  state: SnapshotFrameImportState,
): Omit<SnapshotFrameImportState, "header"> {
  const { header: _header, ...durable } = state;
  void _header;
  return durable;
}

function validateSnapshotFrameImportTransition(
  current: SnapshotFrameImportState,
  next: SnapshotFrameImportState,
  blockPlacements: readonly IdPlacement[],
): void {
  if (
    current.identity !== next.identity ||
    current.ownerId !== next.ownerId ||
    current.ledgerId !== next.ledgerId ||
    current.completedReplay !== next.completedReplay ||
    next.nextSequence <= current.nextSequence ||
    next.ledgerLength < current.ledgerLength ||
    (current.completedReplay
      ? blockPlacements.length !== 0
      : next.blockCount - current.blockCount !== blockPlacements.length)
  ) {
    throw new Error(
      `Snapshot frame import state transition is invalid: ` +
        `${current.identity === next.identity ? "identity-ok" : "identity"},` +
        `${current.ownerId === next.ownerId ? "owner-ok" : "owner"},` +
        `${current.ledgerId === next.ledgerId ? "ledger-ok" : "ledger"},` +
        `${String(current.nextSequence)}->${String(next.nextSequence)},` +
        `${String(current.ledgerLength)}->${String(next.ledgerLength)},` +
        `blocks ${String(current.blockCount)}->${String(next.blockCount)}/` +
        String(blockPlacements.length),
    );
  }
}

function validateSnapshotFrameImportLastBatch(
  state: SnapshotFrameImportState,
  ledger: SnapshotFrameLedger,
): void {
  if (state.nextSequence === 0) {
    if (
      state.lastBatchStartSequence !== null ||
      state.lastBatchOffset !== null ||
      state.lastBatchFrameCount !== 0
    ) {
      throw new Error("Empty snapshot frame import records a replay batch");
    }
    return;
  }
  if (
    state.lastBatchStartSequence === null ||
    state.lastBatchOffset === null ||
    state.lastBatchFrameCount < 1 ||
    state.lastBatchStartSequence + state.lastBatchFrameCount !== state.nextSequence
  ) {
    throw new Error("Snapshot frame import replay batch cursor is invalid");
  }
  let offset = state.lastBatchOffset;
  for (let index = 0; index < state.lastBatchFrameCount; index += 1) {
    const read = ledger.read(offset, state.ledgerLength);
    if (read.record.sequence !== state.lastBatchStartSequence + index) {
      throw new Error("Snapshot frame import replay batch is not contiguous");
    }
    offset = read.nextOffset;
  }
  if (offset !== state.ledgerLength) {
    throw new Error("Snapshot frame import replay batch does not end at its durable boundary");
  }
}

function validateSnapshotFrameFooter(
  session: SnapshotFrameImportState,
  footer: FinishSnapshotFrameImportInput["footer"],
): void {
  let frameCount = 0;
  let itemCount = 0;
  let storedBytes = 0;
  for (const kind of SNAPSHOT_FRAME_KINDS) {
    const actual = session.observed[kind];
    const expected = session.header.kinds[kind];
    if (
      actual.frameCount !== expected.frameCount ||
      actual.itemCount !== expected.itemCount ||
      actual.storedBytes !== expected.storedBytes
    ) {
      throw new Error(`Snapshot ${kind} generation is incomplete`);
    }
    frameCount = safeByteSum(frameCount, actual.frameCount, "Snapshot footer frame count");
    itemCount = safeByteSum(itemCount, actual.itemCount, "Snapshot footer item count");
    storedBytes = safeByteSum(storedBytes, actual.storedBytes, "Snapshot footer stored bytes");
  }
  if (
    footer.frameCount !== frameCount ||
    footer.itemCount !== itemCount ||
    footer.storedBytes !== storedBytes ||
    footer.checksum !== session.checksum
  ) {
    throw new Error("Snapshot footer does not match its staged generation");
  }
}

function placementKey(placement: Placement): string {
  return `${String(placement.extent)}:${String(placement.offset)}:${String(placement.length)}:${String(placement.checksum)}`;
}

function uniquePlacements(placements: readonly Placement[]): Placement[] {
  return [...new Map(placements.map((placement) => [placementKey(placement), placement])).values()];
}

function corruption(location: string, message: string, cause?: unknown): StorageCorruptionError {
  const error = new StorageCorruptionError("opfs", location, message);
  if (cause !== undefined) Object.defineProperty(error, "cause", { value: cause });
  return error;
}

function errorMessage(error: unknown): string | undefined {
  if (error === undefined) return undefined;
  if (error instanceof Error) return error.message;
  return typeof error === "string" ? error : "Unknown storage maintenance failure";
}

const EMPTY_BYTES = new Uint8Array(0);

function EMPTY_CORE_STATE(): RecordCoreState {
  return {
    currentVersion: null,
    catalogEpoch: 0,
    schemaEpoch: 0,
    manifests: [],
    manifestBlocks: [],
    transactions: [],
    tables: [],
    segments: [],
    leases: [],
    compactionJobs: [],
    garbageCollectionJobs: [],
    nextRowIds: [],
    nextAutoIncrement: [],
    ftsBases: [],
    ftsDeltas: [],
    uniqueKeys: [],
    uniqueKeyBuilds: [],
    tempOwners: [],
  };
}

function tempPagePath(ownerId: string, runId: string, pageIndex: number): string[] {
  return ["temp", encodeSegment(ownerId), encodeSegment(runId), String(pageIndex)];
}

function parseTempPageFilePath(
  path: readonly string[],
): { ownerId: string; runId: string; pageIndex: number } | undefined {
  if (path.length !== 3) return undefined;
  const encodedOwner = path[0];
  const encodedRun = path[1];
  const encodedPage = path[2];
  if (encodedOwner === undefined || encodedRun === undefined || encodedPage === undefined) {
    return undefined;
  }
  try {
    const ownerId = decodeSegment(encodedOwner);
    const runId = decodeSegment(encodedRun);
    if (encodeSegment(ownerId) !== encodedOwner || encodeSegment(runId) !== encodedRun) {
      return undefined;
    }
    if (!/^(?:0|[1-9][0-9]*)$/.test(encodedPage)) return undefined;
    const pageIndex = Number(encodedPage);
    validateTempRunPageIdentity(ownerId, runId, pageIndex);
    if (String(pageIndex) !== encodedPage) return undefined;
    return { ownerId, runId, pageIndex };
  } catch {
    return undefined;
  }
}

function placementsOf(entry: WalEntry): Placement[] {
  switch (entry.op) {
    case "stageTransactionArtifacts":
    case "writeTransaction":
      return entry.blocks.map(({ placement }) => placement);
    case "writeFtsBase":
      return entry.pointer.chunks;
    case "writeFtsBaseBuildChunk":
      return [entry.placement];
    case "appendSnapshotImportFrames":
      return entry.replay ? [] : entry.blockPlacements.map(({ placement }) => placement);
    case "relocatePayloads":
      return [
        ...entry.blocks.map(({ placement }) => placement),
        ...entry.ftsChunks.map(({ placement }) => placement),
        ...entry.ftsBuildChunks.map(({ placement }) => placement),
      ];
    default:
      return [];
  }
}

function samePlacement(left: Placement | undefined, right: Placement): boolean {
  return (
    left?.extent === right.extent &&
    left.offset === right.offset &&
    left.length === right.length &&
    left.checksum === right.checksum
  );
}

/** Conservative retained-heap model for one decoded postings chunk and its nested arrays. */
function modeledFtsChunkBytes(chunk: readonly FtsPosting[]): number {
  let bytes = 64;
  for (const posting of chunk) {
    bytes += 64 + posting.term.length * 2 + posting.rowIds.length * 40;
    if (!Number.isSafeInteger(bytes)) return Number.MAX_SAFE_INTEGER;
  }
  return bytes;
}

function encodeFtsChunk(chunk: FtsPosting[]): Uint8Array {
  const bytes = encodePostingChunk(chunk);
  if (bytes.byteLength > MAX_STORED_BLOCK_BYTE_LENGTH) {
    throw new RangeError("Full-text chunk exceeds the stored-byte limit");
  }
  return bytes;
}

function decodeFtsChunk(bytes: Uint8Array): FtsPosting[] {
  const chunk = decodePostingChunk(bytes);
  if (chunk === undefined) throw new Error("Full-text base chunk is unreadable");
  return chunk;
}

/** Extent CRC is only the outer transport checksum; validate the immutable payload too. */
function verifyFtsChunkBytes(
  bytes: Uint8Array,
  expectedBounds: { first: string; last: string } | undefined,
  label: string,
): FtsPosting[] {
  if (expectedBounds === undefined) throw new Error(`${label} has no checkpointed bounds`);
  const chunk = decodeFtsChunk(bytes);
  validateFtsPostingChunks([chunk], label);
  const actualFirst = chunk[0]?.term ?? "";
  const actualLast = chunk.at(-1)?.term ?? "";
  if (actualFirst !== expectedBounds.first || actualLast !== expectedBounds.last) {
    throw new Error(`${label} terms do not match checkpointed bounds`);
  }
  return chunk;
}

/** The ordinals of base chunks whose term range can contain any of the query's terms. */
function selectFtsChunks(
  chunkBounds: ReadonlyArray<{ first: string; last: string }>,
  terms: readonly FtsPostingQuery[],
): number[] {
  const ordinals: number[] = [];
  for (const [ordinal, bounds] of chunkBounds.entries()) {
    const needed = terms.some((query) => {
      const lower = "term" in query ? query.term : query.lower;
      const upper = "term" in query ? (query.prefix ? `${query.term}￿` : query.term) : query.upper;
      return (
        (lower === undefined || lower <= bounds.last) &&
        (upper === undefined || upper >= bounds.first)
      );
    });
    if (needed) ordinals.push(ordinal);
  }
  return ordinals;
}
