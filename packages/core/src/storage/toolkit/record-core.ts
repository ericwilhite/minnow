import {
  type BeginTransactionInput,
  type BeginTransactionResult,
  type AbortTransactionIfExpiredInput,
  type AdoptAbortedSegmentInput,
  type RenewTransactionInput,
  type CommitTransactionInput,
  type CompactionJobRecord,
  CompactionBacklogError,
  CompactionJobConflictError,
  assertCompactionOutputProvenance,
  compactionOutputSegmentIds,
  type CompactionJobRecordUpdate,
  createManifest,
  type CreateGarbageCollectionJobInput,
  type UpdateGarbageCollectionPlanningInput,
  type DropTableColumnInput,
  type DropTableInput,
  createGarbageCollectionJobRecord,
  type CatalogProbe,
  advanceGarbageCollectionJobRecord,
  collectFtsCandidates,
  collectFtsPostingsBounded,
  activePostingStorageColumnIds,
  invalidateUncoveredFtsColumns,
  invalidateUncoveredSecondaryIndexes,
  type FtsCandidates,
  type FtsChanges,
  type FtsPostingQuery,
  type FtsPosting,
  type GarbageCollectionJobRecord,
  GarbageCollectionJobConflictError,
  type GarbageCollectionStepResult,
  type LeaseRecord,
  LeaseConflictError,
  LeaseExpiredError,
  LeaseOwnerConflictError,
  MAX_LEASE_TTL_MS,
  MAX_ACTIVE_LEASES,
  MAX_ACTIVE_TEMP_OWNERS,
  MAX_ACTIVE_COMPACTION_JOBS,
  MAX_ACTIVE_UNIQUE_KEY_BUILDS,
  MAX_ACTIVE_TRANSACTIONS,
  MAX_TERMINAL_TRANSACTION_RECORDS,
  MAX_TERMINAL_COMPACTION_JOB_RECORDS,
  MAX_COMPLETED_GARBAGE_COLLECTION_JOB_RECORDS,
  MAX_PINNED_MANIFEST_VERSION_LAG,
  MAX_PINNED_RETIRED_BLOCKS,
  MAX_PINNED_RETIRED_BYTES,
  MAX_RETIRED_HISTORY_BYTES,
  MAX_GLOBAL_STAGED_ARTIFACT_BYTES,
  MAX_GLOBAL_STAGED_BLOCKS,
  MAX_GLOBAL_STAGED_SEGMENTS,
  MAX_TEMP_OWNER_TTL_MS,
  type MoveLeaseInput,
  type RenewLeaseInput,
  type RenewTempOwnerInput,
  type Manifest,
  type ManifestBlockRecord,
  type ListManifestBlockPageInput,
  type ListRetiredManifestBlockPageInput,
  type ManifestBlockPage,
  type ManifestSummary,
  type RowIdRange,
  MAX_ROW_ID,
  MAX_ROW_ID_EXCLUSIVE_END,
  MAX_AUTO_INCREMENT_EXCLUSIVE_END,
  MAX_LEVEL_ZERO_SEGMENTS,
  MAX_MANIFEST_BLOCK_PRESENCE_IDS,
  MAX_STORAGE_ID_CHARACTERS,
  MAX_CATALOG_RECORDS,
  MAX_CATALOG_RETAINED_BYTES,
  MAX_MANIFEST_RECORDS,
  MAX_MANIFEST_RETAINED_BYTES,
  MAX_SEGMENT_RECORDS,
  MAX_SEGMENT_RETAINED_BYTES,
  MAX_FTS_CANDIDATE_ROW_IDS,
  MAX_FTS_POSTINGS_PER_CHUNK,
  MAX_FTS_POSTING_ROW_IDS_PER_CHUNK,
  MAX_FTS_POSTING_TERM_CHARACTERS,
  MAX_FTS_TOKENS_PER_DOCUMENT,
  MAX_FTS_BASE_CHUNKS,
  MAX_FTS_DELTA_CHUNKS,
  type RunGarbageCollectionStepInput,
  type SegmentRecord,
  SnapshotManifestMissingError,
  StorageResourceLimitError,
  type RollbackTransactionArtifactsInput,
  type StageTransactionArtifactsInput,
  type StoragePage,
  type CatalogMutationOptions,
  type TableRecord,
  type TableRecordUpdate,
  type SecondaryIndexRecord,
  TableInUseError,
  TableRecordConflictError,
  type TempOwnerRecord,
  TempOwnerConflictError,
  type TempRunPage,
  type TransactionRecord,
  TransactionRecordConflictError,
  type TransactionRecordUpdate,
  type UniqueKeyBuildRecord,
  type BeginUniqueKeyBuildInput,
  type RenewUniqueKeyBuildInput,
  type AppendUniqueKeyBuildChunkInput,
  type FinishUniqueKeyBuildInput,
  type AbortUniqueKeyBuildInput,
  UniqueKeyConflictError,
  UniqueKeyBuildConflictError,
  UniqueIndexCoverageError,
  type WriteTransactionInput,
  assertTransactionArtifactBatchLimits,
  assertTransactionArtifactJournalLimits,
  assertTempRunPageBatchLimits,
  assertStorageBulkReadItems,
  boundedMaintenanceBatchItems,
  normalizeCompactionJobRecord,
  normalizeGarbageCollectionJobRecord,
  normalizeSegmentRecord,
  updateCompactionJobRecord,
  updateTransactionRecord,
  updateGarbageCollectionPlanningRecord,
  validateTableColumns,
  validateSecondaryIndexes,
  secondaryIndexColumnIds,
  secondaryIndexWriteContractChanged,
  secondaryUniqueKeyNamespace,
  transactionCommitDeltaRetainedBytes,
  uniqueKeyBuildChunkRetainedBytes,
  MAX_UNIQUE_KEY_BUILD_TTL_MS,
  MAX_UNIQUE_KEY_BUILD_STAGED_BYTES,
  MAX_UNIQUE_KEY_BUILD_STAGED_BYTES_TOTAL,
  MAX_UNIQUE_KEY_BUILD_TOKENS_PER_CHUNK,
  type SnapshotCatalogItem,
  type SnapshotMetadataItem,
  type SnapshotFrameStreamHeader,
  type SnapshotPostingGenerationItem,
  type SnapshotPostingItem,
  type SnapshotSegmentItem,
  type SnapshotTransactionItem,
  type SnapshotUniqueItem,
  validateFtsPostingQueries,
  validateCanonicalManifestChangedTableIds,
  catalogRecordRetainedBytes,
  manifestRecordRetainedReservationBytes,
  segmentRecordRetainedBytes,
  snapshotAcceleratorItemRetainedUsage,
  assertSnapshotImportAcceleratorUsage,
  SchemaConflictError,
  WriteConflictError,
} from "../types.js";
import {
  assertWellFormedString,
  crc32,
  MAX_STORED_BLOCK_BYTE_LENGTH,
} from "../../block-format/index.js";

const postingTextEncoder = new TextEncoder();
const RESOURCE_EXPIRY_SWEEP_ITEMS = 64;
/** Leaves ample canonical-wire overhead below the 4 MiB metadata frame ceiling. */
const SNAPSHOT_UNIQUE_TOKENS_PER_CHUNK = Math.min(MAX_UNIQUE_KEY_BUILD_TOKENS_PER_CHUNK, 1_024);
const SNAPSHOT_POSTING_TERMS_PER_CHUNK = 128;

/**
 * How the record core sees block bytes it does not hold. The core owns every small record —
 * catalog, manifests, transactions, leases, jobs, counters, keys — but block payloads live
 * wherever the enclosing store keeps them (a Map in memory, files on OPFS), so existence and
 * size checks go through this seam.
 */
export interface PhysicalBlocks {
  hasBlock(id: string): boolean;
  /** `undefined` means the block does not exist. */
  blockByteLength(id: string): number | undefined;
  /** Exact stored-payload CRC when the adapter persists manifest provenance. */
  blockChecksum?(id: string): number | undefined;
}

type OrderedKey = string | number;

/**
 * A small chunked ordered index. Random UUID insertion only shifts one bounded chunk, while a
 * cursor seek binary-searches chunks and keys before walking the requested page sequentially.
 */
/** @internal Exported only so the storage hardening suite can assert operation counts. */
export class OrderedKeyIndex<Key> {
  static readonly CHUNK_SIZE = 128;
  readonly #compare: (left: Key, right: Key) => number;
  readonly #chunks: Key[][] = [];

  constructor(compare: (left: Key, right: Key) => number) {
    this.#compare = compare;
  }

  clear(): void {
    this.#chunks.length = 0;
  }

  get empty(): boolean {
    return this.#chunks.length === 0;
  }

  add(key: Key): void {
    const chunkIndex = this.#firstChunkEndingAtOrAfter(key);
    if (chunkIndex === this.#chunks.length) {
      const tail = this.#chunks.at(-1);
      if (tail === undefined || tail.length >= OrderedKeyIndex.CHUNK_SIZE) {
        this.#chunks.push([key]);
      } else {
        tail.push(key);
      }
      return;
    }
    const chunk = this.#chunks[chunkIndex];
    if (chunk === undefined) return;
    const keyIndex = this.#lowerBound(chunk, key);
    if (keyIndex < chunk.length && this.#compare(chunk[keyIndex] as Key, key) === 0) return;
    chunk.splice(keyIndex, 0, key);
    if (chunk.length > OrderedKeyIndex.CHUNK_SIZE) {
      const split = chunk.splice(OrderedKeyIndex.CHUNK_SIZE >>> 1);
      this.#chunks.splice(chunkIndex + 1, 0, split);
    }
  }

  delete(key: Key): void {
    const chunkIndex = this.#firstChunkEndingAtOrAfter(key);
    const chunk = this.#chunks[chunkIndex];
    if (chunk === undefined) return;
    const keyIndex = this.#lowerBound(chunk, key);
    if (keyIndex >= chunk.length || this.#compare(chunk[keyIndex] as Key, key) !== 0) return;
    chunk.splice(keyIndex, 1);
    if (chunk.length === 0) {
      this.#chunks.splice(chunkIndex, 1);
      return;
    }
    const next = this.#chunks[chunkIndex + 1];
    if (next !== undefined && chunk.length + next.length <= OrderedKeyIndex.CHUNK_SIZE) {
      chunk.push(...next);
      this.#chunks.splice(chunkIndex + 1, 1);
      return;
    }
    const previous = this.#chunks[chunkIndex - 1];
    if (previous !== undefined && previous.length + chunk.length <= OrderedKeyIndex.CHUNK_SIZE) {
      previous.push(...chunk);
      this.#chunks.splice(chunkIndex, 1);
    }
  }

  *after(after: Key | null): IterableIterator<Key> {
    let chunkIndex = after === null ? 0 : this.#firstChunkEndingAtOrAfter(after);
    let keyIndex = 0;
    if (after !== null) {
      const chunk = this.#chunks[chunkIndex];
      if (chunk === undefined) return;
      keyIndex = this.#upperBound(chunk, after);
    }
    while (chunkIndex < this.#chunks.length) {
      const chunk = this.#chunks[chunkIndex];
      if (chunk !== undefined) {
        while (keyIndex < chunk.length) yield chunk[keyIndex++] as Key;
      }
      chunkIndex += 1;
      keyIndex = 0;
    }
  }

  #firstChunkEndingAtOrAfter(key: Key): number {
    let low = 0;
    let high = this.#chunks.length;
    while (low < high) {
      const middle = (low + high) >>> 1;
      const last = this.#chunks[middle]?.at(-1);
      if (last !== undefined && this.#compare(last, key) < 0) low = middle + 1;
      else high = middle;
    }
    return low;
  }

  #lowerBound(chunk: readonly Key[], key: Key): number {
    let low = 0;
    let high = chunk.length;
    while (low < high) {
      const middle = (low + high) >>> 1;
      if (this.#compare(chunk[middle] as Key, key) < 0) low = middle + 1;
      else high = middle;
    }
    return low;
  }

  #upperBound(chunk: readonly Key[], key: Key): number {
    let low = 0;
    let high = chunk.length;
    while (low < high) {
      const middle = (low + high) >>> 1;
      if (this.#compare(chunk[middle] as Key, key) <= 0) low = middle + 1;
      else high = middle;
    }
    return low;
  }
}

/** Membership lookup plus stable lexical streaming without sorting on every bulk write. */
/** @internal Exported only so storage hardening tests can exercise lazy-index transitions. */
export class OrderedStringSet extends Set<string> {
  readonly #index = new OrderedKeyIndex<string>(compareOrderedKeys);
  #indexCurrent = true;

  constructor(values?: Iterable<string>) {
    super();
    if (values !== undefined) this.addMany(values);
  }

  override add(value: string): this {
    if (!this.has(value) && this.#indexCurrent) this.#index.add(value);
    return super.add(value);
  }

  /**
   * Adds a commit delta without maintaining snapshot order token-by-token. Membership remains
   * eager and exact; the snapshot-only index is rebuilt once, deterministically, if ordered
   * iteration is later requested. Clearing a stale index also releases its duplicate references.
   */
  addMany(values: Iterable<string>): this {
    let changed = false;
    for (const value of values) {
      if (this.has(value)) continue;
      super.add(value);
      changed = true;
    }
    if (changed && this.#indexCurrent) {
      this.#index.clear();
      this.#indexCurrent = false;
    }
    return this;
  }

  override delete(value: string): boolean {
    const deleted = super.delete(value);
    if (deleted && this.#indexCurrent) this.#index.delete(value);
    return deleted;
  }

  override clear(): void {
    super.clear();
    this.#index.clear();
    this.#indexCurrent = true;
  }

  orderedValues(): IterableIterator<string> {
    if (!this.#indexCurrent) {
      const ordered = [...this].sort(compareOrderedKeys);
      this.#index.clear();
      for (const value of ordered) this.#index.add(value);
      this.#indexCurrent = true;
    }
    return this.#index.after(null);
  }
}

const compareOrderedKeys = <Key extends OrderedKey>(left: Key, right: Key): number =>
  left < right ? -1 : left > right ? 1 : 0;

class OrderedRecordMap<Key extends OrderedKey, Value> extends Map<Key, Value> {
  readonly #index = new OrderedKeyIndex<Key>(compareOrderedKeys);
  readonly #onChange: ((previous: Value | undefined, next: Value | undefined) => void) | undefined;

  constructor(onChange?: (previous: Value | undefined, next: Value | undefined) => void) {
    super();
    this.#onChange = onChange;
  }

  override set(key: Key, value: Value): this {
    const previous = this.get(key);
    if (!this.has(key)) this.#index.add(key);
    super.set(key, value);
    this.#onChange?.(previous, value);
    return this;
  }

  override delete(key: Key): boolean {
    const previous = this.get(key);
    const deleted = super.delete(key);
    if (deleted) {
      this.#index.delete(key);
      this.#onChange?.(previous, undefined);
    }
    return deleted;
  }

  override clear(): void {
    const previous = this.#onChange === undefined ? [] : [...this.values()];
    super.clear();
    this.#index.clear();
    for (const value of previous) this.#onChange?.(value, undefined);
  }

  *orderedValues(after: Key | null): IterableIterator<Value> {
    for (const key of this.#index.after(after)) {
      const value = this.get(key);
      if (value !== undefined) yield value;
    }
  }
}

type ExpiryKey = readonly [expiresAt: string, id: string];
type RetiredManifestKey = readonly [addedVersion: number, blockId: string];

const compareExpiryKeys = (left: ExpiryKey, right: ExpiryKey): number =>
  left[0] < right[0]
    ? -1
    : left[0] > right[0]
      ? 1
      : left[1] < right[1]
        ? -1
        : left[1] > right[1]
          ? 1
          : 0;

const compareRetiredManifestKeys = (left: RetiredManifestKey, right: RetiredManifestKey): number =>
  left[0] - right[0] || (left[1] < right[1] ? -1 : left[1] > right[1] ? 1 : 0);

class ExpiringRecordMap<Value extends { expiresAt: string }> extends OrderedRecordMap<
  string,
  Value
> {
  readonly #expiryIndex = new OrderedKeyIndex<ExpiryKey>(compareExpiryKeys);
  readonly #expiryKeys = new Map<string, ExpiryKey>();

  override set(id: string, value: Value): this {
    const previous = this.#expiryKeys.get(id);
    if (previous !== undefined) this.#expiryIndex.delete(previous);
    const key: ExpiryKey = [value.expiresAt, id];
    this.#expiryKeys.set(id, key);
    this.#expiryIndex.add(key);
    return super.set(id, value);
  }

  override delete(id: string): boolean {
    const deleted = super.delete(id);
    if (deleted) {
      const key = this.#expiryKeys.get(id);
      if (key !== undefined) this.#expiryIndex.delete(key);
      this.#expiryKeys.delete(id);
    }
    return deleted;
  }

  override clear(): void {
    super.clear();
    this.#expiryIndex.clear();
    this.#expiryKeys.clear();
  }

  *expiringValues(after: ExpiryKey | null, cutoff: string): IterableIterator<Value> {
    for (const key of this.#expiryIndex.after(after)) {
      if (key[0] > cutoff) break;
      const value = this.get(key[1]);
      if (value !== undefined) yield value;
    }
  }
}

/** @internal Exported only so the storage hardening suite can exercise table-local traversal. */
export class SegmentRecordMap extends OrderedRecordMap<string, SegmentRecord> {
  readonly #byTable = new Map<string, OrderedKeyIndex<string>>();
  readonly #byBlock = new Map<string, Set<string>>();
  readonly #byOwner = new Map<string, Set<string>>();
  readonly #blockReferences = new Map<string, number>();
  readonly #tableBlockReferences = new Map<string, Map<string, number>>();

  override set(id: string, record: SegmentRecord): this {
    const previous = this.get(id);
    if (previous !== undefined) this.#removeReferences(previous);
    if (previous?.tableId !== record.tableId) {
      if (previous !== undefined) this.#removeFromTable(previous.tableId, id);
      let index = this.#byTable.get(record.tableId);
      if (index === undefined) {
        index = new OrderedKeyIndex(compareOrderedKeys);
        this.#byTable.set(record.tableId, index);
      }
      index.add(id);
    }
    this.#addReferences(record);
    return super.set(id, record);
  }

  override delete(id: string): boolean {
    const record = this.get(id);
    const deleted = super.delete(id);
    if (deleted && record !== undefined) {
      this.#removeFromTable(record.tableId, id);
      this.#removeReferences(record);
    }
    return deleted;
  }

  override clear(): void {
    super.clear();
    this.#byTable.clear();
    this.#byBlock.clear();
    this.#byOwner.clear();
    this.#blockReferences.clear();
    this.#tableBlockReferences.clear();
  }

  *tableValues(tableId: string): IterableIterator<SegmentRecord> {
    for (const id of this.#byTable.get(tableId)?.after(null) ?? []) {
      const record = this.get(id);
      if (record !== undefined) yield record;
    }
  }

  blockReferenceCount(blockId: string): number {
    return this.#blockReferences.get(blockId) ?? 0;
  }

  tableBlockReferenceCount(tableId: string, blockId: string): number {
    return this.#tableBlockReferences.get(tableId)?.get(blockId) ?? 0;
  }

  segmentIdsForBlock(blockId: string): ReadonlySet<string> {
    return this.#byBlock.get(blockId) ?? EMPTY_STRING_SET;
  }

  ownerReferenceCount(transactionId: string): number {
    return this.#byOwner.get(transactionId)?.size ?? 0;
  }

  #removeFromTable(tableId: string, id: string): void {
    const index = this.#byTable.get(tableId);
    index?.delete(id);
    if (index?.empty === true) this.#byTable.delete(tableId);
  }

  #addReferences(record: SegmentRecord): void {
    addSetReference(this.#byOwner, record.transactionId, record.id);
    let table = this.#tableBlockReferences.get(record.tableId);
    if (table === undefined) {
      table = new Map();
      this.#tableBlockReferences.set(record.tableId, table);
    }
    for (const blockId of segmentBlockIds(record)) {
      updateReferenceCount(this.#blockReferences, blockId, 1);
      updateReferenceCount(table, blockId, 1);
      addSetReference(this.#byBlock, blockId, record.id);
    }
  }

  #removeReferences(record: SegmentRecord): void {
    deleteSetReference(this.#byOwner, record.transactionId, record.id);
    const table = this.#tableBlockReferences.get(record.tableId);
    for (const blockId of segmentBlockIds(record)) {
      updateReferenceCount(this.#blockReferences, blockId, -1);
      if (table !== undefined) updateReferenceCount(table, blockId, -1);
      deleteSetReference(this.#byBlock, blockId, record.id);
    }
    if (table?.size === 0) this.#tableBlockReferences.delete(record.tableId);
  }
}

const EMPTY_STRING_SET: ReadonlySet<string> = new Set();

function updateReferenceCount<Key>(counts: Map<Key, number>, id: Key, delta: number): void {
  const next = (counts.get(id) ?? 0) + delta;
  if (next < 0) throw new Error(`Reference count underflow: ${String(id)}`);
  if (next === 0) counts.delete(id);
  else counts.set(id, next);
}

function addSetReference(
  references: Map<string, Set<string>>,
  namespace: string,
  id: string,
): void {
  let values = references.get(namespace);
  if (values === undefined) {
    values = new Set();
    references.set(namespace, values);
  }
  values.add(id);
}

function deleteSetReference(
  references: Map<string, Set<string>>,
  namespace: string,
  id: string,
): void {
  const values = references.get(namespace);
  values?.delete(id);
  if (values?.size === 0) references.delete(namespace);
}

class RecordRootIndex {
  readonly readableManifestVersions = new OrderedKeyIndex<number>(compareOrderedKeys);
  readonly prunedManifestVersions = new OrderedKeyIndex<number>(compareOrderedKeys);
  readonly #transactionBlocks = new Map<string, number>();
  readonly #abortedTransactionBlocks = new Map<string, number>();
  readonly #activeTransactionBlocks = new Map<string, number>();
  readonly #transactionSegments = new Map<string, number>();
  readonly #activeTransactionSegments = new Map<string, number>();
  readonly #activeSnapshotVersions = new Map<number, number>();
  readonly #activeTransactionIds = new Set<string>();
  readonly #allJobBlocks = new Map<string, number>();
  readonly #terminalJobBlocks = new Map<string, number>();
  readonly #liveJobBlocks = new Map<string, number>();
  readonly #allJobSegments = new Map<string, number>();
  readonly #liveJobSegments = new Map<string, number>();
  readonly #liveJobSourceVersions = new Map<number, number>();
  readonly #liveJobTransactions = new Map<string, number>();
  readonly #linkedCommittedVersions = new Map<number, number>();
  readonly #garbageBlocks = new Map<string, number>();
  readonly #garbageSegments = new Map<string, number>();
  readonly #leaseExpiries = new Map<number, Map<string, string>>();
  readonly #maximumLeaseExpiry = new Map<number, string>();

  changeManifest(record: Manifest, delta: 1 | -1): void {
    const index =
      record.prunedAt === undefined ? this.readableManifestVersions : this.prunedManifestVersions;
    if (delta === 1) index.add(record.version);
    else index.delete(record.version);
  }

  changeTransaction(record: TransactionRecord, delta: 1 | -1): void {
    this.#change(this.#transactionBlocks, record.pendingBlockIds, delta);
    this.#change(this.#transactionSegments, record.pendingSegmentIds, delta);
    if (record.status === "aborted") {
      this.#change(this.#abortedTransactionBlocks, record.pendingBlockIds, delta);
    }
    if (record.status !== "active") return;
    this.#change(this.#activeTransactionBlocks, record.pendingBlockIds, delta);
    this.#change(this.#activeTransactionSegments, record.pendingSegmentIds, delta);
    if (record.snapshotVersion !== null) {
      updateReferenceCount(this.#activeSnapshotVersions, record.snapshotVersion, delta);
    }
    if (delta === 1) this.#activeTransactionIds.add(record.id);
    else this.#activeTransactionIds.delete(record.id);
  }

  changeLease(record: LeaseRecord, delta: 1 | -1): void {
    if (record.manifestVersion === null) return;
    let expiries = this.#leaseExpiries.get(record.manifestVersion);
    if (delta === 1) {
      if (expiries === undefined) {
        expiries = new Map();
        this.#leaseExpiries.set(record.manifestVersion, expiries);
      }
      expiries.set(record.id, record.expiresAt);
      const maximum = this.#maximumLeaseExpiry.get(record.manifestVersion);
      if (maximum === undefined || record.expiresAt > maximum) {
        this.#maximumLeaseExpiry.set(record.manifestVersion, record.expiresAt);
      }
      return;
    }
    expiries?.delete(record.id);
    if (expiries?.size === 0) {
      this.#leaseExpiries.delete(record.manifestVersion);
      this.#maximumLeaseExpiry.delete(record.manifestVersion);
    } else if (this.#maximumLeaseExpiry.get(record.manifestVersion) === record.expiresAt) {
      let maximum = "";
      for (const expiry of expiries?.values() ?? []) if (expiry > maximum) maximum = expiry;
      this.#maximumLeaseExpiry.set(record.manifestVersion, maximum);
    }
  }

  changeCompactionJob(record: CompactionJobRecord, delta: 1 | -1): void {
    const sourceBlocks = Array.isArray(record.sourceBlockIds) ? record.sourceBlockIds : [];
    const outputBlocks = Array.isArray(record.outputBlockIds) ? record.outputBlockIds : [];
    const sourceSegments = Array.isArray(record.sourceSegmentIds) ? record.sourceSegmentIds : [];
    const blocks = [...sourceBlocks, ...outputBlocks];
    const segments = [...sourceSegments, ...compactionOutputSegmentIds(record)];
    this.#change(this.#allJobBlocks, blocks, delta);
    this.#change(this.#allJobSegments, segments, delta);
    if (isTerminalCompactionJob(record)) {
      this.#change(this.#terminalJobBlocks, blocks, delta);
      return;
    }
    this.#change(this.#liveJobBlocks, blocks, delta);
    this.#change(this.#liveJobSegments, segments, delta);
    updateReferenceCount(this.#liveJobSourceVersions, record.sourceManifestVersion, delta);
    if (record.transactionId !== null) {
      updateReferenceCount(this.#liveJobTransactions, record.transactionId, delta);
    }
  }

  changeGarbageCollectionJob(record: GarbageCollectionJobRecord, delta: 1 | -1): void {
    this.#change(this.#garbageBlocks, record.candidateBlockIds, delta);
    this.#change(this.#garbageSegments, record.candidateSegmentIds, delta);
  }

  changeLinkedCommittedVersion(version: number | null, delta: number): void {
    if (version !== null) updateReferenceCount(this.#linkedCommittedVersions, version, delta);
  }

  liveJobTransactionCount(id: string): number {
    return this.#liveJobTransactions.get(id) ?? 0;
  }

  activeOwner(id: string): boolean {
    return this.#activeTransactionIds.has(id);
  }

  transactionBlockCount(id: string): number {
    return this.#transactionBlocks.get(id) ?? 0;
  }

  abortedTransactionBlockCount(id: string): number {
    return this.#abortedTransactionBlocks.get(id) ?? 0;
  }

  activeTransactionBlockCount(id: string): number {
    return this.#activeTransactionBlocks.get(id) ?? 0;
  }

  transactionSegmentCount(id: string): number {
    return this.#transactionSegments.get(id) ?? 0;
  }

  activeTransactionSegmentCount(id: string): number {
    return this.#activeTransactionSegments.get(id) ?? 0;
  }

  allJobBlockCount(id: string): number {
    return this.#allJobBlocks.get(id) ?? 0;
  }

  terminalJobBlockCount(id: string): number {
    return this.#terminalJobBlocks.get(id) ?? 0;
  }

  liveJobBlockCount(id: string): number {
    return this.#liveJobBlocks.get(id) ?? 0;
  }

  allJobSegmentCount(id: string): number {
    return this.#allJobSegments.get(id) ?? 0;
  }

  liveJobSegmentCount(id: string): number {
    return this.#liveJobSegments.get(id) ?? 0;
  }

  garbageBlockCount(id: string): number {
    return this.#garbageBlocks.get(id) ?? 0;
  }

  garbageSegmentCount(id: string): number {
    return this.#garbageSegments.get(id) ?? 0;
  }

  isManifestPinned(version: number, currentVersion: number | null, cutoff: string): boolean {
    return (
      currentVersion === version ||
      (this.#activeSnapshotVersions.get(version) ?? 0) > 0 ||
      (this.#liveJobSourceVersions.get(version) ?? 0) > 0 ||
      (this.#linkedCommittedVersions.get(version) ?? 0) > 0 ||
      (this.#maximumLeaseExpiry.get(version) ?? "") > cutoff
    );
  }

  minimumPinnedVersion(
    cutoff: string,
    closingActiveSnapshotVersion?: number | null,
  ): number | undefined {
    let minimum: number | undefined;
    const include = (versions: Iterable<number>): void => {
      for (const version of versions) {
        if (minimum === undefined || version < minimum) minimum = version;
      }
    };
    for (const [version, count] of this.#activeSnapshotVersions) {
      if (version !== closingActiveSnapshotVersion || count > 1) include([version]);
    }
    include(this.#liveJobSourceVersions.keys());
    include(this.#linkedCommittedVersions.keys());
    for (const [version, expiresAt] of this.#maximumLeaseExpiry) {
      if (expiresAt > cutoff && (minimum === undefined || version < minimum)) minimum = version;
    }
    return minimum;
  }

  pinnedVersions(
    cutoff: string,
    closingActiveSnapshotVersion?: number | null,
    replacingLease?: { id: string; version: number | null },
  ): number[] {
    const versions = new Set<number>();
    for (const [version, count] of this.#activeSnapshotVersions) {
      if (version !== closingActiveSnapshotVersion || count > 1) versions.add(version);
    }
    for (const version of this.#liveJobSourceVersions.keys()) versions.add(version);
    for (const version of this.#linkedCommittedVersions.keys()) versions.add(version);
    for (const [version, expiries] of this.#leaseExpiries) {
      let live = false;
      for (const [id, expiresAt] of expiries) {
        if (
          expiresAt > cutoff &&
          (replacingLease?.version !== version || replacingLease.id !== id)
        ) {
          live = true;
          break;
        }
      }
      if (live) versions.add(version);
    }
    return [...versions].sort((left, right) => left - right);
  }

  #change(counts: Map<string, number>, ids: unknown, delta: 1 | -1): void {
    if (!Array.isArray(ids)) return;
    for (const id of ids) if (typeof id === "string") updateReferenceCount(counts, id, delta);
  }
}

/** `RecordCore.dump()`'s shape; see there. Arrays rather than Maps so the value serializes. */
export interface RecordCoreState {
  currentVersion: number | null;
  catalogEpoch: number;
  schemaEpoch: number;
  manifests: Manifest[];
  manifestBlocks: ManifestBlockRecord[];
  transactions: TransactionRecord[];
  tables: TableRecord[];
  segments: SegmentRecord[];
  leases: LeaseRecord[];
  compactionJobs: CompactionJobRecord[];
  garbageCollectionJobs: GarbageCollectionJobRecord[];
  nextRowIds: Array<readonly [string, bigint]>;
  nextAutoIncrement: Array<readonly [string, bigint]>;
  ftsBases: Array<
    readonly [string, { coversVersion: number; chunks: FtsPosting[][]; totalTokens: number }]
  >;
  ftsDeltas: Array<
    readonly [string, Array<readonly [number, { postings: FtsPosting[]; totalTokens: number }]>]
  >;
  uniqueKeys: Array<readonly [string, string[]]>;
  uniqueKeyBuilds: Array<
    readonly [UniqueKeyBuildRecord, string[][], FinishUniqueKeyBuildInput | null]
  >;
  tempOwners: TempOwnerRecord[];
}

interface UniqueKeyBuildState {
  record: UniqueKeyBuildRecord;
  chunks: string[][];
  tokens: Set<string>;
  completedInput: FinishUniqueKeyBuildInput | null;
}

/** A commit validated and resolved but not yet applied: everything `#applyCommit` writes. */
interface CommitPlan {
  manifest: Manifest;
  addedManifestBlocks: ManifestBlockRecord[];
  removedManifestBlocks: ManifestBlockRecord[];
  committed: TransactionRecord;
  pendingSegments: SegmentRecord[];
  uniqueKeyDeltas: Map<string, { added: Set<string>; removed: Set<string> }>;
  ftsChanges: readonly FtsChanges[] | undefined;
  pendingTable?: TableRecord;
  pendingTableNextRowId?: bigint;
}

interface StageTransactionArtifactsPlan {
  updated: TransactionRecord;
  segments: SegmentRecord[];
}

interface WriteTransactionPlan {
  staged: TransactionRecord;
  segments: Map<string, SegmentRecord>;
  commit: CommitPlan;
}

/**
 * The synchronous record-state machine behind `MemoryBlockStore` and `OpfsBlockStore`, offered
 * to adapter authors as the semantics half of a `BlockStore`: every record family the contract
 * names, every validation order, every typed conflict error, every defensive copy — one method
 * body per contract operation, so an adapter that wraps it inherits the behaviour the
 * conformance kit checks and spends its own code purely on persistence.
 *
 * What wrapping it requires of the adapter:
 *
 * - **One writer at a time.** Methods validate then mutate synchronously; they are only safe
 *   when calls cannot interleave. Serialize access — a promise-chain queue (the memory store),
 *   a leader that is provably the sole writer (the OPFS store), a process-wide lock.
 * - **Durability is yours.** The core is memory. Record each mutation (a write-ahead-log frame
 *   of the method and its input), checkpoint with `dump()`/`load()`, and replay on open.
 * - **Block bytes are yours.** The core tracks records; the `PhysicalBlocks` seam asks you
 *   whether payload bytes exist and how large they are.
 *
 * Changing behaviour here changes every store built on it — which is the point.
 */
export class RecordCore {
  readonly #physical: PhysicalBlocks;
  readonly #roots = new RecordRootIndex();
  readonly #manifests = new OrderedRecordMap<number, Manifest>((previous, next) => {
    if (previous !== undefined) this.#roots.changeManifest(previous, -1);
    if (next !== undefined) this.#roots.changeManifest(next, 1);
  });
  #manifestRetainedBytes = 0;
  #retiredHistoryBytes = 0;
  readonly #retiredManifestVersions = new OrderedKeyIndex<number>(compareOrderedKeys);
  readonly #retiredManifestBlocksByVersion = new Map<number, OrderedKeyIndex<RetiredManifestKey>>();
  readonly #manifestBlocks = new OrderedRecordMap<string, ManifestBlockRecord>((previous, next) => {
    if (previous !== undefined && previous.removedVersion !== null) {
      this.#retiredHistoryBytes -= previous.byteLength;
      this.#changeRetiredManifestBlock(previous, -1);
    }
    if (next !== undefined && next.removedVersion !== null) {
      this.#retiredHistoryBytes += next.byteLength;
      this.#changeRetiredManifestBlock(next, 1);
    }
  });
  #activeTransactionCount = 0;
  #activeStagedBlockCount = 0;
  #activeStagedSegmentCount = 0;
  #activeStagedArtifactBytes = 0;
  #terminalTransactionCount = 0;
  #transactionBlockLengthOverrides: ReadonlyMap<string, number> | undefined;
  readonly #activeTransactionExpiries = new OrderedKeyIndex<ExpiryKey>(compareExpiryKeys);
  readonly #activeTransactionExpiryKeys = new Map<string, ExpiryKey>();
  readonly #transactions = new OrderedRecordMap<string, TransactionRecord>((previous, next) => {
    this.#changeTransactionReferences(previous, next);
  });
  readonly #triggerOwnersByName = new Map<string, { tableId: string; triggerId: string }>();
  readonly #triggerOwnersById = new Map<string, { tableId: string; triggerName: string }>();
  readonly #tables = new OrderedRecordMap<string, TableRecord>((previous, next) => {
    this.#changeTableTriggerOwnership(previous, next);
  });
  #catalogRetainedBytes = 0;
  #pendingCatalogRecords = 0;
  #pendingCatalogRetainedBytes = 0;
  readonly #tableIdsByName = new Map<string, string>();
  readonly #segments = new SegmentRecordMap();
  #segmentRetainedBytes = 0;
  readonly #leases = new ExpiringRecordMap<LeaseRecord>((previous, next) => {
    if (previous !== undefined) this.#roots.changeLease(previous, -1);
    if (next !== undefined) this.#roots.changeLease(next, 1);
  });
  #terminalCompactionJobCount = 0;
  readonly #compactionJobs = new OrderedRecordMap<string, CompactionJobRecord>((previous, next) => {
    this.#terminalCompactionJobCount +=
      Number(next !== undefined && isTerminalCompactionJob(next)) -
      Number(previous !== undefined && isTerminalCompactionJob(previous));
    this.#changeCompactionJobReferences(previous, next);
  });
  #completedGarbageCollectionJobCount = 0;
  readonly #garbageCollectionJobs = new OrderedRecordMap<string, GarbageCollectionJobRecord>(
    (previous, next) => {
      this.#completedGarbageCollectionJobCount +=
        Number(next?.state === "completed") - Number(previous?.state === "completed");
      if (previous !== undefined) this.#roots.changeGarbageCollectionJob(previous, -1);
      if (next !== undefined) this.#roots.changeGarbageCollectionJob(next, 1);
    },
  );
  readonly #nextRowIds = new Map<string, bigint>();
  readonly #nextAutoIncrement = new Map<string, bigint>();
  readonly #ftsBases = new OrderedRecordMap<
    string,
    { coversVersion: number; chunks: FtsPosting[][]; totalTokens: number }
  >();
  readonly #ftsDeltas = new OrderedRecordMap<
    string,
    Map<number, { postings: FtsPosting[]; totalTokens: number }>
  >();
  readonly #uniqueKeys = new OrderedRecordMap<string, OrderedStringSet>();
  readonly #uniqueKeyBuilds = new Map<string, UniqueKeyBuildState>();
  #activeUniqueKeyBuildCount = 0;
  #uniqueKeyBuildStagedBytes = 0;
  #uniqueKeyBuildStagedEntries = 0;
  readonly #tempOwners = new ExpiringRecordMap<TempOwnerRecord>();
  #currentVersion: number | null = null;
  /** Advances on every catalog mutation; see `CatalogProbe` for the freshness contract. */
  #catalogEpoch = 0;
  /** Advances only when a structural catalog change invalidates prepared write artifacts. */
  #schemaEpoch = 0;
  #prunedManifestRemovalCursor: number | null = null;

  constructor(physical: PhysicalBlocks) {
    this.#physical = physical;
  }

  #changeTransactionReferences(
    previous: TransactionRecord | undefined,
    next: TransactionRecord | undefined,
  ): void {
    const previousPending =
      previous?.status === "active" && previous.pendingTable !== undefined
        ? catalogRecordRetainedBytes(previous.pendingTable)
        : 0;
    const nextPending =
      next?.status === "active" && next.pendingTable !== undefined
        ? catalogRecordRetainedBytes(next.pendingTable)
        : 0;
    this.#pendingCatalogRecords += Number(nextPending > 0) - Number(previousPending > 0);
    this.#pendingCatalogRetainedBytes += nextPending - previousPending;
    this.#terminalTransactionCount +=
      Number(next !== undefined && next.status !== "active") -
      Number(previous !== undefined && previous.status !== "active");
    if (previous !== undefined) {
      const previousExpiry = this.#activeTransactionExpiryKeys.get(previous.id);
      if (previousExpiry !== undefined) this.#activeTransactionExpiries.delete(previousExpiry);
      this.#activeTransactionExpiryKeys.delete(previous.id);
    }
    if (next?.status === "active") {
      const nextExpiry: ExpiryKey = [next.expiresAt, next.id];
      this.#activeTransactionExpiries.add(nextExpiry);
      this.#activeTransactionExpiryKeys.set(next.id, nextExpiry);
    }
    const before = this.#transactionResources(previous, this.#transactionBlockLengthOverrides);
    const after = this.#transactionResources(next, this.#transactionBlockLengthOverrides);
    this.#activeTransactionCount += after.transactions - before.transactions;
    this.#activeStagedBlockCount += after.blocks - before.blocks;
    this.#activeStagedSegmentCount += after.segments - before.segments;
    this.#activeStagedArtifactBytes += after.bytes - before.bytes;
    if (previous !== undefined) {
      const linkedJobs = this.#roots.liveJobTransactionCount(previous.id);
      if (previous.status === "committed") {
        this.#roots.changeLinkedCommittedVersion(previous.committedVersion, -linkedJobs);
      }
      this.#roots.changeTransaction(previous, -1);
    }
    if (next !== undefined) {
      this.#roots.changeTransaction(next, 1);
      const linkedJobs = this.#roots.liveJobTransactionCount(next.id);
      if (next.status === "committed") {
        this.#roots.changeLinkedCommittedVersion(next.committedVersion, linkedJobs);
      }
    }
  }

  #sweepExpiredTransactions(expiresAtCutoff: string): void {
    const expiredIds: string[] = [];
    for (const [expiresAt, id] of this.#activeTransactionExpiries.after(null)) {
      if (expiresAt > expiresAtCutoff) break;
      expiredIds.push(id);
      if (expiredIds.length === RESOURCE_EXPIRY_SWEEP_ITEMS) break;
    }
    for (const id of expiredIds) {
      const current = this.#transactions.get(id);
      if (current?.status !== "active" || current.expiresAt > expiresAtCutoff) continue;
      this.#setTransaction(
        updateTransactionRecord(current, { status: "aborted", updatedAt: expiresAtCutoff }),
      );
    }
  }

  #assertPinnedManifestLag(version: number | null): void {
    if (version === null || this.#currentVersion === null) return;
    const lag = this.#currentVersion - version;
    if (lag > MAX_PINNED_MANIFEST_VERSION_LAG) {
      throw new StorageResourceLimitError(
        "pinned manifest version lag",
        lag,
        MAX_PINNED_MANIFEST_VERSION_LAG,
      );
    }
  }

  #assertRetiredHistoryAddition(records: readonly ManifestBlockRecord[]): void {
    const addedBytes = records.reduce(
      (total, record) => safeStorageSum(total, record.byteLength),
      0,
    );
    const next = safeStorageSum(this.#retiredHistoryBytes, addedBytes);
    if (next > MAX_RETIRED_HISTORY_BYTES) {
      throw new StorageResourceLimitError("retired history byte", next, MAX_RETIRED_HISTORY_BYTES);
    }
  }

  #changeRetiredManifestBlock(record: ManifestBlockRecord, delta: 1 | -1): void {
    const removedVersion = record.removedVersion;
    if (removedVersion === null) return;
    let index = this.#retiredManifestBlocksByVersion.get(removedVersion);
    if (delta === 1) {
      if (index === undefined) {
        index = new OrderedKeyIndex<RetiredManifestKey>(compareRetiredManifestKeys);
        this.#retiredManifestBlocksByVersion.set(removedVersion, index);
        this.#retiredManifestVersions.add(removedVersion);
      }
      index.add([record.addedVersion, record.blockId]);
      return;
    }
    index?.delete([record.addedVersion, record.blockId]);
    if (index?.empty === true) {
      this.#retiredManifestBlocksByVersion.delete(removedVersion);
      this.#retiredManifestVersions.delete(removedVersion);
    }
  }

  #pinnedRetiredUsage(
    cutoff: string,
    closingActiveSnapshotVersion?: number | null,
    replacingLease?: { id: string; version: number | null },
    additionalVersions: readonly number[] = [],
    additionalRetired: readonly ManifestBlockRecord[] = [],
  ): { blocks: number; bytes: number } {
    const pins = this.#roots.pinnedVersions(cutoff, closingActiveSnapshotVersion, replacingLease);
    for (const version of additionalVersions) {
      const index = lowerBoundNumbers(pins, version);
      if (pins[index] !== version) pins.splice(index, 0, version);
    }
    if (pins.length === 0) return { blocks: 0, bytes: 0 };
    let blocks = 0;
    let bytes = 0;
    const minimumPin = pins[0];
    if (minimumPin === undefined) return { blocks, bytes };
    for (const removedVersion of this.#retiredManifestVersions.after(minimumPin)) {
      const pinIndex = lowerBoundNumbers(pins, removedVersion) - 1;
      if (pinIndex < 0) continue;
      const latestPin = pins[pinIndex];
      if (latestPin === undefined) continue;
      const records = this.#retiredManifestBlocksByVersion.get(removedVersion);
      if (records === undefined) continue;
      for (const [addedVersion, blockId] of records.after(null)) {
        if (addedVersion > latestPin) break;
        const record = this.#manifestBlocks.get(blockId);
        if (record === undefined) throw new Error(`Retired manifest block disappeared: ${blockId}`);
        blocks = safeWholeIncrement(blocks, "Pinned retired block count");
        bytes = safeStorageSum(bytes, record.byteLength);
        if (blocks > MAX_PINNED_RETIRED_BLOCKS || bytes > MAX_PINNED_RETIRED_BYTES) {
          return { blocks, bytes };
        }
      }
    }
    for (const record of additionalRetired) {
      const pinIndex = lowerBoundNumbers(pins, record.removedVersion ?? 0) - 1;
      const latestPin = pinIndex < 0 ? undefined : pins[pinIndex];
      if (latestPin === undefined || latestPin < record.addedVersion) continue;
      blocks = safeWholeIncrement(blocks, "Pinned retired block count");
      bytes = safeStorageSum(bytes, record.byteLength);
    }
    return { blocks, bytes };
  }

  #assertPinnedRetiredLimits(
    cutoff: string,
    closingActiveSnapshotVersion?: number | null,
    replacingLease?: { id: string; version: number | null },
    additionalVersions: readonly number[] = [],
    additionalRetired: readonly ManifestBlockRecord[] = [],
  ): void {
    const usage = this.#pinnedRetiredUsage(
      cutoff,
      closingActiveSnapshotVersion,
      replacingLease,
      additionalVersions,
      additionalRetired,
    );
    if (usage.blocks > MAX_PINNED_RETIRED_BLOCKS) {
      throw new StorageResourceLimitError(
        "pinned retired block",
        usage.blocks,
        MAX_PINNED_RETIRED_BLOCKS,
      );
    }
    if (usage.bytes > MAX_PINNED_RETIRED_BYTES) {
      throw new StorageResourceLimitError(
        "pinned retired byte",
        usage.bytes,
        MAX_PINNED_RETIRED_BYTES,
      );
    }
  }

  /** Reconstructs and validates time-sensitive pinned-retired quotas after durable recovery. */
  validatePinnedRetiredLimits(expiresAtCutoff: string): void {
    validateTimestampRuntime(expiresAtCutoff, "Pinned-retired validation cutoff");
    this.#assertPinnedRetiredLimits(expiresAtCutoff);
  }

  #assertNextManifestPinLag(
    nextVersion: number,
    cutoff: string,
    closingActiveSnapshotVersion?: number | null,
  ): void {
    const minimum = this.#roots.minimumPinnedVersion(cutoff, closingActiveSnapshotVersion);
    if (minimum === undefined) return;
    const lag = nextVersion - minimum;
    if (lag > MAX_PINNED_MANIFEST_VERSION_LAG) {
      throw new StorageResourceLimitError(
        "pinned manifest version lag",
        lag,
        MAX_PINNED_MANIFEST_VERSION_LAG,
      );
    }
  }

  #transactionResources(
    record: TransactionRecord | undefined,
    overrides?: ReadonlyMap<string, number>,
  ): { transactions: number; blocks: number; segments: number; bytes: number } {
    if (record?.status !== "active") return { transactions: 0, blocks: 0, segments: 0, bytes: 0 };
    let bytes = 0;
    for (const id of record.pendingBlockIds) {
      const length = overrides?.get(id) ?? this.#physical.blockByteLength(id);
      if (length === undefined)
        throw new Error(`Active transaction references missing block: ${id}`);
      bytes += length;
      if (!Number.isSafeInteger(bytes)) {
        throw new RangeError("Active transaction staged bytes exceed the safe integer range");
      }
    }
    return {
      transactions: 1,
      blocks: record.pendingBlockIds.length,
      segments: record.pendingSegmentIds.length,
      bytes,
    };
  }

  #setTransaction(
    record: TransactionRecord,
    blockLengthOverrides?: ReadonlyMap<string, number>,
  ): void {
    if (record.status === "active" && record.pendingTable !== undefined) {
      this.#assertTableTriggerOwnership(record.pendingTable, record.id);
    }
    this.#assertTransactionResourceTransition(record, blockLengthOverrides);
    const previous = this.#transactions.get(record.id);
    const previousPending =
      previous?.status === "active" && previous.pendingTable !== undefined
        ? catalogRecordRetainedBytes(previous.pendingTable)
        : 0;
    const nextPending =
      record.status === "active" && record.pendingTable !== undefined
        ? catalogRecordRetainedBytes(record.pendingTable)
        : 0;
    const pendingCount =
      this.#pendingCatalogRecords + Number(nextPending > 0) - Number(previousPending > 0);
    const pendingBytes = this.#pendingCatalogRetainedBytes + nextPending - previousPending;
    const totalCount = this.#tables.size + pendingCount;
    const totalBytes = this.#catalogRetainedBytes + pendingBytes;
    if (totalCount > MAX_CATALOG_RECORDS) {
      throw new StorageResourceLimitError("catalog record", totalCount, MAX_CATALOG_RECORDS);
    }
    if (totalBytes > MAX_CATALOG_RETAINED_BYTES) {
      throw new StorageResourceLimitError("catalog byte", totalBytes, MAX_CATALOG_RETAINED_BYTES);
    }
    this.#transactionBlockLengthOverrides = blockLengthOverrides;
    try {
      this.#transactions.set(record.id, record);
    } finally {
      this.#transactionBlockLengthOverrides = undefined;
    }
  }

  #catalogRetainedBytesAfter(record: TableRecord): number {
    const previous = this.#tables.get(record.id);
    const previousBytes = previous === undefined ? 0 : catalogRecordRetainedBytes(previous);
    const recordBytes = catalogRecordRetainedBytes(record);
    const retainedBytes = safeStorageSum(this.#catalogRetainedBytes - previousBytes, recordBytes);
    if (retainedBytes + this.#pendingCatalogRetainedBytes > MAX_CATALOG_RETAINED_BYTES) {
      throw new StorageResourceLimitError(
        "catalog byte",
        retainedBytes + this.#pendingCatalogRetainedBytes,
        MAX_CATALOG_RETAINED_BYTES,
      );
    }
    return retainedBytes;
  }

  #changeTableTriggerOwnership(
    previous: TableRecord | undefined,
    next: TableRecord | undefined,
  ): void {
    if (previous !== undefined) {
      for (const trigger of previous.triggers ?? []) {
        const byName = this.#triggerOwnersByName.get(trigger.name);
        if (byName?.tableId === previous.id && byName.triggerId === trigger.id) {
          this.#triggerOwnersByName.delete(trigger.name);
        }
        const byId = this.#triggerOwnersById.get(trigger.id);
        if (byId?.tableId === previous.id && byId.triggerName === trigger.name) {
          this.#triggerOwnersById.delete(trigger.id);
        }
      }
    }
    if (next !== undefined) {
      for (const trigger of next.triggers ?? []) {
        this.#triggerOwnersByName.set(trigger.name, { tableId: next.id, triggerId: trigger.id });
        this.#triggerOwnersById.set(trigger.id, { tableId: next.id, triggerName: trigger.name });
      }
    }
  }

  /** Global trigger names and immutable IDs are one atomic catalog namespace. */
  #assertTableTriggerOwnership(record: TableRecord, pendingTransactionId?: string): void {
    for (const trigger of record.triggers ?? []) {
      const byName = this.#triggerOwnersByName.get(trigger.name);
      if (
        byName !== undefined &&
        (byName.tableId !== record.id || byName.triggerId !== trigger.id)
      ) {
        throw new TypeError(`Trigger already exists: ${trigger.name}`);
      }
      const byId = this.#triggerOwnersById.get(trigger.id);
      if (byId !== undefined && (byId.tableId !== record.id || byId.triggerName !== trigger.name)) {
        throw new TypeError(`Trigger ID already exists: ${trigger.id}`);
      }
      for (const transaction of this.#transactions.values()) {
        if (
          transaction.id === pendingTransactionId ||
          transaction.status !== "active" ||
          transaction.pendingTable === undefined ||
          transaction.pendingTable.id === record.id
        ) {
          continue;
        }
        for (const pendingTrigger of transaction.pendingTable.triggers ?? []) {
          if (pendingTrigger.name === trigger.name) {
            throw new TypeError(`Trigger already exists: ${trigger.name}`);
          }
          if (pendingTrigger.id === trigger.id) {
            throw new TypeError(`Trigger ID already exists: ${trigger.id}`);
          }
        }
      }
    }
  }

  #setTable(record: TableRecord): void {
    this.#assertTableTriggerOwnership(record);
    const retainedBytes = this.#catalogRetainedBytesAfter(record);
    this.#tables.set(record.id, record);
    this.#catalogRetainedBytes = retainedBytes;
  }

  #deleteTable(record: TableRecord): void {
    const retainedBytes = this.#catalogRetainedBytes - catalogRecordRetainedBytes(record);
    if (retainedBytes < 0) throw new Error("Catalog retained-byte accounting underflow");
    this.#tables.delete(record.id);
    this.#catalogRetainedBytes = retainedBytes;
  }

  #manifestUsageAfter(
    records: readonly Manifest[],
    removeVersions: readonly number[] = [],
  ): { count: number; retainedBytes: number; changes: Map<number, Manifest | undefined> } {
    const changes = new Map<number, Manifest | undefined>();
    for (const version of removeVersions) changes.set(version, undefined);
    for (const record of records) changes.set(record.version, record);
    let count = this.#manifests.size;
    let retainedBytes = this.#manifestRetainedBytes;
    for (const [version, record] of changes) {
      const previous = this.#manifests.get(version);
      if (previous !== undefined) {
        count -= 1;
        retainedBytes -= manifestRecordRetainedReservationBytes(previous);
      }
      if (record !== undefined) {
        count += 1;
        retainedBytes = safeStorageSum(
          retainedBytes,
          manifestRecordRetainedReservationBytes(record),
        );
      }
    }
    if (count > MAX_MANIFEST_RECORDS) {
      throw new StorageResourceLimitError("manifest record", count, MAX_MANIFEST_RECORDS);
    }
    if (retainedBytes > MAX_MANIFEST_RETAINED_BYTES) {
      throw new StorageResourceLimitError(
        "manifest byte",
        retainedBytes,
        MAX_MANIFEST_RETAINED_BYTES,
      );
    }
    if (retainedBytes < 0) throw new Error("Manifest retained-byte accounting underflow");
    return { count, retainedBytes, changes };
  }

  #replaceManifests(records: readonly Manifest[], removeVersions: readonly number[] = []): void {
    const { retainedBytes, changes } = this.#manifestUsageAfter(records, removeVersions);
    for (const [version, record] of changes) {
      if (record === undefined) this.#manifests.delete(version);
      else this.#manifests.set(version, record);
    }
    this.#manifestRetainedBytes = retainedBytes;
  }

  #setManifest(record: Manifest): void {
    this.#replaceManifests([record]);
  }

  #segmentUsageAfter(
    records: readonly SegmentRecord[],
    removeIds: readonly string[] = [],
  ): { count: number; retainedBytes: number; changes: Map<string, SegmentRecord | undefined> } {
    const changes = new Map<string, SegmentRecord | undefined>();
    for (const id of removeIds) changes.set(id, undefined);
    for (const record of records) changes.set(record.id, record);
    let count = this.#segments.size;
    let retainedBytes = this.#segmentRetainedBytes;
    for (const [id, record] of changes) {
      const previous = this.#segments.get(id);
      if (previous !== undefined) {
        count -= 1;
        retainedBytes -= segmentRecordRetainedBytes(previous);
      }
      if (record !== undefined) {
        count += 1;
        retainedBytes = safeStorageSum(retainedBytes, segmentRecordRetainedBytes(record));
      }
    }
    if (count > MAX_SEGMENT_RECORDS) {
      throw new StorageResourceLimitError("segment record", count, MAX_SEGMENT_RECORDS);
    }
    if (retainedBytes > MAX_SEGMENT_RETAINED_BYTES) {
      throw new StorageResourceLimitError(
        "segment byte",
        retainedBytes,
        MAX_SEGMENT_RETAINED_BYTES,
      );
    }
    if (retainedBytes < 0) throw new Error("Segment retained-byte accounting underflow");
    return { count, retainedBytes, changes };
  }

  #replaceSegments(records: readonly SegmentRecord[], removeIds: readonly string[] = []): void {
    const { retainedBytes, changes } = this.#segmentUsageAfter(records, removeIds);
    for (const [id, record] of changes) {
      if (record === undefined) this.#segments.delete(id);
      else this.#segments.set(id, record);
    }
    this.#segmentRetainedBytes = retainedBytes;
  }

  #setSegment(record: SegmentRecord): void {
    this.#replaceSegments([record]);
  }

  #assertTransactionResourceTransition(
    record: TransactionRecord,
    blockLengthOverrides?: ReadonlyMap<string, number>,
  ): void {
    const previous = this.#transactions.get(record.id);
    const before = this.#transactionResources(previous);
    const after = this.#transactionResources(record, blockLengthOverrides);
    const nextTransactions =
      this.#activeTransactionCount - before.transactions + after.transactions;
    const nextBlocks = this.#activeStagedBlockCount - before.blocks + after.blocks;
    const nextSegments = this.#activeStagedSegmentCount - before.segments + after.segments;
    const nextBytes = this.#activeStagedArtifactBytes - before.bytes + after.bytes;
    const nextTerminalTransactions =
      this.#terminalTransactionCount -
      Number(previous !== undefined && previous.status !== "active") +
      Number(record.status !== "active");
    if (nextTerminalTransactions > MAX_TERMINAL_TRANSACTION_RECORDS) {
      throw new StorageResourceLimitError(
        "terminal transaction",
        nextTerminalTransactions,
        MAX_TERMINAL_TRANSACTION_RECORDS,
      );
    }
    if (nextTransactions > MAX_ACTIVE_TRANSACTIONS) {
      throw new StorageResourceLimitError("transaction", nextTransactions, MAX_ACTIVE_TRANSACTIONS);
    }
    if (nextBlocks > MAX_GLOBAL_STAGED_BLOCKS) {
      throw new StorageResourceLimitError("staged block", nextBlocks, MAX_GLOBAL_STAGED_BLOCKS);
    }
    if (nextSegments > MAX_GLOBAL_STAGED_SEGMENTS) {
      throw new StorageResourceLimitError(
        "staged segment",
        nextSegments,
        MAX_GLOBAL_STAGED_SEGMENTS,
      );
    }
    if (nextBytes > MAX_GLOBAL_STAGED_ARTIFACT_BYTES) {
      throw new StorageResourceLimitError(
        "staged artifact byte",
        nextBytes,
        MAX_GLOBAL_STAGED_ARTIFACT_BYTES,
      );
    }
  }

  #changeCompactionJobReferences(
    previous: CompactionJobRecord | undefined,
    next: CompactionJobRecord | undefined,
  ): void {
    if (previous !== undefined) {
      if (!isTerminalCompactionJob(previous) && previous.transactionId !== null) {
        const transaction = this.#transactions.get(previous.transactionId);
        if (transaction?.status === "committed") {
          this.#roots.changeLinkedCommittedVersion(transaction.committedVersion, -1);
        }
      }
      this.#roots.changeCompactionJob(previous, -1);
    }
    if (next !== undefined) {
      this.#roots.changeCompactionJob(next, 1);
      if (!isTerminalCompactionJob(next) && next.transactionId !== null) {
        const transaction = this.#transactions.get(next.transactionId);
        if (transaction?.status === "committed") {
          this.#roots.changeLinkedCommittedVersion(transaction.committedVersion, 1);
        }
      }
    }
  }

  #assertTerminalCompactionJobTransition(
    previous: CompactionJobRecord | undefined,
    next: CompactionJobRecord,
  ): void {
    const count =
      this.#terminalCompactionJobCount -
      Number(previous !== undefined && isTerminalCompactionJob(previous)) +
      Number(isTerminalCompactionJob(next));
    if (count > MAX_TERMINAL_COMPACTION_JOB_RECORDS) {
      throw new StorageResourceLimitError(
        "terminal compaction job",
        count,
        MAX_TERMINAL_COMPACTION_JOB_RECORDS,
      );
    }
  }

  #pruneOneCompletedGarbageCollectionJob(): void {
    if (this.#completedGarbageCollectionJobCount < MAX_COMPLETED_GARBAGE_COLLECTION_JOB_RECORDS) {
      return;
    }
    let oldest: GarbageCollectionJobRecord | undefined;
    for (const record of this.#garbageCollectionJobs.values()) {
      if (record.state !== "completed") continue;
      if (
        oldest === undefined ||
        record.updatedAt < oldest.updatedAt ||
        (record.updatedAt === oldest.updatedAt && record.id < oldest.id)
      ) {
        oldest = record;
      }
    }
    if (oldest !== undefined) this.#garbageCollectionJobs.delete(oldest.id);
  }

  createTempOwner(record: TempOwnerRecord): void {
    validateTempOwnerRecord(record);
    if (this.#tempOwners.has(record.ownerId)) {
      throw new Error(`Temp owner already exists: ${record.ownerId}`);
    }
    for (const expired of boundedExpiryPage(
      this.#tempOwners,
      record.createdAt,
      null,
      RESOURCE_EXPIRY_SWEEP_ITEMS,
      (owner) => owner.ownerId,
      "Temp-owner create cleanup",
    ).records) {
      this.#tempOwners.delete(expired.ownerId);
    }
    if (this.#tempOwners.size >= MAX_ACTIVE_TEMP_OWNERS) {
      throw new StorageResourceLimitError(
        "temp owner",
        this.#tempOwners.size + 1,
        MAX_ACTIVE_TEMP_OWNERS,
      );
    }
    this.#tempOwners.set(record.ownerId, structuredClone(record));
  }

  getTempOwner(ownerId: string): TempOwnerRecord | undefined {
    validateId(ownerId);
    const record = this.#tempOwners.get(ownerId);
    return record === undefined ? undefined : structuredClone(record);
  }

  renewTempOwner(input: RenewTempOwnerInput): TempOwnerRecord {
    validateId(input.ownerId);
    const cutoff = validateBoundedExpiration(
      input.expiresAtCutoff,
      input.expiresAt,
      "Temp owner",
      MAX_TEMP_OWNER_TTL_MS,
    );
    const record = this.#tempOwners.get(input.ownerId);
    if (record?.revision !== input.expectedRevision) {
      throw new TempOwnerConflictError(
        input.ownerId,
        input.expectedRevision,
        record?.revision ?? null,
      );
    }
    if (Date.parse(record.expiresAt) <= cutoff) {
      throw new Error(`Temp owner ${input.ownerId} is expired`);
    }
    const renewed = {
      ...record,
      expiresAt: input.expiresAt,
      revision: safeWholeIncrement(record.revision, "Temp owner revision"),
    };
    this.#tempOwners.set(input.ownerId, renewed);
    return structuredClone(renewed);
  }

  listExpiredTempOwnerPage(
    expiresAtCutoff: string,
    afterCursor: string | null,
    limit: number,
  ): StoragePage<string, string> {
    const page = boundedExpiryPage(
      this.#tempOwners,
      expiresAtCutoff,
      afterCursor,
      limit,
      (record) => record.ownerId,
      "Temp owner page",
    );
    return { records: page.records.map((record) => record.ownerId), nextCursor: page.nextCursor };
  }

  /**
   * Removes the owner record when it is absent or expired, and reports whether the caller
   * should also remove the owner's physical pages. Page bytes are the enclosing store's.
   */
  removeTempOwnerIfExpired(ownerId: string, expiresAtCutoff: string): boolean {
    validateId(ownerId);
    const cutoff = Date.parse(expiresAtCutoff);
    if (!Number.isFinite(cutoff)) throw new TypeError("Temp owner expiry cutoff must be valid");
    const record = this.#tempOwners.get(ownerId);
    if (record !== undefined) {
      const expiresAt = Date.parse(record.expiresAt);
      if (Number.isFinite(expiresAt) && expiresAt > cutoff) return false;
    }
    this.#tempOwners.delete(ownerId);
    return true;
  }

  removeTempOwner(ownerId: string): void {
    validateId(ownerId);
    this.#tempOwners.delete(ownerId);
  }

  /**
   * Pages through owner IDs. `extraOwnerIds` carries owners the enclosing store discovered from
   * physical pages that have no record — orphaned spill runs still need to be listed so the
   * expiry sweep can reclaim them.
   */
  listTempOwnerIdsPage(
    afterOwnerId: string | null,
    limit: number,
    extraOwnerIds: Iterable<string>,
  ): StoragePage<string, string> {
    validatePageLimit(limit);
    const records: string[] = [];
    const add = (ownerId: string): void => {
      if (afterOwnerId !== null && ownerId <= afterOwnerId) return;
      const index = records.findIndex((existing) => existing >= ownerId);
      if (records[index] === ownerId) return;
      if (index < 0) records.push(ownerId);
      else records.splice(index, 0, ownerId);
      if (records.length > limit) records.pop();
    };
    for (const record of this.#tempOwners.orderedValues(afterOwnerId)) {
      add(record.ownerId);
      if (records.length === limit) break;
    }
    for (const ownerId of extraOwnerIds) add(ownerId);
    return {
      records,
      nextCursor: records.length === limit ? (records.at(-1) ?? null) : null,
    };
  }

  addTable(record: TableRecord, options: CatalogMutationOptions = {}): void {
    validateTableRuntimeRecord(record, "Table");
    this.#assertCatalogEpoch(record.id, record.revision, options.expectedCatalogEpoch);
    if (this.#tables.has(record.id)) throw new Error(`Table already exists: ${record.id}`);
    if (this.#tableIdsByName.has(record.name))
      throw new Error(`Table name already exists: ${record.name}`);
    for (const transaction of this.#transactions.values()) {
      if (transaction.status !== "active" || transaction.pendingTable === undefined) continue;
      if (transaction.pendingTable.id === record.id) {
        throw new Error(`Table already exists: ${record.id}`);
      }
      if (transaction.pendingTable.name === record.name) {
        throw new Error(`Table name already exists: ${record.name}`);
      }
    }
    this.#assertTableForeignKeys(record);
    const catalogCount = safeWholeIncrement(
      this.#tables.size + this.#pendingCatalogRecords,
      "Catalog record count",
    );
    if (catalogCount > MAX_CATALOG_RECORDS) {
      throw new StorageResourceLimitError("catalog record", catalogCount, MAX_CATALOG_RECORDS);
    }
    const newIndexNames = new Set<string>();
    for (const index of Object.values(record.secondaryIndexes ?? {})) {
      if (newIndexNames.has(index.name)) throw new TypeError(`Index already exists: ${index.name}`);
      newIndexNames.add(index.name);
      for (const table of this.#tables.values()) {
        if (
          Object.values(table.secondaryIndexes ?? {}).some(
            (existing) => existing.name === index.name,
          )
        ) {
          throw new TypeError(`Index already exists: ${index.name}`);
        }
      }
    }
    const nextCatalogEpoch = safeWholeIncrement(this.#catalogEpoch, "Catalog epoch");
    const nextSchemaEpoch = safeWholeIncrement(this.#schemaEpoch, "Schema epoch");
    this.#installTableRecord(record);
    this.#catalogEpoch = nextCatalogEpoch;
    this.#schemaEpoch = nextSchemaEpoch;
  }

  /** Applies a table whose complete admission checks already passed. */
  #installTableRecord(record: TableRecord): void {
    this.#setTable(structuredClone(record));
    this.#tableIdsByName.set(record.name, record.id);
    if (record.uniqueKeyColumnId !== undefined)
      this.#uniqueKeys.set(record.id, new OrderedStringSet());
    for (const [indexId, index] of Object.entries(record.secondaryIndexes ?? {})) {
      if (index.unique === true && index.uniqueEnforced === true) {
        this.#uniqueKeys.set(
          secondaryUniqueKeyNamespace(record.id, indexId),
          new OrderedStringSet(),
        );
      }
    }
  }

  /** Validates every declared parent against one coherent catalog state. */
  #assertTableForeignKeys(record: TableRecord): void {
    for (const key of record.foreignKeys ?? []) {
      const parent =
        key.parentTable === record.name
          ? record
          : (() => {
              const parentId = this.#tableIdsByName.get(key.parentTable);
              return parentId === undefined ? undefined : this.#tables.get(parentId);
            })();
      if (parent === undefined) {
        throw new TypeError(
          `FOREIGN KEY ${key.name} references a missing table: ${key.parentTable}`,
        );
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

  getTable(id: string): TableRecord | undefined {
    const record = this.#tables.get(id);
    return record === undefined ? undefined : structuredClone(record);
  }

  updateTable(id: string, expectedRevision: number, update: TableRecordUpdate): TableRecord {
    const record = this.#tables.get(id);
    const actualRevision = record?.revision ?? null;
    if (record === undefined || actualRevision !== expectedRevision) {
      throw new TableRecordConflictError(id, expectedRevision, actualRevision);
    }
    this.#assertCatalogEpoch(id, expectedRevision, update.expectedCatalogEpoch);
    if (
      update.expectedManifestVersion !== undefined &&
      (this.getCurrentManifestVersion() ?? null) !== update.expectedManifestVersion.value
    ) {
      throw new WriteConflictError(
        update.expectedManifestVersion.value,
        this.getCurrentManifestVersion(),
      );
    }
    let uniqueSeed: Set<string> | undefined;
    if (update.uniqueKeySeed !== undefined) {
      uniqueSeed = new Set<string>();
      for (const token of update.uniqueKeySeed.keyTokens) {
        if (uniqueSeed.has(token))
          throw new UniqueKeyConflictError(update.uniqueKeySeed.namespaceId, token);
        uniqueSeed.add(token);
      }
    }
    if (update.autoIncrementSeed !== undefined) {
      validateId(update.autoIncrementSeed.columnId);
      validateAutoIncrementReservation(0, update.autoIncrementSeed.atLeast);
    }
    if (update.columns !== undefined) validateTableColumns(update.columns);
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
    if (nextSecondary !== null && nextSecondary !== undefined && retainedColumnIds !== undefined) {
      nextSecondary = Object.fromEntries(
        Object.entries(nextSecondary).filter(([, index]) =>
          secondaryIndexColumnIds(index).every((columnId) => retainedColumnIds.has(columnId)),
        ),
      );
      if (Object.keys(nextSecondary).length === 0) nextSecondary = null;
    }
    const nextIndexNames = new Set<string>();
    for (const index of Object.values(nextSecondary ?? {})) {
      if (nextIndexNames.has(index.name))
        throw new TypeError(`Index already exists: ${index.name}`);
      nextIndexNames.add(index.name);
      for (const [otherTableId, table] of this.#tables) {
        if (otherTableId === id) continue;
        if (
          Object.values(table.secondaryIndexes ?? {}).some(
            (existing) => existing.name === index.name,
          )
        ) {
          throw new TypeError(`Index already exists: ${index.name}`);
        }
      }
    }
    const nextTriggers = update.triggers === undefined ? previousTriggers : update.triggers;
    const nextView = update.view === undefined ? previousView : update.view;
    validateTableView(nextView ?? undefined);
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
      revision: safeWholeIncrement(expectedRevision, "Table revision"),
    };
    validateSecondaryIndexes(updated);
    this.#assertTableTriggerOwnership(updated);
    let autoIncrementCounter: { key: string; next: bigint } | undefined;
    if (update.autoIncrementSeed !== undefined) {
      const { columnId, atLeast } = update.autoIncrementSeed;
      const column = updated.columns.find((candidate) => candidate.id === columnId);
      if (column?.defaultValue?.kind !== "autoincrement") {
        throw new TypeError(
          `Auto-increment seed has no declared column: ${updated.id}/${columnId}`,
        );
      }
      const key = `${updated.id}/${columnId}`;
      const current = this.#nextAutoIncrement.get(key) ?? 1n;
      autoIncrementCounter = { key, next: current > atLeast ? current : atLeast };
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
    this.#catalogRetainedBytesAfter(updated);
    const nextCatalogEpoch = safeWholeIncrement(this.#catalogEpoch, "Catalog epoch");
    const structural =
      update.columns !== undefined ||
      secondaryIndexWriteContractChanged(previousSecondary, nextSecondary) ||
      update.triggers !== undefined ||
      update.view !== undefined;
    const nextSchemaEpoch = structural
      ? safeWholeIncrement(this.#schemaEpoch, "Schema epoch")
      : this.#schemaEpoch;
    if (retainedColumnIds !== undefined) {
      for (const column of record.columns) {
        if (!retainedColumnIds.has(column.id)) this.#deleteFtsColumn(record.id, column.id);
      }
    }
    const retainedIndexStorage = new Set(
      Object.values(nextSecondary ?? {}).map((index) => index.storageColumnId),
    );
    for (const index of Object.values(previousSecondary ?? {})) {
      if (!retainedIndexStorage.has(index.storageColumnId)) {
        this.#deleteFtsColumn(record.id, index.storageColumnId);
      }
    }
    for (const [indexId, previous] of Object.entries(previousSecondary ?? {})) {
      if (previous.unique === true && nextSecondary?.[indexId]?.unique !== true) {
        this.#uniqueKeys.delete(secondaryUniqueKeyNamespace(record.id, indexId));
      }
    }
    if (update.uniqueKeySeed !== undefined) {
      this.#uniqueKeys.set(
        update.uniqueKeySeed.namespaceId,
        new OrderedStringSet(uniqueSeed ?? []),
      );
    }
    if (autoIncrementCounter !== undefined) {
      this.#nextAutoIncrement.set(autoIncrementCounter.key, autoIncrementCounter.next);
    }
    this.#setTable(updated);
    this.#catalogEpoch = nextCatalogEpoch;
    this.#schemaEpoch = nextSchemaEpoch;
    return structuredClone(updated);
  }

  removeTable(id: string, expectedRevision: number, options: CatalogMutationOptions = {}): void {
    const record = this.#tables.get(id);
    const actualRevision = record?.revision ?? null;
    if (record === undefined || actualRevision !== expectedRevision) {
      throw new TableRecordConflictError(id, expectedRevision, actualRevision);
    }
    this.#assertCatalogEpoch(id, expectedRevision, options.expectedCatalogEpoch);
    this.#assertTableNotInUse(id);
    if (this.#segments.tableValues(id).next().done !== true) {
      throw new TypeError(`Cannot remove non-empty table ${id}; use dropTable`);
    }
    const nextCatalogEpoch = safeWholeIncrement(this.#catalogEpoch, "Catalog epoch");
    const nextSchemaEpoch = safeWholeIncrement(this.#schemaEpoch, "Schema epoch");
    this.#deleteTableRecords(record);
    this.#catalogEpoch = nextCatalogEpoch;
    this.#schemaEpoch = nextSchemaEpoch;
  }

  #assertCatalogEpoch(
    tableId: string,
    expectedRevision: number,
    expectedCatalogEpoch: number | undefined,
  ): void {
    if (expectedCatalogEpoch === undefined) return;
    if (!Number.isSafeInteger(expectedCatalogEpoch) || expectedCatalogEpoch < 0) {
      throw new TypeError("Expected catalog epoch is invalid");
    }
    if (this.#catalogEpoch !== expectedCatalogEpoch) {
      const actualRevision = this.#tables.get(tableId)?.revision ?? null;
      throw new TableRecordConflictError(tableId, expectedRevision, actualRevision);
    }
  }

  #planManifestRemovals(
    expectedVersion: number | null,
    candidateIds: Iterable<string>,
    changedTableIds: readonly string[],
    createdAt: string,
  ): { manifest: Manifest; removed: ManifestBlockRecord[] } {
    const base = expectedVersion === null ? undefined : this.#manifests.get(expectedVersion);
    const nextVersion = nextManifestVersion(expectedVersion);
    this.#assertNextManifestPinLag(nextVersion, createdAt);
    const removed: ManifestBlockRecord[] = [];
    for (const id of new Set(candidateIds)) {
      const record = this.#manifestBlocks.get(id);
      if (record?.removedVersion === null) {
        removed.push({ ...record, removedVersion: nextVersion });
      }
    }
    this.#assertRetiredHistoryAddition(removed);
    this.#assertPinnedRetiredLimits(createdAt, undefined, undefined, [], removed);
    const removedBytes = removed.reduce((sum, record) => sum + record.byteLength, 0);
    const manifest = createManifest({
      expectedVersion,
      liveBlockCount: (base?.liveBlockCount ?? 0) - removed.length,
      liveBlockBytes: (base?.liveBlockBytes ?? 0) - removedBytes,
      changedTableIds,
      createdAt,
    });
    this.#manifestUsageAfter([manifest]);
    return { manifest, removed };
  }

  #applyManifestRemovals(manifest: Manifest, removed: readonly ManifestBlockRecord[]): void {
    this.#setManifest(manifest);
    for (const record of removed) this.#manifestBlocks.set(record.blockId, record);
    this.#currentVersion = manifest.version;
  }

  dropTable(input: DropTableInput): ManifestSummary {
    validateId(input.tableId);
    validateTimestampRuntime(input.committedAt, "Table drop commit timestamp");
    const table = this.#tables.get(input.tableId);
    const actualRevision = table?.revision ?? null;
    if (table === undefined || actualRevision !== input.expectedTableRevision) {
      throw new TableRecordConflictError(
        input.tableId,
        input.expectedTableRevision,
        actualRevision,
      );
    }
    if (this.#currentVersion !== input.expectedManifestVersion) {
      throw new WriteConflictError(input.expectedManifestVersion, this.#currentVersion);
    }
    if (this.#catalogEpoch !== input.expectedCatalogEpoch) {
      throw new TableRecordConflictError(
        input.tableId,
        input.expectedTableRevision,
        actualRevision,
      );
    }
    this.#assertTableNotInUse(input.tableId);

    const tableBlockIds = new Set(
      [...this.#segments.tableValues(input.tableId)].flatMap((segment) =>
        Object.values(segment.columnBlockIds).flat(),
      ),
    );
    // Aliased payloads remain live when another table still references them.
    const otherTableBlockIds = new Set(
      [...tableBlockIds].filter(
        (blockId) =>
          this.#segments.blockReferenceCount(blockId) >
          this.#segments.tableBlockReferenceCount(input.tableId, blockId),
      ),
    );
    const { manifest, removed } = this.#planManifestRemovals(
      input.expectedManifestVersion,
      [...tableBlockIds].filter((id) => !otherTableBlockIds.has(id)),
      [input.tableId],
      input.committedAt,
    );
    const nextCatalogEpoch = safeWholeIncrement(this.#catalogEpoch, "Catalog epoch");
    const nextSchemaEpoch = safeWholeIncrement(this.#schemaEpoch, "Schema epoch");

    this.#applyManifestRemovals(manifest, removed);
    this.#deleteTableRecords(table);
    this.#catalogEpoch = nextCatalogEpoch;
    this.#schemaEpoch = nextSchemaEpoch;
    return structuredClone(manifest);
  }

  dropTableColumn(input: DropTableColumnInput): ManifestSummary {
    validateId(input.tableId);
    validateId(input.columnId);
    validateTimestampRuntime(input.committedAt, "Column drop commit timestamp");
    const table = this.#tables.get(input.tableId);
    const actualRevision = table?.revision ?? null;
    if (table === undefined || actualRevision !== input.expectedTableRevision) {
      throw new TableRecordConflictError(
        input.tableId,
        input.expectedTableRevision,
        actualRevision,
      );
    }
    if (this.#currentVersion !== input.expectedManifestVersion) {
      throw new WriteConflictError(input.expectedManifestVersion, this.#currentVersion);
    }
    if (this.#catalogEpoch !== input.expectedCatalogEpoch) {
      throw new TableRecordConflictError(
        input.tableId,
        input.expectedTableRevision,
        actualRevision,
      );
    }
    this.#assertTableNotInUse(input.tableId);

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

    const candidateBlockIds = new Set<string>();
    const remainingBlockIds = new Set<string>();
    const updatedSegments = new Map<string, SegmentRecord>();
    for (const segment of this.#segments.tableValues(input.tableId)) {
      for (const id of segment.columnBlockIds[input.columnId] ?? []) candidateBlockIds.add(id);
      const columnBlockIds = Object.fromEntries(
        Object.entries(segment.columnBlockIds).filter(([columnId]) => columnId !== input.columnId),
      );
      for (const ids of Object.values(columnBlockIds)) {
        for (const id of ids) remainingBlockIds.add(id);
      }
      if (segment.columnBlockIds[input.columnId] === undefined) continue;
      // An empty physical column map still carries the segment's row envelope. For example,
      updatedSegments.set(
        segment.id,
        validateSegmentRuntimeRecord(
          { ...segment, columnBlockIds },
          `Column retirement segment ${segment.id}`,
        ),
      );
    }
    for (const id of candidateBlockIds) {
      if (
        this.#segments.blockReferenceCount(id) >
        this.#segments.tableBlockReferenceCount(input.tableId, id)
      ) {
        remainingBlockIds.add(id);
      }
    }

    const { manifest, removed } = this.#planManifestRemovals(
      input.expectedManifestVersion,
      [...candidateBlockIds].filter((id) => !remainingBlockIds.has(id)),
      [input.tableId],
      input.committedAt,
    );
    const { ftsColumns: previousFts, revision: _previousRevision, ...tableBase } = table;
    void _previousRevision;
    const nextFts = Object.fromEntries(
      Object.entries(previousFts ?? {}).filter(([columnId]) => columnId !== input.columnId),
    );
    const updatedTable: TableRecord = {
      ...tableBase,
      columns: table.columns.filter((candidate) => candidate.id !== input.columnId),
      ...(Object.keys(nextFts).length === 0 ? {} : { ftsColumns: nextFts }),
      revision: safeWholeIncrement(actualRevision, "Table revision"),
    };
    validateTableColumns(updatedTable.columns);
    validateSecondaryIndexes(updatedTable);
    const nextCatalogEpoch = safeWholeIncrement(this.#catalogEpoch, "Catalog epoch");
    const nextSchemaEpoch = safeWholeIncrement(this.#schemaEpoch, "Schema epoch");

    this.#applyManifestRemovals(manifest, removed);
    this.#setTable(updatedTable);
    this.#replaceSegments([...updatedSegments.values()]);
    this.#nextAutoIncrement.delete(`${input.tableId}/${input.columnId}`);
    this.#deleteFtsColumn(input.tableId, input.columnId);
    this.#catalogEpoch = nextCatalogEpoch;
    this.#schemaEpoch = nextSchemaEpoch;
    return structuredClone(manifest);
  }

  #assertTableNotInUse(id: string): void {
    const target = this.#tables.get(id);
    if (target !== undefined) {
      const publishedChild = [...this.#tables.values()].find(
        (table) =>
          table.id !== id &&
          (table.foreignKeys ?? []).some((key) => key.parentTable === target.name),
      );
      if (publishedChild !== undefined) {
        throw new Error(`Cannot remove ${target.name}: referenced by ${publishedChild.name}`);
      }
      const pendingChild = [...this.#transactions.values()]
        .filter(
          (transaction) =>
            transaction.status === "active" &&
            transaction.pendingTable !== undefined &&
            (transaction.pendingTable.foreignKeys ?? []).some(
              (key) => key.parentTable === target.name,
            ),
        )
        .sort((left, right) => left.id.localeCompare(right.id))[0];
      if (pendingChild !== undefined) {
        throw new TableInUseError(id, "transaction", pendingChild.id);
      }
    }
    const activeTransactionId = [
      ...new Set(
        [...this.#segments.tableValues(id)].flatMap((segment) =>
          this.#transactions.get(segment.transactionId)?.status === "active"
            ? [segment.transactionId]
            : [],
        ),
      ),
    ].sort()[0];
    if (activeTransactionId !== undefined) {
      throw new TableInUseError(id, "transaction", activeTransactionId);
    }
    const activeCompactionJobId = [...this.#compactionJobs.values()]
      .filter((job) => job.tableId === id && !isTerminalCompactionJob(job))
      .map((job) => job.id)
      .sort()[0];
    if (activeCompactionJobId !== undefined) {
      throw new TableInUseError(id, "compaction job", activeCompactionJobId);
    }
  }

  #deleteTableRecords(record: TableRecord): void {
    const id = record.id;
    this.#replaceSegments(
      [],
      [...this.#segments.tableValues(id)].map((segment) => segment.id),
    );
    const owned = `${id}/`;
    for (const key of [...this.#ftsBases.keys()]) {
      if (key.startsWith(owned)) this.#ftsBases.delete(key);
    }
    for (const key of [...this.#ftsDeltas.keys()]) {
      if (key.startsWith(owned)) this.#ftsDeltas.delete(key);
    }
    for (const key of [...this.#nextAutoIncrement.keys()]) {
      if (key.startsWith(owned)) this.#nextAutoIncrement.delete(key);
    }
    this.#uniqueKeys.delete(id);
    const secondaryUniquePrefix = `${id}\u0000secondary-index\u0000`;
    for (const namespaceId of [...this.#uniqueKeys.keys()]) {
      if (namespaceId.startsWith(secondaryUniquePrefix)) this.#uniqueKeys.delete(namespaceId);
    }
    for (const [buildId, build] of [...this.#uniqueKeyBuilds]) {
      if (build.record.tableId === id) this.#setUniqueKeyBuild(undefined, buildId);
    }
    this.#nextRowIds.delete(id);
    this.#tableIdsByName.delete(record.name);
    this.#deleteTable(record);
  }

  writeFtsBase(
    tableId: string,
    columnId: string,
    input: { coversVersion: number; chunks: FtsPosting[][]; totalTokens: number },
  ): void {
    const table = this.#tables.get(tableId);
    if (table === undefined || !activePostingStorageColumnIds(table).has(columnId)) {
      throw new Error(`Postings index is no longer active: ${tableId}/${columnId}`);
    }
    validateFtsBaseInput(input, "Full-text base");
    const key = `${tableId}/${columnId}`;
    this.#ftsBases.set(key, structuredClone(input));
    const deltas = this.#ftsDeltas.get(key);
    if (deltas !== undefined) {
      for (const version of [...deltas.keys()]) {
        if (version <= input.coversVersion) deltas.delete(version);
      }
    }
  }

  removeFtsColumn(tableId: string, columnId: string): void {
    const table = this.#tables.get(tableId);
    if (table !== undefined && activePostingStorageColumnIds(table).has(columnId)) {
      throw new Error(`Postings index is still active: ${tableId}/${columnId}`);
    }
    this.#deleteFtsColumn(tableId, columnId);
  }

  #deleteFtsColumn(tableId: string, columnId: string): void {
    const key = `${tableId}/${columnId}`;
    this.#ftsBases.delete(key);
    this.#ftsDeltas.delete(key);
  }

  readFtsCandidates(
    tableId: string,
    columnId: string,
    terms: readonly FtsPostingQuery[],
    upToVersion: number,
    maxRowIds = MAX_FTS_CANDIDATE_ROW_IDS,
  ): FtsCandidates & {
    deltaChunkCount: number;
    totalTokens: number;
    coversVersion: number;
    hasBase: boolean;
  } {
    validateId(tableId);
    validateId(columnId);
    validateFtsPostingQueries(terms);
    if (!Number.isSafeInteger(upToVersion) || upToVersion < -1) {
      throw new RangeError("Full-text query version must be a safe integer at least -1");
    }
    validateFtsCandidateLimit(maxRowIds);
    const key = `${tableId}/${columnId}`;
    const base = this.#ftsBases.get(key);
    const deltas =
      this.#ftsDeltas.get(key) ??
      new Map<number, { postings: FtsPosting[]; totalTokens: number }>();
    const chunkLists: Array<readonly FtsPosting[]> = [...(base?.chunks ?? [])];
    let deltaChunkCount = 0;
    let totalTokens = base?.totalTokens ?? 0;
    for (const [version, delta] of deltas) {
      if (version <= (base?.coversVersion ?? -1) || version > upToVersion) continue;
      deltaChunkCount += 1;
      totalTokens += delta.totalTokens;
      chunkLists.push(delta.postings);
    }
    return {
      ...collectFtsCandidates(chunkLists, terms, maxRowIds),
      deltaChunkCount,
      totalTokens,
      coversVersion: base?.coversVersion ?? -1,
      hasBase: base !== undefined,
    };
  }

  /**
   * Applies a commit's full-text deltas, and closes the stale-writer race: a commit adding
   * segments to a table with an active index but no delta for one of its columns flips that
   * column to "invalid" so a rebuild self-heals instead of the data commit failing.
   */
  #applyFtsChanges(
    pendingSegments: readonly SegmentRecord[],
    changeList: readonly FtsChanges[] | undefined,
    version: number,
    logicallyChangedTableIds?: readonly string[],
  ): void {
    const changedTableIds = new Set(pendingSegments.map((segment) => segment.tableId));
    const scalarChangedTableIds =
      logicallyChangedTableIds === undefined ? changedTableIds : new Set(logicallyChangedTableIds);
    for (const tableId of changedTableIds) {
      const record = this.#tables.get(tableId);
      if (record === undefined) continue;
      const forTable = (changeList ?? []).find((entry) => entry.tableId === tableId);
      const covered = new Set(forTable?.columns.map((column) => column.columnId) ?? []);
      const invalidated = invalidateUncoveredFtsColumns(record, covered);
      const withFts = invalidated ?? record;
      const withSecondary = scalarChangedTableIds.has(tableId)
        ? invalidateUncoveredSecondaryIndexes(withFts, covered)
        : undefined;
      if (withSecondary !== undefined) this.#setTable(withSecondary);
      else if (invalidated !== undefined) this.#setTable(invalidated);
    }
    for (const changes of changeList ?? []) this.#applyFtsEntry(changes, version);
  }

  #applyFtsEntry(changes: FtsChanges, version: number): void {
    const initialTable = this.#tables.get(changes.tableId);
    if (initialTable === undefined) return;
    let table: TableRecord = initialTable;
    for (const column of changes.columns) {
      const currentTable: TableRecord = table;
      const active = activePostingStorageColumnIds(currentTable);
      if (!active.has(column.columnId)) continue;
      const key = `${changes.tableId}/${column.columnId}`;
      const deltas =
        this.#ftsDeltas.get(key) ??
        new Map<number, { postings: FtsPosting[]; totalTokens: number }>();
      if (!deltas.has(version) && deltas.size >= MAX_FTS_DELTA_CHUNKS) {
        const retained = new Set(active);
        retained.delete(column.columnId);
        const withoutFts: TableRecord =
          invalidateUncoveredFtsColumns(currentTable, retained) ?? currentTable;
        const invalidated = invalidateUncoveredSecondaryIndexes(withoutFts, retained);
        table = invalidated ?? withoutFts;
        this.#setTable(table);
        this.#ftsDeltas.delete(key);
        continue;
      }
      deltas.set(version, {
        postings: structuredClone(column.postings),
        totalTokens: column.totalTokens,
      });
      this.#ftsDeltas.set(key, deltas);
    }
  }

  /**
   * The delta half of a full-text read, for stores that keep base chunks outside the core
   * (the OPFS store keeps them as immutable files). Mirrors `readFtsCandidates`' delta
   * selection exactly: versions above the base's coverage, up to the read's snapshot.
   */
  readFtsDeltas(
    tableId: string,
    columnId: string,
    coversVersion: number,
    upToVersion: number,
  ): { chunkLists: FtsPosting[][]; deltaChunkCount: number; deltaTokens: number } {
    const deltas = this.#ftsDeltas.get(`${tableId}/${columnId}`);
    const chunkLists: FtsPosting[][] = [];
    let deltaChunkCount = 0;
    let deltaTokens = 0;
    for (const [version, delta] of deltas ?? []) {
      if (version <= coversVersion || version > upToVersion) continue;
      deltaChunkCount += 1;
      deltaTokens += delta.totalTokens;
      chunkLists.push(structuredClone(delta.postings));
    }
    return { chunkLists, deltaChunkCount, deltaTokens };
  }

  readFtsPostings(
    tableId: string,
    columnId: string,
    upToVersion: number,
    maxRowIds = MAX_FTS_CANDIDATE_ROW_IDS,
    maxRetainedBytes?: number,
  ): {
    postings: FtsPosting[];
    overflow: boolean;
    deltaChunkCount: number;
    coversVersion: number;
    hasBase: boolean;
  } {
    validateId(tableId);
    validateId(columnId);
    if (!Number.isSafeInteger(upToVersion) || upToVersion < -1) {
      throw new RangeError("Full-text query version must be a safe integer at least -1");
    }
    const base = this.#ftsBases.get(`${tableId}/${columnId}`);
    const coversVersion = base?.coversVersion ?? -1;
    const delta = this.readFtsDeltas(tableId, columnId, coversVersion, upToVersion);
    const result = collectFtsPostingsBounded(
      [...(base?.chunks ?? []), ...delta.chunkLists],
      maxRowIds,
      maxRetainedBytes,
    );
    return {
      ...result,
      deltaChunkCount: delta.deltaChunkCount,
      coversVersion,
      hasBase: base !== undefined,
    };
  }

  /** The delta-pruning half of `writeFtsBase`, for stores that keep the base itself outside. */
  pruneFtsDeltas(tableId: string, columnId: string, coversVersion: number): void {
    const deltas = this.#ftsDeltas.get(`${tableId}/${columnId}`);
    if (deltas === undefined) return;
    for (const version of [...deltas.keys()]) {
      if (version <= coversVersion) deltas.delete(version);
    }
  }

  /**
   * Applies a garbage-collection step's already-resolved record effect: the advanced job
   * record, manifest tombstones, and segment removals — exactly the mutations
   * `runGarbageCollectionStep` makes, reproduced from its outcome. The OPFS store logs the
   * outcome rather than the request so replaying is deterministic without the executor's
   * physical file knowledge.
   */
  applyGarbageCollectionEffect(effect: {
    job: GarbageCollectionJobRecord;
    prunedManifestVersions: readonly number[];
    reclaimedSegmentIds: readonly string[];
    reclaimedBlockIds: readonly string[];
    reclaimedTransactionIds: readonly string[];
    updatedAt: string;
  }): void {
    const current = this.#garbageCollectionJobs.get(effect.job.id);
    if (current === undefined) {
      throw new Error(`Garbage collection effect has no job: ${effect.job.id}`);
    }
    const updated = normalizeGarbageCollectionJobRecord(effect.job);
    const prunedManifestVersions = validateUniqueWholeNumbersRuntime(
      effect.prunedManifestVersions,
      "Garbage collection effect manifest versions",
    );
    const reclaimedSegmentIds = validateUniqueIdsRuntime(
      effect.reclaimedSegmentIds,
      "Garbage collection effect segment IDs",
    );
    const reclaimedBlockIds = validateUniqueIdsRuntime(
      effect.reclaimedBlockIds,
      "Garbage collection effect block IDs",
    );
    const reclaimedTransactionIds = validateUniqueIdsRuntime(
      effect.reclaimedTransactionIds,
      "Garbage collection effect transaction IDs",
    );
    validateTimestampRuntime(effect.updatedAt, "Garbage collection effect timestamp");

    if (current.state === "completed") {
      if (
        prunedManifestVersions.length !== 0 ||
        reclaimedSegmentIds.length !== 0 ||
        reclaimedBlockIds.length !== 0 ||
        reclaimedTransactionIds.length !== 0 ||
        !deepRecordEqual(updated, current)
      ) {
        throw new TypeError("A completed garbage collection effect cannot mutate records");
      }
    } else {
      const increment = (next: number, before: number, label: string): number => {
        const value = next - before;
        if (!Number.isSafeInteger(value) || value < 0) {
          throw new TypeError(`${label} moves backwards or exceeds the safe range`);
        }
        return value;
      };
      const accounting = {
        examinedManifestCount: increment(
          updated.cursor.manifestIndex,
          current.cursor.manifestIndex,
          "Garbage collection manifest cursor",
        ),
        prunedManifestCount: increment(
          updated.prunedManifestCount,
          current.prunedManifestCount,
          "Garbage collection pruned manifest count",
        ),
        alreadyPrunedManifestCount: increment(
          updated.alreadyPrunedManifestCount,
          current.alreadyPrunedManifestCount,
          "Garbage collection already-pruned manifest count",
        ),
        retainedManifestCount: increment(
          updated.retainedManifestCount,
          current.retainedManifestCount,
          "Garbage collection retained manifest count",
        ),
        missingManifestCount: increment(
          updated.missingManifestCount,
          current.missingManifestCount,
          "Garbage collection missing manifest count",
        ),
        examinedSegmentCount: increment(
          updated.cursor.segmentIndex,
          current.cursor.segmentIndex,
          "Garbage collection segment cursor",
        ),
        reclaimedSegmentCount: increment(
          updated.reclaimedSegmentCount,
          current.reclaimedSegmentCount,
          "Garbage collection reclaimed segment count",
        ),
        retainedSegmentCount: increment(
          updated.retainedSegmentCount,
          current.retainedSegmentCount,
          "Garbage collection retained segment count",
        ),
        missingSegmentCount: increment(
          updated.missingSegmentCount,
          current.missingSegmentCount,
          "Garbage collection missing segment count",
        ),
        examinedBlockCount: increment(
          updated.cursor.blockIndex,
          current.cursor.blockIndex,
          "Garbage collection block cursor",
        ),
        reclaimedBlockCount: increment(
          updated.reclaimedBlockCount,
          current.reclaimedBlockCount,
          "Garbage collection reclaimed block count",
        ),
        retainedBlockCount: increment(
          updated.retainedBlockCount,
          current.retainedBlockCount,
          "Garbage collection retained block count",
        ),
        missingBlockCount: increment(
          updated.missingBlockCount,
          current.missingBlockCount,
          "Garbage collection missing block count",
        ),
        reclaimedBlockBytes: increment(
          updated.reclaimedBlockBytes,
          current.reclaimedBlockBytes,
          "Garbage collection reclaimed block bytes",
        ),
        examinedTransactionCount: increment(
          updated.cursor.transactionIndex,
          current.cursor.transactionIndex,
          "Garbage collection transaction cursor",
        ),
        reclaimedTransactionCount: increment(
          updated.reclaimedTransactionCount,
          current.reclaimedTransactionCount,
          "Garbage collection reclaimed transaction count",
        ),
        retainedTransactionCount: increment(
          updated.retainedTransactionCount,
          current.retainedTransactionCount,
          "Garbage collection retained transaction count",
        ),
        missingTransactionCount: increment(
          updated.missingTransactionCount,
          current.missingTransactionCount,
          "Garbage collection missing transaction count",
        ),
        updatedAt: effect.updatedAt,
      };
      const expected = advanceGarbageCollectionJobRecord(current, accounting);
      if (!deepRecordEqual(expected, updated)) {
        throw new TypeError("Garbage collection effect job is not the next valid state");
      }
      if (
        accounting.prunedManifestCount !== prunedManifestVersions.length ||
        accounting.reclaimedSegmentCount !== reclaimedSegmentIds.length ||
        accounting.reclaimedBlockCount !== reclaimedBlockIds.length ||
        accounting.reclaimedTransactionCount !== reclaimedTransactionIds.length
      ) {
        throw new TypeError("Garbage collection effect arrays disagree with job accounting");
      }
      const reclaimedBytes = reclaimedBlockIds.reduce((total, id) => {
        const bytes = this.#physical.blockByteLength(id);
        if (bytes === undefined)
          throw new Error(`Garbage collection effect block is missing: ${id}`);
        return safeStorageSum(total, bytes);
      }, 0);
      if (reclaimedBytes !== accounting.reclaimedBlockBytes) {
        throw new TypeError("Garbage collection effect block bytes disagree with job accounting");
      }
    }
    for (const version of prunedManifestVersions) {
      const manifest = this.#manifests.get(version);
      if (
        manifest === undefined ||
        manifest.prunedAt !== undefined ||
        !current.candidateManifestVersions.includes(version)
      ) {
        throw new Error(`Garbage collection effect cannot prune manifest: ${String(version)}`);
      }
    }
    for (const id of reclaimedSegmentIds) {
      if (!this.#segments.has(id) || !current.candidateSegmentIds.includes(id)) {
        throw new Error(`Garbage collection effect cannot reclaim segment: ${id}`);
      }
    }
    for (const id of reclaimedBlockIds) {
      if (!current.candidateBlockIds.includes(id)) {
        throw new Error(`Garbage collection effect cannot reclaim block: ${id}`);
      }
    }
    for (const id of reclaimedTransactionIds) {
      if (!this.#transactions.has(id) || !current.candidateTransactionIds.includes(id)) {
        throw new Error(`Garbage collection effect cannot reclaim transaction: ${id}`);
      }
    }
    this.#replaceManifests(
      prunedManifestVersions.flatMap((version) => {
        const manifest = this.#manifests.get(version);
        return manifest === undefined ? [] : [{ ...manifest, prunedAt: effect.updatedAt }];
      }),
    );
    this.#replaceSegments([], reclaimedSegmentIds);
    // Payload reclamation closes this provenance interval's entire recovery horizon. Delete the
    // bounded tombstone in the same atomic record update so lifetime write history cannot grow
    // the provenance index forever.
    for (const id of reclaimedBlockIds) this.#manifestBlocks.delete(id);
    for (const id of reclaimedTransactionIds) this.#transactions.delete(id);
    this.#garbageCollectionJobs.set(updated.id, structuredClone(updated));
  }

  getTableByName(name: string): TableRecord | undefined {
    const id = this.#tableIdsByName.get(name);
    return id === undefined ? undefined : this.getTable(id);
  }

  /** For stores that compose lookups over their own overridable methods. */
  getTableIdByName(name: string): string | undefined {
    return this.#tableIdsByName.get(name);
  }

  listTables(): TableRecord[] {
    return [...this.#tables.values()]
      .sort((left, right) => left.name.localeCompare(right.name))
      .map((record) => structuredClone(record));
  }

  getSegment(id: string): SegmentRecord | undefined {
    const record = this.#segments.get(id);
    return record === undefined ? undefined : normalizeSegmentRecord(record);
  }

  listSegmentPage(afterId: string | null, limit: number): StoragePage<SegmentRecord, string> {
    validatePageLimit(limit);
    const records = boundedRecordPage(this.#segments, afterId, limit).map((record) =>
      normalizeSegmentRecord(record),
    );
    return { records, nextCursor: records.length === limit ? (records.at(-1)?.id ?? null) : null };
  }

  listTableSegmentPage(
    tableId: string,
    afterId: string | null,
    limit: number,
  ): StoragePage<SegmentRecord, string> {
    validateId(tableId);
    if (afterId !== null) validateId(afterId);
    validatePageLimit(limit);
    const records: SegmentRecord[] = [];
    for (const record of this.#segments.tableValues(tableId)) {
      if (afterId !== null && record.id.localeCompare(afterId) <= 0) continue;
      records.push(normalizeSegmentRecord(record));
      if (records.length === limit) break;
    }
    return { records, nextCursor: records.length === limit ? (records.at(-1)?.id ?? null) : null };
  }

  removeAbortedSegment(id: string, expectedTransactionId: string): boolean {
    validateId(id);
    validateId(expectedTransactionId);
    const segment = this.#segments.get(id);
    if (segment === undefined) return false;
    if (segment.transactionId !== expectedTransactionId) {
      throw new Error(`Segment ${id} belongs to another transaction`);
    }
    const owner = this.#transactions.get(expectedTransactionId);
    if (owner?.status !== "aborted") {
      throw new Error(`Segment ${id} is not owned by an aborted transaction`);
    }
    if (!owner.pendingSegmentIds.includes(id)) {
      throw new Error(`Segment ${id} is not journaled by its aborted transaction`);
    }
    const blockIds = new Set(Object.values(segment.columnBlockIds).flat());
    for (const blockId of blockIds) {
      const provenance = this.#manifestBlocks.get(blockId);
      if (provenance !== undefined && this.#manifestBlockHasReadableVersion(provenance)) {
        throw new Error(`Segment ${id} is still reachable from a readable manifest`);
      }
    }
    for (const transaction of this.#transactions.values()) {
      if (transaction.status !== "active") continue;
      if (transaction.pendingSegmentIds.includes(id)) {
        throw new Error(
          `Segment ${id} is still referenced by active transaction ${transaction.id}`,
        );
      }
    }
    for (const job of this.#compactionJobs.values()) {
      if (isTerminalCompactionJob(job)) continue;
      if (job.sourceSegmentIds.includes(id) || compactionOutputSegmentIds(job).includes(id)) {
        throw new Error(`Segment ${id} is still referenced by compaction job ${job.id}`);
      }
    }
    const retainedSegmentIds = owner.pendingSegmentIds.filter((segmentId) => segmentId !== id);
    const rebasedSegments = retainedSegmentIds.map((segmentId, commitOrdinal) => {
      const retained = this.#segments.get(segmentId);
      if (retained?.transactionId !== owner.id) {
        throw new Error(`Aborted transaction segment is missing or foreign: ${segmentId}`);
      }
      return retained.commitOrdinal === commitOrdinal ? retained : { ...retained, commitOrdinal };
    });
    const updatedOwner = updateTransactionRecord(owner, {
      pendingSegmentIds: retainedSegmentIds,
      updatedAt: owner.updatedAt,
    });
    this.#setTransaction(updatedOwner);
    this.#replaceSegments(rebasedSegments, [segment.id]);
    return true;
  }

  adoptAbortedSegment(input: AdoptAbortedSegmentInput): TransactionRecord {
    validateTimestampRuntime(input.updatedAt, "Segment adoption timestamp");
    const oldOwner = this.#transactions.get(input.expectedAbortedTransactionId);
    if (oldOwner?.revision !== input.expectedAbortedTransactionRevision) {
      throw new TransactionRecordConflictError(
        input.expectedAbortedTransactionId,
        input.expectedAbortedTransactionRevision,
        oldOwner?.revision ?? null,
      );
    }
    const replacement = this.#transactions.get(input.replacementTransactionId);
    if (replacement?.revision !== input.expectedReplacementTransactionRevision) {
      throw new TransactionRecordConflictError(
        input.replacementTransactionId,
        input.expectedReplacementTransactionRevision,
        replacement?.revision ?? null,
      );
    }
    if (oldOwner.status !== "aborted" || !oldOwner.pendingSegmentIds.includes(input.segment.id)) {
      throw new Error(`Segment ${input.segment.id} is not journaled by its aborted owner`);
    }
    if (replacement.status !== "active") {
      throw new Error(`Replacement transaction is not active: ${replacement.id}`);
    }
    const stored = this.#segments.get(input.segment.id);
    if (stored?.transactionId !== oldOwner.id) {
      throw new Error(`Segment ${input.segment.id} is not owned by the aborted transaction`);
    }
    const desired = validateSegmentRuntimeRecord(input.segment, `Segment ${input.segment.id}`);
    if (desired.transactionId !== replacement.id) {
      throw new Error(`Segment ${desired.id} does not name its replacement transaction`);
    }
    if (desired.commitOrdinal !== replacement.pendingSegmentIds.length) {
      throw new Error(`Replacement segment commit ordinal is not the next journal ordinal`);
    }
    const {
      transactionId: _storedOwner,
      commitOrdinal: _storedOrdinal,
      ...storedContent
    } = normalizeSegmentRecord(stored);
    const {
      transactionId: _desiredOwner,
      commitOrdinal: _desiredOrdinal,
      ...desiredContent
    } = desired;
    void _storedOwner;
    void _desiredOwner;
    void _storedOrdinal;
    void _desiredOrdinal;
    if (!deepRecordEqual(storedContent, desiredContent)) {
      throw new Error(`Replacement segment differs from stored segment: ${desired.id}`);
    }
    const journalBlocks = new Set(replacement.pendingBlockIds);
    for (const id of segmentBlockIds(desired)) {
      if (this.#isManifestBlockVisible(replacement.snapshotVersion, id)) journalBlocks.add(id);
    }
    this.#validateStagedSegment(desired, replacement.id, journalBlocks);
    for (const id of segmentBlockIds(desired)) {
      if (!this.#physical.hasBlock(id))
        throw new Error(`Replacement segment block is missing: ${id}`);
    }
    if (replacement.pendingSegmentIds.includes(desired.id)) {
      throw new Error(`Replacement transaction already journals segment: ${desired.id}`);
    }
    assertTransactionArtifactJournalLimits(replacement.pendingBlockIds, [
      ...replacement.pendingSegmentIds,
      desired.id,
    ]);
    for (const id of segmentBlockIds(stored)) {
      const provenance = this.#manifestBlocks.get(id);
      if (provenance !== undefined && this.#manifestBlockHasReadableVersion(provenance)) {
        throw new Error(`Segment ${stored.id} is already published`);
      }
    }
    for (const transaction of this.#transactions.values()) {
      if (transaction.id === oldOwner.id || transaction.id === replacement.id) continue;
      if (transaction.status === "active" && transaction.pendingSegmentIds.includes(stored.id)) {
        throw new Error(`Segment ${stored.id} is journaled by transaction ${transaction.id}`);
      }
    }
    const job = this.#compactionJobs.get(input.compactionJobId);
    if (
      job === undefined ||
      isTerminalCompactionJob(job) ||
      job.transactionId !== replacement.id ||
      !compactionOutputSegmentIds(job).includes(stored.id) ||
      job.sourceSegmentIds.includes(stored.id)
    ) {
      throw new Error(
        `Compaction job does not authorize segment adoption: ${input.compactionJobId}`,
      );
    }
    for (const other of this.#compactionJobs.values()) {
      if (other.id === job.id || isTerminalCompactionJob(other)) continue;
      if (
        other.sourceSegmentIds.includes(stored.id) ||
        compactionOutputSegmentIds(other).includes(stored.id)
      ) {
        throw new Error(`Segment ${stored.id} is referenced by compaction job ${other.id}`);
      }
    }
    const oldOwnerSegmentIds = oldOwner.pendingSegmentIds.filter((id) => id !== stored.id);
    const rebasedOldOwnerSegments = oldOwnerSegmentIds.map((id, commitOrdinal) => {
      const segment = this.#segments.get(id);
      if (segment?.transactionId !== oldOwner.id) {
        throw new Error(`Aborted transaction segment is missing or foreign: ${id}`);
      }
      return segment.commitOrdinal === commitOrdinal ? segment : { ...segment, commitOrdinal };
    });
    const updatedOldOwner = updateTransactionRecord(oldOwner, {
      pendingSegmentIds: oldOwnerSegmentIds,
      updatedAt: input.updatedAt,
    });
    const updatedReplacement = updateTransactionRecord(replacement, {
      pendingSegmentIds: [...replacement.pendingSegmentIds, stored.id],
      updatedAt: input.updatedAt,
    });
    // Both CAS results must be known admissible before changing the segment owner or either
    // journal. In particular, the replacement can cross a global staged-segment quota even
    // though removing the old aborted journal does not release active quota.
    this.#assertTransactionResourceTransition(updatedOldOwner);
    this.#assertTransactionResourceTransition(updatedReplacement);
    this.#replaceSegments([desired, ...rebasedOldOwnerSegments]);
    this.#setTransaction(updatedOldOwner);
    this.#setTransaction(updatedReplacement);
    return structuredClone(updatedReplacement);
  }

  reserveRowIds(tableId: string, count: number): RowIdRange {
    validateId(tableId);
    validateCount(count);
    if (!this.#tables.has(tableId)) throw new Error(`Row ID reservation has no table: ${tableId}`);
    const start = this.#nextRowIds.get(tableId) ?? 1n;
    const endExclusive = start + BigInt(count);
    assertCounterEndInRange(endExclusive, MAX_ROW_ID_EXCLUSIVE_END, "Row ID reservation");
    this.#nextRowIds.set(tableId, endExclusive);
    return { start, endExclusive };
  }

  reserveAutoIncrement(
    tableId: string,
    columnId: string,
    count: number,
    atLeast?: bigint,
  ): RowIdRange {
    this.#assertAutoIncrementColumn(tableId, columnId);
    validateAutoIncrementReservation(count, atLeast);
    return this.#reserveAutoIncrement(tableId, columnId, count, atLeast);
  }

  #reserveAutoIncrement(
    tableId: string,
    columnId: string,
    count: number,
    atLeast: bigint | undefined,
  ): RowIdRange {
    const key = `${tableId}/${columnId}`;
    const current = this.#nextAutoIncrement.get(key) ?? 1n;
    const floor = atLeast ?? 1n;
    const start = current > floor ? current : floor;
    const endExclusive = start + BigInt(count);
    assertCounterEndInRange(
      endExclusive,
      MAX_AUTO_INCREMENT_EXCLUSIVE_END,
      "Auto-increment reservation",
    );
    this.#nextAutoIncrement.set(key, endExclusive);
    return { start, endExclusive };
  }

  #assertAutoIncrementColumn(tableId: string, columnId: string): void {
    validateId(tableId);
    validateId(columnId);
    const column = this.#tables.get(tableId)?.columns.find((entry) => entry.id === columnId);
    if (column?.defaultValue?.kind !== "autoincrement") {
      throw new Error(`Auto-increment reservation has no declared column: ${tableId}/${columnId}`);
    }
  }

  getExistingUniqueKeys(tableId: string, keyTokens: readonly string[]): string[] {
    validateId(tableId);
    assertStorageBulkReadItems(keyTokens, "Unique-key lookup");
    const existing = this.#uniqueKeys.get(tableId);
    if (existing === undefined) return [];
    return [...new Set(keyTokens)].filter((token) => existing.has(token)).sort();
  }

  #uniqueBuildCatalog(
    buildId: string,
    tableId: string,
    indexId: string,
    namespaceId: string,
  ): { table: TableRecord; index: SecondaryIndexRecord } {
    validateId(buildId);
    validateId(tableId);
    validateId(indexId);
    validateId(namespaceId);
    const table = this.#tables.get(tableId);
    const index = table?.secondaryIndexes?.[indexId];
    if (
      table === undefined ||
      index?.unique !== true ||
      index.state !== "building" ||
      index.buildId !== buildId ||
      secondaryUniqueKeyNamespace(tableId, indexId) !== namespaceId
    ) {
      throw new UniqueKeyBuildConflictError(buildId, "catalog ownership changed");
    }
    return { table, index };
  }

  #setUniqueKeyBuild(next: UniqueKeyBuildState | undefined, buildId: string): void {
    const previous = this.#uniqueKeyBuilds.get(buildId);
    const previousActive = previous?.record.state === "active";
    const nextActive = next?.record.state === "active";
    const nextCount = this.#activeUniqueKeyBuildCount - Number(previousActive) + Number(nextActive);
    const nextBytes =
      this.#uniqueKeyBuildStagedBytes -
      (previous?.record.state === "active" ? previous.record.retainedBytes : 0) +
      (next?.record.state === "active" ? next.record.retainedBytes : 0);
    const nextEntries =
      this.#uniqueKeyBuildStagedEntries -
      (previous?.record.state === "active" ? previous.record.tokenCount : 0) +
      (next?.record.state === "active" ? next.record.tokenCount : 0);
    if (nextCount > MAX_ACTIVE_UNIQUE_KEY_BUILDS) {
      throw new StorageResourceLimitError(
        "unique-key build",
        nextCount,
        MAX_ACTIVE_UNIQUE_KEY_BUILDS,
      );
    }
    if (nextBytes > MAX_UNIQUE_KEY_BUILD_STAGED_BYTES_TOTAL) {
      throw new StorageResourceLimitError(
        "staged artifact byte",
        nextBytes,
        MAX_UNIQUE_KEY_BUILD_STAGED_BYTES_TOTAL,
      );
    }
    this.#activeUniqueKeyBuildCount = nextCount;
    this.#uniqueKeyBuildStagedBytes = nextBytes;
    this.#uniqueKeyBuildStagedEntries = nextEntries;
    if (next === undefined) this.#uniqueKeyBuilds.delete(buildId);
    else this.#uniqueKeyBuilds.set(buildId, next);
  }

  /** O(1) staged UNIQUE usage for enclosing adapters' cross-accelerator admission. */
  uniqueKeyBuildStagedUsage(): { bytes: number; entries: number } {
    return {
      bytes: this.#uniqueKeyBuildStagedBytes,
      entries: this.#uniqueKeyBuildStagedEntries,
    };
  }

  beginUniqueKeyBuild(input: BeginUniqueKeyBuildInput): UniqueKeyBuildRecord {
    validateId(input.ownerId);
    const { table: _table, index: _index } = this.#uniqueBuildCatalog(
      input.buildId,
      input.tableId,
      input.indexId,
      input.namespaceId,
    );
    void _table;
    void _index;
    validateBoundedExpiration(
      input.createdAt,
      input.expiresAt,
      "UNIQUE build",
      MAX_UNIQUE_KEY_BUILD_TTL_MS,
    );
    const current = this.#uniqueKeyBuilds.get(input.buildId);
    if (current?.record.state === "active" && current.record.expiresAt > input.createdAt) {
      const expected: UniqueKeyBuildRecord = {
        buildId: input.buildId,
        tableId: input.tableId,
        indexId: input.indexId,
        namespaceId: input.namespaceId,
        ownerId: input.ownerId,
        state: "active",
        nextOrdinal: 0,
        tokenCount: 0,
        retainedBytes: 0,
        expiresAt: input.expiresAt,
        createdAt: input.createdAt,
        updatedAt: input.createdAt,
      };
      if (deepRecordEqual(current.record, expected)) return structuredClone(current.record);
      throw new UniqueKeyBuildConflictError(input.buildId, "another live owner exists");
    }
    if (current?.record.state === "completed") {
      throw new UniqueKeyBuildConflictError(input.buildId, "the build is already completed");
    }
    for (const state of this.#uniqueKeyBuilds.values()) {
      if (
        state.record.buildId !== input.buildId &&
        state.record.namespaceId === input.namespaceId &&
        state.record.state === "active" &&
        state.record.expiresAt > input.createdAt
      ) {
        throw new UniqueKeyBuildConflictError(
          input.buildId,
          "the namespace has another live build",
        );
      }
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
      expiresAt: input.expiresAt,
      createdAt: input.createdAt,
      updatedAt: input.createdAt,
    };
    this.#setUniqueKeyBuild(
      { record, chunks: [], tokens: new Set(), completedInput: null },
      input.buildId,
    );
    return structuredClone(record);
  }

  getUniqueKeyBuild(buildId: string): UniqueKeyBuildRecord | undefined {
    validateId(buildId);
    const state = this.#uniqueKeyBuilds.get(buildId);
    return state === undefined ? undefined : structuredClone(state.record);
  }

  renewUniqueKeyBuild(input: RenewUniqueKeyBuildInput): UniqueKeyBuildRecord {
    validateId(input.buildId);
    validateId(input.ownerId);
    validateTimestampRuntime(input.updatedAt, "UNIQUE build renewal timestamp");
    validateBoundedExpiration(
      input.expiresAtCutoff,
      input.expiresAt,
      "UNIQUE build renewal",
      MAX_UNIQUE_KEY_BUILD_TTL_MS,
    );
    const state = this.#uniqueKeyBuilds.get(input.buildId);
    if (
      state?.record.state !== "active" ||
      state.record.ownerId !== input.ownerId ||
      state.record.expiresAt <= input.expiresAtCutoff
    ) {
      throw new UniqueKeyBuildConflictError(input.buildId, "ownership expired or changed");
    }
    if (input.expiresAt <= state.record.expiresAt) return structuredClone(state.record);
    const next: UniqueKeyBuildState = {
      ...state,
      record: { ...state.record, expiresAt: input.expiresAt, updatedAt: input.updatedAt },
    };
    this.#setUniqueKeyBuild(next, input.buildId);
    return structuredClone(next.record);
  }

  appendUniqueKeyBuildChunk(input: AppendUniqueKeyBuildChunkInput): UniqueKeyBuildRecord {
    validateId(input.buildId);
    validateId(input.ownerId);
    validateTimestampRuntime(input.expiresAtCutoff, "UNIQUE build append cutoff");
    validateTimestampRuntime(input.updatedAt, "UNIQUE build append timestamp");
    const retainedBytes = uniqueKeyBuildChunkRetainedBytes(input.keyTokens);
    if (!Number.isSafeInteger(input.ordinal) || input.ordinal < 0) {
      throw new TypeError("UNIQUE build chunk ordinal must be a non-negative safe integer");
    }
    const state = this.#uniqueKeyBuilds.get(input.buildId);
    if (
      state?.record.state !== "active" ||
      state.record.ownerId !== input.ownerId ||
      state.record.expiresAt <= input.expiresAtCutoff
    ) {
      throw new UniqueKeyBuildConflictError(input.buildId, "ownership expired or changed");
    }
    if (input.ordinal < state.record.nextOrdinal) {
      if (deepRecordEqual(state.chunks[input.ordinal], input.keyTokens)) {
        return structuredClone(state.record);
      }
      throw new UniqueKeyBuildConflictError(input.buildId, "chunk replay changed");
    }
    if (input.ordinal !== state.record.nextOrdinal) {
      throw new UniqueKeyBuildConflictError(input.buildId, "chunk ordinal is not contiguous");
    }
    for (const token of input.keyTokens) {
      if (state.tokens.has(token))
        throw new UniqueKeyConflictError(state.record.namespaceId, token);
    }
    const nextRetainedBytes = safeStorageSum(state.record.retainedBytes, retainedBytes);
    if (nextRetainedBytes > MAX_UNIQUE_KEY_BUILD_STAGED_BYTES) {
      throw new StorageResourceLimitError(
        "staged artifact byte",
        nextRetainedBytes,
        MAX_UNIQUE_KEY_BUILD_STAGED_BYTES,
      );
    }
    const nextTokens = new Set(state.tokens);
    for (const token of input.keyTokens) nextTokens.add(token);
    const next: UniqueKeyBuildState = {
      record: {
        ...state.record,
        nextOrdinal: safeWholeIncrement(state.record.nextOrdinal, "UNIQUE build chunk ordinal"),
        tokenCount: safeStorageSum(state.record.tokenCount, input.keyTokens.length),
        retainedBytes: nextRetainedBytes,
        updatedAt: input.updatedAt,
      },
      chunks: [...state.chunks, [...input.keyTokens]],
      tokens: nextTokens,
      completedInput: null,
    };
    this.#setUniqueKeyBuild(next, input.buildId);
    return structuredClone(next.record);
  }

  finishUniqueKeyBuild(input: FinishUniqueKeyBuildInput): TableRecord {
    validateId(input.buildId);
    validateId(input.ownerId);
    validateTimestampRuntime(input.expiresAtCutoff, "UNIQUE build finish cutoff");
    validateTimestampRuntime(input.completedAt, "UNIQUE build completion timestamp");
    const state = this.#uniqueKeyBuilds.get(input.buildId);
    if (state?.record.state === "completed") {
      if (!deepRecordEqual(state.completedInput, input)) {
        throw new UniqueKeyBuildConflictError(input.buildId, "completed retry changed");
      }
      const table = this.#tables.get(state.record.tableId);
      if (table === undefined)
        throw new UniqueKeyBuildConflictError(input.buildId, "table was removed");
      return structuredClone(table);
    }
    if (
      state?.record.state !== "active" ||
      state.record.ownerId !== input.ownerId ||
      state.record.expiresAt <= input.expiresAtCutoff
    ) {
      throw new UniqueKeyBuildConflictError(input.buildId, "ownership expired or changed");
    }
    const { table, index } = this.#uniqueBuildCatalog(
      state.record.buildId,
      state.record.tableId,
      state.record.indexId,
      state.record.namespaceId,
    );
    if (table.revision !== input.expectedTableRevision) {
      throw new TableRecordConflictError(table.id, input.expectedTableRevision, table.revision);
    }
    if (this.#currentVersion !== input.expectedManifestVersion) {
      throw new WriteConflictError(input.expectedManifestVersion, this.#currentVersion);
    }
    if (
      input.chunkCount !== state.record.nextOrdinal ||
      input.coversVersion !== (input.expectedManifestVersion ?? -1)
    ) {
      throw new UniqueKeyBuildConflictError(
        input.buildId,
        "completion does not match staged input",
      );
    }
    const { buildId: _buildId, ...readyBase } = index;
    void _buildId;
    const updated: TableRecord = {
      ...table,
      secondaryIndexes: {
        ...table.secondaryIndexes,
        [state.record.indexId]: {
          ...readyBase,
          state: "ready",
          uniqueEnforced: true,
          buildFromVersion: input.coversVersion,
        },
      },
      revision: safeWholeIncrement(table.revision, "Table revision"),
    };
    validateTableRuntimeRecord(updated, `Table ${updated.id}`);
    const nextCatalogEpoch = safeWholeIncrement(this.#catalogEpoch, "Catalog epoch");
    const nextSchemaEpoch = safeWholeIncrement(this.#schemaEpoch, "Schema epoch");
    const completedRecord: UniqueKeyBuildRecord = {
      ...state.record,
      state: "completed",
      retainedBytes: 0,
      updatedAt: input.completedAt,
      completedAt: input.completedAt,
    };
    this.#setTable(updated);
    this.#uniqueKeys.set(state.record.namespaceId, new OrderedStringSet(state.tokens));
    this.#setUniqueKeyBuild(
      {
        record: completedRecord,
        chunks: [],
        tokens: state.tokens,
        completedInput: structuredClone(input),
      },
      input.buildId,
    );
    this.#catalogEpoch = nextCatalogEpoch;
    this.#schemaEpoch = nextSchemaEpoch;
    return structuredClone(updated);
  }

  abortUniqueKeyBuild(input: AbortUniqueKeyBuildInput): boolean {
    validateId(input.buildId);
    validateId(input.ownerId);
    validateTimestampRuntime(input.expiresAtCutoff, "UNIQUE build abort cutoff");
    const state = this.#uniqueKeyBuilds.get(input.buildId);
    if (state === undefined) return false;
    if (state.record.state !== "active") {
      throw new UniqueKeyBuildConflictError(input.buildId, "the build is already completed");
    }
    if (state.record.ownerId !== input.ownerId && state.record.expiresAt > input.expiresAtCutoff) {
      throw new UniqueKeyBuildConflictError(input.buildId, "another live owner exists");
    }
    this.#setUniqueKeyBuild(undefined, input.buildId);
    return true;
  }

  getCurrentManifestVersion(): number | null {
    return this.#currentVersion;
  }

  #isManifestBlockVisible(version: number | null, id: string): boolean {
    if (version === null) return false;
    const record = this.#manifestBlocks.get(id);
    return record !== undefined && manifestBlockVisibleAt(record, version);
  }

  #manifestBlockHasReadableVersion(record: ManifestBlockRecord): boolean {
    for (const version of this.#roots.readableManifestVersions.after(record.addedVersion - 1)) {
      return record.removedVersion === null || version < record.removedVersion;
    }
    return false;
  }

  #blockHasReadableManifest(id: string): boolean {
    const record = this.#manifestBlocks.get(id);
    return record !== undefined && this.#manifestBlockHasReadableVersion(record);
  }

  getCatalogProbe(): CatalogProbe {
    return {
      manifestVersion: this.#currentVersion,
      catalogEpoch: this.#catalogEpoch,
      schemaEpoch: this.#schemaEpoch,
    };
  }

  hasManifestBlocks(version: number | null, ids: readonly string[]): boolean[] {
    if (version !== null && (!Number.isSafeInteger(version) || version < 0)) {
      throw new TypeError("Manifest version must be a non-negative safe integer or null");
    }
    const rawIds: unknown = ids;
    if (!Array.isArray(rawIds) || rawIds.length > MAX_MANIFEST_BLOCK_PRESENCE_IDS) {
      throw new RangeError(
        `Manifest block presence accepts at most ${String(MAX_MANIFEST_BLOCK_PRESENCE_IDS)} ids`,
      );
    }
    for (const id of rawIds as unknown[]) {
      if (typeof id !== "string") throw new TypeError("Manifest block ID must be a string");
      validateId(id);
    }
    const validatedIds = rawIds as unknown as readonly string[];
    if (version === null) return validatedIds.map(() => false);
    const manifest = this.#manifests.get(version);
    if (manifest === undefined || manifest.prunedAt !== undefined) {
      return validatedIds.map(() => false);
    }
    return validatedIds.map((id) => this.#isManifestBlockVisible(version, id));
  }

  listManifestBlockPage(input: ListManifestBlockPageInput): ManifestBlockPage {
    if (!Number.isSafeInteger(input.version) || input.version < 0) {
      throw new TypeError("Manifest version must be a non-negative safe integer");
    }
    if (input.afterBlockId !== null) validateId(input.afterBlockId);
    validatePageLimit(input.limit);
    // Summary tombstones are deliberately removed in bounded passes after pruning, while the
    // per-block provenance intervals remain until their payloads are reclaimed. Maintenance and
    // snapshot capture must therefore be able to continue an exact-version cursor after the
    // summary disappeared; requiring a readable Manifest here loses the only bounded route to
    // the remaining obsolete IDs.
    if (this.#currentVersion === null || input.version > this.#currentVersion) {
      return { records: [], nextCursor: null };
    }
    const records: Array<Pick<ManifestBlockRecord, "blockId" | "byteLength" | "checksum">> = [];
    for (const record of this.#manifestBlocks.orderedValues(input.afterBlockId)) {
      if (!manifestBlockVisibleAt(record, input.version)) continue;
      records.push({
        blockId: record.blockId,
        byteLength: record.byteLength,
        checksum: record.checksum,
      });
      if (records.length === input.limit) break;
    }
    return {
      records,
      nextCursor: records.length === input.limit ? (records.at(-1)?.blockId ?? null) : null,
    };
  }

  listRetiredManifestBlockPage(input: ListRetiredManifestBlockPageInput): ManifestBlockPage {
    if (!Number.isSafeInteger(input.removedThroughVersion) || input.removedThroughVersion < 0) {
      throw new TypeError("Retired manifest block cutoff must be a non-negative safe integer");
    }
    if (input.afterBlockId !== null) validateId(input.afterBlockId);
    validatePageLimit(input.limit);
    const records: Array<Pick<ManifestBlockRecord, "blockId" | "byteLength" | "checksum">> = [];
    for (const record of this.#manifestBlocks.orderedValues(input.afterBlockId)) {
      if (record.removedVersion === null || record.removedVersion > input.removedThroughVersion) {
        continue;
      }
      records.push({
        blockId: record.blockId,
        byteLength: record.byteLength,
        checksum: record.checksum,
      });
      if (records.length === input.limit) break;
    }
    return {
      records,
      nextCursor: records.length === input.limit ? (records.at(-1)?.blockId ?? null) : null,
    };
  }

  getCurrentManifest(): Manifest | undefined {
    const manifest =
      this.#currentVersion === null ? undefined : this.#manifests.get(this.#currentVersion);
    return manifest === undefined ? undefined : structuredClone(manifest);
  }

  getManifest(version: number): Manifest | undefined {
    const manifest = this.#manifests.get(version);
    return manifest === undefined ? undefined : structuredClone(manifest);
  }

  listManifestPage(afterVersion: number | null, limit: number): StoragePage<Manifest, number> {
    validatePageLimit(limit);
    const records = boundedRecordPage(this.#manifests, afterVersion, limit).map((manifest) =>
      structuredClone(manifest),
    );
    return {
      records,
      nextCursor: records.length === limit ? (records.at(-1)?.version ?? null) : null,
    };
  }

  /** Removes only bounded summary tombstones; block discovery lives in provenance intervals. */
  removePrunedManifestRecords(maxItems: number): number {
    boundedMaintenanceBatchItems(maxItems, "Pruned manifest removal limit");
    const earliestReadable = this.#roots.readableManifestVersions.after(null).next();
    const safeBelow = earliestReadable.done ? Number.POSITIVE_INFINITY : earliestReadable.value;
    const selected: Manifest[] = [];
    let visited = 0;
    for (const version of this.#roots.prunedManifestVersions.after(
      this.#prunedManifestRemovalCursor,
    )) {
      this.#prunedManifestRemovalCursor = version;
      visited += 1;
      const manifest = this.#manifests.get(version);
      if (manifest !== undefined && version < safeBelow) {
        selected.push(manifest);
      }
      if (visited === maxItems) break;
    }
    if (visited < maxItems) this.#prunedManifestRemovalCursor = null;
    this.#replaceManifests(
      [],
      selected.map((manifest) => manifest.version),
    );
    return selected.length;
  }

  beginTransaction(input: BeginTransactionInput): BeginTransactionResult {
    validateBeginTransactionInput(input);
    const pending = input.pendingTable;
    if (pending !== undefined) {
      validateTableRuntimeRecord(pending.record, "Pending table");
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
      if (pending.expectedCatalogEpoch !== this.#catalogEpoch) {
        throw new TableRecordConflictError(pending.record.id, 0, null);
      }
      if (this.#tables.has(pending.record.id)) {
        throw new Error(`Table already exists: ${pending.record.id}`);
      }
      if (this.#tableIdsByName.has(pending.record.name)) {
        throw new Error(`Table name already exists: ${pending.record.name}`);
      }
      for (const transaction of this.#transactions.values()) {
        if (transaction.status !== "active" || transaction.pendingTable === undefined) continue;
        if (transaction.pendingTable.id === pending.record.id) {
          throw new Error(`Table already exists: ${pending.record.id}`);
        }
        if (transaction.pendingTable.name === pending.record.name) {
          throw new Error(`Table name already exists: ${pending.record.name}`);
        }
      }
      this.#assertTableForeignKeys(pending.record);
    }
    const record: TransactionRecord = {
      ...structuredClone(input.record),
      snapshotVersion: this.#currentVersion,
      schemaEpochGuard: this.#schemaEpoch,
      ...(pending === undefined
        ? {}
        : {
            pendingTable: structuredClone(pending.record),
            pendingTableNextRowId: pending.nextRowId,
            catalogEpochGuard: pending.expectedCatalogEpoch,
          }),
    };
    validateTransactionRuntimeRecord(record, "Transaction");
    this.#assertSnapshotAvailable(record.snapshotVersion);
    if (this.#transactions.has(record.id)) {
      throw new Error(`Transaction already exists: ${record.id}`);
    }
    // Admission cleanup is only needed when the durable active-owner quota is full. Sweeping on
    // every begin races the explicit recovery manager: it can mark an expired journal terminal
    // before recovery has durably nominated its staged artifacts for collection. Below the cap,
    // leave expiry reconciliation to recovery so abort + artifact discovery remains one logical
    // operation.
    if (this.#activeTransactionCount >= MAX_ACTIVE_TRANSACTIONS) {
      this.#sweepExpiredTransactions(record.startedAt);
    }
    let rowIds: RowIdRange | undefined;
    let rowCounterKey: string | undefined;
    if (input.reserveRowIds !== undefined) {
      validateId(input.reserveRowIds.tableId);
      validateCount(input.reserveRowIds.count);
      if (!this.#tables.has(input.reserveRowIds.tableId)) {
        throw new Error(`Row ID reservation has no table: ${input.reserveRowIds.tableId}`);
      }
      const current = this.#nextRowIds.get(input.reserveRowIds.tableId) ?? 1n;
      const endExclusive = current + BigInt(input.reserveRowIds.count);
      assertCounterEndInRange(endExclusive, MAX_ROW_ID_EXCLUSIVE_END, "Row ID reservation");
      rowCounterKey = input.reserveRowIds.tableId;
      rowIds = { start: current, endExclusive };
    }
    let autoIncrementValues: RowIdRange | undefined;
    let autoCounterKey: string | undefined;
    if (input.reserveAutoIncrement !== undefined) {
      const { tableId, columnId, count, atLeast } = input.reserveAutoIncrement;
      this.#assertAutoIncrementColumn(tableId, columnId);
      validateAutoIncrementReservation(count, atLeast);
      autoCounterKey = `${tableId}/${columnId}`;
      const current = this.#nextAutoIncrement.get(autoCounterKey) ?? 1n;
      const floor = atLeast ?? 1n;
      const start = current > floor ? current : floor;
      autoIncrementValues = { start, endExclusive: start + BigInt(count) };
      assertCounterEndInRange(
        autoIncrementValues.endExclusive,
        MAX_AUTO_INCREMENT_EXCLUSIVE_END,
        "Auto-increment reservation",
      );
    }
    this.#setTransaction(record);
    if (rowCounterKey !== undefined && rowIds !== undefined) {
      this.#nextRowIds.set(rowCounterKey, rowIds.endExclusive);
    }
    if (autoCounterKey !== undefined && autoIncrementValues !== undefined) {
      this.#nextAutoIncrement.set(autoCounterKey, autoIncrementValues.endExclusive);
    }
    return {
      record: structuredClone(record),
      ...(rowIds === undefined ? {} : { rowIds }),
      ...(autoIncrementValues === undefined ? {} : { autoIncrementValues }),
    };
  }

  createTransaction(record: TransactionRecord): void {
    const normalized: TransactionRecord = {
      ...structuredClone(record),
      ...(record.status === "active" && record.schemaEpochGuard === undefined
        ? { schemaEpochGuard: this.#schemaEpoch }
        : {}),
    };
    validateTransactionRuntimeRecord(normalized, "Transaction");
    if (normalized.pendingBlockIds.length > 0 || normalized.pendingSegmentIds.length > 0) {
      throw new TypeError("A fresh transaction cannot begin with pending artifacts");
    }
    if (
      normalized.pendingTable !== undefined ||
      normalized.pendingTableNextRowId !== undefined ||
      normalized.catalogEpochGuard !== undefined
    ) {
      throw new TypeError("Pending tables must be reserved through beginTransaction");
    }
    if (normalized.schemaEpochGuard !== this.#schemaEpoch) {
      throw new SchemaConflictError(normalized.schemaEpochGuard ?? -1, this.#schemaEpoch);
    }
    if (this.#transactions.has(normalized.id)) {
      throw new Error(`Transaction already exists: ${normalized.id}`);
    }
    if (normalized.status === "active" && normalized.snapshotVersion !== null) {
      this.#assertPinnedRetiredLimits(normalized.startedAt, undefined, undefined, [
        normalized.snapshotVersion,
      ]);
    }
    if (this.#activeTransactionCount >= MAX_ACTIVE_TRANSACTIONS) {
      this.#sweepExpiredTransactions(normalized.startedAt);
    }
    this.#assertSnapshotAvailable(normalized.snapshotVersion);
    this.#assertPendingArtifactsAvailable(normalized);
    const pendingBlocks = new Set(normalized.pendingBlockIds);
    for (const id of normalized.pendingSegmentIds) {
      const segment = this.#segments.get(id);
      if (segment === undefined)
        throw new Error(`Transaction references missing pending segment: ${id}`);
      this.#validateStagedSegment(segment, normalized.id, pendingBlocks);
    }
    this.#setTransaction(normalized);
  }

  getTransaction(id: string): TransactionRecord | undefined {
    const record = this.#transactions.get(id);
    return record === undefined ? undefined : structuredClone(record);
  }

  getTransactions(ids: readonly string[]): Array<TransactionRecord | undefined> {
    assertStorageBulkReadItems(ids, "Transaction read");
    for (const id of ids) validateId(id);
    return ids.map((id) => {
      const record = this.#transactions.get(id);
      return record === undefined ? undefined : structuredClone(record);
    });
  }

  listTransactionPage(
    afterId: string | null,
    limit: number,
  ): StoragePage<TransactionRecord, string> {
    validatePageLimit(limit);
    const records = boundedRecordPage(this.#transactions, afterId, limit).map((record) =>
      structuredClone(record),
    );
    return { records, nextCursor: records.length === limit ? (records.at(-1)?.id ?? null) : null };
  }

  updateTransaction(
    id: string,
    expectedRevision: number,
    update: TransactionRecordUpdate,
  ): TransactionRecord {
    const current = this.#transactions.get(id);
    if (current?.revision !== expectedRevision) {
      throw new TransactionRecordConflictError(id, expectedRevision, current?.revision ?? null);
    }
    assertGenericTransactionUpdateAllowed(current, update);
    const updated = updateTransactionRecord(current, update);
    if (update.snapshotVersion !== undefined) {
      this.#assertSnapshotAvailable(updated.snapshotVersion);
    }
    this.#assertPendingArtifactsAvailable(
      updated,
      update.pendingBlockIds !== undefined,
      update.pendingSegmentIds !== undefined,
    );
    this.#setTransaction(updated);
    return structuredClone(updated);
  }

  renewTransaction(input: RenewTransactionInput): boolean {
    validateId(input.transactionId);
    validateId(input.ownerId);
    const cutoff = validateBoundedExpiration(
      input.expiresAtCutoff,
      input.expiresAt,
      "Transaction renewal",
      MAX_LEASE_TTL_MS,
    );
    const current = this.#transactions.get(input.transactionId);
    if (
      current?.status !== "active" ||
      current.ownerId !== input.ownerId ||
      Date.parse(current.expiresAt) <= cutoff
    ) {
      return false;
    }
    this.#assertPinnedManifestLag(current.snapshotVersion);
    if (Date.parse(input.expiresAt) <= Date.parse(current.expiresAt)) return true;
    this.#setTransaction({ ...current, expiresAt: input.expiresAt });
    return true;
  }

  abortTransactionIfExpired(input: AbortTransactionIfExpiredInput): TransactionRecord | undefined {
    validateId(input.transactionId);
    validateId(input.expectedOwnerId);
    validateTimestampRuntime(input.expiresAtCutoff, "Transaction expiration cutoff");
    validateTimestampRuntime(input.updatedAt, "Transaction abort timestamp");
    const current = this.#transactions.get(input.transactionId);
    if (
      current?.status !== "active" ||
      current.ownerId !== input.expectedOwnerId ||
      Date.parse(current.expiresAt) > Date.parse(input.expiresAtCutoff)
    ) {
      return undefined;
    }
    const updated = updateTransactionRecord(current, {
      status: "aborted",
      updatedAt: input.updatedAt,
    });
    this.#setTransaction(updated);
    return structuredClone(updated);
  }

  /**
   * Validates and applies the record half of staging: the journal update and the segment
   * records. Block bytes are the enclosing store's to write — after this returns, in the same
   * atomic step.
   */
  stageTransactionArtifacts(
    input: StageTransactionArtifactsInput,
    options: {
      /**
       * The enclosing store already ran the duplicate check and wrote the bytes — the OPFS
       * store writes block files before its log entry, so at apply time the files exist and
       * the check would misread them as duplicates.
       */
      blocksPrevalidated?: boolean;
      /** Exact persisted byte lengths when prevalidated adapter bodies omit payload bytes. */
      blockByteLengths?: ReadonlyMap<string, number>;
    } = {},
  ): TransactionRecord {
    const plan = this.#planTransactionArtifactStage(input, options);
    const blockByteLengths = new Map(
      input.blocks.map((block) => [
        block.id,
        options.blockByteLengths?.get(block.id) ?? block.bytes.byteLength,
      ]),
    );
    this.#assertTransactionResourceTransition(plan.updated, blockByteLengths);
    this.#replaceSegments(plan.segments);
    this.#setTransaction(plan.updated, blockByteLengths);
    return structuredClone(plan.updated);
  }

  /**
   * Runs the complete staging preflight without changing record state. Persistent adapters use
   * this before allocating payload storage; the real apply repeats it under the same writer lock.
   */
  preflightTransactionArtifactStage(input: StageTransactionArtifactsInput): void {
    const plan = this.#planTransactionArtifactStage(input, {});
    this.#assertTransactionResourceTransition(
      plan.updated,
      new Map(input.blocks.map((block) => [block.id, block.bytes.byteLength])),
    );
  }

  #planTransactionArtifactStage(
    input: StageTransactionArtifactsInput,
    options: { blocksPrevalidated?: boolean; blockByteLengths?: ReadonlyMap<string, number> },
  ): StageTransactionArtifactsPlan {
    // CAS/status precedence is part of the public contract. A stale caller must not learn an
    // artifact validation error from state it no longer owns.
    const current = this.#transactions.get(input.transactionId);
    if (current?.revision !== input.expectedRevision) {
      throw new TransactionRecordConflictError(
        input.transactionId,
        input.expectedRevision,
        current?.revision ?? null,
      );
    }
    assertGenericTransactionUpdateAllowed(current, { updatedAt: input.updatedAt });
    validateTimestampRuntime(input.updatedAt, "Transaction staging timestamp");
    assertTransactionArtifactBatchLimits(input.blocks, input.segments);
    const pendingBlockIds = [...current.pendingBlockIds, ...input.blocks.map((block) => block.id)];
    const pendingSegmentIds = [
      ...current.pendingSegmentIds,
      ...input.segments.map((segment) => segment.id),
    ];
    assertTransactionArtifactJournalLimits(pendingBlockIds, pendingSegmentIds);
    const inputBlockIds = new Set<string>();
    for (const block of input.blocks) {
      validateId(block.id);
      validateBlockWriteBytes(block.bytes);
      if (
        inputBlockIds.has(block.id) ||
        (options.blocksPrevalidated !== true && this.#physical.hasBlock(block.id))
      ) {
        throw new Error(`Block already exists: ${block.id}`);
      }
      inputBlockIds.add(block.id);
    }
    const stagedBlockIds = new Set([
      ...current.pendingBlockIds,
      ...input.blocks.map((block) => block.id),
    ]);
    const segmentIds = new Set<string>();
    const normalizedSegments: SegmentRecord[] = [];
    for (const segment of input.segments) {
      if (segment.transactionId !== input.transactionId) {
        throw new Error(`Segment ${segment.id} belongs to another transaction`);
      }
      if (segmentIds.has(segment.id) || this.#segments.has(segment.id)) {
        throw new Error(`Segment already exists: ${segment.id}`);
      }
      segmentIds.add(segment.id);
      const normalized = validateSegmentRuntimeRecord(segment, `Segment ${segment.id}`);
      const expectedCommitOrdinal = current.pendingSegmentIds.length + normalizedSegments.length;
      if (normalized.commitOrdinal !== expectedCommitOrdinal) {
        throw new TypeError(
          `Segment ${segment.id} commit ordinal must be ${String(expectedCommitOrdinal)}`,
        );
      }
      const availableBlockIds = new Set(stagedBlockIds);
      for (const id of segmentBlockIds(normalized)) {
        if (this.#isManifestBlockVisible(current.snapshotVersion, id)) availableBlockIds.add(id);
      }
      this.#validateStagedSegment(normalized, input.transactionId, availableBlockIds);
      normalizedSegments.push(normalized);
    }
    const update: TransactionRecordUpdate = {
      pendingBlockIds,
      pendingSegmentIds,
      updatedAt: input.updatedAt,
    };
    if (current.pendingTable !== undefined) {
      let nextRowId = current.pendingTableNextRowId;
      if (nextRowId === undefined) throw new Error("Pending table row-ID state is missing");
      for (const segment of normalizedSegments) {
        if (segment.tableId !== current.pendingTable.id || segment.level !== 0) {
          throw new TypeError("A pending-table transaction can stage only its own level-zero rows");
        }
        if (segment.kind !== "insert" || segment.rowIdStart !== nextRowId) {
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
    // Only previously journaled artifacts need existence checks; the new ones land with the
    // journal update in this same atomic step.
    this.#assertPendingArtifactsAvailable(current, true, true);
    this.#segmentUsageAfter(normalizedSegments);
    return { updated, segments: normalizedSegments };
  }

  /** Validates and applies the record/segment half of an atomic savepoint rewind. */
  rollbackTransactionArtifacts(input: RollbackTransactionArtifactsInput): TransactionRecord {
    const current = this.#transactions.get(input.transactionId);
    if (current?.revision !== input.expectedRevision) {
      throw new TransactionRecordConflictError(
        input.transactionId,
        input.expectedRevision,
        current?.revision ?? null,
      );
    }
    if (
      !exactStringPartition(current.pendingBlockIds, input.pendingBlockIds, input.removeBlockIds) ||
      !exactStringPartition(
        current.pendingSegmentIds,
        input.pendingSegmentIds,
        input.removeSegmentIds,
      )
    ) {
      throw new TypeError("Transaction rollback artifacts do not match its current journal");
    }
    input.pendingSegmentIds.forEach((id, commitOrdinal) => {
      const segment = this.#segments.get(id);
      if (segment === undefined) throw new Error(`Transaction segment is missing: ${id}`);
      if (segment.commitOrdinal !== commitOrdinal) {
        throw new TypeError("Transaction rollback must retain a commit-ordinal prefix");
      }
    });
    for (const id of input.removeBlockIds) {
      if (!this.#physical.hasBlock(id)) throw new Error(`Transaction block is missing: ${id}`);
    }
    for (const id of input.removeSegmentIds) {
      const segment = this.#segments.get(id);
      if (segment === undefined) throw new Error(`Transaction segment is missing: ${id}`);
      if (segment.transactionId !== input.transactionId) {
        throw new Error(`Transaction segment belongs to another transaction: ${id}`);
      }
    }
    const removedSegments = new Set(input.removeSegmentIds);
    const removedBlocks = new Set(input.removeBlockIds);
    for (const id of removedBlocks) {
      const provenance = this.#manifestBlocks.get(id);
      if (provenance !== undefined && this.#manifestBlockHasReadableVersion(provenance)) {
        throw new Error(`Transaction block is still reachable from a readable manifest: ${id}`);
      }
    }
    for (const segment of this.#segments.values()) {
      if (removedSegments.has(segment.id)) continue;
      for (const id of Object.values(segment.columnBlockIds).flat()) {
        if (removedBlocks.has(id)) {
          throw new Error(`Transaction block is still reachable from segment ${segment.id}: ${id}`);
        }
      }
    }
    for (const transaction of this.#transactions.values()) {
      if (transaction.id === input.transactionId) continue;
      for (const id of transaction.pendingBlockIds) {
        if (removedBlocks.has(id)) {
          throw new Error(`Transaction block is still journaled by ${transaction.id}: ${id}`);
        }
      }
    }
    for (const job of this.#compactionJobs.values()) {
      const referencedBlock = [...job.sourceBlockIds, ...job.outputBlockIds].find((id) =>
        removedBlocks.has(id),
      );
      const referencedSegment = [...job.sourceSegmentIds, ...compactionOutputSegmentIds(job)].find(
        (id) => removedSegments.has(id),
      );
      if (referencedBlock !== undefined || referencedSegment !== undefined) {
        throw new Error(
          `Transaction rollback artifact is referenced by compaction job ${job.id}: ` +
            String(referencedBlock ?? referencedSegment),
        );
      }
    }
    for (const job of this.#garbageCollectionJobs.values()) {
      const referencedBlock = job.candidateBlockIds.find((id) => removedBlocks.has(id));
      const referencedSegment = job.candidateSegmentIds.find((id) => removedSegments.has(id));
      if (referencedBlock !== undefined || referencedSegment !== undefined) {
        throw new Error(
          `Transaction rollback artifact is referenced by garbage collection job ${job.id}: ` +
            String(referencedBlock ?? referencedSegment),
        );
      }
    }
    const update: TransactionRecordUpdate = {
      pendingBlockIds: input.pendingBlockIds,
      pendingSegmentIds: input.pendingSegmentIds,
      updatedAt: input.updatedAt,
    };
    if (current.pendingTable !== undefined) {
      let nextRowId = 1n;
      const retained = input.pendingSegmentIds
        .map((id) => this.#segments.get(id))
        .filter(
          (segment): segment is SegmentRecord => segment?.tableId === current.pendingTable?.id,
        );
      for (const segment of retained) {
        if (segment.kind !== "insert" || segment.rowIdStart !== nextRowId) {
          throw new TypeError("Retained pending-table segments have a discontinuous row-ID range");
        }
        nextRowId = segment.rowIdEndExclusive;
      }
      update.pendingTableNextRowId = nextRowId;
    }
    assertGenericTransactionUpdateAllowed(current, update);
    const updated = updateTransactionRecord(current, update);
    this.#replaceSegments([], input.removeSegmentIds);
    this.#setTransaction(updated);
    return structuredClone(updated);
  }

  #validateStagedSegment(
    segment: SegmentRecord,
    transactionId: string,
    journalBlockIds: ReadonlySet<string>,
  ): void {
    if (segment.transactionId !== transactionId) {
      throw new Error(`Segment ${segment.id} belongs to another transaction`);
    }
    if (segmentBlockIds(segment).length === 0) {
      throw new TypeError(`Segment ${segment.id} must reference at least one block`);
    }
    const owner = this.#transactions.get(transactionId);
    const table =
      this.#tables.get(segment.tableId) ??
      (owner?.status === "active" && owner.pendingTable?.id === segment.tableId
        ? owner.pendingTable
        : undefined);
    if (table === undefined) throw new Error(`Segment ${segment.id} references missing table`);
    const columns = new Set(table.columns.map((column) => column.id));
    for (const [columnId, blockIds] of Object.entries(segment.columnBlockIds)) {
      if (!columns.has(columnId)) {
        throw new Error(`Segment ${segment.id} references unknown column: ${columnId}`);
      }
      for (const id of blockIds) {
        if (!journalBlockIds.has(id)) {
          throw new Error(`Segment ${segment.id} references block outside its transaction: ${id}`);
        }
      }
    }
  }

  commitTransaction(input: CommitTransactionInput): ManifestSummary {
    const transaction = this.#transactions.get(input.transactionId);
    if (
      transaction?.revision !== input.expectedTransactionRevision ||
      transaction.status !== "active"
    ) {
      throw new TransactionRecordConflictError(
        input.transactionId,
        input.expectedTransactionRevision,
        transaction?.revision ?? null,
      );
    }
    const plan = this.#planCommit(
      transaction,
      input,
      (id) => this.#physical.hasBlock(id),
      (id) => this.#physical.blockByteLength(id),
      (id) => this.#physical.blockChecksum?.(id),
      (id) => this.#segments.get(id),
    );
    return this.#applyCommit(plan);
  }

  /**
   * The single-shot write: begin or continue a transaction, journal the artifacts, and commit
   * — validated in full before anything mutates, so a refusal anywhere (a stale revision, a
   * moved manifest, a duplicate key) leaves the core exactly as it was, fresh record included.
   * Block bytes are the enclosing store's to write afterwards, in the same atomic step.
   */
  writeTransaction(
    input: WriteTransactionInput,
    options: {
      /** As on `stageTransactionArtifacts`: the enclosing store already checked the block ids. */
      blocksPrevalidated?: boolean;
      /** Exact persisted lengths when an adapter replays placement-only block bodies. */
      blockByteLengths?: ReadonlyMap<string, number>;
      /** Exact persisted checksums when an adapter replays placement-only block bodies. */
      blockChecksums?: ReadonlyMap<string, number>;
    } = {},
  ): ManifestSummary {
    const plan = this.#planWriteTransaction(input, options);
    this.#replaceSegments([...plan.segments.values()]);
    return this.#applyCommit(plan.commit);
  }

  /** Complete, non-mutating preflight used before a persistent adapter allocates block bytes. */
  preflightWriteTransaction(input: WriteTransactionInput): void {
    this.#planWriteTransaction(input, {});
  }

  #planWriteTransaction(
    input: WriteTransactionInput,
    options: {
      blocksPrevalidated?: boolean;
      blockByteLengths?: ReadonlyMap<string, number>;
      blockChecksums?: ReadonlyMap<string, number>;
    },
  ): WriteTransactionPlan {
    // Preserve CAS precedence before inspecting any caller-owned artifact bytes/records.
    if (!("record" in input.transaction)) {
      const current = this.#transactions.get(input.transaction.id);
      if (current?.revision !== input.transaction.expectedRevision) {
        throw new TransactionRecordConflictError(
          input.transaction.id,
          input.transaction.expectedRevision,
          current?.revision ?? null,
        );
      }
    }
    assertTransactionArtifactBatchLimits(input.blocks, input.segments);
    const blockIds = new Set<string>();
    for (const block of input.blocks) {
      validateId(block.id);
      validateBlockWriteBytes(block.bytes);
      if (blockIds.has(block.id)) throw new Error(`Block already exists: ${block.id}`);
      if (options.blocksPrevalidated !== true) {
        if (this.#physical.hasBlock(block.id)) {
          throw new Error(`Block already exists: ${block.id}`);
        }
      }
      blockIds.add(block.id);
    }
    const segments = new Map<string, SegmentRecord>();
    for (const segment of input.segments) {
      if (segments.has(segment.id) || this.#segments.has(segment.id)) {
        throw new Error(`Segment already exists: ${segment.id}`);
      }
      segments.set(segment.id, validateSegmentRuntimeRecord(segment, `Segment ${segment.id}`));
    }
    let base: TransactionRecord;
    if ("record" in input.transaction) {
      if (
        input.transaction.record.pendingBlockIds.length > 0 ||
        input.transaction.record.pendingSegmentIds.length > 0
      ) {
        throw new TypeError("A fresh transaction cannot begin with pending artifacts");
      }
      if (this.#transactions.has(input.transaction.record.id)) {
        throw new Error(`Transaction already exists: ${input.transaction.record.id}`);
      }
      if (this.#currentVersion !== input.expectedManifestVersion) {
        throw new WriteConflictError(input.expectedManifestVersion, this.#currentVersion);
      }
      base = {
        ...structuredClone(input.transaction.record),
        snapshotVersion: input.expectedManifestVersion,
      };
      validateTransactionRuntimeRecord(base, "Transaction");
      this.#assertSnapshotAvailable(base.snapshotVersion);
    } else {
      const current = this.#transactions.get(input.transaction.id);
      if (current?.revision !== input.transaction.expectedRevision) {
        throw new TransactionRecordConflictError(
          input.transaction.id,
          input.transaction.expectedRevision,
          current?.revision ?? null,
        );
      }
      base = current;
    }
    const pendingBlockIds = [...base.pendingBlockIds, ...blockIds];
    const pendingSegmentIds = [...base.pendingSegmentIds, ...segments.keys()];
    assertTransactionArtifactJournalLimits(pendingBlockIds, pendingSegmentIds);
    let segmentIndex = 0;
    for (const segment of segments.values()) {
      const expectedCommitOrdinal = base.pendingSegmentIds.length + segmentIndex;
      if (segment.commitOrdinal !== expectedCommitOrdinal) {
        throw new TypeError(
          `Segment ${segment.id} commit ordinal must be ${String(expectedCommitOrdinal)}`,
        );
      }
      segmentIndex += 1;
    }
    const update: TransactionRecordUpdate = {
      pendingBlockIds,
      pendingSegmentIds,
      updatedAt: input.committedAt,
    };
    if (base.pendingTable !== undefined) {
      let nextRowId = base.pendingTableNextRowId;
      if (nextRowId === undefined) throw new Error("Pending table row-ID state is missing");
      for (const segment of segments.values()) {
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
      update.pendingTableNextRowId = nextRowId;
    }
    assertGenericTransactionUpdateAllowed(base, update);
    const staged = updateTransactionRecord(base, update);
    const stagedBlockIds = new Set(staged.pendingBlockIds);
    for (const segment of segments.values()) {
      this.#validateStagedSegment(segment, staged.id, stagedBlockIds);
    }
    // Previously journaled artifacts must exist; the ones staged here land with the commit.
    this.#assertPendingArtifactsAvailable(base, true, true);
    const commit = this.#planCommit(
      staged,
      input,
      (id) => blockIds.has(id) || this.#physical.hasBlock(id),
      (id) =>
        options.blockByteLengths?.get(id) ??
        input.blocks.find((block) => block.id === id)?.bytes.byteLength ??
        this.#physical.blockByteLength(id),
      (id) => {
        const persisted = options.blockChecksums?.get(id);
        if (persisted !== undefined) return persisted;
        const block = input.blocks.find((candidate) => candidate.id === id);
        return block === undefined ? this.#physical.blockChecksum?.(id) : crc32(block.bytes);
      },
      (id) => segments.get(id) ?? this.#segments.get(id),
    );
    this.#segmentUsageAfter([...segments.values()]);
    return { staged, segments, commit };
  }

  /**
   * Everything a commit checks, against a transaction record the caller already resolved, with
   * the artifact lookups injected so the single-shot write can count its not-yet-stored blocks
   * and segments as present. Pure: mutations happen in `#applyCommit`.
   */
  #planCommit(
    transaction: TransactionRecord,
    input: Omit<CommitTransactionInput, "transactionId" | "expectedTransactionRevision">,
    hasBlock: (id: string) => boolean,
    blockByteLength: (id: string) => number | undefined,
    blockChecksum: (id: string) => number | undefined,
    getSegment: (id: string) => SegmentRecord | undefined,
  ): CommitPlan {
    if (transaction.schemaEpochGuard !== this.#schemaEpoch) {
      throw new SchemaConflictError(transaction.schemaEpochGuard ?? -1, this.#schemaEpoch);
    }
    if (this.#currentVersion !== input.expectedManifestVersion) {
      throw new WriteConflictError(input.expectedManifestVersion, this.#currentVersion);
    }
    if (transaction.snapshotVersion !== input.expectedManifestVersion) {
      throw new Error("Transaction snapshot does not match the expected manifest");
    }
    const pendingTable = transaction.pendingTable;
    if (pendingTable !== undefined) {
      if (transaction.catalogEpochGuard !== this.#catalogEpoch) {
        throw new TableRecordConflictError(
          pendingTable.id,
          pendingTable.revision,
          this.#tables.get(pendingTable.id)?.revision ?? null,
        );
      }
      if (this.#tables.has(pendingTable.id) || this.#tableIdsByName.has(pendingTable.name)) {
        throw new Error(`Table already exists: ${pendingTable.name}`);
      }
      this.#assertTableForeignKeys(pendingTable);
      this.#assertTableTriggerOwnership(pendingTable, transaction.id);
      if (transaction.pendingTableNextRowId === undefined) {
        throw new Error("Pending table row-ID state is missing");
      }
    }
    const baseManifest =
      input.expectedManifestVersion === null
        ? undefined
        : this.#manifests.get(input.expectedManifestVersion);
    if (input.expectedManifestVersion !== null && baseManifest === undefined) {
      throw new Error(`Snapshot manifest is missing: ${String(input.expectedManifestVersion)}`);
    }
    if (
      input.removedBlockIds !== undefined &&
      new Set(input.removedBlockIds).size !== input.removedBlockIds.length
    ) {
      throw new TypeError("Compaction block removals contain duplicates");
    }
    const removedBlockIds = [...new Set(input.removedBlockIds ?? [])];
    if (removedBlockIds.length > 0 && input.compactionJobId === undefined) {
      throw new TypeError("Block removals require a compaction job intent");
    }
    let compactionJob: CompactionJobRecord | undefined;
    if (input.compactionJobId !== undefined) {
      validateId(input.compactionJobId);
      const job = this.#compactionJobs.get(input.compactionJobId);
      if (job === undefined)
        throw new Error(`Compaction job does not exist: ${input.compactionJobId}`);
      if (job.state !== "ready") {
        throw new Error(`Compaction job is not ready: ${input.compactionJobId}`);
      }
      if (job.transactionId !== transaction.id) {
        throw new Error(`Compaction job belongs to another transaction: ${input.compactionJobId}`);
      }
      compactionJob = job;
      const sourceBlockIds = new Set(job.sourceBlockIds);
      const removedBlockIdSet = new Set(removedBlockIds);
      if (
        sourceBlockIds.size !== removedBlockIdSet.size ||
        [...sourceBlockIds].some((id) => !removedBlockIdSet.has(id))
      ) {
        throw new Error(`Compaction removals do not match job ${input.compactionJobId}`);
      }
      const segmentSourceBlockIds = new Set<string>();
      const sourceReferenceCounts = new Map<string, number>();
      for (const segmentId of job.sourceSegmentIds) {
        const segment = this.#segments.get(segmentId);
        if (segment === undefined) {
          throw new Error(`Compaction source segment is missing: ${segmentId}`);
        }
        if (segment.tableId !== job.tableId) {
          throw new Error(`Compaction source segment belongs to another table: ${segmentId}`);
        }
        for (const blockId of segmentBlockIds(segment)) {
          segmentSourceBlockIds.add(blockId);
          sourceReferenceCounts.set(blockId, (sourceReferenceCounts.get(blockId) ?? 0) + 1);
        }
      }
      if (
        segmentSourceBlockIds.size !== sourceBlockIds.size ||
        [...segmentSourceBlockIds].some((id) => !sourceBlockIds.has(id))
      ) {
        throw new Error(`Compaction source segments disagree with job ${input.compactionJobId}`);
      }
      const alias = [...sourceBlockIds].find(
        (blockId) =>
          this.#segments.blockReferenceCount(blockId) > (sourceReferenceCounts.get(blockId) ?? 0),
      );
      if (alias !== undefined) {
        throw new Error(`Compaction cannot retire aliased block ${alias}`);
      }
    }
    for (const id of removedBlockIds) {
      if (!this.#isManifestBlockVisible(input.expectedManifestVersion, id)) {
        throw new Error(`Cannot supersede a block outside the transaction snapshot: ${id}`);
      }
      if (transaction.pendingBlockIds.includes(id)) {
        throw new Error(`Cannot supersede a pending block: ${id}`);
      }
    }
    const addedManifestBlocks: ManifestBlockRecord[] = [];
    for (const id of transaction.pendingBlockIds) {
      if (!hasBlock(id)) throw new Error(`Manifest references missing block: ${id}`);
      if (this.#manifestBlocks.has(id)) {
        throw new Error(`Manifest block ID has already been published: ${id}`);
      }
      const byteLength = blockByteLength(id);
      if (byteLength === undefined || !Number.isSafeInteger(byteLength) || byteLength <= 0) {
        throw new Error(`Manifest block has no valid persisted length: ${id}`);
      }
      const checksum = blockChecksum(id) ?? 0;
      if (!Number.isSafeInteger(checksum) || checksum < 0 || checksum > 0xffff_ffff) {
        throw new Error(`Manifest block has no valid persisted checksum: ${id}`);
      }
      addedManifestBlocks.push({
        blockId: id,
        byteLength,
        checksum,
        addedVersion: nextManifestVersion(input.expectedManifestVersion),
        removedVersion: null,
      });
    }
    const pendingSegments = transaction.pendingSegmentIds.map((id) => {
      const segment = getSegment(id);
      if (segment === undefined) throw new Error(`Transaction references missing segment: ${id}`);
      if (segment.transactionId !== transaction.id) {
        throw new Error(`Segment ${id} belongs to another transaction`);
      }
      const table =
        this.#tables.get(segment.tableId) ??
        (pendingTable?.id === segment.tableId ? pendingTable : undefined);
      if (table === undefined) throw new Error(`Segment ${id} references missing table`);
      const columns = new Set(table.columns.map((column) => column.id));
      const journalBlocks = new Set(transaction.pendingBlockIds);
      for (const [columnId, blockIds] of Object.entries(segment.columnBlockIds)) {
        if (!columns.has(columnId))
          throw new Error(`Segment ${id} references unknown column: ${columnId}`);
        for (const blockId of blockIds) {
          if (
            (!journalBlocks.has(blockId) &&
              !this.#isManifestBlockVisible(input.expectedManifestVersion, blockId)) ||
            !hasBlock(blockId)
          ) {
            throw new Error(`Segment ${id} references unpublished block: ${blockId}`);
          }
        }
      }
      return segment;
    });
    for (const [commitOrdinal, segment] of pendingSegments.entries()) {
      if (segment.commitOrdinal !== commitOrdinal) {
        throw new Error(
          `Transaction pending segment ${segment.id} has commit ordinal ${String(segment.commitOrdinal)}; expected ${String(commitOrdinal)}`,
        );
      }
    }
    if (pendingTable !== undefined) {
      let nextRowId = 1n;
      for (const segment of pendingSegments) {
        if (
          segment.tableId !== pendingTable.id ||
          segment.level !== 0 ||
          segment.kind !== "insert" ||
          segment.rowIdStart !== nextRowId
        ) {
          throw new TypeError("Pending-table transaction journal is not contiguous");
        }
        nextRowId = segment.rowIdEndExclusive;
      }
      if (transaction.pendingTableNextRowId !== nextRowId) {
        throw new Error("Pending-table row-ID counter disagrees with its segment journal");
      }
    }
    if (compactionJob === undefined) {
      const nonLevelZero = pendingSegments.find((segment) => segment.level > 0);
      if (nonLevelZero !== undefined) {
        throw new Error(`No compaction job authorizes output segment ${nonLevelZero.id}`);
      }
    } else {
      const table = this.#tables.get(compactionJob.tableId);
      if (table === undefined) throw new Error(`Compaction job ${compactionJob.id} has no table`);
      const sourceSegments = compactionJob.sourceSegmentIds.map((id) => {
        const segment = this.#segments.get(id);
        if (segment === undefined) throw new Error(`Compaction source segment is missing: ${id}`);
        return segment;
      });
      assertCompactionOutputProvenance(
        compactionJob,
        table,
        transaction,
        sourceSegments,
        pendingSegments,
      );
    }
    const pendingLevelZeroCounts = new Map<string, number>();
    for (const segment of pendingSegments) {
      if (segment.level !== 0) continue;
      pendingLevelZeroCounts.set(
        segment.tableId,
        (pendingLevelZeroCounts.get(segment.tableId) ?? 0) + 1,
      );
    }
    const pendingLevelZeroTables = new Set(pendingLevelZeroCounts.keys());
    const levelZeroLimits = new Map<string, number>();
    for (const entry of input.levelZeroSegmentLimits ?? []) {
      validateId(entry.tableId);
      if (
        !Number.isSafeInteger(entry.limit) ||
        entry.limit <= 0 ||
        entry.limit > MAX_LEVEL_ZERO_SEGMENTS
      ) {
        throw new RangeError(
          `Level-zero segment limit must be between 1 and ${String(MAX_LEVEL_ZERO_SEGMENTS)}`,
        );
      }
      if (levelZeroLimits.has(entry.tableId)) {
        throw new TypeError(`Duplicate level-zero segment limit for table: ${entry.tableId}`);
      }
      levelZeroLimits.set(entry.tableId, entry.limit);
    }
    if (
      levelZeroLimits.size !== pendingLevelZeroTables.size ||
      [...pendingLevelZeroTables].some((tableId) => !levelZeroLimits.has(tableId)) ||
      [...levelZeroLimits].some(([tableId]) => !pendingLevelZeroTables.has(tableId))
    ) {
      throw new TypeError("Level-zero segment limits must exactly cover pending level-zero tables");
    }
    if (pendingLevelZeroTables.size > 0) {
      const visibleCounts = new Map<string, number>();
      for (const tableId of pendingLevelZeroTables) {
        for (const segment of this.#segments.tableValues(tableId)) {
          if (segment.level !== 0) continue;
          const owner = this.#transactions.get(segment.transactionId);
          if (owner?.status !== "committed") continue;
          const blocks = segmentBlockIds(segment);
          if (
            blocks.length > 0 &&
            blocks.some((id) => !this.#isManifestBlockVisible(input.expectedManifestVersion, id))
          ) {
            continue;
          }
          visibleCounts.set(tableId, (visibleCounts.get(tableId) ?? 0) + 1);
        }
      }
      for (const tableId of pendingLevelZeroTables) {
        const count =
          (visibleCounts.get(tableId) ?? 0) + (pendingLevelZeroCounts.get(tableId) ?? 0);
        const limit = levelZeroLimits.get(tableId);
        const table =
          this.#tables.get(tableId) ?? (pendingTable?.id === tableId ? pendingTable : undefined);
        if (limit === undefined || table === undefined) {
          throw new Error(`Level-zero segment limit references missing table: ${tableId}`);
        }
        if (count > limit) throw new CompactionBacklogError(table.name, count, limit);
      }
    }
    const uniqueKeyEntries = input.uniqueKeyChanges ?? [];
    const ftsChanges = validateFtsChangesRuntime(input.ftsChanges);
    transactionCommitDeltaRetainedBytes(uniqueKeyEntries, ftsChanges);
    const coveredUniqueNamespaces = new Set(uniqueKeyEntries.map((entry) => entry.tableId));
    // Pending segments are a fail-closed proof of a logical table change for an ordinary
    // commit: a caller cannot hide a write from live readers by supplying an empty or partial
    // `changedTableIds` list. A validated compaction job is the one exception. Its exact source
    // and output provenance was proved above and the rewrite preserves logical rows, so the
    // transaction's explicit list (normally empty) remains authoritative.
    const physicallyChangedTableIds = new Set([
      ...(input.changedTableIds ?? []),
      ...pendingSegments.map((segment) => segment.tableId),
    ]);
    for (const tableId of physicallyChangedTableIds) {
      const changedTable =
        this.#tables.get(tableId) ?? (pendingTable?.id === tableId ? pendingTable : undefined);
      if (changedTable === undefined) continue;
      for (const [indexId, index] of Object.entries(changedTable.secondaryIndexes ?? {})) {
        if (
          index.uniqueEnforced === true &&
          !coveredUniqueNamespaces.has(secondaryUniqueKeyNamespace(tableId, indexId))
        ) {
          throw new UniqueIndexCoverageError(tableId, index.name);
        }
      }
    }
    // Entries apply in operation order over per-table deltas against the live membership, so
    // a scope that inserts the same key twice conflicts exactly like two separate commits
    // would. A delta rather than a working copy of the table's whole key set: the copy made
    // every keyed commit cost O(table), and it is only ever adopted after validation passes,
    // which the delta achieves by being applied at the mutation step below.
    const uniqueKeyDeltas = new Map<string, { added: Set<string>; removed: Set<string> }>();
    for (const entry of uniqueKeyEntries) {
      let delta = uniqueKeyDeltas.get(entry.tableId);
      if (delta === undefined) {
        delta = { added: new Set(), removed: new Set() };
        uniqueKeyDeltas.set(entry.tableId, delta);
      }
      const existing = this.#uniqueKeys.get(entry.tableId);
      for (const token of entry.keyTokens) {
        if (entry.remove === true) {
          delta.added.delete(token);
          delta.removed.add(token);
          continue;
        }
        const present =
          delta.added.has(token) || (existing?.has(token) === true && !delta.removed.has(token));
        if (entry.requireAbsent && present) {
          throw new UniqueKeyConflictError(entry.tableId, token);
        }
        delta.removed.delete(token);
        delta.added.add(token);
      }
    }
    const nextVersion = nextManifestVersion(input.expectedManifestVersion);
    this.#assertNextManifestPinLag(nextVersion, input.committedAt, transaction.snapshotVersion);
    const removedManifestBlocks = removedBlockIds.map((id) => {
      const current = this.#manifestBlocks.get(id);
      if (current?.removedVersion !== null) {
        throw new Error(`Cannot retire non-live manifest block: ${id}`);
      }
      return { ...current, removedVersion: nextVersion };
    });
    this.#assertRetiredHistoryAddition(removedManifestBlocks);
    this.#assertPinnedRetiredLimits(
      input.committedAt,
      transaction.snapshotVersion,
      undefined,
      [],
      removedManifestBlocks,
    );
    const addedBytes = addedManifestBlocks.reduce((sum, record) => sum + record.byteLength, 0);
    const removedBytes = removedManifestBlocks.reduce((sum, record) => sum + record.byteLength, 0);
    const logicallyChangedTableIds = new Set(input.changedTableIds ?? []);
    if (compactionJob === undefined) {
      for (const segment of pendingSegments) logicallyChangedTableIds.add(segment.tableId);
    }
    if (pendingTable !== undefined) logicallyChangedTableIds.add(pendingTable.id);
    const manifest = createManifest({
      expectedVersion: input.expectedManifestVersion,
      liveBlockCount:
        (baseManifest?.liveBlockCount ?? 0) +
        addedManifestBlocks.length -
        removedManifestBlocks.length,
      liveBlockBytes: (baseManifest?.liveBlockBytes ?? 0) + addedBytes - removedBytes,
      createdAt: input.committedAt,
      changedTableIds: [...logicallyChangedTableIds],
    });
    this.#manifestUsageAfter([manifest]);
    this.#segmentUsageAfter(
      pendingSegments.map((segment) => ({
        ...segment,
        logicalOrder: segment.level === 0 ? manifest.version : segment.logicalOrder,
      })),
    );
    const committed = updateTransactionRecord(transaction, {
      status: "committed",
      committedVersion: manifest.version,
      updatedAt: input.committedAt,
    });
    return {
      manifest,
      addedManifestBlocks,
      removedManifestBlocks,
      committed,
      pendingSegments,
      uniqueKeyDeltas,
      ftsChanges,
      ...(pendingTable === undefined
        ? {}
        : {
            pendingTable,
            pendingTableNextRowId: transaction.pendingTableNextRowId,
          }),
    };
  }

  #applyCommit(plan: CommitPlan): ManifestSummary {
    const { manifest, addedManifestBlocks, removedManifestBlocks, committed, pendingSegments } =
      plan;
    const nextCatalogEpoch = safeWholeIncrement(this.#catalogEpoch, "Catalog epoch");
    const nextSchemaEpoch =
      plan.pendingTable === undefined
        ? this.#schemaEpoch
        : safeWholeIncrement(this.#schemaEpoch, "Schema epoch");
    const committedSegments = pendingSegments.map((segment) => ({
      ...segment,
      // Level-zero records are staged before their publication version exists. Their durable
      // read order is the manifest that first makes them visible. Compaction output already
      // carries its exact level/order and keeps it.
      logicalOrder: segment.level === 0 ? manifest.version : segment.logicalOrder,
    }));
    this.#setManifest(manifest);
    this.#replaceSegments(committedSegments);
    for (const record of addedManifestBlocks) this.#manifestBlocks.set(record.blockId, record);
    for (const record of removedManifestBlocks) this.#manifestBlocks.set(record.blockId, record);
    this.#currentVersion = manifest.version;
    this.#setTransaction(committed);
    if (plan.pendingTable !== undefined) {
      this.#installTableRecord(plan.pendingTable);
      this.#nextRowIds.set(plan.pendingTable.id, plan.pendingTableNextRowId ?? 1n);
    }
    for (const [tableId, delta] of plan.uniqueKeyDeltas) {
      let tokens = this.#uniqueKeys.get(tableId);
      if (tokens === undefined) {
        tokens = new OrderedStringSet();
        this.#uniqueKeys.set(tableId, tokens);
      }
      for (const token of delta.removed) tokens.delete(token);
      tokens.addMany(delta.added);
    }
    this.#applyFtsChanges(
      pendingSegments,
      plan.ftsChanges,
      manifest.version,
      manifest.changedTableIds,
    );
    this.#catalogEpoch = nextCatalogEpoch;
    this.#schemaEpoch = nextSchemaEpoch;
    const ftsDeltaCounts = (plan.ftsChanges ?? []).flatMap((changes) =>
      changes.columns.map((column) => ({
        tableId: changes.tableId,
        columnId: column.columnId,
        count: this.#ftsDeltas.get(`${changes.tableId}/${column.columnId}`)?.size ?? 0,
      })),
    );
    return structuredClone({
      ...manifest,
      ...(ftsDeltaCounts.length === 0 ? {} : { ftsDeltaCounts }),
    });
  }

  createLease(record: LeaseRecord): void {
    validateLeaseRuntimeRecord(record, "Lease", true);
    if (this.#leases.has(record.id)) throw new Error(`Lease already exists: ${record.id}`);
    this.#assertSnapshotAvailable(record.manifestVersion);
    this.#assertPinnedRetiredLimits(
      record.createdAt,
      undefined,
      undefined,
      record.manifestVersion === null ? [] : [record.manifestVersion],
    );
    for (const expired of boundedExpiryPage(
      this.#leases,
      record.createdAt,
      null,
      RESOURCE_EXPIRY_SWEEP_ITEMS,
      (lease) => lease.id,
      "Lease create cleanup",
    ).records) {
      this.#leases.delete(expired.id);
    }
    if (this.#leases.size >= MAX_ACTIVE_LEASES) {
      throw new StorageResourceLimitError("lease", this.#leases.size + 1, MAX_ACTIVE_LEASES);
    }
    this.#leases.set(record.id, structuredClone(record));
  }

  getLease(id: string): LeaseRecord | undefined {
    const record = this.#leases.get(id);
    return record === undefined ? undefined : structuredClone(record);
  }

  listLeases(): LeaseRecord[] {
    return [...this.#leases.values()]
      .sort((left, right) => left.id.localeCompare(right.id))
      .map((record) => structuredClone(record));
  }

  listExpiredLeasePage(
    expiresAtCutoff: string,
    afterCursor: string | null,
    limit: number,
  ): StoragePage<LeaseRecord, string> {
    const page = boundedExpiryPage(
      this.#leases,
      expiresAtCutoff,
      afterCursor,
      limit,
      (record) => record.id,
      "Lease page",
    );
    return {
      records: page.records.map((record) => structuredClone(record)),
      nextCursor: page.nextCursor,
    };
  }

  renewLease(input: RenewLeaseInput): LeaseRecord {
    const cutoff = validateBoundedExpiration(
      input.expiresAtCutoff,
      input.expiresAt,
      "Lease",
      MAX_LEASE_TTL_MS,
    );
    const record = this.#leases.get(input.id);
    if (record?.revision !== input.expectedRevision) {
      throw new LeaseConflictError(input.id, input.expectedRevision, record?.revision ?? null);
    }
    if (Date.parse(record.expiresAt) <= cutoff) {
      throw new LeaseExpiredError(input.id, record.expiresAt, input.expiresAtCutoff);
    }
    this.#assertSnapshotAvailable(record.manifestVersion);
    const renewed = {
      ...record,
      expiresAt: input.expiresAt,
      revision: safeWholeIncrement(record.revision, "Lease revision"),
    };
    this.#leases.set(input.id, renewed);
    return structuredClone(renewed);
  }

  moveLease(input: MoveLeaseInput): LeaseRecord {
    const cutoff = validateBoundedExpiration(
      input.expiresAtCutoff,
      input.expiresAt,
      "Lease",
      MAX_LEASE_TTL_MS,
    );
    const record = this.#leases.get(input.id);
    if (record?.revision !== input.expectedRevision) {
      throw new LeaseConflictError(input.id, input.expectedRevision, record?.revision ?? null);
    }
    if (Date.parse(record.expiresAt) <= cutoff) {
      throw new LeaseExpiredError(input.id, record.expiresAt, input.expiresAtCutoff);
    }
    this.#assertSnapshotAvailable(input.manifestVersion);
    this.#assertPinnedRetiredLimits(
      input.expiresAtCutoff,
      undefined,
      { id: record.id, version: record.manifestVersion },
      input.manifestVersion === null ? [] : [input.manifestVersion],
    );
    const moved = {
      ...record,
      manifestVersion: input.manifestVersion,
      expiresAt: input.expiresAt,
      revision: safeWholeIncrement(record.revision, "Lease revision"),
    };
    this.#leases.set(input.id, moved);
    return structuredClone(moved);
  }

  removeLeaseIfExpired(id: string, expectedRevision: number, expiresAtCutoff: string): boolean {
    const cutoff = Date.parse(expiresAtCutoff);
    if (!Number.isFinite(cutoff)) throw new TypeError("Lease expiry cutoff must be valid");
    const record = this.#leases.get(id);
    if (record?.revision !== expectedRevision) {
      throw new LeaseConflictError(id, expectedRevision, record?.revision ?? null);
    }
    const expiresAt = Date.parse(record.expiresAt);
    if (!Number.isFinite(expiresAt) || expiresAt > cutoff) return false;
    this.#leases.delete(id);
    return true;
  }

  removeLease(input: { id: string; ownerId: string }): boolean {
    validateId(input.id);
    validateId(input.ownerId);
    const lease = this.#leases.get(input.id);
    if (lease === undefined) return false;
    if (lease.ownerId !== input.ownerId) {
      throw new LeaseOwnerConflictError(input.id, input.ownerId, lease.ownerId);
    }
    this.#leases.delete(input.id);
    return true;
  }

  createCompactionJob(record: CompactionJobRecord): void {
    const normalized = normalizeCompactionJobRecord(record);
    const duplicate = this.#compactionJobs.get(normalized.id);
    if (duplicate !== undefined) {
      throw new CompactionJobConflictError(normalized.id, normalized.revision, duplicate.revision);
    }
    let activeJobs = 0;
    for (const current of this.#compactionJobs.values()) {
      if (!isTerminalCompactionJob(current)) activeJobs += 1;
      if (current.tableId === normalized.tableId && !isTerminalCompactionJob(current)) {
        throw new CompactionJobConflictError(normalized.id, normalized.revision, current.revision);
      }
    }
    if (!isTerminalCompactionJob(normalized) && activeJobs >= MAX_ACTIVE_COMPACTION_JOBS) {
      throw new StorageResourceLimitError(
        "compaction job",
        activeJobs + 1,
        MAX_ACTIVE_COMPACTION_JOBS,
      );
    }
    this.#assertCompactionJobReferences(normalized);
    this.#assertTerminalCompactionJobTransition(undefined, normalized);
    this.#compactionJobs.set(normalized.id, normalized);
  }

  getCompactionJob(id: string): CompactionJobRecord | undefined {
    const record = this.#compactionJobs.get(id);
    return record === undefined ? undefined : structuredClone(record);
  }

  listCompactionJobs(tableId?: string): CompactionJobRecord[] {
    return [...this.#compactionJobs.values()]
      .filter((record) => tableId === undefined || record.tableId === tableId)
      .sort(
        (left, right) =>
          left.createdAt.localeCompare(right.createdAt) || left.id.localeCompare(right.id),
      )
      .map((record) => structuredClone(record));
  }

  listCompactionJobPage(
    afterId: string | null,
    limit: number,
  ): StoragePage<CompactionJobRecord, string> {
    validatePageLimit(limit);
    const records = boundedRecordPage(this.#compactionJobs, afterId, limit).map((record) =>
      structuredClone(record),
    );
    return { records, nextCursor: records.length === limit ? (records.at(-1)?.id ?? null) : null };
  }

  updateCompactionJob(
    id: string,
    expectedRevision: number,
    update: CompactionJobRecordUpdate,
  ): CompactionJobRecord {
    if (update.state === "cancelled") {
      throw new TypeError("Use cancelCompactionJob to cancel a compaction job");
    }
    const current = this.#compactionJobs.get(id);
    if (current?.revision !== expectedRevision) {
      throw new CompactionJobConflictError(id, expectedRevision, current?.revision ?? null);
    }
    const updated = updateCompactionJobRecord(current, update);
    this.#assertCompactionJobReferences(updated);
    this.#assertTerminalCompactionJobTransition(current, updated);
    this.#compactionJobs.set(id, updated);
    return structuredClone(updated);
  }

  #assertCompactionJobReferences(job: CompactionJobRecord): void {
    // Cancelled/aborted jobs are retained diagnostics rather than live physical roots.
    if (job.state === "cancelled" || job.state === "aborted") return;
    if (!this.#tables.has(job.tableId)) throw new Error(`Compaction job ${job.id} has no table`);
    const sourceManifest = this.#manifests.get(job.sourceManifestVersion);
    if (sourceManifest === undefined || sourceManifest.prunedAt !== undefined) {
      throw new Error(`Compaction job ${job.id} has no readable source manifest`);
    }
    if (!isTerminalCompactionJob(job)) this.#assertPinnedManifestLag(job.sourceManifestVersion);
    if (!isTerminalCompactionJob(job)) {
      const transaction =
        job.transactionId === null ? undefined : this.#transactions.get(job.transactionId);
      const linkedVersion =
        transaction?.status === "committed" ? transaction.committedVersion : null;
      this.#assertPinnedRetiredLimits(job.updatedAt, undefined, undefined, [
        job.sourceManifestVersion,
        ...(linkedVersion === null ? [] : [linkedVersion]),
      ]);
    }
    const foreignSourceBlockId = job.sourceBlockIds.find(
      (id) => !this.#isManifestBlockVisible(job.sourceManifestVersion, id),
    );
    if (foreignSourceBlockId !== undefined) {
      throw new Error(
        `Compaction job ${job.id} source block is outside its source manifest: ${foreignSourceBlockId}`,
      );
    }
    for (const id of job.sourceSegmentIds) {
      const segment = this.#segments.get(id);
      if (segment === undefined) {
        throw new Error(`Compaction job ${job.id} has no source segment: ${id}`);
      }
      if (segment.tableId !== job.tableId) {
        throw new Error(`Compaction job ${job.id} source segment belongs to another table: ${id}`);
      }
    }
    for (const id of [...job.sourceBlockIds, ...job.outputBlockIds]) {
      if (!this.#physical.hasBlock(id)) {
        throw new Error(`Compaction job ${job.id} has no block: ${id}`);
      }
    }
    if (job.transactionId !== null && !this.#transactions.has(job.transactionId)) {
      throw new Error(`Compaction job ${job.id} has no transaction`);
    }
    if (job.state === "ready" || job.state === "published") {
      for (const outputId of compactionOutputSegmentIds(job)) {
        const output = this.#segments.get(outputId);
        if (output === undefined) {
          throw new Error(`Compaction job ${job.id} has no output segment: ${outputId}`);
        }
        if (output.tableId !== job.tableId || output.transactionId !== job.transactionId) {
          throw new Error(`Compaction job ${job.id} output segment ownership is inconsistent`);
        }
      }
    }
    if (job.state === "published") {
      const published =
        job.publishedVersion === null ? undefined : this.#manifests.get(job.publishedVersion);
      if (published === undefined || published.prunedAt !== undefined) {
        throw new Error(`Compaction job ${job.id} has no readable published manifest`);
      }
    }
  }

  cancelCompactionJob(
    id: string,
    expectedRevision: number,
    cancelledAt: string,
  ): CompactionJobRecord {
    const current = this.#compactionJobs.get(id);
    if (current?.revision !== expectedRevision) {
      throw new CompactionJobConflictError(id, expectedRevision, current?.revision ?? null);
    }
    if (isTerminalCompactionJob(current)) {
      return structuredClone(current);
    }

    const transaction =
      current.transactionId === null ? undefined : this.#transactions.get(current.transactionId);
    if (transaction?.status === "committed") {
      if (transaction.committedVersion === null) {
        throw new Error(`Committed transaction has no manifest version: ${transaction.id}`);
      }
      const published = updateCompactionJobRecord(current, {
        state: "published",
        publishedVersion: transaction.committedVersion,
        updatedAt: cancelledAt,
        error: null,
      });
      this.#assertTerminalCompactionJobTransition(current, published);
      this.#compactionJobs.set(id, published);
      return structuredClone(published);
    }

    const cancelled = updateCompactionJobRecord(current, {
      state: "cancelled",
      updatedAt: cancelledAt,
      error: null,
    });
    this.#assertTerminalCompactionJobTransition(current, cancelled);
    const abortedTransaction =
      transaction?.status === "active"
        ? updateTransactionRecord(transaction, {
            status: "aborted",
            updatedAt: cancelledAt,
            committedVersion: null,
          })
        : undefined;
    if (abortedTransaction !== undefined) {
      this.#setTransaction(abortedTransaction);
    }
    this.#compactionJobs.set(id, cancelled);
    return structuredClone(cancelled);
  }

  removeCompactionJob(id: string): boolean {
    if (!this.canRemoveCompactionJob(id)) return false;
    this.#compactionJobs.delete(id);
    return true;
  }

  canRemoveCompactionJob(id: string): boolean {
    const job = this.#compactionJobs.get(id);
    if (job === undefined) return false;
    if (!isTerminalCompactionJob(job)) {
      throw new Error(`Cannot remove nonterminal compaction job: ${id}`);
    }
    return this.#compactionJobHasNoSoleProvenance(job);
  }

  #compactionJobHasNoSoleProvenance(job: CompactionJobRecord): boolean {
    const outputIds = compactionOutputSegmentIds(job);
    const referencedSegmentIds = [...job.sourceSegmentIds, ...outputIds];
    const segmentIsRooted = (segment: SegmentRecord): boolean => {
      const ids = segmentBlockIds(segment);
      return (
        this.#roots.transactionSegmentCount(segment.id) > 0 ||
        this.#roots.activeOwner(segment.transactionId) ||
        this.#roots.allJobSegmentCount(segment.id) -
          Number(job.sourceSegmentIds.includes(segment.id)) -
          Number(outputIds.includes(segment.id)) >
          0 ||
        this.#roots.garbageSegmentCount(segment.id) > 0 ||
        (ids.length > 0 && ids.every((id) => this.#blockHasReadableManifest(id)))
      );
    };
    for (const segmentId of [...new Set(referencedSegmentIds)].sort()) {
      const segment = this.#segments.get(segmentId);
      if (segment !== undefined && !segmentIsRooted(segment)) {
        return false;
      }
    }

    const jobBlockIds = new Set([...job.sourceBlockIds, ...job.outputBlockIds]);
    for (const blockId of [...jobBlockIds].sort()) {
      const directlyRooted =
        this.#blockHasReadableManifest(blockId) ||
        this.#roots.transactionBlockCount(blockId) > 0 ||
        this.#roots.allJobBlockCount(blockId) -
          Number(job.sourceBlockIds.includes(blockId)) -
          Number(job.outputBlockIds.includes(blockId)) >
          0 ||
        this.#roots.garbageBlockCount(blockId) > 0;
      const segmentRooted = [...this.#segments.segmentIdsForBlock(blockId)].some((segmentId) => {
        const segment = this.#segments.get(segmentId);
        return segment !== undefined && segmentIsRooted(segment);
      });
      if (this.#physical.hasBlock(blockId) && !directlyRooted && !segmentRooted) {
        return false;
      }
    }
    return true;
  }

  createGarbageCollectionJob(input: CreateGarbageCollectionJobInput): GarbageCollectionJobRecord {
    const record = createGarbageCollectionJobRecord(input);
    const duplicate = this.#garbageCollectionJobs.get(record.id);
    if (duplicate !== undefined) {
      throw new GarbageCollectionJobConflictError(record.id, record.revision, duplicate.revision);
    }
    for (const current of this.#garbageCollectionJobs.values()) {
      if (current.state !== "completed") {
        throw new GarbageCollectionJobConflictError(record.id, record.revision, current.revision);
      }
    }
    assertGarbageCollectionCandidateProvenance(
      record,
      this.#manifests,
      this.#manifestBlocks,
      this.#segments,
      this.#transactions,
      this.#roots,
    );
    this.#pruneOneCompletedGarbageCollectionJob();
    this.#garbageCollectionJobs.set(record.id, record);
    return structuredClone(record);
  }

  updateGarbageCollectionPlanning(
    input: UpdateGarbageCollectionPlanningInput,
  ): GarbageCollectionJobRecord {
    const current = this.#garbageCollectionJobs.get(input.jobId);
    if (current?.revision !== input.expectedRevision) {
      throw new GarbageCollectionJobConflictError(
        input.jobId,
        input.expectedRevision,
        current?.revision ?? null,
      );
    }
    const updated = updateGarbageCollectionPlanningRecord(current, input);
    assertGarbageCollectionCandidateProvenance(
      updated,
      this.#manifests,
      this.#manifestBlocks,
      this.#segments,
      this.#transactions,
      this.#roots,
    );
    this.#garbageCollectionJobs.set(updated.id, updated);
    return structuredClone(updated);
  }

  getGarbageCollectionJob(id: string): GarbageCollectionJobRecord | undefined {
    const record = this.#garbageCollectionJobs.get(id);
    return record === undefined ? undefined : structuredClone(record);
  }

  listGarbageCollectionJobs(): GarbageCollectionJobRecord[] {
    return [...this.#garbageCollectionJobs.values()]
      .sort(
        (left, right) =>
          left.createdAt.localeCompare(right.createdAt) || left.id.localeCompare(right.id),
      )
      .map((record) => structuredClone(record));
  }

  listGarbageCollectionJobPage(
    afterId: string | null,
    limit: number,
  ): StoragePage<GarbageCollectionJobRecord, string> {
    validatePageLimit(limit);
    const records = boundedRecordPage(this.#garbageCollectionJobs, afterId, limit).map((record) =>
      structuredClone(record),
    );
    return { records, nextCursor: records.length === limit ? (records.at(-1)?.id ?? null) : null };
  }

  /**
   * Advances the job and mutates every record it reclaims. Physical block bytes are the
   * caller's to delete afterwards, from the result's `reclaimedBlockIds` — in the same atomic
   * step for the memory store, or after the logged entry for a store whose bytes are files.
   */
  runGarbageCollectionStep(input: RunGarbageCollectionStepInput): GarbageCollectionStepResult {
    validateGarbageCollectionStepInput(input);
    const current = this.#garbageCollectionJobs.get(input.jobId);
    if (current?.revision !== input.expectedRevision) {
      throw new GarbageCollectionJobConflictError(
        input.jobId,
        input.expectedRevision,
        current?.revision ?? null,
      );
    }
    if (current.state === "completed") return emptyGarbageCollectionStep(current);

    const prunedManifestVersions: number[] = [];
    const alreadyPrunedManifestVersions: number[] = [];
    const retainedManifestVersions: number[] = [];
    const missingManifestVersions: number[] = [];
    const reclaimedSegmentIds: string[] = [];
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
      const manifest = this.#manifests.get(version);
      if (manifest === undefined) missingManifestVersions.push(version);
      else if (manifest.prunedAt !== undefined) alreadyPrunedManifestVersions.push(version);
      else if (this.#roots.isManifestPinned(version, this.#currentVersion, current.leaseCutoff))
        retainedManifestVersions.push(version);
      else prunedManifestVersions.push(version);
      manifestIndex += 1;
      remaining -= 1;
    }

    const prunedManifestVersionSet = new Set(prunedManifestVersions);
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
    const roots = collectIndexedPhysicalRoots(
      segmentIdsToExamine,
      blockIdsToExamine,
      this.#manifestBlocks,
      prunedManifestVersionSet,
      this.#segments,
      this.#roots,
    );
    while (
      remaining > 0 &&
      manifestIndex === current.candidateManifestVersions.length &&
      segmentIndex < current.candidateSegmentIds.length
    ) {
      const id = current.candidateSegmentIds[segmentIndex];
      if (id === undefined) throw new Error("Garbage collection segment cursor is invalid");
      if (!this.#segments.has(id)) missingSegmentIds.push(id);
      else if (roots.segmentIds.has(id)) retainedSegmentIds.push(id);
      else reclaimedSegmentIds.push(id);
      segmentIndex += 1;
      remaining -= 1;
    }

    let blockIndex = current.cursor.blockIndex;
    // Transaction records are last. A committed record remains an
    // idempotency/reconciliation witness until its manifest has actually been pruned, and it
    // remains structural metadata while any segment or unfinished fold still names it.
    let transactionIndex = current.cursor.transactionIndex;
    if (
      remaining > 0 &&
      manifestIndex === current.candidateManifestVersions.length &&
      segmentIndex === current.candidateSegmentIds.length &&
      blockIndex === current.candidateBlockIds.length
    ) {
      const reclaimedOwnerCounts = new Map<string, number>();
      for (const segmentId of reclaimedSegmentIds) {
        const owner = this.#segments.get(segmentId)?.transactionId;
        if (owner !== undefined) updateReferenceCount(reclaimedOwnerCounts, owner, 1);
      }
      while (remaining > 0 && transactionIndex < current.candidateTransactionIds.length) {
        const id = current.candidateTransactionIds[transactionIndex];
        if (id === undefined) throw new Error("Garbage collection transaction cursor is invalid");
        const record = this.#transactions.get(id);
        const manifest =
          record?.committedVersion === null || record?.committedVersion === undefined
            ? undefined
            : this.#manifests.get(record.committedVersion);
        if (record === undefined) missingTransactionIds.push(id);
        else if (
          this.#roots.liveJobTransactionCount(id) > 0 ||
          (record.status === "committed"
            ? (manifest !== undefined &&
                manifest.prunedAt === undefined &&
                !prunedManifestVersionSet.has(record.committedVersion ?? -1)) ||
              this.#segments.ownerReferenceCount(id) - (reclaimedOwnerCounts.get(id) ?? 0) > 0
            : record.status === "aborted"
              ? record.pendingBlockIds.some((blockId) => this.#physical.hasBlock(blockId)) ||
                record.pendingSegmentIds.some((segmentId) => this.#segments.has(segmentId))
              : true)
        ) {
          retainedTransactionIds.push(id);
        } else {
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
      const byteLength = this.#physical.blockByteLength(id);
      if (byteLength === undefined) missingBlockIds.push(id);
      else if (roots.blockIds.has(id)) retainedBlockIds.push(id);
      else {
        reclaimedBlockIds.push(id);
        reclaimedBlockBytes = safeStorageSum(reclaimedBlockBytes, byteLength);
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
    if (updated.state === "completed") {
      this.#pruneOneCompletedGarbageCollectionJob();
    }
    this.#replaceManifests(
      prunedManifestVersions.flatMap((version) => {
        const manifest = this.#manifests.get(version);
        return manifest === undefined ? [] : [{ ...manifest, prunedAt: input.updatedAt }];
      }),
    );
    this.#replaceSegments([], reclaimedSegmentIds);
    reclaimedBlockIds.forEach((id) => this.#manifestBlocks.delete(id));
    reclaimedTransactionIds.forEach((id) => this.#transactions.delete(id));
    this.#garbageCollectionJobs.set(updated.id, updated);
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
  }

  removeGarbageCollectionJob(id: string): void {
    const job = this.#garbageCollectionJobs.get(id);
    if (job !== undefined && job.state !== "completed") {
      throw new Error(`Cannot remove incomplete garbage collection job: ${id}`);
    }
    this.#garbageCollectionJobs.delete(id);
  }

  /**
   * Streams the canonical metadata half of a framed snapshot while the enclosing adapter holds
   * its writer queue. The iterator never builds a database-sized array: ordered indexes yield
   * one record at a time and UNIQUE membership is split into bounded immutable generations.
   * Posting generations are substrate-specific (OPFS keeps bases outside this core) and are
   * therefore emitted by the adapter after these five record families.
   *
   * @internal The returned records alias immutable-at-the-call-boundary core state. Exhaust the
   * iterator before releasing the enclosing writer lock.
   */
  *snapshotFrameMetadataItems(): IterableIterator<
    SnapshotCatalogItem | SnapshotSegmentItem | SnapshotTransactionItem | SnapshotUniqueItem
  > {
    const version = this.#currentVersion;
    if (version === null) throw new Error("There is no committed version to snapshot");
    if (this.#manifests.get(version) === undefined) throw new SnapshotManifestMissingError(version);

    for (const table of this.#tables.orderedValues(null)) {
      const autoIncrement = table.columns.flatMap((column) => {
        const next = this.#nextAutoIncrement.get(`${table.id}/${column.id}`);
        return next === undefined ? [] : [{ columnId: column.id, next }];
      });
      yield {
        kind: "table",
        record: structuredClone(table),
        nextRowId: this.#nextRowIds.get(table.id) ?? 1n,
        autoIncrement,
      };
    }

    for (const record of this.#segments.orderedValues(null)) {
      const owner = this.#transactions.get(record.transactionId);
      if (
        owner?.status !== "committed" ||
        owner.committedVersion === null ||
        owner.committedVersion > version
      ) {
        continue;
      }
      if (segmentBlockIds(record).some((id) => !this.#isManifestBlockVisible(version, id))) {
        continue;
      }
      const table = this.#tables.get(record.tableId);
      if (table === undefined) throw new Error(`Snapshot segment has no table: ${record.id}`);
      const columns = new Set(table.columns.map((column) => column.id));
      const segment = normalizeSegmentRecord(record);
      const filtered: SegmentRecord = {
        ...segment,
        columnBlockIds: Object.fromEntries(
          Object.entries(segment.columnBlockIds).filter(([columnId]) => columns.has(columnId)),
        ),
      };
      if (filtered.keyColumnId !== undefined && !columns.has(filtered.keyColumnId)) {
        delete filtered.keyColumnId;
      }
      yield { kind: "segment", record: filtered };
    }

    for (const record of this.#transactions.orderedValues(null)) {
      if (
        record.status !== "committed" ||
        record.committedVersion === null ||
        record.committedVersion > version
      ) {
        continue;
      }
      yield {
        kind: "transaction",
        record: {
          ...structuredClone(record),
          pendingBlockIds: [],
          pendingSegmentIds: [],
        },
      };
    }

    let uniqueGenerationOrdinal = 0;
    for (const table of this.#tables.orderedValues(null)) {
      const namespaces: Array<{ indexId: string | null; namespaceId: string }> = [];
      if (table.uniqueKeyColumnId !== undefined) {
        namespaces.push({ indexId: null, namespaceId: table.id });
      }
      for (const [indexId, index] of Object.entries(table.secondaryIndexes ?? {})) {
        if (index.unique === true && index.uniqueEnforced === true) {
          namespaces.push({
            indexId,
            namespaceId: secondaryUniqueKeyNamespace(table.id, indexId),
          });
        }
      }
      namespaces.sort((left, right) => left.namespaceId.localeCompare(right.namespaceId));
      for (const { indexId, namespaceId } of namespaces) {
        // Generation IDs are persisted in one database-wide namespace by every built-in
        // adapter. Keep the snapshot-derived ID deterministic for lost-ack replay, while making
        // it distinct for every UNIQUE namespace in the snapshot.
        const generationId = `snapshot-u-${String(version)}-${String(uniqueGenerationOrdinal)}`;
        uniqueGenerationOrdinal += 1;
        const membership = this.#uniqueKeys.get(namespaceId) ?? new OrderedStringSet();
        const chunkCount = Math.ceil(membership.size / SNAPSHOT_UNIQUE_TOKENS_PER_CHUNK);
        yield {
          kind: "unique-generation",
          tableId: table.id,
          indexId,
          namespaceId,
          generationId,
          chunkCount,
          tokenCount: membership.size,
        };
        let ordinal = 0;
        let chunk: string[] = [];
        for (const token of membership.orderedValues()) {
          chunk.push(token);
          if (chunk.length < SNAPSHOT_UNIQUE_TOKENS_PER_CHUNK) continue;
          yield {
            kind: "unique-chunk",
            namespaceId,
            generationId,
            ordinal,
            keyTokens: chunk,
          };
          ordinal += 1;
          chunk = [];
        }
        if (chunk.length > 0) {
          yield {
            kind: "unique-chunk",
            namespaceId,
            generationId,
            ordinal,
            keyTokens: chunk,
          };
        }
      }
    }
  }

  /**
   * Streams complete current posting generations in canonical storage-column/term order.
   * The merge retains only one term's row map and one bounded output chunk; it never builds a
   * second database-sized postings array. A cheap first pass derives exact descriptor totals,
   * then a second pass yields the immutable chunks.
   *
   * @internal Memory-backed framed snapshot export. OPFS streams its extent-backed bases.
   */
  *snapshotFramePostingItems(): IterableIterator<SnapshotPostingItem> {
    const version = this.#currentVersion;
    if (version === null) throw new Error("There is no committed version to snapshot");
    const owners: Array<{
      tableId: string;
      ownerKind: "fts-column" | "secondary-index";
      ownerId: string;
      storageColumnId: string;
    }> = [];
    for (const table of this.#tables.orderedValues(null)) {
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
      const key = `${owner.tableId}/${owner.storageColumnId}`;
      const base = this.#ftsBases.get(key);
      if (base === undefined) continue;
      const delta = this.readFtsDeltas(
        owner.tableId,
        owner.storageColumnId,
        base.coversVersion,
        version,
      );
      const sources: ReadonlyArray<readonly FtsPosting[]> = [...base.chunks, ...delta.chunkLists];
      let chunkCount = 0;
      let totalTokens = 0;
      let termCount = 0;
      for (const posting of mergedFtsPostingIterator(sources)) {
        termCount += 1;
        if (termCount === SNAPSHOT_POSTING_TERMS_PER_CHUNK) {
          chunkCount += 1;
          termCount = 0;
        }
        for (const frequency of posting.tf) {
          totalTokens = safeStorageSum(
            totalTokens,
            owner.ownerKind === "secondary-index" ? 1 : frequency,
          );
        }
      }
      if (termCount > 0) chunkCount += 1;
      const generationId = `snapshot-p-${String(version)}-${String(ownerOrdinal)}`;
      yield {
        kind: "posting-generation",
        ...owner,
        generationId,
        coversVersion: version,
        chunkCount,
        totalTokens,
      };
      let ordinal = 0;
      let postings: FtsPosting[] = [];
      for (const posting of mergedFtsPostingIterator(sources)) {
        postings.push(
          owner.ownerKind === "secondary-index"
            ? { ...posting, tf: posting.rowIds.map(() => 1) }
            : posting,
        );
        if (postings.length < SNAPSHOT_POSTING_TERMS_PER_CHUNK) continue;
        yield {
          kind: "posting-chunk",
          storageColumnId: owner.storageColumnId,
          generationId,
          ordinal,
          postings,
        };
        ordinal += 1;
        postings = [];
      }
      if (postings.length > 0) {
        yield {
          kind: "posting-chunk",
          storageColumnId: owner.storageColumnId,
          generationId,
          ordinal,
          postings,
        };
      }
    }
  }

  /** Exact bounded snapshot block summary; payload IDs remain in the provenance page index. */
  snapshotFrameManifest(): ManifestSummary {
    const version = this.#currentVersion;
    if (version === null) throw new Error("There is no committed version to snapshot");
    const manifest = this.#manifests.get(version);
    if (manifest === undefined) throw new SnapshotManifestMissingError(version);
    return structuredClone(manifest);
  }

  /** O(1) counters for diagnostics; callers never need to clone the full durable state. */
  storageRecordStats(): {
    manifestCount: number;
    transactionCount: number;
    segmentCount: number;
    tableCount: number;
    leaseCount: number;
    compactionJobCount: number;
    garbageCollectionJobCount: number;
    rowCounterCount: number;
    autoIncrementCounterCount: number;
    ftsBaseCount: number;
    ftsDeltaCount: number;
    uniqueNamespaceCount: number;
    uniqueKeyBuildCount: number;
    tempOwnerCount: number;
    liveBlockCount: number;
    liveBlockBytes: number;
  } {
    const current =
      this.#currentVersion === null ? undefined : this.#manifests.get(this.#currentVersion);
    return {
      manifestCount: this.#manifests.size,
      transactionCount: this.#transactions.size,
      segmentCount: this.#segments.size,
      tableCount: this.#tables.size,
      leaseCount: this.#leases.size,
      compactionJobCount: this.#compactionJobs.size,
      garbageCollectionJobCount: this.#garbageCollectionJobs.size,
      rowCounterCount: this.#nextRowIds.size,
      autoIncrementCounterCount: this.#nextAutoIncrement.size,
      ftsBaseCount: this.#ftsBases.size,
      ftsDeltaCount: this.#ftsDeltas.size,
      uniqueNamespaceCount: this.#uniqueKeys.size,
      uniqueKeyBuildCount: this.#uniqueKeyBuilds.size,
      tempOwnerCount: this.#tempOwners.size,
      liveBlockCount: current?.liveBlockCount ?? 0,
      liveBlockBytes: current?.liveBlockBytes ?? 0,
    };
  }

  /**
   * Atomically promotes one already-decoded framed snapshot generation. The caller supplies a
   * streaming iterable over metadata pages and the final physical block identities; this method
   * builds a disposable core, validates every cross-reference, then swaps state only after the
   * complete generation proves self-consistent.
   *
   * @internal Framed adapters call this only at the durable finish boundary.
   */
  loadSnapshotFrameItems(
    header: SnapshotFrameStreamHeader,
    items: Iterable<SnapshotMetadataItem>,
    blocks: Iterable<Pick<ManifestBlockRecord, "blockId" | "byteLength" | "checksum">>,
  ): void {
    if (this.#currentVersion !== null || this.#tables.size > 0) {
      throw new Error("This store already holds a database");
    }
    const trial = new RecordCore(this.#physical);
    let previousTableId: string | undefined;
    let previousSegmentId: string | undefined;
    let previousTransactionId: string | undefined;
    let previousUniqueNamespace: string | undefined;
    let previousPostingIdentity: string | undefined;
    let unique:
      | {
          descriptor: Extract<SnapshotUniqueItem, { kind: "unique-generation" }>;
          tokens: OrderedStringSet;
          nextOrdinal: number;
          previousToken: string | undefined;
        }
      | undefined;
    let posting:
      | {
          descriptor: SnapshotPostingGenerationItem;
          chunks: FtsPosting[][];
          nextOrdinal: number;
          previousTerm: string | undefined;
          totalTokens: number;
        }
      | undefined;
    const uniqueNamespaces = new Set<string>();
    const postingColumns = new Set<string>();
    let acceleratorRetainedBytes = 0;
    let acceleratorRetainedEntries = 0;
    let declaredUniqueEntries = 0;

    const retainAcceleratorChunk = (
      item: Extract<SnapshotMetadataItem, { kind: "unique-chunk" | "posting-chunk" }>,
    ): void => {
      const usage = snapshotAcceleratorItemRetainedUsage(item);
      const nextBytes = safeStorageSum(acceleratorRetainedBytes, usage.bytes);
      const nextEntries = safeStorageSum(acceleratorRetainedEntries, usage.entries);
      assertSnapshotImportAcceleratorUsage(nextBytes, nextEntries);
      acceleratorRetainedBytes = nextBytes;
      acceleratorRetainedEntries = nextEntries;
    };

    const finishUnique = (): void => {
      if (unique === undefined) return;
      if (
        unique.nextOrdinal !== unique.descriptor.chunkCount ||
        unique.tokens.size !== unique.descriptor.tokenCount
      ) {
        throw new Error(
          `Snapshot UNIQUE generation is incomplete: ${unique.descriptor.namespaceId}`,
        );
      }
      trial.#uniqueKeys.set(unique.descriptor.namespaceId, unique.tokens);
      uniqueNamespaces.add(unique.descriptor.namespaceId);
      unique = undefined;
    };
    const finishPosting = (): void => {
      if (posting === undefined) return;
      if (
        posting.nextOrdinal !== posting.descriptor.chunkCount ||
        posting.totalTokens !== posting.descriptor.totalTokens
      ) {
        throw new Error(
          `Snapshot posting generation is incomplete: ${posting.descriptor.storageColumnId}`,
        );
      }
      trial.#ftsBases.set(`${posting.descriptor.tableId}/${posting.descriptor.storageColumnId}`, {
        coversVersion: posting.descriptor.coversVersion,
        chunks: posting.chunks,
        totalTokens: posting.descriptor.totalTokens,
      });
      postingColumns.add(`${posting.descriptor.tableId}/${posting.descriptor.storageColumnId}`);
      posting = undefined;
    };

    let phase = 0;
    for (const item of items) {
      const itemPhase = snapshotMetadataItemPhase(item);
      if (itemPhase < phase) throw new Error("Snapshot metadata kinds are out of order");
      if (itemPhase !== phase) {
        finishUnique();
        finishPosting();
        phase = itemPhase;
      }
      switch (item.kind) {
        case "table": {
          if (previousTableId !== undefined && item.record.id <= previousTableId) {
            throw new Error("Snapshot tables are not in canonical ID order");
          }
          previousTableId = item.record.id;
          const record = structuredClone(item.record);
          validateTableRuntimeRecord(record, `Snapshot table ${item.record.id}`);
          if (trial.#tables.has(record.id) || trial.#tableIdsByName.has(record.name)) {
            throw new Error(`Snapshot repeats a table identity: ${record.id}`);
          }
          if (typeof item.nextRowId !== "bigint") {
            throw new TypeError("Snapshot row counter must be a bigint");
          }
          assertCounterEndInRange(item.nextRowId, MAX_ROW_ID_EXCLUSIVE_END, "Snapshot row counter");
          trial.#setTable(record);
          trial.#tableIdsByName.set(record.name, record.id);
          trial.#nextRowIds.set(record.id, item.nextRowId);
          const seenColumns = new Set<string>();
          for (const entry of item.autoIncrement) {
            if (seenColumns.has(entry.columnId)) {
              throw new Error(`Snapshot repeats an auto-increment counter: ${entry.columnId}`);
            }
            seenColumns.add(entry.columnId);
            trial.#assertAutoIncrementColumn(record.id, entry.columnId);
            if (typeof entry.next !== "bigint") {
              throw new TypeError("Snapshot auto-increment counter must be a bigint");
            }
            assertCounterEndInRange(
              entry.next,
              MAX_AUTO_INCREMENT_EXCLUSIVE_END,
              "Snapshot auto-increment counter",
            );
            trial.#nextAutoIncrement.set(`${record.id}/${entry.columnId}`, entry.next);
          }
          break;
        }
        case "segment": {
          if (previousSegmentId !== undefined && item.record.id <= previousSegmentId) {
            throw new Error("Snapshot segments are not in canonical ID order");
          }
          previousSegmentId = item.record.id;
          trial.#setSegment(
            validateSegmentRuntimeRecord(item.record, `Snapshot segment ${item.record.id}`),
          );
          break;
        }
        case "transaction": {
          if (previousTransactionId !== undefined && item.record.id <= previousTransactionId) {
            throw new Error("Snapshot transactions are not in canonical ID order");
          }
          previousTransactionId = item.record.id;
          const record = validateTransactionRuntimeRecord(
            item.record,
            `Snapshot transaction ${item.record.id}`,
          );
          if (record.status !== "committed" || record.committedVersion === null) {
            throw new Error(`Snapshot transaction is not committed: ${record.id}`);
          }
          trial.#setTransaction(structuredClone(record));
          break;
        }
        case "unique-generation": {
          finishUnique();
          if (
            previousUniqueNamespace !== undefined &&
            item.namespaceId <= previousUniqueNamespace
          ) {
            throw new Error("Snapshot UNIQUE namespaces are not in canonical order");
          }
          previousUniqueNamespace = item.namespaceId;
          trial.#assertSnapshotUniqueDescriptor(item);
          declaredUniqueEntries = safeStorageSum(declaredUniqueEntries, item.tokenCount);
          assertSnapshotImportAcceleratorUsage(0, declaredUniqueEntries);
          unique = {
            descriptor: item,
            tokens: new OrderedStringSet(),
            nextOrdinal: 0,
            previousToken: undefined,
          };
          break;
        }
        case "unique-chunk": {
          if (
            unique?.descriptor.namespaceId !== item.namespaceId ||
            item.generationId !== unique.descriptor.generationId ||
            item.ordinal !== unique.nextOrdinal
          ) {
            throw new Error(`Snapshot UNIQUE chunk is out of order: ${item.namespaceId}`);
          }
          retainAcceleratorChunk(item);
          for (const token of item.keyTokens) {
            if (unique.previousToken !== undefined && token <= unique.previousToken) {
              throw new Error(
                `Snapshot UNIQUE tokens are not globally ordered: ${item.namespaceId}`,
              );
            }
            unique.tokens.add(token);
            unique.previousToken = token;
          }
          unique.nextOrdinal += 1;
          break;
        }
        case "posting-generation": {
          finishPosting();
          const identity = `${item.tableId}/${item.storageColumnId}`;
          if (previousPostingIdentity !== undefined && identity <= previousPostingIdentity) {
            throw new Error("Snapshot posting columns are not in canonical order");
          }
          previousPostingIdentity = identity;
          trial.#assertSnapshotPostingDescriptor(item, header.databaseVersion);
          posting = {
            descriptor: item,
            chunks: [],
            nextOrdinal: 0,
            previousTerm: undefined,
            totalTokens: 0,
          };
          break;
        }
        case "posting-chunk": {
          if (
            posting?.descriptor.storageColumnId !== item.storageColumnId ||
            item.generationId !== posting.descriptor.generationId ||
            item.ordinal !== posting.nextOrdinal
          ) {
            throw new Error(`Snapshot posting chunk is out of order: ${item.storageColumnId}`);
          }
          retainAcceleratorChunk(item);
          const chunk = structuredClone(item.postings) as FtsPosting[];
          if (
            posting.previousTerm !== undefined &&
            chunk[0] !== undefined &&
            chunk[0].term <= posting.previousTerm
          ) {
            throw new Error(
              `Snapshot posting terms are not globally ordered: ${item.storageColumnId}`,
            );
          }
          posting.previousTerm = chunk.at(-1)?.term ?? posting.previousTerm;
          for (const term of chunk) {
            for (const frequency of term.tf) {
              if (posting.descriptor.ownerKind === "secondary-index" && frequency !== 1) {
                throw new Error(
                  `Snapshot secondary posting frequency is not one: ${item.storageColumnId}`,
                );
              }
              posting.totalTokens = safeStorageSum(posting.totalTokens, frequency);
            }
          }
          posting.chunks.push(chunk);
          posting.nextOrdinal += 1;
          break;
        }
      }
    }
    finishUnique();
    finishPosting();

    for (const table of trial.#tables.values()) {
      if (table.uniqueKeyColumnId !== undefined && !uniqueNamespaces.has(table.id)) {
        throw new Error(`Snapshot is missing table UNIQUE membership: ${table.id}`);
      }
      for (const [indexId, index] of Object.entries(table.secondaryIndexes ?? {})) {
        if (
          index.uniqueEnforced === true &&
          !uniqueNamespaces.has(secondaryUniqueKeyNamespace(table.id, indexId))
        ) {
          throw new Error(`Snapshot is missing UNIQUE-index membership: ${index.name}`);
        }
      }
      for (const storageColumnId of activePostingStorageColumnIds(table)) {
        if (!postingColumns.has(`${table.id}/${storageColumnId}`)) {
          // Missing accelerators are never treated as truth. A restored catalog marks them
          // invalid so the engine scans/rebuilds instead of serving incomplete postings.
          const retained = new Set(activePostingStorageColumnIds(table));
          retained.delete(storageColumnId);
          const withoutFts = invalidateUncoveredFtsColumns(table, retained) ?? table;
          const invalidated = invalidateUncoveredSecondaryIndexes(withoutFts, retained);
          trial.#tables.set(table.id, invalidated ?? withoutFts);
        }
      }
    }

    let liveBlockCount = 0;
    let liveBlockBytes = 0;
    let previousBlockId: string | undefined;
    for (const block of blocks) {
      validateId(block.blockId);
      if (previousBlockId !== undefined && block.blockId <= previousBlockId) {
        throw new Error("Snapshot blocks are not in canonical ID order");
      }
      previousBlockId = block.blockId;
      if (
        !Number.isSafeInteger(block.byteLength) ||
        block.byteLength < 1 ||
        !Number.isSafeInteger(block.checksum) ||
        block.checksum < 0 ||
        block.checksum > 0xffff_ffff ||
        !trial.#physical.hasBlock(block.blockId) ||
        trial.#physical.blockByteLength(block.blockId) !== block.byteLength ||
        trial.#physical.blockChecksum?.(block.blockId) !== block.checksum
      ) {
        throw new Error(`Snapshot block identity is invalid: ${block.blockId}`);
      }
      trial.#manifestBlocks.set(block.blockId, {
        ...block,
        addedVersion: header.databaseVersion,
        removedVersion: null,
      });
      liveBlockCount += 1;
      liveBlockBytes = safeStorageSum(liveBlockBytes, block.byteLength);
    }
    trial.#setManifest({
      version: header.databaseVersion,
      previousVersion: null,
      createdAt: header.createdAt,
      changedTableIds: [],
      liveBlockCount,
      liveBlockBytes,
    });
    trial.#currentVersion = header.databaseVersion;
    trial.#catalogEpoch = 1;
    trial.#schemaEpoch = 1;
    const state = trial.dump();
    validateRecordCoreState(state, trial.#physical);
    this.load(state);
  }

  #assertSnapshotUniqueDescriptor(
    item: Extract<SnapshotUniqueItem, { kind: "unique-generation" }>,
  ): void {
    const table = this.#tables.get(item.tableId);
    if (table === undefined)
      throw new Error(`Snapshot UNIQUE generation has no table: ${item.tableId}`);
    if (item.indexId === null) {
      if (table.uniqueKeyColumnId === undefined || item.namespaceId !== table.id) {
        throw new Error(`Snapshot table UNIQUE namespace is invalid: ${item.namespaceId}`);
      }
      return;
    }
    const index = table.secondaryIndexes?.[item.indexId];
    if (
      index?.unique !== true ||
      index.uniqueEnforced !== true ||
      item.namespaceId !== secondaryUniqueKeyNamespace(item.tableId, item.indexId)
    ) {
      throw new Error(`Snapshot secondary UNIQUE namespace is invalid: ${item.namespaceId}`);
    }
  }

  #assertSnapshotPostingDescriptor(item: SnapshotPostingGenerationItem, version: number): void {
    const table = this.#tables.get(item.tableId);
    if (table === undefined || item.coversVersion !== version) {
      throw new Error(`Snapshot posting generation is not current: ${item.storageColumnId}`);
    }
    if (item.ownerKind === "fts-column") {
      if (
        item.ownerId !== item.storageColumnId ||
        table.ftsColumns?.[item.ownerId]?.state !== "ready"
      ) {
        throw new Error(`Snapshot full-text posting owner is invalid: ${item.ownerId}`);
      }
    } else {
      const index = table.secondaryIndexes?.[item.ownerId];
      if (index?.state !== "ready" || index.storageColumnId !== item.storageColumnId) {
        throw new Error(`Snapshot secondary posting owner is invalid: ${item.ownerId}`);
      }
    }
  }

  /**
   * The complete record state as one value — the checkpoint payload of a log-structured store.
   * Everything `load` needs to reproduce this core exactly, including operational state the
   * portable snapshot format deliberately drops (transactions, leases, jobs, counters). The
   * shape is JSON plus bigints; `encodeRecordJson` in this module serializes it verbatim.
   *
   * The arrays are fresh but their records alias live state: serialize (or clone) the result
   * before the next mutation. The one caller serializes synchronously in the same run, and
   * `load` clones on the way in — skipping a deep clone here saves tens of milliseconds per
   * checkpoint at scale.
   */
  dump(): RecordCoreState {
    return {
      currentVersion: this.#currentVersion,
      catalogEpoch: this.#catalogEpoch,
      schemaEpoch: this.#schemaEpoch,
      manifests: [...this.#manifests.values()],
      manifestBlocks: [...this.#manifestBlocks.values()],
      transactions: [...this.#transactions.values()],
      tables: [...this.#tables.values()],
      segments: [...this.#segments.values()],
      leases: [...this.#leases.values()],
      compactionJobs: [...this.#compactionJobs.values()],
      garbageCollectionJobs: [...this.#garbageCollectionJobs.values()],
      nextRowIds: [...this.#nextRowIds.entries()],
      nextAutoIncrement: [...this.#nextAutoIncrement.entries()],
      ftsBases: [...this.#ftsBases.entries()],
      ftsDeltas: [...this.#ftsDeltas.entries()].map(
        ([key, deltas]) => [key, [...deltas.entries()]] as const,
      ),
      uniqueKeys: [...this.#uniqueKeys.entries()].map(
        ([tableId, tokens]) => [tableId, [...tokens]] as const,
      ),
      uniqueKeyBuilds: [...this.#uniqueKeyBuilds.values()].map((state) => [
        state.record,
        state.chunks,
        state.completedInput,
      ]),
      tempOwners: [...this.#tempOwners.values()],
    };
  }

  /** Replaces the whole record state with a dump's content. */
  load(state: RecordCoreState): void {
    const cloned = structuredClone(state);
    validateRecordCoreState(cloned, this.#physical);
    // A recovery candidate may intentionally discard an unpublished WAL suffix whose physical
    // blocks were already reclaimed. Clearing the old candidate state must therefore not need
    // those obsolete bytes merely to decrement counters; the counters are reset immediately.
    this.#transactionBlockLengthOverrides = new Map(
      [...this.#transactions.values()].flatMap((record) =>
        record.pendingBlockIds.map((id) => [id, this.#physical.blockByteLength(id) ?? 0] as const),
      ),
    );
    for (const map of [
      this.#manifests,
      this.#manifestBlocks,
      this.#transactions,
      this.#tables,
      this.#triggerOwnersByName,
      this.#triggerOwnersById,
      this.#tableIdsByName,
      this.#segments,
      this.#leases,
      this.#compactionJobs,
      this.#garbageCollectionJobs,
      this.#nextRowIds,
      this.#nextAutoIncrement,
      this.#ftsBases,
      this.#ftsDeltas,
      this.#uniqueKeys,
      this.#uniqueKeyBuilds,
      this.#tempOwners,
    ] as Array<Map<unknown, unknown>>) {
      map.clear();
    }
    this.#transactionBlockLengthOverrides = undefined;
    this.#activeTransactionCount = 0;
    this.#activeStagedBlockCount = 0;
    this.#activeStagedSegmentCount = 0;
    this.#activeStagedArtifactBytes = 0;
    this.#catalogRetainedBytes = 0;
    this.#pendingCatalogRecords = 0;
    this.#pendingCatalogRetainedBytes = 0;
    this.#activeTransactionExpiries.clear();
    this.#activeTransactionExpiryKeys.clear();
    this.#currentVersion = cloned.currentVersion;
    this.#catalogEpoch = cloned.catalogEpoch;
    this.#schemaEpoch = cloned.schemaEpoch;
    this.#prunedManifestRemovalCursor = null;
    this.#manifestRetainedBytes = 0;
    this.#segmentRetainedBytes = 0;
    for (const manifest of cloned.manifests) this.#setManifest(manifest);
    for (const record of cloned.manifestBlocks) this.#manifestBlocks.set(record.blockId, record);
    for (const record of cloned.transactions) this.#setTransaction(record);
    for (const record of cloned.tables) {
      this.#setTable(record);
      this.#tableIdsByName.set(record.name, record.id);
    }
    for (const record of cloned.segments) this.#setSegment(record);
    for (const record of cloned.leases) this.#leases.set(record.id, record);
    for (const record of cloned.compactionJobs) this.#compactionJobs.set(record.id, record);
    for (const record of cloned.garbageCollectionJobs) {
      this.#garbageCollectionJobs.set(record.id, record);
    }
    for (const [tableId, next] of cloned.nextRowIds) this.#nextRowIds.set(tableId, next);
    for (const [key, next] of cloned.nextAutoIncrement) this.#nextAutoIncrement.set(key, next);
    for (const [key, base] of cloned.ftsBases) this.#ftsBases.set(key, base);
    for (const [key, deltas] of cloned.ftsDeltas) this.#ftsDeltas.set(key, new Map(deltas));
    for (const [tableId, tokens] of cloned.uniqueKeys) {
      this.#uniqueKeys.set(tableId, new OrderedStringSet(tokens));
    }
    this.#activeUniqueKeyBuildCount = 0;
    this.#uniqueKeyBuildStagedBytes = 0;
    this.#uniqueKeyBuildStagedEntries = 0;
    for (const [record, chunks, completedInput] of cloned.uniqueKeyBuilds) {
      const tokens = new Set(chunks.flat());
      this.#uniqueKeyBuilds.set(record.buildId, { record, chunks, tokens, completedInput });
      if (record.state === "active") {
        this.#activeUniqueKeyBuildCount += 1;
        this.#uniqueKeyBuildStagedBytes += record.retainedBytes;
        this.#uniqueKeyBuildStagedEntries += record.tokenCount;
      }
    }
    for (const record of cloned.tempOwners) this.#tempOwners.set(record.ownerId, record);
  }

  #assertSnapshotAvailable(version: number | null): void {
    assertSnapshotAvailable(version, this.#manifests);
    this.#assertPinnedManifestLag(version);
  }

  #assertPendingArtifactsAvailable(
    transaction: TransactionRecord,
    validateBlocks = true,
    validateSegments = true,
  ): void {
    assertPendingArtifactsAvailable(
      transaction,
      this.#physical,
      this.#segments,
      validateBlocks,
      validateSegments,
    );
  }
}

export function validateId(id: string): void {
  if (typeof id !== "string" || id.length === 0) throw new TypeError("Block ID cannot be empty");
  if (id.length > MAX_STORAGE_ID_CHARACTERS) {
    throw new TypeError(`Storage ID exceeds ${String(MAX_STORAGE_ID_CHARACTERS)} characters`);
  }
  assertWellFormedString(id, "Storage ID");
}

/** Validates payload ownership at the public storage boundary without copying the hot path. */
export function validateBlockWriteBytes(bytes: unknown): asserts bytes is Uint8Array {
  if (!(bytes instanceof Uint8Array)) throw new TypeError("Block bytes must be a Uint8Array");
  if (typeof SharedArrayBuffer !== "undefined" && bytes.buffer instanceof SharedArrayBuffer) {
    throw new TypeError("SharedArrayBuffer-backed block bytes are not accepted");
  }
}

export function validateFtsBaseInput(
  input: { coversVersion: number; chunks: FtsPosting[][]; totalTokens: number },
  label: string,
): void {
  if (
    !Number.isSafeInteger(input.coversVersion) ||
    input.coversVersion < -1 ||
    !Number.isSafeInteger(input.totalTokens) ||
    input.totalTokens < 0
  ) {
    throw new TypeError(`${label} metadata is invalid`);
  }
  if (input.chunks.length > MAX_FTS_BASE_CHUNKS) {
    throw new RangeError(`${label} exceeds the chunk-count limit`);
  }
  if (validateFtsPostingChunks(input.chunks, label) !== input.totalTokens) {
    throw new TypeError(`${label} token total does not match its postings`);
  }
}

/** Validates each canonical term-sorted postings chunk without copying it. */
export function validateFtsPostingChunks(chunks: unknown, label: string): number {
  if (!Array.isArray(chunks)) throw new TypeError(`${label} chunks must be an array`);
  let totalTokens = 0;
  for (const [chunkIndex, chunk] of chunks.entries()) {
    if (!Array.isArray(chunk) || chunk.length === 0) {
      throw new TypeError(`${label} chunk ${String(chunkIndex)} must not be empty`);
    }
    if (chunk.length > MAX_FTS_POSTINGS_PER_CHUNK) {
      throw new RangeError(`${label} chunk exceeds the posting-count limit`);
    }
    let rowIdCount = 0;
    let encodedBytes = 20 + varuintByteLength(BigInt(chunk.length));
    let previousTerm: string | undefined;
    for (const postingValue of chunk as unknown[]) {
      if (
        typeof postingValue !== "object" ||
        postingValue === null ||
        Array.isArray(postingValue) ||
        Object.keys(postingValue).some((key) => key !== "term" && key !== "rowIds" && key !== "tf")
      ) {
        throw new TypeError(`${label} posting is invalid`);
      }
      const record = postingValue as Partial<FtsPosting>;
      if (
        typeof record.term !== "string" ||
        record.term.length === 0 ||
        !Array.isArray(record.rowIds) ||
        !Array.isArray(record.tf) ||
        (previousTerm !== undefined && record.term <= previousTerm)
      ) {
        throw new TypeError(`${label} terms are not strictly sorted`);
      }
      assertWellFormedString(record.term, `${label} posting term`);
      if (record.term.length > MAX_FTS_POSTING_TERM_CHARACTERS) {
        throw new RangeError(`${label} posting term exceeds the character limit`);
      }
      if (record.rowIds.length !== record.tf.length || record.rowIds.length === 0) {
        throw new TypeError(`${label} posting arrays are empty or have different lengths`);
      }
      rowIdCount += record.rowIds.length;
      if (rowIdCount > MAX_FTS_POSTING_ROW_IDS_PER_CHUNK) {
        throw new RangeError(`${label} chunk exceeds the row-ID limit`);
      }
      const termBytes = postingTextEncoder.encode(record.term).byteLength;
      encodedBytes +=
        varuintByteLength(BigInt(termBytes)) +
        termBytes +
        varuintByteLength(BigInt(record.rowIds.length));
      let previousRowId: bigint | undefined;
      for (const [index, rowId] of record.rowIds.entries()) {
        if (
          typeof rowId !== "bigint" ||
          rowId < 1n ||
          rowId > MAX_ROW_ID ||
          (previousRowId !== undefined && rowId <= previousRowId)
        ) {
          throw new TypeError(`${label} posting row IDs are not positive and strictly sorted`);
        }
        const frequency = record.tf[index];
        if (
          typeof frequency !== "number" ||
          !Number.isSafeInteger(frequency) ||
          frequency <= 0 ||
          frequency > MAX_FTS_TOKENS_PER_DOCUMENT
        ) {
          throw new TypeError(`${label} posting frequency must be a positive whole number`);
        }
        totalTokens = safeStorageSum(totalTokens, frequency);
        encodedBytes += varuintByteLength(rowId - (previousRowId ?? 0n));
        encodedBytes += varuintByteLength(BigInt(frequency));
        previousRowId = rowId;
      }
      if (encodedBytes > MAX_STORED_BLOCK_BYTE_LENGTH) {
        throw new RangeError(`${label} chunk exceeds the stored-byte limit`);
      }
      previousTerm = record.term;
    }
  }
  return totalTokens;
}

/** K-way canonical merge that retains only one term's row map plus one cursor per source. */
function* mergedFtsPostingIterator(
  chunkLists: ReadonlyArray<readonly FtsPosting[]>,
): IterableIterator<FtsPosting> {
  interface Cursor {
    postings: readonly FtsPosting[];
    position: number;
    ordinal: number;
  }
  const heap: Cursor[] = [];
  const termOf = (cursor: Cursor): string => cursor.postings[cursor.position]?.term ?? "";
  const before = (left: Cursor, right: Cursor): boolean =>
    termOf(left) < termOf(right) ||
    (termOf(left) === termOf(right) && left.ordinal < right.ordinal);
  const push = (cursor: Cursor): void => {
    heap.push(cursor);
    let child = heap.length - 1;
    while (child > 0) {
      const parent = (child - 1) >>> 1;
      const childCursor = heap[child];
      const parentCursor = heap[parent];
      if (
        childCursor === undefined ||
        parentCursor === undefined ||
        !before(childCursor, parentCursor)
      ) {
        break;
      }
      heap[parent] = childCursor;
      heap[child] = parentCursor;
      child = parent;
    }
  };
  const pop = (): Cursor | undefined => {
    const first = heap[0];
    const last = heap.pop();
    if (first === undefined || last === undefined || heap.length === 0) return first;
    heap[0] = last;
    let parent = 0;
    for (;;) {
      const left = parent * 2 + 1;
      if (left >= heap.length) break;
      const right = left + 1;
      const leftCursor = heap[left];
      const rightCursor = heap[right];
      if (leftCursor === undefined) break;
      const child = rightCursor !== undefined && before(rightCursor, leftCursor) ? right : left;
      const childCursor = heap[child];
      const parentCursor = heap[parent];
      if (
        childCursor === undefined ||
        parentCursor === undefined ||
        !before(childCursor, parentCursor)
      ) {
        break;
      }
      heap[parent] = childCursor;
      heap[child] = parentCursor;
      parent = child;
    }
    return first;
  };
  for (const [ordinal, postings] of chunkLists.entries()) {
    if (postings.length > 0) push({ postings, position: 0, ordinal });
  }
  while (heap.length > 0) {
    const head = heap[0];
    if (head === undefined) throw new Error("Posting merge heap is inconsistent");
    const term = termOf(head);
    const frequencies = new Map<bigint, number>();
    for (;;) {
      const next = heap[0];
      if (next === undefined || termOf(next) !== term) break;
      const cursor = pop();
      if (cursor === undefined) throw new Error("Posting merge heap is inconsistent");
      const posting = cursor.postings[cursor.position];
      if (posting === undefined) throw new Error("Posting merge cursor is inconsistent");
      for (const [index, rowId] of posting.rowIds.entries()) {
        frequencies.set(rowId, safeStorageSum(frequencies.get(rowId) ?? 0, posting.tf[index] ?? 0));
      }
      cursor.position += 1;
      if (cursor.position < cursor.postings.length) push(cursor);
    }
    const rowIds = [...frequencies.keys()].sort((left, right) =>
      left < right ? -1 : left > right ? 1 : 0,
    );
    yield { term, rowIds, tf: rowIds.map((rowId) => frequencies.get(rowId) ?? 0) };
  }
}

function validateFtsChangesRuntime(value: unknown): readonly FtsChanges[] {
  if (value === undefined) return [];
  if (!Array.isArray(value)) throw new TypeError("Full-text changes must be an array");
  const tableIds = new Set<string>();
  for (const [changeIndex, changeValue] of (value as unknown[]).entries()) {
    if (
      typeof changeValue !== "object" ||
      changeValue === null ||
      Array.isArray(changeValue) ||
      !hasExactFields(changeValue, ["tableId", "columns"])
    ) {
      throw new TypeError(`Full-text change ${String(changeIndex)} is invalid`);
    }
    const change = changeValue as { tableId: unknown; columns: unknown };
    if (typeof change.tableId !== "string") {
      throw new TypeError(`Full-text change ${String(changeIndex)} has an invalid table ID`);
    }
    validateId(change.tableId);
    if (tableIds.has(change.tableId)) {
      throw new TypeError(`Full-text changes repeat table ${change.tableId}`);
    }
    tableIds.add(change.tableId);
    if (!Array.isArray(change.columns)) {
      throw new TypeError(`Full-text change ${String(changeIndex)} columns must be an array`);
    }
    const columnIds = new Set<string>();
    for (const [columnIndex, columnValue] of (change.columns as unknown[]).entries()) {
      if (
        typeof columnValue !== "object" ||
        columnValue === null ||
        Array.isArray(columnValue) ||
        !hasExactFields(columnValue, ["columnId", "postings", "totalTokens"])
      ) {
        throw new TypeError(
          `Full-text change ${String(changeIndex)} column ${String(columnIndex)} is invalid`,
        );
      }
      const column = columnValue as {
        columnId: unknown;
        postings: unknown;
        totalTokens: unknown;
      };
      if (typeof column.columnId !== "string") {
        throw new TypeError("Full-text change has an invalid column ID");
      }
      validateId(column.columnId);
      if (columnIds.has(column.columnId)) {
        throw new TypeError(`Full-text changes repeat column ${column.columnId}`);
      }
      columnIds.add(column.columnId);
      if (!Number.isSafeInteger(column.totalTokens) || (column.totalTokens as number) < 0) {
        throw new TypeError("Full-text change token count must be a non-negative whole number");
      }
      if (!Array.isArray(column.postings)) {
        throw new TypeError("Full-text change postings must be an array");
      }
      if (column.postings.length > 0) {
        if (
          validateFtsPostingChunks([column.postings], "Full-text change") !== column.totalTokens
        ) {
          throw new TypeError("Full-text change token total does not match its postings");
        }
      } else if (column.totalTokens !== 0) {
        throw new TypeError("Empty full-text changes must have a zero token total");
      }
    }
  }
  return value as FtsChanges[];
}

function hasExactFields(value: object, fields: readonly string[]): boolean {
  const keys = Object.keys(value);
  return keys.length === fields.length && keys.every((key) => fields.includes(key));
}

function validateTimestampRuntime(value: unknown, label: string): asserts value is string {
  validateCanonicalTimestamp(value, label);
}

function validateUniqueIdsRuntime(value: unknown, label: string): string[] {
  if (!Array.isArray(value)) throw new TypeError(`${label} must be an array`);
  const result: string[] = [];
  for (const candidate of value as unknown[]) {
    if (typeof candidate !== "string") throw new TypeError(`${label} must contain strings`);
    validateId(candidate);
    result.push(candidate);
  }
  if (new Set(result).size !== result.length) throw new TypeError(`${label} contains duplicates`);
  return result;
}

function validateUniqueWholeNumbersRuntime(value: unknown, label: string): number[] {
  if (!Array.isArray(value)) throw new TypeError(`${label} must be an array`);
  const result: number[] = [];
  for (const candidate of value as unknown[]) {
    if (!Number.isSafeInteger(candidate) || (candidate as number) < 0) {
      throw new TypeError(`${label} must contain non-negative whole numbers`);
    }
    result.push(candidate as number);
  }
  if (new Set(result).size !== result.length) throw new TypeError(`${label} contains duplicates`);
  return result;
}

function deepRecordEqual(left: unknown, right: unknown): boolean {
  if (Object.is(left, right)) return true;
  if (typeof left !== "object" || left === null || typeof right !== "object" || right === null) {
    return false;
  }
  if (Array.isArray(left) || Array.isArray(right)) {
    return (
      Array.isArray(left) &&
      Array.isArray(right) &&
      left.length === right.length &&
      left.every((entry, index) => deepRecordEqual(entry, right[index]))
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
        key === rightKeys[index] && deepRecordEqual(leftRecord[key], rightRecord[key]),
    )
  );
}

const SEGMENT_RECORD_FIELDS = new Set([
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
]);

export function validateSegmentRuntimeRecord(value: unknown, label: string): SegmentRecord {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new TypeError(`${label} must be an object`);
  }
  const record = value as SegmentRecord;
  if (Object.keys(record).some((field) => !SEGMENT_RECORD_FIELDS.has(field))) {
    throw new TypeError(`${label} has unknown fields`);
  }
  validateId(record.id);
  validateId(record.tableId);
  validateId(record.transactionId);
  if (!Number.isSafeInteger(record.rowCount) || record.rowCount <= 0) {
    throw new TypeError(`${label} row count must be positive`);
  }
  if (
    typeof record.rowIdStart !== "bigint" ||
    typeof record.rowIdEndExclusive !== "bigint" ||
    record.rowIdStart < 0n ||
    record.rowIdStart > MAX_ROW_ID ||
    record.rowIdEndExclusive > MAX_ROW_ID_EXCLUSIVE_END ||
    record.rowIdEndExclusive < record.rowIdStart
  ) {
    throw new TypeError(`${label} has an invalid row ID envelope`);
  }
  const rawColumnBlockIds: unknown = Reflect.get(record, "columnBlockIds");
  if (
    typeof rawColumnBlockIds !== "object" ||
    rawColumnBlockIds === null ||
    Array.isArray(rawColumnBlockIds)
  ) {
    throw new TypeError(`${label} column blocks must be an object`);
  }
  for (const [columnId, blockIds] of Object.entries(rawColumnBlockIds)) {
    validateId(columnId);
    const normalizedBlockIds = validateUniqueIdsRuntime(
      blockIds,
      `${label} column ${columnId} blocks`,
    );
    if (normalizedBlockIds.length === 0)
      throw new TypeError(`${label} column ${columnId} has no blocks`);
  }
  const rawKind: unknown = record.kind;
  const kind = rawKind;
  if (
    kind !== "insert" &&
    kind !== "upsert" &&
    kind !== "update" &&
    kind !== "delete" &&
    kind !== "base"
  ) {
    throw new TypeError(`${label} has an invalid kind`);
  }
  if (record.keyColumnId !== undefined) validateId(record.keyColumnId);
  for (const [field, candidate] of [
    ["level", record.level],
    ["commit ordinal", record.commitOrdinal],
  ] as const) {
    if (!Number.isSafeInteger(candidate) || candidate < 0 || (field === "level" && candidate > 2)) {
      throw new TypeError(`${label} ${field} is invalid`);
    }
  }
  if (
    record.partitionOrdinal !== undefined &&
    (!Number.isSafeInteger(record.partitionOrdinal) || record.partitionOrdinal < 0)
  ) {
    throw new TypeError(`${label} partition ordinal is invalid`);
  }
  if (record.level === 2 && record.partitionOrdinal === undefined) {
    throw new TypeError(`${label} level two requires a partition ordinal`);
  }
  if (record.level !== 2 && record.partitionOrdinal !== undefined) {
    throw new TypeError(`${label} partition ordinal requires level two`);
  }
  if (!Number.isFinite(record.logicalOrder) || record.logicalOrder < 0) {
    throw new TypeError(`${label} logical order is invalid`);
  }
  const zeroKeyedEnvelope =
    record.rowIdStart === 0n &&
    record.rowIdEndExclusive === 0n &&
    (kind === "update" || kind === "delete") &&
    record.keyColumnId !== undefined;
  const rawRowIdSpans: unknown = record.rowIdSpans;
  if (!Array.isArray(rawRowIdSpans)) {
    throw new TypeError(`${label} row ID spans are invalid`);
  }
  if (rawRowIdSpans.length > 0) {
    if (zeroKeyedEnvelope) {
      throw new TypeError(`${label} row ID spans are invalid`);
    }
    let expectedRowStart = 0;
    let minimumRowId: bigint | undefined;
    let maximumRowIdEnd = 0n;
    const intervals: Array<{ start: bigint; end: bigint }> = [];
    for (const spanValue of rawRowIdSpans as unknown[]) {
      if (typeof spanValue !== "object" || spanValue === null || Array.isArray(spanValue)) {
        throw new TypeError(`${label} row ID spans are invalid`);
      }
      const span = spanValue as Record<string, unknown>;
      if (
        Object.keys(span).some(
          (field) => field !== "rowStart" && field !== "rowCount" && field !== "rowIdStart",
        ) ||
        !Number.isSafeInteger(span.rowStart) ||
        (span.rowStart as number) !== expectedRowStart ||
        !Number.isSafeInteger(span.rowCount) ||
        (span.rowCount as number) <= 0 ||
        typeof span.rowIdStart !== "bigint" ||
        span.rowIdStart < 1n ||
        span.rowIdStart > MAX_ROW_ID
      ) {
        throw new TypeError(`${label} row ID spans are invalid`);
      }
      const rowStart = span.rowStart as number;
      const rowCount = span.rowCount as number;
      const rowIdStart = span.rowIdStart;
      const end = rowIdStart + BigInt(rowCount);
      if (end > MAX_ROW_ID_EXCLUSIVE_END) {
        throw new TypeError(`${label} row ID span exceeds uint64 storage`);
      }
      intervals.push({ start: rowIdStart, end });
      minimumRowId =
        minimumRowId === undefined || rowIdStart < minimumRowId ? rowIdStart : minimumRowId;
      if (end > maximumRowIdEnd) maximumRowIdEnd = end;
      expectedRowStart = rowStart + rowCount;
    }
    intervals.sort((left, right) =>
      left.start < right.start ? -1 : left.start > right.start ? 1 : 0,
    );
    if (
      intervals.some(
        (interval, index) => index > 0 && interval.start < (intervals[index - 1]?.end ?? 0n),
      )
    ) {
      throw new TypeError(`${label} row ID spans overlap`);
    }
    if (
      expectedRowStart !== record.rowCount ||
      minimumRowId !== record.rowIdStart ||
      maximumRowIdEnd !== record.rowIdEndExclusive
    ) {
      throw new TypeError(`${label} row ID spans do not match its envelope`);
    }
  } else if (
    !zeroKeyedEnvelope &&
    (record.rowIdStart < 1n ||
      record.rowIdEndExclusive !== record.rowIdStart + BigInt(record.rowCount))
  ) {
    throw new TypeError(`${label} row ID envelope does not match row count`);
  }
  validateTimestampRuntime(record.createdAt, `${label} creation timestamp`);
  return normalizeSegmentRecord(record);
}

function validateTransactionRuntimeRecord(value: unknown, label: string): TransactionRecord {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new TypeError(`${label} must be an object`);
  }
  const record = value as TransactionRecord;
  validateId(record.id);
  if (record.snapshotVersion !== null) {
    if (!Number.isSafeInteger(record.snapshotVersion) || record.snapshotVersion < 0) {
      throw new TypeError(`${label} snapshot version is invalid`);
    }
  }
  validateUniqueIdsRuntime(record.pendingBlockIds, `${label} pending block IDs`);
  validateUniqueIdsRuntime(record.pendingSegmentIds, `${label} pending segment IDs`);
  const status: unknown = record.status;
  if (status !== "active" && status !== "committed" && status !== "aborted") {
    throw new TypeError(`${label} status is invalid`);
  }
  if (!Number.isSafeInteger(record.revision) || record.revision < 0) {
    throw new TypeError(`${label} revision is invalid`);
  }
  validateTimestampRuntime(record.startedAt, `${label} start timestamp`);
  validateTimestampRuntime(record.updatedAt, `${label} update timestamp`);
  validateId(record.ownerId);
  validateBoundedExpiration(record.startedAt, record.expiresAt, label, MAX_LEASE_TTL_MS);
  if (record.committedVersion !== null) {
    if (!Number.isSafeInteger(record.committedVersion) || record.committedVersion < 0) {
      throw new TypeError(`${label} committed version is invalid`);
    }
  }
  if ((record.status === "committed") !== (record.committedVersion !== null)) {
    throw new TypeError(`${label} committed status and version disagree`);
  }
  if (record.status === "active") {
    if (!Number.isSafeInteger(record.schemaEpochGuard) || (record.schemaEpochGuard ?? -1) < 0) {
      throw new TypeError(`${label} schema epoch guard is invalid`);
    }
  } else if (record.schemaEpochGuard !== undefined) {
    throw new TypeError(`${label} terminal record retains a schema epoch guard`);
  }
  if (record.pendingTable !== undefined) {
    if (record.status !== "active") throw new TypeError(`${label} terminal record owns a table`);
    validateTableRuntimeRecord(record.pendingTable, `${label} pending table`);
    if (
      typeof record.pendingTableNextRowId !== "bigint" ||
      record.pendingTableNextRowId < 1n ||
      record.pendingTableNextRowId > MAX_ROW_ID_EXCLUSIVE_END
    ) {
      throw new TypeError(`${label} pending table row-ID state is invalid`);
    }
    if (!Number.isSafeInteger(record.catalogEpochGuard) || (record.catalogEpochGuard ?? -1) < 0) {
      throw new TypeError(`${label} pending table catalog epoch is invalid`);
    }
  } else if (record.pendingTableNextRowId !== undefined || record.catalogEpochGuard !== undefined) {
    throw new TypeError(`${label} has pending-table state without a table`);
  }
  return record;
}

function exactStringPartition(
  current: readonly string[],
  retained: readonly string[],
  removed: readonly string[],
): boolean {
  if (retained.length + removed.length !== current.length) return false;
  return (
    retained.every((value, index) => current[index] === value) &&
    removed.every((value, index) => current[retained.length + index] === value)
  );
}

/** Exhaustive checkpoint validation before `load` clears any live state. */
function validateRecordCoreState(state: RecordCoreState, physical: PhysicalBlocks): void {
  const runtimeState: unknown = state;
  if (typeof runtimeState !== "object" || runtimeState === null) {
    throw new TypeError("Checkpoint record state must be an object");
  }
  for (const key of [
    "manifests",
    "manifestBlocks",
    "transactions",
    "tables",
    "segments",
    "leases",
    "compactionJobs",
    "garbageCollectionJobs",
    "nextRowIds",
    "nextAutoIncrement",
    "ftsBases",
    "ftsDeltas",
    "uniqueKeys",
    "uniqueKeyBuilds",
    "tempOwners",
  ] as const) {
    if (!Array.isArray(state[key])) throw new TypeError(`Checkpoint ${key} must be an array`);
  }
  const whole = (value: number, label: string): void => {
    if (!Number.isSafeInteger(value) || value < 0) {
      throw new TypeError(`${label} must be a non-negative whole number`);
    }
  };
  const unique = <T>(
    values: readonly T[],
    key: (value: T) => string,
    label: string,
  ): Set<string> => {
    const keys = new Set<string>();
    for (const value of values) {
      const id = key(value);
      if (typeof id !== "string" || id.length === 0) {
        throw new TypeError(`${label} has an invalid identity`);
      }
      if (keys.has(id)) throw new TypeError(`${label} repeats identity: ${id}`);
      keys.add(id);
    }
    return keys;
  };
  const uniqueStrings = (values: readonly string[], label: string): void => {
    if (values.some((value) => typeof value !== "string" || value.length === 0)) {
      throw new TypeError(`${label} contains an empty identity`);
    }
    if (new Set(values).size !== values.length) throw new TypeError(`${label} contains duplicates`);
  };
  if (state.currentVersion !== null) whole(state.currentVersion, "Current manifest version");
  whole(state.catalogEpoch, "Catalog epoch");
  whole(state.schemaEpoch, "Schema epoch");
  if (state.schemaEpoch > state.catalogEpoch) {
    throw new TypeError("Schema epoch cannot exceed the catalog epoch");
  }
  const manifestVersions = new Set<number>();
  const manifestsByVersion = new Map<number, Manifest>();
  let highestVersion: number | null = null;
  for (const manifest of state.manifests) {
    whole(manifest.version, "Manifest version");
    if (manifestVersions.has(manifest.version)) {
      throw new TypeError(`Checkpoint repeats manifest version: ${String(manifest.version)}`);
    }
    manifestVersions.add(manifest.version);
    manifestsByVersion.set(manifest.version, manifest);
    validateTimestampRuntime(manifest.createdAt, `Manifest ${String(manifest.version)} createdAt`);
    if (manifest.prunedAt !== undefined)
      validateTimestampRuntime(manifest.prunedAt, "Manifest prunedAt");
    whole(manifest.liveBlockCount, `Manifest ${String(manifest.version)} live block count`);
    whole(manifest.liveBlockBytes, `Manifest ${String(manifest.version)} live block bytes`);
    validateCanonicalManifestChangedTableIds(manifest.changedTableIds);
    highestVersion =
      highestVersion === null ? manifest.version : Math.max(highestVersion, manifest.version);
  }
  if (state.manifests.length > MAX_MANIFEST_RECORDS) {
    throw new StorageResourceLimitError(
      "manifest record",
      state.manifests.length,
      MAX_MANIFEST_RECORDS,
    );
  }
  let manifestRetainedBytes = 0;
  for (const manifest of state.manifests) {
    manifestRetainedBytes = safeStorageSum(
      manifestRetainedBytes,
      manifestRecordRetainedReservationBytes(manifest),
    );
  }
  if (manifestRetainedBytes > MAX_MANIFEST_RETAINED_BYTES) {
    throw new StorageResourceLimitError(
      "manifest byte",
      manifestRetainedBytes,
      MAX_MANIFEST_RETAINED_BYTES,
    );
  }
  const orderedManifests = [...state.manifests].sort((left, right) => left.version - right.version);
  const readableVersions = orderedManifests
    .filter((manifest) => manifest.prunedAt === undefined)
    .map((manifest) => manifest.version);
  const addedCounts = new Map<number, number>();
  const addedBytes = new Map<number, number>();
  const removedCounts = new Map<number, number>();
  const removedBytes = new Map<number, number>();
  let retiredHistoryBytes = 0;
  const manifestBlockIds = unique(
    state.manifestBlocks,
    (record) => record.blockId,
    "Checkpoint manifest blocks",
  );
  void manifestBlockIds;
  for (const record of state.manifestBlocks) {
    validateId(record.blockId);
    whole(record.byteLength, `Manifest block ${record.blockId} byte length`);
    if (record.byteLength === 0) throw new TypeError(`Manifest block ${record.blockId} is empty`);
    whole(record.checksum, `Manifest block ${record.blockId} checksum`);
    if (record.checksum > 0xffff_ffff) {
      throw new TypeError(`Manifest block ${record.blockId} checksum is invalid`);
    }
    whole(record.addedVersion, `Manifest block ${record.blockId} added version`);
    if (record.removedVersion !== null) {
      retiredHistoryBytes = safeStorageSum(retiredHistoryBytes, record.byteLength);
      whole(record.removedVersion, `Manifest block ${record.blockId} removed version`);
      if (record.removedVersion <= record.addedVersion) {
        throw new TypeError(`Manifest block ${record.blockId} has an invalid lifetime`);
      }
    }
    addedCounts.set(record.addedVersion, (addedCounts.get(record.addedVersion) ?? 0) + 1);
    addedBytes.set(
      record.addedVersion,
      safeStorageSum(addedBytes.get(record.addedVersion) ?? 0, record.byteLength),
    );
    if (record.removedVersion !== null) {
      removedCounts.set(record.removedVersion, (removedCounts.get(record.removedVersion) ?? 0) + 1);
      removedBytes.set(
        record.removedVersion,
        safeStorageSum(removedBytes.get(record.removedVersion) ?? 0, record.byteLength),
      );
    }
    const readable = sortedVersionInInterval(
      readableVersions,
      record.addedVersion,
      record.removedVersion,
    );
    if (readable && !physical.hasBlock(record.blockId)) {
      throw new Error(`Readable manifest provenance references missing block: ${record.blockId}`);
    }
  }
  if (retiredHistoryBytes > MAX_RETIRED_HISTORY_BYTES) {
    throw new StorageResourceLimitError(
      "retired history byte",
      retiredHistoryBytes,
      MAX_RETIRED_HISTORY_BYTES,
    );
  }
  for (const [index, manifest] of orderedManifests.entries()) {
    if (manifest.previousVersion !== null) {
      whole(manifest.previousVersion, `Manifest ${String(manifest.version)} predecessor`);
      if (manifest.previousVersion >= manifest.version) {
        throw new TypeError(`Manifest ${String(manifest.version)} predecessor is not older`);
      }
    }
    const predecessor = orderedManifests[index - 1]?.version;
    const brokenOldest =
      index === 0 &&
      ((manifest.version === 0 && manifest.previousVersion !== null) ||
        (manifest.previousVersion !== null && manifest.previousVersion >= manifest.version));
    if (brokenOldest || (index > 0 && manifest.previousVersion !== predecessor)) {
      throw new TypeError(`Manifest ${String(manifest.version)} has a non-contiguous predecessor`);
    }
  }
  const oldestReadableIndex = orderedManifests.findIndex(
    (manifest) => manifest.prunedAt === undefined,
  );
  if (oldestReadableIndex >= 0) {
    const oldestVersion = orderedManifests[oldestReadableIndex]?.version ?? 0;
    let liveCount = 0;
    let liveBytes = 0;
    for (const record of state.manifestBlocks) {
      if (manifestBlockVisibleAt(record, oldestVersion)) {
        liveCount += 1;
        liveBytes = safeStorageSum(liveBytes, record.byteLength);
      }
    }
    for (let index = oldestReadableIndex; index < orderedManifests.length; index += 1) {
      const manifest = orderedManifests[index];
      if (manifest === undefined) throw new Error("Readable manifest index is invalid");
      if (index > oldestReadableIndex) {
        liveCount +=
          (addedCounts.get(manifest.version) ?? 0) - (removedCounts.get(manifest.version) ?? 0);
        liveBytes = safeStorageSum(liveBytes, addedBytes.get(manifest.version) ?? 0);
        liveBytes -= removedBytes.get(manifest.version) ?? 0;
      }
      if (
        manifest.prunedAt === undefined &&
        (manifest.liveBlockCount !== liveCount || manifest.liveBlockBytes !== liveBytes)
      ) {
        throw new TypeError(`Manifest ${String(manifest.version)} live summary is inconsistent`);
      }
    }
  } else if (orderedManifests.length === 0 && state.manifestBlocks.length > 0) {
    throw new TypeError("Manifest block provenance exists without a manifest");
  }
  if (
    state.currentVersion !== highestVersion ||
    (state.currentVersion !== null && !manifestVersions.has(state.currentVersion))
  ) {
    throw new TypeError("Current manifest version is not the newest checkpoint manifest");
  }
  if (state.currentVersion !== null) {
    const current = manifestsByVersion.get(state.currentVersion);
    if (current?.prunedAt !== undefined) {
      throw new TypeError("Current manifest is pruned and cannot be read");
    }
  }

  if (state.tables.length > MAX_CATALOG_RECORDS) {
    throw new StorageResourceLimitError("catalog record", state.tables.length, MAX_CATALOG_RECORDS);
  }
  let catalogRetainedBytes = 0;
  for (const table of state.tables) {
    catalogRetainedBytes = safeStorageSum(catalogRetainedBytes, catalogRecordRetainedBytes(table));
  }
  if (catalogRetainedBytes > MAX_CATALOG_RETAINED_BYTES) {
    throw new StorageResourceLimitError(
      "catalog byte",
      catalogRetainedBytes,
      MAX_CATALOG_RETAINED_BYTES,
    );
  }
  const tableIds = unique(state.tables, (record) => record.id, "Checkpoint tables");
  unique(state.tables, (record) => record.name, "Checkpoint table names");
  const indexNames = new Set<string>();
  const triggerNames = new Set<string>();
  const triggerIds = new Set<string>();
  for (const table of state.tables) {
    validateTableRuntimeRecord(table, `Table ${table.id}`);
    for (const index of Object.values(table.secondaryIndexes ?? {})) {
      if (indexNames.has(index.name)) throw new TypeError(`Index already exists: ${index.name}`);
      indexNames.add(index.name);
    }
    for (const trigger of table.triggers ?? []) {
      if (triggerNames.has(trigger.name)) {
        throw new TypeError(`Trigger already exists: ${trigger.name}`);
      }
      if (triggerIds.has(trigger.id)) {
        throw new TypeError(`Trigger ID already exists: ${trigger.id}`);
      }
      triggerNames.add(trigger.name);
      triggerIds.add(trigger.id);
    }
  }
  const tablesById = new Map(state.tables.map((record) => [record.id, record] as const));

  const transactionIds = unique(
    state.transactions,
    (record) => record.id,
    "Checkpoint transactions",
  );
  const transactionsById = new Map(
    state.transactions.map((record) => [record.id, record] as const),
  );
  const activeTransactionSegmentIds = new Map(
    state.transactions
      .filter((record) => record.status === "active")
      .map((record) => [record.id, new Set(record.pendingSegmentIds)] as const),
  );
  for (const transaction of state.transactions) {
    validateTransactionRuntimeRecord(transaction, `Transaction ${transaction.id}`);
  }
  const activePendingTables = state.transactions.flatMap((transaction) =>
    transaction.status === "active" && transaction.pendingTable !== undefined
      ? [transaction.pendingTable]
      : [],
  );
  const combinedCatalog = [...state.tables, ...activePendingTables];
  if (combinedCatalog.length > MAX_CATALOG_RECORDS) {
    throw new StorageResourceLimitError(
      "catalog record",
      combinedCatalog.length,
      MAX_CATALOG_RECORDS,
    );
  }
  let combinedCatalogRetainedBytes = 0;
  for (const table of combinedCatalog) {
    combinedCatalogRetainedBytes = safeStorageSum(
      combinedCatalogRetainedBytes,
      catalogRecordRetainedBytes(table),
    );
  }
  if (combinedCatalogRetainedBytes > MAX_CATALOG_RETAINED_BYTES) {
    throw new StorageResourceLimitError(
      "catalog byte",
      combinedCatalogRetainedBytes,
      MAX_CATALOG_RETAINED_BYTES,
    );
  }
  unique(combinedCatalog, (record) => record.id, "Checkpoint published and pending tables");
  unique(combinedCatalog, (record) => record.name, "Checkpoint published and pending table names");
  for (const table of activePendingTables) {
    validateTableRuntimeRecord(table, `Pending table ${table.id}`);
  }
  const combinedTriggerNames = new Set<string>();
  const combinedTriggerIds = new Set<string>();
  for (const table of combinedCatalog) {
    for (const trigger of table.triggers ?? []) {
      if (combinedTriggerNames.has(trigger.name)) {
        throw new TypeError(`Trigger already exists: ${trigger.name}`);
      }
      if (combinedTriggerIds.has(trigger.id)) {
        throw new TypeError(`Trigger ID already exists: ${trigger.id}`);
      }
      combinedTriggerNames.add(trigger.name);
      combinedTriggerIds.add(trigger.id);
    }
  }
  const publishedByName = new Map(state.tables.map((record) => [record.name, record] as const));
  for (const table of combinedCatalog) {
    for (const key of table.foreignKeys ?? []) {
      const parent = key.parentTable === table.name ? table : publishedByName.get(key.parentTable);
      if (parent === undefined) {
        throw new TypeError(
          `FOREIGN KEY ${key.name} references a missing table: ${key.parentTable}`,
        );
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
  const terminalTransactionCount = state.transactions.filter(
    (transaction) => transaction.status !== "active",
  ).length;
  if (terminalTransactionCount > MAX_TERMINAL_TRANSACTION_RECORDS) {
    throw new StorageResourceLimitError(
      "terminal transaction",
      terminalTransactionCount,
      MAX_TERMINAL_TRANSACTION_RECORDS,
    );
  }

  if (state.segments.length > MAX_SEGMENT_RECORDS) {
    throw new StorageResourceLimitError(
      "segment record",
      state.segments.length,
      MAX_SEGMENT_RECORDS,
    );
  }
  let segmentRetainedBytes = 0;
  for (const segment of state.segments) {
    segmentRetainedBytes = safeStorageSum(
      segmentRetainedBytes,
      segmentRecordRetainedBytes(segment),
    );
  }
  if (segmentRetainedBytes > MAX_SEGMENT_RETAINED_BYTES) {
    throw new StorageResourceLimitError(
      "segment byte",
      segmentRetainedBytes,
      MAX_SEGMENT_RETAINED_BYTES,
    );
  }
  unique(state.segments, (record) => record.id, "Checkpoint segments");
  const segmentsById = new Map(state.segments.map((record) => [record.id, record] as const));
  const segmentBlockOwners = new Map<string, Set<string>>();
  const segmentsByBlock = new Map<string, string[]>();
  const manifestLiveBlocks = new Set(
    state.manifestBlocks
      .filter((record) =>
        sortedVersionInInterval(readableVersions, record.addedVersion, record.removedVersion),
      )
      .map((record) => record.blockId),
  );
  for (const segment of state.segments) {
    const normalized = validateSegmentRuntimeRecord(segment, `Checkpoint segment ${segment.id}`);
    if (!transactionIds.has(normalized.transactionId)) {
      throw new Error(`Segment ${segment.id} has no transaction`);
    }
    const owner = transactionsById.get(normalized.transactionId);
    if (owner === undefined) {
      throw new Error(`Segment ${segment.id} is absent from its transaction journal`);
    }
    const table =
      tablesById.get(normalized.tableId) ??
      (owner.status === "active" && owner.pendingTable?.id === normalized.tableId
        ? owner.pendingTable
        : undefined);
    if (table === undefined) throw new Error(`Segment ${segment.id} has no table`);
    if (!tablesById.has(normalized.tableId) && !owner.pendingSegmentIds.includes(normalized.id)) {
      throw new Error(`Pending-table segment ${segment.id} is absent from its owner journal`);
    }
    if (!tablesById.has(normalized.tableId)) {
      const columns = new Set(table.columns.map((column) => column.id));
      for (const columnId of Object.keys(normalized.columnBlockIds)) {
        if (!columns.has(columnId)) {
          throw new Error(`Pending-table segment ${segment.id} has an unknown column`);
        }
      }
    }
    // Historical segment column identities are intentionally not checked against the current
    // table: metadata-only DROP COLUMN leaves immutable old bytes until compaction.
    for (const id of Object.values(normalized.columnBlockIds).flat()) {
      if (!physical.hasBlock(id))
        throw new Error(`Segment ${segment.id} references missing block: ${id}`);
      const owners = segmentBlockOwners.get(id) ?? new Set<string>();
      owners.add(normalized.transactionId);
      segmentBlockOwners.set(id, owners);
      segmentsByBlock.set(id, [...(segmentsByBlock.get(id) ?? []), normalized.id]);
    }
  }
  const currentBlockIds = new Set(
    state.currentVersion === null
      ? []
      : state.manifestBlocks
          .filter((record) => manifestBlockVisibleAt(record, state.currentVersion ?? -1))
          .map((record) => record.blockId),
  );
  const visibleLevelZeroCounts = new Map<string, number>();
  for (const segment of state.segments) {
    if (segment.level !== 0) continue;
    const owner = transactionsById.get(segment.transactionId);
    if (owner?.status !== "committed") continue;
    const blockIds = segmentBlockIds(segment);
    if (blockIds.length > 0 && blockIds.some((id) => !currentBlockIds.has(id))) continue;
    const count = (visibleLevelZeroCounts.get(segment.tableId) ?? 0) + 1;
    if (count > MAX_LEVEL_ZERO_SEGMENTS) {
      throw new TypeError(`Table ${segment.tableId} exceeds the level-zero segment limit`);
    }
    visibleLevelZeroCounts.set(segment.tableId, count);
  }
  const segmentIds = new Set(state.segments.map((record) => record.id));
  for (const transaction of state.transactions) {
    for (const [commitOrdinal, id] of transaction.pendingSegmentIds.entries()) {
      const segment = segmentsById.get(id);
      if (segment === undefined) {
        if (transaction.status === "active") {
          throw new Error(`Active transaction ${transaction.id} has a missing segment`);
        }
        continue;
      }
      if (segment.transactionId !== transaction.id) {
        throw new Error(`Transaction ${transaction.id} journals another transaction's segment`);
      }
      if (segment.commitOrdinal !== commitOrdinal) {
        throw new Error(
          `Transaction ${transaction.id} segment journal has a noncanonical commit ordinal`,
        );
      }
    }
  }
  for (const segment of state.segments) {
    const owner = transactionsById.get(segment.transactionId);
    if (owner?.status === "active" && !activeTransactionSegmentIds.get(owner.id)?.has(segment.id)) {
      throw new Error(`Active transaction ${owner.id} does not journal segment ${segment.id}`);
    }
  }
  const adoptionOwnerBySegment = new Map<string, string>();
  for (const job of state.compactionJobs) {
    if (!isTerminalCompactionJob(job) && job.transactionId !== null) {
      for (const outputId of compactionOutputSegmentIds(job)) {
        if (job.sourceSegmentIds.includes(outputId)) continue;
        if (adoptionOwnerBySegment.has(outputId)) {
          throw new Error(`Segment ${outputId} is output by multiple compaction jobs`);
        }
        adoptionOwnerBySegment.set(outputId, job.transactionId);
      }
    }
  }
  const pendingBlockOwners = new Map<string, string>();
  let activeTransactionCount = 0;
  let activeStagedBlockCount = 0;
  let activeStagedSegmentCount = 0;
  let activeStagedArtifactBytes = 0;
  for (const transaction of state.transactions) {
    if (transaction.status !== "active") continue;
    activeTransactionCount += 1;
    activeStagedBlockCount += transaction.pendingBlockIds.length;
    activeStagedSegmentCount += transaction.pendingSegmentIds.length;
    for (const id of transaction.pendingBlockIds) {
      activeStagedArtifactBytes += physical.blockByteLength(id) ?? 0;
    }
    if (activeTransactionCount > MAX_ACTIVE_TRANSACTIONS) {
      throw new StorageResourceLimitError(
        "transaction",
        activeTransactionCount,
        MAX_ACTIVE_TRANSACTIONS,
      );
    }
    if (activeStagedBlockCount > MAX_GLOBAL_STAGED_BLOCKS) {
      throw new StorageResourceLimitError(
        "staged block",
        activeStagedBlockCount,
        MAX_GLOBAL_STAGED_BLOCKS,
      );
    }
    if (activeStagedSegmentCount > MAX_GLOBAL_STAGED_SEGMENTS) {
      throw new StorageResourceLimitError(
        "staged segment",
        activeStagedSegmentCount,
        MAX_GLOBAL_STAGED_SEGMENTS,
      );
    }
    if (
      !Number.isSafeInteger(activeStagedArtifactBytes) ||
      activeStagedArtifactBytes > MAX_GLOBAL_STAGED_ARTIFACT_BYTES
    ) {
      throw new StorageResourceLimitError(
        "staged artifact byte",
        activeStagedArtifactBytes,
        MAX_GLOBAL_STAGED_ARTIFACT_BYTES,
      );
    }
    if (transaction.snapshotVersion !== null) {
      const manifest = manifestsByVersion.get(transaction.snapshotVersion);
      if (manifest === undefined || manifest.prunedAt !== undefined) {
        throw new Error(`Active transaction ${transaction.id} has no readable snapshot`);
      }
      const lag =
        (state.currentVersion ?? transaction.snapshotVersion) - transaction.snapshotVersion;
      if (lag > MAX_PINNED_MANIFEST_VERSION_LAG) {
        throw new StorageResourceLimitError(
          "pinned manifest version lag",
          lag,
          MAX_PINNED_MANIFEST_VERSION_LAG,
        );
      }
    }
    for (const id of transaction.pendingBlockIds) {
      if (!physical.hasBlock(id))
        throw new Error(`Active transaction ${transaction.id} has a missing block`);
      if (manifestLiveBlocks.has(id)) {
        throw new Error(`Active transaction ${transaction.id} owns a manifest-live block`);
      }
      const previousOwner = pendingBlockOwners.get(id);
      if (previousOwner !== undefined && previousOwner !== transaction.id) {
        throw new Error(`Pending block ${id} is owned by multiple transactions`);
      }
      pendingBlockOwners.set(id, transaction.id);
      const foreignSegments = (segmentsByBlock.get(id) ?? []).filter(
        (segmentId) => segmentsById.get(segmentId)?.transactionId !== transaction.id,
      );
      const authorizedAdoption = foreignSegments.every((segmentId) => {
        const segmentOwner = transactionsById.get(segmentsById.get(segmentId)?.transactionId ?? "");
        return (
          segmentOwner?.status === "aborted" &&
          adoptionOwnerBySegment.get(segmentId) === transaction.id
        );
      });
      if (!authorizedAdoption) {
        throw new Error(`Active transaction ${transaction.id} owns another transaction's block`);
      }
    }
    let pendingTableNextRowId = 1n;
    for (const [commitOrdinal, id] of transaction.pendingSegmentIds.entries()) {
      if (!segmentIds.has(id))
        throw new Error(`Active transaction ${transaction.id} has a missing segment`);
      const segment = segmentsById.get(id);
      if (segment?.transactionId !== transaction.id) {
        throw new Error(`Active transaction ${transaction.id} owns another transaction's segment`);
      }
      if (segment.commitOrdinal !== commitOrdinal) {
        throw new Error(
          `Active transaction ${transaction.id} segment journal has a noncanonical commit ordinal`,
        );
      }
      if (transaction.pendingTable?.id === segment.tableId) {
        if (
          segment.kind !== "insert" ||
          segment.rowIdStart !== pendingTableNextRowId ||
          segment.rowIdEndExclusive <= segment.rowIdStart
        ) {
          throw new Error(`Active pending table ${transaction.pendingTable.id} has invalid rows`);
        }
        pendingTableNextRowId = segment.rowIdEndExclusive;
      }
    }
    if (
      transaction.pendingTable !== undefined &&
      transaction.pendingTableNextRowId !== pendingTableNextRowId
    ) {
      throw new Error(`Active pending table ${transaction.pendingTable.id} row counter is invalid`);
    }
  }

  unique(state.leases, (record) => record.id, "Checkpoint leases");
  if (state.leases.length > MAX_ACTIVE_LEASES) {
    throw new StorageResourceLimitError("lease", state.leases.length, MAX_ACTIVE_LEASES);
  }
  for (const lease of state.leases) {
    validateLeaseRuntimeRecord(lease, `Lease ${lease.id}`, false);
    if (lease.manifestVersion !== null) {
      const manifest = manifestsByVersion.get(lease.manifestVersion);
      if (manifest === undefined || manifest.prunedAt !== undefined) {
        throw new Error(`Lease ${lease.id} has no readable manifest`);
      }
      const lag = (state.currentVersion ?? lease.manifestVersion) - lease.manifestVersion;
      if (lag > MAX_PINNED_MANIFEST_VERSION_LAG) {
        throw new StorageResourceLimitError(
          "pinned manifest version lag",
          lag,
          MAX_PINNED_MANIFEST_VERSION_LAG,
        );
      }
    }
  }
  unique(state.compactionJobs, (record) => record.id, "Checkpoint compaction jobs");
  const terminalCompactionJobCount = state.compactionJobs.filter(isTerminalCompactionJob).length;
  if (terminalCompactionJobCount > MAX_TERMINAL_COMPACTION_JOB_RECORDS) {
    throw new StorageResourceLimitError(
      "terminal compaction job",
      terminalCompactionJobCount,
      MAX_TERMINAL_COMPACTION_JOB_RECORDS,
    );
  }
  const activeCompactionByTable = new Map<string, string>();
  for (const source of state.compactionJobs) {
    const job = normalizeCompactionJobRecord(source);
    if (job.state === "published") {
      const transaction =
        job.transactionId === null ? undefined : transactionsById.get(job.transactionId);
      const table = tablesById.get(job.tableId);
      const sourceSegments = job.sourceSegmentIds.map((id) => segmentsById.get(id));
      const outputSegments = compactionOutputSegmentIds(job).map((id) => segmentsById.get(id));
      const artifactsRetained =
        transaction !== undefined &&
        table !== undefined &&
        sourceSegments.every((segment) => segment !== undefined) &&
        outputSegments.every((segment) => segment !== undefined) &&
        job.outputBlockIds.every((id) => physical.hasBlock(id));
      if (artifactsRetained) {
        assertCompactionOutputProvenance(job, table, transaction, sourceSegments, outputSegments);
      }
    }
    if (isTerminalCompactionJob(job)) continue;
    const activeId = activeCompactionByTable.get(job.tableId);
    if (activeId !== undefined) {
      throw new Error(
        `Nonterminal compaction job already exists for table ${job.tableId}: ${activeId}`,
      );
    }
    activeCompactionByTable.set(job.tableId, job.id);
    if (activeCompactionByTable.size > MAX_ACTIVE_COMPACTION_JOBS) {
      throw new StorageResourceLimitError(
        "compaction job",
        activeCompactionByTable.size,
        MAX_ACTIVE_COMPACTION_JOBS,
      );
    }
    if (!tableIds.has(job.tableId)) throw new Error(`Compaction job ${job.id} has no table`);
    const manifest = manifestsByVersion.get(job.sourceManifestVersion);
    if (manifest === undefined || manifest.prunedAt !== undefined) {
      throw new Error(`Compaction job ${job.id} has no readable source manifest`);
    }
    const sourceLag =
      (state.currentVersion ?? job.sourceManifestVersion) - job.sourceManifestVersion;
    if (sourceLag > MAX_PINNED_MANIFEST_VERSION_LAG) {
      throw new StorageResourceLimitError(
        "pinned manifest version lag",
        sourceLag,
        MAX_PINNED_MANIFEST_VERSION_LAG,
      );
    }
    for (const id of job.sourceSegmentIds) {
      if (!segmentsById.has(id))
        throw new Error(`Compaction job ${job.id} has no source segment: ${id}`);
    }
    for (const id of [...job.sourceBlockIds, ...job.outputBlockIds]) {
      if (!physical.hasBlock(id)) throw new Error(`Compaction job ${job.id} has no block: ${id}`);
    }
    if (job.transactionId !== null && !transactionIds.has(job.transactionId)) {
      throw new Error(`Compaction job ${job.id} has no transaction`);
    }
    if (job.state === "ready" || job.state === "published") {
      for (const outputId of compactionOutputSegmentIds(job)) {
        if (!segmentsById.has(outputId)) {
          throw new Error(`Compaction job ${job.id} has no output segment: ${outputId}`);
        }
      }
    }
    if (job.transactionId !== null) {
      const transaction = transactionsById.get(job.transactionId);
      if (transaction === undefined) throw new Error(`Compaction job ${job.id} has no transaction`);
      if (job.state === "ready" || transaction.pendingSegmentIds.length > 0) {
        const table = tablesById.get(job.tableId);
        if (table === undefined) throw new Error(`Compaction job ${job.id} has no table`);
        const sourceSegments = job.sourceSegmentIds.map((id) => {
          const segment = segmentsById.get(id);
          if (segment === undefined) {
            throw new Error(`Compaction job ${job.id} has no source segment: ${id}`);
          }
          return segment;
        });
        const outputSegments = transaction.pendingSegmentIds.map((id) => {
          const segment = segmentsById.get(id);
          if (segment === undefined) {
            throw new Error(`Compaction job ${job.id} has no output segment: ${id}`);
          }
          return segment;
        });
        assertCompactionOutputProvenance(job, table, transaction, sourceSegments, outputSegments, {
          allowOutputPrefix: job.state === "running",
        });
      }
    }
  }
  unique(state.garbageCollectionJobs, (record) => record.id, "Checkpoint collection jobs");
  const completedCollectionCount = state.garbageCollectionJobs.filter(
    (record) => record.state === "completed",
  ).length;
  if (completedCollectionCount > MAX_COMPLETED_GARBAGE_COLLECTION_JOB_RECORDS) {
    throw new StorageResourceLimitError(
      "completed garbage collection job",
      completedCollectionCount,
      MAX_COMPLETED_GARBAGE_COLLECTION_JOB_RECORDS,
    );
  }
  let activeCollectionId: string | undefined;
  for (const source of state.garbageCollectionJobs) {
    const job = normalizeGarbageCollectionJobRecord(source);
    if (job.state === "completed") continue;
    if (activeCollectionId !== undefined) {
      throw new Error(`Garbage collection job already active: ${activeCollectionId}`);
    }
    activeCollectionId = job.id;
  }
  unique(state.tempOwners, (record) => record.ownerId, "Checkpoint temp owners");
  if (state.tempOwners.length > MAX_ACTIVE_TEMP_OWNERS) {
    throw new StorageResourceLimitError(
      "temp owner",
      state.tempOwners.length,
      MAX_ACTIVE_TEMP_OWNERS,
    );
  }
  for (const owner of state.tempOwners) validateTempOwnerRecord(owner);

  const validateCounters = (
    entries: ReadonlyArray<readonly [string, bigint]>,
    label: string,
    maximumExclusive: bigint,
  ): void => {
    unique(entries, ([key]) => key, label);
    for (const [, next] of entries) {
      if (typeof next !== "bigint" || next < 1n || next > maximumExclusive) {
        throw new TypeError(`${label} must be in the persisted counter range`);
      }
    }
  };
  validateCounters(state.nextRowIds, "Checkpoint row counters", MAX_ROW_ID_EXCLUSIVE_END);
  for (const [tableId] of state.nextRowIds) {
    if (!tableIds.has(tableId)) throw new Error(`Row counter has no table: ${tableId}`);
  }
  validateCounters(
    state.nextAutoIncrement,
    "Checkpoint auto-increment counters",
    MAX_AUTO_INCREMENT_EXCLUSIVE_END,
  );
  for (const [key] of state.nextAutoIncrement) {
    const slash = key.indexOf("/");
    const table = slash < 1 ? undefined : tablesById.get(key.slice(0, slash));
    const column = table?.columns.find((entry) => entry.id === key.slice(slash + 1));
    if (column?.defaultValue?.kind !== "autoincrement") {
      throw new Error(`Auto-increment counter has no declared column: ${key}`);
    }
  }
  const declaredPostingKeys = new Set<string>();
  for (const table of state.tables) {
    for (const columnId of Object.keys(table.ftsColumns ?? {})) {
      declaredPostingKeys.add(`${table.id}/${columnId}`);
    }
    for (const index of Object.values(table.secondaryIndexes ?? {})) {
      declaredPostingKeys.add(`${table.id}/${index.storageColumnId}`);
    }
  }
  unique(state.ftsBases, ([key]) => key, "Checkpoint full-text bases");
  const ftsBaseVersions = new Map<string, number>();
  for (const [key, base] of state.ftsBases) {
    if (!declaredPostingKeys.has(key))
      throw new Error(`Full-text base has no catalog owner: ${key}`);
    if (
      !Number.isSafeInteger(base.coversVersion) ||
      base.coversVersion < -1 ||
      (state.currentVersion !== null && base.coversVersion > state.currentVersion)
    ) {
      throw new TypeError(`Full-text base ${key} has an invalid version`);
    }
    whole(base.totalTokens, `Full-text base ${key} token count`);
    if (base.chunks.length > MAX_FTS_BASE_CHUNKS) {
      throw new RangeError(`Full-text base ${key} exceeds the chunk-count limit`);
    }
    if (validateFtsPostingChunks(base.chunks, `Full-text base ${key}`) !== base.totalTokens) {
      throw new TypeError(`Full-text base ${key} token total does not match its postings`);
    }
    ftsBaseVersions.set(key, base.coversVersion);
  }
  unique(state.ftsDeltas, ([key]) => key, "Checkpoint full-text deltas");
  for (const [key, deltas] of state.ftsDeltas) {
    if (!declaredPostingKeys.has(key))
      throw new Error(`Full-text deltas have no catalog owner: ${key}`);
    const baseVersion = ftsBaseVersions.get(key) ?? -1;
    if (deltas.length > MAX_FTS_DELTA_CHUNKS) {
      throw new RangeError(`Full-text delta ${key} exceeds the version-count limit`);
    }
    const versions = new Set<number>();
    for (const [version, delta] of deltas) {
      whole(version, `Full-text delta ${key} version`);
      if (
        version <= baseVersion ||
        state.currentVersion === null ||
        version > state.currentVersion
      ) {
        throw new TypeError(`Full-text delta ${key} is outside the readable version range`);
      }
      if (versions.has(version)) throw new TypeError(`Full-text delta ${key} repeats a version`);
      versions.add(version);
      whole(delta.totalTokens, `Full-text delta ${key} token count`);
      if (
        validateFtsPostingChunks([delta.postings], `Full-text delta ${key}`) !== delta.totalTokens
      ) {
        throw new TypeError(`Full-text delta ${key} token total does not match its postings`);
      }
    }
  }
  const membershipNamespaces = unique(
    state.uniqueKeys,
    ([key]) => key,
    "Checkpoint unique memberships",
  );
  for (const [key, tokens] of state.uniqueKeys) uniqueStrings(tokens, `Unique membership ${key}`);
  const expectedMembershipNamespaces = new Set<string>();
  for (const table of state.tables) {
    if (table.uniqueKeyColumnId !== undefined) expectedMembershipNamespaces.add(table.id);
    for (const [indexId, index] of Object.entries(table.secondaryIndexes ?? {})) {
      if (index.unique === true && index.uniqueEnforced === true) {
        expectedMembershipNamespaces.add(secondaryUniqueKeyNamespace(table.id, indexId));
      }
    }
  }
  for (const namespace of expectedMembershipNamespaces) {
    if (!membershipNamespaces.has(namespace)) {
      throw new Error(`Checkpoint is missing unique membership: ${namespace}`);
    }
  }
  for (const namespace of membershipNamespaces) {
    if (!expectedMembershipNamespaces.has(namespace)) {
      throw new Error(`Checkpoint has orphan unique membership: ${namespace}`);
    }
  }
  const buildIds = new Set<string>();
  const activeBuildNamespaces = new Set<string>();
  let activeBuildCount = 0;
  let activeBuildBytes = 0;
  for (const entry of state.uniqueKeyBuilds) {
    const unknownEntry: unknown = entry;
    if (!Array.isArray(unknownEntry) || unknownEntry.length !== 3) {
      throw new TypeError("Checkpoint UNIQUE build entry is invalid");
    }
    const [record, chunks, completedInput] =
      unknownEntry as unknown as RecordCoreState["uniqueKeyBuilds"][number];
    if (buildIds.has(record.buildId)) {
      throw new TypeError(`Checkpoint repeats UNIQUE build: ${record.buildId}`);
    }
    buildIds.add(record.buildId);
    for (const id of [
      record.buildId,
      record.tableId,
      record.indexId,
      record.namespaceId,
      record.ownerId,
    ]) {
      validateId(id);
    }
    for (const [value, label] of [
      [record.nextOrdinal, "ordinal"],
      [record.tokenCount, "token count"],
      [record.retainedBytes, "retained bytes"],
    ] as const) {
      whole(value, `UNIQUE build ${record.buildId} ${label}`);
    }
    validateBoundedExpiration(
      record.createdAt,
      record.expiresAt,
      `UNIQUE build ${record.buildId}`,
      MAX_UNIQUE_KEY_BUILD_TTL_MS,
    );
    validateTimestampRuntime(record.updatedAt, `UNIQUE build ${record.buildId} updatedAt`);
    if (!Array.isArray(chunks))
      throw new TypeError(`UNIQUE build ${record.buildId} chunks are invalid`);
    const tokens = new Set<string>();
    let retainedBytes = 0;
    for (const chunk of chunks) {
      retainedBytes = safeStorageSum(retainedBytes, uniqueKeyBuildChunkRetainedBytes(chunk));
      for (const token of chunk) {
        if (tokens.has(token)) throw new UniqueKeyConflictError(record.namespaceId, token);
        tokens.add(token);
      }
    }
    const table = tablesById.get(record.tableId);
    const index = table?.secondaryIndexes?.[record.indexId];
    const buildState: unknown = record.state;
    if (buildState === "active") {
      if (
        record.completedAt !== undefined ||
        completedInput !== null ||
        chunks.length !== record.nextOrdinal ||
        tokens.size !== record.tokenCount ||
        retainedBytes !== record.retainedBytes ||
        retainedBytes > MAX_UNIQUE_KEY_BUILD_STAGED_BYTES ||
        index?.unique !== true ||
        index.state !== "building" ||
        index.buildId !== record.buildId ||
        secondaryUniqueKeyNamespace(record.tableId, record.indexId) !== record.namespaceId
      ) {
        throw new TypeError(`Active UNIQUE build ${record.buildId} is inconsistent`);
      }
      if (activeBuildNamespaces.has(record.namespaceId)) {
        throw new TypeError(`UNIQUE namespace has multiple active builds: ${record.namespaceId}`);
      }
      activeBuildNamespaces.add(record.namespaceId);
      activeBuildCount += 1;
      activeBuildBytes = safeStorageSum(activeBuildBytes, retainedBytes);
    } else if (buildState === "completed") {
      if (
        completedInput === null ||
        record.completedAt === undefined ||
        record.retainedBytes !== 0 ||
        chunks.length !== 0 ||
        index?.unique !== true ||
        index.state !== "ready" ||
        index.uniqueEnforced !== true ||
        !membershipNamespaces.has(record.namespaceId)
      ) {
        throw new TypeError(`Completed UNIQUE build ${record.buildId} is inconsistent`);
      }
      validateTimestampRuntime(record.completedAt, `UNIQUE build ${record.buildId} completedAt`);
    } else {
      throw new TypeError(`UNIQUE build ${record.buildId} has an invalid state`);
    }
  }
  if (activeBuildCount > MAX_ACTIVE_UNIQUE_KEY_BUILDS) {
    throw new StorageResourceLimitError(
      "unique-key build",
      activeBuildCount,
      MAX_ACTIVE_UNIQUE_KEY_BUILDS,
    );
  }
  if (activeBuildBytes > MAX_UNIQUE_KEY_BUILD_STAGED_BYTES_TOTAL) {
    throw new StorageResourceLimitError(
      "staged artifact byte",
      activeBuildBytes,
      MAX_UNIQUE_KEY_BUILD_STAGED_BYTES_TOTAL,
    );
  }
}

function validateCount(count: number): void {
  if (!Number.isSafeInteger(count) || count <= 0) {
    throw new RangeError("Row ID reservation count must be a positive whole number");
  }
}

function validateTableView(view: TableRecord["view"]): void {
  const candidate: unknown = view;
  if (candidate === undefined) return;
  if (typeof candidate !== "object" || candidate === null || Array.isArray(candidate)) {
    throw new TypeError("Table view metadata is invalid");
  }
  const record = candidate as Record<string, unknown>;
  if (
    typeof record.sql !== "string" ||
    record.sql.length === 0 ||
    (record.managed !== undefined && typeof record.managed !== "boolean") ||
    Object.keys(record).some((key) => key !== "sql" && key !== "managed")
  ) {
    throw new TypeError("Table view metadata is invalid");
  }
}

function validateTableRuntimeRecord(record: TableRecord, label: string): void {
  if (typeof record.id !== "string" || record.id.length === 0) {
    throw new TypeError(`${label} ID is invalid`);
  }
  if (typeof record.name !== "string" || record.name.length === 0) {
    throw new TypeError(`${label} name is invalid`);
  }
  validateCanonicalTimestamp(record.createdAt, `${label} creation timestamp`);
  if (!Number.isSafeInteger(record.revision) || record.revision < 0) {
    throw new TypeError(`${label} revision is invalid`);
  }
  validateTableColumns(record.columns);
  validateSecondaryIndexes(record);
  validateTableView(record.view);
}

export function validateAutoIncrementReservation(count: number, atLeast: bigint | undefined): void {
  if (!Number.isSafeInteger(count) || count < 0) {
    throw new RangeError("Auto-increment reservation count must be a non-negative whole number");
  }
  if (
    atLeast !== undefined &&
    (typeof atLeast !== "bigint" || atLeast < 1n || atLeast > MAX_AUTO_INCREMENT_EXCLUSIVE_END)
  ) {
    throw new RangeError(
      `Auto-increment bump target must be between 1 and ${String(MAX_AUTO_INCREMENT_EXCLUSIVE_END)}`,
    );
  }
}

function assertCounterEndInRange(endExclusive: bigint, limit: bigint, label: string): void {
  if (endExclusive < 1n || endExclusive > limit) {
    throw new RangeError(`${label} exceeds its persisted numeric range`);
  }
}

function snapshotMetadataItemPhase(item: SnapshotMetadataItem): number {
  switch (item.kind) {
    case "table":
      return 0;
    case "segment":
      return 1;
    case "transaction":
      return 2;
    case "unique-generation":
    case "unique-chunk":
      return 3;
    case "posting-generation":
    case "posting-chunk":
      return 4;
  }
}

function safeWholeIncrement(value: number, label: string): number {
  if (!Number.isSafeInteger(value) || value < 0 || value >= Number.MAX_SAFE_INTEGER) {
    throw new RangeError(`${label} cannot be incremented beyond the safe integer range`);
  }
  return value + 1;
}

function nextManifestVersion(expectedVersion: number | null): number {
  return expectedVersion === null ? 0 : safeWholeIncrement(expectedVersion, "Manifest version");
}

export function validateBeginTransactionInput(input: BeginTransactionInput): void {
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

function assertSnapshotAvailable(
  version: number | null,
  manifests: ReadonlyMap<number, Manifest>,
): void {
  if (version === null) return;
  const manifest = manifests.get(version);
  if (manifest === undefined || manifest.prunedAt !== undefined) {
    throw new SnapshotManifestMissingError(version);
  }
}

function assertPendingArtifactsAvailable(
  transaction: TransactionRecord,
  physical: PhysicalBlocks,
  segments: ReadonlyMap<string, SegmentRecord>,
  validateBlocks = true,
  validateSegments = true,
): void {
  const missingBlockId = validateBlocks
    ? transaction.pendingBlockIds.find((id) => !physical.hasBlock(id))
    : undefined;
  if (missingBlockId !== undefined) {
    throw new Error(`Transaction references missing pending block: ${missingBlockId}`);
  }
  const missingSegmentId = validateSegments
    ? transaction.pendingSegmentIds.find((id) => !segments.has(id))
    : undefined;
  if (missingSegmentId !== undefined) {
    throw new Error(`Transaction references missing pending segment: ${missingSegmentId}`);
  }
  if (validateSegments) {
    for (const [commitOrdinal, id] of transaction.pendingSegmentIds.entries()) {
      const segment = segments.get(id);
      if (segment === undefined) continue;
      if (segment.transactionId !== transaction.id) {
        throw new Error(`Transaction pending segment belongs to another transaction: ${id}`);
      }
      if (segment.commitOrdinal !== commitOrdinal) {
        throw new Error(
          `Transaction pending segment ${id} has commit ordinal ${String(segment.commitOrdinal)}; expected ${String(commitOrdinal)}`,
        );
      }
    }
  }
}

function assertGarbageCollectionCandidateProvenance(
  job: GarbageCollectionJobRecord,
  manifests: ReadonlyMap<number, Manifest>,
  manifestBlocks: ReadonlyMap<string, ManifestBlockRecord>,
  segments: ReadonlyMap<string, SegmentRecord>,
  transactions: ReadonlyMap<string, TransactionRecord>,
  roots: RecordRootIndex,
): void {
  for (const version of job.candidateManifestVersions) {
    if (!manifests.has(version)) {
      throw new Error(`Garbage collection candidate manifest is missing: ${String(version)}`);
    }
  }
  for (const id of job.candidateTransactionIds) {
    const transaction = transactions.get(id);
    if (
      transaction === undefined ||
      (transaction.status !== "aborted" &&
        (transaction.status !== "committed" || transaction.committedVersion === null))
    ) {
      throw new Error(`Garbage collection transaction candidate is not terminal: ${id}`);
    }
  }
  const unprovenBlockId = job.candidateBlockIds.find(
    (id) =>
      !manifestBlocks.has(id) &&
      roots.abortedTransactionBlockCount(id) === 0 &&
      roots.terminalJobBlockCount(id) === 0,
  );
  if (unprovenBlockId !== undefined) {
    throw new Error(
      `Garbage collection block candidate has no persisted provenance: ${unprovenBlockId}`,
    );
  }
  const unprovenSegmentId = job.candidateSegmentIds.find((id) => {
    // The persisted segment record is sufficient provenance for nominating that exact id.
    // Its blocks may already have been reclaimed by an earlier bounded pass, and terminal
    // compaction records are intentionally aged out, so neither is a durable discovery source.
    return !segments.has(id);
  });
  if (unprovenSegmentId !== undefined) {
    throw new Error(
      `Garbage collection segment candidate has no persisted provenance: ${unprovenSegmentId}`,
    );
  }
}

function collectIndexedPhysicalRoots(
  candidateSegmentIds: readonly string[],
  candidateBlockIds: readonly string[],
  manifestBlocks: ReadonlyMap<string, ManifestBlockRecord>,
  newlyPrunedVersions: ReadonlySet<number>,
  segments: SegmentRecordMap,
  roots: RecordRootIndex,
): { blockIds: Set<string>; segmentIds: Set<string> } {
  const hasReadableManifestRoot = (id: string): boolean => {
    const record = manifestBlocks.get(id);
    if (record === undefined) return false;
    for (const version of roots.readableManifestVersions.after(record.addedVersion - 1)) {
      if (record.removedVersion !== null && version >= record.removedVersion) return false;
      if (!newlyPrunedVersions.has(version)) return true;
    }
    return false;
  };
  const isDirectBlockRoot = (id: string): boolean => {
    return (
      hasReadableManifestRoot(id) ||
      roots.activeTransactionBlockCount(id) > 0 ||
      roots.liveJobBlockCount(id) > 0
    );
  };
  const isSegmentRoot = (segment: SegmentRecord): boolean => {
    const ids = segmentBlockIds(segment);
    return (
      roots.activeTransactionSegmentCount(segment.id) > 0 ||
      roots.liveJobSegmentCount(segment.id) > 0 ||
      roots.activeOwner(segment.transactionId) ||
      (ids.length > 0 && ids.every(isDirectBlockRoot))
    );
  };
  const rootedBlockIds = new Set<string>();
  const rootedSegmentIds = new Set<string>();
  for (const id of candidateSegmentIds) {
    const segment = segments.get(id);
    if (segment !== undefined && isSegmentRoot(segment)) rootedSegmentIds.add(id);
  }
  for (const id of candidateBlockIds) {
    if (isDirectBlockRoot(id)) {
      rootedBlockIds.add(id);
      continue;
    }
    for (const segmentId of segments.segmentIdsForBlock(id)) {
      const segment = segments.get(segmentId);
      if (segment !== undefined && isSegmentRoot(segment)) {
        rootedBlockIds.add(id);
        break;
      }
    }
  }
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

function validatePageLimit(limit: number): void {
  boundedMaintenanceBatchItems(limit, "Storage page limit");
}

/** Selects the next sorted page without copying or sorting the complete backing map. */
function boundedRecordPage<T, Key extends OrderedKey>(
  records: OrderedRecordMap<Key, T>,
  after: Key | null,
  limit: number,
  accept: (value: T) => boolean = () => true,
): T[] {
  const selected: T[] = [];
  for (const value of records.orderedValues(after)) {
    if (!accept(value)) continue;
    selected.push(value);
    if (selected.length === limit) break;
  }
  return selected;
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
    !isCanonicalTimestamp(value[0])
  ) {
    throw new TypeError(`${label} cursor is invalid`);
  }
  return [value[0], value[1]];
}

function boundedExpiryPage<T extends { expiresAt: string }>(
  records: ExpiringRecordMap<T>,
  expiresAtCutoff: string,
  afterCursor: string | null,
  limit: number,
  idOf: (value: T) => string,
  label: string,
): StoragePage<T, string> {
  validatePageLimit(limit);
  validateCanonicalTimestamp(expiresAtCutoff, `${label} expiry cutoff`);
  const after = decodeExpiryPageCursor(afterCursor, label);
  const selected: Array<{ expiresAt: string; id: string; value: T }> = [];
  for (const value of records.expiringValues(after, expiresAtCutoff)) {
    const expiresAt = value.expiresAt;
    const id = idOf(value);
    selected.push({ expiresAt, id, value });
    if (selected.length === limit) break;
  }
  const last = selected.at(-1);
  return {
    records: selected.map(({ value }) => value),
    nextCursor:
      selected.length === limit && last !== undefined
        ? encodeExpiryPageCursor(last.expiresAt, last.id)
        : null,
  };
}

function validateTempOwnerRecord(record: TempOwnerRecord): void {
  validateId(record.ownerId);
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

export function validateLeaseExpiration(expiresAt: string): void {
  if (
    typeof expiresAt !== "string" ||
    expiresAt.length === 0 ||
    !Number.isFinite(Date.parse(expiresAt))
  ) {
    throw new TypeError("Lease expiration must be valid");
  }
}

function isCanonicalTimestamp(value: string): boolean {
  const timestamp = Date.parse(value);
  return Number.isFinite(timestamp) && new Date(timestamp).toISOString() === value;
}

function validateCanonicalTimestamp(value: unknown, label: string): number {
  if (typeof value !== "string" || !isCanonicalTimestamp(value)) {
    throw new TypeError(`${label} must be canonical UTC ISO-8601`);
  }
  return Date.parse(value);
}

function validateBoundedExpiration(
  cutoffAt: string,
  expiresAt: string,
  label: string,
  maximumTtlMs: number,
): number {
  const cutoff = validateCanonicalTimestamp(cutoffAt, `${label} cutoff`);
  const expiry = validateCanonicalTimestamp(expiresAt, `${label} expiration`);
  if (expiry <= cutoff) throw new RangeError(`${label} expiration must be after its cutoff`);
  if (expiry - cutoff > maximumTtlMs) {
    throw new RangeError(`${label} expiration exceeds the maximum TTL`);
  }
  return cutoff;
}

function validateLeaseRuntimeRecord(
  record: LeaseRecord,
  label: string,
  requireInitialRevision: boolean,
): void {
  validateId(record.id);
  validateId(record.ownerId);
  const kind: unknown = record.kind;
  if (kind !== "reader" && kind !== "backup") {
    throw new TypeError(`${label} kind is invalid`);
  }
  if (
    record.manifestVersion !== null &&
    (!Number.isSafeInteger(record.manifestVersion) || record.manifestVersion < 0)
  ) {
    throw new TypeError(`${label} manifest version is invalid`);
  }
  validateBoundedExpiration(record.createdAt, record.expiresAt, label, MAX_LEASE_TTL_MS);
  if (
    !Number.isSafeInteger(record.revision) ||
    record.revision < 0 ||
    (requireInitialRevision && record.revision !== 0)
  ) {
    throw new RangeError(
      requireInitialRevision
        ? `${label} must be created at revision zero`
        : `${label} revision is invalid`,
    );
  }
}

export function validateTempRunPage(page: TempRunPage): void {
  assertTempRunPageBatchLimits([page]);
  validateTempRunPageIdentity(page.ownerId, page.runId, page.pageIndex);
  validateBlockWriteBytes(page.bytes);
}

export function validateTempRunPageIdentity(
  ownerId: string,
  runId: string,
  pageIndex: number,
): void {
  validateId(ownerId);
  validateId(runId);
  if (!Number.isSafeInteger(pageIndex) || pageIndex < 0) {
    throw new RangeError("Temp run page index must be a non-negative whole number");
  }
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

function lowerBoundNumbers(values: readonly number[], target: number): number {
  let low = 0;
  let high = values.length;
  while (low < high) {
    const middle = low + ((high - low) >> 1);
    if ((values[middle] ?? Number.POSITIVE_INFINITY) < target) low = middle + 1;
    else high = middle;
  }
  return low;
}

function safeStorageSum(left: number, right: number): number {
  const total = left + right;
  if (!Number.isSafeInteger(total)) {
    throw new RangeError("Garbage collection reclaimed block bytes exceed the safe range");
  }
  return total;
}

function manifestBlockVisibleAt(record: ManifestBlockRecord, version: number): boolean {
  return (
    record.addedVersion <= version &&
    (record.removedVersion === null || version < record.removedVersion)
  );
}

function sortedVersionInInterval(
  versions: readonly number[],
  addedVersion: number,
  removedVersion: number | null,
): boolean {
  let low = 0;
  let high = versions.length;
  while (low < high) {
    const middle = (low + high) >>> 1;
    if ((versions[middle] ?? -1) < addedVersion) low = middle + 1;
    else high = middle;
  }
  const version = versions[low];
  return version !== undefined && (removedVersion === null || version < removedVersion);
}

function validateFtsCandidateLimit(value: number): void {
  if (!Number.isSafeInteger(value) || value < 1 || value > MAX_FTS_CANDIDATE_ROW_IDS) {
    throw new RangeError(
      `Full-text candidate limit must be between 1 and ${String(MAX_FTS_CANDIDATE_ROW_IDS)}`,
    );
  }
}

function varuintByteLength(value: bigint): number {
  let length = 1;
  while (value >= 0x80n) {
    value >>= 7n;
    length += 1;
  }
  return length;
}
