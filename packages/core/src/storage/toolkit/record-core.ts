import {
  type BeginTransactionInput,
  type BeginTransactionResult,
  type CommitTransactionInput,
  type CompactionJobRecord,
  CompactionJobConflictError,
  type CompactionJobRecordUpdate,
  createManifest,
  type CreateGarbageCollectionJobInput,
  createGarbageCollectionJobRecord,
  type CatalogProbe,
  type TriggerRecord,
  advanceGarbageCollectionJobRecord,
  collectFtsCandidates,
  invalidateUncoveredFtsColumns,
  type FtsCandidates,
  type FtsChanges,
  type FtsColumnIndexRecord,
  type FtsPosting,
  type GarbageCollectionJobRecord,
  GarbageCollectionJobConflictError,
  type GarbageCollectionStepResult,
  type LeaseRecord,
  LeaseConflictError,
  type Manifest,
  type ManifestSummary,
  type PublishManifestInput,
  type QueryCatalogState,
  type RowIdRange,
  type RunGarbageCollectionStepInput,
  type SegmentRecord,
  SnapshotManifestMissingError,
  type StageTransactionArtifactsInput,
  type StoragePage,
  type TableColumnRecord,
  type TableRecord,
  TableRecordConflictError,
  type TempOwnerRecord,
  TempOwnerConflictError,
  type TempRunPage,
  type TransactionRecord,
  TransactionRecordConflictError,
  type TransactionRecordUpdate,
  UniqueKeyConflictError,
  type WriteTransactionInput,
  normalizeCompactionJobRecord,
  normalizeSegmentRecord,
  updateCompactionJobRecord,
  updateTransactionRecord,
  WriteConflictError,
} from "../types.js";
import { selectLiveRecords, type DatabaseSnapshot, type SnapshotFtsIndex } from "../snapshot.js";

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
}

/** `RecordCore.dump()`'s shape; see there. Arrays rather than Maps so the value serializes. */
export interface RecordCoreState {
  currentVersion: number | null;
  catalogEpoch: number;
  manifests: Manifest[];
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
  tempOwners: TempOwnerRecord[];
}

/** A commit validated and resolved but not yet applied: everything `#applyCommit` writes. */
interface CommitPlan {
  manifest: Manifest;
  committed: TransactionRecord;
  pendingSegments: SegmentRecord[];
  uniqueKeyDeltas: Map<string, { added: Set<string>; removed: Set<string> }>;
  ftsChanges: readonly FtsChanges[] | undefined;
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
  readonly #manifests = new Map<number, Manifest>();
  readonly #transactions = new Map<string, TransactionRecord>();
  readonly #tables = new Map<string, TableRecord>();
  readonly #tableIdsByName = new Map<string, string>();
  readonly #segments = new Map<string, SegmentRecord>();
  readonly #leases = new Map<string, LeaseRecord>();
  readonly #compactionJobs = new Map<string, CompactionJobRecord>();
  readonly #garbageCollectionJobs = new Map<string, GarbageCollectionJobRecord>();
  readonly #nextRowIds = new Map<string, bigint>();
  readonly #nextAutoIncrement = new Map<string, bigint>();
  readonly #ftsBases = new Map<
    string,
    { coversVersion: number; chunks: FtsPosting[][]; totalTokens: number }
  >();
  readonly #ftsDeltas = new Map<
    string,
    Map<number, { postings: FtsPosting[]; totalTokens: number }>
  >();
  readonly #uniqueKeys = new Map<string, Set<string>>();
  readonly #tempOwners = new Map<string, TempOwnerRecord>();
  #currentVersion: number | null = null;
  /** Advances on every catalog mutation; see `CatalogProbe` for the freshness contract. */
  #catalogEpoch = 0;

  constructor(physical: PhysicalBlocks) {
    this.#physical = physical;
  }

  createTempOwner(record: TempOwnerRecord): void {
    validateTempOwnerRecord(record);
    if (this.#tempOwners.has(record.ownerId)) {
      throw new Error(`Temp owner already exists: ${record.ownerId}`);
    }
    this.#tempOwners.set(record.ownerId, structuredClone(record));
  }

  getTempOwner(ownerId: string): TempOwnerRecord | undefined {
    validateId(ownerId);
    const record = this.#tempOwners.get(ownerId);
    return record === undefined ? undefined : structuredClone(record);
  }

  renewTempOwner(ownerId: string, expectedRevision: number, expiresAt: string): TempOwnerRecord {
    validateLeaseExpiration(expiresAt);
    const record = this.#tempOwners.get(ownerId);
    if (record?.revision !== expectedRevision) {
      throw new TempOwnerConflictError(ownerId, expectedRevision, record?.revision ?? null);
    }
    const renewed = { ...record, expiresAt, revision: record.revision + 1 };
    this.#tempOwners.set(ownerId, renewed);
    return structuredClone(renewed);
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
    const ownerIds = new Set<string>(this.#tempOwners.keys());
    for (const ownerId of extraOwnerIds) ownerIds.add(ownerId);
    const sorted = [...ownerIds]
      .filter((ownerId) => afterOwnerId === null || ownerId > afterOwnerId)
      .sort();
    const records = sorted.slice(0, limit);
    return {
      records,
      nextCursor: sorted.length > limit ? (records[records.length - 1] ?? null) : null,
    };
  }

  addTable(record: TableRecord): void {
    if (this.#tables.has(record.id)) throw new Error(`Table already exists: ${record.id}`);
    if (this.#tableIdsByName.has(record.name))
      throw new Error(`Table name already exists: ${record.name}`);
    this.#tables.set(record.id, structuredClone(record));
    this.#tableIdsByName.set(record.name, record.id);
    this.#catalogEpoch += 1;
  }

  getTable(id: string): TableRecord | undefined {
    const record = this.#tables.get(id);
    return record === undefined ? undefined : structuredClone(record);
  }

  updateTable(
    id: string,
    expectedRevision: number,
    update: {
      columns?: TableColumnRecord[];
      ftsColumns?: Record<string, FtsColumnIndexRecord> | null;
      triggers?: TriggerRecord[] | null;
    },
  ): TableRecord {
    const record = this.#tables.get(id);
    const actualRevision = record === undefined ? null : (record.revision ?? 0);
    if (record === undefined || actualRevision !== expectedRevision) {
      throw new TableRecordConflictError(id, expectedRevision, actualRevision);
    }
    if (update.columns !== undefined) validateTableColumns(update.columns);
    const { ftsColumns: previousFts, triggers: previousTriggers, ...base } = record;
    const nextFts = update.ftsColumns === undefined ? previousFts : update.ftsColumns;
    const nextTriggers = update.triggers === undefined ? previousTriggers : update.triggers;
    const updated: TableRecord = {
      ...base,
      columns: update.columns === undefined ? record.columns : structuredClone(update.columns),
      ...(nextFts === null || nextFts === undefined
        ? {}
        : { ftsColumns: structuredClone(nextFts) }),
      ...(nextTriggers === null || nextTriggers === undefined
        ? {}
        : { triggers: structuredClone(nextTriggers) }),
      revision: expectedRevision + 1,
    };
    this.#tables.set(id, updated);
    this.#catalogEpoch += 1;
    return structuredClone(updated);
  }

  removeTable(id: string, expectedRevision: number): void {
    const record = this.#tables.get(id);
    const actualRevision = record === undefined ? null : (record.revision ?? 0);
    if (record === undefined || actualRevision !== expectedRevision) {
      throw new TableRecordConflictError(id, expectedRevision, actualRevision);
    }
    for (const [segmentId, segment] of this.#segments) {
      if (segment.tableId === id) this.#segments.delete(segmentId);
    }
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
    this.#nextRowIds.delete(id);
    this.#tableIdsByName.delete(record.name);
    this.#tables.delete(id);
    this.#catalogEpoch += 1;
  }

  writeFtsBase(
    tableId: string,
    columnId: string,
    input: { coversVersion: number; chunks: FtsPosting[][]; totalTokens: number },
  ): void {
    const key = `${tableId}/${columnId}`;
    this.#ftsBases.set(key, structuredClone(input));
    const deltas = this.#ftsDeltas.get(key);
    if (deltas !== undefined) {
      for (const version of [...deltas.keys()]) {
        if (version <= input.coversVersion) deltas.delete(version);
      }
    }
  }

  readFtsCandidates(
    tableId: string,
    columnId: string,
    terms: ReadonlyArray<{ term: string; prefix: boolean }>,
    upToVersion: number,
  ): FtsCandidates & { deltaChunkCount: number; totalTokens: number; coversVersion: number } {
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
      ...collectFtsCandidates(chunkLists, terms),
      deltaChunkCount,
      totalTokens,
      coversVersion: base?.coversVersion ?? -1,
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
  ): void {
    const changedTableIds = new Set(pendingSegments.map((segment) => segment.tableId));
    for (const tableId of changedTableIds) {
      const record = this.#tables.get(tableId);
      if (record === undefined) continue;
      const forTable = (changeList ?? []).find((entry) => entry.tableId === tableId);
      const covered = new Set(forTable?.columns.map((column) => column.columnId) ?? []);
      const invalidated = invalidateUncoveredFtsColumns(record, covered);
      if (invalidated !== undefined) this.#tables.set(record.id, invalidated);
    }
    for (const changes of changeList ?? []) this.#applyFtsEntry(changes, version);
  }

  #applyFtsEntry(changes: FtsChanges, version: number): void {
    for (const column of changes.columns) {
      const key = `${changes.tableId}/${column.columnId}`;
      const deltas =
        this.#ftsDeltas.get(key) ??
        new Map<number, { postings: FtsPosting[]; totalTokens: number }>();
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
    updatedAt: string;
  }): void {
    for (const version of effect.prunedManifestVersions) {
      const manifest = this.#manifests.get(version);
      if (manifest !== undefined) {
        this.#manifests.set(version, { ...manifest, prunedAt: effect.updatedAt });
      }
    }
    for (const id of effect.reclaimedSegmentIds) this.#segments.delete(id);
    this.#garbageCollectionJobs.set(effect.job.id, structuredClone(effect.job));
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

  addSegment(record: SegmentRecord): void {
    const normalized = normalizeSegmentRecord(record);
    if (this.#segments.has(normalized.id)) {
      throw new Error(`Segment already exists: ${normalized.id}`);
    }
    this.#segments.set(normalized.id, normalized);
  }

  getSegment(id: string): SegmentRecord | undefined {
    const record = this.#segments.get(id);
    return record === undefined ? undefined : normalizeSegmentRecord(record);
  }

  listSegments(tableId?: string): SegmentRecord[] {
    return [...this.#segments.values()]
      .filter((record) => tableId === undefined || record.tableId === tableId)
      .sort((left, right) => left.id.localeCompare(right.id))
      .map((record) => normalizeSegmentRecord(record));
  }

  removeSegment(id: string): void {
    this.#segments.delete(id);
  }

  reserveRowIds(tableId: string, count: number): RowIdRange {
    validateCount(count);
    const start = this.#nextRowIds.get(tableId) ?? 1n;
    const endExclusive = start + BigInt(count);
    this.#nextRowIds.set(tableId, endExclusive);
    return { start, endExclusive };
  }

  reserveAutoIncrement(
    tableId: string,
    columnId: string,
    count: number,
    atLeast?: bigint,
  ): RowIdRange {
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
    this.#nextAutoIncrement.set(key, endExclusive);
    return { start, endExclusive };
  }

  getExistingUniqueKeys(tableId: string, keyTokens: readonly string[]): string[] {
    const existing = this.#uniqueKeys.get(tableId);
    if (existing === undefined) return [];
    return [...new Set(keyTokens)].filter((token) => existing.has(token)).sort();
  }

  getCurrentManifestVersion(): number | null {
    return this.#currentVersion;
  }

  getCatalogProbe(): CatalogProbe {
    return { manifestVersion: this.#currentVersion, catalogEpoch: this.#catalogEpoch };
  }

  getQueryCatalogState(tableNames: readonly string[]): QueryCatalogState {
    const tables = tableNames.map((name) => this.getTableByName(name));
    const foundTableIds = new Set(
      tables.filter((table): table is TableRecord => table !== undefined).map((table) => table.id),
    );
    // Filter raw records first: normalizing and sorting every segment in the database to
    // keep a handful is the wrong order of operations on a hot path.
    const segments = [...this.#segments.values()]
      .filter((record) => foundTableIds.has(record.tableId))
      .sort((left, right) => left.id.localeCompare(right.id))
      .map((record) => normalizeSegmentRecord(record));
    const transactionIds = [...new Set(segments.map((segment) => segment.transactionId))];
    const transactions = this.getTransactions(transactionIds).filter(
      (record): record is TransactionRecord => record !== undefined,
    );
    return {
      manifestVersion: this.#currentVersion,
      tables,
      segments,
      transactions,
      catalogEpoch: this.#catalogEpoch,
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

  listManifests(): Manifest[] {
    return [...this.#manifests.values()]
      .sort((left, right) => left.version - right.version)
      .map((manifest) => structuredClone(manifest));
  }

  listManifestPage(afterVersion: number | null, limit: number): StoragePage<Manifest, number> {
    validatePageLimit(limit);
    const records = [...this.#manifests.values()]
      .filter((manifest) => afterVersion === null || manifest.version > afterVersion)
      .sort((left, right) => left.version - right.version)
      .slice(0, limit)
      .map((manifest) => structuredClone(manifest));
    return {
      records,
      nextCursor: records.length === limit ? (records.at(-1)?.version ?? null) : null,
    };
  }

  publishManifest(input: PublishManifestInput): Manifest {
    if (this.#currentVersion !== input.expectedVersion) {
      throw new WriteConflictError(input.expectedVersion, this.#currentVersion);
    }
    for (const id of input.blockIds) {
      if (!this.#physical.hasBlock(id)) throw new Error(`Manifest references missing block: ${id}`);
    }
    const manifest = createManifest(input);
    this.#manifests.set(manifest.version, manifest);
    this.#currentVersion = manifest.version;
    this.#catalogEpoch += 1;
    return structuredClone(manifest);
  }

  beginTransaction(input: BeginTransactionInput): BeginTransactionResult {
    validateBeginTransactionInput(input);
    const record: TransactionRecord = {
      ...structuredClone(input.record),
      snapshotVersion: this.#currentVersion,
    };
    this.#assertSnapshotAvailable(record.snapshotVersion);
    if (this.#transactions.has(record.id)) {
      throw new Error(`Transaction already exists: ${record.id}`);
    }
    this.#transactions.set(record.id, record);
    let rowIds: RowIdRange | undefined;
    if (input.reserveRowIds !== undefined) {
      validateCount(input.reserveRowIds.count);
      const current = this.#nextRowIds.get(input.reserveRowIds.tableId) ?? 1n;
      const endExclusive = current + BigInt(input.reserveRowIds.count);
      this.#nextRowIds.set(input.reserveRowIds.tableId, endExclusive);
      rowIds = { start: current, endExclusive };
    }
    let autoIncrementValues: RowIdRange | undefined;
    if (input.reserveAutoIncrement !== undefined) {
      const { tableId, columnId, count, atLeast } = input.reserveAutoIncrement;
      validateAutoIncrementReservation(count, atLeast);
      autoIncrementValues = this.#reserveAutoIncrement(tableId, columnId, count, atLeast);
    }
    return {
      record: structuredClone(record),
      ...(rowIds === undefined ? {} : { rowIds }),
      ...(autoIncrementValues === undefined ? {} : { autoIncrementValues }),
    };
  }

  createTransaction(record: TransactionRecord): void {
    if (this.#transactions.has(record.id)) {
      throw new Error(`Transaction already exists: ${record.id}`);
    }
    this.#assertSnapshotAvailable(record.snapshotVersion);
    this.#assertPendingArtifactsAvailable(record);
    this.#transactions.set(record.id, structuredClone(record));
  }

  getTransaction(id: string): TransactionRecord | undefined {
    const record = this.#transactions.get(id);
    return record === undefined ? undefined : structuredClone(record);
  }

  getTransactions(ids: readonly string[]): Array<TransactionRecord | undefined> {
    return ids.map((id) => {
      const record = this.#transactions.get(id);
      return record === undefined ? undefined : structuredClone(record);
    });
  }

  listTransactions(): TransactionRecord[] {
    return [...this.#transactions.values()]
      .sort(
        (left, right) =>
          left.startedAt.localeCompare(right.startedAt) || left.id.localeCompare(right.id),
      )
      .map((record) => structuredClone(record));
  }

  listTransactionPage(
    afterId: string | null,
    limit: number,
  ): StoragePage<TransactionRecord, string> {
    validatePageLimit(limit);
    const records = [...this.#transactions.values()]
      .filter((record) => afterId === null || record.id > afterId)
      .sort((left, right) => left.id.localeCompare(right.id))
      .slice(0, limit)
      .map((record) => structuredClone(record));
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
    this.#transactions.set(id, updated);
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
    } = {},
  ): TransactionRecord {
    if (options.blocksPrevalidated !== true) {
      const ids = new Set<string>();
      for (const block of input.blocks) {
        validateId(block.id);
        if (ids.has(block.id) || this.#physical.hasBlock(block.id)) {
          throw new Error(`Block already exists: ${block.id}`);
        }
        ids.add(block.id);
      }
    }
    for (const segment of input.segments) {
      if (this.#segments.has(segment.id)) {
        throw new Error(`Segment already exists: ${segment.id}`);
      }
    }
    const current = this.#transactions.get(input.transactionId);
    if (current?.revision !== input.expectedRevision) {
      throw new TransactionRecordConflictError(
        input.transactionId,
        input.expectedRevision,
        current?.revision ?? null,
      );
    }
    const update: TransactionRecordUpdate = {
      pendingBlockIds: [...current.pendingBlockIds, ...input.blocks.map((block) => block.id)],
      pendingSegmentIds: [
        ...current.pendingSegmentIds,
        ...input.segments.map((segment) => segment.id),
      ],
      updatedAt: input.updatedAt,
    };
    assertGenericTransactionUpdateAllowed(current, update);
    const updated = updateTransactionRecord(current, update);
    // Only previously journaled artifacts need existence checks; the new ones land with the
    // journal update in this same atomic step.
    this.#assertPendingArtifactsAvailable(current, true, true);
    for (const segment of input.segments) {
      this.#segments.set(segment.id, normalizeSegmentRecord(segment));
    }
    this.#transactions.set(input.transactionId, updated);
    return structuredClone(updated);
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
    } = {},
  ): ManifestSummary {
    const blockIds = new Set<string>();
    for (const block of input.blocks) {
      if (options.blocksPrevalidated !== true) {
        validateId(block.id);
        if (blockIds.has(block.id) || this.#physical.hasBlock(block.id)) {
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
      segments.set(segment.id, normalizeSegmentRecord(segment));
    }
    let base: TransactionRecord;
    if ("record" in input.transaction) {
      validateBeginTransactionInput({ record: input.transaction.record });
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
    const update: TransactionRecordUpdate = {
      pendingBlockIds: [...base.pendingBlockIds, ...blockIds],
      pendingSegmentIds: [...base.pendingSegmentIds, ...segments.keys()],
      updatedAt: input.committedAt,
    };
    assertGenericTransactionUpdateAllowed(base, update);
    const staged = updateTransactionRecord(base, update);
    // Previously journaled artifacts must exist; the ones staged here land with the commit.
    this.#assertPendingArtifactsAvailable(base, true, true);
    const plan = this.#planCommit(
      staged,
      input,
      (id) => blockIds.has(id) || this.#physical.hasBlock(id),
      (id) => segments.get(id) ?? this.#segments.get(id),
    );
    for (const [id, segment] of segments) this.#segments.set(id, segment);
    this.#transactions.set(staged.id, staged);
    return this.#applyCommit(plan);
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
    getSegment: (id: string) => SegmentRecord | undefined,
  ): CommitPlan {
    if (this.#currentVersion !== input.expectedManifestVersion) {
      throw new WriteConflictError(input.expectedManifestVersion, this.#currentVersion);
    }
    if (transaction.snapshotVersion !== input.expectedManifestVersion) {
      throw new Error("Transaction snapshot does not match the expected manifest");
    }
    const baseManifest =
      input.expectedManifestVersion === null
        ? undefined
        : this.#manifests.get(input.expectedManifestVersion);
    if (input.expectedManifestVersion !== null && baseManifest === undefined) {
      throw new Error(`Snapshot manifest is missing: ${String(input.expectedManifestVersion)}`);
    }
    const baseBlockIds = baseManifest?.blockIds ?? [];
    const removedBlockIds = [...new Set(input.removedBlockIds ?? [])];
    const baseBlockIdSet = new Set(baseBlockIds);
    for (const id of removedBlockIds) {
      if (!baseBlockIdSet.has(id)) {
        throw new Error(`Cannot supersede a block outside the transaction snapshot: ${id}`);
      }
      if (transaction.pendingBlockIds.includes(id)) {
        throw new Error(`Cannot supersede a pending block: ${id}`);
      }
    }
    const removedBlockIdSet = new Set(removedBlockIds);
    // The published list derives from the stored base plus this commit's delta; the record
    // core keeps full manifests internally since cloning in memory is cheap.
    const nextBlockIds = [
      ...baseBlockIds.filter((id) => !removedBlockIdSet.has(id)),
      ...transaction.pendingBlockIds,
    ];
    for (const id of transaction.pendingBlockIds) {
      if (!hasBlock(id)) throw new Error(`Manifest references missing block: ${id}`);
    }
    const pendingSegments = transaction.pendingSegmentIds.map((id) => {
      const segment = getSegment(id);
      if (segment === undefined) throw new Error(`Transaction references missing segment: ${id}`);
      if (segment.transactionId !== transaction.id) {
        throw new Error(`Segment ${id} belongs to another transaction`);
      }
      return segment;
    });
    const uniqueKeyEntries = input.uniqueKeyChanges ?? [];
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
    const manifest = createManifest({
      expectedVersion: input.expectedManifestVersion,
      blockIds: nextBlockIds,
      createdAt: input.committedAt,
      ...(input.changedTableIds === undefined ? {} : { changedTableIds: input.changedTableIds }),
    });
    const committed = updateTransactionRecord(transaction, {
      status: "committed",
      committedVersion: manifest.version,
      updatedAt: input.committedAt,
    });
    return {
      manifest,
      committed,
      pendingSegments,
      uniqueKeyDeltas,
      ftsChanges: input.ftsChanges,
    };
  }

  #applyCommit(plan: CommitPlan): ManifestSummary {
    const { manifest, committed, pendingSegments } = plan;
    this.#manifests.set(manifest.version, manifest);
    this.#currentVersion = manifest.version;
    this.#transactions.set(committed.id, committed);
    for (const segment of pendingSegments) {
      this.#segments.set(segment.id, {
        ...segment,
        level: segment.level ?? 0,
        logicalOrder: segment.logicalOrder ?? manifest.version,
      });
    }
    for (const [tableId, delta] of plan.uniqueKeyDeltas) {
      let tokens = this.#uniqueKeys.get(tableId);
      if (tokens === undefined) {
        tokens = new Set();
        this.#uniqueKeys.set(tableId, tokens);
      }
      for (const token of delta.removed) tokens.delete(token);
      for (const token of delta.added) tokens.add(token);
    }
    this.#applyFtsChanges(pendingSegments, plan.ftsChanges, manifest.version);
    this.#catalogEpoch += 1;
    // Match the IndexedDB store's observable commit shape: the summary without blockIds.
    const { blockIds: _resolved, ...summary } = manifest;
    void _resolved;
    return structuredClone(summary);
  }

  createLease(record: LeaseRecord): void {
    validateLeaseExpiration(record.expiresAt);
    if (this.#leases.has(record.id)) throw new Error(`Lease already exists: ${record.id}`);
    this.#assertSnapshotAvailable(record.manifestVersion);
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

  renewLease(id: string, expectedRevision: number, expiresAt: string): LeaseRecord {
    validateLeaseExpiration(expiresAt);
    const record = this.#leases.get(id);
    if (record?.revision !== expectedRevision) {
      throw new LeaseConflictError(id, expectedRevision, record?.revision ?? null);
    }
    this.#assertSnapshotAvailable(record.manifestVersion);
    const renewed = { ...record, expiresAt, revision: record.revision + 1 };
    this.#leases.set(id, renewed);
    return structuredClone(renewed);
  }

  moveLease(
    id: string,
    expectedRevision: number,
    manifestVersion: number | null,
    expiresAt: string,
  ): LeaseRecord {
    validateLeaseExpiration(expiresAt);
    const record = this.#leases.get(id);
    if (record?.revision !== expectedRevision) {
      throw new LeaseConflictError(id, expectedRevision, record?.revision ?? null);
    }
    this.#assertSnapshotAvailable(manifestVersion);
    const moved = { ...record, manifestVersion, expiresAt, revision: record.revision + 1 };
    this.#leases.set(id, moved);
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

  removeLease(id: string): void {
    this.#leases.delete(id);
  }

  createCompactionJob(record: CompactionJobRecord): void {
    const normalized = normalizeCompactionJobRecord(record);
    if (this.#compactionJobs.has(normalized.id)) {
      throw new Error(`Compaction job already exists: ${normalized.id}`);
    }
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
    const records = [...this.#compactionJobs.values()]
      .filter((record) => afterId === null || record.id > afterId)
      .sort((left, right) => left.id.localeCompare(right.id))
      .slice(0, limit)
      .map((record) => structuredClone(record));
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
    this.#compactionJobs.set(id, updated);
    return structuredClone(updated);
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
      this.#compactionJobs.set(id, published);
      return structuredClone(published);
    }

    const cancelled = updateCompactionJobRecord(current, {
      state: "cancelled",
      updatedAt: cancelledAt,
      error: null,
    });
    const abortedTransaction =
      transaction?.status === "active"
        ? updateTransactionRecord(transaction, {
            status: "aborted",
            updatedAt: cancelledAt,
            committedVersion: null,
          })
        : undefined;
    if (abortedTransaction !== undefined) {
      this.#transactions.set(abortedTransaction.id, abortedTransaction);
    }
    this.#compactionJobs.set(id, cancelled);
    return structuredClone(cancelled);
  }

  removeCompactionJob(id: string): void {
    this.#compactionJobs.delete(id);
  }

  createGarbageCollectionJob(input: CreateGarbageCollectionJobInput): GarbageCollectionJobRecord {
    const record = createGarbageCollectionJobRecord(input);
    if (this.#garbageCollectionJobs.has(record.id)) {
      throw new Error(`Garbage collection job already exists: ${record.id}`);
    }
    assertGarbageCollectionCandidateProvenance(
      record,
      this.#manifests,
      this.#segments,
      this.#transactions,
      this.#compactionJobs,
    );
    this.#garbageCollectionJobs.set(record.id, record);
    return structuredClone(record);
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
    let reclaimedBlockBytes = 0;
    let remaining = input.maxItems;

    const leaseCutoff = Date.parse(current.leaseCutoff);
    assertGarbageCollectionPinsAvailable(
      this.#currentVersion,
      this.#transactions,
      this.#leases.values(),
      this.#compactionJobs.values(),
      leaseCutoff,
      this.#manifests,
      this.#physical,
    );
    let manifestIndex = current.cursor.manifestIndex;
    while (remaining > 0 && manifestIndex < current.candidateManifestVersions.length) {
      const version = current.candidateManifestVersions[manifestIndex];
      if (version === undefined) throw new Error("Garbage collection manifest cursor is invalid");
      const manifest = this.#manifests.get(version);
      if (manifest === undefined) missingManifestVersions.push(version);
      else if (manifest.prunedAt !== undefined) alreadyPrunedManifestVersions.push(version);
      else if (
        isManifestVersionPinned(
          version,
          this.#currentVersion,
          this.#transactions,
          this.#leases.values(),
          this.#compactionJobs.values(),
          leaseCutoff,
        )
      )
        retainedManifestVersions.push(version);
      else prunedManifestVersions.push(version);
      manifestIndex += 1;
      remaining -= 1;
    }

    const prunedManifestVersionSet = new Set(prunedManifestVersions);
    assertRemainingManifestBlocksAvailable(
      this.#manifests.values(),
      prunedManifestVersionSet,
      this.#physical,
    );
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
    const roots = collectBoundedPhysicalRoots(
      segmentIdsToExamine,
      blockIdsToExamine,
      this.#manifests.values(),
      prunedManifestVersionSet,
      this.#segments,
      this.#transactions,
      this.#compactionJobs.values(),
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
      updatedAt: input.updatedAt,
    });
    // A pruned manifest is a tombstone: it cannot be pinned or read, and it roots nothing. The
    // only use left for its block list is finding the blocks that became garbage when it was
    // pruned — the ones the current manifest no longer carries — so that is all it keeps.
    // Keeping the whole list made every commit's manifest cost O(blocks) of memory forever,
    // and every OPFS checkpoint re-serialize all of them.
    const currentBlockIds = new Set(
      this.#currentVersion === null
        ? []
        : (this.#manifests.get(this.#currentVersion)?.blockIds ?? []),
    );
    prunedManifestVersions.forEach((version) => {
      const manifest = this.#manifests.get(version);
      if (manifest !== undefined) {
        this.#manifests.set(version, {
          ...manifest,
          prunedAt: input.updatedAt,
          blockIds: manifest.blockIds.filter((id) => !currentBlockIds.has(id)),
        });
      }
    });
    reclaimedSegmentIds.forEach((id) => this.#segments.delete(id));
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
    };
  }

  removeGarbageCollectionJob(id: string): void {
    this.#garbageCollectionJobs.delete(id);
  }

  /**
   * Copies the current version out as a portable snapshot. The caller provides the block bytes
   * and the atomicity — run this where no commit can interleave.
   *
   * Only blocks the current manifest still points at are copied, which drops every superseded
   * block a compaction left behind, and only full-text bases that already cover this exact
   * version are carried; any other indexed column is exported as `invalid` so the loaded
   * database rebuilds it. That costs a rebuild, never a wrong answer — the index is a pruning
   * accelerator and the scan re-verifies every candidate.
   */
  exportSnapshot(readBlockBytes: (id: string) => Uint8Array | undefined): DatabaseSnapshot {
    const { records, blockIds } = this.exportSnapshotRecords();
    const blocks = blockIds.map((id) => {
      const bytes = readBlockBytes(id);
      if (bytes === undefined) throw new Error(`Manifest references missing block: ${id}`);
      return { id, bytes: bytes.slice() };
    });
    return { ...records, blocks };
  }

  /**
   * The record half of `exportSnapshot`, for stores whose block bytes need asynchronous reads:
   * capture the records synchronously at one version, then read the immutable (and pinned)
   * block files at leisure.
   */
  exportSnapshotRecords(): {
    records: Omit<DatabaseSnapshot, "blocks">;
    blockIds: string[];
  } {
    const version = this.#currentVersion;
    if (version === null) throw new Error("There is no committed version to snapshot");
    const manifest = this.#manifests.get(version);
    if (manifest === undefined) throw new SnapshotManifestMissingError(version);

    const liveBlockIds = new Set(manifest.blockIds);

    const { segments, transactions } = selectLiveRecords({
      liveBlockIds,
      segments: [...this.#segments.values()].map((record) => normalizeSegmentRecord(record)),
      transactions: [...this.#transactions.values()],
      version,
    });

    const tables = [...this.#tables.values()].map((table) => {
      const fts: SnapshotFtsIndex[] = [];
      const ftsColumns = { ...(table.ftsColumns ?? {}) };
      for (const [columnId, state] of Object.entries(ftsColumns)) {
        const base = this.#ftsBases.get(`${table.id}/${columnId}`);
        if (state.state === "ready" && base?.coversVersion === version) {
          fts.push({ columnId, ...structuredClone(base) });
        } else {
          ftsColumns[columnId] = { ...state, state: "invalid" };
        }
      }
      return {
        record: {
          ...structuredClone(table),
          ...(Object.keys(ftsColumns).length === 0 ? {} : { ftsColumns }),
        },
        nextRowId: this.#nextRowIds.get(table.id) ?? 1n,
        autoIncrement: table.columns.flatMap((column) => {
          const next = this.#nextAutoIncrement.get(`${table.id}/${column.id}`);
          return next === undefined ? [] : [{ columnId: column.id, next }];
        }),
        uniqueKeyTokens: [...(this.#uniqueKeys.get(table.id) ?? [])],
        fts,
      };
    });

    return {
      records: {
        version,
        createdAt: new Date().toISOString(),
        tables,
        segments,
        transactions,
      },
      blockIds: [...manifest.blockIds],
    };
  }

  /** Refuses to load over an existing database; see `loadSnapshot` for the load itself. */
  importSnapshot(
    snapshot: DatabaseSnapshot,
    storeBlockBytes: (id: string, bytes: Uint8Array) => void,
  ): void {
    if (this.#currentVersion !== null) throw new Error("This store already holds a database");
    if (this.#tables.size > 0) throw new Error("This store already holds a catalog");
    this.loadSnapshot(snapshot, storeBlockBytes);
  }

  loadSnapshot(
    snapshot: DatabaseSnapshot,
    storeBlockBytes: (id: string, bytes: Uint8Array) => void,
  ): void {
    for (const block of snapshot.blocks) storeBlockBytes(block.id, block.bytes);
    this.loadSnapshotRecords(
      { ...snapshot, blocks: undefined },
      snapshot.blocks.map((block) => block.id),
    );
  }

  /** The record half of a snapshot load; block bytes are the caller's, ids theirs to name. */
  loadSnapshotRecords(
    snapshot: Omit<DatabaseSnapshot, "blocks"> & { blocks?: undefined },
    blockIds: readonly string[],
  ): void {
    for (const segment of snapshot.segments) {
      this.#segments.set(segment.id, normalizeSegmentRecord(segment));
    }
    for (const record of snapshot.transactions) {
      this.#transactions.set(record.id, structuredClone(record));
    }
    for (const table of snapshot.tables) {
      const record = structuredClone(table.record);
      this.#tables.set(record.id, record);
      this.#tableIdsByName.set(record.name, record.id);
      this.#nextRowIds.set(record.id, table.nextRowId);
      for (const entry of table.autoIncrement) {
        this.#nextAutoIncrement.set(`${record.id}/${entry.columnId}`, entry.next);
      }
      if (table.uniqueKeyTokens.length > 0) {
        this.#uniqueKeys.set(record.id, new Set(table.uniqueKeyTokens));
      }
      for (const index of table.fts) {
        this.#ftsBases.set(`${record.id}/${index.columnId}`, {
          coversVersion: index.coversVersion,
          chunks: structuredClone(index.chunks),
          totalTokens: index.totalTokens,
        });
      }
    }
    // One checkpoint, not the captured history: a snapshot restores as a single readable
    // version, and the version number itself is preserved so committed transactions still
    // sort and compare against it.
    this.#manifests.set(snapshot.version, {
      version: snapshot.version,
      previousVersion: null,
      blockIds: [...blockIds],
      createdAt: snapshot.createdAt,
    });
    this.#currentVersion = snapshot.version;
    this.#catalogEpoch += 1;
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
      manifests: [...this.#manifests.values()],
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
      tempOwners: [...this.#tempOwners.values()],
    };
  }

  /** Replaces the whole record state with a dump's content. */
  load(state: RecordCoreState): void {
    const cloned = structuredClone(state);
    for (const map of [
      this.#manifests,
      this.#transactions,
      this.#tables,
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
      this.#tempOwners,
    ] as Array<Map<unknown, unknown>>) {
      map.clear();
    }
    this.#currentVersion = cloned.currentVersion;
    this.#catalogEpoch = cloned.catalogEpoch;
    for (const manifest of cloned.manifests) this.#manifests.set(manifest.version, manifest);
    for (const record of cloned.transactions) this.#transactions.set(record.id, record);
    for (const record of cloned.tables) {
      this.#tables.set(record.id, record);
      this.#tableIdsByName.set(record.name, record.id);
    }
    for (const record of cloned.segments) this.#segments.set(record.id, record);
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
      this.#uniqueKeys.set(tableId, new Set(tokens));
    }
    for (const record of cloned.tempOwners) this.#tempOwners.set(record.ownerId, record);
  }

  #assertSnapshotAvailable(version: number | null): void {
    assertSnapshotAvailable(version, this.#manifests, this.#physical);
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
  if (id.length === 0) throw new TypeError("Block ID cannot be empty");
}

function validateCount(count: number): void {
  if (!Number.isSafeInteger(count) || count <= 0) {
    throw new RangeError("Row ID reservation count must be a positive whole number");
  }
}

export function validateAutoIncrementReservation(count: number, atLeast: bigint | undefined): void {
  if (!Number.isSafeInteger(count) || count < 0) {
    throw new RangeError("Auto-increment reservation count must be a non-negative whole number");
  }
  if (atLeast !== undefined && atLeast < 1n) {
    throw new RangeError("Auto-increment bump target must be at least 1");
  }
}

export function validateBeginTransactionInput(input: BeginTransactionInput): void {
  if (input.record.pendingBlockIds.length > 0 || input.record.pendingSegmentIds.length > 0) {
    throw new TypeError("A fresh transaction cannot begin with pending artifacts");
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
  physical: PhysicalBlocks,
): void {
  if (version === null) return;
  const manifest = manifests.get(version);
  if (
    manifest === undefined ||
    manifest.prunedAt !== undefined ||
    manifest.blockIds.some((id) => !physical.hasBlock(id))
  ) {
    throw new SnapshotManifestMissingError(version);
  }
}

function assertGarbageCollectionPinsAvailable(
  currentVersion: number | null,
  transactions: ReadonlyMap<string, TransactionRecord>,
  leases: Iterable<LeaseRecord>,
  compactionJobs: Iterable<CompactionJobRecord>,
  leaseCutoff: number,
  manifests: ReadonlyMap<number, Manifest>,
  physical: PhysicalBlocks,
): void {
  if (currentVersion !== null) assertSnapshotAvailable(currentVersion, manifests, physical);
  for (const transaction of transactions.values()) {
    if (transaction.status === "active") {
      assertSnapshotAvailable(transaction.snapshotVersion, manifests, physical);
    }
  }
  for (const lease of leases) {
    const expiresAt = Date.parse(lease.expiresAt);
    if (!Number.isFinite(expiresAt) || expiresAt > leaseCutoff) {
      assertSnapshotAvailable(lease.manifestVersion, manifests, physical);
    }
  }
  for (const job of compactionJobs) {
    if (isTerminalCompactionJob(job)) continue;
    assertSnapshotAvailable(job.sourceManifestVersion, manifests, physical);
    const linkedTransaction =
      job.transactionId === null ? undefined : transactions.get(job.transactionId);
    if (linkedTransaction?.status === "committed") {
      if (linkedTransaction.committedVersion === null) {
        throw new Error(`Committed transaction has no manifest version: ${linkedTransaction.id}`);
      }
      assertSnapshotAvailable(linkedTransaction.committedVersion, manifests, physical);
    }
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
}

function assertRemainingManifestBlocksAvailable(
  manifests: Iterable<Manifest>,
  newlyPrunedVersions: ReadonlySet<number>,
  physical: PhysicalBlocks,
): void {
  for (const manifest of manifests) {
    if (manifest.prunedAt !== undefined || newlyPrunedVersions.has(manifest.version)) continue;
    if (manifest.blockIds.some((id) => !physical.hasBlock(id))) {
      throw new SnapshotManifestMissingError(manifest.version);
    }
  }
}

function assertGarbageCollectionCandidateProvenance(
  job: GarbageCollectionJobRecord,
  manifests: ReadonlyMap<number, Manifest>,
  segments: ReadonlyMap<string, SegmentRecord>,
  transactions: ReadonlyMap<string, TransactionRecord>,
  compactionJobs: ReadonlyMap<string, CompactionJobRecord>,
): void {
  for (const version of job.candidateManifestVersions) {
    if (!manifests.has(version)) {
      throw new Error(`Garbage collection candidate manifest is missing: ${String(version)}`);
    }
  }
  const blockHasProvenance = (id: string): boolean => {
    for (const version of job.candidateManifestVersions) {
      if (manifests.get(version)?.blockIds.includes(id)) return true;
    }
    for (const transaction of transactions.values()) {
      if (transaction.status === "aborted" && transaction.pendingBlockIds.includes(id)) return true;
    }
    for (const compaction of compactionJobs.values()) {
      if (
        isTerminalCompactionJob(compaction) &&
        (compaction.sourceBlockIds.includes(id) || compaction.outputBlockIds.includes(id))
      ) {
        return true;
      }
    }
    return false;
  };
  const unprovenBlockId = job.candidateBlockIds.find((id) => !blockHasProvenance(id));
  if (unprovenBlockId !== undefined) {
    throw new Error(
      `Garbage collection block candidate has no persisted provenance: ${unprovenBlockId}`,
    );
  }
  const unprovenSegmentId = job.candidateSegmentIds.find((id) => {
    for (const transaction of transactions.values()) {
      if (transaction.status === "aborted" && transaction.pendingSegmentIds.includes(id)) {
        return false;
      }
    }
    for (const compaction of compactionJobs.values()) {
      if (
        isTerminalCompactionJob(compaction) &&
        (compaction.sourceSegmentIds.includes(id) || compaction.outputSegmentId === id)
      ) {
        return false;
      }
    }
    const segment = segments.get(id);
    const blockIds = segment === undefined ? [] : segmentBlockIds(segment);
    return blockIds.length === 0 || !blockIds.every(blockHasProvenance);
  });
  if (unprovenSegmentId !== undefined) {
    throw new Error(
      `Garbage collection segment candidate has no persisted provenance: ${unprovenSegmentId}`,
    );
  }
}

function isManifestVersionPinned(
  version: number,
  currentVersion: number | null,
  transactions: ReadonlyMap<string, TransactionRecord>,
  leases: Iterable<LeaseRecord>,
  compactionJobs: Iterable<CompactionJobRecord>,
  leaseCutoff: number,
): boolean {
  if (currentVersion === version) return true;
  for (const transaction of transactions.values()) {
    if (transaction.status === "active" && transaction.snapshotVersion === version) return true;
  }
  for (const lease of leases) {
    const expiresAt = Date.parse(lease.expiresAt);
    if (
      lease.manifestVersion === version &&
      (!Number.isFinite(expiresAt) || expiresAt > leaseCutoff)
    ) {
      return true;
    }
  }
  for (const job of compactionJobs) {
    if (isTerminalCompactionJob(job)) continue;
    if (job.sourceManifestVersion === version) return true;
    const linkedTransaction =
      job.transactionId === null ? undefined : transactions.get(job.transactionId);
    if (linkedTransaction?.status === "committed") {
      if (linkedTransaction.committedVersion === null) {
        throw new Error(`Committed transaction has no manifest version: ${linkedTransaction.id}`);
      }
      if (linkedTransaction.committedVersion === version) return true;
    }
  }
  return false;
}

const MAX_GARBAGE_COLLECTION_ROOT_DEPENDENCIES = 4_096;

function collectBoundedPhysicalRoots(
  candidateSegmentIds: readonly string[],
  candidateBlockIds: readonly string[],
  manifests: Iterable<Manifest>,
  newlyPrunedVersions: ReadonlySet<number>,
  segments: ReadonlyMap<string, SegmentRecord>,
  transactions: ReadonlyMap<string, TransactionRecord>,
  compactionJobs: Iterable<CompactionJobRecord>,
): { blockIds: Set<string>; segmentIds: Set<string> } {
  const candidateBlocks = new Set(candidateBlockIds);
  const probeBlockIds = new Set(candidateBlockIds);
  const probeSegmentIds = new Set(candidateSegmentIds);
  const relatedSegments = new Map<string, SegmentRecord>();
  let dependencyOverflow = false;
  for (const segment of segments.values()) {
    const ids = segmentBlockIds(segment);
    if (!candidateSegmentIds.includes(segment.id) && !ids.some((id) => candidateBlocks.has(id))) {
      continue;
    }
    const newBlockIds = new Set(ids.filter((id) => !probeBlockIds.has(id)));
    if (
      relatedSegments.size >= MAX_GARBAGE_COLLECTION_ROOT_DEPENDENCIES ||
      probeBlockIds.size + newBlockIds.size > MAX_GARBAGE_COLLECTION_ROOT_DEPENDENCIES
    ) {
      dependencyOverflow = true;
      break;
    }
    relatedSegments.set(segment.id, segment);
    probeSegmentIds.add(segment.id);
    ids.forEach((id) => probeBlockIds.add(id));
  }
  if (dependencyOverflow) {
    return { blockIds: new Set(candidateBlockIds), segmentIds: new Set(candidateSegmentIds) };
  }

  const directBlockRoots = new Set<string>();
  const directSegmentRoots = new Set<string>();
  for (const manifest of manifests) {
    if (manifest.prunedAt !== undefined || newlyPrunedVersions.has(manifest.version)) continue;
    for (const id of manifest.blockIds) if (probeBlockIds.has(id)) directBlockRoots.add(id);
  }
  for (const transaction of transactions.values()) {
    if (transaction.status !== "active") continue;
    for (const id of transaction.pendingBlockIds)
      if (probeBlockIds.has(id)) directBlockRoots.add(id);
    for (const id of transaction.pendingSegmentIds)
      if (probeSegmentIds.has(id)) directSegmentRoots.add(id);
  }
  for (const job of compactionJobs) {
    if (isTerminalCompactionJob(job)) continue;
    for (const id of job.sourceBlockIds) if (probeBlockIds.has(id)) directBlockRoots.add(id);
    for (const id of job.outputBlockIds) if (probeBlockIds.has(id)) directBlockRoots.add(id);
    for (const id of job.sourceSegmentIds) if (probeSegmentIds.has(id)) directSegmentRoots.add(id);
    if (job.outputSegmentId !== null && probeSegmentIds.has(job.outputSegmentId)) {
      directSegmentRoots.add(job.outputSegmentId);
    }
  }

  const rootedBlockIds = new Set<string>();
  const rootedSegmentIds = new Set<string>();
  for (const segment of relatedSegments.values()) {
    const ids = segmentBlockIds(segment);
    if (
      directSegmentRoots.has(segment.id) ||
      transactions.get(segment.transactionId)?.status === "active" ||
      (ids.length > 0 && ids.every((id) => directBlockRoots.has(id)))
    ) {
      if (candidateSegmentIds.includes(segment.id)) rootedSegmentIds.add(segment.id);
      for (const id of ids) if (candidateBlocks.has(id)) rootedBlockIds.add(id);
    }
  }
  for (const id of candidateBlockIds) if (directBlockRoots.has(id)) rootedBlockIds.add(id);
  for (const id of candidateSegmentIds) if (directSegmentRoots.has(id)) rootedSegmentIds.add(id);
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
  if (!Number.isSafeInteger(input.maxItems) || input.maxItems <= 0) {
    throw new RangeError("Garbage collection item limit must be a positive whole number");
  }
  if (input.updatedAt.length === 0 || !Number.isFinite(Date.parse(input.updatedAt))) {
    throw new TypeError("Garbage collection update timestamp must be valid");
  }
}

function validatePageLimit(limit: number): void {
  if (!Number.isSafeInteger(limit) || limit <= 0) {
    throw new RangeError("Storage page limit must be a positive whole number");
  }
}

function validateTableColumns(columns: readonly TableColumnRecord[]): void {
  if (columns.length === 0) throw new TypeError("A table needs at least one column");
  const ids = new Set(columns.map(({ id }) => id));
  const names = new Set(columns.map(({ name }) => name));
  if (ids.size !== columns.length || names.size !== columns.length) {
    throw new TypeError("Table columns must have unique IDs and names");
  }
}

function validateTempOwnerRecord(record: TempOwnerRecord): void {
  validateId(record.ownerId);
  validateLeaseExpiration(record.expiresAt);
  if (record.revision !== 0) {
    throw new RangeError("Temp owner record must be created at revision zero");
  }
}

export function validateLeaseExpiration(expiresAt: string): void {
  if (expiresAt.length === 0 || !Number.isFinite(Date.parse(expiresAt))) {
    throw new TypeError("Lease expiration must be valid");
  }
}

export function validateTempRunPage(page: TempRunPage): void {
  validateTempRunPageIdentity(page.ownerId, page.runId, page.pageIndex);
  if (!(page.bytes instanceof Uint8Array)) throw new TypeError("Temp run page bytes are invalid");
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
  };
}

function safeStorageSum(left: number, right: number): number {
  const total = left + right;
  if (!Number.isSafeInteger(total)) {
    throw new RangeError("Garbage collection reclaimed block bytes exceed the safe range");
  }
  return total;
}
