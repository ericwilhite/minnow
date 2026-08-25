import { dateIsoString, dateMilliseconds } from "../date-value.js";
import {
  assertTransactionArtifactBatchLimits,
  assertTransactionArtifactJournalLimits,
  type BlockStore,
  type BlockWrite,
  GarbageCollectionJobConflictError,
  LeaseConflictError,
  LeaseExpiredError,
  type LeaseKind,
  type LeaseRecord,
  MAX_LEVEL_ZERO_SEGMENTS,
  MAX_MANIFEST_BLOCK_PRESENCE_IDS,
  MAX_LEASE_TTL_MS,
  MAX_TRANSACTION_STAGE_BLOCKS,
  MAX_TRANSACTION_STAGE_BYTES,
  MAX_TRANSACTION_STAGE_SEGMENTS,
  MAX_TRANSACTION_COMMIT_DELTA_BYTES,
  MAX_TRANSACTION_COMMIT_DELTA_ENTRIES,
  transactionCommitDeltaRetainedBytes,
  type ManifestSummary,
  type RowIdRange,
  type SegmentRecord,
  type TableRecord,
  SnapshotManifestMissingError,
  type TransactionRecord,
  TransactionRecordConflictError,
  type FtsChanges,
  type FtsPosting,
  type UniqueKeyChanges,
  WriteConflictError,
} from "../storage/types.js";

export interface TransactionManagerOptions {
  now?: () => Date;
  createId?: () => string;
  /** Durable writer deadline; live transactions renew every third of this interval. */
  transactionTtlMs?: number;
  /** Test seam for deterministic collection of an abandoned transaction reference. */
  createWeakRef?: (transaction: DatabaseTransaction) => {
    deref(): DatabaseTransaction | undefined;
  };
}

export const DEFAULT_TRANSACTION_TTL_MS = 30_000;
export { MAX_LEASE_TTL_MS };

export interface RecoveryOptions {
  staleBefore: Date;
  removePendingBlocks?: boolean;
}

export interface RecoveryReport {
  abortedTransactionIds: string[];
  skippedTransactionIds: string[];
  removedBlockIds: string[];
  retainedBlockIds: string[];
  removedSegmentIds: string[];
  retainedSegmentIds: string[];
}

export interface OpenLeasedSnapshotOptions {
  /** Optional caller-owned ID used by internal short-lived leases. */
  id?: string;
  ownerId: string;
  ttlMs: number;
  kind?: LeaseKind;
  version?: number | null;
}

export interface BeginDeferredOptions {
  /**
   * Pins the captured snapshot with a renewable durable reader lease until this transaction
   * either persists its own durable owner or finishes. Enabled by default; tightly bounded
   * internal statements that cannot idle may opt out to retain the one-write fast path.
   */
  durableSnapshot?: boolean;
}

export class TransactionClosedError extends Error {
  override readonly name = "TransactionClosedError";

  constructor(readonly transactionId: string) {
    super(`Transaction is no longer active: ${transactionId}`);
  }
}

/** Opaque staged-state marker used by SQL SAVEPOINT. */
export interface TransactionCheckpoint {
  readonly transactionId: string;
  readonly pendingBlockIds: readonly string[];
  readonly pendingSegmentIds: readonly string[];
  readonly uniqueKeyChanges: readonly UniqueKeyChanges[];
  readonly ftsChanges: readonly FtsChanges[];
  readonly compactionJobId: string | null;
  readonly compactionSourceBlockIds: readonly string[];
  readonly levelZeroSegmentLimits: ReadonlyArray<{ tableId: string; limit: number }>;
  readonly changedTableIds: readonly string[];
  readonly logicallyUnchanged: boolean;
}

export class Snapshot {
  constructor(
    private readonly store: BlockStore,
    readonly version: number | null,
  ) {}

  async hasBlock(id: string): Promise<boolean> {
    return (await this.store.hasManifestBlocks(this.version, [id]))[0] === true;
  }

  async hasBlocks(ids: readonly string[]): Promise<boolean[]> {
    const present: boolean[] = [];
    for (let start = 0; start < ids.length; start += MAX_MANIFEST_BLOCK_PRESENCE_IDS) {
      present.push(
        ...(await this.store.hasManifestBlocks(
          this.version,
          ids.slice(start, start + MAX_MANIFEST_BLOCK_PRESENCE_IDS),
        )),
      );
    }
    return present;
  }

  async getBlock(id: string): Promise<Uint8Array | undefined> {
    return this.store.readManifestBlock(this.version, id);
  }
}

export class LeasedSnapshot extends Snapshot {
  #record: LeaseRecord;
  #released = false;

  constructor(
    store: BlockStore,
    version: number | null,
    record: LeaseRecord,
    private readonly now: () => Date,
  ) {
    super(store, version);
    this.#record = structuredClone(record);
    this.#store = store;
  }

  readonly #store: BlockStore;

  get leaseId(): string {
    return this.#record.id;
  }

  get expiresAt(): Date {
    return new Date(this.#record.expiresAt);
  }

  override async getBlock(id: string): Promise<Uint8Array | undefined> {
    this.#assertOpen();
    return super.getBlock(id);
  }

  async renew(ttlMs: number): Promise<Date> {
    this.#assertOpen();
    const now = this.now();
    const expiresAt = expiryAfter(now, ttlMs);
    this.#record = await this.#store.renewLease({
      id: this.#record.id,
      expectedRevision: this.#record.revision,
      expiresAtCutoff: dateIsoString(now),
      expiresAt,
    });
    return new Date(this.#record.expiresAt);
  }

  /**
   * Re-pins this lease to another version in one store step, handing the record over to the
   * returned snapshot; this one is closed on success (the lease is not released — it has
   * moved). Undefined when the store lacks the atomic move; the store's typed refusals
   * (`SnapshotManifestMissingError`, `LeaseConflictError`) leave this snapshot open and pass
   * through.
   */
  async moveTo(version: number | null, ttlMs: number): Promise<LeasedSnapshot | undefined> {
    this.#assertOpen();
    const move = this.#store.moveLease?.bind(this.#store);
    if (move === undefined) return undefined;
    const now = this.now();
    const expiresAt = expiryAfter(now, ttlMs);
    const record = await move({
      id: this.#record.id,
      expectedRevision: this.#record.revision,
      manifestVersion: version,
      expiresAtCutoff: dateIsoString(now),
      expiresAt,
    });
    this.#released = true;
    return new LeasedSnapshot(this.#store, version, record, this.now);
  }

  async release(): Promise<void> {
    if (this.#released) return;
    await this.#store.removeLease({ id: this.#record.id, ownerId: this.#record.ownerId });
    this.#released = true;
  }

  /** Closes this snapshot without touching the store — for a lease the store no longer holds. */
  discard(): void {
    this.#released = true;
  }

  #assertOpen(): void {
    if (this.#released) throw new Error(`Snapshot lease has been released: ${this.#record.id}`);
  }
}

export class DatabaseTransaction {
  #record: TransactionRecord;
  readonly #uniqueKeyChanges: UniqueKeyChanges[] = [];
  readonly #ftsChanges = new Map<
    string,
    Map<string, { postings: Map<string, FtsPosting>; totalTokens: number }>
  >();
  #commitDeltaBytes = 0;
  #commitDeltaEntries = 0;
  #compactionJobId: string | null = null;
  readonly #compactionSourceBlockIds = new Set<string>();
  readonly #changedTableIds = new Set<string>();
  readonly #levelZeroSegmentLimits = new Map<string, number>();
  #logicallyUnchanged = false;
  /**
   * One storage-sized artifact batch may stay process-local until commit. This lets the common
   * one-statement transaction use the store's atomic writeTransaction operation instead of
   * staging a durable journal and committing it in a second adapter transaction. A second batch
   * flushes this one first, so retained bytes remain bounded by the storage batch limits.
   */
  readonly #deferredBlocks: BlockWrite[] = [];
  readonly #deferredSegments: SegmentRecord[] = [];
  /** Segment metadata retained for local post-commit catalog-cache advancement. */
  readonly #knownSegments = new Map<string, SegmentRecord>();
  /**
   * Whether the record exists in the store. A deferred transaction (`beginDeferred`) starts
   * without one: it is written on the first two-step staging call, or folded into the
   * single-shot `stageArtifactsAndCommit`, or never — an abort of an unpersisted transaction
   * touches nothing.
   */
  #persisted: boolean;
  /** Durable pin for a deferred transaction that has no transaction record yet. */
  #snapshotLease: LeasedSnapshot | undefined;
  /** Serializes revisioned operations against the one lease record. */
  #snapshotLeaseOperation: Promise<void> = Promise.resolve();
  #snapshotRenewal: Promise<void> | undefined;
  #heartbeatTimer: ReturnType<typeof setInterval> | undefined;
  #renewal: Promise<{ owned: boolean; expiresAt: string }> | undefined;
  readonly #initialCatalogProbe:
    { catalogEpoch: number; manifestVersion: number | null; schemaEpoch: number } | undefined;

  constructor(
    private readonly store: BlockStore,
    record: TransactionRecord,
    private readonly now: () => Date,
    private readonly ttlMs: number,
    createWeakRef: (transaction: DatabaseTransaction) => {
      deref(): DatabaseTransaction | undefined;
    },
    options: {
      persisted?: boolean;
      initialCatalogProbe?: {
        catalogEpoch: number;
        manifestVersion: number | null;
        schemaEpoch: number;
      };
      snapshotLease?: LeasedSnapshot;
    } = {},
  ) {
    this.#record = structuredClone(record);
    this.#persisted = options.persisted ?? true;
    this.#initialCatalogProbe = options.initialCatalogProbe;
    this.#snapshotLease = options.snapshotLease;
    const reference = createWeakRef(this);
    const timer = setInterval(
      () => {
        const transaction = reference.deref();
        if (transaction === undefined) {
          clearInterval(timer);
          return;
        }
        void transaction.#renewOwnership(true).catch(() => undefined);
      },
      Math.max(1, Math.floor(ttlMs / 3)),
    );
    (timer as { unref?: () => void }).unref?.();
    this.#heartbeatTimer = timer;
  }

  get id(): string {
    return this.#record.id;
  }

  get status(): TransactionRecord["status"] {
    return this.#record.status;
  }

  get snapshotVersion(): number | null {
    return this.#record.snapshotVersion;
  }

  get pendingBlockIds(): string[] {
    return [...this.#record.pendingBlockIds, ...this.#deferredBlocks.map((block) => block.id)];
  }

  get pendingSegmentIds(): string[] {
    return [
      ...this.#record.pendingSegmentIds,
      ...this.#deferredSegments.map((segment) => segment.id),
    ];
  }

  /** Process-local segments retained for a bounded single-shot commit. */
  get deferredSegments(): SegmentRecord[] {
    return structuredClone(this.#deferredSegments);
  }

  get pendingTableNextRowId(): bigint | undefined {
    return this.#record.pendingTableNextRowId;
  }

  get initialCatalogProbe():
    { catalogEpoch: number; manifestVersion: number | null; schemaEpoch: number } | undefined {
    return this.#initialCatalogProbe === undefined ? undefined : { ...this.#initialCatalogProbe };
  }

  /** Exact local commit contribution; undefined until this transaction commits. */
  get committedCatalogContribution():
    | {
        transaction: TransactionRecord;
        segments: SegmentRecord[];
        initialCatalogProbe?: {
          catalogEpoch: number;
          manifestVersion: number | null;
          schemaEpoch: number;
        };
      }
    | undefined {
    if (this.#record.status !== "committed") return undefined;
    return {
      transaction: structuredClone(this.#record),
      segments: structuredClone([...this.#knownSegments.values()]),
      ...(this.#initialCatalogProbe === undefined
        ? {}
        : { initialCatalogProbe: { ...this.#initialCatalogProbe } }),
    };
  }

  get compactionSourceBlockIds(): string[] {
    return [...this.#compactionSourceBlockIds].sort();
  }

  /** Extends durable ownership; scope owners call this while user code is legitimately waiting. */
  async renew(): Promise<void> {
    this.#assertActive();
    await this.#renewOwnership(true);
  }

  /**
   * Counts every registration this transaction carries: staged blocks and segments, unique-key
   * entries, full-text entries, and supersessions. A caller that fails part-way through a
   * multi-step stage compares this before and after to tell "nothing was registered" (the
   * statement is cleanly undone) from "partial work landed" (the transaction can only abort),
   * since there is no statement-level rollback inside a transaction.
   */
  get stagedWorkCount(): number {
    return (
      this.pendingBlockIds.length +
      this.pendingSegmentIds.length +
      this.#uniqueKeyChanges.length +
      this.#ftsChanges.size +
      this.#compactionSourceBlockIds.size
    );
  }

  /** Captures all publish-relevant state; row-id/autoincrement reservations deliberately burn. */
  checkpoint(): TransactionCheckpoint {
    this.#assertActive();
    return {
      transactionId: this.id,
      pendingBlockIds: this.pendingBlockIds,
      pendingSegmentIds: this.pendingSegmentIds,
      uniqueKeyChanges: structuredClone(this.#uniqueKeyChanges),
      ftsChanges: structuredClone(this.#materializedFtsChanges()),
      compactionJobId: this.#compactionJobId,
      compactionSourceBlockIds: [...this.#compactionSourceBlockIds],
      levelZeroSegmentLimits: [...this.#levelZeroSegmentLimits].map(([tableId, limit]) => ({
        tableId,
        limit,
      })),
      changedTableIds: [...this.#changedTableIds],
      logicallyUnchanged: this.#logicallyUnchanged,
    };
  }

  /** Conservative retained-size estimate used to refuse savepoint clones before allocating. */
  checkpointRetainedBytes(): number {
    this.#assertActive();
    let bytes = 256;
    const addString = (value: string): void => {
      bytes = checkpointByteSum(bytes, 16 + value.length * 2);
    };
    addString(this.id);
    for (const id of this.pendingBlockIds) addString(id);
    for (const id of this.pendingSegmentIds) addString(id);
    for (const change of this.#uniqueKeyChanges) {
      addString(change.tableId);
      for (const token of change.keyTokens) addString(token);
      bytes = checkpointByteSum(bytes, 32);
    }
    for (const change of this.#materializedFtsChanges()) {
      addString(change.tableId);
      for (const column of change.columns) {
        addString(column.columnId);
        bytes = checkpointByteSum(bytes, 48);
        for (const posting of column.postings) {
          addString(posting.term);
          bytes = checkpointByteSum(bytes, posting.rowIds.length * 8 + posting.tf.length * 8 + 32);
        }
      }
    }
    if (this.#compactionJobId !== null) addString(this.#compactionJobId);
    for (const id of this.#compactionSourceBlockIds) addString(id);
    for (const [tableId] of this.#levelZeroSegmentLimits) addString(tableId);
    for (const id of this.#changedTableIds) addString(id);
    return bytes;
  }

  /** Rewinds staged artifacts and every in-memory commit delta to one earlier checkpoint. */
  async rollbackTo(checkpoint: TransactionCheckpoint): Promise<void> {
    this.#assertActive();
    if (checkpoint.transactionId !== this.id) {
      throw new TypeError("A transaction checkpoint belongs to another transaction");
    }
    const blockPrefix = checkpoint.pendingBlockIds;
    const segmentPrefix = checkpoint.pendingSegmentIds;
    const retainedBlocks = new Set(blockPrefix);
    const retainedSegments = new Set(segmentPrefix);
    const currentBlockIds = this.pendingBlockIds;
    const currentSegmentIds = this.pendingSegmentIds;
    if (
      blockPrefix.some((id) => !currentBlockIds.includes(id)) ||
      segmentPrefix.some((id) => !currentSegmentIds.includes(id))
    ) {
      throw new TypeError("A transaction checkpoint is no longer reachable");
    }
    const removedBlockIds = currentBlockIds.filter((id) => !retainedBlocks.has(id));
    const removedSegmentIds = currentSegmentIds.filter((id) => !retainedSegments.has(id));
    if (this.#persisted && (removedBlockIds.length > 0 || removedSegmentIds.length > 0)) {
      await this.#renewOwnership();
      this.#record = await this.store.rollbackTransactionArtifacts({
        transactionId: this.id,
        expectedRevision: this.#record.revision,
        pendingBlockIds: blockPrefix,
        pendingSegmentIds: segmentPrefix,
        removeBlockIds: removedBlockIds,
        removeSegmentIds: removedSegmentIds,
        updatedAt: dateIsoString(this.now()),
      });
    } else if (!this.#persisted) {
      for (let index = this.#deferredBlocks.length - 1; index >= 0; index -= 1) {
        if (!retainedBlocks.has(this.#deferredBlocks[index]?.id ?? "")) {
          this.#deferredBlocks.splice(index, 1);
        }
      }
      for (let index = this.#deferredSegments.length - 1; index >= 0; index -= 1) {
        if (!retainedSegments.has(this.#deferredSegments[index]?.id ?? "")) {
          this.#deferredSegments.splice(index, 1);
        }
      }
    }
    for (const id of removedSegmentIds) this.#knownSegments.delete(id);
    this.#uniqueKeyChanges.splice(0);
    this.#ftsChanges.clear();
    this.#commitDeltaBytes = 0;
    this.#commitDeltaEntries = 0;
    for (const changes of checkpoint.uniqueKeyChanges) this.setUniqueKeyChanges(changes);
    for (const changes of checkpoint.ftsChanges) this.setFtsChanges(changes);
    this.#compactionJobId = checkpoint.compactionJobId;
    this.#compactionSourceBlockIds.clear();
    for (const id of checkpoint.compactionSourceBlockIds) this.#compactionSourceBlockIds.add(id);
    this.#levelZeroSegmentLimits.clear();
    for (const { tableId, limit } of checkpoint.levelZeroSegmentLimits) {
      this.#levelZeroSegmentLimits.set(tableId, limit);
    }
    this.#changedTableIds.clear();
    for (const id of checkpoint.changedTableIds) this.#changedTableIds.add(id);
    this.#logicallyUnchanged = checkpoint.logicallyUnchanged;
  }

  async snapshot(): Promise<Snapshot> {
    return loadSnapshot(this.store, this.#record.snapshotVersion);
  }

  async getBlock(id: string): Promise<Uint8Array | undefined> {
    this.#assertActive();
    const deferred = this.#deferredBlocks.find((block) => block.id === id);
    if (deferred !== undefined) return new Uint8Array(deferred.bytes);
    if (this.#record.pendingBlockIds.includes(id)) return this.store.getBlock(id);
    return (await this.snapshot()).getBlock(id);
  }

  #assertProspectiveArtifactJournal(
    blocks: readonly BlockWrite[],
    segments: readonly SegmentRecord[],
  ): void {
    const blockIds = new Set(this.pendingBlockIds);
    const segmentIds = new Set(this.pendingSegmentIds);
    for (const block of blocks) {
      if (block.id.length === 0) throw new TypeError("Block ID cannot be empty");
      if (blockIds.has(block.id)) throw new Error(`Block already exists: ${block.id}`);
      blockIds.add(block.id);
    }
    for (const segment of segments) {
      if (segment.id.length === 0) throw new TypeError("Segment ID cannot be empty");
      if (segmentIds.has(segment.id)) throw new Error(`Segment already exists: ${segment.id}`);
      segmentIds.add(segment.id);
    }
    assertTransactionArtifactJournalLimits([...blockIds], [...segmentIds]);
  }

  async #stageArtifactBatch(
    blocks: readonly BlockWrite[],
    segments: readonly SegmentRecord[],
  ): Promise<void> {
    if (blocks.length === 0 && segments.length === 0) return;
    assertTransactionArtifactBatchLimits(blocks, segments);
    await this.#ensurePersisted();
    await this.#renewOwnership();
    try {
      this.#record = await this.store.stageTransactionArtifacts({
        transactionId: this.id,
        expectedRevision: this.#record.revision,
        blocks,
        segments,
        updatedAt: dateIsoString(this.now()),
      });
    } catch (error) {
      await this.#recoverStagedAcknowledgement(error, blocks, segments);
    }
  }

  async stageBlock(id: string, bytes: Uint8Array): Promise<void> {
    return this.stageBlocks([{ id, bytes }]);
  }

  async stageBlocks(blocks: readonly BlockWrite[]): Promise<void> {
    this.#assertActive();
    if (blocks.length === 0) return;
    await this.stageArtifacts(blocks, []);
  }

  /**
   * Stages blocks and segments in one journal step. When the store implements the atomic
   * combined operation this is a single storage transaction instead of the four the sequential
   * shape costs (block writes, journal update, segment writes, journal update); either way the
   * observable journal invariant is identical — a journaled artifact always exists.
   */
  async stageArtifacts(
    blocks: readonly BlockWrite[],
    segments: readonly SegmentRecord[],
  ): Promise<void> {
    this.#assertActive();
    for (const segment of segments) {
      if (segment.transactionId !== this.id) {
        throw new Error(`Segment ${segment.id} belongs to another transaction`);
      }
    }
    // The journal is an append-only ordered sequence. Stamp the same position on each segment:
    // reads and recovery use the redundant pair to reject reordering or ambiguous same-commit
    // folds before publication.
    const ordinalBase = this.pendingSegmentIds.length;
    const ordered = segments.map((segment, index) => ({
      ...segment,
      commitOrdinal: ordinalBase + index,
    }));
    this.#assertProspectiveArtifactJournal(blocks, ordered);
    this.#registerLevelZeroSegments(ordered);
    if (blocks.length === 0 && ordered.length === 0) return;
    const batches = transactionArtifactBatches(blocks, ordered);
    if (
      !this.#persisted &&
      this.#deferredBlocks.length === 0 &&
      this.#deferredSegments.length === 0 &&
      batches.length === 1
    ) {
      assertTransactionArtifactBatchLimits(blocks, ordered);
      this.#deferredBlocks.push(
        ...blocks.map((block) => ({ id: block.id, bytes: new Uint8Array(block.bytes) })),
      );
      this.#deferredSegments.push(...structuredClone(ordered));
      for (const segment of ordered) this.#knownSegments.set(segment.id, structuredClone(segment));
      for (const segment of ordered) this.#changedTableIds.add(segment.tableId);
      return;
    }
    await this.#persistDeferredArtifacts();
    for (const batch of batches) {
      await this.#stageArtifactBatch(batch.blocks, batch.segments);
    }
    for (const segment of ordered) this.#knownSegments.set(segment.id, structuredClone(segment));
    for (const segment of ordered) this.#changedTableIds.add(segment.tableId);
  }

  /**
   * Stages blocks and segments and commits, in one step. On a store with the single-shot
   * `writeTransaction` that is one storage transaction — and for a deferred transaction the
   * only one, the record begun, journaled, and committed together. Elsewhere it is
   * `stageArtifacts` followed by `commit`. The outcome and its typed refusals match the
   * two-step shape exactly, and a retry after `WriteConflictError` + `rebase()` is safe:
   * artifacts a failed attempt left journaled are committed rather than staged twice, and a
   * single-shot refusal left nothing to repeat.
   */
  async stageArtifactsAndCommit(
    blocks: readonly BlockWrite[],
    segments: readonly SegmentRecord[],
  ): Promise<ManifestSummary> {
    this.#assertActive();
    for (const segment of segments) {
      if (segment.transactionId !== this.id) {
        throw new Error(`Segment ${segment.id} belongs to another transaction`);
      }
    }
    const pendingBlockIds = new Set(this.#record.pendingBlockIds);
    const pendingSegmentIds = new Set(this.#record.pendingSegmentIds);
    await this.#assertRetainedArtifactsUnchanged(
      blocks.filter((block) => pendingBlockIds.has(block.id)),
      segments.filter((segment) => pendingSegmentIds.has(segment.id)),
    );
    const unstagedBlocks = blocks.filter((block) => !pendingBlockIds.has(block.id));
    const unstagedSegments = segments.filter((segment) => !pendingSegmentIds.has(segment.id));
    if (
      blocks.length + segments.length > 0 &&
      unstagedBlocks.length === 0 &&
      unstagedSegments.length === 0
    ) {
      return this.commit();
    }
    this.#assertProspectiveArtifactJournal(unstagedBlocks, unstagedSegments);
    const single = this.store.writeTransaction?.bind(this.store);
    const batches = transactionArtifactBatches(unstagedBlocks, unstagedSegments);
    if (single === undefined || batches.length !== 1) {
      await this.stageArtifacts(unstagedBlocks, unstagedSegments);
      return this.commit();
    }
    await this.#renewOwnership();
    const ordinalBase = this.#record.pendingSegmentIds.length;
    const ordered = unstagedSegments.map((segment, index) => ({
      ...segment,
      commitOrdinal: ordinalBase + index,
    }));
    assertTransactionArtifactBatchLimits(unstagedBlocks, ordered);
    this.#registerLevelZeroSegments(ordered);
    const changedTableIds = new Set(this.#changedTableIds);
    for (const segment of ordered) changedTableIds.add(segment.tableId);
    const { snapshotVersion: _pinned, ...fresh } = this.#record;
    void _pinned;
    const committedAt = dateIsoString(this.now());
    const committedRevision = advanceRevision(this.#record.revision, 2);
    const compactionJobId = this.#compactionJobId;
    const ftsChanges = this.#materializedFtsChanges();
    // Keep the exact staged metadata before issuing the atomic write. A store is allowed to
    // commit and then lose the acknowledgement; #recoverCommitted turns that into success, so
    // recording this only after the await would make the caller's local catalog advancement
    // omit segments that are already durable.
    for (const segment of ordered) this.#knownSegments.set(segment.id, structuredClone(segment));
    for (const segment of ordered) this.#changedTableIds.add(segment.tableId);
    try {
      const manifest = await single({
        transaction: this.#persisted
          ? { id: this.id, expectedRevision: this.#record.revision }
          : { record: fresh },
        blocks: unstagedBlocks,
        segments: ordered,
        expectedManifestVersion: this.#record.snapshotVersion,
        changedTableIds: this.#logicallyUnchanged ? [] : [...changedTableIds],
        ...(this.#compactionSourceBlockIds.size === 0
          ? {}
          : {
              compactionJobId: requiredCompactionJobId(compactionJobId),
              removedBlockIds: [...this.#compactionSourceBlockIds],
            }),
        ...(this.#uniqueKeyChanges.length === 0
          ? {}
          : { uniqueKeyChanges: this.#uniqueKeyChanges }),
        ...(ftsChanges.length === 0 ? {} : { ftsChanges }),
        ...(this.#levelZeroSegmentLimits.size === 0
          ? {}
          : {
              levelZeroSegmentLimits: [...this.#levelZeroSegmentLimits].map(([tableId, limit]) => ({
                tableId,
                limit,
              })),
            }),
        committedAt,
      });
      this.#record = {
        ...this.#record,
        pendingBlockIds: [...pendingBlockIds, ...unstagedBlocks.map((block) => block.id)],
        pendingSegmentIds: [...pendingSegmentIds, ...ordered.map((segment) => segment.id)],
        status: "committed",
        committedVersion: manifest.version,
        // One journal step and one commit, as the two-step shape would have left it.
        revision: committedRevision,
        updatedAt: committedAt,
      };
      this.#persisted = true;
      this.#stopHeartbeat();
      await this.#releaseSnapshotLease().catch(() => undefined);
      return manifest;
    } catch (error) {
      return this.#recoverCommitted(error);
    }
  }

  /**
   * Adds already-persisted immutable blocks to this transaction's journal.
   *
   * Resumable background jobs use this to reconcile the narrow crash window
   * between an immutable block write and the transaction-record update. The
   * bytes must already exist, and already-journaled IDs are ignored.
   */
  async stageExistingBlocks(blockIds: readonly string[]): Promise<void> {
    this.#assertActive();
    await this.#persistDeferredArtifacts();
    const pending = new Set(this.#record.pendingBlockIds);
    const additions = [...new Set(blockIds)].filter((id) => !pending.has(id));
    if (additions.length === 0) return;
    if (additions.some((id) => id.length === 0)) {
      throw new TypeError("Block ID cannot be empty");
    }
    assertTransactionArtifactJournalLimits(
      [...this.#record.pendingBlockIds, ...additions],
      this.#record.pendingSegmentIds,
    );
    for (const id of additions) {
      if ((await this.store.getBlock(id)) === undefined) {
        throw new Error(`Cannot stage a missing existing block: ${id}`);
      }
    }
    await this.#ensurePersisted();
    await this.#renewOwnership();
    this.#record = await this.store.updateTransaction(this.id, this.#record.revision, {
      pendingBlockIds: [...this.#record.pendingBlockIds, ...additions],
      updatedAt: dateIsoString(this.now()),
    });
  }

  async stageSegment(record: SegmentRecord): Promise<void> {
    this.#assertActive();
    if (record.transactionId !== this.id) {
      throw new Error(`Segment ${record.id} belongs to another transaction`);
    }
    await this.stageArtifacts([], [record]);
  }

  /** Reconciles a persisted segment whose journal update was interrupted. */
  async stageExistingSegment(segmentId: string): Promise<void> {
    this.#assertActive();
    await this.#persistDeferredArtifacts();
    if (this.#record.pendingSegmentIds.includes(segmentId)) return;
    const record = await this.store.getSegment(segmentId);
    if (record === undefined)
      throw new Error(`Cannot stage a missing existing segment: ${segmentId}`);
    if (record.transactionId !== this.id) {
      throw new Error(`Segment ${segmentId} belongs to another transaction`);
    }
    if (record.commitOrdinal !== this.#record.pendingSegmentIds.length) {
      throw new Error(`Existing segment ${segmentId} is not the next journal ordinal`);
    }
    assertTransactionArtifactJournalLimits(this.#record.pendingBlockIds, [
      ...this.#record.pendingSegmentIds,
      segmentId,
    ]);
    this.#registerLevelZeroSegments([record]);
    this.#changedTableIds.add(record.tableId);
    await this.#ensurePersisted();
    await this.#renewOwnership();
    this.#record = await this.store.updateTransaction(this.id, this.#record.revision, {
      pendingSegmentIds: [...this.#record.pendingSegmentIds, segmentId],
      updatedAt: dateIsoString(this.now()),
    });
  }

  /** Atomically adopts an exact unpublished output left by an aborted attempt of this job. */
  async adoptAbortedCompactionSegment(
    record: SegmentRecord,
    abortedOwner: TransactionRecord,
  ): Promise<void> {
    this.#assertActive();
    await this.#persistDeferredArtifacts();
    if (record.transactionId !== this.id) {
      throw new Error(`Segment ${record.id} belongs to another transaction`);
    }
    if (abortedOwner.status !== "aborted") {
      throw new Error(`Segment ${record.id} owner is not aborted`);
    }
    const compactionJobId = requiredCompactionJobId(this.#compactionJobId);
    this.#assertProspectiveArtifactJournal([], [record]);
    this.#registerLevelZeroSegments([record]);
    this.#changedTableIds.add(record.tableId);
    await this.#ensurePersisted();
    await this.#renewOwnership();
    try {
      this.#record = await this.store.adoptAbortedSegment({
        segment: record,
        expectedAbortedTransactionId: abortedOwner.id,
        expectedAbortedTransactionRevision: abortedOwner.revision,
        replacementTransactionId: this.id,
        expectedReplacementTransactionRevision: this.#record.revision,
        compactionJobId,
        updatedAt: dateIsoString(this.now()),
      });
    } catch (error) {
      // A lost acknowledgement is safe to reconcile: the exact segment and journal move are one
      // adapter transaction. Any partial or foreign state fails the strict equality check.
      const [current, segment] = await Promise.all([
        this.store.getTransaction(this.id),
        this.store.getSegment(record.id),
      ]);
      if (
        current?.status !== "active" ||
        !current.pendingSegmentIds.includes(record.id) ||
        segment?.transactionId !== this.id ||
        !sameSegmentArtifacts(segment, record)
      ) {
        throw error;
      }
      this.#record = current;
    }
  }

  /**
   * Records that this commit changes a table's logical content without staging a segment for
   * it — dropping the table, for instance, whose only publish is the retirement of its blocks.
   * Change-driven readers such as live queries need to see it move.
   */
  markTableChanged(tableId: string): void {
    this.#assertActive();
    this.#changedTableIds.add(tableId);
  }

  /**
   * Marks this commit as logically content-preserving (for example a compaction rewrite), so
   * change-driven readers such as live queries skip it even though it stages segments.
   */
  markLogicallyUnchanged(): void {
    this.#assertActive();
    this.#logicallyUnchanged = true;
  }

  /** Accumulated key changes in operation order, for in-scope membership overlays. */
  get accumulatedUniqueKeyChanges(): readonly UniqueKeyChanges[] {
    return this.#uniqueKeyChanges;
  }

  /** Appends one operation's key changes; entries commit in operation order. */
  setUniqueKeyChanges(changes: UniqueKeyChanges): void {
    this.#assertActive();
    if (changes.keyTokens.length > MAX_TRANSACTION_COMMIT_DELTA_ENTRIES) {
      throw new RangeError(
        `Transaction commit deltas exceed ${String(MAX_TRANSACTION_COMMIT_DELTA_ENTRIES)} entries`,
      );
    }
    // Walk the caller-owned array before allocating a Set or clone. The aggregate tracker below
    // charges the deduplicated retained form, while this preflight prevents one hostile call from
    // creating a database-sized temporary allocation first.
    transactionCommitDeltaRetainedBytes([changes], []);
    const normalized: UniqueKeyChanges = {
      tableId: changes.tableId,
      // Deduplicated, not sorted: membership is all a store reads from these, and sorting fifty
      // thousand tokens cost a bulk delete a fifth of its time.
      keyTokens: [...new Set(changes.keyTokens)],
      requireAbsent: changes.requireAbsent,
      ...(changes.remove === undefined ? {} : { remove: changes.remove }),
    };
    this.#reserveCommitDelta([normalized], []);
    this.#uniqueKeyChanges.push(normalized);
  }

  /**
   * Attaches one batch's full-text deltas; applied atomically with the publish. A second
   * batch for the same table merges per column — postings re-sorted by term (each batch's
   * reserved row ids are strictly above the last, so rowIds stay ascending within a term)
   * and token totals summed — so a scope can insert into one FTS table any number of times.
   */
  setFtsChanges(changes: FtsChanges): void {
    this.#assertActive();
    const existingColumns = this.#ftsChanges.get(changes.tableId);
    const tokenTotals = new Map<string, number>();
    for (const column of changes.columns) {
      const prior =
        tokenTotals.get(column.columnId) ?? existingColumns?.get(column.columnId)?.totalTokens ?? 0;
      tokenTotals.set(
        column.columnId,
        safeWholeNumberSum([prior, column.totalTokens], "Full-text transaction token count"),
      );
    }
    this.#reserveCommitDelta([], [changes]);
    let columns = this.#ftsChanges.get(changes.tableId);
    if (columns === undefined) {
      columns = new Map();
      this.#ftsChanges.set(changes.tableId, columns);
    }
    for (const column of changes.columns) {
      const present = columns.get(column.columnId);
      if (present === undefined) {
        columns.set(column.columnId, {
          postings: new Map(
            column.postings.map((posting) => [
              posting.term,
              {
                term: posting.term,
                rowIds: [...posting.rowIds],
                tf: [...posting.tf],
              },
            ]),
          ),
          totalTokens: column.totalTokens,
        });
        continue;
      }
      for (const posting of column.postings) {
        const held = present.postings.get(posting.term);
        if (held === undefined) {
          present.postings.set(posting.term, {
            term: posting.term,
            rowIds: [...posting.rowIds],
            tf: [...posting.tf],
          });
        } else {
          for (const rowId of posting.rowIds) held.rowIds.push(rowId);
          for (const tf of posting.tf) held.tf.push(tf);
        }
      }
      present.totalTokens = tokenTotals.get(column.columnId) ?? present.totalTokens;
    }
  }

  #materializedFtsChanges(): FtsChanges[] {
    return [...this.#ftsChanges].map(([tableId, columns]) => ({
      tableId,
      columns: [...columns].map(([columnId, change]) => ({
        columnId,
        postings: [...change.postings.values()].sort((left, right) =>
          left.term < right.term ? -1 : left.term > right.term ? 1 : 0,
        ),
        totalTokens: change.totalTokens,
      })),
    }));
  }

  #reserveCommitDelta(
    uniqueKeyChanges: readonly UniqueKeyChanges[],
    ftsChanges: readonly FtsChanges[],
  ): void {
    const added = transactionCommitDeltaRetainedBytes(uniqueKeyChanges, ftsChanges);
    const bytes = this.#commitDeltaBytes + added.bytes;
    const entries = this.#commitDeltaEntries + added.entries;
    if (!Number.isSafeInteger(bytes) || bytes > MAX_TRANSACTION_COMMIT_DELTA_BYTES) {
      throw new RangeError(
        `Transaction commit deltas exceed ${String(MAX_TRANSACTION_COMMIT_DELTA_BYTES)} retained bytes`,
      );
    }
    if (!Number.isSafeInteger(entries) || entries > MAX_TRANSACTION_COMMIT_DELTA_ENTRIES) {
      throw new RangeError(
        `Transaction commit deltas exceed ${String(MAX_TRANSACTION_COMMIT_DELTA_ENTRIES)} entries`,
      );
    }
    this.#commitDeltaBytes = bytes;
    this.#commitDeltaEntries = entries;
  }

  /**
   * Binds a physical retirement to one persisted compaction job. Stores revalidate the job,
   * source segments, current manifest, and aliases atomically with publication; this in-memory
   * declaration is intent, never authority to hide arbitrary blocks.
   */
  setCompactionIntent(compactionJobId: string, sourceBlockIds: readonly string[]): void {
    this.#assertActive();
    if (compactionJobId.length === 0) throw new TypeError("Compaction job ID cannot be empty");
    if (sourceBlockIds.length === 0) {
      throw new TypeError("A compaction retirement must name at least one source block");
    }
    const next = new Set(sourceBlockIds);
    if (next.size !== sourceBlockIds.length) {
      throw new TypeError("Compaction source block IDs must be unique");
    }
    if (this.#compactionJobId !== null) {
      if (
        this.#compactionJobId !== compactionJobId ||
        next.size !== this.#compactionSourceBlockIds.size ||
        [...next].some((id) => !this.#compactionSourceBlockIds.has(id))
      ) {
        throw new Error("A transaction cannot change its compaction retirement intent");
      }
      return;
    }
    for (const id of next) {
      if (id.length === 0) throw new TypeError("Compaction source block ID cannot be empty");
      if (this.#record.pendingBlockIds.includes(id)) {
        throw new Error(`A compaction cannot retire its own pending block: ${id}`);
      }
    }
    this.#compactionJobId = compactionJobId;
    for (const id of next) this.#compactionSourceBlockIds.add(id);
  }

  /** Adds an adapter-enforced post-commit level-zero ceiling for one table. */
  limitLevelZeroSegments(tableId: string, limit: number): void {
    this.#assertActive();
    if (tableId.length === 0) throw new TypeError("Table ID cannot be empty");
    if (!Number.isSafeInteger(limit) || limit <= 0) {
      throw new RangeError("Level-zero segment limit must be a positive safe integer");
    }
    const existing = this.#levelZeroSegmentLimits.get(tableId);
    this.#levelZeroSegmentLimits.set(
      tableId,
      existing === undefined ? limit : Math.min(existing, limit),
    );
  }

  #registerLevelZeroSegments(segments: readonly SegmentRecord[]): void {
    for (const segment of segments) {
      if (segment.level !== 0) continue;
      const existing = this.#levelZeroSegmentLimits.get(segment.tableId);
      if (existing === undefined) {
        this.#levelZeroSegmentLimits.set(segment.tableId, MAX_LEVEL_ZERO_SEGMENTS);
      }
    }
  }

  async commit(): Promise<ManifestSummary> {
    this.#assertActive();
    if (this.#deferredBlocks.length > 0 || this.#deferredSegments.length > 0) {
      if (this.store.writeTransaction === undefined) {
        await this.#persistDeferredArtifacts();
      } else {
        const blocks = this.#deferredBlocks.splice(0);
        const segments = this.#deferredSegments.splice(0);
        try {
          return await this.stageArtifactsAndCommit(blocks, segments);
        } catch (error) {
          // A single-shot refusal mutates nothing. Keep the exact immutable artifacts available
          // for the caller's ordinary rebase-and-retry path.
          this.#deferredBlocks.push(...blocks);
          this.#deferredSegments.push(...segments);
          throw error;
        }
      }
    }
    await this.#ensurePersisted();
    await this.#renewOwnership();
    // The store derives the published manifest from its stored base plus this delta, so the
    // commit neither loads nor rebuilds the full block list.
    const committedAt = dateIsoString(this.now());
    const committedRevision = advanceRevision(this.#record.revision, 1);
    const compactionJobId = this.#compactionJobId;
    const ftsChanges = this.#materializedFtsChanges();
    try {
      const manifest = await this.store.commitTransaction({
        transactionId: this.id,
        expectedTransactionRevision: this.#record.revision,
        expectedManifestVersion: this.#record.snapshotVersion,
        changedTableIds: this.#logicallyUnchanged ? [] : [...this.#changedTableIds],
        ...(this.#compactionSourceBlockIds.size === 0
          ? {}
          : {
              compactionJobId: requiredCompactionJobId(compactionJobId),
              removedBlockIds: [...this.#compactionSourceBlockIds],
            }),
        ...(this.#uniqueKeyChanges.length === 0
          ? {}
          : { uniqueKeyChanges: this.#uniqueKeyChanges }),
        ...(ftsChanges.length === 0 ? {} : { ftsChanges }),
        ...(this.#levelZeroSegmentLimits.size === 0
          ? {}
          : {
              levelZeroSegmentLimits: [...this.#levelZeroSegmentLimits].map(([tableId, limit]) => ({
                tableId,
                limit,
              })),
            }),
        committedAt,
      });
      this.#record = {
        ...this.#record,
        status: "committed",
        committedVersion: manifest.version,
        revision: committedRevision,
        updatedAt: committedAt,
      };
      this.#stopHeartbeat();
      await this.#releaseSnapshotLease().catch(() => undefined);
      return manifest;
    } catch (error) {
      return this.#recoverCommitted(error);
    }
  }

  async rebase(): Promise<Snapshot> {
    this.#assertActive();
    for (;;) {
      const currentVersion = await this.store.getCurrentManifestVersion();
      if (!this.#persisted) {
        // Nothing is in the store yet, so move the temporary durable pin before changing the
        // local snapshot. The old version remains protected until the replacement is admitted.
        await this.#moveSnapshotLease(currentVersion);
        this.#record = {
          ...this.#record,
          snapshotVersion: currentVersion,
          updatedAt: dateIsoString(this.now()),
        };
        return new Snapshot(this.store, currentVersion);
      }
      try {
        await this.#renewOwnership();
        this.#record = await this.store.updateTransaction(this.id, this.#record.revision, {
          snapshotVersion: currentVersion,
          updatedAt: dateIsoString(this.now()),
        });
        return new Snapshot(this.store, currentVersion);
      } catch (error) {
        if (error instanceof SnapshotManifestMissingError) {
          continue;
        }
        throw error;
      }
    }
  }

  async abort(): Promise<void> {
    this.#assertActive();
    if (!this.#persisted) {
      // Never written, so there is nothing to mark: the transaction simply ends here.
      this.#deferredBlocks.length = 0;
      this.#deferredSegments.length = 0;
      this.#knownSegments.clear();
      this.#record = { ...this.#record, status: "aborted", updatedAt: dateIsoString(this.now()) };
      this.#stopHeartbeat();
      await this.#releaseSnapshotLease().catch(() => undefined);
      return;
    }
    await this.#renewOwnership();
    this.#record = await this.store.updateTransaction(this.id, this.#record.revision, {
      status: "aborted",
      updatedAt: dateIsoString(this.now()),
    });
    this.#stopHeartbeat();
    await this.#releaseSnapshotLease().catch(() => undefined);
  }

  #assertActive(): void {
    if (this.#record.status !== "active") throw new TransactionClosedError(this.id);
  }

  /**
   * Writes a deferred transaction's record before the first two-step staging call. The pinned
   * version can only be unavailable if a commit moved the manifest on and collection pruned it
   * meanwhile, so that surfaces as the `WriteConflictError` the rebase loops already handle.
   */
  async #ensurePersisted(): Promise<void> {
    if (this.#persisted) return;
    this.#record = {
      ...this.#record,
      expiresAt: this.#nextExpiry(),
      updatedAt: dateIsoString(this.now()),
    };
    try {
      await this.store.createTransaction(this.#record);
    } catch (error) {
      if (error instanceof SnapshotManifestMissingError) {
        throw new WriteConflictError(
          this.#record.snapshotVersion,
          await this.store.getCurrentManifestVersion(),
        );
      }
      throw error;
    }
    this.#persisted = true;
    // The transaction record now owns the same durable pin and renews on this heartbeat. Keep
    // only one record rooted; a failed cleanup remains bounded by the lease expiry.
    void this.#releaseSnapshotLease().catch(() => undefined);
  }

  /** Flushes the bounded process-local batch before a second durable staging operation. */
  async #persistDeferredArtifacts(): Promise<void> {
    if (this.#deferredBlocks.length === 0 && this.#deferredSegments.length === 0) return;
    const blocks = [...this.#deferredBlocks];
    const segments = [...this.#deferredSegments];
    await this.#stageArtifactBatch(blocks, segments);
    this.#deferredBlocks.length = 0;
    this.#deferredSegments.length = 0;
  }

  /** A commit whose acknowledgement was lost still committed: answer from the persisted record. */
  async #recoverCommitted(error: unknown): Promise<ManifestSummary> {
    const persisted = await this.store.getTransaction(this.id);
    if (persisted?.status === "committed" && persisted.committedVersion !== null) {
      const manifest = await this.store.getManifest(persisted.committedVersion);
      if (manifest !== undefined) {
        this.#record = persisted;
        this.#persisted = true;
        this.#stopHeartbeat();
        await this.#releaseSnapshotLease().catch(() => undefined);
        return manifest;
      }
    }
    throw error;
  }

  async #renewOwnership(force = false): Promise<void> {
    await this.#renewSnapshotLease(force);
    if (!this.#persisted || this.#record.status !== "active") return;
    const now = dateMilliseconds(this.now());
    if (!force && Date.parse(this.#record.expiresAt) - now > this.ttlMs / 3) return;
    const expiresAt = expiryAfter(new Date(now), this.ttlMs);
    const renewal =
      this.#renewal ??
      this.store
        .renewTransaction({
          transactionId: this.id,
          ownerId: this.#record.ownerId,
          expiresAtCutoff: dateIsoString(new Date(now)),
          expiresAt,
        })
        .then((owned) => ({ owned, expiresAt }));
    this.#renewal = renewal;
    let result: { owned: boolean; expiresAt: string };
    try {
      result = await renewal;
    } finally {
      if (this.#renewal === renewal) this.#renewal = undefined;
    }
    if (result.owned) {
      this.#record = { ...this.#record, expiresAt: result.expiresAt };
      return;
    }
    const persisted = await this.store.getTransaction(this.id);
    if (persisted !== undefined) this.#record = persisted;
    this.#stopHeartbeat();
    await this.#releaseSnapshotLease().catch(() => undefined);
    throw new TransactionClosedError(this.id);
  }

  async #renewSnapshotLease(force: boolean): Promise<void> {
    const lease = this.#snapshotLease;
    if (lease === undefined || this.#record.status !== "active") return;
    if (
      !force &&
      dateMilliseconds(lease.expiresAt) - dateMilliseconds(this.now()) > this.ttlMs / 3
    ) {
      return;
    }
    const renewal =
      this.#snapshotRenewal ??
      this.#serializeSnapshotLeaseOperation(async () => {
        const current = this.#snapshotLease;
        if (current === undefined || this.#record.status !== "active") return;
        if (
          !force &&
          dateMilliseconds(current.expiresAt) - dateMilliseconds(this.now()) > this.ttlMs / 3
        ) {
          return;
        }
        await current.renew(this.ttlMs);
      });
    this.#snapshotRenewal = renewal;
    try {
      await renewal;
    } finally {
      if (this.#snapshotRenewal === renewal) this.#snapshotRenewal = undefined;
    }
  }

  async #moveSnapshotLease(version: number | null): Promise<void> {
    await this.#serializeSnapshotLeaseOperation(async () => {
      const current = this.#snapshotLease;
      if (current === undefined || current.version === version) return;
      const moved = await current.moveTo(version, this.ttlMs);
      if (moved !== undefined) {
        if (this.#snapshotLease === current) {
          this.#snapshotLease = moved;
        } else {
          // A terminal path detached the old lease while the adapter move was in flight. Do not
          // resurrect it in memory; retire the moved durable record as part of this operation.
          await moved.release().catch(() => undefined);
        }
        return;
      }
      // Custom stores may omit atomic moveLease. Admit a replacement before releasing the old
      // pin, so there is never a reclamation window between versions.
      const createdAt = dateIsoString(this.now());
      const record: LeaseRecord = {
        id: crypto.randomUUID(),
        kind: "reader",
        manifestVersion: version,
        ownerId: this.#record.ownerId,
        createdAt,
        expiresAt: expiryAfter(new Date(createdAt), this.ttlMs),
        revision: 0,
      };
      await this.store.createLease(record);
      const replacement = new LeasedSnapshot(this.store, version, record, this.now);
      if (this.#snapshotLease === current) {
        this.#snapshotLease = replacement;
      } else {
        await replacement.release().catch(() => undefined);
      }
      await current.release().catch(() => undefined);
    });
  }

  async #releaseSnapshotLease(): Promise<void> {
    const lease = this.#snapshotLease;
    if (lease === undefined) return;
    // Detach before the adapter call. Once a transaction record is persisted it is the durable
    // owner; a slow lease removal must not let the next ownership check renew the retiring pin
    // concurrently and turn a healthy handoff into a LeaseConflictError.
    this.#snapshotLease = undefined;
    await this.#serializeSnapshotLeaseOperation(() => lease.release());
  }

  /**
   * Lease revisions are compare-and-swap tokens, so renew, move, and release must observe one
   * another's returned record before issuing the next adapter operation. Keep the tail fulfilled
   * after a refusal: the caller still receives its error, while a terminal cleanup can proceed.
   */
  #serializeSnapshotLeaseOperation<T>(operation: () => Promise<T>): Promise<T> {
    const run = this.#snapshotLeaseOperation.then(operation);
    this.#snapshotLeaseOperation = run.then(
      () => undefined,
      () => undefined,
    );
    return run;
  }

  #nextExpiry(): string {
    return expiryAfter(this.now(), this.ttlMs);
  }

  #stopHeartbeat(): void {
    if (this.#heartbeatTimer !== undefined) clearInterval(this.#heartbeatTimer);
    this.#heartbeatTimer = undefined;
  }

  /**
   * An adapter can durably stage the artifacts and lose the acknowledgement afterwards. Refresh
   * the local revision only when the persisted journal is exactly the one this call would have
   * produced, so the caller's abort/rollback can clean it up without masking the original error.
   */
  async #recoverStagedAcknowledgement(
    error: unknown,
    blocks: readonly BlockWrite[],
    segments: readonly SegmentRecord[],
  ): Promise<never> {
    try {
      const persisted = await this.store.getTransaction(this.id);
      const expectedBlocks = [...this.#record.pendingBlockIds, ...blocks.map((block) => block.id)];
      const expectedSegments = [
        ...this.#record.pendingSegmentIds,
        ...segments.map((segment) => segment.id),
      ];
      if (
        persisted?.status === "active" &&
        persisted.revision === advanceRevision(this.#record.revision, 1) &&
        sameStrings(persisted.pendingBlockIds, expectedBlocks) &&
        sameStrings(persisted.pendingSegmentIds, expectedSegments)
      ) {
        const [storedBlocks, storedSegments] = await Promise.all([
          Promise.all(blocks.map((block) => this.store.getBlock(block.id))),
          Promise.all(segments.map((segment) => this.store.getSegment(segment.id))),
        ]);
        if (
          blocks.every((block, index) => sameBytes(block.bytes, storedBlocks[index])) &&
          segments.every((segment, index) => {
            const stored = storedSegments[index];
            return stored !== undefined && sameSegmentArtifacts(segment, stored);
          })
        ) {
          this.#record = persisted;
          this.#persisted = true;
        }
      }
    } catch {
      // Preserve the operation error. A failed diagnostic read cannot prove a different outcome.
    }
    throw error;
  }

  async #assertRetainedArtifactsUnchanged(
    blocks: readonly BlockWrite[],
    segments: readonly SegmentRecord[],
  ): Promise<void> {
    for (const block of blocks) {
      if (!sameBytes(block.bytes, await this.store.getBlock(block.id))) {
        throw new Error(`Retried block differs from the staged artifact: ${block.id}`);
      }
    }
    for (const segment of segments) {
      const stored = await this.store.getSegment(segment.id);
      if (
        stored === undefined ||
        !sameSegmentArtifacts(stored, { ...segment, commitOrdinal: stored.commitOrdinal })
      ) {
        throw new Error(`Retried segment differs from the staged artifact: ${segment.id}`);
      }
    }
  }
}

function sameStrings(left: readonly string[], right: readonly string[]): boolean {
  return left.length === right.length && left.every((value, index) => value === right[index]);
}

function sameBytes(left: Uint8Array, right: Uint8Array | undefined): boolean {
  return (
    left.byteLength === right?.byteLength && left.every((byte, index) => byte === right[index])
  );
}

export class TransactionManager {
  readonly #now: () => Date;
  readonly #createId: () => string;
  readonly #transactionTtlMs: number;
  #leaseCleanupCursor: string | null = null;
  readonly #createWeakRef: (transaction: DatabaseTransaction) => {
    deref(): DatabaseTransaction | undefined;
  };

  constructor(
    private readonly store: BlockStore,
    options: TransactionManagerOptions = {},
  ) {
    this.#now = options.now ?? (() => new Date());
    this.#createId = options.createId ?? (() => crypto.randomUUID());
    this.#transactionTtlMs = options.transactionTtlMs ?? DEFAULT_TRANSACTION_TTL_MS;
    validateTtl(this.#transactionTtlMs);
    this.#createWeakRef = options.createWeakRef ?? ((transaction) => new WeakRef(transaction));
  }

  async begin(): Promise<DatabaseTransaction> {
    return (await this.beginWithReservation()).transaction;
  }

  /** Atomically reserves an invisible catalog table for a create-and-populate transaction. */
  async beginWithPendingTable(
    record: TableRecord,
    expectedCatalogEpoch: number,
  ): Promise<DatabaseTransaction> {
    const id = this.#createId();
    const ownerId = `${id}/owner`;
    const timestamp = dateIsoString(this.#now());
    let begun: Awaited<ReturnType<BlockStore["beginTransaction"]>>;
    for (;;) {
      try {
        begun = await this.store.beginTransaction({
          record: {
            id,
            ownerId,
            expiresAt: this.#transactionExpiry(),
            pendingBlockIds: [],
            pendingSegmentIds: [],
            status: "active",
            revision: 0,
            startedAt: timestamp,
            updatedAt: timestamp,
            committedVersion: null,
          },
          pendingTable: { record, nextRowId: 1n, expectedCatalogEpoch },
        });
        break;
      } catch (error) {
        // Atomic begin writes nothing when its current-manifest pin loses a pruning race.
        if (error instanceof SnapshotManifestMissingError) continue;
        throw error;
      }
    }
    return new DatabaseTransaction(
      this.store,
      begun.record,
      this.#now,
      this.#transactionTtlMs,
      this.#createWeakRef,
    );
  }

  /**
   * Begins without a transaction record. By default a renewable reader lease durably pins the
   * probed manifest until the first two-step stage hands ownership to a transaction record. The
   * record and lease are both omitted only for internal, uninterrupted single-shot writes that
   * opt out and publish through `writeTransaction`. For writes whose artifacts need no prior
   * reservation; `beginWithReservation` is the shape for the rest.
   */
  async beginDeferred(options: BeginDeferredOptions = {}): Promise<DatabaseTransaction> {
    const durableSnapshot = options.durableSnapshot ?? true;
    for (;;) {
      const probe = await this.store.getCatalogProbe();
      const timestamp = dateIsoString(this.#now());
      const id = this.#createId();
      const ownerId = `${id}/owner`;
      let snapshotLease: LeasedSnapshot | undefined;
      if (durableSnapshot) {
        try {
          snapshotLease = await this.openLeasedSnapshot({
            id: `${id}/snapshot`,
            ownerId,
            ttlMs: this.#transactionTtlMs,
            version: probe.manifestVersion,
          });
        } catch (error) {
          // The version was current at the probe but collection won before the durable pin.
          if (error instanceof SnapshotManifestMissingError) continue;
          throw error;
        }
      }
      return new DatabaseTransaction(
        this.store,
        {
          id,
          ownerId,
          expiresAt: this.#transactionExpiry(),
          snapshotVersion: probe.manifestVersion,
          schemaEpochGuard: probe.schemaEpoch,
          pendingBlockIds: [],
          pendingSegmentIds: [],
          status: "active",
          revision: 0,
          startedAt: timestamp,
          updatedAt: timestamp,
          committedVersion: null,
        },
        this.#now,
        this.#transactionTtlMs,
        this.#createWeakRef,
        {
          persisted: false,
          initialCatalogProbe: probe,
          ...(snapshotLease === undefined ? {} : { snapshotLease }),
        },
      );
    }
  }

  /**
   * Begins a transaction at the current manifest version and optionally reserves row ids and
   * auto-increment values in the same step. Stores implementing the atomic `beginTransaction`
   * pay one storage round trip instead of the version-read/create/reserve sequence; the
   * read-then-create fallback retries when the version it read is pruned before the record lands.
   */
  async beginWithReservation(
    reserveRowIds?: {
      tableId: string;
      count: number;
    },
    reserveAutoIncrement?: { tableId: string; columnId: string; count: number; atLeast?: bigint },
  ): Promise<{
    transaction: DatabaseTransaction;
    rowIds?: RowIdRange;
    autoIncrementValues?: RowIdRange;
  }> {
    const id = this.#createId();
    const ownerId = `${id}/owner`;
    const timestamp = dateIsoString(this.#now());
    let begun: Awaited<ReturnType<BlockStore["beginTransaction"]>>;
    for (;;) {
      try {
        begun = await this.store.beginTransaction({
          record: {
            id,
            ownerId,
            expiresAt: this.#transactionExpiry(),
            pendingBlockIds: [],
            pendingSegmentIds: [],
            status: "active",
            revision: 0,
            startedAt: timestamp,
            updatedAt: timestamp,
            committedVersion: null,
          },
          ...(reserveRowIds === undefined ? {} : { reserveRowIds }),
          ...(reserveAutoIncrement === undefined ? {} : { reserveAutoIncrement }),
        });
        break;
      } catch (error) {
        // The store's atomic begin aborted in full, including any counter reservation.
        if (error instanceof SnapshotManifestMissingError) continue;
        throw error;
      }
    }
    return {
      transaction: new DatabaseTransaction(
        this.store,
        begun.record,
        this.#now,
        this.#transactionTtlMs,
        this.#createWeakRef,
      ),
      ...(begun.rowIds === undefined ? {} : { rowIds: begun.rowIds }),
      ...(begun.autoIncrementValues === undefined
        ? {}
        : { autoIncrementValues: begun.autoIncrementValues }),
    };
  }

  async resume(transactionId: string): Promise<DatabaseTransaction> {
    const record = await this.store.getTransaction(transactionId);
    if (record === undefined) throw new Error(`Transaction not found: ${transactionId}`);
    if (record.status !== "active") throw new TransactionClosedError(transactionId);
    const transaction = new DatabaseTransaction(
      this.store,
      record,
      this.#now,
      this.#transactionTtlMs,
      this.#createWeakRef,
    );
    for (const segmentId of record.pendingSegmentIds) {
      const segment = await this.store.getSegment(segmentId);
      if (segment?.level === 0) {
        transaction.limitLevelZeroSegments(segment.tableId, MAX_LEVEL_ZERO_SEGMENTS);
      }
    }
    await transaction.renew();
    return transaction;
  }

  #transactionExpiry(): string {
    return expiryAfter(this.#now(), this.#transactionTtlMs);
  }

  async openSnapshot(version?: number | null): Promise<Snapshot> {
    if (version === null) return new Snapshot(this.store, null);
    if (version !== undefined) return loadSnapshot(this.store, version);
    return new Snapshot(this.store, await this.store.getCurrentManifestVersion());
  }

  async openLeasedSnapshot(options: OpenLeasedSnapshotOptions): Promise<LeasedSnapshot> {
    validateTtl(options.ttlMs);
    if (options.ownerId.trim().length === 0) throw new TypeError("Lease owner cannot be empty");
    if (options.id?.trim().length === 0) {
      throw new TypeError("Lease ID cannot be empty");
    }
    const id = options.id ?? this.#createId();
    for (;;) {
      const snapshot = new Snapshot(
        this.store,
        options.version === undefined
          ? await this.store.getCurrentManifestVersion()
          : options.version,
      );
      const createdAt = dateIsoString(this.#now());
      const record: LeaseRecord = {
        id,
        kind: options.kind ?? "reader",
        manifestVersion: snapshot.version,
        ownerId: options.ownerId,
        createdAt,
        expiresAt: expiryAfter(new Date(createdAt), options.ttlMs),
        revision: 0,
      };
      try {
        await this.store.createLease(record);
        return new LeasedSnapshot(this.store, snapshot.version, record, this.#now);
      } catch (error) {
        if (options.version === undefined && error instanceof SnapshotManifestMissingError) {
          continue;
        }
        throw error;
      }
    }
  }

  /**
   * Re-pins an open leased snapshot to another version — one reader moving on to a newer
   * commit. With the store's atomic `moveLease` the record stays and moves in one round trip;
   * otherwise a fresh lease is created at the target (`options.id` names it) and the old one
   * released. A lease the store no longer holds — expired and swept — is simply replaced. The
   * given snapshot is closed on return either way; a missing target manifest throws
   * `SnapshotManifestMissingError` and leaves it open.
   */
  async moveLeasedSnapshot(
    snapshot: LeasedSnapshot,
    options: OpenLeasedSnapshotOptions & { version: number | null },
  ): Promise<LeasedSnapshot> {
    validateTtl(options.ttlMs);
    try {
      const moved = await snapshot.moveTo(options.version, options.ttlMs);
      if (moved !== undefined) return moved;
    } catch (error) {
      if (!(error instanceof LeaseConflictError) && !(error instanceof LeaseExpiredError)) {
        throw error;
      }
      // Not ours to move any more — swept after expiring, most likely. A fresh lease takes its
      // place; whatever record remains is left to expire rather than deleted from under anyone.
      const replacement = await this.openLeasedSnapshot(options);
      snapshot.discard();
      return replacement;
    }
    const replacement = await this.openLeasedSnapshot(options);
    await snapshot.release();
    return replacement;
  }

  async removeExpiredLeases(at: Date = this.#now()): Promise<string[]> {
    const timestamp = dateMilliseconds(at);
    if (!Number.isFinite(timestamp)) throw new TypeError("Lease cutoff must be a valid date");
    const removed: string[] = [];
    const cutoff = dateIsoString(at);
    const page = await this.store.listExpiredLeasePage(cutoff, this.#leaseCleanupCursor, 64);
    this.#leaseCleanupCursor = page.nextCursor;
    for (const lease of page.records) {
      try {
        if (await this.store.removeLeaseIfExpired(lease.id, lease.revision, cutoff)) {
          removed.push(lease.id);
        }
      } catch (error) {
        if (error instanceof LeaseConflictError) continue;
        throw error;
      }
    }
    return removed;
  }

  async recover(options: RecoveryOptions): Promise<RecoveryReport> {
    const staleBefore = dateMilliseconds(options.staleBefore);
    if (!Number.isFinite(staleBefore)) throw new TypeError("Recovery cutoff must be a valid date");
    const abortedTransactionIds: string[] = [];
    const skippedTransactionIds: string[] = [];
    const candidates = new Set<string>();
    const segmentCandidates = new Set<string>();
    let transactionCursor: string | null = null;
    do {
      const page = await this.store.listTransactionPage(transactionCursor, 64);
      for (const record of page.records) {
        if (record.status !== "active" || Date.parse(record.expiresAt) > staleBefore) continue;
        try {
          const aborted = await this.store.abortTransactionIfExpired({
            transactionId: record.id,
            expectedOwnerId: record.ownerId,
            expiresAtCutoff: dateIsoString(options.staleBefore),
            updatedAt: dateIsoString(this.#now()),
          });
          if (aborted === undefined) {
            skippedTransactionIds.push(record.id);
            continue;
          }
          abortedTransactionIds.push(record.id);
          aborted.pendingBlockIds.forEach((id) => candidates.add(id));
          aborted.pendingSegmentIds.forEach((id) => segmentCandidates.add(id));
        } catch (error) {
          if (error instanceof TransactionRecordConflictError) {
            skippedTransactionIds.push(record.id);
            continue;
          }
          throw error;
        }
      }
      transactionCursor = page.nextCursor;
    } while (transactionCursor !== null);

    const removedBlockIds: string[] = [];
    const retainedBlockIds: string[] = [];
    const removedSegmentIds: string[] = [];
    const retainedSegmentIds: string[] = [];
    if (options.removePendingBlocks === false) {
      retainedBlockIds.push(...[...candidates].sort());
      retainedSegmentIds.push(...[...segmentCandidates].sort());
    } else if (candidates.size > 0 || segmentCandidates.size > 0) {
      const timestamp = dateIsoString(this.#now());
      const baseId = `recovery/${timestamp}/${this.#createId()}`;
      let suffix = 0;
      let job;
      for (;;) {
        const id = suffix === 0 ? baseId : `${baseId}/${String(suffix)}`;
        if ((await this.store.getGarbageCollectionJob(id)) !== undefined) {
          suffix += 1;
          continue;
        }
        try {
          job = await this.store.createGarbageCollectionJob({
            id,
            candidateManifestVersions: [],
            candidateSegmentIds: [...segmentCandidates],
            candidateBlockIds: [...candidates],
            leaseCutoff: timestamp,
            createdAt: timestamp,
          });
          break;
        } catch (error) {
          if ((await this.store.getGarbageCollectionJob(id)) === undefined) throw error;
          suffix += 1;
        }
      }
      while (job.state !== "completed") {
        try {
          const step = await this.store.runGarbageCollectionStep({
            jobId: job.id,
            expectedRevision: job.revision,
            maxItems: 128,
            updatedAt: dateIsoString(this.#now()),
          });
          removedBlockIds.push(...step.reclaimedBlockIds);
          retainedBlockIds.push(...step.retainedBlockIds);
          removedSegmentIds.push(...step.reclaimedSegmentIds);
          retainedSegmentIds.push(...step.retainedSegmentIds);
          job = step.job;
        } catch (error) {
          if (!(error instanceof GarbageCollectionJobConflictError)) throw error;
          const latest = await this.store.getGarbageCollectionJob(job.id);
          if (latest === undefined)
            throw new Error(`Garbage collection job not found: ${job.id}`, { cause: error });
          job = latest;
        }
      }
    }
    return {
      abortedTransactionIds: abortedTransactionIds.sort(),
      skippedTransactionIds: skippedTransactionIds.sort(),
      removedBlockIds,
      retainedBlockIds,
      removedSegmentIds,
      retainedSegmentIds,
    };
  }
}

function validateTtl(ttlMs: number): void {
  if (!Number.isFinite(ttlMs) || ttlMs <= 0 || ttlMs > MAX_LEASE_TTL_MS) {
    throw new RangeError(
      `Lease lifetime must be a positive number no greater than ${String(MAX_LEASE_TTL_MS)} milliseconds`,
    );
  }
}

function expiryAfter(now: Date, ttlMs: number): string {
  validateTtl(ttlMs);
  const expiresAt = dateMilliseconds(now) + ttlMs;
  if (!Number.isSafeInteger(expiresAt) || expiresAt > 8_640_000_000_000_000) {
    throw new RangeError("Lease expiry is outside the supported Date range");
  }
  return dateIsoString(new Date(expiresAt));
}

function advanceRevision(revision: number, amount: 1 | 2): number {
  if (
    !Number.isSafeInteger(revision) ||
    revision < 0 ||
    revision > Number.MAX_SAFE_INTEGER - amount
  ) {
    throw new RangeError("Transaction revision is outside the safe integer range");
  }
  return revision + amount;
}

function checkpointByteSum(total: number, addition: number): number {
  const next = total + addition;
  if (!Number.isSafeInteger(next)) {
    throw new RangeError("Transaction checkpoint size exceeds the safe integer range");
  }
  return next;
}

function safeWholeNumberSum(values: readonly number[], label: string): number {
  const total = values.reduce((sum, value) => sum + value, 0);
  if (!Number.isSafeInteger(total) || total < 0) {
    throw new RangeError(`${label} exceeds the safe integer range`);
  }
  return total;
}

function requiredCompactionJobId(id: string | null): string {
  if (id === null) throw new Error("Compaction source blocks require a compaction job");
  return id;
}

function sameSegmentArtifacts(left: SegmentRecord, right: SegmentRecord): boolean {
  const leftColumns = Object.keys(left.columnBlockIds).sort();
  const rightColumns = Object.keys(right.columnBlockIds).sort();
  const leftSpans = left.rowIdSpans;
  const rightSpans = right.rowIdSpans;
  return (
    left.id === right.id &&
    left.tableId === right.tableId &&
    left.rowCount === right.rowCount &&
    left.rowIdStart === right.rowIdStart &&
    left.rowIdEndExclusive === right.rowIdEndExclusive &&
    left.kind === right.kind &&
    left.keyColumnId === right.keyColumnId &&
    left.level === right.level &&
    left.logicalOrder === right.logicalOrder &&
    left.commitOrdinal === right.commitOrdinal &&
    left.partitionOrdinal === right.partitionOrdinal &&
    left.createdAt === right.createdAt &&
    leftColumns.length === rightColumns.length &&
    leftColumns.every((columnId, index) => {
      if (columnId !== rightColumns[index]) return false;
      const leftIds = left.columnBlockIds[columnId] ?? [];
      const rightIds = right.columnBlockIds[columnId] ?? [];
      return (
        leftIds.length === rightIds.length && leftIds.every((id, item) => id === rightIds[item])
      );
    }) &&
    leftSpans.length === rightSpans.length &&
    leftSpans.every((span, index) => {
      const other = rightSpans[index];
      return (
        span.rowStart === other?.rowStart &&
        span.rowCount === other.rowCount &&
        span.rowIdStart === other.rowIdStart
      );
    })
  );
}

interface TransactionArtifactBatch {
  blocks: readonly BlockWrite[];
  segments: readonly SegmentRecord[];
}

function transactionBlockBatches(blocks: readonly BlockWrite[]): readonly BlockWrite[][] {
  const batches: BlockWrite[][] = [];
  let batch: BlockWrite[] = [];
  let batchBytes = 0;
  for (const block of blocks) {
    assertTransactionArtifactBatchLimits([block], []);
    if (
      batch.length === MAX_TRANSACTION_STAGE_BLOCKS ||
      batchBytes + block.bytes.byteLength > MAX_TRANSACTION_STAGE_BYTES
    ) {
      batches.push(batch);
      batch = [];
      batchBytes = 0;
    }
    batch.push(block);
    batchBytes += block.bytes.byteLength;
  }
  if (batch.length > 0) batches.push(batch);
  return batches;
}

function transactionArtifactBatches(
  blocks: readonly BlockWrite[],
  segments: readonly SegmentRecord[],
): readonly TransactionArtifactBatch[] {
  let blockBytes = 0;
  for (const block of blocks) {
    assertTransactionArtifactBatchLimits([block], []);
    blockBytes += block.bytes.byteLength;
  }
  if (
    blocks.length <= MAX_TRANSACTION_STAGE_BLOCKS &&
    segments.length <= MAX_TRANSACTION_STAGE_SEGMENTS &&
    blockBytes <= MAX_TRANSACTION_STAGE_BYTES
  ) {
    return [{ blocks, segments }];
  }
  const batches: TransactionArtifactBatch[] = transactionBlockBatches(blocks).map((batch) => ({
    blocks: batch,
    segments: [],
  }));
  for (let start = 0; start < segments.length; start += MAX_TRANSACTION_STAGE_SEGMENTS) {
    batches.push({
      blocks: [],
      segments: segments.slice(start, start + MAX_TRANSACTION_STAGE_SEGMENTS),
    });
  }
  return batches;
}

async function loadSnapshot(store: BlockStore, version: number | null): Promise<Snapshot> {
  if (version === null) return new Snapshot(store, null);
  const manifest = await store.getManifest(version);
  if (manifest === undefined || manifest.prunedAt !== undefined) {
    throw new SnapshotManifestMissingError(version);
  }
  return new Snapshot(store, manifest.version);
}
