import {
  type BlockStore,
  type BlockWrite,
  type LeaseKind,
  type LeaseRecord,
  type Manifest,
  type SegmentRecord,
  type TransactionRecord,
  TransactionRecordConflictError,
  type UniqueKeyChanges,
} from "@browserdatabase/storage-idb";

export interface TransactionManagerOptions {
  now?: () => Date;
  createId?: () => string;
}

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
  ownerId: string;
  ttlMs: number;
  kind?: LeaseKind;
  version?: number;
}

export class TransactionClosedError extends Error {
  override readonly name = "TransactionClosedError";

  constructor(readonly transactionId: string) {
    super(`Transaction is no longer active: ${transactionId}`);
  }
}

export class Snapshot {
  readonly #blockIds: Set<string>;

  constructor(
    private readonly store: BlockStore,
    readonly version: number | null,
    blockIds: readonly string[],
  ) {
    this.#blockIds = new Set(blockIds);
  }

  listBlockIds(): string[] {
    return [...this.#blockIds].sort();
  }

  hasBlock(id: string): boolean {
    return this.#blockIds.has(id);
  }

  async getBlock(id: string): Promise<Uint8Array | undefined> {
    if (!this.#blockIds.has(id)) return undefined;
    return this.store.getBlock(id);
  }
}

export class LeasedSnapshot extends Snapshot {
  #record: LeaseRecord;
  #released = false;

  constructor(
    store: BlockStore,
    version: number | null,
    blockIds: readonly string[],
    record: LeaseRecord,
    private readonly now: () => Date,
  ) {
    super(store, version, blockIds);
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

  async renew(ttlMs: number): Promise<Date> {
    this.#assertOpen();
    validateTtl(ttlMs);
    const expiresAt = new Date(this.now().getTime() + ttlMs).toISOString();
    this.#record = await this.#store.renewLease(this.#record.id, this.#record.revision, expiresAt);
    return new Date(this.#record.expiresAt);
  }

  async release(): Promise<void> {
    if (this.#released) return;
    await this.#store.removeLease(this.#record.id);
    this.#released = true;
  }

  #assertOpen(): void {
    if (this.#released) throw new Error(`Snapshot lease has been released: ${this.#record.id}`);
  }
}

export class DatabaseTransaction {
  #record: TransactionRecord;
  #uniqueKeyChanges: UniqueKeyChanges | undefined;
  readonly #supersededBlockIds = new Set<string>();

  constructor(
    private readonly store: BlockStore,
    record: TransactionRecord,
    private readonly now: () => Date,
  ) {
    this.#record = structuredClone(record);
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
    return [...this.#record.pendingBlockIds];
  }

  get pendingSegmentIds(): string[] {
    return [...this.#record.pendingSegmentIds];
  }

  get supersededBlockIds(): string[] {
    return [...this.#supersededBlockIds].sort();
  }

  async snapshot(): Promise<Snapshot> {
    return loadSnapshot(this.store, this.#record.snapshotVersion);
  }

  async getBlock(id: string): Promise<Uint8Array | undefined> {
    this.#assertActive();
    if (this.#record.pendingBlockIds.includes(id)) return this.store.getBlock(id);
    return (await this.snapshot()).getBlock(id);
  }

  async stageBlock(id: string, bytes: Uint8Array): Promise<void> {
    return this.stageBlocks([{ id, bytes }]);
  }

  async stageBlocks(blocks: readonly BlockWrite[]): Promise<void> {
    this.#assertActive();
    if (blocks.length === 0) return;
    await this.store.addBlocks(blocks);
    this.#record = await this.store.updateTransaction(this.id, this.#record.revision, {
      pendingBlockIds: [...this.#record.pendingBlockIds, ...blocks.map((block) => block.id)],
      updatedAt: this.now().toISOString(),
    });
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
    const pending = new Set(this.#record.pendingBlockIds);
    const additions = [...new Set(blockIds)].filter((id) => !pending.has(id));
    if (additions.length === 0) return;
    if (additions.some((id) => id.length === 0)) {
      throw new TypeError("Block ID cannot be empty");
    }
    const blocks = await this.store.getBlocks(additions);
    const missingIndex = blocks.findIndex((bytes) => bytes === undefined);
    if (missingIndex >= 0) {
      throw new Error(`Cannot stage a missing existing block: ${additions[missingIndex] ?? ""}`);
    }
    this.#record = await this.store.updateTransaction(this.id, this.#record.revision, {
      pendingBlockIds: [...this.#record.pendingBlockIds, ...additions],
      updatedAt: this.now().toISOString(),
    });
  }

  async stageSegment(record: SegmentRecord): Promise<void> {
    this.#assertActive();
    if (record.transactionId !== this.id) {
      throw new Error(`Segment ${record.id} belongs to another transaction`);
    }
    await this.store.addSegment(record);
    this.#record = await this.store.updateTransaction(this.id, this.#record.revision, {
      pendingSegmentIds: [...this.#record.pendingSegmentIds, record.id],
      updatedAt: this.now().toISOString(),
    });
  }

  /** Reconciles a persisted segment whose journal update was interrupted. */
  async stageExistingSegment(segmentId: string): Promise<void> {
    this.#assertActive();
    if (this.#record.pendingSegmentIds.includes(segmentId)) return;
    const record = await this.store.getSegment(segmentId);
    if (record === undefined)
      throw new Error(`Cannot stage a missing existing segment: ${segmentId}`);
    if (record.transactionId !== this.id) {
      throw new Error(`Segment ${segmentId} belongs to another transaction`);
    }
    this.#record = await this.store.updateTransaction(this.id, this.#record.revision, {
      pendingSegmentIds: [...this.#record.pendingSegmentIds, segmentId],
      updatedAt: this.now().toISOString(),
    });
  }

  setUniqueKeyChanges(changes: UniqueKeyChanges): void {
    this.#assertActive();
    this.#uniqueKeyChanges = {
      tableId: changes.tableId,
      keyTokens: [...new Set(changes.keyTokens)].sort(),
      requireAbsent: changes.requireAbsent,
      ...(changes.remove === undefined ? {} : { remove: changes.remove }),
      ...(changes.storageMode === undefined ? {} : { storageMode: changes.storageMode }),
    };
  }

  supersedeBlocks(blockIds: readonly string[]): void {
    this.#assertActive();
    for (const id of blockIds) {
      if (id.length === 0) throw new TypeError("Superseded block ID cannot be empty");
      if (this.#record.pendingBlockIds.includes(id)) {
        throw new Error(`A transaction cannot supersede its own pending block: ${id}`);
      }
      this.#supersededBlockIds.add(id);
    }
  }

  async commit(): Promise<Manifest> {
    this.#assertActive();
    const base = await this.snapshot();
    const blockIds = [
      ...base.listBlockIds().filter((id) => !this.#supersededBlockIds.has(id)),
      ...this.#record.pendingBlockIds,
    ];
    const committedAt = this.now().toISOString();
    try {
      const manifest = await this.store.commitTransaction({
        transactionId: this.id,
        expectedTransactionRevision: this.#record.revision,
        expectedManifestVersion: this.#record.snapshotVersion,
        blockIds,
        ...(this.#supersededBlockIds.size === 0
          ? {}
          : { removedBlockIds: [...this.#supersededBlockIds] }),
        ...(this.#uniqueKeyChanges === undefined
          ? {}
          : { uniqueKeyChanges: this.#uniqueKeyChanges }),
        committedAt,
      });
      this.#record = {
        ...this.#record,
        status: "committed",
        committedVersion: manifest.version,
        revision: this.#record.revision + 1,
        updatedAt: committedAt,
      };
      return manifest;
    } catch (error) {
      const persisted = await this.store.getTransaction(this.id);
      if (persisted?.status === "committed" && persisted.committedVersion !== null) {
        const manifest = await this.store.getManifest(persisted.committedVersion);
        if (manifest !== undefined) {
          this.#record = persisted;
          return manifest;
        }
      }
      throw error;
    }
  }

  async rebase(): Promise<Snapshot> {
    this.#assertActive();
    const current = await this.store.getCurrentManifest();
    this.#record = await this.store.updateTransaction(this.id, this.#record.revision, {
      snapshotVersion: current?.version ?? null,
      updatedAt: this.now().toISOString(),
    });
    return new Snapshot(this.store, current?.version ?? null, current?.blockIds ?? []);
  }

  async abort(): Promise<void> {
    this.#assertActive();
    this.#record = await this.store.updateTransaction(this.id, this.#record.revision, {
      status: "aborted",
      updatedAt: this.now().toISOString(),
    });
  }

  #assertActive(): void {
    if (this.#record.status !== "active") throw new TransactionClosedError(this.id);
  }
}

export class TransactionManager {
  readonly #now: () => Date;
  readonly #createId: () => string;

  constructor(
    private readonly store: BlockStore,
    options: TransactionManagerOptions = {},
  ) {
    this.#now = options.now ?? (() => new Date());
    this.#createId = options.createId ?? (() => crypto.randomUUID());
  }

  async begin(): Promise<DatabaseTransaction> {
    const current = await this.store.getCurrentManifest();
    const timestamp = this.#now().toISOString();
    const record: TransactionRecord = {
      id: this.#createId(),
      snapshotVersion: current?.version ?? null,
      pendingBlockIds: [],
      pendingSegmentIds: [],
      status: "active",
      revision: 0,
      startedAt: timestamp,
      updatedAt: timestamp,
      committedVersion: null,
    };
    await this.store.createTransaction(record);
    return new DatabaseTransaction(this.store, record, this.#now);
  }

  async resume(transactionId: string): Promise<DatabaseTransaction> {
    const record = await this.store.getTransaction(transactionId);
    if (record === undefined) throw new Error(`Transaction not found: ${transactionId}`);
    if (record.status !== "active") throw new TransactionClosedError(transactionId);
    return new DatabaseTransaction(this.store, record, this.#now);
  }

  async openSnapshot(version?: number | null): Promise<Snapshot> {
    if (version === null) return new Snapshot(this.store, null, []);
    if (version !== undefined) return loadSnapshot(this.store, version);
    const current = await this.store.getCurrentManifest();
    return new Snapshot(this.store, current?.version ?? null, current?.blockIds ?? []);
  }

  async openLeasedSnapshot(options: OpenLeasedSnapshotOptions): Promise<LeasedSnapshot> {
    validateTtl(options.ttlMs);
    if (options.ownerId.trim().length === 0) throw new TypeError("Lease owner cannot be empty");
    const snapshot = await this.openSnapshot(options.version);
    const record: LeaseRecord = {
      id: this.#createId(),
      kind: options.kind ?? "reader",
      manifestVersion: snapshot.version,
      ownerId: options.ownerId,
      expiresAt: new Date(this.#now().getTime() + options.ttlMs).toISOString(),
      revision: 0,
    };
    await this.store.createLease(record);
    return new LeasedSnapshot(
      this.store,
      snapshot.version,
      snapshot.listBlockIds(),
      record,
      this.#now,
    );
  }

  async removeExpiredLeases(at: Date = this.#now()): Promise<string[]> {
    const timestamp = at.getTime();
    if (!Number.isFinite(timestamp)) throw new TypeError("Lease cutoff must be a valid date");
    const removed: string[] = [];
    for (const lease of await this.store.listLeases()) {
      if (Date.parse(lease.expiresAt) > timestamp) continue;
      await this.store.removeLease(lease.id);
      removed.push(lease.id);
    }
    return removed.sort();
  }

  async recover(options: RecoveryOptions): Promise<RecoveryReport> {
    const staleBefore = options.staleBefore.getTime();
    if (!Number.isFinite(staleBefore)) throw new TypeError("Recovery cutoff must be a valid date");
    const abortedTransactionIds: string[] = [];
    const skippedTransactionIds: string[] = [];
    const candidates = new Set<string>();
    const segmentCandidates = new Set<string>();
    const records = await this.store.listTransactions();

    for (const record of records) {
      if (record.status !== "active" || Date.parse(record.updatedAt) >= staleBefore) continue;
      try {
        await this.store.updateTransaction(record.id, record.revision, {
          status: "aborted",
          updatedAt: this.#now().toISOString(),
        });
        abortedTransactionIds.push(record.id);
        record.pendingBlockIds.forEach((id) => candidates.add(id));
        record.pendingSegmentIds.forEach((id) => segmentCandidates.add(id));
      } catch (error) {
        if (error instanceof TransactionRecordConflictError) {
          skippedTransactionIds.push(record.id);
          continue;
        }
        throw error;
      }
    }

    const roots = new Set<string>();
    for (const manifest of await this.store.listManifests()) {
      manifest.blockIds.forEach((id) => roots.add(id));
    }
    for (const record of await this.store.listTransactions()) {
      if (record.status === "active") record.pendingBlockIds.forEach((id) => roots.add(id));
    }
    const activeCompactionJobs = (await this.store.listCompactionJobs()).filter(
      (job) => job.state !== "published" && job.state !== "aborted",
    );
    for (const job of activeCompactionJobs) {
      job.sourceBlockIds.forEach((id) => roots.add(id));
      job.outputBlockIds.forEach((id) => roots.add(id));
    }

    const activeSegmentIds = new Set<string>();
    for (const record of await this.store.listTransactions()) {
      if (record.status === "active") {
        record.pendingSegmentIds.forEach((id) => activeSegmentIds.add(id));
      }
    }
    for (const job of activeCompactionJobs) {
      if (job.outputSegmentId !== null) activeSegmentIds.add(job.outputSegmentId);
    }

    const removedBlockIds: string[] = [];
    const retainedBlockIds: string[] = [];
    for (const id of [...candidates].sort()) {
      if (roots.has(id) || options.removePendingBlocks === false) {
        retainedBlockIds.push(id);
      } else {
        await this.store.removeBlock(id);
        removedBlockIds.push(id);
      }
    }
    const removedSegmentIds: string[] = [];
    const retainedSegmentIds: string[] = [];
    for (const id of [...segmentCandidates].sort()) {
      const segment = await this.store.getSegment(id);
      const published =
        segment !== undefined &&
        Object.values(segment.columnBlockIds)
          .flat()
          .every((blockId) => roots.has(blockId));
      if (published || activeSegmentIds.has(id) || options.removePendingBlocks === false) {
        retainedSegmentIds.push(id);
      } else {
        await this.store.removeSegment(id);
        removedSegmentIds.push(id);
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
  if (!Number.isFinite(ttlMs) || ttlMs <= 0) {
    throw new RangeError("Lease lifetime must be a positive number of milliseconds");
  }
}

async function loadSnapshot(store: BlockStore, version: number | null): Promise<Snapshot> {
  if (version === null) return new Snapshot(store, null, []);
  const manifest = await store.getManifest(version);
  if (manifest === undefined) throw new Error(`Snapshot manifest is missing: ${String(version)}`);
  return new Snapshot(store, manifest.version, manifest.blockIds);
}
