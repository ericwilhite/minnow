import {
  type BlockStore,
  type BlockWrite,
  GarbageCollectionJobConflictError,
  LeaseConflictError,
  type LeaseKind,
  type LeaseRecord,
  type ManifestSummary,
  type RowIdRange,
  type SegmentRecord,
  SnapshotManifestMissingError,
  type TransactionRecord,
  TransactionRecordConflictError,
  type FtsChanges,
  type UniqueKeyChanges,
  WriteConflictError,
} from "../storage/index.js";

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
  /** Optional caller-owned ID used by internal short-lived leases. */
  id?: string;
  ownerId: string;
  ttlMs: number;
  kind?: LeaseKind;
  version?: number | null;
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

  override async getBlock(id: string): Promise<Uint8Array | undefined> {
    this.#assertOpen();
    return super.getBlock(id);
  }

  async renew(ttlMs: number): Promise<Date> {
    this.#assertOpen();
    validateTtl(ttlMs);
    const expiresAt = new Date(this.now().getTime() + ttlMs).toISOString();
    this.#record = await this.#store.renewLease(this.#record.id, this.#record.revision, expiresAt);
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
    validateTtl(ttlMs);
    const move = this.#store.moveLease?.bind(this.#store);
    if (move === undefined) return undefined;
    const target = await loadSnapshot(this.#store, version);
    const expiresAt = new Date(this.now().getTime() + ttlMs).toISOString();
    const record = await move(this.#record.id, this.#record.revision, version, expiresAt);
    this.#released = true;
    return new LeasedSnapshot(this.#store, version, target.listBlockIds(), record, this.now);
  }

  async release(): Promise<void> {
    if (this.#released) return;
    await this.#store.removeLease(this.#record.id);
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
  readonly #ftsChanges: FtsChanges[] = [];
  readonly #supersededBlockIds = new Set<string>();
  readonly #changedTableIds = new Set<string>();
  #logicallyUnchanged = false;
  /**
   * Whether the record exists in the store. A deferred transaction (`beginDeferred`) starts
   * without one: it is written on the first two-step staging call, or folded into the
   * single-shot `stageArtifactsAndCommit`, or never — an abort of an unpersisted transaction
   * touches nothing.
   */
  #persisted: boolean;

  constructor(
    private readonly store: BlockStore,
    record: TransactionRecord,
    private readonly now: () => Date,
    options: { persisted?: boolean } = {},
  ) {
    this.#record = structuredClone(record);
    this.#persisted = options.persisted ?? true;
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

  /**
   * Counts every registration this transaction carries: staged blocks and segments, unique-key
   * entries, full-text entries, and supersessions. A caller that fails part-way through a
   * multi-step stage compares this before and after to tell "nothing was registered" (the
   * statement is cleanly undone) from "partial work landed" (the transaction can only abort),
   * since there is no statement-level rollback inside a transaction.
   */
  get stagedWorkCount(): number {
    return (
      this.#record.pendingBlockIds.length +
      this.#record.pendingSegmentIds.length +
      this.#uniqueKeyChanges.length +
      this.#ftsChanges.length +
      this.#supersededBlockIds.size
    );
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
    await this.#ensurePersisted();
    await this.store.addBlocks(blocks);
    this.#record = await this.store.updateTransaction(this.id, this.#record.revision, {
      pendingBlockIds: [...this.#record.pendingBlockIds, ...blocks.map((block) => block.id)],
      updatedAt: this.now().toISOString(),
    });
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
    // The journal stores pendingSegmentIds deduped and sorted, so staging position is the
    // only record of operation order within this transaction. Stamp it on the segments:
    // reads use it to fold same-commit segments in the order they were staged.
    const ordinalBase = this.#record.pendingSegmentIds.length;
    const ordered = segments.map((segment, index) => ({
      ...segment,
      commitOrdinal: segment.commitOrdinal ?? ordinalBase + index,
    }));
    const batched = this.store.stageTransactionArtifacts?.bind(this.store);
    if (batched === undefined) {
      await this.stageBlocks(blocks);
      for (const segment of ordered) await this.stageSegment(segment);
      return;
    }
    if (blocks.length === 0 && ordered.length === 0) return;
    await this.#ensurePersisted();
    this.#record = await batched({
      transactionId: this.id,
      expectedRevision: this.#record.revision,
      blocks,
      segments: ordered,
      updatedAt: this.now().toISOString(),
    });
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
    if (
      blocks.length + segments.length > 0 &&
      blocks.every((block) => pendingBlockIds.has(block.id)) &&
      segments.every((segment) => pendingSegmentIds.has(segment.id))
    ) {
      return this.commit();
    }
    const single = this.store.writeTransaction?.bind(this.store);
    if (single === undefined) {
      await this.stageArtifacts(blocks, segments);
      return this.commit();
    }
    const ordinalBase = this.#record.pendingSegmentIds.length;
    const ordered = segments.map((segment, index) => ({
      ...segment,
      commitOrdinal: segment.commitOrdinal ?? ordinalBase + index,
    }));
    const changedTableIds = new Set(this.#changedTableIds);
    for (const segment of ordered) changedTableIds.add(segment.tableId);
    const { snapshotVersion: _pinned, ...fresh } = this.#record;
    void _pinned;
    const committedAt = this.now().toISOString();
    try {
      const manifest = await single({
        transaction: this.#persisted
          ? { id: this.id, expectedRevision: this.#record.revision }
          : { record: fresh },
        blocks,
        segments: ordered,
        expectedManifestVersion: this.#record.snapshotVersion,
        changedTableIds: this.#logicallyUnchanged ? [] : [...changedTableIds],
        ...(this.#supersededBlockIds.size === 0
          ? {}
          : { removedBlockIds: [...this.#supersededBlockIds] }),
        ...(this.#uniqueKeyChanges.length === 0
          ? {}
          : { uniqueKeyChanges: this.#uniqueKeyChanges }),
        ...(this.#ftsChanges.length === 0 ? {} : { ftsChanges: this.#ftsChanges }),
        committedAt,
      });
      for (const segment of ordered) this.#changedTableIds.add(segment.tableId);
      this.#record = {
        ...this.#record,
        pendingBlockIds: [
          ...new Set([...pendingBlockIds, ...blocks.map((block) => block.id)]),
        ].sort(),
        pendingSegmentIds: [
          ...new Set([...pendingSegmentIds, ...ordered.map((segment) => segment.id)]),
        ].sort(),
        status: "committed",
        committedVersion: manifest.version,
        // One journal step and one commit, as the two-step shape would have left it.
        revision: this.#record.revision + 2,
        updatedAt: committedAt,
      };
      this.#persisted = true;
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
    const pending = new Set(this.#record.pendingBlockIds);
    const additions = [...new Set(blockIds)].filter((id) => !pending.has(id));
    if (additions.length === 0) return;
    if (additions.some((id) => id.length === 0)) {
      throw new TypeError("Block ID cannot be empty");
    }
    for (const id of additions) {
      if ((await this.store.getBlock(id)) === undefined) {
        throw new Error(`Cannot stage a missing existing block: ${id}`);
      }
    }
    await this.#ensurePersisted();
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
    if (record.commitOrdinal === undefined) {
      record = { ...record, commitOrdinal: this.#record.pendingSegmentIds.length };
    }
    this.#changedTableIds.add(record.tableId);
    await this.#ensurePersisted();
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
    this.#changedTableIds.add(record.tableId);
    await this.#ensurePersisted();
    this.#record = await this.store.updateTransaction(this.id, this.#record.revision, {
      pendingSegmentIds: [...this.#record.pendingSegmentIds, segmentId],
      updatedAt: this.now().toISOString(),
    });
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
    this.#uniqueKeyChanges.push({
      tableId: changes.tableId,
      keyTokens: [...new Set(changes.keyTokens)].sort(),
      requireAbsent: changes.requireAbsent,
      ...(changes.remove === undefined ? {} : { remove: changes.remove }),
    });
  }

  /**
   * Attaches one batch's full-text deltas; applied atomically with the publish. A second
   * batch for the same table merges per column — postings re-sorted by term (each batch's
   * reserved row ids are strictly above the last, so rowIds stay ascending within a term)
   * and token totals summed — so a scope can insert into one FTS table any number of times.
   */
  setFtsChanges(changes: FtsChanges): void {
    this.#assertActive();
    const existing = this.#ftsChanges.find((entry) => entry.tableId === changes.tableId);
    if (existing === undefined) {
      this.#ftsChanges.push(changes);
      return;
    }
    const merged = new Map(existing.columns.map((column) => [column.columnId, column] as const));
    for (const column of changes.columns) {
      const present = merged.get(column.columnId);
      if (present === undefined) {
        merged.set(column.columnId, column);
        continue;
      }
      const byTerm = new Map(present.postings.map((posting) => [posting.term, posting] as const));
      for (const posting of column.postings) {
        const held = byTerm.get(posting.term);
        if (held === undefined) {
          byTerm.set(posting.term, posting);
        } else {
          byTerm.set(posting.term, {
            term: posting.term,
            rowIds: [...held.rowIds, ...posting.rowIds],
            tf: [...held.tf, ...posting.tf],
          });
        }
      }
      merged.set(column.columnId, {
        columnId: column.columnId,
        postings: [...byTerm.values()].sort((left, right) =>
          left.term < right.term ? -1 : left.term > right.term ? 1 : 0,
        ),
        totalTokens: present.totalTokens + column.totalTokens,
      });
    }
    this.#ftsChanges[this.#ftsChanges.indexOf(existing)] = {
      tableId: changes.tableId,
      columns: [...merged.values()],
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

  async commit(): Promise<ManifestSummary> {
    this.#assertActive();
    await this.#ensurePersisted();
    // The store derives the published manifest from its stored base plus this delta, so the
    // commit neither loads nor rebuilds the full block list.
    const committedAt = this.now().toISOString();
    try {
      const manifest = await this.store.commitTransaction({
        transactionId: this.id,
        expectedTransactionRevision: this.#record.revision,
        expectedManifestVersion: this.#record.snapshotVersion,
        changedTableIds: this.#logicallyUnchanged ? [] : [...this.#changedTableIds],
        ...(this.#supersededBlockIds.size === 0
          ? {}
          : { removedBlockIds: [...this.#supersededBlockIds] }),
        ...(this.#uniqueKeyChanges.length === 0
          ? {}
          : { uniqueKeyChanges: this.#uniqueKeyChanges }),
        ...(this.#ftsChanges.length === 0 ? {} : { ftsChanges: this.#ftsChanges }),
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
      return this.#recoverCommitted(error);
    }
  }

  async rebase(): Promise<Snapshot> {
    this.#assertActive();
    for (;;) {
      const current = await this.store.getCurrentManifest();
      if (!this.#persisted) {
        // Nothing is in the store yet, so the pin moves locally; it lands with the record.
        this.#record = {
          ...this.#record,
          snapshotVersion: current?.version ?? null,
          updatedAt: this.now().toISOString(),
        };
        return new Snapshot(this.store, current?.version ?? null, current?.blockIds ?? []);
      }
      try {
        this.#record = await this.store.updateTransaction(this.id, this.#record.revision, {
          snapshotVersion: current?.version ?? null,
          updatedAt: this.now().toISOString(),
        });
        return new Snapshot(this.store, current?.version ?? null, current?.blockIds ?? []);
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
      this.#record = { ...this.#record, status: "aborted", updatedAt: this.now().toISOString() };
      return;
    }
    this.#record = await this.store.updateTransaction(this.id, this.#record.revision, {
      status: "aborted",
      updatedAt: this.now().toISOString(),
    });
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
  }

  /** A commit whose acknowledgement was lost still committed: answer from the persisted record. */
  async #recoverCommitted(error: unknown): Promise<ManifestSummary> {
    const persisted = await this.store.getTransaction(this.id);
    if (persisted?.status === "committed" && persisted.committedVersion !== null) {
      const manifest = await this.store.getManifest(persisted.committedVersion);
      if (manifest !== undefined) {
        this.#record = persisted;
        this.#persisted = true;
        return manifest;
      }
    }
    throw error;
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
    return (await this.beginWithReservation()).transaction;
  }

  /**
   * Begins a transaction without a storage write: the record is pinned at the current manifest
   * version (one read) and written only when something is staged through the two-step methods
   * — or never, when the whole write goes through `stageArtifactsAndCommit` on a store with
   * the single-shot `writeTransaction`, or the transaction aborts untouched. For writes whose
   * artifacts need no prior reservation; `beginWithReservation` is the shape for the rest.
   */
  async beginDeferred(): Promise<DatabaseTransaction> {
    const currentVersion = await this.store.getCurrentManifestVersion();
    const timestamp = this.#now().toISOString();
    return new DatabaseTransaction(
      this.store,
      {
        id: this.#createId(),
        snapshotVersion: currentVersion,
        pendingBlockIds: [],
        pendingSegmentIds: [],
        status: "active",
        revision: 0,
        startedAt: timestamp,
        updatedAt: timestamp,
        committedVersion: null,
      },
      this.#now,
      { persisted: false },
    );
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
    const batched = this.store.beginTransaction?.bind(this.store);
    if (batched !== undefined) {
      const timestamp = this.#now().toISOString();
      const begun = await batched({
        record: {
          id,
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
      return {
        transaction: new DatabaseTransaction(this.store, begun.record, this.#now),
        ...(begun.rowIds === undefined ? {} : { rowIds: begun.rowIds }),
        ...(begun.autoIncrementValues === undefined
          ? {}
          : { autoIncrementValues: begun.autoIncrementValues }),
      };
    }
    for (;;) {
      const currentVersion = await this.store.getCurrentManifestVersion();
      const timestamp = this.#now().toISOString();
      const record: TransactionRecord = {
        id,
        snapshotVersion: currentVersion,
        pendingBlockIds: [],
        pendingSegmentIds: [],
        status: "active",
        revision: 0,
        startedAt: timestamp,
        updatedAt: timestamp,
        committedVersion: null,
      };
      try {
        await this.store.createTransaction(record);
        const transaction = new DatabaseTransaction(this.store, record, this.#now);
        const autoIncrementValues =
          reserveAutoIncrement === undefined
            ? undefined
            : await this.store.reserveAutoIncrement(
                reserveAutoIncrement.tableId,
                reserveAutoIncrement.columnId,
                reserveAutoIncrement.count,
                reserveAutoIncrement.atLeast,
              );
        const rowIds =
          reserveRowIds === undefined
            ? undefined
            : await this.store.reserveRowIds(reserveRowIds.tableId, reserveRowIds.count);
        return {
          transaction,
          ...(rowIds === undefined ? {} : { rowIds }),
          ...(autoIncrementValues === undefined ? {} : { autoIncrementValues }),
        };
      } catch (error) {
        if (error instanceof SnapshotManifestMissingError) {
          continue;
        }
        throw error;
      }
    }
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
    if (options.id?.trim().length === 0) {
      throw new TypeError("Lease ID cannot be empty");
    }
    const id = options.id ?? this.#createId();
    for (;;) {
      const snapshot = await this.openSnapshot(options.version);
      const record: LeaseRecord = {
        id,
        kind: options.kind ?? "reader",
        manifestVersion: snapshot.version,
        ownerId: options.ownerId,
        expiresAt: new Date(this.#now().getTime() + options.ttlMs).toISOString(),
        revision: 0,
      };
      try {
        await this.store.createLease(record);
        return new LeasedSnapshot(
          this.store,
          snapshot.version,
          snapshot.listBlockIds(),
          record,
          this.#now,
        );
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
      if (!(error instanceof LeaseConflictError)) throw error;
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
    const timestamp = at.getTime();
    if (!Number.isFinite(timestamp)) throw new TypeError("Lease cutoff must be a valid date");
    const removed: string[] = [];
    for (const lease of await this.store.listLeases()) {
      if (Date.parse(lease.expiresAt) > timestamp) continue;
      try {
        if (await this.store.removeLeaseIfExpired(lease.id, lease.revision, at.toISOString())) {
          removed.push(lease.id);
        }
      } catch (error) {
        if (error instanceof LeaseConflictError) continue;
        throw error;
      }
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
    let transactionCursor: string | null = null;
    do {
      const page = await this.store.listTransactionPage(transactionCursor, 64);
      for (const record of page.records) {
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
      const timestamp = this.#now().toISOString();
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
            updatedAt: this.#now().toISOString(),
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
  if (!Number.isFinite(ttlMs) || ttlMs <= 0) {
    throw new RangeError("Lease lifetime must be a positive number of milliseconds");
  }
}

async function loadSnapshot(store: BlockStore, version: number | null): Promise<Snapshot> {
  if (version === null) return new Snapshot(store, null, []);
  const manifest = await store.getManifest(version);
  if (manifest === undefined || manifest.prunedAt !== undefined) {
    throw new SnapshotManifestMissingError(version);
  }
  return new Snapshot(store, manifest.version, manifest.blockIds);
}
