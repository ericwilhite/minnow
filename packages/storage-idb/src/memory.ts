import {
  type CommitTransactionInput,
  type CompactionJobRecord,
  CompactionJobConflictError,
  type CompactionJobRecordUpdate,
  createManifest,
  type CreateGarbageCollectionJobInput,
  createGarbageCollectionJobRecord,
  type BlockStore,
  type BlockWrite,
  advanceGarbageCollectionJobRecord,
  type GarbageCollectionJobRecord,
  GarbageCollectionJobConflictError,
  type GarbageCollectionStepResult,
  type LeaseRecord,
  LeaseConflictError,
  type Manifest,
  type PublishManifestInput,
  type RowIdRange,
  type RunGarbageCollectionStepInput,
  type SegmentRecord,
  SnapshotManifestMissingError,
  type StoragePage,
  type TableRecord,
  type TempOwnerRecord,
  TempOwnerConflictError,
  type TempRunPage,
  type TransactionRecord,
  TransactionRecordConflictError,
  type TransactionRecordUpdate,
  UniqueKeyConflictError,
  normalizeCompactionJobRecord,
  normalizeSegmentRecord,
  updateCompactionJobRecord,
  updateTransactionRecord,
  WriteConflictError,
} from "./types.js";

export class MemoryBlockStore implements BlockStore {
  readonly #blocks = new Map<string, Uint8Array>();
  readonly #manifests = new Map<number, Manifest>();
  readonly #transactions = new Map<string, TransactionRecord>();
  readonly #tables = new Map<string, TableRecord>();
  readonly #tableIdsByName = new Map<string, string>();
  readonly #segments = new Map<string, SegmentRecord>();
  readonly #leases = new Map<string, LeaseRecord>();
  readonly #compactionJobs = new Map<string, CompactionJobRecord>();
  readonly #garbageCollectionJobs = new Map<string, GarbageCollectionJobRecord>();
  readonly #nextRowIds = new Map<string, bigint>();
  readonly #uniqueKeys = new Map<string, Set<string>>();
  readonly #tempRunPages = new Map<string, Uint8Array>();
  readonly #tempOwners = new Map<string, TempOwnerRecord>();
  #currentVersion: number | null = null;
  #commitQueue = Promise.resolve();

  async addBlock(id: string, bytes: Uint8Array): Promise<void> {
    return this.addBlocks([{ id, bytes }]);
  }

  async addBlocks(blocks: readonly BlockWrite[]): Promise<void> {
    const ids = new Set<string>();
    for (const block of blocks) {
      validateId(block.id);
      if (ids.has(block.id) || this.#blocks.has(block.id)) {
        throw new Error(`Block already exists: ${block.id}`);
      }
      ids.add(block.id);
    }
    for (const block of blocks) this.#blocks.set(block.id, new Uint8Array(block.bytes));
  }

  async getBlock(id: string): Promise<Uint8Array | undefined> {
    const bytes = this.#blocks.get(id);
    return bytes === undefined ? undefined : new Uint8Array(bytes);
  }

  async getBlocks(ids: readonly string[]): Promise<Array<Uint8Array | undefined>> {
    return ids.map((id) => {
      const bytes = this.#blocks.get(id);
      return bytes === undefined ? undefined : new Uint8Array(bytes);
    });
  }

  async removeBlock(id: string): Promise<void> {
    this.#blocks.delete(id);
  }

  async listBlockIds(): Promise<string[]> {
    return [...this.#blocks.keys()].sort();
  }

  async putTempRunPage(page: TempRunPage): Promise<void> {
    validateTempRunPage(page);
    this.#tempRunPages.set(
      tempRunPageKey(page.ownerId, page.runId, page.pageIndex),
      page.bytes.slice(),
    );
  }

  async getTempRunPage(
    ownerId: string,
    runId: string,
    pageIndex: number,
  ): Promise<Uint8Array | undefined> {
    validateTempRunPageIdentity(ownerId, runId, pageIndex);
    return this.#tempRunPages.get(tempRunPageKey(ownerId, runId, pageIndex))?.slice();
  }

  async removeTempRun(ownerId: string, runId: string): Promise<void> {
    validateTempRunPageIdentity(ownerId, runId, 0);
    const prefix = tempRunPagePrefix(ownerId, runId);
    for (const key of this.#tempRunPages.keys())
      if (key.startsWith(prefix)) this.#tempRunPages.delete(key);
  }

  async removeTempOwner(ownerId: string): Promise<void> {
    validateId(ownerId);
    const prefix = `${String(ownerId.length)}:${ownerId}:`;
    for (const key of this.#tempRunPages.keys())
      if (key.startsWith(prefix)) this.#tempRunPages.delete(key);
    this.#tempOwners.delete(ownerId);
  }

  async createTempOwner(record: TempOwnerRecord): Promise<void> {
    validateTempOwnerRecord(record);
    return this.#runAtomic(() => {
      if (this.#tempOwners.has(record.ownerId)) {
        throw new Error(`Temp owner already exists: ${record.ownerId}`);
      }
      this.#tempOwners.set(record.ownerId, structuredClone(record));
    });
  }

  async getTempOwner(ownerId: string): Promise<TempOwnerRecord | undefined> {
    validateId(ownerId);
    const record = this.#tempOwners.get(ownerId);
    return record === undefined ? undefined : structuredClone(record);
  }

  async renewTempOwner(
    ownerId: string,
    expectedRevision: number,
    expiresAt: string,
  ): Promise<TempOwnerRecord> {
    validateLeaseExpiration(expiresAt);
    return this.#runAtomic(() => {
      const record = this.#tempOwners.get(ownerId);
      if (record?.revision !== expectedRevision) {
        throw new TempOwnerConflictError(ownerId, expectedRevision, record?.revision ?? null);
      }
      const renewed = { ...record, expiresAt, revision: record.revision + 1 };
      this.#tempOwners.set(ownerId, renewed);
      return structuredClone(renewed);
    });
  }

  async removeTempOwnerIfExpired(ownerId: string, expiresAtCutoff: string): Promise<boolean> {
    validateId(ownerId);
    const cutoff = Date.parse(expiresAtCutoff);
    if (!Number.isFinite(cutoff)) throw new TypeError("Temp owner expiry cutoff must be valid");
    return this.#runAtomic(() => {
      const record = this.#tempOwners.get(ownerId);
      if (record !== undefined) {
        const expiresAt = Date.parse(record.expiresAt);
        if (Number.isFinite(expiresAt) && expiresAt > cutoff) return false;
      }
      const prefix = `${String(ownerId.length)}:${ownerId}:`;
      for (const key of this.#tempRunPages.keys())
        if (key.startsWith(prefix)) this.#tempRunPages.delete(key);
      this.#tempOwners.delete(ownerId);
      return true;
    });
  }

  async listTempOwnerIdsPage(
    afterOwnerId: string | null,
    limit: number,
  ): Promise<StoragePage<string, string>> {
    validatePageLimit(limit);
    const ownerIds = new Set<string>(this.#tempOwners.keys());
    for (const key of this.#tempRunPages.keys()) {
      const separator = key.indexOf(":");
      const length = Number(key.slice(0, separator));
      ownerIds.add(key.slice(separator + 1, separator + 1 + length));
    }
    const sorted = [...ownerIds]
      .filter((ownerId) => afterOwnerId === null || ownerId > afterOwnerId)
      .sort();
    const records = sorted.slice(0, limit);
    return {
      records,
      nextCursor: sorted.length > limit ? (records[records.length - 1] ?? null) : null,
    };
  }

  async addTable(record: TableRecord): Promise<void> {
    if (this.#tables.has(record.id)) throw new Error(`Table already exists: ${record.id}`);
    if (this.#tableIdsByName.has(record.name))
      throw new Error(`Table name already exists: ${record.name}`);
    this.#tables.set(record.id, structuredClone(record));
    this.#tableIdsByName.set(record.name, record.id);
  }

  async getTable(id: string): Promise<TableRecord | undefined> {
    const record = this.#tables.get(id);
    return record === undefined ? undefined : structuredClone(record);
  }

  async getTableByName(name: string): Promise<TableRecord | undefined> {
    const id = this.#tableIdsByName.get(name);
    return id === undefined ? undefined : this.getTable(id);
  }

  async listTables(): Promise<TableRecord[]> {
    return [...this.#tables.values()]
      .sort((left, right) => left.name.localeCompare(right.name))
      .map((record) => structuredClone(record));
  }

  async addSegment(record: SegmentRecord): Promise<void> {
    const normalized = normalizeSegmentRecord(record);
    if (this.#segments.has(normalized.id)) {
      throw new Error(`Segment already exists: ${normalized.id}`);
    }
    this.#segments.set(normalized.id, normalized);
  }

  async getSegment(id: string): Promise<SegmentRecord | undefined> {
    const record = this.#segments.get(id);
    return record === undefined ? undefined : normalizeSegmentRecord(record);
  }

  async listSegments(tableId?: string): Promise<SegmentRecord[]> {
    return [...this.#segments.values()]
      .filter((record) => tableId === undefined || record.tableId === tableId)
      .sort((left, right) => left.id.localeCompare(right.id))
      .map((record) => normalizeSegmentRecord(record));
  }

  async removeSegment(id: string): Promise<void> {
    this.#segments.delete(id);
  }

  async reserveRowIds(tableId: string, count: number): Promise<RowIdRange> {
    validateCount(count);
    const start = this.#nextRowIds.get(tableId) ?? 1n;
    const endExclusive = start + BigInt(count);
    this.#nextRowIds.set(tableId, endExclusive);
    return { start, endExclusive };
  }

  async getExistingUniqueKeys(tableId: string, keyTokens: readonly string[]): Promise<string[]> {
    const existing = this.#uniqueKeys.get(tableId);
    if (existing === undefined) return [];
    return [...new Set(keyTokens)].filter((token) => existing.has(token)).sort();
  }

  async getCurrentManifest(): Promise<Manifest | undefined> {
    const manifest =
      this.#currentVersion === null ? undefined : this.#manifests.get(this.#currentVersion);
    return manifest === undefined ? undefined : structuredClone(manifest);
  }

  async getManifest(version: number): Promise<Manifest | undefined> {
    const manifest = this.#manifests.get(version);
    return manifest === undefined ? undefined : structuredClone(manifest);
  }

  async listManifests(): Promise<Manifest[]> {
    return [...this.#manifests.values()]
      .sort((left, right) => left.version - right.version)
      .map((manifest) => structuredClone(manifest));
  }

  async listManifestPage(afterVersion: number | null, limit: number) {
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

  async publishManifest(input: PublishManifestInput): Promise<Manifest> {
    let resolveResult: (manifest: Manifest) => void;
    let rejectResult: (reason: unknown) => void;
    const result = new Promise<Manifest>((resolve, reject) => {
      resolveResult = resolve;
      rejectResult = reject;
    });
    this.#commitQueue = this.#commitQueue.then(() => {
      try {
        if (this.#currentVersion !== input.expectedVersion) {
          throw new WriteConflictError(input.expectedVersion, this.#currentVersion);
        }
        for (const id of input.blockIds) {
          if (!this.#blocks.has(id)) throw new Error(`Manifest references missing block: ${id}`);
        }
        const manifest = createManifest(input);
        this.#manifests.set(manifest.version, manifest);
        this.#currentVersion = manifest.version;
        resolveResult(structuredClone(manifest));
      } catch (error) {
        rejectResult(error);
      }
    });
    return result;
  }

  async createTransaction(record: TransactionRecord): Promise<void> {
    return this.#runAtomic(() => {
      if (this.#transactions.has(record.id)) {
        throw new Error(`Transaction already exists: ${record.id}`);
      }
      assertSnapshotAvailable(record.snapshotVersion, this.#manifests, this.#blocks);
      assertPendingArtifactsAvailable(record, this.#blocks, this.#segments);
      this.#transactions.set(record.id, structuredClone(record));
    });
  }

  async getTransaction(id: string): Promise<TransactionRecord | undefined> {
    const record = this.#transactions.get(id);
    return record === undefined ? undefined : structuredClone(record);
  }

  async getTransactions(ids: readonly string[]): Promise<Array<TransactionRecord | undefined>> {
    return ids.map((id) => {
      const record = this.#transactions.get(id);
      return record === undefined ? undefined : structuredClone(record);
    });
  }

  async listTransactions(): Promise<TransactionRecord[]> {
    return [...this.#transactions.values()]
      .sort(
        (left, right) =>
          left.startedAt.localeCompare(right.startedAt) || left.id.localeCompare(right.id),
      )
      .map((record) => structuredClone(record));
  }

  async listTransactionPage(afterId: string | null, limit: number) {
    validatePageLimit(limit);
    const records = [...this.#transactions.values()]
      .filter((record) => afterId === null || record.id > afterId)
      .sort((left, right) => left.id.localeCompare(right.id))
      .slice(0, limit)
      .map((record) => structuredClone(record));
    return { records, nextCursor: records.length === limit ? (records.at(-1)?.id ?? null) : null };
  }

  async updateTransaction(
    id: string,
    expectedRevision: number,
    update: TransactionRecordUpdate,
  ): Promise<TransactionRecord> {
    return this.#runAtomic(() => {
      const current = this.#transactions.get(id);
      if (current?.revision !== expectedRevision) {
        throw new TransactionRecordConflictError(id, expectedRevision, current?.revision ?? null);
      }
      assertGenericTransactionUpdateAllowed(current, update);
      const updated = updateTransactionRecord(current, update);
      if (update.snapshotVersion !== undefined) {
        assertSnapshotAvailable(updated.snapshotVersion, this.#manifests, this.#blocks);
      }
      assertPendingArtifactsAvailable(
        updated,
        this.#blocks,
        this.#segments,
        update.pendingBlockIds !== undefined,
        update.pendingSegmentIds !== undefined,
      );
      this.#transactions.set(id, updated);
      return structuredClone(updated);
    });
  }

  async commitTransaction(input: CommitTransactionInput): Promise<Manifest> {
    let resolveResult: (manifest: Manifest) => void;
    let rejectResult: (reason: unknown) => void;
    const result = new Promise<Manifest>((resolve, reject) => {
      resolveResult = resolve;
      rejectResult = reject;
    });
    this.#commitQueue = this.#commitQueue.then(() => {
      try {
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
        assertBlockSet(input.blockIds, [
          ...baseBlockIds.filter((id) => !removedBlockIdSet.has(id)),
          ...transaction.pendingBlockIds,
        ]);
        for (const id of transaction.pendingBlockIds) {
          if (!this.#blocks.has(id)) throw new Error(`Manifest references missing block: ${id}`);
        }
        const pendingSegments = transaction.pendingSegmentIds.map((id) => {
          const segment = this.#segments.get(id);
          if (segment === undefined)
            throw new Error(`Transaction references missing segment: ${id}`);
          if (segment.transactionId !== transaction.id) {
            throw new Error(`Segment ${id} belongs to another transaction`);
          }
          return segment;
        });
        const uniqueKeyChanges = input.uniqueKeyChanges;
        if (uniqueKeyChanges !== undefined) {
          const existing = this.#uniqueKeys.get(uniqueKeyChanges.tableId) ?? new Set<string>();
          if (uniqueKeyChanges.requireAbsent) {
            const conflict = uniqueKeyChanges.keyTokens.find((token) => existing.has(token));
            if (conflict !== undefined) {
              throw new UniqueKeyConflictError(uniqueKeyChanges.tableId, conflict);
            }
          }
        }
        const manifest = createManifest({
          expectedVersion: input.expectedManifestVersion,
          blockIds: input.blockIds,
          createdAt: input.committedAt,
        });
        const committed = updateTransactionRecord(transaction, {
          status: "committed",
          committedVersion: manifest.version,
          updatedAt: input.committedAt,
        });
        this.#manifests.set(manifest.version, manifest);
        this.#currentVersion = manifest.version;
        this.#transactions.set(transaction.id, committed);
        for (const segment of pendingSegments) {
          this.#segments.set(segment.id, {
            ...segment,
            level: segment.level ?? 0,
            logicalOrder: segment.logicalOrder ?? manifest.version,
          });
        }
        if (uniqueKeyChanges !== undefined) {
          const existing = this.#uniqueKeys.get(uniqueKeyChanges.tableId) ?? new Set<string>();
          uniqueKeyChanges.keyTokens.forEach((token) => {
            if (uniqueKeyChanges.remove === true) existing.delete(token);
            else existing.add(token);
          });
          this.#uniqueKeys.set(uniqueKeyChanges.tableId, existing);
        }
        resolveResult(structuredClone(manifest));
      } catch (error) {
        rejectResult(error);
      }
    });
    return result;
  }

  async createLease(record: LeaseRecord): Promise<void> {
    validateLeaseExpiration(record.expiresAt);
    return this.#runAtomic(() => {
      if (this.#leases.has(record.id)) throw new Error(`Lease already exists: ${record.id}`);
      assertSnapshotAvailable(record.manifestVersion, this.#manifests, this.#blocks);
      this.#leases.set(record.id, structuredClone(record));
    });
  }

  async getLease(id: string): Promise<LeaseRecord | undefined> {
    const record = this.#leases.get(id);
    return record === undefined ? undefined : structuredClone(record);
  }

  async listLeases(): Promise<LeaseRecord[]> {
    return [...this.#leases.values()]
      .sort((left, right) => left.id.localeCompare(right.id))
      .map((record) => structuredClone(record));
  }

  async renewLease(id: string, expectedRevision: number, expiresAt: string): Promise<LeaseRecord> {
    validateLeaseExpiration(expiresAt);
    return this.#runAtomic(() => {
      const record = this.#leases.get(id);
      if (record?.revision !== expectedRevision) {
        throw new LeaseConflictError(id, expectedRevision, record?.revision ?? null);
      }
      assertSnapshotAvailable(record.manifestVersion, this.#manifests, this.#blocks);
      const renewed = { ...record, expiresAt, revision: record.revision + 1 };
      this.#leases.set(id, renewed);
      return structuredClone(renewed);
    });
  }

  async removeLeaseIfExpired(
    id: string,
    expectedRevision: number,
    expiresAtCutoff: string,
  ): Promise<boolean> {
    const cutoff = Date.parse(expiresAtCutoff);
    if (!Number.isFinite(cutoff)) throw new TypeError("Lease expiry cutoff must be valid");
    return this.#runAtomic(() => {
      const record = this.#leases.get(id);
      if (record?.revision !== expectedRevision) {
        throw new LeaseConflictError(id, expectedRevision, record?.revision ?? null);
      }
      const expiresAt = Date.parse(record.expiresAt);
      if (!Number.isFinite(expiresAt) || expiresAt > cutoff) return false;
      this.#leases.delete(id);
      return true;
    });
  }

  async removeLease(id: string): Promise<void> {
    return this.#runAtomic(() => {
      this.#leases.delete(id);
    });
  }

  async createCompactionJob(record: CompactionJobRecord): Promise<void> {
    const normalized = normalizeCompactionJobRecord(record);
    if (this.#compactionJobs.has(normalized.id)) {
      throw new Error(`Compaction job already exists: ${normalized.id}`);
    }
    this.#compactionJobs.set(normalized.id, normalized);
  }

  async getCompactionJob(id: string): Promise<CompactionJobRecord | undefined> {
    const record = this.#compactionJobs.get(id);
    return record === undefined ? undefined : structuredClone(record);
  }

  async listCompactionJobs(tableId?: string): Promise<CompactionJobRecord[]> {
    return [...this.#compactionJobs.values()]
      .filter((record) => tableId === undefined || record.tableId === tableId)
      .sort(
        (left, right) =>
          left.createdAt.localeCompare(right.createdAt) || left.id.localeCompare(right.id),
      )
      .map((record) => structuredClone(record));
  }

  async listCompactionJobPage(afterId: string | null, limit: number) {
    validatePageLimit(limit);
    const records = [...this.#compactionJobs.values()]
      .filter((record) => afterId === null || record.id > afterId)
      .sort((left, right) => left.id.localeCompare(right.id))
      .slice(0, limit)
      .map((record) => structuredClone(record));
    return { records, nextCursor: records.length === limit ? (records.at(-1)?.id ?? null) : null };
  }

  async updateCompactionJob(
    id: string,
    expectedRevision: number,
    update: CompactionJobRecordUpdate,
  ): Promise<CompactionJobRecord> {
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

  async cancelCompactionJob(
    id: string,
    expectedRevision: number,
    cancelledAt: string,
  ): Promise<CompactionJobRecord> {
    let resolveResult: (record: CompactionJobRecord) => void;
    let rejectResult: (reason: unknown) => void;
    const result = new Promise<CompactionJobRecord>((resolve, reject) => {
      resolveResult = resolve;
      rejectResult = reject;
    });
    this.#commitQueue = this.#commitQueue.then(() => {
      try {
        const current = this.#compactionJobs.get(id);
        if (current?.revision !== expectedRevision) {
          throw new CompactionJobConflictError(id, expectedRevision, current?.revision ?? null);
        }
        if (isTerminalCompactionJob(current)) {
          resolveResult(structuredClone(current));
          return;
        }

        const transaction =
          current.transactionId === null
            ? undefined
            : this.#transactions.get(current.transactionId);
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
          resolveResult(structuredClone(published));
          return;
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
        resolveResult(structuredClone(cancelled));
      } catch (error) {
        rejectResult(error);
      }
    });
    return result;
  }

  async removeCompactionJob(id: string): Promise<void> {
    this.#compactionJobs.delete(id);
  }

  async createGarbageCollectionJob(
    input: CreateGarbageCollectionJobInput,
  ): Promise<GarbageCollectionJobRecord> {
    const record = createGarbageCollectionJobRecord(input);
    return this.#runAtomic(() => {
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
    });
  }

  async getGarbageCollectionJob(id: string): Promise<GarbageCollectionJobRecord | undefined> {
    const record = this.#garbageCollectionJobs.get(id);
    return record === undefined ? undefined : structuredClone(record);
  }

  async listGarbageCollectionJobs(): Promise<GarbageCollectionJobRecord[]> {
    return [...this.#garbageCollectionJobs.values()]
      .sort(
        (left, right) =>
          left.createdAt.localeCompare(right.createdAt) || left.id.localeCompare(right.id),
      )
      .map((record) => structuredClone(record));
  }

  async runGarbageCollectionStep(
    input: RunGarbageCollectionStepInput,
  ): Promise<GarbageCollectionStepResult> {
    validateGarbageCollectionStepInput(input);
    return this.#runAtomic(() => {
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
        this.#blocks,
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
        this.#blocks,
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
        const bytes = this.#blocks.get(id);
        if (bytes === undefined) missingBlockIds.push(id);
        else if (roots.blockIds.has(id)) retainedBlockIds.push(id);
        else {
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
        updatedAt: input.updatedAt,
      });
      prunedManifestVersions.forEach((version) => {
        const manifest = this.#manifests.get(version);
        if (manifest !== undefined)
          this.#manifests.set(version, { ...manifest, prunedAt: input.updatedAt });
      });
      reclaimedSegmentIds.forEach((id) => this.#segments.delete(id));
      reclaimedBlockIds.forEach((id) => this.#blocks.delete(id));
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
    });
  }

  async removeGarbageCollectionJob(id: string): Promise<void> {
    return this.#runAtomic(() => {
      this.#garbageCollectionJobs.delete(id);
    });
  }

  close(): void {
    // The in-memory implementation owns no external resources.
  }

  #runAtomic<T>(operation: () => T): Promise<T> {
    let resolveResult: (value: T | PromiseLike<T>) => void;
    let rejectResult: (reason: unknown) => void;
    const result = new Promise<T>((resolve, reject) => {
      resolveResult = resolve;
      rejectResult = reject;
    });
    this.#commitQueue = this.#commitQueue.then(() => {
      try {
        resolveResult(operation());
      } catch (error) {
        rejectResult(error);
      }
    });
    return result;
  }
}

function validateId(id: string): void {
  if (id.length === 0) throw new TypeError("Block ID cannot be empty");
}

function validateCount(count: number): void {
  if (!Number.isSafeInteger(count) || count <= 0) {
    throw new RangeError("Row ID reservation count must be a positive whole number");
  }
}

function assertBlockSet(actual: readonly string[], expected: readonly string[]): void {
  const actualSet = new Set(actual);
  const expectedSet = new Set(expected);
  if (
    actualSet.size !== expectedSet.size ||
    [...expectedSet].some((blockId) => !actualSet.has(blockId))
  ) {
    throw new Error("Transaction manifest does not match its snapshot and pending blocks");
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
  blocks: ReadonlyMap<string, Uint8Array>,
): void {
  if (version === null) return;
  const manifest = manifests.get(version);
  if (
    manifest === undefined ||
    manifest.prunedAt !== undefined ||
    manifest.blockIds.some((id) => !blocks.has(id))
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
  blocks: ReadonlyMap<string, Uint8Array>,
): void {
  if (currentVersion !== null) assertSnapshotAvailable(currentVersion, manifests, blocks);
  for (const transaction of transactions.values()) {
    if (transaction.status === "active") {
      assertSnapshotAvailable(transaction.snapshotVersion, manifests, blocks);
    }
  }
  for (const lease of leases) {
    const expiresAt = Date.parse(lease.expiresAt);
    if (!Number.isFinite(expiresAt) || expiresAt > leaseCutoff) {
      assertSnapshotAvailable(lease.manifestVersion, manifests, blocks);
    }
  }
  for (const job of compactionJobs) {
    if (isTerminalCompactionJob(job)) continue;
    assertSnapshotAvailable(job.sourceManifestVersion, manifests, blocks);
    const linkedTransaction =
      job.transactionId === null ? undefined : transactions.get(job.transactionId);
    if (linkedTransaction?.status === "committed") {
      if (linkedTransaction.committedVersion === null) {
        throw new Error(`Committed transaction has no manifest version: ${linkedTransaction.id}`);
      }
      assertSnapshotAvailable(linkedTransaction.committedVersion, manifests, blocks);
    }
  }
}

function assertPendingArtifactsAvailable(
  transaction: TransactionRecord,
  blocks: ReadonlyMap<string, Uint8Array>,
  segments: ReadonlyMap<string, SegmentRecord>,
  validateBlocks = true,
  validateSegments = true,
): void {
  const missingBlockId = validateBlocks
    ? transaction.pendingBlockIds.find((id) => !blocks.has(id))
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
  blocks: ReadonlyMap<string, Uint8Array>,
): void {
  for (const manifest of manifests) {
    if (manifest.prunedAt !== undefined || newlyPrunedVersions.has(manifest.version)) continue;
    if (manifest.blockIds.some((id) => !blocks.has(id))) {
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

function validateTempRunPage(page: TempRunPage): void {
  validateTempRunPageIdentity(page.ownerId, page.runId, page.pageIndex);
  if (!(page.bytes instanceof Uint8Array)) throw new TypeError("Temp run page bytes are invalid");
}

function validateTempRunPageIdentity(ownerId: string, runId: string, pageIndex: number): void {
  validateId(ownerId);
  validateId(runId);
  if (!Number.isSafeInteger(pageIndex) || pageIndex < 0) {
    throw new RangeError("Temp run page index must be a non-negative whole number");
  }
}

function validateTempOwnerRecord(record: TempOwnerRecord): void {
  validateId(record.ownerId);
  validateLeaseExpiration(record.expiresAt);
  if (record.revision !== 0) {
    throw new RangeError("Temp owner record must be created at revision zero");
  }
}

function tempRunPageKey(ownerId: string, runId: string, pageIndex: number): string {
  return `${String(ownerId.length)}:${ownerId}:${String(runId.length)}:${runId}:${String(pageIndex)}`;
}

function tempRunPagePrefix(ownerId: string, runId: string): string {
  return `${String(ownerId.length)}:${ownerId}:${String(runId.length)}:${runId}:`;
}

function validateLeaseExpiration(expiresAt: string): void {
  if (expiresAt.length === 0 || !Number.isFinite(Date.parse(expiresAt))) {
    throw new TypeError("Lease expiration must be valid");
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
