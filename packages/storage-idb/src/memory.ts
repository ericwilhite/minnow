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
  type TableRecord,
  type TransactionRecord,
  TransactionRecordConflictError,
  type TransactionRecordUpdate,
  UniqueKeyConflictError,
  normalizeCompactionJobRecord,
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
    if (this.#segments.has(record.id)) throw new Error(`Segment already exists: ${record.id}`);
    this.#segments.set(record.id, structuredClone(record));
  }

  async getSegment(id: string): Promise<SegmentRecord | undefined> {
    const record = this.#segments.get(id);
    return record === undefined ? undefined : structuredClone(record);
  }

  async listSegments(tableId?: string): Promise<SegmentRecord[]> {
    return [...this.#segments.values()]
      .filter((record) => tableId === undefined || record.tableId === tableId)
      .sort((left, right) => left.id.localeCompare(right.id))
      .map((record) => structuredClone(record));
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

  async listTransactions(): Promise<TransactionRecord[]> {
    return [...this.#transactions.values()]
      .sort(
        (left, right) =>
          left.startedAt.localeCompare(right.startedAt) || left.id.localeCompare(right.id),
      )
      .map((record) => structuredClone(record));
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

      const pinnedManifestVersions = collectPinnedManifestVersions(
        this.#currentVersion,
        this.#transactions.values(),
        this.#leases.values(),
        this.#compactionJobs.values(),
        Date.parse(current.leaseCutoff),
      );
      assertPinnedManifestsAvailable(pinnedManifestVersions, this.#manifests, this.#blocks);
      let manifestIndex = current.cursor.manifestIndex;
      while (remaining > 0 && manifestIndex < current.candidateManifestVersions.length) {
        const version = current.candidateManifestVersions[manifestIndex];
        if (version === undefined) throw new Error("Garbage collection manifest cursor is invalid");
        const manifest = this.#manifests.get(version);
        if (manifest === undefined) missingManifestVersions.push(version);
        else if (manifest.prunedAt !== undefined) alreadyPrunedManifestVersions.push(version);
        else if (pinnedManifestVersions.has(version)) retainedManifestVersions.push(version);
        else prunedManifestVersions.push(version);
        manifestIndex += 1;
        remaining -= 1;
      }

      const prunedManifestVersionSet = new Set(prunedManifestVersions);
      const remainingManifests = [...this.#manifests.values()].filter(
        (manifest) =>
          manifest.prunedAt === undefined && !prunedManifestVersionSet.has(manifest.version),
      );
      const roots = collectPhysicalRoots(
        remainingManifests,
        this.#segments.values(),
        this.#transactions.values(),
        this.#compactionJobs.values(),
      );
      assertAllManifestBlocksAvailable(remainingManifests, this.#blocks);
      let segmentIndex = current.cursor.segmentIndex;
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

function assertPinnedManifestsAvailable(
  versions: ReadonlySet<number>,
  manifests: ReadonlyMap<number, Manifest>,
  blocks: ReadonlyMap<string, Uint8Array>,
): void {
  for (const version of versions) assertSnapshotAvailable(version, manifests, blocks);
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

function assertAllManifestBlocksAvailable(
  manifests: Iterable<Manifest>,
  blocks: ReadonlyMap<string, Uint8Array>,
): void {
  for (const manifest of manifests) {
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
  const provenBlockIds = new Set<string>();
  const provenSegmentIds = new Set<string>();
  for (const version of job.candidateManifestVersions) {
    const manifest = manifests.get(version);
    if (manifest === undefined) {
      throw new Error(`Garbage collection candidate manifest is missing: ${String(version)}`);
    }
    manifest.blockIds.forEach((id) => provenBlockIds.add(id));
  }
  for (const transaction of transactions.values()) {
    if (transaction.status !== "aborted") continue;
    transaction.pendingBlockIds.forEach((id) => provenBlockIds.add(id));
    transaction.pendingSegmentIds.forEach((id) => provenSegmentIds.add(id));
  }
  for (const compaction of compactionJobs.values()) {
    if (!isTerminalCompactionJob(compaction)) continue;
    compaction.sourceBlockIds.forEach((id) => provenBlockIds.add(id));
    compaction.outputBlockIds.forEach((id) => provenBlockIds.add(id));
    compaction.sourceSegmentIds.forEach((id) => provenSegmentIds.add(id));
    if (compaction.outputSegmentId !== null) provenSegmentIds.add(compaction.outputSegmentId);
  }
  for (const segment of segments.values()) {
    const blockIds = segmentBlockIds(segment);
    if (blockIds.length > 0 && blockIds.every((id) => provenBlockIds.has(id))) {
      provenSegmentIds.add(segment.id);
    }
  }
  const unprovenBlockId = job.candidateBlockIds.find((id) => !provenBlockIds.has(id));
  if (unprovenBlockId !== undefined) {
    throw new Error(
      `Garbage collection block candidate has no persisted provenance: ${unprovenBlockId}`,
    );
  }
  const unprovenSegmentId = job.candidateSegmentIds.find((id) => !provenSegmentIds.has(id));
  if (unprovenSegmentId !== undefined) {
    throw new Error(
      `Garbage collection segment candidate has no persisted provenance: ${unprovenSegmentId}`,
    );
  }
}

function collectPinnedManifestVersions(
  currentVersion: number | null,
  transactions: Iterable<TransactionRecord>,
  leases: Iterable<LeaseRecord>,
  compactionJobs: Iterable<CompactionJobRecord>,
  leaseCutoff: number,
): Set<number> {
  const transactionRecords = [...transactions];
  const transactionsById = new Map(
    transactionRecords.map((transaction) => [transaction.id, transaction]),
  );
  const versions = new Set<number>();
  if (currentVersion !== null) versions.add(currentVersion);
  for (const transaction of transactionRecords) {
    if (transaction.status === "active" && transaction.snapshotVersion !== null) {
      versions.add(transaction.snapshotVersion);
    }
  }
  for (const lease of leases) {
    const expiresAt = Date.parse(lease.expiresAt);
    if (
      lease.manifestVersion !== null &&
      (!Number.isFinite(expiresAt) || expiresAt > leaseCutoff)
    ) {
      versions.add(lease.manifestVersion);
    }
  }
  for (const job of compactionJobs) {
    if (isTerminalCompactionJob(job)) continue;
    versions.add(job.sourceManifestVersion);
    const linkedTransaction =
      job.transactionId === null ? undefined : transactionsById.get(job.transactionId);
    if (linkedTransaction?.status === "committed") {
      if (linkedTransaction.committedVersion === null) {
        throw new Error(`Committed transaction has no manifest version: ${linkedTransaction.id}`);
      }
      versions.add(linkedTransaction.committedVersion);
    }
  }
  return versions;
}

function collectPhysicalRoots(
  manifests: Iterable<Manifest>,
  segments: Iterable<SegmentRecord>,
  transactions: Iterable<TransactionRecord>,
  compactionJobs: Iterable<CompactionJobRecord>,
): { blockIds: Set<string>; segmentIds: Set<string> } {
  const blockIds = new Set<string>();
  const segmentIds = new Set<string>();
  const segmentRecords = [...segments];
  const activeTransactionIds = new Set<string>();
  for (const manifest of manifests) manifest.blockIds.forEach((id) => blockIds.add(id));
  for (const transaction of transactions) {
    if (transaction.status !== "active") continue;
    activeTransactionIds.add(transaction.id);
    transaction.pendingBlockIds.forEach((id) => blockIds.add(id));
    transaction.pendingSegmentIds.forEach((id) => segmentIds.add(id));
  }
  for (const job of compactionJobs) {
    if (isTerminalCompactionJob(job)) continue;
    job.sourceBlockIds.forEach((id) => blockIds.add(id));
    job.outputBlockIds.forEach((id) => blockIds.add(id));
    job.sourceSegmentIds.forEach((id) => segmentIds.add(id));
    if (job.outputSegmentId !== null) segmentIds.add(job.outputSegmentId);
  }
  for (const segment of segmentRecords) {
    const ids = segmentBlockIds(segment);
    if (
      (ids.length > 0 && ids.every((id) => blockIds.has(id))) ||
      segmentIds.has(segment.id) ||
      activeTransactionIds.has(segment.transactionId)
    ) {
      segmentIds.add(segment.id);
      ids.forEach((id) => blockIds.add(id));
    }
  }
  return { blockIds, segmentIds };
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
