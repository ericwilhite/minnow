import {
  type CommitTransactionInput,
  type CompactionJobRecord,
  CompactionJobConflictError,
  type CompactionJobRecordUpdate,
  createManifest,
  type BlockStore,
  type BlockWrite,
  type LeaseRecord,
  LeaseConflictError,
  type Manifest,
  type PublishManifestInput,
  type RowIdRange,
  type SegmentRecord,
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
    if (this.#transactions.has(record.id))
      throw new Error(`Transaction already exists: ${record.id}`);
    this.#transactions.set(record.id, structuredClone(record));
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
    const current = this.#transactions.get(id);
    if (current?.revision !== expectedRevision) {
      throw new TransactionRecordConflictError(id, expectedRevision, current?.revision ?? null);
    }
    assertGenericTransactionUpdateAllowed(current, update);
    const updated = updateTransactionRecord(current, update);
    this.#transactions.set(id, updated);
    return structuredClone(updated);
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
    if (this.#leases.has(record.id)) throw new Error(`Lease already exists: ${record.id}`);
    this.#leases.set(record.id, structuredClone(record));
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
    const record = this.#leases.get(id);
    if (record?.revision !== expectedRevision) {
      throw new LeaseConflictError(id, expectedRevision, record?.revision ?? null);
    }
    const renewed = { ...record, expiresAt, revision: record.revision + 1 };
    this.#leases.set(id, renewed);
    return structuredClone(renewed);
  }

  async removeLease(id: string): Promise<void> {
    this.#leases.delete(id);
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

  close(): void {
    // The in-memory implementation owns no external resources.
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
