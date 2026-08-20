import {
  type BeginTransactionInput,
  type BeginTransactionResult,
  type CommitTransactionInput,
  type CompactionJobRecord,
  type CompactionJobRecordUpdate,
  type CreateGarbageCollectionJobInput,
  type BlockStore,
  type CatalogProbe,
  type TriggerRecord,
  type BlockWrite,
  type FtsCandidates,
  type FtsColumnIndexRecord,
  type FtsPosting,
  type GarbageCollectionJobRecord,
  type GarbageCollectionStepResult,
  type LeaseRecord,
  type Manifest,
  type ManifestSummary,
  type PublishManifestInput,
  type QueryCatalogState,
  type RowIdRange,
  type RunGarbageCollectionStepInput,
  type SegmentRecord,
  type StageTransactionArtifactsInput,
  type StoragePage,
  type TableColumnRecord,
  type TableRecord,
  type TempOwnerRecord,
  type TempRunPage,
  type TransactionRecord,
  type TransactionRecordUpdate,
  type WriteTransactionInput,
} from "./types.js";
import type { DatabaseSnapshot, SnapshotLoadProgress } from "./snapshot.js";
import {
  RecordCore,
  validateId,
  validateTempRunPage,
  validateTempRunPageIdentity,
} from "./toolkit/record-core.js";

/**
 * The in-process store: record semantics live in `RecordCore` (shared with the OPFS store),
 * block and temp-page bytes live in Maps here, and atomicity comes from running every mutating
 * record operation on a promise-chain queue — each queued body is synchronous, so no operation
 * ever observes another mid-mutation.
 */
export class MemoryBlockStore implements BlockStore {
  readonly #blocks = new Map<string, Uint8Array>();
  readonly #tempRunPages = new Map<string, Uint8Array>();
  readonly #core = new RecordCore({
    hasBlock: (id) => this.#blocks.has(id),
    blockByteLength: (id) => this.#blocks.get(id)?.byteLength,
  });
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

  async putTempRunPages(pages: readonly TempRunPage[]): Promise<void> {
    for (const page of pages) validateTempRunPage(page);
    for (const page of pages) {
      this.#tempRunPages.set(
        tempRunPageKey(page.ownerId, page.runId, page.pageIndex),
        page.bytes.slice(),
      );
    }
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
    this.#removeTempOwnerPages(ownerId);
    this.#core.removeTempOwner(ownerId);
  }

  async createTempOwner(record: TempOwnerRecord): Promise<void> {
    return this.#runAtomic(() => {
      this.#core.createTempOwner(record);
    });
  }

  async getTempOwner(ownerId: string): Promise<TempOwnerRecord | undefined> {
    return this.#core.getTempOwner(ownerId);
  }

  async renewTempOwner(
    ownerId: string,
    expectedRevision: number,
    expiresAt: string,
  ): Promise<TempOwnerRecord> {
    return this.#runAtomic(() => this.#core.renewTempOwner(ownerId, expectedRevision, expiresAt));
  }

  async removeTempOwnerIfExpired(ownerId: string, expiresAtCutoff: string): Promise<boolean> {
    return this.#runAtomic(() => {
      const removed = this.#core.removeTempOwnerIfExpired(ownerId, expiresAtCutoff);
      if (removed) this.#removeTempOwnerPages(ownerId);
      return removed;
    });
  }

  async listTempOwnerIdsPage(
    afterOwnerId: string | null,
    limit: number,
  ): Promise<StoragePage<string, string>> {
    const pageOwnerIds: string[] = [];
    for (const key of this.#tempRunPages.keys()) {
      const separator = key.indexOf(":");
      const length = Number(key.slice(0, separator));
      pageOwnerIds.push(key.slice(separator + 1, separator + 1 + length));
    }
    return this.#core.listTempOwnerIdsPage(afterOwnerId, limit, pageOwnerIds);
  }

  #removeTempOwnerPages(ownerId: string): void {
    const prefix = `${String(ownerId.length)}:${ownerId}:`;
    for (const key of this.#tempRunPages.keys())
      if (key.startsWith(prefix)) this.#tempRunPages.delete(key);
  }

  async addTable(record: TableRecord): Promise<void> {
    this.#core.addTable(record);
  }

  async getTable(id: string): Promise<TableRecord | undefined> {
    return this.#core.getTable(id);
  }

  async updateTable(
    id: string,
    expectedRevision: number,
    update: {
      columns?: TableColumnRecord[];
      ftsColumns?: Record<string, FtsColumnIndexRecord> | null;
      triggers?: TriggerRecord[] | null;
    },
  ): Promise<TableRecord> {
    return this.#runAtomic(() => this.#core.updateTable(id, expectedRevision, update));
  }

  async removeTable(id: string, expectedRevision: number): Promise<void> {
    return this.#runAtomic(() => {
      this.#core.removeTable(id, expectedRevision);
    });
  }

  async writeFtsBase(
    tableId: string,
    columnId: string,
    input: { coversVersion: number; chunks: FtsPosting[][]; totalTokens: number },
  ): Promise<void> {
    return this.#runAtomic(() => {
      this.#core.writeFtsBase(tableId, columnId, input);
    });
  }

  async readFtsCandidates(
    tableId: string,
    columnId: string,
    terms: ReadonlyArray<{ term: string; prefix: boolean }>,
    upToVersion: number,
  ): Promise<
    FtsCandidates & { deltaChunkCount: number; totalTokens: number; coversVersion: number }
  > {
    return this.#core.readFtsCandidates(tableId, columnId, terms, upToVersion);
  }

  async getTableByName(name: string): Promise<TableRecord | undefined> {
    const id = this.#core.getTableIdByName(name);
    return id === undefined ? undefined : this.getTable(id);
  }

  async listTables(): Promise<TableRecord[]> {
    return this.#core.listTables();
  }

  async addSegment(record: SegmentRecord): Promise<void> {
    this.#core.addSegment(record);
  }

  async getSegment(id: string): Promise<SegmentRecord | undefined> {
    return this.#core.getSegment(id);
  }

  async listSegments(tableId?: string): Promise<SegmentRecord[]> {
    return this.#core.listSegments(tableId);
  }

  async removeSegment(id: string): Promise<void> {
    this.#core.removeSegment(id);
  }

  async reserveRowIds(tableId: string, count: number): Promise<RowIdRange> {
    return this.#core.reserveRowIds(tableId, count);
  }

  async reserveAutoIncrement(
    tableId: string,
    columnId: string,
    count: number,
    atLeast?: bigint,
  ): Promise<RowIdRange> {
    return this.#runAtomic(() =>
      this.#core.reserveAutoIncrement(tableId, columnId, count, atLeast),
    );
  }

  async getExistingUniqueKeys(tableId: string, keyTokens: readonly string[]): Promise<string[]> {
    return this.#core.getExistingUniqueKeys(tableId, keyTokens);
  }

  async getCurrentManifestVersion(): Promise<number | null> {
    return this.#core.getCurrentManifestVersion();
  }

  async getCatalogProbe(): Promise<CatalogProbe> {
    return this.#core.getCatalogProbe();
  }

  /**
   * Composed over this store's own methods rather than delegated to the core, so a subclass
   * that instruments `getTransactions` or `listSegments` observes these internal reads too —
   * the engine's read-count tests rely on that.
   */
  async getQueryCatalogState(tableNames: readonly string[]): Promise<QueryCatalogState> {
    const tables = await Promise.all(tableNames.map((name) => this.getTableByName(name)));
    const foundTableIds = new Set(
      tables.filter((table): table is TableRecord => table !== undefined).map((table) => table.id),
    );
    const segments = (await this.listSegments()).filter((record) =>
      foundTableIds.has(record.tableId),
    );
    const transactionIds = [...new Set(segments.map((segment) => segment.transactionId))];
    const transactions = (await this.getTransactions(transactionIds)).filter(
      (record): record is TransactionRecord => record !== undefined,
    );
    const probe = this.#core.getCatalogProbe();
    return {
      manifestVersion: probe.manifestVersion,
      tables,
      segments,
      transactions,
      catalogEpoch: probe.catalogEpoch,
    };
  }

  async getCurrentManifest(): Promise<Manifest | undefined> {
    return this.#core.getCurrentManifest();
  }

  async getManifest(version: number): Promise<Manifest | undefined> {
    return this.#core.getManifest(version);
  }

  async listManifests(): Promise<Manifest[]> {
    return this.#core.listManifests();
  }

  async listManifestPage(afterVersion: number | null, limit: number) {
    return this.#core.listManifestPage(afterVersion, limit);
  }

  async publishManifest(input: PublishManifestInput): Promise<Manifest> {
    return this.#runAtomic(() => this.#core.publishManifest(input));
  }

  async beginTransaction(input: BeginTransactionInput): Promise<BeginTransactionResult> {
    return this.#runAtomic(() => this.#core.beginTransaction(input));
  }

  async createTransaction(record: TransactionRecord): Promise<void> {
    return this.#runAtomic(() => {
      this.#core.createTransaction(record);
    });
  }

  async getTransaction(id: string): Promise<TransactionRecord | undefined> {
    return this.#core.getTransaction(id);
  }

  async getTransactions(ids: readonly string[]): Promise<Array<TransactionRecord | undefined>> {
    return this.#core.getTransactions(ids);
  }

  async listTransactions(): Promise<TransactionRecord[]> {
    return this.#core.listTransactions();
  }

  async listTransactionPage(afterId: string | null, limit: number) {
    return this.#core.listTransactionPage(afterId, limit);
  }

  async updateTransaction(
    id: string,
    expectedRevision: number,
    update: TransactionRecordUpdate,
  ): Promise<TransactionRecord> {
    return this.#runAtomic(() => this.#core.updateTransaction(id, expectedRevision, update));
  }

  async stageTransactionArtifacts(
    input: StageTransactionArtifactsInput,
  ): Promise<TransactionRecord> {
    return this.#runAtomic(() => {
      const updated = this.#core.stageTransactionArtifacts(input);
      // The record half validated everything; the bytes land in this same atomic step.
      for (const block of input.blocks) this.#blocks.set(block.id, new Uint8Array(block.bytes));
      return updated;
    });
  }

  async commitTransaction(input: CommitTransactionInput): Promise<ManifestSummary> {
    return this.#runAtomic(() => this.#core.commitTransaction(input));
  }

  async writeTransaction(input: WriteTransactionInput): Promise<ManifestSummary> {
    return this.#runAtomic(() => {
      const summary = this.#core.writeTransaction(input);
      // The record half validated everything; the bytes land in this same atomic step.
      for (const block of input.blocks) this.#blocks.set(block.id, new Uint8Array(block.bytes));
      return summary;
    });
  }

  async createLease(record: LeaseRecord): Promise<void> {
    return this.#runAtomic(() => {
      this.#core.createLease(record);
    });
  }

  async getLease(id: string): Promise<LeaseRecord | undefined> {
    return this.#core.getLease(id);
  }

  async listLeases(): Promise<LeaseRecord[]> {
    return this.#core.listLeases();
  }

  async renewLease(id: string, expectedRevision: number, expiresAt: string): Promise<LeaseRecord> {
    return this.#runAtomic(() => this.#core.renewLease(id, expectedRevision, expiresAt));
  }

  async moveLease(
    id: string,
    expectedRevision: number,
    manifestVersion: number | null,
    expiresAt: string,
  ): Promise<LeaseRecord> {
    return this.#runAtomic(() =>
      this.#core.moveLease(id, expectedRevision, manifestVersion, expiresAt),
    );
  }

  async removeLeaseIfExpired(
    id: string,
    expectedRevision: number,
    expiresAtCutoff: string,
  ): Promise<boolean> {
    return this.#runAtomic(() =>
      this.#core.removeLeaseIfExpired(id, expectedRevision, expiresAtCutoff),
    );
  }

  async removeLease(id: string): Promise<void> {
    return this.#runAtomic(() => {
      this.#core.removeLease(id);
    });
  }

  async createCompactionJob(record: CompactionJobRecord): Promise<void> {
    this.#core.createCompactionJob(record);
  }

  async getCompactionJob(id: string): Promise<CompactionJobRecord | undefined> {
    return this.#core.getCompactionJob(id);
  }

  async listCompactionJobs(tableId?: string): Promise<CompactionJobRecord[]> {
    return this.#core.listCompactionJobs(tableId);
  }

  async listCompactionJobPage(afterId: string | null, limit: number) {
    return this.#core.listCompactionJobPage(afterId, limit);
  }

  async updateCompactionJob(
    id: string,
    expectedRevision: number,
    update: CompactionJobRecordUpdate,
  ): Promise<CompactionJobRecord> {
    return this.#core.updateCompactionJob(id, expectedRevision, update);
  }

  async cancelCompactionJob(
    id: string,
    expectedRevision: number,
    cancelledAt: string,
  ): Promise<CompactionJobRecord> {
    return this.#runAtomic(() => this.#core.cancelCompactionJob(id, expectedRevision, cancelledAt));
  }

  async removeCompactionJob(id: string): Promise<void> {
    this.#core.removeCompactionJob(id);
  }

  async createGarbageCollectionJob(
    input: CreateGarbageCollectionJobInput,
  ): Promise<GarbageCollectionJobRecord> {
    return this.#runAtomic(() => this.#core.createGarbageCollectionJob(input));
  }

  async getGarbageCollectionJob(id: string): Promise<GarbageCollectionJobRecord | undefined> {
    return this.#core.getGarbageCollectionJob(id);
  }

  async listGarbageCollectionJobs(): Promise<GarbageCollectionJobRecord[]> {
    return this.#core.listGarbageCollectionJobs();
  }

  async runGarbageCollectionStep(
    input: RunGarbageCollectionStepInput,
  ): Promise<GarbageCollectionStepResult> {
    return this.#runAtomic(() => {
      const result = this.#core.runGarbageCollectionStep(input);
      result.reclaimedBlockIds.forEach((id) => this.#blocks.delete(id));
      return result;
    });
  }

  async removePrunedManifestRecords(): Promise<number> {
    return this.#runAtomic(() => this.#core.removePrunedManifestRecords().length);
  }

  async removeGarbageCollectionJob(id: string): Promise<void> {
    return this.#runAtomic(() => {
      this.#core.removeGarbageCollectionJob(id);
    });
  }

  /**
   * Copies the current version out as a portable snapshot. Runs on the commit queue, so it sees
   * one consistent version rather than a commit in progress. What the snapshot carries and
   * drops is `RecordCore.exportSnapshot`'s contract.
   */
  exportSnapshot(): Promise<DatabaseSnapshot> {
    return this.#runAtomic(() => this.#core.exportSnapshot((id) => this.#blocks.get(id)));
  }

  /**
   * Loads a snapshot into this store, which must be empty — the same contract as the IndexedDB
   * store, so a caller holding a `BlockStore` can restore into either without asking which it
   * has. The load runs on the commit queue, so it cannot interleave with a commit.
   *
   * Progress is reported for parity with the IndexedDB store rather than because it is needed:
   * an in-memory load is a handful of map writes with no transaction to break it into, so it
   * reports the start and then the end.
   */
  async importSnapshot(
    snapshot: DatabaseSnapshot,
    options: { onProgress?: (progress: SnapshotLoadProgress) => void } = {},
  ): Promise<void> {
    const totalBytes = snapshot.blocks.reduce((total, block) => total + block.bytes.byteLength, 0);
    options.onProgress?.({ phase: "blocks", writtenBytes: 0, totalBytes });
    await this.#runAtomic(() => {
      this.#core.importSnapshot(snapshot, (id, bytes) => this.#blocks.set(id, bytes.slice()));
    });
    options.onProgress?.({ phase: "done", writtenBytes: totalBytes, totalBytes });
  }

  /** Builds a store holding exactly what the snapshot captured. */
  static fromSnapshot(snapshot: DatabaseSnapshot): MemoryBlockStore {
    const store = new MemoryBlockStore();
    store.#core.loadSnapshot(snapshot, (id, bytes) => store.#blocks.set(id, bytes.slice()));
    return store;
  }

  /** Block and spill bytes held; the record maps are negligible beside them. */
  async getLogicalStorageBytes(): Promise<number> {
    let total = 0;
    for (const bytes of this.#blocks.values()) total += bytes.byteLength;
    for (const bytes of this.#tempRunPages.values()) total += bytes.byteLength;
    return total;
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

function tempRunPageKey(ownerId: string, runId: string, pageIndex: number): string {
  return `${String(ownerId.length)}:${ownerId}:${String(runId.length)}:${runId}:${String(pageIndex)}`;
}

function tempRunPagePrefix(ownerId: string, runId: string): string {
  return `${String(ownerId.length)}:${ownerId}:${String(runId.length)}:${runId}:`;
}
