import type {
  BlockStore,
  BlockWrite,
  CompactionJobRecord,
  CompactionJobRecordUpdate,
  CreateGarbageCollectionJobInput,
  GarbageCollectionJobRecord,
  GarbageCollectionStepResult,
  CommitTransactionInput,
  LeaseRecord,
  Manifest,
  PublishManifestInput,
  RowIdRange,
  RunGarbageCollectionStepInput,
  SegmentRecord,
  TableRecord,
  TransactionRecord,
  TransactionRecordUpdate,
} from "@browserdatabase/storage-idb";

export const faultPoints = [
  "beforeBlockWrite",
  "afterBlockWrite",
  "beforeBlockRead",
  "afterBlockRead",
  "beforeManifestCommit",
  "afterManifestCommit",
  "beforeTransactionCommit",
  "afterTransactionCommit",
] as const;

export type FaultPoint = (typeof faultPoints)[number];
export type FaultInjector = (point: FaultPoint) => void | Promise<void>;

export class FaultInjectingBlockStore implements BlockStore {
  constructor(
    private readonly inner: BlockStore,
    private readonly inject: FaultInjector,
  ) {}

  async addBlock(id: string, bytes: Uint8Array): Promise<void> {
    await this.inject("beforeBlockWrite");
    await this.inner.addBlock(id, bytes);
    await this.inject("afterBlockWrite");
  }

  async addBlocks(blocks: readonly BlockWrite[]): Promise<void> {
    await this.inject("beforeBlockWrite");
    await this.inner.addBlocks(blocks);
    await this.inject("afterBlockWrite");
  }

  async getBlock(id: string): Promise<Uint8Array | undefined> {
    await this.inject("beforeBlockRead");
    const bytes = await this.inner.getBlock(id);
    await this.inject("afterBlockRead");
    return bytes;
  }

  async getBlocks(ids: readonly string[]): Promise<Array<Uint8Array | undefined>> {
    await this.inject("beforeBlockRead");
    const blocks = await this.inner.getBlocks(ids);
    await this.inject("afterBlockRead");
    return blocks;
  }

  removeBlock(id: string): Promise<void> {
    return this.inner.removeBlock(id);
  }

  listBlockIds(): Promise<string[]> {
    return this.inner.listBlockIds();
  }

  addTable(record: TableRecord): Promise<void> {
    return this.inner.addTable(record);
  }

  getTable(id: string): Promise<TableRecord | undefined> {
    return this.inner.getTable(id);
  }

  getTableByName(name: string): Promise<TableRecord | undefined> {
    return this.inner.getTableByName(name);
  }

  listTables(): Promise<TableRecord[]> {
    return this.inner.listTables();
  }

  addSegment(record: SegmentRecord): Promise<void> {
    return this.inner.addSegment(record);
  }

  getSegment(id: string): Promise<SegmentRecord | undefined> {
    return this.inner.getSegment(id);
  }

  listSegments(tableId?: string): Promise<SegmentRecord[]> {
    return this.inner.listSegments(tableId);
  }

  removeSegment(id: string): Promise<void> {
    return this.inner.removeSegment(id);
  }

  reserveRowIds(tableId: string, count: number): Promise<RowIdRange> {
    return this.inner.reserveRowIds(tableId, count);
  }

  getExistingUniqueKeys(tableId: string, keyTokens: readonly string[]): Promise<string[]> {
    return this.inner.getExistingUniqueKeys(tableId, keyTokens);
  }

  getCurrentManifest(): Promise<Manifest | undefined> {
    return this.inner.getCurrentManifest();
  }

  getManifest(version: number): Promise<Manifest | undefined> {
    return this.inner.getManifest(version);
  }

  listManifests(): Promise<Manifest[]> {
    return this.inner.listManifests();
  }

  async publishManifest(input: PublishManifestInput): Promise<Manifest> {
    await this.inject("beforeManifestCommit");
    const manifest = await this.inner.publishManifest(input);
    await this.inject("afterManifestCommit");
    return manifest;
  }

  createTransaction(record: TransactionRecord): Promise<void> {
    return this.inner.createTransaction(record);
  }

  getTransaction(id: string): Promise<TransactionRecord | undefined> {
    return this.inner.getTransaction(id);
  }

  listTransactions(): Promise<TransactionRecord[]> {
    return this.inner.listTransactions();
  }

  updateTransaction(
    id: string,
    expectedRevision: number,
    update: TransactionRecordUpdate,
  ): Promise<TransactionRecord> {
    return this.inner.updateTransaction(id, expectedRevision, update);
  }

  async commitTransaction(input: CommitTransactionInput): Promise<Manifest> {
    await this.inject("beforeTransactionCommit");
    const manifest = await this.inner.commitTransaction(input);
    await this.inject("afterTransactionCommit");
    return manifest;
  }

  createLease(record: LeaseRecord): Promise<void> {
    return this.inner.createLease(record);
  }

  getLease(id: string): Promise<LeaseRecord | undefined> {
    return this.inner.getLease(id);
  }

  listLeases(): Promise<LeaseRecord[]> {
    return this.inner.listLeases();
  }

  renewLease(id: string, expectedRevision: number, expiresAt: string): Promise<LeaseRecord> {
    return this.inner.renewLease(id, expectedRevision, expiresAt);
  }

  removeLeaseIfExpired(
    id: string,
    expectedRevision: number,
    expiresAtCutoff: string,
  ): Promise<boolean> {
    return this.inner.removeLeaseIfExpired(id, expectedRevision, expiresAtCutoff);
  }

  removeLease(id: string): Promise<void> {
    return this.inner.removeLease(id);
  }

  createCompactionJob(record: CompactionJobRecord): Promise<void> {
    return this.inner.createCompactionJob(record);
  }

  getCompactionJob(id: string): Promise<CompactionJobRecord | undefined> {
    return this.inner.getCompactionJob(id);
  }

  listCompactionJobs(tableId?: string): Promise<CompactionJobRecord[]> {
    return this.inner.listCompactionJobs(tableId);
  }

  updateCompactionJob(
    id: string,
    expectedRevision: number,
    update: CompactionJobRecordUpdate,
  ): Promise<CompactionJobRecord> {
    return this.inner.updateCompactionJob(id, expectedRevision, update);
  }

  cancelCompactionJob(
    id: string,
    expectedRevision: number,
    cancelledAt: string,
  ): Promise<CompactionJobRecord> {
    return this.inner.cancelCompactionJob(id, expectedRevision, cancelledAt);
  }

  removeCompactionJob(id: string): Promise<void> {
    return this.inner.removeCompactionJob(id);
  }

  createGarbageCollectionJob(
    input: CreateGarbageCollectionJobInput,
  ): Promise<GarbageCollectionJobRecord> {
    return this.inner.createGarbageCollectionJob(input);
  }

  getGarbageCollectionJob(id: string): Promise<GarbageCollectionJobRecord | undefined> {
    return this.inner.getGarbageCollectionJob(id);
  }

  listGarbageCollectionJobs(): Promise<GarbageCollectionJobRecord[]> {
    return this.inner.listGarbageCollectionJobs();
  }

  runGarbageCollectionStep(
    input: RunGarbageCollectionStepInput,
  ): Promise<GarbageCollectionStepResult> {
    return this.inner.runGarbageCollectionStep(input);
  }

  removeGarbageCollectionJob(id: string): Promise<void> {
    return this.inner.removeGarbageCollectionJob(id);
  }

  close(): void {
    this.inner.close();
  }
}
