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
  ManifestSummary,
  PublishManifestInput,
  CatalogProbe,
  QueryCatalogState,
  RowIdRange,
  RunGarbageCollectionStepInput,
  SegmentRecord,
  StoragePage,
  TableRecord,
  TempOwnerRecord,
  TempRunPage,
  TransactionRecord,
  TransactionRecordUpdate,
} from "../storage/index.js";

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
  /** Present only when the inner store implements it, so callers' fallbacks stay honest. */
  getQueryCatalogState?: (tableNames: readonly string[]) => Promise<QueryCatalogState>;
  /** Present only when the inner store implements it, so callers' fallbacks stay honest. */
  getCatalogProbe?: () => Promise<CatalogProbe>;

  constructor(
    private readonly inner: BlockStore,
    private readonly inject: FaultInjector,
  ) {
    const innerCatalogState = inner.getQueryCatalogState?.bind(inner);
    if (innerCatalogState !== undefined) {
      this.getQueryCatalogState = (tableNames) => innerCatalogState(tableNames);
    }
    const innerCatalogProbe = inner.getCatalogProbe?.bind(inner);
    if (innerCatalogProbe !== undefined) {
      this.getCatalogProbe = () => innerCatalogProbe();
    }
  }

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

  updateTable(
    id: string,
    expectedRevision: number,
    update: Parameters<BlockStore["updateTable"]>[2],
  ): Promise<TableRecord> {
    return this.inner.updateTable(id, expectedRevision, update);
  }

  removeTable(id: string, expectedRevision: number): Promise<void> {
    return this.inner.removeTable(id, expectedRevision);
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

  listSegmentPage(afterId: string | null, limit: number) {
    return this.inner.listSegmentPage(afterId, limit);
  }

  removeSegment(id: string): Promise<void> {
    return this.inner.removeSegment(id);
  }

  reserveRowIds(tableId: string, count: number): Promise<RowIdRange> {
    return this.inner.reserveRowIds(tableId, count);
  }

  reserveAutoIncrement(
    tableId: string,
    columnId: string,
    count: number,
    atLeast?: bigint,
  ): Promise<RowIdRange> {
    return this.inner.reserveAutoIncrement(tableId, columnId, count, atLeast);
  }

  writeFtsBase(
    tableId: string,
    columnId: string,
    input: Parameters<BlockStore["writeFtsBase"]>[2],
  ): Promise<void> {
    return this.inner.writeFtsBase(tableId, columnId, input);
  }

  beginFtsBaseBuild(tableId: string, columnId: string, buildId: string): Promise<void> {
    return this.inner.beginFtsBaseBuild(tableId, columnId, buildId);
  }

  writeFtsBaseBuildChunk(
    tableId: string,
    columnId: string,
    buildId: string,
    ordinal: number,
    chunk: Parameters<BlockStore["writeFtsBaseBuildChunk"]>[4],
  ): Promise<void> {
    return this.inner.writeFtsBaseBuildChunk(tableId, columnId, buildId, ordinal, chunk);
  }

  finishFtsBaseBuild(
    tableId: string,
    columnId: string,
    buildId: string,
    input: Parameters<BlockStore["finishFtsBaseBuild"]>[3],
  ): Promise<void> {
    return this.inner.finishFtsBaseBuild(tableId, columnId, buildId, input);
  }

  abortFtsBaseBuild(tableId: string, columnId: string, buildId: string): Promise<void> {
    return this.inner.abortFtsBaseBuild(tableId, columnId, buildId);
  }

  removeFtsColumn(tableId: string, columnId: string): Promise<void> {
    return this.inner.removeFtsColumn(tableId, columnId);
  }

  readFtsCandidates(
    tableId: string,
    columnId: string,
    terms: Parameters<BlockStore["readFtsCandidates"]>[2],
    upToVersion: number,
  ): ReturnType<BlockStore["readFtsCandidates"]> {
    return this.inner.readFtsCandidates(tableId, columnId, terms, upToVersion);
  }

  getExistingUniqueKeys(tableId: string, keyTokens: readonly string[]): Promise<string[]> {
    return this.inner.getExistingUniqueKeys(tableId, keyTokens);
  }

  getCurrentManifestVersion(): Promise<number | null> {
    return this.inner.getCurrentManifestVersion();
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

  listManifestPage(afterVersion: number | null, limit: number) {
    return this.inner.listManifestPage(afterVersion, limit);
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

  getTransactions(ids: readonly string[]): Promise<Array<TransactionRecord | undefined>> {
    return this.inner.getTransactions(ids);
  }

  listTransactions(): Promise<TransactionRecord[]> {
    return this.inner.listTransactions();
  }

  listTransactionPage(afterId: string | null, limit: number) {
    return this.inner.listTransactionPage(afterId, limit);
  }

  updateTransaction(
    id: string,
    expectedRevision: number,
    update: TransactionRecordUpdate,
  ): Promise<TransactionRecord> {
    return this.inner.updateTransaction(id, expectedRevision, update);
  }

  async commitTransaction(input: CommitTransactionInput): Promise<ManifestSummary> {
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

  listCompactionJobPage(afterId: string | null, limit: number) {
    return this.inner.listCompactionJobPage(afterId, limit);
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

  removePrunedManifestRecords(): Promise<number> {
    return this.inner.removePrunedManifestRecords();
  }

  removeGarbageCollectionJob(id: string): Promise<void> {
    return this.inner.removeGarbageCollectionJob(id);
  }

  putTempRunPage(page: TempRunPage): Promise<void> {
    return this.inner.putTempRunPage(page);
  }

  getTempRunPage(
    ownerId: string,
    runId: string,
    pageIndex: number,
  ): Promise<Uint8Array | undefined> {
    return this.inner.getTempRunPage(ownerId, runId, pageIndex);
  }

  removeTempRun(ownerId: string, runId: string): Promise<void> {
    return this.inner.removeTempRun(ownerId, runId);
  }

  removeTempOwner(ownerId: string): Promise<void> {
    return this.inner.removeTempOwner(ownerId);
  }

  createTempOwner(record: TempOwnerRecord): Promise<void> {
    return this.inner.createTempOwner(record);
  }

  getTempOwner(ownerId: string): Promise<TempOwnerRecord | undefined> {
    return this.inner.getTempOwner(ownerId);
  }

  renewTempOwner(
    ownerId: string,
    expectedRevision: number,
    expiresAt: string,
  ): Promise<TempOwnerRecord> {
    return this.inner.renewTempOwner(ownerId, expectedRevision, expiresAt);
  }

  removeTempOwnerIfExpired(ownerId: string, expiresAtCutoff: string): Promise<boolean> {
    return this.inner.removeTempOwnerIfExpired(ownerId, expiresAtCutoff);
  }

  listTempOwnerIdsPage(
    afterOwnerId: string | null,
    limit: number,
  ): Promise<StoragePage<string, string>> {
    return this.inner.listTempOwnerIdsPage(afterOwnerId, limit);
  }

  close(): void {
    this.inner.close();
  }
}

export {
  blockStoreConformanceCases,
  runBlockStoreConformance,
  type BlockStoreConformanceCase,
  type BlockStoreConformanceTarget,
} from "./block-store-conformance.js";

export { MemoryOpfs, type WriteFault } from "./opfs-shim.js";

export {
  DeterministicScheduler,
  generateSimulationPlan,
  parseSimulationPlan,
  runSimulation,
  scheduledBlockStore,
  type CollectionPassSummary,
  type SimulationDelete,
  type SimulationMutation,
  type SimulationOptions,
  type SimulationPlan,
  type SimulationPut,
  type SimulationResult,
  type SimulationStep,
  type SimulationTraceEvent,
} from "./simulator.js";

export {
  SqlLogicFailure,
  SqlLogicParseError,
  md5Hex,
  parseSqlLogicTest,
  parseSqlLogicTestLines,
  renderSqlLogicValue,
  runSqlLogicTest,
  type SqlLogicCondition,
  type SqlLogicDatabase,
  type SqlLogicExpectedHash,
  type SqlLogicExpectedValues,
  type SqlLogicHalt,
  type SqlLogicHashThreshold,
  type SqlLogicLocation,
  type SqlLogicQuery,
  type SqlLogicRecord,
  type SqlLogicRunStatistics,
  type SqlLogicRunOptions,
  type SqlLogicSortMode,
  type SqlLogicStatement,
  type SqlLogicType,
} from "./sqllogictest.js";
