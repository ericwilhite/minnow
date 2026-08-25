import type {
  BlockStore,
  AdoptAbortedSegmentInput,
  AbortTransactionIfExpiredInput,
  CompactionJobRecord,
  CompactionJobRecordUpdate,
  CreateGarbageCollectionJobInput,
  GarbageCollectionJobRecord,
  GarbageCollectionStepResult,
  CommitTransactionInput,
  LeaseRecord,
  Manifest,
  ManifestSummary,
  CatalogProbe,
  RowIdRange,
  RenewTransactionInput,
  RunGarbageCollectionStepInput,
  RollbackTransactionArtifactsInput,
  SegmentRecord,
  StageTransactionArtifactsInput,
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

  getCatalogProbe(): Promise<CatalogProbe> {
    return this.inner.getCatalogProbe();
  }

  beginTransaction(
    input: Parameters<BlockStore["beginTransaction"]>[0],
  ): ReturnType<BlockStore["beginTransaction"]> {
    return this.inner.beginTransaction(input);
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

  async readManifestBlock(version: number | null, id: string): Promise<Uint8Array | undefined> {
    await this.inject("beforeBlockRead");
    const bytes = await this.inner.readManifestBlock(version, id);
    await this.inject("afterBlockRead");
    return bytes;
  }

  hasManifestBlocks(version: number | null, ids: readonly string[]): Promise<boolean[]> {
    return this.inner.hasManifestBlocks(version, ids);
  }

  addTable(record: TableRecord, options?: Parameters<BlockStore["addTable"]>[1]): Promise<void> {
    return this.inner.addTable(record, options);
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

  removeTable(
    id: string,
    expectedRevision: number,
    options?: Parameters<BlockStore["removeTable"]>[2],
  ): Promise<void> {
    return this.inner.removeTable(id, expectedRevision, options);
  }

  dropTable(input: Parameters<BlockStore["dropTable"]>[0]) {
    return this.inner.dropTable(input);
  }

  dropTableColumn(input: Parameters<BlockStore["dropTableColumn"]>[0]) {
    return this.inner.dropTableColumn(input);
  }

  getSegment(id: string): Promise<SegmentRecord | undefined> {
    return this.inner.getSegment(id);
  }

  listSegmentPage(afterId: string | null, limit: number) {
    return this.inner.listSegmentPage(afterId, limit);
  }

  listTableSegmentPage(tableId: string, afterId: string | null, limit: number) {
    return this.inner.listTableSegmentPage(tableId, afterId, limit);
  }

  removeAbortedSegment(id: string, expectedTransactionId: string): Promise<boolean> {
    return this.inner.removeAbortedSegment(id, expectedTransactionId);
  }

  adoptAbortedSegment(input: AdoptAbortedSegmentInput): Promise<TransactionRecord> {
    return this.inner.adoptAbortedSegment(input);
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

  beginFtsBaseBuild(input: Parameters<BlockStore["beginFtsBaseBuild"]>[0]): Promise<void> {
    return this.inner.beginFtsBaseBuild(input);
  }

  renewFtsBaseBuild(input: Parameters<BlockStore["renewFtsBaseBuild"]>[0]): Promise<void> {
    return this.inner.renewFtsBaseBuild(input);
  }

  writeFtsBaseBuildChunk(
    input: Parameters<BlockStore["writeFtsBaseBuildChunk"]>[0],
  ): Promise<void> {
    return this.inner.writeFtsBaseBuildChunk(input);
  }

  finishFtsBaseBuild(input: Parameters<BlockStore["finishFtsBaseBuild"]>[0]): Promise<void> {
    return this.inner.finishFtsBaseBuild(input);
  }

  abortFtsBaseBuild(input: Parameters<BlockStore["abortFtsBaseBuild"]>[0]): Promise<void> {
    return this.inner.abortFtsBaseBuild(input);
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

  readFtsPostings(
    tableId: string,
    columnId: string,
    upToVersion: number,
    maxRowIds?: number,
    maxRetainedBytes?: number,
  ): ReturnType<BlockStore["readFtsPostings"]> {
    return this.inner.readFtsPostings(tableId, columnId, upToVersion, maxRowIds, maxRetainedBytes);
  }

  getExistingUniqueKeys(tableId: string, keyTokens: readonly string[]): Promise<string[]> {
    return this.inner.getExistingUniqueKeys(tableId, keyTokens);
  }

  beginUniqueKeyBuild(input: Parameters<BlockStore["beginUniqueKeyBuild"]>[0]) {
    return this.inner.beginUniqueKeyBuild(input);
  }

  getUniqueKeyBuild(buildId: string) {
    return this.inner.getUniqueKeyBuild(buildId);
  }

  renewUniqueKeyBuild(input: Parameters<BlockStore["renewUniqueKeyBuild"]>[0]) {
    return this.inner.renewUniqueKeyBuild(input);
  }

  appendUniqueKeyBuildChunk(input: Parameters<BlockStore["appendUniqueKeyBuildChunk"]>[0]) {
    return this.inner.appendUniqueKeyBuildChunk(input);
  }

  finishUniqueKeyBuild(input: Parameters<BlockStore["finishUniqueKeyBuild"]>[0]) {
    return this.inner.finishUniqueKeyBuild(input);
  }

  abortUniqueKeyBuild(input: Parameters<BlockStore["abortUniqueKeyBuild"]>[0]) {
    return this.inner.abortUniqueKeyBuild(input);
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

  listManifestBlockPage(input: Parameters<BlockStore["listManifestBlockPage"]>[0]) {
    return this.inner.listManifestBlockPage(input);
  }

  listRetiredManifestBlockPage(input: Parameters<BlockStore["listRetiredManifestBlockPage"]>[0]) {
    return this.inner.listRetiredManifestBlockPage(input);
  }

  listManifestPage(afterVersion: number | null, limit: number) {
    return this.inner.listManifestPage(afterVersion, limit);
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

  renewTransaction(input: RenewTransactionInput): Promise<boolean> {
    return this.inner.renewTransaction(input);
  }

  abortTransactionIfExpired(
    input: AbortTransactionIfExpiredInput,
  ): Promise<TransactionRecord | undefined> {
    return this.inner.abortTransactionIfExpired(input);
  }

  async stageTransactionArtifacts(
    input: StageTransactionArtifactsInput,
  ): Promise<TransactionRecord> {
    await this.inject("beforeBlockWrite");
    const transaction = await this.inner.stageTransactionArtifacts(input);
    await this.inject("afterBlockWrite");
    return transaction;
  }

  rollbackTransactionArtifacts(
    input: RollbackTransactionArtifactsInput,
  ): Promise<TransactionRecord> {
    return this.inner.rollbackTransactionArtifacts(input);
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

  listExpiredLeasePage(
    expiresAtCutoff: string,
    afterCursor: string | null,
    limit: number,
  ): Promise<StoragePage<LeaseRecord, string>> {
    return this.inner.listExpiredLeasePage(expiresAtCutoff, afterCursor, limit);
  }

  renewLease(input: Parameters<BlockStore["renewLease"]>[0]): Promise<LeaseRecord> {
    return this.inner.renewLease(input);
  }

  removeLeaseIfExpired(
    id: string,
    expectedRevision: number,
    expiresAtCutoff: string,
  ): Promise<boolean> {
    return this.inner.removeLeaseIfExpired(id, expectedRevision, expiresAtCutoff);
  }

  removeLease(input: Parameters<BlockStore["removeLease"]>[0]): Promise<boolean> {
    return this.inner.removeLease(input);
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

  removeCompactionJob(id: string): Promise<boolean> {
    return this.inner.removeCompactionJob(id);
  }

  createGarbageCollectionJob(
    input: CreateGarbageCollectionJobInput,
  ): Promise<GarbageCollectionJobRecord> {
    return this.inner.createGarbageCollectionJob(input);
  }

  updateGarbageCollectionPlanning(
    input: Parameters<BlockStore["updateGarbageCollectionPlanning"]>[0],
  ): Promise<GarbageCollectionJobRecord> {
    return this.inner.updateGarbageCollectionPlanning(input);
  }

  getGarbageCollectionJob(id: string): Promise<GarbageCollectionJobRecord | undefined> {
    return this.inner.getGarbageCollectionJob(id);
  }

  listGarbageCollectionJobs(): Promise<GarbageCollectionJobRecord[]> {
    return this.inner.listGarbageCollectionJobs();
  }

  listGarbageCollectionJobPage(
    afterId: string | null,
    limit: number,
  ): Promise<StoragePage<GarbageCollectionJobRecord, string>> {
    return this.inner.listGarbageCollectionJobPage(afterId, limit);
  }

  runGarbageCollectionStep(
    input: RunGarbageCollectionStepInput,
  ): Promise<GarbageCollectionStepResult> {
    return this.inner.runGarbageCollectionStep(input);
  }

  removePrunedManifestRecords(maxItems: number): Promise<number> {
    return this.inner.removePrunedManifestRecords(maxItems);
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

  renewTempOwner(input: Parameters<BlockStore["renewTempOwner"]>[0]): Promise<TempOwnerRecord> {
    return this.inner.renewTempOwner(input);
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

  listExpiredTempOwnerPage(
    expiresAtCutoff: string,
    afterCursor: string | null,
    limit: number,
  ): Promise<StoragePage<string, string>> {
    return this.inner.listExpiredTempOwnerPage(expiresAtCutoff, afterCursor, limit);
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

export { MemoryOpfs, type TransferLimit, type WriteFault } from "./opfs-shim.js";

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
