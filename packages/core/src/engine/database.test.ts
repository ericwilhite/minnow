import {
  BLOCK_HEADER_LENGTH,
  crc32,
  decodeBlock,
  encodeBlock,
  inspectBlock,
} from "../block-format/index.js";
import { IDBFactory } from "fake-indexeddb";
import { describe, expect, it, vi } from "vitest";
import {
  IndexedDbBlockStore,
  BlockReadBatchTooLargeError,
  MAX_ACTIVE_LEASES,
  MAX_MAINTENANCE_BATCH_ITEMS,
  MAX_INDEXED_STRING_CHARACTERS,
  MAX_TRANSACTION_STAGE_BYTES,
  MemoryBlockStore,
  StorageResourceLimitError,
  TableInUseError,
  TableRecordConflictError,
  type BlockStore,
  type CompactionJobRecord,
  type CompactionJobRecordUpdate,
  type LeaseRecord,
  type RowIdSpan,
  type SegmentRecord,
  type TableRecord,
} from "../storage/index.js";
import { FaultInjectingBlockStore } from "../testing/index.js";
import { TransactionManager } from "../transactions/index.js";
import { QueryMemoryBudgetError } from "./memory.js";
import { MAX_CACHEABLE_TEXT_CHARACTERS } from "./cache-limits.js";
import { compileQuery, type QueryRow } from "./query.js";
import { column, schema, table } from "./schema.js";
import { allVisibleSegments } from "./storage-test-helpers.js";
import {
  attachLifecycleFlush,
  DatabaseReadBacklogError,
  MAX_DATABASE_ACTIVE_READS,
  MAX_DATABASE_PENDING_WRITES,
  MAX_BUFFERED_WRITER_PENDING_ADDS,
  MAX_GARBAGE_COLLECTION_RETAIN_RECENT_VERSIONS,
  MAX_VISIBLE_SEGMENT_PAGE_ITEMS,
  MinnowDatabase,
  CompactionJobCancelledError,
  CompactionMemoryBudgetError,
  type DatabaseRow,
  type VisibleSegmentPageCursor,
  MissingKeyError,
  UniqueConstraintError,
  VisibleSegmentCursorStaleError,
} from "./database.js";

async function manifestBlockIdsAt(store: BlockStore, version: number): Promise<string[]> {
  const ids: string[] = [];
  let afterBlockId: string | null = null;
  for (;;) {
    const page = await store.listManifestBlockPage({ version, afterBlockId, limit: 1_024 });
    ids.push(...page.records.map(({ blockId }) => blockId));
    if (page.nextCursor === null) return ids;
    afterBlockId = page.nextCursor;
  }
}

async function currentManifestBlockIds(store: BlockStore): Promise<string[]> {
  const manifest = await store.getCurrentManifest();
  return manifest === undefined ? [] : manifestBlockIdsAt(store, manifest.version);
}

async function segmentRecords(store: BlockStore): Promise<SegmentRecord[]> {
  const records: SegmentRecord[] = [];
  let cursor: string | null = null;
  do {
    const page = await store.listSegmentPage(cursor, 1_024);
    records.push(...page.records);
    cursor = page.nextCursor;
  } while (cursor !== null);
  return records;
}

async function tableSegmentRecords(store: BlockStore, tableId: string): Promise<SegmentRecord[]> {
  const records: SegmentRecord[] = [];
  let cursor: string | null = null;
  do {
    const page = await store.listTableSegmentPage(tableId, cursor, 1_024);
    records.push(...page.records);
    cursor = page.nextCursor;
  } while (cursor !== null);
  return records;
}

async function physicalBlockCount(store: BlockStore): Promise<number> {
  if (store.getStorageStats === undefined) throw new Error("Store has no storage statistics");
  const stats = await store.getStorageStats();
  return stats.liveBlockCount + stats.obsoleteBlockCount;
}

async function transactionRecords(store: BlockStore) {
  const records: Array<Awaited<ReturnType<BlockStore["listTransactionPage"]>>["records"][number]> =
    [];
  let cursor: string | null = null;
  do {
    const page = await store.listTransactionPage(cursor, 1_024);
    records.push(...page.records);
    cursor = page.nextCursor;
  } while (cursor !== null);
  return records;
}

async function manifestRecords(store: BlockStore) {
  const records: Array<Awaited<ReturnType<BlockStore["listManifestPage"]>>["records"][number]> = [];
  let cursor: number | null = null;
  do {
    const page = await store.listManifestPage(cursor, 1_024);
    records.push(...page.records);
    cursor = page.nextCursor;
  } while (cursor !== null);
  return records;
}

it("caps public maintenance step sizes before durable work", async () => {
  const store = new MemoryBlockStore();
  const database = new MinnowDatabase(store, { autoCollect: false, autoCompact: false });
  await database.createTable({
    name: "bounded_maintenance",
    uniqueKey: "id",
    columns: [{ name: "id", type: "number" }],
  });
  const oversized = MAX_MAINTENANCE_BATCH_ITEMS + 1;
  await expect(database.collectGarbageStep({ maxItems: oversized })).rejects.toThrow(
    `cannot exceed ${String(MAX_MAINTENANCE_BATCH_ITEMS)}`,
  );
  await expect(database.collectGarbageStep({ maxPlanningItems: oversized })).rejects.toThrow(
    `cannot exceed ${String(MAX_MAINTENANCE_BATCH_ITEMS)}`,
  );
  await expect(
    database.collectGarbageStep({
      retainRecentVersions: MAX_GARBAGE_COLLECTION_RETAIN_RECENT_VERSIONS + 1,
    }),
  ).rejects.toThrow(`cannot exceed ${String(MAX_GARBAGE_COLLECTION_RETAIN_RECENT_VERSIONS)}`);
  await expect(
    database.compactTableStep("bounded_maintenance", { maxBlocks: oversized }),
  ).rejects.toThrow(`cannot exceed ${String(MAX_MAINTENANCE_BATCH_ITEMS)}`);
  expect(await store.listGarbageCollectionJobs()).toEqual([]);
  expect(await store.listCompactionJobs()).toEqual([]);
  await database.close();
});

it("converges when two database instances race to plan garbage collection", async () => {
  const store = new GarbageCollectionCreateBarrierStore();
  const left = new MinnowDatabase(store, { autoCollect: false, autoCompact: false });
  const right = new MinnowDatabase(store, { autoCollect: false, autoCompact: false });
  const [leftProgress, rightProgress] = await Promise.all([
    left.collectGarbageStep(),
    right.collectGarbageStep(),
  ]);
  expect(leftProgress.jobId).toBe(rightProgress.jobId);
  const jobs = await store.listGarbageCollectionJobs();
  expect(jobs).toHaveLength(1);
  expect(jobs.filter((job) => job.state !== "completed")).toHaveLength(0);
  await left.close();
  await right.close();
});

it("bounds indexed strings without restricting ordinary stored text", async () => {
  const store = new MemoryBlockStore();
  const database = new MinnowDatabase(store, { autoCollect: false, autoCompact: false });
  await database.createTable({
    name: "bounded_index_values",
    uniqueKey: "id",
    columns: [
      { name: "id", type: "number" },
      { name: "indexed", type: "string" },
      { name: "payload", type: "string" },
    ],
  });
  await database.insert("bounded_index_values", {
    id: 1,
    indexed: "ordinary",
    payload: "ordinary",
  });
  await database.createIndex("bounded_indexed_idx", "bounded_index_values", "indexed");
  const oversized = "x".repeat(MAX_INDEXED_STRING_CHARACTERS + 1);
  await expect(
    database.insert("bounded_index_values", { id: 2, indexed: oversized, payload: oversized }),
  ).resolves.toMatchObject({ rowCount: 1, tableName: "bounded_index_values" });
  expect(
    (
      await database.query("SELECT id, payload FROM bounded_index_values WHERE indexed = ?", {
        params: [oversized],
        memoize: false,
      })
    ).rows,
  ).toEqual([{ id: 2, payload: oversized }]);
  expect(
    Object.values((await store.getTableByName("bounded_index_values"))?.secondaryIndexes ?? {})[0]
      ?.state,
  ).toBe("invalid");

  await expect(
    database.createIndex("oversized_unique_idx", "bounded_index_values", "payload", {
      unique: true,
    }),
  ).rejects.toThrow(`cannot exceed ${String(MAX_INDEXED_STRING_CHARACTERS)} characters`);
  expect(
    Object.values(
      (await store.getTableByName("bounded_index_values"))?.secondaryIndexes ?? {},
    ).some((index) => index.name === "oversized_unique_idx"),
  ).toBe(false);

  await database.createTable({
    name: "bounded_primary_key",
    uniqueKey: "id",
    columns: [{ name: "id", type: "string" }],
  });
  await expect(database.insert("bounded_primary_key", { id: oversized })).rejects.toThrow(
    `cannot exceed ${String(MAX_INDEXED_STRING_CHARACTERS)} characters`,
  );
  expect(await database.readTable("bounded_primary_key")).toEqual([]);
  await database.close();
});

it("executes oversized SQL without retaining its text or compiled trees", async () => {
  const database = new MinnowDatabase(new MemoryBlockStore(), {
    autoCollect: false,
    autoCompact: false,
  });
  const before = database.maintenanceStatus();
  const text = "x".repeat(MAX_CACHEABLE_TEXT_CHARACTERS + 1);
  expect((await database.query(`SELECT '${text}' AS value`, { memoize: false })).rows).toEqual([
    { value: text },
  ]);
  expect(database.maintenanceStatus()).toMatchObject({
    retainedPlanEntries: before.retainedPlanEntries,
    retainedStatementEntries: before.retainedStatementEntries,
  });
  await database.execute(`${" ".repeat(MAX_CACHEABLE_TEXT_CHARACTERS + 1)}BEGIN`);
  expect(database.maintenanceStatus().retainedStatementEntries).toBe(
    before.retainedStatementEntries,
  );
  await database.execute("ROLLBACK");
  await database.close();
});

class CountingMemoryBlockStore extends MemoryBlockStore {
  blockWriteCalls = 0;
  blockReadCalls = 0;
  transactionListCalls = 0;
  transactionGetCalls = 0;
  transactionBatchCalls = 0;
  queryCatalogStateCalls = 0;
  segmentListCalls = 0;
  globalSegmentListCalls = 0;
  scopedSegmentTableIds: string[] = [];
  manifestGetCalls = 0;
  currentManifestGetCalls = 0;
  manifestMembershipIds = 0;
  blockIdsRead: string[][] = [];
  singleBlockIdsRead: string[] = [];
  pendingBlockJournalSizes: number[] = [];
  stagedBlockBatchSizes: number[] = [];
  stagedBlockBatchBytes: number[] = [];

  override async getBlocks(
    ids: Parameters<MemoryBlockStore["getBlocks"]>[0],
  ): Promise<Array<Uint8Array | undefined>> {
    this.blockReadCalls += 1;
    this.blockIdsRead.push([...ids]);
    return super.getBlocks(ids);
  }

  override async getBlock(id: string): Promise<Uint8Array | undefined> {
    this.singleBlockIdsRead.push(id);
    return super.getBlock(id);
  }

  override async listTransactionPage(
    afterId: Parameters<MemoryBlockStore["listTransactionPage"]>[0],
    limit: Parameters<MemoryBlockStore["listTransactionPage"]>[1],
  ) {
    this.transactionListCalls += 1;
    return super.listTransactionPage(afterId, limit);
  }

  override async getTransaction(id: string) {
    this.transactionGetCalls += 1;
    return super.getTransaction(id);
  }

  override async getTransactions(ids: readonly string[]) {
    this.transactionBatchCalls += 1;
    this.transactionGetCalls += ids.length;
    return super.getTransactions(ids);
  }

  override async getQueryCatalogState(names: readonly string[]) {
    this.queryCatalogStateCalls += 1;
    return super.getQueryCatalogState(names);
  }

  override async listSegmentPage(
    afterId: Parameters<MemoryBlockStore["listSegmentPage"]>[0],
    limit: Parameters<MemoryBlockStore["listSegmentPage"]>[1],
  ) {
    this.segmentListCalls += 1;
    this.globalSegmentListCalls += 1;
    return super.listSegmentPage(afterId, limit);
  }

  override async listTableSegmentPage(
    tableId: Parameters<MemoryBlockStore["listTableSegmentPage"]>[0],
    afterId: Parameters<MemoryBlockStore["listTableSegmentPage"]>[1],
    limit: Parameters<MemoryBlockStore["listTableSegmentPage"]>[2],
  ) {
    this.segmentListCalls += 1;
    this.scopedSegmentTableIds.push(tableId);
    return super.listTableSegmentPage(tableId, afterId, limit);
  }

  override async getManifest(version: number) {
    this.manifestGetCalls += 1;
    return super.getManifest(version);
  }

  override async getCurrentManifest() {
    this.currentManifestGetCalls += 1;
    return super.getCurrentManifest();
  }

  override async hasManifestBlocks(version: number | null, ids: readonly string[]) {
    this.manifestMembershipIds += ids.length;
    return super.hasManifestBlocks(version, ids);
  }

  override async updateTransaction(
    id: Parameters<MemoryBlockStore["updateTransaction"]>[0],
    expectedRevision: Parameters<MemoryBlockStore["updateTransaction"]>[1],
    update: Parameters<MemoryBlockStore["updateTransaction"]>[2],
  ) {
    if (update.pendingBlockIds !== undefined) {
      this.pendingBlockJournalSizes.push(update.pendingBlockIds.length);
    }
    return super.updateTransaction(id, expectedRevision, update);
  }

  override async stageTransactionArtifacts(
    input: Parameters<NonNullable<MemoryBlockStore["stageTransactionArtifacts"]>>[0],
  ) {
    if (input.blocks.length > 0) {
      this.blockWriteCalls += 1;
      this.stagedBlockBatchSizes.push(input.blocks.length);
      this.stagedBlockBatchBytes.push(
        input.blocks.reduce((total, block) => total + block.bytes.byteLength, 0),
      );
    }
    const updated = await super.stageTransactionArtifacts(input);
    this.pendingBlockJournalSizes.push(updated.pendingBlockIds.length);
    return updated;
  }

  override async writeTransaction(
    input: Parameters<NonNullable<MemoryBlockStore["writeTransaction"]>>[0],
  ) {
    if (input.blocks.length > 0) this.blockWriteCalls += 1;
    return super.writeTransaction(input);
  }
}

class ReadAdmissionBarrierStore extends MemoryBlockStore {
  enabled = false;
  entered = 0;
  readonly #gate: Promise<void>;
  #release!: () => void;

  constructor() {
    super();
    this.#gate = new Promise((resolve) => {
      this.#release = resolve;
    });
  }

  release(): void {
    this.#release();
  }

  async #waitForRelease(): Promise<void> {
    if (!this.enabled) return;
    this.entered += 1;
    await this.#gate;
  }

  override async getQueryCatalogState(names: readonly string[]) {
    await this.#waitForRelease();
    return super.getQueryCatalogState(names);
  }

  override async getBlocks(ids: readonly string[]) {
    await this.#waitForRelease();
    return super.getBlocks(ids);
  }

  override async getBlock(id: string) {
    await this.#waitForRelease();
    return super.getBlock(id);
  }

  override async listTableSegmentPage(
    tableId: Parameters<MemoryBlockStore["listTableSegmentPage"]>[0],
    afterId: Parameters<MemoryBlockStore["listTableSegmentPage"]>[1],
    limit: Parameters<MemoryBlockStore["listTableSegmentPage"]>[2],
  ) {
    await this.#waitForRelease();
    return super.listTableSegmentPage(tableId, afterId, limit);
  }
}

class RefusingLeaseAdmissionStore extends MemoryBlockStore {
  refuseLeaseAdmission = true;

  override async createLease(record: LeaseRecord): Promise<void> {
    if (this.refuseLeaseAdmission) {
      throw new StorageResourceLimitError("lease", MAX_ACTIVE_LEASES + 1, MAX_ACTIVE_LEASES);
    }
    return super.createLease(record);
  }
}

class DelayedRetiredSharedLeaseStore extends MemoryBlockStore {
  holdNextBlockRead = false;
  delayLeaseRemoval = false;
  #releaseBlockRead!: () => void;
  #signalBlockReadStarted!: () => void;
  #releaseLeaseRemoval!: () => void;
  #signalLeaseRemovalStarted!: () => void;
  readonly blockReadStarted = new Promise<void>((resolve) => {
    this.#signalBlockReadStarted = resolve;
  });
  readonly leaseRemovalStarted = new Promise<void>((resolve) => {
    this.#signalLeaseRemovalStarted = resolve;
  });
  readonly #blockReadGate = new Promise<void>((resolve) => {
    this.#releaseBlockRead = resolve;
  });
  readonly #leaseRemovalGate = new Promise<void>((resolve) => {
    this.#releaseLeaseRemoval = resolve;
  });

  releaseBlockRead(): void {
    this.#releaseBlockRead();
  }

  releaseLeaseRemoval(): void {
    this.#releaseLeaseRemoval();
  }

  override async getBlocks(ids: readonly string[]): Promise<Array<Uint8Array | undefined>> {
    if (this.holdNextBlockRead) {
      this.holdNextBlockRead = false;
      this.#signalBlockReadStarted();
      await this.#blockReadGate;
    }
    return super.getBlocks(ids);
  }

  override async removeLease(input: { id: string; ownerId: string }): Promise<boolean> {
    const removed = await super.removeLease(input);
    if (!this.delayLeaseRemoval) return removed;
    this.#signalLeaseRemovalStarted();
    await this.#leaseRemovalGate;
    return removed;
  }
}

it("rejects BEGIN promptly when durable lease admission is full and can begin later", async () => {
  const store = new RefusingLeaseAdmissionStore();
  const database = new MinnowDatabase(store, { autoCollect: false, autoCompact: false });

  await expect(database.execute("BEGIN")).rejects.toMatchObject({
    name: "StorageResourceLimitError",
    resource: "lease",
    count: MAX_ACTIVE_LEASES + 1,
    limit: MAX_ACTIVE_LEASES,
  });
  await expect(database.execute("ROLLBACK")).rejects.toThrow(
    "ROLLBACK without an open transaction",
  );

  store.refuseLeaseAdmission = false;
  await expect(database.execute("BEGIN")).resolves.toEqual({
    kind: "transaction",
    action: "begin",
  });
  await expect(database.execute("ROLLBACK")).resolves.toEqual({
    kind: "transaction",
    action: "rollback",
  });
  await database.close();
});

it("joins a retired shared-lease removal before database close returns", async () => {
  const store = new DelayedRetiredSharedLeaseStore();
  const database = new MinnowDatabase(store, { autoCollect: false, autoCompact: false });
  await database.createTable({
    name: "close_retired_lease",
    columns: [{ name: "value", type: "number" }],
  });
  await database.insert("close_retired_lease", { value: 1 });

  store.holdNextBlockRead = true;
  const oldQuery = database.query("SELECT value FROM close_retired_lease ORDER BY value", {
    memoize: false,
  });
  await store.blockReadStarted;
  await database.insert("close_retired_lease", { value: 2 });
  expect(
    (
      await database.query("SELECT value FROM close_retired_lease ORDER BY value", {
        memoize: false,
      })
    ).rows,
  ).toEqual([{ value: 1 }, { value: 2 }]);

  // The old query now retires its no-longer-shared pin. Model a store that applied the durable
  // delete but has not acknowledged its transaction yet.
  store.delayLeaseRemoval = true;
  store.releaseBlockRead();
  await expect(oldQuery).resolves.toMatchObject({ rows: [{ value: 1 }] });
  await store.leaseRemovalStarted;

  const closing = database.close();
  await expect(
    Promise.race([closing.then(() => "closed"), Promise.resolve("pending")]),
  ).resolves.toBe("pending");
  store.releaseLeaseRemoval();
  await expect(closing).resolves.toBeUndefined();
  expect(await store.listLeases()).toEqual([]);
  store.close();
});

it("waits for an active snapshot scope and its lease removal before close returns", async () => {
  const store = new DelayedRetiredSharedLeaseStore();
  store.delayLeaseRemoval = true;
  const database = new MinnowDatabase(store, { autoCollect: false, autoCompact: false });
  let signalScopeEntered!: () => void;
  const scopeEntered = new Promise<void>((resolve) => {
    signalScopeEntered = resolve;
  });
  let finishScope!: () => void;
  const scopeGate = new Promise<void>((resolve) => {
    finishScope = resolve;
  });
  const scoped = database.snapshot(async () => {
    signalScopeEntered();
    await scopeGate;
    return "finished";
  });
  await scopeEntered;
  expect(await store.listLeases()).toHaveLength(1);

  const closing = database.close();
  await expect(
    Promise.race([closing.then(() => "closed"), Promise.resolve("pending")]),
  ).resolves.toBe("pending");
  finishScope();
  await store.leaseRemovalStarted;
  await expect(scoped).resolves.toBe("finished");
  await expect(
    Promise.race([closing.then(() => "closed"), Promise.resolve("pending")]),
  ).resolves.toBe("pending");

  store.releaseLeaseRemoval();
  await expect(closing).resolves.toBeUndefined();
  expect(await store.listLeases()).toEqual([]);
  store.close();
});

it("bounds concurrent direct reads without serializing admitted work", async () => {
  const store = new ReadAdmissionBarrierStore();
  const database = new MinnowDatabase(store, { autoCollect: false, autoCompact: false });
  await database.createTable({
    name: "read_admission",
    columns: [{ name: "value", type: "number" }],
  });
  await database.insert("read_admission", { value: 1 });
  store.enabled = true;
  const admitted = Array.from({ length: MAX_DATABASE_ACTIVE_READS }, () =>
    database.query("SELECT value FROM read_admission", { memoize: false }),
  );
  await vi.waitFor(() => expect(store.entered).toBe(MAX_DATABASE_ACTIVE_READS));
  await expect(database.readTable("read_admission")).rejects.toBeInstanceOf(
    DatabaseReadBacklogError,
  );
  const cursor = database.queryCursor("SELECT value FROM read_admission", { memoize: false });
  await expect(cursor.next()).rejects.toBeInstanceOf(DatabaseReadBacklogError);
  store.release();
  await expect(Promise.all(admitted)).resolves.toHaveLength(MAX_DATABASE_ACTIVE_READS);
  store.enabled = false;
  await expect(database.readTable("read_admission")).resolves.toEqual([{ value: 1 }]);
  await database.close();
});

it.each(["snapshot", "write"] as const)(
  "bounds concurrent reads inside one %s scope",
  async (scope) => {
    const store = new ReadAdmissionBarrierStore();
    const database = new MinnowDatabase(store, { autoCollect: false, autoCompact: false });
    await database.createTable({
      name: "scoped_read_admission",
      columns: [{ name: "value", type: "number" }],
    });
    await database.insert("scoped_read_admission", { value: 1 });
    store.enabled = true;
    const exercise = async (session: { query(sql: string): Promise<unknown> }) => {
      const admitted = Array.from({ length: MAX_DATABASE_ACTIVE_READS }, () =>
        session.query("SELECT value FROM scoped_read_admission"),
      );
      await vi.waitFor(() => expect(store.entered).toBe(MAX_DATABASE_ACTIVE_READS));
      await expect(session.query("SELECT value FROM scoped_read_admission")).rejects.toBeInstanceOf(
        DatabaseReadBacklogError,
      );
      store.release();
      await expect(Promise.all(admitted)).resolves.toHaveLength(MAX_DATABASE_ACTIVE_READS);
    };
    if (scope === "snapshot") await database.snapshot(exercise);
    else await database.write(exercise);
    await database.close();
  },
);

class ReplacementRestageFaultMemoryBlockStore extends CountingMemoryBlockStore {
  failNextRestageRead = false;

  override async getBlock(id: string): Promise<Uint8Array | undefined> {
    if (this.failNextRestageRead && id.includes("/rewrite/window/")) {
      this.failNextRestageRead = false;
      throw new Error("injected before replacement output restaging");
    }
    return super.getBlock(id);
  }
}

class TransientTableInUseMemoryBlockStore extends MemoryBlockStore {
  dropTableCalls = 0;
  refuseNextRemoval = true;

  override async dropTable(input: Parameters<MemoryBlockStore["dropTable"]>[0]) {
    this.dropTableCalls += 1;
    if (this.refuseNextRemoval) {
      this.refuseNextRemoval = false;
      throw new TableInUseError(input.tableId, "compaction job", "late-fold");
    }
    return super.dropTable(input);
  }
}

class RefusingViewUpdateMemoryBlockStore extends MemoryBlockStore {
  refuseNextViewUpdate = false;

  override async updateTable(
    id: string,
    expectedRevision: number,
    update: Parameters<MemoryBlockStore["updateTable"]>[2],
  ) {
    if (this.refuseNextViewUpdate && update.view !== undefined) {
      this.refuseNextViewUpdate = false;
      throw new DOMException("injected view quota refusal", "QuotaExceededError");
    }
    return super.updateTable(id, expectedRevision, update);
  }
}

class RacingViewUpdateMemoryBlockStore extends MemoryBlockStore {
  raceNextViewUpdate = false;

  override async updateTable(
    id: string,
    expectedRevision: number,
    update: Parameters<MemoryBlockStore["updateTable"]>[2],
  ) {
    if (this.raceNextViewUpdate && update.view !== undefined) {
      this.raceNextViewUpdate = false;
      const current = await this.getTable(id);
      if (current === undefined) throw new Error(`Missing racing view: ${id}`);
      await super.updateTable(id, expectedRevision, {
        columns: current.columns,
        view: { sql: "SELECT 7 AS value", managed: false },
      });
    }
    return super.updateTable(id, expectedRevision, update);
  }
}

class CheckpointFaultMemoryBlockStore extends MemoryBlockStore {
  failOutputCheckpoint = true;

  override async updateCompactionJob(
    id: string,
    expectedRevision: number,
    update: CompactionJobRecordUpdate,
  ) {
    if (update.outputBlockIds !== undefined && this.failOutputCheckpoint) {
      this.failOutputCheckpoint = false;
      throw new Error("injected before compaction cursor checkpoint");
    }
    return super.updateCompactionJob(id, expectedRevision, update);
  }
}

class PersistentPostingBuildFaultStore extends MemoryBlockStore {
  failFtsBuilds = false;
  failSecondaryBuilds = false;
  failPostingInvalidations = false;
  postingInvalidationAttempts = 0;

  override async writeFtsBase(
    tableId: Parameters<MemoryBlockStore["writeFtsBase"]>[0],
    columnId: Parameters<MemoryBlockStore["writeFtsBase"]>[1],
    input: Parameters<MemoryBlockStore["writeFtsBase"]>[2],
  ): Promise<void> {
    if (this.failFtsBuilds) throw new Error("injected full-text rebuild failure");
    return super.writeFtsBase(tableId, columnId, input);
  }

  override async beginFtsBaseBuild(
    input: Parameters<MemoryBlockStore["beginFtsBaseBuild"]>[0],
  ): Promise<void> {
    if (this.failSecondaryBuilds) throw new Error("injected scalar rebuild failure");
    return super.beginFtsBaseBuild(input);
  }

  override async writeFtsBaseBuildChunk(
    input: Parameters<MemoryBlockStore["writeFtsBaseBuildChunk"]>[0],
  ): Promise<void> {
    if (this.failFtsBuilds) throw new Error("injected streamed full-text build failure");
    return super.writeFtsBaseBuildChunk(input);
  }

  override async updateTable(
    id: Parameters<MemoryBlockStore["updateTable"]>[0],
    expectedRevision: Parameters<MemoryBlockStore["updateTable"]>[1],
    update: Parameters<MemoryBlockStore["updateTable"]>[2],
  ): ReturnType<MemoryBlockStore["updateTable"]> {
    const invalidatesPostingStorage =
      Object.values(update.ftsColumns ?? {}).some((state) => state.state === "invalid") ||
      Object.values(update.secondaryIndexes ?? {}).some((state) => state.state === "invalid");
    if (invalidatesPostingStorage && this.failPostingInvalidations) {
      this.postingInvalidationAttempts += 1;
      throw new Error("injected postings invalidation failure");
    }
    return super.updateTable(id, expectedRevision, update);
  }
}

class OverflowingFtsCandidateStore extends MemoryBlockStore {
  candidateReads = 0;

  override async readFtsCandidates(
    tableId: string,
    columnId: string,
    terms: Parameters<MemoryBlockStore["readFtsCandidates"]>[2],
    upToVersion: number,
    maxRowIds?: number,
  ) {
    if (terms.length === 0) {
      return super.readFtsCandidates(tableId, columnId, terms, upToVersion, maxRowIds);
    }
    this.candidateReads += 1;
    const metadata = await super.readFtsCandidates(tableId, columnId, [], upToVersion, maxRowIds);
    return {
      ...metadata,
      rowIdsByTerm: terms.map(() => []),
      overflow: true,
    };
  }
}

class TinyBlockReadBatchStore extends MemoryBlockStore {
  largestAttempt = 0;

  override async getBlocks(ids: readonly string[]) {
    this.largestAttempt = Math.max(this.largestAttempt, ids.length);
    if (ids.length > 2) throw new BlockReadBatchTooLargeError(ids.length, 2);
    return super.getBlocks(ids);
  }
}

class GarbageCollectionCreateBarrierStore extends MemoryBlockStore {
  #arrivals = 0;
  #release!: () => void;
  readonly #gate = new Promise<void>((resolve) => {
    this.#release = resolve;
  });

  override async createGarbageCollectionJob(
    input: Parameters<MemoryBlockStore["createGarbageCollectionJob"]>[0],
  ) {
    this.#arrivals += 1;
    if (this.#arrivals === 2) this.#release();
    await this.#gate;
    return super.createGarbageCollectionJob(input);
  }
}

class GzipVariantCheckpointFaultMemoryBlockStore extends CheckpointFaultMemoryBlockStore {
  returnHeaderVariant = false;
  variantReadCount = 0;

  async getCanonicalBlock(id: string): Promise<Uint8Array | undefined> {
    return super.getBlock(id);
  }

  override async getBlock(id: string): Promise<Uint8Array | undefined> {
    const bytes = await super.getBlock(id);
    if (
      bytes === undefined ||
      !this.returnHeaderVariant ||
      !id.includes("/rewrite/window/") ||
      inspectBlock(bytes).compression !== "gzip"
    ) {
      return bytes;
    }
    const variant = new Uint8Array(bytes);
    const view = new DataView(variant.buffer, variant.byteOffset, variant.byteLength);
    const metadataLength = view.getUint32(24, true);
    const storedOffset = BLOCK_HEADER_LENGTH + metadataLength;
    if (variant[storedOffset] !== 0x1f || variant[storedOffset + 1] !== 0x8b) {
      throw new Error("Expected a gzip compaction payload");
    }
    variant[storedOffset + 4] = (variant[storedOffset + 4] ?? 0) ^ 1;
    view.setUint32(40, crc32(variant.subarray(storedOffset)), true);
    view.setUint32(4, crc32(variant.subarray(8, BLOCK_HEADER_LENGTH + metadataLength)), true);
    this.variantReadCount += 1;
    return variant;
  }
}

class InitialCompactionPlanningBarrierStore extends MemoryBlockStore {
  #pageReadCount = 0;
  #jobReadCount = 0;
  #releaseListReads: (() => void) | undefined;
  #releaseJobReads: (() => void) | undefined;
  readonly #listReadsReady = new Promise<void>((resolve) => {
    this.#releaseListReads = resolve;
  });
  readonly #jobReadsReady = new Promise<void>((resolve) => {
    this.#releaseJobReads = resolve;
  });

  override async listCompactionJobPage(
    afterId: Parameters<MemoryBlockStore["listCompactionJobPage"]>[0],
    limit: Parameters<MemoryBlockStore["listCompactionJobPage"]>[1],
  ) {
    if (this.#pageReadCount >= 2) return super.listCompactionJobPage(afterId, limit);
    const page = await super.listCompactionJobPage(afterId, limit);
    this.#pageReadCount += 1;
    if (this.#pageReadCount === 2) this.#releaseListReads?.();
    await this.#listReadsReady;
    return page;
  }

  override async getCompactionJob(id: string): Promise<CompactionJobRecord | undefined> {
    if (this.#jobReadCount >= 2) return super.getCompactionJob(id);
    const record = await super.getCompactionJob(id);
    this.#jobReadCount += 1;
    if (this.#jobReadCount === 2) this.#releaseJobReads?.();
    await this.#jobReadsReady;
    return record;
  }
}

class FirstCommitBarrierMemoryBlockStore extends MemoryBlockStore {
  #pauseNextCommit = true;
  #signalFirstCommit: (() => void) | undefined;
  #releaseFirstCommit: (() => void) | undefined;
  readonly firstCommitReached = new Promise<void>((resolve) => {
    this.#signalFirstCommit = resolve;
  });
  readonly #firstCommitRelease = new Promise<void>((resolve) => {
    this.#releaseFirstCommit = resolve;
  });

  releaseFirstCommit(): void {
    this.#releaseFirstCommit?.();
  }

  override async commitTransaction(
    input: Parameters<MemoryBlockStore["commitTransaction"]>[0],
  ): ReturnType<MemoryBlockStore["commitTransaction"]> {
    await this.#pauseFirst();
    return super.commitTransaction(input);
  }

  // The engine's simple writes commit through the single-shot path; the barrier sits there too.
  override async writeTransaction(
    input: Parameters<NonNullable<MemoryBlockStore["writeTransaction"]>>[0],
  ): ReturnType<NonNullable<MemoryBlockStore["writeTransaction"]>> {
    await this.#pauseFirst();
    return super.writeTransaction(input);
  }

  async #pauseFirst(): Promise<void> {
    if (this.#pauseNextCommit) {
      this.#pauseNextCommit = false;
      this.#signalFirstCommit?.();
      await this.#firstCommitRelease;
    }
  }
}

function implementations(): Array<{ name: string; create: () => Promise<BlockStore> }> {
  return [
    { name: "memory", create: async () => new MemoryBlockStore() },
    {
      name: "indexeddb",
      create: async () =>
        IndexedDbBlockStore.open({ name: crypto.randomUUID(), indexedDB: new IDBFactory() }),
    },
  ];
}

function recoveryImplementations(): Array<{
  name: string;
  create: () => Promise<{ store: BlockStore; reopen: () => Promise<BlockStore> }>;
}> {
  return [
    {
      name: "memory",
      create: async () => {
        const store = new MemoryBlockStore();
        return { store, reopen: async () => store };
      },
    },
    {
      name: "indexeddb reopen",
      create: async () => {
        const indexedDB = new IDBFactory();
        const name = crypto.randomUUID();
        let store = await IndexedDbBlockStore.open({ name, indexedDB });
        return {
          store,
          reopen: async () => {
            store.close();
            store = await IndexedDbBlockStore.open({ name, indexedDB });
            return store;
          },
        };
      },
    },
  ];
}

for (const implementation of implementations()) {
  describe(`${implementation.name} SQL boundary durability`, () => {
    it("advances same-instance mutation visibility and reads deferred blocks in a write scope", async () => {
      const store = await implementation.create();
      const database = new MinnowDatabase(store, {
        autoCompact: false,
        compression: "raw",
        rowsPerBlock: 1,
      });
      await database.execute(
        "CREATE TABLE mutation_visibility (id INTEGER PRIMARY KEY, n INTEGER NOT NULL)",
      );
      await database.execute("INSERT INTO mutation_visibility VALUES (1, 0), (2, 10), (3, 20)");

      await database.execute("UPDATE mutation_visibility SET n = n + 1 WHERE id = 1");
      await database.execute("UPDATE mutation_visibility SET n = n + 1 WHERE id = 1");
      await database.execute("DELETE FROM mutation_visibility WHERE id = 2");
      expect(
        (
          await database.query("SELECT id, n FROM mutation_visibility ORDER BY id", {
            memoize: false,
          })
        ).rows,
      ).toEqual([
        { id: 1, n: 2 },
        { id: 3, n: 20 },
      ]);

      await database.write(async (session) => {
        // The second stage flushes the first bounded batch and makes an active owner plus its
        // segments visible to catalog reads. A concurrent ordinary read still sees only the
        // committed snapshot, and caching that state must not hide those segments after commit.
        await session.insertBatch("mutation_visibility", [{ id: 4, n: 40 }]);
        await session.updateBatch("mutation_visibility", {
          keys: [1, 3],
          changes: { n: [3, 21] },
        });
        expect(
          (
            await database.query("SELECT id, n FROM mutation_visibility ORDER BY id", {
              memoize: false,
            })
          ).rows,
        ).toEqual([
          { id: 1, n: 2 },
          { id: 3, n: 20 },
        ]);
        expect(
          (await session.query("SELECT id, n FROM mutation_visibility ORDER BY id")).rows,
        ).toEqual([
          { id: 1, n: 3 },
          { id: 3, n: 21 },
          { id: 4, n: 40 },
        ]);
      });
      expect(
        (
          await database.query("SELECT id, n FROM mutation_visibility ORDER BY id", {
            memoize: false,
          })
        ).rows,
      ).toEqual([
        { id: 1, n: 3 },
        { id: 3, n: 21 },
        { id: 4, n: 40 },
      ]);
      store.close();
    });

    it("preserves private-namespace TEXT through queries, transactions, compaction, and snapshots", async () => {
      const store = await implementation.create();
      const database = new MinnowDatabase(store, {
        autoCompact: false,
        compression: "raw",
        rowsPerBlock: 1,
      });
      const numericTwo = "\0minnow-domain:numeric:2";
      const numericTen = "\0minnow-domain:numeric:10";
      const invalidInterval = "\0minnow-domain:interval:not-json";
      const nestedText = "\0minnow-domain:text:\0minnow-domain:numeric:2";
      const committedText = '\0minnow-domain:jsonb:{"a":1}';
      const originalRows = [
        { id: 1, value: numericTwo },
        { id: 2, value: numericTen },
        { id: 3, value: invalidInterval },
        { id: 4, value: nestedText },
      ];

      await database.execute("CREATE TABLE tagged_text (id INTEGER PRIMARY KEY, value TEXT)");
      await database.insertBatch("tagged_text", originalRows.slice(0, 2));
      const baseline = await database.insertBatch("tagged_text", originalRows.slice(2));
      await database.execute("CREATE INDEX tagged_text_value_idx ON tagged_text (value)");

      expect(await database.readTable("tagged_text")).toEqual(originalRows);
      expect((await database.query("SELECT id FROM tagged_text ORDER BY value, id")).rows).toEqual([
        { id: 3 },
        { id: 2 },
        { id: 1 },
        { id: 4 },
      ]);
      expect(
        (
          await database.query("SELECT id FROM tagged_text WHERE value = ? ORDER BY id", {
            params: [numericTwo],
          })
        ).rows,
      ).toEqual([{ id: 1 }]);
      expect(
        (
          await database.query(
            "SELECT value, COUNT(*) AS n FROM tagged_text GROUP BY value ORDER BY value",
          )
        ).rows,
      ).toEqual([
        { value: invalidInterval, n: 1 },
        { value: numericTen, n: 1 },
        { value: numericTwo, n: 1 },
        { value: nestedText, n: 1 },
      ]);

      await database.execute("CREATE TABLE copied_text (value TEXT)");
      await database.insertBatch("copied_text", [{ value: numericTwo }, { value: nestedText }]);
      expect(
        (
          await database.query(
            "SELECT t.id FROM tagged_text t JOIN copied_text c ON t.value = c.value ORDER BY t.id",
          )
        ).rows,
      ).toEqual([{ id: 1 }, { id: 4 }]);
      await expect(
        database.execute("INSERT INTO copied_text VALUES (?) RETURNING value", [invalidInterval]),
      ).resolves.toMatchObject({ returnedRows: [{ value: invalidInterval }] });
      expect(
        (
          await database.query(
            "SELECT id FROM tagged_text WHERE MATCH(value) AGAINST 'numeric' ORDER BY id",
          )
        ).rows,
      ).toEqual([{ id: 1 }, { id: 2 }, { id: 4 }]);

      await database.execute("BEGIN");
      await database.execute("UPDATE tagged_text SET value = ? WHERE id = 1", [invalidInterval]);
      expect((await database.query("SELECT value FROM tagged_text WHERE id = 1")).rows).toEqual([
        { value: invalidInterval },
      ]);
      await database.execute("ROLLBACK");
      expect((await database.query("SELECT value FROM tagged_text WHERE id = 1")).rows).toEqual([
        { value: numericTwo },
      ]);

      await database.execute("UPDATE tagged_text SET value = ? WHERE id = 2", [nestedText]);
      expect(
        (
          await database.query("SELECT id FROM tagged_text WHERE value = ? ORDER BY id", {
            params: [nestedText],
          })
        ).rows,
      ).toEqual([{ id: 2 }, { id: 4 }]);
      await database.execute("UPDATE tagged_text SET value = ? WHERE id = 2", [numericTen]);

      await database.execute("BEGIN");
      await database.execute("UPDATE tagged_text SET value = ? WHERE id = 4", [committedText]);
      const committed = await database.execute("COMMIT");
      if (committed.kind !== "transaction" || committed.version === undefined) {
        throw new Error("Expected a committed SQL transaction version");
      }
      const currentRows = [
        originalRows[0],
        originalRows[1],
        originalRows[2],
        { id: 4, value: committedText },
      ];
      expect(await database.readTable("tagged_text")).toEqual(currentRows);
      expect(await database.readTable("tagged_text", baseline.version)).toEqual(originalRows);

      await database.execute("CREATE TABLE selected_text (id INTEGER PRIMARY KEY, value TEXT)");
      await database.execute("INSERT INTO selected_text SELECT id, value FROM tagged_text");
      expect(await database.readTable("selected_text")).toEqual(currentRows);
      await database.execute("CREATE TABLE cloned_text AS SELECT value FROM tagged_text");
      expect((await database.query("SELECT value FROM cloned_text ORDER BY value")).rows).toEqual([
        { value: invalidInterval },
        { value: committedText },
        { value: numericTen },
        { value: numericTwo },
      ]);

      await database.execute("CREATE TABLE keyed_text (id TEXT PRIMARY KEY, bucket INTEGER)");
      await database.insertBatch("keyed_text", [
        { id: numericTwo, bucket: 1 },
        { id: nestedText, bucket: 2 },
        { id: invalidInterval, bucket: 2 },
      ]);
      await database.execute("CREATE INDEX keyed_text_bucket_idx ON keyed_text (bucket)");
      expect(
        (await database.query("SELECT id FROM keyed_text WHERE bucket = 2 ORDER BY id")).rows,
      ).toEqual([{ id: invalidInterval }, { id: nestedText }]);
      await database.execute("UPDATE keyed_text SET bucket = 3 WHERE id = ?", [numericTwo]);
      expect((await database.query("SELECT id FROM keyed_text WHERE bucket = 3")).rows).toEqual([
        { id: numericTwo },
      ]);
      await expect(
        database.execute("INSERT INTO keyed_text VALUES (?, 9) ON CONFLICT (id) DO NOTHING", [
          numericTwo,
        ]),
      ).resolves.toMatchObject({ rowCount: 0 });

      await database.execute("CREATE TABLE expression_text (id INTEGER PRIMARY KEY, value TEXT)");
      await database.execute("INSERT INTO expression_text VALUES (1, ?)", [numericTwo]);
      await expect(
        database.execute(
          "INSERT INTO expression_text VALUES (1, ?) " +
            "ON CONFLICT (id) DO UPDATE SET value = EXCLUDED.value RETURNING value",
          [nestedText],
        ),
      ).resolves.toMatchObject({ returnedRows: [{ value: nestedText }] });
      await database.execute("CREATE TABLE merge_text (id INTEGER PRIMARY KEY, value TEXT)");
      await database.insertBatch("merge_text", [
        { id: 1, value: invalidInterval },
        { id: 2, value: numericTen },
      ]);
      await database.execute(
        "MERGE INTO expression_text t USING merge_text s ON t.id = s.id " +
          "WHEN MATCHED THEN UPDATE SET value = s.value " +
          "WHEN NOT MATCHED THEN INSERT (id, value) VALUES (s.id, s.value)",
      );
      expect(await database.readTable("expression_text")).toEqual([
        { id: 1, value: invalidInterval },
        { id: 2, value: numericTen },
      ]);

      await database.execute("CREATE TABLE trigger_target (id INTEGER PRIMARY KEY, value TEXT)");
      await database.execute("INSERT INTO trigger_target VALUES (1, ?)", [numericTwo]);
      await database.execute("CREATE TABLE trigger_source (id INTEGER PRIMARY KEY, value TEXT)");
      await database.execute(
        "CREATE TRIGGER copy_private_text AFTER INSERT ON trigger_source BEGIN " +
          "UPDATE trigger_target SET value = NEW.value WHERE id = 1; END",
      );
      await database.insertBatch("trigger_source", [{ id: 1, value: nestedText }]);
      expect(await database.readTable("trigger_target")).toEqual([{ id: 1, value: nestedText }]);

      expect((await database.compactTable("tagged_text")).compacted).toBe(true);
      expect(await database.readTable("tagged_text")).toEqual(currentRows);
      expect(await database.readTable("tagged_text", baseline.version)).toEqual(originalRows);

      const bytes = await database.exportSnapshot();
      const restoredStore = await implementation.create();
      const restored = new MinnowDatabase(restoredStore, { autoCompact: false });
      await restored.importSnapshot(bytes);
      expect(await restored.readTable("tagged_text")).toEqual(currentRows);
      expect(await restored.readTable("selected_text")).toEqual(currentRows);
      expect((await restored.query("SELECT value FROM cloned_text ORDER BY value")).rows).toEqual([
        { value: invalidInterval },
        { value: committedText },
        { value: numericTen },
        { value: numericTwo },
      ]);
      expect(
        (await restored.query("SELECT id FROM keyed_text WHERE bucket = 2 ORDER BY id")).rows,
      ).toEqual([{ id: invalidInterval }, { id: nestedText }]);
      expect((await restored.query("SELECT id FROM keyed_text WHERE bucket = 3")).rows).toEqual([
        { id: numericTwo },
      ]);
      expect(await restored.readTable("expression_text")).toEqual([
        { id: 1, value: invalidInterval },
        { id: 2, value: numericTen },
      ]);
      expect(await restored.readTable("trigger_target")).toEqual([{ id: 1, value: nestedText }]);
      expect((await restored.query("SELECT id FROM tagged_text ORDER BY value, id")).rows).toEqual([
        { id: 3 },
        { id: 4 },
        { id: 2 },
        { id: 1 },
      ]);
      expect(
        (
          await restored.query("SELECT id FROM tagged_text WHERE value = ?", {
            params: [numericTwo],
          })
        ).rows,
      ).toEqual([{ id: 1 }]);
      expect(
        (
          await restored.query(
            "SELECT id FROM tagged_text WHERE MATCH(*) AGAINST 'numeric' ORDER BY id",
          )
        ).rows,
      ).toEqual([{ id: 1 }, { id: 2 }]);

      await restored.close();
      await database.close();
      restoredStore.close();
      store.close();
    });

    it("keeps composite row locators private and valid after compaction and restore", async () => {
      const store = await implementation.create();
      const database = new MinnowDatabase(store, {
        autoCompact: false,
        compression: "raw",
        rowsPerBlock: 1,
      });
      const visibleRows = [
        { shop: 1, receipt: 1, note: "alpha" },
        { shop: 1, receipt: 2, note: "bravo" },
        { shop: 2, receipt: 1, note: "charlie" },
      ];
      await database.execute(
        "CREATE TABLE receipts (shop INTEGER, receipt INTEGER, note TEXT, PRIMARY KEY (shop, receipt))",
      );
      await database.execute("INSERT INTO receipts VALUES (1, 1, 'alpha'), (1, 2, 'bravo')");
      await database.execute("INSERT INTO receipts VALUES (2, 1, 'charlie')");

      expect(await database.readTable("receipts")).toEqual(visibleRows);
      expect((await database.query("SELECT * FROM receipts ORDER BY shop, receipt")).rows).toEqual(
        visibleRows,
      );
      await expect(
        database.readTable("receipts", { columns: ["\0minnow_primary_key"] }),
      ).rejects.toThrow("Unknown column");
      expect(
        (await database.query("SELECT * FROM receipts WHERE MATCH(*) AGAINST 'bff*'")).rows,
      ).toEqual([]);
      await expect(
        database.execute("INSERT INTO receipts VALUES (1, 2, 'duplicate')"),
      ).rejects.toThrow("receipts.(shop, receipt)");

      expect((await database.compactTable("receipts")).compacted).toBe(true);
      expect(await database.readTable("receipts")).toEqual(visibleRows);
      const bytes = await database.exportSnapshot();
      const restoredStore = await implementation.create();
      const restored = new MinnowDatabase(restoredStore, { autoCompact: false });
      await restored.importSnapshot(bytes);

      expect(await restored.readTable("receipts")).toEqual(visibleRows);
      expect((await restored.query("SELECT * FROM receipts ORDER BY shop, receipt")).rows).toEqual(
        visibleRows,
      );
      await expect(
        restored.readTable("receipts", { columns: ["\0minnow_primary_key"] }),
      ).rejects.toThrow("Unknown column");
      expect(
        (await restored.query("SELECT * FROM receipts WHERE MATCH(*) AGAINST 'bff*'")).rows,
      ).toEqual([]);
      await expect(
        restored.execute("INSERT INTO receipts VALUES (1, 2, 'duplicate')"),
      ).rejects.toThrow("receipts.(shop, receipt)");
      await expect(
        restored.execute("UPDATE receipts SET receipt = 3 WHERE shop = 1"),
      ).rejects.toThrow("Primary key column cannot be updated");
      const physical = await restoredStore.getTableByName("receipts");
      expect(physical?.columns.filter(({ hidden }) => hidden === true)).toHaveLength(1);
      expect(physical?.primaryKeyColumnIds).toHaveLength(2);

      await restored.close();
      await database.close();
      restoredStore.close();
      store.close();
    });
  });
}

it("reports unsupported storage diagnostics explicitly for custom stores", async () => {
  const store = new MemoryBlockStore();
  Object.defineProperties(store, {
    checkIntegrity: { configurable: true, value: undefined },
    getStorageStats: { configurable: true, value: undefined },
    inspectInterruptedImport: { configurable: true, value: undefined },
    abortInterruptedImport: { configurable: true, value: undefined },
  });
  const database = new MinnowDatabase(store);
  await expect(database.checkIntegrity()).rejects.toThrow("cannot check integrity");
  await expect(database.storageStats()).rejects.toThrow("cannot report storage stats");
  await expect(database.inspectInterruptedImport()).resolves.toBeNull();
  await expect(database.abortInterruptedImport("missing")).rejects.toThrow(
    "cannot abort interrupted imports",
  );
  await database.close();
});

describe("table-scoped segment catalog reads", () => {
  it("does not scan unrelated segment history for an explicit-version join", async () => {
    const store = new CountingMemoryBlockStore();
    const database = new MinnowDatabase(store, { autoCompact: false, autoCollect: false });
    await database.execute("CREATE TABLE scoped_left (id INTEGER PRIMARY KEY, value INTEGER)");
    await database.execute("CREATE TABLE scoped_right (id INTEGER PRIMARY KEY, label TEXT)");
    await database.execute("INSERT INTO scoped_left VALUES (1, 10)");
    await database.execute("INSERT INTO scoped_right VALUES (1, 'one')");
    for (let index = 0; index < 32; index += 1) {
      const name = `unrelated_${String(index)}`;
      await database.execute(`CREATE TABLE ${name} (id INTEGER PRIMARY KEY)`);
      await database.execute(`INSERT INTO ${name} VALUES (${String(index)})`);
    }
    const left = await store.getTableByName("scoped_left");
    const right = await store.getTableByName("scoped_right");
    const version = await store.getCurrentManifestVersion();
    if (left === undefined || right === undefined || version === null) {
      throw new Error("Scoped segment test setup failed");
    }
    store.globalSegmentListCalls = 0;
    store.scopedSegmentTableIds = [];

    const result = await database.query(
      "SELECT l.value, r.label FROM scoped_left l JOIN scoped_right r ON r.id = l.id",
      { version },
    );

    expect(result.rows).toEqual([{ value: 10, label: "one" }]);
    expect(store.globalSegmentListCalls).toBe(0);
    expect(new Set(store.scopedSegmentTableIds)).toEqual(new Set([left.id, right.id]));
    await database.close();
  });
});

it("renews a durable snapshot pin while one bounded write batch idles process-local", async () => {
  const store = new MemoryBlockStore();
  const transactionRenewals = vi.spyOn(store, "renewTransaction");
  const leaseRenewals = vi.spyOn(store, "renewLease");
  const database = new MinnowDatabase(store, {
    transactionOwnerLeaseMs: 30,
    autoCompact: false,
    autoCollect: false,
  });
  await database.createTable({
    name: "idle_scope_rows",
    columns: [{ name: "value", type: "number" }],
  });
  let staged!: () => void;
  const didStage = new Promise<void>((resolve) => {
    staged = resolve;
  });
  let release!: () => void;
  const idle = new Promise<void>((resolve) => {
    release = resolve;
  });
  const write = database.write(async (session) => {
    await session.insertBatch("idle_scope_rows", [{ value: 1 }]);
    staged();
    await idle;
  });
  await didStage;
  await new Promise((resolve) => setTimeout(resolve, 80));
  expect(transactionRenewals).not.toHaveBeenCalled();
  expect(leaseRenewals.mock.calls.length).toBeGreaterThanOrEqual(2);
  expect(await transactionRecords(store)).toEqual([]);
  expect(await store.listLeases()).toHaveLength(1);
  release();
  await write;
  expect((await transactionRecords(store)).map((record) => record.status)).toEqual(["committed"]);
  await vi.waitFor(async () => expect(await store.listLeases()).toEqual([]));
  expect(await database.readTable("idle_scope_rows")).toEqual([{ value: 1 }]);
  await database.close();
});

it("applies the database memory default to ordinary, transactional, and mutation queries", async () => {
  const store = new MemoryBlockStore();
  const pageWrites = vi.spyOn(store, "putTempRunPages");
  const database = new MinnowDatabase(store, {
    rowsPerBlock: 256,
    compression: "raw",
    executionMemoryBudgetBytes: 280_000,
    autoCompact: false,
    autoCollect: false,
  });
  await database.createTable({
    name: "default_budget_rows",
    uniqueKey: "id",
    columns: [
      { name: "id", type: "number" },
      { name: "bucket", type: "number" },
    ],
  });
  await database.insertBatch("default_budget_rows", {
    columns: {
      id: Array.from({ length: 5_000 }, (_, index) => index),
      bucket: Array.from({ length: 5_000 }, (_, index) => index % 17),
    },
  });
  const sql = "SELECT id, bucket FROM default_budget_rows ORDER BY bucket, id DESC";
  const ordinary = await database.query(sql, { memoize: false, spillPageRows: 64 });
  expect(ordinary.rows).toHaveLength(5_000);
  // The streamed form fits under the database default and therefore stays on the radix path;
  // having a finite default is not itself a reason to write durable spill pages.
  expect(pageWrites).not.toHaveBeenCalled();

  pageWrites.mockClear();
  await database.execute("BEGIN");
  const transactional = await database.query(sql, { memoize: false });
  expect(transactional.rows).toEqual(ordinary.rows);
  expect(pageWrites).toHaveBeenCalled();
  await database.execute("ROLLBACK");

  const before = await database.query("SELECT bucket FROM default_budget_rows WHERE id = 1");
  const constrained = new MinnowDatabase(store, {
    executionMemoryBudgetBytes: 1,
    autoCompact: false,
    autoCollect: false,
  });
  await expect(
    constrained.execute("UPDATE default_budget_rows SET bucket = bucket + 1 WHERE id = 1"),
  ).rejects.toBeInstanceOf(QueryMemoryBudgetError);
  expect(await database.query("SELECT bucket FROM default_budget_rows WHERE id = 1")).toEqual(
    before,
  );
  await constrained.close();
  await database.close();
});

it("supports per-query budget overrides and an explicit unbounded database opt-out", async () => {
  const store = new MemoryBlockStore();
  const bounded = new MinnowDatabase(store, {
    executionMemoryBudgetBytes: 1,
    autoCompact: false,
    autoCollect: false,
  });
  await bounded.createTable({
    name: "budget_override_rows",
    columns: [{ name: "value", type: "number" }],
  });
  await bounded.insertBatch("budget_override_rows", {
    columns: { value: Array.from({ length: 1_000 }, (_, index) => index) },
  });
  await expect(
    bounded.query("SELECT value FROM budget_override_rows", { memoize: false }),
  ).rejects.toBeInstanceOf(QueryMemoryBudgetError);
  expect(
    (
      await bounded.query("SELECT value FROM budget_override_rows", {
        memoize: false,
        executionMemoryBudgetBytes: 256_000,
      })
    ).rows,
  ).toHaveLength(1_000);

  await bounded.execute("BEGIN");
  await expect(
    bounded.query("SELECT value FROM budget_override_rows", { memoize: false }),
  ).rejects.toBeInstanceOf(QueryMemoryBudgetError);
  expect(
    (
      await bounded.query("SELECT value FROM budget_override_rows", {
        memoize: false,
        executionMemoryBudgetBytes: 256_000,
      })
    ).rows,
  ).toHaveLength(1_000);
  await bounded.execute("ROLLBACK");
  await bounded.close();

  const unbounded = new MinnowDatabase(store, {
    executionMemoryBudgetBytes: null,
    autoCompact: false,
    autoCollect: false,
  });
  expect(
    (await unbounded.query("SELECT value FROM budget_override_rows", { memoize: false })).rows,
  ).toHaveLength(1_000);
  await unbounded.close();
});

it("keeps a full numeric sort in memory when it fits the default query budget", async () => {
  const store = new MemoryBlockStore();
  const pageWrites = vi.spyOn(store, "putTempRunPages");
  const ownerCreates = vi.spyOn(store, "createTempOwner");
  const database = new MinnowDatabase(store, {
    rowsPerBlock: 512,
    compression: "raw",
    autoCompact: false,
    autoCollect: false,
  });
  await database.createTable({
    name: "fitting_sort_rows",
    columns: [
      { name: "id", type: "number" },
      { name: "amount", type: "number" },
    ],
  });
  const rowCount = 20_000;
  await database.insertBatch("fitting_sort_rows", {
    columns: {
      id: Array.from({ length: rowCount }, (_, index) => index),
      amount: Array.from({ length: rowCount }, (_, index) => (index * 7_919) % 1_000),
    },
  });

  const result = await database.query(
    "SELECT id, amount FROM fitting_sort_rows ORDER BY amount, id",
    { memoize: false },
  );

  expect(result.rows).toHaveLength(rowCount);
  expect(result.rows[0]).toEqual({ id: 0, amount: 0 });
  expect(result.rows.at(-1)).toEqual({ id: 19_321, amount: 999 });
  let ordered = true;
  for (let index = 1; index < result.rows.length; index += 1) {
    const previous = result.rows[index - 1];
    const current = result.rows[index];
    if (
      Number(previous?.amount) > Number(current?.amount) ||
      (previous?.amount === current?.amount && Number(previous?.id) >= Number(current?.id))
    ) {
      ordered = false;
      break;
    }
  }
  expect(ordered).toBe(true);
  expect(ownerCreates).not.toHaveBeenCalled();
  expect(pageWrites).not.toHaveBeenCalled();
  await database.close();
});

it("invalidates and reclaims a persistently failing full-text delta fold, then rebuilds", async () => {
  const store = new PersistentPostingBuildFaultStore();
  const database = new MinnowDatabase(store, {
    rowsPerBlock: 2,
    compression: "raw",
    autoCompact: false,
    autoCollect: false,
  });
  await database.createTable({
    name: "tail_articles",
    columns: [{ name: "body", type: "string" }],
  });
  await database.insert("tail_articles", { body: "seed" });
  await database.buildFtsIndex("tail_articles", "body");
  store.failFtsBuilds = true;
  store.failPostingInvalidations = true;
  for (let commit = 0; commit < 80 && store.postingInvalidationAttempts === 0; commit += 1) {
    await database.insert("tail_articles", { body: `later ${String(commit)}` });
    await new Promise((resolve) => setTimeout(resolve, 0));
  }
  expect(store.postingInvalidationAttempts).toBeGreaterThan(0);
  // Let the failed background attempt release its in-flight key. The hard-tail marker must
  // survive so exactly one later commit is enough to trigger a successful retry.
  await new Promise((resolve) => setTimeout(resolve, 2));
  store.failPostingInvalidations = false;
  await database.insert("tail_articles", { body: "later retry" });
  let table = await store.getTableByName("tail_articles");
  const column = table?.columns[0];
  if (table === undefined || column === undefined) throw new Error("FTS table is missing");
  for (
    let attempt = 0;
    attempt < 500 && table.ftsColumns?.[column.id]?.state !== "invalid";
    attempt += 1
  ) {
    await new Promise((resolve) => setTimeout(resolve, 2));
    table = await store.getTableByName("tail_articles");
    if (table === undefined) throw new Error("FTS table disappeared");
  }
  expect(table.ftsColumns?.[column.id]?.state).toBe("invalid");
  const failedTail = await store.readFtsCandidates(
    table.id,
    column.id,
    [{ term: "later", prefix: false }],
    (await store.getCurrentManifestVersion()) ?? -1,
  );
  expect(failedTail.hasBase).toBe(false);
  expect(failedTail.deltaChunkCount).toBe(0);
  expect(
    (
      await database.query(
        "SELECT COUNT(*) AS n FROM tail_articles WHERE MATCH(body) AGAINST 'later'",
        { memoize: false },
      )
    ).rows,
  ).toEqual([{ n: (await database.readTable("tail_articles")).length - 1 }]);

  store.failFtsBuilds = false;
  for (let attempt = 0; attempt < 500; attempt += 1) {
    await database.query(
      "SELECT COUNT(*) AS n FROM tail_articles WHERE MATCH(body) AGAINST 'later'",
      { memoize: false },
    );
    table = await store.getTableByName("tail_articles");
    if (table?.ftsColumns?.[column.id]?.state === "ready") break;
    await new Promise((resolve) => setTimeout(resolve, 2));
  }
  expect(table?.ftsColumns?.[column.id]?.state).toBe("ready");
  await database.close();
});

it("aborts a failed streamed full-text build and remains scan-correct", async () => {
  const store = new PersistentPostingBuildFaultStore();
  const database = new MinnowDatabase(store, {
    rowsPerBlock: 1,
    compression: "raw",
    autoCompact: false,
    autoCollect: false,
  });
  await database.createTable({
    name: "stream_build_fault",
    columns: [{ name: "body", type: "string" }],
  });
  await database.insertBatch(
    "stream_build_fault",
    Array.from({ length: 64 }, (_, index) => ({ body: `needle ${String(index)}` })),
  );

  store.failFtsBuilds = true;
  await expect(database.buildFtsIndex("stream_build_fault", "body")).rejects.toThrow(
    "injected streamed full-text build failure",
  );
  const failed = await store.getTableByName("stream_build_fault");
  const column = failed?.columns[0];
  if (failed === undefined || column === undefined) throw new Error("FTS fixture is missing");
  expect(failed.ftsColumns?.[column.id]?.state).toBe("invalid");
  expect(
    await store.readFtsCandidates(
      failed.id,
      column.id,
      [{ term: "needle", prefix: false }],
      (await store.getCurrentManifestVersion()) ?? -1,
    ),
  ).toMatchObject({ hasBase: false, deltaChunkCount: 0 });
  expect(
    (
      await database.query(
        "SELECT COUNT(*) AS n FROM stream_build_fault WHERE MATCH(body) AGAINST 'needle'",
        { memoize: false },
      )
    ).rows,
  ).toEqual([{ n: 64 }]);

  store.failFtsBuilds = false;
  await database.buildFtsIndex("stream_build_fault", "body");
  expect((await store.getTableByName("stream_build_fault"))?.ftsColumns?.[column.id]?.state).toBe(
    "ready",
  );
  await database.close();
});

it("falls back to an exact scan when a postings candidate read reaches its memory fuse", async () => {
  const store = new OverflowingFtsCandidateStore();
  const database = new MinnowDatabase(store, {
    rowsPerBlock: 2,
    compression: "raw",
    autoCompact: false,
    autoCollect: false,
  });
  await database.createTable({
    name: "candidate_fuse",
    columns: [{ name: "body", type: "string" }],
  });
  await database.insertBatch(
    "candidate_fuse",
    Array.from({ length: 20 }, (_, index) => ({
      body: index % 2 === 0 ? `common ${String(index)}` : `other ${String(index)}`,
    })),
  );
  await database.buildFtsIndex("candidate_fuse", "body");

  expect(
    (
      await database.query(
        "SELECT COUNT(*) AS n FROM candidate_fuse WHERE MATCH(body) AGAINST 'common'",
        { memoize: false },
      )
    ).rows,
  ).toEqual([{ n: 10 }]);
  expect(store.candidateReads).toBeGreaterThan(0);
  await database.close();
});

it("adaptively splits a storage-refused block batch without changing query results", async () => {
  const store = new TinyBlockReadBatchStore();
  const database = new MinnowDatabase(store, {
    rowsPerBlock: 1,
    compression: "raw",
    autoCompact: false,
    autoCollect: false,
  });
  await database.createTable({
    name: "split_block_reads",
    columns: [{ name: "value", type: "number" }],
  });
  await database.insertBatch(
    "split_block_reads",
    Array.from({ length: 8 }, (_, value) => ({ value })),
  );
  expect((await database.query("SELECT value FROM split_block_reads ORDER BY value")).rows).toEqual(
    Array.from({ length: 8 }, (_, value) => ({ value })),
  );
  expect(store.largestAttempt).toBeGreaterThan(2);
  await database.close();
});

it("releases posting-tail retry markers when their table is dropped", async () => {
  const store = new PersistentPostingBuildFaultStore();
  const database = new MinnowDatabase(store, {
    rowsPerBlock: 2,
    compression: "raw",
    autoCompact: false,
    autoCollect: false,
  });
  await database.createTable({
    name: "temporary_articles",
    columns: [{ name: "body", type: "string" }],
  });
  await database.insert("temporary_articles", { body: "seed" });
  await database.buildFtsIndex("temporary_articles", "body");
  store.failFtsBuilds = true;
  store.failPostingInvalidations = true;
  for (
    let commit = 0;
    commit < 80 && database.maintenanceStatus().postingDeltaTailMarkers === 0;
    commit += 1
  ) {
    await database.insert("temporary_articles", { body: `later ${String(commit)}` });
    await new Promise((resolve) => setTimeout(resolve, 0));
  }
  expect(database.maintenanceStatus().postingDeltaTailMarkers).toBeGreaterThan(0);
  await database.dropTable("temporary_articles");
  expect(database.maintenanceStatus().postingDeltaTailMarkers).toBe(0);
  await database.close();
});

it("invalidates and reclaims a failing scalar-index fold without weakening UNIQUE state", async () => {
  const store = new PersistentPostingBuildFaultStore();
  const database = new MinnowDatabase(store, {
    rowsPerBlock: 2,
    compression: "raw",
    autoCompact: false,
    autoCollect: false,
  });
  await database.createTable({
    name: "tail_values",
    uniqueKey: "id",
    columns: [
      { name: "id", type: "number" },
      { name: "value", type: "number" },
    ],
  });
  await database.insert("tail_values", { id: 0, value: 0 });
  await database.createIndex("tail_value_idx", "tail_values", "value", { unique: true });
  store.failSecondaryBuilds = true;
  for (let commit = 1; commit <= 40; commit += 1) {
    await database.insert("tail_values", { id: commit, value: commit });
  }
  let table = await store.getTableByName("tail_values");
  const indexEntry = Object.entries(table?.secondaryIndexes ?? {})[0];
  if (table === undefined || indexEntry === undefined) throw new Error("Scalar index is missing");
  const [indexId, index] = indexEntry;
  for (
    let attempt = 0;
    attempt < 500 && table.secondaryIndexes?.[indexId]?.state !== "invalid";
    attempt += 1
  ) {
    await new Promise((resolve) => setTimeout(resolve, 2));
    table = await store.getTableByName("tail_values");
    if (table === undefined) throw new Error("Scalar table disappeared");
  }
  expect(table.secondaryIndexes?.[indexId]).toMatchObject({
    state: "invalid",
    unique: true,
    uniqueEnforced: true,
  });
  const failedTail = await store.readFtsCandidates(
    table.id,
    index.storageColumnId,
    [],
    (await store.getCurrentManifestVersion()) ?? -1,
  );
  expect(failedTail.hasBase).toBe(false);
  expect(failedTail.deltaChunkCount).toBe(0);
  expect(
    (await database.query("SELECT id FROM tail_values WHERE value = 37", { memoize: false })).rows,
  ).toEqual([{ id: 37 }]);
  await expect(database.insert("tail_values", { id: 99, value: 37 })).rejects.toBeInstanceOf(
    UniqueConstraintError,
  );

  store.failSecondaryBuilds = false;
  for (let attempt = 0; attempt < 500; attempt += 1) {
    await database.query("SELECT id FROM tail_values WHERE value = 37", { memoize: false });
    table = await store.getTableByName("tail_values");
    if (table?.secondaryIndexes?.[indexId]?.state === "ready") break;
    await new Promise((resolve) => setTimeout(resolve, 2));
  }
  expect(table?.secondaryIndexes?.[indexId]).toMatchObject({
    state: "ready",
    uniqueEnforced: true,
  });
  await database.close();
});

interface MutationCompactionFixture {
  database: MinnowDatabase;
  tableId: string;
  snapshots: Array<{ version: number; rows: DatabaseRow[] }>;
  expectedRows: DatabaseRow[];
  expectedRowIds: bigint[];
  sourceBlockIds: string[];
  sourceSegmentIds: string[];
}

async function createMutationCompactionFixture(
  store: BlockStore,
  tableName = "mutation_accounts",
): Promise<MutationCompactionFixture> {
  const database = new MinnowDatabase(store, { compression: "raw", rowsPerBlock: 2 });
  await database.createTable({
    name: tableName,
    uniqueKey: "email",
    columns: [
      { name: "email", type: "string" },
      { name: "score", type: "number", nullable: true },
      { name: "active", type: "boolean" },
      { name: "note", type: "string", nullable: true },
    ],
  });

  const snapshots: Array<{ version: number; rows: DatabaseRow[] }> = [];
  const remember = async (version: number | null): Promise<void> => {
    if (version === null) throw new Error("Expected a committed mutation version");
    snapshots.push({ version, rows: await database.readTable(tableName, version) });
  };

  const inserted = await database.insertBatch(tableName, {
    columns: {
      email: ["a@example.com", "b@example.com", "c@example.com"],
      score: [1, 2, 3],
      active: [true, true, false],
      note: ["a", "b", null],
    },
  });
  await remember(inserted.version);
  const upserted = await database.upsertBatch(tableName, {
    columns: {
      email: ["b@example.com", "d@example.com"],
      score: [20, 4],
      active: [false, true],
      note: ["b2", "d"],
    },
  });
  await remember(upserted.version);
  const updated = await database.updateBatch(tableName, {
    keys: ["b@example.com", "d@example.com"],
    changes: { score: [21, 40], note: [null, "d2"] },
  });
  await remember(updated.version);
  const deletedA = await database.deleteBatch(tableName, {
    keys: ["a@example.com", "missing@example.com"],
  });
  await remember(deletedA.version);
  const resurrectedA = await database.upsert(tableName, {
    email: "a@example.com",
    score: 10,
    active: false,
    note: "a2",
  });
  await remember(resurrectedA.version);
  const deletedC = await database.deleteBatch(tableName, { keys: ["c@example.com"] });
  await remember(deletedC.version);

  const table = await store.getTableByName(tableName);
  if (table === undefined) throw new Error(`Expected mutation table ${tableName}`);
  const initialSegment = await requiredSegment(store, inserted.segmentId);
  const upsertSegment = await requiredSegment(store, upserted.segmentId ?? "");
  const resurrectionSegment = await requiredSegment(store, resurrectedA.segmentId ?? "");
  const initialRowIds = expandSegmentRowIds(initialSegment);
  const upsertRowIds = expandSegmentRowIds(upsertSegment);
  const resurrectionRowIds = expandSegmentRowIds(resurrectionSegment);
  const expectedRowIds = [
    requiredItem(initialRowIds, 1, "initial B row ID"),
    requiredItem(upsertRowIds, 1, "upserted D row ID"),
    requiredItem(resurrectionRowIds, 0, "resurrected A row ID"),
  ];
  expect(expectedRowIds[0]).not.toBe(upsertRowIds[0]);

  const visibleSourceIds = (await allVisibleSegments(database, tableName)).map(
    (segment) => segment.id,
  );
  const sourceSegments = await Promise.all(
    visibleSourceIds.map((segmentId) => requiredSegment(store, segmentId)),
  );
  const sourceBlockIds = [
    ...new Set(sourceSegments.flatMap((segment) => Object.values(segment.columnBlockIds).flat())),
  ].sort();
  const expectedRows: DatabaseRow[] = [
    { email: "b@example.com", score: 21, active: false, note: null },
    { email: "d@example.com", score: 40, active: true, note: "d2" },
    { email: "a@example.com", score: 10, active: false, note: "a2" },
  ];
  expect(await database.readTable(tableName)).toEqual(expectedRows);

  return {
    database,
    tableId: table.id,
    snapshots,
    expectedRows,
    expectedRowIds,
    sourceBlockIds,
    sourceSegmentIds: visibleSourceIds,
  };
}

async function requiredSegment(store: BlockStore, segmentId: string): Promise<SegmentRecord> {
  const segment = await store.getSegment(segmentId);
  if (segment === undefined) throw new Error(`Expected segment ${segmentId}`);
  return segment;
}

interface CompactionSourceStats {
  blockIds: string[];
  storedBytes: number;
}

async function compactionSourceStats(
  store: BlockStore,
  segmentIds: readonly string[],
): Promise<CompactionSourceStats> {
  const segments = await Promise.all(
    segmentIds.map((segmentId) => requiredSegment(store, segmentId)),
  );
  const blockIds = [
    ...new Set(segments.flatMap((segment) => Object.values(segment.columnBlockIds).flat())),
  ].sort();
  const blocks = await store.getBlocks(blockIds);
  let storedBytes = 0;
  for (const [index, bytes] of blocks.entries()) {
    if (bytes === undefined) {
      throw new Error(`Expected compaction source block ${blockIds[index] ?? ""}`);
    }
    storedBytes += bytes.byteLength;
  }
  return { blockIds, storedBytes };
}

async function assertPersistedCompactionSelection(
  store: BlockStore,
  jobId: string,
  sourceSegmentIds: readonly string[],
  anchorSegmentId: string | null,
): Promise<CompactionSourceStats> {
  const job = await store.getCompactionJob(jobId);
  if (job === undefined) throw new Error(`Expected compaction job ${jobId}`);
  const sourceStats = await compactionSourceStats(store, sourceSegmentIds);
  const level0SourceIds = anchorSegmentId === null ? sourceSegmentIds : sourceSegmentIds.slice(1);
  const level0Stats = await compactionSourceStats(store, level0SourceIds);
  const anchorStats =
    anchorSegmentId === null
      ? { blockIds: [], storedBytes: 0 }
      : await compactionSourceStats(store, [anchorSegmentId]);
  expect(job).toMatchObject({
    sourceSegmentIds: [...sourceSegmentIds],
    sourceStoredBytes: sourceStats.storedBytes,
    level0SourceStoredBytes: level0Stats.storedBytes,
    anchorSourceStoredBytes: anchorStats.storedBytes,
  });
  expect([...job.sourceBlockIds].sort()).toEqual(sourceStats.blockIds);
  expect(job.sourceStoredBytes).toBe(level0Stats.storedBytes + anchorStats.storedBytes);
  return sourceStats;
}

function requiredItem<T>(values: readonly T[], index: number, label: string): T {
  const value = values[index];
  if (value === undefined) throw new Error(`Expected ${label}`);
  return value;
}

function expandSegmentRowIds(segment: SegmentRecord): bigint[] {
  const spans =
    segment.rowIdSpans.length === 0 && segment.rowCount > 0
      ? [{ rowStart: 0, rowCount: segment.rowCount, rowIdStart: segment.rowIdStart }]
      : segment.rowIdSpans;
  const rowIds: Array<bigint | undefined> = Array.from({ length: segment.rowCount });
  for (const span of spans) {
    for (let offset = 0; offset < span.rowCount; offset += 1) {
      rowIds[span.rowStart + offset] = span.rowIdStart + BigInt(offset);
    }
  }
  if (rowIds.some((rowId) => rowId === undefined)) {
    throw new Error(`Row-ID spans do not cover segment ${segment.id}`);
  }
  return rowIds as bigint[];
}

function canonicalRowIdSpans(rowIds: readonly bigint[]): RowIdSpan[] {
  const spans: Array<{ rowStart: number; rowCount: number; rowIdStart: bigint }> = [];
  for (const [rowStart, rowId] of rowIds.entries()) {
    const previous = spans.at(-1);
    if (previous !== undefined && previous.rowIdStart + BigInt(previous.rowCount) === rowId) {
      previous.rowCount += 1;
    } else {
      spans.push({ rowStart, rowCount: 1, rowIdStart: rowId });
    }
  }
  return spans;
}

function appendOnlyCompactionEligible(segment: SegmentRecord): boolean {
  return segment.kind === "insert";
}

async function assertPublishedMutationMerge(
  store: BlockStore,
  database: MinnowDatabase,
  tableName: string,
  jobId: string,
  expectedRows: readonly DatabaseRow[],
  expectedRowIds: readonly bigint[],
): Promise<SegmentRecord> {
  const job = await store.getCompactionJob(jobId);
  if (job === undefined) throw new Error(`Expected compaction job ${jobId}`);
  if (job.rewritePlan.kind !== "merge-v1") throw new Error("Expected a merge-v1 plan");
  expect(job).toMatchObject({ state: "published", processedRows: expectedRows.length });
  expect(new Set(job.outputBlockIds).size).toBe(job.outputBlockIds.length);
  expect(job.rewritePlan).toMatchObject({
    totalRows: expectedRows.length,
    rowIdSpans: canonicalRowIdSpans(expectedRowIds),
  });
  if (job.outputSegmentId === null) throw new Error("Expected a merged output segment");
  const output = await requiredSegment(store, job.outputSegmentId);
  const table = await store.getTableByName(tableName);
  if (table === undefined) throw new Error(`Expected table ${tableName}`);
  const minimumRowId = expectedRowIds.reduce((minimum, rowId) =>
    rowId < minimum ? rowId : minimum,
  );
  const maximumRowId = expectedRowIds.reduce((maximum, rowId) =>
    rowId > maximum ? rowId : maximum,
  );
  expect(output).toMatchObject({
    tableId: table.id,
    kind: "base",
    keyColumnId: table.uniqueKeyColumnId,
    level: 1,
    rowCount: expectedRows.length,
    rowIdStart: minimumRowId,
    rowIdEndExclusive: maximumRowId + 1n,
    rowIdSpans: canonicalRowIdSpans(expectedRowIds),
  });
  expect(Object.keys(output.columnBlockIds).sort()).toEqual(
    table.columns.map((column) => column.id).sort(),
  );
  expect(expandSegmentRowIds(output)).toEqual(expectedRowIds);
  expect(await database.readTable(tableName)).toEqual(expectedRows);
  expect(await allVisibleSegments(database, tableName)).toHaveLength(1);
  return output;
}

interface MutationRebaseGuardFixture {
  database: MinnowDatabase;
  table: TableRecord;
  job: CompactionJobRecord;
  expectedRows: DatabaseRow[];
}

async function createMutationRebaseGuardFixture(
  store: MemoryBlockStore,
  tableName: string,
): Promise<MutationRebaseGuardFixture> {
  const database = new MinnowDatabase(store, { compression: "raw" });
  await database.createTable({
    name: tableName,
    uniqueKey: "email",
    columns: [
      { name: "email", type: "string" },
      { name: "score", type: "number" },
    ],
  });
  await database.insert(tableName, { email: "a@example.com", score: 1 });
  await database.insert(tableName, { email: "b@example.com", score: 2 });
  await database.update(tableName, "a@example.com", { score: 10 });
  const expectedRows: DatabaseRow[] = [
    { email: "a@example.com", score: 10 },
    { email: "b@example.com", score: 2 },
  ];
  expect(await database.readTable(tableName)).toEqual(expectedRows);

  const progress = await database.compactTableStep(tableName, {
    maxBlocks: 1,
    targetBlockBytes: 64,
    outputCompression: "raw",
  });
  if (progress.jobId === null) throw new Error("Expected a guarded mutation compaction job");
  expect(progress).toMatchObject({ state: "running", outputBlockCount: 1, result: null });
  const job = await store.getCompactionJob(progress.jobId);
  if (job?.rewritePlan.kind !== "merge-v1") throw new Error("Expected a merge-v1 guard plan");
  const table = await store.getTableByName(tableName);
  if (table === undefined) throw new Error(`Expected guard table ${tableName}`);
  return { database, table, job, expectedRows };
}

async function commitLowLevelDeleteSegment(
  store: MemoryBlockStore,
  table: TableRecord,
  input: {
    segmentId: string;
    logicalOrder: number;
    blockId?: string;
    key?: string;
  },
): Promise<{ manifestVersion: number; segmentId: string; blockId: string }> {
  const keyColumn = table.columns.find((column) => column.id === table.uniqueKeyColumnId);
  if (keyColumn?.type !== "string") {
    throw new Error("Expected a string-key guard table");
  }
  const manager = new TransactionManager(store, {
    createId: () => `${input.segmentId}/transaction`,
  });
  const transaction = await manager.begin();
  const blockId = input.blockId ?? `${input.segmentId}/key-block`;
  if (input.blockId === undefined) {
    await transaction.stageBlock(
      blockId,
      await encodeBlock({ type: "string", values: [input.key ?? "missing@example.com"] }, "raw"),
    );
  }
  const bytes = await store.getBlock(blockId);
  if (bytes === undefined) throw new Error(`Expected guard block ${blockId}`);
  const rowCount = inspectBlock(bytes).rowCount;
  await transaction.stageSegment({
    id: input.segmentId,
    tableId: table.id,
    transactionId: transaction.id,
    rowCount,
    rowIdStart: 0n,
    rowIdEndExclusive: 0n,
    columnBlockIds: { [keyColumn.id]: [blockId] },
    kind: "delete",
    keyColumnId: keyColumn.id,
    level: 0,
    logicalOrder: input.logicalOrder,
    commitOrdinal: 0,
    rowIdSpans: [],
    createdAt: "2026-01-01T00:00:00.000Z",
  });
  const manifest = await transaction.commit();
  return { manifestVersion: manifest.version, segmentId: input.segmentId, blockId };
}

async function commitLowLevelNumberSegment(
  store: MemoryBlockStore,
  table: TableRecord,
  input: {
    segmentId: string;
    level: number;
    partitionOrdinal?: number;
    logicalOrder: number;
    rowId: bigint;
    value: number;
  },
): Promise<void> {
  const column = requiredItem(table.columns, 0, "low-level number column");
  if (column.type !== "number") throw new Error("Expected a number layout-test column");
  const manager = new TransactionManager(store, {
    createId: () => `${input.segmentId}/transaction`,
  });
  const transaction = await manager.begin();
  const blockId = `${input.segmentId}/block`;
  await transaction.stageBlock(
    blockId,
    await encodeBlock({ type: "number", values: [input.value] }, "raw"),
  );
  await transaction.stageSegment({
    id: input.segmentId,
    tableId: table.id,
    transactionId: transaction.id,
    rowCount: 1,
    rowIdStart: input.rowId,
    rowIdEndExclusive: input.rowId + 1n,
    columnBlockIds: { [column.id]: [blockId] },
    kind: "insert",
    level: input.level,
    ...(input.partitionOrdinal === undefined ? {} : { partitionOrdinal: input.partitionOrdinal }),
    logicalOrder: input.logicalOrder,
    commitOrdinal: 0,
    rowIdSpans: [],
    createdAt: "2026-01-01T00:00:00.000Z",
  });
  await transaction.commit();
}

it("retries only a typed transient table-use race without repeating block retirement", async () => {
  const store = new TransientTableInUseMemoryBlockStore();
  const database = new MinnowDatabase(store, { maxCommitRetries: 2 });
  await database.createTable({
    name: "notes",
    columns: [{ name: "value", type: "number" }],
  });
  await database.insert("notes", { value: 1 });
  const versionBeforeDrop = await store.getCurrentManifestVersion();

  await expect(database.dropTable("notes")).resolves.toBe(true);

  expect(store.dropTableCalls).toBe(2);
  expect(await store.getCurrentManifestVersion()).toBe((versionBeforeDrop ?? -1) + 1);
  expect(await store.getTableByName("notes")).toBeUndefined();
  store.close();
});

it("keeps the old view intact when an atomic replacement is refused", async () => {
  const store = new RefusingViewUpdateMemoryBlockStore();
  const database = new MinnowDatabase(store);
  await database.createView("answer", "SELECT 1 AS value");
  const before = await store.getTableByName("answer");
  store.refuseNextViewUpdate = true;

  await expect(
    database.createView("answer", "SELECT 2 AS value", { orReplace: true }),
  ).rejects.toMatchObject({ name: "QuotaExceededError" });

  expect(await store.getTableByName("answer")).toEqual(before);
  expect((await database.query("SELECT value FROM answer")).rows).toEqual([{ value: 1 }]);
  store.close();
});

it("preserves a concurrent view update when OR REPLACE loses its catalog CAS", async () => {
  const store = new RacingViewUpdateMemoryBlockStore();
  const database = new MinnowDatabase(store);
  await database.createView("answer", "SELECT 1 AS value");
  store.raceNextViewUpdate = true;

  await expect(
    database.createView("answer", "SELECT 2 AS value", { orReplace: true }),
  ).rejects.toBeInstanceOf(TableRecordConflictError);

  expect((await database.query("SELECT value FROM answer")).rows).toEqual([{ value: 7 }]);
  store.close();
});

it("keeps dependent views valid across replace and drop DDL", async () => {
  const store = new MemoryBlockStore();
  const database = new MinnowDatabase(store);
  await database.execute("CREATE TABLE source_values (value INTEGER)");
  await database.execute("INSERT INTO source_values VALUES (-1), (1)");
  await database.createView("source_view", "SELECT value FROM source_values");
  await database.createView("dependent_view", "SELECT value FROM source_view");

  await database.createView("source_view", "SELECT value FROM source_values WHERE value > 0", {
    orReplace: true,
  });
  expect((await database.query("SELECT value FROM dependent_view")).rows).toEqual([{ value: 1 }]);
  await expect(
    database.createView("source_view", "SELECT value AS renamed FROM source_values", {
      orReplace: true,
    }),
  ).rejects.toThrow("dependent_view depends on its current schema");
  await expect(database.dropView("source_view")).rejects.toThrow("view dependent_view reads it");
  expect((await database.query("SELECT value FROM dependent_view")).rows).toEqual([{ value: 1 }]);
  store.close();
});

it("rejects view cycles at DDL time and preserves the previous definition", async () => {
  const store = new MemoryBlockStore();
  const database = new MinnowDatabase(store);
  await database.createView("first_view", "SELECT 1 AS value");
  await database.createView("second_view", "SELECT value FROM first_view");

  await expect(
    database.createView("first_view", "SELECT value FROM second_view", { orReplace: true }),
  ).rejects.toThrow("View dependency cycle");
  expect((await database.query("SELECT value FROM first_view")).rows).toEqual([{ value: 1 }]);
  store.close();
});

it("protects stored view schemas when columns are added", async () => {
  const store = new MemoryBlockStore();
  const database = new MinnowDatabase(store);
  await database.execute("CREATE TABLE view_source (id INTEGER)");
  await database.createView("star_view", "SELECT * FROM view_source");
  await expect(database.execute("ALTER TABLE view_source ADD COLUMN note TEXT")).rejects.toThrow(
    "star_view depends on its current schema",
  );
  await database.dropView("star_view");
  await database.createView("explicit_view", "SELECT id FROM view_source");
  await expect(
    database.execute("ALTER TABLE view_source ADD COLUMN note TEXT"),
  ).resolves.toMatchObject({ kind: "add-column" });
  expect((await database.query("SELECT * FROM explicit_view")).columns).toEqual(["id"]);
  store.close();
});

it("refuses trigger ownership or targets that are views", async () => {
  const store = new MemoryBlockStore();
  const database = new MinnowDatabase(store);
  await database.execute("CREATE TABLE trigger_rows (id INTEGER PRIMARY KEY)");
  await database.createView("trigger_view", "SELECT id FROM trigger_rows");
  await expect(
    database.execute(
      "CREATE TRIGGER view_owner AFTER INSERT ON trigger_view BEGIN " +
        "INSERT INTO trigger_rows (id) VALUES (NEW.id); END",
    ),
  ).rejects.toThrow("Cannot create a trigger on view");
  await expect(
    database.execute(
      "CREATE TRIGGER view_target AFTER INSERT ON trigger_rows BEGIN " +
        "INSERT INTO trigger_view (id) VALUES (NEW.id); END",
    ),
  ).rejects.toThrow("Trigger bodies cannot write to views");
  store.close();
});

it("validates CHECK references and one table-wide constraint-name namespace before admission", async () => {
  const store = new MemoryBlockStore();
  const database = new MinnowDatabase(store);

  await expect(
    database.createTable({
      name: "bad_check",
      columns: [{ name: "id", type: "number" }],
      checks: [{ name: "known_columns", sql: "missing > 0" }],
    }),
  ).rejects.toThrow("CHECK known_columns names an unknown column: missing");
  await expect(
    database.createTable({
      name: "cross_table_check",
      columns: [{ name: "id", type: "number" }],
      checks: [{ name: "local_only", sql: "other.id > 0" }],
    }),
  ).rejects.toThrow("CHECK local_only references another table: other.id");
  await expect(
    database.createTable({
      name: "duplicate_constraints",
      uniqueKey: "id",
      columns: [
        { name: "id", type: "number" },
        { name: "parent_id", type: "number" },
      ],
      foreignKeys: [
        {
          name: "same_name",
          column: "parent_id",
          parentTable: "duplicate_constraints",
          parentColumn: "id",
          onDelete: "restrict",
        },
      ],
      checks: [{ name: "same_name", sql: "parent_id >= 0" }],
    }),
  ).rejects.toThrow("Constraint already exists: same_name");

  expect(await store.listTables()).toEqual([]);
  store.close();
});

it("makes concurrent CREATE TABLE/INDEX IF NOT EXISTS converge on one catalog object", async () => {
  const store = new MemoryBlockStore();
  const left = new MinnowDatabase(store);
  const right = new MinnowDatabase(store);

  await Promise.all([
    left.execute("CREATE TABLE IF NOT EXISTS shared_rows (id INTEGER PRIMARY KEY, value TEXT)"),
    right.execute("CREATE TABLE IF NOT EXISTS shared_rows (id INTEGER PRIMARY KEY, value TEXT)"),
  ]);
  expect((await store.listTables()).filter((table) => table.name === "shared_rows")).toHaveLength(
    1,
  );

  await Promise.all([
    left.execute("CREATE INDEX IF NOT EXISTS shared_value_idx ON shared_rows (value)"),
    right.execute("CREATE INDEX IF NOT EXISTS shared_value_idx ON shared_rows (value)"),
  ]);
  const indexes = (await store.listTables()).flatMap((table) =>
    Object.values(table.secondaryIndexes ?? {}).filter(
      (index) => index.name === "shared_value_idx",
    ),
  );
  expect(indexes).toHaveLength(1);
  expect(indexes[0]?.state).toBe("ready");
  store.close();
});

for (const implementation of implementations()) {
  describe(implementation.name, () => {
    it("drops a table and everything keyed to it, leaving the bytes for the collector", async () => {
      const store = await implementation.create();
      const database = new MinnowDatabase(store, { rowsPerBlock: 2 });
      await database.createTable({
        name: "notes",
        uniqueKey: "id",
        columns: [
          { name: "id", type: "number" },
          { name: "body", type: "string" },
        ],
      });
      await database.createTable({
        name: "kept",
        uniqueKey: "id",
        columns: [
          { name: "id", type: "number" },
          { name: "body", type: "string" },
        ],
      });
      await database.insertBatch("notes", { columns: { id: [1, 2, 3], body: ["a", "b", "c"] } });
      await database.insertBatch("kept", { columns: { id: [1], body: ["stays"] } });
      const before = await physicalBlockCount(store);
      const notesId = (await store.getTableByName("notes"))?.id ?? "";
      expect((await tableSegmentRecords(store, notesId)).length).toBeGreaterThan(0);

      expect(await database.dropTable("notes")).toBe(true);

      // The catalog no longer knows the table, and neither does a query.
      expect((await store.listTables()).map((table) => table.name)).toEqual(["kept"]);
      await expect(database.query("SELECT COUNT(*) AS n FROM notes")).rejects.toThrow(
        "Unknown table: notes",
      );
      // Its segments go with it, so nothing points at a table that is not there.
      expect(await tableSegmentRecords(store, notesId)).toEqual([]);
      const remainingTables = await store.listTables();
      const liveIds = new Set(remainingTables.map((table) => table.id));
      expect((await segmentRecords(store)).every((segment) => liveIds.has(segment.tableId))).toBe(
        true,
      );
      // The bytes are retired rather than deleted: still stored until the lease-aware collector
      // reclaims them, which is what keeps a pinned reader's blocks resolvable.
      expect(await physicalBlockCount(store)).toBe(before);
      await database.collectGarbage();
      expect(await physicalBlockCount(store)).toBeLessThan(before);
      // The surviving table is untouched, and the name is free again.
      expect((await database.query("SELECT body FROM kept")).rows).toEqual([{ body: "stays" }]);
      await database.createTable({
        name: "notes",
        columns: [{ name: "different", type: "number" }],
      });
      expect((await database.query("SELECT COUNT(*) AS n FROM notes")).rows).toEqual([{ n: 0 }]);

      expect(await database.dropTable("missing", { ifExists: true })).toBe(false);
      await expect(database.dropTable("missing")).rejects.toThrow("Unknown table: missing");
    });

    it("refuses to drop a table another table's trigger writes to", async () => {
      const store = await implementation.create();
      const database = new MinnowDatabase(store);
      await database.createTable({
        name: "audit",
        columns: [{ name: "note", type: "string" }],
      });
      await database.createTable({
        name: "items",
        uniqueKey: "id",
        columns: [{ name: "id", type: "number" }],
      });
      await database.execute(
        "CREATE TRIGGER items_ins AFTER INSERT ON items BEGIN INSERT INTO audit (note) VALUES ('ins'); END",
      );
      await expect(database.dropTable("audit")).rejects.toThrow(
        "Cannot drop audit: trigger items_ins on items writes to it",
      );
      // The table the trigger belongs to can still go: its triggers leave with it.
      expect(await database.dropTable("items")).toBe(true);
      expect(await database.dropTable("audit")).toBe(true);
    });

    it("creates tables with only simple data types and inserts a column batch", async () => {
      const store = await implementation.create();
      const database = new MinnowDatabase(store, { rowsPerBlock: 2 });
      await database.createTable({
        name: "people",
        columns: [
          { name: "active", type: "boolean" },
          { name: "score", type: "number" },
          { name: "name", type: "string" },
          { name: "joined", type: "datetime", nullable: true },
        ],
      });
      const result = await database.insertBatch("people", {
        columns: {
          active: [true, false, true],
          score: [1.5, 2, 3],
          name: ["Ada", "Grace", "Linus"],
          joined: [new Date("2026-01-01"), null, new Date("2026-01-03")],
        },
      });

      expect(result).toMatchObject({
        tableName: "people",
        rowCount: 3,
        blockCount: 8,
        version: 0,
      });
      expect(result.storedBytes).toBeGreaterThan(0);
      expect(result.metrics).toMatchObject({
        storedBytes: result.storedBytes,
        retries: 0,
      });
      expect(result.metrics.logicalBytes).toBeGreaterThan(0);
      expect(result.metrics.rowsPerSecond).toBeGreaterThan(0);
      expect(result.metrics.writeAmplification).toBeGreaterThan(0);
      expect(await database.listTables()).toEqual([
        {
          name: "people",
          columns: [
            { name: "active", type: "boolean", nullable: false },
            { name: "score", type: "number", nullable: false },
            { name: "name", type: "string", nullable: false },
            { name: "joined", type: "datetime", nullable: true },
          ],
        },
      ]);
      expect(await allVisibleSegments(database, "people")).toHaveLength(1);

      const table = (await store.listTables())[0];
      const segment =
        table === undefined ? undefined : (await tableSegmentRecords(store, table.id))[0];
      const nameColumn = table?.columns.find((column) => column.name === "name");
      const firstNameBlockId =
        nameColumn === undefined ? undefined : segment?.columnBlockIds[nameColumn.id]?.[0];
      expect(firstNameBlockId).toBeDefined();
      const bytes =
        firstNameBlockId === undefined ? undefined : await store.getBlock(firstNameBlockId);
      expect(bytes).toBeDefined();
      if (bytes !== undefined)
        expect((await decodeBlock(bytes)).column.values).toEqual(["Ada", "Grace"]);
      store.close();
    });

    it("keeps wide-table block layout deterministic while encoding columns concurrently", async () => {
      const store = await implementation.create();
      const database = new MinnowDatabase(store, { rowsPerBlock: 2, compression: "gzip" });
      await database.createTable({
        name: "wide_rows",
        columns: [
          { name: "c0", type: "number" },
          { name: "c1", type: "number" },
          { name: "c2", type: "number" },
          { name: "c3", type: "number" },
          { name: "c4", type: "number" },
          { name: "c5", type: "number" },
          { name: "c6", type: "number" },
          { name: "c7", type: "number" },
        ],
      });
      const result = await database.insertBatch("wide_rows", {
        columns: {
          c0: [0, 1, 2, 3, 4],
          c1: [10, 11, 12, 13, 14],
          c2: [20, 21, 22, 23, 24],
          c3: [30, 31, 32, 33, 34],
          c4: [40, 41, 42, 43, 44],
          c5: [50, 51, 52, 53, 54],
          c6: [60, 61, 62, 63, 64],
          c7: [70, 71, 72, 73, 74],
        },
      });

      expect(result.blockCount).toBe(24);
      const table = (await store.listTables())[0];
      const segment =
        table === undefined ? undefined : (await tableSegmentRecords(store, table.id))[0];
      expect(segment).toBeDefined();
      for (const column of table?.columns ?? []) {
        const ids = segment?.columnBlockIds[column.id];
        expect(ids).toHaveLength(3);
        expect(ids?.map((id) => id.slice(id.lastIndexOf("/") + 1))).toEqual([
          "000000",
          "000001",
          "000002",
        ]);
      }
      expect((await database.query("SELECT * FROM wide_rows ORDER BY c0")).rows).toEqual(
        Array.from({ length: 5 }, (_, row) =>
          Object.fromEntries(
            Array.from({ length: 8 }, (_, column) => [`c${String(column)}`, column * 10 + row]),
          ),
        ),
      );
      store.close();
    });

    it("round-trips column defaults through the catalog and validates them", async () => {
      const store = await implementation.create();
      const database = new MinnowDatabase(store);
      await database.createTable({
        name: "notes",
        columns: [
          { name: "id", type: "number", defaultValue: { kind: "autoincrement" } },
          { name: "status", type: "string", defaultValue: { kind: "literal", value: "draft" } },
          {
            name: "created",
            type: "datetime",
            defaultValue: { kind: "expression", sql: "CURRENT_TIMESTAMP" },
          },
        ],
        uniqueKey: "id",
      });
      expect((await database.listTables())[0]?.columns).toEqual([
        { name: "id", type: "number", nullable: false, defaultValue: { kind: "autoincrement" } },
        {
          name: "status",
          type: "string",
          nullable: false,
          defaultValue: { kind: "literal", value: "draft" },
        },
        {
          name: "created",
          type: "datetime",
          nullable: false,
          defaultValue: { kind: "expression", sql: "CURRENT_TIMESTAMP" },
        },
      ]);

      const create = (column: {
        name: string;
        type: "boolean" | "number" | "string" | "datetime";
        nullable?: boolean;
        defaultValue?: import("../storage/types.js").ColumnDefault;
      }) =>
        database.createTable({
          name: `invalid_${column.name}`,
          columns: [{ name: "key", type: "number" }, column],
          uniqueKey: "key",
        });
      await expect(
        create({
          name: "a",
          type: "string",
          nullable: true,
          defaultValue: { kind: "literal", value: "x" },
        }),
      ).resolves.toBeUndefined();
      await expect(
        create({
          name: "b",
          type: "string",
          defaultValue: { kind: "uuid" } as unknown as import("../storage/types.js").ColumnDefault,
        }),
      ).rejects.toThrow("Unknown default kind: uuid");
      await expect(
        create({ name: "c", type: "string", defaultValue: { kind: "expression", sql: "" } }),
      ).rejects.toThrow("trimmed non-empty expression");
      await expect(
        create({ name: "d", type: "string", defaultValue: { kind: "autoincrement" } }),
      ).rejects.toThrow("Auto-increment requires a number column");
      await expect(
        create({ name: "e", type: "number", defaultValue: { kind: "autoincrement" } }),
      ).rejects.toThrow("Auto-increment requires the unique key column");
      await expect(
        create({ name: "f", type: "number", defaultValue: { kind: "literal", value: "x" } }),
      ).rejects.toThrow("Default literal must be a number");
      await expect(
        create({
          name: "g",
          type: "number",
          defaultValue: { kind: "literal", value: Number.POSITIVE_INFINITY },
        }),
      ).rejects.toThrow("Default literal must be finite");
      await expect(
        create({ name: "h", type: "datetime", defaultValue: { kind: "literal", value: 1 } }),
      ).rejects.toThrow("Default literal must be a datetime");
      await expect(
        database.createTable({
          name: "invalid_key_literal",
          columns: [
            { name: "key", type: "string", defaultValue: { kind: "literal", value: "constant" } },
          ],
          uniqueKey: "key",
        }),
      ).resolves.toBeUndefined();
      store.close();
    });

    it("round-trips enum columns through the catalog and validates every write", async () => {
      const store = await implementation.create();
      const database = new MinnowDatabase(store);
      await database.createTable({
        name: "tickets",
        columns: [
          { name: "id", type: "number" },
          {
            name: "status",
            type: "string",
            enumValues: ["open", "closed"],
            defaultValue: { kind: "literal", value: "open" },
          },
          { name: "severity", type: "string", nullable: true, enumValues: ["low", "high"] },
        ],
        uniqueKey: "id",
      });
      expect((await database.listTables())[0]?.columns).toEqual([
        { name: "id", type: "number", nullable: false },
        {
          name: "status",
          type: "string",
          nullable: false,
          defaultValue: { kind: "literal", value: "open" },
          enumValues: ["open", "closed"],
        },
        { name: "severity", type: "string", nullable: true, enumValues: ["low", "high"] },
      ]);

      await database.insertBatch("tickets", {
        columns: { id: [1, 2], status: ["open", "closed"], severity: ["low", null] },
      });
      await expect(
        database.insertBatch("tickets", {
          columns: { id: [3], status: ["reopened"], severity: [null] },
        }),
      ).rejects.toThrow("status[0] must be one of: open, closed");
      await expect(
        database.upsertBatch("tickets", {
          columns: { id: [1], status: ["open"], severity: ["medium"] },
        }),
      ).rejects.toThrow("severity[0] must be one of: low, high");
      await expect(
        database.updateBatch("tickets", { keys: [1], changes: { status: ["reopened"] } }),
      ).rejects.toThrow("status[0] must be one of: open, closed");
      await expect(
        database.execute("UPDATE tickets SET status = 'gone' WHERE id = 1"),
      ).rejects.toThrow("status[0] must be one of: open, closed");

      await expect(
        database.createTable({
          name: "bad_type",
          columns: [{ name: "state", type: "number", enumValues: ["a"] }],
        }),
      ).rejects.toThrow("Enum values require a string column: state");
      await expect(
        database.createTable({
          name: "bad_empty",
          columns: [{ name: "state", type: "string", enumValues: [] }],
        }),
      ).rejects.toThrow("An enum needs at least one value: state");
      await expect(
        database.createTable({
          name: "bad_duplicate",
          columns: [{ name: "state", type: "string", enumValues: ["a", "a"] }],
        }),
      ).rejects.toThrow('Duplicate enum value: state has "a" twice');
      await expect(
        database.createTable({
          name: "bad_default",
          columns: [
            {
              name: "state",
              type: "string",
              enumValues: ["a", "b"],
              defaultValue: { kind: "literal", value: "c" },
            },
          ],
        }),
      ).rejects.toThrow("Default must be one of the enum values: state");
      store.close();
    });

    it("fills defaults and auto-increment keys on insert", async () => {
      const store = await implementation.create();
      const stamped = new Date("2026-02-03T04:05:06.000Z");
      const database = new MinnowDatabase(store, { now: () => stamped });
      await database.createTable({
        name: "notes",
        columns: [
          { name: "id", type: "number", defaultValue: { kind: "autoincrement" } },
          { name: "status", type: "string", defaultValue: { kind: "literal", value: "draft" } },
          {
            name: "created",
            type: "datetime",
            defaultValue: { kind: "expression", sql: "CURRENT_TIMESTAMP" },
          },
        ],
        uniqueKey: "id",
      });
      const result = await database.insertBatch("notes", [{}, {}, {}]);
      expect(result.rowCount).toBe(3);
      const generated = result.generatedColumns;
      expect(generated?.id).toEqual([1, 2, 3]);
      expect(generated?.status).toEqual(["draft", "draft", "draft"]);
      expect(generated?.created).toEqual([stamped, stamped, stamped]);
      const rows = await database.readTable("notes");
      expect(rows.map((row) => row.id).sort()).toEqual([1, 2, 3]);
      store.close();
    });

    it("passes explicit values through and bumps the counter past them", async () => {
      const store = await implementation.create();
      const database = new MinnowDatabase(store);
      await database.createTable({
        name: "events",
        columns: [
          { name: "id", type: "number", defaultValue: { kind: "autoincrement" } },
          { name: "label", type: "string" },
        ],
        uniqueKey: "id",
      });
      // Import scenario: explicit-only inserts still advance the counter atomically.
      const explicitOnly = await database.insertBatch("events", [{ id: 100, label: "imported" }]);
      expect(explicitOnly.generatedColumns).toBeUndefined();
      const generatedAfter = await database.insertBatch("events", [{ label: "fresh" }]);
      expect(generatedAfter.generatedColumns?.id).toEqual([101]);
      // A mixed batch: the generated value never collides with the batch's own explicit max.
      const mixed = await database.insertBatch("events", [
        { id: 200, label: "explicit" },
        { label: "generated" },
      ]);
      expect(mixed.generatedColumns?.id).toEqual([200, 201]);
      await expect(
        database.insertBatch("events", [{ id: 100, label: "duplicate" }]),
      ).rejects.toThrow(UniqueConstraintError);
      store.close();
    });

    it("does not mutate caller-owned columnar vectors while filling", async () => {
      const store = await implementation.create();
      const database = new MinnowDatabase(store);
      await database.createTable({
        name: "events",
        columns: [
          { name: "id", type: "number", defaultValue: { kind: "autoincrement" } },
          { name: "label", type: "string" },
        ],
        uniqueKey: "id",
      });
      const idVector: Array<number | null> = [5, null];
      const result = await database.insertBatch("events", {
        columns: { id: idVector, label: ["a", "b"] },
        omitted: { id: [false, true] },
      });
      expect(result.generatedColumns?.id).toEqual([5, 6]);
      expect(idVector).toEqual([5, null]);
      store.close();
    });

    it("rejects malformed columnar omission metadata before evaluating defaults", async () => {
      const store = await implementation.create();
      const database = new MinnowDatabase(store);
      await database.createTable({
        name: "events",
        columns: [
          { name: "id", type: "number", defaultValue: { kind: "autoincrement" } },
          { name: "label", type: "string", defaultValue: { kind: "literal", value: "new" } },
        ],
        uniqueKey: "id",
      });
      await expect(
        database.insertBatch("events", {
          columns: { id: [null, null], label: [null, null] },
          omitted: { label: [true] },
          rowCount: 2,
        }),
      ).rejects.toThrow("Omission mask label has 1 rows; expected 2");
      await expect(
        database.insertBatch("events", {
          columns: { id: [null], label: [null] },
          omitted: { missing: [true] },
          rowCount: 1,
        }),
      ).rejects.toThrow("Unknown column in omission mask: missing");
      await expect(
        database.insertBatch("events", {
          columns: { id: [null], label: [null] },
          omitted: { label: [1 as unknown as boolean] },
          rowCount: 1,
        }),
      ).rejects.toThrow("Omission mask label must contain booleans");
      await expect(
        database.insertBatch("events", {
          columns: { id: [null], label: [null] },
          omitted: { id: [true, true], label: [true, true] },
          rowCount: 2,
        }),
      ).rejects.toThrow("Column id has 1 rows; expected 2");
      store.close();
    });

    it("rejects unsafe auto-increment values and range overflow", async () => {
      const store = await implementation.create();
      const database = new MinnowDatabase(store);
      await database.createTable({
        name: "events",
        columns: [{ name: "id", type: "number", defaultValue: { kind: "autoincrement" } }],
        uniqueKey: "id",
      });
      await expect(database.insertBatch("events", [{ id: 1.5 }])).rejects.toThrow(
        "safe integer range",
      );
      await database.insertBatch("events", [{ id: Number.MAX_SAFE_INTEGER }]);
      await expect(database.insertBatch("events", [{}])).rejects.toBeInstanceOf(RangeError);
      store.close();
    });

    it("generates fresh keys for upserts that omit the auto-increment column", async () => {
      const store = await implementation.create();
      const database = new MinnowDatabase(store);
      await database.createTable({
        name: "events",
        columns: [
          { name: "id", type: "number", defaultValue: { kind: "autoincrement" } },
          { name: "label", type: "string" },
        ],
        uniqueKey: "id",
      });
      const first = await database.upsertBatch("events", [{ label: "created" }]);
      expect(first.generatedColumns?.id).toEqual([1]);
      expect(first.insertedRowCount).toBe(1);
      const replaced = await database.upsertBatch("events", [{ id: 1, label: "replaced" }]);
      expect(replaced.updatedRowCount).toBe(1);
      expect(replaced.generatedColumns).toBeUndefined();
      const fresh = await database.upsertBatch("events", [{ label: "fresh" }]);
      expect(fresh.generatedColumns?.id).toEqual([2]);
      expect(fresh.insertedRowCount).toBe(1);
      expect((await database.readTable("events")).map((row) => row.label).sort()).toEqual([
        "fresh",
        "replaced",
      ]);
      store.close();
    });

    it("fills defaults for SQL inserts that omit default-bearing columns", async () => {
      const store = await implementation.create();
      const database = new MinnowDatabase(store);
      await database.createTable({
        name: "notes",
        columns: [
          { name: "id", type: "number", defaultValue: { kind: "autoincrement" } },
          { name: "body", type: "string" },
        ],
        uniqueKey: "id",
      });
      await database.execute("INSERT INTO notes (body) VALUES ('first'), ('second')");
      const rows = await database.readTable("notes");
      expect(rows.map((row) => row.id).sort()).toEqual([1, 2]);
      store.close();
    });

    it("still rejects nulls in non-default non-nullable columns", async () => {
      const store = await implementation.create();
      const database = new MinnowDatabase(store);
      await database.createTable({
        name: "events",
        columns: [
          { name: "id", type: "number", defaultValue: { kind: "autoincrement" } },
          { name: "label", type: "string" },
        ],
        uniqueKey: "id",
      });
      await expect(database.insertBatch("events", [{ label: null }])).rejects.toThrow(
        "label[0] cannot be null",
      );
      store.close();
    });

    it("matches documents across columns with MATCH ... AGAINST", async () => {
      const store = await implementation.create();
      const database = new MinnowDatabase(store, { rowsPerBlock: 2, compression: "raw" });
      await database.createTable({
        name: "articles",
        columns: [
          { name: "title", type: "string" },
          { name: "body", type: "string", nullable: true },
          { name: "views", type: "number" },
          { name: "published", type: "datetime", nullable: true },
          { name: "featured", type: "boolean" },
        ],
      });
      await database.insertBatch("articles", [
        {
          title: "Quick start guide",
          body: "The quick brown fox",
          views: 42,
          published: new Date("2026-08-12T00:00:00.000Z"),
          featured: true,
        },
        {
          title: "Database internals",
          body: "Columnar storage layout",
          views: 7,
          published: null,
          featured: false,
        },
        {
          title: "Fox hunting quick tips",
          body: null,
          views: 99,
          published: new Date("2025-01-01T00:00:00.000Z"),
          featured: false,
        },
      ]);

      // AND across terms, OR across columns: both terms must appear somewhere in the document.
      const both = await database.query(
        "SELECT title FROM articles WHERE MATCH(title, body) AGAINST 'quick fox' ORDER BY title",
      );
      expect(both.rows.map((row) => row.title)).toEqual([
        "Fox hunting quick tips",
        "Quick start guide",
      ]);

      const prefix = await database.query(
        "SELECT title FROM articles WHERE MATCH(title) AGAINST 'databas*'",
      );
      expect(prefix.rows.map((row) => row.title)).toEqual(["Database internals"]);

      // MATCH(*) searches numbers and datetimes through their canonical rendering.
      const numeric = await database.query(
        "SELECT title FROM articles WHERE MATCH(*) AGAINST '42'",
      );
      expect(numeric.rows.map((row) => row.title)).toEqual(["Quick start guide"]);
      const year = await database.query("SELECT title FROM articles WHERE MATCH(*) AGAINST '2025'");
      expect(year.rows.map((row) => row.title)).toEqual(["Fox hunting quick tips"]);

      // A row whose listed columns are all NULL is unknown: NOT(unknown) stays unknown, so the
      // null-bodied row is dropped rather than matched.
      const negated = await database.query(
        "SELECT title FROM articles WHERE NOT (MATCH(body) AGAINST 'fox')",
      );
      expect(negated.rows.map((row) => row.title)).toEqual(["Database internals"]);

      const none = await database.query("SELECT title FROM articles WHERE MATCH(*) AGAINST '  '");
      expect(none.rows).toEqual([]);

      // The budgeted streamed scan takes the same path through sliding dictionary windows.
      const streamed = await database.query(
        "SELECT title FROM articles WHERE MATCH(title, body) AGAINST 'quick fox' ORDER BY title",
        { executionMemoryBudgetBytes: 200_000 },
      );
      expect(streamed.rows).toEqual(both.rows);

      await expect(
        database.query("SELECT title FROM articles WHERE MATCH(featured) AGAINST 'x'"),
      ).rejects.toThrow("boolean column");
      await expect(
        database.query("SELECT title FROM articles WHERE MATCH(title) AGAINST title"),
      ).rejects.toThrow("string literal");

      expect(
        await database.explain("SELECT title FROM articles WHERE MATCH(*) AGAINST 'fox'"),
      ).toContain("full-text MATCH evaluates via per-dictionary term tables");
      store.close();
    });

    it("scores documents with BM25 ... AGAINST and orders by relevance", async () => {
      const store = await implementation.create();
      const database = new MinnowDatabase(store, { rowsPerBlock: 2, compression: "raw" });
      await database.createTable({
        name: "articles",
        columns: [
          { name: "id", type: "number" },
          { name: "title", type: "string" },
          { name: "body", type: "string", nullable: true },
        ],
      });
      await database.insertBatch("articles", [
        { id: 1, title: "quick brown fox", body: "jumps high" },
        { id: 2, title: "lazy dog", body: "sleeps quick" },
        // Term frequencies sum across fields: "quick" twice in the title plus once implied by
        // the shorter document pushes this row's score above row 1's.
        { id: 3, title: "quick fox again quick", body: null },
        { id: 4, title: "nothing here", body: "at all" },
      ]);
      const scored = await database.query(
        "SELECT id, BM25(title, body) AGAINST 'quick fox' AS score FROM articles WHERE MATCH(title, body) AGAINST 'quick' ORDER BY score DESC",
      );
      expect(scored.rows.map((row) => row.id)).toEqual([3, 1, 2]);
      const scores = scored.rows.map((row) => row.score as number);
      expect(scores[0]).toBeGreaterThan(scores[1] ?? 0);
      expect(scores[1]).toBeGreaterThan(scores[2] ?? 0);
      expect(scores[2]).toBeGreaterThan(0);

      // Unmatched documents score 0; all-null documents are SQL null.
      const all = await database.query(
        "SELECT id, BM25(body) AGAINST 'quick' AS score FROM articles ORDER BY id",
      );
      expect(all.rows.map((row) => row.score === null || typeof row.score === "number")).toEqual([
        true,
        true,
        true,
        true,
      ]);
      expect(all.rows[3]?.score).toBe(0);
      expect(all.rows[2]?.score).toBeNull();

      // The budgeted path falls back to a materialized scan (never streams) and agrees.
      const budgeted = await database.query(
        "SELECT id, BM25(title, body) AGAINST 'quick fox' AS score FROM articles WHERE MATCH(title, body) AGAINST 'quick' ORDER BY score DESC",
        { executionMemoryBudgetBytes: 500_000 },
      );
      expect(budgeted.rows).toEqual(scored.rows);

      expect(
        await database.explain(
          "SELECT id, BM25(*) AGAINST 'quick' AS score FROM articles ORDER BY score DESC",
        ),
      ).toContain("index pruning does not apply");
      store.close();
    });

    it("builds a persisted full-text index, prunes scans, and merges live deltas", async () => {
      const store = await implementation.create();
      const database = new MinnowDatabase(store, { rowsPerBlock: 2, compression: "raw" });
      await database.createTable({
        name: "articles",
        columns: [
          { name: "title", type: "string" },
          { name: "body", type: "string", nullable: true },
        ],
      });
      await database.insertBatch("articles", [
        { title: "quick brown fox", body: "jumps high" },
        { title: "lazy dog", body: null },
      ]);
      await database.insertBatch("articles", [
        { title: "columnar storage", body: "quick reads" },
        { title: "manifest deltas", body: "cheap commits" },
      ]);
      const sql =
        "SELECT title FROM articles WHERE MATCH(title, body) AGAINST 'quick' ORDER BY title";
      const scanned = await database.query(sql);
      expect(scanned.rows.map((row) => row.title)).toEqual(["columnar storage", "quick brown fox"]);

      await database.buildFtsIndex("articles", "title");
      await database.buildFtsIndex("articles", "body");
      const record = (await store.listTables())[0];
      const states = Object.values(record?.ftsColumns ?? {});
      expect(states.map((state) => state.state)).toEqual(["ready", "ready"]);

      // Same results through the index-pruned path, and the pruning is observable: a term
      // confined to the second segment never materializes the first segment's blocks.
      const indexed = await database.query(sql);
      expect(indexed.rows).toEqual(scanned.rows);
      const confined = await database.query(
        "SELECT title FROM articles WHERE MATCH(title, body) AGAINST 'manifest'",
      );
      expect(confined.rows.map((row) => row.title)).toEqual(["manifest deltas"]);

      // explain() reports the pruning a plan actually gets: a bare MATCH prunes, and a scoring
      // plan prunes too once the index serves its exact corpus statistics.
      const matchExplain = await database.explain(sql);
      expect(matchExplain).toContain("index prunes the base scan");
      const scoringExplain = await database.explain(
        "SELECT title, BM25(title) AGAINST 'quick' AS score FROM articles WHERE MATCH(title) AGAINST 'quick' ORDER BY score DESC",
      );
      expect(scoringExplain).toContain("index prunes the base scan");
      expect(scoringExplain).toContain("BM25 statistics come from the full-text index");

      // Index-served statistics also lift the streaming restriction: a budgeted scoring query
      // streams its scan and returns exactly the unbudgeted (and pre-index) results.
      const scoringSql =
        "SELECT title, BM25(title, body) AGAINST 'quick' AS score FROM articles WHERE MATCH(title, body) AGAINST 'quick' ORDER BY score DESC, title";
      const unbudgetedScores = await database.query(scoringSql);
      const budgetedScores = await database.query(scoringSql, {
        executionMemoryBudgetBytes: 500_000,
      });
      expect(budgetedScores.rows).toEqual(unbudgetedScores.rows);
      expect(unbudgetedScores.rows.length).toBeGreaterThan(0);
      // The .search() shape — ordering by an unselected score expression — streams identically:
      // the desugared projection wrapper is transparent to the streamed path.
      const searchSql =
        "SELECT title FROM articles WHERE MATCH(title, body) AGAINST 'quick' ORDER BY BM25(title, body) AGAINST 'quick' DESC, title";
      const unbudgetedSearch = await database.query(searchSql);
      const budgetedSearch = await database.query(searchSql, {
        executionMemoryBudgetBytes: 500_000,
      });
      expect(budgetedSearch.rows).toEqual(unbudgetedSearch.rows);
      expect(unbudgetedSearch.rows.map((row) => row.title)).toEqual(
        unbudgetedScores.rows.map((row) => row.title),
      );

      // Writers that see the catalog entry maintain the index through commit deltas.
      await database.insertBatch("articles", [{ title: "quick patch", body: null }]);
      const merged = await database.query(sql);
      expect(merged.rows.map((row) => row.title)).toEqual([
        "columnar storage",
        "quick brown fox",
        "quick patch",
      ]);
      store.close();
    });

    it("folds a full-text delta tail after bounded writes even without another search", async () => {
      const store = await implementation.create();
      const database = new MinnowDatabase(store, { rowsPerBlock: 2, compression: "raw" });
      await database.createTable({
        name: "articles",
        columns: [{ name: "title", type: "string" }],
      });
      await database.insertBatch("articles", [{ title: "seed document" }]);
      await database.buildFtsIndex("articles", "title");

      // One delta record per commit used to grow forever unless a later MATCH observed the
      // tail. Cross the fold threshold without issuing any search at all.
      for (let commit = 0; commit <= 16; commit += 1) {
        await database.insertBatch("articles", [{ title: `later document ${String(commit)}` }]);
      }
      // close() joins an already-scheduled rebuild, making the persistent shape deterministic.
      await database.close();
      const table = (await store.listTables())[0];
      const column = table?.columns[0];
      if (table === undefined || column === undefined) throw new Error("FTS table is missing");
      const version = (await store.getCurrentManifestVersion()) ?? -1;
      const candidates = await store.readFtsCandidates(
        table.id,
        column.id,
        [{ term: "later", prefix: false }],
        version,
      );
      expect(candidates.deltaChunkCount).toBeLessThanOrEqual(16);
      store.close();
    });

    it("invalidates the full-text index on keyed mutations and stays correct via scan", async () => {
      const store = await implementation.create();
      const database = new MinnowDatabase(store, { rowsPerBlock: 2, compression: "raw" });
      await database.createTable({
        name: "notes",
        uniqueKey: "slug",
        columns: [
          { name: "slug", type: "string" },
          { name: "body", type: "string" },
        ],
      });
      await database.insertBatch("notes", [
        { slug: "a", body: "quick fox" },
        { slug: "b", body: "lazy dog" },
      ]);
      await database.buildFtsIndex("notes", "body");
      expect(Object.values((await store.listTables())[0]?.ftsColumns ?? {})[0]?.state).toBe(
        "ready",
      );
      // An upsert emits no delta on purpose: the publish flips the column to invalid, and the
      // query falls back to the always-correct scan, seeing the replaced document.
      await database.upsertBatch("notes", [{ slug: "a", body: "silent owl" }]);
      expect(Object.values((await store.listTables())[0]?.ftsColumns ?? {})[0]?.state).toBe(
        "invalid",
      );
      const rows = await database.query("SELECT slug FROM notes WHERE MATCH(body) AGAINST 'owl'");
      expect(rows.rows.map((row) => row.slug)).toEqual(["a"]);
      const gone = await database.query("SELECT slug FROM notes WHERE MATCH(body) AGAINST 'quick'");
      expect(gone.rows).toEqual([]);
      // Rebuilding on a keyed history is rejected explicitly.
      await expect(database.buildFtsIndex("notes", "body")).rejects.toThrow("append-only tables");
      store.close();
    });

    it("keeps BM25 corpus statistics exact when zone maps could prune the scan", async () => {
      const store = await implementation.create();
      const database = new MinnowDatabase(store, { rowsPerBlock: 2, compression: "raw" });
      await database.createTable({
        name: "readings",
        columns: [
          { name: "id", type: "number" },
          { name: "note", type: "string" },
        ],
      });
      // Two segments with disjoint id ranges: the range predicate could zone-prune the
      // second, which would corrupt scan-computed corpus statistics (docCount, df).
      await database.insertBatch("readings", [
        { id: 1, note: "quick fox" },
        { id: 2, note: "quick quick brown" },
      ]);
      await database.insertBatch("readings", [
        { id: 10, note: "lazy dog" },
        { id: 11, note: "quiet river" },
      ]);
      const prunable = await database.query(
        "SELECT id, BM25(note) AGAINST 'quick' AS score FROM readings WHERE id <= 2 AND MATCH(note) AGAINST 'quick' ORDER BY id",
      );
      // The arithmetic spelling of the same predicate never zone-prunes; whole-table
      // statistics must make the two spellings score identically.
      const unprunable = await database.query(
        "SELECT id, BM25(note) AGAINST 'quick' AS score FROM readings WHERE id + 0 <= 2 AND MATCH(note) AGAINST 'quick' ORDER BY id",
      );
      expect(prunable.rows).toEqual(unprunable.rows);
      expect(prunable.rows).toHaveLength(2);
      store.close();
    });

    it("time-travel BM25 ignores an index built past the queried version", async () => {
      const store = await implementation.create();
      const database = new MinnowDatabase(store, { rowsPerBlock: 2, compression: "raw" });
      await database.createTable({
        name: "docs",
        columns: [{ name: "body", type: "string" }],
      });
      const first = await database.insertBatch("docs", [
        { body: "quick fox" },
        { body: "lazy dog" },
      ]);
      const sql =
        "SELECT body, BM25(body) AGAINST 'quick' AS score FROM docs WHERE MATCH(body) AGAINST 'quick' ORDER BY body";
      const before = await database.query(sql, { version: first.version });
      // More rows and an index covering them: the index's statistics describe a corpus the
      // queried version cannot see, so time travel must fall back to scan statistics.
      await database.insertBatch("docs", [{ body: "quick quick quick" }, { body: "quiet" }]);
      await database.buildFtsIndex("docs", "body");
      const after = await database.query(sql, { version: first.version });
      expect(after).toEqual(before);
      expect(after.rows).toHaveLength(1);
      store.close();
    });

    it("re-expands MATCH(*) after a migration adds a column", async () => {
      const store = await implementation.create();
      const database = new MinnowDatabase(store, { rowsPerBlock: 2, compression: "raw" });
      await database.migrate(schema([table("posts", { title: column.string() })]));
      await database.insertBatch("posts", [{ title: "alpha" }]);
      const sql = "SELECT title FROM posts WHERE MATCH(*) AGAINST 'zebra'";
      // Prime the plan cache with the pre-migration expansion of "*".
      expect((await database.query(sql)).rows).toEqual([]);
      await database.migrate(
        schema([table("posts", { title: column.string(), notes: column.string().nullable() })]),
      );
      await database.insertBatch("posts", [{ title: "plain", notes: "a zebra hides here" }]);
      // The cached compiled plan must keep "*": expansion re-runs against the live catalog, so
      // the row matching only in the migrated-in column is found.
      expect((await database.query(sql)).rows.map((row) => row.title)).toEqual(["plain"]);
      store.close();
    });

    it("retries migrate when background catalog activity moves a table revision", async () => {
      const store = await implementation.create();
      let conflictsToInject = 1;
      const database = new MinnowDatabase(
        new (class extends FaultInjectingBlockStore {
          override updateTable(
            id: string,
            expectedRevision: number,
            update: Parameters<BlockStore["updateTable"]>[2],
          ): ReturnType<BlockStore["updateTable"]> {
            if (conflictsToInject > 0) {
              conflictsToInject -= 1;
              throw new TableRecordConflictError(id, expectedRevision, expectedRevision + 1);
            }
            return super.updateTable(id, expectedRevision, update);
          }
        })(store, () => undefined),
      );
      const v1 = table("posts", { title: column.string() });
      await database.migrate(schema([v1]));
      const v2 = table("posts", {
        title: column.string(),
        notes: column.string().nullable(),
      });
      // The first CAS loses (as if an index build stamped the record mid-migrate); the retry
      // re-plans from fresh records and succeeds instead of surfacing the conflict.
      const result = await database.migrate(schema([v2]));
      expect(result.alteredTables).toEqual(["posts"]);
      expect(conflictsToInject).toBe(0);
      store.close();
    });

    it("reports the complete durable migration when a retry follows partial progress", async () => {
      const store = await implementation.create();
      const initial = schema([
        table("first_table", { id: column.number().unique() }),
        table("second_table", { id: column.number().unique() }),
      ]);
      await new MinnowDatabase(store).migrate(initial);
      let updateCalls = 0;
      const database = new MinnowDatabase(
        new (class extends FaultInjectingBlockStore {
          override updateTable(
            id: string,
            expectedRevision: number,
            update: Parameters<BlockStore["updateTable"]>[2],
          ): ReturnType<BlockStore["updateTable"]> {
            updateCalls += 1;
            if (updateCalls === 2) {
              throw new TableRecordConflictError(id, expectedRevision, expectedRevision + 1);
            }
            return super.updateTable(id, expectedRevision, update);
          }
        })(store, () => undefined),
      );

      const result = await database.migrate(
        schema([
          table("first_table", {
            id: column.number().unique(),
            note: column.string().nullable(),
          }),
          table("second_table", {
            id: column.number().unique(),
            note: column.string().nullable(),
          }),
        ]),
      );

      expect(result.alteredTables).toEqual(["first_table", "second_table"]);
      expect(result.steps.filter((step) => step.kind === "add-column")).toHaveLength(2);
      expect(updateCalls).toBe(3);
      expect((await database.introspect()).tables.map((record) => record.columns.length)).toEqual([
        2, 2,
      ]);
      store.close();
    });

    it("agrees between indexed and scan-only search over a randomized corpus", async () => {
      const store = await implementation.create();
      const database = new MinnowDatabase(store, { rowsPerBlock: 4, compression: "raw" });
      const words = ["quick", "brown", "fox", "stone", "river", "moss", "42", "quiet"];
      let seed = 1234;
      const random = () => {
        // Deterministic LCG so both stores exercise the same corpus.
        seed = (seed * 1103515245 + 12345) % 2147483648;
        return seed / 2147483648;
      };
      await database.createTable({
        name: "articles",
        columns: [
          { name: "title", type: "string" },
          { name: "body", type: "string", nullable: true },
        ],
      });
      const makeText = () =>
        Array.from(
          { length: 2 + Math.floor(random() * 3) },
          () => words[Math.floor(random() * words.length)],
        ).join(" ");
      for (let batch = 0; batch < 4; batch += 1) {
        await database.insertBatch(
          "articles",
          Array.from({ length: 10 }, () => ({
            title: makeText(),
            body: random() < 0.2 ? null : makeText(),
          })),
        );
      }
      const queries = [
        "quick",
        "fox stone",
        "riv*",
        "moss quick",
        "zzz",
        "qu* mo*",
        "42 river",
      ].map(
        (query) =>
          `SELECT title, body FROM articles WHERE MATCH(title, body) AGAINST '${query}' ORDER BY title, body`,
      );
      // BM25 scores must not move when the index appears: scoring plans see the whole corpus
      // (index pruning is disabled for them), so scores stay identical before and after.
      queries.push(
        "SELECT title, body, BM25(title, body) AGAINST 'quick moss' AS score FROM articles WHERE MATCH(title, body) AGAINST 'quick' ORDER BY title, body",
      );
      const scanned = [];
      for (const sql of queries) scanned.push((await database.query(sql)).rows);
      await database.buildFtsIndex("articles", "title");
      await database.buildFtsIndex("articles", "body");
      for (let index = 0; index < queries.length; index += 1) {
        const indexed = await database.query(queries[index] ?? "");
        expect(indexed.rows, queries[index]).toEqual(scanned[index]);
      }
      store.close();
    });

    it("schedules a lazy full-text index build once a searched table crosses the threshold", async () => {
      const store = await implementation.create();
      const database = new MinnowDatabase(store, {
        rowsPerBlock: 2,
        compression: "raw",
        ftsAutoIndexRows: 1,
      });
      await database.createTable({
        name: "articles",
        columns: [{ name: "title", type: "string" }],
      });
      await database.insertBatch("articles", [{ title: "quick brown fox" }, { title: "lazy dog" }]);
      const sql = "SELECT title FROM articles WHERE MATCH(title) AGAINST 'quick'";
      const first = await database.query(sql);
      expect(first.rows.map((row) => row.title)).toEqual(["quick brown fox"]);
      // The scan-mode search fired a background build; poll the catalog until it lands.
      for (let attempt = 0; attempt < 50; attempt += 1) {
        const state = Object.values((await store.listTables())[0]?.ftsColumns ?? {})[0]?.state;
        if (state === "ready") break;
        await new Promise((resolve) => setTimeout(resolve, 10));
      }
      expect(Object.values((await store.listTables())[0]?.ftsColumns ?? {})[0]?.state).toBe(
        "ready",
      );
      expect((await database.query(sql)).rows).toEqual(first.rows);
      store.close();
    });

    it("inserts and upserts an array of rows", async () => {
      const store = await implementation.create();
      const database = new MinnowDatabase(store);
      await database.createTable({
        name: "people",
        uniqueKey: "name",
        columns: [
          { name: "name", type: "string" },
          { name: "score", type: "number" },
          { name: "joined", type: "datetime", nullable: true },
        ],
      });
      const joined = new Date("2026-01-01");
      const result = await database.insertBatch("people", [
        { name: "Ada", score: 10, joined },
        // A nullable column left out of a row is written as null, not rejected.
        { name: "Grace", score: 20 },
      ]);
      expect(result).toMatchObject({ tableName: "people", rowCount: 2 });
      expect(await database.readTable("people")).toEqual([
        { name: "Ada", score: 10, joined },
        { name: "Grace", score: 20, joined: null },
      ]);

      await database.upsertBatch("people", [
        { name: "Grace", score: 25, joined },
        { name: "Linus", score: 30, joined: null },
      ]);
      expect(await database.readTable("people")).toEqual([
        { name: "Ada", score: 10, joined },
        { name: "Grace", score: 25, joined },
        { name: "Linus", score: 30, joined: null },
      ]);

      // An omitted column with no default becomes NULL and then fails its nullability check.
      await expect(database.insertBatch("people", [{ name: "Katherine", joined }])).rejects.toThrow(
        "score[0] cannot be null",
      );
      await expect(
        database.insertBatch("people", [
          { name: "Katherine", score: 40, joined },
          { name: "Barbara", joined },
        ]),
      ).rejects.toThrow("score[1] cannot be null");
      await expect(database.insertBatch("people", [])).rejects.toThrow(
        "A batch needs at least one row",
      );
      store.close();
    });

    it("rejects malformed batches before writing", async () => {
      const store = await implementation.create();
      const database = new MinnowDatabase(store);
      await database.createTable({
        name: "events",
        columns: [{ name: "value", type: "number" }],
      });
      await expect(
        database.insertBatch("events", { columns: { value: [1, "wrong"] } }),
      ).rejects.toThrow("must be number");
      // Sparse arrays and explicit undefined must be rejected up front: forEach-style validation
      // skips holes, which once let a hole reach the encoder and persist a corrupt block.
      const sparse = new Array<number>(3);
      sparse[0] = 1;
      sparse[2] = 3;
      await expect(database.insertBatch("events", { columns: { value: sparse } })).rejects.toThrow(
        "must be number",
      );
      await expect(
        database.insertBatch("events", {
          columns: { value: [1, undefined] as unknown as number[] },
        }),
      ).rejects.toThrow("must be number");
      expect(await physicalBlockCount(store)).toBe(0);
      store.close();
    });

    it("upserts new and matching rows using a simple unique key", async () => {
      const store = await implementation.create();
      const database = new MinnowDatabase(store, { rowsPerBlock: 2 });
      await database.createTable({
        name: "accounts",
        uniqueKey: "email",
        columns: [
          { name: "email", type: "string" },
          { name: "score", type: "number" },
          { name: "active", type: "boolean" },
        ],
      });
      await database.insertBatch("accounts", {
        columns: {
          email: ["ada@example.com", "grace@example.com"],
          score: [10, 20],
          active: [true, true],
        },
      });
      const result = await database.upsertBatch("accounts", {
        columns: {
          email: ["grace@example.com", "linus@example.com"],
          score: [25, 30],
          active: [false, true],
        },
      });

      expect(result).toMatchObject({
        rowCount: 2,
        insertedRowCount: 1,
        updatedRowCount: 1,
        version: 1,
      });
      expect(await database.readTable("accounts")).toEqual([
        { email: "ada@example.com", score: 10, active: true },
        { email: "grace@example.com", score: 25, active: false },
        { email: "linus@example.com", score: 30, active: true },
      ]);
      expect(await database.readTable("accounts", 0)).toEqual([
        { email: "ada@example.com", score: 10, active: true },
        { email: "grace@example.com", score: 20, active: true },
      ]);
      expect((await database.listTables())[0]?.uniqueKey).toBe("email");
      store.close();
    });

    it("writes partial update segments and preserves older snapshots", async () => {
      const store = await implementation.create();
      const database = new MinnowDatabase(store, { rowsPerBlock: 2 });
      await database.createTable({
        name: "accounts",
        uniqueKey: "email",
        columns: [
          { name: "email", type: "string" },
          { name: "score", type: "number" },
          { name: "active", type: "boolean" },
        ],
      });
      const inserted = await database.insertBatch("accounts", {
        columns: {
          email: ["ada@example.com", "grace@example.com"],
          score: [10, 20],
          active: [true, true],
        },
      });
      const updated = await database.updateBatch("accounts", {
        keys: ["ada@example.com", "grace@example.com"],
        changes: { score: [15, 25] },
      });

      expect(updated).toMatchObject({
        requestedRowCount: 2,
        updatedRowCount: 2,
        changedColumns: ["score"],
        blockCount: 2,
        version: 1,
      });
      expect(updated.metrics.logicalBytes).toBeGreaterThan(0);
      expect(updated.metrics.storedBytes).toBe(updated.storedBytes);
      expect(updated.metrics.rowsPerSecond).toBeGreaterThan(0);
      expect(await database.readTable("accounts")).toEqual([
        { email: "ada@example.com", score: 15, active: true },
        { email: "grace@example.com", score: 25, active: true },
      ]);
      expect(await database.readTable("accounts", inserted.version)).toEqual([
        { email: "ada@example.com", score: 10, active: true },
        { email: "grace@example.com", score: 20, active: true },
      ]);
      expect(
        await database.readTable("accounts", {
          columns: ["active", "score"],
        }),
      ).toEqual([
        { active: true, score: 15 },
        { active: true, score: 25 },
      ]);
      const table = (await store.listTables())[0];
      const updateSegment = (
        table === undefined ? [] : await tableSegmentRecords(store, table.id)
      ).find((segment) => segment.kind === "update");
      expect(Object.keys(updateSegment?.columnBlockIds ?? {})).toHaveLength(2);
      store.close();
    });

    it("validates update keys and changed columns before publishing", async () => {
      const store = await implementation.create();
      const database = new MinnowDatabase(store);
      await database.createTable({
        name: "accounts",
        uniqueKey: "email",
        columns: [
          { name: "email", type: "string" },
          { name: "score", type: "number" },
        ],
      });
      await database.insert("accounts", { email: "ada@example.com", score: 10 });

      await expect(
        database.update("accounts", "missing@example.com", { score: 20 }),
      ).rejects.toBeInstanceOf(MissingKeyError);
      await expect(
        database.updateBatch("accounts", {
          keys: ["ada@example.com"],
          changes: { email: ["changed@example.com"] },
        }),
      ).rejects.toThrow("Unique key cannot be updated");
      await expect(
        database.updateBatch("accounts", {
          keys: ["ada@example.com", "ada@example.com"],
          changes: { score: [1, 2] },
        }),
      ).rejects.toThrow("Duplicate update key");
      await expect(
        database.updateBatch("accounts", {
          keys: ["ada@example.com"],
          changes: { score: [] },
        }),
      ).rejects.toThrow("same row count");
      expect(await database.readTable("accounts")).toEqual([
        { email: "ada@example.com", score: 10 },
      ]);
      store.close();
    });

    it("rejects duplicate unique keys and upserts without a unique key", async () => {
      const store = await implementation.create();
      const database = new MinnowDatabase(store);
      await database.createTable({
        name: "accounts",
        uniqueKey: "email",
        columns: [
          { name: "email", type: "string" },
          { name: "score", type: "number" },
        ],
      });
      await expect(
        database.insertBatch("accounts", {
          columns: { email: ["same@example.com", "same@example.com"], score: [1, 2] },
        }),
      ).rejects.toBeInstanceOf(UniqueConstraintError);
      await database.insertBatch("accounts", {
        columns: { email: ["saved@example.com"], score: [1] },
      });
      await expect(
        database.insertBatch("accounts", {
          columns: { email: ["saved@example.com"], score: [2] },
        }),
      ).rejects.toBeInstanceOf(UniqueConstraintError);
      expect(
        (await transactionRecords(store)).filter((record) => record.status === "active"),
      ).toEqual([]);

      await database.createTable({
        name: "logs",
        columns: [{ name: "message", type: "string" }],
      });
      await expect(
        database.upsertBatch("logs", { columns: { message: ["hello"] } }),
      ).rejects.toThrow("needs a unique key");
      await expect(
        database.createTable({
          name: "invalid",
          uniqueKey: "key",
          columns: [{ name: "key", type: "string", nullable: true }],
        }),
      ).rejects.toThrow("Unique key cannot be nullable");
      store.close();
    });

    it("checks the persistent unique-key lookup without reading table blocks", async () => {
      const store = await implementation.create();
      const database = new MinnowDatabase(store);
      await database.createTable({
        name: "accounts",
        uniqueKey: "email",
        columns: [
          { name: "email", type: "string" },
          { name: "score", type: "number" },
        ],
      });
      await database.insertBatch("accounts", {
        columns: { email: ["saved@example.com"], score: [1] },
      });
      store.getBlock = async () => {
        throw new Error("Table blocks should not be read for a key check");
      };
      store.getBlocks = async () => {
        throw new Error("Table blocks should not be read for a key check");
      };

      await expect(
        database.insertBatch("accounts", {
          columns: { email: ["saved@example.com"], score: [2] },
        }),
      ).rejects.toBeInstanceOf(UniqueConstraintError);
      store.close();
    });

    it("deletes rows by unique key without changing older snapshots", async () => {
      const store = await implementation.create();
      const database = new MinnowDatabase(store, { rowsPerBlock: 2 });
      await database.createTable({
        name: "accounts",
        uniqueKey: "email",
        columns: [
          { name: "email", type: "string" },
          { name: "score", type: "number" },
        ],
      });
      await database.insertBatch("accounts", {
        columns: {
          email: ["ada@example.com", "grace@example.com", "linus@example.com"],
          score: [10, 20, 30],
        },
      });

      const result = await database.deleteBatch("accounts", {
        keys: ["grace@example.com", "missing@example.com"],
      });
      expect(result).toMatchObject({
        requestedKeyCount: 2,
        deletedRowCount: 1,
        blockCount: 1,
        version: 1,
      });
      expect(result.metrics).toMatchObject({ storedBytes: result.storedBytes, retries: 0 });
      expect(await database.readTable("accounts")).toEqual([
        { email: "ada@example.com", score: 10 },
        { email: "linus@example.com", score: 30 },
      ]);
      expect(await database.readTable("accounts", 0)).toHaveLength(3);

      await database.insert("accounts", { email: "grace@example.com", score: 25 });
      expect(await database.readTable("accounts")).toEqual([
        { email: "ada@example.com", score: 10 },
        { email: "linus@example.com", score: 30 },
        { email: "grace@example.com", score: 25 },
      ]);
      store.close();
    });

    it("supports single-row inserts and upserts", async () => {
      const store = await implementation.create();
      const database = new MinnowDatabase(store);
      await database.createTable({
        name: "settings",
        uniqueKey: "name",
        columns: [
          { name: "name", type: "string" },
          { name: "value", type: "string" },
        ],
      });
      await database.insert("settings", { name: "theme", value: "light" });
      const result = await database.upsert("settings", { name: "theme", value: "dark" });
      expect(result.updatedRowCount).toBe(1);
      expect(await database.readTable("settings")).toEqual([{ name: "theme", value: "dark" }]);
      store.close();
    });

    it("flushes buffered rows at the configured row limit and on close", async () => {
      const store = await implementation.create();
      const database = new MinnowDatabase(store);
      await database.createTable({
        name: "events",
        columns: [
          { name: "name", type: "string" },
          { name: "value", type: "number" },
        ],
      });
      const writer = database.bufferedWriter("events", { maxRows: 2, maxAgeMs: 60_000 });
      expect(await writer.add({ name: "one", value: 1 })).toBeUndefined();
      expect((await writer.add({ name: "two", value: 2 }))?.rowCount).toBe(2);
      expect(writer.pendingRowCount).toBe(0);
      await writer.add({ name: "three", value: 3 });
      expect((await writer.close())?.rowCount).toBe(1);
      expect(await database.readTable("events")).toEqual([
        { name: "one", value: 1 },
        { name: "two", value: 2 },
        { name: "three", value: 3 },
      ]);
      await expect(writer.add({ name: "four", value: 4 })).rejects.toThrow("closed");
      store.close();
    });

    it("flushes buffered rows at the byte limit and can discard a failed batch", async () => {
      const store = await implementation.create();
      const database = new MinnowDatabase(store);
      await database.createTable({
        name: "messages",
        columns: [{ name: "text", type: "string" }],
      });
      const writer = database.bufferedWriter("messages", {
        maxBytes: 4,
        maxRows: 100,
        maxAgeMs: 60_000,
      });
      expect((await writer.add({ text: "hello" }))?.rowCount).toBe(1);
      await expect(writer.add({ text: 42 })).rejects.toThrow("must be string");
      expect(writer.pendingRowCount).toBe(1);
      expect(writer.discard()).toBe(1);
      await writer.close();
      expect(await database.readTable("messages")).toEqual([{ text: "hello" }]);
      store.close();
    });
  });
}

it("keeps concurrent batch inserts from two browser connections", async () => {
  const factory = new IDBFactory();
  const name = crypto.randomUUID();
  const leftStore = await IndexedDbBlockStore.open({ name, indexedDB: factory });
  const rightStore = await IndexedDbBlockStore.open({ name, indexedDB: factory });
  const left = new MinnowDatabase(leftStore);
  const right = new MinnowDatabase(rightStore);
  await left.createTable({
    name: "events",
    columns: [{ name: "value", type: "number" }],
  });

  const results = await Promise.all([
    left.insertBatch("events", { columns: { value: [1, 2] } }),
    right.insertBatch("events", { columns: { value: [3, 4] } }),
  ]);

  expect(results.map((result) => result.version).sort()).toEqual([0, 1]);
  expect(results.map((result) => result.metrics.retries).sort()).toEqual([0, 1]);
  expect(await allVisibleSegments(left, "events")).toHaveLength(2);
  const tableId = (await leftStore.listTables())[0]?.id;
  const segments = tableId === undefined ? [] : await tableSegmentRecords(leftStore, tableId);
  const ranges = segments
    .map((segment): [bigint, bigint] => [segment.rowIdStart, segment.rowIdEndExclusive])
    .sort((left, right) => (left[0] < right[0] ? -1 : 1));
  expect(ranges).toEqual([
    [1n, 3n],
    [3n, 5n],
  ]);
  leftStore.close();
  rightStore.close();
});

it("generates unique auto-increment keys across two IndexedDB connections", async () => {
  const factory = new IDBFactory();
  const name = crypto.randomUUID();
  const leftStore = await IndexedDbBlockStore.open({ name, indexedDB: factory });
  const rightStore = await IndexedDbBlockStore.open({ name, indexedDB: factory });
  const left = new MinnowDatabase(leftStore);
  const right = new MinnowDatabase(rightStore);
  await left.createTable({
    name: "events",
    columns: [
      { name: "id", type: "number", defaultValue: { kind: "autoincrement" } },
      { name: "source", type: "string" },
    ],
    uniqueKey: "id",
  });

  // Both connections race the manifest CAS; the loser rebases and its generated keys must
  // survive the retry unchanged and stay globally unique.
  const results = await Promise.all([
    left.insertBatch("events", [{ source: "left" }, { source: "left" }, { source: "left" }]),
    right.insertBatch("events", [{ source: "right" }, { source: "right" }]),
  ]);
  const ids = results.flatMap((result) => result.generatedColumns?.id ?? []);
  expect(ids).toHaveLength(5);
  expect(new Set(ids).size).toBe(5);
  const rows = await left.readTable("events");
  expect(rows.map((row) => row.id).sort((a, b) => Number(a) - Number(b))).toEqual(
    [...ids].sort((a, b) => Number(a) - Number(b)),
  );
  leftStore.close();
  rightStore.close();
});

it("streams a search-shaped query under a budget too small to materialize", async () => {
  const store = new MemoryBlockStore();
  const database = new MinnowDatabase(store, { rowsPerBlock: 512, compression: "raw" });
  await database.createTable({
    name: "docs",
    columns: [{ name: "body", type: "string" }],
  });
  const words = ["quick", "brown", "fox", "stone", "river"];
  const wordAt = (index: number): string => words[index % words.length] ?? "";
  const rowCount = 20_000;
  await database.insertBatch("docs", {
    columns: {
      body: Array.from(
        { length: rowCount },
        (_, index) => `${wordAt(index)} ${wordAt(index + 1)} entry ${String(index)}`,
      ),
    },
  });
  await database.buildFtsIndex("docs", "body");
  // The .search() shape: ordering by an unselected BM25 expression wraps the plan in a
  // projection block. The budget is far below what materializing the scan needs, so this
  // succeeds only if the wrapper is transparent to the streamed path.
  const sql =
    "SELECT body FROM docs WHERE MATCH(body) AGAINST 'quick' ORDER BY BM25(body) AGAINST 'quick' DESC, body LIMIT 5";
  const budget = 192_000;
  // The materialized fallback (spill disabled) cannot run under this budget, while the
  // streamed default returns the full result.
  await expect(
    database.query(sql, { executionMemoryBudgetBytes: budget, spillToStorage: false }),
  ).rejects.toThrow(QueryMemoryBudgetError);
  const streamed = await database.query(sql, { executionMemoryBudgetBytes: budget });
  expect(streamed.columns).toEqual(["body"]);
  expect(streamed.rows).toHaveLength(5);
  expect(streamed.rows).toEqual((await database.query(sql)).rows);
  store.close();
});

it("maintains the full-text index across two IndexedDB connections", async () => {
  const factory = new IDBFactory();
  const name = crypto.randomUUID();
  const leftStore = await IndexedDbBlockStore.open({ name, indexedDB: factory });
  const rightStore = await IndexedDbBlockStore.open({ name, indexedDB: factory });
  const left = new MinnowDatabase(leftStore, { rowsPerBlock: 2, compression: "raw" });
  const right = new MinnowDatabase(rightStore, { rowsPerBlock: 2, compression: "raw" });
  await left.createTable({
    name: "articles",
    columns: [{ name: "title", type: "string" }],
  });
  await left.insertBatch("articles", [{ title: "quick brown fox" }, { title: "lazy dog" }]);
  await left.buildFtsIndex("articles", "title");
  // The other connection reads the catalog fresh per write, sees the index, and emits deltas.
  await right.insertBatch("articles", [{ title: "quick patch" }, { title: "slow release" }]);
  const sql = "SELECT title FROM articles WHERE MATCH(title) AGAINST 'quick' ORDER BY title";
  const viaLeft = await left.query(sql);
  const viaRight = await right.query(sql);
  expect(viaLeft.rows.map((row) => row.title)).toEqual(["quick brown fox", "quick patch"]);
  expect(viaRight.rows).toEqual(viaLeft.rows);
  expect(Object.values((await leftStore.listTables())[0]?.ftsColumns ?? {})[0]?.state).toBe(
    "ready",
  );
  leftStore.close();
  rightStore.close();
});

it("burns the reserved auto-increment range when a write aborts", async () => {
  const store = new MemoryBlockStore();
  let failNextBlockWrite = false;
  // The fault-injecting proxy deliberately lacks beginTransaction, so this also exercises the
  // fallback reservation path.
  const faultStore = new FaultInjectingBlockStore(store, (point) => {
    if (point === "afterBlockWrite" && failNextBlockWrite) {
      failNextBlockWrite = false;
      throw new Error("injected write failure");
    }
  });
  const database = new MinnowDatabase(faultStore);
  await database.createTable({
    name: "events",
    columns: [
      { name: "id", type: "number", defaultValue: { kind: "autoincrement" } },
      { name: "label", type: "string" },
    ],
    uniqueKey: "id",
  });
  await database.insertBatch("events", [{ label: "first" }]);
  failNextBlockWrite = true;
  await expect(database.insertBatch("events", [{ label: "lost" }])).rejects.toThrow(
    "injected write failure",
  );
  const result = await database.insertBatch("events", [{ label: "after" }]);
  // Row 1 committed, the aborted write burned id 2, so the next generated key is 3.
  expect(result.generatedColumns?.id).toEqual([3]);
  expect((await database.readTable("events")).map((row) => row.label).sort()).toEqual([
    "after",
    "first",
  ]);
  store.close();
});

it("rechecks unique keys when two IndexedDB connections insert the same value", async () => {
  const factory = new IDBFactory();
  const name = crypto.randomUUID();
  const leftStore = await IndexedDbBlockStore.open({ name, indexedDB: factory });
  const rightStore = await IndexedDbBlockStore.open({ name, indexedDB: factory });
  const left = new MinnowDatabase(leftStore);
  const right = new MinnowDatabase(rightStore);
  await left.createTable({
    name: "accounts",
    uniqueKey: "email",
    columns: [
      { name: "email", type: "string" },
      { name: "score", type: "number" },
    ],
  });

  const results = await Promise.allSettled([
    left.insertBatch("accounts", { columns: { email: ["same@example.com"], score: [1] } }),
    right.insertBatch("accounts", { columns: { email: ["same@example.com"], score: [2] } }),
  ]);

  expect(results.filter((result) => result.status === "fulfilled")).toHaveLength(1);
  const rejected = results.find((result) => result.status === "rejected");
  expect(rejected?.status).toBe("rejected");
  if (rejected?.status === "rejected") {
    expect(rejected.reason).toBeInstanceOf(UniqueConstraintError);
  }
  expect(await left.readTable("accounts")).toHaveLength(1);
  leftStore.close();
  rightStore.close();
});

it("orders competing upserts by their committed database version", async () => {
  const factory = new IDBFactory();
  const name = crypto.randomUUID();
  const leftStore = await IndexedDbBlockStore.open({ name, indexedDB: factory });
  const rightStore = await IndexedDbBlockStore.open({ name, indexedDB: factory });
  const left = new MinnowDatabase(leftStore);
  const right = new MinnowDatabase(rightStore);
  await left.createTable({
    name: "accounts",
    uniqueKey: "email",
    columns: [
      { name: "email", type: "string" },
      { name: "score", type: "number" },
    ],
  });

  const results = await Promise.all([
    left.upsertBatch("accounts", { columns: { email: ["same@example.com"], score: [1] } }),
    right.upsertBatch("accounts", { columns: { email: ["same@example.com"], score: [2] } }),
  ]);

  expect(results.map((result) => result.version).sort()).toEqual([0, 1]);
  expect(results.map((result) => result.insertedRowCount).sort()).toEqual([0, 1]);
  expect(results.map((result) => result.updatedRowCount).sort()).toEqual([0, 1]);
  const rows = await left.readTable("accounts");
  const lastResult = results.find((result) => result.version === 1);
  const expectedScore = lastResult === results[0] ? 1 : 2;
  expect(rows).toEqual([{ email: "same@example.com", score: expectedScore }]);
  leftStore.close();
  rightStore.close();
});

it("keeps older unique-key tables correct before their lookup is rebuilt", async () => {
  const store = new MemoryBlockStore();
  await store.addTable({
    id: "legacy-table",
    name: "legacy_accounts",
    columns: [
      { id: "legacy-email", name: "email", type: "string", nullable: false },
      { id: "legacy-score", name: "score", type: "number", nullable: false },
    ],
    uniqueKeyColumnId: "legacy-email",
    managed: false,
    revision: 0,
    createdAt: "2026-01-01T00:00:00.000Z",
  });
  const database = new MinnowDatabase(store);
  await database.insertBatch("legacy_accounts", {
    columns: { email: ["saved@example.com"], score: [1] },
  });
  await expect(
    database.insertBatch("legacy_accounts", {
      columns: { email: ["saved@example.com"], score: [2] },
    }),
  ).rejects.toBeInstanceOf(UniqueConstraintError);
  store.close();
});

it("applies concurrent delete and upsert operations in committed order", async () => {
  const factory = new IDBFactory();
  const name = crypto.randomUUID();
  const leftStore = await IndexedDbBlockStore.open({ name, indexedDB: factory });
  const rightStore = await IndexedDbBlockStore.open({ name, indexedDB: factory });
  const left = new MinnowDatabase(leftStore);
  const right = new MinnowDatabase(rightStore);
  await left.createTable({
    name: "accounts",
    uniqueKey: "email",
    columns: [
      { name: "email", type: "string" },
      { name: "score", type: "number" },
    ],
  });
  await left.insert("accounts", { email: "same@example.com", score: 1 });

  const [deleted, upserted] = await Promise.all([
    left.deleteBatch("accounts", { keys: ["same@example.com"] }),
    right.upsert("accounts", { email: "same@example.com", score: 2 }),
  ]);
  const rows = await left.readTable("accounts");
  if ((deleted.version ?? -1) > upserted.version) expect(rows).toEqual([]);
  else expect(rows).toEqual([{ email: "same@example.com", score: 2 }]);
  leftStore.close();
  rightStore.close();
});

it("serializes competing partial updates and never resurrects a deleted key", async () => {
  const factory = new IDBFactory();
  const name = crypto.randomUUID();
  const leftStore = await IndexedDbBlockStore.open({ name, indexedDB: factory });
  const rightStore = await IndexedDbBlockStore.open({ name, indexedDB: factory });
  const left = new MinnowDatabase(leftStore);
  const right = new MinnowDatabase(rightStore);
  await left.createTable({
    name: "accounts",
    uniqueKey: "email",
    columns: [
      { name: "email", type: "string" },
      { name: "score", type: "number" },
      { name: "active", type: "boolean" },
    ],
  });
  await left.insert("accounts", { email: "same@example.com", score: 1, active: true });

  const updates = await Promise.all([
    left.update("accounts", "same@example.com", { score: 2 }),
    right.update("accounts", "same@example.com", { active: false }),
  ]);
  expect(updates.map((result) => result.version).sort()).toEqual([1, 2]);
  expect(await left.readTable("accounts")).toEqual([
    { email: "same@example.com", score: 2, active: false },
  ]);

  const deleteAndUpdate = await Promise.allSettled([
    left.deleteBatch("accounts", { keys: ["same@example.com"] }),
    right.update("accounts", "same@example.com", { score: 3 }),
  ]);
  const rejected = deleteAndUpdate.find((result) => result.status === "rejected");
  if (rejected?.status === "rejected") expect(rejected.reason).toBeInstanceOf(MissingKeyError);
  expect(await left.readTable("accounts")).toEqual([]);
  leftStore.close();
  rightStore.close();
});

it("compacts append-only segments without changing current or older snapshots", async () => {
  const store = new MemoryBlockStore();
  const database = new MinnowDatabase(store, { rowsPerBlock: 2 });
  await database.createTable({
    name: "events",
    columns: [
      { name: "name", type: "string" },
      { name: "value", type: "number" },
    ],
  });
  await database.insertBatch("events", {
    columns: { name: ["one", "two"], value: [1, 2] },
  });
  const second = await database.insertBatch("events", {
    columns: { name: ["three", "four"], value: [3, 4] },
  });
  const expected = await database.readTable("events");
  const oldBlockIds = await currentManifestBlockIds(store);

  const result = await database.compactTable("events");

  expect(result).toMatchObject({
    compacted: true,
    sourceSegmentCount: 2,
    sourceBlockCount: 4,
    outputBlockCount: 2,
    rowCount: 4,
    supersededBlockCount: 4,
    physicallyReclaimedBytes: 0,
    version: 2,
  });
  expect(await database.readTable("events")).toEqual(expected);
  expect(await database.readTable("events", second.version)).toEqual(expected);
  expect(await allVisibleSegments(database, "events")).toHaveLength(1);
  expect(await allVisibleSegments(database, "events", second.version)).toHaveLength(2);
  expect((await store.getBlocks(oldBlockIds)).every((bytes) => bytes !== undefined)).toBe(true);
  store.close();
});

it("pages visible segments at one captured version across concurrent commits", async () => {
  const database = new MinnowDatabase(new MemoryBlockStore(), {
    autoCollect: false,
    autoCompact: false,
  });
  await database.createTable({
    name: "paged_events",
    columns: [{ name: "value", type: "number" }],
  });
  const before = [];
  for (let value = 1; value <= 5; value += 1) {
    before.push(await database.insert("paged_events", { value }));
  }

  const first = await database.listVisibleSegmentPage("paged_events", { limit: 2 });
  expect(first.records).toHaveLength(2);
  expect(first.records.map(({ id }) => id)).toEqual([...first.records.map(({ id }) => id)].sort());
  expect(first.nextCursor).toMatchObject({
    tableName: "paged_events",
    version: first.version,
  });
  const concurrent = await database.insert("paged_events", { value: 6 });

  const captured = [...first.records];
  let cursor = first.nextCursor;
  while (cursor !== null) {
    const page = await database.listVisibleSegmentPage("paged_events", { cursor, limit: 2 });
    expect(page.records.length).toBeLessThanOrEqual(2);
    expect(page.version).toBe(first.version);
    captured.push(...page.records);
    cursor = page.nextCursor;
  }
  expect(captured.map(({ id }) => id).sort()).toEqual(
    before.map(({ segmentId }) => segmentId).sort(),
  );
  expect(captured.map(({ id }) => id)).not.toContain(concurrent.segmentId);
  expect((await allVisibleSegments(database, "paged_events")).map(({ id }) => id)).toContain(
    concurrent.segmentId,
  );

  await expect(
    database.listVisibleSegmentPage("paged_events", {
      limit: MAX_VISIBLE_SEGMENT_PAGE_ITEMS + 1,
    }),
  ).rejects.toThrow(`cannot exceed ${String(MAX_VISIBLE_SEGMENT_PAGE_ITEMS)}`);
  if (first.nextCursor === null) throw new Error("Expected a visible segment cursor");
  await expect(
    database.listVisibleSegmentPage("paged_events", {
      cursor: first.nextCursor,
      version: first.version,
    }),
  ).rejects.toThrow(/cannot be combined/u);
  await expect(
    database.listVisibleSegmentPage("other_table", { cursor: first.nextCursor }),
  ).rejects.toThrow(/different table name/u);

  const capturedTableId = first.nextCursor.tableId;
  await database.dropTable("paged_events");
  await database.createTable({
    name: "paged_events",
    columns: [{ name: "value", type: "number" }],
  });
  const stale = await database
    .listVisibleSegmentPage("paged_events", { cursor: first.nextCursor })
    .catch((error: unknown) => error);
  expect(stale).toBeInstanceOf(VisibleSegmentCursorStaleError);
  const typedStale = stale as VisibleSegmentCursorStaleError;
  expect(typedStale).toMatchObject({ tableName: "paged_events", capturedTableId });
  expect(typeof typedStale.currentTableId).toBe("string");
  expect(typedStale.currentTableId).not.toBe(capturedTableId);
  await database.close();
});

it("advances a visible-segment cursor across empty historical windows", async () => {
  const database = new MinnowDatabase(new MemoryBlockStore(), {
    autoCollect: false,
    autoCompact: false,
  });
  await database.createTable({
    name: "paged_history",
    columns: [{ name: "value", type: "number" }],
  });
  for (let value = 1; value <= 3; value += 1) {
    await database.insert("paged_history", { value });
  }
  await database.compactTable("paged_history", { outputCompression: "raw" });

  let cursor: VisibleSegmentPageCursor | null = null;
  let sawEmptyContinuation = false;
  let visible = 0;
  do {
    const page = await database.listVisibleSegmentPage(
      "paged_history",
      cursor === null ? { limit: 1 } : { cursor, limit: 1 },
    );
    visible += page.records.length;
    if (page.records.length === 0 && page.nextCursor !== null) sawEmptyContinuation = true;
    cursor = page.nextCursor;
  } while (cursor !== null);
  expect(sawEmptyContinuation).toBe(true);
  expect(visible).toBe(1);
  await database.close();
});

it("physically rechunks every simple type on shared bitmap-aligned row windows", async () => {
  const store = new MemoryBlockStore();
  const database = new MinnowDatabase(store, { compression: "raw", rowsPerBlock: 64 });
  await database.createTable({
    name: "readings",
    columns: [
      { name: "active", type: "boolean", nullable: true },
      { name: "score", type: "number", nullable: true },
      { name: "label", type: "string", nullable: true },
      { name: "recordedAt", type: "datetime", nullable: true },
    ],
  });
  const expected = Array.from({ length: 17 }, (_, index) => ({
    active: [0, 7, 8, 15, 16].includes(index) ? null : index % 2 === 0,
    score: [1, 8, 16].includes(index) ? null : index + 0.25,
    label: [2, 7, 9, 15].includes(index) ? null : String.fromCharCode(97 + index),
    recordedAt: [3, 8, 14, 16].includes(index) ? null : new Date(Date.UTC(2026, 0, index + 1)),
  }));
  const insertRange = async (start: number, end: number) => {
    const rows = expected.slice(start, end);
    return database.insertBatch("readings", {
      columns: {
        active: rows.map((row) => row.active),
        score: rows.map((row) => row.score),
        label: rows.map((row) => row.label),
        recordedAt: rows.map((row) => row.recordedAt),
      },
    });
  };

  const first = await insertRange(0, 5);
  await insertRange(5, 10);
  const sourceSnapshot = await insertRange(10, 17);

  const result = await database.compactTable("readings", {
    targetBlockBytes: 75,
    outputCompression: "raw",
    maxBlocksPerStep: 1,
  });

  expect(result).toMatchObject({
    compacted: true,
    sourceSegmentCount: 3,
    sourceBlockCount: 12,
    outputBlockCount: 8,
    rowCount: 17,
    targetBlockBytes: 75,
    outputCompression: "raw",
  });
  expect(await database.readTable("readings")).toEqual(expected);
  expect(await database.readTable("readings", first.version)).toEqual(expected.slice(0, 5));
  expect(await database.readTable("readings", sourceSnapshot.version)).toEqual(expected);

  const job = (await database.listCompactionJobs("readings"))[0];
  if (job?.rewritePlan.kind !== "rechunk-v1") {
    throw new Error("Expected a persisted rechunk plan");
  }
  expect(job.rewritePlan.outputs).toEqual([
    { rowStart: 0, rowCount: 9 },
    { rowStart: 9, rowCount: 8 },
  ]);
  if (job.outputSegmentId === null) throw new Error("Expected a compaction output segment");
  const outputSegment = await store.getSegment(job.outputSegmentId);
  if (outputSegment === undefined) throw new Error("Expected a compaction output segment");
  const outputColumns = Object.values(outputSegment.columnBlockIds);
  expect(outputColumns.map((blockIds) => blockIds.length)).toEqual([2, 2, 2, 2]);

  const outputTypes = new Set<string>();
  for (const blockIds of outputColumns) {
    for (const [outputIndex, blockId] of blockIds.entries()) {
      const bytes = await store.getBlock(blockId);
      if (bytes === undefined) throw new Error(`Expected compaction block ${blockId}`);
      const description = inspectBlock(bytes);
      outputTypes.add(description.type);
      expect(description.compression).toBe("raw");
      expect(description.rowCount).toBe(job.rewritePlan.outputs[outputIndex]?.rowCount);
    }
  }
  expect([...outputTypes].sort()).toEqual(["boolean", "datetime", "number", "string"]);
  store.close();
});

it("prepares append and compacted snapshots directly into stable vectors", async () => {
  const store = new MemoryBlockStore();
  const database = new MinnowDatabase(store, { compression: "gzip", rowsPerBlock: 2 });
  await database.createTable({
    name: "vector_readings",
    columns: [
      { name: "active", type: "boolean", nullable: true },
      { name: "score", type: "number", nullable: true },
      { name: "label", type: "string", nullable: true },
      { name: "recordedAt", type: "datetime", nullable: true },
    ],
  });
  const first = await database.insertBatch("vector_readings", {
    columns: {
      active: [true, null, false],
      score: [1.5, null, -2],
      label: ["shared", null, "third"],
      recordedAt: [new Date("2026-01-01T00:00:00Z"), null, new Date("2026-01-03T00:00:00Z")],
    },
  });
  await database.insertBatch("vector_readings", {
    columns: {
      active: [null, true],
      score: [4, 5],
      label: ["shared", "fifth"],
      recordedAt: [null, new Date("2026-01-05T00:00:00Z")],
    },
  });

  const readingsSql = "SELECT active, score, label, recordedAt FROM vector_readings ORDER BY score";
  const beforeCompaction = await database.snapshot(async (session) => {
    const before = await session.query(readingsSql);
    await database.compactTable("vector_readings", {
      targetBlockBytes: 64,
      outputCompression: "raw",
    });
    // The pinned session keeps observing the pre-compaction snapshot mid-scope.
    expect(await session.query(readingsSql)).toEqual(before);
    return before;
  });
  expect(
    await database.query(
      "SELECT active, score, label, recordedAt FROM vector_readings ORDER BY score",
    ),
  ).toEqual(beforeCompaction);
  expect(
    await database.query(
      "SELECT active, score, label, recordedAt FROM vector_readings ORDER BY score",
      { version: first.version },
    ),
  ).toEqual({
    columns: ["active", "score", "label", "recordedAt"],
    columnDomains: [null, null, null, null],
    rows: [
      {
        active: false,
        score: -2,
        label: "third",
        recordedAt: new Date("2026-01-03T00:00:00Z"),
      },
      {
        active: true,
        score: 1.5,
        label: "shared",
        recordedAt: new Date("2026-01-01T00:00:00Z"),
      },
      {
        active: null,
        score: null,
        label: null,
        recordedAt: null,
      },
    ],
  });
  store.close();
});

it("replays keyed mutations into typed vectors without changing historical results", async () => {
  const store = new MemoryBlockStore();
  const database = new MinnowDatabase(store, { compression: "raw", rowsPerBlock: 1 });
  await database.createTable({
    name: "vector_accounts",
    uniqueKey: "email",
    columns: [
      { name: "email", type: "string" },
      { name: "active", type: "boolean", nullable: true },
      { name: "score", type: "number", nullable: true },
      { name: "seenAt", type: "datetime", nullable: true },
    ],
  });
  const inserted = await database.insertBatch("vector_accounts", {
    columns: {
      email: ["a@example.com", "b@example.com", "c@example.com"],
      active: [true, false, null],
      score: [1, 2, null],
      seenAt: [new Date("2026-02-01T00:00:00Z"), null, new Date("2026-02-03T00:00:00Z")],
    },
  });
  await database.update("vector_accounts", "a@example.com", {
    active: null,
    score: 4,
    seenAt: new Date("2026-02-04T00:00:00Z"),
  });
  await database.deleteBatch("vector_accounts", { keys: ["b@example.com"] });
  await database.upsert("vector_accounts", {
    email: "c@example.com",
    active: true,
    score: 5,
    seenAt: null,
  });
  await database.upsert("vector_accounts", {
    email: "d@example.com",
    active: false,
    score: 6,
    seenAt: new Date("2026-02-06T00:00:00Z"),
  });

  expect(
    await database.query("SELECT email, active, score, seenAt FROM vector_accounts ORDER BY email"),
  ).toEqual({
    columns: ["email", "active", "score", "seenAt"],
    columnDomains: [null, null, null, null],
    rows: [
      {
        email: "a@example.com",
        active: null,
        score: 4,
        seenAt: new Date("2026-02-04T00:00:00Z"),
      },
      { email: "c@example.com", active: true, score: 5, seenAt: null },
      {
        email: "d@example.com",
        active: false,
        score: 6,
        seenAt: new Date("2026-02-06T00:00:00Z"),
      },
    ],
  });
  expect(
    await database.query(
      "SELECT email, active, score, seenAt FROM vector_accounts ORDER BY email",
      { version: inserted.version },
    ),
  ).toEqual({
    columns: ["email", "active", "score", "seenAt"],
    columnDomains: [null, null, null, null],
    rows: [
      {
        email: "a@example.com",
        active: true,
        score: 1,
        seenAt: new Date("2026-02-01T00:00:00Z"),
      },
      { email: "b@example.com", active: false, score: 2, seenAt: null },
      {
        email: "c@example.com",
        active: null,
        score: null,
        seenAt: new Date("2026-02-03T00:00:00Z"),
      },
    ],
  });
  store.close();
});

it("refines skewed strings to exact target-sized windows before persisting the plan", async () => {
  const store = new MemoryBlockStore();
  const database = new MinnowDatabase(store, { compression: "raw", rowsPerBlock: 64 });
  await database.createTable({
    name: "messages",
    columns: [{ name: "body", type: "string" }],
  });
  const values = Array.from({ length: 24 }, (_, index) =>
    index === 7 ? "x".repeat(100) : String.fromCharCode(97 + index),
  );
  await database.insertBatch("messages", { columns: { body: values.slice(0, 16) } });
  await database.insertBatch("messages", { columns: { body: values.slice(16) } });

  let progress = await database.compactTableStep("messages", {
    maxBlocks: 1,
    targetBlockBytes: 64,
    outputCompression: "raw",
  });
  if (progress.jobId === null) throw new Error("Expected a persisted compaction job");
  const jobId = progress.jobId;
  const planned = await store.getCompactionJob(jobId);
  if (planned?.rewritePlan.kind !== "rechunk-v1") {
    throw new Error("Expected a persisted rechunk plan");
  }
  expect(planned.rewritePlan.outputs).toContainEqual({ rowStart: 7, rowCount: 1 });
  expect(
    planned.rewritePlan.outputs.reduce((rowCount, output) => rowCount + output.rowCount, 0),
  ).toBe(values.length);

  const reopened = new MinnowDatabase(store, { compression: "gzip", rowsPerBlock: 1 });
  while (progress.result === null) {
    progress = await reopened.resumeCompactionJob(jobId, { maxBlocks: 1 });
  }
  const completed = await store.getCompactionJob(jobId);
  if (completed?.outputSegmentId === null || completed?.outputSegmentId === undefined) {
    throw new Error("Expected a completed output segment");
  }
  const outputSegment = await store.getSegment(completed.outputSegmentId);
  if (outputSegment === undefined) throw new Error("Expected a completed output segment");
  const outputBlockIds = Object.values(outputSegment.columnBlockIds).flat();
  expect(outputBlockIds).toHaveLength(planned.rewritePlan.outputs.length);
  for (const [index, blockId] of outputBlockIds.entries()) {
    const bytes = await store.getBlock(blockId);
    if (bytes === undefined) throw new Error(`Expected output block ${blockId}`);
    const description = inspectBlock(bytes);
    const window = planned.rewritePlan.outputs[index];
    expect(description.rowCount).toBe(window?.rowCount);
    if ((window?.rowCount ?? 0) > 1) expect(description.encodedLength).toBeLessThanOrEqual(64);
    if (window?.rowStart === 7) {
      expect(window.rowCount).toBe(1);
      expect(description.encodedLength).toBeGreaterThan(64);
    }
  }
  expect(await reopened.readTable("messages")).toEqual(values.map((body) => ({ body })));
  store.close();
});

for (const outputCompression of ["raw", "gzip"] as const) {
  it(`rewrites mixed source codecs with ${outputCompression} preferred after reopen`, async () => {
    const store = new MemoryBlockStore();
    const raw = new MinnowDatabase(store, { compression: "raw", rowsPerBlock: 64 });
    await raw.createTable({
      name: "events",
      columns: [
        { name: "value", type: "number", nullable: true },
        { name: "label", type: "string", nullable: true },
      ],
    });
    const rawInsert = await raw.insertBatch("events", {
      columns: { value: [1, 2], label: ["a", null] },
    });
    const secondRaw = new MinnowDatabase(store, { compression: "raw", rowsPerBlock: 64 });
    const secondRawInsert = await secondRaw.insertBatch("events", {
      columns: { value: [null, 4], label: ["b", "c"] },
    });
    const gzip = new MinnowDatabase(store, { compression: "gzip", rowsPerBlock: 64 });
    // A label long enough that its block clears the gzip floor; the two-number value block
    // stays under it and is written raw, which is exactly the codec mix this test wants.
    const longLabel = "d".repeat(2_100);
    const gzipInsert = await gzip.insertBatch("events", {
      columns: { value: [5, 6], label: [null, longLabel] },
    });
    const expected = [
      { value: 1, label: "a" },
      { value: 2, label: null },
      { value: null, label: "b" },
      { value: 4, label: "c" },
      { value: 5, label: null },
      { value: 6, label: longLabel },
    ];

    for (const [segmentId, compressionOf] of [
      [rawInsert.segmentId, () => "raw"],
      [secondRawInsert.segmentId, () => "raw"],
      [gzipInsert.segmentId, (type: string) => (type === "string" ? "gzip" : "raw")],
    ] as const) {
      const segment = await store.getSegment(segmentId);
      if (segment === undefined) throw new Error(`Expected source segment ${segmentId}`);
      for (const blockId of Object.values(segment.columnBlockIds).flat()) {
        const bytes = await store.getBlock(blockId);
        if (bytes === undefined) throw new Error(`Expected source block ${blockId}`);
        const description = inspectBlock(bytes);
        expect(description.compression).toBe(compressionOf(description.type));
      }
    }

    let progress = await gzip.compactTableStep("events", {
      maxBlocks: 1,
      targetBlockBytes: 20,
      outputCompression,
    });
    if (progress.jobId === null) throw new Error("Expected a persisted compaction job");
    const jobId = progress.jobId;
    expect(progress).toMatchObject({ state: "running", outputBlockCount: 1, result: null });
    expect(await store.getCompactionJob(jobId)).toMatchObject({
      rewritePlan: { kind: "rechunk-v1", targetBlockBytes: 20, outputCompression },
    });

    store.close();
    const reopened = new MinnowDatabase(store, {
      compression: outputCompression === "raw" ? "gzip" : "raw",
      rowsPerBlock: 1,
    });
    while (progress.result === null) {
      progress = await reopened.resumeCompactionJob(jobId, { maxBlocks: 1 });
    }

    expect(progress.result).toMatchObject({
      compacted: true,
      outputCompression,
      targetBlockBytes: 20,
      rowCount: 6,
    });
    const completed = await store.getCompactionJob(jobId);
    if (completed?.outputSegmentId === null || completed?.outputSegmentId === undefined) {
      throw new Error("Expected a completed output segment");
    }
    const outputSegment = await store.getSegment(completed.outputSegmentId);
    if (outputSegment === undefined) throw new Error("Expected a completed output segment");
    for (const blockId of Object.values(outputSegment.columnBlockIds).flat()) {
      const bytes = await store.getBlock(blockId);
      if (bytes === undefined) throw new Error(`Expected output block ${blockId}`);
      // These deliberately tiny target blocks stay raw even when gzip is preferred; the
      // persisted plan records the preference while every block records its actual codec.
      expect(inspectBlock(bytes).compression).toBe("raw");
    }
    expect(await reopened.readTable("events")).toEqual(expected);
    expect(await reopened.readTable("events", gzipInsert.version)).toEqual(expected);
    store.close();
  });
}

it("enforces the persisted compaction memory bound before publishing job output", async () => {
  const store = new MemoryBlockStore();
  const database = new MinnowDatabase(store, { compression: "raw" });
  await database.createTable({
    name: "events",
    columns: [
      { name: "value", type: "number", nullable: true },
      { name: "label", type: "string", nullable: true },
    ],
  });
  await database.insertBatch("events", {
    columns: { value: [1, null, 3], label: ["one", null, "three"] },
  });
  await database.insertBatch("events", {
    columns: { value: [4, 5, null], label: ["four", "five", null] },
  });
  const sourceBlockIds = await currentManifestBlockIds(store);
  const sourceManifestCount = (await manifestRecords(store)).length;
  const options = { targetBlockBytes: 64, outputCompression: "raw" as const };

  let discoveryError: unknown;
  try {
    await database.compactTable("events", { ...options, memoryBudgetBytes: 1 });
  } catch (error) {
    discoveryError = error;
  }
  expect(discoveryError).toBeInstanceOf(CompactionMemoryBudgetError);
  if (!(discoveryError instanceof CompactionMemoryBudgetError)) {
    throw new Error("Expected a compaction memory budget error");
  }
  expect(discoveryError.minimumBytes).toBeGreaterThan(1);

  await expect(
    database.compactTable("events", {
      ...options,
      memoryBudgetBytes: discoveryError.minimumBytes - 1,
    }),
  ).rejects.toMatchObject({
    name: "CompactionMemoryBudgetError",
    budgetBytes: discoveryError.minimumBytes - 1,
    minimumBytes: discoveryError.minimumBytes,
  });
  expect(await store.listCompactionJobs()).toEqual([]);
  expect(await currentManifestBlockIds(store)).toEqual(sourceBlockIds);
  expect(await manifestRecords(store)).toHaveLength(sourceManifestCount);

  const result = await database.compactTable("events", {
    ...options,
    memoryBudgetBytes: discoveryError.minimumBytes,
  });
  expect(result).toMatchObject({
    compacted: true,
    memoryBudgetBytes: discoveryError.minimumBytes,
    minimumMemoryBytes: discoveryError.minimumBytes,
  });
  expect(result.peakWorkingBytes).toBeLessThanOrEqual(discoveryError.minimumBytes);
  expect(await database.readTable("events")).toEqual([
    { value: 1, label: "one" },
    { value: null, label: null },
    { value: 3, label: "three" },
    { value: 4, label: "four" },
    { value: 5, label: "five" },
    { value: null, label: null },
  ]);
  store.close();
});

it("resumes with persisted rewrite settings after an IndexedDB close and reopen", async () => {
  const indexedDB = new IDBFactory();
  const name = crypto.randomUUID();
  let store = await IndexedDbBlockStore.open({ name, indexedDB });
  const database = new MinnowDatabase(store, { compression: "raw", rowsPerBlock: 1 });
  await database.createTable({
    name: "events",
    columns: [{ name: "value", type: "number" }],
  });
  await database.insert("events", { value: 1 });
  await database.insert("events", { value: 2 });
  await database.insert("events", { value: 3 });

  let progress = await database.compactTableStep("events", {
    maxBlocks: 1,
    targetBlockBytes: 9,
    outputCompression: "gzip",
    memoryBudgetBytes: 1_000_000,
  });
  if (progress.jobId === null) throw new Error("Expected a persisted compaction job");
  const jobId = progress.jobId;
  expect(progress).toMatchObject({ state: "running", outputBlockCount: 1 });
  expect(await store.getCompactionJob(jobId)).toMatchObject({
    rewritePlan: {
      kind: "rechunk-v1",
      targetBlockBytes: 9,
      outputCompression: "gzip",
    },
    memoryBudgetBytes: 1_000_000,
  });
  store.close();

  store = await IndexedDbBlockStore.open({ name, indexedDB });
  const reopened = new MinnowDatabase(store, { compression: "raw", rowsPerBlock: 2048 });
  while (progress.result === null) {
    progress = await reopened.resumeCompactionJob(jobId, { maxBlocks: 1 });
  }

  expect(progress.result).toMatchObject({
    compacted: true,
    targetBlockBytes: 9,
    outputCompression: "gzip",
    memoryBudgetBytes: 1_000_000,
    outputBlockCount: 3,
  });
  const completed = await store.getCompactionJob(jobId);
  if (completed?.outputSegmentId === null || completed?.outputSegmentId === undefined) {
    throw new Error("Expected a completed compaction output segment");
  }
  const outputSegment = await store.getSegment(completed.outputSegmentId);
  if (outputSegment === undefined) throw new Error("Expected a completed output segment");
  for (const blockId of Object.values(outputSegment.columnBlockIds).flat()) {
    const bytes = await store.getBlock(blockId);
    if (bytes === undefined) throw new Error(`Expected compaction block ${blockId}`);
    expect(inspectBlock(bytes).compression).toBe("raw");
  }
  expect(await reopened.readTable("events")).toEqual([{ value: 1 }, { value: 2 }, { value: 3 }]);
  store.close();
});

it("coalesces many small source segments into fewer physical blocks", async () => {
  const store = new MemoryBlockStore();
  const database = new MinnowDatabase(store, { rowsPerBlock: 1 });
  await database.createTable({
    name: "events",
    columns: [{ name: "value", type: "number" }],
  });
  for (let value = 0; value < 10; value += 1) {
    await database.insert("events", { value });
  }

  const result = await database.compactTable("events", {
    targetBlockBytes: 1024,
    outputCompression: "raw",
  });

  expect(result).toMatchObject({
    compacted: true,
    sourceSegmentCount: 10,
    sourceBlockCount: 10,
    outputBlockCount: 1,
    rowCount: 10,
  });
  expect(result.outputBlockCount).toBeLessThan(result.sourceBlockCount);
  expect(await database.readTable("events")).toEqual(
    Array.from({ length: 10 }, (_, value) => ({ value })),
  );
  store.close();
});

it("rejects a rechunk target whose codec worst case exceeds the block format limit", async () => {
  const store = new MemoryBlockStore();
  const database = new MinnowDatabase(store);
  await database.createTable({
    name: "events",
    columns: [{ name: "value", type: "number" }],
  });
  await database.insert("events", { value: 1 });
  await database.insert("events", { value: 2 });

  await expect(
    database.compactTable("events", {
      targetBlockBytes: 32 * 1024 * 1024 + 1,
      outputCompression: "gzip",
    }),
  ).rejects.toThrow("Compaction target block bytes exceed the gzip worst-case format limit");
  expect(await store.listCompactionJobs()).toEqual([]);
  store.close();
});

it("checkpoints and resumes append compaction one immutable block at a time", async () => {
  const store = new MemoryBlockStore();
  const database = new MinnowDatabase(store);
  await database.createTable({
    name: "events",
    columns: [{ name: "value", type: "number" }],
  });
  await database.insert("events", { value: 1 });
  await database.insert("events", { value: 2 });
  const third = await database.insert("events", { value: 3 });

  let progress = await database.compactTableStep("events", {
    maxBlocks: 1,
    targetBlockBytes: 9,
    outputCompression: "raw",
  });
  expect(progress).toMatchObject({
    state: "running",
    processedRows: 1,
    sourceSegmentCount: 3,
    sourceBlockCount: 3,
    outputBlockCount: 1,
    result: null,
  });
  expect((await database.listCompactionJobs("events"))[0]).toMatchObject({
    outputCursor: { outputIndex: 1, columnIndex: 0, rowStart: 1 },
    processedRows: 1,
  });

  const reopened = new MinnowDatabase(store);
  while (progress.result === null) {
    if (progress.jobId === null) throw new Error("Expected a persisted compaction job");
    progress = await reopened.resumeCompactionJob(progress.jobId, { maxBlocks: 1 });
  }

  expect(progress).toMatchObject({ state: "published", processedRows: 3, outputBlockCount: 3 });
  expect(await reopened.readTable("events")).toEqual([{ value: 1 }, { value: 2 }, { value: 3 }]);
  expect(await reopened.readTable("events", third.version)).toEqual([
    { value: 1 },
    { value: 2 },
    { value: 3 },
  ]);
  expect(await allVisibleSegments(reopened, "events")).toHaveLength(1);
  const job = (await reopened.listCompactionJobs("events"))[0];
  const output =
    job?.outputSegmentId === null ? undefined : await store.getSegment(job?.outputSegmentId ?? "");
  expect(output).toMatchObject({ level: 1, logicalOrder: 0, rowCount: 3 });
  store.close();
});

it("rebases resumable compaction across an append without reordering rows", async () => {
  const store = new MemoryBlockStore();
  const database = new MinnowDatabase(store);
  await database.createTable({
    name: "events",
    columns: [{ name: "value", type: "number" }],
  });
  await database.insert("events", { value: 1 });
  const second = await database.insert("events", { value: 2 });
  const progress = await database.compactTableStep("events", {
    maxBlocks: 1,
    targetBlockBytes: 9,
    outputCompression: "raw",
  });
  expect(progress.state).toBe("running");

  await database.insert("events", { value: 3 });
  const result = await database.compactTable("events");

  expect(result).toMatchObject({ compacted: true, sourceSegmentCount: 2, version: 3 });
  expect(await database.readTable("events")).toEqual([{ value: 1 }, { value: 2 }, { value: 3 }]);
  expect(await database.readTable("events", second.version)).toEqual([{ value: 1 }, { value: 2 }]);
  expect(await allVisibleSegments(database, "events")).toHaveLength(2);
  store.close();
});

it("recovers a compaction block written before its journal checkpoint", async () => {
  const store = new MemoryBlockStore();
  const database = new MinnowDatabase(store);
  await database.createTable({
    name: "events",
    columns: [{ name: "value", type: "number" }],
  });
  await database.insert("events", { value: 1 });
  await database.insert("events", { value: 2 });

  let failAfterWrite = true;
  const faultStore = new FaultInjectingBlockStore(store, (point) => {
    if (point === "afterBlockWrite" && failAfterWrite) {
      failAfterWrite = false;
      throw new Error("injected after compaction block write");
    }
  });
  const interrupted = new MinnowDatabase(faultStore);
  await expect(interrupted.compactTableStep("events", { maxBlocks: 1 })).rejects.toThrow(
    "injected after compaction block write",
  );
  const interruptedJob = (await store.listCompactionJobs())[0];
  expect(interruptedJob).toMatchObject({ state: "running", outputBlockIds: [] });

  const reopened = new MinnowDatabase(store);
  const result = await reopened.compactTable("events");
  expect(result.compacted).toBe(true);
  expect(await reopened.readTable("events")).toEqual([{ value: 1 }, { value: 2 }]);
  expect((await reopened.listCompactionJobs("events"))[0]?.state).toBe("published");
  store.close();
});

it("reconciles a valid gzip header variant by decoded physical content", async () => {
  const store = new GzipVariantCheckpointFaultMemoryBlockStore();
  const database = new MinnowDatabase(store);
  await database.createTable({
    name: "events",
    columns: [{ name: "value", type: "string" }],
  });
  const firstValue = "a".repeat(5_000);
  const secondValue = "b".repeat(5_000);
  await database.insert("events", { value: firstValue });
  await database.insert("events", { value: secondValue });

  await expect(
    database.compactTableStep("events", {
      maxBlocks: 1,
      outputCompression: "gzip",
    }),
  ).rejects.toThrow("injected before compaction cursor checkpoint");
  const interrupted = (await store.listCompactionJobs())[0];
  if (interrupted?.transactionId === null || interrupted?.transactionId === undefined) {
    throw new Error("Expected a linked compaction transaction");
  }
  const transaction = await store.getTransaction(interrupted.transactionId);
  const outputBlockId = transaction?.pendingBlockIds[0];
  if (outputBlockId === undefined) throw new Error("Expected an uncheckpointed output block");
  const canonical = await store.getCanonicalBlock(outputBlockId);
  if (canonical === undefined) throw new Error("Expected canonical gzip output");
  store.returnHeaderVariant = true;
  const variant = await store.getBlock(outputBlockId);
  if (variant === undefined) throw new Error("Expected gzip header variant");
  expect(variant).not.toEqual(canonical);
  const [decodedVariant, decodedCanonical] = await Promise.all([
    decodeBlock(variant),
    decodeBlock(canonical),
  ]);
  expect(decodedVariant.column).toEqual(decodedCanonical.column);
  expect(decodedVariant.description.checksum).toBe(decodedCanonical.description.checksum);

  const result = await new MinnowDatabase(store).compactTable("events");

  expect(result).toMatchObject({ compacted: true, outputCompression: "gzip", rowCount: 2 });
  expect(store.variantReadCount).toBeGreaterThan(0);
  expect(await database.readTable("events")).toEqual([
    { value: firstValue },
    { value: secondValue },
  ]);
  store.close();
});

it("reconciles a journaled compaction block when its cursor checkpoint is lost", async () => {
  const store = new CheckpointFaultMemoryBlockStore();
  const database = new MinnowDatabase(store);
  await database.createTable({
    name: "events",
    columns: [{ name: "value", type: "number" }],
  });
  await database.insert("events", { value: 1 });
  await database.insert("events", { value: 2 });

  await expect(database.compactTableStep("events", { maxBlocks: 1 })).rejects.toThrow(
    "injected before compaction cursor checkpoint",
  );
  const job = (await store.listCompactionJobs())[0];
  const transaction =
    job?.transactionId === null ? undefined : await store.getTransaction(job?.transactionId ?? "");
  expect(job).toMatchObject({
    cursor: { sourceSegmentIndex: 0, sourceBlockIndex: 0 },
    outputBlockIds: [],
  });
  expect(transaction?.pendingBlockIds).toHaveLength(1);

  const result = await new MinnowDatabase(store).compactTable("events");
  expect(result.compacted).toBe(true);
  expect(await database.readTable("events")).toEqual([{ value: 1 }, { value: 2 }]);
  store.close();
});

it("resumes a ready compaction interrupted before manifest publication", async () => {
  const store = new MemoryBlockStore();
  const database = new MinnowDatabase(store);
  await database.createTable({
    name: "events",
    columns: [{ name: "value", type: "number" }],
  });
  await database.insert("events", { value: 1 });
  await database.insert("events", { value: 2 });

  let failBeforeCommit = true;
  const faultStore = new FaultInjectingBlockStore(store, (point) => {
    if (point === "beforeTransactionCommit" && failBeforeCommit) {
      failBeforeCommit = false;
      throw new Error("injected before compaction publication");
    }
  });
  await expect(new MinnowDatabase(faultStore).compactTable("events")).rejects.toThrow(
    "injected before compaction publication",
  );
  expect((await store.listCompactionJobs())[0]?.state).toBe("ready");

  const reopened = new MinnowDatabase(store);
  const result = await reopened.compactTable("events");
  expect(result.compacted).toBe(true);
  expect(await reopened.readTable("events")).toEqual([{ value: 1 }, { value: 2 }]);
  store.close();
});

for (const implementation of recoveryImplementations()) {
  it(`${implementation.name} durably cancels partial compaction without deleting artifacts`, async () => {
    const harness = await implementation.create();
    let store = harness.store;
    const database = new MinnowDatabase(store);
    await database.createTable({
      name: "events",
      columns: [{ name: "value", type: "number" }],
    });
    for (let value = 1; value <= 4; value += 1) {
      await database.insert("events", { value });
    }

    const sourceManifest = await store.getCurrentManifest();
    const progress = await database.compactTableStep("events", {
      maxBlocks: 1,
      targetBlockBytes: 9,
      outputCompression: "raw",
    });
    if (progress.jobId === null) throw new Error("Expected a persisted compaction job");
    const jobId = progress.jobId;
    const interrupted = await store.getCompactionJob(jobId);
    if (interrupted?.transactionId === null || interrupted?.transactionId === undefined) {
      throw new Error("Expected a linked compaction transaction");
    }
    const transactionId = interrupted.transactionId;
    const interruptedTransaction = await store.getTransaction(transactionId);
    const outputBlockId = interrupted.outputBlockIds[0];
    if (outputBlockId === undefined) throw new Error("Expected a checkpointed output block");
    const outputBytes = await store.getBlock(outputBlockId);
    if (outputBytes === undefined) throw new Error("Expected persisted compaction output");
    expect(interrupted).toMatchObject({
      state: "running",
      outputBlockIds: [outputBlockId],
      outputCursor: { outputIndex: 1, columnIndex: 0, rowStart: 1 },
    });

    expect(await database.cancelCompactionJob(jobId)).toEqual({
      jobId,
      state: "cancelled",
      publishedVersion: null,
    });
    const cancelled = await store.getCompactionJob(jobId);
    const cancelledTransaction = await store.getTransaction(transactionId);
    expect(cancelled).toMatchObject({
      state: "cancelled",
      revision: interrupted.revision + 1,
      outputBlockIds: interrupted.outputBlockIds,
      outputCursor: interrupted.outputCursor,
      processedRows: interrupted.processedRows,
    });
    expect(cancelledTransaction).toMatchObject({
      status: "aborted",
      revision: (interruptedTransaction?.revision ?? 0) + 1,
      pendingBlockIds: interrupted.outputBlockIds,
    });
    expect(await store.getCurrentManifest()).toEqual(sourceManifest);
    expect(await store.getBlock(outputBlockId)).toEqual(outputBytes);
    expect(await database.readTable("events")).toEqual([
      { value: 1 },
      { value: 2 },
      { value: 3 },
      { value: 4 },
    ]);

    store = await harness.reopen();
    const reopened = new MinnowDatabase(store);
    const persisted = await store.getCompactionJob(jobId);
    const persistedTransaction = await store.getTransaction(transactionId);
    expect(persisted).toMatchObject({ state: "cancelled", revision: cancelled?.revision });
    expect(persistedTransaction).toMatchObject({
      status: "aborted",
      revision: cancelledTransaction?.revision,
    });
    expect(await store.getBlock(outputBlockId)).toEqual(outputBytes);

    const repeated = await reopened.cancelCompactionJob(jobId);
    expect(repeated).toEqual({ jobId, state: "cancelled", publishedVersion: null });
    expect((await store.getCompactionJob(jobId))?.revision).toBe(persisted?.revision);
    expect((await store.getTransaction(transactionId))?.revision).toBe(
      persistedTransaction?.revision,
    );

    const resumeError = await reopened
      .resumeCompactionJob(jobId)
      .then<unknown>(() => undefined)
      .catch((error: unknown) => error);
    expect(resumeError).toBeInstanceOf(CompactionJobCancelledError);
    expect(resumeError).toMatchObject({ jobId });

    const retry = await reopened.compactTable("events", {
      maxBlocksPerStep: 1,
      targetBlockBytes: 9,
      outputCompression: "raw",
    });
    expect(retry).toMatchObject({ compacted: true, rowCount: 4 });
    expect(retry.jobId).not.toBe(jobId);
    expect(await store.getBlock(outputBlockId)).toEqual(outputBytes);
    expect(await reopened.readTable("events")).toEqual([
      { value: 1 },
      { value: 2 },
      { value: 3 },
      { value: 4 },
    ]);
    store.close();
  });
}

for (const implementation of implementations()) {
  it(`${implementation.name} cancels ready compaction before commit and retains prepared output`, async () => {
    const store = await implementation.create();
    const database = new MinnowDatabase(store);
    await database.createTable({
      name: "events",
      columns: [{ name: "value", type: "number" }],
    });
    await database.insert("events", { value: 1 });
    await database.insert("events", { value: 2 });
    const sourceManifest = await store.getCurrentManifest();

    let failBeforeCommit = true;
    const faultStore = new FaultInjectingBlockStore(store, (point) => {
      if (point === "beforeTransactionCommit" && failBeforeCommit) {
        failBeforeCommit = false;
        throw new Error("injected before cancellable compaction commit");
      }
    });
    await expect(new MinnowDatabase(faultStore).compactTable("events")).rejects.toThrow(
      "injected before cancellable compaction commit",
    );
    const ready = (await store.listCompactionJobs())[0];
    if (ready?.transactionId === null || ready?.transactionId === undefined) {
      throw new Error("Expected a linked compaction transaction");
    }
    if (ready.outputSegmentId === null) throw new Error("Expected a prepared output segment");
    const outputBytes = await Promise.all(
      ready.outputBlockIds.map(async (id) => {
        const bytes = await store.getBlock(id);
        if (bytes === undefined) throw new Error(`Expected compaction output ${id}`);
        return bytes;
      }),
    );
    expect(ready.state).toBe("ready");
    expect(await store.getSegment(ready.outputSegmentId)).toBeDefined();

    expect(await database.cancelCompactionJob(ready.id)).toEqual({
      jobId: ready.id,
      state: "cancelled",
      publishedVersion: null,
    });
    expect(await store.getCompactionJob(ready.id)).toMatchObject({ state: "cancelled" });
    expect(await store.getTransaction(ready.transactionId)).toMatchObject({ status: "aborted" });
    expect(await store.getCurrentManifest()).toEqual(sourceManifest);
    expect(await store.getSegment(ready.outputSegmentId)).toBeDefined();
    await Promise.all(
      ready.outputBlockIds.map(async (id, index) => {
        expect(await store.getBlock(id)).toEqual(outputBytes[index]);
      }),
    );
    expect(await database.readTable("events")).toEqual([{ value: 1 }, { value: 2 }]);
    store.close();
  });

  it(`${implementation.name} makes concurrent cancellation idempotent`, async () => {
    const store = await implementation.create();
    const database = new MinnowDatabase(store);
    await database.createTable({
      name: "events",
      columns: [{ name: "value", type: "number" }],
    });
    for (let value = 1; value <= 3; value += 1) {
      await database.insert("events", { value });
    }
    const progress = await database.compactTableStep("events", {
      maxBlocks: 1,
      targetBlockBytes: 9,
      outputCompression: "raw",
    });
    if (progress.jobId === null) throw new Error("Expected a persisted compaction job");
    const interrupted = await store.getCompactionJob(progress.jobId);
    if (interrupted?.transactionId === null || interrupted?.transactionId === undefined) {
      throw new Error("Expected a linked compaction transaction");
    }
    const interruptedTransaction = await store.getTransaction(interrupted.transactionId);

    const results = await Promise.all([
      new MinnowDatabase(store).cancelCompactionJob(progress.jobId),
      new MinnowDatabase(store).cancelCompactionJob(progress.jobId),
    ]);
    expect(results).toEqual([
      { jobId: progress.jobId, state: "cancelled", publishedVersion: null },
      { jobId: progress.jobId, state: "cancelled", publishedVersion: null },
    ]);
    expect(await store.getCompactionJob(progress.jobId)).toMatchObject({
      state: "cancelled",
      revision: interrupted.revision + 1,
    });
    expect(await store.getTransaction(interrupted.transactionId)).toMatchObject({
      status: "aborted",
      revision: (interruptedTransaction?.revision ?? 0) + 1,
    });
    store.close();
  });

  it(`${implementation.name} rejects cancellation of a missing compaction job`, async () => {
    const store = await implementation.create();
    await expect(new MinnowDatabase(store).cancelCompactionJob("missing-job")).rejects.toThrow(
      "Compaction job not found: missing-job",
    );
    expect(await store.listCompactionJobs()).toEqual([]);
    store.close();
  });
}

it("reconciles cancellation with a compaction transaction that already committed", async () => {
  const store = new MemoryBlockStore();
  const database = new MinnowDatabase(store);
  await database.createTable({
    name: "events",
    columns: [{ name: "value", type: "number" }],
  });
  await database.insert("events", { value: 1 });
  await database.insert("events", { value: 2 });
  const manifestCount = (await manifestRecords(store)).length;

  let signalCommitted: (() => void) | undefined;
  const committed = new Promise<void>((resolve) => {
    signalCommitted = resolve;
  });
  let releaseCommit: (() => void) | undefined;
  const commitRelease = new Promise<void>((resolve) => {
    releaseCommit = resolve;
  });
  const faultStore = new FaultInjectingBlockStore(store, async (point) => {
    if (point !== "afterTransactionCommit") return;
    signalCommitted?.();
    await commitRelease;
  });
  const completionPromise = new MinnowDatabase(faultStore).compactTable("events");
  await committed;

  const ready = (await store.listCompactionJobs())[0];
  if (ready?.transactionId === null || ready?.transactionId === undefined) {
    releaseCommit?.();
    throw new Error("Expected a committed compaction transaction");
  }
  const committedTransaction = await store.getTransaction(ready.transactionId);
  expect(ready.state).toBe("ready");
  expect(committedTransaction?.status).toBe("committed");
  if (committedTransaction?.committedVersion === null || committedTransaction === undefined) {
    releaseCommit?.();
    throw new Error("Expected the compaction transaction's committed manifest version");
  }
  const committedVersion = committedTransaction.committedVersion;

  let cancellation: Awaited<ReturnType<MinnowDatabase["cancelCompactionJob"]>>;
  try {
    cancellation = await database.cancelCompactionJob(ready.id);
  } finally {
    releaseCommit?.();
  }
  const completion = await completionPromise;
  expect(cancellation).toEqual({
    jobId: ready.id,
    state: "published",
    publishedVersion: committedVersion,
  });
  expect(completion).toMatchObject({ jobId: ready.id, compacted: true });
  expect(await store.getCompactionJob(ready.id)).toMatchObject({
    state: "published",
    publishedVersion: committedVersion,
  });
  expect(await manifestRecords(store)).toHaveLength(manifestCount + 1);
  expect(await database.readTable("events")).toEqual([{ value: 1 }, { value: 2 }]);
  store.close();
});

it("translates cancellation during an in-flight output checkpoint", async () => {
  const store = new MemoryBlockStore();
  const database = new MinnowDatabase(store);
  await database.createTable({
    name: "events",
    columns: [{ name: "value", type: "number" }],
  });
  for (let value = 1; value <= 3; value += 1) {
    await database.insert("events", { value });
  }
  const progress = await database.compactTableStep("events", {
    maxBlocks: 1,
    targetBlockBytes: 9,
    outputCompression: "raw",
  });
  if (progress.jobId === null) throw new Error("Expected a persisted compaction job");
  const manifestBeforeResume = await store.getCurrentManifest();

  let signalBlockWritten: (() => void) | undefined;
  const blockWritten = new Promise<void>((resolve) => {
    signalBlockWritten = resolve;
  });
  let releaseCheckpoint: (() => void) | undefined;
  const checkpointRelease = new Promise<void>((resolve) => {
    releaseCheckpoint = resolve;
  });
  let pauseNextBlockWrite = true;
  const faultStore = new FaultInjectingBlockStore(store, async (point) => {
    if (point !== "afterBlockWrite" || !pauseNextBlockWrite) return;
    pauseNextBlockWrite = false;
    signalBlockWritten?.();
    await checkpointRelease;
  });
  const resumePromise = new MinnowDatabase(faultStore).resumeCompactionJob(progress.jobId, {
    maxBlocks: 1,
  });
  await blockWritten;

  let cancellation: Awaited<ReturnType<MinnowDatabase["cancelCompactionJob"]>>;
  try {
    cancellation = await database.cancelCompactionJob(progress.jobId);
  } finally {
    releaseCheckpoint?.();
  }
  const resumeError = await resumePromise
    .then<unknown>(() => undefined)
    .catch((error: unknown) => error);

  expect(cancellation).toEqual({
    jobId: progress.jobId,
    state: "cancelled",
    publishedVersion: null,
  });
  expect(resumeError).toBeInstanceOf(CompactionJobCancelledError);
  expect(resumeError).toMatchObject({ jobId: progress.jobId });
  expect(await store.getCompactionJob(progress.jobId)).toMatchObject({ state: "cancelled" });
  expect(await store.getCurrentManifest()).toEqual(manifestBeforeResume);
  store.close();
});

it("restages checkpointed outputs with bounded reads after stale-transaction recovery", async () => {
  let now = new Date("2026-01-01T00:00:00.000Z");
  const store = new CountingMemoryBlockStore();
  const database = new MinnowDatabase(store, { now: () => new Date(now.getTime()) });
  await database.createTable({
    name: "events",
    columns: [{ name: "value", type: "number" }],
  });
  for (let value = 1; value <= 4; value += 1) {
    await database.insert("events", { value });
  }

  const progress = await database.compactTableStep("events", {
    maxBlocks: 3,
    targetBlockBytes: 9,
    outputCompression: "raw",
  });
  if (progress.jobId === null) throw new Error("Expected a persisted compaction job");
  const interrupted = await store.getCompactionJob(progress.jobId);
  expect(interrupted).toMatchObject({
    state: "running",
    outputBlockIds: [expect.any(String), expect.any(String), expect.any(String)],
    outputCursor: { outputIndex: 3, columnIndex: 0, rowStart: 3 },
  });
  if (interrupted?.transactionId === null || interrupted?.transactionId === undefined) {
    throw new Error("Expected a linked compaction transaction");
  }

  now = new Date("2026-01-01T01:00:00.000Z");
  const recovery = new TransactionManager(store, { now: () => new Date(now.getTime()) });
  const report = await recovery.recover({
    staleBefore: new Date("2026-01-01T00:30:00.000Z"),
  });
  expect(report.abortedTransactionIds).toContain(interrupted.transactionId);
  expect(report.retainedBlockIds).toEqual([...interrupted.outputBlockIds].sort());

  store.blockReadCalls = 0;
  store.blockIdsRead = [];
  store.singleBlockIdsRead = [];
  store.pendingBlockJournalSizes = [];
  const reopened = new MinnowDatabase(store, { now: () => new Date(now.getTime()) });
  const resumed = await reopened.resumeCompactionJob(progress.jobId, { maxBlocks: 1 });

  expect(resumed).toMatchObject({
    state: "published",
    outputBlockCount: 4,
    result: { compacted: true, rowCount: 4 },
  });
  expect(store.blockReadCalls).toBe(0);
  expect(
    store.singleBlockIdsRead.filter((blockId) => interrupted.outputBlockIds.includes(blockId)),
  ).toEqual(interrupted.outputBlockIds);
  // Three recovered blocks, the final block, then the output segment's atomic journal step.
  expect(store.pendingBlockJournalSizes).toEqual([3, 4, 4]);
  const completed = await store.getCompactionJob(progress.jobId);
  expect(completed?.transactionId).not.toBe(interrupted.transactionId);
  expect(await reopened.readTable("events")).toEqual([
    { value: 1 },
    { value: 2 },
    { value: 3 },
    { value: 4 },
  ]);
  store.close();
});

it("reconciles outputs when a crash follows replacement-transaction linkage", async () => {
  let now = new Date("2026-01-01T00:00:00.000Z");
  const store = new ReplacementRestageFaultMemoryBlockStore();
  const database = new MinnowDatabase(store, { now: () => new Date(now.getTime()) });
  await database.createTable({
    name: "events",
    columns: [{ name: "value", type: "number" }],
  });
  for (let value = 1; value <= 4; value += 1) {
    await database.insert("events", { value });
  }

  const progress = await database.compactTableStep("events", {
    maxBlocks: 3,
    targetBlockBytes: 9,
    outputCompression: "raw",
  });
  if (progress.jobId === null) throw new Error("Expected a persisted compaction job");
  const interrupted = await store.getCompactionJob(progress.jobId);
  if (interrupted?.transactionId === null || interrupted?.transactionId === undefined) {
    throw new Error("Expected a linked compaction transaction");
  }
  expect(interrupted.outputBlockIds).toHaveLength(3);

  now = new Date("2026-01-01T01:00:00.000Z");
  const recovery = new TransactionManager(store, { now: () => new Date(now.getTime()) });
  const report = await recovery.recover({
    staleBefore: new Date("2026-01-01T00:30:00.000Z"),
  });
  expect(report.abortedTransactionIds).toContain(interrupted.transactionId);
  store.failNextRestageRead = true;

  await expect(
    new MinnowDatabase(store, { now: () => new Date(now.getTime()) }).resumeCompactionJob(
      progress.jobId,
      { maxBlocks: 1 },
    ),
  ).rejects.toThrow("injected before replacement output restaging");
  const relinked = await store.getCompactionJob(progress.jobId);
  if (relinked?.transactionId === null || relinked?.transactionId === undefined) {
    throw new Error("Expected a replacement compaction transaction");
  }
  expect(relinked.transactionId).not.toBe(interrupted.transactionId);
  expect(relinked.outputBlockIds).toEqual(interrupted.outputBlockIds);
  expect(relinked.error).toBe("injected before replacement output restaging");
  expect(await store.getTransaction(relinked.transactionId)).toMatchObject({
    status: "active",
    pendingBlockIds: [],
  });

  store.blockReadCalls = 0;
  store.blockIdsRead = [];
  store.singleBlockIdsRead = [];
  store.pendingBlockJournalSizes = [];
  const reopened = new MinnowDatabase(store, { now: () => new Date(now.getTime()) });
  const resumed = await reopened.resumeCompactionJob(progress.jobId, { maxBlocks: 1 });

  expect(resumed).toMatchObject({
    state: "published",
    outputBlockCount: 4,
    result: { compacted: true, rowCount: 4 },
  });
  expect(store.blockReadCalls).toBe(0);
  expect(
    store.singleBlockIdsRead.filter((blockId) => interrupted.outputBlockIds.includes(blockId)),
  ).toEqual(interrupted.outputBlockIds);
  expect(store.pendingBlockJournalSizes).toEqual([3, 4, 4]);
  expect(await reopened.readTable("events")).toEqual([
    { value: 1 },
    { value: 2 },
    { value: 3 },
    { value: 4 },
  ]);
  store.close();
});

for (const implementation of recoveryImplementations()) {
  it(`${implementation.name} resumes checkpointed compaction after recovery aborts its transaction`, async () => {
    const harness = await implementation.create();
    let now = new Date("2026-01-01T00:00:00.000Z");
    let store = harness.store;
    const database = new MinnowDatabase(store, { now: () => new Date(now.getTime()) });
    await database.createTable({
      name: "events",
      columns: [{ name: "value", type: "number" }],
    });
    await database.insert("events", { value: 1 });
    await database.insert("events", { value: 2 });
    await database.insert("events", { value: 3 });

    const progress = await database.compactTableStep("events", {
      maxBlocks: 1,
      targetBlockBytes: 9,
      outputCompression: "raw",
    });
    if (progress.jobId === null) throw new Error("Expected a persisted compaction job");
    const interrupted = await store.getCompactionJob(progress.jobId);
    expect(interrupted).toMatchObject({
      state: "running",
      outputCursor: { outputIndex: 1, columnIndex: 0, rowStart: 1 },
      outputBlockIds: [expect.any(String)],
    });
    if (interrupted?.transactionId === null || interrupted?.transactionId === undefined) {
      throw new Error("Expected a linked compaction transaction");
    }

    now = new Date("2026-01-01T01:00:00.000Z");
    const recovery = new TransactionManager(store, { now: () => new Date(now.getTime()) });
    const report = await recovery.recover({
      staleBefore: new Date("2026-01-01T00:30:00.000Z"),
    });
    expect(report.abortedTransactionIds).toContain(interrupted.transactionId);

    store = await harness.reopen();
    const reopened = new MinnowDatabase(store, { now: () => new Date(now.getTime()) });
    const result = await reopened.compactTable("events", { maxBlocksPerStep: 1 });
    expect(result).toMatchObject({ jobId: progress.jobId, compacted: true, rowCount: 3 });
    expect(await reopened.readTable("events")).toEqual([{ value: 1 }, { value: 2 }, { value: 3 }]);
    const completed = await store.getCompactionJob(progress.jobId);
    expect(completed?.state).toBe("published");
    expect(completed?.transactionId).not.toBe(interrupted.transactionId);
    store.close();
  });

  it(`${implementation.name} resumes with a replacement transaction when recovery retains a prepared output segment`, async () => {
    const harness = await implementation.create();
    let now = new Date("2026-01-01T00:00:00.000Z");
    let store = harness.store;
    const database = new MinnowDatabase(store, { now: () => new Date(now.getTime()) });
    await database.createTable({
      name: "events",
      columns: [{ name: "value", type: "number" }],
    });
    await database.insert("events", { value: 1 });
    await database.insert("events", { value: 2 });

    let failBeforeCommit = true;
    const faultStore = new FaultInjectingBlockStore(store, (point) => {
      if (point === "beforeTransactionCommit" && failBeforeCommit) {
        failBeforeCommit = false;
        throw new Error("injected after compaction output preparation");
      }
    });
    await expect(
      new MinnowDatabase(faultStore, { now: () => new Date(now.getTime()) }).compactTable("events"),
    ).rejects.toThrow("injected after compaction output preparation");
    const interrupted = (await store.listCompactionJobs())[0];
    expect(interrupted?.state).toBe("ready");
    expect(typeof interrupted?.outputSegmentId).toBe("string");
    expect(typeof interrupted?.transactionId).toBe("string");
    if (interrupted?.transactionId === null || interrupted?.transactionId === undefined) {
      throw new Error("Expected a linked compaction transaction");
    }
    if (interrupted.outputSegmentId === null) throw new Error("Expected a prepared output segment");
    expect(await store.getSegment(interrupted.outputSegmentId)).toMatchObject({
      transactionId: interrupted.transactionId,
    });

    now = new Date("2026-01-01T01:00:00.000Z");
    const recovery = new TransactionManager(store, { now: () => new Date(now.getTime()) });
    const report = await recovery.recover({
      staleBefore: new Date("2026-01-01T00:30:00.000Z"),
    });
    expect(report.abortedTransactionIds).toContain(interrupted.transactionId);
    expect(report.retainedSegmentIds).toContain(interrupted.outputSegmentId);

    store = await harness.reopen();
    const reopened = new MinnowDatabase(store, { now: () => new Date(now.getTime()) });
    let resumed = await reopened.resumeCompactionJob(interrupted.id);
    while (resumed.result === null) {
      resumed = await reopened.resumeCompactionJob(interrupted.id);
    }
    expect(resumed).toMatchObject({ state: "published", result: { compacted: true, rowCount: 2 } });
    expect(await reopened.readTable("events")).toEqual([{ value: 1 }, { value: 2 }]);
    store.close();
  });
}

it("resumes a partial compaction with a replacement transaction when recovery retains blocks", async () => {
  let now = new Date("2026-01-01T00:00:00.000Z");
  const store = new MemoryBlockStore();
  const database = new MinnowDatabase(store, { now: () => new Date(now.getTime()) });
  await database.createTable({
    name: "events",
    columns: [{ name: "value", type: "number" }],
  });
  await database.insert("events", { value: 1 });
  await database.insert("events", { value: 2 });
  await database.insert("events", { value: 3 });

  const progress = await database.compactTableStep("events", {
    maxBlocks: 1,
    targetBlockBytes: 9,
    outputCompression: "raw",
  });
  if (progress.jobId === null) throw new Error("Expected a persisted compaction job");
  const interrupted = await store.getCompactionJob(progress.jobId);
  if (interrupted?.transactionId === null || interrupted?.transactionId === undefined) {
    throw new Error("Expected a linked compaction transaction");
  }

  now = new Date("2026-01-01T01:00:00.000Z");
  const recovery = new TransactionManager(store, { now: () => new Date(now.getTime()) });
  const report = await recovery.recover({
    staleBefore: new Date("2026-01-01T00:30:00.000Z"),
    removePendingBlocks: false,
  });
  expect(report.abortedTransactionIds).toContain(interrupted.transactionId);

  const reopened = new MinnowDatabase(store, { now: () => new Date(now.getTime()) });
  let resumed = await reopened.resumeCompactionJob(progress.jobId);
  while (resumed.result === null) resumed = await reopened.resumeCompactionJob(progress.jobId);
  expect(resumed).toMatchObject({
    jobId: progress.jobId,
    state: "published",
    result: { compacted: true, rowCount: 3 },
  });
  expect(await store.getTransaction(interrupted.transactionId)).toMatchObject({
    status: "aborted",
  });
  const completed = await store.getCompactionJob(progress.jobId);
  expect(completed?.state).toBe("published");
  expect(completed?.transactionId).not.toBe(interrupted.transactionId);
  expect(await reopened.readTable("events")).toEqual([{ value: 1 }, { value: 2 }, { value: 3 }]);
  store.close();
});

it("returns a published compaction job repeatedly without publishing another manifest", async () => {
  const store = new MemoryBlockStore();
  const database = new MinnowDatabase(store);
  await database.createTable({
    name: "events",
    columns: [{ name: "value", type: "number" }],
  });
  await database.insert("events", { value: 1 });
  await database.insert("events", { value: 2 });

  const result = await database.compactTable("events");
  if (result.jobId === undefined) throw new Error("Expected a persisted compaction job");
  const published = await store.getCompactionJob(result.jobId);
  if (published === undefined) throw new Error("Expected the published compaction job");
  if (published.transactionId === null) throw new Error("Expected a published transaction");
  const publishedTransaction = await store.getTransaction(published.transactionId);
  const manifestCount = (await manifestRecords(store)).length;
  const transactionCount = (await transactionRecords(store)).length;

  const first = await new MinnowDatabase(store).resumeCompactionJob(result.jobId);
  const second = await new MinnowDatabase(store).resumeCompactionJob(result.jobId);
  const firstCancellation = await database.cancelCompactionJob(result.jobId);
  const secondCancellation = await database.cancelCompactionJob(result.jobId);

  expect(first).toEqual(second);
  expect(first).toMatchObject({
    state: "published",
    result: { jobId: result.jobId, version: result.version },
  });
  expect(firstCancellation).toEqual(secondCancellation);
  expect(firstCancellation).toEqual({
    jobId: result.jobId,
    state: "published",
    publishedVersion: result.version,
  });
  expect(await store.getCompactionJob(result.jobId)).toEqual(published);
  expect(await store.getTransaction(published.transactionId)).toEqual(publishedTransaction);
  expect(await manifestRecords(store)).toHaveLength(manifestCount);
  expect(await transactionRecords(store)).toHaveLength(transactionCount);
  store.close();
});

it("aborts the unlinked transaction when initial compaction coordinators race", async () => {
  const store = new InitialCompactionPlanningBarrierStore();
  const setup = new MinnowDatabase(store);
  await setup.createTable({
    name: "events",
    columns: [{ name: "value", type: "number" }],
  });
  await setup.insert("events", { value: 1 });
  await setup.insert("events", { value: 2 });
  await setup.insert("events", { value: 3 });
  await setup.insert("events", { value: 4 });

  let leftId = 0;
  let rightId = 0;
  const left = new MinnowDatabase(store, {
    createId: () => `left-coordinator-${String((leftId += 1))}`,
  });
  const right = new MinnowDatabase(store, {
    createId: () => `right-coordinator-${String((rightId += 1))}`,
  });

  await Promise.allSettled([
    left.compactTableStep("events", { maxBlocks: 1 }),
    right.compactTableStep("events", { maxBlocks: 1 }),
  ]);

  const job = (await store.listCompactionJobs())[0];
  expect(job?.transactionId).toEqual(expect.any(String));
  const coordinatorTransactions = (await transactionRecords(store)).filter(
    (record) =>
      record.id.startsWith("left-coordinator-") || record.id.startsWith("right-coordinator-"),
  );
  expect(coordinatorTransactions).toHaveLength(2);
  expect(
    coordinatorTransactions.filter(
      (record) => record.id !== job?.transactionId && record.status === "active",
    ),
  ).toEqual([]);
  expect(coordinatorTransactions.find((record) => record.id !== job?.transactionId)?.status).toBe(
    "aborted",
  );
  store.close();
});

for (const implementation of implementations()) {
  it(`${implementation.name} merges mutation deltas with stable row IDs and can merge new deltas again`, async () => {
    const store = await implementation.create();
    const tableName = `merge_${implementation.name}`;
    const fixture = await createMutationCompactionFixture(store, tableName);

    const first = await fixture.database.compactTable(tableName, {
      maxBlocksPerStep: 1,
      targetBlockBytes: 64,
      outputCompression: "raw",
    });
    if (first.jobId === undefined || first.version === null) {
      throw new Error("Expected a published mutation compaction");
    }
    expect(first).toMatchObject({
      compacted: true,
      sourceSegmentCount: fixture.sourceSegmentIds.length,
      sourceBlockCount: fixture.sourceBlockIds.length,
      rowCount: fixture.expectedRows.length,
      supersededBlockCount: fixture.sourceBlockIds.length,
      physicallyReclaimedBytes: 0,
      outputCompression: "raw",
    });
    const firstOutput = await assertPublishedMutationMerge(
      store,
      fixture.database,
      tableName,
      first.jobId,
      fixture.expectedRows,
      fixture.expectedRowIds,
    );
    expect(appendOnlyCompactionEligible(firstOutput)).toBe(false);
    const firstManifest = await store.getCurrentManifest();
    const firstManifestBlockIds =
      firstManifest === undefined ? [] : await manifestBlockIdsAt(store, firstManifest.version);
    expect(
      fixture.sourceBlockIds.every((blockId) => !firstManifestBlockIds.includes(blockId)),
    ).toBe(true);
    for (const snapshot of fixture.snapshots) {
      expect(await fixture.database.readTable(tableName, snapshot.version)).toEqual(snapshot.rows);
    }
    expect(await fixture.database.readTable(tableName, first.version)).toEqual(
      fixture.expectedRows,
    );

    await fixture.database.update(tableName, "b@example.com", { active: true });
    await fixture.database.deleteBatch(tableName, { keys: ["d@example.com"] });
    const postMergeUpsert = await fixture.database.upsertBatch(tableName, {
      columns: {
        email: ["a@example.com", "e@example.com"],
        score: [11, 5],
        active: [true, false],
        note: ["a3", "e"],
      },
    });
    const postMergeUpsertSegment = await requiredSegment(store, postMergeUpsert.segmentId ?? "");
    const postMergeCandidateIds = expandSegmentRowIds(postMergeUpsertSegment);
    const secondExpectedRowIds = [
      requiredItem(fixture.expectedRowIds, 0, "B row ID after first merge"),
      requiredItem(fixture.expectedRowIds, 2, "A row ID after first merge"),
      requiredItem(postMergeCandidateIds, 1, "new E row ID"),
    ];
    expect(postMergeCandidateIds[0]).not.toBe(secondExpectedRowIds[1]);
    const secondExpectedRows: DatabaseRow[] = [
      { email: "b@example.com", score: 21, active: true, note: null },
      { email: "a@example.com", score: 11, active: true, note: "a3" },
      { email: "e@example.com", score: 5, active: false, note: "e" },
    ];
    expect(await fixture.database.readTable(tableName)).toEqual(secondExpectedRows);
    const preSecondMergeSnapshot = await store.getCurrentManifest();
    if (preSecondMergeSnapshot === undefined) throw new Error("Expected a second merge snapshot");

    const second = await fixture.database.compactTable(tableName, {
      maxBlocksPerStep: 1,
      targetBlockBytes: 64,
      outputCompression: "raw",
    });
    if (second.jobId === undefined) throw new Error("Expected a second mutation compaction");
    expect(second).toMatchObject({
      compacted: true,
      sourceSegmentCount: 4,
      rowCount: secondExpectedRows.length,
      outputCompression: "raw",
    });
    await assertPublishedMutationMerge(
      store,
      fixture.database,
      tableName,
      second.jobId,
      secondExpectedRows,
      secondExpectedRowIds,
    );
    expect(await fixture.database.readTable(tableName, first.version)).toEqual(
      fixture.expectedRows,
    );
    expect(await fixture.database.readTable(tableName, preSecondMergeSnapshot.version)).toEqual(
      secondExpectedRows,
    );
    for (const snapshot of fixture.snapshots) {
      expect(await fixture.database.readTable(tableName, snapshot.version)).toEqual(snapshot.rows);
    }
    store.close();
  });

  it(`${implementation.name} publishes an all-deleted merge without a globally visible empty segment`, async () => {
    const store = await implementation.create();
    const tableName = `empty_merge_${implementation.name}`;
    const database = new MinnowDatabase(store, { compression: "raw" });
    await database.createTable({
      name: tableName,
      uniqueKey: "email",
      columns: [
        { name: "email", type: "string" },
        { name: "score", type: "number" },
      ],
    });
    const inserted = await database.insert(tableName, { email: "a@example.com", score: 1 });
    const insertedSegment = await requiredSegment(store, inserted.segmentId);
    const insertedRowId = requiredItem(expandSegmentRowIds(insertedSegment), 0, "inserted row ID");
    await database.deleteBatch(tableName, { keys: ["a@example.com"] });
    const sourceBlockIds = await currentManifestBlockIds(store);

    const result = await database.compactTable(tableName, {
      maxBlocksPerStep: 1,
      outputCompression: "raw",
    });
    if (result.jobId === undefined) throw new Error("Expected an empty merge job");
    expect(result).toMatchObject({
      compacted: true,
      sourceSegmentCount: 2,
      sourceBlockCount: sourceBlockIds.length,
      outputSegmentId: null,
      outputBlockCount: 0,
      rowCount: 0,
      supersededBlockCount: sourceBlockIds.length,
    });
    const job = await store.getCompactionJob(result.jobId);
    expect(job).toMatchObject({
      state: "published",
      outputBlockIds: [],
      outputSegmentId: null,
      processedRows: 0,
      rewritePlan: { kind: "merge-v1", totalRows: 0, rowIdSpans: [], outputs: [] },
    });
    expect(await currentManifestBlockIds(store)).toEqual([]);
    expect(await database.readTable(tableName)).toEqual([]);
    expect(await allVisibleSegments(database, tableName)).toEqual([]);
    expect(await database.readTable(tableName, inserted.version)).toEqual([
      { email: "a@example.com", score: 1 },
    ]);
    const reinserted = await database.insert(tableName, { email: "a@example.com", score: 2 });
    const reinsertedSegment = await requiredSegment(store, reinserted.segmentId);
    expect(
      requiredItem(expandSegmentRowIds(reinsertedSegment), 0, "reinserted row ID"),
    ).toBeGreaterThan(insertedRowId);
    expect(await database.readTable(tableName)).toEqual([{ email: "a@example.com", score: 2 }]);
    store.close();
  });
}

for (const implementation of recoveryImplementations()) {
  it(`${implementation.name} resumes a checkpointed mutation merge with its persisted row identities`, async () => {
    const harness = await implementation.create();
    let store = harness.store;
    const tableName = `resume_merge_${implementation.name.replaceAll(" ", "_")}`;
    const fixture = await createMutationCompactionFixture(store, tableName);
    let progress = await fixture.database.compactTableStep(tableName, {
      maxBlocks: 1,
      targetBlockBytes: 64,
      outputCompression: "gzip",
    });
    if (progress.jobId === null) throw new Error("Expected a checkpointed mutation merge");
    const jobId = progress.jobId;
    expect(progress).toMatchObject({ state: "running", outputBlockCount: 1, result: null });
    expect(await store.getCompactionJob(jobId)).toMatchObject({
      rewritePlan: {
        kind: "merge-v1",
        rowIdSpans: canonicalRowIdSpans(fixture.expectedRowIds),
      },
      outputBlockIds: [expect.any(String)],
    });

    store = await harness.reopen();
    const reopened = new MinnowDatabase(store, { compression: "raw", rowsPerBlock: 1 });
    while (progress.result === null) {
      progress = await reopened.resumeCompactionJob(jobId, { maxBlocks: 1 });
    }
    expect(progress).toMatchObject({
      state: "published",
      result: { compacted: true, outputCompression: "gzip", rowCount: 3 },
    });
    await assertPublishedMutationMerge(
      store,
      reopened,
      tableName,
      jobId,
      fixture.expectedRows,
      fixture.expectedRowIds,
    );
    for (const snapshot of fixture.snapshots) {
      expect(await reopened.readTable(tableName, snapshot.version)).toEqual(snapshot.rows);
    }
    store.close();
  });
}

it("recovers a mutation-merge block whose durable cursor checkpoint was lost", async () => {
  const store = new CheckpointFaultMemoryBlockStore();
  const tableName = "checkpoint_merge_accounts";
  const fixture = await createMutationCompactionFixture(store, tableName);

  await expect(
    fixture.database.compactTableStep(tableName, {
      maxBlocks: 1,
      targetBlockBytes: 64,
      outputCompression: "raw",
    }),
  ).rejects.toThrow("injected before compaction cursor checkpoint");
  const interrupted = (await store.listCompactionJobs(fixture.tableId))[0];
  if (interrupted === undefined) throw new Error("Expected an interrupted merge job");
  expect(interrupted).toMatchObject({
    state: "running",
    outputBlockIds: [],
    rewritePlan: { kind: "merge-v1" },
  });
  if (interrupted.transactionId === null) throw new Error("Expected a merge transaction");
  expect((await store.getTransaction(interrupted.transactionId))?.pendingBlockIds).toHaveLength(1);

  const reopened = new MinnowDatabase(store);
  const result = await reopened.compactTable(tableName, { maxBlocksPerStep: 1 });
  expect(result).toMatchObject({ jobId: interrupted.id, compacted: true, rowCount: 3 });
  await assertPublishedMutationMerge(
    store,
    reopened,
    tableName,
    interrupted.id,
    fixture.expectedRows,
    fixture.expectedRowIds,
  );
  store.close();
});

it("preserves logical row order when row-ID reservation order differs from commit order", async () => {
  const store = new FirstCommitBarrierMemoryBlockStore();
  const database = new MinnowDatabase(store);
  await database.createTable({
    name: "reverse_ids",
    uniqueKey: "email",
    columns: [
      { name: "email", type: "string" },
      { name: "score", type: "number" },
    ],
  });

  const firstPromise = database.insert("reverse_ids", { email: "first@example.com", score: 1 });
  await store.firstCommitReached;
  // One database runs its writes in turn, so the overtaking insert comes from a second
  // instance over the same store — another tab, whose commit lands while the first is held.
  const overtaking = new MinnowDatabase(store);
  let second;
  try {
    second = await overtaking.insert("reverse_ids", { email: "second@example.com", score: 2 });
  } finally {
    store.releaseFirstCommit();
  }
  const first = await firstPromise;
  expect(second.version).toBeLessThan(first.version);
  const firstId = requiredItem(
    expandSegmentRowIds(await requiredSegment(store, first.segmentId)),
    0,
    "first reserved row ID",
  );
  const secondId = requiredItem(
    expandSegmentRowIds(await requiredSegment(store, second.segmentId)),
    0,
    "second reserved row ID",
  );
  expect(firstId).toBeLessThan(secondId);
  expect(await database.readTable("reverse_ids")).toEqual([
    { email: "second@example.com", score: 2 },
    { email: "first@example.com", score: 1 },
  ]);
  await database.update("reverse_ids", "first@example.com", { score: 10 });

  const result = await database.compactTable("reverse_ids", { outputCompression: "raw" });
  if (result.jobId === undefined) throw new Error("Expected a reverse-order merge job");
  await assertPublishedMutationMerge(
    store,
    database,
    "reverse_ids",
    result.jobId,
    [
      { email: "second@example.com", score: 2 },
      { email: "first@example.com", score: 10 },
    ],
    [secondId, firstId],
  );
  store.close();
});

it("rebases a mutation merge across later IndexedDB deltas without absorbing them", async () => {
  const indexedDB = new IDBFactory();
  const name = crypto.randomUUID();
  const compactorStore = await IndexedDbBlockStore.open({ name, indexedDB });
  const writerStore = await IndexedDbBlockStore.open({ name, indexedDB });
  const tableName = "concurrent_merge_accounts";
  const fixture = await createMutationCompactionFixture(compactorStore, tableName);
  const writer = new MinnowDatabase(writerStore);

  const progress = await fixture.database.compactTableStep(tableName, {
    maxBlocks: 1,
    targetBlockBytes: 64,
    outputCompression: "raw",
  });
  if (progress.jobId === null) throw new Error("Expected a concurrent mutation merge");
  const sourceVersion = fixture.snapshots.at(-1)?.version;
  if (sourceVersion === undefined) throw new Error("Expected a mutation source version");
  await writer.update(tableName, "b@example.com", { active: true });
  await writer.deleteBatch(tableName, { keys: ["d@example.com"] });
  const laterUpsert = await writer.upsertBatch(tableName, {
    columns: {
      email: ["a@example.com", "e@example.com"],
      score: [11, 5],
      active: [true, false],
      note: ["a3", "e"],
    },
  });
  const laterCandidateIds = expandSegmentRowIds(
    await requiredSegment(writerStore, laterUpsert.segmentId ?? ""),
  );
  const expectedRows: DatabaseRow[] = [
    { email: "b@example.com", score: 21, active: true, note: null },
    { email: "a@example.com", score: 11, active: true, note: "a3" },
    { email: "e@example.com", score: 5, active: false, note: "e" },
  ];
  const expectedRowIds = [
    requiredItem(fixture.expectedRowIds, 0, "concurrent B row ID"),
    requiredItem(fixture.expectedRowIds, 2, "concurrent A row ID"),
    requiredItem(laterCandidateIds, 1, "concurrent E row ID"),
  ];

  let resumed = await fixture.database.resumeCompactionJob(progress.jobId, { maxBlocks: 1 });
  while (resumed.result === null) {
    resumed = await fixture.database.resumeCompactionJob(progress.jobId, { maxBlocks: 1 });
  }
  expect(resumed.result).toMatchObject({ compacted: true, sourceSegmentCount: 6 });
  expect(await fixture.database.readTable(tableName)).toEqual(expectedRows);
  expect(await fixture.database.readTable(tableName, sourceVersion)).toEqual(fixture.expectedRows);
  expect(await allVisibleSegments(fixture.database, tableName)).toHaveLength(4);
  const firstOutput = await requiredSegment(
    compactorStore,
    resumed.result.outputSegmentId ?? "missing-output",
  );
  expect(expandSegmentRowIds(firstOutput)).toEqual(fixture.expectedRowIds);

  const consolidated = await fixture.database.compactTable(tableName, {
    maxBlocksPerStep: 1,
    outputCompression: "gzip",
  });
  if (consolidated.jobId === undefined) throw new Error("Expected a post-rebase merge");
  await assertPublishedMutationMerge(
    compactorStore,
    fixture.database,
    tableName,
    consolidated.jobId,
    expectedRows,
    expectedRowIds,
  );
  expect(await fixture.database.readTable(tableName, sourceVersion)).toEqual(fixture.expectedRows);
  compactorStore.close();
  writerStore.close();
});

it("canonicalizes a hostile concurrent logical order and safely rebases a mutation merge", async () => {
  const store = new MemoryBlockStore();
  const fixture = await createMutationRebaseGuardFixture(store, "interleaved_merge_guard");
  const plan = fixture.job.rewritePlan;
  if (plan.kind !== "merge-v1") throw new Error("Expected an interleaved merge plan");
  const earliest = requiredItem(plan.sourceSegments, 0, "earliest guarded source");
  const latest = requiredItem(
    plan.sourceSegments,
    plan.sourceSegments.length - 1,
    "latest guarded source",
  );
  const interleavedLogicalOrder = 1;
  expect(interleavedLogicalOrder).toBeGreaterThan(earliest.logicalOrder);
  expect(interleavedLogicalOrder).toBeLessThan(latest.logicalOrder);
  const manifestBeforeConcurrent = await store.getCurrentManifest();
  if (manifestBeforeConcurrent === undefined) throw new Error("Expected a guarded source manifest");
  const concurrent = await commitLowLevelDeleteSegment(store, fixture.table, {
    segmentId: "interleaved-concurrent-delete",
    logicalOrder: interleavedLogicalOrder,
    key: "missing@example.com",
  });
  const concurrentManifestBlockIds = await currentManifestBlockIds(store);
  expect(concurrent.manifestVersion).toBe(manifestBeforeConcurrent.version + 1);
  expect(concurrentManifestBlockIds).toContain(concurrent.blockId);
  expect((await requiredSegment(store, concurrent.segmentId)).logicalOrder).toBe(
    concurrent.manifestVersion,
  );

  const resumed = await fixture.database.resumeCompactionJob(fixture.job.id, { maxBlocks: 16 });
  expect(resumed).toMatchObject({ state: "published", result: { compacted: true } });
  const published = await store.getCompactionJob(fixture.job.id);
  expect(published).toMatchObject({ state: "published" });
  if (fixture.job.transactionId === null) throw new Error("Expected a guarded transaction");
  expect(await store.getTransaction(fixture.job.transactionId)).toMatchObject({
    status: "committed",
  });
  expect((await store.getCurrentManifest())?.version).toBe(concurrent.manifestVersion + 1);
  expect(await fixture.database.readTable(fixture.table.name)).toEqual(fixture.expectedRows);
  store.close();
});

it("canonicalizes a hostile equal logical order before rebasing a mutation merge", async () => {
  const store = new MemoryBlockStore();
  const fixture = await createMutationRebaseGuardFixture(store, "equal_order_merge_guard");
  const plan = fixture.job.rewritePlan;
  if (plan.kind !== "merge-v1") throw new Error("Expected an equal-order merge plan");
  const latestPlannedCommit = Math.max(
    ...plan.sourceSegments.map((segment) => segment.committedVersion),
  );
  const manifestBeforeConcurrent = await store.getCurrentManifest();
  if (manifestBeforeConcurrent === undefined) throw new Error("Expected a guarded source manifest");
  const concurrent = await commitLowLevelDeleteSegment(store, fixture.table, {
    segmentId: "equal-base-order-concurrent-delete",
    logicalOrder: plan.logicalOrder,
    key: "missing@example.com",
  });
  const concurrentManifestBlockIds = await currentManifestBlockIds(store);
  expect(concurrent.manifestVersion).toBeGreaterThan(latestPlannedCommit);
  expect(concurrent.manifestVersion).toBe(manifestBeforeConcurrent.version + 1);
  expect(concurrentManifestBlockIds).toContain(concurrent.blockId);
  expect((await requiredSegment(store, concurrent.segmentId)).logicalOrder).toBe(
    concurrent.manifestVersion,
  );

  const resumed = await fixture.database.resumeCompactionJob(fixture.job.id, { maxBlocks: 16 });
  expect(resumed).toMatchObject({ state: "published", result: { compacted: true } });
  const published = await store.getCompactionJob(fixture.job.id);
  expect(published).toMatchObject({ state: "published" });
  if (fixture.job.transactionId === null) throw new Error("Expected a guarded transaction");
  expect(await store.getTransaction(fixture.job.transactionId)).toMatchObject({
    status: "committed",
  });
  expect((await store.getCurrentManifest())?.version).toBe(concurrent.manifestVersion + 1);
  expect(await fixture.database.readTable(fixture.table.name)).toEqual(fixture.expectedRows);
  store.close();
});

it("aborts a mutation merge before supersession when a concurrent segment aliases a source block", async () => {
  const store = new MemoryBlockStore();
  const fixture = await createMutationRebaseGuardFixture(store, "shared_block_merge_guard");
  const plan = fixture.job.rewritePlan;
  if (plan.kind !== "merge-v1") throw new Error("Expected a shared-block merge plan");
  const keyBlockId = plan.sourceSegments
    .flatMap((segment) => segment.columns)
    .find((column) => column.columnId === plan.keyColumnId)?.sourceBlocks[0]?.blockId;
  if (keyBlockId === undefined) throw new Error("Expected a guarded source key block");
  const manifestBeforeConcurrent = await store.getCurrentManifest();
  if (manifestBeforeConcurrent === undefined) throw new Error("Expected a guarded source manifest");
  const manifestBlockIdsBeforeConcurrent = await manifestBlockIdsAt(
    store,
    manifestBeforeConcurrent.version,
  );
  const concurrent = await commitLowLevelDeleteSegment(store, fixture.table, {
    segmentId: "shared-source-block-delete",
    logicalOrder: 10,
    blockId: keyBlockId,
  });
  const concurrentManifest = await store.getCurrentManifest();
  const concurrentManifestBlockIds = await currentManifestBlockIds(store);
  expect(concurrent.manifestVersion).toBe(manifestBeforeConcurrent.version + 1);
  expect(concurrentManifestBlockIds).toEqual(manifestBlockIdsBeforeConcurrent);

  await expect(
    fixture.database.resumeCompactionJob(fixture.job.id, { maxBlocks: 16 }),
  ).rejects.toThrow(`Concurrent segment shares a compaction source block: ${concurrent.segmentId}`);
  const aborted = await store.getCompactionJob(fixture.job.id);
  expect(aborted).toMatchObject({
    state: "aborted",
    error: `Concurrent segment shares a compaction source block: ${concurrent.segmentId}`,
    publishedVersion: null,
  });
  if (fixture.job.transactionId === null) throw new Error("Expected a guarded transaction");
  expect(await store.getTransaction(fixture.job.transactionId)).toMatchObject({
    status: "aborted",
  });
  expect(await store.getCurrentManifest()).toEqual(concurrentManifest);
  expect(
    fixture.job.sourceBlockIds.every((blockId) => concurrentManifestBlockIds.includes(blockId)),
  ).toBe(true);
  expect(
    fixture.job.outputBlockIds.every((blockId) => !concurrentManifestBlockIds.includes(blockId)),
  ).toBe(true);
  store.close();
});

it("aborts a mutation merge when a segment from another table aliases a global source block", async () => {
  const store = new MemoryBlockStore();
  const setup = new MinnowDatabase(store);
  await setup.createTable({
    name: "source_alias_owner",
    uniqueKey: "alias",
    columns: [{ name: "alias", type: "string" }],
  });
  const fixture = await createMutationRebaseGuardFixture(store, "cross_table_block_merge_guard");
  const plan = fixture.job.rewritePlan;
  if (plan.kind !== "merge-v1") throw new Error("Expected a cross-table merge plan");
  const keyBlockId = plan.sourceSegments
    .flatMap((segment) => segment.columns)
    .find((column) => column.columnId === plan.keyColumnId)?.sourceBlocks[0]?.blockId;
  if (keyBlockId === undefined) throw new Error("Expected a cross-table guarded source block");
  const aliasTable = await store.getTableByName("source_alias_owner");
  if (aliasTable === undefined) throw new Error("Expected a source-alias table");
  expect(aliasTable.id).not.toBe(fixture.table.id);
  const manifestBeforeConcurrent = await store.getCurrentManifest();
  if (manifestBeforeConcurrent === undefined) throw new Error("Expected a guarded source manifest");
  const manifestBlockIdsBeforeConcurrent = await manifestBlockIdsAt(
    store,
    manifestBeforeConcurrent.version,
  );
  const concurrent = await commitLowLevelDeleteSegment(store, aliasTable, {
    segmentId: "cross-table-shared-source-block-delete",
    logicalOrder: 0,
    blockId: keyBlockId,
  });
  expect((await store.getSegment(concurrent.segmentId))?.tableId).toBe(aliasTable.id);
  const concurrentManifest = await store.getCurrentManifest();
  const concurrentManifestBlockIds = await currentManifestBlockIds(store);
  expect(concurrent.manifestVersion).toBe(manifestBeforeConcurrent.version + 1);
  expect(concurrentManifestBlockIds).toEqual(manifestBlockIdsBeforeConcurrent);

  await expect(
    fixture.database.resumeCompactionJob(fixture.job.id, { maxBlocks: 16 }),
  ).rejects.toThrow(`Concurrent segment shares a compaction source block: ${concurrent.segmentId}`);
  const aborted = await store.getCompactionJob(fixture.job.id);
  expect(aborted).toMatchObject({
    state: "aborted",
    error: `Concurrent segment shares a compaction source block: ${concurrent.segmentId}`,
    publishedVersion: null,
  });
  if (fixture.job.transactionId === null) throw new Error("Expected a guarded transaction");
  expect(await store.getTransaction(fixture.job.transactionId)).toMatchObject({
    status: "aborted",
  });
  expect(await store.getCurrentManifest()).toEqual(concurrentManifest);
  expect(
    fixture.job.sourceBlockIds.every((blockId) => concurrentManifestBlockIds.includes(blockId)),
  ).toBe(true);
  expect(
    fixture.job.outputBlockIds.every((blockId) => !concurrentManifestBlockIds.includes(blockId)),
  ).toBe(true);
  store.close();
});

for (const implementation of implementations()) {
  it(`${implementation.name} repeatedly compacts bounded oldest append prefixes with subset metrics`, async () => {
    const store = await implementation.create();
    const tableName = `bounded_append_${implementation.name}`;
    const database = new MinnowDatabase(store, { compression: "raw" });
    await database.createTable({
      name: tableName,
      columns: [{ name: "value", type: "number" }],
    });
    const inserts = [];
    for (let value = 1; value <= 6; value += 1) {
      inserts.push(await database.insert(tableName, { value }));
    }
    const originalSegmentIds = inserts.map((insert) => insert.segmentId);
    const expectedRows = inserts.map((_insert, index) => ({ value: index + 1 }));
    const snapshots = inserts.map((insert, index) => ({
      version: insert.version,
      rows: expectedRows.slice(0, index + 1),
    }));
    expect((await allVisibleSegments(database, tableName)).map((segment) => segment.id)).toEqual(
      originalSegmentIds,
    );

    let anchorSegmentId: string | null = null;
    for (let jobIndex = 0; jobIndex < 3; jobIndex += 1) {
      const promotedIds = originalSegmentIds.slice(jobIndex * 2, jobIndex * 2 + 2);
      const expectedSourceIds =
        anchorSegmentId === null ? promotedIds : [anchorSegmentId, ...promotedIds];
      const level0Stats = await compactionSourceStats(store, promotedIds);
      const result = await database.compactTable(tableName, {
        minimumLevel0Segments: 2,
        maxLevel0Segments: 2,
        maxLevel0StoredBytes: 1024 * 1024,
        targetBlockBytes: 1024,
        outputCompression: "raw",
        maxBlocksPerStep: 1,
      });
      if (result.jobId === undefined || result.outputSegmentId === null) {
        throw new Error("Expected a bounded append output");
      }
      const sourceStats = await assertPersistedCompactionSelection(
        store,
        result.jobId,
        expectedSourceIds,
        anchorSegmentId,
      );
      const selectedRowCount = (jobIndex + 1) * 2;
      expect(result).toMatchObject({
        compacted: true,
        sourceSegmentCount: expectedSourceIds.length,
        sourceBlockCount: sourceStats.blockIds.length,
        rowCount: selectedRowCount,
        sourceStoredBytes: sourceStats.storedBytes,
        level0SourceStoredBytes: level0Stats.storedBytes,
        supersededBlockCount: sourceStats.blockIds.length,
        physicallyReclaimedBytes: 0,
      });
      expect(result.anchorSourceStoredBytes).toBe(
        sourceStats.storedBytes - level0Stats.storedBytes,
      );
      expect(result.compactionWriteAmplification).toBe(
        result.outputStoredBytes / level0Stats.storedBytes,
      );
      expect(result.metrics).toMatchObject({
        logicalBytes: result.outputLogicalBytes,
        storedBytes: result.outputStoredBytes,
        writeAmplification:
          result.outputStoredBytes / (result.outputLogicalBytes ?? Number.POSITIVE_INFINITY),
        retries: 0,
      });
      expect(result.metrics?.rowsPerSecond).toBeGreaterThan(0);

      const output = await requiredSegment(store, result.outputSegmentId);
      expect(output).toMatchObject({ level: 1, rowCount: selectedRowCount, logicalOrder: 0 });
      anchorSegmentId = output.id;
      expect((await allVisibleSegments(database, tableName)).map((segment) => segment.id)).toEqual([
        anchorSegmentId,
        ...originalSegmentIds.slice((jobIndex + 1) * 2),
      ]);
      expect(await database.readTable(tableName)).toEqual(expectedRows);
      for (const snapshot of snapshots) {
        expect(await database.readTable(tableName, snapshot.version)).toEqual(snapshot.rows);
      }
    }
    expect(await allVisibleSegments(database, tableName)).toHaveLength(1);
    store.close();
  });

  it(`${implementation.name} converges bounded base and mutation prefixes without changing row identities`, async () => {
    const store = await implementation.create();
    const tableName = `bounded_mutation_${implementation.name}`;
    const fixture = await createMutationCompactionFixture(store, tableName);
    const originalSegmentIds = fixture.sourceSegmentIds;
    const initialRowIds = expandSegmentRowIds(
      await requiredSegment(
        store,
        requiredItem(originalSegmentIds, 0, "initial bounded mutation segment"),
      ),
    );
    const upsertRowIds = expandSegmentRowIds(
      await requiredSegment(
        store,
        requiredItem(originalSegmentIds, 1, "bounded mutation upsert segment"),
      ),
    );
    const expectedOutputRowIds = [
      [
        requiredItem(initialRowIds, 0, "initial A row ID"),
        requiredItem(initialRowIds, 1, "initial B row ID"),
        requiredItem(initialRowIds, 2, "initial C row ID"),
        requiredItem(upsertRowIds, 1, "new D row ID"),
      ],
      [
        requiredItem(initialRowIds, 1, "updated B row ID"),
        requiredItem(initialRowIds, 2, "retained C row ID"),
        requiredItem(upsertRowIds, 1, "updated D row ID"),
      ],
      fixture.expectedRowIds,
    ];

    let anchorSegmentId: string | null = null;
    for (let jobIndex = 0; jobIndex < 3; jobIndex += 1) {
      const promotedIds = originalSegmentIds.slice(jobIndex * 2, jobIndex * 2 + 2);
      const expectedSourceIds =
        anchorSegmentId === null ? promotedIds : [anchorSegmentId, ...promotedIds];
      const result = await fixture.database.compactTable(tableName, {
        minimumLevel0Segments: 2,
        maxLevel0Segments: 2,
        maxLevel0StoredBytes: 1024 * 1024,
        targetBlockBytes: 64,
        outputCompression: "raw",
        maxBlocksPerStep: 1,
      });
      if (result.jobId === undefined || result.outputSegmentId === null) {
        throw new Error("Expected a bounded mutation base");
      }
      const sourceStats = await assertPersistedCompactionSelection(
        store,
        result.jobId,
        expectedSourceIds,
        anchorSegmentId,
      );
      const rowIds = requiredItem(expectedOutputRowIds, jobIndex, "bounded output row IDs");
      expect(result).toMatchObject({
        compacted: true,
        sourceSegmentCount: expectedSourceIds.length,
        sourceBlockCount: sourceStats.blockIds.length,
        sourceStoredBytes: sourceStats.storedBytes,
        rowCount: rowIds.length,
        supersededBlockCount: sourceStats.blockIds.length,
      });
      const job = await store.getCompactionJob(result.jobId);
      expect(job?.rewritePlan).toMatchObject({
        kind: "merge-v1",
        totalRows: rowIds.length,
        rowIdSpans: canonicalRowIdSpans(rowIds),
      });
      const output = await requiredSegment(store, result.outputSegmentId);
      expect(output).toMatchObject({
        kind: "base",
        level: 1,
        rowCount: rowIds.length,
        rowIdSpans: canonicalRowIdSpans(rowIds),
      });
      expect(expandSegmentRowIds(output)).toEqual(rowIds);
      anchorSegmentId = output.id;
      expect(
        (await allVisibleSegments(fixture.database, tableName)).map((segment) => segment.id),
      ).toEqual([anchorSegmentId, ...originalSegmentIds.slice((jobIndex + 1) * 2)]);
      expect(await fixture.database.readTable(tableName)).toEqual(fixture.expectedRows);
      for (const snapshot of fixture.snapshots) {
        expect(await fixture.database.readTable(tableName, snapshot.version)).toEqual(
          snapshot.rows,
        );
      }
    }
    expect(await allVisibleSegments(fixture.database, tableName)).toHaveLength(1);
    store.close();
  });

  it(`${implementation.name} preserves a later upsert when a bounded prefix deletes every base row`, async () => {
    const store = await implementation.create();
    const tableName = `bounded_empty_${implementation.name}`;
    const database = new MinnowDatabase(store, { compression: "raw" });
    await database.createTable({
      name: tableName,
      uniqueKey: "email",
      columns: [
        { name: "email", type: "string" },
        { name: "score", type: "number" },
      ],
    });
    const inserted = await database.insert(tableName, { email: "a@example.com", score: 1 });
    const insertedRowId = requiredItem(
      expandSegmentRowIds(await requiredSegment(store, inserted.segmentId)),
      0,
      "bounded deleted row ID",
    );
    const deleted = await database.deleteBatch(tableName, { keys: ["a@example.com"] });
    if (deleted.segmentId === null) throw new Error("Expected a bounded delete segment");
    const later = await database.upsert(tableName, { email: "a@example.com", score: 2 });
    const laterRowId = requiredItem(
      expandSegmentRowIds(await requiredSegment(store, later.segmentId ?? "")),
      0,
      "bounded later upsert row ID",
    );
    expect(laterRowId).toBeGreaterThan(insertedRowId);

    const result = await database.compactTable(tableName, {
      minimumLevel0Segments: 2,
      maxLevel0Segments: 2,
      outputCompression: "raw",
    });
    if (result.jobId === undefined) throw new Error("Expected a bounded empty-prefix job");
    await assertPersistedCompactionSelection(
      store,
      result.jobId,
      [inserted.segmentId, deleted.segmentId],
      null,
    );
    expect(result).toMatchObject({
      compacted: true,
      sourceSegmentCount: 2,
      outputSegmentId: null,
      outputBlockCount: 0,
      rowCount: 0,
    });
    expect((await allVisibleSegments(database, tableName)).map((segment) => segment.id)).toEqual([
      later.segmentId,
    ]);
    expect(await database.readTable(tableName)).toEqual([{ email: "a@example.com", score: 2 }]);
    expect(await database.readTable(tableName, inserted.version)).toEqual([
      { email: "a@example.com", score: 1 },
    ]);
    store.close();
  });
}

for (const implementation of recoveryImplementations()) {
  it(`${implementation.name} reopens a checkpointed bounded job with its exact persisted prefix`, async () => {
    const harness = await implementation.create();
    let store = harness.store;
    const tableName = `bounded_reopen_${implementation.name.replaceAll(" ", "_")}`;
    const database = new MinnowDatabase(store, { compression: "raw" });
    await database.createTable({
      name: tableName,
      columns: [{ name: "value", type: "number" }],
    });
    const inserts = [];
    for (let value = 1; value <= 4; value += 1) {
      inserts.push(await database.insert(tableName, { value }));
    }
    const sourceSegmentIds = inserts.slice(0, 2).map((insert) => insert.segmentId);
    let progress = await database.compactTableStep(tableName, {
      minimumLevel0Segments: 2,
      maxLevel0Segments: 2,
      maxLevel0StoredBytes: 1024 * 1024,
      maxBlocks: 1,
      targetBlockBytes: 9,
      outputCompression: "raw",
    });
    if (progress.jobId === null) throw new Error("Expected a checkpointed bounded job");
    const jobId = progress.jobId;
    expect(progress).toMatchObject({
      state: "running",
      sourceSegmentCount: 2,
      outputBlockCount: 1,
      result: null,
    });
    await assertPersistedCompactionSelection(store, jobId, sourceSegmentIds, null);

    store = await harness.reopen();
    await assertPersistedCompactionSelection(store, jobId, sourceSegmentIds, null);
    const reopened = new MinnowDatabase(store, { compression: "gzip", rowsPerBlock: 2048 });
    while (progress.result === null) {
      progress = await reopened.resumeCompactionJob(jobId, { maxBlocks: 1 });
    }
    if (progress.result.outputSegmentId === null) {
      throw new Error("Expected a reopened bounded output");
    }
    expect(progress.result).toMatchObject({
      compacted: true,
      sourceSegmentCount: 2,
      rowCount: 2,
      outputCompression: "raw",
    });
    expect((await allVisibleSegments(reopened, tableName)).map((segment) => segment.id)).toEqual([
      progress.result.outputSegmentId,
      ...inserts.slice(2).map((insert) => insert.segmentId),
    ]);
    expect(await reopened.readTable(tableName)).toEqual(
      inserts.map((_insert, index) => ({ value: index + 1 })),
    );
    expect(
      await reopened.readTable(tableName, requiredItem(inserts, 3, "last insert").version),
    ).toEqual(inserts.map((_insert, index) => ({ value: index + 1 })));
    store.close();
  });
}

it("rebases a bounded IndexedDB mutation prefix without absorbing its existing or concurrent tail", async () => {
  const indexedDB = new IDBFactory();
  const name = crypto.randomUUID();
  const compactorStore = await IndexedDbBlockStore.open({ name, indexedDB });
  const writerStore = await IndexedDbBlockStore.open({ name, indexedDB });
  const tableName = "bounded_rebase_accounts";
  const compactor = new MinnowDatabase(compactorStore, { compression: "raw" });
  const writer = new MinnowDatabase(writerStore, { compression: "raw" });
  await compactor.createTable({
    name: tableName,
    uniqueKey: "email",
    columns: [
      { name: "email", type: "string" },
      { name: "score", type: "number" },
    ],
  });
  const inserted = await compactor.insertBatch(tableName, {
    columns: { email: ["a@example.com", "b@example.com"], score: [1, 2] },
  });
  const initialRowIds = expandSegmentRowIds(
    await requiredSegment(compactorStore, inserted.segmentId),
  );
  const upserted = await compactor.upsertBatch(tableName, {
    columns: { email: ["b@example.com", "c@example.com"], score: [20, 3] },
  });
  const upsertRowIds = expandSegmentRowIds(
    await requiredSegment(compactorStore, upserted.segmentId ?? ""),
  );
  const tailUpdate = await compactor.update(tableName, "a@example.com", { score: 10 });
  const sourceVersion = tailUpdate.version;

  let progress = await compactor.compactTableStep(tableName, {
    minimumLevel0Segments: 2,
    maxLevel0Segments: 2,
    maxBlocks: 1,
    targetBlockBytes: 64,
    outputCompression: "raw",
  });
  if (progress.jobId === null) throw new Error("Expected a bounded rebase job");
  const jobId = progress.jobId;
  await assertPersistedCompactionSelection(
    compactorStore,
    jobId,
    [inserted.segmentId, upserted.segmentId ?? ""],
    null,
  );
  const concurrentDelete = await writer.deleteBatch(tableName, { keys: ["c@example.com"] });
  if (concurrentDelete.segmentId === null) throw new Error("Expected a concurrent delete tail");

  while (progress.result === null) {
    progress = await compactor.resumeCompactionJob(jobId, { maxBlocks: 1 });
  }
  if (progress.result.outputSegmentId === null) throw new Error("Expected a rebased bounded base");
  const firstOutput = await requiredSegment(compactorStore, progress.result.outputSegmentId);
  expect(firstOutput).toMatchObject({ kind: "base", level: 1 });
  expect(expandSegmentRowIds(firstOutput)).toEqual([
    requiredItem(initialRowIds, 0, "rebased A row ID"),
    requiredItem(initialRowIds, 1, "rebased B row ID"),
    requiredItem(upsertRowIds, 1, "rebased C row ID"),
  ]);
  expect((await allVisibleSegments(compactor, tableName)).map((segment) => segment.id)).toEqual([
    firstOutput.id,
    tailUpdate.segmentId,
    concurrentDelete.segmentId,
  ]);
  expect(await compactor.readTable(tableName)).toEqual([
    { email: "a@example.com", score: 10 },
    { email: "b@example.com", score: 20 },
  ]);
  expect(await compactor.readTable(tableName, sourceVersion)).toEqual([
    { email: "a@example.com", score: 10 },
    { email: "b@example.com", score: 20 },
    { email: "c@example.com", score: 3 },
  ]);

  const converged = await compactor.compactTable(tableName, {
    minimumLevel0Segments: 2,
    maxLevel0Segments: 2,
    outputCompression: "raw",
  });
  if (converged.outputSegmentId === null) throw new Error("Expected a converged bounded base");
  expect((await allVisibleSegments(compactor, tableName)).map((segment) => segment.id)).toEqual([
    converged.outputSegmentId,
  ]);
  expect(
    expandSegmentRowIds(await requiredSegment(compactorStore, converged.outputSegmentId)),
  ).toEqual([
    requiredItem(initialRowIds, 0, "converged A row ID"),
    requiredItem(initialRowIds, 1, "converged B row ID"),
  ]);
  expect(await compactor.readTable(tableName)).toEqual([
    { email: "a@example.com", score: 10 },
    { email: "b@example.com", score: 20 },
  ]);
  compactorStore.close();
  writerStore.close();
});

it("canonicalizes caller-supplied L0 orders before bounded prefix selection", async () => {
  const store = new MemoryBlockStore();
  const database = new MinnowDatabase(store);
  await database.createTable({
    name: "equal_order_prefix",
    columns: [{ name: "value", type: "number" }],
  });
  const table = await store.getTableByName("equal_order_prefix");
  if (table === undefined) throw new Error("Expected an equal-order table");
  const segmentIds = ["equal-group-a", "equal-group-b", "equal-group-c", "later-group"];
  for (const [index, segmentId] of segmentIds.entries()) {
    await commitLowLevelNumberSegment(store, table, {
      segmentId,
      level: 0,
      logicalOrder: index < 3 ? 0 : 1,
      rowId: BigInt(index + 1),
      value: index + 1,
    });
  }

  const result = await database.compactTable("equal_order_prefix", {
    minimumLevel0Segments: 2,
    maxLevel0Segments: 2,
    maxLevel0StoredBytes: 1,
    outputCompression: "raw",
  });
  if (result.jobId === undefined || result.outputSegmentId === null) {
    throw new Error("Expected an oversized equal-order prefix");
  }
  expect(result.sourceSegmentCount).toBe(2);
  await assertPersistedCompactionSelection(store, result.jobId, segmentIds.slice(0, 2), null);
  expect(
    (await allVisibleSegments(database, "equal_order_prefix")).map((segment) => segment.id),
  ).toEqual([result.outputSegmentId, ...segmentIds.slice(2)]);
  expect(await database.readTable("equal_order_prefix")).toEqual([
    { value: 1 },
    { value: 2 },
    { value: 3 },
    { value: 4 },
  ]);
  store.close();
});

it("stops an oldest-prefix selection at its L0 stored-byte cap after the minimum", async () => {
  const store = new MemoryBlockStore();
  const database = new MinnowDatabase(store, { compression: "raw" });
  await database.createTable({
    name: "stored_byte_prefix",
    columns: [{ name: "value", type: "number" }],
  });
  const inserts = [];
  for (let value = 1; value <= 4; value += 1) {
    inserts.push(await database.insert("stored_byte_prefix", { value }));
  }
  const sourceSegmentIds = inserts.slice(0, 2).map((insert) => insert.segmentId);
  const sourceStats = await compactionSourceStats(store, sourceSegmentIds);

  const result = await database.compactTable("stored_byte_prefix", {
    minimumLevel0Segments: 2,
    maxLevel0Segments: 16,
    maxLevel0StoredBytes: sourceStats.storedBytes,
    outputCompression: "raw",
  });
  if (result.jobId === undefined || result.outputSegmentId === null) {
    throw new Error("Expected a stored-byte-bounded prefix");
  }
  await assertPersistedCompactionSelection(store, result.jobId, sourceSegmentIds, null);
  expect(result).toMatchObject({
    compacted: true,
    sourceSegmentCount: 2,
    sourceBlockCount: sourceStats.blockIds.length,
    sourceStoredBytes: sourceStats.storedBytes,
    level0SourceStoredBytes: sourceStats.storedBytes,
  });
  expect(
    (await allVisibleSegments(database, "stored_byte_prefix")).map((segment) => segment.id),
  ).toEqual([result.outputSegmentId, ...inserts.slice(2).map((insert) => insert.segmentId)]);
  expect(await database.readTable("stored_byte_prefix")).toEqual(
    inserts.map((_insert, index) => ({ value: index + 1 })),
  );
  store.close();
});

it("drains an odd append tail only when an anchored job explicitly lowers its minimum", async () => {
  const store = new MemoryBlockStore();
  const database = new MinnowDatabase(store, { compression: "raw" });
  await database.createTable({
    name: "odd_tail_drain",
    columns: [{ name: "value", type: "number" }],
  });
  const inserts = [];
  for (let value = 1; value <= 5; value += 1) {
    inserts.push(await database.insert("odd_tail_drain", { value }));
  }
  const expectedRows = inserts.map((_insert, index) => ({ value: index + 1 }));
  const snapshots = inserts.map((insert, index) => ({
    version: insert.version,
    rows: expectedRows.slice(0, index + 1),
  }));

  const first = await database.compactTable("odd_tail_drain", {
    minimumLevel0Segments: 2,
    maxLevel0Segments: 2,
    outputCompression: "raw",
  });
  if (first.jobId === undefined || first.outputSegmentId === null) {
    throw new Error("Expected the first odd-tail compaction");
  }
  await assertPersistedCompactionSelection(
    store,
    first.jobId,
    inserts.slice(0, 2).map((insert) => insert.segmentId),
    null,
  );

  const second = await database.compactTable("odd_tail_drain", {
    minimumLevel0Segments: 2,
    maxLevel0Segments: 2,
    outputCompression: "raw",
  });
  if (second.jobId === undefined || second.outputSegmentId === null) {
    throw new Error("Expected the second odd-tail compaction");
  }
  await assertPersistedCompactionSelection(
    store,
    second.jobId,
    [first.outputSegmentId, ...inserts.slice(2, 4).map((insert) => insert.segmentId)],
    first.outputSegmentId,
  );
  expect(
    (await allVisibleSegments(database, "odd_tail_drain")).map((segment) => segment.id),
  ).toEqual([second.outputSegmentId, requiredItem(inserts, 4, "odd L0 tail").segmentId]);

  const skipped = await database.compactTable("odd_tail_drain");
  expect(skipped).toMatchObject({
    compacted: false,
    skipReason: "below-segment-threshold",
    sourceSegmentCount: 2,
  });
  expect(await store.listCompactionJobs()).toHaveLength(2);

  const drained = await database.compactTable("odd_tail_drain", {
    minimumLevel0Segments: 1,
    maxLevel0Segments: 1,
    outputCompression: "raw",
  });
  if (drained.jobId === undefined || drained.outputSegmentId === null) {
    throw new Error("Expected the explicit odd-tail drain");
  }
  await assertPersistedCompactionSelection(
    store,
    drained.jobId,
    [second.outputSegmentId, requiredItem(inserts, 4, "drained L0 tail").segmentId],
    second.outputSegmentId,
  );
  expect(await allVisibleSegments(database, "odd_tail_drain")).toEqual([
    expect.objectContaining({ id: drained.outputSegmentId, rowCount: 5 }),
  ]);
  expect(await database.readTable("odd_tail_drain")).toEqual(expectedRows);
  for (const snapshot of snapshots) {
    expect(await database.readTable("odd_tail_drain", snapshot.version)).toEqual(snapshot.rows);
  }
  store.close();
});

it("validates bounded compaction options", async () => {
  const store = new MemoryBlockStore();
  const database = new MinnowDatabase(store);
  await database.createTable({
    name: "bounded_options",
    columns: [{ name: "value", type: "number" }],
  });
  for (const options of [
    { minimumLevel0Segments: 0 },
    { minimumLevel0Segments: 2.5 },
    { minimumLevel0Segments: 3, maxLevel0Segments: 2 },
    { maxLevel0Segments: 0 },
    { maxLevel0Segments: 2.5 },
    { maxLevel0StoredBytes: 0 },
    { maxLevel0StoredBytes: 2.5 },
    { targetLevel: 3 },
  ]) {
    await expect(database.compactTable("bounded_options", options)).rejects.toBeInstanceOf(
      RangeError,
    );
  }
  expect(await store.listCompactionJobs()).toEqual([]);

  await database.insert("bounded_options", { value: 1 });
  expect(
    await database.compactTable("bounded_options", {
      minimumLevel0Segments: 1,
      maxLevel0Segments: 1,
    }),
  ).toMatchObject({
    compacted: false,
    skipReason: "below-segment-threshold",
    sourceSegmentCount: 1,
  });
  await database.insert("bounded_options", { value: 2 });
  const compacted = await database.compactTable("bounded_options", {
    minimumLevel0Segments: 2,
    outputCompression: "raw",
  });
  expect(compacted).toMatchObject({ compacted: true, sourceSegmentCount: 2 });
  store.close();
});

it("refuses high-level segments that no compaction job authorized", async () => {
  const store = new MemoryBlockStore();
  const database = new MinnowDatabase(store);
  await database.createTable({
    name: "unauthorized_layout",
    columns: [{ name: "value", type: "number" }],
  });
  const table = await store.getTableByName("unauthorized_layout");
  if (table === undefined) throw new Error("Expected the authorization test table");
  for (const level of [1, 2]) {
    await expect(
      commitLowLevelNumberSegment(store, table, {
        segmentId: `unauthorized-level-${String(level)}`,
        level,
        ...(level === 2 ? { partitionOrdinal: 0 } : {}),
        logicalOrder: level,
        rowId: BigInt(level),
        value: level,
      }),
    ).rejects.toThrow(/compaction job authorize(s)? output segment/iu);
  }
  expect(await store.listCompactionJobs()).toEqual([]);
  store.close();
});

for (const implementation of implementations()) {
  it(`${implementation.name} repeatedly promotes L0 prefixes into immutable ordered L2 partitions`, async () => {
    const store = await implementation.create();
    const tableName = `l2_prefix_${implementation.name}`;
    const database = new MinnowDatabase(store, { compression: "raw" });
    await database.createTable({
      name: tableName,
      columns: [{ name: "value", type: "number" }],
    });
    const inserts = [];
    for (let value = 1; value <= 6; value += 1) {
      inserts.push(await database.insert(tableName, { value }));
    }
    const originalSegmentIds = inserts.map((insert) => insert.segmentId);
    const expectedRows = inserts.map((_insert, index) => ({ value: index + 1 }));
    const snapshots = inserts.map((insert, index) => ({
      version: insert.version,
      rows: expectedRows.slice(0, index + 1),
    }));
    const partitions: Array<{
      segment: SegmentRecord;
      blocks: Array<{ id: string; bytes: Uint8Array }>;
    }> = [];

    for (let partitionOrdinal = 0; partitionOrdinal < 3; partitionOrdinal += 1) {
      const promotedIds = originalSegmentIds.slice(partitionOrdinal * 2, partitionOrdinal * 2 + 2);
      const sourceStats = await compactionSourceStats(store, promotedIds);
      const result = await database.compactTable(tableName, {
        ...(partitionOrdinal === 0 ? { targetLevel: 2 } : {}),
        minimumLevel0Segments: 2,
        maxLevel0Segments: 2,
        maxLevel0StoredBytes: 1024 * 1024,
        targetBlockBytes: 1024,
        outputCompression: "raw",
        maxBlocksPerStep: 1,
      });
      if (result.jobId === undefined || result.outputSegmentId === null) {
        throw new Error("Expected an L2 output partition");
      }
      expect(result).toMatchObject({
        compacted: true,
        sourceSegmentCount: 2,
        sourceBlockCount: sourceStats.blockIds.length,
        rowCount: 2,
        sourceStoredBytes: sourceStats.storedBytes,
        level0SourceStoredBytes: sourceStats.storedBytes,
        anchorSourceStoredBytes: 0,
        outputPartitionOrdinal: partitionOrdinal,
        maxWriteAmplification: 16,
      });
      expect(result.maximumOutputStoredBytes).toBe(Math.floor(sourceStats.storedBytes * 16));
      expect(result.outputStoredBytes).toBeLessThanOrEqual(
        result.plannedOutputStoredBytesUpperBound ?? -1,
      );
      expect(result.plannedOutputStoredBytesUpperBound).toBeLessThanOrEqual(
        result.maximumOutputStoredBytes ?? -1,
      );

      const job = await store.getCompactionJob(result.jobId);
      expect(job).toMatchObject({
        targetLevel: 2,
        sourceSegmentIds: promotedIds,
        sourceStoredBytes: sourceStats.storedBytes,
        level0SourceStoredBytes: sourceStats.storedBytes,
        anchorSourceStoredBytes: 0,
        outputPartitionOrdinal: partitionOrdinal,
        maxWriteAmplification: 16,
        maximumOutputStoredBytes: result.maximumOutputStoredBytes,
        plannedOutputStoredBytesUpperBound: result.plannedOutputStoredBytesUpperBound,
      });
      expect([...(job?.sourceBlockIds ?? [])].sort()).toEqual(sourceStats.blockIds);

      const sourceSegments = await Promise.all(
        promotedIds.map((segmentId) => requiredSegment(store, segmentId)),
      );
      const firstSource = requiredItem(sourceSegments, 0, "first L2 source");
      const lastSource = requiredItem(sourceSegments, 1, "last L2 source");
      const output = await requiredSegment(store, result.outputSegmentId);
      expect(output).toMatchObject({
        kind: "insert",
        level: 2,
        partitionOrdinal,
        rowCount: 2,
        rowIdStart: firstSource.rowIdStart,
        rowIdEndExclusive: lastSource.rowIdEndExclusive,
      });

      for (const preserved of partitions) {
        expect(await requiredSegment(store, preserved.segment.id)).toEqual(preserved.segment);
        for (const block of preserved.blocks) {
          expect(await store.getBlock(block.id)).toEqual(block.bytes);
        }
      }
      const outputBlockIds = Object.values(output.columnBlockIds).flat();
      const outputBlocks = await Promise.all(
        outputBlockIds.map(async (id) => {
          const bytes = await store.getBlock(id);
          if (bytes === undefined) throw new Error(`Expected L2 output block ${id}`);
          return { id, bytes: new Uint8Array(bytes) };
        }),
      );
      partitions.push({ segment: output, blocks: outputBlocks });

      expect((await allVisibleSegments(database, tableName)).map((segment) => segment.id)).toEqual([
        ...partitions.map((partition) => partition.segment.id),
        ...originalSegmentIds.slice((partitionOrdinal + 1) * 2),
      ]);
      expect(await database.readTable(tableName)).toEqual(expectedRows);
      for (const snapshot of snapshots) {
        expect(await database.readTable(tableName, snapshot.version)).toEqual(snapshot.rows);
      }
    }

    for (let index = 1; index < partitions.length; index += 1) {
      const previous = requiredItem(partitions, index - 1, "previous L2 partition").segment;
      const current = requiredItem(partitions, index, "current L2 partition").segment;
      expect(
        previous.rowIdEndExclusive <= current.rowIdStart ||
          current.rowIdEndExclusive <= previous.rowIdStart,
      ).toBe(true);
      expect(previous.partitionOrdinal).toBe(index - 1);
      expect(current.partitionOrdinal).toBe(index);
    }
    store.close();
  });
}

it("retains an optional L1 prefix while omitted targets continue established L2 partitioning", async () => {
  const store = new MemoryBlockStore();
  const database = new MinnowDatabase(store, { compression: "raw" });
  await database.createTable({
    name: "l1_l2_transition",
    columns: [{ name: "value", type: "number" }],
  });
  const inserts = [];
  for (let value = 1; value <= 2; value += 1) {
    inserts.push(await database.insert("l1_l2_transition", { value }));
  }
  const levelOne = await database.compactTable("l1_l2_transition", {
    outputCompression: "raw",
  });
  if (levelOne.outputSegmentId === null) throw new Error("Expected a retained L1 prefix");
  const retainedSegment = await requiredSegment(store, levelOne.outputSegmentId);
  const retainedBlockIds = Object.values(retainedSegment.columnBlockIds).flat();
  const retainedBlocks = await store.getBlocks(retainedBlockIds);

  for (let value = 3; value <= 6; value += 1) {
    inserts.push(await database.insert("l1_l2_transition", { value }));
  }
  const firstL2 = await database.compactTable("l1_l2_transition", {
    targetLevel: 2,
    minimumLevel0Segments: 2,
    maxLevel0Segments: 2,
    outputCompression: "raw",
  });
  const secondL2 = await database.compactTable("l1_l2_transition", {
    minimumLevel0Segments: 2,
    maxLevel0Segments: 2,
    outputCompression: "raw",
  });
  if (firstL2.outputSegmentId === null || secondL2.outputSegmentId === null) {
    throw new Error("Expected two L2 transition partitions");
  }
  expect(firstL2.outputPartitionOrdinal).toBe(0);
  expect(secondL2.outputPartitionOrdinal).toBe(1);
  expect(await requiredSegment(store, retainedSegment.id)).toEqual(retainedSegment);
  for (const [index, blockId] of retainedBlockIds.entries()) {
    expect(await store.getBlock(blockId)).toEqual(retainedBlocks[index]);
  }
  expect(
    (await allVisibleSegments(database, "l1_l2_transition")).map((segment) => segment.id),
  ).toEqual([retainedSegment.id, firstL2.outputSegmentId, secondL2.outputSegmentId]);
  expect(await database.readTable("l1_l2_transition")).toEqual(
    inserts.map((_insert, index) => ({ value: index + 1 })),
  );
  expect(
    await database.readTable("l1_l2_transition", requiredItem(inserts, 1, "L1 snapshot").version),
  ).toEqual([{ value: 1 }, { value: 2 }]);
  store.close();
});

it("promotes keyed mutation prefixes into immutable multi-range L2 partitions", async () => {
  const store = new MemoryBlockStore();
  const database = new MinnowDatabase(store, { compression: "raw" });
  await database.createTable({
    name: "keyed_l2",
    uniqueKey: "id",
    columns: [
      { name: "id", type: "number" },
      { name: "label", type: "string" },
    ],
  });
  const query = "SELECT id, label FROM keyed_l2 ORDER BY id";
  const partitionSpans: Array<Array<{ rowIdStart: bigint; rowCount: number }>> = [];
  for (let wave = 0; wave < 3; wave += 1) {
    // Each wave inserts twelve keys, updates four of them, and deletes two — a self-contained
    // keyed prefix whose merge folds every mutation it references.
    const base = wave * 100;
    await database.insertBatch("keyed_l2", {
      columns: {
        id: Array.from({ length: 12 }, (_, index) => base + index),
        label: Array.from({ length: 12 }, (_, index) => `w${String(wave)}-${String(index)}`),
      },
    });
    await database.updateBatch("keyed_l2", {
      keys: [base, base + 1, base + 2, base + 3],
      changes: {
        label: [
          `u${String(wave)}-0`,
          `u${String(wave)}-1`,
          `u${String(wave)}-2`,
          `u${String(wave)}-3`,
        ],
      },
    });
    await database.deleteBatch("keyed_l2", { keys: [base + 10, base + 11] });

    const before = await database.query(query);
    const result = await database.compactTable("keyed_l2", {
      targetLevel: 2,
      minimumLevel0Segments: 1,
      outputCompression: "raw",
    });
    expect(result).toMatchObject({
      compacted: true,
      outputPartitionOrdinal: wave,
      priorAttemptOutputStoredBytes: 0,
    });
    expect(result.outputStoredBytes).toBeLessThanOrEqual(
      result.plannedOutputStoredBytesUpperBound ?? 0,
    );
    expect(result.plannedOutputStoredBytesUpperBound ?? 0).toBeLessThanOrEqual(
      result.maximumOutputStoredBytes ?? 0,
    );
    // The published partition is a merged full-row base carrying disjoint row-ID spans.
    const table = await store.getTableByName("keyed_l2");
    if (table === undefined) throw new Error("Expected the keyed table record");
    const partitions = (await tableSegmentRecords(store, table.id))
      .filter((segment) => segment.partitionOrdinal !== undefined)
      .sort((left, right) => (left.partitionOrdinal ?? 0) - (right.partitionOrdinal ?? 0));
    expect(partitions.map((segment) => segment.partitionOrdinal)).toEqual(
      Array.from({ length: wave + 1 }, (_, index) => index),
    );
    const record = partitions[wave];
    if (record === undefined) throw new Error("Expected a published keyed partition");
    expect(record).toMatchObject({ kind: "base", level: 2, partitionOrdinal: wave });
    expect(record.rowIdSpans.length).toBeGreaterThan(0);
    partitionSpans.push(
      record.rowIdSpans.map((span) => ({
        rowIdStart: span.rowIdStart,
        rowCount: span.rowCount,
      })),
    );
    // Rows and order are untouched by the promotion.
    expect(await database.query(query)).toEqual(before);
  }
  // Spans are pairwise disjoint across all published partitions.
  const intervals = partitionSpans
    .flat()
    .map((span) => ({ start: span.rowIdStart, end: span.rowIdStart + BigInt(span.rowCount) }))
    .sort((left, right) => (left.start < right.start ? -1 : 1));
  for (let index = 1; index < intervals.length; index += 1) {
    const interval = intervals[index];
    const previous = intervals[index - 1];
    if (interval === undefined || previous === undefined) throw new Error("Missing interval");
    expect(interval.start >= previous.end).toBe(true);
  }

  // A delta referencing a key frozen into a published partition cannot fold without rewriting
  // that partition; the planner skips explicitly and the data stays correct via replay.
  await database.updateBatch("keyed_l2", { keys: [5], changes: { label: ["rewritten"] } });
  const beforeSkip = await database.query(query);
  expect(
    await database.compactTable("keyed_l2", {
      targetLevel: 2,
      minimumLevel0Segments: 1,
      outputCompression: "raw",
    }),
  ).toMatchObject({ compacted: false, skipReason: "keys-outside-selected-sources" });
  expect(await store.listCompactionJobs()).toEqual(
    (await store.listCompactionJobs()).filter((job) => job.state === "published"),
  );
  expect(await database.query(query)).toEqual(beforeSkip);
  expect(beforeSkip.rows.find((row) => row.id === 5)?.label).toBe("rewritten");
  store.close();
});

it("retries a cancelled keyed L2 promotion under the shared lifetime ceiling", async () => {
  const store = new MemoryBlockStore();
  const database = new MinnowDatabase(store, { compression: "raw" });
  await database.createTable({
    name: "keyed_l2_retry",
    uniqueKey: "id",
    columns: [
      { name: "id", type: "number" },
      { name: "label", type: "string" },
    ],
  });
  await database.insertBatch("keyed_l2_retry", {
    columns: {
      id: Array.from({ length: 16 }, (_, index) => index),
      label: Array.from({ length: 16 }, (_, index) => `row-${String(index)}`),
    },
  });
  await database.updateBatch("keyed_l2_retry", {
    keys: [1, 3],
    changes: { label: ["updated-1", "updated-3"] },
  });
  const query = "SELECT id, label FROM keyed_l2_retry ORDER BY id";
  const before = await database.query(query);
  const options = {
    targetLevel: 2 as const,
    minimumLevel0Segments: 1,
    targetBlockBytes: 64,
    outputCompression: "raw" as const,
  };
  const first = await database.compactTableStep("keyed_l2_retry", { ...options, maxBlocks: 1 });
  expect(first.result).toBeNull();
  if (first.jobId === null) throw new Error("Expected a keyed promotion job");
  const interrupted = await store.getCompactionJob(first.jobId);
  const attemptBytes = interrupted?.outputStoredBytes ?? 0;
  expect(interrupted?.rewritePlan.kind).toBe("merge-v1");
  expect(interrupted?.outputPartitionOrdinal).toBe(0);
  expect(attemptBytes).toBeGreaterThan(0);
  await database.cancelCompactionJob(first.jobId);

  const published = await database.compactTable("keyed_l2_retry", options);
  expect(published).toMatchObject({
    compacted: true,
    outputPartitionOrdinal: 0,
    priorAttemptOutputStoredBytes: attemptBytes,
    lifetimeOutputStoredBytes: attemptBytes + published.outputStoredBytes,
  });
  expect(await database.query(query)).toEqual(before);
  store.close();
});

it("shares one lifetime write-amplification budget across failed L2 attempts", async () => {
  const store = new MemoryBlockStore();
  const database = new MinnowDatabase(store, { compression: "raw" });
  await database.createTable({ name: "events", columns: [{ name: "value", type: "number" }] });
  for (let value = 1; value <= 8; value += 1) await database.insert("events", { value });
  const options = {
    targetLevel: 2 as const,
    minimumLevel0Segments: 8,
    maxLevel0Segments: 8,
    targetBlockBytes: 64,
    outputCompression: "raw" as const,
  };

  // First attempt: one checkpointed output block, then cancellation.
  const first = await database.compactTableStep("events", { ...options, maxBlocks: 1 });
  expect(first.result).toBeNull();
  if (first.jobId === null) throw new Error("Expected a persisted compaction job");
  const interrupted = await store.getCompactionJob(first.jobId);
  if (interrupted === undefined) throw new Error("Expected the interrupted job record");
  const attemptBytes = interrupted.outputStoredBytes;
  const fullCeiling = interrupted.maximumOutputStoredBytes ?? 0;
  const plannedUpperBound = interrupted.plannedOutputStoredBytesUpperBound ?? 0;
  const level0Bytes = interrupted.level0SourceStoredBytes;
  expect(attemptBytes).toBeGreaterThan(0);
  expect(interrupted.priorAttemptOutputStoredBytes).toBe(0);
  await database.cancelCompactionJob(first.jobId);

  // The retry shares the lifetime ceiling: its budget shrinks by the cancelled attempt's bytes.
  const second = await database.compactTableStep("events", { ...options, maxBlocks: 1 });
  if (second.jobId === null) throw new Error("Expected a retry compaction job");
  expect(second.jobId).not.toBe(first.jobId);
  const retry = await store.getCompactionJob(second.jobId);
  expect(retry).toMatchObject({
    priorAttemptOutputStoredBytes: attemptBytes,
    maximumOutputStoredBytes: fullCeiling - attemptBytes,
  });
  const secondBytes = retry?.outputStoredBytes ?? 0;
  await database.cancelCompactionJob(second.jobId);

  // With a cap sized exactly to the planned output, the bytes failed attempts already wrote
  // starve the remaining budget and planning skips before writing anything.
  const starvedAmplification = plannedUpperBound / level0Bytes;
  const starved = await database.compactTableStep("events", {
    ...options,
    maxBlocks: 1,
    maxWriteAmplification: starvedAmplification,
  });
  expect(starved.result).toMatchObject({
    compacted: false,
    skipReason: "write-amplification-budget",
    priorAttemptOutputStoredBytes: attemptBytes + secondBytes,
    lifetimeOutputStoredBytes: attemptBytes + secondBytes,
    maximumOutputStoredBytes: Math.max(0, plannedUpperBound - attemptBytes - secondBytes),
  });

  // A published attempt reports the shared lifetime spend alongside its own output.
  const published = await database.compactTable("events", options);
  expect(published).toMatchObject({
    compacted: true,
    priorAttemptOutputStoredBytes: attemptBytes + secondBytes,
    lifetimeOutputStoredBytes: attemptBytes + secondBytes + published.outputStoredBytes,
  });
  expect(published.outputStoredBytes).toBeLessThanOrEqual(
    (published.maximumOutputStoredBytes ?? 0) + 0,
  );
  expect((published.maximumOutputStoredBytes ?? 0) + attemptBytes + secondBytes).toBe(
    Math.floor(level0Bytes * 16),
  );
  store.close();
});

it("keeps the implicit L1 default but permits an explicit single-segment L2 promotion", async () => {
  const store = new MemoryBlockStore();
  const database = new MinnowDatabase(store, { compression: "raw" });
  await database.createTable({
    name: "single_l2_partition",
    columns: [{ name: "value", type: "number" }],
  });
  const inserted = await database.insert("single_l2_partition", { value: 1 });

  expect(
    await database.compactTable("single_l2_partition", {
      minimumLevel0Segments: 1,
      maxLevel0Segments: 1,
    }),
  ).toMatchObject({
    compacted: false,
    skipReason: "below-segment-threshold",
  });
  expect(await store.listCompactionJobs()).toEqual([]);

  const promoted = await database.compactTable("single_l2_partition", {
    targetLevel: 2,
    minimumLevel0Segments: 1,
    maxLevel0Segments: 1,
    maxWriteAmplification: 64,
    outputCompression: "raw",
  });
  if (promoted.outputSegmentId === null) throw new Error("Expected one promoted L2 segment");
  expect(promoted).toMatchObject({
    compacted: true,
    sourceSegmentCount: 1,
    outputPartitionOrdinal: 0,
  });
  expect(await requiredSegment(store, promoted.outputSegmentId)).toMatchObject({
    level: 2,
    partitionOrdinal: 0,
    rowCount: 1,
  });
  expect(await database.readTable("single_l2_partition")).toEqual([{ value: 1 }]);
  expect(await database.readTable("single_l2_partition", inserted.version)).toEqual([{ value: 1 }]);
  store.close();
});

it("enforces the conservative L2 write-amplification ceiling at an exact byte boundary", async () => {
  const store = new MemoryBlockStore();
  const database = new MinnowDatabase(store, { compression: "raw" });
  const seed = async (tableName: string) => {
    await database.createTable({
      name: tableName,
      columns: [{ name: "value", type: "number" }],
    });
    const first = await database.insert(tableName, { value: 1 });
    const second = await database.insert(tableName, { value: 2 });
    return [first, second] as const;
  };
  const options = {
    targetLevel: 2 as const,
    minimumLevel0Segments: 2,
    maxLevel0Segments: 2,
    targetBlockBytes: 9,
    outputCompression: "raw" as const,
  };

  const calibrationSources = await seed("l2_budget_calibration");
  const calibration = await database.compactTable("l2_budget_calibration", {
    ...options,
    maxWriteAmplification: 1024,
  });
  if (!calibration.compacted || calibration.plannedOutputStoredBytesUpperBound === undefined) {
    throw new Error("Expected a calibrated L2 bound");
  }
  const calibratedSourceStats = await compactionSourceStats(
    store,
    calibrationSources.map((source) => source.segmentId),
  );
  const plannedUpperBound = calibration.plannedOutputStoredBytesUpperBound;
  const exactLimit = (plannedUpperBound + 0.25) / calibratedSourceStats.storedBytes;
  const belowLimit = (plannedUpperBound - 0.25) / calibratedSourceStats.storedBytes;
  expect(Math.floor(calibratedSourceStats.storedBytes * exactLimit)).toBe(plannedUpperBound);
  expect(Math.floor(calibratedSourceStats.storedBytes * belowLimit)).toBe(plannedUpperBound - 1);

  const exactSources = await seed("l2_budget_exact");
  const exactStats = await compactionSourceStats(
    store,
    exactSources.map((source) => source.segmentId),
  );
  expect(exactStats.storedBytes).toBe(calibratedSourceStats.storedBytes);
  const exact = await database.compactTable("l2_budget_exact", {
    ...options,
    maxWriteAmplification: exactLimit,
  });
  expect(exact).toMatchObject({
    compacted: true,
    maxWriteAmplification: exactLimit,
    maximumOutputStoredBytes: plannedUpperBound,
    plannedOutputStoredBytesUpperBound: plannedUpperBound,
  });
  expect(exact.outputStoredBytes).toBeLessThanOrEqual(plannedUpperBound);

  const belowSources = await seed("l2_budget_below");
  const belowStats = await compactionSourceStats(
    store,
    belowSources.map((source) => source.segmentId),
  );
  expect(belowStats.storedBytes).toBe(calibratedSourceStats.storedBytes);
  const jobsBefore = await store.listCompactionJobs();
  const blocksBefore = await currentManifestBlockIds(store);
  const segmentsBefore = await segmentRecords(store);
  const manifestBefore = await store.getCurrentManifest();
  const below = await database.compactTable("l2_budget_below", {
    ...options,
    maxWriteAmplification: belowLimit,
  });
  expect(below).toMatchObject({
    compacted: false,
    skipReason: "write-amplification-budget",
    maxWriteAmplification: belowLimit,
    maximumOutputStoredBytes: plannedUpperBound - 1,
    plannedOutputStoredBytesUpperBound: plannedUpperBound,
  });
  expect(await store.listCompactionJobs()).toEqual(jobsBefore);
  expect(await currentManifestBlockIds(store)).toEqual(blocksBefore);
  expect(await segmentRecords(store)).toEqual(segmentsBefore);
  expect(await store.getCurrentManifest()).toEqual(manifestBefore);
  expect(await database.readTable("l2_budget_below")).toEqual([{ value: 1 }, { value: 2 }]);

  expect(
    await database.compactTable("l2_budget_below", {
      ...options,
      maxWriteAmplification: Number.MIN_VALUE,
    }),
  ).toMatchObject({
    compacted: false,
    skipReason: "write-amplification-budget",
    maximumOutputStoredBytes: 0,
  });

  for (const maxWriteAmplification of [0, -1, Number.NaN, Number.POSITIVE_INFINITY]) {
    await expect(
      database.compactTable("l2_budget_below", {
        ...options,
        maxWriteAmplification,
      }),
    ).rejects.toBeInstanceOf(RangeError);
  }
  store.close();
});

it("does not round a binary write-amplification ratio up by one output byte", async () => {
  const store = new MemoryBlockStore();
  const database = new MinnowDatabase(store, { compression: "raw" });
  const seed = async (tableName: string) => {
    await database.createTable({
      name: tableName,
      columns: [{ name: "value", type: "boolean" }],
    });
    const inserts = [];
    for (let index = 0; index < 5; index += 1) {
      inserts.push(await database.insert(tableName, { value: index % 2 === 0 }));
    }
    return compactionSourceStats(
      store,
      inserts.map((insert) => insert.segmentId),
    );
  };
  const options = {
    targetLevel: 2 as const,
    minimumLevel0Segments: 5,
    maxLevel0Segments: 5,
    targetBlockBytes: 1024,
    outputCompression: "raw" as const,
  };

  const belowStats = await seed("l2_binary_ratio_below");
  expect(belowStats.storedBytes).toBe(240);
  expect(
    await database.compactTable("l2_binary_ratio_below", {
      ...options,
      maxWriteAmplification: 0.19999999999999998,
    }),
  ).toMatchObject({
    compacted: false,
    skipReason: "write-amplification-budget",
    maximumOutputStoredBytes: 47,
    plannedOutputStoredBytesUpperBound: 48,
  });

  const exactStats = await seed("l2_binary_ratio_exact");
  expect(exactStats.storedBytes).toBe(240);
  expect(
    await database.compactTable("l2_binary_ratio_exact", {
      ...options,
      maxWriteAmplification: 0.2,
    }),
  ).toMatchObject({
    compacted: true,
    maximumOutputStoredBytes: 48,
    plannedOutputStoredBytesUpperBound: 48,
    outputStoredBytes: 48,
  });
  store.close();
});

for (const implementation of recoveryImplementations()) {
  it(`${implementation.name} reopens an L2 checkpoint with its exact prefix, ordinal, and ceiling`, async () => {
    const harness = await implementation.create();
    let store = harness.store;
    const tableName = `l2_reopen_${implementation.name.replaceAll(" ", "_")}`;
    const database = new MinnowDatabase(store, { compression: "raw" });
    await database.createTable({
      name: tableName,
      columns: [{ name: "value", type: "number" }],
    });
    const inserts = [];
    for (let value = 1; value <= 4; value += 1) {
      inserts.push(await database.insert(tableName, { value }));
    }
    const sourceSegmentIds = inserts.slice(0, 2).map((insert) => insert.segmentId);
    let progress = await database.compactTableStep(tableName, {
      targetLevel: 2,
      minimumLevel0Segments: 2,
      maxLevel0Segments: 2,
      maxWriteAmplification: 64,
      maxBlocks: 1,
      targetBlockBytes: 9,
      outputCompression: "raw",
    });
    if (progress.jobId === null) throw new Error("Expected a checkpointed L2 job");
    const jobId = progress.jobId;
    expect(progress).toMatchObject({
      state: "running",
      sourceSegmentCount: 2,
      outputBlockCount: 1,
      result: null,
    });
    const beforeReopen = await store.getCompactionJob(jobId);
    expect(beforeReopen).toMatchObject({
      targetLevel: 2,
      sourceSegmentIds,
      outputPartitionOrdinal: 0,
      maxWriteAmplification: 64,
    });

    store = await harness.reopen();
    expect(await store.getCompactionJob(jobId)).toEqual(beforeReopen);
    const reopened = new MinnowDatabase(store, { compression: "gzip", rowsPerBlock: 2048 });
    while (progress.result === null) {
      progress = await reopened.resumeCompactionJob(jobId, { maxBlocks: 1 });
    }
    if (progress.result.outputSegmentId === null) throw new Error("Expected a reopened L2 output");
    expect(progress.result).toMatchObject({
      compacted: true,
      sourceSegmentCount: 2,
      outputPartitionOrdinal: 0,
      outputCompression: "raw",
      maxWriteAmplification: 64,
      maximumOutputStoredBytes: beforeReopen?.maximumOutputStoredBytes,
      plannedOutputStoredBytesUpperBound: beforeReopen?.plannedOutputStoredBytesUpperBound,
    });
    expect(progress.result.outputStoredBytes).toBeLessThanOrEqual(
      progress.result.plannedOutputStoredBytesUpperBound ?? -1,
    );
    expect(await requiredSegment(store, progress.result.outputSegmentId)).toMatchObject({
      level: 2,
      partitionOrdinal: 0,
    });

    const second = await reopened.compactTable(tableName, {
      minimumLevel0Segments: 2,
      maxLevel0Segments: 2,
      maxWriteAmplification: 64,
      outputCompression: "raw",
    });
    if (second.outputSegmentId === null) throw new Error("Expected a second L2 output");
    expect(second.outputPartitionOrdinal).toBe(1);
    expect((await allVisibleSegments(reopened, tableName)).map((segment) => segment.id)).toEqual([
      progress.result.outputSegmentId,
      second.outputSegmentId,
    ]);
    expect(await reopened.readTable(tableName)).toEqual(
      inserts.map((_insert, index) => ({ value: index + 1 })),
    );
    expect(
      await reopened.readTable(tableName, requiredItem(inserts, 3, "L2 source snapshot").version),
    ).toEqual(inserts.map((_insert, index) => ({ value: index + 1 })));
    store.close();
  });
}

it("rebases an L2 prefix across a concurrent L0 append while retaining prior partitions", async () => {
  const indexedDB = new IDBFactory();
  const name = crypto.randomUUID();
  const compactorStore = await IndexedDbBlockStore.open({ name, indexedDB });
  const writerStore = await IndexedDbBlockStore.open({ name, indexedDB });
  const compactor = new MinnowDatabase(compactorStore, { compression: "raw" });
  const writer = new MinnowDatabase(writerStore, { compression: "raw" });
  const tableName = "l2_concurrent_append";
  await compactor.createTable({
    name: tableName,
    columns: [{ name: "value", type: "number" }],
  });
  const inserts = [];
  for (let value = 1; value <= 4; value += 1) {
    inserts.push(await compactor.insert(tableName, { value }));
  }
  const first = await compactor.compactTable(tableName, {
    targetLevel: 2,
    minimumLevel0Segments: 2,
    maxLevel0Segments: 2,
    outputCompression: "raw",
  });
  if (first.outputSegmentId === null) throw new Error("Expected an initial L2 partition");
  const retained = await requiredSegment(compactorStore, first.outputSegmentId);
  const retainedBlockIds = Object.values(retained.columnBlockIds).flat();
  const retainedBlocks = await compactorStore.getBlocks(retainedBlockIds);
  const sourceVersion = requiredItem(inserts, 3, "concurrent L2 source").version;

  let progress = await compactor.compactTableStep(tableName, {
    minimumLevel0Segments: 2,
    maxLevel0Segments: 2,
    maxWriteAmplification: 64,
    maxBlocks: 1,
    targetBlockBytes: 9,
    outputCompression: "raw",
  });
  if (progress.jobId === null) throw new Error("Expected a concurrent L2 job");
  const jobId = progress.jobId;
  expect(await compactorStore.getCompactionJob(jobId)).toMatchObject({
    targetLevel: 2,
    sourceSegmentIds: inserts.slice(2).map((insert) => insert.segmentId),
    outputPartitionOrdinal: 1,
  });
  const concurrent = await writer.insert(tableName, { value: 5 });

  while (progress.result === null) {
    progress = await compactor.resumeCompactionJob(jobId, { maxBlocks: 1 });
  }
  if (progress.result.outputSegmentId === null) throw new Error("Expected a rebased L2 output");
  expect(progress.result.outputPartitionOrdinal).toBe(1);
  expect(await requiredSegment(compactorStore, retained.id)).toEqual(retained);
  for (const [index, blockId] of retainedBlockIds.entries()) {
    expect(await compactorStore.getBlock(blockId)).toEqual(retainedBlocks[index]);
  }
  expect((await allVisibleSegments(compactor, tableName)).map((segment) => segment.id)).toEqual([
    retained.id,
    progress.result.outputSegmentId,
    concurrent.segmentId,
  ]);
  expect(await compactor.readTable(tableName)).toEqual([
    { value: 1 },
    { value: 2 },
    { value: 3 },
    { value: 4 },
    { value: 5 },
  ]);
  expect(await compactor.readTable(tableName, sourceVersion)).toEqual([
    { value: 1 },
    { value: 2 },
    { value: 3 },
    { value: 4 },
  ]);
  compactorStore.close();
  writerStore.close();
});

it("flushes a buffered writer after its age limit", async () => {
  const store = new MemoryBlockStore();
  const database = new MinnowDatabase(store);
  await database.createTable({
    name: "events",
    columns: [{ name: "value", type: "number" }],
  });
  const writer = database.bufferedWriter("events", { maxAgeMs: 5, maxRows: 100 });
  await writer.add({ value: 1 });
  await new Promise((resolve) => setTimeout(resolve, 30));
  expect(writer.pendingRowCount).toBe(0);
  expect(await database.readTable("events")).toEqual([{ value: 1 }]);
  await writer.close();
  store.close();
});

it("does not lose an age flush that fires behind an in-flight batch", async () => {
  const store = new FirstCommitBarrierMemoryBlockStore();
  const database = new MinnowDatabase(store);
  await database.createTable({
    name: "events",
    columns: [{ name: "value", type: "number" }],
  });
  const writer = database.bufferedWriter("events", { maxAgeMs: 5, maxRows: 100 });
  await writer.add({ value: 1 });
  const firstFlush = writer.flush();
  await store.firstCommitReached;
  await writer.add({ value: 2 });
  await new Promise((resolve) => setTimeout(resolve, 20));

  store.releaseFirstCommit();
  await firstFlush;
  await new Promise((resolve) => setTimeout(resolve, 20));
  expect(writer.pendingRowCount).toBe(0);
  expect(await database.readTable("events")).toEqual([{ value: 1 }, { value: 2 }]);
  await writer.close();
  store.close();
});

it("backpressures concurrent buffered adds before cloning an unbounded queue", async () => {
  const store = new FirstCommitBarrierMemoryBlockStore();
  const database = new MinnowDatabase(store);
  await database.createTable({
    name: "events",
    columns: [{ name: "value", type: "number" }],
  });
  const writer = database.bufferedWriter("events", {
    maxAgeMs: 60_000,
    maxRows: 1,
  });
  const firstAdd = writer.add({ value: 0 });
  await store.firstCommitReached;

  const queued = Array.from({ length: MAX_BUFFERED_WRITER_PENDING_ADDS - 1 }, (_, index) =>
    writer.add({ value: index + 1 }),
  );
  expect(writer.pendingRowCount).toBe(0);
  await expect(writer.add({ value: 999 })).rejects.toThrow(
    `more than ${String(MAX_BUFFERED_WRITER_PENDING_ADDS)} adds`,
  );
  expect(writer.pendingRowCount).toBe(0);

  store.releaseFirstCommit();
  await Promise.all([firstAdd, ...queued]);
  await writer.add({ value: MAX_BUFFERED_WRITER_PENDING_ADDS });
  await writer.close();
  expect(await database.readTable("events")).toEqual(
    Array.from({ length: MAX_BUFFERED_WRITER_PENDING_ADDS + 1 }, (_, value) => ({ value })),
  );
  store.close();
});

it("bounds the direct write queue before preprocessing caller rows", async () => {
  const store = new FirstCommitBarrierMemoryBlockStore();
  const database = new MinnowDatabase(store, { autoCollect: false, autoCompact: false });
  await database.createTable({
    name: "events",
    columns: [{ name: "value", type: "number" }],
  });
  const first = database.insert("events", { value: 0 });
  await store.firstCommitReached;
  const queued = Array.from({ length: MAX_DATABASE_PENDING_WRITES - 1 }, (_, index) =>
    database.insert("events", { value: index + 1 }),
  );
  let inspected = false;
  const refused = {
    get value(): number {
      inspected = true;
      return 999;
    },
  };
  await expect(database.insert("events", refused)).rejects.toThrow(
    `more than ${String(MAX_DATABASE_PENDING_WRITES)} writes`,
  );
  expect(inspected).toBe(false);

  store.releaseFirstCommit();
  await Promise.all([first, ...queued]);
  await database.insert("events", { value: MAX_DATABASE_PENDING_WRITES });
  expect(await database.readTable("events")).toEqual(
    Array.from({ length: MAX_DATABASE_PENDING_WRITES + 1 }, (_, value) => ({ value })),
  );
  await database.close();
});

it("requests a best-effort buffered flush when a page becomes hidden", async () => {
  const store = new MemoryBlockStore();
  const database = new MinnowDatabase(store);
  await database.createTable({
    name: "events",
    columns: [{ name: "value", type: "number" }],
  });
  const writer = database.bufferedWriter("events", { maxAgeMs: 60_000, maxRows: 100 });
  const listeners = new Set<() => void>();
  const documentTarget = {
    visibilityState: "visible",
    addEventListener: (_type: "visibilitychange", listener: () => void) => listeners.add(listener),
    removeEventListener: (_type: "visibilitychange", listener: () => void) =>
      listeners.delete(listener),
  };
  const detach = attachLifecycleFlush(writer, { document: documentTarget });
  await writer.add({ value: 1 });
  documentTarget.visibilityState = "hidden";
  listeners.forEach((listener) => listener());

  await writer.close();
  expect(await database.readTable("events")).toEqual([{ value: 1 }]);
  detach();
  expect(listeners).toHaveLength(0);
  store.close();
});

it("stages a bounded batch in one write and uses bulk reads", async () => {
  const store = new CountingMemoryBlockStore();
  const database = new MinnowDatabase(store, { rowsPerBlock: 1 });
  await database.createTable({
    name: "events",
    columns: [
      { name: "name", type: "string" },
      { name: "value", type: "number" },
    ],
  });
  await database.insertBatch("events", {
    columns: { name: ["one", "two", "three"], value: [1, 2, 3] },
  });
  expect(store.blockWriteCalls).toBe(1);
  await database.readTable("events");
  expect(store.blockReadCalls).toBe(1);
  expect(store.blockIdsRead[0]).toHaveLength(6);
  store.blockReadCalls = 0;
  store.blockIdsRead = [];
  expect(await database.readTable("events", { columns: ["value"] })).toEqual([
    { value: 1 },
    { value: 2 },
    { value: 3 },
  ]);
  expect(store.blockReadCalls).toBe(1);
  expect(store.blockIdsRead[0]).toHaveLength(3);
  await expect(database.readTable("events", { columns: [] })).rejects.toThrow(
    "at least one column",
  );
  await expect(database.readTable("events", { columns: ["missing"] })).rejects.toThrow(
    "Unknown column",
  );
  store.close();
});

it("stages encoded blocks early in bounded batches and aborts them after a late trigger fault", async () => {
  const store = new CountingMemoryBlockStore();
  const database = new MinnowDatabase(store, {
    autoCollect: false,
    compression: "raw",
    rowsPerBlock: 1,
  });
  await database.execute("CREATE TABLE bounded_source (value INTEGER NOT NULL)");
  await database.execute("CREATE TABLE bounded_audit (value INTEGER NOT NULL)");
  await database.execute(
    "CREATE TRIGGER bad_after AFTER INSERT ON bounded_source BEGIN " +
      "INSERT INTO bounded_audit (value) VALUES ('not a number'); END",
  );

  await expect(
    database.insertBatch("bounded_source", {
      columns: { value: Array.from({ length: 130 }, (_, value) => value) },
    }),
  ).rejects.toThrow();

  expect(store.stagedBlockBatchSizes.length).toBeGreaterThanOrEqual(3);
  expect(Math.max(...store.stagedBlockBatchSizes)).toBeLessThanOrEqual(64);
  expect(Math.max(...store.stagedBlockBatchBytes)).toBeLessThanOrEqual(MAX_TRANSACTION_STAGE_BYTES);
  expect(await database.readTable("bounded_source")).toEqual([]);
  expect(await database.readTable("bounded_audit")).toEqual([]);
  const aborted = await transactionRecords(store);
  expect(aborted).toHaveLength(1);
  expect(aborted[0]?.status).toBe("aborted");
  expect(aborted[0]?.pendingBlockIds).toHaveLength(130);
  await database.close();
});

it("answers metadata-only queries without loading a data block", async () => {
  const store = new CountingMemoryBlockStore();
  const database = new MinnowDatabase(store);
  await database.createTable({
    name: "events",
    columns: [{ name: "value", type: "number" }],
  });
  await database.insertBatch("events", { columns: { value: [1, 2, 3] } });
  store.blockReadCalls = 0;
  store.blockIdsRead = [];
  store.transactionListCalls = 0;
  store.transactionGetCalls = 0;
  store.transactionBatchCalls = 0;
  store.queryCatalogStateCalls = 0;
  store.segmentListCalls = 0;

  expect(await database.query("SELECT COUNT(*) AS count FROM events")).toEqual({
    columns: ["count"],
    columnDomains: [null],
    rows: [{ count: 3 }],
  });
  expect(store.blockReadCalls).toBe(0);
  // Memory's atomic catalog read answers directly from RecordCore, so none of the composed
  // per-record store methods run on this hot path.
  expect(store.transactionListCalls).toBe(0);
  expect(store.transactionGetCalls).toBe(0);
  expect(store.transactionBatchCalls).toBe(0);
  expect(store.segmentListCalls).toBe(0);
  expect(store.queryCatalogStateCalls).toBeLessThanOrEqual(1);
  store.close();
});

it("prunes numeric row groups and late-loads projected blocks after predicate selection", async () => {
  const store = new CountingMemoryBlockStore();
  const database = new MinnowDatabase(store, { compression: "raw", rowsPerBlock: 2 });
  await database.createTable({
    name: "pruned_metrics",
    columns: [
      { name: "score", type: "number", nullable: true },
      { name: "label", type: "string" },
      { name: "recordedAt", type: "datetime" },
    ],
  });
  await database.insertBatch("pruned_metrics", {
    columns: {
      score: [null, 2, 3, 4, 5, 6, 7, 8],
      label: ["one", "two", "three", "four", "five", "six", "seven", "eight"],
      recordedAt: Array.from({ length: 8 }, (_, index) => new Date(Date.UTC(2026, 0, index + 1))),
    },
  });
  const table = (await store.listTables()).find((candidate) => candidate.name === "pruned_metrics");
  if (table === undefined) throw new Error("Expected pruning table metadata");
  const scoreColumn = table.columns.find((column) => column.name === "score");
  const labelColumn = table.columns.find((column) => column.name === "label");
  const recordedAtColumn = table.columns.find((column) => column.name === "recordedAt");
  if (scoreColumn === undefined || labelColumn === undefined || recordedAtColumn === undefined) {
    throw new Error("Expected pruning columns");
  }
  const segment = (await allVisibleSegments(database, "pruned_metrics"))[0];
  if (segment === undefined) throw new Error("Expected pruning segment");

  store.blockReadCalls = 0;
  store.blockIdsRead = [];
  expect(await database.query("SELECT label FROM pruned_metrics WHERE score = 7")).toEqual({
    columns: ["label"],
    columnDomains: [null],
    rows: [{ label: "seven" }],
  });
  const firstReadIds = store.blockIdsRead.flat();
  expect(firstReadIds).toEqual([
    ...(segment.columnBlockIds[scoreColumn.id] ?? []),
    (segment.columnBlockIds[labelColumn.id] ?? [])[3],
  ]);
  expect(firstReadIds).toHaveLength(5);

  store.blockReadCalls = 0;
  store.blockIdsRead = [];
  expect(
    await database.query(
      "SELECT label FROM pruned_metrics WHERE DATE '2026-01-07' <= recordedAt ORDER BY label",
    ),
  ).toEqual({
    columns: ["label"],
    columnDomains: [null],
    rows: [{ label: "eight" }, { label: "seven" }],
  });
  // The surviving label block decoded during the first query is served from the decoded-block
  // cache, so only the new predicate column's blocks reach the store.
  expect(store.blockIdsRead.flat()).toEqual(segment.columnBlockIds[recordedAtColumn.id] ?? []);

  store.blockReadCalls = 0;
  store.blockIdsRead = [];
  expect(await database.query("SELECT score FROM pruned_metrics WHERE label = 'seven'")).toEqual({
    columns: ["score"],
    columnDomains: [null],
    rows: [{ score: 7 }],
  });
  // Full scan over both columns (4 blocks each), minus the score and label blocks already in
  // the decoded-block cache from the pruned queries above.
  expect(store.blockIdsRead.flat()).toHaveLength(6);
  store.close();
});

for (const implementation of [
  { name: "memory", create: async (): Promise<BlockStore> => new MemoryBlockStore() },
  {
    name: "indexeddb",
    create: async (): Promise<BlockStore> =>
      IndexedDbBlockStore.open({ name: crypto.randomUUID(), indexedDB: new IDBFactory() }),
  },
]) {
  it(`${implementation.name} streams keyed-mutation scans through a budget too small to materialize`, async () => {
    const store = await implementation.create();
    const database = new MinnowDatabase(store, { rowsPerBlock: 512 });
    await database.createTable({
      name: "mutated",
      uniqueKey: "id",
      columns: [
        { name: "id", type: "number" },
        { name: "label", type: "string" },
        { name: "score", type: "number" },
      ],
    });
    const total = 20_000;
    for (let start = 0; start < total; start += 5_000) {
      await database.insertBatch("mutated", {
        columns: {
          id: Array.from({ length: 5_000 }, (_, index) => start + index),
          label: Array.from(
            { length: 5_000 },
            (_, index) => `label-${String((start + index) % 97)}`,
          ),
          score: Array.from({ length: 5_000 }, (_, index) => (start + index) % 1_000),
        },
      });
    }
    // First update wave, then deletions, then an overlapping update (last writer wins), then a
    // re-insert of deleted keys (they must reappear at their new slot position, not the old one).
    const updatedKeys = Array.from({ length: 400 }, (_, index) => index * 20);
    await database.updateBatch("mutated", {
      keys: updatedKeys,
      changes: {
        score: updatedKeys.map((key) => key + 100_000),
        label: updatedKeys.map((key) => `updated-${String(key)}`),
      },
    });
    const deletedKeys = Array.from({ length: 200 }, (_, index) => 12_000 + index * 3);
    await database.deleteBatch("mutated", { keys: deletedKeys });
    const secondUpdateKeys = updatedKeys.slice(0, 100);
    await database.updateBatch("mutated", {
      keys: secondUpdateKeys,
      changes: { score: secondUpdateKeys.map((key) => key + 500_000) },
    });
    await database.insertBatch("mutated", [
      { id: deletedKeys[0] ?? 1, label: "reborn", score: -1 },
    ]);

    const statements = [
      "SELECT id, label, score FROM mutated WHERE score >= 100000",
      "SELECT label, COUNT(*) AS n, SUM(score) AS s FROM mutated GROUP BY label ORDER BY label",
      "SELECT id, score FROM mutated ORDER BY score DESC, id LIMIT 50",
      "SELECT COUNT(*) AS n FROM mutated",
    ];
    for (const sql of statements) {
      const expected = await database.query(sql);
      if (sql === "SELECT COUNT(*) AS n FROM mutated") {
        // Metadata-only shape: pin streamed result equality under an ordinary budget.
        expect(await database.query(sql, { executionMemoryBudgetBytes: 1_000_000 })).toEqual(
          expected,
        );
        continue;
      }
      // Measured bracket for this fixture: every statement streams under ~252k modeled bytes
      // while the materialized fallback needs at least ~313k, so 300k splits them cleanly.
      const budget = 300_000;
      await expect(
        database.query(sql, { executionMemoryBudgetBytes: budget, spillToStorage: false }),
      ).rejects.toThrow(QueryMemoryBudgetError);
      expect(await database.query(sql, { executionMemoryBudgetBytes: budget })).toEqual(expected);
    }
    store.close();
  });

  it(`${implementation.name} partitions an inner join whose build side exceeds the budget`, async () => {
    const store = await implementation.create();
    const database = new MinnowDatabase(store, { rowsPerBlock: 1_024 });
    await database.createTable({
      name: "build_side",
      columns: [
        { name: "id", type: "number", nullable: true },
        { name: "payload", type: "string" },
      ],
    });
    await database.createTable({
      name: "probe_side",
      uniqueKey: "pid",
      columns: [
        { name: "pid", type: "number" },
        { name: "build_id", type: "number" },
        { name: "score", type: "number" },
      ],
    });
    // 30k build rows: every third id duplicated (fanout), some null keys (never match), payloads
    // distinct so sorted comparisons are total.
    const buildCount = 30_000;
    await database.insertBatch("build_side", {
      columns: {
        id: Array.from({ length: buildCount }, (_, index) =>
          index % 97 === 0 ? null : index % 3 === 0 ? index - (index % 2) : index,
        ),
        payload: Array.from(
          { length: buildCount },
          (_, index) => `payload-${String(index).padStart(6, "0")}-${"x".repeat(24)}`,
        ),
      },
    });
    // 4k probe rows referencing build ids, including misses beyond the build range.
    const probeCount = 4_000;
    await database.insertBatch("probe_side", {
      columns: {
        pid: Array.from({ length: probeCount }, (_, index) => index),
        build_id: Array.from({ length: probeCount }, (_, index) => (index * 7) % 40_000),
        score: Array.from({ length: probeCount }, (_, index) => index % 100),
      },
    });
    const sql =
      "SELECT p.pid AS pid, j.payload AS payload, p.score AS score " +
      "FROM probe_side p JOIN build_side j ON p.build_id = j.id WHERE p.score >= 25";
    const expected = await database.query(sql);
    const budget = 1_500_000;
    // The materialized path (spill disabled) genuinely cannot prepare under this budget...
    await expect(
      database.query(sql, { executionMemoryBudgetBytes: budget, spillToStorage: false }),
    ).rejects.toThrow(QueryMemoryBudgetError);
    // ...while the partitioned join returns the same multiset (row order across partitions is
    // implementation-defined, like any unordered query).
    const budgeted = await database.query(sql, { executionMemoryBudgetBytes: budget });
    const sorted = (rows: QueryRow[]) =>
      rows
        .slice()
        .sort(
          (left, right) =>
            Number(left.pid) - Number(right.pid) ||
            String(left.payload).localeCompare(String(right.payload)),
        );
    expect(budgeted.columns).toEqual(expected.columns);
    expect(sorted(budgeted.rows)).toEqual(sorted(expected.rows));
    expect(budgeted.rows.length).toBeGreaterThan(0);

    // LIMIT under the budget returns exactly that many rows, all from the expected multiset.
    const limited = await database.query(`${sql} LIMIT 40`, {
      executionMemoryBudgetBytes: budget,
    });
    expect(limited.rows).toHaveLength(40);
    const expectedKeys = new Set(
      expected.rows.map((row) => `${String(row.pid)}|${String(row.payload)}`),
    );
    for (const row of limited.rows) {
      expect(expectedKeys.has(`${String(row.pid)}|${String(row.payload)}`)).toBe(true);
    }

    // Streaming reorders the append-only join before it chooses the scan side: the large table
    // streams while the small table becomes the build index, so ordering also fits the budget.
    const ordered = await database.query(`${sql} ORDER BY pid`, {
      executionMemoryBudgetBytes: budget,
    });
    expect(ordered.rows).toEqual(sorted(expected.rows));
    store.close();
  });

  it(`${implementation.name} partitions a join over a build side with mutation history`, async () => {
    const store = await implementation.create();
    const database = new MinnowDatabase(store, { rowsPerBlock: 512 });
    await database.createTable({
      name: "dim",
      uniqueKey: "id",
      columns: [
        { name: "id", type: "number" },
        { name: "label", type: "string" },
      ],
    });
    await database.createTable({
      name: "fact",
      uniqueKey: "fid",
      columns: [
        { name: "fid", type: "number" },
        { name: "dim_id", type: "number" },
      ],
    });
    const dimCount = 12_000;
    await database.insertBatch("dim", {
      columns: {
        id: Array.from({ length: dimCount }, (_, index) => index),
        label: Array.from(
          { length: dimCount },
          (_, index) => `dim-${String(index)}-${"y".repeat(20)}`,
        ),
      },
    });
    const relabeled = Array.from({ length: 300 }, (_, index) => index * 11);
    await database.updateBatch("dim", {
      keys: relabeled,
      changes: { label: relabeled.map((key) => `relabeled-${String(key)}`) },
    });
    await database.deleteBatch("dim", {
      keys: Array.from({ length: 150 }, (_, index) => 6_000 + index * 5),
    });
    await database.insertBatch("fact", {
      columns: {
        fid: Array.from({ length: 3_000 }, (_, index) => index),
        dim_id: Array.from({ length: 3_000 }, (_, index) => (index * 13) % 13_000),
      },
    });
    const sql = "SELECT f.fid AS fid, d.label AS label FROM fact f JOIN dim d ON f.dim_id = d.id";
    const expected = await database.query(sql);
    // Measured bracket: the partitioned streamed join fits from ~429k modeled bytes while the
    // materialized fallback needs at least ~752k, so 550k splits them cleanly.
    const budget = 550_000;
    await expect(
      database.query(sql, { executionMemoryBudgetBytes: budget, spillToStorage: false }),
    ).rejects.toThrow(QueryMemoryBudgetError);
    const budgeted = await database.query(sql, { executionMemoryBudgetBytes: budget });
    const byFid = (rows: QueryRow[]) =>
      rows.slice().sort((left, right) => Number(left.fid) - Number(right.fid));
    expect(byFid(budgeted.rows)).toEqual(byFid(expected.rows));
    store.close();
  });

  it(`${implementation.name} keeps upsert histories on the materialized path with correct results`, async () => {
    const store = await implementation.create();
    const database = new MinnowDatabase(store, { rowsPerBlock: 128 });
    await database.createTable({
      name: "upserted",
      uniqueKey: "id",
      columns: [
        { name: "id", type: "number" },
        { name: "score", type: "number" },
      ],
    });
    await database.insertBatch("upserted", {
      columns: {
        id: Array.from({ length: 1_000 }, (_, index) => index),
        score: Array.from({ length: 1_000 }, (_, index) => index),
      },
    });
    await database.upsertBatch("upserted", [
      { id: 5, score: 999 },
      { id: 2_000, score: 1 },
    ]);
    await database.deleteBatch("upserted", { keys: [7] });
    const sql = "SELECT id, score FROM upserted WHERE score > 500";
    const expected = await database.query(sql);
    expect(await database.query(sql, { executionMemoryBudgetBytes: 8_000_000 })).toEqual(expected);
    store.close();
  });
}

it("indexeddb folds unique-key chunk tails into base records and keeps conflict detection", async () => {
  const store = await IndexedDbBlockStore.open({
    name: crypto.randomUUID(),
    indexedDB: new IDBFactory(),
  });
  const database = new MinnowDatabase(store);
  await database.createTable({
    name: "folded",
    uniqueKey: "id",
    columns: [
      { name: "id", type: "string" },
      { name: "value", type: "number" },
    ],
  });
  // 20 single-row commits cross the 16-chunk fold threshold, so early keys live in folded
  // base records while late keys are still in tail chunks.
  for (let index = 0; index < 20; index += 1) {
    await database.insertBatch("folded", [{ id: `key-${String(index)}`, value: index }]);
  }
  await expect(database.insertBatch("folded", [{ id: "key-0", value: 99 }])).rejects.toThrow(
    UniqueConstraintError,
  );
  await expect(database.insertBatch("folded", [{ id: "key-19", value: 99 }])).rejects.toThrow(
    UniqueConstraintError,
  );
  // Removal and re-insert cross the fold boundary in both directions.
  await database.deleteBatch("folded", { keys: ["key-0"] });
  await database.insertBatch("folded", [{ id: "key-0", value: 100 }]);
  // Upsert classification sees folded and tail keys alike.
  const upserted = await database.upsertBatch("folded", [
    { id: "key-1", value: 200 },
    { id: "brand-new", value: 1 },
  ]);
  expect(upserted).toMatchObject({ insertedRowCount: 1, updatedRowCount: 1 });
  expect(await database.query("SELECT COUNT(*) AS n FROM folded")).toEqual({
    columns: ["n"],
    columnDomains: [null],
    rows: [{ n: 21 }],
  });
  store.close();
});

it("shares one visibility catalog across multi-table query preparation", async () => {
  const store = new CountingMemoryBlockStore();
  const database = new MinnowDatabase(store);
  for (const table of ["left_rows", "right_rows"]) {
    await database.createTable({
      name: table,
      columns: [{ name: "id", type: "number" }],
    });
    await database.insertBatch(table, { columns: { id: [1, 2] } });
  }
  store.transactionListCalls = 0;
  store.transactionGetCalls = 0;
  store.transactionBatchCalls = 0;
  store.queryCatalogStateCalls = 0;
  store.segmentListCalls = 0;

  expect(
    await database.query(
      "SELECT COUNT(*) AS count FROM left_rows l JOIN right_rows r ON r.id = l.id",
    ),
  ).toEqual({ columns: ["count"], columnDomains: [null], rows: [{ count: 2 }] });
  expect(store.transactionListCalls).toBe(0);
  expect(store.transactionGetCalls).toBe(0);
  expect(store.transactionBatchCalls).toBe(0);
  expect(store.segmentListCalls).toBe(0);
  expect(store.queryCatalogStateCalls).toBe(1);
  store.close();
});

for (const implementation of implementations()) {
  it(`${implementation.name} executes durable ORDER BY spill through the public query API`, async () => {
    const store = await implementation.create();
    const database = new MinnowDatabase(store, { rowsPerBlock: 256, compression: "raw" });
    await database.createTable({
      name: "spill_rows",
      columns: [
        { name: "id", type: "number" },
        { name: "bucket", type: "number" },
      ],
    });
    const rowCount = 5_000;
    await database.insertBatch("spill_rows", {
      columns: {
        id: Array.from({ length: rowCount }, (_, index) => index),
        bucket: Array.from({ length: rowCount }, (_, index) => index % 11),
      },
    });

    const result = await database.query(
      "SELECT id, bucket FROM spill_rows ORDER BY bucket, id DESC LIMIT 137",
      { executionMemoryBudgetBytes: 150_000, spillPageRows: 64 },
    );
    expect(result.rows).toHaveLength(137);
    expect(result.rows.slice(0, 3)).toEqual([
      { id: 4994, bucket: 0 },
      { id: 4983, bucket: 0 },
      { id: 4972, bucket: 0 },
    ]);
    expect(result.rows[136]).toEqual({ id: 3498, bucket: 0 });
    const grouped = await database.query(
      "SELECT id, COUNT(*) AS count FROM spill_rows GROUP BY id ORDER BY id DESC LIMIT 101",
      { executionMemoryBudgetBytes: 150_000, spillPageRows: 64 },
    );
    expect(grouped.rows).toHaveLength(101);
    expect(grouped.rows.slice(0, 2)).toEqual([
      { id: 4999, count: 1 },
      { id: 4998, count: 1 },
    ]);
    store.close();
  });
}

it("pages a streamed scan the same at every depth, ties kept in arrival order", async () => {
  const database = new MinnowDatabase(new MemoryBlockStore(), {
    rowsPerBlock: 512,
    compression: "raw",
  });
  await database.createTable({
    name: "paged",
    columns: [
      { name: "v", type: "number" },
      { name: "tag", type: "string", nullable: true },
      { name: "score", type: "number", nullable: true },
    ],
  });
  const rowCount = 10_000;
  await database.insertBatch("paged", {
    columns: {
      v: Array.from({ length: rowCount }, (_, index) => index),
      tag: Array.from({ length: rowCount }, (_, index) =>
        index % 7 === 0 ? null : `tag-${String(index % 5)}`,
      ),
      score: Array.from({ length: rowCount }, (_, index) =>
        index % 13 === 0 ? null : (index * 7919) % 1000,
      ),
    },
  });
  // Rows arrive in `v` order, so a stable sort on the other columns alone must agree with the
  // same sort tie-broken by `v` explicitly — whichever way the page is kept: a bound under a
  // tenth of the table, or every row kept and sorted once above it.
  const pages: ReadonlyArray<[limit: number, offset: number]> = [
    [100, 0],
    [900, 50],
    [1_000, 0],
    [2_500, 500],
    [9_000, 2_000],
    [50, 9_990],
  ];
  for (const [order, tieBroken] of [
    ["tag, score DESC", "tag, score DESC, v"],
    ["score DESC NULLS LAST", "score DESC NULLS LAST, v"],
    ["tag DESC NULLS FIRST, score", "tag DESC NULLS FIRST, score, v"],
  ]) {
    const reference = await database.query(
      `SELECT v, tag, score FROM paged ORDER BY ${tieBroken ?? ""}`,
    );
    expect(reference.rows).toHaveLength(rowCount);
    for (const [limit, offset] of pages) {
      const sql = `SELECT v, tag, score FROM paged ORDER BY ${order ?? ""} LIMIT ${String(limit)} OFFSET ${String(offset)}`;
      const page = await database.query(sql);
      expect(page.rows, sql).toEqual(reference.rows.slice(offset, offset + limit));
    }
  }
});

for (const implementation of implementations()) {
  it(`${implementation.name} streams append scans through a budget too small to materialize`, async () => {
    const store = await implementation.create();
    const database = new MinnowDatabase(store, { rowsPerBlock: 512, compression: "raw" });
    await database.createTable({
      name: "streamed_rows",
      columns: [
        { name: "v", type: "number" },
        { name: "tag", type: "string", nullable: true },
      ],
    });
    const rowCount = 20_000;
    await database.insertBatch("streamed_rows", {
      columns: {
        v: Array.from({ length: rowCount }, (_, index) => index),
        tag: Array.from({ length: rowCount }, (_, index) =>
          index % 7 === 0 ? null : `tag-${String(index % 5)}`,
        ),
      },
    });
    const budget = 64_000;

    await expect(
      database.query("SELECT SUM(v) AS total, COUNT(*) AS rows FROM streamed_rows", {
        executionMemoryBudgetBytes: budget,
        spillToStorage: false,
      }),
    ).rejects.toThrow(QueryMemoryBudgetError);

    const streamed = await database.query(
      "SELECT SUM(v) AS total, COUNT(*) AS rows FROM streamed_rows",
      { executionMemoryBudgetBytes: budget },
    );
    expect(streamed.rows).toEqual([{ total: (rowCount * (rowCount - 1)) / 2, rows: rowCount }]);

    const grouped = await database.query(
      "SELECT tag, COUNT(*) AS count FROM streamed_rows WHERE v >= 10000 GROUP BY tag",
      { executionMemoryBudgetBytes: budget },
    );
    const reference = await database.query(
      "SELECT tag, COUNT(*) AS count FROM streamed_rows WHERE v >= 10000 GROUP BY tag",
    );
    const byTag = (left: { tag?: unknown }, right: { tag?: unknown }) =>
      String(left.tag).localeCompare(String(right.tag));
    expect([...grouped.rows].sort(byTag)).toEqual([...reference.rows].sort(byTag));

    const ordered = await database.query(
      "SELECT v, tag FROM streamed_rows WHERE v < 19000 ORDER BY v DESC LIMIT 23",
      { executionMemoryBudgetBytes: budget, spillPageRows: 512 },
    );
    const orderedReference = await database.query(
      "SELECT v, tag FROM streamed_rows WHERE v < 19000 ORDER BY v DESC LIMIT 23",
    );
    expect(ordered.rows).toEqual(orderedReference.rows);

    const groupedOrdered = await database.query(
      "SELECT tag, COUNT(*) AS count, MIN(v) AS low, MAX(v) AS high, AVG(v) AS mean FROM streamed_rows GROUP BY tag ORDER BY tag LIMIT 4",
      { executionMemoryBudgetBytes: budget, spillPageRows: 512 },
    );
    const groupedOrderedReference = await database.query(
      "SELECT tag, COUNT(*) AS count, MIN(v) AS low, MAX(v) AS high, AVG(v) AS mean FROM streamed_rows GROUP BY tag ORDER BY tag LIMIT 4",
    );
    expect(groupedOrdered.rows).toEqual(groupedOrderedReference.rows);

    const unorderedGroups = await database.query(
      "SELECT v, COUNT(*) AS count FROM streamed_rows GROUP BY v",
      { executionMemoryBudgetBytes: budget, spillPageRows: 512 },
    );
    const unorderedReference = await database.query(
      "SELECT v, COUNT(*) AS count FROM streamed_rows GROUP BY v",
    );
    const byV = (left: { v?: unknown }, right: { v?: unknown }) => Number(left.v) - Number(right.v);
    expect([...unorderedGroups.rows].sort(byV)).toEqual([...unorderedReference.rows].sort(byV));
    expect(unorderedGroups.rows).toHaveLength(rowCount);

    const dictionaryFiltered = await database.query(
      "SELECT COUNT(*) AS count FROM streamed_rows WHERE tag = 'tag-1'",
      { executionMemoryBudgetBytes: budget },
    );
    expect(dictionaryFiltered.rows).toEqual(
      (await database.query("SELECT COUNT(*) AS count FROM streamed_rows WHERE tag = 'tag-1'"))
        .rows,
    );
    const dictionaryNegated = await database.query(
      "SELECT COUNT(*) AS count FROM streamed_rows WHERE tag != 'missing-tag'",
      { executionMemoryBudgetBytes: budget },
    );
    expect(dictionaryNegated.rows).toEqual(
      (
        await database.query(
          "SELECT COUNT(*) AS count FROM streamed_rows WHERE tag != 'missing-tag'",
        )
      ).rows,
    );

    const distinct = await database.query("SELECT DISTINCT v FROM streamed_rows", {
      executionMemoryBudgetBytes: budget,
      spillPageRows: 512,
    });
    expect(distinct.rows).toHaveLength(rowCount);
    const distinctTags = await database.query(
      "SELECT DISTINCT tag FROM streamed_rows ORDER BY tag",
      { executionMemoryBudgetBytes: budget, spillPageRows: 512 },
    );
    expect(distinctTags.rows).toEqual(
      (await database.query("SELECT DISTINCT tag FROM streamed_rows ORDER BY tag")).rows,
    );

    const having = await database.query(
      "SELECT tag, COUNT(*) AS count FROM streamed_rows GROUP BY tag HAVING COUNT(*) > 3000 ORDER BY tag",
      { executionMemoryBudgetBytes: budget, spillPageRows: 512 },
    );
    expect(having.rows).toEqual(
      (
        await database.query(
          "SELECT tag, COUNT(*) AS count FROM streamed_rows GROUP BY tag HAVING COUNT(*) > 3000 ORDER BY tag",
        )
      ).rows,
    );
    expect(having.rows.length).toBeGreaterThan(0);

    expect(await store.listTempOwnerIdsPage(null, 4)).toEqual({ records: [], nextCursor: null });
    store.close();
  });
}

for (const implementation of implementations()) {
  it(`${implementation.name} streams the joined probe side with materialized build sides`, async () => {
    const store = await implementation.create();
    const database = new MinnowDatabase(store, { rowsPerBlock: 512, compression: "raw" });
    await database.createTable({
      name: "stream_fact",
      columns: [
        { name: "v", type: "number" },
        { name: "code", type: "string" },
      ],
    });
    await database.createTable({
      name: "stream_dim",
      uniqueKey: "code",
      columns: [
        { name: "code", type: "string" },
        { name: "segment", type: "string" },
      ],
    });
    const rowCount = 20_000;
    await database.insertBatch("stream_fact", {
      columns: {
        v: Array.from({ length: rowCount }, (_, index) => index),
        code: Array.from({ length: rowCount }, (_, index) => `c-${String(index % 45)}`),
      },
    });
    await database.insertBatch("stream_dim", {
      columns: {
        code: Array.from({ length: 40 }, (_, index) => `c-${String(index)}`),
        segment: Array.from({ length: 40 }, (_, index) => `seg-${String(index % 6)}`),
      },
    });
    await database.upsertBatch("stream_dim", {
      columns: { code: ["c-1", "c-2"], segment: ["seg-x", "seg-y"] },
    });
    const budget = 96_000;

    await expect(
      database.query(
        "SELECT d.segment, SUM(o.v) AS total FROM stream_fact o JOIN stream_dim d ON d.code = o.code GROUP BY d.segment",
        { executionMemoryBudgetBytes: budget, spillToStorage: false },
      ),
    ).rejects.toThrow(QueryMemoryBudgetError);

    const orderedSql =
      "SELECT o.v, d.segment FROM stream_fact o JOIN stream_dim d ON d.code = o.code WHERE o.v < 19000 ORDER BY o.v DESC LIMIT 21";
    expect(
      (
        await database.query(orderedSql, {
          executionMemoryBudgetBytes: budget,
          spillPageRows: 512,
        })
      ).rows,
    ).toEqual((await database.query(orderedSql)).rows);

    const groupedSql =
      "SELECT d.segment, COUNT(*) AS orders, SUM(o.v) AS total FROM stream_fact o JOIN stream_dim d ON d.code = o.code GROUP BY d.segment ORDER BY total DESC LIMIT 4";
    expect(
      (
        await database.query(groupedSql, {
          executionMemoryBudgetBytes: budget,
          spillPageRows: 512,
        })
      ).rows,
    ).toEqual((await database.query(groupedSql)).rows);

    const leftSql =
      "SELECT o.v, d.segment FROM stream_fact o LEFT JOIN stream_dim d ON d.code = o.code WHERE o.v >= 19975 AND o.v < 19985";
    const left = await database.query(leftSql, { executionMemoryBudgetBytes: budget });
    expect(left.rows).toEqual((await database.query(leftSql)).rows);
    expect(left.rows.some((row) => row.segment === null)).toBe(true);

    expect(await store.listTempOwnerIdsPage(null, 4)).toEqual({ records: [], nextCursor: null });
    store.close();
  });
}

for (const implementation of implementations()) {
  it(`${implementation.name} spills grouped ordered joins through value-carrying partitions`, async () => {
    const store = await implementation.create();
    const database = new MinnowDatabase(store, { rowsPerBlock: 256, compression: "raw" });
    await database.createTable({
      name: "join_orders",
      columns: [
        { name: "customer_id", type: "number" },
        { name: "total", type: "number" },
      ],
    });
    await database.createTable({
      name: "join_customers",
      columns: [
        { name: "id", type: "number" },
        { name: "segment", type: "string" },
      ],
    });
    const orderCount = 3_000;
    await database.insertBatch("join_orders", {
      columns: {
        customer_id: Array.from({ length: orderCount }, (_, index) => index % 40),
        total: Array.from({ length: orderCount }, (_, index) => index),
      },
    });
    await database.insertBatch("join_customers", {
      columns: {
        id: Array.from({ length: 40 }, (_, index) => index),
        segment: Array.from({ length: 40 }, (_, index) => `segment-${String(index % 6)}`),
      },
    });

    const sql =
      "SELECT c.segment, COUNT(*) AS orders, SUM(o.total) AS revenue FROM join_orders o JOIN join_customers c ON c.id = o.customer_id GROUP BY c.segment ORDER BY revenue DESC LIMIT 5";
    const spilled = await database.query(sql, {
      executionMemoryBudgetBytes: 400_000,
      spillToStorage: true,
      spillPageRows: 256,
    });
    const reference = await database.query(sql);
    expect(spilled.rows).toEqual(reference.rows);
    expect(spilled.rows).toHaveLength(5);
    expect(await store.listTempOwnerIdsPage(null, 4)).toEqual({ records: [], nextCursor: null });
    store.close();
  });
}

for (const implementation of implementations()) {
  it(`${implementation.name} executes SQL mutations through keyed batches`, async () => {
    const store = await implementation.create();
    const database = new MinnowDatabase(store, { rowsPerBlock: 32, compression: "raw" });
    await database.createTable({
      name: "sql_people",
      uniqueKey: "name",
      columns: [
        { name: "name", type: "string" },
        { name: "score", type: "number" },
        { name: "joined", type: "datetime", nullable: true },
      ],
    });

    const inserted = await database.execute(
      "INSERT INTO sql_people (name, score, joined) VALUES ('Ada', 10, DATE '2026-01-01'), ('Grace', 20, NULL), ('Katherine', 30, NULL)",
    );
    expect(inserted).toMatchObject({ kind: "insert", table: "sql_people", rowCount: 3 });

    const updated = await database.execute(
      "UPDATE sql_people SET score = score * 2 + 1 WHERE score >= 20",
    );
    expect(updated).toMatchObject({ kind: "update", rowCount: 2 });
    expect((await database.query("SELECT name, score FROM sql_people ORDER BY name")).rows).toEqual(
      [
        { name: "Ada", score: 10 },
        { name: "Grace", score: 41 },
        { name: "Katherine", score: 61 },
      ],
    );

    const noMatch = await database.execute("UPDATE sql_people SET score = 0 WHERE score > 100");
    expect(noMatch).toMatchObject({ kind: "update", rowCount: 0 });

    const deleted = await database.execute(
      "DELETE FROM sql_people WHERE name IN ('Ada', 'missing')",
    );
    expect(deleted).toMatchObject({ kind: "delete", rowCount: 1 });
    const remaining = await database.execute("SELECT COUNT(*) AS count FROM sql_people");
    expect(remaining).toMatchObject({ kind: "rows" });
    if (remaining.kind === "rows") {
      expect(remaining.result.rows).toEqual([{ count: 2 }]);
    }

    await database.createTable({
      name: "sql_events",
      columns: [{ name: "value", type: "number" }],
    });
    await expect(database.execute("DELETE FROM sql_events")).rejects.toThrow(
      "DELETE requires a table with a unique key",
    );
    await expect(database.execute("DROP TABLE sql_people CASCADE")).rejects.toThrow(
      "DROP TABLE CASCADE is not supported",
    );
    await expect(database.execute("DROP INDEX whatever")).rejects.toThrow(
      "Unknown index: whatever",
    );
    await expect(database.query("DELETE FROM sql_people")).rejects.toThrow("Expected SELECT");
    await expect(
      database.execute("INSERT INTO sql_people (name, score) VALUES ('Zoe')"),
    ).rejects.toThrow("Each INSERT row must match the column list length");
    // Division by zero is NULL (SQLite semantics); the write boundary then rejects the NULL
    // for a non-nullable column, so the statement still fails loudly rather than corrupting.
    await expect(
      database.execute("UPDATE sql_people SET score = score / 0 WHERE score > 0"),
    ).rejects.toThrow("score[0] cannot be null");
    store.close();
  });
}

for (const implementation of implementations()) {
  it(`${implementation.name} executes CTEs and derived tables through the public API`, async () => {
    const store = await implementation.create();
    const database = new MinnowDatabase(store, { rowsPerBlock: 64, compression: "raw" });
    await database.createTable({
      name: "cte_sales",
      columns: [
        { name: "region", type: "string", nullable: true },
        { name: "amount", type: "number" },
      ],
    });
    await database.insertBatch("cte_sales", {
      columns: {
        region: Array.from({ length: 300 }, (_, index) =>
          index % 11 === 0 ? null : `region-${String(index % 4)}`,
        ),
        amount: Array.from({ length: 300 }, (_, index) => index),
      },
    });

    const nested = await database.query(
      "WITH totals AS (SELECT region, SUM(amount) AS total, COUNT(*) AS orders FROM cte_sales GROUP BY region) SELECT COUNT(*) AS regions, MAX(total) AS peak FROM totals",
    );
    const flat = await database.query(
      "SELECT region, SUM(amount) AS total FROM cte_sales GROUP BY region",
    );
    expect(nested.rows[0]?.regions).toBe(flat.rows.length);
    expect(nested.rows[0]?.peak).toBe(Math.max(...flat.rows.map((row) => Number(row.total))));

    const joined = await database.query(
      "WITH hot AS (SELECT region, SUM(amount) AS total FROM cte_sales GROUP BY region) SELECT s.amount, h.total FROM cte_sales s JOIN hot h ON h.region = s.region WHERE s.amount < 5 ORDER BY s.amount",
    );
    expect(joined.rows.length).toBeGreaterThan(0);

    const emptyDerived = await database.query(
      "WITH none AS (SELECT region, amount FROM cte_sales WHERE amount < 0) SELECT COUNT(*) AS count, MAX(amount) AS peak FROM none",
      { executionMemoryBudgetBytes: 512_000 },
    );
    expect(emptyDerived.rows).toEqual([{ count: 0, peak: null }]);

    const aboveAverage = await database.query(
      "SELECT COUNT(*) AS count FROM cte_sales WHERE amount > (SELECT AVG(amount) FROM cte_sales)",
      { executionMemoryBudgetBytes: 512_000 },
    );
    const average = 299 / 2;
    expect(aboveAverage.rows).toEqual([
      { count: Array.from({ length: 300 }, (_, index) => index).filter((v) => v > average).length },
    ]);

    const membership = await database.query(
      "SELECT region, COUNT(*) AS count FROM cte_sales WHERE region IN (SELECT region FROM cte_sales WHERE amount = 1) GROUP BY region",
    );
    expect(membership.rows).toHaveLength(1);

    const explained = await database.explain(
      "WITH scaled AS (SELECT region, amount * 2 AS doubled FROM cte_sales) SELECT doubled FROM scaled WHERE doubled > 10",
    );
    expect(explained).toContain("where (amount * 2) > 10");
    // After predicate pushdown the outer block is a pure projection wrapper, which executes
    // as its inner filtered scan — a streamable shape.
    expect(explained).toContain("-- streams the base scan");
    expect(
      await database.explain("SELECT SUM(amount) AS total FROM cte_sales WHERE amount > 5"),
    ).toContain("-- streams the base scan");

    const union = await database.query(
      "SELECT region, amount FROM cte_sales WHERE amount < 3 UNION SELECT region, amount FROM cte_sales WHERE amount < 5 ORDER BY amount",
    );
    expect(union.rows).toHaveLength(5);
    const unionAll = await database.query(
      "SELECT amount FROM cte_sales WHERE amount < 3 UNION ALL SELECT amount FROM cte_sales WHERE amount < 5 ORDER BY amount LIMIT 6",
      { executionMemoryBudgetBytes: 512_000 },
    );
    expect(unionAll.rows.map((row) => row.amount)).toEqual([0, 0, 1, 1, 2, 2]);
    await expect(
      database.query("SELECT region FROM cte_sales UNION SELECT amount FROM cte_sales"),
    ).rejects.toThrow("UNION member column types must match");
    store.close();
  });
}

for (const implementation of implementations()) {
  it(`${implementation.name} falls back to materialized replay for keyed mutation snapshots`, async () => {
    const store = await implementation.create();
    const database = new MinnowDatabase(store, { rowsPerBlock: 128, compression: "raw" });
    await database.createTable({
      name: "streamed_keyed",
      uniqueKey: "id",
      columns: [
        { name: "id", type: "string" },
        { name: "score", type: "number" },
      ],
    });
    await database.insertBatch("streamed_keyed", {
      columns: {
        id: Array.from({ length: 600 }, (_, index) => `key-${String(index)}`),
        score: Array.from({ length: 600 }, (_, index) => index),
      },
    });
    await database.upsertBatch("streamed_keyed", {
      columns: { id: ["key-1", "key-2"], score: [1_000, 2_000] },
    });
    await database.deleteBatch("streamed_keyed", { keys: ["key-0"] });

    const result = await database.query(
      "SELECT COUNT(*) AS rows, SUM(score) AS total FROM streamed_keyed",
      { executionMemoryBudgetBytes: 512_000 },
    );
    const expectedTotal = (599 * 600) / 2 - 1 - 2 + 1_000 + 2_000;
    expect(result.rows).toEqual([{ rows: 599, total: expectedTotal }]);
    store.close();
  });
}

for (const implementation of implementations()) {
  it(`${implementation.name} leases spill owners during execution and leaves no temp state behind`, async () => {
    const store = await implementation.create();
    let nowMs = Date.parse("2026-01-01T00:00:00.000Z");
    const database = new MinnowDatabase(store, {
      rowsPerBlock: 256,
      compression: "raw",
      now: () => new Date((nowMs += 1)),
      spillOwnerLeaseMs: 10,
    });
    await database.createTable({
      name: "leased_spill_rows",
      columns: [
        { name: "id", type: "number" },
        { name: "bucket", type: "number" },
      ],
    });
    const rowCount = 5_000;
    await database.insertBatch("leased_spill_rows", {
      columns: {
        id: Array.from({ length: rowCount }, (_, index) => index),
        bucket: Array.from({ length: rowCount }, (_, index) => index % 11),
      },
    });

    const result = await database.query(
      "SELECT id, bucket FROM leased_spill_rows ORDER BY bucket, id DESC LIMIT 7",
      { executionMemoryBudgetBytes: 150_000, spillPageRows: 64 },
    );
    expect(result.rows).toHaveLength(7);
    expect(await store.listTempOwnerIdsPage(null, 16)).toEqual({
      records: [],
      nextCursor: null,
    });
    store.close();
  });
}

for (const implementation of implementations()) {
  it(`${implementation.name} reclaims abandoned spill owners only after their lease expires`, async () => {
    const store = await implementation.create();
    let nowMs = Date.parse("2026-01-01T00:00:00.000Z");
    const database = new MinnowDatabase(store, { now: () => new Date(nowMs) });

    await store.createTempOwner({
      ownerId: "abandoned-query",
      createdAt: "2026-01-01T00:00:00.000Z",
      expiresAt: "2026-01-01T00:01:00.000Z",
      revision: 0,
    });
    await store.putTempRunPage({
      ownerId: "abandoned-query",
      runId: "run-1",
      pageIndex: 0,
      bytes: Uint8Array.of(1, 2),
    });
    await store.createTempOwner({
      ownerId: "live-query",
      createdAt: "2026-01-01T00:00:00.000Z",
      expiresAt: "2026-01-01T01:00:00.000Z",
      revision: 0,
    });
    await store.putTempRunPage({
      ownerId: "live-query",
      runId: "run-1",
      pageIndex: 0,
      bytes: Uint8Array.of(3),
    });
    const early = await database.cleanupQuerySpill();
    expect(early).toEqual({ ownersExamined: 2, ownersReclaimed: 0, ownersRetained: 2 });
    expect(await store.getTempRunPage("abandoned-query", "run-1", 0)).toEqual(Uint8Array.of(1, 2));

    nowMs = Date.parse("2026-01-01T00:02:00.000Z");
    const later = await database.cleanupQuerySpill({ maxOwners: 8 });
    expect(later).toEqual({ ownersExamined: 2, ownersReclaimed: 1, ownersRetained: 1 });
    expect(await store.getTempOwner("abandoned-query")).toBeUndefined();
    expect(await store.getTempRunPage("abandoned-query", "run-1", 0)).toBeUndefined();
    expect(await store.getTempRunPage("live-query", "run-1", 0)).toEqual(Uint8Array.of(3));
    expect(await store.getTempOwner("live-query")).toMatchObject({ revision: 0 });
    store.close();
  });
}

for (const implementation of implementations()) {
  it(`${implementation.name} collection sweeps crashed leases, spill owners, and aborted journals`, async () => {
    const store = await implementation.create();
    const nowMs = Date.parse("2026-01-01T00:02:00.000Z");
    const database = new MinnowDatabase(store, {
      autoCollect: false,
      now: () => new Date(nowMs),
    });
    const manager = new TransactionManager(store, {
      createId: () => "abandoned-transaction",
      now: () => new Date(nowMs),
    });
    const transaction = await manager.begin();
    await transaction.stageBlock("abandoned-block", Uint8Array.of(1, 2, 3));
    await transaction.abort();
    await store.createLease({
      id: "crashed-reader",
      kind: "reader",
      manifestVersion: null,
      ownerId: "dead-tab",
      createdAt: "2026-01-01T00:00:00.000Z",
      expiresAt: "2026-01-01T00:01:00.000Z",
      revision: 0,
    });
    await store.createTempOwner({
      ownerId: "crashed-query",
      createdAt: "2026-01-01T00:00:00.000Z",
      expiresAt: "2026-01-01T00:01:00.000Z",
      revision: 0,
    });
    await store.putTempRunPage({
      ownerId: "crashed-query",
      runId: "run",
      pageIndex: 0,
      bytes: Uint8Array.of(4),
    });

    const first = await database.collectGarbage();
    expect(await store.getBlock("abandoned-block")).toBeUndefined();
    expect(await store.getLease("crashed-reader")).toBeUndefined();
    expect(await store.getTempOwner("crashed-query")).toBeUndefined();
    expect(await store.getTempRunPage("crashed-query", "run", 0)).toBeUndefined();
    // The journal was still a provenance root when the first pass planned its block. Once the
    // artifact is gone, the next pass is allowed to remove the terminal record itself.
    const second = await database.collectGarbage();
    expect(first.reclaimedTransactionCount + second.reclaimedTransactionCount).toBe(1);
    expect(await store.getTransaction("abandoned-transaction")).toBeUndefined();
    store.close();
  });

  it(`${implementation.name} reclaims segment and transaction metadata after diagnostic compaction history expires`, async () => {
    const store = await implementation.create();
    const database = new MinnowDatabase(store, { autoCompact: false, autoCollect: false });
    await database.createTable({
      name: "gc_transaction_records",
      columns: [{ name: "value", type: "number" }],
    });
    for (let value = 0; value < 6; value += 1) {
      await database.insert("gc_transaction_records", { value });
    }
    const sourceRecords = await Promise.all(
      (await allVisibleSegments(database, "gc_transaction_records")).map((segment) =>
        store.getSegment(segment.id),
      ),
    );
    const sourceTransactionIds = sourceRecords.map((segment) => {
      if (segment === undefined) throw new Error("Expected a source segment record");
      return segment.transactionId;
    });
    expect(new Set(sourceTransactionIds).size).toBe(6);

    const compacted = await database.compactTable("gc_transaction_records", {
      outputCompression: "raw",
    });
    expect(compacted.compacted).toBe(true);
    const outputSummary = (await allVisibleSegments(database, "gc_transaction_records"))[0];
    const output =
      outputSummary === undefined ? undefined : await store.getSegment(outputSummary.id);
    if (output === undefined) throw new Error("Expected a folded output segment");

    // Segment discovery must not depend on terminal compaction records. Production retains only
    // a bounded diagnostic tail, which can expire long before an old metadata backlog is swept.
    for (const job of await store.listCompactionJobs()) await store.removeCompactionJob(job.id);

    // A store may reclaim segments and then their now-unreferenced owners atomically in one
    // ordered job, or retain the owners until the next bounded plan. Both passes together must
    // reclaim the complete source history.
    const firstCollection = await database.collectGarbage();
    const secondCollection = await database.collectGarbage();
    expect(
      firstCollection.reclaimedTransactionCount + secondCollection.reclaimedTransactionCount,
    ).toBeGreaterThanOrEqual(sourceTransactionIds.length);
    for (const id of sourceTransactionIds) expect(await store.getTransaction(id)).toBeUndefined();
    expect(await store.getTransaction(output.transactionId)).toMatchObject({ status: "committed" });
    expect(await segmentRecords(store)).toEqual([output]);
    expect(await transactionRecords(store)).toEqual([
      expect.objectContaining({ id: output.transactionId, status: "committed" }),
    ]);
    expect(await database.readTable("gc_transaction_records")).toEqual(
      Array.from({ length: 6 }, (_, value) => ({ value })),
    );
    store.close();
  });

  it(`${implementation.name} reclaims cancelled compaction output without changing current rows`, async () => {
    const store = await implementation.create();
    const database = new MinnowDatabase(store);
    await database.createTable({
      name: "gc_cancelled_events",
      columns: [{ name: "value", type: "number" }],
    });
    for (let value = 1; value <= 4; value += 1) {
      await database.insert("gc_cancelled_events", { value });
    }

    const progress = await database.compactTableStep("gc_cancelled_events", {
      maxBlocks: 1,
      targetBlockBytes: 9,
      outputCompression: "raw",
    });
    if (progress.jobId === null) throw new Error("Expected a cancellable compaction job");
    const partial = await store.getCompactionJob(progress.jobId);
    const outputBlockId = partial?.outputBlockIds[0];
    if (outputBlockId === undefined) throw new Error("Expected a partial compaction output");
    const outputBytes = await store.getBlock(outputBlockId);
    if (outputBytes === undefined) throw new Error("Expected persisted compaction output bytes");
    await database.cancelCompactionJob(progress.jobId);

    const result = await database.collectGarbage({ maxItemsPerStep: 1 });

    expect(result).toMatchObject({
      reclaimedBlockCount: 1,
      physicallyReclaimedBytes: outputBytes.byteLength,
    });
    expect(await store.getBlock(outputBlockId)).toBeUndefined();
    expect(await database.readTable("gc_cancelled_events")).toEqual([
      { value: 1 },
      { value: 2 },
      { value: 3 },
      { value: 4 },
    ]);
    store.close();
  });

  it(`${implementation.name} cancels an active fold before dropping its table`, async () => {
    const store = await implementation.create();
    const database = new MinnowDatabase(store, { autoCollect: false });
    await database.createTable({
      name: "drop_during_fold",
      columns: [{ name: "value", type: "string" }],
    });
    for (let value = 0; value < 8; value += 1) {
      await database.insert("drop_during_fold", { value: `value-${String(value).repeat(20)}` });
    }
    const progress = await database.compactTableStep("drop_during_fold", {
      maxBlocks: 1,
      targetBlockBytes: 32,
      outputCompression: "raw",
    });
    if (progress.jobId === null || progress.result !== null) {
      throw new Error("Expected a partially written compaction");
    }
    const active = await store.getCompactionJob(progress.jobId);
    if (active?.transactionId === null || active?.transactionId === undefined) {
      throw new Error("Expected the fold's active transaction");
    }
    const outputIds = [...active.outputBlockIds];

    expect(await database.dropTable("drop_during_fold")).toBe(true);
    expect(await store.getCompactionJob(progress.jobId)).toMatchObject({ state: "cancelled" });
    expect(await store.getTransaction(active.transactionId)).toMatchObject({ status: "aborted" });
    await database.collectGarbage();
    await database.collectGarbage();
    for (const id of outputIds) expect(await store.getBlock(id)).toBeUndefined();
    expect(await store.getTransaction(active.transactionId)).toBeUndefined();
    expect(await store.getTableByName("drop_during_fold")).toBeUndefined();
    store.close();
  });

  it(`${implementation.name} caps each paged garbage-collection plan`, async () => {
    const store = await implementation.create();
    const database = new MinnowDatabase(store);
    await database.createTable({
      name: "gc_paged_candidates",
      columns: [{ name: "value", type: "number" }],
    });
    for (let value = 1; value <= 6; value += 1) {
      await database.insert("gc_paged_candidates", { value });
    }
    const compaction = await database.compactTableStep("gc_paged_candidates", {
      maxBlocks: 3,
      targetBlockBytes: 9,
      outputCompression: "raw",
    });
    if (compaction.jobId === null) throw new Error("Expected a paged GC compaction job");
    await database.cancelCompactionJob(compaction.jobId);

    const progress = await database.collectGarbageStep({ maxItems: 1, maxPlanningItems: 2 });
    const job = await store.getGarbageCollectionJob(progress.jobId);
    if (job === undefined) throw new Error("Expected a paged garbage-collection job");
    expect(job.candidateBlockIds.length + job.candidateSegmentIds.length).toBeLessThanOrEqual(2);
    expect(
      job.candidateManifestVersions.length +
        job.candidateBlockIds.length +
        job.candidateSegmentIds.length,
    ).toBeGreaterThan(0);
    store.close();
  });

  it(`${implementation.name} bounds failed compaction history`, async () => {
    const store = await implementation.create();
    const database = new MinnowDatabase(store, { autoCollect: false });
    await database.createTable({
      name: "bounded_failed_jobs",
      columns: [{ name: "value", type: "number" }],
    });
    await database.insert("bounded_failed_jobs", { value: 1 });
    const table = await store.getTableByName("bounded_failed_jobs");
    const version = await store.getCurrentManifestVersion();
    if (table === undefined || version === null) throw new Error("Expected a source snapshot");
    const source = (await tableSegmentRecords(store, table.id))[0];
    if (source === undefined) throw new Error("Expected a source segment");
    const sourceBlockIds = Object.values(source.columnBlockIds).flat();
    const sourceStoredBytes = (
      await Promise.all(sourceBlockIds.map((id) => store.getBlock(id)))
    ).reduce((total, bytes) => total + (bytes?.byteLength ?? 0), 0);
    const baseId = `compaction/${table.id}/manifest/${String(version)}`;
    for (let attempt = 0; attempt < 20; attempt += 1) {
      const timestamp = new Date(Date.UTC(2026, 0, 1, 0, 0, attempt)).toISOString();
      await store.createCompactionJob({
        id: attempt === 0 ? baseId : `${baseId}/retry/${String(attempt)}`,
        tableId: table.id,
        sourceManifestVersion: version,
        sourceSegmentIds: [source.id],
        sourceBlockIds,
        outputBlockIds: [],
        cursor: { sourceSegmentIndex: 0, sourceBlockIndex: 0 },
        processedRows: 0,
        sourceStoredBytes,
        outputStoredBytes: 10,
        logicalBytes: 100,
        rewritePlan: { kind: "copy-v1" },
        outputCursor: null,
        memoryBudgetBytes: 0,
        minimumMemoryBytes: 0,
        level0SourceStoredBytes: sourceStoredBytes,
        anchorSourceStoredBytes: 0,
        peakWorkingBytes: 0,
        outputLogicalBytes: 100,
        targetLevel: 1,
        state: "cancelled",
        transactionId: null,
        outputSegmentId: null,
        publishedVersion: null,
        revision: 0,
        createdAt: timestamp,
        updatedAt: timestamp,
      });
    }

    await database.collectGarbage();
    const retained = await store.listCompactionJobs(table.id);
    expect(retained).toHaveLength(8);
    expect(retained.at(-1)).toMatchObject({
      id: `${baseId}/retry/19`,
      outputStoredBytes: 10,
    });
    store.close();
  });

  it(`${implementation.name} keeps compacted history while leased and reclaims exact bytes after release`, async () => {
    const store = await implementation.create();
    const database = new MinnowDatabase(store);
    await database.createTable({
      name: "gc_leased_events",
      columns: [{ name: "value", type: "number" }],
    });
    await database.insert("gc_leased_events", { value: 1 });
    const source = await database.insert("gc_leased_events", { value: 2 });
    const sourceManifest = await store.getManifest(source.version);
    if (sourceManifest === undefined) throw new Error("Expected a source manifest");
    const sourceManifestBlockIds = await manifestBlockIdsAt(store, sourceManifest.version);
    const sourceBytes = await Promise.all(
      sourceManifestBlockIds.map(async (id) => {
        const bytes = await store.getBlock(id);
        if (bytes === undefined) throw new Error(`Expected source block ${id}`);
        return bytes;
      }),
    );
    const expectedReclaimedBytes = sourceBytes.reduce(
      (total, bytes) => total + bytes.byteLength,
      0,
    );
    const lease = await new TransactionManager(store, {
      createId: () => "gc-history-lease",
    }).openLeasedSnapshot({
      ownerId: "tab-1",
      ttlMs: 60_000,
      version: source.version,
    });
    await database.compactTable("gc_leased_events", {
      targetBlockBytes: 9,
      outputCompression: "raw",
    });

    const retained = await database.collectGarbage({ maxItemsPerStep: 2 });
    expect(retained.retainedManifestCount).toBeGreaterThan(0);
    expect(retained.retainedBlockCount).toBeGreaterThanOrEqual(sourceManifestBlockIds.length);
    for (const [index, id] of sourceManifestBlockIds.entries()) {
      expect(await lease.getBlock(id)).toEqual(sourceBytes[index]);
    }

    await lease.release();
    const reclaimed = await database.collectGarbage({ maxItemsPerStep: 2 });
    expect(reclaimed).toMatchObject({
      reclaimedBlockCount: sourceManifestBlockIds.length,
      physicallyReclaimedBytes: expectedReclaimedBytes,
    });
    for (const id of sourceManifestBlockIds) {
      expect(await store.getBlock(id)).toBeUndefined();
    }
    expect(await database.readTable("gc_leased_events")).toEqual([{ value: 1 }, { value: 2 }]);
    store.close();
  });

  it(`${implementation.name} holds an internal read lease across concurrent compaction and collection`, async () => {
    const store = await implementation.create();
    const writer = new MinnowDatabase(store);
    await writer.createTable({
      name: "gc_read_race_events",
      columns: [{ name: "value", type: "number" }],
    });
    await writer.insert("gc_read_race_events", { value: 1 });
    await writer.insert("gc_read_race_events", { value: 2 });
    const sourceManifest = await store.getCurrentManifest();
    if (sourceManifest === undefined) throw new Error("Expected a source manifest");
    const sourceManifestBlockIds = await manifestBlockIdsAt(store, sourceManifest.version);

    let signalReadStarted: (() => void) | undefined;
    const readStarted = new Promise<void>((resolve) => {
      signalReadStarted = resolve;
    });
    let releaseRead: (() => void) | undefined;
    const readRelease = new Promise<void>((resolve) => {
      releaseRead = resolve;
    });
    let pauseFirstRead = true;
    const readerStore = new FaultInjectingBlockStore(store, async (point) => {
      if (point !== "beforeBlockRead" || !pauseFirstRead) return;
      pauseFirstRead = false;
      signalReadStarted?.();
      await readRelease;
    });
    const readPromise = new MinnowDatabase(readerStore).readTable("gc_read_race_events");
    await readStarted;

    try {
      expect(await store.listLeases()).toHaveLength(1);
      await writer.compactTable("gc_read_race_events", {
        targetBlockBytes: 9,
        outputCompression: "raw",
      });
      await writer.collectGarbage({ maxItemsPerStep: 1 });
      for (const id of sourceManifestBlockIds) {
        expect(await store.getBlock(id)).toBeDefined();
      }
    } finally {
      releaseRead?.();
    }

    expect(await readPromise).toEqual([{ value: 1 }, { value: 2 }]);
    expect(await store.listLeases()).toEqual([]);
    await writer.collectGarbage({ maxItemsPerStep: 1 });
    for (const id of sourceManifestBlockIds) {
      expect(await store.getBlock(id)).toBeUndefined();
    }
    store.close();
  });
}

for (const implementation of recoveryImplementations()) {
  it(`${implementation.name} resumes a bounded garbage-collection job after reopen`, async () => {
    const harness = await implementation.create();
    let store = harness.store;
    let database = new MinnowDatabase(store);
    await database.createTable({
      name: "gc_resume_events",
      columns: [{ name: "value", type: "number" }],
    });
    for (let value = 1; value <= 3; value += 1) {
      await database.insert("gc_resume_events", { value });
    }
    const compaction = await database.compactTableStep("gc_resume_events", {
      maxBlocks: 1,
      targetBlockBytes: 9,
      outputCompression: "raw",
    });
    if (compaction.jobId === null) throw new Error("Expected a cancellable compaction job");
    const partial = await store.getCompactionJob(compaction.jobId);
    const outputBlockId = partial?.outputBlockIds[0];
    if (outputBlockId === undefined) throw new Error("Expected a partial compaction output");
    const outputBytes = await store.getBlock(outputBlockId);
    if (outputBytes === undefined) throw new Error("Expected partial output bytes");
    await database.cancelCompactionJob(compaction.jobId);

    let progress = await database.collectGarbageStep({ maxItems: 1 });
    expect(progress).toMatchObject({
      state: "planned",
      examinedManifestCount: 0,
      examinedSegmentCount: 0,
      examinedBlockCount: 0,
      result: null,
    });
    const jobId = progress.jobId;

    store = await harness.reopen();
    database = new MinnowDatabase(store);
    expect((await database.listGarbageCollectionJobs()).map((job) => job.id)).toContain(jobId);
    while (progress.result === null) {
      progress = await database.resumeGarbageCollectionJob(jobId, { maxItems: 1 });
    }
    expect(progress.result).toMatchObject({
      jobId,
      reclaimedBlockCount: 1,
      physicallyReclaimedBytes: outputBytes.byteLength,
    });
    expect(await store.getBlock(outputBlockId)).toBeUndefined();
    expect(await database.readTable("gc_resume_events")).toEqual([
      { value: 1 },
      { value: 2 },
      { value: 3 },
    ]);
    store.close();
  });
}

it("executes the extended SQL surface through the stored query path", async () => {
  const store = new MemoryBlockStore();
  const database = new MinnowDatabase(store);
  await database.createTable({
    name: "orders",
    columns: [
      { name: "region", type: "string", nullable: true },
      { name: "amount", type: "number", nullable: true },
    ],
  });
  await database.insertBatch("orders", {
    columns: {
      region: ["west", "west", "east", null],
      amount: [10, 3, 8, 5],
    },
  });
  await database.createTable({
    name: "targets",
    columns: [
      { name: "region", type: "string" },
      { name: "goal", type: "number" },
    ],
  });
  await database.insertBatch("targets", {
    columns: { region: ["west", "south"], goal: [9, 1] },
  });

  expect(
    await database.query(
      "SELECT region, amount FROM orders WHERE amount > 9 OR region = 'east' ORDER BY amount",
    ),
  ).toEqual({
    columns: ["region", "amount"],
    columnDomains: [null, null],
    rows: [
      { region: "east", amount: 8 },
      { region: "west", amount: 10 },
    ],
  });
  expect(
    await database.query(
      "SELECT region FROM orders WHERE region LIKE 'w%' AND EXISTS (SELECT region FROM targets WHERE goal > 5)",
    ),
  ).toEqual({
    columns: ["region"],
    columnDomains: [null],
    rows: [{ region: "west" }, { region: "west" }],
  });
  expect(
    await database.query(
      "SELECT region, amount, CASE WHEN amount >= 8 THEN 'high' ELSE 'low' END AS band FROM orders ORDER BY amount LIMIT 2 OFFSET 1",
    ),
  ).toEqual({
    columns: ["region", "amount", "band"],
    columnDomains: [null, null, null],
    rows: [
      { region: null, amount: 5, band: "low" },
      { region: "east", amount: 8, band: "high" },
    ],
  });
  expect(
    await database.query("SELECT region FROM orders INTERSECT SELECT region FROM targets"),
  ).toEqual({ columns: ["region"], columnDomains: [null], rows: [{ region: "west" }] });
  expect(
    await database.query(
      "SELECT o.region, t.goal FROM orders o JOIN targets t ON t.region = o.region AND t.goal > o.amount",
    ),
  ).toEqual({
    columns: ["region", "goal"],
    columnDomains: [null, null],
    rows: [{ region: "west", goal: 9 }],
  });
  expect(
    await database.query(
      "WITH RECURSIVE n AS (SELECT MIN(amount) AS v FROM orders UNION ALL SELECT v * 2 FROM n WHERE v < 20) SELECT v FROM n ORDER BY v",
    ),
  ).toEqual({
    columns: ["v"],
    columnDomains: [null],
    rows: [{ v: 3 }, { v: 6 }, { v: 12 }, { v: 24 }],
  });
  expect(
    await database.query(
      "SELECT region, amount, SUM(amount) OVER (PARTITION BY region) AS total FROM orders WHERE region = 'west' ORDER BY amount",
    ),
  ).toEqual({
    columns: ["region", "amount", "total"],
    columnDomains: [null, null, null],
    rows: [
      { region: "west", amount: 3, total: 13 },
      { region: "west", amount: 10, total: 13 },
    ],
  });
  store.close();
});

describe("prepared-input cache and shared read lease", () => {
  class LeaseCountingStore extends CountingMemoryBlockStore {
    leaseCreates = 0;
    leaseMoves = 0;
    catalogStateCalls = 0;
    probeCalls = 0;

    override getCatalogProbe(): ReturnType<MemoryBlockStore["getCatalogProbe"]> {
      this.probeCalls += 1;
      return super.getCatalogProbe();
    }

    override async createLease(
      record: Parameters<MemoryBlockStore["createLease"]>[0],
    ): Promise<void> {
      this.leaseCreates += 1;
      return super.createLease(record);
    }

    override async moveLease(
      ...args: Parameters<NonNullable<MemoryBlockStore["moveLease"]>>
    ): ReturnType<NonNullable<MemoryBlockStore["moveLease"]>> {
      this.leaseMoves += 1;
      return super.moveLease(...args);
    }

    override async listTableSegmentPage(
      tableId: Parameters<MemoryBlockStore["listTableSegmentPage"]>[0],
      afterId: Parameters<MemoryBlockStore["listTableSegmentPage"]>[1],
      limit: Parameters<MemoryBlockStore["listTableSegmentPage"]>[2],
    ): ReturnType<MemoryBlockStore["listTableSegmentPage"]> {
      this.catalogStateCalls += 1;
      return super.listTableSegmentPage(tableId, afterId, limit);
    }
  }

  class SegmentIdReadCountingStore extends LeaseCountingStore {
    segmentIdReads = 0;
    segmentTableIdReads = 0;

    override async listTableSegmentPage(
      tableId: Parameters<MemoryBlockStore["listTableSegmentPage"]>[0],
      afterId: Parameters<MemoryBlockStore["listTableSegmentPage"]>[1],
      limit: Parameters<MemoryBlockStore["listTableSegmentPage"]>[2],
    ): ReturnType<MemoryBlockStore["listTableSegmentPage"]> {
      const page = await super.listTableSegmentPage(tableId, afterId, limit);
      return {
        ...page,
        records: page.records.map((record) => {
          const id = record.id;
          const tableId = record.tableId;
          const counted = { ...record };
          Object.defineProperty(counted, "id", {
            enumerable: true,
            configurable: true,
            get: () => {
              this.segmentIdReads += 1;
              return id;
            },
          });
          Object.defineProperty(counted, "tableId", {
            enumerable: true,
            configurable: true,
            get: () => {
              this.segmentTableIdReads += 1;
              return tableId;
            },
          });
          return counted;
        }),
      };
    }
  }

  async function seededStore(
    options: ConstructorParameters<typeof MinnowDatabase>[1] = {},
  ): Promise<{ store: LeaseCountingStore; database: MinnowDatabase }> {
    const store = new LeaseCountingStore();
    const database = new MinnowDatabase(store, { rowsPerBlock: 4, ...options });
    await database.createTable({
      name: "orders",
      columns: [
        { name: "region", type: "string" },
        { name: "amount", type: "number" },
      ],
    });
    await database.insertBatch("orders", {
      columns: {
        region: ["west", "east", "west", "north", "east", "west"],
        amount: [3, 8, 10, 4, 6, 2],
      },
    });
    return { store, database };
  }

  it("opens ordinary queries without cloning or scanning the database manifest", async () => {
    const { store, database } = await seededStore();
    store.manifestGetCalls = 0;
    store.currentManifestGetCalls = 0;
    store.manifestMembershipIds = 0;

    await Promise.all(
      Array.from({ length: 16 }, () =>
        database.query("SELECT amount FROM orders WHERE region = 'west'", { memoize: false }),
      ),
    );

    expect(store.manifestGetCalls).toBe(0);
    expect(store.currentManifestGetCalls).toBe(0);
    expect(store.manifestMembershipIds).toBe(64);
    await database.close();
  });

  it("skips block reads and lease writes on repeated queries at one version", async () => {
    const { store, database } = await seededStore();
    const first = await database.query(
      "SELECT region, SUM(amount) AS total FROM orders GROUP BY region ORDER BY region",
    );
    expect(store.blockReadCalls).toBeGreaterThan(0);
    // The atomic catalog state carries the exact manifest block set. Pinning that version must
    // validate the lease, but must not resolve the same manifest a second time first.
    expect(store.manifestGetCalls).toBe(0);
    const blockReadsAfterFirst = store.blockReadCalls;
    const leasesAfterFirst = store.leaseCreates;
    const second = await database.query(
      "SELECT region, SUM(amount) AS total FROM orders GROUP BY region ORDER BY region",
    );
    expect(second.rows).toEqual(first.rows);
    expect(store.blockReadCalls).toBe(blockReadsAfterFirst);
    expect(store.leaseCreates).toBe(leasesAfterFirst);
    expect(store.catalogStateCalls).toBeGreaterThan(0);
  });

  it("shares cached vectors across different statements over the same columns", async () => {
    const { store, database } = await seededStore();
    await database.query("SELECT region, amount FROM orders ORDER BY amount");
    const blockReadsAfterFirst = store.blockReadCalls;
    const grouped = await database.query(
      "SELECT region, COUNT(*) AS orders FROM orders GROUP BY region ORDER BY region",
    );
    expect(grouped.rows).toEqual([
      { region: "east", orders: 2 },
      { region: "north", orders: 1 },
      { region: "west", orders: 3 },
    ]);
    expect(store.blockReadCalls).toBe(blockReadsAfterFirst);
  });

  it("serves fresh rows and re-pins the shared lease after a write changes the segment set", async () => {
    const { store, database } = await seededStore();
    const before = await database.query("SELECT COUNT(*) AS orders FROM orders");
    expect(before.rows).toEqual([{ orders: 6 }]);
    const leasesBeforeWrite = store.leaseCreates;
    const movesBeforeWrite = store.leaseMoves;
    const written = await database.insertBatch("orders", {
      columns: { region: ["south"], amount: [11] },
    });
    const after = await database.query("SELECT COUNT(*) AS orders FROM orders");
    expect(after.rows).toEqual([{ orders: 7 }]);
    // No reader was left on the old version, so the one lease record moved to the new one in
    // place: one storage write, no second record, nothing to remove later.
    expect(store.leaseMoves).toBe(movesBeforeWrite + 1);
    expect(store.leaseCreates).toBe(leasesBeforeWrite);
    const leases = await store.listLeases();
    expect(leases).toHaveLength(1);
    expect(leases[0]?.manifestVersion).toBe(written.version);
    const totals = await database.query(
      "SELECT region, SUM(amount) AS total FROM orders WHERE region = 'south' GROUP BY region",
    );
    expect(totals.rows).toEqual([{ region: "south", total: 11 }]);
  });

  it("re-reads blocks when the cache is disabled", async () => {
    const { store, database } = await seededStore({ bufferPoolBytes: 0 });
    await database.query("SELECT SUM(amount) AS total FROM orders");
    const blockReadsAfterFirst = store.blockReadCalls;
    await database.query("SELECT SUM(amount) AS total FROM orders");
    expect(store.blockReadCalls).toBeGreaterThan(blockReadsAfterFirst);
  });

  it("keeps results exact under a cache too small to hold every column", async () => {
    const { database } = await seededStore({ bufferPoolBytes: 96 });
    for (let round = 0; round < 3; round += 1) {
      const result = await database.query(
        "SELECT region, SUM(amount) AS total FROM orders GROUP BY region ORDER BY region",
      );
      expect(result.rows).toEqual([
        { region: "east", total: 14 },
        { region: "north", total: 4 },
        { region: "west", total: 15 },
      ]);
    }
  });

  it("charges decoded gzip payloads by retained memory instead of compressed bytes", async () => {
    const store = new LeaseCountingStore();
    const database = new MinnowDatabase(store, {
      autoCollect: false,
      autoCompact: false,
      bufferPoolBytes: 128 * 1024,
      compression: "gzip",
      rowsPerBlock: 1_024,
    });
    await database.createTable({
      name: "compressed_cache",
      columns: [{ name: "value", type: "string" }],
    });
    const values = Array.from(
      { length: 1_024 },
      (_, index) => `${"x".repeat(1_024)}${String(index).padStart(4, "0")}`,
    );
    await database.insertBatch("compressed_cache", { columns: { value: values } });

    const query = "SELECT MIN(value) AS first FROM compressed_cache";
    await database.query(query, { memoize: false });
    const firstReadCount = store.blockReadCalls;
    expect(firstReadCount).toBeGreaterThan(0);
    expect(database.bufferPoolStats().usedBytes).toBeLessThanOrEqual(128 * 1024);

    // The decoded physical payload and its string vector both exceed the pool. A second scan
    // must re-read the block rather than retaining a multi-megabyte decompression under the
    // small stored gzip length.
    await database.query(query, { memoize: false });
    expect(store.blockReadCalls).toBeGreaterThan(firstReadCount);
    expect(database.bufferPoolStats().usedBytes).toBeLessThanOrEqual(128 * 1024);
    await database.close();
  });

  it("rejects invalid cache sizes", () => {
    const store = new MemoryBlockStore();
    expect(() => new MinnowDatabase(store, { bufferPoolBytes: -1 })).toThrow(RangeError);
    expect(() => new MinnowDatabase(store, { bufferPoolBytes: 1.5 })).toThrow(RangeError);
  });

  it("keeps mutation-replay reads uncached and exact", async () => {
    const store = new LeaseCountingStore();
    const database = new MinnowDatabase(store, { rowsPerBlock: 4 });
    await database.createTable({
      name: "inventory",
      uniqueKey: "sku",
      columns: [
        { name: "sku", type: "string" },
        { name: "count", type: "number" },
      ],
    });
    await database.insertBatch("inventory", {
      columns: { sku: ["a", "b", "c"], count: [1, 2, 3] },
    });
    await database.upsertBatch("inventory", {
      columns: { sku: ["b", "d"], count: [20, 4] },
    });
    for (let round = 0; round < 2; round += 1) {
      const result = await database.query("SELECT sku, count FROM inventory ORDER BY sku");
      expect(result.rows).toEqual([
        { sku: "a", count: 1 },
        { sku: "b", count: 20 },
        { sku: "c", count: 3 },
        { sku: "d", count: 4 },
      ]);
    }
  });

  it("collapses repeated catalog reads into probes while the epoch holds", async () => {
    const { store, database } = await seededStore();
    await database.query("SELECT COUNT(*) AS orders FROM orders");
    const catalogReadsAfterFirst = store.catalogStateCalls;
    for (let round = 0; round < 5; round += 1) {
      await database.query("SELECT COUNT(*) AS orders FROM orders");
      await database.query("SELECT region, SUM(amount) AS total FROM orders GROUP BY region");
    }
    // Every repeat is served by the (version, epoch) probe plus the cached state: both
    // statements read the same table set, so no further catalog read happens at all until
    // something actually changes.
    expect(store.catalogStateCalls).toBe(catalogReadsAfterFirst);
    await database.insertBatch("orders", { columns: { region: ["south"], amount: [11] } });
    const after = await database.query("SELECT COUNT(*) AS orders FROM orders");
    expect(after.rows).toEqual([{ orders: 7 }]);
    expect(store.catalogStateCalls).toBeGreaterThan(catalogReadsAfterFirst);
  });

  it("keeps warm result-cache identity work independent of visible segment count", async () => {
    const store = new SegmentIdReadCountingStore();
    const database = new MinnowDatabase(store, {
      autoCollect: false,
      autoCompact: false,
      rowsPerBlock: 1,
    });
    await database.createTable({
      name: "many_segments",
      columns: [{ name: "value", type: "number" }],
    });
    for (let value = 0; value < 96; value += 1) {
      await database.insert("many_segments", { value });
    }
    const sql = "SELECT SUM(value) AS total FROM many_segments";
    expect((await database.query(sql)).rows).toEqual([{ total: 4_560 }]);
    const hits = database.bufferPoolStats().hits;
    store.segmentIdReads = 0;

    expect((await database.query(sql)).rows).toEqual([{ total: 4_560 }]);
    expect(database.bufferPoolStats().hits).toBeGreaterThan(hits);
    expect(store.segmentIdReads).toBe(0);
    await database.close();
  });

  it("keeps uncached query preparation independent of historical segment count", async () => {
    const store = new SegmentIdReadCountingStore();
    const database = new MinnowDatabase(store, {
      autoCollect: false,
      autoCompact: false,
      rowsPerBlock: 1,
    });
    await database.createTable({
      name: "historical_segments",
      columns: [{ name: "value", type: "number" }],
    });
    for (let value = 0; value < 96; value += 1) {
      await database.insert("historical_segments", { value });
    }
    expect((await database.compactTable("historical_segments")).compacted).toBe(true);
    const sql = "SELECT COUNT(*) AS total FROM historical_segments";
    expect((await database.query(sql, { memoize: false })).rows).toEqual([{ total: 96 }]);
    store.segmentTableIdReads = 0;

    expect((await database.query(sql, { memoize: false })).rows).toEqual([{ total: 96 }]);
    expect(store.segmentTableIdReads).toBe(0);

    // The derived maps live exactly as long as the epoch-gated state: a later commit must build
    // a fresh view and serve the new row, then repeated preparation is O(current visible shape)
    // again instead of O(all historical records).
    await database.insert("historical_segments", { value: 96 });
    store.segmentTableIdReads = 0;
    expect((await database.query(sql, { memoize: false })).rows).toEqual([{ total: 97 }]);
    store.segmentTableIdReads = 0;
    expect((await database.query(sql, { memoize: false })).rows).toEqual([{ total: 97 }]);
    expect(store.segmentTableIdReads).toBe(0);
    await database.close();
  });

  it("reports buffer pool stats and compacts a fragmented table in the background", async () => {
    const store = new LeaseCountingStore();
    const database = new MinnowDatabase(store, { rowsPerBlock: 4 });
    await database.createTable({
      name: "fragmented",
      columns: [{ name: "value", type: "number" }],
    });
    // 50 one-row commits: past the 48-segment threshold both a commit and a streamed scan
    // react to, so the fold may already be under way before the query runs.
    for (let index = 0; index < 50; index += 1) {
      await database.insertBatch("fragmented", { columns: { value: [index] } });
    }
    await database.query("SELECT SUM(value) AS total FROM fragmented");
    const stats = database.bufferPoolStats();
    expect(stats.limitBytes).toBe(64 * 1024 * 1024);
    expect(stats.usedBytes).toBeGreaterThan(0);
    expect(stats.usedBytes).toBeLessThanOrEqual(stats.limitBytes);
    expect(stats.hits + stats.misses).toBeGreaterThan(0);
    // Compaction is fire-and-forget; poll briefly for its publish.
    let after = 50;
    for (let attempt = 0; attempt < 200 && after >= 50; attempt += 1) {
      await new Promise((resolve) => setTimeout(resolve, 10));
      after = (await allVisibleSegments(database, "fragmented")).length;
    }
    expect(after).toBeLessThan(50);
    // Results stay exact across the background rewrite.
    expect((await database.query("SELECT SUM(value) AS total FROM fragmented")).rows).toEqual([
      { total: 1225 },
    ]);

    // Opting out leaves fragmentation untouched.
    const optOutStore = new LeaseCountingStore();
    const optOut = new MinnowDatabase(optOutStore, { rowsPerBlock: 4, autoCompact: false });
    await optOut.createTable({
      name: "fragmented",
      columns: [{ name: "value", type: "number" }],
    });
    for (let index = 0; index < 50; index += 1) {
      await optOut.insertBatch("fragmented", { columns: { value: [index] } });
    }
    await optOut.query("SELECT SUM(value) AS total FROM fragmented");
    await new Promise((resolve) => setTimeout(resolve, 50));
    expect((await allVisibleSegments(optOut, "fragmented")).length).toBe(50);
  });

  it("folds a keyed table's deltas from the write path, with no read to trigger it", async () => {
    const database = new MinnowDatabase(new MemoryBlockStore(), { rowsPerBlock: 64 });
    await database.createTable({
      name: "accounts",
      uniqueKey: "id",
      columns: [
        { name: "id", type: "number" },
        { name: "balance", type: "number" },
      ],
    });
    const ids = Array.from({ length: 200 }, (_, index) => index);
    await database.insertBatch("accounts", { columns: { id: ids, balance: ids } });
    // Forty point updates and not one query: the commits alone must schedule the fold.
    for (let id = 0; id < 40; id += 1) {
      await database.updateBatch("accounts", { keys: [id], changes: { balance: [1000 + id] } });
    }
    let visible = 41;
    for (let attempt = 0; attempt < 300 && visible >= 41; attempt += 1) {
      await new Promise((resolve) => setTimeout(resolve, 10));
      visible = (await allVisibleSegments(database, "accounts")).length;
    }
    // The job planned at the 32-delta threshold folded those; at most the later few remain.
    expect(visible).toBeLessThan(20);
    expect(
      (await database.query("SELECT id, balance FROM accounts WHERE id < 40 ORDER BY id")).rows,
    ).toEqual(ids.slice(0, 40).map((id) => ({ id, balance: 1000 + id })));
    expect(
      (await database.query("SELECT SUM(balance) AS total FROM accounts WHERE id >= 40")).rows,
    ).toEqual([{ total: ids.slice(40).reduce((sum, id) => sum + id, 0) }]);
  });

  it("merges a keyed table whose row count alone once exceeded the planner budget", async () => {
    // The planner's memory used to scale with rows × columns (256 bytes a cell); 60k rows of
    // three columns modelled as 61 MiB against the 32 MiB default, so a table this size could
    // never fold its deltas. The slot-based replay scales with the deltas instead.
    const database = new MinnowDatabase(new MemoryBlockStore(), { autoCompact: false });
    await database.createTable({
      name: "wide",
      uniqueKey: "id",
      columns: [
        { name: "id", type: "number" },
        { name: "label", type: "string" },
        { name: "amount", type: "number" },
      ],
    });
    const rows = 60_000;
    const ids = Array.from({ length: rows }, (_, index) => index);
    await database.insertBatch("wide", {
      columns: {
        id: ids,
        label: ids.map((id) => `item-${String(id % 97)}`),
        amount: ids.map((id) => id % 1000),
      },
    });
    await database.updateBatch("wide", { keys: [5, 59_999], changes: { amount: [-1, -2] } });
    await database.deleteBatch("wide", { keys: [7] });
    await database.upsertBatch("wide", {
      columns: { id: [9, rows], label: ["nine", "new"], amount: [90, 1] },
    });
    const result = await database.compactTable("wide");
    expect(result.compacted).toBe(true);
    expect(result.memoryBudgetBytes).toBe(32 * 1024 * 1024);
    expect(result.rowCount).toBe(rows);
    // The fold publishes the table as bounded level-one partitions: 60k rows at the default
    // 16,384-row target is four, each a separate segment.
    const visible = await allVisibleSegments(database, "wide");
    expect(visible.length).toBe(Math.ceil(rows / 16_384));
    expect(visible.map((segment) => segment.id)).toEqual(result.outputSegmentIds);
    expect(visible.every((segment) => segment.rowCount <= 16_384)).toBe(true);
    expect(
      (
        await database.query(
          "SELECT id, label, amount FROM wide WHERE id IN (5, 7, 9, 59999, 60000) ORDER BY id",
        )
      ).rows,
    ).toEqual([
      { id: 5, label: "item-5", amount: -1 },
      { id: 9, label: "nine", amount: 90 },
      { id: 59_999, label: `item-${String(59_999 % 97)}`, amount: -2 },
      { id: 60_000, label: "new", amount: 1 },
    ]);
    expect((await database.query("SELECT COUNT(*) AS n FROM wide")).rows).toEqual([{ n: rows }]);
  });

  it("probes the catalog once per query, and memoizes typed queries like SQL", async () => {
    // Every probe is a read transaction on IndexedDB — a floor under every small query — so the
    // one read before execution is handed to the view lookup and the catalog state rather than
    // each probing again: a cold memoizable query pays two (before and after, for the memo), a
    // hit or an unmemoized query one.
    const { store, database } = await seededStore();
    const sql = "SELECT region, SUM(amount) AS total FROM orders GROUP BY region ORDER BY region";
    await database.query(sql, { memoize: false });
    let before = store.probeCalls;
    await database.query(sql, { memoize: false });
    expect(store.probeCalls - before).toBe(1);
    before = store.probeCalls;
    await database.query(sql);
    expect(store.probeCalls - before).toBe(2);
    before = store.probeCalls;
    await database.query(sql);
    expect(store.probeCalls - before).toBe(1);

    // A typed query is compiled once and run many times; it gets the same memo.
    const typed = { kind: "typed-query" as const, plan: compileQuery(sql) };
    const first = await database.run(typed);
    expect(first).toEqual([
      { region: "east", total: 14 },
      { region: "north", total: 4 },
      { region: "west", total: 15 },
    ]);
    before = store.probeCalls;
    const stateReads = store.catalogStateCalls;
    const again = await database.run(typed);
    expect(again).toEqual(first);
    expect(store.probeCalls - before).toBe(1);
    expect(store.catalogStateCalls).toBe(stateReads);
    // And stays fresh: a commit moves the epoch and the next run re-executes.
    await database.insertBatch("orders", { columns: { region: ["north"], amount: [1] } });
    expect((await database.run(typed))[1]).toEqual({ region: "north", total: 5 });
    // A run's rows are the caller's: mutating them must not poison the next hit.
    ((await database.run(typed))[0] as Record<string, unknown>).total = -1;
    expect((await database.run(typed))[0]).toEqual({ region: "east", total: 14 });
  });

  it("memoizes results under the probe and stays fresh and unpoisonable", async () => {
    const { store, database } = await seededStore();
    const sql = "SELECT region, SUM(amount) AS total FROM orders GROUP BY region ORDER BY region";
    const first = await database.query(sql);
    // Mutating a returned row must not poison later hits: every hit is a defensive copy.
    (first.rows[0] as Record<string, unknown>).total = -999;
    const second = await database.query(sql);
    expect(second.rows[0]).toEqual({ region: "east", total: 14 });
    // The memo hit serves without a fresh catalog read or lease acquisition.
    const catalogReads = store.catalogStateCalls;
    const leases = store.leaseCreates;
    await database.query(sql);
    expect(store.catalogStateCalls).toBe(catalogReads);
    expect(store.leaseCreates).toBe(leases);
    // A commit moves the epoch: the very next query recomputes, never serves the old entry.
    await database.insertBatch("orders", { columns: { region: ["east"], amount: [100] } });
    const fresh = await database.query(sql);
    expect(fresh.rows[0]).toEqual({ region: "east", total: 114 });
  });

  it("keeps memoized parameter tuples collision-free", async () => {
    const database = new MinnowDatabase(new MemoryBlockStore());
    const sql = "SELECT ? AS x, ? AS y";
    const first = ["a\u0001string:b", "c"] as const;
    const second = ["a", "b\u0001string:c"] as const;

    expect((await database.query(sql, { params: first })).rows).toEqual([
      { x: first[0], y: first[1] },
    ]);
    expect((await database.query(sql, { params: second })).rows).toEqual([
      { x: second[0], y: second[1] },
    ]);
  });

  it("preserves special JavaScript property names in projected rows", async () => {
    const database = new MinnowDatabase(new MemoryBlockStore());
    await database.createTable({
      name: "special_names",
      columns: [{ name: "__proto__", type: "string" }],
    });
    await database.insertBatch("special_names", { columns: { __proto__: ["kept"] } });

    const result = await database.query('SELECT "__proto__" FROM special_names');
    expect(result.columns).toEqual(["__proto__"]);
    expect(Object.hasOwn(result.rows[0] ?? {}, "__proto__")).toBe(true);
    expect(Reflect.get(result.rows[0] ?? {}, "__proto__")).toBe("kept");
    const memoized = await database.query('SELECT "__proto__" FROM special_names');
    expect(Object.hasOwn(memoized.rows[0] ?? {}, "__proto__")).toBe(true);
    expect(Reflect.get(memoized.rows[0] ?? {}, "__proto__")).toBe("kept");
  });

  it("serves another instance's committed writes on the very next query", async () => {
    const store = new LeaseCountingStore();
    const writer = new MinnowDatabase(store, { rowsPerBlock: 4 });
    const reader = new MinnowDatabase(store, { rowsPerBlock: 4 });
    await writer.createTable({
      name: "people",
      columns: [
        { name: "name", type: "string" },
        { name: "score", type: "number" },
      ],
    });
    await writer.insertBatch("people", {
      columns: { name: ["Ada", "Grace"], score: [10, 25] },
    });
    const before = await reader.query("SELECT COUNT(*) AS people FROM people");
    expect(before.rows).toEqual([{ people: 2 }]);
    // The reader's catalog cache is warm; the writer's commit must invalidate it through the
    // epoch alone — there is no in-process signal between the two instances.
    await writer.insertBatch("people", { columns: { name: ["Margaret"], score: [40] } });
    const after = await reader.query("SELECT COUNT(*) AS people FROM people");
    expect(after.rows).toEqual([{ people: 3 }]);
  });

  it("serves another instance's DDL on the very next query", async () => {
    const store = new LeaseCountingStore();
    const writer = new MinnowDatabase(store, { rowsPerBlock: 4 });
    const reader = new MinnowDatabase(store, { rowsPerBlock: 4 });
    await writer.createTable({
      name: "seed",
      columns: [{ name: "value", type: "number" }],
    });
    await writer.insertBatch("seed", { columns: { value: [1] } });
    // Warm the reader's cache, and prove a missing-table miss does not stick.
    expect((await reader.query("SELECT value FROM seed")).rows).toEqual([{ value: 1 }]);
    await expect(reader.query("SELECT label FROM labels")).rejects.toThrow("Unknown table: labels");
    await writer.createTable({
      name: "labels",
      columns: [{ name: "label", type: "string" }],
    });
    await writer.insertBatch("labels", { columns: { label: ["fresh"] } });
    const result = await reader.query("SELECT label FROM labels");
    expect(result.rows).toEqual([{ label: "fresh" }]);
  });
});

describe("derived-block and pruned-projection caching", () => {
  class ReadCountingStore extends CountingMemoryBlockStore {}

  async function seeded(): Promise<{ store: ReadCountingStore; database: MinnowDatabase }> {
    const store = new ReadCountingStore();
    const database = new MinnowDatabase(store, { rowsPerBlock: 4 });
    await database.createTable({
      name: "orders",
      columns: [
        { name: "region", type: "string" },
        { name: "amount", type: "number" },
      ],
    });
    await database.insertBatch("orders", {
      columns: {
        region: ["west", "east", "west", "north", "east", "west"],
        amount: [3, 8, 10, 4, 6, 2],
      },
    });
    return { store, database };
  }

  it("reuses CTE block results across prepares and refreshes them after writes", async () => {
    const { store, database } = await seeded();
    const sql =
      "WITH totals AS (SELECT region, SUM(amount) AS total FROM orders GROUP BY region) " +
      "SELECT region, total FROM totals ORDER BY total DESC";
    const first = await database.query(sql);
    expect(first.rows).toEqual([
      { region: "west", total: 15 },
      { region: "east", total: 14 },
      { region: "north", total: 4 },
    ]);
    const readsAfterFirst = store.blockReadCalls;
    const second = await database.query(sql);
    expect(second.rows).toEqual(first.rows);
    expect(store.blockReadCalls).toBe(readsAfterFirst);
    await database.insertBatch("orders", {
      columns: { region: ["north"], amount: [40] },
    });
    const third = await database.query(sql);
    expect(third.rows).toEqual([
      { region: "north", total: 44 },
      { region: "west", total: 15 },
      { region: "east", total: 14 },
    ]);
  });

  it("re-prepares window functions over CTEs without re-registered schemas", async () => {
    const { store, database } = await seeded();
    const sql =
      "WITH totals AS (SELECT region, SUM(amount) AS revenue FROM orders GROUP BY region), " +
      "ranked AS (SELECT region, revenue, DENSE_RANK() OVER (ORDER BY revenue DESC) AS rank FROM totals) " +
      "SELECT region, revenue, rank FROM ranked WHERE rank <= 2 ORDER BY rank";
    const expected = [
      { region: "west", revenue: 15, rank: 1 },
      { region: "east", revenue: 14, rank: 2 },
    ];
    const first = await database.query(sql);
    expect(first.rows).toEqual(expected);
    const readsAfterFirst = store.blockReadCalls;
    // The second prepare hits the cached windowed inner block; its schema must come from
    // the cache because the nested CTE registration is skipped on a hit.
    const second = await database.query(sql);
    expect(second.rows).toEqual(expected);
    expect(store.blockReadCalls).toBe(readsAfterFirst);
    await database.insertBatch("orders", {
      columns: { region: ["north"], amount: [40] },
    });
    const third = await database.query(sql);
    expect(third.rows).toEqual([
      { region: "north", revenue: 44, rank: 1 },
      { region: "west", revenue: 15, rank: 2 },
    ]);
  });

  it("reuses scalar subquery results and refreshes them after writes", async () => {
    const { database } = await seeded();
    const sql = "SELECT region FROM orders WHERE amount > (SELECT AVG(amount) FROM orders)";
    const first = await database.query(sql);
    expect(new Set(first.rows.map((row) => row.region))).toEqual(new Set(["east", "west"]));
    await database.insertBatch("orders", {
      columns: { region: ["south"], amount: [100] },
    });
    const second = await database.query(sql);
    expect(second.rows).toEqual([{ region: "south" }]);
  });

  it("reuses pruned projections per predicate and refreshes them after writes", async () => {
    const { store, database } = await seeded();
    const sql = "SELECT region, amount FROM orders WHERE amount > 7";
    const first = await database.query(sql);
    expect(first.rows).toEqual([
      { region: "east", amount: 8 },
      { region: "west", amount: 10 },
    ]);
    const readsAfterFirst = store.blockReadCalls;
    const second = await database.query(sql);
    expect(second.rows).toEqual(first.rows);
    expect(store.blockReadCalls).toBe(readsAfterFirst);
    const other = await database.query("SELECT region, amount FROM orders WHERE amount > 9");
    expect(other.rows).toEqual([{ region: "west", amount: 10 }]);
    await database.insertBatch("orders", {
      columns: { region: ["south"], amount: [50] },
    });
    const third = await database.query(sql);
    expect(third.rows).toEqual([
      { region: "east", amount: 8 },
      { region: "west", amount: 10 },
      { region: "south", amount: 50 },
    ]);
  });
});
