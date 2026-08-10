import { decodeBlock, encodeBlock, inspectBlock } from "@browserdatabase/block-format";
import { IDBFactory } from "fake-indexeddb";
import { describe, expect, it } from "vitest";
import {
  IndexedDbBlockStore,
  MemoryBlockStore,
  type BlockStore,
  type CompactionJobRecord,
  type CompactionJobRecordUpdate,
  type RowIdSpan,
  type SegmentRecord,
  type TableRecord,
} from "@browserdatabase/storage-idb";
import { FaultInjectingBlockStore } from "@browserdatabase/testing";
import { TransactionManager } from "@browserdatabase/transactions";
import { QueryMemoryBudgetError } from "./memory.js";
import {
  attachLifecycleFlush,
  BrowserDatabase,
  CompactionJobCancelledError,
  CompactionMemoryBudgetError,
  type DatabaseRow,
  MissingKeyError,
  UniqueConstraintError,
} from "./database.js";

class CountingMemoryBlockStore extends MemoryBlockStore {
  blockWriteCalls = 0;
  blockReadCalls = 0;
  transactionListCalls = 0;
  transactionGetCalls = 0;
  transactionBatchCalls = 0;
  segmentListCalls = 0;
  blockIdsRead: string[][] = [];
  singleBlockIdsRead: string[] = [];
  pendingBlockJournalSizes: number[] = [];

  override async addBlocks(blocks: Parameters<MemoryBlockStore["addBlocks"]>[0]): Promise<void> {
    this.blockWriteCalls += 1;
    return super.addBlocks(blocks);
  }

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

  override async listTransactions() {
    this.transactionListCalls += 1;
    return super.listTransactions();
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

  override async listSegments(tableId?: string) {
    this.segmentListCalls += 1;
    return super.listSegments(tableId);
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
}

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
    const storedOffset = 36 + view.getUint32(20, true);
    if (variant[storedOffset] !== 0x1f || variant[storedOffset + 1] !== 0x8b) {
      throw new Error("Expected a gzip compaction payload");
    }
    variant[storedOffset + 4] = (variant[storedOffset + 4] ?? 0) ^ 1;
    this.variantReadCount += 1;
    return variant;
  }
}

class PublicationCheckpointFaultMemoryBlockStore extends MemoryBlockStore {
  failPublishedCheckpoint = true;

  override async updateCompactionJob(
    id: string,
    expectedRevision: number,
    update: CompactionJobRecordUpdate,
  ) {
    if (update.state === "published" && this.failPublishedCheckpoint) {
      throw new Error("injected before compaction publication checkpoint");
    }
    return super.updateCompactionJob(id, expectedRevision, update);
  }
}

class InitialCompactionPlanningBarrierStore extends MemoryBlockStore {
  #listReadCount = 0;
  #jobReadCount = 0;
  #releaseListReads: (() => void) | undefined;
  #releaseJobReads: (() => void) | undefined;
  readonly #listReadsReady = new Promise<void>((resolve) => {
    this.#releaseListReads = resolve;
  });
  readonly #jobReadsReady = new Promise<void>((resolve) => {
    this.#releaseJobReads = resolve;
  });

  override async listCompactionJobs(tableId?: string): Promise<CompactionJobRecord[]> {
    if (this.#listReadCount >= 2) return super.listCompactionJobs(tableId);
    const records = await super.listCompactionJobs(tableId);
    this.#listReadCount += 1;
    if (this.#listReadCount === 2) this.#releaseListReads?.();
    await this.#listReadsReady;
    return records;
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
    if (this.#pauseNextCommit) {
      this.#pauseNextCommit = false;
      this.#signalFirstCommit?.();
      await this.#firstCommitRelease;
    }
    return super.commitTransaction(input);
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

interface MutationCompactionFixture {
  database: BrowserDatabase;
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
  const database = new BrowserDatabase(store, { compression: "raw", rowsPerBlock: 2 });
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
  const upsertSegment = await requiredSegment(store, upserted.segmentId);
  const resurrectionSegment = await requiredSegment(store, resurrectedA.segmentId);
  const initialRowIds = expandSegmentRowIds(initialSegment);
  const upsertRowIds = expandSegmentRowIds(upsertSegment);
  const resurrectionRowIds = expandSegmentRowIds(resurrectionSegment);
  const expectedRowIds = [
    requiredItem(initialRowIds, 1, "initial B row ID"),
    requiredItem(upsertRowIds, 1, "upserted D row ID"),
    requiredItem(resurrectionRowIds, 0, "resurrected A row ID"),
  ];
  expect(expectedRowIds[0]).not.toBe(upsertRowIds[0]);

  const visibleSourceIds = (await database.listVisibleSegments(tableName)).map(
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
    segment.rowIdSpans ??
    (segment.rowCount === 0
      ? []
      : [{ rowStart: 0, rowCount: segment.rowCount, rowIdStart: segment.rowIdStart }]);
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

function legacyAppendOnlyCompactionEligible(segment: SegmentRecord): boolean {
  return (segment.kind ?? "insert") === "insert";
}

async function assertPublishedMutationMerge(
  store: BlockStore,
  database: BrowserDatabase,
  tableName: string,
  jobId: string,
  expectedRows: readonly DatabaseRow[],
  expectedRowIds: readonly bigint[],
): Promise<SegmentRecord> {
  const job = await store.getCompactionJob(jobId);
  if (job === undefined) throw new Error(`Expected compaction job ${jobId}`);
  if (job.rewritePlan?.kind !== "merge-v1") throw new Error("Expected a merge-v1 plan");
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
  expect(await database.listVisibleSegments(tableName)).toHaveLength(1);
  return output;
}

interface MutationRebaseGuardFixture {
  database: BrowserDatabase;
  table: TableRecord;
  job: CompactionJobRecord;
  expectedRows: DatabaseRow[];
}

async function createMutationRebaseGuardFixture(
  store: MemoryBlockStore,
  tableName: string,
): Promise<MutationRebaseGuardFixture> {
  const database = new BrowserDatabase(store, { compression: "raw" });
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
  if (job?.rewritePlan?.kind !== "merge-v1") throw new Error("Expected a merge-v1 guard plan");
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
    createdAt: "2026-01-01T00:00:00.000Z",
  });
  await transaction.commit();
}

for (const implementation of implementations()) {
  describe(implementation.name, () => {
    it("creates tables with only simple data types and inserts a column batch", async () => {
      const store = await implementation.create();
      const database = new BrowserDatabase(store, { rowsPerBlock: 2 });
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
      expect(await database.listVisibleSegments("people")).toHaveLength(1);

      const table = (await store.listTables())[0];
      const segment = (await store.listSegments(table?.id))[0];
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

    it("rejects malformed batches before writing", async () => {
      const store = await implementation.create();
      const database = new BrowserDatabase(store);
      await database.createTable({
        name: "events",
        columns: [{ name: "value", type: "number" }],
      });
      await expect(
        database.insertBatch("events", { columns: { value: [1, "wrong"] } }),
      ).rejects.toThrow("must be number");
      expect(await store.listBlockIds()).toEqual([]);
      store.close();
    });

    it("upserts new and matching rows using a simple unique key", async () => {
      const store = await implementation.create();
      const database = new BrowserDatabase(store, { rowsPerBlock: 2 });
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
      const database = new BrowserDatabase(store, { rowsPerBlock: 2 });
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
      const updateSegment = (await store.listSegments(table?.id)).find(
        (segment) => segment.kind === "update",
      );
      expect(Object.keys(updateSegment?.columnBlockIds ?? {})).toHaveLength(2);
      store.close();
    });

    it("validates update keys and changed columns before publishing", async () => {
      const store = await implementation.create();
      const database = new BrowserDatabase(store);
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
      const database = new BrowserDatabase(store);
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
        (await store.listTransactions()).filter((record) => record.status === "active"),
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
      const database = new BrowserDatabase(store);
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
      const database = new BrowserDatabase(store, { rowsPerBlock: 2 });
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
      const database = new BrowserDatabase(store);
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
      const database = new BrowserDatabase(store);
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
      const database = new BrowserDatabase(store);
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
  const left = new BrowserDatabase(leftStore);
  const right = new BrowserDatabase(rightStore);
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
  expect(await left.listVisibleSegments("events")).toHaveLength(2);
  const segments = await leftStore.listSegments((await leftStore.listTables())[0]?.id);
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

it("rechecks unique keys when two IndexedDB connections insert the same value", async () => {
  const factory = new IDBFactory();
  const name = crypto.randomUUID();
  const leftStore = await IndexedDbBlockStore.open({ name, indexedDB: factory });
  const rightStore = await IndexedDbBlockStore.open({ name, indexedDB: factory });
  const left = new BrowserDatabase(leftStore);
  const right = new BrowserDatabase(rightStore);
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
  const left = new BrowserDatabase(leftStore);
  const right = new BrowserDatabase(rightStore);
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
    createdAt: "2026-01-01T00:00:00.000Z",
  });
  const database = new BrowserDatabase(store);
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
  const left = new BrowserDatabase(leftStore);
  const right = new BrowserDatabase(rightStore);
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
  const left = new BrowserDatabase(leftStore);
  const right = new BrowserDatabase(rightStore);
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
  const database = new BrowserDatabase(store, { rowsPerBlock: 2 });
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
  const oldBlockIds = await store.listBlockIds();

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
  expect(await database.listVisibleSegments("events")).toHaveLength(1);
  expect(await database.listVisibleSegments("events", second.version)).toHaveLength(2);
  expect((await store.listBlockIds()).filter((id) => oldBlockIds.includes(id))).toEqual(
    oldBlockIds,
  );
  store.close();
});

it("physically rechunks every simple type on shared bitmap-aligned row windows", async () => {
  const store = new MemoryBlockStore();
  const database = new BrowserDatabase(store, { compression: "raw", rowsPerBlock: 64 });
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
    outputCompression: "rle",
    maxBlocksPerStep: 1,
  });

  expect(result).toMatchObject({
    compacted: true,
    sourceSegmentCount: 3,
    sourceBlockCount: 12,
    outputBlockCount: 8,
    rowCount: 17,
    targetBlockBytes: 75,
    outputCompression: "rle",
  });
  expect(await database.readTable("readings")).toEqual(expected);
  expect(await database.readTable("readings", first.version)).toEqual(expected.slice(0, 5));
  expect(await database.readTable("readings", sourceSnapshot.version)).toEqual(expected);

  const job = (await database.listCompactionJobs("readings"))[0];
  if (job?.rewritePlan?.kind !== "rechunk-v1") {
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
      expect(description.compression).toBe("rle");
      expect(description.rowCount).toBe(job.rewritePlan.outputs[outputIndex]?.rowCount);
    }
  }
  expect([...outputTypes].sort()).toEqual(["boolean", "datetime", "number", "string"]);
  store.close();
});

it("prepares append and compacted snapshots directly into stable vectors", async () => {
  const store = new MemoryBlockStore();
  const database = new BrowserDatabase(store, { compression: "gzip", rowsPerBlock: 2 });
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

  const prepared = await database.prepareQuery(
    "SELECT active, score, label, recordedAt FROM vector_readings ORDER BY score",
  );
  const beforeCompaction = prepared.execute();
  await database.compactTable("vector_readings", {
    targetBlockBytes: 64,
    outputCompression: "rle",
  });
  expect(prepared.execute()).toEqual(beforeCompaction);
  prepared.close();
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
    rows: [
      {
        active: null,
        score: null,
        label: null,
        recordedAt: null,
      },
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
    ],
  });
  store.close();
});

it("replays keyed mutations into typed vectors without changing historical results", async () => {
  const store = new MemoryBlockStore();
  const database = new BrowserDatabase(store, { compression: "rle", rowsPerBlock: 1 });
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
  const database = new BrowserDatabase(store, { compression: "raw", rowsPerBlock: 64 });
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
  if (planned?.rewritePlan?.kind !== "rechunk-v1") {
    throw new Error("Expected a persisted rechunk plan");
  }
  expect(planned.rewritePlan.outputs).toContainEqual({ rowStart: 7, rowCount: 1 });
  expect(
    planned.rewritePlan.outputs.reduce((rowCount, output) => rowCount + output.rowCount, 0),
  ).toBe(values.length);

  const reopened = new BrowserDatabase(store, { compression: "gzip", rowsPerBlock: 1 });
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

for (const outputCompression of ["raw", "rle", "gzip"] as const) {
  it(`rewrites mixed source codecs to persisted ${outputCompression} output after reopen`, async () => {
    const store = new MemoryBlockStore();
    const raw = new BrowserDatabase(store, { compression: "raw", rowsPerBlock: 64 });
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
    const rle = new BrowserDatabase(store, { compression: "rle", rowsPerBlock: 64 });
    const rleInsert = await rle.insertBatch("events", {
      columns: { value: [null, 4], label: ["b", "c"] },
    });
    const gzip = new BrowserDatabase(store, { compression: "gzip", rowsPerBlock: 64 });
    const gzipInsert = await gzip.insertBatch("events", {
      columns: { value: [5, 6], label: [null, "d"] },
    });
    const expected = [
      { value: 1, label: "a" },
      { value: 2, label: null },
      { value: null, label: "b" },
      { value: 4, label: "c" },
      { value: 5, label: null },
      { value: 6, label: "d" },
    ];

    for (const [segmentId, compression] of [
      [rawInsert.segmentId, "raw"],
      [rleInsert.segmentId, "rle"],
      [gzipInsert.segmentId, "gzip"],
    ] as const) {
      const segment = await store.getSegment(segmentId);
      if (segment === undefined) throw new Error(`Expected source segment ${segmentId}`);
      for (const blockId of Object.values(segment.columnBlockIds).flat()) {
        const bytes = await store.getBlock(blockId);
        if (bytes === undefined) throw new Error(`Expected source block ${blockId}`);
        expect(inspectBlock(bytes).compression).toBe(compression);
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
    const reopened = new BrowserDatabase(store, {
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
      expect(inspectBlock(bytes).compression).toBe(outputCompression);
    }
    expect(await reopened.readTable("events")).toEqual(expected);
    expect(await reopened.readTable("events", gzipInsert.version)).toEqual(expected);
    store.close();
  });
}

it("enforces the persisted compaction memory bound before publishing job output", async () => {
  const store = new MemoryBlockStore();
  const database = new BrowserDatabase(store, { compression: "raw" });
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
  const sourceBlockIds = await store.listBlockIds();
  const sourceManifestCount = (await store.listManifests()).length;
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
  expect(await store.listBlockIds()).toEqual(sourceBlockIds);
  expect(await store.listManifests()).toHaveLength(sourceManifestCount);

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
  const database = new BrowserDatabase(store, { compression: "raw", rowsPerBlock: 1 });
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
  const reopened = new BrowserDatabase(store, { compression: "rle", rowsPerBlock: 2048 });
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
    expect(inspectBlock(bytes).compression).toBe("gzip");
  }
  expect(await reopened.readTable("events")).toEqual([{ value: 1 }, { value: 2 }, { value: 3 }]);
  store.close();
});

it("coalesces many small source segments into fewer physical blocks", async () => {
  const store = new MemoryBlockStore();
  const database = new BrowserDatabase(store, { rowsPerBlock: 1 });
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
  const database = new BrowserDatabase(store);
  await database.createTable({
    name: "events",
    columns: [{ name: "value", type: "number" }],
  });
  await database.insert("events", { value: 1 });
  await database.insert("events", { value: 2 });

  await expect(
    database.compactTable("events", {
      targetBlockBytes: 32 * 1024 * 1024 + 1,
      outputCompression: "rle",
    }),
  ).rejects.toThrow("Compaction target block bytes exceed the rle worst-case format limit");
  expect(await store.listCompactionJobs()).toEqual([]);
  store.close();
});

it("checkpoints and resumes append compaction one immutable block at a time", async () => {
  const store = new MemoryBlockStore();
  const database = new BrowserDatabase(store);
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

  const reopened = new BrowserDatabase(store);
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
  expect(await reopened.listVisibleSegments("events")).toHaveLength(1);
  const job = (await reopened.listCompactionJobs("events"))[0];
  const output =
    job?.outputSegmentId === null ? undefined : await store.getSegment(job?.outputSegmentId ?? "");
  expect(output).toMatchObject({ level: 1, logicalOrder: 0, rowCount: 3 });
  store.close();
});

it("rebases resumable compaction across an append without reordering rows", async () => {
  const store = new MemoryBlockStore();
  const database = new BrowserDatabase(store);
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
  expect(await database.listVisibleSegments("events")).toHaveLength(2);
  store.close();
});

it("recovers a compaction block written before its journal checkpoint", async () => {
  const store = new MemoryBlockStore();
  const database = new BrowserDatabase(store);
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
  const interrupted = new BrowserDatabase(faultStore);
  await expect(interrupted.compactTableStep("events", { maxBlocks: 1 })).rejects.toThrow(
    "injected after compaction block write",
  );
  const interruptedJob = (await store.listCompactionJobs())[0];
  expect(interruptedJob).toMatchObject({ state: "running", outputBlockIds: [] });

  const reopened = new BrowserDatabase(store);
  const result = await reopened.compactTable("events");
  expect(result.compacted).toBe(true);
  expect(await reopened.readTable("events")).toEqual([{ value: 1 }, { value: 2 }]);
  expect((await reopened.listCompactionJobs("events"))[0]?.state).toBe("published");
  store.close();
});

it("reconciles a valid gzip header variant by decoded physical content", async () => {
  const store = new GzipVariantCheckpointFaultMemoryBlockStore();
  const database = new BrowserDatabase(store);
  await database.createTable({
    name: "events",
    columns: [{ name: "value", type: "number" }],
  });
  await database.insert("events", { value: 1 });
  await database.insert("events", { value: 2 });

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
  expect(await decodeBlock(variant)).toEqual(await decodeBlock(canonical));

  const result = await new BrowserDatabase(store).compactTable("events");

  expect(result).toMatchObject({ compacted: true, outputCompression: "gzip", rowCount: 2 });
  expect(store.variantReadCount).toBeGreaterThan(0);
  expect(await database.readTable("events")).toEqual([{ value: 1 }, { value: 2 }]);
  store.close();
});

it("reconciles a journaled compaction block when its cursor checkpoint is lost", async () => {
  const store = new CheckpointFaultMemoryBlockStore();
  const database = new BrowserDatabase(store);
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

  const result = await new BrowserDatabase(store).compactTable("events");
  expect(result.compacted).toBe(true);
  expect(await database.readTable("events")).toEqual([{ value: 1 }, { value: 2 }]);
  store.close();
});

it("resumes a ready compaction interrupted before manifest publication", async () => {
  const store = new MemoryBlockStore();
  const database = new BrowserDatabase(store);
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
  await expect(new BrowserDatabase(faultStore).compactTable("events")).rejects.toThrow(
    "injected before compaction publication",
  );
  expect((await store.listCompactionJobs())[0]?.state).toBe("ready");

  const reopened = new BrowserDatabase(store);
  const result = await reopened.compactTable("events");
  expect(result.compacted).toBe(true);
  expect(await reopened.readTable("events")).toEqual([{ value: 1 }, { value: 2 }]);
  store.close();
});

for (const implementation of recoveryImplementations()) {
  it(`${implementation.name} durably cancels partial compaction without deleting artifacts`, async () => {
    const harness = await implementation.create();
    let store = harness.store;
    const database = new BrowserDatabase(store);
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
    const reopened = new BrowserDatabase(store);
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
    const database = new BrowserDatabase(store);
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
    await expect(new BrowserDatabase(faultStore).compactTable("events")).rejects.toThrow(
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
    const database = new BrowserDatabase(store);
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
      new BrowserDatabase(store).cancelCompactionJob(progress.jobId),
      new BrowserDatabase(store).cancelCompactionJob(progress.jobId),
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
    await expect(new BrowserDatabase(store).cancelCompactionJob("missing-job")).rejects.toThrow(
      "Compaction job not found: missing-job",
    );
    expect(await store.listCompactionJobs()).toEqual([]);
    store.close();
  });
}

it("reconciles cancellation with a compaction transaction that already committed", async () => {
  const store = new MemoryBlockStore();
  const database = new BrowserDatabase(store);
  await database.createTable({
    name: "events",
    columns: [{ name: "value", type: "number" }],
  });
  await database.insert("events", { value: 1 });
  await database.insert("events", { value: 2 });
  const manifestCount = (await store.listManifests()).length;

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
  const completionPromise = new BrowserDatabase(faultStore).compactTable("events");
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

  let cancellation: Awaited<ReturnType<BrowserDatabase["cancelCompactionJob"]>>;
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
  expect(await store.listManifests()).toHaveLength(manifestCount + 1);
  expect(await database.readTable("events")).toEqual([{ value: 1 }, { value: 2 }]);
  store.close();
});

it("translates cancellation during an in-flight output checkpoint", async () => {
  const store = new MemoryBlockStore();
  const database = new BrowserDatabase(store);
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
  const resumePromise = new BrowserDatabase(faultStore).resumeCompactionJob(progress.jobId, {
    maxBlocks: 1,
  });
  await blockWritten;

  let cancellation: Awaited<ReturnType<BrowserDatabase["cancelCompactionJob"]>>;
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
  const database = new BrowserDatabase(store, { now: () => new Date(now.getTime()) });
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
  const reopened = new BrowserDatabase(store, { now: () => new Date(now.getTime()) });
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
  expect(store.pendingBlockJournalSizes).toEqual([3, 4]);
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
  const database = new BrowserDatabase(store, { now: () => new Date(now.getTime()) });
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
    new BrowserDatabase(store, { now: () => new Date(now.getTime()) }).resumeCompactionJob(
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
  const reopened = new BrowserDatabase(store, { now: () => new Date(now.getTime()) });
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
  expect(store.pendingBlockJournalSizes).toEqual([3, 4]);
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
    const database = new BrowserDatabase(store, { now: () => new Date(now.getTime()) });
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
    const reopened = new BrowserDatabase(store, { now: () => new Date(now.getTime()) });
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
    const database = new BrowserDatabase(store, { now: () => new Date(now.getTime()) });
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
      new BrowserDatabase(faultStore, { now: () => new Date(now.getTime()) }).compactTable(
        "events",
      ),
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
    const reopened = new BrowserDatabase(store, { now: () => new Date(now.getTime()) });
    let resumed = await reopened.resumeCompactionJob(interrupted.id);
    while (resumed.result === null) {
      resumed = await reopened.resumeCompactionJob(interrupted.id);
    }
    expect(resumed).toMatchObject({ state: "published", result: { compacted: true, rowCount: 2 } });
    expect(await reopened.readTable("events")).toEqual([{ value: 1 }, { value: 2 }]);
    store.close();
  });
}

it("aborts a replacement compaction transaction when its sources were superseded", async () => {
  let now = new Date("2026-01-01T00:00:00.000Z");
  const store = new MemoryBlockStore();
  const database = new BrowserDatabase(store, { now: () => new Date(now.getTime()) });
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

  const replacer = new TransactionManager(store, {
    now: () => new Date(now.getTime()),
    createId: () => "source-replacer",
  });
  const replacement = await replacer.begin();
  replacement.supersedeBlocks(interrupted.sourceBlockIds);
  await replacement.commit();

  now = new Date("2026-01-01T01:00:00.000Z");
  const recovery = new TransactionManager(store, { now: () => new Date(now.getTime()) });
  const report = await recovery.recover({
    staleBefore: new Date("2026-01-01T00:30:00.000Z"),
    removePendingBlocks: false,
  });
  expect(report.abortedTransactionIds).toContain(interrupted.transactionId);

  const reopened = new BrowserDatabase(store, { now: () => new Date(now.getTime()) });
  await expect(reopened.resumeCompactionJob(progress.jobId)).rejects.toThrow(
    "Compaction source is no longer visible",
  );
  const aborted = await store.getCompactionJob(progress.jobId);
  expect(aborted?.state).toBe("aborted");
  expect(aborted?.error).toContain("Compaction source is no longer visible");
  if (aborted === undefined) throw new Error("Expected a failed compaction job");
  const abortedTransaction =
    aborted.transactionId === null ? undefined : await store.getTransaction(aborted.transactionId);
  const manifestBeforeCancellation = await store.getCurrentManifest();

  expect(await reopened.cancelCompactionJob(progress.jobId)).toEqual({
    jobId: progress.jobId,
    state: "aborted",
    publishedVersion: null,
  });
  expect(await store.getCompactionJob(progress.jobId)).toEqual(aborted);
  expect(
    aborted.transactionId === null ? undefined : await store.getTransaction(aborted.transactionId),
  ).toEqual(abortedTransaction);
  expect(await store.getCurrentManifest()).toEqual(manifestBeforeCancellation);
  store.close();
});

it("returns a published compaction job repeatedly without publishing another manifest", async () => {
  const store = new MemoryBlockStore();
  const database = new BrowserDatabase(store);
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
  const manifestCount = (await store.listManifests()).length;
  const transactionCount = (await store.listTransactions()).length;

  const first = await new BrowserDatabase(store).resumeCompactionJob(result.jobId);
  const second = await new BrowserDatabase(store).resumeCompactionJob(result.jobId);
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
  expect(await store.listManifests()).toHaveLength(manifestCount);
  expect(await store.listTransactions()).toHaveLength(transactionCount);
  store.close();
});

it("reconciles a committed compaction after its source segment metadata is reclaimed", async () => {
  const store = new PublicationCheckpointFaultMemoryBlockStore();
  const database = new BrowserDatabase(store);
  await database.createTable({
    name: "events",
    columns: [{ name: "value", type: "number" }],
  });
  await database.insert("events", { value: 1 });
  await database.insert("events", { value: 2 });

  await expect(database.compactTable("events")).rejects.toThrow(
    "injected before compaction publication checkpoint",
  );
  const interrupted = (await store.listCompactionJobs())[0];
  expect(interrupted?.state).toBe("ready");
  if (interrupted?.transactionId === null || interrupted?.transactionId === undefined) {
    throw new Error("Expected a committed compaction transaction");
  }
  const committed = await store.getTransaction(interrupted.transactionId);
  expect(committed?.status).toBe("committed");
  expect(committed?.committedVersion).not.toBeNull();
  for (const segmentId of interrupted.sourceSegmentIds) await store.removeSegment(segmentId);
  store.failPublishedCheckpoint = false;

  const resumed = await new BrowserDatabase(store).resumeCompactionJob(interrupted.id);
  expect(resumed).toMatchObject({
    state: "published",
    result: { compacted: true, rowCount: 2 },
  });
  expect(await database.readTable("events")).toEqual([{ value: 1 }, { value: 2 }]);
  store.close();
});

it("aborts the unlinked transaction when initial compaction coordinators race", async () => {
  const store = new InitialCompactionPlanningBarrierStore();
  const setup = new BrowserDatabase(store);
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
  const left = new BrowserDatabase(store, {
    createId: () => `left-coordinator-${String((leftId += 1))}`,
  });
  const right = new BrowserDatabase(store, {
    createId: () => `right-coordinator-${String((rightId += 1))}`,
  });

  await Promise.allSettled([
    left.compactTableStep("events", { maxBlocks: 1 }),
    right.compactTableStep("events", { maxBlocks: 1 }),
  ]);

  const job = (await store.listCompactionJobs())[0];
  expect(job?.transactionId).toEqual(expect.any(String));
  const coordinatorTransactions = (await store.listTransactions()).filter(
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
    expect(legacyAppendOnlyCompactionEligible(firstOutput)).toBe(false);
    const firstManifest = await store.getCurrentManifest();
    expect(
      fixture.sourceBlockIds.every((blockId) => !firstManifest?.blockIds.includes(blockId)),
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
    const postMergeUpsertSegment = await requiredSegment(store, postMergeUpsert.segmentId);
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
      outputCompression: "rle",
    });
    if (second.jobId === undefined) throw new Error("Expected a second mutation compaction");
    expect(second).toMatchObject({
      compacted: true,
      sourceSegmentCount: 4,
      rowCount: secondExpectedRows.length,
      outputCompression: "rle",
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
    const database = new BrowserDatabase(store, { compression: "raw" });
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
    const sourceBlockIds = await store.listBlockIds();

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
    expect((await store.getCurrentManifest())?.blockIds).toEqual([]);
    expect(await database.readTable(tableName)).toEqual([]);
    expect(await database.listVisibleSegments(tableName)).toEqual([]);
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
    const reopened = new BrowserDatabase(store, { compression: "rle", rowsPerBlock: 1 });
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

  const reopened = new BrowserDatabase(store);
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
  const database = new BrowserDatabase(store);
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
  let second;
  try {
    second = await database.insert("reverse_ids", { email: "second@example.com", score: 2 });
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
  const writer = new BrowserDatabase(writerStore);

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
    await requiredSegment(writerStore, laterUpsert.segmentId),
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
  expect(await fixture.database.listVisibleSegments(tableName)).toHaveLength(4);
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

it("aborts a mutation merge when a concurrent segment would sort inside its source interval", async () => {
  const store = new MemoryBlockStore();
  const fixture = await createMutationRebaseGuardFixture(store, "interleaved_merge_guard");
  const plan = fixture.job.rewritePlan;
  if (plan?.kind !== "merge-v1") throw new Error("Expected an interleaved merge plan");
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
  const concurrentManifest = await store.getCurrentManifest();
  expect(concurrent.manifestVersion).toBe(manifestBeforeConcurrent.version + 1);
  expect(concurrentManifest?.blockIds).toContain(concurrent.blockId);

  await expect(
    fixture.database.resumeCompactionJob(fixture.job.id, { maxBlocks: 16 }),
  ).rejects.toThrow(`Concurrent segment would reorder compaction output: ${concurrent.segmentId}`);
  const aborted = await store.getCompactionJob(fixture.job.id);
  expect(aborted).toMatchObject({
    state: "aborted",
    error: `Concurrent segment would reorder compaction output: ${concurrent.segmentId}`,
    publishedVersion: null,
  });
  if (fixture.job.transactionId === null) throw new Error("Expected a guarded transaction");
  expect(await store.getTransaction(fixture.job.transactionId)).toMatchObject({
    status: "aborted",
  });
  expect(await store.getCurrentManifest()).toEqual(concurrentManifest);
  expect(
    fixture.job.sourceBlockIds.every((blockId) => concurrentManifest?.blockIds.includes(blockId)),
  ).toBe(true);
  expect(
    fixture.job.outputBlockIds.every((blockId) => !concurrentManifest?.blockIds.includes(blockId)),
  ).toBe(true);
  expect(await fixture.database.readTable(fixture.table.name)).toEqual(fixture.expectedRows);
  store.close();
});

it("aborts a mutation merge when a later concurrent tuple shares the base logical order", async () => {
  const store = new MemoryBlockStore();
  const fixture = await createMutationRebaseGuardFixture(store, "equal_order_merge_guard");
  const plan = fixture.job.rewritePlan;
  if (plan?.kind !== "merge-v1") throw new Error("Expected an equal-order merge plan");
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
  const concurrentManifest = await store.getCurrentManifest();
  expect(concurrent.manifestVersion).toBeGreaterThan(latestPlannedCommit);
  expect(concurrent.manifestVersion).toBe(manifestBeforeConcurrent.version + 1);
  expect(concurrentManifest?.blockIds).toContain(concurrent.blockId);

  await expect(
    fixture.database.resumeCompactionJob(fixture.job.id, { maxBlocks: 16 }),
  ).rejects.toThrow(`Concurrent segment would reorder compaction output: ${concurrent.segmentId}`);
  const aborted = await store.getCompactionJob(fixture.job.id);
  expect(aborted).toMatchObject({
    state: "aborted",
    error: `Concurrent segment would reorder compaction output: ${concurrent.segmentId}`,
    publishedVersion: null,
  });
  if (fixture.job.transactionId === null) throw new Error("Expected a guarded transaction");
  expect(await store.getTransaction(fixture.job.transactionId)).toMatchObject({
    status: "aborted",
  });
  expect(await store.getCurrentManifest()).toEqual(concurrentManifest);
  expect(
    fixture.job.sourceBlockIds.every((blockId) => concurrentManifest?.blockIds.includes(blockId)),
  ).toBe(true);
  expect(
    fixture.job.outputBlockIds.every((blockId) => !concurrentManifest?.blockIds.includes(blockId)),
  ).toBe(true);
  expect(await fixture.database.readTable(fixture.table.name)).toEqual(fixture.expectedRows);
  store.close();
});

it("aborts a mutation merge before supersession when a concurrent segment aliases a source block", async () => {
  const store = new MemoryBlockStore();
  const fixture = await createMutationRebaseGuardFixture(store, "shared_block_merge_guard");
  const plan = fixture.job.rewritePlan;
  if (plan?.kind !== "merge-v1") throw new Error("Expected a shared-block merge plan");
  const keyBlockId = plan.sourceSegments
    .flatMap((segment) => segment.columns)
    .find((column) => column.columnId === plan.keyColumnId)?.sourceBlocks[0]?.blockId;
  if (keyBlockId === undefined) throw new Error("Expected a guarded source key block");
  const manifestBeforeConcurrent = await store.getCurrentManifest();
  if (manifestBeforeConcurrent === undefined) throw new Error("Expected a guarded source manifest");
  const concurrent = await commitLowLevelDeleteSegment(store, fixture.table, {
    segmentId: "shared-source-block-delete",
    logicalOrder: 10,
    blockId: keyBlockId,
  });
  const concurrentManifest = await store.getCurrentManifest();
  expect(concurrent.manifestVersion).toBe(manifestBeforeConcurrent.version + 1);
  expect(concurrentManifest?.blockIds).toEqual(manifestBeforeConcurrent.blockIds);

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
    fixture.job.sourceBlockIds.every((blockId) => concurrentManifest?.blockIds.includes(blockId)),
  ).toBe(true);
  expect(
    fixture.job.outputBlockIds.every((blockId) => !concurrentManifest?.blockIds.includes(blockId)),
  ).toBe(true);
  store.close();
});

it("aborts a mutation merge when a segment from another table aliases a global source block", async () => {
  const store = new MemoryBlockStore();
  const fixture = await createMutationRebaseGuardFixture(store, "cross_table_block_merge_guard");
  const plan = fixture.job.rewritePlan;
  if (plan?.kind !== "merge-v1") throw new Error("Expected a cross-table merge plan");
  const keyBlockId = plan.sourceSegments
    .flatMap((segment) => segment.columns)
    .find((column) => column.columnId === plan.keyColumnId)?.sourceBlocks[0]?.blockId;
  if (keyBlockId === undefined) throw new Error("Expected a cross-table guarded source block");
  await fixture.database.createTable({
    name: "source_alias_owner",
    uniqueKey: "alias",
    columns: [{ name: "alias", type: "string" }],
  });
  const aliasTable = await store.getTableByName("source_alias_owner");
  if (aliasTable === undefined) throw new Error("Expected a source-alias table");
  expect(aliasTable.id).not.toBe(fixture.table.id);
  const manifestBeforeConcurrent = await store.getCurrentManifest();
  if (manifestBeforeConcurrent === undefined) throw new Error("Expected a guarded source manifest");
  const concurrent = await commitLowLevelDeleteSegment(store, aliasTable, {
    segmentId: "cross-table-shared-source-block-delete",
    logicalOrder: 0,
    blockId: keyBlockId,
  });
  expect((await store.getSegment(concurrent.segmentId))?.tableId).toBe(aliasTable.id);
  const concurrentManifest = await store.getCurrentManifest();
  expect(concurrent.manifestVersion).toBe(manifestBeforeConcurrent.version + 1);
  expect(concurrentManifest?.blockIds).toEqual(manifestBeforeConcurrent.blockIds);

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
    fixture.job.sourceBlockIds.every((blockId) => concurrentManifest?.blockIds.includes(blockId)),
  ).toBe(true);
  expect(
    fixture.job.outputBlockIds.every((blockId) => !concurrentManifest?.blockIds.includes(blockId)),
  ).toBe(true);
  store.close();
});

for (const implementation of implementations()) {
  it(`${implementation.name} repeatedly compacts bounded oldest append prefixes with subset metrics`, async () => {
    const store = await implementation.create();
    const tableName = `bounded_append_${implementation.name}`;
    const database = new BrowserDatabase(store, { compression: "raw" });
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
    expect((await database.listVisibleSegments(tableName)).map((segment) => segment.id)).toEqual(
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
      expect((await database.listVisibleSegments(tableName)).map((segment) => segment.id)).toEqual([
        anchorSegmentId,
        ...originalSegmentIds.slice((jobIndex + 1) * 2),
      ]);
      expect(await database.readTable(tableName)).toEqual(expectedRows);
      for (const snapshot of snapshots) {
        expect(await database.readTable(tableName, snapshot.version)).toEqual(snapshot.rows);
      }
    }
    expect(await database.listVisibleSegments(tableName)).toHaveLength(1);
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
        (await fixture.database.listVisibleSegments(tableName)).map((segment) => segment.id),
      ).toEqual([anchorSegmentId, ...originalSegmentIds.slice((jobIndex + 1) * 2)]);
      expect(await fixture.database.readTable(tableName)).toEqual(fixture.expectedRows);
      for (const snapshot of fixture.snapshots) {
        expect(await fixture.database.readTable(tableName, snapshot.version)).toEqual(
          snapshot.rows,
        );
      }
    }
    expect(await fixture.database.listVisibleSegments(tableName)).toHaveLength(1);
    store.close();
  });

  it(`${implementation.name} preserves a later upsert when a bounded prefix deletes every base row`, async () => {
    const store = await implementation.create();
    const tableName = `bounded_empty_${implementation.name}`;
    const database = new BrowserDatabase(store, { compression: "raw" });
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
      expandSegmentRowIds(await requiredSegment(store, later.segmentId)),
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
    expect((await database.listVisibleSegments(tableName)).map((segment) => segment.id)).toEqual([
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
    const database = new BrowserDatabase(store, { compression: "raw" });
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
    const reopened = new BrowserDatabase(store, { compression: "gzip", rowsPerBlock: 2048 });
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
    expect((await reopened.listVisibleSegments(tableName)).map((segment) => segment.id)).toEqual([
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
  const compactor = new BrowserDatabase(compactorStore, { compression: "raw" });
  const writer = new BrowserDatabase(writerStore, { compression: "raw" });
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
    await requiredSegment(compactorStore, upserted.segmentId),
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
    [inserted.segmentId, upserted.segmentId],
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
  expect((await compactor.listVisibleSegments(tableName)).map((segment) => segment.id)).toEqual([
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
  expect((await compactor.listVisibleSegments(tableName)).map((segment) => segment.id)).toEqual([
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

it("keeps an oversized oldest equal-order L0 group indivisible", async () => {
  const store = new MemoryBlockStore();
  const database = new BrowserDatabase(store);
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
      rowId: BigInt(index),
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
  expect(result.sourceSegmentCount).toBe(3);
  await assertPersistedCompactionSelection(store, result.jobId, segmentIds.slice(0, 3), null);
  expect(
    (await database.listVisibleSegments("equal_order_prefix")).map((segment) => segment.id),
  ).toEqual([result.outputSegmentId, requiredItem(segmentIds, 3, "later equal-order segment")]);
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
  const database = new BrowserDatabase(store, { compression: "raw" });
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
    (await database.listVisibleSegments("stored_byte_prefix")).map((segment) => segment.id),
  ).toEqual([result.outputSegmentId, ...inserts.slice(2).map((insert) => insert.segmentId)]);
  expect(await database.readTable("stored_byte_prefix")).toEqual(
    inserts.map((_insert, index) => ({ value: index + 1 })),
  );
  store.close();
});

it("drains an odd append tail only when an anchored job explicitly lowers its minimum", async () => {
  const store = new MemoryBlockStore();
  const database = new BrowserDatabase(store, { compression: "raw" });
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
    (await database.listVisibleSegments("odd_tail_drain")).map((segment) => segment.id),
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
  expect(await database.listVisibleSegments("odd_tail_drain")).toEqual([
    expect.objectContaining({ id: drained.outputSegmentId, rowCount: 5 }),
  ]);
  expect(await database.readTable("odd_tail_drain")).toEqual(expectedRows);
  for (const snapshot of snapshots) {
    expect(await database.readTable("odd_tail_drain", snapshot.version)).toEqual(snapshot.rows);
  }
  store.close();
});

it("validates bounded compaction options and preserves the deprecated threshold alias", async () => {
  const store = new MemoryBlockStore();
  const database = new BrowserDatabase(store);
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
    { minimumSegments: 2, minimumLevel0Segments: 3 },
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
  const aliased = await database.compactTable("bounded_options", {
    minimumSegments: 2,
    outputCompression: "raw",
  });
  expect(aliased).toMatchObject({ compacted: true, sourceSegmentCount: 2 });
  store.close();
});

it("skips unsupported compaction level layouts without creating jobs", async () => {
  const store = new MemoryBlockStore();
  const database = new BrowserDatabase(store);
  const layouts = [
    {
      tableName: "multiple_anchors",
      levels: [1, 1, 0, 0],
    },
    {
      tableName: "nonleading_anchor",
      levels: [0, 1, 0],
    },
    {
      tableName: "level_two_anchor",
      levels: [2, 0, 0],
    },
  ];
  for (const { tableName, levels } of layouts) {
    await database.createTable({
      name: tableName,
      columns: [{ name: "value", type: "number" }],
    });
    const table = await store.getTableByName(tableName);
    if (table === undefined) throw new Error(`Expected layout table ${tableName}`);
    for (const [index, level] of levels.entries()) {
      await commitLowLevelNumberSegment(store, table, {
        segmentId: `${tableName}-${String(index)}`,
        level,
        logicalOrder: index,
        rowId: BigInt(index),
        value: index,
      });
    }
    const result = await database.compactTable(tableName, { minimumLevel0Segments: 2 });
    expect(result).toMatchObject({
      compacted: false,
      skipReason: "unsupported-level-layout",
      sourceSegmentCount: levels.length,
    });
  }
  expect(await store.listCompactionJobs()).toEqual([]);
  store.close();
});

for (const implementation of implementations()) {
  it(`${implementation.name} repeatedly promotes L0 prefixes into immutable ordered L2 partitions`, async () => {
    const store = await implementation.create();
    const tableName = `l2_prefix_${implementation.name}`;
    const database = new BrowserDatabase(store, { compression: "raw" });
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

      expect((await database.listVisibleSegments(tableName)).map((segment) => segment.id)).toEqual([
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
  const database = new BrowserDatabase(store, { compression: "raw" });
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
    (await database.listVisibleSegments("l1_l2_transition")).map((segment) => segment.id),
  ).toEqual([retainedSegment.id, firstL2.outputSegmentId, secondL2.outputSegmentId]);
  expect(await database.readTable("l1_l2_transition")).toEqual(
    inserts.map((_insert, index) => ({ value: index + 1 })),
  );
  expect(
    await database.readTable("l1_l2_transition", requiredItem(inserts, 1, "L1 snapshot").version),
  ).toEqual([{ value: 1 }, { value: 2 }]);
  store.close();
});

it("rejects keyed and malformed L2 layouts without creating compaction jobs", async () => {
  const store = new MemoryBlockStore();
  const database = new BrowserDatabase(store, { compression: "raw" });
  await database.createTable({
    name: "keyed_l2_rejection",
    uniqueKey: "key",
    columns: [
      { name: "key", type: "string" },
      { name: "value", type: "number" },
    ],
  });
  await database.insert("keyed_l2_rejection", { key: "a", value: 1 });
  await database.insert("keyed_l2_rejection", { key: "b", value: 2 });
  expect(
    await database.compactTable("keyed_l2_rejection", {
      targetLevel: 2,
      minimumLevel0Segments: 1,
    }),
  ).toMatchObject({ compacted: false, skipReason: "contains-mutation-segments" });

  await database.createTable({
    name: "interleaved_l2_layout",
    columns: [{ name: "value", type: "number" }],
  });
  const interleaved = await store.getTableByName("interleaved_l2_layout");
  if (interleaved === undefined) throw new Error("Expected an interleaved L2 table");
  await commitLowLevelNumberSegment(store, interleaved, {
    segmentId: "interleaved-l0-first",
    level: 0,
    logicalOrder: 0,
    rowId: 0n,
    value: 1,
  });
  await commitLowLevelNumberSegment(store, interleaved, {
    segmentId: "interleaved-l2",
    level: 2,
    partitionOrdinal: 0,
    logicalOrder: 1,
    rowId: 1n,
    value: 2,
  });
  await commitLowLevelNumberSegment(store, interleaved, {
    segmentId: "interleaved-l0-tail",
    level: 0,
    logicalOrder: 2,
    rowId: 2n,
    value: 3,
  });
  expect(
    await database.compactTable("interleaved_l2_layout", {
      targetLevel: 2,
      minimumLevel0Segments: 1,
    }),
  ).toMatchObject({ compacted: false, skipReason: "unsupported-level-layout" });

  await database.createTable({
    name: "gapped_l2_ordinals",
    columns: [{ name: "value", type: "number" }],
  });
  const gapped = await store.getTableByName("gapped_l2_ordinals");
  if (gapped === undefined) throw new Error("Expected a gapped L2 table");
  for (const input of [
    { segmentId: "gapped-l2-zero", level: 2, partitionOrdinal: 0, logicalOrder: 0, rowId: 1n },
    { segmentId: "gapped-l2-two", level: 2, partitionOrdinal: 2, logicalOrder: 1, rowId: 2n },
    { segmentId: "gapped-l0-a", level: 0, logicalOrder: 2, rowId: 3n },
    { segmentId: "gapped-l0-b", level: 0, logicalOrder: 3, rowId: 4n },
  ]) {
    await commitLowLevelNumberSegment(store, gapped, { ...input, value: Number(input.rowId) });
  }
  expect(
    await database.compactTable("gapped_l2_ordinals", {
      targetLevel: 2,
      minimumLevel0Segments: 1,
    }),
  ).toMatchObject({ compacted: false, skipReason: "unsupported-level-layout" });

  await database.createTable({
    name: "overlapping_l2_ranges",
    columns: [{ name: "value", type: "number" }],
  });
  const overlapping = await store.getTableByName("overlapping_l2_ranges");
  if (overlapping === undefined) throw new Error("Expected an overlapping L2 table");
  for (const input of [
    { segmentId: "overlap-l2-zero", level: 2, partitionOrdinal: 0, logicalOrder: 0, rowId: 1n },
    { segmentId: "overlap-l2-one", level: 2, partitionOrdinal: 1, logicalOrder: 1, rowId: 1n },
    { segmentId: "overlap-l0-a", level: 0, logicalOrder: 2, rowId: 3n },
    { segmentId: "overlap-l0-b", level: 0, logicalOrder: 3, rowId: 4n },
  ]) {
    await commitLowLevelNumberSegment(store, overlapping, {
      ...input,
      value: Number(input.rowId),
    });
  }
  expect(
    await database.compactTable("overlapping_l2_ranges", {
      targetLevel: 2,
      minimumLevel0Segments: 1,
    }),
  ).toMatchObject({ compacted: false, skipReason: "unsupported-level-layout" });
  expect(await store.listCompactionJobs()).toEqual([]);
  store.close();
});

it("keeps the implicit L1 default but permits an explicit single-segment L2 promotion", async () => {
  const store = new MemoryBlockStore();
  const database = new BrowserDatabase(store, { compression: "raw" });
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
  const database = new BrowserDatabase(store, { compression: "raw" });
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
  const blocksBefore = await store.listBlockIds();
  const segmentsBefore = await store.listSegments();
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
  expect(await store.listBlockIds()).toEqual(blocksBefore);
  expect(await store.listSegments()).toEqual(segmentsBefore);
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
  const database = new BrowserDatabase(store, { compression: "raw" });
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
  expect(belowStats.storedBytes).toBe(200);
  expect(
    await database.compactTable("l2_binary_ratio_below", {
      ...options,
      maxWriteAmplification: 0.19999999999999998,
    }),
  ).toMatchObject({
    compacted: false,
    skipReason: "write-amplification-budget",
    maximumOutputStoredBytes: 39,
    plannedOutputStoredBytesUpperBound: 40,
  });

  const exactStats = await seed("l2_binary_ratio_exact");
  expect(exactStats.storedBytes).toBe(200);
  expect(
    await database.compactTable("l2_binary_ratio_exact", {
      ...options,
      maxWriteAmplification: 0.2,
    }),
  ).toMatchObject({
    compacted: true,
    maximumOutputStoredBytes: 40,
    plannedOutputStoredBytesUpperBound: 40,
    outputStoredBytes: 40,
  });
  store.close();
});

for (const implementation of recoveryImplementations()) {
  it(`${implementation.name} reopens an L2 checkpoint with its exact prefix, ordinal, and ceiling`, async () => {
    const harness = await implementation.create();
    let store = harness.store;
    const tableName = `l2_reopen_${implementation.name.replaceAll(" ", "_")}`;
    const database = new BrowserDatabase(store, { compression: "raw" });
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
    const reopened = new BrowserDatabase(store, { compression: "gzip", rowsPerBlock: 2048 });
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
    expect((await reopened.listVisibleSegments(tableName)).map((segment) => segment.id)).toEqual([
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
  const compactor = new BrowserDatabase(compactorStore, { compression: "raw" });
  const writer = new BrowserDatabase(writerStore, { compression: "raw" });
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
  expect((await compactor.listVisibleSegments(tableName)).map((segment) => segment.id)).toEqual([
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
  const database = new BrowserDatabase(store);
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
  const database = new BrowserDatabase(store);
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

it("requests a best-effort buffered flush when a page becomes hidden", async () => {
  const store = new MemoryBlockStore();
  const database = new BrowserDatabase(store);
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
  const database = new BrowserDatabase(store, { rowsPerBlock: 1 });
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

it("answers metadata-only queries without loading a data block", async () => {
  const store = new CountingMemoryBlockStore();
  const database = new BrowserDatabase(store);
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
  store.segmentListCalls = 0;

  expect(await database.query("SELECT COUNT(*) AS count FROM events")).toEqual({
    columns: ["count"],
    rows: [{ count: 3 }],
  });
  expect(store.blockReadCalls).toBe(0);
  expect(store.transactionListCalls).toBe(0);
  expect(store.transactionGetCalls).toBe(1);
  expect(store.transactionBatchCalls).toBe(1);
  expect(store.segmentListCalls).toBe(1);
  store.close();
});

it("prunes numeric row groups and late-loads projected blocks after predicate selection", async () => {
  const store = new CountingMemoryBlockStore();
  const database = new BrowserDatabase(store, { compression: "raw", rowsPerBlock: 2 });
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
  const segment = (await database.listVisibleSegments("pruned_metrics"))[0];
  if (segment === undefined) throw new Error("Expected pruning segment");

  store.blockReadCalls = 0;
  store.blockIdsRead = [];
  expect(await database.query("SELECT label FROM pruned_metrics WHERE score = 7")).toEqual({
    columns: ["label"],
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
  ).toEqual({ columns: ["label"], rows: [{ label: "eight" }, { label: "seven" }] });
  expect(store.blockIdsRead.flat()).toEqual([
    ...(segment.columnBlockIds[recordedAtColumn.id] ?? []),
    (segment.columnBlockIds[labelColumn.id] ?? [])[3],
  ]);

  store.blockReadCalls = 0;
  store.blockIdsRead = [];
  expect(await database.query("SELECT score FROM pruned_metrics WHERE label = 'seven'")).toEqual({
    columns: ["score"],
    rows: [{ score: 7 }],
  });
  expect(store.blockIdsRead.flat()).toHaveLength(8);
  store.close();
});

it("shares one visibility catalog across multi-table query preparation", async () => {
  const store = new CountingMemoryBlockStore();
  const database = new BrowserDatabase(store);
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
  store.segmentListCalls = 0;

  expect(
    await database.query(
      "SELECT COUNT(*) AS count FROM left_rows l JOIN right_rows r ON r.id = l.id",
    ),
  ).toEqual({ columns: ["count"], rows: [{ count: 2 }] });
  expect(store.transactionListCalls).toBe(0);
  expect(store.transactionGetCalls).toBe(2);
  expect(store.transactionBatchCalls).toBe(1);
  expect(store.segmentListCalls).toBe(1);
  store.close();
});

for (const implementation of implementations()) {
  it(`${implementation.name} executes durable ORDER BY spill through the public query API`, async () => {
    const store = await implementation.create();
    const database = new BrowserDatabase(store, { rowsPerBlock: 256, compression: "raw" });
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

for (const implementation of implementations()) {
  it(`${implementation.name} streams append scans through a budget too small to materialize`, async () => {
    const store = await implementation.create();
    const database = new BrowserDatabase(store, { rowsPerBlock: 512, compression: "raw" });
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
      database.prepareQuery("SELECT SUM(v) AS total, COUNT(*) AS rows FROM streamed_rows", {
        executionMemoryBudgetBytes: budget,
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
  }, 30_000);
}

for (const implementation of implementations()) {
  it(`${implementation.name} streams the joined probe side with materialized build sides`, async () => {
    const store = await implementation.create();
    const database = new BrowserDatabase(store, { rowsPerBlock: 512, compression: "raw" });
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
      database.prepareQuery(
        "SELECT d.segment, SUM(o.v) AS total FROM stream_fact o JOIN stream_dim d ON d.code = o.code GROUP BY d.segment",
        { executionMemoryBudgetBytes: budget },
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
  }, 30_000);
}

for (const implementation of implementations()) {
  it(`${implementation.name} spills grouped ordered joins through value-carrying partitions`, async () => {
    const store = await implementation.create();
    const database = new BrowserDatabase(store, { rowsPerBlock: 256, compression: "raw" });
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
  it(`${implementation.name} executes CTEs and derived tables through the public API`, async () => {
    const store = await implementation.create();
    const database = new BrowserDatabase(store, { rowsPerBlock: 64, compression: "raw" });
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
    const database = new BrowserDatabase(store, { rowsPerBlock: 128, compression: "raw" });
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
    const database = new BrowserDatabase(store, {
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
    const database = new BrowserDatabase(store, { now: () => new Date(nowMs) });

    await store.createTempOwner({
      ownerId: "abandoned-query",
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
      expiresAt: "2026-01-01T01:00:00.000Z",
      revision: 0,
    });
    await store.putTempRunPage({
      ownerId: "live-query",
      runId: "run-1",
      pageIndex: 0,
      bytes: Uint8Array.of(3),
    });
    await store.putTempRunPage({
      ownerId: "orphan-query",
      runId: "run-1",
      pageIndex: 0,
      bytes: Uint8Array.of(4),
    });

    const early = await database.cleanupQuerySpill();
    expect(early).toEqual({ ownersExamined: 3, ownersReclaimed: 1, ownersRetained: 2 });
    expect(await store.getTempRunPage("orphan-query", "run-1", 0)).toBeUndefined();
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
  it(`${implementation.name} reclaims cancelled compaction output without changing current rows`, async () => {
    const store = await implementation.create();
    const database = new BrowserDatabase(store);
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

  it(`${implementation.name} caps each paged garbage-collection plan`, async () => {
    const store = await implementation.create();
    const database = new BrowserDatabase(store);
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

  it(`${implementation.name} keeps compacted history while leased and reclaims exact bytes after release`, async () => {
    const store = await implementation.create();
    const database = new BrowserDatabase(store);
    await database.createTable({
      name: "gc_leased_events",
      columns: [{ name: "value", type: "number" }],
    });
    await database.insert("gc_leased_events", { value: 1 });
    const source = await database.insert("gc_leased_events", { value: 2 });
    const sourceManifest = await store.getManifest(source.version);
    if (sourceManifest === undefined) throw new Error("Expected a source manifest");
    const sourceBytes = await Promise.all(
      sourceManifest.blockIds.map(async (id) => {
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
    expect(retained.retainedBlockCount).toBeGreaterThanOrEqual(sourceManifest.blockIds.length);
    for (const [index, id] of sourceManifest.blockIds.entries()) {
      expect(await lease.getBlock(id)).toEqual(sourceBytes[index]);
    }

    await lease.release();
    const reclaimed = await database.collectGarbage({ maxItemsPerStep: 2 });
    expect(reclaimed).toMatchObject({
      reclaimedBlockCount: sourceManifest.blockIds.length,
      physicallyReclaimedBytes: expectedReclaimedBytes,
    });
    for (const id of sourceManifest.blockIds) {
      expect(await store.getBlock(id)).toBeUndefined();
    }
    expect(await database.readTable("gc_leased_events")).toEqual([{ value: 1 }, { value: 2 }]);
    store.close();
  });

  it(`${implementation.name} holds an internal read lease across concurrent compaction and collection`, async () => {
    const store = await implementation.create();
    const writer = new BrowserDatabase(store);
    await writer.createTable({
      name: "gc_read_race_events",
      columns: [{ name: "value", type: "number" }],
    });
    await writer.insert("gc_read_race_events", { value: 1 });
    await writer.insert("gc_read_race_events", { value: 2 });
    const sourceManifest = await store.getCurrentManifest();
    if (sourceManifest === undefined) throw new Error("Expected a source manifest");

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
    const readPromise = new BrowserDatabase(readerStore).readTable("gc_read_race_events");
    await readStarted;

    try {
      expect(await store.listLeases()).toHaveLength(1);
      await writer.compactTable("gc_read_race_events", {
        targetBlockBytes: 9,
        outputCompression: "raw",
      });
      await writer.collectGarbage({ maxItemsPerStep: 1 });
      for (const id of sourceManifest.blockIds) {
        expect(await store.getBlock(id)).toBeDefined();
      }
    } finally {
      releaseRead?.();
    }

    expect(await readPromise).toEqual([{ value: 1 }, { value: 2 }]);
    expect(await store.listLeases()).toEqual([]);
    await writer.collectGarbage({ maxItemsPerStep: 1 });
    for (const id of sourceManifest.blockIds) {
      expect(await store.getBlock(id)).toBeUndefined();
    }
    store.close();
  });
}

for (const implementation of recoveryImplementations()) {
  it(`${implementation.name} resumes a bounded garbage-collection job after reopen`, async () => {
    const harness = await implementation.create();
    let store = harness.store;
    let database = new BrowserDatabase(store);
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
      state: "running",
      examinedManifestCount: 1,
      examinedSegmentCount: 0,
      examinedBlockCount: 0,
      result: null,
    });
    const jobId = progress.jobId;

    store = await harness.reopen();
    database = new BrowserDatabase(store);
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
