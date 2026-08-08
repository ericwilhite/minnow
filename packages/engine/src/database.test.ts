import { decodeBlock } from "@browserdatabase/block-format";
import { IDBFactory } from "fake-indexeddb";
import { describe, expect, it } from "vitest";
import {
  IndexedDbBlockStore,
  MemoryBlockStore,
  type BlockStore,
  type CompactionJobRecord,
  type CompactionJobRecordUpdate,
} from "@browserdatabase/storage-idb";
import { FaultInjectingBlockStore } from "@browserdatabase/testing";
import { TransactionManager } from "@browserdatabase/transactions";
import {
  attachLifecycleFlush,
  BrowserDatabase,
  MissingKeyError,
  UniqueConstraintError,
} from "./database.js";

class CountingMemoryBlockStore extends MemoryBlockStore {
  blockWriteCalls = 0;
  blockReadCalls = 0;
  blockIdsRead: string[][] = [];

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
    outputBlockCount: 4,
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

  let progress = await database.compactTableStep("events", { maxBlocks: 1 });
  expect(progress).toMatchObject({
    state: "running",
    processedRows: 1,
    sourceSegmentCount: 3,
    sourceBlockCount: 3,
    outputBlockCount: 1,
    result: null,
  });
  expect((await database.listCompactionJobs("events"))[0]).toMatchObject({
    cursor: { sourceSegmentIndex: 1, sourceBlockIndex: 0 },
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
  const progress = await database.compactTableStep("events", { maxBlocks: 1 });
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

    const progress = await database.compactTableStep("events", { maxBlocks: 1 });
    if (progress.jobId === null) throw new Error("Expected a persisted compaction job");
    const interrupted = await store.getCompactionJob(progress.jobId);
    expect(interrupted).toMatchObject({
      state: "running",
      cursor: { sourceSegmentIndex: 1, sourceBlockIndex: 0 },
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

  const progress = await database.compactTableStep("events", { maxBlocks: 1 });
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
  const manifestCount = (await store.listManifests()).length;
  const transactionCount = (await store.listTransactions()).length;

  const first = await new BrowserDatabase(store).resumeCompactionJob(result.jobId);
  const second = await new BrowserDatabase(store).resumeCompactionJob(result.jobId);

  expect(first).toEqual(second);
  expect(first).toMatchObject({
    state: "published",
    result: { jobId: result.jobId, version: result.version },
  });
  expect((await store.getCompactionJob(result.jobId))?.revision).toBe(published.revision);
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

it("skips mutation segments until delta-aware compaction is available", async () => {
  const store = new MemoryBlockStore();
  const database = new BrowserDatabase(store);
  await database.createTable({
    name: "accounts",
    uniqueKey: "email",
    columns: [
      { name: "email", type: "string" },
      { name: "score", type: "number" },
    ],
  });
  await database.insert("accounts", { email: "a@example.com", score: 1 });
  await database.update("accounts", "a@example.com", { score: 2 });

  const result = await database.compactTable("accounts");

  expect(result).toMatchObject({
    compacted: false,
    skipReason: "contains-mutation-segments",
    sourceSegmentCount: 2,
    version: 1,
  });
  expect(await database.readTable("accounts")).toEqual([{ email: "a@example.com", score: 2 }]);
  store.close();
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
