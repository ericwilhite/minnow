import { IDBFactory } from "fake-indexeddb";
import { describe, expect, it } from "vitest";
import {
  IndexedDbBlockStore,
  MemoryBlockStore,
  WriteConflictError,
  type BlockStore,
} from "@browserdatabase/storage-idb";
import { FaultInjectingBlockStore } from "@browserdatabase/testing";
import { TransactionClosedError, TransactionManager } from "./index.js";

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

for (const implementation of implementations()) {
  describe(implementation.name, () => {
    it("keeps an older snapshot stable after a newer commit", async () => {
      const store = await implementation.create();
      const manager = new TransactionManager(store);
      const first = await manager.begin();
      await first.stageBlock("a", Uint8Array.of(1));
      await first.commit();
      const snapshot = await manager.openSnapshot();

      const second = await manager.begin();
      await second.stageBlock("b", Uint8Array.of(2));
      await second.commit();

      expect(snapshot.version).toBe(0);
      expect(snapshot.listBlockIds()).toEqual(["a"]);
      expect(await snapshot.getBlock("a")).toEqual(Uint8Array.of(1));
      expect(await snapshot.getBlock("b")).toBeUndefined();
      expect((await manager.openSnapshot()).listBlockIds()).toEqual(["a", "b"]);
      store.close();
    });

    it("allows only one competing commit and lets the loser rebase", async () => {
      const store = await implementation.create();
      const manager = new TransactionManager(store);
      const left = await manager.begin();
      const right = await manager.begin();
      await left.stageBlock("left", Uint8Array.of(1));
      await right.stageBlock("right", Uint8Array.of(2));

      const results = await Promise.allSettled([left.commit(), right.commit()]);
      expect(results.filter((result) => result.status === "fulfilled")).toHaveLength(1);
      const rejected = results.find((result) => result.status === "rejected");
      expect(rejected?.status).toBe("rejected");
      if (rejected?.status === "rejected")
        expect(rejected.reason).toBeInstanceOf(WriteConflictError);

      const loser = left.status === "active" ? left : right;
      await loser.rebase();
      const manifest = await loser.commit();
      expect(manifest.version).toBe(1);
      expect(manifest.blockIds).toEqual(["left", "right"]);
      store.close();
    });

    it("atomically replaces snapshot blocks while older snapshots retain them", async () => {
      const store = await implementation.create();
      const manager = new TransactionManager(store);
      const initial = await manager.begin();
      await initial.stageBlock("old", Uint8Array.of(1));
      await initial.commit();
      const oldSnapshot = await manager.openSnapshot();

      const replacement = await manager.begin();
      await replacement.stageBlock("new", Uint8Array.of(2));
      replacement.supersedeBlocks(["old"]);
      const manifest = await replacement.commit();

      expect(manifest.blockIds).toEqual(["new"]);
      expect(oldSnapshot.listBlockIds()).toEqual(["old"]);
      expect(await oldSnapshot.getBlock("old")).toEqual(Uint8Array.of(1));
      expect(await store.getBlock("old")).toEqual(Uint8Array.of(1));
      store.close();
    });

    it("rejects replacing a block outside the transaction snapshot", async () => {
      const store = await implementation.create();
      const transaction = await new TransactionManager(store).begin();
      await transaction.stageBlock("new", Uint8Array.of(2));
      transaction.supersedeBlocks(["not-visible"]);

      await expect(transaction.commit()).rejects.toThrow("outside the transaction snapshot");
      expect(transaction.status).toBe("active");
      store.close();
    });

    it("retains output artifacts rooted by a compaction job when recovering its stale transaction", async () => {
      let now = new Date("2026-01-01T00:00:00.000Z");
      const ids = ["source-transaction", "compaction-transaction"];
      const store = await implementation.create();
      const manager = new TransactionManager(store, {
        now: () => now,
        createId: () => ids.shift() ?? crypto.randomUUID(),
      });

      const source = await manager.begin();
      await source.stageBlock("source-block", Uint8Array.of(1));
      await source.stageSegment({
        id: "source-segment",
        tableId: "events",
        transactionId: source.id,
        rowCount: 1,
        rowIdStart: 1n,
        rowIdEndExclusive: 2n,
        columnBlockIds: { value: ["source-block"] },
        createdAt: now.toISOString(),
      });
      await source.commit();

      const compaction = await manager.begin();
      await compaction.stageBlock("compaction-output-block", Uint8Array.of(1));
      await compaction.stageSegment({
        id: "compaction-output-segment",
        tableId: "events",
        transactionId: compaction.id,
        rowCount: 1,
        rowIdStart: 1n,
        rowIdEndExclusive: 2n,
        columnBlockIds: { value: ["compaction-output-block"] },
        level: 1,
        logicalOrder: 0,
        createdAt: now.toISOString(),
      });
      await store.createCompactionJob({
        id: "compaction-job",
        tableId: "events",
        sourceManifestVersion: 0,
        sourceSegmentIds: ["source-segment"],
        sourceBlockIds: ["source-block"],
        outputBlockIds: ["compaction-output-block"],
        cursor: { sourceSegmentIndex: 1, sourceBlockIndex: 0 },
        processedRows: 1,
        sourceStoredBytes: 1,
        outputStoredBytes: 1,
        logicalBytes: 1,
        targetLevel: 1,
        state: "running",
        transactionId: compaction.id,
        outputSegmentId: "compaction-output-segment",
        publishedVersion: null,
        revision: 0,
        createdAt: now.toISOString(),
        updatedAt: now.toISOString(),
      });

      now = new Date("2026-01-01T01:00:00.000Z");
      const report = await manager.recover({
        staleBefore: new Date("2026-01-01T00:30:00.000Z"),
      });

      expect(report).toMatchObject({
        abortedTransactionIds: ["compaction-transaction"],
        removedBlockIds: [],
        retainedBlockIds: ["compaction-output-block"],
        removedSegmentIds: [],
        retainedSegmentIds: ["compaction-output-segment"],
      });
      expect((await store.getTransaction(compaction.id))?.status).toBe("aborted");
      expect(await store.getBlock("compaction-output-block")).toEqual(Uint8Array.of(1));
      expect(await store.getSegment("compaction-output-segment")).toBeDefined();
      store.close();
    });
  });
}

it("coordinates competing commits across two IndexedDB connections", async () => {
  const factory = new IDBFactory();
  const name = crypto.randomUUID();
  const firstStore = await IndexedDbBlockStore.open({ name, indexedDB: factory });
  const secondStore = await IndexedDbBlockStore.open({ name, indexedDB: factory });
  const first = await new TransactionManager(firstStore).begin();
  const second = await new TransactionManager(secondStore).begin();
  await first.stageBlock("first", Uint8Array.of(1));
  await second.stageBlock("second", Uint8Array.of(2));

  const results = await Promise.allSettled([first.commit(), second.commit()]);
  expect(results.filter((result) => result.status === "fulfilled")).toHaveLength(1);
  expect(results.filter((result) => result.status === "rejected")).toHaveLength(1);
  expect((await firstStore.getCurrentManifest())?.version).toBe(0);
  expect((await secondStore.getCurrentManifest())?.version).toBe(0);
  firstStore.close();
  secondStore.close();
});

it("reconciles a successful commit when the response is lost", async () => {
  const inner = new MemoryBlockStore();
  const store = new FaultInjectingBlockStore(inner, (point) => {
    if (point === "afterTransactionCommit") throw new Error("response lost");
  });
  const transaction = await new TransactionManager(store).begin();
  await transaction.stageBlock("saved", Uint8Array.of(1));
  const manifest = await transaction.commit();

  expect(manifest.blockIds).toEqual(["saved"]);
  expect(transaction.status).toBe("committed");
  expect((await inner.getTransaction(transaction.id))?.status).toBe("committed");
});

it("keeps a transaction active when failure happens before commit", async () => {
  const inner = new MemoryBlockStore();
  const store = new FaultInjectingBlockStore(inner, (point) => {
    if (point === "beforeTransactionCommit") throw new Error("injected before commit");
  });
  const transaction = await new TransactionManager(store).begin();
  await transaction.stageBlock("pending", Uint8Array.of(1));

  await expect(transaction.commit()).rejects.toThrow("injected before commit");
  expect(await inner.getCurrentManifest()).toBeUndefined();
  expect((await inner.getTransaction(transaction.id))?.status).toBe("active");
  expect(await inner.getBlock("pending")).toEqual(Uint8Array.of(1));
});

it("aborts stale transactions and removes only their unreachable blocks", async () => {
  let now = new Date("2026-01-01T00:00:00Z");
  let nextId = "stale";
  const store = new MemoryBlockStore();
  const manager = new TransactionManager(store, {
    now: () => now,
    createId: () => nextId,
  });
  const stale = await manager.begin();
  await stale.stageBlock("orphan", Uint8Array.of(1));

  now = new Date("2026-01-01T01:00:00Z");
  nextId = "live";
  const live = await manager.begin();
  await live.stageBlock("still-in-use", Uint8Array.of(2));
  const report = await manager.recover({
    staleBefore: new Date("2026-01-01T00:30:00Z"),
  });

  expect(report).toEqual({
    abortedTransactionIds: ["stale"],
    skippedTransactionIds: [],
    removedBlockIds: ["orphan"],
    retainedBlockIds: [],
    removedSegmentIds: [],
    retainedSegmentIds: [],
  });
  expect(await store.getBlock("orphan")).toBeUndefined();
  expect(await store.getBlock("still-in-use")).toEqual(Uint8Array.of(2));
  expect((await store.getTransaction("stale"))?.status).toBe("aborted");
  expect((await store.getTransaction("live"))?.status).toBe("active");
});

it("resumes an active transaction and reconciles an existing immutable block", async () => {
  const store = new MemoryBlockStore();
  const manager = new TransactionManager(store, { createId: () => "resumable" });
  const transaction = await manager.begin();
  await store.addBlock("written-before-checkpoint", Uint8Array.of(1, 2, 3));

  const resumed = await manager.resume(transaction.id);
  await resumed.stageExistingBlocks(["written-before-checkpoint", "written-before-checkpoint"]);
  expect(resumed.pendingBlockIds).toEqual(["written-before-checkpoint"]);
  await expect(resumed.stageExistingBlocks(["missing"])).rejects.toThrow("missing existing block");

  await store.addSegment({
    id: "written-segment-before-checkpoint",
    tableId: "table",
    transactionId: transaction.id,
    rowCount: 1,
    rowIdStart: 1n,
    rowIdEndExclusive: 2n,
    columnBlockIds: { value: ["written-before-checkpoint"] },
    createdAt: "2026-01-01T00:00:00.000Z",
  });
  await resumed.stageExistingSegment("written-segment-before-checkpoint");
  expect(resumed.pendingSegmentIds).toEqual(["written-segment-before-checkpoint"]);

  const manifest = await resumed.commit();
  expect(manifest.blockIds).toEqual(["written-before-checkpoint"]);
  await expect(manager.resume(transaction.id)).rejects.toBeInstanceOf(TransactionClosedError);
  store.close();
});

it("pins a snapshot with a renewable lease and releases it", async () => {
  let now = new Date("2026-01-01T00:00:00Z");
  const ids = ["transaction", "lease"];
  const store = new MemoryBlockStore();
  const manager = new TransactionManager(store, {
    now: () => now,
    createId: () => ids.shift() ?? crypto.randomUUID(),
  });
  const transaction = await manager.begin();
  await transaction.stageBlock("one", Uint8Array.of(1));
  await transaction.commit();

  const snapshot = await manager.openLeasedSnapshot({ ownerId: "tab-1", ttlMs: 1_000 });
  expect(snapshot.version).toBe(0);
  expect((await store.getLease("lease"))?.manifestVersion).toBe(0);

  now = new Date("2026-01-01T00:00:00.500Z");
  expect((await snapshot.renew(2_000)).toISOString()).toBe("2026-01-01T00:00:02.500Z");
  expect(await manager.removeExpiredLeases(new Date("2026-01-01T00:00:02Z"))).toEqual([]);
  await snapshot.release();
  expect(await store.getLease("lease")).toBeUndefined();
});

it("removes stale segment metadata along with unreachable blocks", async () => {
  let now = new Date("2026-01-01T00:00:00Z");
  const store = new MemoryBlockStore();
  const manager = new TransactionManager(store, { now: () => now, createId: () => "stale" });
  const transaction = await manager.begin();
  await transaction.stageBlock("orphan", Uint8Array.of(1));
  await transaction.stageSegment({
    id: "segment",
    tableId: "table",
    transactionId: transaction.id,
    rowCount: 1,
    rowIdStart: 1n,
    rowIdEndExclusive: 2n,
    columnBlockIds: { column: ["orphan"] },
    createdAt: now.toISOString(),
  });
  now = new Date("2026-01-01T01:00:00Z");
  const report = await manager.recover({ staleBefore: new Date("2026-01-01T00:30:00Z") });
  expect(report.removedSegmentIds).toEqual(["segment"]);
  expect(await store.getSegment("segment")).toBeUndefined();
});
