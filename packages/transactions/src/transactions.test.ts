import { IDBFactory } from "fake-indexeddb";
import { describe, expect, it } from "vitest";
import {
  IndexedDbBlockStore,
  MemoryBlockStore,
  SnapshotManifestMissingError,
  WriteConflictError,
  type BlockStore,
  type GarbageCollectionStepResult,
  type LeaseRecord,
  type TransactionRecord,
  type TransactionRecordUpdate,
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

async function collectStorageGarbage(
  store: BlockStore,
  input: {
    prefix: string;
    candidateManifestVersions: readonly number[];
    candidateSegmentIds?: readonly string[];
    candidateBlockIds: readonly string[];
    leaseCutoff: string;
    maxItems?: number;
  },
): Promise<GarbageCollectionStepResult> {
  let job = await store.createGarbageCollectionJob({
    id: `${input.prefix}-${crypto.randomUUID()}`,
    candidateManifestVersions: input.candidateManifestVersions,
    candidateSegmentIds: input.candidateSegmentIds ?? [],
    candidateBlockIds: input.candidateBlockIds,
    leaseCutoff: input.leaseCutoff,
    createdAt: "2026-01-01T00:00:00.000Z",
  });
  let result: GarbageCollectionStepResult | undefined;
  while (job.state !== "completed") {
    result = await store.runGarbageCollectionStep({
      jobId: job.id,
      expectedRevision: job.revision,
      maxItems: input.maxItems ?? 32,
      updatedAt: "2026-01-01T00:00:01.000Z",
    });
    job = result.job;
  }
  if (result === undefined) throw new Error("Garbage collection completed without a step");
  return result;
}

class SnapshotPinFaultStore extends FaultInjectingBlockStore {
  transactionPinAttempts = 0;
  leasePinAttempts = 0;
  rebasePinAttempts = 0;
  failNextTransactionPin = false;
  failNextLeasePin = false;
  failNextRebasePin = false;

  constructor(inner: BlockStore) {
    super(inner, () => undefined);
  }

  override async createTransaction(record: TransactionRecord): Promise<void> {
    this.transactionPinAttempts += 1;
    if (this.failNextTransactionPin && record.snapshotVersion !== null) {
      this.failNextTransactionPin = false;
      throw new SnapshotManifestMissingError(record.snapshotVersion);
    }
    return super.createTransaction(record);
  }

  override async createLease(record: LeaseRecord): Promise<void> {
    this.leasePinAttempts += 1;
    if (this.failNextLeasePin && record.manifestVersion !== null) {
      this.failNextLeasePin = false;
      throw new SnapshotManifestMissingError(record.manifestVersion);
    }
    return super.createLease(record);
  }

  override async updateTransaction(
    id: string,
    expectedRevision: number,
    update: TransactionRecordUpdate,
  ): Promise<TransactionRecord> {
    if (update.snapshotVersion !== undefined) {
      this.rebasePinAttempts += 1;
      if (this.failNextRebasePin && update.snapshotVersion !== null) {
        this.failNextRebasePin = false;
        throw new SnapshotManifestMissingError(update.snapshotVersion);
      }
    }
    return super.updateTransaction(id, expectedRevision, update);
  }
}

class LeaseRenewalRaceStore extends FaultInjectingBlockStore {
  raced = false;

  constructor(inner: BlockStore) {
    super(inner, () => undefined);
  }

  override async removeLeaseIfExpired(
    id: string,
    expectedRevision: number,
    expiresAtCutoff: string,
  ): Promise<boolean> {
    if (!this.raced) {
      this.raced = true;
      await super.renewLease(id, expectedRevision, "2026-01-01T00:10:00.000Z");
    }
    return super.removeLeaseIfExpired(id, expectedRevision, expiresAtCutoff);
  }
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
      expect(await store.listGarbageCollectionJobs()).toEqual([
        expect.objectContaining({
          state: "completed",
          retainedBlockCount: 1,
          retainedSegmentCount: 1,
          reclaimedBlockCount: 0,
        }),
      ]);
      store.close();
    });

    it("pins historical blocks with a lease and reclaims their exact bytes after release", async () => {
      const store = await implementation.create();
      const now = new Date("2026-01-01T00:00:00.000Z");
      const manager = new TransactionManager(store, {
        now: () => now,
        createId: () => "historical-reader",
      });
      const oldBytes = Uint8Array.of(1, 2, 3, 4, 5);
      await store.addBlock("historical-block", oldBytes);
      const historical = await store.publishManifest({
        expectedVersion: null,
        blockIds: ["historical-block"],
        createdAt: now.toISOString(),
      });
      const leased = await manager.openLeasedSnapshot({
        ownerId: "tab-1",
        ttlMs: 60_000,
        version: historical.version,
      });
      await store.addBlock("current-block", Uint8Array.of(9));
      await store.publishManifest({
        expectedVersion: historical.version,
        blockIds: ["current-block"],
        createdAt: "2026-01-01T00:00:01.000Z",
      });

      const retained = await collectStorageGarbage(store, {
        prefix: "lease-retains-history",
        candidateManifestVersions: [historical.version],
        candidateBlockIds: ["historical-block"],
        leaseCutoff: "2026-01-01T00:00:30.000Z",
      });
      expect(retained.job).toMatchObject({
        retainedManifestCount: 1,
        retainedBlockCount: 1,
        reclaimedBlockBytes: 0,
      });
      expect(await leased.getBlock("historical-block")).toEqual(oldBytes);

      await leased.release();
      const reclaimed = await collectStorageGarbage(store, {
        prefix: "released-history",
        candidateManifestVersions: [historical.version],
        candidateBlockIds: ["historical-block"],
        leaseCutoff: "2026-01-01T00:00:30.000Z",
      });
      expect(reclaimed.job).toMatchObject({
        prunedManifestCount: 1,
        reclaimedBlockCount: 1,
        reclaimedBlockBytes: oldBytes.byteLength,
      });
      const prunedManifest = await store.getManifest(historical.version);
      expect(prunedManifest?.version).toBe(historical.version);
      expect(typeof prunedManifest?.prunedAt).toBe("string");
      expect(await store.getBlock("historical-block")).toBeUndefined();
      await expect(manager.openSnapshot(historical.version)).rejects.toBeInstanceOf(
        SnapshotManifestMissingError,
      );
      await expect(
        manager.openLeasedSnapshot({
          ownerId: "tab-1",
          ttlMs: 60_000,
          version: historical.version,
        }),
      ).rejects.toBeInstanceOf(SnapshotManifestMissingError);
      expect((await store.getCurrentManifest())?.blockIds).toEqual(["current-block"]);
      store.close();
    });

    it("treats a lease expiring exactly at the collection cutoff as expired", async () => {
      const store = await implementation.create();
      const now = new Date("2026-01-01T00:00:00.000Z");
      const manager = new TransactionManager(store, {
        now: () => now,
        createId: () => "expiring-reader",
      });
      const oldBytes = Uint8Array.of(1, 2, 3);
      await store.addBlock("expired-history", oldBytes);
      const historical = await store.publishManifest({
        expectedVersion: null,
        blockIds: ["expired-history"],
        createdAt: now.toISOString(),
      });
      await manager.openLeasedSnapshot({
        ownerId: "tab-1",
        ttlMs: 1_000,
        version: historical.version,
      });
      await store.addBlock("new-history", Uint8Array.of(8));
      await store.publishManifest({
        expectedVersion: historical.version,
        blockIds: ["new-history"],
        createdAt: "2026-01-01T00:00:00.500Z",
      });

      const result = await collectStorageGarbage(store, {
        prefix: "expired-history",
        candidateManifestVersions: [historical.version],
        candidateBlockIds: ["expired-history"],
        leaseCutoff: "2026-01-01T00:00:01.000Z",
      });
      expect(result.job).toMatchObject({
        prunedManifestCount: 1,
        reclaimedBlockCount: 1,
        reclaimedBlockBytes: oldBytes.byteLength,
      });
      expect(await store.getBlock("expired-history")).toBeUndefined();
      store.close();
    });

    it("retains an active transaction snapshot until the transaction closes", async () => {
      const store = await implementation.create();
      const manager = new TransactionManager(store, { createId: () => "snapshot-owner" });
      const oldBytes = Uint8Array.of(1, 2, 3, 4);
      await store.addBlock("transaction-history", oldBytes);
      const historical = await store.publishManifest({
        expectedVersion: null,
        blockIds: ["transaction-history"],
        createdAt: "2026-01-01T00:00:00.000Z",
      });
      const transaction = await manager.begin();
      await store.addBlock("transaction-current", Uint8Array.of(9));
      await store.publishManifest({
        expectedVersion: historical.version,
        blockIds: ["transaction-current"],
        createdAt: "2026-01-01T00:00:01.000Z",
      });

      const retained = await collectStorageGarbage(store, {
        prefix: "active-transaction",
        candidateManifestVersions: [historical.version],
        candidateBlockIds: ["transaction-history"],
        leaseCutoff: "2026-01-01T00:01:00.000Z",
      });
      expect(retained.job).toMatchObject({
        retainedManifestCount: 1,
        retainedBlockCount: 1,
        reclaimedBlockBytes: 0,
      });
      expect(await transaction.getBlock("transaction-history")).toEqual(oldBytes);

      await transaction.abort();
      const reclaimed = await collectStorageGarbage(store, {
        prefix: "closed-transaction",
        candidateManifestVersions: [historical.version],
        candidateBlockIds: ["transaction-history"],
        leaseCutoff: "2026-01-01T00:01:00.000Z",
      });
      expect(reclaimed.job).toMatchObject({
        prunedManifestCount: 1,
        reclaimedBlockCount: 1,
        reclaimedBlockBytes: oldBytes.byteLength,
      });
      store.close();
    });

    it("retries latest transaction, lease, and rebase pins after a pruned-manifest race", async () => {
      const inner = await implementation.create();
      await inner.addBlock("pin-source", Uint8Array.of(1));
      await inner.publishManifest({ expectedVersion: null, blockIds: ["pin-source"] });
      const store = new SnapshotPinFaultStore(inner);
      const ids = ["pin-transaction", "pin-lease"];
      const manager = new TransactionManager(store, {
        createId: () => ids.shift() ?? crypto.randomUUID(),
      });

      store.failNextTransactionPin = true;
      const transaction = await manager.begin();
      expect(transaction.snapshotVersion).toBe(0);
      expect(store.transactionPinAttempts).toBe(2);

      store.failNextLeasePin = true;
      const lease = await manager.openLeasedSnapshot({ ownerId: "tab-1", ttlMs: 60_000 });
      expect(lease.version).toBe(0);
      expect(store.leasePinAttempts).toBe(2);

      store.failNextRebasePin = true;
      expect((await transaction.rebase()).version).toBe(0);
      expect(store.rebasePinAttempts).toBe(2);

      await lease.release();
      await transaction.abort();
      store.close();
    });

    it("does not retry an explicitly requested historical lease after it is pruned", async () => {
      const inner = await implementation.create();
      await inner.addBlock("explicit-pin-source", Uint8Array.of(1));
      await inner.publishManifest({ expectedVersion: null, blockIds: ["explicit-pin-source"] });
      const store = new SnapshotPinFaultStore(inner);
      const manager = new TransactionManager(store, { createId: () => "explicit-pin-lease" });
      store.failNextLeasePin = true;

      await expect(
        manager.openLeasedSnapshot({ ownerId: "tab-1", ttlMs: 60_000, version: 0 }),
      ).rejects.toBeInstanceOf(SnapshotManifestMissingError);
      expect(store.leasePinAttempts).toBe(1);
      expect(await inner.listLeases()).toEqual([]);
      store.close();
    });

    it("does not remove a lease that renews during expired-lease cleanup", async () => {
      let now = new Date("2026-01-01T00:00:00.000Z");
      const inner = await implementation.create();
      const store = new LeaseRenewalRaceStore(inner);
      const manager = new TransactionManager(store, {
        now: () => now,
        createId: () => "renewed-during-cleanup",
      });
      await manager.openLeasedSnapshot({ ownerId: "tab-1", ttlMs: 1_000 });
      now = new Date("2026-01-01T00:01:00.000Z");

      expect(await manager.removeExpiredLeases()).toEqual([]);
      expect(store.raced).toBe(true);
      expect(await inner.getLease("renewed-during-cleanup")).toMatchObject({
        revision: 1,
        expiresAt: "2026-01-01T00:10:00.000Z",
      });
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

it("reconciles a lost commit response from a tombstoned manifest descriptor", async () => {
  const inner = new MemoryBlockStore();
  let signalCommitted: (() => void) | undefined;
  const committed = new Promise<void>((resolve) => {
    signalCommitted = resolve;
  });
  let releaseResponse: (() => void) | undefined;
  const responseRelease = new Promise<void>((resolve) => {
    releaseResponse = resolve;
  });
  const faultStore = new FaultInjectingBlockStore(inner, async (point) => {
    if (point !== "afterTransactionCommit") return;
    signalCommitted?.();
    await responseRelease;
    throw new Error("commit response lost after reclamation");
  });
  const first = await new TransactionManager(faultStore, {
    createId: () => "lost-response-transaction",
  }).begin();
  await first.stageBlock("lost-response-block", Uint8Array.of(1, 2, 3));
  const firstCommit = first.commit();
  await committed;

  try {
    const successor = await new TransactionManager(inner, {
      createId: () => "successor-transaction",
    }).begin();
    await successor.stageBlock("successor-block", Uint8Array.of(4));
    successor.supersedeBlocks(["lost-response-block"]);
    await successor.commit();
    const collection = await collectStorageGarbage(inner, {
      prefix: "lost-response-history",
      candidateManifestVersions: [0],
      candidateBlockIds: ["lost-response-block"],
      leaseCutoff: "2026-01-01T00:01:00.000Z",
    });
    expect(collection.job).toMatchObject({
      prunedManifestCount: 1,
      reclaimedBlockCount: 1,
      reclaimedBlockBytes: 3,
    });
    expect(typeof (await inner.getManifest(0))?.prunedAt).toBe("string");
    expect(await inner.getBlock("lost-response-block")).toBeUndefined();
  } finally {
    releaseResponse?.();
  }

  const reconciled = await firstCommit;
  expect(reconciled).toMatchObject({
    version: 0,
    blockIds: ["lost-response-block"],
  });
  expect(typeof reconciled.prunedAt).toBe("string");
  expect(first.status).toBe("committed");
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
  expect(await store.listGarbageCollectionJobs()).toEqual([
    expect.objectContaining({
      state: "completed",
      reclaimedBlockCount: 1,
      reclaimedBlockBytes: 1,
    }),
  ]);
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
