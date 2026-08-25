import { IDBFactory } from "fake-indexeddb";
import { describe, expect, it, vi } from "vitest";
import {
  IndexedDbBlockStore,
  MemoryBlockStore,
  MAX_TRANSACTION_PENDING_BLOCKS,
  OpfsBlockStore,
  SnapshotManifestMissingError,
  WriteConflictError,
  type BeginTransactionInput,
  type BeginTransactionResult,
  type BlockStore,
  type GarbageCollectionStepResult,
  type LeaseRecord,
  type SegmentRecord,
  type StageTransactionArtifactsInput,
  type TransactionRecord,
  type TransactionRecordUpdate,
} from "../storage/index.js";
import { FaultInjectingBlockStore } from "../testing/index.js";
import { MemoryOpfs } from "../testing/opfs-shim.js";
import { TransactionClosedError, TransactionManager } from "./index.js";

function implementations(): Array<{ name: string; create: () => Promise<BlockStore> }> {
  return [
    { name: "memory", create: async () => new MemoryBlockStore() },
    {
      name: "indexeddb",
      create: async () =>
        IndexedDbBlockStore.open({ name: crypto.randomUUID(), indexedDB: new IDBFactory() }),
    },
    {
      name: "opfs",
      create: async () =>
        OpfsBlockStore.open({ name: crypto.randomUUID(), root: new MemoryOpfs().root }),
    },
  ];
}

async function manifestBlockIds(store: BlockStore, version: number): Promise<string[]> {
  const ids: string[] = [];
  let afterBlockId: string | null = null;
  for (;;) {
    const page = await store.listManifestBlockPage({ version, afterBlockId, limit: 256 });
    ids.push(...page.records.map(({ blockId }) => blockId));
    if (page.nextCursor === null) return ids;
    afterBlockId = page.nextCursor;
  }
}

async function currentManifestBlockIds(store: BlockStore): Promise<string[]> {
  const version = await store.getCurrentManifestVersion();
  return version === null ? [] : manifestBlockIds(store, version);
}

async function transactionRecords(store: BlockStore): Promise<TransactionRecord[]> {
  const records: TransactionRecord[] = [];
  let afterId: string | null = null;
  for (;;) {
    const page = await store.listTransactionPage(afterId, 256);
    records.push(...page.records);
    if (page.nextCursor === null) return records;
    afterId = page.nextCursor;
  }
}

async function addSegmentTable(store: BlockStore, id = "table"): Promise<void> {
  await store.addTable({
    id,
    name: id,
    columns: [
      { id: "value", name: "value", type: "number", nullable: false },
      { id: "column", name: "column", type: "number", nullable: false },
    ],
    managed: false,
    revision: 0,
    createdAt: "2026-01-01T00:00:00.000Z",
  });
}

async function commitTableBlock(
  store: BlockStore,
  tableId: string,
  blockId: string,
  bytes: Uint8Array,
): Promise<number> {
  const transaction = await new TransactionManager(store, {
    createId: () => `txn-${blockId}`,
  }).begin();
  await transaction.stageBlock(blockId, bytes);
  await transaction.stageSegment({
    id: `segment-${blockId}`,
    tableId,
    transactionId: transaction.id,
    rowCount: 1,
    rowIdStart: 1n,
    rowIdEndExclusive: 2n,
    columnBlockIds: { value: [blockId] },
    kind: "insert",
    level: 0,
    logicalOrder: 0,
    commitOrdinal: 0,
    rowIdSpans: [],
    createdAt: "2026-01-01T00:00:00.000Z",
  });
  return (await transaction.commit()).version;
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

  override async beginTransaction(input: BeginTransactionInput): Promise<BeginTransactionResult> {
    this.transactionPinAttempts += 1;
    const version = await super.getCurrentManifestVersion();
    if (this.failNextTransactionPin && version !== null) {
      this.failNextTransactionPin = false;
      throw new SnapshotManifestMissingError(version);
    }
    return super.beginTransaction(input);
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

class ArtifactBatchObservingStore extends MemoryBlockStore {
  readonly artifactBatchBlockCounts: number[] = [];

  override async stageTransactionArtifacts(input: StageTransactionArtifactsInput) {
    this.artifactBatchBlockCounts.push(input.blocks.length);
    return super.stageTransactionArtifacts(input);
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
      await super.renewLease({
        id,
        expectedRevision,
        expiresAtCutoff: "2026-01-01T00:00:00.000Z",
        expiresAt: "2026-01-01T00:10:00.000Z",
      });
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
      expect(await snapshot.hasBlocks(["a", "b"])).toEqual([true, false]);
      expect(await snapshot.getBlock("a")).toEqual(Uint8Array.of(1));
      expect(await snapshot.getBlock("b")).toBeUndefined();
      expect(await (await manager.openSnapshot()).hasBlocks(["a", "b"])).toEqual([true, true]);
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
      expect(await manifestBlockIds(store, manifest.version)).toEqual(["left", "right"]);
      store.close();
    });

    it("commits a deferred transaction in one step, and a refused one leaves nothing", async () => {
      const store = await implementation.create();
      await addSegmentTable(store);
      const manager = new TransactionManager(store);
      const segmentFor = (transactionId: string, blockId: string) => ({
        id: `segment-${blockId}`,
        tableId: "table",
        transactionId,
        rowCount: 1,
        rowIdStart: 1n,
        rowIdEndExclusive: 2n,
        columnBlockIds: { value: [blockId] },
        kind: "insert" as const,
        level: 0,
        logicalOrder: 0,
        commitOrdinal: 0,
        rowIdSpans: [],
        createdAt: "2026-01-01T00:00:00.000Z",
      });

      const left = await manager.beginDeferred();
      const right = await manager.beginDeferred();
      const leftManifest = await left.stageArtifactsAndCommit(
        [{ id: "left", bytes: Uint8Array.of(1) }],
        [segmentFor(left.id, "left")],
      );
      expect(leftManifest.version).toBe(0);
      expect(left.status).toBe("committed");
      expect(await store.getTransaction(left.id)).toMatchObject({
        status: "committed",
        committedVersion: 0,
        snapshotVersion: null,
        pendingBlockIds: ["left"],
        pendingSegmentIds: ["segment-left"],
      });
      expect((await store.getSegment("segment-left"))?.logicalOrder).toBe(0);

      // The loser's single-shot write refuses as a typed conflict and wrote nothing at all —
      // not even its record; a rebase and the same call again then commits on top.
      await expect(
        right.stageArtifactsAndCommit(
          [{ id: "right", bytes: Uint8Array.of(2) }],
          [segmentFor(right.id, "right")],
        ),
      ).rejects.toBeInstanceOf(WriteConflictError);
      expect(right.status).toBe("active");
      expect(await store.getTransaction(right.id)).toBeUndefined();
      expect(await store.getBlock("right")).toBeUndefined();
      expect(await store.getSegment("segment-right")).toBeUndefined();
      await right.rebase();
      const rightManifest = await right.stageArtifactsAndCommit(
        [{ id: "right", bytes: Uint8Array.of(2) }],
        [segmentFor(right.id, "right")],
      );
      expect(rightManifest.version).toBe(1);
      expect(await manifestBlockIds(store, 1)).toEqual(["left", "right"]);

      // An aborted deferred transaction that staged nothing never touched the store.
      const abandoned = await manager.beginDeferred();
      await abandoned.abort();
      expect(abandoned.status).toBe("aborted");
      expect((await transactionRecords(store)).map((record) => record.id).sort()).toEqual(
        [left.id, right.id].sort(),
      );
      store.close();
    });

    it("moves a leased snapshot to a newer version in place", async () => {
      const store = await implementation.create();
      const ids = ["txn-a", "reader", "txn-b", "reader-2"];
      const manager = new TransactionManager(store, {
        createId: () => ids.shift() ?? crypto.randomUUID(),
      });
      const first = await manager.begin();
      await first.stageBlock("a", Uint8Array.of(1));
      await first.commit();
      const pinned = await manager.openLeasedSnapshot({ ownerId: "tab-1", ttlMs: 60_000 });
      expect(pinned.version).toBe(0);
      const second = await manager.begin();
      await second.stageBlock("b", Uint8Array.of(2));
      await second.commit();

      // Moving to a version with no manifest refuses and leaves the snapshot open and pinned.
      await expect(
        manager.moveLeasedSnapshot(pinned, { ownerId: "tab-1", ttlMs: 60_000, version: 99 }),
      ).rejects.toBeInstanceOf(SnapshotManifestMissingError);
      expect(await pinned.getBlock("a")).toEqual(Uint8Array.of(1));

      const moved = await manager.moveLeasedSnapshot(pinned, {
        id: "reader-2",
        ownerId: "tab-1",
        ttlMs: 60_000,
        version: 1,
      });
      expect(moved.version).toBe(1);
      expect(await moved.hasBlocks(["a", "b"])).toEqual([true, true]);
      expect(await moved.getBlock("b")).toEqual(Uint8Array.of(2));
      await expect(pinned.getBlock("a")).rejects.toThrow("released");
      // One record, re-pinned and renewed — never a second one to remove later.
      const leases = await store.listLeases();
      expect(leases.map((lease) => [lease.id, lease.manifestVersion, lease.revision])).toEqual([
        ["reader", 1, 1],
      ]);
      await moved.renew(60_000);
      expect((await store.getLease("reader"))?.revision).toBe(2);
      await moved.release();
      expect(await store.listLeases()).toEqual([]);

      // A lease the store swept meanwhile is replaced by a fresh one rather than failing.
      const swept = await manager.openLeasedSnapshot({
        id: "swept",
        ownerId: "tab-1",
        ttlMs: 60_000,
      });
      await store.removeLease({ id: "swept", ownerId: "tab-1" });
      const replacement = await manager.moveLeasedSnapshot(swept, {
        id: "fresh",
        ownerId: "tab-1",
        ttlMs: 60_000,
        version: 1,
      });
      expect(replacement.version).toBe(1);
      expect((await store.listLeases()).map((lease) => lease.id)).toEqual(["fresh"]);
      await expect(swept.getBlock("a")).rejects.toThrow("released");
      store.close();
    });

    it("retains output artifacts rooted by a compaction job when recovering its stale transaction", async () => {
      let now = new Date("2026-01-01T00:00:00.000Z");
      const ids = ["source-transaction", "compaction-transaction"];
      const store = await implementation.create();
      await addSegmentTable(store, "events");
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
        kind: "insert",
        level: 0,
        logicalOrder: 0,
        commitOrdinal: 0,
        rowIdSpans: [],
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
        kind: "base",
        level: 1,
        logicalOrder: 0,
        commitOrdinal: 0,
        rowIdSpans: [],
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
        rewritePlan: { kind: "copy-v1" },
        outputCursor: null,
        memoryBudgetBytes: 0,
        minimumMemoryBytes: 0,
        level0SourceStoredBytes: 1,
        anchorSourceStoredBytes: 0,
        peakWorkingBytes: 0,
        outputLogicalBytes: 1,
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
      await addSegmentTable(store, "history");
      const historicalVersion = await commitTableBlock(
        store,
        "history",
        "historical-block",
        oldBytes,
      );
      const leased = await manager.openLeasedSnapshot({
        ownerId: "tab-1",
        ttlMs: 60_000,
        version: historicalVersion,
      });
      await store.dropTable({
        tableId: "history",
        expectedTableRevision: 0,
        expectedManifestVersion: historicalVersion,
        expectedCatalogEpoch: (await store.getCatalogProbe()).catalogEpoch,
        committedAt: "2026-01-01T00:00:01.000Z",
      });

      const retained = await collectStorageGarbage(store, {
        prefix: "lease-retains-history",
        candidateManifestVersions: [historicalVersion],
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
        candidateManifestVersions: [historicalVersion],
        candidateBlockIds: ["historical-block"],
        leaseCutoff: "2026-01-01T00:00:30.000Z",
      });
      expect(reclaimed.job).toMatchObject({
        prunedManifestCount: 1,
        reclaimedBlockCount: 1,
        reclaimedBlockBytes: oldBytes.byteLength,
      });
      const prunedManifest = await store.getManifest(historicalVersion);
      expect(prunedManifest?.version).toBe(historicalVersion);
      expect(typeof prunedManifest?.prunedAt).toBe("string");
      expect(await store.getBlock("historical-block")).toBeUndefined();
      await expect(manager.openSnapshot(historicalVersion)).rejects.toBeInstanceOf(
        SnapshotManifestMissingError,
      );
      await expect(
        manager.openLeasedSnapshot({
          ownerId: "tab-1",
          ttlMs: 60_000,
          version: historicalVersion,
        }),
      ).rejects.toBeInstanceOf(SnapshotManifestMissingError);
      expect(await currentManifestBlockIds(store)).toEqual([]);
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
      await addSegmentTable(store, "history");
      const historicalVersion = await commitTableBlock(
        store,
        "history",
        "expired-history",
        oldBytes,
      );
      await manager.openLeasedSnapshot({
        ownerId: "tab-1",
        ttlMs: 1_000,
        version: historicalVersion,
      });
      await store.dropTable({
        tableId: "history",
        expectedTableRevision: 0,
        expectedManifestVersion: historicalVersion,
        expectedCatalogEpoch: (await store.getCatalogProbe()).catalogEpoch,
        committedAt: "2026-01-01T00:00:00.500Z",
      });

      const result = await collectStorageGarbage(store, {
        prefix: "expired-history",
        candidateManifestVersions: [historicalVersion],
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
      await addSegmentTable(store, "history");
      const historicalVersion = await commitTableBlock(
        store,
        "history",
        "transaction-history",
        oldBytes,
      );
      const transaction = await manager.begin();
      await store.dropTable({
        tableId: "history",
        expectedTableRevision: 0,
        expectedManifestVersion: historicalVersion,
        expectedCatalogEpoch: (await store.getCatalogProbe()).catalogEpoch,
        committedAt: "2026-01-01T00:00:01.000Z",
      });

      const retained = await collectStorageGarbage(store, {
        prefix: "active-transaction",
        candidateManifestVersions: [historicalVersion],
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
        candidateManifestVersions: [historicalVersion],
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
      const bootstrap = await new TransactionManager(inner, {
        createId: () => "pin-source-transaction",
      }).begin();
      await bootstrap.stageBlock("pin-source", Uint8Array.of(1));
      await bootstrap.commit();
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
      const bootstrap = await new TransactionManager(inner, {
        createId: () => "explicit-pin-source-transaction",
      }).begin();
      await bootstrap.stageBlock("explicit-pin-source", Uint8Array.of(1));
      await bootstrap.commit();
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

it("chunks large staging calls and refuses an oversized journal before mutation", async () => {
  const store = new ArtifactBatchObservingStore();
  const transaction = await new TransactionManager(store).begin();
  const blocks = Array.from({ length: 129 }, (_, index) => ({
    id: `chunk-${String(index)}`,
    bytes: Uint8Array.of(index & 0xff),
  }));
  await transaction.stageBlocks(blocks);
  expect(store.artifactBatchBlockCounts).toEqual([64, 64, 1]);
  expect((await store.getTransaction(transaction.id))?.pendingBlockIds).toHaveLength(129);

  const tooMany = Array.from({ length: MAX_TRANSACTION_PENDING_BLOCKS + 1 }, (_, index) => ({
    id: `too-many-${String(index)}`,
    bytes: Uint8Array.of(1),
  }));
  const callsBeforeRefusal = store.artifactBatchBlockCounts.length;
  const pendingBeforeRefusal = (await store.getTransaction(transaction.id))?.pendingBlockIds;
  await expect(transaction.stageBlocks(tooMany)).rejects.toBeInstanceOf(RangeError);
  expect(store.artifactBatchBlockCounts).toHaveLength(callsBeforeRefusal);
  expect((await store.getTransaction(transaction.id))?.pendingBlockIds).toEqual(
    pendingBeforeRefusal,
  );
  store.close();
});

it("rejects malformed checkpoints and transaction artifact intents before durable mutation", async () => {
  const store = new MemoryBlockStore();
  const manager = new TransactionManager(store, {
    createId: (() => {
      let next = 0;
      return () => `validation-${String((next += 1))}`;
    })(),
  });
  const transaction = await manager.begin();
  const initial = transaction.checkpoint();
  expect(transaction.pendingTableNextRowId).toBeUndefined();
  expect(transaction.compactionSourceBlockIds).toEqual([]);
  await expect(transaction.stageBlock("", Uint8Array.of(1))).rejects.toThrow(
    "Block ID cannot be empty",
  );

  await transaction.stageBlock("later", Uint8Array.of(1));
  const future = transaction.checkpoint();
  await transaction.rollbackTo(initial);
  await expect(transaction.rollbackTo(future)).rejects.toThrow(
    "transaction checkpoint is no longer reachable",
  );
  const other = await manager.begin();
  await expect(other.rollbackTo(initial)).rejects.toThrow(
    "transaction checkpoint belongs to another transaction",
  );

  const wrongOwner = {
    id: "wrong-owner-segment",
    tableId: "table",
    transactionId: other.id,
    rowCount: 0,
    rowIdStart: 0n,
    rowIdEndExclusive: 0n,
    columnBlockIds: {},
    kind: "insert",
    level: 0,
    logicalOrder: 0,
    commitOrdinal: 0,
    rowIdSpans: [],
    createdAt: "2026-01-01T00:00:00.000Z",
  } satisfies SegmentRecord;
  await expect(transaction.stageArtifacts([], [wrongOwner])).rejects.toThrow(
    "belongs to another transaction",
  );
  await expect(transaction.stageSegment(wrongOwner)).rejects.toThrow(
    "belongs to another transaction",
  );
  await expect(transaction.stageExistingSegment("missing-segment")).rejects.toThrow(
    "Cannot stage a missing existing segment",
  );

  expect(() => transaction.setCompactionIntent("job", [])).toThrow(
    "must name at least one source block",
  );
  expect(() => transaction.setCompactionIntent("job", ["source", "source"])).toThrow(
    "source block IDs must be unique",
  );
  expect(() => transaction.setCompactionIntent("", ["source"])).toThrow(
    "Compaction job ID cannot be empty",
  );
  transaction.setCompactionIntent("job", ["source-b", "source-a"]);
  expect(transaction.compactionSourceBlockIds).toEqual(["source-a", "source-b"]);
  transaction.setCompactionIntent("job", ["source-a", "source-b"]);
  expect(() => transaction.setCompactionIntent("another-job", ["source-a", "source-b"])).toThrow(
    "cannot change its compaction retirement intent",
  );
  expect(() => transaction.limitLevelZeroSegments("", 1)).toThrow("Table ID cannot be empty");
  for (const limit of [0, -1, Number.NaN, Number.POSITIVE_INFINITY]) {
    expect(() => transaction.limitLevelZeroSegments("table", limit)).toThrow(
      "positive safe integer",
    );
  }
  transaction.limitLevelZeroSegments("table", 4);
  transaction.limitLevelZeroSegments("table", 2);

  const adopted = { ...wrongOwner, transactionId: transaction.id };
  const activeOwner = await store.getTransaction(other.id);
  if (activeOwner === undefined) throw new Error("Expected the active owner record");
  await expect(transaction.adoptAbortedCompactionSegment(adopted, activeOwner)).rejects.toThrow(
    "owner is not aborted",
  );

  const ownPending = await manager.begin();
  await ownPending.stageBlock("own-pending", Uint8Array.of(1));
  expect(() => ownPending.setCompactionIntent("job", ["own-pending"])).toThrow(
    "cannot retire its own pending block",
  );
  await Promise.all([transaction.abort(), other.abort(), ownPending.abort()]);
  store.close();
});

it("exposes the scalar snapshot presence and leased-snapshot identity APIs", async () => {
  const store = new MemoryBlockStore();
  const manager = new TransactionManager(store, { createId: () => crypto.randomUUID() });
  const transaction = await manager.begin();
  await transaction.stageBlock("present", Uint8Array.of(1));
  const manifest = await transaction.commit();
  const snapshot = await manager.openSnapshot(manifest.version);
  expect(await snapshot.hasBlock("present")).toBe(true);
  expect(await snapshot.hasBlock("absent")).toBe(false);

  const leased = await manager.openLeasedSnapshot({ ownerId: "coverage-owner", ttlMs: 1_000 });
  expect(leased.leaseId).not.toHaveLength(0);
  await leased.release();
  store.close();
});

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

it("coordinates competing commits across two OPFS instances on one directory", async () => {
  // The same shape as the IndexedDB two-connection test: two independent store instances over
  // one storage root, racing commits — this exercises the command log's sequence-handle CAS.
  const shim = new MemoryOpfs();
  const name = crypto.randomUUID();
  const firstStore = await OpfsBlockStore.open({ name, root: shim.root });
  const secondStore = await OpfsBlockStore.open({ name, root: shim.root });
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

  expect(await manifestBlockIds(store, manifest.version)).toEqual(["saved"]);
  expect(transaction.status).toBe("committed");
  expect((await inner.getTransaction(transaction.id))?.status).toBe("committed");
});

it("reconciles a lost commit response from a tombstoned manifest descriptor", async () => {
  const inner = new MemoryBlockStore();
  await addSegmentTable(inner, "lost-response-table");
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
  await first.stageSegment({
    id: "lost-response-segment",
    tableId: "lost-response-table",
    transactionId: first.id,
    rowCount: 1,
    rowIdStart: 1n,
    rowIdEndExclusive: 2n,
    columnBlockIds: { value: ["lost-response-block"] },
    kind: "insert",
    level: 0,
    logicalOrder: 0,
    commitOrdinal: 0,
    rowIdSpans: [],
    createdAt: "2026-01-01T00:00:00.000Z",
  });
  const firstCommit = first.commit();
  await committed;

  try {
    await inner.dropTable({
      tableId: "lost-response-table",
      expectedTableRevision: 0,
      expectedManifestVersion: 0,
      expectedCatalogEpoch: (await inner.getCatalogProbe()).catalogEpoch,
      committedAt: "2026-01-01T00:00:01.000Z",
    });
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
    liveBlockCount: 1,
    liveBlockBytes: 3,
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
    // Recovery is governed by the durable owner deadline, not the transaction start time.
    // The first owner has expired at this cutoff; the second was created at it and remains live.
    staleBefore: new Date("2026-01-01T01:00:00Z"),
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

it("resumes an active transaction with its atomically staged artifacts", async () => {
  const store = new MemoryBlockStore();
  await addSegmentTable(store);
  const manager = new TransactionManager(store, { createId: () => "resumable" });
  const transaction = await manager.begin();
  await store.stageTransactionArtifacts({
    transactionId: transaction.id,
    expectedRevision: 0,
    blocks: [{ id: "written-before-checkpoint", bytes: Uint8Array.of(1, 2, 3) }],
    segments: [
      {
        id: "written-segment-before-checkpoint",
        tableId: "table",
        transactionId: transaction.id,
        rowCount: 1,
        rowIdStart: 1n,
        rowIdEndExclusive: 2n,
        columnBlockIds: { value: ["written-before-checkpoint"] },
        kind: "insert",
        level: 0,
        logicalOrder: 0,
        commitOrdinal: 0,
        rowIdSpans: [],
        createdAt: "2026-01-01T00:00:00.000Z",
      },
    ],
    updatedAt: "2026-01-01T00:00:00.000Z",
  });

  const resumed = await manager.resume(transaction.id);
  expect(resumed.pendingBlockIds).toEqual(["written-before-checkpoint"]);
  expect(resumed.pendingSegmentIds).toEqual(["written-segment-before-checkpoint"]);
  await expect(resumed.stageExistingBlocks(["missing"])).rejects.toThrow("missing existing block");

  const manifest = await resumed.commit();
  expect(await manifestBlockIds(store, manifest.version)).toEqual(["written-before-checkpoint"]);
  await expect(manager.resume(transaction.id)).rejects.toBeInstanceOf(TransactionClosedError);
  store.close();
});

it("falls back to the two-step shapes on a store without the single-shot write or lease move", async () => {
  // The fault-injecting proxy deliberately lacks writeTransaction and moveLease.
  const store = new FaultInjectingBlockStore(new MemoryBlockStore(), () => undefined);
  await addSegmentTable(store);
  const ids = ["txn-left", "txn-right", "reader", "txn-c", "reader-2"];
  const manager = new TransactionManager(store, {
    createId: () => ids.shift() ?? crypto.randomUUID(),
  });
  const segmentFor = (transactionId: string, blockId: string) => ({
    id: `segment-${blockId}`,
    tableId: "table",
    transactionId,
    rowCount: 1,
    rowIdStart: 1n,
    rowIdEndExclusive: 2n,
    columnBlockIds: { value: [blockId] },
    kind: "insert" as const,
    level: 0,
    logicalOrder: 0,
    commitOrdinal: 0,
    rowIdSpans: [],
    createdAt: "2026-01-01T00:00:00.000Z",
  });
  const left = await manager.beginDeferred();
  const right = await manager.beginDeferred();
  expect(await transactionRecords(store)).toEqual([]);
  await left.stageArtifactsAndCommit(
    [{ id: "left", bytes: Uint8Array.of(1) }],
    [segmentFor("txn-left", "left")],
  );
  expect(await store.getTransaction("txn-left")).toMatchObject({
    status: "committed",
    committedVersion: 0,
  });
  // Staged in two steps, the loser's artifacts are journaled when its commit refuses; the
  // retry after the rebase commits them rather than staging them a second time.
  await expect(
    right.stageArtifactsAndCommit(
      [{ id: "right", bytes: Uint8Array.of(2) }],
      [segmentFor("txn-right", "right")],
    ),
  ).rejects.toBeInstanceOf(WriteConflictError);
  expect(right.pendingBlockIds).toEqual(["right"]);
  await right.rebase();
  const manifest = await right.stageArtifactsAndCommit(
    [{ id: "right", bytes: Uint8Array.of(2) }],
    [segmentFor("txn-right", "right")],
  );
  expect(manifest.version).toBe(1);
  expect(await manifestBlockIds(store, 1)).toEqual(["left", "right"]);

  const pinned = await manager.openLeasedSnapshot({ ownerId: "tab-1", ttlMs: 60_000, version: 0 });
  const moved = await manager.moveLeasedSnapshot(pinned, {
    id: "reader-2",
    ownerId: "tab-1",
    ttlMs: 60_000,
    version: 1,
  });
  expect(moved.version).toBe(1);
  expect(await moved.hasBlocks(["left", "right"])).toEqual([true, true]);
  // Create at the new version, then remove the old pin: the same end state, two round trips.
  expect((await store.listLeases()).map((lease) => [lease.id, lease.manifestVersion])).toEqual([
    ["reader-2", 1],
  ]);
  await expect(pinned.getBlock("left")).rejects.toThrow("released");
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
  await addSegmentTable(store);
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
    kind: "insert",
    level: 0,
    logicalOrder: 0,
    commitOrdinal: 0,
    rowIdSpans: [],
    createdAt: now.toISOString(),
  });
  now = new Date("2026-01-01T01:00:00Z");
  const report = await manager.recover({ staleBefore: new Date("2026-01-01T00:30:00Z") });
  expect(report.removedSegmentIds).toEqual(["segment"]);
  expect(await store.getSegment("segment")).toBeUndefined();
});

it("renews a legitimately idle live transaction without retaining an abandoned reference", async () => {
  vi.useFakeTimers();
  vi.setSystemTime(new Date("2026-01-01T00:00:00.000Z"));
  try {
    const store = new MemoryBlockStore();
    const renewals = vi.spyOn(store, "renewTransaction");
    let reachable = true;
    const manager = new TransactionManager(store, {
      now: () => new Date(Date.now()),
      createId: () => "weak-owner",
      transactionTtlMs: 30,
      createWeakRef: (transaction) => ({
        deref: () => (reachable ? transaction : undefined),
      }),
    });
    const transaction = await manager.begin();
    await vi.advanceTimersByTimeAsync(25);
    expect(renewals).toHaveBeenCalledTimes(2);
    expect(
      Date.parse((await store.getTransaction(transaction.id))?.expiresAt ?? ""),
    ).toBeGreaterThan(Date.now());

    reachable = false;
    const beforeAbandon = renewals.mock.calls.length;
    await vi.advanceTimersByTimeAsync(100);
    expect(renewals).toHaveBeenCalledTimes(beforeAbandon);
    expect(
      Date.parse((await store.getTransaction(transaction.id))?.expiresAt ?? ""),
    ).toBeLessThanOrEqual(Date.now());
    store.close();
  } finally {
    vi.useRealTimers();
  }
});

it("never resumes an active transaction after its durable ownership deadline", async () => {
  let now = new Date("2026-01-01T00:00:00.000Z");
  const store = new MemoryBlockStore();
  const manager = new TransactionManager(store, {
    now: () => now,
    createId: () => "expired-owner",
    transactionTtlMs: 10,
  });
  const transaction = await manager.begin();
  now = new Date("2026-01-01T00:00:00.010Z");
  await expect(manager.resume(transaction.id)).rejects.toBeInstanceOf(TransactionClosedError);
  expect((await store.getTransaction(transaction.id))?.expiresAt).toBe("2026-01-01T00:00:00.010Z");
  store.close();
});

it("bounds and incrementally merges transaction full-text deltas without partial registration", async () => {
  const store = new MemoryBlockStore();
  await store.addTable({
    id: "fts-delta-table",
    name: "fts_delta_table",
    columns: [{ id: "body", name: "body", type: "string", nullable: false }],
    managed: false,
    ftsColumns: {
      body: {
        storage: "fts-chunks-v1",
        tokenizerVersion: 1,
        state: "ready",
        buildFromVersion: -1,
      },
    },
    revision: 0,
    createdAt: "2026-01-01T00:00:00.000Z",
  });
  const transaction = await new TransactionManager(store).begin();
  expect(() =>
    transaction.setFtsChanges({
      tableId: "fts-delta-table",
      columns: [
        {
          columnId: "body",
          postings: [{ term: "broken", rowIds: [1n], tf: [] }],
          totalTokens: 1,
        },
      ],
    }),
  ).toThrow("row and frequency counts differ");
  expect(transaction.stagedWorkCount).toBe(0);

  for (let index = 0; index < 100; index += 1) {
    transaction.setFtsChanges({
      tableId: "fts-delta-table",
      columns: [
        {
          columnId: "body",
          postings: [{ term: "shared", rowIds: [BigInt(index + 1)], tf: [1] }],
          totalTokens: 1,
        },
      ],
    });
  }
  expect(transaction.stagedWorkCount).toBe(1);
  const version = (await transaction.commit()).version;
  const candidates = await store.readFtsCandidates(
    "fts-delta-table",
    "body",
    [{ term: "shared", prefix: false }],
    version,
  );
  expect(candidates.rowIdsByTerm[0]).toEqual(
    Array.from({ length: 100 }, (_, index) => BigInt(index + 1)),
  );
  store.close();
});

it("hands a deferred scope from its lease to its transaction before lease removal settles", async () => {
  const store = new MemoryBlockStore();
  await addSegmentTable(store);
  const removeLease = store.removeLease.bind(store);
  let signalRemovalStarted!: () => void;
  const removalStarted = new Promise<void>((resolve) => {
    signalRemovalStarted = resolve;
  });
  let allowRemovalResponse!: () => void;
  const removalResponse = new Promise<void>((resolve) => {
    allowRemovalResponse = resolve;
  });
  let signalRemovalFinished!: () => void;
  const removalFinished = new Promise<void>((resolve) => {
    signalRemovalFinished = resolve;
  });
  vi.spyOn(store, "removeLease").mockImplementation(async (input) => {
    // Model a durable delete whose acknowledgement is delayed. Once the underlying removal
    // returns, any attempt to renew the old in-memory lease would conflict with the store.
    const removed = await removeLease(input);
    signalRemovalStarted();
    await removalResponse;
    signalRemovalFinished();
    return removed;
  });

  const manager = new TransactionManager(store, {
    createId: () => "lease-handoff",
    transactionTtlMs: 30,
  });
  const transaction = await manager.beginDeferred();
  const segment = (suffix: string, blockId: string, rowId: bigint): SegmentRecord => ({
    id: `lease-handoff-${suffix}`,
    tableId: "table",
    transactionId: transaction.id,
    rowCount: 1,
    rowIdStart: rowId,
    rowIdEndExclusive: rowId + 1n,
    columnBlockIds: { value: [blockId] },
    kind: "insert",
    level: 0,
    logicalOrder: 0,
    commitOrdinal: 0,
    rowIdSpans: [],
    createdAt: "2026-01-01T00:00:00.000Z",
  });

  await transaction.stageArtifacts(
    [{ id: "lease-handoff-a", bytes: Uint8Array.of(1) }],
    [segment("a", "lease-handoff-a", 1n)],
  );
  await transaction.stageArtifacts(
    [{ id: "lease-handoff-b", bytes: Uint8Array.of(2) }],
    [segment("b", "lease-handoff-b", 2n)],
  );
  await removalStarted;
  expect(await store.getLease("lease-handoff/snapshot")).toBeUndefined();
  expect(await store.getTransaction(transaction.id)).toMatchObject({ status: "active" });

  // The retiring lease is detached synchronously. Forced ownership checks now renew the durable
  // transaction instead of racing the already-applied lease removal, and commit does not wait for
  // the delayed removal acknowledgement.
  await expect(transaction.renew()).resolves.toBeUndefined();
  const manifest = await transaction.commit();
  expect(await manifestBlockIds(store, manifest.version)).toEqual([
    "lease-handoff-a",
    "lease-handoff-b",
  ]);

  allowRemovalResponse();
  await removalFinished;
  expect(await store.listLeases()).toEqual([]);
  store.close();
});

it("serializes a snapshot rebase behind an in-flight durable lease renewal", async () => {
  const store = new MemoryBlockStore();
  const ids = ["renew-move-scope", "renew-move-writer"];
  const manager = new TransactionManager(store, {
    createId: () => ids.shift() ?? crypto.randomUUID(),
  });
  const transaction = await manager.beginDeferred();
  const writer = await manager.begin();
  await writer.stageBlock("renew-move-block", Uint8Array.of(1));
  await writer.commit();

  const renewLease = store.renewLease.bind(store);
  let signalRenewed!: () => void;
  const durablyRenewed = new Promise<void>((resolve) => {
    signalRenewed = resolve;
  });
  let releaseRenewResponse!: () => void;
  const renewResponse = new Promise<void>((resolve) => {
    releaseRenewResponse = resolve;
  });
  vi.spyOn(store, "renewLease").mockImplementation(async (input) => {
    const record = await renewLease(input);
    signalRenewed();
    await renewResponse;
    return record;
  });

  const renewing = transaction.renew();
  await durablyRenewed;
  // The adapter revision has advanced but the lease object cannot see the response yet. Rebase
  // must wait instead of issuing moveLease with the stale revision.
  const rebasing = transaction.rebase();
  releaseRenewResponse();
  await expect(renewing).resolves.toBeUndefined();
  await expect(rebasing).resolves.toMatchObject({ version: 0 });
  expect(transaction.snapshotVersion).toBe(0);
  expect(
    (await store.listLeases()).map(({ manifestVersion, ownerId }) => ({
      manifestVersion,
      ownerId,
    })),
  ).toEqual([{ manifestVersion: 0, ownerId: "renew-move-scope/owner" }]);

  await transaction.abort();
  expect(await store.listLeases()).toEqual([]);
  store.close();
});

it("lets an unacknowledged terminal lease cleanup expire without changing the outcome", async () => {
  let now = new Date("2026-01-01T00:00:00.000Z");
  const store = new MemoryBlockStore();
  const removal = vi
    .spyOn(store, "removeLease")
    .mockRejectedValue(new Error("injected lease cleanup refusal"));
  const manager = new TransactionManager(store, {
    now: () => now,
    createId: () => "terminal-lease-cleanup",
    transactionTtlMs: 30,
  });
  const transaction = await manager.beginDeferred();

  // Abort is a terminal local decision even if deleting its deadline-bounded pin is temporarily
  // unavailable. Collection can remove the abandoned lease once that deadline passes.
  await expect(transaction.abort()).resolves.toBeUndefined();
  expect(await store.listLeases()).toHaveLength(1);
  now = new Date("2026-01-01T00:00:00.031Z");
  removal.mockRestore();
  await manager.removeExpiredLeases(now);
  expect(await store.listLeases()).toEqual([]);
  store.close();
});
