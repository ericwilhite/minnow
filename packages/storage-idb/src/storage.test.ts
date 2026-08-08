import { IDBFactory } from "fake-indexeddb";
import { describe, expect, it } from "vitest";
import {
  CompactionJobConflictError,
  GarbageCollectionJobConflictError,
  IndexedDbBlockStore,
  LeaseConflictError,
  MemoryBlockStore,
  SnapshotManifestMissingError,
  UniqueKeyConflictError,
  type BlockStore,
  type CompactionJobRecord,
  type CompactionJobRecordUpdate,
  type CommitTransactionInput,
  type TransactionRecord,
  type TransactionRecordUpdate,
} from "./index.js";

function stores(): Array<{ name: string; create: () => Promise<BlockStore> }> {
  return [
    { name: "memory", create: async () => new MemoryBlockStore() },
    {
      name: "indexeddb",
      create: async () =>
        IndexedDbBlockStore.open({ name: crypto.randomUUID(), indexedDB: new IDBFactory() }),
    },
  ];
}

function activeTransaction(id: string): TransactionRecord {
  const createdAt = "2026-01-01T00:00:00.000Z";
  return {
    id,
    snapshotVersion: null,
    pendingBlockIds: [],
    pendingSegmentIds: [],
    status: "active",
    revision: 0,
    startedAt: createdAt,
    updatedAt: createdAt,
    committedVersion: null,
  };
}

function rechunkCompactionJob(id = "rechunk-job"): CompactionJobRecord {
  return {
    id,
    tableId: "events",
    sourceManifestVersion: 7,
    sourceSegmentIds: ["segment-1", "segment-2"],
    sourceBlockIds: ["name-block", "id-block-2", "id-block-1"],
    outputBlockIds: [],
    cursor: { sourceSegmentIndex: 0, sourceBlockIndex: 0 },
    processedRows: 0,
    sourceStoredBytes: 360,
    outputStoredBytes: 0,
    logicalBytes: 300,
    rewritePlan: {
      kind: "rechunk-v1",
      targetBlockBytes: 2 * 1024 * 1024,
      outputCompression: "gzip",
      totalRows: 4,
      rowIdStart: 10n,
      rowIdEndExclusive: 14n,
      logicalOrder: 5,
      columns: [
        {
          columnId: "id-column",
          type: "number",
          sourceBlocks: [
            {
              blockId: "id-block-1",
              rowStart: 0,
              rowCount: 2,
              storedBytes: 100,
              encodedBytes: 80,
              checksum: 11,
            },
            {
              blockId: "id-block-2",
              rowStart: 2,
              rowCount: 2,
              storedBytes: 110,
              encodedBytes: 90,
              checksum: 12,
            },
          ],
        },
        {
          columnId: "name-column",
          type: "string",
          sourceBlocks: [
            {
              blockId: "name-block",
              rowStart: 0,
              rowCount: 4,
              storedBytes: 150,
              encodedBytes: 130,
              checksum: 13,
            },
          ],
        },
      ],
      outputs: [
        { rowStart: 0, rowCount: 3 },
        { rowStart: 3, rowCount: 1 },
      ],
    },
    memoryBudgetBytes: 4096,
    minimumMemoryBytes: 512,
    peakWorkingBytes: 0,
    outputLogicalBytes: 0,
    targetLevel: 1,
    state: "planned",
    transactionId: null,
    outputSegmentId: `${id}/output-segment`,
    publishedVersion: null,
    revision: 0,
    createdAt: "2026-01-01T00:00:00.000Z",
    updatedAt: "2026-01-01T00:00:00.000Z",
  };
}

async function createReadyCompaction(
  store: BlockStore,
  prefix = "cancellation",
  transactionState: Pick<TransactionRecord, "status" | "committedVersion"> = {
    status: "active",
    committedVersion: null,
  },
): Promise<{ job: CompactionJobRecord; commit: CommitTransactionInput }> {
  const createdAt = "2026-01-01T00:00:00.000Z";
  const sourceBlockId = `${prefix}/source-block`;
  const outputBlockId = `${prefix}/output-block`;
  const transactionId = `${prefix}/transaction`;
  const outputSegmentId = `${prefix}/output-segment`;
  await store.addBlock(sourceBlockId, Uint8Array.of(1));
  const previous = await store.getCurrentManifest();
  const sourceManifest = await store.publishManifest({
    expectedVersion: previous?.version ?? null,
    blockIds: [...(previous?.blockIds ?? []), sourceBlockId],
    createdAt,
  });
  await store.addBlock(outputBlockId, Uint8Array.of(2));
  await store.addSegment({
    id: outputSegmentId,
    tableId: "events",
    transactionId,
    rowCount: 1,
    rowIdStart: 1n,
    rowIdEndExclusive: 2n,
    columnBlockIds: { value: [outputBlockId] },
    level: 1,
    logicalOrder: sourceManifest.version,
    createdAt,
  });
  await store.createTransaction({
    id: transactionId,
    snapshotVersion: sourceManifest.version,
    pendingBlockIds: [outputBlockId],
    pendingSegmentIds: [outputSegmentId],
    status: transactionState.status,
    revision: 0,
    startedAt: createdAt,
    updatedAt: createdAt,
    committedVersion: transactionState.committedVersion,
  });
  const job: CompactionJobRecord = {
    id: `${prefix}/job`,
    tableId: "events",
    sourceManifestVersion: sourceManifest.version,
    sourceSegmentIds: [`${prefix}/source-segment`],
    sourceBlockIds: [sourceBlockId],
    outputBlockIds: [outputBlockId],
    cursor: { sourceSegmentIndex: 1, sourceBlockIndex: 0 },
    processedRows: 1,
    sourceStoredBytes: 1,
    outputStoredBytes: 1,
    logicalBytes: 1,
    targetLevel: 1,
    state: "ready",
    transactionId,
    outputSegmentId,
    publishedVersion: null,
    revision: 0,
    createdAt,
    updatedAt: createdAt,
    error: "prior transient failure",
  };
  await store.createCompactionJob(job);
  return {
    job,
    commit: {
      transactionId,
      expectedTransactionRevision: 0,
      expectedManifestVersion: sourceManifest.version,
      blockIds: [
        ...sourceManifest.blockIds.filter((blockId) => blockId !== sourceBlockId),
        outputBlockId,
      ],
      removedBlockIds: [sourceBlockId],
      committedAt: "2026-01-01T00:00:01.000Z",
    },
  };
}

async function createSupersededStorage(store: BlockStore, prefix: string): Promise<void> {
  const timestamp = "2026-01-01T00:00:00.000Z";
  const oldBlockId = `${prefix}/old-block`;
  const oldSegmentId = `${prefix}/old-segment`;
  const oldTransactionId = `${prefix}/old-transaction`;
  await store.addBlock(oldBlockId, Uint8Array.of(1, 2, 3));
  await store.addSegment({
    id: oldSegmentId,
    tableId: "events",
    transactionId: oldTransactionId,
    rowCount: 1,
    rowIdStart: 1n,
    rowIdEndExclusive: 2n,
    columnBlockIds: { value: [oldBlockId] },
    createdAt: timestamp,
  });
  await store.createTransaction({
    ...activeTransaction(oldTransactionId),
    pendingBlockIds: [oldBlockId],
    pendingSegmentIds: [oldSegmentId],
  });
  await store.commitTransaction({
    transactionId: oldTransactionId,
    expectedTransactionRevision: 0,
    expectedManifestVersion: null,
    blockIds: [oldBlockId],
    committedAt: timestamp,
  });

  const currentBlockId = `${prefix}/current-block`;
  const currentSegmentId = `${prefix}/current-segment`;
  const currentTransactionId = `${prefix}/current-transaction`;
  await store.addBlock(currentBlockId, Uint8Array.of(4, 5));
  await store.addSegment({
    id: currentSegmentId,
    tableId: "events",
    transactionId: currentTransactionId,
    rowCount: 1,
    rowIdStart: 1n,
    rowIdEndExclusive: 2n,
    columnBlockIds: { value: [currentBlockId] },
    createdAt: timestamp,
  });
  await store.createTransaction({
    ...activeTransaction(currentTransactionId),
    snapshotVersion: 0,
    pendingBlockIds: [currentBlockId],
    pendingSegmentIds: [currentSegmentId],
  });
  await store.commitTransaction({
    transactionId: currentTransactionId,
    expectedTransactionRevision: 0,
    expectedManifestVersion: 0,
    blockIds: [currentBlockId],
    removedBlockIds: [oldBlockId],
    committedAt: "2026-01-01T00:00:01.000Z",
  });
}

async function createSupersededGarbageCollectionJob(
  store: BlockStore,
  prefix: string,
  leaseCutoff = "2026-01-01T00:10:00.000Z",
) {
  return store.createGarbageCollectionJob({
    id: `${prefix}/gc`,
    candidateManifestVersions: [0],
    candidateSegmentIds: [`${prefix}/old-segment`],
    candidateBlockIds: [`${prefix}/old-block`],
    leaseCutoff,
    createdAt: "2026-01-01T00:02:00.000Z",
  });
}

for (const implementation of stores()) {
  describe(implementation.name, () => {
    it("stores immutable blocks and defensive copies", async () => {
      const store = await implementation.create();
      const source = Uint8Array.of(1, 2, 3);
      await store.addBlock("a", source);
      source[0] = 9;
      expect(await store.getBlock("a")).toEqual(Uint8Array.of(1, 2, 3));
      await expect(store.addBlock("a", Uint8Array.of(4))).rejects.toThrow();
      store.close();
    });

    it("writes and reads blocks in bulk without partial duplicate batches", async () => {
      const store = await implementation.create();
      await store.addBlocks([
        { id: "a", bytes: Uint8Array.of(1) },
        { id: "b", bytes: Uint8Array.of(2) },
      ]);
      expect(await store.getBlocks(["b", "missing", "a"])).toEqual([
        Uint8Array.of(2),
        undefined,
        Uint8Array.of(1),
      ]);
      await expect(
        store.addBlocks([
          { id: "c", bytes: Uint8Array.of(3) },
          { id: "c", bytes: Uint8Array.of(4) },
        ]),
      ).rejects.toThrow("already exists");
      expect(await store.getBlock("c")).toBeUndefined();
      store.close();
    });

    it("publishes manifests with compare-and-swap", async () => {
      const store = await implementation.create();
      await store.addBlock("a", Uint8Array.of(1));
      const first = await store.publishManifest({ expectedVersion: null, blockIds: ["a"] });
      expect(first.version).toBe(0);
      await expect(
        store.publishManifest({ expectedVersion: null, blockIds: ["a"] }),
      ).rejects.toThrow("expected null");
      expect((await store.getCurrentManifest())?.version).toBe(0);
      store.close();
    });

    it("never publishes a missing block", async () => {
      const store = await implementation.create();
      await expect(
        store.publishManifest({ expectedVersion: null, blockIds: ["missing"] }),
      ).rejects.toThrow("missing block");
      expect(await store.getCurrentManifest()).toBeUndefined();
      store.close();
    });

    it("stores table and segment records", async () => {
      const store = await implementation.create();
      await store.addTable({
        id: "people-id",
        name: "people",
        columns: [{ id: "name-id", name: "name", type: "string", nullable: false }],
        createdAt: "2026-01-01T00:00:00.000Z",
      });
      await expect(
        store.addTable({
          id: "another-id",
          name: "people",
          columns: [{ id: "age-id", name: "age", type: "number", nullable: false }],
          createdAt: "2026-01-01T00:00:00.000Z",
        }),
      ).rejects.toThrow("name already exists");
      await store.addSegment({
        id: "segment-1",
        tableId: "people-id",
        transactionId: "transaction-1",
        rowCount: 2,
        rowIdStart: 1n,
        rowIdEndExclusive: 3n,
        columnBlockIds: { "name-id": ["block-1"] },
        createdAt: "2026-01-01T00:00:00.000Z",
      });
      await store.addSegment({
        id: "segment-2",
        tableId: "people-id",
        transactionId: "transaction-2",
        rowCount: 1,
        rowIdStart: 3n,
        rowIdEndExclusive: 4n,
        columnBlockIds: { "name-id": ["block-2"] },
        level: 1,
        logicalOrder: 4,
        createdAt: "2026-01-01T00:00:01.000Z",
      });

      expect((await store.listTables())[0]?.name).toBe("people");
      expect((await store.listSegments("people-id"))[0]?.rowIdEndExclusive).toBe(3n);
      expect(await store.getSegment("segment-1")).not.toHaveProperty("level");
      expect(await store.getSegment("segment-2")).toMatchObject({ level: 1, logicalOrder: 4 });
      store.close();
    });

    it("atomically stamps committed segments with stable logical order", async () => {
      const store = await implementation.create();
      const timestamp = "2026-01-01T00:00:00.000Z";
      await store.addBlock("segment-block", Uint8Array.of(1));
      await store.addSegment({
        id: "committed-segment",
        tableId: "events",
        transactionId: "segment-transaction",
        rowCount: 1,
        rowIdStart: 1n,
        rowIdEndExclusive: 2n,
        columnBlockIds: { value: ["segment-block"] },
        createdAt: timestamp,
      });
      await store.createTransaction({
        id: "segment-transaction",
        snapshotVersion: null,
        pendingBlockIds: ["segment-block"],
        pendingSegmentIds: ["committed-segment"],
        status: "active",
        revision: 0,
        startedAt: timestamp,
        updatedAt: timestamp,
        committedVersion: null,
      });

      await store.commitTransaction({
        transactionId: "segment-transaction",
        expectedTransactionRevision: 0,
        expectedManifestVersion: null,
        blockIds: ["segment-block"],
        committedAt: timestamp,
      });

      expect(await store.getSegment("committed-segment")).toMatchObject({
        level: 0,
        logicalOrder: 0,
      });
      store.close();
    });

    it("prevents generic transaction updates from forging a commit", async () => {
      const store = await implementation.create();
      const created = activeTransaction("forged-commit");
      await store.createTransaction(created);
      const before = await store.getTransaction(created.id);
      const forgedUpdates: TransactionRecordUpdate[] = [
        {
          status: "committed",
          updatedAt: "2026-01-01T00:00:01.000Z",
        },
        {
          committedVersion: 99,
          updatedAt: "2026-01-01T00:00:02.000Z",
        },
        {
          committedVersion: null,
          updatedAt: "2026-01-01T00:00:03.000Z",
        },
      ];

      for (const update of forgedUpdates) {
        await expect(store.updateTransaction(created.id, created.revision, update)).rejects.toThrow(
          "commitTransaction",
        );
        expect(await store.getTransaction(created.id)).toEqual(before);
      }
      expect(await store.getCurrentManifest()).toBeUndefined();
      store.close();
    });

    it("prevents an aborted transaction from being reactivated or mutated", async () => {
      const store = await implementation.create();
      const created = activeTransaction("terminal-abort");
      await store.createTransaction(created);
      const aborted = await store.updateTransaction(created.id, created.revision, {
        status: "aborted",
        updatedAt: "2026-01-01T00:00:01.000Z",
      });

      await expect(
        store.updateTransaction(created.id, aborted.revision, {
          status: "active",
          updatedAt: "2026-01-01T00:00:02.000Z",
        }),
      ).rejects.toThrow("Only active transactions can be updated; found aborted");
      await expect(
        store.updateTransaction(created.id, aborted.revision, {
          pendingBlockIds: ["late-block"],
          updatedAt: "2026-01-01T00:00:03.000Z",
        }),
      ).rejects.toThrow("Only active transactions can be updated; found aborted");
      expect(await store.getTransaction(created.id)).toEqual(aborted);
      store.close();
    });

    it("rejects creating a transaction whose pending segment metadata is missing", async () => {
      const store = await implementation.create();
      const timestamp = "2026-01-01T00:00:00.000Z";
      await store.addBlock("segment-block", Uint8Array.of(1));
      await expect(
        store.createTransaction({
          id: "missing-segment-transaction",
          snapshotVersion: null,
          pendingBlockIds: ["segment-block"],
          pendingSegmentIds: ["missing-segment"],
          status: "active",
          revision: 0,
          startedAt: timestamp,
          updatedAt: timestamp,
          committedVersion: null,
        }),
      ).rejects.toThrow("missing pending segment");
      expect(await store.getTransaction("missing-segment-transaction")).toBeUndefined();
      store.close();
    });

    it("reserves non-overlapping internal row ID ranges", async () => {
      const store = await implementation.create();
      const [first, second] = await Promise.all([
        store.reserveRowIds("people", 3),
        store.reserveRowIds("people", 2),
      ]);
      expect([first, second].sort((left, right) => (left.start < right.start ? -1 : 1))).toEqual([
        { start: 1n, endExclusive: 4n },
        { start: 4n, endExclusive: 6n },
      ]);
      store.close();
    });

    it("creates and renews persistent leases", async () => {
      const store = await implementation.create();
      await store.createLease({
        id: "reader-1",
        kind: "reader",
        manifestVersion: null,
        ownerId: "tab-1",
        expiresAt: "2026-01-01T00:01:00.000Z",
        revision: 0,
      });
      const renewed = await store.renewLease("reader-1", 0, "2026-01-01T00:02:00.000Z");
      expect(renewed.revision).toBe(1);
      await expect(store.renewLease("reader-1", 0, renewed.expiresAt)).rejects.toThrow(
        "expected revision 0",
      );
      await store.removeLease("reader-1");
      expect(await store.listLeases()).toEqual([]);
      store.close();
    });

    it("persists resumable compaction jobs with revision conflicts", async () => {
      const store = await implementation.create();
      const created: CompactionJobRecord = {
        id: "job-b",
        tableId: "events",
        sourceManifestVersion: 7,
        sourceSegmentIds: ["segment-b", "segment-a", "segment-b"],
        sourceBlockIds: ["block-b", "block-a", "block-b"],
        outputBlockIds: [],
        cursor: { sourceSegmentIndex: 0, sourceBlockIndex: 0 },
        processedRows: 0,
        sourceStoredBytes: 0,
        outputStoredBytes: 0,
        logicalBytes: 0,
        targetLevel: 1,
        state: "planned",
        transactionId: null,
        outputSegmentId: null,
        publishedVersion: null,
        revision: 0,
        createdAt: "2026-01-01T00:00:01.000Z",
        updatedAt: "2026-01-01T00:00:01.000Z",
      };
      await store.createCompactionJob(created);
      created.sourceSegmentIds[0] = "mutated-segment";
      created.cursor.sourceBlockIndex = 99;

      expect(await store.getCompactionJob("job-b")).toMatchObject({
        sourceSegmentIds: ["segment-b", "segment-a"],
        sourceBlockIds: ["block-a", "block-b"],
        cursor: { sourceSegmentIndex: 0, sourceBlockIndex: 0 },
        rewritePlan: { kind: "copy-v1" },
        outputCursor: null,
        memoryBudgetBytes: 0,
        minimumMemoryBytes: 0,
        peakWorkingBytes: 0,
        outputLogicalBytes: 0,
      });

      await store.createCompactionJob({
        ...created,
        id: "job-a",
        tableId: "accounts",
        sourceSegmentIds: ["account-segment"],
        sourceBlockIds: ["account-block"],
        cursor: { sourceSegmentIndex: 0, sourceBlockIndex: 0 },
        createdAt: "2026-01-01T00:00:00.000Z",
        updatedAt: "2026-01-01T00:00:00.000Z",
      });
      expect((await store.listCompactionJobs()).map((job) => job.id)).toEqual(["job-a", "job-b"]);
      expect((await store.listCompactionJobs("events")).map((job) => job.id)).toEqual(["job-b"]);

      const outputBlockIds = ["output-b", "output-a", "output-b"];
      const cursor = { sourceSegmentIndex: 1, sourceBlockIndex: 8 };
      const updated = await store.updateCompactionJob("job-b", 0, {
        outputBlockIds,
        cursor,
        processedRows: 128,
        state: "running",
        transactionId: "transaction-9",
        outputSegmentId: "output-segment",
        error: "interrupted",
        updatedAt: "2026-01-01T00:00:02.000Z",
      });
      outputBlockIds[0] = "mutated-output";
      cursor.sourceBlockIndex = 999;
      expect(updated).toMatchObject({
        outputBlockIds: ["output-a", "output-b"],
        cursor: { sourceSegmentIndex: 1, sourceBlockIndex: 8 },
        processedRows: 128,
        state: "running",
        transactionId: "transaction-9",
        outputSegmentId: "output-segment",
        revision: 1,
        error: "interrupted",
      });
      await expect(
        store.updateCompactionJob("job-b", 0, {
          state: "aborted",
          updatedAt: "2026-01-01T00:00:03.000Z",
        }),
      ).rejects.toBeInstanceOf(CompactionJobConflictError);
      expect((await store.getCompactionJob("job-b"))?.revision).toBe(1);

      const recovered = await store.updateCompactionJob("job-b", 1, {
        state: "ready",
        cursor: { sourceSegmentIndex: 2, sourceBlockIndex: 0 },
        error: null,
        updatedAt: "2026-01-01T00:00:04.000Z",
      });
      expect(recovered).not.toHaveProperty("error");
      expect(recovered.revision).toBe(2);

      await store.removeCompactionJob("job-b");
      expect(await store.getCompactionJob("job-b")).toBeUndefined();
      store.close();
    });

    it("atomically cancels a compaction and aborts its active transaction", async () => {
      const store = await implementation.create();
      const { job, commit } = await createReadyCompaction(store);
      const cancelledAt = "2026-01-01T00:00:02.000Z";

      const cancelled = await store.cancelCompactionJob(job.id, job.revision, cancelledAt);

      expect(cancelled).toMatchObject({
        state: "cancelled",
        revision: 1,
        updatedAt: cancelledAt,
        outputBlockIds: job.outputBlockIds,
        outputSegmentId: job.outputSegmentId,
      });
      expect(cancelled).not.toHaveProperty("error");
      expect(await store.getTransaction(commit.transactionId)).toMatchObject({
        status: "aborted",
        revision: 1,
        updatedAt: cancelledAt,
        pendingBlockIds: job.outputBlockIds,
        pendingSegmentIds: [job.outputSegmentId],
      });
      expect(await store.getBlock(job.outputBlockIds[0] ?? "")).toEqual(Uint8Array.of(2));
      expect(await store.getSegment(job.outputSegmentId ?? "")).toBeDefined();
      await expect(store.commitTransaction(commit)).rejects.toThrow("changed");
      expect((await store.getCurrentManifest())?.version).toBe(job.sourceManifestVersion);
      store.close();
    });

    it("reconciles cancellation to publication when the linked commit wins", async () => {
      const store = await implementation.create();
      const { job, commit } = await createReadyCompaction(store, "commit-wins");

      const [manifest, reconciled] = await Promise.all([
        store.commitTransaction(commit),
        store.cancelCompactionJob(job.id, job.revision, "2026-01-01T00:00:02.000Z"),
      ]);

      expect(reconciled).toMatchObject({
        state: "published",
        publishedVersion: manifest.version,
        revision: 1,
      });
      expect(reconciled).not.toHaveProperty("error");
      expect(await store.getTransaction(commit.transactionId)).toMatchObject({
        status: "committed",
        committedVersion: manifest.version,
        revision: 1,
      });
      expect((await store.getCurrentManifest())?.version).toBe(manifest.version);
      store.close();
    });

    it("treats terminal compaction cancellation as an exact no-op", async () => {
      const store = await implementation.create();
      const { job } = await createReadyCompaction(store, "terminal");
      const cancelled = await store.cancelCompactionJob(
        job.id,
        job.revision,
        "2026-01-01T00:00:02.000Z",
      );
      const transactionBefore = await store.getTransaction(job.transactionId ?? "");
      expect(
        await store.cancelCompactionJob(job.id, cancelled.revision, "2026-01-01T00:00:03.000Z"),
      ).toEqual(cancelled);
      expect(await store.getTransaction(job.transactionId ?? "")).toEqual(transactionBefore);

      for (const state of ["published", "aborted"] as const) {
        const terminalJob: CompactionJobRecord = {
          ...job,
          id: `${state}-terminal-job`,
          transactionId: `${state}-terminal-transaction`,
          state,
          publishedVersion: state === "published" ? 9 : null,
          revision: 4,
          updatedAt: "2026-01-01T00:00:04.000Z",
        };
        await store.createCompactionJob(terminalJob);
        const before = await store.getCompactionJob(terminalJob.id);
        const after = await store.cancelCompactionJob(
          terminalJob.id,
          terminalJob.revision,
          "2026-01-01T00:00:05.000Z",
        );
        expect(after).toEqual(before);
      }
      store.close();
    });

    it("rejects stale cancellation without changing either record", async () => {
      const store = await implementation.create();
      const { job } = await createReadyCompaction(store, "stale-cancel");
      const transactionBefore = await store.getTransaction(job.transactionId ?? "");

      await expect(
        store.cancelCompactionJob(job.id, job.revision + 1, "2026-01-01T00:00:02.000Z"),
      ).rejects.toBeInstanceOf(CompactionJobConflictError);

      expect(await store.getCompactionJob(job.id)).toMatchObject({
        state: "ready",
        revision: job.revision,
        error: "prior transient failure",
      });
      expect(await store.getTransaction(job.transactionId ?? "")).toEqual(transactionBefore);
      store.close();
    });

    it("prevents generic checkpoint updates from bypassing atomic cancellation", async () => {
      const store = await implementation.create();
      const { job } = await createReadyCompaction(store, "generic-cancel");
      const transactionBefore = await store.getTransaction(job.transactionId ?? "");

      await expect(
        store.updateCompactionJob(job.id, job.revision, {
          state: "cancelled",
          error: null,
          updatedAt: "2026-01-01T00:00:02.000Z",
        }),
      ).rejects.toThrow("Use cancelCompactionJob");

      expect(await store.getCompactionJob(job.id)).toMatchObject({
        state: "ready",
        revision: job.revision,
        error: "prior transient failure",
      });
      expect(await store.getTransaction(job.transactionId ?? "")).toEqual(transactionBefore);
      store.close();
    });

    it("cancels safely when the linked transaction is missing or already aborted", async () => {
      const store = await implementation.create();
      const { job } = await createReadyCompaction(store, "inactive-transaction");
      const abortedTransaction = await store.updateTransaction(job.transactionId ?? "", 0, {
        status: "aborted",
        updatedAt: "2026-01-01T00:00:01.000Z",
      });

      const cancelled = await store.cancelCompactionJob(
        job.id,
        job.revision,
        "2026-01-01T00:00:02.000Z",
      );
      expect(cancelled.state).toBe("cancelled");
      expect(await store.getTransaction(job.transactionId ?? "")).toEqual(abortedTransaction);

      const missingTransactionJob: CompactionJobRecord = {
        ...job,
        id: "missing-transaction-job",
        transactionId: "missing-transaction",
      };
      await store.createCompactionJob(missingTransactionJob);
      expect(
        await store.cancelCompactionJob(
          missingTransactionJob.id,
          missingTransactionJob.revision,
          "2026-01-01T00:00:03.000Z",
        ),
      ).toMatchObject({ state: "cancelled", revision: 1 });
      expect(await store.getTransaction("missing-transaction")).toBeUndefined();
      store.close();
    });

    it("fails closed when a committed transaction has no manifest version", async () => {
      const store = await implementation.create();
      const { job } = await createReadyCompaction(store, "invalid-commit", {
        status: "committed",
        committedVersion: null,
      });
      const invalidTransaction = await store.getTransaction(job.transactionId ?? "");

      await expect(
        store.cancelCompactionJob(job.id, job.revision, "2026-01-01T00:00:02.000Z"),
      ).rejects.toThrow("has no manifest version");
      expect(await store.getCompactionJob(job.id)).toMatchObject({
        state: "ready",
        revision: job.revision,
      });
      expect(await store.getTransaction(job.transactionId ?? "")).toEqual(invalidTransaction);
      store.close();
    });

    it("rejects cancelled compaction records that retain an error", async () => {
      const store = await implementation.create();
      await expect(
        store.createCompactionJob({
          ...rechunkCompactionJob("invalid-cancelled-error"),
          state: "cancelled",
          error: "should have been cleared",
        }),
      ).rejects.toThrow("cannot contain an error");
      expect(await store.getCompactionJob("invalid-cancelled-error")).toBeUndefined();
      store.close();
    });

    it("persists deterministic rechunk plans and output-driven checkpoints", async () => {
      const store = await implementation.create();
      const created = rechunkCompactionJob();
      await store.createCompactionJob(created);
      const createdPlan = created.rewritePlan;
      if (createdPlan?.kind !== "rechunk-v1") throw new Error("Expected a rechunk plan");
      (createdPlan.columns[0]?.sourceBlocks[0] as { blockId: string }).blockId = "mutated";
      (createdPlan.outputs[0] as { rowCount: number }).rowCount = 99;

      const persisted = await store.getCompactionJob(created.id);
      expect(persisted?.rewritePlan).toEqual(rechunkCompactionJob().rewritePlan);
      expect(persisted).toMatchObject({
        sourceBlockIds: ["id-block-1", "id-block-2", "name-block"],
        outputCursor: { outputIndex: 0, columnIndex: 0, rowStart: 0 },
        memoryBudgetBytes: 4096,
        minimumMemoryBytes: 512,
        peakWorkingBytes: 0,
        outputLogicalBytes: 0,
      });

      const first = await store.updateCompactionJob(created.id, 0, {
        outputBlockIds: ["output-0-id"],
        outputCursor: { outputIndex: 0, columnIndex: 1, rowStart: 0 },
        outputStoredBytes: 70,
        outputLogicalBytes: 80,
        peakWorkingBytes: 600,
        state: "running",
        transactionId: "rechunk-transaction",
        updatedAt: "2026-01-01T00:00:01.000Z",
      });
      expect(first).toMatchObject({
        outputBlockIds: ["output-0-id"],
        outputCursor: { outputIndex: 0, columnIndex: 1, rowStart: 0 },
        processedRows: 0,
        outputStoredBytes: 70,
        outputLogicalBytes: 80,
        peakWorkingBytes: 600,
        revision: 1,
      });
      await expect(
        store.updateCompactionJob(created.id, 0, {
          peakWorkingBytes: 700,
          updatedAt: "2026-01-01T00:00:02.000Z",
        }),
      ).rejects.toBeInstanceOf(CompactionJobConflictError);

      const firstWindow = await store.updateCompactionJob(created.id, first.revision, {
        outputBlockIds: ["output-0-id", "output-0-name"],
        outputCursor: { outputIndex: 1, columnIndex: 0, rowStart: 3 },
        processedRows: 3,
        outputStoredBytes: 150,
        outputLogicalBytes: 210,
        peakWorkingBytes: 700,
        updatedAt: "2026-01-01T00:00:03.000Z",
      });
      const ready = await store.updateCompactionJob(created.id, firstWindow.revision, {
        outputBlockIds: ["output-0-id", "output-0-name", "output-1-id", "output-1-name"],
        outputCursor: { outputIndex: 2, columnIndex: 0, rowStart: 4 },
        processedRows: 4,
        outputStoredBytes: 240,
        outputLogicalBytes: 300,
        peakWorkingBytes: 720,
        state: "ready",
        updatedAt: "2026-01-01T00:00:04.000Z",
      });
      expect(ready).toMatchObject({
        state: "ready",
        processedRows: 4,
        outputBlockIds: ["output-0-id", "output-0-name", "output-1-id", "output-1-name"],
        revision: 3,
      });

      await expect(
        store.updateCompactionJob(created.id, ready.revision, {
          rewritePlan: { kind: "copy-v1" },
          updatedAt: "2026-01-01T00:00:05.000Z",
        } as CompactionJobRecordUpdate & { rewritePlan: { kind: "copy-v1" } }),
      ).rejects.toThrow("immutable");
      await expect(
        store.updateCompactionJob(created.id, ready.revision, {
          outputStoredBytes: 239,
          updatedAt: "2026-01-01T00:00:05.000Z",
        }),
      ).rejects.toThrow("cannot decrease");
      expect((await store.getCompactionJob(created.id))?.revision).toBe(ready.revision);
      store.close();
    });

    it("rejects invalid rechunk layouts, budgets, and output checkpoints", async () => {
      const store = await implementation.create();
      const tooSmall = rechunkCompactionJob("too-small");
      await expect(
        store.createCompactionJob({ ...tooSmall, memoryBudgetBytes: 511 }),
      ).rejects.toThrow("minimum memory exceeds");

      const missingSource = rechunkCompactionJob("missing-source");
      await expect(
        store.createCompactionJob({
          ...missingSource,
          sourceBlockIds: ["id-block-1", "name-block"],
        }),
      ).rejects.toThrow("every selected source block");

      const duplicateSource = rechunkCompactionJob("duplicate-source");
      await expect(
        store.createCompactionJob({
          ...duplicateSource,
          sourceBlockIds: [...duplicateSource.sourceBlockIds, "name-block"],
        }),
      ).rejects.toThrow("cannot contain duplicates");

      const badRange = rechunkCompactionJob("bad-range");
      if (badRange.rewritePlan?.kind !== "rechunk-v1") throw new Error("Expected rechunk plan");
      await expect(
        store.createCompactionJob({
          ...badRange,
          rewritePlan: {
            ...badRange.rewritePlan,
            outputs: [
              { rowStart: 0, rowCount: 2 },
              { rowStart: 3, rowCount: 1 },
            ],
          },
        }),
      ).rejects.toThrow("contiguously");

      const checkpoint = rechunkCompactionJob("bad-checkpoint");
      await store.createCompactionJob(checkpoint);
      await expect(
        store.updateCompactionJob(checkpoint.id, 0, {
          outputBlockIds: ["only-one-output"],
          outputCursor: { outputIndex: 1, columnIndex: 0, rowStart: 3 },
          processedRows: 3,
          outputStoredBytes: 50,
          outputLogicalBytes: 60,
          peakWorkingBytes: 600,
          state: "running",
          transactionId: "rechunk-transaction",
          updatedAt: "2026-01-01T00:00:01.000Z",
        }),
      ).rejects.toThrow("output IDs must match");
      expect((await store.getCompactionJob(checkpoint.id))?.revision).toBe(0);
      store.close();
    });

    it("validates compaction checkpoints before persistence", async () => {
      const store = await implementation.create();
      await expect(
        store.createCompactionJob({
          id: "invalid-job",
          tableId: "events",
          sourceManifestVersion: 0,
          sourceSegmentIds: [],
          sourceBlockIds: [],
          outputBlockIds: [],
          cursor: { sourceSegmentIndex: 0, sourceBlockIndex: 0 },
          processedRows: 0,
          sourceStoredBytes: 0,
          outputStoredBytes: 0,
          logicalBytes: 0,
          targetLevel: 1,
          state: "planned",
          transactionId: null,
          outputSegmentId: null,
          publishedVersion: null,
          revision: 0,
          createdAt: "2026-01-01T00:00:00.000Z",
          updatedAt: "2026-01-01T00:00:00.000Z",
        }),
      ).rejects.toThrow("at least one source segment");
      expect(await store.listCompactionJobs()).toEqual([]);
      store.close();
    });

    it("rejects inconsistent terminal compaction checkpoints and state regression", async () => {
      const store = await implementation.create();
      const planned: CompactionJobRecord = {
        id: "stateful-job",
        tableId: "events",
        sourceManifestVersion: 0,
        sourceSegmentIds: ["source-segment"],
        sourceBlockIds: ["source-block"],
        outputBlockIds: [],
        cursor: { sourceSegmentIndex: 0, sourceBlockIndex: 0 },
        processedRows: 0,
        sourceStoredBytes: 0,
        outputStoredBytes: 0,
        logicalBytes: 0,
        targetLevel: 1,
        state: "planned",
        transactionId: null,
        outputSegmentId: "output-segment",
        publishedVersion: null,
        revision: 0,
        createdAt: "2026-01-01T00:00:00.000Z",
        updatedAt: "2026-01-01T00:00:00.000Z",
      };

      for (const state of ["ready", "published"] as const) {
        await expect(
          store.createCompactionJob({
            ...planned,
            id: `inconsistent-${state}`,
            state,
            outputSegmentId: null,
          }),
        ).rejects.toThrow();
        expect(await store.getCompactionJob(`inconsistent-${state}`)).toBeUndefined();
      }

      await store.createCompactionJob(planned);
      const running = await store.updateCompactionJob(planned.id, 0, {
        state: "running",
        transactionId: "compaction-transaction",
        updatedAt: "2026-01-01T00:00:01.000Z",
      });
      const ready = await store.updateCompactionJob(planned.id, running.revision, {
        state: "ready",
        outputBlockIds: ["output-block"],
        cursor: { sourceSegmentIndex: 1, sourceBlockIndex: 0 },
        processedRows: 1,
        updatedAt: "2026-01-01T00:00:02.000Z",
      });
      const published = await store.updateCompactionJob(planned.id, ready.revision, {
        state: "published",
        publishedVersion: 1,
        updatedAt: "2026-01-01T00:00:03.000Z",
      });

      await expect(
        store.updateCompactionJob(planned.id, published.revision, {
          state: "running",
          publishedVersion: null,
          updatedAt: "2026-01-01T00:00:04.000Z",
        }),
      ).rejects.toThrow();
      expect(await store.getCompactionJob(planned.id)).toMatchObject({
        state: "published",
        revision: published.revision,
      });
      store.close();
    });

    it("updates compaction checkpoints atomically", async () => {
      const store = await implementation.create();
      await store.createCompactionJob({
        id: "contended-job",
        tableId: "events",
        sourceManifestVersion: 0,
        sourceSegmentIds: ["segment-1"],
        sourceBlockIds: ["block-1"],
        outputBlockIds: [],
        cursor: { sourceSegmentIndex: 0, sourceBlockIndex: 0 },
        processedRows: 0,
        sourceStoredBytes: 0,
        outputStoredBytes: 0,
        logicalBytes: 0,
        targetLevel: 1,
        state: "planned",
        transactionId: null,
        outputSegmentId: null,
        publishedVersion: null,
        revision: 0,
        createdAt: "2026-01-01T00:00:00.000Z",
        updatedAt: "2026-01-01T00:00:00.000Z",
      });
      const results = await Promise.allSettled([
        store.updateCompactionJob("contended-job", 0, {
          state: "running",
          transactionId: "transaction-a",
          updatedAt: "2026-01-01T00:00:01.000Z",
        }),
        store.updateCompactionJob("contended-job", 0, {
          state: "running",
          transactionId: "transaction-b",
          updatedAt: "2026-01-01T00:00:02.000Z",
        }),
      ]);
      expect(results.filter((result) => result.status === "fulfilled")).toHaveLength(1);
      const rejection = results.find((result) => result.status === "rejected");
      expect(rejection?.reason).toBeInstanceOf(CompactionJobConflictError);
      expect((await store.getCompactionJob("contended-job"))?.revision).toBe(1);
      store.close();
    });

    it("prunes manifests and reclaims exact physical artifacts in bounded durable steps", async () => {
      const store = await implementation.create();
      const prefix = "bounded-gc";
      await createSupersededStorage(store, prefix);
      const created = await createSupersededGarbageCollectionJob(store, prefix);

      expect(created).toMatchObject({
        candidateManifestVersions: [0],
        candidateSegmentIds: [`${prefix}/old-segment`],
        candidateBlockIds: [`${prefix}/old-block`],
        cursor: { manifestIndex: 0, segmentIndex: 0, blockIndex: 0 },
        state: "planned",
        revision: 0,
      });

      const manifestStep = await store.runGarbageCollectionStep({
        jobId: created.id,
        expectedRevision: 0,
        maxItems: 1,
        updatedAt: "2026-01-01T00:03:00.000Z",
      });
      expect(manifestStep).toMatchObject({
        prunedManifestVersions: [0],
        reclaimedSegmentIds: [],
        reclaimedBlockIds: [],
        reclaimedBlockBytes: 0,
        job: {
          cursor: { manifestIndex: 1, segmentIndex: 0, blockIndex: 0 },
          state: "running",
          revision: 1,
        },
      });
      expect(await store.getManifest(0)).toMatchObject({
        blockIds: [`${prefix}/old-block`],
        prunedAt: "2026-01-01T00:03:00.000Z",
      });
      expect(await store.getSegment(`${prefix}/old-segment`)).toBeDefined();
      expect(await store.getBlock(`${prefix}/old-block`)).toEqual(Uint8Array.of(1, 2, 3));

      const segmentStep = await store.runGarbageCollectionStep({
        jobId: created.id,
        expectedRevision: 1,
        maxItems: 1,
        updatedAt: "2026-01-01T00:04:00.000Z",
      });
      expect(segmentStep).toMatchObject({
        reclaimedSegmentIds: [`${prefix}/old-segment`],
        reclaimedBlockIds: [],
        job: {
          cursor: { manifestIndex: 1, segmentIndex: 1, blockIndex: 0 },
          state: "running",
          revision: 2,
        },
      });
      expect(await store.getSegment(`${prefix}/old-segment`)).toBeUndefined();

      const blockStep = await store.runGarbageCollectionStep({
        jobId: created.id,
        expectedRevision: 2,
        maxItems: 1,
        updatedAt: "2026-01-01T00:05:00.000Z",
      });
      expect(blockStep).toMatchObject({
        reclaimedBlockIds: [`${prefix}/old-block`],
        reclaimedBlockBytes: 3,
        job: {
          cursor: { manifestIndex: 1, segmentIndex: 1, blockIndex: 1 },
          prunedManifestCount: 1,
          reclaimedSegmentCount: 1,
          reclaimedBlockCount: 1,
          reclaimedBlockBytes: 3,
          state: "completed",
          revision: 3,
        },
      });
      expect(await store.getBlock(`${prefix}/old-block`)).toBeUndefined();
      expect(await store.getBlock(`${prefix}/current-block`)).toEqual(Uint8Array.of(4, 5));
      expect((await store.getCurrentManifest())?.version).toBe(1);

      const repeated = await store.runGarbageCollectionStep({
        jobId: created.id,
        expectedRevision: 3,
        maxItems: 1,
        updatedAt: "2026-01-01T00:06:00.000Z",
      });
      expect(repeated.job.revision).toBe(3);
      expect(repeated.reclaimedBlockIds).toEqual([]);
      expect(repeated.reclaimedBlockBytes).toBe(0);
      store.close();
    });

    it("retains every artifact reachable from an unexpired snapshot lease", async () => {
      const store = await implementation.create();
      const prefix = "leased-gc";
      await createSupersededStorage(store, prefix);
      await store.createLease({
        id: `${prefix}/lease`,
        kind: "reader",
        manifestVersion: 0,
        ownerId: "reader",
        expiresAt: "2026-01-01T00:20:00.000Z",
        revision: 0,
      });
      const job = await createSupersededGarbageCollectionJob(store, prefix);
      const result = await store.runGarbageCollectionStep({
        jobId: job.id,
        expectedRevision: 0,
        maxItems: 3,
        updatedAt: "2026-01-01T00:03:00.000Z",
      });

      expect(result).toMatchObject({
        retainedManifestVersions: [0],
        retainedSegmentIds: [`${prefix}/old-segment`],
        retainedBlockIds: [`${prefix}/old-block`],
        reclaimedBlockBytes: 0,
        job: {
          retainedManifestCount: 1,
          retainedSegmentCount: 1,
          retainedBlockCount: 1,
          state: "completed",
        },
      });
      expect((await store.getManifest(0))?.prunedAt).toBeUndefined();
      expect(await store.getBlock(`${prefix}/old-block`)).toBeDefined();
      store.close();
    });

    it("rejects snapshot pins after an expired lease loses the GC race", async () => {
      const store = await implementation.create();
      const prefix = "expired-lease-gc";
      await createSupersededStorage(store, prefix);
      await store.createLease({
        id: `${prefix}/lease`,
        kind: "reader",
        manifestVersion: 0,
        ownerId: "reader",
        expiresAt: "2026-01-01T00:05:00.000Z",
        revision: 0,
      });
      const job = await createSupersededGarbageCollectionJob(store, prefix);
      await store.runGarbageCollectionStep({
        jobId: job.id,
        expectedRevision: 0,
        maxItems: 3,
        updatedAt: "2026-01-01T00:11:00.000Z",
      });

      expect(await store.getManifest(0)).toMatchObject({
        prunedAt: "2026-01-01T00:11:00.000Z",
      });
      await expect(
        store.renewLease(`${prefix}/lease`, 0, "2026-01-01T01:00:00.000Z"),
      ).rejects.toBeInstanceOf(SnapshotManifestMissingError);
      await expect(
        store.createLease({
          id: `${prefix}/late-lease`,
          kind: "reader",
          manifestVersion: 0,
          ownerId: "late-reader",
          expiresAt: "2026-01-01T01:00:00.000Z",
          revision: 0,
        }),
      ).rejects.toBeInstanceOf(SnapshotManifestMissingError);
      await expect(
        store.createTransaction({
          ...activeTransaction(`${prefix}/late-transaction`),
          snapshotVersion: 0,
        }),
      ).rejects.toBeInstanceOf(SnapshotManifestMissingError);

      const currentTransaction = activeTransaction(`${prefix}/post-gc-transaction`);
      currentTransaction.snapshotVersion = 1;
      await store.createTransaction(currentTransaction);
      await expect(
        store.updateTransaction(currentTransaction.id, 0, {
          snapshotVersion: 0,
          updatedAt: "2026-01-01T00:12:00.000Z",
        }),
      ).rejects.toBeInstanceOf(SnapshotManifestMissingError);
      expect((await store.getTransaction(currentTransaction.id))?.revision).toBe(0);
      store.close();
    });

    it("rejects candidates without persisted manifest or terminal-artifact provenance", async () => {
      const store = await implementation.create();
      await store.addBlock("unproven-orphan", Uint8Array.of(1));
      await expect(
        store.createGarbageCollectionJob({
          id: "unsafe-gc",
          candidateManifestVersions: [],
          candidateSegmentIds: [],
          candidateBlockIds: ["unproven-orphan"],
          leaseCutoff: "2026-01-01T00:00:00.000Z",
          createdAt: "2026-01-01T00:00:00.000Z",
        }),
      ).rejects.toThrow("no persisted provenance");
      expect(await store.getBlock("unproven-orphan")).toEqual(Uint8Array.of(1));
      expect(await store.getGarbageCollectionJob("unsafe-gc")).toBeUndefined();
      store.close();
    });

    it("allows only one garbage collection step for a job revision", async () => {
      const store = await implementation.create();
      const prefix = "contended-gc";
      await createSupersededStorage(store, prefix);
      const job = await createSupersededGarbageCollectionJob(store, prefix);
      const input = {
        jobId: job.id,
        expectedRevision: 0,
        maxItems: 1,
        updatedAt: "2026-01-01T00:03:00.000Z",
      };
      const results = await Promise.allSettled([
        store.runGarbageCollectionStep(input),
        store.runGarbageCollectionStep(input),
      ]);

      expect(results.filter((result) => result.status === "fulfilled")).toHaveLength(1);
      const rejection = results.find((result) => result.status === "rejected");
      expect(rejection?.reason).toBeInstanceOf(GarbageCollectionJobConflictError);
      expect((await store.getGarbageCollectionJob(job.id))?.revision).toBe(1);
      store.close();
    });

    it("removes an expired lease with CAS without erasing a concurrent renewal", async () => {
      const store = await implementation.create();
      await expect(
        store.createLease({
          id: "invalid-expiry-lease",
          kind: "reader",
          manifestVersion: null,
          ownerId: "reader",
          expiresAt: "not-a-date",
          revision: 0,
        }),
      ).rejects.toThrow("expiration must be valid");
      await store.createLease({
        id: "expiry-cas-lease",
        kind: "reader",
        manifestVersion: null,
        ownerId: "reader",
        expiresAt: "2026-01-01T00:05:00.000Z",
        revision: 0,
      });
      await expect(store.renewLease("expiry-cas-lease", 0, "not-a-date")).rejects.toThrow(
        "expiration must be valid",
      );
      expect((await store.getLease("expiry-cas-lease"))?.revision).toBe(0);
      const renewed = await store.renewLease("expiry-cas-lease", 0, "2026-01-01T00:20:00.000Z");
      await expect(
        store.removeLeaseIfExpired("expiry-cas-lease", 0, "2026-01-01T00:30:00.000Z"),
      ).rejects.toBeInstanceOf(LeaseConflictError);
      expect(
        await store.removeLeaseIfExpired(
          "expiry-cas-lease",
          renewed.revision,
          "2026-01-01T00:10:00.000Z",
        ),
      ).toBe(false);
      expect(
        await store.removeLeaseIfExpired(
          "expiry-cas-lease",
          renewed.revision,
          "2026-01-01T00:30:00.000Z",
        ),
      ).toBe(true);
      expect(await store.getLease("expiry-cas-lease")).toBeUndefined();
      store.close();
    });

    it("roots a segment owned by an active transaction before its journal checkpoint", async () => {
      const store = await implementation.create();
      const prefix = "segment-journal-gap";
      await createSupersededStorage(store, prefix);
      const transaction = activeTransaction(`${prefix}/active-transaction`);
      transaction.snapshotVersion = 1;
      await store.createTransaction(transaction);
      await store.addSegment({
        id: `${prefix}/uncheckpointed-segment`,
        tableId: "events",
        transactionId: transaction.id,
        rowCount: 1,
        rowIdStart: 1n,
        rowIdEndExclusive: 2n,
        columnBlockIds: { value: [`${prefix}/old-block`] },
        createdAt: "2026-01-01T00:02:00.000Z",
      });
      const job = await store.createGarbageCollectionJob({
        id: `${prefix}/gc`,
        candidateManifestVersions: [0],
        candidateSegmentIds: [`${prefix}/uncheckpointed-segment`],
        candidateBlockIds: [`${prefix}/old-block`],
        leaseCutoff: "2026-01-01T00:10:00.000Z",
        createdAt: "2026-01-01T00:02:00.000Z",
      });
      const result = await store.runGarbageCollectionStep({
        jobId: job.id,
        expectedRevision: 0,
        maxItems: 3,
        updatedAt: "2026-01-01T00:03:00.000Z",
      });

      expect(result.prunedManifestVersions).toEqual([0]);
      expect(result.retainedSegmentIds).toEqual([`${prefix}/uncheckpointed-segment`]);
      expect(result.retainedBlockIds).toEqual([`${prefix}/old-block`]);
      expect(await store.getSegment(`${prefix}/uncheckpointed-segment`)).toBeDefined();
      expect(await store.getBlock(`${prefix}/old-block`)).toBeDefined();
      store.close();
    });

    it("pins a committed manifest until its compaction job is reconciled", async () => {
      const store = await implementation.create();
      const prefix = "unreconciled-compaction";
      const { job, commit } = await createReadyCompaction(store, prefix);
      const compactedManifest = await store.commitTransaction(commit);
      await store.addBlock(`${prefix}/tail-block`, Uint8Array.of(3));
      await store.createTransaction({
        ...activeTransaction(`${prefix}/tail-transaction`),
        snapshotVersion: compactedManifest.version,
        pendingBlockIds: [`${prefix}/tail-block`],
      });
      await store.commitTransaction({
        transactionId: `${prefix}/tail-transaction`,
        expectedTransactionRevision: 0,
        expectedManifestVersion: compactedManifest.version,
        blockIds: [...compactedManifest.blockIds, `${prefix}/tail-block`],
        committedAt: "2026-01-01T00:00:02.000Z",
      });
      expect((await store.getCompactionJob(job.id))?.state).toBe("ready");

      const gc = await store.createGarbageCollectionJob({
        id: `${prefix}/gc`,
        candidateManifestVersions: [compactedManifest.version],
        candidateSegmentIds: [],
        candidateBlockIds: [],
        leaseCutoff: "2026-01-01T00:10:00.000Z",
        createdAt: "2026-01-01T00:03:00.000Z",
      });
      const result = await store.runGarbageCollectionStep({
        jobId: gc.id,
        expectedRevision: 0,
        maxItems: 1,
        updatedAt: "2026-01-01T00:04:00.000Z",
      });

      expect(result.retainedManifestVersions).toEqual([compactedManifest.version]);
      expect((await store.getManifest(compactedManifest.version))?.prunedAt).toBeUndefined();
      store.close();
    });

    it("atomically rejects transaction journals that reference missing artifacts", async () => {
      const store = await implementation.create();
      const transaction = activeTransaction("missing-artifact-transaction");
      await store.createTransaction(transaction);
      await expect(
        store.updateTransaction(transaction.id, 0, {
          pendingBlockIds: ["missing-block"],
          updatedAt: "2026-01-01T00:01:00.000Z",
        }),
      ).rejects.toThrow("missing pending block");
      await expect(
        store.updateTransaction(transaction.id, 0, {
          pendingSegmentIds: ["missing-segment"],
          updatedAt: "2026-01-01T00:01:00.000Z",
        }),
      ).rejects.toThrow("missing pending segment");
      expect(await store.getTransaction(transaction.id)).toMatchObject({
        pendingBlockIds: [],
        pendingSegmentIds: [],
        revision: 0,
      });
      store.close();
    });

    it("allows a status-only abort to close a transaction whose artifact is already missing", async () => {
      const store = await implementation.create();
      await store.addBlock("lost-pending-block", Uint8Array.of(1));
      const transaction = {
        ...activeTransaction("dangling-active-transaction"),
        pendingBlockIds: ["lost-pending-block"],
      };
      await store.createTransaction(transaction);
      await store.removeBlock("lost-pending-block");

      const aborted = await store.updateTransaction(transaction.id, 0, {
        status: "aborted",
        updatedAt: "2026-01-01T00:01:00.000Z",
      });
      expect(aborted).toMatchObject({
        status: "aborted",
        pendingBlockIds: ["lost-pending-block"],
        revision: 1,
      });
      store.close();
    });

    it("serializes GC with adoption of an existing block into an active journal", async () => {
      const store = await implementation.create();
      const timestamp = "2026-01-01T00:00:00.000Z";
      await store.addBlock("adoptable-block", Uint8Array.of(7, 8));
      await store.createTransaction({
        ...activeTransaction("aborted-owner"),
        pendingBlockIds: ["adoptable-block"],
        status: "aborted",
      });
      await store.createTransaction(activeTransaction("active-adopter"));
      const job = await store.createGarbageCollectionJob({
        id: "adoption-race-gc",
        candidateManifestVersions: [],
        candidateSegmentIds: [],
        candidateBlockIds: ["adoptable-block"],
        leaseCutoff: timestamp,
        createdAt: timestamp,
      });

      const [gcResult, adoptionResult] = await Promise.allSettled([
        store.runGarbageCollectionStep({
          jobId: job.id,
          expectedRevision: 0,
          maxItems: 1,
          updatedAt: "2026-01-01T00:01:00.000Z",
        }),
        store.updateTransaction("active-adopter", 0, {
          pendingBlockIds: ["adoptable-block"],
          updatedAt: "2026-01-01T00:01:00.000Z",
        }),
      ]);

      expect(gcResult.status).toBe("fulfilled");
      if (gcResult.status !== "fulfilled") throw gcResult.reason;
      if (adoptionResult.status === "fulfilled") {
        expect(gcResult.value.retainedBlockIds).toEqual(["adoptable-block"]);
        expect(await store.getBlock("adoptable-block")).toEqual(Uint8Array.of(7, 8));
        expect(adoptionResult.value.pendingBlockIds).toEqual(["adoptable-block"]);
      } else {
        expect(String(adoptionResult.reason)).toContain("missing pending block");
        expect(gcResult.value.reclaimedBlockIds).toEqual(["adoptable-block"]);
        expect(await store.getBlock("adoptable-block")).toBeUndefined();
        expect((await store.getTransaction("active-adopter"))?.pendingBlockIds).toEqual([]);
      }
      store.close();
    });

    it("commits unique-key lookups with the database version", async () => {
      const store = await implementation.create();
      const timestamp = "2026-01-01T00:00:00.000Z";
      await store.addBlock("first", Uint8Array.of(1));
      await store.createTransaction({
        id: "first-transaction",
        snapshotVersion: null,
        pendingBlockIds: ["first"],
        pendingSegmentIds: [],
        status: "active",
        revision: 0,
        startedAt: timestamp,
        updatedAt: timestamp,
        committedVersion: null,
      });
      await store.commitTransaction({
        transactionId: "first-transaction",
        expectedTransactionRevision: 0,
        expectedManifestVersion: null,
        blockIds: ["first"],
        uniqueKeyChanges: {
          tableId: "accounts",
          keyTokens: ["string:ada@example.com"],
          requireAbsent: true,
        },
        committedAt: timestamp,
      });
      expect(
        await store.getExistingUniqueKeys("accounts", [
          "string:missing@example.com",
          "string:ada@example.com",
        ]),
      ).toEqual(["string:ada@example.com"]);

      await store.addBlock("second", Uint8Array.of(2));
      await store.createTransaction({
        id: "second-transaction",
        snapshotVersion: 0,
        pendingBlockIds: ["second"],
        pendingSegmentIds: [],
        status: "active",
        revision: 0,
        startedAt: timestamp,
        updatedAt: timestamp,
        committedVersion: null,
      });
      await expect(
        store.commitTransaction({
          transactionId: "second-transaction",
          expectedTransactionRevision: 0,
          expectedManifestVersion: 0,
          blockIds: ["first", "second"],
          uniqueKeyChanges: {
            tableId: "accounts",
            keyTokens: ["string:ada@example.com"],
            requireAbsent: true,
          },
          committedAt: timestamp,
        }),
      ).rejects.toBeInstanceOf(UniqueKeyConflictError);
      expect((await store.getCurrentManifest())?.version).toBe(0);
      store.close();
    });
  });
}

it("resumes a garbage collection job atomically after IndexedDB reopen", async () => {
  const indexedDB = new IDBFactory();
  const name = crypto.randomUUID();
  const prefix = "reopen-gc";
  let store = await IndexedDbBlockStore.open({ name, indexedDB });
  await createSupersededStorage(store, prefix);
  const job = await createSupersededGarbageCollectionJob(store, prefix);
  await store.runGarbageCollectionStep({
    jobId: job.id,
    expectedRevision: 0,
    maxItems: 1,
    updatedAt: "2026-01-01T00:03:00.000Z",
  });
  store.close();

  store = await IndexedDbBlockStore.open({ name, indexedDB });
  expect(await store.getGarbageCollectionJob(job.id)).toMatchObject({
    cursor: { manifestIndex: 1, segmentIndex: 0, blockIndex: 0 },
    prunedManifestCount: 1,
    state: "running",
    revision: 1,
  });
  expect(await store.getManifest(0)).toMatchObject({
    prunedAt: "2026-01-01T00:03:00.000Z",
  });
  const completed = await store.runGarbageCollectionStep({
    jobId: job.id,
    expectedRevision: 1,
    maxItems: 2,
    updatedAt: "2026-01-01T00:04:00.000Z",
  });
  expect(completed.job).toMatchObject({
    state: "completed",
    reclaimedSegmentCount: 1,
    reclaimedBlockCount: 1,
    reclaimedBlockBytes: 3,
    revision: 2,
  });
  store.close();

  store = await IndexedDbBlockStore.open({ name, indexedDB });
  expect(await store.getBlock(`${prefix}/old-block`)).toBeUndefined();
  expect(await store.getSegment(`${prefix}/old-segment`)).toBeUndefined();
  expect(await store.getManifest(0)).toBeDefined();
  expect((await store.getGarbageCollectionJob(job.id))?.reclaimedBlockBytes).toBe(3);
  store.close();
});

it("serializes historical lease creation with GC across IndexedDB connections", async () => {
  const indexedDB = new IDBFactory();
  const name = crypto.randomUUID();
  const prefix = "lease-race-gc";
  const collector = await IndexedDbBlockStore.open({ name, indexedDB });
  await createSupersededStorage(collector, prefix);
  const job = await createSupersededGarbageCollectionJob(collector, prefix);
  const reader = await IndexedDbBlockStore.open({ name, indexedDB });

  const [gcResult, leaseResult] = await Promise.allSettled([
    collector.runGarbageCollectionStep({
      jobId: job.id,
      expectedRevision: 0,
      maxItems: 3,
      updatedAt: "2026-01-01T00:03:00.000Z",
    }),
    reader.createLease({
      id: `${prefix}/lease`,
      kind: "reader",
      manifestVersion: 0,
      ownerId: "reader",
      expiresAt: "2026-01-01T01:00:00.000Z",
      revision: 0,
    }),
  ]);

  expect(gcResult.status).toBe("fulfilled");
  if (gcResult.status !== "fulfilled") throw gcResult.reason;
  if (leaseResult.status === "fulfilled") {
    expect(gcResult.value.retainedManifestVersions).toEqual([0]);
    expect(gcResult.value.retainedBlockIds).toEqual([`${prefix}/old-block`]);
    expect(await reader.getBlock(`${prefix}/old-block`)).toBeDefined();
  } else {
    expect(leaseResult.reason).toBeInstanceOf(SnapshotManifestMissingError);
    expect(gcResult.value.prunedManifestVersions).toEqual([0]);
    expect(gcResult.value.reclaimedBlockIds).toEqual([`${prefix}/old-block`]);
  }
  collector.close();
  reader.close();
});

it("reports complete logical IndexedDB payload after reopen", async () => {
  const indexedDB = new IDBFactory();
  const name = crypto.randomUUID();
  let store = await IndexedDbBlockStore.open({ name, indexedDB });
  await store.addTable({
    id: "events-id",
    name: "events",
    columns: [{ id: "event-id", name: "event_id", type: "number", nullable: false }],
    uniqueKeyColumnId: "event-id",
    uniqueKeyLookupReady: true,
    uniqueKeyStorage: "chunks-v1",
    createdAt: "2026-01-01T00:00:00.000Z",
  });
  await store.addBlock("payload", Uint8Array.of(1, 2, 3, 4));
  const before = await store.getLogicalStorageBytes();
  expect(before).toBeGreaterThan(4);
  store.close();
  store = await IndexedDbBlockStore.open({ name, indexedDB });
  expect(await store.getLogicalStorageBytes()).toBe(before);
  store.close();
});

it("reserves row IDs atomically across IndexedDB connections", async () => {
  const factory = new IDBFactory();
  const name = crypto.randomUUID();
  const left = await IndexedDbBlockStore.open({ name, indexedDB: factory });
  const right = await IndexedDbBlockStore.open({ name, indexedDB: factory });
  const ranges = await Promise.all([
    left.reserveRowIds("events", 10),
    right.reserveRowIds("events", 10),
  ]);
  expect(new Set(ranges.map((range) => range.start.toString())).size).toBe(2);
  expect(ranges.map((range) => range.endExclusive - range.start)).toEqual([10n, 10n]);
  left.close();
  right.close();
});

it("persists compaction checkpoints across IndexedDB connections", async () => {
  const factory = new IDBFactory();
  const name = crypto.randomUUID();
  let store = await IndexedDbBlockStore.open({ name, indexedDB: factory });
  await store.createCompactionJob({
    id: "restartable-job",
    tableId: "events",
    sourceManifestVersion: 2,
    sourceSegmentIds: ["segment-1", "segment-2"],
    sourceBlockIds: ["block-1", "block-2"],
    outputBlockIds: ["output-1"],
    cursor: { sourceSegmentIndex: 1, sourceBlockIndex: 12 },
    processedRows: 52,
    sourceStoredBytes: 1024,
    outputStoredBytes: 1024,
    logicalBytes: 4096,
    targetLevel: 1,
    state: "running",
    transactionId: "transaction-1",
    outputSegmentId: "segment-output",
    publishedVersion: null,
    revision: 3,
    createdAt: "2026-01-01T00:00:00.000Z",
    updatedAt: "2026-01-01T00:01:00.000Z",
  });
  store.close();

  store = await IndexedDbBlockStore.open({ name, indexedDB: factory });
  expect(await store.getCompactionJob("restartable-job")).toMatchObject({
    cursor: { sourceSegmentIndex: 1, sourceBlockIndex: 12 },
    processedRows: 52,
    transactionId: "transaction-1",
    rewritePlan: { kind: "copy-v1" },
    outputCursor: null,
    outputLogicalBytes: 4096,
    revision: 3,
  });
  store.close();
});

it("persists compaction cancellation and transaction abort atomically across reopen", async () => {
  const factory = new IDBFactory();
  const name = crypto.randomUUID();
  let store = await IndexedDbBlockStore.open({ name, indexedDB: factory });
  const { job } = await createReadyCompaction(store, "persistent-cancellation");
  await store.cancelCompactionJob(job.id, job.revision, "2026-01-01T00:00:02.000Z");
  store.close();

  store = await IndexedDbBlockStore.open({ name, indexedDB: factory });
  expect(await store.getCompactionJob(job.id)).toMatchObject({
    state: "cancelled",
    revision: 1,
    outputBlockIds: job.outputBlockIds,
    outputSegmentId: job.outputSegmentId,
  });
  expect(await store.getTransaction(job.transactionId ?? "")).toMatchObject({
    status: "aborted",
    revision: 1,
    pendingBlockIds: job.outputBlockIds,
    pendingSegmentIds: [job.outputSegmentId],
  });
  expect(await store.getBlock(job.outputBlockIds[0] ?? "")).toEqual(Uint8Array.of(2));
  expect(await store.getSegment(job.outputSegmentId ?? "")).toBeDefined();
  store.close();
});

it("persists rechunk plans and memory accounting across IndexedDB connections", async () => {
  const factory = new IDBFactory();
  const name = crypto.randomUUID();
  let store = await IndexedDbBlockStore.open({ name, indexedDB: factory });
  const job = rechunkCompactionJob("reopen-rechunk-job");
  await store.createCompactionJob(job);
  await store.updateCompactionJob(job.id, 0, {
    outputBlockIds: ["output-0-id"],
    outputCursor: { outputIndex: 0, columnIndex: 1, rowStart: 0 },
    outputStoredBytes: 70,
    outputLogicalBytes: 80,
    peakWorkingBytes: 600,
    state: "running",
    transactionId: "rechunk-transaction",
    updatedAt: "2026-01-01T00:01:00.000Z",
  });
  store.close();

  store = await IndexedDbBlockStore.open({ name, indexedDB: factory });
  expect(await store.getCompactionJob(job.id)).toMatchObject({
    rewritePlan: {
      kind: "rechunk-v1",
      targetBlockBytes: 2 * 1024 * 1024,
      outputCompression: "gzip",
      rowIdStart: 10n,
      rowIdEndExclusive: 14n,
      logicalOrder: 5,
      outputs: [
        { rowStart: 0, rowCount: 3 },
        { rowStart: 3, rowCount: 1 },
      ],
    },
    outputCursor: { outputIndex: 0, columnIndex: 1, rowStart: 0 },
    memoryBudgetBytes: 4096,
    minimumMemoryBytes: 512,
    peakWorkingBytes: 600,
    outputStoredBytes: 70,
    outputLogicalBytes: 80,
    revision: 1,
  });
  store.close();
});
