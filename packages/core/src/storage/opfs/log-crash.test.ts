/**
 * Crash and coordination shapes of the OPFS store's leader design: torn WAL tails, death
 * without a goodbye, checkpoint slot corruption, leader failover, graceful handoff, and the
 * foreground preference. The conformance suites prove the store behaves; this suite proves
 * the recovery and election rules by manufacturing the exact states they exist for.
 */
import { describe, expect, it } from "vitest";
import { MemoryOpfs } from "../../testing/opfs-shim.js";
import { OpfsBlockStore } from "./index.js";
import {
  MAX_MANIFEST_BLOCK_PRESENCE_IDS,
  MAX_BLOCK_READ_BATCH_BYTES,
  MAX_STORAGE_ID_CHARACTERS,
  MAX_TEMP_RUN_PAGES_PER_BATCH,
  MAX_TRANSACTION_STAGE_BLOCKS,
  MAX_ACTIVE_FTS_BASE_BUILDS,
  MAX_CATALOG_RETAINED_BYTES,
  PostingBuildConflictError,
  SchemaConflictError,
  StorageCorruptionError,
  StorageFormatVersionError,
  StorageResourceLimitError,
  TableRecordConflictError,
  secondaryUniqueKeyNamespace,
  catalogRecordRetainedBytes,
} from "../types.js";
import type { TableRecord } from "../types.js";
import { encodeBlock } from "../../block-format/index.js";
import { WalWriter } from "../toolkit/wal.js";
import { decodeSyncCheckpoint, encodeSyncCheckpoint, LOG_FORMAT_VERSION } from "../toolkit/wire.js";
import type { SyncFileHandle } from "../toolkit/sync-file.js";
import { OpfsTree } from "./files.js";
import {
  assertBlockReadBatchByteLimit,
  MAX_OPFS_CHECKPOINT_BYTES,
  MAX_OPFS_WAL_BYTES,
  OpfsLeader,
} from "./leader.js";

function table(name: string): TableRecord {
  return {
    id: `table-${name}`,
    name,
    columns: [{ id: "c1", name: "id", type: "number", nullable: false }],
    managed: false,
    revision: 0,
    createdAt: "2026-08-19T00:00:00.000Z",
  };
}

async function waitFor(condition: () => Promise<boolean> | boolean, what: string): Promise<void> {
  for (let attempt = 0; attempt < 400; attempt += 1) {
    if (await condition()) return;
    await new Promise((resolve) => setTimeout(resolve, 5));
  }
  throw new Error(`Timed out waiting for ${what}`);
}

let testTransactionOrdinal = 0;

async function createTestTransaction(store: OpfsBlockStore, label: string): Promise<string> {
  const ordinal = testTransactionOrdinal;
  testTransactionOrdinal += 1;
  const transactionId = `test-${label}-${String(ordinal)}`;
  await store.beginTransaction({
    record: {
      id: transactionId,
      ownerId: `owner-${transactionId}`,
      expiresAt: "2026-08-24T01:00:00.000Z",
      pendingBlockIds: [],
      pendingSegmentIds: [],
      status: "active",
      revision: 0,
      startedAt: "2026-08-24T00:00:00.000Z",
      updatedAt: "2026-08-24T00:00:00.000Z",
      committedVersion: null,
    },
  });
  return transactionId;
}

async function stageTestBlocks(
  store: OpfsBlockStore,
  blocks: ReadonlyArray<{ id: string; bytes: Uint8Array }>,
  label = "payload",
): Promise<void> {
  const transactionId = await createTestTransaction(store, label);
  await store.stageTransactionArtifacts({
    transactionId,
    expectedRevision: 0,
    blocks,
    segments: [],
    updatedAt: "2026-08-24T00:00:01.000Z",
  });
}

async function retireBlockThroughGc(
  store: OpfsBlockStore,
  id: string,
  blocks: ReadonlyArray<{ id: string; bytes: Uint8Array }>,
  jobId: string,
): Promise<void> {
  const liveIds = blocks.map((block) => block.id);
  const tableId = `gc-retired-table-${jobId}`;
  const retainedTableId = `gc-retained-table-${jobId}`;
  await store.addTable({ ...table(`retired-${jobId}`), id: tableId });
  await store.addTable({ ...table(`retained-${jobId}`), id: retainedTableId });
  const transactionId = `gc-source-${jobId}`;
  await store.createTransaction({
    id: transactionId,
    ownerId: `owner-${jobId}`,
    expiresAt: "2026-08-24T01:00:00.000Z",
    snapshotVersion: null,
    pendingBlockIds: [],
    pendingSegmentIds: [],
    status: "active",
    revision: 0,
    startedAt: "2026-08-24T00:00:00.000Z",
    updatedAt: "2026-08-24T00:00:00.000Z",
    committedVersion: null,
  });
  const segmentId = `gc-retired-segment-${jobId}`;
  const retainedIds = liveIds.filter((candidate) => candidate !== id);
  const retainedSegmentId = `gc-retained-segment-${jobId}`;
  const staged = await store.stageTransactionArtifacts({
    transactionId,
    expectedRevision: 0,
    blocks,
    segments: [
      {
        id: segmentId,
        kind: "insert",
        level: 0,
        logicalOrder: 0,
        commitOrdinal: 0,
        rowIdSpans: [],
        tableId,
        transactionId,
        rowCount: 1,
        rowIdStart: 1n,
        rowIdEndExclusive: 2n,
        columnBlockIds: { c1: [id] },
        createdAt: "2026-08-24T00:00:01.000Z",
      },
      {
        id: retainedSegmentId,
        kind: "insert",
        level: 0,
        logicalOrder: 0,
        commitOrdinal: 1,
        rowIdSpans: [],
        tableId: retainedTableId,
        transactionId,
        rowCount: 1,
        rowIdStart: 1n,
        rowIdEndExclusive: 2n,
        columnBlockIds: { c1: retainedIds },
        createdAt: "2026-08-24T00:00:01.000Z",
      },
    ],
    updatedAt: "2026-08-24T00:00:02.000Z",
  });
  const first = await store.commitTransaction({
    transactionId,
    expectedTransactionRevision: staged.revision,
    expectedManifestVersion: null,
    levelZeroSegmentLimits: [
      { tableId, limit: 4096 },
      { tableId: retainedTableId, limit: 4096 },
    ],
    committedAt: "2026-08-24T00:00:03.000Z",
  });
  await store.dropTable({
    tableId,
    expectedTableRevision: 0,
    expectedManifestVersion: first.version,
    expectedCatalogEpoch: (await store.getCatalogProbe()).catalogEpoch,
    committedAt: "2026-08-24T00:00:05.000Z",
  });
  const job = await store.createGarbageCollectionJob({
    id: jobId,
    candidateManifestVersions: [first.version],
    candidateSegmentIds: [],
    candidateBlockIds: [id],
    leaseCutoff: "2026-08-24T00:01:00.000Z",
    createdAt: "2026-08-24T00:01:00.000Z",
  });
  await store.runGarbageCollectionStep({
    jobId,
    expectedRevision: job.revision,
    maxItems: 10,
    updatedAt: "2026-08-24T00:01:01.000Z",
  });
}

describe("OPFS write-ahead log crash shapes", () => {
  it("persists structural epochs and rejects an old-schema journal after recovery", async () => {
    const shim = new MemoryOpfs();
    const name = "schema-epoch-recovery";
    let store = await OpfsBlockStore.open({ name, root: shim.root, checkpointEntries: 1 });
    await store.addTable(table("schema-epoch"));
    const transactionId = await createTestTransaction(store, "schema-epoch");
    const staged = await store.stageTransactionArtifacts({
      transactionId,
      expectedRevision: 0,
      blocks: [{ id: "schema-epoch-block", bytes: Uint8Array.of(1) }],
      segments: [
        {
          id: "schema-epoch-segment",
          tableId: "table-schema-epoch",
          transactionId,
          rowCount: 1,
          rowIdStart: 1n,
          rowIdEndExclusive: 2n,
          columnBlockIds: { c1: ["schema-epoch-block"] },
          kind: "insert",
          level: 0,
          logicalOrder: 0,
          commitOrdinal: 0,
          rowIdSpans: [],
          createdAt: "2026-08-24T00:00:01.000Z",
        },
      ],
      updatedAt: "2026-08-24T00:00:01.000Z",
    });
    await store.updateTable("table-schema-epoch", 0, {
      columns: [
        { id: "c1", name: "id", type: "number", nullable: false },
        { id: "c2", name: "note", type: "string", nullable: true },
      ],
    });
    const beforeCrash = await store.getCatalogProbe();
    expect(beforeCrash.schemaEpoch).toBe(2);
    store._crashForTests();

    store = await OpfsBlockStore.open({ name, root: shim.root, checkpointEntries: 1 });
    expect((await store.getCatalogProbe()).schemaEpoch).toBe(beforeCrash.schemaEpoch);
    expect((await store.getTransaction(transactionId))?.schemaEpochGuard).toBe(1);
    await expect(
      store.commitTransaction({
        transactionId,
        expectedTransactionRevision: staged.revision,
        expectedManifestVersion: null,
        levelZeroSegmentLimits: [{ tableId: "table-schema-epoch", limit: 4096 }],
        committedAt: "2026-08-24T00:00:02.000Z",
      }),
    ).rejects.toBeInstanceOf(SchemaConflictError);
    expect(await store.getCurrentManifestVersion()).toBeNull();
    expect(await store.getTransaction(transactionId)).toMatchObject({ status: "active" });
    store.close();
  });

  it("replays a chunked UNIQUE build and its atomic generation promotion", async () => {
    const shim = new MemoryOpfs();
    const name = "unique-generation-replay";
    let store = await OpfsBlockStore.open({ name, root: shim.root, checkpointEntries: 1_000 });
    const namespaceId = secondaryUniqueKeyNamespace("unique-table", "unique-index");
    await store.addTable({
      id: "unique-table",
      name: "unique_table",
      managed: false,
      revision: 0,
      columns: [{ id: "value", name: "value", type: "string", nullable: false }],
      secondaryIndexes: {
        "unique-index": {
          name: "unique_index",
          columnId: "value",
          columnIds: ["value"],
          directions: ["asc"],
          unique: true,
          termEncoding: "tuple-v1",
          storage: "postings-v1",
          storageColumnId: "unique-storage",
          locator: "row-id",
          state: "building",
          buildId: "unique-build",
          buildFromVersion: -1,
        },
      },
      createdAt: "2026-08-24T00:00:00.000Z",
    });
    await store.beginUniqueKeyBuild({
      buildId: "unique-build",
      tableId: "unique-table",
      indexId: "unique-index",
      namespaceId,
      ownerId: "unique-owner",
      createdAt: "2026-08-24T00:00:00.000Z",
      expiresAt: "2026-08-24T01:00:00.000Z",
    });
    await store.appendUniqueKeyBuildChunk({
      buildId: "unique-build",
      ownerId: "unique-owner",
      expiresAtCutoff: "2026-08-24T00:01:00.000Z",
      ordinal: 0,
      keyTokens: ["alpha", "beta"],
      updatedAt: "2026-08-24T00:01:00.000Z",
    });
    store._crashForTests();
    store = await OpfsBlockStore.open({ name, root: shim.root, checkpointEntries: 1_000 });
    expect(await store.getUniqueKeyBuild("unique-build")).toMatchObject({
      state: "active",
      nextOrdinal: 1,
      tokenCount: 2,
    });
    const finish = {
      buildId: "unique-build",
      ownerId: "unique-owner",
      expiresAtCutoff: "2026-08-24T00:02:00.000Z",
      expectedTableRevision: 0,
      expectedManifestVersion: null,
      chunkCount: 1,
      coversVersion: -1,
      completedAt: "2026-08-24T00:02:00.000Z",
    } as const;
    await store.finishUniqueKeyBuild(finish);
    store._crashForTests();
    store = await OpfsBlockStore.open({ name, root: shim.root, checkpointEntries: 1_000 });
    expect(await store.getExistingUniqueKeys(namespaceId, ["beta", "missing"])).toEqual(["beta"]);
    expect(await store.finishUniqueKeyBuild(finish)).toMatchObject({ revision: 1 });
    store.close();
  });

  it("serializes competing maintenance creation and rejects a duplicate active checkpoint", async () => {
    const shim = new MemoryOpfs();
    const name = "single-active-maintenance";
    const store = await OpfsBlockStore.open({ name, root: shim.root, checkpointEntries: 1 });
    const transactionId = await createTestTransaction(store, "maintenance-root");
    await store.commitTransaction({
      transactionId,
      expectedTransactionRevision: 0,
      expectedManifestVersion: null,
      committedAt: "2026-08-24T00:00:01.000Z",
    });
    const collection = (id: string) =>
      store.createGarbageCollectionJob({
        id,
        candidateManifestVersions: [0],
        candidateSegmentIds: [],
        candidateBlockIds: [],
        candidateTransactionIds: [],
        leaseCutoff: "2026-08-24T00:10:00.000Z",
        createdAt: "2026-08-24T00:10:00.000Z",
      });
    const attempts = await Promise.allSettled([
      collection("single-active-1"),
      collection("single-active-2"),
    ]);
    expect(attempts.filter(({ status }) => status === "fulfilled")).toHaveLength(1);
    expect(attempts.filter(({ status }) => status === "rejected")).toHaveLength(1);
    expect(await store.listGarbageCollectionJobs()).toHaveLength(1);
    await waitFor(
      () => (shim.readFileBytes(`minnowdb/${name}/wal`)?.byteLength ?? -1) === 0,
      "single active maintenance checkpoint",
    );
    store._crashForTests();

    const slotPaths = [`minnowdb/${name}/checkpoint-a`, `minnowdb/${name}/checkpoint-b`];
    const checkpoint = slotPaths
      .map((path) => shim.readFileBytes(path))
      .filter((bytes): bytes is Uint8Array => bytes !== undefined)
      .map(
        (bytes) =>
          decodeSyncCheckpoint(bytes) as {
            core: { garbageCollectionJobs: Array<{ id: string }> };
          },
      )
      .find(({ core }) => core.garbageCollectionJobs.length === 1);
    if (checkpoint === undefined) throw new Error("Expected maintenance checkpoint");
    const firstCollection = checkpoint.core.garbageCollectionJobs[0];
    if (firstCollection === undefined) throw new Error("Expected maintenance job");
    checkpoint.core.garbageCollectionJobs.push({
      ...firstCollection,
      id: "single-active-forged",
    });
    const corrupt = encodeSyncCheckpoint(checkpoint);
    for (const path of slotPaths) shim.writeFileBytes(path, corrupt);
    await expect(OpfsBlockStore.open({ name, root: shim.root })).rejects.toThrow(
      /Garbage collection job already active/,
    );
  });

  it("refuses exhausted WAL sequence and checkpoint generations before mutating durable state", async () => {
    const shim = new MemoryOpfs();
    const name = "numeric-exhaustion";
    const store = await OpfsBlockStore.open({ name, root: shim.root, checkpointEntries: 1 });
    await store.addTable(table("original"));
    await waitFor(
      () => (shim.readFileBytes(`minnowdb/${name}/wal`)?.byteLength ?? -1) === 0,
      "numeric fixture checkpoint",
    );
    store._crashForTests();

    const slotPathA = `minnowdb/${name}/checkpoint-a`;
    const slotPathB = `minnowdb/${name}/checkpoint-b`;
    const source =
      shim.readFileBytes(slotPathA) ?? shim.readFileBytes(slotPathB) ?? new Uint8Array(0);
    const decoded = decodeSyncCheckpoint(source);
    if (typeof decoded !== "object" || decoded === null) throw new Error("Missing checkpoint");
    const state = decoded as Record<string, unknown>;
    state.lastSeq = Number.MAX_SAFE_INTEGER;
    const exhaustedSequence = encodeSyncCheckpoint(state);
    shim.writeFileBytes(slotPathA, exhaustedSequence);
    shim.writeFileBytes(slotPathB, exhaustedSequence);

    const sequenceStore = await OpfsBlockStore.open({ name, root: shim.root });
    await expect(sequenceStore.addTable(table("must-refuse"))).rejects.toThrow(
      /WAL sequence cannot exceed the safe integer range/,
    );
    expect((await sequenceStore.listTables()).map((record) => record.name)).toEqual(["original"]);
    expect(shim.readFileBytes(`minnowdb/${name}/wal`)?.byteLength ?? 0).toBe(0);
    sequenceStore._crashForTests();

    state.generation = Number.MAX_SAFE_INTEGER;
    const exhaustedGeneration = encodeSyncCheckpoint(state);
    shim.writeFileBytes(slotPathA, exhaustedGeneration);
    shim.writeFileBytes(slotPathB, exhaustedGeneration);
    const minnow = await shim.root.getDirectoryHandle("minnowdb");
    const database = await minnow.getDirectoryHandle(name);
    const tree = new OpfsTree(database);
    const wal = await tree.openHandle(["wal"], { create: false });
    const slotA = await tree.openHandle(["checkpoint-a"], { create: false });
    const slotB = await tree.openHandle(["checkpoint-b"], { create: false });
    const leader = await OpfsLeader.recover(tree, true, { wal, slotA, slotB });
    expect(() => leader.checkpointNow()).toThrow(
      /checkpoint generation cannot exceed the safe integer range/,
    );
    expect(shim.readFileBytes(slotPathA)).toEqual(exhaustedGeneration);
    expect(shim.readFileBytes(slotPathB)).toEqual(exhaustedGeneration);
    leader.crash();
  });

  it("refuses aggregate catalog bytes before WAL mutation and preserves the exact cap on reopen", async () => {
    const shim = new MemoryOpfs();
    const name = "catalog-byte-cap";
    const store = await OpfsBlockStore.open({ name, root: shim.root });
    let retainedBytes = 0;
    let ordinal = 0;
    while (retainedBytes < MAX_CATALOG_RETAINED_BYTES) {
      const base: TableRecord = {
        id: `catalog-${String(ordinal)}`,
        name: `catalog_${String(ordinal)}`,
        managed: false,
        revision: 0,
        columns: [{ id: "value", name: "value", type: "string", nullable: true }],
        view: { sql: "", managed: false },
        createdAt: "2026-08-24T00:00:00.000Z",
      };
      const baseBytes = catalogRecordRetainedBytes(base);
      const remaining = MAX_CATALOG_RETAINED_BYTES - retainedBytes;
      const targetBytes = remaining > 3_000_000 ? 3_000_000 : remaining;
      const euroCharacters = Math.floor((targetBytes - baseBytes) / 3);
      const asciiCharacters = targetBytes - baseBytes - euroCharacters * 3;
      const record: TableRecord = {
        ...base,
        view: {
          sql: `${"€".repeat(euroCharacters)}${"x".repeat(asciiCharacters)}`,
          managed: false,
        },
      };
      await store.addTable(record);
      retainedBytes += catalogRecordRetainedBytes(record);
      ordinal += 1;
    }
    expect(retainedBytes).toBe(MAX_CATALOG_RETAINED_BYTES);
    const before = await store.getStorageStats();
    await expect(store.addTable(table("catalog-over"))).rejects.toBeInstanceOf(
      StorageResourceLimitError,
    );
    expect((await store.getStorageStats()).walBytes).toBe(before.walBytes);
    expect(await store.getTable("table-catalog-over")).toBeUndefined();
    store._crashForTests();

    const reopened = await OpfsBlockStore.open({ name, root: shim.root });
    expect(await reopened.getTable("catalog-0")).toMatchObject({ name: "catalog_0" });
    await expect(reopened.addTable(table("catalog-over-reopen"))).rejects.toBeInstanceOf(
      StorageResourceLimitError,
    );
    reopened.close();
  });

  it("replays a fused table drop without exposing a retired manifest or stranded metadata", async () => {
    const shim = new MemoryOpfs();
    const name = "fused-table-drop";
    const store = await OpfsBlockStore.open({ name, root: shim.root });
    const block = await encodeBlock({ type: "number", values: [7] }, "raw");
    await store.addTable(table("drop"));
    await store.createTransaction({
      id: "drop-tx",
      ownerId: "drop-owner",
      expiresAt: "2026-08-24T01:00:00.000Z",
      snapshotVersion: null,
      pendingBlockIds: [],
      pendingSegmentIds: [],
      status: "active",
      revision: 0,
      startedAt: "2026-08-24T00:00:00.000Z",
      updatedAt: "2026-08-24T00:00:00.000Z",
      committedVersion: null,
    });
    const staged = await store.stageTransactionArtifacts({
      transactionId: "drop-tx",
      expectedRevision: 0,
      blocks: [{ id: "drop-block", bytes: block }],
      segments: [
        {
          id: "drop-segment",
          kind: "insert",
          level: 0,
          logicalOrder: 0,
          commitOrdinal: 0,
          rowIdSpans: [],
          tableId: "table-drop",
          transactionId: "drop-tx",
          rowCount: 1,
          rowIdStart: 1n,
          rowIdEndExclusive: 2n,
          columnBlockIds: { c1: ["drop-block"] },
          createdAt: "2026-08-24T00:00:00.000Z",
        },
      ],
      updatedAt: "2026-08-24T00:00:01.000Z",
    });
    await store.commitTransaction({
      transactionId: "drop-tx",
      expectedTransactionRevision: staged.revision,
      expectedManifestVersion: null,
      levelZeroSegmentLimits: [{ tableId: "table-drop", limit: 4096 }],
      committedAt: "2026-08-24T00:00:02.000Z",
    });
    const dropped = await store.dropTable({
      tableId: "table-drop",
      expectedTableRevision: 0,
      expectedManifestVersion: 0,
      expectedCatalogEpoch: (await store.getCatalogProbe()).catalogEpoch,
      committedAt: "2026-08-24T00:00:03.000Z",
    });
    expect(dropped).toMatchObject({ version: 1, changedTableIds: ["table-drop"] });
    store._crashForTests();

    const reopened = await OpfsBlockStore.open({ name, root: shim.root });
    expect(await reopened.getTable("table-drop")).toBeUndefined();
    expect(await reopened.getSegment("drop-segment")).toBeUndefined();
    expect(await reopened.getCurrentManifest()).toMatchObject({ version: 1, liveBlockCount: 0 });
    expect(await reopened.getBlock("drop-block")).toEqual(block);
    reopened.close();
  });

  it("replays fused column retirement and keeps retired bytes pinned by a reader lease", async () => {
    const shim = new MemoryOpfs();
    const name = "fused-column-drop";
    const store = await OpfsBlockStore.open({ name, root: shim.root });
    const removed = await encodeBlock({ type: "number", values: [7] }, "raw");
    const retained = await encodeBlock({ type: "string", values: ["kept"] }, "raw");
    await store.addTable({
      id: "column-table",
      name: "column_table",
      columns: [
        { id: "old", name: "old", type: "number", nullable: true },
        { id: "keep", name: "keep", type: "string", nullable: true },
      ],
      managed: false,
      revision: 0,
      createdAt: "2026-08-24T00:00:00.000Z",
    });
    await store.createTransaction({
      id: "column-tx",
      ownerId: "column-owner",
      expiresAt: "2026-08-24T01:00:00.000Z",
      snapshotVersion: null,
      pendingBlockIds: [],
      pendingSegmentIds: [],
      status: "active",
      revision: 0,
      startedAt: "2026-08-24T00:00:00.000Z",
      updatedAt: "2026-08-24T00:00:00.000Z",
      committedVersion: null,
    });
    const staged = await store.stageTransactionArtifacts({
      transactionId: "column-tx",
      expectedRevision: 0,
      blocks: [
        { id: "old-block", bytes: removed },
        { id: "keep-block", bytes: retained },
      ],
      segments: [
        {
          id: "column-segment",
          kind: "insert",
          level: 0,
          logicalOrder: 0,
          commitOrdinal: 0,
          rowIdSpans: [],
          tableId: "column-table",
          transactionId: "column-tx",
          rowCount: 1,
          rowIdStart: 1n,
          rowIdEndExclusive: 2n,
          columnBlockIds: { old: ["old-block"], keep: ["keep-block"] },
          createdAt: "2026-08-24T00:00:01.000Z",
        },
      ],
      updatedAt: "2026-08-24T00:00:01.000Z",
    });
    await store.commitTransaction({
      transactionId: "column-tx",
      expectedTransactionRevision: staged.revision,
      expectedManifestVersion: null,
      levelZeroSegmentLimits: [{ tableId: "column-table", limit: 4096 }],
      committedAt: "2026-08-24T00:00:02.000Z",
    });
    await store.createLease({
      id: "column-reader",
      kind: "reader",
      manifestVersion: 0,
      ownerId: "reader-owner",
      createdAt: "2026-08-24T00:00:00.000Z",
      expiresAt: "2026-08-24T01:00:00.000Z",
      revision: 0,
    });

    const dropped = await store.dropTableColumn({
      tableId: "column-table",
      columnId: "old",
      expectedTableRevision: 0,
      expectedManifestVersion: 0,
      expectedCatalogEpoch: (await store.getCatalogProbe()).catalogEpoch,
      committedAt: "2026-08-24T00:00:03.000Z",
    });
    expect(dropped).toMatchObject({ version: 1, changedTableIds: ["column-table"] });
    const job = await store.createGarbageCollectionJob({
      id: "column-gc",
      candidateManifestVersions: [0],
      candidateSegmentIds: [],
      candidateBlockIds: ["old-block"],
      leaseCutoff: "2026-08-24T00:01:00.000Z",
      createdAt: "2026-08-24T00:01:00.000Z",
    });
    const pinned = await store.runGarbageCollectionStep({
      jobId: job.id,
      expectedRevision: job.revision,
      maxItems: 10,
      updatedAt: "2026-08-24T00:01:01.000Z",
    });
    expect(pinned.retainedBlockIds).toContain("old-block");
    store._crashForTests();

    const reopened = await OpfsBlockStore.open({ name, root: shim.root });
    expect(await reopened.getTable("column-table")).toMatchObject({
      revision: 1,
      columns: [{ id: "keep" }],
    });
    expect(await reopened.getSegment("column-segment")).toMatchObject({
      columnBlockIds: { keep: ["keep-block"] },
    });
    expect(await reopened.getCurrentManifest()).toMatchObject({
      version: 1,
      liveBlockCount: 1,
    });
    expect(await reopened.getBlock("old-block")).toEqual(removed);
    expect(await reopened.getBlock("keep-block")).toEqual(retained);
    expect((await reopened.checkIntegrity({ mode: "full" })).ok).toBe(true);
    reopened.close();
  });

  it("persists owner-scoped transaction renewal and atomically aborts only after expiry", async () => {
    const shim = new MemoryOpfs();
    const name = "transaction-liveness";
    const store = await OpfsBlockStore.open({ name, root: shim.root });
    await store.createTransaction({
      id: "tx",
      ownerId: "writer-a",
      expiresAt: "2026-08-24T00:01:00.000Z",
      snapshotVersion: null,
      pendingBlockIds: [],
      pendingSegmentIds: [],
      status: "active",
      revision: 0,
      startedAt: "2026-08-24T00:00:00.000Z",
      updatedAt: "2026-08-24T00:00:00.000Z",
      committedVersion: null,
    });
    await expect(
      store.renewTransaction({
        transactionId: "tx",
        ownerId: "writer-b",
        expiresAtCutoff: "2026-08-24T00:00:30.000Z",
        expiresAt: "2026-08-24T00:03:00.000Z",
      }),
    ).resolves.toBe(false);
    await expect(
      store.renewTransaction({
        transactionId: "tx",
        ownerId: "writer-a",
        expiresAtCutoff: "2026-08-24T00:01:00.000Z",
        expiresAt: "2026-08-24T00:03:00.000Z",
      }),
    ).resolves.toBe(false);
    await expect(
      store.renewTransaction({
        transactionId: "tx",
        ownerId: "writer-a",
        expiresAtCutoff: "2026-08-24T00:00:30.000Z",
        expiresAt: "2026-08-24T00:02:00.000Z",
      }),
    ).resolves.toBe(true);
    await expect(
      store.renewTransaction({
        transactionId: "tx",
        ownerId: "writer-a",
        expiresAtCutoff: "2026-08-24T00:00:45.000Z",
        expiresAt: "2026-08-24T00:01:30.000Z",
      }),
    ).resolves.toBe(true);
    expect(await store.getTransaction("tx")).toMatchObject({
      revision: 0,
      expiresAt: "2026-08-24T00:02:00.000Z",
    });
    await expect(
      store.abortTransactionIfExpired({
        transactionId: "tx",
        expectedOwnerId: "writer-a",
        expiresAtCutoff: "2026-08-24T00:01:59.999Z",
        updatedAt: "2026-08-24T00:03:00.000Z",
      }),
    ).resolves.toBeUndefined();
    const aborted = await store.abortTransactionIfExpired({
      transactionId: "tx",
      expectedOwnerId: "writer-a",
      expiresAtCutoff: "2026-08-24T00:02:00.000Z",
      updatedAt: "2026-08-24T00:03:00.000Z",
    });
    expect(aborted).toMatchObject({ status: "aborted", revision: 1 });
    store._crashForTests();

    const reopened = await OpfsBlockStore.open({ name, root: shim.root });
    expect(await reopened.getTransaction("tx")).toEqual(aborted);
    await expect(
      reopened.renewTransaction({
        transactionId: "tx",
        ownerId: "writer-a",
        expiresAtCutoff: "2026-08-24T00:03:30.000Z",
        expiresAt: "2026-08-24T00:04:00.000Z",
      }),
    ).resolves.toBe(false);
    reopened.close();
  });

  it("pages only expired lease and temp owners in stable expiry/id order across reopen", async () => {
    const shim = new MemoryOpfs();
    const name = "expiry-pages";
    const store = await OpfsBlockStore.open({ name, root: shim.root });
    for (const [id, expiresAt] of [
      ["lease-b", "2026-08-24T00:05:00.000Z"],
      ["lease-a", "2026-08-24T00:05:00.000Z"],
      ["lease-live", "2026-08-24T00:20:00.000Z"],
    ] as const) {
      await store.createLease({
        id,
        kind: "reader",
        manifestVersion: null,
        ownerId: `owner-${id}`,
        createdAt: "2026-08-24T00:00:00.000Z",
        expiresAt,
        revision: 0,
      });
    }
    for (const [ownerId, expiresAt] of [
      ["temp-b", "2026-08-24T00:05:00.000Z"],
      ["temp-a", "2026-08-24T00:05:00.000Z"],
      ["temp-live", "2026-08-24T00:20:00.000Z"],
    ] as const) {
      await store.createTempOwner({
        ownerId,
        createdAt: "2026-08-24T00:00:00.000Z",
        expiresAt,
        revision: 0,
      });
    }
    const leaseFirst = await store.listExpiredLeasePage("2026-08-24T00:10:00.000Z", null, 1);
    expect(leaseFirst.records.map(({ id }) => id)).toEqual(["lease-a"]);
    expect(leaseFirst.nextCursor).not.toBeNull();
    expect(
      (
        await store.listExpiredLeasePage("2026-08-24T00:10:00.000Z", leaseFirst.nextCursor, 1)
      ).records.map(({ id }) => id),
    ).toEqual(["lease-b"]);
    const tempFirst = await store.listExpiredTempOwnerPage("2026-08-24T00:10:00.000Z", null, 1);
    expect(tempFirst.records).toEqual(["temp-a"]);
    expect(tempFirst.nextCursor).not.toBeNull();

    const expiredLeaseBefore = await store.getLease("lease-a");
    await expect(
      store.renewLease({
        id: "lease-a",
        expectedRevision: 0,
        expiresAtCutoff: "2026-08-24T00:05:00.000Z",
        expiresAt: "2026-08-24T00:06:00.000Z",
      }),
    ).rejects.toThrow(/expired/);
    expect(await store.getLease("lease-a")).toEqual(expiredLeaseBefore);
    const expiredTempBefore = await store.getTempOwner("temp-a");
    await expect(
      store.renewTempOwner({
        ownerId: "temp-a",
        expectedRevision: 0,
        expiresAtCutoff: "2026-08-24T00:05:00.000Z",
        expiresAt: "2026-08-24T00:06:00.000Z",
      }),
    ).rejects.toThrow(/expired/);
    expect(await store.getTempOwner("temp-a")).toEqual(expiredTempBefore);
    await expect(
      store.renewLease({
        id: "lease-live",
        expectedRevision: 0,
        expiresAtCutoff: "2026-08-24T00:10:00.000Z",
        expiresAt: "2026-08-24T01:10:00.001Z",
      }),
    ).rejects.toThrow(/maximum TTL/);
    expect((await store.getLease("lease-live"))?.revision).toBe(0);

    store._crashForTests();
    const reopened = await OpfsBlockStore.open({ name, root: shim.root });
    expect(
      (await reopened.listExpiredTempOwnerPage("2026-08-24T00:10:00.000Z", tempFirst.nextCursor, 1))
        .records,
    ).toEqual(["temp-b"]);
    expect(
      (await reopened.listExpiredLeasePage("2026-08-24T00:10:00.000Z", null, 10)).records.map(
        ({ id }) => id,
      ),
    ).toEqual(["lease-a", "lease-b"]);
    reopened.close();
  });

  it("recovers an atomically staged existing-block segment without a journal gap", async () => {
    const shim = new MemoryOpfs();
    const name = "segment-journal-window";
    const block = await encodeBlock({ type: "string", values: ["kept"] }, "raw");
    const store = await OpfsBlockStore.open({ name, root: shim.root });
    await store.addTable(table("segment-owner"));
    await store.createTransaction({
      id: "source-tx",
      ownerId: "source-owner",
      expiresAt: "2026-08-24T01:00:00.000Z",
      snapshotVersion: null,
      pendingBlockIds: [],
      pendingSegmentIds: [],
      status: "active",
      revision: 0,
      startedAt: "2026-08-24T00:00:00.000Z",
      updatedAt: "2026-08-24T00:00:00.000Z",
      committedVersion: null,
    });
    const sourceStaged = await store.stageTransactionArtifacts({
      transactionId: "source-tx",
      expectedRevision: 0,
      blocks: [{ id: "source", bytes: block }],
      segments: [
        {
          id: "source-segment",
          kind: "insert",
          level: 0,
          logicalOrder: 0,
          commitOrdinal: 0,
          rowIdSpans: [],
          tableId: "table-segment-owner",
          transactionId: "source-tx",
          rowCount: 1,
          rowIdStart: 1n,
          rowIdEndExclusive: 2n,
          columnBlockIds: { c1: ["source"] },
          createdAt: "2026-08-24T00:00:01.000Z",
        },
      ],
      updatedAt: "2026-08-24T00:00:02.000Z",
    });
    await store.commitTransaction({
      transactionId: "source-tx",
      expectedTransactionRevision: sourceStaged.revision,
      expectedManifestVersion: null,
      levelZeroSegmentLimits: [{ tableId: "table-segment-owner", limit: 4096 }],
      committedAt: "2026-08-24T00:00:03.000Z",
    });
    await store.createTransaction({
      id: "tx",
      ownerId: "owner",
      expiresAt: "2026-08-24T01:00:00.000Z",
      snapshotVersion: 0,
      pendingBlockIds: [],
      pendingSegmentIds: [],
      status: "active",
      revision: 0,
      startedAt: "2026-08-24T00:00:00.000Z",
      updatedAt: "2026-08-24T00:00:00.000Z",
      committedVersion: null,
    });
    const staged = await store.stageTransactionArtifacts({
      transactionId: "tx",
      expectedRevision: 0,
      blocks: [],
      segments: [
        {
          id: "segment",
          kind: "insert",
          level: 0,
          logicalOrder: 0,
          commitOrdinal: 0,
          rowIdSpans: [],
          tableId: "table-segment-owner",
          transactionId: "tx",
          rowCount: 1,
          rowIdStart: 1n,
          rowIdEndExclusive: 2n,
          columnBlockIds: { c1: ["source"] },
          createdAt: "2026-08-24T00:00:00.000Z",
        },
      ],
      updatedAt: "2026-08-24T00:01:00.000Z",
    });
    store._crashForTests();

    const reopened = await OpfsBlockStore.open({ name, root: shim.root });
    expect(await reopened.getSegment("segment")).toBeDefined();
    await reopened.commitTransaction({
      transactionId: "tx",
      expectedTransactionRevision: staged.revision,
      expectedManifestVersion: 0,
      levelZeroSegmentLimits: [{ tableId: "table-segment-owner", limit: 4096 }],
      committedAt: "2026-08-24T00:02:00.000Z",
    });
    expect(await reopened.getBlock("source")).toEqual(block);
    reopened.close();
  });

  it("refuses an over-limit level-zero commit atomically and replays the accepted boundary", async () => {
    const shim = new MemoryOpfs();
    const name = "level-zero-limit";
    const store = await OpfsBlockStore.open({ name, root: shim.root });
    await store.addTable(table("level-zero"));
    const transactionId = await createTestTransaction(store, "level-zero");
    const bytes = await encodeBlock({ type: "number", values: [1, 2] }, "raw");
    const staged = await store.stageTransactionArtifacts({
      transactionId,
      expectedRevision: 0,
      blocks: [
        { id: "level-zero-a", bytes },
        { id: "level-zero-b", bytes },
      ],
      segments: [
        {
          id: "level-zero-segment-a",
          kind: "insert",
          level: 0,
          logicalOrder: 0,
          commitOrdinal: 0,
          rowIdSpans: [],
          tableId: "table-level-zero",
          transactionId,
          rowCount: 1,
          rowIdStart: 1n,
          rowIdEndExclusive: 2n,
          columnBlockIds: { c1: ["level-zero-a"] },
          createdAt: "2026-08-24T00:00:01.000Z",
        },
        {
          id: "level-zero-segment-b",
          kind: "insert",
          level: 0,
          logicalOrder: 0,
          commitOrdinal: 1,
          rowIdSpans: [],
          tableId: "table-level-zero",
          transactionId,
          rowCount: 1,
          rowIdStart: 2n,
          rowIdEndExclusive: 3n,
          columnBlockIds: { c1: ["level-zero-b"] },
          createdAt: "2026-08-24T00:00:01.000Z",
        },
      ],
      updatedAt: "2026-08-24T00:00:01.000Z",
    });
    const before = await store.getTransaction(transactionId);
    const walBefore = shim.readFileBytes(`minnowdb/${name}/wal`);
    await expect(
      store.commitTransaction({
        transactionId,
        expectedTransactionRevision: staged.revision,
        expectedManifestVersion: null,
        levelZeroSegmentLimits: [{ tableId: "table-level-zero", limit: 1 }],
        committedAt: "2026-08-24T00:00:02.000Z",
      }),
    ).rejects.toMatchObject({
      name: "CompactionBacklogError",
      tableName: "level-zero",
      levelZeroSegments: 2,
      limit: 1,
    });
    expect(await store.getCurrentManifest()).toBeUndefined();
    expect(await store.getTransaction(transactionId)).toEqual(before);
    expect(shim.readFileBytes(`minnowdb/${name}/wal`)).toEqual(walBefore);

    await store.commitTransaction({
      transactionId,
      expectedTransactionRevision: staged.revision,
      expectedManifestVersion: null,
      levelZeroSegmentLimits: [{ tableId: "table-level-zero", limit: 2 }],
      committedAt: "2026-08-24T00:00:02.000Z",
    });
    store._crashForTests();
    const reopened = await OpfsBlockStore.open({ name, root: shim.root });
    expect(await reopened.getCurrentManifest()).toMatchObject({
      version: 0,
      liveBlockCount: 2,
    });
    expect((await reopened.getTransaction(transactionId))?.status).toBe("committed");
    reopened.close();
  });

  it("logs bounded garbage-collection discovery atomically and replays its final cursor", async () => {
    const shim = new MemoryOpfs();
    const name = "bounded-gc-discovery";
    const store = await OpfsBlockStore.open({ name, root: shim.root });
    const discovery = {
      phase: "manifests" as const,
      currentManifestVersion: null,
      retainAboveVersion: -1,
      retainAfter: Date.parse("2026-08-24T00:00:00.000Z"),
      maxPlanningItems: 64,
      manifestCursor: null,
      segmentCursor: null,
      transactionCursor: null,
      compactionCursor: null,
      visitedRecords: 0,
    };
    const job = await store.createGarbageCollectionJob({
      id: "bounded-gc",
      candidateManifestVersions: [],
      candidateSegmentIds: [],
      candidateBlockIds: [],
      candidateTransactionIds: [],
      leaseCutoff: "2026-08-24T00:10:00.000Z",
      createdAt: "2026-08-24T00:10:00.000Z",
      discovery,
    });
    const before = shim.readFileBytes(`minnowdb/${name}/wal`);
    await expect(
      store.updateGarbageCollectionPlanning({
        jobId: job.id,
        expectedRevision: job.revision,
        discovery: { ...discovery, maxPlanningItems: 65 },
        updatedAt: "2026-08-24T00:10:01.000Z",
      }),
    ).rejects.toThrow(/immutable/);
    expect(await store.getGarbageCollectionJob(job.id)).toEqual(job);
    expect(shim.readFileBytes(`minnowdb/${name}/wal`)).toEqual(before);

    const completed = await store.updateGarbageCollectionPlanning({
      jobId: job.id,
      expectedRevision: job.revision,
      discovery: { ...discovery, phase: "complete", visitedRecords: 4 },
      updatedAt: "2026-08-24T00:10:01.000Z",
    });
    expect(completed).toMatchObject({ revision: 1, state: "completed" });
    store._crashForTests();
    const reopened = await OpfsBlockStore.open({ name, root: shim.root });
    expect(await reopened.getGarbageCollectionJob(job.id)).toEqual(completed);
    reopened.close();
  });

  it("never publishes an extent placement whose write stopped making progress", async () => {
    const shim = new MemoryOpfs();
    const store = await OpfsBlockStore.open({ name: "short-extent", root: shim.root });
    const block = await encodeBlock({ type: "number", values: [1, 2, 3] }, "raw");
    const transactionId = await createTestTransaction(store, "short-extent");
    const walBefore = shim.readFileBytes("minnowdb/short-extent/wal")?.byteLength ?? 0;
    shim.setTransferLimit((path, operation, requested, at) => {
      if (path.includes("/extents/") && operation === "write") {
        return at >= 7 ? 0 : Math.min(7, requested);
      }
      return requested;
    });
    await expect(
      store.stageTransactionArtifacts({
        transactionId,
        expectedRevision: 0,
        blocks: [{ id: "stalled", bytes: block }],
        segments: [],
        updatedAt: "2026-08-24T00:00:01.000Z",
      }),
    ).rejects.toThrow(/no progress.*extent 0/);
    expect(shim.readFileBytes("minnowdb/short-extent/wal")?.byteLength ?? 0).toBe(walBefore);
    shim.setTransferLimit(null);
    expect(await store.getBlock("stalled")).toBeUndefined();
    await store.stageTransactionArtifacts({
      transactionId,
      expectedRevision: 0,
      blocks: [{ id: "stalled", bytes: block }],
      segments: [],
      updatedAt: "2026-08-24T00:00:01.000Z",
    });
    expect(await store.getBlock("stalled")).toEqual(block);
    store.close();
  });

  it("refuses oversized artifact batches before changing records, WAL, or extents", async () => {
    const shim = new MemoryOpfs();
    const name = "artifact-batch-limit";
    const store = await OpfsBlockStore.open({ name, root: shim.root });
    const transactionId = await createTestTransaction(store, name);
    const blocks = Array.from({ length: MAX_TRANSACTION_STAGE_BLOCKS + 1 }, (_, index) => ({
      id: `oversized-${String(index)}`,
      bytes: Uint8Array.of(index & 0xff),
    }));
    const transactionBefore = await store.getTransaction(transactionId);
    const walBefore = shim.readFileBytes(`minnowdb/${name}/wal`);
    const extentBefore = shim.readFileBytes(`minnowdb/${name}/extents/000000`);

    await expect(
      store.stageTransactionArtifacts({
        transactionId,
        expectedRevision: 0,
        blocks,
        segments: [],
        updatedAt: "2026-08-24T00:00:01.000Z",
      }),
    ).rejects.toThrow(/exceeds 64 blocks/);
    expect(await store.getTransaction(transactionId)).toEqual(transactionBefore);
    expect(shim.readFileBytes(`minnowdb/${name}/wal`)).toEqual(walBefore);
    expect(shim.readFileBytes(`minnowdb/${name}/extents/000000`)).toEqual(extentBefore);

    await expect(
      store.writeTransaction({
        transaction: {
          record: {
            id: "oversized-single-shot",
            ownerId: "owner-oversized-single-shot",
            expiresAt: "2026-08-24T01:00:00.000Z",
            pendingBlockIds: [],
            pendingSegmentIds: [],
            status: "active",
            revision: 0,
            startedAt: "2026-08-24T00:00:00.000Z",
            updatedAt: "2026-08-24T00:00:00.000Z",
            committedVersion: null,
          },
        },
        expectedManifestVersion: null,
        blocks,
        segments: [],
        committedAt: "2026-08-24T00:00:01.000Z",
      }),
    ).rejects.toThrow(/exceeds 64 blocks/);
    expect(await store.getTransaction("oversized-single-shot")).toBeUndefined();
    expect(shim.readFileBytes(`minnowdb/${name}/wal`)).toEqual(walBefore);
    expect(shim.readFileBytes(`minnowdb/${name}/extents/000000`)).toEqual(extentBefore);
    store.close();
  });

  it("refuses oversized temp-page batches before creating any spill file", async () => {
    const shim = new MemoryOpfs();
    const name = "temp-batch-limit";
    const store = await OpfsBlockStore.open({ name, root: shim.root });
    const pages = Array.from({ length: MAX_TEMP_RUN_PAGES_PER_BATCH + 1 }, (_, pageIndex) => ({
      ownerId: "temp-owner",
      runId: "temp-run",
      pageIndex,
      bytes: Uint8Array.of(pageIndex & 0xff),
    }));
    await expect(store.putTempRunPages(pages)).rejects.toThrow(/exceeds 64 pages/);
    expect(shim.readFileBytes(`minnowdb/${name}/temp/temp-owner/temp-run/0`)).toBeUndefined();
    store.close();
  });

  it("refuses oversized temp identities before creating a path", async () => {
    const shim = new MemoryOpfs();
    const name = "temp-identity-limit";
    const store = await OpfsBlockStore.open({ name, root: shim.root });
    const ownerId = "x".repeat(MAX_STORAGE_ID_CHARACTERS + 1);
    await expect(
      store.putTempRunPage({
        ownerId,
        runId: "temp-run",
        pageIndex: 0,
        bytes: Uint8Array.of(1),
      }),
    ).rejects.toThrow(/Storage ID exceeds 1024 characters/);
    expect(shim.readFileBytes(`minnowdb/${name}/temp/${ownerId}/temp-run/0`)).toBeUndefined();
    store.close();
  });

  it("accounts temp pages across followers, overwrite deltas, crashes, and reopen", async () => {
    const shim = new MemoryOpfs();
    const name = "temp-ledger-routing";
    const leader = await OpfsBlockStore.open({ name, root: shim.root, rpcTimeoutMs: 100 });
    const follower = await OpfsBlockStore.open({ name, root: shim.root, rpcTimeoutMs: 100 });
    await leader.createTempOwner({
      ownerId: "temp-owner",
      createdAt: "2026-08-24T00:00:00.000Z",
      expiresAt: "2026-08-24T01:00:00.000Z",
      revision: 0,
    });
    await follower.putTempRunPage({
      ownerId: "temp-owner",
      runId: "run",
      pageIndex: 0,
      bytes: Uint8Array.of(1, 2, 3),
    });
    expect(await leader.getTempRunPage("temp-owner", "run", 0)).toEqual(Uint8Array.of(1, 2, 3));
    expect((await follower.getStorageStats()).temporaryBytes).toBe(3);
    await follower.putTempRunPage({
      ownerId: "temp-owner",
      runId: "run",
      pageIndex: 0,
      bytes: Uint8Array.of(4),
    });
    expect((await leader.getStorageStats()).temporaryBytes).toBe(1);
    follower.close();
    leader._crashForTests();

    const reopened = await OpfsBlockStore.open({ name, root: shim.root });
    expect(await reopened.getTempRunPage("temp-owner", "run", 0)).toEqual(Uint8Array.of(4));
    expect((await reopened.getStorageStats()).temporaryBytes).toBe(1);
    expect((await reopened.checkIntegrity({ mode: "full" })).ok).toBe(true);
    reopened.close();
  });

  it("reconciles a crash between a durable temp reservation and its refused file write", async () => {
    const shim = new MemoryOpfs();
    const name = "temp-ledger-refused-write";
    const store = await OpfsBlockStore.open({ name, root: shim.root });
    await store.createTempOwner({
      ownerId: "temp-owner",
      createdAt: "2026-08-24T00:00:00.000Z",
      expiresAt: "2026-08-24T01:00:00.000Z",
      revision: 0,
    });
    shim.setWriteFault((path, phase) => {
      if (path.includes("/temp/temp-owner/run/0") && phase === "write") {
        throw new DOMException("temp write refused", "QuotaExceededError");
      }
    });
    await expect(
      store.putTempRunPage({
        ownerId: "temp-owner",
        runId: "run",
        pageIndex: 0,
        bytes: Uint8Array.of(1, 2, 3),
      }),
    ).rejects.toMatchObject({ name: "QuotaExceededError" });
    shim.setWriteFault(null);
    store._crashForTests();

    const reopened = await OpfsBlockStore.open({ name, root: shim.root });
    expect(await reopened.getTempRunPage("temp-owner", "run", 0)).toBeUndefined();
    expect((await reopened.getStorageStats()).temporaryBytes).toBe(0);
    expect((await reopened.checkIntegrity()).ok).toBe(true);
    reopened.close();
  });

  it("bounds refused temp cleanup debt before admitting more files and recovers", async () => {
    const shim = new MemoryOpfs();
    const name = "temp-ledger-cleanup-debt";
    const store = await OpfsBlockStore.open({
      name,
      root: shim.root,
      cleanupLimitBytes: 64 * 1024,
    });
    await store.createTempOwner({
      ownerId: "temp-owner",
      createdAt: "2026-08-24T00:00:00.000Z",
      expiresAt: "2026-08-24T01:00:00.000Z",
      revision: 0,
    });
    await store.putTempRunPage({
      ownerId: "temp-owner",
      runId: "old",
      pageIndex: 0,
      bytes: Uint8Array.of(1),
    });
    shim.setDeleteFault((path) => {
      if (path.includes("/temp/temp-owner/old")) {
        throw new DOMException("temp delete refused", "NoModificationAllowedError");
      }
    });
    await store.removeTempRun("temp-owner", "old");
    const degraded = await store.getStorageStats();
    expect(degraded.maintenance?.cleanupDebtBytes).toBeGreaterThanOrEqual(64 * 1024);
    await expect(
      store.putTempRunPage({
        ownerId: "temp-owner",
        runId: "new",
        pageIndex: 0,
        bytes: Uint8Array.of(2),
      }),
    ).rejects.toMatchObject({ name: "QuotaExceededError" });
    expect(shim.readFileBytes(`minnowdb/${name}/temp/temp-owner/new/0`)).toBeUndefined();

    shim.setDeleteFault(null);
    await store.putTempRunPage({
      ownerId: "temp-owner",
      runId: "new",
      pageIndex: 0,
      bytes: Uint8Array.of(2),
    });
    expect(shim.readFileBytes(`minnowdb/${name}/temp/temp-owner/old/0`)).toBeUndefined();
    expect((await store.getStorageStats()).maintenance?.cleanupDebtBytes).toBe(0);
    store.close();
  });

  it("replays atomic aborted-segment adoption without an ownership crash window", async () => {
    const shim = new MemoryOpfs();
    const name = "adopt-aborted-segment";
    const store = await OpfsBlockStore.open({ name, root: shim.root, checkpointEntries: 1 });
    await store.addTable({ ...table("adoption"), id: "adoption-table" });
    const sourceBytes = await encodeBlock({ type: "number", values: [1] }, "raw");
    const outputBytes = await encodeBlock({ type: "number", values: [2] }, "raw");
    const sourceOwner = await createTestTransaction(store, "adoption-source");
    const sourceStaged = await store.stageTransactionArtifacts({
      transactionId: sourceOwner,
      expectedRevision: 0,
      blocks: [{ id: "adoption-source-block", bytes: sourceBytes }],
      segments: [
        {
          id: "adoption-source-segment",
          kind: "insert",
          level: 0,
          logicalOrder: 0,
          commitOrdinal: 0,
          rowIdSpans: [],
          tableId: "adoption-table",
          transactionId: sourceOwner,
          rowCount: 1,
          rowIdStart: 1n,
          rowIdEndExclusive: 2n,
          columnBlockIds: { c1: ["adoption-source-block"] },
          createdAt: "2026-08-24T00:00:01.000Z",
        },
      ],
      updatedAt: "2026-08-24T00:00:01.000Z",
    });
    await store.commitTransaction({
      transactionId: sourceOwner,
      expectedTransactionRevision: sourceStaged.revision,
      expectedManifestVersion: null,
      levelZeroSegmentLimits: [{ tableId: "adoption-table", limit: 10 }],
      committedAt: "2026-08-24T00:00:02.000Z",
    });
    const abortedOwner = await createTestTransaction(store, "adoption-aborted");
    const outputBlockId = "adoption-job/output/segment/000000/column/000000/part/000000";
    const outputSegment = {
      id: "adoption-output-segment",
      kind: "insert" as const,
      tableId: "adoption-table",
      transactionId: abortedOwner,
      rowCount: 1,
      rowIdStart: 1n,
      rowIdEndExclusive: 2n,
      columnBlockIds: { c1: [outputBlockId] },
      level: 1,
      logicalOrder: 0,
      commitOrdinal: 0,
      rowIdSpans: [],
      createdAt: "2026-08-24T00:00:03.000Z",
    };
    const outputStaged = await store.stageTransactionArtifacts({
      transactionId: abortedOwner,
      expectedRevision: 0,
      blocks: [{ id: outputBlockId, bytes: outputBytes }],
      segments: [outputSegment],
      updatedAt: "2026-08-24T00:00:03.000Z",
    });
    const aborted = await store.updateTransaction(abortedOwner, outputStaged.revision, {
      status: "aborted",
      updatedAt: "2026-08-24T00:00:04.000Z",
    });
    const replacementOwner = await createTestTransaction(store, "adoption-replacement");
    await store.createCompactionJob({
      id: "adoption-job",
      tableId: "adoption-table",
      sourceManifestVersion: 0,
      sourceSegmentIds: ["adoption-source-segment"],
      sourceBlockIds: ["adoption-source-block"],
      outputBlockIds: [outputBlockId],
      cursor: { sourceSegmentIndex: 1, sourceBlockIndex: 0 },
      processedRows: 1,
      sourceStoredBytes: sourceBytes.byteLength,
      outputStoredBytes: outputBytes.byteLength,
      logicalBytes: outputBytes.byteLength,
      rewritePlan: { kind: "copy-v1" },
      outputCursor: null,
      memoryBudgetBytes: 0,
      minimumMemoryBytes: 0,
      level0SourceStoredBytes: sourceBytes.byteLength,
      anchorSourceStoredBytes: 0,
      peakWorkingBytes: 0,
      outputLogicalBytes: outputBytes.byteLength,
      targetLevel: 1,
      state: "running",
      transactionId: replacementOwner,
      outputSegmentId: outputSegment.id,
      publishedVersion: null,
      revision: 0,
      createdAt: "2026-08-24T00:00:05.000Z",
      updatedAt: "2026-08-24T00:00:05.000Z",
    });
    await waitFor(
      () => (shim.readFileBytes(`minnowdb/${name}/wal`)?.byteLength ?? -1) === 0,
      "adoption fixture checkpoint",
    );
    store._crashForTests();

    const checkpointPaths = [`minnowdb/${name}/checkpoint-a`, `minnowdb/${name}/checkpoint-b`];
    const checkpoints = checkpointPaths.flatMap((path) => {
      const bytes = shim.readFileBytes(path);
      const value = bytes === undefined ? undefined : decodeSyncCheckpoint(bytes);
      return value === undefined ? [] : [{ path, value }];
    });
    const newest = checkpoints.sort(
      (left, right) =>
        ((right.value as { generation?: number }).generation ?? 0) -
        ((left.value as { generation?: number }).generation ?? 0),
    )[0];
    if (newest === undefined) throw new Error("Missing adoption checkpoint");
    const forged = structuredClone(newest.value) as {
      core: {
        compactionJobs: Array<{
          id: string;
          state: string;
          transactionId: string | null;
        }>;
        transactions: Array<{
          id: string;
          pendingBlockIds: string[];
          pendingSegmentIds: string[];
        }>;
      };
    };
    const replacement = forged.core.transactions.find(({ id }) => id === replacementOwner);
    if (replacement === undefined) throw new Error("Missing replacement transaction");
    expect(replacement.pendingSegmentIds).toEqual([]);
    expect(forged.core.compactionJobs.find(({ id }) => id === "adoption-job")).toMatchObject({
      state: "running",
      transactionId: replacementOwner,
    });
    replacement.pendingBlockIds = [outputBlockId];
    const encoded = encodeSyncCheckpoint(forged);
    for (const path of checkpointPaths) shim.writeFileBytes(path, encoded);

    const reopened = await OpfsBlockStore.open({ name, root: shim.root });
    const input = {
      segment: { ...outputSegment, transactionId: replacementOwner },
      expectedAbortedTransactionId: abortedOwner,
      expectedAbortedTransactionRevision: aborted.revision,
      replacementTransactionId: replacementOwner,
      expectedReplacementTransactionRevision: 0,
      compactionJobId: "adoption-job",
      updatedAt: "2026-08-24T00:00:06.000Z",
    };
    const adopted = await reopened.adoptAbortedSegment(input);
    expect(adopted).toMatchObject({ revision: 1, pendingSegmentIds: [outputSegment.id] });
    reopened._crashForTests();

    const recovered = await OpfsBlockStore.open({ name, root: shim.root });
    expect((await recovered.getSegment(outputSegment.id))?.transactionId).toBe(replacementOwner);
    expect(await recovered.getTransaction(abortedOwner)).toMatchObject({
      revision: aborted.revision + 1,
      pendingSegmentIds: [],
    });
    expect(await recovered.getTransaction(replacementOwner)).toMatchObject({
      revision: 1,
      pendingSegmentIds: [outputSegment.id],
    });
    recovered.close();
  });

  it("does not publish extent bytes when the following WAL write stalls", async () => {
    const shim = new MemoryOpfs();
    const store = await OpfsBlockStore.open({ name: "short-wal", root: shim.root });
    const block = await encodeBlock({ type: "string", values: ["durable"] }, "raw");
    const transactionId = await createTestTransaction(store, "short-wal");
    shim.setTransferLimit((path, operation, requested, at) => {
      if (path.endsWith("/wal") && operation === "write") {
        return at >= 9 ? 0 : Math.min(9, requested);
      }
      return requested;
    });
    await expect(
      store.stageTransactionArtifacts({
        transactionId,
        expectedRevision: 0,
        blocks: [{ id: "unpublished", bytes: block }],
        segments: [],
        updatedAt: "2026-08-24T00:00:01.000Z",
      }),
    ).rejects.toThrow(/no progress.*WAL frame/);
    shim.setTransferLimit(null);
    expect(await store.getBlock("unpublished")).toBeUndefined();
    store._crashForTests();

    const reopened = await OpfsBlockStore.open({ name: "short-wal", root: shim.root });
    expect(await reopened.getBlock("unpublished")).toBeUndefined();
    reopened.close();
  });

  it("reads multiple blocks concurrently from the same sealed extent", async () => {
    const shim = new MemoryOpfs();
    const store = await OpfsBlockStore.open({ name: "sealed-bulk-read", root: shim.root });
    const first = Uint8Array.of(1, 2, 3);
    const second = Uint8Array.of(4, 5, 6);
    await stageTestBlocks(
      store,
      [
        { id: "first", bytes: first },
        { id: "second", bytes: second },
      ],
      "sealed-read",
    );
    await stageTestBlocks(
      store,
      [{ id: "new-tail", bytes: new Uint8Array(9 * 1024 * 1024) }],
      "sealed-tail",
    );

    await expect(store.getBlocks(["first", "second"])).resolves.toEqual([first, second]);
    store.close();
  });

  it("refuses aggregate block reads above the byte ceiling before allocating payloads", () => {
    const half = Math.floor(MAX_BLOCK_READ_BATCH_BYTES / 2) + 1;
    expect(() => assertBlockReadBatchByteLimit([{ length: half }, { length: half }])).toThrow(
      expect.objectContaining({
        name: "BlockReadBatchTooLargeError",
        requestedBytes: half * 2,
        limitBytes: MAX_BLOCK_READ_BATCH_BYTES,
      }),
    );
    expect(() =>
      assertBlockReadBatchByteLimit([
        { length: Math.floor(MAX_BLOCK_READ_BATCH_BYTES / 2) },
        { length: Math.ceil(MAX_BLOCK_READ_BATCH_BYTES / 2) },
      ]),
    ).not.toThrow();
  });

  it("bounds manifest membership and reports a readable member with missing bytes as corruption", async () => {
    const shim = new MemoryOpfs();
    const name = "manifest-membership-corruption";
    const store = await OpfsBlockStore.open({ name, root: shim.root });
    const transactionId = await createTestTransaction(store, "manifest-member");
    const staged = await store.stageTransactionArtifacts({
      transactionId,
      expectedRevision: 0,
      blocks: [{ id: "manifest-member", bytes: Uint8Array.of(1, 2, 3) }],
      segments: [],
      updatedAt: "2026-08-24T00:00:01.000Z",
    });
    await store.commitTransaction({
      transactionId,
      expectedTransactionRevision: staged.revision,
      expectedManifestVersion: null,
      levelZeroSegmentLimits: [],
      committedAt: "2026-08-24T00:00:02.000Z",
    });
    expect(await store.hasManifestBlocks(0, ["manifest-member", "missing"])).toEqual([true, false]);
    await expect(
      store.hasManifestBlocks(
        0,
        Array.from({ length: MAX_MANIFEST_BLOCK_PRESENCE_IDS + 1 }, () => "manifest-member"),
      ),
    ).rejects.toThrow(/cannot exceed 1024 items/);

    // Rolling the tail closes extent zero. Removing that immutable file simulates external
    // storage loss after the manifest and placement were durably published.
    await stageTestBlocks(
      store,
      [{ id: "new-tail", bytes: new Uint8Array(9 * 1024 * 1024) }],
      "manifest-new-tail",
    );
    const namespace = await shim.root.getDirectoryHandle("minnowdb");
    const database = await namespace.getDirectoryHandle(name);
    const extents = await database.getDirectoryHandle("extents");
    await extents.removeEntry("000000");
    await expect(store.readManifestBlock(0, "manifest-member")).rejects.toMatchObject({
      name: "StorageCorruptionError",
      backend: "opfs",
      location: "blocks/manifest-member",
    });
    expect(await store.readManifestBlock(0, "missing")).toBeUndefined();
    store.close();
  });

  it("recovers only the fully stored prefix of a relaxed WAL", async () => {
    const shim = new MemoryOpfs();
    const store = await OpfsBlockStore.open({
      name: "stored-prefix",
      root: shim.root,
      durability: "relaxed",
    });
    const first = await encodeBlock({ type: "number", values: [1] }, "raw");
    const second = await encodeBlock({ type: "number", values: [2] }, "raw");
    const dependent = await encodeBlock({ type: "number", values: [99] }, "raw");
    await stageTestBlocks(store, [{ id: "first", bytes: first }], "stored-first");
    await stageTestBlocks(store, [{ id: "second", bytes: second }], "stored-second");
    await stageTestBlocks(store, [{ id: "dependent", bytes: dependent }], "stored-dependent");
    store._crashForTests();

    // All three checksum-valid WAL frames survived, but the second frame's referenced extent
    // payload did not. Recovery must retain the first and discard it plus the later dependent
    // frame, even though that third payload itself is intact.
    const extentPath = "minnowdb/stored-prefix/extents/000000";
    const extent = shim.readFileBytes(extentPath);
    if (extent === undefined) throw new Error("Expected the test extent");
    extent[first.byteLength + second.byteLength - 1] =
      (extent[first.byteLength + second.byteLength - 1] ?? 0) ^ 0xff;
    shim.writeFileBytes(extentPath, extent);

    const recovered = await OpfsBlockStore.open({
      name: "stored-prefix",
      root: shim.root,
      durability: "relaxed",
    });
    expect(await recovered.getBlock("first")).toEqual(first);
    expect(await recovered.getBlock("second")).toBeUndefined();
    expect(await recovered.getBlock("dependent")).toBeUndefined();
    const replacement = await encodeBlock({ type: "number", values: [3] }, "raw");
    await stageTestBlocks(
      recovered,
      [{ id: "replacement", bytes: replacement }],
      "stored-replacement",
    );
    recovered._crashForTests();

    const reopened = await OpfsBlockStore.open({
      name: "stored-prefix",
      root: shim.root,
      durability: "relaxed",
    });
    expect(await reopened.getBlock("first")).toEqual(first);
    expect(await reopened.getBlock("second")).toBeUndefined();
    expect(await reopened.getBlock("dependent")).toBeUndefined();
    expect(await reopened.getBlock("replacement")).toEqual(replacement);
    reopened.close();
  });

  it("rejects placement overflow before recovery allocates or reads payload bytes", async () => {
    const shim = new MemoryOpfs();
    const empty = await OpfsBlockStore.open({ name: "placement-overflow", root: shim.root });
    empty._crashForTests();
    const handle = await new OpfsTree(shim.root).openHandle(
      ["minnowdb", "placement-overflow", "wal"],
      { create: false },
    );
    new WalWriter(handle, 0).append(
      {
        seq: 1,
        op: "stageTransactionArtifacts",
        transactionId: "overflow-transaction",
        expectedRevision: 0,
        blocks: [
          {
            id: "overflow",
            placement: {
              extent: 0,
              offset: Number.MAX_SAFE_INTEGER,
              length: 2,
              checksum: 0,
            },
          },
        ],
        segments: [],
        updatedAt: "2026-08-24T00:00:00.000Z",
      },
      false,
    );
    handle.close();

    await expect(
      OpfsBlockStore.open({ name: "placement-overflow", root: shim.root }),
    ).rejects.toThrow(/Invalid extent placement/);
  });

  it("rejects a sparse over-limit WAL before reading any of it", async () => {
    const tree = new OpfsTree(new MemoryOpfs().root);
    const slotA = await tree.openHandle(["slot-a"], { create: true });
    const slotB = await tree.openHandle(["slot-b"], { create: true });
    let reads = 0;
    const sparseWal: SyncFileHandle = {
      getSize: () => MAX_OPFS_WAL_BYTES + 1,
      read: () => {
        reads += 1;
        return 0;
      },
      write: () => 0,
      truncate: () => undefined,
      flush: () => undefined,
      close: () => undefined,
    };
    await expect(
      OpfsLeader.recover(tree, true, {
        wal: sparseWal,
        slotA,
        slotB,
      }),
    ).rejects.toThrow(/WAL exceeds its .* byte recovery limit/);
    expect(reads).toBe(0);
  });

  it("rejects a sparse over-limit checkpoint before allocating or reading it", async () => {
    const tree = new OpfsTree(new MemoryOpfs().root);
    const wal = await tree.openHandle(["wal"], { create: true });
    const slotB = await tree.openHandle(["slot-b"], { create: true });
    let reads = 0;
    const sparseSlot: SyncFileHandle = {
      getSize: () => MAX_OPFS_CHECKPOINT_BYTES + 1,
      read: () => {
        reads += 1;
        return 0;
      },
      write: () => 0,
      truncate: () => undefined,
      flush: () => undefined,
      close: () => undefined,
    };
    await expect(OpfsLeader.recover(tree, true, { wal, slotA: sparseSlot, slotB })).rejects.toThrow(
      /Every OPFS checkpoint copy is corrupt/,
    );
    expect(reads).toBe(0);
  });

  it("refuses corrupt strictly durable payloads instead of silently rolling them back", async () => {
    const shim = new MemoryOpfs();
    const store = await OpfsBlockStore.open({
      name: "strict-corruption",
      root: shim.root,
      durability: "strict",
    });
    const block = await encodeBlock({ type: "number", values: [7] }, "raw");
    await stageTestBlocks(store, [{ id: "strict", bytes: block }], "strict-corrupt");
    store._crashForTests();
    const path = "minnowdb/strict-corruption/extents/000000";
    const extent = shim.readFileBytes(path);
    if (extent === undefined) throw new Error("Expected the strict test extent");
    extent[extent.byteLength - 1] = (extent[extent.byteLength - 1] ?? 0) ^ 0xff;
    shim.writeFileBytes(path, extent);
    await expect(
      OpfsBlockStore.open({ name: "strict-corruption", root: shim.root, durability: "strict" }),
    ).rejects.toThrow(/Strict OPFS recovery found an invalid stored payload/);
  });

  it("reports payload corruption through a bounded full integrity scan", async () => {
    const shim = new MemoryOpfs();
    const store = await OpfsBlockStore.open({ name: "integrity-corruption", root: shim.root });
    const block = await encodeBlock({ type: "number", values: [1, 2, 3] }, "raw");
    await stageTestBlocks(store, [{ id: "damaged", bytes: block }], "integrity-corrupt");
    const path = "minnowdb/integrity-corruption/extents/000000";
    const bytes = shim.readFileBytes(path);
    if (bytes === undefined) throw new Error("Expected an extent payload");
    shim.corruptFileByte(path, bytes.length - 1);

    expect((await store.checkIntegrity({ mode: "metadata" })).ok).toBe(true);
    const full = await store.checkIntegrity({ mode: "full", maxIssues: 1 });
    expect(full).toMatchObject({ ok: false, issueCount: 1, checkedBlocks: 1 });
    expect(full.issues).toHaveLength(1);
    expect(full.issues[0]).toMatchObject({ code: "payload-checksum", location: "blocks/damaged" });
    store._crashForTests();
  });

  it("never blesses an invalid block envelope while relocating an extent", async () => {
    const shim = new MemoryOpfs();
    const cleanupLimitBytes = 1024 * 1024;
    const store = await OpfsBlockStore.open({
      name: "relocation-inner-crc",
      root: shim.root,
      cleanupLimitBytes,
    });
    const large = await encodeBlock(
      { type: "string", values: ["x".repeat(6 * 1024 * 1024)] },
      "raw",
    );
    const corrupt = await encodeBlock({ type: "number", values: [7, 8, 9] }, "raw");
    corrupt[corrupt.byteLength - 1] = (corrupt[corrupt.byteLength - 1] ?? 0) ^ 0xff;
    const rollover = await encodeBlock(
      { type: "string", values: ["y".repeat(3 * 1024 * 1024)] },
      "raw",
    );
    const blockedTransactionId = await createTestTransaction(store, "blocked-relocation");
    // The logical removal is durable and acknowledged even though best-effort compaction
    // refuses to copy and re-checksum the malformed survivor.
    await retireBlockThroughGc(
      store,
      "large",
      [
        { id: "large", bytes: large },
        { id: "inner-corrupt", bytes: corrupt },
        { id: "rollover", bytes: rollover },
      ],
      "relocation-corruption-gc",
    );
    expect(await store.getBlock("large")).toBeUndefined();
    const full = await store.checkIntegrity({ mode: "full" });
    expect(full.ok).toBe(false);
    expect(full.issues).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ code: "payload-checksum", location: "blocks/inner-corrupt" }),
        expect.objectContaining({ code: "cleanup-degraded", location: "extents" }),
      ]),
    );
    const atLimit = await store.getStorageStats();
    expect(atLimit.maintenance?.cleanupDebtBytes).toBeGreaterThan(cleanupLimitBytes);
    for (let attempt = 0; attempt < 5; attempt += 1) {
      await expect(
        store.stageTransactionArtifacts({
          transactionId: blockedTransactionId,
          expectedRevision: 0,
          blocks: [{ id: `blocked-${String(attempt)}`, bytes: rollover }],
          segments: [],
          updatedAt: "2026-08-24T00:00:01.000Z",
        }),
      ).rejects.toMatchObject({ name: "QuotaExceededError" });
    }
    expect((await store.getStorageStats()).physicalBytes).toBe(atLimit.physicalBytes);
    store._crashForTests();
  });

  it("relocates a high-cardinality extent in bounded durable batches", async () => {
    const shim = new MemoryOpfs();
    const name = "bounded-relocation";
    const store = await OpfsBlockStore.open({ name, root: shim.root });
    const small = await encodeBlock({ type: "number", values: [1] }, "raw");
    const smallIds = Array.from({ length: 300 }, (_, index) => `small-${String(index)}`);
    for (let offset = 0; offset < smallIds.length; offset += 60) {
      const ids = smallIds.slice(offset, offset + 60);
      const transactionId = await createTestTransaction(store, `relocation-${String(offset)}`);
      await store.stageTransactionArtifacts({
        transactionId,
        expectedRevision: 0,
        blocks: ids.map((id) => ({ id, bytes: small })),
        segments: [],
        updatedAt: "2026-08-24T00:00:01.000Z",
      });
    }
    const large = await encodeBlock(
      { type: "string", values: ["x".repeat(6 * 1024 * 1024)] },
      "raw",
    );
    const rollover = await encodeBlock(
      { type: "string", values: ["y".repeat(3 * 1024 * 1024)] },
      "raw",
    );
    await retireBlockThroughGc(
      store,
      "relocation-large",
      [
        { id: "relocation-large", bytes: large },
        { id: "relocation-rollover", bytes: rollover },
      ],
      "bounded-relocation-gc",
    );
    await waitFor(
      () => shim.readFileBytes(`minnowdb/${name}/extents/000000`) === undefined,
      "bounded relocation batches to drain their source extent",
    );
    expect(await store.getBlocks(smallIds)).toEqual(smallIds.map(() => small));
    store._crashForTests();

    const reopened = await OpfsBlockStore.open({ name, root: shim.root });
    expect(await reopened.getBlocks(smallIds)).toEqual(smallIds.map(() => small));
    expect((await reopened.checkIntegrity({ mode: "full" })).ok).toBe(true);
    reopened.close();
  });

  it("acknowledges committed removal across a transient extent-delete failure and retries cleanup", async () => {
    const shim = new MemoryOpfs();
    const store = await OpfsBlockStore.open({ name: "cleanup-ack", root: shim.root });
    const large = await encodeBlock(
      { type: "string", values: ["x".repeat(6 * 1024 * 1024)] },
      "raw",
    );
    const survivor = await encodeBlock({ type: "number", values: [1, 2, 3] }, "raw");
    const rollover = await encodeBlock(
      { type: "string", values: ["y".repeat(3 * 1024 * 1024)] },
      "raw",
    );
    let failures = 0;
    shim.setDeleteFault((path) => {
      if (path.endsWith("/extents/000000") && failures === 0) {
        failures += 1;
        throw new DOMException("injected delete refusal", "NoModificationAllowedError");
      }
    });
    await retireBlockThroughGc(
      store,
      "remove",
      [
        { id: "remove", bytes: large },
        { id: "survivor", bytes: survivor },
        { id: "rollover", bytes: rollover },
      ],
      "cleanup-ack-gc",
    );
    expect(await store.getBlock("remove")).toBeUndefined();
    expect(await store.getBlock("survivor")).toEqual(survivor);
    await waitFor(
      () => shim.readFileBytes("minnowdb/cleanup-ack/extents/000000") === undefined,
      "transiently refused extent deletion to retry",
    );
    expect(failures).toBe(1);
    expect((await store.getStorageStats()).maintenance?.degraded).toBe(false);
    store.close();
  });

  it("bounds persistent cleanup debt and resumes byte-growing work after reclamation recovers", async () => {
    const shim = new MemoryOpfs();
    const cleanupLimitBytes = 1024 * 1024;
    const store = await OpfsBlockStore.open({
      name: "cleanup-backpressure",
      root: shim.root,
      cleanupLimitBytes,
    });
    const drained = await encodeBlock(
      { type: "string", values: ["x".repeat(2 * 1024 * 1024)] },
      "raw",
    );
    const rollover = await encodeBlock(
      { type: "string", values: ["y".repeat(7 * 1024 * 1024)] },
      "raw",
    );
    const later = await encodeBlock({ type: "number", values: [42] }, "raw");
    const blockedTransactionId = await createTestTransaction(store, "blocked-cleanup");
    shim.setDeleteFault((path) => {
      if (path.endsWith("/extents/000000")) {
        throw new DOMException("persistent delete refusal", "NoModificationAllowedError");
      }
    });

    await retireBlockThroughGc(
      store,
      "drained",
      [
        { id: "drained", bytes: drained },
        { id: "rollover", bytes: rollover },
      ],
      "cleanup-backpressure-gc",
    );
    const degraded = await store.getStorageStats();
    expect(degraded.maintenance).toMatchObject({
      degraded: true,
      cleanupLimitBytes,
    });
    expect(degraded.maintenance?.cleanupDebtBytes).toBeGreaterThan(cleanupLimitBytes);
    const physicalAtLimit = degraded.physicalBytes;
    for (let attempt = 0; attempt < 5; attempt += 1) {
      await expect(
        store.stageTransactionArtifacts({
          transactionId: blockedTransactionId,
          expectedRevision: 0,
          blocks: [{ id: `blocked-${String(attempt)}`, bytes: later }],
          segments: [],
          updatedAt: "2026-08-24T00:00:01.000Z",
        }),
      ).rejects.toMatchObject({ name: "QuotaExceededError" });
    }
    expect((await store.getStorageStats()).physicalBytes).toBe(physicalAtLimit);

    shim.setDeleteFault(null);
    await store.stageTransactionArtifacts({
      transactionId: blockedTransactionId,
      expectedRevision: 0,
      blocks: [{ id: "after-cleanup", bytes: later }],
      segments: [],
      updatedAt: "2026-08-24T00:00:01.000Z",
    });
    expect(await store.getBlock("after-cleanup")).toEqual(later);
    expect((await store.getStorageStats()).maintenance).toMatchObject({
      degraded: false,
      cleanupDebtBytes: 0,
      cleanupLimitBytes,
    });
    store.close();
  });

  it("finishes partial checkpoint transfers before flushing and resetting the WAL", async () => {
    const shim = new MemoryOpfs();
    const store = await OpfsBlockStore.open({
      name: "partial-checkpoint",
      root: shim.root,
      checkpointEntries: 1,
    });
    shim.setTransferLimit((path, operation, requested) =>
      path.includes("checkpoint-") && operation === "write" ? Math.min(7, requested) : requested,
    );
    await store.addTable(table("partial"));
    await waitFor(
      () =>
        (shim.readFileBytes("minnowdb/partial-checkpoint/checkpoint-b")?.byteLength ?? 0) > 7 &&
        (shim.readFileBytes("minnowdb/partial-checkpoint/wal")?.byteLength ?? 0) === 0,
      "partial checkpoint write to finish before WAL reset",
    );
    store._crashForTests();

    // Keep short reads enabled: recovery must fill the complete checkpoint buffer too.
    const reopened = await OpfsBlockStore.open({ name: "partial-checkpoint", root: shim.root });
    expect((await reopened.listTables()).map((record) => record.name)).toEqual(["partial"]);
    reopened.close();
  });

  it("keeps the WAL when a partial checkpoint write stops making progress", async () => {
    const shim = new MemoryOpfs();
    const store = await OpfsBlockStore.open({
      name: "stalled-checkpoint",
      root: shim.root,
      checkpointEntries: 1,
    });
    shim.setTransferLimit((path, operation, requested, at) => {
      if (!path.includes("checkpoint-") || operation !== "write") return requested;
      return at >= 9 ? 0 : Math.min(9, requested);
    });
    await store.addTable(table("wal-backed"));
    await waitFor(
      () => (shim.readFileBytes("minnowdb/stalled-checkpoint/checkpoint-b")?.byteLength ?? 0) === 9,
      "checkpoint write to stall",
    );
    expect(shim.readFileBytes("minnowdb/stalled-checkpoint/wal")?.byteLength ?? 0).toBeGreaterThan(
      0,
    );
    shim.setTransferLimit(null);
    store._crashForTests();

    const recovered = await OpfsBlockStore.open({ name: "stalled-checkpoint", root: shim.root });
    expect((await recovered.listTables()).map((record) => record.name)).toEqual(["wal-backed"]);
    recovered.close();
  });

  it("orders strict and checkpoint durability flushes before publication", async () => {
    const shim = new MemoryOpfs();
    const events: string[] = [];
    const strict = await OpfsBlockStore.open({
      name: "flush-order-strict",
      root: shim.root,
      durability: "strict",
    });
    const block = await encodeBlock({ type: "number", values: [42] }, "raw");
    const transactionId = await createTestTransaction(strict, "strict-flush");
    shim.setWriteFault((path, phase) => events.push(`${path}:${phase}`));
    await strict.stageTransactionArtifacts({
      transactionId,
      expectedRevision: 0,
      blocks: [
        { id: "strict-a", bytes: block },
        { id: "strict-b", bytes: block },
        { id: "strict-c", bytes: block },
      ],
      segments: [],
      updatedAt: "2026-08-24T00:00:01.000Z",
    });
    const strictExtentFlushes = events
      .map((event, index) => ({ event, index }))
      .filter(({ event }) => event.includes("/extents/") && event.endsWith(":flush"));
    const strictWalFlushes = events
      .map((event, index) => ({ event, index }))
      .filter(({ event }) => event.endsWith("/wal:flush"));
    expect(strictExtentFlushes).toHaveLength(1);
    expect(strictWalFlushes).toHaveLength(1);
    const strictExtentFlush = strictExtentFlushes[0]?.index ?? -1;
    const strictWalFlush = strictWalFlushes[0]?.index ?? -1;
    expect(strictExtentFlush).toBeGreaterThanOrEqual(0);
    expect(strictWalFlush).toBeGreaterThan(strictExtentFlush);
    strict.close();

    events.length = 0;
    shim.setWriteFault(null);
    const multiExtent = await OpfsBlockStore.open({
      name: "flush-order-multi-extent",
      root: shim.root,
      durability: "strict",
    });
    const multiTransactionId = await createTestTransaction(multiExtent, "multi-flush");
    shim.setWriteFault((path, phase) => events.push(`${path}:${phase}`));
    await multiExtent.stageTransactionArtifacts({
      transactionId: multiTransactionId,
      expectedRevision: 0,
      blocks: [
        { id: "large-a", bytes: new Uint8Array(9 * 1024 * 1024) },
        { id: "large-b", bytes: new Uint8Array(9 * 1024 * 1024) },
      ],
      segments: [],
      updatedAt: "2026-08-24T00:00:01.000Z",
    });
    const multiExtentFlushes = events.filter(
      (event) => event.includes("/extents/") && event.endsWith(":flush"),
    );
    const multiWalFlush = events.findIndex((event) => event.endsWith("/wal:flush"));
    expect(multiExtentFlushes).toHaveLength(2);
    expect(multiWalFlush).toBeGreaterThan(
      events.lastIndexOf(multiExtentFlushes[multiExtentFlushes.length - 1] ?? ""),
    );
    multiExtent.close();

    events.length = 0;
    shim.setWriteFault(null);
    const relaxed = await OpfsBlockStore.open({
      name: "flush-order-checkpoint",
      root: shim.root,
      checkpointEntries: 2,
    });
    const checkpointTransactionId = await createTestTransaction(relaxed, "checkpoint-flush");
    shim.setWriteFault((path, phase) => events.push(`${path}:${phase}`));
    await relaxed.stageTransactionArtifacts({
      transactionId: checkpointTransactionId,
      expectedRevision: 0,
      blocks: [{ id: "checkpointed", bytes: block }],
      segments: [],
      updatedAt: "2026-08-24T00:00:01.000Z",
    });
    await waitFor(
      () => (shim.readFileBytes("minnowdb/flush-order-checkpoint/wal")?.byteLength ?? 0) === 0,
      "checkpoint WAL reset",
    );
    const extentFlush = events.findIndex(
      (event) => event.includes("/extents/") && event.endsWith(":flush"),
    );
    const checkpointFlush = events.findIndex(
      (event) => event.includes("checkpoint-") && event.endsWith(":flush"),
    );
    const walReset = events.findIndex(
      (event, index) => index > checkpointFlush && event.endsWith("/wal:write"),
    );
    expect(extentFlush).toBeGreaterThanOrEqual(0);
    expect(checkpointFlush).toBeGreaterThan(extentFlush);
    expect(walReset).toBeGreaterThan(checkpointFlush);
    relaxed.close();
  });

  it("reloads after a coalesced strict payload flush is refused", async () => {
    const shim = new MemoryOpfs();
    const store = await OpfsBlockStore.open({
      name: "strict-flush-refusal",
      root: shim.root,
      durability: "strict",
    });
    shim.setWriteFault((path, phase) => {
      if (path.includes("/extents/") && phase === "flush") {
        throw new DOMException("The flush was refused", "QuotaExceededError");
      }
    });
    await expect(
      stageTestBlocks(
        store,
        [
          { id: "a", bytes: Uint8Array.of(1) },
          { id: "b", bytes: Uint8Array.of(2) },
        ],
        "refused-flush",
      ),
    ).rejects.toMatchObject({ name: "QuotaExceededError" });

    shim.setWriteFault(null);
    expect(await store.getBlocks(["a", "b"])).toEqual([undefined, undefined]);
    await stageTestBlocks(
      store,
      [
        { id: "a", bytes: Uint8Array.of(3) },
        { id: "b", bytes: Uint8Array.of(4) },
      ],
      "recovered-flush",
    );
    expect(await store.getBlocks(["a", "b"])).toEqual([Uint8Array.of(3), Uint8Array.of(4)]);
    store.close();
  });

  it("survives a torn WAL tail: the last frame is invisible, the rest intact", async () => {
    const shim = new MemoryOpfs();
    const store = await OpfsBlockStore.open({ name: "db", root: shim.root });
    await store.addTable(table("first"));
    await store.addTable(table("second"));
    store._crashForTests();

    // A crash mid-append leaves a torn frame at the tail.
    const wal = shim.readFileBytes("minnowdb/db/wal") ?? new Uint8Array(0);
    const torn = new Uint8Array(wal.byteLength + 7);
    torn.set(wal);
    torn.set([0x4d, 0x4e, 0x57, 0x4c, 0xff, 0xff, 0xff], wal.byteLength);
    shim.writeFileBytes("minnowdb/db/wal", torn);

    const reopened = await OpfsBlockStore.open({ name: "db", root: shim.root });
    expect((await reopened.listTables()).map((record) => record.name)).toEqual(["first", "second"]);
    await reopened.addTable(table("third"));
    expect(await reopened.listTables()).toHaveLength(3);
    reopened.close();
  });

  it("fails closed on a complete checksum-invalid WAL frame", async () => {
    const shim = new MemoryOpfs();
    const store = await OpfsBlockStore.open({ name: "corrupt-wal", root: shim.root });
    await store.addTable(table("durable"));
    store._crashForTests();

    const path = "minnowdb/corrupt-wal/wal";
    const wal = shim.readFileBytes(path);
    if (wal === undefined || wal.byteLength === 0) throw new Error("Expected WAL bytes");
    wal[wal.byteLength - 1] = (wal[wal.byteLength - 1] ?? 0) ^ 0xff;
    shim.writeFileBytes(path, wal);

    await expect(OpfsBlockStore.open({ name: "corrupt-wal", root: shim.root })).rejects.toThrow(
      /WAL frame checksum mismatch/,
    );
  });

  it("recovers everything acknowledged before a death with no shutdown", async () => {
    const shim = new MemoryOpfs();
    const store = await OpfsBlockStore.open({ name: "db", root: shim.root });
    for (let index = 0; index < 20; index += 1) await store.addTable(table(`t${String(index)}`));
    store._crashForTests(); // No flush, no checkpoint, no goodbye — just released locks.

    const reopened = await OpfsBlockStore.open({ name: "db", root: shim.root });
    expect(await reopened.listTables()).toHaveLength(20);
    reopened.close();
  });

  it("recovers an interrupted postings build without publishing partial chunks", async () => {
    const shim = new MemoryOpfs();
    const store = await OpfsBlockStore.open({ name: "postings-build", root: shim.root });
    await store.addTable({
      ...table("postings"),
      id: "table",
      ftsColumns: {
        index: {
          storage: "fts-chunks-v1",
          tokenizerVersion: 1,
          state: "building",
          buildFromVersion: -1,
        },
      },
    });
    await store.beginFtsBaseBuild({
      tableId: "table",
      columnId: "index",
      buildId: "interrupted",
      ownerId: "interrupted-owner",
      createdAt: "2026-08-24T00:00:00.000Z",
      expiresAt: "2026-08-24T01:00:00.000Z",
    });
    await store.writeFtsBaseBuildChunk({
      tableId: "table",
      columnId: "index",
      buildId: "interrupted",
      ownerId: "interrupted-owner",
      expiresAtCutoff: "2026-08-24T00:01:00.000Z",
      expiresAt: "2026-08-24T01:00:00.000Z",
      updatedAt: "2026-08-24T00:01:00.000Z",
      ordinal: 0,
      chunk: [{ term: "partial", rowIds: [1n], tf: [1] }],
    });
    store._crashForTests();

    const recovered = await OpfsBlockStore.open({ name: "postings-build", root: shim.root });
    expect(
      await recovered.readFtsCandidates("table", "index", [{ term: "partial", prefix: false }], 9),
    ).toMatchObject({ rowIdsByTerm: [[]], coversVersion: -1 });

    const beforeReplay = await recovered.getStorageStats();
    await recovered.writeFtsBaseBuildChunk({
      tableId: "table",
      columnId: "index",
      buildId: "interrupted",
      ownerId: "interrupted-owner",
      expiresAtCutoff: "2026-08-24T00:02:00.000Z",
      expiresAt: "2026-08-24T01:00:00.000Z",
      updatedAt: "2026-08-24T00:02:00.000Z",
      ordinal: 0,
      chunk: [{ term: "partial", rowIds: [1n], tf: [1] }],
    });
    const afterReplay = await recovered.getStorageStats();
    expect(afterReplay.physicalBytes).toBe(beforeReplay.physicalBytes);
    expect(afterReplay.walBytes).toBe(beforeReplay.walBytes);
    await expect(
      recovered.writeFtsBaseBuildChunk({
        tableId: "table",
        columnId: "index",
        buildId: "interrupted",
        ownerId: "interrupted-owner",
        expiresAtCutoff: "2026-08-24T00:02:00.000Z",
        expiresAt: "2026-08-24T01:00:00.000Z",
        updatedAt: "2026-08-24T00:02:00.000Z",
        ordinal: 0,
        chunk: [{ term: "changed", rowIds: [1n], tf: [1] }],
      }),
    ).rejects.toBeInstanceOf(PostingBuildConflictError);
    expect((await recovered.getStorageStats()).physicalBytes).toBe(beforeReplay.physicalBytes);
    recovered._crashForTests();

    const replayed = await OpfsBlockStore.open({ name: "postings-build", root: shim.root });
    await replayed.writeFtsBaseBuildChunk({
      tableId: "table",
      columnId: "index",
      buildId: "interrupted",
      ownerId: "interrupted-owner",
      expiresAtCutoff: "2026-08-24T00:03:00.000Z",
      expiresAt: "2026-08-24T01:00:00.000Z",
      updatedAt: "2026-08-24T00:03:00.000Z",
      ordinal: 0,
      chunk: [{ term: "partial", rowIds: [1n], tf: [1] }],
    });

    // Restarting the same logical build discards the recovered staging generation. Only the
    // final pointer swap makes any of the replacement chunks visible to readers.
    await replayed.beginFtsBaseBuild({
      tableId: "table",
      columnId: "index",
      buildId: "replacement",
      ownerId: "replacement-owner",
      createdAt: "2026-08-24T01:00:00.000Z",
      expiresAt: "2026-08-24T02:00:00.000Z",
    });
    await replayed.writeFtsBaseBuildChunk({
      tableId: "table",
      columnId: "index",
      buildId: "replacement",
      ownerId: "replacement-owner",
      expiresAtCutoff: "2026-08-24T01:01:00.000Z",
      expiresAt: "2026-08-24T02:00:00.000Z",
      updatedAt: "2026-08-24T01:01:00.000Z",
      ordinal: 0,
      chunk: [{ term: "omega", rowIds: [2n], tf: [1] }],
    });
    // Build chunks are row windows, so their locally sorted terms may overlap or restart.
    await replayed.writeFtsBaseBuildChunk({
      tableId: "table",
      columnId: "index",
      buildId: "replacement",
      ownerId: "replacement-owner",
      expiresAtCutoff: "2026-08-24T01:02:00.000Z",
      expiresAt: "2026-08-24T02:00:00.000Z",
      updatedAt: "2026-08-24T01:02:00.000Z",
      ordinal: 1,
      chunk: [{ term: "alpha", rowIds: [3n], tf: [1] }],
    });
    await replayed.finishFtsBaseBuild({
      tableId: "table",
      columnId: "index",
      buildId: "replacement",
      ownerId: "replacement-owner",
      expiresAtCutoff: "2026-08-24T01:03:00.000Z",
      coversVersion: 9,
      chunkCount: 2,
      totalTokens: 2,
      completedAt: "2026-08-24T01:03:00.000Z",
    });
    replayed._crashForTests();

    const published = await OpfsBlockStore.open({ name: "postings-build", root: shim.root });
    expect(
      await published.readFtsCandidates(
        "table",
        "index",
        [
          { term: "partial", prefix: false },
          { lower: "alpha", lowerInclusive: true, upper: "omega", upperInclusive: true },
        ],
        9,
      ),
    ).toMatchObject({ rowIdsByTerm: [[], [2n, 3n]], coversVersion: 9 });
    published.close();
  });

  it("enforces the global postings-build quota before WAL or extent mutation and reopens", async () => {
    const shim = new MemoryOpfs();
    const name = "postings-build-quota";
    const store = await OpfsBlockStore.open({ name, root: shim.root });
    const ftsColumns = Object.fromEntries(
      Array.from({ length: MAX_ACTIVE_FTS_BASE_BUILDS + 1 }, (_, index) => [
        `posting-${String(index)}`,
        {
          storage: "fts-chunks-v1" as const,
          tokenizerVersion: 1,
          state: "building" as const,
          buildFromVersion: -1,
        },
      ]),
    );
    await store.addTable({ ...table("posting-quota"), id: "table", ftsColumns });
    for (let index = 0; index < MAX_ACTIVE_FTS_BASE_BUILDS; index += 1) {
      await store.beginFtsBaseBuild({
        tableId: "table",
        columnId: `posting-${String(index)}`,
        buildId: `build-${String(index)}`,
        ownerId: `owner-${String(index)}`,
        createdAt: "2026-08-24T00:00:00.000Z",
        expiresAt: "2026-08-24T01:00:00.000Z",
      });
    }
    const before = await store.getStorageStats();
    await expect(
      store.beginFtsBaseBuild({
        tableId: "table",
        columnId: `posting-${String(MAX_ACTIVE_FTS_BASE_BUILDS)}`,
        buildId: "over-limit",
        ownerId: "over-limit-owner",
        createdAt: "2026-08-24T00:30:00.000Z",
        expiresAt: "2026-08-24T01:30:00.000Z",
      }),
    ).rejects.toBeInstanceOf(StorageResourceLimitError);
    const refused = await store.getStorageStats();
    expect(refused.walBytes).toBe(before.walBytes);
    expect(refused.physicalBytes).toBe(before.physicalBytes);
    store._crashForTests();

    const reopened = await OpfsBlockStore.open({ name, root: shim.root });
    await reopened.beginFtsBaseBuild({
      tableId: "table",
      columnId: `posting-${String(MAX_ACTIVE_FTS_BASE_BUILDS)}`,
      buildId: "after-expiry",
      ownerId: "after-expiry-owner",
      createdAt: "2026-08-24T01:00:00.000Z",
      expiresAt: "2026-08-24T02:00:00.000Z",
    });
    await reopened.writeFtsBaseBuildChunk({
      tableId: "table",
      columnId: `posting-${String(MAX_ACTIVE_FTS_BASE_BUILDS)}`,
      buildId: "after-expiry",
      ownerId: "after-expiry-owner",
      expiresAtCutoff: "2026-08-24T01:01:00.000Z",
      expiresAt: "2026-08-24T02:00:00.000Z",
      updatedAt: "2026-08-24T01:01:00.000Z",
      ordinal: 0,
      chunk: [{ term: "bounded", rowIds: [1n], tf: [1] }],
    });
    reopened._crashForTests();
    const recovered = await OpfsBlockStore.open({ name, root: shim.root });
    await expect(
      recovered.writeFtsBaseBuildChunk({
        tableId: "table",
        columnId: `posting-${String(MAX_ACTIVE_FTS_BASE_BUILDS)}`,
        buildId: "after-expiry",
        ownerId: "after-expiry-owner",
        expiresAtCutoff: "2026-08-24T01:02:00.000Z",
        expiresAt: "2026-08-24T02:00:00.000Z",
        updatedAt: "2026-08-24T01:02:00.000Z",
        ordinal: 0,
        chunk: [{ term: "bounded", rowIds: [1n], tf: [1] }],
      }),
    ).resolves.toBeUndefined();
    recovered.close();
  });

  it("rejects corrupt staged postings quota accounting on reopen", async () => {
    const shim = new MemoryOpfs();
    const name = "postings-build-accounting";
    const store = await OpfsBlockStore.open({ name, root: shim.root, checkpointEntries: 1 });
    await store.addTable({
      ...table("posting-accounting"),
      id: "table",
      ftsColumns: {
        posting: {
          storage: "fts-chunks-v1",
          tokenizerVersion: 1,
          state: "building",
          buildFromVersion: -1,
        },
      },
    });
    await store.beginFtsBaseBuild({
      tableId: "table",
      columnId: "posting",
      buildId: "build",
      ownerId: "owner",
      createdAt: "2026-08-24T00:00:00.000Z",
      expiresAt: "2026-08-24T01:00:00.000Z",
    });
    await store.writeFtsBaseBuildChunk({
      tableId: "table",
      columnId: "posting",
      buildId: "build",
      ownerId: "owner",
      expiresAtCutoff: "2026-08-24T00:01:00.000Z",
      expiresAt: "2026-08-24T01:00:00.000Z",
      updatedAt: "2026-08-24T00:01:00.000Z",
      ordinal: 0,
      chunk: [{ term: "one", rowIds: [1n], tf: [1] }],
    });
    await waitFor(() => {
      if ((shim.readFileBytes(`minnowdb/${name}/wal`)?.byteLength ?? -1) !== 0) return false;
      return [`minnowdb/${name}/checkpoint-a`, `minnowdb/${name}/checkpoint-b`].some((path) => {
        const bytes = shim.readFileBytes(path);
        if (bytes === undefined) return false;
        const checkpoint = decodeSyncCheckpoint(bytes) as { ftsBuilds?: unknown[] };
        return checkpoint.ftsBuilds?.length === 1;
      });
    }, "staged postings accounting checkpoint");
    store._crashForTests();

    const slotA = `minnowdb/${name}/checkpoint-a`;
    const slotB = `minnowdb/${name}/checkpoint-b`;
    const source = shim.readFileBytes(slotA) ?? shim.readFileBytes(slotB);
    if (source === undefined) throw new Error("Expected staged postings checkpoint");
    const checkpoint = decodeSyncCheckpoint(source) as {
      ftsBuilds: Array<[string, { retainedEntries: number }]>;
    };
    const pointer = checkpoint.ftsBuilds[0]?.[1];
    if (pointer === undefined) throw new Error("Expected staged postings pointer");
    pointer.retainedEntries += 1;
    const corrupt = encodeSyncCheckpoint(checkpoint);
    shim.writeFileBytes(slotA, corrupt);
    shim.writeFileBytes(slotB, corrupt);
    await expect(OpfsBlockStore.open({ name, root: shim.root })).rejects.toThrow(
      /Staged postings accounting is corrupt/,
    );
  });

  it("keeps sole compaction provenance until GC durably takes it over", async () => {
    const shim = new MemoryOpfs();
    const name = "compaction-provenance";
    const store = await OpfsBlockStore.open({ name, root: shim.root, checkpointEntries: 1 });
    const block = await encodeBlock({ type: "number", values: [7] }, "raw");
    await store.addTable(table("provenance"));
    const transactionId = await createTestTransaction(store, "compaction-provenance");
    const staged = await store.stageTransactionArtifacts({
      transactionId,
      expectedRevision: 0,
      blocks: [{ id: "orphan-block", bytes: block }],
      segments: [
        {
          id: "orphan-segment",
          kind: "insert",
          level: 0,
          logicalOrder: 0,
          commitOrdinal: 0,
          rowIdSpans: [],
          tableId: "table-provenance",
          transactionId,
          rowCount: 1,
          rowIdStart: 1n,
          rowIdEndExclusive: 2n,
          columnBlockIds: { c1: ["orphan-block"] },
          createdAt: "2026-08-24T00:00:01.000Z",
        },
      ],
      updatedAt: "2026-08-24T00:00:01.000Z",
    });
    await store.updateTransaction(transactionId, staged.revision, {
      status: "aborted",
      updatedAt: "2026-08-24T00:00:02.000Z",
    });
    await store.createCompactionJob({
      id: "orphan-compaction",
      tableId: "table-provenance",
      sourceManifestVersion: 0,
      sourceSegmentIds: ["orphan-segment"],
      sourceBlockIds: ["orphan-block"],
      outputBlockIds: [],
      cursor: { sourceSegmentIndex: 1, sourceBlockIndex: 0 },
      processedRows: 1,
      sourceStoredBytes: block.byteLength,
      outputStoredBytes: 0,
      logicalBytes: block.byteLength,
      rewritePlan: { kind: "copy-v1" },
      outputCursor: null,
      memoryBudgetBytes: 0,
      minimumMemoryBytes: 0,
      level0SourceStoredBytes: block.byteLength,
      anchorSourceStoredBytes: 0,
      peakWorkingBytes: 0,
      outputLogicalBytes: block.byteLength,
      targetLevel: 1,
      state: "cancelled",
      transactionId: null,
      outputSegmentId: null,
      publishedVersion: null,
      revision: 0,
      createdAt: "2026-08-24T00:00:02.000Z",
      updatedAt: "2026-08-24T00:00:02.000Z",
    });
    await waitFor(
      () => (shim.readFileBytes(`minnowdb/${name}/wal`)?.byteLength ?? -1) === 0,
      "compaction provenance checkpoint",
    );
    store._crashForTests();

    // Manufacture the crash window after the aborted staging journal was reclaimed but before
    // its terminal compaction diagnostic was pruned. The job is now the only durable discovery
    // path for both the segment descriptor and its physical block.
    const slotPathA = `minnowdb/${name}/checkpoint-a`;
    const slotPathB = `minnowdb/${name}/checkpoint-b`;
    const bytes = shim.readFileBytes(slotPathA) ?? shim.readFileBytes(slotPathB);
    if (bytes === undefined) throw new Error("Expected compaction provenance checkpoint");
    const checkpoint = decodeSyncCheckpoint(bytes) as {
      core: {
        transactions: Array<{
          id: string;
          pendingBlockIds: string[];
          pendingSegmentIds: string[];
        }>;
      };
    };
    const owner = checkpoint.core.transactions.find((record) => record.id === transactionId);
    if (owner === undefined) throw new Error("Expected compaction output owner");
    owner.pendingBlockIds = [];
    owner.pendingSegmentIds = [];
    const soleProvenance = encodeSyncCheckpoint(checkpoint);
    shim.writeFileBytes(slotPathA, soleProvenance);
    shim.writeFileBytes(slotPathB, soleProvenance);

    const recovered = await OpfsBlockStore.open({ name, root: shim.root });
    const walBefore = shim.readFileBytes(`minnowdb/${name}/wal`);
    await expect(recovered.removeCompactionJob("orphan-compaction")).resolves.toBe(false);
    expect(shim.readFileBytes(`minnowdb/${name}/wal`)).toEqual(walBefore);
    expect(await recovered.getCompactionJob("orphan-compaction")).toMatchObject({
      state: "cancelled",
      revision: 0,
    });
    expect(await recovered.getSegment("orphan-segment")).toBeDefined();

    const collection = await recovered.createGarbageCollectionJob({
      id: "orphan-collection",
      candidateManifestVersions: [],
      candidateSegmentIds: ["orphan-segment"],
      candidateBlockIds: ["orphan-block"],
      candidateTransactionIds: [],
      leaseCutoff: "2026-08-24T00:10:00.000Z",
      createdAt: "2026-08-24T00:10:00.000Z",
    });
    const step = await recovered.runGarbageCollectionStep({
      jobId: collection.id,
      expectedRevision: collection.revision,
      maxItems: 2,
      updatedAt: "2026-08-24T00:10:01.000Z",
    });
    expect(step.reclaimedSegmentIds).toEqual(["orphan-segment"]);
    expect(step.reclaimedBlockIds).toEqual(["orphan-block"]);
    expect(await recovered.getBlock("orphan-block")).toBeUndefined();
    expect(await recovered.getSegment("orphan-segment")).toBeUndefined();
    recovered._crashForTests();

    const afterGc = await OpfsBlockStore.open({ name, root: shim.root });
    expect(await afterGc.removeCompactionJob("orphan-compaction")).toBe(true);
    afterGc._crashForTests();
    const final = await OpfsBlockStore.open({ name, root: shim.root });
    expect(await final.getCompactionJob("orphan-compaction")).toBeUndefined();
    final.close();
  });

  it("bounds mutation deduplication and releases connection state on close", async () => {
    const shim = new MemoryOpfs();
    const leader = await OpfsBlockStore.open({ name: "bounded-rpc", root: shim.root });
    const follower = await OpfsBlockStore.open({
      name: "bounded-rpc",
      root: shim.root,
      rpcTimeoutMs: 200,
    });
    expect(leader._isLeaderForTests()).toBe(true);
    for (let index = 0; index < 530; index += 1) {
      await follower.addTable(table(`bounded-${String(index)}`));
      expect(leader._residentStateForTests().dedupeEntries).toBeLessThanOrEqual(512);
    }
    expect(leader._residentStateForTests()).toMatchObject({
      answerChannels: 1,
      dedupeEntries: 512,
      pendingRequests: 0,
    });

    follower.close();
    leader.close();
    expect(leader._residentStateForTests()).toMatchObject({
      dedupeEntries: 0,
      pendingRequests: 0,
      closed: true,
    });
    await waitFor(
      () => leader._residentStateForTests().answerChannels === 0,
      "leader answer channels to close",
    );
  });

  it("never evicts in-flight mutation identities and bounds both RPC queues", async () => {
    const shim = new MemoryOpfs();
    const leader = await OpfsBlockStore.open({
      name: "bounded-inflight-rpc",
      root: shim.root,
      rpcTimeoutMs: 10_000,
    });
    const follower = await OpfsBlockStore.open({
      name: "bounded-inflight-rpc",
      root: shim.root,
      rpcTimeoutMs: 10_000,
    });
    const overflowFollower = await OpfsBlockStore.open({
      name: "bounded-inflight-rpc",
      root: shim.root,
      rpcTimeoutMs: 10_000,
    });
    await follower.listTables();
    await overflowFollower.listTables();
    const release = leader._holdServedMutationsForTests();
    const requests = Array.from({ length: 512 }, (_, index) =>
      follower.addTable(table(`queued-${String(index)}`)).then(
        () => undefined,
        (error: unknown) => error,
      ),
    );
    await waitFor(
      () => leader._residentStateForTests().inFlightMutations === 512,
      "the bounded leader mutation queue to fill",
    );
    expect(follower._residentStateForTests().pendingRequests).toBe(512);

    const clientOverload = await follower.addTable(table("client-overload")).then(
      () => undefined,
      (error: unknown) => error,
    );
    expect(clientOverload).toMatchObject({
      message: "The OPFS follower request queue is full",
    });
    // A duplicate of the oldest admitted identity attaches even while admission is full.
    follower._resendOldestPendingForTests();
    const serverOverload = await overflowFollower.addTable(table("server-overload")).then(
      () => undefined,
      (error: unknown) => error,
    );
    expect(serverOverload).toMatchObject({
      message: "The OPFS leader mutation queue is full",
    });

    release();
    expect((await Promise.all(requests)).filter((result) => result instanceof Error)).toEqual([]);
    expect(await leader.listTables()).toHaveLength(512);
    expect(leader._residentStateForTests()).toMatchObject({
      dedupeEntries: 512,
      inFlightMutations: 0,
    });
    expect(follower._residentStateForTests().pendingRequests).toBe(0);
    await follower.addTable(table("queue-reused"));
    expect(await leader.listTables()).toHaveLength(513);
    overflowFollower.close();
    follower.close();
    leader.close();
  });

  it("checkpoints into alternating slots and resets the WAL", async () => {
    const shim = new MemoryOpfs();
    const store = await OpfsBlockStore.open({ name: "db", root: shim.root, checkpointEntries: 8 });
    for (let index = 0; index < 20; index += 1) await store.addTable(table(`t${String(index)}`));
    // Two checkpoints have passed; the WAL holds only the entries since the last one.
    const wal = shim.readFileBytes("minnowdb/db/wal") ?? new Uint8Array(0);
    const slotA = shim.readFileBytes("minnowdb/db/checkpoint-a") ?? new Uint8Array(0);
    expect(slotA.byteLength).toBeGreaterThan(0);
    expect(wal.byteLength).toBeLessThan(2_000);
    store._crashForTests();

    const reopened = await OpfsBlockStore.open({ name: "db", root: shim.root });
    expect(await reopened.listTables()).toHaveLength(20);
    reopened.close();
  });

  it("mirrors every successful checkpoint so one later corruption cannot roll state back", async () => {
    const shim = new MemoryOpfs();
    const store = await OpfsBlockStore.open({
      name: "mirrored-checkpoint",
      root: shim.root,
      checkpointEntries: 1,
    });
    await store.addTable(table("durable"));
    await waitFor(
      () => (shim.readFileBytes("minnowdb/mirrored-checkpoint/wal")?.byteLength ?? -1) === 0,
      "mirrored checkpoint WAL reset",
    );
    store._crashForTests();

    const slotA = shim.readFileBytes("minnowdb/mirrored-checkpoint/checkpoint-a");
    const slotB = shim.readFileBytes("minnowdb/mirrored-checkpoint/checkpoint-b");
    expect(slotA).toEqual(slotB);
    if (slotA === undefined) throw new Error("Expected mirrored checkpoints");
    slotA[slotA.length - 1] = (slotA[slotA.length - 1] ?? 0) ^ 0xff;
    shim.writeFileBytes("minnowdb/mirrored-checkpoint/checkpoint-a", slotA);

    const reopened = await OpfsBlockStore.open({ name: "mirrored-checkpoint", root: shim.root });
    expect((await reopened.listTables()).map(({ name }) => name)).toEqual(["durable"]);
    reopened.close();
  });

  it("fails closed when all post-reset checkpoint copies are corrupt", async () => {
    const shim = new MemoryOpfs();
    const store = await OpfsBlockStore.open({
      name: "all-checkpoints-corrupt",
      root: shim.root,
      checkpointEntries: 1,
    });
    await store.addTable(table("must-not-disappear"));
    await waitFor(
      () => (shim.readFileBytes("minnowdb/all-checkpoints-corrupt/wal")?.byteLength ?? -1) === 0,
      "checkpoint WAL reset",
    );
    store._crashForTests();
    shim.writeFileBytes("minnowdb/all-checkpoints-corrupt/checkpoint-a", Uint8Array.of(1));
    shim.writeFileBytes("minnowdb/all-checkpoints-corrupt/checkpoint-b", Uint8Array.of(2));
    await expect(
      OpfsBlockStore.open({ name: "all-checkpoints-corrupt", root: shim.root }),
    ).rejects.toMatchObject({ name: "StorageCorruptionError", backend: "opfs" });
  });

  it("rejects a checksum-valid checkpoint with a noncanonical durable timestamp", async () => {
    const shim = new MemoryOpfs();
    const name = "noncanonical-checkpoint-time";
    const store = await OpfsBlockStore.open({ name, root: shim.root, checkpointEntries: 1 });
    await store.addTable(table("timestamp"));
    await waitFor(
      () => (shim.readFileBytes(`minnowdb/${name}/wal`)?.byteLength ?? -1) === 0,
      "timestamp checkpoint",
    );
    store._crashForTests();
    const slotA = `minnowdb/${name}/checkpoint-a`;
    const slotB = `minnowdb/${name}/checkpoint-b`;
    const bytes = shim.readFileBytes(slotA);
    if (bytes === undefined) throw new Error("Expected timestamp checkpoint");
    const checkpoint = decodeSyncCheckpoint(bytes) as {
      core: { tables: Array<{ createdAt: string }> };
    };
    const record = checkpoint.core.tables[0];
    if (record === undefined) throw new Error("Expected timestamp table");
    record.createdAt = "2026-08-19 00:00:00Z";
    const forged = encodeSyncCheckpoint(checkpoint);
    shim.writeFileBytes(slotA, forged);
    shim.writeFileBytes(slotB, forged);
    await expect(OpfsBlockStore.open({ name, root: shim.root })).rejects.toThrow(
      /timestamp|createdAt|creation time|canonical/i,
    );
  });

  it("rejects a checksum-valid layout-5 checkpoint that omits staged full-text state", async () => {
    const shim = new MemoryOpfs();
    const name = "missing-checkpoint-fts-builds";
    const store = await OpfsBlockStore.open({ name, root: shim.root, checkpointEntries: 1 });
    await store.addTable(table("canonical-checkpoint"));
    await waitFor(
      () => (shim.readFileBytes(`minnowdb/${name}/wal`)?.byteLength ?? -1) === 0,
      "canonical checkpoint",
    );
    store._crashForTests();
    const slotA = `minnowdb/${name}/checkpoint-a`;
    const slotB = `minnowdb/${name}/checkpoint-b`;
    const bytes = shim.readFileBytes(slotA);
    if (bytes === undefined) throw new Error("Expected canonical checkpoint");
    const checkpoint = decodeSyncCheckpoint(bytes) as Record<string, unknown>;
    delete checkpoint.ftsBuilds;
    const forged = encodeSyncCheckpoint(checkpoint);
    shim.writeFileBytes(slotA, forged);
    shim.writeFileBytes(slotB, forged);
    await expect(OpfsBlockStore.open({ name, root: shim.root })).rejects.toThrow(
      /Every OPFS checkpoint copy is corrupt/,
    );
  });

  it.each([
    [
      "gap",
      { seq: 2, op: "removePrunedManifestRecords", maxItems: 1 },
      /starts at sequence 2 instead of 1/,
    ],
    ["unknown", { seq: 1, op: "futureOperation" }, /Unsupported OPFS WAL operation/],
  ])("rejects a checksum-valid %s WAL entry", async (_label, entry, message) => {
    const shim = new MemoryOpfs();
    const store = await OpfsBlockStore.open({ name: `bad-wal-${_label}`, root: shim.root });
    store._crashForTests();
    const handle = await new OpfsTree(shim.root).openHandle(
      ["minnowdb", `bad-wal-${_label}`, "wal"],
      { create: false },
    );
    new WalWriter(handle, 0).append(entry, false);
    handle.close();
    await expect(
      OpfsBlockStore.open({ name: `bad-wal-${_label}`, root: shim.root }),
    ).rejects.toThrow(message);
  });

  it.each([
    [
      "removal",
      { seq: 1, op: "removeAbortedSegment", id: "segment" },
      /Invalid removeAbortedSegment transaction id/,
    ],
    [
      "counter",
      { seq: 1, op: "reserveRowIds", tableId: "t", count: "1" },
      /Invalid reserveRowIds count/,
    ],
    ["record", { seq: 1, op: "createLease", record: null }, /Invalid createLease record/],
    [
      "nested-record",
      {
        seq: 1,
        op: "addTable",
        record: { ...table("runtime-shape"), id: 42 },
        expectedCatalogEpoch: 0,
      },
      /table.*(id|identity)/i,
    ],
    [
      "committed-transaction-without-version",
      {
        seq: 1,
        op: "createTransaction",
        record: {
          id: "invalid-commit",
          ownerId: "invalid-owner",
          expiresAt: "2026-08-24T01:00:00.000Z",
          snapshotVersion: null,
          pendingBlockIds: [],
          pendingSegmentIds: [],
          status: "committed",
          revision: 0,
          startedAt: "2026-08-24T00:00:00.000Z",
          updatedAt: "2026-08-24T00:00:00.000Z",
          committedVersion: null,
        },
      },
      /committed status and version disagree/,
    ],
    [
      "update",
      { seq: 1, op: "updateTable", id: "t", expectedRevision: 0, update: null },
      /Invalid updateTable update/,
    ],
    [
      "lease",
      {
        seq: 1,
        op: "moveLease",
        input: {
          id: "lease",
          expectedRevision: 0,
          manifestVersion: "0",
          expiresAtCutoff: "2026-08-24T00:00:00.000Z",
          expiresAt: "2026-08-24T00:01:00.000Z",
        },
      },
      /Invalid moveLease manifestVersion/,
    ],
    [
      "noncanonical-timestamp",
      {
        seq: 1,
        op: "renewTransaction",
        input: {
          transactionId: "tx",
          ownerId: "owner",
          expiresAtCutoff: "2026-08-24 00:00:00Z",
          expiresAt: "2026-08-24T00:01:00.000Z",
        },
      },
      /Invalid renewTransaction cutoff/,
    ],
    [
      "full-text",
      { seq: 1, op: "writeFtsBase", tableId: "t", columnId: "c", pointer: [] },
      /Invalid writeFtsBase pointer/,
    ],
    [
      "extra-field",
      { seq: 1, op: "removePrunedManifestRecords", maxItems: 1, ignored: true },
      /Unexpected removePrunedManifestRecords WAL field/,
    ],
    [
      "rollback",
      { seq: 1, op: "rollbackTransactionArtifacts", input: { transactionId: "tx" } },
      /Invalid rollbackTransactionArtifacts revision/,
    ],
    [
      "removed-snapshot-operation",
      { seq: 1, op: "importSnapshot", records: [], blockPlacements: [] },
      /Unsupported OPFS WAL operation/,
    ],
    [
      "collection-effect",
      { seq: 1, op: "garbageCollectionStep", effect: { job: {} } },
      /Invalid garbageCollectionStep effect fields/,
    ],
    [
      "collection-effect-old-layout",
      {
        seq: 1,
        op: "garbageCollectionStep",
        effect: {
          job: {},
          prunedManifestVersions: [],
          reclaimedSegmentIds: [],
          reclaimedBlockIds: [],
          updatedAt: "2026-08-24T00:00:00.000Z",
        },
      },
      /Invalid garbageCollectionStep effect fields/,
    ],
    [
      "relocation-old-layout",
      { seq: 1, op: "relocatePayloads", blocks: [], ftsChunks: [] },
      /Invalid staged full-text relocations/,
    ],
    [
      "oversized-placement-identity",
      {
        seq: 1,
        op: "stageTransactionArtifacts",
        transactionId: "transaction",
        expectedRevision: 0,
        blocks: [
          {
            id: "x".repeat(MAX_STORAGE_ID_CHARACTERS + 1),
            placement: { extent: 0, offset: 0, length: 1, checksum: 0 },
          },
        ],
        segments: [],
        updatedAt: "2026-08-24T00:00:00.000Z",
      },
      /Storage ID exceeds 1024 characters/,
    ],
    [
      "relocation",
      { seq: 1, op: "relocatePayloads", blocks: [{}], ftsChunks: [] },
      /Invalid block relocations identity/,
    ],
  ])("fails closed on malformed checksum-valid %s WAL bodies", async (label, entry, message) => {
    const shim = new MemoryOpfs();
    const name = `malformed-wal-${label}`;
    const store = await OpfsBlockStore.open({ name, root: shim.root });
    store._crashForTests();
    const handle = await new OpfsTree(shim.root).openHandle(["minnowdb", name, "wal"], {
      create: false,
    });
    new WalWriter(handle, 0).append(entry, false);
    handle.close();
    await expect(OpfsBlockStore.open({ name, root: shim.root })).rejects.toThrow(message);
  });

  it("rejects a checksum-valid collection effect whose arrays disagree with its job", async () => {
    const shim = new MemoryOpfs();
    const name = "malformed-gc-effect";
    const store = await OpfsBlockStore.open({ name, root: shim.root });
    await store.createTransaction({
      id: "empty-manifest",
      ownerId: "fixture-owner",
      expiresAt: "2026-08-24T01:00:00.000Z",
      snapshotVersion: null,
      pendingBlockIds: [],
      pendingSegmentIds: [],
      status: "active",
      revision: 0,
      startedAt: "2026-08-24T00:00:00.000Z",
      updatedAt: "2026-08-24T00:00:00.000Z",
      committedVersion: null,
    });
    await store.commitTransaction({
      transactionId: "empty-manifest",
      expectedTransactionRevision: 0,
      expectedManifestVersion: null,
      committedAt: "2026-08-24T00:00:01.000Z",
    });
    const job = await store.createGarbageCollectionJob({
      id: "forged",
      candidateManifestVersions: [0],
      candidateSegmentIds: [],
      candidateBlockIds: [],
      leaseCutoff: "2026-08-24T00:00:00.000Z",
      createdAt: "2026-08-24T00:00:00.000Z",
    });
    store._crashForTests();
    const tree = new OpfsTree(shim.root);
    const handle = await tree.openHandle(["minnowdb", name, "wal"], { create: false });
    const offset = handle.getSize();
    new WalWriter(handle, offset).append(
      {
        seq: 4,
        op: "garbageCollectionStep",
        effect: {
          job: {
            ...job,
            cursor: { ...job.cursor, manifestIndex: 1 },
            retainedManifestCount: 1,
            state: "completed",
            revision: 1,
            updatedAt: "2026-08-24T00:01:00.000Z",
          },
          // The current manifest was accounted as retained, not pruned.
          prunedManifestVersions: [0],
          reclaimedSegmentIds: [],
          reclaimedBlockIds: [],
          reclaimedTransactionIds: [],
          updatedAt: "2026-08-24T00:01:00.000Z",
        },
      },
      false,
    );
    handle.close();

    await expect(OpfsBlockStore.open({ name, root: shim.root })).rejects.toThrow(
      /effect arrays disagree with job accounting/,
    );
  });

  it("reclaims unknown extent files and unpublished tail suffixes during recovery", async () => {
    const shim = new MemoryOpfs();
    const store = await OpfsBlockStore.open({
      name: "extent-orphans",
      root: shim.root,
      checkpointEntries: 1,
    });
    await store.addTable(table("anchor"));
    await waitFor(
      () => (shim.readFileBytes("minnowdb/extent-orphans/wal")?.byteLength ?? -1) === 0,
      "empty checkpoint",
    );
    store._crashForTests();
    shim.writeFileBytes("minnowdb/extent-orphans/extents/000000", new Uint8Array(4096));
    shim.writeFileBytes("minnowdb/extent-orphans/extents/999999", new Uint8Array(8192));

    const reopened = await OpfsBlockStore.open({ name: "extent-orphans", root: shim.root });
    expect(shim.readFileBytes("minnowdb/extent-orphans/extents/000000")?.byteLength).toBe(0);
    expect(shim.readFileBytes("minnowdb/extent-orphans/extents/999999")).toBeUndefined();
    const stats = await reopened.getStorageStats();
    expect(stats.orphanBytes).toBe(0);
    expect(stats.physicalBytes).toBeGreaterThanOrEqual(stats.logicalBytes);
    expect(stats.checkpointBytes).toBeGreaterThan(0);
    expect(stats.maintenance).toMatchObject({ degraded: false });
    expect((await reopened.checkIntegrity({ mode: "full" })).ok).toBe(true);
    reopened.close();
  });

  it("serves durable reads when recovery cleanup is refused and bounds growth until retry", async () => {
    const shim = new MemoryOpfs();
    const name = "refused-recovery-cleanup";
    const store = await OpfsBlockStore.open({
      name,
      root: shim.root,
      checkpointEntries: 1,
      cleanupLimitBytes: 1024,
    });
    await store.addTable(table("anchor"));
    await waitFor(
      () => (shim.readFileBytes(`minnowdb/${name}/wal`)?.byteLength ?? -1) === 0,
      "recovery-cleanup checkpoint",
    );
    store._crashForTests();
    shim.writeFileBytes(`minnowdb/${name}/extents/000000`, new Uint8Array(4096));
    shim.writeFileBytes(`minnowdb/${name}/extents/999999`, new Uint8Array(4096));
    shim.setWriteFault((path, phase) => {
      if (path.endsWith("/extents/000000") && phase === "write") {
        throw new DOMException("persistent truncate refusal", "NoModificationAllowedError");
      }
    });
    shim.setDeleteFault((path) => {
      if (path.endsWith("/extents/999999")) {
        throw new DOMException("persistent delete refusal", "NoModificationAllowedError");
      }
    });

    const reopened = await OpfsBlockStore.open({
      name,
      root: shim.root,
      cleanupLimitBytes: 1024,
    });
    expect((await reopened.listTables()).map((record) => record.name)).toEqual(["anchor"]);
    const degraded = await reopened.getStorageStats();
    expect(degraded.maintenance).toMatchObject({ degraded: true, cleanupLimitBytes: 1024 });
    expect(degraded.maintenance?.cleanupDebtBytes).toBeGreaterThan(1024);
    const block = await encodeBlock({ type: "number", values: [1] }, "raw");
    await expect(
      stageTestBlocks(reopened, [{ id: "blocked", bytes: block }], "blocked-recovery"),
    ).rejects.toMatchObject({
      name: "QuotaExceededError",
    });

    shim.setWriteFault(null);
    shim.setDeleteFault(null);
    await stageTestBlocks(reopened, [{ id: "after-recovery", bytes: block }], "after-recovery");
    expect(await reopened.getBlock("after-recovery")).toEqual(block);
    expect((await reopened.getStorageStats()).maintenance).toMatchObject({
      degraded: false,
      cleanupDebtBytes: 0,
    });
    reopened.close();
  });

  it("recovers from a crash during a checkpoint write: the other slot plus the WAL", async () => {
    const shim = new MemoryOpfs();
    const store = await OpfsBlockStore.open({ name: "db", root: shim.root, checkpointEntries: 8 });
    // Stop one entry short of the checkpoint trigger and capture the WAL as it stands.
    for (let index = 0; index < 7; index += 1) await store.addTable(table(`t${String(index)}`));
    const walBeforeCheckpoint = shim.readFileBytes("minnowdb/db/wal") ?? new Uint8Array(0);
    expect(walBeforeCheckpoint.byteLength).toBeGreaterThan(0);
    // The eighth entry appends and then checkpoints (slot write, flush, WAL reset).
    await store.addTable(table("t7"));
    store._crashForTests();

    // Manufacture the crash-mid-checkpoint state: the slot is torn, and the WAL still holds
    // everything — the reset only ever happens after the slot flush, so this is the worst
    // on-disk state that crash can leave. The torn tail of the eighth entry's frame is
    // reconstructed by appending it to the captured WAL image.
    // The first checkpoint lands in slot B (slots alternate starting opposite the empty A).
    const checkpointed = shim.readFileBytes("minnowdb/db/checkpoint-b");
    expect(checkpointed?.byteLength ?? 0).toBeGreaterThan(0);
    shim.writeFileBytes("minnowdb/db/checkpoint-b", new Uint8Array(24)); // torn slot
    shim.writeFileBytes("minnowdb/db/checkpoint-a", new Uint8Array(0));
    // Restore the WAL to its pre-reset content plus the final entry's frame, taken from a
    // sibling run of the same deterministic append.
    const sibling = new MemoryOpfs();
    const rebuild = await OpfsBlockStore.open({ name: "db", root: sibling.root });
    for (let index = 0; index < 7; index += 1) await rebuild.addTable(table(`t${String(index)}`));
    await rebuild.addTable(table("t7"));
    rebuild._crashForTests();
    shim.writeFileBytes(
      "minnowdb/db/wal",
      sibling.readFileBytes("minnowdb/db/wal") ?? new Uint8Array(0),
    );

    const reopened = await OpfsBlockStore.open({ name: "db", root: shim.root });
    expect(await reopened.listTables()).toHaveLength(8);
    reopened.close();
  });

  it("leaves a refused operation out of the WAL entirely", async () => {
    const shim = new MemoryOpfs();
    const store = await OpfsBlockStore.open({ name: "db", root: shim.root });
    await store.addTable(table("real"));
    const walAfterAdd = (shim.readFileBytes("minnowdb/db/wal") ?? new Uint8Array(0)).byteLength;
    await expect(store.updateTable("table-real", 7, {})).rejects.toThrow(TableRecordConflictError);
    const walAfterRefusal = (shim.readFileBytes("minnowdb/db/wal") ?? new Uint8Array(0)).byteLength;
    expect(walAfterRefusal).toBe(walAfterAdd);
    store.close();
  });

  it("does not fall back or clean up when one checkpoint envelope has a newer version", async () => {
    const shim = new MemoryOpfs();
    const name = "future-checkpoint-envelope";
    const store = await OpfsBlockStore.open({ name, root: shim.root, checkpointEntries: 1 });
    await store.addTable(table("anchor"));
    await waitFor(
      () => (shim.readFileBytes(`minnowdb/${name}/wal`)?.byteLength ?? -1) === 0,
      "future-envelope checkpoint",
    );
    store._crashForTests();

    const slotAPath = `minnowdb/${name}/checkpoint-a`;
    const slotBPath = `minnowdb/${name}/checkpoint-b`;
    const slotA = shim.readFileBytes(slotAPath);
    const slotB = shim.readFileBytes(slotBPath);
    if (slotA === undefined || slotB === undefined) throw new Error("Expected checkpoint copies");
    const incompatible = slotA.slice();
    new DataView(incompatible.buffer).setUint32(8, LOG_FORMAT_VERSION + 1, true);
    const orphanPath = `minnowdb/${name}/extents/999999`;
    const orphan = Uint8Array.of(4, 3, 2, 1);
    shim.writeFileBytes(slotAPath, incompatible);
    shim.writeFileBytes(orphanPath, orphan);

    await expect(OpfsBlockStore.open({ name, root: shim.root })).rejects.toMatchObject({
      name: "StorageFormatVersionError",
      location: "envelope/MNWCKPS1",
      actualVersion: LOG_FORMAT_VERSION + 1,
      supportedVersion: LOG_FORMAT_VERSION,
      relation: "newer",
    });
    expect(shim.readFileBytes(slotAPath)).toEqual(incompatible);
    expect(shim.readFileBytes(slotBPath)).toEqual(slotB);
    expect(shim.readFileBytes(orphanPath)).toEqual(orphan);
  });

  it("does not fall back or clean up when one checkpoint state has a newer version", async () => {
    const shim = new MemoryOpfs();
    const name = "future-checkpoint-state";
    const store = await OpfsBlockStore.open({ name, root: shim.root, checkpointEntries: 1 });
    await store.addTable(table("anchor"));
    await waitFor(
      () => (shim.readFileBytes(`minnowdb/${name}/wal`)?.byteLength ?? -1) === 0,
      "future-state checkpoint",
    );
    store._crashForTests();

    const slotAPath = `minnowdb/${name}/checkpoint-a`;
    const slotBPath = `minnowdb/${name}/checkpoint-b`;
    const slotA = shim.readFileBytes(slotAPath);
    const slotB = shim.readFileBytes(slotBPath);
    if (slotA === undefined || slotB === undefined) throw new Error("Expected checkpoint copies");
    const state = decodeSyncCheckpoint(slotA) as Record<string, unknown>;
    state.formatVersion = 2;
    const incompatible = encodeSyncCheckpoint(state);
    const orphanPath = `minnowdb/${name}/extents/999999`;
    const orphan = Uint8Array.of(8, 7, 6, 5);
    shim.writeFileBytes(slotAPath, incompatible);
    shim.writeFileBytes(orphanPath, orphan);

    await expect(OpfsBlockStore.open({ name, root: shim.root })).rejects.toMatchObject({
      name: "StorageFormatVersionError",
      location: "checkpoint/state",
      actualVersion: 2,
      supportedVersion: 1,
      relation: "newer",
    });
    expect(shim.readFileBytes(slotAPath)).toEqual(incompatible);
    expect(shim.readFileBytes(slotBPath)).toEqual(slotB);
    expect(shim.readFileBytes(orphanPath)).toEqual(orphan);
  });

  it.each([
    [LOG_FORMAT_VERSION - 1, "older"],
    [LOG_FORMAT_VERSION + 1, "newer"],
  ] as const)(
    "refuses layout version %s without modifying its artifacts",
    async (formatVersion, relation) => {
      const shim = new MemoryOpfs();
      const marker = new TextEncoder().encode(JSON.stringify({ formatVersion }));
      const wal = Uint8Array.of(0x4d, 0x4e, 0x57, 0x4c, 1, 2, 3, 4);
      const extent = Uint8Array.of(5, 6, 7, 8);
      shim.writeFileBytes("minnowdb/db/format.json", marker);
      shim.writeFileBytes("minnowdb/db/wal", wal);
      shim.writeFileBytes("minnowdb/db/extents/000000", extent);

      await expect(OpfsBlockStore.open({ name: "db", root: shim.root })).rejects.toMatchObject({
        name: "StorageFormatVersionError",
        backend: "opfs",
        location: "format.json",
        actualVersion: formatVersion,
        supportedVersion: LOG_FORMAT_VERSION,
        relation,
      });
      expect(shim.readFileBytes("minnowdb/db/format.json")).toEqual(marker);
      expect(shim.readFileBytes("minnowdb/db/wal")).toEqual(wal);
      expect(shim.readFileBytes("minnowdb/db/extents/000000")).toEqual(extent);
    },
  );

  it.each([
    ["an object with no version", "{}"],
    ["an array", "[]"],
    ["null", "null"],
    ["a string version", '{"formatVersion":"3"}'],
  ])("refuses %s as a format marker", async (_description, marker) => {
    const shim = new MemoryOpfs();
    shim.writeFileBytes("minnowdb/db/format.json", new TextEncoder().encode(marker));
    await expect(OpfsBlockStore.open({ name: "db", root: shim.root })).rejects.toThrow(
      /format marker is invalid.*safe integer/,
    );
  });

  it.each([
    [
      "an extra field",
      `{"formatVersion":${String(LOG_FORMAT_VERSION)},"migration":{"target":${String(LOG_FORMAT_VERSION + 1)}}}`,
    ],
    [
      "a duplicate version",
      `{"formatVersion":${String(LOG_FORMAT_VERSION + 1)},"formatVersion":${String(LOG_FORMAT_VERSION)}}`,
    ],
    ["noncanonical whitespace", `{ "formatVersion": ${String(LOG_FORMAT_VERSION)} }`],
  ])("refuses %s in the locked marker without modifying artifacts", async (_, marker) => {
    const shim = new MemoryOpfs();
    const markerBytes = new TextEncoder().encode(marker);
    const wal = Uint8Array.of(9, 8, 7, 6);
    shim.writeFileBytes("minnowdb/db/format.json", markerBytes);
    shim.writeFileBytes("minnowdb/db/wal", wal);

    await expect(OpfsBlockStore.open({ name: "db", root: shim.root })).rejects.toBeInstanceOf(
      StorageCorruptionError,
    );
    expect(shim.readFileBytes("minnowdb/db/format.json")).toEqual(markerBytes);
    expect(shim.readFileBytes("minnowdb/db/wal")).toEqual(wal);
  });

  it.each([
    ["missing", undefined],
    ["torn", "{"],
  ])("refuses a %s marker beside storage artifacts without modifying them", async (_, marker) => {
    const shim = new MemoryOpfs();
    if (marker !== undefined) {
      shim.writeFileBytes("minnowdb/db/format.json", new TextEncoder().encode(marker));
    }
    const wal = Uint8Array.of(0x4d, 0x4e, 0x57, 0x4c, 1, 2, 3, 4);
    shim.writeFileBytes("minnowdb/db/wal", wal);

    const failure = await OpfsBlockStore.open({ name: "db", root: shim.root }).catch(
      (error: unknown) => error,
    );
    expect(failure).toMatchObject({
      name: "StorageCorruptionError",
      backend: "opfs",
      location: "format.json",
    });
    expect(failure).toBeInstanceOf(StorageCorruptionError);
    expect((failure as Error).message).toMatch(
      /format marker is (?:missing|torn).*storage artifacts \(wal\)/,
    );
    expect(shim.readFileBytes("minnowdb/db/wal")).toEqual(wal);
  });

  it("repairs a torn marker only in an otherwise empty database directory", async () => {
    const shim = new MemoryOpfs();
    shim.writeFileBytes("minnowdb/db/format.json", new TextEncoder().encode("{"));

    const store = await OpfsBlockStore.open({ name: "db", root: shim.root });
    expect(
      JSON.parse(new TextDecoder().decode(shim.readFileBytes("minnowdb/db/format.json"))),
    ).toEqual({ formatVersion: LOG_FORMAT_VERSION });
    store.close();
  });

  it("validates the marker published by a concurrent opener after lock contention", async () => {
    const shim = new MemoryOpfs();
    shim.writeFileBytes("minnowdb/db/format.json", new TextEncoder().encode('{"formatVersion":2}'));
    const held = await new OpfsTree(shim.root).openHandle(["minnowdb", "db", "format.json"], {
      create: false,
    });
    setTimeout(() => held.close(), 10);

    await expect(OpfsBlockStore.open({ name: "db", root: shim.root })).rejects.toBeInstanceOf(
      StorageFormatVersionError,
    );
  });
});

describe("OPFS leadership", () => {
  it("fails over to a follower when the leader dies without a goodbye", async () => {
    const shim = new MemoryOpfs();
    const name = "db";
    const leader = await OpfsBlockStore.open({ name, root: shim.root, rpcTimeoutMs: 100 });
    const follower = await OpfsBlockStore.open({ name, root: shim.root, rpcTimeoutMs: 100 });
    await leader.addTable(table("before"));
    expect((await follower.listTables()).map((record) => record.name)).toEqual(["before"]);
    expect((await follower.getStorageStats()).backend).toBe("opfs");
    expect((await follower.checkIntegrity()).ok).toBe(true);

    leader._crashForTests();
    // A mutation sent toward an apparently live leader cannot be proven absent after its
    // timeout, so the adapter refuses to replay it automatically. Reconcile, then retry the
    // stable table id after the follower has acquired and replayed the database.
    await expect(follower.addTable(table("after"))).rejects.toMatchObject({
      name: "OpfsUncertainOutcomeError",
    });
    expect(await follower.getTableByName("after")).toBeUndefined();
    await follower.addTable(table("after"));
    expect((await follower.listTables()).map((record) => record.name)).toEqual(["after", "before"]);
    follower.close();
  });

  it("hands leadership over on a graceful close", async () => {
    const shim = new MemoryOpfs();
    const first = await OpfsBlockStore.open({ name: "db", root: shim.root, rpcTimeoutMs: 100 });
    const second = await OpfsBlockStore.open({ name: "db", root: shim.root, rpcTimeoutMs: 100 });
    await first.addTable(table("kept"));
    first.close();
    await waitFor(async () => {
      try {
        await second.addTable(table("next"));
        return true;
      } catch {
        return false;
      }
    }, "the second connection to take over");
    expect((await second.listTables()).map((record) => record.name)).toEqual(["kept", "next"]);
    second.close();
  });

  it("yields leadership to the connection the user is looking at", async () => {
    const shim = new MemoryOpfs();
    const background = await OpfsBlockStore.open({
      name: "db",
      root: shim.root,
      rpcTimeoutMs: 100,
    });
    const foreground = await OpfsBlockStore.open({
      name: "db",
      root: shim.root,
      rpcTimeoutMs: 100,
    });
    await background.addTable(table("seed"));
    expect(background._isLeaderForTests()).toBe(true);
    expect(foreground._isLeaderForTests()).toBe(false);

    foreground.setForeground(true);
    // The handoff itself, not a proxy for it: the bidder ends up holding the handles and the
    // incumbent does not.
    await waitFor(() => foreground._isLeaderForTests(), "the foreground bidder to take over");
    expect(background._isLeaderForTests()).toBe(false);

    await foreground.addTable(table("written-as-leader"));
    const throughBackground = await background.listTables();
    const throughForeground = await foreground.listTables();
    expect(throughBackground.map((record) => record.name)).toEqual(
      throughForeground.map((record) => record.name),
    );
    background.close();
    foreground.close();
  });

  it("executes a duplicated mutation request only once", async () => {
    const shim = new MemoryOpfs();
    const leader = await OpfsBlockStore.open({ name: "dup", root: shim.root, rpcTimeoutMs: 200 });
    const follower = await OpfsBlockStore.open({
      name: "dup",
      root: shim.root,
      rpcTimeoutMs: 200,
    });
    // Eavesdrop on the leader's inbox and replay the follower's frame verbatim — exactly what
    // a retry whose acknowledgement was lost looks like to the leader.
    const spy = new BroadcastChannel(`minnowdb-store:dup:${leader._instanceIdForTests()}`);
    const captured: unknown[] = [];
    spy.onmessage = (event: MessageEvent<unknown>) => {
      const message = event.data as { kind?: string; method?: string };
      if (message.kind === "op" && message.method === "reserveRowIds") captured.push(event.data);
    };

    await leader.addTable(table("x"));
    const first = await follower.reserveRowIds("table-x", 10);
    await waitFor(() => captured.length >= 1, "the frame to be observed");
    spy.postMessage(captured[0]); // the duplicate
    await new Promise((resolve) => setTimeout(resolve, 50));

    // The duplicate replayed the remembered result instead of burning a second range: the next
    // reservation continues exactly where the first ended.
    const second = await follower.reserveRowIds("table-x", 10);
    expect(second.start).toBe(first.endExclusive);
    spy.close();
    leader.close();
    follower.close();
  });

  it("namespaces request identities by follower and rejects changed duplicate bodies", async () => {
    const shim = new MemoryOpfs();
    const name = "rpc-request-identity";
    const leader = await OpfsBlockStore.open({ name, root: shim.root, rpcTimeoutMs: 2_000 });
    await leader.addTable(table("identity"));
    const inbox = new BroadcastChannel(`minnowdb-store:${name}:${leader._instanceIdForTests()}`);
    const replies = new Map<string, unknown[]>();
    const listen = (requester: string): BroadcastChannel => {
      const channel = new BroadcastChannel(`minnowdb-store:${name}:${requester}`);
      channel.onmessage = (event: MessageEvent<unknown>) => {
        const values = replies.get(requester) ?? [];
        values.push(event.data);
        replies.set(requester, values);
      };
      return channel;
    };
    const firstReply = listen("requester-a");
    const secondReply = listen("requester-b");
    const shared = {
      kind: "op" as const,
      requestId: "same-request-id",
      method: "reserveRowIds",
    };
    inbox.postMessage({ ...shared, from: "requester-a", args: ["table-identity", 10] });
    inbox.postMessage({ ...shared, from: "requester-b", args: ["table-identity", 20] });
    await waitFor(
      () => (replies.get("requester-a")?.length ?? 0) === 1,
      "the first namespaced request",
    );
    await waitFor(
      () => (replies.get("requester-b")?.length ?? 0) === 1,
      "the second namespaced request",
    );
    expect(await leader.reserveRowIds("table-identity", 1)).toEqual({
      start: 31n,
      endExclusive: 32n,
    });

    const changedReply = listen("requester-c");
    const release = leader._holdServedMutationsForTests();
    inbox.postMessage({
      ...shared,
      requestId: "changed-body",
      from: "requester-c",
      args: ["table-identity", 5],
    });
    await waitFor(
      () => leader._residentStateForTests().inFlightMutations === 1,
      "the original request identity to be admitted",
    );
    inbox.postMessage({
      ...shared,
      requestId: "changed-body",
      from: "requester-c",
      args: ["table-identity", 7],
    });
    await waitFor(
      () =>
        (replies.get("requester-c") ?? []).some(
          (message) =>
            (message as { error?: { message?: string } }).error?.message ===
            "The OPFS RPC request identity was reused with different contents",
        ),
      "the changed request body to be rejected",
    );
    release();
    await waitFor(
      () => (replies.get("requester-c")?.length ?? 0) === 2,
      "the original request to settle",
    );
    expect(await leader.reserveRowIds("table-identity", 1)).toEqual({
      start: 37n,
      endExclusive: 38n,
    });
    changedReply.close();
    secondReply.close();
    firstReply.close();
    inbox.close();
    leader.close();
  });

  it("does not replay an unacknowledged mutation across leader failover", async () => {
    const shim = new MemoryOpfs();
    const leader = await OpfsBlockStore.open({
      name: "uncertain-failover",
      root: shim.root,
      rpcTimeoutMs: 500,
    });
    const follower = await OpfsBlockStore.open({
      name: "uncertain-failover",
      root: shim.root,
      rpcTimeoutMs: 500,
    });
    await leader.addTable(table("counter"));
    const walBefore = shim.readFileBytes("minnowdb/uncertain-failover/wal")?.byteLength ?? 0;
    leader._dropNextRpcResultForTests();
    const pending = follower.reserveRowIds("table-counter", 10).then(
      () => undefined,
      (error: unknown) => error,
    );
    await waitFor(
      () => (shim.readFileBytes("minnowdb/uncertain-failover/wal")?.byteLength ?? 0) > walBefore,
      "the mutation WAL frame before dropping its acknowledgement",
    );
    leader._crashForTests();
    const recovered = await OpfsBlockStore.open({
      name: "uncertain-failover",
      root: shim.root,
      rpcTimeoutMs: 500,
    });
    expect(await pending).toMatchObject({
      name: "OpfsUncertainOutcomeError",
      method: "reserveRowIds",
    });

    // The recovered range starts after the one committed by the dead leader. The follower did
    // not resend its old request and burn a second ten-row range during takeover.
    expect(await recovered.reserveRowIds("table-counter", 10)).toEqual({
      start: 11n,
      endExclusive: 21n,
    });
    follower.close();
    recovered.close();
  });

  it("never serves state the disk refused, even to reads", async () => {
    const shim = new MemoryOpfs();
    const store = await OpfsBlockStore.open({ name: "db", root: shim.root });
    await store.addTable(table("real"));
    shim.setWriteFault((path) => {
      if (path.endsWith("/wal")) {
        throw new DOMException("The quota has been exceeded.", "QuotaExceededError");
      }
    });
    await expect(store.addTable(table("phantom"))).rejects.toMatchObject({
      name: "QuotaExceededError",
    });
    shim.setWriteFault(null);
    // The very first access afterwards is a read: it must reload from disk rather than serve
    // the in-memory state that ran ahead of the refused write.
    expect((await store.listTables()).map((record) => record.name)).toEqual(["real"]);
    expect(await store.getTableByName("phantom")).toBeUndefined();
    // And the store keeps working.
    await store.addTable(table("after"));
    expect(await store.listTables()).toHaveLength(2);
    store.close();
  });

  it("keeps acknowledging operations when checkpointing itself fails", async () => {
    const shim = new MemoryOpfs();
    const store = await OpfsBlockStore.open({ name: "db", root: shim.root, checkpointEntries: 4 });
    let checkpointAttempts = 0;
    shim.setWriteFault((path) => {
      if (path.includes("checkpoint-")) {
        checkpointAttempts += 1;
        throw new DOMException("The quota has been exceeded.", "QuotaExceededError");
      }
    });
    // Every operation is durable in the WAL the moment it is acknowledged; a checkpoint that
    // cannot be written is a deferred maintenance failure, not an operation failure.
    for (let index = 0; index < 12; index += 1) await store.addTable(table(`t${String(index)}`));
    expect(await store.listTables()).toHaveLength(12);
    const degraded = await store.getStorageStats();
    expect(degraded.maintenance).toMatchObject({
      degraded: true,
      walLimitBytes: 256 * 1024 * 1024,
    });
    expect(degraded.maintenance?.consecutiveFailures).toBeGreaterThan(0);
    expect(checkpointAttempts).toBeLessThan(12);
    shim.setWriteFault(null);
    store._crashForTests();

    const reopened = await OpfsBlockStore.open({ name: "db", root: shim.root });
    expect(await reopened.listTables()).toHaveLength(12);
    reopened.close();
  });

  it("does not strand the database when a connection closes mid-election", async () => {
    const shim = new MemoryOpfs();
    const first = await OpfsBlockStore.open({ name: "db", root: shim.root, rpcTimeoutMs: 100 });
    const second = await OpfsBlockStore.open({ name: "db", root: shim.root, rpcTimeoutMs: 100 });
    await first.addTable(table("seed"));

    // Trigger a handoff toward `second` and close it immediately: its election may complete
    // after close(), and an installed-but-ownerless leader would hold the file lock forever.
    second.setForeground(true);
    second.close();

    const third = await OpfsBlockStore.open({ name: "db", root: shim.root, rpcTimeoutMs: 100 });
    await waitFor(async () => {
      try {
        await third.addTable(table(`probe-${String(Math.random()).slice(2, 8)}`));
        return true;
      } catch {
        return false;
      }
    }, "some connection to be able to lead again");
    first.close();
    third.close();
  });
});
