/* eslint-disable no-restricted-imports -- Node-only test reads fixture provenance; this file is not shipped. */
import { readFileSync } from "node:fs";
import { IDBFactory } from "fake-indexeddb";
import { describe, expect, it } from "vitest";
import {
  CompactionJobConflictError,
  GarbageCollectionJobConflictError,
  IndexedDbBlockStore,
  LeaseConflictError,
  MAX_TRANSACTION_STAGE_BLOCKS,
  MAX_TRANSACTION_STAGE_BYTES,
  MAX_TRANSACTION_STAGE_SEGMENTS,
  MemoryBlockStore,
  OpfsBlockStore,
  SchemaConflictError,
  SnapshotManifestMissingError,
  StorageCorruptionError,
  StorageFormatVersionError,
  TableRecordConflictError,
  secondaryUniqueKeyNamespace,
  TempOwnerConflictError,
  UniqueKeyConflictError,
  floorWholeNumberProduct,
  storeNames,
  type BlockStore,
  type CompactionJobRecord,
  type CompactionJobRecordUpdate,
  type CommitTransactionInput,
  type MergeCompactionRewritePlan,
  type ManifestSummary,
  type SegmentRecord,
  type StoragePage,
  type TransactionRecord,
  type TransactionRecordUpdate,
} from "./index.js";
import { MemoryOpfs } from "../testing/opfs-shim.js";
import { crc32, encodeBlock, MAX_BLOCK_ROW_COUNT } from "../block-format/index.js";

const POSTING_BUILD_CREATED_AT = "2026-01-01T00:00:00.000Z";
const POSTING_BUILD_EXPIRES_AT = "2026-01-01T00:30:00.000Z";
const currentPackageVersion = (
  JSON.parse(readFileSync(new URL("../../package.json", import.meta.url), "utf8")) as {
    version: string;
  }
).version;

function packageVersionTuple(version: string): [number, number, number] {
  const match = /^(\d+)\.(\d+)\.(\d+)(?:-|$)/u.exec(version);
  if (match === null) throw new TypeError(`Invalid fixture package version: ${version}`);
  return [Number(match[1]), Number(match[2]), Number(match[3])];
}

function comparePackageVersions(left: string, right: string): number {
  const leftParts = packageVersionTuple(left);
  const rightParts = packageVersionTuple(right);
  return (
    leftParts[0] - rightParts[0] || leftParts[1] - rightParts[1] || leftParts[2] - rightParts[2]
  );
}

/** Frozen first-stable native schema. Do not update this fixture on a schema bump: a new release
 * must migrate this exact v1 database through the ordered production migration registry. */
function installFrozenIndexedDbV1(request: IDBOpenDBRequest): void {
  for (const storeName of storeNames) request.result.createObjectStore(storeName);
  request.result.createObjectStore("snapshotHeaders");
  const upgrade = request.transaction;
  if (upgrade === null) throw new Error("missing frozen-v1 upgrade transaction");
  upgrade.objectStore("segments").createIndex("byTable", "tableId");
  upgrade.objectStore("leases").createIndex("byExpiry", ["expiresAt", "id"]);
  upgrade.objectStore("transactions").createIndex("byStatus", "status");
  upgrade.objectStore("temp").createIndex("byOwnerExpiry", ["expiresAt", "ownerId"]);
  const catalog = upgrade.objectStore("catalog");
  catalog.createIndex("byFtsBuildUpdatedAt", "updatedAt");
  catalog.createIndex("byFtsBuildExpiry", "ftsBuildExpiry");
  catalog.createIndex("byFtsRetirementUpdatedAt", "retirementUpdatedAt");
  catalog.createIndex("byUniqueKeyBuildActive", "activeBuildState");
  catalog.createIndex("byUniqueKeyBuildExpiry", "activeExpiry");
  catalog.createIndex("byManifestBlockId", "blockId", { unique: true });
  upgrade.objectStore("gc").add(
    {
      activeCompactionJobs: 0,
      terminalCompactionJobs: 0,
      activeGarbageCollectionJobs: 0,
      completedGarbageCollectionJobs: 0,
    },
    "maintenance/quota",
  );
  const statistics = upgrade.objectStore("statistics");
  statistics.add(
    { stagedBlockCount: 0, stagedSegmentCount: 0, stagedBytes: 0, retiredHistoryBytes: 0 },
    "resource/global",
  );
  statistics.add(
    { recordCount: 0, retainedBytes: 0, checksum: crc32(new TextEncoder().encode("0:0")) },
    "resource/catalog",
  );
  statistics.add(
    {
      manifestCount: 0,
      manifestBytes: 0,
      segmentCount: 0,
      segmentBytes: 0,
      checksum: crc32(new TextEncoder().encode("0:0:0:0")),
    },
    "resource/records",
  );
}

const FIRST_STABLE_INDEXED_DB_SCHEMA_VERSION = 1;
const frozenIndexedDbSchemas: ReadonlyArray<{
  version: number;
  writerPackageVersion: string;
  install: (request: IDBOpenDBRequest) => void;
}> = [{ version: 1, writerPackageVersion: "0.3.0", install: installFrozenIndexedDbV1 }];

function openNativeIndexedDb(
  indexedDB: IDBFactory,
  name: string,
  version?: number,
  upgrade?: (request: IDBOpenDBRequest) => void,
): Promise<IDBDatabase> {
  return new Promise((resolve, reject) => {
    const request = version === undefined ? indexedDB.open(name) : indexedDB.open(name, version);
    if (upgrade !== undefined) request.onupgradeneeded = () => upgrade(request);
    request.onsuccess = () => resolve(request.result);
    request.onerror = () => reject(request.error ?? new Error("native IndexedDB open failed"));
  });
}

function postingBuildOwner(buildId: string): string {
  return `${buildId}-owner`;
}

async function beginPostingBuild(
  store: BlockStore,
  tableId: string,
  columnId: string,
  buildId: string,
  createdAt = POSTING_BUILD_CREATED_AT,
  expiresAt = POSTING_BUILD_EXPIRES_AT,
): Promise<void> {
  await store.beginFtsBaseBuild({
    tableId,
    columnId,
    buildId,
    ownerId: postingBuildOwner(buildId),
    createdAt,
    expiresAt,
  });
}

async function appendPostingBuild(
  store: BlockStore,
  tableId: string,
  columnId: string,
  buildId: string,
  ordinal: number,
  chunk: Parameters<BlockStore["writeFtsBaseBuildChunk"]>[0]["chunk"],
  expiresAtCutoff = POSTING_BUILD_CREATED_AT,
  expiresAt = POSTING_BUILD_EXPIRES_AT,
): Promise<void> {
  await store.writeFtsBaseBuildChunk({
    tableId,
    columnId,
    buildId,
    ownerId: postingBuildOwner(buildId),
    expiresAtCutoff,
    expiresAt,
    updatedAt: expiresAtCutoff,
    ordinal,
    chunk,
  });
}

async function finishPostingBuild(
  store: BlockStore,
  tableId: string,
  columnId: string,
  buildId: string,
  input: { coversVersion: number; chunkCount: number; totalTokens: number },
): Promise<void> {
  await store.finishFtsBaseBuild({
    tableId,
    columnId,
    buildId,
    ownerId: postingBuildOwner(buildId),
    expiresAtCutoff: POSTING_BUILD_CREATED_AT,
    completedAt: POSTING_BUILD_CREATED_AT,
    ...input,
  });
}

it("floors amplification products without rounding a binary ratio upward", () => {
  expect(50 * 1.3399999999999999).toBe(67);
  expect(floorWholeNumberProduct(50, 1.3399999999999999, "Amplification product")).toBe(66);
});

it("OPFS repacks mixed live/dead extents when collection completes", async () => {
  const shim = new MemoryOpfs();
  const name = crypto.randomUUID();
  const store = await OpfsBlockStore.open({ name, root: shim.root });
  // Keep four independently verifiable block envelopes in the first 8 MiB extent. Removing
  // three then exercises relocation of the remaining live block; opaque filler bytes would
  // (correctly) be refused by corruption-safe relocation.
  const blocks = await Promise.all(
    Array.from({ length: 5 }, async (_, index) => ({
      id: `extent-block-${String(index)}`,
      bytes: await encodeBlock(
        { type: "string" as const, values: [String(index + 1).repeat(1_900_000)] },
        "raw",
      ),
    })),
  );
  const staged = await stageTestArtifacts(store, { blocks });
  await store.updateTransaction(staged.id, staged.revision, {
    status: "aborted",
    updatedAt: "2026-01-01T00:00:01.000Z",
  });
  const job = await store.createGarbageCollectionJob({
    id: "repack-extents",
    candidateManifestVersions: [],
    candidateSegmentIds: [],
    candidateBlockIds: blocks.slice(0, 3).map((block) => block.id),
    leaseCutoff: "2026-01-01T00:01:00.000Z",
    createdAt: "2026-01-01T00:01:00.000Z",
  });
  const result = await store.runGarbageCollectionStep({
    jobId: job.id,
    expectedRevision: job.revision,
    maxItems: 10,
    updatedAt: "2026-01-01T00:01:01.000Z",
  });

  expect(result.job.state).toBe("completed");
  expect(shim.readFileBytes(`minnowdb/${name}/extents/000000`)).toBeUndefined();
  expect(await store.getBlock("extent-block-3")).toEqual(blocks[3]?.bytes);
  expect(await store.getBlock("extent-block-4")).toEqual(blocks[4]?.bytes);
  store.close();
  const reopened = await OpfsBlockStore.open({ name, root: shim.root });
  expect(await reopened.getBlock("extent-block-3")).toEqual(blocks[3]?.bytes);
  expect(await reopened.getBlock("extent-block-4")).toEqual(blocks[4]?.bytes);
  reopened.close();
}, 60_000);

it("OPFS relocates a staged postings build instead of letting it block extent compaction", async () => {
  const shim = new MemoryOpfs();
  const name = crypto.randomUUID();
  const store = await OpfsBlockStore.open({ name, root: shim.root });
  await store.addTable({
    managed: false,
    id: "postings-table",
    name: "postings_table",
    columns: [{ id: "value", name: "value", type: "string", nullable: false }],
    ftsColumns: {
      value: {
        storage: "fts-chunks-v1",
        tokenizerVersion: 1,
        state: "building",
        buildFromVersion: -1,
      },
    },
    revision: 0,
    createdAt: "2026-01-01T00:00:00.000Z",
  });
  const dead = { id: "staged-dead", bytes: new Uint8Array(3 * 1024 * 1024).fill(1) };
  const live = { id: "staged-live", bytes: new Uint8Array(3 * 1024 * 1024).fill(2) };
  const filler = { id: "staged-filler", bytes: new Uint8Array(3 * 1024 * 1024).fill(3) };
  await beginPostingBuild(store, "postings-table", "value", "moving-build");
  await appendPostingBuild(store, "postings-table", "value", "moving-build", 0, [
    { term: "survives-relocation", rowIds: [7n], tf: [1] },
  ]);
  const staged = await stageTestArtifacts(store, { blocks: [dead, live, filler] });
  await store.updateTransaction(staged.id, staged.revision, {
    status: "aborted",
    updatedAt: "2026-01-01T00:00:01.000Z",
  });
  const job = await store.createGarbageCollectionJob({
    id: "collect-beside-staged-build",
    candidateManifestVersions: [],
    candidateSegmentIds: [],
    candidateBlockIds: [dead.id],
    leaseCutoff: "2026-01-01T00:01:00.000Z",
    createdAt: "2026-01-01T00:01:00.000Z",
  });
  expect(
    (
      await store.runGarbageCollectionStep({
        jobId: job.id,
        expectedRevision: job.revision,
        maxItems: 10,
        updatedAt: "2026-01-01T00:01:01.000Z",
      })
    ).job.state,
  ).toBe("completed");
  await finishPostingBuild(store, "postings-table", "value", "moving-build", {
    coversVersion: 1,
    chunkCount: 1,
    totalTokens: 1,
  });
  expect(
    await store.readFtsCandidates(
      "postings-table",
      "value",
      [{ term: "survives-relocation", prefix: false }],
      1,
    ),
  ).toMatchObject({ rowIdsByTerm: [[7n]], coversVersion: 1 });
  store.close();

  const reopened = await OpfsBlockStore.open({ name, root: shim.root });
  expect(await reopened.getBlock(live.id)).toEqual(live.bytes);
  expect(
    await reopened.readFtsCandidates(
      "postings-table",
      "value",
      [{ term: "survives-relocation", prefix: false }],
      1,
    ),
  ).toMatchObject({ rowIdsByTerm: [[7n]], coversVersion: 1 });
  reopened.close();
});

function stores(): Array<{ name: string; create: () => Promise<BlockStore> }> {
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

async function listAllManifests(store: BlockStore): Promise<ManifestSummary[]> {
  const records: ManifestSummary[] = [];
  let cursor: number | null = null;
  do {
    const page: StoragePage<ManifestSummary, number> = await store.listManifestPage(cursor, 64);
    records.push(...page.records);
    cursor = page.nextCursor;
  } while (cursor !== null);
  return records;
}

async function listAllSegments(store: BlockStore, tableId?: string): Promise<SegmentRecord[]> {
  const records: SegmentRecord[] = [];
  let cursor: string | null = null;
  do {
    const page: StoragePage<SegmentRecord, string> =
      tableId === undefined
        ? await store.listSegmentPage(cursor, 64)
        : await store.listTableSegmentPage(tableId, cursor, 64);
    records.push(...page.records);
    cursor = page.nextCursor;
  } while (cursor !== null);
  return records;
}

for (const implementation of stores()) {
  it(`${implementation.name} atomically guards catalog-wide mutation proofs by epoch`, async () => {
    const store = await implementation.create();
    const initial = await store.getCatalogProbe();
    const record = {
      id: "catalog-guard-table",
      name: "catalog_guard_table",
      managed: false,
      revision: 0,
      columns: [{ id: "value", name: "value", type: "number" as const, nullable: true }],
      createdAt: "2026-01-01T00:00:00.000Z",
    };
    await store.addTable(record, { expectedCatalogEpoch: initial.catalogEpoch });
    const afterAdd = await store.getCatalogProbe();

    await expect(
      store.updateTable(record.id, 0, {
        columns: record.columns,
        expectedCatalogEpoch: initial.catalogEpoch,
      }),
    ).rejects.toBeInstanceOf(TableRecordConflictError);
    expect(await store.getTable(record.id)).toEqual(record);

    const updated = await store.updateTable(record.id, 0, {
      columns: record.columns,
      expectedCatalogEpoch: afterAdd.catalogEpoch,
    });
    const afterUpdate = await store.getCatalogProbe();
    await expect(
      store.addTable(
        { ...record, id: "stale-add", name: "stale_add" },
        { expectedCatalogEpoch: afterAdd.catalogEpoch },
      ),
    ).rejects.toBeInstanceOf(TableRecordConflictError);
    expect(await store.getTable("stale-add")).toBeUndefined();
    await expect(
      store.removeTable(record.id, updated.revision, {
        expectedCatalogEpoch: afterAdd.catalogEpoch,
      }),
    ).rejects.toBeInstanceOf(TableRecordConflictError);
    expect(await store.getTable(record.id)).toEqual(updated);

    await store.removeTable(record.id, updated.revision, {
      expectedCatalogEpoch: afterUpdate.catalogEpoch,
    });
    expect(await store.getTable(record.id)).toBeUndefined();
    store.close();
  });

  it(`${implementation.name} reclaims active accelerator generations with their table`, async () => {
    const store = await implementation.create();
    const table = {
      id: "accelerator-owner",
      name: "accelerator_owner",
      managed: false,
      revision: 0,
      columns: [{ id: "value", name: "value", type: "string" as const, nullable: false }],
      ftsColumns: {
        value: {
          storage: "fts-chunks-v1" as const,
          tokenizerVersion: 1,
          state: "building" as const,
          buildFromVersion: -1,
        },
      },
      secondaryIndexes: {
        unique_value: {
          name: "unique_value",
          columnId: "value",
          columnIds: ["value"],
          directions: ["asc" as const],
          unique: true as const,
          termEncoding: "tuple-v1" as const,
          storage: "postings-v1" as const,
          storageColumnId: "unique-value-storage",
          locator: "row-id" as const,
          state: "building" as const,
          buildId: "reusable-build",
          buildFromVersion: -1,
        },
      },
      createdAt: "2026-01-01T00:00:00.000Z",
    };
    await store.addTable(table);
    const namespaceId = secondaryUniqueKeyNamespace(table.id, "unique_value");
    await store.beginUniqueKeyBuild({
      buildId: "reusable-build",
      tableId: table.id,
      indexId: "unique_value",
      namespaceId,
      ownerId: "unique-owner",
      createdAt: POSTING_BUILD_CREATED_AT,
      expiresAt: POSTING_BUILD_EXPIRES_AT,
    });
    await store.appendUniqueKeyBuildChunk({
      buildId: "reusable-build",
      ownerId: "unique-owner",
      expiresAtCutoff: POSTING_BUILD_CREATED_AT,
      ordinal: 0,
      keyTokens: ["value"],
      updatedAt: POSTING_BUILD_CREATED_AT,
    });
    await beginPostingBuild(store, table.id, "value", "fts-build");
    await appendPostingBuild(store, table.id, "value", "fts-build", 0, [
      { term: "value", rowIds: [1n], tf: [1] },
    ]);

    await store.removeTable(table.id, table.revision);
    expect(await store.getUniqueKeyBuild("reusable-build")).toBeUndefined();
    await expect(
      appendPostingBuild(store, table.id, "value", "fts-build", 1, [
        { term: "stale", rowIds: [2n], tf: [1] },
      ]),
    ).rejects.toThrow();

    const replacement = {
      ...table,
      id: "accelerator-replacement",
      name: "accelerator_replacement",
    };
    await store.addTable(replacement);
    await expect(
      store.beginUniqueKeyBuild({
        buildId: "reusable-build",
        tableId: replacement.id,
        indexId: "unique_value",
        namespaceId: secondaryUniqueKeyNamespace(replacement.id, "unique_value"),
        ownerId: "replacement-owner",
        createdAt: POSTING_BUILD_CREATED_AT,
        expiresAt: POSTING_BUILD_EXPIRES_AT,
      }),
    ).resolves.toMatchObject({ tableId: replacement.id, state: "active" });
    if ("checkIntegrity" in store && typeof store.checkIntegrity === "function") {
      expect(await store.checkIntegrity()).toMatchObject({ ok: true });
    }
    store.close();
  });

  it(`${implementation.name} resolves manifests identically across delta chains and checkpoints`, async () => {
    const store = await implementation.create();
    // 70 commits cross two checkpoint intervals; each adds one block, and every third commit
    // also supersedes the block from two commits earlier, so deltas carry removals too.
    const expectedByVersion: string[][] = [];
    const live = new Set<string>();
    for (let index = 0; index < 70; index += 1) {
      const blockId = `chain-block-${String(index).padStart(3, "0")}`;
      const transactionId = `chain-transaction-${String(index)}`;
      const staged = await stageTestArtifacts(store, {
        transactionId,
        snapshotVersion: index === 0 ? null : index - 1,
        blocks: [{ id: blockId, bytes: Uint8Array.of(index) }],
      });
      await store.commitTransaction({
        transactionId,
        expectedTransactionRevision: staged.revision,
        expectedManifestVersion: index === 0 ? null : index - 1,
        removedBlockIds: [],
        committedAt: "2026-01-01T00:00:00.000Z",
      });
      live.add(blockId);
      expectedByVersion.push([...live].sort());
    }
    // Every historical version resolves to its exact block set, from either read path.
    for (const version of [0, 1, 30, 31, 32, 33, 63, 64, 65, 69]) {
      expect(await readManifestBlockIds(store, version)).toEqual(expectedByVersion[version]);
    }
    expect(await readManifestBlockIds(store, 69)).toEqual(expectedByVersion[69]);
    const listed = await listAllManifests(store);
    expect(listed).toHaveLength(70);
    for (const manifest of listed) {
      expect(await readManifestBlockIds(store, manifest.version)).toEqual(
        expectedByVersion[manifest.version],
      );
    }
    const page = await store.listManifestPage(40, 10);
    expect(page.records.map((manifest) => manifest.version)).toEqual([
      41, 42, 43, 44, 45, 46, 47, 48, 49, 50,
    ]);
    for (const manifest of page.records) {
      expect(await readManifestBlockIds(store, manifest.version)).toEqual(
        expectedByVersion[manifest.version],
      );
    }
    store.close();
  });

  it(`${implementation.name} physically bounds pruned manifest history without breaking delta reads`, async () => {
    const store = await implementation.create();
    for (let version = 0; version < 70; version += 1) {
      await publishManifest(store, {
        expectedVersion: version === 0 ? null : version - 1,
        blockIds: [],
        createdAt: new Date(Date.UTC(2026, 0, 1, 0, 0, version)).toISOString(),
      });
    }
    const candidates = Array.from({ length: 40 }, (_, version) => version);
    const job = await store.createGarbageCollectionJob({
      id: "manifest-prefix-cleanup",
      candidateManifestVersions: candidates,
      candidateSegmentIds: [],
      candidateBlockIds: [],
      leaseCutoff: "2027-01-01T00:00:00.000Z",
      createdAt: "2027-01-01T00:00:00.000Z",
    });
    await store.runGarbageCollectionStep({
      jobId: job.id,
      expectedRevision: job.revision,
      maxItems: 100,
      updatedAt: "2027-01-01T00:00:01.000Z",
    });

    let removed = 0;
    for (;;) {
      const page = await store.removePrunedManifestRecords(64);
      removed += page;
      if (page === 0) break;
    }
    expect(removed).toBeGreaterThanOrEqual(32);
    expect(removed).toBeLessThanOrEqual(40);
    expect(await store.getManifest(31)).toBeUndefined();
    expect(await store.getManifest(40)).toMatchObject({ version: 40, liveBlockCount: 0 });
    expect(await store.getCurrentManifest()).toMatchObject({ version: 69, liveBlockCount: 0 });
    expect((await listAllManifests(store)).every((manifest) => manifest.version >= 32)).toBe(true);
    expect(await store.removePrunedManifestRecords(64)).toBe(0);
    store.close();
  });

  it(`${implementation.name} removes a pruned summary while retaining garbage provenance`, async () => {
    const store = await implementation.create();
    await store.addTable({
      managed: false,
      id: "pruned-table",
      name: "pruned_table",
      columns: [{ id: "value", name: "value", type: "number", nullable: false }],
      revision: 0,
      createdAt: "2026-01-01T00:00:00.000Z",
    });
    const staged = await stageTestArtifacts(store, {
      transactionId: "pruned-table-writer",
      blocks: [{ id: "garbage-from-pruned-manifest", bytes: Uint8Array.of(1, 2, 3) }],
      segments: [
        {
          id: "pruned-table-segment",
          tableId: "pruned-table",
          transactionId: "pruned-table-writer",
          rowCount: 1,
          rowIdStart: 1n,
          rowIdEndExclusive: 2n,
          columnBlockIds: { value: ["garbage-from-pruned-manifest"] },
          createdAt: "2026-01-01T00:00:00.000Z",
        },
      ],
    });
    await store.commitTransaction({
      transactionId: staged.id,
      expectedTransactionRevision: staged.revision,
      expectedManifestVersion: null,
      removedBlockIds: [],
      levelZeroSegmentLimits: [{ tableId: "pruned-table", limit: 1 }],
      committedAt: "2026-01-01T00:00:00.000Z",
    });
    await store.dropTable({
      tableId: "pruned-table",
      expectedTableRevision: 0,
      expectedManifestVersion: 0,
      expectedCatalogEpoch: (await store.getCatalogProbe()).catalogEpoch,
      committedAt: "2026-01-01T00:00:01.000Z",
    });
    const collection = await store.createGarbageCollectionJob({
      id: "prune-before-block-reclaim",
      candidateManifestVersions: [0],
      candidateSegmentIds: [],
      candidateBlockIds: ["garbage-from-pruned-manifest"],
      leaseCutoff: "2027-01-01T00:00:00.000Z",
      createdAt: "2027-01-01T00:00:00.000Z",
    });
    const pruned = await store.runGarbageCollectionStep({
      jobId: collection.id,
      expectedRevision: collection.revision,
      maxItems: 1,
      updatedAt: "2027-01-01T00:00:01.000Z",
    });

    expect(await store.removePrunedManifestRecords(64)).toBe(1);
    expect(await store.getManifest(0)).toBeUndefined();
    expect(await store.getBlock("garbage-from-pruned-manifest")).toBeDefined();

    await store.runGarbageCollectionStep({
      jobId: collection.id,
      expectedRevision: pruned.job.revision,
      maxItems: 1,
      updatedAt: "2027-01-01T00:00:03.000Z",
    });

    expect(await store.removePrunedManifestRecords(64)).toBe(0);
    expect(await store.getManifest(0)).toBeUndefined();
    expect(await store.getBlock("garbage-from-pruned-manifest")).toBeUndefined();
    store.close();
  });

  it(`${implementation.name} isolates, clones, and removes temp run pages`, async () => {
    const store = await implementation.create();
    for (const ownerId of ["query-a", "query-b"]) {
      await store.createTempOwner({
        ownerId,
        createdAt: "2026-01-01T00:00:00.000Z",
        expiresAt: "2026-01-01T00:30:00.000Z",
        revision: 0,
      });
    }
    const bytes = Uint8Array.of(1, 2, 3);
    await store.putTempRunPage({ ownerId: "query-a", runId: "run-1", pageIndex: 0, bytes });
    await store.putTempRunPage({
      ownerId: "query-a",
      runId: "run-2",
      pageIndex: 0,
      bytes: Uint8Array.of(4),
    });
    await store.putTempRunPage({
      ownerId: "query-b",
      runId: "run-1",
      pageIndex: 0,
      bytes: Uint8Array.of(5),
    });
    bytes[0] = 9;
    const loaded = await store.getTempRunPage("query-a", "run-1", 0);
    expect(loaded).toEqual(Uint8Array.of(1, 2, 3));
    if (loaded !== undefined) loaded[1] = 9;
    expect(await store.getTempRunPage("query-a", "run-1", 0)).toEqual(Uint8Array.of(1, 2, 3));

    await store.removeTempRun("query-a", "run-1");
    expect(await store.getTempRunPage("query-a", "run-1", 0)).toBeUndefined();
    expect(await store.getTempRunPage("query-a", "run-2", 0)).toEqual(Uint8Array.of(4));
    await store.removeTempOwner("query-a");
    expect(await store.getTempRunPage("query-a", "run-2", 0)).toBeUndefined();
    expect(await store.getTempRunPage("query-b", "run-1", 0)).toEqual(Uint8Array.of(5));
    store.close();
  });

  it(`${implementation.name} enforces temp owner lease creation and renewal`, async () => {
    const store = await implementation.create();
    await store.createTempOwner({
      ownerId: "query-a",
      createdAt: "2026-01-01T00:00:00.000Z",
      expiresAt: "2026-01-01T00:01:00.000Z",
      revision: 0,
    });
    await expect(
      store.createTempOwner({
        ownerId: "query-a",
        createdAt: "2026-01-01T00:00:00.000Z",
        expiresAt: "2026-01-01T00:02:00.000Z",
        revision: 0,
      }),
    ).rejects.toThrow(/already exists/);
    await expect(
      store.createTempOwner({
        ownerId: "query-b",
        createdAt: "2026-01-01T00:00:00.000Z",
        expiresAt: "2026-01-01T00:01:00.000Z",
        revision: 3,
      }),
    ).rejects.toThrow(/revision zero/);
    expect(await store.getTempOwner("query-a")).toEqual({
      ownerId: "query-a",
      createdAt: "2026-01-01T00:00:00.000Z",
      expiresAt: "2026-01-01T00:01:00.000Z",
      revision: 0,
    });
    const renewed = await store.renewTempOwner({
      ownerId: "query-a",
      expectedRevision: 0,
      expiresAtCutoff: "2026-01-01T00:00:00.000Z",
      expiresAt: "2026-01-01T00:03:00.000Z",
    });
    expect(renewed).toEqual({
      ownerId: "query-a",
      createdAt: "2026-01-01T00:00:00.000Z",
      expiresAt: "2026-01-01T00:03:00.000Z",
      revision: 1,
    });
    await expect(
      store.renewTempOwner({
        ownerId: "query-a",
        expectedRevision: 0,
        expiresAtCutoff: "2026-01-01T00:00:00.000Z",
        expiresAt: "2026-01-01T00:04:00.000Z",
      }),
    ).rejects.toThrow(TempOwnerConflictError);
    await expect(
      store.renewTempOwner({
        ownerId: "missing",
        expectedRevision: 0,
        expiresAtCutoff: "2026-01-01T00:00:00.000Z",
        expiresAt: "2026-01-01T00:04:00.000Z",
      }),
    ).rejects.toThrow(TempOwnerConflictError);
    store.close();
  });

  it(`${implementation.name} reclaims expired temp spill state`, async () => {
    const store = await implementation.create();
    await store.createTempOwner({
      ownerId: "query-live",
      createdAt: "2026-01-01T00:00:00.000Z",
      expiresAt: "2026-01-01T00:10:00.000Z",
      revision: 0,
    });
    await store.putTempRunPage({
      ownerId: "query-live",
      runId: "run-1",
      pageIndex: 0,
      bytes: Uint8Array.of(1),
    });
    await store.createTempOwner({
      ownerId: "query-stale",
      createdAt: "2026-01-01T00:00:00.000Z",
      expiresAt: "2026-01-01T00:01:00.000Z",
      revision: 0,
    });
    await store.putTempRunPage({
      ownerId: "query-stale",
      runId: "run-1",
      pageIndex: 0,
      bytes: Uint8Array.of(2),
    });
    expect(await store.removeTempOwnerIfExpired("query-live", "2026-01-01T00:05:00.000Z")).toBe(
      false,
    );
    expect(await store.getTempRunPage("query-live", "run-1", 0)).toEqual(Uint8Array.of(1));
    expect(await store.removeTempOwnerIfExpired("query-stale", "2026-01-01T00:05:00.000Z")).toBe(
      true,
    );
    expect(await store.getTempOwner("query-stale")).toBeUndefined();
    expect(await store.getTempRunPage("query-stale", "run-1", 0)).toBeUndefined();
    expect(await store.getTempRunPage("query-live", "run-1", 0)).toEqual(Uint8Array.of(1));

    await store.removeTempOwner("query-live");
    expect(await store.getTempOwner("query-live")).toBeUndefined();
    await store.createTempOwner({
      ownerId: "query-live",
      createdAt: "2026-01-01T00:05:00.000Z",
      expiresAt: "2026-01-01T00:20:00.000Z",
      revision: 0,
    });
    store.close();
  });

  it(`${implementation.name} pages distinct temp owner IDs across records and pages`, async () => {
    const store = await implementation.create();
    await store.createTempOwner({
      ownerId: "owner-a",
      createdAt: "2026-01-01T00:00:00.000Z",
      expiresAt: "2026-01-01T00:01:00.000Z",
      revision: 0,
    });
    await store.createTempOwner({
      ownerId: "owner-d",
      createdAt: "2026-01-01T00:00:00.000Z",
      expiresAt: "2026-01-01T00:01:00.000Z",
      revision: 0,
    });
    for (const ownerId of ["owner-b", "owner-c"]) {
      await store.createTempOwner({
        ownerId,
        createdAt: "2026-01-01T00:00:00.000Z",
        expiresAt: "2026-01-01T00:01:00.000Z",
        revision: 0,
      });
    }
    await store.putTempRunPage({
      ownerId: "owner-a",
      runId: "run-1",
      pageIndex: 0,
      bytes: Uint8Array.of(1),
    });
    await store.putTempRunPage({
      ownerId: "owner-b",
      runId: "run-1",
      pageIndex: 0,
      bytes: Uint8Array.of(2),
    });
    await store.putTempRunPage({
      ownerId: "owner-b",
      runId: "run-2",
      pageIndex: 1,
      bytes: Uint8Array.of(3),
    });
    await store.putTempRunPage({
      ownerId: "owner-c",
      runId: "run-1",
      pageIndex: 0,
      bytes: Uint8Array.of(4),
    });

    const first = await store.listTempOwnerIdsPage(null, 2);
    expect(first.records).toEqual(["owner-a", "owner-b"]);
    expect(first.nextCursor).toBe("owner-b");
    const second = await store.listTempOwnerIdsPage(first.nextCursor, 2);
    expect(second.records).toEqual(["owner-c", "owner-d"]);
    const third = await store.listTempOwnerIdsPage("owner-d", 2);
    expect(third.records).toEqual([]);
    expect(third.nextCursor).toBeNull();
    const all = await store.listTempOwnerIdsPage(null, 16);
    expect(all.records).toEqual(["owner-a", "owner-b", "owner-c", "owner-d"]);
    expect(all.nextCursor).toBeNull();
    store.close();
  });
}

function activeTransaction(id: string): TransactionRecord {
  const createdAt = "2026-01-01T00:00:00.000Z";
  return {
    id,
    ownerId: `${id}/owner`,
    expiresAt: "2026-01-01T00:30:00.000Z",
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

type TestSegmentRecord = Omit<
  SegmentRecord,
  "kind" | "level" | "logicalOrder" | "commitOrdinal" | "rowIdSpans"
> &
  Partial<Pick<SegmentRecord, "kind" | "level" | "logicalOrder" | "commitOrdinal" | "rowIdSpans">>;

async function stageTestArtifacts(
  store: BlockStore,
  input: {
    transactionId?: string;
    snapshotVersion?: number | null;
    blocks?: ReadonlyArray<{ id: string; bytes: Uint8Array }>;
    segments?: readonly TestSegmentRecord[];
  },
): Promise<TransactionRecord> {
  const transactionId = input.transactionId ?? `staged-test-artifacts-${crypto.randomUUID()}`;
  await store.createTransaction({
    ...activeTransaction(transactionId),
    snapshotVersion: input.snapshotVersion ?? null,
  });
  let record = await store.getTransaction(transactionId);
  if (record === undefined) throw new Error(`Missing staged test transaction: ${transactionId}`);
  const blocks = input.blocks ?? [];
  const segments = (input.segments ?? []).map((segment, commitOrdinal) => ({
    ...segment,
    kind: segment.kind ?? "insert",
    level: segment.level ?? 0,
    logicalOrder: segment.logicalOrder ?? 0,
    commitOrdinal: segment.commitOrdinal ?? commitOrdinal,
    rowIdSpans: segment.rowIdSpans ?? [],
  }));
  const blockBytes = blocks.reduce((total, block) => total + block.bytes.byteLength, 0);
  if (
    blocks.length <= MAX_TRANSACTION_STAGE_BLOCKS &&
    segments.length <= MAX_TRANSACTION_STAGE_SEGMENTS &&
    blockBytes <= MAX_TRANSACTION_STAGE_BYTES
  ) {
    if (blocks.length === 0 && segments.length === 0) return record;
    return store.stageTransactionArtifacts({
      transactionId,
      expectedRevision: record.revision,
      blocks,
      segments,
      updatedAt: "2026-01-01T00:00:00.000Z",
    });
  }
  for (let start = 0; start < blocks.length;) {
    let end = start;
    let bytes = 0;
    while (end < blocks.length && end - start < MAX_TRANSACTION_STAGE_BLOCKS) {
      const next = blocks[end];
      if (next === undefined) break;
      if (end > start && bytes + next.bytes.byteLength > MAX_TRANSACTION_STAGE_BYTES) break;
      bytes += next.bytes.byteLength;
      end += 1;
    }
    record = await store.stageTransactionArtifacts({
      transactionId,
      expectedRevision: record.revision,
      blocks: blocks.slice(start, end),
      segments: [],
      updatedAt: "2026-01-01T00:00:00.000Z",
    });
    start = end;
  }
  for (let start = 0; start < segments.length; start += MAX_TRANSACTION_STAGE_SEGMENTS) {
    record = await store.stageTransactionArtifacts({
      transactionId,
      expectedRevision: record.revision,
      blocks: [],
      segments: segments.slice(start, start + MAX_TRANSACTION_STAGE_SEGMENTS),
      updatedAt: "2026-01-01T00:00:00.000Z",
    });
  }
  return record;
}

async function publishTestBlocks(
  store: BlockStore,
  input: {
    expectedVersion: number | null;
    blocks: ReadonlyArray<{ id: string; bytes: Uint8Array }>;
    createdAt?: string;
  },
) {
  const staged = await stageTestArtifacts(store, {
    snapshotVersion: input.expectedVersion,
    blocks: input.blocks,
  });
  return store.commitTransaction({
    transactionId: staged.id,
    expectedTransactionRevision: staged.revision,
    expectedManifestVersion: input.expectedVersion,
    removedBlockIds: [],
    committedAt: input.createdAt ?? "2026-01-01T00:00:00.000Z",
  });
}

async function addCounterFixtureTables(store: BlockStore): Promise<void> {
  await store.addTable({
    managed: false,
    id: "people",
    name: "people",
    columns: [
      {
        id: "id",
        name: "id",
        type: "number",
        integer: true,
        nullable: false,
        defaultValue: { kind: "autoincrement" },
      },
    ],
    primaryKeyColumnIds: ["id"],
    uniqueKeyColumnId: "id",
    revision: 0,
    createdAt: "2026-01-01T00:00:00.000Z",
  });
  await store.addTable({
    managed: false,
    id: "people-other",
    name: "people_other",
    columns: [
      {
        id: "other",
        name: "other",
        type: "number",
        integer: true,
        nullable: false,
        defaultValue: { kind: "autoincrement" },
      },
    ],
    primaryKeyColumnIds: ["other"],
    uniqueKeyColumnId: "other",
    revision: 0,
    createdAt: "2026-01-01T00:00:00.000Z",
  });
}

async function publishManifest(
  store: BlockStore,
  input: {
    expectedVersion: number | null;
    blockIds: readonly string[];
    createdAt?: string;
  },
) {
  const current = await store.getCurrentManifest();
  const currentIds = new Set(
    current === undefined ? [] : await readManifestBlockIds(store, current.version),
  );
  const requestedIds = new Set(input.blockIds);
  const pendingBlockIds = [...requestedIds].filter((id) => !currentIds.has(id));
  const removedBlockIds = [...currentIds].filter((id) => !requestedIds.has(id));
  const transaction = {
    ...activeTransaction(`manifest-${crypto.randomUUID()}`),
    snapshotVersion: input.expectedVersion,
  };
  await store.createTransaction(transaction);
  const staged =
    pendingBlockIds.length === 0
      ? transaction
      : await store.updateTransaction(transaction.id, 0, {
          pendingBlockIds,
          updatedAt: input.createdAt ?? "2026-01-01T00:00:00.000Z",
        });
  return store.commitTransaction({
    transactionId: transaction.id,
    expectedTransactionRevision: staged.revision,
    expectedManifestVersion: input.expectedVersion,
    ...(removedBlockIds.length === 0 ? {} : { removedBlockIds }),
    committedAt: input.createdAt ?? "2026-01-01T00:00:00.000Z",
  });
}

async function readManifestBlockIds(store: BlockStore, version: number): Promise<string[]> {
  const ids: string[] = [];
  let cursor: string | null = null;
  do {
    const page = await store.listManifestBlockPage({ version, afterBlockId: cursor, limit: 256 });
    ids.push(...page.records.map((record) => record.blockId));
    cursor = page.nextCursor;
  } while (cursor !== null);
  return ids;
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
    outputCursor: { outputIndex: 0, columnIndex: 0, rowStart: 0 },
    memoryBudgetBytes: 4096,
    minimumMemoryBytes: 512,
    level0SourceStoredBytes: 360,
    anchorSourceStoredBytes: 0,
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

function level2CompactionJob(id = "level-two-job"): CompactionJobRecord {
  return {
    ...rechunkCompactionJob(id),
    level0SourceStoredBytes: 360,
    anchorSourceStoredBytes: 0,
    outputPartitionOrdinal: 3,
    maxWriteAmplification: 2,
    maximumOutputStoredBytes: 720,
    plannedOutputStoredBytesUpperBound: 600,
    targetLevel: 2,
  };
}

function mergeCompactionJob(id = "merge-job"): CompactionJobRecord {
  return {
    id,
    tableId: "events",
    sourceManifestVersion: 9,
    sourceSegmentIds: ["base-segment", "delete-segment", "upsert-segment"],
    sourceBlockIds: ["upsert-value", "base-key", "delete-key", "upsert-key", "base-value"],
    outputBlockIds: [],
    cursor: { sourceSegmentIndex: 0, sourceBlockIndex: 0 },
    processedRows: 0,
    sourceStoredBytes: 45,
    outputStoredBytes: 0,
    logicalBytes: 35,
    rewritePlan: {
      kind: "merge-v1",
      targetBlockBytes: 2 * 1024 * 1024,
      outputCompression: "gzip",
      keyColumnId: "id-column",
      totalRows: 2,
      rowIdStart: 3n,
      rowIdEndExclusive: 11n,
      rowIdSpans: [
        { rowStart: 0, rowCount: 1, rowIdStart: 10n },
        { rowStart: 1, rowCount: 1, rowIdStart: 3n },
      ],
      logicalOrder: 0,
      sourceSegments: [
        {
          segmentId: "base-segment",
          transactionId: "base-transaction",
          committedVersion: 7,
          kind: "base",
          keyColumnId: "id-column",
          level: 0,
          logicalOrder: 0,
          rowCount: 2,
          rowIdStart: 10n,
          rowIdEndExclusive: 12n,
          rowIdSpans: [{ rowStart: 0, rowCount: 2, rowIdStart: 10n }],
          columns: [
            {
              columnId: "id-column",
              type: "number",
              sourceBlocks: [
                {
                  blockId: "base-key",
                  rowStart: 0,
                  rowCount: 2,
                  storedBytes: 10,
                  encodedBytes: 8,
                  checksum: 1,
                },
              ],
            },
            {
              columnId: "value-column",
              type: "string",
              sourceBlocks: [
                {
                  blockId: "base-value",
                  rowStart: 0,
                  rowCount: 2,
                  storedBytes: 11,
                  encodedBytes: 9,
                  checksum: 2,
                },
              ],
            },
          ],
        },
        {
          segmentId: "delete-segment",
          transactionId: "delete-transaction",
          committedVersion: 8,
          kind: "delete",
          keyColumnId: "id-column",
          level: 0,
          logicalOrder: 1,
          rowCount: 1,
          rowIdStart: 0n,
          rowIdEndExclusive: 0n,
          rowIdSpans: [],
          columns: [
            {
              columnId: "id-column",
              type: "number",
              sourceBlocks: [
                {
                  blockId: "delete-key",
                  rowStart: 0,
                  rowCount: 1,
                  storedBytes: 7,
                  encodedBytes: 5,
                  checksum: 3,
                },
              ],
            },
          ],
        },
        {
          segmentId: "upsert-segment",
          transactionId: "upsert-transaction",
          committedVersion: 9,
          kind: "upsert",
          keyColumnId: "id-column",
          level: 0,
          logicalOrder: 2,
          rowCount: 1,
          rowIdStart: 3n,
          rowIdEndExclusive: 4n,
          rowIdSpans: [{ rowStart: 0, rowCount: 1, rowIdStart: 3n }],
          columns: [
            {
              columnId: "id-column",
              type: "number",
              sourceBlocks: [
                {
                  blockId: "upsert-key",
                  rowStart: 0,
                  rowCount: 1,
                  storedBytes: 8,
                  encodedBytes: 6,
                  checksum: 4,
                },
              ],
            },
            {
              columnId: "value-column",
              type: "string",
              sourceBlocks: [
                {
                  blockId: "upsert-value",
                  rowStart: 0,
                  rowCount: 1,
                  storedBytes: 9,
                  encodedBytes: 7,
                  checksum: 5,
                },
              ],
            },
          ],
        },
      ],
      columns: [
        {
          columnId: "id-column",
          type: "number",
          sourceRanges: [
            {
              outputRowStart: 0,
              sourceBlockId: "base-key",
              sourceRowStart: 0,
              rowCount: 1,
            },
            {
              outputRowStart: 1,
              sourceBlockId: "upsert-key",
              sourceRowStart: 0,
              rowCount: 1,
            },
          ],
        },
        {
          columnId: "value-column",
          type: "string",
          sourceRanges: [
            {
              outputRowStart: 0,
              sourceBlockId: "base-value",
              sourceRowStart: 0,
              rowCount: 1,
            },
            {
              outputRowStart: 1,
              sourceBlockId: "upsert-value",
              sourceRowStart: 0,
              rowCount: 1,
            },
          ],
        },
      ],
      outputs: [{ rowStart: 0, rowCount: 2 }],
    },
    outputCursor: { outputIndex: 0, columnIndex: 0, rowStart: 0 },
    memoryBudgetBytes: 4096,
    minimumMemoryBytes: 512,
    level0SourceStoredBytes: 45,
    anchorSourceStoredBytes: 0,
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

async function prepareCompactionSourceFixtures(
  store: BlockStore,
  jobs: readonly CompactionJobRecord[],
): Promise<void> {
  if (jobs.length === 0) return;
  if ((await store.getCurrentManifest()) !== undefined) {
    throw new Error("Compaction source fixtures require an empty store");
  }
  const tables = new Map(jobs.map((job) => [job.tableId, job.tableId]));
  for (const tableId of tables.keys()) {
    await store.addTable({
      managed: false,
      id: tableId,
      name: `fixture_${tableId.replaceAll(/[^a-zA-Z0-9_]/g, "_")}`,
      columns: [
        { id: "value", name: "value", type: "number", nullable: true },
        { id: "id-column", name: "id_column", type: "number", nullable: true },
        { id: "name-column", name: "name_column", type: "string", nullable: true },
        { id: "value-column", name: "value_column", type: "string", nullable: true },
      ],
      revision: 0,
      createdAt: "2026-01-01T00:00:00.000Z",
    });
  }
  const blocks = [...new Set(jobs.flatMap((job) => job.sourceBlockIds))].map((id) => ({
    id,
    bytes: Uint8Array.of(1),
  }));
  const segmentsById = new Map<string, SegmentRecord>();
  for (const job of jobs) {
    for (const [index, id] of job.sourceSegmentIds.entries()) {
      if (segmentsById.has(id)) continue;
      const blockId = job.sourceBlockIds[index % job.sourceBlockIds.length];
      if (blockId === undefined)
        throw new Error(`Compaction fixture ${job.id} has no source block`);
      segmentsById.set(id, {
        id,
        tableId: job.tableId,
        transactionId: "compaction-source-fixture",
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
    }
  }
  const staged = await stageTestArtifacts(store, {
    transactionId: "compaction-source-fixture",
    blocks,
    segments: [...segmentsById.values()].map((segment, commitOrdinal) => ({
      ...segment,
      commitOrdinal,
    })),
  });
  const tablesWithSegments = [
    ...new Set([...segmentsById.values()].map((segment) => segment.tableId)),
  ];
  const first = await store.commitTransaction({
    transactionId: staged.id,
    expectedTransactionRevision: staged.revision,
    expectedManifestVersion: null,
    levelZeroSegmentLimits: tablesWithSegments.map((tableId) => ({ tableId, limit: 4096 })),
    committedAt: "2026-01-01T00:00:00.000Z",
  });
  const maxVersion = Math.max(...jobs.map((job) => job.sourceManifestVersion));
  for (let version = first.version; version < maxVersion; version += 1) {
    await publishManifest(store, {
      expectedVersion: version,
      blockIds: blocks.map(({ id }) => id),
      createdAt: new Date(Date.UTC(2026, 0, 1, 0, 0, version + 1)).toISOString(),
    });
  }
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
  const jobId = `${prefix}/job`;
  const sourceBlockId = `${prefix}/source-block`;
  const outputBlockId = `${jobId}/output/segment/000000/column/000000/part/000000`;
  const transactionId = `${prefix}/transaction`;
  const outputSegmentId = `${prefix}/output-segment`;
  if ((await store.getTable("events")) === undefined) {
    await store.addTable({
      managed: false,
      id: "events",
      name: "events",
      columns: [{ id: "value", name: "value", type: "number", nullable: false }],
      revision: 0,
      createdAt,
    });
  }
  const sourceTransactionId = `${prefix}/source-transaction`;
  const sourceSegmentId = `${prefix}/source-segment`;
  const previous = await store.getCurrentManifest();
  const stagedSource = await stageTestArtifacts(store, {
    transactionId: sourceTransactionId,
    snapshotVersion: previous?.version ?? null,
    blocks: [{ id: sourceBlockId, bytes: Uint8Array.of(1) }],
    segments: [
      {
        id: sourceSegmentId,
        tableId: "events",
        transactionId: sourceTransactionId,
        rowCount: 1,
        rowIdStart: 1n,
        rowIdEndExclusive: 2n,
        columnBlockIds: { value: [sourceBlockId] },
        createdAt,
      },
    ],
  });
  const sourceManifest = await store.commitTransaction({
    transactionId: sourceTransactionId,
    expectedTransactionRevision: stagedSource.revision,
    expectedManifestVersion: previous?.version ?? null,
    levelZeroSegmentLimits: [{ tableId: "events", limit: 4096 }],
    committedAt: createdAt,
  });
  const stagedOutput = await stageTestArtifacts(store, {
    transactionId,
    snapshotVersion: sourceManifest.version,
    blocks: [{ id: outputBlockId, bytes: Uint8Array.of(2) }],
    segments: [
      {
        id: outputSegmentId,
        tableId: "events",
        transactionId,
        rowCount: 1,
        rowIdStart: 1n,
        rowIdEndExclusive: 2n,
        columnBlockIds: { value: [outputBlockId] },
        level: 1,
        logicalOrder: 0,
        createdAt,
      },
    ],
  });
  if (transactionState.status === "aborted") {
    await store.updateTransaction(transactionId, stagedOutput.revision, {
      status: "aborted",
      updatedAt: createdAt,
    });
  }
  const job: CompactionJobRecord = {
    id: jobId,
    tableId: "events",
    sourceManifestVersion: sourceManifest.version,
    sourceSegmentIds: [sourceSegmentId],
    sourceBlockIds: [sourceBlockId],
    outputBlockIds: [outputBlockId],
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
      expectedTransactionRevision: stagedOutput.revision,
      expectedManifestVersion: sourceManifest.version,
      removedBlockIds: [sourceBlockId],
      compactionJobId: job.id,
      committedAt: "2026-01-01T00:00:01.000Z",
    },
  };
}

async function createSupersededStorage(store: BlockStore, prefix: string): Promise<void> {
  const timestamp = "2026-01-01T00:00:00.000Z";
  const compactionId = `${prefix}/supersede-job`;
  const oldBlockId = `${prefix}/old-block`;
  const oldSegmentId = `${prefix}/old-segment`;
  const oldTransactionId = `${prefix}/old-transaction`;
  if ((await store.getTable("events")) === undefined) {
    await store.addTable({
      managed: false,
      id: "events",
      name: "events",
      columns: [{ id: "value", name: "value", type: "number", nullable: false }],
      revision: 0,
      createdAt: timestamp,
    });
  }
  const stagedOld = await stageTestArtifacts(store, {
    transactionId: oldTransactionId,
    blocks: [{ id: oldBlockId, bytes: Uint8Array.of(1, 2, 3) }],
    segments: [
      {
        id: oldSegmentId,
        tableId: "events",
        transactionId: oldTransactionId,
        rowCount: 1,
        rowIdStart: 1n,
        rowIdEndExclusive: 2n,
        columnBlockIds: { value: [oldBlockId] },
        createdAt: timestamp,
      },
    ],
  });
  await store.commitTransaction({
    transactionId: oldTransactionId,
    expectedTransactionRevision: stagedOld.revision,
    expectedManifestVersion: null,
    levelZeroSegmentLimits: [{ tableId: "events", limit: 4096 }],
    committedAt: timestamp,
  });

  const currentBlockId = `${compactionId}/output/segment/000000/column/000000/part/000000`;
  const currentSegmentId = `${prefix}/current-segment`;
  const currentTransactionId = `${prefix}/current-transaction`;
  const stagedCurrent = await stageTestArtifacts(store, {
    transactionId: currentTransactionId,
    snapshotVersion: 0,
    blocks: [{ id: currentBlockId, bytes: Uint8Array.of(4, 5) }],
    segments: [
      {
        id: currentSegmentId,
        tableId: "events",
        transactionId: currentTransactionId,
        rowCount: 1,
        rowIdStart: 1n,
        rowIdEndExclusive: 2n,
        columnBlockIds: { value: [currentBlockId] },
        level: 1,
        logicalOrder: 0,
        createdAt: timestamp,
      },
    ],
  });
  const compaction: CompactionJobRecord = {
    id: compactionId,
    tableId: "events",
    sourceManifestVersion: 0,
    sourceSegmentIds: [oldSegmentId],
    sourceBlockIds: [oldBlockId],
    outputBlockIds: [currentBlockId],
    cursor: { sourceSegmentIndex: 1, sourceBlockIndex: 0 },
    processedRows: 1,
    sourceStoredBytes: 3,
    outputStoredBytes: 2,
    logicalBytes: 2,
    rewritePlan: { kind: "copy-v1" },
    outputCursor: null,
    memoryBudgetBytes: 0,
    minimumMemoryBytes: 0,
    level0SourceStoredBytes: 3,
    anchorSourceStoredBytes: 0,
    peakWorkingBytes: 0,
    outputLogicalBytes: 2,
    targetLevel: 1,
    state: "ready",
    transactionId: currentTransactionId,
    outputSegmentId: currentSegmentId,
    publishedVersion: null,
    revision: 0,
    createdAt: timestamp,
    updatedAt: timestamp,
  };
  await store.createCompactionJob(compaction);
  await store.commitTransaction({
    transactionId: currentTransactionId,
    expectedTransactionRevision: stagedCurrent.revision,
    expectedManifestVersion: 0,
    removedBlockIds: [oldBlockId],
    compactionJobId: compaction.id,
    committedAt: "2026-01-01T00:00:01.000Z",
  });
  await store.updateCompactionJob(compaction.id, 0, {
    state: "published",
    publishedVersion: 1,
    updatedAt: "2026-01-01T00:00:01.000Z",
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
      const staged = await stageTestArtifacts(store, { blocks: [{ id: "a", bytes: source }] });
      source[0] = 9;
      expect(await store.getBlock("a")).toEqual(Uint8Array.of(1, 2, 3));
      await expect(
        store.stageTransactionArtifacts({
          transactionId: staged.id,
          expectedRevision: staged.revision,
          blocks: [{ id: "a", bytes: Uint8Array.of(4) }],
          segments: [],
          updatedAt: "2026-01-01T00:00:00.000Z",
        }),
      ).rejects.toThrow();
      store.close();
    });

    it("writes and reads blocks in bulk without partial duplicate batches", async () => {
      const store = await implementation.create();
      const staged = await stageTestArtifacts(store, {
        blocks: [
          { id: "a", bytes: Uint8Array.of(1) },
          { id: "b", bytes: Uint8Array.of(2) },
        ],
      });
      expect(await store.getBlocks(["b", "missing", "a"])).toEqual([
        Uint8Array.of(2),
        undefined,
        Uint8Array.of(1),
      ]);
      await expect(
        store.stageTransactionArtifacts({
          transactionId: staged.id,
          expectedRevision: staged.revision,
          blocks: [
            { id: "c", bytes: Uint8Array.of(3) },
            { id: "c", bytes: Uint8Array.of(4) },
          ],
          segments: [],
          updatedAt: "2026-01-01T00:00:00.000Z",
        }),
      ).rejects.toThrow("already exists");
      expect(await store.getBlock("c")).toBeUndefined();
      store.close();
    });

    it("publishes manifests with compare-and-swap", async () => {
      const store = await implementation.create();
      const first = await publishTestBlocks(store, {
        expectedVersion: null,
        blocks: [{ id: "a", bytes: Uint8Array.of(1) }],
      });
      expect(first.version).toBe(0);
      await expect(
        publishManifest(store, { expectedVersion: null, blockIds: ["a"] }),
      ).rejects.toThrow("expected null");
      expect((await store.getCurrentManifest())?.version).toBe(0);
      store.close();
    });

    it("derives logical table changes from ordinary staged segments", async () => {
      const store = await implementation.create();
      const timestamp = "2026-01-01T00:00:00.000Z";
      await store.addTable({
        managed: false,
        id: "derived-change-table",
        name: "derived_change_table",
        columns: [{ id: "value", name: "value", type: "number", nullable: false }],
        revision: 0,
        createdAt: timestamp,
      });
      const staged = await stageTestArtifacts(store, {
        transactionId: "derived-change-owner",
        blocks: [{ id: "derived-change-block", bytes: Uint8Array.of(1) }],
        segments: [
          {
            id: "derived-change-segment",
            tableId: "derived-change-table",
            transactionId: "derived-change-owner",
            rowCount: 1,
            rowIdStart: 1n,
            rowIdEndExclusive: 2n,
            columnBlockIds: { value: ["derived-change-block"] },
            createdAt: timestamp,
          },
        ],
      });

      const manifest = await store.commitTransaction({
        transactionId: staged.id,
        expectedTransactionRevision: staged.revision,
        expectedManifestVersion: null,
        // A low-level caller cannot hide an ordinary data write from live/change readers.
        changedTableIds: [],
        levelZeroSegmentLimits: [{ tableId: "derived-change-table", limit: 4096 }],
        committedAt: timestamp,
      });

      expect(manifest.changedTableIds).toEqual(["derived-change-table"]);
      store.close();
    });

    it("preserves and validates SQL integer-domain catalog metadata", async () => {
      const store = await implementation.create();
      const timestamp = "2026-01-01T00:00:00.000Z";
      await store.addTable({
        managed: false,
        id: "integer-table",
        name: "integers",
        columns: [
          { id: "integer-column", name: "value", type: "number", integer: true, nullable: false },
        ],
        revision: 0,
        createdAt: timestamp,
      });
      expect((await store.getTable("integer-table"))?.columns[0]).toMatchObject({
        type: "number",
        integer: true,
      });
      await expect(
        store.addTable({
          managed: false,
          id: "invalid-integer-table",
          name: "invalid-integers",
          columns: [
            {
              id: "invalid-integer-column",
              name: "value",
              type: "string",
              integer: true,
              nullable: false,
            },
          ],
          revision: 0,
          createdAt: timestamp,
        }),
      ).rejects.toThrow("Integer domain requires a number column");
      store.close();
    });

    it("returns one coherent query catalog state matching the individual reads", async () => {
      const store = await implementation.create();
      const timestamp = "2026-01-01T00:00:00.000Z";
      await store.addTable({
        managed: false,
        id: "events-id",
        name: "events",
        columns: [{ id: "value-column", name: "value", type: "number", nullable: false }],
        revision: 0,
        createdAt: timestamp,
      });
      await store.addTable({
        managed: false,
        id: "other-id",
        name: "other",
        columns: [{ id: "other-column", name: "label", type: "string", nullable: false }],
        revision: 0,
        createdAt: timestamp,
      });
      const stagedState = await stageTestArtifacts(store, {
        transactionId: "state-transaction",
        blocks: [{ id: "state-block", bytes: Uint8Array.of(1) }],
        segments: [
          {
            id: "state-segment",
            tableId: "events-id",
            transactionId: "state-transaction",
            rowCount: 1,
            rowIdStart: 1n,
            rowIdEndExclusive: 2n,
            columnBlockIds: { "value-column": ["state-block"] },
            createdAt: timestamp,
          },
          {
            id: "other-segment",
            tableId: "other-id",
            transactionId: "state-transaction",
            rowCount: 1,
            rowIdStart: 1n,
            rowIdEndExclusive: 2n,
            columnBlockIds: { "other-column": ["state-block"] },
            createdAt: timestamp,
          },
        ],
      });
      await store.commitTransaction({
        transactionId: "state-transaction",
        expectedTransactionRevision: stagedState.revision,
        expectedManifestVersion: null,
        levelZeroSegmentLimits: [
          { tableId: "events-id", limit: 4096 },
          { tableId: "other-id", limit: 4096 },
        ],
        committedAt: timestamp,
      });
      // A retained historical/staged record for this table is not part of the current
      // manifest. The atomic query read must not make every foreground prepare pay for it.
      await stageTestArtifacts(store, {
        transactionId: "state-historical-transaction",
        snapshotVersion: 0,
        blocks: [{ id: "state-historical-block", bytes: Uint8Array.of(2) }],
        segments: [
          {
            id: "state-historical-segment",
            tableId: "events-id",
            transactionId: "state-historical-transaction",
            rowCount: 1,
            rowIdStart: 2n,
            rowIdEndExclusive: 3n,
            columnBlockIds: { "value-column": ["state-historical-block"] },
            createdAt: timestamp,
          },
        ],
      });

      const before = await store.getCatalogProbe();
      const tables = [await store.getTableByName("events"), await store.getTableByName("missing")];
      const allSegments = await listAllSegments(store, "events-id");
      const membership = await store.hasManifestBlocks(before.manifestVersion, [
        "state-block",
        "state-historical-block",
      ]);
      const segments = allSegments.filter((segment) =>
        Object.values(segment.columnBlockIds)
          .flat()
          .every((id) => id === "state-block" && membership[0] === true),
      );
      const transactions = (await store.getTransactions(["state-transaction"])).filter(
        (record) => record !== undefined,
      );
      expect(await store.getCatalogProbe()).toEqual(before);
      expect(before.manifestVersion).toBe(await store.getCurrentManifestVersion());
      expect(
        await store.hasManifestBlocks(before.manifestVersion, [
          "state-block",
          "state-historical-block",
        ]),
      ).toEqual([true, false]);
      expect(tables).toEqual([await store.getTableByName("events"), undefined]);
      // Only the found tables' current-manifest segments are returned. Explicit historical
      // reads continue to use listSegments and can see the retained record.
      expect((await listAllSegments(store, "events-id")).map((segment) => segment.id)).toEqual([
        "state-historical-segment",
        "state-segment",
      ]);
      expect(segments.map((segment) => segment.id)).toEqual(["state-segment"]);
      expect(transactions).toEqual(
        (await store.getTransactions(["state-transaction"])).filter(
          (record) => record !== undefined,
        ),
      );
      store.close();
    });

    it("advances the catalog epoch on every catalog mutation and only those", async () => {
      const store = await implementation.create();
      const probe = store.getCatalogProbe.bind(store);
      const timestamp = "2026-01-01T00:00:00.000Z";
      const initial = await probe();
      expect(initial.manifestVersion).toBeNull();
      expect(initial.schemaEpoch).toBe(0);

      await store.addTable({
        managed: false,
        id: "epoch-id",
        name: "epoch",
        columns: [{ id: "epoch-column", name: "value", type: "number", nullable: false }],
        revision: 0,
        createdAt: timestamp,
      });
      const afterAdd = await probe();
      expect(afterAdd.catalogEpoch).toBeGreaterThan(initial.catalogEpoch);
      expect(afterAdd.schemaEpoch).toBeGreaterThan(initial.schemaEpoch);

      await store.updateTable("epoch-id", 0, {
        columns: [{ id: "epoch-column", name: "renamed", type: "number", nullable: false }],
      });
      const afterUpdate = await probe();
      expect(afterUpdate.catalogEpoch).toBeGreaterThan(afterAdd.catalogEpoch);
      expect(afterUpdate.schemaEpoch).toBeGreaterThan(afterAdd.schemaEpoch);

      // Block and segment staging are not catalog mutations: nothing a reader could have
      // cached changes until the publish makes the staged work visible.
      const stagedEpoch = await stageTestArtifacts(store, {
        transactionId: "epoch-transaction",
        blocks: [{ id: "epoch-block", bytes: Uint8Array.of(1) }],
        segments: [
          {
            id: "epoch-segment",
            tableId: "epoch-id",
            transactionId: "epoch-transaction",
            rowCount: 1,
            rowIdStart: 1n,
            rowIdEndExclusive: 2n,
            columnBlockIds: { "epoch-column": ["epoch-block"] },
            createdAt: timestamp,
          },
        ],
      });
      const afterStaging = await probe();
      expect(afterStaging.catalogEpoch).toBe(afterUpdate.catalogEpoch);
      expect(afterStaging.schemaEpoch).toBe(afterUpdate.schemaEpoch);

      await store.commitTransaction({
        transactionId: "epoch-transaction",
        expectedTransactionRevision: stagedEpoch.revision,
        expectedManifestVersion: null,
        levelZeroSegmentLimits: [{ tableId: "epoch-id", limit: 4096 }],
        committedAt: timestamp,
      });
      const afterCommit = await probe();
      expect(afterCommit.catalogEpoch).toBeGreaterThan(afterStaging.catalogEpoch);
      expect(afterCommit.schemaEpoch).toBe(afterStaging.schemaEpoch);
      expect(afterCommit.manifestVersion).toBe(0);

      await publishManifest(store, { expectedVersion: 0, blockIds: ["epoch-block"] });
      const afterPublish = await probe();
      expect(afterPublish.catalogEpoch).toBeGreaterThan(afterCommit.catalogEpoch);
      expect(afterPublish.schemaEpoch).toBe(afterCommit.schemaEpoch);
      expect(afterPublish.manifestVersion).toBe(1);

      expect((await store.getCatalogProbe()).catalogEpoch).toBe(afterPublish.catalogEpoch);
      store.close();
    });

    it("serializes staged writes with structural DDL but ignores accelerator build churn", async () => {
      const store = await implementation.create();
      const timestamp = "2026-01-01T00:00:00.000Z";
      const index = {
        name: "epoch_value_idx",
        columnId: "value",
        columnIds: ["value"],
        directions: ["asc" as const],
        termEncoding: "tuple-v1" as const,
        storage: "postings-v1" as const,
        storageColumnId: "index-storage",
        locator: "row-id" as const,
        state: "building" as const,
        buildId: "index-build",
        buildFromVersion: -1,
      };
      const { buildId: _buildId, ...readyIndex } = index;
      void _buildId;
      await store.addTable({
        managed: false,
        id: "schema-guard-table",
        name: "schema_guard_table",
        columns: [{ id: "value", name: "value", type: "number", nullable: false }],
        secondaryIndexes: { "value-index": index },
        revision: 0,
        createdAt: timestamp,
      });

      const begun = await store.beginTransaction({
        record: {
          ...activeTransaction("schema-build-churn"),
          revision: 0,
          startedAt: timestamp,
          updatedAt: timestamp,
          expiresAt: "2026-01-01T00:30:00.000Z",
        },
      });
      const staged = await store.stageTransactionArtifacts({
        transactionId: begun.record.id,
        expectedRevision: begun.record.revision,
        blocks: [{ id: "schema-build-block", bytes: Uint8Array.of(1) }],
        segments: [
          {
            id: "schema-build-segment",
            tableId: "schema-guard-table",
            transactionId: begun.record.id,
            rowCount: 1,
            rowIdStart: 1n,
            rowIdEndExclusive: 2n,
            columnBlockIds: { value: ["schema-build-block"] },
            kind: "insert",
            level: 0,
            logicalOrder: 0,
            commitOrdinal: 0,
            rowIdSpans: [],
            createdAt: timestamp,
          },
        ],
        updatedAt: timestamp,
      });
      const beforeBuildState = await store.getCatalogProbe();
      await store.updateTable("schema-guard-table", 0, {
        secondaryIndexes: {
          "value-index": {
            ...readyIndex,
            state: "ready",
            buildFromVersion: -1,
          },
        },
      });
      const afterBuildState = await store.getCatalogProbe();
      expect(afterBuildState.catalogEpoch).toBeGreaterThan(beforeBuildState.catalogEpoch);
      expect(afterBuildState.schemaEpoch).toBe(beforeBuildState.schemaEpoch);
      await store.commitTransaction({
        transactionId: staged.id,
        expectedTransactionRevision: staged.revision,
        expectedManifestVersion: null,
        levelZeroSegmentLimits: [{ tableId: "schema-guard-table", limit: 4096 }],
        committedAt: timestamp,
      });

      let tableRevision = (await store.getTable("schema-guard-table"))?.revision;
      if (tableRevision === undefined) throw new Error("Schema-guard table disappeared");
      let structuralProbe = await store.getCatalogProbe();
      await store.updateTable("schema-guard-table", tableRevision, { secondaryIndexes: null });
      tableRevision += 1;
      let nextStructuralProbe = await store.getCatalogProbe();
      expect(nextStructuralProbe.schemaEpoch).toBeGreaterThan(structuralProbe.schemaEpoch);
      structuralProbe = nextStructuralProbe;
      await store.updateTable("schema-guard-table", tableRevision, {
        secondaryIndexes: { "value-index": index },
      });
      tableRevision += 1;
      nextStructuralProbe = await store.getCatalogProbe();
      expect(nextStructuralProbe.schemaEpoch).toBeGreaterThan(structuralProbe.schemaEpoch);
      structuralProbe = nextStructuralProbe;
      await store.updateTable("schema-guard-table", tableRevision, {
        secondaryIndexes: { "value-index": { ...index, unique: true } },
      });
      tableRevision += 1;
      nextStructuralProbe = await store.getCatalogProbe();
      expect(nextStructuralProbe.schemaEpoch).toBeGreaterThan(structuralProbe.schemaEpoch);
      structuralProbe = nextStructuralProbe;
      await store.updateTable("schema-guard-table", tableRevision, {
        secondaryIndexes: {
          "value-index": {
            ...readyIndex,
            unique: true,
            uniqueEnforced: true,
            state: "ready",
          },
        },
        uniqueKeySeed: {
          namespaceId: secondaryUniqueKeyNamespace("schema-guard-table", "value-index"),
          keyTokens: [],
        },
      });
      tableRevision += 1;
      nextStructuralProbe = await store.getCatalogProbe();
      expect(nextStructuralProbe.schemaEpoch).toBeGreaterThan(structuralProbe.schemaEpoch);

      const stale = await store.beginTransaction({
        record: {
          ...activeTransaction("stale-schema-writer"),
          revision: 0,
          startedAt: timestamp,
          updatedAt: timestamp,
          expiresAt: "2026-01-01T00:30:00.000Z",
        },
      });
      const staleStaged = await store.stageTransactionArtifacts({
        transactionId: stale.record.id,
        expectedRevision: stale.record.revision,
        blocks: [{ id: "stale-schema-block", bytes: Uint8Array.of(2) }],
        segments: [
          {
            id: "stale-schema-segment",
            tableId: "schema-guard-table",
            transactionId: stale.record.id,
            rowCount: 1,
            rowIdStart: 2n,
            rowIdEndExclusive: 3n,
            columnBlockIds: { value: ["stale-schema-block"] },
            kind: "insert",
            level: 0,
            logicalOrder: 0,
            commitOrdinal: 0,
            rowIdSpans: [],
            createdAt: timestamp,
          },
        ],
        updatedAt: timestamp,
      });
      await store.updateTable("schema-guard-table", tableRevision, {
        columns: [
          { id: "value", name: "value", type: "number", nullable: false },
          { id: "added", name: "added", type: "string", nullable: true },
        ],
      });
      const beforeRefusal = await store.getTransaction(staleStaged.id);
      await expect(
        store.commitTransaction({
          transactionId: staleStaged.id,
          expectedTransactionRevision: staleStaged.revision,
          expectedManifestVersion: 0,
          levelZeroSegmentLimits: [{ tableId: "schema-guard-table", limit: 4096 }],
          committedAt: timestamp,
        }),
      ).rejects.toBeInstanceOf(SchemaConflictError);
      expect(await store.getTransaction(staleStaged.id)).toEqual(beforeRefusal);
      expect(await store.getCurrentManifestVersion()).toBe(0);
      await store.updateTransaction(staleStaged.id, staleStaged.revision, {
        status: "aborted",
        updatedAt: timestamp,
      });
      store.close();
    });

    it("sorts multi-table catalog segments globally by id", async () => {
      const store = await implementation.create();
      const timestamp = "2026-01-01T00:00:00.000Z";
      for (const [tableId, name] of [
        ["sort-a-id", "sort-a"],
        ["sort-b-id", "sort-b"],
      ] as const) {
        await store.addTable({
          managed: false,
          id: tableId,
          name,
          columns: [{ id: `${tableId}-column`, name: "value", type: "number", nullable: false }],
          revision: 0,
          createdAt: timestamp,
        });
      }
      // Interleave segment ids across the two tables so a per-table concatenation without a
      // global re-sort would come back misordered.
      const segmentIds = [
        ["segment-1", "sort-a-id"],
        ["segment-2", "sort-b-id"],
        ["segment-3", "sort-a-id"],
        ["segment-4", "sort-b-id"],
      ] as const;
      const stagedSort = await stageTestArtifacts(store, {
        transactionId: "sort-transaction",
        blocks: [{ id: "sort-block", bytes: Uint8Array.of(1) }],
        segments: segmentIds.map(([segmentId, tableId]) => ({
          id: segmentId,
          tableId,
          transactionId: "sort-transaction",
          rowCount: 1,
          rowIdStart: 1n,
          rowIdEndExclusive: 2n,
          columnBlockIds: { [`${tableId}-column`]: ["sort-block"] },
          createdAt: timestamp,
        })),
      });
      await store.commitTransaction({
        transactionId: "sort-transaction",
        expectedTransactionRevision: stagedSort.revision,
        expectedManifestVersion: null,
        levelZeroSegmentLimits: [
          { tableId: "sort-a-id", limit: 4096 },
          { tableId: "sort-b-id", limit: 4096 },
        ],
        committedAt: timestamp,
      });
      const segments = [
        ...(await listAllSegments(store, "sort-b-id")),
        ...(await listAllSegments(store, "sort-a-id")),
      ].sort((left, right) => left.id.localeCompare(right.id));
      expect(segments.map((segment) => segment.id)).toEqual([
        "segment-1",
        "segment-2",
        "segment-3",
        "segment-4",
      ]);
      store.close();
    });

    it("pages manifests, segments, and transactions with stable exclusive cursors", async () => {
      const store = await implementation.create();
      await store.addTable({
        managed: false,
        id: "page-table",
        name: "page_table",
        columns: [{ id: "value", name: "value", type: "number", nullable: false }],
        revision: 0,
        createdAt: "2026-01-01T00:00:00.000Z",
      });
      await publishTestBlocks(store, {
        expectedVersion: null,
        blocks: [{ id: "page-block", bytes: Uint8Array.of(1) }],
      });
      await publishManifest(store, { expectedVersion: 0, blockIds: ["page-block"] });
      await publishManifest(store, { expectedVersion: 1, blockIds: ["page-block"] });
      for (const id of ["transaction-c", "transaction-b"]) {
        await store.createTransaction({ ...activeTransaction(id), snapshotVersion: 2 });
      }
      await stageTestArtifacts(store, {
        transactionId: "transaction-a",
        snapshotVersion: 2,
        blocks: ["c", "a", "b"].map((suffix) => ({
          id: `page-segment-block-${suffix}`,
          bytes: Uint8Array.of(1),
        })),
        segments: ["c", "a", "b"].map((suffix) => ({
          id: `segment-${suffix}`,
          tableId: "page-table",
          transactionId: "transaction-a",
          rowCount: 1,
          rowIdStart: 1n,
          rowIdEndExclusive: 2n,
          columnBlockIds: { value: [`page-segment-block-${suffix}`] },
          createdAt: "2026-01-01T00:00:00.000Z",
        })),
      });

      const firstManifests = await store.listManifestPage(null, 2);
      expect(firstManifests.records.map((manifest) => manifest.version)).toEqual([0, 1]);
      expect(firstManifests.nextCursor).toBe(1);
      const finalManifests = await store.listManifestPage(firstManifests.nextCursor, 2);
      expect(finalManifests.records.map((manifest) => manifest.version)).toEqual([2]);
      expect(finalManifests.nextCursor).toBeNull();

      const firstSegments = await store.listSegmentPage(null, 2);
      expect(firstSegments.records.map((segment) => segment.id)).toEqual([
        "segment-a",
        "segment-b",
      ]);
      expect(firstSegments.nextCursor).toBe("segment-b");
      const finalSegments = await store.listSegmentPage(firstSegments.nextCursor, 2);
      expect(finalSegments.records.map((segment) => segment.id)).toEqual(["segment-c"]);
      expect(finalSegments.nextCursor).toBeNull();

      const transactionIds: string[] = [];
      let transactionCursor: string | null = null;
      do {
        const page = await store.listTransactionPage(transactionCursor, 2);
        transactionIds.push(...page.records.map((transaction) => transaction.id));
        transactionCursor = page.nextCursor;
      } while (transactionCursor !== null);
      expect(transactionIds).toEqual([...transactionIds].sort());
      expect(transactionIds).toEqual(
        expect.arrayContaining(["transaction-a", "transaction-b", "transaction-c"]),
      );
      await expect(store.listManifestPage(null, 0)).rejects.toThrow("positive");
      await expect(store.listSegmentPage(null, 0)).rejects.toThrow("positive");
      store.close();
    });

    it("never publishes a missing block", async () => {
      const store = await implementation.create();
      await expect(
        publishManifest(store, { expectedVersion: null, blockIds: ["missing"] }),
      ).rejects.toThrow(/missing.*block/);
      expect(await store.getCurrentManifest()).toBeUndefined();
      store.close();
    });

    it("stores table and segment records", async () => {
      const store = await implementation.create();
      await store.addTable({
        managed: false,
        id: "people-id",
        name: "people",
        columns: [{ id: "name-id", name: "name", type: "string", nullable: false }],
        revision: 0,
        createdAt: "2026-01-01T00:00:00.000Z",
      });
      await expect(
        store.addTable({
          managed: false,
          id: "another-id",
          name: "people",
          columns: [{ id: "age-id", name: "age", type: "number", nullable: false }],
          revision: 0,
          createdAt: "2026-01-01T00:00:00.000Z",
        }),
      ).rejects.toThrow("name already exists");
      await stageTestArtifacts(store, {
        transactionId: "transaction-1",
        blocks: [{ id: "block-1", bytes: Uint8Array.of(1) }],
        segments: [
          {
            id: "segment-1",
            tableId: "people-id",
            transactionId: "transaction-1",
            rowCount: 2,
            rowIdStart: 1n,
            rowIdEndExclusive: 3n,
            columnBlockIds: { "name-id": ["block-1"] },
            createdAt: "2026-01-01T00:00:00.000Z",
          },
        ],
      });
      await stageTestArtifacts(store, {
        transactionId: "transaction-2",
        blocks: [{ id: "block-2", bytes: Uint8Array.of(2) }],
        segments: [
          {
            id: "segment-2",
            tableId: "people-id",
            transactionId: "transaction-2",
            rowCount: 1,
            rowIdStart: 3n,
            rowIdEndExclusive: 4n,
            columnBlockIds: { "name-id": ["block-2"] },
            level: 1,
            logicalOrder: 4.5,
            createdAt: "2026-01-01T00:00:01.000Z",
          },
        ],
      });

      expect((await store.listTables())[0]?.name).toBe("people");
      expect((await listAllSegments(store, "people-id"))[0]?.rowIdEndExclusive).toBe(3n);
      expect(await store.getSegment("segment-1")).toMatchObject({ kind: "insert", level: 0 });
      expect(await store.getSegment("segment-2")).toMatchObject({ level: 1, logicalOrder: 4.5 });
      store.close();
    });

    it("round trips ordered row-ID spans and contiguous row-ID envelopes", async () => {
      const store = await implementation.create();
      await store.addTable({
        managed: false,
        id: "events",
        name: "events",
        columns: [{ id: "value", name: "value", type: "number", nullable: false }],
        revision: 0,
        createdAt: "2026-01-01T00:00:00.000Z",
      });
      const rowIdSpans = [
        { rowStart: 0, rowCount: 1, rowIdStart: 10n },
        { rowStart: 1, rowCount: 1, rowIdStart: 3n },
      ];
      await stageTestArtifacts(store, {
        transactionId: "merge-transaction",
        blocks: [{ id: "merged-block", bytes: Uint8Array.of(1) }],
        segments: [
          {
            id: "merged-segment",
            tableId: "events",
            transactionId: "merge-transaction",
            rowCount: 2,
            rowIdStart: 3n,
            rowIdEndExclusive: 11n,
            rowIdSpans,
            columnBlockIds: { value: ["merged-block"] },
            kind: "base",
            level: 1,
            logicalOrder: 0,
            createdAt: "2026-01-01T00:00:00.000Z",
          },
        ],
      });
      rowIdSpans[0] = { rowStart: 0, rowCount: 2, rowIdStart: 99n };
      expect(await store.getSegment("merged-segment")).toMatchObject({
        kind: "base",
        rowIdStart: 3n,
        rowIdEndExclusive: 11n,
        rowIdSpans: [
          { rowStart: 0, rowCount: 1, rowIdStart: 10n },
          { rowStart: 1, rowCount: 1, rowIdStart: 3n },
        ],
      });

      await stageTestArtifacts(store, {
        transactionId: "legacy-transaction",
        blocks: [{ id: "legacy-block", bytes: Uint8Array.of(2) }],
        segments: [
          {
            id: "legacy-segment",
            tableId: "events",
            transactionId: "legacy-transaction",
            rowCount: 1,
            rowIdStart: 20n,
            rowIdEndExclusive: 21n,
            columnBlockIds: { value: ["legacy-block"] },
            kind: "insert",
            level: 0,
            logicalOrder: 0,
            commitOrdinal: 0,
            rowIdSpans: [],
            createdAt: "2026-01-01T00:00:00.000Z",
          },
        ],
      });
      expect(await store.getSegment("legacy-segment")).toMatchObject({ rowIdSpans: [] });
      store.close();
    });

    it("round trips canonical append-row-range L2 partition metadata", async () => {
      const store = await implementation.create();
      await store.addTable({
        managed: false,
        id: "events",
        name: "events",
        columns: [{ id: "value", name: "value", type: "number", nullable: false }],
        revision: 0,
        createdAt: "2026-01-01T00:00:00.000Z",
      });
      const partition: SegmentRecord = {
        id: "partition-2",
        tableId: "events",
        transactionId: "partition-transaction",
        rowCount: 3,
        rowIdStart: 7n,
        rowIdEndExclusive: 10n,
        columnBlockIds: { value: ["partition-block"] },
        kind: "insert",
        level: 2,
        logicalOrder: 11,
        commitOrdinal: 0,
        rowIdSpans: [],
        partitionOrdinal: 2,
        createdAt: "2026-01-01T00:00:00.000Z",
      };
      await stageTestArtifacts(store, {
        transactionId: partition.transactionId,
        blocks: [{ id: "partition-block", bytes: Uint8Array.of(1) }],
        segments: [partition],
      });
      expect(await store.getSegment(partition.id)).toEqual(partition);
      expect((await listAllSegments(store, partition.tableId)).map(({ id }) => id)).toEqual([
        "partition-2",
      ]);
      store.close();
    });

    it("rejects malformed append-row-range L2 partition metadata", async () => {
      const store = await implementation.create();
      await store.addTable({
        managed: false,
        id: "events",
        name: "events",
        columns: [{ id: "value", name: "value", type: "number", nullable: false }],
        revision: 0,
        createdAt: "2026-01-01T00:00:00.000Z",
      });
      const staged = await stageTestArtifacts(store, {
        transactionId: "partition-transaction",
        blocks: [{ id: "partition-block", bytes: Uint8Array.of(1) }],
      });
      const valid: SegmentRecord = {
        id: "valid-partition",
        tableId: "events",
        transactionId: "partition-transaction",
        rowCount: 2,
        rowIdStart: 5n,
        rowIdEndExclusive: 7n,
        columnBlockIds: { value: ["partition-block"] },
        kind: "insert",
        level: 2,
        logicalOrder: 4,
        commitOrdinal: 0,
        rowIdSpans: [],
        partitionOrdinal: 0,
        createdAt: "2026-01-01T00:00:00.000Z",
      };
      const withoutLogicalOrder = { ...valid };
      delete (withoutLogicalOrder as { logicalOrder?: number }).logicalOrder;
      const withoutPartitionOrdinal = { ...valid, id: "missing-partition-ordinal" };
      delete (withoutPartitionOrdinal as { partitionOrdinal?: number }).partitionOrdinal;
      const invalid: Array<{ record: SegmentRecord; message: string | RegExp }> = [
        {
          record: withoutPartitionOrdinal,
          message: "requires a partition ordinal",
        },
        {
          record: { ...valid, id: "negative-ordinal", partitionOrdinal: -1 },
          message: "partition ordinal",
        },
        {
          record: { ...valid, id: "fractional-ordinal", partitionOrdinal: 0.5 },
          message: "partition ordinal",
        },
        {
          record: { ...valid, id: "wrong-level", level: 1 },
          message: /requires level two|explicit level two/u,
        },
        {
          record: { ...valid, id: "wrong-kind", kind: "update" },
          message: "must be an insert or a merged base",
        },
        {
          record: { ...valid, id: "base-without-spans", kind: "base" },
          message: "requires row ID spans",
        },
        {
          record: {
            ...valid,
            id: "base-overlapping-spans",
            kind: "base",
            rowCount: 4,
            rowIdSpans: [
              { rowStart: 0, rowCount: 2, rowIdStart: 5n },
              { rowStart: 2, rowCount: 2, rowIdStart: 6n },
            ],
          },
          message: "overlap",
        },
        {
          record: {
            ...valid,
            id: "base-span-row-count-mismatch",
            kind: "base",
            rowCount: 5,
            rowIdSpans: [{ rowStart: 0, rowCount: 2, rowIdStart: 5n }],
          },
          message: /cover exactly the row count|row ID spans do not match/,
        },
        {
          record: { ...withoutLogicalOrder, id: "missing-logical-order" },
          message: /logical order (?:must be a non-negative finite number|is invalid)/,
        },
        {
          record: { ...valid, id: "negative-logical-order", logicalOrder: -1 },
          message: /logical order (?:must be a non-negative finite number|is invalid)/,
        },
        {
          record: {
            ...valid,
            id: "spanned-partition",
            rowIdSpans: [{ rowStart: 0, rowCount: 2, rowIdStart: 5n }],
          },
          message: /cannot contain row ID spans|row ID spans are invalid/,
        },
        {
          record: {
            ...valid,
            id: "empty-partition",
            rowCount: 0,
            rowIdEndExclusive: valid.rowIdStart,
          },
          message: "row count must be positive",
        },
        {
          record: {
            ...valid,
            id: "zero-row-id",
            rowIdStart: 0n,
            rowIdEndExclusive: 2n,
          },
          message: /row ID start must be a positive bigint|row ID envelope does not match/,
        },
        {
          record: { ...valid, id: "gapped-envelope", rowIdEndExclusive: 8n },
          message: /contiguous positive row ID envelope|row ID envelope does not match/,
        },
      ];

      for (const { record, message } of invalid) {
        await expect(
          store.stageTransactionArtifacts({
            transactionId: staged.id,
            expectedRevision: staged.revision,
            blocks: [],
            segments: [record],
            updatedAt: "2026-01-01T00:00:00.000Z",
          }),
        ).rejects.toThrow(message);
        expect(await store.getSegment(record.id)).toBeUndefined();
      }
      store.close();
    });

    it("atomically stamps committed segments with stable logical order", async () => {
      const store = await implementation.create();
      const timestamp = "2026-01-01T00:00:00.000Z";
      await store.addTable({
        managed: false,
        id: "events",
        name: "events",
        columns: [{ id: "value", name: "value", type: "number", nullable: false }],
        revision: 0,
        createdAt: timestamp,
      });
      const staged = await stageTestArtifacts(store, {
        transactionId: "segment-transaction",
        blocks: [{ id: "segment-block", bytes: Uint8Array.of(1) }],
        segments: [
          {
            id: "committed-segment",
            tableId: "events",
            transactionId: "segment-transaction",
            rowCount: 1,
            rowIdStart: 1n,
            rowIdEndExclusive: 2n,
            columnBlockIds: { value: ["segment-block"] },
            createdAt: timestamp,
          },
        ],
      });

      await store.commitTransaction({
        transactionId: "segment-transaction",
        expectedTransactionRevision: staged.revision,
        expectedManifestVersion: null,
        levelZeroSegmentLimits: [{ tableId: "events", limit: 4096 }],
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

    it("reads transaction batches in request order with isolated records", async () => {
      const store = await implementation.create();
      const first = activeTransaction("batch-first");
      const second = activeTransaction("batch-second");
      await store.createTransaction(first);
      await store.createTransaction(second);
      const storedFirst = await store.getTransaction(first.id);
      const storedSecond = await store.getTransaction(second.id);
      expect(storedFirst).toBeDefined();
      expect(storedSecond).toBeDefined();

      const records = await store.getTransactions([
        second.id,
        "batch-missing",
        first.id,
        second.id,
      ]);
      expect(records).toEqual([storedSecond, undefined, storedFirst, storedSecond]);
      const returned = records[0];
      if (returned !== undefined) returned.pendingBlockIds.push("mutated-result");
      expect(await store.getTransaction(second.id)).toEqual(storedSecond);
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

    it("rejects creating a transaction with a prepopulated artifact journal", async () => {
      const store = await implementation.create();
      const timestamp = "2026-01-01T00:00:00.000Z";
      await expect(
        store.createTransaction({
          id: "missing-segment-transaction",
          ownerId: "missing-segment-transaction/owner",
          expiresAt: "2026-01-01T00:30:00.000Z",
          snapshotVersion: null,
          pendingBlockIds: [],
          pendingSegmentIds: ["missing-segment"],
          status: "active",
          revision: 0,
          startedAt: timestamp,
          updatedAt: timestamp,
          committedVersion: null,
        }),
      ).rejects.toThrow("fresh transaction cannot begin with pending artifacts");
      expect(await store.getTransaction("missing-segment-transaction")).toBeUndefined();
      store.close();
    });

    it("reserves non-overlapping internal row ID ranges", async () => {
      const store = await implementation.create();
      await addCounterFixtureTables(store);
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

    it("reserves monotone auto-increment ranges per column", async () => {
      const store = await implementation.create();
      await addCounterFixtureTables(store);
      expect(await store.reserveAutoIncrement("people", "id", 3)).toEqual({
        start: 1n,
        endExclusive: 4n,
      });
      expect(await store.reserveAutoIncrement("people", "id", 2)).toEqual({
        start: 4n,
        endExclusive: 6n,
      });
      // Counters are per column and independent of the row-id counter.
      expect(await store.reserveAutoIncrement("people-other", "other", 1)).toEqual({
        start: 1n,
        endExclusive: 2n,
      });
      expect(await store.reserveRowIds("people", 1)).toEqual({ start: 1n, endExclusive: 2n });
      store.close();
    });

    it("bumps the auto-increment counter past explicit values", async () => {
      const store = await implementation.create();
      await addCounterFixtureTables(store);
      // A pure bump reserves nothing but advances the counter past imported values.
      expect(await store.reserveAutoIncrement("people", "id", 0, 101n)).toEqual({
        start: 101n,
        endExclusive: 101n,
      });
      expect(await store.reserveAutoIncrement("people", "id", 2)).toEqual({
        start: 101n,
        endExclusive: 103n,
      });
      // A bump below the stored counter is a no-op floor, never a rewind.
      expect(await store.reserveAutoIncrement("people", "id", 1, 5n)).toEqual({
        start: 103n,
        endExclusive: 104n,
      });
      await expect(store.reserveAutoIncrement("people", "id", -1)).rejects.toThrow(
        "non-negative whole number",
      );
      await expect(store.reserveAutoIncrement("people", "id", 1, 0n)).rejects.toThrow(
        /bump target|at least 1/,
      );
      store.close();
    });

    it("round-trips full-text base chunks and merges commit deltas", async () => {
      const store = await implementation.create();
      await store.addTable({
        managed: false,
        id: "articles",
        name: "articles",
        columns: [{ id: "title", name: "title", type: "string", nullable: false }],
        ftsColumns: {
          title: {
            storage: "fts-chunks-v1",
            tokenizerVersion: 1,
            state: "ready",
            buildFromVersion: 4,
          },
        },
        revision: 0,
        createdAt: "2026-01-01T00:00:00.000Z",
      });
      await store.writeFtsBase("articles", "title", {
        coversVersion: 4,
        chunks: [
          [
            { term: "alpha", rowIds: [1n, 3n], tf: [1, 2] },
            { term: "beta", rowIds: [2n], tf: [1] },
          ],
          [{ term: "gamma", rowIds: [3n], tf: [1] }],
        ],
        totalTokens: 5,
      });
      const exact = await store.readFtsCandidates(
        "articles",
        "title",
        [{ term: "alpha", prefix: false }],
        10,
      );
      expect(exact.rowIdsByTerm).toEqual([[1n, 3n]]);
      expect(exact.overflow).toBe(false);
      expect(exact.deltaChunkCount).toBe(0);
      expect(exact.totalTokens).toBe(5);
      // Prefix terms match a term range; ga* reaches into the second chunk.
      const prefix = await store.readFtsCandidates(
        "articles",
        "title",
        [
          { term: "b", prefix: true },
          { term: "ga", prefix: true },
        ],
        10,
      );
      expect(prefix.rowIdsByTerm).toEqual([[2n], [3n]]);
      const range = await store.readFtsCandidates(
        "articles",
        "title",
        [{ lower: "beta", lowerInclusive: true, upper: "gamma", upperInclusive: false }],
        10,
      );
      expect(range.rowIdsByTerm).toEqual([[2n]]);
      store.close();
    });

    it("publishes a chunked postings generation atomically and replaces an abandoned build", async () => {
      const store = await implementation.create();
      await store.addTable({
        managed: false,
        id: "bounded",
        name: "bounded",
        columns: [{ id: "value", name: "value", type: "string", nullable: false }],
        ftsColumns: {
          value: {
            storage: "fts-chunks-v1",
            tokenizerVersion: 1,
            state: "building",
            buildFromVersion: -1,
          },
        },
        revision: 0,
        createdAt: "2026-01-01T00:00:00.000Z",
      });
      const abandonedExpiry = "2026-01-01T00:05:00.000Z";
      await beginPostingBuild(
        store,
        "bounded",
        "value",
        "abandoned",
        POSTING_BUILD_CREATED_AT,
        abandonedExpiry,
      );
      await appendPostingBuild(
        store,
        "bounded",
        "value",
        "abandoned",
        0,
        [{ term: "old", rowIds: [1n], tf: [1] }],
        POSTING_BUILD_CREATED_AT,
        abandonedExpiry,
      );
      // Beginning a replacement reclaims the unfinished generation; it never becomes readable.
      await beginPostingBuild(
        store,
        "bounded",
        "value",
        "replacement",
        "2026-01-01T00:10:00.000Z",
        "2026-01-01T00:40:00.000Z",
      );
      await appendPostingBuild(
        store,
        "bounded",
        "value",
        "replacement",
        0,
        [{ term: "alpha", rowIds: [2n], tf: [1] }],
        "2026-01-01T00:10:00.000Z",
        "2026-01-01T00:40:00.000Z",
      );
      await appendPostingBuild(
        store,
        "bounded",
        "value",
        "replacement",
        1,
        [{ term: "omega", rowIds: [3n], tf: [1] }],
        "2026-01-01T00:10:00.000Z",
        "2026-01-01T00:40:00.000Z",
      );
      await finishPostingBuild(store, "bounded", "value", "replacement", {
        coversVersion: 7,
        chunkCount: 2,
        totalTokens: 2,
      });
      expect(
        await store.readFtsCandidates(
          "bounded",
          "value",
          [
            { term: "old", prefix: false },
            { lower: "alpha", lowerInclusive: true, upper: "omega", upperInclusive: true },
          ],
          7,
        ),
      ).toMatchObject({ rowIdsByTerm: [[], [2n, 3n]], coversVersion: 7 });
      store.close();
    });

    it("cannot publish a staged postings generation after its catalog index is dropped", async () => {
      const store = await implementation.create();
      await store.addTable({
        managed: false,
        id: "dropped-build",
        name: "dropped_build",
        columns: [{ id: "value", name: "value", type: "string", nullable: false }],
        ftsColumns: {
          value: {
            storage: "fts-chunks-v1",
            tokenizerVersion: 1,
            state: "building",
            buildFromVersion: -1,
          },
        },
        revision: 0,
        createdAt: "2026-01-01T00:00:00.000Z",
      });
      await beginPostingBuild(store, "dropped-build", "value", "loser");
      await appendPostingBuild(store, "dropped-build", "value", "loser", 0, [
        { term: "must-not-publish", rowIds: [1n], tf: [1] },
      ]);
      await store.updateTable("dropped-build", 0, { ftsColumns: null });
      await store
        .finishFtsBaseBuild({
          tableId: "dropped-build",
          columnId: "value",
          buildId: "loser",
          ownerId: postingBuildOwner("loser"),
          expiresAtCutoff: POSTING_BUILD_CREATED_AT,
          completedAt: POSTING_BUILD_CREATED_AT,
          coversVersion: 0,
          chunkCount: 1,
          totalTokens: 1,
        })
        .catch(() => undefined);
      expect(
        await store.readFtsCandidates(
          "dropped-build",
          "value",
          [{ term: "must-not-publish", prefix: false }],
          0,
        ),
      ).toMatchObject({ rowIdsByTerm: [[]], coversVersion: -1 });
      store.close();
    });

    it("invalidates stale scalar-index writers and cannot resurrect a dropped index", async () => {
      const store = await implementation.create();
      await store.addTable({
        managed: false,
        id: "scalar-table",
        name: "scalar_table",
        columns: [{ id: "value-id", name: "value", type: "number", nullable: false }],
        secondaryIndexes: {
          "index-id": {
            name: "by_value",
            columnId: "value-id",
            columnIds: ["value-id"],
            directions: ["asc"],
            termEncoding: "tuple-v1",
            storage: "postings-v1",
            storageColumnId: "scalar-storage-id",
            locator: "row-id",
            state: "ready",
            buildFromVersion: -1,
          },
        },
        createdAt: "2026-01-01T00:00:00.000Z",
        revision: 0,
      });
      await store.writeFtsBase("scalar-table", "scalar-storage-id", {
        coversVersion: -1,
        chunks: [[{ term: "one", rowIds: [1n], tf: [1] }]],
        totalTokens: 1,
      });
      const timestamp = "2026-01-01T00:00:00.000Z";
      const commit = async (
        transactionId: string,
        expectedVersion: number | null,
        withDelta: boolean,
      ) => {
        const blockId = `${transactionId}-block`;
        const segmentId = `${transactionId}-segment`;
        const staged = await stageTestArtifacts(store, {
          transactionId,
          snapshotVersion: expectedVersion,
          blocks: [{ id: blockId, bytes: Uint8Array.of(1) }],
          segments: [
            {
              id: segmentId,
              tableId: "scalar-table",
              transactionId,
              rowCount: 1,
              rowIdStart: 2n,
              rowIdEndExclusive: 3n,
              columnBlockIds: { "value-id": [blockId] },
              kind: "insert",
              createdAt: timestamp,
            },
          ],
        });
        return store.commitTransaction({
          transactionId,
          expectedTransactionRevision: staged.revision,
          expectedManifestVersion: expectedVersion,
          changedTableIds: ["scalar-table"],
          levelZeroSegmentLimits: [{ tableId: "scalar-table", limit: 4096 }],
          ...(withDelta
            ? {
                ftsChanges: [
                  {
                    tableId: "scalar-table",
                    columns: [
                      {
                        columnId: "scalar-storage-id",
                        postings: [{ term: "two", rowIds: [2n], tf: [1] }],
                        totalTokens: 1,
                      },
                    ],
                  },
                ],
              }
            : {}),
          committedAt: timestamp,
        });
      };
      const first = await commit("scalar-covered", null, true);
      expect((await store.getTable("scalar-table"))?.secondaryIndexes?.["index-id"]?.state).toBe(
        "ready",
      );
      await commit("scalar-stale", first.version, false);
      const invalid = await store.getTable("scalar-table");
      expect(invalid?.secondaryIndexes?.["index-id"]?.state).toBe("invalid");

      await store.updateTable("scalar-table", invalid?.revision ?? 0, {
        secondaryIndexes: null,
      });
      expect(
        await store.readFtsCandidates(
          "scalar-table",
          "scalar-storage-id",
          [{ term: "one", prefix: false }],
          10,
        ),
      ).toMatchObject({ rowIdsByTerm: [[]], deltaChunkCount: 0, coversVersion: -1 });
      store.close();
    });

    it("removes full-text state atomically when its column leaves the catalog", async () => {
      const store = await implementation.create();
      await store.addTable({
        managed: false,
        id: "drop-index-table",
        name: "drop_index",
        columns: [
          { id: "drop-index-title", name: "title", type: "string", nullable: false },
          { id: "drop-index-body", name: "body", type: "string", nullable: false },
        ],
        ftsColumns: {
          "drop-index-title": {
            storage: "fts-chunks-v1",
            tokenizerVersion: 1,
            state: "ready",
            buildFromVersion: 0,
          },
        },
        revision: 0,
        createdAt: "2026-01-01T00:00:00.000Z",
      });
      await store.writeFtsBase("drop-index-table", "drop-index-title", {
        coversVersion: 0,
        chunks: [[{ term: "alpha", rowIds: [1n], tf: [1] }]],
        totalTokens: 1,
      });
      await store.updateTable("drop-index-table", 0, {
        columns: [{ id: "drop-index-body", name: "body", type: "string", nullable: false }],
      });
      expect((await store.getTable("drop-index-table"))?.ftsColumns).toBeUndefined();
      expect(
        await store.readFtsCandidates(
          "drop-index-table",
          "drop-index-title",
          [{ term: "alpha", prefix: false }],
          1,
        ),
      ).toMatchObject({ rowIdsByTerm: [[]], deltaChunkCount: 0, coversVersion: -1 });
      store.close();
    });

    it("applies full-text commit deltas atomically and flips stale writers to invalid", async () => {
      const store = await implementation.create();
      await store.addTable({
        managed: false,
        id: "articles-id",
        name: "articles",
        columns: [{ id: "title-id", name: "title", type: "string", nullable: false }],
        ftsColumns: {
          "title-id": {
            storage: "fts-chunks-v1",
            tokenizerVersion: 1,
            state: "ready",
            buildFromVersion: -1,
          },
        },
        createdAt: "2026-01-01T00:00:00.000Z",
        revision: 0,
      });
      const timestamp = "2026-01-01T00:00:00.000Z";
      const commitSegment = async (
        ordinal: number,
        expectedVersion: number | null,
        withDelta: boolean,
      ) => {
        const transactionId = `fts-tx-${String(ordinal)}`;
        const blockId = `fts-block-${String(ordinal)}`;
        const segmentId = `fts-segment-${String(ordinal)}`;
        const staged = await stageTestArtifacts(store, {
          transactionId,
          snapshotVersion: expectedVersion,
          blocks: [{ id: blockId, bytes: Uint8Array.of(1) }],
          segments: [
            {
              id: segmentId,
              tableId: "articles-id",
              transactionId,
              rowCount: 1,
              rowIdStart: BigInt(ordinal),
              rowIdEndExclusive: BigInt(ordinal) + 1n,
              columnBlockIds: { "title-id": [blockId] },
              kind: "insert",
              createdAt: timestamp,
            },
          ],
        });
        return store.commitTransaction({
          transactionId,
          expectedTransactionRevision: staged.revision,
          expectedManifestVersion: expectedVersion,
          changedTableIds: ["articles-id"],
          levelZeroSegmentLimits: [{ tableId: "articles-id", limit: 4096 }],
          ...(withDelta
            ? {
                ftsChanges: [
                  {
                    tableId: "articles-id",
                    columns: [
                      {
                        columnId: "title-id",
                        postings: [{ term: "quick", rowIds: [BigInt(ordinal)], tf: [1] }],
                        totalTokens: 1,
                      },
                    ],
                  },
                ],
              }
            : {}),
          committedAt: timestamp,
        });
      };
      const first = await commitSegment(1, null, true);
      const merged = await store.readFtsCandidates(
        "articles-id",
        "title-id",
        [{ term: "quick", prefix: false }],
        first.version,
      );
      expect(merged.rowIdsByTerm).toEqual([[1n]]);
      expect(merged.deltaChunkCount).toBe(1);
      expect(merged.totalTokens).toBe(1);
      expect((await store.getTable("articles-id"))?.ftsColumns?.["title-id"]?.state).toBe("ready");
      // A commit that adds segments without a delta is a stale writer: the column flips to
      // invalid in the same publish, and the data commit itself succeeds.
      await commitSegment(2, first.version, false);
      expect((await store.getTable("articles-id"))?.ftsColumns?.["title-id"]?.state).toBe(
        "invalid",
      );
      store.close();
    });

    it("reserves row ids and auto-increment values while beginning a transaction", async () => {
      const store = await implementation.create();
      await addCounterFixtureTables(store);
      const result = await store.beginTransaction({
        record: activeTransaction("begin-reservations"),
        reserveRowIds: { tableId: "people", count: 4 },
        reserveAutoIncrement: { tableId: "people", columnId: "id", count: 2, atLeast: 10n },
      });
      expect(result.rowIds).toEqual({ start: 1n, endExclusive: 5n });
      expect(result.autoIncrementValues).toEqual({ start: 10n, endExclusive: 12n });
      // The bump landed atomically with the begin: later reservations continue past it.
      expect(await store.reserveAutoIncrement("people", "id", 1)).toEqual({
        start: 12n,
        endExclusive: 13n,
      });
      store.close();
    });

    it("creates and renews persistent leases", async () => {
      const store = await implementation.create();
      await store.createLease({
        id: "reader-1",
        kind: "reader",
        manifestVersion: null,
        ownerId: "tab-1",
        createdAt: "2026-01-01T00:00:00.000Z",
        expiresAt: "2026-01-01T00:01:00.000Z",
        revision: 0,
      });
      const renewed = await store.renewLease({
        id: "reader-1",
        expectedRevision: 0,
        expiresAtCutoff: "2026-01-01T00:00:00.000Z",
        expiresAt: "2026-01-01T00:02:00.000Z",
      });
      expect(renewed.revision).toBe(1);
      await expect(
        store.renewLease({
          id: "reader-1",
          expectedRevision: 0,
          expiresAtCutoff: "2026-01-01T00:00:00.000Z",
          expiresAt: renewed.expiresAt,
        }),
      ).rejects.toThrow("expected revision 0");
      await store.removeLease({ id: "reader-1", ownerId: "tab-1" });
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
        rewritePlan: { kind: "copy-v1" },
        outputCursor: null,
        memoryBudgetBytes: 0,
        minimumMemoryBytes: 0,
        level0SourceStoredBytes: 0,
        anchorSourceStoredBytes: 0,
        peakWorkingBytes: 0,
        outputLogicalBytes: 0,
        targetLevel: 1,
        state: "planned",
        transactionId: null,
        outputSegmentId: null,
        publishedVersion: null,
        revision: 0,
        createdAt: "2026-01-01T00:00:01.000Z",
        updatedAt: "2026-01-01T00:00:01.000Z",
      };
      const accountJob: CompactionJobRecord = {
        ...created,
        id: "job-a",
        tableId: "accounts",
        sourceSegmentIds: ["account-segment"],
        sourceBlockIds: ["account-block"],
        cursor: { sourceSegmentIndex: 0, sourceBlockIndex: 0 },
        createdAt: "2026-01-01T00:00:00.000Z",
        updatedAt: "2026-01-01T00:00:00.000Z",
      };
      await prepareCompactionSourceFixtures(store, [created, accountJob]);
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

      await store.createCompactionJob(accountJob);
      expect((await store.listCompactionJobs()).map((job) => job.id)).toEqual(["job-a", "job-b"]);
      expect((await store.listCompactionJobs("events")).map((job) => job.id)).toEqual(["job-b"]);
      const firstPage = await store.listCompactionJobPage(null, 1);
      expect(firstPage.records.map((job) => job.id)).toEqual(["job-a"]);
      expect(firstPage.nextCursor).toBe("job-a");
      expect(
        (await store.listCompactionJobPage(firstPage.nextCursor, 1)).records.map((job) => job.id),
      ).toEqual(["job-b"]);

      const outputBlockIds = ["output-b", "output-a", "output-b"];
      const cursor = { sourceSegmentIndex: 1, sourceBlockIndex: 8 };
      await stageTestArtifacts(store, {
        transactionId: "transaction-9",
        snapshotVersion: 7,
        blocks: [
          { id: "output-a", bytes: Uint8Array.of(1) },
          { id: "output-b", bytes: Uint8Array.of(2) },
        ],
        segments: [
          {
            id: "output-segment",
            tableId: "events",
            transactionId: "transaction-9",
            rowCount: 1,
            rowIdStart: 1n,
            rowIdEndExclusive: 2n,
            columnBlockIds: { value: ["output-a", "output-b"] },
            level: 1,
            logicalOrder: 7,
            createdAt: "2026-01-01T00:00:02.000Z",
          },
        ],
      });
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

      expect(await store.getCompactionJob("job-b")).toMatchObject({ state: "ready" });
      store.close();
    });

    it("persists immutable source-level compaction byte accounting", async () => {
      const store = await implementation.create();
      const accounted: CompactionJobRecord = {
        ...rechunkCompactionJob("source-level-byte-accounting"),
        level0SourceStoredBytes: 240,
        anchorSourceStoredBytes: 120,
      };
      const missingLevelZero = rechunkCompactionJob("missing-level-zero-byte-accounting");
      delete (missingLevelZero as unknown as { level0SourceStoredBytes?: number })
        .level0SourceStoredBytes;
      const missingAnchor = rechunkCompactionJob("missing-anchor-byte-accounting");
      delete (missingAnchor as unknown as { anchorSourceStoredBytes?: number })
        .anchorSourceStoredBytes;
      const requiredCheckpointFields = [
        "rewritePlan",
        "outputCursor",
        "memoryBudgetBytes",
        "minimumMemoryBytes",
        "peakWorkingBytes",
        "outputLogicalBytes",
      ] as const;
      const missingRequired = requiredCheckpointFields.map((field) => {
        const record = rechunkCompactionJob(`missing-${field}`);
        Reflect.deleteProperty(record, field);
        return record;
      });
      await prepareCompactionSourceFixtures(store, [
        accounted,
        missingLevelZero,
        missingAnchor,
        ...missingRequired,
      ]);
      await store.createCompactionJob(accounted);
      expect(await store.getCompactionJob(accounted.id)).toMatchObject({
        sourceStoredBytes: 360,
        level0SourceStoredBytes: 240,
        anchorSourceStoredBytes: 120,
      });

      const invalidRecords: Array<{ record: CompactionJobRecord; message: string }> = [
        {
          record: {
            ...missingLevelZero,
          },
          message: "level-zero source stored bytes",
        },
        {
          record: {
            ...missingAnchor,
          },
          message: "anchor source stored bytes",
        },
        {
          record: {
            ...rechunkCompactionJob("mismatched-source-level-byte-accounting"),
            level0SourceStoredBytes: 240,
            anchorSourceStoredBytes: 119,
          },
          message: "must equal source stored bytes",
        },
      ];
      for (const invalid of invalidRecords) {
        await expect(store.createCompactionJob(invalid.record)).rejects.toThrow(invalid.message);
        expect(await store.getCompactionJob(invalid.record.id)).toBeUndefined();
      }
      for (const invalid of missingRequired) {
        await expect(store.createCompactionJob(invalid)).rejects.toThrow();
        expect(await store.getCompactionJob(invalid.id)).toBeUndefined();
      }

      const immutableBypassUpdates = [
        [
          {
            level0SourceStoredBytes: 1,
            updatedAt: "2026-01-01T00:00:01.000Z",
          },
          "level0SourceStoredBytes",
        ],
        [
          {
            anchorSourceStoredBytes: 1,
            updatedAt: "2026-01-01T00:00:01.000Z",
          },
          "anchorSourceStoredBytes",
        ],
      ] as const;
      for (const [update, field] of immutableBypassUpdates) {
        await expect(store.updateCompactionJob(accounted.id, 0, update)).rejects.toThrow(
          `Compaction ${field} is immutable`,
        );
      }
      expect((await store.getCompactionJob(accounted.id))?.revision).toBe(0);
      await store.cancelCompactionJob(accounted.id, 0, "2026-01-01T00:00:02.000Z");
      store.close();
    });

    it("persists and enforces the append-row-range L2 output byte budget", async () => {
      const store = await implementation.create();
      const bounded: CompactionJobRecord = {
        ...level2CompactionJob("bounded-level-two"),
        maximumOutputStoredBytes: 720,
        plannedOutputStoredBytesUpperBound: 720,
      };
      const overflow = {
        ...bounded,
        id: "overflow-level-two",
        maximumOutputStoredBytes: 719,
        plannedOutputStoredBytesUpperBound: 719,
      };
      await prepareCompactionSourceFixtures(store, [bounded, overflow]);
      await store.createCompactionJob(bounded);
      expect(await store.getCompactionJob(bounded.id)).toMatchObject({
        outputPartitionOrdinal: 3,
        maxWriteAmplification: 2,
        maximumOutputStoredBytes: 720,
        plannedOutputStoredBytesUpperBound: 720,
      });

      await stageTestArtifacts(store, {
        transactionId: "bounded-level-two-transaction",
        snapshotVersion: bounded.sourceManifestVersion,
        blocks: [{ id: "bounded-level-two-output-0", bytes: Uint8Array.of(1) }],
      });
      const boundary = await store.updateCompactionJob(bounded.id, 0, {
        state: "running",
        transactionId: "bounded-level-two-transaction",
        outputBlockIds: ["bounded-level-two-output-0"],
        outputCursor: { outputIndex: 0, columnIndex: 1, rowStart: 0 },
        outputStoredBytes: 720,
        outputLogicalBytes: 1,
        updatedAt: "2026-01-01T00:00:01.000Z",
      });
      expect(boundary).toMatchObject({ outputStoredBytes: 720, revision: 1 });

      await store.cancelCompactionJob(bounded.id, 1, "2026-01-01T00:00:02.000Z");
      await store.createCompactionJob(overflow);
      await expect(
        store.updateCompactionJob(overflow.id, 0, {
          state: "running",
          transactionId: "overflow-level-two-transaction",
          outputBlockIds: ["overflow-level-two-output-0"],
          outputCursor: { outputIndex: 0, columnIndex: 1, rowStart: 0 },
          outputStoredBytes: 720,
          outputLogicalBytes: 1,
          updatedAt: "2026-01-01T00:00:01.000Z",
        }),
      ).rejects.toThrow("exceed their planned upper bound");
      expect((await store.getCompactionJob(overflow.id))?.revision).toBe(0);
      store.close();
    });

    it("rejects inconsistent append-row-range L2 policy fields", async () => {
      const store = await implementation.create();
      const valid = level2CompactionJob("valid-level-two-policy");
      const invalidRecords: Array<{ record: CompactionJobRecord; message: string }> = [
        {
          record: {
            ...rechunkCompactionJob("partial-level-two-policy"),
            outputPartitionOrdinal: 0,
          },
          message: "must be present together",
        },
        {
          record: { ...valid, id: "wrong-level-two-target", targetLevel: 1 },
          message: "must target level two",
        },
        {
          record: {
            ...valid,
            id: "anchored-level-two-policy",
            level0SourceStoredBytes: 359,
            anchorSourceStoredBytes: 1,
          },
          message: "requires only level-zero source bytes",
        },
        {
          record: { ...valid, id: "negative-output-partition", outputPartitionOrdinal: -1 },
          message: "partition ordinal must be a non-negative whole number",
        },
        {
          record: { ...valid, id: "zero-amplification", maxWriteAmplification: 0 },
          message: "must be a positive finite number",
        },
        {
          record: {
            ...valid,
            id: "infinite-amplification",
            maxWriteAmplification: Number.POSITIVE_INFINITY,
          },
          message: "must be a positive finite number",
        },
        {
          record: {
            ...valid,
            id: "unsafe-amplification-product",
            maxWriteAmplification: Number.MAX_SAFE_INTEGER,
          },
          message: "product exceeds the safe range",
        },
        {
          record: {
            ...valid,
            id: "oversized-output-ceiling",
            maxWriteAmplification: 1,
            maximumOutputStoredBytes: 361,
            plannedOutputStoredBytesUpperBound: 360,
          },
          message: "ceiling exceeds its amplification limit",
        },
        {
          record: {
            ...valid,
            id: "oversized-output-plan",
            maximumOutputStoredBytes: 500,
            plannedOutputStoredBytesUpperBound: 501,
          },
          message: "planned output exceeds its stored byte ceiling",
        },
      ];
      const mergePolicy = mergeCompactionJob("merge-level-two-policy-missing-accounting");
      delete (mergePolicy as unknown as { level0SourceStoredBytes?: number })
        .level0SourceStoredBytes;
      delete (mergePolicy as unknown as { anchorSourceStoredBytes?: number })
        .anchorSourceStoredBytes;
      invalidRecords.push({
        record: {
          ...mergePolicy,
          outputPartitionOrdinal: 0,
          maxWriteAmplification: 2,
          maximumOutputStoredBytes: 90,
          plannedOutputStoredBytesUpperBound: 80,
          targetLevel: 2,
        },
        message: "level-zero source stored bytes",
      });
      invalidRecords.push({
        record: {
          ...level2CompactionJob("prior-attempts-without-policy"),
          outputPartitionOrdinal: undefined,
          maxWriteAmplification: undefined,
          maximumOutputStoredBytes: undefined,
          plannedOutputStoredBytesUpperBound: undefined,
          priorAttemptOutputStoredBytes: 10,
        } as unknown as CompactionJobRecord,
        message: "requires the L2 compaction policy fields",
      });
      invalidRecords.push({
        record: {
          ...level2CompactionJob("prior-attempts-exceed-ceiling"),
          maxWriteAmplification: 1,
          maximumOutputStoredBytes: 355,
          plannedOutputStoredBytesUpperBound: 300,
          priorAttemptOutputStoredBytes: 6,
        },
        message: "ceiling exceeds its amplification limit",
      });

      for (const invalid of invalidRecords) {
        await expect(store.createCompactionJob(invalid.record)).rejects.toThrow(invalid.message);
        expect(await store.getCompactionJob(invalid.record.id)).toBeUndefined();
      }

      // A keyed merge promotion persists the policy fields (with anchor bytes) and the shared
      // lifetime accounting.
      const keyedMerge = mergeCompactionJob("keyed-merge-level-two-policy");
      const keyedMergeJob: CompactionJobRecord = {
        ...keyedMerge,
        level0SourceStoredBytes: keyedMerge.sourceStoredBytes - 10,
        anchorSourceStoredBytes: 10,
        outputPartitionOrdinal: 1,
        maxWriteAmplification: 2,
        maximumOutputStoredBytes: 85,
        plannedOutputStoredBytesUpperBound: 80,
        priorAttemptOutputStoredBytes: 5,
        targetLevel: 2,
      };
      const immutable = level2CompactionJob("immutable-level-two-policy");
      const levelOne = rechunkCompactionJob("level-one-without-level-two-policy");
      await prepareCompactionSourceFixtures(store, [keyedMergeJob, immutable, levelOne]);
      await store.createCompactionJob(keyedMergeJob);
      expect(await store.getCompactionJob(keyedMergeJob.id)).toMatchObject({
        outputPartitionOrdinal: 1,
        priorAttemptOutputStoredBytes: 5,
        anchorSourceStoredBytes: 10,
      });

      await store.cancelCompactionJob(keyedMergeJob.id, 0, "2026-01-01T00:00:02.000Z");
      await store.createCompactionJob(immutable);
      for (const [field, value] of [
        ["outputPartitionOrdinal", 4],
        ["maxWriteAmplification", 3],
        ["maximumOutputStoredBytes", 700],
        ["plannedOutputStoredBytesUpperBound", 500],
        ["priorAttemptOutputStoredBytes", 25],
      ] as const) {
        const update = {
          [field]: value,
          updatedAt: "2026-01-01T00:00:01.000Z",
        } as unknown as CompactionJobRecordUpdate;
        await expect(store.updateCompactionJob(immutable.id, 0, update)).rejects.toThrow(
          `Compaction ${field} is immutable`,
        );
      }
      expect((await store.getCompactionJob(immutable.id))?.revision).toBe(0);

      await store.cancelCompactionJob(immutable.id, 0, "2026-01-01T00:00:03.000Z");
      await store.createCompactionJob(levelOne);
      const persistedLevelOne = await store.getCompactionJob(levelOne.id);
      expect(persistedLevelOne).not.toHaveProperty("outputPartitionOrdinal");
      expect(persistedLevelOne).not.toHaveProperty("maxWriteAmplification");
      expect(persistedLevelOne).not.toHaveProperty("maximumOutputStoredBytes");
      expect(persistedLevelOne).not.toHaveProperty("plannedOutputStoredBytesUpperBound");
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
        revision: commit.expectedTransactionRevision + 1,
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
        revision: commit.expectedTransactionRevision + 1,
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

      for (const state of ["aborted"] as const) {
        const terminalJob: CompactionJobRecord = {
          ...job,
          id: `${state}-terminal-job`,
          transactionId: job.transactionId,
          state,
          publishedVersion: null,
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
      const linked = await store.getTransaction(job.transactionId ?? "");
      if (linked === undefined) throw new Error("Missing linked transaction fixture");
      const abortedTransaction = await store.updateTransaction(linked.id, linked.revision, {
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
      await expect(store.createCompactionJob(missingTransactionJob)).rejects.toThrow(
        /missing transaction|no transaction/,
      );
      expect(await store.getTransaction("missing-transaction")).toBeUndefined();
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
      await prepareCompactionSourceFixtures(store, [created]);
      await store.createCompactionJob(created);
      const createdPlan = created.rewritePlan;
      if (createdPlan.kind !== "rechunk-v1") throw new Error("Expected a rechunk plan");
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

      await stageTestArtifacts(store, {
        transactionId: "rechunk-transaction",
        snapshotVersion: created.sourceManifestVersion,
        blocks: ["output-0-id", "output-0-name", "output-1-id", "output-1-name"].map((id) => ({
          id,
          bytes: Uint8Array.of(1),
        })),
        segments: [
          {
            id: created.outputSegmentId ?? "",
            tableId: created.tableId,
            transactionId: "rechunk-transaction",
            rowCount: 4,
            rowIdStart: 10n,
            rowIdEndExclusive: 14n,
            columnBlockIds: {
              value: ["output-0-id", "output-0-name", "output-1-id", "output-1-name"],
            },
            level: 1,
            logicalOrder: created.sourceManifestVersion,
            createdAt: "2026-01-01T00:00:01.000Z",
          },
        ],
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
      if (badRange.rewritePlan.kind !== "rechunk-v1") throw new Error("Expected rechunk plan");
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

      const oversizedOutput = rechunkCompactionJob("oversized-output");
      if (oversizedOutput.rewritePlan.kind !== "rechunk-v1") {
        throw new Error("Expected rechunk plan");
      }
      await expect(
        store.createCompactionJob({
          ...oversizedOutput,
          rewritePlan: {
            ...oversizedOutput.rewritePlan,
            outputs: [{ rowStart: 0, rowCount: MAX_BLOCK_ROW_COUNT + 1 }],
          },
        }),
      ).rejects.toThrow("block format row limit");

      const checkpoint = rechunkCompactionJob("bad-checkpoint");
      await prepareCompactionSourceFixtures(store, [checkpoint]);
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

    it("persists immutable merge plans and advances output-driven checkpoints", async () => {
      const store = await implementation.create();
      const created = mergeCompactionJob();
      await prepareCompactionSourceFixtures(store, [created]);
      await store.createCompactionJob(created);
      const createdPlan = created.rewritePlan;
      if (createdPlan.kind !== "merge-v1") throw new Error("Expected a merge plan");
      (createdPlan.rowIdSpans[0] as { rowIdStart: bigint }).rowIdStart = 999n;
      (createdPlan.sourceSegments[0]?.columns[0]?.sourceBlocks[0] as { blockId: string }).blockId =
        "mutated";

      const persisted = await store.getCompactionJob(created.id);
      expect(persisted).toMatchObject({
        sourceSegmentIds: ["base-segment", "delete-segment", "upsert-segment"],
        sourceBlockIds: ["base-key", "base-value", "delete-key", "upsert-key", "upsert-value"],
        rewritePlan: {
          kind: "merge-v1",
          totalRows: 2,
          rowIdStart: 3n,
          rowIdEndExclusive: 11n,
          rowIdSpans: [
            { rowStart: 0, rowCount: 1, rowIdStart: 10n },
            { rowStart: 1, rowCount: 1, rowIdStart: 3n },
          ],
        },
        outputCursor: { outputIndex: 0, columnIndex: 0, rowStart: 0 },
      });

      await stageTestArtifacts(store, {
        transactionId: "merge-transaction",
        snapshotVersion: created.sourceManifestVersion,
        blocks: ["merge-output-id", "merge-output-value"].map((id) => ({
          id,
          bytes: Uint8Array.of(1),
        })),
        segments: [
          {
            id: created.outputSegmentId ?? "",
            tableId: created.tableId,
            transactionId: "merge-transaction",
            rowCount: 2,
            rowIdStart: 3n,
            rowIdEndExclusive: 5n,
            columnBlockIds: { value: ["merge-output-id", "merge-output-value"] },
            level: 1,
            logicalOrder: created.sourceManifestVersion,
            createdAt: "2026-01-01T00:00:01.000Z",
          },
        ],
      });

      const first = await store.updateCompactionJob(created.id, 0, {
        outputBlockIds: ["merge-output-id"],
        outputCursor: { outputIndex: 0, columnIndex: 1, rowStart: 0 },
        outputStoredBytes: 70,
        outputLogicalBytes: 80,
        peakWorkingBytes: 600,
        state: "running",
        transactionId: "merge-transaction",
        updatedAt: "2026-01-01T00:00:01.000Z",
      });
      expect(first).toMatchObject({
        processedRows: 0,
        outputBlockIds: ["merge-output-id"],
        outputCursor: { outputIndex: 0, columnIndex: 1, rowStart: 0 },
        revision: 1,
      });

      const ready = await store.updateCompactionJob(created.id, first.revision, {
        outputBlockIds: ["merge-output-id", "merge-output-value"],
        outputCursor: { outputIndex: 1, columnIndex: 0, rowStart: 2 },
        processedRows: 2,
        outputStoredBytes: 140,
        outputLogicalBytes: 160,
        peakWorkingBytes: 700,
        state: "ready",
        updatedAt: "2026-01-01T00:00:02.000Z",
      });
      expect(ready).toMatchObject({ state: "ready", processedRows: 2, revision: 2 });

      await expect(
        store.updateCompactionJob(created.id, ready.revision, {
          outputSegmentId: "replacement-output-segment",
          updatedAt: "2026-01-01T00:00:03.000Z",
        }),
      ).rejects.toThrow("immutable");
      expect((await store.getCompactionJob(created.id))?.revision).toBe(ready.revision);
      store.close();
    });

    it("rejects malformed merge fingerprints, source maps, and row-ID spans", async () => {
      const store = await implementation.create();

      const mismatchedSegments = mergeCompactionJob("merge-mismatched-segments");
      mismatchedSegments.sourceSegmentIds = ["delete-segment", "base-segment", "upsert-segment"];
      await expect(store.createCompactionJob(mismatchedSegments)).rejects.toThrow(
        "preserve every selected source segment",
      );

      const duplicateBlock = mergeCompactionJob("merge-duplicate-block");
      if (duplicateBlock.rewritePlan.kind !== "merge-v1") throw new Error("Expected merge plan");
      (
        duplicateBlock.rewritePlan.sourceSegments[2]?.columns[0]?.sourceBlocks[0] as {
          blockId: string;
        }
      ).blockId = "base-key";
      await expect(store.createCompactionJob(duplicateBlock)).rejects.toThrow(
        "source block can only appear once",
      );

      const unknownRange = mergeCompactionJob("merge-unknown-range");
      if (unknownRange.rewritePlan.kind !== "merge-v1") throw new Error("Expected merge plan");
      (
        unknownRange.rewritePlan.columns[0]?.sourceRanges[0] as { sourceBlockId: string }
      ).sourceBlockId = "unknown";
      await expect(store.createCompactionJob(unknownRange)).rejects.toThrow("unknown source block");

      const overlappingIds = mergeCompactionJob("merge-overlapping-row-ids");
      if (overlappingIds.rewritePlan.kind !== "merge-v1") throw new Error("Expected merge plan");
      (overlappingIds.rewritePlan.rowIdSpans[1] as { rowIdStart: bigint }).rowIdStart = 10n;
      await expect(store.createCompactionJob(overlappingIds)).rejects.toThrow(
        "overlapping row IDs",
      );

      const gappedRanges = mergeCompactionJob("merge-gapped-ranges");
      if (gappedRanges.rewritePlan.kind !== "merge-v1") throw new Error("Expected merge plan");
      (
        gappedRanges.rewritePlan.columns[0]?.sourceRanges[1] as { outputRowStart: number }
      ).outputRowStart = 2;
      await expect(store.createCompactionJob(gappedRanges)).rejects.toThrow(
        "cover output rows contiguously",
      );

      const invalidDelete = mergeCompactionJob("merge-invalid-delete");
      if (invalidDelete.rewritePlan.kind !== "merge-v1") throw new Error("Expected merge plan");
      (invalidDelete.rewritePlan.sourceSegments[1] as { kind: "update" }).kind = "update";
      await expect(store.createCompactionJob(invalidDelete)).rejects.toThrow(
        "update segment requires",
      );

      expect(await store.listCompactionJobs()).toEqual([]);
      store.close();
    });

    it("publishes an empty merge without fabricating an output segment", async () => {
      const store = await implementation.create();
      const nonEmpty = mergeCompactionJob("empty-merge");
      if (nonEmpty.rewritePlan.kind !== "merge-v1") throw new Error("Expected merge plan");
      const emptyPlan: MergeCompactionRewritePlan = {
        ...nonEmpty.rewritePlan,
        totalRows: 0,
        rowIdStart: 0n,
        rowIdEndExclusive: 0n,
        rowIdSpans: [],
        columns: nonEmpty.rewritePlan.columns.map((column) => ({ ...column, sourceRanges: [] })),
        outputs: [],
      };
      const empty: CompactionJobRecord = {
        ...nonEmpty,
        rewritePlan: emptyPlan,
        outputSegmentId: null,
        outputCursor: { outputIndex: 0, columnIndex: 0, rowStart: 0 },
        minimumMemoryBytes: 0,
      };
      await prepareCompactionSourceFixtures(store, [empty]);
      await store.createCompactionJob(empty);

      await store.createTransaction({
        ...activeTransaction("empty-merge-transaction"),
        snapshotVersion: empty.sourceManifestVersion,
      });
      await publishManifest(store, {
        expectedVersion: empty.sourceManifestVersion,
        blockIds: empty.sourceBlockIds,
        createdAt: "2026-01-01T00:00:02.500Z",
      });

      const running = await store.updateCompactionJob(empty.id, 0, {
        state: "running",
        transactionId: "empty-merge-transaction",
        updatedAt: "2026-01-01T00:00:01.000Z",
      });
      const ready = await store.updateCompactionJob(empty.id, running.revision, {
        state: "ready",
        updatedAt: "2026-01-01T00:00:02.000Z",
      });
      const published = await store.updateCompactionJob(empty.id, ready.revision, {
        state: "published",
        publishedVersion: 10,
        updatedAt: "2026-01-01T00:00:03.000Z",
      });
      expect(published).toMatchObject({
        state: "published",
        outputSegmentId: null,
        outputBlockIds: [],
        processedRows: 0,
        publishedVersion: 10,
      });

      const invalidEmpty: CompactionJobRecord = {
        ...mergeCompactionJob("invalid-empty-merge"),
        rewritePlan: emptyPlan,
        minimumMemoryBytes: 0,
      };
      await expect(store.createCompactionJob(invalidEmpty)).rejects.toThrow(
        "cannot have an output segment",
      );

      const invalidNonEmpty = mergeCompactionJob("invalid-nonempty-merge");
      invalidNonEmpty.outputSegmentId = null;
      await expect(store.createCompactionJob(invalidNonEmpty)).rejects.toThrow(
        "requires an output segment ID",
      );
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
          rewritePlan: { kind: "copy-v1" },
          outputCursor: null,
          memoryBudgetBytes: 0,
          minimumMemoryBytes: 0,
          level0SourceStoredBytes: 0,
          anchorSourceStoredBytes: 0,
          peakWorkingBytes: 0,
          outputLogicalBytes: 0,
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
        rewritePlan: { kind: "copy-v1" },
        outputCursor: null,
        memoryBudgetBytes: 0,
        minimumMemoryBytes: 0,
        level0SourceStoredBytes: 0,
        anchorSourceStoredBytes: 0,
        peakWorkingBytes: 0,
        outputLogicalBytes: 0,
        targetLevel: 1,
        state: "planned",
        transactionId: null,
        outputSegmentId: "output-segment",
        publishedVersion: null,
        revision: 0,
        createdAt: "2026-01-01T00:00:00.000Z",
        updatedAt: "2026-01-01T00:00:00.000Z",
      };

      await prepareCompactionSourceFixtures(store, [planned]);
      await stageTestArtifacts(store, {
        transactionId: "compaction-transaction",
        snapshotVersion: 0,
        blocks: [{ id: "output-block", bytes: Uint8Array.of(2) }],
        segments: [
          {
            id: "output-segment",
            tableId: "events",
            transactionId: "compaction-transaction",
            rowCount: 1,
            rowIdStart: 1n,
            rowIdEndExclusive: 2n,
            columnBlockIds: { value: ["output-block"] },
            level: 1,
            logicalOrder: 0,
            createdAt: "2026-01-01T00:00:00.000Z",
          },
        ],
      });
      await publishManifest(store, {
        expectedVersion: 0,
        blockIds: planned.sourceBlockIds,
        createdAt: "2026-01-01T00:00:00.500Z",
      });

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
      const contended: CompactionJobRecord = {
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
        rewritePlan: { kind: "copy-v1" },
        outputCursor: null,
        memoryBudgetBytes: 0,
        minimumMemoryBytes: 0,
        level0SourceStoredBytes: 0,
        anchorSourceStoredBytes: 0,
        peakWorkingBytes: 0,
        outputLogicalBytes: 0,
        targetLevel: 1,
        state: "planned",
        transactionId: null,
        outputSegmentId: null,
        publishedVersion: null,
        revision: 0,
        createdAt: "2026-01-01T00:00:00.000Z",
        updatedAt: "2026-01-01T00:00:00.000Z",
      };
      await prepareCompactionSourceFixtures(store, [contended]);
      await store.createCompactionJob(contended);
      await store.createTransaction(activeTransaction("transaction-a"));
      await store.createTransaction(activeTransaction("transaction-b"));
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
        prunedAt: "2026-01-01T00:03:00.000Z",
      });
      // Historical membership is retained in interval provenance after the bounded summary
      // tombstone is marked pruned. Later GC passes must still be able to continue enumerating it.
      expect(await readManifestBlockIds(store, 0)).toEqual([`${prefix}/old-block`]);
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
      expect(
        await store.getBlock(
          `${prefix}/supersede-job/output/segment/000000/column/000000/part/000000`,
        ),
      ).toEqual(Uint8Array.of(4, 5));
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
        createdAt: "2026-01-01T00:00:00.000Z",
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
        createdAt: "2026-01-01T00:00:00.000Z",
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
        store.renewLease({
          id: `${prefix}/lease`,
          expectedRevision: 0,
          expiresAtCutoff: "2026-01-01T00:10:00.000Z",
          expiresAt: "2026-01-01T01:00:00.000Z",
        }),
      ).rejects.toThrow(/expired/);
      await expect(
        store.createLease({
          id: `${prefix}/late-lease`,
          kind: "reader",
          manifestVersion: 0,
          ownerId: "late-reader",
          createdAt: "2026-01-01T00:10:00.000Z",
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
      expect(await store.getBlock("unproven-orphan")).toBeUndefined();
      expect(await store.getGarbageCollectionJob("unsafe-gc")).toBeUndefined();
      store.close();
    });

    it("does not re-reject an earlier page's candidate reclaimed by concurrent maintenance", async () => {
      const store = await implementation.create();
      await store.addTable({
        managed: false,
        id: "mid-plan-table",
        name: "mid_plan_table",
        columns: [{ id: "value", name: "value", type: "number", nullable: false }],
        revision: 0,
        createdAt: "2026-01-01T00:00:00.000Z",
      });
      const staged = await stageTestArtifacts(store, {
        transactionId: "mid-plan-writer",
        blocks: [{ id: "mid-plan-block", bytes: Uint8Array.of(1) }],
        segments: [
          {
            id: "mid-plan-segment",
            tableId: "mid-plan-table",
            transactionId: "mid-plan-writer",
            rowCount: 1,
            rowIdStart: 1n,
            rowIdEndExclusive: 2n,
            columnBlockIds: { value: ["mid-plan-block"] },
            createdAt: "2026-01-01T00:00:00.000Z",
          },
        ],
      });
      await store.commitTransaction({
        transactionId: staged.id,
        expectedTransactionRevision: staged.revision,
        expectedManifestVersion: null,
        removedBlockIds: [],
        levelZeroSegmentLimits: [{ tableId: "mid-plan-table", limit: 1 }],
        committedAt: "2026-01-01T00:00:00.000Z",
      });

      const discovery = {
        phase: "segments" as const,
        currentManifestVersion: (await store.getCurrentManifest())?.version ?? null,
        retainAboveVersion: 0,
        retainAfter: 0,
        maxPlanningItems: 8,
        manifestCursor: null,
        segmentCursor: null,
        transactionCursor: null,
        compactionCursor: null,
        visitedRecords: 0,
      };

      // Page 1: nominate a segment that currently exists. This is the normal, valid case.
      const job = await store.createGarbageCollectionJob({
        id: "mid-plan-gc",
        candidateManifestVersions: [],
        candidateSegmentIds: ["mid-plan-segment"],
        candidateBlockIds: [],
        leaseCutoff: "2026-01-01T00:10:00.000Z",
        createdAt: "2026-01-01T00:01:00.000Z",
        discovery,
      });
      expect(job.candidateSegmentIds).toEqual(["mid-plan-segment"]);

      // Concurrent maintenance (here, dropping the owning table) reclaims that same segment
      // before planning has finished paging through the rest of the store.
      await store.dropTable({
        tableId: "mid-plan-table",
        expectedTableRevision: 0,
        expectedManifestVersion: 0,
        expectedCatalogEpoch: (await store.getCatalogProbe()).catalogEpoch,
        committedAt: "2026-01-01T00:02:00.000Z",
      });
      expect(await store.getSegment("mid-plan-segment")).toBeUndefined();

      // Page 2 only appends new, still-valid candidates. It must not be wedged by the earlier
      // page's candidate having since lost its provenance through legitimate reclamation.
      const advanced = await store.updateGarbageCollectionPlanning({
        jobId: job.id,
        expectedRevision: job.revision,
        candidateManifestVersions: [],
        discovery: { ...discovery, phase: "complete", visitedRecords: 1 },
        updatedAt: "2026-01-01T00:03:00.000Z",
      });
      expect(advanced.candidateSegmentIds).toEqual(["mid-plan-segment"]);

      const step = await store.runGarbageCollectionStep({
        jobId: advanced.id,
        expectedRevision: advanced.revision,
        maxItems: 10,
        updatedAt: "2026-01-01T00:04:00.000Z",
      });
      expect(step.missingSegmentIds).toEqual(["mid-plan-segment"]);
      expect(step.job.state).toBe("completed");
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
          createdAt: "2026-01-01T00:00:00.000Z",
          expiresAt: "not-a-date",
          revision: 0,
        }),
      ).rejects.toThrow(/valid|canonical|timestamps/);
      await store.createLease({
        id: "expiry-cas-lease",
        kind: "reader",
        manifestVersion: null,
        ownerId: "reader",
        createdAt: "2026-01-01T00:00:00.000Z",
        expiresAt: "2026-01-01T00:05:00.000Z",
        revision: 0,
      });
      await expect(
        store.renewLease({
          id: "expiry-cas-lease",
          expectedRevision: 0,
          expiresAtCutoff: "2026-01-01T00:00:00.000Z",
          expiresAt: "not-a-date",
        }),
      ).rejects.toThrow(/valid|canonical|timestamps/);
      expect((await store.getLease("expiry-cas-lease"))?.revision).toBe(0);
      const renewed = await store.renewLease({
        id: "expiry-cas-lease",
        expectedRevision: 0,
        expiresAtCutoff: "2026-01-01T00:00:00.000Z",
        expiresAt: "2026-01-01T00:20:00.000Z",
      });
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

    it("does not turn a wide segment dependency set into a permanent garbage root", async () => {
      const store = await implementation.create();
      const segmentId = "wide-orphan-segment";
      await store.addTable({
        managed: false,
        id: "events",
        name: "events",
        columns: [{ id: "value", name: "value", type: "number", nullable: false }],
        revision: 0,
        createdAt: "2026-01-01T00:00:00.000Z",
      });
      const blocks = Array.from({ length: 4_096 }, (_, index) => ({
        id: `gone-${String(index)}`,
        bytes: Uint8Array.of(1),
      }));
      const staged = await stageTestArtifacts(store, {
        transactionId: "finished-owner",
        blocks,
        segments: [
          {
            id: segmentId,
            tableId: "events",
            transactionId: "finished-owner",
            rowCount: 1,
            rowIdStart: 1n,
            rowIdEndExclusive: 2n,
            columnBlockIds: { value: blocks.map(({ id }) => id) },
            createdAt: "2026-01-01T00:00:00.000Z",
          },
        ],
      });
      await store.updateTransaction(staged.id, staged.revision, {
        status: "aborted",
        updatedAt: "2026-01-01T00:01:00.000Z",
      });
      const job = await store.createGarbageCollectionJob({
        id: "wide-orphan-gc",
        candidateManifestVersions: [],
        candidateSegmentIds: [segmentId],
        candidateBlockIds: [],
        leaseCutoff: "2026-01-01T00:10:00.000Z",
        createdAt: "2026-01-01T00:02:00.000Z",
      });

      const result = await store.runGarbageCollectionStep({
        jobId: job.id,
        expectedRevision: job.revision,
        maxItems: 1,
        updatedAt: "2026-01-01T00:03:00.000Z",
      });

      expect(result.reclaimedSegmentIds).toEqual([segmentId]);
      expect(await store.getSegment(segmentId)).toBeUndefined();
      store.close();
    });

    it("reclaims a committed transaction only after its manifest and segments stop rooting it", async () => {
      const store = await implementation.create();
      const prefix = "committed-transaction-gc";
      await createSupersededStorage(store, prefix);
      const transactionId = `${prefix}/old-transaction`;
      const segmentId = `${prefix}/old-segment`;
      const first = await store.createGarbageCollectionJob({
        id: `${prefix}/gc-retained`,
        candidateManifestVersions: [0],
        candidateSegmentIds: [],
        candidateBlockIds: [],
        candidateTransactionIds: [transactionId],
        leaseCutoff: "2026-01-01T00:10:00.000Z",
        createdAt: "2026-01-01T00:02:00.000Z",
      });
      const retained = await store.runGarbageCollectionStep({
        jobId: first.id,
        expectedRevision: first.revision,
        maxItems: 2,
        updatedAt: "2026-01-01T00:03:00.000Z",
      });
      expect(retained.prunedManifestVersions).toEqual([0]);
      expect(retained.retainedTransactionIds).toEqual([transactionId]);
      expect(await store.getTransaction(transactionId)).toMatchObject({ status: "committed" });

      const second = await store.createGarbageCollectionJob({
        id: `${prefix}/gc-reclaimed`,
        candidateManifestVersions: [],
        candidateSegmentIds: [segmentId],
        candidateBlockIds: [],
        candidateTransactionIds: [transactionId],
        leaseCutoff: "2026-01-01T00:10:00.000Z",
        createdAt: "2026-01-01T00:04:00.000Z",
      });
      const reclaimed = await store.runGarbageCollectionStep({
        jobId: second.id,
        expectedRevision: second.revision,
        maxItems: 2,
        updatedAt: "2026-01-01T00:05:00.000Z",
      });
      expect(reclaimed.reclaimedTransactionIds).toEqual([transactionId]);
      expect(reclaimed.reclaimedSegmentIds).toEqual([segmentId]);
      expect(reclaimed.job).toMatchObject({
        state: "completed",
        reclaimedTransactionCount: 1,
      });
      expect(await store.getTransaction(transactionId)).toBeUndefined();
      store.close();
    });

    it("pins a committed manifest until its compaction job is reconciled", async () => {
      const store = await implementation.create();
      const prefix = "unreconciled-compaction";
      const { job, commit } = await createReadyCompaction(store, prefix);
      const compactedManifest = await store.commitTransaction(commit);
      const tail = await stageTestArtifacts(store, {
        transactionId: `${prefix}/tail-transaction`,
        snapshotVersion: compactedManifest.version,
        blocks: [{ id: `${prefix}/tail-block`, bytes: Uint8Array.of(3) }],
      });
      await store.commitTransaction({
        transactionId: `${prefix}/tail-transaction`,
        expectedTransactionRevision: tail.revision,
        expectedManifestVersion: compactedManifest.version,
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

    it("allows a status-only abort without deleting its recoverable artifacts", async () => {
      const store = await implementation.create();
      const transaction = await stageTestArtifacts(store, {
        transactionId: "dangling-active-transaction",
        blocks: [{ id: "lost-pending-block", bytes: Uint8Array.of(1) }],
      });

      const aborted = await store.updateTransaction(transaction.id, transaction.revision, {
        status: "aborted",
        updatedAt: "2026-01-01T00:01:00.000Z",
      });
      expect(aborted).toMatchObject({
        status: "aborted",
        pendingBlockIds: ["lost-pending-block"],
        revision: transaction.revision + 1,
      });
      expect(await store.getBlock("lost-pending-block")).toEqual(Uint8Array.of(1));
      store.close();
    });

    it("serializes GC with adoption of an existing block into an active journal", async () => {
      const store = await implementation.create();
      const timestamp = "2026-01-01T00:00:00.000Z";
      const original = await stageTestArtifacts(store, {
        transactionId: "aborted-owner",
        blocks: [{ id: "adoptable-block", bytes: Uint8Array.of(7, 8) }],
      });
      await store.updateTransaction(original.id, original.revision, {
        status: "aborted",
        updatedAt: timestamp,
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
      await store.addTable({
        managed: false,
        id: "accounts",
        name: "accounts",
        columns: [
          {
            id: "account-email",
            name: "email",
            type: "string",
            nullable: false,
          },
        ],
        uniqueKeyColumnId: "account-email",
        primaryKeyColumnIds: ["account-email"],
        uniqueKeyLookupReady: true,
        createdAt: timestamp,
        revision: 0,
      });
      const first = await stageTestArtifacts(store, {
        transactionId: "first-transaction",
        snapshotVersion: null,
        blocks: [{ id: "first", bytes: Uint8Array.of(1) }],
      });
      await store.commitTransaction({
        transactionId: "first-transaction",
        expectedTransactionRevision: first.revision,
        expectedManifestVersion: null,
        uniqueKeyChanges: [
          {
            tableId: "accounts",
            keyTokens: ["string:ada@example.com"],
            requireAbsent: true,
          },
        ],
        committedAt: timestamp,
      });
      expect(
        await store.getExistingUniqueKeys("accounts", [
          "string:missing@example.com",
          "string:ada@example.com",
        ]),
      ).toEqual(["string:ada@example.com"]);

      const second = await stageTestArtifacts(store, {
        transactionId: "second-transaction",
        snapshotVersion: 0,
        blocks: [{ id: "second", bytes: Uint8Array.of(2) }],
      });
      await expect(
        store.commitTransaction({
          transactionId: "second-transaction",
          expectedTransactionRevision: second.revision,
          expectedManifestVersion: 0,
          uniqueKeyChanges: [
            {
              tableId: "accounts",
              keyTokens: ["string:ada@example.com"],
              requireAbsent: true,
            },
          ],
          committedAt: timestamp,
        }),
      ).rejects.toBeInstanceOf(UniqueKeyConflictError);
      expect((await store.getCurrentManifest())?.version).toBe(0);
      store.close();
    });
  });
}

it("retains, migrates, and writes every stable IndexedDB schema fixture", async () => {
  const indexedDB = new IDBFactory();
  const currentName = crypto.randomUUID();
  const currentStore = await IndexedDbBlockStore.open({ name: currentName, indexedDB });
  currentStore.close();
  const currentNative = await openNativeIndexedDb(indexedDB, currentName);
  const currentVersion = currentNative.version;
  currentNative.close();

  expect(frozenIndexedDbSchemas.map(({ version }) => version)).toEqual(
    Array.from(
      { length: currentVersion - FIRST_STABLE_INDEXED_DB_SCHEMA_VERSION + 1 },
      (_, index) => FIRST_STABLE_INDEXED_DB_SCHEMA_VERSION + index,
    ),
  );

  for (const fixture of frozenIndexedDbSchemas) {
    expect(
      comparePackageVersions(fixture.writerPackageVersion, currentPackageVersion),
      `IndexedDB schema ${String(fixture.version)} claims a writer newer than this package`,
    ).toBeLessThanOrEqual(0);
    const name = crypto.randomUUID();
    const native = await openNativeIndexedDb(indexedDB, name, fixture.version, fixture.install);
    native.close();

    const bytes = await encodeBlock({
      type: "string",
      values: [`stable-v${String(fixture.version)}`],
    });
    const blockId = `stable-v${String(fixture.version)}-block`;
    const store = await IndexedDbBlockStore.open({ name, indexedDB });
    await stageTestArtifacts(store, { blocks: [{ id: blockId, bytes }] });
    expect(await store.getBlock(blockId)).toEqual(bytes);
    store.close();
  }
});

it("rejects a newer IndexedDB schema without mutating it", async () => {
  const indexedDB = new IDBFactory();
  const name = crypto.randomUUID();
  const newer = await openNativeIndexedDb(indexedDB, name, 2, (request) => {
    request.result.createObjectStore("future");
  });
  await new Promise<void>((resolve, reject) => {
    const transaction = newer.transaction("future", "readwrite");
    transaction.objectStore("future").put("preserve", "sentinel");
    transaction.oncomplete = () => resolve();
    transaction.onerror = () => reject(transaction.error ?? new Error("future write failed"));
  });
  newer.close();

  const mismatch = await IndexedDbBlockStore.open({ name, indexedDB }).catch(
    (error: unknown) => error,
  );
  expect(mismatch).toBeInstanceOf(StorageFormatVersionError);
  expect(mismatch).toMatchObject({
    name: "StorageFormatVersionError",
    backend: "indexeddb",
    location: name,
    actualVersion: 2,
    supportedVersion: 1,
    relation: "newer",
  });
  const unchanged = await openNativeIndexedDb(indexedDB, name, 2);
  expect(unchanged.objectStoreNames.contains("future")).toBe(true);
  unchanged.close();
});

it("reports a corrupt current IndexedDB layout as corruption, never a version mismatch", async () => {
  const indexedDB = new IDBFactory();
  const name = crypto.randomUUID();
  const malformed = await openNativeIndexedDb(indexedDB, name, 1, (request) => {
    request.result.createObjectStore("blocks");
  });
  malformed.close();

  const corruption = await IndexedDbBlockStore.open({ name, indexedDB }).catch(
    (error: unknown) => error,
  );
  expect(corruption).toBeInstanceOf(StorageCorruptionError);
  expect(corruption).toMatchObject({
    name: "StorageCorruptionError",
    backend: "indexeddb",
    location: "schema",
  });
});

it("closes an IndexedDB connection when a newer schema version arrives", async () => {
  const indexedDB = new IDBFactory();
  const name = crypto.randomUUID();
  const store = await IndexedDbBlockStore.open({ name, indexedDB });
  const upgraded = await new Promise<IDBDatabase>((resolve, reject) => {
    const request = indexedDB.open(name, 2);
    request.onsuccess = () => resolve(request.result);
    request.onerror = () => reject(request.error ?? new Error("upgrade failed"));
  });
  await expect(store.getCurrentManifestVersion()).rejects.toThrow(/connection is closed/);
  upgraded.close();
});

it("pages IndexedDB framed block export without materializing all payloads", async () => {
  const indexedDB = new IDBFactory();
  const store = await IndexedDbBlockStore.open({ name: crypto.randomUUID(), indexedDB });
  const bytes = await encodeBlock({ type: "number", values: [1] });
  const blocks = Array.from({ length: 513 }, (_, index) => ({
    id: `batched-export-${String(index).padStart(3, "0")}`,
    bytes,
  }));
  await publishTestBlocks(store, {
    expectedVersion: null,
    blocks,
    createdAt: "2026-08-24T12:00:00.000Z",
  });
  const session = await store.beginSnapshotFrameExport({
    ownerId: "batched-export-owner",
    createdAt: "2026-08-24T12:00:00.000Z",
    expiresAt: "2026-08-24T12:30:00.000Z",
  });
  const frameCount = Object.values(session.header.kinds).reduce(
    (total, summary) => total + summary.frameCount,
    0,
  );
  const exportedBlockIds: string[] = [];
  for (let sequence = 0; sequence < frameCount; sequence += 1) {
    const frame = await store.readSnapshotExportFrame({
      sessionId: session.sessionId,
      ownerId: "batched-export-owner",
      sequence,
      expiresAtCutoff: "2026-08-24T12:00:00.000Z",
      expiresAt: "2026-08-24T12:30:00.000Z",
    });
    if (frame?.kind === "block" && frame.key !== null) exportedBlockIds.push(frame.key);
  }
  expect(exportedBlockIds).toEqual(blocks.map(({ id }) => id));
  expect(
    await store.closeSnapshotFrameExport({
      sessionId: session.sessionId,
      ownerId: "batched-export-owner",
    }),
  ).toBe(true);
  store.close();
});

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
      createdAt: "2026-01-01T00:00:00.000Z",
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
    managed: false,
    id: "events-id",
    name: "events",
    columns: [{ id: "event-id", name: "event_id", type: "number", nullable: false }],
    uniqueKeyColumnId: "event-id",
    uniqueKeyLookupReady: true,
    revision: 0,
    createdAt: "2026-01-01T00:00:00.000Z",
  });
  await stageTestArtifacts(store, {
    blocks: [{ id: "payload", bytes: Uint8Array.of(1, 2, 3, 4) }],
  });
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
  await addCounterFixtureTables(left);
  const right = await IndexedDbBlockStore.open({ name, indexedDB: factory });
  const ranges = await Promise.all([
    left.reserveRowIds("people", 10),
    right.reserveRowIds("people", 10),
  ]);
  expect(new Set(ranges.map((range) => range.start.toString())).size).toBe(2);
  expect(ranges.map((range) => range.endExclusive - range.start)).toEqual([10n, 10n]);
  left.close();
  right.close();
});

it("reserves auto-increment values atomically across IndexedDB connections", async () => {
  const factory = new IDBFactory();
  const name = crypto.randomUUID();
  const left = await IndexedDbBlockStore.open({ name, indexedDB: factory });
  await addCounterFixtureTables(left);
  const right = await IndexedDbBlockStore.open({ name, indexedDB: factory });
  const ranges = await Promise.all([
    left.reserveAutoIncrement("people", "id", 10),
    right.reserveAutoIncrement("people", "id", 10, 5n),
  ]);
  const starts = ranges.map((range) => range.start).sort((a, b) => (a < b ? -1 : 1));
  // Disjoint ranges regardless of which connection won the race.
  expect(starts[1]).toBeGreaterThanOrEqual((starts[0] ?? 0n) + 10n);
  expect(ranges.map((range) => range.endExclusive - range.start)).toEqual([10n, 10n]);
  left.close();
  right.close();
});

it("persists the auto-increment counter across IndexedDB connections", async () => {
  const factory = new IDBFactory();
  const name = crypto.randomUUID();
  let store = await IndexedDbBlockStore.open({ name, indexedDB: factory });
  await addCounterFixtureTables(store);
  await store.reserveAutoIncrement("people", "id", 7, 100n);
  store.close();
  store = await IndexedDbBlockStore.open({ name, indexedDB: factory });
  expect(await store.reserveAutoIncrement("people", "id", 1)).toEqual({
    start: 107n,
    endExclusive: 108n,
  });
  store.close();
});

it("folds unique keys and rejects duplicates through cache, bulk, and probe", async () => {
  const indexedDB = new IDBFactory();
  const name = crypto.randomUUID();
  const timestamp = "2026-01-01T00:00:00.000Z";
  let sequence = 0;
  const commitKeyed = async (
    store: IndexedDbBlockStore,
    tokens: string[],
    expectedManifestVersion: number | null,
    remove = false,
  ): Promise<void> => {
    sequence += 1;
    const id = `keyed-${String(sequence)}`;
    const staged = await stageTestArtifacts(store, {
      transactionId: id,
      snapshotVersion: expectedManifestVersion,
      blocks: [{ id, bytes: Uint8Array.of(1) }],
    });
    await store.commitTransaction({
      transactionId: id,
      expectedTransactionRevision: staged.revision,
      expectedManifestVersion,
      uniqueKeyChanges: [
        {
          tableId: "events",
          keyTokens: tokens,
          requireAbsent: !remove,
          ...(remove ? { remove: true } : {}),
        },
      ],
      committedAt: timestamp,
    });
  };
  const token = (index: number): string => `number:${String(index)}`;

  // 17 commits overflow the 16-chunk tail, folding everything into the base representation.
  let store = await IndexedDbBlockStore.open({ name, indexedDB });
  await store.addTable({
    managed: false,
    id: "events",
    name: "events",
    columns: [{ id: "event-id", name: "event_id", type: "number", nullable: false }],
    uniqueKeyColumnId: "event-id",
    uniqueKeyLookupReady: true,
    revision: 0,
    createdAt: timestamp,
  });
  let version: number | null = null;
  let next = 0;
  for (let commit = 0; commit < 17; commit += 1) {
    await commitKeyed(
      store,
      Array.from({ length: 120 }, () => token(next++)),
      version,
    );
    version = version === null ? 0 : version + 1;
  }
  // Same instance: the memoized membership must still catch a folded duplicate.
  await expect(commitKeyed(store, [token(5), token(9_999)], version)).rejects.toBeInstanceOf(
    UniqueKeyConflictError,
  );
  store.close();

  // A fresh instance has no cache. A batch above the bulk threshold resolves the whole base;
  // a small batch reads only what the tokens need. Both must see the folded keys.
  store = await IndexedDbBlockStore.open({ name, indexedDB });
  const bigBatch = Array.from({ length: 2_500 }, (_, index) => token(10_000 + index));
  bigBatch[1_250] = token(3); // one folded duplicate hidden mid-batch
  await expect(commitKeyed(store, bigBatch, version)).rejects.toBeInstanceOf(
    UniqueKeyConflictError,
  );
  await expect(commitKeyed(store, [token(200_000), token(7)], version)).rejects.toBeInstanceOf(
    UniqueKeyConflictError,
  );
  expect(await store.getExistingUniqueKeys("events", [token(7), token(999_999)])).toEqual([
    token(7),
  ]);
  // New keys still commit, and a second fresh instance sees them plus the folded base.
  await commitKeyed(store, [token(300_000)], version);
  version = (version ?? -1) + 1;
  store.close();
  store = await IndexedDbBlockStore.open({ name, indexedDB });
  await expect(commitKeyed(store, [token(300_000)], version)).rejects.toBeInstanceOf(
    UniqueKeyConflictError,
  );
  await expect(commitKeyed(store, [token(0)], version)).rejects.toBeInstanceOf(
    UniqueKeyConflictError,
  );

  // A delete crosses the fold representation: removed keys become insertable again and stay
  // gone for point lookups, through a second fold and a reopen.
  await commitKeyed(store, [token(5), token(11)], version, true);
  version += 1;
  expect(await store.getExistingUniqueKeys("events", [token(5), token(11), token(6)])).toEqual([
    token(6),
  ]);
  for (let commit = 0; commit < 17; commit += 1) {
    await commitKeyed(
      store,
      Array.from({ length: 120 }, () => token(next++)),
      version,
    );
    version += 1;
  }
  store.close();
  store = await IndexedDbBlockStore.open({ name, indexedDB });
  await commitKeyed(store, [token(5)], version);
  version += 1;
  await expect(commitKeyed(store, [token(11), token(5)], version)).rejects.toBeInstanceOf(
    UniqueKeyConflictError,
  );
  store.close();
});

it("persists compaction checkpoints across IndexedDB connections", async () => {
  const factory = new IDBFactory();
  const name = crypto.randomUUID();
  let store = await IndexedDbBlockStore.open({ name, indexedDB: factory });
  const restartable: CompactionJobRecord = {
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
    rewritePlan: { kind: "copy-v1" },
    outputCursor: null,
    memoryBudgetBytes: 0,
    minimumMemoryBytes: 0,
    level0SourceStoredBytes: 1024,
    anchorSourceStoredBytes: 0,
    peakWorkingBytes: 0,
    outputLogicalBytes: 4096,
    targetLevel: 1,
    state: "running",
    transactionId: "transaction-1",
    outputSegmentId: "segment-output",
    publishedVersion: null,
    revision: 3,
    createdAt: "2026-01-01T00:00:00.000Z",
    updatedAt: "2026-01-01T00:01:00.000Z",
  };
  await prepareCompactionSourceFixtures(store, [restartable]);
  await stageTestArtifacts(store, {
    transactionId: "transaction-1",
    snapshotVersion: restartable.sourceManifestVersion,
    blocks: [{ id: "output-1", bytes: Uint8Array.of(2) }],
  });
  await store.createCompactionJob(restartable);
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
    revision: 2,
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
  await prepareCompactionSourceFixtures(store, [job]);
  await stageTestArtifacts(store, {
    transactionId: "rechunk-transaction",
    snapshotVersion: job.sourceManifestVersion,
    blocks: [{ id: "output-0-id", bytes: Uint8Array.of(2) }],
  });
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

it("persists append-row-range L2 segment and budget metadata across IndexedDB connections", async () => {
  const factory = new IDBFactory();
  const name = crypto.randomUUID();
  let store = await IndexedDbBlockStore.open({ name, indexedDB: factory });
  const segment: SegmentRecord = {
    id: "reopen-level-two-segment",
    tableId: "events",
    transactionId: "reopen-level-two-transaction",
    rowCount: 2,
    rowIdStart: 20n,
    rowIdEndExclusive: 22n,
    columnBlockIds: { value: ["reopen-level-two-block"] },
    kind: "insert",
    level: 2,
    logicalOrder: 9,
    commitOrdinal: 0,
    rowIdSpans: [],
    partitionOrdinal: 4,
    createdAt: "2026-01-01T00:00:00.000Z",
  };
  const job = {
    ...level2CompactionJob("reopen-level-two-job"),
    outputPartitionOrdinal: 4,
  };
  await prepareCompactionSourceFixtures(store, [job]);
  await stageTestArtifacts(store, {
    transactionId: segment.transactionId,
    snapshotVersion: job.sourceManifestVersion,
    blocks: [{ id: "reopen-level-two-block", bytes: Uint8Array.of(2) }],
    segments: [segment],
  });
  await store.createCompactionJob(job);
  store.close();

  store = await IndexedDbBlockStore.open({ name, indexedDB: factory });
  expect(await store.getSegment(segment.id)).toEqual(segment);
  expect(await store.getCompactionJob(job.id)).toMatchObject({
    targetLevel: 2,
    outputPartitionOrdinal: 4,
    maxWriteAmplification: 2,
    maximumOutputStoredBytes: 720,
    plannedOutputStoredBytesUpperBound: 600,
  });
  store.close();
});

it("persists merge source maps and row-ID spans across IndexedDB connections", async () => {
  const factory = new IDBFactory();
  const name = crypto.randomUUID();
  let store = await IndexedDbBlockStore.open({ name, indexedDB: factory });
  const job = mergeCompactionJob("reopen-merge-job");
  await prepareCompactionSourceFixtures(store, [job]);
  await stageTestArtifacts(store, {
    transactionId: "merge-transaction",
    snapshotVersion: job.sourceManifestVersion,
    blocks: [{ id: "merge-output-id", bytes: Uint8Array.of(2) }],
  });
  await store.createCompactionJob(job);
  await store.updateCompactionJob(job.id, 0, {
    outputBlockIds: ["merge-output-id"],
    outputCursor: { outputIndex: 0, columnIndex: 1, rowStart: 0 },
    outputStoredBytes: 70,
    outputLogicalBytes: 80,
    peakWorkingBytes: 600,
    state: "running",
    transactionId: "merge-transaction",
    updatedAt: "2026-01-01T00:01:00.000Z",
  });
  store.close();

  store = await IndexedDbBlockStore.open({ name, indexedDB: factory });
  expect(await store.getCompactionJob(job.id)).toMatchObject({
    sourceSegmentIds: ["base-segment", "delete-segment", "upsert-segment"],
    rewritePlan: {
      kind: "merge-v1",
      keyColumnId: "id-column",
      totalRows: 2,
      rowIdStart: 3n,
      rowIdEndExclusive: 11n,
      rowIdSpans: [
        { rowStart: 0, rowCount: 1, rowIdStart: 10n },
        { rowStart: 1, rowCount: 1, rowIdStart: 3n },
      ],
      sourceSegments: [
        { segmentId: "base-segment", committedVersion: 7, kind: "base" },
        { segmentId: "delete-segment", committedVersion: 8, kind: "delete" },
        { segmentId: "upsert-segment", committedVersion: 9, kind: "upsert" },
      ],
    },
    outputCursor: { outputIndex: 0, columnIndex: 1, rowStart: 0 },
    outputBlockIds: ["merge-output-id"],
    revision: 1,
  });
  store.close();
});

describe("table lookup memo", () => {
  for (const implementation of stores()) {
    it(`${implementation.name} resolves each name to its own record across repeats`, async () => {
      const store = await implementation.create();
      const createdAt = "2026-01-01T00:00:00.000Z";
      await store.addTable({
        managed: false,
        id: "events-id",
        name: "events",
        columns: [{ id: "value-column", name: "value", type: "number", nullable: false }],
        revision: 0,
        createdAt,
      });
      await store.addTable({
        managed: false,
        id: "people-id",
        name: "people",
        columns: [{ id: "label-column", name: "label", type: "string", nullable: false }],
        revision: 0,
        createdAt,
      });
      // Repeats are the point: the first read fills the remembered mapping and the rest go
      // through it, so a mapping that answered with the wrong table would show up here.
      for (let round = 0; round < 3; round += 1) {
        expect((await store.getTableByName("events"))?.id).toBe("events-id");
        expect((await store.getTableByName("people"))?.id).toBe("people-id");
        expect(await store.getTableByName("missing")).toBeUndefined();
      }
    });
  }
});

it("re-resolves a table name when the remembered record no longer carries it", async () => {
  // No public call renames or drops a table, so the disagreement is staged directly in the
  // catalog: the remembered mapping must be treated as a hint that the record itself confirms,
  // never as an answer.
  const indexedDB = new IDBFactory();
  const name = crypto.randomUUID();
  const store = await IndexedDbBlockStore.open({ name, indexedDB });
  await store.addTable({
    managed: false,
    id: "events-id",
    name: "events",
    columns: [{ id: "value-column", name: "value", type: "number", nullable: false }],
    revision: 0,
    createdAt: "2026-01-01T00:00:00.000Z",
  });
  expect((await store.getTableByName("events"))?.id).toBe("events-id");

  const connection = await new Promise<IDBDatabase>((resolve, reject) => {
    const request = indexedDB.open(name);
    request.onsuccess = () => {
      resolve(request.result);
    };
    request.onerror = () => {
      reject(request.error ?? new Error("open failed"));
    };
  });
  await new Promise<void>((resolve, reject) => {
    const transaction = connection.transaction("catalog", "readwrite");
    const catalog = transaction.objectStore("catalog");
    const read = catalog.get("table/id/events-id");
    read.onsuccess = () => {
      catalog.put(
        { ...(read.result as Record<string, unknown>), name: "renamed" },
        "table/id/events-id",
      );
      catalog.delete("table/name/events");
      catalog.put("events-id", "table/name/renamed");
    };
    transaction.oncomplete = () => {
      resolve();
    };
    transaction.onerror = () => {
      reject(transaction.error ?? new Error("stage failed"));
    };
  });
  connection.close();

  expect(await store.getTableByName("events")).toBeUndefined();
  expect((await store.getTableByName("renamed"))?.id).toBe("events-id");
  store.close();
});
