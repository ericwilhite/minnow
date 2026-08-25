import { expect, it } from "vitest";
import { MemoryBlockStore, type BlockStore } from "../storage/index.js";
import { FaultInjectingBlockStore } from "./index.js";

async function stageOne(store: FaultInjectingBlockStore, suffix: string) {
  await store.addTable({
    id: "table-t",
    name: "t",
    columns: [{ id: "col-v", name: "v", type: "number", nullable: false }],
    managed: false,
    revision: 0,
    createdAt: "2026-01-01T00:00:00.000Z",
  });
  const transactionId = `txn-${suffix}`;
  const blockId = `block-${suffix}`;
  await store.createTransaction({
    id: transactionId,
    ownerId: `owner-${suffix}`,
    expiresAt: "2026-01-01T01:00:00.000Z",
    snapshotVersion: null,
    pendingBlockIds: [],
    pendingSegmentIds: [],
    status: "active",
    revision: 0,
    startedAt: "2026-01-01T00:00:00.000Z",
    updatedAt: "2026-01-01T00:00:00.000Z",
    committedVersion: null,
  });
  const staged = await store.stageTransactionArtifacts({
    transactionId,
    expectedRevision: 0,
    blocks: [{ id: blockId, bytes: Uint8Array.of(1, 2, 3) }],
    segments: [
      {
        id: `segment-${suffix}`,
        tableId: "table-t",
        transactionId,
        rowCount: 1,
        rowIdStart: 1n,
        rowIdEndExclusive: 2n,
        columnBlockIds: { "col-v": [blockId] },
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
  return { blockId, transactionId, revision: staged.revision };
}

it("leaves complete staged artifacts when transaction publication fails", async () => {
  const inner = new MemoryBlockStore();
  const store = new FaultInjectingBlockStore(inner, (point) => {
    if (point === "beforeTransactionCommit") throw new Error("injected");
  });
  const staged = await stageOne(store, "pending");
  await expect(
    store.commitTransaction({
      transactionId: staged.transactionId,
      expectedTransactionRevision: staged.revision,
      expectedManifestVersion: null,
      levelZeroSegmentLimits: [{ tableId: "table-t", limit: 4096 }],
      committedAt: "2026-01-01T01:00:00.000Z",
    }),
  ).rejects.toThrow("injected");
  expect(await inner.getBlock(staged.blockId)).toEqual(Uint8Array.of(1, 2, 3));
  expect(await inner.getCurrentManifest()).toBeUndefined();
});

it("models an uncertain outcome after a successful transaction commit", async () => {
  const inner = new MemoryBlockStore();
  const store = new FaultInjectingBlockStore(inner, (point) => {
    if (point === "afterTransactionCommit") throw new Error("response lost");
  });
  const staged = await stageOne(store, "ready");
  await expect(
    store.commitTransaction({
      transactionId: staged.transactionId,
      expectedTransactionRevision: staged.revision,
      expectedManifestVersion: null,
      levelZeroSegmentLimits: [{ tableId: "table-t", limit: 4096 }],
      committedAt: "2026-01-01T01:00:00.000Z",
    }),
  ).rejects.toThrow("response lost");
  const manifest = await inner.getCurrentManifest();
  expect(manifest?.liveBlockCount).toBe(1);
  expect(
    manifest === undefined
      ? false
      : (await inner.hasManifestBlocks(manifest.version, [staged.blockId]))[0],
  ).toBe(true);
});

it("forwards the complete maintenance and recovery surface without changing arguments", async () => {
  const calls: Array<{ method: string; args: unknown[] }> = [];
  const inner = new Proxy(
    {},
    {
      get:
        (_target, property) =>
        (...args: unknown[]): Promise<undefined> => {
          calls.push({ method: String(property), args });
          return Promise.resolve(undefined);
        },
    },
  ) as BlockStore;
  const faults: string[] = [];
  const store = new FaultInjectingBlockStore(inner, (point) => {
    faults.push(point);
  });
  const cases: Array<{ method: keyof BlockStore; args: unknown[] }> = [
    { method: "readManifestBlock", args: [1, "block"] },
    { method: "removeTable", args: ["table", 2, { expectedCatalogEpoch: 3 }] },
    { method: "dropTable", args: [{ tableId: "table" }] },
    { method: "dropTableColumn", args: [{ tableId: "table", columnId: "column" }] },
    { method: "removeAbortedSegment", args: ["segment", "transaction"] },
    { method: "adoptAbortedSegment", args: [{ segmentId: "segment" }] },
    { method: "reserveAutoIncrement", args: ["table", "column", 4, 9n] },
    { method: "writeFtsBase", args: ["table", "column", { expectedRevision: 1 }] },
    { method: "beginFtsBaseBuild", args: [{ buildId: "build" }] },
    { method: "renewFtsBaseBuild", args: [{ buildId: "build" }] },
    { method: "writeFtsBaseBuildChunk", args: [{ buildId: "build" }] },
    { method: "finishFtsBaseBuild", args: [{ buildId: "build" }] },
    { method: "abortFtsBaseBuild", args: [{ buildId: "build" }] },
    { method: "removeFtsColumn", args: ["table", "column"] },
    { method: "readFtsCandidates", args: ["table", "column", ["term"], 5] },
    { method: "readFtsPostings", args: ["table", "column", 5, 6, 7] },
    { method: "beginUniqueKeyBuild", args: [{ buildId: "build" }] },
    { method: "getUniqueKeyBuild", args: ["build"] },
    { method: "renewUniqueKeyBuild", args: [{ buildId: "build" }] },
    { method: "appendUniqueKeyBuildChunk", args: [{ buildId: "build" }] },
    { method: "finishUniqueKeyBuild", args: [{ buildId: "build" }] },
    { method: "abortUniqueKeyBuild", args: [{ buildId: "build" }] },
    { method: "abortTransactionIfExpired", args: [{ transactionId: "transaction" }] },
    { method: "rollbackTransactionArtifacts", args: [{ transactionId: "transaction" }] },
    { method: "getLease", args: ["lease"] },
    { method: "listCompactionJobs", args: ["table"] },
    { method: "cancelCompactionJob", args: ["job", 1, "2026-01-01T00:00:00.000Z"] },
    { method: "listGarbageCollectionJobs", args: [] },
    { method: "getTempOwner", args: ["owner"] },
    { method: "renewTempOwner", args: [{ ownerId: "owner" }] },
    {
      method: "removeTempOwnerIfExpired",
      args: ["owner", "2026-01-01T00:00:00.000Z"],
    },
    { method: "listTempOwnerIdsPage", args: [null, 8] },
  ];

  const dynamicStore = store as unknown as Record<string, (...args: unknown[]) => unknown>;
  for (const entry of cases) {
    const method = dynamicStore[entry.method];
    expect(method, entry.method).toBeTypeOf("function");
    await Reflect.apply(method ?? (() => undefined), store, entry.args);
  }

  expect(calls).toEqual(cases.map((entry) => ({ method: entry.method, args: entry.args })));
  expect(faults).toEqual(["beforeBlockRead", "afterBlockRead"]);
});
