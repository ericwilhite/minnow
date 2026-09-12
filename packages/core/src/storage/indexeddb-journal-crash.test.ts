/**
 * The chunked transaction journal under IndexedDB transaction aborts: a stage that aborts between
 * chunk writes leaves nothing behind (including in the adapter's in-memory journal cache), a
 * many-chunk commit is one IndexedDB transaction acknowledged only after `complete`, and a
 * single-shot write touches no other record's chunks.
 */
import { IDBFactory } from "fake-indexeddb";
import { describe, expect, it } from "vitest";
import {
  EVENTS_TABLE,
  NOW,
  activeTransaction,
  instrumentFactory,
  openStore,
  readRawKeys,
  readRawValue,
  segment,
  stageBlocks,
  stageSegments,
} from "./indexeddb-audit-helpers.js";

async function journalChunkKeys(
  indexedDB: IDBFactory,
  name: string,
  transactionId: string,
): Promise<IDBValidKey[]> {
  return (await readRawKeys(indexedDB, name, "transactionJournal")).filter(
    (key) => Array.isArray(key) && key[0] === transactionId,
  );
}

describe("IndexedDB transaction journal under aborts", () => {
  it("rolls back a stage aborted between chunk writes and keeps the journal cache truthful", async () => {
    const indexedDB = new IDBFactory();
    const name = crypto.randomUUID();
    const instrumented = instrumentFactory(indexedDB);
    const store = await openStore(indexedDB, name);
    await store.addTable(EVENTS_TABLE);
    let record = activeTransaction("crash", null);
    await store.createTransaction(record);
    // 1,000 blocks in the journal: the next 64-block stage tops chunk 0 up and opens chunk 1.
    const ids = Array.from({ length: 1_000 }, (_, index) => `c${String(index)}`);
    record = await stageBlocks(store, record, ids);
    const revisionBefore = record.revision;
    const blocksBefore = await readRawKeys(indexedDB, name, "blocks");

    // Abort after the first (topped-up) chunk write, before the second chunk lands.
    let journalPuts = 0;
    instrumented.setHook((info) => {
      if (info.storeName === "transactionJournal" && info.method === "put") {
        journalPuts += 1;
        if (journalPuts === 1) return "abort";
      }
      return undefined;
    });
    const batch = Array.from({ length: 64 }, (_, index) => ({
      id: `crash-${String(index)}`,
      bytes: Uint8Array.of(1),
    }));
    await expect(
      store.stageTransactionArtifacts({
        transactionId: "crash",
        expectedRevision: record.revision,
        blocks: batch,
        segments: [],
        updatedAt: NOW,
      }),
    ).rejects.toThrow();
    instrumented.setHook(undefined);
    expect(journalPuts).toBeGreaterThanOrEqual(1);

    // Nothing of the aborted stage is visible: header, chunks, and blocks are as before.
    expect(await readRawValue(indexedDB, name, "transactions", "crash")).toMatchObject({
      revision: revisionBefore,
      journalChunkCount: 1,
      pendingBlockCount: 1_000,
    });
    expect(await readRawValue(indexedDB, name, "transactionJournal", ["crash", 1])).toBeUndefined();
    expect(await readRawKeys(indexedDB, name, "blocks")).toEqual(blocksBefore);
    expect(await store.getTransaction("crash")).toMatchObject({
      revision: revisionBefore,
      pendingBlockIds: ids,
    });

    // The adapter's journal cache has not absorbed the refused ids: the same batch stages
    // cleanly now and the returned record is exactly the journal plus this batch.
    const after = await store.stageTransactionArtifacts({
      transactionId: "crash",
      expectedRevision: revisionBefore,
      blocks: batch,
      segments: [],
      updatedAt: NOW,
    });
    expect(after.pendingBlockIds).toEqual([...ids, ...batch.map((block) => block.id)]);
    expect(await store.getTransaction("crash")).toEqual(after);
    expect((await store.checkIntegrity()).issues).toEqual([]);
    store.close();
  });

  it("commits a many-chunk journal in one IndexedDB transaction and rewinds across chunks", async () => {
    const indexedDB = new IDBFactory();
    const name = crypto.randomUUID();
    const instrumented = instrumentFactory(indexedDB);
    const store = await openStore(indexedDB, name);
    await store.addTable(EVENTS_TABLE);
    let record = activeTransaction("many", null);
    await store.createTransaction(record);
    const ids = Array.from({ length: 5_000 }, (_, index) => `m${String(index)}`);
    record = await stageBlocks(store, record, ids);
    const segmentIds = Array.from({ length: 100 }, (_, index) => `ms${String(index)}`);
    record = await stageSegments(store, record, segmentIds, "m0");
    expect(await journalChunkKeys(indexedDB, name, "many")).toHaveLength(5);

    // Rewind to a savepoint that cuts through chunk 2.
    const keptBlocks = ids.slice(0, 2_500);
    const keptSegments = segmentIds.slice(0, 20);
    record = await store.rollbackTransactionArtifacts({
      transactionId: "many",
      expectedRevision: record.revision,
      pendingBlockIds: keptBlocks,
      pendingSegmentIds: keptSegments,
      removeBlockIds: ids.slice(2_500),
      removeSegmentIds: segmentIds.slice(20),
      updatedAt: NOW,
    });
    expect(record.pendingBlockIds).toEqual(keptBlocks);
    expect(record.pendingSegmentIds).toEqual(keptSegments);
    const chunksAfterRewind = Math.ceil(2_520 / 1_024);
    expect(await journalChunkKeys(indexedDB, name, "many")).toHaveLength(chunksAfterRewind);
    expect(await store.getTransaction("many")).toEqual(record);

    // Commit: one readwrite IndexedDB transaction, acknowledged only after `complete`.
    instrumented.reset();
    let completeSeen = false;
    const commit = store.commitTransaction({
      transactionId: "many",
      expectedTransactionRevision: record.revision,
      expectedManifestVersion: null,
      levelZeroSegmentLimits: [{ tableId: "events", limit: 4_096 }],
      committedAt: NOW,
    });
    // The transaction is created synchronously inside commitTransaction.
    const created = instrumented.transactions.filter((entry) => entry.mode === "readwrite");
    expect(created).toHaveLength(1);
    created[0]?.transaction.addEventListener("complete", () => {
      completeSeen = true;
    });
    const completeBeforeResolve = commit.then(() => completeSeen);
    const manifest = await commit;
    expect(manifest.version).toBe(0);
    expect(await completeBeforeResolve).toBe(true);
    expect(instrumented.transactions.filter((entry) => entry.mode === "readwrite")).toHaveLength(1);
    expect(await store.getTransaction("many")).toMatchObject({
      status: "committed",
      pendingBlockIds: keptBlocks,
      pendingSegmentIds: keptSegments,
    });
    // Commit rewrites the header only; the chunks stay as the rewind left them.
    expect(await journalChunkKeys(indexedDB, name, "many")).toHaveLength(chunksAfterRewind);
    expect((await store.checkIntegrity()).issues).toEqual([]);
    store.close();
  });

  it("writes only its own journal chunks in a single-shot writeTransaction", async () => {
    const indexedDB = new IDBFactory();
    const name = crypto.randomUUID();
    const instrumented = instrumentFactory(indexedDB);
    const store = await openStore(indexedDB, name);
    await store.addTable(EVENTS_TABLE);
    const other = activeTransaction("other", null);
    await store.createTransaction(other);
    const otherIds = Array.from({ length: 10 }, (_, index) => `o${String(index)}`);
    await stageBlocks(store, other, otherIds);
    const otherChunk = await readRawValue(indexedDB, name, "transactionJournal", ["other", 0]);
    await store.createTransaction(activeTransaction("single", null));

    instrumented.reset();
    const touched: IDBValidKey[] = [];
    instrumented.setHook((info) => {
      if (info.storeName === "transactionJournal" && info.method !== "get") {
        touched.push(info.key ?? "?");
      }
      return undefined;
    });
    await store.writeTransaction({
      transaction: { id: "single", expectedRevision: 0 },
      expectedManifestVersion: null,
      blocks: [{ id: "single-block", bytes: Uint8Array.of(1) }],
      segments: [segment("single-seg", "single", "single-block")],
      levelZeroSegmentLimits: [{ tableId: "events", limit: 4_096 }],
      committedAt: NOW,
    });
    instrumented.setHook(undefined);

    expect(touched.length).toBeGreaterThan(0);
    expect(touched.every((key) => Array.isArray(key) && key[0] === "single")).toBe(true);
    expect(await readRawValue(indexedDB, name, "transactionJournal", ["other", 0])).toEqual(
      otherChunk,
    );
    expect((await store.getTransaction("other"))?.pendingBlockIds).toEqual(otherIds);
    expect(await store.getTransaction("single")).toMatchObject({
      status: "committed",
      pendingBlockIds: ["single-block"],
      pendingSegmentIds: ["single-seg"],
    });
    expect((await store.checkIntegrity()).issues).toEqual([]);
    store.close();
  });
});
