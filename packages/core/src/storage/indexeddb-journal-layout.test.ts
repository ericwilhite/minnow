/**
 * The chunked transaction journal: appends leave a stored layout that differs from a fresh
 * repack of the same ids (a closed chunk can mix blocks and segments in append order), and
 * every journal writer must extend the stored tail rather than assume the canonical layout.
 */
import { IDBFactory } from "fake-indexeddb";
import { describe, expect, it } from "vitest";
import {
  EVENTS_TABLE,
  NOW,
  activeTransaction,
  openStore,
  readRawKeys,
  readRawValue,
  stageBlocks,
  stageSegments,
} from "./indexeddb-audit-helpers.js";

interface JournalChunk {
  blockIds: string[];
  segmentIds: string[];
}

async function readChunk(
  indexedDB: IDBFactory,
  name: string,
  transactionId: string,
  index: number,
): Promise<JournalChunk | undefined> {
  return (await readRawValue(indexedDB, name, "transactionJournal", [transactionId, index])) as
    JournalChunk | undefined;
}

async function journalChunkKeys(
  indexedDB: IDBFactory,
  name: string,
  transactionId: string,
): Promise<IDBValidKey[]> {
  return (await readRawKeys(indexedDB, name, "transactionJournal")).filter(
    (key) => Array.isArray(key) && key[0] === transactionId,
  );
}

describe("IndexedDB transaction journal layout", () => {
  it("keeps the journal consistent when updateTransaction appends past a closed mixed chunk", async () => {
    const indexedDB = new IDBFactory();
    const name = crypto.randomUUID();
    const store = await openStore(indexedDB, name);
    await store.addTable(EVENTS_TABLE);
    let record = activeTransaction("layout", null);
    await store.createTransaction(record);

    // 5 blocks, then 1,024 segments, then 10 blocks. Appending produces
    //   chunk 0 = {5 blocks, 1,019 segments}   (closed: full)
    //   chunk 1 = {10 blocks, 5 segments}
    // whereas a fresh repack of the same arrays would put 15 blocks and 1,009 segments in
    // chunk 0. A writer that derives the tail from a repack corrupts the stored journal.
    const firstBlocks = Array.from({ length: 5 }, (_, index) => `b${String(index)}`);
    record = await stageBlocks(store, record, firstBlocks);
    const segmentIds = Array.from({ length: 1_024 }, (_, index) => `s${String(index)}`);
    record = await stageSegments(store, record, segmentIds, "b0");
    const laterBlocks = Array.from({ length: 10 }, (_, index) => `late${String(index)}`);
    record = await stageBlocks(store, record, laterBlocks);

    const chunk0 = await readChunk(indexedDB, name, "layout", 0);
    const chunk1 = await readChunk(indexedDB, name, "layout", 1);
    expect(chunk0).toEqual({ blockIds: firstBlocks, segmentIds: segmentIds.slice(0, 1_019) });
    expect(chunk1).toEqual({ blockIds: laterBlocks, segmentIds: segmentIds.slice(1_019) });
    expect(await store.getTransaction("layout")).toMatchObject({
      pendingBlockIds: [...firstBlocks, ...laterBlocks],
      pendingSegmentIds: segmentIds,
    });

    // The generic contract path (the engine's compaction-recovery `stageExistingBlocks`) extends
    // the journal by one block that is already persisted: written by a transaction that then
    // aborted without committing it.
    const existing = "existing-block";
    await store.createTransaction(activeTransaction("helper", null));
    const helper = await store.stageTransactionArtifacts({
      transactionId: "helper",
      expectedRevision: 0,
      blocks: [{ id: existing, bytes: Uint8Array.of(9) }],
      segments: [],
      updatedAt: NOW,
    });
    await store.updateTransaction("helper", helper.revision, { status: "aborted", updatedAt: NOW });
    const extended = await store.updateTransaction("layout", record.revision, {
      pendingBlockIds: [...record.pendingBlockIds, existing],
      updatedAt: NOW,
    });
    expect(extended.pendingBlockIds).toEqual([...firstBlocks, ...laterBlocks, existing]);
    expect(extended.pendingSegmentIds).toEqual(segmentIds);

    // The closed chunk is untouched; the open tail gained exactly the new id.
    expect(await readChunk(indexedDB, name, "layout", 0)).toEqual(chunk0);
    expect(await readChunk(indexedDB, name, "layout", 1)).toEqual({
      blockIds: [...laterBlocks, existing],
      segmentIds: segmentIds.slice(1_019),
    });
    expect(await journalChunkKeys(indexedDB, name, "layout")).toHaveLength(2);
    expect(await store.getTransaction("layout")).toEqual(extended);

    // A savepoint rewind across the chunk boundary reads the same stored layout back.
    const keptBlocks = [...firstBlocks, ...laterBlocks, existing];
    const keptSegments = segmentIds.slice(0, 1_000);
    const rewound = await store.rollbackTransactionArtifacts({
      transactionId: "layout",
      expectedRevision: extended.revision,
      pendingBlockIds: keptBlocks,
      pendingSegmentIds: keptSegments,
      removeBlockIds: [],
      removeSegmentIds: segmentIds.slice(1_000),
      updatedAt: NOW,
    });
    expect(rewound.pendingBlockIds).toEqual(keptBlocks);
    expect(rewound.pendingSegmentIds).toEqual(keptSegments);
    expect(await journalChunkKeys(indexedDB, name, "layout")).toHaveLength(1);
    expect(await store.getTransaction("layout")).toEqual(rewound);

    // The record commits from the stored journal.
    const manifest = await store.commitTransaction({
      transactionId: "layout",
      expectedTransactionRevision: rewound.revision,
      expectedManifestVersion: null,
      levelZeroSegmentLimits: [{ tableId: "events", limit: 4_096 }],
      committedAt: NOW,
    });
    expect(manifest.version).toBe(0);
    expect(await store.getTransaction("layout")).toMatchObject({
      status: "committed",
      pendingBlockIds: keptBlocks,
      pendingSegmentIds: keptSegments,
    });
    expect((await store.checkIntegrity()).issues).toEqual([]);
    store.close();
  });
});
