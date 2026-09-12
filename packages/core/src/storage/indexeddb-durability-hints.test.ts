/**
 * Durability on IndexedDB: the `durability` option reaches every readwrite transaction as its
 * IndexedDB durability hint (defaulting to "strict"), and no write is acknowledged before its
 * transaction has fired `complete` (or `abort`), never on the last request's `success`.
 */
import { IDBFactory } from "fake-indexeddb";
import { describe, expect, it } from "vitest";
import { MinnowDatabase } from "../engine/database.js";
import {
  EVENTS_TABLE,
  NOW,
  activeTransaction,
  instrumentFactory,
  openStore,
  segment,
} from "./indexeddb-audit-helpers.js";

describe("IndexedDB durability hints", () => {
  it("passes the durability hint to every readwrite transaction and none to readonly ones", async () => {
    for (const durability of ["strict", "relaxed", undefined] as const) {
      const indexedDB = new IDBFactory();
      const instrumented = instrumentFactory(indexedDB);
      const store = await openStore(indexedDB, crypto.randomUUID(), durability);
      instrumented.reset();
      await store.addTable(EVENTS_TABLE);
      await store.getTable("events");
      await store.getCurrentManifest();
      const readwrite = instrumented.transactions.filter((entry) => entry.mode === "readwrite");
      const readonly = instrumented.transactions.filter((entry) => entry.mode === "readonly");
      expect(readwrite.length).toBeGreaterThan(0);
      expect(readonly.length).toBeGreaterThan(0);
      const expected = durability ?? "strict";
      expect(readwrite.map((entry) => entry.options?.durability)).toEqual(
        readwrite.map(() => expected),
      );
      expect(readonly.map((entry) => entry.options)).toEqual(readonly.map(() => undefined));
      store.close();
    }
  });

  it("acknowledges every store write only after its transaction completed or aborted", async () => {
    const indexedDB = new IDBFactory();
    const instrumented = instrumentFactory(indexedDB);
    const store = await openStore(indexedDB);
    const unfinished: string[] = [];
    const failures: string[] = [];
    // Check synchronously in the continuation of each call's promise: at that point every
    // readwrite transaction the call created must already have fired complete or abort.
    const check = async (label: string, run: () => Promise<unknown>): Promise<void> => {
      const before = instrumented.transactions.length;
      await run().then(
        () => undefined,
        (error: unknown) => {
          failures.push(`${label}: ${String(error)}`);
        },
      );
      for (const entry of instrumented.transactions.slice(before)) {
        if (entry.mode === "readwrite" && !entry.completed && !entry.aborted) {
          unfinished.push(`${label}: readwrite over ${entry.stores.join(",")}`);
        }
      }
    };
    await check("open", () => openStore(indexedDB).then((opened) => opened.close()));
    await check("addTable", () => store.addTable(EVENTS_TABLE));
    const record = activeTransaction("dur", null);
    await check("createTransaction", () => store.createTransaction(record));
    let staged = record;
    await check("stageTransactionArtifacts", async () => {
      staged = await store.stageTransactionArtifacts({
        transactionId: "dur",
        expectedRevision: 0,
        blocks: [{ id: "d1", bytes: Uint8Array.of(1) }],
        segments: [segment("ds1", "dur", "d1", 0, 1n)],
        updatedAt: NOW,
      });
    });
    await check("renewTransaction", () =>
      store.renewTransaction({
        transactionId: "dur",
        ownerId: record.ownerId,
        expiresAtCutoff: NOW,
        expiresAt: "2026-09-12T12:40:00.000Z",
      }),
    );
    await check("rollbackTransactionArtifacts", async () => {
      staged = await store.rollbackTransactionArtifacts({
        transactionId: "dur",
        expectedRevision: staged.revision,
        pendingBlockIds: ["d1"],
        pendingSegmentIds: [],
        removeBlockIds: [],
        removeSegmentIds: ["ds1"],
        updatedAt: NOW,
      });
    });
    await check("updateTransaction", async () => {
      staged = await store.updateTransaction("dur", staged.revision, { updatedAt: NOW });
    });
    await check("commitTransaction", () =>
      store.commitTransaction({
        transactionId: "dur",
        expectedTransactionRevision: staged.revision,
        expectedManifestVersion: null,
        committedAt: NOW,
      }),
    );
    const { schemaEpoch } = await store.getCatalogProbe();
    await check("writeTransaction", () =>
      store.writeTransaction({
        transaction: { record: { ...activeTransaction("dur2", 0), schemaEpochGuard: schemaEpoch } },
        expectedManifestVersion: 0,
        blocks: [{ id: "d2", bytes: Uint8Array.of(2) }],
        segments: [segment("ds2", "dur2", "d2", 0, 1n)],
        levelZeroSegmentLimits: [{ tableId: "events", limit: 4_096 }],
        committedAt: NOW,
      }),
    );
    await check("createLease", () =>
      store.createLease({
        id: "lease",
        kind: "reader",
        ownerId: "o",
        manifestVersion: 1,
        expiresAt: "2026-09-12T12:40:00.000Z",
        createdAt: NOW,
        revision: 0,
      }),
    );
    await check("renewLease", () =>
      store.renewLease({
        id: "lease",
        expectedRevision: 0,
        expiresAtCutoff: NOW,
        expiresAt: "2026-09-12T12:45:00.000Z",
      }),
    );
    await check("removeLease", () => store.removeLease({ id: "lease", ownerId: "o" }));
    await check("createTempOwner", () =>
      store.createTempOwner({
        ownerId: "t",
        createdAt: NOW,
        expiresAt: "2026-09-12T12:40:00.000Z",
        revision: 0,
      }),
    );
    await check("putTempRunPage", () =>
      store.putTempRunPage({ ownerId: "t", runId: "r", pageIndex: 0, bytes: Uint8Array.of(1) }),
    );
    await check("removeTempOwner", () => store.removeTempOwner("t"));
    await check("reserveRowIds", () => store.reserveRowIds("events", 10));
    await check("garbage collection job", async () => {
      const job = await store.createGarbageCollectionJob({
        id: "gc",
        candidateManifestVersions: [0],
        candidateSegmentIds: [],
        candidateBlockIds: [],
        candidateTransactionIds: ["dur"],
        leaseCutoff: NOW,
        createdAt: NOW,
      });
      await store.runGarbageCollectionStep({
        jobId: job.id,
        expectedRevision: job.revision,
        maxItems: 8,
        updatedAt: NOW,
      });
      await store.removeGarbageCollectionJob("gc");
    });
    await check("removePrunedManifestRecords", () => store.removePrunedManifestRecords(8));
    await check("engine insert", async () => {
      const database = new MinnowDatabase(store, { autoCompact: false });
      await database.createTable({
        name: "items",
        uniqueKey: "id",
        columns: [{ name: "id", type: "number" }],
      });
      await database.insert("items", { id: 1 });
      await database.write(async (session) => {
        await session.insertBatch("items", [{ id: 2 }]);
      });
    });
    // Every operation must have run so every write path was exercised.
    expect(failures).toEqual([]);
    expect(instrumented.transactions.some((entry) => entry.mode === "readwrite")).toBe(true);
    expect(unfinished).toEqual([]);
    store.close();
  });

  it("does not resolve a stage before `complete` even once every request succeeded", async () => {
    const indexedDB = new IDBFactory();
    const instrumented = instrumentFactory(indexedDB);
    const store = await openStore(indexedDB);
    await store.addTable(EVENTS_TABLE);
    await store.createTransaction(activeTransaction("ack", null));
    instrumented.reset();
    let completeSeen = false;
    const promise = store.stageTransactionArtifacts({
      transactionId: "ack",
      expectedRevision: 0,
      blocks: [{ id: "a1", bytes: Uint8Array.of(1) }],
      segments: [],
      updatedAt: NOW,
    });
    // The readwrite transaction is created synchronously inside the call; fake-indexeddb
    // dispatches `complete` from a queued task, so a listener registered now observes it.
    const entry = instrumented.transactions.find((candidate) => candidate.mode === "readwrite");
    expect(entry).toBeDefined();
    entry?.transaction.addEventListener("complete", () => {
      completeSeen = true;
    });
    const completeBeforeResolve = promise.then(() => completeSeen);
    await promise;
    expect(await completeBeforeResolve).toBe(true);
    store.close();
  });
});
