import { IDBCursor, IDBFactory, IDBKeyRange, IDBObjectStore, IDBRequest } from "fake-indexeddb";
import { afterEach, expect, it, vi } from "vitest";
import { MinnowDatabase } from "../engine/database.js";
import { IndexedDbBlockStore } from "./indexeddb.js";

afterEach(() => {
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
});

it("batches retained history checks and excludes block history from table listing", async () => {
  vi.stubGlobal("IDBKeyRange", IDBKeyRange);
  const store = await IndexedDbBlockStore.open({
    indexedDB: new IDBFactory(),
    name: crypto.randomUUID(),
  });
  const db = new MinnowDatabase(store, { autoCompact: false, autoCollect: false });
  try {
    await db.execute("CREATE TABLE items(id INTEGER PRIMARY KEY, value INTEGER)");
    for (let id = 0; id < 160; id += 1) {
      await db.insertBatch("items", [{ id, value: id }]);
    }
    const listingCursor = vi.spyOn(IDBCursor.prototype, "continue");
    expect((await store.listTables()).map((table) => table.name)).toEqual(["items"]);
    expect(listingCursor.mock.calls.length).toBeLessThan(10);
    listingCursor.mockClear();
    // A reader pinning an older version must validate retired history without one IPC per block.
    await store.createLease({
      id: "old-reader",
      kind: "reader",
      ownerId: "reader",
      manifestVersion: 0,
      revision: 0,
      createdAt: new Date().toISOString(),
      expiresAt: new Date(Date.now() + 60_000).toISOString(),
    });
    expect(listingCursor.mock.calls.length).toBeLessThan(20);
    await store.removeLease({ id: "old-reader", ownerId: "reader" });
    listingCursor.mockRestore();
    const job = await store.createGarbageCollectionJob({
      id: "retained-history",
      candidateManifestVersions: [0],
      candidateSegmentIds: [],
      candidateBlockIds: [],
      candidateTransactionIds: [],
      leaseCutoff: new Date().toISOString(),
      createdAt: new Date().toISOString(),
    });
    const cursor = vi.spyOn(IDBCursor.prototype, "continue");
    const batches = vi.spyOn(IDBObjectStore.prototype, "getAll");
    const result = await store.runGarbageCollectionStep({
      jobId: job.id,
      expectedRevision: job.revision,
      maxItems: 1,
      updatedAt: new Date().toISOString(),
    });
    // 160 manifests and hundreds of retained blocks must not mean hundreds of IPC round trips.
    expect(cursor.mock.calls.length).toBeLessThan(100);
    expect(batches.mock.calls.length).toBeGreaterThan(1);
    expect(batches.mock.calls.every((call) => call[1] === 128)).toBe(true);
    expect(result.prunedManifestVersions).toEqual([0]);
    expect((await db.query("SELECT COUNT(*) AS n, SUM(value) AS total FROM items")).rows).toEqual([
      { n: 160, total: 12720 },
    ]);
  } finally {
    await db.close();
    store.close();
  }
});

function requestValue<T>(request: IDBRequest<T>): Promise<T> {
  return new Promise((resolve, reject) => {
    request.onsuccess = () => resolve(request.result);
    request.onerror = () => reject(request.error ?? new Error("Request failed"));
  });
}

for (const failure of ["missing block", "invalid transaction status"] as const) {
  it(`aborts collection atomically on ${failure} with native batch reads`, async () => {
    vi.stubGlobal("IDBKeyRange", IDBKeyRange);
    const indexedDB = new IDBFactory();
    const name = crypto.randomUUID();
    const store = await IndexedDbBlockStore.open({ indexedDB, name });
    const db = new MinnowDatabase(store, {
      autoCompact: false,
      autoCollect: false,
      rowsPerBlock: 1,
    });
    const raw = await requestValue(indexedDB.open(name));
    try {
      await db.execute("CREATE TABLE items(id INTEGER PRIMARY KEY, value INTEGER)");
      await db.insertBatch(
        "items",
        Array.from({ length: 160 }, (_, id) => ({ id, value: id })),
      );
      await db.insertBatch("items", [{ id: 160, value: 160 }]);
      const job = await store.createGarbageCollectionJob({
        id: "corruption",
        candidateManifestVersions: [0],
        candidateSegmentIds: [],
        candidateBlockIds: [],
        candidateTransactionIds: [],
        leaseCutoff: new Date().toISOString(),
        createdAt: new Date().toISOString(),
      });
      const transaction = raw.transaction(["blocks", "transactions"], "readwrite");
      const completion = new Promise<void>((resolve, reject) => {
        transaction.oncomplete = () => resolve();
        transaction.onabort = () => reject(transaction.error ?? new Error("Transaction aborted"));
      });
      if (failure === "missing block") {
        const blocks = transaction.objectStore("blocks");
        const keys = await requestValue(blocks.getAllKeys());
        // Delete a block beyond the first validation page, not an early fast-failure case.
        expect(keys.length).toBeGreaterThan(128);
        const last = keys.at(-1);
        if (last === undefined) throw new Error("Expected blocks");
        blocks.delete(last);
      } else {
        transaction
          .objectStore("transactions")
          .put({ id: "corrupt", status: "unknown" }, "corrupt");
      }
      await completion;
      await expect(
        store.runGarbageCollectionStep({
          jobId: job.id,
          expectedRevision: 0,
          maxItems: 1,
          updatedAt: new Date().toISOString(),
        }),
      ).rejects.toThrow();
      expect((await store.getManifest(0))?.prunedAt).toBeUndefined();
      expect((await store.getGarbageCollectionJob(job.id))?.revision).toBe(0);
    } finally {
      await db.close();
      raw.close();
      store.close();
    }
  });
}

it("remembers an abort while request failure processing is delayed", async () => {
  const store = await IndexedDbBlockStore.open({
    indexedDB: new IDBFactory(),
    name: crypto.randomUUID(),
  });
  // eslint-disable-next-line @typescript-eslint/unbound-method -- Called with its native receiver below.
  const originalGet = IDBObjectStore.prototype.get;
  // eslint-disable-next-line @typescript-eslint/unbound-method -- Called with its native receiver below.
  const originalListen = IDBRequest.prototype.addEventListener;
  // Hold the engine's failure continuation until after native abort dispatch. Recovery must
  // observe the completed transaction rather than wait for a second abort event.
  const listening = vi.spyOn(IDBRequest.prototype, "addEventListener").mockImplementation(function (
    this: IDBRequest,
    type,
    listener,
    options,
  ) {
    if (type !== "error") return originalListen.call(this, type, listener, options);
    originalListen.call(
      this,
      type,
      (event) => {
        setTimeout(() => {
          if (typeof listener === "function") listener.call(this, event);
          else listener.handleEvent(event);
        }, 0);
      },
      options,
    );
  });
  const get = vi.spyOn(IDBObjectStore.prototype, "get").mockImplementation(function (
    this: IDBObjectStore,
    key,
  ) {
    const request = originalGet.call(this, key);
    this.transaction.abort();
    return request;
  });
  try {
    await expect(store.getBlocks(["missing"])).rejects.toThrow();
  } finally {
    get.mockRestore();
    listening.mockRestore();
    store.close();
  }
});
