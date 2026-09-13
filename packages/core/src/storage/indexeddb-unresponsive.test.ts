import { IDBFactory } from "fake-indexeddb";
import { describe, expect, it } from "vitest";
import { classifyError } from "../engine/errors.js";
import { IndexedDbBlockStore, INDEXEDDB_UNRESPONSIVE_AFTER_MS } from "./indexeddb.js";
import { StorageUnresponsiveError, UnknownOutcomeError } from "./types.js";

/**
 * A connection that answers nothing.
 *
 * WebKit keeps a terminated worker's IndexedDB connection, and its unfinished transaction,
 * registered until the owning document goes away; while it is registered every connection to
 * that database blocks, new ones included, and no event ever arrives. Nothing in IndexedDB can
 * be cancelled or timed out, so the adapter's own deadline is the only thing between that and a
 * call that hangs forever. These transactions reproduce exactly that: they are attached to the
 * real connection (so the adapter's guard is found through `transaction.db`) and they deliver no
 * events at all.
 */
function wedge(db: IDBDatabase): void {
  // The stub graph is cyclic: requests name their transaction and the transaction names its
  // stores, so the transaction is filled in after both exist.
  const shared: { transaction?: IDBTransaction } = {};
  const request = (): IDBRequest =>
    ({
      get transaction() {
        return shared.transaction;
      },
      result: undefined,
      error: null,
      source: null,
      readyState: "pending",
      addEventListener: () => undefined,
      removeEventListener: () => undefined,
    }) as unknown as IDBRequest;
  const objectStore = {
    get: request,
    getAll: request,
    getAllKeys: request,
    getKey: request,
    count: request,
    put: request,
    add: request,
    delete: request,
    clear: request,
    openCursor: request,
    openKeyCursor: request,
    index: () => objectStore,
    get transaction() {
      return shared.transaction;
    },
  } as unknown as IDBObjectStore;
  shared.transaction = {
    db,
    error: null,
    mode: "readonly",
    durability: "strict",
    objectStoreNames: [] as unknown as DOMStringList,
    objectStore: () => objectStore,
    abort: () => undefined,
    commit: () => undefined,
    addEventListener: () => undefined,
    removeEventListener: () => undefined,
  } as unknown as IDBTransaction;
  Object.defineProperty(db, "transaction", {
    value: () => shared.transaction,
    configurable: true,
  });
}

function firstConnection(connections: IDBDatabase[]): IDBDatabase {
  const connection = connections[0];
  if (connection === undefined) throw new Error("The store opened no connection");
  return connection;
}

/** Opens a real fake-indexeddb store while keeping the connection reachable for wedging. */
async function openStore(unresponsiveAfterMs: number): Promise<{
  store: IndexedDbBlockStore;
  connections: IDBDatabase[];
}> {
  const base = new IDBFactory();
  const connections: IDBDatabase[] = [];
  const indexedDB = {
    open: (name: string, version?: number) => {
      const request = base.open(name, version);
      request.addEventListener("success", () => connections.push(request.result), { once: true });
      return request;
    },
    deleteDatabase: (name: string) => base.deleteDatabase(name),
    databases: () => base.databases(),
    cmp: (first: IDBValidKey, second: IDBValidKey) => base.cmp(first, second),
  };
  const store = await IndexedDbBlockStore.open({
    name: "unresponsive",
    indexedDB,
    unresponsiveAfterMs,
  });
  return { store, connections };
}

describe("an IndexedDB connection that stops answering", () => {
  it("fails the waiting call with a typed error instead of hanging", async () => {
    const { store, connections } = await openStore(50);
    wedge(firstConnection(connections));
    const failure = await store.getBlock("block-1").then(
      () => undefined,
      (error: unknown) => error,
    );
    expect(failure).toBeInstanceOf(StorageUnresponsiveError);
    expect(failure).toBeInstanceOf(UnknownOutcomeError);
    const error = failure as StorageUnresponsiveError;
    expect(error.name).toBe("StorageUnresponsiveError");
    expect(error.backend).toBe("indexeddb");
    expect(error.databaseName).toBe("unresponsive");
    expect(error.waitedMs).toBe(50);
  });

  it("refuses later calls at once rather than waiting again", async () => {
    const { store, connections } = await openStore(50);
    wedge(firstConnection(connections));
    await expect(store.getBlock("block-1")).rejects.toBeInstanceOf(StorageUnresponsiveError);
    const startedAt = Date.now();
    await expect(store.getBlock("block-2")).rejects.toBeInstanceOf(StorageUnresponsiveError);
    // The deadline is 50ms; a second wait would spend it again instead of failing from the mark.
    expect(Date.now() - startedAt).toBeLessThan(40);
  });

  it("reports the connection as unusable and the outcome as unknown", async () => {
    const { store, connections } = await openStore(50);
    wedge(firstConnection(connections));
    const failure = await store.getBlock("block-1").catch((error: unknown) => error);
    expect(classifyError(failure)).toMatchObject({
      kind: "unknown-outcome",
      mayHavePublished: true,
      retry: "after-reconcile",
      connectionUsable: false,
    });
  });

  it("never trips while the connection keeps answering", async () => {
    // The deadline measures silence, not duration: a run far longer than it must still pass.
    const { store } = await openStore(40);
    const deadline = Date.now() + 400;
    let reads = 0;
    while (Date.now() < deadline) {
      expect(await store.getBlock(`block-${String(reads)}`)).toBeUndefined();
      reads += 1;
    }
    expect(reads).toBeGreaterThan(5);
    store.close();
  });

  it("bounds opening a database that never answers", async () => {
    const never = {
      open: () =>
        ({
          addEventListener: () => undefined,
          removeEventListener: () => undefined,
          transaction: null,
          error: null,
          result: undefined,
        }) as unknown as IDBOpenDBRequest,
      deleteDatabase: () => undefined,
      databases: async () => [],
      cmp: () => 0,
    } as unknown as IDBFactory;
    await expect(
      IndexedDbBlockStore.open({ name: "wedged", indexedDB: never, unresponsiveAfterMs: 50 }),
    ).rejects.toBeInstanceOf(StorageUnresponsiveError);
  });

  it("refuses a deadline that is not a positive whole number of milliseconds", async () => {
    const indexedDB = new IDBFactory();
    await expect(
      IndexedDbBlockStore.open({ name: "bad", indexedDB, unresponsiveAfterMs: 0 }),
    ).rejects.toBeInstanceOf(RangeError);
    expect(INDEXEDDB_UNRESPONSIVE_AFTER_MS).toBeGreaterThan(0);
  });
});
