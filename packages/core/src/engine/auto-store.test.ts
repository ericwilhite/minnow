import { IDBFactory } from "fake-indexeddb";
import { afterEach, expect, it } from "vitest";
import { autoStoreTestHooks, forgetStoreChoice, resolveAutoStoreKind } from "./auto-store.js";
import { MinnowDatabaseClient } from "./client.js";
import { DatabaseStoreUnavailableError } from "./errors.js";
import { attachDatabaseWorker, type RpcScope } from "./worker-host.js";
import { attachWorkerHost } from "./worker-server.js";
import { autoWorkerStore } from "./worker-store-auto.js";

/**
 * `{ kind: "auto" }` picks OPFS where the worker can hold synchronous access handles and
 * IndexedDB elsewhere, remembers the choice per database name, and refuses to reopen a database
 * on the other store — where it would be empty — when the remembered one cannot open.
 */

const originalIndexedDb = (globalThis as { indexedDB?: IDBFactory }).indexedDB;

afterEach(() => {
  delete autoStoreTestHooks.opfsAvailable;
  delete autoStoreTestHooks.indexedDB;
  if (originalIndexedDb === undefined) delete (globalThis as { indexedDB?: IDBFactory }).indexedDB;
  else (globalThis as { indexedDB?: IDBFactory }).indexedDB = originalIndexedDb;
});

type Listener = (event: MessageEvent<unknown>) => void;

interface Side {
  listeners: Set<Listener>;
  peer?: Side;
  scope: {
    addEventListener: (type: string, listener: Listener) => void;
    removeEventListener: (type: string, listener: Listener) => void;
    postMessage: (message: unknown) => void;
  };
}

function makeSide(): Side {
  const side: Side = {
    listeners: new Set(),
    scope: {
      addEventListener: (type, listener) => {
        if (type === "message") side.listeners.add(listener);
      },
      removeEventListener: (type, listener) => {
        if (type === "message") side.listeners.delete(listener);
      },
      postMessage: (message) => {
        const target = side.peer;
        if (target === undefined) return;
        queueMicrotask(() => {
          for (const listener of target.listeners) {
            listener({ data: structuredClone(message) } as MessageEvent<unknown>);
          }
        });
      },
    },
  };
  return side;
}

/** Two ends of a worker message channel, in one thread. */
function boundary(): { clientSide: Worker; workerSide: RpcScope } {
  const client = makeSide();
  const worker = makeSide();
  client.peer = worker;
  worker.peer = client;
  return {
    clientSide: client.scope as unknown as Worker,
    workerSide: worker.scope,
  };
}

it("chooses OPFS when the probe allows it, remembers it, and refuses to fall back later", async () => {
  autoStoreTestHooks.indexedDB = new IDBFactory();
  autoStoreTestHooks.opfsAvailable = async () => true;
  expect(await resolveAutoStoreKind("shop")).toEqual({ kind: "opfs", reserved: true });
  // The probe is not consulted for a remembered choice that still holds, and nothing is reserved.
  expect(await resolveAutoStoreKind("shop")).toEqual({ kind: "opfs", reserved: false });
  autoStoreTestHooks.opfsAvailable = async () => false;
  await expect(resolveAutoStoreKind("shop")).rejects.toBeInstanceOf(DatabaseStoreUnavailableError);
  await expect(resolveAutoStoreKind("shop")).rejects.toMatchObject({
    store: "opfs",
    databaseName: "shop",
  });
  // Another name decides for itself; forgetting the first lets it decide again.
  expect((await resolveAutoStoreKind("other")).kind).toBe("indexeddb");
  await forgetStoreChoice("shop");
  expect((await resolveAutoStoreKind("shop")).kind).toBe("indexeddb");
});

it("keeps a database on IndexedDB once it started there, even when OPFS appears later", async () => {
  autoStoreTestHooks.indexedDB = new IDBFactory();
  autoStoreTestHooks.opfsAvailable = async () => false;
  expect((await resolveAutoStoreKind("shop")).kind).toBe("indexeddb");
  autoStoreTestHooks.opfsAvailable = async () => true;
  expect((await resolveAutoStoreKind("shop")).kind).toBe("indexeddb");
});

it("probes every time when nothing can remember the choice", async () => {
  delete (globalThis as { indexedDB?: IDBFactory }).indexedDB;
  let probes = 0;
  autoStoreTestHooks.opfsAvailable = async () => {
    probes += 1;
    return true;
  };
  expect((await resolveAutoStoreKind("shop")).kind).toBe("opfs");
  expect((await resolveAutoStoreKind("shop")).kind).toBe("opfs");
  expect(probes).toBe(2);
  await expect(forgetStoreChoice("shop")).resolves.toBeUndefined();
});

it("opens the resolved store through the stock worker entry and reports it to the client", async () => {
  const factory = new IDBFactory();
  autoStoreTestHooks.indexedDB = factory;
  (globalThis as { indexedDB?: IDBFactory }).indexedDB = factory;
  autoStoreTestHooks.opfsAvailable = async () => false;
  const ends = boundary();
  attachDatabaseWorker(ends.workerSide);
  const client = new MinnowDatabaseClient(ends.clientSide, {
    store: { kind: "auto", name: "auto-stock", indexeddb: { durability: "relaxed" } },
  });
  try {
    expect(await client.storeKind()).toBe("indexeddb");
    await client.execute('CREATE TABLE "t" ("id" INTEGER PRIMARY KEY, "v" TEXT)');
    await client.execute('INSERT INTO "t" ("id", "v") VALUES (1, \'one\')');
    expect((await client.query('SELECT "v" FROM "t"')).rows).toEqual([{ v: "one" }]);
  } finally {
    await client.close();
  }
  // The choice outlives the connection: the next open of this name goes to the same store.
  expect((await resolveAutoStoreKind("auto-stock")).kind).toBe("indexeddb");
});

it("bundles both durable stores in the auto entry and refuses the memory store", async () => {
  const factory = new IDBFactory();
  autoStoreTestHooks.indexedDB = factory;
  (globalThis as { indexedDB?: IDBFactory }).indexedDB = factory;
  autoStoreTestHooks.opfsAvailable = async () => false;
  const ends = boundary();
  attachWorkerHost(ends.workerSide, autoWorkerStore);
  const memory = new MinnowDatabaseClient(ends.clientSide, { store: { kind: "memory" } });
  await expect(memory.ready()).rejects.toThrow(/bundles only the OPFS-or-IndexedDB store/);
  await memory.close().catch(() => undefined);
  const named = boundary();
  attachWorkerHost(named.workerSide, autoWorkerStore);
  const explicit = new MinnowDatabaseClient(named.clientSide, {
    store: { kind: "indexeddb", name: "auto-entry-explicit" },
  });
  try {
    expect(await explicit.storeKind()).toBe("indexeddb");
  } finally {
    await explicit.close();
  }
});
