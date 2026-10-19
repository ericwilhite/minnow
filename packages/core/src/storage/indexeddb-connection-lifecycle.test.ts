/**
 * What an IndexedDB connection's callers see when the connection ends under them: a call parked
 * on a transaction that never answers still fails at the unresponsive deadline after close();
 * a connection a newer build's schema upgrade closed reports itself superseded and lets the
 * upgrade through; a database a newer build laid out is refused untouched; and a write whose
 * completion arrives after the deadline is an unknown outcome, not a failure.
 */
import { IDBFactory } from "fake-indexeddb";
import { describe, expect, it, vi } from "vitest";
import { classifyError } from "../engine/errors.js";
import { EVENTS_TABLE } from "./indexeddb-audit-helpers.js";
import { IndexedDbBlockStore } from "./indexeddb.js";
import {
  ConnectionLostError,
  StorageConnectionClosedError,
  StorageFormatVersionError,
  StorageUnresponsiveError,
  UnknownOutcomeError,
} from "./types.js";

vi.setConfig({ testTimeout: 60_000 });

/** A fake-indexeddb factory that also hands back every connection it opens. */
function observedFactory(): { indexedDB: IDBFactory; connections: IDBDatabase[] } {
  const base = new IDBFactory();
  const connections: IDBDatabase[] = [];
  const indexedDB = {
    open: (name: string, version?: number) => {
      const request = version === undefined ? base.open(name) : base.open(name, version);
      request.addEventListener("success", () => connections.push(request.result), { once: true });
      return request;
    },
    deleteDatabase: (name: string) => base.deleteDatabase(name),
    databases: () => base.databases(),
    cmp: (first: IDBValidKey, second: IDBValidKey) => base.cmp(first, second),
  };
  return { indexedDB, connections };
}

/** Real transactions whose own events never reach the adapter; their requests still answer. */
function silenceTransactions(db: IDBDatabase): void {
  const original = db.transaction.bind(db);
  Object.defineProperty(db, "transaction", {
    configurable: true,
    value: (...args: Parameters<IDBDatabase["transaction"]>) => {
      const transaction = original(...args);
      return new Proxy(transaction, {
        get(target, property) {
          if (property === "addEventListener") return () => undefined;
          const value: unknown = Reflect.get(target, property, target);
          if (typeof value !== "function") return value;
          return (...inner: unknown[]): unknown => Reflect.apply(value, target, inner);
        },
      });
    },
  });
}

/** Readwrite transactions whose `complete` reaches the adapter `delayMs` late. */
function delayCompletion(db: IDBDatabase, delayMs: number): void {
  const original = db.transaction.bind(db);
  Object.defineProperty(db, "transaction", {
    configurable: true,
    value: (...args: Parameters<IDBDatabase["transaction"]>) => {
      const transaction = original(...args);
      if (args[1] !== "readwrite") return transaction;
      const add = transaction.addEventListener.bind(transaction);
      return new Proxy(transaction, {
        get(target, property) {
          if (property === "addEventListener") {
            return (type: string, listener: EventListener, options?: AddEventListenerOptions) => {
              if (type !== "complete") return add(type, listener, options);
              return add(
                type,
                (event) => {
                  setTimeout(() => listener(event), delayMs);
                },
                options,
              );
            };
          }
          const value: unknown = Reflect.get(target, property, target);
          if (typeof value !== "function") return value;
          return (...inner: unknown[]): unknown => Reflect.apply(value, target, inner);
        },
      });
    },
  });
}

function nativeVersion(indexedDB: IDBFactory, name: string): Promise<number> {
  return new Promise((resolve, reject) => {
    const request = indexedDB.open(name);
    request.onsuccess = () => {
      const version = request.result.version;
      request.result.close();
      resolve(version);
    };
    request.onerror = () => reject(request.error ?? new Error("native open failed"));
  });
}

async function settles(promise: Promise<unknown>, withinMs: number): Promise<string> {
  return Promise.race([
    promise.then(
      () => "resolved",
      (error: unknown) => (error instanceof Error ? error.name : "rejected"),
    ),
    new Promise<string>((resolve) => setTimeout(() => resolve("hang"), withinMs)),
  ]);
}

describe("an IndexedDB connection that ends under its callers", () => {
  it("a call parked on a silent transaction when close() runs still fails at the deadline", async () => {
    const { indexedDB, connections } = observedFactory();
    const store = await IndexedDbBlockStore.open({
      name: "closing",
      indexedDB,
      unresponsiveAfterMs: 200,
    });
    const connection = connections[0];
    if (connection === undefined) throw new Error("no connection");
    silenceTransactions(connection);
    const pending = store.getCurrentManifestVersion();
    // Let the request answer, so the call is waiting on the transaction's completion alone.
    await new Promise((resolve) => setTimeout(resolve, 50));
    store.close();
    expect(await settles(pending, 2_000)).toBe("StorageUnresponsiveError");
    const failure = await pending.catch((error: unknown) => error);
    expect(failure).toBeInstanceOf(UnknownOutcomeError);
    // Once the deadline tripped, the connection is unresponsive, and later calls say so at once.
    const after = await store.getCurrentManifestVersion().catch((error: unknown) => error);
    expect(after).toBeInstanceOf(StorageUnresponsiveError);
  });

  it("a newer build's schema upgrade is not blocked, and the superseded connection says so", async () => {
    const indexedDB = new IDBFactory();
    const name = "superseded";
    const older = await IndexedDbBlockStore.open({ name, indexedDB });
    const current = await nativeVersion(indexedDB, name);
    // A newer deployment in another tab requests the next schema version.
    const blocked = vi.fn();
    await new Promise<void>((resolve, reject) => {
      const request = indexedDB.open(name, current + 1);
      request.onblocked = blocked;
      request.onupgradeneeded = () => undefined;
      request.onsuccess = () => {
        request.result.close();
        resolve();
      };
      request.onerror = () => reject(request.error ?? new Error("upgrade failed"));
    });
    expect(blocked).not.toHaveBeenCalled();
    expect(await nativeVersion(indexedDB, name)).toBe(current + 1);
    const failure = await older.getCurrentManifestVersion().catch((error: unknown) => error);
    expect(failure).toBeInstanceOf(StorageConnectionClosedError);
    expect(failure).toBeInstanceOf(ConnectionLostError);
    expect(failure).toMatchObject({
      backend: "indexeddb",
      databaseName: name,
      reason: "superseded",
    });
    expect((failure as Error).message).toMatch(/reload the page/);
    expect(classifyError(failure)).toMatchObject({
      kind: "connection-lost",
      connectionUsable: false,
    });
    // Closing again is harmless and keeps the reason.
    older.close();
    const again = await older.listTables().catch((error: unknown) => error);
    expect((again as StorageConnectionClosedError).reason).toBe("superseded");
  });

  it('a connection the application closed reports reason "closed"', async () => {
    const indexedDB = new IDBFactory();
    const store = await IndexedDbBlockStore.open({ name: "closed", indexedDB });
    store.close();
    const failure = await store.listTables().catch((error: unknown) => error);
    expect(failure).toBeInstanceOf(StorageConnectionClosedError);
    expect((failure as StorageConnectionClosedError).reason).toBe("closed");
  });

  it("refuses a database a newer build laid out and leaves it at that schema", async () => {
    const indexedDB = new IDBFactory();
    const name = "future";
    (await IndexedDbBlockStore.open({ name, indexedDB })).close();
    const current = await nativeVersion(indexedDB, name);
    await new Promise<void>((resolve, reject) => {
      const request = indexedDB.open(name, current + 1);
      request.onupgradeneeded = () => {
        request.result.createObjectStore("future-store");
      };
      request.onsuccess = () => {
        request.result.close();
        resolve();
      };
      request.onerror = () => reject(request.error ?? new Error("bump failed"));
    });
    const failure = await IndexedDbBlockStore.open({ name, indexedDB }).catch(
      (error: unknown) => error,
    );
    expect(failure).toBeInstanceOf(StorageFormatVersionError);
    expect(await nativeVersion(indexedDB, name)).toBe(current + 1);
  });

  it("a write whose completion arrives after the deadline is an unknown outcome, and durable", async () => {
    const { indexedDB, connections } = observedFactory();
    const store = await IndexedDbBlockStore.open({
      name: "late",
      indexedDB,
      unresponsiveAfterMs: 300,
    });
    const connection = connections[0];
    if (connection === undefined) throw new Error("no connection");
    delayCompletion(connection, 1_000);
    const failure = await store.addTable(EVENTS_TABLE).catch((error: unknown) => error);
    expect(failure).toBeInstanceOf(StorageUnresponsiveError);
    expect(failure).toBeInstanceOf(UnknownOutcomeError);
    expect(classifyError(failure)).toMatchObject({
      kind: "unknown-outcome",
      mayHavePublished: true,
    });
    await new Promise((resolve) => setTimeout(resolve, 1_200));
    const reopened = await IndexedDbBlockStore.open({ name: "late", indexedDB });
    try {
      expect((await reopened.getTable(EVENTS_TABLE.id))?.name).toBe("events");
    } finally {
      reopened.close();
    }
  });
});
