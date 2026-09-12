/**
 * Shared helpers for the IndexedDB regression suites that drive `IndexedDbBlockStore` over
 * fake-indexeddb: fixture records, raw object-store access that bypasses the adapter, and a
 * factory wrapper that observes or sabotages individual requests. Test-only: nothing here ships.
 */
import { IndexedDbBlockStore } from "./indexeddb.js";
import type { SegmentRecord, TableRecord, TransactionRecord } from "./types.js";

export const NOW = "2026-09-12T12:00:00.000Z";

export async function openStore(
  indexedDB: IDBFactory,
  name: string = crypto.randomUUID(),
  durability?: "strict" | "relaxed",
): Promise<IndexedDbBlockStore> {
  return IndexedDbBlockStore.open({ name, indexedDB, ...(durability ? { durability } : {}) });
}

export const EVENTS_TABLE: TableRecord = {
  managed: false,
  id: "events",
  name: "events",
  columns: [{ id: "value", name: "value", type: "number", nullable: false }],
  revision: 0,
  createdAt: NOW,
};

export function activeTransaction(id: string, snapshotVersion: number | null): TransactionRecord {
  return {
    id,
    ownerId: `owner-${id}`,
    expiresAt: "2026-09-12T12:30:00.000Z",
    snapshotVersion,
    pendingBlockIds: [],
    pendingSegmentIds: [],
    status: "active",
    revision: 0,
    startedAt: NOW,
    updatedAt: NOW,
    committedVersion: null,
  };
}

export function segment(
  id: string,
  transactionId: string,
  blockId: string,
  commitOrdinal = 0,
  rowIdStart = 1n,
): SegmentRecord {
  return {
    id,
    tableId: "events",
    transactionId,
    rowCount: 1,
    rowIdStart,
    rowIdEndExclusive: rowIdStart + 1n,
    columnBlockIds: { value: [blockId] },
    kind: "insert",
    level: 0,
    logicalOrder: 0,
    commitOrdinal,
    rowIdSpans: [],
    createdAt: NOW,
  };
}

async function rawOpen(indexedDB: IDBFactory, name: string): Promise<IDBDatabase> {
  return new Promise<IDBDatabase>((resolve, reject) => {
    const request = indexedDB.open(name);
    request.onsuccess = () => resolve(request.result);
    request.onerror = () => reject(request.error ?? new Error("raw IndexedDB open failed"));
  });
}

export async function readRawValue(
  indexedDB: IDBFactory,
  name: string,
  storeName: string,
  key: IDBValidKey,
): Promise<unknown> {
  const database = await rawOpen(indexedDB, name);
  try {
    const transaction = database.transaction(storeName, "readonly");
    const value = await new Promise<unknown>((resolve, reject) => {
      const request = transaction.objectStore(storeName).get(key);
      request.onsuccess = () => resolve(request.result as unknown);
      request.onerror = () => reject(request.error ?? new Error("raw IndexedDB read failed"));
    });
    await new Promise<void>((resolve, reject) => {
      transaction.oncomplete = () => resolve();
      transaction.onabort = () => reject(transaction.error ?? new Error("raw read aborted"));
    });
    return value;
  } finally {
    database.close();
  }
}

export async function readRawKeys(
  indexedDB: IDBFactory,
  name: string,
  storeName: string,
): Promise<IDBValidKey[]> {
  const database = await rawOpen(indexedDB, name);
  try {
    const transaction = database.transaction(storeName, "readonly");
    const keys = await new Promise<IDBValidKey[]>((resolve, reject) => {
      const request = transaction.objectStore(storeName).getAllKeys();
      request.onsuccess = () => resolve(request.result);
      request.onerror = () => reject(request.error ?? new Error("raw IndexedDB read failed"));
    });
    await new Promise<void>((resolve, reject) => {
      transaction.oncomplete = () => resolve();
      transaction.onabort = () => reject(transaction.error ?? new Error("raw read aborted"));
    });
    return keys;
  } finally {
    database.close();
  }
}

/**
 * Wraps a factory so every transaction and object-store request the store makes can be observed
 * or sabotaged. Each transaction is recorded as it is created, and the hook set through
 * `setHook` sees every `put`/`add`/`delete`/`get`/`getKey` request and may abort the
 * transaction or make that request fail with a QuotaExceededError.
 */
export type StoreRequestHook = (info: {
  transaction: IDBTransaction;
  storeName: string;
  method: "put" | "add" | "delete" | "get" | "getKey";
  key: IDBValidKey | undefined;
  value: unknown;
}) => "abort" | "throw-quota" | undefined;

export interface InstrumentedFactory {
  factory: IDBFactory;
  transactions: Array<{
    transaction: IDBTransaction;
    stores: string[];
    mode: IDBTransactionMode;
    options: IDBTransactionOptions | undefined;
    completed: boolean;
    aborted: boolean;
  }>;
  /** Number of requests issued per store name (put/add/delete/get/getKey). */
  requestCounts: Map<string, number>;
  setHook(hook: StoreRequestHook | undefined): void;
  reset(): void;
}

export function instrumentFactory(indexedDB: IDBFactory): InstrumentedFactory {
  const state: InstrumentedFactory = {
    factory: indexedDB,
    transactions: [],
    requestCounts: new Map(),
    setHook(hook) {
      currentHook = hook;
    },
    reset() {
      state.transactions.length = 0;
      state.requestCounts.clear();
    },
  };
  let currentHook: StoreRequestHook | undefined;
  const wrappedScanners = new WeakSet();
  /** Counts openCursor/openKeyCursor/getAll/getAllKeys/count calls and cursor steps. */
  const countScans = (target: IDBObjectStore | IDBIndex, label: string): void => {
    if (wrappedScanners.has(target)) return;
    wrappedScanners.add(target);
    for (const method of [
      "openCursor",
      "openKeyCursor",
      "getAll",
      "getAllKeys",
      "count",
    ] as const) {
      const original = (target[method] as (...args: unknown[]) => IDBRequest).bind(target);
      Object.defineProperty(target, method, {
        configurable: true,
        value: (...args: unknown[]) => {
          const key = `${label}:${method}`;
          state.requestCounts.set(key, (state.requestCounts.get(key) ?? 0) + 1);
          const request = original(...args);
          if (method === "openCursor" || method === "openKeyCursor") {
            request.addEventListener("success", () => {
              const stepKey = `${label}:cursor-steps`;
              if (request.result !== null && request.result !== undefined) {
                state.requestCounts.set(stepKey, (state.requestCounts.get(stepKey) ?? 0) + 1);
              }
            });
          }
          return request;
        },
      });
    }
  };
  // IndexedDB returns the same IDBObjectStore instance per (transaction, name); wrap it once.
  const wrappedStores = new WeakSet<IDBObjectStore>();
  const originalOpen = indexedDB.open.bind(indexedDB);
  Object.defineProperty(indexedDB, "open", {
    configurable: true,
    value: (name: string, version?: number) => {
      const request = version === undefined ? originalOpen(name) : originalOpen(name, version);
      request.addEventListener("success", () => {
        const database = request.result;
        const originalTransaction = database.transaction.bind(database);
        Object.defineProperty(database, "transaction", {
          configurable: true,
          value: (
            stores: string | string[],
            mode?: IDBTransactionMode,
            options?: IDBTransactionOptions,
          ) => {
            const transaction = originalTransaction(stores, mode, options);
            const entry = {
              transaction,
              stores: typeof stores === "string" ? [stores] : [...stores],
              mode: mode ?? "readonly",
              options,
              completed: false,
              aborted: false,
            };
            transaction.addEventListener("complete", () => {
              entry.completed = true;
            });
            transaction.addEventListener("abort", () => {
              entry.aborted = true;
            });
            state.transactions.push(entry);
            const originalObjectStore = transaction.objectStore.bind(transaction);
            Object.defineProperty(transaction, "objectStore", {
              configurable: true,
              value: (storeName: string) => {
                const store = originalObjectStore(storeName);
                if (wrappedStores.has(store)) return store;
                wrappedStores.add(store);
                countScans(store, storeName);
                const originalIndex = store.index.bind(store);
                Object.defineProperty(store, "index", {
                  configurable: true,
                  value: (indexName: string) => {
                    const index = originalIndex(indexName);
                    countScans(index, `${storeName}.${indexName}`);
                    return index;
                  },
                });
                for (const method of ["put", "add", "delete", "get", "getKey"] as const) {
                  const original = (store[method] as (...args: unknown[]) => IDBRequest).bind(
                    store,
                  );
                  Object.defineProperty(store, method, {
                    configurable: true,
                    value: (...args: unknown[]) => {
                      state.requestCounts.set(
                        storeName,
                        (state.requestCounts.get(storeName) ?? 0) + 1,
                      );
                      const [first, second] = args;
                      const isWrite = method === "put" || method === "add";
                      const decision = currentHook?.({
                        transaction,
                        storeName,
                        method,
                        key: (isWrite ? second : first) as IDBValidKey | undefined,
                        value: isWrite ? first : undefined,
                      });
                      if (decision === "throw-quota") {
                        // Faithful failure: fake-indexeddb runs the queued `operation` later,
                        // exactly where a browser would hit the quota. Replacing it makes the
                        // request fail with QuotaExceededError, which aborts the transaction
                        // with that error and fails every later request with AbortError, the
                        // sequence the IndexedDB spec prescribes.
                        const failing = original(...args);
                        const queue = (
                          transaction as unknown as {
                            _requests: Array<{ operation: () => unknown; request: IDBRequest }>;
                          }
                        )._requests;
                        const entry = queue.find((candidate) => candidate.request === failing);
                        if (entry === undefined) throw new Error("request not queued");
                        entry.operation = () => {
                          throw new DOMException(
                            "The quota has been exceeded.",
                            "QuotaExceededError",
                          );
                        };
                        return failing;
                      }
                      const result = original(...args);
                      if (decision === "abort") {
                        try {
                          transaction.abort();
                        } catch {
                          // already finished
                        }
                      }
                      return result;
                    },
                  });
                }
                return store;
              },
            });
            return transaction;
          },
        });
      });
      return request;
    },
  });
  return state;
}

export async function stageBlocks(
  store: IndexedDbBlockStore,
  record: TransactionRecord,
  ids: readonly string[],
): Promise<TransactionRecord> {
  let current = record;
  for (let start = 0; start < ids.length; start += 64) {
    const slice = ids.slice(start, start + 64);
    current = await store.stageTransactionArtifacts({
      transactionId: record.id,
      expectedRevision: current.revision,
      blocks: slice.map((id) => ({ id, bytes: Uint8Array.of(1) })),
      segments: [],
      updatedAt: NOW,
    });
  }
  return current;
}

export async function stageSegments(
  store: IndexedDbBlockStore,
  record: TransactionRecord,
  ids: readonly string[],
  blockId: string,
): Promise<TransactionRecord> {
  let current = record;
  for (let start = 0; start < ids.length; start += 64) {
    const slice = ids.slice(start, start + 64);
    current = await store.stageTransactionArtifacts({
      transactionId: record.id,
      expectedRevision: current.revision,
      blocks: [],
      segments: slice.map((id, offset) =>
        segment(
          id,
          record.id,
          blockId,
          current.pendingSegmentIds.length + offset,
          BigInt(current.pendingSegmentIds.length + offset + 1),
        ),
      ),
      updatedAt: NOW,
    });
  }
  return current;
}
