/**
 * The slice of IndexedDB the IndexedDB backend (indexeddb.ts) uses, shaped like the platform so
 * its rules hold for both implementations: the real one (`openIndexedDb`, below) and the
 * simulated one with a crash model (sim-idb.ts), which the storage kit runs.
 *
 * ## Rules (the platform's)
 * - A transaction's requests are issued from its body and from its requests' callbacks only; it
 *   commits when the last callback returns with nothing pending (no awaits on other work).
 * - Read-write transactions whose scopes overlap run one at a time, in creation order.
 * - `strict` durability: when the transaction completes, it and every transaction committed before
 *   it are durable. `relaxed`: durable at the next strict commit (one write-ahead log per origin
 *   database: Chromium's LevelDB, Firefox's and WebKit's SQLite). A transaction that changed
 *   nothing need not flush, so callers that need a barrier write something.
 * - Requests execute in the order they are issued; a read sees every earlier request of its
 *   transaction and none after. Values cross a structured-clone boundary at `put` and at every
 *   read. A request issued while the transaction is not active (outside its body and its
 *   callbacks, or after it finished) throws `TransactionInactiveError`; an empty key range throws
 *   `DataError`; a write in a read-only transaction throws `ReadOnlyError`. `idb-scenarios.ts` pins
 *   these, and the browser suite checks the simulator against the platform on them.
 * - An exception in a callback, or `abort(reason)`, aborts the transaction: nothing it did
 *   applies, and the promise rejects with the reason. A quota failure rejects with
 *   `StoreQuotaError`.
 *
 * ## Values
 * Numeric keys for byte records (offset → `{ offset, bytes }`), string keys for small records.
 * Bytes are stored as exact-size `ArrayBuffer`s, never Blobs (Firefox writes Blobs at 8 MB/s with
 * one fsync each) and never views (structured clone would copy the whole underlying buffer).
 */
import { isLegacyIndexedDb, LegacyDataError, refuseLegacyIndexedDb } from "./legacy-data.js";
import { StoreQuotaError } from "./contract.js";

export type IdbKey = number | string;

/** A numeric key range; an omitted bound is open-ended. */
export interface IdbRange {
  readonly lower?: number;
  readonly upper?: number;
  readonly lowerOpen?: boolean;
  readonly upperOpen?: boolean;
}

/** A byte record: `bytes` stored at data-space (or log, or scratch) offset `offset`. */
export interface ByteRecord {
  readonly offset: number;
  readonly bytes: ArrayBuffer;
}

export interface IdbStoreLike {
  get(key: IdbKey, then: (value: unknown) => void): void;
  /** Values with numeric keys in `range`, in key order (at most `count` when given). */
  getAll(range: IdbRange, then: (values: unknown[]) => void, count?: number): void;
  /** Keys in `range`, in key order (no values are read; at most `count` when given). */
  getAllKeys(range: IdbRange, then: (keys: IdbKey[]) => void, count?: number): void;
  /** The value with the highest key in `range` (or in the store), or undefined. */
  last(range: IdbRange | null, then: (value: unknown) => void): void;
  put(key: IdbKey, value: unknown): void;
  delete(key: IdbKey): void;
  deleteRange(range: IdbRange): void;
  clear(): void;
}

export interface IdbTransactionLike {
  store(name: string): IdbStoreLike;
  /** Aborts: the transaction's promise rejects with `reason`. */
  abort(reason: Error): void;
}

export interface IdbDatabaseLike {
  transaction(
    stores: readonly string[],
    mode: "readonly" | "readwrite",
    durability: "strict" | "relaxed",
    body: (tx: IdbTransactionLike) => void,
  ): Promise<void>;
  close(): void;
}

/** The object stores of one database. */
export const IDB_STORES = [
  "meta",
  "roots",
  "data",
  "data-index",
  "log",
  "log-index",
  "spill",
  "spill-index",
] as const;
export const IDB_VERSION = 2;

const errorName = (error: unknown): string =>
  typeof error === "object" && error !== null && "name" in error ? String(error.name) : "";

function keyRange(range: IdbRange | null): IDBKeyRange | null {
  if (range === null) return null;
  const { lower, upper, lowerOpen = false, upperOpen = false } = range;
  if (lower !== undefined && upper !== undefined)
    return IDBKeyRange.bound(lower, upper, lowerOpen, upperOpen);
  if (lower !== undefined) return IDBKeyRange.lowerBound(lower, lowerOpen);
  if (upper !== undefined) return IDBKeyRange.upperBound(upper, upperOpen);
  return null;
}

/** Opens (creating if needed) the database `name` on the platform's IndexedDB. */
export async function openIndexedDb(factory: IDBFactory, name: string): Promise<IdbDatabaseLike> {
  // Unversioned inspection precedes the native schema upgrade, including direct callers.
  await refuseLegacyIndexedDb(factory, name);
  const database = await new Promise<IDBDatabase>((resolve, reject) => {
    let failure: LegacyDataError | null = null;
    let settled = false;
    const request = factory.open(name, IDB_VERSION);
    request.onupgradeneeded = () => {
      // An old writer may have created the database since the preflight. Abort before edits.
      if (isLegacyIndexedDb(Array.from(request.result.objectStoreNames))) {
        failure = new LegacyDataError("indexeddb", name);
        request.transaction?.abort();
        return;
      }
      if (settled) {
        request.transaction?.abort();
        return;
      }
      for (const store of IDB_STORES)
        if (!request.result.objectStoreNames.contains(store))
          request.result.createObjectStore(store);
    };
    request.onsuccess = () => {
      if (isLegacyIndexedDb(Array.from(request.result.objectStoreNames)))
        failure = new LegacyDataError("indexeddb", name);
      if (settled || failure !== null) request.result.close();
      if (!settled) {
        if (failure !== null) reject(failure);
        else resolve(request.result);
      }
      settled = true;
    };
    request.onerror = () => {
      const cause = failure ?? request.error ?? new Error(`Cannot open IndexedDB ${name}`);
      if (!settled) {
        // A racing old writer at I3–I8 can reject the native v2 open before upgrade fires.
        if (errorName(cause) === "VersionError")
          void refuseLegacyIndexedDb(factory, name).then(() => reject(cause), reject);
        else reject(cause);
      }
      settled = true;
    };
    request.onblocked = (event) => {
      settled = true;
      // The native schema is v2. A blocked upgrade from v1 is the I1 legacy marker,
      // including a writer that won the race after preflight and kept its connection.
      // Another open would queue behind this blocked request and never inspect it.
      reject(
        event.oldVersion === 1
          ? new LegacyDataError("indexeddb", name)
          : new Error(`IndexedDB ${name} is blocked by an open connection of another version`),
      );
    };
  });
  return {
    transaction(stores, mode, durability, body) {
      return new Promise<void>((resolve, reject) => {
        let failure: Error | null = null;
        const tx = database.transaction([...stores], mode, { durability });
        const fail = (error: unknown) => {
          failure ??= error instanceof Error ? error : new Error(String(error));
          try {
            tx.abort();
          } catch {
            // Already finished.
          }
        };
        /** Runs a request callback; an exception aborts the transaction. */
        const guarded =
          <T>(then: (value: T) => void) =>
          (value: T) => {
            try {
              then(value);
            } catch (error) {
              fail(error);
            }
          };
        tx.oncomplete = () => {
          resolve();
        };
        tx.onabort = () => {
          const cause = failure ?? tx.error;
          reject(
            errorName(cause) === "QuotaExceededError"
              ? new StoreQuotaError(`IndexedDB refused space (${errorName(cause)})`)
              : (cause ?? new Error("IndexedDB transaction aborted")),
          );
        };
        const wrap = (name: string): IdbStoreLike => {
          const store = tx.objectStore(name);
          return {
            get(key, then) {
              const request = store.get(key);
              request.onsuccess = () => guarded(then)(request.result);
            },
            getAll(range, then, count) {
              const request = store.getAll(keyRange(range), count);
              request.onsuccess = () => guarded(then)(request.result);
            },
            getAllKeys(range, then, count) {
              const request = store.getAllKeys(keyRange(range), count);
              request.onsuccess = () => guarded(then)(request.result as IdbKey[]);
            },
            last(range, then) {
              const request = store.openCursor(keyRange(range), "prev");
              request.onsuccess = () => guarded(then)(request.result?.value);
            },
            put(key, value) {
              store.put(value, key);
            },
            delete(key) {
              store.delete(key);
            },
            deleteRange(range) {
              const bounds = keyRange(range);
              if (bounds === null) store.clear();
              else store.delete(bounds);
            },
            clear() {
              store.clear();
            },
          };
        };
        try {
          body({ store: wrap, abort: fail });
        } catch (error) {
          fail(error);
        }
      });
    },
    close() {
      database.close();
    },
  };
}
