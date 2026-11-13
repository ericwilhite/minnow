/** R25.5: opening data/log depends on their boundaries, never physical record count. */
import { IDBFactory, IDBKeyRange } from "fake-indexeddb";
import { expect, it } from "vitest";
import { openIndexedDb, type IdbDatabaseLike, type IdbStoreLike } from "./idb-like.js";
import { createIndexedDbBackend } from "./indexeddb.js";
import { SimLocks } from "./sim-disk.js";

(globalThis as { IDBKeyRange?: unknown }).IDBKeyRange ??= IDBKeyRange;

it.each([0, 1, 129, 8193, 32769])(
  "reopens %i records in each byte space with two boundary lookups and no payload",
  async (records) => {
    const factory = new IDBFactory();
    const raw = await openIndexedDb(factory, "scale");
    await raw.transaction(
      ["data", "data-index", "log", "log-index"],
      "readwrite",
      "strict",
      (tx) => {
        // fake-indexeddb shifts its pending-request array. Enqueuing 131,076 requests at
        // once makes fixture creation quadratic; keep the same single transaction and data,
        // but issue the next batch from a request callback while the transaction is active.
        const names = ["data", "log"] as const;
        let space = 0,
          at = 0;
        const seedBatch = () => {
          const name = names[space];
          if (name === undefined || records === 0) return;
          const end = Math.min(records, at + 512);
          for (; at < end; at++) {
            const offset = at * 4096;
            tx.store(name).put(offset, { offset, bytes: new ArrayBuffer(8) });
            tx.store(`${name}-index`).put(offset, { offset, length: 8 });
          }
          tx.store(`${name}-index`).get((at - 1) * 4096, () => {
            if (at === records) {
              space++;
              at = 0;
            }
            seedBatch();
          });
        };
        seedBatch();
      },
    );
    // Fixture checks are outside the reopen counters. Prove batching kept every key in all
    // four stores, including the 32,769-record case that exceeded the setup deadline.
    await raw.transaction(
      ["data", "data-index", "log", "log-index"],
      "readonly",
      "relaxed",
      (tx) => {
        for (const name of ["data", "data-index", "log", "log-index"])
          tx.store(name).getAllKeys({}, (keys) => {
            expect(keys).toHaveLength(records);
            expect(keys.every((key, index) => key === index * 4096)).toBe(true);
          });
      },
    );
    raw.close();
    const requests: string[] = [];
    const roots: string[] = [];
    const values: unknown[] = [];
    const observe = (name: string, method: string, then: (value: unknown) => void) => {
      if (name === "roots") roots.push(`${name}.${method}`);
      else if (name !== "meta") requests.push(`${name}.${method}`);
      return (value: unknown) => {
        if (name !== "meta" && name !== "roots" && value !== undefined) {
          const found: unknown[] = Array.isArray(value) ? value : [value];
          values.push(...found);
        }
        then(value);
      };
    };
    const wrap = (name: string, store: IdbStoreLike): IdbStoreLike => ({
      ...store,
      get: (key, then) => store.get(key, observe(name, "get", then)),
      last: (range, then) => store.last(range, observe(name, "last", then)),
      getAll: (range, then, count) =>
        store.getAll(range, (found) => observe(name, "getAll", () => then(found))(found), count),
      getAllKeys: (range, then, count) =>
        store.getAllKeys(
          range,
          (found) => observe(name, "getAllKeys", () => then(found))(found),
          count,
        ),
    });
    const backend = createIndexedDbBackend({
      name: "scale",
      locks: new SimLocks(),
      database: async (): Promise<IdbDatabaseLike> => {
        const db = await openIndexedDb(factory, "scale");
        return {
          close: () => db.close(),
          transaction: (stores, mode, durability, body) =>
            db.transaction(stores, mode, durability, (tx) =>
              body({
                store: (name) => wrap(name, tx.store(name)),
                abort: (error) => tx.abort(error),
              }),
            ),
        };
      },
    });
    const lease = await backend.acquire();
    try {
      const session = await backend.open(lease);
      expect(await session.dataLength()).toBe(Math.max(0, (records - 1) * 4096 + 8));
      expect(session.logLength()).toBe(Math.max(0, (records - 1) * 4096 + 8));
      // Count point requests too: a loop of get()/last() must fail just like bulk enumeration.
      // Fixed pre-fence version checks are independent of data/log record count.
      expect(roots).toEqual(["roots.get", "roots.get"]);
      expect(requests.length).toBeLessThanOrEqual(2);
      expect(values.length).toBeLessThanOrEqual(2);
      for (const value of values)
        expect(value).toEqual({ offset: (records - 1) * 4096, length: 8 });
      await session.close();
    } finally {
      await lease.release();
    }
  },
  // Fixture population is not the latency guard; native 2 GiB timings are measured separately.
  60_000,
);
