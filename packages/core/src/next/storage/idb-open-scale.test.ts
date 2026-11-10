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
        for (const name of ["data", "log"])
          for (let i = 0; i < records; i++) {
            const offset = i * 4096;
            tx.store(name).put(offset, { offset, bytes: new ArrayBuffer(8) });
            tx.store(`${name}-index`).put(offset, { offset, length: 8 });
          }
      },
    );
    raw.close();
    const requests: string[] = [];
    const values: unknown[] = [];
    const observe = (name: string, method: string, then: (value: unknown) => void) => {
      if (name !== "meta") requests.push(`${name}.${method}`);
      return (value: unknown) => {
        if (name !== "meta" && value !== undefined) {
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
