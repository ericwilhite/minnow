/** R25.5: each index page's three bulk requests are queued before any completes. */
import { IDBFactory, IDBKeyRange } from "fake-indexeddb";
import { expect, it } from "vitest";
import { ByteStore, LOAD_PAGE } from "./idb-bytes.js";
import { openIndexedDb, type IdbDatabaseLike, type IdbStoreLike } from "./idb-like.js";

(globalThis as { IDBKeyRange?: unknown }).IDBKeyRange ??= IDBKeyRange;

it("queues index values, index keys and record keys together on every page", async () => {
  const db = await openIndexedDb(new IDBFactory(), "bulk-pages");
  await db.transaction(["data", "data-index"], "readwrite", "strict", (tx) => {
    for (let index = 0; index < LOAD_PAGE + 1; index++) {
      const offset = index * 4096;
      tx.store("data").put(offset, { offset, bytes: new ArrayBuffer(1) });
      tx.store("data-index").put(offset, { offset, length: 1 });
    }
  });
  let issued = 0;
  let completed = 0;
  let transactions = 0;
  const widths: number[] = [];
  const complete = () => {
    if (completed === 0) widths.push(issued);
    if (++completed === 3) {
      issued = 0;
      completed = 0;
    }
  };
  const wrap = (store: IdbStoreLike): IdbStoreLike => ({
    get: store.get.bind(store),
    last: store.last.bind(store),
    put: store.put.bind(store),
    delete: store.delete.bind(store),
    deleteRange: store.deleteRange.bind(store),
    clear: store.clear.bind(store),
    getAll(range, then, count) {
      issued++;
      store.getAll(
        range,
        (values) => {
          complete();
          then(values);
        },
        count,
      );
    },
    getAllKeys(range, then, count) {
      issued++;
      store.getAllKeys(
        range,
        (keys) => {
          complete();
          then(keys);
        },
        count,
      );
    },
  });
  const tracked: IdbDatabaseLike = {
    close: () => db.close(),
    transaction(stores, mode, durability, body) {
      transactions++;
      return db.transaction(stores, mode, durability, (tx) =>
        body({
          store: (name) => wrap(tx.store(name)),
          abort: (reason) => tx.abort(reason),
        }),
      );
    },
  };
  try {
    const loaded = await ByteStore.load(tracked, "data");
    expect(loaded.records).toBe(LOAD_PAGE + 1);
    expect(transactions).toBe(1);
    expect(widths).toEqual([3, 3]);
    loaded.close();
  } finally {
    db.close();
  }
});
