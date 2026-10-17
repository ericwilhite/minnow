/**
 * What an ordinary single-row commit reads on IndexedDB. Every request inside the publishing
 * transaction is a round trip to the browser's storage process, so a commit that reads in
 * proportion to the database pays for its size on every write. These count the requests and
 * cursor steps issued inside readwrite transactions while a single-row UPDATE runs, on databases
 * of different sizes.
 */
import { IDBCursor, IDBFactory, IDBIndex, IDBObjectStore } from "fake-indexeddb";
import { afterEach, expect, it, vi } from "vitest";
import { MinnowDatabase } from "../engine/database.js";
import { IndexedDbBlockStore } from "./indexeddb.js";

afterEach(() => {
  vi.restoreAllMocks();
});

function owningStore(source: IDBObjectStore | IDBIndex): IDBObjectStore {
  return source instanceof IDBIndex ? source.objectStore : source;
}

/** Counts requests and cursor steps issued inside readwrite transactions, by `store.method`. */
function countReadwriteRequests(): Map<string, number> {
  const counts = new Map<string, number>();
  const count = (store: IDBObjectStore, method: string): void => {
    if (store.transaction.mode !== "readwrite") return;
    const key = `${store.name}.${method}`;
    counts.set(key, (counts.get(key) ?? 0) + 1);
  };
  const methods = [
    "get",
    "getAll",
    "getAllKeys",
    "getKey",
    "openCursor",
    "openKeyCursor",
    "count",
  ] as const;
  for (const method of methods) {
    for (const prototype of [IDBObjectStore.prototype, IDBIndex.prototype]) {
      // eslint-disable-next-line @typescript-eslint/unbound-method -- Called with its native receiver below.
      const original = prototype[method] as (...args: unknown[]) => IDBRequest;
      vi.spyOn(prototype, method).mockImplementation(function (
        this: IDBObjectStore | IDBIndex,
        ...args: unknown[]
      ) {
        count(owningStore(this), method);
        return original.apply(this, args);
      } as never);
    }
  }
  // eslint-disable-next-line @typescript-eslint/unbound-method -- Called with its native receiver below.
  const advance = IDBCursor.prototype.continue;
  vi.spyOn(IDBCursor.prototype, "continue").mockImplementation(function (
    this: IDBCursor,
    ...args: unknown[]
  ) {
    count(owningStore(this.source), "continue");
    return advance.apply(this, args as never);
  });
  return counts;
}

async function commitRequests(batches: number): Promise<Map<string, number>> {
  const store = await IndexedDbBlockStore.open({
    name: crypto.randomUUID(),
    indexedDB: new IDBFactory(),
  });
  const db = new MinnowDatabase(store, { autoCompact: false, autoCollect: false });
  try {
    await db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, amount INTEGER NOT NULL)");
    // Each batch is its own segment and blocks: the database's block count grows with it.
    for (let batch = 0; batch < batches; batch += 1) {
      await db.insertBatch(
        "t",
        Array.from({ length: 10 }, (_, index) => ({ id: batch * 10 + index, amount: 0 })),
      );
    }
    await db.query("SELECT amount FROM t WHERE id = 1", { memoize: false });
    await db.execute("UPDATE t SET amount = amount + 1 WHERE id = 1");
    // The read pins the shared reader lease at this version, so the commit below publishes past
    // a pinned version: the case whose admission check walked every manifest-block record.
    await db.query("SELECT amount FROM t WHERE id = 2", { memoize: false });
    const counts = countReadwriteRequests();
    await db.execute("UPDATE t SET amount = amount + 1 WHERE id = 2");
    vi.restoreAllMocks();
    return counts;
  } finally {
    await db.close();
  }
}

it("an ordinary commit reads the same however many blocks the database holds", async () => {
  const small = await commitRequests(4);
  const large = await commitRequests(60);
  // Neither the pinned-history admission nor the level-zero ceiling reads every record of a
  // kind: the large database's commit issues exactly the small one's requests and steps.
  expect(Object.fromEntries(large)).toEqual(Object.fromEntries(small));
});
