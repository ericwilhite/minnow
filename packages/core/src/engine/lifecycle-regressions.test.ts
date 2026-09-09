import { IDBFactory } from "fake-indexeddb";
import { afterEach, describe, expect, it, vi } from "vitest";
import {
  IndexedDbBlockStore,
  MemoryBlockStore,
  OpfsBlockStore,
  type BlockStore,
} from "../storage/index.js";
import { MemoryOpfs } from "../testing/opfs-shim.js";
import { MinnowDatabase } from "./database.js";
import { TransactionExpiredError } from "./errors.js";

const stores = [
  { name: "memory", open: async (): Promise<BlockStore> => new MemoryBlockStore() },
  {
    name: "indexeddb",
    open: async (): Promise<BlockStore> =>
      IndexedDbBlockStore.open({ name: crypto.randomUUID(), indexedDB: new IDBFactory() }),
  },
  {
    name: "opfs",
    open: async (): Promise<BlockStore> =>
      OpfsBlockStore.open({ name: crypto.randomUUID(), root: new MemoryOpfs().root }),
  },
];
const cleanup: Array<() => Promise<void>> = [];
afterEach(async () => {
  for (const close of cleanup.splice(0).reverse()) await close();
  vi.useRealTimers();
});

async function open(factory: () => Promise<BlockStore>, options = {}) {
  const store = await factory();
  const db = new MinnowDatabase(store, { autoCollect: false, autoCompact: false, ...options });
  cleanup.push(async () => {
    await db.close();
    store.close();
  });
  await db.execute("CREATE TABLE items(id INTEGER PRIMARY KEY, value INTEGER)");
  return { db, store };
}

for (const adapter of stores)
  describe(adapter.name, () => {
    it("keeps timed-out SQL transactions failed until explicit acknowledgement", async () => {
      let now = Date.parse("2026-09-06T12:00:00.000Z");
      const { db } = await open(adapter.open, {
        now: () => new Date(now),
        transactionIdleTimeoutMs: 1000,
      });
      await db.execute("BEGIN");
      await db.execute("INSERT INTO items VALUES (1, 10)");
      now += 1001;
      await expect(db.execute("INSERT INTO items VALUES (2, 20)")).rejects.toBeInstanceOf(
        TransactionExpiredError,
      );
      await expect(db.execute("COMMIT")).rejects.toBeInstanceOf(TransactionExpiredError);
      await expect(db.query("SELECT * FROM items")).rejects.toBeInstanceOf(TransactionExpiredError);
      await db.execute("ROLLBACK");
      expect((await db.query("SELECT * FROM items")).rows).toEqual([]);
      await db.execute("BEGIN");
      now += 1001;
      await db.execute("BEGIN");
      await db.execute("INSERT INTO items VALUES (3, 30)");
      await db.execute("COMMIT");
      expect((await db.query("SELECT * FROM items")).rows).toEqual([{ id: 3, value: 30 }]);
    });

    it("orders parallel stages and drains admitted writes before publishing", async () => {
      const { db } = await open(adapter.open);
      await db.write(async (tx) => {
        const first = tx.insertBatch("items", [{ id: 1, value: 10 }]);
        const second = tx.updateBatch("items", { keys: [1], changes: { value: [20] } });
        const read = tx.query("SELECT value FROM items WHERE id = 1");
        const third = tx.insertBatch("items", [{ id: 2, value: 30 }]);
        await expect(read).resolves.toMatchObject({ rows: [{ value: 20 }] });
        await Promise.all([first, second, third]);
      });
      expect((await db.query("SELECT * FROM items ORDER BY id")).rows).toEqual([
        { id: 1, value: 20 },
        { id: 2, value: 30 },
      ]);
      await expect(
        db.write(async (tx) => {
          void tx.insertBatch("items", [{ id: 3, value: 40 }]).catch(() => undefined);
          throw new Error("cancel checkout");
        }),
      ).rejects.toThrow("cancel checkout");
      expect((await db.query("SELECT COUNT(*) AS n FROM items")).rows).toEqual([{ n: 2 }]);
    });

    it("reserves BEGIN before opening and commits after earlier SQL mutations", async () => {
      const { db } = await open(adapter.open);
      const begins = await Promise.allSettled([db.execute("BEGIN"), db.execute("BEGIN")]);
      expect(begins.map((result) => result.status)).toEqual(["fulfilled", "rejected"]);
      await Promise.all([db.execute("INSERT INTO items VALUES (1, 10)"), db.execute("COMMIT")]);
      expect((await db.query("SELECT * FROM items")).rows).toEqual([{ id: 1, value: 10 }]);
    });

    it("pins an empty snapshot across the first commit", async () => {
      const { db } = await open(adapter.open);
      await db.snapshot(async (snapshot) => {
        expect(snapshot.version).toBeNull();
        expect((await snapshot.query("SELECT COUNT(*) AS n FROM items")).rows).toEqual([{ n: 0 }]);
        await db.insertBatch("items", [{ id: 1, value: 10 }]);
        expect((await snapshot.query("SELECT COUNT(*) AS n FROM items")).rows).toEqual([{ n: 0 }]);
      });
      expect((await db.query("SELECT COUNT(*) AS n FROM items")).rows).toEqual([{ n: 1 }]);
    });

    it("evaluates VALUES subqueries and trigger INSERTs through staged state", async () => {
      const { db } = await open(adapter.open);
      await db.execute("CREATE TABLE audit(value INTEGER)");
      await db.insertBatch("items", [{ id: 1, value: 10 }]);
      await db.execute(
        "CREATE TRIGGER copy_value AFTER INSERT ON items BEGIN INSERT INTO audit VALUES ((SELECT value FROM items WHERE id = 1)); END",
      );
      await db.execute("BEGIN");
      await db.execute("UPDATE items SET value = 20 WHERE id = 1");
      await db.execute("INSERT INTO items VALUES (2, (SELECT value FROM items WHERE id = 1))");
      expect((await db.query("SELECT * FROM items ORDER BY id")).rows).toEqual([
        { id: 1, value: 20 },
        { id: 2, value: 20 },
      ]);
      expect((await db.query("SELECT * FROM audit")).rows).toEqual([{ value: 20 }]);
      await db.execute("ROLLBACK");
      expect((await db.query("SELECT * FROM items")).rows).toEqual([{ id: 1, value: 10 }]);
      expect((await db.query("SELECT * FROM audit")).rows).toEqual([]);
    });

    it("refuses an expired snapshot after collection instead of switching versions", async () => {
      let now = Date.parse("2026-09-06T12:00:00.000Z");
      const { db } = await open(adapter.open, { now: () => new Date(now) });
      await db.insertBatch("items", [{ id: 1, value: 10 }]);
      await expect(
        db.snapshot(async (snapshot) => {
          await db.updateBatch("items", { keys: [1], changes: { value: [20] } });
          // Simulate a suspended owner: no timer runs while the shared wall clock advances.
          now += 120_000;
          await db.compactTable("items");
          await db.collectGarbage();
          await snapshot.query("SELECT value FROM items");
        }),
      ).rejects.toThrow();
      expect((await db.query("SELECT value FROM items")).rows).toEqual([{ value: 20 }]);
    });

    it("closes paused query and export iterators before the adapter closes", async () => {
      const { db, store } = await open(adapter.open);
      await db.insertBatch(
        "items",
        Array.from({ length: 10 }, (_, id) => ({ id, value: id })),
      );
      const cursor = db.queryCursor("SELECT * FROM items", { batchRows: 1 });
      expect((await cursor.next()).done).toBe(false);
      const exported = db.exportSnapshotStream();
      expect((await exported.next()).done).toBe(false);
      await db.close();
      expect(await store.listLeases()).toEqual([]);
      await expect(cursor.next()).rejects.toThrow(/closed/i);
      await expect(exported.next()).rejects.toThrow(/closed/i);
      await cursor.return?.();
      await exported.return(undefined);
    });

    it("closes an idle scope without letting its resumed callback publish", async () => {
      const { db, store } = await open(adapter.open);
      let release!: () => void;
      let started!: () => void;
      const ready = new Promise<void>((resolve) => {
        started = resolve;
      });
      const idle = new Promise<void>((resolve) => {
        release = resolve;
      });
      const scope = db.write(async (tx) => {
        await tx.insertBatch("items", [{ id: 1, value: 10 }]);
        started();
        await idle;
        await tx.insertBatch("items", [{ id: 2, value: 20 }]);
      });
      const rejected = expect(scope).rejects.toThrow("Database is closed");
      await ready;
      await db.close();
      await rejected;
      release();
      await expect(
        db.write(async (tx) => tx.insertBatch("items", [{ id: 3, value: 30 }])),
      ).rejects.toThrow("closed");
      await expect(db.insertBatch("items", [{ id: 4, value: 40 }])).rejects.toThrow("closed");
      const reader = new MinnowDatabase(store);
      expect((await reader.query("SELECT * FROM items")).rows).toEqual([]);
      await reader.close();
    });
  });

it("renews runnable idle snapshots through compaction and garbage collection", async () => {
  vi.useFakeTimers({ toFake: ["Date", "setInterval", "clearInterval"] });
  const { db } = await open(async () => new MemoryBlockStore());
  await db.insertBatch("items", [{ id: 1, value: 10 }]);
  await db.snapshot(async (snapshot) => {
    await db.updateBatch("items", { keys: [1], changes: { value: [20] } });
    await vi.advanceTimersByTimeAsync(120_000);
    await db.compactTable("items");
    await db.collectGarbage();
    expect((await snapshot.query("SELECT value FROM items")).rows).toEqual([{ value: 10 }]);
  });
  expect((await db.query("SELECT value FROM items")).rows).toEqual([{ value: 20 }]);
});

it("close cancels an idle snapshot and rejects late catalog, typed, and SQL reads", async () => {
  const { db } = await open(async () => new MemoryBlockStore());
  let entered!: () => void;
  let release!: () => void;
  const ready = new Promise<void>((resolve) => {
    entered = resolve;
  });
  const idle = new Promise<void>((resolve) => {
    release = resolve;
  });
  const snapshot = db.snapshot(async (session) => {
    entered();
    await idle;
    return session.query("SELECT * FROM items");
  });
  const rejected = expect(snapshot).rejects.toThrow("closed");
  await ready;
  await db.close();
  await rejected;
  release();
  await expect(db.query("SELECT 1")).rejects.toThrow("closed");
  await expect(db.listTables()).rejects.toThrow("closed");
  await expect(db.execute("SELECT 1")).rejects.toThrow("closed");
  await expect(
    db.createTable({ name: "late", columns: [{ name: "n", type: "number" }] }),
  ).rejects.toThrow("closed");
});

it("joins a catalog write already in storage before close releases resources", async () => {
  let entered!: () => void;
  let release!: () => void;
  const ready = new Promise<void>((resolve) => {
    entered = resolve;
  });
  const paused = new Promise<void>((resolve) => {
    release = resolve;
  });
  class CatalogBarrier extends MemoryBlockStore {
    override async addTable(record: Parameters<MemoryBlockStore["addTable"]>[0]) {
      entered();
      await paused;
      return super.addTable(record);
    }
  }
  const store = new CatalogBarrier();
  const db = new MinnowDatabase(store);
  const creating = db.createTable({ name: "items", columns: [{ name: "n", type: "number" }] });
  await ready;
  let closed = false;
  const closing = db.close().then(() => {
    closed = true;
  });
  await Promise.resolve();
  expect(closed).toBe(false);
  release();
  await creating;
  await closing;
  expect((await store.listTables()).map(({ name }) => name)).toEqual(["items"]);
  store.close();
});

it("uses at most six strict IndexedDB writes for a three-table checkout", async () => {
  const factory = new IDBFactory();
  const originalOpen = factory.open.bind(factory);
  let writes = 0;
  vi.spyOn(factory, "open").mockImplementation((name, version) => {
    const request = originalOpen(name, version);
    request.addEventListener("success", () => {
      const original = request.result.transaction.bind(request.result);
      vi.spyOn(request.result, "transaction").mockImplementation((names, mode, options) => {
        if (mode === "readwrite") {
          writes += 1;
          expect(options?.durability).toBe("strict");
        }
        return original(names, mode, options);
      });
    });
    return request;
  });
  const { db } = await open(() =>
    IndexedDbBlockStore.open({ name: crypto.randomUUID(), indexedDB: factory }),
  );
  await db.execute("CREATE TABLE sales(id INTEGER PRIMARY KEY, total INTEGER)");
  await db.execute("CREATE TABLE lines(id INTEGER PRIMARY KEY, sale_id INTEGER, qty INTEGER)");
  await db.insertBatch("items", [{ id: 1, value: 100 }]);
  for (let id = 0; id < 3; id += 1) {
    writes = 0;
    await db.write(async (tx) => {
      await tx.execute("UPDATE items SET value = value - 1 WHERE id = 1");
      await tx.insertBatch("sales", [{ id, total: 1000 }]);
      await tx.insertBatch("lines", [{ id, sale_id: id, qty: 1 }]);
    });
    expect(writes).toBeLessThanOrEqual(6);
  }
  expect((await db.query("SELECT value FROM items")).rows).toEqual([{ value: 97 }]);
  expect((await db.query("SELECT COUNT(*) AS n FROM sales")).rows).toEqual([{ n: 3 }]);
  expect((await db.query("SELECT COUNT(*) AS n FROM lines")).rows).toEqual([{ n: 3 }]);
});

it("cancels write admission on close without waiting for another connection's storage", async () => {
  let entered!: () => void;
  let release!: () => void;
  const ready = new Promise<void>((resolve) => {
    entered = resolve;
  });
  const paused = new Promise<void>((resolve) => {
    release = resolve;
  });
  class CommitBarrier extends MemoryBlockStore {
    override async writeTransaction(input: Parameters<MemoryBlockStore["writeTransaction"]>[0]) {
      entered();
      await paused;
      return super.writeTransaction(input);
    }
  }
  const store = new CommitBarrier();
  const first = new MinnowDatabase(store, { autoCollect: false, autoCompact: false });
  const waiting = new MinnowDatabase(store, { autoCollect: false, autoCompact: false });
  await first.execute("CREATE TABLE items(id INTEGER PRIMARY KEY)");
  const writing = first.insertBatch("items", [{ id: 1 }]);
  await ready;
  const queued = waiting.insertBatch("items", [{ id: 2 }]);
  const cancelled = expect(queued).rejects.toThrow("closed");
  await waiting.close();
  await cancelled;
  release();
  await writing;
  expect((await first.query("SELECT * FROM items")).rows).toEqual([{ id: 1 }]);
  await first.close();
  store.close();
});
