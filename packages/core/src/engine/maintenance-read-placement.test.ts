/**
 * Where background maintenance reads land relative to this connection's own writes.
 *
 * IndexedDB runs one connection's overlapping transactions in creation order, so a read created
 * while one of the connection's own readwrite transactions is open waits inside IndexedDB behind
 * it. WebKit can leak a transaction that is still waiting there when its worker is terminated,
 * and the leaked transaction then blocks the database in every tab until the tab that ran the
 * worker closes. The automatic compaction check fires from timers at any moment, so its reads
 * wait for a moment between this database's writer turns instead.
 */
import { IDBDatabase as FakeIDBDatabase, IDBFactory } from "fake-indexeddb";
import { afterEach, expect, it, vi } from "vitest";
import { IndexedDbBlockStore } from "../storage/indexeddb.js";
import { MemoryBlockStore } from "../storage/memory.js";
import { MinnowDatabase } from "./database.js";
import { heavyTestTimeout } from "./storage-test-helpers.js";
import { WriterAdmissions } from "./writer-admissions.js";

vi.setConfig({ testTimeout: heavyTestTimeout(60_000) });

/**
 * How long each commit's transaction stays open: longer than the compaction controller's 25 ms
 * quiet-table timer, as a durable commit on a slow disk can be.
 */
const COMMIT_MS = 40;

interface OpenTransaction {
  readonly mode: IDBTransactionMode;
  readonly stores: readonly string[];
}

afterEach(() => {
  vi.restoreAllMocks();
});

/**
 * Records every read-only transaction created while a readwrite transaction with an overlapping
 * scope is open — one IndexedDB queues behind it — and holds each commit open for COMMIT_MS by
 * placing reads from inside its own request callbacks, which keeps a transaction alive.
 */
function observeTransactions() {
  const open = new Set<OpenTransaction>();
  const queuedReads: Array<{ stores: string[]; behind: string[][]; at: string[] }> = [];
  let checkReads = 0;
  // eslint-disable-next-line @typescript-eslint/unbound-method -- Called with its native receiver below.
  const createTransaction = FakeIDBDatabase.prototype.transaction;
  vi.spyOn(FakeIDBDatabase.prototype, "transaction").mockImplementation(function (
    this: InstanceType<typeof FakeIDBDatabase>,
    stores,
    mode,
    options,
  ) {
    const transaction = createTransaction.call(this, stores, mode, options);
    const entry: OpenTransaction = {
      mode: mode ?? "readonly",
      stores: typeof stores === "string" ? [stores] : [...stores],
    };
    if (entry.mode === "readonly") {
      const stack = (new Error().stack ?? "").split("\n").slice(2, 8);
      if (stack.some((frame) => frame.includes("checkAutoCompaction"))) checkReads += 1;
      const behind = [...open].filter(
        (other) =>
          other.mode === "readwrite" && other.stores.some((store) => entry.stores.includes(store)),
      );
      if (behind.length > 0) {
        queuedReads.push({
          stores: [...entry.stores],
          behind: behind.map((other) => [...other.stores]),
          at: stack.map((frame) => frame.trim()),
        });
      }
    }
    open.add(entry);
    const done = (): void => {
      open.delete(entry);
    };
    transaction.addEventListener("complete", done);
    transaction.addEventListener("abort", done);
    if (entry.mode === "readwrite" && entry.stores.includes("segments")) {
      const deadline = performance.now() + COMMIT_MS;
      const catalog = transaction.objectStore("catalog");
      const extend = (): void => {
        if (performance.now() < deadline) catalog.get("held").onsuccess = extend;
      };
      catalog.get("held").onsuccess = extend;
    }
    return transaction;
  });
  return { queuedReads, checkReads: () => checkReads };
}

it("starts automatic compaction checks between writer turns, never behind this connection's commit", async () => {
  const store = await IndexedDbBlockStore.open({
    name: crypto.randomUUID(),
    indexedDB: new IDBFactory(),
  });
  const database = new MinnowDatabase(store, { autoCollect: false });
  try {
    await database.execute("CREATE TABLE stock(id INTEGER PRIMARY KEY, qty INTEGER)");
    await database.execute("CREATE TABLE sales(id INTEGER PRIMARY KEY, total INTEGER)");
    await database.execute(
      "CREATE TABLE lines(id INTEGER PRIMARY KEY, sale_id INTEGER, qty INTEGER)",
    );
    await database.insertBatch("stock", [{ id: 1, qty: 1_000 }]);
    const observed = observeTransactions();
    // A checkout: each scope changes three tables, so every commit owes each of them a check —
    // every eighth commit right away, and 25 ms after the latest one, while the next commit is
    // still open.
    for (let id = 10; id < 34; id += 1) {
      await database.write(async (tx) => {
        await tx.execute("UPDATE stock SET qty = qty - 1 WHERE id = 1");
        await tx.insertBatch("sales", [{ id, total: 1_000 }]);
        await tx.insertBatch("lines", [{ id, sale_id: id, qty: 1 }]);
      });
    }
    // The quiet-table checks after the last commit.
    await new Promise((resolve) => setTimeout(resolve, 100));
    expect(observed.checkReads()).toBeGreaterThan(0);
    expect(observed.queuedReads).toEqual([]);
    expect((await database.query("SELECT qty FROM stock WHERE id = 1")).rows).toEqual([
      { qty: 976 },
    ]);
  } finally {
    await database.close();
    store.close();
  }
});

function admissions(store: MemoryBlockStore, shutdown: AbortSignal): WriterAdmissions {
  return new WriterAdmissions({
    store,
    shutdown,
    closedError: () => new Error("closed"),
    report: () => undefined,
  });
}

/** Admits a writer turn that runs until the returned release is called. */
async function holdTurn(writers: WriterAdmissions): Promise<{
  release: () => void;
  done: Promise<void>;
}> {
  let release = (): void => undefined;
  let admitted = (): void => undefined;
  const running = new Promise<void>((resolve) => {
    admitted = resolve;
  });
  const done = writers.admit("statement", async () => {
    admitted();
    await new Promise<void>((resolve) => {
      release = resolve;
    });
  });
  await running;
  return { release, done };
}

async function settled(promise: Promise<void>): Promise<boolean> {
  let resolved = false;
  void promise.then(() => {
    resolved = true;
  });
  await new Promise((resolve) => setTimeout(resolve, 0));
  return resolved;
}

it("lets background reads wait out this database's running turn, not another's", async () => {
  const store = new MemoryBlockStore();
  const shutdown = new AbortController();
  const own = admissions(store, shutdown.signal);
  const other = admissions(store, new AbortController().signal);
  expect(await settled(own.outsideTurns())).toBe(true);

  // Another connection's turn: this database's writer only waits for admission, so nothing of
  // its own is open in the store, and its background reads need not wait.
  const foreign = await holdTurn(other);
  const queued = own.admit("statement", async () => undefined);
  expect(await settled(own.outsideTurns())).toBe(true);
  foreign.release();
  await Promise.all([foreign.done, queued]);

  // Its own running turn holds them back until the turn ends.
  const turn = await holdTurn(own);
  const outside = own.outsideTurns();
  expect(await settled(outside)).toBe(false);
  turn.release();
  await turn.done;
  expect(await settled(outside)).toBe(true);

  // Closing releases a waiter even while a turn is still running.
  const stuck = await holdTurn(own);
  const closing = own.outsideTurns();
  expect(await settled(closing)).toBe(false);
  shutdown.abort(new Error("closed"));
  expect(await settled(closing)).toBe(true);
  expect(await settled(own.outsideTurns())).toBe(true);
  stuck.release();
  await stuck.done;
});
