import { IDBFactory } from "fake-indexeddb";
import { describe, expect, it, vi } from "vitest";
import { IndexedDbBlockStore, MemoryBlockStore, OpfsBlockStore } from "../storage/index.js";
import { MemoryOpfs } from "../testing/opfs-shim.js";
import { MinnowDatabaseClient } from "./client.js";
import { createBoundary } from "./client-audit-harness.js";
import { MinnowDatabase } from "./database.js";
import { writeAdmissionTestHooks } from "./write-coordinator.js";
import { exposeDatabase } from "./worker-host.js";

const stores = [
  { name: "memory", open: async () => new MemoryBlockStore() },
  {
    name: "indexeddb",
    open: () =>
      IndexedDbBlockStore.open({ name: crypto.randomUUID(), indexedDB: new IDBFactory() }),
  },
  {
    name: "opfs",
    open: () => OpfsBlockStore.open({ name: crypto.randomUUID(), root: new MemoryOpfs().root }),
  },
];

for (const storage of stores) {
  describe(storage.name, () => {
    it.each([false, true])(
      "loads 45 tables at concurrency 1, 6 and 12 (worker=%s) without retries",
      async (worker) => {
        for (const concurrency of [1, 6, 12]) {
          const store = await storage.open();
          const database = new MinnowDatabase(store, {
            maxCommitRetries: 0,
            compression: "raw",
            autoCollect: false,
            autoCompact: false,
          });
          const boundary = createBoundary();
          if (worker) exposeDatabase(database, boundary.workerSide);
          const client = worker ? new MinnowDatabaseClient(boundary.clientSide) : undefined;
          const writer = client ?? database;
          try {
            for (let table = 0; table < 45; table += 1) {
              await database.createTable({
                name: `items_${String(table)}`,
                uniqueKey: "id",
                columns: [
                  { name: "id", type: "number" },
                  { name: "value", type: "number" },
                ],
              });
            }
            await database.execute("CREATE TABLE counter (id INTEGER PRIMARY KEY, value INTEGER)");
            await database.execute("INSERT INTO counter VALUES (1, 0)");
            let next = 0;
            let callbacks = 0;
            const seen: number[] = [];
            await Promise.all(
              Array.from({ length: concurrency }, async () => {
                while (next < 45) {
                  const table = next++;
                  const { result } = await writer.write(async (tx) => {
                    callbacks += 1;
                    const before = (await tx.query("SELECT value FROM counter WHERE id = 1"))
                      .rows[0]?.value as number;
                    await tx.upsertBatch(
                      `items_${String(table)}`,
                      Array.from({ length: 20 }, (_, id) => ({ id, value: table })),
                    );
                    await tx.execute("UPDATE counter SET value = ? WHERE id = 1", [before + 1]);
                    return before;
                  });
                  seen.push(result);
                }
              }),
            );
            expect(callbacks).toBe(45);
            expect(seen.sort((a, b) => a - b)).toEqual(
              Array.from({ length: 45 }, (_, index) => index),
            );
            for (let table = 0; table < 45; table += 1) {
              expect(
                (await database.query(`SELECT id, value FROM items_${String(table)} ORDER BY id`))
                  .rows,
              ).toEqual(Array.from({ length: 20 }, (_, id) => ({ id, value: table })));
            }
          } finally {
            await client?.close();
            await database.close();
            store.close();
          }
        }
      },
    );
  });
}

it.each([false, true])(
  "queues scopes with batch and SQL autocommit writes (worker=%s)",
  async (worker) => {
    const database = new MinnowDatabase(new MemoryBlockStore(), {
      maxCommitRetries: 0,
      autoCollect: false,
      autoCompact: false,
    });
    const boundary = createBoundary();
    if (worker) exposeDatabase(database, boundary.workerSide);
    const client = worker ? new MinnowDatabaseClient(boundary.clientSide) : undefined;
    const writer = client ?? database;
    try {
      await writer.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, value INTEGER)");
      let release!: () => void;
      const gate = new Promise<void>((resolve) => {
        release = resolve;
      });
      let entered!: () => void;
      const started = new Promise<void>((resolve) => {
        entered = resolve;
      });
      const first = writer.write(async (tx) => {
        await tx.insertBatch("items", [{ id: 1, value: 1 }]);
        entered();
        await gate;
      });
      await started;
      const batch = writer.insertBatch("items", [{ id: 2, value: 2 }]);
      const sql = writer.execute("INSERT INTO items VALUES (3, 3)");
      const failed = writer
        .write(async (tx) => {
          await tx.insertBatch("items", [{ id: 4, value: 4 }]);
          throw new Error("deliberate rollback");
        })
        .catch((error: unknown) => error);
      const last = writer.write(async (tx) => {
        await tx.insertBatch("items", [{ id: 5, value: 5 }]);
      });
      // Reads do not wait behind an idle write callback and cannot see its uncommitted rows.
      expect((await writer.query("SELECT * FROM items")).rows).toEqual([]);
      release();
      await Promise.all([first, batch, sql, last]);
      expect(await failed).toMatchObject({ message: "deliberate rollback" });
      expect((await writer.query("SELECT id FROM items ORDER BY id")).rows).toEqual([
        { id: 1 },
        { id: 2 },
        { id: 3 },
        { id: 5 },
      ]);
    } finally {
      await client?.close();
      await database.close();
    }
  },
);

it("cancels queued scopes on close without running their callbacks or acquiring leases", async () => {
  const store = new MemoryBlockStore();
  const database = new MinnowDatabase(store);
  await database.execute("CREATE TABLE items (id INTEGER)");
  let entered!: () => void;
  const started = new Promise<void>((resolve) => {
    entered = resolve;
  });
  const first = database
    .write(async (tx) => {
      await tx.insertBatch("items", [{ id: 1 }]);
      entered();
      await new Promise<void>(() => undefined);
    })
    .catch((error: unknown) => error);
  await started;
  const callback = vi.fn(async () => undefined);
  const queued = Array.from({ length: 12 }, () =>
    database.write(callback).catch((error: unknown) => error),
  );
  expect(await store.listLeases()).toHaveLength(1);
  await database.close();
  for (const error of await Promise.all([first, ...queued]))
    expect(error).toMatchObject({ message: "Database is closed" });
  expect(callback).not.toHaveBeenCalled();
  expect(await store.listLeases()).toEqual([]);
});

it("reports a scope that waits on its own callback instead of deadlocking silently", async () => {
  writeAdmissionTestHooks.stallReportMs = 50;
  const reports: unknown[] = [];
  const database = new MinnowDatabase(new MemoryBlockStore(), {
    autoCollect: false,
    autoCompact: false,
    onBackgroundError: (error, context) => reports.push({ context, error }),
  });
  try {
    await database.execute("CREATE TABLE items (id INTEGER PRIMARY KEY)");
    let innerSettled = false;
    const outer = database.write<string>(async (tx) => {
      await tx.insertBatch("items", [{ id: 1 }]);
      // Misuse: a write on the same database from inside a callback waits for the callback.
      const inner = database
        .write(async (nested) => {
          await nested.insertBatch("items", [{ id: 2 }]);
        })
        .finally(() => {
          innerSettled = true;
        });
      await Promise.race([inner, new Promise((resolve) => setTimeout(resolve, 200))]);
      return "outer done";
    });
    const { result } = await outer;
    expect(result).toBe("outer done");
    expect(reports).toHaveLength(1);
    expect(reports[0]).toMatchObject({
      context: "write admission",
      error: { name: "WriteAdmissionStalledError", holder: "this-context", holderKind: "scope" },
    });
    // Once the outer callback returns, the nested scope gets its turn and lands.
    await new Promise((resolve) => setTimeout(resolve, 20));
    expect(innerSettled).toBe(true);
    expect((await database.query("SELECT id FROM items ORDER BY id")).rows).toEqual([
      { id: 1 },
      { id: 2 },
    ]);
  } finally {
    writeAdmissionTestHooks.stallReportMs = undefined;
    await database.close();
  }
});

it("cancels a queued scope through its own signal without running its callback", async () => {
  const database = new MinnowDatabase(new MemoryBlockStore(), {
    autoCollect: false,
    autoCompact: false,
  });
  try {
    await database.execute("CREATE TABLE items (id INTEGER PRIMARY KEY)");
    let release!: () => void;
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    const holder = database.write(async (tx) => {
      await tx.insertBatch("items", [{ id: 1 }]);
      await gate;
    });
    const controller = new AbortController();
    const callback = vi.fn(async () => undefined);
    const queued = database.write(callback, { signal: controller.signal });
    controller.abort(new Error("user navigated away"));
    await expect(queued).rejects.toThrow("user navigated away");
    release();
    await holder;
    expect(callback).not.toHaveBeenCalled();
    expect((await database.query("SELECT id FROM items")).rows).toEqual([{ id: 1 }]);
  } finally {
    await database.close();
  }
});
