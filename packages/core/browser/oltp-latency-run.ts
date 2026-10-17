/**
 * Single-row latency over real IndexedDB and OPFS, for the gated `oltp-latency-bench.spec.ts`
 * measurement: point reads, updates, inserts, and deletes on a 20,000-row keyed table, through
 * the engine on the page and through the published worker, beside the raw IndexedDB floor of
 * the same browser. Timings are printed, never asserted; they are not published numbers.
 */
import { MinnowDatabaseClient } from "@minnowdb/core/client";
import { IndexedDbBlockStore } from "@minnowdb/core/storage/indexeddb";
import { MinnowDatabase } from "@minnowdb/core";

type StoreKind = "indexeddb" | "opfs";

export interface OltpLatency {
  /** Microseconds per operation, median of the sampled rounds. */
  pointRead: number;
  pointReadAfterWrites: number;
  pointUpdate: number;
  pointInsert: number;
  pointDelete: number;
  checksum: number;
}

const ROWS = 20_000;

interface Driver {
  execute(sql: string, params?: readonly unknown[]): Promise<unknown>;
  query(
    sql: string,
    params?: readonly unknown[],
  ): Promise<{ rows: Array<Record<string, unknown>> }>;
  close(): Promise<void>;
}

function median(values: number[]): number {
  const sorted = [...values].sort((left, right) => left - right);
  return sorted[Math.floor(sorted.length / 2)] ?? 0;
}

async function measure(driver: Driver, rounds = 5, perRound = 100): Promise<OltpLatency> {
  await driver.execute(
    "CREATE TABLE items (id INTEGER PRIMARY KEY, region TEXT, amount REAL NOT NULL, label TEXT NOT NULL)",
  );
  for (let start = 0; start < ROWS; start += 1_000) {
    const values: string[] = [];
    for (let id = start + 1; id <= start + 1_000; id += 1) {
      values.push(
        `(${String(id)}, 'r${String(id % 7)}', ${String(id / 4)}, 'label-${String(id % 1000)}')`,
      );
    }
    await driver.execute(`INSERT INTO items VALUES ${values.join(", ")}`);
  }
  const time = async (operation: (index: number) => Promise<unknown>): Promise<number> => {
    const samples: number[] = [];
    let index = 0;
    for (let round = 0; round < rounds; round += 1) {
      const started = performance.now();
      for (let repeat = 0; repeat < perRound; repeat += 1) await operation(index++);
      samples.push(((performance.now() - started) / perRound) * 1000);
    }
    return median(samples);
  };
  const key = (index: number) => ((index * 7919) % ROWS) + 1;
  // Warm the caches the way a running application has them.
  for (let index = 0; index < 50; index += 1) {
    await driver.query("SELECT id, amount, label FROM items WHERE id = ?", [key(index)]);
  }
  const pointRead = await time((index) =>
    driver.query("SELECT id, amount, label FROM items WHERE id = ?", [key(index)]),
  );
  const pointUpdate = await time((index) =>
    driver.execute("UPDATE items SET amount = amount + 1 WHERE id = ?", [key(index + 11)]),
  );
  const pointReadAfterWrites = await time((index) =>
    driver.query("SELECT id, amount, label FROM items WHERE id = ?", [key(index + 3)]),
  );
  const pointInsert = await time((index) =>
    driver.execute("INSERT INTO items VALUES (?, 'r0', 1.5, 'new')", [ROWS + 1 + index]),
  );
  const pointDelete = await time((index) =>
    driver.execute("DELETE FROM items WHERE id = ?", [ROWS + 1 + index]),
  );
  const { rows } = await driver.query("SELECT COUNT(*) AS n, SUM(amount) AS total FROM items");
  return {
    pointRead,
    pointReadAfterWrites,
    pointUpdate,
    pointInsert,
    pointDelete,
    checksum: Number(rows[0]?.n ?? 0) + Number(rows[0]?.total ?? 0),
  };
}

/** The engine on this thread over IndexedDB: no worker hop, so this is the engine plus the store. */
export async function measureDirectIndexedDb(): Promise<OltpLatency> {
  const name = `oltp-${crypto.randomUUID()}`;
  const database = new MinnowDatabase(await IndexedDbBlockStore.open({ name }));
  try {
    return await measure({
      execute: (sql, params) => database.execute(sql, params as never),
      query: (sql, params) =>
        database.query(sql, { params: (params ?? []) as never, memoize: false }),
      close: () => database.close(),
    });
  } finally {
    await database.close();
  }
}

/** The published worker, as an application uses it. */
export async function measureWorker(kind: StoreKind): Promise<OltpLatency> {
  const client = new MinnowDatabaseClient(
    new Worker(new URL("./published-worker.ts", import.meta.url), { type: "module" }),
    { store: { kind, name: `oltp-${crypto.randomUUID()}` } },
  );
  try {
    return await measure({
      execute: (sql, params) => client.execute(sql, params as never),
      query: (sql, params) =>
        client.query(sql, { params: (params ?? []) as never, memoize: false }),
      close: () => client.close({ terminateWorker: true }),
    });
  } finally {
    await client.close({ terminateWorker: true });
  }
}

/** The raw IndexedDB floor: what one transaction of a given shape costs in this browser. */
export async function measureRawIndexedDb(): Promise<Record<string, number>> {
  const name = `raw-${crypto.randomUUID()}`;
  const db = await new Promise<IDBDatabase>((resolve, reject) => {
    const request = indexedDB.open(name, 1);
    request.onupgradeneeded = () => request.result.createObjectStore("s");
    request.onsuccess = () => resolve(request.result);
    request.onerror = () => reject(request.error ?? new Error("open failed"));
  });
  const done = (transaction: IDBTransaction) =>
    new Promise<void>((resolve, reject) => {
      transaction.oncomplete = () => resolve();
      transaction.onerror = () => reject(transaction.error ?? new Error("failed"));
      transaction.onabort = () => reject(transaction.error ?? new Error("aborted"));
    });
  {
    const transaction = db.transaction("s", "readwrite");
    for (let index = 0; index < 1_000; index += 1) {
      transaction.objectStore("s").put({ index, payload: "x".repeat(200) }, index);
    }
    await done(transaction);
  }
  const results: Record<string, number> = {};
  const time = async (label: string, run: (index: number) => Promise<void>, count = 50) => {
    for (let index = 0; index < 5; index += 1) await run(index);
    const started = performance.now();
    for (let index = 0; index < count; index += 1) await run(index);
    results[label] = ((performance.now() - started) / count) * 1000;
  };
  await time("readonly 1 get", async (index) => {
    const transaction = db.transaction("s", "readonly");
    transaction.objectStore("s").get(index % 1_000);
    await done(transaction);
  });
  await time("readonly 100 gets", async (index) => {
    const transaction = db.transaction("s", "readonly");
    for (let get = 0; get < 100; get += 1) transaction.objectStore("s").get((index + get) % 1_000);
    await done(transaction);
  });
  for (const durability of ["strict", "relaxed"] as const) {
    await time(`readwrite ${durability} 1 put`, async (index) => {
      const transaction = db.transaction("s", "readwrite", { durability });
      transaction.objectStore("s").put({ index }, 10_000 + index);
      await done(transaction);
    });
    await time(`readwrite ${durability} 1 put + 175 gets`, async (index) => {
      const transaction = db.transaction("s", "readwrite", { durability });
      for (let get = 0; get < 175; get += 1) {
        transaction.objectStore("s").get((index + get) % 1_000);
      }
      transaction.objectStore("s").put({ index }, 10_000 + index);
      await done(transaction);
    });
  }
  db.close();
  indexedDB.deleteDatabase(name);
  return results;
}
