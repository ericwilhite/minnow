/**
 * Databases the last released writer, 0.15.0, left — IndexedDB schema 5, OPFS layout 10 — with
 * single-row updates and deletes over folded partitions. A delta merge's job record (`delta-v1`)
 * and the level-zero segments it publishes under the order of the run they replace are records
 * an older reader refuses as corruption, so IndexedDB schema 7 and OPFS layout 12 bar it; the
 * upgrade moves only the version marker. (Schema 6 and layout 11 were never released: 0.16.0
 * writes 7 and 12. This build writes 8 and 13, which add TIMESTAMP without time zone columns;
 * see timestamp-kind-upgrade.test.ts.) These tests take the real 0.15.0 writer's databases through the ordinary
 * open API: the history reads the same, the deltas it left merge, writes resume, the database
 * reopens, and the released reader is refused afterwards without a change. They also cut the
 * IndexedDB upgrade mid-way and retry, race current and released openers, and refuse a damaged
 * merge record as corruption. The OPFS marker's publication steps are cut in
 * marker-upgrade.test.ts, over the frozen layout-10 fixture.
 */
import { IDBFactory } from "fake-indexeddb";
import { describe, expect, it, vi } from "vitest";
import { MinnowDatabase as ReleasedDatabase } from "@minnowdb/core-layout10";
import { IndexedDbBlockStore as ReleasedIndexedDbStore } from "@minnowdb/core-layout10/storage/indexeddb";
import { OpfsBlockStore as ReleasedOpfsStore } from "@minnowdb/core-layout10/storage/opfs";
import {
  IndexedDbBlockStore,
  OpfsBlockStore,
  StorageCorruptionError,
  type BlockStore,
  type CompactionJobRecord,
} from "../storage/index.js";
import { readRawValue } from "../storage/indexeddb-audit-helpers.js";
import { MemoryOpfs } from "../testing/opfs-shim.js";
import { MinnowDatabase } from "./database.js";
import { internalsOf } from "./internals.js";
import { heavyTestTimeout } from "./storage-test-helpers.js";

vi.setConfig({ testTimeout: heavyTestTimeout(120_000) });

/** What this build writes, and what the 0.15.0 writer wrote. */
const CURRENT_SCHEMA = 8;
const RELEASED_SCHEMA = 5;
const CURRENT_LAYOUT = 13;
const RELEASED_LAYOUT = 10;
const NAME = "delta-merge-upgrade";
const OPTIONS = { autoCompact: false, autoCollect: false } as const;

interface Item {
  id: number;
  amount: number;
  label: string;
}

/** The released writer, as these tests drive it. */
interface ReleasedWriter {
  createTable(definition: unknown): Promise<unknown>;
  insertBatch(table: string, rows: Item[]): Promise<unknown>;
  execute(sql: string, params?: Array<number | string>): Promise<unknown>;
  compactTable(table: string, options?: { partitionRows?: number }): Promise<unknown>;
  query(sql: string): Promise<{ rows: unknown[] }>;
  close(): Promise<void>;
}

const ALL = "SELECT id, amount, label FROM items ORDER BY id";

/**
 * A keyed table folded into partitions, then single-row updates and deletes left as level-zero
 * deltas: what a delta merge takes once the current build opens the database.
 */
async function writeReleasedHistory(writer: ReleasedWriter): Promise<unknown[]> {
  await writer.createTable({
    name: "items",
    uniqueKey: "id",
    columns: [
      { name: "id", type: "number" },
      { name: "amount", type: "number" },
      { name: "label", type: "string" },
    ],
  });
  for (let start = 0; start < 1_200; start += 400) {
    await writer.insertBatch(
      "items",
      Array.from({ length: 400 }, (_, index) => ({
        id: start + index,
        amount: start + index,
        label: `item-${String(start + index)}`,
      })),
    );
  }
  await writer.compactTable("items", { partitionRows: 400 });
  for (let commit = 0; commit < 24; commit += 1) {
    const id = (commit * 97) % 1_200;
    if (commit % 6 === 5) await writer.execute("DELETE FROM items WHERE id = ?", [id]);
    else if (commit % 2 === 0) {
      await writer.execute("UPDATE items SET amount = amount + 1000 WHERE id = ?", [id]);
    } else {
      await writer.execute("UPDATE items SET label = ? WHERE id = ?", [
        `moved-${String(commit)}`,
        id,
      ]);
    }
  }
  return (await writer.query(ALL)).rows;
}

/** Merges the history's deltas through the automatic step and checks a delta merge published. */
async function mergeDeltas(database: MinnowDatabase): Promise<CompactionJobRecord> {
  const progress = await internalsOf(database).automaticCompactionStep("items");
  expect(progress.result?.compacted).toBe(true);
  const job = (await database.listCompactionJobs()).find(
    (candidate) => candidate.id === progress.result?.jobId,
  );
  if (job?.rewritePlan.kind !== "delta-v1") throw new Error("Expected a delta merge");
  expect(job.state).toBe("published");
  return job;
}

/** The current build's writes on top of an upgraded, merged database. */
async function writeCurrent(database: MinnowDatabase, rows: unknown[]): Promise<unknown[]> {
  await database.execute("UPDATE items SET label = 'after-upgrade' WHERE id = 3");
  await database.execute("INSERT INTO items VALUES (5000, 5000, 'new')");
  const next = (rows as Item[])
    .map((row) => (row.id === 3 ? { ...row, label: "after-upgrade" } : row))
    .concat({ id: 5_000, amount: 5_000, label: "new" });
  expect((await database.query(ALL, { memoize: false })).rows).toEqual(next);
  return next;
}

function nativeVersion(indexedDB: IDBFactory, name: string): Promise<number> {
  return new Promise((resolve, reject) => {
    const request = indexedDB.open(name);
    request.onsuccess = () => {
      const version = request.result.version;
      request.result.close();
      resolve(version);
    };
    request.onerror = () => reject(request.error ?? new Error("native open failed"));
  });
}

async function releasedIndexedDb(indexedDB: IDBFactory, name: string): Promise<unknown[]> {
  const store = await ReleasedIndexedDbStore.open({ name, indexedDB });
  const writer = new ReleasedDatabase(store, OPTIONS) as unknown as ReleasedWriter;
  const rows = await writeReleasedHistory(writer);
  await writer.close();
  store.close();
  expect(await nativeVersion(indexedDB, name)).toBe(RELEASED_SCHEMA);
  return rows;
}

describe("IndexedDB: the 0.15.0 writer's deltas after the schema upgrade", () => {
  it("merges them, writes on, reopens, and refuses the released reader", async () => {
    const indexedDB = new IDBFactory();
    const name = crypto.randomUUID();
    const rows = await releasedIndexedDb(indexedDB, name);

    let store = await IndexedDbBlockStore.open({ name, indexedDB });
    expect(await nativeVersion(indexedDB, name)).toBe(CURRENT_SCHEMA);
    let database = new MinnowDatabase(store, OPTIONS);
    expect((await database.query(ALL, { memoize: false })).rows).toEqual(rows);
    await mergeDeltas(database);
    expect((await database.query(ALL, { memoize: false })).rows).toEqual(rows);
    const latest = await writeCurrent(database, rows);
    expect((await store.checkIntegrity({ mode: "full" })).issues).toEqual([]);
    await database.close();
    store.close();

    store = await IndexedDbBlockStore.open({ name, indexedDB });
    database = new MinnowDatabase(store, OPTIONS);
    expect((await database.query(ALL, { memoize: false })).rows).toEqual(latest);
    expect((await store.checkIntegrity({ mode: "full" })).issues).toEqual([]);
    await database.close();
    store.close();

    await expect(ReleasedIndexedDbStore.open({ name, indexedDB })).rejects.toMatchObject({
      name: "StorageFormatVersionError",
      actualVersion: CURRENT_SCHEMA,
      supportedVersion: RELEASED_SCHEMA,
      relation: "newer",
    });
    expect(await nativeVersion(indexedDB, name)).toBe(CURRENT_SCHEMA);
    store = await IndexedDbBlockStore.open({ name, indexedDB });
    database = new MinnowDatabase(store, OPTIONS);
    expect((await database.query(ALL, { memoize: false })).rows).toEqual(latest);
    await database.close();
    store.close();
  });

  it("leaves schema 5 for the released writer when the upgrade is interrupted, then retries", async () => {
    const indexedDB = new IDBFactory();
    const name = crypto.randomUUID();
    const rows = (await releasedIndexedDb(indexedDB, name)) as Item[];
    // The tab dies inside the version-change transaction: IndexedDB rolls all of it back.
    let aborted = false;
    const open = indexedDB.open.bind(indexedDB);
    Object.defineProperty(indexedDB, "open", {
      configurable: true,
      value: (databaseName: string, version?: number) => {
        const request = version === undefined ? open(databaseName) : open(databaseName, version);
        queueMicrotask(() =>
          request.addEventListener("upgradeneeded", () => {
            if (aborted) return;
            aborted = true;
            request.transaction?.abort();
          }),
        );
        return request;
      },
    });
    await expect(IndexedDbBlockStore.open({ name, indexedDB })).rejects.toThrow();
    expect(aborted).toBe(true);
    expect(await nativeVersion(indexedDB, name)).toBe(RELEASED_SCHEMA);
    // Still the released writer's database, readable and writable by it.
    const releasedStore = await ReleasedIndexedDbStore.open({ name, indexedDB });
    const released = new ReleasedDatabase(releasedStore, OPTIONS) as unknown as ReleasedWriter;
    await released.execute("UPDATE items SET amount = -1 WHERE id = 7");
    await released.close();
    releasedStore.close();
    const expected = rows.map((row) => (row.id === 7 ? { ...row, amount: -1 } : row));

    const store = await IndexedDbBlockStore.open({ name, indexedDB });
    expect(await nativeVersion(indexedDB, name)).toBe(CURRENT_SCHEMA);
    const database = new MinnowDatabase(store, OPTIONS);
    expect((await database.query(ALL, { memoize: false })).rows).toEqual(expected);
    await mergeDeltas(database);
    expect((await database.query(ALL, { memoize: false })).rows).toEqual(expected);
    expect((await store.checkIntegrity({ mode: "full" })).issues).toEqual([]);
    await database.close();
    store.close();
  });

  it("lets concurrent current openers share one upgrade and closes or refuses released ones", async () => {
    const indexedDB = new IDBFactory();
    const name = crypto.randomUUID();
    const rows = await releasedIndexedDb(indexedDB, name);
    const stale = await ReleasedIndexedDbStore.open({ name, indexedDB });
    const [first, second, late] = await Promise.allSettled([
      IndexedDbBlockStore.open({ name, indexedDB }),
      IndexedDbBlockStore.open({ name, indexedDB }),
      ReleasedIndexedDbStore.open({ name, indexedDB }),
    ]);
    expect(first.status).toBe("fulfilled");
    expect(second.status).toBe("fulfilled");
    // A released opener that arrives behind the upgrade is refused, never served the new schema.
    expect(late).toMatchObject({
      status: "rejected",
      reason: { name: "StorageFormatVersionError", actualVersion: CURRENT_SCHEMA },
    });
    // The released connection that was open when the upgrade arrived closed itself.
    await expect(stale.getCurrentManifestVersion()).rejects.toThrow(/connection is closed/);
    const stores = [first, second].map((result) =>
      result.status === "fulfilled" ? result.value : undefined,
    );
    const [one, two] = stores.map((store) =>
      store === undefined ? undefined : new MinnowDatabase(store, OPTIONS),
    );
    if (one === undefined || two === undefined) throw new Error("Both openers must succeed");
    // One connection merges; the other reads the merged history and writes on it.
    await mergeDeltas(one);
    expect((await two.query(ALL, { memoize: false })).rows).toEqual(rows);
    await writeCurrent(two, rows);
    await one.close();
    await two.close();
    for (const store of stores) store?.close();
    expect(await nativeVersion(indexedDB, name)).toBe(CURRENT_SCHEMA);
  });

  it("refuses a damaged merge record as corruption", async () => {
    const indexedDB = new IDBFactory();
    const name = crypto.randomUUID();
    await releasedIndexedDb(indexedDB, name);
    const store = await IndexedDbBlockStore.open({ name, indexedDB });
    const database = new MinnowDatabase(store, OPTIONS);
    const job = await mergeDeltas(database);
    await database.close();
    store.close();
    const key = `compaction-job/${job.id}`;
    const raw = (await readRawValue(indexedDB, name, "gc", key)) as {
      record: { rewritePlan: { logicalOrder: number } };
    };
    // The merged run claims a place one past its last source: it would reorder the history.
    raw.record.rewritePlan.logicalOrder += 1;
    await rawPut(indexedDB, name, "gc", key, raw);
    const damaged = await IndexedDbBlockStore.open({ name, indexedDB });
    await expect(damaged.getCompactionJob(job.id)).rejects.toBeInstanceOf(StorageCorruptionError);
    expect((await damaged.checkIntegrity({ mode: "full" })).ok).toBe(false);
    damaged.close();
  });
});

async function rawPut(
  indexedDB: IDBFactory,
  name: string,
  storeName: string,
  key: IDBValidKey,
  value: unknown,
): Promise<void> {
  const database = await new Promise<IDBDatabase>((resolve, reject) => {
    const request = indexedDB.open(name);
    request.onsuccess = () => resolve(request.result);
    request.onerror = () => reject(request.error ?? new Error("raw open failed"));
  });
  try {
    await new Promise<void>((resolve, reject) => {
      const transaction = database.transaction(storeName, "readwrite");
      transaction.objectStore(storeName).put(value, key);
      transaction.oncomplete = () => resolve();
      transaction.onabort = () => reject(transaction.error ?? new Error("raw write aborted"));
    });
  } finally {
    database.close();
  }
}

describe("OPFS: the 0.15.0 writer's deltas after the layout upgrade", () => {
  const marker = (shim: MemoryOpfs) =>
    new TextDecoder().decode(shim.readFileBytes(`minnowdb/${NAME}/format.json`));

  async function releasedOpfs(shim: MemoryOpfs): Promise<unknown[]> {
    const store = await ReleasedOpfsStore.open({ name: NAME, root: shim.root });
    const writer = new ReleasedDatabase(store, OPTIONS) as unknown as ReleasedWriter;
    const rows = await writeReleasedHistory(writer);
    await writer.close();
    store._crashForTests();
    expect(marker(shim)).toBe(`{"formatVersion":${String(RELEASED_LAYOUT)}}`);
    return rows;
  }

  it("merges them, writes on, reopens, and refuses the released reader unchanged", async () => {
    const shim = new MemoryOpfs();
    const rows = await releasedOpfs(shim);

    let store: BlockStore & { _crashForTests(): void } = await OpfsBlockStore.open({
      name: NAME,
      root: shim.root,
    });
    expect(marker(shim)).toBe(`{"formatVersion":${String(CURRENT_LAYOUT)}}`);
    let database = new MinnowDatabase(store, OPTIONS);
    expect((await database.query(ALL, { memoize: false })).rows).toEqual(rows);
    await mergeDeltas(database);
    const latest = await writeCurrent(database, rows);
    await database.close();
    store._crashForTests();

    store = await OpfsBlockStore.open({ name: NAME, root: shim.root });
    database = new MinnowDatabase(store, OPTIONS);
    expect((await database.query(ALL, { memoize: false })).rows).toEqual(latest);
    expect(await store.checkIntegrity?.({ mode: "full" })).toMatchObject({ ok: true });
    await database.close();
    store._crashForTests();

    const writes: string[] = [];
    shim.setWriteFault((path, phase) => {
      if (phase !== "flush") writes.push(`${phase}: ${path}`);
    });
    shim.setDeleteFault((path) => writes.push(`delete: ${path}`));
    await expect(ReleasedOpfsStore.open({ name: NAME, root: shim.root })).rejects.toMatchObject({
      name: "StorageFormatVersionError",
      actualVersion: CURRENT_LAYOUT,
      supportedVersion: RELEASED_LAYOUT,
      relation: "newer",
    });
    shim.setWriteFault(null);
    shim.setDeleteFault(null);
    expect(writes).toEqual([]);
    expect(marker(shim)).toBe(`{"formatVersion":${String(CURRENT_LAYOUT)}}`);
    store = await OpfsBlockStore.open({ name: NAME, root: shim.root });
    database = new MinnowDatabase(store, OPTIONS);
    expect((await database.query(ALL, { memoize: false })).rows).toEqual(latest);
    await database.close();
    store._crashForTests();
  });

  it("serializes concurrent current openers through the upgrade", async () => {
    const shim = new MemoryOpfs();
    const rows = await releasedOpfs(shim);
    const stores = await Promise.all(
      Array.from({ length: 3 }, () => OpfsBlockStore.open({ name: NAME, root: shim.root })),
    );
    try {
      expect(marker(shim)).toBe(`{"formatVersion":${String(CURRENT_LAYOUT)}}`);
      const [first, second] = stores.map((store) => new MinnowDatabase(store, OPTIONS));
      if (first === undefined || second === undefined) throw new Error("Openers are missing");
      await mergeDeltas(first);
      expect((await second.query(ALL, { memoize: false })).rows).toEqual(rows);
      await writeCurrent(second, rows);
      await first.close();
      await second.close();
    } finally {
      for (const store of stores) store._crashForTests();
    }
  });
});
