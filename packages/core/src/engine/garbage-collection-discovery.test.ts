/**
 * Collection discovery decides committed transactions by their manifests, a page at a time. It
 * used to list and decide them one record per planning step, and every planning step is a
 * durable job update: a database retaining a few hundred transaction records paid a few hundred
 * storage writes per collection cycle, which on IndexedDB is a readwrite transaction each.
 */
import { expect, it } from "vitest";
import { MemoryBlockStore, type BlockStore } from "../storage/index.js";
import { MinnowDatabase } from "./database.js";

function counted(store: BlockStore): { store: BlockStore; calls: Map<string, number> } {
  const calls = new Map<string, number>();
  const proxy = new Proxy(store, {
    get(target, property, receiver) {
      const value: unknown = Reflect.get(target, property, receiver);
      if (typeof value !== "function" || typeof property !== "string") return value;
      return (...args: unknown[]) => {
        calls.set(property, (calls.get(property) ?? 0) + 1);
        return (value as (...args: unknown[]) => unknown).apply(target, args);
      };
    },
  });
  return { store: proxy, calls };
}

it("one collection cycle updates its plan per page of transactions, not per transaction", async () => {
  const memory = new MemoryBlockStore();
  const { store, calls } = counted(memory);
  const db = new MinnowDatabase(store, { autoCompact: false, autoCollect: false });
  try {
    await db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, amount INTEGER NOT NULL)");
    await db.insertBatch(
      "t",
      Array.from({ length: 50 }, (_, id) => ({ id, amount: 0 })),
    );
    const writes = 300;
    for (let index = 0; index < writes; index += 1) {
      await db.execute("UPDATE t SET amount = amount + 1 WHERE id = ?", [index % 50]);
    }
    const transactions = (await memory.getStorageStats()).transactionCount;
    expect(transactions).toBeGreaterThanOrEqual(writes);
    calls.clear();
    await db.collectGarbage({ retainRecentVersions: writes * 2 });
    // Nothing is old enough to reclaim, so the cycle only discovers. Each planning update covers
    // up to a page (64) of transactions; one per transaction would be at least 300.
    expect(calls.get("updateGarbageCollectionPlanning") ?? 0).toBeLessThan(40);
    expect(calls.get("listTransactionPage") ?? 0).toBeLessThan(20);
    // Their manifests are read as ordered pages, not one point read per transaction.
    expect(calls.get("getManifest") ?? 0).toBeLessThan(40);
    expect((await db.query("SELECT SUM(amount) AS total FROM t")).rows).toEqual([
      { total: writes },
    ]);
  } finally {
    await db.close();
  }
});

it("one collection cycle asks which segment blocks are current once per page of segments", async () => {
  const memory = new MemoryBlockStore();
  const { store, calls } = counted(memory);
  const db = new MinnowDatabase(store, { autoCompact: false, autoCollect: false });
  try {
    await db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, amount INTEGER NOT NULL)");
    for (let batch = 0; batch < 200; batch += 1) {
      await db.insertBatch("t", [{ id: batch, amount: 0 }]);
    }
    expect((await memory.getStorageStats()).segmentCount).toBeGreaterThanOrEqual(200);
    calls.clear();
    await db.collectGarbage({ retainRecentVersions: 400 });
    // Two hundred segments are four pages; one presence question per segment would be 200.
    expect(calls.get("hasManifestBlocks") ?? 0).toBeLessThan(20);
    expect((await db.query("SELECT COUNT(*) AS n FROM t")).rows).toEqual([{ n: 200 }]);
  } finally {
    await db.close();
  }
});

it("a live segment is proved current by one block, not one per column", async () => {
  const memory = new MemoryBlockStore();
  let askedIds = 0;
  const store = new Proxy(memory, {
    get(target, property, receiver) {
      const value: unknown = Reflect.get(target, property, receiver);
      if (typeof value !== "function") return value;
      return (...args: unknown[]) => {
        if (property === "hasManifestBlocks") askedIds += (args[1] as readonly string[]).length;
        return (value as (...args: unknown[]) => unknown).apply(target, args);
      };
    },
  });
  const db = new MinnowDatabase(store, { autoCompact: false, autoCollect: false });
  try {
    const columns = Array.from({ length: 30 }, (_, index) => `c${String(index)} INTEGER`);
    await db.execute(`CREATE TABLE t (id INTEGER PRIMARY KEY, ${columns.join(", ")})`);
    for (let id = 0; id < 100; id += 1) await db.insertBatch("t", [{ id }]);
    askedIds = 0;
    await db.collectGarbage({ retainRecentVersions: 400 });
    // A hundred live segments of 31 blocks: asking every block was 3,100 ids.
    expect(askedIds).toBeGreaterThan(0);
    expect(askedIds).toBeLessThanOrEqual(200);
    expect((await db.query("SELECT COUNT(*) AS n FROM t")).rows).toEqual([{ n: 100 }]);
  } finally {
    await db.close();
  }
});

const FIXED_POINT = { manifests: 3, transactions: 202, segments: 202, obsoleteBlocks: 128 };

it("paged discovery reclaims exactly what single-record discovery did", async () => {
  const store = new MemoryBlockStore();
  const db = new MinnowDatabase(store, { autoCompact: false, autoCollect: false });
  try {
    await db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, amount INTEGER NOT NULL)");
    await db.insertBatch(
      "t",
      Array.from({ length: 20 }, (_, id) => ({ id, amount: 0 })),
    );
    for (let index = 0; index < 200; index += 1) {
      await db.execute("UPDATE t SET amount = amount + 1 WHERE id = ?", [index % 20]);
    }
    await db.compactTable("t");
    // Retain nothing but the current version, and collect until a cycle changes nothing: every
    // superseded transaction, manifest, segment, and block is garbage.
    let previous = "";
    for (let cycle = 0; cycle < 20; cycle += 1) {
      await db.collectGarbage({ retainRecentVersions: 0 });
      const footprint = JSON.stringify(await store.getStorageStats());
      if (footprint === previous) break;
      previous = footprint;
    }
    const stats = await store.getStorageStats();
    // The fixed point single-record discovery reached on this workload.
    expect({
      manifests: stats.manifestCount,
      transactions: stats.transactionCount,
      segments: stats.segmentCount,
      obsoleteBlocks: stats.obsoleteBlockCount,
    }).toEqual(FIXED_POINT);
    expect((await db.query("SELECT COUNT(*) AS n, SUM(amount) AS total FROM t")).rows).toEqual([
      { n: 20, total: 200 },
    ]);
  } finally {
    await db.close();
  }
});
