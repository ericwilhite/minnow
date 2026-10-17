/**
 * Storage round trips per single-row statement. On IndexedDB every store call is a native
 * transaction, so a write that reads its own new blocks back, or a read that touches storage past
 * its freshness probe, pays for it in every statement. These count calls at the store contract
 * on a warm database with background maintenance off, so each number is the statement's own.
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

async function warmDatabase(): Promise<{ db: MinnowDatabase; calls: Map<string, number> }> {
  const { store, calls } = counted(new MemoryBlockStore());
  const db = new MinnowDatabase(store, { autoCompact: false, autoCollect: false });
  await db.execute(
    "CREATE TABLE t (id INTEGER PRIMARY KEY, amount INTEGER NOT NULL, label TEXT NOT NULL)",
  );
  await db.insertBatch(
    "t",
    Array.from({ length: 1_000 }, (_, id) => ({ id, amount: 0, label: `l${String(id % 7)}` })),
  );
  for (let id = 0; id < 20; id += 1) {
    await db.execute("UPDATE t SET amount = amount + 1 WHERE id = ?", [id]);
    await db.query("SELECT amount FROM t WHERE id = ?", { params: [id], memoize: false });
  }
  return { db, calls };
}

const blockReads = (calls: Map<string, number>): number =>
  (calls.get("getBlock") ?? 0) +
  (calls.get("getBlocks") ?? 0) +
  (calls.get("readManifestBlock") ?? 0);

it("a point update reads none of the blocks it or earlier writes encoded", async () => {
  const { db, calls } = await warmDatabase();
  try {
    for (let id = 100; id < 110; id += 1) {
      calls.clear();
      await db.execute("UPDATE t SET amount = amount + 1 WHERE id = ?", [id]);
      // The row lives in a decoded base block and every delta's zone map was recorded when the
      // delta was encoded, so the statement needs no block from storage.
      expect(blockReads(calls)).toBe(0);
      expect(calls.get("writeTransaction")).toBe(1);
      calls.clear();
      // Updating the same row again replays the delta just written, kept decoded.
      await db.execute("UPDATE t SET amount = amount + 1 WHERE id = ?", [id]);
      expect(blockReads(calls)).toBe(0);
    }
  } finally {
    await db.close();
  }
});

it("a point read after a write costs its probe and lease re-pin, and reads no block", async () => {
  const { db, calls } = await warmDatabase();
  try {
    for (let id = 200; id < 210; id += 1) {
      await db.execute("UPDATE t SET amount = amount + 1 WHERE id = ?", [id]);
      calls.clear();
      const { rows } = await db.query("SELECT amount FROM t WHERE id = ?", {
        params: [id],
        memoize: false,
      });
      expect(rows).toEqual([{ amount: 1 }]);
      // The replay decodes the new delta's key and amount blocks, which the write kept decoded;
      // the only other call moves the shared reader lease to the version the write published.
      expect(Object.fromEntries(calls)).toEqual({ getCatalogProbe: 1, moveLease: 1 });
    }
  } finally {
    await db.close();
  }
});
