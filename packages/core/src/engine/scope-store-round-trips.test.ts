/**
 * What a statement inside a write scope costs the store. Each store call is an IndexedDB
 * transaction on that adapter, so a scope loop's speed is set by how many a statement makes:
 * the table record is resolved once per scope, a keyed UPDATE probes key presence once, and a
 * guarded upsert over a long committed history reads its pre-images by point reads rather than
 * by scanning the table's delta history.
 */
import { expect, it } from "vitest";
import { MemoryBlockStore, type BlockStore } from "../storage/index.js";
import { MinnowDatabase } from "./database.js";

function countingStore(): { store: BlockStore; calls: Map<string, number>; reset: () => void } {
  const calls = new Map<string, number>();
  const target = new MemoryBlockStore();
  const store = new Proxy(target, {
    get(object, property, receiver) {
      const value: unknown = Reflect.get(object, property, receiver);
      if (typeof value !== "function" || typeof property !== "string") return value;
      return (...args: unknown[]) => {
        calls.set(property, (calls.get(property) ?? 0) + 1);
        return (value as (...call: unknown[]) => unknown).apply(object, args);
      };
    },
  });
  return { store, calls, reset: () => calls.clear() };
}

it("resolves the table once per scope and probes a keyed UPDATE's keys once", async () => {
  const { store, calls, reset } = countingStore();
  const db = new MinnowDatabase(store, {});
  await db.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, amount INTEGER NOT NULL)");
  await db.execute("INSERT INTO items (id, amount) VALUES (1, 1), (2, 2), (3, 3)");
  reset();
  await db.write(async (tx) => {
    for (let round = 0; round < 20; round += 1) {
      await tx.execute(
        `UPDATE items SET amount = ${String(round)} WHERE id = ${String((round % 3) + 1)}`,
      );
    }
  });
  expect(calls.get("getTableByName") ?? 0).toBeLessThanOrEqual(2);
  expect(calls.get("getExistingUniqueKeys") ?? 0).toBeLessThanOrEqual(20);
  expect((await db.query("SELECT amount FROM items WHERE id = 3")).rows).toEqual([{ amount: 17 }]);
  await db.close();
});

it("reads a guarded upsert's pre-images by point read over a long committed history", async () => {
  const { store, calls, reset } = countingStore();
  const db = new MinnowDatabase(store, { autoCompact: false });
  await db.execute(
    "CREATE TABLE items (id INTEGER PRIMARY KEY, revision INTEGER NOT NULL, note VARCHAR)",
  );
  await db.insertBatch(
    "items",
    Array.from({ length: 200 }, (_, index) => ({ id: index, revision: 0, note: "seed" })),
  );
  // Two hundred committed single-row upserts: a delta segment each, none folded.
  for (let index = 0; index < 200; index += 1) {
    await db.upsertBatch("items", [{ id: index, revision: 1, note: `r${String(index)}` }]);
  }
  reset();
  const started = performance.now();
  await db.write(async (tx) => {
    for (let index = 0; index < 50; index += 1) {
      await tx.upsertBatch("items", [{ id: index, revision: 2, note: "guarded" }], {
        conflictWhere: { column: "revision", operator: "=", value: 1 },
      });
    }
  });
  const perStatementMs = (performance.now() - started) / 50;
  // The delta history is never scanned: the committed segments and their owners are listed
  // once for the scope, not once per statement (fifty statements used to mean fifty listings).
  expect(calls.get("listTableSegmentPage") ?? 0).toBeLessThanOrEqual(5);
  expect(calls.get("getTransactions") ?? 0).toBeLessThanOrEqual(5);
  expect(perStatementMs).toBeLessThan(5);
  expect((await db.query("SELECT COUNT(*) AS n FROM items WHERE revision = 2")).rows).toEqual([
    { n: 50 },
  ]);
  await db.close();
});
