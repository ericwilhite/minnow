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
import { pointReadTestHooks } from "./point-read.js";
import { heavyTestTimeout } from "./storage-test-helpers.js";

vi.setConfig({ testTimeout: heavyTestTimeout(120_000) });

const stores = [
  { name: "memory", create: async (): Promise<BlockStore> => new MemoryBlockStore() },
  {
    name: "indexeddb",
    create: async (): Promise<BlockStore> =>
      IndexedDbBlockStore.open({ name: crypto.randomUUID(), indexedDB: new IDBFactory() }),
  },
  {
    name: "opfs",
    create: async (): Promise<BlockStore> =>
      OpfsBlockStore.open({ name: crypto.randomUUID(), root: new MemoryOpfs().root }),
  },
];

afterEach(() => {
  pointReadTestHooks.disabled = false;
});

it("falls back before exceeding the bounded header inspection window", async () => {
  const database = new MinnowDatabase(new MemoryBlockStore(), {
    autoCompact: false,
    autoCollect: false,
  });
  try {
    await database.execute("CREATE TABLE history (id INTEGER PRIMARY KEY, amount INTEGER)");
    await database.insertBatch("history", [
      { id: 0, amount: 7 },
      { id: 1, amount: 0 },
    ]);
    for (let amount = 1; amount <= 1024; amount += 1) {
      await database.updateBatch("history", { keys: [1], changes: { amount: [amount] } });
    }
    const before = pointReadTestHooks.served;
    expect(
      (await database.query("SELECT amount FROM history WHERE id=0", { memoize: false })).rows,
    ).toEqual([{ amount: 7 }]);
    expect(pointReadTestHooks.served).toBe(before);
    expect(
      (await database.query("SELECT amount FROM history WHERE id=1", { memoize: false })).rows,
    ).toEqual([{ amount: 1024 }]);
  } finally {
    await database.close();
  }
});

describe.each(stores)("bounded point replay on $name", ({ create }) => {
  it("keeps unrelated single-block mutations outside the bounded point replay", async () => {
    const store = await create();
    const database = new MinnowDatabase(store, { autoCompact: false, autoCollect: false });
    try {
      await database.execute("CREATE TABLE history (id INTEGER PRIMARY KEY, amount INTEGER)");
      await database.insertBatch(
        "history",
        Array.from({ length: 512 }, (_, id) => ({ id, amount: id })),
      );
      for (let id = 1; id <= 300; id += 1) {
        await database.updateBatch("history", { keys: [id], changes: { amount: [id * 2] } });
      }
      const lookup = "SELECT id, amount FROM history WHERE id=?";
      const compare = async (reader: MinnowDatabase, fast: boolean) => {
        pointReadTestHooks.disabled = false;
        const before = pointReadTestHooks.served;
        const actual = await reader.query(lookup, { params: [0], memoize: false });
        expect(pointReadTestHooks.served > before).toBe(fast);
        pointReadTestHooks.disabled = true;
        const expected = await reader.query(lookup, { params: [0], memoize: false });
        pointReadTestHooks.disabled = false;
        expect(actual).toEqual(expected);
        return expected.rows;
      };
      expect(await compare(database, true)).toEqual([{ id: 0, amount: 0 }]);
      // A cold reader must inspect durable headers too, without relying on writer caches.
      const cold = new MinnowDatabase(store, { autoCompact: false, autoCollect: false });
      try {
        expect(await compare(cold, true)).toEqual([{ id: 0, amount: 0 }]);
      } finally {
        await cold.close();
      }
      await database.deleteBatch("history", { keys: [0] });
      expect(await compare(database, true)).toEqual([]);
      await database.insert("history", { id: 0, amount: 7 });
      expect(await compare(database, true)).toEqual([{ id: 0, amount: 7 }]);
      // Relevant history still has the same decoding cap and ordinary fallback.
      for (let amount = 8; amount < 150; amount += 1) {
        await database.updateBatch("history", { keys: [0], changes: { amount: [amount] } });
      }
      expect(await compare(database, false)).toEqual([{ id: 0, amount: 149 }]);
      await database.compactTable("history", { maxLevel0Segments: 1024 });
      expect(await compare(database, true)).toEqual([{ id: 0, amount: 149 }]);
    } finally {
      await database.close();
    }
  });
});
