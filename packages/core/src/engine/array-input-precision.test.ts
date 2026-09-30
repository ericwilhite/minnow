import { expect, it } from "vitest";
import { IDBFactory } from "fake-indexeddb";
import { MinnowDatabase } from "./database.js";
import { MemoryBlockStore } from "../storage/memory.js";
import { IndexedDbBlockStore } from "../storage/indexeddb.js";
import { OpfsBlockStore } from "../storage/opfs/store.js";
import { MemoryOpfs } from "../testing/opfs-shim.js";

it.each(["memory", "indexeddb", "opfs"] as const)(
  "%s: refuses lossy ARRAY text atomically and preserves exact string members across reopening",
  async (kind) => {
    const memory = new MemoryBlockStore();
    const indexedDB = new IDBFactory();
    const root = new MemoryOpfs().root;
    const open = () =>
      kind === "memory"
        ? Promise.resolve(memory)
        : kind === "indexeddb"
          ? IndexedDbBlockStore.open({ name: "arrays", indexedDB })
          : OpfsBlockStore.open({ name: "arrays", root });
    let store = await open();
    let db = new MinnowDatabase(store, { autoCompact: false, autoCollect: false });
    try {
      await db.execute("CREATE TABLE arrays (id INTEGER PRIMARY KEY, a INTEGER[])");
      for (const a of [
        "[9007199254740993]",
        "[1.0000000000000001]",
        "[1e400]",
        "[1e-400]",
        '[{"n":9007199254740993}]',
      ]) {
        await expect(db.insert("arrays", { id: 1, a })).rejects.toThrow(/lose precision/);
        expect((await db.query("SELECT COUNT(*) AS n FROM arrays")).rows).toEqual([{ n: 0 }]);
      }
      await db.insert("arrays", { id: 1, a: '["9007199254740993","1.0000000000000001"]' });
      await db.insert("arrays", { id: 2, a: "[9007199254740992,0.1,1e3,null]" });
      expect((await db.query("SELECT ARRAY[9007199254740993::NUMERIC] AS a")).rows).toEqual([
        { a: '["9007199254740993"]' },
      ]);
      await db.close();
      if (kind !== "memory") store.close();
      store = await open();
      db = new MinnowDatabase(store, { autoCompact: false, autoCollect: false });
      expect((await db.query("SELECT id,a FROM arrays ORDER BY id")).rows).toEqual([
        { id: 1, a: '["9007199254740993","1.0000000000000001"]' },
        { id: 2, a: "[9007199254740992,0.1,1000,null]" },
      ]);
    } finally {
      await db.close();
      store.close();
    }
  },
);
