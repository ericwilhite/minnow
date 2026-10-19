/**
 * A dropped table's blocks are reclaimed by the next collection pass, with no read in between.
 *
 * The engine keeps one shared reader lease across queries so the next query at the same version
 * reuses it. Left idle at a pre-drop version, that lease rooted every block the dropped table
 * held, and an explicit `collectGarbage()` — the call an application makes right after a drop
 * to get its space back — retained all of them, pass after pass, until some unrelated read moved
 * the lease. Every pass now releases the connection's own idle lease before it plans.
 */
import { IDBFactory } from "fake-indexeddb";
import { expect, it } from "vitest";
import {
  IndexedDbBlockStore,
  MemoryBlockStore,
  OpfsBlockStore,
  segmentStoredBlockIds,
  type BlockStore,
} from "../storage/index.js";
import { MemoryOpfs } from "../testing/opfs-shim.js";
import { MinnowDatabase } from "./database.js";
import { allSegmentRecords } from "./storage-test-helpers.js";

const stores: Record<string, () => Promise<BlockStore>> = {
  memory: () => Promise.resolve(new MemoryBlockStore()),
  indexeddb: () => IndexedDbBlockStore.open({ name: "drop-table", indexedDB: new IDBFactory() }),
  opfs: () => OpfsBlockStore.open({ name: "drop-table", root: new MemoryOpfs().root }),
};

it.each(Object.entries(stores))(
  "collectGarbage right after DROP TABLE reclaims every block of the table on %s",
  async (_kind, open) => {
    const store = await open();
    const db = new MinnowDatabase(store, { autoCompact: false, autoCollect: false });
    try {
      await db.execute("CREATE TABLE a (id INTEGER PRIMARY KEY, c1 TEXT, c2 TEXT, c3 TEXT)");
      await db.insertBatch(
        "a",
        Array.from({ length: 300 }, (_, id) => ({ id, c1: `x${String(id)}`, c2: "y", c3: "z" })),
      );
      for (let id = 300; id < 306; id += 1) {
        await db.insertBatch("a", [{ id, c1: `x${String(id)}`, c2: "y", c3: "z" }]);
      }
      await db.execute("UPDATE a SET c1 = 'changed' WHERE id = 302");
      // A read pins the shared lease at the pre-drop version; nothing reads after the drop.
      expect((await db.query("SELECT COUNT(*) AS n FROM a")).rows).toEqual([{ n: 306 }]);
      const table = await store.getTableByName("a");
      const ids = (await allSegmentRecords(store))
        .filter((segment) => segment.tableId === table?.id)
        .flatMap((segment) => segmentStoredBlockIds(segment));
      expect(ids.length).toBeGreaterThan(8);
      await db.dropTable("a");
      const result = await db.collectGarbage();
      expect(result.retainedBlockCount).toBe(0);
      expect(result.reclaimedBlockCount).toBe(ids.length);
      for (const id of ids) expect(await store.getBlock(id)).toBeUndefined();
      // The database stays usable: the next read takes a fresh lease.
      await db.execute("CREATE TABLE b (id INTEGER PRIMARY KEY)");
      await db.execute("INSERT INTO b VALUES (1)");
      expect((await db.query("SELECT COUNT(*) AS n FROM b")).rows).toEqual([{ n: 1 }]);
    } finally {
      await db.close();
    }
  },
);
