/**
 * A tab can sleep in the middle of work — a laptop lid, a backgrounded phone — and wake with
 * its wall clock days ahead. The internal read leases it held have lapsed by then. Work that
 * renews one must carry on from the version it was reading, not fail: the version is still
 * there unless something reclaimed it, and pinning it again is what opening a lease does.
 */
import { IDBFactory } from "fake-indexeddb";
import { describe, expect, it } from "vitest";
import { MemoryOpfs } from "../testing/opfs-shim.js";
import {
  IndexedDbBlockStore,
  MemoryBlockStore,
  OpfsBlockStore,
  type BlockStore,
} from "../storage/index.js";
import { MinnowDatabase } from "./database.js";

const DAY = 24 * 60 * 60 * 1000;

const stores: Array<{ name: string; create: () => Promise<BlockStore> }> = [
  { name: "memory", create: async () => new MemoryBlockStore() },
  {
    name: "indexeddb",
    create: async () =>
      IndexedDbBlockStore.open({ name: crypto.randomUUID(), indexedDB: new IDBFactory() }),
  },
  {
    name: "opfs",
    create: async () =>
      OpfsBlockStore.open({ name: crypto.randomUUID(), root: new MemoryOpfs().root }),
  },
];

/** A store whose block reads move the clock forward once, the first time after `arm()`. */
function sleepingStore(store: BlockStore, advance: () => void): { arm: () => void } {
  let armed = false;
  for (const method of ["getBlock", "getBlocks", "readManifestBlock"] as const) {
    const original = (store as unknown as Record<string, (...args: unknown[]) => unknown>)[method];
    if (typeof original !== "function") continue;
    Object.defineProperty(store, method, {
      configurable: true,
      value: (...args: unknown[]) => {
        if (armed) {
          armed = false;
          advance();
        }
        return original.apply(store, args);
      },
    });
  }
  return { arm: () => (armed = true) };
}

describe.each(stores)("a clock that jumps days mid-read on $name", ({ create }) => {
  async function setup() {
    let current = Date.parse("2026-10-15T12:00:00.000Z");
    const store = await create();
    const sleep = sleepingStore(store, () => (current += 2 * DAY));
    const database = new MinnowDatabase(store, {
      rowsPerBlock: 16,
      autoCompact: false,
      autoCollect: false,
      now: () => new Date(current),
    });
    await database.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, x INTEGER, label TEXT)");
    // Many small commits, so a fold has many segments and blocks to read through.
    for (let batch = 0; batch < 24; batch += 1) {
      await database.insertBatch(
        "t",
        Array.from({ length: 32 }, (_, index) => {
          const id = batch * 32 + index;
          return { id, x: id % 10, label: `row ${String(id)}` };
        }),
      );
    }
    return { store, database, sleep };
  }

  it("finishes a compaction whose read lease lapsed while it planned", async () => {
    const { store, database, sleep } = await setup();
    try {
      sleep.arm();
      const result = await database.compactTable("t");
      expect(result.compacted).toBe(true);
      expect((await database.query("SELECT count(*) AS n, sum(x) AS s FROM t")).rows).toEqual([
        { n: 768, s: 3448 },
      ]);
      await database.collectGarbage();
      expect((await database.query("SELECT label FROM t WHERE id = 700")).rows).toEqual([
        { label: "row 700" },
      ]);
    } finally {
      await database.close();
      store.close();
    }
  });

  it("answers a query whose read lease lapsed while it scanned", async () => {
    const { store, database, sleep } = await setup();
    try {
      await database.query("SELECT count(*) FROM t");
      sleep.arm();
      expect(
        (
          await database.query("SELECT count(*) AS n, sum(x) AS s FROM t WHERE label LIKE 'row%'", {
            memoize: false,
          })
        ).rows,
      ).toEqual([{ n: 768, s: 3448 }]);
      await database.collectGarbage();
    } finally {
      await database.close();
      store.close();
    }
  });
});
