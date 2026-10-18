/**
 * Collection planning pages through retired block provenance after every few dozen commits.
 * Live provenance far outnumbers retired provenance, so the listing must find the retired
 * records without paying for the live ones: the record core keeps them in their own ordered
 * index, and IndexedDB reads provenance a page per round trip instead of a cursor step per
 * record. These tests hold every store's listing to the blocks a fold actually retired, in
 * block-id order across cursor pages, and bound IndexedDB's round trips.
 */
import { IDBCursor, IDBFactory, IDBKeyRange, IDBObjectStore } from "fake-indexeddb";
import { afterEach, expect, it, vi } from "vitest";
import { MinnowDatabase } from "../engine/database.js";
import { MemoryOpfs } from "../testing/opfs-shim.js";
import { IndexedDbBlockStore } from "./indexeddb.js";
import { MemoryBlockStore, OpfsBlockStore, type BlockStore } from "./index.js";
import { recordCoreTestHooks } from "./toolkit/record-core.js";

afterEach(() => {
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
});

async function liveBlockIds(store: BlockStore, version: number): Promise<Set<string>> {
  const ids = new Set<string>();
  let afterBlockId: string | null = null;
  do {
    const page = await store.listManifestBlockPage({ version, afterBlockId, limit: 64 });
    for (const record of page.records) ids.add(record.blockId);
    afterBlockId = page.nextCursor;
  } while (afterBlockId !== null);
  return ids;
}

async function retiredBlockIds(
  store: BlockStore,
  removedThroughVersion: number,
  limit: number,
): Promise<string[]> {
  const ids: string[] = [];
  let afterBlockId: string | null = null;
  do {
    const page = await store.listRetiredManifestBlockPage({
      removedThroughVersion,
      afterBlockId,
      limit,
    });
    ids.push(...page.records.map((record) => record.blockId));
    afterBlockId = page.nextCursor;
  } while (afterBlockId !== null);
  return ids;
}

const adapters: Array<{ name: string; open: () => Promise<BlockStore> }> = [
  { name: "memory", open: async () => new MemoryBlockStore() },
  {
    name: "opfs",
    open: () => OpfsBlockStore.open({ name: crypto.randomUUID(), root: new MemoryOpfs().root }),
  },
  {
    name: "indexeddb",
    open: () => {
      // Browsers install the key-range constructor globally, and the batched read needs it.
      vi.stubGlobal("IDBKeyRange", IDBKeyRange);
      return IndexedDbBlockStore.open({ name: crypto.randomUUID(), indexedDB: new IDBFactory() });
    },
  },
];

for (const adapter of adapters) {
  it(`${adapter.name}: lists exactly the retired provenance, in order, among live records`, async () => {
    const store = await adapter.open();
    const db = new MinnowDatabase(store, { autoCompact: false, autoCollect: false });
    try {
      await db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b TEXT)");
      for (let id = 0; id < 20; id += 1) {
        await db.insertBatch("t", [{ id, a: id, b: `v${String(id)}` }]);
      }
      const beforeFold = await store.getCurrentManifestVersion();
      if (beforeFold === null) throw new Error("nothing was published");
      const liveBefore = await liveBlockIds(store, beforeFold);
      await db.compactTable("t");
      // Live provenance on both sides of the retired ids in block-id order.
      for (let id = 20; id < 60; id += 1) {
        await db.insertBatch("t", [{ id, a: id, b: `v${String(id)}` }]);
      }
      const current = await store.getCurrentManifestVersion();
      if (current === null) throw new Error("nothing was published");
      const liveNow = await liveBlockIds(store, current);
      const expected = [...liveBefore].filter((id) => !liveNow.has(id)).sort();
      expect(expected.length).toBeGreaterThan(0);
      expect(liveNow.size).toBeGreaterThan(expected.length);
      for (const limit of [1, 7, 64]) {
        expect(await retiredBlockIds(store, current, limit)).toEqual(expected);
      }
      // Retired after the cutoff: not listed.
      expect(await retiredBlockIds(store, beforeFold, 64)).toEqual([]);
    } finally {
      await db.close();
    }
  });
}

it("indexeddb: the retired listing reads provenance a page per round trip", async () => {
  vi.stubGlobal("IDBKeyRange", IDBKeyRange);
  const store = await IndexedDbBlockStore.open({
    name: crypto.randomUUID(),
    indexedDB: new IDBFactory(),
  });
  const db = new MinnowDatabase(store, { autoCompact: false, autoCollect: false });
  try {
    await db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER)");
    // Several hundred live provenance records and none retired: the listing reads them all.
    for (let id = 0; id < 300; id += 1) await db.insertBatch("t", [{ id, a: id }]);
    const current = await store.getCurrentManifestVersion();
    if (current === null) throw new Error("nothing was published");
    let catalogReads = 0;
    for (const method of ["get", "getAll", "getAllKeys", "openCursor"] as const) {
      // eslint-disable-next-line @typescript-eslint/unbound-method -- Called with its native receiver below.
      const original = IDBObjectStore.prototype[method] as (...args: unknown[]) => IDBRequest;
      vi.spyOn(IDBObjectStore.prototype, method).mockImplementation(function (
        this: IDBObjectStore,
        ...args: unknown[]
      ) {
        if (this.name === "catalog") catalogReads += 1;
        return original.apply(this, args);
      } as never);
    }
    // eslint-disable-next-line @typescript-eslint/unbound-method -- Called with its native receiver below.
    const advance = IDBCursor.prototype.continue;
    vi.spyOn(IDBCursor.prototype, "continue").mockImplementation(function (
      this: IDBCursor,
      ...args: unknown[]
    ) {
      catalogReads += 1;
      return advance.apply(this, args as never);
    });
    expect(await retiredBlockIds(store, current, 64)).toEqual([]);
    // A cursor visited each of the 600 records with its own round trip.
    expect(catalogReads).toBeLessThan(20);
  } finally {
    await db.close();
  }
});

it("memory: the retired listing examines no live provenance", async () => {
  const store = new MemoryBlockStore();
  const db = new MinnowDatabase(store, { autoCompact: false, autoCollect: false });
  try {
    await db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER)");
    for (let id = 0; id < 100; id += 1) await db.insertBatch("t", [{ id, a: id }]);
    const current = await store.getCurrentManifestVersion();
    if (current === null) throw new Error("nothing was published");
    const before = recordCoreTestHooks.retiredProvenanceVisited;
    expect(await retiredBlockIds(store, current, 64)).toEqual([]);
    // Two hundred live records, none retired: walking provenance in id order visited them all.
    expect(recordCoreTestHooks.retiredProvenanceVisited - before).toBe(0);
  } finally {
    await db.close();
  }
});
