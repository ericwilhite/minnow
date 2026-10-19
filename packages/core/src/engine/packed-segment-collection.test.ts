/**
 * Packed segments and the paths that count their one stored block: collection under a reader
 * lease, DROP COLUMN, write results, snapshots after a schema change, and an OPFS follower
 * whose leader crashes. Each one failed in review only when a count assumed a packed segment
 * stored one block per column; these keep every store on `segmentStoredBlockIds`.
 */
import { afterEach, describe, expect, it, vi } from "vitest";
import { IDBFactory } from "fake-indexeddb";
import { MinnowDatabase as ReleasedDatabase } from "@minnowdb/core-layout12";
import { MemoryBlockStore as ReleasedMemoryStore } from "@minnowdb/core-layout12/storage/memory";
import {
  IndexedDbBlockStore,
  MemoryBlockStore,
  OpfsBlockStore,
  segmentStoredBlockIds,
  type BlockStore,
  type SegmentRecord,
} from "../storage/index.js";
import { MemoryOpfs } from "../testing/opfs-shim.js";
import { MinnowDatabase } from "./database.js";
import { packedSegmentTestHooks } from "./packed-members.js";
import {
  allSegmentRecords,
  heavyTestTimeout,
  withoutPackedSegments,
} from "./storage-test-helpers.js";

vi.setConfig({ testTimeout: heavyTestTimeout(120_000) });

afterEach(() => {
  packedSegmentTestHooks.forcePacking = false;
  packedSegmentTestHooks.missIsFatal = false;
});

const KINDS = ["memory", "indexeddb", "opfs"] as const;
type StoreKind = (typeof KINDS)[number];
const OPTIONS = { autoCompact: false, autoCollect: false } as const;
const WIDTH = 24;

interface OpenedStore {
  store: BlockStore;
  reopen(): Promise<BlockStore>;
  crash(): void;
}

async function openStore(kind: StoreKind): Promise<OpenedStore> {
  if (kind === "memory") {
    const store = new MemoryBlockStore();
    return { store, reopen: async () => store, crash: () => undefined };
  }
  if (kind === "indexeddb") {
    const options = { name: crypto.randomUUID(), indexedDB: new IDBFactory() };
    const store = await IndexedDbBlockStore.open(options);
    return {
      store,
      reopen: () => IndexedDbBlockStore.open(options),
      crash: () => store.close(),
    };
  }
  // One database name per store: stores in one process coordinate by name, whatever root.
  const shim = new MemoryOpfs();
  const name = `packed-collection-${crypto.randomUUID()}`;
  const store = await OpfsBlockStore.open({ name, root: shim.root });
  return {
    store,
    reopen: () => OpfsBlockStore.open({ name, root: shim.root }),
    crash: () => store._crashForTests(),
  };
}

function columns(): Array<{
  name: string;
  type: "number" | "string" | "boolean";
  nullable?: boolean;
}> {
  const result: Array<{ name: string; type: "number" | "string" | "boolean"; nullable?: boolean }> =
    [
      { name: "id", type: "number" },
      { name: "flag", type: "boolean", nullable: true },
    ];
  for (let index = 0; index < WIDTH; index += 1) {
    result.push({
      name: `c${String(index)}`,
      type: index % 2 === 0 ? "number" : "string",
      nullable: true,
    });
  }
  return result;
}

function row(id: number): Record<string, unknown> {
  const value: Record<string, unknown> = { id, flag: id % 3 === 0 ? null : id % 2 === 0 };
  for (let index = 0; index < WIDTH; index += 1) {
    value[`c${String(index)}`] =
      (id + index) % 7 === 0 ? null : index % 2 === 0 ? id * 10 + index : `v${String(id + index)}`;
  }
  return value;
}

async function packedSegments(store: BlockStore): Promise<SegmentRecord[]> {
  return (await allSegmentRecords(store)).filter((segment) => segment.packedBlockId !== undefined);
}

async function visiblePacked(db: MinnowDatabase, table: string): Promise<string[]> {
  const ids: string[] = [];
  let cursor: Parameters<MinnowDatabase["listVisibleSegmentPage"]>[1] = {};
  for (;;) {
    const page = await db.listVisibleSegmentPage(table, cursor);
    for (const record of page.records) if (record.packedBlockId !== undefined) ids.push(record.id);
    if (page.nextCursor === null) return ids;
    cursor = { cursor: page.nextCursor };
  }
}

/** 300 bulk rows (ordinary blocks), then six single-row inserts, an update, and a delete. */
async function seed(db: MinnowDatabase, table = "wide"): Promise<unknown[]> {
  await db.createTable({ name: table, uniqueKey: "id", columns: columns() });
  await db.insertBatch(table, Array.from({ length: 300 }, (_, index) => row(index)) as never);
  for (let id = 300; id < 306; id += 1) await db.insertBatch(table, [row(id)] as never);
  await db.execute(`UPDATE ${table} SET c1 = 'changed' WHERE id = 302`);
  await db.deleteBatch(table, { keys: [7] });
  return readAll(db, table);
}

async function readAll(db: MinnowDatabase, table = "wide"): Promise<unknown[]> {
  return (await db.query(`SELECT * FROM ${table} ORDER BY id`, { memoize: false })).rows;
}

async function stored(store: BlockStore, ids: readonly string[]): Promise<string[]> {
  const found: string[] = [];
  for (const id of ids) if ((await store.getBlock(id)) !== undefined) found.push(id);
  return found;
}

function lifetime(): { createdAt: string; expiresAt: string } {
  const created = Date.now();
  return {
    createdAt: new Date(created).toISOString(),
    expiresAt: new Date(created + 10 * 60_000).toISOString(),
  };
}

describe.each(KINDS)("packed segment collection on %s", (kind) => {
  it("keeps packed blocks for a leased version through a fold and a collection, then reclaims them", async () => {
    packedSegmentTestHooks.missIsFatal = true;
    const { store } = await openStore(kind);
    const db = new MinnowDatabase(store, OPTIONS);
    try {
      const rows = await seed(db);
      const packed = await packedSegments(store);
      expect(packed.length).toBeGreaterThanOrEqual(7);
      const packedIds = packed.map((segment) => segment.packedBlockId ?? "");
      const v0 = await store.getCurrentManifestVersion();
      if (v0 === null) throw new Error("Expected a manifest");
      // A durable reader lease on v0, as another tab's long read holds one.
      const lease = {
        id: `lease/${crypto.randomUUID()}`,
        kind: "reader" as const,
        manifestVersion: v0,
        ownerId: "lease-owner",
        ...lifetime(),
        revision: 0,
      };
      await store.createLease(lease);

      await db.compactTable("wide");
      expect(await visiblePacked(db, "wide")).toEqual([]);
      await db.collectGarbage();
      expect(await stored(store, packedIds)).toEqual(packedIds);
      expect(await db.readTable("wide", v0)).toEqual(rows);
      if (kind !== "memory") {
        expect((await store.checkIntegrity?.({ mode: "full" }))?.issues).toEqual([]);
      }

      await store.removeLease({ id: lease.id, ownerId: lease.ownerId });
      await db.collectGarbage();
      expect(await stored(store, packedIds)).toEqual([]);
      expect(await packedSegments(store)).toEqual([]);
      expect(await readAll(db)).toEqual(rows);
      if (kind !== "memory") {
        expect((await store.checkIntegrity?.({ mode: "full" }))?.issues).toEqual([]);
      }
    } finally {
      await db.close();
    }
  });

  it("keeps a packed block through DROP COLUMN and reclaims it after the fold", async () => {
    packedSegmentTestHooks.missIsFatal = true;
    const { store } = await openStore(kind);
    const db = new MinnowDatabase(store, OPTIONS);
    try {
      const rows = await seed(db);
      const packedIds = (await packedSegments(store)).map((segment) => segment.packedBlockId ?? "");
      await db.execute("ALTER TABLE wide DROP COLUMN c1");
      await db.collectGarbage();
      // The dropped column's member stays in the block, named by no segment: still every block.
      expect(await stored(store, packedIds)).toEqual(packedIds);
      const expected = rows.map((value) => {
        const { c1, ...rest } = value as Record<string, unknown>;
        void c1;
        return rest;
      });
      expect(await readAll(db)).toEqual(expected);
      if (kind !== "memory") {
        expect((await store.checkIntegrity?.({ mode: "full" }))?.issues).toEqual([]);
      }
      await db.compactTable("wide");
      await db.collectGarbage();
      expect(await stored(store, packedIds)).toEqual([]);
      expect(await readAll(db)).toEqual(expected);
    } finally {
      await db.close();
    }
  });

  it("reports one stored block and its bytes for a packed write, one per column otherwise", async () => {
    const { store } = await openStore(kind);
    const db = new MinnowDatabase(store, OPTIONS);
    const control = new MinnowDatabase(withoutPackedSegments(store), OPTIONS);
    try {
      await db.createTable({ name: "wide", uniqueKey: "id", columns: columns() });
      const packedResult = await db.insertBatch("wide", [row(1)] as never);
      const segment = (await allSegmentRecords(store)).find(
        (candidate) => candidate.id === packedResult.segmentId,
      );
      expect(segment?.packedBlockId).toBe(packedResult.segmentId);
      expect(packedResult.blockCount).toBe(1);
      expect(packedResult.storedBytes).toBe(
        (await store.getBlock(packedResult.segmentId))?.byteLength,
      );

      const unpackedResult = await control.insertBatch("wide", [row(2)] as never);
      const unpacked = (await allSegmentRecords(store)).find(
        (candidate) => candidate.id === unpackedResult.segmentId,
      );
      expect(unpacked?.packedBlockId).toBeUndefined();
      expect(unpackedResult.blockCount).toBe(columns().length);
      let bytes = 0;
      for (const id of segmentStoredBlockIds(unpacked ?? { columnBlockIds: {} })) {
        bytes += (await store.getBlock(id))?.byteLength ?? 0;
      }
      expect(unpackedResult.storedBytes).toBe(bytes);
    } finally {
      await control.close();
      await db.close();
    }
  });

  it.each([false, true])(
    "forced=%s: a snapshot taken after DROP COLUMN and ADD COLUMN imports here and in 0.16.0",
    async (forced) => {
      packedSegmentTestHooks.forcePacking = forced;
      const { store } = await openStore(kind);
      const db = new MinnowDatabase(store, OPTIONS);
      try {
        await seed(db);
        await db.execute("ALTER TABLE wide DROP COLUMN c1");
        await db.execute("ALTER TABLE wide ADD COLUMN extra TEXT DEFAULT 'x'");
        await db.insertBatch("wide", [{ ...row(500), c1: undefined, extra: "y" }] as never);
        const expected = await readAll(db);
        expect((await packedSegments(store)).length).toBeGreaterThan(0);
        const bytes = await db.exportSnapshot();

        const current = new MinnowDatabase(new MemoryBlockStore(), OPTIONS);
        await current.importSnapshot(bytes);
        expect(await readAll(current)).toEqual(expected);
        expect(await visiblePacked(current, "wide")).toEqual([]);
        await current.close();

        const released = new ReleasedDatabase(new ReleasedMemoryStore(), OPTIONS);
        await released.importSnapshot(bytes);
        expect((await released.query("SELECT * FROM wide ORDER BY id")).rows).toEqual(expected);
        await released.close();

        // Imported into a packing store, the next small write packs again.
        const again = await openStore(kind);
        const back = new MinnowDatabase(again.store, OPTIONS);
        try {
          await back.importSnapshot(bytes);
          expect(await readAll(back)).toEqual(expected);
          await back.insertBatch("wide", [{ ...row(501), c1: undefined, extra: "z" }] as never);
          expect((await packedSegments(again.store)).length).toBe(1);
          expect((await readAll(back)).length).toBe(expected.length + 1);
          if (kind !== "memory") {
            expect((await again.store.checkIntegrity?.({ mode: "full" }))?.issues).toEqual([]);
          }
        } finally {
          await back.close();
        }
      } finally {
        await db.close();
      }
    },
  );
});

describe("OPFS follower and packed blocks", () => {
  async function pair(
    name: string,
  ): Promise<{ shim: MemoryOpfs; leader: OpfsBlockStore; follower: OpfsBlockStore }> {
    const shim = new MemoryOpfs();
    const leader = await OpfsBlockStore.open({ name, root: shim.root, rpcTimeoutMs: 30_000 });
    const follower = await OpfsBlockStore.open({ name, root: shim.root, rpcTimeoutMs: 30_000 });
    await follower.listTables();
    expect(leader._isLeaderForTests()).toBe(true);
    expect(follower._isLeaderForTests()).toBe(false);
    return { shim, leader, follower };
  }

  it("a follower's packed commits survive the leader's crash and fold on a fresh leader", async () => {
    packedSegmentTestHooks.missIsFatal = true;
    const name = `packed-follower-${crypto.randomUUID()}`;
    const { shim, leader, follower } = await pair(name);
    const db = new MinnowDatabase(follower, OPTIONS);
    const rows = await seed(db);
    expect((await packedSegments(follower)).length).toBeGreaterThanOrEqual(7);
    // The leader's own engine lists the follower's segments before it reads their members.
    const leaderDb = new MinnowDatabase(leader, OPTIONS);
    expect(await readAll(leaderDb)).toEqual(rows);
    await leaderDb.close();
    await db.close();
    follower.close();
    leader._crashForTests();

    const reopened = await OpfsBlockStore.open({ name, root: shim.root });
    const reader = new MinnowDatabase(reopened, OPTIONS);
    try {
      expect(await readAll(reader)).toEqual(rows);
      expect((await reopened.checkIntegrity({ mode: "full" })).ok).toBe(true);
      await reader.compactTable("wide");
      await reader.collectGarbage();
      expect(await readAll(reader)).toEqual(rows);
      expect((await reopened.checkIntegrity({ mode: "full" })).ok).toBe(true);
    } finally {
      await reader.close();
      reopened.close();
    }
  });

  it("the leader crashing during a follower's packed commit leaves it whole or absent", async () => {
    packedSegmentTestHooks.missIsFatal = true;
    const name = `packed-follower-crash-${crypto.randomUUID()}`;
    const { shim, leader, follower } = await pair(name);
    const db = new MinnowDatabase(follower, OPTIONS);
    const rows = await seed(db);
    const write = db.insertBatch("wide", [row(800)] as never);
    leader._crashForTests();
    const committed = await write.then(
      () => true,
      () => false,
    );
    await db.close().catch(() => undefined);
    follower.close();
    const reopened = await OpfsBlockStore.open({ name, root: shim.root });
    const reader = new MinnowDatabase(reopened, OPTIONS);
    try {
      const after = await readAll(reader);
      const has800 = after.some((value) => (value as { id: number }).id === 800);
      if (committed) expect(has800).toBe(true);
      expect(after.filter((value) => (value as { id: number }).id !== 800)).toEqual(rows);
      expect((await reopened.checkIntegrity({ mode: "full" })).ok).toBe(true);
    } finally {
      await reader.close();
      reopened.close();
    }
  });
});
