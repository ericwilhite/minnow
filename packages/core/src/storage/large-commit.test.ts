/**
 * A commit with many UNIQUE key changes checks them a slice at a time before it commits, and on
 * OPFS encodes its log frame the same way, so the step that must not be split — applying the
 * commit and writing its frame — stays short. These pin that such a commit still refuses a
 * conflicting key without changing anything, publishes exactly what it wrote, and on OPFS that
 * the frame it encoded ahead replays after a crash.
 */
import { describe, expect, it, vi } from "vitest";
import { MinnowDatabase } from "../engine/database.js";
import { heavyTestTimeout } from "../engine/storage-test-helpers.js";
import { MemoryOpfs } from "../testing/opfs-shim.js";
import { MemoryBlockStore, type BlockStore } from "./index.js";
import { OpfsBlockStore } from "./opfs/index.js";

vi.setConfig({ testTimeout: heavyTestTimeout(60_000) });

const ROWS = 40_000;

function rows(start: number, count: number, version: number) {
  return Array.from({ length: count }, (_, index) => ({
    id: start + index,
    label: `label-${String(start + index)}-${String(version)}`,
  }));
}

async function itemsTable(store: BlockStore): Promise<MinnowDatabase> {
  const db = new MinnowDatabase(store, { autoCompact: false, autoCollect: false });
  await db.createTable({
    name: "items",
    uniqueKey: "id",
    columns: [
      { name: "id", type: "number" },
      { name: "label", type: "string" },
    ],
  });
  return db;
}

const stores: Array<{ name: string; open: () => Promise<BlockStore> }> = [
  { name: "memory", open: async () => new MemoryBlockStore() },
  {
    name: "opfs",
    open: () => OpfsBlockStore.open({ name: "large", root: new MemoryOpfs().root }),
  },
];

describe.each(stores)("large commits ($name)", ({ open }) => {
  it("refuses a conflicting key without changing anything, then publishes what it wrote", async () => {
    const store = await open();
    const db = await itemsTable(store);
    await db.insertBatch("items", rows(0, ROWS, 0));
    const version = await store.getCurrentManifestVersion();
    // Every key but the last is new; the last collides with a committed row.
    const colliding = [...rows(ROWS, ROWS - 1, 0), { id: 7, label: "duplicate" }];
    await expect(db.insertBatch("items", colliding)).rejects.toThrow(/unique|duplicate/i);
    expect(await store.getCurrentManifestVersion()).toBe(version);
    expect((await db.query("SELECT COUNT(*) AS n FROM items")).rows).toEqual([{ n: ROWS }]);

    await db.upsertBatch("items", rows(0, ROWS, 1));
    await db.insertBatch("items", rows(ROWS, ROWS, 1));
    expect((await db.query("SELECT COUNT(*) AS n FROM items")).rows).toEqual([{ n: ROWS * 2 }]);
    expect(
      (await db.query("SELECT label FROM items WHERE id = ?", { params: [123] })).rows,
    ).toEqual([{ label: "label-123-1" }]);
    await expect(db.insertBatch("items", [{ id: ROWS + 5, label: "again" }])).rejects.toThrow(
      /unique|duplicate/i,
    );
    await db.close();
    store.close();
  });
});

describe("large commits on OPFS", () => {
  it("replays a large commit's ahead-encoded frame after a crash", async () => {
    const shim = new MemoryOpfs();
    const store = await OpfsBlockStore.open({
      name: "replay",
      root: shim.root,
      checkpointEntries: 1_000_000,
    });
    const db = await itemsTable(store);
    await db.insertBatch("items", rows(0, ROWS, 0));
    await db.upsertBatch("items", rows(0, ROWS, 1));
    store._crashForTests();
    await db.close().catch(() => undefined);

    const reopened = await OpfsBlockStore.open({ name: "replay", root: shim.root });
    const reader = new MinnowDatabase(reopened, { autoCompact: false, autoCollect: false });
    expect((await reader.query("SELECT COUNT(*) AS n FROM items")).rows).toEqual([{ n: ROWS }]);
    expect(
      (await reader.query("SELECT label FROM items WHERE id = ?", { params: [ROWS - 1] })).rows,
    ).toEqual([{ label: `label-${String(ROWS - 1)}-1` }]);
    await expect(reader.insertBatch("items", [{ id: 3, label: "dup" }])).rejects.toThrow(
      /unique|duplicate/i,
    );
    expect((await reopened.checkIntegrity({ mode: "full" })).ok).toBe(true);
    await reader.close();
    reopened.close();
  });
});
