/**
 * Snapshot format 2: format 1's container and frames, written only when the snapshot holds the
 * engine's internal key-forwards table (`keyForwards: true`). A release through 0.14.1 would
 * restore that record as an ordinary table and lose its forwards, so it must refuse the
 * snapshot; the version is how it knows. A snapshot without the record is still format 1, so
 * older builds keep importing it.
 */
import { IDBFactory } from "fake-indexeddb";
import { describe, expect, it } from "vitest";
import { MinnowDatabase as ReleasedDatabase } from "@minnowdb/core-layout9";
import { MemoryBlockStore as ReleasedMemoryStore } from "@minnowdb/core-layout9/storage/memory";
import { readSnapshotSummary as releasedSummary } from "@minnowdb/core-layout9/storage/snapshots";
import { MinnowDatabase } from "../engine/database.js";
import { keyForwardsTableRecord } from "../engine/key-forwards.js";
import { MemoryOpfs } from "../testing/opfs-shim.js";
import { IndexedDbBlockStore } from "./indexeddb.js";
import { MemoryBlockStore } from "./memory.js";
import { OpfsBlockStore } from "./opfs/index.js";
import {
  decodeSnapshotFrameStream,
  encodeSnapshotFrameStreamHeader,
  readSnapshotSummary,
} from "./snapshot.js";
import type { BlockStore, SnapshotFrameStreamHeader } from "./types.js";

const KEY_FORWARDS = keyForwardsTableRecord("2026-10-01T12:00:00.000Z");
const options = { autoCompact: false, autoCollect: false } as const;

const openMemory = async (): Promise<BlockStore> => new MemoryBlockStore();
const stores: Array<{ name: string; open: () => Promise<BlockStore> }> = [
  { name: "memory", open: openMemory },
  {
    name: "indexeddb",
    open: () =>
      IndexedDbBlockStore.open({ name: crypto.randomUUID(), indexedDB: new IDBFactory() }),
  },
  {
    name: "opfs",
    open: () => OpfsBlockStore.open({ name: crypto.randomUUID(), root: new MemoryOpfs().root }),
  },
];

async function writeRows(database: MinnowDatabase): Promise<void> {
  await database.createTable({
    name: "items",
    uniqueKey: "id",
    columns: [
      { name: "id", type: "number" },
      { name: "label", type: "string" },
    ],
  });
  await database.insertBatch(
    "items",
    Array.from({ length: 50 }, (_, id) => ({ id, label: `item-${String(id)}` })),
  );
}

function containerVersion(bytes: Uint8Array): number {
  return new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength).getUint32(8, true);
}

async function header(bytes: Uint8Array): Promise<SnapshotFrameStreamHeader> {
  const entries = decodeSnapshotFrameStream(
    (async function* (): AsyncGenerator<Uint8Array> {
      yield bytes;
    })(),
  );
  const first = await entries.next();
  await entries.return(undefined);
  if (first.done || first.value.type !== "header") throw new Error("No header");
  return first.value.header;
}

/** The same snapshot with its header re-encoded; frames and footer stay byte for byte. */
async function withHeader(
  bytes: Uint8Array,
  change: (header: SnapshotFrameStreamHeader) => SnapshotFrameStreamHeader,
): Promise<Uint8Array> {
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  const headerEnd = 20 + view.getUint32(12, true);
  const encoded = encodeSnapshotFrameStreamHeader(change(await header(bytes)));
  const result = new Uint8Array(encoded.byteLength + bytes.byteLength - headerEnd);
  result.set(encoded, 0);
  result.set(bytes.subarray(headerEnd), encoded.byteLength);
  return result;
}

async function exportWith(open: () => Promise<BlockStore>, forwards: boolean): Promise<Uint8Array> {
  const store = await open();
  const database = new MinnowDatabase(store, options);
  await writeRows(database);
  if (forwards) await store.addTable(KEY_FORWARDS);
  const bytes = await database.exportSnapshot();
  await database.close();
  store.close();
  return bytes;
}

async function restore(
  bytes: Uint8Array,
  open: () => Promise<BlockStore> = async () => new MemoryBlockStore(),
): Promise<{ store: BlockStore; db: MinnowDatabase }> {
  const store = await open();
  const db = new MinnowDatabase(store, options);
  await db.importSnapshot(bytes);
  return { store, db };
}

describe.each(stores)("snapshot format from the $name store", ({ open }) => {
  it("writes format 2 when the key-forwards table exists, and restores it", async () => {
    const bytes = await exportWith(open, true);
    expect(containerVersion(bytes)).toBe(2);
    expect((await header(bytes)).formatVersion).toBe(2);
    expect((await readSnapshotSummary(bytes)).formatVersion).toBe(2);

    // Into the same kind of store it came from.
    const { store, db } = await restore(bytes, open);
    expect(await store.getTable(KEY_FORWARDS.id)).toEqual(KEY_FORWARDS);
    expect((await db.query("SELECT COUNT(*) AS n FROM items")).rows).toEqual([{ n: 50 }]);
    // A restored database exports format 2 again.
    expect(containerVersion(await db.exportSnapshot())).toBe(2);
    await db.close();
    store.close();
  });

  it("keeps writing format 1 without it, which the released 0.14.1 build imports", async () => {
    const bytes = await exportWith(open, false);
    expect(containerVersion(bytes)).toBe(1);
    expect((await readSnapshotSummary(bytes)).formatVersion).toBe(1);
    expect((await releasedSummary(bytes)).formatVersion).toBe(1);
    const released = new ReleasedDatabase(new ReleasedMemoryStore(), options);
    await released.importSnapshot(bytes);
    expect((await released.query("SELECT COUNT(*) AS n FROM items")).rows).toEqual([{ n: 50 }]);
    await released.close();
  });

  it("is refused by the released 0.14.1 importer", async () => {
    const bytes = await exportWith(open, true);
    await expect(releasedSummary(bytes)).rejects.toThrow(/Unsupported snapshot format version/);
    const store = new ReleasedMemoryStore();
    const released = new ReleasedDatabase(store, options);
    await expect(released.importSnapshot(bytes)).rejects.toThrow(
      /Unsupported snapshot format version/,
    );
    expect(await store.listTables()).toEqual([]);
    await released.close();
  });
});

describe("snapshot format 2 refusals", () => {
  it("refuses a format-1 snapshot that holds a key-forwards table", async () => {
    const bytes = await exportWith(openMemory, true);
    const relabeled = await withHeader(bytes, (value) => ({ ...value, formatVersion: 1 }));
    expect(containerVersion(relabeled)).toBe(1);
    const store = new MemoryBlockStore();
    const database = new MinnowDatabase(store, options);
    await expect(database.importSnapshot(relabeled)).rejects.toThrow(
      /Snapshot format 1 cannot hold a key-forwards record/,
    );
    expect(await store.listTables()).toEqual([]);
    await database.close();
  });

  it("accepts a format-2 label on a snapshot without the table", async () => {
    const bytes = await exportWith(openMemory, false);
    const relabeled = await withHeader(bytes, (value) => ({ ...value, formatVersion: 2 }));
    const { db } = await restore(relabeled);
    expect((await db.query("SELECT COUNT(*) AS n FROM items")).rows).toEqual([{ n: 50 }]);
    await db.close();
  });

  it("refuses a container version that disagrees with its header, and unknown versions", async () => {
    const bytes = await exportWith(openMemory, true);
    for (const version of [1, 3, 0]) {
      const patched = bytes.slice();
      new DataView(patched.buffer).setUint32(8, version, true);
      await expect(readSnapshotSummary(patched), `container ${String(version)}`).rejects.toThrow(
        version === 1 ? /not canonically encoded/ : /Unsupported snapshot format version/,
      );
    }
    expect(() =>
      encodeSnapshotFrameStreamHeader({
        ...({} as SnapshotFrameStreamHeader),
        formatVersion: 3 as 2,
      }),
    ).toThrow(/Unsupported snapshot format version/);
  });
});

describe("empty keyed tables through an IndexedDB snapshot round trip", () => {
  // The key-forwards table is keyed and empties again once its forwards expire. An IndexedDB
  // import used to record an empty table's key membership as a base with no parts, which the
  // next export refused as corruption.
  it("imports, re-exports, and still enforces the key of an empty keyed table", async () => {
    const indexedDB = new IDBFactory();
    const source = await IndexedDbBlockStore.open({ name: crypto.randomUUID(), indexedDB });
    const database = new MinnowDatabase(source, options);
    await writeRows(database);
    await database.execute("CREATE TABLE empty_keyed (k TEXT PRIMARY KEY, v TEXT)");
    await source.addTable(KEY_FORWARDS);
    const bytes = await database.exportSnapshot();
    await database.close();
    source.close();

    const store = await IndexedDbBlockStore.open({ name: crypto.randomUUID(), indexedDB });
    const restored = new MinnowDatabase(store, options);
    await restored.importSnapshot(bytes);
    expect(containerVersion(await restored.exportSnapshot())).toBe(2);
    expect((await store.checkIntegrity({ mode: "full" })).issues).toEqual([]);
    await restored.execute("INSERT INTO empty_keyed VALUES ('a', 'first')");
    await expect(
      restored.execute("INSERT INTO empty_keyed VALUES ('a', 'again')"),
    ).rejects.toThrow();
    expect((await restored.query("SELECT k, v FROM empty_keyed")).rows).toEqual([
      { k: "a", v: "first" },
    ]);
    await restored.close();
    store.close();
  });
});
