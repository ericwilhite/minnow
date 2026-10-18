/**
 * Snapshot format 3: format 1's container and frames, written only when a table has a column
 * declared TIMESTAMP without time zone (`withoutTimeZone: true`). The released 0.16.0 build
 * imports formats 1 and 2; given that column it would refuse the record as corruption (into
 * IndexedDB) or restore it as `timestamp with time zone` (into memory or OPFS), so it must
 * refuse the snapshot, and the version is how it knows. A snapshot without such a column is
 * still format 1 or 2, so older builds keep importing it, and a format-1 or format-2 snapshot
 * the released writer exported imports here with its columns as the instants they were.
 */
import { IDBFactory } from "fake-indexeddb";
import { describe, expect, it } from "vitest";
import { MinnowDatabase as ReleasedDatabase } from "@minnowdb/core-layout12";
import { IndexedDbBlockStore as ReleasedIndexedDbStore } from "@minnowdb/core-layout12/storage/indexeddb";
import { MemoryBlockStore as ReleasedMemoryStore } from "@minnowdb/core-layout12/storage/memory";
import { OpfsBlockStore as ReleasedOpfsStore } from "@minnowdb/core-layout12/storage/opfs";
import { readSnapshotSummary as releasedSummary } from "@minnowdb/core-layout12/storage/snapshots";
import { MinnowDatabase } from "../engine/database.js";
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

const options = { autoCompact: false, autoCollect: false } as const;

const stores: Array<{ name: string; open: () => Promise<BlockStore> }> = [
  { name: "memory", open: async () => new MemoryBlockStore() },
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

/** Released importers, one per store kind: each must refuse format 3 and keep nothing. */
const releasedStores: Array<{ name: string; open: () => Promise<BlockStore> }> = [
  { name: "memory", open: async () => new ReleasedMemoryStore() },
  {
    name: "indexeddb",
    open: () =>
      ReleasedIndexedDbStore.open({
        name: crypto.randomUUID(),
        indexedDB: new IDBFactory(),
      }),
  },
  {
    name: "opfs",
    open: () =>
      ReleasedOpfsStore.open({
        name: crypto.randomUUID(),
        root: new MemoryOpfs().root,
      }),
  },
];

const TEXT = "SELECT id, local_at::text AS l, zoned_at::text AS z FROM stamped ORDER BY id";
const EXPECTED = [
  { id: 1, l: "2026-01-02 03:04:05.5", z: "2026-01-02 03:04:05.5+00" },
  { id: 2, l: null, z: "2025-06-01 00:00:00+00" },
];

async function writeRows(database: MinnowDatabase, timestamp: boolean): Promise<void> {
  await database.execute(
    `CREATE TABLE stamped (id INTEGER PRIMARY KEY, local_at ${timestamp ? "TIMESTAMP" : "TIMESTAMPTZ"}, zoned_at TIMESTAMPTZ)`,
  );
  await database.execute(
    "INSERT INTO stamped VALUES (1, '2026-01-02 03:04:05.5', '2026-01-02 03:04:05.5'), (2, NULL, '2025-06-01 00:00:00')",
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

async function exportWith(
  open: () => Promise<BlockStore>,
  timestamp: boolean,
  rekey = false,
): Promise<Uint8Array> {
  const store = await open();
  const database = new MinnowDatabase(store, options);
  await writeRows(database, timestamp);
  // A rekey writes the internal key-forwards table, format 2's record.
  if (rekey) await database.execute("UPDATE stamped SET id = 20 WHERE id = 2");
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

describe.each(stores)("snapshot format 3 from the $name store", ({ open }) => {
  it("writes format 3 when a column is declared TIMESTAMP, and restores its kind", async () => {
    const bytes = await exportWith(open, true);
    expect(containerVersion(bytes)).toBe(3);
    expect((await header(bytes)).formatVersion).toBe(3);
    expect((await readSnapshotSummary(bytes)).formatVersion).toBe(3);

    // Into every kind of store.
    for (const target of stores) {
      const { store, db } = await restore(bytes, target.open);
      expect((await db.query(TEXT)).rows, target.name).toEqual(EXPECTED);
      const columns = (await store.getTableByName("stamped"))?.columns ?? [];
      expect(
        columns.map(({ name, withoutTimeZone }) => [name, withoutTimeZone]),
        target.name,
      ).toEqual([
        ["id", undefined],
        ["local_at", true],
        ["zoned_at", undefined],
      ]);
      // A restored database exports format 3 again.
      expect(containerVersion(await db.exportSnapshot()), target.name).toBe(3);
      await db.close();
      store.close();
    }
  });

  it("holds the key-forwards table in format 3 too", async () => {
    const bytes = await exportWith(open, true, true);
    expect(containerVersion(bytes)).toBe(3);
    const { store, db } = await restore(bytes);
    expect((await db.query(TEXT)).rows).toEqual([EXPECTED[0], { ...EXPECTED[1], id: 20 }]);
    // The forward the rekey recorded resolves the retired key.
    expect(await db.resolveKey("stamped", 2)).toBe(20);
    await db.close();
    store.close();
  });

  it("keeps writing format 1 without the column, which the released 0.16.0 build imports", async () => {
    const bytes = await exportWith(open, false);
    expect(containerVersion(bytes)).toBe(1);
    expect((await releasedSummary(bytes)).formatVersion).toBe(1);
    const released = new ReleasedDatabase(new ReleasedMemoryStore(), options);
    await released.importSnapshot(bytes);
    expect((await released.query("SELECT COUNT(*) AS n FROM stamped")).rows).toEqual([{ n: 2 }]);
    await released.close();
  });

  it("is refused by the released 0.16.0 importer, which keeps nothing", async () => {
    const bytes = await exportWith(open, true);
    await expect(releasedSummary(bytes)).rejects.toThrow(/Unsupported snapshot format version/);
    for (const target of releasedStores) {
      const store = await target.open();
      const released = new ReleasedDatabase(store as never, options);
      await expect(released.importSnapshot(bytes), target.name).rejects.toThrow(
        /Unsupported snapshot format version/,
      );
      expect(await store.listTables(), target.name).toEqual([]);
      await released.close();
      store.close();
    }
  });
});

describe("snapshots the released 0.16.0 writer exported", () => {
  it("import with every datetime an instant, as that writer stored them", async () => {
    const released = new ReleasedDatabase(new ReleasedMemoryStore(), options);
    // 0.16.0 stored a column declared TIMESTAMP as an instant, and exported format 1.
    await writeRows(released as unknown as MinnowDatabase, true);
    const bytes = await released.exportSnapshot();
    await released.close();
    expect(containerVersion(bytes)).toBe(1);
    for (const target of stores) {
      const { store, db } = await restore(bytes, target.open);
      expect((await db.query(TEXT)).rows, target.name).toEqual([
        { ...EXPECTED[0], l: "2026-01-02 03:04:05.5+00" },
        EXPECTED[1],
      ]);
      expect(
        (await store.getTableByName("stamped"))?.columns.some(
          ({ withoutTimeZone }) => withoutTimeZone !== undefined,
        ),
        target.name,
      ).toBe(false);
      // Exported again, it is still format 1, which the released build imports.
      expect(containerVersion(await db.exportSnapshot()), target.name).toBe(1);
      await db.close();
      store.close();
    }
  });
});

describe("snapshot format 3 refusals", () => {
  it("refuses a format-1 or format-2 snapshot that holds a TIMESTAMP column", async () => {
    const bytes = await exportWith(async () => new MemoryBlockStore(), true);
    for (const formatVersion of [1, 2] as const) {
      const relabeled = await withHeader(bytes, (value) => ({ ...value, formatVersion }));
      expect(containerVersion(relabeled)).toBe(formatVersion);
      const store = new MemoryBlockStore();
      const database = new MinnowDatabase(store, options);
      await expect(database.importSnapshot(relabeled)).rejects.toThrow(
        `Snapshot format ${String(formatVersion)} cannot hold a TIMESTAMP without time zone column`,
      );
      expect(await store.listTables()).toEqual([]);
      await database.close();
    }
  });

  it("accepts a format-3 label on a snapshot without one", async () => {
    const bytes = await exportWith(async () => new MemoryBlockStore(), false);
    const relabeled = await withHeader(bytes, (value) => ({ ...value, formatVersion: 3 }));
    const { db } = await restore(relabeled);
    expect((await db.query("SELECT COUNT(*) AS n FROM stamped")).rows).toEqual([{ n: 2 }]);
    await db.close();
  });
});
