/**
 * Databases the last released writer, 0.16.0, left — IndexedDB schema 7, OPFS layout 12. A
 * packed segment (one stored block holding a small commit's columns, named by `packedBlockId`)
 * is a record an older reader would refuse or count wrongly, and a staging call may now carry
 * 1,024 blocks, so IndexedDB schema 8 and OPFS layout 13 bar older readers; the upgrade moves
 * only the version marker. These tests take the real 0.16.0 writer's databases through the
 * ordinary open API: the history reads the same, new small commits pack, the packed and the
 * released segments fold together, the database reopens, and the released reader is refused
 * afterwards without a change. They also cut the IndexedDB upgrade mid-way and retry, race
 * current and released openers, and refuse damaged packed blocks as corruption. The OPFS
 * marker's publication steps are cut in marker-upgrade.test.ts, over the frozen layout-12
 * fixture.
 */
import { IDBFactory } from "fake-indexeddb";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MinnowDatabase as ReleasedDatabase } from "@minnowdb/core-layout12";
import { IndexedDbBlockStore as ReleasedIndexedDbStore } from "@minnowdb/core-layout12/storage/indexeddb";
import { OpfsBlockStore as ReleasedOpfsStore } from "@minnowdb/core-layout12/storage/opfs";
import {
  crc32,
  inspectPackedBlock,
  PACKED_BLOCK_ENTRY_LENGTH,
  PACKED_BLOCK_HEADER_LENGTH,
} from "../block-format/index.js";
import {
  IndexedDbBlockStore,
  OpfsBlockStore,
  type BlockStore,
  type SegmentRecord,
} from "../storage/index.js";
import { readRawValue } from "../storage/indexeddb-audit-helpers.js";
import { MemoryOpfs } from "../testing/opfs-shim.js";
import { MinnowDatabase } from "./database.js";
import { packedSegmentTestHooks } from "./packed-members.js";
import { allSegmentRecords, heavyTestTimeout } from "./storage-test-helpers.js";

vi.setConfig({ testTimeout: heavyTestTimeout(120_000) });

afterEach(() => {
  packedSegmentTestHooks.missIsFatal = false;
});

/** What this build writes, and what 0.16.0 wrote. */
const CURRENT_SCHEMA = 8;
const RELEASED_SCHEMA = 7;
const CURRENT_LAYOUT = 13;
const RELEASED_LAYOUT = 12;
const NAME = "packed-segment-upgrade";
const OPTIONS = { autoCompact: false, autoCollect: false } as const;
const WIDTH = 8;
const ALL = "SELECT * FROM items ORDER BY id";

type Item = Record<string, number | string | null>;

/** The released writer, as these tests drive it. */
interface ReleasedWriter {
  createTable(definition: unknown): Promise<unknown>;
  insertBatch(table: string, rows: Item[]): Promise<unknown>;
  execute(sql: string, params?: Array<number | string>): Promise<unknown>;
  query(sql: string): Promise<{ rows: unknown[] }>;
  close(): Promise<void>;
}

function item(id: number, version = 0): Item {
  const row: Item = { id };
  for (let column = 0; column < WIDTH; column += 1) {
    row[`c${String(column)}`] =
      (id + column) % 5 === 0
        ? null
        : column % 2 === 0
          ? id * 10 + column + version
          : `v${String(id + version)}`;
  }
  return row;
}

/** A wide keyed table: one bulk insert, then the single-row commits 0.16.0 wrote unpacked. */
async function writeReleasedHistory(writer: ReleasedWriter): Promise<unknown[]> {
  await writer.createTable({
    name: "items",
    uniqueKey: "id",
    columns: [
      { name: "id", type: "number" },
      ...Array.from({ length: WIDTH }, (_, column) => ({
        name: `c${String(column)}`,
        type: column % 2 === 0 ? "number" : "string",
        nullable: true,
      })),
    ],
  });
  await writer.insertBatch(
    "items",
    Array.from({ length: 400 }, (_, id) => item(id)),
  );
  for (let id = 400; id < 406; id += 1) await writer.insertBatch("items", [item(id)]);
  await writer.execute("UPDATE items SET c1 = 'released' WHERE id = 7");
  await writer.execute("DELETE FROM items WHERE id = 401");
  return (await writer.query(ALL)).rows;
}

/** Packed segments the store holds, collected or not. */
async function packedSegments(store: BlockStore): Promise<SegmentRecord[]> {
  return (await allSegmentRecords(store)).filter(
    ({ packedBlockId }) => packedBlockId !== undefined,
  );
}

/** Packed segments a read of the table sees now. */
async function visiblePacked(database: MinnowDatabase): Promise<number> {
  let count = 0;
  let cursor: Parameters<MinnowDatabase["listVisibleSegmentPage"]>[1] = {};
  for (;;) {
    const page = await database.listVisibleSegmentPage("items", cursor);
    for (const record of page.records) if (record.packedBlockId !== undefined) count += 1;
    if (page.nextCursor === null) return count;
    cursor = { cursor: page.nextCursor };
  }
}

/**
 * The current build's writes on top of the upgraded history: single-row commits that pack, read
 * back before and after a fold that takes them together with the released writer's segments.
 */
async function writeCurrent(
  database: MinnowDatabase,
  store: BlockStore,
  rows: unknown[],
): Promise<unknown[]> {
  packedSegmentTestHooks.missIsFatal = true;
  const before = (await packedSegments(store)).length;
  const visibleBefore = await visiblePacked(database);
  for (let id = 900; id < 904; id += 1) await database.insertBatch("items", [item(id)] as never);
  await database.execute("UPDATE items SET c1 = 'current', c2 = 7 WHERE id = 3");
  await database.execute("UPDATE items SET c3 = 'again' WHERE id = 402");
  // Four inserts and two updates, each one packed block.
  expect((await packedSegments(store)).length).toBe(before + 6);
  expect(await visiblePacked(database)).toBe(visibleBefore + 6);
  const next = (rows as Item[])
    .map((row) =>
      row.id === 3
        ? { ...row, c1: "current", c2: 7 }
        : row.id === 402
          ? { ...row, c3: "again" }
          : row,
    )
    .concat(Array.from({ length: 4 }, (_, index) => item(900 + index)))
    .sort((left, right) => Number(left.id) - Number(right.id));
  expect((await database.query(ALL, { memoize: false })).rows).toEqual(next);
  expect(
    (await database.query("SELECT c1 FROM items WHERE id = 3", { memoize: false })).rows,
  ).toEqual([{ c1: "current" }]);
  const folded = await database.compactTable("items");
  expect(folded.compacted).toBe(true);
  expect(await visiblePacked(database)).toBe(0);
  expect((await database.query(ALL, { memoize: false })).rows).toEqual(next);
  return next;
}

function nativeVersion(indexedDB: IDBFactory, name: string): Promise<number> {
  return new Promise((resolve, reject) => {
    const request = indexedDB.open(name);
    request.onsuccess = () => {
      const version = request.result.version;
      request.result.close();
      resolve(version);
    };
    request.onerror = () => reject(request.error ?? new Error("native open failed"));
  });
}

async function releasedIndexedDb(indexedDB: IDBFactory, name: string): Promise<unknown[]> {
  const store = await ReleasedIndexedDbStore.open({ name, indexedDB });
  const writer = new ReleasedDatabase(store, OPTIONS) as unknown as ReleasedWriter;
  const rows = await writeReleasedHistory(writer);
  await writer.close();
  store.close();
  expect(await nativeVersion(indexedDB, name)).toBe(RELEASED_SCHEMA);
  return rows;
}

async function rawPut(
  indexedDB: IDBFactory,
  name: string,
  writes: ReadonlyArray<{ store: string; key: IDBValidKey; value: unknown }>,
): Promise<void> {
  const database = await new Promise<IDBDatabase>((resolve, reject) => {
    const request = indexedDB.open(name);
    request.onsuccess = () => resolve(request.result);
    request.onerror = () => reject(request.error ?? new Error("raw open failed"));
  });
  try {
    await new Promise<void>((resolve, reject) => {
      const transaction = database.transaction(
        [...new Set(writes.map(({ store }) => store))],
        "readwrite",
      );
      for (const write of writes) transaction.objectStore(write.store).put(write.value, write.key);
      transaction.oncomplete = () => resolve();
      transaction.onabort = () => reject(transaction.error ?? new Error("raw write aborted"));
    });
  } finally {
    database.close();
  }
}

/** Replaces a stored block's bytes and the checksum IndexedDB keeps beside them. */
async function rewriteBlock(
  indexedDB: IDBFactory,
  name: string,
  id: string,
  bytes: Uint8Array,
): Promise<void> {
  await rawPut(indexedDB, name, [
    { store: "blocks", key: id, value: bytes },
    {
      store: "catalog",
      key: `block-metadata/${id}`,
      value: { byteLength: bytes.byteLength, checksum: crc32(bytes) },
    },
  ]);
}

describe("IndexedDB: the 0.16.0 writer's database after the schema 8 upgrade", () => {
  it("reads the history, packs new commits, folds them, reopens, and refuses the released reader", async () => {
    const indexedDB = new IDBFactory();
    const name = crypto.randomUUID();
    const rows = await releasedIndexedDb(indexedDB, name);

    let store = await IndexedDbBlockStore.open({ name, indexedDB });
    expect(await nativeVersion(indexedDB, name)).toBe(CURRENT_SCHEMA);
    expect(await packedSegments(store)).toEqual([]);
    let database = new MinnowDatabase(store, OPTIONS);
    expect((await database.query(ALL, { memoize: false })).rows).toEqual(rows);
    const latest = await writeCurrent(database, store, rows);
    expect((await store.checkIntegrity({ mode: "full" })).issues).toEqual([]);
    // One more packed commit after the fold, so the reopened database holds one.
    await database.insertBatch("items", [item(950)] as never);
    latest.push(item(950));
    await database.close();
    store.close();

    store = await IndexedDbBlockStore.open({ name, indexedDB });
    database = new MinnowDatabase(store, OPTIONS);
    expect(await visiblePacked(database)).toBe(1);
    expect((await database.query(ALL, { memoize: false })).rows).toEqual(latest);
    expect((await store.checkIntegrity({ mode: "full" })).issues).toEqual([]);
    await database.close();
    store.close();

    await expect(ReleasedIndexedDbStore.open({ name, indexedDB })).rejects.toMatchObject({
      name: "StorageFormatVersionError",
      actualVersion: CURRENT_SCHEMA,
      supportedVersion: RELEASED_SCHEMA,
      relation: "newer",
    });
    expect(await nativeVersion(indexedDB, name)).toBe(CURRENT_SCHEMA);
    store = await IndexedDbBlockStore.open({ name, indexedDB });
    database = new MinnowDatabase(store, OPTIONS);
    expect((await database.query(ALL, { memoize: false })).rows).toEqual(latest);
    await database.close();
    store.close();
  });

  it("leaves schema 7 for the released writer when the upgrade is interrupted, then retries", async () => {
    const indexedDB = new IDBFactory();
    const name = crypto.randomUUID();
    const rows = (await releasedIndexedDb(indexedDB, name)) as Item[];
    // The tab dies inside the version-change transaction: IndexedDB rolls all of it back.
    let aborted = false;
    const open = indexedDB.open.bind(indexedDB);
    Object.defineProperty(indexedDB, "open", {
      configurable: true,
      value: (databaseName: string, version?: number) => {
        const request = version === undefined ? open(databaseName) : open(databaseName, version);
        queueMicrotask(() =>
          request.addEventListener("upgradeneeded", () => {
            if (aborted) return;
            aborted = true;
            request.transaction?.abort();
          }),
        );
        return request;
      },
    });
    await expect(IndexedDbBlockStore.open({ name, indexedDB })).rejects.toThrow();
    expect(aborted).toBe(true);
    expect(await nativeVersion(indexedDB, name)).toBe(RELEASED_SCHEMA);
    // Still the released writer's database, readable and writable by it.
    const releasedStore = await ReleasedIndexedDbStore.open({ name, indexedDB });
    const released = new ReleasedDatabase(releasedStore, OPTIONS) as unknown as ReleasedWriter;
    await released.execute("UPDATE items SET c0 = -1 WHERE id = 9");
    await released.close();
    releasedStore.close();
    const expected = rows.map((row) => (row.id === 9 ? { ...row, c0: -1 } : row));

    const store = await IndexedDbBlockStore.open({ name, indexedDB });
    expect(await nativeVersion(indexedDB, name)).toBe(CURRENT_SCHEMA);
    const database = new MinnowDatabase(store, OPTIONS);
    expect((await database.query(ALL, { memoize: false })).rows).toEqual(expected);
    await writeCurrent(database, store, expected);
    expect((await store.checkIntegrity({ mode: "full" })).issues).toEqual([]);
    await database.close();
    store.close();
  });

  it("lets concurrent current openers share one upgrade and closes or refuses released ones", async () => {
    const indexedDB = new IDBFactory();
    const name = crypto.randomUUID();
    const rows = await releasedIndexedDb(indexedDB, name);
    const stale = await ReleasedIndexedDbStore.open({ name, indexedDB });
    const [first, second, late] = await Promise.allSettled([
      IndexedDbBlockStore.open({ name, indexedDB }),
      IndexedDbBlockStore.open({ name, indexedDB }),
      ReleasedIndexedDbStore.open({ name, indexedDB }),
    ]);
    expect(first.status).toBe("fulfilled");
    expect(second.status).toBe("fulfilled");
    // A released opener that arrives behind the upgrade is refused, never served schema 8.
    expect(late).toMatchObject({
      status: "rejected",
      reason: { name: "StorageFormatVersionError", actualVersion: CURRENT_SCHEMA },
    });
    // The released connection that was open when the upgrade arrived closed itself.
    await expect(stale.getCurrentManifestVersion()).rejects.toThrow(/connection is closed/);
    const stores = [first, second].map((result) =>
      result.status === "fulfilled" ? result.value : undefined,
    );
    const [one, two] = stores;
    if (one === undefined || two === undefined) throw new Error("Both openers must succeed");
    const writer = new MinnowDatabase(one, OPTIONS);
    const reader = new MinnowDatabase(two, OPTIONS);
    // One connection writes packed commits; the other reads them without having written them.
    for (let id = 900; id < 903; id += 1) await writer.insertBatch("items", [item(id)] as never);
    expect((await packedSegments(one)).length).toBe(3);
    expect((await reader.query(ALL, { memoize: false })).rows).toEqual(
      (rows as Item[]).concat([item(900), item(901), item(902)]),
    );
    await writer.close();
    await reader.close();
    for (const store of stores) store?.close();
    expect(await nativeVersion(indexedDB, name)).toBe(CURRENT_SCHEMA);
  });

  it("refuses damaged packed blocks and a record naming a member its block lacks", async () => {
    const indexedDB = new IDBFactory();
    const name = crypto.randomUUID();
    await releasedIndexedDb(indexedDB, name);
    const store = await IndexedDbBlockStore.open({ name, indexedDB });
    const database = new MinnowDatabase(store, OPTIONS);
    await database.insertBatch("items", [item(900)] as never);
    await database.execute("UPDATE items SET c1 = 'patched' WHERE id = 5");
    const [insert, update] = (await packedSegments(store)).sort(
      (left, right) => left.logicalOrder - right.logicalOrder,
    );
    if (insert?.packedBlockId === undefined || update?.packedBlockId === undefined) {
      throw new Error("Expected two packed segments");
    }
    const columnId = new Map(
      (await store.getTableByName("items"))?.columns.map((column) => [column.name, column.id]),
    );
    const [idColumn, c1Column, c3Column] = ["id", "c1", "c3"].map(
      (column) => columnId.get(column) ?? "",
    );
    // The update names the key and the one column it set.
    expect(Object.keys(update.columnBlockIds).sort()).toEqual([idColumn, c1Column].sort());
    const original = await store.getBlock(insert.packedBlockId);
    if (original === undefined) throw new Error("The packed block is missing");
    await database.close();
    store.close();

    const reopen = async () => {
      const reopened = await IndexedDbBlockStore.open({ name, indexedDB });
      return { reopened, reader: new MinnowDatabase(reopened, OPTIONS) };
    };

    // A byte of the directory flipped, under a storage checksum that still matches: the
    // container's own envelope checksum refuses it, on read and in a full integrity check.
    const directory = original.slice();
    directory[26] = (directory[26] ?? 0) ^ 0xff;
    await rewriteBlock(indexedDB, name, insert.packedBlockId, directory);
    let { reopened, reader } = await reopen();
    await expect(
      reader.query("SELECT * FROM items WHERE id = 900", { memoize: false }),
    ).rejects.toThrow(/packed block envelope checksum/i);
    expect((await reopened.checkIntegrity({ mode: "full" })).ok).toBe(false);
    await reader.close();
    reopened.close();

    // A byte of a member flipped: the member's own v2 checksum refuses it, and the container's
    // members checksum fails the full check.
    const member = original.slice();
    member[member.byteLength - 1] = (member[member.byteLength - 1] ?? 0) ^ 0xff;
    await rewriteBlock(indexedDB, name, insert.packedBlockId, member);
    ({ reopened, reader } = await reopen());
    await expect(
      reader.query("SELECT * FROM items WHERE id = 900", { memoize: false }),
    ).rejects.toThrow(/checksum/i);
    expect((await reopened.checkIntegrity({ mode: "full" })).ok).toBe(false);
    await reader.close();
    reopened.close();
    await rewriteBlock(indexedDB, name, insert.packedBlockId, original);

    // The update's record rewritten to claim its member serves another column. The member id
    // is still canonical, but the directory holds no member for that (id, column) pair.
    const raw = (await readRawValue(indexedDB, name, "segments", update.id)) as SegmentRecord;
    const forged: SegmentRecord = {
      ...raw,
      columnBlockIds: {
        [idColumn ?? ""]: raw.columnBlockIds[idColumn ?? ""] ?? [],
        [c3Column ?? ""]: [`${update.id}\u0000${c3Column ?? ""}`],
      },
    };
    await rawPut(indexedDB, name, [{ store: "segments", key: update.id, value: forged }]);
    ({ reopened, reader } = await reopen());
    await expect(
      reader.query("SELECT c3 FROM items WHERE id = 5", { memoize: false }),
    ).rejects.toThrow(/has no member/);
    // The block and its storage checksum are intact, so only the full check's member
    // cross-check can name the problem; the metadata check reads no block and passes.
    expect((await reopened.checkIntegrity()).ok).toBe(true);
    const full = await reopened.checkIntegrity({ mode: "full" });
    expect(full.ok).toBe(false);
    expect(full.issues).toEqual([
      expect.objectContaining({
        code: "invalid-segment",
        location: `segments/${update.id}`,
        message: expect.stringMatching(/packed block lacks member/) as string,
      }),
    ]);
    await reader.close();
    reopened.close();
  });
});

describe("OPFS: the 0.16.0 writer's database after the layout 13 upgrade", () => {
  const marker = (shim: MemoryOpfs) =>
    new TextDecoder().decode(shim.readFileBytes(`minnowdb/${NAME}/format.json`));

  async function releasedOpfs(shim: MemoryOpfs): Promise<unknown[]> {
    const store = await ReleasedOpfsStore.open({ name: NAME, root: shim.root });
    const writer = new ReleasedDatabase(store, OPTIONS) as unknown as ReleasedWriter;
    const rows = await writeReleasedHistory(writer);
    await writer.close();
    store._crashForTests();
    expect(marker(shim)).toBe(`{"formatVersion":${String(RELEASED_LAYOUT)}}`);
    return rows;
  }

  it("reads the history, packs new commits, folds them, reopens, and refuses the released reader unchanged", async () => {
    const shim = new MemoryOpfs();
    const rows = await releasedOpfs(shim);

    let store = await OpfsBlockStore.open({ name: NAME, root: shim.root });
    expect(marker(shim)).toBe(`{"formatVersion":${String(CURRENT_LAYOUT)}}`);
    let database = new MinnowDatabase(store, OPTIONS);
    expect((await database.query(ALL, { memoize: false })).rows).toEqual(rows);
    const latest = await writeCurrent(database, store, rows);
    await database.insertBatch("items", [item(950)] as never);
    latest.push(item(950));
    await database.close();
    store._crashForTests();

    // Replayed from the log, then from a checkpoint after a graceful close.
    for (const step of ["replay", "checkpoint"] as const) {
      store = await OpfsBlockStore.open({ name: NAME, root: shim.root });
      database = new MinnowDatabase(store, OPTIONS);
      expect(await visiblePacked(database)).toBe(1);
      expect((await database.query(ALL, { memoize: false })).rows).toEqual(latest);
      expect(await store.checkIntegrity({ mode: "full" })).toMatchObject({ ok: true });
      await database.close();
      if (step === "replay") store.close();
      else store._crashForTests();
    }

    const writes: string[] = [];
    shim.setWriteFault((path, phase) => {
      if (phase !== "flush") writes.push(`${phase}: ${path}`);
    });
    shim.setDeleteFault((path) => writes.push(`delete: ${path}`));
    await expect(ReleasedOpfsStore.open({ name: NAME, root: shim.root })).rejects.toMatchObject({
      name: "StorageFormatVersionError",
      actualVersion: CURRENT_LAYOUT,
      supportedVersion: RELEASED_LAYOUT,
      relation: "newer",
    });
    shim.setWriteFault(null);
    shim.setDeleteFault(null);
    expect(writes).toEqual([]);
    expect(marker(shim)).toBe(`{"formatVersion":${String(CURRENT_LAYOUT)}}`);
    store = await OpfsBlockStore.open({ name: NAME, root: shim.root });
    database = new MinnowDatabase(store, OPTIONS);
    expect((await database.query(ALL, { memoize: false })).rows).toEqual(latest);
    await database.close();
    store._crashForTests();
  });

  it("serializes concurrent current openers through the upgrade", async () => {
    const shim = new MemoryOpfs();
    const rows = await releasedOpfs(shim);
    const stores = await Promise.all(
      Array.from({ length: 3 }, () => OpfsBlockStore.open({ name: NAME, root: shim.root })),
    );
    try {
      expect(marker(shim)).toBe(`{"formatVersion":${String(CURRENT_LAYOUT)}}`);
      const [first, second] = stores.map((store) => new MinnowDatabase(store, OPTIONS));
      if (first === undefined || second === undefined) throw new Error("Openers are missing");
      await first.insertBatch("items", [item(990)] as never);
      expect((await second.query(ALL, { memoize: false })).rows).toEqual(
        (rows as Item[]).concat(item(990)),
      );
      await first.close();
      await second.close();
    } finally {
      for (const store of stores) store._crashForTests();
    }
    const reopened = await OpfsBlockStore.open({ name: NAME, root: shim.root });
    try {
      const database = new MinnowDatabase(reopened, OPTIONS);
      await writeCurrent(database, reopened, (rows as Item[]).concat(item(990)));
      await database.close();
    } finally {
      reopened._crashForTests();
    }
  });

  it("reports a stored packed block that lost a member its segment names, under an intact storage checksum", async () => {
    const shim = new MemoryOpfs();
    await releasedOpfs(shim);
    let store = await OpfsBlockStore.open({ name: NAME, root: shim.root });
    let database = new MinnowDatabase(store, OPTIONS);
    await database.insertBatch("items", [item(900)] as never);
    const [insert] = await packedSegments(store);
    if (insert?.packedBlockId === undefined) throw new Error("Expected a packed insert");
    const original = await store.getBlock(insert.packedBlockId);
    if (original === undefined) throw new Error("The packed block is missing");
    await database.close();
    store._crashForTests();

    // The fault a bit flip in the first directory entry's tag would leave if the extent's
    // whole-payload checksum happened to survive it: the container is still valid (its envelope
    // checksum rewritten, its members untouched), so only the member cross-check sees that the
    // segment's first column now names a member the directory does not hold.
    const forged = retagFirstMemberKeepingChecksum(original);
    expect(crc32(forged)).toBe(crc32(original));
    expect(inspectPackedBlock(forged).memberCount).toBe(inspectPackedBlock(original).memberCount);
    // Blocks live in extent files under the database directory; patch the one holding it.
    const database_ = await (
      await shim.root.getDirectoryHandle("minnowdb")
    ).getDirectoryHandle(NAME);
    let patched = 0;
    const walk = async (directory: FileSystemDirectoryHandle, prefix: string): Promise<void> => {
      for await (const [entryName, handle] of directory.entries()) {
        if (handle.kind === "directory") {
          await walk(handle as FileSystemDirectoryHandle, `${prefix}${entryName}/`);
          continue;
        }
        const path = `${prefix}${entryName}`;
        const bytes = shim.readFileBytes(path);
        if (bytes === undefined) continue;
        const at = indexOfBytes(bytes, original);
        if (at < 0) continue;
        bytes.set(forged, at);
        shim.writeFileBytes(path, bytes);
        patched += 1;
      }
    };
    await walk(database_, `minnowdb/${NAME}/`);
    expect(patched).toBe(1);

    store = await OpfsBlockStore.open({ name: NAME, root: shim.root });
    database = new MinnowDatabase(store, OPTIONS);
    try {
      expect((await store.checkIntegrity()).ok).toBe(true);
      const full = await store.checkIntegrity({ mode: "full" });
      expect(full.ok).toBe(false);
      expect(full.issues).toEqual([
        expect.objectContaining({
          code: "invalid-segment",
          location: `segments/${insert.id}`,
          message: expect.stringMatching(/packed block lacks member/) as string,
        }),
      ]);
      await expect(database.query(ALL, { memoize: false })).rejects.toThrow(/has no member/);
    } finally {
      await database.close();
      store._crashForTests();
    }
  });
});

function indexOfBytes(haystack: Uint8Array, needle: Uint8Array): number {
  const first = needle[0];
  for (let at = 0; at + needle.byteLength <= haystack.byteLength; at += 1) {
    if (haystack[at] !== first) continue;
    let matched = true;
    for (let offset = 1; offset < needle.byteLength; offset += 1) {
      if (haystack[at + offset] !== needle[offset]) {
        matched = false;
        break;
      }
    }
    if (matched) return at;
  }
  return -1;
}

/**
 * The packed block with its first directory entry's tag changed and its envelope checksum
 * rewritten, chosen so that the whole payload's CRC-32 is unchanged: the entry's second tag
 * word is solved for over GF(2), since CRC-32 is affine in the bytes it covers.
 */
function retagFirstMemberKeepingChecksum(original: Uint8Array): Uint8Array {
  const target = crc32(original);
  const directoryEnd =
    PACKED_BLOCK_HEADER_LENGTH +
    PACKED_BLOCK_ENTRY_LENGTH * inspectPackedBlock(original).memberCount;
  const entry = PACKED_BLOCK_HEADER_LENGTH;
  const candidate = (fnv: number): Uint8Array => {
    const bytes = original.slice();
    const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
    view.setUint32(entry + 4, view.getUint32(entry + 4, true) ^ 1, true);
    view.setUint32(entry + 8, fnv >>> 0, true);
    view.setUint32(4, crc32(bytes.subarray(8, directoryEnd)), true);
    return bytes;
  };
  const base = crc32(candidate(0));
  const columns = Array.from({ length: 32 }, (_, bit) => crc32(candidate(1 << bit)) ^ base);
  // Solve sum(columns[bit] for set bits) = target ^ base by Gaussian elimination.
  let wanted = (target ^ base) >>> 0;
  const rows = columns.map((column, bit) => ({ column: column >>> 0, bits: 1 << bit }));
  let solution = 0;
  for (let pivot = 31; pivot >= 0; pivot -= 1) {
    const mask = 1 << pivot;
    const index = rows.findIndex((row) => (row.column & mask) !== 0);
    if (index < 0) continue;
    const [row] = rows.splice(index, 1);
    if (row === undefined) continue;
    for (const other of rows) {
      if ((other.column & mask) !== 0) {
        other.column = (other.column ^ row.column) >>> 0;
        other.bits ^= row.bits;
      }
    }
    if ((wanted & mask) !== 0) {
      wanted = (wanted ^ row.column) >>> 0;
      solution ^= row.bits;
    }
  }
  if (wanted !== 0) throw new Error("No tag keeps the storage checksum");
  const forged = candidate(solution);
  if (crc32(forged) !== target) throw new Error("The forged tag changed the storage checksum");
  return forged;
}
