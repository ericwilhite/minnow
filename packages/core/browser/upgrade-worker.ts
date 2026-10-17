import { MinnowDatabase } from "@minnowdb/core";
import { decodeBlock } from "@minnowdb/core/block-format";
import { IndexedDbBlockStore } from "@minnowdb/core/storage/indexeddb";
import { OpfsBlockStore } from "@minnowdb/core/storage/opfs";
import { MinnowDatabase as Layout10Database } from "@minnowdb/core-layout10";
import { IndexedDbBlockStore as Layout10IndexedDb } from "@minnowdb/core-layout10/storage/indexeddb";
import { OpfsBlockStore as Layout10Store } from "@minnowdb/core-layout10/storage/opfs";
import { MinnowDatabase as Layout6Database } from "@minnowdb/core-layout6";
import { IndexedDbBlockStore as Layout6IndexedDb } from "@minnowdb/core-layout6/storage/indexeddb";
import { OpfsBlockStore as Layout6Store } from "@minnowdb/core-layout6/storage/opfs";
import { MinnowDatabase as Layout7Database } from "@minnowdb/core-layout7";
import { IndexedDbBlockStore as Layout7IndexedDb } from "@minnowdb/core-layout7/storage/indexeddb";
import { OpfsBlockStore as Layout7Store } from "@minnowdb/core-layout7/storage/opfs";
import { MinnowDatabase as Layout8Database } from "@minnowdb/core-layout8";
import { IndexedDbBlockStore as Layout8IndexedDb } from "@minnowdb/core-layout8/storage/indexeddb";
import { OpfsBlockStore as Layout8Store } from "@minnowdb/core-layout8/storage/opfs";
import { MinnowDatabase as Layout9Database } from "@minnowdb/core-layout9";
import { IndexedDbBlockStore as Layout9IndexedDb } from "@minnowdb/core-layout9/storage/indexeddb";
import { OpfsBlockStore as Layout9Store } from "@minnowdb/core-layout9/storage/opfs";
import type {
  IndexedDbUpgradeResult,
  IndexedDbWriterVersion,
  NativeUpgradeResult,
  ReleasedOpfsUpgradeResult,
} from "./upgrade-run.js";

type UpgradeMessage =
  | { kind?: "opfs"; files: Record<string, string>; name: string }
  | { kind: "opfs-released"; name: string }
  | { kind: "indexeddb"; writer: IndexedDbWriterVersion; name: string };

/** The OPFS layout this build writes. */
const OPFS_LAYOUT = 12;
/** The engine's internal key-forwards table, created by the first rekey. */
const KEY_FORWARDS_TABLE = "\u0000minnow_key_forwards";

self.onmessage = (event: MessageEvent<UpgradeMessage>) => {
  const message = event.data;
  const work: Promise<NativeUpgradeResult | IndexedDbUpgradeResult | ReleasedOpfsUpgradeResult> =
    message.kind === "indexeddb"
      ? runIndexedDb(message.writer, message.name)
      : message.kind === "opfs-released"
        ? runReleasedOpfs(message.name)
        : run(message);
  void work.then(
    (result) => self.postMessage({ result }),
    (error: unknown) =>
      self.postMessage({
        error: error instanceof Error ? `${error.name}: ${error.message}` : String(error),
      }),
  );
};

async function run({
  files,
  name,
}: {
  files: Record<string, string>;
  name: string;
}): Promise<NativeUpgradeResult> {
  const root = await navigator.storage.getDirectory();
  for (const [path, base64] of Object.entries(files)) {
    const parts = path.split("/");
    parts[1] = name;
    const filename = parts.pop();
    if (filename === undefined) throw new Error("Invalid native fixture path");
    let directory = root;
    for (const part of parts)
      directory = await directory.getDirectoryHandle(part, { create: true });
    const handle = await (
      await directory.getFileHandle(filename, { create: true })
    ).createSyncAccessHandle();
    try {
      handle.write(Uint8Array.from(atob(base64), (char) => char.charCodeAt(0)));
      handle.flush();
    } finally {
      handle.close();
    }
  }
  let store = await OpfsBlockStore.open({ name });
  let database = new MinnowDatabase(store, { autoCompact: false, autoCollect: false });
  try {
    const tables = (await store.listTables()).map(({ name }) => name);
    const bytes = await store.getBlock("fixture-block");
    if (bytes === undefined) throw new Error("Old fixture block was lost");
    const values = (await decodeBlock(bytes)).column.values;
    const followerBlock = await store.getBlock("fixture-follower-block");
    const walOnlyPreserved =
      followerBlock?.length === bytes.length &&
      followerBlock.every((byte, index) => byte === bytes[index]);
    await database.execute("CREATE TABLE after_upgrade (id INTEGER PRIMARY KEY, value TEXT)");
    await database.execute("INSERT INTO after_upgrade VALUES (1, 'retained')");
    // A rekey: the engine records a forward in its internal key-forwards table, layout 10's record.
    await database.execute("UPDATE after_upgrade SET id = 2 WHERE id = 1");
    await database.close();
    store._crashForTests();
    store = await OpfsBlockStore.open({ name });
    database = new MinnowDatabase(store, { autoCompact: false, autoCollect: false });
    const rows = (await database.query("SELECT id, value FROM after_upgrade")).rows;
    const keyForwards = (await store.getTableByName(KEY_FORWARDS_TABLE))?.keyForwards === true;
    const olderReaderRefused = await olderOpfsReadersRefused(name);
    return {
      tables,
      blockValues: Array.from<unknown>(values),
      walOnlyPreserved,
      rows,
      keyForwards,
      format: await opfsFormat(name),
      integrity: (await store.checkIntegrity({ mode: "full" })).ok,
      olderReaderRefused,
    };
  } finally {
    await database.close();
    store.close();
  }
}

async function opfsFormat(name: string): Promise<number> {
  const root = await navigator.storage.getDirectory();
  const directory = await (await root.getDirectoryHandle("minnowdb")).getDirectoryHandle(name);
  const marker = JSON.parse(
    await (await (await directory.getFileHandle("format.json")).getFile()).text(),
  ) as { formatVersion: number };
  return marker.formatVersion;
}

/** Every released reader of an older layout refuses the upgraded database unchanged. */
async function olderOpfsReadersRefused(name: string): Promise<boolean> {
  const olderReaders = [
    { open: (options: { name: string }) => Layout6Store.open(options), supported: 6 },
    { open: (options: { name: string }) => Layout7Store.open(options), supported: 7 },
    { open: (options: { name: string }) => Layout8Store.open(options), supported: 8 },
    { open: (options: { name: string }) => Layout9Store.open(options), supported: 9 },
    { open: (options: { name: string }) => Layout10Store.open(options), supported: 10 },
  ];
  let refusals = 0;
  for (const reader of olderReaders) {
    try {
      const old = await reader.open({ name });
      old.close();
    } catch (error) {
      if (!(error instanceof Error)) throw error;
      const properties = error as Error & {
        actualVersion?: unknown;
        supportedVersion?: unknown;
      };
      if (
        error.name !== "StorageFormatVersionError" ||
        properties.actualVersion !== OPFS_LAYOUT ||
        properties.supportedVersion !== reader.supported
      )
        throw error;
      refusals += 1;
    }
  }
  return refusals === olderReaders.length;
}

// ---------------------------------------------------------------------------------------------
// OPFS: the released 0.14.1 writer's own database through the automatic layout upgrade
// ---------------------------------------------------------------------------------------------

const opfsItem = (id: number) => ({ id, code: id * 7, label: `item-${String(id)}` });

async function opfsAnswers(database: IndexedHistoryDatabase): Promise<unknown> {
  const codes: unknown[] = [];
  for (const code of [7, 351, 350, 21, 999_999]) {
    codes.push(
      (await database.query("SELECT id FROM items WHERE code = ? ORDER BY id", { params: [code] }))
        .rows,
    );
  }
  return { count: (await database.query("SELECT COUNT(*) AS n FROM items")).rows, codes };
}

async function runReleasedOpfs(name: string): Promise<ReleasedOpfsUpgradeResult> {
  const releasedStore = await Layout9Store.open({ name });
  const released = new Layout9Database(releasedStore, databaseOptions);
  await released.createTable({
    name: "items",
    uniqueKey: "id",
    columns: [
      { name: "id", type: "number" },
      { name: "code", type: "number" },
      { name: "label", type: "string" },
    ],
  });
  await released.insertBatch("items", range(0, 400).map(opfsItem));
  await released.execute("CREATE INDEX items_code ON items (code)");
  await released.insertBatch("items", range(400, 700).map(opfsItem));
  await released.execute("UPDATE items SET code = code + 1 WHERE id % 50 = 0");
  await released.execute("DELETE FROM items WHERE id = 3");
  await released.execute("CREATE SEQUENCE item_ids");
  for (let call = 0; call < 3; call += 1) {
    await released.query("SELECT NEXTVAL('item_ids') AS id");
  }
  const before = await opfsAnswers(released);
  await released.close();
  releasedStore.close();
  const releasedFormat = await opfsFormat(name);

  let store = await OpfsBlockStore.open({ name });
  const format = await opfsFormat(name);
  let database = new MinnowDatabase(store, databaseOptions);
  const upgraded = await opfsAnswers(database);
  const nextValue = (await database.query("SELECT NEXTVAL('item_ids') AS id")).rows;
  await database.insertBatch("items", range(1_000, 3_000).map(opfsItem));
  await database.execute("UPDATE items SET id = 90000 WHERE id = 5");
  await database.close();
  store.close();

  store = await OpfsBlockStore.open({ name });
  database = new MinnowDatabase(store, databaseOptions);
  const rekeyed = (await database.query("SELECT id FROM items WHERE code = ?", { params: [35] }))
    .rows;
  const keyForwards = (await store.getTableByName(KEY_FORWARDS_TABLE))?.keyForwards === true;
  const integrity = (await store.checkIntegrity({ mode: "full" })).ok;
  await database.close();
  store.close();

  const olderReadersRefused = await olderOpfsReadersRefused(name);
  return {
    releasedFormat,
    format,
    answersPreserved: JSON.stringify(upgraded) === JSON.stringify(before),
    nextValue,
    rekeyed,
    keyForwards,
    integrity,
    olderReadersRefused,
    finalFormat: await opfsFormat(name),
  };
}

// ---------------------------------------------------------------------------------------------
// IndexedDB: a released writer's index deltas through the automatic schema upgrade
// ---------------------------------------------------------------------------------------------

/** The IndexedDB schema this build writes. */
const INDEXEDDB_SCHEMA = 7;

interface IndexedHistoryDatabase {
  createTable(definition: unknown): Promise<unknown>;
  insertBatch(table: string, rows: Array<Record<string, string | number>>): Promise<unknown>;
  execute(sql: string): Promise<unknown>;
  query(sql: string, options?: { params?: number[] }): Promise<{ rows: unknown[] }>;
  buildFtsIndex(table: string, column: string): Promise<void>;
  close(): Promise<void>;
}

const databaseOptions = { autoCompact: false, autoCollect: false } as const;
const releasedIndexedDb: Record<
  IndexedDbWriterVersion,
  {
    schema: number;
    open(name: string): Promise<{ close(): void }>;
    database(store: never): IndexedHistoryDatabase;
  }
> = {
  "0.10.0": {
    schema: 2,
    open: (name) => Layout6IndexedDb.open({ name }),
    database: (store: Layout6IndexedDb) => new Layout6Database(store, databaseOptions),
  },
  "0.12.1": {
    schema: 2,
    open: (name) => Layout7IndexedDb.open({ name }),
    database: (store: Layout7IndexedDb) => new Layout7Database(store, databaseOptions),
  },
  "0.13.1": {
    schema: 3,
    open: (name) => Layout8IndexedDb.open({ name }),
    database: (store: Layout8IndexedDb) => new Layout8Database(store, databaseOptions),
  },
  "0.14.1": {
    schema: 4,
    open: (name) => Layout9IndexedDb.open({ name }),
    database: (store: Layout9IndexedDb) => new Layout9Database(store, databaseOptions),
  },
  "0.15.0": {
    schema: 5,
    open: (name) => Layout10IndexedDb.open({ name }),
    database: (store: Layout10IndexedDb) => new Layout10Database(store, databaseOptions),
  },
};

const WORDS = ["amber", "birch", "cedar", "delta", "ember"];
const range = (start: number, end: number) =>
  Array.from({ length: end - start }, (_, index) => start + index);
const item = (id: number) => ({ id, code: id * 7, label: `item-${String(id)}` });
const note = (index: number) => ({ body: `entry ${WORDS[index % 5] ?? ""} n${String(index)}` });

function nativeVersion(name: string): Promise<number> {
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

/** Base postings plus deltas for a secondary index and a full-text index. */
async function writeIndexedHistory(database: IndexedHistoryDatabase): Promise<void> {
  await database.createTable({
    name: "items",
    uniqueKey: "id",
    columns: [
      { name: "id", type: "number" },
      { name: "code", type: "number" },
      { name: "label", type: "string" },
    ],
  });
  await database.insertBatch("items", range(0, 400).map(item));
  await database.execute("CREATE INDEX items_code ON items (code)");
  await database.insertBatch("items", range(400, 700).map(item));
  await database.execute("UPDATE items SET code = code + 1 WHERE id % 50 = 0");
  await database.createTable({ name: "notes", columns: [{ name: "body", type: "string" }] });
  await database.insertBatch("notes", range(0, 300).map(note));
  await database.buildFtsIndex("notes", "body");
  await database.insertBatch("notes", range(300, 500).map(note));
}

async function indexedAnswers(database: IndexedHistoryDatabase, extraCodes: number[] = []) {
  const codes: unknown[] = [];
  for (const code of [7, 351, 350, 1, 2_807, 4_893, 999_999, ...extraCodes]) {
    codes.push(
      (await database.query("SELECT id FROM items WHERE code = ? ORDER BY id", { params: [code] }))
        .rows,
    );
  }
  const matches: unknown[] = [];
  for (const word of ["cedar", "n12", "n450", "n9999"]) {
    matches.push(
      (await database.query(`SELECT COUNT(*) AS n FROM notes WHERE MATCH(body) AGAINST '${word}'`))
        .rows,
    );
  }
  return { codes, matches };
}

async function runIndexedDb(
  writer: IndexedDbWriterVersion,
  name: string,
): Promise<IndexedDbUpgradeResult> {
  const released = releasedIndexedDb[writer];
  const releasedStore = await released.open(name);
  const releasedDatabase = released.database(releasedStore as never);
  await writeIndexedHistory(releasedDatabase);
  const before = await indexedAnswers(releasedDatabase);
  await releasedDatabase.close();
  releasedStore.close();
  const releasedSchema = await nativeVersion(name);

  let store = await IndexedDbBlockStore.open({ name });
  const schema = await nativeVersion(name);
  let database = new MinnowDatabase(store, databaseOptions);
  const upgraded = await indexedAnswers(database);
  // More distinct values and terms in one commit than a schema-3 delta record could hold.
  await database.insertBatch("items", range(10_000, 80_000).map(item));
  await database.insertBatch("notes", range(500, 10_000).map(note));
  const written = await indexedAnswers(database, [70_000 * 7]);
  await database.close();
  store.close();

  store = await IndexedDbBlockStore.open({ name });
  database = new MinnowDatabase(store, databaseOptions);
  const reopened = await indexedAnswers(database, [70_000 * 7]);
  // A rekey: the engine records a forward in its internal key-forwards table, schema 5's record.
  await database.execute("UPDATE items SET id = 90000 WHERE id = 5");
  await database.close();
  store.close();

  store = await IndexedDbBlockStore.open({ name });
  database = new MinnowDatabase(store, databaseOptions);
  const rekeyed = (await database.query("SELECT id FROM items WHERE code = ?", { params: [35] }))
    .rows;
  const keyForwards = (await store.getTableByName(KEY_FORWARDS_TABLE))?.keyForwards === true;
  // Schema 6's record: a fold of the released writer's populated table across ADD COLUMN, whose
  // older sources have no blocks for the column and fold to its default.
  await database.execute("ALTER TABLE items ADD COLUMN tag TEXT DEFAULT 'old'");
  await database.execute("INSERT INTO items VALUES (95000, 1, 'after', 'new')");
  await database.compactTable("items", { minimumLevel0Segments: 1 });
  const foldedAcrossAddColumn = (
    await database.query("SELECT tag, COUNT(*) AS n FROM items GROUP BY tag ORDER BY tag")
  ).rows;
  // A fold of the released writer's append-only table keeps its full-text index: the rows and
  // row IDs the postings name are the ones the fold wrote back.
  await database.compactTable("notes", { minimumLevel0Segments: 1 });
  const foldedFullText = {
    states: Object.values((await store.getTableByName("notes"))?.ftsColumns ?? {}).map(
      (index) => index.state,
    ),
    matches: (await indexedAnswers(database)).matches,
  };
  const integrity = (await store.checkIntegrity({ mode: "full" })).ok;
  await database.close();
  store.close();

  // Every released reader refuses the upgraded database and leaves it as it is.
  let refusals = 0;
  for (const reader of Object.values(releasedIndexedDb)) {
    try {
      (await reader.open(name)).close();
    } catch (error) {
      const properties = error as Error & { actualVersion?: unknown; supportedVersion?: unknown };
      if (
        properties.name !== "StorageFormatVersionError" ||
        properties.actualVersion !== INDEXEDDB_SCHEMA ||
        properties.supportedVersion !== reader.schema
      ) {
        throw error;
      }
      refusals += 1;
    }
  }
  return {
    releasedSchema,
    schema,
    answersPreserved: JSON.stringify(upgraded) === JSON.stringify(before),
    newRow: written.codes.at(-1),
    matches: written.matches,
    reopenedSame: JSON.stringify(reopened) === JSON.stringify(written),
    rekeyed,
    keyForwards,
    foldedAcrossAddColumn,
    foldedFullText,
    integrity,
    olderReadersRefused: refusals === Object.keys(releasedIndexedDb).length,
    finalSchema: await nativeVersion(name),
  };
}
