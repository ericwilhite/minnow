import { MinnowDatabase } from "@minnowdb/core";
import { decodeBlock } from "@minnowdb/core/block-format";
import { IndexedDbBlockStore } from "@minnowdb/core/storage/indexeddb";
import { OpfsBlockStore } from "@minnowdb/core/storage/opfs";
import { MinnowDatabase as Layout6Database } from "@minnowdb/core-layout6";
import { IndexedDbBlockStore as Layout6IndexedDb } from "@minnowdb/core-layout6/storage/indexeddb";
import { OpfsBlockStore as Layout6Store } from "@minnowdb/core-layout6/storage/opfs";
import { MinnowDatabase as Layout7Database } from "@minnowdb/core-layout7";
import { IndexedDbBlockStore as Layout7IndexedDb } from "@minnowdb/core-layout7/storage/indexeddb";
import { OpfsBlockStore as Layout7Store } from "@minnowdb/core-layout7/storage/opfs";
import { MinnowDatabase as Layout8Database } from "@minnowdb/core-layout8";
import { IndexedDbBlockStore as Layout8IndexedDb } from "@minnowdb/core-layout8/storage/indexeddb";
import type {
  IndexedDbUpgradeResult,
  IndexedDbWriterVersion,
  NativeUpgradeResult,
} from "./upgrade-run.js";

type UpgradeMessage =
  | { kind?: "opfs"; files: Record<string, string>; name: string }
  | { kind: "indexeddb"; writer: IndexedDbWriterVersion; name: string };

self.onmessage = (event: MessageEvent<UpgradeMessage>) => {
  const message = event.data;
  const work: Promise<NativeUpgradeResult | IndexedDbUpgradeResult> =
    message.kind === "indexeddb" ? runIndexedDb(message.writer, message.name) : run(message);
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
    await database.close();
    store._crashForTests();
    store = await OpfsBlockStore.open({ name });
    database = new MinnowDatabase(store, { autoCompact: false, autoCollect: false });
    const rows = (await database.query("SELECT id, value FROM after_upgrade")).rows;
    const directory = await (await root.getDirectoryHandle("minnowdb")).getDirectoryHandle(name);
    const marker = JSON.parse(
      await (await (await directory.getFileHandle("format.json")).getFile()).text(),
    ) as { formatVersion: number };
    // Every released reader of an older layout refuses the upgraded database unchanged.
    const olderReaders = [
      { open: (options: { name: string }) => Layout6Store.open(options), supported: 6 },
      { open: (options: { name: string }) => Layout7Store.open(options), supported: 7 },
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
          properties.actualVersion !== 8 ||
          properties.supportedVersion !== reader.supported
        )
          throw error;
        refusals += 1;
      }
    }
    const olderReaderRefused = refusals === olderReaders.length;
    return {
      tables,
      blockValues: Array.from<unknown>(values),
      walOnlyPreserved,
      rows,
      format: marker.formatVersion,
      integrity: (await store.checkIntegrity({ mode: "full" })).ok,
      olderReaderRefused,
    };
  } finally {
    await database.close();
    store.close();
  }
}

// ---------------------------------------------------------------------------------------------
// IndexedDB: a released writer's index deltas through the automatic schema upgrade
// ---------------------------------------------------------------------------------------------

/** The IndexedDB schema this build writes. */
const INDEXEDDB_SCHEMA = 4;

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
    integrity,
    olderReadersRefused: refusals === Object.keys(releasedIndexedDb).length,
    finalSchema: await nativeVersion(name),
  };
}
