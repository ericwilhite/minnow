/**
 * IndexedDB schema 5: the catalog may hold the engine's internal key-forwards table, a table
 * record marked `keyForwards: true` that maps each rekeyed row's retired key to its current one.
 * A release through 0.14.1 would read that record as an ordinary table and ignore its forwards,
 * so the version is the barrier that keeps those readers out. Nothing stored changes shape, so
 * the upgrade rewrites nothing.
 *
 * Every supported older writer — 0.10.0 and 0.12.1 (schema 2), 0.13.1 (schema 3), 0.14.1
 * (schema 4) — leaves a keyed table with a secondary index and, where the release supports it on
 * IndexedDB, a sequence. The current build opens each through the ordinary API, answers from it,
 * writes more (an ordinary table and the key-forwards table among it), and reopens; the released
 * reader is refused afterwards without a change. The rest pins an interrupted upgrade and its
 * retry, concurrent openers, and the refusal of a malformed key-forwards marker as corruption.
 */
import { IDBFactory } from "fake-indexeddb";
import { describe, expect, it, vi } from "vitest";
import { MinnowDatabase as Layout6Database } from "@minnowdb/core-layout6";
import { IndexedDbBlockStore as Layout6Store } from "@minnowdb/core-layout6/storage/indexeddb";
import { MinnowDatabase as Layout7Database } from "@minnowdb/core-layout7";
import { IndexedDbBlockStore as Layout7Store } from "@minnowdb/core-layout7/storage/indexeddb";
import { MinnowDatabase as Layout8Database } from "@minnowdb/core-layout8";
import { IndexedDbBlockStore as Layout8Store } from "@minnowdb/core-layout8/storage/indexeddb";
import { MinnowDatabase as Layout9Database } from "@minnowdb/core-layout9";
import { IndexedDbBlockStore as Layout9Store } from "@minnowdb/core-layout9/storage/indexeddb";
import { MinnowDatabase } from "../engine/database.js";
import { keyForwardsTableRecord } from "../engine/key-forwards.js";
import { heavyTestTimeout } from "../engine/storage-test-helpers.js";
import { NOW, openStore, readRawValue } from "./indexeddb-audit-helpers.js";
import { IndexedDbBlockStore } from "./indexeddb.js";
import { StorageCorruptionError, type TableRecord } from "./types.js";

vi.setConfig({ testTimeout: heavyTestTimeout(60_000) });

/** The schema this build writes: schema 6 (fills in compaction plans) followed schema 5. */
const CURRENT_SCHEMA = 6;

/** The engine's internal key-forwards table, as the engine creates it on its first rekey. */
const KEY_FORWARDS: TableRecord = keyForwardsTableRecord(NOW);

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

async function rawCatalogPut(
  indexedDB: IDBFactory,
  name: string,
  entries: ReadonlyArray<readonly [IDBValidKey, unknown]>,
): Promise<void> {
  const database = await new Promise<IDBDatabase>((resolve, reject) => {
    const request = indexedDB.open(name);
    request.onsuccess = () => resolve(request.result);
    request.onerror = () => reject(request.error ?? new Error("raw open failed"));
  });
  try {
    await new Promise<void>((resolve, reject) => {
      const transaction = database.transaction("catalog", "readwrite");
      for (const [key, value] of entries) transaction.objectStore("catalog").put(value, key);
      transaction.oncomplete = () => resolve();
      transaction.onabort = () => reject(transaction.error ?? new Error("raw write aborted"));
    });
  } finally {
    database.close();
  }
}

// ---------------------------------------------------------------------------------------------
// Released writers
// ---------------------------------------------------------------------------------------------

interface ReleasedDatabase {
  createTable(definition: unknown): Promise<unknown>;
  insertBatch(table: string, rows: Array<Record<string, string | number>>): Promise<unknown>;
  execute(sql: string): Promise<unknown>;
  query(sql: string, options?: { params?: number[] }): Promise<{ rows: unknown[] }>;
  close(): Promise<void>;
}

interface ReleasedBuild {
  version: string;
  schema: number;
  /** Whether the release creates a sequence on IndexedDB (0.10.3 fixed it there). */
  sequences: boolean;
  openStore(indexedDB: IDBFactory, name: string): Promise<{ close(): void }>;
  openDatabase(store: never): ReleasedDatabase;
}

// The clock is the fixture's, so the forwards table a rekey creates is exactly KEY_FORWARDS.
const options = { autoCompact: false, autoCollect: false, now: () => new Date(NOW) } as const;
const releasedBuilds: ReleasedBuild[] = [
  {
    version: "0.10.0",
    schema: 2,
    sequences: false,
    openStore: (indexedDB, name) => Layout6Store.open({ name, indexedDB }),
    openDatabase: (store: Layout6Store) => new Layout6Database(store, options),
  },
  {
    version: "0.12.1",
    schema: 2,
    sequences: true,
    openStore: (indexedDB, name) => Layout7Store.open({ name, indexedDB }),
    openDatabase: (store: Layout7Store) => new Layout7Database(store, options),
  },
  {
    version: "0.13.1",
    schema: 3,
    sequences: true,
    openStore: (indexedDB, name) => Layout8Store.open({ name, indexedDB }),
    openDatabase: (store: Layout8Store) => new Layout8Database(store, options),
  },
  {
    version: "0.14.1",
    schema: 4,
    sequences: true,
    openStore: (indexedDB, name) => Layout9Store.open({ name, indexedDB }),
    openDatabase: (store: Layout9Store) => new Layout9Database(store, options),
  },
];

const range = (start: number, end: number) =>
  Array.from({ length: end - start }, (_, index) => start + index);
const item = (id: number) => ({ id, code: id * 7, label: `item-${String(id)}` });

/** A keyed table with a secondary index over base and delta postings, and a sequence. */
async function writeHistory(database: ReleasedDatabase, sequences: boolean): Promise<void> {
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
  await database.execute("DELETE FROM items WHERE id = 3");
  if (sequences) {
    await database.execute("CREATE SEQUENCE item_ids");
    for (let call = 0; call < 3; call += 1) {
      await database.query("SELECT NEXTVAL('item_ids') AS id");
    }
  }
}

interface Answers {
  count: unknown;
  codes: unknown[];
  label: unknown;
}

async function answers(database: ReleasedDatabase, extraCodes: number[] = []): Promise<Answers> {
  const codes: unknown[] = [];
  for (const code of [7, 351, 350, 21, 4_893, 999_999, ...extraCodes]) {
    codes.push(
      (await database.query("SELECT id FROM items WHERE code = ? ORDER BY id", { params: [code] }))
        .rows,
    );
  }
  return {
    count: (await database.query("SELECT COUNT(*) AS n FROM items")).rows,
    codes,
    label: (await database.query("SELECT label FROM items WHERE id = ?", { params: [650] })).rows,
  };
}

async function releasedHistory(
  build: ReleasedBuild,
  indexedDB: IDBFactory,
  name: string,
): Promise<Answers> {
  const store = await build.openStore(indexedDB, name);
  const database = build.openDatabase(store as never);
  await writeHistory(database, build.sequences);
  const expected = await answers(database);
  await database.close();
  store.close();
  expect(await nativeVersion(indexedDB, name)).toBe(build.schema);
  return expected;
}

/** The current build's writes on top of an upgraded database. */
async function writeCurrent(store: IndexedDbBlockStore, database: MinnowDatabase): Promise<void> {
  await database.insertBatch("items", range(1_000, 3_000).map(item));
  await database.execute("CREATE TABLE after_upgrade (id INTEGER PRIMARY KEY, value TEXT)");
  await database.execute("INSERT INTO after_upgrade VALUES (1, 'retained')");
  // A real rekey through the ordinary API: it creates the key-forwards table, moves the row,
  // and stores the forward, all on data the released writer wrote.
  const before = await store.getCatalogProbe();
  await database.execute("UPDATE items SET id = 900650 WHERE id = 650");
  const after = await store.getCatalogProbe();
  expect(after.catalogEpoch).toBeGreaterThan(before.catalogEpoch);
  expect(after.schemaEpoch).toBe(before.schemaEpoch);
}

async function expectCurrentWrites(store: IndexedDbBlockStore, database: MinnowDatabase) {
  expect((await database.query("SELECT id, value FROM after_upgrade")).rows).toEqual([
    { id: 1, value: "retained" },
  ]);
  expect(
    (await database.query("SELECT id FROM items WHERE code = ?", { params: [2_999 * 7] })).rows,
  ).toEqual([{ id: 2_999 }]);
  expect(await store.getTable(KEY_FORWARDS.id)).toEqual(KEY_FORWARDS);
  expect(await store.getTableByName(KEY_FORWARDS.name)).toEqual(KEY_FORWARDS);
  // The retired key still reaches the moved row, by SQL and by resolveKey.
  expect(
    (await database.query("SELECT id, label FROM items WHERE id = ?", { params: [650] })).rows,
  ).toEqual([{ id: 900650, label: "item-650" }]);
  expect(await database.resolveKey("items", 650)).toBe(900650);
}

describe.each(releasedBuilds)("IndexedDB schema 5 upgrade from the $version writer", (build) => {
  it("answers from the released history, writes the key-forwards table, and reopens", async () => {
    const indexedDB = new IDBFactory();
    const name = crypto.randomUUID();
    const expected = await releasedHistory(build, indexedDB, name);

    let store = await IndexedDbBlockStore.open({ name, indexedDB });
    expect(await nativeVersion(indexedDB, name)).toBe(CURRENT_SCHEMA);
    let database = new MinnowDatabase(store, options);
    expect(await answers(database)).toEqual(expected);
    expect((await store.checkIntegrity({ mode: "full" })).issues).toEqual([]);
    await writeCurrent(store, database);
    if (build.sequences) {
      // The sequence continues past the released writer's three calls.
      expect((await database.query("SELECT NEXTVAL('item_ids') AS id")).rows).toEqual([{ id: 4 }]);
    }
    const latest = await answers(database, [2_000 * 7]);
    await database.close();
    store.close();

    store = await IndexedDbBlockStore.open({ name, indexedDB });
    database = new MinnowDatabase(store, options);
    expect(await answers(database, [2_000 * 7])).toEqual(latest);
    await expectCurrentWrites(store, database);
    if (build.sequences) {
      expect((await database.query("SELECT NEXTVAL('item_ids') AS id")).rows).toEqual([{ id: 5 }]);
    }
    expect((await store.checkIntegrity({ mode: "full" })).issues).toEqual([]);
    await database.close();
    store.close();
  });

  it("refuses the released reader after the upgrade and leaves the database intact", async () => {
    const indexedDB = new IDBFactory();
    const name = crypto.randomUUID();
    const expected = await releasedHistory(build, indexedDB, name);
    const upgraded = await IndexedDbBlockStore.open({ name, indexedDB });
    const upgradedDatabase = new MinnowDatabase(upgraded, options);
    await writeCurrent(upgraded, upgradedDatabase);
    const latest = await answers(upgradedDatabase, [2_000 * 7]);
    await upgradedDatabase.close();
    upgraded.close();

    await expect(build.openStore(indexedDB, name)).rejects.toMatchObject({
      name: "StorageFormatVersionError",
      actualVersion: CURRENT_SCHEMA,
      supportedVersion: build.schema,
      relation: "newer",
    });
    expect(await nativeVersion(indexedDB, name)).toBe(CURRENT_SCHEMA);
    expect(await readRawValue(indexedDB, name, "catalog", `table/id/${KEY_FORWARDS.id}`)).toEqual(
      KEY_FORWARDS,
    );
    const store = await IndexedDbBlockStore.open({ name, indexedDB });
    const database = new MinnowDatabase(store, options);
    expect(await answers(database, [2_000 * 7])).toEqual(latest);
    expect(latest.count).not.toEqual(expected.count);
    await expectCurrentWrites(store, database);
    await database.close();
    store.close();
  });
});

/** A factory whose next upgrade transaction aborts after the store's own upgrade handler ran. */
function abortNextUpgrade(indexedDB: IDBFactory): { aborted: () => boolean } {
  let aborted = false;
  const open = indexedDB.open.bind(indexedDB);
  Object.defineProperty(indexedDB, "open", {
    configurable: true,
    value: (name: string, version?: number) => {
      const request = version === undefined ? open(name) : open(name, version);
      // The store registers its handler right after open returns; this one runs after it.
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
  return { aborted: () => aborted };
}

describe("IndexedDB schema 4 to current upgrade", () => {
  const released = releasedBuilds.find((build) => build.schema === 4);
  if (released === undefined) throw new Error("The schema-4 writer is missing");

  it("leaves schema 4 for the released reader when the upgrade is interrupted, then retries", async () => {
    const indexedDB = new IDBFactory();
    const name = crypto.randomUUID();
    const expected = await releasedHistory(released, indexedDB, name);
    // The tab dies inside the version-change transaction: IndexedDB rolls all of it back.
    const interruption = abortNextUpgrade(indexedDB);
    await expect(IndexedDbBlockStore.open({ name, indexedDB })).rejects.toThrow();
    expect(interruption.aborted()).toBe(true);
    expect(await nativeVersion(indexedDB, name)).toBe(4);
    // Still the released writer's database, readable and writable by it.
    const releasedStore = await released.openStore(indexedDB, name);
    const releasedDatabase = released.openDatabase(releasedStore as never);
    expect(await answers(releasedDatabase)).toEqual(expected);
    await releasedDatabase.insertBatch("items", [item(5_000)]);
    await releasedDatabase.close();
    releasedStore.close();

    const store = await IndexedDbBlockStore.open({ name, indexedDB });
    expect(await nativeVersion(indexedDB, name)).toBe(CURRENT_SCHEMA);
    const database = new MinnowDatabase(store, options);
    expect((await answers(database, [35_000])).codes).toEqual([...expected.codes, [{ id: 5_000 }]]);
    await writeCurrent(store, database);
    await expectCurrentWrites(store, database);
    expect((await store.checkIntegrity({ mode: "full" })).issues).toEqual([]);
    await database.close();
    store.close();
  });

  it("lets concurrent current openers share one upgrade and closes or refuses released ones", async () => {
    const indexedDB = new IDBFactory();
    const name = crypto.randomUUID();
    const expected = await releasedHistory(released, indexedDB, name);
    const stale = await released.openStore(indexedDB, name);
    const [first, second, late] = await Promise.allSettled([
      IndexedDbBlockStore.open({ name, indexedDB }),
      IndexedDbBlockStore.open({ name, indexedDB }),
      released.openStore(indexedDB, name),
    ]);
    expect(first.status).toBe("fulfilled");
    expect(second.status).toBe("fulfilled");
    // A released opener that arrives behind the upgrade is refused, never served the current schema.
    expect(late).toMatchObject({
      status: "rejected",
      reason: { name: "StorageFormatVersionError", actualVersion: CURRENT_SCHEMA },
    });
    // A released connection that was open when the upgrade arrived closed itself.
    await expect(
      (
        stale as unknown as { getCurrentManifestVersion(): Promise<number | null> }
      ).getCurrentManifestVersion(),
    ).rejects.toThrow(/connection is closed/);
    const stores = [first, second].map((result) =>
      result.status === "fulfilled" ? result.value : undefined,
    );
    for (const store of stores) {
      if (store === undefined) continue;
      const database = new MinnowDatabase(store, options);
      expect(await answers(database)).toEqual(expected);
    }
    await stores[0]?.addTable(KEY_FORWARDS);
    expect(await stores[1]?.getTableByName(KEY_FORWARDS.name)).toEqual(KEY_FORWARDS);
    for (const store of stores) store?.close();
    expect(await nativeVersion(indexedDB, name)).toBe(CURRENT_SCHEMA);
  });
});

describe("IndexedDB key-forwards catalog record", () => {
  it("refuses a malformed key-forwards marker as corruption", async () => {
    const { keyForwards: _marker, ...plain } = KEY_FORWARDS;
    void _marker;
    const malformed: Array<[string, Record<string, unknown>]> = [
      ["false", { ...plain, keyForwards: false }],
      ["a string", { ...plain, keyForwards: "true" }],
      ["a number", { ...plain, keyForwards: 1 }],
      ["an object", { ...plain, keyForwards: { enabled: true } }],
      ["null", { ...plain, keyForwards: null }],
      [
        "beside a sequence",
        { ...plain, keyForwards: true, sequence: { name: "s", start: 1, columnId: "retired" } },
      ],
      ["beside an enum type", { ...plain, keyForwards: true, enumType: { name: "e", values: [] } }],
    ];
    for (const [label, record] of malformed) {
      const indexedDB = new IDBFactory();
      const name = crypto.randomUUID();
      const store = await openStore(indexedDB, name);
      await rawCatalogPut(indexedDB, name, [
        [`table/id/${KEY_FORWARDS.id}`, record],
        [`table/name/${KEY_FORWARDS.name}`, KEY_FORWARDS.id],
      ]);
      await expect(store.getTable(KEY_FORWARDS.id), label).rejects.toBeInstanceOf(
        StorageCorruptionError,
      );
      await expect(store.listTables(), label).rejects.toBeInstanceOf(StorageCorruptionError);
      const report = await store.checkIntegrity({ mode: "full" });
      expect(report.ok, label).toBe(false);
      store.close();
    }
  });

  it("reads a well-formed marker written outside the adapter", async () => {
    const indexedDB = new IDBFactory();
    const name = crypto.randomUUID();
    const store = await openStore(indexedDB, name);
    await rawCatalogPut(indexedDB, name, [
      [`table/id/${KEY_FORWARDS.id}`, KEY_FORWARDS],
      [`table/name/${KEY_FORWARDS.name}`, KEY_FORWARDS.id],
    ]);
    expect(await store.getTable(KEY_FORWARDS.id)).toEqual(KEY_FORWARDS);
    store.close();
  });
});
