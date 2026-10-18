/**
 * Databases the last released writer, 0.16.0, left — IndexedDB schema 7, OPFS layout 12 — with
 * columns it declared TIMESTAMP and TIMESTAMPTZ, which that release stored alike, as instants.
 * This build records a column declared TIMESTAMP (without time zone) on its catalog record
 * (`withoutTimeZone: true`) and renders its text without a zone. A 0.16.0 reader refuses that
 * field as corruption on IndexedDB and would render the column as an instant on OPFS, so
 * IndexedDB schema 8 and OPFS layout 13 bar it; the upgrade moves only the version marker.
 *
 * These tests take the real 0.16.0 writer's databases through the ordinary open API: the columns
 * it wrote stay instants (their text keeps `+00`, whatever the DDL said), the history reads the
 * same, new TIMESTAMP columns render without a zone, writes resume, the database reopens, and the
 * released reader is refused afterwards without a change. They also cut the IndexedDB upgrade
 * mid-way and retry, race current and released openers, and refuse a damaged field as
 * corruption. The OPFS marker's publication steps are cut, by tab death and by power loss, in
 * marker-upgrade.test.ts over the frozen layout-12 fixture.
 */
import { IDBFactory } from "fake-indexeddb";
import { describe, expect, it, vi } from "vitest";
import { MinnowDatabase as ReleasedDatabase } from "@minnowdb/core-layout12";
import { IndexedDbBlockStore as ReleasedIndexedDbStore } from "@minnowdb/core-layout12/storage/indexeddb";
import { OpfsBlockStore as ReleasedOpfsStore } from "@minnowdb/core-layout12/storage/opfs";
import {
  IndexedDbBlockStore,
  OpfsBlockStore,
  StorageCorruptionError,
  type BlockStore,
} from "../storage/index.js";
import { MemoryOpfs } from "../testing/opfs-shim.js";
import { MinnowDatabase } from "./database.js";
import { heavyTestTimeout } from "./storage-test-helpers.js";

vi.setConfig({ testTimeout: heavyTestTimeout(120_000) });

/** What this build writes, and what the 0.16.0 writer wrote. */
const CURRENT_SCHEMA = 8;
const RELEASED_SCHEMA = 7;
const CURRENT_LAYOUT = 13;
const RELEASED_LAYOUT = 12;
const NAME = "timestamp-kind-upgrade";
const OPTIONS = { autoCompact: false, autoCollect: false } as const;
const HOUR = 3_600_000;
const START = Date.UTC(2025, 11, 1);

/** The released writer, as these tests drive it. */
interface ReleasedWriter {
  execute(sql: string, params?: Array<number | string>): Promise<unknown>;
  insertBatch(table: string, rows: Array<Record<string, unknown>>): Promise<unknown>;
  query(sql: string): Promise<{ rows: unknown[] }>;
  close(): Promise<void>;
}

/** Every column the released writer declared, and its text through both renderings. */
const ALL =
  "SELECT id, at, zoned, label, at::text AS at_text, 'z ' || zoned AS zoned_text, TO_JSON(at) AS at_json FROM events ORDER BY id";

/** `2025-12-01 03:00:00` for id 3: the text of the hour `id` hours after START. */
function hourText(id: number): string {
  return new Date(START + id * HOUR).toISOString().slice(0, 19).replace("T", " ");
}

/**
 * Columns the released writer declared TIMESTAMP and TIMESTAMPTZ, an index over one, a view, and
 * updates and deletes: a 0.16.0 database as an application would have left it.
 */
async function writeReleasedHistory(writer: ReleasedWriter): Promise<unknown[]> {
  await writer.execute(
    "CREATE TABLE events (id INTEGER PRIMARY KEY, at TIMESTAMP, zoned TIMESTAMPTZ, label TEXT)",
  );
  await writer.execute("CREATE INDEX events_at ON events (at)");
  for (let start = 0; start < 300; start += 100) {
    await writer.insertBatch(
      "events",
      Array.from({ length: 100 }, (_, index) => ({
        id: start + index,
        at: new Date(START + (start + index) * HOUR),
        zoned: new Date(START + (start + index) * HOUR),
        label: `event-${String(start + index)}`,
      })),
    );
  }
  await writer.execute("UPDATE events SET label = 'moved' WHERE id % 7 = 0");
  await writer.execute("DELETE FROM events WHERE id = 5");
  await writer.execute("CREATE VIEW recent AS SELECT id, at FROM events WHERE id >= 250");
  const rows = (await writer.query(ALL)).rows;
  // 0.16.0 stored both declarations as instants: TIMESTAMP text carries `+00` there too.
  expect(rows[3]).toMatchObject({
    at_text: `${hourText(3)}+00`,
    zoned_text: `z ${hourText(3)}+00`,
  });
  return rows;
}

/** What the upgraded database must answer about the released writer's history. */
async function expectHistory(database: MinnowDatabase, rows: unknown[]): Promise<void> {
  expect((await database.query(ALL, { memoize: false })).rows).toEqual(rows);
  // The released writer's columns stay instants, whatever their DDL said.
  const columns = (await database.introspect()).tables.find(({ name }) => name === "events");
  const released = columns?.columns.slice(0, 4);
  expect(released?.map(({ name, withoutTimeZone }) => [name, withoutTimeZone])).toEqual([
    ["id", undefined],
    ["at", undefined],
    ["zoned", undefined],
    ["label", undefined],
  ]);
  expect(
    (await database.query("SELECT at::text AS t FROM recent WHERE id = 250", { memoize: false }))
      .rows,
  ).toEqual([{ t: `${hourText(250)}+00` }]);
  expect(
    (
      await database.query("SELECT id FROM events WHERE at = ?", {
        params: [new Date(START + 42 * HOUR)],
        memoize: false,
      })
    ).rows,
  ).toEqual([{ id: 42 }]);
}

/** The current build's TIMESTAMP columns on top of the upgraded database. */
async function writeCurrent(database: MinnowDatabase): Promise<void> {
  await database.execute("ALTER TABLE events ADD COLUMN local_at TIMESTAMP");
  await database.execute("UPDATE events SET local_at = at WHERE id = 3");
  await database.execute(
    "CREATE TABLE stamps (id INTEGER PRIMARY KEY, local_at TIMESTAMP, zoned_at TIMESTAMPTZ)",
  );
  await database.execute(
    "INSERT INTO stamps VALUES (1, '2026-01-02 03:04:05.5', '2026-01-02 03:04:05.5')",
  );
  await expectCurrent(database);
}

const CURRENT_TEXT =
  "SELECT s.local_at::text AS local_text, s.zoned_at::text AS zoned_text, TO_JSON(s.local_at) AS local_json, e.local_at::text AS added, e.local_at = e.at AS same FROM stamps s, events e WHERE e.id = 3";

async function expectCurrent(database: MinnowDatabase): Promise<void> {
  expect((await database.query(CURRENT_TEXT, { memoize: false })).rows).toEqual([
    {
      local_text: "2026-01-02 03:04:05.5",
      zoned_text: "2026-01-02 03:04:05.5+00",
      local_json: '"2026-01-02T03:04:05.5"',
      added: hourText(3),
      same: true,
    },
  ]);
  const catalog = await database.introspect();
  const kinds = (table: string) =>
    catalog.tables
      .find(({ name }) => name === table)
      ?.columns.map(({ name, withoutTimeZone }) => [name, withoutTimeZone === true]);
  expect(kinds("stamps")).toEqual([
    ["id", false],
    ["local_at", true],
    ["zoned_at", false],
  ]);
  expect(kinds("events")?.at(-1)).toEqual(["local_at", true]);
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

async function rawCatalog(
  indexedDB: IDBFactory,
  name: string,
  change: (catalog: IDBObjectStore) => void,
): Promise<void> {
  const database = await new Promise<IDBDatabase>((resolve, reject) => {
    const request = indexedDB.open(name);
    request.onsuccess = () => resolve(request.result);
    request.onerror = () => reject(request.error ?? new Error("raw open failed"));
  });
  try {
    await new Promise<void>((resolve, reject) => {
      const transaction = database.transaction("catalog", "readwrite");
      change(transaction.objectStore("catalog"));
      transaction.oncomplete = () => resolve();
      transaction.onabort = () => reject(transaction.error ?? new Error("raw write aborted"));
    });
  } finally {
    database.close();
  }
}

async function rawTableRecord(
  indexedDB: IDBFactory,
  name: string,
  id: string,
): Promise<Record<string, unknown>> {
  let record: unknown;
  await rawCatalog(indexedDB, name, (catalog) => {
    const request = catalog.get(`table/id/${id}`);
    request.onsuccess = () => {
      record = request.result;
    };
  });
  if (typeof record !== "object" || record === null) throw new Error(`No raw table ${id}`);
  return record as Record<string, unknown>;
}

describe("IndexedDB: the 0.16.0 writer's datetimes after the schema 8 upgrade", () => {
  it("keeps its columns as instants, declares TIMESTAMP columns, reopens, and refuses the released reader", async () => {
    const indexedDB = new IDBFactory();
    const name = crypto.randomUUID();
    const rows = await releasedIndexedDb(indexedDB, name);

    let store = await IndexedDbBlockStore.open({ name, indexedDB });
    expect(await nativeVersion(indexedDB, name)).toBe(CURRENT_SCHEMA);
    let database = new MinnowDatabase(store, OPTIONS);
    await expectHistory(database, rows);
    await writeCurrent(database);
    expect((await store.checkIntegrity({ mode: "full" })).issues).toEqual([]);
    await database.close();
    store.close();

    store = await IndexedDbBlockStore.open({ name, indexedDB });
    database = new MinnowDatabase(store, OPTIONS);
    await expectCurrent(database);
    expect(
      (await database.query("SELECT id, at, zoned, label FROM events WHERE id <> 3 ORDER BY id"))
        .rows,
    ).toEqual(
      (rows as Array<Record<string, unknown>>)
        .filter(({ id }) => id !== 3)
        .map(({ id, at, zoned, label }) => ({ id, at, zoned, label })),
    );
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
    await expectCurrent(database);
    await database.close();
    store.close();
  });

  it("leaves schema 7 for the released writer when the upgrade is interrupted, then retries", async () => {
    const indexedDB = new IDBFactory();
    const name = crypto.randomUUID();
    const rows = (await releasedIndexedDb(indexedDB, name)) as Array<Record<string, unknown>>;
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
    await released.execute("UPDATE events SET label = 'released-after' WHERE id = 7");
    await released.close();
    releasedStore.close();
    const expected = rows.map((row) => (row.id === 7 ? { ...row, label: "released-after" } : row));

    const store = await IndexedDbBlockStore.open({ name, indexedDB });
    expect(await nativeVersion(indexedDB, name)).toBe(CURRENT_SCHEMA);
    const database = new MinnowDatabase(store, OPTIONS);
    await expectHistory(database, expected);
    await writeCurrent(database);
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
    const [one, two] = stores.map((store) =>
      store === undefined ? undefined : new MinnowDatabase(store, OPTIONS),
    );
    if (one === undefined || two === undefined) throw new Error("Both openers must succeed");
    // One connection declares the TIMESTAMP columns; the other reads them after it.
    await writeCurrent(one);
    await expectHistory(two, rows);
    await expectCurrent(two);
    await one.close();
    await two.close();
    for (const store of stores) store?.close();
    expect(await nativeVersion(indexedDB, name)).toBe(CURRENT_SCHEMA);
  });

  it("refuses a damaged time-zone mark as corruption", async () => {
    const malformed: Array<[string, (columns: Array<Record<string, unknown>>) => void]> = [
      ["a string", (columns) => Object.assign(columns[1] ?? {}, { withoutTimeZone: "yes" })],
      ["false", (columns) => Object.assign(columns[1] ?? {}, { withoutTimeZone: false })],
      ["a TEXT column", (columns) => Object.assign(columns[3] ?? {}, { withoutTimeZone: true })],
    ];
    for (const [label, damage] of malformed) {
      const indexedDB = new IDBFactory();
      const name = crypto.randomUUID();
      await releasedIndexedDb(indexedDB, name);
      const store = await IndexedDbBlockStore.open({ name, indexedDB });
      const id = (await store.getTableByName("events"))?.id;
      if (id === undefined) throw new Error("The released table is missing");
      const record = await rawTableRecord(indexedDB, name, id);
      damage(record.columns as Array<Record<string, unknown>>);
      await rawCatalog(indexedDB, name, (catalog) => catalog.put(record, `table/id/${id}`));
      // An open connection refuses to read it, and a new one refuses to open.
      await expect(store.getTable(id), label).rejects.toBeInstanceOf(StorageCorruptionError);
      await expect(store.listTables(), label).rejects.toBeInstanceOf(StorageCorruptionError);
      expect((await store.checkIntegrity({ mode: "full" })).ok, label).toBe(false);
      store.close();
      await expect(IndexedDbBlockStore.open({ name, indexedDB }), label).rejects.toBeInstanceOf(
        StorageCorruptionError,
      );
    }
  });
});

describe("OPFS: the 0.16.0 writer's datetimes after the layout 13 upgrade", () => {
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

  it("keeps its columns as instants, declares TIMESTAMP columns, reopens, and refuses the released reader unchanged", async () => {
    const shim = new MemoryOpfs();
    const rows = await releasedOpfs(shim);

    let store: BlockStore & { _crashForTests(): void } = await OpfsBlockStore.open({
      name: NAME,
      root: shim.root,
    });
    expect(marker(shim)).toBe(`{"formatVersion":${String(CURRENT_LAYOUT)}}`);
    let database = new MinnowDatabase(store, OPTIONS);
    await expectHistory(database, rows);
    await writeCurrent(database);
    await database.close();
    store._crashForTests();

    // Replayed from the log; a graceful close checkpoints; then loaded from that checkpoint.
    for (const step of ["replay", "checkpoint"] as const) {
      store = await OpfsBlockStore.open({ name: NAME, root: shim.root });
      database = new MinnowDatabase(store, OPTIONS);
      await expectCurrent(database);
      expect(await store.checkIntegrity?.({ mode: "full" })).toMatchObject({ ok: true });
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
    await expectCurrent(database);
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
      await writeCurrent(first);
      await expectHistory(second, rows);
      await expectCurrent(second);
      await first.close();
      await second.close();
    } finally {
      for (const store of stores) store._crashForTests();
    }
  });
});
