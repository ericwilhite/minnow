/**
 * Databases a released build left unable to fold. Through 0.15.0 a fold refused any table that
 * had gained a column after rows were written (its older segments have no blocks for it) or
 * lost one that an update had changed (the update keeps only its key), so every commit added a
 * level-zero segment until the 4,096-segment ceiling refused writes to the table.
 *
 * The fold now fills an absent column with what reads give it — the column's backfill, or NULL —
 * and retires inert updates. Its plans can describe such sources, which a 0.15.0 reader refuses
 * as corruption, so IndexedDB schema 7 and OPFS layout 12 bar that reader; the upgrade moves
 * only the version marker. These tests take the real 0.15.0 writer's damaged databases through
 * the ordinary open API: the table folds, writes resume — at the ceiling, on the first write —
 * the data reads the same, and the released reader is refused afterwards. They also cut the
 * IndexedDB upgrade mid-way and retry, race current and released openers, and refuse plans whose
 * fills do not match their sources.
 */
import { IDBFactory } from "fake-indexeddb";
import { describe, expect, it, vi } from "vitest";
import { MinnowDatabase as ReleasedDatabase } from "@minnowdb/core-layout10";
import { IndexedDbBlockStore as ReleasedIndexedDbStore } from "@minnowdb/core-layout10/storage/indexeddb";
import { OpfsBlockStore as ReleasedOpfsStore } from "@minnowdb/core-layout10/storage/opfs";
import {
  CompactionBacklogError,
  IndexedDbBlockStore,
  MemoryBlockStore,
  OpfsBlockStore,
  type BlockStore,
  type CompactionJobRecord,
} from "../storage/index.js";
import { MemoryOpfs } from "../testing/opfs-shim.js";
import { MinnowDatabase } from "./database.js";
import { allVisibleSegments, heavyTestTimeout } from "./storage-test-helpers.js";

vi.setConfig({ testTimeout: heavyTestTimeout(180_000) });

const NAME = "schema-change-folds";
const MAX_LEVEL_ZERO_SEGMENTS = 4_096;
/**
 * What this build writes. Schema 6 and layout 11 admitted these folds but were never released;
 * schema 7 and layout 12 also admit delta merges.
 */
const CURRENT_SCHEMA = 7;
const CURRENT_LAYOUT = 12;
const CURRENT_MARKER = `{"formatVersion":${String(CURRENT_LAYOUT)}}`;

interface Writer {
  execute(sql: string, params?: Array<number | string | boolean>): Promise<unknown>;
  compactTable(table: string, options?: { minimumLevel0Segments?: number }): Promise<unknown>;
  close(): Promise<void>;
}

/**
 * The history that left 0.15.0 unable to fold: an update to a column that is then dropped, a
 * column added to the populated table, and `commits` single-row inserts. Returns the next id, or
 * the id the ceiling refused.
 */
async function damagedHistory(writer: Writer, commits: number): Promise<number> {
  await writer.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, c INTEGER, c0 INTEGER)");
  await writer.execute("INSERT INTO t VALUES (1, 1, 1), (2, 2, 2)");
  await writer.execute("UPDATE t SET c0 = 9 WHERE id = 1");
  await writer.execute("ALTER TABLE t ADD COLUMN a BOOLEAN");
  await writer.execute("ALTER TABLE t ADD COLUMN s TEXT NOT NULL DEFAULT 'west'");
  await writer.execute("ALTER TABLE t DROP COLUMN c0");
  let id = 3;
  for (; id < 3 + commits; id += 1) {
    try {
      await writer.execute("INSERT INTO t VALUES (?, ?, ?, ?)", [
        id,
        id,
        id % 2 === 0,
        `r${String(id)}`,
      ]);
    } catch (error) {
      expect(error).toMatchObject({ name: "CompactionBacklogError" });
      break;
    }
  }
  return id;
}

/** What the damaged history reads as, for the ids written. */
function expectedRows(nextId: number): Array<Record<string, unknown>> {
  return [
    { id: 1, c: 1, a: null, s: "west" },
    { id: 2, c: 2, a: null, s: "west" },
    ...Array.from({ length: nextId - 3 }, (_, index) => {
      const id = index + 3;
      return { id, c: id, a: id % 2 === 0, s: `r${String(id)}` };
    }),
  ];
}

async function readAll(database: MinnowDatabase): Promise<unknown[]> {
  return (await database.query("SELECT * FROM t ORDER BY id", { memoize: false })).rows;
}

async function levelZeroCount(
  database: MinnowDatabase,
  store: BlockStore,
  table = "t",
): Promise<number> {
  let count = 0;
  for (const segment of await allVisibleSegments(database, table)) {
    if ((await store.getSegment(segment.id))?.level === 0) count += 1;
  }
  return count;
}

/** Waits for background maintenance to bring the table under the automatic fold threshold. */
async function settle(database: MinnowDatabase, store: BlockStore): Promise<number> {
  let levelZero = Number.POSITIVE_INFINITY;
  for (let attempt = 0; attempt < 600; attempt += 1) {
    const active = (await database.listCompactionJobs()).some(
      (job) => job.state !== "published" && job.state !== "cancelled" && job.state !== "aborted",
    );
    levelZero = await levelZeroCount(database, store);
    if (!active && levelZero < 48) return levelZero;
    // A read of the fragmented table is what schedules a fold when nothing writes.
    await database.query("SELECT COUNT(*) AS n FROM t", { memoize: false });
    await new Promise((resolve) => setTimeout(resolve, 25));
  }
  throw new Error(`Background folds did not settle: ${String(levelZero)} level-zero segments`);
}

describe("OPFS: a table 0.15.0 left at the level-zero ceiling", () => {
  it("folds on the first write after the upgrade, and the released reader is refused", async () => {
    const shim = new MemoryOpfs();
    const releasedStore = await ReleasedOpfsStore.open({ name: NAME, root: shim.root });
    const released = new ReleasedDatabase(releasedStore, { autoCompact: false });
    const nextId = await damagedHistory(released, MAX_LEVEL_ZERO_SEGMENTS + 16);
    // The released build is stuck: the table is at the ceiling, and its fold refuses the sources.
    expect(nextId).toBeLessThan(3 + MAX_LEVEL_ZERO_SEGMENTS + 16);
    await expect(released.compactTable("t", { minimumLevel0Segments: 1 })).rejects.toThrow(
      "Full-row compaction source is missing columns",
    );
    await released.close();
    releasedStore.close();
    const marker = () =>
      new TextDecoder().decode(shim.readFileBytes(`minnowdb/${NAME}/format.json`));
    expect(marker()).toBe('{"formatVersion":10}');

    const errors: unknown[] = [];
    let store = await OpfsBlockStore.open({ name: NAME, root: shim.root });
    expect(marker()).toBe(CURRENT_MARKER);
    let database = new MinnowDatabase(store, { onBackgroundError: (error) => errors.push(error) });
    expect(await levelZeroCount(database, store)).toBeGreaterThanOrEqual(MAX_LEVEL_ZERO_SEGMENTS);
    // The write at the ceiling lends its turn to a fold, which now publishes.
    await database.execute("INSERT INTO t VALUES (?, ?, ?, ?)", [nextId, nextId, false, "next"]);
    const rows = [...expectedRows(nextId), { id: nextId, c: nextId, a: false, s: "next" }];
    expect(await readAll(database)).toEqual(rows);
    expect(await settle(database, store)).toBeLessThan(48);
    expect(errors).toEqual([]);
    expect(await readAll(database)).toEqual(rows);
    await database.close();
    store._crashForTests();

    store = await OpfsBlockStore.open({ name: NAME, root: shim.root });
    database = new MinnowDatabase(store, { autoCompact: false, autoCollect: false });
    expect(await readAll(database)).toEqual(rows);
    expect(await store.checkIntegrity({ mode: "full" })).toMatchObject({ ok: true });
    await database.close();
    store._crashForTests();

    await expect(ReleasedOpfsStore.open({ name: NAME, root: shim.root })).rejects.toMatchObject({
      name: "StorageFormatVersionError",
      actualVersion: CURRENT_LAYOUT,
      supportedVersion: 10,
      relation: "newer",
    });
    expect(marker()).toBe(CURRENT_MARKER);
  });
});

describe("IndexedDB: a table 0.15.0 could not fold", () => {
  const commits = 300;

  async function releasedDatabase(indexedDB: IDBFactory, name: string): Promise<number> {
    const store = await ReleasedIndexedDbStore.open({ name, indexedDB });
    const database = new ReleasedDatabase(store, { autoCompact: false });
    const nextId = await damagedHistory(database, commits);
    await expect(database.compactTable("t", { minimumLevel0Segments: 1 })).rejects.toThrow(
      "Full-row compaction source is missing columns",
    );
    await database.close();
    store.close();
    return nextId;
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

  it("folds in the background after the upgrade, writes on, and refuses the released reader", async () => {
    const indexedDB = new IDBFactory();
    const name = crypto.randomUUID();
    const nextId = await releasedDatabase(indexedDB, name);
    expect(await nativeVersion(indexedDB, name)).toBe(5);

    const errors: unknown[] = [];
    let store = await IndexedDbBlockStore.open({ name, indexedDB });
    expect(await nativeVersion(indexedDB, name)).toBe(CURRENT_SCHEMA);
    let database = new MinnowDatabase(store, { onBackgroundError: (error) => errors.push(error) });
    expect(await readAll(database)).toEqual(expectedRows(nextId));
    expect(await settle(database, store)).toBeLessThan(48);
    await database.execute("INSERT INTO t VALUES (?, ?, ?, ?)", [nextId, nextId, true, "after"]);
    await database.execute("UPDATE t SET a = TRUE, s = 'east' WHERE id = 2");
    const rows = [
      ...expectedRows(nextId).map((row) => (row.id === 2 ? { ...row, a: true, s: "east" } : row)),
      { id: nextId, c: nextId, a: true, s: "after" },
    ];
    expect(await readAll(database)).toEqual(rows);
    expect(errors).toEqual([]);
    await database.close();
    store.close();

    store = await IndexedDbBlockStore.open({ name, indexedDB });
    database = new MinnowDatabase(store, { autoCompact: false, autoCollect: false });
    expect(await readAll(database)).toEqual(rows);
    expect((await store.checkIntegrity({ mode: "full" })).issues).toEqual([]);
    await database.close();
    store.close();

    await expect(ReleasedIndexedDbStore.open({ name, indexedDB })).rejects.toMatchObject({
      name: "StorageFormatVersionError",
      actualVersion: CURRENT_SCHEMA,
      supportedVersion: 5,
      relation: "newer",
    });
    expect(await nativeVersion(indexedDB, name)).toBe(CURRENT_SCHEMA);
  });

  it("leaves schema 5 for the released writer when the upgrade is interrupted, then retries", async () => {
    const indexedDB = new IDBFactory();
    const name = crypto.randomUUID();
    const nextId = await releasedDatabase(indexedDB, name);
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
    expect(await nativeVersion(indexedDB, name)).toBe(5);
    // Still the released writer's database, readable and writable by it.
    const releasedStore = await ReleasedIndexedDbStore.open({ name, indexedDB });
    const released = new ReleasedDatabase(releasedStore, { autoCompact: false });
    await released.execute("INSERT INTO t VALUES (?, ?, ?, ?)", [nextId, nextId, true, "late"]);
    await released.close();
    releasedStore.close();

    const store = await IndexedDbBlockStore.open({ name, indexedDB });
    expect(await nativeVersion(indexedDB, name)).toBe(CURRENT_SCHEMA);
    const database = new MinnowDatabase(store, { autoCompact: false, autoCollect: false });
    const rows = [...expectedRows(nextId), { id: nextId, c: nextId, a: true, s: "late" }];
    expect(await readAll(database)).toEqual(rows);
    expect((await database.compactTable("t", { minimumLevel0Segments: 1 })).compacted).toBe(true);
    expect(await readAll(database)).toEqual(rows);
    await database.close();
    store.close();
  });

  it("lets concurrent current openers share one upgrade and closes or refuses released ones", async () => {
    const indexedDB = new IDBFactory();
    const name = crypto.randomUUID();
    const nextId = await releasedDatabase(indexedDB, name);
    const stale = await ReleasedIndexedDbStore.open({ name, indexedDB });
    const [first, second, late] = await Promise.allSettled([
      IndexedDbBlockStore.open({ name, indexedDB }),
      IndexedDbBlockStore.open({ name, indexedDB }),
      ReleasedIndexedDbStore.open({ name, indexedDB }),
    ]);
    expect(first.status).toBe("fulfilled");
    expect(second.status).toBe("fulfilled");
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
      store === undefined
        ? undefined
        : new MinnowDatabase(store, { autoCompact: false, autoCollect: false }),
    );
    if (one === undefined || two === undefined) throw new Error("A current opener failed");
    expect((await one.compactTable("t", { minimumLevel0Segments: 1 })).compacted).toBe(true);
    expect(await readAll(two)).toEqual(expectedRows(nextId));
    await one.close();
    await two.close();
    for (const store of stores) store?.close();
  });
});

describe("a store at the level-zero ceiling with folds that used to be refused", () => {
  it("recovers on the first write once folds run again", async () => {
    const store = new MemoryBlockStore();
    // Folding disabled, so the table reaches the ceiling as it did when every fold failed.
    const stuck = new MinnowDatabase(store, { autoCompact: false });
    const nextId = await damagedHistory(stuck, MAX_LEVEL_ZERO_SEGMENTS + 16);
    await expect(
      stuck.execute("INSERT INTO t VALUES (?, ?, ?, ?)", [nextId, nextId, true, "x"]),
    ).rejects.toBeInstanceOf(CompactionBacklogError);
    await stuck.close();

    const errors: unknown[] = [];
    const database = new MinnowDatabase(store, {
      onBackgroundError: (error) => errors.push(error),
    });
    await database.execute("INSERT INTO t VALUES (?, ?, ?, ?)", [nextId, nextId, true, "x"]);
    const rows = [...expectedRows(nextId), { id: nextId, c: nextId, a: true, s: "x" }];
    expect(await readAll(database)).toEqual(rows);
    expect(await settle(database, store)).toBeLessThan(48);
    expect(await readAll(database)).toEqual(rows);
    expect(errors).toEqual([]);
    await database.close();
  });
});

describe("plans whose fills do not match their sources", () => {
  /** Captures the job a fold plans, without running it. */
  async function plannedJob(
    store: MemoryBlockStore,
    database: MinnowDatabase,
    table: string,
  ): Promise<CompactionJobRecord> {
    const create = store.createCompactionJob.bind(store);
    let planned: CompactionJobRecord | undefined;
    store.createCompactionJob = (record) => {
      planned = structuredClone(record);
      return Promise.reject(new Error("captured"));
    };
    try {
      await expect(database.compactTableStep(table, { minimumLevel0Segments: 1 })).rejects.toThrow(
        "captured",
      );
    } finally {
      store.createCompactionJob = create;
    }
    if (planned === undefined) throw new Error("No fold was planned");
    return planned;
  }

  async function run(database: MinnowDatabase, jobId: string) {
    let progress = await database.resumeCompactionJob(jobId, { maxBlocks: 64 });
    while (progress.result === null) {
      progress = await database.resumeCompactionJob(jobId, { maxBlocks: 64 });
    }
    return progress.result;
  }

  it("refuses a merge plan that leaves out a column its source has", async () => {
    const store = new MemoryBlockStore();
    const database = new MinnowDatabase(store, { autoCompact: false, autoCollect: false });
    await database.execute("CREATE TABLE k (id INTEGER PRIMARY KEY, v TEXT)");
    await database.execute("INSERT INTO k VALUES (1, 'one'), (2, 'two')");
    await database.execute("ALTER TABLE k ADD COLUMN w INTEGER");
    await database.execute("INSERT INTO k VALUES (3, 'three', 3)");
    const planned = await plannedJob(store, database, "k");
    if (planned.rewritePlan.kind !== "merge-v2") throw new Error("Expected a replayed merge");
    // The newer source has `w`; a plan claiming it does not would fill its value with NULL.
    const forged = structuredClone(planned) as CompactionJobRecord & {
      rewritePlan: typeof planned.rewritePlan;
    };
    const later = forged.rewritePlan.sourceSegments.at(-1);
    if (later?.columns.length !== 3) throw new Error("Expected 3 columns");
    const dropped = later.columns[2];
    (later as { columns: readonly unknown[] }).columns = later.columns.slice(0, 2);
    const id = `${planned.id}/forged`;
    const droppedStoredBytes =
      dropped?.sourceBlocks.reduce((sum, block) => sum + block.storedBytes, 0) ?? 0;
    await store.createCompactionJob({
      ...forged,
      id,
      outputSegmentId: `${id}/output`,
      sourceBlockIds: forged.sourceBlockIds.filter(
        (blockId) => !dropped?.sourceBlocks.some((block) => block.blockId === blockId),
      ),
      sourceStoredBytes: forged.sourceStoredBytes - droppedStoredBytes,
      level0SourceStoredBytes: forged.level0SourceStoredBytes - droppedStoredBytes,
      logicalBytes:
        forged.logicalBytes -
        (dropped?.sourceBlocks.reduce((sum, block) => sum + block.encodedBytes, 0) ?? 0),
    });
    await expect(run(database, id)).rejects.toThrow("differs from its plan");
    expect((await store.getCompactionJob(id))?.state).not.toBe("published");
    expect((await database.query("SELECT w FROM k ORDER BY id")).rows).toEqual([
      { w: null },
      { w: null },
      { w: 3 },
    ]);

    // A fold planned from the sources themselves runs.
    await database.compactTable("k", { minimumLevel0Segments: 1 });
    expect(await levelZeroCount(database, store, "k")).toBe(0);
    expect((await database.query("SELECT * FROM k ORDER BY id")).rows).toEqual([
      { id: 1, v: "one", w: null },
      { id: 2, v: "two", w: null },
      { id: 3, v: "three", w: 3 },
    ]);
    await database.close();
  });

  it("refuses a rechunk plan whose gap is not where the column was missing", async () => {
    const store = new MemoryBlockStore();
    const database = new MinnowDatabase(store, { autoCompact: false, autoCollect: false });
    await database.execute("CREATE TABLE logs (seq INTEGER, msg TEXT)");
    await database.execute("INSERT INTO logs VALUES (1, 'a'), (2, 'b')");
    await database.execute("ALTER TABLE logs ADD COLUMN host TEXT DEFAULT 'h'");
    await database.execute("INSERT INTO logs VALUES (3, 'c', 'h3'), (4, 'd', 'h4')");
    const planned = await plannedJob(store, database, "logs");
    if (planned.rewritePlan.kind !== "rechunk-v1") throw new Error("Expected a rechunk");
    const host = planned.rewritePlan.columns[2];
    expect(host?.sourceBlocks.map((block) => block.rowStart)).toEqual([2]);
    // The newer rows' host values moved over the older rows: they would read h3, h4 there.
    const forged = structuredClone(planned);
    if (forged.rewritePlan.kind !== "rechunk-v1") throw new Error("Expected a rechunk");
    const forgedHost = forged.rewritePlan.columns[2];
    (forgedHost?.sourceBlocks[0] as { rowStart: number }).rowStart = 0;
    const id = `${planned.id}/forged`;
    await store.createCompactionJob({ ...forged, id, outputSegmentId: `${id}/output` });
    await expect(run(database, id)).rejects.toThrow(/^Compaction source/);
    expect((await store.getCompactionJob(id))?.state).not.toBe("published");
    expect((await database.query("SELECT host FROM logs ORDER BY seq")).rows).toEqual([
      { host: "h" },
      { host: "h" },
      { host: "h3" },
      { host: "h4" },
    ]);

    await database.compactTable("logs", { minimumLevel0Segments: 1 });
    expect(await levelZeroCount(database, store, "logs")).toBe(0);
    expect((await database.query("SELECT * FROM logs ORDER BY seq")).rows).toEqual([
      { seq: 1, msg: "a", host: "h" },
      { seq: 2, msg: "b", host: "h" },
      { seq: 3, msg: "c", host: "h3" },
      { seq: 4, msg: "d", host: "h4" },
    ]);
    await database.close();
  });
});
