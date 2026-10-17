/**
 * Full-text indexes across folds and invalidations, on every store.
 *
 * A fold of an append-only table (a copy or a rechunk) keeps every row, its value, and its row
 * ID, so the table's postings stay exact. Through 0.16.0 the fold's commit carried no coverage
 * for the index, and the store's stale-writer rule took it for a writer that never saw the
 * index: every fold flipped the index to "invalid" (searches scanned until a full rebuild) and
 * left the invalidated postings stored. IndexedDB's integrity check then reported them as
 * "postings table of contents has no catalog owner". Any other invalidation -- a keyed mutation,
 * a keyed fold -- left them too, and an index that was never rebuilt kept them until its table
 * or column was dropped.
 *
 * Now a fold covers the indexes it preserves, the integrity check accepts postings an
 * invalidated index still declares, and garbage collection reclaims them. Databases the released
 * 0.15.0 writer left that way are repaired by the current build's first collection. The same
 * check also reported every UNIQUE column of CREATE TABLE until a query built its index a base,
 * though reads treat that state as unbuilt and build one.
 */
import { IDBFactory } from "fake-indexeddb";
import { describe, expect, it, vi } from "vitest";
import { MinnowDatabase as ReleasedDatabase } from "@minnowdb/core-layout10";
import { IndexedDbBlockStore as ReleasedIndexedDbStore } from "@minnowdb/core-layout10/storage/indexeddb";
import { OpfsBlockStore as ReleasedOpfsStore } from "@minnowdb/core-layout10/storage/opfs";
import {
  IndexedDbBlockStore,
  MemoryBlockStore,
  OpfsBlockStore,
  type BlockStore,
  type StorageIntegrityIssue,
  type TableRecord,
} from "../storage/index.js";
import { readRawKeys, readRawValue } from "../storage/indexeddb-audit-helpers.js";
import { MemoryOpfs } from "../testing/opfs-shim.js";
import { MinnowDatabase } from "./database.js";
import { heavyTestTimeout } from "./storage-test-helpers.js";

vi.setConfig({ testTimeout: heavyTestTimeout(60_000) });

const OPTIONS = { autoCompact: false, autoCollect: false } as const;
const WORDS = ["amber", "birch", "cedar", "delta", "ember"];
const range = (start: number, end: number) =>
  Array.from({ length: end - start }, (_, index) => start + index);
const note = (index: number) => ({ body: `entry ${WORDS[index % 5] ?? ""} n${String(index)}` });

interface Harness {
  readonly name: string;
  open(): Promise<BlockStore>;
  /** Raw IndexedDB access, for the store whose catalog is inspected key by key. */
  readonly indexedDB?: { factory: IDBFactory; name: string };
}

/** Each call is a new, empty database on that store; `open` reopens the same one. */
const harnesses: Record<string, () => Harness> = {
  memory: () => {
    const memory = new MemoryBlockStore();
    return { name: "memory", open: async () => memory };
  },
  indexeddb: () => {
    const indexedDB = new IDBFactory();
    const name = crypto.randomUUID();
    return {
      name: "indexeddb",
      open: () => IndexedDbBlockStore.open({ name, indexedDB }),
      indexedDB: { factory: indexedDB, name },
    };
  },
  opfs: () => {
    const shim = new MemoryOpfs();
    return { name: "opfs", open: () => OpfsBlockStore.open({ name: "fts-fold", root: shim.root }) };
  },
};

async function close(database: MinnowDatabase, store: BlockStore): Promise<void> {
  await database.close();
  if (!(store instanceof MemoryBlockStore)) store.close();
}

async function notesTable(store: BlockStore): Promise<TableRecord> {
  const table = await store.getTableByName("notes");
  if (table === undefined) throw new Error("notes is missing");
  return table;
}

async function indexState(store: BlockStore, column = "body"): Promise<string | undefined> {
  const table = await notesTable(store);
  const id = table.columns.find((candidate) => candidate.name === column)?.id ?? "";
  return table.ftsColumns?.[id]?.state;
}

/** What the store holds for the column's index: base and delta postings, and one term's rows. */
async function stored(store: BlockStore, term = "cedar") {
  const table = await notesTable(store);
  const id = table.columns.find((candidate) => candidate.name === "body")?.id ?? "";
  const version = (await store.getCurrentManifestVersion()) ?? -1;
  const read = await store.readFtsCandidates(table.id, id, [{ term, prefix: false }], version);
  return {
    hasBase: read.hasBase,
    deltas: read.deltaChunkCount,
    rows: read.rowIdsByTerm[0]?.length ?? 0,
  };
}

async function integrity(store: BlockStore): Promise<readonly StorageIntegrityIssue[]> {
  return (await store.checkIntegrity?.({ mode: "full" }))?.issues ?? [];
}

/** Matches, prefix matches, and BM25 scores over the notes table. */
async function answers(database: Pick<MinnowDatabase, "query">): Promise<unknown[]> {
  const out: unknown[] = [];
  for (const query of ["cedar", "n12", "n450", "n9999", "amb*", "birch n3*"]) {
    out.push(
      (
        await database.query(
          `SELECT body FROM notes WHERE MATCH(body) AGAINST '${query}' ORDER BY body`,
          { memoize: false },
        )
      ).rows,
    );
  }
  out.push(
    (
      await database.query(
        "SELECT body, BM25(body) AGAINST 'cedar n1*' AS score FROM notes " +
          "WHERE MATCH(body) AGAINST 'cedar n1*' ORDER BY score DESC, body",
        { memoize: false },
      )
    ).rows,
  );
  return out;
}

/** The same answers from an index built once over the same rows in a fresh database. */
async function rebuilt(rows: number): Promise<unknown[]> {
  const database = new MinnowDatabase(new MemoryBlockStore(), OPTIONS);
  await database.createTable({ name: "notes", columns: [{ name: "body", type: "string" }] });
  await database.insertBatch("notes", range(0, rows).map(note));
  await database.buildFtsIndex("notes", "body");
  const result = await answers(database);
  await database.close();
  return result;
}

for (const [kind, create] of Object.entries(harnesses)) {
  describe(`${kind}: full-text indexes across folds`, () => {
    it("a fold of an append-only table keeps its index exact and ready", async () => {
      const harness = create();
      let store = await harness.open();
      let database = new MinnowDatabase(store, OPTIONS);
      await database.createTable({ name: "notes", columns: [{ name: "body", type: "string" }] });
      await database.insertBatch("notes", range(0, 300).map(note));
      await database.buildFtsIndex("notes", "body");
      await database.insertBatch("notes", range(300, 500).map(note));
      await database.insertBatch("notes", range(500, 600).map(note));
      const expected = await rebuilt(600);
      expect(await answers(database)).toEqual(expected);

      const fold = await database.compactTable("notes", { minimumLevel0Segments: 1 });
      expect(fold.compacted).toBe(true);
      expect(await indexState(store)).toBe("ready");
      // The postings still name every cedar row, by the row IDs the fold kept.
      expect(await stored(store)).toEqual({ hasBase: true, deltas: 2, rows: 120 });
      expect(await answers(database)).toEqual(expected);
      expect(await integrity(store)).toEqual([]);

      // Inserts after the fold land as deltas on the same index.
      await database.insertBatch("notes", range(600, 700).map(note));
      expect(await indexState(store)).toBe("ready");
      expect(await stored(store)).toEqual({ hasBase: true, deltas: 3, rows: 140 });
      const grown = await rebuilt(700);
      expect(await answers(database)).toEqual(grown);

      await database.collectGarbage();
      expect(await answers(database)).toEqual(grown);
      expect(await integrity(store)).toEqual([]);
      await close(database, store);

      store = await harness.open();
      database = new MinnowDatabase(store, OPTIONS);
      expect(await indexState(store)).toBe("ready");
      expect(await answers(database)).toEqual(grown);
      expect(await integrity(store)).toEqual([]);
      // A rebuild over the folded table (a delta fold does one) still reads it as append-only.
      await database.buildFtsIndex("notes", "body");
      expect(await stored(store)).toEqual({ hasBase: true, deltas: 0, rows: 140 });
      expect(await answers(database)).toEqual(grown);
      await close(database, store);
    });

    for (const invalidation of ["keyed mutation", "keyed fold"] as const) {
      it(`collection reclaims the postings a ${invalidation} invalidates`, async () => {
        const harness = create();
        let store = await harness.open();
        let database = new MinnowDatabase(store, OPTIONS);
        await database.createTable({
          name: "notes",
          uniqueKey: "slug",
          columns: [
            { name: "slug", type: "string" },
            { name: "body", type: "string" },
          ],
        });
        await database.insertBatch("notes", [{ slug: "a", body: "quick fox" }]);
        await database.insertBatch("notes", [{ slug: "b", body: "lazy dog" }]);
        await database.buildFtsIndex("notes", "body");
        await database.insertBatch("notes", [{ slug: "c", body: "fox den" }]);
        if (invalidation === "keyed mutation") {
          await database.upsertBatch("notes", [{ slug: "a", body: "silent owl" }]);
        } else {
          await database.compactTable("notes", { minimumLevel0Segments: 1 });
        }
        // A merge writes base segments no full-text index can serve, and an upsert carries no
        // postings: both invalidate, keeping the data commit and, until collection, the postings.
        expect(await indexState(store)).toBe("invalid");
        expect(await stored(store, "fox")).toMatchObject({ hasBase: true, deltas: 1 });
        expect(await integrity(store)).toEqual([]);
        const fox = invalidation === "keyed mutation" ? ["c"] : ["a", "c"];
        const sql = "SELECT slug FROM notes WHERE MATCH(body) AGAINST 'fox' ORDER BY slug";
        expect((await database.query(sql)).rows.map((row) => row.slug)).toEqual(fox);

        await database.collectGarbage();
        expect(await stored(store, "fox")).toEqual({ hasBase: false, deltas: 0, rows: 0 });
        expect(await indexState(store)).toBe("invalid");
        expect((await database.query(sql)).rows.map((row) => row.slug)).toEqual(fox);
        expect(await integrity(store)).toEqual([]);
        if (harness.indexedDB !== undefined) {
          const { factory, name } = harness.indexedDB;
          const keys = (await readRawKeys(factory, name, "catalog")).filter(
            (key) => typeof key === "string" && key.startsWith("fts-"),
          );
          // Every chunk is gone, and no retirement is left pending.
          expect(keys).toEqual([]);
        }
        // Nothing reappears for another pass or connection to find.
        await database.collectGarbage();
        await close(database, store);
        store = await harness.open();
        database = new MinnowDatabase(store, OPTIONS);
        expect(await stored(store, "fox")).toEqual({ hasBase: false, deltas: 0, rows: 0 });
        expect((await database.query(sql)).rows.map((row) => row.slug)).toEqual(fox);
        await close(database, store);
      });
    }
  });
}

async function putRaw(
  indexedDB: IDBFactory,
  name: string,
  edit: (catalog: IDBObjectStore) => void,
): Promise<void> {
  await new Promise<void>((resolve, reject) => {
    const request = indexedDB.open(name);
    request.onerror = () => reject(request.error ?? new Error("raw open failed"));
    request.onsuccess = () => {
      const raw = request.result;
      const transaction = raw.transaction("catalog", "readwrite");
      edit(transaction.objectStore("catalog"));
      transaction.oncomplete = () => {
        raw.close();
        resolve();
      };
      transaction.onabort = () => reject(transaction.error ?? new Error("raw edit aborted"));
    };
  });
}

describe("IndexedDB integrity of postings", () => {
  it("accepts a UNIQUE column's index before its first build, and checks a built base", async () => {
    const indexedDB = new IDBFactory();
    const name = crypto.randomUUID();
    const store = await IndexedDbBlockStore.open({ name, indexedDB });
    const database = new MinnowDatabase(store, OPTIONS);
    // Declared with its table, the index is ready with no base: a read scans and schedules the
    // build that writes one.
    await database.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, u TEXT UNIQUE, v INTEGER)");
    expect(await integrity(store)).toEqual([]);
    await database.execute("INSERT INTO t VALUES (1, 'a', 10), (2, 'b', 20), (3, 'c', 30)");
    expect(await integrity(store)).toEqual([]);
    // An index CREATE INDEX builds has a base from the start.
    await database.execute("CREATE INDEX t_v ON t (v)");
    expect(await integrity(store)).toEqual([]);
    const table = await store.getTableByName("t");
    const built = Object.values(table?.secondaryIndexes ?? {}).find(
      (index) => index.name === "t_v",
    );
    const toc = `fts-base-index/${table?.id ?? ""}/${built?.storageColumnId ?? ""}`;
    expect(await readRawValue(indexedDB, name, "catalog", toc)).toBeDefined();
    // A built base that loses its table of contents leaves chunks nothing owns.
    await putRaw(indexedDB, name, (catalog) => catalog.delete(toc));
    const issues = await integrity(store);
    expect(issues).toHaveLength(1);
    expect(issues[0]?.message).toContain("postings base chunk has no table of contents");
    await close(database, store);
  });

  it("still reports postings no index declares as orphaned", async () => {
    const indexedDB = new IDBFactory();
    const name = crypto.randomUUID();
    const store = await IndexedDbBlockStore.open({ name, indexedDB });
    const database = new MinnowDatabase(store, OPTIONS);
    await database.createTable({ name: "notes", columns: [{ name: "body", type: "string" }] });
    await database.insertBatch("notes", range(0, 50).map(note));
    await database.buildFtsIndex("notes", "body");
    const table = await notesTable(store);
    const column = table.columns[0]?.id ?? "";
    const toc = await readRawValue(
      indexedDB,
      name,
      "catalog",
      `fts-base-index/${table.id}/${column}`,
    );
    expect(toc).toBeDefined();
    const stray = `fts-base-index/${table.id}/${crypto.randomUUID()}`;
    await putRaw(indexedDB, name, (catalog) => catalog.put(toc, stray));
    const issues = await integrity(store);
    expect(issues).toHaveLength(1);
    expect(issues[0]?.location).toBe(stray);
    expect(issues[0]?.message).toContain("postings table of contents has no catalog owner");
    await close(database, store);
  });
});

interface ReleasedWriter {
  createTable(definition: unknown): Promise<unknown>;
  insertBatch(table: string, rows: Array<Record<string, string>>): Promise<unknown>;
  buildFtsIndex(table: string, column: string): Promise<void>;
  compactTable(table: string, options?: { minimumLevel0Segments?: number }): Promise<unknown>;
  close(): Promise<void>;
}

/** One database the released 0.15.0 writer opens first and the current build after it. */
interface ReleasedSource {
  readonly name: string;
  released(): Promise<BlockStore>;
  current(): Promise<BlockStore>;
  readonly indexedDB?: { factory: IDBFactory; name: string };
}

const releasedIndexedDb = { factory: new IDBFactory(), name: crypto.randomUUID() };
const releasedOpfs = new MemoryOpfs();
const released: ReleasedSource[] = [
  {
    name: "IndexedDB",
    released: () =>
      ReleasedIndexedDbStore.open({
        name: releasedIndexedDb.name,
        indexedDB: releasedIndexedDb.factory,
      }),
    current: () =>
      IndexedDbBlockStore.open({
        name: releasedIndexedDb.name,
        indexedDB: releasedIndexedDb.factory,
      }),
    indexedDB: releasedIndexedDb,
  },
  {
    name: "OPFS",
    released: () => ReleasedOpfsStore.open({ name: "fts-fold-released", root: releasedOpfs.root }),
    current: () => OpfsBlockStore.open({ name: "fts-fold-released", root: releasedOpfs.root }),
  },
];

for (const source of released) {
  describe(`${source.name}: a full-text index 0.15.0 invalidated by a fold`, () => {
    it("is reclaimed by the first collection, rebuilds, and then survives folds", async () => {
      const releasedStore = await source.released();
      const writer = new ReleasedDatabase(releasedStore, OPTIONS) as ReleasedWriter;
      await writer.createTable({ name: "notes", columns: [{ name: "body", type: "string" }] });
      await writer.insertBatch("notes", range(0, 300).map(note));
      await writer.buildFtsIndex("notes", "body");
      await writer.insertBatch("notes", range(300, 500).map(note));
      await writer.compactTable("notes", { minimumLevel0Segments: 1 });
      // The released writer's fold invalidated the index and kept its postings.
      expect(await indexState(releasedStore)).toBe("invalid");
      expect(await stored(releasedStore)).toMatchObject({ hasBase: true, rows: 100 });
      await writer.close();
      releasedStore.close();

      let store = await source.current();
      let database = new MinnowDatabase(store, OPTIONS);
      const expected = await rebuilt(500);
      expect(await integrity(store)).toEqual([]);
      expect(await stored(store)).toMatchObject({ hasBase: true, rows: 100 });
      await database.collectGarbage();
      expect(await stored(store)).toEqual({ hasBase: false, deltas: 0, rows: 0 });
      expect(await integrity(store)).toEqual([]);
      if (source.indexedDB !== undefined) {
        const { factory, name } = source.indexedDB;
        const keys = (await readRawKeys(factory, name, "catalog")).filter(
          (key) => typeof key === "string" && key.startsWith("fts-"),
        );
        expect(keys).toEqual([]);
      }
      // Searches scan meanwhile, and the first one schedules the rebuild this joins.
      expect(await answers(database)).toEqual(expected);
      await database.buildFtsIndex("notes", "body");
      expect(await indexState(store)).toBe("ready");
      expect(await answers(database)).toEqual(expected);

      // The current build's folds keep the index.
      await database.insertBatch("notes", range(500, 600).map(note));
      await database.compactTable("notes", { minimumLevel0Segments: 1 });
      expect(await indexState(store)).toBe("ready");
      expect(await stored(store)).toEqual({ hasBase: true, deltas: 1, rows: 120 });
      const grown = await rebuilt(600);
      expect(await answers(database)).toEqual(grown);
      await close(database, store);

      store = await source.current();
      database = new MinnowDatabase(store, OPTIONS);
      expect(await indexState(store)).toBe("ready");
      expect(await answers(database)).toEqual(grown);
      expect(await integrity(store)).toEqual([]);
      await close(database, store);
    });
  });
}
