/**
 * IndexedDB schema 4: one commit's postings delta for one indexed column has no size limit. It is
 * stored as ordered parts — part 0 at the schema-3 key, later parts and a directory of each
 * part's term range under structured keys — and the delta index records each version's part
 * count. A schema-3 delta is a valid one-part delta, so the upgrade rewrites nothing; the version
 * is the barrier that keeps released readers, which would miss later parts, out.
 *
 * Every supported older writer (0.10.0 and 0.12.1 write schema 2, 0.13.1 schema 3) leaves base
 * and delta postings for a secondary index and a full-text index; the current build opens them
 * through the ordinary API, answers from them, writes deltas past the old 65,536-posting record
 * limit, and reopens, and the released reader is refused afterwards. The rest pins interrupted
 * upgrades, concurrent openers, crashes mid-commit, corruption refusal, folds, drops, snapshot
 * export, and the event-loop turns a large commit hands out.
 */
import fc from "fast-check";
import { IDBFactory } from "fake-indexeddb";
import { describe, expect, it, vi } from "vitest";
import { MinnowDatabase as Layout6Database } from "@minnowdb/core-layout6";
import { IndexedDbBlockStore as Layout6Store } from "@minnowdb/core-layout6/storage/indexeddb";
import { MinnowDatabase as Layout7Database } from "@minnowdb/core-layout7";
import { IndexedDbBlockStore as Layout7Store } from "@minnowdb/core-layout7/storage/indexeddb";
import { MinnowDatabase as Layout8Database } from "@minnowdb/core-layout8";
import { IndexedDbBlockStore as Layout8Store } from "@minnowdb/core-layout8/storage/indexeddb";
import { MinnowDatabase } from "../engine/database.js";
import { heavyTestTimeout } from "../engine/storage-test-helpers.js";
import {
  NOW,
  activeTransaction,
  instrumentFactory,
  openStore,
  readRawKeys,
  readRawValue,
} from "./indexeddb-audit-helpers.js";
import { IndexedDbBlockStore } from "./indexeddb.js";
import type { FtsPosting, FtsPostingQuery, ManifestSummary, TableRecord } from "./types.js";

vi.setConfig({ testTimeout: heavyTestTimeout(60_000) });

const CURRENT_SCHEMA = 4;
const PART = "fts-delta-part";
/** The per-part ceilings the store writes; a test that needs several parts exceeds them. */
const PART_POSTINGS = 8_192;
const PART_ROW_IDS = 65_536;

// ---------------------------------------------------------------------------------------------
// Raw access and fixtures
// ---------------------------------------------------------------------------------------------

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

async function mutate(
  indexedDB: IDBFactory,
  name: string,
  write: (catalog: IDBObjectStore) => void,
): Promise<void> {
  const database = await new Promise<IDBDatabase>((resolve, reject) => {
    const request = indexedDB.open(name);
    request.onsuccess = () => resolve(request.result);
    request.onerror = () => reject(request.error ?? new Error("raw open failed"));
  });
  try {
    await new Promise<void>((resolve, reject) => {
      const transaction = database.transaction("catalog", "readwrite");
      write(transaction.objectStore("catalog"));
      transaction.oncomplete = () => resolve();
      transaction.onabort = () => reject(transaction.error ?? new Error("raw mutation aborted"));
    });
  } finally {
    database.close();
  }
}

async function partKeys(indexedDB: IDBFactory, name: string, identity?: string) {
  return (await readRawKeys(indexedDB, name, "catalog")).filter(
    (key): key is [string, string, number, number | string] =>
      Array.isArray(key) && key[0] === PART && (identity === undefined || key[1] === identity),
  );
}

async function legacyDeltaKeys(indexedDB: IDBFactory, name: string): Promise<string[]> {
  return (await readRawKeys(indexedDB, name, "catalog")).filter(
    (key): key is string =>
      typeof key === "string" &&
      key.startsWith("fts-chunk/") &&
      !key.startsWith("fts-chunk/index/"),
  );
}

interface StoredPart {
  postings: FtsPosting[];
  totalTokens: number;
}

/** One version's delta as stored: its part records, in order. */
async function readRawParts(
  indexedDB: IDBFactory,
  name: string,
  identity: string,
  version: number,
): Promise<StoredPart[]> {
  const index = (await readRawValue(indexedDB, name, "catalog", `fts-chunk/index/${identity}`)) as {
    versions: number[];
    parts?: number[];
  };
  const position = index.versions.indexOf(version);
  const count = index.parts?.[position] ?? 1;
  const parts: StoredPart[] = [];
  for (let part = 0; part < count; part += 1) {
    const key =
      part === 0 ? `fts-chunk/${identity}/${String(version)}` : [PART, identity, version, part];
    parts.push((await readRawValue(indexedDB, name, "catalog", key)) as StoredPart);
  }
  return parts;
}

/** Concatenates parts, rejoining a term split across a boundary: the commit's input exactly. */
function rejoin(parts: readonly StoredPart[]): FtsPosting[] {
  const postings: FtsPosting[] = [];
  for (const part of parts) {
    for (const posting of part.postings) {
      const last = postings.at(-1);
      if (last?.term === posting.term) {
        last.rowIds.push(...posting.rowIds);
        last.tf.push(...posting.tf);
      } else {
        postings.push({ term: posting.term, rowIds: [...posting.rowIds], tf: [...posting.tf] });
      }
    }
  }
  return postings;
}

const ARTICLES: TableRecord = {
  managed: false,
  id: "articles",
  name: "articles",
  columns: [{ id: "body", name: "body", type: "string", nullable: false }],
  ftsColumns: {
    body: { storage: "fts-chunks-v1", tokenizerVersion: 1, state: "ready", buildFromVersion: -1 },
  },
  revision: 0,
  createdAt: NOW,
};
const IDENTITY = "articles/body";

async function articleStore(
  indexedDB: IDBFactory,
  name: string = crypto.randomUUID(),
): Promise<IndexedDbBlockStore> {
  const store = await openStore(indexedDB, name);
  await store.addTable(ARTICLES);
  await store.writeFtsBase("articles", "body", { coversVersion: -1, chunks: [], totalTokens: 0 });
  return store;
}

function tokenTotal(postings: readonly FtsPosting[]): number {
  return postings.reduce(
    (total, posting) => total + posting.tf.reduce((sum, tf) => sum + tf, 0),
    0,
  );
}

async function commitPostings(
  store: IndexedDbBlockStore,
  expectedVersion: number | null,
  postings: FtsPosting[],
  totalTokens = tokenTotal(postings),
): Promise<ManifestSummary> {
  const id = crypto.randomUUID();
  await store.createTransaction(activeTransaction(id, expectedVersion));
  return store.commitTransaction({
    transactionId: id,
    expectedTransactionRevision: 0,
    expectedManifestVersion: expectedVersion,
    ftsChanges: [{ tableId: "articles", columns: [{ columnId: "body", postings, totalTokens }] }],
    committedAt: NOW,
  });
}

const term = (prefix: string, index: number) => `${prefix}${String(index).padStart(7, "0")}`;

function distinct(prefix: string, count: number, firstRowId = 1): FtsPosting[] {
  return Array.from({ length: count }, (_, index) => ({
    term: term(prefix, index),
    rowIds: [BigInt(firstRowId + index)],
    tf: [1 + (index % 3)],
  }));
}

function wide(name: string, count: number, firstRowId = 1): FtsPosting {
  return {
    term: name,
    rowIds: Array.from({ length: count }, (_, index) => BigInt(firstRowId + index)),
    tf: Array.from({ length: count }, () => 1),
  };
}

/** A delta spanning several parts: distinct terms on both sides of one term too big for a part. */
function mixedDelta(): FtsPosting[] {
  return [
    ...distinct("a-", 10_000),
    wide("m-wide", 140_000, 1_000_000),
    ...distinct("z-", 10_000, 2_000_000),
  ];
}

async function candidates(store: IndexedDbBlockStore, queries: FtsPostingQuery[], version: number) {
  return store.readFtsCandidates("articles", "body", queries, version);
}

// ---------------------------------------------------------------------------------------------
// Writing and reading multi-part deltas
// ---------------------------------------------------------------------------------------------

describe("IndexedDB schema 4 delta parts", () => {
  it("stores a delta of any size as ordered parts within the part ceilings", async () => {
    const indexedDB = new IDBFactory();
    const name = crypto.randomUUID();
    const store = await articleStore(indexedDB, name);
    const postings = mixedDelta();
    const manifest = await commitPostings(store, null, structuredClone(postings));
    // A version count: many parts are still one generation of the tail.
    expect(manifest.ftsDeltaCounts).toEqual([{ tableId: "articles", columnId: "body", count: 1 }]);

    const parts = await readRawParts(indexedDB, name, IDENTITY, manifest.version);
    expect(parts.length).toBeGreaterThan(4);
    for (const part of parts) {
      expect(part.postings.length).toBeGreaterThan(0);
      expect(part.postings.length).toBeLessThanOrEqual(PART_POSTINGS);
      const rowIds = part.postings.reduce((total, posting) => total + posting.rowIds.length, 0);
      expect(rowIds).toBeLessThanOrEqual(PART_ROW_IDS);
      expect(part.totalTokens).toBe(tokenTotal(part.postings));
    }
    expect(rejoin(parts)).toEqual(postings);
    const directory = (await readRawValue(indexedDB, name, "catalog", [
      PART,
      IDENTITY,
      manifest.version,
      "directory",
    ])) as { boundaries: Array<{ first: string; last: string }>; totalTokens: number };
    expect(directory.boundaries).toEqual(
      parts.map((part) => ({
        first: part.postings[0]?.term,
        last: part.postings.at(-1)?.term,
      })),
    );
    expect(directory.totalTokens).toBe(tokenTotal(postings));
    expect(await readRawValue(indexedDB, name, "catalog", `fts-chunk/index/${IDENTITY}`)).toEqual({
      versions: [manifest.version],
      parts: [parts.length],
    });

    // Lookups read the parts their term range touches and report exact totals.
    const exact = await candidates(
      store,
      [
        { term: term("a-", 42), prefix: false },
        { term: term("z-", 9_999), prefix: false },
        { term: "nothing", prefix: false },
      ],
      manifest.version,
    );
    expect(exact).toMatchObject({
      rowIdsByTerm: [[43n], [2_009_999n], []],
      overflow: false,
      hasBase: true,
      deltaChunkCount: 1,
      totalTokens: tokenTotal(postings),
    });
    expect(
      await candidates(store, [{ term: "m-wide", prefix: false }], manifest.version),
    ).toMatchObject({ overflow: true });
    expect((await store.checkIntegrity({ mode: "full" })).issues).toEqual([]);
    store.close();
  });

  it("keeps a delta that fits one part in the schema-3 layout", async () => {
    const indexedDB = new IDBFactory();
    const name = crypto.randomUUID();
    const store = await articleStore(indexedDB, name);
    const postings = distinct("a-", 100);
    const manifest = await commitPostings(store, null, postings);
    expect(await partKeys(indexedDB, name)).toEqual([]);
    expect(await readRawValue(indexedDB, name, "catalog", `fts-chunk/index/${IDENTITY}`)).toEqual({
      versions: [manifest.version],
    });
    expect(
      await readRawValue(
        indexedDB,
        name,
        "catalog",
        `fts-chunk/${IDENTITY}/${String(manifest.version)}`,
      ),
    ).toEqual({ postings, totalTokens: tokenTotal(postings) });
    store.close();
  });

  it("answers lookups exactly like a reference model across random multi-part commits", async () => {
    await fc.assert(
      fc.asyncProperty(
        fc.array(
          fc.record({
            terms: fc.integer({ min: 0, max: 20_000 }),
            wideRows: fc.oneof(fc.constant(0), fc.integer({ min: 1, max: 150_000 })),
            seed: fc.integer({ min: 1, max: 2 ** 30 }),
          }),
          { minLength: 1, maxLength: 3 },
        ),
        async (commits) => {
          const indexedDB = new IDBFactory();
          const name = crypto.randomUUID();
          const store = await articleStore(indexedDB, name);
          const reference = new Map<string, Map<bigint, number>>();
          let version: number | null = null;
          let tokens = 0;
          const versions: number[] = [];
          for (const commit of commits) {
            let state = commit.seed;
            const next = () => {
              state = (state * 1_103_515_245 + 12_345) % 2 ** 31;
              return state;
            };
            const postings: FtsPosting[] = [];
            // Terms come from a small shared space so commits overlap on terms and rows.
            const chosen = new Set<number>();
            for (let index = 0; index < commit.terms; index += 1) chosen.add(next() % 30_000);
            const sorted = [...chosen]
              .sort((left, right) => left - right)
              .map((value) => term("t", value));
            const wideAt = commit.wideRows > 0 ? next() % (sorted.length + 1) : -1;
            if (wideAt === 0) postings.push(wide("s-w", commit.wideRows, 1 + (next() % 1_000)));
            sorted.forEach((value, position) => {
              const rows = [
                ...new Set(Array.from({ length: 1 + (next() % 3) }, () => 1 + (next() % 50_000))),
              ]
                .sort((left, right) => left - right)
                .map(BigInt);
              postings.push({ term: value, rowIds: rows, tf: rows.map(() => 1 + (next() % 4)) });
              // "t0001234-w" sorts after "t0001234" and before the next padded term.
              if (position + 1 === wideAt) {
                postings.push(wide(`${value}-w`, commit.wideRows, 1 + (next() % 1_000)));
              }
            });
            const manifest: ManifestSummary = await commitPostings(store, version, postings);
            version = manifest.version;
            if (postings.length > 0) versions.push(version);
            tokens += tokenTotal(postings);
            for (const posting of postings) {
              const rows = reference.get(posting.term) ?? new Map<bigint, number>();
              posting.rowIds.forEach((rowId, index) => {
                rows.set(rowId, Math.max(rows.get(rowId) ?? 0, posting.tf[index] ?? 0));
              });
              reference.set(posting.term, rows);
            }
            if (postings.length > 0) {
              // The stored parts are exactly the commit's postings, split and nothing else.
              expect(rejoin(await readRawParts(indexedDB, name, IDENTITY, version))).toEqual(
                postings,
              );
            }
          }
          const terms = [...reference.keys()].sort();
          const queries: FtsPostingQuery[] = [
            { term: terms[0] ?? "t", prefix: false },
            { term: terms.at(-1) ?? "t", prefix: false },
            { term: terms[Math.floor(terms.length / 2)] ?? "t", prefix: false },
            { term: "t000001", prefix: true },
            { lower: "t0001", upper: "t00012", upperInclusive: false },
            { term: "absent", prefix: false },
          ];
          for (const query of queries) {
            const matched = terms.filter((candidate) =>
              "term" in query
                ? query.prefix
                  ? candidate.startsWith(query.term)
                  : candidate === query.term
                : (query.lower === undefined || candidate >= query.lower) &&
                  (query.upper === undefined ||
                    candidate < query.upper ||
                    (candidate === query.upper && query.upperInclusive !== false)),
            );
            const expected = [
              ...new Set(
                matched.flatMap((candidate) => [...(reference.get(candidate)?.keys() ?? [])]),
              ),
            ].sort((left, right) => (left < right ? -1 : 1));
            const result = await candidates(store, [query], version ?? -1);
            const overflow = expected.length > 65_536;
            expect(result.overflow).toBe(overflow);
            expect(result.hasBase).toBe(!overflow);
            if (!overflow) {
              expect(result.rowIdsByTerm).toEqual([expected]);
              // Exact totals and a version count, whichever parts the lookup skipped.
              expect(result.deltaChunkCount).toBe(versions.length);
              expect(result.totalTokens).toBe(tokens);
            }
          }
          expect((await store.checkIntegrity({ mode: "full" })).issues).toEqual([]);
          store.close();
        },
      ),
      { numRuns: 8 },
    );
  });

  it("refuses a malformed delta of any size before any part becomes visible", async () => {
    const indexedDB = new IDBFactory();
    const name = crypto.randomUUID();
    const store = await articleStore(indexedDB, name);
    const base = await commitPostings(store, null, distinct("a-", 10));
    const descendingAcrossParts = [
      ...distinct("b-", 20_000),
      { term: "a-0000000", rowIds: [5n], tf: [1] },
    ];
    const rowsBackwards = wide("m-wide", 100_000);
    rowsBackwards.rowIds[90_000] = 3n;
    const zeroFrequency = distinct("c-", 12_000).map((posting, index) =>
      index === 11_000 ? { ...posting, tf: [0] } : posting,
    );
    for (const postings of [descendingAcrossParts, [rowsBackwards], zeroFrequency]) {
      await expect(commitPostings(store, base.version, postings)).rejects.toThrow(
        /Full-text delta is invalid/,
      );
    }
    // A token total that disagrees with the postings is refused after every part was written.
    await expect(commitPostings(store, base.version, distinct("d-", 20_000), 1)).rejects.toThrow(
      /Full-text delta is invalid/,
    );
    expect(await store.getCurrentManifestVersion()).toBe(base.version);
    expect(await partKeys(indexedDB, name)).toEqual([]);
    expect(await legacyDeltaKeys(indexedDB, name)).toEqual([
      `fts-chunk/${IDENTITY}/${String(base.version)}`,
    ]);
    expect((await store.checkIntegrity({ mode: "full" })).issues).toEqual([]);
    store.close();
  });

  it("leaves nothing of a multi-part delta after a crash mid-commit, and the retry commits", async () => {
    const indexedDB = new IDBFactory();
    const instrumented = instrumentFactory(indexedDB);
    const name = crypto.randomUUID();
    const store = await articleStore(instrumented.factory, name);
    const base = await commitPostings(store, null, distinct("a-", 10));
    for (const failure of ["abort", "throw-quota"] as const) {
      let fired = false;
      instrumented.setHook(({ storeName, method, key }) => {
        if (fired || storeName !== "catalog" || method !== "put" || !Array.isArray(key)) {
          return undefined;
        }
        // The power cut lands on the third part; the quota failure on the directory, the last
        // write before the delta index.
        if (key[0] === PART && (failure === "abort" ? key[3] === 3 : key[3] === "directory")) {
          fired = true;
          return failure;
        }
        return undefined;
      });
      await expect(commitPostings(store, base.version, mixedDelta())).rejects.toThrow();
      expect(fired).toBe(true);
      instrumented.setHook(undefined);
      expect(await store.getCurrentManifestVersion()).toBe(base.version);
      expect(await partKeys(indexedDB, name)).toEqual([]);
      expect(await legacyDeltaKeys(indexedDB, name)).toHaveLength(1);
      expect(await candidates(store, [{ term: term("z-", 5), prefix: false }], 99)).toMatchObject({
        rowIdsByTerm: [[]],
        hasBase: true,
        deltaChunkCount: 1,
      });
      expect((await store.checkIntegrity({ mode: "full" })).issues).toEqual([]);
    }
    const retried = await commitPostings(store, base.version, mixedDelta());
    expect(
      await candidates(store, [{ term: term("z-", 5), prefix: false }], retried.version),
    ).toMatchObject({
      rowIdsByTerm: [[2_000_005n]],
      hasBase: true,
      deltaChunkCount: 2,
    });
    store.close();
  });

  it("hands the event loop turns between the parts of a large delta without committing early", async () => {
    const indexedDB = new IDBFactory();
    const instrumented = instrumentFactory(indexedDB);
    const name = crypto.randomUUID();
    const store = await articleStore(instrumented.factory, name);
    const putTimes: number[] = [];
    instrumented.setHook(({ storeName, method, key }) => {
      if (storeName === "catalog" && method === "put" && Array.isArray(key) && key[0] === PART) {
        putTimes.push(performance.now());
      }
      return undefined;
    });
    const ticks: number[] = [];
    const tick = setInterval(() => ticks.push(performance.now()), 0);
    const postings = distinct("a-", 120_000);
    const manifest = await commitPostings(store, null, postings);
    clearInterval(tick);
    const first = putTimes[0] ?? 0;
    const last = putTimes.at(-1) ?? 0;
    expect(putTimes.length).toBeGreaterThan(10);
    expect(ticks.filter((time) => time > first && time < last).length).toBeGreaterThan(0);
    // One transaction all the same: every part landed with the delta index.
    expect(rejoin(await readRawParts(indexedDB, name, IDENTITY, manifest.version))).toEqual(
      postings,
    );
    store.close();
  });
});

// ---------------------------------------------------------------------------------------------
// Corruption refusal
// ---------------------------------------------------------------------------------------------

describe("IndexedDB schema 4 corrupt deltas", () => {
  async function committedMixed() {
    const indexedDB = new IDBFactory();
    const name = crypto.randomUUID();
    const store = await articleStore(indexedDB, name);
    const manifest = await commitPostings(store, null, mixedDelta());
    const parts = await readRawParts(indexedDB, name, IDENTITY, manifest.version);
    return { indexedDB, name, store, version: manifest.version, parts };
  }

  async function expectRefused(
    store: IndexedDbBlockStore,
    version: number,
    touched: FtsPostingQuery,
  ): Promise<void> {
    expect((await candidates(store, [touched], version)).hasBase).toBe(false);
    expect((await store.readFtsPostings("articles", "body", version, 65_536)).hasBase).toBe(false);
    expect((await store.checkIntegrity({ mode: "full" })).ok).toBe(false);
  }

  it("treats a missing later part as an incomplete index", async () => {
    const { indexedDB, name, store, version, parts } = await committedMixed();
    const last = parts.length - 1;
    await mutate(indexedDB, name, (catalog) => catalog.delete([PART, IDENTITY, version, last]));
    await expectRefused(store, version, { term: term("z-", 9_999), prefix: false });
    // A lookup whose range the directory keeps away from the gap still has its answer.
    expect(
      await candidates(store, [{ term: term("a-", 3), prefix: false }], version),
    ).toMatchObject({ rowIdsByTerm: [[4n]], hasBase: true });
    store.close();
  });

  it("reports a stray part beyond the recorded count", async () => {
    const { indexedDB, name, store, version, parts } = await committedMixed();
    await mutate(indexedDB, name, (catalog) =>
      catalog.put(parts[0], [PART, IDENTITY, version, parts.length]),
    );
    const report = await store.checkIntegrity({ mode: "full" });
    expect(report.issues).toEqual([
      expect.objectContaining({
        code: "invalid-catalog-record",
        message: expect.stringMatching(/no index or retirement marker/) as unknown,
      }),
    ]);
    store.close();
  });

  it("refuses a malformed part, swapped parts, and a missing or mismatched directory", async () => {
    const malformed = await committedMixed();
    await mutate(malformed.indexedDB, malformed.name, (catalog) =>
      catalog.put({ postings: "broken", totalTokens: 1 }, [PART, IDENTITY, malformed.version, 1]),
    );
    await expectRefused(malformed.store, malformed.version, { term: "m-wide", prefix: true });
    malformed.store.close();

    const swapped = await committedMixed();
    await mutate(swapped.indexedDB, swapped.name, (catalog) => {
      catalog.put(swapped.parts[2], [PART, IDENTITY, swapped.version, 1]);
      catalog.put(swapped.parts[1], [PART, IDENTITY, swapped.version, 2]);
    });
    await expectRefused(swapped.store, swapped.version, { term: "m-wide", prefix: true });
    swapped.store.close();

    const noDirectory = await committedMixed();
    await mutate(noDirectory.indexedDB, noDirectory.name, (catalog) =>
      catalog.delete([PART, IDENTITY, noDirectory.version, "directory"]),
    );
    await expectRefused(noDirectory.store, noDirectory.version, {
      term: term("a-", 1),
      prefix: false,
    });
    noDirectory.store.close();

    const shortDirectory = await committedMixed();
    await mutate(shortDirectory.indexedDB, shortDirectory.name, (catalog) =>
      catalog.put(
        {
          boundaries: shortDirectory.parts.slice(1).map((part) => ({
            first: part.postings[0]?.term,
            last: part.postings.at(-1)?.term,
          })),
          totalTokens: 1,
        },
        [PART, IDENTITY, shortDirectory.version, "directory"],
      ),
    );
    await expectRefused(shortDirectory.store, shortDirectory.version, {
      term: term("a-", 1),
      prefix: false,
    });
    shortDirectory.store.close();
  });

  it("refuses row ids that go backwards where a term continues into the next part", async () => {
    const { indexedDB, name, store, version, parts } = await committedMixed();
    const position = parts.findIndex(
      (part, index) =>
        index > 0 && part.postings[0]?.term === parts[index - 1]?.postings.at(-1)?.term,
    );
    expect(position).toBeGreaterThan(0);
    const continued = structuredClone(parts[position]);
    const first = continued?.postings[0];
    if (first === undefined) throw new Error("The continued part is empty");
    // Still ascending inside the part, but starting below where the previous part ended.
    first.rowIds = first.rowIds.map((_, index) => BigInt(index + 1));
    await mutate(indexedDB, name, (catalog) =>
      catalog.put(continued, [PART, IDENTITY, version, position]),
    );
    await expectRefused(store, version, { term: "m-wide", prefix: false });
    store.close();
  });

  it("refuses an unreadable part count, and the next commit rebuilds from nothing", async () => {
    const { indexedDB, name, store, version } = await committedMixed();
    await mutate(indexedDB, name, (catalog) =>
      catalog.put({ versions: [version], parts: [0] }, `fts-chunk/index/${IDENTITY}`),
    );
    await expectRefused(store, version, { term: term("a-", 1), prefix: false });
    await commitPostings(store, version, distinct("b-", 3));
    // The commit cannot trust the index, so it invalidates the column and deletes every part.
    expect((await store.getTable("articles"))?.ftsColumns?.body?.state).toBe("invalid");
    expect(await partKeys(indexedDB, name)).toEqual([]);
    store.close();
  });
});

// ---------------------------------------------------------------------------------------------
// Folds, drops, export
// ---------------------------------------------------------------------------------------------

describe("IndexedDB schema 4 delta lifecycle", () => {
  it("retires a folded multi-part delta and deletes every part", async () => {
    const indexedDB = new IDBFactory();
    const name = crypto.randomUUID();
    let store = await articleStore(indexedDB, name);
    const first = await commitPostings(store, null, mixedDelta());
    const second = await commitPostings(store, first.version, distinct("q-", 9_000, 5_000_000));
    // Fold the first version only; the second, also multi-part, survives with its part count.
    await store.writeFtsBase("articles", "body", {
      coversVersion: first.version,
      chunks: [distinct("a-", 3)],
      totalTokens: tokenTotal(distinct("a-", 3)),
    });
    expect(await readRawValue(indexedDB, name, "catalog", `fts-chunk/index/${IDENTITY}`)).toEqual({
      versions: [second.version],
      parts: [2],
    });
    // Retiring parts are owned by the retirement marker until its cleanup runs.
    expect((await store.checkIntegrity({ mode: "full" })).issues).toEqual([]);
    store.close();
    store = await openStore(indexedDB, name);
    expect((await partKeys(indexedDB, name)).map((key) => key[2])).toEqual([
      second.version,
      second.version,
    ]);
    expect(
      await candidates(store, [{ term: term("q-", 8_999), prefix: false }], second.version),
    ).toMatchObject({
      rowIdsByTerm: [[5_008_999n]],
      hasBase: true,
      deltaChunkCount: 1,
    });
    // Dropping the column retires the survivor too.
    const table = await store.getTable("articles");
    await store.updateTable("articles", table?.revision ?? 0, { ftsColumns: null });
    await store.removeFtsColumn("articles", "body");
    expect(await partKeys(indexedDB, name)).toEqual([]);
    expect(await legacyDeltaKeys(indexedDB, name)).toEqual([]);
    expect((await store.checkIntegrity({ mode: "full" })).issues).toEqual([]);
    store.close();
  });
});

// ---------------------------------------------------------------------------------------------
// Through the engine
// ---------------------------------------------------------------------------------------------

const WORDS = ["amber", "birch", "cedar", "delta", "ember"];
const range = (start: number, end: number) =>
  Array.from({ length: end - start }, (_, index) => start + index);
const item = (id: number) => ({ id, code: id * 7, label: `item-${String(id)}` });
const note = (index: number) => ({ body: `entry ${WORDS[index % 5] ?? ""} n${String(index)}` });

/** Records whether each index lookup the engine makes is answered completely by the index. */
function watchLookups(store: IndexedDbBlockStore): { complete: boolean[] } {
  const watched = { complete: [] as boolean[] };
  const read = store.readFtsCandidates.bind(store);
  store.readFtsCandidates = async (...args: Parameters<typeof read>) => {
    const result = await read(...args);
    watched.complete.push(result.hasBase && !result.overflow);
    return result;
  };
  return watched;
}

interface Answers {
  codes: unknown[];
  codeRange: unknown;
  matches: unknown[];
}

interface QueryableDatabase {
  query(sql: string, options?: { params?: number[] }): Promise<{ rows: unknown[] }>;
}

async function answers(database: QueryableDatabase, extraCodes: number[] = []): Promise<Answers> {
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
  return {
    codes,
    codeRange: (
      await database.query("SELECT COUNT(*) AS n FROM items WHERE code >= ? AND code < ?", {
        params: [1_000, 3_000],
      })
    ).rows,
    matches,
  };
}

/**
 * The same history on any build: a keyed table with a secondary index built after its first rows
 * and an append-only table with a full-text index, each left with base postings plus deltas.
 */
async function writeIndexedHistory(database: {
  createTable(definition: unknown): Promise<unknown>;
  insertBatch(table: string, rows: Array<Record<string, string | number>>): Promise<unknown>;
  execute(sql: string): Promise<unknown>;
  buildFtsIndex(table: string, column: string): Promise<void>;
}): Promise<void> {
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

/** Writes past every schema-3 delta limit: more distinct values and terms than one record held. */
async function writePastOldLimits(database: MinnowDatabase): Promise<void> {
  await database.insertBatch("items", range(10_000, 80_000).map(item));
  await database.insertBatch("notes", range(500, 10_000).map(note));
}

describe("IndexedDB schema 4 through the engine", () => {
  it("commits index deltas past the schema-3 record limit and answers from them", async () => {
    const indexedDB = new IDBFactory();
    const name = crypto.randomUUID();
    let store = await IndexedDbBlockStore.open({ name, indexedDB });
    let database = new MinnowDatabase(store, { autoCompact: false, autoCollect: false });
    await writeIndexedHistory(database);
    await writePastOldLimits(database);
    expect((await partKeys(indexedDB, name)).length).toBeGreaterThan(10);
    const watched = watchLookups(store);
    const expected = await answers(database, [70_000 * 7]);
    expect(expected.codes.at(-1)).toEqual([{ id: 70_000 }]);
    expect(expected.matches).toEqual([[{ n: 2_000 }], [{ n: 1 }], [{ n: 1 }], [{ n: 1 }]]);
    expect(watched.complete.length).toBeGreaterThan(0);
    expect(watched.complete.every(Boolean)).toBe(true);
    expect((await store.checkIntegrity({ mode: "full" })).issues).toEqual([]);

    // A snapshot carries the merged postings; the restored index answers the same.
    const snapshot = await database.exportSnapshot();
    const restoredStore = await IndexedDbBlockStore.open({ name: crypto.randomUUID(), indexedDB });
    const restored = new MinnowDatabase(restoredStore, { autoCompact: false, autoCollect: false });
    await restored.importSnapshot(snapshot);
    const restoredWatch = watchLookups(restoredStore);
    expect(await answers(restored, [70_000 * 7])).toEqual(expected);
    expect(restoredWatch.complete.every(Boolean)).toBe(true);
    await restored.close();
    restoredStore.close();

    // Dropping the index retires its parts, which are gone by the next open at the latest.
    await database.execute("DROP INDEX items_code");
    expect((await store.checkIntegrity({ mode: "full" })).issues).toEqual([]);
    await database.close();
    store.close();
    store = await IndexedDbBlockStore.open({ name, indexedDB });
    database = new MinnowDatabase(store, { autoCompact: false, autoCollect: false });
    const remaining = await partKeys(indexedDB, name);
    expect(remaining.some((key) => key[1].includes("/secondary-index:"))).toBe(false);
    expect(remaining.length).toBeGreaterThan(0);
    expect(
      (await database.query("SELECT id FROM items WHERE code = ?", { params: [7] })).rows,
    ).toEqual([{ id: 1 }]);

    // Dropping the tables deletes every part.
    await database.execute("DROP TABLE items");
    await database.execute("DROP TABLE notes");
    expect(await partKeys(indexedDB, name)).toEqual([]);
    expect(await legacyDeltaKeys(indexedDB, name)).toEqual([]);
    expect((await store.checkIntegrity({ mode: "full" })).issues).toEqual([]);
    await database.close();
    store.close();
  });

  it("recovers from a crash mid-commit through the ordinary API", async () => {
    const indexedDB = new IDBFactory();
    const instrumented = instrumentFactory(indexedDB);
    const name = crypto.randomUUID();
    const store = await IndexedDbBlockStore.open({ name, indexedDB: instrumented.factory });
    const database = new MinnowDatabase(store, { autoCompact: false, autoCollect: false });
    await writeIndexedHistory(database);
    const before = await answers(database);
    let fired = false;
    instrumented.setHook(({ storeName, method, key }) => {
      if (
        !fired &&
        storeName === "catalog" &&
        method === "put" &&
        Array.isArray(key) &&
        key[0] === PART &&
        key[3] === 4
      ) {
        fired = true;
        return "abort";
      }
      return undefined;
    });
    await expect(database.insertBatch("items", range(10_000, 80_000).map(item))).rejects.toThrow();
    instrumented.setHook(undefined);
    expect(fired).toBe(true);
    expect(await answers(database)).toEqual(before);
    expect(await partKeys(indexedDB, name)).toEqual([]);
    expect((await database.query("SELECT COUNT(*) AS n FROM items")).rows).toEqual([{ n: 700 }]);
    expect((await store.checkIntegrity({ mode: "full" })).issues).toEqual([]);
    await writePastOldLimits(database);
    expect(
      (await database.query("SELECT id FROM items WHERE code = ?", { params: [70_000 * 7] })).rows,
    ).toEqual([{ id: 70_000 }]);
    await database.close();
    store.close();
  });

  it(
    "commits one term of more than 1,048,576 rows",
    async () => {
      const indexedDB = new IDBFactory();
      const name = crypto.randomUUID();
      const store = await IndexedDbBlockStore.open({ name, indexedDB });
      const database = new MinnowDatabase(store, { autoCompact: false, autoCollect: false });
      await database.createTable({
        name: "items",
        uniqueKey: "id",
        columns: [
          { name: "id", type: "number" },
          { name: "code", type: "number" },
        ],
      });
      await database.execute("CREATE INDEX items_code ON items (code)");
      const rows = 1_100_000;
      // Every row shares one code but the last ten, which each have their own.
      await database.insertBatch(
        "items",
        Array.from({ length: rows }, (_, id) => ({ id, code: id >= rows - 10 ? id : 7 })),
      );
      const watched = watchLookups(store);
      expect((await database.query("SELECT COUNT(*) AS n FROM items WHERE code = 7")).rows).toEqual(
        [{ n: rows - 10 }],
      );
      expect(
        (await database.query("SELECT id FROM items WHERE code = ?", { params: [rows - 3] })).rows,
      ).toEqual([{ id: rows - 3 }]);
      // The shared code overflows the candidate ceiling and scans; the single row is answered
      // from the one part its term lands in.
      expect(watched.complete).toContain(true);
      expect((await store.checkIntegrity({ mode: "full" })).issues).toEqual([]);
      await database.close();
      store.close();
    },
    heavyTestTimeout(120_000),
  );
});

// ---------------------------------------------------------------------------------------------
// Upgrades from every supported older writer
// ---------------------------------------------------------------------------------------------

interface ReleasedDatabase {
  createTable(definition: unknown): Promise<unknown>;
  insertBatch(table: string, rows: Array<Record<string, string | number>>): Promise<unknown>;
  execute(sql: string): Promise<unknown>;
  query(sql: string, options?: { params?: number[] }): Promise<{ rows: unknown[] }>;
  buildFtsIndex(table: string, column: string): Promise<void>;
  close(): Promise<void>;
}

interface ReleasedBuild {
  version: string;
  schema: number;
  openStore(indexedDB: IDBFactory, name: string): Promise<{ close(): void }>;
  openDatabase(store: never): ReleasedDatabase;
}

const options = { autoCompact: false, autoCollect: false } as const;
const releasedBuilds: ReleasedBuild[] = [
  {
    version: "0.10.0",
    schema: 2,
    openStore: (indexedDB, name) => Layout6Store.open({ name, indexedDB }),
    openDatabase: (store: Layout6Store) => new Layout6Database(store, options),
  },
  {
    version: "0.12.1",
    schema: 2,
    openStore: (indexedDB, name) => Layout7Store.open({ name, indexedDB }),
    openDatabase: (store: Layout7Store) => new Layout7Database(store, options),
  },
  {
    version: "0.13.1",
    schema: 3,
    openStore: (indexedDB, name) => Layout8Store.open({ name, indexedDB }),
    openDatabase: (store: Layout8Store) => new Layout8Database(store, options),
  },
];

async function releasedHistory(
  build: ReleasedBuild,
  indexedDB: IDBFactory,
  name: string,
): Promise<Answers> {
  const store = await build.openStore(indexedDB, name);
  const database = build.openDatabase(store as never);
  await writeIndexedHistory(database);
  const expected = await answers(database);
  await database.close();
  store.close();
  expect(await nativeVersion(indexedDB, name)).toBe(build.schema);
  // Both indexes were left with deltas beside their bases.
  const deltas = await legacyDeltaKeys(indexedDB, name);
  expect(deltas.some((key) => key.includes("/secondary-index:"))).toBe(true);
  expect(deltas.some((key) => !key.includes("/secondary-index:"))).toBe(true);
  return expected;
}

describe.each(releasedBuilds)("IndexedDB upgrade from the $version writer", (build) => {
  it("answers from its index deltas, writes past the old limits, and reopens", async () => {
    const indexedDB = new IDBFactory();
    const name = crypto.randomUUID();
    const expected = await releasedHistory(build, indexedDB, name);

    let store = await IndexedDbBlockStore.open({ name, indexedDB });
    expect(await nativeVersion(indexedDB, name)).toBe(CURRENT_SCHEMA);
    let database = new MinnowDatabase(store, options);
    let watched = watchLookups(store);
    expect(await answers(database)).toEqual(expected);
    expect(watched.complete.length).toBeGreaterThan(0);
    expect(watched.complete.every(Boolean)).toBe(true);
    expect((await store.checkIntegrity({ mode: "full" })).issues).toEqual([]);

    await writePastOldLimits(database);
    expect((await partKeys(indexedDB, name)).length).toBeGreaterThan(10);
    const latest = await answers(database, [70_000 * 7]);
    expect(latest.codes.slice(0, -1)).toEqual(expected.codes);
    expect(latest.codes.at(-1)).toEqual([{ id: 70_000 }]);
    expect(latest.matches).toEqual([[{ n: 2_000 }], [{ n: 1 }], [{ n: 1 }], [{ n: 1 }]]);
    await database.close();
    store.close();

    store = await IndexedDbBlockStore.open({ name, indexedDB });
    database = new MinnowDatabase(store, options);
    watched = watchLookups(store);
    expect(await answers(database, [70_000 * 7])).toEqual(latest);
    expect(watched.complete.every(Boolean)).toBe(true);
    expect((await store.checkIntegrity({ mode: "full" })).issues).toEqual([]);
    await database.close();
    store.close();
  });

  it("refuses the released reader after the upgrade and leaves the database intact", async () => {
    const indexedDB = new IDBFactory();
    const name = crypto.randomUUID();
    const expected = await releasedHistory(build, indexedDB, name);
    (await IndexedDbBlockStore.open({ name, indexedDB })).close();
    await expect(build.openStore(indexedDB, name)).rejects.toMatchObject({
      name: "StorageFormatVersionError",
      actualVersion: CURRENT_SCHEMA,
      supportedVersion: build.schema,
      relation: "newer",
    });
    expect(await nativeVersion(indexedDB, name)).toBe(CURRENT_SCHEMA);
    const store = await IndexedDbBlockStore.open({ name, indexedDB });
    const database = new MinnowDatabase(store, options);
    expect(await answers(database)).toEqual(expected);
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

describe("IndexedDB schema 3 to 4 upgrade", () => {
  const released = releasedBuilds.find((build) => build.schema === 3);
  if (released === undefined) throw new Error("The schema-3 writer is missing");

  it("leaves schema 3 for the released reader when the upgrade is interrupted, then retries", async () => {
    const indexedDB = new IDBFactory();
    const name = crypto.randomUUID();
    const expected = await releasedHistory(released, indexedDB, name);
    const interruption = abortNextUpgrade(indexedDB);
    await expect(IndexedDbBlockStore.open({ name, indexedDB })).rejects.toThrow();
    expect(interruption.aborted()).toBe(true);
    expect(await nativeVersion(indexedDB, name)).toBe(3);
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
    expect(await answers(database, [35_000])).toEqual({
      ...expected,
      codes: [...expected.codes, [{ id: 5_000 }]],
    });
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
    // A released opener that arrives behind the upgrade is refused, never served schema 4.
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
    for (const store of stores) store?.close();
    expect(await nativeVersion(indexedDB, name)).toBe(CURRENT_SCHEMA);
  });
});
