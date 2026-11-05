/**
 * Review B2-p2a (rebuild/format-p2a 43686a2): each probe of the review, as a test that rejects
 * it. Findings: 1 cached directories skip the chunk bounds check; 2 the governor kept only its
 * last registered cache; 3 a leading BOM was stripped; 4 a `__proto__` ledger count was lost;
 * 5 decoders did not enforce the blob cap; 6 the walk accepted contradictory directory totals;
 * 7 non-canonical bytes (extent and type tables, bigints) were accepted.
 */
import { describe, expect, it } from "vitest";
import { Governor } from "../memory/governor.js";
import { MemoryExtentSpace, UNLIMITED } from "./memory-extents.js";
import { encodeDirectory, type ChunkDirectory } from "./directory.js";
import { PageCorruptionError, ValueTag } from "./format.js";
import { ChunkReader } from "./reader.js";
import { MAX_BLOB_BYTES, type BlobRef, type ByteBudget } from "../storage/extents.js";
import { MemoryBackend } from "../storage/memory-store.js";
import { openExtentSpace } from "../storage/space.js";
import { bytesOf, countingBudget, jsonReachable } from "../storage/space-test-support.js";
import { BlobCache } from "../format/blob-cache.js";
import { ByteReader, ByteWriter, ManifestCorruptionError } from "../format/bytes.js";
import {
  decodeGroupIndex,
  decodeGroupPage,
  decodeGroupRoot,
  decodeManifestRoot,
  encodeGroupIndex,
  encodeGroupPage,
  encodeGroupRoot,
  encodeManifestRoot,
  writeGroupDirectory,
  type GroupPageEntry,
} from "../format/manifest-codec.js";
import {
  PageEncoding,
  type ColumnChunk,
  type GroupRef,
  type ManifestRoot,
  type TableRoot,
} from "../format/manifest.js";
import { reachableExtents } from "../format/reachable.js";

const EMPTY: BlobRef = { extent: { offset: 0, length: 0 }, offset: 0, length: 0, checksum: 0 };
const MAGIC = { root: 0x31524d4d, page: 0x3150474d, group: 0x3152474d };
const ref: BlobRef = { extent: { offset: 0, length: 4096 }, offset: 0, length: 10, checksum: 0 };
const root: ManifestRoot = {
  formatVersion: 1,
  sequence: 1,
  epoch: 1,
  log: { salt: 1, firstLsn: 1, rootSequence: 1 },
  globalLsn: 1,
  catalogVersion: 1,
  catalog: ref,
  nextTableId: 1,
  sequences: [],
  tables: [],
  clients: [],
};
const freeMemory = (): { charge: () => boolean; release: () => void } => ({
  charge: () => true,
  release: () => undefined,
});

/** A budget that records whether anything was asked of it. */
function watched(): ByteBudget & { asked: number } {
  const budget = {
    asked: 0,
    allocate: () => {
      budget.asked++;
      return true;
    },
    free: () => undefined,
  };
  return budget;
}

const pause = (ms: number): Promise<void> =>
  new Promise((resolve) => {
    setTimeout(resolve, ms);
  });

describe("review B2-p2a #1: a cached directory is checked against every chunk that references it", () => {
  const entry = (offset: number, length: number, encoding: number) => ({
    offset,
    length,
    firstRow: 0,
    rowCount: 1,
    encoding,
    nullCount: 0,
    min: null,
    max: null,
    checksum: 0,
  });

  async function opened(directory: ChunkDirectory, type: ColumnChunk["type"]) {
    const space = new MemoryExtentSpace();
    const writer = space.beginWrite("r");
    const blob = (await writer.write([encodeDirectory(directory)])).blobs[0];
    if (blob === undefined) throw new Error("not written");
    const chunk: ColumnChunk = {
      columnId: 1,
      type,
      data: { extent: { offset: 0, length: 4096 }, offset: 0, length: 200 },
      directory: blob,
      statistics: { nullCount: 0, min: null, max: null, dictionarySize: null },
      distinctSketch: null,
    };
    const directories = new BlobCache<ChunkDirectory>(freeMemory);
    // The first reader parses it for a chunk it fits and caches it.
    (await ChunkReader.open(space, chunk, { budget: UNLIMITED, directories })).close();
    expect(directories.entryCount).toBe(1);
    return { space, chunk, directories };
  }

  it("refuses a cached directory whose pages pass a shorter chunk (the probe), and unpins it", async () => {
    const { space, chunk, directories } = await opened(
      {
        tag: ValueTag.i32,
        rowCount: 1,
        scale: 0,
        hasSpecials: false,
        pages: [entry(150, 10, PageEncoding.plain)],
        dictionary: null,
        sketch: null,
      },
      { kind: "int32", bits: 32 },
    );
    const short = { ...chunk, data: { ...chunk.data, length: 100 } };
    const budget = countingBudget(Infinity);
    await expect(
      Promise.resolve().then(() => ChunkReader.open(space, short, { budget })),
    ).rejects.toThrow(PageCorruptionError);
    // From the cache the refusal is synchronous, like the hit it replaces.
    expect(() => ChunkReader.open(space, short, { budget, directories })).toThrow(
      PageCorruptionError,
    );
    expect(budget.used).toBe(0);
    // Unpinned again: the governor can evict it.
    expect(directories.evictableBytes).toBeGreaterThan(0);
    // The chunk it was parsed for still opens from the cache.
    const again = ChunkReader.open(space, chunk, { budget, directories });
    if (again instanceof Promise) throw new Error("expected a cache hit");
    again.close();
  });

  it("checks the dictionary page too", async () => {
    const { space, chunk, directories } = await opened(
      {
        tag: ValueTag.varlen,
        rowCount: 1,
        scale: 0,
        hasSpecials: false,
        pages: [entry(0, 10, PageEncoding.dictionary)],
        dictionary: { ...entry(100, 50, PageEncoding.plain), rowCount: 3 },
        sketch: null,
      },
      { kind: "text" },
    );
    // Its data page fits 120 bytes; its dictionary (100 … 150) does not.
    const short = { ...chunk, data: { ...chunk.data, length: 120 } };
    expect(() => ChunkReader.open(space, short, { budget: UNLIMITED, directories })).toThrow(
      PageCorruptionError,
    );
    expect(directories.evictableBytes).toBeGreaterThan(0);
  });
});

describe("review B2-p2a #2: the governor evicts every registered cache, the entry unused longest first", () => {
  it("admits after evicting an earlier-registered cache (the probe)", () => {
    const governor = new Governor(64 * 1024 * 1024);
    const register = governor.registerCache.bind(governor);
    const a = new BlobCache<string>(register);
    a.admit(ref, "old", 1024);
    a.unpin(ref);
    const request = {
      label: "review",
      minimumBytes: governor.capBytes - governor.reserveBytes,
      spillable: true,
    };
    new BlobCache<string>(register); // registered second, holds nothing
    const grant = governor.tryAdmit(request);
    expect(grant).not.toBeNull();
    expect(a.evictableBytes).toBe(0);
    grant?.close();
    expect(governor.usedBytes).toBe(0);
  });

  it("orders eviction across the space's page cache and the format caches", async () => {
    const governor = new Governor(64 * 1024 * 1024);
    const register = governor.registerCache.bind(governor);
    const backend = new MemoryBackend();
    const session = await backend.open(await backend.acquire({ steal: false, timeoutMs: 2000 }));
    const budget = countingBudget(Infinity);
    const space = await openExtentSpace(session, {
      reachable: jsonReachable,
      budget,
      registerCache: register,
    });
    const roots = new BlobCache<string>(register);
    const pages = new BlobCache<string>(register);
    const writer = space.beginWrite("t");
    const [first, second] = (await writer.write([bytesOf(100_000, 1), bytesOf(100_000, 2)])).blobs;
    if (first === undefined || second === undefined) throw new Error("not written");
    const use = async (blob: BlobRef): Promise<void> => {
      (await space.read([blob], { budget })).release();
      await pause(3);
    };
    const keep = async (cache: BlobCache<string>, size: number): Promise<BlobRef> => {
      const key = { ...ref, checksum: size };
      cache.admit(key, "decoded", size);
      cache.unpin(key);
      await pause(3);
      return key;
    };
    // Last used, oldest first: page `first`, a group root, page `second`, a directory page.
    await use(first);
    const rootKey = await keep(roots, 30_000);
    await use(second);
    await keep(pages, 20_000);
    const resident = (): number => space.stats.cache.residentBytes;
    expect(resident()).toBe(200_000);
    const admitOneMore = (): void => {
      const free = governor.capBytes - governor.reserveBytes - governor.usedBytes;
      const grant = governor.tryAdmit({ label: "probe", minimumBytes: free + 1, spillable: true });
      expect(grant).not.toBeNull();
      grant?.close();
    };
    admitOneMore();
    expect([resident(), roots.evictableBytes, pages.evictableBytes]).toEqual([
      100_000, 30_000, 20_000,
    ]);
    admitOneMore();
    expect([resident(), roots.evictableBytes, pages.evictableBytes]).toEqual([100_000, 0, 20_000]);
    admitOneMore();
    expect([resident(), roots.evictableBytes, pages.evictableBytes]).toEqual([0, 0, 20_000]);
    // A use moves an entry to the back: a group root kept before a second directory page but used
    // after it outlives that page.
    const again = await keep(roots, 30_000);
    await keep(pages, 10_000);
    expect(roots.pin(again)?.value).toBe("decoded");
    roots.unpin(again);
    admitOneMore(); // the first directory page (oldest)
    expect([roots.evictableBytes, pages.evictableBytes]).toEqual([30_000, 10_000]);
    admitOneMore(); // the second, released before the group root's last use
    expect([roots.evictableBytes, pages.evictableBytes]).toEqual([30_000, 0]);
    expect(again).toEqual(rootKey);
    await space.close();
  });
});

describe("review B2-p2a #3, #4: strings and ledger keys decode as written", () => {
  it("keeps a leading byte-order mark (the probe)", () => {
    const withBom = {
      ...root,
      clients: [{ clientId: "﻿client", resolvedThrough: 0, outcomes: [] }],
    };
    const decoded = decodeManifestRoot(encodeManifestRoot(withBom), UNLIMITED).value;
    expect(decoded.clients[0]?.clientId).toBe("﻿client");
    const out = new ByteWriter();
    out.string("﻿");
    expect(new ByteReader(out.finish()).string()).toBe("﻿");
  });

  it("keeps a __proto__ ledger count (the probe) and refuses duplicate or unordered keys", () => {
    const counts = JSON.parse('{"__proto__":7,"inserted":2}') as Record<string, number>;
    const withProto = {
      ...root,
      clients: [
        {
          clientId: "x",
          resolvedThrough: 0,
          outcomes: [{ sequence: 1, lsn: 1, counts, recordedAtMs: 0 }],
        },
      ],
    };
    const bytes = encodeManifestRoot(withProto);
    const decoded = decodeManifestRoot(bytes, UNLIMITED).value;
    const got = decoded.clients[0]?.outcomes[0]?.counts ?? {};
    expect(Object.keys(got)).toEqual(["__proto__", "inserted"]);
    expect(Object.getPrototypeOf(got)).toBeNull();
    expect(encodeManifestRoot(decoded)).toEqual(bytes);
    // Crafted: the same key twice, and two keys out of order.
    const keysAt = (keys: string[]): Uint8Array => {
      const out = new ByteWriter();
      out.u32(MAGIC.root);
      out.u8(1);
      out.uint(1); // extent table: the catalog's extent
      out.uint(0);
      out.uint(4096);
      for (const n of [1, 1, 1]) out.uint(n); // format version, sequence, epoch
      out.u32(1); // log salt
      for (const n of [1, 1, 1, 1, 0, 0, 10]) out.uint(n); // log, lsn, catalog version, catalog
      out.u32(0);
      for (const n of [1, 0, 0, 1]) out.uint(n); // next table id, no sequences or tables, 1 client
      out.string("x");
      out.uint(0); // resolved through
      out.uint(1); // one outcome
      out.uint(1); // sequence
      out.uint(1); // lsn
      out.uint(keys.length);
      for (const key of keys) {
        out.string(key);
        out.f64(1);
      }
      out.f64(0); // recorded at
      return out.finish();
    };
    expect(decodeManifestRoot(keysAt(["__proto__", "a"]), UNLIMITED).value.clients).toHaveLength(1);
    expect(() => decodeManifestRoot(keysAt(["__proto__", "__proto__"]), UNLIMITED)).toThrow(
      ManifestCorruptionError,
    );
    expect(() => decodeManifestRoot(keysAt(["b", "a"]), UNLIMITED)).toThrow(
      ManifestCorruptionError,
    );
  });
});

describe("review B2-p2a #5: every decoder refuses a blob over its cap before charging", () => {
  it("refuses the probe's 1,048,612-byte root without asking the budget", () => {
    const out = new ByteWriter();
    out.u32(MAGIC.root);
    out.u8(1);
    out.uint(1);
    out.uint(0);
    out.uint(4096);
    for (const n of [1, 1, 1]) out.uint(n);
    out.u32(1);
    for (const n of [1, 1, 1, 1, 0, 0, 10]) out.uint(n);
    out.u32(0);
    for (const n of [1, 0, 0, 1]) out.uint(n);
    out.string("x".repeat(1024 * 1024));
    out.uint(0);
    out.uint(0);
    const bytes = out.finish();
    expect(bytes.length).toBe(1_048_612);
    const budget = watched();
    expect(() => decodeManifestRoot(bytes, budget)).toThrow(ManifestCorruptionError);
    expect(budget.asked).toBe(0);
  });

  it("refuses an oversize group root, directory page and index alike", () => {
    const big = new Uint8Array(MAX_BLOB_BYTES + 1);
    for (const decode of [decodeGroupRoot, decodeGroupPage, decodeGroupIndex, decodeManifestRoot]) {
      const budget = watched();
      expect(() => decode(big, budget)).toThrow(/cap/u);
      expect(budget.asked).toBe(0);
    }
  });
});

describe("review B2-p2a #6: the walk cross-checks directories against what references them", () => {
  async function table(
    shape: {
      groups?: number;
      rows?: number;
      rootRows?: number;
      index?: (pages: readonly GroupPageEntry[]) => GroupPageEntry[];
      table?: Partial<TableRoot>;
      staged?: { firstRowId: number; rowCount: number; groups: number } | null;
      firstRowId?: number;
    } = {},
  ): Promise<{ space: MemoryExtentSpace; bytes: Uint8Array }> {
    const space = new MemoryExtentSpace();
    const writer = space.beginWrite("t");
    const rows = shape.rows ?? 100;
    const directory = async (count: number, first: number) => {
      const blobs = await writer.write(
        Array.from({ length: count }, () =>
          encodeGroupRoot({
            rowCount: shape.rootRows ?? rows,
            rowIds: { kind: "dense" },
            columns: [],
          }),
        ),
      );
      const groups: GroupRef[] = blobs.blobs.map((blob, k) => ({
        groupId: k + 1,
        firstRowId: first + k * rows,
        lastRowId: first + (k + 1) * rows - 1,
        rowCount: rows,
        liveRowCount: rows - 1,
        root: blob,
        deletions: null,
      }));
      const written = await writeGroupDirectory(writer, groups);
      if (shape.index === undefined) return written.ref;
      const index = (await writer.write([encodeGroupIndex(shape.index(written.pages))])).blobs[0];
      if (index === undefined) throw new Error("not written");
      return index;
    };
    const count = shape.groups ?? 3;
    const groups = await directory(count, shape.firstRowId ?? 1);
    const staged = shape.staged ?? null;
    const tableRoot: TableRoot = {
      tableId: 1,
      schemaVersion: 1,
      nextRowId: (count + (staged?.groups ?? 0)) * rows + 1,
      flushedThroughLsn: 0,
      liveRowCount: count * (rows - 1),
      groups,
      groupCount: count,
      staged:
        staged === null
          ? []
          : [
              {
                transaction: 1,
                firstRowId: staged.firstRowId,
                rowCount: staged.rowCount,
                groups: await directory(staged.groups, count * rows + 1),
                indexRuns: [],
              },
            ],
      keyIndex: null,
      secondaryIndexes: [],
      statistics: null,
      ...shape.table,
    };
    const catalog = (await writer.write([new Uint8Array(4)])).blobs[0] ?? ref;
    return { space, bytes: encodeManifestRoot({ ...root, catalog, tables: [tableRoot] }) };
  }

  async function walk(built: { space: MemoryExtentSpace; bytes: Uint8Array }): Promise<number> {
    let count = 0;
    for await (const reached of reachableExtents()(built.bytes, built.space, UNLIMITED)) {
      void reached;
      count++;
    }
    return count;
  }

  it("walks a consistent table, staged rows included", async () => {
    expect(await walk(await table())).toBeGreaterThan(0);
    expect(
      await walk(await table({ staged: { firstRowId: 301, rowCount: 200, groups: 2 } })),
    ).toBeGreaterThan(0);
  });

  it("refuses the probe: an index entry whose range and counts the page contradicts", async () => {
    const space = new MemoryExtentSpace();
    const writer = space.beginWrite("r");
    const gr = (
      await writer.write([encodeGroupRoot({ rowCount: 1, rowIds: { kind: "dense" }, columns: [] })])
    ).blobs[0];
    if (gr === undefined) throw new Error("not written");
    const page = encodeGroupPage([
      {
        groupId: 1,
        firstRowId: 100,
        lastRowId: 100,
        rowCount: 1,
        liveRowCount: 1,
        root: gr,
        deletions: null,
      },
    ]);
    const pg = (await writer.write([page])).blobs[0];
    if (pg === undefined) throw new Error("not written");
    const index = encodeGroupIndex([
      { ref: pg, firstRowId: 1, lastRowId: 2, groupCount: 2, rowCount: 2, liveRowCount: 2 },
    ]);
    const idx = (await writer.write([index])).blobs[0];
    if (idx === undefined) throw new Error("not written");
    const bad: ManifestRoot = {
      ...root,
      tables: [
        {
          tableId: 1,
          schemaVersion: 1,
          nextRowId: 3,
          flushedThroughLsn: 0,
          liveRowCount: 2,
          groups: idx,
          groupCount: 2,
          staged: [],
          keyIndex: null,
          secondaryIndexes: [],
          statistics: null,
        },
      ],
    };
    await expect(walk({ space, bytes: encodeManifestRoot(bad) })).rejects.toThrow(
      /index entry disagrees with its page/u,
    );
  });

  const lying: Array<[string, Parameters<typeof table>[0], RegExp]> = [
    [
      "an index entry's group count",
      { index: (p) => p.map((e) => ({ ...e, groupCount: e.groupCount + 1 })) },
      /index entry/u,
    ],
    [
      "an index entry's row total",
      { index: (p) => p.map((e) => ({ ...e, rowCount: e.rowCount - 1 })) },
      /index entry/u,
    ],
    [
      "an index entry's live rows",
      { index: (p) => p.map((e) => ({ ...e, liveRowCount: e.liveRowCount + 1 })) },
      /index entry/u,
    ],
    [
      "an index entry's first row id",
      {
        index: (p) => p.map((e) => ({ ...e, firstRowId: 1, lastRowId: e.lastRowId })),
        firstRowId: 5,
      },
      /index entry/u,
    ],
    [
      "an index entry's last row id",
      { index: (p) => p.map((e) => ({ ...e, lastRowId: e.lastRowId + 5 })) },
      /index entry/u,
    ],
    ["the table's group count", { table: { groupCount: 4 } }, /group count/u],
    ["the table's live row count", { table: { liveRowCount: 1 } }, /live row count/u],
    ["the table's next row id", { table: { nextRowId: 300 } }, /next row id/u],
    ["a group root's row count", { rootRows: 99 }, /group root's row count/u],
    [
      "an empty directory's group count",
      { table: { groups: EMPTY, groupCount: 1, liveRowCount: 0 } },
      /group count/u,
    ],
    [
      "an empty directory's live rows",
      { table: { groups: EMPTY, groupCount: 0, liveRowCount: 5 } },
      /live row count/u,
    ],
    [
      "a staged directory's row count",
      { staged: { firstRowId: 301, rowCount: 150, groups: 2 } },
      /Staged row count/u,
    ],
    [
      "a staged directory's first row id",
      { staged: { firstRowId: 302, rowCount: 200, groups: 2 } },
      /Staged rows before/u,
    ],
  ];
  it.each(lying)("refuses a lie about %s", async (_, shape, message) => {
    await expect(walk(await table(shape))).rejects.toThrow(message);
  });

  it("refuses a dense group whose row-id range is not its row count", async () => {
    const space = new MemoryExtentSpace();
    const writer = space.beginWrite("r");
    const gr = (
      await writer.write([
        encodeGroupRoot({ rowCount: 10, rowIds: { kind: "dense" }, columns: [] }),
      ])
    ).blobs[0];
    if (gr === undefined) throw new Error("not written");
    const group: GroupRef = {
      groupId: 1,
      firstRowId: 1,
      lastRowId: 20,
      rowCount: 10,
      liveRowCount: 10,
      root: gr,
      deletions: null,
    };
    const directory = await writeGroupDirectory(writer, [group]);
    const table: TableRoot = {
      tableId: 1,
      schemaVersion: 1,
      nextRowId: 21,
      flushedThroughLsn: 0,
      liveRowCount: 10,
      groups: directory.ref,
      groupCount: 1,
      staged: [],
      keyIndex: null,
      secondaryIndexes: [],
      statistics: null,
    };
    await expect(
      walk({ space, bytes: encodeManifestRoot({ ...root, catalog: gr, tables: [table] }) }),
    ).rejects.toThrow(/dense group's row-id range/u);
  });
});

describe("review B2-p2a #7: decoders accept only canonical bytes", () => {
  /** A directory page of one-row groups; group k references extent table entry `refs[k]`. */
  function page(extents: Array<[number, number]>, refs: number[]): Uint8Array {
    const out = new ByteWriter();
    out.u32(MAGIC.page);
    out.u8(1);
    out.uint(extents.length);
    for (const [offset, length] of extents) {
      out.uint(offset);
      out.uint(length);
    }
    out.uint(refs.length);
    refs.forEach((extent, k) => {
      for (const n of [k + 1, k + 1, 0, 1, 1, extent, 0, 10]) out.uint(n);
      out.u32(0);
      out.u8(0);
    });
    return out.finish();
  }

  it("decodes a canonical page to the bytes it re-encodes", () => {
    const bytes = page(
      [
        [0, 4096],
        [8192, 4096],
      ],
      [0, 1, 0],
    );
    expect(encodeGroupPage(decodeGroupPage(bytes, UNLIMITED).value)).toEqual(bytes);
  });

  it("refuses duplicate (the probe), unused and out-of-order extent table entries", () => {
    expect(() =>
      decodeGroupPage(
        page(
          [
            [0, 4096],
            [0, 4096],
          ],
          [0, 1],
        ),
        UNLIMITED,
      ),
    ).toThrow(/appears twice/u);
    expect(() =>
      decodeGroupPage(
        page(
          [
            [0, 4096],
            [8192, 4096],
          ],
          [0, 0],
        ),
        UNLIMITED,
      ),
    ).toThrow(/never referenced/u);
    expect(() =>
      decodeGroupPage(
        page(
          [
            [0, 4096],
            [8192, 4096],
          ],
          [1, 0],
        ),
        UNLIMITED,
      ),
    ).toThrow(/first-use order/u);
  });

  it("refuses duplicate, unused and out-of-order type table entries", () => {
    const chunk = (columnId: number, type: ColumnChunk["type"]): ColumnChunk => ({
      columnId,
      type,
      data: { extent: { offset: 0, length: 4096 }, offset: 0, length: 100 },
      directory: { extent: { offset: 0, length: 4096 }, offset: 100, length: 10, checksum: 0 },
      statistics: { nullCount: 0, min: null, max: null, dictionarySize: null },
      distinctSketch: null,
    });
    const bytes = encodeGroupRoot({
      rowCount: 1,
      rowIds: { kind: "dense" },
      columns: [chunk(1, { kind: "int32", bits: 32 }), chunk(2, { kind: "float64" })],
    });
    // Find the type table: after the header, the extent table, the row count and row-id kind.
    const reader = new ByteReader(bytes);
    reader.u32();
    reader.u8();
    const extents = reader.uint();
    for (let k = 0; k < extents * 2; k++) reader.uint();
    reader.uint();
    reader.u8();
    const countAt = reader.at;
    expect(reader.uint()).toBe(2);
    const int32 = Array.from(bytes.subarray(countAt + 1, countAt + 3));
    const float64 = [bytes[countAt + 3] ?? 0];
    const head = Array.from(bytes.subarray(0, countAt));
    const tail = Array.from(bytes.subarray(countAt + 4));
    const withTypes = (count: number, types: number[]): Uint8Array =>
      Uint8Array.from([...head, count, ...types, ...tail]);
    expect(
      decodeGroupRoot(withTypes(2, [...int32, ...float64]), UNLIMITED).value.columns,
    ).toHaveLength(2);
    // The two column records differ only in column id and type index (both one byte): swap the
    // indices, so the first column references entry 1 before entry 0 is used.
    const record = (tail.length - 1) / 2;
    expect([tail[0], tail[1], tail[2], tail[1 + record], tail[2 + record]]).toEqual([
      2, 1, 0, 2, 1,
    ]);
    const swapped = [...tail];
    swapped[2] = 1;
    swapped[2 + record] = 0;
    expect(() =>
      decodeGroupRoot(Uint8Array.from([...head, 2, ...int32, ...float64, ...swapped]), UNLIMITED),
    ).toThrow(/first-use order/u);
    expect(() => decodeGroupRoot(withTypes(2, [...int32, ...int32]), UNLIMITED)).toThrow(
      /appears twice/u,
    );
    expect(() =>
      decodeGroupRoot(withTypes(3, [...int32, ...float64, int32[0] ?? 0, 16]), UNLIMITED),
    ).toThrow(/never used/u);
  });

  it("refuses non-minimal bigints (the probe) and varints", () => {
    const bigint = (bytes: number[]): bigint => new ByteReader(Uint8Array.from(bytes)).bigint();
    expect(() => bigint([2, 1, 0])).toThrow(/not minimal/u);
    expect(() => bigint([2, 0x80, 0xff])).toThrow(/not minimal/u); // −128 in two bytes
    expect(bigint([2, 0x80, 0])).toBe(128n);
    expect(bigint([1, 0x80])).toBe(-128n);
    for (const value of [0n, -1n, 127n, -129n, (1n << 127n) - 1n, -(1n << 127n)]) {
      const out = new ByteWriter();
      out.bigint(value);
      expect(bigint(Array.from(out.finish()))).toBe(value);
    }
    for (const bytes of [
      [0x80, 0],
      [0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x10],
    ])
      expect(() => new ByteReader(Uint8Array.from(bytes)).uint()).toThrow(ManifestCorruptionError);
  });
});
