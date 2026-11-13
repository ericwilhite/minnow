/**
 * The manifest codecs (format/manifest-codec.ts), the metadata reader and the directory cache
 * end to end over the in-memory extent space: real chunks, group roots, paged group directories
 * and roots, walked by the real `reachableExtents`.
 */
import { describe, expect, it } from "vitest";
import { BlobCache } from "../format/blob-cache.js";
import {
  decodeGroupIndex,
  encodeGroupRoot,
  encodeManifestRoot,
  GROUPS_PER_PAGE,
  writeGroupDirectory,
  type GroupPageEntry,
} from "../format/manifest-codec.js";
import type { BlobRef, GroupRef, GroupRoot, ManifestRoot, TableRoot } from "../format/manifest.js";
import { MetadataReader, manifestReader } from "../format/metadata-reader.js";
import { reachable, reachableExtents } from "../format/reachable.js";
import type { ByteBudget, CacheMemory, ExtentRef } from "../storage/extents.js";
import type { ChunkDirectory } from "./directory.js";
import { PageCorruptionError } from "./format.js";
import { MemoryExtentSpace, UNLIMITED } from "./memory-extents.js";
import { ChunkReader } from "./reader.js";
import { defined, readChunk, vectorOf } from "./test-support.js";
import { GroupWriter } from "./writer.js";

const encoder = new TextEncoder();

function tracked(): ByteBudget & { held: number } {
  return {
    held: 0,
    allocate(bytes: number): boolean {
      this.held += bytes;
      return true;
    },
    free(bytes: number): void {
      this.held -= bytes;
    },
  };
}

function cacheMemory(): CacheMemory & { charged: number } {
  return {
    charged: 0,
    charge(bytes: number): boolean {
      this.charged += bytes;
      return true;
    },
    release(bytes: number): void {
      this.charged -= bytes;
    },
  };
}

const extentKey = (extent: ExtentRef): string =>
  `${String(extent.offset)}:${String(extent.length)}`;

/** One table of `groups` real groups (int64 + text), written through `space`'s writer. */
async function writeTable(space: MemoryExtentSpace, groups: number, rows: number) {
  const writer = space.beginWrite("table");
  const refs: GroupRef[] = [];
  const roots: GroupRoot[] = [];
  for (let g = 0; g < groups; g++) {
    const group = new GroupWriter({
      columns: [
        { columnId: 1, type: { kind: "int64" } },
        { columnId: 2, type: { kind: "text" } },
      ],
      writer,
    });
    const first = g * rows;
    await group.append(
      [
        vectorOf(
          { kind: "int64" },
          Array.from({ length: rows }, (_, i) => BigInt(first + i)),
        ),
        vectorOf(
          { kind: "text" },
          Array.from({ length: rows }, (_, i) => encoder.encode(`v${String((first + i) % 13)}`)),
        ),
      ],
      rows,
    );
    const written = await group.finish();
    roots.push({ rowCount: rows, rowIds: { kind: "dense" }, columns: written.columns });
  }
  // Group roots are small metadata blobs: packed into one extent.
  const rootBlobs = await writer.write(roots.map((root) => encodeGroupRoot(root)));
  for (let g = 0; g < groups; g++) {
    const root = defined(rootBlobs.blobs[g]);
    refs.push({
      groupId: g + 1,
      firstRowId: g * rows + 1,
      lastRowId: (g + 1) * rows,
      rowCount: rows,
      liveRowCount: rows,
      root,
      deletions: null,
    });
  }
  const directory = await writeGroupDirectory(writer, refs);
  return { writer, refs, roots, directory };
}

function table(tableId: number, groups: BlobRef, groupCount: number, rows = 0): TableRoot {
  return {
    tableId,
    schemaVersion: 1,
    nextRowId: groupCount * rows + 1,
    flushedThroughLsn: 0,
    liveRowCount: groupCount * rows,
    groups,
    groupCount,
    staged: [],
    keyIndex: null,
    secondaryIndexes: [],
    statistics: null,
  };
}

describe("manifest metadata end to end", () => {
  it("the open walk reaches exactly the extents a root's writes sealed", async () => {
    const space = new MemoryExtentSpace();
    const { writer, directory } = await writeTable(space, 5, 3000);
    const catalog = defined((await writer.write([encoder.encode('{"tables":[]}')])).blobs[0]);
    const empty: BlobRef = { extent: { offset: 0, length: 0 }, offset: 0, length: 0, checksum: 0 };
    const root: ManifestRoot = {
      formatVersion: 3,
      sequence: 1,
      epoch: 1,
      log: { salt: 7, firstLsn: 1, rootSequence: 1 },
      globalLsn: 10,
      catalogVersion: 1,
      catalog,
      nextTableId: 3,
      sequences: [{ sequenceId: 1, reservedThrough: 100n }],
      tables: [table(1, directory.ref, 5, 3000), table(2, empty, 0)],
      clients: [],
      extentList: [],
    };
    const budget = tracked();
    const yielded = new Set<string>();
    const walk = reachableExtents();
    for await (const ref of walk(encodeManifestRoot(root), space, budget))
      yielded.add(extentKey("extent" in ref ? ref.extent : ref));
    expect(yielded).toEqual(new Set(writer.extents.map(extentKey)));
    expect(budget.held).toBe(0); // every parse charge and lease returned
    expect(space.leased).toBe(0);
  });

  it("appends to a paged directory by rewriting only its last page", async () => {
    const space = new MemoryExtentSpace();
    const writer = space.beginWrite("directory");
    const target = defined((await writer.write([new Uint8Array(64)])).blobs[0]);
    const group = (k: number): GroupRef => ({
      groupId: k,
      firstRowId: k * 100 + 1,
      lastRowId: k * 100 + 100,
      rowCount: 100,
      liveRowCount: 99,
      root: target,
      deletions: k % 3 === 0 ? target : null,
    });
    const groups = Array.from({ length: GROUPS_PER_PAGE * 2 + 500 }, (_, k) => group(k));
    const first = await writeGroupDirectory(writer, groups);
    expect(first.pages.map((page) => page.groupCount)).toEqual([
      GROUPS_PER_PAGE,
      GROUPS_PER_PAGE,
      500,
    ]);
    const before = writer.bytes;
    // A flush: keep the full pages, rewrite the last one with its new groups.
    const lastPage = groups.slice(GROUPS_PER_PAGE * 2);
    const added = Array.from({ length: 10 }, (_, k) => group(groups.length + k));
    const second = await writeGroupDirectory(
      writer,
      [...lastPage, ...added],
      first.pages.slice(0, 2),
    );
    const written = writer.bytes - before;
    expect(second.pages.slice(0, 2)).toEqual(first.pages.slice(0, 2)); // reused as they are
    expect(written).toBeLessThan(3 * 4096 + defined(first.pages[2]).ref.length * 2);
    // Reading it back through the metadata reader gives every group in row-id order.
    const reader = new MetadataReader(space);
    const budget = tracked();
    const index = await reader.groupIndex(second.ref, budget);
    const all: GroupRef[] = [];
    for (const page of index.value) {
      const held = await reader.groupPage(page.ref, budget);
      all.push(...held.value);
      held.release();
    }
    index.release();
    expect(all).toEqual([...groups, ...added]);
    expect(budget.held).toBe(0);
    expect(() => decodeGroupIndex(new Uint8Array(3), budget)).toThrow();
    expect(budget.held).toBe(0);
  });

  it("refuses new groups that do not follow the kept pages", async () => {
    const space = new MemoryExtentSpace();
    const writer = space.beginWrite("directory");
    const target = defined((await writer.write([new Uint8Array(8)])).blobs[0]);
    const keep: GroupPageEntry[] = [
      {
        ref: target,
        firstRowId: 1,
        lastRowId: 100,
        groupCount: 1,
        rowCount: 100,
        liveRowCount: 100,
      },
    ];
    const overlapping: GroupRef = {
      groupId: 2,
      firstRowId: 50,
      lastRowId: 60,
      rowCount: 11,
      liveRowCount: 11,
      root: target,
      deletions: null,
    };
    await expect(writeGroupDirectory(writer, [overlapping], keep)).rejects.toBeInstanceOf(
      RangeError,
    );
  });

  it("caches decoded group roots: a hit is synchronous, pinned while held, charged to the cache after", async () => {
    const space = new MemoryExtentSpace();
    const { refs, roots } = await writeTable(space, 2, 1000);
    const memory = cacheMemory();
    const groupRoots = new BlobCache<GroupRoot>(() => memory);
    const reader = new MetadataReader(space.view({ cache: false }), { groupRoots });
    const budget = tracked();
    const ref = defined(refs[0]).root;
    const cold = reader.groupRoot(ref, budget);
    expect(cold).toBeInstanceOf(Promise);
    const first = await cold;
    expect(first.value).toEqual(roots[0]);
    expect(budget.held).toBeGreaterThan(0);
    first.release();
    expect(budget.held).toBe(0);
    expect(memory.charged).toBe(groupRoots.residentBytes);
    const warm = reader.groupRoot(ref, budget);
    if (warm instanceof Promise) throw new Error("a cached group root reads synchronously");
    expect(warm.value).toBe(first.value);
    expect(memory.charged).toBe(0); // pinned: charged to the holder, not the cache
    warm.release();
    expect(groupRoots.evict(Number.POSITIVE_INFINITY)).toBeGreaterThan(0);
    expect(memory.charged).toBe(0);
    // The walk works over the same reader (a ManifestReader).
    const found = await reachable(
      {
        formatVersion: 3,
        sequence: 1,
        epoch: 1,
        log: { salt: 1, firstLsn: 1, rootSequence: 1 },
        globalLsn: 1,
        catalogVersion: 1,
        catalog: ref,
        nextTableId: 2,
        sequences: [],
        tables: [],
        clients: [],
        extentList: [],
      },
      manifestReader(reader, budget),
    );
    expect(found.length).toBe(1);
  });
});

describe("the chunk directory cache (R15.7)", () => {
  it("parses a directory once; later opens are synchronous hits, charged while held", async () => {
    const space = new MemoryExtentSpace();
    const { roots } = await writeTable(space, 1, 20_000);
    const chunk = defined(defined(roots[0]).columns[1]);
    const memory = cacheMemory();
    const directories = new BlobCache<ChunkDirectory>(() => memory);
    const cold = space.view({ cache: false });
    const budget = tracked();
    const first = await ChunkReader.open(cold, chunk, { budget, directories });
    expect(directories.misses).toBe(1);
    const held = budget.held;
    expect(held).toBeGreaterThan(0);
    // A second reader on a space with nothing cached still opens synchronously: the directory is
    // parsed already.
    const second = ChunkReader.open(cold, chunk, { budget, directories });
    if (second instanceof Promise) throw new Error("a cached directory opens synchronously");
    expect(second.directory).toBe(first.directory);
    expect(directories.hits).toBe(1);
    expect(budget.held).toBe(held * 2);
    first.close();
    second.close();
    expect(budget.held).toBe(0);
    expect(memory.charged).toBe(directories.residentBytes);
    const values = await readChunk(space, chunk);
    expect(values.length).toBe(20_000);
    // A chunk of another type naming the same directory is refused, and the entry stays unpinned.
    const wrong = { ...chunk, type: { kind: "int64" } as const };
    expect(() => ChunkReader.open(cold, wrong, { budget, directories })).toThrow(
      PageCorruptionError,
    );
    expect(budget.held).toBe(0);
    expect(memory.charged).toBe(directories.residentBytes);
    directories.clear();
    expect(directories.residentBytes).toBe(0);
    expect(memory.charged).toBe(0);
  });

  it("a refused cache charge drops the entry; a refused budget holds nothing", async () => {
    const space = new MemoryExtentSpace();
    const { roots } = await writeTable(space, 1, 5000);
    const chunk = defined(defined(roots[0]).columns[0]);
    const refusing: CacheMemory = { charge: () => false, release: () => undefined };
    const directories = new BlobCache<ChunkDirectory>(() => refusing);
    const reader = await ChunkReader.open(space, chunk, { budget: UNLIMITED, directories });
    reader.close();
    expect(directories.entryCount).toBe(0);
    const memory = cacheMemory();
    const kept = new BlobCache<ChunkDirectory>(() => memory);
    (await ChunkReader.open(space, chunk, { budget: UNLIMITED, directories: kept })).close();
    const none: ByteBudget = { allocate: () => false, free: () => undefined };
    expect(() => ChunkReader.open(space, chunk, { budget: none, directories: kept })).toThrow();
    expect(memory.charged).toBe(kept.residentBytes); // the hit was unpinned again
  });
});
