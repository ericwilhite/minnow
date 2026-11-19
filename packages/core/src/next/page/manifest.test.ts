import { writeTestCatalog } from "../format/catalog-test-support.js";
import { MANIFEST_FORMAT_VERSION } from "../durable-format.js";
/**
 * The manifest codecs (format/manifest-codec.ts), the metadata reader and the directory cache
 * end to end over the in-memory extent space: real chunks, group roots, paged group directories
 * and roots, walked by the real `reachableExtents`.
 */
import { describe, expect, it, vi } from "vitest";
import fc from "fast-check";
import { seedsFor } from "../../testing/seeds.js";
import {
  createReach,
  DIRECTORY_CACHE_HIT_BUDGET_REACH,
  DIRECTORY_CACHE_PAYLOAD_REACH,
} from "../../testing/reach.js";
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
import { ChunkReader, PageBudgetError, DECODE_MEMO_BYTES } from "./reader.js";
import * as directoryCode from "./directory.js";
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
    const catalog = await writeTestCatalog(writer);
    const empty: BlobRef = { extent: { offset: 0, length: 0 }, offset: 0, length: 0, checksum: 0 };
    const root: ManifestRoot = {
      formatVersion: MANIFEST_FORMAT_VERSION,
      databaseId: 1n,
      fts: { checkpoint: null, staged: null },
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
        formatVersion: MANIFEST_FORMAT_VERSION,
        databaseId: 1n,
        fts: { checkpoint: null, staged: null },
        sequence: 1,
        epoch: 1,
        log: { salt: 1, firstLsn: 1, rootSequence: 1 },
        globalLsn: 1,
        catalogVersion: 1,
        catalog: await writeTestCatalog(space.beginWrite("catalog walk fixture")),
        nextTableId: 2,
        sequences: [],
        tables: [],
        clients: [],
        extentList: [],
      },
      manifestReader(reader, budget),
    );
    expect(found.length).toBe(2); // native catalog root and its payload page
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

for (const seed of seedsFor("next-directory-hit-budget", [0xb505, 0xb506]))
  it(`returns a failed hit's pin for refused and throwing budgets seed ${String(seed)}`, async () => {
    const reach = createReach("directory hit funding", DIRECTORY_CACHE_HIT_BUDGET_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 64, max: 257 }),
        fc.boolean(),
        fc.boolean(),
        async (rows, once, throws) => {
          reach.beginHistory();
          const space = new MemoryExtentSpace(),
            { roots } = await writeTable(space, 1, rows),
            chunk = defined(defined(roots[0]).columns[0]),
            memory = cacheMemory(),
            directories = new BlobCache<ChunkDirectory>(() => memory),
            funded = tracked();
          (await ChunkReader.open(space, chunk, { budget: funded, directories })).close();
          const failed = new Error("Directory holder funding rejected");
          let allocations = 0,
            frees = 0,
            error: unknown,
            unexpected: ChunkReader | undefined;
          const budget: ByteBudget = {
            allocate() {
              allocations++;
              if (throws) throw failed;
              return false;
            },
            free() {
              frees++;
            },
          };
          try {
            try {
              unexpected = await ChunkReader.open(space, chunk, {
                budget,
                directories,
                ...(once ? { once: true } : {}),
              });
            } catch (cause) {
              error = cause;
            }
            if (throws) expect(error).toBe(failed);
            else expect(error).toBeInstanceOf(PageBudgetError);
            expect(allocations).toBe(1);
            expect(frees).toBe(0);
            expect(directories.evictableBytes, "failed cache hit must return its pin").toBe(
              directories.residentBytes,
            );
            expect(memory.charged).toBe(directories.residentBytes);
            expect(funded.held).toBe(0);
            reach.reach(throws ? "throwing holder budget" : "refused holder budget");
            reach.reach(once ? "once pin returned" : "ordinary pin returned");
            reach.reach("failed hit remains evictable");
            reach.reach("holder credit unchanged");
          } finally {
            unexpected?.close();
            // Test-owned disposal for the parent/mutant's leaked pin, AFTER the law
            // assertion. Never repair it before observing the failure.
            if (directories.evictableBytes < directories.residentBytes)
              directories.unpin(chunk.directory, once ? { once: true } : {});
            directories.clear();
          }
          expect(memory.charged).toBe(0);
          expect(space.leased).toBe(0);
          reach.reach("all fixture credit returned");
        },
      ),
      {
        seed,
        numRuns: 40,
        examples: [
          [128, false, true],
          [256, true, true],
          [64, false, false],
          [129, true, false],
        ],
      },
    );
    reach.assertReached();
  }, 180_000);

for (const seed of seedsFor("next-directory-payload-budget", [0xb507, 0xb508]))
  it(`funds copied directory payload before parsing and while retained seed ${String(seed)}`, async () => {
    const reach = createReach("directory payload funding", DIRECTORY_CACHE_PAYLOAD_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.boolean(),
        fc.boolean(),
        fc.constantFrom("retain", "refuse-valid", "refuse-invalid"),
        fc.integer({ min: 1029, max: 8192 }),
        async (dense, text, mode, invalidLength) => {
          reach.beginHistory();
          const space = new MemoryExtentSpace(),
            { roots } = await writeTable(space, 1, dense ? 640 : 64),
            original = defined(defined(roots[0]).columns[text ? 1 : 0]),
            cold = space.view({ cache: true });
          const initial = await cold.read([original.directory], { budget: UNLIMITED });
          initial.release();
          const encoded = cold.peek(original.directory);
          if (encoded === null) throw new Error("Missing primed directory bytes");
          const parsed = directoryCode.parseDirectory(encoded, original.data.length),
            copied =
              (parsed.sketch?.byteLength ?? 0) +
              parsed.pages.reduce(
                (sum, page) =>
                  sum +
                  (page.min instanceof Uint8Array ? page.min.byteLength : 0) +
                  (page.max instanceof Uint8Array ? page.max.byteLength : 0),
                0,
              ),
            // The descriptor terms are the existing reader contract. Payload copies
            // are additional bytes, not values inferred from a new engine charge.
            descriptors = 256 + 192 * (parsed.pages.length + (parsed.dictionary ? 1 : 0));
          expect(copied).toBeGreaterThan(0);
          if (!text) reach.reach(parsed.sketch?.[1] === 0 ? "dense sketch" : "sparse sketch");
          reach.reach(text ? "byte statistics" : "integer statistics");
          let chunk = original;
          if (mode === "refuse-invalid") {
            const bad = new Uint8Array(invalidLength);
            bad.set([10, 0, 0, 4]); // dense header, but an invalid body length
            const ref = defined(
              (
                await space
                  .beginWrite("bad sketch funding")
                  .write([directoryCode.encodeDirectory({ ...parsed, sketch: bad })])
              ).blobs[0],
            );
            const primed = await cold.read([ref], { budget: UNLIMITED });
            primed.release();
            chunk = { ...original, directory: ref };
          }
          const memory = cacheMemory(),
            directories = new BlobCache<ChunkDirectory>(() => memory),
            holder = tracked(),
            parse = vi.spyOn(directoryCode, "parseDirectory");
          let reader: ChunkReader | undefined, error: unknown;
          const budget: ByteBudget =
            mode === "retain"
              ? holder
              : {
                  allocate(bytes) {
                    if (holder.held + bytes > descriptors + DECODE_MEMO_BYTES) return false;
                    return holder.allocate(bytes);
                  },
                  free: (bytes) => holder.free(bytes),
                };
          try {
            try {
              reader = await ChunkReader.open(cold, chunk, { budget, directories });
            } catch (cause) {
              error = cause;
            }
            if (mode === "retain") {
              expect(error).toBeUndefined();
              expect(reader).toBeDefined();
              reader?.close();
              reader = undefined;
              expect(
                memory.charged,
                "retained directory must fund copied payload",
              ).toBeGreaterThanOrEqual(descriptors + copied);
              reach.reach("retained payload funded");
            } else {
              expect(error, "unfunded directory payload must refuse before parse").toBeInstanceOf(
                PageBudgetError,
              );
              expect(parse.mock.calls.length, "no payload parse before funding").toBe(0);
              expect(directories.entryCount).toBe(0);
              reach.reach(
                mode === "refuse-valid"
                  ? "valid payload refused before parse"
                  : "invalid payload refused before parse",
              );
            }
          } finally {
            reader?.close();
            parse.mockClear();
            parse.mockRestore();
            directories.clear();
          }
          expect(holder.held).toBe(0);
          expect(memory.charged).toBe(0);
          expect(cold.leased).toBe(0);
          reach.reach("all fixture credit returned");
        },
      ),
      {
        seed,
        numRuns: 40,
        examples: [false, true].flatMap(
          (
            dense,
          ): Array<[boolean, boolean, "retain" | "refuse-valid" | "refuse-invalid", number]> =>
            [false, true].flatMap((text) => [
              [dense, text, "retain", 2048],
              [dense, text, "refuse-valid", 4096],
              [dense, text, "refuse-invalid", 8192],
            ]),
        ),
      },
    );
    reach.assertReached();
  }, 180_000);
