/**
 * Parsed page descriptors riding on space-cache entries (reader.ts `PAGE_DESCRIPTOR_BYTES`,
 * storage/extents.ts `ExtentReader.parsed` / `attach`): a page is validated once per cache
 * residency, the descriptor is charged in its entry and dropped with it, and a descriptor is
 * reused only for the directory fields it was checked against.
 */
import { describe, expect, it } from "vitest";
import type { ColumnChunk } from "../format/manifest.js";
import { MemoryBackend } from "../storage/memory-store.js";
import { ENTRY_OVERHEAD } from "../storage/page-cache.js";
import { countingMemory, openOver } from "../storage/space-test-support.js";
import { encodeDirectory } from "./directory.js";
import { PageCorruptionError } from "./format.js";
import { PageEncoding } from "../format/manifest.js";
import { UNLIMITED } from "./memory-extents.js";
import { ChunkReader, PAGE_DESCRIPTOR_BYTES } from "./reader.js";
import { defined, targetFor, valueAt, vectorOf, writeGroup } from "./test-support.js";
import { GroupWriter } from "./writer.js";

const values = Array.from({ length: 20_000 }, (_, k) =>
  k % 9 === 0 ? null : (k * 7919) % 100_000,
);

async function readAll(reader: ChunkReader): Promise<unknown[]> {
  await reader.load(reader.directory.pages.map((_, page) => page));
  const target = targetFor(reader, reader.rowCount, 0);
  reader.decodeRange(0, reader.rowCount, target, 0);
  return Array.from({ length: reader.rowCount }, (_, row) => valueAt(target, row));
}

describe("parsed page descriptors on cache entries", () => {
  it("parses a page once per residency: the next reader reuses its descriptor", async () => {
    const { space, group } = await writeGroup([{ type: { kind: "int32", bits: 32 }, values }]);
    const chunk = defined(group.columns[0]);
    const first = await ChunkReader.open(space, chunk, { budget: UNLIMITED });
    const pages = first.directory.pages.length;
    expect(await readAll(first)).toEqual(values);
    first.close();
    expect([space.attached, space.parsedHits]).toEqual([pages, 0]);
    const second = await ChunkReader.open(space, chunk, { budget: UNLIMITED });
    expect(await readAll(second)).toEqual(values);
    second.close();
    expect([space.attached, space.parsedHits]).toEqual([pages, pages]);
    // Evicted with the entry: the next reader parses (and attaches) again.
    space.evict();
    const third = await ChunkReader.open(space, chunk, { budget: UNLIMITED });
    expect(await readAll(third)).toEqual(values);
    third.close();
    expect(space.attached).toBe(2 * pages);
  });

  it("does not trust a descriptor checked against other directory fields", async () => {
    const { space, group } = await writeGroup([{ type: { kind: "int32", bits: 32 }, values }]);
    const chunk = defined(group.columns[0]);
    const honest = await ChunkReader.open(space, chunk, { budget: UNLIMITED });
    await readAll(honest);
    const directory = honest.directory;
    honest.close();
    // The same page blobs under a directory that lies about a null count.
    const lying = encodeDirectory({
      ...directory,
      pages: directory.pages.map((page, index) =>
        index === 0 ? { ...page, nullCount: page.nullCount + 1 } : page,
      ),
    });
    const blob = defined((await space.beginWrite("forged").write([lying])).blobs[0]);
    const forged: ColumnChunk = { ...chunk, directory: blob };
    const reader = await ChunkReader.open(space, forged, { budget: UNLIMITED });
    const hits = space.parsedHits;
    // Cached: the refusal is synchronous, like the hit it replaces.
    await expect((async () => reader.load([0]))()).rejects.toThrow(PageCorruptionError);
    expect(space.parsedHits).toBe(hits + 1); // found, refused, parsed afresh
    reader.close();
  });

  it("does not reuse a descriptor checked against a larger dictionary (review B2-p2d #1)", async () => {
    const encoder = new TextEncoder();
    const text = Array.from({ length: 4000 }, (_, row) =>
      encoder.encode(row % 2 === 1 ? "b" : "a"),
    );
    const { space, group } = await writeGroup([{ type: { kind: "text" }, values: text }]);
    const chunk = defined(group.columns[0]);
    const honest = await ChunkReader.open(space, chunk, { budget: UNLIMITED });
    await honest.load([0]);
    const directory = honest.directory;
    honest.close();
    expect(directory.pages[0]?.encoding).toBe(PageEncoding.dictionary);
    // The same pages under a directory claiming a one-entry dictionary: code 1 is outside it.
    const dictionary = defined(directory.dictionary ?? undefined);
    const smaller = encodeDirectory({ ...directory, dictionary: { ...dictionary, rowCount: 1 } });
    const blob = defined((await space.beginWrite("smaller").write([smaller])).blobs[0]);
    const reader = await ChunkReader.open(
      space,
      { ...chunk, directory: blob },
      { budget: UNLIMITED },
    );
    const hits = space.parsedHits;
    await expect((async () => reader.load([0]))()).rejects.toThrow(/outside the dictionary/u);
    expect(space.parsedHits).toBe(hits + 1); // found, refused for its dictionary size, parsed afresh
    reader.close();
  });

  it("charges the descriptor in its cache entry, and eviction returns all of it", async () => {
    const memory = countingMemory(Number.POSITIVE_INFINITY);
    const { space, budget } = await openOver(new MemoryBackend(), { memory });
    const writer = space.beginWrite("descriptors");
    const group = new GroupWriter({
      columns: [{ columnId: 1, type: { kind: "int32", bits: 32 } }],
      writer,
      inlineBytes: 0,
    });
    await group.append([vectorOf({ kind: "int32", bits: 32 }, values)], values.length);
    const chunk = defined((await group.finish()).columns[0]);
    const baseline = memory.used;
    // Cold: pages come through a lease (pinned while parsed): charged when the lease goes.
    const reader = await ChunkReader.open(space, chunk, { budget });
    expect(await readAll(reader)).toEqual(values);
    reader.close();
    const pages = reader.directory.pages;
    const stored = pages.reduce((sum, page) => sum + page.length, 0);
    const directoryBlob = chunk.directory.length + ENTRY_OVERHEAD;
    expect(memory.used - baseline).toBe(
      stored + pages.length * (ENTRY_OVERHEAD + PAGE_DESCRIPTOR_BYTES) + directoryBlob,
    );
    // Warm: peeked and reused; nothing more is charged.
    const again = await ChunkReader.open(space, chunk, { budget });
    expect(await readAll(again)).toEqual(values);
    again.close();
    expect(memory.used - baseline).toBe(
      stored + pages.length * (ENTRY_OVERHEAD + PAGE_DESCRIPTOR_BYTES) + directoryBlob,
    );
    const cache = defined(memory.cache ?? undefined);
    expect(cache.evictableBytes).toBe(memory.used - baseline);
    cache.evict(Number.POSITIVE_INFINITY);
    expect(memory.used).toBe(baseline);
    expect(budget.used).toBe(0);
  });

  it("charges a descriptor attached to an unpinned entry at once, in its list's bytes", async () => {
    const memory = countingMemory(Number.POSITIVE_INFINITY);
    const { space, budget } = await openOver(new MemoryBackend(), { memory });
    const writer = space.beginWrite("unpinned");
    const group = new GroupWriter({
      columns: [{ columnId: 1, type: { kind: "int32", bits: 32 } }],
      writer,
      inlineBytes: 0,
    });
    await group.append([vectorOf({ kind: "int32", bits: 32 }, values)], values.length);
    const chunk = defined((await group.finish()).columns[0]);
    const probe = await ChunkReader.open(space, chunk, { budget });
    const pages = probe.directory.pages;
    probe.close();
    // Cache the pages without a reader: resident, unpinned, no descriptor.
    const refs = pages.map((page) => ({
      extent: chunk.data.extent,
      offset: chunk.data.offset + page.offset,
      length: page.length,
      checksum: page.checksum,
    }));
    (await space.read(refs, { budget })).release();
    const cache = defined(memory.cache ?? undefined);
    const before = memory.used;
    const evictable = cache.evictableBytes;
    // A reader now peeks them and attaches to unpinned entries.
    const reader = await ChunkReader.open(space, chunk, { budget });
    expect(await readAll(reader)).toEqual(values);
    reader.close();
    expect(memory.used - before).toBe(pages.length * PAGE_DESCRIPTOR_BYTES);
    expect(cache.evictableBytes - evictable).toBe(pages.length * PAGE_DESCRIPTOR_BYTES);
    const charged = cache.evictableBytes;
    expect(cache.evict(Number.POSITIVE_INFINITY)).toBe(charged);
    expect(memory.used).toBe(before - evictable);
  });

  it("states a bound the measured descriptors fit", () => {
    expect(PAGE_DESCRIPTOR_BYTES).toBe(768);
  });
});
