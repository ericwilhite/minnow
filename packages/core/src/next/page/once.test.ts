/**
 * One-pass scans (storage/extents.ts `ReadOptions.once`, ChunkReadOptions.once) and scan
 * readahead (`ChunkReader.pagesAhead`): a full export reads every page once without keeping any
 * of it in the page cache or the directory cache, and without promoting what it hits, so the hot
 * working set stays and the export's memory stays flat.
 */
import { describe, expect, it } from "vitest";
import { BlobCache } from "../format/blob-cache.js";
import type { ColumnChunk } from "../format/manifest.js";
import type { BlobRef } from "../storage/extents.js";
import { MemoryBackend } from "../storage/memory-store.js";
import { countingMemory, openOver } from "../storage/space-test-support.js";
import type { ChunkDirectory } from "./directory.js";
import { UNLIMITED } from "./memory-extents.js";
import { ChunkReader } from "./reader.js";
import { defined, targetFor, valueAt, vectorOf, writeGroup } from "./test-support.js";
import { GroupWriter } from "./writer.js";

const type = { kind: "int32", bits: 32 } as const;
const values = Array.from({ length: 30_000 }, (_, row) =>
  row % 9 === 0 ? null : (row * 7919) % 100_000,
);

async function onDisk() {
  const memory = countingMemory(Number.POSITIVE_INFINITY);
  const { space, budget } = await openOver(new MemoryBackend(), { memory });
  const writer = new GroupWriter({
    columns: [{ columnId: 1, type }],
    writer: space.beginWrite("once"),
    inlineBytes: 0,
  });
  await writer.append([vectorOf(type, values)], values.length);
  const chunk: ColumnChunk = defined((await writer.finish()).columns[0]);
  const probe = await ChunkReader.open(space, chunk, { budget, once: true });
  const pages = probe.directory.pages;
  probe.close();
  const refs: BlobRef[] = pages.map((page) => ({
    extent: chunk.data.extent,
    offset: chunk.data.offset + page.offset,
    length: page.length,
    checksum: page.checksum,
  }));
  return { space, budget, memory, chunk, pages, refs };
}

describe("one-pass scans", () => {
  it("keep nothing they miss in the page cache, and charge only their leases", async () => {
    const { space, budget, memory, refs } = await onDisk();
    const before = memory.used;
    const lease = await space.read(refs, { budget, once: true });
    expect(budget.used).toBe(refs.reduce((sum, ref) => sum + ref.length, 0));
    lease.release();
    expect(refs.every((ref) => space.peek(ref) === null)).toBe(true);
    expect([memory.used, budget.used]).toEqual([before, 0]);
  });

  it("do not promote what they hit: the hot working set keeps its place", async () => {
    const { space, budget, memory, refs } = await onDisk();
    const [scanned, hot] = [defined(refs[0]), defined(refs[1])];
    (await space.read([hot], { budget })).release();
    (await space.read([hot], { budget })).release(); // used twice: protected
    (await space.read([scanned], { budget })).release(); // used once: probation
    // A one-pass scan hits `scanned` twice: an ordinary reader would promote it to protected.
    for (let use = 0; use < 2; use++)
      (await space.read([scanned], { budget, once: true })).release();
    const cache = defined(
      (memory.cache ?? undefined) as { evict(bytes: number): number } | undefined,
    );
    cache.evict(1); // probation goes first: the scanned page, not the hot one
    expect([space.peek(scanned) === null, space.peek(hot) === null]).toEqual([true, false]);
  });

  it("a once reader decodes everything, attaches no descriptors and admits no directory", async () => {
    const { space, budget, memory, chunk, refs } = await onDisk();
    const directories = new BlobCache<ChunkDirectory>(() => ({
      charge: () => true,
      release: () => undefined,
    }));
    const before = memory.used;
    const reader = await ChunkReader.open(space, chunk, { budget, directories, once: true });
    await reader.load(reader.directory.pages.map((_, page) => page));
    const target = targetFor(reader, values.length, 0);
    reader.decodeRange(0, values.length, target, 0);
    expect(Array.from({ length: values.length }, (_, row) => valueAt(target, row))).toEqual(values);
    reader.close();
    expect(directories.entryCount).toBe(0);
    expect(refs.every((ref) => space.parsed?.(ref) == null && space.peek(ref) === null)).toBe(true);
    expect([memory.used - before, budget.used]).toEqual([0, 0]);
    // A directory an ordinary reader cached is still used by a once reader.
    const warm = await ChunkReader.open(space, chunk, { budget, directories });
    warm.close();
    expect(directories.entryCount).toBe(1);
    const hits = directories.hits;
    const again = await ChunkReader.open(space, chunk, { budget, directories, once: true });
    again.close();
    expect(directories.hits).toBe(hits + 1);
  });

  it("are honoured by the in-memory test space too", async () => {
    const { space, group } = await writeGroup([{ type, values }]);
    const chunk = defined(group.columns[0]);
    const reader = await ChunkReader.open(space, chunk, { budget: UNLIMITED, once: true });
    const page = defined(reader.directory.pages[0]);
    await reader.load([0]);
    reader.close();
    expect(
      space.peek({
        extent: chunk.data.extent,
        offset: chunk.data.offset + page.offset,
        length: page.length,
        checksum: page.checksum,
      }),
    ).toBeNull();
  });
});

describe("scan readahead", () => {
  it("names the pages a batch needs and the following ones within the byte budget", async () => {
    const { space, group } = await writeGroup([{ type, values }], {
      pageBytes: 1024,
      rawPageBytes: 1024,
    });
    const reader = await ChunkReader.open(space, defined(group.columns[0]), { budget: UNLIMITED });
    const pages = reader.directory.pages;
    expect(pages.length).toBeGreaterThan(8);
    const [first, end] = reader.pagesInRange(0, 2048);
    expect(reader.pagesAhead(0, 2048, 0)).toEqual(
      Array.from({ length: end - first }, (_, k) => first + k),
    );
    const ahead = reader.pagesAhead(0, 2048, 4096);
    const extra = ahead.slice(end - first);
    expect(extra[0]).toBe(end);
    expect(extra.reduce((sum, page) => sum + defined(pages[page]).length, 0)).toBeLessThanOrEqual(
      4096,
    );
    const next = pages[end + extra.length];
    if (next !== undefined)
      expect(
        extra.reduce((sum, page) => sum + defined(pages[page]).length, 0) + next.length,
      ).toBeGreaterThan(4096);
    expect(reader.pagesAhead(0, values.length, 1 << 30)).toEqual(pages.map((_, page) => page));
    reader.close();
  });
});
