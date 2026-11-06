/**
 * Review B2-p2h (rebuild/format-p2g 919a6cc, rebuild/format-p2h 1e0c691). #1 is
 * storage/page-cache-policy.test.ts and #2's cache side format/blob-cache-policy.test.ts; here,
 * #2 through the reader: a one-pass reader reuses a parsed directory or an inflated page with a
 * one-pass pin, which leaves their eviction order alone.
 */
import { describe, expect, it } from "vitest";
import { BlobCache } from "../format/blob-cache.js";
import { PageEncoding } from "../format/manifest.js";
import type { ChunkDirectory } from "./directory.js";
import { InflatedPageCache } from "./inflated-cache.js";
import { UNLIMITED } from "./memory-extents.js";
import { ChunkReader } from "./reader.js";
import { defined, writeGroup } from "./test-support.js";

const ints = { kind: "int32", bits: 32 } as const;
const encoder = new TextEncoder();

describe("review B2-p2h #2: a one-pass reader's cache hits leave eviction order alone", () => {
  it("reusing a parsed directory (the probe, inverted)", async () => {
    const column = { type: ints, values: Array.from({ length: 4000 }, (_, i) => i) };
    const { space, group } = await writeGroup([column, column]);
    const directories = new BlobCache<ChunkDirectory>(() => ({
      charge: () => true,
      release: () => undefined,
    }));
    const [a, b] = [defined(group.columns[0]), defined(group.columns[1])];
    (await ChunkReader.open(space, a, { budget: UNLIMITED, directories })).close();
    (await ChunkReader.open(space, b, { budget: UNLIMITED, directories })).close();
    (await ChunkReader.open(space, a, { budget: UNLIMITED, directories, once: true })).close();
    directories.evict(1); // the older release, a, goes; b stays
    expect(directories.pin(a.directory, { once: true })).toBeNull();
    expect(directories.pin(b.directory, { once: true })).not.toBeNull();
  });

  it("reusing an inflated gzip page", async () => {
    const text = (salt: number) =>
      Array.from({ length: 3000 }, (_, row) =>
        encoder.encode(`event ${String((row + salt) % 600)} ok`),
      );
    const options = { dictionaryEntries: 1, pageBytes: 64 * 1024, gzip: true } as const;
    const first = await writeGroup([{ type: { kind: "text" }, values: text(0) }], options);
    const second = await writeGroup([{ type: { kind: "text" }, values: text(7) }], {
      ...options,
      space: first.space,
    });
    const inflated = new InflatedPageCache(() => ({
      charge: () => true,
      release: () => undefined,
    }));
    const view = first.space.view({ cache: false });
    const all = (reader: ChunkReader) => reader.directory.pages.map((_, page) => page);
    const [a, b] = [defined(first.group.columns[0]), defined(second.group.columns[0])];
    for (const chunk of [a, b]) {
      const reader = await ChunkReader.open(view, chunk, { budget: UNLIMITED, inflated });
      expect(reader.directory.pages.some((page) => (page.encoding & PageEncoding.gzip) !== 0)).toBe(
        true,
      );
      await reader.load(all(reader));
      reader.close();
    }
    const resident = inflated.residentBytes;
    // A one-pass reader hits a's inflated pages; they keep their place, older than b's.
    const scan = await ChunkReader.open(view, a, { budget: UNLIMITED, inflated, once: true });
    const hits = inflated.hits;
    await scan.load(all(scan));
    scan.close();
    expect(inflated.hits).toBeGreaterThan(hits);
    expect(inflated.residentBytes).toBe(resident);
    inflated.evict(1);
    // The oldest release goes: one of a's pages, none of b's.
    const missesOf = async (chunk: typeof a): Promise<number> => {
      const probe = await ChunkReader.open(view, chunk, {
        budget: UNLIMITED,
        inflated,
        once: true,
      });
      const before = inflated.misses;
      await probe.load(all(probe));
      probe.close();
      return inflated.misses - before;
    };
    expect(await missesOf(b)).toBe(0);
    expect(await missesOf(a)).toBeGreaterThan(0);
  });
});
