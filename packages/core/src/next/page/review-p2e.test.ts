/**
 * Review B2-p2e (rebuild/format-p2e acdce88). #1: a parsed descriptor stays charged from the
 * moment it is attached until its cache entry is gone, pinned or not (storage/page-cache.ts):
 * leases pay for blob bytes, the cache for the descriptor. #2: under MINNOW_VERIFY_CACHED_BYTES
 * (set for every suite by vitest.config.ts), a consumer that writes into cached bytes fails at
 * the lease's release or the entry's eviction (storage/verify-cached.ts).
 */
import { describe, expect, it } from "vitest";
import type { ColumnChunk } from "../format/manifest.js";
import type { BlobRef, CacheMemory } from "../storage/extents.js";
import { MemoryBackend } from "../storage/memory-store.js";
import { ENTRY_OVERHEAD } from "../storage/page-cache.js";
import { countingBudget, countingMemory, openOver } from "../storage/space-test-support.js";
import { CachedBytesWrittenError, VERIFY_CACHED_BYTES } from "../storage/verify-cached.js";
import { UNLIMITED } from "./memory-extents.js";
import { compilePredicate } from "./predicate.js";
import { ChunkReader, directoryCharge, PAGE_DESCRIPTOR_BYTES } from "./reader.js";
import { defined, vectorOf, writeGroup } from "./test-support.js";
import { GroupWriter } from "./writer.js";

const type = { kind: "int32", bits: 32 } as const;
const values = Array.from({ length: 20_000 }, (_, row) =>
  row % 9 === 0 ? null : (row * 7919) % 100_000,
);

/** Flips the low bit of `bytes[at]` (a consumer writing into a lease). */
function flip(bytes: Uint8Array, at: number): void {
  bytes[at] = (bytes[at] ?? 0) ^ 1;
}

/** Cache memory whose limit can move (the governor's room changing under the cache). */
function adjustableMemory(): CacheMemory & { used: number; limit: number; cache: unknown } {
  const memory = {
    used: 0,
    limit: Number.POSITIVE_INFINITY,
    cache: null as unknown,
    charge(bytes: number): boolean {
      if (memory.used + bytes > memory.limit) return false;
      memory.used += bytes;
      return true;
    },
    release(bytes: number): void {
      memory.used -= bytes;
    },
  };
  return memory;
}

async function table(memory: CacheMemory & { used: number; cache: unknown }) {
  const { space, budget } = await openOver(new MemoryBackend(), {
    memory: memory as ReturnType<typeof countingMemory>,
  });
  const writer = new GroupWriter({
    columns: [{ columnId: 1, type }],
    writer: space.beginWrite("p2e"),
    inlineBytes: 0,
  });
  await writer.append([vectorOf(type, values)], values.length);
  const chunk: ColumnChunk = defined((await writer.finish()).columns[0]);
  const reader = await ChunkReader.open(space, chunk, { budget });
  const pages = reader.directory.pages;
  reader.close();
  const refs: BlobRef[] = pages.map((page) => ({
    extent: chunk.data.extent,
    offset: chunk.data.offset + page.offset,
    length: page.length,
    checksum: page.checksum,
  }));
  const stored = pages.reduce((sum, page) => sum + page.length, 0);
  return { space, budget, chunk, pages, refs, stored };
}

describe("review B2-p2e #1: descriptors are charged continuously, pinned or not", () => {
  it("charges a descriptor attached to a pinned entry at once, and keeps it charged across pins (the probe, inverted)", async () => {
    const memory = countingMemory(Number.POSITIVE_INFINITY);
    const { space, budget, chunk, pages, refs, stored } = await table(memory);
    const descriptors = pages.length * PAGE_DESCRIPTOR_BYTES;
    const before = memory.used + budget.used;
    // Cold: every page comes through a lease and gets its descriptor while pinned.
    const reader = await ChunkReader.open(space, chunk, { budget });
    await reader.load(pages.map((_, page) => page));
    expect(refs.every((ref) => space.parsed?.(ref) != null)).toBe(true);
    // The reader holds its pages (leases: blob bytes) and its parsed directory; the cache, the
    // descriptors.
    expect(memory.used + budget.used - before).toBe(
      stored + descriptors + directoryCharge(reader.directory),
    );
    reader.close();
    // Cached: the blobs and their descriptors.
    const cached = memory.used;
    expect(cached + budget.used - before).toBe(
      stored + pages.length * ENTRY_OVERHEAD + descriptors,
    );
    // A lease moves the blob bytes (and overhead) to the budget; the descriptors stay charged.
    const lease = await space.read(refs, { budget });
    expect(budget.used).toBe(stored);
    expect(cached - memory.used).toBe(stored + pages.length * ENTRY_OVERHEAD);
    expect(refs.every((ref) => space.parsed?.(ref) != null)).toBe(true);
    lease.release();
    expect(memory.used).toBe(cached);
    expect(budget.used).toBe(0);
  });

  it("charges each descriptor once while several leases hold its entry, before any release", async () => {
    const memory = countingMemory(Number.POSITIVE_INFINITY);
    const { space, budget, chunk, pages, refs, stored } = await table(memory);
    const descriptors = pages.length * PAGE_DESCRIPTOR_BYTES;
    const base = memory.used;
    const first = await space.read(refs, { budget });
    const second = await space.read(refs, { budget });
    // Readers attach while both leases pin every entry.
    const reader = await ChunkReader.open(space, chunk, { budget });
    await reader.load(pages.map((_, page) => page));
    const other = await ChunkReader.open(space, chunk, { budget });
    await other.load(pages.map((_, page) => page));
    expect(refs.every((ref) => space.parsed?.(ref) != null)).toBe(true);
    expect(memory.used - base).toBe(descriptors); // once, though four holders hold each blob
    expect(budget.used).toBeGreaterThanOrEqual(4 * stored); // two leases, two readers' peeks
    reader.close();
    other.close();
    first.release();
    expect(memory.used - base).toBe(descriptors); // still pinned by `second`
    second.release();
    expect(memory.used - base).toBe(stored + pages.length * ENTRY_OVERHEAD + descriptors);
    expect(budget.used).toBe(0);
  });

  it("attaches nothing when the cache cannot pay for a descriptor, pinned or not", async () => {
    const memory = adjustableMemory();
    const { space, budget, chunk, pages, refs } = await table(memory);
    memory.limit = memory.used; // no room at all
    const reader = await ChunkReader.open(space, chunk, { budget });
    await reader.load(pages.map((_, page) => page)); // pinned by its leases while parsed
    expect(refs.some((ref) => space.parsed?.(ref) != null)).toBe(false);
    expect(memory.used).toBe(memory.limit);
    reader.close(); // and no room to keep the blobs either: dropped
    expect(memory.used).toBeLessThanOrEqual(memory.limit);
    expect(budget.used).toBe(0);
  });

  it("gives a descriptor's charge back when its entry cannot be kept after the last lease", async () => {
    const memory = adjustableMemory();
    const { space, budget, chunk, pages, refs } = await table(memory);
    const base = memory.used;
    const reader = await ChunkReader.open(space, chunk, { budget });
    await reader.load(pages.map((_, page) => page)); // pinned; descriptors charged
    expect(memory.used - base).toBe(pages.length * PAGE_DESCRIPTOR_BYTES);
    memory.limit = memory.used; // the blobs cannot come back into the cache
    reader.close();
    expect(refs.some((ref) => space.parsed?.(ref) != null)).toBe(false);
    expect(memory.used).toBe(base);
    expect(budget.used).toBe(0);
  });
});

describe("review B2-p2e #2: a consumer that writes into cached bytes fails loudly in tests", () => {
  it("is on in every suite", () => {
    expect(VERIFY_CACHED_BYTES).toBe(true);
  });

  it("refuses a written lease at its release, on the space's page cache", async () => {
    const { space, budget, refs } = await table(countingMemory(Number.POSITIVE_INFINITY));
    const lease = await space.read(refs.slice(0, 1), { budget });
    const bytes = defined(lease.bytes[0]);
    flip(bytes, 3);
    expect(() => lease.release()).toThrow(CachedBytesWrittenError);
    flip(bytes, 3);
  });

  it("refuses a written cached blob at its eviction, on the space's page cache", async () => {
    const memory = countingMemory(Number.POSITIVE_INFINITY);
    const { space, budget, refs } = await table(memory);
    (await space.read(refs.slice(0, 1), { budget })).release();
    const bytes = defined(space.peek(defined(refs[0])) ?? undefined);
    flip(bytes, 5);
    const cache = defined(
      (memory.cache ?? undefined) as { evict(bytes: number): number } | undefined,
    );
    expect(() => cache.evict(Number.POSITIVE_INFINITY)).toThrow(CachedBytesWrittenError);
  });

  it("refuses the same on the in-memory test space", async () => {
    const { space, group } = await writeGroup([{ type, values }]);
    const chunk = defined(group.columns[0]);
    const reader = await ChunkReader.open(space, chunk, { budget: UNLIMITED });
    const page = defined(reader.directory.pages[0]);
    reader.close();
    const ref = {
      extent: chunk.data.extent,
      offset: chunk.data.offset + page.offset,
      length: page.length,
      checksum: page.checksum,
    };
    const lease = await space.read([ref], { budget: countingBudget(Number.POSITIVE_INFINITY) });
    const bytes = defined(lease.bytes[0]);
    flip(bytes, 0);
    expect(() => lease.release()).toThrow(CachedBytesWrittenError);
    expect(() => space.evict()).toThrow(CachedBytesWrittenError);
    flip(bytes, 0);
    space.evict();
  });
});

describe("review B2-p2e: what the reviewer found sound stays so", () => {
  it("partial multi-page masks agree with select and keep guard bytes (the probe)", async () => {
    const nullable = Array.from({ length: 20_000 }, (_, row) =>
      row % 7 === 0 ? null : (row * 7919) % 100_000,
    );
    const { space, group } = await writeGroup([{ type, values: nullable }]);
    const reader = await ChunkReader.open(space, defined(group.columns[0]), { budget: UNLIMITED });
    await reader.load(reader.directory.pages.map((_, page) => page));
    for (const predicate of [
      { op: ">=", value: 50_000 },
      { op: "isNull" },
      { op: "in", values: [7919, 15_838] },
    ] as const) {
      const compiled = defined(compilePredicate(type, 0, predicate) ?? undefined);
      const from = 123;
      const to = 19_877;
      const at = 5;
      const mask = new Uint8Array(to - from + at + 3).fill(9);
      const out = new Uint16Array(to - from);
      const count = reader.select(compiled, from, to, out);
      expect(reader.selectMask(compiled, from, to, mask, at)).toBe(count);
      expect(
        Array.from(mask.subarray(at, at + to - from)).flatMap((one, k) =>
          one === 1 ? [from + k] : [],
        ),
      ).toEqual(Array.from(out.subarray(0, count)));
      expect(Array.from(mask.slice(0, at))).toEqual(Array<number>(at).fill(9));
      expect(Array.from(mask.slice(-3))).toEqual([9, 9, 9]);
    }
    reader.close();
  });
});
