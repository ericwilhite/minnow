/** Source-sized (non-memoized) decoding across real cache eviction, groups, encodings and large
 * values. Reference byte counts never call the reader's optimized sizing methods.
 */
import fc from "fast-check";
import { describe, expect, it } from "vitest";
import { createReach, SHAPE_DIFFERENTIAL_REACH } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import { ColumnTarget, type DecodeSpec } from "../exec/column-target.js";
import { sourceSizedPages } from "../exec/column-shape-test-support.js";
import { BlobCache } from "../format/blob-cache.js";
import { PageEncoding } from "../format/manifest.js";
import type { ChunkDirectory } from "../page/directory.js";
import { LARGE_VALUE_BYTES } from "../page/large-value.js";
import { ChunkReader } from "../page/reader.js";
import { defined, sameValue, valueAt, vectorOf, type TestValue } from "../page/test-support.js";
import { GroupWriter } from "../page/writer.js";
import { PageCache } from "../storage/page-cache.js";
import { MemoryBackend } from "../storage/memory-store.js";
import { openExtentSpace } from "../storage/space.js";
import {
  countingBudget,
  countingMemory,
  jsonReachable,
  rootOf,
} from "../storage/space-test-support.js";
import type { BlobRef, ExtentRef } from "../storage/extents.js";
import type { PhysicalType } from "../types.js";
import { isValid } from "../vector/validity.js";
import type { Vector } from "../vector/vector.js";

const PROFILES = ["large", "gzip", "dictionary0", "dictionary8", "dictionary16"] as const;
type Profile = (typeof PROFILES)[number];
function valuesOf(profile: Profile, count: number, salt: number, group: number): TestValue[] {
  return Array.from({ length: count }, (_, row) => {
    // Force actual non-NULL large values; sparse masks vary elsewhere, independently of lengths.
    const large = profile === "large" && (row === 3 || row === count - 1);
    if (group === 2 || (!large && group === 1 && (row + salt) % 7 === 0)) return null;
    if (large)
      return new Uint8Array(LARGE_VALUE_BYTES + 17 + salt + group * 13).fill(65 + (row % 20));
    if (profile === "gzip")
      return new TextEncoder().encode(`${String(row % 19)}:${"x".repeat(1024 + (salt % 13))}`);
    if (profile.startsWith("dictionary")) {
      const distinct = profile === "dictionary0" ? 1 : profile === "dictionary8" ? 31 : 300;
      // Repeat each code before introducing the next, so every page's dictionary pays for itself.
      return new TextEncoder().encode(
        `${String(Math.floor(row / 2) % distinct)}:${"x".repeat(128)}:${String(salt + group)}`,
      );
    }
    return new Uint8Array(9 + ((row * salt + group) % 31)).fill(row % 251);
  });
}
function dataRows(v: Vector): TestValue[] {
  if (v.form !== "flat" && v.form !== "dictionary") throw new Error("Unexpected shape");
  return Array.from({ length: v.length }, (_, row) =>
    !isValid(v.validity, row)
      ? null
      : v.form === "flat"
        ? valueAt(v, row)
        : valueAt(v.dictionary, v.codes[row] ?? 0),
  );
}
function equal(actual: readonly TestValue[], expected: readonly TestValue[]): void {
  if (
    actual.length !== expected.length ||
    !actual.every((v, i) => sameValue(v, expected[i] ?? null))
  )
    expect(actual).toEqual(expected);
}

// Profiles are stratified: every seed must really reach its named stored encoding.
describe.each(PROFILES)("shape differential: %s", (profile) => {
  it.each(seedsFor("next-shape-differential", [0xb2d9, 0xb2da]))(
    "source sizes, same-layout groups and eviction (seed %i)",
    async (seed) => {
      const expectedEncoding =
        profile === "large"
          ? "out-of-line >64KiB"
          : profile === "gzip"
            ? "gzip"
            : profile === "dictionary0"
              ? "dictionary width 0"
              : profile === "dictionary8"
                ? "dictionary width 1"
                : "dictionary width 2";
      const reach = createReach(`next-shape-differential/${profile}`, [
        ...SHAPE_DIFFERENTIAL_REACH,
        expectedEncoding,
      ]);
      await fc.assert(
        fc.asyncProperty(fc.integer({ min: 1, max: 29 }), async (salt) => {
          reach.beginHistory();
          const backend = new MemoryBackend(),
            lease = await backend.acquire({ timeoutMs: 2000 }),
            session = await backend.open(lease);
          const memory = countingMemory(Infinity),
            rootBudget = countingBudget(Infinity),
            budget = countingBudget(Infinity);
          let registeredCaches = 0;
          const space = await openExtentSpace(session, {
            budget: rootBudget,
            reachable: jsonReachable,
            registerCache: (cache) => {
              registeredCaches++;
              // Like main's first-registration fix, select pages rather than retained metadata.
              if (cache instanceof PageCache) memory.cache = cache;
              return memory;
            },
          });
          expect(registeredCaches).toBeGreaterThan(1);
          reach.reach("separate page and metadata caches");
          const directories = new BlobCache<ChunkDirectory>(() => memory);
          const type: PhysicalType = profile === "large" ? { kind: "bytes" } : { kind: "text" };
          const count =
            profile === "dictionary16"
              ? 1024
              : profile === "dictionary8"
                ? 512
                : profile === "large"
                  ? 24
                  : 128;
          let targetBytes = 0;
          const allocation = {
            take(n: number) {
              targetBytes += n;
            },
            give(n: number) {
              targetBytes -= n;
            },
          };
          const target = new ColumnTarget(type, count, allocation),
            reference = new ColumnTarget(type, count, allocation);
          const extents: ExtentRef[] = [];
          try {
            for (let group = 0; group < 3; group++) {
              const source = valuesOf(profile, count, salt, group),
                writer = space.beginWrite(`group ${String(group)}`);
              const columns = new GroupWriter({
                columns: [{ columnId: 1, type }],
                writer,
                inlineBytes: 0,
                rawPageBytes: profile.startsWith("dictionary") ? 128 * 1024 : 32 * 1024,
                pageBytes: 64 * 1024,
                dictionaryEntries: profile.startsWith("dictionary") ? 65536 : 1,
                gzip: profile === "gzip",
              });
              await columns.append([vectorOf(type, source)], count);
              const written = await columns.finish(),
                chunk = defined(written.columns[0]);
              extents.push(...written.extents);
              await space.publish(rootOf([], extents), { writers: [writer], retired: [] });
              const reader = await ChunkReader.open(space, chunk, { budget, directories });
              try {
                const indices = reader.directory.pages.map((_, index) => index);
                const pageRef = (index: number): BlobRef => {
                  const page = defined(reader.directory.pages[index]);
                  return {
                    extent: chunk.data.extent,
                    offset: chunk.data.offset + page.offset,
                    length: page.length,
                    checksum: page.checksum,
                  };
                };
                if (group < 2) {
                  if (profile === "large") {
                    expect(
                      source.some((v) => v instanceof Uint8Array && v.length > LARGE_VALUE_BYTES),
                    ).toBe(true);
                    expect(
                      reader.directory.pages.some(
                        (p) => (p.largeRegion?.length ?? 0) > LARGE_VALUE_BYTES,
                      ),
                    ).toBe(true);
                    reach.reach("out-of-line >64KiB");
                  } else if (profile === "gzip") {
                    expect(
                      reader.directory.pages.some((p) => (p.encoding & PageEncoding.gzip) !== 0),
                    ).toBe(true);
                    reach.reach("gzip");
                  } else {
                    let matched = false;
                    for (const index of indices) {
                      const page = defined(reader.directory.pages[index]);
                      const bytes = await space.read([pageRef(index)], { budget });
                      try {
                        const width = defined(bytes.bytes[0])[3];
                        if (
                          (page.encoding & ~PageEncoding.gzip) === PageEncoding.dictionary ||
                          reader.isDictionaryRange(page.firstRow, page.firstRow + page.rowCount)
                        ) {
                          if (
                            width ===
                            (profile === "dictionary0" ? 0 : profile === "dictionary8" ? 1 : 2)
                          )
                            matched = true;
                        }
                      } finally {
                        bytes.release();
                      }
                    }
                    expect(matched, `required ${profile} stored code width`).toBe(true);
                    reach.reach(expectedEncoding);
                  }
                }
                for (const residency of ["loaded", "reloaded"] as const) {
                  await reader.load(indices);
                  await reader.loadValues(0, count);
                  const legacy = sourceSizedPages(reader, source);
                  const gathered = [
                    Uint16Array.of(0, 3, count - 1),
                    Uint16Array.of(0, count - 2, count - 1),
                  ];
                  const specs: DecodeSpec[] = [
                    { from: 0, to: count },
                    { from: 1, to: 3 },
                    { from: 3, to: 4 },
                    ...gathered.map((rows) => ({ rows, count: rows.length })),
                    { from: 0, to: 0 },
                    { rows: new Uint16Array(0), count: 0 },
                  ];
                  for (const spec of specs) {
                    const expected =
                      "from" in spec
                        ? source.slice(spec.from, spec.to)
                        : Array.from(
                            spec.rows.subarray(0, spec.count),
                            (row) => source[row] ?? null,
                          );
                    for (const codes of [true, false]) {
                      expect(
                        target.outputBytes(reader, spec, codes, true),
                        "memoized size versus SOURCE byte count",
                      ).toBe(reference.outputBytes(legacy, spec, codes, true));
                      expect(target.growthFor(reader, spec)).toBe(
                        reference.growthFor(legacy, spec),
                      );
                      equal(dataRows(target.decode(reader, spec)), expected);
                      equal(dataRows(reference.decode(legacy, spec)), expected);
                      const own = target.decodeOwned(reader, spec, codes),
                        old = reference.decodeOwned(legacy, spec, codes);
                      equal(dataRows(own.vector), expected);
                      equal(dataRows(old.vector), expected);
                      expect(own.bytes).toBe(old.bytes);
                      allocation.give(own.bytes + old.bytes);
                    }
                  }
                  reach.reach("empty range");
                  reach.reach("empty gather");
                  reach.reach("same endpoints different interiors");
                  reach.reach("independent source lengths");
                  if (residency === "loaded") {
                    reader.release();
                    const cache = defined(memory.cache ?? undefined);
                    expect(cache.evict(Infinity)).toBeGreaterThan(0);
                    expect(space.peek(pageRef(0))).toBeNull();
                    reach.reach("actual cache eviction");
                    const before = space.stats.cache.platformReads;
                    await reader.load(indices);
                    expect(space.stats.cache.platformReads).toBeGreaterThan(before);
                    reach.reach("backend reload after eviction");
                  }
                }
                reach.reach(
                  group === 0
                    ? "all-valid group"
                    : group === 1
                      ? "sparse-null group"
                      : "all-null group",
                );
                if (group > 0) reach.reach("same target switches group");
              } finally {
                reader.close();
              }
              expect(budget.used).toBe(0);
            }
          } finally {
            target.close();
            reference.close();
            directories.clear();
            await space.close();
            await session.close();
            await lease.release();
          }
          expect(targetBytes).toBe(0);
          expect(budget.used).toBe(0);
          expect(rootBudget.used).toBe(0);
          expect(memory.used).toBe(0);
        }),
        { seed, numRuns: 16 },
      );
      reach.assertReached();
      console.log(
        "SHAPE_DIFFERENTIAL_REACH",
        JSON.stringify({ seed, profile, states: Object.fromEntries(reach.counts()) }),
      );
    },
  );
});
