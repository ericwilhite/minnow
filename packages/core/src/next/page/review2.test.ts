/**
 * One test per finding of review B2-2 (reviews/B2-page-format-review-2.md), each running the
 * reviewer's probe (minnow-review-b2b/tree/review-probes.ts) and rejecting what it found.
 */
import fc from "fast-check";
import { describe, expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { encodeSortKey } from "../keys/key-encoding.js";
import type { ColumnChunk, PageEntry } from "../format/manifest.js";
import { PageEncoding } from "../format/manifest.js";
import type { BlobRef } from "../storage/extents.js";
import { compareBytes } from "../vector/varlen.js";
import type { PhysicalType } from "../types.js";
import { decodeRange, gatherRows, parsePage } from "./decode.js";
import { PageCorruptionError } from "./format.js";
import { InflatedPageCache } from "./inflated-cache.js";
import { MemoryExtentSpace, UNLIMITED } from "./memory-extents.js";
import { ChunkReader, PageBudgetError } from "./reader.js";
import {
  defined,
  openChunk,
  readChunk,
  targetFor,
  writeGroup,
  type DecimalValue,
  type TestValue,
} from "./test-support.js";
import { GroupWriter } from "./writer.js";

const SEEDS = seedsFor("next-page-bounds", [0xb2b2]);
const encoder = new TextEncoder();
const NUMERIC: PhysicalType = { kind: "decimal", precision: null, scale: null };

function sortKey(value: DecimalValue): Uint8Array {
  const special =
    value.special === 1
      ? "NaN"
      : value.special === 2
        ? "Infinity"
        : value.special === 3
          ? "-Infinity"
          : undefined;
  return encodeSortKey([
    {
      type: NUMERIC,
      value:
        special === undefined
          ? { coefficient: value.coefficient, scale: value.scale ?? 0 }
          : { coefficient: 0n, scale: 0, special },
      direction: "asc",
      nulls: "first",
    },
  ]);
}

const order = (a: Uint8Array, b: Uint8Array): number =>
  compareBytes(a, 0, a.length, b, 0, b.length);

function tracked(): { held: number; allocate(n: number): boolean; free(n: number): void } {
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

describe("B2-2 #1: NUMERIC bounds compare the exact sort keys", () => {
  it("does not swap min and max when 10^309 overflows a double (the review's probe)", async () => {
    const values: DecimalValue[] = [
      { coefficient: 10n ** 38n, scale: 309, special: 0 },
      { coefficient: 1n, scale: 308, special: 0 },
    ];
    const w = await writeGroup([{ type: NUMERIC, values, vector: { tier: "int128" } }]);
    const chunk = defined(w.group.columns[0]);
    // 10^38 × 10^−309 = 10^−271 is greater than 1 × 10^−308.
    expect(chunk.statistics.min).toEqual(sortKey(defined(values[1])));
    expect(chunk.statistics.max).toEqual(sortKey(defined(values[0])));
    expect(await readChunk(w.space, chunk)).toEqual(values);
  });

  it("orders values that differ only past double precision and past the first key bytes", async () => {
    // All within 10^−25 of 1 (or −1): a double sees one value; the keys differ late.
    const near = (sign: bigint, offsets: readonly bigint[]): DecimalValue[] =>
      offsets.map((offset, k) => ({
        coefficient: sign * (10n ** BigInt(30 + k) + offset),
        special: 0,
        scale: 30 + k,
      }));
    for (const sign of [1n, -1n]) {
      const values = near(sign, [5n, 0n, 9n * 10n, 3n * 100n, 1n]);
      const w = await writeGroup([{ type: NUMERIC, values, vector: { tier: "int128" } }]);
      const keys = values.map(sortKey).sort(order);
      const stats = defined(w.group.columns[0]).statistics;
      expect(stats.min).toEqual(keys[0]);
      expect(stats.max).toEqual(keys[keys.length - 1]);
    }
  });

  it("matches the brute-force bounds at extreme scales, both signs, near-equal values and specials", async () => {
    const coefficient = fc.oneof(
      fc.bigInt({ min: -(2n ** 126n), max: 2n ** 126n }),
      fc.bigInt({ min: -1000n, max: 1000n }),
      fc.constantFrom(0n, 1n, -1n, 10n ** 38n, -(10n ** 38n), 2n ** 64n + 1n),
    );
    const scale = fc.oneof(
      fc.integer({ min: 0, max: 40 }),
      fc.integer({ min: 300, max: 320 }),
      fc.constantFrom(0, 16_383),
    );
    const value: fc.Arbitrary<DecimalValue> = fc.oneof(
      { weight: 12, arbitrary: fc.record({ coefficient, special: fc.constant(0), scale }) },
      // Near-equal: the same value at two scales, and one ulp-of-a-double apart.
      {
        weight: 3,
        arbitrary: fc
          .tuple(
            fc.bigInt({ min: -(10n ** 20n), max: 10n ** 20n }),
            fc.integer({ min: 0, max: 30 }),
            fc.boolean(),
          )
          .map(([c, s, bump]) => ({
            coefficient: c * 10n ** 10n + (bump ? 1n : 0n),
            special: 0,
            scale: s + 10,
          })),
      },
      {
        weight: 1,
        arbitrary: fc.record({
          coefficient: fc.constant(0n),
          special: fc.constantFrom(1, 2, 3),
          scale: fc.constant(0),
        }),
      },
    );
    // Whole columns of near-equal values (B2-2 #1): one value plus offsets far below a double's ulp.
    const cluster: fc.Arbitrary<DecimalValue[]> = fc
      .tuple(
        // |base × 10^30| < 2^127 (the int128 tier).
        fc.bigInt({ min: -(10n ** 7n), max: 10n ** 7n }),
        fc.integer({ min: 0, max: 300 }),
        fc.array(fc.bigInt({ min: 0n, max: 10n ** 6n }), { minLength: 1, maxLength: 200 }),
      )
      .map(([base, scale, offsets]) =>
        offsets.map((offset) => ({
          coefficient: base * 10n ** 30n + offset,
          special: 0,
          scale: scale + 30,
        })),
      );
    for (const seed of SEEDS)
      await fc.assert(
        fc.asyncProperty(
          fc.oneof(fc.array(value, { minLength: 1, maxLength: 300 }), cluster),
          fc.constantFrom(1024, 32 * 1024),
          async (values, rawPageBytes) => {
            const w = await writeGroup([{ type: NUMERIC, values, vector: { tier: "int128" } }], {
              rawPageBytes,
            });
            const keys = values.map(sortKey).sort(order);
            const stats = defined(w.group.columns[0]).statistics;
            expect(stats.min).toEqual(keys[0]);
            expect(stats.max).toEqual(keys[keys.length - 1]);
          },
        ),
        { seed, numRuns: 60 },
      );
  });
});

/** Every page of `chunk`, stored bytes with the safe flag forged on. */
async function forgedPages(
  space: MemoryExtentSpace,
  chunk: ColumnChunk,
): Promise<Array<{ bytes: Uint8Array; entry: PageEntry }>> {
  const reader = await openChunk(space, chunk);
  const out = reader.directory.pages.map((entry) => {
    const at = chunk.data.offset + entry.offset;
    const bytes = space.bytes.slice(at, at + entry.length);
    bytes[2] = (bytes[2] ?? 0) | 16;
    return { bytes, entry };
  });
  reader.close();
  return out;
}

describe("B2-2 #2: a forged safe flag is caught for every int64-bearing layout, range and gather", () => {
  const big = 9_007_199_254_740_993n; // 2^53 + 1
  interface Case {
    name: string;
    type: PhysicalType;
    values: TestValue[];
    scale?: number;
    encoding: number;
  }
  const cases: Case[] = [];
  for (const type of [
    { kind: "int64" },
    { kind: "timestamp", zoned: true },
    { kind: "time" },
  ] as PhysicalType[]) {
    cases.push({
      name: `${type.kind} constant`,
      type,
      values: Array<TestValue>(300).fill(big),
      encoding: PageEncoding.constant,
    });
    cases.push({
      name: `${type.kind} FOR`,
      type,
      values: Array.from({ length: 300 }, (_, i) => big + BigInt((i * 7919) % 200)),
      encoding: PageEncoding.frameOfReference,
    });
    cases.push({
      name: `${type.kind} plain`,
      type,
      values: Array.from({ length: 300 }, (_, i) =>
        i % 2 === 0 ? big : -(BigInt(i) << 40n) * 977n,
      ),
      encoding: PageEncoding.plain,
    });
  }
  cases.push({
    name: "interval constant (the review's probe)",
    type: { kind: "interval" },
    values: Array<TestValue>(300).fill({ months: 0, days: 0, micros: big }),
    encoding: PageEncoding.constant,
  });
  cases.push({
    name: "interval plain",
    type: { kind: "interval" },
    values: Array.from({ length: 300 }, (_, i) => ({
      months: i,
      days: i % 7,
      micros: i % 2 === 0 ? big : BigInt(i),
    })),
    encoding: PageEncoding.plain,
  });
  const decimal: PhysicalType = { kind: "decimal", precision: 18, scale: 2 };
  cases.push({
    name: "decimal constant",
    type: decimal,
    values: Array<TestValue>(300).fill({ coefficient: big, special: 0 }),
    scale: 2,
    encoding: PageEncoding.constant,
  });
  cases.push({
    name: "decimal FOR",
    type: decimal,
    values: Array.from({ length: 300 }, (_, i) => ({
      coefficient: big + BigInt(i % 50),
      special: 0,
    })),
    scale: 2,
    encoding: PageEncoding.frameOfReference,
  });
  cases.push({
    name: "decimal plain",
    type: decimal,
    values: Array.from({ length: 300 }, (_, i) => ({
      coefficient: i % 2 === 0 ? big : -(BigInt(i) << 45n),
      special: 0,
    })),
    scale: 2,
    encoding: PageEncoding.plain,
  });

  for (const test of cases)
    it(test.name, async () => {
      const w = await writeGroup([
        { type: test.type, values: test.values, vector: { scale: test.scale ?? 0 } },
      ]);
      const chunk = defined(w.group.columns[0]);
      const reader = await openChunk(w.space, chunk);
      expect(reader.directory.pages.map((page) => page.encoding & 0x7f)).toContain(test.encoding);
      const tag = reader.directory.tag;
      const scale = reader.directory.scale;
      for (const { bytes, entry } of await forgedPages(w.space, chunk)) {
        const view = parsePage(bytes, entry, tag, scale);
        const safeTarget = (): ReturnType<typeof targetFor> => {
          const target = targetFor(reader, entry.rowCount, 0);
          const safe = { repr: "safe" as const, values: new Float64Array(entry.rowCount) };
          const data = target.data;
          if (data.layout === "int64") return { ...target, data: { ...data, values: safe } };
          if (data.layout === "interval") return { ...target, data: { ...data, micros: safe } };
          if (data.layout === "decimal")
            return { ...target, data: { ...data, coefficients: safe } };
          throw new Error(`no int64 field in ${data.layout}`);
        };
        expect(() => decodeRange(view, 0, entry.rowCount, safeTarget(), 0)).toThrow(
          PageCorruptionError,
        );
        const rows = Uint16Array.from({ length: entry.rowCount }, (_, k) => entry.firstRow + k);
        expect(() =>
          gatherRows(view, rows, 0, rows.length, entry.firstRow, safeTarget(), 0),
        ).toThrow(PageCorruptionError);
      }
      reader.close();
    });
});

describe("B2-2 #3: one cleanup owner per hold, whichever page of a cached load fails", () => {
  const blobOf = (
    chunk: ColumnChunk,
    entry: { offset: number; length: number; checksum: number },
  ): BlobRef => ({
    extent: chunk.data,
    offset: entry.offset,
    length: entry.length,
    checksum: entry.checksum,
  });

  it("the space's cache: a malformed cached page at every position (the review's probe)", async () => {
    const values = Array.from({ length: 6000 }, (_, i) => encoder.encode(`k${String(i % 40)}`));
    const w = await writeGroup([{ type: { kind: "text" }, values }], { rawPageBytes: 2048 });
    const chunk = defined(w.group.columns[0]);
    const space = w.space.view({});
    const warm = await ChunkReader.open(space, chunk, { budget: UNLIMITED });
    const pages = warm.directory.pages.map((_, page) => page);
    await warm.load(pages);
    warm.close();
    const entries = [...warm.directory.pages, defined(warm.directory.dictionary ?? undefined)];
    expect(entries.length).toBeGreaterThan(3);
    for (const entry of entries) {
      const cached = defined(space.peek(blobOf(chunk, entry)) ?? undefined);
      const original = cached[12] ?? 0;
      cached[12] = 1; // the header's reserved field: structurally invalid
      const budget = tracked();
      try {
        const reader = ChunkReader.open(space, chunk, { budget });
        if (reader instanceof Promise) throw new Error("cached directory opens synchronously");
        expect(() => reader.load(pages)).toThrow(PageCorruptionError);
        reader.close();
        expect(budget.held).toBe(0);
      } finally {
        cached[12] = original;
      }
    }
  });

  it("the inflated cache: a malformed inflated page at every position; pins stay balanced", async () => {
    const values = Array.from({ length: 12_000 }, (_, i) =>
      encoder.encode(`event ${String(i % 700)} ok`),
    );
    const w = await writeGroup([{ type: { kind: "text" }, values }], {
      dictionaryEntries: 1,
      gzip: true,
    });
    const chunk = defined(w.group.columns[0]);
    const space = w.space.view({});
    let cacheHeld = 0;
    const inflated = new InflatedPageCache(() => ({
      charge: (bytes: number) => ((cacheHeld += bytes), true),
      release: (bytes: number) => void (cacheHeld -= bytes),
    }));
    const warm = await ChunkReader.open(space, chunk, { budget: UNLIMITED, inflated });
    const pages = warm.directory.pages.map((_, page) => page);
    await warm.load(pages);
    warm.close();
    const zipped = warm.directory.pages.filter((page) => (page.encoding & PageEncoding.gzip) !== 0);
    expect(zipped.length).toBeGreaterThan(2);
    for (const entry of zipped) {
      const ref = blobOf(chunk, entry);
      const bytes = defined(inflated.pin(ref) ?? undefined);
      inflated.unpin(ref);
      const original = bytes[12] ?? 0;
      bytes[12] = 1;
      const budget = tracked();
      try {
        const reader = ChunkReader.open(space, chunk, { budget, inflated });
        if (reader instanceof Promise) throw new Error("cached directory opens synchronously");
        expect(() => reader.load(pages)).toThrow(PageCorruptionError);
        reader.close();
        expect(budget.held).toBe(0);
        // Every pin was returned exactly once: all of it is charged to the cache again.
        expect(cacheHeld).toBe(inflated.residentBytes);
      } finally {
        bytes[12] = original;
      }
    }
    // Nothing is left pinned: clearing frees every entry and all of the cache's charge.
    inflated.clear();
    expect(inflated.residentBytes).toBe(0);
    expect(cacheHeld).toBe(0);
  });
});

describe("B2-2 #4: charge before allocating the scratch, and before parsing a directory", () => {
  it("allocates no ArrayBuffer before the first refused charge (the review's probe)", () => {
    const Native = globalThis.ArrayBuffer;
    let before = 0;
    let refusedAfter = -1;
    globalThis.ArrayBuffer = new Proxy(Native, {
      construct(target, args: [number]) {
        before += args[0];
        return Reflect.construct(target, args) as ArrayBuffer;
      },
    });
    try {
      expect(
        () =>
          new GroupWriter({
            columns: [{ columnId: 1, type: { kind: "int64" } }],
            writer: new MemoryExtentSpace().beginWrite("refused"),
            charge: {
              allocate: () => {
                if (refusedAfter < 0) refusedAfter = before;
                return false;
              },
              free: () => undefined,
            },
          }),
      ).toThrow(RangeError);
    } finally {
      globalThis.ArrayBuffer = Native;
    }
    expect(refusedAfter).toBe(0);
  });

  it("refuses a directory's charge before parsing it, and returns it when the parse fails", async () => {
    const w = await writeGroup([
      { type: { kind: "int32", bits: 32 }, values: Array.from({ length: 30_000 }, (_, i) => i) },
    ]);
    const chunk = defined(w.group.columns[0]);
    const space = w.space.view({});
    (await ChunkReader.open(space, chunk, { budget: UNLIMITED })).close();
    const cached = defined(space.peek(chunk.directory) ?? undefined);
    const original = cached[0] ?? 0;
    cached[0] = original ^ 0xff; // a bad magic: the parse would fail
    try {
      const refusing = { allocate: (): boolean => false, free: (): void => undefined };
      expect(() => ChunkReader.open(space, chunk, { budget: refusing })).toThrow(PageBudgetError);
      const budget = tracked();
      expect(() => ChunkReader.open(space, chunk, { budget })).toThrow(PageCorruptionError);
      expect(budget.held).toBe(0);
    } finally {
      cached[0] = original;
    }
  });
});
