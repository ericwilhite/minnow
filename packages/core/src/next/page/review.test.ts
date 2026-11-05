/**
 * One test per finding of review B2-1 (reviews/B2-page-format-review-1.md), each running the
 * reviewer's probe (minnow-review-b2/tree/probe.ts) and rejecting the behaviour it found.
 */
import { dataAddress } from "./reader.js";
import { describe, expect, it } from "vitest";
import { encodeSortKey } from "../keys/key-encoding.js";
import { PageEncoding } from "../format/manifest.js";
import { blobAddress, ReadDeniedError, type CacheMemory } from "../storage/extents.js";
import type { PhysicalType } from "../types.js";
import { decodeRange, parsePage } from "./decode.js";
import { PageCorruptionError } from "./format.js";
import { InflatedPageCache } from "./inflated-cache.js";
import { MemoryExtentSpace, UNLIMITED } from "./memory-extents.js";
import { ChunkReader, PageBudgetError, ReaderClosedError } from "./reader.js";
import { DistinctSketch } from "./sketch.js";
import {
  defined,
  openChunk,
  readChunk,
  sameValue,
  targetFor,
  valueAt,
  vectorOf,
  writeGroup,
  type TestValue,
} from "./test-support.js";
import { GroupWriter } from "./writer.js";

const encoder = new TextEncoder();

function tracked(): {
  held: number;
  peak: number;
  allocate(n: number): boolean;
  free(n: number): void;
} {
  return {
    held: 0,
    peak: 0,
    allocate(bytes: number): boolean {
      this.held += bytes;
      this.peak = Math.max(this.peak, this.held);
      return true;
    },
    free(bytes: number): void {
      this.held -= bytes;
    },
  };
}

async function sketchEstimate(
  space: MemoryExtentSpace,
  ref: Parameters<MemoryExtentSpace["read"]>[0][number],
): Promise<number> {
  const lease = await space.read([ref], { budget: UNLIMITED });
  const estimate = DistinctSketch.parse(defined(lease.bytes[0])).estimate();
  lease.release();
  return estimate;
}

describe("B2-1 #2: float bounds track whether any non-NaN value was seen", () => {
  for (const kind of ["float64", "float32"] as const) {
    it(`${kind}: page and chunk bounds in PostgreSQL order`, async () => {
      const cases: Array<[number[], number, number]> = [
        [[Infinity, Number.NaN], Infinity, Number.NaN],
        [[-Infinity, Number.NaN], -Infinity, Number.NaN],
        [[Number.NaN, Number.NaN], Number.NaN, Number.NaN],
        [[Infinity, -Infinity], -Infinity, Infinity],
        [[Infinity], Infinity, Infinity],
      ];
      for (const [values, min, max] of cases) {
        const { group } = await writeGroup([{ type: { kind }, values }]);
        const stats = defined(group.columns[0]).statistics;
        expect([stats.min, stats.max], JSON.stringify(values.map(String))).toEqual([min, max]);
      }
    });

    it(`${kind}: bounds merge across pages (an all-NaN page beside a numeric one)`, async () => {
      const values = [
        ...Array<number>(600).fill(Number.NaN),
        ...Array<number>(600).fill(Infinity),
        ...Array<number>(600).fill(-3),
      ];
      const { space, group } = await writeGroup([{ type: { kind }, values }], {
        rawPageBytes: 1024,
        pageBytes: 256,
      });
      const chunk = defined(group.columns[0]);
      const reader = await openChunk(space, chunk);
      expect(reader.directory.pages.length).toBeGreaterThan(2);
      const first = defined(reader.directory.pages[0]);
      expect([first.min, first.max]).toEqual([Number.NaN, Number.NaN]);
      expect([chunk.statistics.min, chunk.statistics.max]).toEqual([-3, Number.NaN]);
    });
  }
});

describe("B2-1 #3: concurrent cold loads share one read; close during a load leaks nothing", () => {
  it("two loads of one page lease once and return everything", async () => {
    const w = await writeGroup([{ type: { kind: "int64" }, values: [9_007_199_254_740_993n] }]);
    const chunk = defined(w.group.columns[0]);
    const budget = tracked();
    const space = w.space.view({ cache: false });
    const reader = await ChunkReader.open(space, chunk, { budget });
    const reads = space.reads;
    await Promise.all([reader.load([0]), reader.load([0]), reader.load([0])]);
    expect(space.reads - reads).toBe(1);
    reader.close();
    expect(budget.held).toBe(0);
    expect(space.leased).toBe(0);
  });

  it("concurrent dictionary loads share one read", async () => {
    const values = Array.from({ length: 3000 }, (_, row) => encoder.encode(`k${String(row % 9)}`));
    const w = await writeGroup([{ type: { kind: "text" }, values }]);
    const chunk = defined(w.group.columns[0]);
    const budget = tracked();
    const space = w.space.view({ cache: false });
    const reader = await ChunkReader.open(space, chunk, { budget });
    const reads = space.reads;
    const [a, b] = await Promise.all([
      reader.loadDictionary(),
      reader.loadDictionary(),
      reader.load([0]),
      reader.load([0]),
    ]);
    expect(a).toBe(b);
    // One read for the dictionary, one for page 0: never a second of either.
    expect(space.reads - reads).toBe(2);
    reader.close();
    expect(budget.held).toBe(0);
    expect(space.leased).toBe(0);
  });

  it("close() during a load: the load rejects with ReaderClosedError and nothing stays held", async () => {
    const values = Array.from({ length: 5000 }, (_, row) => BigInt(row) * 977n);
    const w = await writeGroup([{ type: { kind: "int64" }, values }], { rawPageBytes: 2048 });
    const chunk = defined(w.group.columns[0]);
    const budget = tracked();
    const space = w.space.view({ cache: false });
    const reader = await ChunkReader.open(space, chunk, { budget });
    const all = reader.directory.pages.map((_, page) => page);
    const loading = reader.load(all);
    reader.close();
    await expect(loading).rejects.toBeInstanceOf(ReaderClosedError);
    expect(reader.isLoaded(0)).toBe(false);
    expect(budget.held).toBe(0);
    expect(space.leased).toBe(0);
    expect(() => reader.load([0])).toThrow(ReaderClosedError);
  });
});

describe("B2-1 #4: int128 NUMERIC sketches hash every significant limb", () => {
  it("counts 1,000 values that differ only above bit 64 (the review's probe)", async () => {
    const values = Array.from({ length: 1000 }, (_, i) => ({
      coefficient: BigInt(i + 1) * (1n << 64n) + 1n,
      special: 0,
      scale: 0,
    }));
    const w = await writeGroup([
      {
        type: { kind: "decimal", precision: null, scale: null },
        values,
        vector: { tier: "int128" },
      },
    ]);
    const estimate = await sketchEstimate(
      w.space,
      defined(defined(w.group.columns[0]).distinctSketch ?? undefined),
    );
    expect(Math.abs(estimate - 1000) / 1000).toBeLessThan(0.1);
  });

  it("hashes equal values equally in the int64 and int128 tiers", async () => {
    const values: TestValue[] = Array.from({ length: 500 }, (_, i) => ({
      coefficient: BigInt(i * 1_000_003 - 250_000_000),
      special: 0,
      scale: i % 3,
    }));
    const type: PhysicalType = { kind: "decimal", precision: null, scale: null };
    const narrow = await writeGroup([{ type, values }]);
    const wide = await writeGroup([{ type, values, vector: { tier: "int128" } }]);
    const registers = async (w: typeof narrow): Promise<Uint8Array> => {
      const ref = defined(defined(w.group.columns[0]).distinctSketch ?? undefined);
      const lease = await w.space.read([ref], { budget: UNLIMITED });
      const out = DistinctSketch.parse(defined(lease.bytes[0])).registers.slice();
      lease.release();
      return out;
    };
    expect(await registers(wide)).toEqual(await registers(narrow));
  });
});

describe("B2-1 #5 / R15.5: gzip pages stay inflated in the cache, charged before allocation", () => {
  const values = Array.from({ length: 3000 }, (_, row) =>
    encoder.encode(`event ${String(row % 600)} ok`),
  );
  const cacheMemory = (): CacheMemory & { charged: number; refuse: boolean } => ({
    charged: 0,
    refuse: false,
    charge(bytes: number): boolean {
      if (this.refuse) return false;
      this.charged += bytes;
      return true;
    },
    release(bytes: number): void {
      this.charged -= bytes;
    },
  });

  it("inflates once per residency; a cached page loads synchronously", async () => {
    const w = await writeGroup([{ type: { kind: "text" }, values }], {
      dictionaryEntries: 1,
      pageBytes: 64 * 1024,
      gzip: true,
    });
    const chunk = defined(w.group.columns[0]);
    const space = w.space.view({});
    const memory = cacheMemory();
    const inflated = new InflatedPageCache(() => memory);
    const pages = (reader: ChunkReader): number[] => reader.directory.pages.map((_, page) => page);
    const first = await ChunkReader.open(space, chunk, { budget: UNLIMITED, inflated });
    expect(first.directory.pages.some((page) => (page.encoding & PageEncoding.gzip) !== 0)).toBe(
      true,
    );
    await first.load(pages(first));
    first.close();
    expect(memory.charged).toBe(inflated.residentBytes);
    expect(memory.charged).toBeGreaterThan(0);

    const budget = tracked();
    const second = ChunkReader.open(space, chunk, { budget, inflated });
    if (second instanceof Promise) throw new Error("a cached directory opens synchronously");
    const hits = inflated.hits;
    expect(second.load(pages(second))).toBeUndefined();
    expect(inflated.hits).toBeGreaterThan(hits);
    // Pinned while held: charged to the reader, not to the cache.
    expect(memory.charged).toBeLessThan(inflated.residentBytes);
    const target = targetFor(second, second.rowCount, second.varlenBytes(0, second.rowCount));
    second.decodeRange(0, second.rowCount, target, 0);
    expect(values.every((value, row) => sameValue(valueAt(target, row), value))).toBe(true);
    second.close();
    expect(budget.held).toBe(0);
    expect(memory.charged).toBe(inflated.residentBytes);
    // Evictable once unpinned; a refused re-charge drops the entry.
    expect(inflated.evict(Number.POSITIVE_INFINITY)).toBeGreaterThan(0);
    expect(memory.charged).toBe(0);
  });

  it("charges the inflated size before inflating: a refused charge holds nothing", async () => {
    const w = await writeGroup([{ type: { kind: "text" }, values }], {
      dictionaryEntries: 1,
      pageBytes: 64 * 1024,
      gzip: true,
    });
    const chunk = defined(w.group.columns[0]);
    const space = w.space.view({ cache: false });
    const gzipPage = 0;
    let allowed = 2; // the directory's lease and its parsed charge
    let held = 0;
    const budget = {
      allocate: (bytes: number): boolean => {
        if (allowed <= 0 && bytes > 16 * 1024) return false;
        allowed--;
        held += bytes;
        return true;
      },
      free: (bytes: number): void => void (held -= bytes),
    };
    const reader = await ChunkReader.open(space, chunk, { budget });
    expect(reader.directory.pages[gzipPage]?.encoding).toBe(PageEncoding.plain | PageEncoding.gzip);
    await expect(reader.load([gzipPage])).rejects.toBeInstanceOf(PageBudgetError);
    expect(space.leased).toBe(0);
    reader.close();
    expect(held).toBe(0);
  });

  it("memtable flushes never gzip (the option is maintenance compaction's)", async () => {
    const w = await writeGroup([{ type: { kind: "text" }, values }], {
      dictionaryEntries: 1,
      pageBytes: 64 * 1024,
    });
    const reader = await openChunk(w.space, defined(w.group.columns[0]));
    expect(reader.directory.pages.every((page) => (page.encoding & PageEncoding.gzip) === 0)).toBe(
      true,
    );
  });
});

describe("B2-1 #6 / R15.1–R15.2: chunks are written through ExtentBuilders; refs carry extents", () => {
  it("every chunk's directory and sketch name the chunk's extent; small chunks share a pack", async () => {
    const big = Array.from({ length: 20_000 }, (_, row) => Math.imul(row, 2_654_435_761));
    const { space, group } = await writeGroup([
      { type: { kind: "int32", bits: 32 }, values: Array<number>(20_000).fill(1) },
      { type: { kind: "int32", bits: 32 }, values: big },
      { type: { kind: "bool" }, values: big.map((value) => value > 0) },
    ]);
    const [small, wide, bools] = group.columns.map((chunk) => chunk);
    for (const chunk of group.columns) {
      expect(chunk.directory.extent).toBe(chunk.data.extent);
      expect(chunk.distinctSketch?.extent).toBe(chunk.data.extent);
      expect(blobAddress(chunk.directory) + chunk.directory.length).toBeLessThanOrEqual(
        dataAddress(chunk.data) + chunk.data.length,
      );
    }
    expect(defined(small).data.extent).toBe(defined(bools).data.extent); // packed together
    expect(defined(wide).data.extent).not.toBe(defined(small).data.extent);
    expect(new Set(group.extents).size).toBe(2);
    for (const chunk of group.columns) expect((await readChunk(space, chunk)).length).toBe(20_000);
  });

  it("abort discards every extent the group sealed or reserved", async () => {
    const space = new MemoryExtentSpace();
    const writer = space.beginWrite("aborted group");
    const group = new GroupWriter({
      columns: [{ columnId: 1, type: { kind: "int64" } }],
      writer,
      inlineBytes: 0,
    });
    const values = Array.from({ length: 30_000 }, (_, row) => BigInt(row) * 1_000_003n);
    await group.append([vectorOf({ kind: "int64" }, values)], values.length);
    await group.abort();
    expect(space.allocator.reserved).toBe(0);
    expect(writer.extents).toEqual([]);
  });
});

describe("B2-1 #7 / R15.10: unconstrained NUMERIC bounds are normalized (value order)", () => {
  const type: PhysicalType = { kind: "decimal", precision: null, scale: null };
  const key = (
    coefficient: bigint,
    scale: number,
    special?: "NaN" | "Infinity" | "-Infinity",
  ): Uint8Array =>
    encodeSortKey([
      {
        type,
        value:
          special === undefined ? { coefficient, scale } : { coefficient: 0n, scale: 0, special },
        direction: "asc",
        nulls: "first",
      },
    ]);

  it("orders by value, scale ignored, specials in PostgreSQL's order, across pages", async () => {
    const values: TestValue[] = [
      { coefficient: 15n, special: 0, scale: 1 }, // 1.5
      { coefficient: 150n, special: 0, scale: 2 }, // 1.50
      { coefficient: 25n, special: 0, scale: 2 }, // 0.25 (least)
      { coefficient: 10n, special: 0, scale: 0 }, // 10 (greatest finite)
      { coefficient: 9_999n, special: 0, scale: 3 }, // 9.999
      { coefficient: 1n << 100n, special: 0, scale: 30 }, // ~1.27, needs the exact compare
    ];
    const { group } = await writeGroup([{ type, values, vector: { tier: "int128" } }]);
    const stats = defined(group.columns[0]).statistics;
    expect(stats.min).toEqual(key(25n, 2));
    expect(stats.max).toEqual(key(10n, 0));

    const withSpecials = [
      ...values,
      { coefficient: 0n, special: 3, scale: 0 },
      { coefficient: 0n, special: 1, scale: 0 },
    ];
    const many = Array.from(
      { length: 1200 },
      (_, row) => withSpecials[row % withSpecials.length] ?? null,
    );
    const { space, group: paged } = await writeGroup(
      [{ type, values: many, vector: { tier: "int128" } }],
      {
        rawPageBytes: 2048,
      },
    );
    const chunk = defined(paged.columns[0]);
    expect(chunk.statistics.min).toEqual(key(0n, 0, "-Infinity"));
    expect(chunk.statistics.max).toEqual(key(0n, 0, "NaN"));
    const reader = await openChunk(space, chunk);
    expect(reader.directory.pages.length).toBeGreaterThan(1);
    for (const page of reader.directory.pages) expect(page.min).toBeInstanceOf(Uint8Array);
  });
});

describe("ruling: a page flagged safe is rechecked while decoding", () => {
  it("rejects a forged safe flag instead of rounding 2^53 + 1 (the review's probe)", async () => {
    const w = await writeGroup([{ type: { kind: "int64" }, values: [9_007_199_254_740_993n] }]);
    const chunk = defined(w.group.columns[0]);
    const reader = await openChunk(w.space, chunk);
    const entry = defined(reader.directory.pages[0]);
    const at = dataAddress(chunk.data) + entry.offset;
    const bytes = w.space.bytes.slice(at, at + entry.length);
    bytes[2] = (bytes[2] ?? 0) | 16;
    const view = parsePage(bytes, entry, 3);
    const target = targetFor(reader, 1, 0);
    const safe = {
      ...target,
      data: {
        layout: "int64" as const,
        values: { repr: "safe" as const, values: new Float64Array(1) },
      },
    };
    expect(() => decodeRange(view, 0, 1, safe, 0)).toThrow(PageCorruptionError);
  });
});

describe("ruling: writer memory is bounded per column and charged before allocation", () => {
  it("a 200-column text group stays within 16 MiB of working set (dictionaries apart)", async () => {
    const columns = Array.from({ length: 200 }, (_, index) => ({
      columnId: index,
      type: { kind: "text" } as const,
    }));
    const budget = tracked();
    const space = new MemoryExtentSpace({ cache: false });
    const writer = new GroupWriter({ columns, writer: space.beginWrite("wide"), charge: budget });
    const rows = 65_536;
    const batch = 2048;
    for (let from = 0; from < rows; from += batch) {
      const vectors = columns.map((column) =>
        vectorOf(
          column.type,
          Array.from({ length: batch }, (_, k) =>
            encoder.encode(
              column.columnId % 2 === 0
                ? `user${String(Math.imul(from + k, 2_654_435_761 + column.columnId) >>> 12)}@mail.example.com`
                : `d${String(column.columnId)}_${String((from + k) % 97)}`,
            ),
          ),
        ),
      );
      await writer.append(vectors, batch);
    }
    const group = await writer.finish();
    expect(group.rowCount).toBe(rows);
    const working = writer.peakWorkingBytes;
    console.log(
      `200-column text group: peak working set ${String(working)} B, peak charged ${String(budget.peak)} B`,
    );
    expect(working).toBeLessThanOrEqual(16 * 1024 * 1024);
    expect(budget.held).toBe(0);
  }, 120_000);

  it("refuses to build when the grant cannot hold its buffers, holding nothing", () => {
    let held = 0;
    const charge = {
      allocate: (bytes: number): boolean =>
        held + bytes > 100_000 ? false : ((held += bytes), true),
      free: (bytes: number): void => void (held -= bytes),
    };
    const space = new MemoryExtentSpace();
    expect(
      () =>
        new GroupWriter({
          columns: Array.from({ length: 50 }, (_, columnId) => ({
            columnId,
            type: { kind: "int64" },
          })),
          writer: space.beginWrite("refused"),
          charge,
        }),
    ).toThrow(RangeError);
    expect(held).toBe(0);
  });
});

describe("parsed directories are charged", () => {
  it("open charges the directory and close returns it; a refused charge is PageBudgetError", async () => {
    const w = await writeGroup([
      {
        type: { kind: "int32", bits: 32 },
        values: Array.from({ length: 20_000 }, (_, i) => i * 7),
      },
    ]);
    const chunk = defined(w.group.columns[0]);
    const budget = tracked();
    const reader = await ChunkReader.open(w.space.view({}), chunk, { budget });
    expect(budget.held).toBeGreaterThan(0);
    reader.close();
    expect(budget.held).toBe(0);
    let leases = 1;
    const refusing = {
      allocate: (): boolean => leases-- > 0,
      free: (): void => undefined,
    };
    await expect(
      ChunkReader.open(w.space.view({ cache: false }), chunk, { budget: refusing }),
    ).rejects.toBeInstanceOf(PageBudgetError);
    await expect(
      ChunkReader.open(w.space.view({ cache: false }), chunk, {
        budget: { allocate: () => false, free: () => undefined },
      }),
    ).rejects.toBeInstanceOf(ReadDeniedError);
  });
});

describe("contracts round 5: ColumnChunk.data is the chunk's range inside its extent", () => {
  it("packed chunks start at nonzero offsets and read correctly, cold, cached and by gather", async () => {
    const rows = 3000;
    const columns: Array<{ type: PhysicalType; values: TestValue[] }> = [
      { type: { kind: "int32", bits: 32 }, values: Array<TestValue>(rows).fill(7) },
      { type: { kind: "bool" }, values: Array.from({ length: rows }, (_, i) => i % 3 === 0) },
      {
        type: { kind: "text" },
        values: Array.from({ length: rows }, (_, i) => encoder.encode(`k${String(i % 5)}`)),
      },
      { type: { kind: "int64" }, values: Array.from({ length: rows }, (_, i) => BigInt(i) * 5n) },
    ];
    const { space, group } = await writeGroup(columns);
    const extent = defined(group.columns[0]).data.extent;
    expect(group.columns.every((chunk) => chunk.data.extent === extent)).toBe(true); // one pack
    const offsets = group.columns.map((chunk) => chunk.data.offset);
    expect(offsets[0]).toBe(0);
    expect(offsets.slice(1).every((offset) => offset > 0)).toBe(true);
    // Ranges are disjoint, in order, inside the extent, and cover each chunk's directory.
    for (const [index, chunk] of group.columns.entries()) {
      expect(chunk.data.offset + chunk.data.length).toBeLessThanOrEqual(extent.length);
      const next = group.columns[index + 1];
      if (next !== undefined)
        expect(chunk.data.offset + chunk.data.length).toBeLessThanOrEqual(next.data.offset);
      expect(chunk.directory.offset).toBeGreaterThanOrEqual(chunk.data.offset);
      expect(chunk.directory.offset + chunk.directory.length).toBe(
        chunk.data.offset + chunk.data.length,
      );
    }
    const cached = space.view({});
    for (const [index, chunk] of group.columns.entries()) {
      const values = defined(columns[index]).values;
      // Cold (verified reads); each page entry's offset is relative to the data range.
      const read = await readChunk(space, chunk);
      expect(read.every((value, row) => sameValue(value, values[row] ?? null))).toBe(true);
      const cold = await openChunk(space, chunk);
      for (const page of cold.directory.pages) {
        const at = dataAddress(chunk.data) + page.offset;
        expect(space.bytes[at]).toBe(page.encoding & 0x7f); // the page header's kind byte
        expect(page.offset + page.length).toBeLessThanOrEqual(chunk.data.length);
      }
      cold.close();
      // Cached: synchronous, then a gather.
      await (await ChunkReader.open(cached, chunk, { budget: UNLIMITED })).load([0]);
      const reader = ChunkReader.open(cached, chunk, { budget: UNLIMITED });
      if (reader instanceof Promise) throw new Error("a cached directory opens synchronously");
      const all = reader.directory.pages.map((_, page) => page);
      await reader.load(all);
      expect(reader.load(all)).toBeUndefined();
      const rowsWanted = Uint16Array.of(0, 1, 2, 999, 1500, rows - 1);
      const target = targetFor(reader, rowsWanted.length, reader.gatherBytes(rowsWanted));
      reader.gather(rowsWanted, target, 0);
      for (const [k, row] of [...rowsWanted].entries())
        expect(sameValue(valueAt(target, k), values[row] ?? null)).toBe(true);
      reader.close();
    }
  });
});
