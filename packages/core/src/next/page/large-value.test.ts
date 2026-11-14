import { writeTestCatalog } from "../format/catalog-test-support.js";
import { MANIFEST_FORMAT_VERSION } from "../durable-format.js";
/**
 * Out-of-line large values (ruling R15.4, large-value.ts): written as ≤ 1 MiB parts through the
 * chunk's extent builder, read back with bounded memory (loaded, charged before the read, in
 * reads of at most `MAX_READ_BYTES`) or streamed one part at a time, verified, inside the
 * chunk's data range (so the reachability walk reaches them and they are reclaimed with it).
 */
import fc from "fast-check";
import { afterAll, describe, expect, it } from "vitest";
import { createReach } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import {
  encodeGroupRoot,
  encodeManifestRoot,
  writeGroupDirectory,
} from "../format/manifest-codec.js";
import type { ColumnChunk, ManifestRoot, PageEntry } from "../format/manifest.js";
import { reachableExtents } from "../format/reachable.js";
import { StoreCorruptionError } from "../storage/contract.js";
import {
  MAX_READ_BYTES,
  type BlobRef,
  type ByteBudget,
  type ExtentReader,
  type PeekOptions,
  type ReadOptions,
} from "../storage/extents.js";
import type { PhysicalType } from "../types.js";
import { decodeArrayValue, encodeArrayValue, type ArrayValue } from "../vector/array-value.js";
import { parsePage, ValueNotLoadedError } from "./decode.js";
import { PageCorruptionError, UnsupportedPageLayoutError, ValueTag } from "./format.js";
import {
  decodePointer,
  encodePointer,
  LARGE_VALUE_BYTES,
  MAX_VALUE_BYTES,
  partsOf,
  VALUE_PART_BYTES,
} from "./large-value.js";
import { MemoryExtentSpace, UNLIMITED } from "./memory-extents.js";
import { ChunkReader } from "./reader.js";
import { DistinctSketch } from "./sketch.js";
import {
  defined,
  openChunk,
  targetFor,
  valueAt,
  vectorOf,
  writeGroup,
  type TestValue,
} from "./test-support.js";
import { GroupWriter } from "./writer.js";
import { PageEncoding } from "../format/manifest.js";

const KiB = 1024;
const MiB = 1024 * 1024;
const SEEDS = seedsFor("next-large-values", [0x1a4e, 0x51]);

/** `length` bytes of a pattern from `seed` (ASCII, so text values are valid UTF-8). */
function value(length: number, seed: number): Uint8Array {
  const out = new Uint8Array(length);
  for (let k = 0; k < length; k++) out[k] = 97 + ((k * 7 + seed * 13 + (k >>> 10)) % 26);
  return out;
}

const same = (a: TestValue | undefined, b: TestValue | undefined): boolean =>
  a === null || b === null || a === undefined || b === undefined
    ? a === b
    : a instanceof Uint8Array && b instanceof Uint8Array && Buffer.from(a).equals(Buffer.from(b));

function expectSame(actual: readonly TestValue[], expected: readonly TestValue[]): void {
  expect(actual.length).toBe(expected.length);
  const wrong = expected.findIndex((each, row) => !same(actual[row], each));
  expect(wrong, `row ${String(wrong)} differs`).toBe(-1);
}

/** A budget that tracks what it holds and its peak, refusing past `limit`. */
function tracked(limit = Number.POSITIVE_INFINITY): ByteBudget & { held: number; peak: number } {
  const budget = {
    held: 0,
    peak: 0,
    allocate(bytes: number): boolean {
      if (budget.held + bytes > limit) return false;
      budget.held += bytes;
      budget.peak = Math.max(budget.peak, budget.held);
      return true;
    },
    free(bytes: number): void {
      budget.held -= bytes;
    },
  };
  return budget;
}

/** An `ExtentReader` that records the bytes of every read. */
function recording(space: ExtentReader): ExtentReader & { reads: number[] } {
  const reads: number[] = [];
  return {
    reads,
    read: (refs: readonly BlobRef[], options: ReadOptions) => {
      reads.push(refs.reduce((sum, ref) => sum + ref.length, 0));
      return space.read(refs, options);
    },
    peek: (ref: BlobRef, options?: PeekOptions) => space.peek(ref, options),
  };
}

const allPages = (reader: ChunkReader): number[] =>
  Array.from({ length: reader.directory.pages.length }, (_, page) => page);

/** Reads every row back: load the pages, then the values, then decode. */
async function readAll(reader: ChunkReader): Promise<TestValue[]> {
  const rows = reader.rowCount;
  await reader.load(allPages(reader));
  await reader.loadValues(0, rows);
  const target = targetFor(reader, rows, reader.varlenBytes(0, rows));
  reader.decodeRange(0, rows, target, 0);
  return Array.from({ length: rows }, (_, row) => valueAt(target, row));
}

/** Reads the selected rows back through `gather`. */
async function gatherAll(reader: ChunkReader, rows: Uint32Array): Promise<TestValue[]> {
  await reader.load(reader.pagesForRows(rows));
  await reader.loadValuesOf(rows);
  const target = targetFor(reader, rows.length, reader.gatherBytes(rows));
  reader.gather(rows, target, 0);
  return Array.from({ length: rows.length }, (_, k) => valueAt(target, k));
}

/** The pointer of each out-of-line row of a loaded chunk. */
async function pointersOf(space: MemoryExtentSpace, chunk: ColumnChunk) {
  const reader = await openChunk(space, chunk);
  await reader.load(allPages(reader));
  const out = [];
  for (const [page, entry] of reader.directory.pages.entries()) {
    const view = await pageView(space, chunk, entry);
    if (view.large === null) continue;
    for (let row = 0; row < entry.rowCount; row++)
      if ((((view.large[row >>> 5] ?? 0) >>> (row & 31)) & 1) === 1) {
        const offsets = defined(view.unsigned ?? undefined);
        out.push({
          page,
          row: entry.firstRow + row,
          pointer: decodePointer(
            defined(view.data ?? undefined),
            offsets[row] ?? 0,
            offsets[row + 1] ?? 0,
          ),
        });
      }
  }
  reader.close();
  return out;
}

const pageRef = (chunk: ColumnChunk, entry: PageEntry): BlobRef => ({
  extent: chunk.data.extent,
  offset: chunk.data.offset + entry.offset,
  length: entry.length,
  checksum: entry.checksum,
});

async function pageBytes(space: MemoryExtentSpace, chunk: ColumnChunk, entry: PageEntry) {
  const lease = await space.read([pageRef(chunk, entry)], { budget: UNLIMITED });
  const bytes = defined(lease.bytes[0]).slice();
  lease.release();
  return bytes;
}

async function pageView(space: MemoryExtentSpace, chunk: ColumnChunk, entry: PageEntry) {
  return parsePage(await pageBytes(space, chunk, entry), entry, ValueTag.varlen);
}

// ── Round trips ────────────────────────────────────────────────────────────────────────────

const SIZES = [
  10,
  LARGE_VALUE_BYTES, // the largest inline value
  LARGE_VALUE_BYTES + 1, // the smallest out-of-line one
  512 * KiB,
  MiB, // one full part
  MiB + 1, // two parts, the last of one byte
  1.5 * MiB,
  5 * MiB, // more than one read
];

const reach = createReach("next-large-values", [
  "inline and out-of-line values on one page",
  "a null between out-of-line values",
  "a value split over several reads",
  "several values in one read",
  "a dictionary page beside an out-of-line page",
  "a selection that skips out-of-line rows",
]);

afterAll(() => {
  reach.assertReached();
});

describe.each(SEEDS)("out-of-line values round-trip (seed %i)", (seed) => {
  it("by range and by selection, whatever the layout, batches and type", async () => {
    const layout = fc.record({
      type: fc.constantFrom<PhysicalType>(
        { kind: "text" },
        { kind: "bytes" },
        { kind: "json", binary: false },
      ),
      // Small repeated values (dictionary-friendly), sizes from SIZES, and nulls.
      rows: fc.array(
        fc.oneof(
          { weight: 3, arbitrary: fc.nat({ max: 4 }).map((k) => ({ size: 3 + k, seed: k })) },
          {
            weight: 2,
            arbitrary: fc
              .constantFrom(...SIZES)
              .chain((size) => fc.nat({ max: 2 }).map((k) => ({ size, seed: 10 + k }))),
          },
          { weight: 1, arbitrary: fc.constant(null) },
        ),
        { minLength: 1, maxLength: 24 },
      ),
      batch: fc.integer({ min: 1, max: 9 }),
      pageBytes: fc.constantFrom(8, 256, 64 * KiB),
      // Dictionary-friendly rows ahead of the generated ones, so dictionary pages meet plain ones.
      repeated: fc.constantFrom(0, 2000),
      selectSeed: fc.nat(),
    });
    await fc.assert(
      fc.asyncProperty(layout, async ({ type, rows, batch, pageBytes, repeated, selectSeed }) => {
        const values: Array<Uint8Array | null> = [
          ...Array.from({ length: repeated }, (_, k) =>
            new TextEncoder().encode(`value-${String(k % 4)}-xxxxxxxxxxxx`),
          ),
          ...rows.map((row) => (row === null ? null : value(row.size, row.seed))),
        ];
        const selected = (row: number): boolean =>
          ((row * 2654435761 + selectSeed) >>> 0) % 3 !== 0;
        const total = values.reduce((sum, each) => sum + (each?.length ?? 0), 0);
        fc.pre(total <= 24 * MiB);
        const { space, group } = await writeGroup([{ type, values }], { batch, pageBytes });
        const chunk = defined(group.columns[0]);
        const reader = await openChunk(space, chunk);
        expectSame(await readAll(reader), values);
        const picked = Uint32Array.from(values.flatMap((_, row) => (selected(row) ? [row] : [])));
        expectSame(
          await gatherAll(reader, picked),
          Array.from(picked, (row) => defined(values[row])),
        );
        reader.close();

        // Declared reach, from what was written.
        reach.beginHistory();
        const large = (row: number): boolean => (values[row]?.length ?? 0) > LARGE_VALUE_BYTES;
        const reader2 = await openChunk(space, chunk);
        await reader2.load(allPages(reader2));
        for (const entry of reader2.directory.pages) {
          const range = Array.from({ length: entry.rowCount }, (_, k) => entry.firstRow + k);
          const inPage = range.filter(large);
          if (
            inPage.length > 0 &&
            inPage.length < range.filter((row) => values[row] !== null).length
          )
            reach.reach("inline and out-of-line values on one page");
          if (
            entry.encoding === PageEncoding.dictionary &&
            reader2.directory.pages.some((other) =>
              Array.from({ length: other.rowCount }, (_, k) => other.firstRow + k).some(large),
            )
          )
            reach.reach("a dictionary page beside an out-of-line page");
        }
        for (let row = 1; row + 1 < values.length; row++)
          if (values[row] === null && large(row - 1) && large(row + 1))
            reach.reach("a null between out-of-line values");
        if (values.some((each) => (each?.length ?? 0) > MAX_READ_BYTES))
          reach.reach("a value split over several reads");
        if (values.filter((each, row) => large(row) && (each?.length ?? 0) <= MiB).length >= 2)
          reach.reach("several values in one read");
        if (values.some((_, row) => large(row) && !selected(row)) && picked.length > 0)
          reach.reach("a selection that skips out-of-line rows");
        reader2.close();
      }),
      { seed, numRuns: 25 },
    );
  }, 240_000);
});

describe("out-of-line values", () => {
  it("stores values over LARGE_VALUE_BYTES out of line, in parts inside the chunk's data", async () => {
    const values = [
      value(LARGE_VALUE_BYTES, 1),
      value(LARGE_VALUE_BYTES + 1, 2),
      value(2.5 * MiB, 3),
    ];
    const { space, group } = await writeGroup([{ type: { kind: "bytes" }, values }]);
    const chunk = defined(group.columns[0]);
    const pointers = await pointersOf(space, chunk);
    expect(pointers.map(({ row }) => row)).toEqual([1, 2]);
    expect(pointers.map(({ pointer }) => pointer.checksums.length)).toEqual([1, 3]);
    for (const { pointer } of pointers) {
      expect(pointer.offset + pointer.length).toBeLessThanOrEqual(chunk.data.length);
      const parts = partsOf(pointer);
      expect(parts.every((part) => part.length <= VALUE_PART_BYTES)).toBe(true);
      expect(parts.reduce((sum, part) => sum + part.length, 0)).toBe(pointer.length);
    }
  });

  it("refuses a decode before the values are loaded, and counts their full length", async () => {
    const values = [value(10, 1), value(200 * KiB, 2), null];
    const { space, group } = await writeGroup([{ type: { kind: "text" }, values }]);
    const reader = await openChunk(space, defined(group.columns[0]));
    await reader.load(allPages(reader));
    expect(reader.largeRows(0, 3)).toEqual([1]);
    expect(reader.largeValueBytes(0, 3)).toBe(200 * KiB);
    expect(reader.varlenBytes(0, 3)).toBe(10 + 200 * KiB);
    const target = targetFor(reader, 3, reader.varlenBytes(0, 3));
    expect(() => reader.decodeRange(0, 3, target, 0)).toThrow(ValueNotLoadedError);
    // Rows without out-of-line values decode with nothing loaded.
    const first = targetFor(reader, 1, reader.varlenBytes(0, 1));
    reader.decodeRange(0, 1, first, 0);
    expectSame([valueAt(first, 0)], [values[0] ?? null]);
    reader.close();
  });

  it("round-trips a 64 MiB value, streams it in 1 MiB of memory, and refuses one byte more", async () => {
    const huge = value(MAX_VALUE_BYTES, 9);
    const { space, group } = await writeGroup([
      { type: { kind: "bytes" }, values: [value(5, 1), huge] },
    ]);
    const chunk = defined(group.columns[0]);
    const budget = tracked();
    const reader = await ChunkReader.open(recording(space.view({ cache: false })), chunk, {
      budget,
    });
    await reader.load(allPages(reader));
    const pages = budget.held;
    let offset = 0;
    let matches = true;
    for await (const part of reader.valueParts(1)) {
      matches &&= Buffer.from(part).equals(
        Buffer.from(huge.subarray(offset, offset + part.length)),
      );
      offset += part.length;
    }
    expect([offset, matches]).toEqual([MAX_VALUE_BYTES, true]);
    expect(budget.peak - pages).toBeLessThanOrEqual(VALUE_PART_BYTES);
    expect(budget.held).toBe(pages);
    // Loaded whole: charged at its length, in reads of at most MAX_READ_BYTES.
    const reads = recording(space.view({ cache: false }));
    const whole = await ChunkReader.open(reads, chunk, { budget });
    expectSame(await readAll(whole), [value(5, 1), huge]);
    expect(Math.max(...reads.reads)).toBeLessThanOrEqual(MAX_READ_BYTES);
    whole.close();
    reader.close();
    expect(budget.held).toBe(0);

    const writer = new GroupWriter({
      columns: [{ columnId: 1, type: { kind: "bytes" } }],
      writer: space.beginWrite("too large"),
    });
    await expect(
      writer.append([vectorOf({ kind: "bytes" }, [new Uint8Array(MAX_VALUE_BYTES + 1)])], 1),
    ).rejects.toThrow(UnsupportedPageLayoutError);
    await writer.abort();
  }, 120_000);

  it("charges values before it reads them, and gives everything back", async () => {
    const values = [value(3 * MiB, 1), value(100 * KiB, 2), value(100 * KiB, 3), value(9, 4)];
    const { space, group } = await writeGroup([{ type: { kind: "bytes" }, values }]);
    const chunk = defined(group.columns[0]);
    const cold = space.view({ cache: false });
    // A budget with room for the pages but not the values: refused, nothing held.
    const probe = tracked();
    const opened = await ChunkReader.open(cold, chunk, { budget: probe });
    await opened.load(allPages(opened));
    const pages = probe.held;
    opened.close();
    const budget = tracked(pages + 1 * MiB);
    const reader = await ChunkReader.open(cold, chunk, { budget });
    await reader.load(allPages(reader));
    const leased = cold.leased; // the pages
    await expect(Promise.resolve(reader.loadValues(0, 4))).rejects.toThrow(/budget|denied|bytes/iu);
    expect(budget.held).toBe(pages);
    expect(cold.leased).toBe(leased);
    // The small values fit: loaded together in one read, then released.
    const reads = recording(cold);
    const small = await ChunkReader.open(reads, chunk, { budget });
    await small.load(allPages(small));
    const before = reads.reads.length;
    await small.loadValues(1, 3);
    expect(reads.reads.length - before).toBe(1);
    expect(small.isValueLoaded(1) && small.isValueLoaded(2)).toBe(true);
    small.releaseValues([1]);
    expect(small.isValueLoaded(1)).toBe(false);
    small.release(allPages(small)); // the named pages, and their values
    expect(small.isValueLoaded(2)).toBe(false);
    small.close();
    reader.close();
    expect(budget.held).toBe(0);
    expect(cold.leased).toBe(0);
  });

  it("shares one read between concurrent loads, and a close in flight gives the read back", async () => {
    const values = [value(300 * KiB, 1), value(300 * KiB, 2)];
    const { space, group } = await writeGroup([{ type: { kind: "bytes" }, values }]);
    const chunk = defined(group.columns[0]);
    const cold = space.view({ cache: false });
    const reads = recording(cold);
    const budget = tracked();
    const reader = await ChunkReader.open(reads, chunk, { budget });
    await reader.load(allPages(reader));
    const before = reads.reads.length;
    await Promise.all([reader.loadValues(0, 2), reader.loadValues(0, 2), reader.loadValues(1, 2)]);
    expect(reads.reads.length - before).toBe(1);
    reader.releaseValues();
    const inFlight = reader.loadValues(0, 2);
    reader.close();
    await expect(Promise.resolve(inFlight)).rejects.toThrow(/closed/u);
    expect(budget.held).toBe(0);
    expect(cold.leased).toBe(0);
  });

  it("streams with one part held: a consumer that stops early or a close gives it back", async () => {
    const huge = value(3 * MiB + 5, 4);
    const { space, group } = await writeGroup([{ type: { kind: "bytes" }, values: [huge] }]);
    const chunk = defined(group.columns[0]);
    const cold = space.view({ cache: false });
    const budget = tracked();
    const reader = await ChunkReader.open(cold, chunk, { budget });
    await reader.load([0]);
    const pages = budget.held;
    const leased = cold.leased;
    for await (const part of reader.valueParts(0)) {
      expect(part.length).toBe(VALUE_PART_BYTES);
      expect(budget.held - pages).toBe(VALUE_PART_BYTES);
      break; // stops early: the part's lease goes back
    }
    expect([budget.held, cold.leased]).toEqual([pages, leased]);
    const stream = reader.valueParts(0);
    expect((await stream.next()).done).toBe(false);
    reader.close();
    await expect(stream.next()).rejects.toThrow(/closed/u);
    expect([budget.held, cold.leased]).toEqual([0, 0]);
  });

  it("loads synchronously when every part is resident", async () => {
    const values = [value(200 * KiB, 1)];
    const { space, group } = await writeGroup([{ type: { kind: "bytes" }, values }]);
    const chunk = defined(group.columns[0]);
    const warm = await ChunkReader.open(space, chunk, { budget: UNLIMITED });
    await warm.load([0]);
    await warm.loadValues(0, 1);
    warm.close();
    const budget = tracked();
    const reader = await ChunkReader.open(space, chunk, { budget });
    await reader.load([0]);
    const pages = budget.held;
    expect(reader.loadValues(0, 1)).toBeUndefined();
    expect(budget.held - pages).toBe(200 * KiB); // peeked parts are charged by the reader
    reader.close();
    expect(budget.held).toBe(0);
  });

  it("keeps statistics, the sketch and the dictionary right around out-of-line values", async () => {
    const big = (seed: number): Uint8Array => value(100 * KiB, seed);
    const small = Array.from({ length: 3000 }, (_, k) =>
      new TextEncoder().encode(`value-${String(k % 4)}-xxxxxxxxxxxx`),
    );
    // Small repeated values (dictionary pages) around out-of-line ones (a plain page).
    // The last large value sorts above every other: the chunk's max must cover it.
    const top = new Uint8Array(100 * KiB).fill(122);
    const values: TestValue[] = [...small, big(1), big(2), big(1), top, null, ...small];
    const { space, group } = await writeGroup([{ type: { kind: "bytes" }, values }]);
    const chunk = defined(group.columns[0]);
    const pages = (await openChunk(space, chunk)).directory;
    const holdsLarge = (page: PageEntry): boolean =>
      [3000, 3001, 3002, 3003].some(
        (row) => row >= page.firstRow && row < page.firstRow + page.rowCount,
      );
    expect(pages.pages.filter(holdsLarge).length).toBeGreaterThan(0);
    // The dictionary carries on after the plain page: later pages are dictionary-encoded too.
    const last = defined(pages.pages[pages.pages.length - 1]);
    expect(last.encoding).toBe(PageEncoding.dictionary);
    expect(
      pages.pages.filter(holdsLarge).every((page) => page.encoding === PageEncoding.plain),
    ).toBe(true);
    expect(pages.pages.some((page) => page.encoding === PageEncoding.dictionary)).toBe(true);
    expect(pages.dictionary).not.toBeNull();
    const { min, max } = chunk.statistics;
    if (!(min instanceof Uint8Array) || !(max instanceof Uint8Array)) throw new Error("no bounds");
    expect(Buffer.compare(Buffer.from(max), Buffer.from(top))).toBeGreaterThanOrEqual(0);
    for (const each of values)
      if (each instanceof Uint8Array) {
        expect(Buffer.compare(Buffer.from(min), Buffer.from(each))).toBeLessThanOrEqual(0);
        expect(Buffer.compare(Buffer.from(max), Buffer.from(each))).toBeGreaterThanOrEqual(0);
      }
    const reader = await openChunk(space, chunk);
    const sketch = DistinctSketch.parse(defined(reader.directory.sketch ?? undefined));
    expect(Math.round(sketch.estimate())).toBe(7); // four small and three distinct large values
    expectSame(await readAll(reader), values);
    reader.close();
  });

  it("refuses a damaged part", async () => {
    const values = [value(200 * KiB, 1)];
    const { space, group } = await writeGroup([{ type: { kind: "bytes" }, values }]);
    const chunk = defined(group.columns[0]);
    const { pointer } = defined((await pointersOf(space, chunk))[0]);
    // A damaged part fails its checksum in the space; nothing is held.
    const damaged = new MemoryExtentSpace({ cache: false });
    damaged.bytes = space.bytes.slice();
    const at = chunk.data.extent.offset + chunk.data.offset + pointer.offset + 77;
    damaged.bytes[at] = (damaged.bytes[at] ?? 0) ^ 1;
    const budget = tracked();
    const reader = await ChunkReader.open(damaged, chunk, { budget });
    await reader.load([0]);
    const pages = budget.held;
    await expect(Promise.resolve(reader.loadValues(0, 1))).rejects.toThrow(StoreCorruptionError);
    expect(budget.held).toBe(pages);
    reader.close();

    // Pointers that alias or overlap: page/review-p2b.test.ts (generated forgeries).
  });

  it("refuses malformed pointers and a large-value bit on a null row", async () => {
    const pointer = { offset: 4096, length: MiB + 5, checksums: [1, 2] };
    const bytes = encodePointer(pointer);
    expect(decodePointer(bytes, 0, bytes.length)).toEqual(pointer);
    const bad = (edit: (view: DataView) => void, length = bytes.length): void => {
      const copy = bytes.slice(0, length);
      if (length === bytes.length) edit(new DataView(copy.buffer));
      expect(() => decodePointer(copy, 0, copy.length)).toThrow(PageCorruptionError);
    };
    bad((view) => view.setUint8(0, 2)); // version
    bad((view) => view.setUint8(1, 1)); // reserved byte
    bad((view) => view.setUint16(2, 3, true)); // part count disagrees with the length
    bad((view) => view.setUint32(4, LARGE_VALUE_BYTES, true)); // small enough to be inline
    bad((view) => view.setUint32(4, MAX_VALUE_BYTES + 1, true)); // too large
    bad((view) => view.setUint32(12, 0x0020_0000, true)); // offset past 2^53
    bad(() => undefined, bytes.length - 1); // truncated
    expect(() => encodePointer({ ...pointer, checksums: [1] })).toThrow(RangeError);

    const values = [value(100 * KiB, 1), null, value(5, 2)];
    const { space, group } = await writeGroup([{ type: { kind: "bytes" }, values }]);
    const chunk = defined(group.columns[0]);
    const entry = defined((await openChunk(space, chunk)).directory.pages[0]);
    const page = await pageBytes(space, chunk, entry);
    expect(parsePage(page, entry, ValueTag.varlen).large?.[0]).toBe(1);
    // The large section follows the header (16 bytes) and the validity words (8-aligned).
    const largeAt = 16 + 8;
    expect(new Uint32Array(page.buffer, page.byteOffset + largeAt, 1)[0]).toBe(1);
    page[largeAt] = 0b011; // row 1 is null
    expect(() => parsePage(page, entry, ValueTag.varlen)).toThrow(/null row is marked large/u);
    page[largeAt] = 0b1001; // row 3 is past the page
    expect(() => parsePage(page, entry, ValueTag.varlen)).toThrow(/past the page/u);
  });

  it("is reached by the walk through its chunk, and reclaimed with it on abort", async () => {
    const space = new MemoryExtentSpace();
    const writer = space.beginWrite("table");
    const group = new GroupWriter({ columns: [{ columnId: 1, type: { kind: "bytes" } }], writer });
    await group.append([vectorOf({ kind: "bytes" }, [value(3 * MiB, 1), value(7, 2)])], 2);
    const written = await group.finish();
    const root = defined(
      (
        await writer.write([
          encodeGroupRoot({ rowCount: 2, rowIds: { kind: "dense" }, columns: written.columns }),
        ])
      ).blobs[0],
    );
    const directory = await writeGroupDirectory(writer, [
      {
        groupId: 1,
        firstRowId: 1,
        lastRowId: 2,
        rowCount: 2,
        liveRowCount: 2,
        root,
        deletions: null,
      },
    ]);
    const catalog = await writeTestCatalog(writer);
    const manifest: ManifestRoot = {
      formatVersion: MANIFEST_FORMAT_VERSION,
      sequence: 1,
      epoch: 1,
      log: { salt: 1, firstLsn: 1, rootSequence: 1 },
      globalLsn: 1,
      catalogVersion: 1,
      catalog,
      nextTableId: 2,
      sequences: [],
      tables: [
        {
          tableId: 1,
          schemaVersion: 1,
          nextRowId: 3,
          flushedThroughLsn: 0,
          liveRowCount: 2,
          groups: directory.ref,
          groupCount: 1,
          staged: [],
          keyIndex: null,
          secondaryIndexes: [],
          statistics: null,
        },
      ],
      clients: [],
      extentList: [],
    };
    const key = (extent: { offset: number; length: number }): string =>
      `${String(extent.offset)}:${String(extent.length)}`;
    const reached = new Set<string>();
    for await (const ref of reachableExtents()(encodeManifestRoot(manifest), space, UNLIMITED))
      reached.add(key("extent" in ref ? ref.extent : ref));
    expect(reached).toEqual(new Set(writer.extents.map(key)));
    const chunk = defined(written.columns[0]);
    for (const { pointer } of await pointersOf(space, chunk))
      expect(pointer.offset + pointer.length).toBeLessThanOrEqual(chunk.data.length);

    const aborted = new MemoryExtentSpace();
    const abortWriter = aborted.beginWrite("aborted");
    const abortGroup = new GroupWriter({
      columns: [{ columnId: 1, type: { kind: "bytes" } }],
      writer: abortWriter,
    });
    await abortGroup.append([vectorOf({ kind: "bytes" }, [value(3 * MiB, 1)])], 1);
    await abortGroup.abort();
    expect(aborted.allocator.reserved).toBe(0);
    expect(abortWriter.extents).toEqual([]);
  });

  it("stores array values out of line like any varlen value, and decodes them back", async () => {
    const element: PhysicalType = { kind: "int64" };
    const type: PhysicalType = { kind: "array", element };
    const arrayOf = (count: number, seed: number): ArrayValue => ({
      dimensions: [count],
      lowerBounds: [1],
      elements: Array.from({ length: count }, (_, k) =>
        k % 97 === 0 ? null : BigInt(k * 1_000_003 + seed),
      ),
    });
    const arrays: Array<ArrayValue | null> = [
      arrayOf(3, 1), // inline
      arrayOf(20_000, 2), // one part
      null,
      arrayOf(300_000, 3), // three parts
      arrayOf(5, 4),
    ];
    const values = arrays.map((each) => (each === null ? null : encodeArrayValue(element, each)));
    expect((values[3]?.length ?? 0) > 2 * MiB).toBe(true);
    const { space, group } = await writeGroup([{ type, values }]);
    const chunk = defined(group.columns[0]);
    expect((await pointersOf(space, chunk)).map(({ row }) => row)).toEqual([1, 3]);
    const reader = await openChunk(space, chunk);
    const read = await readAll(reader);
    expectSame(read, values);
    read.forEach((bytes, row) => {
      expect(bytes === null ? null : decodeArrayValue(element, bytes as Uint8Array)).toEqual(
        arrays[row],
      );
    });
    expectSame(await gatherAll(reader, Uint32Array.of(1, 4)), [
      values[1] ?? null,
      values[4] ?? null,
    ]);
    reader.close();
  });

  it("ends a group before a varlen chunk passes maxChunkBytes, but always takes one row", async () => {
    const space = new MemoryExtentSpace();
    const group = new GroupWriter({
      columns: [{ columnId: 1, type: { kind: "bytes" } }],
      writer: space.beginWrite("capped"),
      maxChunkBytes: 1 * MiB,
    });
    const values = Array.from({ length: 6 }, (_, k) => value(300 * KiB, k));
    // 300 KiB + 4 per row: three fit in 1 MiB, the fourth would pass it.
    expect(await group.append([vectorOf({ kind: "bytes" }, values)], values.length)).toBe(3);
    expect(await group.append([vectorOf({ kind: "bytes" }, values)], 1, 3)).toBe(0);
    const written = await group.finish();
    expect(written.rowCount).toBe(3);
    const alone = new GroupWriter({
      columns: [{ columnId: 1, type: { kind: "bytes" } }],
      writer: space.beginWrite("one row"),
      maxChunkBytes: 100 * KiB,
    });
    expect(await alone.append([vectorOf({ kind: "bytes" }, [value(200 * KiB, 1)])], 1)).toBe(1);
    expectSame(await readAll(await openChunk(space, defined((await alone.finish()).columns[0]))), [
      value(200 * KiB, 1),
    ]);
  });

  it("writes values straight from the caller's vector: the writer's memory does not grow with them", async () => {
    const peak = async (size: number): Promise<number> => {
      const space = new MemoryExtentSpace();
      const group = new GroupWriter({
        columns: [{ columnId: 1, type: { kind: "bytes" } }],
        writer: space.beginWrite("memory"),
      });
      const values = Array.from({ length: 4 }, (_, k) => value(size, k));
      await group.append([vectorOf({ kind: "bytes" }, values)], values.length);
      await group.finish();
      return group.peakWorkingBytes;
    };
    const small = await peak(LARGE_VALUE_BYTES + 1);
    const large = await peak(6 * MiB);
    expect(large - small).toBeLessThan(64 * KiB);
  });
});
