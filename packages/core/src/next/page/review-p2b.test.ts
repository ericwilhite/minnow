/**
 * Review B2-p2b (rebuild/format-p2b b7383b2): each probe as a test that rejects it, plus
 * generated forgeries. Findings: 1 pointers were checked only against the chunk, so one could
 * alias another value or a page; 2 a `valueParts` stream outlived `close`; 3 a release during
 * `loadValues` was undone when the load completed; 4 a stream may yield verified parts before a
 * later part fails (documented: the whole stream is then invalid).
 */
import fc from "fast-check";
import { afterAll, describe, expect, it } from "vitest";
import { createReach } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import { xxHash32 } from "../hash/xxhash32.js";
import type { ColumnChunk, PageEntry } from "../format/manifest.js";
import { StoreCorruptionError } from "../storage/contract.js";
import type { ByteBudget } from "../storage/extents.js";
import { parsePage, ValueNotLoadedError } from "./decode.js";
import { encodeDirectory, type ChunkDirectory } from "./directory.js";
import { PageCorruptionError, ValueTag } from "./format.js";
import { decodePointer, LARGE_VALUE_BYTES } from "./large-value.js";
import { MemoryExtentSpace, UNLIMITED } from "./memory-extents.js";
import { ChunkReader } from "./reader.js";
import { defined, targetFor, valueAt, writeGroup } from "./test-support.js";

const KiB = 1024;
const MiB = 1024 * 1024;

function tracked(): ByteBudget & { held: number } {
  const budget = {
    held: 0,
    allocate(bytes: number): boolean {
      budget.held += bytes;
      return true;
    },
    free(bytes: number): void {
      budget.held -= bytes;
    },
  };
  return budget;
}

async function fixture(
  values: ReadonlyArray<Uint8Array | null>,
  options: { pageBytes?: number; dictionaryBytes?: number } = {},
): Promise<{ space: MemoryExtentSpace; chunk: ColumnChunk; directory: ChunkDirectory }> {
  const { space, group } = await writeGroup([{ type: { kind: "bytes" }, values }], options);
  const chunk = defined(group.columns[0]);
  const reader = await ChunkReader.open(space, chunk, { budget: UNLIMITED });
  const directory = reader.directory;
  reader.close();
  return { space, chunk, directory };
}

const all = (reader: ChunkReader): number[] => reader.directory.pages.map((_, page) => page);

/** Opens, loads, loads values and decodes everything (what a forged chunk must not survive). */
async function readEverything(space: MemoryExtentSpace, chunk: ColumnChunk): Promise<void> {
  const reader = await ChunkReader.open(space.view({ cache: false }), chunk, { budget: UNLIMITED });
  try {
    await reader.load(all(reader));
    await reader.loadValues(0, reader.rowCount);
    const target = targetFor(reader, reader.rowCount, reader.varlenBytes(0, reader.rowCount));
    reader.decodeRange(0, reader.rowCount, target, 0);
  } finally {
    reader.close();
  }
}

/** The page's stored bytes (a copy), where each large row's pointer is in them, and the view. */
function pageOf(space: MemoryExtentSpace, chunk: ColumnChunk, entry: PageEntry) {
  const address = chunk.data.extent.offset + chunk.data.offset + entry.offset;
  const raw = space.bytes.slice(address, address + entry.length);
  const view = parsePage(raw, entry, ValueTag.varlen);
  const data = defined(view.data ?? undefined);
  const offsets = defined(view.unsigned ?? undefined);
  const base = data.byteOffset - raw.byteOffset;
  const rows: Array<{ row: number; at: number; end: number }> = [];
  for (let row = 0; row < entry.rowCount; row++)
    if ((((view.large?.[row >>> 5] ?? 0) >>> (row & 31)) & 1) === 1)
      rows.push({ row, at: base + (offsets[row] ?? 0), end: base + (offsets[row + 1] ?? 0) });
  return { address, raw, rows };
}

/**
 * Writes `raw` back in place of page `index`, and a new directory naming it (with `entry`
 * changes), checksums recomputed: a forgery only the format's own rules can refuse.
 */
async function forge(
  space: MemoryExtentSpace,
  chunk: ColumnChunk,
  directory: ChunkDirectory,
  index: number,
  raw: Uint8Array,
  address: number,
  entry: (entry: PageEntry) => PageEntry = (same) => same,
): Promise<ColumnChunk> {
  space.bytes.set(raw, address);
  space.evict();
  const pages = directory.pages.map((page, k) =>
    k === index ? entry({ ...page, checksum: xxHash32(raw) }) : page,
  );
  const blob = defined(
    (await space.beginWrite("forged").write([encodeDirectory({ ...directory, pages })])).blobs[0],
  );
  return { ...chunk, directory: blob };
}

const fill = (length: number, byte: number): Uint8Array => new Uint8Array(length).fill(byte);

describe("review B2-p2b #1: pointers tile their page's region, so nothing aliases", () => {
  it("records each page's region in its directory entry", async () => {
    const { directory } = await fixture([fill(10, 1), fill(100 * KiB, 2), fill(200 * KiB, 3)]);
    const page = defined(directory.pages[0]);
    expect(page.largeRegion?.length).toBe(300 * KiB);
    expect(defined(page.largeRegion).offset + 300 * KiB).toBeLessThanOrEqual(page.offset);
  });

  it("refuses the probe: row 1 holding row 0's pointer", async () => {
    const { space, chunk, directory } = await fixture([fill(100_000, 65), fill(100_000, 66)]);
    const entry = defined(directory.pages[0]);
    const { address, raw, rows } = pageOf(space, chunk, entry);
    const [first, second] = rows.map(defined);
    raw.copyWithin(defined(second).at, defined(first).at, defined(first).end);
    const forged = await forge(space, chunk, directory, 0, raw, address);
    await expect(readEverything(space, forged)).rejects.toThrow(PageCorruptionError);
  });

  it("refuses the probe: a pointer at another page's bytes", async () => {
    const { space, chunk, directory } = await fixture([fill(65_536, 65), fill(100_000, 66)], {
      dictionaryBytes: 0,
    });
    const index = directory.pages.findIndex((page) => page.largeRegion !== undefined);
    const prior = defined(directory.pages[index - 1]);
    const entry = defined(directory.pages[index]);
    const { address, raw, rows } = pageOf(space, chunk, entry);
    const view = new DataView(raw.buffer, defined(rows[0]).at);
    view.setUint32(4, Math.max(prior.length, LARGE_VALUE_BYTES + 1), true);
    view.setUint32(8, prior.offset, true);
    view.setUint32(12, 0, true);
    const forged = await forge(space, chunk, directory, index, raw, address);
    await expect(readEverything(space, forged)).rejects.toThrow(PageCorruptionError);
  });

  const SEEDS = seedsFor("next-large-value-forgery", [0x7a11, 0x3c]);
  const kinds = [
    "alias another row",
    "swap two rows",
    "shift a pointer",
    "point at a page",
    "point at the dictionary",
    "change a length",
    "region over the previous page",
    "region into its own page",
    "region shifted",
    "region and pointers moved onto the previous page",
    "region and pointers moved onto their own page",
  ] as const;
  type Kind = (typeof kinds)[number];
  const reach = createReach("next-large-value-forgery", kinds);
  afterAll(() => {
    reach.assertReached();
  });

  it.each(SEEDS)(
    "refuses every generated alias or overlap forgery (seed %i)",
    async (seed) => {
      const plan = fc.record({
        sizes: fc.array(fc.integer({ min: LARGE_VALUE_BYTES + 1, max: 300 * KiB }), {
          minLength: 2,
          maxLength: 5,
        }),
        kind: fc.constantFrom(...kinds),
        pick: fc.nat(),
        delta: fc.constantFrom(-4096, -1, 1, 16, 4096),
      });
      await fc.assert(
        fc.asyncProperty(plan, async ({ sizes, kind, pick, delta }) => {
          // Small repeated values first (a dictionary page and a page before the large ones).
          const small = Array.from({ length: 3000 }, (_, k) => fill(20, 97 + (k % 4)));
          const values = [...small, ...sizes.map((size, k) => fill(size, 65 + k)), ...small];
          const { space, chunk, directory } = await fixture(values);
          const index = directory.pages.findIndex((page) => page.largeRegion !== undefined);
          const entry = defined(directory.pages[index]);
          const region = defined(entry.largeRegion);
          const { address, raw, rows } = pageOf(space, chunk, entry);
          const one = defined(rows[pick % rows.length]);
          const other = defined(rows[(pick + 1) % rows.length]);
          const view = (at: number): DataView => new DataView(raw.buffer, at);
          const pointerOf = (row: { at: number; end: number }) =>
            decodePointer(raw, row.at, row.end);
          let edit: ((page: PageEntry) => PageEntry) | undefined;
          const kindDone = (name: Kind): void => {
            reach.beginHistory();
            reach.reach(name);
          };
          switch (kind) {
            case "alias another row": {
              fc.pre(one !== other && one.end - one.at === other.end - other.at);
              raw.copyWithin(other.at, one.at, one.end);
              break;
            }
            case "swap two rows": {
              fc.pre(one !== other && one.end - one.at === other.end - other.at);
              const copy = raw.slice(one.at, one.end);
              raw.copyWithin(one.at, other.at, other.end);
              raw.set(copy, other.at);
              break;
            }
            case "shift a pointer":
              view(one.at).setUint32(8, pointerOf(one).offset + delta, true);
              break;
            case "point at a page": {
              const target = defined(directory.pages[pick % directory.pages.length]);
              view(one.at).setUint32(8, target.offset, true);
              break;
            }
            case "point at the dictionary": {
              const dictionary = directory.dictionary;
              fc.pre(dictionary !== null);
              view(one.at).setUint32(8, dictionary.offset, true);
              break;
            }
            case "change a length": {
              const length = pointerOf(one).length + delta;
              fc.pre(Math.ceil(length / MiB) === pointerOf(one).checksums.length);
              view(one.at).setUint32(4, length, true);
              break;
            }
            case "region over the previous page": {
              const previous = directory.pages[index - 1];
              fc.pre(previous !== undefined);
              edit = (page) => ({
                ...page,
                largeRegion: { offset: defined(previous).offset, length: region.length },
              });
              break;
            }
            case "region into its own page":
              edit = (page) => ({
                ...page,
                largeRegion: { offset: region.offset + Math.abs(delta), length: region.length },
              });
              break;
            case "region shifted":
              edit = (page) => ({
                ...page,
                largeRegion: { offset: region.offset + delta, length: region.length },
              });
              break;
          }
          if (kind.startsWith("region and pointers")) {
            // Moved together, the pointers still tile the region: only its placement is wrong.
            const onto =
              kind === "region and pointers moved onto the previous page"
                ? directory.pages[index - 1]?.offset
                : entry.offset;
            fc.pre(onto !== undefined);
            const shift = defined(onto) - region.offset;
            for (const row of rows) view(row.at).setUint32(8, pointerOf(row).offset + shift, true);
            edit = (page) => ({
              ...page,
              largeRegion: { offset: region.offset + shift, length: region.length },
            });
          }
          kindDone(kind);
          const forged = await forge(space, chunk, directory, index, raw, address, edit);
          await expect(readEverything(space, forged)).rejects.toThrow(PageCorruptionError);
        }),
        { seed, numRuns: 40 },
      );
    },
    240_000,
  );
});

describe("review B2-p2b #2: close ends every stream and releases what it holds", () => {
  it("releases a suspended stream's part at once (the probe)", async () => {
    const { space, chunk } = await fixture([fill(3 * MiB, 65)]);
    const cold = space.view({ cache: false });
    const budget = tracked();
    const reader = await ChunkReader.open(cold, chunk, { budget });
    await reader.load([0]);
    const stream = reader.valueParts(0);
    const first = await stream.next();
    expect(first.done === true ? 0 : first.value.length).toBe(MiB);
    reader.close();
    expect([budget.held, cold.leased]).toEqual([0, 0]);
    await expect(stream.next()).rejects.toThrow(/closed/u);
    await stream.return(undefined);
    expect([budget.held, cold.leased]).toEqual([0, 0]);
  });

  it("ends a stream over a loaded value: nothing after close (the probe)", async () => {
    const { space, chunk } = await fixture([fill(3 * MiB, 65)]);
    const cold = space.view({ cache: false });
    const budget = tracked();
    const reader = await ChunkReader.open(cold, chunk, { budget });
    await reader.load([0]);
    await reader.loadValues(0, 1);
    const stream = reader.valueParts(0);
    await stream.next();
    reader.close();
    expect(budget.held).toBe(0);
    await expect(stream.next()).rejects.toThrow(/closed/u);
  });

  it("refuses a stream whose read completes after close, and one never started", async () => {
    const { space, chunk } = await fixture([fill(3 * MiB, 65)]);
    const cold = space.view({ cache: false });
    const budget = tracked();
    const reader = await ChunkReader.open(cold, chunk, { budget });
    await reader.load([0]);
    const stream = reader.valueParts(0);
    const unstarted = reader.valueParts(0);
    const first = stream.next(); // its read is in flight
    reader.close();
    await expect(first).rejects.toThrow(/closed/u);
    await expect(unstarted.next()).rejects.toThrow(/closed/u);
    expect([budget.held, cold.leased]).toEqual([0, 0]);
  });

  it("keeps streaming from storage when the loaded value is released mid-stream", async () => {
    const value = Uint8Array.from({ length: 3 * MiB }, (_, k) => k % 251);
    const { space, chunk } = await fixture([value]);
    const reader = await ChunkReader.open(space.view({ cache: false }), chunk, {
      budget: tracked(),
    });
    await reader.load([0]);
    await reader.loadValues(0, 1);
    const parts: Uint8Array[] = [];
    for await (const part of reader.valueParts(0)) {
      parts.push(part.slice());
      if (parts.length === 1) reader.releaseValues();
    }
    expect(Buffer.concat(parts).equals(Buffer.from(value))).toBe(true);
    reader.close();
  });
});

describe("review B2-p2b #3: a release during loadValues wins", () => {
  it("does not resurrect a value released while it loads (the probe)", async () => {
    const { space, chunk } = await fixture([fill(3 * MiB, 65)]);
    const cold = space.view({ cache: false });
    const budget = tracked();
    const reader = await ChunkReader.open(cold, chunk, { budget });
    const directoryCharge = budget.held;
    await reader.load([0]);
    const loading = reader.loadValues(0, 1);
    reader.release([0]);
    await loading;
    expect([reader.isLoaded(0), reader.isValueLoaded(0), budget.held]).toEqual([
      false,
      false,
      directoryCharge,
    ]);
    reader.close();
    expect(cold.leased).toBe(0);
  });

  it("discards grouped and split reads released in flight, and a retry loads afresh", async () => {
    const values = [fill(300 * KiB, 1), fill(300 * KiB, 2), fill(6 * MiB, 3)];
    const { space, chunk } = await fixture(values);
    const cold = space.view({ cache: false });
    const budget = tracked();
    const reader = await ChunkReader.open(cold, chunk, { budget });
    await reader.load(all(reader));
    const pages = budget.held;
    // Rows 0 and 1 share one read; row 2 is split over two.
    const loading = reader.loadValues(0, 3);
    reader.releaseValues([0, 2]);
    await loading;
    expect([0, 1, 2].map((row) => reader.isValueLoaded(row))).toEqual([false, true, false]);
    // Rows 0 and 1 came in one lease: it stays charged until its last value goes.
    expect(budget.held - pages).toBe(600 * KiB);
    reader.releaseValues([1]);
    expect(budget.held).toBe(pages);
    // A retry reads again (it does not wait for the discarded load) and installs.
    await reader.loadValues(0, 3);
    expect([0, 1, 2].map((row) => reader.isValueLoaded(row))).toEqual([true, true, true]);
    const target = targetFor(reader, 3, reader.varlenBytes(0, 3));
    reader.decodeRange(0, 3, target, 0);
    expect((valueAt(target, 2) as Uint8Array).length).toBe(6 * MiB);
    // Releasing everything while a load is in flight leaves nothing behind.
    reader.releaseValues();
    const again = reader.loadValues(0, 3);
    reader.releaseValues();
    await again;
    expect(budget.held).toBe(pages);
    reader.close();
    expect([budget.held, cold.leased]).toEqual([0, 0]);
  });
});

describe("review B2-p2b #4: a stream that fails is wholly invalid; decodes verify first", () => {
  it("yields a verified part, then fails on the damaged one (the probe); loadValues refuses all", async () => {
    const { space, chunk, directory } = await fixture([fill(3 * MiB, 65)]);
    const region = defined(defined(directory.pages[0]).largeRegion);
    const cold = space.view({ cache: false });
    const damaged = chunk.data.extent.offset + chunk.data.offset + region.offset + MiB + 50;
    cold.bytes[damaged] = (cold.bytes[damaged] ?? 0) ^ 1;
    const budget = tracked();
    const reader = await ChunkReader.open(cold, chunk, { budget });
    await reader.load([0]);
    const pages = budget.held;
    let yielded = 0;
    await expect(
      (async () => {
        for await (const part of reader.valueParts(0)) yielded += part.length;
      })(),
    ).rejects.toThrow(StoreCorruptionError);
    expect(yielded).toBe(MiB); // the consumer must discard it
    await expect(Promise.resolve(reader.loadValues(0, 1))).rejects.toThrow(StoreCorruptionError);
    expect([reader.isValueLoaded(0), budget.held]).toEqual([false, pages]);
    const target = targetFor(reader, 1, reader.varlenBytes(0, 1));
    expect(() => reader.decodeRange(0, 1, target, 0)).toThrow(ValueNotLoadedError);
    reader.close();
  });
});
