/**
 * Increment 9's export read path:
 * - the writer starts every page, dictionary page and directory at an 8-byte offset of its
 *   extent (page/writer.ts `appendTo`), so pages read together into one buffer are viewed in
 *   place by typed arrays;
 * - a one-pass read's misses are verified views of one buffer per slab that only its lease
 *   holds, charged for their bytes and the gaps between them, never copied, never cached
 *   (storage/page-cache.ts `#readPrivate`);
 * - `ChunkReader.loadTogether` loads several readers' pages through shared reads and installs
 *   exactly what each reader's own `load` would.
 */
import fc from "fast-check";
import { afterAll, describe, expect, it } from "vitest";
import { createReach } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import type { ColumnChunk } from "../format/manifest.js";
import type { PhysicalType } from "../types.js";
import type { BlobRef } from "../storage/extents.js";
import { StoreCorruptionError, type StoreSession } from "../storage/contract.js";
import { MemoryBackend } from "../storage/memory-store.js";
import { countingBudget, countingMemory, openOver } from "../storage/space-test-support.js";
import { CachedBytesWrittenError } from "../storage/verify-cached.js";
import { ChunkReader } from "./reader.js";
import {
  defined,
  sameValue,
  targetFor,
  UNLIMITED,
  valueAt,
  vectorOf,
  writeGroup,
  type TestValue,
} from "./test-support.js";
import { GroupWriter } from "./writer.js";

const ints = { kind: "int32", bits: 32 } as const;
const encoder = new TextEncoder();

/** One small-paged column (int, or plain text) on the real extent space and page cache. */
async function onDisk(wrap?: (session: StoreSession) => StoreSession, text = false) {
  const memory = countingMemory(Number.POSITIVE_INFINITY);
  const { space, budget } = await openOver(new MemoryBackend(), {
    memory,
    ...(wrap === undefined ? {} : { wrap }),
  });
  const type = text ? ({ kind: "text" } as const) : ints;
  const values: TestValue[] = text
    ? Array.from({ length: 20_000 }, (_, row) => encoder.encode(`row ${String(row * 7919)}`))
    : Array.from({ length: 20_000 }, (_, row) => (row * 7919) % 100_003);
  const writer = new GroupWriter({
    columns: [{ columnId: 1, type }],
    writer: space.beginWrite("export path"),
    inlineBytes: 0,
    pageBytes: 1024,
    rawPageBytes: 1024,
  });
  await writer.append([vectorOf(type, values)], values.length);
  const chunk = defined((await writer.finish()).columns[0]);
  const probe = await ChunkReader.open(space, chunk, { budget, once: true });
  const refs: BlobRef[] = probe.directory.pages.map((page) => ({
    extent: chunk.data.extent,
    offset: chunk.data.offset + page.offset,
    length: page.length,
    checksum: page.checksum,
  }));
  probe.close();
  return { space, budget, memory, chunk, refs, values };
}

const address = (ref: BlobRef): number => ref.extent.offset + ref.offset;

/** `session` with its data reads replaced (its other methods bound to it). */
function withRead(
  session: StoreSession,
  readDataInto: (at: number, target: Uint8Array) => Promise<void>,
): StoreSession {
  return new Proxy(session, {
    get(target, property) {
      if (property === "readDataInto") return readDataInto;
      const value: unknown = Reflect.get(target, property, target);
      return typeof value === "function"
        ? (value as (...args: unknown[]) => unknown).bind(target)
        : value;
    },
  });
}

describe("one-pass misses: views of one buffer per slab, charged with their gaps", () => {
  it("are views of one buffer, charged for blobs and gaps, never cached, all given back", async () => {
    const { space, budget, memory, refs } = await onDisk();
    const wanted = refs.slice(0, 12);
    const before = memory.used;
    const lease = await space.read(wanted, { budget, once: true });
    const buffers = new Set(lease.bytes.map((bytes) => bytes.buffer));
    expect(buffers.size).toBe(1); // one slab, one buffer
    for (const bytes of lease.bytes) expect(bytes.byteOffset % 8).toBe(0); // aligned views
    const last = defined(wanted.at(-1));
    const span = address(last) + last.length - address(defined(wanted[0]));
    expect(budget.used).toBe(span); // the blobs and any padding between them
    lease.release();
    expect(budget.used).toBe(0);
    expect(memory.used).toBe(before);
    expect(wanted.every((ref) => space.peek(ref, { once: true }) === null)).toBe(true);
  });

  it("charge the padding between blobs too (pages of any length)", async () => {
    const { space, budget, refs } = await onDisk(undefined, true);
    const wanted = refs.slice(0, 12);
    const blobs = wanted.reduce((sum, ref) => sum + ref.length, 0);
    const last = defined(wanted.at(-1));
    const span = address(last) + last.length - address(defined(wanted[0]));
    expect(span).toBeGreaterThan(blobs); // padded: the writer aligned pages of odd lengths
    const lease = await space.read(wanted, { budget, once: true });
    expect(budget.used).toBe(span);
    lease.release();
    expect(budget.used).toBe(0);
  });

  it("refuse a corrupt blob and give everything back", async () => {
    let corrupt: number | null = null;
    const { space, budget, refs } = await onDisk((session) =>
      withRead(session, async (at, target) => {
        await session.readDataInto(at, target);
        if (corrupt !== null && corrupt >= at && corrupt < at + target.length)
          target[corrupt - at] = (target[corrupt - at] ?? 0) ^ 1;
      }),
    );
    corrupt = address(defined(refs[5])) + 3;
    await expect(space.read(refs.slice(0, 12), { budget, once: true })).rejects.toThrow(
      StoreCorruptionError,
    );
    expect(budget.used).toBe(0);
  });

  it("stop on abort and give everything back", async () => {
    const controller = new AbortController();
    const { space, budget, refs } = await onDisk((session) =>
      withRead(session, async (at, target) => {
        await session.readDataInto(at, target);
        controller.abort();
      }),
    );
    // Two slabs (a gap wider than the slab gap between them): aborted after the first.
    const far = defined(refs.at(-1));
    await expect(
      space.read([defined(refs[0]), far], { budget, once: true, signal: controller.signal }),
    ).rejects.toThrow();
    expect(budget.used).toBe(0);
  });

  it("are still guarded against writes by their consumer", async () => {
    const { space, budget, refs } = await onDisk();
    const lease = await space.read(refs.slice(0, 4), { budget, once: true });
    const bytes = defined(lease.bytes[1]);
    const flip = (): void => {
      bytes[2] = (bytes[2] ?? 0) ^ 1;
    };
    flip();
    expect(() => lease.release()).toThrow(CachedBytesWrittenError);
    flip();
  });

  it("decode as the cached path does through a one-pass reader", async () => {
    const { space, budget, chunk, values } = await onDisk();
    const reader = await ChunkReader.open(space, chunk, { budget, once: true });
    await reader.load(reader.directory.pages.map((_, page) => page));
    const target = targetFor(reader, values.length, 0);
    reader.decodeRange(0, values.length, target, 0);
    expect(Array.from({ length: values.length }, (_, row) => valueAt(target, row))).toEqual(values);
    reader.close();
    expect(budget.used).toBe(0);
  });
});

// ── Generated: alignment and loadTogether ─────────────────────────────────────────────────────

const SEEDS = seedsFor("next-export-path", [0x919, 0x2c]);
const reach = createReach("next-export-path", [
  "a packed chunk",
  "a chunk in its own extent",
  "a dictionary page",
  "a large value",
  "a reader loading alone (dictionary)",
  "a reader closed before the shared load settles",
  "pages already loaded",
]);
afterAll(() => {
  reach.assertReached();
});

const TYPES: readonly PhysicalType[] = [
  ints,
  { kind: "int64" },
  { kind: "float64" },
  { kind: "bool" },
  { kind: "text" },
  { kind: "decimal", precision: 12, scale: 2 },
];

function valuesFor(type: PhysicalType, rows: number, seed: number, large: boolean): TestValue[] {
  let state = seed | 1;
  const next = (): number => {
    state ^= state << 13;
    state ^= state >>> 17;
    state ^= state << 5;
    return state >>> 0;
  };
  return Array.from({ length: rows }, (_, row) => {
    const u = next();
    if (u % 11 === 0) return null;
    switch (type.kind) {
      case "int32":
        return u % 1000;
      case "int64":
        return BigInt(u) * 7n;
      case "float64":
        return (u % 100_000) / 100;
      case "bool":
        return (u & 1) === 1;
      case "decimal":
        return { coefficient: BigInt(u % 1_000_000), special: 0 };
      default:
        return encoder.encode(
          large && row === 3
            ? "x".repeat(70_000)
            : `value-${String(u % (seed % 3 === 0 ? 7 : 5000))}`,
        );
    }
  });
}

describe.each(SEEDS)("export read path (seed %i)", (seed) => {
  it("aligns every blob a chunk reads, and loadTogether installs what load would", async () => {
    const column = fc.record({
      type: fc.integer({ min: 0, max: TYPES.length - 1 }),
      salt: fc.nat(),
      large: fc.boolean(),
    });
    await fc.assert(
      fc.asyncProperty(
        fc.array(column, { minLength: 2, maxLength: 6 }),
        fc.integer({ min: 200, max: 6000 }),
        fc.array(fc.nat(), { minLength: 6, maxLength: 6 }),
        async (columns, rows, picks) => {
          reach.beginHistory();
          const specs = columns.map(({ type, salt, large }) => {
            const kind = defined(TYPES[type]);
            return { type: kind, values: valuesFor(kind, rows, salt + 1, large) };
          });
          const { space, group } = await writeGroup(specs, { pageBytes: 2048, rawPageBytes: 2048 });
          // Alignment: every page, dictionary page and directory starts at an 8-byte address.
          const extentOf = (chunk: ColumnChunk) => chunk.data.extent.offset + chunk.data.offset;
          for (const chunk of group.columns) {
            const reader = await ChunkReader.open(space, chunk, { budget: UNLIMITED });
            const shared = group.columns.filter(
              (other) => other.data.extent.offset === chunk.data.extent.offset,
            ).length;
            reach.reach(shared > 1 ? "a packed chunk" : "a chunk in its own extent");
            if (reader.directory.dictionary !== null) reach.reach("a dictionary page");
            if (reader.directory.pages.some((page) => page.largeRegion !== undefined))
              reach.reach("a large value");
            for (const page of reader.directory.pages)
              expect((extentOf(chunk) + page.offset) % 8).toBe(0);
            if (reader.directory.dictionary !== null)
              expect((extentOf(chunk) + reader.directory.dictionary.offset) % 8).toBe(0);
            expect((chunk.directory.extent.offset + chunk.directory.offset) % 8).toBe(0);
            reader.close();
          }
          // loadTogether: some readers load some pages first, one may close, one may need its
          // dictionary; the rest share reads. Each must hold exactly the pages load would give.
          const budget = countingBudget(Number.POSITIVE_INFINITY);
          const view = space.view({ cache: false });
          const readers: ChunkReader[] = [];
          for (const chunk of group.columns)
            readers.push(await ChunkReader.open(view, chunk, { budget, once: true }));
          const wanted = readers.map((reader, k) => {
            const pages = reader.directory.pages.map((_, page) => page);
            const pick = picks[k % picks.length] ?? 0;
            return pages.filter((page) => (page + pick) % 3 !== 0);
          });
          const pre = picks[0] ?? 0;
          const first = readers[0];
          if (first !== undefined && pre % 2 === 0 && (wanted[0]?.length ?? 0) > 0) {
            await first.load(wanted[0]?.slice(0, 1) ?? []);
            reach.reach("pages already loaded");
          }
          const closed = (picks[1] ?? 0) % 3 === 0 ? readers.length - 1 : -1;
          const loads = readers.map((reader, k) => ({ reader, pages: wanted[k] ?? [] }));
          if (
            loads.some(({ reader, pages }) =>
              pages.some((page) =>
                reader.isDictionaryRange(
                  defined(reader.directory.pages[page]).firstRow,
                  defined(reader.directory.pages[page]).firstRow + 1,
                ),
              ),
            )
          )
            reach.reach("a reader loading alone (dictionary)");
          const together = ChunkReader.loadTogether(loads);
          if (closed >= 0) {
            readers[closed]?.close();
            reach.reach("a reader closed before the shared load settles");
          }
          await together;
          for (const [k, reader] of readers.entries()) {
            if (k === closed) continue;
            const spec = defined(specs[k]);
            for (const page of wanted[k] ?? []) {
              expect(reader.isLoaded(page)).toBe(true);
              const entry = defined(reader.directory.pages[page]);
              // Out-of-line values need their own load (loadValues); their pages are checked
              // for alignment above.
              if (entry.largeRegion !== undefined) continue;
              const from = entry.firstRow;
              const to = entry.firstRow + entry.rowCount;
              const target = targetFor(reader, to - from, reader.varlenBytes(from, to));
              if (reader.isDictionaryRange(from, to)) await reader.loadDictionary();
              reader.decodeRange(from, to, target, 0);
              for (let row = from; row < to; row++)
                expect(sameValue(valueAt(target, row - from), spec.values[row] ?? null)).toBe(true);
            }
            reader.close();
          }
          expect(budget.used).toBe(0);
        },
      ),
      { seed, numRuns: 25 },
    );
  }, 240_000);
});
