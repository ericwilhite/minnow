/**
 * Corruption: every single-byte change of a stored page, dictionary page or directory is refused
 * (`PageCorruptionError`) before any value is decoded; and with the checksum bypassed (a writer
 * bug, a hash collision), structural validation still keeps decode inside its sections.
 */
import { beforeAll, describe, expect, it } from "vitest";
import { mulberry32 } from "../../testing/seeds.js";
import type { ColumnChunk, PageEntry } from "../format/manifest.js";
import { PageEncoding } from "../format/manifest.js";
import type { PhysicalType } from "../types.js";
import { decodeRange, parsePage } from "./decode.js";
import { PageCorruptionError } from "./format.js";
import { StoreCorruptionError } from "../storage/contract.js";
import { blobAddress } from "../storage/extents.js";
import { targetFor, writeGroup, type TestValue, type Written } from "./test-support.js";
import { openChunk } from "./test-support.js";
import { defined } from "./test-support.js";

const encoder = new TextEncoder();
const ROWS = 1500;

function columns(): Array<{ type: PhysicalType; values: TestValue[] }> {
  const random = mulberry32(0xc0de);
  const maybe = <T>(value: T, rate = 0.1): T | null => (random() < rate ? null : value);
  const words = ["alpha", "bravo", "charlie", "delta", "echo"];
  return [
    { type: { kind: "bool" }, values: Array.from({ length: ROWS }, () => maybe(random() < 0.5)) },
    {
      type: { kind: "int32", bits: 16 },
      values: Array.from({ length: ROWS }, () => maybe(7, 0.3)),
    },
    {
      type: { kind: "int32", bits: 32 },
      values: Array.from({ length: ROWS }, () => maybe(1000 + Math.floor(random() * 200))),
    },
    {
      type: { kind: "int32", bits: 32 },
      values: Array.from({ length: ROWS }, (_, row) => row * 3 + (row % 5)),
    },
    {
      type: { kind: "int32", bits: 32 },
      values: Array.from({ length: ROWS }, () => Math.floor((random() - 0.5) * 4e9)),
    },
    {
      type: { kind: "int64" },
      values: Array.from({ length: ROWS }, () =>
        maybe(2n ** 60n + BigInt(Math.floor(random() * 60_000))),
      ),
    },
    {
      type: { kind: "timestamp", zoned: true },
      values: Array.from({ length: ROWS }, (_, row) => BigInt(row) * 30_000_000n + 7n),
    },
    {
      type: { kind: "float64" },
      values: Array.from({ length: ROWS }, () => maybe(random() * 1e6)),
    },
    {
      type: { kind: "float32" },
      values: Array.from({ length: ROWS }, () => Math.fround(random())),
    },
    {
      type: { kind: "text" },
      values: Array.from({ length: ROWS }, () =>
        maybe(encoder.encode(words[Math.floor(random() * 5)] ?? "")),
      ),
    },
    {
      type: { kind: "text" },
      values: Array.from({ length: ROWS }, (_, row) =>
        encoder.encode(`user${String(row)}@mail.example.com`),
      ),
    },
    {
      type: { kind: "uuid" },
      values: Array.from({ length: ROWS }, () =>
        Uint8Array.from({ length: 16 }, () => Math.floor(random() * 256)),
      ),
    },
    {
      type: { kind: "interval" },
      values: Array.from({ length: ROWS }, (_, row) =>
        maybe({ months: row % 13, days: row % 31, micros: BigInt(row) * 1000n }),
      ),
    },
    {
      type: { kind: "decimal", precision: 12, scale: 2 },
      values: Array.from({ length: ROWS }, (_, row) =>
        maybe({ coefficient: BigInt(row * 1234 - 500_000), special: row % 97 === 0 ? 1 : 0 }),
      ),
    },
    {
      type: { kind: "decimal", precision: null, scale: 2 },
      values: Array.from({ length: ROWS }, (_, row) => ({
        coefficient: 10n ** 30n + BigInt(row),
        special: 0,
      })),
    },
  ];
}

interface StoredPage {
  readonly chunk: ColumnChunk;
  readonly entry: PageEntry;
  readonly dictionary: boolean;
}

let written: Written;
let pages: StoredPage[] = [];

beforeAll(async () => {
  const input = columns();
  written = await writeGroup(
    input.map((column, index) => ({
      ...column,
      vector: index === 14 ? { tier: "int128" as const } : {},
    })),
    { pageBytes: 1024, rawPageBytes: 2048, inlineBytes: 0 },
  );
  // One large compressible page so a gzip stored form is covered too.
  const zipped = await writeGroup(
    [
      {
        type: { kind: "text" },
        values: Array.from({ length: 3000 }, (_, row) =>
          encoder.encode(`event ${String(row % 600)} ok`),
        ),
      },
    ],
    { dictionaryEntries: 1, pageBytes: 64 * 1024, gzip: true, space: written.space },
  );
  const moved = defined(zipped.group.columns[0]);
  pages = [];
  for (const chunk of [...written.group.columns, moved]) {
    const reader = await openChunk(written.space, chunk);
    for (const entry of reader.directory.pages) pages.push({ chunk, entry, dictionary: false });
    if (reader.directory.dictionary !== null)
      pages.push({ chunk, entry: reader.directory.dictionary, dictionary: true });
  }
});

async function loadOne(page: StoredPage): Promise<void> {
  const reader = await openChunk(written.space, page.chunk);
  if (page.dictionary) await reader.loadDictionary();
  else
    await reader.load([
      reader.directory.pages.findIndex((entry) => entry.offset === page.entry.offset),
    ]);
}

describe("corruption is refused, never decoded", () => {
  it("covers every encoding and the gzip stored form", () => {
    const kinds = new Set(pages.map((page) => page.entry.encoding));
    for (const encoding of [
      PageEncoding.plain,
      PageEncoding.constant,
      PageEncoding.bitmap,
      PageEncoding.frameOfReference,
      PageEncoding.dictionary,
    ])
      expect(kinds).toContain(encoding);
    expect([...kinds].some((encoding) => (encoding & PageEncoding.gzip) !== 0)).toBe(true);
  });

  it("detects every single-byte change of every stored page by checksum", async () => {
    const bytes = written.space.bytes;
    const missed: number[] = [];
    let flips = 0;
    for (const page of pages) {
      await loadOne(page); // intact: loads
      const start = page.chunk.data.offset + page.entry.offset;
      for (let at = start; at < start + page.entry.length; at++) {
        for (const mask of [0x01, 0xff]) {
          const original = bytes[at] ?? 0;
          bytes[at] = original ^ mask;
          try {
            await loadOne(page);
            missed.push(at);
          } catch (error) {
            if (!(error instanceof PageCorruptionError || error instanceof StoreCorruptionError))
              missed.push(at);
          } finally {
            bytes[at] = original;
          }
          flips++;
        }
      }
    }
    expect(missed).toEqual([]);
    expect(flips).toBeGreaterThan(50_000);
  }, 120_000);

  it("detects every single-byte change of every page directory", async () => {
    const bytes = written.space.bytes;
    const missed: number[] = [];
    for (const chunk of written.group.columns) {
      const offset = blobAddress(chunk.directory);
      const { length } = chunk.directory;
      for (let at = offset; at < offset + length; at++) {
        const original = bytes[at] ?? 0;
        bytes[at] = original ^ 0x01;
        try {
          await openChunk(written.space, chunk);
          missed.push(at);
        } catch (error) {
          if (!(error instanceof PageCorruptionError || error instanceof StoreCorruptionError))
            missed.push(at);
        } finally {
          bytes[at] = original;
        }
      }
    }
    expect(missed).toEqual([]);
  });

  it("with the checksum bypassed, structure still bounds decode and catches header damage", async () => {
    const bytes = written.space.bytes;
    const problems: string[] = [];
    const varlenBytes = 1 << 20;
    let structural = 0;
    let decoded = 0;
    for (const page of pages) {
      if (page.dictionary || (page.entry.encoding & PageEncoding.gzip) !== 0) continue;
      const reader = await openChunk(written.space, page.chunk);
      const tag = reader.directory.tag;
      const start = page.chunk.data.offset + page.entry.offset;
      for (let at = start; at < start + page.entry.length; at++) {
        for (const mask of [0x01, 0xff]) {
          const copy = bytes.slice(start, start + page.entry.length);
          copy[at - start] = (copy[at - start] ?? 0) ^ mask;
          let view;
          try {
            view = parsePage(copy, page.entry, tag);
          } catch (error) {
            if (!(error instanceof PageCorruptionError)) problems.push(`parse ${String(error)}`);
            structural++;
            continue;
          }
          // Header fields that fix a page's layout are always caught: kind, tag, width, rows,
          // reserved (bytes 0, 1, 3, 4–7, 12–15).
          const field = at - start;
          if (!(field === 2 || (field >= 8 && field < 12) || field >= 16))
            problems.push(`header byte ${String(field)} passed`);
          const target = targetFor(reader, page.entry.rowCount, varlenBytes, { forceInt64: true });
          if (target.data.layout === "varlen") target.data.offsets[0] = 0;
          try {
            decodeRange(view, 0, page.entry.rowCount, target, 0, null);
            decoded++;
          } catch (error) {
            // A typed refusal (e.g. a dictionary page without its dictionary here) is fine;
            // anything else (an out-of-bounds read surfacing as TypeError) is not.
            if (!(error instanceof PageCorruptionError || error instanceof RangeError))
              problems.push(String(error));
          }
        }
      }
    }
    expect(problems).toEqual([]);
    expect(structural).toBeGreaterThan(1000);
    expect(decoded).toBeGreaterThan(1000);
  }, 120_000);
});
