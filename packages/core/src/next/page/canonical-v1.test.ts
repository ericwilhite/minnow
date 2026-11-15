/** PC3/PC4: current writer bytes stay valid; reserved fields/ranks refuse in the decode pass. */
import fc from "fast-check";
import { expect, it, vi } from "vitest";
import {
  createReach,
  CANONICAL_WRITER_REACH,
  CANONICAL_REFUSAL_REACH,
} from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import type { PhysicalType } from "../types.js";
import type { ColumnChunk, PageEntry } from "../format/manifest.js";
import { encodeArrayValue } from "../vector/array-value.js";
import { decodeRange, parsePage } from "./decode.js";
import { encodeDirectory, parseDirectory } from "./directory.js";
import { inflatePage } from "./gzip.js";
import { PageEncoding } from "../format/manifest.js";
import { DistinctSketch, validateSketch } from "./sketch.js";
import { dataAddress } from "./reader.js";
import type { MemoryExtentSpace } from "./memory-extents.js";
import {
  openChunk,
  readChunk,
  sameValue,
  targetFor,
  valueAt,
  writeGroup,
  type TestValue,
  type VectorOptions,
} from "./test-support.js";

const utf8 = new TextEncoder();
interface Column {
  name: (typeof CANONICAL_WRITER_REACH)[number];
  type: PhysicalType;
  values: TestValue[];
  vector?: VectorOptions;
}
function corpus(count: number, salt: number): Column[] {
  const rows = Array.from({ length: count }, (_, i) => i),
    bytes = (row: number) => Uint8Array.of(row & 255, salt, 0, 255),
    decimal = { kind: "decimal", precision: null, scale: 2 } as const,
    mixed = { kind: "decimal", precision: null, scale: null } as const;
  return [
    { name: "bool", type: { kind: "bool" }, values: rows.map((i) => (i & 1) === 1) },
    {
      name: "i32 plain",
      type: { kind: "int32", bits: 32 },
      values: rows.map((i) => (i & 1 ? 0x70000000 : -0x70000000) + i),
    },
    { name: "i32 FOR", type: { kind: "int32", bits: 32 }, values: rows.map((i) => (i * 17) % 19) },
    { name: "i32 constant", type: { kind: "int32", bits: 32 }, values: rows.map(() => salt) },
    {
      name: "i64 safe",
      type: { kind: "int64" },
      values: rows.map((i) => BigInt(i * salt)),
      vector: { safe: true },
    },
    {
      name: "i64 words",
      type: { kind: "int64" },
      values: rows.map((i) => (1n << 60n) + BigInt(i)),
    },
    { name: "date", type: { kind: "date" }, values: rows.map((i) => i - salt) },
    { name: "time", type: { kind: "time" }, values: rows.map((i) => BigInt(i * 1000)) },
    {
      name: "timestamp",
      type: { kind: "timestamp", zoned: false },
      values: rows.map((i) => BigInt(i * 1000 - salt)),
    },
    {
      name: "timestamptz",
      type: { kind: "timestamp", zoned: true },
      values: rows.map((i) => BigInt(i * 1000 - salt)),
      vector: { safe: true },
    },
    {
      name: "f32",
      type: { kind: "float32" },
      values: rows.map((i) => (i === 0 ? -0 : Math.fround(i / 7))),
    },
    { name: "f64 scaled", type: { kind: "float64" }, values: rows.map((i) => (i + salt) / 100) },
    {
      name: "f64 patched",
      type: { kind: "float64" },
      values: rows.map((i) => (i === 1 ? -0 : (i + salt) / 100)),
    },
    {
      name: "f64 special",
      type: { kind: "float64" },
      values: rows.map((i) => [-0, NaN, Infinity, -Infinity, Number.MIN_VALUE][i % 5] ?? 0),
    },
    {
      name: "decimal safe",
      type: decimal,
      values: rows.map((i) => ({ coefficient: BigInt(i * salt), special: 0 })),
      vector: { safe: true, scale: 2 },
    },
    {
      name: "decimal i64",
      type: decimal,
      values: rows.map((i) => ({ coefficient: (1n << 60n) + BigInt(i), special: 0 })),
      vector: { scale: 2 },
    },
    {
      name: "decimal i128",
      type: decimal,
      values: rows.map((i) => ({ coefficient: (1n << 100n) + BigInt(i), special: 0 })),
      vector: { tier: "int128", scale: 2 },
    },
    {
      name: "decimal mixed",
      type: mixed,
      values: rows.map((i) => ({
        coefficient: i % 17 === 0 ? 0n : BigInt(i + salt),
        special: i % 17 === 0 ? 1 : 0,
        scale: i % 5,
      })),
      vector: { tier: "int128" },
    },
    {
      name: "decimal uniform",
      type: mixed,
      values: rows.map((i) => ({ coefficient: BigInt(i + salt), special: 0, scale: 3 })),
      vector: { tier: "int128" },
    },
    {
      name: "interval safe",
      type: { kind: "interval" },
      values: rows.map((i) => ({ months: i % 4, days: i % 7, micros: BigInt(i * 1000) })),
      vector: { safe: true },
    },
    {
      name: "interval wide",
      type: { kind: "interval" },
      values: rows.map((i) => ({ months: i % 4, days: i % 7, micros: (1n << 60n) + BigInt(i) })),
    },
    {
      name: "text",
      type: { kind: "text" },
      values: rows.map((i) => utf8.encode(`label${String(i % 7)}`)),
    },
    {
      name: "text constant",
      type: { kind: "text" },
      values: rows.map(() => utf8.encode(`constant${String(salt)}`)),
    },
    {
      name: "UUID dictionary",
      type: { kind: "uuid" },
      values: rows.map((i) => new Uint8Array(16).fill((i % 3) + salt)),
    },
    { name: "bytes", type: { kind: "bytes" }, values: rows.map(bytes) },
    {
      name: "JSON",
      type: { kind: "json", binary: false },
      values: rows.map((i) => utf8.encode(JSON.stringify({ n: i }))),
    },
    {
      name: "JSONB",
      type: { kind: "json", binary: true },
      values: rows.map((i) => utf8.encode(JSON.stringify({ n: i }))),
    },
    {
      name: "UUID",
      type: { kind: "uuid" },
      values: rows.map((i) => Uint8Array.from({ length: 16 }, (_, at) => (i + at + salt) & 255)),
    },
    {
      name: "enum",
      type: { kind: "enum", name: "e", labels: ["a", "b"] },
      values: rows.map((i) => utf8.encode(i & 1 ? "a" : "b")),
    },
    {
      name: "array",
      type: { kind: "array", element: { kind: "int32", bits: 32 } },
      values: rows.map((i) =>
        encodeArrayValue(
          { kind: "int32", bits: 32 },
          { dimensions: [1], lowerBounds: [1], elements: [i] },
        ),
      ),
    },
    {
      name: "large value",
      type: { kind: "bytes" },
      values: rows.map((i) => (i === 1 ? new Uint8Array(70000 + salt).fill(65) : bytes(i))),
    },
    {
      name: "gzip source",
      type: { kind: "text" },
      values: rows.map((i) => utf8.encode("compressible ".repeat(80) + String(i))),
    },
  ];
}
const equalBytes = (a: Uint8Array, b: Uint8Array) =>
  Buffer.from(a.buffer, a.byteOffset, a.byteLength).equals(
    Buffer.from(b.buffer, b.byteOffset, b.byteLength),
  );
async function raw(space: MemoryExtentSpace, chunk: ColumnChunk, page: PageEntry) {
  const start = dataAddress(chunk.data) + page.offset,
    bytes = space.bytes.slice(start, start + page.length);
  return (page.encoding & PageEncoding.gzip) === 0 ? bytes : inflatePage(bytes);
}
for (const seed of seedsFor("next-canonical-writer", [0xb2ad, 0xb2ae]))
  it(`round-trips the current writer corpus byte-identically seed ${String(seed)}`, async () => {
    const reach = createReach("canonical writer corpus", CANONICAL_WRITER_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 64, max: 80 }),
        fc.integer({ min: 1, max: 31 }),
        fc.integer({ min: 1, max: 8 }),
        async (count, salt, batch) => {
          reach.beginHistory();
          for (const nulls of ["none", "sparse", "all"] as const)
            for (const gzip of [false, true]) {
              const columns = corpus(count, salt).map((column) => ({
                ...column,
                values: column.values.map((value, row) =>
                  nulls === "all" || (nulls === "sparse" && row % 5 === 0) ? null : value,
                ),
              }));
              const options = {
                maxRows: count,
                estimatedRows: count,
                batch,
                gzip,
                dictionaryEntries: gzip ? 0 : 65536,
                rawPageBytes: 128 << 10,
                inlineBytes: 0,
              };
              const original = await writeGroup(columns, options),
                stored = original.space.bytes.slice();
              const decoded: Array<(typeof columns)[number]> = [];
              for (let at = 0; at < columns.length; at++) {
                const column = columns[at],
                  chunk = original.group.columns[at];
                if (column === undefined || chunk === undefined)
                  throw new Error("Missing corpus column");
                const values = await readChunk(original.space, chunk);
                for (let row = 0; row < count; row++)
                  expect(
                    sameValue(values[row] ?? null, column.values[row] ?? null),
                    `${column.name} row${String(row)}`,
                  ).toBe(true);
                decoded.push({ ...column, values });
                reach.reach(column.name);
              }
              expect(
                equalBytes(original.space.bytes, stored),
                "decoding never changes stored writer bytes",
              ).toBe(true);
              const rewritten = await writeGroup(decoded, options);
              for (let at = 0; at < columns.length; at++) {
                const a = original.group.columns[at],
                  b = rewritten.group.columns[at];
                if (a === undefined || b === undefined) throw new Error("Missing rewritten column");
                const ar = await openChunk(original.space, a),
                  br = await openChunk(rewritten.space, b);
                try {
                  expect(br.directory.pages.length).toBe(ar.directory.pages.length);
                  expect(br.directory.tag).toBe(ar.directory.tag);
                  expect(br.directory.scale).toBe(ar.directory.scale);
                  expect(br.directory.hasSpecials).toBe(ar.directory.hasSpecials);
                  for (let page = 0; page < ar.directory.pages.length; page++) {
                    const ap = ar.directory.pages[page],
                      bp = br.directory.pages[page];
                    if (ap === undefined || bp === undefined)
                      throw new Error("Missing round-trip page");
                    expect(
                      equalBytes(
                        await raw(original.space, a, ap),
                        await raw(rewritten.space, b, bp),
                      ),
                      "decoded page bytes round-trip exactly",
                    ).toBe(true);
                  }
                  const ad = ar.directory.dictionary,
                    bd = br.directory.dictionary;
                  expect(bd === null).toBe(ad === null);
                  if (ad !== null && bd !== null)
                    expect(
                      equalBytes(
                        await raw(original.space, a, ad),
                        await raw(rewritten.space, b, bd),
                      ),
                    ).toBe(true);
                  if (ar.directory.sketch !== null) {
                    const sketch = DistinctSketch.parse(ar.directory.sketch);
                    expect(equalBytes(sketch.serialize(), ar.directory.sketch)).toBe(true);
                  }
                } finally {
                  ar.close();
                  br.close();
                }
              }
            }
        },
      ),
      { seed, numRuns: 4 },
    );
    reach.assertReached();
  }, 240_000);

for (const seed of seedsFor("next-canonical-refusal", [0xb2af, 0xb2b0]))
  it(`refuses every unused aux bit and canonical sketch corruption seed ${String(seed)}`, async () => {
    const reach = createReach("canonical reserved refusal", CANONICAL_REFUSAL_REACH);
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 3, max: 20 }),
        fc.integer({ min: 0, max: 1023 }),
        fc.integer({ min: 1, max: 23 }),
        async (small, salt, rank) => {
          reach.beginHistory();
          const columns = corpus(64, 1).filter((c) =>
            ["bool", "i32 plain", "i32 FOR", "i32 constant", "UUID", "interval wide"].includes(
              c.name,
            ),
          );
          for (const allNull of [false, true]) {
            const written = await writeGroup(
              columns.map((c) => ({ ...c, values: allNull ? c.values.map(() => null) : c.values })),
              { maxRows: 64, estimatedRows: 64, dictionaryEntries: 0, gzip: false },
            );
            for (const chunk of written.group.columns) {
              const reader = await openChunk(written.space, chunk);
              try {
                for (const page of reader.directory.pages) {
                  const bytes = await raw(written.space, chunk, page);
                  parsePage(bytes, page, reader.directory.tag, reader.directory.scale);
                  expect(new DataView(bytes.buffer, bytes.byteOffset).getUint32(8, true)).toBe(0);
                  for (let bit = 0; bit < 32; bit++) {
                    const changed = bytes.slice();
                    new DataView(changed.buffer).setUint32(8, (1 << bit) >>> 0, true);
                    expect(() =>
                      parsePage(changed, page, reader.directory.tag, reader.directory.scale),
                    ).toThrow(expect.objectContaining({ code: "MINNOW_PAGE_CORRUPT" }));
                  }
                }
                if (reader.directory.tag !== 9) {
                  const bytes = encodeDirectory(reader.directory);
                  bytes[6] = (bytes[6] ?? 0) | 4;
                  expect(() => parseDirectory(bytes, chunk.data.length)).toThrow(
                    expect.objectContaining({ code: "MINNOW_PAGE_CORRUPT" }),
                  );
                }
              } finally {
                reader.close();
              }
            }
            reach.reach(allNull ? "all-NULL aux" : "unused aux");
          }
          reach.reach("nondecimal specials");
          // NULL scalar payloads are don't-care bytes, not reserved fields. Alter only a NULL
          // row's value bits and prove every decoded value, including the NULL, is unchanged.
          for (const column of corpus(64, salt).filter((c) =>
            ["bool", "i32 plain", "UUID"].includes(c.name),
          )) {
            const values = column.values.map((value, row) => (row % 5 === 0 ? null : value));
            const written = await writeGroup([{ ...column, values }], {
              maxRows: 64,
              estimatedRows: 64,
              gzip: false,
              dictionaryEntries: 0,
            });
            const chunk = written.group.columns[0];
            if (chunk === undefined) throw new Error("Missing NULL payload chunk");
            const reader = await openChunk(written.space, chunk);
            try {
              for (const page of reader.directory.pages) {
                const bytes = await raw(written.space, chunk, page),
                  view = parsePage(bytes, page, reader.directory.tag, reader.directory.scale);
                const row = (5 - (page.firstRow % 5)) % 5;
                expect(row).toBeLessThan(page.rowCount);
                if (view.kind === PageEncoding.bitmap)
                  bytes[view.start + (row >>> 3)] =
                    (bytes[view.start + (row >>> 3)] ?? 0) ^ (1 << (row & 7));
                else {
                  expect(view.kind).toBe(PageEncoding.plain);
                  bytes.fill(
                    0xff,
                    view.start + row * view.width,
                    view.start + (row + 1) * view.width,
                  );
                }
                const changed = parsePage(
                    bytes,
                    page,
                    reader.directory.tag,
                    reader.directory.scale,
                  ),
                  target = targetFor(reader, page.rowCount, 0);
                decodeRange(changed, 0, page.rowCount, target, 0);
                for (let at = 0; at < page.rowCount; at++)
                  expect(sameValue(valueAt(target, at), values[page.firstRow + at] ?? null)).toBe(
                    true,
                  );
              }
            } finally {
              reader.close();
            }
          }
          reach.reach("NULL scalar payload");
          const sketch = new DistinctSketch();
          for (let i = 0; i < small; i++)
            sketch.add((((i + salt) % 1024) << 22) | (rank === 23 ? 0 : 1 << (22 - rank)));
          const sparse = sketch.serialize();
          expect(sparse[1]).toBe(1);
          expect(equalBytes(DistinctSketch.parse(sparse).serialize(), sparse)).toBe(true);
          const duplicate = sparse.slice();
          duplicate[7] = duplicate[4] ?? 0;
          duplicate[8] = duplicate[5] ?? 0;
          expect(() => DistinctSketch.parse(duplicate)).toThrow(
            expect.objectContaining({ code: "MINNOW_PAGE_CORRUPT" }),
          );
          reach.reach("duplicate sparse");
          const unordered = sparse.slice(),
            first = unordered.slice(4, 7);
          unordered.copyWithin(4, 7, 10);
          unordered.set(first, 7);
          expect(() => DistinctSketch.parse(unordered)).toThrow(
            expect.objectContaining({ code: "MINNOW_PAGE_CORRUPT" }),
          );
          reach.reach("unordered sparse");
          const dense = new Uint8Array(1028);
          dense.set([10, 0, 0, 4]);
          dense.set(sketch.registers, 4);
          // Validation must not allocate even a DataView or a typed subview. Build fixtures
          // first; forbid constructors/copy helpers only during the synchronous body walk.
          const allocation = () => {
              throw new Error("HLL validation allocated");
            },
            subarray = vi.spyOn(Uint8Array.prototype, "subarray").mockImplementation(allocation),
            slice = vi.spyOn(ArrayBuffer.prototype, "slice").mockImplementation(allocation);
          vi.stubGlobal("DataView", new Proxy(DataView, { construct: allocation }));
          vi.stubGlobal("Uint8Array", new Proxy(Uint8Array, { construct: allocation }));
          try {
            validateSketch(dense);
            validateSketch(sparse);
          } finally {
            vi.unstubAllGlobals();
            subarray.mockRestore();
            slice.mockRestore();
          }
          reach.reach("allocation-free HLL validation");
          expect(equalBytes(DistinctSketch.parse(dense).registers, sketch.registers)).toBe(true);
          for (let invalid = 24; invalid < 256; invalid++) {
            const changed = dense.slice();
            changed[4 + salt] = invalid;
            expect(() => DistinctSketch.parse(changed)).toThrow(
              expect.objectContaining({ code: "MINNOW_PAGE_CORRUPT" }),
            );
          }
          reach.reach("dense ranks");
          reach.reach("dense at sparse density");
          const many = 342 + (salt % 683),
            full = new DistinctSketch();
          for (let i = 0; i < many; i++) full.add((i << 22) | (rank === 23 ? 0 : 1 << (22 - rank)));
          const canonical = full.serialize();
          expect(canonical[1]).toBe(0);
          expect(equalBytes(DistinctSketch.parse(canonical).serialize(), canonical)).toBe(true);
          const alternate = new Uint8Array(4 + many * 3);
          alternate.set([10, 1, many & 255, many >>> 8]);
          for (let i = 0; i < many; i++) {
            alternate[4 + i * 3] = i & 255;
            alternate[5 + i * 3] = i >>> 8;
            alternate[6 + i * 3] = rank;
          }
          expect(equalBytes(DistinctSketch.parse(alternate).registers, full.registers)).toBe(true);
          reach.reach("sparse at dense density");
          reach.reach("writer HLL bytes");
        },
      ),
      { seed, numRuns: 8 },
    );
    reach.assertReached();
  }, 120_000);
