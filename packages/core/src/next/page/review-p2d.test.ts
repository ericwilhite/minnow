/**
 * Review B2-p2d (rebuild/format-p2d 6936b72): the reviewer's probes as tests that reject the
 * defects. 1: a dictionary page's codes are checked against the chunk's dictionary once, where
 * the page is validated (`checkDictionaryCodes` in `parsePage`), and every consumer relies on it:
 * select and refine no longer drop an out-of-dictionary row that decode refuses. 2: 128-bit
 * selection compares numeric limb bounds prepared once per page, no BigInt work per row.
 */
import { PGlite } from "@electric-sql/pglite";
import { describe, expect, it, vi } from "vitest";
import { xxHash32 } from "../hash/xxhash32.js";
import { PageEncoding } from "../format/manifest.js";
import type { PhysicalType } from "../types.js";
import { decodeRange, parsePage } from "./decode.js";
import { encodeDirectory } from "./directory.js";
import { PageCorruptionError, ValueTag } from "./format.js";
import { UNLIMITED } from "./memory-extents.js";
import { compilePredicate } from "./predicate.js";
import { ChunkReader } from "./reader.js";
import { dictionaryMatches, newScratch, selectPage } from "./select.js";
import { defined, writeGroup, type TestValue } from "./test-support.js";

const encoder = new TextEncoder();

/** A two-row dictionary page with codes `codes` (width 1, or width 0 with every row `aux`). */
function dictionaryPage(codes: number[] | null, aux: number) {
  const width = codes === null ? 0 : 1;
  const bytes = new Uint8Array(16 + (codes === null ? 0 : 2));
  bytes[0] = PageEncoding.dictionary;
  bytes[1] = ValueTag.varlen;
  bytes[3] = width;
  new DataView(bytes.buffer).setUint32(4, 2, true);
  new DataView(bytes.buffer).setUint32(8, aux, true);
  if (codes !== null) bytes.set(codes, 16);
  const entry = {
    firstRow: 0,
    rowCount: 2,
    nullCount: 0,
    encoding: PageEncoding.dictionary,
    offset: 0,
    length: bytes.length,
    checksum: 0,
    min: null,
    max: null,
  };
  return { bytes, entry };
}

const oneEntry = {
  tag: ValueTag.varlen,
  size: 1,
  bytes: encoder.encode("a"),
  offsets: new Int32Array([0, 1]),
};

describe("review B2-p2d #1: one check of the dictionary domain, for every consumer", () => {
  for (const [label, codes, aux] of [
    ["codes [0, 1]", [0, 1], 1],
    ["width zero, every row code 1", null, 1],
  ] as const) {
    it(`refuses ${label} against a one-entry dictionary, wherever it is checked (the probe)`, () => {
      const { bytes, entry } = dictionaryPage(codes === null ? null : [...codes], aux);
      // Where the page is validated against its chunk: refused at parse.
      expect(() => parsePage(bytes, entry, ValueTag.varlen, null, 1)).toThrow(PageCorruptionError);
      // Handed in directly (parsed without the dictionary's size): select refuses, like decode.
      const view = parsePage(bytes, entry, ValueTag.varlen, null);
      const type: PhysicalType = { kind: "text" };
      const compiled = defined(
        compilePredicate(type, null, { op: "=", value: encoder.encode("a") }) ?? undefined,
      );
      const context = {
        view,
        entry,
        compiled,
        dictionary: oneEntry,
        codes: dictionaryMatches(oneEntry, compiled),
        scratch: newScratch(2),
        values: null,
        type,
      };
      expect(() => selectPage(context, null, 0, 2, new Uint16Array(2), 0)).toThrow(
        /outside the dictionary/u,
      );
      expect(() => selectPage(context, Uint16Array.of(0, 1), 0, 2, new Uint16Array(2), 0)).toThrow(
        /outside the dictionary/u,
      );
      expect(() =>
        decodeRange(
          view,
          0,
          2,
          {
            form: "flat",
            type,
            length: 2,
            validity: null,
            data: { layout: "varlen", bytes: new Uint8Array(10), offsets: new Int32Array(3) },
          },
          0,
          oneEntry,
        ),
      ).toThrow(/outside the dictionary/u);
      // In the domain, the same page is accepted.
      const valid = dictionaryPage(codes === null ? null : [0, 0], 0);
      expect(() => parsePage(valid.bytes, valid.entry, ValueTag.varlen, null, 1)).not.toThrow();
    });
  }

  it("refuses a writer-forged code at load, before select, refine or decode see it (the probe)", async () => {
    const values = Array.from({ length: 100 }, (_, row) =>
      encoder.encode(row % 2 === 1 ? "b" : "a"),
    );
    const { space, group } = await writeGroup([{ type: { kind: "text" }, values }]);
    const chunk = defined(group.columns[0]);
    const original = await ChunkReader.open(space, chunk, { budget: UNLIMITED });
    const directory = original.directory;
    const entry = defined(directory.pages[0]);
    original.close();
    expect(entry.encoding).toBe(PageEncoding.dictionary);
    const address = chunk.data.extent.offset + chunk.data.offset + entry.offset;
    const raw = space.bytes.slice(address, address + entry.length);
    new DataView(raw.buffer).setUint32(8, 2, true); // aux 2
    raw[16] = 2; // first code 2: one past a two-entry dictionary
    space.bytes.set(raw, address);
    space.evict();
    const pages = directory.pages.map((page, index) =>
      index === 0 ? { ...page, checksum: xxHash32(raw) } : page,
    );
    const blob = defined(
      (await space.beginWrite("forged").write([encodeDirectory({ ...directory, pages })])).blobs[0],
    );
    const reader = await ChunkReader.open(
      space,
      { ...chunk, directory: blob },
      { budget: UNLIMITED },
    );
    const compiled = defined(
      reader.compilePredicate({ op: "in", values: [encoder.encode("a"), encoder.encode("b")] }) ??
        undefined,
    );
    await expect(Promise.resolve(reader.load(reader.pagesMatching(compiled)))).rejects.toThrow(
      /outside the dictionary/u,
    );
    expect(reader.isLoaded(0)).toBe(false);
    reader.close();
  });
});

describe("review B2-p2d #2: 128-bit selection does no BigInt work per row", () => {
  it("prepares limb bounds once per page (the probe, instrumented)", async () => {
    const rows = 3000;
    const values: TestValue[] = Array.from({ length: rows }, (_, row) => ({
      coefficient: (1n << 100n) + BigInt(row),
      special: 0,
    }));
    const { space, group } = await writeGroup(
      [
        {
          type: { kind: "decimal", precision: null, scale: 2 },
          values,
          vector: { scale: 2, tier: "int128" },
        },
      ],
      { pageBytes: 8192 },
    );
    const reader = await ChunkReader.open(space, defined(group.columns[0]), { budget: UNLIMITED });
    const compiled = defined(
      reader.compilePredicate({ op: ">=", value: { coefficient: 1n << 100n, scale: 2 } }) ??
        undefined,
    );
    await reader.load(reader.pagesMatching(compiled));
    const pages = reader.directory.pages.length;
    expect(pages).toBeGreaterThan(1);
    const asIntN = vi.spyOn(BigInt, "asIntN");
    const asUintN = vi.spyOn(BigInt, "asUintN");
    let selected: number;
    let refined: number;
    try {
      const out = new Uint16Array(rows);
      selected = reader.select(compiled, 0, rows, out);
      refined = reader.refine(compiled, out, 0, selected, out);
    } finally {
      reader.close();
    }
    const calls = asIntN.mock.calls.length + asUintN.mock.calls.length;
    asIntN.mockRestore();
    asUintN.mockRestore();
    expect([selected, refined]).toEqual([rows, rows]);
    // Per page and per call, eight limb conversions per range (two bounds of four limbs); none
    // per row: far fewer than the rows.
    expect(calls).toBeLessThanOrEqual(2 * pages * 8);
    expect(calls).toBeLessThan(rows / 10);
  });
});

describe("review B2-p2d: what the reviewer found sound stays so", () => {
  it("65,536 positions, FOR bounds outside the domain, float32 widening and PostgreSQL float semantics (the probe)", async () => {
    const cases: Array<
      [PhysicalType, TestValue[], Parameters<typeof compilePredicate>[2], number]
    > = [
      [
        { kind: "int32", bits: 16 },
        Array.from({ length: 65536 }, (_, row) => row % 32000),
        { op: "<", value: 70000 },
        65536,
      ],
      [
        { kind: "int64" },
        [9223372036854775805n, 9223372036854775806n, 9223372036854775807n],
        { op: ">", value: 1n << 64n },
        0,
      ],
      [{ kind: "float32" }, [Math.fround(0.1), 0, -0, Number.NaN], { op: "=", value: 0.1 }, 0],
      [
        { kind: "float64" },
        [Number.NaN, Infinity, 0, -0, null],
        { op: "between", low: Number.NaN, high: Infinity },
        0,
      ],
      [
        { kind: "decimal", precision: 18, scale: 2 },
        [null, { coefficient: 123n, special: 0 }, { coefficient: 123n, special: 0 }],
        { op: "=", value: { coefficient: 1230n, scale: 3 } },
        2,
      ],
    ];
    for (const [type, values, predicate, expected] of cases) {
      const { space, group } = await writeGroup([
        { type, values, ...(type.kind === "decimal" ? { vector: { scale: 2 } } : {}) },
      ]);
      const reader = await ChunkReader.open(space, defined(group.columns[0]), {
        budget: UNLIMITED,
      });
      const compiled = defined(reader.compilePredicate(predicate) ?? undefined);
      await reader.load(reader.pagesMatching(compiled));
      const out = new Uint16Array(values.length);
      const count = reader.select(compiled, 0, values.length, out);
      expect(count, JSON.stringify(type)).toBe(expected);
      if (count === 65536) {
        expect(out[65535]).toBe(65535);
        expect(reader.refine(compiled, out, 123, 65536, out)).toBe(65536 - 123);
        expect(out[0]).toBe(123);
      }
      reader.close();
      reader.close();
    }
    const pg = new PGlite();
    try {
      const result = await pg.query(
        "select 'NaN'::float8 = 'NaN'::float8 as eq, 'NaN'::float8 > 'Infinity'::float8 as gt, 0.1::float4 = 0.1::float8 as rounded, 2 in (1,null) as unknown, 2 not in (1,null) as negated, 2 between 3 and 1 as reversed",
      );
      expect(result.rows).toEqual([
        { eq: true, gt: true, rounded: false, unknown: null, negated: null, reversed: false },
      ]);
    } finally {
      await pg.close();
    }
  }, 60_000);
});
