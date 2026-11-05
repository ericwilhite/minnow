import fc from "fast-check";
import { describe, expect, it } from "vitest";
import { mulberry32, seedsFor } from "../../testing/seeds.js";
import { PageEncoding } from "../format/manifest.js";
import { INT128_MAX, INT128_MIN } from "../vector/int128.js";
import {
  DATE_NEG_INFINITY,
  DATE_POS_INFINITY,
  TIMESTAMP_NEG_INFINITY,
  TIMESTAMP_POS_INFINITY,
} from "../temporal.js";
import { INT32_MAX, INT32_MIN, INT64_MAX, INT64_MIN, type PhysicalType } from "../types.js";
import { ChunkReader, directoryCharge, MAX_BLOB_BYTES, MAX_READ_BYTES } from "./reader.js";
import { ReadDeniedError } from "../storage/extents.js";
import { DistinctSketch, mix32 } from "./sketch.js";
import {
  readChunk,
  sameValue,
  targetFor,
  valueAt,
  writeGroup,
  type DecimalValue,
  type TestValue,
} from "./test-support.js";
import { openChunk, UNLIMITED } from "./test-support.js";
import { defined } from "./test-support.js";
import { DELTA_BLOCK, UnsupportedPageLayoutError } from "./format.js";
import { GroupWriter } from "./writer.js";
import { MemoryExtentSpace } from "./memory-extents.js";
import { vectorOf } from "./test-support.js";

const SEEDS = seedsFor("next-page-codecs", [0x9a6e, 0x51]);
const RUNS = 60;
const encoder = new TextEncoder();

/** Reach: every (tag, encoding, width, flags) a property run produced, by name. */
const reached = new Set<string>();
const TAGS = [
  "",
  "bool",
  "i32",
  "i64",
  "f64",
  "f32",
  "varlen",
  "fixed16",
  "interval",
  "dec64",
  "dec128",
];
const KINDS = ["", "plain", "constant", "bitmap", "for", "dictionary"];
function recordReach(
  space: MemoryExtentSpace,
  offset: number,
  pages: ReadonlyArray<{ offset: number; encoding: number }>,
): void {
  for (const page of pages) {
    if ((page.encoding & PageEncoding.gzip) !== 0) {
      reached.add("gzip");
      continue;
    }
    const at = offset + page.offset;
    const kind = KINDS[space.bytes[at] ?? 0] ?? "?";
    const tag = TAGS[space.bytes[at + 1] ?? 0] ?? "?";
    const flags = space.bytes[at + 2] ?? 0;
    const width = space.bytes[at + 3] ?? 0;
    const name = `${tag}:${(flags & 4) !== 0 ? "delta" : kind}`;
    reached.add(name);
    reached.add(`${name}:${String(width)}`);
    if ((flags & 1) !== 0) reached.add(`${tag}:validity`);
    if ((flags & 2) !== 0) reached.add(`${tag}:allNull`);
    if ((flags & 8) !== 0) reached.add(`${tag}:specials`);
    if ((flags & 32) !== 0) reached.add(`${tag}:scales`);
    if ((flags & 64) !== 0) reached.add(`${tag}:uniformScale`);
  }
}

interface Case {
  readonly values: TestValue[];
  readonly nullSeed: number;
  readonly nullRate: number;
  readonly options: {
    readonly pageBytes: number;
    readonly rawPageBytes: number;
    readonly inlineBytes: number;
    readonly slabBytes: number;
    readonly batch: number;
    readonly dictionaryEntries: number;
    readonly gzip: boolean;
  };
}

const writerOptions = fc.record({
  pageBytes: fc.constantFrom(256, 2048, 32 * 1024),
  rawPageBytes: fc.constantFrom(1024, 8192, 64 * 1024, 256 * 1024),
  inlineBytes: fc.constantFrom(0, 16 * 1024),
  slabBytes: fc.constantFrom(4096, 16 * 1024),
  batch: fc.integer({ min: 1, max: 3000 }),
  dictionaryEntries: fc.constantFrom(3, 64, 65_536),
  gzip: fc.boolean(),
});

function withNulls(values: fc.Arbitrary<TestValue[]>): fc.Arbitrary<Case> {
  return fc.record({
    values,
    nullSeed: fc.integer(),
    nullRate: fc.constantFrom(0, 0, 0, 0.03, 0.5, 1),
    options: writerOptions,
  });
}

function applyNulls(test: Case): TestValue[] {
  const random = mulberry32(test.nullSeed);
  return test.values.map((value) => (random() < test.nullRate ? null : value));
}

/** Writes, reads back by range and by selection (both index widths), and compares. */
async function roundTrip(
  type: PhysicalType,
  test: Case,
  vector: Parameters<typeof writeGroup>[0][number]["vector"] = {},
): Promise<void> {
  const values = applyNulls(test);
  if (values.length === 0) return;
  // A second column checks that chunks sharing a pack or extents stay apart.
  const companion = values.map((_, row) => row);
  const { space, group } = await writeGroup(
    [
      { type, values, vector },
      { type: { kind: "int32", bits: 32 }, values: companion },
    ],
    test.options,
  );
  const chunk = group.columns[0];
  if (chunk === undefined) throw new Error("no chunk");
  expect(group.rowCount).toBe(values.length);
  expect(chunk.statistics.nullCount).toBe(values.filter((value) => value === null).length);
  for (const forceInt64 of [false, true]) {
    const read = await readChunk(space, chunk, { forceInt64 });
    for (let row = 0; row < values.length; row++)
      if (!sameValue(read[row] ?? null, values[row] ?? null))
        expect({ row, read: read[row], wrote: values[row] }).toBeNull();
  }
  const second = await readChunk(space, group.columns[1] ?? chunk);
  expect(second).toEqual(companion);

  // Selection: about 1 in 7 rows, ascending.
  const random = mulberry32(test.nullSeed ^ 0x5e1);
  const picked: number[] = [];
  for (let row = 0; row < values.length; row++) if (random() < 0.15) picked.push(row);
  if (picked.length === 0) picked.push(values.length - 1);
  const reader = await openChunk(space, chunk);
  recordReach(space, chunk.data.offset, reader.directory.pages);
  if (reader.directory.dictionary !== null) reached.add(`${type.kind}:dictionary-chunk`);
  if (
    reader.directory.pages.some((page) => (page.encoding & 0x7f) === PageEncoding.dictionary) &&
    reader.directory.pages.some((page) => (page.encoding & 0x7f) === PageEncoding.plain)
  )
    reached.add(`${type.kind}:dictionary-overflow`);
  for (const rows of [Uint16Array.from(picked), Int32Array.from(picked)]) {
    reader.release();
    await reader.load(reader.pagesForRows(rows));
    const target = targetFor(reader, rows.length, reader.gatherBytes(rows));
    reader.gather(rows, target, 0);
    for (let k = 0; k < rows.length; k++) {
      const row = rows[k] ?? 0;
      if (!sameValue(valueAt(target, k), values[row] ?? null))
        expect({ row, read: valueAt(target, k), wrote: values[row] }).toBeNull();
    }
    // Codes, when every page is dictionary-encoded.
    if (reader.isDictionaryRange(0, reader.rowCount)) {
      const dictionary = await reader.loadDictionary();
      if (dictionary === null) throw new Error("dictionary pages without a dictionary");
      const codes = new Int32Array(rows.length);
      const validity = new Uint32Array((rows.length + 31) >>> 5);
      reader.gatherCodes(rows, codes, validity, 0);
      for (let k = 0; k < rows.length; k++) {
        const valid = (((validity[k >>> 5] ?? 0) >>> (k & 31)) & 1) === 1;
        const expected = values[rows[k] ?? 0] ?? null;
        expect(valid).toBe(expected !== null);
        if (valid) expect(sameValue(valueAt(dictionary, codes[k] ?? 0), expected)).toBe(true);
      }
    }
  }
}

const int32 = fc.integer({ min: INT32_MIN, max: INT32_MAX });
const clamp32 = (value: number): number => Math.max(INT32_MIN, Math.min(INT32_MAX, value));
/** A walk from `start` by `step`s: sorted and near-sorted columns (delta pages). */
function walk32(step: fc.Arbitrary<number>): fc.Arbitrary<number[]> {
  return fc
    .tuple(fc.integer({ min: -1e9, max: 1e9 }), fc.array(step, { maxLength: 3000, size: "large" }))
    .map(([start, steps]) => {
      let value = start;
      return steps.map((delta) => (value = clamp32(value + delta)));
    });
}
function near32(span: number): fc.Arbitrary<number[]> {
  return fc
    .tuple(int32, fc.array(fc.integer({ min: 0, max: span }), { maxLength: 3000, size: "large" }))
    .map(([base, deltas]) => deltas.map((delta) => clamp32(base + delta)));
}
const int32Columns = fc.oneof(
  fc.array(int32, { maxLength: 3000, size: "large" }),
  near32(200),
  near32(60_000),
  walk32(fc.constant(7)),
  walk32(fc.integer({ min: -3, max: 200 })),
  walk32(fc.integer({ min: -3, max: 60_000 })),
  walk32(fc.integer({ min: -1e7, max: 1e7 })),
  fc
    .tuple(int32, fc.integer({ min: 1, max: 3000 }))
    .map(([value, count]) => Array<number>(count).fill(value)),
  fc.array(fc.constantFrom(INT32_MIN, INT32_MAX, 0, -1, 1), { maxLength: 2000, size: "large" }),
);

const int64 = fc.bigInt({ min: INT64_MIN, max: INT64_MAX });
const big = (value: bigint): bigint =>
  value > INT64_MAX ? INT64_MAX : value < INT64_MIN ? INT64_MIN : value;
function walk64(step: fc.Arbitrary<bigint>): fc.Arbitrary<bigint[]> {
  return fc
    .tuple(int64, fc.array(step, { maxLength: 2500, size: "large" }))
    .map(([start, steps]) => {
      let value = start;
      return steps.map((delta) => (value = big(value + delta)));
    });
}
function near64(span: bigint): fc.Arbitrary<bigint[]> {
  return fc
    .tuple(int64, fc.array(fc.bigInt({ min: 0n, max: span }), { maxLength: 2500, size: "large" }))
    .map(([base, deltas]) => deltas.map((delta) => big(base + delta)));
}
const int64Columns = fc.oneof(
  fc.array(int64, { maxLength: 2500, size: "large" }),
  near64(200n),
  near64(60_000n),
  near64(2n ** 32n - 1n),
  near64(2n ** 33n),
  walk64(fc.constant(3n)),
  walk64(fc.bigInt({ min: -5n, max: 200n })),
  walk64(fc.bigInt({ min: -5n, max: 2n ** 31n })),
  walk64(fc.bigInt({ min: -(2n ** 53n), max: 2n ** 53n })),
  fc
    .tuple(int64, fc.integer({ min: 1, max: 2500 }))
    .map(([value, count]) => Array<bigint>(count).fill(value)),
  fc.array(
    fc.constantFrom(
      INT64_MIN,
      INT64_MAX,
      0n,
      -1n,
      2n ** 53n - 1n,
      -(2n ** 53n) + 1n,
      2n ** 53n,
      -(2n ** 53n),
    ),
    { maxLength: 2000, size: "large" },
  ),
  fc.array(fc.bigInt({ min: -(2n ** 53n) + 1n, max: 2n ** 53n - 1n }), {
    maxLength: 2500,
    size: "large",
  }),
);

const timestamps = fc.oneof(
  int64Columns,
  fc.array(
    fc.oneof(
      { weight: 8, arbitrary: fc.bigInt({ min: -(2n ** 50n), max: 2n ** 50n }) },
      { weight: 1, arbitrary: fc.constantFrom(TIMESTAMP_NEG_INFINITY, TIMESTAMP_POS_INFINITY) },
    ),
    { maxLength: 2500, size: "large" },
  ),
);

const float64Columns = fc.oneof(
  fc.array(fc.double(), { maxLength: 2500, size: "large" }),
  fc.array(
    fc.constantFrom(0, -0, Number.NaN, Infinity, -Infinity, Number.MIN_VALUE, Number.MAX_VALUE),
    { maxLength: 2000, size: "large" },
  ),
  fc
    .tuple(fc.double(), fc.integer({ min: 1, max: 2500 }))
    .map(([value, count]) => Array<number>(count).fill(value)),
);
const float32Columns = float64Columns.map((values) => values.map((value) => Math.fround(value)));

function pooled<T>(item: fc.Arbitrary<T>, maxPool: number, maxLength = 2500): fc.Arbitrary<T[]> {
  return fc
    .tuple(
      fc.array(item, {
        minLength: 1,
        maxLength: maxPool,
        size: maxPool >= 300 ? "large" : "small",
      }),
      fc.array(fc.nat(), { maxLength, size: "large" }),
    )
    .map(([pool, picks]) => picks.map((pick) => pool[pick % pool.length] as T));
}

const text = fc.string({ unit: "binary", maxLength: 40 }).map((value) => encoder.encode(value));
const textColumns = fc.oneof(
  fc.array(text, { maxLength: 2000, size: "large" }),
  pooled(text, 6),
  pooled(text, 300),
  pooled(text, 1000),
  // More distinct values than a u8 code holds, each repeated: u16 codes.
  fc
    .tuple(
      fc.integer({ min: 300, max: 1500 }),
      fc.array(fc.nat(), { minLength: 500, maxLength: 3000, size: "large" }),
    )
    .map(([pool, picks]) => picks.map((pick) => encoder.encode(`key-${String(pick % pool)}`))),
  // Few distinct values first, many later: dictionary pages, then overflow to plain.
  fc
    .tuple(pooled(text, 3), fc.array(text, { maxLength: 2000, size: "large" }))
    .map(([low, high]) => [...low, ...high]),
  // Long values that gzip cannot halve: pages past 64 KiB take u32 offsets.
  fc.array(
    fc.tuple(fc.integer({ min: 30_000, max: 90_000 }), fc.integer()).map(([count, seed]) => {
      const random = mulberry32(seed);
      return encoder.encode(
        Array.from({ length: count }, () =>
          String.fromCharCode(0x21 + Math.floor(random() * 94)),
        ).join(""),
      );
    }),
    { maxLength: 3 },
  ),
  fc.array(fc.constant(new Uint8Array(0)), { maxLength: 500, size: "large" }),
  fc.array(
    fc
      .string({ unit: "grapheme", minLength: 3000, maxLength: 30_000 })
      .map((value) => encoder.encode(value)),
    {
      maxLength: 6,
    },
  ),
);
const uuid = fc.uint8Array({ minLength: 16, maxLength: 16 });
const uuidColumns = fc.oneof(fc.array(uuid, { maxLength: 2000, size: "large" }), pooled(uuid, 20));

const intervals: fc.Arbitrary<TestValue[]> = fc.array(
  fc.record({
    months: fc.integer({ min: INT32_MIN, max: INT32_MAX }),
    days: fc.integer({ min: INT32_MIN, max: INT32_MAX }),
    micros: fc.oneof(int64, fc.bigInt({ min: -(10n ** 12n), max: 10n ** 12n })),
  }),
  { maxLength: 1500, size: "large" },
);

const decimal64: fc.Arbitrary<DecimalValue> = fc.oneof(
  { weight: 10, arbitrary: fc.record({ coefficient: int64, special: fc.constant(0) }) },
  {
    weight: 10,
    arbitrary: fc.record({
      coefficient: fc.bigInt({ min: -(10n ** 9n), max: 10n ** 9n }),
      special: fc.constant(0),
    }),
  },
  {
    weight: 2,
    arbitrary: fc.record({
      coefficient: fc.constantFrom(INT64_MIN, INT64_MAX, 0n),
      special: fc.constant(0),
    }),
  },
  {
    weight: 1,
    arbitrary: fc.record({ coefficient: fc.constant(0n), special: fc.constantFrom(1, 2, 3) }),
  },
);
const decimalNarrow: fc.Arbitrary<DecimalValue> = fc.record({
  coefficient: fc.bigInt({ min: -50_000n, max: 50_000n }),
  special: fc.constant(0),
});
const decimal128: fc.Arbitrary<DecimalValue> = fc.oneof(
  decimal64,
  fc.record({
    coefficient: fc.bigInt({ min: INT128_MIN, max: INT128_MAX }),
    special: fc.constant(0),
  }),
  fc.record({
    coefficient: fc.constantFrom(INT128_MIN, INT128_MAX, INT64_MAX + 1n, INT64_MIN - 1n),
    special: fc.constant(0),
  }),
);

/** Every shape the generators must reach (checked after each seed's properties). */
const REACH = [
  "bool:constant",
  "bool:bitmap",
  "bool:allNull",
  "bool:validity",
  "i32:plain:4",
  "i32:constant",
  "i32:for:1",
  "i32:for:2",
  "i32:delta:0",
  "i32:delta:1",
  "i32:delta:2",
  "i32:allNull",
  "i32:validity",
  "i64:plain:8",
  "i64:constant",
  "i64:for:1",
  "i64:for:2",
  "i64:for:4",
  "i64:delta:0",
  "i64:delta:1",
  "i64:delta:4",
  "i64:validity",
  "f64:plain",
  "f64:constant",
  "f32:plain",
  "f32:constant",
  "varlen:plain:2",
  "varlen:constant",
  "varlen:dictionary:0",
  "varlen:dictionary:1",
  "varlen:dictionary:2",
  "varlen:validity",
  "varlen:allNull",
  "text:dictionary-overflow",
  "fixed16:plain",
  "fixed16:dictionary:1",
  "fixed16:constant",
  "interval:plain",
  "interval:constant",
  "dec64:for",
  "dec64:plain",
  "dec64:specials",
  "dec128:plain",
  "dec128:specials",
];

describe.each(SEEDS)("page codecs round-trip every value (seed %i)", (seed) => {
  const property = (
    arbitrary: fc.Arbitrary<Case>,
    run: (test: Case) => Promise<void>,
    numRuns = RUNS,
  ): Promise<void> =>
    // A failure shrinks for at most 30 s, then reports its best counterexample; a run that is
    // merely slow fails instead of silently doing fewer cases.
    fc.assert(fc.asyncProperty(arbitrary, run), {
      seed,
      numRuns,
      interruptAfterTimeLimit: 30_000,
      markInterruptAsFailure: true,
    });

  it("bool", () =>
    property(
      withNulls(
        fc.oneof(
          fc.array(fc.boolean(), { maxLength: 4000, size: "large" }),
          fc.array(fc.constant(true), { maxLength: 100 }),
        ),
      ),
      (test) => roundTrip({ kind: "bool" }, test),
    ));

  it("int32 and date", async () => {
    await property(withNulls(int32Columns), (test) => roundTrip({ kind: "int32", bits: 32 }, test));
    await property(
      withNulls(
        fc.array(fc.oneof(int32, fc.constantFrom(DATE_NEG_INFINITY, DATE_POS_INFINITY)), {
          maxLength: 2000,
          size: "large",
        }),
      ),
      (test) => roundTrip({ kind: "date" }, test),
      RUNS / 2,
    );
  });

  it("int64 from both representations", async () => {
    await property(withNulls(int64Columns), (test) => roundTrip({ kind: "int64" }, test));
    await property(withNulls(int64Columns), (test) =>
      roundTrip({ kind: "int64" }, test, { safe: true }),
    );
  });

  it("timestamps with infinity sentinels", () =>
    property(withNulls(timestamps), (test) =>
      roundTrip({ kind: "timestamp", zoned: true }, test, { safe: true }),
    ));

  it("float64 and float32 including -0, NaN and infinities", async () => {
    await property(withNulls(float64Columns), (test) => roundTrip({ kind: "float64" }, test));
    await property(withNulls(float32Columns), (test) => roundTrip({ kind: "float32" }, test));
  });

  it("text: empty, long, pooled and unique values", () =>
    property(withNulls(textColumns), (test) => roundTrip({ kind: "text" }, test)));

  it("bytes and uuid", async () => {
    await property(
      withNulls(fc.array(fc.uint8Array({ maxLength: 64 }), { maxLength: 1500, size: "large" })),
      (test) => roundTrip({ kind: "bytes" }, test),
    );
    await property(withNulls(uuidColumns), (test) => roundTrip({ kind: "uuid" }, test));
  });

  it("interval", () =>
    property(withNulls(intervals), (test) => roundTrip({ kind: "interval" }, test), RUNS / 2));

  it("decimal extremes per tier", async () => {
    const type: PhysicalType = { kind: "decimal", precision: null, scale: 2 };
    await property(withNulls(fc.array(decimal64, { maxLength: 2000, size: "large" })), (test) =>
      roundTrip(type, test, { scale: 2 }),
    );
    await property(withNulls(fc.array(decimalNarrow, { maxLength: 2000, size: "large" })), (test) =>
      roundTrip(type, test, { scale: 2, tier: "int128" }),
    );
    await property(withNulls(fc.array(decimal64, { maxLength: 2000, size: "large" })), (test) =>
      roundTrip(type, test, { scale: 2, safe: true }),
    );
    await property(withNulls(fc.array(decimal128, { maxLength: 2000, size: "large" })), (test) =>
      roundTrip(type, test, { scale: 2, tier: "int128" }),
    );
  });

  it("mixed-scale NUMERIC keeps every value's own scale", async () => {
    const type: PhysicalType = { kind: "decimal", precision: null, scale: null };
    const scaled = (
      coefficient: fc.Arbitrary<DecimalValue>,
      scale: fc.Arbitrary<number>,
    ): fc.Arbitrary<DecimalValue> =>
      fc.tuple(coefficient, scale).map(([value, s]) => ({ ...value, scale: s }));
    const columnsOf = (coefficient: fc.Arbitrary<DecimalValue>): fc.Arbitrary<TestValue[]> =>
      fc.oneof(
        fc.array(scaled(coefficient, fc.integer({ min: 0, max: 20 })), {
          maxLength: 2000,
          size: "large",
        }),
        fc.array(scaled(decimalNarrow, fc.constantFrom(0, 2, 16_383)), {
          maxLength: 2000,
          size: "large",
        }),
        // Long runs at one scale: pages with a single (uniform) scale.
        fc
          .tuple(
            fc.integer({ min: 0, max: 30 }),
            fc.array(coefficient, { maxLength: 2000, size: "large" }),
          )
          .map(([s, values]) => values.map((value) => ({ ...value, scale: s }))),
      );
    await property(withNulls(columnsOf(decimal128)), (test) =>
      roundTrip(type, test, { tier: "int128" }),
    );
    await property(withNulls(columnsOf(decimal64)), (test) => roundTrip(type, test));
  });

  it("reached every page shape (runs last)", () => {
    expect(REACH.filter((name) => !reached.has(name))).toEqual([]);
  });
});

// ---------------------------------------------------------------------------------------------

async function encodingsOf(
  type: PhysicalType,
  values: TestValue[],
  options = {},
): Promise<number[]> {
  const { space, group } = await writeGroup([{ type, values }], options);
  const chunk = group.columns[0];
  if (chunk === undefined) throw new Error("no chunk");
  const reader = await openChunk(space, chunk);
  return reader.directory.pages.map((page) => page.encoding);
}

describe("page encoding selection", () => {
  const rows = 4096;
  it("picks constant, FOR, delta and plain for integers", async () => {
    const int = { kind: "int32", bits: 32 } as const;
    expect(await encodingsOf(int, Array<number>(rows).fill(7))).toEqual([PageEncoding.constant]);
    const narrow = await encodingsOf(
      int,
      Array.from({ length: rows }, (_, row) => 1_000_000 + ((row * 37) % 200)),
    );
    expect(narrow.every((encoding) => encoding === PageEncoding.frameOfReference)).toBe(true);
    const random = mulberry32(1);
    const wide = await encodingsOf(
      int,
      Array.from({ length: rows }, () => Math.floor((random() - 0.5) * 4e9)),
    );
    expect(wide.every((encoding) => encoding === PageEncoding.plain)).toBe(true);
    const { space, group } = await writeGroup([
      {
        type: { kind: "int64" },
        values: Array.from({ length: rows }, (_, row) => 2n ** 60n + BigInt(row) * 3n),
      },
    ]);
    const reader = await openChunk(space, defined(group.columns[0]));
    expect(reader.directory.pages.map((page) => page.encoding)).toEqual([
      PageEncoding.frameOfReference,
    ]);
    await reader.load([0]);
    // An arithmetic sequence is a delta page of width 0: header, base, one checkpoint per block.
    expect(reader.directory.pages[0]?.length).toBe(16 + 8 + Math.ceil(rows / DELTA_BLOCK) * 8);
  });

  it("stores low-cardinality text as dictionary codes and unique text plain", async () => {
    const pool = ["alpha", "bravo", "charlie"].map((word) => encoder.encode(word));
    const low = await encodingsOf(
      { kind: "text" },
      Array.from({ length: rows }, (_, row) => pool[row % 3] ?? null),
    );
    expect(low.every((encoding) => encoding === PageEncoding.dictionary)).toBe(true);
    const unique = await encodingsOf(
      { kind: "text" },
      Array.from({ length: rows }, (_, row) => encoder.encode(`user-${String(row)}`)),
    );
    expect(unique.every((encoding) => (encoding & 0x7f) === PageEncoding.plain)).toBe(true);
  });

  it("overflows a full dictionary to plain pages and still reads every value", async () => {
    const values = Array.from({ length: rows }, (_, row) =>
      encoder.encode(`v${String(row < 2000 ? row % 10 : row)}`),
    );
    const encodings = await encodingsOf({ kind: "text" }, values, {
      dictionaryEntries: 16,
      pageBytes: 512,
    });
    expect(encodings).toContain(PageEncoding.dictionary);
    expect(encodings).toContain(PageEncoding.plain);
    const { space, group } = await writeGroup([{ type: { kind: "text" }, values }], {
      dictionaryEntries: 16,
      pageBytes: 512,
    });
    const read = await readChunk(space, defined(group.columns[0]));
    expect(read.every((value, row) => sameValue(value, values[row] ?? null))).toBe(true);
  });

  it("uses u32 offsets for a plain page past 64 KiB (raw pages above the default)", async () => {
    const random = mulberry32(4);
    const values = Array.from({ length: 4 }, () =>
      encoder.encode(
        Array.from({ length: 40_000 }, () =>
          String.fromCharCode(0x21 + Math.floor(random() * 94)),
        ).join(""),
      ),
    );
    const { space, group } = await writeGroup([{ type: { kind: "text" }, values }], {
      rawPageBytes: 256 * 1024,
      gzip: false,
    });
    const chunk = defined(group.columns[0]);
    const reader = await openChunk(space, chunk);
    const page = defined(reader.directory.pages[0]);
    expect(page.rowCount).toBe(4);
    expect(space.bytes[chunk.data.offset + page.offset + 3]).toBe(4);
    const read = await readChunk(space, chunk);
    expect(read.every((value, row) => sameValue(value, values[row] ?? null))).toBe(true);
  });

  it("gzips a large compressible page only when it halves it", async () => {
    const text = Array.from({ length: rows }, (_, row) =>
      encoder.encode(`the quick brown fox ${String(row % 977)} jumps`),
    );
    const zipped = await encodingsOf({ kind: "text" }, text, {
      dictionaryEntries: 1,
      pageBytes: 64 * 1024,
      gzip: true,
    });
    expect(zipped.some((encoding) => (encoding & PageEncoding.gzip) !== 0)).toBe(true);
    const random = mulberry32(9);
    const noise = await encodingsOf(
      { kind: "float64" },
      Array.from({ length: rows }, () => random()),
    );
    expect(noise.every((encoding) => (encoding & PageEncoding.gzip) === 0)).toBe(true);
    const { space, group } = await writeGroup([{ type: { kind: "text" }, values: text }], {
      dictionaryEntries: 1,
      pageBytes: 64 * 1024,
      gzip: true,
    });
    const read = await readChunk(space, defined(group.columns[0]));
    expect(read.every((value, row) => sameValue(value, text[row] ?? null))).toBe(true);
  });
});

describe("page writer contract", () => {
  it("refuses coefficients past 128 bits (typed) and a scale its declaration forbids", async () => {
    const type: PhysicalType = { kind: "decimal", precision: null, scale: null };
    const open = (column: PhysicalType): GroupWriter =>
      new GroupWriter({
        columns: [{ columnId: 1, type: column }],
        writer: new MemoryExtentSpace().beginWrite("test"),
      });
    const vector = vectorOf(type, [{ coefficient: 15n, special: 0 }], { scale: 2 });
    const huge = {
      ...vector,
      data: { ...vector.data, coefficients: { repr: "wide", values: [2n ** 200n] } },
    } as typeof vector;
    await expect(open(type).append([huge], 1)).rejects.toBeInstanceOf(UnsupportedPageLayoutError);
    const declared: PhysicalType = { kind: "decimal", precision: 10, scale: 2 };
    await expect(
      open(declared).append(
        [vectorOf(declared, [{ coefficient: 1n, special: 0 }], { scale: 3 })],
        1,
      ),
    ).rejects.toBeInstanceOf(RangeError);
  });

  it("stores a uniform-scale batch of an unconstrained NUMERIC as one page scale", async () => {
    const type: PhysicalType = { kind: "decimal", precision: null, scale: null };
    const space = new MemoryExtentSpace();
    const writer = new GroupWriter({
      columns: [{ columnId: 1, type }],
      writer: space.beginWrite("test"),
    });
    const values = Array.from({ length: 100 }, (_, row) => ({
      coefficient: BigInt(row) * 7n,
      special: 0,
    }));
    await writer.append([vectorOf(type, values, { scale: 4 })], values.length);
    const group = await writer.finish();
    const read = await readChunk(space, defined(group.columns[0]));
    expect(read).toEqual(values.map((value) => ({ ...value, scale: 4 })));
  });

  it("splits groups at maxRows and frees every extent on abort", async () => {
    const space = new MemoryExtentSpace();
    const type: PhysicalType = { kind: "int64" };
    const writer = new GroupWriter({
      columns: [{ columnId: 1, type }],
      writer: space.beginWrite("test"),
      maxRows: 1000,
      inlineBytes: 0,
    });
    const values = Array.from({ length: 1500 }, (_, row) => BigInt(row) * 1_000_003n);
    expect(await writer.append([vectorOf(type, values)], 1500)).toBe(1000);
    expect(await writer.append([vectorOf(type, values)], 1500)).toBe(0);
    await writer.abort();
    expect(space.allocator.reserved).toBe(0);
  });

  it("relocates a chunk that outgrows its reservation and trims every extent", async () => {
    const space = new MemoryExtentSpace();
    const type: PhysicalType = { kind: "text" };
    const writer = new GroupWriter({
      columns: [
        { columnId: 1, type },
        { columnId: 2, type },
      ],
      writer: space.beginWrite("test"),
      inlineBytes: 0,
      maxRows: 3000,
      dictionaryEntries: 1,
    });
    const values: TestValue[] = [];
    const random = mulberry32(3);
    for (let batch = 0; batch < 30; batch++) {
      // Values grow from 5 to ~2,000 bytes, so early estimates fall short and both columns
      // interleave their extents (neither can grow in place).
      const part = Array.from({ length: 100 }, () =>
        encoder.encode("x".repeat(5 + Math.floor(random() * batch * 70))),
      );
      values.push(...part);
      await writer.append([vectorOf(type, part), vectorOf(type, part)], part.length);
    }
    const group = await writer.finish();
    const used = group.extents.reduce(
      (sum, extent) => sum + Math.ceil(extent.length / 4096) * 4096,
      0,
    );
    expect(space.allocator.reserved).toBe(used);
    expect(space.relocations).toBeGreaterThan(0);
    for (const chunk of group.columns) {
      const read = await readChunk(space, chunk);
      expect(read.every((value, row) => sameValue(value, values[row] ?? null))).toBe(true);
    }
  });

  it("accepts constant and dictionary input vectors", async () => {
    const type: PhysicalType = { kind: "text" };
    const dictionary = vectorOf(type, [encoder.encode("a"), encoder.encode("bb")]);
    const codes = Int32Array.of(1, 0, 1, 1);
    const space = new MemoryExtentSpace();
    const writer = new GroupWriter({
      columns: [{ columnId: 1, type }],
      writer: space.beginWrite("test"),
    });
    await writer.append(
      [{ form: "dictionary", type, length: 4, codes, validity: null, dictionary, dictionaryId: 1 }],
      4,
    );
    await writer.append(
      [{ form: "constant", type, length: 3, value: vectorOf(type, [encoder.encode("ccc")]) }],
      3,
    );
    const group = await writer.finish();
    const read = await readChunk(space, defined(group.columns[0]));
    expect(read.map((value) => new TextDecoder().decode(value as Uint8Array))).toEqual([
      "bb",
      "a",
      "bb",
      "bb",
      "ccc",
      "ccc",
      "ccc",
    ]);
  });

  it("charges its buffers to a grant and returns them", async () => {
    let held = 0;
    const charge = {
      allocate: (bytes: number) => ((held += bytes), true),
      free: (bytes: number) => void (held -= bytes),
    };
    const { group } = await writeGroup(
      [
        {
          type: { kind: "text" },
          values: Array.from({ length: 5000 }, (_, row) => encoder.encode(`k${String(row % 50)}`)),
        },
      ],
      { charge },
    );
    expect(group.rowCount).toBe(5000);
    expect(held).toBe(0);
    let refused = 0;
    const tight = {
      allocate: (bytes: number) =>
        refused + bytes > 2_000_000 ? false : ((refused += bytes), true),
      free: (bytes: number) => void (refused -= bytes),
    };
    const values = Array.from({ length: 5000 }, (_, row) => encoder.encode(`k${String(row % 50)}`));
    const { space, group: tightGroup } = await writeGroup([{ type: { kind: "text" }, values }], {
      charge: tight,
    });
    const read = await readChunk(space, defined(tightGroup.columns[0]));
    expect(read.every((value, row) => sameValue(value, values[row] ?? null))).toBe(true);
  });
});

describe("page and chunk statistics", () => {
  it("records exact bounds, nulls, dictionary size and a distinct sketch per chunk", async () => {
    const rows = 10_000;
    const long = encoder.encode("z".repeat(40));
    const { space, group } = await writeGroup(
      [
        {
          type: { kind: "int32", bits: 32 },
          values: Array.from({ length: rows }, (_, row) => (row % 7 === 0 ? null : row - 5000)),
        },
        {
          type: { kind: "float64" },
          values: Array.from({ length: rows }, (_, row) => (row === 77 ? Number.NaN : row / 4)),
        },
        {
          type: { kind: "text" },
          values: Array.from({ length: rows }, (_, row) =>
            row === 3 ? long : encoder.encode(`k${String(row % 40)}`),
          ),
        },
        {
          type: { kind: "int64" },
          values: Array.from({ length: rows }, (_, row) => BigInt(row) * 1_000_003n),
        },
      ],
      { pageBytes: 4096 },
    );
    const [ints, floats, texts, bigs] = group.columns.map((chunk) => chunk);
    expect(defined(ints).statistics).toMatchObject({
      nullCount: Math.ceil(rows / 7),
      min: -4999,
      max: 4999,
    });
    expect(defined(floats).statistics.min).toBe(0);
    expect(defined(floats).statistics.max).toBeNaN();
    const text = defined(texts).statistics;
    expect(text.dictionarySize).toBe(41);
    expect(text.min).toEqual(encoder.encode("k0"));
    // A bound longer than the prefix: the shortest string ≥ every value ("z" × 31 then "{").
    expect(text.max).toEqual(encoder.encode(`${"z".repeat(31)}{`));
    expect(defined(bigs).statistics).toMatchObject({ min: 0n, max: BigInt(rows - 1) * 1_000_003n });
    // Page entries tile the rows and carry their own bounds.
    const reader = await openChunk(space, defined(ints));
    let next = 0;
    for (const page of reader.directory.pages) {
      expect(page.firstRow).toBe(next);
      next += page.rowCount;
      expect(page.min).toBeTypeOf("number");
    }
    expect(next).toBe(rows);
    // The sketch lives inside the directory blob, under its own checksum.
    for (const [chunk, distinct] of [
      [ints, rows - Math.ceil(rows / 7)],
      [texts, 41],
      [bigs, rows],
    ] as const) {
      const ref = defined(chunk).distinctSketch;
      if (ref === null) throw new Error("no sketch");
      const lease = await space.read([ref], { budget: UNLIMITED });
      const estimate = DistinctSketch.parse(defined(lease.bytes[0])).estimate();
      lease.release();
      expect(Math.abs(estimate - distinct) / distinct).toBeLessThan(0.1);
    }
  });
});

describe("byte bounds only for byte-ordered types", () => {
  it("gives text, bytes and uuid bounds, and arrays and JSON none", async () => {
    const values = Array.from({ length: 300 }, (_, row) => encoder.encode(`v${String(row % 17)}`));
    const types: PhysicalType[] = [
      { kind: "text" },
      { kind: "bytes" },
      { kind: "json", binary: true },
      { kind: "array", element: { kind: "int32", bits: 32 } },
    ];
    const { group } = await writeGroup(types.map((type) => ({ type, values })));
    expect(group.columns.map((chunk) => chunk.statistics.min !== null)).toEqual([
      true,
      true,
      false,
      false,
    ]);
    expect(group.columns.map((chunk) => chunk.statistics.dictionarySize)).toEqual([17, 17, 17, 17]);
  });
});

describe("distinct sketch", () => {
  it("merges by register maximum and round-trips both serialized forms", () => {
    const a = new DistinctSketch();
    const b = new DistinctSketch();
    for (let value = 0; value < 30_000; value++) (value < 20_000 ? a : b).add(mix32(value));
    a.merge(b);
    expect(Math.abs(a.estimate() - 30_000) / 30_000).toBeLessThan(0.1);
    expect(DistinctSketch.parse(a.serialize()).registers).toEqual(a.registers);
    const small = new DistinctSketch();
    for (let value = 0; value < 50; value++) small.add(mix32(value));
    expect(small.serialize().length).toBeLessThan(200);
    expect(Math.round(DistinctSketch.parse(small.serialize()).estimate())).toBeGreaterThan(45);
  });
});

describe("chunk reader over the extent space", () => {
  const tracked = (): {
    held: number;
    allocate(bytes: number): boolean;
    free(bytes: number): void;
  } => ({
    held: 0,
    allocate(bytes: number): boolean {
      this.held += bytes;
      return true;
    },
    free(bytes: number): void {
      this.held -= bytes;
    },
  });

  it("is synchronous when the directory and pages are cached, and charges what it holds", async () => {
    const values = Array.from({ length: 5000 }, (_, row) => encoder.encode(`k${String(row % 30)}`));
    const { space: data, group } = await writeGroup([{ type: { kind: "text" }, values }], {
      pageBytes: 1024,
    });
    const chunk = defined(group.columns[0]);
    const space = data.view({});
    const cold = tracked();
    const opening = ChunkReader.open(space, chunk, { budget: cold });
    expect(opening).toBeInstanceOf(Promise);
    const first = await opening;
    const all = Array.from({ length: first.directory.pages.length }, (_, page) => page);
    const loading = first.load(all);
    expect(loading).toBeInstanceOf(Promise);
    await loading;
    expect(space.leased).toBeGreaterThan(0);
    first.close();
    expect(space.leased).toBe(0);
    expect(cold.held).toBe(0);

    const warm = tracked();
    const second = ChunkReader.open(space, chunk, { budget: warm });
    if (second instanceof Promise) throw new Error("a cached directory opens synchronously");
    expect(second.load(all)).toBeUndefined();
    expect(second.loadDictionary()).toBe(second.dictionary);
    const stored = second.directory.pages.reduce((sum, page) => sum + page.length, 0);
    const directory = directoryCharge(second.directory);
    expect(warm.held).toBe(directory + stored + (second.directory.dictionary?.length ?? 0));
    const codes = new Int32Array(values.length);
    second.decodeCodes(0, values.length, codes, null, 0);
    const dictionary = defined(second.dictionary ?? undefined);
    expect(
      codes.every((code, row) => sameValue(valueAt(dictionary, code), values[row] ?? null)),
    ).toBe(true);
    second.release([0]);
    expect(warm.held).toBe(
      directory +
        stored +
        (second.directory.dictionary?.length ?? 0) -
        defined(second.directory.pages[0]).length,
    );
    second.close();
    expect(warm.held).toBe(0);
  });

  it("reads in calls of at most 4 MiB and holds nothing after a refused budget", async () => {
    const random = mulberry32(11);
    const values = Array.from({ length: 60_000 }, () =>
      encoder.encode(
        Array.from({ length: 90 }, () =>
          String.fromCharCode(0x21 + Math.floor(random() * 94)),
        ).join(""),
      ),
    );
    const { space: data, group } = await writeGroup([{ type: { kind: "text" }, values }], {
      gzip: false,
    });
    const chunk = defined(group.columns[0]);
    const space = data.view({ cache: false });
    const reader = await ChunkReader.open(space, chunk, { budget: UNLIMITED });
    const all = Array.from({ length: reader.directory.pages.length }, (_, page) => page);
    const total = reader.directory.pages.reduce((sum, page) => sum + page.length, 0);
    expect(total).toBeGreaterThan(MAX_READ_BYTES);
    const before = space.reads;
    await reader.load(all);
    expect(space.reads - before).toBeGreaterThanOrEqual(Math.ceil(total / MAX_READ_BYTES));
    const read = await readChunk(data, chunk);
    expect(read.every((value, row) => sameValue(value, values[row] ?? null))).toBe(true);
    reader.close();
    expect(space.leased).toBe(0);

    // A budget that admits the directory read and its parsed charge, then refuses.
    let admitted = 2;
    const refusing = { allocate: (): boolean => admitted-- > 0, free: (): void => undefined };
    const strict = await ChunkReader.open(space, chunk, { budget: refusing });
    await expect(strict.load([0])).rejects.toBeInstanceOf(ReadDeniedError);
    expect(strict.isLoaded(0)).toBe(false);
    expect(space.leased).toBe(0);
  });
});

describe("the 1 MiB blob cap", () => {
  const noise = (length: number, seed: number): Uint8Array => {
    const random = mulberry32(seed);
    return Uint8Array.from({ length }, () => 0x21 + Math.floor(random() * 94));
  };

  it("keeps every page under 1 MiB, splitting large values across pages", async () => {
    const values = [noise(600_000, 1), noise(600_000, 2), noise(10, 3)];
    const { space, group } = await writeGroup([{ type: { kind: "bytes" }, values }], {
      rawPageBytes: 4 << 20,
    });
    const chunk = defined(group.columns[0]);
    const reader = await openChunk(space, chunk);
    expect(reader.directory.pages.every((page) => page.length <= MAX_BLOB_BYTES)).toBe(true);
    const read = await readChunk(space, chunk);
    expect(read.every((value, row) => sameValue(value, values[row] ?? null))).toBe(true);
  });

  it("refuses one value too large for a page with a typed error", async () => {
    await expect(
      writeGroup([{ type: { kind: "bytes" }, values: [noise(MAX_BLOB_BYTES + 1, 4)] }]),
    ).rejects.toBeInstanceOf(UnsupportedPageLayoutError);
  });
});
