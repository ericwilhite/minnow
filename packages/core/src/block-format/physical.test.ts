import { describe, expect, it } from "vitest";
import {
  buildPhysicalColumnFromRanges,
  concatenatePhysicalColumns,
  crc32,
  decodeBlock,
  decodeColumn,
  decodePhysicalBlock,
  encodeBlock,
  encodeColumn,
  encodePhysicalBlock,
  measurePhysicalColumnRanges,
  physicalBitmapByteLength,
  physicalColumnByteLength,
  slicePhysicalColumn,
  validatePhysicalColumn,
} from "./index.js";
import type { ColumnInput, Compression, LogicalType, PhysicalColumnPayload } from "./index.js";

/** Recomputes the block envelope checksum after a test mutates header or metadata bytes. */
function resignEnvelope(block: Uint8Array): void {
  const view = new DataView(block.buffer, block.byteOffset, block.byteLength);
  const metadataLength = view.getUint32(24, true);
  view.setUint32(4, crc32(block.subarray(8, 40 + metadataLength)), true);
}

const columns: ColumnInput[] = [
  {
    type: "boolean",
    values: [true, false, null, true, null, false, true, true, false, null],
  },
  { type: "number", values: [1, -2.5, null, 42, 0, -0, 0.125, null, 99] },
  { type: "string", values: ["hello", "", null, "🌍", "café", "漢字", "a\u0301"] },
  {
    type: "datetime",
    values: [
      new Date("2026-01-02T03:04:05Z"),
      null,
      new Date("1970-01-01T00:00:00Z"),
      new Date("2040-12-31T23:59:59Z"),
    ],
  },
];

it("rejects unknown physical logical types at runtime", () => {
  expect(() => physicalColumnByteLength("binary" as LogicalType, 0)).toThrow(
    "Unknown logical type: binary",
  );
  expect(() =>
    validatePhysicalColumn({
      type: "binary" as LogicalType,
      rowCount: 0,
      bytes: new Uint8Array(),
    }),
  ).toThrow("Unknown logical type: binary");
});

for (const compression of ["raw", "rle", "gzip"] satisfies Compression[]) {
  describe(`${compression} physical blocks`, () => {
    for (const column of columns) {
      it(`round trips ${column.type} without materializing rows`, async () => {
        const rowBlock = await encodeBlock(column, compression);
        const physical = await decodePhysicalBlock(rowBlock);
        const expected = encodeColumn(column);

        expect(physical.column.bytes).toEqual(expected.bytes);
        expect(physical.column.rowCount).toBe(column.values.length);
        expect(physical.column.nullCount).toBe(
          column.values.filter((value) => value === null).length,
        );
        expect(physical.column.metadata).toEqual(expected.metadata);
        expect("values" in physical.column).toBe(false);

        const physicalBlock = await encodePhysicalBlock(physical.column, compression);
        expect((await decodeBlock(physicalBlock)).column).toEqual(column);
        expect((await decodePhysicalBlock(physicalBlock)).column.bytes).toEqual(expected.bytes);
      });
    }
  });
}

it("measures and rebuilds boolean ranges across bitmap byte boundaries", () => {
  const firstValues = [
    true,
    null,
    false,
    true,
    false,
    null,
    true,
    false,
    true,
    null,
    false,
    true,
    true,
    false,
    null,
    true,
    false,
    true,
  ] as const;
  const secondValues = [
    null,
    false,
    true,
    true,
    null,
    false,
    true,
    false,
    true,
    true,
    false,
    null,
    true,
    false,
    true,
    null,
    false,
    true,
    true,
  ] as const;
  const first = physical({ type: "boolean", values: firstValues });
  const second = physical({ type: "boolean", values: secondValues });
  const ranges = [
    { column: first, start: 5, end: 13 },
    { column: second, start: 7, end: 18 },
  ];
  const expected = [...firstValues.slice(5, 13), ...secondValues.slice(7, 18)];

  const measurement = measurePhysicalColumnRanges("boolean", ranges);
  expect(measurement).toMatchObject({
    rowCount: 19,
    nullCount: expected.filter((value) => value === null).length,
    validityByteLength: 3,
    valueByteLength: 3,
    stringContentByteLength: 0,
    encodedByteLength: 6,
    metadata: {},
  });
  const rebuilt = buildPhysicalColumnFromRanges("boolean", ranges);
  expect(decodePhysical(rebuilt)).toEqual(expected);
  expect(rebuilt.bytes.byteLength).toBe(measurement.encodedByteLength);
  expect(Array.from(rebuilt.bytes)).toEqual([0xee, 0xef, 0x06, 0xca, 0xa6, 0x04]);
});

it("slices fixed-width number rows at bit boundaries and recalculates the zone map", () => {
  const values = [null, 100, 99, 98, 97, 96, 95, null, -8, 7, 6, 5, 4, 3, 2, null, -16, 1];
  const source = physical({ type: "number", values });
  const sliced = slicePhysicalColumn(source, 7, 17);

  expect(sliced.rowCount).toBe(10);
  expect(sliced.nullCount).toBe(2);
  expect(sliced.metadata.zoneMap).toEqual({ min: -16, max: 7 });
  expect(sliced.bytes.byteLength).toBe(physicalBitmapByteLength(10) + 10 * 8);
  expect(decodePhysical(sliced)).toEqual(values.slice(7, 17));
});

it("concatenates datetime ranges and derives metadata from only selected rows", () => {
  const early = [
    new Date("1999-01-01T00:00:00Z"),
    null,
    new Date("2001-01-01T00:00:00Z"),
    new Date("2002-01-01T00:00:00Z"),
  ];
  const late = [new Date("2040-01-01T00:00:00Z"), new Date("2050-01-01T00:00:00Z"), null];
  const first = slicePhysicalColumn(physical({ type: "datetime", values: early }), 1, 4);
  const second = slicePhysicalColumn(physical({ type: "datetime", values: late }), 0, 2);
  const concatenated = concatenatePhysicalColumns("datetime", [first, second]);
  const expected = [...early.slice(1, 4), ...late.slice(0, 2)];

  expect(decodePhysical(concatenated)).toEqual(expected);
  expect(concatenated.nullCount).toBe(1);
  expect(concatenated.metadata.zoneMap).toEqual({
    min: new Date("2001-01-01T00:00:00Z").getTime(),
    max: new Date("2050-01-01T00:00:00Z").getTime(),
  });
});

it("returns no zone map for selected all-null fixed-width rows", () => {
  const numbers = physical({ type: "number", values: [4, null, null, 9] });
  const datetimes = physical({
    type: "datetime",
    values: [new Date("2020-01-01T00:00:00Z"), null, null],
  });

  expect(slicePhysicalColumn(numbers, 1, 3).metadata).toEqual({});
  expect(slicePhysicalColumn(datetimes, 1, 3).metadata).toEqual({});
});

it("rebases string offsets for null, empty, and multibyte row ranges", () => {
  const firstValues = ["zero", "é", null, "🌍", "", "漢字", "a\u0301", "end"];
  const secondValues = [null, "🚀x", "mañana", "tail"];
  const first = physical({ type: "string", values: firstValues });
  const second = physical({ type: "string", values: secondValues });
  const ranges = [
    { column: first, start: 1, end: 7 },
    { column: second, start: 0, end: 3 },
  ];
  const expected = [...firstValues.slice(1, 7), ...secondValues.slice(0, 3)];
  const expectedContentByteLength = expected.reduce(
    (total, value) => total + new TextEncoder().encode(value ?? "").byteLength,
    0,
  );

  const measurement = measurePhysicalColumnRanges("string", ranges);
  expect(measurement).toMatchObject({
    rowCount: 9,
    nullCount: 2,
    validityByteLength: 2,
    valueByteLength: 40 + expectedContentByteLength,
    stringContentByteLength: expectedContentByteLength,
    encodedByteLength: 42 + expectedContentByteLength,
    metadata: {},
  });
  const rebuilt = buildPhysicalColumnFromRanges("string", ranges);
  expect(rebuilt.bytes.byteLength).toBe(measurement.encodedByteLength);
  expect(decodePhysical(rebuilt)).toEqual(expected);

  const offsets = new DataView(rebuilt.bytes.buffer, rebuilt.bytes.byteOffset + 2, 40);
  expect(offsets.getUint32(0, true)).toBe(0);
  expect(offsets.getUint32(2 * 4, true)).toBe(offsets.getUint32(1 * 4, true));
  expect(offsets.getUint32(7 * 4, true)).toBe(offsets.getUint32(6 * 4, true));
  expect(offsets.getUint32(9 * 4, true)).toBe(expectedContentByteLength);
});

for (const type of ["boolean", "number", "string", "datetime"] satisfies LogicalType[]) {
  it(`builds an exact empty ${type} physical column`, () => {
    const column = concatenatePhysicalColumns(type, []);
    expect(column).toMatchObject({ type, rowCount: 0, nullCount: 0, metadata: {} });
    expect(column.bytes.byteLength).toBe(physicalColumnByteLength(type, 0));
    expect(decodePhysical(column)).toEqual([]);
  });
}

it("preserves canonical null counts and numeric metadata in physical block headers", async () => {
  const source = physical({ type: "number", values: [100, null, -4, 8] });
  const selected = slicePhysicalColumn(source, 1, 4);
  const block = await encodePhysicalBlock(selected, "gzip");
  const decoded = await decodePhysicalBlock(block);

  expect(decoded.description.nullCount).toBe(1);
  expect(decoded.description.metadata.zoneMap).toEqual({ min: -4, max: 8 });
  expect(decoded.column.nullCount).toBe(1);
  expect(decoded.column.metadata.zoneMap).toEqual({ min: -4, max: 8 });

  // Raw header corruption is caught by the envelope checksum before any field is trusted.
  const unsignedCorruption = new Uint8Array(block);
  new DataView(unsignedCorruption.buffer).setUint32(20, 2, true);
  await expect(decodePhysicalBlock(unsignedCorruption)).rejects.toThrow("envelope checksum");

  // A consistently re-signed wrong header still fails the payload cross-checks on decode.
  const wrongNullCount = new Uint8Array(block);
  new DataView(wrongNullCount.buffer).setUint32(20, 2, true);
  resignEnvelope(wrongNullCount);
  await expect(decodePhysicalBlock(wrongNullCount)).rejects.toThrow("null count");

  const wrongMetadata = new Uint8Array(block);
  const description = decoded.description;
  const metadataLength = wrongMetadata.byteLength - 40 - description.storedLength;
  const metadata = new TextDecoder().decode(wrongMetadata.subarray(40, 40 + metadataLength));
  const maxCharacter = metadata.lastIndexOf("8");
  expect(maxCharacter).toBeGreaterThanOrEqual(0);
  wrongMetadata[40 + maxCharacter] = "9".charCodeAt(0);
  resignEnvelope(wrongMetadata);
  await expect(decodePhysicalBlock(wrongMetadata)).rejects.toThrow("metadata");
});

it("checksum-verifies physical blocks before returning uncompressed bytes", async () => {
  const block = await encodePhysicalBlock(physical({ type: "string", values: ["safe", "🌍"] }));
  block[block.byteLength - 1] = (block[block.byteLength - 1] ?? 0) ^ 0xff;
  await expect(decodePhysicalBlock(block)).rejects.toThrow("checksum");
});

it("rejects non-canonical bitmaps and invalid fixed-width physical values", async () => {
  const padded = physical({ type: "boolean", values: [true] });
  padded.bytes[0] = (padded.bytes[0] ?? 0) | 0x80;
  expect(() => validatePhysicalColumn(padded)).toThrow("padding");

  const nullBoolean = physical({ type: "boolean", values: [null] });
  nullBoolean.bytes[1] = 1;
  expect(() => validatePhysicalColumn(nullBoolean)).toThrow("null row");

  const nonfinite = physical({ type: "number", values: [1] });
  new DataView(nonfinite.bytes.buffer, nonfinite.bytes.byteOffset + 1, 8).setFloat64(
    0,
    Number.NaN,
    true,
  );
  expect(() => validatePhysicalColumn(nonfinite)).toThrow("physical value");
  await expect(encodePhysicalBlock(nonfinite)).rejects.toThrow("physical value");

  const fractionalDatetime = physical({
    type: "datetime",
    values: [new Date("2026-01-01T00:00:00Z")],
  });
  new DataView(
    fractionalDatetime.bytes.buffer,
    fractionalDatetime.bytes.byteOffset + 1,
    8,
  ).setFloat64(0, 0.5, true);
  expect(() => validatePhysicalColumn(fractionalDatetime)).toThrow("datetime physical value");
});

for (const type of ["number", "datetime"] as const) {
  it(`rejects non-zero ${type} bytes hidden behind a null validity bit`, async () => {
    const source =
      type === "number"
        ? physical({ type, values: [1, null, 2] })
        : physical({
            type,
            values: [new Date("2020-01-01T00:00:00Z"), null, new Date("2021-01-01T00:00:00Z")],
          });
    const validityByteLength = physicalBitmapByteLength(source.rowCount);
    const nullSlotOffset = validityByteLength + 8;
    source.bytes[nullSlotOffset + 7] = 0x80;

    expect(() => validatePhysicalColumn(source)).toThrow(`${type} value bytes`);
    await expect(encodePhysicalBlock(source)).rejects.toThrow(`${type} value bytes`);
  });
}

it("validates string offsets, null spans, and UTF-8 without decoding row strings", () => {
  const splitCodePoint = physical({ type: "string", values: ["🌍"] });
  new DataView(splitCodePoint.bytes.buffer, splitCodePoint.bytes.byteOffset + 1, 8).setUint32(
    4,
    1,
    true,
  );
  expect(() => validatePhysicalColumn(splitCodePoint)).toThrow("UTF-8");

  const hiddenNullContent: PhysicalColumnPayload<"string"> = {
    type: "string",
    rowCount: 1,
    bytes: Uint8Array.of(0, 0, 0, 0, 0, 1, 0, 0, 0, 0x78),
  };
  expect(() => validatePhysicalColumn(hiddenNullContent)).toThrow("Null strings");

  const nonzeroFirstOffset = physical({ type: "string", values: ["x"] });
  new DataView(
    nonzeroFirstOffset.bytes.buffer,
    nonzeroFirstOffset.bytes.byteOffset + 1,
    8,
  ).setUint32(0, 1, true);
  expect(() => validatePhysicalColumn(nonzeroFirstOffset)).toThrow("first string offset");
});

it("validates range bounds before allocating an output", () => {
  const source = physical({ type: "number", values: [1, 2, 3] });
  expect(() =>
    measurePhysicalColumnRanges("number", [{ column: source, start: 2, end: 4 }]),
  ).toThrow("out of bounds");
  expect(() => slicePhysicalColumn(source, 2, 1)).toThrow("out of bounds");
});

it("allows overlapping ranges from the same source payload", () => {
  const source = physical({ type: "string", values: ["a", "b", "🌍", null] });
  const rebuilt = buildPhysicalColumnFromRanges("string", [
    { column: source, start: 0, end: 3 },
    { column: source, start: 1, end: 4 },
  ]);
  expect(decodePhysical(rebuilt)).toEqual(["a", "b", "🌍", "b", "🌍", null]);
});

it("validation retains the caller-owned physical byte allocation", () => {
  const input = physical({ type: "string", values: ["owned"] });
  expect(validatePhysicalColumn(input).bytes).toBe(input.bytes);
});

function physical<T extends LogicalType>(input: ColumnInput<T>): PhysicalColumnPayload<T> {
  return {
    type: input.type,
    rowCount: input.values.length,
    bytes: encodeColumn(input).bytes,
  };
}

function decodePhysical<T extends LogicalType>(column: PhysicalColumnPayload<T>): unknown[] {
  // Tests alone use the row decoder as an oracle. The physical implementation never calls it.
  return decodeColumn(column.type, column.bytes, column.rowCount).values;
}
