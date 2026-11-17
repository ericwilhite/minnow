import { required } from "../owner/result-values.js";
/** DECISIONS.md D1/D2/C1–C6 and owner/api.ts: views retain exact typed buffers;
 * scalar decoding is lazy and independent of frame boundaries/encoding. */
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, PUBLIC_RESULT_REACH } from "../../testing/reach.js";
import { decodeResult } from "../owner/result.js";
import type { FramePayload, WireFlat } from "../owner/frame.js";
import type { StatementSummary, WireColumnMeta } from "../owner/protocol.js";
import { encodeArrayValue, decodeArrayValue } from "../vector/array-value.js";
import { setInt128 } from "../vector/int128.js";
import type { PhysicalType } from "../types.js";
export const SUMMARY: StatementSummary = {
  command: "SELECT",
  rowCount: null,
  counts: null,
  lsn: null,
  replayed: false,
  closedEarly: false,
  transaction: "idle",
  catalogVersion: 1,
};
const text = (values: readonly string[]): WireFlat => {
  const encoded = values.map((value) => new TextEncoder().encode(value));
  const bytes = new Uint8Array(encoded.reduce((sum, value) => sum + value.length, 0));
  const offsets = new Int32Array(values.length + 1);
  encoded.forEach((value, i) => {
    bytes.set(value, required(offsets[i]));
    offsets[i + 1] = required(offsets[i]) + value.length;
  });
  return {
    form: "flat",
    length: values.length,
    validity: null,
    data: { layout: "varlen", bytes, offsets },
  };
};
const meta = (
  name: string,
  type: PhysicalType,
  render: WireColumnMeta["render"] = null,
): WireColumnMeta => ({ name, type, nullable: true, sqlType: type.kind, render });
it.each(seedsFor("next-public-result", [0xb730, 0xb731]))(
  "keeps exact values and column ownership across generated wire shapes (seed %i)",
  (seed) => {
    const reach = createReach("public Result", PUBLIC_RESULT_REACH);
    fc.assert(
      fc.property(
        fc.record({
          length: fc.integer({ min: 2, max: 75 }),
          value: fc.bigInt({ min: -(1n << 62n), max: (1n << 62n) - 1n }),
          scale: fc.integer({ min: 0, max: 20 }),
          label: fc.string({ minLength: 0, maxLength: 20 }),
          nullable: fc.boolean(),
        }),
        ({ length, value, scale, label, nullable }) => {
          reach.beginHistory();
          const columns = [
            meta("big", { kind: "int64" }, "bigint"),
            meta("s", { kind: "text" }),
            meta("null", { kind: "int64" }),
            meta("t", { kind: "timestamp", zoned: true }),
            meta("n", { kind: "decimal", precision: null, scale: null }),
          ];
          const expected: unknown[][] = [],
            frames: FramePayload[] = [];
          for (let part = 0; part < 3; part++) {
            const validity = new Uint32Array(Math.ceil(length / 32));
            for (let i = 0; i < length; i++)
              if (!nullable || i % 3 !== 0)
                validity[i >>> 5] = (validity[i >>> 5] ?? 0) | (1 << (i & 31));
            const decimalValue = part === 1 ? value << 50n : part === 2 ? value << 100n : value;
            const limbs = new Uint32Array(length * 4);
            for (let i = 0; i < length; i++) setInt128(limbs, i, part === 1 ? decimalValue : value);
            const coefficients =
              part === 0
                ? { repr: "int64" as const, values: new BigInt64Array(length).fill(value) }
                : part === 1
                  ? { repr: "int128" as const, limbs }
                  : { repr: "wide" as const, values: Array<bigint>(length).fill(decimalValue) };
            frames.push({
              sequence: part,
              rowCount: length,
              dictionaries:
                part === 2
                  ? []
                  : [{ id: 7, from: part, values: text([`${label}${String(part)}`]) }],
              columns: [
                {
                  form: "flat",
                  length,
                  validity,
                  data: {
                    layout: "int64",
                    values: { repr: "int64", values: new BigInt64Array(length).fill(value) },
                  },
                },
                part === 2
                  ? { form: "constant", length, value: text([`${label}constant`]) }
                  : {
                      form: "dictionary",
                      length,
                      validity: null,
                      dictionary: 7,
                      codes: new Int32Array(length).fill(part),
                    },
                {
                  form: "flat",
                  length,
                  validity: new Uint32Array(Math.ceil(length / 32)),
                  data: {
                    layout: "int64",
                    values: { repr: "safe", values: new Float64Array(length).fill(NaN) },
                  },
                },
                {
                  form: "constant",
                  length,
                  value: {
                    form: "flat",
                    length: 1,
                    validity: null,
                    data: {
                      layout: "int64",
                      values: { repr: "safe", values: Float64Array.of(123456) },
                    },
                  },
                },
                {
                  form: "flat",
                  length,
                  validity: null,
                  data: { layout: "decimal", scale, scales: null, coefficients, specials: null },
                },
              ],
            });
            const abs = (decimalValue < 0n ? -decimalValue : decimalValue)
              .toString()
              .padStart(scale + 1, "0");
            const decimal =
              (decimalValue < 0n ? "-" : "") +
              (scale ? `${abs.slice(0, -scale)}.${abs.slice(-scale)}` : abs);
            for (let i = 0; i < length; i++)
              expected.push([
                nullable && i % 3 === 0 ? null : value,
                `${label}${part === 2 ? "constant" : String(part)}`,
                null,
                "2000-01-01T00:00:00.123456Z",
                decimal,
              ]);
          }
          const result = decodeResult({ columns, frames, summary: SUMMARY });
          expect(result.rowCount).toBe(length * 3);
          expect(result.tuples()).toEqual(expected);
          expect(result.tuples()).toBe(result.tuples());
          expect(result.rows()).toEqual(
            expected.map((row) => Object.fromEntries(columns.map((c, i) => [c.name, row[i]]))),
          );
          expect(Array.from(result)).toEqual(result.rows());
          expect(result.row(length)).toBe(result.rows()[length]);
          expect(result.values("big")).toBe(result.values(0));
          const chunks = result.columnChunks("big");
          expect(chunks).toBe(result.columnChunks(0));
          expect(chunks[0]?.kind).toBe("int64");
          const first = required(chunks[0]);
          if (
            first.kind !== "int64" ||
            frames[0]?.columns[0]?.form !== "flat" ||
            frames[0].columns[0].data.layout !== "int64"
          )
            throw new Error("Expected int64");
          expect(first.values).toBe(frames[0].columns[0].data.values.values);
          const joined = result.column("big");
          expect(joined).toBe(result.column(0));
          if (joined.kind !== "int64") throw new Error("Expected int64");
          expect(joined.values).toHaveLength(length * 3);
          for (let i = 0; i < length * 3; i++)
            if (expected[i]?.[0] !== null) expect(joined.values[i]).toBe(value);
          expect(result.column("null").validity).not.toBeNull();
          const dictionary = required(result.columnChunks("s")[0]);
          if (!("encoding" in dictionary) || dictionary.encoding !== "dictionary")
            throw new Error("Expected dictionary");
          expect(dictionary.codes).toBeInstanceOf(Int32Array);
          expect(result.column("s")).toMatchObject({ encoding: "plain" });
          expect(result.column("n")).toMatchObject({
            kind: "decimal",
            coefficients: { repr: "wide" },
          });
          for (const state of PUBLIC_RESULT_REACH) reach.reach(state);
        },
      ),
      { seed, numRuns: 32 },
    );
    reach.assertReached();
  },
);
it("decodes every scalar, exact multidimensional arrays and special metadata", () => {
  const arrayType: PhysicalType = { kind: "int64" };
  const array = encodeArrayValue(arrayType, {
    dimensions: [2, 2],
    lowerBounds: [-2, 4],
    elements: [1n, null, 9223372036854775807n, -1n],
  });
  const one = (data: WireFlat["data"]): WireFlat => ({
    form: "flat",
    length: 1,
    validity: null,
    data,
  });
  const v = text(["unused"]);
  if (v.data.layout !== "varlen") throw new Error("Expected bytes");
  const scalars: Array<[WireColumnMeta, WireFlat, unknown]> = [
    [meta("bool", { kind: "bool" }), one({ layout: "bool", values: Uint8Array.of(1) }), true],
    [
      meta("i", { kind: "int32", bits: 16 }),
      one({ layout: "i32", values: Int32Array.of(-32768) }),
      -32768,
    ],
    [
      meta("r", { kind: "float32" }),
      one({ layout: "f32", values: Float32Array.of(0.1) }),
      Math.fround(0.1),
    ],
    [meta("f", { kind: "float64" }), one({ layout: "f64", values: Float64Array.of(-0) }), -0],
    [meta("d", { kind: "date" }), one({ layout: "i32", values: Int32Array.of(0) }), "2000-01-01"],
    [
      meta("m", { kind: "time" }),
      one({ layout: "int64", values: { repr: "int64", values: BigInt64Array.of(86400000000n) } }),
      "24:00:00",
    ],
    [
      meta("t", { kind: "timestamp", zoned: false }),
      one({ layout: "int64", values: { repr: "int64", values: BigInt64Array.of(-1n) } }),
      "1999-12-31T23:59:59.999999",
    ],
    [
      meta("interval", { kind: "interval" }),
      one({
        layout: "interval",
        months: Int32Array.of(1),
        days: Int32Array.of(-2),
        micros: { repr: "safe", values: Float64Array.of(1) },
      }),
      "1 mon -2 days +00:00:00.000001",
    ],
    [
      meta("bytes", { kind: "bytes" }),
      one({ layout: "varlen", bytes: Uint8Array.of(0, 255), offsets: Int32Array.of(0, 2) }),
      Uint8Array.of(0, 255),
    ],
    [
      meta("uuid", { kind: "uuid" }),
      one({ layout: "fixed16", bytes: Uint8Array.from({ length: 16 }, (_, i) => i) }),
      "00010203-0405-0607-0809-0a0b0c0d0e0f",
    ],
    [
      meta("array", { kind: "array", element: arrayType }),
      one({ layout: "varlen", bytes: array, offsets: Int32Array.of(0, array.length) }),
      [
        [1n, null],
        [9223372036854775807n, -1n],
      ],
    ],
    [meta("enum", { kind: "enum", name: "state", labels: ["λ"] }), text(["λ"]), "λ"],
    [meta("json", { kind: "json", binary: false }), text(['{"x":1.00}']), '{"x":1.00}'],
    [
      meta("count", { kind: "int64" }, "number"),
      one({ layout: "int64", values: { repr: "int64", values: BigInt64Array.of(123n) } }),
      123,
    ],
    [
      meta("sum", { kind: "decimal", precision: null, scale: 0 }, "bigint"),
      one({
        layout: "decimal",
        scale: 0,
        scales: null,
        coefficients: { repr: "wide", values: [1n << 90n] },
        specials: null,
      }),
      1n << 90n,
    ],
  ];
  const result = decodeResult({
    columns: scalars.map((c) => c[0]),
    frames: [{ sequence: 0, rowCount: 1, dictionaries: [], columns: scalars.map((c) => c[1]) }],
    summary: SUMMARY,
  });
  expect(result.tuples()).toEqual([scalars.map((c) => c[2])]);
  scalars.forEach((c, i) => expect(result.column(i).kind).toBe(c[0].type.kind));
  expect(decodeArrayValue(arrayType, array).lowerBounds).toEqual([-2, 4]);
  expect(result.rowCount).toBe(1);
  expect(result.affectedRows).toBeNull();
  const command = decodeResult({
    columns: [],
    frames: [],
    summary: { ...SUMMARY, command: "UPDATE", rowCount: 5, replayed: true },
  });
  expect(command).toMatchObject({ affectedRows: 5, rowCount: 0, replayed: true });
  expect(command.rows()).toEqual([]);
});
it("keeps duplicate and special identifier identity; row(i) decodes only that row", () => {
  const poison = {
    form: "flat" as const,
    length: 2,
    validity: null,
    data: {
      layout: "int64" as const,
      values: { repr: "safe" as const, values: Float64Array.of(1, NaN) },
    },
  };
  const result = decodeResult({
    columns: [
      meta("a.b", { kind: "text" }),
      meta("__proto__", { kind: "text" }),
      meta("a.b", { kind: "int64" }),
    ],
    frames: [
      {
        sequence: 0,
        rowCount: 2,
        dictionaries: [],
        columns: [text(["first", "second"]), text(["safe", "safe"]), poison],
      },
    ],
    summary: SUMMARY,
  });
  expect(result.row(0)).toEqual({ "a.b": 1n, ["__proto__"]: "safe" });
  expect(Object.getPrototypeOf(result.row(0))).toBe(Object.prototype);
  expect(() => result.row(1)).toThrow(RangeError);
  expect(() => result.row(-1)).toThrow(RangeError);
  expect(() => result.column("missing")).toThrow(RangeError);
});
it("refuses unsafe number rendering while decoding exact decimal specials and infinity", () => {
  const column = meta("n", { kind: "decimal", precision: null, scale: null });
  const result = decodeResult({
    columns: [column],
    frames: [
      {
        sequence: 0,
        rowCount: 4,
        dictionaries: [],
        columns: [
          {
            form: "flat",
            length: 4,
            validity: null,
            data: {
              layout: "decimal",
              scale: null,
              scales: Uint16Array.of(0, 0, 0, 2),
              coefficients: { repr: "wide", values: [0n, 0n, 0n, 150n] },
              specials: Uint8Array.of(1, 2, 3, 0),
            },
          },
        ],
      },
    ],
    summary: SUMMARY,
  });
  expect(result.values(0)).toEqual(["NaN", "Infinity", "-Infinity", "1.50"]);
  const unsafe = decodeResult({
    columns: [meta("count", { kind: "int64" }, "number")],
    frames: [
      {
        sequence: 0,
        rowCount: 1,
        dictionaries: [],
        columns: [
          {
            form: "flat",
            length: 1,
            validity: null,
            data: {
              layout: "int64",
              values: { repr: "int64", values: BigInt64Array.of(9007199254740993n) },
            },
          },
        ],
      },
    ],
    summary: SUMMARY,
  });
  expect(() => unsafe.rows()).toThrow(/::bigint/u);
});
it("keeps empty schemas and refuses internal locator metadata", () => {
  const rows = decodeResult({
    columns: [],
    frames: [{ sequence: 0, rowCount: 3, columns: [], dictionaries: [] }],
    summary: SUMMARY,
  });
  expect(rows.rowCount).toBe(3);
  expect(rows.rows()).toEqual([{}, {}, {}]);
  expect(rows.tuples()).toEqual([[], [], []]);
  expect(() =>
    decodeResult({
      columns: [meta("hidden", { kind: "locator", tableId: 1 })],
      frames: [],
      summary: SUMMARY,
    }),
  ).toThrow(/internal/u);
});
