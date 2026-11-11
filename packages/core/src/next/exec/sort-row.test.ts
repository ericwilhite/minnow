/** The vector borrow contract: one row keeps native representations and never reads NULL data. */
import { expect, it } from "vitest";
import type { PhysicalType } from "../types.js";
import type { FlatVector, Vector, VectorData } from "../vector/vector.js";
import { setInt128 } from "../vector/int128.js";
import { sortRowView } from "./sort-row.js";
import { decodePage, PageWriter } from "./spill-pages.js";

const limbs = (values: bigint[]) => {
  const out = new Uint32Array(values.length * 4);
  values.forEach((value, at) => setInt128(out, at, value));
  return out;
};
const safe = (values: number[]) => ({ repr: "safe" as const, values: Float64Array.from(values) });
const wide = (values: bigint[]) => ({ repr: "int64" as const, values: BigInt64Array.from(values) });
const numeric: PhysicalType = { kind: "decimal", precision: null, scale: null };
const cases: Array<[string, PhysicalType, VectorData, VectorData]> = [
  [
    "bool",
    { kind: "bool" },
    { layout: "bool", values: new Uint8Array([0, 1]) },
    { layout: "bool", values: new Uint8Array([1]) },
  ],
  [
    "i32",
    { kind: "int32", bits: 32 },
    { layout: "i32", values: new Int32Array([7, -5]) },
    { layout: "i32", values: new Int32Array([-5]) },
  ],
  [
    "f32",
    { kind: "float32" },
    { layout: "f32", values: new Float32Array([1.25, -0]) },
    { layout: "f32", values: new Float32Array([-0]) },
  ],
  [
    "f64",
    { kind: "float64" },
    { layout: "f64", values: new Float64Array([NaN, -0]) },
    { layout: "f64", values: new Float64Array([-0]) },
  ],
  [
    "safe int64",
    { kind: "int64" },
    { layout: "int64", values: safe([9, -9007199254740991]) },
    { layout: "int64", values: safe([-9007199254740991]) },
  ],
  [
    "native int64",
    { kind: "int64" },
    { layout: "int64", values: wide([1n << 62n, -(1n << 62n)]) },
    { layout: "int64", values: wide([-(1n << 62n)]) },
  ],
  [
    "UTF8",
    { kind: "text" },
    {
      layout: "varlen",
      bytes: new Uint8Array([65, 66, 0xe6, 0xb1, 0x89]),
      offsets: new Int32Array([0, 2, 5]),
    },
    {
      layout: "varlen",
      bytes: new Uint8Array([65, 66, 0xe6, 0xb1, 0x89]),
      offsets: new Int32Array([2, 5]),
    },
  ],
  [
    "fixed16",
    { kind: "uuid" },
    { layout: "fixed16", bytes: Uint8Array.from({ length: 32 }, (_, i) => i) },
    { layout: "fixed16", bytes: Uint8Array.from({ length: 16 }, (_, i) => 16 + i) },
  ],
  [
    "safe interval",
    { kind: "interval" },
    {
      layout: "interval",
      months: new Int32Array([1, -2]),
      days: new Int32Array([3, -4]),
      micros: safe([5, -6]),
    },
    {
      layout: "interval",
      months: new Int32Array([-2]),
      days: new Int32Array([-4]),
      micros: safe([-6]),
    },
  ],
  [
    "wide interval",
    { kind: "interval" },
    {
      layout: "interval",
      months: new Int32Array([1, -2]),
      days: new Int32Array([3, -4]),
      micros: wide([1n << 62n, -(1n << 62n)]),
    },
    {
      layout: "interval",
      months: new Int32Array([-2]),
      days: new Int32Array([-4]),
      micros: wide([-(1n << 62n)]),
    },
  ],
  [
    "safe decimal",
    numeric,
    { layout: "decimal", scale: 2, scales: null, coefficients: safe([15, -35]), specials: null },
    { layout: "decimal", scale: 2, scales: null, coefficients: safe([-35]), specials: null },
  ],
  [
    "int64 decimal",
    numeric,
    {
      layout: "decimal",
      scale: null,
      scales: new Uint16Array([2, 7]),
      coefficients: wide([1n << 62n, -(1n << 62n)]),
      specials: new Uint8Array([0, 0]),
    },
    {
      layout: "decimal",
      scale: null,
      scales: new Uint16Array([7]),
      coefficients: wide([-(1n << 62n)]),
      specials: new Uint8Array([0]),
    },
  ],
  [
    "int128 decimal",
    numeric,
    {
      layout: "decimal",
      scale: null,
      scales: new Uint16Array([3, 9]),
      coefficients: { repr: "int128", limbs: limbs([1n << 100n, -(1n << 120n)]) },
      specials: new Uint8Array([0, 0]),
    },
    {
      layout: "decimal",
      scale: null,
      scales: new Uint16Array([9]),
      coefficients: { repr: "int128", limbs: limbs([-(1n << 120n)]) },
      specials: new Uint8Array([0]),
    },
  ],
  [
    "wide decimal",
    numeric,
    {
      layout: "decimal",
      scale: null,
      scales: new Uint16Array([4, 11]),
      coefficients: { repr: "wide", values: [1n << 200n, -(1n << 222n)] },
      specials: new Uint8Array([0, 0]),
    },
    {
      layout: "decimal",
      scale: null,
      scales: new Uint16Array([11]),
      coefficients: { repr: "wide", values: [-(1n << 222n)] },
      specials: new Uint8Array([0]),
    },
  ],
];
function buffers(value: unknown): ArrayBufferLike[] {
  if (ArrayBuffer.isView(value)) return [value.buffer];
  return value && typeof value === "object" ? Object.values(value).flatMap(buffers) : [];
}
for (const [name, type, data, expected] of cases)
  for (const form of ["flat", "constant", "dictionary"] as const)
    it(`borrows the exact ${name} row from ${form}`, () => {
      const original: FlatVector = { form: "flat", type, length: 2, validity: null, data };
      const column: Vector =
        form === "flat"
          ? original
          : form === "constant"
            ? {
                form,
                type,
                length: 8,
                value: { form: "flat", type, length: 1, validity: null, data: expected },
              }
            : {
                form,
                type,
                length: 8,
                codes: new Int32Array(8).fill(1),
                validity: null,
                dictionary: original,
                dictionaryId: 1,
              };
      const row = sortRowView(column, form === "flat" ? 1 : 7);
      expect(row.data).toEqual(expected);
      const borrowed = new Set(buffers(form === "constant" ? expected : data));
      for (const buffer of buffers(row.data)) expect(borrowed.has(buffer)).toBe(true);
      let allocated = 0;
      const charge = {
        allocate: (n: number) => {
          allocated += n;
          return true;
        },
        free: (n: number) => {
          allocated -= n;
        },
      };
      const writer = new PageWriter(charge, 256);
      const page = writer.encode({ queryId: 1, run: 0, partition: 0, sequence: 0 }, [row], 1);
      if (!page) throw new Error("unexpected refusal");
      const decoded = decodePage(page, [type], charge);
      if ("needed" in decoded) throw new Error("unexpected decode refusal");
      const canonical =
        expected.layout === "varlen"
          ? {
              layout: "varlen",
              bytes: expected.bytes.subarray(expected.offsets[0], expected.offsets[1]),
              offsets: new Int32Array([0, (expected.offsets[1] ?? 0) - (expected.offsets[0] ?? 0)]),
            }
          : expected;
      expect(decoded.columns[0]?.data).toEqual(canonical);
      charge.free(decoded.charged);
      writer.close();
      expect(allocated).toBe(0);
    });
for (const form of ["flat", "constant", "dictionary"] as const)
  it(`does not read unspecified NULL payload from ${form}`, () => {
    const type: PhysicalType = { kind: "text" };
    const invalid: FlatVector = {
      form: "flat",
      type,
      length: 1,
      validity: new Uint32Array(1),
      get data(): VectorData {
        throw new Error("read NULL payload");
      },
    };
    const column: Vector =
      form === "flat"
        ? invalid
        : form === "constant"
          ? { form, type, length: 8, value: invalid }
          : {
              form,
              type,
              length: 8,
              codes: new Int32Array(8).fill(99999),
              validity: new Uint32Array(1),
              dictionary: {
                form: "flat",
                type,
                length: 1,
                validity: null,
                get data(): VectorData {
                  throw new Error("read NULL dictionary");
                },
              },
              dictionaryId: 1,
            };
    const result = sortRowView(column, form === "flat" ? 0 : 7);
    expect(result.validity?.[0]).toBe(0);
    expect(result.data).toEqual({
      layout: "varlen",
      bytes: new Uint8Array(0),
      offsets: new Int32Array(2),
    });
  });
