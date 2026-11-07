/**
 * Column encoding (the log's insert columns): every slice of a memtable column survives
 * `encodeColumn` → `decodeColumn`, whatever its offset and length — word-aligned or not, a few rows
 * (the scratch-free paths) or many, with and without NULLs, fixed-width and varlen.
 */
import fc from "fast-check";
import { describe, expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import type { PhysicalType } from "../types.js";
import { ByteWindow } from "./window.js";
import { columnLength } from "./stream-body.js";
import { ByteWriter } from "./bytes.js";
import { decodeColumn, encodeColumn } from "./columns.js";
import { BIG, ints, texts, type Value } from "./test-support.js";

const SEEDS = seedsFor("next-write-columns", [0xc01, 77]);
const F64: PhysicalType = { kind: "float64" };

function valuesOf(vector: ReturnType<typeof ints>): Value[] {
  const out: Value[] = [];
  for (let k = 0; k < vector.length; k++) {
    if (vector.validity !== null && ((vector.validity[k >>> 5] ?? 0) & (1 << (k & 31))) === 0) {
      out.push(null);
      continue;
    }
    const data = vector.data;
    if (data.layout === "i32" || data.layout === "f64") out.push(data.values[k] ?? 0);
    else if (data.layout === "int64" && data.values.repr === "safe")
      out.push(data.values.values[k] ?? 0);
    else if (data.layout === "varlen")
      out.push(new TextDecoder().decode(data.bytes.subarray(data.offsets[k], data.offsets[k + 1])));
  }
  return out;
}

const column = fc.oneof(
  fc
    .array(fc.option(fc.integer({ min: -(2 ** 31), max: 2 ** 31 - 1 }), { nil: null, freq: 3 }), {
      minLength: 1,
      maxLength: 300,
    })
    .map((values) => ({ values, vector: ints(values) })),
  fc
    .array(fc.option(fc.integer({ min: -(2 ** 40), max: 2 ** 40 }), { nil: null, freq: 3 }), {
      minLength: 1,
      maxLength: 300,
    })
    .map((values) => ({ values, vector: ints(values, BIG) })),
  fc
    .array(fc.option(fc.double({ noNaN: true }), { nil: null, freq: 3 }), {
      minLength: 1,
      maxLength: 300,
    })
    .map((values) => {
      const vector = ints(values.map((value) => (value === null ? null : 0)));
      const f64 = {
        ...vector,
        type: F64,
        data: { layout: "f64" as const, values: Float64Array.from(values, (value) => value ?? 0) },
      };
      return {
        values: values.map((value) => (value === null ? null : Object.is(value, -0) ? 0 : value)),
        vector: f64,
      };
    }),
  fc
    .array(fc.option(fc.string({ maxLength: 12 }), { nil: null, freq: 3 }), {
      minLength: 1,
      maxLength: 300,
    })
    .map((values) => ({ values, vector: texts(values) })),
  // No NULLs at all: the vector has no validity.
  fc
    .array(fc.integer(), { minLength: 1, maxLength: 300 })
    .map((values) => ({ values, vector: ints(values) })),
);

describe("column encoding", () => {
  for (const seed of SEEDS)
    it(`round-trips any slice (seed ${String(seed)})`, () => {
      fc.assert(
        fc.property(
          column,
          fc.nat(),
          fc.nat(),
          fc.boolean(),
          ({ values, vector }, a, b, shared) => {
            const from = a % values.length;
            const count = b % (values.length - from + 1);
            // Encoding into a shared writer after other bytes, as the commit path does.
            const writer = new ByteWriter(16);
            if (shared) writer.u8(0xee);
            const start = writer.length;
            encodeColumn(vector, from, count, writer);
            const bytes = shared
              ? writer.bytes.slice(start, writer.length)
              : encodeColumn(vector, from, count);
            expect(columnLength(vector, from, count)).toBe(bytes.length);
            const windowed = new Uint8Array(bytes.length);
            const scratch = new Uint8Array(16);
            for (let at = 0; at < windowed.length; at += 31) {
              const window = new ByteWindow(windowed.subarray(at, at + 31), at, scratch);
              encodeColumn(vector, from, count, window);
              expect(window.length).toBe(bytes.length);
            }
            expect(Buffer.compare(windowed, bytes)).toBe(0);
            const decoded = decodeColumn(bytes, vector.type);
            const expected = values
              .slice(from, from + count)
              .map((value) => (typeof value === "number" && Object.is(value, -0) ? 0 : value));
            expect(
              valuesOf(decoded).map((value) =>
                typeof value === "number" && Object.is(value, -0) ? 0 : value,
              ),
            ).toEqual(expected);
          },
        ),
        { seed, numRuns: 400 },
      );
    });
});
