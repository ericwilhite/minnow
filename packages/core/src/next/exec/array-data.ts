/** Bounded views of already-validated native array values; never decode a growing
 * array into a JS element list. The bytes remain the array-value.ts format. */
import { DURABLE_FORMATS, requireHeader } from "../durable-format.js";
import {
  arrayElementTag,
  encodeArrayValue,
  MalformedArrayError,
  MAX_ARRAY_DIMENSIONS,
  type ArrayElement,
} from "../vector/array-value.js";
import type { PhysicalType } from "../types.js";
import type { Vector } from "../vector/vector.js";
import type { ByteCharge } from "../vector/varlen.js";
import { bigintBytes } from "../memory/governor.js";
import { ExecutionError } from "./errors.js";
import { int64At } from "../vector/int64.js";
import { coefficientAt, scaleAt, specialAt } from "./kernels/numeric.js";
import { isValidSlot, slotOf } from "./kernels/vectors.js";
export interface NativeArray {
  readonly dimensions: readonly number[];
  readonly lowerBounds: readonly number[];
  readonly count: number;
  readonly nulls: Uint8Array | null;
  readonly payload: Uint8Array;
}
export function valueBytes(value: Vector, row: number): Uint8Array {
  const [flat, slot] = slotOf(value, row),
    d = flat.data;
  if (d.layout !== "varlen") throw new TypeError("Expected variable bytes");
  return d.bytes.subarray(d.offsets[slot] ?? 0, d.offsets[slot + 1] ?? 0);
}
export function nativeArray(element: PhysicalType, bytes: Uint8Array): NativeArray {
  requireHeader("arrayValue", bytes, MalformedArrayError);
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.length);
  if (bytes.length < 4 || bytes[1] !== arrayElementTag(element))
    throw new MalformedArrayError("Bad array element tag");
  const dims = bytes[2] ?? 0,
    flags = bytes[3] ?? 0;
  if (dims > MAX_ARRAY_DIMENSIONS || flags > 1 || bytes.length < 4 + 8 * dims)
    throw new MalformedArrayError("Bad array header");
  const dimensions: number[] = [],
    lowerBounds: number[] = [];
  let count = dims ? 1 : 0;
  for (let i = 0; i < dims; i++) {
    const n = view.getInt32(4 + 8 * i, true);
    if (n <= 0) throw new MalformedArrayError("Bad array dimension");
    dimensions.push(n);
    lowerBounds.push(view.getInt32(8 + 8 * i, true));
    count *= n;
  }
  const start = 4 + 8 * dims,
    bitmap = flags ? Math.ceil(count / 8) : 0;
  if (!Number.isSafeInteger(count) || count > 0x7fffffff || start + bitmap > bytes.length)
    throw new MalformedArrayError("Bad array cardinality");
  return {
    dimensions,
    lowerBounds,
    count,
    nulls: flags ? bytes.subarray(start, start + bitmap) : null,
    payload: bytes.subarray(start + bitmap),
  };
}
/** PostgreSQL ArrayCheckBounds checks the one-past-end bound when constructing
 * an array. Identity concatenation returns its input without that construction. */
export function arrayFunctionBounds(dimensions: readonly number[], lower: readonly number[]): void {
  let count = dimensions.length ? 1 : 0;
  for (const [i, n] of dimensions.entries()) {
    count *= n;
    if (!Number.isSafeInteger(count) || count > 0x7fffffff || (lower[i] ?? 1) + n > 0x7fffffff)
      throw new ExecutionError("array bound is too large", "MINNOW_ARRAY_BOUNDS", "54000");
  }
}
export function arrayHeader(
  element: PhysicalType,
  dimensions: readonly number[],
  lowerBounds: readonly number[],
  nulls: boolean,
): Uint8Array {
  const bytes = new Uint8Array(4 + 8 * dimensions.length),
    view = new DataView(bytes.buffer);
  bytes[0] = DURABLE_FORMATS.arrayValue.version;
  bytes[1] = arrayElementTag(element);
  bytes[2] = dimensions.length;
  bytes[3] = Number(nulls);
  for (let i = 0; i < dimensions.length; i++) {
    view.setInt32(4 + 8 * i, dimensions[i] ?? 0, true);
    view.setInt32(8 + 8 * i, lowerBounds[i] ?? 1, true);
  }
  return bytes;
}
/** Hex spelling, sign normalization, codec writer and final bytes coexist for one
 * coefficient. Twelve times its native size covers those linear copies, including
 * validation spelling; fixed scalars and borrowed strings need only bounded headers. */
export function arrayElementScratch(value: Vector, row: number): number {
  const [flat, slot] = slotOf(value, row),
    d = flat.data;
  return d.layout === "decimal" && d.coefficients.repr === "wide"
    ? 4096 + 12 * bigintBytes(coefficientAt(d, slot))
    : 2048;
}
/** One scalar's encoding only. Variable data is borrowed, not copied. Fixed/numeric
 * codecs retain their existing exact representation and round-trip checks; their
 * bounded temporary writer is prepaid before constructing it. */
export class ArrayElementBytes {
  readonly parts: readonly Uint8Array[];
  readonly length: number;
  readonly #charged: number;
  #closed = false;
  constructor(
    value: Vector,
    row: number,
    readonly charge: ByteCharge,
    prepaid = false,
  ) {
    const [flat, slot] = slotOf(value, row),
      d = flat.data;
    this.#charged = arrayElementScratch(value, row);
    if (!prepaid && !charge.allocate(this.#charged))
      throw new RangeError("Array element scratch was not admitted");
    try {
      if (!isValidSlot(value, row)) {
        this.parts = [];
        this.length = 0;
        return;
      }
      if (d.layout === "varlen") {
        const payload = valueBytes(value, row),
          header = new Uint8Array(4);
        new DataView(header.buffer).setUint32(0, payload.length, true);
        this.parts = [header, payload];
        this.length = 4 + payload.length;
        return;
      }
      let item: ArrayElement;
      switch (d.layout) {
        case "bool":
          item = d.values[slot] !== 0;
          break;
        case "i32":
        case "f32":
        case "f64":
          item = d.values[slot] ?? 0;
          break;
        case "int64":
          item = int64At(d.values, slot);
          break;
        case "fixed16":
          item = d.bytes.subarray(16 * slot, 16 * (slot + 1));
          break;
        case "interval":
          item = {
            months: d.months[slot] ?? 0,
            days: d.days[slot] ?? 0,
            micros: int64At(d.micros, slot),
          };
          break;
        case "decimal": {
          const s = specialAt(d, slot);
          item = {
            coefficient: s ? 0n : coefficientAt(d, slot),
            scale: scaleAt(d, slot),
            ...(s
              ? {
                  special:
                    s === 1
                      ? ("NaN" as const)
                      : s === 2
                        ? ("Infinity" as const)
                        : ("-Infinity" as const),
                }
              : {}),
          };
          break;
        }
      }
      const encoded = encodeArrayValue(value.type, {
        dimensions: [1],
        lowerBounds: [1],
        elements: [item],
      });
      this.parts = [encoded.subarray(12)];
      this.length = encoded.length - 12;
    } catch (error) {
      charge.free(this.#charged);
      throw error;
    }
  }
  close(): void {
    if (!this.#closed) {
      this.#closed = true;
      this.charge.free(this.#charged);
    }
  }
}
