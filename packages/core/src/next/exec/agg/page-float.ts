/** Ordered float8 SUM page transitions. No vectors, payload buffers or per-row calls.
 * The caller supplies the ordinary admitted accumulator arrays and a verified page span.
 * These loops preserve the existing finite-prefix/float8pl tail, including -0 and errors.
 */
import type { Validity } from "../../vector/vector.js";
import type { PageRows } from "../../page/consume.js";
import { NumericOutOfRangeError } from "../errors.js";

export function sumPlainF64Page(
  sums: Float64Array,
  has: Uint8Array,
  values: Float64Array,
  validity: Validity | null,
  rows: PageRows | null,
  from: number,
  to: number,
  firstRow: number,
): void {
  if (rows === null) {
    let row = from;
    if (validity === null) {
      let sum = sums[0] ?? 0;
      for (; row < to; row++) {
        const next = sum + (values[row] ?? 0);
        if (next - next !== 0) break;
        sum = next;
      }
      sums[0] = sum;
      if (row > from) has[0] = 1;
    }
    for (; row < to; row++) {
      if (validity !== null && ((validity[row >>> 5] ?? 0) & (1 << (row & 31))) === 0) continue;
      const old = sums[0] ?? 0,
        value = values[row] ?? 0,
        next = old + value;
      if (
        (next === Infinity || next === -Infinity) &&
        old !== Infinity &&
        old !== -Infinity &&
        value !== Infinity &&
        value !== -Infinity
      )
        throw new NumericOutOfRangeError("value out of range: overflow");
      sums[0] = next;
      has[0] = 1;
    }
    return;
  }
  let k = from;
  if (validity === null) {
    let sum = sums[0] ?? 0;
    for (; k < to; k++) {
      const row = (rows[k] ?? 0) - firstRow,
        next = sum + (values[row] ?? 0);
      if (next - next !== 0) break;
      sum = next;
    }
    sums[0] = sum;
    if (k > from) has[0] = 1;
  }
  for (; k < to; k++) {
    const row = (rows[k] ?? 0) - firstRow;
    if (validity !== null && ((validity[row >>> 5] ?? 0) & (1 << (row & 31))) === 0) continue;
    const old = sums[0] ?? 0,
      value = values[row] ?? 0,
      next = old + value;
    if (
      (next === Infinity || next === -Infinity) &&
      old !== Infinity &&
      old !== -Infinity &&
      value !== Infinity &&
      value !== -Infinity
    )
      throw new NumericOutOfRangeError("value out of range: overflow");
    sums[0] = next;
    has[0] = 1;
  }
}

/** Constant pages retain sequential float8pl; multiplying by count would reassociate it. */
export function sumConstantF64Page(
  sums: Float64Array,
  has: Uint8Array,
  value: number,
  validity: Validity | null,
  rows: PageRows | null,
  from: number,
  to: number,
  firstRow: number,
): void {
  for (let k = from; k < to; k++) {
    const row = rows === null ? k : (rows[k] ?? 0) - firstRow;
    if (validity !== null && ((validity[row >>> 5] ?? 0) & (1 << (row & 31))) === 0) continue;
    const old = sums[0] ?? 0,
      next = old + value;
    if (
      (next === Infinity || next === -Infinity) &&
      old !== Infinity &&
      old !== -Infinity &&
      value !== Infinity &&
      value !== -Infinity
    )
      throw new NumericOutOfRangeError("value out of range: overflow");
    sums[0] = next;
    has[0] = 1;
  }
}
