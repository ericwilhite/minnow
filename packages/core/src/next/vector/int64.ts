/**
 * Exact int64 helpers: the proof that lets an int64-family vector use the Float64Array fast
 * path, and the conversions between the two representations.
 *
 * The rule (vector.ts): `repr: "safe"` is allowed only when every valid slot holds an integer
 * in ±MAX_SAFE. Page statistics prove it for stored data (`provenSafe(min, max)`); a kernel proves
 * it for its output by checking (`safeAdd`, `safeMultiply`), or by a bound computed up front
 * (`sumStaysSafe`), and otherwise switches to BigInt64Array. A safe slot never holds −0.
 */
import { INT64_MAX, INT64_MIN } from "../types.js";
import { isValid } from "./validity.js";
import type { Int64Values, Validity } from "./vector.js";

export const MAX_SAFE = Number.MAX_SAFE_INTEGER;
const MAX_SAFE_BIG = BigInt(MAX_SAFE);

/** Page min/max (as stored, exact) prove a page's values fit the safe representation. */
export function provenSafe(min: bigint, max: bigint): boolean {
  return min >= -MAX_SAFE_BIG && max <= MAX_SAFE_BIG;
}

/**
 * Whether summing `count` values each of magnitude ≤ `maxMagnitude` onto an accumulator of
 * magnitude ≤ `accumulator` stays exact in f64. Lets SUM skip per-value checks for a batch.
 */
export function sumStaysSafe(accumulator: number, maxMagnitude: number, count: number): boolean {
  return Math.abs(accumulator) + maxMagnitude * count <= MAX_SAFE;
}

/** a + b for safe integers, or NaN when the exact result is not safe (caller widens). */
export function safeAdd(a: number, b: number): number {
  const sum = a + b;
  return sum >= -MAX_SAFE && sum <= MAX_SAFE ? sum : Number.NaN;
}

/** a × b for safe integers, or NaN when the exact result is not safe (caller widens). */
export function safeMultiply(a: number, b: number): number {
  const product = a * b;
  // `+ 0` turns −0 into 0: the safe representation never holds −0 (int64 has no signed zero).
  return product >= -MAX_SAFE && product <= MAX_SAFE ? product + 0 : Number.NaN;
}

export function inInt64Range(value: bigint): boolean {
  return value >= INT64_MIN && value <= INT64_MAX;
}

/** One slot as an exact bigint, whatever the representation. */
export function int64At(values: Int64Values, index: number): bigint {
  return values.repr === "safe" ? BigInt(values.values[index] ?? 0) : (values.values[index] ?? 0n);
}

/**
 * Widens slots 0 … length into `target` (BigInt64Array). Invalid slots (garbage under a null,
 * NaN included) become 0n and never throw.
 */
export function widenToInt64(
  source: Float64Array,
  length: number,
  validity: Validity | null,
  target: BigInt64Array,
): void {
  for (let index = 0; index < length; index++)
    target[index] = isValid(validity, index) ? BigInt(source[index] ?? 0) : 0n;
}

/**
 * Narrows slots 0 … length into `target` if every valid slot is safe; returns false (with
 * `target` partially written) otherwise. Invalid slots become 0.
 */
export function narrowToSafe(
  source: BigInt64Array,
  length: number,
  validity: Validity | null,
  target: Float64Array,
): boolean {
  for (let index = 0; index < length; index++) {
    if (!isValid(validity, index)) {
      target[index] = 0;
      continue;
    }
    const value = source[index] ?? 0n;
    if (value < -MAX_SAFE_BIG || value > MAX_SAFE_BIG) return false;
    target[index] = Number(value);
  }
  return true;
}
