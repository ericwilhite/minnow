/**
 * Allocation-free 128-bit integer arithmetic for the decimal int64 and int128 tiers
 * (vector/vector.ts "Decimal coefficients have three tiers").
 *
 * A value is four little-endian uint32 limbs, two's complement, at `offset` (in values) of a
 * Uint32Array: the same layout as vector/int128.ts. Every operation works on plain numbers over
 * typed arrays, so no BigInt is created per value on any engine; int64-tier inputs are read
 * through a Uint32Array view of their BigInt64Array (two limbs, sign-extended).
 *
 * Operations report overflow past 128 bits by returning false; the caller then recomputes the
 * whole batch in the `wide` tier with one BigInt per value. A batch goes there when one of its
 * values (an input, an aligned operand or a result) is wider than 128 bits, an input is already
 * `wide`, or a product passes the display-scale cap; scale diversity alone never sends it there.
 */

const TWO32 = 4294967296;

if (new Uint8Array(new Uint32Array([1]).buffer)[0] !== 1)
  throw new Error("The executor's limb arithmetic assumes a little-endian platform");

/** Scratch limbs for intermediate values (offsets 0 … 7 are free for callers). */
export const SCRATCH = new Uint32Array(4 * 16);

/** A Uint32Array view over a BigInt64Array (two limbs per value, low word first). */
export function int64Words(values: BigInt64Array): Uint32Array {
  return new Uint32Array(values.buffer, values.byteOffset, values.length * 2);
}

/** t[to] = v for a safe integer v. */
export function setNumber(t: Uint32Array, to: number, v: number): void {
  const o = to * 4;
  const negative = v < 0;
  const a = negative ? -v : v;
  t[o] = a >>> 0;
  t[o + 1] = Math.floor(a / TWO32) >>> 0;
  t[o + 2] = 0;
  t[o + 3] = 0;
  if (negative) negate(t, to);
}

/** t[to] = the int64 at `index` of `words` (int64Words), sign-extended. */
export function setInt64(t: Uint32Array, to: number, words: Uint32Array, index: number): void {
  const o = to * 4;
  const hi = words[index * 2 + 1] ?? 0;
  t[o] = words[index * 2] ?? 0;
  t[o + 1] = hi;
  const extension = hi >>> 31 === 1 ? 0xffff_ffff : 0;
  t[o + 2] = extension;
  t[o + 3] = extension;
}

export function copy(t: Uint32Array, to: number, s: Uint32Array, from: number): void {
  const o = to * 4;
  const f = from * 4;
  t[o] = s[f] ?? 0;
  t[o + 1] = s[f + 1] ?? 0;
  t[o + 2] = s[f + 2] ?? 0;
  t[o + 3] = s[f + 3] ?? 0;
}

export function isNegative(t: Uint32Array, at: number): boolean {
  return (t[at * 4 + 3] ?? 0) >>> 31 === 1;
}

export function isZero(t: Uint32Array, at: number): boolean {
  const o = at * 4;
  return ((t[o] ?? 0) | (t[o + 1] ?? 0) | (t[o + 2] ?? 0) | (t[o + 3] ?? 0)) === 0;
}

/** −1, 0 or 1. */
export function sign(t: Uint32Array, at: number): number {
  return isNegative(t, at) ? -1 : isZero(t, at) ? 0 : 1;
}

/** In place two's complement negation (−MIN wraps to MIN; callers check with isNegative). */
export function negate(t: Uint32Array, at: number): void {
  const o = at * 4;
  let carry = 1;
  for (let k = 0; k < 4; k++) {
    const v = (~(t[o + k] ?? 0) >>> 0) + carry;
    t[o + k] = v >>> 0;
    carry = v > 0xffff_ffff ? 1 : 0;
  }
}

/** t[to] = a + b (or a − b); false on overflow past 128 bits. `to` may alias a or b. */
export function addTo(
  t: Uint32Array,
  to: number,
  a: Uint32Array,
  ai: number,
  b: Uint32Array,
  bi: number,
  subtract: boolean,
): boolean {
  const ao = ai * 4;
  const bo = bi * 4;
  const o = to * 4;
  const signA = (a[ao + 3] ?? 0) >>> 31;
  const signB = ((b[bo + 3] ?? 0) >>> 31) ^ (subtract ? 1 : 0);
  let carry = subtract ? 1 : 0;
  for (let k = 0; k < 4; k++) {
    const y = subtract ? ~(b[bo + k] ?? 0) >>> 0 : (b[bo + k] ?? 0);
    const v = (a[ao + k] ?? 0) + y + carry;
    t[o + k] = v >>> 0;
    carry = v > 0xffff_ffff ? 1 : 0;
  }
  const signR = (t[o + 3] ?? 0) >>> 31;
  // −b of b = MIN cannot be represented: its "negation" flips no sign, so check it directly.
  if (
    subtract &&
    (b[bo + 3] ?? 0) === 0x8000_0000 &&
    ((b[bo] ?? 0) | (b[bo + 1] ?? 0) | (b[bo + 2] ?? 0)) === 0
  )
    return signA === 1;
  return !(signA === signB && signR !== signA);
}

/** Multiplies |t[at]| in place by m (1 ≤ m ≤ 10^4); false when the magnitude reaches 2^127. */
function multiplyMagnitude(t: Uint32Array, o: number, m: number): boolean {
  let carry = 0;
  for (let k = 0; k < 4; k++) {
    const v = (t[o + k] ?? 0) * m + carry; // < 2^32 · 10^4 + 10^4 < 2^53
    t[o + k] = v >>> 0;
    carry = Math.floor(v / TWO32);
  }
  return carry === 0 && (t[o + 3] ?? 0) >>> 31 === 0;
}

/** t[at] ·= 10^e in place (e ≥ 0); false on overflow past 128 bits. */
export function scaleUp(t: Uint32Array, at: number, e: number): boolean {
  if (e === 0) return true;
  const negative = isNegative(t, at);
  if (negative) negate(t, at);
  if (isNegative(t, at)) return false; // MIN
  const o = at * 4;
  let rest = e;
  while (rest > 0) {
    const step = Math.min(rest, 4);
    if (!multiplyMagnitude(t, o, 10 ** step)) return false;
    rest -= step;
  }
  if (negative) negate(t, at);
  return true;
}

const HALVES_A = new Float64Array(8);
const HALVES_B = new Float64Array(8);
const COLUMNS = new Float64Array(17);

/** t[to] = a · b; false on overflow past 128 bits. `to` may alias a or b. */
export function multiplyTo(
  t: Uint32Array,
  to: number,
  a: Uint32Array,
  ai: number,
  b: Uint32Array,
  bi: number,
): boolean {
  // Magnitudes in 16-bit halves: every partial product is < 2^32, every column sum < 2^36.
  const negA = (a[ai * 4 + 3] ?? 0) >>> 31 === 1;
  const negB = (b[bi * 4 + 3] ?? 0) >>> 31 === 1;
  const sa = SCRATCH;
  copy(sa, 14, a, ai);
  copy(sa, 15, b, bi);
  if (negA) negate(sa, 14);
  if (negB) negate(sa, 15);
  for (let k = 0; k < 4; k++) {
    const x = sa[56 + k] ?? 0;
    const y = sa[60 + k] ?? 0;
    HALVES_A[2 * k] = x & 0xffff;
    HALVES_A[2 * k + 1] = x >>> 16;
    HALVES_B[2 * k] = y & 0xffff;
    HALVES_B[2 * k + 1] = y >>> 16;
  }
  COLUMNS.fill(0);
  for (let i = 0; i < 8; i++) {
    const x = HALVES_A[i] ?? 0;
    if (x === 0) continue;
    for (let j = 0; j < 8; j++) COLUMNS[i + j] = (COLUMNS[i + j] ?? 0) + x * (HALVES_B[j] ?? 0);
  }
  // Carry columns into 16-bit digits; anything at or above digit 8 is overflow.
  let carry = 0;
  const o = to * 4;
  for (let c = 0; c < 16; c++) {
    const v = (COLUMNS[c] ?? 0) + carry;
    const digit = v % 65536;
    carry = Math.floor(v / 65536);
    if (c >= 8) {
      if (digit !== 0) return false;
      continue;
    }
    if ((c & 1) === 0) t[o + (c >> 1)] = digit;
    else t[o + (c >> 1)] = ((t[o + (c >> 1)] ?? 0) | (digit << 16)) >>> 0;
  }
  if (carry !== 0 || (t[o + 3] ?? 0) >>> 31 === 1) return false;
  if (negA !== negB) negate(t, to);
  return true;
}

/** Whether t[at] fits int64 (its high limbs are the sign extension of limb 1). */
export function fitsInt64(t: Uint32Array, at: number): boolean {
  const o = at * 4;
  const extension = (t[o + 1] ?? 0) >>> 31 === 1 ? 0xffff_ffff : 0;
  return (t[o + 2] ?? 0) === extension && (t[o + 3] ?? 0) === extension;
}

/** Writes t[at] (which fits int64) into `words` (int64Words) at `index`. */
export function storeInt64(t: Uint32Array, at: number, words: Uint32Array, index: number): void {
  words[index * 2] = t[at * 4] ?? 0;
  words[index * 2 + 1] = t[at * 4 + 1] ?? 0;
}

/** −1, 0, 1 for a − b. */
export function compare(a: Uint32Array, ai: number, b: Uint32Array, bi: number): number {
  const ao = ai * 4;
  const bo = bi * 4;
  const ha = (a[ao + 3] ?? 0) | 0;
  const hb = (b[bo + 3] ?? 0) | 0;
  if (ha !== hb) return ha < hb ? -1 : 1;
  for (let k = 2; k >= 0; k--) {
    const x = a[ao + k] ?? 0;
    const y = b[bo + k] ?? 0;
    if (x !== y) return x < y ? -1 : 1;
  }
  return 0;
}

/**
 * t[at] = v when v fits 128 bits (two's complement); false otherwise. For values that are already
 * BigInts (a `wide` input), so it adds no BigInt of its own beyond the masking.
 */
export function setBigInt(t: Uint32Array, at: number, v: bigint): boolean {
  if (v < -(1n << 127n) || v >= 1n << 127n) return false;
  const u = BigInt.asUintN(128, v);
  const o = at * 4;
  t[o] = Number(u & 0xffff_ffffn);
  t[o + 1] = Number((u >> 32n) & 0xffff_ffffn);
  t[o + 2] = Number((u >> 64n) & 0xffff_ffffn);
  t[o + 3] = Number(u >> 96n);
  return true;
}

/** The value as a bigint (tests, and the rare wide fallback). */
export function toBigInt(t: Uint32Array, at: number): bigint {
  const o = at * 4;
  const unsigned =
    BigInt(t[o] ?? 0) |
    (BigInt(t[o + 1] ?? 0) << 32n) |
    (BigInt(t[o + 2] ?? 0) << 64n) |
    (BigInt(t[o + 3] ?? 0) << 96n);
  return BigInt.asIntN(128, unsigned);
}

/** t[at] as the nearest double (exact while |value| ≤ 2^53). */
export function toDouble(t: Uint32Array, at: number): number {
  const o = at * 4;
  const negative = isNegative(t, at);
  if (!negative)
    return (
      (t[o + 3] ?? 0) * 2 ** 96 + (t[o + 2] ?? 0) * 2 ** 64 + (t[o + 1] ?? 0) * TWO32 + (t[o] ?? 0)
    );
  copy(SCRATCH, 13, t, at);
  negate(SCRATCH, 13);
  return -toDouble(SCRATCH, 13);
}

/** 10^0 … 10^38 as limbs (every power of ten that fits 127 bits). */
export const POWERS_OF_TEN: Uint32Array = (() => {
  const powers = new Uint32Array(4 * 39);
  setNumber(powers, 0, 1);
  for (let k = 1; k < 39; k++) {
    copy(powers, k, powers, k - 1);
    scaleUp(powers, k, 1);
  }
  return powers;
})();

/** Decimal digits of |t[at]| (1 for zero); t[at] must not be MIN. */
export function digitCount(t: Uint32Array, at: number): number {
  copy(SCRATCH, 12, t, at);
  if (isNegative(SCRATCH, 12)) negate(SCRATCH, 12);
  let digits = 1;
  while (digits < 39 && compare(SCRATCH, 12, POWERS_OF_TEN, digits) >= 0) digits += 1;
  return digits;
}

/**
 * q = n / d, rounded half away from zero (`round`) or truncated toward zero, written to t[to];
 * false when the quotient may not be exact here (|q| ≥ 2^50: the caller uses BigInt). d ≠ 0;
 * n and d must not be MIN.
 *
 * The quotient is first estimated in doubles (relative error < 2^−51, so off by at most one
 * below 2^50), then corrected exactly with r = n − q·d in limbs until 0 ≤ |r| < |d| with r
 * carrying n's sign.
 */
export function divideTo(
  t: Uint32Array,
  to: number,
  n: Uint32Array,
  ni: number,
  d: Uint32Array,
  di: number,
  round: boolean,
): boolean {
  const estimate = Math.trunc(toDouble(n, ni) / toDouble(d, di));
  if (!(Math.abs(estimate) < 2 ** 50)) return false;
  const s = SCRATCH;
  // |d| < 2^126 keeps 2r (for rounding) inside 127 bits.
  if (Math.abs(toDouble(d, di)) >= 2 ** 125) return false;
  // s[8] = |n|, s[9] = |d|, s[10] = q, s[11] = r = |n| − q·|d|.
  copy(s, 8, n, ni);
  copy(s, 9, d, di);
  const negN = isNegative(s, 8);
  const negD = isNegative(s, 9);
  if (negN) negate(s, 8);
  if (negD) negate(s, 9);
  let q = Math.abs(estimate);
  for (let attempt = 0; attempt < 4; attempt++) {
    setNumber(s, 10, q);
    if (!multiplyTo(s, 11, s, 10, s, 9)) return false;
    if (!addTo(s, 11, s, 8, s, 11, true)) return false;
    if (isNegative(s, 11)) {
      q -= 1;
      continue;
    }
    if (compare(s, 11, s, 9) >= 0) {
      q += 1;
      continue;
    }
    // 0 ≤ r < |d|: q is the truncated quotient of the magnitudes.
    if (round) {
      addTo(s, 11, s, 11, s, 11, false); // 2r (r < |d| < 2^127, so no overflow past 128 bits)
      if (compare(s, 11, s, 9) >= 0) q += 1;
    }
    setNumber(t, to, q);
    if (negN !== negD) negate(t, to);
    return true;
  }
  return false;
}

/**
 * PostgreSQL's (weight, first digit) of c·10^−scale in base 10000 (numeric.c's NumericVar view):
 * writes them to `out` [weight, firstDigit]; zero is (0, 0).
 */
export function leadingDigitOf(t: Uint32Array, at: number, scale: number, out: Int32Array): void {
  if (isZero(t, at)) {
    out[0] = 0;
    out[1] = 0;
    return;
  }
  const digits = digitCount(t, at);
  const exponent = digits - 1 - scale; // |v| in [10^e, 10^(e+1))
  const weight = Math.floor(exponent / 4);
  const shift = scale + 4 * weight;
  copy(SCRATCH, 12, t, at);
  if (isNegative(SCRATCH, 12)) negate(SCRATCH, 12);
  let first: number;
  if (shift <= 0)
    first = toDouble(SCRATCH, 12) * 10 ** -shift; // small: exact
  else {
    first = Math.floor(toDouble(SCRATCH, 12) / 10 ** shift);
    // Exact correction: first · 10^shift ≤ |c| < (first + 1) · 10^shift.
    setNumber(SCRATCH, 11, first);
    multiplyTo(SCRATCH, 11, SCRATCH, 11, POWERS_OF_TEN, shift);
    if (compare(SCRATCH, 11, SCRATCH, 12) > 0) first -= 1;
    else {
      setNumber(SCRATCH, 11, first + 1);
      multiplyTo(SCRATCH, 11, SCRATCH, 11, POWERS_OF_TEN, shift);
      if (compare(SCRATCH, 11, SCRATCH, 12) <= 0) first += 1;
    }
  }
  out[0] = weight;
  out[1] = first;
}
