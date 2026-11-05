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
 * batch in the `wide` (bigint) tier, which only values genuinely wider than 128 bits reach.
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
