/**
 * NUMERIC division and modulo with PostgreSQL's rules (src/backend/utils/adt/numeric.c:
 * `select_div_scale`, `div_var` with rounding, `mod_var`, and the special-value tables of
 * `numeric_div` / `numeric_mod`, PostgreSQL 14+).
 *
 * - Quotient display scale: PostgreSQL works in base-10000 digits. With w the weight (position)
 *   and d the value of each operand's leading base-10000 digit, qweight = w1 − w2, minus one when
 *   d1 ≤ d2; rscale = max(16 − 4·qweight, dscale1, dscale2, 0), at most 1000. The exact quotient is
 *   rounded half away from zero to rscale digits. So 1/3 = 0.33333333333333333333 (20 digits) and
 *   10/2 = 5.0000000000000000.
 * - Modulo: x − trunc(x / y)·y, display scale max(dscale1, dscale2); the dividend's sign.
 * - Division or modulo by zero is 22012 (also ±Infinity / 0 and ±Infinity % 0).
 * - Specials: NaN absorbs; ∞ / ∞ is NaN; ∞ / finite is ±∞; finite / ∞ is 0; ∞ % y is NaN;
 *   x % ∞ is x.
 */
import { DivisionByZeroError } from "../errors.js";

export const NUMERIC_MIN_SIG_DIGITS = 16;
export const NUMERIC_MAX_DISPLAY_SCALE = 1000;

const NAN = 1;
const POS = 2;
const NEG = 3;

function abs(value: bigint): bigint {
  return value < 0n ? -value : value;
}

/** PostgreSQL's (weight, first digit) of c·10^−s in base 10000; zero is (0, 0). */
export function leadingDigit(c: bigint, s: number): readonly [number, number] {
  if (c === 0n) return [0, 0];
  const a = abs(c);
  const exponent = a.toString().length - 1 - s; // |v| in [10^e, 10^(e+1))
  const weight = Math.floor(exponent / 4);
  const shift = s + 4 * weight;
  const first = shift >= 0 ? a / 10n ** BigInt(shift) : a * 10n ** BigInt(-shift);
  return [weight, Number(first)];
}

/** PostgreSQL's select_div_scale. */
export function divisionScale(c1: bigint, s1: number, c2: bigint, s2: number): number {
  const [w1, d1] = leadingDigit(c1, s1);
  const [w2, d2] = leadingDigit(c2, s2);
  let qweight = w1 - w2;
  if (d1 <= d2) qweight -= 1;
  const rscale = Math.max(NUMERIC_MIN_SIG_DIGITS - qweight * 4, s1, s2, 0);
  return Math.min(rscale, NUMERIC_MAX_DISPLAY_SCALE);
}

/** (c1·10^−s1) / (c2·10^−s2) rounded half away from zero to `rscale`; c2 ≠ 0. */
export function divideRounded(
  c1: bigint,
  s1: number,
  c2: bigint,
  s2: number,
  rscale: number,
): bigint {
  const e = rscale - s1 + s2;
  const numerator = e >= 0 ? c1 * 10n ** BigInt(e) : c1;
  const denominator = e < 0 ? c2 * 10n ** BigInt(-e) : c2;
  const q = numerator / denominator;
  const r = numerator - q * denominator;
  if (2n * abs(r) >= abs(denominator)) return numerator < 0n === denominator < 0n ? q + 1n : q - 1n;
  return q;
}

export interface DecimalResult {
  /** 0 finite, else NaN / +Infinity / −Infinity. */
  readonly special: number;
  readonly coefficient: bigint;
  readonly scale: number;
}

const result = (special: number, coefficient = 0n, scale = 0): DecimalResult => ({
  special,
  coefficient,
  scale,
});

/** `x / y` over (special, coefficient, scale) triples. */
export function numericDivide(
  xs: number,
  c1: bigint,
  s1: number,
  ys: number,
  c2: bigint,
  s2: number,
): DecimalResult {
  if (xs === NAN || ys === NAN) return result(NAN);
  if (xs !== 0) {
    if (ys !== 0) return result(NAN);
    if (c2 === 0n) throw new DivisionByZeroError();
    return result((xs === POS) === c2 > 0n ? POS : NEG);
  }
  if (ys !== 0) return result(0); // finite / ±∞
  if (c2 === 0n) throw new DivisionByZeroError();
  const rscale = divisionScale(c1, s1, c2, s2);
  return result(0, divideRounded(c1, s1, c2, s2, rscale), rscale);
}

/** `x % y` over (special, coefficient, scale) triples. */
export function numericModulo(
  xs: number,
  c1: bigint,
  s1: number,
  ys: number,
  c2: bigint,
  s2: number,
): DecimalResult {
  if (xs === NAN || ys === NAN) return result(NAN);
  if (xs !== 0) {
    if (ys === 0 && c2 === 0n) throw new DivisionByZeroError();
    return result(NAN);
  }
  if (ys !== 0) return result(0, c1, s1); // x % ±∞ = x
  if (c2 === 0n) throw new DivisionByZeroError();
  const scale = Math.max(s1, s2);
  const a1 = c1 * 10n ** BigInt(scale - s1);
  const a2 = c2 * 10n ** BigInt(scale - s2);
  return result(0, a1 - (a1 / a2) * a2, scale);
}
