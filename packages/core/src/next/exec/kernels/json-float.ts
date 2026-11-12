/** PostgreSQL's shortest float output, independently derived from exact rounding intervals.
 * PG excludes BOTH binary midpoints (STRICTLY_SHORTEST=0 in common/ryu_common.h), even
 * when a reader using ties-to-even would accept one. Within that open interval choose the
 * shortest decimal, nearest to the exact binary value, with decimal ties to even.
 *
 * No Ryu tables/code are copied here. The largest binary64 operand is < 1,200 bits;
 * the synchronous temporaries fit JsonNode's prepaid 4 KiB cursor/scratch allowance.
 */
const bits = new DataView(new ArrayBuffer(8));

function notation(coefficient: bigint, power: number, single: boolean): string {
  while (coefficient % 10n === 0n) {
    coefficient /= 10n;
    power++;
  }
  const digits = coefficient.toString();
  const exponent = digits.length - 1 + power;
  if (exponent < -4 || exponent >= (single ? 6 : 15)) {
    const fraction = digits.length > 1 ? "." + digits.slice(1) : "";
    return (
      digits.charAt(0) +
      fraction +
      "e" +
      (exponent < 0 ? "-" : "+") +
      String(Math.abs(exponent)).padStart(2, "0")
    );
  }
  const point = digits.length + power;
  return point <= 0
    ? "0." + "0".repeat(-point) + digits
    : point >= digits.length
      ? digits + "0".repeat(point - digits.length)
      : digits.slice(0, point) + "." + digits.slice(point);
}

function exactFloatText(value: number, single: boolean): string {
  if (Object.is(value, -0)) return "-0";
  if (value === 0 || !Number.isFinite(value)) return String(value);
  const negative = value < 0;
  const magnitude = Math.abs(value);
  let fraction: bigint, exponent: number, width: number, bias: number;
  if (single) {
    bits.setFloat32(0, magnitude);
    const word = bits.getUint32(0);
    fraction = BigInt(word & 0x7fffff);
    exponent = word >>> 23;
    width = 23;
    bias = 127;
  } else {
    bits.setFloat64(0, magnitude);
    const word = bits.getBigUint64(0);
    fraction = word & 0xfffffffffffffn;
    exponent = Number(word >> 52n);
    width = 52;
    bias = 1023;
  }
  const mantissa = fraction + (exponent === 0 ? 0n : 1n << BigInt(width));
  const binaryPower = Math.max(1, exponent) - bias - width - 2;
  // A power of two has a half-sized predecessor gap, except at the normal/subnormal seam.
  const lower = 4n * mantissa - (fraction !== 0n || exponent <= 1 ? 2n : 1n);
  const center = 4n * mantissa;
  const upper = center + 2n;
  const numeratorScale = binaryPower >= 0 ? 1n << BigInt(binaryPower) : 1n;
  const denominator = binaryPower < 0 ? 1n << BigInt(-binaryPower) : 1n;
  const exact = center * numeratorScale;
  let decimalExponent = Math.floor(Math.log10(magnitude));
  const comparePower = (power: number): bigint =>
    power >= 0
      ? exact - denominator * 10n ** BigInt(power)
      : exact * 10n ** BigInt(-power) - denominator;
  // log10 is only an estimate near decimal powers. Correct it using the exact ratio.
  while (comparePower(decimalExponent) < 0n) decimalExponent--;
  while (comparePower(decimalExponent + 1) >= 0n) decimalExponent++;
  for (let digits = 1; digits <= (single ? 9 : 17); digits++) {
    const power = decimalExponent - digits + 1;
    const scale = power < 0 ? 10n ** BigInt(-power) : 1n;
    const divisor = denominator * (power > 0 ? 10n ** BigInt(power) : 1n);
    const low = lower * numeratorScale * scale;
    const high = upper * numeratorScale * scale;
    // Strict inequalities exclude exact midpoints even for an even binary significand.
    const first = low / divisor + 1n;
    const last = (high - 1n) / divisor;
    if (first > last) continue;
    const scaled = exact * scale;
    let coefficient = scaled / divisor;
    const remainder = scaled % divisor;
    if (2n * remainder > divisor || (2n * remainder === divisor && coefficient % 2n !== 0n))
      coefficient++;
    if (coefficient < first) coefficient = first;
    if (coefficient > last) coefficient = last;
    return (negative ? "-" : "") + notation(coefficient, power, single);
  }
  throw new Error("No shortest float inside its binary rounding interval");
}

/** V8's shortest binary64 conversion also chooses nearest digits and even decimal ties.
 * Below 2^53 a shortest decimal cannot be a binary midpoint: fractional midpoints
 * need more significant digits than their neighbour, and integer midpoints start
 * above the safe-integer range. Above that bound retain the exact open-interval proof.
 * Binary32 neighbours/midpoints are exact binary64 values. A strictly interior
 * rounded candidate proves its decimal is interior too; a rounded endpoint falls
 * back. Power-of-two asymmetric intervals and near decimal ties also fall back.
 */
export function jsonFloatText(value: number, single: boolean): string {
  if (value === 0) return Object.is(value, -0) ? "-0" : "0";
  if (!Number.isFinite(value)) return String(value);
  const magnitude = Math.abs(value);
  if (!single) {
    if (magnitude >= 2 ** 53) return exactFloatText(value, false);
    return pgNotation(value, false);
  }
  bits.setFloat32(0, magnitude);
  const word = bits.getUint32(0),
    exponent = word >>> 23;
  if ((word & 0x7fffff) === 0 && exponent > 1) return exactFloatText(value, true);
  bits.setUint32(0, word - 1);
  const lower = (magnitude + bits.getFloat32(0)) / 2;
  bits.setUint32(0, word + 1);
  // Largest finite float's next mathematical neighbour is 2^128, not Infinity.
  const upper =
    (magnitude +
      (exponent === 254 && (word & 0x7fffff) === 0x7fffff ? 2 ** 128 : bits.getFloat32(0))) /
    2;
  for (let precision = 1; precision <= 9; precision++) {
    const text = magnitude.toPrecision(precision);
    const candidate = Number(text);
    if (Math.fround(candidate) !== magnitude) continue;
    if (!(candidate > lower && candidate < upper)) return exactFloatText(value, true);
    const power = decimalPower(text, precision);
    const mantissa = (word & 0x7fffff) + (exponent === 0 ? 0 : 0x800000);
    const trailing = 31 - Math.clz32(mantissa & -mantissa);
    // A decimal-grid tie requires exactly one uncancelled factor 2 downstairs.
    // Multiplying/dividing by 5 never changes that valuation. This necessary test
    // is exact; for positive decimal powers it may conservatively fall back.
    if (Math.max(1, exponent) - 127 - 23 + trailing - power === -1)
      return exactFloatText(value, true);
    return pgNotation(value < 0 ? -candidate : candidate, true);
  }
  return exactFloatText(value, true);
}

/** Reuse native decimal digits; change notation only where PG's thresholds differ. */
function pgNotation(value: number, single: boolean): string {
  const magnitude = Math.abs(value);
  if (magnitude >= 1e-4 && magnitude < (single ? 1e6 : 1e15)) return String(value);
  const text = value.toExponential();
  const at = text.length - 2;
  return text.charCodeAt(at) === 43 || text.charCodeAt(at) === 45
    ? text.slice(0, at + 1) + "0" + text.charAt(at + 1)
    : text;
}

/** The exact grid toPrecision rounded on, including a carry into a new exponent. */
function decimalPower(text: string, precision: number): number {
  const e = text.indexOf("e");
  if (e >= 0) return Number(text.slice(e + 1)) - precision + 1;
  const point = text.indexOf(".");
  if (text.charCodeAt(0) !== 48) return (point < 0 ? text.length : point) - precision;
  let first = 2;
  while (text.charCodeAt(first) === 48) first++;
  return 2 - first - precision;
}
