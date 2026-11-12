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

export function jsonFloatText(value: number, single: boolean): string {
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
