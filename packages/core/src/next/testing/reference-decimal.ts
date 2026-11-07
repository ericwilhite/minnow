/**
 * Exact NUMERIC values for the IR reference interpreter (ir-reference.ts): a coefficient and a
 * scale (value = coefficient / 10^scale), or one of PostgreSQL's specials NaN, +Infinity and
 * -Infinity (PostgreSQL 14+). Ordering follows PostgreSQL's cmp_numerics: -Infinity < every
 * finite value < +Infinity < NaN, and NaN equals NaN. Arithmetic is exact; only what the
 * reference needs is here (comparison, addition, rescaling and conversions).
 */

export type DecimalSpecialKind = "nan" | "+inf" | "-inf";

export class Decimal {
  private constructor(
    /** null for a finite value. */
    readonly special: DecimalSpecialKind | null,
    readonly coefficient: bigint,
    readonly scale: number,
  ) {}

  static finite(coefficient: bigint, scale: number): Decimal {
    if (!Number.isInteger(scale) || scale < 0)
      throw new RangeError(`bad decimal scale ${String(scale)}`);
    return new Decimal(null, coefficient, scale);
  }

  static special(kind: DecimalSpecialKind): Decimal {
    return new Decimal(kind, 0n, 0);
  }

  static fromInteger(value: bigint): Decimal {
    return Decimal.finite(value, 0);
  }

  /**
   * PostgreSQL's numeric_in: an optionally signed decimal with an optional exponent, or NaN /
   * Infinity / inf (any case, signed). Null when `text` is not one.
   */
  static parse(text: string): Decimal | null {
    const trimmed = text.trim();
    const word = trimmed.toLowerCase();
    if (word === "nan") return Decimal.special("nan");
    if (word === "infinity" || word === "+infinity" || word === "inf" || word === "+inf")
      return Decimal.special("+inf");
    if (word === "-infinity" || word === "-inf") return Decimal.special("-inf");
    const match = /^([+-]?)(\d*)(?:\.(\d*))?(?:[eE]([+-]?\d+))?$/u.exec(trimmed);
    if (match === null) return null;
    const [, sign = "", whole = "", fraction = "", exponent = "0"] = match;
    if (whole === "" && fraction === "") return null;
    let coefficient = BigInt(`${whole}${fraction}` || "0");
    let scale = fraction.length - Number(exponent);
    if (scale < 0) {
      coefficient *= 10n ** BigInt(-scale);
      scale = 0;
    }
    return Decimal.finite(sign === "-" ? -coefficient : coefficient, scale);
  }

  /**
   * PostgreSQL's float8_numeric (float4_numeric with `digits` 6): the float printed with 15
   * significant digits (`%.15g`, trailing zeros dropped) and read back; NaN and ±Infinity map to
   * the specials.
   */
  static fromFloat(value: number, digits = 15): Decimal {
    if (Number.isNaN(value)) return Decimal.special("nan");
    if (value === Infinity) return Decimal.special("+inf");
    if (value === -Infinity) return Decimal.special("-inf");
    const printed = value.toPrecision(digits);
    const [mantissa = "0", exponent] = printed.split("e");
    const trimmed = mantissa.includes(".") ? mantissa.replace(/\.?0+$/u, "") : mantissa;
    const parsed = Decimal.parse(exponent === undefined ? trimmed : `${trimmed}e${exponent}`);
    if (parsed === null) throw new RangeError(`unprintable float ${String(value)}`);
    return parsed;
  }

  get isNaN(): boolean {
    return this.special === "nan";
  }

  get isFinite(): boolean {
    return this.special === null;
  }

  /** -1, 0 or 1 by PostgreSQL's numeric order (NaN greatest, NaN = NaN). */
  compare(other: Decimal): number {
    const rank = (value: Decimal) =>
      value.special === "-inf" ? 0 : value.special === null ? 1 : value.special === "+inf" ? 2 : 3;
    const left = rank(this);
    const right = rank(other);
    if (left !== right || left !== 1) return Math.sign(left - right);
    const scale = Math.max(this.scale, other.scale);
    const a = this.coefficient * 10n ** BigInt(scale - this.scale);
    const b = other.coefficient * 10n ** BigInt(scale - other.scale);
    return a < b ? -1 : a > b ? 1 : 0;
  }

  /** The exact sum; +Infinity + -Infinity and anything + NaN are NaN, as PostgreSQL. */
  add(other: Decimal): Decimal {
    if (this.isNaN || other.isNaN) return Decimal.special("nan");
    if (this.special !== null || other.special !== null) {
      if (this.special !== null && other.special !== null && this.special !== other.special)
        return Decimal.special("nan");
      return this.special !== null ? this : other;
    }
    const scale = Math.max(this.scale, other.scale);
    return Decimal.finite(
      this.coefficient * 10n ** BigInt(scale - this.scale) +
        other.coefficient * 10n ** BigInt(scale - other.scale),
      scale,
    );
  }

  negate(): Decimal {
    if (this.special === "+inf") return Decimal.special("-inf");
    if (this.special === "-inf") return Decimal.special("+inf");
    if (this.special === "nan") return this;
    return Decimal.finite(-this.coefficient, this.scale);
  }

  get sign(): number {
    if (this.special === "+inf") return 1;
    if (this.special === "-inf") return -1;
    if (this.special === "nan") return Number.NaN;
    return this.coefficient < 0n ? -1 : this.coefficient > 0n ? 1 : 0;
  }

  /** Rounded half away from zero to `scale` digits (numeric's rounding), exactly. */
  rescale(scale: number): Decimal {
    if (this.special !== null || scale === this.scale) return this;
    if (scale > this.scale)
      return Decimal.finite(this.coefficient * 10n ** BigInt(scale - this.scale), scale);
    const divisor = 10n ** BigInt(this.scale - scale);
    const magnitude = this.coefficient < 0n ? -this.coefficient : this.coefficient;
    let quotient = magnitude / divisor;
    if ((magnitude % divisor) * 2n >= divisor) quotient += 1n;
    return Decimal.finite(this.coefficient < 0n ? -quotient : quotient, scale);
  }

  /** The integer rounded half away from zero (numeric → integer casts). Finite values only. */
  toInteger(): bigint {
    if (this.special !== null) throw new RangeError("a special numeric has no integer");
    return this.rescale(0).coefficient;
  }

  /** The nearest float (numeric → float8). */
  toNumber(): number {
    if (this.special === "nan") return Number.NaN;
    if (this.special === "+inf") return Infinity;
    if (this.special === "-inf") return -Infinity;
    return Number(this.toString());
  }

  /** `toNumber`, so arithmetic the reference leaves approximate keeps its old behaviour. */
  valueOf(): number {
    return this.toNumber();
  }

  /** PostgreSQL's numeric_out: every digit of the scale, `NaN`, `Infinity`, `-Infinity`. */
  toString(): string {
    if (this.special === "nan") return "NaN";
    if (this.special === "+inf") return "Infinity";
    if (this.special === "-inf") return "-Infinity";
    const negative = this.coefficient < 0n;
    const digits = (negative ? -this.coefficient : this.coefficient)
      .toString()
      .padStart(this.scale + 1, "0");
    const whole = digits.slice(0, digits.length - this.scale);
    const fraction = digits.slice(digits.length - this.scale);
    return `${negative ? "-" : ""}${whole}${this.scale > 0 ? `.${fraction}` : ""}`;
  }

  /** One key for every value equal to this one (1.50 and 1.5 share it). */
  key(): string {
    if (this.special !== null) return this.toString();
    let { coefficient, scale } = this;
    while (scale > 0 && coefficient % 10n === 0n) {
      coefficient /= 10n;
      scale -= 1;
    }
    return Decimal.finite(coefficient, scale).toString();
  }
}

/** A numeric operand as an exact Decimal: integers exactly, floats as float8_numeric does. */
export function toDecimal(value: bigint | number | Decimal): Decimal {
  if (value instanceof Decimal) return value;
  if (typeof value === "bigint") return Decimal.fromInteger(value);
  return Decimal.fromFloat(value);
}
