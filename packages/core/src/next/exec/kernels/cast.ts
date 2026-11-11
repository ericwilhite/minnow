/**
 * Cast kernels between numeric and temporal types, plus temporal output functions.
 *
 * PostgreSQL's rules:
 * - Integer narrowing checks the range (22003 `smallint/integer out of range`).
 * - float → integer rounds half to even (`rint`) and checks the range; float8 → float4 errors when
 *   the value overflows float4 (22003 `value out of range: overflow`).
 * - numeric → integer rounds half away from zero; numeric → float is correctly rounded.
 * - float → numeric goes through 15 significant digits (`%.15g`, PostgreSQL's float8_numeric).
 * - numeric → NUMERIC(p, s) rounds half away from zero to scale s and refuses values with more
 *   than p − s integer digits (22003 `numeric field overflow`); NaN stays NaN; ±Infinity fails
 *   for a typmod with a precision.
 * - date → timestamp is midnight; timestamp → date floors to the day; infinities map to
 *   infinities. timestamp ↔ timestamptz keeps the stored µs (session time zone UTC, D1).
 */
import type { PhysicalType } from "../../types.js";
import {
  INT16_MAX,
  INT16_MIN,
  INT32_MAX,
  INT32_MIN,
  INT64_MAX,
  INT64_MIN,
  MICROS_PER_DAY,
  samePhysicalType,
} from "../../types.js";
import { sqlTypeOf } from "../../ir/functions.js";
import {
  DATE_NEG_INFINITY,
  DATE_POS_INFINITY,
  TIMESTAMP_NEG_INFINITY,
  TIMESTAMP_POS_INFINITY,
} from "../../temporal.js";
import { MAX_SAFE } from "../../vector/int64.js";
import type { Batch, FlatVector, Int64Values, Selection, Vector } from "../../vector/vector.js";
import {
  CannotConvertError,
  DatetimeOverflowError,
  NumericOutOfRangeError,
  UnsupportedPlanError,
} from "../errors.js";
import { compileJson } from "./json.js";
import { compileTemporalText } from "./temporal-text.js";
import { castRefusal } from "./bindings.js";
import { coefficientAt, scaleAt, specialAt, type DecimalData } from "./numeric.js";
import {
  nullConstant,
  countIn,
  rowsIn,
  slotOf,
  validAt,
  type Compiled,
  type OutputArena,
} from "./vectors.js";

const MICROS_PER_DAY_BIG = BigInt(MICROS_PER_DAY);
const FLOAT4_MAX = 3.4028234663852886e38;

/** Round half to even (C's rint in the default rounding mode). */
function rint(x: number): number {
  const r = Math.round(x);
  return r - x === 0.5 && r % 2 !== 0 ? r - 1 : r;
}

/** Round coefficient `c` at scale `from` to scale `to` < from, half away from zero. */
function roundCoefficient(c: bigint, from: number, to: number): bigint {
  const divisor = 10n ** BigInt(from - to);
  const q = c / divisor;
  const r = c - q * divisor;
  const twice = (r < 0n ? -r : r) * 2n;
  return twice >= divisor ? q + (c < 0n ? -1n : 1n) : q;
}

/** A finite decimal as an exact float (correctly rounded, through its decimal text). */
function decimalToFloat(c: bigint, scale: number): number {
  return Number(scale === 0 ? c.toString() : `${c.toString()}e-${String(scale)}`);
}

/**
 * PostgreSQL float → numeric: `digits` significant digits (15 for float8, 6 for float4: DBL_DIG
 * and FLT_DIG), trailing fraction zeros dropped.
 */
function floatToDecimal(x: number, significant: number): { coefficient: bigint; scale: number } {
  const text = x.toPrecision(significant);
  const [mantissa = "0", exponentText] = text.split("e");
  const exponent = exponentText === undefined ? 0 : Number(exponentText);
  const negative = mantissa.startsWith("-");
  const [whole = "0", fraction = ""] = (negative ? mantissa.slice(1) : mantissa).split(".");
  let digits = whole + fraction;
  let scale = fraction.length - exponent;
  while (scale > 0 && digits.endsWith("0")) {
    digits = digits.slice(0, -1);
    scale -= 1;
  }
  let coefficient = BigInt(digits === "" ? "0" : digits);
  if (scale < 0) {
    coefficient *= 10n ** BigInt(-scale);
    scale = 0;
  }
  return { coefficient: negative ? -coefficient : coefficient, scale };
}

type Reader = (row: number) => number | bigint;

/** One output slot writer per target layout; rows are written only when valid. */
abstract class CastNode implements Compiled {
  readonly #null: Vector;
  constructor(
    readonly type: PhysicalType,
    readonly input: Compiled,
  ) {
    this.#null = nullConstant(type);
  }
  evaluate(batch: Batch, rows: Selection | null): Vector {
    const value = this.input.evaluate(batch, rows);
    if (value.form === "constant" && !validAt(value.value.validity, 0)) {
      (this.#null as { length: number }).length = batch.length;
      return this.#null;
    }
    const active = rowsIn(rows);
    const count = countIn(rows, batch.length);
    return this.cast(value, active, count, batch.length);
  }
  abstract cast(value: Vector, rows: Uint16Array, count: number, length: number): Vector;
}

function validityOf(value: Vector): Uint32Array | null {
  return value.form === "constant" ? null : value.validity;
}

function isValidRow(value: Vector, row: number): boolean {
  return value.form === "constant"
    ? validAt(value.value.validity, 0)
    : validAt(value.validity, row);
}

/**
 * The values of a flat or constant number-like vector (bool, int32, float, safe int64) and their
 * stride, or null (dictionaries, BigInt64Array, decimals): the casts' branch-free fast path.
 */
function numberArray(value: Vector): [ArrayLike<number>, number] | null {
  if (value.form === "dictionary") return null;
  const flat = value.form === "flat" ? value : value.value;
  const stride = value.form === "flat" ? 1 : 0;
  const data = flat.data;
  if (
    data.layout === "bool" ||
    data.layout === "i32" ||
    data.layout === "f32" ||
    data.layout === "f64"
  )
    return [data.values, stride];
  if (data.layout === "int64" && data.values.repr === "safe") return [data.values.values, stride];
  return null;
}

/** Reads row `row` of a numeric-layout vector as a JS number or bigint (exact). */
function readerOf(value: Vector): Reader {
  return (row) => {
    const [flat, slot] = slotOf(value, row);
    const data = flat.data;
    switch (data.layout) {
      case "bool":
      case "i32":
      case "f32":
      case "f64":
        return data.values[slot] ?? 0;
      case "int64":
        return data.values.repr === "safe"
          ? (data.values.values[slot] ?? 0)
          : (data.values.values[slot] ?? 0n);
      default:
        throw new TypeError(`no numeric reader for ${data.layout}`);
    }
  };
}

class ToInt32 extends CastNode {
  readonly #out: FlatVector;
  readonly #min: number;
  readonly #max: number;
  readonly #message: string;
  constructor(type: PhysicalType, input: Compiled, arena: OutputArena) {
    super(type, input);
    this.#out = arena.flat(type).vector;
    const small = type.kind === "int32" && type.bits === 16;
    this.#min = small ? INT16_MIN : INT32_MIN;
    this.#max = small ? INT16_MAX : INT32_MAX;
    this.#message = small ? "smallint out of range" : "integer out of range";
  }
  cast(value: Vector, rows: Uint16Array, count: number, length: number): Vector {
    const out = (this.#out.data as { values: Int32Array }).values;
    const from = this.input.type.kind;
    const [flat] = slotOf(value, 0);
    const read = flat.data.layout === "decimal" ? null : readerOf(value);
    for (let k = 0; k < count; k++) {
      const i = rows[k] ?? 0;
      if (!isValidRow(value, i)) continue;
      let v: number;
      if (read === null) {
        const [f, slot] = slotOf(value, i);
        const d = f.data as DecimalData;
        if (specialAt(d, slot) !== 0)
          throw new CannotConvertError(
            specialAt(d, slot),
            this.#message.split(" ")[0] ?? "integer",
          );
        const scale = scaleAt(d, slot);
        const c = coefficientAt(d, slot);
        const rounded = scale === 0 ? c : roundCoefficient(c, scale, 0);
        if (rounded < BigInt(this.#min) || rounded > BigInt(this.#max))
          throw new NumericOutOfRangeError(this.#message);
        v = Number(rounded);
      } else {
        const raw = read(i);
        if (typeof raw === "bigint") {
          if (raw < BigInt(this.#min) || raw > BigInt(this.#max))
            throw new NumericOutOfRangeError(this.#message);
          v = Number(raw);
        } else {
          v = from === "float64" || from === "float32" ? rint(raw) : raw;
          if (!(v >= this.#min && v <= this.#max)) throw new NumericOutOfRangeError(this.#message);
        }
      }
      out[i] = v;
    }
    this.#out.length = length;
    this.#out.validity = validityOf(value);
    return this.#out;
  }
}

class ToInt64 extends CastNode {
  readonly #safe: FlatVector;
  readonly #wide: FlatVector;
  constructor(type: PhysicalType, input: Compiled, arena: OutputArena) {
    super(type, input);
    this.#safe = arena.flat(type).vector;
    arena.take(8 * arena.capacity);
    this.#wide = {
      form: "flat",
      type,
      length: 0,
      validity: null,
      data: {
        layout: "int64",
        values: { repr: "int64", values: new BigInt64Array(arena.capacity) },
      },
    };
  }
  cast(value: Vector, rows: Uint16Array, count: number, length: number): Vector {
    const from = this.input.type;
    // int32 → int64 and safe int64 inputs stay safe without a per-row check.
    const safe = ((this.#safe.data as { values: Int64Values }).values as { values: Float64Array })
      .values;
    const wide = ((this.#wide.data as { values: Int64Values }).values as { values: BigInt64Array })
      .values;
    if (from.kind === "int32" || (from.kind === "int64" && numberArray(value) !== null)) {
      const fast = numberArray(value);
      if (fast !== null) {
        const [x, stride] = fast;
        for (let k = 0; k < count; k++) {
          const i = rows[k] ?? 0;
          safe[i] = x[i * stride] ?? 0;
        }
        this.#safe.length = length;
        this.#safe.validity = validityOf(value);
        return this.#safe;
      }
    }
    const read = from.kind === "decimal" ? null : readerOf(value);
    let useWide = false;
    for (let k = 0; k < count; k++) {
      const i = rows[k] ?? 0;
      if (!isValidRow(value, i)) continue;
      let v: number | bigint;
      if (read === null) {
        const [f, slot] = slotOf(value, i);
        const d = f.data as DecimalData;
        if (specialAt(d, slot) !== 0) throw new CannotConvertError(specialAt(d, slot), "bigint");
        const scale = scaleAt(d, slot);
        const c = coefficientAt(d, slot);
        v = scale === 0 ? c : roundCoefficient(c, scale, 0);
      } else {
        const raw = read(i);
        if (typeof raw === "bigint") v = raw;
        else if (from.kind === "float64" || from.kind === "float32") {
          const r = rint(raw);
          if (!(r >= -9223372036854775808 && r < 9223372036854775808))
            throw new NumericOutOfRangeError("bigint out of range");
          v = Number.isSafeInteger(r) ? r : BigInt(r);
        } else v = raw;
      }
      if (typeof v === "bigint") {
        if (v > INT64_MAX || v < INT64_MIN) throw new NumericOutOfRangeError("bigint out of range");
        if (v >= -BigInt(MAX_SAFE) && v <= BigInt(MAX_SAFE)) {
          safe[i] = Number(v);
          wide[i] = v;
        } else {
          useWide = true;
          wide[i] = v;
        }
      } else {
        safe[i] = v + 0;
        wide[i] = BigInt(v);
      }
    }
    const out = useWide ? this.#wide : this.#safe;
    out.length = length;
    out.validity = validityOf(value);
    return out;
  }
}

class ToFloat extends CastNode {
  readonly #out: FlatVector;
  constructor(type: PhysicalType, input: Compiled, arena: OutputArena) {
    super(type, input);
    this.#out = arena.flat(type).vector;
  }
  cast(value: Vector, rows: Uint16Array, count: number, length: number): Vector {
    const out = (this.#out.data as { values: Float64Array | Float32Array }).values;
    const real = this.type.kind === "float32";
    const fast = numberArray(value);
    if (fast !== null && (!real || this.input.type.kind === "float32")) {
      // int32, safe int64 and float32 widen exactly (or within PostgreSQL's float8 rounding).
      const [x, stride] = fast;
      for (let k = 0; k < count; k++) {
        const i = rows[k] ?? 0;
        out[i] = x[i * stride] ?? 0;
      }
      this.#out.length = length;
      this.#out.validity = validityOf(value);
      return this.#out;
    }
    const decimal = this.input.type.kind === "decimal";
    const read = decimal ? null : readerOf(value);
    for (let k = 0; k < count; k++) {
      const i = rows[k] ?? 0;
      if (!isValidRow(value, i)) continue;
      let v: number;
      if (read === null) {
        const [f, slot] = slotOf(value, i);
        const d = f.data as DecimalData;
        const special = specialAt(d, slot);
        if (special !== 0) v = special === 1 ? NaN : special === 2 ? Infinity : -Infinity;
        else {
          // PostgreSQL parses the numeric's text as a float: a finite value past the float range
          // is an overflow, a nonzero one that rounds to zero an underflow (22003).
          const c = coefficientAt(d, slot);
          v = decimalToFloat(c, scaleAt(d, slot));
          const rounded = real ? Math.fround(v) : v;
          if (!Number.isFinite(rounded))
            throw new NumericOutOfRangeError("value out of range: overflow");
          if (rounded === 0 && c !== 0n)
            throw new NumericOutOfRangeError("value out of range: underflow");
        }
      } else {
        const raw = read(i);
        v = typeof raw === "bigint" ? Number(raw) : raw;
      }
      if (real) {
        if (Number.isFinite(v) && Math.abs(v) > FLOAT4_MAX && Math.abs(Math.fround(v)) === Infinity)
          throw new NumericOutOfRangeError("value out of range: overflow");
        const r = Math.fround(v);
        if (r === 0 && v !== 0 && Number.isFinite(v))
          throw new NumericOutOfRangeError("value out of range: underflow");
        out[i] = r;
      } else out[i] = v;
    }
    this.#out.length = length;
    this.#out.validity = validityOf(value);
    return this.#out;
  }
}

class ToDecimal extends CastNode {
  /** Results by tier and specials: safe, safe + specials, wide, wide + specials. */
  readonly #outputs: FlatVector[];
  readonly #scales: Uint16Array;
  readonly #safe: Float64Array;
  readonly #wide: bigint[];
  readonly #specials: Uint8Array;
  readonly #precision: number | null;
  readonly #scale: number | null;
  constructor(type: PhysicalType, input: Compiled, arena: OutputArena) {
    super(type, input);
    const capacity = arena.capacity;
    arena.take(2 * capacity + 8 * capacity + capacity + 32 * capacity);
    this.#scales = new Uint16Array(capacity);
    this.#safe = new Float64Array(capacity);
    this.#wide = new Array<bigint>(capacity).fill(0n);
    this.#specials = new Uint8Array(capacity);
    this.#precision = type.kind === "decimal" ? type.precision : null;
    this.#scale = type.kind === "decimal" ? type.scale : null;
    // A declared scale (negative ones are stored at 0) makes the result uniform.
    const uniform = this.#scale === null ? null : Math.max(0, this.#scale);
    const scales = uniform === null ? this.#scales : null;
    this.#outputs = [
      { repr: "safe", values: this.#safe } as const,
      { repr: "wide", values: this.#wide } as const,
    ].flatMap((coefficients) =>
      [null, this.#specials].map((specials): FlatVector => ({
        form: "flat",
        type,
        length: 0,
        validity: null,
        data: { layout: "decimal", scale: uniform, scales, coefficients, specials },
      })),
    );
  }
  cast(value: Vector, rows: Uint16Array, count: number, length: number): Vector {
    const from = this.input.type.kind;
    const target = this.#scale;
    const precision = this.#precision;
    const limit = precision === null ? null : 10n ** BigInt(precision);
    const scales = this.#scales;
    const wide = this.#wide;
    const safe = this.#safe;
    const specials = this.#specials;
    let anySpecial = false;
    let allSafe = true;
    const read = from === "decimal" ? null : readerOf(value);
    for (let k = 0; k < count; k++) {
      const i = rows[k] ?? 0;
      specials[i] = 0;
      if (!isValidRow(value, i)) continue;
      let c: bigint;
      let s: number;
      let special = 0;
      if (read === null) {
        const [f, slot] = slotOf(value, i);
        const d = f.data as DecimalData;
        special = specialAt(d, slot);
        c = special === 0 ? coefficientAt(d, slot) : 0n;
        s = scaleAt(d, slot);
      } else {
        const raw = read(i);
        if (from === "float64" || from === "float32") {
          const x = raw as number;
          if (x !== x) special = 1;
          else if (x === Infinity) special = 2;
          else if (x === -Infinity) special = 3;
          // PostgreSQL: float8 → numeric through 15 significant digits, float4 through 6.
          if (special === 0)
            ({ coefficient: c, scale: s } = floatToDecimal(x, from === "float32" ? 6 : 15));
          else {
            c = 0n;
            s = 0;
          }
        } else {
          c = typeof raw === "bigint" ? raw : BigInt(raw);
          s = 0;
        }
      }
      if (special !== 0) {
        if (special !== 1 && precision !== null)
          throw new NumericOutOfRangeError("numeric field overflow");
        specials[i] = special;
        anySpecial = true;
        scales[i] = 0;
        wide[i] = 0n;
        safe[i] = 0;
        continue;
      }
      if (target !== null) {
        // Round to the declared scale, which may be negative (NUMERIC(3, −2) rounds to hundreds);
        // the precision counts the digits at that scale. A negative scale is stored at scale 0.
        if (s > target) c = roundCoefficient(c, s, target);
        else if (s < target) c *= 10n ** BigInt(target - s);
        s = target;
      }
      if (limit !== null && (c < 0n ? -c : c) >= limit)
        throw new NumericOutOfRangeError("numeric field overflow");
      if (s < 0) {
        c *= 10n ** BigInt(-s);
        s = 0;
      }
      scales[i] = s;
      wide[i] = c;
      if (c >= -BigInt(MAX_SAFE) && c <= BigInt(MAX_SAFE)) safe[i] = Number(c);
      else allSafe = false;
    }
    const out = this.#outputs[(allSafe ? 0 : 2) + (anySpecial ? 1 : 0)];
    if (out === undefined) throw new TypeError("no decimal cast output");
    out.length = length;
    out.validity = validityOf(value);
    return out;
  }
}

/**
 * The days (since 2000-01-01) whose midnight is a finite TIMESTAMP: 4714-11-24 BC through
 * 294276-12-31 (PostgreSQL's date2timestamp: later dates are 22008, verified in PGlite). The same
 * bounds hold for TIMESTAMPTZ under the UTC session zone.
 */
const TIMESTAMP_FIRST_DAY = -2_451_545;
const TIMESTAMP_LAST_DAY = 106_751_982;

class DateToTimestamp extends CastNode {
  readonly #out: FlatVector;
  constructor(type: PhysicalType, input: Compiled, arena: OutputArena) {
    super(type, input);
    arena.take(8 * arena.capacity);
    this.#out = {
      form: "flat",
      type,
      length: 0,
      validity: null,
      data: {
        layout: "int64",
        values: { repr: "int64", values: new BigInt64Array(arena.capacity) },
      },
    };
  }
  cast(value: Vector, rows: Uint16Array, count: number, length: number): Vector {
    const out = ((this.#out.data as { values: Int64Values }).values as { values: BigInt64Array })
      .values;
    const read = readerOf(value);
    for (let k = 0; k < count; k++) {
      const i = rows[k] ?? 0;
      if (!isValidRow(value, i)) continue;
      const days = read(i) as number;
      if (days === DATE_POS_INFINITY) out[i] = TIMESTAMP_POS_INFINITY;
      else if (days === DATE_NEG_INFINITY) out[i] = TIMESTAMP_NEG_INFINITY;
      else if (days < TIMESTAMP_FIRST_DAY || days > TIMESTAMP_LAST_DAY)
        throw new DatetimeOverflowError("date out of range for timestamp");
      else out[i] = BigInt(days) * MICROS_PER_DAY_BIG;
    }
    this.#out.length = length;
    this.#out.validity = validityOf(value);
    return this.#out;
  }
}

class TimestampToDate extends CastNode {
  readonly #out: FlatVector;
  constructor(type: PhysicalType, input: Compiled, arena: OutputArena) {
    super(type, input);
    this.#out = arena.flat(type).vector;
  }
  cast(value: Vector, rows: Uint16Array, count: number, length: number): Vector {
    const out = (this.#out.data as { values: Int32Array }).values;
    const read = readerOf(value);
    for (let k = 0; k < count; k++) {
      const i = rows[k] ?? 0;
      if (!isValidRow(value, i)) continue;
      const raw = read(i);
      const micros = typeof raw === "bigint" ? raw : BigInt(raw);
      if (micros === TIMESTAMP_POS_INFINITY) out[i] = DATE_POS_INFINITY;
      else if (micros === TIMESTAMP_NEG_INFINITY) out[i] = DATE_NEG_INFINITY;
      else {
        let days = micros / MICROS_PER_DAY_BIG;
        if (micros < 0n && days * MICROS_PER_DAY_BIG !== micros) days -= 1n;
        out[i] = Number(days);
      }
    }
    this.#out.length = length;
    this.#out.validity = validityOf(value);
    return this.#out;
  }
}

/** Timestamp ↔ timestamptz and other same-layout retypings: the same values, a new type. */
class Retype implements Compiled {
  constructor(
    readonly type: PhysicalType,
    readonly input: Compiled,
  ) {}
  evaluate(batch: Batch, rows: Selection | null): Vector {
    const value = this.input.evaluate(batch, rows);
    switch (value.form) {
      case "flat":
        return { ...value, type: this.type };
      case "constant":
        return { ...value, type: this.type, value: { ...value.value, type: this.type } };
      case "dictionary":
        return { ...value, type: this.type };
    }
  }
}

const NUMERIC = new Set(["int32", "int64", "float64", "float32", "decimal"]);

/**
 * The kernel for `CAST(input AS type)`: the pairs ir/functions.ts allows minus bindings.ts
 * `CAST_REFUSALS` (bindings.test.ts checks every pair). Assignment casts (`cast.assignment`) use
 * the same kernels: numeric range and precision errors are the same, and the one difference,
 * 22001 for an over-long text, is refused with the bounded text casts until the text kernels.
 */
export function compileCast(type: PhysicalType, input: Compiled, arena: OutputArena): Compiled {
  const from = input.type;
  const source = sqlTypeOf(from);
  const target = sqlTypeOf(type);
  const refuse = (reason: string) =>
    new UnsupportedPlanError(
      `a cast from ${source ?? from.kind} to ${target ?? type.kind}: ${reason}`,
      { construct: "cast" },
    );
  if (source !== null && target !== null) {
    const refusal = castRefusal(source, target);
    if (refusal !== null) throw refuse(refusal);
  }
  if (type.kind === "json" && from.kind === "text")
    return compileJson(type, type.binary ? "validate_jsonb" : "validate_json", [input], arena);
  if (type.kind === "text" && type.maxLength === undefined && from.kind === "json")
    return from.binary
      ? compileJson(type, "validate_jsonb", [input], arena)
      : new Retype(type, input);
  if (
    type.kind === "text" &&
    (from.kind === "date" || from.kind === "timestamp" || from.kind === "interval")
  )
    return compileTemporalText(type, input, arena);
  if (NUMERIC.has(from.kind) && NUMERIC.has(type.kind)) {
    switch (type.kind) {
      case "int32":
        if (from.kind === "int32" && from.bits === type.bits) return input;
        // Widening int2 → int4 keeps every value; the node must still carry the target type,
        // since calls bind their overload from their arguments' compiled types (R19).
        if (from.kind === "int32" && type.bits === 32) return new Retype(type, input);
        return new ToInt32(type, input, arena);
      case "int64":
        return from.kind === "int64" ? input : new ToInt64(type, input, arena);
      case "float64":
      case "float32":
        return from.kind === type.kind ? input : new ToFloat(type, input, arena);
      case "decimal":
        return new ToDecimal(type, input, arena);
    }
  }
  if (from.kind === "date" && type.kind === "timestamp")
    return new DateToTimestamp(type, input, arena);
  if (from.kind === "timestamp" && type.kind === "date")
    return new TimestampToDate(type, input, arena);
  // Same stored value: the session time zone is UTC (types.ts), so zoned ↔ unzoned relabels.
  if (from.kind === "timestamp" && type.kind === "timestamp") return new Retype(type, input);
  if (type.kind === "text" && from.kind === "text") {
    if (type.maxLength !== undefined)
      throw refuse("bounded text (truncation and 22001 come with the text kernels)");
    return new Retype(type, input);
  }
  if (
    from.kind === type.kind &&
    (from.kind === "bool" ||
      from.kind === "date" ||
      from.kind === "time" ||
      from.kind === "interval" ||
      from.kind === "uuid" ||
      from.kind === "bytes" ||
      samePhysicalType(from, type))
  )
    return input;
  throw refuse("no kernel");
}
