/** Execution-owned parameter vectors. Binding uses the accepted forms on executor.ts. */
import type { PhysicalPlan } from "../ir/ir.js";
import {
  INT16_MIN,
  INT16_MAX,
  INT32_MIN,
  INT32_MAX,
  INT64_MIN,
  INT64_MAX,
  type PhysicalType,
} from "../types.js";
import {
  DATE_MIN,
  DATE_MAX,
  DATE_NEG_INFINITY,
  DATE_POS_INFINITY,
  civilFromDays,
  daysFromCivil,
  TIMESTAMP_MIN,
  TIMESTAMP_END,
  TIMESTAMP_NEG_INFINITY,
  TIMESTAMP_POS_INFINITY,
  parseTimestamp,
  timestampFromDate,
} from "../temporal.js";
import { allocateFlat, type FlatVector, type VectorData } from "../vector/vector.js";
import type { BoundParameters, ParameterValue } from "./executor.js";
import { ParameterBindError, UnsupportedPlanError } from "./errors.js";
import { compileCast } from "./kernels/cast.js";
import { flatOf, OutputArena } from "./kernels/vectors.js";

const UNCHARGED = { allocate: () => true, free: () => undefined };
const TEXT: PhysicalType = { kind: "text" };
const NUMERIC: PhysicalType = { kind: "decimal", precision: null, scale: null };

/** Refuse unsupported binding types at prepare, before admitting or touching a snapshot. */
export function checkParameterType(type: PhysicalType): void {
  if (
    [
      "bool",
      "int32",
      "int64",
      "float32",
      "float64",
      "decimal",
      "date",
      "timestamp",
      "bytes",
    ].includes(type.kind)
  )
    return;
  if (type.kind === "text" && type.maxLength === undefined) return;
  throw new UnsupportedPlanError(`a ${type.kind} parameter`, { construct: "parameter" });
}

function vector(type: PhysicalType, data: VectorData): FlatVector {
  return { form: "flat", type, length: 1, validity: null, data };
}

export function nullParameter(type: PhysicalType): FlatVector {
  return { ...allocateFlat(type, 1), length: 1, validity: new Uint32Array(1) };
}

/** Statement clocks are stored microseconds, not client timestamp values. */
export function clockParameter(type: PhysicalType, micros: bigint): FlatVector {
  return vector(type, {
    layout: "int64",
    values: { repr: "int64", values: BigInt64Array.of(micros) },
  });
}

function text(value: string, type: PhysicalType = TEXT): FlatVector {
  if (value.includes("\0")) throw new RangeError("text cannot contain a zero byte");
  const bytes = new TextEncoder().encode(value);
  return vector(type, { layout: "varlen", bytes, offsets: Int32Array.of(0, bytes.length) });
}

function numeric(value: string): FlatVector {
  const source = value.trim();
  const special = /^(?:NaN|[+-]?Infinity)$/iu.exec(source);
  if (special !== null)
    return vector(NUMERIC, {
      layout: "decimal",
      scale: 0,
      scales: null,
      coefficients: { repr: "safe", values: Float64Array.of(0) },
      specials: Uint8Array.of(/^nan$/iu.test(source) ? 1 : source.startsWith("-") ? 3 : 2),
    });
  const match = /^([+-]?)(\d*)(?:\.(\d*))?(?:e([+-]?\d+))?$/iu.exec(source);
  if (match === null || ((match[2] ?? "") === "" && (match[3] ?? "") === ""))
    throw new RangeError("numeric text expected");
  const fraction = match[3] ?? "";
  const digits = ((match[2] ?? "") + fraction).replace(/^0+(?=\d)/u, "");
  const exponent = Number(match[4] ?? 0);
  let scale = fraction.length - exponent;
  // PostgreSQL NUMERIC's representable digit/exponent limits. Refuse before constructing a
  // giant coefficient or power of ten; the typmod cast below applies rounding and precision.
  if (!Number.isSafeInteger(exponent) || scale > 16_383 || digits.length - scale > 131_072)
    throw new RangeError("numeric input is out of range");
  let coefficient = BigInt((match[1] === "-" ? "-" : "") + digits);
  if (scale < 0) {
    coefficient *= 10n ** BigInt(-scale);
    scale = 0;
  }
  return vector(NUMERIC, {
    layout: "decimal",
    scale,
    scales: null,
    coefficients: { repr: "wide", values: [coefficient] },
    specials: null,
  });
}

function date(value: string): number {
  if (/^\s*\+?infinity\s*$/iu.test(value)) return DATE_POS_INFINITY;
  if (/^\s*-infinity\s*$/iu.test(value)) return DATE_NEG_INFINITY;
  const m = /^(\d+)-(\d{2})-(\d{2})(?:\s+(BC))?$/iu.exec(value.trim());
  if (m === null) throw new RangeError("date text expected");
  const y = Number(m[1]);
  const year = m[4] === undefined ? y : 1 - y;
  const month = Number(m[2]),
    day = Number(m[3]);
  const days = daysFromCivil(year, month, day);
  const civil = civilFromDays(days);
  if (
    y === 0 ||
    !Number.isSafeInteger(y) ||
    month < 1 ||
    month > 12 ||
    day < 1 ||
    civil[0] !== year ||
    civil[1] !== month ||
    civil[2] !== day ||
    days < DATE_MIN ||
    days > DATE_MAX
  )
    throw new RangeError("date is out of range");
  return days;
}

function timestamp(value: string | Date, zoned: boolean): bigint {
  if (value instanceof Date) {
    const micros = timestampFromDate(value);
    if (micros < TIMESTAMP_MIN || micros >= TIMESTAMP_END)
      throw new RangeError("timestamp is out of range");
    return micros;
  }
  const source = value.trim();
  if (/^\+?infinity$/iu.test(source)) return TIMESTAMP_POS_INFINITY;
  if (/^-infinity$/iu.test(source)) return TIMESTAMP_NEG_INFINITY;
  let micros = parseTimestamp(source);
  if (micros === null) {
    // A date alone supplies midnight. Other offsets are normalized exactly in integer µs;
    // timestamp without time zone discards the written offset, as PostgreSQL does.
    const match =
      /^([+-]?\d{1,6})-(\d{2})-(\d{2})(?:[T ](\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?))?(Z|[+-]\d{2}(?::?\d{2})?)?(?:\s+(BC))?$/iu.exec(
        source,
      );
    if (match === null) throw new RangeError("timestamp text expected");
    const year = match[6] === undefined ? Number(match[1]) : 1 - Number(match[1]);
    const isoYear =
      year >= 0 && year <= 9999
        ? String(year).padStart(4, "0")
        : `${year < 0 ? "-" : "+"}${String(Math.abs(year)).padStart(6, "0")}`;
    micros = parseTimestamp(
      `${isoYear}-${String(match[2])}-${String(match[3])}T${match[4] ?? "00:00:00"}Z`,
    );
    if (micros === null) throw new RangeError("timestamp is out of range");
    const zone = match[5] ?? "Z";
    if (zone.toUpperCase() !== "Z") {
      const digits = zone.slice(1).replace(":", "");
      const hours = Number(digits.slice(0, 2)),
        minutes = Number(digits.slice(2) || 0);
      if (hours > 15 || minutes > 59) throw new RangeError("timestamp offset is out of range");
      if (zoned)
        micros -= BigInt((zone.startsWith("-") ? -1 : 1) * (hours * 60 + minutes)) * 60_000_000n;
    }
  }
  if (
    micros !== TIMESTAMP_NEG_INFINITY &&
    micros !== TIMESTAMP_POS_INFINITY &&
    (micros < TIMESTAMP_MIN || micros >= TIMESTAMP_END)
  )
    throw new RangeError("timestamp is out of range");
  return micros;
}

export function bindParameter(type: PhysicalType, value: ParameterValue, slot: number): FlatVector {
  checkParameterType(type);
  try {
    if (value === null) return nullParameter(type);
    const invalid = (): never => {
      throw new RangeError(`${type.kind} value expected`);
    };
    switch (type.kind) {
      case "bool":
        return typeof value === "boolean"
          ? vector(type, { layout: "bool", values: Uint8Array.of(value ? 1 : 0) })
          : invalid();
      case "int32":
      case "int64": {
        const integer =
          typeof value === "bigint"
            ? value
            : typeof value === "number" && Number.isSafeInteger(value)
              ? BigInt(value)
              : invalid();
        const min =
          type.kind === "int64" ? INT64_MIN : BigInt(type.bits === 16 ? INT16_MIN : INT32_MIN);
        const max =
          type.kind === "int64" ? INT64_MAX : BigInt(type.bits === 16 ? INT16_MAX : INT32_MAX);
        if (integer < min || integer > max) return invalid();
        return type.kind === "int64"
          ? clockParameter(type, integer)
          : vector(type, { layout: "i32", values: Int32Array.of(Number(integer)) });
      }
      case "float64":
      case "float32": {
        if (typeof value !== "number") return invalid();
        const source = vector(
          { kind: "float64" },
          { layout: "f64", values: Float64Array.of(value) },
        );
        return cast(type, source);
      }
      case "decimal": {
        if (
          typeof value !== "string" &&
          typeof value !== "bigint" &&
          (typeof value !== "number" || !Number.isFinite(value))
        )
          return invalid();
        return cast(type, numeric(String(value)));
      }
      case "date":
        return typeof value === "string"
          ? vector(type, { layout: "i32", values: Int32Array.of(date(value)) })
          : invalid();
      case "timestamp":
        return typeof value === "string" || value instanceof Date
          ? clockParameter(type, timestamp(value, type.zoned))
          : invalid();
      case "text":
        return typeof value === "string" ? text(value, type) : invalid();
      case "bytes": {
        if (!(value instanceof Uint8Array)) return invalid();
        const bytes = value.slice();
        return vector(type, { layout: "varlen", bytes, offsets: Int32Array.of(0, bytes.length) });
      }
      default:
        return invalid();
    }
  } catch (error) {
    throw new ParameterBindError(
      `$${String(slot + 1)}: ${error instanceof Error ? error.message : String(error)}`,
      slot,
    );
  }
}

function cast(type: PhysicalType, source: FlatVector): FlatVector {
  const arena = new OutputArena(UNCHARGED, 1);
  try {
    const compiled = compileCast(type, { type: source.type, evaluate: () => source }, arena);
    return flatOf(compiled.evaluate({ length: 1, columns: [], selection: null }, null));
  } finally {
    arena.close();
  }
}

export function bindParameters(
  plan: PhysicalPlan,
  values: readonly ParameterValue[],
): BoundParameters {
  if (values.length !== plan.placeholderCount)
    throw new ParameterBindError(
      `Expected ${String(plan.placeholderCount)} parameters, received ${String(values.length)}`,
      null,
    );
  return {
    slots: plan.parameters.map((type, slot) => {
      if (slot >= plan.placeholderCount) return nullParameter(type);
      const value = values[slot];
      if (value === undefined)
        throw new ParameterBindError(
          `$${String(slot + 1)}: undefined is not a parameter value`,
          slot,
        );
      return bindParameter(type, value, slot);
    }),
  };
}
