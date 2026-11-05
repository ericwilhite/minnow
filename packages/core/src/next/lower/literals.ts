/**
 * IR literals: a `literal` expression holds a flat vector of length 1 (ir.ts). Lowering builds
 * them only for values it knows exactly at plan time: booleans, integers, floats, exact decimals,
 * text, timestamps from the front end's `Date` constants, sequence ids, and NULL of any type.
 * Every other constant (a DATE, INTERVAL, UUID or JSON spelled as text) stays text and is cast by
 * the executor, which owns those parsers.
 */
import type { ScalarExpr } from "../ir/ir.js";
import { INT16_MAX, INT16_MIN, INT32_MAX, INT32_MIN, INT64_MAX, INT64_MIN } from "../types.js";
import type { PhysicalType } from "../types.js";
import { allocateValidity } from "../vector/validity.js";
import { allocateData, type FlatVector, type Int64Values } from "../vector/vector.js";

const encoder = new TextEncoder();

function flat(type: PhysicalType, data: FlatVector["data"]): FlatVector {
  return { form: "flat", type, length: 1, validity: null, data };
}

function int64Values(value: bigint): Int64Values {
  return Number.isSafeInteger(Number(value)) && BigInt(Number(value)) === value
    ? { repr: "safe", values: Float64Array.of(Number(value)) }
    : { repr: "int64", values: BigInt64Array.of(value) };
}

/** SQL NULL of `type`. */
export function nullLiteral(type: PhysicalType): ScalarExpr {
  const value: FlatVector = {
    form: "flat",
    type,
    length: 1,
    validity: allocateValidity(1),
    data: allocateData(type, 1),
  };
  return { op: "literal", value, type };
}

export function boolLiteral(value: boolean): ScalarExpr {
  const type: PhysicalType = { kind: "bool" };
  return {
    op: "literal",
    type,
    value: flat(type, { layout: "bool", values: Uint8Array.of(value ? 1 : 0) }),
  };
}

/**
 * An integer of an integer type, or null when it does not fit `type` (`int4_col = 5000000000`
 * cannot be an int4 key: lowering treats that as no match, never as a wrapped value).
 */
export function integerLiteral(value: bigint, type: PhysicalType): ScalarExpr | null {
  if (type.kind === "int32") {
    const [min, max] = type.bits === 16 ? [INT16_MIN, INT16_MAX] : [INT32_MIN, INT32_MAX];
    if (value < BigInt(min) || value > BigInt(max)) return null;
    return {
      op: "literal",
      type,
      value: flat(type, { layout: "i32", values: Int32Array.of(Number(value)) }),
    };
  }
  if (type.kind === "int64") {
    if (value < INT64_MIN || value > INT64_MAX) return null;
    return {
      op: "literal",
      type,
      value: flat(type, { layout: "int64", values: int64Values(value) }),
    };
  }
  if (type.kind === "decimal") return decimalLiteral(value, 0, type);
  return null;
}

export function floatLiteral(value: number, type: PhysicalType): ScalarExpr {
  if (type.kind === "float32")
    return {
      op: "literal",
      type,
      value: flat(type, { layout: "f32", values: Float32Array.of(value) }),
    };
  if (type.kind !== "float64") throw new TypeError(`Not a float type: ${type.kind}`);
  return {
    op: "literal",
    type,
    value: flat(type, { layout: "f64", values: Float64Array.of(value) }),
  };
}

/**
 * An exact decimal `coefficient × 10^−scale` with display scale `scale`, of `type` (a decimal
 * type; unconstrained when omitted). Coefficients past int64 use the wide tier.
 */
export function decimalLiteral(
  coefficient: bigint,
  scale: number,
  type: PhysicalType = { kind: "decimal", precision: null, scale: null },
): ScalarExpr {
  const coefficients =
    coefficient >= INT64_MIN && coefficient <= INT64_MAX
      ? int64Values(coefficient)
      : { repr: "wide" as const, values: [coefficient] };
  const fixed = type.kind === "decimal" && type.scale !== null && type.scale === scale;
  return {
    op: "literal",
    type,
    value: flat(type, {
      layout: "decimal",
      scale: fixed ? scale : null,
      scales: fixed ? null : Uint16Array.of(scale),
      coefficients,
      specials: null,
    }),
  };
}

export function textLiteral(value: string, type: PhysicalType = { kind: "text" }): ScalarExpr {
  const bytes = encoder.encode(value);
  return {
    op: "literal",
    type,
    value: flat(type, { layout: "varlen", bytes, offsets: Int32Array.of(0, bytes.length) }),
  };
}

/** A timestamp in µs since 2000-01-01 (temporal.ts). */
export function timestampLiteral(micros: bigint, zoned: boolean): ScalarExpr {
  const type: PhysicalType = { kind: "timestamp", zoned };
  return {
    op: "literal",
    type,
    value: flat(type, { layout: "int64", values: int64Values(micros) }),
  };
}

/** Exact decimal text (`-12.50`, `1e3`, `.5`) as coefficient and display scale, or null. */
export function parseDecimalText(text: string): { coefficient: bigint; scale: number } | null {
  const match = /^([+-]?)(\d*)(?:\.(\d*))?(?:[eE]([+-]?\d+))?$/u.exec(text.trim());
  if (match === null) return null;
  const [, sign = "", whole = "", fraction = "", exponentText] = match;
  if (whole === "" && fraction === "") return null;
  const exponent = exponentText === undefined ? 0 : Number(exponentText);
  let digits = `${whole}${fraction}`.replace(/^0+(?=\d)/u, "");
  let scale = fraction.length - exponent;
  if (scale < 0) {
    digits += "0".repeat(-scale);
    scale = 0;
  }
  const coefficient = BigInt(digits === "" ? "0" : digits);
  return { coefficient: sign === "-" ? -coefficient : coefficient, scale };
}

/** The value of an integer-typed literal, or null for NULL or a non-integer literal. */
export function integerLiteralValue(expr: ScalarExpr): bigint | null {
  if (expr.op !== "literal" || expr.value.validity !== null) return null;
  const data = expr.value.data;
  if (data.layout === "i32") return BigInt(data.values[0] ?? 0);
  if (data.layout === "int64")
    return data.values.repr === "safe"
      ? BigInt(data.values.values[0] ?? 0)
      : (data.values.values[0] ?? 0n);
  return null;
}

/** Whether `expr` is a NULL literal. */
export function isNullLiteral(expr: ScalarExpr): boolean {
  return (
    expr.op === "literal" && expr.value.validity !== null && (expr.value.validity[0] ?? 0) === 0
  );
}
