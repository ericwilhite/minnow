/** Client-side decoding of native scalar values (DECISIONS D1/D2 and C1–C6).
 * No executor implementation, Date conversion, or lossy NUMERIC/INT8 coercion. */
import type { PhysicalType } from "../types.js";
import type { Int64Values, VectorData } from "../vector/vector.js";
import { int128At } from "../vector/int128.js";
import { decodeArrayValue, type ArrayElement } from "../vector/array-value.js";
import { formatDate, formatTimestamp } from "../temporal.js";
import { formatSqlInterval, formatSqlTime } from "../temporal-text.js";
import type { Value } from "./api.js";
import { ProtocolError, type WireColumnMeta } from "./protocol.js";
const utf8 = new TextDecoder();
export const bigAt = (values: Int64Values, row: number): bigint =>
  values.repr === "safe" ? BigInt(required(values.values[row])) : required(values.values[row]);
export function decimalText(coefficient: bigint, scale: number): string {
  const sign = coefficient < 0n ? "-" : "";
  const digits = (coefficient < 0n ? -coefficient : coefficient).toString();
  if (scale === 0) return sign + digits;
  const padded = digits.padStart(scale + 1, "0");
  return `${sign}${padded.slice(0, -scale)}.${padded.slice(-scale)}`;
}
export function coefficientAt(
  data: Extract<
    VectorData,
    {
      layout: "decimal";
    }
  >,
  row: number,
) {
  const c = data.coefficients;
  return c.repr === "int128"
    ? int128At(c.limbs, row)
    : c.repr === "wide"
      ? required(c.values[row])
      : bigAt(c, row);
}
function integer(value: bigint, render: WireColumnMeta["render"]): number | bigint {
  if (render !== "number") return value;
  const number = Number(value);
  if (!Number.isSafeInteger(number))
    throw new RangeError("Integer result exceeds JavaScript's exact range; use ::bigint");
  return number;
}
function uuid(bytes: Uint8Array): string {
  let text = "";
  for (let i = 0; i < 16; i++) {
    if (i === 4 || i === 6 || i === 8 || i === 10) text += "-";
    text += required(bytes[i]).toString(16).padStart(2, "0");
  }
  return text;
}
function arrayElement(type: PhysicalType, value: ArrayElement | null): Value {
  if (value === null) return null;
  if (typeof value === "object" && !(value instanceof Uint8Array)) {
    if ("coefficient" in value) return value.special ?? decimalText(value.coefficient, value.scale);
    return formatSqlInterval(value.months, value.days, value.micros);
  }
  if (value instanceof Uint8Array) {
    if (type.kind === "uuid") return uuid(value);
    return type.kind === "bytes" ? value : utf8.decode(value);
  }
  if (type.kind === "date") return formatDate(value as number);
  if (type.kind === "timestamp") return formatTimestamp(value as bigint, type.zoned);
  if (type.kind === "time") return formatSqlTime(value as bigint);
  return value;
}
function array(type: PhysicalType, bytes: Uint8Array): readonly Value[] {
  const decoded = decodeArrayValue(type, bytes);
  let at = 0;
  const dimension = (depth: number): Value[] => {
    const rows: Value[] = [];
    for (let i = 0; i < required(decoded.dimensions[depth]); i++)
      rows.push(
        depth + 1 === decoded.dimensions.length
          ? arrayElement(type, decoded.elements[at++] ?? null)
          : dimension(depth + 1),
      );
    return rows;
  };
  // JS arrays are zero-based; exact SQL bounds remain in column()'s C9 encoded bytes.
  return decoded.dimensions.length === 0 ? [] : dimension(0);
}
/** The caller has tested validity; invalid payloads are deliberately never interpreted. */
export function scalarValue(meta: WireColumnMeta, data: VectorData, row: number): Value {
  const type = meta.type;
  switch (data.layout) {
    case "bool":
      return data.values[row] === 1;
    case "i32":
      return type.kind === "date"
        ? formatDate(required(data.values[row]))
        : required(data.values[row]);
    case "f64":
    case "f32":
      return required(data.values[row]);
    case "int64": {
      const value = bigAt(data.values, row);
      if (type.kind === "timestamp") return formatTimestamp(value, type.zoned);
      if (type.kind === "time") return formatSqlTime(value);
      return integer(value, meta.render);
    }
    case "decimal": {
      const special = data.specials?.[row] ?? 0;
      if (special !== 0) return special === 1 ? "NaN" : special === 2 ? "Infinity" : "-Infinity";
      const value = coefficientAt(data, row),
        scale = data.scale ?? required(required(data.scales)[row]);
      if (meta.render === "bigint") {
        const divisor = 10n ** BigInt(scale);
        if (value % divisor !== 0n) throw new RangeError("A bigint result has a fractional value");
        return value / divisor;
      }
      return decimalText(value, scale);
    }
    case "varlen": {
      const bytes = data.bytes.subarray(
        required(data.offsets[row]),
        required(data.offsets[row + 1]),
      );
      if (type.kind === "bytes") return bytes;
      if (type.kind === "array") return array(type.element, bytes);
      return utf8.decode(bytes);
    }
    case "fixed16":
      return uuid(data.bytes.subarray(row * 16, (row + 1) * 16));
    case "interval":
      return formatSqlInterval(
        required(data.months[row]),
        required(data.days[row]),
        bigAt(data.micros, row),
      );
  }
}

/** Missing wire fields are protocol errors; SQL NULL slots are tested before reading. */
export function required<T>(value: T | null | undefined): T {
  if (value === null || value === undefined) throw new ProtocolError("A result field is missing");
  return value;
}
