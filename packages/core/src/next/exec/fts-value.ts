/** Version-2 native FTS logical rendering; never tokenize encoded vector bytes or dictionary codes. */
import { MAX_INDEXED_STRING_CHARACTERS, renderDocumentValue } from "../full-text.js";
import { formatDate, formatTimestamp, unixMillisFromTimestamp } from "../temporal.js";
import { decodeArrayValue, type ArrayElement } from "../vector/array-value.js";
import type { PhysicalType } from "../types.js";
import { jsonFloatText } from "./kernels/json-float.js";
import { formatSqlTimestamp, formatSqlInterval } from "../temporal-text.js";
import { int64At } from "../vector/int64.js";
import { decodeUtf8 } from "../vector/varlen.js";
import type { Vector } from "../vector/vector.js";
import { coefficientAt, scaleAt } from "./kernels/numeric.js";
import { flatOf, slotIndex, isValidSlot } from "./kernels/vectors.js";

/** NFKC expands a scalar by at most 18 code units (Unicode normalization conformance);
 * default lowercase by at most 3. Account input + both strings + regex slice + 4096
 * bounded token objects/strings. emitRun streams code points, never Array.from(normalized).
 * One arena shares this scratch across synchronous cells/nodes, not per input row. */
export function ftsWorkspaceBytes(inputBytes: number): number {
  return 64 * 1024 + 512 * Math.min(16384, Math.max(0, inputBytes));
}
export const FTS_WORKSPACE_BYTES = ftsWorkspaceBytes(16384);
/** Numeric scale can render many zeros from a tiny coefficient; encoded bytes alone do
 * not bound its text. Arrays of numerics keep one bounded rendered document as well. */
export function ftsDeclaredBytes(type: PhysicalType): number {
  if (type.kind === "array") return type.element.kind === "decimal" ? 16384 : 0;
  if (type.kind !== "decimal") return 0;
  return type.precision === null || type.scale === null
    ? 16384
    : Math.max(type.precision, Math.abs(type.scale)) + 3;
}

function decimalText(coefficient: bigint, scale: number): string {
  // Legacy NUMERIC stores canonical value text, not the declared output padding.
  while (scale > 0 && coefficient % 10n === 0n) {
    coefficient /= 10n;
    scale--;
  }
  const negative = coefficient < 0n;
  const digits = String(negative ? -coefficient : coefficient).padStart(scale + 1, "0");
  return (
    (negative ? "-" : "") + (scale ? digits.slice(0, -scale) + "." + digits.slice(-scale) : digits)
  );
}

export function ftsTimeText(value: bigint): string {
  const seconds = value / 1_000_000n,
    fraction = value % 1_000_000n;
  return `${String(seconds / 3600n).padStart(2, "0")}:${String((seconds / 60n) % 60n).padStart(2, "0")}:${String(seconds % 60n).padStart(2, "0")}${fraction ? "." + String(fraction).padStart(6, "0").replace(/0+$/u, "") : ""}`;
}
function arrayElement(value: ArrayElement | null, type: PhysicalType): string {
  if (value === null) return "NULL";
  let text: string;
  if (value instanceof Uint8Array) {
    if (type.kind === "bytes") text = hexText(value);
    else if (type.kind === "uuid") text = uuidText(value, 0);
    else text = new TextDecoder().decode(value);
  } else if (typeof value === "object") {
    if ("coefficient" in value) {
      const negative = value.coefficient < 0n;
      const digits = String(negative ? -value.coefficient : value.coefficient).padStart(
        value.scale + 1,
        "0",
      );
      text =
        value.special ??
        (negative ? "-" : "") +
          (value.scale ? digits.slice(0, -value.scale) + "." + digits.slice(-value.scale) : digits);
    } else text = formatSqlInterval(value.months, value.days, value.micros);
  } else if (type.kind === "date") text = formatDate(Number(value));
  else if (type.kind === "timestamp") text = formatSqlTimestamp(BigInt(value), type.zoned);
  else if (type.kind === "time") text = ftsTimeText(BigInt(value));
  else if (type.kind === "bool") text = value ? "t" : "f";
  else if (type.kind === "float64" || type.kind === "float32")
    text = jsonFloatText(Number(value), type.kind === "float32");
  else text = String(value);
  return text === "" || /^null$/iu.test(text) || /[{},"\\\s]/u.test(text)
    ? '"' + text.replace(/["\\]/gu, (value) => "\\" + value) + '"'
    : text;
}
function arrayText(bytes: Uint8Array, element: PhysicalType): string {
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  const dimensions = bytes[2] ?? 0;
  let count = dimensions ? 1 : 0;
  for (let d = 0; d < dimensions; d++) count *= view.getInt32(4 + 8 * d, true);
  if (count > 8192 || bytes.length > 65536)
    throw new RangeError("Full-text values cannot exceed 16384 characters");
  const value = decodeArrayValue(element, bytes);
  let at = 0,
    produced = 0;
  const account = (text: string): string => {
    produced += text.length;
    if (produced > MAX_INDEXED_STRING_CHARACTERS)
      throw new RangeError("Full-text values cannot exceed 16384 characters");
    return text;
  };
  const nested = (dimension: number): string => {
    const length = value.dimensions[dimension] ?? 0;
    const parts = new Array<string>(length);
    for (let i = 0; i < length; i++)
      parts[i] =
        dimension + 1 === value.dimensions.length
          ? account(arrayElement(value.elements[at++] ?? null, element))
          : nested(dimension + 1);
    account("{" + ",".repeat(Math.max(0, length - 1)) + "}");
    return "{" + parts.join(",") + "}";
  };
  const bounds = value.lowerBounds.some((bound) => bound !== 1)
    ? value.dimensions
        .map(
          (length, d) =>
            `[${String(value.lowerBounds[d])}:${String((value.lowerBounds[d] ?? 1) + length - 1)}]`,
        )
        .join("") + "="
    : "";
  account(bounds);
  return bounds + nested(0);
}
function hexText(bytes: Uint8Array): string {
  let text = "\\x";
  for (const byte of bytes) text += byte.toString(16).padStart(2, "0");
  return text;
}
function uuidText(bytes: Uint8Array, offset: number): string {
  let text = "";
  for (let i = 0; i < 16; i++) {
    if (i === 4 || i === 6 || i === 8 || i === 10) text += "-";
    text += (bytes[offset + i] ?? 0).toString(16).padStart(2, "0");
  }
  return text;
}

export function ftsValue(vector: Vector, row: number): string | undefined {
  if (!isValidSlot(vector, row) || vector.type.kind === "bool") return undefined;
  const data = flatOf(vector).data,
    slot = slotIndex(vector, row);
  switch (data.layout) {
    case "i32":
      return vector.type.kind === "date"
        ? formatDate(data.values[slot] ?? 0)
        : String(data.values[slot]);
    case "f64":
    case "f32":
      return String(data.values[slot]);
    case "int64": {
      const value = int64At(data.values, slot);
      if (vector.type.kind === "timestamp") {
        const date = new Date(unixMillisFromTimestamp(value));
        // Common 0.x Date values keep the exact intrinsic ISO spelling. Native-only
        // sub-ms / extended-range / infinite values retain the full native ISO value.
        return value % 1000n === 0n && Number.isFinite(date.getTime())
          ? renderDocumentValue(date)
          : formatTimestamp(value, vector.type.zoned);
      }
      if (vector.type.kind === "time") return ftsTimeText(value);
      return String(value);
    }
    case "decimal": {
      const special = data.specials?.[slot] ?? 0;
      if (special) return special === 1 ? "NaN" : special === 2 ? "Infinity" : "-Infinity";
      return decimalText(coefficientAt(data, slot), scaleAt(data, slot));
    }
    case "varlen": {
      if (vector.type.kind === "array")
        return arrayText(
          data.bytes.subarray(data.offsets[slot] ?? 0, data.offsets[slot + 1] ?? 0),
          vector.type.element,
        );
      if (vector.type.kind === "bytes") {
        const start = data.offsets[slot] ?? 0,
          end = data.offsets[slot + 1] ?? 0;
        if (end - start > 8191)
          throw new RangeError("Full-text values cannot exceed 16384 characters");
        return hexText(data.bytes.subarray(start, end));
      }
      const bytes = (data.offsets[slot + 1] ?? 0) - (data.offsets[slot] ?? 0);
      if (bytes > 4 * MAX_INDEXED_STRING_CHARACTERS)
        throw new RangeError("Full-text values cannot exceed 16384 characters");
      return decodeUtf8(data, slot);
    }
    case "interval":
      return `${String(data.months[slot])} mons ${String(data.days[slot])} days ${String(int64At(data.micros, slot))} usecs`;
    case "bool":
      return undefined;
    case "fixed16":
      return uuidText(data.bytes, 16 * slot);
  }
}
