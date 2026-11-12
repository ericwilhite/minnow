/** Typed PostgreSQL JSON output, streamed into a counted/prepaid sink. */
import type { Vector } from "../../vector/vector.js";
import { int64At } from "../../vector/int64.js";
import { bigintBytes } from "../../memory/governor.js";
import { formatDate } from "../../temporal.js";
import { formatSqlTimestamp, formatSqlInterval, isInfiniteInterval } from "../../temporal-text.js";
import type { Slice, SlicedJob } from "../slices.js";
import { ExecutionError, UnsupportedPlanError } from "../errors.js";
import { coefficientAt, scaleAt, specialAt } from "./numeric.js";
import { isValidSlot, slotOf } from "./vectors.js";
import type { JsonMemory } from "./json.js";
import { JsonOutput } from "./json-output.js";
import { jsonFloatText } from "./json-float.js";

export const JSON_VALUE_KINDS = new Set([
  "bool",
  "int32",
  "int64",
  "float32",
  "float64",
  "decimal",
  "text",
  "enum",
  "json",
  "date",
  "timestamp",
  "interval",
]);
const HEX = "0123456789abcdef";
/** UTF8 is already valid; escape only JSON's ASCII syntax/control bytes. */
export function* quotedJson(bytes: Uint8Array, out: JsonOutput): SlicedJob<void> {
  out.put(34);
  let start = 0;
  for (let at = 0; at < bytes.length; at++) {
    const c = bytes[at] ?? 0;
    if (c === 34 || c === 92 || c < 32) {
      yield* out.copy(bytes.subarray(start, at));
      out.put(92);
      const escape =
        c === 8 ? 98 : c === 9 ? 116 : c === 10 ? 110 : c === 12 ? 102 : c === 13 ? 114 : c;
      if (c < 32 && escape === c) {
        out.put(117);
        out.put(48);
        out.put(48);
        out.put(HEX.charCodeAt(c >>> 4));
        out.put(HEX.charCodeAt(c & 15));
      } else out.put(escape);
      start = at + 1;
    }
    if ((at & 1023) === 0 && out.slice.due()) yield;
  }
  yield* out.copy(bytes.subarray(start));
  out.put(34);
}
export function* jsonAscii(text: string, out: JsonOutput): SlicedJob<void> {
  for (let i = 0; i < text.length; i++) {
    out.put(text.charCodeAt(i));
    if ((i & 1023) === 0 && out.slice.due()) yield;
  }
}
/** SQL keys are scalar output strings; JSON/arrays/records cannot be keys (22023). */
export function* jsonValue(
  vector: Vector,
  row: number,
  out: JsonOutput,
  memory: JsonMemory,
  slice: Slice,
  key = false,
  rawKey = false,
): SlicedJob<void> {
  if (!isValidSlot(vector, row)) {
    if (key)
      throw new ExecutionError(
        "null value not allowed for object key",
        "MINNOW_JSON_KEY_NULL",
        "22004",
      );
    yield* jsonAscii("null", out);
    return;
  }
  const [flat, slot] = slotOf(vector, row),
    data = flat.data,
    type = vector.type;
  if (data.layout === "varlen") {
    const bytes = data.bytes.subarray(data.offsets[slot] ?? 0, data.offsets[slot + 1] ?? 0);
    if (type.kind === "json") {
      if (key)
        throw new ExecutionError(
          "key value must be scalar, not array, composite, or json",
          "MINNOW_JSON_KEY_TYPE",
          "22023",
        );
      yield* out.copy(bytes);
    } else if (rawKey) yield* out.copy(bytes);
    else yield* quotedJson(bytes, out);
    return;
  }
  let scratch = 0;
  if (data.layout === "decimal") {
    // Same magnitude accounting as governor/parameter copies, prepaid BEFORE decimal strings.
    scratch = 8 * bigintBytes(coefficientAt(data, slot)) + 128;
    yield* memory.take(scratch);
  }
  try {
    let text: string,
      quote = key;
    switch (type.kind) {
      case "bool":
        if (data.layout !== "bool") throw new TypeError("Expected bool");
        text = data.values[slot] ? "true" : "false";
        break;
      case "int32":
        if (data.layout !== "i32") throw new TypeError("Expected int4");
        text = String(data.values[slot] ?? 0);
        break;
      case "int64":
        if (data.layout !== "int64") throw new TypeError("Expected int8");
        text = String(int64At(data.values, slot));
        break;
      case "float32":
      case "float64": {
        if (data.layout !== "f64" && data.layout !== "f32") throw new TypeError("Expected float");
        const value = data.values[slot] ?? 0;
        text = jsonFloatText(value, type.kind === "float32");
        quote ||= !Number.isFinite(value);
        break;
      }
      case "decimal": {
        if (data.layout !== "decimal") throw new TypeError("Expected numeric");
        const special = specialAt(data, slot);
        if (special) {
          text = special === 1 ? "NaN" : special === 2 ? "Infinity" : "-Infinity";
          quote = true;
          break;
        }
        const coefficient = coefficientAt(data, slot),
          scale = scaleAt(data, slot),
          digits = coefficient.toString();
        const negative = coefficient < 0n,
          start = negative ? 1 : 0,
          integral = digits.length - start - scale;
        if (key && !rawKey) out.put(34);
        if (negative) out.put(45);
        if (integral <= 0) out.put(48);
        for (let i = 0; i < Math.max(integral, 0); i++) {
          out.put(start + i >= digits.length ? 48 : digits.charCodeAt(start + i));
          if ((i & 1023) === 0 && slice.due()) yield;
        }
        if (scale > 0) {
          out.put(46);
          for (let i = 0; i < scale; i++) {
            out.put(integral + i < 0 ? 48 : digits.charCodeAt(start + integral + i));
            if ((i & 1023) === 0 && slice.due()) yield;
          }
        }
        if (key && !rawKey) out.put(34);
        return;
      }
      case "date":
        if (data.layout !== "i32") throw new TypeError("Expected date");
        text = formatDate(data.values[slot] ?? 0);
        quote = true;
        break;
      case "timestamp":
        if (data.layout !== "int64") throw new TypeError("Expected timestamp");
        text = formatSqlTimestamp(int64At(data.values, slot), type.zoned).replace(" ", "T");
        if (type.zoned) text = text.replace(/\+00(?= BC|$)/u, "+00:00");
        quote = true;
        break;
      case "interval":
        if (data.layout !== "interval") throw new TypeError("Expected interval");
        if (
          isInfiniteInterval(
            data.months[slot] ?? 0,
            data.days[slot] ?? 0,
            int64At(data.micros, slot),
          )
        )
          throw new UnsupportedPlanError("infinite INTERVAL JSON construction", {
            construct: "function",
          });
        text = formatSqlInterval(
          data.months[slot] ?? 0,
          data.days[slot] ?? 0,
          int64At(data.micros, slot),
        );
        quote = true;
        break;
      default:
        throw new UnsupportedPlanError("JSON value type " + type.kind, { construct: "function" });
    }
    if (rawKey) quote = false;
    if (quote) out.put(34);
    yield* jsonAscii(text, out);
    if (quote) out.put(34);
  } finally {
    if (scratch) memory.free(scratch);
  }
}
