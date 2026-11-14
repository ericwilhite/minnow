/** PostgreSQL array_out, shared by casts and renderer2. Borrow payloads and stream
 * syntax; never retain a decoded element list or a growing JavaScript string. */
import type { PhysicalType } from "../types.js";
import { formatDate } from "../temporal.js";
import { formatSqlTimestamp, formatSqlInterval, formatSqlTime } from "../temporal-text.js";
import {
  arrayValueElements,
  isArrayDecodeScratch,
  type ArrayElement,
} from "../vector/array-value.js";
import type { SlicedJob } from "./slices.js";
import { JsonOutput } from "./kernels/json-output.js";
import type { JsonMemory } from "./kernels/json.js";
import { jsonAscii, decimalAscii } from "./kernels/json-value.js";
import { jsonFloatText } from "./kernels/json-float.js";
const HEX = "0123456789abcdef";
export function sqlUuidText(bytes: Uint8Array): string {
  let text = "";
  for (let i = 0; i < 16; i++) {
    if (i === 4 || i === 6 || i === 8 || i === 10) text += "-";
    const n = bytes[i] ?? 0;
    text += HEX.charAt(n >>> 4) + HEX.charAt(n & 15);
  }
  return text;
}
/** These fields have a bounded output and bounded conversion scratch. NUMERIC,
 * variable text/bytes and array output use their streaming paths below. */
export function sqlScalarText(value: ArrayElement, type: PhysicalType, array = false): string {
  if (typeof value === "boolean") return array ? (value ? "t" : "f") : value ? "true" : "false";
  if (value instanceof Uint8Array && type.kind === "uuid") return sqlUuidText(value);
  if (typeof value === "object" && "months" in value)
    return formatSqlInterval(value.months, value.days, value.micros);
  if (type.kind === "date") return formatDate(Number(value));
  if (type.kind === "timestamp") return formatSqlTimestamp(value as bigint, type.zoned);
  if (type.kind === "time") return formatSqlTime(value as bigint);
  if (type.kind === "float32" || type.kind === "float64")
    return jsonFloatText(Number(value), type.kind === "float32");
  if (typeof value === "bigint" || typeof value === "number") return String(value);
  throw new TypeError("Expected a fixed SQL output field");
}
/** Only the six ASCII whitespace characters are array syntax, not Unicode space. */
function quoteByte(c: number): boolean {
  return (
    c === 123 || c === 125 || c === 44 || c === 34 || c === 92 || c === 32 || (c >= 9 && c <= 13)
  );
}
function* arrayString(bytes: Uint8Array, out: JsonOutput): SlicedJob<void> {
  let quote =
    bytes.length === 0 ||
    (bytes.length === 4 &&
      ((bytes[0] ?? 0) | 32) === 110 &&
      ((bytes[1] ?? 0) | 32) === 117 &&
      ((bytes[2] ?? 0) | 32) === 108 &&
      ((bytes[3] ?? 0) | 32) === 108);
  for (let i = 0; !quote && i < bytes.length; i++) {
    quote = quoteByte(bytes[i] ?? 0);
    if ((i & 1023) === 0 && out.slice.due()) yield;
  }
  if (!quote) {
    yield* out.copy(bytes);
    return;
  }
  out.put(34);
  let start = 0;
  for (let i = 0; i < bytes.length; i++) {
    if (bytes[i] === 34 || bytes[i] === 92) {
      yield* out.copy(bytes.subarray(start, i));
      out.put(92);
      start = i;
    }
    if ((i & 1023) === 0 && out.slice.due()) yield;
  }
  yield* out.copy(bytes.subarray(start));
  out.put(34);
}
export function* sqlHex(bytes: Uint8Array, out: JsonOutput, array = false): SlicedJob<void> {
  if (array) {
    out.put(34);
    out.put(92);
  }
  out.put(92);
  out.put(120);
  for (let i = 0; i < bytes.length; i++) {
    const n = bytes[i] ?? 0;
    out.put(HEX.charCodeAt(n >>> 4));
    out.put(HEX.charCodeAt(n & 15));
    if ((i & 1023) === 0 && out.slice.due()) yield;
  }
  if (array) out.put(34);
}
export interface ArrayTextGuards {
  begin?(count: number): void;
  beforeBytes?(value: Uint8Array): void;
}
export function* sqlArrayText(
  element: PhysicalType,
  bytes: Uint8Array,
  out: JsonOutput,
  memory: JsonMemory,
  guards: ArrayTextGuards = {},
): SlicedJob<void> {
  let dimensions: readonly number[] = [],
    lowerBounds: readonly number[] = [];
  const values = arrayValueElements(element, bytes, {
    begin(shape, count) {
      dimensions = shape.dimensions;
      lowerBounds = shape.lowerBounds;
      guards.begin?.(count);
    },
    ...(guards.beforeBytes
      ? { beforeBytes: (value: Uint8Array) => guards.beforeBytes?.(value) }
      : {}),
  });
  let step = values.next();
  function* scalar(): SlicedJob<void> {
    let scratch = 0;
    try {
      while (!step.done && isArrayDecodeScratch(step.value)) {
        yield* memory.take(step.value.scratchBytes);
        scratch += step.value.scratchBytes;
        if (out.slice.due()) yield;
        step = values.next();
      }
      if (step.done || isArrayDecodeScratch(step.value))
        throw new TypeError("Missing array element");
      const value = step.value;
      if (value === null) yield* jsonAscii("NULL", out);
      else if (value instanceof Uint8Array) {
        if (element.kind === "uuid") yield* jsonAscii(sqlUuidText(value), out);
        else if (element.kind === "bytes") yield* sqlHex(value, out, true);
        else yield* arrayString(value, out);
      } else if (typeof value === "object" && "coefficient" in value) {
        if (value.special) yield* jsonAscii(value.special, out);
        else yield* decimalAscii(value.coefficient, value.scale, out);
      } else yield* arrayString(new TextEncoder().encode(sqlScalarText(value, element, true)), out);
    } finally {
      memory.free(scratch);
    }
    step = values.next();
  }
  function* nested(d: number): SlicedJob<void> {
    out.put(123);
    for (let i = 0; i < (dimensions[d] ?? 0); i++) {
      if (i) out.put(44);
      if (d + 1 === dimensions.length) yield* scalar();
      else yield* nested(d + 1);
      if ((i & 255) === 0 && out.slice.due()) yield;
    }
    out.put(125);
  }
  try {
    if (lowerBounds.some((n) => n !== 1)) {
      for (const [i, n] of dimensions.entries())
        yield* jsonAscii(
          `[${String(lowerBounds[i])}:${String((lowerBounds[i] ?? 1) + n - 1)}]`,
          out,
        );
      out.put(61);
    }
    yield* nested(0);
    if (!step.done) throw new TypeError("Extra array elements");
  } finally {
    values.return();
  }
}
