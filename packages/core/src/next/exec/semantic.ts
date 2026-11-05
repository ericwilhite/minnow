/**
 * Semantic ordering and equality for the types that have no persisted key (keys/key-encoding.ts
 * `indexKeyKind`): SQL arrays and JSONB. Sorting, grouping, DISTINCT and joins on these use these
 * functions at query time, never byte encodings. Each comparator returns −1, 0 or 1, and two
 * values compare 0 exactly when their equality keys (`*EqualityKey`) are equal, so hashing the
 * equality key agrees with the comparator.
 *
 * ## JSONB (PostgreSQL's `compareJsonbContainers`, verified against PGlite)
 * Both values are walked in storage order as token streams (begin/end of arrays and objects,
 * keys, values, elements; a top-level scalar is a one-element "raw scalar" array) and compared
 * token by token:
 * - Same token, same type: scalars compare as values: null = null, strings (and keys) by bytes
 *   (C collation), numbers numerically and exactly (`1` = `1.0`, `2` < `10`), false < true;
 *   arrays: a raw scalar sorts below a real array, but a different element count decides first;
 *   objects: more pairs sort higher.
 * - Otherwise: by type, Null < String < Number < Boolean < Array < Object.
 * Object keys are visited in storage order (shorter first, then bytes), but two keys met at the
 * same position compare lexically: `{"b":0}` > `{"aa":0}`. Consequences: `[]` < every scalar,
 * `1` < `[1]`.
 *
 * `parseJsonb` accepts exactly PostgreSQL's JSON: whitespace is space, tab, LF, CR only; numbers
 * have no leading zeros, `+`, bare `.` or trailing `.`, and fit NUMERIC (|exponent| ≤ 2^30 − 1,
 * display scale ≤ 16,383, ≤ 131,072 digits before the point); strings have no raw control characters,
 * no `\u0000`, and no unpaired surrogate (escaped or raw).
 *
 * ## SQL arrays (PostgreSQL's array_cmp)
 * Element by element over the shorter array (null elements sort above non-null ones and equal
 * each other), then fewer elements < more, then fewer dimensions, then dimension lengths, then
 * lower bounds. Elements must be scalar key types; arrays of JSON are refused.
 */
import {
  KeyWriter,
  encodeEqualityKey,
  encodeSortKey,
  type DecimalKey,
  type KeyValue,
} from "../keys/key-encoding.js";
import type { PhysicalType } from "../types.js";

export interface JsonbNumber {
  readonly number: DecimalKey;
}
export interface JsonbObject {
  /** Unique keys, sorted by (UTF-8 length, bytes): PostgreSQL's storage order. */
  readonly entries: ReadonlyArray<readonly [string, JsonbValue]>;
}
export type JsonbValue =
  null | boolean | string | JsonbNumber | readonly JsonbValue[] | JsonbObject;

const encoder = new TextEncoder();

function compareCodePoints(a: string, b: string): number {
  const length = Math.min(a.length, b.length);
  for (let index = 0; index < length;) {
    const x = a.codePointAt(index) ?? 0;
    const y = b.codePointAt(index) ?? 0;
    if (x !== y) return x < y ? -1 : 1;
    index += x > 0xffff ? 2 : 1;
  }
  return a.length === b.length ? 0 : a.length < b.length ? -1 : 1;
}

function utf8Length(text: string): number {
  return encoder.encode(text).length;
}

/** PostgreSQL's jsonb key storage order: shorter UTF-8 first, then bytes. */
function compareKeys(a: string, b: string): number {
  const difference = utf8Length(a) - utf8Length(b);
  return difference !== 0 ? Math.sign(difference) : compareCodePoints(a, b);
}

function compareDecimals(a: DecimalKey, b: DecimalKey): number {
  const scale = Math.max(a.scale, b.scale);
  const x = a.coefficient * 10n ** BigInt(scale - a.scale);
  const y = b.coefficient * 10n ** BigInt(scale - b.scale);
  return x === y ? 0 : x < y ? -1 : 1;
}

/** PostgreSQL's jbvType order. */
const JsonbType = { Null: 0, String: 1, Numeric: 2, Bool: 3, Array: 16, Object: 17 } as const;
type JsonbType = (typeof JsonbType)[keyof typeof JsonbType];

function typeOf(value: JsonbValue): JsonbType {
  if (value === null) return JsonbType.Null;
  if (typeof value === "string") return JsonbType.String;
  if (typeof value === "boolean") return JsonbType.Bool;
  if (Array.isArray(value)) return JsonbType.Array;
  return "number" in (value as object) ? JsonbType.Numeric : JsonbType.Object;
}

type Token =
  | { readonly token: "beginArray"; readonly raw: boolean; readonly count: number }
  | { readonly token: "beginObject"; readonly count: number }
  | { readonly token: "endArray" | "endObject" }
  | { readonly token: "key"; readonly value: string }
  | { readonly token: "value" | "element"; readonly value: JsonbValue };

/** PostgreSQL's iterator order: storage order, scalars inline, containers opened and closed. */
function tokens(value: JsonbValue): Token[] {
  const out: Token[] = [];
  const walk = (item: JsonbValue, position: "value" | "element") => {
    const type = typeOf(item);
    if (type === JsonbType.Array) {
      const items = item as readonly JsonbValue[];
      out.push({ token: "beginArray", raw: false, count: items.length });
      for (const element of items) walk(element, "element");
      out.push({ token: "endArray" });
    } else if (type === JsonbType.Object) {
      const entries = (item as JsonbObject).entries;
      out.push({ token: "beginObject", count: entries.length });
      for (const [key, member] of entries) {
        out.push({ token: "key", value: key });
        walk(member, "value");
      }
      out.push({ token: "endObject" });
    } else out.push({ token: position, value: item });
  };
  const type = typeOf(value);
  if (type === JsonbType.Array || type === JsonbType.Object) walk(value, "element");
  else {
    out.push({ token: "beginArray", raw: true, count: 1 });
    out.push({ token: "element", value });
    out.push({ token: "endArray" });
  }
  return out;
}

function tokenType(token: Token): JsonbType {
  switch (token.token) {
    case "beginArray":
    case "endArray":
      return JsonbType.Array;
    case "beginObject":
    case "endObject":
      return JsonbType.Object;
    case "key":
      return JsonbType.String;
    default:
      return typeOf(token.value);
  }
}

function compareScalars(a: JsonbValue, b: JsonbValue): number {
  switch (typeOf(a)) {
    case JsonbType.Null:
      return 0;
    case JsonbType.String:
      return compareCodePoints(a as string, b as string);
    case JsonbType.Bool:
      return Number(a) - Number(b);
    default:
      return compareDecimals((a as JsonbNumber).number, (b as JsonbNumber).number);
  }
}

/** PostgreSQL's jsonb order (see the header). */
export function compareJsonb(a: JsonbValue, b: JsonbValue): number {
  const x = tokens(a);
  const y = tokens(b);
  for (let index = 0; index < x.length && index < y.length; index++) {
    const ta = x[index];
    const tb = y[index];
    if (ta === undefined || tb === undefined) break;
    let result = 0;
    if (ta.token === tb.token) {
      if (ta.token === "endArray" || ta.token === "endObject") continue;
      const typeA = tokenType(ta);
      const typeB = tokenType(tb);
      if (typeA !== typeB) result = typeA > typeB ? 1 : -1;
      else if (ta.token === "beginArray" && tb.token === "beginArray") {
        if (ta.raw !== tb.raw) result = ta.raw ? -1 : 1;
        if (ta.count !== tb.count) result = ta.count > tb.count ? 1 : -1;
      } else if (ta.token === "beginObject" && tb.token === "beginObject") {
        if (ta.count !== tb.count) result = ta.count > tb.count ? 1 : -1;
      } else if ("value" in ta && "value" in tb) result = compareScalars(ta.value, tb.value);
    } else {
      const typeA = tokenType(ta);
      const typeB = tokenType(tb);
      result = typeA === typeB ? 0 : typeA > typeB ? 1 : -1;
    }
    if (result !== 0) return Math.sign(result);
  }
  return Math.sign(x.length - y.length);
}

function writeJsonb(writer: KeyWriter, value: JsonbValue): void {
  writer.byte(typeOf(value));
  if (value === null) return;
  if (typeof value === "string") return writer.escaped(encoder.encode(value));
  if (typeof value === "boolean") return writer.byte(value ? 1 : 0);
  if (Array.isArray(value)) {
    writer.int32(value.length);
    for (const item of value as readonly JsonbValue[]) writeJsonb(writer, item);
    return;
  }
  if ("number" in (value as object)) {
    const { coefficient, scale } = (value as JsonbNumber).number;
    return writer.decimal(coefficient, scale);
  }
  const entries = (value as JsonbObject).entries;
  writer.int32(entries.length);
  for (const [key, item] of entries) {
    writer.escaped(encoder.encode(key));
    writeJsonb(writer, item);
  }
}

/** Bytes equal exactly when `compareJsonb` returns 0 (numbers normalized: 1 = 1.0). */
export function jsonbEqualityKey(value: JsonbValue): Uint8Array {
  const writer = new KeyWriter(64);
  writeJsonb(writer, value);
  return writer.view().slice();
}

const SPACE = new Set([" ", "\t", "\n", "\r"]);
const ESCAPES: Readonly<Record<string, string>> = {
  '"': '"',
  "\\": "\\",
  "/": "/",
  b: "\b",
  f: "\f",
  n: "\n",
  r: "\r",
  t: "\t",
};

const NUMERIC_EXPONENT_LIMIT = 1_073_741_823;
const NUMERIC_MAX_DISPLAY_SCALE = 16_383;
const NUMERIC_MAX_INTEGER_DIGITS = 131_072;

/** Parses PostgreSQL JSON into a JsonbValue: exact numbers, last duplicate key wins, keys sorted. */
export function parseJsonb(text: string): JsonbValue {
  let position = 0;
  const fail = (what: string): never => {
    throw new SyntaxError(`Invalid JSON at ${String(position)}: ${what}`);
  };
  const space = () => {
    while (SPACE.has(text[position] ?? "")) position += 1;
  };
  const string = (): string => {
    position += 1;
    let out = "";
    for (;;) {
      const char = text[position];
      if (char === undefined) return fail("unterminated string");
      position += 1;
      if (char === '"') break;
      const code = char.charCodeAt(0);
      if (code < 0x20) fail("control character in string");
      if (char !== "\\") {
        out += char;
        continue;
      }
      const escape = text[position] ?? "";
      position += 1;
      if (escape !== "u") {
        out += ESCAPES[escape] ?? fail("bad escape");
        continue;
      }
      const hex = text.slice(position, position + 4);
      if (!/^[0-9a-fA-F]{4}$/u.test(hex)) fail("bad \\u escape");
      position += 4;
      const unit = parseInt(hex, 16);
      if (unit === 0) fail("\\u0000 is not supported");
      out += String.fromCharCode(unit);
    }
    // Every surrogate must be paired (PostgreSQL rejects lone surrogates).
    for (let index = 0; index < out.length; index++) {
      const unit = out.charCodeAt(index);
      if (unit >= 0xd800 && unit <= 0xdbff) {
        const next = out.charCodeAt(index + 1);
        if (!(next >= 0xdc00 && next <= 0xdfff)) fail("unpaired high surrogate");
        index += 1;
      } else if (unit >= 0xdc00 && unit <= 0xdfff) fail("unpaired low surrogate");
    }
    return out;
  };
  const value = (): JsonbValue => {
    space();
    const char = text[position];
    if (char === "{") {
      position += 1;
      const map = new Map<string, JsonbValue>();
      space();
      if (text[position] === "}") position += 1;
      else
        for (;;) {
          space();
          if (text[position] !== '"') fail("expected a key");
          const key = string();
          space();
          if (text[position++] !== ":") fail("expected ':'");
          map.set(key, value());
          space();
          const next = text[position++];
          if (next === "}") break;
          if (next !== ",") fail("expected ',' or '}'");
        }
      return { entries: [...map].sort(([a], [b]) => compareKeys(a, b)) };
    }
    if (char === "[") {
      position += 1;
      const items: JsonbValue[] = [];
      space();
      if (text[position] === "]") position += 1;
      else
        for (;;) {
          items.push(value());
          space();
          const next = text[position++];
          if (next === "]") break;
          if (next !== ",") fail("expected ',' or ']'");
        }
      return items;
    }
    if (char === '"') return string();
    for (const [word, result] of [
      ["true", true],
      ["false", false],
      ["null", null],
    ] as const)
      if (text.startsWith(word, position)) {
        position += word.length;
        return result;
      }
    const match =
      /^-?(0|[1-9]\d*)(?:\.(\d+))?(?:[eE]([+-]?\d+))?/u.exec(text.slice(position)) ??
      fail("expected a value");
    position += match[0].length;
    if (/^[\d.]/u.test(text[position] ?? "")) fail("malformed number");
    const [, whole = "", fraction = "", exponent = "0"] = match;
    // PostgreSQL NUMERIC's limits (checked against PGlite): an exponent of at most 2^30 − 1 in
    // magnitude (inclusive), a display scale of at most 16,383, and at most 131,072 digits before
    // the point.
    const power = exponent.replace(/^[+-]?0*/u, "").length > 10 ? Infinity : Number(exponent);
    if (!(Math.abs(power) <= NUMERIC_EXPONENT_LIMIT)) fail("numeric exponent out of range");
    const scale = fraction.length - power;
    if (scale > NUMERIC_MAX_DISPLAY_SCALE) fail("numeric scale out of range");
    const significant = (whole + fraction).replace(/^0+/u, "").length;
    if (significant > 0 && significant - scale > NUMERIC_MAX_INTEGER_DIGITS)
      fail("numeric value out of range");
    const digits = BigInt(whole + fraction);
    return {
      number: { coefficient: match[0].startsWith("-") ? -digits : digits, scale },
    };
  };
  const result = value();
  space();
  if (position !== text.length) fail("trailing characters");
  return result;
}

export interface SqlArray {
  readonly element: PhysicalType;
  /** Length of each dimension. */
  readonly dimensions: readonly number[];
  readonly lowerBounds: readonly number[];
  /** Elements in storage (row-major) order. */
  readonly elements: ReadonlyArray<KeyValue | null>;
}

function elementKey(element: PhysicalType, value: KeyValue | null): Uint8Array {
  return encodeSortKey([{ type: element, value, direction: "asc", nulls: "last" }]);
}

function compareBytes(a: Uint8Array, b: Uint8Array): number {
  const length = Math.min(a.length, b.length);
  for (let index = 0; index < length; index++) {
    const difference = (a[index] ?? 0) - (b[index] ?? 0);
    if (difference !== 0) return Math.sign(difference);
  }
  return Math.sign(a.length - b.length);
}

function compareNumbers(a: readonly number[], b: readonly number[]): number {
  if (a.length !== b.length) return a.length < b.length ? -1 : 1;
  for (let index = 0; index < a.length; index++)
    if (a[index] !== b[index]) return (a[index] ?? 0) < (b[index] ?? 0) ? -1 : 1;
  return 0;
}

/** PostgreSQL's array order (array_cmp). */
export function compareArrays(a: SqlArray, b: SqlArray): number {
  const shared = Math.min(a.elements.length, b.elements.length);
  for (let index = 0; index < shared; index++) {
    const order = compareBytes(
      elementKey(a.element, a.elements[index] ?? null),
      elementKey(b.element, b.elements[index] ?? null),
    );
    if (order !== 0) return order;
  }
  if (a.elements.length !== b.elements.length)
    return a.elements.length < b.elements.length ? -1 : 1;
  if (a.dimensions.length !== b.dimensions.length)
    return a.dimensions.length < b.dimensions.length ? -1 : 1;
  return compareNumbers(a.dimensions, b.dimensions) || compareNumbers(a.lowerBounds, b.lowerBounds);
}

/** Bytes equal exactly when `compareArrays` returns 0. */
export function arrayEqualityKey(value: SqlArray): Uint8Array {
  const writer = new KeyWriter(64);
  writer.int32(value.dimensions.length);
  for (const length of value.dimensions) writer.int32(length);
  for (const bound of value.lowerBounds) writer.int32(bound);
  for (const item of value.elements) {
    const key = encodeEqualityKey([{ type: value.element, value: item }]);
    writer.int32(key.length);
    writer.fixed(key, 0, key.length);
  }
  return writer.view().slice();
}
