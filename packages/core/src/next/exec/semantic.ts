/**
 * Semantic ordering and equality for the types that have no persisted key (keys/key-encoding.ts
 * `indexKeyKind`): SQL arrays and JSONB. Sorting, grouping, DISTINCT and joins on these use these
 * functions at query time, never byte encodings. Each comparator returns −1, 0 or 1, and two
 * values compare 0 exactly when their equality keys (`*EqualityKey`) are equal, so hashing the
 * equality key agrees with the comparator.
 *
 * ## JSONB (PostgreSQL's jsonb btree order)
 * Object > Array > Boolean > Number > String > Null, except that an empty top-level array sorts
 * below null (PostgreSQL's historical rule). Objects with more pairs sort higher; then pairs are
 * compared in storage order (keys by length, then bytes), key then value. Arrays with more
 * elements sort higher; then element by element. Numbers compare numerically and exactly
 * (`1` = `1.0`, `2` < `10`). Strings and keys compare by code point (byte) order. false < true.
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

function rank(value: JsonbValue): number {
  if (value === null) return 0;
  if (typeof value === "string") return 1;
  if (typeof value === "boolean") return 3;
  if (Array.isArray(value)) return 4;
  return "number" in (value as object) ? 2 : 5;
}

function compareInner(a: JsonbValue, b: JsonbValue): number {
  const difference = rank(a) - rank(b);
  if (difference !== 0) return Math.sign(difference);
  if (a === null || b === null) return 0;
  if (typeof a === "string") return compareCodePoints(a, b as string);
  if (typeof a === "boolean") return Number(a) - Number(b);
  if (Array.isArray(a)) {
    const list = a as readonly JsonbValue[];
    const other = b as readonly JsonbValue[];
    if (list.length !== other.length) return list.length < other.length ? -1 : 1;
    for (let index = 0; index < list.length; index++) {
      const order = compareInner(list[index] ?? null, other[index] ?? null);
      if (order !== 0) return order;
    }
    return 0;
  }
  if ("number" in (a as object))
    return compareDecimals((a as JsonbNumber).number, (b as JsonbNumber).number);
  const x = (a as JsonbObject).entries;
  const y = (b as JsonbObject).entries;
  if (x.length !== y.length) return x.length < y.length ? -1 : 1;
  for (let index = 0; index < x.length; index++) {
    const [keyA, valueA] = x[index] ?? ["", null];
    const [keyB, valueB] = y[index] ?? ["", null];
    const order = compareKeys(keyA, keyB) || compareInner(valueA, valueB);
    if (order !== 0) return order;
  }
  return 0;
}

const isEmptyArray = (value: JsonbValue) => Array.isArray(value) && value.length === 0;

/** PostgreSQL's jsonb order for two top-level values. */
export function compareJsonb(a: JsonbValue, b: JsonbValue): number {
  const emptyA = isEmptyArray(a);
  const emptyB = isEmptyArray(b);
  if (emptyA || emptyB) return emptyA === emptyB ? 0 : emptyA ? -1 : 1;
  return compareInner(a, b);
}

function writeJsonb(writer: KeyWriter, value: JsonbValue): void {
  writer.byte(rank(value));
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
  writer.byte(isEmptyArray(value) ? 0xff : 0);
  writeJsonb(writer, value);
  return writer.view().slice();
}

/** Parses JSON text into a JsonbValue: exact numbers, last duplicate key wins, keys sorted. */
export function parseJsonb(text: string): JsonbValue {
  let position = 0;
  const fail = (): never => {
    throw new SyntaxError(`Invalid JSON at ${String(position)}`);
  };
  const space = () => {
    while (/\s/u.test(text[position] ?? "")) position += 1;
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
          const key = value();
          if (typeof key !== "string") fail();
          space();
          if (text[position++] !== ":") fail();
          map.set(key as string, value());
          space();
          const next = text[position++];
          if (next === "}") break;
          if (next !== ",") fail();
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
          if (next !== ",") fail();
        }
      return items;
    }
    if (char === '"') {
      const match = /^"(?:[^"\\]|\\.)*"/u.exec(text.slice(position)) ?? fail();
      position += match[0].length;
      return JSON.parse(match[0]) as string;
    }
    for (const [word, result] of [
      ["true", true],
      ["false", false],
      ["null", null],
    ] as const)
      if (text.startsWith(word, position)) {
        position += word.length;
        return result;
      }
    const match = /^-?(\d+)(?:\.(\d+))?(?:[eE]([+-]?\d+))?/u.exec(text.slice(position)) ?? fail();
    position += match[0].length;
    const [, whole = "", fraction = "", exponent = "0"] = match;
    const negative = match[0].startsWith("-");
    const digits = BigInt(whole + fraction);
    return {
      number: {
        coefficient: negative ? -digits : digits,
        scale: fraction.length - Number(exponent),
      },
    };
  };
  const result = value();
  space();
  if (position !== text.length) fail();
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
