import { assertWellFormedString } from "../block-format/unicode.js";
import {
  MAX_SQL_NUMERIC_DIGITS,
  MAX_SQL_SCALAR_RESULT_CHARACTERS,
  MAX_SQL_STRUCTURED_VALUE_DEPTH,
  MAX_SQL_STRUCTURED_VALUE_ITEMS,
} from "./cache-limits.js";

/** A JSON number's exact decimal text. Never route a textual JSON number through Float64. */
export class JsonNumber {
  readonly text: string;

  constructor(source: string) {
    const match = /^(-?)(0|[1-9]\d*)(?:\.(\d+))?(?:[eE]([+-]?\d+))?$/.exec(source);
    if (match === null) throw new SyntaxError("Invalid JSON number");
    const fraction = match[3] ?? "";
    const exponent = Number(match[4] ?? 0);
    if (!Number.isSafeInteger(exponent) || Math.abs(exponent) > MAX_SQL_NUMERIC_DIGITS) {
      throw new RangeError("JSON number exponent exceeds the exact numeric limit");
    }
    let digits = `${match[2] ?? "0"}${fraction}`.replace(/^0+/, "");
    let scale = fraction.length - exponent;
    if (digits.length > MAX_SQL_NUMERIC_DIGITS || scale > MAX_SQL_NUMERIC_DIGITS) {
      throw new RangeError("JSON number exceeds the exact numeric digit limit");
    }
    if (digits.length === 0) {
      this.text = "0";
      return;
    }
    if (digits.length - scale > MAX_SQL_NUMERIC_DIGITS) {
      throw new RangeError("JSON number exceeds the exact numeric digit limit");
    }
    if (scale > 0) {
      const trailing = /0+$/.exec(digits)?.[0].length ?? 0;
      const removed = Math.min(scale, trailing);
      digits = digits.slice(0, digits.length - removed);
      scale -= removed;
    }
    const sign = match[1] ?? "";
    if (scale <= 0) this.text = `${sign}${digits}${"0".repeat(-scale)}`;
    else {
      const padded = digits.padStart(scale + 1, "0");
      const split = padded.length - scale;
      this.text = `${sign}${padded.slice(0, split)}.${padded.slice(split)}`;
    }
  }
}

/**
 * Strict, bounded JSON parsing. Native JSON.parse is used only to decode string tokens;
 * numeric tokens retain their exact text. Null-prototype objects safely admit __proto__ keys.
 */
export function parseJsonValue(source: string): unknown {
  if (source.length > MAX_SQL_SCALAR_RESULT_CHARACTERS) {
    throw new RangeError("JSON document exceeds the scalar character limit");
  }
  assertWellFormedString(source, "JSON document");
  let cursor = 0;
  let items = 0;
  const number = /-?(?:0|[1-9]\d*)(?:\.\d+)?(?:[eE][+-]?\d+)?/y;
  const fail = (): never => {
    throw new SyntaxError(`Invalid JSON at character ${String(cursor)}`);
  };
  const whitespace = (): void => {
    while (" \t\r\n".includes(source[cursor] ?? "\u0000")) cursor += 1;
  };
  const charge = (): void => {
    items += 1;
    if (items > MAX_SQL_STRUCTURED_VALUE_ITEMS) {
      throw new RangeError(
        `JSON document cannot exceed ${String(MAX_SQL_STRUCTURED_VALUE_ITEMS)} values and names`,
      );
    }
  };
  const string = (): string => {
    const start = cursor++;
    while (cursor < source.length) {
      const character = source[cursor++];
      if (character === "\\") cursor += 1;
      else if (character === '"') {
        const value: unknown = JSON.parse(source.slice(start, cursor));
        if (typeof value !== "string") return fail();
        assertWellFormedString(value, "JSON string");
        return value;
      }
    }
    return fail();
  };
  const value = (depth: number): unknown => {
    charge();
    if (depth > MAX_SQL_STRUCTURED_VALUE_DEPTH) {
      throw new RangeError(
        `JSON document cannot exceed ${String(MAX_SQL_STRUCTURED_VALUE_DEPTH)} levels`,
      );
    }
    whitespace();
    const character = source[cursor];
    if (character === '"') return string();
    if (character === "[" || character === "{") {
      cursor += 1;
      const array = character === "[";
      const close = array ? "]" : "}";
      const members: unknown[] = [];
      const object: Record<string, unknown> = Object.create(null) as Record<string, unknown>;
      whitespace();
      if (source[cursor] !== close) {
        for (;;) {
          whitespace();
          if (array) members.push(value(depth + 1));
          else {
            if (source[cursor] !== '"') return fail();
            charge();
            const key = string();
            whitespace();
            if (source[cursor++] !== ":") return fail();
            object[key] = value(depth + 1);
          }
          whitespace();
          if (source[cursor] === close) break;
          if (source[cursor++] !== ",") return fail();
        }
      }
      if (source[cursor++] !== close) return fail();
      return array ? members : object;
    }
    for (const [token, result] of [
      ["true", true],
      ["false", false],
      ["null", null],
    ] as const) {
      if (source.startsWith(token, cursor)) {
        cursor += token.length;
        return result;
      }
    }
    number.lastIndex = cursor;
    const matched = number.exec(source);
    if (matched === null) return fail();
    cursor = number.lastIndex;
    return new JsonNumber(matched[0]);
  };
  const result = value(0);
  whitespace();
  if (cursor !== source.length) return fail();
  return result;
}
