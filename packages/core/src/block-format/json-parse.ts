/**
 * `JSON.parse` that is correct on engines with V8's escaped-key bug.
 *
 * V8 (Chrome 151–155 and Node 24 at least) can return the wrong name for an object key whose
 * JSON text holds an escape: after one object with keys `{"a", "\\"}` has been parsed,
 * `JSON.parse('{"a":1,"\\"":2}')` names the second key `\` instead of `"`. The parser reuses the
 * hidden-class transition an earlier object left and compares the escaped key against it without
 * decoding the escape. In a fuzz of objects with escaped keys about one parse in a hundred came
 * back renamed. Values are never affected, and every wrong key seen contains a backslash, the
 * character the misread comparison matched.
 *
 * So the native result is kept whenever it cannot be wrong: the text has no escape in any key, or
 * no key in the result contains a backslash. Otherwise the text is parsed again by a portable
 * parser with JSON.parse's exact semantics (own `__proto__` keys, last duplicate wins in the
 * first one's position). Valid JSON only: the native parse runs first and reports any error.
 */
export function parseJson(text: string): unknown {
  const value: unknown = JSON.parse(text);
  if (!text.includes("\\") || !hasEscapedKey(text) || !hasBackslashKey(value)) return value;
  return parsePortableJson(text);
}

const BACKSLASH = 0x5c;
const QUOTE = 0x22;

/** Whether some object key's JSON text in valid `text` holds an escape. */
function hasEscapedKey(text: string): boolean {
  let at = text.indexOf("\\");
  while (at !== -1) {
    // A backslash only occurs inside a string: walk to that string's closing quote.
    let index = at;
    for (;;) {
      const code = text.charCodeAt(index);
      if (code === BACKSLASH) index += 2;
      else if (code === QUOTE) break;
      else index += 1;
    }
    // A key's closing quote is followed by a colon, with only whitespace between.
    let next = index + 1;
    for (;;) {
      const code = text.charCodeAt(next);
      if (code !== 0x20 && code !== 0x0a && code !== 0x0d && code !== 0x09) break;
      next += 1;
    }
    if (text.charCodeAt(next) === 0x3a) return true;
    at = text.indexOf("\\", index + 1);
  }
  return false;
}

/** Whether some object key anywhere in a parsed value contains a backslash. */
function hasBackslashKey(root: unknown): boolean {
  const pending: unknown[] = [root];
  while (pending.length > 0) {
    const value = pending.pop();
    if (typeof value !== "object" || value === null) continue;
    if (Array.isArray(value)) {
      for (const entry of value)
        if (typeof entry === "object" && entry !== null) pending.push(entry);
      continue;
    }
    const record = value as Record<string, unknown>;
    for (const key of Object.keys(record)) {
      if (key.includes("\\")) return true;
      const entry = record[key];
      if (typeof entry === "object" && entry !== null) pending.push(entry);
    }
  }
  return false;
}

type Container =
  { readonly array: unknown[] } | { readonly record: Record<string, unknown>; key: string };

/**
 * A JSON parser written in JavaScript, for the rare text the native result cannot be trusted
 * for. Iterative, so nesting depth is bounded by memory rather than the call stack.
 */
export function parsePortableJson(text: string): unknown {
  let at = 0;
  const fail = (): never => {
    throw new SyntaxError(`Unexpected token in JSON at position ${String(at)}`);
  };
  const space = (): void => {
    for (;;) {
      const code = text.charCodeAt(at);
      if (code !== 0x20 && code !== 0x0a && code !== 0x0d && code !== 0x09) return;
      at += 1;
    }
  };
  const string = (): string => {
    // `at` is on the opening quote.
    at += 1;
    let out = "";
    let runStart = at;
    for (;;) {
      const code = text.charCodeAt(at);
      if (code === QUOTE) {
        out += text.slice(runStart, at);
        at += 1;
        return out;
      }
      if (Number.isNaN(code) || code < 0x20) fail();
      if (code !== BACKSLASH) {
        at += 1;
        continue;
      }
      out += text.slice(runStart, at);
      const escape = text.charCodeAt(at + 1);
      at += 2;
      switch (escape) {
        case QUOTE:
          out += '"';
          break;
        case BACKSLASH:
          out += "\\";
          break;
        case 0x2f:
          out += "/";
          break;
        case 0x62:
          out += "\b";
          break;
        case 0x66:
          out += "\f";
          break;
        case 0x6e:
          out += "\n";
          break;
        case 0x72:
          out += "\r";
          break;
        case 0x74:
          out += "\t";
          break;
        case 0x75: {
          const hex = text.slice(at, at + 4);
          if (!/^[0-9a-fA-F]{4}$/.test(hex)) fail();
          out += String.fromCharCode(Number.parseInt(hex, 16));
          at += 4;
          break;
        }
        default:
          fail();
      }
      runStart = at;
    }
  };
  const NUMBER = /-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?/y;
  const scalar = (): unknown => {
    const code = text.charCodeAt(at);
    if (code === QUOTE) return string();
    if (text.startsWith("true", at)) {
      at += 4;
      return true;
    }
    if (text.startsWith("false", at)) {
      at += 5;
      return false;
    }
    if (text.startsWith("null", at)) {
      at += 4;
      return null;
    }
    NUMBER.lastIndex = at;
    const match = NUMBER.exec(text);
    if (match === null) return fail();
    at += match[0].length;
    return Number(match[0]);
  };
  const assign = (record: Record<string, unknown>, key: string, value: unknown): void => {
    // JSON.parse creates own data properties: `__proto__` is an ordinary key, not the prototype.
    if (key === "__proto__") {
      Object.defineProperty(record, key, {
        value,
        writable: true,
        enumerable: true,
        configurable: true,
      });
    } else {
      record[key] = value;
    }
  };
  const stack: Container[] = [];
  let result: unknown;
  let value: unknown;
  space();
  for (;;) {
    // Parse one value; containers push a frame and continue with their first member.
    const code = text.charCodeAt(at);
    if (code === 0x5b) {
      at += 1;
      space();
      const array: unknown[] = [];
      if (text.charCodeAt(at) === 0x5d) {
        at += 1;
        value = array;
      } else {
        stack.push({ array });
        continue;
      }
    } else if (code === 0x7b) {
      at += 1;
      space();
      const record: Record<string, unknown> = {};
      if (text.charCodeAt(at) === 0x7d) {
        at += 1;
        value = record;
      } else {
        if (text.charCodeAt(at) !== QUOTE) fail();
        const key = string();
        space();
        if (text.charCodeAt(at) !== 0x3a) fail();
        at += 1;
        space();
        stack.push({ record, key });
        continue;
      }
    } else {
      value = scalar();
    }
    // Hand the finished value to its container, closing every container it completes.
    for (;;) {
      const frame = stack.at(-1);
      if (frame === undefined) {
        result = value;
        space();
        if (at !== text.length) fail();
        return result;
      }
      if ("array" in frame) frame.array.push(value);
      else assign(frame.record, frame.key, value);
      space();
      const next = text.charCodeAt(at);
      if (next === 0x2c) {
        at += 1;
        space();
        if (!("array" in frame)) {
          if (text.charCodeAt(at) !== QUOTE) fail();
          frame.key = string();
          space();
          if (text.charCodeAt(at) !== 0x3a) fail();
          at += 1;
          space();
        }
        break;
      }
      if (next !== ("array" in frame ? 0x5d : 0x7d)) fail();
      at += 1;
      stack.pop();
      value = "array" in frame ? frame.array : frame.record;
    }
  }
}
