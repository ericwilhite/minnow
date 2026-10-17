import { describe, expect, it } from "vitest";
import { parseJson, parsePortableJson } from "./json-parse.js";

/** Deterministic, so a failure names the case that produced it. */
function lcg(seed: number): (bound: number) => number {
  let state = seed;
  return (bound) => {
    state = (Math.imul(state, 1103515245) + 12345) & 0x7fffffff;
    return state % bound;
  };
}

const ALPHABET = ["a", "\\", '"', "\n", "\t", "/", "\u0000", "\u0001", "é", " ", "😀", " ", "\b"];

describe("parseJson", () => {
  it("keeps escaped keys that V8 renames after an earlier parse", () => {
    // On affected V8 builds the second native parse names its key `\`, reusing the transition
    // the first left. A unique leading key keeps the case independent of earlier tests.
    JSON.parse(String.raw`{"json-parse-a":1,"\\":2}`);
    expect(Object.keys(parseJson(String.raw`{"json-parse-a":1,"\"":2}`) as object)).toEqual([
      "json-parse-a",
      '"',
    ]);
    JSON.parse(String.raw`{"json-parse-b":1,"\\":2}`);
    expect(Object.keys(parseJson(String.raw`{"json-parse-b":1,"\n":2}`) as object)).toEqual([
      "json-parse-b",
      "\n",
    ]);
    JSON.parse(String.raw`{"json-parse-c":1,"a\\":2}`);
    expect(Object.keys(parseJson(String.raw`{"json-parse-c":1,"a\"":2}`) as object)).toEqual([
      "json-parse-c",
      'a"',
    ]);
  });

  it("returns every key as written across objects built to trip the transition cache", () => {
    const next = lcg(0x5eed);
    const key = (): string => {
      let text = "";
      const length = 1 + next(3);
      for (let index = 0; index < length; index += 1) text += ALPHABET[next(ALPHABET.length)] ?? "";
      return text;
    };
    let nativeWrong = 0;
    for (let round = 0; round < 60_000; round += 1) {
      const keys: string[] = [];
      const count = 1 + next(3);
      while (keys.length < count) {
        const candidate = key();
        if (!keys.includes(candidate)) keys.push(candidate);
      }
      const source: Record<string, string> = {};
      for (const name of keys) source[name] = `v${key()}`;
      const text = JSON.stringify([source]);
      const nativeKeys = Object.keys((JSON.parse(text) as Array<Record<string, unknown>>)[0] ?? {});
      if (JSON.stringify(nativeKeys) !== JSON.stringify(keys)) nativeWrong += 1;
      expect(parseJson(text), text).toStrictEqual([source]);
    }
    // Not asserted: whether this engine has the bug. Recorded so a run shows what it guarded.
    expect(nativeWrong).toBeGreaterThanOrEqual(0);
  });

  it("returns the native result untouched when no key can be wrong", () => {
    const text = '{"a":"\\u0000x","b":["\\n"],"c":{"d":"\\""}}';
    expect(parseJson(text)).toStrictEqual(JSON.parse(text));
  });
});

describe("parsePortableJson", () => {
  const sample = (next: (bound: number) => number, depth: number): unknown => {
    const choice = next(depth > 3 ? 5 : 8);
    if (choice === 0) return null;
    if (choice === 1) return next(2) === 0;
    if (choice === 2) return [0, 1.5, -2e-7, 1e300, 123456789012345, -42][next(6)];
    if (choice === 3 || choice === 4) {
      let text = "";
      for (let index = next(5); index > 0; index -= 1)
        text += ALPHABET[next(ALPHABET.length)] ?? "";
      return text;
    }
    if (choice === 5) return Array.from({ length: next(4) }, () => sample(next, depth + 1));
    const record: Record<string, unknown> = {};
    for (let index = next(4); index > 0; index -= 1) {
      const name = ["k", "__proto__", "0", "10", "é", "\\", '"', "\u0000"][next(8)] ?? "k";
      Object.defineProperty(record, name, {
        value: sample(next, depth + 1),
        writable: true,
        enumerable: true,
        configurable: true,
      });
    }
    return record;
  };

  it("agrees with JSON.parse on values, key order, own __proto__ keys, and whitespace", () => {
    const next = lcg(77);
    for (let round = 0; round < 5_000; round += 1) {
      const value = sample(next, 0);
      const compact = JSON.stringify(value);
      const pretty = JSON.stringify(value, null, next(2) === 0 ? 2 : "\t");
      for (const text of [compact, pretty]) {
        // Compared with the value itself, which JSON.stringify round-trips exactly here: a native
        // parse is no oracle, since the escaped keys sampled are the ones V8 can rename.
        expect(parsePortableJson(text), text).toStrictEqual(value);
      }
    }
  });

  it("matches JSON.parse on duplicate keys, escapes, and numbers", () => {
    for (const text of [
      '{"a":1,"b":2,"a":3}',
      '{"__proto__":{"x":1},"y":[]}',
      String.raw`"😀é\/\b\f\n\r\t"`,
      "[-0, 0.5e+3, 1E-2, 9007199254740993]",
      ' \n\t {"2":0,"1":1,"b":2} \r\n',
    ]) {
      expect(parsePortableJson(text), text).toStrictEqual(JSON.parse(text));
    }
  });

  it("parses nesting deeper than the call stack allows recursion", () => {
    const depth = 200_000;
    const text = "[".repeat(depth) + "]".repeat(depth);
    let value = parsePortableJson(text);
    let levels = 0;
    while (Array.isArray(value) && value.length > 0) {
      value = value[0];
      levels += 1;
    }
    expect(levels).toBe(depth - 1);
  });
});
