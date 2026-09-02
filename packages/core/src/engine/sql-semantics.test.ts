import { describe, expect, it } from "vitest";
import {
  compareSqlValues,
  compileLikePattern,
  compileSimilarPattern,
  encodeSqlEqualityValue,
  roundSqlNumber,
} from "./sql-semantics.js";

function combinations(alphabet: readonly string[], maximumLength: number): string[] {
  const values = [""];
  let frontier = [""];
  for (let length = 1; length <= maximumLength; length += 1) {
    frontier = frontier.flatMap((prefix) => alphabet.map((character) => prefix + character));
    values.push(...frontier);
  }
  return values;
}

function oracleLike(pattern: string, value: string): boolean {
  const patternCharacters = Array.from(pattern);
  const valueCharacters = Array.from(value);
  const memo = new Map<string, boolean>();
  const match = (patternIndex: number, valueIndex: number): boolean => {
    const key = `${String(patternIndex)}:${String(valueIndex)}`;
    const cached = memo.get(key);
    if (cached !== undefined) return cached;
    const token = patternCharacters[patternIndex];
    let result: boolean;
    if (token === undefined) result = valueIndex === valueCharacters.length;
    else if (token === "%") {
      result =
        match(patternIndex + 1, valueIndex) ||
        (valueIndex < valueCharacters.length && match(patternIndex, valueIndex + 1));
    } else {
      result =
        valueIndex < valueCharacters.length &&
        (token === "_" || token === valueCharacters[valueIndex]) &&
        match(patternIndex + 1, valueIndex + 1);
    }
    memo.set(key, result);
    return result;
  };
  return match(0, 0);
}

function oracleSimilar(pattern: string, value: string): boolean {
  let source = "";
  let characterClass = false;
  for (const character of pattern) {
    if (character === "[") characterClass = true;
    if (!characterClass && character === "%") source += "(?:[\\s\\S]*)";
    else if (!characterClass && character === "_") source += "(?:[\\s\\S])";
    else source += character;
    if (character === "]") characterClass = false;
  }
  return new RegExp(`^(?:${source})$`, "u").test(value);
}

describe("shared SQL value semantics", () => {
  it("orders strings by codepoint independent of the host locale", () => {
    expect(compareSqlValues("a", "A")).toBeGreaterThan(0);
    expect(["a", "A", "z", "Z"].sort(compareSqlValues)).toEqual(["A", "Z", "a", "z"]);
  });

  it("rounds ties away from zero and bounds extreme precision", () => {
    expect(roundSqlNumber(-1.5)).toBe(-2);
    expect(roundSqlNumber(1.25, 1)).toBe(1.3);
    // Whole-number rounding is arithmetic; it must match toFixed(0) on ties, near-ties, signed
    // zero, and magnitudes past 2^52 where every double is already an integer.
    for (const value of [
      0.5,
      1.5,
      2.5,
      -0.5,
      -2.5,
      0.49999999999999994,
      -0.49999999999999994,
      1e15 + 0.5,
      2 ** 52 + 1,
      2 ** 53 + 2,
      -0,
      123.456,
      -123.5,
      1e300,
      -1e300,
    ]) {
      const expected = Number(value.toFixed(0));
      expect(roundSqlNumber(value), String(value)).toBe(expected === 0 ? 0 : expected);
    }
    expect(Number.isNaN(roundSqlNumber(Number.NaN))).toBe(true);
    expect(roundSqlNumber(Number.POSITIVE_INFINITY)).toBe(Number.POSITIVE_INFINITY);
    expect(roundSqlNumber(-1.25, 1)).toBe(-1.3);
    expect(roundSqlNumber(1.23, 1_000)).toBe(1.23);
    expect(roundSqlNumber(123.45, -1)).toBe(123);
    expect(roundSqlNumber(1.005, 2)).toBe(1);
    expect(roundSqlNumber(2.675, 2)).toBe(2.67);
  });

  it("matches LIKE underscore against one Unicode codepoint", () => {
    expect(compileLikePattern("_").test("😀")).toBe(true);
    expect(compileLikePattern("__").test("😀")).toBe(false);
    expect(compileLikePattern("😀_").test("😀x")).toBe(true);
  });

  it("matches LIKE wildcards, escapes, and Unicode case folds without a host RegExp", () => {
    expect(compileLikePattern("a%b_c").test("axyzb😀c")).toBe(true);
    expect(compileLikePattern("a!%b", false, "!").test("a%b")).toBe(true);
    expect(compileLikePattern("a!%b", false, "!").test("axxb")).toBe(false);
    expect(compileLikePattern("%s%", true).test("xſy")).toBe(true);
    expect(compileLikePattern("a", true).test("aa")).toBe(false);
    expect(compileLikePattern("%a", true).test("aba")).toBe(true);
    expect(compileLikePattern("%a", true).test("abax")).toBe(false);
  });

  it("agrees with a dynamic-programming LIKE oracle over exhaustive short inputs", () => {
    const patterns = combinations(["a", "b", "_", "%"], 4);
    const values = combinations(["a", "b", "😀"], 4);
    for (const pattern of patterns) {
      const matcher = compileLikePattern(pattern);
      for (const value of values) {
        expect(
          matcher.test(value),
          `${JSON.stringify(value)} LIKE ${JSON.stringify(pattern)}`,
        ).toBe(oracleLike(pattern, value));
      }
    }
  });

  it("evaluates the supported SIMILAR TO grammar with a Thompson NFA", () => {
    expect(compileSimilarPattern("(west|north)").test("west")).toBe(true);
    expect(compileSimilarPattern("(west|north)").test("east")).toBe(false);
    expect(compileSimilarPattern("w%").test("w\nest")).toBe(true);
    expect(compileSimilarPattern("[a-c]+").test("abccba")).toBe(true);
    expect(compileSimilarPattern("[a-c]+").test("abcd")).toBe(false);
    expect(compileSimilarPattern("[^a]+").test("bbb")).toBe(true);
    expect(compileSimilarPattern(String.raw`[a\-c]+`).test("a-c")).toBe(true);
    expect(compileSimilarPattern(String.raw`\%`).test("%")).toBe(true);
    expect(compileSimilarPattern("(|a)b").test("b")).toBe(true);
  });

  it("agrees with an anchored-regexp oracle over generated valid SIMILAR expressions", () => {
    const atoms = ["a", "b", "_", "%", "[ab]"];
    const pieces = atoms.flatMap((atom) => [atom, `${atom}*`, `${atom}+`]);
    const patterns = [
      "",
      ...pieces,
      ...pieces.flatMap((left) => pieces.map((right) => left + right)),
      "(a|b)",
      "(a|%)",
      "(|a)",
      "(a|b)+",
    ];
    const values = combinations(["a", "b", "c", "😀"], 3);
    for (const pattern of patterns) {
      const matcher = compileSimilarPattern(pattern);
      for (const value of values) {
        expect(
          matcher.test(value),
          `${JSON.stringify(value)} SIMILAR TO ${JSON.stringify(pattern)}`,
        ).toBe(oracleSimilar(pattern, value));
      }
    }
  });

  it("honors an arbitrary SIMILAR TO escape even when it is a grammar character", () => {
    for (const character of ["%", "_", "|", "(", ")", "*", "+", "["]) {
      expect(
        compileSimilarPattern(character + character, character).test(character),
        `escape ${character}`,
      ).toBe(true);
    }
    expect(compileSimilarPattern("a!%b", "!").test("a%b")).toBe(true);
    expect(compileSimilarPattern("a**", "*").test("a*")).toBe(true);
  });

  it("does not backtrack catastrophically on nested SIMILAR TO repetition", () => {
    const matcher = compileSimilarPattern("(a|aa)+b");
    expect(matcher.test(`${"a".repeat(20_000)}c`)).toBe(false);
  });

  it("rejects malformed SIMILAR TO patterns deterministically", () => {
    for (const pattern of ["*a", "+a", "a**", "a+*", "(a", "a)", "[a", "[]"]) {
      expect(() => compileSimilarPattern(pattern), pattern).toThrow();
    }
    expect(() => compileSimilarPattern("a\\")).toThrow(/ends with its escape/);
  });

  it("normalizes signed zero in SQL equality keys", () => {
    expect(encodeSqlEqualityValue(-0)).toEqual(encodeSqlEqualityValue(0));
  });
});
