import { expect, it } from "vitest";
import fc from "fast-check";
import { JsonNumber, parseJsonValue } from "./json-values.js";
import { boundedJsonText } from "./sql-domains.js";
import { MAX_SQL_STRUCTURED_VALUE_DEPTH, MAX_SQL_STRUCTURED_VALUE_ITEMS } from "./cache-limits.js";

it("agrees with native JSON for generated documents whose numbers are exact integers", () => {
  fc.assert(
    fc.property(fc.jsonValue({ maxDepth: 5 }), (value) => {
      const source = JSON.stringify(value);
      const parsed = parseJsonValue(source);
      // Native parsing is the oracle only for structure. Numeric text is independently preserved.
      expect(JSON.parse(boundedJsonText(parsed, false))).toEqual(JSON.parse(source));
    }),
    { seed: 29092026, numRuns: 300 },
  );
});

it.each(["-", "01", "1.", "1e", "[1,]", '{"a":1,}', "true false", "[1]x", '"\\uD800"'])(
  "rejects malformed or ill-formed JSON %s",
  (source) => {
    expect(() => parseJsonValue(source)).toThrow();
  },
);

it("normalizes equivalent numeric lexemes without rounding and safely admits prototype names", () => {
  for (const source of ["9007199254740993", "9007199254740993.0", "900719925474099300e-2"]) {
    expect((parseJsonValue(source) as JsonNumber).text).toBe("9007199254740993");
  }
  const value = parseJsonValue('{"__proto__":{"n":1.0000000000000001},"constructor":true}');
  expect(Object.getPrototypeOf(value)).toBeNull();
  expect(boundedJsonText(value, true)).toBe(
    '{"__proto__":{"n":1.0000000000000001},"constructor":true}',
  );
});

it("rejects exponent, depth and item expansion before unbounded allocation", () => {
  for (const source of ["1e100001", "1e-100001", "1e999999999999999999999999"]) {
    expect(() => parseJsonValue(source)).toThrow(RangeError);
  }
  const depth = MAX_SQL_STRUCTURED_VALUE_DEPTH + 1;
  expect(() => parseJsonValue("[".repeat(depth) + "0" + "]".repeat(depth))).toThrow(RangeError);
  expect(() => parseJsonValue(`[${"0,".repeat(MAX_SQL_STRUCTURED_VALUE_ITEMS)}0]`)).toThrow(
    RangeError,
  );
});
