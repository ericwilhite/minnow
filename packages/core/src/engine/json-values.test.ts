import { expect, it } from "vitest";
import fc from "fast-check";
import { JsonNumber, parseJsonValue } from "./json-values.js";
import {
  boundedJsonText,
  externalSqlDomainValue,
  preservedJsonDomainValue,
} from "./sql-domains.js";
import { jsonConstructor } from "./sql-json.js";
import { MAX_SQL_STRUCTURED_VALUE_DEPTH, MAX_SQL_STRUCTURED_VALUE_ITEMS } from "./cache-limits.js";
import { seedsFor } from "../testing/seeds.js";
import { createReach } from "../testing/reach.js";

/** Seeds for this file's generated properties; `npm run soak` explores others. */
const SEEDS = seedsFor("json-values", [29092026]);

it.each(SEEDS)(
  "agrees with native JSON for generated documents whose numbers are exact integers (seed %i)",
  (seed) => {
    fc.assert(
      fc.property(fc.jsonValue({ maxDepth: 5 }), (value) => {
        const source = JSON.stringify(value);
        const parsed = parseJsonValue(source);
        // Native parsing is the oracle only for structure. Numeric text is independently preserved.
        expect(JSON.parse(boundedJsonText(parsed, false))).toEqual(JSON.parse(source));
      }),
      { seed, numRuns: 300 },
    );
  },
);

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

/** Strings of arbitrary UTF-16 code units, so unpaired surrogates occur as well as pairs. */
const codeUnitString = fc.string({
  unit: fc.integer({ min: 0, max: 0xffff }).map((code) => String.fromCharCode(code)),
});
const jsonScalar = fc.oneof(
  fc.constant(null),
  fc.boolean(),
  fc.double({ noNaN: true, noDefaultInfinity: true }),
  codeUnitString,
  fc.date({ noInvalidDate: true }),
);

it.each(SEEDS)(
  "builds scalar-member JSON_OBJECT and JSON_ARRAY values the validating parse accepts unchanged (seed %i)",
  (seed) => {
    // Without a document member the constructor skips the validating parse. Its value must be
    // exactly what that parse accepts and tags, and a string with a lone surrogate — name or
    // value — must still be refused, as the parse refused it.
    const loneSurrogate = (value: unknown): boolean =>
      typeof value === "string" && /\p{Surrogate}/u.test(value);
    const reach = createReach("json constructors", ["refused name", "refused value", "built"], {
      minimum: 10,
    });
    fc.assert(
      fc.property(fc.array(fc.tuple(codeUnitString, jsonScalar), { maxLength: 6 }), (members) => {
        reach.beginHistory();
        if (members.some(([key]) => loneSurrogate(key))) reach.reach("refused name");
        if (members.some(([, value]) => loneSurrogate(value))) reach.reach("refused value");
        const cases = [
          { name: "JSON_OBJECT" as const, values: members.flat() },
          { name: "JSON_ARRAY" as const, values: members.map(([, value]) => value) },
        ];
        for (const { name, values } of cases) {
          if (values.some(loneSurrogate)) {
            expect(() => jsonConstructor(name, values)).toThrow();
            continue;
          }
          const built = jsonConstructor(name, values);
          expect(preservedJsonDomainValue(externalSqlDomainValue(built) as string)).toBe(built);
          if (values.length > 0) reach.reach("built");
        }
      }),
      { seed, numRuns: 300 },
    );
    reach.assertReached();
  },
);

it("still bounds the depth of constructors nested inside constructors", () => {
  let value = jsonConstructor("JSON_ARRAY", [0]);
  for (let depth = 1; depth < MAX_SQL_STRUCTURED_VALUE_DEPTH; depth += 1) {
    value = jsonConstructor("JSON_ARRAY", [value]);
  }
  expect(() => jsonConstructor("JSON_OBJECT", ["deeper", value])).toThrow(RangeError);
  expect(() => jsonConstructor("JSON_ARRAY", [value])).toThrow(RangeError);
});
