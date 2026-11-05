import fc from "fast-check";
import { describe, expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import {
  arrayEqualityKey,
  compareArrays,
  compareJsonb,
  jsonbEqualityKey,
  parseJsonb,
  type SqlArray,
} from "./semantic.js";

const SEEDS = seedsFor("next-semantic-compare", [0x5e3a]);
const sameBytes = (a: Uint8Array, b: Uint8Array) =>
  a.length === b.length && a.every((value, index) => value === b[index]);
const j = (text: string) => parseJsonb(text);

describe("JSONB semantic order and equality (review A1-2 finding 3)", () => {
  it("compares numbers numerically and exactly (review probe: 2 vs 10, 1 vs 1.0)", () => {
    expect(compareJsonb(j("2"), j("10"))).toBe(-1);
    expect(compareJsonb(j("1"), j("1.0"))).toBe(0);
    expect(sameBytes(jsonbEqualityKey(j("1")), jsonbEqualityKey(j("1.0")))).toBe(true);
    expect(sameBytes(jsonbEqualityKey(j("1")), jsonbEqualityKey(j("1e0")))).toBe(true);
    expect(compareJsonb(j("0.1"), j("1e-1"))).toBe(0);
  });

  it("follows PostgreSQL's type ranks, sizes and key order", () => {
    const ascending = [
      "[]",
      "null",
      '""',
      '"b"',
      "-1",
      "3",
      "false",
      "true",
      "[1]",
      "[1,2]",
      "{}",
      '{"a":1}',
      '{"b":0,"aa":0}',
    ];
    for (let index = 1; index < ascending.length; index++)
      expect(
        compareJsonb(j(ascending[index - 1] ?? ""), j(ascending[index] ?? "")),
        `${ascending[index - 1] ?? ""} < ${ascending[index] ?? ""}`,
      ).toBe(-1);
    // Nested empty arrays are ordinary arrays (only a top-level empty array sorts below null).
    expect(compareJsonb(j("[[]]"), j("[null]"))).toBe(1);
    expect(compareJsonb(j('{"b":1,"a":2,"b":3}'), j('{"a":2,"b":3}'))).toBe(0);
  });

  const leaf = fc.oneof(
    fc.constant("null"),
    fc.boolean().map(String),
    fc.constantFrom('"a"', '"b"', '"é"', '""'),
    fc.integer({ min: -3, max: 12 }).map(String),
    fc.constantFrom("1.0", "1e1", "0.50", "-0", "2.5e-1", "10.00"),
  );
  const tree: fc.Arbitrary<string> = fc.letrec((tie) => ({
    node: fc.oneof(
      { depthSize: "small" },
      leaf,
      fc.array(tie("node"), { maxLength: 3 }).map((items) => `[${(items as string[]).join(",")}]`),
      fc
        .array(fc.tuple(fc.constantFrom("a", "b", "aa"), tie("node")), { maxLength: 3 })
        .map(
          (pairs) =>
            `{${(pairs as Array<[string, string]>).map(([k, v]) => `"${k}":${v}`).join(",")}}`,
        ),
    ),
  })).node;

  it.each(SEEDS)("is a total order whose zero is equality-key equality (seed %i)", (seed) => {
    fc.assert(
      fc.property(tree, tree, tree, (x, y, z) => {
        const [a, b, c] = [j(x), j(y), j(z)];
        expect(compareJsonb(a, b)).toBe(-compareJsonb(b, a) || 0);
        expect(compareJsonb(a, b) === 0).toBe(sameBytes(jsonbEqualityKey(a), jsonbEqualityKey(b)));
        if (compareJsonb(a, b) <= 0 && compareJsonb(b, c) <= 0)
          expect(compareJsonb(a, c)).toBeLessThanOrEqual(0);
      }),
      { seed, numRuns: 3000 },
    );
  });
});

describe("SQL array semantic order and equality", () => {
  const int = { kind: "int32", bits: 32 } as const;
  const arr = (
    elements: Array<number | null>,
    dimensions = [elements.length],
    lowerBounds = [1],
  ): SqlArray => ({
    element: int,
    dimensions,
    lowerBounds,
    elements,
  });

  it("compares elements before metadata, as array_cmp does (review probe)", () => {
    expect(compareArrays(arr([2]), arr([1, 9, 9]))).toBe(1);
    expect(compareArrays(arr([1, 2]), arr([1, 2, 3]))).toBe(-1);
    expect(compareArrays(arr([1, null]), arr([1, 5]))).toBe(1);
    expect(compareArrays(arr([1, 2, 3, 4]), arr([1, 2, 3, 4], [2, 2], [1, 1]))).toBe(-1);
    expect(compareArrays(arr([1]), arr([1], [1], [0]))).toBe(1);
  });

  const arrays = fc
    .tuple(
      fc.array(fc.option(fc.integer({ min: -2, max: 2 }), { freq: 4 }), { maxLength: 4 }),
      fc.boolean(),
      fc.integer({ min: 0, max: 1 }),
    )
    .map(([elements, square, bound]) =>
      square && elements.length === 4
        ? arr(elements, [2, 2], [bound, 1])
        : arr(elements, [elements.length], [bound]),
    );

  it.each(SEEDS)("is a total order whose zero is equality-key equality (seed %i)", (seed) => {
    fc.assert(
      fc.property(arrays, arrays, arrays, (a, b, c) => {
        expect(compareArrays(a, b)).toBe(-compareArrays(b, a) || 0);
        expect(compareArrays(a, b) === 0).toBe(sameBytes(arrayEqualityKey(a), arrayEqualityKey(b)));
        if (compareArrays(a, b) <= 0 && compareArrays(b, c) <= 0)
          expect(compareArrays(a, c)).toBeLessThanOrEqual(0);
      }),
      { seed, numRuns: 3000 },
    );
  });
});
