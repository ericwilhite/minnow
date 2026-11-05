import fc from "fast-check";
import { describe, expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import type { PhysicalType } from "../types.js";
import {
  MalformedArrayError,
  decodeArrayValue,
  encodeArrayValue,
  type ArrayElement,
  type ArrayValue,
} from "./array-value.js";

const SEEDS = seedsFor("next-array-value", [0xa77a]);

const ELEMENTS: ReadonlyArray<[PhysicalType, fc.Arbitrary<ArrayElement>]> = [
  [{ kind: "bool" }, fc.boolean()],
  [{ kind: "int32", bits: 16 }, fc.integer({ min: -32_768, max: 32_767 })],
  [{ kind: "int32", bits: 32 }, fc.integer()],
  [{ kind: "int64" }, fc.bigInt({ min: -(2n ** 63n), max: 2n ** 63n - 1n })],
  [{ kind: "float32" }, fc.float({ noNaN: true }).map(Math.fround)],
  [{ kind: "float64" }, fc.double({ noNaN: true })],
  [
    { kind: "decimal", precision: null, scale: null },
    fc.oneof(
      fc.record({
        coefficient: fc.bigInt({ min: -(10n ** 40n), max: 10n ** 40n }),
        scale: fc.integer({ min: 0, max: 60 }),
      }),
      fc.constantFrom(
        { coefficient: 0n, scale: 0, special: "NaN" as const },
        { coefficient: 0n, scale: 0, special: "-Infinity" as const },
      ),
    ),
  ],
  [{ kind: "date" }, fc.integer()],
  [{ kind: "time" }, fc.bigInt({ min: 0n, max: 86_400_000_000n })],
  [{ kind: "timestamp", zoned: true }, fc.bigInt({ min: -(2n ** 63n), max: 2n ** 63n - 1n })],
  [
    { kind: "interval" },
    fc.record({
      months: fc.integer(),
      days: fc.integer(),
      micros: fc.bigInt({ min: -(2n ** 63n), max: 2n ** 63n - 1n }),
    }),
  ],
  [{ kind: "text" }, fc.uint8Array({ maxLength: 6 })],
  [{ kind: "bytes" }, fc.uint8Array({ maxLength: 6 })],
  [{ kind: "uuid" }, fc.uint8Array({ minLength: 16, maxLength: 16 })],
  [
    { kind: "enum", name: "mood", labels: ["sad", "ok"] },
    fc.constantFrom("sad", "ok").map((s) => new TextEncoder().encode(s)),
  ],
];

const shapes = fc.oneof(
  fc.constant({ dimensions: [] as number[], lowerBounds: [] as number[] }),
  fc
    .tuple(fc.integer({ min: 1, max: 4 }), fc.integer({ min: -3, max: 3 }))
    .map(([n, lb]) => ({ dimensions: [n], lowerBounds: [lb] })),
  fc
    .tuple(fc.integer({ min: 1, max: 3 }), fc.integer({ min: 1, max: 3 }))
    .map(([a, b]) => ({ dimensions: [a, b], lowerBounds: [1, 0] })),
);

describe("array stored values (review A1-3 finding 1)", () => {
  it.each(SEEDS)("round-trips every supported element type exactly (seed %i)", (seed) => {
    for (const [type, element] of ELEMENTS)
      fc.assert(
        fc.property(
          shapes.chain((shape) => {
            const count =
              shape.dimensions.length === 0 ? 0 : shape.dimensions.reduce((p, n) => p * n, 1);
            return fc
              .array(fc.option(element, { freq: 4 }), { minLength: count, maxLength: count })
              .map((elements): ArrayValue => ({ ...shape, elements }));
          }),
          (value) => {
            const decoded = decodeArrayValue(type, encodeArrayValue(type, value));
            expect(decoded).toEqual({
              ...value,
              elements: value.elements.map((item) =>
                typeof item === "number" && Object.is(item, -0) ? -0 : item,
              ),
            });
          },
        ),
        { seed, numRuns: 200 },
      );
  });

  it("keeps what an equality key folds: interval fields and NUMERIC display scale (review probe)", () => {
    const interval: PhysicalType = { kind: "interval" };
    const month = encodeArrayValue(interval, {
      dimensions: [1],
      lowerBounds: [1],
      elements: [{ months: 1, days: 0, micros: 0n }],
    });
    const thirty = encodeArrayValue(interval, {
      dimensions: [1],
      lowerBounds: [1],
      elements: [{ months: 0, days: 30, micros: 0n }],
    });
    expect(Buffer.from(month).equals(Buffer.from(thirty))).toBe(false);
    expect(decodeArrayValue(interval, month).elements[0]).toEqual({
      months: 1,
      days: 0,
      micros: 0n,
    });
    const decimal: PhysicalType = { kind: "decimal", precision: null, scale: null };
    const scaled = decodeArrayValue(
      decimal,
      encodeArrayValue(decimal, {
        dimensions: [2],
        lowerBounds: [1],
        elements: [
          { coefficient: 150n, scale: 2 },
          { coefficient: 15n, scale: 1 },
        ],
      }),
    );
    expect(scaled.elements).toEqual([
      { coefficient: 150n, scale: 2 },
      { coefficient: 15n, scale: 1 },
    ]);
  });

  it("refuses unsupported element types and malformed values", () => {
    expect(() =>
      encodeArrayValue(
        { kind: "json", binary: true },
        { dimensions: [], lowerBounds: [], elements: [] },
      ),
    ).toThrow(TypeError);
    const int: PhysicalType = { kind: "int32", bits: 32 };
    expect(() =>
      encodeArrayValue(int, { dimensions: [2], lowerBounds: [1], elements: [1] }),
    ).toThrow(MalformedArrayError);
    const bytes = encodeArrayValue(int, { dimensions: [2], lowerBounds: [1], elements: [1, null] });
    for (let cut = 0; cut < bytes.length; cut++)
      expect(() => decodeArrayValue(int, bytes.slice(0, cut)), `cut ${String(cut)}`).toThrow(
        MalformedArrayError,
      );
    expect(() => decodeArrayValue({ kind: "text" }, bytes)).toThrow(MalformedArrayError);
    expect(() => decodeArrayValue(int, Uint8Array.from([...bytes, 0]))).toThrow(
      MalformedArrayError,
    );
  });
});

describe("array validation before writing (review A1-4 follow-up)", () => {
  const int: PhysicalType = { kind: "int32", bits: 32 };
  const refuse = (element: PhysicalType, value: ArrayValue, label: string) => {
    expect(() => encodeArrayValue(element, value), label).toThrow(MalformedArrayError);
  };
  const one = (item: ArrayElement): ArrayValue => ({
    dimensions: [1],
    lowerBounds: [1],
    elements: [item],
  });

  it("refuses bounds it would otherwise narrow (review probe: lower bound 2147483648)", () => {
    refuse(int, { dimensions: [1], lowerBounds: [2_147_483_648], elements: [3] }, "lower bound");
    refuse(int, { dimensions: [2], lowerBounds: [2_147_483_647], elements: [1, 2] }, "upper bound");
    refuse(int, { dimensions: [-1], lowerBounds: [1], elements: [] }, "negative length");
    refuse(int, { dimensions: [1.5], lowerBounds: [1], elements: [1] }, "fractional length");
    const edge = { dimensions: [1], lowerBounds: [-2_147_483_648], elements: [2_147_483_647] };
    expect(decodeArrayValue(int, encodeArrayValue(int, edge))).toEqual(edge);
  });

  it("refuses elements outside their type's range", () => {
    const cases: Array<[PhysicalType, ArrayElement, string]> = [
      [{ kind: "int32", bits: 16 }, 32_768, "smallint"],
      [int, 1.5, "fractional int"],
      [int, 2 ** 31, "int32 overflow"],
      [{ kind: "int64" }, 2n ** 63n, "int64 overflow"],
      [{ kind: "time" }, -1n, "negative time"],
      [{ kind: "float32" }, 0.1, "not a float32"],
      [
        { kind: "decimal", precision: null, scale: null },
        { coefficient: 1n, scale: 16_384 },
        "scale",
      ],
      [{ kind: "interval" }, { months: 2 ** 31, days: 0, micros: 0n }, "interval months"],
      [{ kind: "uuid" }, new Uint8Array(15), "uuid length"],
      [
        { kind: "enum", name: "mood", labels: ["sad"] },
        new TextEncoder().encode("glad"),
        "enum label",
      ],
      [{ kind: "text" }, 5, "text"],
    ];
    for (const [element, item, label] of cases) refuse(element, one(item), label);
  });

  it("refuses NUMERIC elements PostgreSQL cannot hold (review A1-5 probe: 10^131072)", () => {
    const decimal: PhysicalType = { kind: "decimal", precision: null, scale: null };
    const accept = (item: ArrayElement) =>
      expect(decodeArrayValue(decimal, encodeArrayValue(decimal, one(item))).elements).toEqual([
        item,
      ]);
    // PostgreSQL (PGlite): '1' followed by 131,071 zeros is accepted; one more zero overflows.
    accept({ coefficient: 10n ** 131_071n, scale: 0 });
    accept({ coefficient: -(10n ** 131_071n), scale: 0 });
    accept({ coefficient: 10n ** 131_073n, scale: 2 });
    accept({ coefficient: 10n ** 16_383n, scale: 16_383 });
    refuse(decimal, one({ coefficient: 10n ** 131_072n, scale: 0 }), "131,073 integer digits");
    refuse(decimal, one({ coefficient: 10n ** 131_074n, scale: 2 }), "with a scale");
    const started = performance.now();
    refuse(decimal, one({ coefficient: 10n ** 2_000_000n, scale: 0 }), "huge");
    expect(performance.now() - started).toBeLessThan(500);
  });

  it.each(SEEDS)(
    "bounds NUMERIC integer digits exactly at the limit (seed %i)",
    (seed) => {
      const decimal: PhysicalType = { kind: "decimal", precision: null, scale: null };
      let accepted = 0;
      let refused = 0;
      fc.assert(
        fc.property(
          fc.integer({ min: 0, max: 64 }),
          fc.integer({ min: -3, max: 3 }),
          fc.oneof(fc.integer({ min: 1, max: 9 }), fc.constant(0)),
          fc.boolean(),
          (scale, delta, lead, negative) => {
            // `digits` digits: a leading digit then zeros, or (lead 0) all nines; integer digits
            // = digits − scale sits within 3 of the limit, where the size estimate is tightest.
            const digits = 131_072 + scale + delta;
            const magnitude =
              lead === 0 ? 10n ** BigInt(digits) - 1n : BigInt(lead) * 10n ** BigInt(digits - 1);
            const value = one({ coefficient: negative ? -magnitude : magnitude, scale });
            if (delta <= 0) {
              accepted += 1;
              expect(decodeArrayValue(decimal, encodeArrayValue(decimal, value))).toEqual(value);
            } else {
              refused += 1;
              refuse(decimal, value, `${String(digits)} digits at scale ${String(scale)}`);
            }
          },
        ),
        { seed, numRuns: 300 },
      );
      // Declared reach: both sides of the limit.
      expect(accepted).toBeGreaterThan(50);
      expect(refused).toBeGreaterThan(50);
      // 300 round trips of ~131k-digit coefficients: ~4 s alone, but well past the default 20 s
      // timeout when the machine is saturated. A load timeout is not a failure of the law.
    },
    120_000,
  );

  it("stores an empty array only with zero dimensions (review A1-5 probe)", () => {
    const smallint: PhysicalType = { kind: "int32", bits: 16 };
    refuse(smallint, { dimensions: [0], lowerBounds: [-2_147_483_648], elements: [] }, "INT32_MIN");
    refuse(int, { dimensions: [0], lowerBounds: [1], elements: [] }, "one empty dimension");
    refuse(int, { dimensions: [2, 0], lowerBounds: [1, 1], elements: [] }, "an empty inner one");
    const empty = { dimensions: [], lowerBounds: [], elements: [] };
    expect(decodeArrayValue(int, encodeArrayValue(int, empty))).toEqual(empty);
  });

  it("validates what it decodes (review A1-5 probe: SMALLINT 40000, upper bound past int32)", () => {
    const smallint: PhysicalType = { kind: "int32", bits: 16 };
    // The reviewer's bytes: one SMALLINT at lower bound 2147483647, rewritten to two elements
    // 40000 and 40001.
    const bytes = encodeArrayValue(smallint, {
      dimensions: [1],
      lowerBounds: [2_147_483_647],
      elements: [1],
    });
    const forged = new Uint8Array(20);
    forged.set(bytes);
    const view = new DataView(forged.buffer);
    view.setInt32(4, 2, true);
    view.setInt32(12, 40_000, true);
    view.setInt32(16, 40_001, true);
    expect(() => decodeArrayValue(smallint, forged)).toThrow(/upper bound/u);
    view.setInt32(8, 1, true); // a valid lower bound: now the elements are what is wrong
    expect(() => decodeArrayValue(smallint, forged)).toThrow(/not a SMALLINT/u);
    view.setInt32(12, 7, true);
    view.setInt32(16, -7, true);
    expect(decodeArrayValue(smallint, forged).elements).toEqual([7, -7]);
    // A stored empty dimension is refused on read too.
    const zero = encodeArrayValue(int, { dimensions: [1], lowerBounds: [1], elements: [5] });
    const emptyDimension = Uint8Array.from([1, zero[1] ?? 0, 1, 0, 0, 0, 0, 0, 1, 0, 0, 0]);
    expect(() => decodeArrayValue(int, emptyDimension)).toThrow(/no dimensions/u);
  });
});
