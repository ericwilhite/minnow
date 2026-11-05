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
    .tuple(fc.integer({ min: 0, max: 4 }), fc.integer({ min: -3, max: 3 }))
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
