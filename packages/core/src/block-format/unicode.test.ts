import fc from "fast-check";
import { describe, expect, it } from "vitest";
import { assertWellFormedString, wellFormedUtf8ByteLength } from "./unicode.js";
import { seedsFor } from "../testing/seeds.js";

/** Seeds for this file's generated properties; `npm run soak` explores others. */
const SEEDS = seedsFor("unicode-properties", [0x7f8]);

const RUNS = 300;

describe("well-formed persistent strings", () => {
  it.each(SEEDS)(
    "measures the same exact length as TextEncoder for arbitrary valid Unicode (seed %i)",
    (seed) => {
      fc.assert(
        fc.property(fc.string({ unit: "grapheme", maxLength: 2_000 }), (value) => {
          expect(wellFormedUtf8ByteLength(value)).toBe(new TextEncoder().encode(value).byteLength);
        }),
        { seed, numRuns: RUNS },
      );
    },
  );

  it.each(SEEDS)("accepts every valid supplementary code point (seed %i)", (seed) => {
    fc.assert(
      fc.property(fc.integer({ min: 0x10_000, max: 0x10_ffff }), (codePoint) => {
        const value = String.fromCodePoint(codePoint);
        expect(wellFormedUtf8ByteLength(value)).toBe(4);
        expect(() => assertWellFormedString(value)).not.toThrow();
      }),
      { seed, numRuns: RUNS },
    );
  });

  it.each(SEEDS)(
    "rejects every isolated high surrogate at its exact UTF-16 index (seed %i)",
    (seed) => {
      fc.assert(
        fc.property(fc.integer({ min: 0xd800, max: 0xdbff }), (codeUnit) => {
          const value = `ok${String.fromCharCode(codeUnit)}x`;
          expect(() => wellFormedUtf8ByteLength(value)).toThrow(
            "unpaired surrogate at UTF-16 index 2",
          );
        }),
        { seed, numRuns: RUNS },
      );
    },
  );

  it.each(SEEDS)("rejects every isolated low surrogate with caller context (seed %i)", (seed) => {
    fc.assert(
      fc.property(fc.integer({ min: 0xdc00, max: 0xdfff }), (codeUnit) => {
        const value = `a${String.fromCharCode(codeUnit)}`;
        expect(() => assertWellFormedString(value, "Primary key")).toThrow(
          "Primary key contains an unpaired surrogate at UTF-16 index 1",
        );
      }),
      { seed, numRuns: RUNS },
    );
  });

  it("reports runtime type errors deterministically", () => {
    expect(() => wellFormedUtf8ByteLength(1 as never)).toThrow("String value must be a string");
  });
});
