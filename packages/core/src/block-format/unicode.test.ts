import fc from "fast-check";
import { describe, expect, it } from "vitest";
import { assertWellFormedString, wellFormedUtf8ByteLength } from "./unicode.js";

const RUNS = 300;

describe("well-formed persistent strings", () => {
  it("measures the same exact length as TextEncoder for arbitrary valid Unicode", () => {
    fc.assert(
      fc.property(fc.string({ unit: "grapheme", maxLength: 2_000 }), (value) => {
        expect(wellFormedUtf8ByteLength(value)).toBe(new TextEncoder().encode(value).byteLength);
      }),
      { numRuns: RUNS },
    );
  });

  it("accepts every valid supplementary code point", () => {
    fc.assert(
      fc.property(fc.integer({ min: 0x10_000, max: 0x10_ffff }), (codePoint) => {
        const value = String.fromCodePoint(codePoint);
        expect(wellFormedUtf8ByteLength(value)).toBe(4);
        expect(() => assertWellFormedString(value)).not.toThrow();
      }),
      { numRuns: RUNS },
    );
  });

  it("rejects every isolated high surrogate at its exact UTF-16 index", () => {
    fc.assert(
      fc.property(fc.integer({ min: 0xd800, max: 0xdbff }), (codeUnit) => {
        const value = `ok${String.fromCharCode(codeUnit)}x`;
        expect(() => wellFormedUtf8ByteLength(value)).toThrow(
          "unpaired surrogate at UTF-16 index 2",
        );
      }),
      { numRuns: RUNS },
    );
  });

  it("rejects every isolated low surrogate with caller context", () => {
    fc.assert(
      fc.property(fc.integer({ min: 0xdc00, max: 0xdfff }), (codeUnit) => {
        const value = `a${String.fromCharCode(codeUnit)}`;
        expect(() => assertWellFormedString(value, "Primary key")).toThrow(
          "Primary key contains an unpaired surrogate at UTF-16 index 1",
        );
      }),
      { numRuns: RUNS },
    );
  });

  it("reports runtime type errors deterministically", () => {
    expect(() => wellFormedUtf8ByteLength(1 as never)).toThrow("String value must be a string");
  });
});
