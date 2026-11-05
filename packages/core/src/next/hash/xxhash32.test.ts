import { describe, expect, it } from "vitest";
import { xxHash32 } from "./xxhash32.js";

const PRIME32 = 2_654_435_761;
const ascii = (text: string): Uint8Array => new TextEncoder().encode(text);

/**
 * The sanity buffer of the reference `xxhsum` (xxHash ≥ 0.7, `BMK_sanityCheck`): byte i is the
 * top byte of a 64-bit generator that starts at PRIME32 and is multiplied by PRIME64 each step.
 */
function sanityBuffer(length: number): Uint8Array {
  const buffer = new Uint8Array(length);
  let generator = 2_654_435_761n;
  for (let index = 0; index < length; index++) {
    buffer[index] = Number(generator >> 56n);
    generator = (generator * 11_400_714_785_074_694_797n) & 0xffff_ffff_ffff_ffffn;
  }
  return buffer;
}

describe("xxHash32", () => {
  // Published vectors: xxhsum's sanity checks (lengths 0, 1, 14, 222 with seeds 0 and PRIME32),
  // and the common string vectors. Each was also re-derived with the reference `xxh32sum` CLI
  // (xxHash 0.8) while writing this test, including the 2,367-byte case.
  it.each([
    [0, 0, 0x02cc5d05],
    [0, PRIME32, 0x36b78ae7],
    [1, 0, 0xcf65b03e],
    [1, PRIME32, 0xb4545aa4],
    [14, 0, 0x1208e7e2],
    [14, PRIME32, 0x6af1d1fe],
    [222, 0, 0x5bd11dbd],
    [222, PRIME32, 0x58803c5f],
    [2367, 0, 0x4c8a9773],
    [2367, PRIME32, 0x6d5366f6],
  ])("matches the reference sanity vector: length %i, seed %i", (length, seed, expected) => {
    expect(xxHash32(sanityBuffer(length), seed)).toBe(expected);
  });

  it.each([
    ["", 0, 0x02cc5d05],
    ["", 1, 0x0b2cb792],
    ["a", 0, 0x550d7456],
    ["abc", 0, 0x32d153ff],
    ["Nobody inspects the spammish repetition", 0, 0xe2293b2f],
    ["The quick brown fox jumps over the lazy dog", 0, 0xe85ea4de],
  ])("matches the reference string vector %j seed %i", (text, seed, expected) => {
    expect(xxHash32(ascii(text), seed)).toBe(expected);
  });

  it("hashes a subrange exactly like a copy of it, at every alignment", () => {
    const source = sanityBuffer(300);
    for (let start = 0; start < 8; start++)
      for (const length of [0, 3, 4, 15, 16, 17, 31, 32, 33, 200]) {
        const copy = source.slice(start, start + length);
        expect(xxHash32(source, 7, start, start + length)).toBe(xxHash32(copy, 7));
        const view = new Uint8Array(source.buffer, start, length);
        expect(xxHash32(view, 7)).toBe(xxHash32(copy, 7));
      }
  });

  it("accepts any unsigned 32-bit seed and returns an unsigned result", () => {
    const bytes = sanityBuffer(40);
    expect(xxHash32(bytes, 0xffff_ffff)).toBe(xxHash32(bytes, -1));
    expect(xxHash32(bytes, 0xffff_ffff)).toBeGreaterThanOrEqual(0);
  });

  it("refuses a range outside the input", () => {
    expect(() => xxHash32(new Uint8Array(4), 0, 2, 5)).toThrow(RangeError);
    expect(() => xxHash32(new Uint8Array(4), 0, 3, 2)).toThrow(RangeError);
  });
});
