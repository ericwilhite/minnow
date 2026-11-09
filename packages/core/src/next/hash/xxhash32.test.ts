import fc from "fast-check";
import { afterAll, describe, expect, it } from "vitest";
import { createReach } from "../../testing/reach.js";
import { seedsFor } from "../../testing/seeds.js";
import { WORD_READ_BYTES, XxHash32Stream, xxHash32, xxHash32View } from "./xxhash32.js";

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
    const bytes = sanityBuffer(length);
    expect(xxHash32(bytes, seed)).toBe(expected);
    expect(xxHash32View(new DataView(bytes.buffer), seed)).toBe(expected);
  });

  it.each([
    ["", 0, 0x02cc5d05],
    ["", 1, 0x0b2cb792],
    ["a", 0, 0x550d7456],
    ["abc", 0, 0x32d153ff],
    ["Nobody inspects the spammish repetition", 0, 0xe2293b2f],
    ["The quick brown fox jumps over the lazy dog", 0, 0xe85ea4de],
  ])("matches the reference string vector %j seed %i", (text, seed, expected) => {
    const bytes = ascii(text);
    expect(xxHash32(bytes, seed)).toBe(expected);
    expect(xxHash32View(new DataView(bytes.buffer), seed)).toBe(expected);
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

  it("takes any number as the seed `seed >>> 0` (review B2-p2c)", () => {
    for (const length of [0, 5, 20, 300])
      for (const seed of [
        1.5,
        -0.5,
        7.999,
        Number.NaN,
        Infinity,
        -Infinity,
        2 ** 53 - 1,
        -(2 ** 53) + 3,
        2 ** 40 + 5,
        2 ** 32 + 7,
        -1,
      ]) {
        const bytes = sanityBuffer(length);
        expect(xxHash32(bytes, seed), `seed ${String(seed)}, ${String(length)} bytes`).toBe(
          xxHash32(bytes, seed >>> 0),
        );
        expect(xxHash32(bytes, seed)).toBe(referenceXxHash32(bytes, seed >>> 0));
        expect(xxHash32View(new DataView(bytes.buffer), seed)).toBe(
          referenceXxHash32(bytes, seed >>> 0),
        );
      }
  });

  it("refuses a range outside the input", () => {
    expect(() => xxHash32(new Uint8Array(4), 0, 2, 5)).toThrow(RangeError);
    expect(() => xxHash32(new Uint8Array(4), 0, 3, 2)).toThrow(RangeError);
    expect(() => xxHash32View(new DataView(new ArrayBuffer(4)), 0, 2, 5)).toThrow(RangeError);
    expect(() => xxHash32View(new DataView(new ArrayBuffer(4)), 0, 3, 2)).toThrow(RangeError);
  });
});

/**
 * A plain byte-at-a-time XXH32 written from the spec (the implementation this file had before
 * the word reads, ruling R15.8): the reference the word path is compared with.
 */
function referenceXxHash32(bytes: Uint8Array, seed: number): number {
  const P1 = 0x9e3779b1;
  const P2 = 0x85ebca77;
  const P3 = 0xc2b2ae3d;
  const P4 = 0x27d4eb2f;
  const P5 = 0x165667b1;
  const lane = (at: number): number =>
    (bytes[at] ?? 0) |
    ((bytes[at + 1] ?? 0) << 8) |
    ((bytes[at + 2] ?? 0) << 16) |
    ((bytes[at + 3] ?? 0) << 24);
  const round = (acc: number, input: number): number => {
    const sum = (acc + Math.imul(input, P2)) | 0;
    return Math.imul((sum << 13) | (sum >>> 19), P1);
  };
  const rotl = (value: number, bits: number): number => (value << bits) | (value >>> (32 - bits));
  let at = 0;
  let hash: number;
  if (bytes.length >= 16) {
    const v = [(seed + P1 + P2) | 0, (seed + P2) | 0, seed | 0, (seed - P1) | 0];
    for (; at + 16 <= bytes.length; at += 16)
      for (let k = 0; k < 4; k++) v[k] = round(v[k] ?? 0, lane(at + 4 * k));
    hash =
      (rotl(v[0] ?? 0, 1) + rotl(v[1] ?? 0, 7) + rotl(v[2] ?? 0, 12) + rotl(v[3] ?? 0, 18)) | 0;
  } else hash = (seed + P5) | 0;
  hash = (hash + bytes.length) | 0;
  for (; at + 4 <= bytes.length; at += 4)
    hash = Math.imul(rotl((hash + Math.imul(lane(at), P3)) | 0, 17), P4);
  for (; at < bytes.length; at++)
    hash = Math.imul(rotl((hash + Math.imul(bytes[at] ?? 0, P5)) | 0, 11), P1);
  hash = Math.imul(hash ^ (hash >>> 15), P2);
  hash = Math.imul(hash ^ (hash >>> 13), P3);
  return (hash ^ (hash >>> 16)) >>> 0;
}

describe("xxHash32 word reads (ruling R15.8)", () => {
  // Derived with the reference CLI (`xxhsum -H0`, xxHash 0.8.4, seed 0) from the sanity buffer:
  // lengths either side of WORD_READ_BYTES, a stripe boundary, and a 1 MiB + 5 blob.
  it.each([
    [255, 0xe048e445],
    [256, 0x520cb910],
    [257, 0x9566588e],
    [271, 0xbb29abc8],
    [272, 0x42ab99e9],
    [4099, 0x64e581b2],
    [1_048_581, 0x5db1695a],
  ])("matches the reference CLI: length %i", (length, expected) => {
    expect(WORD_READ_BYTES).toBe(256);
    const bytes = sanityBuffer(length);
    expect(xxHash32(bytes, 0)).toBe(expected);
    expect(xxHash32View(new DataView(bytes.buffer), 0)).toBe(expected);
    // The same bytes at an odd offset in a larger buffer, and at the end of it.
    const shifted = new Uint8Array(length + 7);
    shifted.set(sanityBuffer(length), 3);
    expect(xxHash32(shifted, 0, 3, 3 + length)).toBe(expected);
    expect(xxHash32(new Uint8Array(shifted.buffer, 3, length), 0)).toBe(expected);
  });

  it("matches the byte-wise reference for every short and boundary length at every alignment", () => {
    const source = sanityBuffer(2 * WORD_READ_BYTES + 64);
    const lengths = [
      ...Array.from({ length: 48 }, (_, k) => k), // every short input
      WORD_READ_BYTES - 17,
      WORD_READ_BYTES - 16,
      WORD_READ_BYTES - 1,
      WORD_READ_BYTES,
      WORD_READ_BYTES + 1,
      WORD_READ_BYTES + 15,
      WORD_READ_BYTES + 16,
      2 * WORD_READ_BYTES + 3,
    ];
    for (const length of lengths)
      for (let offset = 0; offset < 8; offset++)
        for (const seed of [0, 1, PRIME32, 0xffff_ffff]) {
          const copy = source.slice(offset, offset + length);
          const expected = referenceXxHash32(copy, seed);
          expect(xxHash32(source, seed, offset, offset + length)).toBe(expected);
          // The input ending exactly at its buffer's end (no readable byte after it).
          const tight = new Uint8Array(new ArrayBuffer(offset + length), offset, length);
          tight.set(copy);
          expect(xxHash32(tight, seed)).toBe(expected);
        }
  });

  it("reads a SharedArrayBuffer view like any other", () => {
    const length = 3 * WORD_READ_BYTES + 5;
    const shared = new Uint8Array(new SharedArrayBuffer(length + 9), 9, length);
    shared.set(sanityBuffer(length));
    expect(xxHash32(shared, 7)).toBe(referenceXxHash32(sanityBuffer(length), 7));
  });

  const reach = createReach("next-xxhash32", [
    "byte reads",
    "word reads",
    "word reads at an odd byte offset",
    "a range ending at its buffer's end",
    "reused view short words",
    "reused view long words",
    "reused view odd offset",
    "reused view byte tail",
  ]);
  afterAll(() => {
    reach.assertReached();
  });

  it.each(seedsFor("next-xxhash32", [0x5eed, 0x91]))(
    "equals the byte-wise reference on generated inputs and ranges (seed %i)",
    (seed) => {
      const input = fc
        .record({
          bytes: fc.oneof(
            fc.uint8Array({ maxLength: 64 }),
            fc.uint8Array({ minLength: WORD_READ_BYTES, maxLength: 6000 }),
          ),
          lead: fc.nat({ max: 15 }),
          trail: fc.nat({ max: 15 }),
          seed: fc.nat({ max: 0xffff_ffff }),
        })
        .chain((each) =>
          fc
            .tuple(
              fc.nat({ max: each.bytes.length }),
              fc.nat({ max: each.bytes.length }),
              fc.boolean(),
            )
            .map(([a, b, whole]) =>
              whole
                ? { ...each, start: 0, end: each.bytes.length }
                : { ...each, start: Math.min(a, b), end: Math.max(a, b) },
            ),
        );
      fc.assert(
        fc.property(input, ({ bytes, lead, trail, seed: hashSeed, start, end }) => {
          // The bytes at `lead` inside a buffer with `trail` bytes after them.
          const view = new Uint8Array(
            new ArrayBuffer(lead + bytes.length + trail),
            lead,
            bytes.length,
          );
          view.set(bytes);
          const length = end - start;
          reach.beginHistory();
          reach.reach(length >= WORD_READ_BYTES ? "word reads" : "byte reads");
          if (length >= WORD_READ_BYTES && (lead + start) % 4 !== 0)
            reach.reach("word reads at an odd byte offset");
          if (trail === 0 && end === bytes.length)
            reach.reach("a range ending at its buffer's end");
          expect(xxHash32(view, hashSeed, start, end)).toBe(
            referenceXxHash32(bytes.slice(start, end), hashSeed),
          );
          expect(
            xxHash32View(
              new DataView(view.buffer, view.byteOffset, view.byteLength),
              hashSeed,
              start,
              end,
            ),
          ).toBe(referenceXxHash32(bytes.slice(start, end), hashSeed));
          if (length >= 4)
            reach.reach(
              length < WORD_READ_BYTES ? "reused view short words" : "reused view long words",
            );
          if (length >= 4 && (lead + start) % 4 !== 0) reach.reach("reused view odd offset");
          if (length % 4 !== 0) reach.reach("reused view byte tail");
        }),
        { seed, numRuns: 300 },
      );
    },
  );
});

describe("XxHash32Stream (the spec's streaming form)", () => {
  it("equals the one-shot hash for every split of the published sanity buffers", () => {
    for (const length of [0, 1, 14, 15, 16, 17, 31, 32, 33, 222, 1000]) {
      const bytes = sanityBuffer(length);
      for (const seed of [0, 0x9e3779b1, 7]) {
        const expected = xxHash32(bytes, seed);
        for (const piece of [1, 3, 15, 16, 17, 100]) {
          const stream = new XxHash32Stream(seed);
          for (let at = 0; at < length; at += piece)
            stream.update(bytes, at, Math.min(length, at + piece));
          expect(stream.digest(), `length ${String(length)} piece ${String(piece)}`).toBe(expected);
        }
      }
    }
  });

  it("takes its seed as seed >>> 0, like the one-shot hash (review B7-p2c-3 #3)", () => {
    // Fractional, negative, non-finite and out-of-range numbers: the one-shot contract's
    // ToUint32, compared with the hash of the normalized seed and with the raw seed.
    const seeds = [-0.5, 0.5, -1.5, 2.9, -0, NaN, Infinity, -Infinity, 2 ** 32, 2 ** 32 + 5];
    seeds.push(-(2 ** 31) - 1, 2 ** 53, -(2 ** 53), 1e300, 0xffff_ffff + 0.25);
    for (const length of [0, 5, 16, 17, 300])
      for (const seed of seeds) {
        const bytes = sanityBuffer(length);
        const expected = xxHash32(bytes, seed >>> 0);
        expect(xxHash32(bytes, seed)).toBe(expected);
        const stream = new XxHash32Stream(seed);
        stream.update(bytes, 0, length >> 1).update(bytes, length >> 1, length);
        expect(stream.digest(), `seed ${String(seed)} length ${String(length)}`).toBe(expected);
        expect(new XxHash32Stream(seed).update(bytes).digest()).toBe(xxHash32(bytes, seed));
      }
    // The reviewer's case: empty input, seed −0.5 (ToUint32 = 0).
    expect(new XxHash32Stream(-0.5).digest()).toBe(xxHash32(new Uint8Array(0), -0.5));
    expect(xxHash32(new Uint8Array(0), -0.5)).toBe(xxHash32(new Uint8Array(0), 0));
  });

  it("matches the published sanity vectors when fed in pieces", () => {
    // xxhsum's sanity checks (also in the one-shot tests above).
    expect(
      new XxHash32Stream(0)
        .update(sanityBuffer(222), 0, 100)
        .update(sanityBuffer(222), 100, 222)
        .digest(),
    ).toBe(xxHash32(sanityBuffer(222), 0));
  });
});
