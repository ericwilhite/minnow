/**
 * Review B2-p2i (rebuild/format-p2i 47ab0bdb).
 * #1: the scaled-float reader refused valid pages near 2^53: it checked base + the largest delta
 * the width could hold, not the deltas stored. Writer and reader now share one rule
 * (format.ts `scaledFloatSafe`) on the stored deltas, and a page whose stored deltas do leave the
 * safe range is still refused.
 * #2: a float page of distinct NaN payloads became one constant (`Object.is` treats all NaNs
 * alike). Constant selection now compares raw bit patterns, and constants are stored and decoded
 * as raw bytes.
 * #3: `loadTogether` with a reader twice (or overlapping page arrays) installed a page twice and
 * leaked its lease. Requests are now merged by reader before any work starts.
 * The generated coverage is in scaled-float.test.ts (#1, #2) and export-path.test.ts (#3).
 */
import { describe, expect, it } from "vitest";
import { PageEncoding } from "../format/manifest.js";
import type { FlatVector } from "../vector/vector.js";
import { decodeRange, parsePage } from "./decode.js";
import { encodeFloatPage, PageScratch } from "./encode.js";
import { PageCorruptionError, ValueTag } from "./format.js";
import { ChunkReader } from "./reader.js";
import { DistinctSketch } from "./sketch.js";
import { defined, targetFor, writeGroup } from "./test-support.js";

/** Encodes `values` as one float64 page and decodes it as a reader does, into raw bits. */
function roundTrip(values: Float64Array): { encoding: number; bits: BigUint64Array } {
  const encoded = encodeFloatPage(
    values,
    null,
    values.length,
    new DistinctSketch(),
    new PageScratch(),
  );
  const bytes = encoded.bytes.slice();
  const entry = { ...encoded, firstRow: 0, offset: 0, length: bytes.length, checksum: 0 };
  const view = parsePage(bytes, entry, ValueTag.f64);
  const out = new Float64Array(values.length);
  const target: FlatVector = {
    form: "flat",
    type: { kind: "float64" },
    length: values.length,
    validity: null,
    data: { layout: "f64", values: out },
  };
  decodeRange(view, 0, values.length, target, 0);
  return { encoding: encoded.encoding, bits: new BigUint64Array(out.buffer) };
}

describe("review B2-p2i #1: scaled-float pages near 2^53", () => {
  it("round-trip safe integers just below 2^53 (the probe, inverted)", () => {
    const values = Float64Array.from({ length: 128 }, (_, i) => Number.MAX_SAFE_INTEGER - (i % 2));
    const { encoding, bits } = roundTrip(values);
    expect(encoding).toBe(PageEncoding.scaledFloat);
    expect([...bits]).toEqual([...new BigUint64Array(values.buffer)]);
  });

  it("still refuses a page whose stored deltas leave the safe range", () => {
    const values = Float64Array.from({ length: 128 }, (_, i) => 1000 + (i % 5));
    const encoded = encodeFloatPage(
      values,
      null,
      values.length,
      new DistinctSketch(),
      new PageScratch(),
    );
    expect(encoded.encoding).toBe(PageEncoding.scaledFloat);
    const bytes = encoded.bytes.slice();
    // The base (8 bytes after the 16-byte header) moved to 2^53 − 2: base + 4 is not safe.
    new Float64Array(bytes.buffer, bytes.byteOffset + 16, 1)[0] = Number.MAX_SAFE_INTEGER - 1;
    const entry = { ...encoded, firstRow: 0, offset: 0, length: bytes.length, checksum: 0 };
    expect(() => parsePage(bytes, entry, ValueTag.f64)).toThrow(PageCorruptionError);
  });
});

describe("review B2-p2i #2: every float64 bit pattern, constant pages included", () => {
  it("keeps distinct NaN payloads on an all-NaN page (the probe, inverted)", () => {
    const values = new Float64Array(128);
    const raw = new BigUint64Array(values.buffer);
    for (let i = 0; i < 128; i++) raw[i] = 0x7ff8_0000_0000_0001n + BigInt(i);
    const { encoding, bits } = roundTrip(values);
    expect(encoding).not.toBe(PageEncoding.constant);
    expect([...bits]).toEqual([...raw]);
  });

  it("keeps a constant page's own payload, signalling NaNs included", () => {
    for (const pattern of [
      0x7ff0_0000_0000_0001n,
      0xfff4_dead_beef_0001n,
      0x7ff8_0000_0000_0000n,
    ]) {
      const values = new Float64Array(64);
      new BigUint64Array(values.buffer).fill(pattern);
      const { encoding, bits } = roundTrip(values);
      expect(encoding).toBe(PageEncoding.constant);
      expect([...new Set(bits)]).toEqual([pattern]);
    }
  });
});

describe("review B2-p2i #3: loadTogether with a reader more than once", () => {
  it("installs each page once and gives every charge back (the probe, inverted)", async () => {
    const { space, group } = await writeGroup([
      { type: { kind: "float64" }, values: Array.from({ length: 128 }, (_, i) => i / 100) },
    ]);
    let used = 0;
    const budget = {
      allocate: (n: number) => ((used += n), true),
      free: (n: number) => void (used -= n),
    };
    const reader = await ChunkReader.open(space, defined(group.columns[0]), { budget, once: true });
    await ChunkReader.loadTogether([
      { reader, pages: [0] },
      { reader, pages: [0] },
    ]);
    const target = targetFor(reader, 128, 0);
    reader.decodeRange(0, 128, target, 0);
    reader.close();
    expect(used).toBe(0);
  });
});
