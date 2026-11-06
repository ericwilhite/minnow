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
import { DistinctSketch } from "./sketch.js";

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
