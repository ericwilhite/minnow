import { describe, expect, it } from "vitest";
import {
  gzipCodec,
  getCompressionMemoryBound,
  maximumRleCompressedLength,
  rleCodec,
  rleCompressionMemoryBound,
} from "./codecs.js";

describe("RLE compression", () => {
  it("preserves the deterministic pair encoding", async () => {
    await expect(rleCodec.compress(Uint8Array.of())).resolves.toEqual(Uint8Array.of());
    await expect(rleCodec.compress(Uint8Array.of(7, 7, 7, 2, 2, 7))).resolves.toEqual(
      Uint8Array.of(3, 7, 2, 2, 1, 7),
    );
  });

  it("splits runs at the one-byte count limit", async () => {
    const compressed = await rleCodec.compress(new Uint8Array(256).fill(19));
    expect(compressed).toEqual(Uint8Array.of(255, 19, 1, 19));
    await expect(rleCodec.decompress(compressed, 256)).resolves.toEqual(
      new Uint8Array(256).fill(19),
    );
  });

  it("allocates the exact output while staying within the published bound", async () => {
    const input = Uint8Array.of(0, 1, 2, 3, 4, 4, 4, 5);
    const compressed = await rleCodec.compress(input);
    const bound = rleCompressionMemoryBound(input.byteLength);

    expect(compressed).toHaveLength(12);
    expect(compressed.buffer.byteLength).toBe(compressed.byteLength);
    expect(compressed.byteLength).toBeLessThanOrEqual(bound.maximumOutputBytes);
    expect(bound).toEqual({ maximumOutputBytes: 16, scratchBytes: 0 });
    await expect(rleCodec.decompress(compressed, input.byteLength)).resolves.toEqual(input);
  });

  it("validates lengths used for memory accounting", () => {
    expect(rleCompressionMemoryBound(0)).toEqual({ maximumOutputBytes: 0, scratchBytes: 0 });
    expect(maximumRleCompressedLength(12)).toBe(24);
    expect(() => rleCompressionMemoryBound(-1)).toThrow("Invalid RLE input length");
    expect(() => rleCompressionMemoryBound(1.5)).toThrow("Invalid RLE input length");
    expect(() => rleCompressionMemoryBound(Math.ceil(Number.MAX_SAFE_INTEGER / 2))).toThrow(
      "Invalid RLE input length",
    );
  });

  it("bounds caller-owned buffers for every codec without overflowing", () => {
    expect(getCompressionMemoryBound("raw", 100)).toEqual({
      maximumOutputBytes: 100,
      scratchBytes: 0,
    });
    expect(getCompressionMemoryBound("rle", 100)).toEqual({
      maximumOutputBytes: 200,
      scratchBytes: 0,
    });
    expect(getCompressionMemoryBound("gzip", 100)).toEqual({
      maximumOutputBytes: 264,
      scratchBytes: 364,
    });
    expect(() =>
      getCompressionMemoryBound("gzip", Math.floor(Number.MAX_SAFE_INTEGER / 2)),
    ).toThrow("Invalid gzip input length");
  });

  it("keeps gzip output within its advertised owned-buffer bound", async () => {
    const input = Uint8Array.from({ length: 4_096 }, (_, index) => (index * 131) & 0xff);
    const bound = getCompressionMemoryBound("gzip", input.byteLength);
    const compressed = await gzipCodec.compress(input);

    expect(compressed.byteLength).toBeLessThanOrEqual(bound.maximumOutputBytes);
    await expect(gzipCodec.decompress(compressed, input.byteLength)).resolves.toEqual(input);
  });

  it("preserves malformed-payload errors", () => {
    expect(() => rleCodec.decompress(Uint8Array.of(1), 1)).toThrow("Invalid RLE payload");
    expect(() => rleCodec.decompress(Uint8Array.of(0, 9), 0)).toThrow("Invalid RLE run");
    expect(() => rleCodec.decompress(Uint8Array.of(2, 9), 1)).toThrow("Invalid RLE run");
    expect(() => rleCodec.decompress(Uint8Array.of(1, 9), 2)).toThrow(
      "RLE payload length mismatch",
    );
  });
});
