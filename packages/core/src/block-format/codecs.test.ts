import { describe, expect, it } from "vitest";
import { gzipCodec, getCompressionMemoryBound } from "./codecs.js";

describe("block compression", () => {
  it("bounds caller-owned buffers for every codec without overflowing", () => {
    expect(getCompressionMemoryBound("raw", 100)).toEqual({
      maximumOutputBytes: 100,
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

  it("rejects lengths that cannot describe a real payload", () => {
    expect(() => getCompressionMemoryBound("raw", -1)).toThrow("Invalid raw input length");
    expect(() => getCompressionMemoryBound("raw", 1.5)).toThrow("Invalid raw input length");
  });

  it("keeps gzip output within its advertised owned-buffer bound", async () => {
    const input = Uint8Array.from({ length: 4_096 }, (_, index) => (index * 131) & 0xff);
    const bound = getCompressionMemoryBound("gzip", input.byteLength);
    const compressed = await gzipCodec.compress(input);

    expect(compressed.byteLength).toBeLessThanOrEqual(bound.maximumOutputBytes);
    await expect(gzipCodec.decompress(compressed, input.byteLength)).resolves.toEqual(input);
  });
});
