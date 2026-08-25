import { describe, expect, it } from "vitest";
import { CompressionOutputLimitError, gzipCodec, getCompressionMemoryBound } from "./codecs.js";

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

  it("stops at a caller ceiling before joining an oversized compressed output", async () => {
    const input = Uint8Array.from({ length: 4_096 }, (_, index) => (index * 131) & 0xff);
    const maximumOutputLength = 8;
    const NativeUint8Array = Uint8Array;
    const guarded = new Proxy(NativeUint8Array, {
      construct(target, argumentsList) {
        const requested: unknown = argumentsList[0];
        if (typeof requested === "number" && requested > maximumOutputLength) {
          throw new Error("Oversized joined output allocation");
        }
        const constructed: unknown = Reflect.construct(target, argumentsList, target);
        return constructed as Uint8Array;
      },
    });
    try {
      Object.defineProperty(globalThis, "Uint8Array", {
        configurable: true,
        writable: true,
        value: guarded,
      });
      await expect(gzipCodec.compress(input, maximumOutputLength)).rejects.toMatchObject({
        name: "CompressionOutputLimitError",
        maximumOutputLength,
      });
    } finally {
      Object.defineProperty(globalThis, "Uint8Array", {
        configurable: true,
        writable: true,
        value: NativeUint8Array,
      });
    }
  });

  it("validates caller output ceilings for every codec", async () => {
    await expect(gzipCodec.compress(Uint8Array.of(1), -1)).rejects.toThrow(
      "Invalid compression output ceiling",
    );
    await expect(gzipCodec.compress(Uint8Array.of(1), 0)).rejects.toBeInstanceOf(
      CompressionOutputLimitError,
    );
  });
});
