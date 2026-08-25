import { describe, expect, it } from "vitest";
import { crc32, crc32Continue } from "./checksum.js";

describe("CRC-32", () => {
  it("matches the IEEE check value", () => {
    expect(crc32(new TextEncoder().encode("123456789"))).toBe(0xcbf43926);
  });

  it("matches a byte-at-a-time oracle across every slicing boundary", () => {
    for (let length = 0; length <= 257; length += 1) {
      const bytes = Uint8Array.from({ length }, (_, index) => (index * 131 + length * 17) & 0xff);
      expect(crc32(bytes), `length ${String(length)}`).toBe(referenceCrc32(bytes));
    }
  });

  it("continues without concatenating at every split point", () => {
    const bytes = Uint8Array.from({ length: 257 }, (_, index) => (index * 73 + 19) & 0xff);
    const expected = crc32(bytes);
    for (let split = 0; split <= bytes.length; split += 1) {
      expect(
        crc32Continue(crc32(bytes.subarray(0, split)), bytes.subarray(split)),
        `split ${String(split)}`,
      ).toBe(expected);
    }
  });
});

function referenceCrc32(bytes: Uint8Array): number {
  let checksum = 0xffffffff;
  for (const byte of bytes) {
    checksum ^= byte;
    for (let bit = 0; bit < 8; bit += 1) {
      checksum = (checksum & 1) === 1 ? 0xedb88320 ^ (checksum >>> 1) : checksum >>> 1;
    }
  }
  return (checksum ^ 0xffffffff) >>> 0;
}
