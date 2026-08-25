/**
 * Frozen writer vectors for the final v2 envelope and every physical type id.
 *
 * These bytes are deliberately hand-carried in the test rather than regenerated. A writer
 * change must either preserve them or make an explicit format-version decision. Gzip bytes are
 * not frozen because different standards-compliant native compressors may choose different
 * deflate streams; raw vectors freeze every Minnow-owned byte in the envelope and columns.
 */
import { expect, it } from "vitest";
import { decodeBlock, encodeBlock, inspectBlock, verifyStoredBlock } from "./block.js";
import type { ColumnInput } from "./types.js";

const vectors: Array<{ input: ColumnInput; hex: string }> = [
  {
    input: { type: "boolean", values: [true, false, null] },
    hex:
      "425244429a15944202002c00010100000300000001000000020000000200000002000000" +
      "aa71f31daa71f31d7b7d0301",
  },
  {
    input: { type: "number", values: [1.5, null, -2] },
    hex:
      "425244426c918f5402002c00020200000300000001000000200000001900000019000000" +
      "5d52bfa35d52bfa37b227a6f6e654d6170223a7b226d696e223a2d322c226d6178223a" +
      "312e357d7d05000000000000f83f000000000000000000000000000000c0",
  },
  {
    input: { type: "string", values: ["A", "é", null] },
    hex:
      "42524442c2abbc9f02002c00030300000300000001000000020000001400000014000000" +
      "0509088f0509088f7b7d030000000001000000030000000300000041c3a9",
  },
  {
    input: { type: "datetime", values: [new Date(0), null, new Date(1)] },
    hex:
      "42524442120f1f7602002c000404000003000000010000001d0000001900000019000000" +
      "fa182315fa1823157b227a6f6e654d6170223a7b226d696e223a302c226d6178223a317d" +
      "7d0500000000000000000000000000000000000000000000f03f",
  },
];

for (const vector of vectors) {
  it(`keeps the frozen v2 raw ${vector.input.type} vector byte-for-byte`, async () => {
    const frozen = fromHex(vector.hex);
    expect(await encodeBlock(vector.input, "raw")).toEqual(frozen);
    expect(verifyStoredBlock(frozen)).toEqual(inspectBlock(frozen));
    expect((await decodeBlock(frozen)).column).toEqual(vector.input);
  });
}

function fromHex(hex: string): Uint8Array {
  if (hex.length % 2 !== 0) throw new Error("Invalid frozen block vector");
  const bytes = new Uint8Array(hex.length / 2);
  for (let index = 0; index < bytes.length; index += 1) {
    bytes[index] = Number.parseInt(hex.slice(index * 2, index * 2 + 2), 16);
  }
  return bytes;
}
