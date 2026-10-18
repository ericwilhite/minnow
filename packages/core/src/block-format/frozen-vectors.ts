import type { ColumnInput } from "./types.js";

export const frozenVectors: Array<{ input: ColumnInput; hex: string }> = [
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

/**
 * A packed block v1 with one member of every physical type. Frozen like the v2 vectors: a writer
 * change must preserve these bytes or introduce a new packed-block version.
 */
export const frozenPackedVector: {
  members: Array<{ memberId: string; columnId: string; column: ColumnInput }>;
  hex: string;
} = {
  members: [
    {
      memberId: "segment/s1/member/id",
      columnId: "column-id",
      column: { type: "number", values: [7, -1.5] },
    },
    {
      memberId: "segment/s1/member/label",
      columnId: "column-label",
      column: { type: "string", values: ["é", null] },
    },
    {
      memberId: "segment/s1/member/flag",
      columnId: "column-flag",
      column: { type: "boolean", values: [true, null] },
    },
    {
      memberId: "segment/s1/member/at",
      columnId: "column-at",
      column: { type: "datetime", values: [new Date(1), null] },
    },
  ],
  hex:
    "4252504b5842bf28010018000400000024010000d51f30935d000000b4392666525891fc3d000000" +
    "63f877148851835c300000002c54ac5c2a6b3a6b5a000000ed11b21be23f0cef425244421134020802" +
    "002c00020200000200000000000000200000001100000011000000f00dcefef00dcefe7b227a6f6e65" +
    "4d6170223a7b226d696e223a2d312e352c226d6178223a377d7d030000000000001c40000000000000" +
    "f8bf425244420aba24a102002c00030300000200000001000000020000000f0000000f000000f836c8" +
    "68f836c8687b7d01000000000200000002000000c3a942524442706fead002002c0001010000020000" +
    "00010000000200000002000000020000002813c52f2813c52f7b7d0101425244420c67768f02002c00" +
    "0404000002000000010000001d000000110000001100000064b98daf64b98daf7b227a6f6e654d6170" +
    "223a7b226d696e223a312c226d6178223a317d7d01000000000000f03f0000000000000000",
};

export function fromHex(hex: string): Uint8Array {
  if (hex.length % 2 !== 0) throw new Error("Invalid frozen block vector");
  const bytes = new Uint8Array(hex.length / 2);
  for (let index = 0; index < bytes.length; index += 1) {
    bytes[index] = Number.parseInt(hex.slice(index * 2, index * 2 + 2), 16);
  }
  return bytes;
}
