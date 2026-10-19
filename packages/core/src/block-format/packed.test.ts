import { describe, expect, it } from "vitest";
import { crc32 } from "./checksum.js";
import { encodeBlock } from "./block.js";
import { frozenPackedVector, fromHex } from "./frozen-vectors.js";
import {
  PACKED_BLOCK_ENTRY_LENGTH,
  PACKED_BLOCK_HEADER_LENGTH,
  encodePackedBlock,
  findPackedMember,
  inspectPackedBlock,
  isPackedBlock,
  missingPackedMember,
  packedMemberBytes,
  packedMemberTag,
  verifyPackedBlock,
  verifyStoredPayload,
} from "./packed.js";

const frozen = (): Uint8Array => fromHex(frozenPackedVector.hex);
const directoryEnd = (count: number): number =>
  PACKED_BLOCK_HEADER_LENGTH + PACKED_BLOCK_ENTRY_LENGTH * count;

/** Rewrites both checksums after a mutation, so only the mutated field can be refused. */
function resealed(bytes: Uint8Array): Uint8Array {
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  const count = view.getUint16(12, true);
  const end = Math.min(directoryEnd(count), bytes.byteLength);
  view.setUint32(20, crc32(bytes.subarray(end)), true);
  view.setUint32(4, crc32(bytes.subarray(8, end)), true);
  return bytes;
}

function flipLastByte(bytes: Uint8Array): void {
  bytes.set([(bytes.at(-1) ?? 0) ^ 1], bytes.byteLength - 1);
}

function mutated(at: number, write: (view: DataView) => void, reseal = true): Uint8Array {
  const bytes = frozen();
  write(new DataView(bytes.buffer, at));
  return reseal ? resealed(bytes) : bytes;
}

describe("packed blocks", () => {
  it("recognizes its magic and nothing else", async () => {
    expect(isPackedBlock(frozen())).toBe(true);
    expect(isPackedBlock(await encodeBlock({ type: "number", values: [1] }, "raw"))).toBe(false);
    expect(isPackedBlock(new Uint8Array(2))).toBe(false);
  });

  it("verifies v2 blocks and packed blocks through one entry point", async () => {
    expect(() => {
      verifyStoredPayload(frozen());
    }).not.toThrow();
    const v2 = await encodeBlock({ type: "string", values: ["a"] }, "raw");
    expect(() => {
      verifyStoredPayload(v2);
    }).not.toThrow();
    flipLastByte(v2);
    expect(() => {
      verifyStoredPayload(v2);
    }).toThrow(/checksum/);
  });

  it("refuses every damaged envelope field", () => {
    const cases: Array<[string, Uint8Array, RegExp]> = [
      ["magic", mutated(0, (view) => view.setUint8(3, 0x41)), /magic/],
      ["version", mutated(0, (view) => view.setUint16(8, 2, true)), /version 2/],
      ["header length", mutated(0, (view) => view.setUint16(10, 28, true)), /header length/],
      ["member count zero", mutated(0, (view) => view.setUint16(12, 0, true)), /member count/],
      ["member count over limit", mutated(0, (view) => view.setUint16(12, 1025, true)), /count/],
      ["flags", mutated(0, (view) => view.setUint16(14, 1, true)), /flags/],
      ["member length total", mutated(0, (view) => view.setUint32(16, 1, true)), /length/],
      [
        "envelope checksum",
        mutated(0, (view) => view.setUint32(4, view.getUint32(4, true) ^ 1, true), false),
        /envelope checksum/,
      ],
      [
        "a directory length",
        mutated(PACKED_BLOCK_HEADER_LENGTH, (view) => view.setUint32(0, 40, true)),
        /member length/,
      ],
      [
        "lengths that leave slack",
        mutated(PACKED_BLOCK_HEADER_LENGTH, (view) => view.setUint32(0, 92, true)),
        /fill/,
      ],
    ];
    for (const [label, bytes, message] of cases) {
      expect(() => verifyPackedBlock(bytes), label).toThrow(message);
    }
  });

  it("refuses a trailing byte, a truncated directory, and damaged members", () => {
    const longer = new Uint8Array(frozen().byteLength + 1);
    longer.set(frozen());
    expect(() => verifyPackedBlock(longer)).toThrow(/length/);
    expect(() => verifyPackedBlock(frozen().subarray(0, 30))).toThrow(/Truncated/);
    // A flipped member byte with resealed container checksums is still refused by the member.
    const member = frozen();
    flipLastByte(member);
    expect(() => verifyPackedBlock(resealed(member))).toThrow(/checksum/);
    // Without resealing, the container's own members checksum refuses it first.
    const unsealed = frozen();
    flipLastByte(unsealed);
    expect(() => verifyPackedBlock(unsealed)).toThrow(/members checksum/);
  });

  it("refuses a repeated member tag", () => {
    const bytes = frozen();
    const view = new DataView(bytes.buffer);
    const first = PACKED_BLOCK_HEADER_LENGTH;
    const second = first + PACKED_BLOCK_ENTRY_LENGTH;
    view.setUint32(second + 4, view.getUint32(first + 4, true), true);
    view.setUint32(second + 8, view.getUint32(first + 8, true), true);
    expect(() => verifyPackedBlock(resealed(bytes))).toThrow(/repeats a member tag/);
  });

  it("tags a member by the UTF-8 of its id, a NUL, and its column, whatever the characters", () => {
    const reference = (memberId: string, columnId: string) => {
      const bytes = new TextEncoder().encode(`${memberId}\u0000${columnId}`);
      let fnv = 0x811c9dc5;
      for (const byte of bytes) fnv = Math.imul(fnv ^ byte, 0x01000193) >>> 0;
      return { crc: crc32(bytes), fnv };
    };
    for (const [memberId, columnId] of [
      ["segment-1\u0000value", "value"],
      ["", ""],
      ["s\u0000\u0000x", "\u007f"],
      ["caf\u00e9", "col"],
      ["seg", "\u{1f41f}"],
      ["\u0800", "\uffff"],
    ] as const) {
      expect(packedMemberTag(memberId, columnId)).toEqual(reference(memberId, columnId));
    }
  });

  it("finds members by their member and column, and refuses the wrong column", () => {
    const bytes = frozen();
    const directory = inspectPackedBlock(bytes);
    const [id, label] = frozenPackedVector.members;
    if (id === undefined || label === undefined) throw new Error("vector changed");
    expect(findPackedMember(directory, packedMemberTag(id.memberId, id.columnId))).toBe(0);
    expect(findPackedMember(directory, packedMemberTag(label.memberId, label.columnId))).toBe(1);
    // The right member under another column's id is not the member the record names.
    expect(findPackedMember(directory, packedMemberTag(id.memberId, label.columnId))).toBe(-1);
    expect(() =>
      packedMemberBytes(bytes, directory, packedMemberTag("absent", id.columnId), "absent"),
    ).toThrow(/no member absent/);
  });

  it("hands out member copies, never views of the container", () => {
    const bytes = frozen();
    const directory = inspectPackedBlock(bytes);
    const [id] = frozenPackedVector.members;
    if (id === undefined) throw new Error("vector changed");
    const member = packedMemberBytes(
      bytes,
      directory,
      packedMemberTag(id.memberId, id.columnId),
      id.memberId,
    );
    expect(member.buffer).not.toBe(bytes.buffer);
    expect(member.byteOffset).toBe(0);
  });

  it("names the first segment member its packed block does not hold", () => {
    const column = { type: "number", values: [1] } as const;
    const encoded = encodePackedBlock([
      { memberId: "seg/a", columnId: "a", column },
      { memberId: "seg/b", columnId: "b", column },
    ]);
    if (encoded === undefined) throw new Error("expected a packed block");
    expect(missingPackedMember(encoded.bytes, { a: ["seg/a"], b: ["seg/b"] })).toBeUndefined();
    // A member under another column, and a member the block never held.
    expect(missingPackedMember(encoded.bytes, { a: ["seg/b"] })).toBe("seg/b");
    expect(missingPackedMember(encoded.bytes, { a: ["seg/a"], c: ["seg/c"] })).toBe("seg/c");
  });

  it("declines to pack two members whose tags collide", () => {
    const column = { type: "number", values: [1] } as const;
    expect(
      encodePackedBlock([
        { memberId: "m", columnId: "c", column },
        { memberId: "m", columnId: "c", column },
      ]),
    ).toBeUndefined();
    expect(() => encodePackedBlock([])).toThrow(RangeError);
  });
});
