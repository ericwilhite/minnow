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
import { frozenPackedVector, frozenVectors, fromHex } from "./frozen-vectors.js";
import {
  encodePackedBlock,
  packedMemberBytes,
  packedMemberTag,
  verifyPackedBlock,
  verifyStoredPayload,
} from "./packed.js";

for (const vector of frozenVectors) {
  it(`keeps the frozen v2 raw ${vector.input.type} vector byte-for-byte`, async () => {
    const frozen = fromHex(vector.hex);
    expect(await encodeBlock(vector.input, "raw")).toEqual(frozen);
    expect(verifyStoredBlock(frozen)).toEqual(inspectBlock(frozen));
    expect((await decodeBlock(frozen)).column).toEqual(vector.input);
  });
}

it("keeps the frozen packed-block v1 vector byte-for-byte", async () => {
  const frozen = fromHex(frozenPackedVector.hex);
  const encoded = encodePackedBlock(frozenPackedVector.members);
  expect(encoded?.bytes).toEqual(frozen);
  const directory = verifyPackedBlock(frozen);
  expect(() => {
    verifyStoredPayload(frozen);
  }).not.toThrow();
  for (const [index, member] of frozenPackedVector.members.entries()) {
    const bytes = packedMemberBytes(
      frozen,
      directory,
      packedMemberTag(member.memberId, member.columnId),
      member.memberId,
    );
    // Every member is exactly the frozen v2 block the column encodes to on its own.
    expect(bytes).toEqual(await encodeBlock(member.column, "raw"));
    expect(inspectBlock(bytes)).toEqual(encoded?.members[index]?.description);
    expect((await decodeBlock(bytes)).column).toEqual(member.column);
  }
});
