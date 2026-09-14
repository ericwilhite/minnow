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
import { frozenVectors, fromHex } from "./frozen-vectors.js";

for (const vector of frozenVectors) {
  it(`keeps the frozen v2 raw ${vector.input.type} vector byte-for-byte`, async () => {
    const frozen = fromHex(vector.hex);
    expect(await encodeBlock(vector.input, "raw")).toEqual(frozen);
    expect(verifyStoredBlock(frozen)).toEqual(inspectBlock(frozen));
    expect((await decodeBlock(frozen)).column).toEqual(vector.input);
  });
}
