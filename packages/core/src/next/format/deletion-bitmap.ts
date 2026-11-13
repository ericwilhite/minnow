/**
 * GroupRef.deletions: raw little-endian u32 words, low bit first, one bit per stored row.
 * Version inherited from the enclosing groupPage; exactly ceil(rowCount/32)*4 bytes, with
 * unused high bits zero. The extent space verifies the blob checksum before this decoder.
 */
import { requireBits, RESERVED_MASKS } from "../durable-format.js";
import { StoreCorruptionError } from "../storage/contract.js";

/** Validates before making the same owned word copy the write path historically returned. */
export function decodeDeletionBitmap(bytes: Uint8Array, rowCount: number): Uint32Array {
  if (!Number.isSafeInteger(rowCount) || rowCount < 0)
    throw new StoreCorruptionError("Invalid deletion bitmap row count");
  if (bytes.byteLength !== Math.ceil(rowCount / 32) * 4)
    throw new StoreCorruptionError("Deletion bitmap length differs from its group row count");
  const rest = rowCount & 31;
  if (rest !== 0) {
    const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
    requireBits(
      view.getUint32(bytes.byteLength - 4, true) >>> rest,
      RESERVED_MASKS.deletionBitmapTail,
      "deletion bitmap tail",
      StoreCorruptionError,
    );
  }
  return new Uint32Array(bytes.slice().buffer);
}
