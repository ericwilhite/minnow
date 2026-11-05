/**
 * Out-of-line large values (ruling R15.4): a text, bytes, json or array value longer than
 * `LARGE_VALUE_BYTES` is not stored in its page. Its bytes go into the chunk's own extent as
 * consecutive parts of at most `VALUE_PART_BYTES` (1 MiB, the blob cap), each with its own
 * xxHash32, and the page's slot for the row holds a **pointer** to them (the row is marked in the
 * page's `large` bitmap). Values are at most `MAX_VALUE_BYTES` (64 MiB).
 *
 * Because the parts are inside the chunk's data range, they need nothing of their own from the
 * manifest: the reachability walk reaches them through the chunk, and they are reclaimed with
 * it. A page that holds pointers is plain (never dictionary or constant), and its statistics and
 * sketch use the values themselves, not the pointers.
 *
 * Pointer bytes (little-endian): u8 version (1) | u8 0 | u16 part count | u32 value length |
 * u32 offset low | u32 offset high | u32 checksum per part. `offset` is relative to the chunk's
 * data range (like `PageEntry.offset`); part i is `[offset + i × VALUE_PART_BYTES, …)`, the last
 * one shorter.
 */
import { check } from "./format.js";

export const LARGE_VALUE_BYTES = 64 * 1024;
export const MAX_VALUE_BYTES = 64 * 1024 * 1024;
export const VALUE_PART_BYTES = 1024 * 1024;
const HEADER = 16;
const TWO32 = 4_294_967_296;

export interface LargePointer {
  /** Relative to the chunk's data range. */
  readonly offset: number;
  readonly length: number;
  readonly checksums: readonly number[];
}

export const partCount = (length: number): number => Math.ceil(length / VALUE_PART_BYTES);

export function pointerBytes(parts: number): number {
  return HEADER + parts * 4;
}

export function encodePointer(pointer: LargePointer): Uint8Array {
  const parts = pointer.checksums.length;
  if (parts !== partCount(pointer.length))
    throw new RangeError("Pointer part count disagrees with its length");
  const out = new Uint8Array(pointerBytes(parts));
  const view = new DataView(out.buffer);
  view.setUint8(0, 1);
  view.setUint16(2, parts, true);
  view.setUint32(4, pointer.length, true);
  const high = Math.floor(pointer.offset / TWO32);
  view.setUint32(8, pointer.offset - high * TWO32, true);
  view.setUint32(12, high, true);
  pointer.checksums.forEach((checksum, index) =>
    view.setUint32(HEADER + index * 4, checksum, true),
  );
  return out;
}

/** Decodes and checks a pointer stored in `bytes[start … end)` (`PageCorruptionError`). */
export function decodePointer(bytes: Uint8Array, start: number, end: number): LargePointer {
  check(end - start >= HEADER, "Large-value pointer is truncated");
  const view = new DataView(bytes.buffer, bytes.byteOffset + start, end - start);
  check(view.getUint8(0) === 1 && view.getUint8(1) === 0, "Unknown large-value pointer");
  const parts = view.getUint16(2, true);
  const length = view.getUint32(4, true);
  const offset = view.getUint32(8, true) + view.getUint32(12, true) * TWO32;
  check(
    length > LARGE_VALUE_BYTES && length <= MAX_VALUE_BYTES && parts === partCount(length),
    "Bad large-value length",
  );
  check(
    end - start === pointerBytes(parts) && Number.isSafeInteger(offset),
    "Bad large-value pointer",
  );
  const checksums: number[] = [];
  for (let k = 0; k < parts; k++) checksums.push(view.getUint32(HEADER + k * 4, true));
  return { offset, length, checksums };
}

/** The `[offset, length]` of each part of `pointer`, relative to the chunk's data range. */
export function partsOf(
  pointer: LargePointer,
): Array<{ offset: number; length: number; checksum: number }> {
  return pointer.checksums.map((checksum, index) => ({
    offset: pointer.offset + index * VALUE_PART_BYTES,
    length: Math.min(VALUE_PART_BYTES, pointer.length - index * VALUE_PART_BYTES),
    checksum,
  }));
}
