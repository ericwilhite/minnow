/**
 * xxHash32 (Yann Collet's XXH32, spec: https://github.com/Cyan4973/xxHash/blob/dev/doc/xxhash_spec.md).
 *
 * The one checksum of the new core: log frames, header slots, root slots, page references and
 * spill pages all use it. It is not a cryptographic hash and is never used as one.
 *
 * Contract: `xxHash32(bytes, seed, start, end)` equals the reference XXH32 of
 * `bytes.subarray(start, end)` with `seed`, as an unsigned 32-bit integer. It allocates nothing
 * and reads input little-endian byte by byte, so any alignment and any byteOffset work.
 */

const PRIME1 = 0x9e3779b1;
const PRIME2 = 0x85ebca77;
const PRIME3 = 0xc2b2ae3d;
const PRIME4 = 0x27d4eb2f;
const PRIME5 = 0x165667b1;

function read32(bytes: Uint8Array, index: number): number {
  return (
    (bytes[index] ?? 0) |
    ((bytes[index + 1] ?? 0) << 8) |
    ((bytes[index + 2] ?? 0) << 16) |
    ((bytes[index + 3] ?? 0) << 24)
  );
}

function round(accumulator: number, lane: number): number {
  const sum = (accumulator + Math.imul(lane, PRIME2)) | 0;
  return Math.imul((sum << 13) | (sum >>> 19), PRIME1);
}

export function xxHash32(bytes: Uint8Array, seed = 0, start = 0, end = bytes.length): number {
  if (start < 0 || end > bytes.length || start > end)
    throw new RangeError("xxHash32 range is outside the input");
  const length = end - start;
  let index = start;
  let hash: number;
  if (length >= 16) {
    let v1 = (seed + PRIME1 + PRIME2) | 0;
    let v2 = (seed + PRIME2) | 0;
    let v3 = seed | 0;
    let v4 = (seed - PRIME1) | 0;
    const limit = end - 16;
    while (index <= limit) {
      v1 = round(v1, read32(bytes, index));
      v2 = round(v2, read32(bytes, index + 4));
      v3 = round(v3, read32(bytes, index + 8));
      v4 = round(v4, read32(bytes, index + 12));
      index += 16;
    }
    hash =
      (((v1 << 1) | (v1 >>> 31)) +
        ((v2 << 7) | (v2 >>> 25)) +
        ((v3 << 12) | (v3 >>> 20)) +
        ((v4 << 18) | (v4 >>> 14))) |
      0;
  } else {
    hash = (seed + PRIME5) | 0;
  }
  // XXH32 adds the total length modulo 2^32.
  hash = (hash + length) | 0;
  while (index + 4 <= end) {
    hash = (hash + Math.imul(read32(bytes, index), PRIME3)) | 0;
    hash = Math.imul((hash << 17) | (hash >>> 15), PRIME4);
    index += 4;
  }
  while (index < end) {
    hash = (hash + Math.imul(bytes[index] ?? 0, PRIME5)) | 0;
    hash = Math.imul((hash << 11) | (hash >>> 21), PRIME1);
    index += 1;
  }
  hash ^= hash >>> 15;
  hash = Math.imul(hash, PRIME2);
  hash ^= hash >>> 13;
  hash = Math.imul(hash, PRIME3);
  hash ^= hash >>> 16;
  return hash >>> 0;
}
