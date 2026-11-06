/**
 * xxHash32 (Yann Collet's XXH32, spec: https://github.com/Cyan4973/xxHash/blob/dev/doc/xxhash_spec.md).
 *
 * The one checksum of the new core: log frames, header slots, root slots, page references and
 * spill pages all use it. It is not a cryptographic hash and is never used as one.
 *
 * Contract: `xxHash32(bytes, seed, start, end)` equals the reference XXH32 of
 * `bytes.subarray(start, end)` with seed `seed >>> 0`, as an unsigned 32-bit integer, at any
 * alignment and any byteOffset, on any platform byte order.
 *
 * Reading (ruling R15.8): inputs of at least `WORD_READ_BYTES` read their 16-byte stripes a
 * 32-bit word at a time through one `DataView` (explicitly little-endian, unaligned reads are
 * fine); shorter ones read bytes, because creating the view costs more than it saves there
 * (measured: the two meet near 256 bytes; words are ~2.5× faster from 4 KiB). The tail (< 16
 * bytes) always reads bytes. No state is kept between calls, so no buffer outlives its holder.
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

/** Inputs this long read words (scripts/xxhash-bench.mts picks it; see the contract above). */
export const WORD_READ_BYTES = 256;

function round(accumulator: number, lane: number): number {
  const sum = (accumulator + Math.imul(lane, PRIME2)) | 0;
  return Math.imul((sum << 13) | (sum >>> 19), PRIME1);
}

export function xxHash32(bytes: Uint8Array, seed = 0, start = 0, end = bytes.length): number {
  if (start < 0 || end > bytes.length || start > end)
    throw new RangeError("xxHash32 range is outside the input");
  // The seed is a uint32: any number is taken as `seed >>> 0` (ToUint32) before arithmetic, so
  // a fractional, non-finite or large seed means exactly what `>>> 0` makes of it.
  seed >>>= 0;
  const length = end - start;
  let index = start;
  let hash: number;
  if (length >= 16) {
    let v1 = (seed + PRIME1 + PRIME2) | 0;
    let v2 = (seed + PRIME2) | 0;
    let v3 = seed | 0;
    let v4 = (seed - PRIME1) | 0;
    const limit = end - 16;
    if (length >= WORD_READ_BYTES) {
      const view = new DataView(bytes.buffer, bytes.byteOffset + start, length);
      const stripes = length - 16;
      let at = 0;
      for (; at <= stripes; at += 16) {
        v1 = round(v1, view.getInt32(at, true));
        v2 = round(v2, view.getInt32(at + 4, true));
        v3 = round(v3, view.getInt32(at + 8, true));
        v4 = round(v4, view.getInt32(at + 12, true));
      }
      index = start + at;
    } else
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
