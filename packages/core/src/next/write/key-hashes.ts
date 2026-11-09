/** Canonical run Bloom seeds. These values are part of the stored-byte contract. */
export const KEY_BLOOM_SEED = 0x9747b28c;

/** Reusable scalar scratch, never a per-key allocation. `second` is raw, not forced odd. */
export interface KeyHashes {
  first: number;
  second: number;
}

const P1 = 0x9e3779b1;
const P2 = 0x85ebca77;
const P3 = 0xc2b2ae3d;
const P4 = 0x27d4eb2f;
const P5 = 0x165667b1;
const word = (b: Uint8Array, at: number): number =>
  (b[at] ?? 0) | ((b[at + 1] ?? 0) << 8) | ((b[at + 2] ?? 0) << 16) | ((b[at + 3] ?? 0) << 24);
function round(acc: number, lane: number): number {
  const h = (acc + Math.imul(lane, P2)) | 0;
  return Math.imul((h << 13) | (h >>> 19), P1);
}
function finish(h: number): number {
  h ^= h >>> 15;
  h = Math.imul(h, P2);
  h ^= h >>> 13;
  h = Math.imul(h, P3);
  return (h ^ (h >>> 16)) >>> 0;
}

/**
 * One byte traversal, two independent XXH32 accumulators: exactly xxHash32(range, 0)
 * and xxHash32(range, KEY_BLOOM_SEED). No changed seed, combined-hash approximation,
 * host-endian word load, retained buffer or allocation. Callers retain both results with
 * their key so table probes, flushes and later lookups do not traverse its bytes again.
 */
export function hashKey(bytes: Uint8Array, start: number, end: number, out: KeyHashes): void {
  if (start < 0 || end > bytes.length || start > end)
    throw new RangeError("Key hash range is outside the input");
  const length = end - start;
  let at = start;
  let a = P5 | 0;
  let b = (KEY_BLOOM_SEED + P5) | 0;
  if (length >= 16) {
    let a1 = (P1 + P2) | 0,
      a2 = P2 | 0,
      a3 = 0,
      a4 = -P1 | 0;
    let b1 = (KEY_BLOOM_SEED + P1 + P2) | 0,
      b2 = (KEY_BLOOM_SEED + P2) | 0,
      b3 = KEY_BLOOM_SEED | 0,
      b4 = (KEY_BLOOM_SEED - P1) | 0;
    while (at + 16 <= end) {
      const w0 = word(bytes, at),
        w1 = word(bytes, at + 4),
        w2 = word(bytes, at + 8),
        w3 = word(bytes, at + 12);
      a1 = round(a1, w0);
      b1 = round(b1, w0);
      a2 = round(a2, w1);
      b2 = round(b2, w1);
      a3 = round(a3, w2);
      b3 = round(b3, w2);
      a4 = round(a4, w3);
      b4 = round(b4, w3);
      at += 16;
    }
    a =
      (((a1 << 1) | (a1 >>> 31)) +
        ((a2 << 7) | (a2 >>> 25)) +
        ((a3 << 12) | (a3 >>> 20)) +
        ((a4 << 18) | (a4 >>> 14))) |
      0;
    b =
      (((b1 << 1) | (b1 >>> 31)) +
        ((b2 << 7) | (b2 >>> 25)) +
        ((b3 << 12) | (b3 >>> 20)) +
        ((b4 << 18) | (b4 >>> 14))) |
      0;
  }
  a = (a + length) | 0;
  b = (b + length) | 0;
  while (at + 4 <= end) {
    const lane = Math.imul(word(bytes, at), P3);
    a = (a + lane) | 0;
    b = (b + lane) | 0;
    a = Math.imul((a << 17) | (a >>> 15), P4);
    b = Math.imul((b << 17) | (b >>> 15), P4);
    at += 4;
  }
  while (at < end) {
    const lane = Math.imul(bytes[at++] ?? 0, P5);
    a = (a + lane) | 0;
    b = (b + lane) | 0;
    a = Math.imul((a << 11) | (a >>> 21), P1);
    b = Math.imul((b << 11) | (b >>> 21), P1);
  }
  out.first = finish(a);
  out.second = finish(b);
}
