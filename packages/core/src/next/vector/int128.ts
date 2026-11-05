/**
 * The 128-bit decimal coefficient tier (vector.ts `Int128Values`): four little-endian Uint32
 * limbs per slot, two's complement. Exact sums and merges run here without a BigInt per value;
 * a result outside 128 bits widens to `bigint[]` (`addInt128` reports the overflow).
 */
export const INT128_MIN = -(2n ** 127n);
export const INT128_MAX = 2n ** 127n - 1n;

export function int128At(limbs: Uint32Array, index: number): bigint {
  const base = index * 4;
  const unsigned =
    BigInt(limbs[base] ?? 0) |
    (BigInt(limbs[base + 1] ?? 0) << 32n) |
    (BigInt(limbs[base + 2] ?? 0) << 64n) |
    (BigInt(limbs[base + 3] ?? 0) << 96n);
  return BigInt.asIntN(128, unsigned);
}

export function setInt128(limbs: Uint32Array, index: number, value: bigint): void {
  if (value < INT128_MIN || value > INT128_MAX) throw new RangeError("Value is outside 128 bits");
  const unsigned = BigInt.asUintN(128, value);
  const base = index * 4;
  limbs[base] = Number(unsigned & 0xffff_ffffn);
  limbs[base + 1] = Number((unsigned >> 32n) & 0xffff_ffffn);
  limbs[base + 2] = Number((unsigned >> 64n) & 0xffff_ffffn);
  limbs[base + 3] = Number(unsigned >> 96n);
}

/**
 * `target[t] += source[s]` in place, limb by limb with no allocation. Returns false (leaving
 * `target[t]` wrapped) when the exact sum leaves 128 bits: the caller widens and redoes it.
 */
export function addInt128(target: Uint32Array, t: number, source: Uint32Array, s: number): boolean {
  const tb = t * 4;
  const sb = s * 4;
  const signA = (target[tb + 3] ?? 0) >>> 31;
  const signB = (source[sb + 3] ?? 0) >>> 31;
  let carry = 0;
  for (let limb = 0; limb < 4; limb++) {
    const sum = (target[tb + limb] ?? 0) + (source[sb + limb] ?? 0) + carry;
    target[tb + limb] = sum >>> 0;
    carry = sum > 0xffff_ffff ? 1 : 0;
  }
  const signR = (target[tb + 3] ?? 0) >>> 31;
  return !(signA === signB && signR !== signA);
}
