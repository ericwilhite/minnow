/** Allocation-only upper bounds for the next hash batch. These do not apply transitions. */
import { bigintBytes } from "../../memory/governor.js";
import type { Vector } from "../../vector/vector.js";

/** Sum of all new buffers in a sequence of geometric resizes is <2x its final buffer.
 * Existing buffers remain in the caller's current allocation. The bound therefore covers
 * every old/new overlap for geometric resizes within a single input batch. ByteArena
 * also clips arbitrary exact jumps to this pair peak: otherwise a large exact resize
 * followed by a tiny append could request 4x the exact size, outside this envelope.
 */
export function arrayGrowth(length: number, needed: number, bytes: number, double = false): number {
  if (needed <= length) return 0;
  let capacity = Math.max(1, length);
  while (capacity < needed) capacity *= !double && capacity >= 1 << 16 ? 4 : 2;
  return 2 * capacity * bytes;
}

/** Logical source payload, including unused dictionary entries. Used as an upper bound,
 * never as an NDV estimate. Constants need only one copied key.
 */
export function payloadBytes(vector: Vector): number {
  const flat =
    vector.form === "dictionary"
      ? vector.dictionary
      : vector.form === "constant"
        ? vector.value
        : vector;
  const data = flat.data;
  switch (data.layout) {
    case "varlen":
      return (data.offsets[flat.length] ?? 0) - (data.offsets[0] ?? 0);
    case "fixed16":
      return 16 * flat.length;
    case "decimal": {
      const c = data.coefficients;
      if (c.repr !== "wide") return 16 * flat.length;
      let bytes = 0;
      for (let i = 0; i < flat.length; i++) bytes += bigintBytes(c.values[i] ?? 0n);
      return bytes;
    }
    case "interval":
      return 16 * flat.length;
    default:
      return 8 * flat.length;
  }
}
