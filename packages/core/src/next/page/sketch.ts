/**
 * The distinct-count sketch of a column chunk: HyperLogLog with 2^10 one-byte registers
 * (standard error ≈ 3.3 %), mergeable by register maximum. Available for approximate planning and
 * cost estimates, never pruning or correctness. Directory reads validate the body without
 * materializing registers; planning does not currently use its estimate.
 *
 * Values are hashed by SQL equality within one column: −0 = 0 and every NaN is one value for
 * floats, `1 mon` = `30 days` for intervals, and a decimal coefficient hashes the same whatever
 * tier stores it. (Two chunks of an unconstrained NUMERIC column at different scales hash equal
 * values differently; the estimate is then high, never wrong in kind.)
 *
 * Serialized form: u8 precision (10), u8 form (0 dense, 1 sparse), u16 entry count; dense is the
 * 1,024 registers, sparse is `count` × (u16 register, u8 value) for the non-zero registers.
 */
import { requireTag, DURABLE_CODECS, CONDITIONAL_FORMAT } from "../durable-format.js";
import { xxHash32 } from "../hash/xxhash32.js";
import { check, PageCorruptionError } from "./format.js";

export const SKETCH_PRECISION = DURABLE_CODECS.sketchPrecision;
const REGISTERS = 1 << SKETCH_PRECISION;

/** murmur3's 32-bit finalizer. Hashes are int32 (never boxed): only their bits matter. */
export function mix32(value: number): number {
  let h = value | 0;
  h ^= h >>> 16;
  h = Math.imul(h, 0x85eb_ca6b);
  h ^= h >>> 13;
  h = Math.imul(h, 0xc2b2_ae35);
  h ^= h >>> 16;
  return h;
}

/** Hash of an int64 given as (low, high) 32-bit words. */
export function hashInt64(low: number, high: number): number {
  return mix32(mix32(low) ^ Math.imul(high | 0, 0x9e37_79b1) ^ 0x2545_f491);
}

const floatScratch = new Float64Array(1);
const floatWords = new Uint32Array(floatScratch.buffer);

export function hashFloat(value: number): number {
  if (value === 0) return hashInt64(0, 0x7ff0_0001);
  if (value !== value) return hashInt64(0, 0x7ff8_0000);
  floatScratch[0] = value;
  return hashInt64(floatWords[0] ?? 0, floatWords[1] ?? 0);
}

export function hashBytes(bytes: Uint8Array, start: number, end: number): number {
  return xxHash32(bytes, 0x5eed, start, end) | 0;
}

export class DistinctSketch {
  readonly registers = new Uint8Array(REGISTERS);

  add(hash: number): void {
    const index = hash >>> (32 - SKETCH_PRECISION);
    // Rank of the first set bit among the remaining 22 bits (a sentinel bit bounds it at 23).
    const rank = Math.clz32((hash << SKETCH_PRECISION) | (1 << (SKETCH_PRECISION - 1))) + 1;
    if (rank > (this.registers[index] ?? 0)) this.registers[index] = rank;
  }

  merge(other: DistinctSketch): void {
    for (let index = 0; index < REGISTERS; index++)
      if ((other.registers[index] ?? 0) > (this.registers[index] ?? 0))
        this.registers[index] = other.registers[index] ?? 0;
  }

  /** Estimated distinct values (HyperLogLog with linear counting for small cardinalities). */
  estimate(): number {
    let sum = 0;
    let zeros = 0;
    for (let index = 0; index < REGISTERS; index++) {
      const value = this.registers[index] ?? 0;
      sum += 2 ** -value;
      if (value === 0) zeros += 1;
    }
    const alpha = 0.7213 / (1 + 1.079 / REGISTERS);
    const raw = (alpha * REGISTERS * REGISTERS) / sum;
    if (raw <= 2.5 * REGISTERS && zeros > 0) return REGISTERS * Math.log(REGISTERS / zeros);
    return raw;
  }

  serialize(): Uint8Array {
    let nonZero = 0;
    for (let index = 0; index < REGISTERS; index++)
      if ((this.registers[index] ?? 0) !== 0) nonZero++;
    if (nonZero * 3 < REGISTERS) {
      const out = new Uint8Array(4 + nonZero * 3);
      out[0] = SKETCH_PRECISION;
      out[1] = 1;
      out[2] = nonZero & 0xff;
      out[3] = nonZero >>> 8;
      let at = 4;
      for (let index = 0; index < REGISTERS; index++) {
        const value = this.registers[index] ?? 0;
        if (value === 0) continue;
        out[at] = index & 0xff;
        out[at + 1] = index >>> 8;
        out[at + 2] = value;
        at += 3;
      }
      return out;
    }
    const out = new Uint8Array(4 + REGISTERS);
    out[0] = SKETCH_PRECISION;
    out[2] = REGISTERS & 0xff;
    out[3] = REGISTERS >>> 8;
    out.set(this.registers, 4);
    return out;
  }

  static parse(bytes: Uint8Array): DistinctSketch {
    const form = checkedForm(bytes);
    const sketch = new DistinctSketch();
    readRegisters(bytes, form, sketch.registers);
    return sketch;
  }
}

function checkedForm(bytes: Uint8Array): number {
  check(bytes.length >= 4 && bytes[0] === SKETCH_PRECISION, "Bad distinct sketch header");
  const form = bytes[1] ?? 0;
  requireTag(form, DURABLE_CODECS.sketchForm, "sketch form", PageCorruptionError);
  return form;
}
/** Validate while copying, or validate only. One body walk; null allocates no registers. */
function readRegisters(bytes: Uint8Array, form: number, registers: Uint8Array | null): void {
  const count = (bytes[2] ?? 0) | ((bytes[3] ?? 0) << 8);
  if (form === 0) {
    check(count === REGISTERS && bytes.length === 4 + REGISTERS, "Bad dense sketch");
    // Validate while copying: no second pass or additional buffer.
    for (let index = 0; index < REGISTERS; index++) {
      const value = bytes[4 + index] ?? 0;
      check(value <= CONDITIONAL_FORMAT.sketchMaximumRank, "Bad dense sketch rank");
      if (registers !== null) registers[index] = value;
    }
  } else {
    check(form === 1 && bytes.length === 4 + count * 3, "Bad sparse sketch");
    let previous = -1;
    for (let k = 0; k < count; k++) {
      const at = 4 + k * 3;
      const index = (bytes[at] ?? 0) | ((bytes[at + 1] ?? 0) << 8);
      const value = bytes[at + 2] ?? 0;
      check(
        index < REGISTERS && value > 0 && value <= CONDITIONAL_FORMAT.sketchMaximumRank,
        "Bad sketch entry",
      );
      check(index > previous, "Sparse sketch registers are not strictly increasing");
      previous = index;
      if (registers !== null) registers[index] = value;
    }
  }
}

/** A directory must refuse a malformed hint even when planning does not use its estimate.
 * No destination object/buffer, and the same checks as the materializing decoder. */
export function validateSketch(bytes: Uint8Array): void {
  readRegisters(bytes, checkedForm(bytes), null);
}
