/**
 * Per-group values: a growable typed column indexed by group id, appended slot by slot from
 * vectors of any form, overwritten in place, and emitted as flat vectors. It holds group key
 * values (EncodedKeys), MIN/MAX states and single-value results. (B7's exec/column-store.ts is
 * the shared append-only row store for spill and join builds; this one is indexed and updatable.)
 *
 * ## Storage (struct-of-arrays, no object or Map entry per value)
 * - bool, int32, float: one typed array.
 * - int64 and interval microseconds: two 32-bit words per entry (low, high), whatever the input
 *   representation, so wide values cost no BigInt and compare without one.
 * - NUMERIC coefficients: f64 while within ±(2^53 − 1), else four 128-bit limbs per entry (the
 *   limb array is created at the first such value). Only coefficients wider than 128 bits — inputs
 *   already in the `wide` representation — are kept as BigInts, in a dense array created at the
 *   first one and charged at `HUGE_BYTES` per stored value (an estimate of a BigInt's heap size).
 * - varlen / fixed16: bytes in one arena with per-entry start and end; an overwrite that does not
 *   fit in place appends and repoints the entry, and the arena is compacted when more than half
 *   of it is dead.
 *
 * ## Memory
 * Every buffer is charged to the `ByteCharge` (a grant) before it is constructed, and the charge is
 * returned if construction throws. Growth doubles; the old buffer is freed after the copy. `close`
 * frees everything.
 */
import type { PhysicalType } from "../../types.js";
import { layoutOf } from "../../types.js";
import { MAX_SAFE } from "../../vector/int64.js";
import type { ByteCharge } from "../../vector/varlen.js";
import type { FlatVector, Validity, Vector, VectorData } from "../../vector/vector.js";
import { copy, fitsInt64, int64Words, setBigInt, setNumber, toBigInt } from "../kernels/limbs.js";
import { scaleAt, specialAt } from "../kernels/numeric.js";
import { slotOf } from "../kernels/vectors.js";

const TWO32 = 4294967296;
/** Charged per BigInt kept for a coefficient past 128 bits (an estimate of its heap size). */
export const HUGE_BYTES = 64;

/** Decimal coefficient tiers per entry. */
const SAFE = 0;
const LIMBS = 1;
const HUGE = 2;

type Typed = Uint8Array | Uint16Array | Uint32Array | Int32Array | Float32Array | Float64Array;
interface TypedConstructor<T extends Typed> {
  new (length: number): T;
  readonly BYTES_PER_ELEMENT: number;
}

/** Scratch for one int64 value's words (input side of a write). */
const WORDS = new Uint32Array(2);

/** Writes a safe integer's two's-complement words into `words` at `index`. */
function wordsOfNumber(v: number, words: Uint32Array, index: number): void {
  const hi = Math.floor(v / TWO32);
  words[index * 2] = (v - hi * TWO32) >>> 0;
  words[index * 2 + 1] = hi >>> 0;
}

export class GroupValues {
  readonly layout: ReturnType<typeof layoutOf>;
  #length = 0;
  #capacity = 0;
  #charged = 0;
  /** 1 where the entry is SQL null. */
  #nulls = new Uint8Array(0);
  /** bool / int32 / float values; decimal coefficients in the safe tier. */
  #values: Typed = new Float64Array(0);
  /** int64 values and interval microseconds: (low, high) words. */
  #words = new Uint32Array(0);
  /** Decimal: SAFE, LIMBS or HUGE per entry; the limbs (4 per entry) and the huge BigInts. */
  #tiers = new Uint8Array(0);
  #limbs: Uint32Array | null = null;
  #huge: bigint[] | null = null;
  #scales = new Uint16Array(0);
  #specials = new Uint8Array(0);
  /** Interval months and days. */
  #months = new Int32Array(0);
  #days = new Int32Array(0);
  #bytes = new Uint8Array(0);
  /** varlen: start and end of each entry in `#bytes`. */
  #starts = new Int32Array(0);
  #ends = new Int32Array(0);
  #used = 0;
  #dead = 0;

  constructor(
    readonly type: PhysicalType,
    readonly charge: ByteCharge,
  ) {
    this.layout = layoutOf(type);
  }

  get length(): number {
    return this.#length;
  }

  #take(bytes: number): void {
    if (!this.charge.allocate(bytes))
      throw new RangeError(
        `A ${String(bytes)}-byte group value store growth was refused by its grant`,
      );
    this.#charged += bytes;
  }

  #give(bytes: number): void {
    this.charge.free(bytes);
    this.#charged -= bytes;
  }

  /** A new typed array of `length`, charged first (the charge is returned if it cannot be made). */
  #make<T extends Typed>(Type: TypedConstructor<T>, length: number): T {
    const bytes = length * Type.BYTES_PER_ELEMENT;
    this.#take(bytes);
    try {
      return new Type(length);
    } catch (error) {
      this.#give(bytes);
      throw error;
    }
  }

  #grow<T extends Typed>(old: T, Type: TypedConstructor<T>, length: number): T {
    const next = this.#make(Type, length);
    next.set(old);
    this.#give(old.byteLength);
    return next;
  }

  #ensure(capacity: number): void {
    if (capacity <= this.#capacity) return;
    const next = Math.max(64, capacity, 2 * this.#capacity);
    this.#nulls = this.#grow(this.#nulls, Uint8Array, next);
    switch (this.layout) {
      case "bool":
        this.#values = this.#grow(this.#values as Uint8Array, Uint8Array, next);
        break;
      case "i32":
        this.#values = this.#grow(this.#values as Int32Array, Int32Array, next);
        break;
      case "f32":
        this.#values = this.#grow(this.#values as Float32Array, Float32Array, next);
        break;
      case "f64":
        this.#values = this.#grow(this.#values as Float64Array, Float64Array, next);
        break;
      case "int64":
        this.#words = this.#grow(this.#words, Uint32Array, 2 * next);
        break;
      case "decimal":
        this.#values = this.#grow(this.#values as Float64Array, Float64Array, next);
        this.#tiers = this.#grow(this.#tiers, Uint8Array, next);
        this.#scales = this.#grow(this.#scales, Uint16Array, next);
        this.#specials = this.#grow(this.#specials, Uint8Array, next);
        if (this.#limbs !== null) this.#limbs = this.#grow(this.#limbs, Uint32Array, 4 * next);
        if (this.#huge !== null) {
          this.#take(8 * (next - this.#capacity));
          this.#huge.length = next;
          this.#huge.fill(0n, this.#capacity);
        }
        break;
      case "interval":
        this.#months = this.#grow(this.#months, Int32Array, next);
        this.#days = this.#grow(this.#days, Int32Array, next);
        this.#words = this.#grow(this.#words, Uint32Array, 2 * next);
        break;
      case "varlen":
      case "fixed16":
        this.#starts = this.#grow(this.#starts, Int32Array, next);
        this.#ends = this.#grow(this.#ends, Int32Array, next);
        break;
    }
    this.#capacity = next;
  }

  /** Room for `extra` more arena bytes: compacts first when more than half is dead. */
  #bytesFor(extra: number): void {
    if (this.#used + extra <= this.#bytes.length) return;
    if (this.#dead > this.#used / 2) this.#compact();
    if (this.#used + extra <= this.#bytes.length) return;
    const next = this.#make(Uint8Array, Math.max(1024, this.#used + extra, 2 * this.#bytes.length));
    next.set(this.#bytes.subarray(0, this.#used));
    this.#give(this.#bytes.length);
    this.#bytes = next;
  }

  /** Copies every live entry into a new arena of exactly the live size (at least 1 KiB). */
  #compact(): void {
    const next = this.#make(Uint8Array, Math.max(1024, this.#used - this.#dead));
    let at = 0;
    for (let e = 0; e < this.#length; e++) {
      const start = this.#starts[e] ?? 0;
      const end = this.#ends[e] ?? 0;
      next.set(this.#bytes.subarray(start, end), at);
      this.#starts[e] = at;
      at += end - start;
      this.#ends[e] = at;
    }
    this.#give(this.#bytes.length);
    this.#bytes = next;
    this.#used = at;
    this.#dead = 0;
  }

  /** Appends slot `row` of `vector` (null when that slot is invalid). Returns the new index. */
  append(vector: Vector, row: number): number {
    const index = this.#length;
    this.#ensure(index + 1);
    this.#length = index + 1;
    if (this.layout === "varlen" || this.layout === "fixed16") {
      this.#starts[index] = this.#used;
      this.#ends[index] = this.#used;
    }
    this.set(index, vector, row);
    return index;
  }

  /** Overwrites entry `index` with slot `row` of `vector`. */
  set(index: number, vector: Vector, row: number): void {
    const valid =
      vector.form === "constant"
        ? vector.value.validity === null || ((vector.value.validity[0] ?? 0) & 1) === 1
        : vector.validity === null || ((vector.validity[row >>> 5] ?? 0) & (1 << (row & 31))) !== 0;
    this.#nulls[index] = valid ? 0 : 1;
    if (!valid) return;
    const [flat, slot] = slotOf(vector, row);
    this.#setValue(index, flat.data, slot);
  }

  #setValue(index: number, data: VectorData, slot: number): void {
    switch (data.layout) {
      case "bool":
      case "i32":
      case "f32":
      case "f64":
        this.#values[index] = data.values[slot] ?? 0;
        return;
      case "int64":
        this.#setWords(index, data.values, slot);
        return;
      case "decimal": {
        this.#specials[index] = specialAt(data, slot);
        this.#scales[index] = scaleAt(data, slot);
        const c = data.coefficients;
        switch (c.repr) {
          case "safe":
            this.#setTier(index, SAFE);
            this.#values[index] = c.values[slot] ?? 0;
            return;
          case "int64":
            this.#setTier(index, LIMBS);
            copyInt64ToLimbs(this.#limbStore(), index, int64Words(c.values), slot);
            return;
          case "int128":
            this.#setTier(index, LIMBS);
            copy(this.#limbStore(), index, c.limbs, slot);
            return;
          case "wide": {
            const value = c.values[slot] ?? 0n;
            if (value >= -BigInt(MAX_SAFE) && value <= BigInt(MAX_SAFE)) {
              this.#setTier(index, SAFE);
              this.#values[index] = Number(value);
            } else {
              this.#setTier(index, LIMBS);
              if (!setBigInt(this.#limbStore(), index, value)) {
                this.#setTier(index, HUGE);
                this.#hugeStore()[index] = value;
              }
            }
            return;
          }
        }
        return;
      }
      case "interval":
        this.#months[index] = data.months[slot] ?? 0;
        this.#days[index] = data.days[slot] ?? 0;
        this.#setWords(index, data.micros, slot);
        return;
      case "varlen": {
        const start = data.offsets[slot] ?? 0;
        const end = data.offsets[slot + 1] ?? 0;
        this.#writeBytes(index, data.bytes, start, end);
        return;
      }
      case "fixed16":
        this.#writeBytes(index, data.bytes, slot * 16, slot * 16 + 16);
        return;
    }
  }

  #setWords(
    index: number,
    values: { repr: "safe"; values: Float64Array } | { repr: "int64"; values: BigInt64Array },
    slot: number,
  ): void {
    if (values.repr === "safe") wordsOfNumber(values.values[slot] ?? 0, this.#words, index);
    else {
      const source = int64Words(values.values);
      this.#words[index * 2] = source[slot * 2] ?? 0;
      this.#words[index * 2 + 1] = source[slot * 2 + 1] ?? 0;
    }
  }

  /** Moves a decimal entry to `tier`, creating the limb or BigInt storage on first use. */
  #setTier(index: number, tier: number): void {
    const before = this.#tiers[index] ?? SAFE;
    if (before === HUGE && tier !== HUGE) {
      this.#hugeStore()[index] = 0n;
      this.#give(HUGE_BYTES);
    }
    if (tier === LIMBS && this.#limbs === null)
      this.#limbs = this.#make(Uint32Array, 4 * this.#capacity);
    if (tier === HUGE) {
      if (this.#huge === null) {
        this.#take(8 * this.#capacity);
        this.#huge = new Array<bigint>(this.#capacity).fill(0n);
      }
      if (before !== HUGE) this.#take(HUGE_BYTES);
    }
    this.#tiers[index] = tier;
  }

  /** The limb store (created by `#setTier` before any LIMBS entry is written). */
  #limbStore(): Uint32Array {
    this.#limbs ??= this.#make(Uint32Array, 4 * this.#capacity);
    return this.#limbs;
  }

  /** The BigInt store (created by `#setTier` before any HUGE entry is written). */
  #hugeStore(): bigint[] {
    if (this.#huge === null) {
      this.#take(8 * this.#capacity);
      this.#huge = new Array<bigint>(this.#capacity).fill(0n);
    }
    return this.#huge;
  }

  #writeBytes(index: number, source: Uint8Array, start: number, end: number): void {
    const length = end - start;
    const oldStart = this.#starts[index] ?? 0;
    const oldEnd = this.#ends[index] ?? 0;
    if (length <= oldEnd - oldStart) {
      this.#bytes.set(source.subarray(start, end), oldStart);
      this.#dead += oldEnd - oldStart - length;
      this.#ends[index] = oldStart + length;
      return;
    }
    // Empty the entry before any compaction, so the compacted arena holds exactly the live
    // bytes (the old value is dead and must not be copied).
    this.#dead += oldEnd - oldStart;
    this.#ends[index] = oldStart;
    this.#bytesFor(length);
    this.#bytes.set(source.subarray(start, end), this.#used);
    this.#starts[index] = this.#used;
    this.#used += length;
    this.#ends[index] = this.#used;
  }

  isNull(index: number): boolean {
    return this.#nulls[index] === 1;
  }

  /** bool / int32 / float values, and decimal coefficients in the safe tier. */
  number(index: number): number {
    return this.#values[index] ?? 0;
  }

  /** int64 (and interval microseconds): the high word, signed. */
  high(index: number): number {
    return (this.#words[index * 2 + 1] ?? 0) | 0;
  }

  /** int64 (and interval microseconds): the low word, unsigned. */
  low(index: number): number {
    return this.#words[index * 2] ?? 0;
  }

  /** Decimal: whether the coefficient is past 128 bits (kept as a BigInt). */
  isHuge(index: number): boolean {
    return this.#tiers[index] === HUGE;
  }

  /** Decimal: whether the coefficient is in the safe f64 tier. */
  isSafe(index: number): boolean {
    return (this.#tiers[index] ?? SAFE) === SAFE;
  }

  /** Decimal: the coefficient into limb slot `at` of `t` (not for huge entries). */
  loadCoefficient(index: number, t: Uint32Array, at: number): void {
    if ((this.#tiers[index] ?? SAFE) === SAFE) setNumber(t, at, this.#values[index] ?? 0);
    else copy(t, at, this.#limbStore(), index);
  }

  /** Decimal: the coefficient as a bigint (huge entries, and emit paths that need it). */
  big(index: number): bigint {
    const tier = this.#tiers[index] ?? SAFE;
    if (tier === SAFE) return BigInt(this.#values[index] ?? 0);
    if (tier === HUGE) return this.#hugeStore()[index] ?? 0n;
    return toBigInt(this.#limbStore(), index);
  }

  scale(index: number): number {
    return this.#scales[index] ?? 0;
  }

  special(index: number): number {
    return this.#specials[index] ?? 0;
  }

  /** varlen / fixed16 bytes of entry `index` (a view, valid until the next write). */
  bytes(index: number): Uint8Array {
    return this.#bytes.subarray(this.#starts[index] ?? 0, this.#ends[index] ?? 0);
  }

  get byteArena(): Uint8Array {
    return this.#bytes;
  }

  start(index: number): number {
    return this.#starts[index] ?? 0;
  }

  end(index: number): number {
    return this.#ends[index] ?? 0;
  }

  /**
   * An upper bound of the bytes `emitAt` allocates for these entries (the caller charges it before
   * emitting): fixed-width buffers, validity, varlen bytes and offsets, BigInt estimates.
   */
  outputBytes(ids: Int32Array | null, from: number, count: number): number {
    const validity = 4 * ((count + 31) >>> 5);
    switch (this.layout) {
      case "bool":
        return validity + count;
      case "i32":
      case "f32":
        return validity + 4 * count;
      case "f64":
      case "int64":
        return validity + 8 * count;
      case "interval":
        return validity + 16 * count;
      case "fixed16":
        return validity + 16 * count;
      case "decimal": {
        let huge = 0;
        if (this.#huge !== null)
          for (let k = 0; k < count; k++)
            if (this.#tiers[ids === null ? from + k : (ids[k] ?? 0)] === HUGE) huge += 1;
        // Coefficients at their widest tier (16 bytes, or a BigInt each), scales, specials.
        return validity + count * (16 + 2 + 1) + (huge > 0 ? count * HUGE_BYTES : 0);
      }
      case "varlen": {
        let bytes = 0;
        for (let k = 0; k < count; k++) {
          const e = ids === null ? from + k : (ids[k] ?? 0);
          bytes += (this.#ends[e] ?? 0) - (this.#starts[e] ?? 0);
        }
        return validity + bytes + 4 * (count + 1);
      }
    }
  }

  /** Entries [from, from + count) as a new, exactly sized flat vector (caller charges it). */
  emit(from: number, count: number): FlatVector {
    return this.emitAt(null, from, count);
  }

  /** Entries `ids[0 … count)` (or from … from + count − 1 when `ids` is null), gathered. */
  emitAt(ids: Int32Array | null, from: number, count: number): FlatVector {
    const at = (k: number): number => (ids === null ? from + k : (ids[k] ?? 0));
    const type = this.type;
    let anyNull = false;
    for (let k = 0; k < count; k++) if (this.#nulls[at(k)] === 1) anyNull = true;
    let validity: Validity | null = null;
    if (anyNull) {
      validity = new Uint32Array((count + 31) >>> 5);
      for (let k = 0; k < count; k++)
        if (this.#nulls[at(k)] !== 1)
          validity[k >>> 5] = (validity[k >>> 5] ?? 0) | (1 << (k & 31));
    }
    const numbers = this.#values;
    let data: VectorData;
    switch (this.layout) {
      case "bool":
      case "i32":
      case "f32":
      case "f64": {
        const values =
          this.layout === "bool"
            ? new Uint8Array(count)
            : this.layout === "i32"
              ? new Int32Array(count)
              : this.layout === "f32"
                ? new Float32Array(count)
                : new Float64Array(count);
        for (let k = 0; k < count; k++) values[k] = numbers[at(k)] ?? 0;
        data = { layout: this.layout, values } as VectorData;
        break;
      }
      case "int64":
        data = { layout: "int64", values: this.#emitWords(at, count) };
        break;
      case "decimal":
        data = this.#emitDecimal(at, count);
        break;
      case "interval": {
        const months = new Int32Array(count);
        const days = new Int32Array(count);
        for (let k = 0; k < count; k++) {
          months[k] = this.#months[at(k)] ?? 0;
          days[k] = this.#days[at(k)] ?? 0;
        }
        data = { layout: "interval", months, days, micros: this.#emitWords(at, count) };
        break;
      }
      case "fixed16": {
        const bytes = new Uint8Array(16 * count);
        for (let k = 0; k < count; k++) bytes.set(this.bytes(at(k)).subarray(0, 16), 16 * k);
        data = { layout: "fixed16", bytes };
        break;
      }
      case "varlen": {
        let total = 0;
        for (let k = 0; k < count; k++)
          total += (this.#ends[at(k)] ?? 0) - (this.#starts[at(k)] ?? 0);
        const bytes = new Uint8Array(total);
        const offsets = new Int32Array(count + 1);
        let o = 0;
        for (let k = 0; k < count; k++) {
          const start = this.#starts[at(k)] ?? 0;
          const end = this.#ends[at(k)] ?? 0;
          bytes.set(this.#bytes.subarray(start, end), o);
          o += end - start;
          offsets[k + 1] = o;
        }
        data = { layout: "varlen", bytes, offsets };
        break;
      }
    }
    return { form: "flat", type, length: count, validity, data };
  }

  /** int64 words as the safe representation when every value fits, else exact int64. */
  #emitWords(
    at: (k: number) => number,
    count: number,
  ): { repr: "safe"; values: Float64Array } | { repr: "int64"; values: BigInt64Array } {
    let safe = true;
    for (let k = 0; k < count; k++) {
      const e = at(k);
      const value = this.high(e) * TWO32 + this.low(e);
      if (value > MAX_SAFE || value < -MAX_SAFE) safe = false;
    }
    if (safe) {
      const values = new Float64Array(count);
      for (let k = 0; k < count; k++) {
        const e = at(k);
        values[k] = this.high(e) * TWO32 + this.low(e);
      }
      return { repr: "safe", values };
    }
    const values = new BigInt64Array(count);
    const words = int64Words(values);
    for (let k = 0; k < count; k++) {
      const e = at(k);
      words[2 * k] = this.#words[2 * e] ?? 0;
      words[2 * k + 1] = this.#words[2 * e + 1] ?? 0;
    }
    return { repr: "int64", values };
  }

  #emitDecimal(at: (k: number) => number, count: number): VectorData {
    const specials = new Uint8Array(count);
    const scales = new Uint16Array(count);
    let anySpecial = false;
    let tier = SAFE;
    for (let k = 0; k < count; k++) {
      const e = at(k);
      specials[k] = this.#specials[e] ?? 0;
      if (specials[k] !== 0) anySpecial = true;
      scales[k] = this.#scales[e] ?? 0;
      tier = Math.max(tier, this.#tiers[e] ?? SAFE);
    }
    let coefficients: Extract<VectorData, { layout: "decimal" }>["coefficients"];
    if (tier === SAFE) {
      const values = new Float64Array(count);
      for (let k = 0; k < count; k++) values[k] = this.#values[at(k)] ?? 0;
      coefficients = { repr: "safe", values };
    } else if (tier === LIMBS) {
      const limbs = new Uint32Array(4 * count);
      let fits = true;
      for (let k = 0; k < count; k++) {
        this.loadCoefficient(at(k), limbs, k);
        if (!fitsInt64(limbs, k)) fits = false;
      }
      if (fits) {
        const values = new BigInt64Array(count);
        const words = int64Words(values);
        for (let k = 0; k < count; k++) {
          words[2 * k] = limbs[4 * k] ?? 0;
          words[2 * k + 1] = limbs[4 * k + 1] ?? 0;
        }
        coefficients = { repr: "int64", values };
      } else coefficients = { repr: "int128", limbs };
    } else
      coefficients = {
        repr: "wide",
        values: Array.from({ length: count }, (_, k) => this.big(at(k))),
      };
    return {
      layout: "decimal",
      scale: null,
      scales,
      coefficients,
      specials: anySpecial ? specials : null,
    };
  }

  /** Bytes this store holds from its charge. */
  get chargedBytes(): number {
    return this.#charged;
  }

  close(): void {
    if (this.#charged > 0) this.charge.free(this.#charged);
    this.#charged = 0;
    this.#length = 0;
    this.#capacity = 0;
    this.#nulls = new Uint8Array(0);
    this.#values = new Float64Array(0);
    this.#words = new Uint32Array(0);
    this.#tiers = new Uint8Array(0);
    this.#limbs = null;
    this.#huge = null;
    this.#bytes = new Uint8Array(0);
    this.#starts = new Int32Array(0);
    this.#ends = new Int32Array(0);
  }
}

/** limbs[index] = the int64 at `slot` of `words`, sign-extended. */
function copyInt64ToLimbs(
  limbs: Uint32Array,
  index: number,
  words: Uint32Array,
  slot: number,
): void {
  WORDS[0] = words[slot * 2] ?? 0;
  WORDS[1] = words[slot * 2 + 1] ?? 0;
  const o = index * 4;
  limbs[o] = WORDS[0];
  limbs[o + 1] = WORDS[1];
  const extension = WORDS[1] >>> 31 === 1 ? 0xffff_ffff : 0;
  limbs[o + 2] = extension;
  limbs[o + 3] = extension;
}

/** Bytes of a flat vector's buffers (for charging an emitted batch). */
export function flatBytes(vector: FlatVector): number {
  const data = vector.data;
  let bytes = vector.validity?.byteLength ?? 0;
  switch (data.layout) {
    case "bool":
    case "i32":
    case "f32":
    case "f64":
      return bytes + data.values.byteLength;
    case "int64":
      return bytes + data.values.values.byteLength;
    case "decimal":
      bytes += (data.scales?.byteLength ?? 0) + (data.specials?.byteLength ?? 0);
      return (
        bytes +
        (data.coefficients.repr === "wide"
          ? HUGE_BYTES * data.coefficients.values.length
          : data.coefficients.repr === "int128"
            ? data.coefficients.limbs.byteLength
            : data.coefficients.values.byteLength)
      );
    case "varlen":
      return bytes + data.bytes.byteLength + data.offsets.byteLength;
    case "fixed16":
      return bytes + data.bytes.byteLength;
    case "interval":
      return bytes + data.months.byteLength + data.days.byteLength + data.micros.values.byteLength;
  }
}
