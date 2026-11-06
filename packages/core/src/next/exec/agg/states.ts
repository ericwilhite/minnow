/**
 * Aggregate accumulators: struct-of-arrays state indexed by group id (no object per group).
 *
 * ## Contract (`Accumulator`)
 * - `grow(groups)` makes room for groups 0 … groups − 1 (new groups start empty), charged first.
 * - `update(vector, gids, rows, count, scalar)` adds the active rows `rows[0 … count)` of `vector`
 *   (null for COUNT(*)) to groups `gids[row]`; `scalar` means every group id is 0 (no GROUP BY),
 *   which enables dense batch loops. NULL inputs are ignored, as SQL aggregates do.
 * - `emit(from, count)` returns the results of groups [from, from + count) as a new flat vector of
 *   the aggregate's result type: NULL for SUM/AVG/MIN/MAX/BOOL_* over no value, 0 for COUNT.
 *
 * ## Exactness
 * - Integer and NUMERIC sums keep an f64 part within ±2^52 per group plus a 128-bit limb carry
 *   (limbs.ts): every addition is exact, and the per-row check is a predictable branch taken only
 *   when the f64 part would leave its range. A carry overflow moves ±2^128 into an exact BigInt
 *   offset for the group (`ExactSums`), so sums of any size are exact like PostgreSQL's; BigInts
 *   appear only at those overflows, for inputs already wider than 128 bits, at emit per group,
 *   and for a NUMERIC alignment past 128 bits.
 * - The fused scalar NUMERIC SUM: when the input's declared precision is ≤ 12 (|coefficient| <
 *   10^12, so one batch of 2,048 values sums below 2^51 exactly) and the batch is in the safe
 *   tier with one scale, a batch is summed with four independent accumulators and no per-row
 *   check, then folded into the state once.
 * - NUMERIC SUM's display scale is the largest input scale of the group (PostgreSQL); a group that
 *   meets a larger scale is rescaled once. NaN absorbs; +∞ and −∞ together give NaN.
 * - Float sums are IEEE sums in input order (PostgreSQL's float8pl / float4pl, REAL rounded after
 *   each addition), starting from −0 so SUM(−0) is −0; an infinite sum of two finite operands is
 *   22003, row by row as float8pl checks it (a later ±∞ in the same group does not hide it).
 * - MIN / MAX: a tie takes the newer value, as PostgreSQL's transition functions do, so the
 *   result's display (1.5 or 1.50, 0 or −0) matches.
 * - AVG of integers and floats is float64 (DECISIONS.md C11): the exact sum divided by the count.
 *   AVG of NUMERIC is NUMERIC: sum / count with PostgreSQL's division scale (decimal-div.ts).
 */
import type { PhysicalType } from "../../types.js";
import { layoutOf } from "../../types.js";
import { MAX_SAFE } from "../../vector/int64.js";
import { compareBytes } from "../../vector/varlen.js";
import type { ByteCharge } from "../../vector/varlen.js";
import {
  BATCH_ROWS,
  type FlatVector,
  type Int64Values,
  type Validity,
  type Vector,
} from "../../vector/vector.js";
import { CardinalityError, NumericOutOfRangeError, UnsupportedPlanError } from "../errors.js";
import { numericDivide } from "../kernels/decimal-div.js";
import {
  SCRATCH,
  addTo,
  compare as compareLimbs,
  fitsInt64,
  int64Words,
  scaleUp,
  setInt64,
  setBigInt,
  setNumber,
  storeInt64,
  toBigInt,
  toDouble,
} from "../kernels/limbs.js";
import { POW10, coefficientAt, scaleAt, specialAt, type DecimalData } from "../kernels/numeric.js";
import { IDENTITY, flatOf } from "../kernels/vectors.js";
import { Charged } from "./groups.js";
import { bigintBytes } from "../../memory/governor.js";
import { GroupValues } from "./values.js";

export interface Accumulator {
  readonly type: PhysicalType;
  grow(groups: number): void;
  update(
    vector: Vector | null,
    gids: Int32Array,
    rows: Uint16Array,
    count: number,
    scalar: boolean,
  ): void;
  emit(from: number, count: number): FlatVector;
  /** An upper bound of the bytes `emit(from, count)` allocates (charged before emitting). */
  outputBytes(from: number, count: number): number;
  /**
   * Accumulators whose emit does expensive work per group (AVG over NUMERIC: an exact division
   * each) prepare it under a deadline: returns how many groups from `from` (≤ `count`) are ready,
   * at least one. `emit(from, k)` for k up to that is then cheap.
   */
  prepare?(from: number, count: number, expired: () => boolean): number;
  close(): void;
}

/** The f64 part of an exact sum stays within ±LIMIT so every addition of a value ≤ LIMIT is exact. */
const LIMIT = 2 ** 52;

/** How a batch's rows map to the values' flat slots. */
interface Reader {
  readonly flat: FlatVector;
  /** 1 flat, 0 constant. */
  readonly stride: number;
  /** Dictionary codes, or null. */
  readonly codes: Int32Array | null;
  readonly validity: Validity | null;
  /** A constant NULL: the batch adds nothing. */
  readonly empty: boolean;
}

function readerOf(vector: Vector): Reader {
  if (vector.form === "constant") {
    const validity = vector.value.validity;
    const empty = validity !== null && ((validity[0] ?? 0) & 1) === 0;
    return { flat: vector.value, stride: 0, codes: null, validity: null, empty };
  }
  return {
    flat: flatOf(vector),
    stride: 1,
    codes: vector.form === "dictionary" ? vector.codes : null,
    validity: vector.validity,
    empty: false,
  };
}

function slotAt(reader: Reader, row: number): number {
  return reader.codes !== null ? (reader.codes[row] ?? 0) : row * reader.stride;
}

/**
 * Row-indexed f64 scratch: one batch's number values converted by `rowValues`. Module-level and
 * used only within one synchronous `update` (16 KiB, not charged per operator).
 */
const ROW_VALUES = new Float64Array(BATCH_ROWS);

/**
 * The batch's number values (int32, float32, float64, safe int64 or safe decimal coefficients)
 * as one Float64Array indexed by row: the vector's own array when it is flat f64, else converted
 * into `ROW_VALUES` (exactly: every source fits f64). Keeps the accumulator loops monomorphic —
 * one loop that reads Int32Array and Float64Array elements runs several times slower.
 */
function rowValues(
  values: Float64Array | Float32Array | Int32Array,
  reader: Reader,
  length: number,
): Float64Array {
  if (reader.codes === null && reader.stride === 1) {
    if (values instanceof Float64Array) return values;
    ROW_VALUES.set(values.subarray(0, length));
    return ROW_VALUES;
  }
  if (reader.codes === null) ROW_VALUES.fill(values[0] ?? 0, 0, length);
  else {
    const codes = reader.codes;
    for (let r = 0; r < length; r++) ROW_VALUES[r] = values[codes[r] ?? 0] ?? 0;
  }
  return ROW_VALUES;
}

function validAt(validity: Validity | null, row: number): boolean {
  return validity === null || ((validity[row >>> 5] ?? 0) & (1 << (row & 31))) !== 0;
}

/** Validity of `present[from … from + count)` (1: has a value); null when every one has. */
/** Bytes of a validity bitmap for `count` slots. */
const validityBytes = (count: number): number => 4 * ((count + 31) >>> 5);

function validityFrom(present: Uint8Array, from: number, count: number): Validity | null {
  let any = false;
  for (let k = 0; k < count; k++) if (present[from + k] === 0) any = true;
  if (!any) return null;
  const validity = new Uint32Array((count + 31) >>> 5);
  for (let k = 0; k < count; k++)
    if (present[from + k] !== 0) validity[k >>> 5] = (validity[k >>> 5] ?? 0) | (1 << (k & 31));
  return validity;
}

// ---------------------------------------------------------------------------------------------
// COUNT(*), COUNT(x)

class CountAccumulator implements Accumulator {
  readonly #memory: Charged;
  #counts: Float64Array = new Float64Array(0);
  constructor(
    readonly type: PhysicalType,
    readonly star: boolean,
    charge: ByteCharge,
  ) {
    this.#memory = new Charged(charge);
  }
  grow(groups: number): void {
    if (groups > this.#counts.length)
      this.#counts = this.#memory.float64(this.#counts, Math.max(groups, 2 * this.#counts.length));
  }
  update(
    vector: Vector | null,
    gids: Int32Array,
    rows: Uint16Array,
    count: number,
    scalar: boolean,
  ): void {
    const counts = this.#counts;
    const reader = vector === null ? null : readerOf(vector);
    if (reader?.empty === true) return;
    const validity = reader === null ? null : reader.validity;
    if (validity === null) {
      if (scalar) counts[0] = (counts[0] ?? 0) + count;
      else
        for (let k = 0; k < count; k++) {
          const g = gids[rows[k] ?? 0] ?? 0;
          counts[g] = (counts[g] ?? 0) + 1;
        }
      return;
    }
    for (let k = 0; k < count; k++) {
      const i = rows[k] ?? 0;
      const bit = ((validity[i >>> 5] ?? 0) >>> (i & 31)) & 1;
      const g = scalar ? 0 : (gids[i] ?? 0);
      counts[g] = (counts[g] ?? 0) + bit;
    }
  }
  /** Counts as plain numbers (AVG reads them). */
  count(g: number): number {
    return this.#counts[g] ?? 0;
  }
  outputBytes(_from: number, count: number): number {
    return 8 * count;
  }
  emit(from: number, count: number): FlatVector {
    return {
      form: "flat",
      type: this.type,
      length: count,
      validity: null,
      data: {
        layout: "int64",
        values: { repr: "safe", values: this.#counts.slice(from, from + count) },
      },
    };
  }
  close(): void {
    this.#memory.close();
  }
}

// ---------------------------------------------------------------------------------------------
// Exact integer sums (SUM(int2/int4/int8), the sum inside AVG of integers)

const TWO128 = 1n << 128n;
/**
 * PostgreSQL's NUMERIC limits (numeric.c NUMERIC_WEIGHT_MAX / NUMERIC_DSCALE_MAX): at most 131,072
 * digits before the decimal point and 16,383 after. A result beyond them is 22003 "value overflows
 * numeric format" when it is produced (emitted): exact intermediates may pass it and come back.
 */
const NUMERIC_MAX_INTEGER_DIGITS = 131_072;

/** Throws 22003 when `coefficient × 10^−scale` has more integer digits than NUMERIC allows. */
function checkNumericRange(coefficient: bigint, scale: number): bigint {
  // Only values far past 128 bits can be out of range (39 digits), so the digit count is rare.
  if (coefficient > 10n ** 38n || coefficient < -(10n ** 38n)) {
    const digits = (coefficient < 0n ? -coefficient : coefficient).toString().length;
    if (digits - scale > NUMERIC_MAX_INTEGER_DIGITS)
      throw new NumericOutOfRangeError("value overflows numeric format");
  }
  return coefficient;
}

/** A slot of a JS array of BigInts (the pointer), charged per group of a dense offset array. */
const SLOT_BYTES = 8;

/**
 * Per-group exact sum: an f64 part within ±LIMIT, plus a 128-bit limb carry, plus an exact BigInt
 * offset for whatever does not fit 128 bits (created at the first such group, charged at
 * `HUGE_BYTES` per group). A carry addition that overflows leaves the wrapped value in the carry
 * and moves ±2^128 into the offset (both operands had that sign), so the offset is touched once per
 * overflow, never per row; only inputs already past 128 bits add to it directly.
 */
class ExactSums {
  readonly #memory: Charged;
  sums: Float64Array = new Float64Array(0);
  carry: Uint32Array = new Uint32Array(0);
  /** 1 once the group has a value. */
  has: Uint8Array = new Uint8Array(0);
  /** Groups whose carry is nonzero (so emit can pick the safe tier quickly). */
  carried = 0;
  /** The offsets past 128 bits, or null while every total fits the carry. */
  wide: bigint[] | null = null;
  /** Per group: the `bigintBytes` charged for its offset (0 while it is 0n). */
  #wideBytes: Float64Array = new Float64Array(0);
  constructor(charge: ByteCharge) {
    this.#memory = new Charged(charge);
  }
  grow(groups: number): void {
    if (groups <= this.sums.length) return;
    const before = this.sums.length;
    const capacity = Math.max(groups, 2 * this.sums.length);
    this.sums = this.#memory.float64(this.sums, capacity);
    this.carry = this.#memory.uint32(this.carry, 4 * capacity);
    this.has = this.#memory.uint8(this.has, capacity);
    if (this.wide !== null) {
      this.#wideBytes = this.#memory.float64(this.#wideBytes, capacity);
      this.#memory.take(SLOT_BYTES * (capacity - before));
      this.wide.length = capacity;
      this.wide.fill(0n, before);
    }
  }
  #offsets(): bigint[] {
    if (this.wide === null) {
      this.#wideBytes = this.#memory.float64(this.#wideBytes, this.sums.length);
      this.#memory.take(SLOT_BYTES * this.sums.length);
      this.wide = new Array<bigint>(this.sums.length).fill(0n);
    }
    return this.wide;
  }
  /**
   * Stores group g's new offset, charging its size change first (the governor's `bigintBytes`
   * per retained BigInt): a refused charge throws before anything changes.
   */
  #setOffset(g: number, value: bigint): void {
    const offsets = this.#offsets();
    const bytes = value === 0n ? 0 : bigintBytes(value);
    const before = this.#wideBytes[g] ?? 0;
    if (bytes > before) this.#memory.take(bytes - before);
    else if (bytes < before) this.#memory.give(before - bytes);
    this.#wideBytes[g] = bytes;
    offsets[g] = value;
  }
  /**
   * Bytes of the BigInts a `wide` output of groups [from, from + count) holds: once any of them is
   * past 128 bits, every value of the batch is a BigInt (its offset's size plus the carry's
   * 128 bits, or a small one) and an array slot.
   */
  wideOutputBytes(from: number, count: number): number {
    if (this.wide === null) return 0;
    let bytes = 0;
    let any = false;
    for (let k = 0; k < count; k++) {
      const offset = this.#wideBytes[from + k] ?? 0;
      if (offset > 0) any = true;
      bytes += (offset > 0 ? offset + 16 : 32) + SLOT_BYTES;
    }
    return any ? bytes : 0;
  }

  /** Bytes the offset of group g holds (its BigInt's `bigintBytes`; 0 when none). */
  offsetBytes(g: number): number {
    return this.#wideBytes[g] ?? 0;
  }
  /** carry[g] += t[at] (128-bit), exact: an overflow moves ±2^128 into the offset. */
  addCarry(g: number, t: Uint32Array, at: number): void {
    if (!addTo(this.carry, g, this.carry, g, t, at, false)) {
      const offset = this.wide?.[g] ?? 0n;
      this.#setOffset(g, offset + ((t[at * 4 + 3] ?? 0) >>> 31 === 1 ? -TWO128 : TWO128));
    }
    this.carried += 1;
  }
  /** Adds an exact value of any size (an input past 128 bits) to group g. */
  addBig(g: number, value: bigint): void {
    this.#setOffset(g, (this.wide?.[g] ?? 0n) + value);
  }
  /** Moves `value` (exact, any magnitude up to 2^63) into group g's carry. */
  spill(g: number, value: number): void {
    setNumber(SCRATCH, 0, value);
    this.addCarry(g, SCRATCH, 0);
  }
  /** Adds the int64 at `index` of `words` (int64Words) to group g's carry. */
  spillWords(g: number, words: Uint32Array, index: number): void {
    setInt64(SCRATCH, 0, words, index);
    this.addCarry(g, SCRATCH, 0);
  }
  /** Adds a number value (exact integer, |v| ≤ 2^53) to group g. */
  add(g: number, v: number): void {
    if (v > LIMIT || v < -LIMIT) {
      this.spill(g, v);
      return;
    }
    const s = (this.sums[g] ?? 0) + v;
    if (s > LIMIT || s < -LIMIT) {
      this.spill(g, this.sums[g] ?? 0);
      this.sums[g] = v;
    } else this.sums[g] = s;
  }
  /**
   * Group g's sum into limb slot `to` of `t` when it fits 128 bits; false when it does not (read
   * it with `big`).
   */
  total(g: number, t: Uint32Array, to: number): boolean {
    if (this.wide !== null && this.wide[g] !== 0n) return false;
    setNumber(t, to, this.sums[g] ?? 0);
    return this.carried === 0 || addTo(t, to, t, to, this.carry, g, false);
  }
  /** Group g's exact sum (per group, at emit or rescale: never per row). */
  big(g: number): bigint {
    return (
      BigInt(this.sums[g] ?? 0) +
      (this.carried > 0 ? toBigInt(this.carry, g) : 0n) +
      (this.wide?.[g] ?? 0n)
    );
  }
  /** Replaces group g's sum with `value` (exact, any size). */
  set(g: number, value: bigint): void {
    this.sums[g] = 0;
    if (setBigInt(this.carry, g, value)) {
      if (this.wide !== null) this.#setOffset(g, 0n);
    } else {
      // The low 128 bits (two's complement) in the carry, the rest in the offset.
      const low = BigInt.asIntN(128, value);
      setBigInt(this.carry, g, low);
      this.#setOffset(g, value - low);
    }
    this.carried += 1;
  }
  close(): void {
    this.#memory.close();
    this.wide = null;
  }
}

/** Adds an integer vector's active rows to `sums` (int32 or int64, any representation). */
function addIntegers(
  sums: ExactSums,
  vector: Vector,
  gids: Int32Array,
  rows: Uint16Array,
  count: number,
  scalar: boolean,
): void {
  const reader = readerOf(vector);
  if (reader.empty) return;
  const data = reader.flat.data;
  const validity = reader.validity;
  const has = sums.has;
  if (data.layout === "int64" && data.values.repr === "int64") {
    const words = int64Words(data.values.values);
    for (let k = 0; k < count; k++) {
      const i = rows[k] ?? 0;
      if (!validAt(validity, i)) continue;
      const g = scalar ? 0 : (gids[i] ?? 0);
      has[g] = 1;
      sums.spillWords(g, words, slotAt(reader, i));
    }
    return;
  }
  const values = rowValues(
    data.layout === "int64"
      ? (data.values as { values: Float64Array }).values
      : (data as { values: Int32Array }).values,
    reader,
    vector.length,
  );
  if (scalar && data.layout === "i32") {
    // int32: a batch sums below 2^42, exactly, with no per-row check.
    let s0 = 0;
    let s1 = 0;
    let n = 0;
    if (validity === null) {
      let k = 0;
      for (; k + 1 < count; k += 2) {
        s0 += values[rows[k] ?? 0] ?? 0;
        s1 += values[rows[k + 1] ?? 0] ?? 0;
      }
      if (k < count) s0 += values[rows[k] ?? 0] ?? 0;
      n = count;
    } else
      for (let k = 0; k < count; k++) {
        const i = rows[k] ?? 0;
        const bit = ((validity[i >>> 5] ?? 0) >>> (i & 31)) & 1;
        s0 += (values[i] ?? 0) * bit;
        n += bit;
      }
    if (n > 0) {
      has[0] = 1;
      sums.add(0, s0 + s1);
    }
    return;
  }
  if (!scalar) {
    addFlat(sums, values, validity, gids, rows, count);
    return;
  }
  for (let k = 0; k < count; k++) {
    const i = rows[k] ?? 0;
    if (!validAt(validity, i)) continue;
    has[0] = 1;
    sums.add(0, values[i] ?? 0);
  }
}

/**
 * Adds flat exact-integer values to their groups, inline: a sum whose f64 part stays within
 * ±LIMIT is exact (the exact result of a ±2^52 part plus a ±2^53 value is representable when it
 * lands within ±2^52, and rounding is monotonic), so only a sum that leaves it takes `add`.
 */
function addFlat(
  sums: ExactSums,
  values: Float64Array,
  validity: Validity | null,
  gids: Int32Array,
  rows: Uint16Array,
  count: number,
): void {
  const parts = sums.sums;
  const has = sums.has;
  if (validity === null)
    for (let k = 0; k < count; k++) {
      const i = rows[k] ?? 0;
      const g = gids[i] ?? 0;
      const v = values[i] ?? 0;
      const total = (parts[g] ?? 0) + v;
      has[g] = 1;
      if (total > LIMIT || total < -LIMIT) sums.add(g, v);
      else parts[g] = total;
    }
  else
    for (let k = 0; k < count; k++) {
      const i = rows[k] ?? 0;
      if (((validity[i >>> 5] ?? 0) & (1 << (i & 31))) === 0) continue;
      const g = gids[i] ?? 0;
      const v = values[i] ?? 0;
      const total = (parts[g] ?? 0) + v;
      has[g] = 1;
      if (total > LIMIT || total < -LIMIT) sums.add(g, v);
      else parts[g] = total;
    }
}

class IntegerSumAccumulator implements Accumulator {
  readonly #sums: ExactSums;
  readonly #counts: CountAccumulator | null;
  constructor(
    readonly type: PhysicalType,
    charge: ByteCharge,
    /** AVG: also count, and emit sum / count as float64. */
    readonly average: boolean,
  ) {
    this.#sums = new ExactSums(charge);
    this.#counts = average ? new CountAccumulator({ kind: "int64" }, false, charge) : null;
  }
  grow(groups: number): void {
    this.#sums.grow(groups);
    this.#counts?.grow(groups);
  }
  update(
    vector: Vector | null,
    gids: Int32Array,
    rows: Uint16Array,
    count: number,
    scalar: boolean,
  ): void {
    if (vector === null) return;
    addIntegers(this.#sums, vector, gids, rows, count, scalar);
    this.#counts?.update(vector, gids, rows, count, scalar);
  }
  outputBytes(from: number, count: number): number {
    // f64 or int64 coefficients, 16 bytes of limbs, or (past 128 bits) a BigInt each; validity.
    return validityBytes(count) + count * 24 + this.#sums.wideOutputBytes(from, count);
  }
  emit(from: number, count: number): FlatVector {
    const sums = this.#sums;
    const validity = validityFrom(sums.has, from, count);
    if (this.#counts !== null) {
      const out = new Float64Array(count);
      for (let k = 0; k < count; k++) {
        const g = from + k;
        if (sums.has[g] !== 1) continue;
        const total = sums.total(g, SCRATCH, 4) ? toDouble(SCRATCH, 4) : Number(sums.big(g));
        out[k] = total / this.#counts.count(g);
      }
      return {
        form: "flat",
        type: this.type,
        length: count,
        validity,
        data: { layout: "f64", values: out },
      };
    }
    // SUM(int2/int4) is int64; SUM(int8) is NUMERIC(·, 0) (exact past 2^63).
    const decimal = this.type.kind === "decimal";
    if (sums.carried === 0 && sums.wide === null) {
      const values = sums.sums.slice(from, from + count);
      return decimal
        ? {
            form: "flat",
            type: this.type,
            length: count,
            validity,
            data: {
              layout: "decimal",
              scale: 0,
              scales: null,
              coefficients: { repr: "safe", values },
              specials: null,
            },
          }
        : {
            form: "flat",
            type: this.type,
            length: count,
            validity,
            data: { layout: "int64", values: { repr: "safe", values } },
          };
    }
    const limbs = new Uint32Array(4 * count);
    let fits = true;
    let wide = false;
    for (let k = 0; k < count; k++) {
      if (!sums.total(from + k, limbs, k)) wide = true;
      else if (!fitsInt64(limbs, k)) fits = false;
    }
    if (!decimal && (wide || !fits)) throw new NumericOutOfRangeError("bigint out of range");
    if (wide)
      return {
        form: "flat",
        type: this.type,
        length: count,
        validity,
        data: {
          layout: "decimal",
          scale: 0,
          scales: null,
          coefficients: {
            repr: "wide",
            values: Array.from({ length: count }, (_, k) =>
              checkNumericRange(sums.big(from + k), 0),
            ),
          },
          specials: null,
        },
      };
    if (fits) {
      const big = new BigInt64Array(count);
      const words = int64Words(big);
      for (let k = 0; k < count; k++) storeInt64(limbs, k, words, k);
      const values: Int64Values = { repr: "int64", values: big };
      return decimal
        ? {
            form: "flat",
            type: this.type,
            length: count,
            validity,
            data: {
              layout: "decimal",
              scale: 0,
              scales: null,
              coefficients: values,
              specials: null,
            },
          }
        : {
            form: "flat",
            type: this.type,
            length: count,
            validity,
            data: { layout: "int64", values },
          };
    }
    return {
      form: "flat",
      type: this.type,
      length: count,
      validity,
      data: {
        layout: "decimal",
        scale: 0,
        scales: null,
        coefficients: { repr: "int128", limbs },
        specials: null,
      },
    };
  }
  close(): void {
    this.#sums.close();
    this.#counts?.close();
  }
}

// ---------------------------------------------------------------------------------------------
// Float sums and averages

/** Float sums of grouped rows without NULLs until the first non-finite sum; returns that row. */
function sumDense(
  sums: Float64Array,
  counts: Float64Array,
  values: Float64Array,
  gids: Int32Array,
  rows: Uint16Array,
  count: number,
): number {
  let k = 0;
  for (; k < count; k++) {
    const i = rows[k] ?? 0;
    const g = gids[i] ?? 0;
    const next = (sums[g] ?? 0) + (values[i] ?? 0);
    if (next - next !== 0) break;
    sums[g] = next;
    counts[g] = (counts[g] ?? 0) + 1;
  }
  return k;
}

/**
 * float8_accum over rows 0 … while every state stays finite (no NULLs, grouped): returns the first
 * row it did not take. A separate function so its loop compiles on its own.
 */
function averageDense(
  sums: Float64Array,
  counts: Float64Array,
  squares: Float64Array,
  values: Float64Array,
  gids: Int32Array,
  rows: Uint16Array,
  count: number,
): number {
  let k = 0;
  for (; k < count; k++) {
    const i = rows[k] ?? 0;
    const g = gids[i] ?? 0;
    const v = values[i] ?? 0;
    const before = counts[g] ?? 0;
    const n = before + 1;
    const sum = (sums[g] ?? 0) + v;
    const t = v * n - sum;
    const sxx = before > 0 ? (squares[g] ?? 0) + (t * t) / (n * before) : 0;
    if (sum - sum !== 0 || sxx - sxx !== 0) break;
    sums[g] = sum;
    counts[g] = n;
    squares[g] = sxx;
  }
  return k;
}

class FloatSumAccumulator implements Accumulator {
  readonly #memory: Charged;
  #sums: Float64Array = new Float64Array(0);
  #counts: Float64Array = new Float64Array(0);
  /**
   * AVG only: the sum of squared deviations (float8_accum's Sxx, Youngs–Cramer). AVG does not
   * output it, but PostgreSQL raises 22003 when it overflows from finite inputs.
   */
  #squares: Float64Array = new Float64Array(0);
  constructor(
    readonly type: PhysicalType,
    charge: ByteCharge,
    readonly average: boolean,
  ) {
    this.#memory = new Charged(charge);
  }
  grow(groups: number): void {
    if (groups <= this.#sums.length) return;
    const capacity = Math.max(groups, 2 * this.#sums.length);
    const before = this.#sums.length;
    this.#sums = this.#memory.float64(this.#sums, capacity);
    // SUM starts from −0, the additive identity (PostgreSQL's first value is the state, so
    // SUM(−0) is −0); AVG's sum starts from 0 (float8_accum's initial {0,0,0}).
    if (!this.average) this.#sums.fill(-0, before);
    this.#counts = this.#memory.float64(this.#counts, capacity);
    if (this.average) this.#squares = this.#memory.float64(this.#squares, capacity);
  }
  update(
    vector: Vector | null,
    gids: Int32Array,
    rows: Uint16Array,
    count: number,
    scalar: boolean,
  ): void {
    if (vector === null) return;
    const reader = readerOf(vector);
    if (reader.empty) return;
    const values = rowValues(
      (reader.flat.data as { values: Float64Array | Float32Array }).values,
      reader,
      vector.length,
    );
    const validity = reader.validity;
    const sums = this.#sums;
    const counts = this.#counts;
    if (this.average) {
      this.#accumulate(values, validity, gids, rows, count, scalar);
      return;
    }
    // SUM(float4) adds in float4; SUM(float8) in float8.
    const float32 = this.type.kind === "float32";
    // Fast loops stop at the first non-finite sum (a predictable branch); the rest of the batch
    // takes the exact per-row rule.
    let k = 0;
    if (!float32 && validity === null) {
      if (scalar) {
        let sum = sums[0] ?? 0;
        for (; k < count; k++) {
          const next = sum + (values[rows[k] ?? 0] ?? 0);
          if (next - next !== 0) break;
          sum = next;
        }
        sums[0] = sum;
        counts[0] = (counts[0] ?? 0) + k;
      } else k = sumDense(sums, counts, values, gids, rows, count);
    }
    if (k < count) this.#exact(values, validity, gids, rows, k, count, scalar, float32);
  }
  /**
   * AVG(float4 / float8): PostgreSQL's float8_accum, in float8. N += 1, Sx += v, and from the
   * second value Sxx += (v·N − Sx)² / (N·(N − 1)); an infinite Sx or Sxx is 22003 when the old Sx
   * and v are finite, and otherwise makes Sxx NaN; a first value that is NaN or ±∞ makes Sxx NaN.
   */
  #accumulate(
    values: Float64Array,
    validity: Validity | null,
    gids: Int32Array,
    rows: Uint16Array,
    count: number,
    scalar: boolean,
  ): void {
    const sums = this.#sums;
    const counts = this.#counts;
    const squares = this.#squares;
    // The common case, tight; it stops at the first non-finite state for the exact loop below.
    let k =
      !scalar && validity === null
        ? averageDense(sums, counts, squares, values, gids, rows, count)
        : 0;
    for (; k < count; k++) {
      const i = rows[k] ?? 0;
      if (validity !== null && ((validity[i >>> 5] ?? 0) & (1 << (i & 31))) === 0) continue;
      const g = scalar ? 0 : (gids[i] ?? 0);
      const v = values[i] ?? 0;
      const before = counts[g] ?? 0;
      const n = before + 1;
      const old = sums[g] ?? 0;
      const sum = old + v;
      let sxx = squares[g] ?? 0;
      if (before > 0) {
        const t = v * n - sum;
        sxx += (t * t) / (n * before);
        // Rare (a non-finite state), so the branch predicts.
        if (sum - sum !== 0 || sxx - sxx !== 0) {
          if (sum === Infinity || sum === -Infinity || sxx === Infinity || sxx === -Infinity) {
            if (old !== Infinity && old !== -Infinity && v !== Infinity && v !== -Infinity)
              throw new NumericOutOfRangeError("value out of range: overflow");
            sxx = NaN;
          }
        }
      } else if (v - v !== 0) sxx = NaN;
      sums[g] = sum;
      counts[g] = n;
      squares[g] = sxx;
    }
  }
  /**
   * Rows from … count − 1, one at a time with PostgreSQL's float8pl / float4pl rule: an infinite
   * sum of two finite operands is 22003; NaN and infinite inputs propagate.
   */
  #exact(
    values: Float64Array,
    validity: Validity | null,
    gids: Int32Array,
    rows: Uint16Array,
    from: number,
    count: number,
    scalar: boolean,
    float32: boolean,
  ): void {
    const sums = this.#sums;
    const counts = this.#counts;
    for (let k = from; k < count; k++) {
      const i = rows[k] ?? 0;
      if (!validAt(validity, i)) continue;
      const g = scalar ? 0 : (gids[i] ?? 0);
      const old = sums[g] ?? 0;
      const v = values[i] ?? 0;
      const sum = float32 ? Math.fround(old + v) : old + v;
      if (
        (sum === Infinity || sum === -Infinity) &&
        old !== Infinity &&
        old !== -Infinity &&
        v !== Infinity &&
        v !== -Infinity
      )
        throw new NumericOutOfRangeError("value out of range: overflow");
      sums[g] = sum;
      counts[g] = (counts[g] ?? 0) + 1;
    }
  }
  outputBytes(_from: number, count: number): number {
    return validityBytes(count) + 8 * count;
  }
  emit(from: number, count: number): FlatVector {
    const present = new Uint8Array(count);
    for (let k = 0; k < count; k++) present[k] = (this.#counts[from + k] ?? 0) > 0 ? 1 : 0;
    const validity = validityFrom(present, 0, count);
    if (this.type.kind === "float32") {
      const out = Float32Array.from(this.#sums.subarray(from, from + count));
      return {
        form: "flat",
        type: this.type,
        length: count,
        validity,
        data: { layout: "f32", values: out },
      };
    }
    const out = this.#sums.slice(from, from + count);
    if (this.average)
      for (let k = 0; k < count; k++) out[k] = (out[k] ?? 0) / (this.#counts[from + k] ?? 1);
    return {
      form: "flat",
      type: this.type,
      length: count,
      validity,
      data: { layout: "f64", values: out },
    };
  }
  close(): void {
    this.#memory.close();
  }
}

// ---------------------------------------------------------------------------------------------
// NUMERIC sums and averages

const HAS = 1;
const NAN = 2;
const POS = 4;
const NEG = 8;
/** Scale of the whole state: not yet known, or not uniform. */
const SCALE_NONE = -2;
const SCALE_MIXED = -1;

class DecimalSumAccumulator implements Accumulator {
  readonly #memory: Charged;
  readonly #sums: ExactSums;
  #scales: Uint16Array = new Uint16Array(0);
  #flags: Uint8Array = new Uint8Array(0);
  #counts: Float64Array = new Float64Array(0);
  /** The scale every group with a value has, or SCALE_NONE / SCALE_MIXED. */
  #scale = SCALE_NONE;
  /** |coefficient| < 10^precision for every input value (the type's declared precision), or null. */
  readonly #precision: number | null;
  constructor(
    readonly type: PhysicalType,
    inputType: PhysicalType,
    charge: ByteCharge,
    readonly average: boolean,
  ) {
    this.#memory = new Charged(charge);
    this.#sums = new ExactSums(charge);
    this.#precision = inputType.kind === "decimal" ? inputType.precision : null;
  }
  grow(groups: number): void {
    this.#sums.grow(groups);
    if (groups <= this.#flags.length) return;
    const capacity = Math.max(groups, 2 * this.#flags.length);
    this.#scales = this.#memory.uint16(this.#scales, capacity);
    this.#flags = this.#memory.uint8(this.#flags, capacity);
    this.#counts = this.#memory.float64(this.#counts, capacity);
  }
  update(
    vector: Vector | null,
    gids: Int32Array,
    rows: Uint16Array,
    count: number,
    scalar: boolean,
  ): void {
    if (vector === null) return;
    const reader = readerOf(vector);
    if (reader.empty) return;
    const data = reader.flat.data as DecimalData;
    const uniform = data.scale;
    if (
      uniform !== null &&
      data.specials === null &&
      (this.#scale === SCALE_NONE || this.#scale === uniform)
    ) {
      if (this.#uniformBatch(reader, data, uniform, gids, rows, count, scalar)) return;
    }
    this.#rowByRow(reader, data, gids, rows, count, scalar);
  }
  /** Every row has the state's scale: add coefficients directly. False if it does not apply. */
  #uniformBatch(
    reader: Reader,
    data: DecimalData,
    scale: number,
    gids: Int32Array,
    rows: Uint16Array,
    count: number,
    scalar: boolean,
  ): boolean {
    const coefficients = data.coefficients;
    if (coefficients.repr === "wide") return false;
    const sums = this.#sums;
    const flags = this.#flags;
    const counts = this.#counts;
    const validity = reader.validity;
    this.#scale = scale;
    if (coefficients.repr === "safe") {
      const values = coefficients.values;
      const precision = this.#precision;
      if (
        scalar &&
        reader.codes === null &&
        reader.stride === 1 &&
        precision !== null &&
        precision <= 12
      ) {
        // The fused SUM: |c| < 10^12, so a batch of ≤ 2,048 sums below 2^51 exactly.
        let s0 = 0;
        let s1 = 0;
        let s2 = 0;
        let s3 = 0;
        let n = count;
        if (validity === null && rows === IDENTITY) {
          // Dense: every row, no indirection.
          let k = 0;
          for (; k + 3 < count; k += 4) {
            s0 += values[k] ?? 0;
            s1 += values[k + 1] ?? 0;
            s2 += values[k + 2] ?? 0;
            s3 += values[k + 3] ?? 0;
          }
          for (; k < count; k++) s0 += values[k] ?? 0;
        } else if (validity === null) {
          let k = 0;
          for (; k + 3 < count; k += 4) {
            s0 += values[rows[k] ?? 0] ?? 0;
            s1 += values[rows[k + 1] ?? 0] ?? 0;
            s2 += values[rows[k + 2] ?? 0] ?? 0;
            s3 += values[rows[k + 3] ?? 0] ?? 0;
          }
          for (; k < count; k++) s0 += values[rows[k] ?? 0] ?? 0;
        } else {
          n = 0;
          for (let k = 0; k < count; k++) {
            const i = rows[k] ?? 0;
            const bit = ((validity[i >>> 5] ?? 0) >>> (i & 31)) & 1;
            s0 += (values[i] ?? 0) * bit;
            n += bit;
          }
        }
        if (n > 0) {
          flags[0] = (flags[0] ?? 0) | HAS;
          sums.has[0] = 1;
          counts[0] = (counts[0] ?? 0) + n;
          sums.add(0, s0 + s1 + (s2 + s3));
        }
        return true;
      }
      if (!scalar && reader.codes === null && reader.stride === 1) {
        addFlat(sums, values, validity, gids, rows, count);
        for (let k = 0; k < count; k++) {
          const i = rows[k] ?? 0;
          if (!validAt(validity, i)) continue;
          const g = gids[i] ?? 0;
          flags[g] = (flags[g] ?? 0) | HAS;
          counts[g] = (counts[g] ?? 0) + 1;
        }
        return true;
      }
      for (let k = 0; k < count; k++) {
        const i = rows[k] ?? 0;
        if (!validAt(validity, i)) continue;
        const g = scalar ? 0 : (gids[i] ?? 0);
        flags[g] = (flags[g] ?? 0) | HAS;
        sums.has[g] = 1;
        counts[g] = (counts[g] ?? 0) + 1;
        sums.add(g, values[slotAt(reader, i)] ?? 0);
      }
      return true;
    }
    // int64 / int128 tiers: straight into the carry limbs.
    const words = coefficients.repr === "int64" ? int64Words(coefficients.values) : null;
    for (let k = 0; k < count; k++) {
      const i = rows[k] ?? 0;
      if (!validAt(validity, i)) continue;
      const g = scalar ? 0 : (gids[i] ?? 0);
      flags[g] = (flags[g] ?? 0) | HAS;
      sums.has[g] = 1;
      counts[g] = (counts[g] ?? 0) + 1;
      const slot = slotAt(reader, i);
      if (words !== null) sums.spillWords(g, words, slot);
      else if (coefficients.repr === "int128") sums.addCarry(g, coefficients.limbs, slot);
    }
    return true;
  }
  /** Any scales, specials and tiers: align each value to its group's scale. */
  #rowByRow(
    reader: Reader,
    data: DecimalData,
    gids: Int32Array,
    rows: Uint16Array,
    count: number,
    scalar: boolean,
  ): void {
    const sums = this.#sums;
    const flags = this.#flags;
    const counts = this.#counts;
    const scales = this.#scales;
    const validity = reader.validity;
    if (this.#scale >= 0) {
      // Groups so far share one scale; per-group scales start from it.
      for (let g = 0; g < this.#flags.length; g++) scales[g] = this.#scale;
    }
    this.#scale = SCALE_MIXED;
    for (let k = 0; k < count; k++) {
      const i = rows[k] ?? 0;
      if (!validAt(validity, i)) continue;
      const g = scalar ? 0 : (gids[i] ?? 0);
      const slot = slotAt(reader, i);
      counts[g] = (counts[g] ?? 0) + 1;
      const special = specialAt(data, slot);
      if (special !== 0) {
        flags[g] = (flags[g] ?? 0) | HAS | (special === 1 ? NAN : special === 2 ? POS : NEG);
        continue;
      }
      const s = scaleAt(data, slot);
      if ((sums.has[g] ?? 0) === 0) {
        scales[g] = s;
        sums.has[g] = 1;
      } else if (s > (scales[g] ?? 0)) this.#rescale(g, s);
      flags[g] = (flags[g] ?? 0) | HAS;
      const shift = (scales[g] ?? 0) - s;
      const c = data.coefficients;
      if (c.repr === "safe" && shift <= 15) {
        const v = (c.values[slot] ?? 0) * (POW10[shift] ?? 1);
        if (v <= MAX_SAFE && v >= -MAX_SAFE) {
          sums.add(g, v);
          continue;
        }
      }
      // Exact: the coefficient as limbs, aligned, into the carry; past 128 bits as a BigInt.
      if (c.repr === "wide") {
        sums.addBig(g, (c.values[slot] ?? 0n) * 10n ** BigInt(shift));
        continue;
      }
      if (c.repr === "safe") setNumber(SCRATCH, 1, c.values[slot] ?? 0);
      else if (c.repr === "int64") setInt64(SCRATCH, 1, int64Words(c.values), slot);
      else SCRATCH.set(c.limbs.subarray(slot * 4, slot * 4 + 4), 4);
      if (scaleUp(SCRATCH, 1, shift)) sums.addCarry(g, SCRATCH, 1);
      else sums.addBig(g, coefficientAt(data, slot) * 10n ** BigInt(shift));
    }
  }
  /** Raises group g's scale to `scale`, multiplying its sum exactly. */
  #rescale(g: number, scale: number): void {
    const sums = this.#sums;
    const d = scale - (this.#scales[g] ?? 0);
    if (sums.total(g, SCRATCH, 2) && scaleUp(SCRATCH, 2, d)) {
      sums.sums[g] = 0;
      sums.carry.set(SCRATCH.subarray(8, 12), g * 4);
      sums.carried += 1;
      if (sums.wide !== null) sums.wide[g] = 0n;
    } else sums.set(g, sums.big(g) * 10n ** BigInt(d));
    this.#scales[g] = scale;
  }
  outputBytes(from: number, count: number): number {
    // Scales, specials, presence and limbs per group; then the BigInts the output holds: every
    // AVG result (the sum's digits plus its division scale, at most ~20 more digits: 24 bytes),
    // or a SUM past 128 bits. Sizes come from what each group's state charged (`bigintBytes`).
    const fixed = validityBytes(count) + count * (16 + 2 + 1 + 1);
    if (!this.average) return fixed + this.#sums.wideOutputBytes(from, count);
    let bytes = fixed;
    for (let k = 0; k < count; k++)
      bytes += Math.max(this.#sums.offsetBytes(from + k), 48) + 24 + 8;
    return bytes;
  }
  emit(from: number, count: number): FlatVector {
    const sums = this.#sums;
    const flags = this.#flags;
    const present = new Uint8Array(count);
    const specials = new Uint8Array(count);
    let anySpecial = false;
    for (let k = 0; k < count; k++) {
      const f = flags[from + k] ?? 0;
      present[k] = f & HAS ? 1 : 0;
      const special = f & NAN || (f & POS && f & NEG) ? 1 : f & POS ? 2 : f & NEG ? 3 : 0;
      specials[k] = special;
      if (special !== 0) anySpecial = true;
    }
    const validity = validityFrom(present, 0, count);
    const uniform = this.#scale >= 0 ? this.#scale : null;
    const scales = uniform === null ? this.#scales.slice(from, from + count) : null;
    if (this.average) return this.#average(from, count, validity, specials);
    let coefficients: DecimalData["coefficients"];
    if (sums.carried === 0 && sums.wide === null)
      coefficients = { repr: "safe", values: sums.sums.slice(from, from + count) };
    else {
      const limbs = new Uint32Array(4 * count);
      let fits = true;
      let wide = false;
      for (let k = 0; k < count; k++) {
        if (!sums.total(from + k, limbs, k)) wide = true;
        else if (!fitsInt64(limbs, k)) fits = false;
      }
      if (wide)
        coefficients = {
          repr: "wide",
          values: Array.from({ length: count }, (_, k) =>
            checkNumericRange(sums.big(from + k), uniform ?? this.#scales[from + k] ?? 0),
          ),
        };
      else if (fits) {
        const big = new BigInt64Array(count);
        const words = int64Words(big);
        for (let k = 0; k < count; k++) storeInt64(limbs, k, words, k);
        coefficients = { repr: "int64", values: big };
      } else coefficients = { repr: "int128", limbs };
    }
    return {
      form: "flat",
      type: this.type,
      length: count,
      validity,
      data: {
        layout: "decimal",
        scale: uniform,
        scales,
        coefficients,
        specials: anySpecial ? specials : null,
      },
    };
  }
  /** AVG(numeric): sum / count with PostgreSQL's division scale, per group. */
  /**
   * Computes AVG results of groups from `from` on into a cache, one group at a time, until
   * `count` are ready or `expired()` says the time slice is over (checked after every group whose
   * sum is past 128 bits, every 256 otherwise; at least one group per call). Returns how many
   * groups from `from` are ready: the operator emits those and resumes the rest after a yield.
   */
  prepare(from: number, count: number, expired: () => boolean): number {
    if (!this.average) return count;
    const cache = this.#prepared;
    if (cache.from !== from) {
      cache.from = from;
      cache.coefficients.length = 0;
      cache.scales.length = 0;
    }
    const uniform = this.#scale >= 0 ? this.#scale : null;
    const sums = this.#sums;
    let since = 0;
    while (cache.coefficients.length < count) {
      const g = from + cache.coefficients.length;
      const f = this.#flags[g] ?? 0;
      let coefficient = 0n;
      let scale = 0;
      const special = f & NAN || f & POS || f & NEG;
      if (!special && (f & HAS) !== 0) {
        // Per group, once: BigInt here is O(groups), never O(rows). PostgreSQL range-checks the
        // sum itself before dividing (AVG(x, x) of the largest x is 22003, verified in PGlite).
        const sumScale = uniform ?? this.#scales[g] ?? 0;
        const sum = checkNumericRange(sums.big(g), sumScale);
        const outcome = numericDivide(0, sum, sumScale, 0, BigInt(this.#counts[g] ?? 1), 0);
        coefficient = checkNumericRange(outcome.coefficient, outcome.scale);
        scale = outcome.scale;
      }
      cache.coefficients.push(coefficient);
      cache.scales.push(scale);
      since += 1;
      if ((sums.offsetBytes(g) > 0 || since >= 256) && expired()) break;
      if (since >= 256) since = 0;
    }
    return cache.coefficients.length;
  }

  /** AVG results prepared for groups `from …` (see `prepare`). */
  readonly #prepared: { from: number; coefficients: bigint[]; scales: number[] } = {
    from: -1,
    coefficients: [],
    scales: [],
  };

  #average(
    from: number,
    count: number,
    validity: Validity | null,
    specials: Uint8Array,
  ): FlatVector {
    if (this.prepare(from, count, () => false) < count)
      throw new RangeError("AVG results were not prepared");
    const cache = this.#prepared;
    const coefficients = cache.coefficients.slice(0, count);
    const scales = Uint16Array.from(cache.scales.slice(0, count));
    // The cache moves on to the groups after this batch.
    cache.from = from + count;
    cache.coefficients.splice(0, count);
    cache.scales.splice(0, count);
    let anySpecial = false;
    for (let k = 0; k < count; k++) if (specials[k] !== 0) anySpecial = true;
    return {
      form: "flat",
      type: this.type,
      length: count,
      validity,
      data: {
        layout: "decimal",
        scale: null,
        scales,
        coefficients: { repr: "wide", values: coefficients },
        specials: anySpecial ? specials : null,
      },
    };
  }
  close(): void {
    this.#memory.close();
    this.#sums.close();
  }
}

// ---------------------------------------------------------------------------------------------
// MIN / MAX

/** −1, 0, 1 for a − b, two signed 64-bit values as (high signed, low unsigned) words. */
function compareWords(ah: number, al: number, bh: number, bl: number): number {
  if (ah !== bh) return ah < bh ? -1 : 1;
  return al === bl ? 0 : al < bl ? -1 : 1;
}

const TWO32 = 4294967296;

/**
 * −1, 0, 1 for stored entry g against slot `slot` of `flat` (same type). NaN is greatest. int64
 * compares 32-bit words and NUMERIC aligns scales in 128-bit limbs: no BigInt per row, except for
 * a coefficient past 128 bits or an alignment that overflows 128 bits.
 */
function compareToStored(values: GroupValues, g: number, flat: FlatVector, slot: number): number {
  const data = flat.data;
  switch (data.layout) {
    case "bool":
    case "i32":
    case "f32":
    case "f64": {
      const a = values.number(g);
      const b = data.values[slot] ?? 0;
      if (a !== a) return b !== b ? 0 : 1;
      if (b !== b) return -1;
      return a < b ? -1 : a > b ? 1 : 0;
    }
    case "int64": {
      let bh: number;
      let bl: number;
      if (data.values.repr === "safe") {
        const v = data.values.values[slot] ?? 0;
        bh = Math.floor(v / TWO32);
        bl = v - bh * TWO32;
      } else {
        const words = int64Words(data.values.values);
        bl = words[slot * 2] ?? 0;
        bh = (words[slot * 2 + 1] ?? 0) | 0;
      }
      return compareWords(values.high(g), values.low(g), bh, bl);
    }
    case "decimal": {
      const rank = (special: number) =>
        special === 3 ? -1 : special === 0 ? 0 : special === 2 ? 1 : 2;
      const ra = rank(values.special(g));
      const rb = rank(specialAt(data, slot));
      if (ra !== rb || ra !== 0) return ra < rb ? -1 : ra > rb ? 1 : 0;
      const sa = values.scale(g);
      const sb = scaleAt(data, slot);
      const c = data.coefficients;
      if (sa === sb && values.isSafe(g) && c.repr === "safe") {
        const a = values.number(g);
        const b = c.values[slot] ?? 0;
        return a < b ? -1 : a > b ? 1 : 0;
      }
      if (!values.isHuge(g) && c.repr !== "wide") {
        values.loadCoefficient(g, SCRATCH, 6);
        if (c.repr === "safe") setNumber(SCRATCH, 7, c.values[slot] ?? 0);
        else if (c.repr === "int64") setInt64(SCRATCH, 7, int64Words(c.values), slot);
        else SCRATCH.set(c.limbs.subarray(slot * 4, slot * 4 + 4), 28);
        if (scaleUp(SCRATCH, 6, Math.max(0, sb - sa)) && scaleUp(SCRATCH, 7, Math.max(0, sa - sb)))
          return compareLimbs(SCRATCH, 6, SCRATCH, 7);
      }
      let a = values.big(g);
      let b = coefficientAt(data, slot);
      if (sa < sb) a *= 10n ** BigInt(sb - sa);
      else if (sb < sa) b *= 10n ** BigInt(sa - sb);
      return a < b ? -1 : a > b ? 1 : 0;
    }
    case "varlen":
      return compareBytes(
        values.byteArena,
        values.start(g),
        values.end(g),
        data.bytes,
        data.offsets[slot] ?? 0,
        data.offsets[slot + 1] ?? 0,
      );
    case "fixed16":
      return compareBytes(
        values.byteArena,
        values.start(g),
        values.end(g),
        data.bytes,
        slot * 16,
        slot * 16 + 16,
      );
    case "interval":
      throw new UnsupportedPlanError("MIN/MAX over intervals", { construct: "aggregate" });
  }
}

class MinMaxAccumulator implements Accumulator {
  readonly #values: GroupValues;
  readonly #memory: Charged;
  #has: Uint8Array = new Uint8Array(0);
  constructor(
    readonly type: PhysicalType,
    readonly max: boolean,
    charge: ByteCharge,
  ) {
    if (
      type.kind === "enum" ||
      type.kind === "json" ||
      type.kind === "array" ||
      type.kind === "interval"
    )
      throw new UnsupportedPlanError(`MIN/MAX over ${type.kind}`, { construct: "aggregate" });
    this.#values = new GroupValues(type, charge);
    this.#memory = new Charged(charge);
  }
  grow(groups: number): void {
    if (groups > this.#has.length)
      this.#has = this.#memory.uint8(this.#has, Math.max(groups, 2 * this.#has.length));
    const nothing: Vector = {
      form: "constant",
      type: this.type,
      length: 1,
      value: NULL_VALUES.get(this.type),
    };
    while (this.#values.length < groups) this.#values.append(nothing, 0);
  }
  update(
    vector: Vector | null,
    gids: Int32Array,
    rows: Uint16Array,
    count: number,
    scalar: boolean,
  ): void {
    if (vector === null) return;
    const reader = readerOf(vector);
    if (reader.empty) return;
    const values = this.#values;
    const has = this.#has;
    const better = this.max ? 1 : -1;
    for (let k = 0; k < count; k++) {
      const i = rows[k] ?? 0;
      if (!validAt(reader.validity, i)) continue;
      const g = scalar ? 0 : (gids[i] ?? 0);
      const slot = slotAt(reader, i);
      if (has[g] === 0) {
        has[g] = 1;
        values.set(g, vector, i);
      } else if (compareToStored(values, g, reader.flat, slot) !== better)
        // Ties take the new value, as PostgreSQL's float8smaller / numeric_larger… do
        // (MIN(1.5, 1.50) is 1.50; MIN(0, −0) is −0).
        values.set(g, vector, i);
    }
  }
  outputBytes(from: number, count: number): number {
    return this.#values.outputBytes(null, from, count);
  }
  emit(from: number, count: number): FlatVector {
    return this.#values.emit(from, count);
  }
  close(): void {
    this.#values.close();
    this.#memory.close();
  }
}

/** Whether a is greater than b in PostgreSQL's float order (NaN greatest, NaN = NaN). */
function greater(a: number, b: number): boolean {
  return a !== a ? b === b : a > b;
}

/** MAX of grouped rows without NULLs: the stored value stays only when strictly greater. */
function maxDense(
  best: Float64Array,
  has: Uint8Array,
  values: Float64Array,
  gids: Int32Array,
  rows: Uint16Array,
  count: number,
): void {
  for (let k = 0; k < count; k++) {
    const i = rows[k] ?? 0;
    const g = gids[i] ?? 0;
    const v = values[i] ?? 0;
    if (has[g] === 0 || !greater(best[g] ?? 0, v)) {
      best[g] = v;
      has[g] = 1;
    }
  }
}

/** MIN of grouped rows without NULLs: the stored value stays only when strictly less. */
function minDense(
  best: Float64Array,
  has: Uint8Array,
  values: Float64Array,
  gids: Int32Array,
  rows: Uint16Array,
  count: number,
): void {
  for (let k = 0; k < count; k++) {
    const i = rows[k] ?? 0;
    const g = gids[i] ?? 0;
    const v = values[i] ?? 0;
    if (has[g] === 0 || !greater(v, best[g] ?? 0)) {
      best[g] = v;
      has[g] = 1;
    }
  }
}

/**
 * MIN / MAX over int32, float32 and float64 layouts: the best value per group in one Float64Array
 * (exact for all three), no GroupValues indirection. Ties take the new value (−0 after 0).
 */
class NumberMinMaxAccumulator implements Accumulator {
  readonly #memory: Charged;
  #best: Float64Array = new Float64Array(0);
  #has: Uint8Array = new Uint8Array(0);
  constructor(
    readonly type: PhysicalType,
    readonly max: boolean,
    charge: ByteCharge,
  ) {
    this.#memory = new Charged(charge);
  }
  grow(groups: number): void {
    if (groups <= this.#best.length) return;
    const capacity = Math.max(groups, 2 * this.#best.length);
    this.#best = this.#memory.float64(this.#best, capacity);
    this.#has = this.#memory.uint8(this.#has, capacity);
  }
  update(
    vector: Vector | null,
    gids: Int32Array,
    rows: Uint16Array,
    count: number,
    scalar: boolean,
  ): void {
    if (vector === null) return;
    const reader = readerOf(vector);
    if (reader.empty) return;
    const values = rowValues(
      (reader.flat.data as { values: Float64Array | Float32Array | Int32Array }).values,
      reader,
      vector.length,
    );
    const validity = reader.validity;
    const best = this.#best;
    const has = this.#has;
    const max = this.max;
    if (!scalar && validity === null) {
      if (max) maxDense(best, has, values, gids, rows, count);
      else minDense(best, has, values, gids, rows, count);
      return;
    }
    for (let k = 0; k < count; k++) {
      const i = rows[k] ?? 0;
      if (validity !== null && ((validity[i >>> 5] ?? 0) & (1 << (i & 31))) === 0) continue;
      const g = scalar ? 0 : (gids[i] ?? 0);
      const v = values[i] ?? 0;
      const stored = best[g] ?? 0;
      // Keep the stored value only when it is strictly better.
      if (has[g] === 0 || !(max ? greater(stored, v) : greater(v, stored))) {
        best[g] = v;
        has[g] = 1;
      }
    }
  }
  outputBytes(_from: number, count: number): number {
    return validityBytes(count) + 8 * count;
  }
  emit(from: number, count: number): FlatVector {
    const validity = validityFrom(this.#has, from, count);
    const best = this.#best.subarray(from, from + count);
    const layout = layoutOf(this.type);
    const data: FlatVector["data"] =
      layout === "i32"
        ? { layout, values: Int32Array.from(best) }
        : layout === "f32"
          ? { layout, values: Float32Array.from(best) }
          : { layout: "f64", values: best.slice() };
    return { form: "flat", type: this.type, length: count, validity, data };
  }
  close(): void {
    this.#memory.close();
  }
}

/** A length-1 NULL flat vector per type (MIN/MAX's empty group entries). */
const NULL_VALUES = {
  cache: new Map<string, FlatVector>(),
  get(type: PhysicalType): FlatVector {
    const key = JSON.stringify(type);
    let vector = this.cache.get(key);
    if (vector === undefined) {
      const layout = layoutOf(type);
      vector = {
        form: "flat",
        type,
        length: 1,
        validity: new Uint32Array(1),
        data:
          layout === "varlen"
            ? { layout, bytes: new Uint8Array(0), offsets: new Int32Array(2) }
            : layout === "fixed16"
              ? { layout, bytes: new Uint8Array(16) }
              : layout === "decimal"
                ? {
                    layout,
                    scale: 0,
                    scales: null,
                    coefficients: { repr: "safe", values: new Float64Array(1) },
                    specials: null,
                  }
                : layout === "int64"
                  ? { layout, values: { repr: "safe", values: new Float64Array(1) } }
                  : layout === "interval"
                    ? {
                        layout,
                        months: new Int32Array(1),
                        days: new Int32Array(1),
                        micros: { repr: "safe", values: new Float64Array(1) },
                      }
                    : layout === "bool"
                      ? { layout, values: new Uint8Array(1) }
                      : layout === "i32"
                        ? { layout, values: new Int32Array(1) }
                        : layout === "f32"
                          ? { layout, values: new Float32Array(1) }
                          : { layout: "f64", values: new Float64Array(1) },
      };
      this.cache.set(key, vector);
    }
    return vector;
  },
};

// ---------------------------------------------------------------------------------------------
// BOOL_AND / BOOL_OR, single value

class BoolAccumulator implements Accumulator {
  readonly #memory: Charged;
  /** 0 no value, 1 false, 2 true. */
  #state: Uint8Array = new Uint8Array(0);
  constructor(
    readonly type: PhysicalType,
    readonly and: boolean,
    charge: ByteCharge,
  ) {
    this.#memory = new Charged(charge);
  }
  grow(groups: number): void {
    if (groups > this.#state.length)
      this.#state = this.#memory.uint8(this.#state, Math.max(groups, 2 * this.#state.length));
  }
  update(
    vector: Vector | null,
    gids: Int32Array,
    rows: Uint16Array,
    count: number,
    scalar: boolean,
  ): void {
    if (vector === null) return;
    const reader = readerOf(vector);
    if (reader.empty) return;
    const values = (reader.flat.data as { values: Uint8Array }).values;
    const state = this.#state;
    for (let k = 0; k < count; k++) {
      const i = rows[k] ?? 0;
      if (!validAt(reader.validity, i)) continue;
      const g = scalar ? 0 : (gids[i] ?? 0);
      const v = (values[slotAt(reader, i)] ?? 0) === 1 ? 2 : 1;
      const s = state[g] ?? 0;
      state[g] = s === 0 ? v : this.and ? Math.min(s, v) : Math.max(s, v);
    }
  }
  outputBytes(_from: number, count: number): number {
    return validityBytes(count) + 2 * count;
  }
  emit(from: number, count: number): FlatVector {
    const present = new Uint8Array(count);
    const out = new Uint8Array(count);
    for (let k = 0; k < count; k++) {
      const s = this.#state[from + k] ?? 0;
      present[k] = s === 0 ? 0 : 1;
      out[k] = s === 2 ? 1 : 0;
    }
    return {
      form: "flat",
      type: this.type,
      length: count,
      validity: validityFrom(present, 0, count),
      data: { layout: "bool", values: out },
    };
  }
  close(): void {
    this.#memory.close();
  }
}

/** The front end's MINNOW_SINGLE_VALUE: a scalar subquery's one value, 21000 on a second row. */
class SingleValueAccumulator implements Accumulator {
  readonly #values: GroupValues;
  readonly #memory: Charged;
  #seen: Uint8Array = new Uint8Array(0);
  constructor(
    readonly type: PhysicalType,
    charge: ByteCharge,
  ) {
    this.#values = new GroupValues(type, charge);
    this.#memory = new Charged(charge);
  }
  grow(groups: number): void {
    if (groups > this.#seen.length)
      this.#seen = this.#memory.uint8(this.#seen, Math.max(groups, 2 * this.#seen.length));
    const nothing: Vector = {
      form: "constant",
      type: this.type,
      length: 1,
      value: NULL_VALUES.get(this.type),
    };
    while (this.#values.length < groups) this.#values.append(nothing, 0);
  }
  update(
    vector: Vector | null,
    gids: Int32Array,
    rows: Uint16Array,
    count: number,
    scalar: boolean,
  ): void {
    if (vector === null) return;
    for (let k = 0; k < count; k++) {
      const i = rows[k] ?? 0;
      const g = scalar ? 0 : (gids[i] ?? 0);
      if (this.#seen[g] === 1) throw new CardinalityError();
      this.#seen[g] = 1;
      this.#values.set(g, vector, i);
    }
  }
  outputBytes(from: number, count: number): number {
    return this.#values.outputBytes(null, from, count);
  }
  emit(from: number, count: number): FlatVector {
    return this.#values.emit(from, count);
  }
  close(): void {
    this.#values.close();
    this.#memory.close();
  }
}

/** The accumulator for one aggregate call over input type `input` (null for COUNT(*)). */
export function accumulatorFor(
  fn: string,
  type: PhysicalType,
  input: PhysicalType | null,
  charge: ByteCharge,
): Accumulator {
  switch (fn) {
    case "count_star":
      return new CountAccumulator(type, true, charge);
    case "count":
      return new CountAccumulator(type, false, charge);
    case "sum":
    case "avg": {
      const average = fn === "avg";
      const kind = input?.kind;
      if (kind === "int32" || kind === "int64")
        return new IntegerSumAccumulator(type, charge, average);
      if (kind === "float64" || kind === "float32")
        return new FloatSumAccumulator(type, charge, average);
      if (kind === "decimal") return new DecimalSumAccumulator(type, input, charge, average);
      throw new UnsupportedPlanError(`${fn} over ${String(kind)}`, { construct: "aggregate" });
    }
    case "min":
    case "max":
      return layoutOf(type) === "i32" || layoutOf(type) === "f32" || layoutOf(type) === "f64"
        ? new NumberMinMaxAccumulator(type, fn === "max", charge)
        : new MinMaxAccumulator(type, fn === "max", charge);
    case "bool_and":
    case "bool_or":
      return new BoolAccumulator(type, fn === "bool_and", charge);
    case "single_value":
      return new SingleValueAccumulator(type, charge);
    default:
      throw new UnsupportedPlanError(`the aggregate ${fn}`, { construct: "aggregate" });
  }
}
