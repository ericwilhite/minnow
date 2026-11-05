/**
 * Arithmetic kernels: `add`, `sub`, `mul`, `div`, `mod` (binary) and `neg` (unary) over int32
 * (SMALLINT/INTEGER, checked), int64 (exact, dual representation), float64, float32 and decimal.
 *
 * ## Semantics (PostgreSQL)
 * - Integers: overflow is 22003 (`smallint out of range`, `integer out of range`, `bigint out of
 *   range`); division truncates toward zero; `%` takes the dividend's sign; `/` or `%` by zero is
 *   22012. MIN / −1 overflows.
 * - int64: a `safe` (Float64Array) result while every value stays within ±(2^53 − 1), checked per
 *   batch; otherwise the whole batch is recomputed exactly in BigInt64Array (`int64` repr).
 * - Floats: IEEE arithmetic; an infinite result from finite inputs is 22003 `value out of range:
 *   overflow`, a zero product or quotient from non-zero inputs is `…: underflow`, and division by
 *   zero is 22012 (PostgreSQL's float8 operators). REAL computes in f64 and rounds with
 *   `Math.fround`; range checks apply to the rounded value.
 * - Decimal (`add`, `sub`, `mul`; `div`/`mod` are not compiled yet): result display scale is
 *   max(scales) for add/sub and the sum of scales for mul, per slot. Coefficients are never
 *   rescaled to a shared maximum across rows. The fast path keeps `safe` coefficients while every
 *   aligned value stays exact in f64; otherwise the batch is recomputed in BigInt into the `wide`
 *   tier. NaN and ±Infinity follow PostgreSQL (NaN absorbs; ∞ − ∞ and ∞ × 0 are NaN).
 */
import type { PhysicalType } from "../../types.js";
import { INT32_MAX, INT32_MIN, INT16_MAX, INT16_MIN, INT64_MAX, INT64_MIN } from "../../types.js";
import { MAX_SAFE } from "../../vector/int64.js";
import type {
  Batch,
  FlatVector,
  Int64Values,
  Selection,
  Validity,
  Vector,
} from "../../vector/vector.js";
import { DivisionByZeroError, NumericOutOfRangeError, UnsupportedPlanError } from "../errors.js";
import {
  SCRATCH,
  addTo,
  copy,
  fitsInt64,
  int64Words,
  multiplyTo,
  scaleUp,
  setInt64,
  setNumber,
  sign,
  storeInt64,
} from "./limbs.js";
import { ADD, DIV, MUL, SUB, floatLoop, int32Loop, zeroSeen } from "./loops.js";
import {
  POW10,
  bigAt,
  coefficientAt,
  constantIsNull,
  scaleAt,
  specialAt,
  type DecimalData,
} from "./numeric.js";
import {
  combineValidity,
  flatOf,
  slotIndex,
  copyFixedSlot,
  nullConstant,
  countIn,
  rowsIn,
  validAt,
  type Compiled,
  type OutputArena,
} from "./vectors.js";

export type ArithOp = "add" | "sub" | "mul" | "div" | "mod";
const MOD = 4;
const OPS: Record<ArithOp, number> = { add: ADD, sub: SUB, mul: MUL, div: DIV, mod: MOD };

/** A fixed-width operand read at `row * stride`, dictionaries materialized into `scratch`. */
class FixedOperand {
  readonly #arena: OutputArena;
  readonly #type: PhysicalType;
  #scratch: FlatVector | null = null;
  #wideScratch: FlatVector | null = null;
  /** The stride of the last `read`: 1 for a flat vector, 0 for a constant. */
  stride: 0 | 1 = 1;
  constructor(arena: OutputArena, type: PhysicalType) {
    this.#arena = arena;
    this.#type = type;
  }
  /** The flat vector to read for the computed rows (a dictionary is materialized). */
  read(vector: Vector, rows: Uint16Array, count: number): FlatVector {
    if (vector.form === "flat") {
      this.stride = 1;
      return vector;
    }
    if (vector.form === "constant") {
      this.stride = 0;
      return vector.value;
    }
    this.stride = 1;
    const source = vector.dictionary.data;
    // A dictionary of exact int64 values materializes into an int64 scratch (finding 7).
    const wide = source.layout === "int64" && source.values.repr === "int64";
    const scratch = wide ? this.#wide() : this.#safe();
    for (let k = 0; k < count; k++) {
      const row = rows[k] ?? 0;
      copyFixedSlot(source, vector.codes[row] ?? 0, scratch.data, row);
    }
    scratch.validity = vector.validity;
    return scratch;
  }
  #safe(): FlatVector {
    this.#scratch ??= this.#arena.flat(this.#type).vector;
    return this.#scratch;
  }
  #wide(): FlatVector {
    if (this.#wideScratch === null) {
      this.#arena.take(8 * this.#arena.capacity);
      this.#wideScratch = {
        form: "flat",
        type: this.#type,
        length: 0,
        validity: null,
        data: {
          layout: "int64",
          values: { repr: "int64", values: new BigInt64Array(this.#arena.capacity) },
        },
      };
    }
    return this.#wideScratch;
  }
}

function operandValidity(vector: Vector): Validity | null {
  return vector.form === "constant" ? null : vector.validity;
}

/** The first computed, valid row for which `bad[row]` holds, or −1. */
function firstValid(
  rows: Uint16Array,
  count: number,
  validity: Validity | null,
  bad: (row: number) => boolean,
): number {
  for (let k = 0; k < count; k++) {
    const row = rows[k] ?? 0;
    if (validAt(validity, row) && bad(row)) return row;
  }
  return -1;
}

abstract class BinaryNode implements Compiled {
  readonly #nullResult: Vector;
  constructor(
    readonly type: PhysicalType,
    readonly left: Compiled,
    readonly right: Compiled,
  ) {
    this.#nullResult = nullConstant(type);
  }
  evaluate(batch: Batch, rows: Selection | null): Vector {
    const l = this.left.evaluate(batch, rows);
    const r = this.right.evaluate(batch, rows);
    if (constantIsNull(l) || constantIsNull(r)) {
      (this.#nullResult as { length: number }).length = batch.length;
      return this.#nullResult;
    }
    const active = rowsIn(rows);
    const count = countIn(rows, batch.length);
    return this.compute(batch.length, l, r, active, count);
  }
  abstract compute(length: number, l: Vector, r: Vector, rows: Uint16Array, count: number): Vector;
}

// ------------------------------------------------------------------------------------------------
// int32 (SMALLINT, INTEGER)

class Int32Node extends BinaryNode {
  readonly #op: number;
  readonly #min: number;
  readonly #max: number;
  readonly #name: string;
  readonly #out: FlatVector;
  readonly #validity: Validity;
  readonly #a: FixedOperand;
  readonly #b: FixedOperand;
  constructor(
    op: ArithOp,
    type: PhysicalType,
    left: Compiled,
    right: Compiled,
    arena: OutputArena,
  ) {
    super(type, left, right);
    this.#op = OPS[op];
    const small = type.kind === "int32" && type.bits === 16;
    this.#min = small ? INT16_MIN : INT32_MIN;
    this.#max = small ? INT16_MAX : INT32_MAX;
    this.#name = small ? "smallint out of range" : "integer out of range";
    const output = arena.flat(type);
    this.#out = output.vector;
    this.#validity = output.validity;
    this.#a = new FixedOperand(arena, left.type);
    this.#b = new FixedOperand(arena, right.type);
  }
  compute(length: number, l: Vector, r: Vector, rows: Uint16Array, count: number): Vector {
    const lf = this.#a.read(l, rows, count);
    const as = this.#a.stride;
    const rf = this.#b.read(r, rows, count);
    const bs = this.#b.stride;
    const a = (lf.data as { values: Int32Array }).values;
    const b = (rf.data as { values: Int32Array }).values;
    const out = (this.#out.data as { values: Int32Array }).values;
    const validity = combineValidity(
      operandValidity(l),
      operandValidity(r),
      this.#validity,
      length,
    );
    const max = this.#max;
    let bad = false;
    switch (this.#op) {
      case ADD:
      case SUB:
      case MUL:
        // Exact in f64 (|result| < 2^62); one range check on the batch's largest |result|.
        bad = int32Loop(this.#op, a, as, b, bs, rows, count, out) > max;
        break;
      case DIV:
        for (let k = 0; k < count; k++) {
          const i = rows[k] ?? 0;
          const d = b[i * bs] ?? 0;
          if (d === 0) {
            bad = true;
            out[i] = 0;
            continue;
          }
          const v = Math.trunc((a[i * as] ?? 0) / d);
          out[i] = v;
          if (v > max) bad = true;
        }
        break;
      case MOD:
        for (let k = 0; k < count; k++) {
          const i = rows[k] ?? 0;
          const d = b[i * bs] ?? 0;
          if (d === 0) {
            bad = true;
            out[i] = 0;
            continue;
          }
          out[i] = (a[i * as] ?? 0) % d;
        }
        break;
    }
    if (bad) this.#raise(a, as, b, bs, rows, count, validity);
    this.#out.length = length;
    this.#out.validity = validity;
    return this.#out;
  }
  #raise(
    a: Int32Array,
    as: number,
    b: Int32Array,
    bs: number,
    rows: Uint16Array,
    count: number,
    validity: Validity | null,
  ): void {
    // The first failing row, in row order, decides the error (row-at-a-time semantics).
    const divides = this.#op === DIV || this.#op === MOD;
    for (let k = 0; k < count; k++) {
      const i = rows[k] ?? 0;
      if (!validAt(validity, i)) continue;
      const x = a[i * as] ?? 0;
      const y = b[i * bs] ?? 0;
      if (divides && y === 0) throw new DivisionByZeroError();
      const v =
        this.#op === ADD
          ? x + y
          : this.#op === SUB
            ? x - y
            : this.#op === MUL
              ? x * y
              : this.#op === DIV
                ? Math.trunc(x / y)
                : 0;
      if (v < this.#min || v > this.#max) throw new NumericOutOfRangeError(this.#name);
    }
  }
}

// ------------------------------------------------------------------------------------------------
// int64 (BIGINT): safe f64 while exact, else BigInt64Array

const INT32_LIMIT = 2 ** 31;

class Int64Node extends BinaryNode {
  readonly #op: number;
  readonly #safe: FlatVector;
  readonly #wide: FlatVector;
  readonly #validity: Validity;
  readonly #a: FixedOperand;
  readonly #b: FixedOperand;
  constructor(
    op: ArithOp,
    type: PhysicalType,
    left: Compiled,
    right: Compiled,
    arena: OutputArena,
  ) {
    super(type, left, right);
    this.#op = OPS[op];
    const safe = arena.flat(type);
    this.#safe = safe.vector;
    this.#validity = safe.validity;
    arena.take(8 * arena.capacity);
    this.#wide = {
      form: "flat",
      type,
      length: 0,
      validity: null,
      data: {
        layout: "int64",
        values: { repr: "int64", values: new BigInt64Array(arena.capacity) },
      },
    };
    this.#a = new FixedOperand(arena, left.type);
    this.#b = new FixedOperand(arena, right.type);
  }
  compute(length: number, l: Vector, r: Vector, rows: Uint16Array, count: number): Vector {
    const lf = this.#a.read(l, rows, count);
    const as = this.#a.stride;
    const rf = this.#b.read(r, rows, count);
    const bs = this.#b.stride;
    const av = (lf.data as { values: Int64Values }).values;
    const bv = (rf.data as { values: Int64Values }).values;
    const validity = combineValidity(
      operandValidity(l),
      operandValidity(r),
      this.#validity,
      length,
    );
    if (
      av.repr === "safe" &&
      bv.repr === "safe" &&
      this.#safePath(av.values, as, bv.values, bs, rows, count, validity)
    ) {
      this.#safe.length = length;
      this.#safe.validity = validity;
      return this.#safe;
    }
    this.#widePath(av, as, bv, bs, rows, count, validity);
    this.#wide.length = length;
    this.#wide.validity = validity;
    return this.#wide;
  }
  /** False when some value leaves the safe range (the caller recomputes in BigInt). */
  #safePath(
    a: Float64Array,
    as: number,
    b: Float64Array,
    bs: number,
    rows: Uint16Array,
    count: number,
    validity: Validity | null,
  ): boolean {
    const out = ((this.#safe.data as { values: Int64Values }).values as { values: Float64Array })
      .values;
    let bad = false;
    switch (this.#op) {
      case ADD:
      case SUB:
      case MUL: {
        const m = floatLoop(this.#op, a, as, b, bs, rows, count, out);
        bad = !(m <= MAX_SAFE);
        // The safe representation never holds −0 (a product like −5 × 0).
        if (this.#op === MUL && zeroSeen)
          for (let k = 0; k < count; k++) {
            const i = rows[k] ?? 0;
            out[i] = (out[i] ?? 0) + 0;
          }
        break;
      }
      case DIV:
        for (let k = 0; k < count; k++) {
          const i = rows[k] ?? 0;
          const x = a[i * as] ?? 0;
          const d = b[i * bs] ?? 0;
          // Truncated f64 division is exact while both operands are within ±2^31.
          if (
            d === 0 ||
            x >= INT32_LIMIT ||
            x <= -INT32_LIMIT ||
            d >= INT32_LIMIT ||
            d <= -INT32_LIMIT
          ) {
            bad = true;
            out[i] = 0;
            continue;
          }
          out[i] = Math.trunc(x / d) + 0;
        }
        break;
      case MOD:
        for (let k = 0; k < count; k++) {
          const i = rows[k] ?? 0;
          const d = b[i * bs] ?? 0;
          if (d === 0) {
            bad = true;
            out[i] = 0;
            continue;
          }
          out[i] = ((a[i * as] ?? 0) % d) + 0; // fmod is exact; + 0 drops −0
        }
        break;
    }
    if (!bad) return true;
    // Only valid rows count: garbage under null may leave the range harmlessly.
    return (
      firstValid(rows, count, validity, (i) => {
        const v = out[i] ?? 0;
        const d = b[i * bs] ?? 0;
        const x = a[i * as] ?? 0;
        if (this.#op === DIV)
          return d === 0 || Math.abs(x) >= INT32_LIMIT || Math.abs(d) >= INT32_LIMIT;
        if (this.#op === MOD) return d === 0;
        return v > MAX_SAFE || v < -MAX_SAFE;
      }) < 0
    );
  }
  #widePath(
    av: Int64Values,
    as: number,
    bv: Int64Values,
    bs: number,
    rows: Uint16Array,
    count: number,
    validity: Validity | null,
  ): void {
    const out = ((this.#wide.data as { values: Int64Values }).values as { values: BigInt64Array })
      .values;
    const op = this.#op;
    for (let k = 0; k < count; k++) {
      const i = rows[k] ?? 0;
      if (!validAt(validity, i)) {
        out[i] = 0n;
        continue;
      }
      const x = bigAt(av, i * as);
      const y = bigAt(bv, i * bs);
      let v: bigint;
      if (op === ADD) v = x + y;
      else if (op === SUB) v = x - y;
      else if (op === MUL) v = x * y;
      else {
        if (y === 0n) throw new DivisionByZeroError();
        v = op === DIV ? x / y : x % y; // BigInt truncates toward zero; % takes the dividend's sign
      }
      if (v > INT64_MAX || v < INT64_MIN) throw new NumericOutOfRangeError("bigint out of range");
      out[i] = v;
    }
  }
}

// ------------------------------------------------------------------------------------------------
// float64 / float32

class FloatNode extends BinaryNode {
  readonly #op: number;
  readonly #real: boolean;
  readonly #out: FlatVector;
  readonly #validity: Validity;
  readonly #a: FixedOperand;
  readonly #b: FixedOperand;
  constructor(
    op: ArithOp,
    type: PhysicalType,
    left: Compiled,
    right: Compiled,
    arena: OutputArena,
  ) {
    super(type, left, right);
    if (op === "mod") throw new TypeError("PostgreSQL has no float modulo");
    this.#op = OPS[op];
    this.#real = type.kind === "float32";
    const output = arena.flat(type);
    this.#out = output.vector;
    this.#validity = output.validity;
    this.#a = new FixedOperand(arena, left.type);
    this.#b = new FixedOperand(arena, right.type);
  }
  compute(length: number, l: Vector, r: Vector, rows: Uint16Array, count: number): Vector {
    const lf = this.#a.read(l, rows, count);
    const as = this.#a.stride;
    const rf = this.#b.read(r, rows, count);
    const bs = this.#b.stride;
    const a = (lf.data as { values: Float64Array }).values;
    const b = (rf.data as { values: Float64Array }).values;
    const out = (this.#out.data as { values: Float64Array }).values;
    const validity = combineValidity(
      operandValidity(l),
      operandValidity(r),
      this.#validity,
      length,
    );
    let m = floatLoop(this.#op, a, as, b, bs, rows, count, out);
    let zero = zeroSeen;
    if (this.#real) {
      // Range checks apply to the REAL value: a nonzero f64 product can round to zero (underflow)
      // or past the float4 maximum (overflow) only here.
      m = 0;
      let z = 0;
      for (let k = 0; k < count; k++) {
        const i = rows[k] ?? 0;
        const v = Math.fround(out[i] ?? 0);
        out[i] = v;
        m = Math.max(m, Math.abs(v));
        z |= +(v === 0);
      }
      zero = z !== 0;
    }
    // Overflow, NaN from division by zero, or an underflow candidate: recheck row by row (inputs
    // that are already NaN or infinite make this batch slower, never wrong).
    const bad = !(m < Infinity) || (zero && (this.#op === MUL || this.#op === DIV));
    if (bad) this.#raise(a, as, b, bs, rows, count, validity);
    this.#out.length = length;
    this.#out.validity = validity;
    return this.#out;
  }
  #raise(
    a: Float64Array,
    as: number,
    b: Float64Array,
    bs: number,
    rows: Uint16Array,
    count: number,
    validity: Validity | null,
  ): void {
    const op = this.#op;
    const out = (this.#out.data as { values: Float64Array }).values;
    for (let k = 0; k < count; k++) {
      const i = rows[k] ?? 0;
      if (!validAt(validity, i)) continue;
      const x = a[i * as] ?? 0;
      const y = b[i * bs] ?? 0;
      const v = out[i] ?? 0;
      // PostgreSQL's float8div / float4div: NaN / 0 is NaN, any other x / 0 is 22012.
      if (op === DIV && y === 0 && x === x) throw new DivisionByZeroError();
      const finite = Number.isFinite(x) && Number.isFinite(y);
      if ((v === Infinity || v === -Infinity) && finite)
        throw new NumericOutOfRangeError("value out of range: overflow");
      if (v === 0 && x !== 0 && (op === DIV || y !== 0) && (op === MUL || op === DIV) && finite)
        throw new NumericOutOfRangeError("value out of range: underflow");
    }
  }
}

// ------------------------------------------------------------------------------------------------
// decimal

const NAN = 1;
const POS = 2;
const NEG = 3;

/** PostgreSQL's largest NUMERIC display scale; a product past it is rounded to it. */
const NUMERIC_MAX_SCALE = 16_383;
/** Distinct (scale, scale) pairs a batch is partitioned into before falling back to per row. */
const MAX_SCALE_PARTITIONS = 32;

function decimalVector(
  type: PhysicalType,
  scale: number | null,
  scales: Uint16Array | null,
  coefficients: DecimalData["coefficients"],
  specials: Uint8Array | null,
): FlatVector {
  return {
    form: "flat",
    type,
    length: 0,
    validity: null,
    data: { layout: "decimal", scale, scales, coefficients, specials },
  };
}

/**
 * Decimal add, sub and mul over every coefficient tier:
 * 1. safe f64 while every aligned value and result stays within ±(2^53 − 1);
 * 2. otherwise 128-bit limbs (limbs.ts), rows grouped by their (scale, scale) pair so each group
 *    aligns with one constant power of ten; the result is the int64 tier when every value fits,
 *    else int128. No BigInt is created per value;
 * 3. only past 128 bits (or for `wide` inputs, or a product past the maximum display scale) the
 *    batch is recomputed exactly in BigInt into the `wide` tier.
 */
class DecimalNode extends BinaryNode {
  readonly #op: number;
  readonly #scales: Uint16Array;
  readonly #safeCoefficients: Float64Array;
  readonly #limbs: Uint32Array;
  readonly #int64: BigInt64Array;
  readonly #int64Words: Uint32Array;
  readonly #wideValues: bigint[];
  readonly #specials: Uint8Array;
  readonly #validity: Validity;
  readonly #perSlot: FlatVector;
  readonly #uniformVectors = new Map<number, FlatVector>();
  /** int64 / int128 / wide outputs, without and with specials. */
  readonly #tiers: readonly FlatVector[];
  /** Row order grouped by scale pair, and the pairs. */
  readonly #order: Uint16Array;
  readonly #pairKeys = new Int32Array(MAX_SCALE_PARTITIONS);
  readonly #pairCounts = new Int32Array(MAX_SCALE_PARTITIONS + 1);
  readonly #pairOf: Uint8Array;
  #wordsCache: { source: BigInt64Array | null; words: Uint32Array } = {
    source: null,
    words: new Uint32Array(0),
  };
  #wordsCacheB: { source: BigInt64Array | null; words: Uint32Array } = {
    source: null,
    words: new Uint32Array(0),
  };
  constructor(
    op: ArithOp,
    type: PhysicalType,
    left: Compiled,
    right: Compiled,
    arena: OutputArena,
  ) {
    super(type, left, right);
    this.#op = OPS[op];
    const capacity = arena.capacity;
    arena.take(
      2 * capacity +
        8 * capacity +
        16 * capacity +
        8 * capacity +
        32 * capacity +
        capacity +
        3 * capacity,
    );
    this.#scales = new Uint16Array(capacity);
    this.#safeCoefficients = new Float64Array(capacity);
    this.#limbs = new Uint32Array(4 * capacity);
    this.#int64 = new BigInt64Array(capacity);
    this.#int64Words = int64Words(this.#int64);
    this.#wideValues = new Array<bigint>(capacity).fill(0n);
    this.#specials = new Uint8Array(capacity);
    this.#order = new Uint16Array(capacity);
    this.#pairOf = new Uint8Array(capacity);
    this.#validity = arena.validity();
    this.#perSlot = decimalVector(
      type,
      null,
      this.#scales,
      { repr: "safe", values: this.#safeCoefficients },
      null,
    );
    const tiers: FlatVector[] = [];
    for (const coefficients of [
      { repr: "int64", values: this.#int64 } as const,
      { repr: "int128", limbs: this.#limbs } as const,
      { repr: "wide", values: this.#wideValues } as const,
    ]) {
      tiers.push(decimalVector(type, null, this.#scales, coefficients, null));
      tiers.push(decimalVector(type, null, this.#scales, coefficients, this.#specials));
    }
    this.#tiers = tiers;
  }
  compute(length: number, l: Vector, r: Vector, rows: Uint16Array, count: number): Vector {
    const validity = combineValidity(
      operandValidity(l),
      operandValidity(r),
      this.#validity,
      length,
    );
    const out =
      this.#fast(l, r, rows, count) ??
      this.#tiered(l, r, rows, count, validity) ??
      this.#slow(l, r, rows, count, validity);
    out.length = length;
    out.validity = validity;
    return out;
  }
  /** Both operands flat or constant, safe tier, no specials: f64 while every value stays exact. */
  #fast(l: Vector, r: Vector, rows: Uint16Array, count: number): FlatVector | null {
    if (l.form === "dictionary" || r.form === "dictionary") return null;
    const lf = l.form === "flat" ? l : l.value;
    const rf = r.form === "flat" ? r : r.value;
    const as = l.form === "flat" ? 1 : 0;
    const bs = r.form === "flat" ? 1 : 0;
    const ld = lf.data as DecimalData;
    const rd = rf.data as DecimalData;
    if (ld.coefficients.repr !== "safe" || rd.coefficients.repr !== "safe") return null;
    if (ld.specials !== null || rd.specials !== null) return null;
    const a = ld.coefficients.values;
    const b = rd.coefficients.values;
    const out = this.#safeCoefficients;
    const op = this.#op;
    // Out of the exact range (possibly only under a null, or in an unselected row): recompute
    // exactly in a wider tier.
    let bad = false;
    if (ld.scale !== null && rd.scale !== null) {
      const sa = ld.scale;
      const sb = rd.scale;
      if (op === MUL) {
        if (sa + sb > NUMERIC_MAX_SCALE) return null;
        for (let k = 0; k < count; k++) {
          const i = rows[k] ?? 0;
          const v = (a[i * as] ?? 0) * (b[i * bs] ?? 0) + 0;
          out[i] = v;
          if (v > MAX_SAFE || v < -MAX_SAFE) bad = true;
        }
      } else {
        const scale = Math.max(sa, sb);
        if (scale - Math.min(sa, sb) > 15) return null;
        const fa = POW10[scale - sa] ?? 1;
        const fb = POW10[scale - sb] ?? 1;
        const sign = op === SUB ? -1 : 1;
        for (let k = 0; k < count; k++) {
          const i = rows[k] ?? 0;
          const x = (a[i * as] ?? 0) * fa;
          const y = (b[i * bs] ?? 0) * fb;
          const v = x + sign * y + 0;
          out[i] = v;
          if (
            x > MAX_SAFE ||
            x < -MAX_SAFE ||
            y > MAX_SAFE ||
            y < -MAX_SAFE ||
            v > MAX_SAFE ||
            v < -MAX_SAFE
          )
            bad = true;
        }
      }
      if (bad) return null;
      return this.#uniform(op === MUL ? sa + sb : Math.max(sa, sb));
    }
    const scales = this.#scales;
    for (let k = 0; k < count; k++) {
      const i = rows[k] ?? 0;
      const sa = scaleAt(ld, i * as);
      const sb = scaleAt(rd, i * bs);
      if (op === MUL) {
        const v = (a[i * as] ?? 0) * (b[i * bs] ?? 0) + 0;
        out[i] = v;
        scales[i] = sa + sb;
        if (v > MAX_SAFE || v < -MAX_SAFE || sa + sb > NUMERIC_MAX_SCALE) bad = true;
        continue;
      }
      const scale = sa > sb ? sa : sb;
      const da = scale - sa;
      const db = scale - sb;
      if (da > 15 || db > 15) {
        bad = true;
        continue;
      }
      const x = (a[i * as] ?? 0) * (POW10[da] ?? 1);
      const y = (b[i * bs] ?? 0) * (POW10[db] ?? 1);
      const v = (op === SUB ? x - y : x + y) + 0;
      out[i] = v;
      scales[i] = scale;
      if (
        x > MAX_SAFE ||
        x < -MAX_SAFE ||
        y > MAX_SAFE ||
        y < -MAX_SAFE ||
        v > MAX_SAFE ||
        v < -MAX_SAFE
      )
        bad = true;
    }
    return bad ? null : this.#perSlot;
  }
  #uniform(scale: number): FlatVector {
    let vector = this.#uniformVectors.get(scale);
    if (vector === undefined) {
      vector = decimalVector(
        this.type,
        scale,
        null,
        { repr: "safe", values: this.#safeCoefficients },
        null,
      );
      this.#uniformVectors.set(scale, vector);
    }
    return vector;
  }
  #words(data: DecimalData, second: boolean): Uint32Array {
    const coefficients = data.coefficients;
    if (coefficients.repr !== "int64") return EMPTY_WORDS;
    const cache = second ? this.#wordsCacheB : this.#wordsCache;
    if (cache.source !== coefficients.values) {
      cache.source = coefficients.values;
      cache.words = int64Words(coefficients.values);
    }
    return cache.words;
  }
  /**
   * Groups the computed valid rows by (left scale, right scale) into `#order`; returns the number
   * of groups, or −1 past MAX_SCALE_PARTITIONS distinct pairs.
   */
  #partition(
    lf: FlatVector,
    l: Vector,
    rf: FlatVector,
    r: Vector,
    rows: Uint16Array,
    count: number,
  ): number {
    const ld = lf.data as DecimalData;
    const rd = rf.data as DecimalData;
    const keys = this.#pairKeys;
    const counts = this.#pairCounts;
    const pairOf = this.#pairOf;
    let pairs = 0;
    counts.fill(0);
    for (let k = 0; k < count; k++) {
      const i = rows[k] ?? 0;
      const key = (scaleAt(ld, slotIndex(l, i)) << 16) | scaleAt(rd, slotIndex(r, i));
      let p = 0;
      while (p < pairs && keys[p] !== key) p++;
      if (p === pairs) {
        if (pairs === MAX_SCALE_PARTITIONS) return -1;
        keys[pairs++] = key;
      }
      pairOf[i] = p;
      counts[p + 1] = (counts[p + 1] ?? 0) + 1;
    }
    for (let p = 1; p <= pairs; p++) counts[p] = (counts[p] ?? 0) + (counts[p - 1] ?? 0);
    const order = this.#order;
    for (let k = 0; k < count; k++) {
      const i = rows[k] ?? 0;
      const p = pairOf[i] ?? 0;
      order[counts[p] ?? 0] = i;
      counts[p] = (counts[p] ?? 0) + 1;
    }
    // counts[p] is now the end of group p.
    return pairs;
  }
  /** The 128-bit limb tier; null when a value passes 128 bits (or the display scale cap). */
  #tiered(
    l: Vector,
    r: Vector,
    rows: Uint16Array,
    count: number,
    validity: Validity | null,
  ): FlatVector | null {
    const lf = flatOf(l);
    const rf = flatOf(r);
    const ld = lf.data as DecimalData;
    const rd = rf.data as DecimalData;
    if (ld.coefficients.repr === "wide" || rd.coefficients.repr === "wide") return null;
    const pairs = this.#partition(lf, l, rf, r, rows, count);
    if (pairs < 0) return null;
    const lw = this.#words(ld, false);
    const rw = this.#words(rd, true);
    const out = this.#limbs;
    const scales = this.#scales;
    const specials = this.#specials;
    const order = this.#order;
    const ends = this.#pairCounts;
    const op = this.#op;
    let fits = true;
    let anySpecial = false;
    let start = 0;
    for (let p = 0; p < pairs; p++) {
      const end = ends[p] ?? 0;
      const key = this.#pairKeys[p] ?? 0;
      const sa = key >>> 16;
      const sb = key & 0xffff;
      const scale = op === MUL ? sa + sb : Math.max(sa, sb);
      if (scale > NUMERIC_MAX_SCALE) return null;
      const da = op === MUL ? 0 : scale - sa;
      const db = op === MUL ? 0 : scale - sb;
      for (let k = start; k < end; k++) {
        const i = order[k] ?? 0;
        specials[i] = 0;
        scales[i] = scale;
        if (!validAt(validity, i)) continue;
        const li = slotIndex(l, i);
        const ri = slotIndex(r, i);
        loadCoefficient(SCRATCH, 0, ld, li, lw);
        loadCoefficient(SCRATCH, 1, rd, ri, rw);
        const xs = specialAt(ld, li);
        const ys = specialAt(rd, ri);
        if (xs !== 0 || ys !== 0) {
          specials[i] = special(op, xs, ys, sign(SCRATCH, 0), sign(SCRATCH, 1));
          anySpecial = true;
          setNumber(out, i, 0);
          continue;
        }
        let ok: boolean;
        if (op === MUL) ok = multiplyTo(out, i, SCRATCH, 0, SCRATCH, 1);
        else
          ok =
            scaleUp(SCRATCH, 0, da) &&
            scaleUp(SCRATCH, 1, db) &&
            addTo(out, i, SCRATCH, 0, SCRATCH, 1, op === SUB);
        if (!ok) return null;
        if (fits && !fitsInt64(out, i)) fits = false;
      }
      start = end;
    }
    if (fits) {
      const words = this.#int64Words;
      for (let k = 0; k < count; k++) {
        const i = rows[k] ?? 0;
        storeInt64(out, i, words, i);
      }
      return this.#tiers[anySpecial ? 1 : 0] ?? null;
    }
    return this.#tiers[anySpecial ? 3 : 2] ?? null;
  }
  /** Exact BigInt for values past 128 bits, `wide` inputs, and products past the scale cap. */
  #slow(
    l: Vector,
    r: Vector,
    rows: Uint16Array,
    count: number,
    validity: Validity | null,
  ): FlatVector {
    const values = this.#wideValues;
    const scales = this.#scales;
    const specials = this.#specials;
    let anySpecial = false;
    const op = this.#op;
    const lf = flatOf(l);
    const rf = flatOf(r);
    const ld = lf.data as DecimalData;
    const rd = rf.data as DecimalData;
    for (let k = 0; k < count; k++) {
      const i = rows[k] ?? 0;
      specials[i] = 0;
      if (!validAt(validity, i)) {
        values[i] = 0n;
        scales[i] = 0;
        continue;
      }
      const li = slotIndex(l, i);
      const ri = slotIndex(r, i);
      const xs = specialAt(ld, li);
      const ys = specialAt(rd, ri);
      const sa = scaleAt(ld, li);
      const sb = scaleAt(rd, ri);
      if (xs !== 0 || ys !== 0) {
        anySpecial = true;
        const x = xs === 0 ? coefficientAt(ld, li) : 0n;
        const y = ys === 0 ? coefficientAt(rd, ri) : 0n;
        specials[i] = special(
          op,
          xs,
          ys,
          x > 0n ? 1 : x < 0n ? -1 : 0,
          y > 0n ? 1 : y < 0n ? -1 : 0,
        );
        values[i] = 0n;
        scales[i] = 0;
        continue;
      }
      const x = coefficientAt(ld, li);
      const y = coefficientAt(rd, ri);
      if (op === MUL) {
        // PostgreSQL rounds a product past the largest display scale to it (mul_var's rscale).
        const scale = sa + sb;
        values[i] =
          scale > NUMERIC_MAX_SCALE ? roundHalfAway(x * y, scale - NUMERIC_MAX_SCALE) : x * y;
        scales[i] = Math.min(scale, NUMERIC_MAX_SCALE);
        continue;
      }
      const scale = sa > sb ? sa : sb;
      const ax = sa === scale ? x : x * 10n ** BigInt(scale - sa);
      const ay = sb === scale ? y : y * 10n ** BigInt(scale - sb);
      values[i] = op === SUB ? ax - ay : ax + ay;
      scales[i] = scale;
    }
    return this.#tiers[anySpecial ? 5 : 4] ?? this.#perSlot;
  }
}

const EMPTY_WORDS = new Uint32Array(0);

/** c / 10^digits rounded half away from zero. */
function roundHalfAway(c: bigint, digits: number): bigint {
  const divisor = 10n ** BigInt(digits);
  const q = c / divisor;
  const r = c - q * divisor;
  return 2n * (r < 0n ? -r : r) >= divisor ? q + (c < 0n ? -1n : 1n) : q;
}

/** Loads coefficient `slot` of a non-wide decimal into limb slot `to` of `t`. */
function loadCoefficient(
  t: Uint32Array,
  to: number,
  data: DecimalData,
  slot: number,
  words: Uint32Array,
): void {
  const c = data.coefficients;
  switch (c.repr) {
    case "safe":
      setNumber(t, to, c.values[slot] ?? 0);
      return;
    case "int64":
      setInt64(t, to, words, slot);
      return;
    case "int128":
      copy(t, to, c.limbs, slot);
      return;
    case "wide":
      throw new TypeError("loadCoefficient: wide tier");
  }
}

/** PostgreSQL's NUMERIC special arithmetic (x or y is NaN/±Infinity); sx, sy are finite signs. */
function special(op: number, xs: number, ys: number, fx: number, fy: number): number {
  if (xs === NAN || ys === NAN) return NAN;
  const sx = xs === POS ? 1 : xs === NEG ? -1 : fx;
  const sy = ys === POS ? 1 : ys === NEG ? -1 : fy;
  if (op === MUL) {
    if (sx === 0 || sy === 0) return NAN; // ∞ × 0
    return sx * sy > 0 ? POS : NEG;
  }
  const ey = op === SUB ? -sy : sy;
  if (xs !== 0 && ys !== 0) return sx === ey ? (sx > 0 ? POS : NEG) : NAN;
  if (xs !== 0) return sx > 0 ? POS : NEG;
  return ey > 0 ? POS : NEG;
}

// ------------------------------------------------------------------------------------------------

/**
 * Unary minus, natively per type (not 0 − x, which loses −0 for floats: −(+0.0) is −0 in
 * PostgreSQL). Integers check −MIN; decimals flip the coefficient (and ±Infinity); NaN stays NaN.
 */
class NegateNode implements Compiled {
  readonly #null: Vector;
  readonly #out: FlatVector;
  readonly #wide: FlatVector | null = null;
  readonly #operand: FixedOperand;
  readonly #message: string;
  readonly #min: number;
  readonly #decimal: DecimalNode | null;
  constructor(
    readonly type: PhysicalType,
    readonly input: Compiled,
    arena: OutputArena,
  ) {
    this.#null = nullConstant(type);
    this.#operand = new FixedOperand(arena, type);
    const small = type.kind === "int32" && type.bits === 16;
    // −MIN overflows for integers (int64's safe values cannot be MIN); NaN never compares equal.
    this.#min = type.kind === "int32" ? (small ? INT16_MIN : INT32_MIN) : NaN;
    this.#message = small
      ? "smallint out of range"
      : type.kind === "int32"
        ? "integer out of range"
        : "bigint out of range";
    if (type.kind === "decimal") {
      // −x = (−1) × x keeps every tier and special exactly (scale 0 × anything adds no scale).
      const minusOne = decimalVector(
        type,
        0,
        null,
        { repr: "safe", values: Float64Array.of(-1) },
        null,
      );
      minusOne.length = 1;
      this.#decimal = new DecimalNode("mul", type, new ConstantNode(type, minusOne), input, arena);
      this.#out = minusOne;
      return;
    }
    this.#decimal = null;
    this.#out = arena.flat(type).vector;
    if (type.kind === "int64") {
      arena.take(8 * arena.capacity);
      this.#wide = {
        form: "flat",
        type,
        length: 0,
        validity: null,
        data: {
          layout: "int64",
          values: { repr: "int64", values: new BigInt64Array(arena.capacity) },
        },
      };
    }
  }
  evaluate(batch: Batch, rows: Selection | null): Vector {
    if (this.#decimal !== null) return this.#decimal.evaluate(batch, rows);
    const value = this.input.evaluate(batch, rows);
    if (constantIsNull(value)) {
      (this.#null as { length: number }).length = batch.length;
      return this.#null;
    }
    const active = rowsIn(rows);
    const count = countIn(rows, batch.length);
    const flat = this.#operand.read(value, active, count);
    const stride = this.#operand.stride;
    const validity = value.form === "constant" ? null : value.validity;
    const data = flat.data;
    let out = this.#out;
    if (data.layout === "int64" && data.values.repr === "int64") {
      const wide = this.#wide;
      if (wide === null) throw new TypeError("negate: no int64 output");
      const source = data.values.values;
      const target = ((wide.data as { values: Int64Values }).values as { values: BigInt64Array })
        .values;
      for (let k = 0; k < count; k++) {
        const i = active[k] ?? 0;
        if (!validAt(validity, i)) continue;
        const v = source[i * stride] ?? 0n;
        if (v === INT64_MIN) throw new NumericOutOfRangeError(this.#message);
        target[i] = -v;
      }
      out = wide;
    } else {
      const source =
        data.layout === "int64"
          ? (data.values as { values: Float64Array }).values
          : (data as { values: Int32Array | Float32Array | Float64Array }).values;
      const target =
        out.data.layout === "int64"
          ? (out.data.values as { values: Float64Array }).values
          : (out.data as { values: Int32Array | Float32Array | Float64Array }).values;
      const integer = this.type.kind === "int32" || this.type.kind === "int64";
      let bad = false;
      for (let k = 0; k < count; k++) {
        const i = active[k] ?? 0;
        const v = source[i * stride] ?? 0;
        // Integers never hold −0 (+ 0 drops it); floats keep IEEE negation, −(+0) = −0.
        target[i] = integer ? -v + 0 : -v;
        bad ||= v === this.#min;
      }
      if (bad)
        for (let k = 0; k < count; k++) {
          const i = active[k] ?? 0;
          if (validAt(validity, i) && source[i * stride] === this.#min)
            throw new NumericOutOfRangeError(this.#message);
        }
    }
    out.length = batch.length;
    out.validity = validity;
    return out;
  }
}

/** A constant operand (negation's −1). */
class ConstantNode implements Compiled {
  readonly #vector: { form: "constant"; type: PhysicalType; length: number; value: FlatVector };
  constructor(
    readonly type: PhysicalType,
    value: FlatVector,
  ) {
    this.#vector = { form: "constant", type, length: 0, value };
  }
  evaluate(batch: Batch): Vector {
    this.#vector.length = batch.length;
    return this.#vector;
  }
}

export function compileNegate(type: PhysicalType, input: Compiled, arena: OutputArena): Compiled {
  if (!["int32", "int64", "float64", "float32", "decimal"].includes(type.kind))
    throw new UnsupportedPlanError(`negation of ${type.kind}`);
  return new NegateNode(type, input, arena);
}

/** Compiles `left op right` for operands already cast to `type` (lowering inserts the casts). */
export function compileArithmetic(
  op: ArithOp,
  type: PhysicalType,
  left: Compiled,
  right: Compiled,
  arena: OutputArena,
): Compiled {
  switch (type.kind) {
    case "int32":
      return new Int32Node(op, type, left, right, arena);
    case "int64":
      return new Int64Node(op, type, left, right, arena);
    case "float64":
    case "float32":
      return new FloatNode(op, type, left, right, arena);
    case "decimal":
      if (op === "div" || op === "mod")
        throw new UnsupportedPlanError("numeric division and modulo");
      return new DecimalNode(op, type, left, right, arena);
    default:
      throw new UnsupportedPlanError(`${op} over ${type.kind}`);
  }
}
