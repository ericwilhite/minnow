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
  copyFixedSlot,
  nullConstant,
  rowsOf,
  slotOf,
  validAt,
  type Compiled,
  type OutputArena,
} from "./vectors.js";

export type ArithOp = "add" | "sub" | "mul" | "div" | "mod";
const MOD = 4;
const OPS: Record<ArithOp, number> = { add: ADD, sub: SUB, mul: MUL, div: DIV, mod: MOD };

/** A fixed-width operand read at `row * stride`, dictionaries materialized into `scratch`. */
class FixedOperand {
  readonly #scratch: FlatVector | null;
  constructor(arena: OutputArena, type: PhysicalType, dictionaryPossible: boolean) {
    this.#scratch = dictionaryPossible ? arena.flat(type).vector : null;
  }
  /** Returns [flat, stride] for the computed rows. */
  read(vector: Vector, rows: Uint16Array, count: number): [FlatVector, 0 | 1] {
    if (vector.form === "flat") return [vector, 1];
    if (vector.form === "constant") return [vector.value, 0];
    const scratch = this.#scratch;
    if (scratch === null) throw new TypeError("unexpected dictionary operand");
    for (let k = 0; k < count; k++) {
      const row = rows[k] ?? 0;
      copyFixedSlot(vector.dictionary.data, vector.codes[row] ?? 0, scratch.data, row);
    }
    scratch.validity = vector.validity;
    return [scratch, 1];
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
    const [active, count] = rowsOf(rows, batch.length);
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
    this.#a = new FixedOperand(arena, left.type, true);
    this.#b = new FixedOperand(arena, right.type, true);
  }
  compute(length: number, l: Vector, r: Vector, rows: Uint16Array, count: number): Vector {
    const [lf, as] = this.#a.read(l, rows, count);
    const [rf, bs] = this.#b.read(r, rows, count);
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
    this.#a = new FixedOperand(arena, left.type, true);
    this.#b = new FixedOperand(arena, right.type, true);
  }
  compute(length: number, l: Vector, r: Vector, rows: Uint16Array, count: number): Vector {
    const [lf, as] = this.#a.read(l, rows, count);
    const [rf, bs] = this.#b.read(r, rows, count);
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
    this.#a = new FixedOperand(arena, left.type, true);
    this.#b = new FixedOperand(arena, right.type, true);
  }
  compute(length: number, l: Vector, r: Vector, rows: Uint16Array, count: number): Vector {
    const [lf, as] = this.#a.read(l, rows, count);
    const [rf, bs] = this.#b.read(r, rows, count);
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
    const zero = zeroSeen;
    if (this.#real) {
      m = 0;
      for (let k = 0; k < count; k++) {
        const i = rows[k] ?? 0;
        const v = Math.fround(out[i] ?? 0);
        out[i] = v;
        m = Math.max(m, Math.abs(v));
      }
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
      if (op === DIV && y === 0) throw new DivisionByZeroError();
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

class DecimalNode extends BinaryNode {
  readonly #op: number;
  readonly #perSlot: FlatVector;
  readonly #safeCoefficients: Float64Array;
  readonly #scales: Uint16Array;
  readonly #wideValues: bigint[];
  readonly #specials: Uint8Array;
  readonly #validity: Validity;
  constructor(
    op: ArithOp,
    type: PhysicalType,
    left: Compiled,
    right: Compiled,
    arena: OutputArena,
  ) {
    super(type, left, right);
    if (op === "div" || op === "mod") throw new UnsupportedPlanError("numeric division and modulo");
    this.#op = OPS[op];
    const capacity = arena.capacity;
    arena.take(8 * capacity + 2 * capacity + capacity + 32 * capacity);
    this.#safeCoefficients = new Float64Array(capacity);
    this.#scales = new Uint16Array(capacity);
    this.#specials = new Uint8Array(capacity);
    this.#wideValues = new Array<bigint>(capacity).fill(0n);
    this.#validity = arena.validity();
    this.#perSlot = this.#vector(
      null,
      this.#scales,
      { repr: "safe", values: this.#safeCoefficients },
      null,
    );
  }
  #vector(
    scale: number | null,
    scales: Uint16Array | null,
    coefficients: DecimalData["coefficients"],
    specials: Uint8Array | null,
  ): FlatVector {
    return {
      form: "flat",
      type: this.type,
      length: 0,
      validity: null,
      data: { layout: "decimal", scale, scales, coefficients, specials },
    };
  }
  compute(length: number, l: Vector, r: Vector, rows: Uint16Array, count: number): Vector {
    const validity = combineValidity(
      operandValidity(l),
      operandValidity(r),
      this.#validity,
      length,
    );
    const fast = this.#fast(l, r, rows, count, validity, length);
    if (fast !== null) return fast;
    return this.#slow(l, r, rows, count, validity, length);
  }
  /** Both operands flat or constant, safe tier, no specials: f64 while every value stays exact. */
  #fast(
    l: Vector,
    r: Vector,
    rows: Uint16Array,
    count: number,
    validity: Validity | null,
    length: number,
  ): FlatVector | null {
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
    const uniform = ld.scale !== null && rd.scale !== null;
    const op = this.#op;
    let bad = false;
    if (uniform) {
      const sa = ld.scale;
      const sb = rd.scale;
      if (op === MUL) {
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
      // Out of the exact range (possibly only under a null): recompute exactly.
      if (bad) return null;
      const scale = op === MUL ? sa + sb : Math.max(sa, sb);
      const vector = this.#uniform(scale);
      vector.length = length;
      vector.validity = validity;
      return vector;
    }
    // Per-slot scales.
    const scales = this.#scales;
    for (let k = 0; k < count; k++) {
      const i = rows[k] ?? 0;
      const sa = scaleAt(ld, i * as);
      const sb = scaleAt(rd, i * bs);
      if (op === MUL) {
        const v = (a[i * as] ?? 0) * (b[i * bs] ?? 0) + 0;
        out[i] = v;
        scales[i] = sa + sb;
        if (v > MAX_SAFE || v < -MAX_SAFE || sa + sb > 16383) bad = true;
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
    if (bad) return null;
    const vector = this.#perSlot;
    vector.length = length;
    vector.validity = validity;
    return vector;
  }
  readonly #uniformVectors = new Map<number, FlatVector>();
  #uniform(scale: number): FlatVector {
    let vector = this.#uniformVectors.get(scale);
    if (vector === undefined) {
      vector = this.#vector(scale, null, { repr: "safe", values: this.#safeCoefficients }, null);
      this.#uniformVectors.set(scale, vector);
    }
    return vector;
  }
  /** Exact BigInt path for every case: any tier, specials, dictionaries. */
  #slow(
    l: Vector,
    r: Vector,
    rows: Uint16Array,
    count: number,
    validity: Validity | null,
    length: number,
  ): FlatVector {
    const values = this.#wideValues;
    const scales = this.#scales;
    const specials = this.#specials;
    let anySpecial = false;
    const op = this.#op;
    for (let k = 0; k < count; k++) {
      const i = rows[k] ?? 0;
      specials[i] = 0;
      if (!validAt(validity, i)) {
        values[i] = 0n;
        scales[i] = 0;
        continue;
      }
      const [lf, li] = slotOf(l, i);
      const [rf, ri] = slotOf(r, i);
      const ld = lf.data as DecimalData;
      const rd = rf.data as DecimalData;
      const xs = specialAt(ld, li);
      const ys = specialAt(rd, ri);
      const sa = scaleAt(ld, li);
      const sb = scaleAt(rd, ri);
      if (xs !== 0 || ys !== 0) {
        anySpecial = true;
        specials[i] = special(
          op,
          xs,
          ys,
          xs === 0 ? coefficientAt(ld, li) : 0n,
          ys === 0 ? coefficientAt(rd, ri) : 0n,
        );
        values[i] = 0n;
        scales[i] = 0;
        continue;
      }
      const x = coefficientAt(ld, li);
      const y = coefficientAt(rd, ri);
      if (op === MUL) {
        values[i] = x * y;
        scales[i] = sa + sb;
        continue;
      }
      const scale = sa > sb ? sa : sb;
      const ax = sa === scale ? x : x * 10n ** BigInt(scale - sa);
      const ay = sb === scale ? y : y * 10n ** BigInt(scale - sb);
      values[i] = op === SUB ? ax - ay : ax + ay;
      scales[i] = scale;
    }
    const vector = this.#vector(
      null,
      scales,
      { repr: "wide", values },
      anySpecial ? specials : null,
    );
    vector.length = length;
    vector.validity = validity;
    return vector;
  }
}

/** PostgreSQL's NUMERIC special arithmetic (x or y is NaN/±Infinity). */
function special(op: number, xs: number, ys: number, x: bigint, y: bigint): number {
  if (xs === NAN || ys === NAN) return NAN;
  const sign = (s: number, v: bigint) =>
    s === POS ? 1 : s === NEG ? -1 : v > 0n ? 1 : v < 0n ? -1 : 0;
  const sx = sign(xs, x);
  const sy = sign(ys, y);
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
      return new DecimalNode(op, type, left, right, arena);
    default:
      throw new UnsupportedPlanError(`${op} over ${type.kind}`);
  }
}
