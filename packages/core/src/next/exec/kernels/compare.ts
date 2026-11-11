/**
 * Comparison kernels: `compare` (= <> < <= > >=) and `distinctFrom` over every orderable layout,
 * producing a bool vector with SQL's null propagation (distinctFrom is never null).
 *
 * ## Orders (PostgreSQL)
 * - Integers, dates, times, timestamps: numeric order of the stored value (±infinity sentinels
 *   order correctly as int min/max).
 * - Floats: NaN equals NaN and is greater than every other value; −0 = 0.
 * - Decimal: by value across scales (1.50 = 1.5); NaN greatest, then +Infinity; −Infinity least.
 * - Text, bytes, uuid: unsigned byte order (the C collation, DECISIONS.md C8). A dictionary
 *   operand against a constant is compared once per dictionary entry and then mapped by code; two
 *   operands over the same dictionary compare codes for (in)equality.
 * - Bool: false < true.
 * Intervals use a separate exact 30-day-month comparator. Explicit ICU collations, enums ordered
 * by rank, JSON and arrays are not compiled here
 * yet (`compileCompare` throws `TypeError`, which the compiler reports as unsupported).
 */
import { IntervalCompare } from "./interval-compare.js";
import type { PhysicalType } from "../../types.js";
import { layoutOf } from "../../types.js";
import { compareBytes } from "../../vector/varlen.js";
import type {
  Batch,
  FlatVector,
  Int64Values,
  Selection,
  Validity,
  Vector,
} from "../../vector/vector.js";
import {
  bigAt,
  coefficientAt,
  constantIsNull,
  scaleAt,
  specialAt,
  POW10,
  type DecimalData,
} from "./numeric.js";
import {
  IDENTITY,
  combineValidity,
  flatOf,
  slotIndex,
  nullConstant,
  countIn,
  rowsIn,
  validAt,
  type Compiled,
  type OutputArena,
} from "./vectors.js";
import { MAX_SAFE } from "../../vector/int64.js";
import { UnsupportedPlanError } from "../errors.js";

export type CompareOp = "=" | "<>" | "<" | "<=" | ">" | ">=";

/** RESULT[op][order + 1]: the comparison's truth for order −1, 0, 1. */
const RESULT: Record<CompareOp, Uint8Array> = {
  "=": Uint8Array.of(0, 1, 0),
  "<>": Uint8Array.of(1, 0, 1),
  "<": Uint8Array.of(1, 0, 0),
  "<=": Uint8Array.of(1, 1, 0),
  ">": Uint8Array.of(0, 0, 1),
  ">=": Uint8Array.of(0, 1, 1),
};

const BOOL: PhysicalType = { kind: "bool" };

function floatOrder(x: number, y: number): number {
  if (x !== x) return y !== y ? 0 : 1; // NaN
  if (y !== y) return -1;
  return x < y ? -1 : x > y ? 1 : 0;
}

function bigOrder(x: bigint, y: bigint): number {
  return x < y ? -1 : x > y ? 1 : 0;
}

/** Order of two finite decimals (coefficient × 10^−scale). */
function decimalOrder(x: bigint, sx: number, y: bigint, sy: number): number {
  if (sx === sy) return bigOrder(x, y);
  return sx < sy
    ? bigOrder(x * 10n ** BigInt(sy - sx), y)
    : bigOrder(x, y * 10n ** BigInt(sx - sy));
}

/** Rank of a decimal special for ordering: −Infinity < finite < +Infinity < NaN. */
function specialRank(special: number): number {
  return special === 3 ? -1 : special === 0 ? 0 : special === 2 ? 1 : 2;
}

function decimalSlotOrder(ld: DecimalData, li: number, rd: DecimalData, ri: number): number {
  const xs = specialAt(ld, li);
  const ys = specialAt(rd, ri);
  if (xs !== 0 || ys !== 0) {
    const a = specialRank(xs);
    const b = specialRank(ys);
    return a < b ? -1 : a > b ? 1 : 0;
  }
  const sx = scaleAt(ld, li);
  const sy = scaleAt(rd, ri);
  const lc = ld.coefficients;
  const rc = rd.coefficients;
  if (lc.repr === "safe" && rc.repr === "safe") {
    const x = lc.values[li] ?? 0;
    const y = rc.values[ri] ?? 0;
    if (sx === sy) return x < y ? -1 : x > y ? 1 : 0;
    const d = Math.abs(sx - sy);
    if (d <= 15) {
      const ax = sx < sy ? x * (POW10[d] ?? 1) : x;
      const ay = sy < sx ? y * (POW10[d] ?? 1) : y;
      if (Math.abs(ax) <= MAX_SAFE && Math.abs(ay) <= MAX_SAFE)
        return ax < ay ? -1 : ax > ay ? 1 : 0;
    }
  }
  return decimalOrder(coefficientAt(ld, li), sx, coefficientAt(rd, ri), sy);
}

/** Order of slot `i` of `l` against slot `j` of `r`, generic over layouts (the slow path). */
function slotOrder(l: FlatVector, i: number, r: FlatVector, j: number): number {
  const a = l.data;
  const b = r.data;
  switch (a.layout) {
    case "bool":
    case "i32":
    case "f32":
    case "f64": {
      const x = a.values[i] ?? 0;
      const y = (b as typeof a).values[j] ?? 0;
      return floatOrder(x, y);
    }
    case "int64": {
      const bv = (b as typeof a).values;
      if (a.values.repr === "safe" && bv.repr === "safe")
        return floatOrder(a.values.values[i] ?? 0, bv.values[j] ?? 0);
      return bigOrder(bigAt(a.values, i), bigAt(bv, j));
    }
    case "decimal":
      return decimalSlotOrder(a, i, b as DecimalData, j);
    case "varlen": {
      const bd = b as typeof a;
      return compareBytes(
        a.bytes,
        a.offsets[i] ?? 0,
        a.offsets[i + 1] ?? 0,
        bd.bytes,
        bd.offsets[j] ?? 0,
        bd.offsets[j + 1] ?? 0,
      );
    }
    case "fixed16": {
      const bd = b as typeof a;
      return compareBytes(a.bytes, i * 16, i * 16 + 16, bd.bytes, j * 16, j * 16 + 16);
    }
    case "interval":
      throw new UnsupportedPlanError("interval comparison");
  }
}

const OP_CODES: Record<CompareOp, number> = { "=": 0, "<>": 1, "<": 2, "<=": 3, ">": 4, ">=": 5 };

/**
 * `out[i] = x[i·as] op y[i·bs]` for the computed rows, branch-free (`+(p < q)` compiles to a set
 * instruction; a data-dependent branch mispredicts on half of random rows). Returns false if a NaN
 * was seen (the caller recomputes with PostgreSQL's NaN order); integer inputs never have one.
 */
function numberLoop(
  op: number,
  x: ArrayLike<number>,
  as: number,
  y: ArrayLike<number>,
  bs: number,
  rows: Uint16Array,
  count: number,
  out: Uint8Array,
): boolean {
  if (rows === IDENTITY && as === 1)
    return bs === 0 ? denseConstant(op, x, y[0] ?? 0, count, out) : denseFlat(op, x, y, count, out);
  let nan = 0;
  switch (op) {
    case 0:
      for (let k = 0; k < count; k++) {
        const i = rows[k] ?? 0;
        const p = x[i * as] ?? 0;
        const q = y[i * bs] ?? 0;
        out[i] = +(p === q);
        nan |= +(p !== p) | +(q !== q);
      }
      break;
    case 1:
      for (let k = 0; k < count; k++) {
        const i = rows[k] ?? 0;
        const p = x[i * as] ?? 0;
        const q = y[i * bs] ?? 0;
        out[i] = +(p !== q);
        nan |= +(p !== p) | +(q !== q);
      }
      break;
    case 2:
      for (let k = 0; k < count; k++) {
        const i = rows[k] ?? 0;
        const p = x[i * as] ?? 0;
        const q = y[i * bs] ?? 0;
        out[i] = +(p < q);
        nan |= +(p !== p) | +(q !== q);
      }
      break;
    case 3:
      for (let k = 0; k < count; k++) {
        const i = rows[k] ?? 0;
        const p = x[i * as] ?? 0;
        const q = y[i * bs] ?? 0;
        out[i] = +(p <= q);
        nan |= +(p !== p) | +(q !== q);
      }
      break;
    case 4:
      for (let k = 0; k < count; k++) {
        const i = rows[k] ?? 0;
        const p = x[i * as] ?? 0;
        const q = y[i * bs] ?? 0;
        out[i] = +(p > q);
        nan |= +(p !== p) | +(q !== q);
      }
      break;
    default:
      for (let k = 0; k < count; k++) {
        const i = rows[k] ?? 0;
        const p = x[i * as] ?? 0;
        const q = y[i * bs] ?? 0;
        out[i] = +(p >= q);
        nan |= +(p !== p) | +(q !== q);
      }
  }
  return nan === 0;
}

/** `numberLoop` over rows 0 … count − 1 against one constant `q`. */
function denseConstant(
  op: number,
  x: ArrayLike<number>,
  q: number,
  count: number,
  out: Uint8Array,
): boolean {
  let nan = +(q !== q);
  switch (op) {
    case 0:
      for (let i = 0; i < count; i++) {
        const p = x[i] ?? 0;
        out[i] = +(p === q);
        nan |= +(p !== p);
      }
      break;
    case 1:
      for (let i = 0; i < count; i++) {
        const p = x[i] ?? 0;
        out[i] = +(p !== q);
        nan |= +(p !== p);
      }
      break;
    case 2:
      for (let i = 0; i < count; i++) {
        const p = x[i] ?? 0;
        out[i] = +(p < q);
        nan |= +(p !== p);
      }
      break;
    case 3:
      for (let i = 0; i < count; i++) {
        const p = x[i] ?? 0;
        out[i] = +(p <= q);
        nan |= +(p !== p);
      }
      break;
    case 4:
      for (let i = 0; i < count; i++) {
        const p = x[i] ?? 0;
        out[i] = +(p > q);
        nan |= +(p !== p);
      }
      break;
    default:
      for (let i = 0; i < count; i++) {
        const p = x[i] ?? 0;
        out[i] = +(p >= q);
        nan |= +(p !== p);
      }
  }
  return nan === 0;
}

/** `numberLoop` over rows 0 … count − 1 of two flat inputs. */
function denseFlat(
  op: number,
  x: ArrayLike<number>,
  y: ArrayLike<number>,
  count: number,
  out: Uint8Array,
): boolean {
  let nan = 0;
  switch (op) {
    case 0:
      for (let i = 0; i < count; i++) {
        const p = x[i] ?? 0;
        const q = y[i] ?? 0;
        out[i] = +(p === q);
        nan |= +(p !== p) | +(q !== q);
      }
      break;
    case 1:
      for (let i = 0; i < count; i++) {
        const p = x[i] ?? 0;
        const q = y[i] ?? 0;
        out[i] = +(p !== q);
        nan |= +(p !== p) | +(q !== q);
      }
      break;
    case 2:
      for (let i = 0; i < count; i++) {
        const p = x[i] ?? 0;
        const q = y[i] ?? 0;
        out[i] = +(p < q);
        nan |= +(p !== p) | +(q !== q);
      }
      break;
    case 3:
      for (let i = 0; i < count; i++) {
        const p = x[i] ?? 0;
        const q = y[i] ?? 0;
        out[i] = +(p <= q);
        nan |= +(p !== p) | +(q !== q);
      }
      break;
    case 4:
      for (let i = 0; i < count; i++) {
        const p = x[i] ?? 0;
        const q = y[i] ?? 0;
        out[i] = +(p > q);
        nan |= +(p !== p) | +(q !== q);
      }
      break;
    default:
      for (let i = 0; i < count; i++) {
        const p = x[i] ?? 0;
        const q = y[i] ?? 0;
        out[i] = +(p >= q);
        nan |= +(p !== p) | +(q !== q);
      }
  }
  return nan === 0;
}

class CompareNode implements Compiled {
  readonly type = BOOL;
  readonly #code: number;
  readonly #result: Uint8Array;
  readonly #out: FlatVector;
  readonly #validity: Validity;
  readonly #null = nullConstant(BOOL);
  /** Per-dictionary-entry results against a constant, keyed by dictionary id. */
  #entryId = -1;
  #entryConstant: FlatVector | null = null;
  #entries: Uint8Array = new Uint8Array(0);
  constructor(
    readonly op: CompareOp,
    readonly left: Compiled,
    readonly right: Compiled,
    arena: OutputArena,
  ) {
    this.#result = RESULT[op];
    this.#code = OP_CODES[op];
    const output = arena.flat(BOOL);
    this.#out = output.vector;
    this.#validity = output.validity;
  }

  evaluate(batch: Batch, rows: Selection | null): Vector {
    const l = this.left.evaluate(batch, rows);
    const r = this.right.evaluate(batch, rows);
    if (constantIsNull(l) || constantIsNull(r)) {
      (this.#null as { length: number }).length = batch.length;
      return this.#null;
    }
    const length = batch.length;
    const active = rowsIn(rows);
    const count = countIn(rows, length);
    const out = (this.#out.data as { values: Uint8Array }).values;
    const lv = l.form === "constant" ? null : l.validity;
    const rv = r.form === "constant" ? null : r.validity;
    const validity = combineValidity(lv, rv, this.#validity, length);
    const result = this.#result;
    if (l.form !== "dictionary" && r.form !== "dictionary") {
      const lf = l.form === "flat" ? l : l.value;
      const rf = r.form === "flat" ? r : r.value;
      const as = l.form === "flat" ? 1 : 0;
      const bs = r.form === "flat" ? 1 : 0;
      this.#flat(lf, as, rf, bs, active, count, out, result, validity);
    } else if (l.form === "dictionary" && r.form === "constant") {
      this.#dictionaryConstant(l, r.value, active, count, out, result, false);
    } else if (r.form === "dictionary" && l.form === "constant") {
      this.#dictionaryConstant(r, l.value, active, count, out, result, true);
    } else if (
      l.form === "dictionary" &&
      r.form === "dictionary" &&
      l.dictionaryId === r.dictionaryId &&
      (this.op === "=" || this.op === "<>")
    ) {
      const equal = this.op === "=" ? 1 : 0;
      for (let k = 0; k < count; k++) {
        const i = active[k] ?? 0;
        out[i] = (l.codes[i] ?? 0) === (r.codes[i] ?? 0) ? equal : 1 - equal;
      }
    } else {
      const lf = flatOf(l);
      const rf = flatOf(r);
      for (let k = 0; k < count; k++) {
        const i = active[k] ?? 0;
        out[i] = validAt(validity, i)
          ? (result[slotOrder(lf, slotIndex(l, i), rf, slotIndex(r, i)) + 1] ?? 0)
          : 0;
      }
    }
    this.#out.length = length;
    this.#out.validity = validity;
    return this.#out;
  }

  #flat(
    lf: FlatVector,
    as: number,
    rf: FlatVector,
    bs: number,
    active: Uint16Array,
    count: number,
    out: Uint8Array,
    result: Uint8Array,
    validity: Validity | null,
  ): void {
    const a = lf.data;
    const b = rf.data;
    const op = this.#code;
    if (a.layout === "i32" || a.layout === "bool") {
      numberLoop(op, a.values, as, (b as typeof a).values, bs, active, count, out);
      return;
    }
    if (a.layout === "f64" || a.layout === "f32") {
      const x = a.values;
      const y = (b as typeof a).values;
      // IEEE comparison is PostgreSQL's order unless a NaN is involved: check, then fall back.
      if (!numberLoop(op, x, as, y, bs, active, count, out))
        for (let k = 0; k < count; k++) {
          const i = active[k] ?? 0;
          out[i] = result[floatOrder(x[i * as] ?? 0, y[i * bs] ?? 0) + 1] ?? 0;
        }
      return;
    }
    if (a.layout === "int64") {
      const x: Int64Values = a.values;
      const y = (b as typeof a).values;
      if (x.repr === "safe" && y.repr === "safe") {
        numberLoop(op, x.values, as, y.values, bs, active, count, out);
        return;
      }
      for (let k = 0; k < count; k++) {
        const i = active[k] ?? 0;
        // Exact paths read only valid rows: garbage under a null may not even be an integer.
        out[i] = validAt(validity, i)
          ? (result[bigOrder(bigAt(x, i * as), bigAt(y, i * bs)) + 1] ?? 0)
          : 0;
      }
      return;
    }
    if (a.layout === "decimal") {
      const bd = b as DecimalData;
      if (
        a.scale !== null &&
        a.scale === bd.scale &&
        a.coefficients.repr === "safe" &&
        bd.coefficients.repr === "safe" &&
        a.specials === null &&
        bd.specials === null
      ) {
        numberLoop(op, a.coefficients.values, as, bd.coefficients.values, bs, active, count, out);
        return;
      }
      for (let k = 0; k < count; k++) {
        const i = active[k] ?? 0;
        out[i] = validAt(validity, i)
          ? (result[decimalSlotOrder(a, i * as, bd, i * bs) + 1] ?? 0)
          : 0;
      }
      return;
    }
    for (let k = 0; k < count; k++) {
      const i = active[k] ?? 0;
      out[i] = validAt(validity, i) ? (result[slotOrder(lf, i * as, rf, i * bs) + 1] ?? 0) : 0;
    }
  }

  /** Compares each dictionary entry with the constant once, then maps rows by code. */
  #dictionaryConstant(
    vector: Extract<Vector, { form: "dictionary" }>,
    constant: FlatVector,
    active: Uint16Array,
    count: number,
    out: Uint8Array,
    result: Uint8Array,
    swapped: boolean,
  ): void {
    if (this.#entryId !== vector.dictionaryId || this.#entryConstant !== constant) {
      const dictionary = vector.dictionary;
      if (this.#entries.length < dictionary.length)
        this.#entries = new Uint8Array(dictionary.length);
      for (let e = 0; e < dictionary.length; e++) {
        const order = slotOrder(dictionary, e, constant, 0);
        this.#entries[e] = result[(swapped ? -order : order) + 1] ?? 0;
      }
      this.#entryId = vector.dictionaryId;
      this.#entryConstant = constant;
    }
    const entries = this.#entries;
    const codes = vector.codes;
    for (let k = 0; k < count; k++) {
      const i = active[k] ?? 0;
      out[i] = entries[codes[i] ?? 0] ?? 0;
    }
  }
}

class DistinctFromNode implements Compiled {
  readonly type = BOOL;
  readonly #out: FlatVector;
  constructor(
    readonly negated: boolean,
    readonly left: Compiled,
    readonly right: Compiled,
    arena: OutputArena,
  ) {
    this.#out = arena.flat(BOOL).vector;
  }
  evaluate(batch: Batch, rows: Selection | null): Vector {
    const l = this.left.evaluate(batch, rows);
    const r = this.right.evaluate(batch, rows);
    const active = rowsIn(rows);
    const count = countIn(rows, batch.length);
    const out = (this.#out.data as { values: Uint8Array }).values;
    const distinct = this.negated ? 0 : 1;
    const lf = flatOf(l);
    const rf = flatOf(r);
    const lValidity = l.form === "constant" ? l.value.validity : l.validity;
    const rValidity = r.form === "constant" ? r.value.validity : r.validity;
    for (let k = 0; k < count; k++) {
      const i = active[k] ?? 0;
      const lValid = validAt(lValidity, l.form === "constant" ? 0 : i);
      const rValid = validAt(rValidity, r.form === "constant" ? 0 : i);
      const same =
        lValid && rValid
          ? slotOrder(lf, slotIndex(l, i), rf, slotIndex(r, i)) === 0
          : lValid === rValid;
      out[i] = same ? 1 - distinct : distinct;
    }
    this.#out.length = batch.length;
    this.#out.validity = null;
    return this.#out;
  }
}

const COMPARABLE = new Set(["bool", "i32", "f64", "f32", "int64", "decimal", "varlen", "fixed16"]);

function checkComparable(type: PhysicalType, collation: string | null): void {
  if (collation !== null && collation !== "C" && collation !== "default")
    throw new UnsupportedPlanError(`comparison under collation ${collation}`);
  if (type.kind === "enum" || type.kind === "json" || type.kind === "array")
    throw new UnsupportedPlanError(`${type.kind} comparison`);
  if (!COMPARABLE.has(layoutOf(type))) throw new UnsupportedPlanError(`${type.kind} comparison`);
}

export function compileCompare(
  op: CompareOp,
  left: Compiled,
  right: Compiled,
  collation: string | null,
  arena: OutputArena,
): Compiled {
  if (
    left.type.kind === "interval" &&
    (collation === null || collation === "C" || collation === "default")
  )
    return new IntervalCompare(op, left, right, arena);
  checkComparable(left.type, collation);
  if (layoutOf(left.type) !== layoutOf(right.type))
    throw new TypeError(`cannot compare ${left.type.kind} with ${right.type.kind}`);
  return new CompareNode(op, left, right, arena);
}

export function compileDistinctFrom(
  negated: boolean,
  left: Compiled,
  right: Compiled,
  arena: OutputArena,
): Compiled {
  if (left.type.kind === "interval") return new IntervalCompare("=", left, right, arena, negated);
  checkComparable(left.type, null);
  if (layoutOf(left.type) !== layoutOf(right.type))
    throw new TypeError(`cannot compare ${left.type.kind} with ${right.type.kind}`);
  return new DistinctFromNode(negated, left, right, arena);
}

export { slotOrder };
