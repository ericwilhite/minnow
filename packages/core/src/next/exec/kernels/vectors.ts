/**
 * Shared pieces of the expression kernels: the compiled-expression interface, output buffers
 * charged to a grant, active-row iteration, and slot copies across vector forms.
 *
 * ## Evaluation contract (`Compiled.evaluate`)
 * - Input: a batch and the rows to compute (`rows`, or every row 0 … length − 1 when null).
 * - Output: a vector of `batch.length` slots whose values and validity are defined at the
 *   computed rows only; other slots are unspecified. It may be an input column itself, a
 *   constant, or the node's own output buffer, which stays valid until the node's next
 *   `evaluate` (the caller copies what it keeps longer).
 * - Errors (overflow, division by zero) are raised only for computed rows that are valid; garbage
 *   under null or uncomputed slots never raises (vector/vector.ts "Value rules").
 * - Memory: every output buffer is allocated once, for `capacity` rows, from the compile-time
 *   `ByteCharge` (a grant), and reused for every batch. Kernels allocate nothing per row on their
 *   number paths. Per batch, a few nodes allocate small objects (a retyped vector's wrapper, a
 *   decimal cast's result wrapper is reused, a cache key array): scripts/next-kernels-bench.mts
 *   measures the bytes per batch. The exact paths that read values as BigInt (the `wide`
 *   decimal tier, int64 values past 2^53 in casts and comparisons) allocate per value; the
 *   int64/int128 decimal tiers do not (limbs.ts). SQL temporal text also uses bounded transient
 *   strings with precharged scratch and reusable ASCII output buffers.
 */
import type { PhysicalType } from "../../types.js";
import { BATCH_ROWS, dataBytes, allocateData } from "../../vector/vector.js";
import type { ByteCharge } from "../../vector/varlen.js";
import type {
  Batch,
  FlatVector,
  Selection,
  Validity,
  Vector,
  VectorData,
} from "../../vector/vector.js";

export interface Compiled {
  readonly type: PhysicalType;
  evaluate(batch: Batch, rows: Selection | null): Vector;
}

/** Rows 0 … 2047, the selection used when every row is active (read-only). */
export const IDENTITY: Uint16Array = Uint16Array.from({ length: BATCH_ROWS }, (_, k) => k);

/** The active row indexes: the selection's, or `IDENTITY` when every row is active. */
export function rowsIn(rows: Selection | null): Uint16Array {
  return rows === null ? IDENTITY : rows.rows;
}

/** How many rows are active. */
export function countIn(rows: Selection | null, length: number): number {
  return rows === null ? length : rows.count;
}

export function validAt(validity: Validity | null, row: number): boolean {
  return validity === null || ((validity[row >>> 5] ?? 0) & (1 << (row & 31))) !== 0;
}

/** Output buffers of compiled expressions, charged to one grant and freed together. */
export class OutputArena {
  #charged = 0;
  constructor(
    readonly charge: ByteCharge,
    readonly capacity: number,
    /** The caller retains borrowed branch values until downstream readiness. */
    public borrowVarlen = false,
  ) {}

  /** Adaptive projection steps avoid geometric over-allocation across disjoint prefixes. */
  exactVarlenGrowth = false;

  /** Charges `bytes` or throws a `RangeError` (compile-time minimums are admitted first). */
  take(bytes: number): void {
    if (!this.charge.allocate(bytes))
      throw new RangeError(`An expression's ${String(bytes)}-byte buffer was refused by its grant`);
    this.#charged += bytes;
  }

  /** A flat output vector of `type` for `capacity` rows, with its own validity buffer. */
  flat(type: PhysicalType, varlenBytes = 0): OutputVector {
    const words = (this.capacity + 31) >>> 5;
    this.take(dataBytes(type, this.capacity, varlenBytes) + 4 * words);
    const vector: FlatVector = {
      form: "flat",
      type,
      length: 0,
      validity: null,
      data: allocateData(type, this.capacity, varlenBytes),
    };
    return { vector, validity: new Uint32Array(words) };
  }

  validity(): Validity {
    const words = (this.capacity + 31) >>> 5;
    this.take(4 * words);
    return new Uint32Array(words);
  }

  /** Returns `bytes` previously taken (a buffer replaced by a larger one). */
  give(bytes: number): void {
    this.charge.free(bytes);
    this.#charged -= bytes;
  }

  get bytes(): number {
    return this.#charged;
  }

  close(): void {
    if (this.#charged > 0) this.charge.free(this.#charged);
    this.#charged = 0;
  }
}

export interface OutputVector {
  readonly vector: FlatVector;
  /** The buffer `vector.validity` points at when some row is null. */
  readonly validity: Validity;
}

/**
 * The validity of a binary result: null when both inputs are all-valid, else the AND of both
 * over the computed rows, written into `target`.
 */
export function combineValidity(
  left: Validity | null,
  right: Validity | null,
  target: Validity,
  length: number,
): Validity | null {
  if (left === null && right === null) return null;
  const words = (length + 31) >>> 5;
  if (left === null)
    target.set(right!.subarray(0, words)); // eslint-disable-line @typescript-eslint/no-non-null-assertion
  else if (right === null) target.set(left.subarray(0, words));
  else
    for (let word = 0; word < words; word++) target[word] = (left[word] ?? 0) & (right[word] ?? 0);
  return target;
}

/** Validity of a vector's slot `row`, whatever its form. */
export function isValidSlot(vector: Vector, row: number): boolean {
  switch (vector.form) {
    case "flat":
    case "dictionary":
      return validAt(vector.validity, row);
    case "constant":
      return validAt(vector.value.validity, 0);
  }
}

/** The flat vector and slot holding row `row`'s value (dictionary entries, constants). */
export function slotOf(vector: Vector, row: number): [FlatVector, number] {
  switch (vector.form) {
    case "flat":
      return [vector, row];
    case "constant":
      return [vector.value, 0];
    case "dictionary":
      return [vector.dictionary, vector.codes[row] ?? 0];
  }
}

/** The flat vector holding a vector's values: itself, a constant's value, a dictionary. */
export function flatOf(vector: Vector): FlatVector {
  return vector.form === "flat"
    ? vector
    : vector.form === "constant"
      ? vector.value
      : vector.dictionary;
}

/** The slot of `flatOf(vector)` holding row `row` (no allocation, unlike `slotOf`). */
export function slotIndex(vector: Vector, row: number): number {
  return vector.form === "flat" ? row : vector.form === "constant" ? 0 : (vector.codes[row] ?? 0);
}

/** Copies slot `from` of `source` into slot `to` of fixed-width `target` (same layout). */
export function copyFixedSlot(
  source: VectorData,
  from: number,
  target: VectorData,
  to: number,
): void {
  switch (target.layout) {
    case "bool":
    case "i32":
    case "f64":
    case "f32":
      (target.values as Float64Array)[to] = (source as typeof target).values[from] ?? 0;
      return;
    case "int64": {
      const s = (source as typeof target).values;
      if (target.values.repr === "int64")
        target.values.values[to] =
          s.repr === "int64" ? (s.values[from] ?? 0n) : BigInt(s.values[from] ?? 0);
      else if (s.repr === "safe") target.values.values[to] = s.values[from] ?? 0;
      else throw new RangeError("copyFixedSlot: an int64 value into a safe target");
      return;
    }
    case "fixed16": {
      const s = (source as typeof target).bytes;
      target.bytes.set(s.subarray(from * 16, from * 16 + 16), to * 16);
      return;
    }
    case "interval": {
      const s = source as typeof target;
      target.months[to] = s.months[from] ?? 0;
      target.days[to] = s.days[from] ?? 0;
      if (target.micros.repr === "int64")
        target.micros.values[to] =
          s.micros.repr === "int64"
            ? (s.micros.values[from] ?? 0n)
            : BigInt(s.micros.values[from] ?? 0);
      else if (s.micros.repr === "safe") target.micros.values[to] = s.micros.values[from] ?? 0;
      else throw new RangeError("copyFixedSlot: an int64 interval into a safe target");
      return;
    }
    case "decimal":
    case "varlen":
      throw new TypeError(`copyFixedSlot does not copy ${target.layout}`);
  }
}

/** Writes slot `row` of `validity` (a no-op when it stays all-valid and `valid`). */
export function setValidity(validity: Validity, row: number, valid: boolean): void {
  const word = row >>> 5;
  const bit = 1 << (row & 31);
  validity[word] = valid ? (validity[word] ?? 0) | bit : (validity[word] ?? 0) & ~bit;
}

/** A length-1 all-null constant of `type` (immutable; shared per compile). */
export function nullConstant(type: PhysicalType): Vector {
  const value: FlatVector = {
    form: "flat",
    type,
    length: 1,
    validity: new Uint32Array(1),
    data: allocateData(type, 1),
  };
  return { form: "constant", type, length: 0, value };
}
