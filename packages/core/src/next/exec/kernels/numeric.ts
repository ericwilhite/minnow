/**
 * Exact numeric value access shared by the arithmetic, comparison and cast kernels: operands in
 * their stride form, int64-family values as bigint, decimal slots as (coefficient, scale, special).
 */
import { int128At } from "../../vector/int128.js";
import type { FlatVector, Int64Values, Validity, Vector, VectorData } from "../../vector/vector.js";

export type DecimalData = Extract<VectorData, { layout: "decimal" }>;

/** 10^0 … 10^15 as doubles (all exact). */
export const POW10: Float64Array = Float64Array.from({ length: 16 }, (_, k) => 10 ** k);

/**
 * An operand as kernels read it: a flat vector read at slot `row * stride`. A constant is its
 * length-1 value with stride 0; a dictionary vector is materialized first (`Materializer`).
 */
export interface Operand {
  readonly flat: FlatVector;
  readonly stride: 0 | 1;
  /** Validity to combine (a constant's is all-or-nothing: see `constantIsNull`). */
  readonly validity: Validity | null;
}

export function constantIsNull(vector: Vector): boolean {
  return (
    vector.form === "constant" &&
    vector.value.validity !== null &&
    ((vector.value.validity[0] ?? 0) & 1) === 0
  );
}

/** The bigint at `index` of int64-family values. */
export function bigAt(values: Int64Values, index: number): bigint {
  return values.repr === "safe" ? BigInt(values.values[index] ?? 0) : (values.values[index] ?? 0n);
}

/** Scale of decimal slot `index`. */
export function scaleAt(data: DecimalData, index: number): number {
  return data.scale ?? data.scales?.[index] ?? 0;
}

/** Coefficient of decimal slot `index`, any tier. */
export function coefficientAt(data: DecimalData, index: number): bigint {
  const c = data.coefficients;
  switch (c.repr) {
    case "safe":
      return BigInt(c.values[index] ?? 0);
    case "int64":
      return c.values[index] ?? 0n;
    case "int128":
      return int128At(c.limbs, index);
    case "wide":
      return c.values[index] ?? 0n;
  }
}

/** `DecimalSpecial` of slot `index` (0 finite, 1 NaN, 2 +Infinity, 3 −Infinity). */
export function specialAt(data: DecimalData, index: number): number {
  return data.specials?.[index] ?? 0;
}

export function pow10(exponent: number): bigint {
  return 10n ** BigInt(exponent);
}
