/**
 * The executor's scalar kernel table, as data lowering can import (lower/ may not import exec/):
 * which `call` functions, comparisons and casts the kernels implement, by argument kind. Lowering
 * resolves overloads against it and inserts casts so arguments arrive in exactly these kinds;
 * anything absent is unsupported (lowering refuses it, or the executor throws
 * `UnsupportedPlanError` at prepare).
 *
 * exec/kernels/functions.test.ts compiles every entry, so the table cannot promise a kernel that
 * does not exist. Owner: executor (B4); additions only.
 */
import type { PhysicalKind } from "../../types.js";

export interface FunctionSignature {
  readonly fn: string;
  /** Argument kinds, all equal for the arithmetic; `variadic` repeats the single kind. */
  readonly args: readonly PhysicalKind[];
  readonly variadic: boolean;
  /**
   * The result kind. Arithmetic keeps its argument type exactly (an int32's `bits`, a decimal's
   * display scale computed per row: max for add/sub, the sum for mul).
   */
  readonly result: PhysicalKind;
}

const NUMERIC_ARITH: ReadonlyArray<readonly [PhysicalKind, readonly string[]]> = [
  ["int32", ["add", "sub", "mul", "div", "mod", "neg"]],
  ["int64", ["add", "sub", "mul", "div", "mod", "neg"]],
  ["float64", ["add", "sub", "mul", "div", "neg"]],
  ["float32", ["add", "sub", "mul", "div", "neg"]],
  ["decimal", ["add", "sub", "mul", "neg"]],
];

/** Kinds whose values `compare`, `distinctFrom`, `inList`, `coalesce` and `nullif` accept. */
export const COMPARABLE_KINDS: readonly PhysicalKind[] = [
  "bool",
  "int32",
  "int64",
  "float64",
  "float32",
  "decimal",
  "date",
  "time",
  "timestamp",
  "text",
  "bytes",
  "uuid",
];

/** Kinds CASE and COALESCE can produce. */
export const CASE_RESULT_KINDS: readonly PhysicalKind[] = [
  ...COMPARABLE_KINDS,
  "json",
  "enum",
  "interval",
];

export const SCALAR_FUNCTIONS: readonly FunctionSignature[] = [
  ...NUMERIC_ARITH.flatMap(([kind, fns]) =>
    fns.map((fn) => ({
      fn,
      args: fn === "neg" ? [kind] : [kind, kind],
      variadic: false,
      result: kind,
    })),
  ),
  ...(["is_true", "is_not_true", "is_false", "is_not_false"] as const).map((fn) => ({
    fn,
    args: ["bool"] as const,
    variadic: false,
    result: "bool" as const,
  })),
  ...COMPARABLE_KINDS.map((kind) => ({
    fn: "nullif",
    args: [kind, kind],
    variadic: false,
    result: kind,
  })),
  ...CASE_RESULT_KINDS.map((kind) => ({
    fn: "coalesce",
    args: [kind],
    variadic: true,
    result: kind,
  })),
];

/** Casts the kernels implement (`cast` nodes), as [from, to]. */
export const CASTS: ReadonlyArray<readonly [PhysicalKind, PhysicalKind]> = [
  ...(["int32", "int64", "float64", "float32", "decimal"] as const).flatMap((from) =>
    (["int32", "int64", "float64", "float32", "decimal"] as const).map((to) => [from, to] as const),
  ),
  ["date", "timestamp"],
  ["timestamp", "date"],
  ["timestamp", "timestamp"],
];

/** The signature of `fn` over `args`, or undefined when no kernel exists. */
export function findFunction(
  fn: string,
  args: readonly PhysicalKind[],
): FunctionSignature | undefined {
  return SCALAR_FUNCTIONS.find(
    (signature) =>
      signature.fn === fn &&
      (signature.variadic
        ? args.length >= 1 && args.every((kind) => kind === signature.args[0])
        : signature.args.length === args.length &&
          signature.args.every((kind, k) => kind === args[k])),
  );
}
