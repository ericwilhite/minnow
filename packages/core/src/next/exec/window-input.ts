/** Compact only proven column aliases, preserving every logical sort/window output position.
 * IR authority: column references are positional, immutable reads (ir/ir.ts). A projection's
 * repeated reference to the same input/type is the same value and validity bitmap. Computed
 * expressions remain distinct: neither equality of values nor volatility is inferred here.
 */
import type { ScalarExpr } from "../ir/ir.js";
import { samePhysicalType } from "../types.js";
import type { WindowFunctionSpec } from "./window.js";

export function windowInputLayout(
  exprs: readonly ScalarExpr[],
  projection: readonly ScalarExpr[] | null,
  enabled = true,
) {
  const compact: ScalarExpr[] = [];
  const restore: number[] = [];
  const roots: number[] = [];
  for (const expr of exprs) {
    let root = -1;
    if (enabled && expr.op === "column") {
      root = expr.index;
      const projected = projection?.[expr.index];
      if (projected?.op === "column") {
        const first = projection?.findIndex(
          (value) =>
            value.op === "column" &&
            value.index === projected.index &&
            samePhysicalType(value.type, projected.type),
        );
        if (first !== undefined && first >= 0) root = first;
      }
    }
    const found =
      root < 0
        ? -1
        : compact.findIndex(
            (previous, at) => roots[at] === root && samePhysicalType(previous.type, expr.type),
          );
    if (found >= 0) restore.push(found);
    else {
      restore.push(compact.length);
      roots.push(root);
      compact.push(expr);
    }
  }
  const index = (column: number): number => {
    const mapped = restore[column];
    if (mapped === undefined) throw new TypeError("window column outside its input layout");
    return mapped;
  };
  return { exprs: compact, types: compact.map((expr) => expr.type), restore, index };
}

export function remapWindowFunction(
  call: WindowFunctionSpec,
  index: (column: number) => number,
): WindowFunctionSpec {
  return {
    ...call,
    arg: call.arg === null ? null : index(call.arg),
    ...(typeof call.amount === "object" && call.amount !== null
      ? { amount: { column: index(call.amount.column) } }
      : {}),
  };
}

/** A non-null int4 literal is already an exact scalar; never materialize it once per row. */
export function literalWindowAmount(expr: ScalarExpr | undefined): number | undefined {
  if (expr?.op !== "literal" || expr.type.kind !== "int32") return undefined;
  const vector = expr.value;
  if (
    vector.data.layout !== "i32" ||
    (vector.validity !== null && ((vector.validity[0] ?? 0) & 1) === 0)
  )
    return undefined;
  return vector.data.values[0];
}
