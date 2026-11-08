/** Fold only literal bags. No scalar evaluation, casts, duplicate removal or relational work. */
import type { Rel, ScalarExpr } from "../ir/ir.js";

export function constantRows(rel: Rel): ReadonlyArray<readonly ScalarExpr[]> | null {
  const rows: Array<readonly ScalarExpr[]> = [];
  const stack: Rel[] = [rel];
  while (stack.length > 0) {
    const node = stack.pop();
    if (node === undefined) break;
    if (node.op === "setOp" && node.kind === "union" && node.all) {
      // A single append-only bag avoids copying a growing UNION ALL prefix for every arm.
      for (let i = node.inputs.length - 1; i >= 0; i--) {
        const input = node.inputs[i];
        if (input !== undefined) stack.push(input);
      }
    } else if (node.op === "values") {
      if (!node.rows.every((row) => row.every((expr) => expr.op === "literal"))) return null;
      for (const row of node.rows) rows.push(row);
    } else if (
      node.op === "project" &&
      node.exprs.every((expr) => expr.op === "literal" || expr.op === "column")
    ) {
      const input = constantRows(node.input);
      if (input === null) return null;
      for (const row of input)
        rows.push(
          node.exprs.map((expr) => {
            if (expr.op === "literal") return expr;
            const value = row[expr.index];
            if (value === undefined) throw new TypeError("Literal projection column out of range");
            return value;
          }),
        );
    } else return null;
  }
  return rows;
}
