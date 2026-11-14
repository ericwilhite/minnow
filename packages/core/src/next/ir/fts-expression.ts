/** FTS requires a task-sliced document loop; scalar kernels remain demand-sensitive. */
import type { ScalarExpr } from "./ir.js";
export function ftsExpression(expr: ScalarExpr): boolean {
  if (expr.op === "call" && expr.fn === "fts_match") return true;
  for (const value of Object.values(expr)) {
    if (!value || typeof value !== "object" || ArrayBuffer.isView(value)) continue;
    if (Array.isArray(value)) {
      for (const item of value as readonly unknown[]) {
        if (!item || typeof item !== "object") continue;
        if ("op" in item && ftsExpression(item as ScalarExpr)) return true;
        if (
          "then" in item &&
          "when" in item &&
          (ftsExpression(item.then as ScalarExpr) || ftsExpression(item.when as ScalarExpr))
        )
          return true;
      }
    } else if ("op" in value && ftsExpression(value as ScalarExpr)) return true;
  }
  return false;
}
