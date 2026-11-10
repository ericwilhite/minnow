/** Expressions that require a resumable, input-bounded JSON evaluation frame. */
import type { ScalarExpr } from "./ir.js";

/** Such expressions need the owner even when a CASE/COALESCE only relays a JSON column. */
export function jsonExpression(expr: ScalarExpr): boolean {
  if (
    (expr.op === "case" || (expr.op === "call" && expr.fn === "coalesce")) &&
    expr.type.kind === "json"
  )
    return true;
  if (expr.op === "cast" && expr.type.kind === "json" && expr.input.type.kind === "text")
    return true;
  if (expr.op === "call" && (expr.fn.startsWith("json_") || expr.fn === "to_json")) return true;
  for (const value of Object.values(expr)) {
    if (value === null || typeof value !== "object") continue;
    if (Array.isArray(value)) {
      for (const item of value) {
        if (
          item !== null &&
          typeof item === "object" &&
          "op" in item &&
          jsonExpression(item as ScalarExpr)
        )
          return true;
        if (
          item !== null &&
          typeof item === "object" &&
          "then" in item &&
          (jsonExpression((item as { then: ScalarExpr }).then) ||
            jsonExpression((item as { when: ScalarExpr }).when))
        )
          return true;
      }
    } else if ("op" in value && jsonExpression(value as ScalarExpr)) return true;
  }
  return false;
}
