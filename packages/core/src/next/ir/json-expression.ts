/** Expressions that require a resumable JSON evaluation frame. */
import type { ScalarExpr } from "./ir.js";

/** Such expressions need the owner even when a CASE/COALESCE only relays a JSON column. */
export function jsonExpression(expr: ScalarExpr): boolean {
  if (
    (expr.op === "case" || (expr.op === "call" && expr.fn === "coalesce")) &&
    expr.type.kind === "json"
  )
    return true;
  if (
    expr.op === "cast" &&
    expr.type.kind === "text" &&
    expr.input.type.kind === "json" &&
    expr.input.type.binary
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

/** Binary JSON canonicalization can expand; its frame needs checkpoint/re-admission. */
export function expandingJsonExpression(expr: ScalarExpr): boolean {
  if (
    expr.op === "cast" &&
    expr.type.kind === "text" &&
    expr.input.type.kind === "json" &&
    expr.input.type.binary
  )
    return true;
  if (
    expr.op === "cast" &&
    expr.type.kind === "json" &&
    expr.type.binary &&
    (expr.input.type.kind !== "json" || !expr.input.type.binary)
  )
    return true;
  if (
    expr.op === "call" &&
    (expr.fn === "json_get" || expr.fn === "json_get_text") &&
    expr.args[0]?.type.kind === "json" &&
    expr.args[0].type.binary
  )
    return true;
  for (const value of Object.values(expr)) {
    if (value === null || typeof value !== "object" || ArrayBuffer.isView(value)) continue;
    for (const child of Array.isArray(value) ? value : [value]) {
      if (child === null || typeof child !== "object") continue;
      if ("op" in child && expandingJsonExpression(child as ScalarExpr)) return true;
      if (
        "then" in child &&
        (expandingJsonExpression((child as { then: ScalarExpr }).then) ||
          expandingJsonExpression((child as { when: ScalarExpr }).when))
      )
        return true;
    }
  }
  return false;
}
