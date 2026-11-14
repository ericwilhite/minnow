/** Sizing-only reach facts: never evaluate expressions or inspect caller parameters. */
import type { ScalarExpr } from "../ir/ir.js";
export function constantNull(e: ScalarExpr): boolean | null {
  if (e.op === "cast") return constantNull(e.input);
  if (e.op === "call" && e.fn === "coalesce") {
    let unknown = false;
    for (const arg of e.args) {
      const value = constantNull(arg);
      if (value === false) return false;
      unknown ||= value === null;
    }
    return unknown ? null : true;
  }
  if (e.op === "call" && e.fn === "nullif" && e.args[0]) {
    const first = constantNull(e.args[0]);
    return first === true || (e.args[1] && constantNull(e.args[1]) === true) ? first : null;
  }
  if (e.op !== "literal") return null;
  return e.value.validity !== null && ((e.value.validity[0] ?? 0) & 1) === 0;
}
export function constantBool(e: ScalarExpr): boolean | null | undefined {
  if (e.op !== "literal" || e.type.kind !== "bool") return undefined;
  if (constantNull(e)) return null;
  return e.value.data.layout === "bool" ? e.value.data.values[0] !== 0 : undefined;
}
