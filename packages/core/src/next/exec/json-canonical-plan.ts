/** Only an executor-owned canonical producer permits a zero-copy JSONB -> TEXT cast. */
import type { Rel, ScalarExpr } from "../ir/ir.js";

function canonical(expr: ScalarExpr, input: Rel): boolean {
  if (expr.type.kind !== "json" || !expr.type.binary) return false;
  if (expr.op === "literal")
    return expr.value.validity !== null && ((expr.value.validity[0] ?? 0) & 1) === 0;
  if (expr.op === "column") return canonicalColumn(input, expr.index);
  if (expr.op === "cast")
    return (
      expr.input.type.kind !== "json" || !expr.input.type.binary || canonical(expr.input, input)
    );
  if (expr.op === "call")
    return (
      ["to_jsonb", "jsonb_build_object", "jsonb_build_array"].includes(expr.fn) ||
      (expr.fn === "coalesce" && expr.args.every((arg) => canonical(arg, input)))
    );
  if (expr.op === "case")
    return (
      expr.branches.every((b) => canonical(b.then, input)) &&
      (expr.otherwise === null || canonical(expr.otherwise, input))
    );
  return false;
}
function canonicalColumn(rel: Rel, index: number): boolean {
  switch (rel.op) {
    case "aggregate":
      return (
        ["jsonb_agg", "jsonb_object_agg"].includes(
          rel.aggregates[index - rel.groupBy.length]?.fn ?? "",
        ) ||
        (index < rel.groupBy.length && canonicalColumn(rel.input, rel.groupBy[index] ?? -1))
      );
    case "project": {
      const expr = rel.exprs[index];
      return expr !== undefined && canonical(expr, rel.input);
    }
    case "filter":
    case "limit":
    case "sort":
    case "distinct":
      return canonicalColumn(rel.input, index);
    case "window":
      return index < rel.input.output.length && canonicalColumn(rel.input, index);
    case "join":
      return index < rel.left.output.length
        ? canonicalColumn(rel.left, index)
        : canonicalColumn(rel.right, index - rel.left.output.length);
    case "setOp":
      return rel.inputs.every((child) => canonicalColumn(child, index));
    default:
      return false;
  }
}
/** Recompute the proof; a caller-supplied annotation never certifies raw scan/literal bytes. */
export function canonicalJsonText(expr: ScalarExpr, input: Rel): ScalarExpr {
  const map = (value: unknown): unknown => {
    if (value === null || typeof value !== "object" || ArrayBuffer.isView(value)) return value;
    if (Array.isArray(value)) return value.map(map);
    if ("op" in value) return canonicalJsonText(value as ScalarExpr, input);
    return Object.fromEntries(Object.entries(value).map(([key, child]) => [key, map(child)]));
  };
  if (expr.op === "column" || expr.op === "param" || expr.op === "literal") return expr;
  const out = Object.fromEntries(
    Object.entries(expr)
      .filter(([key]) => key !== "jsonCanonical")
      .map(([key, value]) => [key, key === "type" ? value : map(value)]),
  ) as unknown as ScalarExpr;
  return out.op === "cast" &&
    out.type.kind === "text" &&
    out.type.maxLength === undefined &&
    out.input.type.kind === "json" &&
    (!out.input.type.binary || canonical(out.input, input))
    ? { ...out, jsonCanonical: true }
    : out;
}
