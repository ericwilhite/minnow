/** A single input row needs no group-boundary comparison; sort preflight stays unchanged. */
import type { Rel, ScalarExpr } from "../ir/ir.js";
export function singleGroupInput(rel: Rel): boolean {
  switch (rel.op) {
    case "values":
      return rel.rows.length <= 1;
    case "project":
    case "filter":
    case "sort":
    case "distinct":
    case "window":
      return singleGroupInput(rel.input);
    case "limit":
      return (rel.limit?.kind === "const" && rel.limit.value <= 1) || singleGroupInput(rel.input);
    case "aggregate":
      return rel.groupBy.length === 0 && rel.groupingSets === null;
    default:
      return false;
  }
}

/** Actual input-column positions consumed by each JSON aggregate helper lane. */
export interface JsonGroupConsumer {
  readonly keys: readonly number[];
  readonly columns: readonly number[];
  readonly boundary: boolean;
}
export function jsonGroupConsumer(
  rel: Extract<Rel, { op: "aggregate" }>,
  slot: number,
): JsonGroupConsumer | null {
  if (slot === 0) return { keys: rel.groupBy, columns: rel.groupBy, boundary: true };
  if (slot === 1 && rel.groupBy.length)
    return { keys: rel.groupBy, columns: rel.input.output.map((_, i) => i), boundary: false };
  let next = rel.groupBy.length ? 2 : 1;
  for (const call of rel.aggregates) {
    if (!["json_agg", "jsonb_agg", "json_object_agg", "jsonb_object_agg"].includes(call.fn))
      continue;
    const column = (expr: ScalarExpr): number => {
      if (expr.op !== "column") throw new Error("JSON sort input must be projected");
      return expr.index;
    };
    const order = call.orderBy.map((k) => column(k.expr));
    const args = call.args.map(column);
    if (call.fn.includes("object")) {
      if (order.length && next++ === slot)
        return { keys: order, columns: [...args, ...order], boundary: false };
      // Object dedup sorts TEXT field names and ordinals, never structural input values.
      if ((call.fn === "jsonb_object_agg" || call.json?.unique) && next++ === slot) return null;
    } else if ((call.distinct || order.length) && next++ === slot)
      return {
        keys: call.distinct ? args.slice(0, 1) : order,
        columns: [...args, ...order],
        boundary: false,
      };
  }
  return null;
}

/** Actual outer sort/window keys; decoder workspace belongs to that native consumer. */
export interface JsonStructuralSort {
  readonly input: Rel;
  readonly keys: readonly ScalarExpr[];
}
export function jsonStructuralSort(rel: Rel): JsonStructuralSort | null {
  if (rel.op !== "sort" && rel.op !== "window") return null;
  const keys: readonly ScalarExpr[] =
    rel.op === "sort"
      ? rel.keys.map((k) => k.expr)
      : [
          ...rel.partitionBy.map((index): ScalarExpr => {
            const column = rel.input.output[index];
            if (!column) throw new RangeError("Window partition column outside input");
            return { op: "column", index, type: column.type };
          }),
          ...rel.orderBy.map((k) => k.expr),
        ];
  const structural = keys.filter((k) => k.type.kind === "json" || k.type.kind === "array");
  return structural.length ? { input: rel.input, keys: structural } : null;
}
