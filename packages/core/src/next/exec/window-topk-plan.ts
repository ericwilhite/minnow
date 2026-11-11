/** A sole immutable rank upper-bound can discard rows before window materialization.
 * PostgreSQL rank counts preceding ROWS (with all boundary peers), not distinct values.
 * Keep the actual filter; this helper only provides an additional executor bound.
 */
import type { Rel, ScalarExpr } from "../ir/ir.js";

function integer(expr: ScalarExpr): number | null {
  if (expr.op !== "literal" || expr.value.validity !== null) return null;
  const data = expr.value.data;
  const value =
    data.layout === "i32"
      ? data.values[0]
      : data.layout === "int64"
        ? data.values.values[0]
        : undefined;
  const n = typeof value === "bigint" ? Number(value) : value;
  return n !== undefined && Number.isSafeInteger(n) && n > 0 && n <= 1_000_000 ? n : null;
}

export function rankingBounds(root: Rel): ReadonlyMap<number, number> {
  const bounds = new Map<number, number>();
  const uses = new Map<number, number>();
  const visit = (rel: Rel): void => {
    if (rel.op === "window") uses.set(rel.id, (uses.get(rel.id) ?? 0) + 1);
    if (rel.op === "filter" && rel.predicate.op === "compare") {
      const predicate = rel.predicate;
      const left = predicate.cmp === "<=" || predicate.cmp === "<";
      const right = predicate.cmp === ">=" || predicate.cmp === ">";
      const column = left ? predicate.left : predicate.right;
      const limit = left ? predicate.right : predicate.left;
      const n = integer(limit);
      if ((left || right) && column.op === "column" && n !== null) {
        let input = rel.input,
          index = column.index;
        // Even an unrelated computed projection could throw or have an effect on discarded
        // rows. Only direct, row-preserving aliases may lie between filter and window.
        while (input.op === "project" && input.exprs.every((expr) => expr.op === "column")) {
          const expr = input.exprs[index];
          if (expr?.op !== "column") break;
          index = expr.index;
          input = input.input;
        }
        if (
          input.op === "window" &&
          input.functions.length === 1 &&
          index === input.input.output.length &&
          input.functions[0]?.frame === null &&
          (input.functions[0].fn === "rank" || input.functions[0].fn === "row_number")
        ) {
          const k = n - (predicate.cmp === "<" || predicate.cmp === ">" ? 1 : 0);
          if (k > 0) bounds.set(input.id, Math.min(bounds.get(input.id) ?? k, k));
        }
      }
    }
    if ("input" in rel) visit(rel.input);
    if ("left" in rel) visit(rel.left);
    if ("right" in rel) visit(rel.right);
    if ("inputs" in rel) for (const input of rel.inputs) visit(input);
  };
  visit(root);
  for (const [id, count] of uses) if (count !== 1) bounds.delete(id);
  return bounds;
}
