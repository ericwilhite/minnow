/** A global window order survives column-only projections. Re-sorting it would evaluate
 * frames beyond a downstream LIMIT, unlike PostgreSQL's already-ordered WindowAgg path.
 * Partitioned grouped windows do not promise a global order and are deliberately excluded.
 */
import type { Rel, SortKey } from "../ir/ir.js";
export function windowSuppliesOrder(input: Rel, keys: readonly SortKey[]): boolean {
  let current = input;
  let wanted = keys;
  while (current.op === "project") {
    if (current.exprs.some((expr) => expr.op !== "column")) return false;
    const projected: SortKey[] = [];
    for (const key of wanted) {
      if (key.expr.op !== "column") return false;
      const expr = current.exprs[key.expr.index];
      if (expr?.op !== "column") return false;
      projected.push({ ...key, expr });
    }
    wanted = projected;
    current = current.input;
  }
  const sourceColumn = (input: Rel, index: number): string => {
    let source = input,
      column = index;
    while (source.op === "project") {
      const expression = source.exprs[column];
      if (expression?.op !== "column") break;
      column = expression.index;
      source = source.input;
    }
    // All keys compared here start at the same window input, so a shared source index
    // is sufficient only when every traversed projection column is direct.
    return `${String(source.id)}:${String(column)}`;
  };
  return (
    current.op === "window" &&
    current.partitionBy.length === 0 &&
    wanted.length <= current.orderBy.length &&
    wanted.every((key, i) => {
      const provided = current.orderBy[i];
      return (
        provided !== undefined &&
        key.expr.op === "column" &&
        provided.expr.op === "column" &&
        sourceColumn(current.input, key.expr.index) ===
          sourceColumn(current.input, provided.expr.index) &&
        key.expr.index < current.input.output.length &&
        key.direction === provided.direction &&
        key.nulls === provided.nulls &&
        key.collation === provided.collation
      );
    })
  );
}
