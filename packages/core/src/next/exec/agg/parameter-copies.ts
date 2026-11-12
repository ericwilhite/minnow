/** Parameter positions in a complete aggregate sort row, including repeated grouping keys.
 * Binding charges include each owned input once; native sorts copy every logical position.
 * Sparse provenance is plan metadata, never payload materialization or caller-buffer reads.
 */
import type { Rel, ScalarExpr } from "../../ir/ir.js";
type Counts = Map<number, number>;
function merge(out: Counts, input: Counts, max = false): void {
  for (const [slot, n] of input)
    out.set(slot, max ? Math.max(out.get(slot) ?? 0, n) : (out.get(slot) ?? 0) + n);
}
export function aggregateParameterCopies(
  input: Rel,
  groupBy: readonly number[],
): ReadonlyMap<number, number> {
  const column = (rel: Rel, index: number): Counts => {
    if (rel.op === "project") {
      const e = rel.exprs[index];
      return e ? expr(e, rel.input) : new Map<number, number>();
    }
    if (rel.op === "values") {
      const out: Counts = new Map<number, number>();
      for (const row of rel.rows) {
        const e = row[index];
        if (e) merge(out, expr(e, rel), true);
      }
      return out;
    }
    if (rel.op === "join")
      return index < rel.left.output.length
        ? column(rel.left, index)
        : column(rel.right, index - rel.left.output.length);
    if (rel.op === "aggregate") {
      const key = rel.groupBy[index];
      return key === undefined ? new Map<number, number>() : column(rel.input, key);
    }
    if (rel.op === "setOp") {
      const out: Counts = new Map<number, number>();
      for (const child of rel.inputs) merge(out, column(child, index), true);
      return out;
    }
    if (
      (rel.op === "filter" ||
        rel.op === "limit" ||
        rel.op === "sort" ||
        rel.op === "distinct" ||
        rel.op === "window") &&
      index < rel.input.output.length
    )
      return column(rel.input, index);
    return new Map<number, number>();
  };
  const expr = (e: ScalarExpr, rel: Rel): Counts => {
    if (e.op === "param") return new Map([[e.slot, 1]]);
    if (e.op === "column") return column(rel, e.index);
    if (e.op === "cast") return expr(e.input, rel);
    const out: Counts = new Map<number, number>();
    if (e.op === "case") {
      for (const b of e.branches) merge(out, expr(b.then, rel), true);
      if (e.otherwise) merge(out, expr(e.otherwise, rel), true);
    } else if (e.op === "call")
      for (const a of e.args) merge(out, expr(a, rel), e.fn === "coalesce" || e.fn === "nullif");
    return out;
  };
  const repeats = new Map<number, number>();
  for (const key of groupBy) repeats.set(key, (repeats.get(key) ?? 0) + 1);
  const out: Counts = new Map<number, number>();
  for (const [index, c] of input.output.entries()) {
    if (!["text", "bytes", "json", "array", "enum", "decimal"].includes(c.type.kind)) continue;
    const counts = column(input, index),
      copies = Math.max(1, repeats.get(index) ?? 1);
    for (const [slot, n] of counts) out.set(slot, (out.get(slot) ?? 0) + n * copies);
  }
  return out;
}
