/** Snapshot RETURNING parameters with mutation input, so writer phases need no producer bindings. */
import type { ScalarExpr, Rel } from "../ir/ir.js";
import type { LoweredWrite } from "./modify.js";
import type { PlanBuilder } from "./plan.js";

export function snapshotReturningParameters(
  write: LoweredWrite,
  builder: PlanBuilder,
): LoweredWrite {
  const rel = write.rel;
  if (rel.op !== "modify" || rel.returning === null) return write;
  const table = builder.catalog.table(rel.tableId);
  if (!table) throw new TypeError("Missing RETURNING table");
  const parameters: Array<Extract<ScalarExpr, { op: "param" }>> = [];
  const slots = new Map<number, number>();
  const map = (expr: ScalarExpr): ScalarExpr => {
    switch (expr.op) {
      case "param": {
        let position = slots.get(expr.slot);
        if (position === undefined) {
          position = parameters.length;
          slots.set(expr.slot, position);
          parameters.push(expr);
        }
        return {
          op: "column",
          index: table.columns.length + (rel.passthrough ?? 0) + position,
          type: expr.type,
        };
      }
      case "literal":
      case "column":
        return expr;
      case "cast":
        return { ...expr, input: map(expr.input) };
      case "and":
      case "or":
      case "call":
        return { ...expr, args: expr.args.map(map) };
      case "compare":
      case "distinctFrom":
        return { ...expr, left: map(expr.left), right: map(expr.right) };
      case "not":
      case "isNull":
        return { ...expr, arg: map(expr.arg) };
      case "case":
        return {
          ...expr,
          branches: expr.branches.map((branch) => ({
            when: map(branch.when),
            then: map(branch.then),
          })),
          otherwise: expr.otherwise === null ? null : map(expr.otherwise),
        };
      case "inList":
        return { ...expr, arg: map(expr.arg), list: expr.list.map(map) };
      case "like":
        return { ...expr, arg: map(expr.arg), pattern: map(expr.pattern) };
    }
  };
  const returning = rel.returning.map(map);
  if (parameters.length === 0) return write;
  const input: Rel = {
    op: "project",
    id: builder.id(),
    input: rel.input,
    exprs: [
      ...rel.input.output.map((col, index): ScalarExpr => ({
        op: "column",
        index,
        type: col.type,
      })),
      ...parameters,
    ],
    output: [
      ...rel.input.output,
      ...parameters.map((expr, index) => ({
        name: `(returning parameter ${String(index)})`,
        type: expr.type,
        nullable: true,
      })),
    ],
  };
  return {
    ...write,
    rel: { ...rel, input, returning, passthrough: (rel.passthrough ?? 0) + parameters.length },
  };
}
