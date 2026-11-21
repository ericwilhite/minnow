/**
 * The input columns a scalar expression reads: the one walker for column demand (join output
 * demand, rule dependencies). The switch is exhaustive over `ScalarExpr`, so a new expression
 * kind fails to compile here until it is classified. An expression it cannot classify at run
 * time (a shape from outside the type) returns `null`: callers must then treat every input
 * column as read, never none.
 */
import type { ScalarExpr } from "./ir.js";

/** Adds every column `expr` reads to `into`; false when `expr` holds an unclassified kind. */
function collect(expr: ScalarExpr, into: Set<number>): boolean {
  switch (expr.op) {
    case "column":
      into.add(expr.index);
      return true;
    case "literal":
    case "param":
      return true;
    case "call":
    case "and":
    case "or":
      return expr.args.every((arg) => collect(arg, into));
    case "cast":
      return collect(expr.input, into);
    case "compare":
    case "distinctFrom":
      return collect(expr.left, into) && collect(expr.right, into);
    case "not":
    case "isNull":
      return collect(expr.arg, into);
    case "case":
      return (
        expr.branches.every((branch) => collect(branch.when, into) && collect(branch.then, into)) &&
        (expr.otherwise === null || collect(expr.otherwise, into))
      );
    case "inList":
      return collect(expr.arg, into) && expr.list.every((item) => collect(item, into));
    case "like":
      return collect(expr.arg, into) && collect(expr.pattern, into);
    default: {
      const unclassified: never = expr;
      void unclassified;
      return false;
    }
  }
}

/** Every input column the expressions read, or `null` (read every column) if any is unclassified. */
export function columnsRead(
  exprs: Iterable<ScalarExpr>,
  into = new Set<number>(),
): Set<number> | null {
  for (const expr of exprs) if (!collect(expr, into)) return null;
  return into;
}
