/** Per-value admission provenance. Unknown expanding expressions retain the legacy recipe.
 * Walks only plan metadata: no evaluation, caller-value reads or JSON/data allocation. */
import { constantNull, constantBool } from "./expression-reach.js";
import type { Rel, ScalarExpr } from "../ir/ir.js";
export interface InputSizes {
  parameter(slot: number): number;
  source(id: number, column: number, coefficient: number): number | null;
  saved?(id: number, column: number): number | undefined;
}
const sum = (values: ReadonlyArray<number | null>): number | null =>
  values.some((value) => value === null)
    ? null
    : values.reduce<number>((n, value) => n + (value ?? 0), 0);
const maximum = (values: ReadonlyArray<number | null>): number | null =>
  values.some((value) => value === null) ? null : Math.max(0, ...(values as number[]));
const variable = (kind: string) =>
  ["text", "bytes", "json", "array", "decimal", "enum"].includes(kind);
export function expressionInputBytes(
  expr: ScalarExpr,
  input: Rel,
  sizes: InputSizes,
  coefficient = 1,
): number | null {
  if (!variable(expr.type.kind)) return 16 * coefficient;
  switch (expr.op) {
    case "column":
      return columnInputBytes(input, expr.index, sizes, coefficient);
    case "param":
      return coefficient * sizes.parameter(expr.slot);
    case "literal": {
      if (expr.value.validity && ((expr.value.validity[0] ?? 0) & 1) === 0) return 0;
      const data = expr.value.data;
      if (data.layout === "decimal" && data.coefficients.repr === "wide") return null;
      return (
        coefficient *
        (data.layout === "varlen" ? (data.offsets[1] ?? 0) - (data.offsets[0] ?? 0) : 16)
      );
    }
    case "cast":
      return expr.input.type.kind === expr.type.kind
        ? expressionInputBytes(expr.input, input, sizes, coefficient)
        : null;
    case "case": {
      const values: Array<number | null> = [];
      let otherwise = true;
      for (const branch of expr.branches) {
        const condition = constantBool(branch.when);
        if (condition === false || condition === null) continue;
        values.push(expressionInputBytes(branch.then, input, sizes, coefficient));
        if (condition === true) {
          otherwise = false;
          break;
        }
      }
      if (otherwise && expr.otherwise)
        values.push(expressionInputBytes(expr.otherwise, input, sizes, coefficient));
      return maximum(values);
    }
    case "call": {
      const first = expr.args[0];
      if (["upper", "lower"].includes(expr.fn))
        return first ? expressionInputBytes(first, input, sizes, 3 * coefficient) : 0;
      if (["nullif", "json_get", "json_get_text"].includes(expr.fn))
        return first ? expressionInputBytes(first, input, sizes, coefficient) : 0;
      if (expr.fn === "coalesce") {
        const values: Array<number | null> = [];
        for (const arg of expr.args) {
          const isNull = constantNull(arg);
          if (isNull !== true) values.push(expressionInputBytes(arg, input, sizes, coefficient));
          if (isNull === false) break;
        }
        return maximum(values);
      }
      if (["concat", "textcat"].includes(expr.fn))
        return sum(expr.args.map((arg) => expressionInputBytes(arg, input, sizes, coefficient)));
      return variable(expr.type.kind) ? null : 16 * coefficient;
    }
    default:
      return 16 * coefficient;
  }
}
export function columnInputBytes(
  rel: Rel,
  column: number,
  sizes: InputSizes,
  coefficient = 1,
): number | null {
  const type = rel.output[column]?.type;
  if (type && !variable(type.kind)) return 16 * coefficient;
  const saved = sizes.saved?.(rel.id, column);
  if (saved !== undefined) return coefficient * saved;
  switch (rel.op) {
    case "scan":
    case "keyLookup":
    case "indexRange": {
      const bytes = sizes.source(rel.id, column, coefficient);
      return bytes === null ? null : coefficient * bytes;
    }
    case "project": {
      const expr = rel.exprs[column];
      return expr ? expressionInputBytes(expr, rel.input, sizes, coefficient) : null;
    }
    case "values":
      return maximum(
        rel.rows.map((row) => {
          const expr = row[column];
          return expr ? expressionInputBytes(expr, rel, sizes, coefficient) : null;
        }),
      );
    case "filter":
    case "limit":
    case "sort":
    case "distinct":
      return columnInputBytes(rel.input, column, sizes, coefficient);
    case "fetch": {
      if (column < rel.input.output.length)
        return columnInputBytes(rel.input, column, sizes, coefficient);
      const bytes = sizes.source(rel.id, column - rel.input.output.length, coefficient);
      return bytes === null ? null : coefficient * bytes;
    }
    case "join":
      return column < rel.left.output.length
        ? columnInputBytes(rel.left, column, sizes, coefficient)
        : column < rel.left.output.length + rel.right.output.length
          ? columnInputBytes(rel.right, column - rel.left.output.length, sizes, coefficient)
          : 16 * coefficient;
    case "aggregate": {
      const key = rel.groupBy[column];
      if (key !== undefined) return columnInputBytes(rel.input, key, sizes, coefficient);
      const call = rel.aggregates[column - rel.groupBy.length];
      return call && ["min", "max"].includes(call.fn) && call.args[0]
        ? expressionInputBytes(call.args[0], rel.input, sizes, coefficient)
        : null;
    }
    case "setOp":
      return maximum(
        rel.inputs.map((child) => columnInputBytes(child, column, sizes, coefficient)),
      );
    case "window":
      return column < rel.input.output.length
        ? columnInputBytes(rel.input, column, sizes, coefficient)
        : variable(rel.output[column]?.type.kind ?? "")
          ? null
          : 16 * coefficient;
    case "series":
      return 16 * coefficient;
    default:
      return null;
  }
}
/** Sum physical projections; source prefixes have a separate complete reservation. */
export function projectedInputBytes(
  exprs: readonly ScalarExpr[],
  input: Rel,
  sizes: InputSizes,
): number | null {
  return sum(exprs.map((expr) => expressionInputBytes(expr, input, sizes)));
}
