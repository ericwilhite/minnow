/** Conservative first late-materialization shape. Other sorts retain the ordinary recipe. */
import type { Rel } from "../ir/ir.js";
import { samePhysicalType } from "../types.js";
import { BATCH_ROWS } from "../vector/vector.js";
import type { SortColumn } from "./sort-keys.js";

export interface LateSortShape {
  readonly scan: Extract<Rel, { op: "scan" }>;
  readonly project: Extract<Rel, { op: "project" }>;
  readonly narrow: Extract<Rel, { op: "scan" }>;
  readonly keys: readonly SortColumn[];
  readonly locator: number;
}

export function lateSortShape(rel: Extract<Rel, { op: "sort" }>): LateSortShape | null {
  if (
    rel.withTies ||
    rel.limit?.kind !== "const" ||
    rel.limit.value < 1 ||
    rel.limit.value > BATCH_ROWS
  )
    return null;
  const project = rel.input;
  if (project.op !== "project" || project.input.op !== "scan") return null;
  const scan = project.input;
  if (
    scan.filter !== null ||
    scan.runtimeFilters.length !== 0 ||
    scan.emitLocator ||
    scan.columns.length < 16
  )
    return null;
  if (
    project.exprs.length !== scan.columns.length ||
    project.output.length !== scan.output.length ||
    project.exprs.some(
      (expr, k) =>
        expr.op !== "column" ||
        expr.index !== k ||
        !scan.output[k] ||
        !samePhysicalType(expr.type, scan.output[k].type),
    )
  )
    return null;
  if (rel.keys.length === 0 || rel.keys.length > 4) return null;
  const indices: number[] = [],
    keys: SortColumn[] = [];
  for (const key of rel.keys) {
    const expr = key.expr;
    if (
      expr.op !== "column" ||
      !["bool", "int32", "int64", "float32", "float64", "date", "time", "timestamp"].includes(
        expr.type.kind,
      )
    )
      return null;
    const field = scan.output[expr.index];
    if (!field || !samePhysicalType(field.type, expr.type)) return null;
    let at = indices.indexOf(expr.index);
    if (at < 0) {
      at = indices.length;
      indices.push(expr.index);
    }
    keys.push({ column: at, direction: key.direction, nulls: key.nulls, collation: key.collation });
  }
  const output = indices.map((k) => {
    const column = scan.output[k];
    if (!column) throw new TypeError("missing narrow scan column");
    return column;
  });
  const locator = output.length;
  output.push({
    name: "$late_locator",
    type: { kind: "locator", tableId: scan.tableId },
    nullable: false,
  });
  const columns = indices.map((k) => {
    const id = scan.columns[k];
    if (id === undefined) throw new TypeError("missing narrow scan column id");
    return id;
  });
  return { scan, project, keys, locator, narrow: { ...scan, columns, output, emitLocator: true } };
}
