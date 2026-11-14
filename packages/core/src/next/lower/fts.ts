/** Full-text lowering: row-local MATCH and unfiltered snapshot-corpus BM25. */
import type { Expression } from "../../plan/model.js";
import type { Rel, ScalarExpr } from "../ir/ir.js";
import type { Typed } from "./coerce.js";
import type { Scope } from "./expressions.js";
import { lowerExpression } from "./expressions.js";
import { textLiteral } from "./literals.js";
import {
  andOf,
  mapColumns,
  outputColumns,
  type PlanBuilder,
  type TableRead,
  type RelColumn,
} from "./plan.js";
import { PlanningError } from "./errors.js";

export class FullTextScope {
  used = false;
  readonly calls: Array<{ index: number; columns: number[]; query: ScalarExpr }> = [];
  constructor(
    readonly builder: PlanBuilder,
    readonly table: TableRead | undefined,
    readonly wildcard: () => Typed[],
    readonly scannedBase = true,
  ) {}
  /** All statement sources share this boundary: score the original snapshot rows before
   * WHERE, including UPDATE/DELETE predicates, then expose ordinary positional columns. */
  finish<
    T extends {
      readonly rel: Rel;
      readonly columns: readonly RelColumn[];
      readonly layout: readonly number[];
    },
  >(base: T, where: readonly Typed[]) {
    let rel = base.rel;
    let columns = base.columns;
    let layout = base.layout;
    const positions = new Map(layout.map((index, position) => [index, position]));
    const place = (index: number): number => {
      const position = positions.get(index);
      if (position === undefined) throw new PlanningError("FTS source column vanished", "XX000");
      return position;
    };
    if (this.calls.length) {
      const start = columns.length;
      for (const [i, call] of this.calls.entries()) positions.set(call.index, start + i);
      columns = [
        ...columns,
        ...this.calls.map((): RelColumn => ({
          name: "(bm25)",
          type: { kind: "float64" },
          nullable: true,
          big: false,
        })),
      ];
      layout = [...layout, ...this.calls.map((call) => call.index)];
      rel = {
        op: "fts",
        id: this.builder.id(),
        input: rel,
        calls: this.calls.map((call) => ({ columns: call.columns.map(place), query: call.query })),
        output: outputColumns(columns),
      };
    }
    const predicate = andOf(
      where.map((value) => ({ ...value, expr: mapColumns(value.expr, place) })),
    );
    if (predicate)
      rel = { op: "filter", id: this.builder.id(), input: rel, predicate, output: rel.output };
    return { ...base, rel, columns, layout };
  }
  lower(expression: Extract<Expression, { kind: "fts" }>, scope: Scope): Typed {
    this.used = true;
    if (!this.scannedBase) throw new TypeError("Full-text search requires a scanned base table");
    if (expression.columns === "*" && !this.table)
      throw new TypeError(
        "Full-text search over every column requires a single-table query; name the columns explicitly",
      );
    if (expression.op === "bm25" && !this.table)
      throw new TypeError("BM25 requires a single scanned base table");
    const columns =
      expression.columns === "*"
        ? this.wildcard().filter((value) => value.expr.type.kind !== "bool")
        : expression.columns.map((value) => {
            if (value.kind !== "column")
              throw new TypeError("Full-text search arguments must be column references");
            return lowerExpression(value, scope, this.builder.state);
          });
    if (columns.length === 0) throw new TypeError("Full-text search found no searchable columns");
    const parameter = expression.queryParameter;
    const query: ScalarExpr =
      parameter === undefined
        ? textLiteral(expression.query)
        : {
            op: "param",
            slot: this.builder.ftsParameter(parameter),
            type: { kind: "text" },
          };
    this.builder.ftsQueries.push(query);
    if (expression.op === "match")
      return {
        expr: {
          op: "call",
          fn: "fts_match",
          args: [query, ...columns.map((value) => value.expr)],
          type: { kind: "bool" },
        },
        nullable: true,
        big: false,
      };
    const index = -this.builder.id();
    this.calls.push({
      index,
      query,
      columns: columns.map((value) => {
        if (value.expr.op !== "column")
          throw new TypeError("BM25 arguments must be source columns");
        return value.expr.index;
      }),
    });
    return { expr: { op: "column", index, type: { kind: "float64" } }, nullable: true, big: false };
  }
}
