/**
 * Window functions: the front end's `windowed` source (an inner block computing every
 * partition, order and argument expression as a named column, plus `WindowSpec`s naming them) as
 * IR `window` operators. Specs with the same PARTITION BY and ORDER BY share one operator; the
 * result is the inner block's columns, then one column per spec, named by its alias.
 *
 * Types (PostgreSQL): row_number, rank, dense_rank are bigint (rendered `number`, R17.1);
 * percent_rank and cume_dist double precision; ntile integer; lag, lead, first_value,
 * last_value and nth_value their argument's type; aggregates as their aggregate forms (SUM over
 * int2/int4 bigint, over int8 exact numeric rendered bigint; AVG of integers and floats double
 * precision, C11). A frame is null when the SQL names none: PostgreSQL's default (RANGE UNBOUNDED
 * PRECEDING to CURRENT ROW with ORDER BY, the whole partition without).
 */
import type { ScalarExpr, SortKey, WindowCall, WindowFrameBound } from "../ir/ir.js";
import { aggregateArgument, aggregateResult } from "../ir/functions.js";
import type { PhysicalType } from "../types.js";
import type { QueryValue, TableSource, WindowFrame, WindowSpec } from "../../plan/model.js";
import { coerce, type Typed } from "./coerce.js";
import { PlanningError, unsupported } from "./errors.js";
import { lowerExpression } from "./expressions.js";
import type { BlockLowerer, OuterResolver } from "./from.js";
import { integerLiteral } from "./literals.js";
import {
  at,
  column,
  outputColumns,
  type LoweredRel,
  type PlanBuilder,
  type RelColumn,
} from "./plan.js";

const INT8: PhysicalType = { kind: "int64" };
const INT4: PhysicalType = { kind: "int32", bits: 32 };
const FLOAT8: PhysicalType = { kind: "float64" };

const RANKING: Readonly<Record<string, string>> = {
  ROW_NUMBER: "row_number",
  RANK: "rank",
  DENSE_RANK: "dense_rank",
  PERCENT_RANK: "percent_rank",
  CUME_DIST: "cume_dist",
  NTILE: "ntile",
  LAG: "lag",
  LEAD: "lead",
  FIRST_VALUE: "first_value",
  LAST_VALUE: "last_value",
  NTH_VALUE: "nth_value",
  COUNT: "count",
  SUM: "sum",
  AVG: "avg",
  MIN: "min",
  MAX: "max",
};

function int(value: number, type: PhysicalType): ScalarExpr {
  const literal = integerLiteral(BigInt(Math.trunc(value)), type);
  if (literal === null)
    throw new PlanningError(`window offset ${String(value)} is out of range`, "22003");
  return literal;
}

/** A spec's argument column, its type, and the call's result type (see the module comment). */
function signature(
  spec: WindowSpec,
  argument: RelColumn | null,
): { fn: string; type: PhysicalType; nullable: boolean; big: boolean } {
  const name = RANKING[spec.name];
  if (name === undefined) throw unsupported("window", `window function ${spec.name.toLowerCase()}`);
  const arg = argument?.type ?? null;
  switch (name) {
    case "row_number":
    case "rank":
    case "dense_rank":
      return { fn: name, type: INT8, nullable: false, big: false };
    case "percent_rank":
    case "cume_dist":
      return { fn: name, type: FLOAT8, nullable: false, big: false };
    case "ntile":
      return { fn: name, type: INT4, nullable: true, big: false };
    case "count":
      return {
        fn: argument === null ? "count_star" : "count",
        type: INT8,
        nullable: false,
        big: false,
      };
  }
  if (arg === null)
    throw new PlanningError(`${spec.name.toLowerCase()} needs an argument`, "42601");
  // ir/functions.ts AGGREGATES (R19) types every aggregate.
  if (name === "sum") {
    const type = aggregateResult("sum", [arg]);
    if (type === null) throw new PlanningError(`function sum(${arg.kind}) does not exist`, "42883");
    return { fn: name, type, nullable: true, big: arg.kind === "int64" };
  }
  if (name === "avg") {
    const type = aggregateResult("avg", [arg]);
    if (type === null) throw new PlanningError(`function avg(${arg.kind}) does not exist`, "42883");
    return { fn: name, type, nullable: true, big: false };
  }
  if (name === "min" || name === "max") {
    if (aggregateResult(name, [arg]) === null)
      throw new PlanningError(`function ${name}(${arg.kind}) does not exist`, "42883");
  }
  return { fn: name, type: arg, nullable: true, big: argument?.big ?? false };
}

function fallbackOf(
  value: QueryValue | undefined,
  type: PhysicalType,
  builder: PlanBuilder,
): ScalarExpr | null {
  if (value === undefined) return null;
  const typed = lowerExpression(
    { kind: "literal", value },
    { column: () => undefined },
    builder.state,
  );
  return coerce(typed, type, "implicit", builder.parameters).expr;
}

function frameOf(
  frame: WindowFrame | undefined,
  orderType: PhysicalType | null,
  builder: PlanBuilder,
): WindowCall["frame"] {
  if (frame === undefined) return null;
  const bound = (input: WindowFrame["start"]): WindowFrameBound => {
    if (input.offset === undefined) return { kind: input.kind, offset: null };
    if (frame.unit !== "range") return { kind: input.kind, offset: int(input.offset, INT8) };
    if (orderType === null)
      throw new PlanningError("RANGE with offset requires exactly one ORDER BY column", "42P20");
    // PostgreSQL's in_range takes an integer key with an integer offset only: a decimal literal
    // (`0.5`, `-0.0`) is numeric there, refused before its value is checked.
    if (input.offsetNumeric === true && (orderType.kind === "int32" || orderType.kind === "int64"))
      throw new PlanningError(
        `RANGE with offset PRECEDING/FOLLOWING is not supported for column type ${orderType.kind === "int64" ? "bigint" : orderType.bits === 16 ? "smallint" : "integer"} and offset type numeric`,
        "0A000",
      );
    const typed: Typed = lowerExpression(
      { kind: "literal", value: input.offset },
      { column: () => undefined },
      builder.state,
    );
    return {
      kind: input.kind,
      offset: coerce(typed, orderType, "implicit", builder.parameters).expr,
    };
  };
  return {
    unit: frame.unit,
    start: bound(frame.start),
    end: bound(frame.end),
    exclude: frame.exclude ?? "no-others",
  };
}

/** A windowed source as a relation: the inner block's columns, then one per window spec. */
export function lowerWindowed(
  windowed: NonNullable<TableSource["windowed"]>,
  builder: PlanBuilder,
  lowerInner: BlockLowerer,
  outer: OuterResolver | undefined,
): LoweredRel {
  const inner = lowerInner(windowed.block, builder, outer);
  const position = (alias: string): number => {
    const index = inner.columns.findIndex((col) => col.name === alias);
    if (index === -1) throw new PlanningError(`window column "${alias}" vanished`, "XX000");
    return index;
  };
  const groups = new Map<string, WindowSpec[]>();
  for (const spec of windowed.windows) {
    const key = JSON.stringify([spec.partitionAliases, spec.orderAliases]);
    groups.set(key, [...(groups.get(key) ?? []), spec]);
  }
  let rel = inner.rel;
  let columns: RelColumn[] = [...inner.columns];
  const results = new Map<WindowSpec, number>();
  for (const specs of groups.values()) {
    const [first] = specs;
    if (first === undefined) continue;
    const orderBy: SortKey[] = first.orderAliases.map((order) => {
      const index = position(order.alias);
      return {
        expr: column(index, inner.columns[index]?.type ?? { kind: "text" }),
        direction: order.direction,
        nulls: order.nulls ?? (order.direction === "desc" ? "first" : "last"),
        collation: null,
      };
    });
    const orderType = orderBy.length === 1 ? (orderBy[0]?.expr.type ?? null) : null;
    const functions: WindowCall[] = [];
    const added: RelColumn[] = [];
    for (const spec of specs) {
      const argumentIndex = spec.argumentAlias === undefined ? null : position(spec.argumentAlias);
      const argument = argumentIndex === null ? null : (inner.columns[argumentIndex] ?? null);
      // sum / avg of a type no overload takes cast implicitly to the one that does (sum(time)).
      const resolved =
        argument !== null && (RANKING[spec.name] === "sum" || RANKING[spec.name] === "avg")
          ? aggregateArgument(RANKING[spec.name] === "sum" ? "sum" : "avg", argument.type)
          : null;
      const cast =
        argument !== null && resolved !== null && resolved !== argument.type ? resolved : null;
      const result = signature(
        spec,
        argument === null || cast === null ? argument : { ...argument, type: cast },
      );
      const args: ScalarExpr[] =
        argument === null || argumentIndex === null
          ? []
          : [
              cast === null
                ? column(argumentIndex, argument.type)
                : { op: "cast", input: column(argumentIndex, argument.type), type: cast },
            ];
      if (result.fn === "ntile") args.push(int(spec.offset ?? 1, INT4));
      if (result.fn === "lag" || result.fn === "lead") {
        args.push(int(spec.offset ?? 1, INT4));
        const fallback = fallbackOf(spec.fallback, result.type, builder);
        if (fallback !== null) args.push(fallback);
      }
      if (result.fn === "nth_value") args.push(int(spec.offset ?? 1, INT4));
      functions.push({
        fn: result.fn,
        args,
        frame: frameOf(spec.frame, orderType, builder),
        type: result.type,
      });
      results.set(spec, columns.length + added.length);
      added.push({
        name: spec.alias,
        type: result.type,
        nullable: result.nullable,
        big: result.big,
      });
    }
    columns = [...columns, ...added];
    rel = {
      op: "window",
      id: builder.id(),
      input: rel,
      partitionBy: first.partitionAliases.map(position),
      orderBy,
      functions,
      output: outputColumns(columns),
    };
  }
  const visible: RelColumn[] = [
    ...inner.columns,
    ...windowed.windows.map((spec) => at(columns, results.get(spec) ?? -1)),
  ];
  const exprs = [
    ...inner.columns.map((col, index) => column(index, col.type)),
    ...windowed.windows.map((spec) => {
      const index = results.get(spec) ?? -1;
      return column(index, columns[index]?.type ?? { kind: "text" });
    }),
  ];
  return {
    rel: { op: "project", id: builder.id(), input: rel, exprs, output: outputColumns(visible) },
    columns: visible,
    ...(inner.rows === undefined ? {} : { rows: inner.rows }),
  };
}
