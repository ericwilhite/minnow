import { windowInputsOf } from "../../engine/window-inputs.js";
import { frameOffsetMetadata } from "../../engine/frame-offsets.js";
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
import type {
  Expression,
  QueryValue,
  TableSource,
  WindowFrame,
  WindowSpec,
} from "../../plan/model.js";
import { baseType, coerce, type Typed } from "./coerce.js";
import { PlanningError, unsupported } from "./errors.js";
import { lowerExpression } from "./expressions.js";
import type { BlockLowerer, OuterResolver } from "./from.js";
import { floatLiteral, integerLiteral, nullLiteral } from "./literals.js";
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

function int(value: number | bigint, type: PhysicalType): ScalarExpr {
  const literal = integerLiteral(
    typeof value === "bigint" ? value : BigInt(Math.trunc(value)),
    type,
  );
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
  // float_ops in_range(float4,float4,float8,bool,bool) keeps the offset in float8.
  const rangeOffsetType =
    orderType?.kind === "float32"
      ? FLOAT8
      : orderType?.kind === "decimal"
        ? baseType(orderType)
        : orderType;
  const bound = (input: WindowFrame["start"]): WindowFrameBound => {
    if (input.offset === undefined) return { kind: input.kind, offset: null };
    // Parser-owned metadata preserves the exact literal without adding public model fields.
    const metadata = frameOffsetMetadata(input);
    if (metadata?.nullOffset === true)
      return {
        kind: input.kind,
        offset: nullLiteral(frame.unit === "range" ? (rangeOffsetType ?? INT8) : INT8),
      };
    if (metadata?.parameter !== undefined) {
      if (frame.unit === "range" && orderType === null)
        throw new PlanningError("RANGE with offset requires exactly one ORDER BY column", "42P20");
      const type = builder.parameters.fix(
        metadata.parameter,
        frame.unit === "range" ? (rangeOffsetType ?? INT8) : INT8,
      );
      return { kind: input.kind, offset: { op: "param", slot: metadata.parameter, type } };
    }
    const exact = metadata?.exact ?? input.offset;
    if (frame.unit !== "range") return { kind: input.kind, offset: int(exact, INT8) };
    if (orderType === null)
      throw new PlanningError("RANGE with offset requires exactly one ORDER BY column", "42P20");
    // PostgreSQL's in_range takes an integer key with an integer offset only: a decimal literal
    // (`0.5`, `-0.0`) is numeric there, refused before its value is checked.
    if (metadata?.numeric === true && (orderType.kind === "int32" || orderType.kind === "int64"))
      throw new PlanningError(
        `RANGE with offset PRECEDING/FOLLOWING is not supported for column type ${orderType.kind === "int64" ? "bigint" : orderType.bits === 16 ? "smallint" : "integer"} and offset type numeric`,
        "0A000",
      );
    // PostgreSQL also supplies cross-width in_range(int4,int4,int8).
    if (orderType.kind === "int32" || orderType.kind === "int64") {
      const value = typeof exact === "bigint" ? exact : BigInt(exact);
      const literal = integerLiteral(value, orderType) ?? integerLiteral(value, INT8);
      if (literal === null)
        throw new PlanningError("RANGE offset outside bigint is not supported", "0A000");
      return { kind: input.kind, offset: literal };
    }
    if (orderType.kind === "float32" || orderType.kind === "float64") {
      const text = metadata?.text ?? String(exact),
        value = Number(text);
      const nonzero = /[1-9]/u.test(text.split(/[eE]/u)[0] ?? "");
      if (metadata?.text !== undefined && (!Number.isFinite(value) || (value === 0 && nonzero)))
        throw new PlanningError("RANGE offset is out of range for double precision", "22003");
      return { kind: input.kind, offset: floatLiteral(value, FLOAT8) };
    }
    const text =
      metadata?.text ?? (metadata?.exact === undefined ? undefined : String(metadata.exact));
    const typed: Typed = lowerExpression(
      text === undefined
        ? { kind: "literal", value: input.offset }
        : { kind: "literal", value: input.offset, exactText: text },
      { column: () => undefined },
      builder.state,
    );
    return {
      kind: input.kind,
      offset: coerce(typed, rangeOffsetType ?? orderType, "implicit", builder.parameters).expr,
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
        collation: inner.columns[index]?.collation ?? null,
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
        const inputs = windowInputsOf(spec);
        const lowerInput = (expression: Expression) =>
          lowerExpression(
            expression,
            {
              column: (reference) => {
                const index = inner.columns.findIndex((col) => col.name === reference);
                const input = inner.columns[index];
                return input === undefined
                  ? undefined
                  : {
                      expr: column(index, input.type),
                      nullable: input.nullable,
                      big: input.big,
                    };
              },
            },
            builder.state,
          );
        if (inputs.offsetExpression === undefined) args.push(int(spec.offset ?? 1, INT4));
        else {
          try {
            args.push(
              coerce(lowerInput(inputs.offsetExpression), INT4, "implicit", builder.parameters)
                .expr,
            );
          } catch (error) {
            if (!(error instanceof PlanningError)) throw error;
            throw new PlanningError(
              `function ${result.fn} with this offset type does not exist`,
              "42883",
            );
          }
        }
        const fallback =
          inputs.fallbackExpression === undefined
            ? fallbackOf(spec.fallback, result.type, builder)
            : coerce(
                lowerExpression(
                  inputs.fallbackExpression.kind === "case"
                    ? (inputs.fallbackExpression.otherwise ?? { kind: "literal", value: null })
                    : inputs.fallbackExpression,
                  {
                    column: (reference) => {
                      const index = inner.columns.findIndex((col) => col.name === reference);
                      const input = inner.columns[index];
                      return input === undefined
                        ? undefined
                        : {
                            expr: column(index, input.type),
                            nullable: input.nullable,
                            big: input.big,
                          };
                    },
                  },
                  builder.state,
                ),
                result.type,
                "implicit",
                builder.parameters,
              ).expr;
        if (fallback !== null) args.push(fallback);
      }
      if (result.fn === "nth_value") args.push(int(spec.offset ?? 1, INT4));
      const frame = frameOf(spec.frame, orderType, builder);
      // PostgreSQL windowfuncs.c SupportRequestOptimizeWindowClause discards these frames.
      const independent = [
        "row_number",
        "rank",
        "dense_rank",
        "percent_rank",
        "cume_dist",
        "ntile",
      ].includes(result.fn);
      functions.push({
        fn: result.fn,
        args,
        frame: independent ? null : frame,
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
