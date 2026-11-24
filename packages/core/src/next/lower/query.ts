/**
 * Query blocks (plan/model.ts `CompiledQuery`, after the front-end passes) to relational IR:
 * the FROM clause and WHERE (lower/from.ts), GROUP BY with
 * aggregates and HAVING, the select list, ORDER BY, LIMIT / OFFSET.
 *
 * The FROM clause (sources, joins and their order, subqueries) is lower/from.ts.
 */
import { strongestRowLock } from "../../engine/row-locking.js";
import { rowOutputName, jsonCallSource } from "../../engine/json-call-sources.js";
import type { AggregateCall, AggregateFn, Bound, Rel, ScalarExpr, SortKey } from "../ir/ir.js";
import { aggregateArgument, aggregateResult } from "../ir/functions.js";
import type { PhysicalType } from "../types.js";
import { isAggregateCall } from "../../engine/query.js";
import { encodeQueryIdentity } from "../../engine/query-identity.js";
import type { CompiledQuery, Expression } from "../../plan/model.js";
import { canImplicitlyCast, coerce, settle, type Typed } from "./coerce.js";
import { PlanningError, unsupported } from "./errors.js";
import { nullLiteral } from "./literals.js";
import {
  asBool,
  lowerPredicate,
  lowerExpression,
  lowerJsonValue,
  type Collated,
  type Scope,
} from "./expressions.js";
import { lowerFrom, type FromClause, type OuterResolver } from "./from.js";
import {
  andOf,
  column,
  mapColumns,
  outputColumns,
  type LoweredRel,
  type PlanBuilder,
  type RelColumn,
} from "./plan.js";

export {
  PlanBuilder,
  TableRead,
  andOf,
  outputColumns,
  type LoweredRel,
  type RelColumn,
} from "./plan.js";

/** A front-end aggregate call as an IR aggregate over the pre-aggregation projection. */
interface AggregateDraft {
  readonly fn: AggregateFn;
  readonly args: readonly Typed[];
  readonly distinct: boolean;
  readonly orderBy: ReadonlyArray<{
    readonly typed: Typed;
    readonly direction: "asc" | "desc";
    readonly nulls: "first" | "last";
    readonly argument?: number;
  }>;
  readonly type: PhysicalType;
  readonly nullable: boolean;
  readonly big: boolean;
  readonly filter?: Typed;
  readonly json?: AggregateCall["json"];
}

const INT8: PhysicalType = { kind: "int64" };

/** Exact structural identity after coercion (including privately normalized
 * temporal casts). No evaluation or reassociation: ORDER BY may reuse its
 * argument slot just as PostgreSQL's aggregate target list does. */
function sameExpression(a: unknown, b: unknown): boolean {
  if (a === b) return true;
  if (a === null || b === null || typeof a !== "object" || typeof b !== "object") return false;
  if (ArrayBuffer.isView(a) || ArrayBuffer.isView(b)) {
    if (
      !ArrayBuffer.isView(a) ||
      !ArrayBuffer.isView(b) ||
      a.constructor !== b.constructor ||
      a.byteLength !== b.byteLength
    )
      return false;
    const x = new Uint8Array(a.buffer, a.byteOffset, a.byteLength),
      y = new Uint8Array(b.buffer, b.byteOffset, b.byteLength);
    for (let i = 0; i < x.length; i++) if (x[i] !== y[i]) return false;
    return true;
  }
  const x = Object.entries(a),
    y = Object.entries(b);
  return (
    x.length === y.length &&
    x.every(
      ([key, value]) =>
        Object.hasOwn(b, key) && sameExpression(value, (b as Record<string, unknown>)[key]),
    )
  );
}

function aggregateDraft(
  expression: Extract<Expression, { kind: "call" }>,
  scope: Scope,
  builder: PlanBuilder,
): AggregateDraft {
  const state = builder.state;
  const name = expression.name;
  const distinct = expression.distinct === true;
  const settleArg = (arg: Expression): Typed =>
    settle(lowerExpression(arg, scope, state), builder.parameters);
  const [first] = expression.arguments;
  if (name === "JSON_ARRAYAGG") {
    const source = jsonCallSource(expression);
    if (!source) throw unsupported("aggregate", "array_agg");
    const object = source.includes("OBJECT_AGG"),
      arity = object ? 2 : 1;
    if (!first) throw new PlanningError("JSON aggregate needs an argument", "42883");
    if (source === "JSON_ARRAYAGG" && distinct)
      throw new PlanningError("JSON_ARRAYAGG DISTINCT is not supported by PostgreSQL", "42601");
    const args = expression.arguments
      .slice(0, arity)
      .map((arg) => settle(lowerJsonValue(arg, scope, state), builder.parameters));
    const binary =
      source.startsWith("JSONB") ||
      (source === "JSON_ARRAYAGG" &&
        args[0]?.expr.type.kind === "json" &&
        args[0].expr.type.binary);
    const fn = object
      ? binary
        ? "jsonb_object_agg"
        : "json_object_agg"
      : binary
        ? "jsonb_agg"
        : "json_agg";
    if (distinct) {
      if (args.some((arg) => arg.expr.type.kind === "json" && !arg.expr.type.binary && !arg.record))
        throw new PlanningError("could not identify an equality operator for type json", "42883");
      const identities = new Set(
        expression.arguments.slice(0, arity).map((arg) => encodeQueryIdentity(arg)),
      );
      if (
        expression.aggregateOrderBy?.some(
          (key) => !identities.has(encodeQueryIdentity(key.expression)),
        )
      )
        throw new PlanningError(
          "in an aggregate with DISTINCT, ORDER BY expressions must appear in argument list",
          "42P10",
        );
      if (args.some((arg) => arg.record ?? arg.expr.type.kind === "json"))
        throw unsupported(
          "aggregate",
          "DISTINCT JSONB or composite JSON input (native equality seam pending)",
        );
    }
    const filter = expression.arguments[arity];
    return {
      fn,
      args,
      distinct,
      orderBy: (expression.aggregateOrderBy ?? []).map((order) => ({
        typed: settleArg(order.expression),
        direction: order.direction,
        nulls: order.nulls ?? (order.direction === "desc" ? "first" : "last"),
      })),
      type: { kind: "json", binary },
      nullable: true,
      big: false,
      ...(filter ? { filter: asBool(lowerExpression(filter, scope, state), state) } : {}),
      json: {
        skipNulls: source.endsWith("STRICT") || source === "JSON_ARRAYAGG",
        unique: source.includes("UNIQUE"),
        record: args[0]?.record === true,
      },
    };
  }
  if (name === "COUNT") {
    if (first === undefined || first.kind === "wildcard")
      return {
        fn: "count_star",
        args: [],
        distinct: false,
        orderBy: [],
        type: INT8,
        nullable: false,
        big: false,
      };
    return {
      fn: "count",
      args: [settleArg(first)],
      distinct,
      orderBy: [],
      type: INT8,
      nullable: false,
      big: false,
    };
  }
  if (name === "PERCENTILE_CONT" || name === "PERCENTILE_DISC" || name === "MODE") {
    const fn =
      name === "MODE" ? "mode" : name === "PERCENTILE_CONT" ? "percentile_cont" : "percentile_disc";
    const [order] = expression.aggregateOrderBy ?? [];
    if (first === undefined || order === undefined || expression.aggregateOrderBy?.length !== 1)
      throw new PlanningError("WITHIN GROUP requires one ordering expression", "42601");
    if (distinct) throw unsupported("aggregate", "ordered-set DISTINCT");
    const input = lowerExpression(first, scope, state);
    let value = input;
    if (fn === "percentile_cont") {
      if (value.expr.type.kind === "interval")
        throw unsupported("aggregate", "interval percentile_cont");
      if (input.pending === undefined && !canImplicitlyCast(input.expr.type, { kind: "float64" }))
        throw new PlanningError("function percentile_cont does not exist for these types", "42883");
      value = coerce(input, { kind: "float64" }, "implicit", builder.parameters);
    } else value = settle(input, builder.parameters);
    const args: Typed[] = [];
    if (fn !== "mode") {
      const fraction = expression.arguments[1];
      if (fraction === undefined)
        throw new PlanningError("percentile requires a fraction", "42601");
      const rawFraction = lowerExpression(fraction, scope, state);
      if (rawFraction.expr.type.kind === "array")
        throw unsupported("aggregate", "array percentile fractions");
      if (
        rawFraction.pending === undefined &&
        !canImplicitlyCast(rawFraction.expr.type, { kind: "float64" })
      )
        throw new PlanningError(`function ${fn} does not exist for these types`, "42883");
      const direct = coerce(rawFraction, { kind: "float64" }, "implicit", builder.parameters);
      // Direct arguments are evaluated once per group, not over the input projection.
      // Correlated/group-key direct arguments require a group scope, which is not wired yet.
      args.push({
        ...direct,
        expr: mapColumns(direct.expr, () => {
          throw unsupported("aggregate", "column-valued ordered-set direct argument");
        }),
      });
    }
    const result = aggregateResult(
      fn,
      args.map((arg) => arg.expr.type),
      value.expr.type,
    );
    if (result === null)
      throw new PlanningError(`function ${fn} does not exist for these types`, "42883");
    return {
      fn,
      args,
      distinct: false,
      orderBy: [
        {
          typed: value,
          direction: order.direction,
          nulls: order.nulls ?? (order.direction === "desc" ? "first" : "last"),
        },
      ],
      type: result,
      nullable: true,
      big: fn !== "percentile_cont" && value.big,
    };
  }
  if (first === undefined)
    throw new PlanningError(`${name.toLowerCase()} needs an argument`, "42601");
  const settled = settleArg(first);
  // An argument no overload takes as is casts implicitly to the one that does (sum(time) is
  // sum(interval), as PostgreSQL resolves it).
  const resolved =
    name === "SUM" || name === "AVG"
      ? aggregateArgument(name === "SUM" ? "sum" : "avg", settled.expr.type)
      : null;
  const arg =
    resolved === null || resolved === settled.expr.type
      ? settled
      : coerce(settled, resolved, "implicit", builder.parameters);
  const type = arg.expr.type;
  switch (name) {
    case "SUM": {
      // ir/functions.ts AGGREGATES (R19) types every aggregate.
      const result = aggregateResult("sum", [type]);
      if (result === null)
        throw new PlanningError(`function sum(${type.kind}) does not exist`, "42883");
      // D2 / R17.2: SUM(int8) is exact and renders bigint; SUM(int2/int4) renders number.
      return {
        fn: "sum",
        args: [arg],
        distinct,
        orderBy: [],
        type: result,
        nullable: true,
        big: type.kind === "int64",
      };
    }
    case "AVG": {
      const result = aggregateResult("avg", [type]);
      if (result === null)
        throw new PlanningError(`function avg(${type.kind}) does not exist`, "42883");
      return {
        fn: "avg",
        args: [arg],
        distinct,
        orderBy: [],
        type: result,
        nullable: true,
        big: false,
      };
    }
    case "MIN":
    case "MAX": {
      const fn = name === "MIN" ? "min" : "max";
      // PostgreSQL has no min / max over bool, json, jsonb or uuid (42883). The result keeps
      // the argument's modifiers (a VARCHAR(n) column's min renders as the column does).
      if (aggregateResult(fn, [type]) === null)
        throw new PlanningError(`function ${fn}(${type.kind}) does not exist`, "42883");
      return {
        fn,
        args: [arg],
        distinct,
        orderBy: [],
        type,
        nullable: true,
        big: arg.big,
      };
    }
    case "STRING_AGG": {
      const [, separator] = expression.arguments;
      if (separator === undefined) throw new PlanningError("string_agg needs a delimiter", "42601");
      const text: PhysicalType = type.kind === "bytes" ? type : { kind: "text" };
      const value = coerce(arg, text, "implicit", builder.parameters);
      const delimiter = coerce(
        lowerExpression(separator, scope, state),
        text,
        "implicit",
        builder.parameters,
      );
      const orderBy = (expression.aggregateOrderBy ?? []).map((order) => ({
        typed: settleArg(order.expression),
        argument: expression.arguments
          .slice(0, 2)
          .findIndex((a) => encodeQueryIdentity(a) === encodeQueryIdentity(order.expression)),
        direction: order.direction,
        nulls: order.nulls ?? (order.direction === "desc" ? ("first" as const) : ("last" as const)),
      }));
      return {
        fn: "string_agg",
        args: [value, delimiter],
        ...(expression.arguments[2]
          ? { filter: asBool(lowerExpression(expression.arguments[2], scope, state), state) }
          : {}),
        distinct,
        orderBy,
        type: text,
        nullable: true,
        big: false,
      };
    }
    case "MINNOW_SINGLE_VALUE":
      return {
        fn: "single_value",
        args: [arg],
        distinct: false,
        orderBy: [],
        type,
        nullable: true,
        big: arg.big,
      };
    default:
      throw unsupported("aggregate", name.toLowerCase());
  }
}

/** The shared front end preserves ARRAY_AGG as its legacy JSON adapter. Recover
 * the original typed aggregate here, without changing the shipped parser/engine. */
function arrayAggregate(expression: Expression): Extract<Expression, { kind: "call" }> | null {
  if (expression.kind !== "call" || expression.name !== "MINNOW_ARRAY_FROM_JSON") return null;
  const call = expression.arguments[0],
    wrapped = call?.kind === "call" ? call.arguments[0] : undefined;
  return call?.kind === "call" &&
    call.name === "JSON_ARRAYAGG" &&
    wrapped?.kind === "call" &&
    wrapped.name === "MINNOW_ARRAY_ELEMENT"
    ? call
    : null;
}
function arrayDraft(
  call: Extract<Expression, { kind: "call" }>,
  scope: Scope,
  builder: PlanBuilder,
): AggregateDraft {
  const wrapped = call.arguments[0],
    arg = wrapped?.kind === "call" ? wrapped.arguments[0] : undefined;
  if (!arg) throw new PlanningError("array_agg needs an argument", "42883");
  const value = settle(lowerExpression(arg, scope, builder.state), builder.parameters);
  const type = aggregateResult("array_agg", [value.expr.type]);
  if (!type) throw new PlanningError("array_agg type", "42883");
  const filter = call.arguments[1];
  return {
    fn: "array_agg",
    args: [value],
    type,
    nullable: true,
    big: false,
    distinct: call.distinct === true,
    ...(filter
      ? { filter: asBool(lowerExpression(filter, scope, builder.state), builder.state) }
      : {}),
    orderBy: (call.aggregateOrderBy ?? []).map((k) => ({
      typed: settle(lowerExpression(k.expression, scope, builder.state), builder.parameters),
      argument: encodeQueryIdentity(arg) === encodeQueryIdentity(k.expression) ? 0 : -1,
      direction: k.direction,
      nulls: k.nulls ?? (k.direction === "desc" ? ("first" as const) : ("last" as const)),
    })),
  };
}

/** PostgreSQL evaluates FILTER before either arguments or aggregate-local ordering keys. */
function filteredArgument(value: Typed, filter: Typed | undefined): Typed {
  if (!filter) return value;
  return {
    ...value,
    nullable: true,
    expr: {
      op: "case",
      branches: [{ when: filter.expr, then: value.expr }],
      otherwise: null,
      type: value.expr.type,
    },
  };
}

/** The output of a block's GROUP BY: keys first, then aggregates. */
class Grouping {
  readonly keys: Typed[] = [];
  readonly #keyIdentities = new Map<string, number>();
  /** Source column positions of keys that are plain columns. */
  readonly #keyColumns = new Map<number, number>();
  readonly aggregates: AggregateDraft[] = [];
  readonly #aggregateIdentities = new Map<string, number>();

  constructor(
    groupBy: readonly Expression[],
    readonly source: Scope,
    readonly builder: PlanBuilder,
  ) {
    for (const expression of groupBy) {
      const identity = encodeQueryIdentity(expression);
      if (this.#keyIdentities.has(identity)) continue;
      const typed = settle(lowerExpression(expression, source, builder.state), builder.parameters);
      const position = this.keys.length;
      this.keys.push(typed);
      this.#keyIdentities.set(identity, position);
      if (typed.expr.op === "column" && !this.#keyColumns.has(typed.expr.index))
        this.#keyColumns.set(typed.expr.index, position);
    }
  }

  #aggregateColumn(position: number): Typed {
    const draft = this.aggregates[position];
    if (draft === undefined) throw new PlanningError("aggregate vanished", "XX000");
    return {
      expr: column(this.keys.length + position, draft.type),
      nullable: draft.nullable,
      big: draft.big,
    };
  }

  #keyColumn(position: number): Typed {
    const key = this.keys[position];
    if (key === undefined) throw new PlanningError("group key vanished", "XX000");
    return { expr: column(position, key.expr.type), nullable: key.nullable, big: key.big };
  }

  /** The scope of expressions over the grouped rows (HAVING, the select list, ORDER BY). */
  scope(): Scope {
    return {
      ...(this.source.rowPresence === undefined
        ? {}
        : {
            rowPresence: (reference: string): Typed => {
              const source = this.source.rowPresence?.(reference);
              if (!source) throw new PlanningError("whole row is not in scope", "42803");
              return {
                ...source,
                expr: mapColumns(source.expr, (index) => {
                  const key = this.#keyColumns.get(index);
                  if (key === undefined)
                    throw new PlanningError("whole row must appear in GROUP BY", "42803");
                  return key;
                }),
              };
            },
          }),
      ...(this.source.rowField === undefined
        ? {}
        : {
            rowField: (qualifier: string, ordinal: number): Typed | undefined => {
              const resolved = this.source.rowField?.(qualifier, ordinal);
              if (resolved === undefined) return undefined;
              const key =
                resolved.expr.op === "column"
                  ? this.#keyColumns.get(resolved.expr.index)
                  : undefined;
              if (key === undefined)
                throw new PlanningError(
                  `column "${qualifier}" must appear in the GROUP BY clause or be used in an aggregate function`,
                  "42803",
                );
              return this.#keyColumn(key);
            },
          }),
      match: (expression) => {
        const identity = encodeQueryIdentity(expression);
        const key = this.#keyIdentities.get(identity);
        if (key !== undefined) return this.#keyColumn(key);
        const array = arrayAggregate(expression);
        if (!isAggregateCall(expression) && array === null) return undefined;
        let position = this.#aggregateIdentities.get(identity);
        if (position === undefined) {
          position = this.aggregates.length;
          this.aggregates.push(
            array
              ? arrayDraft(array, this.source, this.builder)
              : aggregateDraft(
                  expression as Extract<Expression, { kind: "call" }>,
                  this.source,
                  this.builder,
                ),
          );
          this.#aggregateIdentities.set(identity, position);
        }
        return this.#aggregateColumn(position);
      },
      ...(this.source.knowsQualifier === undefined
        ? {}
        : { knowsQualifier: this.source.knowsQualifier.bind(this.source) }),
      column: (reference) => {
        const resolved = this.source.column(reference);
        if (resolved === undefined) return undefined;
        const key =
          resolved.expr.op === "column" ? this.#keyColumns.get(resolved.expr.index) : undefined;
        if (key === undefined)
          throw new PlanningError(
            `column "${reference}" must appear in the GROUP BY clause or be used in an aggregate function`,
            "42803",
          );
        return this.#keyColumn(key);
      },
    };
  }

  /** The projection below the aggregate (keys, then every aggregate argument) and the aggregate. */
  build(input: Rel, remap: (expr: ScalarExpr) => ScalarExpr): LoweredRel {
    const exprs: ScalarExpr[] = this.keys.map((key) => remap(key.expr));
    const projected: RelColumn[] = this.keys.map((key, index) => ({
      name: `(group ${String(index + 1)})`,
      type: key.expr.type,
      nullable: key.nullable,
      big: key.big,
    }));
    const argumentColumn = (typed: Typed): ScalarExpr => {
      const index = exprs.length;
      exprs.push(remap(typed.expr));
      projected.push({
        name: `(argument ${String(index + 1)})`,
        type: typed.expr.type,
        nullable: typed.nullable,
        big: typed.big,
      });
      return column(index, typed.expr.type);
    };
    const calls: AggregateCall[] = this.aggregates.map((draft) => {
      const args = draft.args.map((arg) =>
        draft.fn === "percentile_cont" || draft.fn === "percentile_disc" || draft.fn === "mode"
          ? arg.expr
          : argumentColumn(filteredArgument(arg, draft.filter)),
      );
      return {
        fn: draft.fn,
        args,
        distinct: draft.distinct,
        filter: draft.filter ? argumentColumn(draft.filter) : null,
        ...(draft.json ? { json: draft.json } : {}),
        orderBy: draft.orderBy.map((order): SortKey => {
          const index =
            order.argument !== undefined && order.argument >= 0
              ? order.argument
              : draft.fn === "array_agg" || draft.fn === "string_agg"
                ? draft.args.findIndex((a) => sameExpression(a.expr, order.typed.expr))
                : -1;
          if (
            draft.distinct &&
            (draft.fn === "array_agg" || draft.fn === "string_agg") &&
            index < 0
          )
            throw new PlanningError(
              "in an aggregate with DISTINCT, ORDER BY expressions must appear in argument list",
              "42P10",
            );
          return {
            expr:
              index >= 0
                ? (args[index] ?? argumentColumn(filteredArgument(order.typed, draft.filter)))
                : argumentColumn(filteredArgument(order.typed, draft.filter)),
            direction: order.direction,
            nulls: order.nulls,
            collation: null,
          };
        }),
        type: draft.type,
      };
    });
    const pre: Rel = {
      op: "project",
      id: this.builder.id(),
      input,
      exprs,
      output: outputColumns(projected),
    };
    const columns: RelColumn[] = [
      ...projected.slice(0, this.keys.length),
      ...this.aggregates.map((draft, index) => ({
        name: `(aggregate ${String(index + 1)})`,
        type: draft.type,
        nullable: draft.nullable,
        big: draft.big,
      })),
    ];
    const rel: Rel = {
      op: "aggregate",
      id: this.builder.id(),
      input: pre,
      groupBy: this.keys.map((_, index) => index),
      groupingSets: null,
      aggregates: calls,
      output: outputColumns(columns),
    };
    return { rel, columns };
  }
}

function boundOf(
  value: number | undefined,
  parameter: number | undefined,
  builder: PlanBuilder,
): Bound | null {
  if (parameter !== undefined) {
    builder.parameters.fix(parameter, INT8);
    return { kind: "param", slot: parameter };
  }
  return value === undefined ? null : { kind: "const", value };
}

/**
 * Subqueries in scopes that cannot attach an `apply` (HAVING, grouped select lists, DML
 * values): an uncorrelated scalar subquery or EXISTS becomes an init plan; a correlated one, or
 * IN / ANY / ALL there, is refused until it can be attached (`correlatedSubquery`).
 */
export function installSubqueries(builder: PlanBuilder): void {
  builder.state.subquery = (request, scope) => {
    const uncorrelated: OuterResolver = (reference) => {
      if (scope.column(reference) !== undefined)
        throw unsupported(
          "correlatedSubquery",
          "a correlated subquery outside FROM-level expressions",
        );
      return undefined;
    };
    if (request.kind === "quantified")
      throw unsupported("correlatedSubquery", "IN / ANY / ALL over a subquery here");
    const inner = lowerBlock(request.block, builder, uncorrelated);
    return request.kind === "scalar" ? builder.initPlan(inner) : builder.existsInitPlan(inner);
  };
}

/** `*` and `alias.*` select items expanded over the FROM clause's columns. */
function expandWildcards(
  select: CompiledQuery["select"],
  from: FromClause,
): CompiledQuery["select"] {
  if (!select.some((item) => item.expression.kind === "wildcard")) return select;
  return select.flatMap((item) => {
    const expression = item.expression;
    if (expression.kind !== "wildcard") return [item];
    return from.wildcard(expression.table).map((reference) => ({
      expression: reference,
      alias:
        reference.kind === "column"
          ? (reference.reference.split(".").at(-1) ?? reference.reference)
          : item.alias,
    }));
  });
}

/** One query block as a relation with named output columns. */
export function lowerBlock(
  block: CompiledQuery,
  builder: PlanBuilder,
  outer?: OuterResolver,
  options?: { readonly setOperand?: boolean },
): LoweredRel & { readonly pointRead?: boolean } {
  builder.catalogDeclarations?.(block, outer);
  builder.rowLocking = strongestRowLock([builder.rowLocking, block.rowLocking]);
  if (block.pendingSelectShape !== undefined)
    throw new PlanningError("unbound select shape", "XX000");
  const state = builder.state;
  const from = lowerFrom(block.base, block.joins, builder, {
    ...(outer === undefined ? {} : { outer }),
    lowerBlock,
  });
  const source = from.scope;
  const select = expandWildcards(block.select, from);
  const conjuncts = block.predicates.map((predicate) =>
    asBool(lowerPredicate(predicate, source, state), state),
  );
  const grouped =
    block.groupBy.length > 0 ||
    select.some((item) => hasAggregateCall(item.expression)) ||
    block.having.length > 0;
  const grouping = grouped ? new Grouping(block.groupBy, source, builder) : null;
  const outputScope = grouping?.scope() ?? source;
  const having = block.having.map((predicate) =>
    asBool(lowerPredicate(predicate, outputScope, state), state),
  );
  const raw = select.map((item) => lowerJsonValue(item.expression, outputScope, state));
  // A set operation's member leaves its untyped `$n` unknown, as PostgreSQL's leaves do: the set
  // operation's column matching types it (`SELECT $1 UNION SELECT 1.5` is numeric), so it is
  // neither settled to text here nor fixed before the other members are lowered.
  const unknown = raw.map(
    (typed) =>
      options?.setOperand === true &&
      typed.pending?.kind === "param" &&
      builder.parameters.type(typed.pending.index) === null,
  );
  const items: Collated[] = raw.map((typed, index) =>
    unknown[index] === true
      ? { expr: nullLiteral({ kind: "text" }), nullable: true, big: false }
      : settle(typed, builder.parameters),
  );
  // Unknown-typed constants and parameters a set operation still resolves.
  const pending = raw.map((typed, index) =>
    typed.pending?.kind === "null" || typed.pending?.kind === "text" || unknown[index] === true
      ? typed
      : undefined,
  );
  const names = select.map((item) => item.alias);
  // ORDER BY: an output column by name, else an expression over the block's rows (hidden).
  const hidden: Collated[] = [];
  const order = block.orderBy.map((entry) => {
    const expression = entry.expression;
    let position = expression.kind === "column" ? names.indexOf(expression.reference) : -1;
    let collation: string | null;
    if (position === -1) {
      const typed = settle(
        lowerExpression(expression, outputScope, state),
        builder.parameters,
      ) as Collated;
      collation = typed.collation ?? null;
      const identity = encodeQueryIdentity(expression);
      const existing = select.findIndex(
        (item) => encodeQueryIdentity(item.expression) === identity,
      );
      if (existing !== -1) position = existing;
      else {
        position = items.length + hidden.length;
        hidden.push(typed);
      }
    } else collation = items[position]?.collation ?? null;
    return {
      position,
      direction: entry.direction,
      nulls: entry.nulls ?? (entry.direction === "desc" ? ("first" as const) : ("last" as const)),
      collation,
    };
  });

  // Assemble bottom-up: every expression is typed, so the FROM clause can be built and its
  // virtual columns mapped to positions.
  // A scoring corpus is the visible base table, before WHERE or demand bounds.
  const base = from.build(conjuncts);
  const positions = new Map(base.layout.map((index, position) => [index, position]));
  const remap = (expr: ScalarExpr): ScalarExpr =>
    mapColumns(expr, (index) => {
      const position = positions.get(index);
      if (position === undefined)
        throw new PlanningError(`column ${String(index)} vanished`, "XX000");
      return position;
    });
  let current: LoweredRel = { rel: base.rel, columns: base.columns };
  const projected = [...items, ...hidden];
  const exprs =
    grouping === null
      ? projected.map((item) => remap(item.expr))
      : projected.map((item) => item.expr);
  if (grouping !== null) {
    const aggregated = grouping.build(current.rel, remap);
    const predicate = andOf(having);
    current =
      predicate === null
        ? aggregated
        : {
            rel: {
              op: "filter",
              id: builder.id(),
              input: aggregated.rel,
              predicate,
              output: aggregated.rel.output,
            },
            columns: aggregated.columns,
          };
  }
  const columns: RelColumn[] = projected.map((item, index) => ({
    name: names[index] ?? `(order ${String(index - names.length + 1)})`,
    ...(select[index] === undefined
      ? {}
      : {
          label:
            rowOutputName(select[index]) ??
            rowOutputName(select[index].expression) ??
            select[index].alias,
        }),
    type: item.expr.type,
    nullable: item.nullable,
    big: item.big,
    ...(item.record ? { record: true as const } : {}),
    ...(item.collation === undefined ? {} : { collation: item.collation }),
  }));
  current = {
    rel: {
      op: "project",
      id: builder.id(),
      input: current.rel,
      exprs,
      output: outputColumns(columns),
    },
    columns,
  };
  const limit = boundOf(block.limit, block.limitParameter, builder);
  const offset = boundOf(block.offset, block.offsetParameter, builder);
  if (order.length > 0) {
    const keys: SortKey[] = order.map((key) => ({
      expr: column(key.position, columns[key.position]?.type ?? { kind: "text" }),
      direction: key.direction,
      nulls: key.nulls,
      collation: key.collation,
    }));
    current = {
      rel: {
        op: "sort",
        id: builder.id(),
        input: current.rel,
        keys,
        limit,
        offset,
        withTies: block.limitWithTies === true,
        output: current.rel.output,
      },
      columns,
    };
  } else if (limit !== null || offset !== null) {
    if (block.limitWithTies === true)
      throw new PlanningError("WITH TIES requires ORDER BY", "42601");
    current = {
      rel: {
        op: "limit",
        id: builder.id(),
        input: current.rel,
        limit,
        offset,
        output: current.rel.output,
      },
      columns,
    };
  }
  if (hidden.length > 0) {
    const visible = columns.slice(0, items.length);
    current = {
      rel: {
        op: "project",
        id: builder.id(),
        input: current.rel,
        exprs: visible.map((col, index) => column(index, col.type)),
        output: outputColumns(visible),
      },
      columns: visible,
    };
  }
  const rows =
    grouping === null ? base.rows : Math.max(1, Math.min(base.rows, Math.sqrt(base.rows) * 10));
  const limited = block.limit === undefined ? rows : Math.min(rows, block.limit);
  return {
    ...current,
    rows: limited,
    pointRead: base.pointRead && grouping === null && order.length === 0,
    ...(pending.some((typed) => typed !== undefined) ? { pending } : {}),
  };
}

function hasAggregateCall(expression: Expression): boolean {
  if (isAggregateCall(expression)) return true;
  switch (expression.kind) {
    case "binary":
    case "condition":
    case "logical":
      return hasAggregateCall(expression.left) || hasAggregateCall(expression.right);
    case "not":
      return hasAggregateCall(expression.operand);
    case "call":
      return expression.arguments.some(hasAggregateCall);
    case "case":
      return (
        expression.branches.some(
          (branch) => hasAggregateCall(branch.when) || hasAggregateCall(branch.then),
        ) ||
        (expression.otherwise !== undefined && hasAggregateCall(expression.otherwise))
      );
    case "list":
      return expression.items.some(hasAggregateCall);
    default:
      return false;
  }
}
