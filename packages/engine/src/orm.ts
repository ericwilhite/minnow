import { optimizePlan } from "./optimizer.js";
import {
  type CompiledQuery,
  type Expression,
  type Predicate,
  type PredicateOperator,
  type QueryValue,
} from "./query.js";
import { type AnyTable, type ColumnBuilder } from "./schema.js";

/**
 * Type-safe relational query builder. Every method assembles the same compiled-plan structures
 * the SQL parser produces — never SQL strings — and `build()` runs the same deterministic
 * optimizer, so an ORM query and its equivalent SQL bind to identical plans and results. CRUD
 * and batch writes live on `typedTable`; `query()`/`execute()` remain the raw SQL escape hatch.
 */

export interface TypedExpression<TValue> {
  readonly kind: "typed-expression";
  readonly expression: Expression;
  /** Phantom result type; never materialized at runtime. */
  readonly __value?: TValue;
}

export interface Condition {
  readonly kind: "condition";
  readonly predicate: Predicate;
}

type Operand<TValue> = TypedExpression<TValue> | TValue;

function isTypedExpression(value: unknown): value is TypedExpression<QueryValue> {
  return (
    typeof value === "object" &&
    value !== null &&
    (value as { kind?: unknown }).kind === "typed-expression"
  );
}

function operandExpression(operand: Operand<QueryValue>): Expression {
  if (isTypedExpression(operand)) return operand.expression;
  return { kind: "literal", value: operand };
}

function typed<TValue>(expression: Expression): TypedExpression<TValue> {
  return { kind: "typed-expression", expression };
}

// --- Column references --------------------------------------------------------------------------

type SchemaColumnValue<TColumn> =
  TColumn extends ColumnBuilder<infer TValue, infer TNullable, boolean>
    ? TNullable extends true
      ? TValue | null
      : TValue
    : never;

export type TableRefs<TTable extends AnyTable> = {
  readonly [K in keyof TTable["columns"]]: TypedExpression<SchemaColumnValue<TTable["columns"][K]>>;
};

/** Column references nullable-widened for use as a left join's build side. */
export type NullableTableRefs<TTable extends AnyTable> = {
  readonly [K in keyof TTable["columns"]]: TypedExpression<SchemaColumnValue<
    TTable["columns"][K]
  > | null>;
};

/** Typed column references for a table under an alias (defaults to the table name). */
export function refs<TTable extends AnyTable>(
  tableSchema: TTable,
  alias?: string,
): TableRefs<TTable> {
  const prefix = alias ?? tableSchema.name;
  return Object.fromEntries(
    Object.keys(tableSchema.columns).map((name) => [
      name,
      typed({ kind: "column", reference: `${prefix}.${name}` }),
    ]),
  ) as TableRefs<TTable>;
}

/** The same references typed with `| null`, for tables joined through a left join. */
export function nullableRefs<TTable extends AnyTable>(
  tableSchema: TTable,
  alias?: string,
): NullableTableRefs<TTable> {
  return refs(tableSchema, alias);
}

// --- Expression builders ------------------------------------------------------------------------

function comparison(
  operator: PredicateOperator,
  left: Operand<QueryValue>,
  right: Operand<QueryValue>,
): Condition {
  return {
    kind: "condition",
    predicate: { left: operandExpression(left), operator, right: operandExpression(right) },
  };
}

export const eq = (left: Operand<QueryValue>, right: Operand<QueryValue>): Condition =>
  comparison("=", left, right);
export const ne = (left: Operand<QueryValue>, right: Operand<QueryValue>): Condition =>
  comparison("!=", left, right);
export const gt = (left: Operand<QueryValue>, right: Operand<QueryValue>): Condition =>
  comparison(">", left, right);
export const gte = (left: Operand<QueryValue>, right: Operand<QueryValue>): Condition =>
  comparison(">=", left, right);
export const lt = (left: Operand<QueryValue>, right: Operand<QueryValue>): Condition =>
  comparison("<", left, right);
export const lte = (left: Operand<QueryValue>, right: Operand<QueryValue>): Condition =>
  comparison("<=", left, right);

export function inList(
  value: Operand<QueryValue>,
  values: ReadonlyArray<Operand<QueryValue>>,
): Condition {
  return {
    kind: "condition",
    predicate: {
      left: operandExpression(value),
      operator: "IN",
      right: { kind: "list", items: values.map(operandExpression) },
    },
  };
}

export function notInList(
  value: Operand<QueryValue>,
  values: ReadonlyArray<Operand<QueryValue>>,
): Condition {
  return {
    kind: "condition",
    predicate: {
      left: operandExpression(value),
      operator: "NOT IN",
      right: { kind: "list", items: values.map(operandExpression) },
    },
  };
}

function arithmetic(
  operator: "+" | "-" | "*" | "/",
  left: Operand<number | null>,
  right: Operand<number | null>,
): TypedExpression<number> {
  return typed({
    kind: "binary",
    operator,
    left: operandExpression(left),
    right: operandExpression(right),
  });
}

export const add = (
  a: Operand<number | null>,
  b: Operand<number | null>,
): TypedExpression<number> => arithmetic("+", a, b);
export const sub = (
  a: Operand<number | null>,
  b: Operand<number | null>,
): TypedExpression<number> => arithmetic("-", a, b);
export const mul = (
  a: Operand<number | null>,
  b: Operand<number | null>,
): TypedExpression<number> => arithmetic("*", a, b);
export const div = (
  a: Operand<number | null>,
  b: Operand<number | null>,
): TypedExpression<number> => arithmetic("/", a, b);

export function round(
  value: Operand<number | null>,
  digits?: number,
): TypedExpression<number | null> {
  return typed({
    kind: "call",
    name: "ROUND",
    arguments:
      digits === undefined
        ? [operandExpression(value)]
        : [operandExpression(value), { kind: "literal", value: digits }],
  });
}

export function count(): TypedExpression<number> {
  return typed({ kind: "call", name: "COUNT", arguments: [{ kind: "wildcard" }] });
}
export function countOf(value: TypedExpression<QueryValue>): TypedExpression<number> {
  return typed({ kind: "call", name: "COUNT", arguments: [value.expression] });
}
export function sum(value: TypedExpression<number | null>): TypedExpression<number | null> {
  return typed({ kind: "call", name: "SUM", arguments: [value.expression] });
}
export function avg(value: TypedExpression<number | null>): TypedExpression<number | null> {
  return typed({ kind: "call", name: "AVG", arguments: [value.expression] });
}
export function min<TValue extends QueryValue>(
  value: TypedExpression<TValue>,
): TypedExpression<TValue | null> {
  return typed({ kind: "call", name: "MIN", arguments: [value.expression] });
}
export function max<TValue extends QueryValue>(
  value: TypedExpression<TValue>,
): TypedExpression<TValue | null> {
  return typed({ kind: "call", name: "MAX", arguments: [value.expression] });
}

// --- Query builder ------------------------------------------------------------------------------

type SelectShape = Record<string, TypedExpression<QueryValue>>;

export type InferQueryRow<TShape extends SelectShape> = {
  [K in keyof TShape]: TShape[K] extends TypedExpression<infer TValue> ? TValue : never;
};

export interface TypedQuery<TRow> {
  readonly kind: "typed-query";
  readonly plan: CompiledQuery;
  /** Phantom row type; never materialized at runtime. */
  readonly __row?: TRow;
}

interface BuilderState {
  base: { table: string; alias: string };
  joins: Array<{
    table: string;
    alias: string;
    kind: "inner" | "left";
    left: Expression;
    right: Expression;
  }>;
  predicates: Predicate[];
  groupBy: Expression[];
  having: Predicate[];
  orderBy: Array<{ expression: Expression; direction: "asc" | "desc" }>;
  limit?: number;
  distinct: boolean;
}

export class QueryBuilder<TRow> {
  constructor(
    private readonly state: BuilderState,
    private readonly shape: SelectShape | undefined,
  ) {}

  innerJoin(tableSchema: AnyTable, alias: string, on: Condition): QueryBuilder<TRow> {
    return this.#join(tableSchema, alias, "inner", on);
  }

  leftJoin(tableSchema: AnyTable, alias: string, on: Condition): QueryBuilder<TRow> {
    return this.#join(tableSchema, alias, "left", on);
  }

  #join(
    tableSchema: AnyTable,
    alias: string,
    kind: "inner" | "left",
    on: Condition,
  ): QueryBuilder<TRow> {
    return new QueryBuilder(
      {
        ...this.state,
        joins: [
          ...this.state.joins,
          {
            table: tableSchema.name,
            alias,
            kind,
            left: on.predicate.left,
            right: on.predicate.right,
          },
        ],
      },
      this.shape,
    );
  }

  where(...conditions: Condition[]): QueryBuilder<TRow> {
    return new QueryBuilder(
      {
        ...this.state,
        predicates: [
          ...this.state.predicates,
          ...conditions.map((condition) => condition.predicate),
        ],
      },
      this.shape,
    );
  }

  groupBy(...expressions: Array<TypedExpression<QueryValue>>): QueryBuilder<TRow> {
    return new QueryBuilder(
      {
        ...this.state,
        groupBy: [...this.state.groupBy, ...expressions.map(({ expression }) => expression)],
      },
      this.shape,
    );
  }

  having(...conditions: Condition[]): QueryBuilder<TRow> {
    return new QueryBuilder(
      {
        ...this.state,
        having: [...this.state.having, ...conditions.map((condition) => condition.predicate)],
      },
      this.shape,
    );
  }

  select<TShape extends SelectShape>(shape: TShape): QueryBuilder<InferQueryRow<TShape>> {
    return new QueryBuilder<InferQueryRow<TShape>>(this.state, shape);
  }

  distinct(): QueryBuilder<TRow> {
    return new QueryBuilder({ ...this.state, distinct: true }, this.shape);
  }

  orderBy(outputName: keyof TRow & string, direction: "asc" | "desc" = "asc"): QueryBuilder<TRow> {
    return new QueryBuilder(
      {
        ...this.state,
        orderBy: [
          ...this.state.orderBy,
          { expression: { kind: "column", reference: outputName }, direction },
        ],
      },
      this.shape,
    );
  }

  limit(limit: number): QueryBuilder<TRow> {
    return new QueryBuilder({ ...this.state, limit }, this.shape);
  }

  /** Assembles the plan and runs the same deterministic optimizer as compiled SQL. */
  build(): TypedQuery<TRow> {
    const shape = this.shape;
    if (shape === undefined) {
      throw new TypeError("A query needs a select() shape before build()");
    }
    const select = Object.entries(shape).map(([alias, { expression }]) => ({
      expression,
      alias,
    }));
    let groupBy = this.state.groupBy;
    if (this.state.distinct) {
      if (groupBy.length > 0) {
        throw new TypeError("distinct() cannot be combined with groupBy()");
      }
      // The same desugaring the SQL parser applies: DISTINCT groups by every selected expression.
      groupBy = select.map(({ expression }) => expression);
    }
    const plan: CompiledQuery = {
      sql: "(orm)",
      base: this.state.base,
      joins: this.state.joins,
      select,
      predicates: this.state.predicates,
      groupBy,
      having: this.state.having,
      orderBy: this.state.orderBy,
      ...(this.state.limit === undefined ? {} : { limit: this.state.limit }),
    };
    return { kind: "typed-query", plan: optimizePlan(plan) };
  }
}

/** Starts a query over one table, optionally under an alias. */
export function from(tableSchema: AnyTable, alias?: string): QueryBuilder<never> {
  return new QueryBuilder(
    {
      base: { table: tableSchema.name, alias: alias ?? tableSchema.name },
      joins: [],
      predicates: [],
      groupBy: [],
      having: [],
      orderBy: [],
      distinct: false,
    },
    undefined,
  );
}
