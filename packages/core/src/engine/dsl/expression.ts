import {
  hasAggregate,
  type AggregateName,
  type Expression,
  type PredicateOperator,
  type QueryValue,
  type WindowFunctionName,
} from "../query.js";
import {
  isBlockCompilable,
  materialize,
  type AliasedExpression,
  type BlockCompilable,
  type ColumnReference,
  type ColumnReferenceOf,
  type CompileContext,
  type ContextWithTable,
  type ExpressionSource,
  type ReferencedValue,
  type TableExpression,
} from "./types.js";
import type { SelectQueryBuilder } from "./select-query-builder.js";

/**
 * Kysely-style expression building. `eb(lhs, op, rhs)` plus `eb.and/or/not/fn/case/exists/...`
 * assemble the same Expression trees the SQL parser produces — string left-hand sides are column
 * references, right-hand sides are values (use `eb.ref` to compare columns). Everything holds an
 * ExpressionSource so subqueries compile under the enclosing query's shared derived-source
 * sequence, keeping builder plans byte-identical to parsed SQL.
 */

export type SqlBool = boolean;

export type ComparisonOperatorToken = "=" | "!=" | "<>" | ">" | ">=" | "<" | "<=";
export type LikeOperatorToken = "like" | "not like";
export type InOperatorToken = "in" | "not in";
export type IsOperatorToken = "is" | "is not";
export type ArithmeticOperatorToken = "+" | "-" | "*" | "/";

export type BinaryOperatorToken =
  | ComparisonOperatorToken
  | LikeOperatorToken
  | InOperatorToken
  | IsOperatorToken
  | ArithmeticOperatorToken;

const comparisonTokens = new Set<string>(["=", "!=", "<>", ">", ">=", "<", "<="]);
const arithmeticTokens = new Set<string>(["+", "-", "*", "/"]);

export class ExpressionWrapper<out TValue> {
  readonly kind = "dsl-expression" as const;
  constructor(readonly source: ExpressionSource) {}

  /** Names this expression's output column, as SQL `AS alias` does. */
  as<TAlias extends string>(alias: TAlias): AliasedExpression<TValue, TAlias> {
    return { kind: "aliased-expression", alias, source: this.source };
  }
}

export function isExpressionWrapper(value: unknown): value is ExpressionWrapper<unknown> {
  return value instanceof ExpressionWrapper;
}

/**
 * A subquery usable where a scalar `TValue` is expected: its row must be a single column whose
 * value fits. Only the value side is checked structurally — the builder's `DB`/context do not
 * matter here, hence the `any`s.
 */
/* eslint-disable-next-line @typescript-eslint/no-explicit-any -- type-position wildcard */
export type ScalarSubquery<TValue> = SelectQueryBuilder<any, any, Record<string, TValue | null>>;

/** A value operand: a literal, a wrapped expression, or a scalar subquery of the same type. */
export type ValueOperand<TValue> =
  TValue | null | ExpressionWrapper<TValue | null> | ScalarSubquery<TValue>;

type ArithmeticOperand<TCtx> =
  ColumnReferenceOf<TCtx, number> | number | ExpressionWrapper<number | null>;

// --- Operand resolution -------------------------------------------------------------------------

export function literalExpression(value: unknown): Expression {
  if (
    value === null ||
    typeof value === "boolean" ||
    typeof value === "number" ||
    typeof value === "string" ||
    value instanceof Date
  ) {
    if (typeof value === "number" && !Number.isFinite(value)) {
      throw new TypeError("Query literals must be finite numbers");
    }
    return { kind: "literal", value };
  }
  throw new TypeError(`Unsupported query literal of type ${typeof value}`);
}

function subquerySource(builder: BlockCompilable): ExpressionSource {
  return (context) => ({ kind: "subquery", block: builder.compileBlock(context, "(subquery)") });
}

/** Left-hand sides treat strings as column references. */
function operandAsRef(value: unknown): ExpressionSource {
  if (isExpressionWrapper(value)) return value.source;
  if (isBlockCompilable(value)) return subquerySource(value);
  if (typeof value === "string") return { kind: "column", reference: value };
  return literalExpression(value);
}

/** Right-hand sides treat strings as values. */
function operandAsValue(value: unknown): ExpressionSource {
  if (isExpressionWrapper(value)) return value.source;
  if (isBlockCompilable(value)) return subquerySource(value);
  return literalExpression(value);
}

/**
 * Builds eagerly when every child is concrete, deferring only when a child needs the compile
 * context. The builder receives a bounds-checked accessor over the materialized children.
 */
function combineSources(
  sources: readonly ExpressionSource[],
  build: (expressionAt: (index: number) => Expression) => Expression,
): ExpressionSource {
  const buildFrom = (expressions: readonly Expression[]): Expression =>
    build((index) => {
      const expression = expressions[index];
      if (expression === undefined) {
        throw new TypeError(`Expression child ${String(index)} is missing`);
      }
      return expression;
    });
  if (sources.every((source): source is Expression => typeof source !== "function")) {
    return buildFrom(sources);
  }
  return (context) => buildFrom(sources.map((source) => materialize(source, context)));
}

/** Shared with the query builders: `lhs op rhs` where lhs is a reference and rhs a value. */
export function buildBinaryCondition(
  lhs: unknown,
  operator: string,
  rhs: unknown,
): ExpressionSource {
  if (comparisonTokens.has(operator)) {
    return combineSources([operandAsRef(lhs), operandAsValue(rhs)], (expressionAt) => ({
      kind: "condition",
      operator: operator as PredicateOperator,
      left: expressionAt(0),
      right: expressionAt(1),
    }));
  }
  if (arithmeticTokens.has(operator)) {
    return combineSources([operandAsRef(lhs), operandAsValue(rhs)], (expressionAt) => ({
      kind: "binary",
      operator: operator as ArithmeticOperatorToken,
      left: expressionAt(0),
      right: expressionAt(1),
    }));
  }
  if (operator === "like" || operator === "not like") {
    return combineSources([operandAsRef(lhs), operandAsValue(rhs)], (expressionAt) => ({
      kind: "condition",
      operator: operator === "like" ? "LIKE" : "NOT LIKE",
      left: expressionAt(0),
      right: expressionAt(1),
    }));
  }
  if (operator === "in" || operator === "not in") {
    const conditionOperator = operator === "in" ? "IN" : "NOT IN";
    if (isBlockCompilable(rhs)) {
      return combineSources([operandAsRef(lhs), subquerySource(rhs)], (expressionAt) => ({
        kind: "condition",
        operator: conditionOperator,
        left: expressionAt(0),
        right: expressionAt(1),
      }));
    }
    if (!Array.isArray(rhs)) {
      throw new TypeError(`${operator} requires an array of values or a subquery`);
    }
    const items = (rhs as unknown[]).map(operandAsValue);
    return combineSources([operandAsRef(lhs), ...items], (expressionAt) => ({
      kind: "condition",
      operator: conditionOperator,
      left: expressionAt(0),
      right: { kind: "list", items: items.map((_, index) => expressionAt(index + 1)) },
    }));
  }
  if (operator === "is" || operator === "is not") {
    if (rhs !== null) throw new TypeError(`${operator} only supports null`);
    return combineSources([operandAsRef(lhs)], (expressionAt) => ({
      kind: "condition",
      operator: operator === "is" ? "IS NULL" : "IS NOT NULL",
      left: expressionAt(0),
      right: { kind: "literal", value: null },
    }));
  }
  throw new TypeError(`Unsupported operator: ${operator}`);
}

// --- Window functions ---------------------------------------------------------------------------

interface OverClauseState {
  partitionBy: ExpressionSource[];
  orderBy: Array<{ source: ExpressionSource; direction: "asc" | "desc" }>;
}

export class OverBuilder<in out TCtx> {
  constructor(private readonly state: OverClauseState = { partitionBy: [], orderBy: [] }) {}

  partitionBy(
    ...references: ReadonlyArray<ColumnReference<TCtx> | ExpressionWrapper<unknown>>
  ): OverBuilder<TCtx> {
    return new OverBuilder({
      ...this.state,
      partitionBy: [...this.state.partitionBy, ...references.map(operandAsRef)],
    });
  }

  orderBy(
    reference: ColumnReference<TCtx> | ExpressionWrapper<unknown>,
    direction: "asc" | "desc" = "asc",
  ): OverBuilder<TCtx> {
    return new OverBuilder({
      ...this.state,
      orderBy: [...this.state.orderBy, { source: operandAsRef(reference), direction }],
    });
  }

  /** @internal */
  get clause(): OverClauseState {
    return this.state;
  }
}

function windowSource(
  name: WindowFunctionName,
  over: OverClauseState,
  argument: ExpressionSource | undefined,
): ExpressionSource {
  const children = [
    ...over.partitionBy,
    ...over.orderBy.map(({ source }) => source),
    ...(argument === undefined ? [] : [argument]),
  ];
  return combineSources(children, (expressionAt) => {
    const partitionBy = over.partitionBy.map((_, index) => expressionAt(index));
    const orderBy = over.orderBy.map(({ direction }, index) => ({
      expression: expressionAt(over.partitionBy.length + index),
      direction,
    }));
    const argumentExpression =
      argument === undefined ? undefined : expressionAt(children.length - 1);
    if (argumentExpression !== undefined && hasAggregate(argumentExpression)) {
      throw new TypeError("Window aggregate arguments cannot contain aggregates");
    }
    return {
      kind: "window",
      name,
      partitionBy,
      orderBy,
      ...(argumentExpression === undefined || argumentExpression.kind === "wildcard"
        ? {}
        : { argument: argumentExpression }),
    };
  });
}

type OverConfigure<TCtx> = (over: OverBuilder<TCtx>) => OverBuilder<TCtx>;

function overClauseOf<TCtx>(configure: OverConfigure<TCtx> | undefined): OverClauseState {
  return configure === undefined
    ? { partitionBy: [], orderBy: [] }
    : configure(new OverBuilder<TCtx>()).clause;
}

/** A ranking window function; SQL requires OVER, so only `.over()` yields an expression. */
export class WindowFunctionBuilder<in out TCtx> {
  constructor(private readonly name: WindowFunctionName) {}

  over(configure?: OverConfigure<TCtx>): ExpressionWrapper<number> {
    return new ExpressionWrapper(windowSource(this.name, overClauseOf(configure), undefined));
  }
}

// --- Aggregate functions ------------------------------------------------------------------------

export class AggregateExpressionWrapper<in out TCtx, out TValue> extends ExpressionWrapper<TValue> {
  constructor(
    private readonly name: AggregateName,
    private readonly argument: ExpressionSource,
    private readonly isDistinct = false,
  ) {
    super(
      combineSources([argument], (expressionAt) => ({
        kind: "call",
        name,
        arguments: [expressionAt(0)],
        ...(isDistinct ? { distinct: true } : {}),
      })),
    );
  }

  /** COUNT(DISTINCT e); the engine restricts DISTINCT to COUNT over one scalar argument. */
  distinct(): AggregateExpressionWrapper<TCtx, TValue> {
    if (this.name !== "COUNT") throw new TypeError("DISTINCT is only supported inside COUNT");
    if (typeof this.argument !== "function" && this.argument.kind === "wildcard") {
      throw new TypeError("COUNT(DISTINCT) requires exactly one scalar argument");
    }
    return new AggregateExpressionWrapper(this.name, this.argument, true);
  }

  /** Turns the aggregate into a window over the given partition and ordering. */
  over(configure?: OverConfigure<TCtx>): ExpressionWrapper<TValue> {
    if (this.isDistinct) throw new TypeError("DISTINCT window aggregates are not supported");
    return new ExpressionWrapper(
      windowSource(
        this.name,
        overClauseOf(configure),
        typeof this.argument !== "function" && this.argument.kind === "wildcard"
          ? undefined
          : this.argument,
      ),
    );
  }
}

/** A COALESCE argument: a column reference or any wrapped expression. */
type CoalesceArg<TCtx> = ColumnReference<TCtx> | ExpressionWrapper<unknown>;

type CoalesceArgValue<TCtx, TArg> =
  TArg extends ExpressionWrapper<infer TValue>
    ? TValue
    : TArg extends string
      ? ReferencedValue<TCtx, TArg>
      : never;

/**
 * The union of the operand value types; null stays only when the final fallback is itself
 * nullable, matching COALESCE's first-non-null semantics.
 */
type CoalesceValue<TCtx, TArgs extends readonly unknown[]> = TArgs extends readonly [
  ...unknown[],
  infer TLast,
]
  ? | Exclude<CoalesceArgValue<TCtx, TArgs[number]>, null>
    | Extract<CoalesceArgValue<TCtx, TLast>, null>
  : CoalesceArgValue<TCtx, TArgs[number]>;

export interface FunctionModule<in out TCtx> {
  /** COUNT(argument); counts non-null values. */
  count(
    reference: ColumnReference<TCtx> | ExpressionWrapper<unknown>,
  ): AggregateExpressionWrapper<TCtx, number>;
  /** COUNT(*). */
  countAll(): AggregateExpressionWrapper<TCtx, number>;
  sum(
    reference: ColumnReferenceOf<TCtx, number> | ExpressionWrapper<number | null>,
  ): AggregateExpressionWrapper<TCtx, number | null>;
  avg(
    reference: ColumnReferenceOf<TCtx, number> | ExpressionWrapper<number | null>,
  ): AggregateExpressionWrapper<TCtx, number | null>;
  min<TRef extends ColumnReference<TCtx>>(
    reference: TRef,
  ): AggregateExpressionWrapper<TCtx, ReferencedValue<TCtx, TRef & string> | null>;
  min<TValue>(
    expression: ExpressionWrapper<TValue>,
  ): AggregateExpressionWrapper<TCtx, TValue | null>;
  max<TRef extends ColumnReference<TCtx>>(
    reference: TRef,
  ): AggregateExpressionWrapper<TCtx, ReferencedValue<TCtx, TRef & string> | null>;
  max<TValue>(
    expression: ExpressionWrapper<TValue>,
  ): AggregateExpressionWrapper<TCtx, TValue | null>;
  round(value: ArithmeticOperand<TCtx>, digits?: number): ExpressionWrapper<number | null>;
  coalesce<TArgs extends readonly [CoalesceArg<TCtx>, ...Array<CoalesceArg<TCtx>>]>(
    ...values: TArgs
  ): ExpressionWrapper<CoalesceValue<TCtx, TArgs>>;
  dateTrunc(
    unit: "year" | "month" | "day" | "hour" | "minute" | "second",
    value: ColumnReferenceOf<TCtx, Date> | Date | ExpressionWrapper<Date | null>,
  ): ExpressionWrapper<Date | null>;
}

function createFunctionModule<TCtx>(): FunctionModule<TCtx> {
  const aggregate = <TValue>(
    name: AggregateName,
    argument: unknown,
  ): AggregateExpressionWrapper<TCtx, TValue> =>
    new AggregateExpressionWrapper<TCtx, TValue>(name, operandAsRef(argument));
  const call = (
    name: "ROUND" | "COALESCE" | "DATE_TRUNC",
    args: readonly ExpressionSource[],
  ): ExpressionSource =>
    combineSources(args, (expressionAt) => ({
      kind: "call",
      name,
      arguments: args.map((_, index) => expressionAt(index)),
    }));
  return {
    count: (reference) => aggregate("COUNT", reference),
    countAll: () => new AggregateExpressionWrapper<TCtx, number>("COUNT", { kind: "wildcard" }),
    sum: (reference) => aggregate("SUM", reference),
    avg: (reference) => aggregate("AVG", reference),
    min: ((reference: unknown) => aggregate("MIN", reference)) as FunctionModule<TCtx>["min"],
    max: ((reference: unknown) => aggregate("MAX", reference)) as FunctionModule<TCtx>["max"],
    round: (value, digits) =>
      new ExpressionWrapper(
        call(
          "ROUND",
          digits === undefined
            ? [operandAsRef(value)]
            : [operandAsRef(value), literalExpression(digits)],
        ),
      ),
    coalesce: (...values: ReadonlyArray<CoalesceArg<TCtx>>) => {
      if (values.length === 0) throw new TypeError("COALESCE requires at least one argument");
      return new ExpressionWrapper(call("COALESCE", values.map(operandAsRef)));
    },
    dateTrunc: (unit, value) =>
      new ExpressionWrapper(call("DATE_TRUNC", [literalExpression(unit), operandAsRef(value)])),
  };
}

// --- CASE ---------------------------------------------------------------------------------------

interface CaseBranch {
  when: ExpressionSource;
  then: ExpressionSource;
}

export class CaseThenBuilder<in out TCtx, out TValue> {
  constructor(
    private readonly branches: readonly CaseBranch[],
    private readonly pendingWhen: ExpressionSource,
  ) {}

  then<TNext extends QueryValue>(
    value: TNext | ExpressionWrapper<TNext>,
  ): CaseBuilder<TCtx, TValue | TNext> {
    return new CaseBuilder([
      ...this.branches,
      { when: this.pendingWhen, then: operandAsValue(value) },
    ]);
  }
}

export class CaseBuilder<in out TCtx, out TValue = never> {
  constructor(private readonly branches: readonly CaseBranch[] = []) {}

  when(condition: ExpressionWrapper<SqlBool>): CaseThenBuilder<TCtx, TValue>;
  when<TRef extends ColumnReference<TCtx>>(
    lhs: TRef,
    operator: ComparisonOperatorToken | LikeOperatorToken,
    rhs: ValueOperand<ReferencedValue<TCtx, TRef & string>>,
  ): CaseThenBuilder<TCtx, TValue>;
  when(lhs: unknown, operator?: string, rhs?: unknown): CaseThenBuilder<TCtx, TValue> {
    const when =
      operator === undefined ? operandAsRef(lhs) : buildBinaryCondition(lhs, operator, rhs);
    return new CaseThenBuilder(this.branches, when);
  }

  else<TNext extends QueryValue>(
    value: TNext | ExpressionWrapper<TNext>,
  ): CaseEndBuilder<TValue | TNext> {
    return new CaseEndBuilder(this.branches, operandAsValue(value));
  }

  end(): ExpressionWrapper<TValue | null> {
    return new ExpressionWrapper(buildCase(this.branches, undefined));
  }
}

export class CaseEndBuilder<out TValue> {
  constructor(
    private readonly branches: readonly CaseBranch[],
    private readonly otherwise: ExpressionSource,
  ) {}

  end(): ExpressionWrapper<TValue> {
    return new ExpressionWrapper(buildCase(this.branches, this.otherwise));
  }
}

function buildCase(
  branches: readonly CaseBranch[],
  otherwise: ExpressionSource | undefined,
): ExpressionSource {
  if (branches.length === 0) throw new TypeError("CASE requires at least one WHEN branch");
  const sources = [
    ...branches.flatMap((branch) => [branch.when, branch.then]),
    ...(otherwise === undefined ? [] : [otherwise]),
  ];
  return combineSources(sources, (expressionAt) => ({
    kind: "case",
    branches: branches.map((_, index) => ({
      when: expressionAt(index * 2),
      then: expressionAt(index * 2 + 1),
    })),
    ...(otherwise === undefined ? {} : { otherwise: expressionAt(sources.length - 1) }),
  }));
}

// --- The builder --------------------------------------------------------------------------------

export interface ExpressionBuilder<in out DB, in out TCtx> {
  /** Compares a column (or expression) to a value; string left-hand sides are references. */
  <TRef extends ColumnReference<TCtx> & string>(
    lhs: TRef,
    operator: ComparisonOperatorToken,
    rhs: ValueOperand<ReferencedValue<TCtx, TRef>>,
  ): ExpressionWrapper<SqlBool>;
  (
    lhs: ExpressionWrapper<unknown>,
    operator: ComparisonOperatorToken,
    rhs: QueryValue | ExpressionWrapper<unknown> | ScalarSubquery<unknown>,
  ): ExpressionWrapper<SqlBool>;
  (
    lhs: ColumnReference<TCtx> | ExpressionWrapper<string | null>,
    operator: LikeOperatorToken,
    rhs: string | ExpressionWrapper<string | null>,
  ): ExpressionWrapper<SqlBool>;
  <TRef extends ColumnReference<TCtx> & string>(
    lhs: TRef,
    operator: InOperatorToken,
    rhs:
      | ReadonlyArray<ReferencedValue<TCtx, TRef> | ExpressionWrapper<unknown>>
      | ScalarSubquery<ReferencedValue<TCtx, TRef>>,
  ): ExpressionWrapper<SqlBool>;
  (
    lhs: ColumnReference<TCtx> | ExpressionWrapper<unknown>,
    operator: IsOperatorToken,
    rhs: null,
  ): ExpressionWrapper<SqlBool>;
  (
    lhs: ArithmeticOperand<TCtx>,
    operator: ArithmeticOperatorToken,
    rhs: ArithmeticOperand<TCtx>,
  ): ExpressionWrapper<number>;

  /** ANDs conditions left to right. */
  and(conditions: ReadonlyArray<ExpressionWrapper<SqlBool>>): ExpressionWrapper<SqlBool>;
  /** ORs conditions left to right. */
  or(conditions: ReadonlyArray<ExpressionWrapper<SqlBool>>): ExpressionWrapper<SqlBool>;
  not(condition: ExpressionWrapper<SqlBool>): ExpressionWrapper<SqlBool>;
  /** `lhs BETWEEN lower AND upper`, desugared exactly as the parser does. */
  between<TRef extends ColumnReference<TCtx>>(
    reference: TRef | ExpressionWrapper<unknown>,
    lower: ValueOperand<ReferencedValue<TCtx, TRef & string>>,
    upper: ValueOperand<ReferencedValue<TCtx, TRef & string>>,
  ): ExpressionWrapper<SqlBool>;
  notBetween<TRef extends ColumnReference<TCtx>>(
    reference: TRef | ExpressionWrapper<unknown>,
    lower: ValueOperand<ReferencedValue<TCtx, TRef & string>>,
    upper: ValueOperand<ReferencedValue<TCtx, TRef & string>>,
  ): ExpressionWrapper<SqlBool>;
  exists(subquery: BlockCompilable): ExpressionWrapper<SqlBool>;
  /** A column reference as an expression, for the rare value-position column comparison. */
  ref<TRef extends ColumnReference<TCtx>>(
    reference: TRef,
  ): ExpressionWrapper<ReferencedValue<TCtx, TRef & string>>;
  /** A literal value as an expression. */
  val<TValue extends QueryValue>(value: TValue): ExpressionWrapper<TValue>;
  /** Arithmetic negation. */
  neg(value: ArithmeticOperand<TCtx>): ExpressionWrapper<number>;
  case(): CaseBuilder<TCtx>;
  rowNumber(): WindowFunctionBuilder<TCtx>;
  rank(): WindowFunctionBuilder<TCtx>;
  denseRank(): WindowFunctionBuilder<TCtx>;
  /** Starts a correlated or uncorrelated subquery over the database plus the outer scope. */
  selectFrom<TE extends TableExpression<DB>>(
    table: TE,
  ): SelectQueryBuilder<DB, ContextWithTable<DB, TCtx, TE>, Record<never, unknown>>;
  fn: FunctionModule<TCtx>;
}

function foldLogical(
  operator: "and" | "or",
  conditions: ReadonlyArray<ExpressionWrapper<SqlBool>>,
): ExpressionSource {
  if (conditions.length === 0) {
    throw new TypeError(`${operator}() requires at least one condition`);
  }
  return combineSources(
    conditions.map((condition) => condition.source),
    (expressionAt) => {
      let folded = expressionAt(0);
      for (let index = 1; index < conditions.length; index += 1) {
        folded = { kind: "logical", operator, left: folded, right: expressionAt(index) };
      }
      return folded;
    },
  );
}

function betweenSource(
  reference: unknown,
  lower: unknown,
  upper: unknown,
  negated: boolean,
): ExpressionSource {
  return combineSources(
    [operandAsRef(reference), operandAsValue(lower), operandAsValue(upper)],
    (expressionAt) => {
      const range: Expression = {
        kind: "logical",
        operator: "and",
        left: { kind: "condition", operator: ">=", left: expressionAt(0), right: expressionAt(1) },
        right: {
          kind: "condition",
          operator: "<=",
          left: structuredClone(expressionAt(0)),
          right: expressionAt(2),
        },
      };
      return negated ? { kind: "not", operand: range } : range;
    },
  );
}

/**
 * Creates the callable expression builder. The subquery factory is injected by the select
 * builder module, which owns both sides of that recursion.
 */
export function createExpressionBuilder<DB, TCtx>(
  createSubquery: (table: string) => unknown,
): ExpressionBuilder<DB, TCtx> {
  const callable = (lhs: unknown, operator: string, rhs: unknown): ExpressionWrapper<unknown> =>
    new ExpressionWrapper(buildBinaryCondition(lhs, operator, rhs));
  const members = {
    and: (conditions: ReadonlyArray<ExpressionWrapper<SqlBool>>) =>
      new ExpressionWrapper(foldLogical("and", conditions)),
    or: (conditions: ReadonlyArray<ExpressionWrapper<SqlBool>>) =>
      new ExpressionWrapper(foldLogical("or", conditions)),
    not: (condition: ExpressionWrapper<SqlBool>) =>
      new ExpressionWrapper(
        combineSources([condition.source], (expressionAt) => ({
          kind: "not",
          operand: expressionAt(0),
        })),
      ),
    between: (reference: unknown, lower: unknown, upper: unknown) =>
      new ExpressionWrapper(betweenSource(reference, lower, upper, false)),
    notBetween: (reference: unknown, lower: unknown, upper: unknown) =>
      new ExpressionWrapper(betweenSource(reference, lower, upper, true)),
    exists: (subquery: BlockCompilable) =>
      new ExpressionWrapper((context: CompileContext) => {
        const block = subquery.compileBlock(context, "(subquery)");
        // Existence needs at most one row, so cap the subquery unless it already limits itself.
        block.limit ??= 1;
        return { kind: "exists", block, negated: false } satisfies Expression;
      }),
    ref: (reference: string) =>
      new ExpressionWrapper({ kind: "column", reference } satisfies Expression),
    val: (value: QueryValue) => new ExpressionWrapper(literalExpression(value)),
    neg: (value: unknown) =>
      new ExpressionWrapper(
        combineSources([operandAsRef(value)], (expressionAt) => ({
          kind: "binary",
          operator: "*",
          left: { kind: "literal", value: -1 },
          right: expressionAt(0),
        })),
      ),
    case: () => new CaseBuilder(),
    rowNumber: () => new WindowFunctionBuilder("ROW_NUMBER"),
    rank: () => new WindowFunctionBuilder("RANK"),
    denseRank: () => new WindowFunctionBuilder("DENSE_RANK"),
    selectFrom: (table: string) => createSubquery(table),
    fn: createFunctionModule<TCtx>(),
  };
  return Object.assign(callable, members) as unknown as ExpressionBuilder<DB, TCtx>;
}
