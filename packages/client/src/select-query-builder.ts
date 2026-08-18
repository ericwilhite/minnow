import { optimizePlan } from "@minnowdb/core/plan";
import {
  assembleSelectBlock,
  compoundSelectBlock,
  derivedTableSource,
  splitCondition,
  type CompiledQuery,
  type Expression,
  type JoinPlan,
  type Predicate,
  type SelectItem,
  type SetOperator,
  type TableSource,
  validateLimit,
  validateOffset,
} from "@minnowdb/core/plan";
import {
  buildBinaryCondition,
  buildFtsExpression,
  createExpressionBuilder,
  isExpressionWrapper,
  ExpressionWrapper,
  type ComparisonOperatorToken,
  type ExpressionBuilder,
  type InOperatorToken,
  type IsOperatorToken,
  type LikeOperatorToken,
  type ScalarSubquery,
  type SqlBool,
  type ValueOperand,
} from "./expression.js";
import { LiveQuery, type LiveQueryServices } from "./live-query.js";
import {
  materialize,
  type AliasedExpression,
  type AllColumnsRow,
  type BlockCompilable,
  type ColumnReference,
  type CompileContext,
  type ContextWithLeftTable,
  type ContextWithTable,
  type ExpressionSource,
  type JoinResult,
  type ReferencedValue,
  type RowFromSelections,
  type ColumnReferenceOf,
  type Selection,
  type Simplify,
  type TableExpression,
  type TypedQueryEnvelope,
} from "./types.js";

/**
 * Kysely-style immutable select builder. Every method returns a new builder; `compile()` runs the
 * exact assembly pipeline the SQL parser ends in (`assembleSelectBlock` and friends with a shared
 * derived-source sequence), so a builder query and its equivalent SQL produce identical optimized
 * plans — same validation errors, same desugars, same execution strategy.
 */

/** Thrown by `executeTakeFirstOrThrow` when the query returned no rows. */
export class NoResultError extends Error {
  constructor() {
    super("The query returned no result");
    this.name = "NoResultError";
  }
}

export interface ExecuteServices {
  run<TRow>(query: TypedQueryEnvelope<TRow>): Promise<TRow[]>;
  live: LiveQueryServices | undefined;
}

/** A sub-select named for use as a derived table in `selectFrom`. */
export interface AliasedSelectQuery<out TRow, out TAlias extends string> {
  readonly kind: "dsl-aliased-select";
  readonly alias: TAlias;
  readonly builder: BlockCompilable;
  readonly __row?: TRow;
}

export function isAliasedSelectQuery(value: unknown): value is AliasedSelectQuery<unknown, string> {
  return (
    typeof value === "object" &&
    value !== null &&
    (value as { kind?: unknown }).kind === "dsl-aliased-select"
  );
}

export interface CteDefinition {
  readonly name: string;
  readonly builder: BlockCompilable;
}

type SourceState =
  | { kind: "table"; table: string; alias: string }
  | { kind: "derived"; builder: BlockCompilable; alias: string };

interface JoinState {
  join: "inner" | "left";
  source: SourceState;
  on: ExpressionSource;
}

interface SelectionState {
  alias: string;
  source: ExpressionSource;
}

interface SelectState {
  ctes: readonly CteDefinition[];
  base: SourceState;
  joins: readonly JoinState[];
  wheres: readonly ExpressionSource[];
  groupBys: readonly ExpressionSource[];
  havings: readonly ExpressionSource[];
  orderBys: ReadonlyArray<{ source: ExpressionSource; direction: "asc" | "desc" }>;
  selections: readonly SelectionState[] | "all" | undefined;
  distinct: boolean;
  limit?: number;
  offset?: number;
  setOps: ReadonlyArray<{ op: SetOperator; builder: BlockCompilable }>;
}

export function parseTableExpression(expression: string): { table: string; alias: string } {
  const match = /^(.+?)\s+as\s+(.+)$/i.exec(expression);
  if (match === null) return { table: expression, alias: expression };
  const table = match[1]?.trim() ?? expression;
  const alias = match[2]?.trim() ?? expression;
  return { table, alias };
}

type WhereFactory<DB, TCtx> = (eb: ExpressionBuilder<DB, TCtx>) => ExpressionWrapper<SqlBool>;

type SelectFactory<DB, TCtx, TSel> = (eb: ExpressionBuilder<DB, TCtx>) => readonly TSel[];

export class JoinBuilder<in out TCtx> {
  constructor(readonly conditions: readonly ExpressionSource[] = []) {}

  /** Joins on two columns; the usual `ON a.x = b.y`. */
  onRef(
    lhs: ColumnReference<TCtx>,
    operator: ComparisonOperatorToken,
    rhs: ColumnReference<TCtx>,
  ): JoinBuilder<TCtx> {
    return new JoinBuilder([
      ...this.conditions,
      buildBinaryCondition(
        new ExpressionWrapper({ kind: "column", reference: lhs }),
        operator,
        new ExpressionWrapper({ kind: "column", reference: rhs }),
      ),
    ]);
  }

  /** Joins a column against a value. */
  on<TRef extends ColumnReference<TCtx> & string>(
    lhs: TRef,
    operator: ComparisonOperatorToken | LikeOperatorToken,
    rhs: ValueOperand<ReferencedValue<TCtx, TRef>>,
  ): JoinBuilder<TCtx> {
    return new JoinBuilder([...this.conditions, buildBinaryCondition(lhs, operator, rhs)]);
  }

  /** @internal ANDs the accumulated conditions; a single condition stays an equi-join candidate. */
  get onSource(): ExpressionSource {
    const [first, ...rest] = this.conditions;
    if (first === undefined) {
      throw new TypeError("A join requires at least one ON condition");
    }
    if (rest.length === 0) return first;
    return (context) => {
      let folded = materialize(first, context);
      for (const source of rest) {
        folded = {
          kind: "logical",
          operator: "and",
          left: folded,
          right: materialize(source, context),
        };
      }
      return folded;
    };
  }
}

type JoinCallback<TCtx> = (join: JoinBuilder<TCtx>) => JoinBuilder<TCtx>;

export class SelectQueryBuilder<in out DB, in out TCtx, out TRow> implements BlockCompilable {
  readonly kind = "dsl-select-builder" as const;

  /** Type-only: the output row, e.g. `type Row = typeof query.$inferRow`. Undefined at runtime. */
  declare readonly $inferRow: TRow;

  constructor(
    private readonly state: SelectState,
    private readonly services: ExecuteServices | undefined,
  ) {}

  static create<DB, TCtx, TRow>(
    source: SourceState,
    ctes: readonly CteDefinition[],
    services: ExecuteServices | undefined,
  ): SelectQueryBuilder<DB, TCtx, TRow> {
    return new SelectQueryBuilder(
      {
        ctes,
        base: source,
        joins: [],
        wheres: [],
        groupBys: [],
        havings: [],
        orderBys: [],
        selections: undefined,
        distinct: false,
        setOps: [],
      },
      services,
    );
  }

  #with(patch: Partial<SelectState>): SelectState {
    return { ...this.state, ...patch };
  }

  #eb<TNextCtx>(): ExpressionBuilder<DB, TNextCtx> {
    const { ctes } = this.state;
    const services = this.services;
    return createExpressionBuilder((table: string) => {
      const { table: name, alias } = parseTableExpression(table);
      return SelectQueryBuilder.create({ kind: "table", table: name, alias }, ctes, services);
    });
  }

  // --- Joins ------------------------------------------------------------------------------------

  /* eslint-disable @typescript-eslint/no-explicit-any --
     the join implementation signatures erase the evolving context type; the public overloads
     above them carry the precise ContextWithTable/ContextWithLeftTable types. */
  innerJoin<TE extends TableExpression<DB>>(
    table: TE,
    lhs: ColumnReference<ContextWithTable<DB, TCtx, TE>>,
    rhs: ColumnReference<ContextWithTable<DB, TCtx, TE>>,
  ): JoinResult<TCtx, TE, SelectQueryBuilder<DB, ContextWithTable<DB, TCtx, TE>, TRow>>;
  innerJoin<TE extends TableExpression<DB>>(
    table: TE,
    callback: JoinCallback<ContextWithTable<DB, TCtx, TE>>,
  ): JoinResult<TCtx, TE, SelectQueryBuilder<DB, ContextWithTable<DB, TCtx, TE>, TRow>>;
  innerJoin(table: string, lhsOrCallback: unknown, rhs?: unknown): any {
    return this.#join("inner", table, lhsOrCallback, rhs);
  }

  leftJoin<TE extends TableExpression<DB>>(
    table: TE,
    lhs: ColumnReference<ContextWithTable<DB, TCtx, TE>>,
    rhs: ColumnReference<ContextWithTable<DB, TCtx, TE>>,
  ): JoinResult<TCtx, TE, SelectQueryBuilder<DB, ContextWithLeftTable<DB, TCtx, TE>, TRow>>;
  leftJoin<TE extends TableExpression<DB>>(
    table: TE,
    callback: JoinCallback<ContextWithTable<DB, TCtx, TE>>,
  ): JoinResult<TCtx, TE, SelectQueryBuilder<DB, ContextWithLeftTable<DB, TCtx, TE>, TRow>>;
  leftJoin(table: string, lhsOrCallback: unknown, rhs?: unknown): any {
    return this.#join("left", table, lhsOrCallback, rhs);
  }

  #join(
    join: "inner" | "left",
    table: string,
    lhsOrCallback: unknown,
    rhs: unknown,
  ): SelectQueryBuilder<DB, any, TRow> {
    const { table: name, alias } = parseTableExpression(table);
    // CTE names resolve to derived sources at compile time, in #compileSource.
    const source: SourceState = { kind: "table", table: name, alias };
    const on =
      typeof lhsOrCallback === "function"
        ? (lhsOrCallback as JoinCallback<Record<string, Record<string, unknown>>>)(
            new JoinBuilder(),
          ).onSource
        : new JoinBuilder().onRef(lhsOrCallback as never, "=", rhs as never).onSource;
    return new SelectQueryBuilder(
      this.#with({ joins: [...this.state.joins, { join, source, on }] }),
      this.services,
    );
  }
  /* eslint-enable @typescript-eslint/no-explicit-any */

  // --- Filters and grouping ---------------------------------------------------------------------

  where<TRef extends ColumnReference<TCtx> & string>(
    lhs: TRef,
    operator: ComparisonOperatorToken | LikeOperatorToken,
    rhs: ValueOperand<ReferencedValue<TCtx, TRef>>,
  ): SelectQueryBuilder<DB, TCtx, TRow>;
  where<TRef extends ColumnReference<TCtx> & string>(
    lhs: TRef,
    operator: InOperatorToken,
    rhs: ReadonlyArray<ReferencedValue<TCtx, TRef>> | ScalarSubquery<ReferencedValue<TCtx, TRef>>,
  ): SelectQueryBuilder<DB, TCtx, TRow>;
  where(
    lhs: ColumnReference<TCtx>,
    operator: IsOperatorToken,
    rhs: null,
  ): SelectQueryBuilder<DB, TCtx, TRow>;
  where(factory: WhereFactory<DB, TCtx>): SelectQueryBuilder<DB, TCtx, TRow>;
  where(lhs: unknown, operator?: string, rhs?: unknown): SelectQueryBuilder<DB, TCtx, TRow> {
    const source = this.#booleanSource(lhs, operator, rhs);
    return new SelectQueryBuilder(
      this.#with({ wheres: [...this.state.wheres, source] }),
      this.services,
    );
  }

  /**
   * Typeahead-friendly document search: filters to rows matching every query term and orders
   * by BM25 relevance, descending. Pure sugar for
   * `.where(eb.match(columns, query)).orderBy(fn.bm25(columns, query), "desc")` — the ordering
   * expression rides as a hidden select item, so the row shape is exactly what you selected
   * and repeated `.search()` calls compose. Wanting the score value is the only reason to also
   * select `fn.bm25(...)`; execution (index pruning, streaming under a budget) is identical
   * either way. Columns default to "*" (every non-boolean column of the scanned table); a
   * named select list is required, since hidden ordering columns cannot ride along a wildcard.
   */
  search(
    query: string,
    options: {
      columns?: "*" | ReadonlyArray<ColumnReferenceOf<TCtx, string | number | Date> & string>;
    } = {},
  ): SelectQueryBuilder<DB, TCtx, TRow> {
    const columns = options.columns ?? "*";
    return this.where((eb) => eb.match(columns, query)).orderBy(
      new ExpressionWrapper<number | null>(buildFtsExpression("bm25", columns, query)),
      "desc",
    );
  }

  having<TRef extends ColumnReference<TCtx> & string>(
    lhs: TRef,
    operator: ComparisonOperatorToken | LikeOperatorToken,
    rhs: ValueOperand<ReferencedValue<TCtx, TRef>>,
  ): SelectQueryBuilder<DB, TCtx, TRow>;
  having<TRef extends ColumnReference<TCtx> & string>(
    lhs: TRef,
    operator: InOperatorToken,
    rhs: ReadonlyArray<ReferencedValue<TCtx, TRef>> | ScalarSubquery<ReferencedValue<TCtx, TRef>>,
  ): SelectQueryBuilder<DB, TCtx, TRow>;
  having(
    lhs: ColumnReference<TCtx>,
    operator: IsOperatorToken,
    rhs: null,
  ): SelectQueryBuilder<DB, TCtx, TRow>;
  having(factory: WhereFactory<DB, TCtx>): SelectQueryBuilder<DB, TCtx, TRow>;
  having(lhs: unknown, operator?: string, rhs?: unknown): SelectQueryBuilder<DB, TCtx, TRow> {
    const source = this.#booleanSource(lhs, operator, rhs);
    return new SelectQueryBuilder(
      this.#with({ havings: [...this.state.havings, source] }),
      this.services,
    );
  }

  #booleanSource(lhs: unknown, operator?: string, rhs?: unknown): ExpressionSource {
    if (typeof lhs === "function" && operator === undefined) {
      const wrapper = (lhs as WhereFactory<DB, TCtx>)(this.#eb());
      return wrapper.source;
    }
    if (operator === undefined) throw new TypeError("where() requires an operator");
    return buildBinaryCondition(lhs, operator, rhs);
  }

  groupBy(
    references:
      | ColumnReference<TCtx>
      | ExpressionWrapper<unknown>
      | ReadonlyArray<ColumnReference<TCtx> | ExpressionWrapper<unknown>>
      | ((
          eb: ExpressionBuilder<DB, TCtx>,
        ) => ExpressionWrapper<unknown> | ReadonlyArray<ExpressionWrapper<unknown>>),
  ): SelectQueryBuilder<DB, TCtx, TRow> {
    const resolved = typeof references === "function" ? references(this.#eb()) : references;
    const list = Array.isArray(resolved) ? resolved : [resolved];
    const sources = list.map((reference) =>
      isExpressionWrapper(reference)
        ? reference.source
        : ({ kind: "column", reference: reference as string } satisfies Expression),
    );
    return new SelectQueryBuilder(
      this.#with({ groupBys: [...this.state.groupBys, ...sources] }),
      this.services,
    );
  }

  orderBy(
    reference:
      | ColumnReference<TCtx>
      | (keyof TRow & string)
      | ExpressionWrapper<unknown>
      | ((eb: ExpressionBuilder<DB, TCtx>) => ExpressionWrapper<unknown>),
    direction: "asc" | "desc" = "asc",
  ): SelectQueryBuilder<DB, TCtx, TRow> {
    const resolved = typeof reference === "function" ? reference(this.#eb()) : reference;
    const source: ExpressionSource = isExpressionWrapper(resolved)
      ? resolved.source
      : { kind: "column", reference: resolved };
    return new SelectQueryBuilder(
      this.#with({ orderBys: [...this.state.orderBys, { source, direction }] }),
      this.services,
    );
  }

  limit(limit: number): SelectQueryBuilder<DB, TCtx, TRow> {
    return new SelectQueryBuilder(this.#with({ limit: validateLimit(limit) }), this.services);
  }

  offset(offset: number): SelectQueryBuilder<DB, TCtx, TRow> {
    if (this.state.limit === undefined) {
      throw new TypeError("OFFSET requires LIMIT; call limit() first");
    }
    return new SelectQueryBuilder(this.#with({ offset: validateOffset(offset) }), this.services);
  }

  distinct(): SelectQueryBuilder<DB, TCtx, TRow> {
    return new SelectQueryBuilder(this.#with({ distinct: true }), this.services);
  }

  // --- Selections -------------------------------------------------------------------------------

  select<TSel extends Selection<TCtx>>(
    selections: readonly TSel[] | SelectFactory<DB, TCtx, TSel>,
  ): SelectQueryBuilder<DB, TCtx, Simplify<TRow & RowFromSelections<TCtx, TSel>>>;
  /* eslint-disable @typescript-eslint/unified-signatures --
     kept as a separate overload: a `TSel | readonly TSel[]` union makes factory returns infer
     the array itself as TSel, collapsing the selection types. */
  select<TSel extends Selection<TCtx>>(
    selection: TSel | ((eb: ExpressionBuilder<DB, TCtx>) => TSel),
  ): SelectQueryBuilder<DB, TCtx, Simplify<TRow & RowFromSelections<TCtx, TSel>>>;
  /* eslint-enable @typescript-eslint/unified-signatures */
  select<TSel extends Selection<TCtx>>(
    selections:
      TSel | readonly TSel[] | ((eb: ExpressionBuilder<DB, TCtx>) => TSel | readonly TSel[]),
  ): SelectQueryBuilder<DB, TCtx, Simplify<TRow & RowFromSelections<TCtx, TSel>>> {
    const resolved = typeof selections === "function" ? selections(this.#eb()) : selections;
    const list: ReadonlyArray<Selection<TCtx>> = Array.isArray(resolved) ? resolved : [resolved];
    const additions = list.map((selection) => this.#selection(selection));
    if (this.state.selections === "all") {
      throw new TypeError("SELECT * cannot be mixed with other expressions");
    }
    const merged = [...(this.state.selections ?? []), ...additions];
    return new SelectQueryBuilder(this.#with({ selections: merged }), this.services);
  }

  selectAll(): SelectQueryBuilder<DB, TCtx, Simplify<TRow & AllColumnsRow<TCtx>>> {
    if (this.state.selections !== undefined && this.state.selections !== "all") {
      throw new TypeError("SELECT * cannot be mixed with other expressions");
    }
    return new SelectQueryBuilder(this.#with({ selections: "all" }), this.services);
  }

  #selection(selection: Selection<TCtx>): SelectionState {
    if (typeof selection === "string") {
      const aliased = /^(.+?)\s+as\s+(.+)$/i.exec(selection);
      const reference = (aliased?.[1] ?? selection).trim();
      const alias = (aliased?.[2] ?? reference.split(".").at(-1) ?? reference).trim();
      return { alias, source: { kind: "column", reference } };
    }
    // The unknown-guard is for untyped call sites, e.g. passing a subquery builder or its
    // .as() result (an AliasedSelectQuery) where an aliased expression belongs.
    const candidate: unknown = selection;
    if (
      typeof candidate !== "object" ||
      candidate === null ||
      (candidate as { kind?: unknown }).kind !== "aliased-expression"
    ) {
      throw new TypeError(
        "select() items must be column strings or expressions named with .as(alias)",
      );
    }
    const aliasedExpression = candidate as AliasedExpression<unknown, string>;
    return { alias: aliasedExpression.alias, source: aliasedExpression.source };
  }

  // --- Set operations ---------------------------------------------------------------------------

  union<TOtherCtx>(
    other: SelectQueryBuilder<DB, TOtherCtx, TRow>,
  ): SelectQueryBuilder<DB, TCtx, TRow> {
    return this.#setOp("union", other);
  }

  unionAll<TOtherCtx>(
    other: SelectQueryBuilder<DB, TOtherCtx, TRow>,
  ): SelectQueryBuilder<DB, TCtx, TRow> {
    return this.#setOp("union all", other);
  }

  intersect<TOtherCtx>(
    other: SelectQueryBuilder<DB, TOtherCtx, TRow>,
  ): SelectQueryBuilder<DB, TCtx, TRow> {
    return this.#setOp("intersect", other);
  }

  except<TOtherCtx>(
    other: SelectQueryBuilder<DB, TOtherCtx, TRow>,
  ): SelectQueryBuilder<DB, TCtx, TRow> {
    return this.#setOp("except", other);
  }

  #setOp(op: SetOperator, other: BlockCompilable): SelectQueryBuilder<DB, TCtx, TRow> {
    return new SelectQueryBuilder(
      this.#with({ setOps: [...this.state.setOps, { op, builder: other }] }),
      this.services,
    );
  }

  /** Names this query for use as a derived table in `selectFrom`. */
  as<TAlias extends string>(alias: TAlias): AliasedSelectQuery<TRow, TAlias> {
    return { kind: "dsl-aliased-select", alias, builder: this };
  }

  // --- Compilation ------------------------------------------------------------------------------

  /** Compiles to the optimized plan envelope `run()` executes. */
  compile(): TypedQueryEnvelope<TRow> {
    let sequence = 0;
    const cteBlocks = new Map<string, CompiledQuery>();
    const context: CompileContext = {
      nextSequence: () => {
        sequence += 1;
        return sequence;
      },
      cteFor: (name) => {
        const block = cteBlocks.get(name);
        return block === undefined ? undefined : structuredClone(block);
      },
    };
    for (const cte of this.state.ctes) {
      if (cteBlocks.has(cte.name)) throw new TypeError(`Duplicate CTE name: ${cte.name}`);
      cteBlocks.set(cte.name, cte.builder.compileBlock(context, "(cte)"));
    }
    return { kind: "typed-query", plan: optimizePlan(this.compileBlock(context, "(dsl)")) };
  }

  /** @internal Compiles this builder as one block under the caller's compile context. */
  compileBlock(context: CompileContext, label: string): CompiledQuery {
    if (this.state.setOps.length === 0) return this.#block(context, label, true);
    const first = this.#block(context, label, false);
    const members = [
      first,
      ...this.state.setOps.map(({ builder }) => builder.compileBlock(context, "(union member)")),
    ];
    return compoundSelectBlock(
      label,
      members,
      this.state.setOps.map(({ op }) => op),
      {
        orderBy: this.state.orderBys.map(({ source, direction }) => ({
          expression: materialize(source, context),
          direction,
        })),
        ...(this.state.limit === undefined ? {} : { limit: this.state.limit }),
        ...(this.state.offset === undefined ? {} : { offset: this.state.offset }),
      },
      context.nextSequence,
    );
  }

  #block(context: CompileContext, label: string, includeTail: boolean): CompiledQuery {
    // Materialization follows the parser's order exactly — select list, base source, join
    // sources with their ON conditions, WHERE, GROUP BY, HAVING, ORDER BY — so every derived
    // source numbers identically to the equivalent SQL.
    const select = this.#compileSelections(context);
    const base = this.#compileSource(context, this.state.base);
    const joins: JoinPlan[] = this.state.joins.map((join) => {
      const source = this.#compileSource(context, join.source);
      const condition = materialize(join.on, context);
      if (condition.kind === "condition" && condition.operator === "=") {
        return { ...source, kind: join.join, left: condition.left, right: condition.right };
      }
      // Any other boolean condition becomes a nested-loop join.
      return {
        ...source,
        kind: join.join,
        left: { kind: "literal", value: null },
        right: { kind: "literal", value: null },
        on: condition,
      };
    });
    const predicates: Predicate[] = this.state.wheres.flatMap((where) =>
      splitCondition(materialize(where, context)),
    );
    const groupBy = this.state.groupBys.map((source) => materialize(source, context));
    const having: Predicate[] = this.state.havings.flatMap((source) =>
      splitCondition(materialize(source, context)),
    );
    const orderBy = includeTail
      ? this.state.orderBys.map(({ source, direction }) => ({
          expression: materialize(source, context),
          direction,
        }))
      : [];
    return assembleSelectBlock(
      {
        sql: label,
        base,
        joins,
        select,
        distinct: this.state.distinct,
        predicates,
        groupBy,
        having,
        orderBy,
        ...(includeTail && this.state.limit !== undefined ? { limit: this.state.limit } : {}),
        ...(includeTail && this.state.offset !== undefined ? { offset: this.state.offset } : {}),
      },
      context.nextSequence,
    );
  }

  #compileSelections(context: CompileContext): SelectItem[] {
    const selections = this.state.selections;
    if (selections === undefined) {
      throw new TypeError("A query needs select() or selectAll() before compile()");
    }
    if (selections === "all") return [{ expression: { kind: "wildcard" }, alias: "*" }];
    if (selections.length === 0) {
      throw new TypeError("A query needs select() or selectAll() before compile()");
    }
    const aliases = new Set<string>();
    return selections.map(({ alias, source }) => {
      if (aliases.has(alias)) throw new TypeError(`Duplicate output column: ${alias}`);
      aliases.add(alias);
      return { expression: materialize(source, context), alias };
    });
  }

  #compileSource(context: CompileContext, source: SourceState): TableSource {
    if (source.kind === "derived") {
      const inner = source.builder.compileBlock(context, "(derived)");
      return derivedTableSource(inner, source.alias, context.nextSequence);
    }
    const cte = context.cteFor(source.table);
    if (cte !== undefined) return derivedTableSource(cte, source.alias, context.nextSequence);
    return { table: source.table, alias: source.alias };
  }

  // --- Execution --------------------------------------------------------------------------------

  #services(): ExecuteServices {
    if (this.services === undefined) {
      throw new TypeError("This builder is a subquery; execute the outer query instead");
    }
    return this.services;
  }

  async execute(): Promise<TRow[]> {
    return this.#services().run(this.compile());
  }

  async executeTakeFirst(): Promise<TRow | undefined> {
    const rows = await this.execute();
    return rows[0];
  }

  async executeTakeFirstOrThrow(): Promise<TRow> {
    const row = await this.executeTakeFirst();
    if (row === undefined) throw new NoResultError();
    return row;
  }

  /**
   * Turns the query into a live query: subscribers get the current result immediately and a
   * fresh result after any commit that may have changed it (digest-suppressed when identical).
   */
  live(): LiveQuery<TRow> {
    const services = this.#services();
    if (services.live === undefined) {
      throw new TypeError("Live queries need a database with live query support");
    }
    return new LiveQuery<TRow>(services.live, this.compile());
  }
}
