import {
  hasAggregate,
  splitCondition,
  type CompiledStatement,
  type Predicate,
  type QueryValue,
} from "@minnowdb/core/plan";
import { type ExecuteResult } from "@minnowdb/core";
import {
  buildBinaryCondition,
  createExpressionBuilder,
  isExpressionWrapper,
  literalExpression,
  type ComparisonOperatorToken,
  type ExpressionBuilder,
  type ExpressionWrapper,
  type InOperatorToken,
  type IsOperatorToken,
  type LikeOperatorToken,
  type SqlBool,
  type ValueOperand,
} from "./expression.js";
import { NoResultError } from "./select-query-builder.js";
import { renderMutationSql } from "./plan-sql.js";
import { type RenderedSql } from "./sql-tag.js";
import {
  materialize,
  type ColumnReference,
  type CompileContext,
  type ExpressionSource,
  type InsertRowOf,
  type ReferencedValue,
  type Simplify,
  type SelectRowOf,
  type UpdateRowOf,
} from "./types.js";

/**
 * Kysely-style mutation builders. `execute()` returns an array (one result object, or the
 * `returning(...)` rows); the idiomatic call is `executeTakeFirst()` / `executeTakeFirstOrThrow()`.
 * Inserts pad their rows against the schema and hand them to the engine's batch APIs
 * (`orReplace()` routes to the upsert path)
 * and `returning` echoes the written rows: the padded inputs overlaid with the engine's
 * generated columns (defaults and auto-increment keys), so callers get generated ids back.
 * Updates and deletes run as parameterized SQL; their `returning` rows come back from
 * the statement's own snapshot (post-update values for updates, the deleted rows for deletes).
 */

export interface InsertResult {
  readonly numInsertedRows: number;
}

export interface UpdateResult {
  readonly numUpdatedRows: number;
}

export interface DeleteResult {
  readonly numDeletedRows: number;
}

export interface MutationServices {
  insertBatch(
    tableName: string,
    rows: ReadonlyArray<Readonly<Record<string, QueryValue>>>,
  ): Promise<{ rowCount: number; generatedColumns?: Record<string, QueryValue[]> }>;
  upsertBatch(
    tableName: string,
    rows: ReadonlyArray<Readonly<Record<string, QueryValue>>>,
  ): Promise<{ rowCount: number; generatedColumns?: Record<string, QueryValue[]> }>;
  execute(sql: string, params: readonly QueryValue[]): Promise<ExecuteResult>;
  /** The table's full column list; inserts use it to pad omitted nullable columns. */
  tableColumns?(tableName: string): Promise<readonly string[] | undefined>;
  /**
   * Default generators by column name, when the client knows the schema. Inserts call
   * them for omitted-or-null slots before the batch is sent — the engine never sees the
   * functions, only the generated values.
   */
  columnDefaultFns?(tableName: string): Readonly<Record<string, () => QueryValue>> | undefined;
}

type ReturningState = readonly string[] | "*" | undefined;

const mutationCompileContext: CompileContext = {
  nextSequence: () => {
    throw new TypeError("Derived tables are not supported in mutations");
  },
  cteFor: () => undefined,
};

async function takeFirst<TReturn>(results: Promise<TReturn[]>): Promise<TReturn | undefined> {
  return (await results)[0];
}

async function takeFirstOrThrow<TReturn>(results: Promise<TReturn[]>): Promise<TReturn> {
  const first = await takeFirst(results);
  if (first === undefined) throw new NoResultError();
  return first;
}

// --- INSERT -------------------------------------------------------------------------------------

interface InsertRows {
  readonly previous: InsertRows | undefined;
  readonly rows: ReadonlyArray<Readonly<Record<string, unknown>>>;
}

function collectInsertRows(
  tail: InsertRows | undefined,
): ReadonlyArray<Readonly<Record<string, unknown>>> {
  const chunks: Array<InsertRows["rows"]> = [];
  for (let node = tail; node !== undefined; node = node.previous) chunks.push(node.rows);
  chunks.reverse();
  return chunks.flat();
}

export class InsertQueryBuilder<
  in out DB,
  in out TTable extends keyof DB & string,
  out TReturn = InsertResult,
> {
  /** Type-only: the execute() element, e.g. `typeof q.$inferResult`. Undefined at runtime. */
  declare readonly $inferResult: TReturn;

  constructor(
    private readonly services: MutationServices,
    private readonly table: TTable,
    private readonly rowList?: InsertRows,
    private readonly replaceOnConflict = false,
    private readonly returningColumns?: ReturningState,
  ) {}

  values(
    rows: InsertRowOf<DB[TTable]> | ReadonlyArray<InsertRowOf<DB[TTable]>>,
  ): InsertQueryBuilder<DB, TTable, TReturn> {
    const additions = (Array.isArray(rows) ? rows : [rows]) as ReadonlyArray<
      Readonly<Record<string, unknown>>
    >;
    return new InsertQueryBuilder(
      this.services,
      this.table,
      { previous: this.rowList, rows: additions },
      this.replaceOnConflict,
      this.returningColumns,
    );
  }

  /** Replaces the whole row when the unique key already exists (the engine's upsert). */
  orReplace(): InsertQueryBuilder<DB, TTable, TReturn> {
    return new InsertQueryBuilder(
      this.services,
      this.table,
      this.rowList,
      true,
      this.returningColumns,
    );
  }

  /** Returns the written rows projected to these columns instead of an InsertResult. */
  returning<TCol extends keyof SelectRowOf<DB[TTable]> & string>(
    columns: readonly TCol[],
  ): InsertQueryBuilder<DB, TTable, Simplify<Pick<SelectRowOf<DB[TTable]>, TCol>>> {
    return new InsertQueryBuilder(
      this.services,
      this.table,
      this.rowList,
      this.replaceOnConflict,
      columns,
    );
  }

  /** Returns the complete written rows instead of an InsertResult. */
  returningAll(): InsertQueryBuilder<DB, TTable, Simplify<SelectRowOf<DB[TTable]>>> {
    return new InsertQueryBuilder(
      this.services,
      this.table,
      this.rowList,
      this.replaceOnConflict,
      "*",
    );
  }

  async execute(): Promise<TReturn[]> {
    const rows = collectInsertRows(this.rowList);
    if (rows.length === 0) throw new TypeError("insertInto() requires values()");
    // With the schema known, omitted nullable columns pad with null (the batch API takes every
    // column) and function defaults fill their omitted-or-null slots; unknown extra keys still
    // surface as engine errors.
    const columns = new Set<string>((await this.services.tableColumns?.(this.table)) ?? []);
    for (const row of rows) {
      for (const key of Object.keys(row)) columns.add(key);
    }
    const names = [...columns];
    const defaults = this.services.columnDefaultFns?.(this.table);
    const padded = rows.map((row) =>
      Object.fromEntries(
        names.map((name) => {
          const value = (row[name] ?? null) as QueryValue;
          const fill = defaults?.[name];
          return [name, value === null && fill !== undefined ? fill() : value];
        }),
      ),
    );
    const result = this.replaceOnConflict
      ? await this.services.upsertBatch(this.table, padded)
      : await this.services.insertBatch(this.table, padded);
    if (this.returningColumns === undefined) {
      return [{ numInsertedRows: result.rowCount } as TReturn];
    }
    // The written rows are the padded inputs overlaid with the engine's generated columns
    // (defaults and auto-increment keys), in insertion order.
    const generated = result.generatedColumns ?? {};
    const written = padded.map((row, index) => {
      const overlay = { ...row };
      for (const [name, values] of Object.entries(generated)) {
        overlay[name] = values[index] ?? null;
      }
      return overlay;
    });
    const projected =
      this.returningColumns === "*"
        ? written
        : written.map((row) =>
            Object.fromEntries(
              (this.returningColumns as readonly string[]).map((name) => [name, row[name] ?? null]),
            ),
          );
    return projected as TReturn[];
  }

  async executeTakeFirst(): Promise<TReturn | undefined> {
    return takeFirst(this.execute());
  }

  async executeTakeFirstOrThrow(): Promise<TReturn> {
    return takeFirstOrThrow(this.execute());
  }
}

// --- Shared filtered-mutation machinery ---------------------------------------------------------

type Ctx<DB, TTable extends keyof DB & string> = Record<TTable, SelectRowOf<DB[TTable]>>;

type MutationWhereFactory<DB, TTable extends keyof DB & string> = (
  eb: ExpressionBuilder<DB, Ctx<DB, TTable>>,
) => ExpressionWrapper<SqlBool>;

function mutationWhereSource(
  lhs: unknown,
  operator: string | undefined,
  rhs: unknown,
): ExpressionSource {
  if (typeof lhs === "function" && operator === undefined) {
    const eb = createExpressionBuilder(() => {
      throw new TypeError("Subqueries are not supported in mutation predicates");
    });
    return (lhs as (eb: unknown) => ExpressionWrapper<SqlBool>)(eb).source;
  }
  if (operator === undefined) throw new TypeError("where() requires an operator");
  return buildBinaryCondition(lhs, operator, rhs);
}

function compileMutationPredicates(wheres: readonly ExpressionSource[]): Predicate[] {
  const predicates = wheres.flatMap((where) =>
    splitCondition(materialize(where, mutationCompileContext)),
  );
  for (const predicate of predicates) {
    if (hasAggregate(predicate.left) || hasAggregate(predicate.right)) {
      throw new TypeError("Aggregate functions are not allowed in mutation predicates");
    }
  }
  return predicates;
}

async function runReturning<TReturn>(
  services: MutationServices,
  rendered: RenderedSql,
): Promise<TReturn[]> {
  const result = await services.execute(rendered.sql, rendered.params);
  if (result.kind !== "update" && result.kind !== "delete") {
    throw new TypeError(`Expected a row mutation result, received ${result.kind}`);
  }
  return (result.returnedRows ?? []) as TReturn[];
}

// --- UPDATE -------------------------------------------------------------------------------------

export class UpdateQueryBuilder<
  in out DB,
  in out TTable extends keyof DB & string,
  out TReturn = UpdateResult,
> {
  /** Type-only: the execute() element, e.g. `typeof q.$inferResult`. Undefined at runtime. */
  declare readonly $inferResult: TReturn;
  private renderedSql: RenderedSql | undefined;

  constructor(
    private readonly services: MutationServices,
    private readonly table: TTable,
    private readonly wheres: readonly ExpressionSource[] = [],
    private readonly assignments: ReadonlyArray<{
      column: string;
      source: ExpressionSource;
    }> = [],
    private readonly returningColumns?: ReturningState,
  ) {}

  set<TCol extends keyof UpdateRowOf<DB[TTable]> & string>(
    column: TCol,
    value: UpdateRowOf<DB[TTable]>[TCol] | ExpressionWrapper<UpdateRowOf<DB[TTable]>[TCol]>,
  ): UpdateQueryBuilder<DB, TTable, TReturn>;
  set(
    changes:
      | UpdateRowOf<DB[TTable]>
      | ((eb: ExpressionBuilder<DB, Ctx<DB, TTable>>) => {
          [K in keyof UpdateRowOf<DB[TTable]>]?:
            UpdateRowOf<DB[TTable]>[K] | ExpressionWrapper<UpdateRowOf<DB[TTable]>[K]> | undefined;
        }),
  ): UpdateQueryBuilder<DB, TTable, TReturn>;
  set(changes: unknown, value?: unknown): UpdateQueryBuilder<DB, TTable, TReturn> {
    const resolved: Record<string, unknown> =
      typeof changes === "string"
        ? { [changes]: value }
        : typeof changes === "function"
          ? (changes as (eb: ExpressionBuilder<DB, Ctx<DB, TTable>>) => Record<string, unknown>)(
              createExpressionBuilder<DB, Ctx<DB, TTable>>(() => {
                throw new TypeError("Subqueries are not supported in UPDATE assignments");
              }),
            )
          : (changes as Record<string, unknown>);
    const additions = Object.entries(resolved)
      // Kysely convention: an undefined value means "leave this column untouched", so
      // `set({ ...partialPatch })` is safe. An explicit null still writes NULL.
      .filter(([, entry]) => entry !== undefined)
      .map(([column, entry]) => ({
        column,
        source: isExpressionWrapper(entry) ? entry.source : literalExpression(entry),
      }));
    return new UpdateQueryBuilder(
      this.services,
      this.table,
      this.wheres,
      [...this.assignments, ...additions],
      this.returningColumns,
    );
  }

  where<TRef extends ColumnReference<Ctx<DB, TTable>> & string>(
    lhs: TRef,
    operator: ComparisonOperatorToken | LikeOperatorToken,
    rhs: ValueOperand<ReferencedValue<Ctx<DB, TTable>, TRef>>,
  ): UpdateQueryBuilder<DB, TTable, TReturn>;
  // No subquery arm here: mutations reject derived tables at runtime, so IN takes only values.
  where<TRef extends ColumnReference<Ctx<DB, TTable>> & string>(
    lhs: TRef,
    operator: InOperatorToken,
    rhs: ReadonlyArray<ReferencedValue<Ctx<DB, TTable>, TRef>>,
  ): UpdateQueryBuilder<DB, TTable, TReturn>;
  where(
    lhs: ColumnReference<Ctx<DB, TTable>>,
    operator: IsOperatorToken,
    rhs: null,
  ): UpdateQueryBuilder<DB, TTable, TReturn>;
  where(factory: MutationWhereFactory<DB, TTable>): UpdateQueryBuilder<DB, TTable, TReturn>;
  where(lhs: unknown, operator?: string, rhs?: unknown): UpdateQueryBuilder<DB, TTable, TReturn> {
    return new UpdateQueryBuilder(
      this.services,
      this.table,
      [...this.wheres, mutationWhereSource(lhs, operator, rhs)],
      this.assignments,
      this.returningColumns,
    );
  }

  /** Returns the affected rows (post-update values) projected to these columns. */
  returning<TCol extends keyof SelectRowOf<DB[TTable]> & string>(
    columns: readonly TCol[],
  ): UpdateQueryBuilder<DB, TTable, Simplify<Pick<SelectRowOf<DB[TTable]>, TCol>>> {
    return new UpdateQueryBuilder(
      this.services,
      this.table,
      this.wheres,
      this.assignments,
      columns,
    );
  }

  /** Returns the complete affected rows (post-update values). */
  returningAll(): UpdateQueryBuilder<DB, TTable, Simplify<SelectRowOf<DB[TTable]>>> {
    return new UpdateQueryBuilder(this.services, this.table, this.wheres, this.assignments, "*");
  }

  compile(): Extract<CompiledStatement, { kind: "update" }> {
    if (this.assignments.length === 0) throw new TypeError("updateTable() requires set()");
    const compiled = this.assignments.map(({ column, source }) => {
      const expression = materialize(source, mutationCompileContext);
      if (hasAggregate(expression)) {
        throw new TypeError("Aggregate functions are not allowed in UPDATE assignments");
      }
      return { column, expression };
    });
    if (new Set(compiled.map(({ column }) => column)).size !== compiled.length) {
      throw new TypeError("UPDATE assignments must set each column once");
    }
    return {
      kind: "update",
      table: this.table,
      assignments: compiled,
      predicates: compileMutationPredicates(this.wheres),
    };
  }

  /** Renders this immutable update as parameterized SQL. */
  toSQL(): RenderedSql {
    const rendered = this.#renderSql();
    return { sql: rendered.sql, params: [...rendered.params] };
  }

  #renderSql(): RenderedSql {
    return (this.renderedSql ??= renderMutationSql(this.compile(), this.returningColumns));
  }

  async execute(): Promise<TReturn[]> {
    const rendered = this.#renderSql();
    if (this.returningColumns !== undefined) {
      return runReturning(this.services, rendered);
    }
    const result = await this.services.execute(rendered.sql, rendered.params);
    if (result.kind !== "update") {
      throw new TypeError(`Expected an update result, received ${result.kind}`);
    }
    return [{ numUpdatedRows: result.rowCount } as TReturn];
  }

  async executeTakeFirst(): Promise<TReturn | undefined> {
    return takeFirst(this.execute());
  }

  async executeTakeFirstOrThrow(): Promise<TReturn> {
    return takeFirstOrThrow(this.execute());
  }
}

// --- DELETE -------------------------------------------------------------------------------------

export class DeleteQueryBuilder<
  in out DB,
  in out TTable extends keyof DB & string,
  out TReturn = DeleteResult,
> {
  /** Type-only: the execute() element, e.g. `typeof q.$inferResult`. Undefined at runtime. */
  declare readonly $inferResult: TReturn;
  private renderedSql: RenderedSql | undefined;

  constructor(
    private readonly services: MutationServices,
    private readonly table: TTable,
    private readonly wheres: readonly ExpressionSource[] = [],
    private readonly returningColumns?: ReturningState,
  ) {}

  where<TRef extends ColumnReference<Ctx<DB, TTable>> & string>(
    lhs: TRef,
    operator: ComparisonOperatorToken | LikeOperatorToken,
    rhs: ValueOperand<ReferencedValue<Ctx<DB, TTable>, TRef>>,
  ): DeleteQueryBuilder<DB, TTable, TReturn>;
  // No subquery arm here: mutations reject derived tables at runtime, so IN takes only values.
  where<TRef extends ColumnReference<Ctx<DB, TTable>> & string>(
    lhs: TRef,
    operator: InOperatorToken,
    rhs: ReadonlyArray<ReferencedValue<Ctx<DB, TTable>, TRef>>,
  ): DeleteQueryBuilder<DB, TTable, TReturn>;
  where(
    lhs: ColumnReference<Ctx<DB, TTable>>,
    operator: IsOperatorToken,
    rhs: null,
  ): DeleteQueryBuilder<DB, TTable, TReturn>;
  where(factory: MutationWhereFactory<DB, TTable>): DeleteQueryBuilder<DB, TTable, TReturn>;
  where(lhs: unknown, operator?: string, rhs?: unknown): DeleteQueryBuilder<DB, TTable, TReturn> {
    return new DeleteQueryBuilder(
      this.services,
      this.table,
      [...this.wheres, mutationWhereSource(lhs, operator, rhs)],
      this.returningColumns,
    );
  }

  /** Returns the deleted rows projected to these columns. */
  returning<TCol extends keyof SelectRowOf<DB[TTable]> & string>(
    columns: readonly TCol[],
  ): DeleteQueryBuilder<DB, TTable, Simplify<Pick<SelectRowOf<DB[TTable]>, TCol>>> {
    return new DeleteQueryBuilder(this.services, this.table, this.wheres, columns);
  }

  /** Returns the complete deleted rows. */
  returningAll(): DeleteQueryBuilder<DB, TTable, Simplify<SelectRowOf<DB[TTable]>>> {
    return new DeleteQueryBuilder(this.services, this.table, this.wheres, "*");
  }

  compile(): Extract<CompiledStatement, { kind: "delete" }> {
    return {
      kind: "delete",
      table: this.table,
      predicates: compileMutationPredicates(this.wheres),
    };
  }

  /** Renders this immutable delete as parameterized SQL. */
  toSQL(): RenderedSql {
    const rendered = this.#renderSql();
    return { sql: rendered.sql, params: [...rendered.params] };
  }

  #renderSql(): RenderedSql {
    return (this.renderedSql ??= renderMutationSql(this.compile(), this.returningColumns));
  }

  async execute(): Promise<TReturn[]> {
    const rendered = this.#renderSql();
    if (this.returningColumns !== undefined) {
      return runReturning(this.services, rendered);
    }
    const result = await this.services.execute(rendered.sql, rendered.params);
    if (result.kind !== "delete") {
      throw new TypeError(`Expected a delete result, received ${result.kind}`);
    }
    return [{ numDeletedRows: result.rowCount } as TReturn];
  }

  async executeTakeFirst(): Promise<TReturn | undefined> {
    return takeFirst(this.execute());
  }

  async executeTakeFirstOrThrow(): Promise<TReturn> {
    return takeFirstOrThrow(this.execute());
  }
}
