import {
  hasAggregate,
  splitCondition,
  type CompiledStatement,
  type Predicate,
  type QueryRow,
  type QueryValue,
} from "../query.js";
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
import {
  materialize,
  type ColumnReference,
  type CompileContext,
  type ExpressionSource,
  type InsertRowFor,
  type ReferencedValue,
  type Simplify,
  type UpdateChangesFor,
} from "./types.js";

/**
 * Kysely-style mutation builders. `execute()` returns an array (one result object, or the
 * `returning(...)` rows); the idiomatic call is `executeTakeFirst()` / `executeTakeFirstOrThrow()`.
 * Inserts pad their rows against the schema and hand them to the engine's batch APIs
 * (`orReplace()` routes to the upsert path)
 * and `returning` echoes the written rows: the padded inputs overlaid with the engine's
 * generated columns (defaults and auto-increment keys), so callers get generated ids back.
 * Updates and deletes compile to the same mutation statements SQL produces and
 * run through the engine's read-keys-then-apply pipeline; their `returning` rows come back from
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
  runStatement(
    statement: CompiledStatement,
    options?: { returning?: readonly string[] | "*" },
  ): Promise<{ kind: string; rowCount?: number; returnedRows?: QueryRow[] }>;
  /** The table's full column list when the facade knows the schema; inserts pad from it. */
  tableColumns?(tableName: string): readonly string[] | undefined;
  /**
   * Userland default generators by column name, when the facade knows the schema. Inserts call
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
    private readonly rows: ReadonlyArray<Readonly<Record<string, unknown>>> = [],
    private readonly replaceOnConflict = false,
    private readonly returningColumns?: ReturningState,
  ) {}

  values(
    rows: InsertRowFor<DB[TTable]> | ReadonlyArray<InsertRowFor<DB[TTable]>>,
  ): InsertQueryBuilder<DB, TTable, TReturn> {
    const additions = (Array.isArray(rows) ? rows : [rows]) as ReadonlyArray<
      Readonly<Record<string, unknown>>
    >;
    return new InsertQueryBuilder(
      this.services,
      this.table,
      [...this.rows, ...additions],
      this.replaceOnConflict,
      this.returningColumns,
    );
  }

  /** Replaces the whole row when the unique key already exists (the engine's upsert). */
  orReplace(): InsertQueryBuilder<DB, TTable, TReturn> {
    return new InsertQueryBuilder(
      this.services,
      this.table,
      this.rows,
      true,
      this.returningColumns,
    );
  }

  /** Returns the written rows projected to these columns instead of an InsertResult. */
  returning<TCol extends keyof DB[TTable] & string>(
    columns: readonly TCol[],
  ): InsertQueryBuilder<DB, TTable, Simplify<Pick<DB[TTable], TCol>>> {
    return new InsertQueryBuilder(
      this.services,
      this.table,
      this.rows,
      this.replaceOnConflict,
      columns,
    );
  }

  /** Returns the complete written rows instead of an InsertResult. */
  returningAll(): InsertQueryBuilder<DB, TTable, Simplify<DB[TTable]>> {
    return new InsertQueryBuilder(
      this.services,
      this.table,
      this.rows,
      this.replaceOnConflict,
      "*",
    );
  }

  async execute(): Promise<TReturn[]> {
    if (this.rows.length === 0) throw new TypeError("insertInto() requires values()");
    // With the schema known, omitted nullable columns pad with null (the batch API takes every
    // column) and function defaults fill their omitted-or-null slots; unknown extra keys still
    // surface as engine errors.
    const columns = new Set<string>(this.services.tableColumns?.(this.table) ?? []);
    for (const row of this.rows) {
      for (const key of Object.keys(row)) columns.add(key);
    }
    const names = [...columns];
    const defaults = this.services.columnDefaultFns?.(this.table);
    const padded = this.rows.map((row) =>
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

type Ctx<DB, TTable extends keyof DB & string> = Record<TTable, DB[TTable]>;

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
  statement: CompiledStatement,
  returning: readonly string[] | "*",
): Promise<TReturn[]> {
  const result = await services.runStatement(statement, { returning });
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

  set<TCol extends keyof DB[TTable] & string>(
    column: TCol,
    value: DB[TTable][TCol] | ExpressionWrapper<DB[TTable][TCol]>,
  ): UpdateQueryBuilder<DB, TTable, TReturn>;
  set(
    changes:
      | UpdateChangesFor<DB[TTable]>
      | ((eb: ExpressionBuilder<DB, Ctx<DB, TTable>>) => {
          [K in keyof DB[TTable]]?: DB[TTable][K] | ExpressionWrapper<DB[TTable][K]> | undefined;
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
  returning<TCol extends keyof DB[TTable] & string>(
    columns: readonly TCol[],
  ): UpdateQueryBuilder<DB, TTable, Simplify<Pick<DB[TTable], TCol>>> {
    return new UpdateQueryBuilder(
      this.services,
      this.table,
      this.wheres,
      this.assignments,
      columns,
    );
  }

  /** Returns the complete affected rows (post-update values). */
  returningAll(): UpdateQueryBuilder<DB, TTable, Simplify<DB[TTable]>> {
    return new UpdateQueryBuilder(this.services, this.table, this.wheres, this.assignments, "*");
  }

  compile(): CompiledStatement {
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

  async execute(): Promise<TReturn[]> {
    if (this.returningColumns !== undefined) {
      return runReturning(this.services, this.compile(), this.returningColumns);
    }
    const result = await this.services.runStatement(this.compile());
    return [{ numUpdatedRows: result.rowCount ?? 0 } as TReturn];
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
  returning<TCol extends keyof DB[TTable] & string>(
    columns: readonly TCol[],
  ): DeleteQueryBuilder<DB, TTable, Simplify<Pick<DB[TTable], TCol>>> {
    return new DeleteQueryBuilder(this.services, this.table, this.wheres, columns);
  }

  /** Returns the complete deleted rows. */
  returningAll(): DeleteQueryBuilder<DB, TTable, Simplify<DB[TTable]>> {
    return new DeleteQueryBuilder(this.services, this.table, this.wheres, "*");
  }

  compile(): CompiledStatement {
    return {
      kind: "delete",
      table: this.table,
      predicates: compileMutationPredicates(this.wheres),
    };
  }

  async execute(): Promise<TReturn[]> {
    if (this.returningColumns !== undefined) {
      return runReturning(this.services, this.compile(), this.returningColumns);
    }
    const result = await this.services.runStatement(this.compile());
    return [{ numDeletedRows: result.rowCount ?? 0 } as TReturn];
  }

  async executeTakeFirst(): Promise<TReturn | undefined> {
    return takeFirst(this.execute());
  }

  async executeTakeFirstOrThrow(): Promise<TReturn> {
    return takeFirstOrThrow(this.execute());
  }
}
