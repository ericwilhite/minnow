import { type CompiledQuery, type Expression } from "../query.js";
import { type AnyTable, type InferRow, type SchemaDefinition } from "../schema.js";

/**
 * Type foundation for the Kysely-style query builder. A database type `DB` maps table names to
 * row types and is derived from the runtime schema DSL (`InferDatabase<typeof appSchema>`), so
 * one schema declaration drives migration planning, insert validation, and builder autocomplete.
 * A query context `TCtx` maps in-scope aliases to row types and evolves through `selectFrom` and
 * joins; every column reference type below is resolved against it.
 */

/** Flattens intersections so inferred row types read as one object. */
export type Simplify<T> = { [K in keyof T]: T[K] } & {};

/** Maps a schema's table names to their inferred row shapes. */
export type InferDatabase<TSchema extends SchemaDefinition<readonly AnyTable[]>> = Simplify<{
  [TTable in TSchema["tables"][number] as TTable["name"]]: InferRow<TTable>;
}>;

export type AnyRow = Record<string, unknown>;
export type AnyContext = Record<string, AnyRow>;

/** `"users"` or `"users as u"`. */
export type TableExpression<DB> = (keyof DB & string) | `${keyof DB & string} as ${string}`;

export type ExtractTableAlias<TE extends string> = TE extends `${string} as ${infer TAlias}`
  ? TAlias
  : TE;

export type ExtractTableName<TE extends string> = TE extends `${infer TName} as ${string}`
  ? TName
  : TE;

export type NullableRow<TRow> = { [K in keyof TRow]: TRow[K] | null };

/** The context after bringing a table (or aliased table) into scope. */
export type ContextWithTable<DB, TCtx, TE extends string> = TCtx &
  Record<ExtractTableAlias<TE>, DB[ExtractTableName<TE> & keyof DB]>;

/** The context after a LEFT JOIN: the joined table's columns widen with null. */
export type ContextWithLeftTable<DB, TCtx, TE extends string> = TCtx &
  Record<ExtractTableAlias<TE>, NullableRow<DB[ExtractTableName<TE> & keyof DB]>>;

/** Every column in scope, as a bare name or an alias-qualified `"a.column"`. */
export type ColumnReference<TCtx> = {
  [TAlias in keyof TCtx & string]:
    (keyof TCtx[TAlias] & string) | `${TAlias}.${keyof TCtx[TAlias] & string}`;
}[keyof TCtx & string];

/**
 * Only the columns whose (non-null) value type matches `TValue`, as bare or qualified names.
 * Numeric and date functions constrain their column arguments with this, so `fn.sum("name")`
 * on a string column is a compile error instead of a runtime surprise.
 */
export type ColumnReferenceOf<TCtx, TValue> = {
  [TAlias in keyof TCtx & string]: {
    [TColumn in keyof TCtx[TAlias] & string]: NonNullable<TCtx[TAlias][TColumn]> extends TValue
      ? TColumn | `${TAlias}.${TColumn}`
      : never;
  }[keyof TCtx[TAlias] & string];
}[keyof TCtx & string];

/** A join whose alias collides with one already in scope resolves to this instead of a builder. */
export interface DuplicateJoinAliasError<TMessage extends string> {
  readonly duplicateJoinAlias: TMessage;
}

/**
 * The result of a join: the builder when the alias is new, and a readable branded error type
 * when the alias is already in scope. The guard lives in the return type (not the parameter)
 * so it cannot interfere with inference of the table expression.
 */
export type JoinResult<TCtx, TE extends string, TBuilder> =
  ExtractTableAlias<TE> extends keyof TCtx & string
    ? DuplicateJoinAliasError<`alias '${ExtractTableAlias<TE>}' is already used in this query`>
    : TBuilder;

/** The value type a reference resolves to; bare names union across matching tables. */
export type ReferencedValue<
  TCtx,
  TRef extends string,
> = TRef extends `${infer TAlias}.${infer TColumn}`
  ? TAlias extends keyof TCtx
    ? TColumn extends keyof TCtx[TAlias]
      ? TCtx[TAlias][TColumn]
      : never
    : never
  : {
      [TAlias in keyof TCtx]: TRef extends keyof TCtx[TAlias] ? TCtx[TAlias][TRef] : never;
    }[keyof TCtx];

// --- Selections ---------------------------------------------------------------------------------

/** A selectable string: a column reference, optionally aliased with `as`. */
export type StringSelection<TCtx> = ColumnReference<TCtx> | `${ColumnReference<TCtx>} as ${string}`;

/** An expression given an output name with `.as(alias)`. */
export interface AliasedExpression<out TValue, out TAlias extends string> {
  readonly kind: "aliased-expression";
  readonly alias: TAlias;
  /** Materializes at compile time so subquery sources number under the shared sequence. */
  readonly source: ExpressionSource;
  /** Phantom output type; never materialized at runtime. */
  readonly __value?: TValue;
}

export type Selection<TCtx> = StringSelection<TCtx> | AliasedExpression<unknown, string>;

export type SelectionOutputKey<S extends string> = S extends `${string} as ${infer TAlias}`
  ? TAlias
  : S extends `${string}.${infer TColumn}`
    ? TColumn
    : S;

type StringSelectionValue<TCtx, S extends string> = S extends `${infer TRef} as ${string}`
  ? ReferencedValue<TCtx, TRef>
  : ReferencedValue<TCtx, S>;

/** The output row contributed by one `select(...)` call. */
export type RowFromSelections<TCtx, TSel> = Simplify<
  {
    [S in Extract<TSel, string> as SelectionOutputKey<S>]: StringSelectionValue<TCtx, S>;
  } & {
    [
      E in Extract<TSel, AliasedExpression<unknown, string>> as E["alias"]
    ]: E extends AliasedExpression<infer TValue, string> ? TValue : never;
  }
>;

type UnionToIntersection<U> = (U extends unknown ? (member: U) => void : never) extends (
  member: infer I,
) => void
  ? I
  : never;

type IsUnion<T, U = T> = T extends unknown ? ([U] extends [T] ? false : true) : never;

type QualifiedColumnsRow<TCtx> = UnionToIntersection<
  {
    [TAlias in keyof TCtx & string]: {
      [TColumn in keyof TCtx[TAlias] & string as `${TAlias}.${TColumn}`]: TCtx[TAlias][TColumn];
    };
  }[keyof TCtx & string]
>;

/**
 * The `selectAll()` row: a single-source context keeps bare column names; a joined context
 * qualifies every column as `"alias.column"`, exactly as the executor projects `SELECT *`.
 */
export type AllColumnsRow<TCtx> =
  true extends IsUnion<keyof TCtx> ? Simplify<QualifiedColumnsRow<TCtx>> : TCtx[keyof TCtx];

// --- Mutations ----------------------------------------------------------------------------------

/** Insert rows require non-nullable columns and may omit nullable ones. */
export type InsertRowFor<TRow> = Simplify<
  { [K in keyof TRow as null extends TRow[K] ? never : K]: TRow[K] } & {
    [K in keyof TRow as null extends TRow[K] ? K : never]?: TRow[K] | undefined;
  }
>;

/** An undefined entry means "leave this column untouched", so spread-patches stay safe. */
export type UpdateChangesFor<TRow> = { [K in keyof TRow]?: TRow[K] | undefined };

// --- Deferred compilation -----------------------------------------------------------------------

/**
 * Compile-time services threaded through expression materialization: the shared derived-source
 * sequence (so builder plans number sources exactly as parsed SQL does) and CTE resolution.
 */
export interface CompileContext {
  nextSequence: () => number;
  /** Returns a fresh clone of a CTE body compiled earlier in this compile pass. */
  cteFor: (name: string) => CompiledQuery | undefined;
}

/** An expression, either concrete or deferred until the compile context exists. */
export type ExpressionSource = Expression | ((context: CompileContext) => Expression);

export function materialize(source: ExpressionSource, context: CompileContext): Expression {
  return typeof source === "function" ? source(context) : source;
}

/** A sub-select usable as a derived table, subquery expression, or set-operation member. */
export interface BlockCompilable {
  readonly kind: "dsl-select-builder";
  compileBlock(context: CompileContext, label: string): CompiledQuery;
}

export function isBlockCompilable(value: unknown): value is BlockCompilable {
  return (
    typeof value === "object" &&
    value !== null &&
    (value as { kind?: unknown }).kind === "dsl-select-builder"
  );
}

/** The `{ kind: "typed-query" }` envelope the engine's `run()` executes. */
export interface TypedQueryEnvelope<out TRow> {
  readonly kind: "typed-query";
  readonly plan: CompiledQuery;
  readonly __row?: TRow;
}
