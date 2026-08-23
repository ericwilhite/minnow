import {
  validateColumnDefault,
  validateEnumValues,
  type ColumnDefault,
  type TableColumnRecord,
  type TableRecord,
} from "../storage/index.js";
import { type Catalog, type CatalogColumn, type CatalogTable } from "./catalog.js";
import { compileCheckExpression, expressionColumns, type QueryValue } from "./query.js";

/**
 * Typed schema DSL and catalog migration planning. Column builders carry compile-time value and
 * nullability types; `planMigration` diffs a schema against the live catalog into metadata-only
 * steps and rejects everything it cannot prove safe. Physical rewrites never happen here: adding
 * a nullable column, renaming a column through its stable ID, and widening nullability are
 * catalog-only, and older segments read the new column as NULL.
 */

export type SchemaColumnType = "boolean" | "number" | "string" | "datetime";

/** What a FOREIGN KEY does to child rows when the parent row is deleted (E141-04). */
export type ReferentialAction = "restrict" | "cascade" | "set null";

/** A declared relation. `onDelete` defaults to "restrict", matching SQL's own default. */
export interface ColumnReferenceSpec {
  readonly table: string;
  readonly column: string;
  readonly onDelete: ReferentialAction;
}

/** A row condition every write must satisfy (E141-06); `sql` is a boolean expression. */
export interface TableCheck {
  readonly name: string;
  readonly sql: string;
}

/**
 * The constraint name `migrate()` gives a declared relation. It matches the name the SQL parser
 * derives for an unnamed inline REFERENCES, so a table built either way has the same catalog.
 */
export function foreignKeyName(tableName: string, columnName: string): string {
  return `${tableName}_${columnName}_fkey`;
}

type ValueOf<TType extends SchemaColumnType> = TType extends "boolean"
  ? boolean
  : TType extends "number"
    ? number
    : TType extends "string"
      ? string
      : Date;

/**
 * A flavored value: a column whose slot the engine can fill, so inserts may omit it. The brand
 * is an optional phantom property — plain values stay assignable in both directions.
 */
export type HasDefault<TValue> = TValue & { readonly __minnowHasDefault?: true };

/**
 * The public name for the flavor, for hand-declared `DB` interfaces (the Kysely convention).
 * `InferDatabase` applies it automatically from the schema; write it yourself only when you
 * declare the row types by hand and want generated columns to stay omissible on insert:
 *
 * ```ts
 * interface DB {
 *   notes: FromRow<{ id: Generated<number>; slug: Generated<string>; body: string }>;
 * }
 * ```
 */
export type Generated<TValue> = HasDefault<TValue>;

export interface ColumnBuilder<
  TValue,
  TNullable extends boolean,
  TUnique extends boolean = false,
  THasDefault extends boolean = false,
> {
  readonly kind: "column";
  readonly type: SchemaColumnType;
  readonly isNullable: TNullable;
  readonly isUnique: TUnique;
  readonly hasDefault: THasDefault;
  readonly defaultSpec?: ColumnDefault;
  /**
   * A userland default generator. Never persisted and never sent to the engine — the typed
   * facade (`insertInto`, `typedTable`) calls it for omitted-or-null slots before rendering the
   * parameterized INSERT, so untyped write paths (raw batches, SQL) do not see it.
   */
  readonly defaultFn?: () => TValue;
  /** Present on `column.enum()` builders: the closed set of values writes must draw from. */
  readonly enumValues?: readonly string[];
  /** What rows written before this column existed read as; see `.backfill()`. */
  readonly backfillValue?: TValue | (() => TValue);
  readonly renamedFromName?: string;
  readonly reference?: ColumnReferenceSpec;
  /** Marks the column nullable; inserts may omit it and reads may return null. */
  nullable(
    this: ColumnBuilder<TValue, TNullable>,
  ): ColumnBuilder<TValue, true, TUnique, THasDefault>;
  /** Marks the table's unique key; exactly one non-nullable column may carry it. */
  unique(
    this: ColumnBuilder<TValue, false, TUnique, THasDefault>,
  ): ColumnBuilder<TValue, TNullable, true, THasDefault>;
  /** Declares this column as the rename target of an existing catalog column. */
  renamedFrom(name: string): ColumnBuilder<TValue, TNullable, TUnique, THasDefault>;
  /**
   * What rows written before this column existed read as, instead of NULL. Giving one is what
   * makes adding a non-nullable column possible: no stored byte is rewritten, and reads
   * substitute the value wherever the column has no data.
   *
   * A function runs once, when the migration adds the column, and its result is frozen into the
   * catalog — so it can derive a value (a timestamp, a version stamp) without readers ever
   * disagreeing. It cannot derive from other columns; that would need a value per row.
   */
  backfill(
    this: ColumnBuilder<TValue, false, TUnique, THasDefault>,
    value: TValue | (() => TValue),
  ): ColumnBuilder<TValue, TNullable, TUnique, THasDefault>;
  /**
   * Declares a FOREIGN KEY onto another table's unique key. `migrate()` creates it as a real
   * constraint, so a write naming a parent row that does not exist is rejected — the same
   * behaviour as declaring `REFERENCES` in SQL DDL.
   *
   * `onDelete` defaults to `"restrict"`. `"set null"` requires a nullable column.
   */
  references(
    table: string,
    column: string,
    options?: { onDelete?: Exclude<ReferentialAction, "set null"> },
  ): ColumnBuilder<TValue, TNullable, TUnique, THasDefault>;
  references(
    this: ColumnBuilder<TValue, true, TUnique, THasDefault>,
    table: string,
    column: string,
    options: { onDelete: "set null" },
  ): ColumnBuilder<TValue, TNullable, TUnique, THasDefault>;
  /**
   * Fills null-or-absent slots at insert time. Requires a non-nullable column.
   *
   * A plain value persists in the catalog and fills inside the engine, so every write path
   * gets it — raw batches, SQL statements, other tabs. Datetime columns accept only "now",
   * which stamps one consistent timestamp per batch.
   *
   * A function is a userland generator (`() => ulid()`): the typed facade calls it just
   * before its INSERT is sent, so it never persists and never crosses the worker boundary —
   * and write paths that don't go through the facade don't see it.
   */
  default(
    this: ColumnBuilder<TValue, false, TUnique, THasDefault>,
    value: (TValue extends Date ? "now" : TValue) | (() => TValue),
  ): ColumnBuilder<TValue, TNullable, TUnique, true>;
  /**
   * Generates monotonically increasing integers for null-or-absent slots from a persistent
   * per-table counter that is atomic across tabs. Explicit values are allowed and bump the
   * counter past their maximum. Number unique-key columns only.
   */
  autoIncrement: TValue extends number
    ? (
        this: ColumnBuilder<TValue, false, true, THasDefault>,
      ) => ColumnBuilder<TValue, TNullable, TUnique, true>
    : never;
}

type SchemaValue = boolean | number | string | Date;

/**
 * Metadata shared by every concrete ColumnBuilder. Schema collections need an existential
 * column type: including fluent methods here would make TValue invariant because those methods
 * both consume and return it. Public inference still retains each concrete builder in TColumns.
 */
interface AnyColumn {
  readonly kind: "column";
  readonly type: SchemaColumnType;
  readonly isNullable: boolean;
  readonly isUnique: boolean;
  readonly hasDefault: boolean;
  readonly defaultSpec?: ColumnDefault;
  readonly defaultFn?: () => SchemaValue;
  readonly enumValues?: readonly string[];
  readonly backfillValue?: SchemaValue | (() => SchemaValue);
  readonly renamedFromName?: string;
  readonly reference?: ColumnReferenceSpec;
}

function defaultSpecFromArg(type: SchemaColumnType, value: unknown): ColumnDefault {
  switch (type) {
    case "datetime":
      if (value !== "now") throw new TypeError('Datetime columns default with "now"');
      return { kind: "now" };
    case "string":
      if (typeof value !== "string") throw new TypeError("Default literal must be a string");
      return { kind: "literal", value };
    case "number":
      if (typeof value !== "number" || !Number.isFinite(value)) {
        throw new TypeError("Default literal must be a finite number");
      }
      return { kind: "literal", value };
    case "boolean":
      if (typeof value !== "boolean") throw new TypeError("Default literal must be a boolean");
      return { kind: "literal", value };
  }
}

function createColumn<TType extends SchemaColumnType>(
  type: TType,
  state: Partial<
    Pick<
      AnyColumn,
      | "isNullable"
      | "isUnique"
      | "renamedFromName"
      | "reference"
      | "defaultSpec"
      | "defaultFn"
      | "enumValues"
      | "backfillValue"
    >
  > = {},
): ColumnBuilder<ValueOf<TType>, false> {
  const base = {
    kind: "column" as const,
    type,
    isNullable: (state.isNullable ?? false) as false,
    isUnique: (state.isUnique ?? false) as false,
    hasDefault: (state.defaultSpec !== undefined || state.defaultFn !== undefined) as false,
    ...(state.defaultSpec === undefined ? {} : { defaultSpec: state.defaultSpec }),
    ...(state.defaultFn === undefined
      ? {}
      : { defaultFn: state.defaultFn as () => ValueOf<TType> }),
    ...(state.renamedFromName === undefined ? {} : { renamedFromName: state.renamedFromName }),
    ...(state.reference === undefined ? {} : { reference: state.reference }),
    ...(state.enumValues === undefined ? {} : { enumValues: state.enumValues }),
    ...(state.backfillValue === undefined
      ? {}
      : { backfillValue: state.backfillValue as ValueOf<TType> | (() => ValueOf<TType>) }),
  };
  return {
    ...base,
    nullable: () =>
      createColumn(type, { ...state, isNullable: true }) as unknown as ColumnBuilder<
        ValueOf<TType>,
        true
      >,
    unique: () =>
      createColumn(type, { ...state, isUnique: true }) as unknown as ColumnBuilder<
        ValueOf<TType>,
        false,
        true
      >,
    renamedFrom: (name: string) => {
      validateSchemaName(name, "Rename source");
      return createColumn(type, { ...state, renamedFromName: name });
    },
    backfill: (value: unknown) =>
      createColumn(type, { ...state, backfillValue: value as ValueOf<TType> }),
    references: (
      table: string,
      referencedColumn: string,
      options: { onDelete?: ReferentialAction } = {},
    ) => {
      validateSchemaName(table, "Referenced table");
      validateSchemaName(referencedColumn, "Referenced column");
      return createColumn(type, {
        ...state,
        reference: {
          table,
          column: referencedColumn,
          onDelete: options.onDelete ?? "restrict",
        },
      });
    },
    default: ((value: unknown) => {
      // A declared default is one thing: a function replaces any spec and vice versa.
      const cleared = { ...state };
      delete cleared.defaultSpec;
      delete cleared.defaultFn;
      return typeof value === "function"
        ? createColumn(type, { ...cleared, defaultFn: value as () => ValueOf<TType> })
        : createColumn(type, { ...cleared, defaultSpec: defaultSpecFromArg(type, value) });
    }) as unknown as ColumnBuilder<ValueOf<TType>, false>["default"],
    autoIncrement: (() => {
      if (type !== "number") {
        throw new TypeError("Auto-increment requires a number column");
      }
      const cleared = { ...state };
      delete cleared.defaultFn;
      return createColumn(type, { ...cleared, defaultSpec: { kind: "autoincrement" } });
    }) as unknown as ColumnBuilder<ValueOf<TType>, false>["autoIncrement"],
  };
}

function validateSchemaName(name: string, kind: string): void {
  if (name.length === 0) throw new TypeError(`${kind} name cannot be empty`);
  if (name.trim() !== name) {
    throw new TypeError(
      `${kind} name cannot start or end with whitespace: ${JSON.stringify(name)}`,
    );
  }
}

/**
 * Rebuilds a column carrying an exact default spec — the wire layer's escape hatch, since the
 * public `.default()` interprets "now" on datetime columns and cannot express auto-increment.
 */
export function columnWithDefaultSpec(
  base: Pick<
    AnyColumn,
    "type" | "isNullable" | "isUnique" | "renamedFromName" | "reference" | "enumValues"
  >,
  spec: ColumnDefault,
): AnyColumn {
  return columnFromState({
    type: base.type,
    isNullable: base.isNullable,
    isUnique: base.isUnique,
    ...(base.renamedFromName === undefined ? {} : { renamedFromName: base.renamedFromName }),
    ...(base.reference === undefined ? {} : { reference: base.reference }),
    ...(base.enumValues === undefined ? {} : { enumValues: base.enumValues }),
    defaultSpec: spec,
  });
}

/** Rebuilds a fluent column from structured-clone-safe metadata. */
export function columnFromState(
  state: Pick<AnyColumn, "type" | "isNullable" | "isUnique"> &
    Partial<
      Pick<
        AnyColumn,
        | "renamedFromName"
        | "reference"
        | "defaultSpec"
        | "defaultFn"
        | "enumValues"
        | "backfillValue"
      >
    >,
): AnyColumn {
  return createColumn(state.type, {
    isNullable: state.isNullable,
    isUnique: state.isUnique,
    ...(state.renamedFromName === undefined ? {} : { renamedFromName: state.renamedFromName }),
    ...(state.reference === undefined ? {} : { reference: state.reference }),
    ...(state.defaultSpec === undefined ? {} : { defaultSpec: state.defaultSpec }),
    ...(state.defaultFn === undefined ? {} : { defaultFn: state.defaultFn }),
    ...(state.enumValues === undefined ? {} : { enumValues: state.enumValues }),
    ...(state.backfillValue === undefined ? {} : { backfillValue: state.backfillValue }),
  });
}

export const column = {
  boolean: () => createColumn("boolean"),
  number: () => createColumn("number"),
  string: () => createColumn("string"),
  datetime: () => createColumn("datetime"),
  /**
   * A string column restricted to a closed set of values, typed as their literal union:
   *
   * ```ts
   * status: column.enum(["draft", "published", "archived"]).default("draft")
   * ```
   *
   * Selects return `"draft" | "published" | "archived"` and inserts accept nothing else; every
   * write path also validates membership at runtime. Physically the column is a plain string
   * column, so migrations may add values or relax the column to `column.string()`, but never
   * remove values — existing rows could already hold them.
   */
  enum: <const TValues extends readonly [string, ...string[]]>(
    values: TValues,
  ): ColumnBuilder<TValues[number], false> =>
    createColumn("string", { enumValues: validateEnumValues(values, "enum column") }),
};

export interface TableSchema<
  TColumns extends Record<string, AnyColumn>,
  TName extends string = string,
> {
  readonly kind: "table";
  readonly name: TName;
  readonly columns: TColumns;
  /** Row conditions every write must satisfy; empty when none were declared. */
  readonly checks: readonly TableCheck[];
  /** Standard Schema-compatible runtime validator for one insert row. */
  readonly "~standard": {
    readonly version: 1;
    readonly vendor: "minnow";
    validate(
      value: unknown,
    ):
      | { value: Record<string, unknown> }
      | { issues: Array<{ message: string; path: Array<string | number> }> };
  };
}

export type AnyTable = TableSchema<Record<string, AnyColumn>>;

export interface TableOptions {
  /**
   * Row conditions every write must satisfy (E141-06), each a boolean SQL expression over this
   * table's own columns — the declarative form of `CONSTRAINT name CHECK (sql)`. The engine
   * compiles each one when the table is created, so an expression it cannot evaluate is refused
   * at migration time rather than on the first write.
   */
  readonly checks?: readonly TableCheck[];
}

export function table<const TName extends string, TColumns extends Record<string, AnyColumn>>(
  name: TName,
  columns: TColumns,
  options: TableOptions = {},
): TableSchema<TColumns, TName> {
  validateSchemaName(name, "Table");
  const entries = Object.entries(columns);
  if (entries.length === 0) throw new TypeError(`Table ${name} needs at least one column`);
  const uniqueColumns = entries.filter(([, definition]) => definition.isUnique);
  if (uniqueColumns.length > 1) {
    throw new TypeError(`Table ${name} may name at most one unique column`);
  }
  const uniqueEntry = uniqueColumns[0];
  if (uniqueEntry?.[1].isNullable === true) {
    throw new TypeError(`Table ${name} unique column must not be nullable: ${uniqueEntry[0]}`);
  }
  for (const [columnName, definition] of entries) {
    validateSchemaName(columnName, "Column");
    if (definition.defaultFn !== undefined && definition.isNullable) {
      throw new TypeError(`Defaults require a non-nullable column: ${columnName}`);
    }
    const backfill = definition.backfillValue;
    if (backfill !== undefined && typeof backfill !== "function") {
      const matches =
        definition.type === "datetime"
          ? backfill instanceof Date
          : typeof backfill === definition.type;
      if (!matches) {
        throw new TypeError(`Backfill value must be a ${definition.type}: ${name}.${columnName}`);
      }
      if (
        definition.enumValues !== undefined &&
        typeof backfill === "string" &&
        !definition.enumValues.includes(backfill)
      ) {
        throw new TypeError(
          `Backfill value must be one of: ${definition.enumValues.join(", ")} (${name}.${columnName})`,
        );
      }
    }
    if (backfill !== undefined && definition.isNullable) {
      throw new TypeError(
        `A nullable column needs no backfill: ${name}.${columnName}. Rows without it already read NULL.`,
      );
    }
    if (definition.reference?.onDelete === "set null" && !definition.isNullable) {
      throw new TypeError(`ON DELETE SET NULL requires a nullable column: ${name}.${columnName}`);
    }
    const spec = definition.defaultSpec;
    if (spec === undefined) continue;
    validateColumnDefault(
      {
        name: columnName,
        type: definition.type,
        nullable: definition.isNullable,
        isUniqueKey: definition.isUnique,
        ...(definition.enumValues === undefined ? {} : { enumValues: definition.enumValues }),
      },
      spec,
    );
  }
  const checks = options.checks ?? [];
  const checkNames = new Set<string>();
  for (const check of checks) {
    validateSchemaName(check.name, "CHECK");
    if (checkNames.has(check.name)) {
      throw new TypeError(`Duplicate CHECK in table ${name}: ${check.name}`);
    }
    checkNames.add(check.name);
    if (check.sql.trim().length === 0) {
      throw new TypeError(`CHECK ${check.name} in table ${name} has no expression`);
    }
  }
  return {
    kind: "table",
    name,
    columns,
    checks,
    "~standard": {
      version: 1,
      vendor: "minnow",
      validate(value) {
        if (typeof value !== "object" || value === null) {
          return { issues: [{ message: "A row must be an object", path: [] }] };
        }
        const row = value as Record<string, unknown>;
        const issues: Array<{ message: string; path: Array<string | number> }> = [];
        for (const [columnName, definition] of entries) {
          const columnValue = row[columnName];
          if (columnValue === undefined || columnValue === null) {
            // A default-bearing column may be omitted — the engine (or the facade, for
            // function defaults) fills it at insert time.
            if (
              !definition.isNullable &&
              definition.defaultSpec === undefined &&
              definition.defaultFn === undefined
            ) {
              issues.push({ message: `Missing non-nullable column`, path: [columnName] });
            }
            continue;
          }
          const matches =
            definition.type === "datetime"
              ? columnValue instanceof Date && Number.isFinite(columnValue.getTime())
              : definition.type === "number"
                ? typeof columnValue === "number" && Number.isFinite(columnValue)
                : typeof columnValue === definition.type;
          if (!matches) {
            issues.push({ message: `Expected ${definition.type}`, path: [columnName] });
          } else if (
            definition.enumValues !== undefined &&
            typeof columnValue === "string" &&
            !definition.enumValues.includes(columnValue)
          ) {
            issues.push({
              message: `Expected one of: ${definition.enumValues.join(", ")}`,
              path: [columnName],
            });
          }
        }
        for (const key of Object.keys(row)) {
          if (!(key in columns)) issues.push({ message: "Unknown column", path: [key] });
        }
        return issues.length > 0 ? { issues } : { value: row };
      },
    },
  };
}

/**
 * The FOREIGN KEY constraints a table declares, in column order. This is the single derivation
 * of "what relations does this table have" — `planMigration` diffs against it and `migrate()`
 * creates from it, so the two can never disagree.
 */
export function declaredForeignKeys(definition: AnyTable): Array<{
  name: string;
  column: string;
  parentTable: string;
  parentColumn: string;
  onDelete: ReferentialAction;
}> {
  const keys = [];
  for (const [columnName, columnDefinition] of Object.entries(definition.columns)) {
    const reference = columnDefinition.reference;
    if (reference === undefined) continue;
    keys.push({
      name: foreignKeyName(definition.name, columnName),
      column: columnName,
      parentTable: reference.table,
      parentColumn: reference.column,
      onDelete: reference.onDelete,
    });
  }
  return keys;
}

/**
 * A view declared in the schema: the query it stands for, plus the column shape the author
 * expects it to produce.
 *
 * The shape is declared rather than inferred from a builder because the query builder compiles to
 * a plan, not to SQL text, and `CREATE VIEW` needs text. Declaring it is not a downgrade: the
 * engine infers the real output schema when it creates the view and `migrate()` compares the two,
 * so a body that drifts from its declaration fails at migration time rather than at read time.
 */
export interface ViewSchema<
  TColumns extends Record<string, AnyColumn>,
  TName extends string = string,
> {
  readonly kind: "view";
  readonly name: TName;
  readonly sql: string;
  readonly columns: TColumns;
}

export type AnyView = ViewSchema<Record<string, AnyColumn>>;

/**
 * Declares a view. Its columns use the same builders tables use, so a view row types exactly like
 * a table row — but a view is read-only, and the typed facade will not let a write name one.
 *
 * ```ts
 * const activeCustomers = view("active_customers", {
 *   sql: `SELECT customer_id, name FROM customers WHERE status = 'active'`,
 *   columns: { customer_id: column.number(), name: column.string() },
 * });
 * ```
 */
export function view<const TName extends string, TColumns extends Record<string, AnyColumn>>(
  name: TName,
  definition: { sql: string; columns: TColumns },
): ViewSchema<TColumns, TName> {
  validateSchemaName(name, "View");
  const entries = Object.entries(definition.columns);
  if (entries.length === 0) throw new TypeError(`View ${name} needs at least one column`);
  if (definition.sql.trim().length === 0) throw new TypeError(`View ${name} has no query`);
  for (const [columnName, columnDefinition] of entries) {
    validateSchemaName(columnName, "Column");
    if (columnDefinition.isUnique) {
      throw new TypeError(`A view column cannot be a unique key: ${name}.${columnName}`);
    }
    if (columnDefinition.defaultSpec !== undefined || columnDefinition.defaultFn !== undefined) {
      throw new TypeError(`A view column cannot have a default: ${name}.${columnName}`);
    }
  }
  return { kind: "view", name, sql: definition.sql, columns: definition.columns };
}

export interface SchemaDefinition<
  TTables extends readonly AnyTable[],
  TViews extends readonly AnyView[] = readonly AnyView[],
> {
  readonly kind: "schema";
  readonly tables: TTables;
  readonly views: TViews;
}

export function schema<TTables extends readonly AnyTable[], TViews extends readonly AnyView[] = []>(
  tables: TTables,
  options: { views?: TViews } = {},
): SchemaDefinition<TTables, TViews> {
  const views = (options.views ?? []) as TViews;
  const names = new Set<string>();
  for (const definition of tables) {
    if (names.has(definition.name)) {
      throw new TypeError(`Duplicate table in schema: ${definition.name}`);
    }
    names.add(definition.name);
  }
  for (const definition of views) {
    if (names.has(definition.name)) {
      throw new TypeError(`Duplicate name in schema: ${definition.name}`);
    }
    names.add(definition.name);
  }
  for (const definition of tables) {
    for (const [columnName, columnDefinition] of Object.entries(definition.columns)) {
      const reference = columnDefinition.reference;
      if (reference === undefined) continue;
      const target = tables.find(({ name }) => name === reference.table);
      if (target === undefined || !(reference.column in target.columns)) {
        throw new TypeError(
          `Relation target does not exist: ${definition.name}.${columnName} -> ${reference.table}.${reference.column}`,
        );
      }
      const targetColumn = target.columns[reference.column];
      if (targetColumn?.isUnique !== true) {
        throw new TypeError(
          `Relation target must be the unique key: ${definition.name}.${columnName} -> ${reference.table}.${reference.column}`,
        );
      }
      if (targetColumn.type !== columnDefinition.type) {
        throw new TypeError(
          `Relation types must match: ${definition.name}.${columnName} is ${columnDefinition.type}, ` +
            `${reference.table}.${reference.column} is ${targetColumn.type}`,
        );
      }
    }
  }
  return { kind: "schema", tables, views };
}

// --- Compile-time row types ---------------------------------------------------------------------

type ColumnValue<TColumn> =
  TColumn extends ColumnBuilder<infer TValue, infer TNullable, boolean, infer THasDefault>
    ? THasDefault extends true
      ? HasDefault<TNullable extends true ? TValue | null : TValue>
      : TNullable extends true
        ? TValue | null
        : TValue
    : never;

/** The complete row shape a read returns. */
export type InferRow<TTable extends AnyTable> = {
  [K in keyof TTable["columns"]]: ColumnValue<TTable["columns"][K]>;
};

/**
 * A view's row shape. Views are read-only and their columns carry no keys or defaults, so this is
 * the only row type a view has — there is no insert or update counterpart.
 */
export type InferViewRow<TView extends AnyView> = {
  [K in keyof TView["columns"]]: ColumnValue<TView["columns"][K]>;
};

type NullableKeys<TTable extends AnyTable> = {
  [K in keyof TTable["columns"]]: TTable["columns"][K]["isNullable"] extends true ? K : never;
}[keyof TTable["columns"]];

type DefaultKeys<TTable extends AnyTable> = {
  [K in keyof TTable["columns"]]: TTable["columns"][K]["hasDefault"] extends true ? K : never;
}[keyof TTable["columns"]];

type OptionalInsertKeys<TTable extends AnyTable> = NullableKeys<TTable> | DefaultKeys<TTable>;

/** Insert rows require every non-nullable column and may omit nullable or default-bearing ones. */
export type InferInsertRow<TTable extends AnyTable> = Omit<
  InferRow<TTable>,
  OptionalInsertKeys<TTable>
> & {
  [K in OptionalInsertKeys<TTable>]?: ColumnValue<TTable["columns"][K]>;
};

/**
 * Update changes may cover any column except the unique key. An explicit `undefined` entry is
 * allowed and means "leave this column untouched", so a spread-patch built from optional fields
 * stays assignable under `exactOptionalPropertyTypes`.
 */
export type InferUpdateChanges<TTable extends AnyTable> = {
  [K in keyof TTable["columns"] as TTable["columns"][K]["isUnique"] extends true ? never : K]?:
    ColumnValue<TTable["columns"][K]> | undefined;
};

// --- Migration planning -------------------------------------------------------------------------

export type MigrationStep =
  | { kind: "create-table"; table: AnyTable }
  | {
      kind: "add-column";
      tableName: string;
      columnName: string;
      definition: AnyColumn;
      /** Frozen here: a generator runs once, so every reader of a given row agrees. */
      backfill?: boolean | number | string | Date;
    }
  | { kind: "rename-column"; tableName: string; from: string; to: string }
  | { kind: "widen-nullable"; tableName: string; columnName: string }
  /**
   * NULL -> NOT NULL. Only provable, never assumed: `migrate()` verifies from block headers that
   * no visible row holds NULL before applying it, and refuses the migration when one does.
   */
  | { kind: "tighten-nullable"; tableName: string; columnName: string }
  /** Adopting or dropping the persistent counter; neither touches a stored row. */
  | { kind: "set-auto-increment"; tableName: string; columnName: string; enabled: boolean }
  /** Grows an enum's value set, or drops the restriction entirely (`enumValues: null`). */
  | { kind: "widen-enum"; tableName: string; columnName: string; enumValues: string[] | null }
  | {
      kind: "alter-default";
      tableName: string;
      columnName: string;
      defaultValue: ColumnDefault | null;
    }
  /**
   * A view is derived and disposable: nothing is stored under it, so replacing its body loses no
   * data and needs none of the proofs a table alteration needs. `replace` covers both creating a
   * missing view and redefining an existing one.
   *
   * `drop-view` removes a view a previous migration created and this schema no longer declares.
   * It never names a view the schema did not make — see `planViewSteps`.
   */
  /** Destructive: the column stops being readable. See `isDestructiveStep`. */
  | { kind: "drop-column"; tableName: string; columnName: string }
  /** Destructive: the table and its rows stop being readable. See `isDestructiveStep`. */
  | { kind: "drop-table"; tableName: string }
  | { kind: "replace-view"; view: AnyView }
  | { kind: "drop-view"; viewName: string };

export interface MigrationPlan {
  steps: MigrationStep[];
}

/**
 * Views are derived: nothing is stored under one, so a body change is a replace rather than a
 * rewrite and needs none of the proofs a table alteration needs.
 *
 * Within the set of views a migration created, the schema is authoritative — removing a
 * declaration drops the view. Outside it nothing is touched: a view created with `CREATE VIEW`,
 * or one written before ownership was recorded, is not any schema's to remove. That is the
 * difference between a declaration being the source of truth and a declaration being allowed to
 * destroy things it never made.
 */
function planViewSteps(
  catalog: Catalog,
  definition: SchemaDefinition<readonly AnyTable[]>,
  steps: MigrationStep[],
): void {
  const tableNames = new Set(catalog.tables.map(({ name }) => name));
  const existing = new Map(catalog.views.map((record) => [record.name, record]));
  const declared = new Set(definition.views.map(({ name }) => name));
  for (const viewDefinition of definition.views) {
    if (tableNames.has(viewDefinition.name)) {
      throw new TypeError(`A table already exists with this name: ${viewDefinition.name}`);
    }
    if (existing.get(viewDefinition.name)?.sql === viewDefinition.sql) continue;
    steps.push({ kind: "replace-view", view: viewDefinition });
  }
  for (const record of catalog.views) {
    if (declared.has(record.name) || !record.managed) continue;
    steps.push({ kind: "drop-view", viewName: record.name });
  }
}

/**
 * Creation order, so a table is created after the tables it references. A FOREIGN KEY names a
 * parent that must already exist, which would otherwise make a schema fail purely because its
 * tables were listed child-first. Declaration order is preserved among tables that do not
 * constrain each other, and a reference cycle (or a self-reference, which the engine allows)
 * falls back to declaration order rather than failing here — the engine still has the final say.
 */
function orderedForCreation(tables: readonly AnyTable[]): readonly AnyTable[] {
  const byName = new Map(tables.map((definition) => [definition.name, definition]));
  const ordered: AnyTable[] = [];
  const state = new Map<string, "visiting" | "done">();
  const visit = (definition: AnyTable): void => {
    const status = state.get(definition.name);
    if (status !== undefined) return; // done, or a cycle we decline to reorder
    state.set(definition.name, "visiting");
    for (const key of declaredForeignKeys(definition)) {
      const parent = byName.get(key.parentTable);
      if (parent !== undefined && parent !== definition) visit(parent);
    }
    state.set(definition.name, "done");
    ordered.push(definition);
  };
  for (const definition of tables) visit(definition);
  return ordered;
}

/**
 * Whether a step destroys data a reader could still see. These are the steps `migrate()` refuses
 * to apply unless the caller opts in, so that a schema file drifting out of sync cannot delete
 * rows the next time an application opens.
 */
export function isDestructiveStep(
  step: MigrationStep,
): step is Extract<MigrationStep, { kind: "drop-column" | "drop-table" }> {
  return step.kind === "drop-column" || step.kind === "drop-table";
}

/**
 * A column can stop being projected without rewriting anything, but not while something else in
 * the catalog still points at it. Each of these would leave a constraint or key referring to a
 * column no longer there, so the drop is refused rather than silently invalidating them.
 */
export function assertColumnDroppable(record: CatalogTable, column: CatalogColumn): void {
  const where = `${record.name}.${column.name}`;
  if (record.uniqueKeyColumnId === column.id) {
    throw new TypeError(
      `The unique key cannot be dropped: ${where}. Unique-key changes need the table recreated.`,
    );
  }
  for (const key of record.foreignKeys) {
    if (key.column === column.name) {
      throw new TypeError(`FOREIGN KEY ${key.name} still uses this column: ${where}`);
    }
  }
  for (const check of record.checks) {
    let referenced: readonly string[];
    try {
      referenced = expressionColumns(compileCheckExpression(check.sql, check.name));
    } catch {
      // A check the engine can no longer compile is not a licence to drop what it names.
      throw new TypeError(`CHECK ${check.name} cannot be re-read, so ${where} is not droppable`);
    }
    if (referenced.includes(column.name)) {
      throw new TypeError(`CHECK ${check.name} still uses this column: ${where}`);
    }
  }
}

/**
 * Freezes a column's backfill. A generator runs exactly once — here, while the migration is being
 * planned — so the catalog stores a value rather than a function and no two readers can disagree.
 */
function resolveBackfill(definition: AnyColumn): boolean | number | string | Date | undefined {
  const declared = definition.backfillValue;
  if (declared === undefined) return undefined;
  return typeof declared === "function" ? declared() : declared;
}

/**
 * Constraints on an existing table cannot change through a metadata-only step. Attaching a
 * FOREIGN KEY or CHECK to a table that already holds rows would claim something about those rows
 * that nobody has verified, and no validation scan exists; dropping one is refused for the mirror
 * reason, so that a constraint never disappears because a schema file drifted. Both get the same
 * discipline every other unprovable change gets: an explicit error naming the fix.
 */
function assertConstraintsUnchanged(record: CatalogTable, definition: AnyTable): void {
  const describeKey = (key: {
    column: string;
    parentTable: string;
    parentColumn: string;
    onDelete: string;
  }): string => `${key.column} -> ${key.parentTable}.${key.parentColumn} ON DELETE ${key.onDelete}`;

  const existingKeys = new Map(record.foreignKeys.map((key) => [key.name, key]));
  const declaredKeys = new Map(declaredForeignKeys(definition).map((key) => [key.name, key]));
  for (const [name, declared] of declaredKeys) {
    const existing = existingKeys.get(name);
    if (existing === undefined) {
      throw new TypeError(
        `FOREIGN KEY cannot be added after creation: ${definition.name}.${declared.column}. ` +
          `Existing rows are not known to satisfy it; recreate the table to add a relation.`,
      );
    }
    if (describeKey(existing) !== describeKey(declared)) {
      throw new TypeError(
        `FOREIGN KEY cannot change: ${definition.name}.${declared.column} is ` +
          `${describeKey(existing)}, schema says ${describeKey(declared)}`,
      );
    }
  }
  for (const name of existingKeys.keys()) {
    if (!declaredKeys.has(name)) {
      throw new TypeError(
        `FOREIGN KEY cannot be dropped: ${definition.name} still has ${name}. ` +
          `Declare the relation, or recreate the table without it.`,
      );
    }
  }

  const existingChecks = new Map(record.checks.map((check) => [check.name, check.sql]));
  const declaredChecks = new Map(definition.checks.map((check) => [check.name, check.sql]));
  for (const [name, sql] of declaredChecks) {
    const existing = existingChecks.get(name);
    if (existing === undefined) {
      throw new TypeError(
        `CHECK cannot be added after creation: ${definition.name}.${name}. ` +
          `Existing rows are not known to satisfy it; recreate the table to add a constraint.`,
      );
    }
    if (existing !== sql) {
      throw new TypeError(
        `CHECK cannot change: ${definition.name}.${name} is (${existing}), schema says (${sql})`,
      );
    }
  }
  for (const name of existingChecks.keys()) {
    if (!declaredChecks.has(name)) {
      throw new TypeError(
        `CHECK cannot be dropped: ${definition.name} still has ${name}. ` +
          `Declare the constraint, or recreate the table without it.`,
      );
    }
  }
}

/**
 * Diffs the live catalog against a schema into metadata-only steps, in a deterministic order.
 * Anything unprovable fails explicitly: type changes, dropped columns, unique-key changes,
 * nullable-to-non-null tightening, non-nullable additions, and rename sources that are missing
 * or still defined.
 */
export interface PlanMigrationOptions {
  /**
   * Treats the schema as the whole database, so a managed table it no longer declares is planned
   * for dropping.
   *
   * Off by default, because a schema is not necessarily complete: an application may migrate
   * feature by feature, each call declaring only its own tables. Assuming otherwise would turn
   * that into "drop everything the others made". A column is different — its table is right
   * there in the declaration, so a column missing from it is missing on purpose.
   */
  readonly schemaOwnsDatabase?: boolean;
}

export function planMigration(
  catalog: Catalog,
  definition: SchemaDefinition<readonly AnyTable[]>,
  options: PlanMigrationOptions = {},
): MigrationPlan {
  const steps: MigrationStep[] = [];
  const currentByName = new Map(catalog.tables.map((record) => [record.name, record]));
  for (const tableDefinition of orderedForCreation(definition.tables)) {
    const record = currentByName.get(tableDefinition.name);
    if (record === undefined) {
      steps.push({ kind: "create-table", table: tableDefinition });
      continue;
    }
    const recordColumnsByName = new Map(
      record.columns.map((columnRecord) => [columnRecord.name, columnRecord]),
    );
    const definedNames = new Set(Object.keys(tableDefinition.columns));
    const renameSources = new Set<string>();
    for (const [columnName, columnDefinition] of Object.entries(tableDefinition.columns)) {
      const from = columnDefinition.renamedFromName;
      let existing = recordColumnsByName.get(columnName);
      if (existing === undefined && from !== undefined) {
        const source = recordColumnsByName.get(from);
        if (source !== undefined) {
          if (renameSources.has(from)) {
            throw new TypeError(
              `Rename source is used more than once: ${tableDefinition.name}.${from}`,
            );
          }
          if (definedNames.has(from)) {
            throw new TypeError(`Rename source is still defined: ${tableDefinition.name}.${from}`);
          }
          steps.push({
            kind: "rename-column",
            tableName: tableDefinition.name,
            from,
            to: columnName,
          });
          renameSources.add(from);
          existing = source;
        }
      }
      if (existing === undefined) {
        const backfill = resolveBackfill(columnDefinition);
        if (!columnDefinition.isNullable && backfill === undefined) {
          throw new TypeError(
            `Added columns must be nullable, or carry a backfill: ${tableDefinition.name}.${columnName}`,
          );
        }
        if (columnDefinition.isUnique) {
          throw new TypeError(
            `Unique keys cannot be added after creation: ${tableDefinition.name}.${columnName}`,
          );
        }
        steps.push({
          kind: "add-column",
          tableName: tableDefinition.name,
          columnName,
          definition: columnDefinition,
          ...(backfill === undefined ? {} : { backfill }),
        });
        continue;
      }
      if (existing.type !== columnDefinition.type) {
        throw new TypeError(
          `Column types cannot change: ${tableDefinition.name}.${columnName} is ${existing.type}, schema says ${columnDefinition.type}`,
        );
      }
      const existingEnum = existing.enumValues;
      const definedEnum = columnDefinition.enumValues;
      if (definedEnum !== undefined && existingEnum === undefined) {
        throw new TypeError(
          `Plain string columns cannot tighten to an enum: ${tableDefinition.name}.${columnName}. Existing rows may hold values outside the set.`,
        );
      }
      if (existingEnum !== undefined) {
        if (definedEnum === undefined) {
          steps.push({
            kind: "widen-enum",
            tableName: tableDefinition.name,
            columnName,
            enumValues: null,
          });
        } else {
          const removed = existingEnum.filter((value) => !definedEnum.includes(value));
          if (removed.length > 0) {
            throw new TypeError(
              `Enum values cannot be removed: ${tableDefinition.name}.${columnName} drops ${removed.join(", ")}`,
            );
          }
          if (definedEnum.some((value) => !existingEnum.includes(value))) {
            steps.push({
              kind: "widen-enum",
              tableName: tableDefinition.name,
              columnName,
              enumValues: [...definedEnum],
            });
          }
        }
      }
      const existingIsKey = record.uniqueKeyColumnId === existing.id;
      if (existingIsKey !== columnDefinition.isUnique) {
        throw new TypeError(`Unique keys cannot change: ${tableDefinition.name}.${columnName}`);
      }
      if (existing.nullable && !columnDefinition.isNullable) {
        steps.push({
          kind: "tighten-nullable",
          tableName: tableDefinition.name,
          columnName,
        });
      }
      if (!existing.nullable && columnDefinition.isNullable) {
        steps.push({
          kind: "widen-nullable",
          tableName: tableDefinition.name,
          columnName,
        });
      }
      if (!columnDefaultsEqual(existing.defaultValue, columnDefinition.defaultSpec)) {
        const wasAuto = existing.defaultValue?.kind === "autoincrement";
        const isAuto = columnDefinition.defaultSpec?.kind === "autoincrement";
        if (wasAuto !== isAuto) {
          // Adopting one seeds the counter past the largest key already stored; dropping one
          // simply stops generating. Neither rewrites a row.
          steps.push({
            kind: "set-auto-increment",
            tableName: tableDefinition.name,
            columnName,
            enabled: isAuto,
          });
          continue;
        }
        steps.push({
          kind: "alter-default",
          tableName: tableDefinition.name,
          columnName,
          defaultValue: columnDefinition.defaultSpec ?? null,
        });
      }
    }
    for (const columnRecord of record.columns) {
      if (definedNames.has(columnRecord.name) || renameSources.has(columnRecord.name)) continue;
      assertColumnDroppable(record, columnRecord);
      steps.push({
        kind: "drop-column",
        tableName: tableDefinition.name,
        columnName: columnRecord.name,
      });
    }
    assertConstraintsUnchanged(record, tableDefinition);
  }
  if (options.schemaOwnsDatabase === true) {
    const declaredTables = new Set(definition.tables.map(({ name }) => name));
    for (const record of catalog.tables) {
      // Only tables a migration created; see `managed`. Rows are at stake, so an undeclared
      // table someone else made is never a migration's to remove.
      if (declaredTables.has(record.name) || !record.managed) continue;
      steps.push({ kind: "drop-table", tableName: record.name });
    }
  }
  planViewSteps(catalog, definition, steps);
  // A managed view can depend on a managed table the same authoritative schema removes. Drop
  // dependents first; the SQL engine correctly refuses the reverse order. Replacements stay
  // after table-creation steps so a newly declared body can resolve its sources.
  const droppedViews = steps.filter((step) => step.kind === "drop-view");
  const droppedTables = steps.filter((step) => step.kind === "drop-table");
  return {
    steps: [
      ...steps.filter((step) => step.kind !== "drop-view" && step.kind !== "drop-table"),
      ...droppedViews,
      ...droppedTables,
    ],
  };
}

// --- Typed table handles ------------------------------------------------------------------------

type UniqueKeyValue<TTable extends AnyTable> = {
  [K in keyof TTable["columns"]]: TTable["columns"][K]["isUnique"] extends true
    ? ColumnValue<TTable["columns"][K]>
    : never;
}[keyof TTable["columns"]];

type ColumnArrays<TShape> = { [K in keyof TShape]: ReadonlyArray<TShape[K]> };

interface TypedTableDatabase {
  execute(sql: string, params?: readonly QueryValue[]): Promise<unknown>;
  query(
    sql: string,
    options?: { params?: readonly QueryValue[] },
  ): Promise<{ rows: Array<Record<string, unknown>> }>;
}

/**
 * A thin, fully typed SQL handle: inserts require every non-nullable column, updates exclude the
 * unique key, and reads return the complete inferred row shape. Every operation is rendered as
 * parameterized SQL and goes through the parser/planner/executor; this helper has no private
 * batch or table-read semantics that can drift from SQL.
 */
export function typedTable<TTable extends AnyTable>(
  database: TypedTableDatabase,
  definition: TTable,
): {
  definition: TTable;
  insert(rows: ReadonlyArray<InferInsertRow<TTable>>): Promise<unknown>;
  upsert(rows: ReadonlyArray<InferInsertRow<TTable>>): Promise<unknown>;
  update(input: {
    keys: ReadonlyArray<UniqueKeyValue<TTable>>;
    changes: Partial<ColumnArrays<InferUpdateChanges<TTable>>>;
  }): Promise<unknown>;
  delete(input: { keys: ReadonlyArray<UniqueKeyValue<TTable>> }): Promise<unknown>;
  rows(): Promise<Array<InferRow<TTable>>>;
} {
  const columnNames = Object.keys(definition.columns);
  const quote = (name: string): string => `"${name.replaceAll('"', '""')}"`;
  const uniqueKey = Object.entries(definition.columns).find(
    ([, columnDefinition]) => columnDefinition.isUnique,
  )?.[0];
  const normalizedRows = (
    rows: ReadonlyArray<Record<string, unknown>>,
  ): { names: string[]; rows: Array<Record<string, QueryValue>> } => {
    if (rows.length === 0) throw new TypeError("A batch needs at least one row");
    // Keep extra runtime keys in the SQL column list. TypeScript normally excludes them, but a
    // cast or JavaScript caller must still get the engine's "column does not exist" error rather
    // than having data silently discarded by this facade.
    const names = new Set(columnNames);
    for (const row of rows) for (const name of Object.keys(row)) names.add(name);
    const ordered = [...names];
    return {
      names: ordered,
      rows: rows.map((row) =>
        Object.fromEntries(
          ordered.map((name) => {
            const value = row[name] ?? null;
            const fill = definition.columns[name]?.defaultFn;
            return [name, (value === null && fill !== undefined ? fill() : value) as QueryValue];
          }),
        ),
      ),
    };
  };
  const insert = (
    rows: ReadonlyArray<Record<string, unknown>>,
    replace: boolean,
  ): Promise<unknown> => {
    const normalized = normalizedRows(rows);
    const params: QueryValue[] = [];
    const values = normalized.rows
      .map((row) => {
        const placeholders = normalized.names.map((name) => {
          params.push(row[name] ?? null);
          return `$${String(params.length)}`;
        });
        return `(${placeholders.join(", ")})`;
      })
      .join(", ");
    let sql = `INSERT INTO ${quote(definition.name)} (${normalized.names.map(quote).join(", ")}) VALUES ${values}`;
    if (replace) {
      if (uniqueKey === undefined) {
        throw new TypeError(`Upsert requires a unique key: ${definition.name}`);
      }
      sql += ` ON CONFLICT (${quote(uniqueKey)}) DO REPLACE`;
    }
    return database.execute(sql, params);
  };
  const requireUniqueKey = (operation: string): string => {
    if (uniqueKey === undefined) {
      throw new TypeError(`${operation} requires a table with a unique key: ${definition.name}`);
    }
    return uniqueKey;
  };
  const keyToken = (value: unknown): string =>
    value instanceof Date ? `date:${String(value.getTime())}` : `${typeof value}:${String(value)}`;
  const assertKeys = (keys: readonly unknown[], operation: "update" | "delete"): void => {
    if (keys.length === 0) throw new TypeError(`A ${operation} batch needs at least one key`);
    const seen = new Set<string>();
    for (const key of keys) {
      const token = keyToken(key);
      if (seen.has(token))
        throw new TypeError(`Duplicate key in ${operation} batch: ${String(key)}`);
      seen.add(token);
    }
  };
  return {
    definition,
    insert: (rows) => insert(rows, false),
    upsert: (rows) => insert(rows, true),
    update: (input) => {
      const key = requireUniqueKey("UPDATE");
      assertKeys(input.keys, "update");
      const assignments: string[] = [];
      const params: QueryValue[] = [];
      for (const [name, rawValues] of Object.entries(input.changes)) {
        if (!(name in definition.columns)) {
          // Render it anyway so SQL owns the public validation error.
        }
        const values = rawValues as readonly unknown[];
        if (values.length !== input.keys.length) {
          throw new TypeError(
            `Update column ${name} has ${String(values.length)} values for ${String(input.keys.length)} keys`,
          );
        }
        const branches: string[] = [];
        values.forEach((value, index) => {
          if (value === undefined) return;
          const keyValue = input.keys[index];
          if (keyValue === undefined) {
            throw new TypeError(`Missing update key at position ${String(index)}`);
          }
          params.push(keyValue, value as QueryValue);
          branches.push(
            `WHEN ${quote(key)} = $${String(params.length - 1)} THEN $${String(params.length)}`,
          );
        });
        if (branches.length > 0) {
          assignments.push(`${quote(name)} = CASE ${branches.join(" ")} ELSE ${quote(name)} END`);
        }
      }
      if (assignments.length === 0)
        throw new TypeError("An update batch needs at least one change");
      const keyParams = input.keys.map((value) => value);
      const placeholders = keyParams.map((value) => {
        params.push(value);
        return `$${String(params.length)}`;
      });
      return database.execute(
        `UPDATE ${quote(definition.name)} SET ${assignments.join(", ")} WHERE ${quote(key)} IN (${placeholders.join(", ")})`,
        params,
      );
    },
    delete: (input) => {
      const key = requireUniqueKey("DELETE");
      assertKeys(input.keys, "delete");
      const params = input.keys.map((value) => value);
      const placeholders = params.map((_, index) => `$${String(index + 1)}`);
      return database.execute(
        `DELETE FROM ${quote(definition.name)} WHERE ${quote(key)} IN (${placeholders.join(", ")})`,
        params,
      );
    },
    rows: async () =>
      (
        await database.query(
          `SELECT ${columnNames.map(quote).join(", ")} FROM ${quote(definition.name)}`,
        )
      ).rows as Array<InferRow<TTable>>,
  };
}

/** Applies one table's alteration steps onto its current column records. */
export function applyColumnSteps(
  record: TableRecord,
  steps: readonly MigrationStep[],
  createId: () => string,
): TableColumnRecord[] {
  const columns = structuredClone(record.columns);
  for (const step of steps) {
    if (step.kind === "rename-column") {
      const target = columns.find(({ name }) => name === step.from);
      if (target !== undefined) target.name = step.to;
      continue;
    }
    if (step.kind === "add-column") {
      if (!columns.some(({ name }) => name === step.columnName)) {
        columns.push({
          id: createId(),
          name: step.columnName,
          type: step.definition.type,
          // A backfill is what lets the column be non-nullable: every row has a value, either
          // written or substituted at read time.
          nullable: step.backfill === undefined ? true : step.definition.isNullable,
          ...(step.backfill === undefined ? {} : { backfill: step.backfill }),
          ...(step.definition.defaultSpec === undefined
            ? {}
            : { defaultValue: step.definition.defaultSpec }),
          ...(step.definition.enumValues === undefined
            ? {}
            : { enumValues: [...step.definition.enumValues] }),
        });
      }
      continue;
    }
    if (step.kind === "widen-enum") {
      const target = columns.find(({ name }) => name === step.columnName);
      if (target !== undefined) {
        if (step.enumValues === null) delete target.enumValues;
        else target.enumValues = [...step.enumValues];
      }
      continue;
    }
    if (step.kind === "widen-nullable") {
      const target = columns.find(({ name }) => name === step.columnName);
      if (target !== undefined) target.nullable = true;
      continue;
    }
    if (step.kind === "drop-column") {
      // The column record goes; the stored blocks stay until compaction rewrites the segments
      // that hold them, which is what keeps the drop a metadata step.
      const index = columns.findIndex(({ name }) => name === step.columnName);
      if (index !== -1) columns.splice(index, 1);
      continue;
    }
    if (step.kind === "tighten-nullable") {
      // The proof happens before this runs; see MinnowDatabase#columnHoldsNull.
      const target = columns.find(({ name }) => name === step.columnName);
      if (target !== undefined) target.nullable = false;
      continue;
    }
    if (step.kind === "set-auto-increment") {
      const target = columns.find(({ name }) => name === step.columnName);
      if (target !== undefined) {
        if (step.enabled) target.defaultValue = { kind: "autoincrement" };
        else delete target.defaultValue;
      }
      continue;
    }
    if (step.kind === "alter-default") {
      const target = columns.find(({ name }) => name === step.columnName);
      if (target !== undefined) {
        if (step.defaultValue === null) delete target.defaultValue;
        else target.defaultValue = step.defaultValue;
      }
    }
  }
  return columns;
}

function columnDefaultsEqual(
  left: ColumnDefault | undefined,
  right: ColumnDefault | undefined,
): boolean {
  if (left === undefined || right === undefined) return left === right;
  if (left.kind !== right.kind) return false;
  return left.kind !== "literal" || right.kind !== "literal" || left.value === right.value;
}
