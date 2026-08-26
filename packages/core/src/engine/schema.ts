import { copyDate, dateMilliseconds } from "../date-value.js";
import {
  validateColumnDefault,
  validateEnumValues,
  validateSqlDomain,
  type ColumnDefault,
  type SqlDomain,
  type TableColumnRecord,
  type TableRecord,
} from "../storage/types.js";
import { type Catalog, type CatalogColumn, type CatalogTable } from "./catalog.js";
import {
  compileCheckExpression,
  expressionColumns,
  validateDefaultExpression,
  type QueryValue,
} from "./query.js";
import { externalSqlDomainValue, normalizeSqlDomainValue } from "./sql-domains.js";

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
  readonly enforced: boolean;
}

/** A row condition every write must satisfy (E141-06); `sql` is a boolean expression. */
export interface TableCheck {
  readonly name: string;
  readonly sql: string;
}

/** A table-level relation, used when either side has a composite primary key. */
export interface TableForeignKey<TColumnName extends string = string> {
  readonly name: string;
  readonly columns: readonly TColumnName[];
  readonly references: { readonly table: string; readonly columns: readonly string[] };
  readonly onDelete?: ReferentialAction;
  /** False keeps the relationship in the catalog without validating or cascading rows. */
  readonly enforced?: boolean;
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

export interface ColumnBuilder<
  TValue extends SchemaValue,
  TNullable extends boolean,
  TUnique extends boolean = false,
  THasDefault extends boolean = false,
  TInput extends SchemaValue = TValue,
> {
  readonly kind: "column";
  readonly type: SchemaColumnType;
  readonly isNullable: TNullable;
  readonly isUnique: TUnique;
  readonly hasDefault: THasDefault;
  /** Type-only metadata consumed by adapters; optional so it emits no runtime payload. */
  readonly "~types"?: { readonly select: TValue; readonly input: TInput };
  /** True for exact SQL integer columns; ordinary number columns use Float64 semantics. */
  readonly integer: boolean;
  /** Logical SQL semantics layered over the stable string storage encoding. */
  readonly sqlDomain?: SqlDomain;
  readonly defaultSpec?: ColumnDefault;
  /** Present on `column.enum()` builders: the closed set of values writes must draw from. */
  readonly enumValues?: readonly string[];
  /** What rows written before this column existed read as; see `.backfill()`. */
  readonly backfillValue?: TInput | (() => TInput);
  readonly renamedFromName?: string;
  readonly reference?: ColumnReferenceSpec;
  /** Marks the column nullable; inserts may omit it and reads may return null. */
  nullable(): ColumnBuilder<TValue, true, TUnique, THasDefault, TInput>;
  /** Marks the table's unique key; exactly one non-nullable column may carry it. */
  unique(
    this: ColumnBuilder<TValue, false, TUnique, THasDefault, TInput>,
  ): ColumnBuilder<TValue, TNullable, true, THasDefault, TInput>;
  /** Declares this column as the rename target of an existing catalog column. */
  renamedFrom(name: string): ColumnBuilder<TValue, TNullable, TUnique, THasDefault, TInput>;
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
    this: ColumnBuilder<TValue, false, TUnique, THasDefault, TInput>,
    value: TInput | (() => TInput),
  ): ColumnBuilder<TValue, TNullable, TUnique, THasDefault, TInput>;
  /**
   * Declares a FOREIGN KEY onto another table's unique key. `migrate()` creates it as a real
   * constraint, so a write naming a parent row that does not exist is rejected — the same
   * behaviour as declaring `REFERENCES` in SQL DDL.
   *
   * `onDelete` defaults to `"restrict"`. `"set null"` requires a nullable column.
   */
  references(
    this: ColumnBuilder<TValue, TNullable, TUnique, THasDefault, TInput>,
    table: string,
    column: string,
    options?:
      | { onDelete?: Exclude<ReferentialAction, "set null">; enforced?: true }
      | { enforced: false; onDelete?: never },
  ): ColumnBuilder<TValue, TNullable, TUnique, THasDefault, TInput>;
  references(
    this: ColumnBuilder<TValue, true, TUnique, THasDefault, TInput>,
    table: string,
    column: string,
    options: { onDelete: "set null"; enforced?: true },
  ): ColumnBuilder<TValue, TNullable, TUnique, THasDefault, TInput>;
  /** Declares a literal SQL default. Omission or SQL `DEFAULT` invokes it; NULL does not. */
  default(value: TInput): ColumnBuilder<TValue, TNullable, TUnique, true, TInput>;
  /**
   * Declares a variable-free SQL default expression, such as `CURRENT_TIMESTAMP`,
   * `gen_random_uuid()`, or `nextval('orders_id_seq')`. The engine parses and type-checks it
   * before migration and evaluates it once per omitted row.
   */
  defaultSql(sql: string): ColumnBuilder<TValue, TNullable, TUnique, true, TInput>;
  /**
   * Generates monotonically increasing integers for omitted or SQL `DEFAULT` slots from a persistent
   * per-table counter that is atomic across tabs. Explicit values are allowed and bump the
   * counter past their maximum. Number unique-key columns only.
   */
  autoIncrement: TValue extends number
    ? (
        this: ColumnBuilder<TValue, false, true, THasDefault, TInput>,
      ) => ColumnBuilder<TValue, TNullable, TUnique, true, TInput>
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
  readonly integer: boolean;
  readonly sqlDomain?: SqlDomain;
  readonly defaultSpec?: ColumnDefault;
  readonly enumValues?: readonly string[];
  readonly backfillValue?: SchemaValue | (() => SchemaValue);
  readonly renamedFromName?: string;
  readonly reference?: ColumnReferenceSpec;
}

function defaultSpecFromArg(
  type: SchemaColumnType,
  value: unknown,
  sqlDomain?: SqlDomain,
): ColumnDefault {
  if (sqlDomain?.kind === "numeric") {
    if ((typeof value !== "number" || !Number.isFinite(value)) && typeof value !== "string") {
      throw new TypeError("NUMERIC default must be a finite number or decimal string");
    }
    normalizeSqlDomainValue(sqlDomain, value);
    return { kind: "literal", value };
  }
  switch (type) {
    case "datetime":
      if (!(value instanceof Date) || !Number.isFinite(dateMilliseconds(value))) {
        throw new TypeError("Default literal must be a valid Date");
      }
      return { kind: "literal", value: copyDate(value) };
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

function createColumn<
  TType extends SchemaColumnType,
  TValue extends SchemaValue = ValueOf<TType>,
  TInput extends SchemaValue = TValue,
>(
  type: TType,
  state: {
    isNullable?: boolean;
    isUnique?: boolean;
    integer?: boolean;
    sqlDomain?: SqlDomain;
    renamedFromName?: string;
    reference?: ColumnReferenceSpec;
    defaultSpec?: ColumnDefault | undefined;
    enumValues?: readonly string[];
    backfillValue?: TInput | (() => TInput);
  } = {},
): ColumnBuilder<TValue, false, false, false, TInput> {
  const base = {
    kind: "column" as const,
    type,
    isNullable: (state.isNullable ?? false) as false,
    isUnique: (state.isUnique ?? false) as false,
    hasDefault: (state.defaultSpec !== undefined) as false,
    integer: state.integer ?? false,
    ...(state.sqlDomain === undefined ? {} : { sqlDomain: state.sqlDomain }),
    ...(state.defaultSpec === undefined ? {} : { defaultSpec: state.defaultSpec }),
    ...(state.renamedFromName === undefined ? {} : { renamedFromName: state.renamedFromName }),
    ...(state.reference === undefined ? {} : { reference: state.reference }),
    ...(state.enumValues === undefined ? {} : { enumValues: state.enumValues }),
    ...(state.backfillValue === undefined ? {} : { backfillValue: state.backfillValue }),
  };
  return {
    ...base,
    nullable: () =>
      createColumn<TType, TValue, TInput>(type, {
        ...state,
        isNullable: true,
      }) as unknown as ColumnBuilder<TValue, true, false, false, TInput>,
    unique: () =>
      createColumn<TType, TValue, TInput>(type, {
        ...state,
        isUnique: true,
      }) as unknown as ColumnBuilder<TValue, false, true, false, TInput>,
    renamedFrom: (name: string) => {
      validateSchemaName(name, "Rename source");
      return createColumn<TType, TValue, TInput>(type, { ...state, renamedFromName: name });
    },
    backfill: (value: unknown) =>
      createColumn<TType, TValue, TInput>(type, { ...state, backfillValue: value as TInput }),
    references: (
      table: string,
      referencedColumn: string,
      options: { onDelete?: ReferentialAction; enforced?: boolean } = {},
    ) => {
      validateSchemaName(table, "Referenced table");
      validateSchemaName(referencedColumn, "Referenced column");
      if (options.enforced === false && options.onDelete !== undefined) {
        throw new TypeError("An informational FOREIGN KEY cannot declare ON DELETE behavior");
      }
      return createColumn<TType, TValue, TInput>(type, {
        ...state,
        reference: {
          table,
          column: referencedColumn,
          onDelete: options.onDelete ?? "restrict",
          enforced: options.enforced !== false,
        },
      });
    },
    default: ((value: unknown) => {
      return createColumn<TType, TValue, TInput>(type, {
        ...state,
        defaultSpec: defaultSpecFromArg(type, value, state.sqlDomain),
      });
    }) as unknown as ColumnBuilder<TValue, false, false, false, TInput>["default"],
    defaultSql: ((sql: string) => {
      const expression = sql.trim();
      if (expression.length === 0 || expression !== sql) {
        throw new TypeError("Default SQL must be a trimmed non-empty expression");
      }
      return createColumn<TType, TValue, TInput>(type, {
        ...state,
        defaultSpec: { kind: "expression", sql: expression },
      });
    }) as unknown as ColumnBuilder<TValue, false, false, false, TInput>["defaultSql"],
    autoIncrement: (() => {
      if (type !== "number") {
        throw new TypeError("Auto-increment requires a number column");
      }
      return createColumn<TType, TValue, TInput>(type, {
        ...state,
        defaultSpec: { kind: "autoincrement" },
      });
    }) as unknown as ColumnBuilder<TValue, false, false, false, TInput>["autoIncrement"],
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
 * public `.default()` accepts literals and cannot express auto-increment catalog metadata.
 */
export function columnWithDefaultSpec(
  base: Pick<
    AnyColumn,
    "type" | "isNullable" | "isUnique" | "renamedFromName" | "reference" | "enumValues"
  > &
    Partial<Pick<AnyColumn, "integer" | "sqlDomain">>,
  spec: ColumnDefault,
): AnyColumn {
  return columnFromState({
    type: base.type,
    isNullable: base.isNullable,
    isUnique: base.isUnique,
    integer: base.integer ?? false,
    ...(base.sqlDomain === undefined ? {} : { sqlDomain: base.sqlDomain }),
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
        | "integer"
        | "sqlDomain"
        | "renamedFromName"
        | "reference"
        | "defaultSpec"
        | "enumValues"
        | "backfillValue"
      >
    >,
): AnyColumn {
  return createColumn(state.type, {
    isNullable: state.isNullable,
    isUnique: state.isUnique,
    integer: state.integer ?? false,
    ...(state.sqlDomain === undefined ? {} : { sqlDomain: state.sqlDomain }),
    ...(state.renamedFromName === undefined ? {} : { renamedFromName: state.renamedFromName }),
    ...(state.reference === undefined ? {} : { reference: state.reference }),
    ...(state.defaultSpec === undefined ? {} : { defaultSpec: state.defaultSpec }),
    ...(state.enumValues === undefined ? {} : { enumValues: state.enumValues }),
    ...(state.backfillValue === undefined ? {} : { backfillValue: state.backfillValue }),
  });
}

export const column = {
  boolean: () => createColumn("boolean"),
  number: () => createColumn("number"),
  /** Exact safe-integer semantics, matching SQL INTEGER/SMALLINT/BIGINT. */
  integer: () => createColumn("number", { integer: true }),
  string: () => createColumn("string"),
  datetime: () => createColumn("datetime"),
  /** Exact decimal SQL NUMERIC. Selects return strings; writes accept strings or numbers. */
  numeric: (options: { precision?: number; scale?: number } = {}) => {
    const sqlDomain = validateSqlDomain({ kind: "numeric", ...options }, "numeric column");
    return createColumn<"string", string, string | number>("string", { sqlDomain });
  },
  json: () => createColumn("string", { sqlDomain: { kind: "json" } }),
  jsonb: () => createColumn("string", { sqlDomain: { kind: "jsonb" } }),
  uuid: () => createColumn("string", { sqlDomain: { kind: "uuid" } }),
  /** A zoneless calendar date, represented publicly as canonical `YYYY-MM-DD` text. */
  date: () => createColumn("string", { sqlDomain: { kind: "date" } }),
  time: () => createColumn("string", { sqlDomain: { kind: "time" } }),
  interval: () => createColumn("string", { sqlDomain: { kind: "interval" } }),
  /** JSON array text at the JavaScript boundary, with the SQL element type retained in metadata. */
  array: (element: string) =>
    createColumn("string", {
      sqlDomain: validateSqlDomain({ kind: "array", element }, "array column"),
    }),
  /** A named SQL enum domain, distinct from the lightweight `column.enum()` restriction. */
  sqlEnum: <const TValues extends readonly [string, ...string[]]>(
    name: string,
    values: TValues,
  ): ColumnBuilder<TValues[number], false> => {
    validateSchemaName(name, "Enum type");
    return createColumn<"string", TValues[number]>("string", {
      sqlDomain: validateSqlDomain(
        { kind: "enum", name, values: validateEnumValues(values, name) },
        name,
      ),
    });
  },
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

/** Runtime table shape accepted by schema collections without erasing concrete table inference. */
export interface AnyTable {
  readonly kind: "table";
  readonly name: string;
  readonly columns: Record<string, AnyColumn>;
  readonly primaryKey: readonly string[];
  readonly foreignKeys: readonly TableForeignKey[];
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

export interface TableSchema<
  TColumns extends Record<string, AnyColumn>,
  TName extends string = string,
  TPrimaryKey extends ReadonlyArray<keyof TColumns & string> = readonly [],
> extends AnyTable {
  readonly name: TName;
  readonly columns: TColumns;
  /** Ordered table-level primary key. Empty when `.unique()` owns the scalar identity. */
  readonly primaryKey: TPrimaryKey;
  /** Table-level relations, including composite FOREIGN KEY declarations. */
  readonly foreignKeys: ReadonlyArray<TableForeignKey<keyof TColumns & string>>;
  /** Row conditions every write must satisfy; empty when none were declared. */
  readonly checks: readonly TableCheck[];
}

export interface TableOptions<
  TColumns extends Record<string, AnyColumn> = Record<string, AnyColumn>,
  TPrimaryKey extends ReadonlyArray<keyof TColumns & string> = readonly [],
> {
  /** Ordered PRIMARY KEY columns. Use `.unique()` for the existing scalar shorthand. */
  readonly primaryKey?: TPrimaryKey;
  /** Table-level relations; required for composite FOREIGN KEYs. */
  readonly foreignKeys?: ReadonlyArray<TableForeignKey<keyof TColumns & string>>;
  /**
   * Row conditions every write must satisfy (E141-06), each a boolean SQL expression over this
   * table's own columns — the declarative form of `CONSTRAINT name CHECK (sql)`. The engine
   * compiles each one when the table is created, so an expression it cannot evaluate is refused
   * at migration time rather than on the first write.
   */
  readonly checks?: readonly TableCheck[];
}

function validateDeclaredColumnValue(
  definition: AnyColumn,
  value: unknown,
  context: string,
  label: "Backfill" | "Value",
): void {
  if (definition.sqlDomain !== undefined) {
    try {
      normalizeSqlDomainValue(definition.sqlDomain, value);
    } catch (error) {
      throw new TypeError(
        `${label} does not fit ${context}: ${error instanceof Error ? error.message : String(error)}`,
        { cause: error },
      );
    }
    return;
  }
  const matches =
    definition.type === "datetime"
      ? value instanceof Date && Number.isFinite(dateMilliseconds(value))
      : definition.type === "number"
        ? typeof value === "number" &&
          Number.isFinite(value) &&
          (!definition.integer || Number.isSafeInteger(value))
        : typeof value === definition.type;
  if (!matches) {
    const expected = definition.integer ? "safe integer" : definition.type;
    throw new TypeError(`${label} value must be a ${expected}: ${context}`);
  }
  if (
    definition.enumValues !== undefined &&
    typeof value === "string" &&
    !definition.enumValues.includes(value)
  ) {
    throw new TypeError(
      `${label} value must be one of: ${definition.enumValues.join(", ")} (${context})`,
    );
  }
}

export function table<
  const TName extends string,
  TColumns extends Record<string, AnyColumn>,
  const TPrimaryKey extends ReadonlyArray<keyof TColumns & string>,
>(
  name: TName,
  columns: TColumns,
  options: TableOptions<TColumns, TPrimaryKey> & { readonly primaryKey: TPrimaryKey },
): TableSchema<TColumns, TName, TPrimaryKey>;
export function table<const TName extends string, TColumns extends Record<string, AnyColumn>>(
  name: TName,
  columns: TColumns,
  options?: Omit<TableOptions<TColumns>, "primaryKey"> & {
    readonly primaryKey?: never;
  },
): TableSchema<TColumns, TName>;
export function table(
  name: string,
  columns: Record<string, AnyColumn>,
  options: TableOptions<Record<string, AnyColumn>, readonly string[]> = {},
): AnyTable {
  validateSchemaName(name, "Table");
  const entries = Object.entries(columns);
  if (entries.length === 0) throw new TypeError(`Table ${name} needs at least one column`);
  const uniqueColumns = entries.filter(([, definition]) => definition.isUnique);
  if (uniqueColumns.length > 1) {
    throw new TypeError(`Table ${name} may name at most one unique column`);
  }
  const uniqueEntry = uniqueColumns[0];
  const primaryKey = options.primaryKey ?? [];
  if (uniqueEntry !== undefined && primaryKey.length > 0) {
    throw new TypeError(`Table ${name} cannot declare both .unique() and a table primary key`);
  }
  if (primaryKey.length === 0 && options.primaryKey !== undefined) {
    throw new TypeError(`Table ${name} primary key needs at least one column`);
  }
  if (new Set(primaryKey).size !== primaryKey.length) {
    throw new TypeError(`Table ${name} primary key columns must be distinct`);
  }
  for (const columnName of primaryKey) {
    const definition = columns[columnName];
    if (definition === undefined) {
      throw new TypeError(`PRIMARY KEY column not found: ${name}.${columnName}`);
    }
    if (definition.isNullable) {
      throw new TypeError(`PRIMARY KEY cannot be nullable: ${name}.${columnName}`);
    }
  }
  if (uniqueEntry?.[1].isNullable === true) {
    throw new TypeError(`Table ${name} unique column must not be nullable: ${uniqueEntry[0]}`);
  }
  for (const [columnName, definition] of entries) {
    validateSchemaName(columnName, "Column");
    if (definition.integer && definition.type !== "number") {
      throw new TypeError(`Integer domain requires a number column: ${name}.${columnName}`);
    }
    if (definition.sqlDomain !== undefined) {
      if (definition.type !== "string") {
        throw new TypeError(`SQL domains require string storage: ${name}.${columnName}`);
      }
      validateSqlDomain(definition.sqlDomain, `${name}.${columnName}`);
    }
    if (definition.integer && definition.sqlDomain !== undefined) {
      throw new TypeError(
        `A column cannot be both integer and a SQL domain: ${name}.${columnName}`,
      );
    }
    if (definition.enumValues !== undefined && definition.sqlDomain !== undefined) {
      throw new TypeError(
        `A column cannot have both enum restrictions and a SQL domain: ${name}.${columnName}`,
      );
    }
    const backfill = definition.backfillValue;
    if (backfill !== undefined && typeof backfill !== "function") {
      validateDeclaredColumnValue(definition, backfill, `${name}.${columnName}`, "Backfill");
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
        ...(definition.integer ? { integer: true } : {}),
        ...(definition.sqlDomain === undefined ? {} : { sqlDomain: definition.sqlDomain }),
        nullable: definition.isNullable,
        isUniqueKey:
          definition.isUnique || (primaryKey.length === 1 && primaryKey[0] === columnName),
        ...(definition.enumValues === undefined ? {} : { enumValues: definition.enumValues }),
      },
      spec,
    );
    if (spec.kind === "expression") {
      validateDefaultExpression(spec.sql, {
        name: `${name}.${columnName}`,
        type: definition.type,
        ...(definition.sqlDomain === undefined ? {} : { sqlDomain: definition.sqlDomain }),
      });
    }
    if (spec.kind === "literal" && definition.sqlDomain !== undefined) {
      normalizeSqlDomainValue(definition.sqlDomain, spec.value);
    }
  }
  const foreignKeys = options.foreignKeys ?? [];
  const constraintNames = new Set<string>();
  const foreignKeyNames = new Set<string>();
  for (const key of foreignKeys) {
    validateSchemaName(key.name, "FOREIGN KEY");
    if (key.enforced === false && key.onDelete !== undefined) {
      throw new TypeError(
        `Informational FOREIGN KEY ${key.name} cannot declare ON DELETE behavior`,
      );
    }
    if (foreignKeyNames.has(key.name)) {
      throw new TypeError(`Duplicate FOREIGN KEY in table ${name}: ${key.name}`);
    }
    foreignKeyNames.add(key.name);
    if (constraintNames.has(key.name)) {
      throw new TypeError(`Duplicate constraint in table ${name}: ${key.name}`);
    }
    constraintNames.add(key.name);
    if (key.columns.length === 0 || new Set(key.columns).size !== key.columns.length) {
      throw new TypeError(`FOREIGN KEY ${key.name} needs distinct child columns`);
    }
    if (
      key.references.columns.length === 0 ||
      new Set(key.references.columns).size !== key.references.columns.length
    ) {
      throw new TypeError(`FOREIGN KEY ${key.name} needs distinct parent columns`);
    }
    if (key.columns.length !== key.references.columns.length) {
      throw new TypeError(`FOREIGN KEY ${key.name} has different child and parent arity`);
    }
    validateSchemaName(key.references.table, "Referenced table");
    key.references.columns.forEach((columnName) =>
      validateSchemaName(columnName, "Referenced column"),
    );
    for (const columnName of key.columns) {
      const definition = columns[columnName];
      if (definition === undefined) {
        throw new TypeError(
          `FOREIGN KEY ${key.name} names an unknown column: ${name}.${columnName}`,
        );
      }
      if ((key.onDelete ?? "restrict") === "set null" && !definition.isNullable) {
        throw new TypeError(`FOREIGN KEY ${key.name} cannot SET NULL a NOT NULL column`);
      }
    }
  }
  const checks = options.checks ?? [];
  const checkNames = new Set<string>();
  for (const check of checks) {
    validateSchemaName(check.name, "CHECK");
    if (checkNames.has(check.name)) {
      throw new TypeError(`Duplicate CHECK in table ${name}: ${check.name}`);
    }
    checkNames.add(check.name);
    if (constraintNames.has(check.name)) {
      throw new TypeError(`Duplicate constraint in table ${name}: ${check.name}`);
    }
    constraintNames.add(check.name);
    if (check.sql.trim().length === 0) {
      throw new TypeError(`CHECK ${check.name} in table ${name} has no expression`);
    }
    for (const reference of expressionColumns(compileCheckExpression(check.sql, check.name))) {
      const pieces = reference.split(".");
      const columnName = pieces.at(-1) ?? reference;
      const qualifier = pieces.length > 1 ? pieces.slice(0, -1).join(".") : undefined;
      if (qualifier !== undefined && qualifier !== name) {
        throw new TypeError(`CHECK ${check.name} references another table: ${reference}`);
      }
      if (columns[columnName] === undefined) {
        throw new TypeError(`CHECK ${check.name} names an unknown column: ${columnName}`);
      }
    }
  }
  return {
    kind: "table",
    name,
    columns,
    primaryKey,
    foreignKeys,
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
          if (columnValue === undefined) {
            if (!definition.isNullable && definition.defaultSpec === undefined) {
              issues.push({ message: `Missing non-nullable column`, path: [columnName] });
            }
            continue;
          }
          if (columnValue === null) {
            if (!definition.isNullable) {
              issues.push({ message: `Expected non-null value`, path: [columnName] });
            }
            continue;
          }
          try {
            validateDeclaredColumnValue(definition, columnValue, `${name}.${columnName}`, "Value");
          } catch (error) {
            const message =
              definition.enumValues !== undefined && typeof columnValue === "string"
                ? `Expected one of: ${definition.enumValues.join(", ")}`
                : definition.sqlDomain === undefined
                  ? `Expected ${definition.integer ? "safe integer" : definition.type}`
                  : error instanceof Error
                    ? error.message
                    : `Invalid value`;
            issues.push({
              message,
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
  columns: string[];
  parentTable: string;
  parentColumns: string[];
  onDelete: ReferentialAction;
  enforced: boolean;
}> {
  const keys: ReturnType<typeof declaredForeignKeys> = [];
  for (const [columnName, columnDefinition] of Object.entries(definition.columns)) {
    const reference = columnDefinition.reference;
    if (reference === undefined) continue;
    keys.push({
      name: foreignKeyName(definition.name, columnName),
      columns: [columnName],
      parentTable: reference.table,
      parentColumns: [reference.column],
      onDelete: reference.onDelete,
      enforced: reference.enforced,
    });
  }
  for (const key of definition.foreignKeys) {
    const childColumns = [...key.columns];
    const parentColumns = [...key.references.columns];
    keys.push({
      name: key.name,
      columns: childColumns,
      parentTable: key.references.table,
      parentColumns,
      onDelete: key.onDelete ?? "restrict",
      enforced: key.enforced !== false,
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
    if (columnDefinition.defaultSpec !== undefined) {
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
    const declaredNames = new Set<string>();
    for (const key of declaredForeignKeys(definition)) {
      if (declaredNames.has(key.name)) {
        throw new TypeError(`Duplicate FOREIGN KEY in table ${definition.name}: ${key.name}`);
      }
      declaredNames.add(key.name);
      const childNames = key.columns;
      const parentNames = key.parentColumns;
      const target = tables.find(({ name }) => name === key.parentTable);
      if (
        target === undefined ||
        childNames.some((columnName) => !(columnName in definition.columns)) ||
        parentNames.some((columnName) => !(columnName in target.columns))
      ) {
        throw new TypeError(
          `Relation target does not exist: ${definition.name}.${childNames.join(",")} -> ${key.parentTable}.${parentNames.join(",")}`,
        );
      }
      const targetPrimary =
        target.primaryKey.length > 0
          ? [...target.primaryKey]
          : Object.entries(target.columns).flatMap(([name, columnDefinition]) =>
              columnDefinition.isUnique ? [name] : [],
            );
      if (
        parentNames.length !== targetPrimary.length ||
        parentNames.some((columnName, index) => columnName !== targetPrimary[index])
      ) {
        throw new TypeError(
          `Relation target must be the unique key: ${definition.name}.${childNames.join(",")} -> ${key.parentTable}(${targetPrimary.join(", ")})`,
        );
      }
      childNames.forEach((childName, index) => {
        const child = definition.columns[childName];
        const parentName = parentNames[index];
        const parent = parentName === undefined ? undefined : target.columns[parentName];
        if (
          child === undefined ||
          child.type !== parent?.type ||
          child.integer !== parent.integer ||
          JSON.stringify(child.sqlDomain ?? null) !== JSON.stringify(parent.sqlDomain ?? null)
        ) {
          throw new TypeError(
            `Relation types must match: ${definition.name}.${childName} and ${key.parentTable}.${String(parentName)}`,
          );
        }
      });
    }
  }
  return { kind: "schema", tables, views };
}

// --- Compile-time row types ---------------------------------------------------------------------

type ColumnTypeMetadata<TColumn> = TColumn extends {
  readonly "~types"?: infer TMetadata extends { readonly select: unknown; readonly input: unknown };
}
  ? NonNullable<TMetadata>
  : never;

type ColumnValue<TColumn extends AnyColumn> = TColumn["isNullable"] extends true
  ? ColumnTypeMetadata<TColumn>["select"] | null
  : ColumnTypeMetadata<TColumn>["select"];

type ColumnInputValue<TColumn extends AnyColumn> = TColumn["isNullable"] extends true
  ? ColumnTypeMetadata<TColumn>["input"] | null
  : ColumnTypeMetadata<TColumn>["input"];

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
  { [K in keyof TTable["columns"]]: ColumnInputValue<TTable["columns"][K]> },
  OptionalInsertKeys<TTable>
> & {
  [K in OptionalInsertKeys<TTable>]?: ColumnInputValue<TTable["columns"][K]>;
};

/** Scalar `.unique()` keys plus columns named by a table-level primary key. */
export type PrimaryKeyKeys<TTable extends AnyTable> =
  | {
      [K in keyof TTable["columns"]]: TTable["columns"][K]["isUnique"] extends true ? K : never;
    }[keyof TTable["columns"]]
  | TTable["primaryKey"][number];

/**
 * Update changes may cover any column except the unique key. An explicit `undefined` entry is
 * allowed and means "leave this column untouched", so a spread-patch built from optional fields
 * stays assignable under `exactOptionalPropertyTypes`.
 */
export type InferUpdateChanges<TTable extends AnyTable> = {
  [K in keyof TTable["columns"] as K extends PrimaryKeyKeys<TTable> ? never : K]?:
    ColumnInputValue<TTable["columns"][K]> | undefined;
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
  /** Informational relationships are catalog-only and may change without scanning stored rows. */
  | {
      kind: "alter-foreign-keys";
      tableName: string;
      foreignKeys: ReturnType<typeof declaredForeignKeys>;
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
  if (
    record.uniqueKeyColumnId === column.id ||
    (record.primaryKeyColumnIds ?? []).includes(column.id)
  ) {
    throw new TypeError(
      `The unique key cannot be dropped: ${where}. Unique-key changes need the table recreated.`,
    );
  }
  for (const key of record.foreignKeys) {
    if (key.columns.includes(column.name)) {
      throw new TypeError(`FOREIGN KEY ${key.name} still uses this column: ${where}`);
    }
  }
  for (const index of record.indexes ?? []) {
    if (index.columns.some(({ name }) => name === column.name)) {
      throw new TypeError(`Index ${index.name} still uses this column: ${where}`);
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

function assertColumnRenamable(
  catalog: Catalog,
  record: CatalogTable,
  column: CatalogColumn,
): void {
  const where = `${record.name}.${column.name}`;
  for (const owner of catalog.tables) {
    for (const key of owner.foreignKeys) {
      const usesChild = owner.name === record.name && key.columns.includes(column.name);
      const usesParent = key.parentTable === record.name && key.parentColumns.includes(column.name);
      if (usesChild || usesParent) {
        throw new TypeError(`FOREIGN KEY ${key.name} prevents renaming ${where}`);
      }
    }
  }
  for (const check of record.checks) {
    const referenced = expressionColumns(compileCheckExpression(check.sql, check.name));
    if (referenced.includes(column.name)) {
      throw new TypeError(`CHECK ${check.name} prevents renaming ${where}`);
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
  const value = typeof declared === "function" ? declared() : declared;
  validateDeclaredColumnValue(definition, value, "added column", "Backfill");
  return value;
}

function storedBackfill(
  definition: AnyColumn,
  value: boolean | number | string | Date,
): boolean | number | string | Date {
  if (definition.sqlDomain === undefined) return value;
  const normalized = normalizeSqlDomainValue(definition.sqlDomain, value);
  if (normalized === null) throw new TypeError("A backfill cannot be NULL");
  return normalized;
}

function backfillsEqual(left: unknown, right: unknown): boolean {
  const externalLeft = externalSqlDomainValue(left);
  const externalRight = externalSqlDomainValue(right);
  if (externalLeft instanceof Date || externalRight instanceof Date) {
    return (
      externalLeft instanceof Date &&
      externalRight instanceof Date &&
      dateMilliseconds(externalLeft) === dateMilliseconds(externalRight)
    );
  }
  return externalLeft === externalRight;
}

/**
 * Enforced constraints on an existing table cannot change through a metadata-only step. Attaching
 * a FOREIGN KEY or CHECK to a table that already holds rows would claim something about those rows
 * that nobody has verified, and no validation scan exists; dropping one is refused for the mirror
 * reason, so that a constraint never disappears because a schema file drifted. Informational
 * foreign keys are catalog metadata only, so adding, dropping, or remapping one is safe.
 */
function planConstraintChanges(
  record: CatalogTable,
  definition: AnyTable,
  steps: MigrationStep[],
): void {
  const describeKey = (key: {
    columns: readonly string[];
    parentTable: string;
    parentColumns: readonly string[];
    onDelete: string;
  }): string =>
    `${key.columns.join(",")} -> ${key.parentTable}.` +
    `${key.parentColumns.join(",")} ON DELETE ${key.onDelete}`;

  const declaredList = declaredForeignKeys(definition);
  const existingKeys = new Map(record.foreignKeys.map((key) => [key.name, key]));
  const declaredKeys = new Map(declaredList.map((key) => [key.name, key]));
  let informationalChanged = false;
  for (const [name, declared] of declaredKeys) {
    const existing = existingKeys.get(name);
    if (existing === undefined) {
      if (!declared.enforced) {
        informationalChanged = true;
        continue;
      }
      throw new TypeError(
        `FOREIGN KEY cannot be added after creation: ${definition.name}.${declared.columns.join(",")}. ` +
          `Existing rows are not known to satisfy it; recreate the table to add a relation.`,
      );
    }
    if (existing.enforced !== declared.enforced) {
      throw new TypeError(
        `FOREIGN KEY enforcement cannot change: ${definition.name}.${declared.columns.join(",")}`,
      );
    }
    if (describeKey(existing) !== describeKey(declared)) {
      if (!declared.enforced) {
        informationalChanged = true;
        continue;
      }
      throw new TypeError(
        `FOREIGN KEY cannot change: ${definition.name}.${declared.columns.join(",")} is ` +
          `${describeKey(existing)}, schema says ${describeKey(declared)}`,
      );
    }
  }
  for (const name of existingKeys.keys()) {
    if (!declaredKeys.has(name)) {
      if (existingKeys.get(name)?.enforced === false) {
        informationalChanged = true;
        continue;
      }
      throw new TypeError(
        `FOREIGN KEY cannot be dropped: ${definition.name} still has ${name}. ` +
          `Declare the relation, or recreate the table without it.`,
      );
    }
  }
  if (informationalChanged) {
    steps.push({
      kind: "alter-foreign-keys",
      tableName: definition.name,
      foreignKeys: declaredList,
    });
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
          assertColumnRenamable(catalog, record, source);
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
        if (columnDefinition.isUnique || tableDefinition.primaryKey.includes(columnName)) {
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
      if (
        (existing.integer === true) !== columnDefinition.integer ||
        JSON.stringify(existing.sqlDomain ?? null) !==
          JSON.stringify(columnDefinition.sqlDomain ?? null)
      ) {
        throw new TypeError(`Column domains cannot change: ${tableDefinition.name}.${columnName}`);
      }
      if (existing.backfill === undefined && columnDefinition.backfillValue !== undefined) {
        throw new TypeError(
          `Backfills cannot be added after a column exists: ${tableDefinition.name}.${columnName}`,
        );
      }
      if (existing.backfill !== undefined && columnDefinition.backfillValue === undefined) {
        throw new TypeError(`Backfills cannot be removed: ${tableDefinition.name}.${columnName}`);
      }
      if (
        existing.backfill !== undefined &&
        columnDefinition.backfillValue !== undefined &&
        typeof columnDefinition.backfillValue !== "function" &&
        !backfillsEqual(existing.backfill, columnDefinition.backfillValue)
      ) {
        throw new TypeError(`Backfills cannot change: ${tableDefinition.name}.${columnName}`);
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
      const existingIsKey =
        record.uniqueKeyColumnId === existing.id ||
        (record.primaryKeyColumnIds ?? []).includes(existing.id);
      const definedIsKey =
        columnDefinition.isUnique || tableDefinition.primaryKey.includes(columnName);
      if (existingIsKey !== definedIsKey) {
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
    const existingKeyIds =
      record.primaryKeyColumnIds ??
      (record.uniqueKeyColumnId === undefined ? [] : [record.uniqueKeyColumnId]);
    const desiredKeyNames =
      tableDefinition.primaryKey.length > 0
        ? tableDefinition.primaryKey
        : Object.entries(tableDefinition.columns).flatMap(([name, definition]) =>
            definition.isUnique ? [name] : [],
          );
    const desiredKeyIds = desiredKeyNames.map((name) => {
      const definition = tableDefinition.columns[name];
      const currentName = definition?.renamedFromName ?? name;
      return recordColumnsByName.get(name)?.id ?? recordColumnsByName.get(currentName)?.id;
    });
    if (
      desiredKeyIds.length !== existingKeyIds.length ||
      desiredKeyIds.some((id, index) => id !== existingKeyIds[index])
    ) {
      throw new TypeError(`Primary key order cannot change: ${tableDefinition.name}`);
    }
    planConstraintChanges(record, tableDefinition, steps);
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

type ScalarUniqueKeyValue<TTable extends AnyTable> = {
  [K in keyof TTable["columns"]]: TTable["columns"][K]["isUnique"] extends true
    ? ColumnValue<TTable["columns"][K]>
    : never;
}[keyof TTable["columns"]];

type DeclaredPrimaryKeyValue<TTable extends AnyTable> = {
  [K in TTable["primaryKey"][number]]: ColumnValue<TTable["columns"][K]>;
};

/** Scalar keys stay scalar; composite keys are objects keyed by their declared column names. */
export type PrimaryKeyValue<TTable extends AnyTable> = TTable["primaryKey"] extends readonly []
  ? ScalarUniqueKeyValue<TTable>
  : TTable["primaryKey"] extends readonly [infer TOnly extends keyof TTable["columns"]]
    ? ColumnValue<TTable["columns"][TOnly]>
    : DeclaredPrimaryKeyValue<TTable>;

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
    keys: ReadonlyArray<PrimaryKeyValue<TTable>>;
    changes: Partial<ColumnArrays<InferUpdateChanges<TTable>>>;
  }): Promise<unknown>;
  delete(input: { keys: ReadonlyArray<PrimaryKeyValue<TTable>> }): Promise<unknown>;
  rows(): Promise<Array<InferRow<TTable>>>;
} {
  const columnNames = Object.keys(definition.columns);
  const quote = (name: string): string => `"${name.replaceAll('"', '""')}"`;
  const scalarUniqueKey = Object.entries(definition.columns).find(
    ([, columnDefinition]) => columnDefinition.isUnique,
  )?.[0];
  const keyColumns =
    definition.primaryKey.length > 0
      ? [...definition.primaryKey]
      : scalarUniqueKey === undefined
        ? []
        : [scalarUniqueKey];
  const normalizedRows = (
    rows: ReadonlyArray<Record<string, unknown>>,
  ): { names: string[]; rows: ReadonlyArray<Record<string, unknown>> } => {
    if (rows.length === 0) throw new TypeError("A batch needs at least one row");
    // Keep extra runtime keys in the SQL column list. TypeScript normally excludes them, but a
    // cast or JavaScript caller must still get the engine's "column does not exist" error rather
    // than having data silently discarded by this facade.
    const names = new Set(columnNames);
    for (const row of rows) for (const name of Object.keys(row)) names.add(name);
    const ordered = [...names];
    return { names: ordered, rows };
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
          const value = row[name];
          if (!Object.hasOwn(row, name) || value === undefined) return "DEFAULT";
          params.push(value as QueryValue);
          return `$${String(params.length)}`;
        });
        return `(${placeholders.join(", ")})`;
      })
      .join(", ");
    let sql = `INSERT INTO ${quote(definition.name)} (${normalized.names.map(quote).join(", ")}) VALUES ${values}`;
    if (replace) {
      if (keyColumns.length === 0) {
        throw new TypeError(`Upsert requires a unique key: ${definition.name}`);
      }
      sql += ` ON CONFLICT (${keyColumns.map(quote).join(", ")}) DO REPLACE`;
    }
    return database.execute(sql, params);
  };
  const requireUniqueKey = (operation: string): readonly string[] => {
    if (keyColumns.length === 0) {
      throw new TypeError(`${operation} requires a table with a unique key: ${definition.name}`);
    }
    return keyColumns;
  };
  const keyParts = (key: unknown): unknown[] => {
    if (keyColumns.length === 1) return [key];
    if (typeof key !== "object" || key === null) {
      throw new TypeError(`Composite key for ${definition.name} must be an object`);
    }
    return keyColumns.map((name) => (key as Record<string, unknown>)[name]);
  };
  const keyToken = (value: unknown): string => {
    const parts = keyParts(value);
    return JSON.stringify(
      parts.map((part) =>
        part instanceof Date ? ["date", dateMilliseconds(part)] : [typeof part, String(part)],
      ),
    );
  };
  const assertKeys = (keys: readonly unknown[], operation: "update" | "delete"): void => {
    if (keys.length === 0) throw new TypeError(`A ${operation} batch needs at least one key`);
    const seen = new Set<string>();
    for (const key of keys) {
      if (keyParts(key).some((part) => part === undefined || part === null)) {
        throw new TypeError(`Missing ${operation} key value for ${definition.name}`);
      }
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
      const keys = requireUniqueKey("UPDATE");
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
          const conditions = keyParts(keyValue).map((part, keyIndex) => {
            params.push(part as QueryValue);
            return `${quote(keys[keyIndex] ?? "")} = $${String(params.length)}`;
          });
          params.push(value as QueryValue);
          branches.push(`WHEN ${conditions.join(" AND ")} THEN $${String(params.length)}`);
        });
        if (branches.length > 0) {
          assignments.push(`${quote(name)} = CASE ${branches.join(" ")} ELSE ${quote(name)} END`);
        }
      }
      if (assignments.length === 0)
        throw new TypeError("An update batch needs at least one change");
      const predicates = input.keys.map((keyValue) => {
        const conditions = keyParts(keyValue).map((part, keyIndex) => {
          params.push(part as QueryValue);
          return `${quote(keys[keyIndex] ?? "")} = $${String(params.length)}`;
        });
        return `(${conditions.join(" AND ")})`;
      });
      return database.execute(
        `UPDATE ${quote(definition.name)} SET ${assignments.join(", ")} WHERE ${predicates.join(" OR ")}`,
        params,
      );
    },
    delete: (input) => {
      const keys = requireUniqueKey("DELETE");
      assertKeys(input.keys, "delete");
      const params: QueryValue[] = [];
      const predicates = input.keys.map((keyValue) => {
        const conditions = keyParts(keyValue).map((part, keyIndex) => {
          params.push(part as QueryValue);
          return `${quote(keys[keyIndex] ?? "")} = $${String(params.length)}`;
        });
        return `(${conditions.join(" AND ")})`;
      });
      return database.execute(
        `DELETE FROM ${quote(definition.name)} WHERE ${predicates.join(" OR ")}`,
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
          ...(step.definition.integer ? { integer: true } : {}),
          ...(step.definition.sqlDomain === undefined
            ? {}
            : { sqlDomain: structuredClone(step.definition.sqlDomain) }),
          // A backfill is what lets the column be non-nullable: every row has a value, either
          // written or substituted at read time.
          nullable: step.backfill === undefined ? true : step.definition.isNullable,
          ...(step.backfill === undefined
            ? {}
            : {
                backfill: storedBackfill(step.definition, step.backfill),
              }),
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
  if (left.kind === "expression" && right.kind === "expression") return left.sql === right.sql;
  if (left.kind !== "literal" || right.kind !== "literal") return true;
  if (left.value instanceof Date || right.value instanceof Date) {
    return (
      left.value instanceof Date &&
      right.value instanceof Date &&
      dateMilliseconds(left.value) === dateMilliseconds(right.value)
    );
  }
  return left.value === right.value;
}
