import {
  validateColumnDefault,
  type ColumnDefault,
  type TableColumnRecord,
  type TableRecord,
} from "../storage/index.js";
import { type BatchRow } from "./batch.js";

/**
 * Typed schema DSL and catalog migration planning. Column builders carry compile-time value and
 * nullability types; `planMigration` diffs a schema against the live catalog into metadata-only
 * steps and rejects everything it cannot prove safe. Physical rewrites never happen here: adding
 * a nullable column, renaming a column through its stable ID, and widening nullability are
 * catalog-only, and older segments read the new column as NULL.
 */

export type SchemaColumnType = "boolean" | "number" | "string" | "datetime";

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
 *   notes: { id: Generated<number>; slug: Generated<string>; body: string };
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
  readonly renamedFromName?: string;
  readonly reference?: { table: string; column: string };
  /** Marks the column nullable; inserts may omit it and reads may return null. */
  nullable(): ColumnBuilder<TValue, true, TUnique, THasDefault>;
  /** Marks the table's unique key; exactly one non-nullable column may carry it. */
  unique(): ColumnBuilder<TValue, TNullable, true, THasDefault>;
  /** Declares this column as the rename target of an existing catalog column. */
  renamedFrom(name: string): ColumnBuilder<TValue, TNullable, TUnique, THasDefault>;
  /** Declares a relation for catalog metadata and validation; not enforced at write time. */
  references(table: string, column: string): ColumnBuilder<TValue, TNullable, TUnique, THasDefault>;
  /**
   * Fills null-or-absent slots at insert time. On string columns "uuid" and "nanoid" name
   * generators (and are therefore reserved as literals); datetime columns accept only "now";
   * any other value is a constant. Requires a non-nullable column.
   */
  default(
    value: TValue extends Date ? "now" : TValue,
  ): ColumnBuilder<TValue, TNullable, TUnique, true>;
  /**
   * Generates monotonically increasing integers for null-or-absent slots from a persistent
   * per-table counter that is atomic across tabs. Explicit values are allowed and bump the
   * counter past their maximum. Number unique-key columns only.
   */
  autoIncrement: TValue extends number
    ? () => ColumnBuilder<TValue, TNullable, TUnique, true>
    : never;
}

type AnyColumn = ColumnBuilder<boolean | number | string | Date, boolean, boolean, boolean>;

function defaultSpecFromArg(type: SchemaColumnType, value: unknown): ColumnDefault {
  switch (type) {
    case "datetime":
      if (value !== "now") throw new TypeError('Datetime columns default with "now"');
      return { kind: "now" };
    case "string":
      if (typeof value !== "string") throw new TypeError("Default literal must be a string");
      if (value === "uuid") return { kind: "uuid" };
      if (value === "nanoid") return { kind: "nanoid" };
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
    Pick<AnyColumn, "isNullable" | "isUnique" | "renamedFromName" | "reference" | "defaultSpec">
  > = {},
): ColumnBuilder<ValueOf<TType>, false> {
  const base = {
    kind: "column" as const,
    type,
    isNullable: (state.isNullable ?? false) as false,
    isUnique: (state.isUnique ?? false) as false,
    hasDefault: (state.defaultSpec !== undefined) as false,
    ...(state.defaultSpec === undefined ? {} : { defaultSpec: state.defaultSpec }),
    ...(state.renamedFromName === undefined ? {} : { renamedFromName: state.renamedFromName }),
    ...(state.reference === undefined ? {} : { reference: state.reference }),
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
    renamedFrom: (name: string) => createColumn(type, { ...state, renamedFromName: name }),
    references: (table: string, referencedColumn: string) =>
      createColumn(type, { ...state, reference: { table, column: referencedColumn } }),
    default: ((value: unknown) =>
      createColumn(type, {
        ...state,
        defaultSpec: defaultSpecFromArg(type, value),
      })) as unknown as ColumnBuilder<ValueOf<TType>, false>["default"],
    autoIncrement: (() => {
      if (type !== "number") {
        throw new TypeError("Auto-increment requires a number column");
      }
      return createColumn(type, { ...state, defaultSpec: { kind: "autoincrement" } });
    }) as unknown as ColumnBuilder<ValueOf<TType>, false>["autoIncrement"],
  };
}

/**
 * Rebuilds a column carrying an exact default spec — the wire layer's escape hatch, since the
 * public `.default()` interprets the reserved generator names and cannot express them as
 * literals.
 */
export function columnWithDefaultSpec(
  base: Pick<AnyColumn, "type" | "isNullable" | "isUnique" | "renamedFromName" | "reference">,
  spec: ColumnDefault,
): AnyColumn {
  return createColumn(base.type, {
    isNullable: base.isNullable,
    isUnique: base.isUnique,
    ...(base.renamedFromName === undefined ? {} : { renamedFromName: base.renamedFromName }),
    ...(base.reference === undefined ? {} : { reference: base.reference }),
    defaultSpec: spec,
  }) as unknown as AnyColumn;
}

export const column = {
  boolean: () => createColumn("boolean"),
  number: () => createColumn("number"),
  string: () => createColumn("string"),
  datetime: () => createColumn("datetime"),
};

export interface TableSchema<
  TColumns extends Record<string, AnyColumn>,
  TName extends string = string,
> {
  readonly kind: "table";
  readonly name: TName;
  readonly columns: TColumns;
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

export function table<const TName extends string, TColumns extends Record<string, AnyColumn>>(
  name: TName,
  columns: TColumns,
): TableSchema<TColumns, TName> {
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
    const spec = definition.defaultSpec;
    if (spec === undefined) continue;
    validateColumnDefault(
      {
        name: columnName,
        type: definition.type,
        nullable: definition.isNullable,
        isUniqueKey: definition.isUnique,
      },
      spec,
    );
  }
  return {
    kind: "table",
    name,
    columns,
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
            // A default-bearing column may be omitted — the engine fills it at insert time.
            if (!definition.isNullable && definition.defaultSpec === undefined) {
              issues.push({ message: `Missing non-nullable column`, path: [columnName] });
            }
            continue;
          }
          const matches =
            definition.type === "datetime"
              ? columnValue instanceof Date
              : typeof columnValue === definition.type;
          if (!matches) {
            issues.push({ message: `Expected ${definition.type}`, path: [columnName] });
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

export interface SchemaDefinition<TTables extends readonly AnyTable[]> {
  readonly kind: "schema";
  readonly tables: TTables;
}

export function schema<TTables extends readonly AnyTable[]>(
  tables: TTables,
): SchemaDefinition<TTables> {
  const names = new Set<string>();
  for (const definition of tables) {
    if (names.has(definition.name)) {
      throw new TypeError(`Duplicate table in schema: ${definition.name}`);
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
    }
  }
  return { kind: "schema", tables };
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

/** Update changes may cover any column except the unique key. */
export type InferUpdateChanges<TTable extends AnyTable> = Partial<{
  [
    K in keyof TTable["columns"] as TTable["columns"][K]["isUnique"] extends true ? never : K
  ]: ColumnValue<TTable["columns"][K]>;
}>;

// --- Migration planning -------------------------------------------------------------------------

export type MigrationStep =
  | { kind: "create-table"; table: AnyTable }
  | { kind: "add-column"; tableName: string; columnName: string; definition: AnyColumn }
  | { kind: "rename-column"; tableName: string; from: string; to: string }
  | { kind: "widen-nullable"; tableName: string; columnName: string }
  | {
      kind: "alter-default";
      tableName: string;
      columnName: string;
      defaultValue: ColumnDefault | null;
    };

export interface MigrationPlan {
  steps: MigrationStep[];
}

/**
 * Diffs the live catalog against a schema into metadata-only steps, in a deterministic order.
 * Anything unprovable fails explicitly: type changes, dropped columns, unique-key changes,
 * nullable-to-non-null tightening, non-nullable additions, and rename sources that are missing
 * or still defined.
 */
export function planMigration(
  current: readonly TableRecord[],
  definition: SchemaDefinition<readonly AnyTable[]>,
): MigrationPlan {
  const steps: MigrationStep[] = [];
  const currentByName = new Map(current.map((record) => [record.name, record]));
  for (const tableDefinition of definition.tables) {
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
        if (!columnDefinition.isNullable) {
          throw new TypeError(
            `Added columns must be nullable: ${tableDefinition.name}.${columnName}`,
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
        });
        continue;
      }
      if (existing.type !== columnDefinition.type) {
        throw new TypeError(
          `Column types cannot change: ${tableDefinition.name}.${columnName} is ${existing.type}, schema says ${columnDefinition.type}`,
        );
      }
      const existingIsKey = record.uniqueKeyColumnId === existing.id;
      if (existingIsKey !== columnDefinition.isUnique) {
        throw new TypeError(`Unique keys cannot change: ${tableDefinition.name}.${columnName}`);
      }
      if (existing.nullable && !columnDefinition.isNullable) {
        throw new TypeError(
          `Nullable columns cannot tighten to non-null: ${tableDefinition.name}.${columnName}`,
        );
      }
      if (!existing.nullable && columnDefinition.isNullable) {
        steps.push({
          kind: "widen-nullable",
          tableName: tableDefinition.name,
          columnName,
        });
      }
      if (!columnDefaultsEqual(existing.defaultValue, columnDefinition.defaultSpec)) {
        if (
          existing.defaultValue?.kind === "autoincrement" ||
          columnDefinition.defaultSpec?.kind === "autoincrement"
        ) {
          throw new TypeError(
            `Auto-increment cannot be added or removed after creation: ${tableDefinition.name}.${columnName}. Existing rows cannot be backfilled; recreate the table to change key generation.`,
          );
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
      if (!definedNames.has(columnRecord.name) && !renameSources.has(columnRecord.name)) {
        throw new TypeError(
          `Dropping columns is not supported: ${tableDefinition.name}.${columnRecord.name}`,
        );
      }
    }
  }
  return { steps };
}

// --- Typed table handles ------------------------------------------------------------------------

type UniqueKeyValue<TTable extends AnyTable> = {
  [K in keyof TTable["columns"]]: TTable["columns"][K]["isUnique"] extends true
    ? ColumnValue<TTable["columns"][K]>
    : never;
}[keyof TTable["columns"]];

type ColumnArrays<TShape> = { [K in keyof TShape]: ReadonlyArray<TShape[K]> };

interface TypedTableDatabase {
  insertBatch(tableName: string, rows: readonly BatchRow[]): Promise<unknown>;
  upsertBatch(tableName: string, rows: readonly BatchRow[]): Promise<unknown>;
  updateBatch(
    tableName: string,
    input: {
      keys: readonly unknown[];
      changes: Readonly<Record<string, readonly unknown[]>>;
    },
  ): Promise<unknown>;
  deleteBatch(tableName: string, input: { keys: readonly unknown[] }): Promise<unknown>;
  readTable(
    tableName: string,
    options?: { columns?: readonly string[] },
  ): Promise<Array<Record<string, unknown>>>;
}

/**
 * A thin, fully typed handle over the existing batch APIs: inserts require every non-nullable
 * column, updates exclude the unique key, and reads return the complete inferred row shape.
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
  // Pads every schema column, so omitting a nullable one is an explicit null rather than a
  // "Missing column" error from the engine.
  const pad = (rows: ReadonlyArray<Record<string, unknown>>): BatchRow[] =>
    rows.map(
      (row) => Object.fromEntries(columnNames.map((name) => [name, row[name] ?? null])) as BatchRow,
    );
  return {
    definition,
    insert: (rows) => database.insertBatch(definition.name, pad(rows)),
    upsert: (rows) => database.upsertBatch(definition.name, pad(rows)),
    update: (input) =>
      database.updateBatch(definition.name, {
        keys: input.keys,
        changes: input.changes as Readonly<Record<string, readonly unknown[]>>,
      }),
    delete: (input) => database.deleteBatch(definition.name, { keys: input.keys }),
    rows: async () => (await database.readTable(definition.name)) as Array<InferRow<TTable>>,
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
          nullable: true,
        });
      }
      continue;
    }
    if (step.kind === "widen-nullable") {
      const target = columns.find(({ name }) => name === step.columnName);
      if (target !== undefined) target.nullable = true;
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
