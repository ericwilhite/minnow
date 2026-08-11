import { type TableColumnRecord, type TableRecord } from "@browserdatabase/storage-idb";

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

export interface ColumnBuilder<TValue, TNullable extends boolean, TUnique extends boolean = false> {
  readonly kind: "column";
  readonly type: SchemaColumnType;
  readonly isNullable: TNullable;
  readonly isUnique: TUnique;
  readonly renamedFromName?: string;
  readonly reference?: { table: string; column: string };
  /** Marks the column nullable; inserts may omit it and reads may return null. */
  nullable(): ColumnBuilder<TValue, true, TUnique>;
  /** Marks the table's unique key; exactly one non-nullable column may carry it. */
  unique(): ColumnBuilder<TValue, TNullable, true>;
  /** Declares this column as the rename target of an existing catalog column. */
  renamedFrom(name: string): ColumnBuilder<TValue, TNullable, TUnique>;
  /** Declares a relation for catalog metadata and validation; not enforced at write time. */
  references(table: string, column: string): ColumnBuilder<TValue, TNullable, TUnique>;
}

type AnyColumn = ColumnBuilder<boolean | number | string | Date, boolean, boolean>;

function createColumn<TType extends SchemaColumnType>(
  type: TType,
  state: Partial<Pick<AnyColumn, "isNullable" | "isUnique" | "renamedFromName" | "reference">> = {},
): ColumnBuilder<ValueOf<TType>, false> {
  const base = {
    kind: "column" as const,
    type,
    isNullable: (state.isNullable ?? false) as false,
    isUnique: (state.isUnique ?? false) as false,
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
  };
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
    readonly vendor: "browserdatabase";
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
  return {
    kind: "table",
    name,
    columns,
    "~standard": {
      version: 1,
      vendor: "browserdatabase",
      validate(value) {
        if (typeof value !== "object" || value === null) {
          return { issues: [{ message: "A row must be an object", path: [] }] };
        }
        const row = value as Record<string, unknown>;
        const issues: Array<{ message: string; path: Array<string | number> }> = [];
        for (const [columnName, definition] of entries) {
          const columnValue = row[columnName];
          if (columnValue === undefined || columnValue === null) {
            if (!definition.isNullable) {
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
  TColumn extends ColumnBuilder<infer TValue, infer TNullable, boolean>
    ? TNullable extends true
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

/** Insert rows require every non-nullable column and may omit nullable ones. */
export type InferInsertRow<TTable extends AnyTable> = Omit<
  InferRow<TTable>,
  NullableKeys<TTable>
> & {
  [K in NullableKeys<TTable>]?: ColumnValue<TTable["columns"][K]>;
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
  | { kind: "widen-nullable"; tableName: string; columnName: string };

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
  insertBatch(
    tableName: string,
    input: { columns: Readonly<Record<string, readonly unknown[]>> },
  ): Promise<unknown>;
  upsertBatch(
    tableName: string,
    input: { columns: Readonly<Record<string, readonly unknown[]>> },
  ): Promise<unknown>;
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
  const pivot = (rows: ReadonlyArray<Record<string, unknown>>) =>
    Object.fromEntries(columnNames.map((name) => [name, rows.map((row) => row[name] ?? null)]));
  return {
    definition,
    insert: (rows) => database.insertBatch(definition.name, { columns: pivot(rows) }),
    upsert: (rows) => database.upsertBatch(definition.name, { columns: pivot(rows) }),
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
    }
  }
  return columns;
}
