/**
 * DDL statements to access/ `CatalogChange`s (lower/lowerer.ts `LoweredDdl`). Column types come
 * from declared names (lower/columns.ts, C5, R17.4); defaults, CHECKs and generated columns are
 * stored as the front end's SQL text and type-checked here, against the table being defined, so
 * a bad default fails at CREATE time as in PostgreSQL. IF [NOT] EXISTS is decided against the
 * catalog version being lowered against: a no-op is `change: null`.
 */
import type {
  CatalogChange,
  CatalogView,
  ColumnDef,
  TableDef,
  TableInput,
} from "../access/catalog.js";
import type { CompiledStatement, ForeignKeyDefinition } from "../../engine/query.js";
import {
  compileCheckExpression,
  compileWithDeclaredTypes,
  expressionColumnNames,
} from "../../engine/query.js";
import type { ColumnDefault } from "../../storage/types.js";
import type { PhysicalType, SqlTypeDescriptor } from "../types.js";
import type { FlatVector } from "../vector/vector.js";
import { coerce, ParameterTypes } from "./coerce.js";
import { declaredColumnType, type FrontEndColumn } from "./columns.js";
import { PlanningError, unsupported } from "./errors.js";
import { lowerExpression, type ExpressionState } from "./expressions.js";
import { lowerTableRules } from "./rules.js";

type CreateTable = Extract<CompiledStatement, { kind: "create-table" }>;

/** A CREATE TABLE as SQL compiles it, or as the schema DSL declares it (`CreateTableInput`). */
export interface TableDefinition {
  readonly name: string;
  readonly columns: readonly FrontEndColumn[];
  readonly checks?: ReadonlyArray<{ readonly name: string; readonly sql: string }>;
  readonly foreignKeys?: readonly ForeignKeyDefinition[];
  readonly uniqueKey?: string;
  readonly compositePrimaryKey?: readonly string[];
  readonly uniqueConstraints?: ReadonlyArray<{
    readonly name: string;
    readonly columns: readonly string[];
  }>;
}

function sqlConstant(value: boolean | number | string | Date): string {
  if (typeof value === "boolean") return value ? "TRUE" : "FALSE";
  if (typeof value === "number")
    return Number.isFinite(value) ? String(value) : `'${String(value)}'`;
  const text = value instanceof Date ? value.toISOString() : value;
  return `'${text.replaceAll("'", "''")}'`;
}

function defaultSqlOf(value: ColumnDefault | undefined, column: string): string | null {
  if (value === undefined) return null;
  if (value.kind === "autoincrement")
    throw unsupported(
      "identityColumn",
      `column ${column} (SERIAL / IDENTITY needs owned sequences, R17.7)`,
    );
  return value.kind === "literal" ? sqlConstant(value.value) : value.sql;
}

/**
 * The declared descriptor of a schema-DSL column, which names no SQL type: `column.integer()` is
 * INTEGER and `column.number()` DOUBLE PRECISION (R17.3).
 */
function withSchemaWidths(column: FrontEndColumn): FrontEndColumn {
  if (
    column.type !== "number" ||
    column.sqlDomain !== undefined ||
    column.declaredTypeName !== undefined
  )
    return column;
  return { ...column, declaredTypeName: column.integer === true ? "integer" : "double precision" };
}

function positionsOf(
  names: readonly string[],
  columns: ReadonlyArray<{ readonly name: string }>,
  what: string,
): number[] {
  return names.map((name) => {
    const position = columns.findIndex((column) => column.name === name);
    if (position === -1)
      throw new PlanningError(`column "${name}" named in ${what} does not exist`, "42703");
    return position;
  });
}

function checkColumns(
  sql: string,
  name: string,
  columns: ReadonlyArray<{ readonly name: string }>,
): number[] {
  const expression = compileWithDeclaredTypes(() => compileCheckExpression(sql, name));
  const names = [
    ...new Set(
      expressionColumnNames(expression).map(
        (reference) => reference.split(".").at(-1) ?? reference,
      ),
    ),
  ];
  return positionsOf(names, columns, `CHECK ${name}`);
}

function foreignKeyInput(
  key: ForeignKeyDefinition,
  table: string,
  columns: ReadonlyArray<{ readonly name: string }>,
  catalog: CatalogView,
): TableInput["foreignKeys"][number] {
  if (key.parentTable === table)
    throw unsupported("selfReference", `foreign key ${key.name} references its own table (R17.7)`);
  const parent = catalog.tableByName(key.parentTable);
  if (parent === undefined)
    throw new PlanningError(`relation "${key.parentTable}" does not exist`, "42P01");
  const childNames = key.columns ?? [key.column];
  const parentIds =
    key.parentColumns !== undefined || key.parentColumn !== undefined
      ? positionsOf(
          key.parentColumns ?? [key.parentColumn ?? ""],
          parent.columns,
          `foreign key ${key.name}`,
        ).map((position) => parent.columns[position]?.id ?? -1)
      : (parent.indexes.find((index) => index.id === parent.primaryKey)?.columnIds ?? []);
  const parentIndex = parent.indexes.find(
    (index) =>
      index.unique &&
      index.columnIds.length === parentIds.length &&
      index.columnIds.every((id, position) => id === parentIds[position]),
  );
  if (parentIndex === undefined)
    throw new PlanningError(
      `there is no unique constraint matching given keys for referenced table "${parent.name}"`,
      "42830",
    );
  return {
    name: key.name,
    parentTableId: parent.id,
    parentIndexId: parentIndex.id,
    onDelete: key.onDelete === "set null" ? "setNull" : key.onDelete,
    enforced: key.enforced !== false,
    columns: positionsOf(childNames, columns, `foreign key ${key.name}`),
  };
}

/** A compiled CREATE TABLE as a table definition. */
function tableDefinition(statement: CreateTable): TableDefinition {
  return {
    name: statement.table,
    columns: statement.columns,
    ...(statement.checks === undefined ? {} : { checks: statement.checks }),
    ...(statement.foreignKeys === undefined ? {} : { foreignKeys: statement.foreignKeys }),
    ...(statement.uniqueKey === undefined ? {} : { uniqueKey: statement.uniqueKey }),
    ...(statement.compositePrimaryKey === undefined
      ? {}
      : { compositePrimaryKey: statement.compositePrimaryKey }),
    ...(statement.uniqueConstraints === undefined
      ? {}
      : { uniqueConstraints: statement.uniqueConstraints }),
  };
}

/** A table definition as the next catalog's `TableInput`. */
export function tableInput(
  definition: TableDefinition,
  catalog: CatalogView,
  source: "sql" | "schema",
): TableInput {
  const columns = definition.columns.map((raw) => {
    const front = source === "schema" ? withSchemaWidths(raw) : raw;
    const { type, declared } = declaredColumnType(front, `${definition.name}.${front.name}`);
    return {
      name: front.name,
      type,
      declared,
      nullable: front.nullable === true,
      defaultSql: defaultSqlOf(front.defaultValue, front.name),
      generatedSql: front.generatedValue?.sql ?? null,
      identitySequence: null,
      missing: null,
    };
  });
  if (new Set(columns.map((column) => column.name)).size !== columns.length)
    throw new PlanningError(
      `column specified more than once in table "${definition.name}"`,
      "42701",
    );
  const primary =
    definition.compositePrimaryKey ??
    (definition.uniqueKey === undefined ? null : [definition.uniqueKey]);
  const primaryKey = primary === null ? null : positionsOf(primary, columns, "PRIMARY KEY");
  for (const position of primaryKey ?? []) {
    const column = columns[position];
    if (column !== undefined) column.nullable = false;
  }
  const input: TableInput = {
    name: definition.name,
    columns,
    primaryKey,
    unique: (definition.uniqueConstraints ?? []).map((constraint) => ({
      name: constraint.name,
      columns: positionsOf(constraint.columns, columns, `UNIQUE ${constraint.name}`),
    })),
    foreignKeys: (definition.foreignKeys ?? []).map((key) =>
      foreignKeyInput(key, definition.name, columns, catalog),
    ),
    checks: (definition.checks ?? []).map((check) => ({
      name: check.name,
      conditionSql: check.sql,
      columns: checkColumns(check.sql, check.name, columns),
    })),
  };
  validateRules(input, catalog);
  return input;
}

/**
 * Type-checks the defaults, CHECKs and generated columns of a table about to exist, by lowering
 * its rules against a provisional definition (ids are positions).
 */
function validateRules(input: TableInput, catalog: CatalogView): void {
  const provisional: TableDef = {
    id: -1,
    name: input.name,
    schemaVersion: 0,
    columns: input.columns.map((column, position) => ({ ...column, id: column.id ?? position })),
    primaryKey: null,
    indexes: [],
    foreignKeys: [],
    checks: input.checks.map((check, position) => ({
      id: check.id ?? position,
      name: check.name,
      conditionSql: check.conditionSql,
      columnIds: check.columns,
    })),
  };
  for (const column of provisional.columns)
    if (column.generatedSql !== null && column.defaultSql !== null)
      throw new PlanningError(
        `both default and generation expression specified for column "${column.name}"`,
        "42601",
      );
  lowerTableRules(provisional, catalog);
}

/** The table's current definition as a `TableInput` (every entry keeps its id). */
function currentInput(table: TableDef): {
  columns: Array<TableInput["columns"][number]>;
  input: Omit<TableInput, "columns">;
} {
  const position = (id: number) => table.columns.findIndex((column) => column.id === id);
  const primary = table.indexes.find((index) => index.id === table.primaryKey);
  return {
    columns: table.columns.map((column) => ({ ...column })),
    input: {
      name: table.name,
      primaryKey: primary === undefined ? null : primary.columnIds.map(position),
      unique: table.indexes
        .filter((index) => index.role === "unique")
        .map((index) => ({
          id: index.id,
          name: index.name,
          columns: index.columnIds.map(position),
        })),
      foreignKeys: table.foreignKeys.map((key) => ({
        id: key.id,
        name: key.name,
        parentTableId: key.parentTableId,
        parentIndexId: key.parentIndexId,
        onDelete: key.onDelete,
        enforced: key.enforced,
        columns: key.columnIds.map(position),
      })),
      checks: table.checks.map((check) => ({
        id: check.id,
        name: check.name,
        conditionSql: check.conditionSql,
        columns: check.columnIds.map(position),
      })),
    },
  };
}

function rulesState(catalog: CatalogView): ExpressionState {
  return {
    parameters: new ParameterTypes(0),
    catalog,
    clock: () => 0,
    volatile: false,
    sequences: false,
  };
}

/** The `missing` value of an added column: its constant default, when lowering knows it exactly. */
function missingValue(
  column: FrontEndColumn,
  type: PhysicalType,
  catalog: CatalogView,
): FlatVector | null {
  if (column.backfill === undefined) return null;
  const state = rulesState(catalog);
  const typed = coerce(
    lowerExpression(
      { kind: "literal", value: column.backfill },
      { column: () => undefined },
      state,
    ),
    type,
    "assignment",
    state.parameters,
  );
  if (typed.expr.op !== "literal")
    throw unsupported(
      "alterTable",
      `ADD COLUMN ${column.name} with a constant default of type ${type.kind}`,
    );
  return typed.expr.value;
}

export interface DdlResult {
  readonly change: CatalogChange | null;
  readonly followUp: string | null;
}

const NO_OP: DdlResult = { change: null, followUp: null };

function existingTable(catalog: CatalogView, name: string): TableDef {
  const table = catalog.tableByName(name);
  if (table === undefined) throw new PlanningError(`relation "${name}" does not exist`, "42P01");
  return table;
}

function relationExists(catalog: CatalogView, name: string): boolean {
  return (
    catalog.tableByName(name) !== undefined ||
    catalog.views.some((view) => view.name === name) ||
    catalog.sequences.some((sequence) => sequence.name === name) ||
    catalog.tables.some((table) => table.indexes.some((index) => index.name === name))
  );
}

/** One DDL statement (anything `classifyStatement` calls `ddl` except CREATE TABLE AS). */
export function lowerDdl(statement: CompiledStatement, catalog: CatalogView): DdlResult {
  switch (statement.kind) {
    case "create-table": {
      if (relationExists(catalog, statement.table)) {
        if (statement.ifNotExists === true && catalog.tableByName(statement.table) !== undefined)
          return NO_OP;
        throw new PlanningError(`relation "${statement.table}" already exists`, "42P07");
      }
      return {
        change: {
          kind: "createTable",
          table: tableInput(tableDefinition(statement), catalog, "sql"),
        },
        followUp: null,
      };
    }
    case "add-column": {
      const table = existingTable(catalog, statement.table);
      const front = statement.column;
      if (table.columns.some((column) => column.name === front.name)) {
        if (statement.ifNotExists === true) return NO_OP;
        throw new PlanningError(
          `column "${front.name}" of relation "${table.name}" already exists`,
          "42701",
        );
      }
      if (front.generatedValue !== undefined)
        throw new PlanningError(
          "ALTER TABLE cannot add a generated column without rewriting existing rows",
          "0A000",
        );
      const { type, declared } = declaredColumnType(front, `${table.name}.${front.name}`);
      const { columns, input } = currentInput(table);
      const added = {
        name: front.name,
        type,
        declared,
        nullable: front.nullable === true,
        defaultSql: defaultSqlOf(front.defaultValue, front.name),
        generatedSql: null,
        identitySequence: null,
        missing: missingValue(front, type, catalog),
      };
      const next: TableInput = { ...input, columns: [...columns, added] };
      validateRules(next, catalog);
      return { change: { kind: "alterTable", tableId: table.id, table: next }, followUp: null };
    }
    case "rename-column": {
      const table = existingTable(catalog, statement.table);
      const { columns, input } = currentInput(table);
      if (!columns.some((column) => column.name === statement.from))
        throw new PlanningError(`column "${statement.from}" does not exist`, "42703");
      if (columns.some((column) => column.name === statement.to))
        throw new PlanningError(
          `column "${statement.to}" of relation "${table.name}" already exists`,
          "42701",
        );
      const renamed = columns.map((column) =>
        column.name === statement.from ? { ...column, name: statement.to } : column,
      );
      return {
        change: { kind: "alterTable", tableId: table.id, table: { ...input, columns: renamed } },
        followUp: null,
      };
    }
    case "drop-column": {
      const table = existingTable(catalog, statement.table);
      const position = table.columns.findIndex((column) => column.name === statement.column);
      const def = table.columns[position];
      if (def === undefined) {
        if (statement.ifExists === true) return NO_OP;
        throw new PlanningError(
          `column "${statement.column}" of relation "${table.name}" does not exist`,
          "42703",
        );
      }
      if (dependsOn(table, def))
        throw new PlanningError(
          `cannot drop column ${def.name} of table ${table.name} because other objects depend on it`,
          "2BP01",
        );
      const { columns, input } = currentInput(table);
      const shift = (positions: readonly number[]) =>
        positions.map((p) => (p > position ? p - 1 : p));
      return {
        change: {
          kind: "alterTable",
          tableId: table.id,
          table: {
            ...input,
            columns: columns.filter((_, index) => index !== position),
            primaryKey: input.primaryKey === null ? null : shift(input.primaryKey),
            unique: input.unique.map((entry) => ({ ...entry, columns: shift(entry.columns) })),
            foreignKeys: input.foreignKeys.map((entry) => ({
              ...entry,
              columns: shift(entry.columns),
            })),
            checks: input.checks.map((entry) => ({ ...entry, columns: shift(entry.columns) })),
          },
        },
        followUp: null,
      };
    }
    case "create-index": {
      if (relationExists(catalog, statement.index)) {
        if (statement.ifNotExists === true) return NO_OP;
        throw new PlanningError(`relation "${statement.index}" already exists`, "42P07");
      }
      const table = existingTable(catalog, statement.table);
      const columnIds = positionsOf(
        statement.columns.map((column) => column.name),
        table.columns,
        `index ${statement.index}`,
      ).map((position) => table.columns[position]?.id ?? -1);
      return {
        change: {
          kind: "createIndex",
          index: {
            name: statement.index,
            tableId: table.id,
            columnIds,
            unique: statement.unique === true,
            role: statement.unique === true ? "unique" : "secondary",
          },
        },
        followUp: null,
      };
    }
    case "drop-index": {
      const index = catalog.tables
        .flatMap((table) => table.indexes)
        .find((entry) => entry.name === statement.index);
      if (index === undefined) {
        if (statement.ifExists === true) return NO_OP;
        throw new PlanningError(`index "${statement.index}" does not exist`, "42704");
      }
      return { change: { kind: "dropIndex", indexId: index.id }, followUp: null };
    }
    case "drop-table": {
      const table = catalog.tableByName(statement.table);
      if (table === undefined) {
        if (statement.ifExists === true) return NO_OP;
        throw new PlanningError(`table "${statement.table}" does not exist`, "42P01");
      }
      return { change: { kind: "dropTable", tableId: table.id }, followUp: null };
    }
    case "create-sequence": {
      if (relationExists(catalog, statement.name))
        throw new PlanningError(`relation "${statement.name}" already exists`, "42P07");
      return {
        change: {
          kind: "createSequence",
          sequence: {
            name: statement.name,
            increment: 1n,
            min: 1n,
            max: 2n ** 63n - 1n,
            start: 1n,
            cycle: false,
            ownedBy: null,
          },
        },
        followUp: null,
      };
    }
    case "create-view": {
      if (relationExists(catalog, statement.view)) {
        if (
          statement.orReplace === true &&
          catalog.views.some((view) => view.name === statement.view)
        )
          throw unsupported("replaceView", `CREATE OR REPLACE VIEW ${statement.view}`);
        throw new PlanningError(`relation "${statement.view}" already exists`, "42P07");
      }
      return {
        change: { kind: "createView", view: { name: statement.view, bodySql: statement.sql } },
        followUp: null,
      };
    }
    case "drop-view": {
      const view = catalog.views.find((entry) => entry.name === statement.view);
      if (view === undefined) {
        if (statement.ifExists === true) return NO_OP;
        throw new PlanningError(`view "${statement.view}" does not exist`, "42P01");
      }
      return { change: { kind: "dropView", viewId: view.id }, followUp: null };
    }
    case "create-enum":
      throw unsupported("enumType", `CREATE TYPE ${statement.name} AS ENUM (R17.7)`);
    case "create-trigger":
    case "drop-trigger":
      throw unsupported("trigger", "triggers");
    default:
      throw new PlanningError(`${statement.kind} is not DDL`, "XX000");
  }
}

function dependsOn(table: TableDef, def: ColumnDef): boolean {
  return (
    table.indexes.some((index) => index.columnIds.includes(def.id)) ||
    table.foreignKeys.some((key) => key.columnIds.includes(def.id)) ||
    table.checks.some((check) => check.columnIds.includes(def.id))
  );
}

/** The declared descriptor of a column created from a query's output type (CREATE TABLE AS). */
export function descriptorOf(type: PhysicalType): SqlTypeDescriptor {
  switch (type.kind) {
    case "bool":
      return { type: "boolean" };
    case "int32":
      return {
        type: "number",
        integer: true,
        declaredTypeName: type.bits === 16 ? "smallint" : "integer",
      };
    case "int64":
      return { type: "number", integer: true, declaredTypeName: "bigint" };
    case "float32":
      return { type: "number", declaredTypeName: "real" };
    case "float64":
      return { type: "number", declaredTypeName: "double precision" };
    case "decimal":
      return {
        type: "string",
        sqlDomain: {
          kind: "numeric",
          ...(type.precision === null ? {} : { precision: type.precision }),
          ...(type.scale === null ? {} : { scale: type.scale }),
        },
      };
    case "timestamp":
      return type.zoned ? { type: "datetime" } : { type: "datetime", withoutTimeZone: true };
    case "text":
      return type.maxLength === undefined
        ? { type: "string" }
        : {
            type: "string",
            declaredLength: { length: type.maxLength, fixed: type.fixed === true },
          };
    case "date":
    case "time":
    case "interval":
    case "uuid":
      return { type: "string", sqlDomain: { kind: type.kind } };
    case "json":
      return { type: "string", sqlDomain: { kind: type.binary ? "jsonb" : "json" } };
    case "enum":
      return {
        type: "string",
        sqlDomain: { kind: "enum", name: type.name, values: [...type.labels] },
      };
    default:
      throw unsupported("columnType", `CREATE TABLE AS with a ${type.kind} column`);
  }
}
