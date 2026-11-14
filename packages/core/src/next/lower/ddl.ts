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
  InputExpressionDependencies,
  ColumnDef,
  TableDef,
  TableInput,
} from "../access/catalog.js";
import type { CompiledStatement, ForeignKeyDefinition } from "../../engine/query.js";
import { compileCheckExpression, compileWithDeclaredTypes } from "../../engine/query.js";
import type { ColumnDefault } from "../../storage/types.js";
import {
  ENUM_LABEL_LIMIT,
  ENUM_LABEL_BYTES,
  type PhysicalType,
  type SqlTypeDescriptor,
} from "../types.js";
import { alwaysIdentity } from "../../engine/catalog-ddl-sources.js";
import type { ScalarExpr } from "../ir/ir.js";
import type { NativeStatement } from "./statements.js";
import { builtInCatalogType } from "./catalog-type-names.js";
import { utf8Length } from "../vector/varlen.js";
import { integerLiteralValue } from "./literals.js";
import type { FlatVector } from "../vector/vector.js";
import { coerce, ParameterTypes } from "./coerce.js";
import { declaredColumnType, type FrontEndColumn } from "./columns.js";
import { PlanningError, unsupported } from "./errors.js";
import { lowerExpression, type ExpressionState } from "./expressions.js";
import {
  generatedValue,
  lowerTableRules,
  ruleReferences,
  ruleDependencies,
  tableRowScope,
} from "./rules.js";
import { tableUsesObject } from "../access/catalog-dependencies.js";

type CreateTable = Extract<CompiledStatement, { kind: "create-table" }>;

/** A CREATE TABLE as SQL compiles it, or as the schema DSL declares it (`CreateTableInput`). */
export interface TableDefinition {
  readonly name: string;
  readonly managed?: boolean;
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

export function defaultSqlOf(value: ColumnDefault | undefined, _column: string): string | null {
  void _column;
  if (value === undefined) return null;
  if (value.kind === "autoincrement") return null; // The owned sequence is declared separately.
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
  const names = [...new Set(ruleReferences(expression).map((reference) => reference.name))];
  return positionsOf(names, columns, `CHECK ${name}`);
}

function foreignKeyInput(
  key: ForeignKeyDefinition,
  definition: TableDefinition,
  columns: ReadonlyArray<{ readonly name: string }>,
  catalog: CatalogView,
): TableInput["foreignKeys"][number] {
  if (key.parentTable === definition.name) {
    const primary =
      definition.compositePrimaryKey ??
      (definition.uniqueKey === undefined ? null : [definition.uniqueKey]);
    const names =
      key.parentColumns ?? (key.parentColumn === undefined ? primary : [key.parentColumn]);
    const same = (columns: readonly string[] | null) =>
      names !== null &&
      columns !== null &&
      names.length === columns.length &&
      names.every((name, i) => name === columns[i]);
    const unique = definition.uniqueConstraints?.find((key) => same(key.columns));
    if (names === null || (!same(primary) && unique === undefined))
      throw new PlanningError(
        `there is no unique constraint matching given keys for referenced table "${definition.name}"`,
        "42830",
      );
    if (names.length !== (key.columns ?? [key.column]).length)
      throw new PlanningError(
        "number of referencing and referenced columns for foreign key disagree",
        "42830",
      );
    return {
      name: key.name,
      parentTableId: "self",
      parentIndexId: same(primary) ? "primary" : { unique: unique?.name ?? "" },
      onDelete: key.onDelete === "set null" ? "setNull" : key.onDelete,
      enforced: key.enforced !== false,
      columns: positionsOf(key.columns ?? [key.column], columns, `foreign key ${key.name}`),
    };
  }
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
    ...(statement.managed === undefined ? {} : { managed: statement.managed }),
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
    const { type, declared } = declaredColumnType(
      front,
      `${definition.name}.${front.name}`,
      catalog,
    );
    if (
      source === "sql" &&
      front.defaultValue?.kind === "autoincrement" &&
      type.kind !== "int32" &&
      type.kind !== "int64"
    )
      throw new PlanningError("identity column type must be smallint, integer, or bigint", "22023");
    const namedType = type.kind === "enum" ? catalog.typeByName(type.name) : undefined;
    return {
      name: front.name,
      type,
      declared,
      nullable: front.nullable === true,
      defaultSql: defaultSqlOf(front.defaultValue, front.name),
      generatedSql: front.generatedValue?.sql ?? null,
      identitySequence: null,
      missing: null,
      ...(front.defaultValue?.kind === "autoincrement"
        ? { identity: { start: 1n, increment: 1n } }
        : {}),
      ...(source === "schema" && front.defaultValue?.kind === "autoincrement"
        ? { autoIncrement: true as const }
        : {}),
      ...(alwaysIdentity(front.defaultValue) ? { identityAlways: true as const } : {}),
      ...(namedType === undefined ? {} : { typeId: namedType.id }),
      ...(front.enumValues === undefined ? {} : { allowedValues: front.enumValues }),
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
    ...(definition.managed === undefined ? {} : { managed: definition.managed }),
    columns,
    primaryKey,
    unique: (definition.uniqueConstraints ?? []).map((constraint) => ({
      name: constraint.name,
      columns: positionsOf(constraint.columns, columns, `UNIQUE ${constraint.name}`),
    })),
    foreignKeys: (definition.foreignKeys ?? []).map((key) =>
      foreignKeyInput(key, definition, columns, catalog),
    ),
    checks: (definition.checks ?? []).map((check) => ({
      name: check.name,
      conditionSql: check.sql,
      columns: checkColumns(check.sql, check.name, columns),
    })),
  };
  return validateRules(input, catalog);
}

/**
 * Type-checks the defaults, CHECKs and generated columns of a table about to exist, by lowering
 * its rules against a provisional definition (ids are positions).
 */
export function validateRules(input: TableInput, catalog: CatalogView): TableInput {
  const provisional: TableDef = {
    id: -1,
    name: input.name,
    schemaVersion: 0,
    columns: input.columns.map(
      ({ defaultDependencies: _d, generatedDependencies: _g, ...column }, position) => {
        void _d;
        void _g;
        return {
          ...column,
          // Provisional identity is positional for EVERY column; mixing stable IDs with
          // positions lets a newly added column collide with an existing column's ID.
          id: position,
        };
      },
    ),
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
  const loweredRules = lowerTableRules(provisional, catalog);
  // PostgreSQL: a generation expression must be immutable (42P17; review finding 6): no volatile
  // function, sequence or statement clock.
  const scope = tableRowScope(provisional);
  for (const column of provisional.columns) {
    if (column.generatedSql === null) continue;
    const reads = { clock: false };
    const state: ExpressionState = {
      parameters: new ParameterTypes(0),
      catalog,
      clock: () => {
        reads.clock = true;
        return 0;
      },
      volatile: false,
      sequences: false,
    };
    generatedValue(column, scope, state);
    if (state.volatile || state.stable || state.sequences || reads.clock)
      throw new PlanningError(
        `generation expression for column "${column.name}" is not immutable`,
        "42P17",
      );
  }
  const dependencies = (expr: ScalarExpr, sql?: string): InputExpressionDependencies => {
    const columns = new Set<number>(),
      sequences = new Set<number>();
    const visit = (value: unknown): void => {
      if (typeof value !== "object" || value === null) return;
      const node = value as ScalarExpr;
      if (node.op === "literal") return;
      if (node.op === "column") {
        columns.add(node.index);
        return;
      }
      if (node.op === "call" && ["nextval", "currval", "setval"].includes(node.fn)) {
        const id = node.args[0] && integerLiteralValue(node.args[0]);
        if (id !== null && id !== undefined) sequences.add(Number(id));
      }
      for (const [key, child] of Object.entries(value)) if (key !== "type") visit(child);
    };
    visit(expr);
    const source = sql === undefined ? undefined : ruleDependencies(sql, catalog);
    for (const reference of source?.references ?? []) {
      const position = provisional.columns.findIndex((column) => column.name === reference.name);
      if (position < 0)
        throw new PlanningError(`column "${reference.name}" does not exist`, "42703");
      columns.add(position);
    }
    for (const id of source?.sequenceIds ?? []) sequences.add(id);
    return {
      dialectVersion: 1,
      columns: [...columns].sort((a, b) => a - b),
      sequenceIds: [...sequences].sort((a, b) => a - b),
      ...(source?.typeIds.length ? { typeIds: source.typeIds } : {}),
    };
  };
  return {
    ...input,
    columns: input.columns.map((column, i) => {
      const id = provisional.columns[i]?.id;
      const generated = loweredRules.generated.find((rule) => rule.columnId === id);
      const value = loweredRules.defaults?.find((rule) => rule.columnId === id);
      const { defaultDependencies: _default, generatedDependencies: _generated, ...rest } = column;
      void _default;
      void _generated;
      return {
        ...rest,
        ...(generated === undefined
          ? {}
          : {
              generatedDependencies: dependencies(generated.expr, column.generatedSql ?? undefined),
            }),
        ...(value === undefined
          ? {}
          : { defaultDependencies: dependencies(value.expr, column.defaultSql ?? undefined) }),
      };
    }),
    checks: input.checks.map((check, i) => {
      const rule = loweredRules.checks[i];
      return rule === undefined
        ? check
        : { ...check, dependencies: dependencies(rule.condition, check.conditionSql) };
    }),
  };
}

/** The table's current definition as a `TableInput` (every entry keeps its id). */
export function currentInput(
  table: TableDef,
  kept = table.columns,
): {
  columns: Array<TableInput["columns"][number]>;
  input: Omit<TableInput, "columns">;
} {
  const position = (id: number) => {
    const index = kept.findIndex((column) => column.id === id);
    if (index < 0) throw new PlanningError("cannot remove a referenced column", "2BP01");
    return index;
  };
  const primary = table.indexes.find((index) => index.id === table.primaryKey);
  const inputDependencies = (
    deps: import("../access/catalog.js").ExpressionDependencies,
  ): InputExpressionDependencies => ({
    dialectVersion: deps.dialectVersion,
    columns: deps.columnIds.map(position),
    sequenceIds: deps.sequenceIds,
    ...(deps.typeIds === undefined ? {} : { typeIds: deps.typeIds }),
  });
  return {
    columns: kept.map((column) => {
      const { defaultDependencies, generatedDependencies, ...rest } = column;
      return {
        ...rest,
        ...(defaultDependencies === undefined
          ? {}
          : { defaultDependencies: inputDependencies(defaultDependencies) }),
        ...(generatedDependencies === undefined
          ? {}
          : { generatedDependencies: inputDependencies(generatedDependencies) }),
      };
    }),
    input: {
      name: table.name,
      ...(table.managed === undefined ? {} : { managed: table.managed }),
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
        ...(check.dependencies === undefined
          ? {}
          : { dependencies: inputDependencies(check.dependencies) }),
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
export function missingValue(
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
export function lowerDdl(statement: NativeStatement, catalog: CatalogView): DdlResult {
  switch (statement.kind) {
    case "drop-sequence": {
      const sequence = catalog.sequences.find((sequence) => sequence.name === statement.name);
      if (!sequence) {
        if (statement.ifExists) return NO_OP;
        throw new PlanningError(`sequence "${statement.name}" does not exist`, "42P01");
      }
      if (
        sequence.ownedBy !== null ||
        catalog.tables.some((table) => tableUsesObject(table, "sequence", sequence.id)) ||
        catalog.views.some((view) =>
          view.dependencies?.some((dep) => dep.kind === "sequence" && dep.objectId === sequence.id),
        )
      )
        throw new PlanningError(
          `cannot drop sequence ${sequence.name} because other objects depend on it`,
          "2BP01",
        );
      return { change: { kind: "dropSequence", sequenceId: sequence.id }, followUp: null };
    }
    case "drop-type": {
      const type = catalog.typeByName(statement.name);
      if (!type) {
        if (statement.ifExists) return NO_OP;
        throw new PlanningError(`type "${statement.name}" does not exist`, "42704");
      }
      if (
        catalog.tables.some((table) => tableUsesObject(table, "type", type.id, type.name)) ||
        catalog.views.some((view) =>
          view.dependencies?.some((dep) => dep.kind === "type" && dep.objectId === type.id),
        )
      )
        throw new PlanningError(
          `cannot drop type ${type.name} because other objects depend on it`,
          "2BP01",
        );
      return { change: { kind: "dropType", typeId: type.id }, followUp: null };
    }
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
      const { type, declared } = declaredColumnType(front, `${table.name}.${front.name}`, catalog);
      const { columns, input } = currentInput(table);
      const namedType = type.kind === "enum" ? catalog.typeByName(type.name) : undefined;
      const added = {
        name: front.name,
        type,
        declared,
        nullable: front.nullable === true,
        defaultSql: defaultSqlOf(front.defaultValue, front.name),
        generatedSql: null,
        identitySequence: null,
        missing: missingValue(front, type, catalog),
        ...(namedType === undefined ? {} : { typeId: namedType.id }),
      };
      const next: TableInput = { ...input, columns: [...columns, added] };
      return {
        change: { kind: "alterTable", tableId: table.id, table: validateRules(next, catalog) },
        followUp: null,
      };
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
      if (dependsOn(table, def, catalog))
        throw new PlanningError(
          `cannot drop column ${def.name} of table ${table.name} because other objects depend on it`,
          "2BP01",
        );
      // Resolve every constraint and nested rule reference from its stable ID against
      // the FINAL live column order. Never shift one layer while reusing old positions.
      const { columns, input } = currentInput(
        table,
        table.columns.filter((c) => c.id !== def.id),
      );
      return {
        change: { kind: "alterTable", tableId: table.id, table: { ...input, columns } },
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
            directions: statement.columns.map((column) => column.direction),
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
      if (
        catalog.views.some((view) =>
          view.dependencies?.some((dep) => dep.kind === "table" && dep.objectId === table.id),
        )
      )
        throw new PlanningError(
          `cannot drop table ${table.name} because other objects depend on it`,
          "2BP01",
        );
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
      if (
        catalog.views.some((other) =>
          other.dependencies?.some((dep) => dep.kind === "view" && dep.objectId === view.id),
        )
      )
        throw new PlanningError(
          `cannot drop view ${view.name} because other objects depend on it`,
          "2BP01",
        );
      return { change: { kind: "dropView", viewId: view.id }, followUp: null };
    }
    case "create-enum":
      if (statement.values.length > ENUM_LABEL_LIMIT)
        throw unsupported("enumType", `an enum with more than ${String(ENUM_LABEL_LIMIT)} labels`);
      if (builtInCatalogType(statement.name))
        throw unsupported(
          "enumType",
          "a named enum shadowing a built-in catalog type requires SQL namespaces",
        );
      if (catalog.typeByName(statement.name))
        throw new PlanningError(`type "${statement.name}" already exists`, "42710");
      if (new Set(statement.values).size !== statement.values.length)
        throw new PlanningError("duplicate enum label", "42710");
      if (statement.values.some((value) => utf8Length(value) > ENUM_LABEL_BYTES))
        throw new PlanningError("enum label exceeds 63 UTF8 bytes", "42602");
      return {
        change: { kind: "createType", type: { name: statement.name, labels: statement.values } },
        followUp: null,
      };
    case "create-trigger":
    case "drop-trigger":
      throw unsupported("trigger", "triggers");
    default:
      throw new PlanningError(`${statement.kind} is not DDL`, "XX000");
  }
}

function dependsOn(table: TableDef, def: ColumnDef, catalog: CatalogView): boolean {
  return (
    table.indexes.some((index) => index.columnIds.includes(def.id)) ||
    table.foreignKeys.some((key) => key.columnIds.includes(def.id)) ||
    table.checks.some((check) => check.columnIds.includes(def.id)) ||
    table.columns.some(
      (column) =>
        (column.defaultDependencies?.columnIds.includes(def.id) ?? false) ||
        (column.generatedDependencies?.columnIds.includes(def.id) ?? false),
    ) ||
    catalog.fts.some((defn) => defn.tableId === table.id && defn.columnId === def.id) ||
    catalog.views.some((view) =>
      view.dependencies?.some(
        (dep) =>
          dep.kind === "table" && dep.objectId === table.id && dep.columnIds.includes(def.id),
      ),
    )
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
