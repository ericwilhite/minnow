/** Shipped schema planner steps, translated to the same checked native changes as SQL DDL. */
import { declaredForeignKeys, type AnyTable, type MigrationStep } from "../../engine/schema.js";
type AnyColumn = AnyTable["columns"][string];
import { defaultSourceOf } from "../access/catalog-default-source.js";
import type { ColumnDefinition } from "../../engine/catalog-mutations.js";
import type { CatalogChange, CatalogView, TableInput } from "../access/catalog.js";
import type { Lowerer, LoweringContext } from "./lowerer.js";
import {
  backfillMetadata,
  currentInput,
  defaultSqlOf,
  missingValue,
  tableInput,
  validateRules,
} from "./ddl.js";
import { declaredColumnType } from "./columns.js";
import { PlanningError, unsupported } from "./errors.js";
import { samePhysicalType } from "../types.js";

function front(name: string, column: AnyColumn): ColumnDefinition {
  return {
    name,
    type: column.type,
    nullable: column.isNullable,
    ...(column.integer ? { integer: true } : {}),
    ...(column.sqlDomain === undefined ? {} : { sqlDomain: column.sqlDomain }),
    ...(column.withoutTimeZone === undefined ? {} : { withoutTimeZone: true }),
    ...(column.defaultSpec === undefined ? {} : { defaultValue: column.defaultSpec }),
    ...(column.generatedSpec === undefined ? {} : { generatedValue: column.generatedSpec }),
    ...(column.enumValues === undefined ? {} : { enumValues: column.enumValues }),
  };
}
function create(def: AnyTable, catalog: CatalogView): CatalogChange {
  const unique = Object.entries(def.columns).find(([, column]) => column.isUnique)?.[0];
  return {
    kind: "createTable",
    table: tableInput(
      {
        name: def.name,
        managed: true,
        columns: Object.entries(def.columns).map(([name, column]) => {
          const backfill = column.backfillValue;
          return {
            ...front(name, column),
            ...(backfill === undefined
              ? {}
              : { backfill: typeof backfill === "function" ? backfill() : backfill }),
          };
        }),
        checks: def.checks,
        ...(def.primaryKey.length
          ? { compositePrimaryKey: def.primaryKey }
          : unique === undefined
            ? {}
            : { uniqueKey: unique }),
        foreignKeys: declaredForeignKeys(def).map((key) => ({
          name: key.name,
          columns: key.columns,
          column: key.columns[0] ?? "",
          parentTable: key.parentTable,
          parentColumns: key.parentColumns,
          onDelete: key.onDelete,
          enforced: key.enforced,
        })),
      },
      catalog,
      "schema",
    ),
  };
}
export function lowerMigrationStep(
  step: MigrationStep,
  catalog: CatalogView,
  lowerer: Pick<Lowerer, "lower">,
  context: Omit<LoweringContext, "catalog">,
  identityStart?: bigint,
): CatalogChange | null {
  if (step.kind === "create-table") return create(step.table, catalog);
  if (step.kind === "replace-view") {
    const lowered = lowerer.lower(
      `CREATE OR REPLACE VIEW "${step.view.name.replaceAll('"', '""')}" AS ${step.view.sql}`,
      { ...context, catalog },
    );
    if (
      lowered.kind !== "ddl" ||
      !lowered.change ||
      (lowered.change.kind !== "createView" && lowered.change.kind !== "replaceView")
    )
      throw new PlanningError("Missing view migration change", "XX000");
    const change = lowered.change;
    const expected = tableInput(
      {
        name: step.view.name,
        columns: Object.entries(step.view.columns).map(([name, column]) => front(name, column)),
      },
      catalog,
      "schema",
    ).columns;
    const actual = change.view.outputs ?? [];
    if (
      expected.length !== actual.length ||
      expected.some(
        (column, i) =>
          column.name !== actual.at(i)?.name ||
          !samePhysicalType(column.type, actual.at(i)?.type ?? column.type),
      )
    )
      throw new PlanningError(
        `View ${step.view.name} does not produce its declared columns`,
        "42804",
      );
    return change.kind === "createView"
      ? { ...change, view: { ...change.view, managed: true } }
      : { ...change, view: { ...change.view, managed: change.view.managed === true } };
  }
  if (step.kind === "drop-view") {
    const view = catalog.views.find((view) => view.name === step.viewName);
    return view ? { kind: "dropView", viewId: view.id } : null;
  }
  const table = catalog.tableByName(step.tableName);
  if (!table) throw new PlanningError(`relation "${step.tableName}" does not exist`, "42P01");
  if (step.kind === "drop-table") return { kind: "dropTable", tableId: table.id };
  const { input, columns } = currentInput(table);
  if (step.kind === "rename-column" || step.kind === "drop-column") {
    const quote = (name: string) => `"${name.replaceAll('"', '""')}"`;
    const sql =
      step.kind === "rename-column"
        ? `ALTER TABLE ${quote(table.name)} RENAME COLUMN ${quote(step.from)} TO ${quote(step.to)}`
        : `ALTER TABLE ${quote(table.name)} DROP COLUMN ${quote(step.columnName)}`;
    const lowered = lowerer.lower(sql, { ...context, catalog });
    if (lowered.kind !== "ddl") throw new PlanningError("Missing column migration change", "XX000");
    return lowered.change;
  }
  if (step.kind === "alter-foreign-keys") {
    const foreignKeys: Array<TableInput["foreignKeys"][number]> = [];
    for (const key of step.foreignKeys) {
      const previous = table.foreignKeys.find((old) => old.name === key.name);
      if (key.enforced && !previous)
        throw unsupported("alterTable", "adding an enforced foreign key during migration");
      const parent = catalog.tableByName(key.parentTable),
        parentIds = key.parentColumns.map(
          (name) => parent?.columns.find((column) => column.name === name)?.id,
        );
      const index = parent?.indexes.find(
        (index) =>
          index.unique &&
          index.columnIds.length === parentIds.length &&
          index.columnIds.every((id, i) => id === parentIds[i]),
      );
      if (!parent || !index)
        throw new PlanningError(`foreign key ${key.name} names no unique parent key`, "42830");
      foreignKeys.push({
        ...(previous ? { id: previous.id } : {}),
        name: key.name,
        columns: key.columns.map((name) => columns.findIndex((column) => column.name === name)),
        parentTableId: parent.id,
        parentIndexId: index.id,
        onDelete: key.onDelete === "set null" ? "setNull" : key.onDelete,
        enforced: key.enforced,
      });
    }
    return { kind: "alterTable", tableId: table.id, table: { ...input, columns, foreignKeys } };
  }
  const position = columns.findIndex((column) => column.name === step.columnName);
  if (step.kind === "add-column") {
    if (step.definition.generatedSpec)
      throw unsupported("alterTable", "adding a stored generated column requires a rewrite");
    const declaration = front(step.columnName, step.definition);
    const widths =
      declaration.type === "number" && !declaration.sqlDomain
        ? {
            ...declaration,
            declaredTypeName: declaration.integer
              ? ("integer" as const)
              : ("double precision" as const),
          }
        : declaration;
    const { type, declared } = declaredColumnType(
      widths,
      `${table.name}.${step.columnName}`,
      catalog,
    );
    const defaultSource = defaultSourceOf(step.definition.defaultSpec);
    columns.push({
      name: step.columnName,
      type,
      declared,
      nullable: step.definition.isNullable,
      defaultSql: defaultSqlOf(step.definition.defaultSpec, step.columnName),
      ...(defaultSource === undefined ? {} : { defaultSource }),
      generatedSql: null,
      identitySequence: null,
      missing:
        step.backfill === undefined
          ? null
          : missingValue({ ...widths, backfill: step.backfill }, type, catalog),
      ...backfillMetadata(step.backfill, type),
      ...(step.definition.enumValues ? { allowedValues: step.definition.enumValues } : {}),
    });
  } else {
    const column = columns[position];
    if (!column) throw new PlanningError(`column "${step.columnName}" does not exist`, "42703");
    switch (step.kind) {
      case "widen-nullable":
        columns[position] = { ...column, nullable: true };
        break;
      case "tighten-nullable":
        columns[position] = { ...column, nullable: false };
        break;
      case "widen-enum": {
        const { allowedValues: _allowed, declared, ...rest } = column;
        const { enumValues: _values, ...descriptor } = declared;
        void _allowed;
        void _values;
        columns[position] = {
          ...rest,
          declared: {
            ...descriptor,
            ...(step.enumValues === null ? {} : { enumValues: step.enumValues }),
          },
          ...(step.enumValues === null ? {} : { allowedValues: step.enumValues }),
        };
        break;
      }
      case "alter-default": {
        const { defaultSource: _source, ...rest } = column;
        void _source;
        const defaultSource = defaultSourceOf(step.defaultValue ?? undefined);
        columns[position] = {
          ...rest,
          defaultSql: defaultSqlOf(step.defaultValue ?? undefined, column.name),
          ...(defaultSource === undefined ? {} : { defaultSource }),
        };
        break;
      }
      case "set-auto-increment": {
        const { autoIncrement: _auto, defaultSource: _source, ...rest } = column;
        void _auto;
        void _source;
        columns[position] = {
          ...rest,
          identitySequence: step.enabled ? column.identitySequence : null,
          defaultSql: null,
          ...(step.enabled
            ? { autoIncrement: true, identity: { start: identityStart ?? 1n, increment: 1n } }
            : {}),
        };
        break;
      }
      case "alter-generated":
        if (step.generatedValue?.sql !== column.generatedSql)
          throw unsupported(
            "alterTable",
            "changing a stored generated expression requires a rewrite",
          );
        break;
    }
  }
  return {
    kind: "alterTable",
    tableId: table.id,
    table: validateRules({ ...input, columns }, catalog),
  };
}
