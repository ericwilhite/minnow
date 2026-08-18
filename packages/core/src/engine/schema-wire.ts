import type { ColumnDefault } from "../storage/index.js";
import {
  column,
  columnWithDefaultSpec,
  schema,
  table,
  view,
  type AnyTable,
  type ColumnBuilder,
  type MigrationStep,
  type AnyView,
  type ReferentialAction,
  type SchemaColumnType,
  type SchemaDefinition,
  type TableCheck,
} from "./schema.js";

/**
 * Structured-clone-safe mirror of the schema DSL. Column builders and table validators carry
 * methods, so a schema cannot cross a postMessage boundary as-is; these types keep only the data
 * fields `planMigration` reads, and the worker host rebuilds real builders from them.
 */

export interface WireColumn {
  readonly type: SchemaColumnType;
  readonly isNullable: boolean;
  readonly isUnique: boolean;
  readonly defaultSpec?: ColumnDefault;
  readonly renamedFromName?: string;
  readonly reference?: { table: string; column: string; onDelete: ReferentialAction };
  readonly enumValues?: readonly string[];
}

export interface WireTable {
  readonly name: string;
  readonly columns: Record<string, WireColumn>;
  /** Absent rather than empty so a frame written without the field stays readable. */
  readonly checks?: readonly TableCheck[];
}

export interface WireView {
  readonly name: string;
  readonly sql: string;
  readonly columns: Record<string, WireColumn>;
}

export interface WireSchema {
  readonly tables: readonly WireTable[];
  /** Absent rather than empty so a frame written without the field stays readable. */
  readonly views?: readonly WireView[];
}

export type WireMigrationStep =
  | { kind: "create-table"; table: WireTable }
  | { kind: "add-column"; tableName: string; columnName: string; definition: WireColumn }
  | { kind: "rename-column"; tableName: string; from: string; to: string }
  | { kind: "widen-nullable"; tableName: string; columnName: string }
  | { kind: "widen-enum"; tableName: string; columnName: string; enumValues: string[] | null }
  | {
      kind: "alter-default";
      tableName: string;
      columnName: string;
      defaultValue: ColumnDefault | null;
    }
  | { kind: "replace-view"; view: WireView }
  | { kind: "drop-view"; viewName: string };

type AnyColumn = ColumnBuilder<boolean | number | string | Date, boolean, boolean, boolean>;

function serializeColumn(definition: AnyColumn): WireColumn {
  return {
    type: definition.type,
    isNullable: definition.isNullable,
    isUnique: definition.isUnique,
    ...(definition.defaultSpec === undefined ? {} : { defaultSpec: { ...definition.defaultSpec } }),
    ...(definition.renamedFromName === undefined
      ? {}
      : { renamedFromName: definition.renamedFromName }),
    ...(definition.reference === undefined ? {} : { reference: { ...definition.reference } }),
    ...(definition.enumValues === undefined ? {} : { enumValues: [...definition.enumValues] }),
  };
}

function serializeTable(definition: AnyTable): WireTable {
  return {
    name: definition.name,
    columns: Object.fromEntries(
      Object.entries(definition.columns).map(([name, columnDefinition]) => [
        name,
        serializeColumn(columnDefinition),
      ]),
    ),
    ...(definition.checks.length === 0
      ? {}
      : { checks: definition.checks.map((check) => ({ ...check })) }),
  };
}

function serializeView(definition: AnyView): WireView {
  return {
    name: definition.name,
    sql: definition.sql,
    columns: Object.fromEntries(
      Object.entries(definition.columns).map(([name, columnDefinition]) => [
        name,
        serializeColumn(columnDefinition),
      ]),
    ),
  };
}

function deserializeView(wire: WireView): AnyView {
  return view(wire.name, {
    sql: wire.sql,
    columns: Object.fromEntries(
      Object.entries(wire.columns).map(([name, columnDefinition]) => [
        name,
        deserializeColumn(columnDefinition),
      ]),
    ),
  });
}

export function serializeSchema(definition: SchemaDefinition<readonly AnyTable[]>): WireSchema {
  return {
    tables: definition.tables.map(serializeTable),
    ...(definition.views.length === 0 ? {} : { views: definition.views.map(serializeView) }),
  };
}

export function serializeMigrationSteps(steps: readonly MigrationStep[]): WireMigrationStep[] {
  return steps.map((step) => {
    switch (step.kind) {
      case "create-table":
        return { kind: "create-table", table: serializeTable(step.table) };
      case "add-column":
        return {
          kind: "add-column",
          tableName: step.tableName,
          columnName: step.columnName,
          definition: serializeColumn(step.definition),
        };
      case "replace-view":
        return { kind: "replace-view", view: serializeView(step.view) };
      default:
        return step;
    }
  });
}

function deserializeColumn(wire: WireColumn): AnyColumn {
  if (wire.enumValues !== undefined && wire.type !== "string") {
    throw new TypeError(`Enum values require a string column, got ${wire.type}`);
  }
  let builder: AnyColumn =
    wire.enumValues === undefined
      ? column[wire.type]()
      : column.enum(wire.enumValues as readonly [string, ...string[]]);
  if (wire.isNullable) builder = builder.nullable();
  if (wire.isUnique) builder = builder.unique();
  if (wire.renamedFromName !== undefined) builder = builder.renamedFrom(wire.renamedFromName);
  if (wire.reference !== undefined) {
    builder = builder.references(wire.reference.table, wire.reference.column, {
      onDelete: wire.reference.onDelete,
    });
  }
  // Rebuilt from the spec directly rather than through .default(), which would re-interpret the
  // reserved generator names.
  if (wire.defaultSpec !== undefined) builder = columnWithDefaultSpec(builder, wire.defaultSpec);
  return builder;
}

function deserializeTable(wire: WireTable): AnyTable {
  return table(
    wire.name,
    Object.fromEntries(
      Object.entries(wire.columns).map(([name, columnDefinition]) => [
        name,
        deserializeColumn(columnDefinition),
      ]),
    ),
    wire.checks === undefined ? {} : { checks: wire.checks.map((check) => ({ ...check })) },
  );
}

export function deserializeSchema(wire: WireSchema): SchemaDefinition<readonly AnyTable[]> {
  return schema(wire.tables.map(deserializeTable), {
    views: (wire.views ?? []).map(deserializeView),
  });
}
