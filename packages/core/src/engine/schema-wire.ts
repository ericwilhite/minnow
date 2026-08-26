import type { ColumnDefault, SqlDomain } from "../storage/types.js";
import {
  columnFromState,
  schema,
  table,
  view,
  type AnyTable,
  type MigrationStep,
  type AnyView,
  type ReferentialAction,
  type SchemaColumnType,
  type SchemaDefinition,
  type TableCheck,
  type TableForeignKey,
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
  readonly integer?: true;
  readonly sqlDomain?: SqlDomain;
  readonly defaultSpec?: ColumnDefault;
  readonly renamedFromName?: string;
  readonly reference?: {
    table: string;
    column: string;
    onDelete: ReferentialAction;
    enforced?: boolean;
  };
  readonly enumValues?: readonly string[];
  /** Already frozen by planning, so the wire carries a value and never a generator. */
  readonly backfillValue?: boolean | number | string | Date;
}

export interface WireTable {
  readonly name: string;
  readonly columns: Record<string, WireColumn>;
  readonly primaryKey?: readonly string[];
  readonly foreignKeys?: readonly TableForeignKey[];
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
  | { kind: "tighten-nullable"; tableName: string; columnName: string }
  | { kind: "drop-column"; tableName: string; columnName: string }
  | { kind: "drop-table"; tableName: string }
  | { kind: "set-auto-increment"; tableName: string; columnName: string; enabled: boolean }
  | { kind: "widen-enum"; tableName: string; columnName: string; enumValues: string[] | null }
  | {
      kind: "alter-default";
      tableName: string;
      columnName: string;
      defaultValue: ColumnDefault | null;
    }
  | {
      kind: "alter-foreign-keys";
      tableName: string;
      foreignKeys: Extract<MigrationStep, { kind: "alter-foreign-keys" }>["foreignKeys"];
    }
  | { kind: "replace-view"; view: WireView }
  | { kind: "drop-view"; viewName: string };

type AnyColumn = AnyTable["columns"][string];

function serializeColumn(
  definition: AnyColumn,
  frozen?: { readonly backfillValue?: boolean | number | string | Date },
): WireColumn {
  const backfillValue =
    frozen === undefined
      ? typeof definition.backfillValue === "function"
        ? definition.backfillValue()
        : definition.backfillValue
      : frozen.backfillValue;
  return {
    type: definition.type,
    isNullable: definition.isNullable,
    isUnique: definition.isUnique,
    ...(definition.integer ? { integer: true } : {}),
    ...(definition.sqlDomain === undefined
      ? {}
      : { sqlDomain: structuredClone(definition.sqlDomain) }),
    ...(definition.defaultSpec === undefined ? {} : { defaultSpec: { ...definition.defaultSpec } }),
    ...(definition.renamedFromName === undefined
      ? {}
      : { renamedFromName: definition.renamedFromName }),
    ...(definition.reference === undefined ? {} : { reference: { ...definition.reference } }),
    ...(definition.enumValues === undefined ? {} : { enumValues: [...definition.enumValues] }),
    ...(backfillValue === undefined ? {} : { backfillValue }),
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
    ...(definition.primaryKey.length === 0 ? {} : { primaryKey: [...definition.primaryKey] }),
    ...(definition.foreignKeys.length === 0
      ? {}
      : { foreignKeys: structuredClone(definition.foreignKeys) }),
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
          definition: serializeColumn(
            step.definition,
            step.backfill === undefined ? {} : { backfillValue: step.backfill },
          ),
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
  // Rebuild in one step. Chaining dynamic nullable/unique builders is needlessly invariant and
  // used to discard a backfill when a default spec was applied last.
  return columnFromState({
    type: wire.type,
    isNullable: wire.isNullable,
    isUnique: wire.isUnique,
    integer: wire.integer === true,
    ...(wire.sqlDomain === undefined ? {} : { sqlDomain: structuredClone(wire.sqlDomain) }),
    ...(wire.defaultSpec === undefined ? {} : { defaultSpec: wire.defaultSpec }),
    ...(wire.renamedFromName === undefined ? {} : { renamedFromName: wire.renamedFromName }),
    ...(wire.reference === undefined
      ? {}
      : { reference: { ...wire.reference, enforced: wire.reference.enforced !== false } }),
    ...(wire.enumValues === undefined ? {} : { enumValues: wire.enumValues }),
    ...(wire.backfillValue === undefined ? {} : { backfillValue: wire.backfillValue }),
  });
}

function deserializeTable(wire: WireTable): AnyTable {
  const columns = Object.fromEntries(
    Object.entries(wire.columns).map(([name, columnDefinition]) => [
      name,
      deserializeColumn(columnDefinition),
    ]),
  );
  const options = {
    ...(wire.foreignKeys === undefined ? {} : { foreignKeys: structuredClone(wire.foreignKeys) }),
    ...(wire.checks === undefined ? {} : { checks: wire.checks.map((check) => ({ ...check })) }),
  };
  return wire.primaryKey === undefined
    ? table(wire.name, columns, options)
    : table(wire.name, columns, { ...options, primaryKey: [...wire.primaryKey] });
}

export function deserializeSchema(wire: WireSchema): SchemaDefinition<readonly AnyTable[]> {
  return schema(wire.tables.map(deserializeTable), {
    views: (wire.views ?? []).map(deserializeView),
  });
}
