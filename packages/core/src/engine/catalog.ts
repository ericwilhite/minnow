import {
  secondaryIndexColumnIds,
  secondaryIndexDirections,
  type ColumnDefault,
  type ColumnGenerated,
  type SecondaryIndexState,
  type SimpleDataType,
  type SqlDomain,
  type TableRecord,
} from "../storage/types.js";
import { externalSqlDomainValue } from "./sql-domains.js";

/**
 * The published shape of the catalog: everything a schema tool needs to diff a live database
 * against a desired schema, without reading engine internals.
 *
 * The distinction from `listTables()` is identity. `TableDefinition` answers "what can I select,
 * and of what type", which is enough to render a table and not enough to plan a migration: a
 * rename is only expressible because a column keeps a stable ID across it, and a constraint can
 * only be diffed if it is visible in the first place. This carries both, plus the derived facts
 * (auto-increment, view bodies) that a planner would otherwise have to reverse-engineer.
 */

export interface CatalogColumn {
  /** Stable across renames. Diffing a rename is not expressible without it. */
  readonly id: string;
  readonly name: string;
  readonly type: SimpleDataType;
  /** True for SQL INTEGER/SMALLINT/BIGINT columns, whose values must stay exactly representable. */
  readonly integer?: true;
  readonly sqlDomain?: SqlDomain;
  readonly nullable: boolean;
  /** Filled for omitted or SQL DEFAULT insert slots; never applied at read time. */
  readonly defaultValue?: ColumnDefault;
  /** Stored expression maintained by the engine; callers cannot assign this column. */
  readonly generatedValue?: ColumnGenerated;
  /** String columns only: the closed set of values writes may draw from. */
  readonly enumValues?: readonly string[];
  /** Derived from the default spec, because a planner should not have to decode one. */
  readonly isAutoIncrementing: boolean;
  /** What rows written before this column existed read as, instead of NULL. */
  readonly backfill?: boolean | number | string | Date;
}

export interface CatalogForeignKey {
  readonly name: string;
  readonly columns: readonly string[];
  readonly parentTable: string;
  readonly parentColumns: readonly string[];
  readonly onDelete: "restrict" | "cascade" | "set null";
  /** Whether writes and parent deletes enforce this relationship. */
  readonly enforced: boolean;
}

export interface CatalogCheck {
  readonly name: string;
  /** The boolean expression's text, as declared. */
  readonly sql: string;
}

export interface CatalogTrigger {
  /** Immutable durable identity; stable until this exact trigger is dropped. */
  readonly id: string;
  readonly name: string;
  readonly event: "insert" | "update" | "delete";
  readonly timing: "before" | "after";
}

/** One SQL-declared secondary index, in declared key order. */
export interface CatalogIndex {
  readonly name: string;
  readonly columns: ReadonlyArray<{
    readonly name: string;
    readonly direction: "asc" | "desc";
  }>;
  readonly unique: boolean;
  /** Build health is visible so inspection tools never present an invalid accelerator as ready. */
  readonly state: SecondaryIndexState;
}

export interface CatalogTable {
  readonly name: string;
  /**
   * True when a migration created this table. The schema is then authoritative over it and may
   * drop it; a table created with `CREATE TABLE` belongs to no schema and is never removed by
   * one. See `CatalogView.managed`, which follows the same rule.
   */
  readonly managed: boolean;
  readonly columns: readonly CatalogColumn[];
  /** The unique key's column ID, absent on a table declared without one. */
  readonly uniqueKeyColumnId?: string;
  /** Declared PRIMARY KEY columns, including composite keys, in comparison order. */
  readonly primaryKeyColumnIds?: readonly string[];
  readonly foreignKeys: readonly CatalogForeignKey[];
  readonly checks: readonly CatalogCheck[];
  /** SQL-declared secondary indexes. Absent in catalogs produced before index introspection. */
  readonly indexes?: readonly CatalogIndex[];
  /**
   * Reported but not declarable through the schema DSL: a trigger's body is a statement, not a
   * column, so it stays SQL-only. A tool that rewrites triggers still needs to see them.
   */
  readonly triggers: readonly CatalogTrigger[];
}

export interface CatalogView {
  readonly name: string;
  /** The query text the view stands for. */
  readonly sql: string;
  /** The query's inferred output schema, so a view answers the same questions a table does. */
  readonly columns: readonly CatalogColumn[];
  /**
   * True when a migration created this view from a schema declaration. The schema is then
   * authoritative over it and may drop it; a view created with `CREATE VIEW` — or one written
   * before this was recorded — is not owned by any schema and no migration removes it.
   */
  readonly managed: boolean;
}

export interface Catalog {
  readonly tables: readonly CatalogTable[];
  readonly views: readonly CatalogView[];
}

function toCatalogColumn(column: TableRecord["columns"][number]): CatalogColumn {
  return {
    id: column.id,
    name: column.name,
    type: column.type,
    ...(column.integer === true ? { integer: true } : {}),
    ...(column.sqlDomain === undefined ? {} : { sqlDomain: structuredClone(column.sqlDomain) }),
    nullable: column.nullable,
    ...(column.defaultValue === undefined ? {} : { defaultValue: column.defaultValue }),
    ...(column.generatedValue === undefined ? {} : { generatedValue: column.generatedValue }),
    ...(column.enumValues === undefined ? {} : { enumValues: [...column.enumValues] }),
    ...(column.backfill === undefined
      ? {}
      : { backfill: externalSqlDomainValue(column.backfill) as boolean | number | string | Date }),
    isAutoIncrementing: column.defaultValue?.kind === "autoincrement",
  };
}

/** Projects storage's table records into the published catalog, sorted by name for stable diffs. */
export function toCatalog(records: readonly TableRecord[]): Catalog {
  const byName = [...records].sort((left, right) => left.name.localeCompare(right.name));
  const tables: CatalogTable[] = [];
  const views: CatalogView[] = [];
  for (const record of byName) {
    if (record.enumType !== undefined || record.sequence !== undefined) continue;
    const columns = record.columns.filter((column) => !column.hidden).map(toCatalogColumn);
    if (record.view !== undefined) {
      views.push({
        name: record.name,
        sql: record.view.sql,
        columns,
        managed: record.view.managed,
      });
      continue;
    }
    tables.push({
      name: record.name,
      managed: record.managed,
      columns,
      ...(record.uniqueKeyColumnId === undefined ||
      record.columns.find((column) => column.id === record.uniqueKeyColumnId)?.hidden === true
        ? {}
        : { uniqueKeyColumnId: record.uniqueKeyColumnId }),
      ...(record.primaryKeyColumnIds === undefined
        ? {}
        : { primaryKeyColumnIds: [...record.primaryKeyColumnIds] }),
      foreignKeys: (record.foreignKeys ?? []).map((key) => ({
        ...key,
        enforced: key.enforced !== false,
      })),
      checks: (record.checks ?? []).map((check) => ({ ...check })),
      ...(record.secondaryIndexes === undefined
        ? {}
        : {
            indexes: Object.values(record.secondaryIndexes)
              .map((index) => ({
                name: index.name,
                columns: secondaryIndexColumnIds(index).map((columnId, position) => ({
                  name: record.columns.find((column) => column.id === columnId)?.name ?? columnId,
                  direction: secondaryIndexDirections(index)[position] ?? "asc",
                })),
                unique: index.unique === true,
                state: index.state,
              }))
              .sort((left, right) => left.name.localeCompare(right.name)),
          }),
      triggers: (record.triggers ?? []).map(({ id, name, event, timing }) => ({
        id,
        name,
        event,
        timing,
      })),
    });
  }
  return { tables, views };
}
