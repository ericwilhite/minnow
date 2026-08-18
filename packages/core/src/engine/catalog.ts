import type { ColumnDefault, SimpleDataType, TableRecord } from "../storage/index.js";

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
  readonly nullable: boolean;
  /** Filled at insert time for null-or-absent slots; never applied at read time. */
  readonly defaultValue?: ColumnDefault;
  /** String columns only: the closed set of values writes may draw from. */
  readonly enumValues?: readonly string[];
  /** Derived from the default spec, because a planner should not have to decode one. */
  readonly isAutoIncrementing: boolean;
}

export interface CatalogForeignKey {
  readonly name: string;
  readonly column: string;
  readonly parentTable: string;
  readonly parentColumn: string;
  readonly onDelete: "restrict" | "cascade" | "set null";
}

export interface CatalogCheck {
  readonly name: string;
  /** The boolean expression's text, as declared. */
  readonly sql: string;
}

export interface CatalogTrigger {
  readonly name: string;
  readonly event: "insert" | "update" | "delete";
  readonly timing: "before" | "after";
}

export interface CatalogTable {
  readonly name: string;
  readonly columns: readonly CatalogColumn[];
  /** The unique key's column ID, absent on a table declared without one. */
  readonly uniqueKeyColumnId?: string;
  readonly foreignKeys: readonly CatalogForeignKey[];
  readonly checks: readonly CatalogCheck[];
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
    nullable: column.nullable,
    ...(column.defaultValue === undefined ? {} : { defaultValue: column.defaultValue }),
    ...(column.enumValues === undefined ? {} : { enumValues: [...column.enumValues] }),
    isAutoIncrementing: column.defaultValue?.kind === "autoincrement",
  };
}

/** Projects storage's table records into the published catalog, sorted by name for stable diffs. */
export function toCatalog(records: readonly TableRecord[]): Catalog {
  const byName = [...records].sort((left, right) => left.name.localeCompare(right.name));
  const tables: CatalogTable[] = [];
  const views: CatalogView[] = [];
  for (const record of byName) {
    const columns = record.columns.map(toCatalogColumn);
    if (record.view !== undefined) {
      views.push({ name: record.name, sql: record.view.sql, columns });
      continue;
    }
    tables.push({
      name: record.name,
      columns,
      ...(record.uniqueKeyColumnId === undefined
        ? {}
        : { uniqueKeyColumnId: record.uniqueKeyColumnId }),
      foreignKeys: (record.foreignKeys ?? []).map((key) => ({ ...key })),
      checks: (record.checks ?? []).map((check) => ({ ...check })),
      triggers: (record.triggers ?? []).map(({ name, event, timing }) => ({
        name,
        event,
        timing,
      })),
    });
  }
  return { tables, views };
}
