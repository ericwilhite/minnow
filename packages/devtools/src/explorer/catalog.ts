import type { TableDefinition } from "@minnowdb/core";
import type { ColumnType } from "../sql/literal.js";

export interface ColumnInfo {
  name: string;
  type: ColumnType;
  nullable: boolean;
  isUniqueKey: boolean;
}

export interface TableInfo {
  name: string;
  columns: ColumnInfo[];
  /** The single non-nullable unique-key column, when the table has one. */
  uniqueKey?: string;
}

/** Normalizes the catalog into what the explorer needs, in the order the engine reports it. */
export function toCatalog(tables: readonly TableDefinition[]): TableInfo[] {
  return tables.map((table) => ({
    name: table.name,
    columns: table.columns.map((column) => ({
      name: column.name,
      type: column.type,
      nullable: column.nullable ?? false,
      isUniqueKey: column.name === table.uniqueKey,
    })),
    ...(table.uniqueKey === undefined ? {} : { uniqueKey: table.uniqueKey }),
  }));
}

export function findTable(catalog: readonly TableInfo[], name: string): TableInfo | undefined {
  return catalog.find((table) => table.name === name);
}

export function findColumn(table: TableInfo, name: string): ColumnInfo | undefined {
  return table.columns.find((column) => column.name === name);
}

/**
 * Whether rows in this table can be changed one at a time. The engine keys updates and deletes by
 * the unique key and refuses them outright without one, so a keyless table is browsable but not
 * editable. Phase three's grid reads this; the rail shows it now so it is never a surprise.
 */
export function isEditable(table: TableInfo): boolean {
  return table.uniqueKey !== undefined;
}
