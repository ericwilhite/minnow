import type { Catalog, ColumnDefinition, QueryResult, TableDefinition } from "@minnowdb/core";
import type { ColumnType } from "../sql/literal.js";

export interface ColumnInfo {
  name: string;
  type: ColumnType;
  /**
   * The SQL spelling of the type when it says more than the physical one — `NUMERIC(10,2)`,
   * `INTEGER`, `JSONB`, `TEXT[]` — so a panel reads the way the DDL was written.
   */
  typeLabel?: string;
  nullable: boolean;
  isUniqueKey: boolean;
  /**
   * The engine fills this column when an insert omits it — an auto-incrementing key, a uuid, a
   * timestamp. A form must let it be left blank rather than demanding a value the database is
   * about to choose. Optional so a hand-built `TableInfo` need not spell out its absence.
   */
  hasDefault?: boolean;
  /** The closed set of values an enum column accepts; the editors offer a menu of them. */
  enumValues?: readonly string[];
  /** The stored expression the engine maintains this column from; it is never written directly. */
  generated?: string;
}

export interface ForeignKeyInfo {
  name: string;
  columns: string[];
  parentTable: string;
  parentColumns: string[];
  onDelete: "restrict" | "cascade" | "set null";
  enforced: boolean;
}

export interface CheckInfo {
  name: string;
  sql: string;
}

export interface TriggerInfo {
  name: string;
  event: "insert" | "update" | "delete";
  timing: "before" | "after";
}

export interface TableInfo {
  name: string;
  columns: ColumnInfo[];
  /** SQL-declared indexes, when the target exposes rich catalog introspection. */
  indexes?: IndexInfo[];
  /** The single non-nullable unique-key column, when the table has one. */
  uniqueKey?: string;
  /** Present for a view: the query it stands for. A view is read-only in every editor. */
  view?: { sql: string };
  /** Declared PRIMARY KEY columns, composite keys included, when introspection reports them. */
  primaryKey?: string[];
  foreignKeys?: ForeignKeyInfo[];
  checks?: CheckInfo[];
  triggers?: TriggerInfo[];
}

export interface IndexInfo {
  name: string;
  columns: Array<{ name: string; direction: "asc" | "desc" }>;
  unique: boolean;
  state: "building" | "ready" | "invalid";
}

/** `NUMERIC(10,2)`, `INTEGER`, `JSONB`: the declared type, when it is more than the physical one. */
export function typeLabelOf(column: ColumnDefinition): string | undefined {
  const domain = column.sqlDomain;
  if (domain === undefined) return column.integer === true ? "INTEGER" : undefined;
  return domainLabel(domain);
}

/** A column's declared SQL domain, as the catalog and query results both carry it. */
export type SqlDomain = NonNullable<QueryResult["columnDomains"][number]>;

/** The SQL spelling of a logical domain. */
export function domainLabel(domain: SqlDomain): string {
  switch (domain.kind) {
    case "numeric": {
      if (domain.precision === undefined) return "NUMERIC";
      const scale = domain.scale === undefined ? "" : `,${String(domain.scale)}`;
      return `NUMERIC(${String(domain.precision)}${scale})`;
    }
    case "array":
      return `${domain.element.toUpperCase()}[]`;
    case "enum":
      return domain.name;
    default:
      return domain.kind.toUpperCase();
  }
}

/** The closed value set, whether declared on the column or through a `CREATE TYPE … AS ENUM`. */
function enumValuesOf(column: ColumnDefinition): readonly string[] | undefined {
  if (column.enumValues !== undefined) return column.enumValues;
  return column.sqlDomain?.kind === "enum" ? column.sqlDomain.values : undefined;
}

function toColumn(column: ColumnDefinition, uniqueKey: string | undefined): ColumnInfo {
  const label = typeLabelOf(column);
  const enumValues = enumValuesOf(column);
  return {
    name: column.name,
    type: column.type,
    ...(label === undefined ? {} : { typeLabel: label }),
    nullable: column.nullable ?? false,
    isUniqueKey: column.name === uniqueKey,
    hasDefault: column.defaultValue !== undefined,
    ...(enumValues === undefined ? {} : { enumValues: [...enumValues] }),
    ...(column.generatedValue === undefined ? {} : { generated: column.generatedValue.sql }),
  };
}

/**
 * Normalizes the catalog into what the explorer needs, in the order the engine reports it.
 *
 * `listTables()` is the base: it is what every target has. Introspection, where the target offers
 * it, adds what a browser wants to show beside the columns — indexes, keys, foreign keys, checks,
 * triggers — and says which names are views, which `listTables()` reports as tables. A view the
 * table list does not mention at all is appended from the introspection alone.
 */
export function toCatalog(
  tables: readonly TableDefinition[],
  introspection?: Catalog,
): TableInfo[] {
  const metadata = new Map(introspection?.tables.map((table) => [table.name, table]));
  const views = new Map(introspection?.views.map((view) => [view.name, view]));

  const listed = tables.map((table): TableInfo => {
    const view = views.get(table.name);
    const rich = metadata.get(table.name);
    const columnName = (id: string): string | undefined =>
      rich?.columns.find((column) => column.id === id)?.name;
    const primaryKey = rich?.primaryKeyColumnIds
      ?.map(columnName)
      .filter((name): name is string => name !== undefined);
    return {
      name: table.name,
      columns: table.columns.map((column) => toColumn(column, table.uniqueKey)),
      ...(rich?.indexes === undefined
        ? {}
        : {
            indexes: rich.indexes.map((index) => ({
              name: index.name,
              columns: index.columns.map((column) => ({ ...column })),
              unique: index.unique,
              state: index.state,
            })),
          }),
      ...(table.uniqueKey === undefined ? {} : { uniqueKey: table.uniqueKey }),
      ...(view === undefined ? {} : { view: { sql: view.sql } }),
      ...(primaryKey === undefined || primaryKey.length === 0 ? {} : { primaryKey }),
      ...(rich === undefined
        ? {}
        : {
            foreignKeys: rich.foreignKeys.map((key) => ({
              name: key.name,
              columns: [...key.columns],
              parentTable: key.parentTable,
              parentColumns: [...key.parentColumns],
              onDelete: key.onDelete,
              enforced: key.enforced,
            })),
            checks: rich.checks.map((check) => ({ name: check.name, sql: check.sql })),
            triggers: rich.triggers.map((trigger) => ({
              name: trigger.name,
              event: trigger.event,
              timing: trigger.timing,
            })),
          }),
    };
  });

  const known = new Set(listed.map((table) => table.name));
  const unlisted = [...views.values()]
    .filter((view) => !known.has(view.name))
    .map((view): TableInfo => ({
      name: view.name,
      columns: view.columns.map((column) => toColumn(column, undefined)),
      view: { sql: view.sql },
    }));
  return [...listed, ...unlisted];
}

export function findTable(catalog: readonly TableInfo[], name: string): TableInfo | undefined {
  return catalog.find((table) => table.name === name);
}

export function findColumn(table: TableInfo, name: string): ColumnInfo | undefined {
  return table.columns.find((column) => column.name === name);
}

/**
 * Which table the explorer shows after the catalog changes: the one that was open if it is still
 * there, else the first real table, else the first view, else nothing. Decided here so the rail's
 * highlight and the grid agree.
 */
export function tableToReopen(
  catalog: readonly TableInfo[],
  open: string | undefined,
): string | undefined {
  if (open !== undefined && findTable(catalog, open) !== undefined) return open;
  return (catalog.find((table) => table.view === undefined) ?? catalog[0])?.name;
}

/**
 * Whether rows in this table can be changed one at a time. The engine keys updates and deletes by
 * the unique key and refuses them outright without one, so a keyless table is browsable but not
 * editable; a view is never editable, since its rows are a query's output.
 */
export function isEditable(table: TableInfo): boolean {
  return table.view === undefined && table.uniqueKey !== undefined;
}

/** Whether rows can be added: any real table, keyed or not. */
export function isInsertable(table: TableInfo): boolean {
  return table.view === undefined;
}

/** The foreign key whose child side is this column, when the table has one. */
export function foreignKeyFor(table: TableInfo, column: string): ForeignKeyInfo | undefined {
  return table.foreignKeys?.find((key) => key.columns.length === 1 && key.columns[0] === column);
}
