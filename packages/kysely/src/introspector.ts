import type { CatalogColumn, MinnowSqlDriver } from "@minnowdb/core";
import type {
  ColumnMetadata,
  DatabaseIntrospector,
  DatabaseMetadataOptions,
  SchemaMetadata,
  TableMetadata,
} from "kysely";
import { DEFAULT_MIGRATION_LOCK_TABLE, DEFAULT_MIGRATION_TABLE } from "kysely/migration";

function dataType(column: CatalogColumn): string {
  const domain = column.sqlDomain;
  if (domain?.kind === "numeric") {
    if (domain.precision === undefined) return "numeric";
    return `numeric(${String(domain.precision)},${String(domain.scale ?? 0)})`;
  }
  if (
    domain?.kind === "json" ||
    domain?.kind === "jsonb" ||
    domain?.kind === "uuid" ||
    domain?.kind === "time" ||
    domain?.kind === "interval"
  ) {
    return domain.kind;
  }
  if (domain?.kind === "array") return `${domain.element.toLowerCase()}[]`;
  if (domain?.kind === "enum") return domain.name;
  switch (column.type) {
    case "boolean":
      return "boolean";
    case "number":
      return column.integer === true ? "integer" : "double precision";
    case "string":
      return "text";
    case "datetime":
      return "timestamp";
  }
}

function columnMetadata(column: CatalogColumn): ColumnMetadata {
  return {
    name: column.name,
    dataType: dataType(column),
    isAutoIncrementing: column.isAutoIncrementing,
    isNullable: column.nullable,
    hasDefaultValue: column.defaultValue !== undefined,
  };
}

/** Catalog-backed introspection without pretending the embedded database has schemas. */
export class MinnowKyselyIntrospector implements DatabaseIntrospector {
  readonly #driver: MinnowSqlDriver;

  constructor(driver: MinnowSqlDriver) {
    this.#driver = driver;
  }

  async getSchemas(): Promise<SchemaMetadata[]> {
    return [];
  }

  async getTables(
    options: DatabaseMetadataOptions = { withInternalKyselyTables: false },
  ): Promise<TableMetadata[]> {
    const catalog = await this.#driver.introspect();
    const internal = new Set([DEFAULT_MIGRATION_TABLE, DEFAULT_MIGRATION_LOCK_TABLE]);
    const include = (name: string): boolean =>
      options.withInternalKyselyTables || !internal.has(name);
    return [
      ...catalog.tables
        .filter(({ name }) => include(name))
        .map((table) => ({
          name: table.name,
          isView: false,
          isForeign: false,
          columns: table.columns.map(columnMetadata),
        })),
      ...catalog.views
        .filter(({ name }) => include(name))
        .map((view) => ({
          name: view.name,
          isView: true,
          isForeign: false,
          columns: view.columns.map(columnMetadata),
        })),
    ].sort((left, right) => left.name.localeCompare(right.name));
  }
}
