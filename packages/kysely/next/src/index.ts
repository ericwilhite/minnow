import { NativeKyselyAdapter as Adapter } from "./adapter.js";
/** Internal native dialect qualification. No package export points at this entry. */
import {
  type ColumnMetadata,
  type DatabaseIntrospector,
  type DatabaseMetadataOptions,
  type Dialect,
  type TableMetadata,
} from "kysely";
import { DEFAULT_MIGRATION_LOCK_TABLE, DEFAULT_MIGRATION_TABLE } from "kysely/migration";
import { MinnowQueryCompiler } from "../../dist/compiler.js";
import { NativeKyselyDriver, type NativeResultDecoding, type NativeSqlDriver } from "./driver.js";
export { NativeKyselyDriver } from "./driver.js";
export type { NativeSqlDriver, NativeResultDecoding } from "./driver.js";
export interface NativeDialectConfig {
  readonly driver: NativeSqlDriver;
  readonly resultDecoding?: NativeResultDecoding;
  /** The existing catalog projection; opaque until the public catalog DTO is reviewed. */
  readonly catalog?: () => Promise<unknown>;
}
function object(value: unknown): Record<string, unknown> {
  if (typeof value !== "object" || value === null || Array.isArray(value))
    throw new TypeError("Invalid native catalog projection");
  return value as Record<string, unknown>;
}
function array(value: unknown): unknown[] {
  if (!Array.isArray(value)) throw new TypeError("Invalid native catalog list");
  return value as unknown[];
}
function name(value: unknown): string {
  if (typeof value !== "string") throw new TypeError("Invalid native catalog name");
  return value;
}
class Introspector implements DatabaseIntrospector {
  constructor(readonly config: NativeDialectConfig) {}
  async getSchemas() {
    return [];
  }
  async getTables(
    options: DatabaseMetadataOptions = { withInternalKyselyTables: false },
  ): Promise<TableMetadata[]> {
    const catalog = object(await (this.config.catalog?.() ?? this.config.driver.catalog())),
      tables: TableMetadata[] = [];
    const hidden = new Set([DEFAULT_MIGRATION_TABLE, DEFAULT_MIGRATION_LOCK_TABLE]);
    for (const isView of [false, true])
      for (const raw of array(catalog[isView ? "views" : "tables"])) {
        const table = object(raw),
          tableName = name(table.name);
        if (!options.withInternalKyselyTables && hidden.has(tableName)) continue;
        const stored = array(table.columns).map(object);
        // SQL metadata supplies exact widths/domains; the 0.x introspection projection loses int8.
        const result = await this.config.driver.query(
          `SELECT * FROM "${tableName.replaceAll('"', '""')}" LIMIT 0`,
        );
        if (
          result.columns.length !== stored.length ||
          !result.columns.every((c, i) => c.name === stored[i]?.name)
        )
          throw new Error("Native catalog changed during introspection; retry");
        const columns: ColumnMetadata[] = result.columns.map((column, i) => ({
          name: column.name,
          dataType: column.sqlType,
          isNullable: column.nullable,
          isAutoIncrementing: stored[i]?.isAutoIncrementing === true,
          hasDefaultValue: stored[i]?.defaultValue !== undefined,
        }));
        tables.push({ name: tableName, isView, isForeign: false, columns });
      }
    return tables.sort((a, b) => a.name.localeCompare(b.name));
  }
}
export class NativeMinnowDialect implements Dialect {
  constructor(readonly config: NativeDialectConfig) {}
  createAdapter() {
    return new Adapter(this.config.resultDecoding);
  }
  createDriver() {
    return new NativeKyselyDriver(this.config.driver, this.config.resultDecoding);
  }
  createQueryCompiler() {
    return new MinnowQueryCompiler();
  }
  createIntrospector() {
    return new Introspector(this.config);
  }
}
export { nativeLiveQuery } from "./live.js";
