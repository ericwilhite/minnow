import type { Catalog } from "./catalog.js";
import type { ExecuteResult, QueryCursorOptions, QueryOptions } from "./database.js";
import type { QueryResult, QueryValue } from "./query.js";

/**
 * The stable SQL boundary used by clients and third-party adapters.
 *
 * It is structural: both `MinnowDatabase` and `MinnowDatabaseClient` implement it without a
 * wrapper. Adapters should emit PostgreSQL-style SQL and keep engine-specific APIs outside
 * this small contract.
 */
export interface MinnowSqlExecutor {
  query(sql: string, options?: QueryOptions): Promise<QueryResult>;
  queryCursor(sql: string, options?: QueryCursorOptions): AsyncIterableIterator<QueryResult>;
  execute(sql: string, params?: readonly QueryValue[]): Promise<ExecuteResult>;
}

/** SQL execution plus the catalog surface schema-aware adapters need. */
export interface MinnowSqlDriver extends MinnowSqlExecutor {
  introspect(): Promise<Catalog>;
}
