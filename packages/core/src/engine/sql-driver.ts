import type { Catalog } from "./catalog.js";
import type {
  ExecuteOptions,
  ExecuteResult,
  QueryCursorOptions,
  QueryOptions,
} from "./database.js";
import type { QueryResult, QueryValue } from "./query.js";

/**
 * The stable SQL boundary used by clients and third-party adapters.
 *
 * It is structural: both `MinnowDatabase` and `MinnowDatabaseClient` implement it without a
 * wrapper. Adapters should emit PostgreSQL-style SQL and keep engine-specific APIs outside
 * this small contract.
 *
 * `execute` takes the same engine controls a `query` does — `signal`, `onStats`, `memoize`,
 * `executionMemoryBudgetBytes` — as a trailing optional argument, so an adapter can cancel a
 * buffered statement or observe its cost without reaching around the boundary. A driver written
 * against the two-argument form remains a valid implementation; it simply ignores the controls.
 */
export interface MinnowSqlExecutor {
  query(sql: string, options?: QueryOptions): Promise<QueryResult>;
  queryCursor(sql: string, options?: QueryCursorOptions): AsyncIterableIterator<QueryResult>;
  execute(
    sql: string,
    params?: readonly QueryValue[],
    options?: ExecuteOptions,
  ): Promise<ExecuteResult>;
}

/** SQL execution plus the catalog surface schema-aware adapters need. */
export interface MinnowSqlDriver extends MinnowSqlExecutor {
  introspect(): Promise<Catalog>;
}
