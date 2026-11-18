/** Internal adapter boundary. Native facades satisfy it directly; no result conversion/dispatcher. */
import type { CallOptions, SqlExecutor, TransactionOptions } from "./api.js";

/** Canonical Results and cursors, including exact int8/NUMERIC/temporal values. */
export type MinnowSqlExecutor = Pick<SqlExecutor, "query" | "execute" | "cursor" | "prepare">;

/** SQL plus isolated callback sessions and the current opaque catalog projection. */
export interface MinnowSqlDriver extends MinnowSqlExecutor {
  transaction<T>(body: (tx: SqlExecutor) => Promise<T>, options?: TransactionOptions): Promise<T>;
  /** The public catalog DTO remains a separate cutover review; adapters validate what they read. */
  catalog(options?: CallOptions): Promise<unknown>;
}
