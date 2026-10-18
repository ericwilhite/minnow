/**
 * The everyday in-thread API. Larger optional surfaces have their own entry points so exporting
 * one helper cannot make an application download the worker client or typed live-query layer.
 *
 * Each name is exported from the module that defines it, never re-exported through
 * `database.js`: a bundler resolves an import through the module that re-exports it and keeps
 * that module's top-level statements, so an error class taken through the engine module would
 * carry part of the engine (and the SQL function tables it reads) onto a page that only declares
 * a schema and catches errors.
 */
export * from "./catalog.js";
export * from "./database.js";
export * from "./errors.js";
export {
  attachLifecycleFlush,
  BufferedTableWriter,
  MAX_BUFFERED_WRITER_PENDING_ADDS,
  type BufferedFlushResult,
  type BufferedWriterOptions,
  type LifecycleDocumentTarget,
  type LifecycleFlushOptions,
  type LifecycleFlushRequester,
  type LifecyclePageTarget,
} from "./buffered-writer.js";
export type {
  WriteAdmissionStall,
  WriteCoordinationScope,
  WriterKind,
} from "./write-coordinator.js";
export { forgetStoreChoice } from "./auto-store.js";
export { QueryMemoryBudgetError, type QueryMemoryUsage } from "./memory.js";
export {
  MAX_SQL_NESTING_DEPTH,
  MAX_SQL_NUMERIC_DIGITS,
  MAX_SQL_PARAMETERS,
  MAX_SQL_PATTERN_CHARACTERS,
  MAX_SQL_PATTERN_MATCH_STEPS,
  MAX_SQL_SCALAR_RESULT_CHARACTERS,
  MAX_SQL_STRUCTURED_VALUE_DEPTH,
  MAX_SQL_STRUCTURED_VALUE_ITEMS,
  MAX_SQL_TEXT_CHARACTERS,
  MAX_SQL_TOKENS,
} from "./cache-limits.js";
/**
 * The core schema declarations stay on the everyday entry. The optional typed-table renderer is
 * available from `@minnowdb/core/schema`, so applications that execute SQL directly do not ship
 * its statement builder. Advanced schema internals remain available from that same subpath.
 */
export {
  column,
  foreignKeyName,
  isDestructiveStep,
  planMigration,
  schema,
  table,
  view,
} from "./schema.js";
export type * from "./schema.js";
export type {
  InsertValue,
  CompiledQuery,
  CompiledStatement,
  QueryResult,
  QueryRow,
  QueryExecutionOptions,
  QueryParameter,
  QueryValue,
  SqlColumnSchema,
  SqlColumnType,
} from "./query.js";
export type {
  AsyncQueryExecutionOptions,
  QueryBatchExecutionOptions,
  QuerySpillStore,
} from "./vector.js";
export type { MinnowSqlDriver, MinnowSqlExecutor } from "./sql-driver.js";

export {
  CompactionBacklogError,
  OpfsCoordinationError,
  OpfsDatabaseInUseError,
  OpfsUncertainOutcomeError,
  StorageUnresponsiveError,
  TableInUseError,
} from "../storage/types.js";
/** `JSON.parse`, guarded against V8 renaming escaped object keys; see json-parse.ts. */
export { parseJson } from "../block-format/json-parse.js";
