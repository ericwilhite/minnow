/** SQL compilation, standalone execution, and plan inspection for tooling and adapters. */
export {
  bindPlanParameters,
  bindStatementParameters,
  compileQuery,
  compileStatement,
  executeQuery,
  referencedColumns,
  type InsertValue,
  type CompiledQuery,
  type CompiledStatement,
  type QueryResult,
  type QueryRow,
  type QueryExecutionOptions,
  type QueryValue,
  type SqlColumnSchema,
  type SqlColumnType,
} from "./query.js";
export { optimizePlan, renderPlan } from "./optimizer.js";
