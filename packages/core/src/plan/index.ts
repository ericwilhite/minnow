/**
 * The plan-construction primitives, published so a typed client can be built outside this package.
 *
 * A query builder's whole job is to assemble the same logical plan the SQL parser assembles, and
 * then hand it to the engine. That requires the plan's types, the block-assembly functions the
 * parser itself ends in, and the validators that keep a hand-built plan as strict as a parsed one.
 * Exposing them here is what lets `@minnowdb/client` — or anyone else's builder — produce plans the
 * engine treats as indistinguishable from parsed SQL.
 *
 * This module re-exports rather than owns: the plan types still live beside the parser that
 * produces them. Splitting them into a leaf module of their own is the next step, and it will not
 * change this entry point.
 */
export {
  assembleSelectBlock,
  compoundSelectBlock,
  derivedTableSource,
  hasAggregate,
  splitCondition,
  validateLimit,
  validateOffset,
  type AggregateName,
  type CompiledQuery,
  type CompiledStatement,
  type Expression,
  type JoinPlan,
  type Predicate,
  type PredicateOperator,
  type QueryResult,
  type QueryRow,
  type QueryValue,
  type SelectItem,
  type SetOperator,
  type TableSource,
  type WindowFunctionName,
} from "../engine/query.js";
export { optimizePlan, renderPlan } from "../engine/optimizer.js";
export { validateFtsQuery } from "../engine/fts.js";
