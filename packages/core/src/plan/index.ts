/**
 * The plan-construction primitives, published so a typed query layer can be built externally.
 *
 * A query builder's whole job is to assemble the same logical plan the SQL parser assembles, and
 * then hand it to the engine. That requires the plan's types, the block-assembly functions the
 * parser itself ends in, and the validators that keep a hand-built plan as strict as a parsed one.
 * Exposing them here lets an external builder produce plans the engine treats as indistinguishable
 * from parsed SQL.
 *
 * The recursive logical model lives in the dependency-light `model` leaf; executable assembly
 * and validation stay with the compiler. This entry point keeps that distinction invisible to
 * consumers.
 */
export {
  assembleSelectBlock,
  compoundSelectBlock,
  derivedTableSource,
  hasAggregate,
  splitCondition,
  validateLimit,
  validateOffset,
  type CompiledStatement,
} from "../engine/query.js";
export type {
  AggregateName,
  CompiledQuery,
  Expression,
  JoinPlan,
  Predicate,
  PredicateOperator,
  QueryResult,
  QueryRow,
  QueryValue,
  SelectItem,
  SetOperator,
  TableSource,
  WindowFunctionName,
} from "./model.js";
export { optimizePlan, renderPlan } from "../engine/optimizer.js";
export { validateFtsQuery } from "../engine/fts.js";
