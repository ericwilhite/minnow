/**
 * The planner port: lowering's own `Lowerer` (lower/lowerer.ts, B6), as the composition root
 * (compose.ts) calls it. Planning is synchronous and runs the whole front end (parse, catalog
 * rewrites, lowering) over one catalog version, with the owner's planning statistics and the
 * executor's costs. The composition caches templates/markers by normalized family and raw plans by text, each
 * including catalog version and parameter types, and re-lowers when
 * the statistics a plan was costed on drift (`statisticsDrifted`).
 */
import type { Lowerer } from "../lower/lowerer.js";

export type {
  LoweredDdl,
  LoweredModify,
  LoweredQuery,
  LoweredStatement,
  LoweredTransactionControl,
  LoweredUtility,
  LoweringContext as PlannerContext,
  WriteShape,
} from "../lower/lowerer.js";

/** What the owner calls on lowering: statements, batch writes, and table rules for the executor. */
export type StatementPlanner = Pick<
  Lowerer,
  "lower" | "lowerWrite" | "lowerTableRules" | "parameterize" | "lowerParameterized"
>;
