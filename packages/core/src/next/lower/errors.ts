/**
 * Errors lowering raises itself. Errors from the front end (parser and passes: `TypeError`,
 * `SqlCompileError`) pass through unchanged, as they do in the current engine.
 */

/**
 * A front-end construct lowering does not translate yet: one key per row of the "Unsupported at
 * first" table in INVENTORY.md, so the differential harness can count refusals per construct.
 */
export type UnsupportedConstruct =
  /** MERGE (needs the IR `merge` modify kind, INVENTORY.md "IR requests" 2). */
  | "merge"
  /** CREATE TRIGGER / DROP TRIGGER and firing (the new catalog has no triggers). */
  | "trigger"
  /** CREATE TYPE … AS ENUM and columns of a named enum type (the new catalog has no types). */
  | "enumType"
  /** `@@` / BM25 full-text search (no IR operator). */
  | "fullTextSearch"
  /** A LATERAL source the decorrelator left correlated (needs a relation-valued `apply`). */
  | "lateral"
  /** A correlated subquery the decorrelator left in a position `apply` cannot express. */
  | "correlatedSubquery"
  /** A function or operator overload with no kernel signature yet. */
  | "function"
  /** A column type the new core's catalog cannot declare yet (an array of an unknown element). */
  | "columnType"
  /** An ALTER TABLE form beyond ADD / RENAME / DROP COLUMN. */
  | "alterTable";

/**
 * Lowering refuses a statement it cannot translate. Same `code` and SQLSTATE (0A000,
 * feature_not_supported) as the executor's `UnsupportedPlanError` (exec/errors.ts): the owner
 * treats both as "not supported by the new core", which the differential harness reports per
 * `construct` rather than as a wrong answer. Proposed: both move to next/errors.ts as one class
 * with `stage: "lower" | "execute"` (INVENTORY.md "Contract requests").
 */
export class UnsupportedPlanError extends Error {
  readonly code = "MINNOW_UNSUPPORTED_PLAN";
  readonly sqlState = "0A000";
  readonly stage = "lower";
  constructor(
    readonly construct: UnsupportedConstruct,
    detail: string,
  ) {
    super(`Not supported yet (${construct}): ${detail}`);
    this.name = "UnsupportedPlanError";
  }
}

/**
 * A SQL error found while typing and binding the statement against the catalog: an unknown
 * table (42P01) or column (42703), an ambiguous name (42702), no operator or function for the
 * argument types (42883), a type mismatch (42804), a constant out of its type's range (22003).
 * `sqlState` is PostgreSQL's code for the condition.
 */
export class PlanningError extends Error {
  readonly code = "MINNOW_PLANNING";
  constructor(
    message: string,
    readonly sqlState: string,
  ) {
    super(message);
    this.name = "PlanningError";
  }
}
