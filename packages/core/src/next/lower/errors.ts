import { UnsupportedPlanError } from "../errors.js";

/**
 * Errors lowering raises itself. Errors from the front end (parser and passes: `TypeError`,
 * `SqlCompileError`) pass through unchanged, as they do in the current engine.
 */

/**
 * A front-end construct lowering does not translate yet: one key per row of the "Unsupported at
 * first" table in INVENTORY.md, so the differential harness can count refusals per construct.
 */
export type UnsupportedConstruct =
  /** A MERGE form lowering cannot express (a generated column reading a later volatile value). */
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
  | "alterTable"
  /** A subquery where no FROM clause can attach it. */
  | "subquery"
  /** A window function lowering does not translate (explicit frames are carried as written). */
  | "window"
  /** Ordered-set aggregates (percentile_cont/disc, mode) and JSON aggregates. */
  | "aggregate"
  /** SERIAL / IDENTITY columns (needs the write path's owned sequences, R17.7). */
  | "identityColumn"
  /** A foreign key to its own table (needs the write path's "self" parent, R17.7). */
  | "selfReference"
  /** CREATE OR REPLACE VIEW over an existing view (two catalog changes). */
  | "replaceView"
  /** A batch write that finds rows by key (update / delete batches need a lookup join, M3). */
  | "batchWrite";

/** Lowering refuses `detail`: the shared `UnsupportedPlanError` (next/errors.ts, R17.6). */
export function unsupported(construct: UnsupportedConstruct, detail: string): UnsupportedPlanError {
  return new UnsupportedPlanError(detail, { stage: "lower", construct });
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
