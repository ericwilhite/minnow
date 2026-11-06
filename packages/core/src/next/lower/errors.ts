import { UnsupportedPlanError } from "../errors.js";

/**
 * Errors lowering raises. Every failure of `Lowerer.lower` is a `PlanningError` with a SQLSTATE
 * or an `UnsupportedPlanError` (0A000): front-end failures (the parser's `SqlCompileError`, the
 * passes' `TypeError`) are translated by `frontEndError`, keeping the original as `cause` and the
 * parser's position.
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
  /** Where the parser failed: character offset and length in the statement text (additive). */
  readonly position?: { readonly offset: number; readonly length: number };
  constructor(
    message: string,
    readonly sqlState: string,
    options?: {
      readonly cause?: unknown;
      readonly position?: { readonly offset: number; readonly length: number };
    },
  ) {
    super(message, options?.cause === undefined ? undefined : { cause: options.cause });
    this.name = "PlanningError";
    if (options?.position !== undefined) this.position = options.position;
  }
}

/**
 * PostgreSQL's SQLSTATE for a front-end failure, by what its message says (the front end's
 * errors carry none). The front end checks some conditions in its own order, so a statement
 * with two faults may report the other one PostgreSQL would report first.
 */
const FRONT_END_STATES: ReadonlyArray<readonly [RegExp, string]> = [
  [/^SELECT \* requires known columns for: /u, "42P01"],
  [/^Unknown table alias: /u, "42P01"],
  [/^(Unknown|Unsupported) function: /u, "42883"],
  [/ requires (exactly |at least |at most )?\w+ arguments?\b/u, "42883"],
  [/^Unsupported CAST target: /u, "42704"],
  [/column names must be unique/u, "42701"],
  [/must appear in GROUP BY/u, "42803"],
  [/^Ambiguous or missing column: /u, "42702"],
  [/references an unavailable outer source: /u, "42P01"],
  [
    /not supported|is only supported|are supported only|supports? only|cannot (use|contain|select)|^Unsupported /u,
    "0A000",
  ],
];

/** A front-end failure as a `PlanningError` (see FRONT_END_STATES); others are returned as is. */
export function frontEndError(error: unknown): unknown {
  if (!(error instanceof TypeError)) return error;
  const message = error.message;
  const located =
    "offset" in error && typeof error.offset === "number" && "length" in error
      ? { offset: error.offset, length: Number(error.length) }
      : undefined;
  const mapped = FRONT_END_STATES.find(([pattern]) => pattern.test(message))?.[1];
  // A positioned failure the table does not name is the parser's: a syntax error. Anything
  // else is a refusal of the SQL by a pass, in the syntax-or-access-rule class.
  const sqlState = mapped ?? (located === undefined ? "42000" : "42601");
  const relation = /^SELECT \* requires known columns for: (.+)$/u.exec(message)?.[1];
  return new PlanningError(
    relation === undefined ? message : `relation "${relation}" does not exist`,
    sqlState,
    { cause: error, ...(located === undefined ? {} : { position: located }) },
  );
}
