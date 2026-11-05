/**
 * Errors the executor's entry point raises (executor.ts). Kernel errors (division by zero,
 * overflow, invalid input…) follow the same shape: a stable `code` and PostgreSQL's SQLSTATE.
 */

/** Base of every error the executor raises on purpose. */
export class ExecutionError extends Error {
  constructor(
    message: string,
    readonly code: string,
    /** PostgreSQL's SQLSTATE for the condition. */
    readonly sqlState: string,
    options?: ErrorOptions,
  ) {
    super(message, options);
    this.name = "ExecutionError";
  }
}

/** The execution was cancelled (signal, `Execution.cancel`, or the consumer cancelled results). */
export class QueryCancelledError extends ExecutionError {
  constructor(reason?: unknown) {
    super("The query was cancelled", "MINNOW_QUERY_CANCELLED", "57014", { cause: reason });
    this.name = "QueryCancelledError";
  }
}

/**
 * The plan was lowered against another catalog version than the snapshot's. Not a user error:
 * the owner re-lowers the statement and executes it again (nothing ran).
 */
export class PlanStaleError extends ExecutionError {
  constructor(planVersion: number, snapshotVersion: number) {
    super(
      `The plan was lowered at catalog version ${String(planVersion)}, the snapshot is at ${String(snapshotVersion)}`,
      "MINNOW_PLAN_STALE",
      "0A000",
    );
    this.name = "PlanStaleError";
  }
}

/** A parameter value does not fit its slot's type, or the count is wrong. */
export class ParameterBindError extends ExecutionError {
  constructor(
    message: string,
    /** The 0-based placeholder index ($1 is 0), or null for a count mismatch. */
    readonly slot: number | null,
  ) {
    super(message, "MINNOW_PARAMETER_BIND", slot === null ? "08P01" : "22023");
    this.name = "ParameterBindError";
  }
}

/**
 * The plan uses an operator, function overload or type the executor does not implement: the one
 * shared class (next/errors.ts, ruling R17.6), `stage: "execute"` by default.
 */
export { UnsupportedPlanError } from "../errors.js";

/** 22012: division (or modulo) by zero, for every numeric type (PostgreSQL raises it for floats too). */
export class DivisionByZeroError extends ExecutionError {
  constructor() {
    super("division by zero", "MINNOW_DIVISION_BY_ZERO", "22012");
    this.name = "DivisionByZeroError";
  }
}

/**
 * 22003: a result outside its type, with PostgreSQL's message: "smallint out of range",
 * "integer out of range", "bigint out of range", "value out of range: overflow" (float),
 * "value out of range: underflow" (float), "numeric field overflow".
 */
export class NumericOutOfRangeError extends ExecutionError {
  constructor(message: string) {
    super(message, "MINNOW_NUMERIC_OUT_OF_RANGE", "22003");
    this.name = "NumericOutOfRangeError";
  }
}

/** 21000: a scalar subquery or init plan returned more than one row. */
export class CardinalityError extends ExecutionError {
  constructor() {
    super(
      "more than one row returned by a subquery used as an expression",
      "MINNOW_CARDINALITY",
      "21000",
    );
    this.name = "CardinalityError";
  }
}

/** 0A000: NaN or ±Infinity cannot become an integer (PostgreSQL's numeric → int casts). */
export class CannotConvertError extends ExecutionError {
  constructor(special: number, target: string) {
    super(
      `cannot convert ${special === 1 ? "NaN" : "infinity"} to ${target}`,
      "MINNOW_CANNOT_CONVERT",
      "0A000",
    );
    this.name = "CannotConvertError";
  }
}
