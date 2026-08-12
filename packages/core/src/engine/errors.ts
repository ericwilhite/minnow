/**
 * Typed engine errors, kept free of engine imports so the main-thread worker client can rehydrate
 * them (name, fields, instanceof) without bundling the executor.
 */

type ErrorValue = boolean | number | string | Date;

function formatValue(value: ErrorValue): string {
  return value instanceof Date ? value.toISOString() : String(value);
}

export class UniqueConstraintError extends Error {
  override readonly name = "UniqueConstraintError";

  constructor(
    readonly tableName: string,
    readonly columnName: string,
    readonly value: ErrorValue,
  ) {
    super(`Duplicate value for ${tableName}.${columnName}: ${formatValue(value)}`);
  }
}

export class MissingKeyError extends Error {
  override readonly name = "MissingKeyError";

  constructor(
    readonly tableName: string,
    readonly columnName: string,
    readonly value: ErrorValue,
  ) {
    super(`Missing value for ${tableName}.${columnName}: ${formatValue(value)}`);
  }
}

export class CompactionMemoryBudgetError extends Error {
  override readonly name = "CompactionMemoryBudgetError";

  constructor(
    readonly budgetBytes: number,
    readonly minimumBytes: number,
  ) {
    super(
      `Compaction needs at least ${String(minimumBytes)} bytes of working memory; budget is ${String(budgetBytes)} bytes`,
    );
  }
}

export class CompactionWriteAmplificationError extends Error {
  override readonly name = "CompactionWriteAmplificationError";

  constructor(
    readonly outputBytes: number,
    readonly maximumOutputBytes: number,
  ) {
    super(
      `Compaction output would use ${String(outputBytes)} stored bytes; limit is ${String(maximumOutputBytes)} bytes`,
    );
  }
}

export class CompactionJobCancelledError extends Error {
  override readonly name = "CompactionJobCancelledError";

  constructor(readonly jobId: string) {
    super(`Compaction job cancelled: ${jobId}`);
  }
}

/**
 * SQL that failed to compile, located in the text the caller passed. `offset` and `length` are
 * character positions into that exact string — leading whitespace included — so an editor can
 * underline the token that failed without re-deriving the position from the message.
 *
 * `length` is 0 where the failure has no width: an empty statement, or a query that ended before
 * the parser expected it to. Errors raised after parsing succeeds (plan optimization, execution)
 * carry no position and stay plain `TypeError`s.
 *
 * It extends `TypeError` because that is what compilation failures threw before positions existed.
 */
export class SqlCompileError extends TypeError {
  override readonly name = "SqlCompileError";

  constructor(
    message: string,
    readonly offset: number,
    readonly length: number,
  ) {
    super(message);
  }
}
