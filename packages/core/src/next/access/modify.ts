/**
 * Writing rows: how the executor's `modify` operator (ir/ir.ts) hands INSERT, UPDATE, DELETE,
 * upsert (ON CONFLICT) and MERGE to the write path, how conflicts come back, and how RETURNING is
 * produced (owner: write path, track B3).
 *
 * ## Division of labour
 * - **Executor:** evaluates every expression: values, defaults (lowering fills them), casts and
 *   range checks to the column type, generated columns, assignments, WHERE, ON CONFLICT's DO
 *   UPDATE SET/WHERE, MERGE conditions, RETURNING, and CHECK conditions (through `TableRules`,
 *   which the write path calls). It finds target rows (scan or `keyLookup` with locators).
 * - **Write path:** row identity and storage, NOT NULL, uniqueness (primary key, unique
 *   constraints), foreign keys and their actions (ON DELETE CASCADE / SET NULL, rekey follow),
 *   index maintenance, the "row affected twice" rule, and statement atomicity.
 *
 * ## Row identity
 * Rows are named by **locators of the statement's view** (`Statement.view`), the same values a
 * scan with `emitLocator` or an index probe produces; row ids stay internal. Every active row of an
 * update or delete batch must carry a valid locator (outer-join null rows are filtered first).
 * An UPDATE writes the row under a new row id (delete + insert); the executor never sees either.
 *
 * ## Batches (`Batch` from vector/vector.ts, borrowed for the call: the write path copies)
 * - `insert`: one column per table column in catalog order (`TableDef.columns`), each of exactly
 *   the column's physical type. Generated and default values already filled.
 * - `update`: column 0 is the locator; then one column per `assigned` column id (`TableWriter`
 *   options), in that order. The write path copies every other column from the old row.
 * - `delete`: column 0 is the locator.
 * - `conflicts`: the proposed rows, shaped like `insert`.
 * Only active rows (the batch's selection) are written. `written`, when given, receives the
 * physical rows actually written, ascending (capacity ≥ active rows): every active row for insert;
 * for update and delete, all but repeats skipped under `onRepeat: "first"`.
 *
 * ## When constraints are checked (PostgreSQL's non-deferrable timing, today's engine's results)
 * - Immediately, in the call: type/shape errors (`TypeError`), NOT NULL, CHECK (via
 *   `TableRules.check` on the complete new row), the row-affected-twice rule.
 * - At `Statement.end()`: uniqueness and foreign keys, after every row of the statement is
 *   written, so a statement may swap two keys or insert a child before its parent. `end()` also
 *   runs foreign-key actions (cascades, SET NULL, rekey follow), recursively, as part of the same
 *   statement, calling `TableRules` of the tables they write.
 * ON CONFLICT is different by design: `conflicts` reports clashes on the arbiter indexes
 * immediately, against everything written so far, including this statement's earlier rows.
 *
 * ## Errors and atomicity
 * Constraint errors are the classes below (with PostgreSQL's SQLSTATE in `sqlState`). After any
 * error from a statement method, the statement is failed: its only valid call is `rollback()`,
 * which undoes all of its writes (statement-level atomicity). Whether the transaction continues
 * is the session's policy, not the write path's.
 *
 * ## RETURNING
 * The write path never materializes RETURNING. The executor evaluates it over the rows it
 * supplied (insert, update: the assigned values plus old values it fetched through the view) or
 * fetched (delete: the old row), restricted to `written`. It holds them (spilling if needed) and
 * delivers them only after `Statement.end()` succeeds, and for an autocommit statement after the
 * commit is acknowledged (write-path.ts "Delivery"). A duplicate request answered from the ledger
 * gets counts only (format/ledger.ts).
 *
 * ## Memory and async
 * The statement's writes live in the table's memtable as pending rows (snapshot.ts); the write
 * path charges them to its own grant. Calls return a Promise only when they must wait: reading
 * index blocks for checks, or flushing a full memtable (backpressure). One call at a time per
 * statement; callers check their cancellation signal between calls and `rollback()` on abort.
 */
import type { Batch, Selection } from "../vector/vector.js";
import type { CatalogChange, CatalogView } from "./catalog.js";
import type { MaybePromise } from "./key-index.js";
import type { Snapshot } from "./snapshot.js";

export interface StatementOptions {
  /**
   * The executor's row rules per table, compiled against the statement's catalog: called for
   * every row the statement writes, including rows written by foreign-key actions.
   */
  readonly rules: (tableId: number) => TableRules;
  /** Cancels waits inside the statement (backpressure, index reads). */
  readonly signal?: AbortSignal;
}

export interface Statement {
  /** 1, 2, … within its transaction. */
  readonly ordinal: number;
  /** What the statement reads: a statement view (snapshot.ts). Released by `end`/`rollback`. */
  readonly view: Snapshot;
  /** The catalog including this transaction's DDL so far. */
  readonly catalog: CatalogView;
  /**
   * A writer for one table. A statement may hold several writers of one table with different
   * options (MERGE); they share the statement's state: repeat detection, deferred checks.
   */
  table(tableId: number, options?: TableWriteOptions): TableWriter;
  /**
   * Applies one DDL change. Synchronous changes resolve at once; ones that read existing rows
   * (CREATE INDEX, ADD UNIQUE / CHECK / FOREIGN KEY / NOT NULL) scan through the write path and
   * may take long; `rules` validates new CHECKs. Later statements see the new catalog.
   */
  changeCatalog(change: CatalogChange): MaybePromise<{ readonly id: number | null }>;
  /** Runs deferred checks and foreign-key actions; then the statement is done. */
  end(): Promise<void>;
  /** Undoes everything the statement wrote, including its DDL. Idempotent. */
  rollback(): void;
}

export interface TableWriteOptions {
  /** Columns an `update` batch assigns, in batch order after the locator. Default: none. */
  readonly assigned?: readonly number[];
  /**
   * A second update or delete of the same row within this statement: `"error"` throws
   * `CardinalityViolationError` (MERGE, ON CONFLICT DO UPDATE); `"first"` keeps the first and
   * skips the repeat (UPDATE … FROM, DELETE … USING). Default `"error"`.
   */
  readonly onRepeat?: "error" | "first";
}

export interface TableWriter {
  readonly tableId: number;
  /** Returns the number of rows written. */
  insert(batch: Batch, written: Selection | null): MaybePromise<number>;
  update(batch: Batch, written: Selection | null): MaybePromise<number>;
  delete(batch: Batch, written: Selection | null): MaybePromise<number>;
  /**
   * ON CONFLICT: for each active proposed row, whether its key on one of `arbiters` (index ids,
   * tried in order; an empty list means every unique index, for a bare `DO NOTHING`) is already
   * taken by a live row, considering everything written so far in the transaction, including this
   * statement. Writes nothing. See `ConflictOutput`.
   */
  conflicts(batch: Batch, arbiters: readonly number[], out: ConflictOutput): MaybePromise<void>;
}

export const ConflictKind = {
  /** No live row holds the key: insert it. */
  none: 0,
  /** A row that existed before this statement holds it: `locators[k]` (DO UPDATE / DO NOTHING). */
  existing: 1,
  /**
   * A row this statement wrote holds it: `locators[k]`. DO UPDATE must raise
   * `CardinalityViolationError`; DO NOTHING skips.
   */
  statement: 2,
  /**
   * The key is new, but an earlier active row of this batch proposes it too: `earlier[k]` is the
   * first such row (physical), which itself reports `none`. DO UPDATE raises
   * `CardinalityViolationError`; DO NOTHING skips.
   */
  batch: 3,
} as const;
export type ConflictKind = (typeof ConflictKind)[keyof typeof ConflictKind];

/**
 * Caller-owned, indexed by physical row of the batch (capacity ≥ batch length); slots of
 * inactive rows are untouched. `locators[k]` is a locator of the statement's view (fetch serves it
 * even when the row is one this statement wrote).
 */
export interface ConflictOutput {
  readonly kinds: Uint8Array;
  readonly locators: Float64Array;
  readonly earlier: Int32Array;
  /** The arbiter index that matched (for kinds 1–3). */
  readonly indexIds: Int32Array;
}

/**
 * The executor's compiled rules for one table (one instance per table and catalog version). All
 * methods are synchronous kernels over a batch of complete new rows (every column, catalog
 * order); they must not retain the batch.
 */
export interface TableRules {
  /**
   * CHECK constraints over the active rows: the first failing row (physical) and constraint id,
   * or null when all pass (NULL passes). `columnIds`, when given, are the columns the write
   * changed: the rules may skip checks that read none of them.
   */
  check(
    batch: Batch,
    columnIds: readonly number[] | null,
  ): { readonly row: number; readonly checkId: number } | null;
  /**
   * Recomputes stored generated columns of rows the write path writes itself (foreign-key
   * actions changed `columnIds`), writing into the batch's own vectors (owned by the write path,
   * which allocated them for this call).
   */
  generate(batch: Batch, columnIds: readonly number[]): void;
}

abstract class ConstraintError extends Error {
  abstract readonly sqlState: string;
  constructor(
    message: string,
    readonly tableId: number,
  ) {
    super(message);
  }
}

/** 23505. `key` is the equality-key bytes (decodable for the message; never null-containing). */
export class UniqueViolationError extends ConstraintError {
  readonly code = "MINNOW_UNIQUE_VIOLATION";
  readonly sqlState = "23505";
  constructor(
    tableId: number,
    readonly indexId: number,
    readonly key: Uint8Array,
  ) {
    super(`Duplicate key in unique index ${String(indexId)}`, tableId);
    this.name = "UniqueViolationError";
  }
}

/**
 * 23503. `missingParent`: a reference names no parent row (`tableId` is the child). `referenced`:
 * a RESTRICT parent row is still referenced (`tableId` is the parent).
 */
export class ForeignKeyViolationError extends ConstraintError {
  readonly code = "MINNOW_FOREIGN_KEY_VIOLATION";
  readonly sqlState = "23503";
  constructor(
    tableId: number,
    readonly foreignKeyId: number,
    readonly problem: "missingParent" | "referenced",
    readonly key: Uint8Array,
  ) {
    super(`Foreign key ${String(foreignKeyId)} violated (${problem})`, tableId);
    this.name = "ForeignKeyViolationError";
  }
}

/** 23502. `row` is the physical batch row, or −1 for a row a foreign-key action wrote. */
export class NotNullViolationError extends ConstraintError {
  readonly code = "MINNOW_NOT_NULL_VIOLATION";
  readonly sqlState = "23502";
  constructor(
    tableId: number,
    readonly columnId: number,
    readonly row: number,
  ) {
    super(`Null value in non-null column ${String(columnId)}`, tableId);
    this.name = "NotNullViolationError";
  }
}

/** 23514. `row` as for `NotNullViolationError`. */
export class CheckViolationError extends ConstraintError {
  readonly code = "MINNOW_CHECK_VIOLATION";
  readonly sqlState = "23514";
  constructor(
    tableId: number,
    readonly checkId: number,
    readonly row: number,
  ) {
    super(`Check constraint ${String(checkId)} violated`, tableId);
    this.name = "CheckViolationError";
  }
}

/** 21000: a statement would update or delete one row twice (MERGE, ON CONFLICT DO UPDATE). */
export class CardinalityViolationError extends ConstraintError {
  readonly code = "MINNOW_CARDINALITY_VIOLATION";
  readonly sqlState = "21000";
  constructor(
    tableId: number,
    readonly row: number,
  ) {
    super("A command cannot affect a row a second time", tableId);
    this.name = "CardinalityViolationError";
  }
}
