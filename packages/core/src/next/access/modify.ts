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
 * ## RETURNING (ruling R2)
 * The write path never materializes RETURNING. The executor evaluates it over the rows it
 * supplied (insert, update: the assigned values plus old values it fetched through the view) or
 * fetched (delete: the old row), restricted to `written`, into a charged, spillable result spool.
 * A statement has three completions: **executed** (`Statement.end()` resolved: every deferred
 * check passed), **committed** (the owner acknowledged the commit per durability mode, after
 * `WriteTransaction.commit`), **delivered**. The spool offers nothing before the owner calls the
 * executor's `acknowledgeCommit()`; on abort or rollback it is discarded. A duplicate request
 * answered from the ledger gets counts only (format/ledger.ts).
 *
 * ## Memory and async
 * The statement's writes live in the table's memtable as pending rows (snapshot.ts), charged to
 * the write path's grant. When a transaction's pending rows pass the memtable bounds, the write
 * path writes them as **staged groups**, with staged runs of their index puts, through the extent
 * space (readable by the transaction's later statements as memory segments re-pointed to the
 * staged group, at the same positions; committed through `publishedRows`, write-path.ts
 * "Commit"), so a bulk load is never resident whole. Calls return a Promise only when they must wait: reading index blocks for checks,
 * writing staged groups, or backpressure. One call at a time per statement; callers check their
 * cancellation signal between calls and `rollback()` on abort (which also aborts the statement's
 * staged writers).
 */
import type { Batch, Selection } from "../vector/vector.js";
import type { CatalogChange, CatalogView } from "./catalog.js";
import type { MaybePromise } from "./key-index.js";
import type { Snapshot } from "./snapshot.js";
import type { LentHoldings, WaitingHoldings } from "../memory/governor.js";

export interface StatementOptions {
  /** Owner-classified query: retains a read view but may not open row or DDL writers.
   * Its presence alone cannot pin a dormant transaction's future write minimum. */
  readonly readOnly?: boolean;
  /**
   * The executor's row rules per table, compiled against the statement's catalog (the executor's
   * `compileTableRules(table, catalogVersion)`, cached per version): called for every row the
   * statement writes, including rows written by foreign-key actions.
   */
  readonly rules: (tableId: number) => TableRules;
  /** Admitted, bounded repeat tracking. Returns true if this native row ID was already seen.
   * Shared by every writer of the statement; the owner fences borrows before closing it. */
  readonly targets?: { testAndSet(tableId: number, rowId: number): MaybePromise<boolean> };
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
   * Separately pinned read of the current statement's own write prefix, for a trigger body.
   * Unlike view, includes this statement's writes. Consume/spool the complete body input
   * before performing its mutations: reads and mutations of a statement do not run concurrently.
   * This is a phase pin, an exception to Snapshot's immutable-read promise: its bound includes
   * mutable own writes. After any mutation begins it MUST NOT be read again, even through a
   * previously obtained table/index/segment; retain it only for inputView locator resolution.
   * Caller releases after read/writer borrows drain; end/rollback also invalidates the pin.
   */
  readOwn(): Snapshot;
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
  /** After an accepted UPDATE/DELETE/upsert (including its INSERT arm). Native delete undo
   * independently invalidates FTS. No accepted rows means no effect. Rolls back with rows. */
  noteFtsMutation(tableId: number, kind: "update" | "delete" | "upsert"): void;
  /** Runs deferred checks/FK actions and FTS preparation. Both the statement signal and
   * optional end signal cancel waits; cancellation keeps statement rollback available. */
  end(signal?: AbortSignal): Promise<void>;
  /** Undoes everything the statement wrote, including its DDL. Idempotent. */
  rollback(): void;
  /**
   * The writer's holdings while this statement holds the writer turn (null for a read-only
   * statement): its grants, and the bytes of them it keeps however long the statement waits.
   * Part of what the statement cannot release while one of its requests waits
   * (`MemoryGovernor.servableNow`). Optional for statements without a native writer.
   */
  writerHoldings?(): LentHoldings | null;
  /**
   * While a query executes this statement, what the statement keeps held if one of its write
   * steps waits for memory: its query's grants, its writer's holdings, what only its snapshot
   * keeps (`WaitingHoldings`). Set by the executing query (exec/run.ts), restored when it
   * closes. Without it, a write step refuses rather than wait.
   */
  waitHoldings?: (() => WaitingHoldings) | null;
  /** The executing query's cancellation, set and restored with `waitHoldings`: a write step's
   * memory wait ends with it. */
  waitSignal?: AbortSignal | null;
}

export interface TableWriteOptions {
  /** Locators from this statement's readOwn() pin, not its start view. A foreign pin,
   * even from the same transaction/path, is refused. Caller retains it through writes. */
  readonly inputView?: Snapshot;
  /** Columns an `update` batch assigns, in batch order after the locator. Default: none. */
  readonly assigned?: readonly number[];
  /**
   * A second update or delete of the same row within this statement: `"error"` throws
   * `CardinalityViolationError` (MERGE, ON CONFLICT DO UPDATE); `"first"` keeps the first and
   * skips the repeat (UPDATE … FROM, DELETE … USING). Default `"error"`.
   */
  readonly onRepeat?: "error" | "first";
  /** Caller proves one visit per original target; omit repeat tracking for a scan/probe.
   * Never set for a join, MERGE or ON CONFLICT input. */
  readonly uniqueTargets?: boolean;
  /** SQL streams without the unique-target proof require an admitted tracker. Refuse before
   * writer lookup when it is missing; the direct-native legacy Set is not a SQL fallback. */
  readonly requireTrackedTargets?: boolean;
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
 * The executor's bound rules for one table/statement. Immutable programs may be cached per
 * catalog identity/table; `bindTableRules` supplies execution-owned charged buffers. Close that
 * binding only after the last writer borrow and statement.end() FK actions. All
 * methods are synchronous kernels over a batch of complete new rows (every column, catalog
 * order); they must not retain the batch.
 */
export interface TableRules {
  /** Optional complete-row validator: physical rows, column NOT NULL/closed sets, named CHECKs. */
  validate?(batch: Batch, columnIds: readonly number[] | null): void;
  /**
   * CHECK constraints over the active rows: the first failing row (physical) and constraint id,
   * or null when all pass (NULL passes), ordered by physical row then PostgreSQL constraint
   * name. `columnIds`, when given, are the columns the write
   * changed: the rules may skip checks that read none of them.
   */
  check(
    batch: Batch,
    columnIds: readonly number[] | null,
  ): { readonly row: number; readonly checkId: number } | null;
  /**
   * Recomputes stored generated columns of rows the write path writes itself (foreign-key
   * actions changed `columnIds`), writing into the batch's own vectors (owned by the write path,
   * which allocated their writable wrappers for this call). Generated data may be replaced by
   * binding-owned buffers, valid until that binding's next generate/close; writer copies finish
   * before reuse. OLD and target wrappers must be distinct, even when their old data is shared).
   * A SQL data error in speculative batch generation is replayed by row: generated columns,
   * NOT NULL/closed sets, then CHECKs in constraint-name order. An earlier failure can therefore
   * throw its typed constraint error from generate(), before a later row's expression error.
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

/** The most rows one write batch may carry: row positions and selections are 16-bit. */
export const MAX_WRITE_BATCH_ROWS = 0x10000;

/** 54000 (program limit exceeded). A write batch longer than `MAX_WRITE_BATCH_ROWS` is refused
 * before anything is written; the statement stays open for smaller batches. */
export class BatchTooLargeError extends Error {
  readonly code = "MINNOW_BATCH_TOO_LARGE";
  readonly sqlState = "54000";
  constructor(
    readonly rows: number,
    readonly limit: number = MAX_WRITE_BATCH_ROWS,
  ) {
    super(`A write batch of ${String(rows)} rows exceeds the ${String(limit)}-row limit`);
    this.name = "BatchTooLargeError";
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

/** 23001. Explicit RESTRICT refuses a referenced parent at its ordered RI event. */
export class RestrictViolationError extends ConstraintError {
  readonly code = "MINNOW_RESTRICT_VIOLATION";
  readonly sqlState = "23001";
  constructor(
    tableId: number,
    readonly foreignKeyId: number,
    readonly key: Uint8Array,
  ) {
    super(`Foreign key ${String(foreignKeyId)} restricts the parent change`, tableId);
    this.name = "RestrictViolationError";
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

/** A closed text set is a column restriction, not a named enum with label-order comparison. */
export class ClosedSetViolationError extends ConstraintError {
  readonly code = "MINNOW_CLOSED_SET_VIOLATION";
  readonly sqlState = "23514";
  constructor(
    tableId: number,
    readonly columnId: number,
    readonly row: number,
  ) {
    super(`Value outside the closed set of column ${String(columnId)}`, tableId);
    this.name = "ClosedSetViolationError";
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
