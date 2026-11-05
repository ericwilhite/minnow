/**
 * The executor's write port: how a `modify` plan (ir/ir.ts) hands rows, row identities and new
 * values to the write path, and what it gets back for RETURNING and affected-row counts.
 *
 * The write-path track (B3) owns staging, row ids, unique checks, the key index and commit; the
 * executor cannot import `write/`, so it states the calls it makes here and B3's statement writer
 * satisfies them. The executor evaluates every expression (VALUES rows, defaults lowering put in
 * the input, SET assignments, ON CONFLICT … WHERE, RETURNING); the write path evaluates none.
 *
 * ## One target per statement
 * - The owner opens a `ModifyTarget` for one statement on one table, inside its transaction, and
 *   passes it in `ExecuteRequest.modify`. The executor calls it sequentially (never two calls in
 *   flight) and then exactly one of `finish` or `abort`.
 * - `finish` applies statement-end checks and makes the statement's changes part of the
 *   transaction. `abort` (error or cancellation) discards every change this target staged, as an
 *   implicit savepoint; what happens to the enclosing transaction is the write path's rule.
 * - The executor never commits. The owner commits after `done` resolves (autocommit) or at COMMIT.
 *
 * ## Row identity
 * - Rows to update or delete are named by locators of the statement's snapshot (type
 *   `{ kind: "locator", tableId }`, vector/locator.ts), as the plan's scan emitted them. The
 *   write path resolves them to row ids and reads the old index keys itself (the log frame needs
 *   delete keys of every index: format/log-frame.ts `TableChange.deletes`).
 * - A row named twice in one statement (UPDATE … FROM with several matches, DELETE … USING) is
 *   changed once, by its first occurrence in call order; later occurrences are skipped and not
 *   counted, as in PostgreSQL. A null locator is skipped.
 * - The statement's own writes are invisible to its snapshot (exec/snapshot.ts), so an updated
 *   row is never seen, and so never updated, again.
 *
 * ## Batches
 * - Every batch is borrowed for the call: the target copies what it keeps before the promise
 *   settles, charged to its own memory (the executor's grants never pay for staged rows).
 * - Row batches carry every table column in catalog order (`TableSnapshot`'s columns, hidden ones
 *   excluded), already cast to the column types; the active rows are the batch's selection.
 * - The target checks NOT NULL, unique and primary keys (23502 / 23505 errors). Range and type
 *   checks were done by the executor's cast kernels.
 *
 * ## Outcomes
 * Each call returns which active rows took effect, as a selection over the input batch; the
 * executor evaluates RETURNING over exactly those rows and counts them.
 */
import type { Batch, FlatVector, Selection } from "../vector/vector.js";

export type ConflictMode =
  /** No ON CONFLICT: a duplicate key fails the statement (unique violation, 23505). */
  | { readonly kind: "error" }
  /** ON CONFLICT DO NOTHING: conflicting rows are skipped. */
  | { readonly kind: "nothing"; readonly indexId: number | null }
  /** ON CONFLICT … DO UPDATE: conflicting rows are returned for the executor to update. */
  | { readonly kind: "update"; readonly indexId: number };

export interface InsertResult {
  /** Active input rows that were inserted. Owned by the target; valid until its next call. */
  readonly inserted: Selection;
  /**
   * DO UPDATE only (empty otherwise): input rows that conflicted, with the existing row each one
   * met. `existing` has every table column, row k aligned with `rows[k]`, and `locators` names the
   * existing rows for `update`. A row inserted or updated earlier by this statement is a
   * cardinality error, raised by the target ("ON CONFLICT DO UPDATE command cannot affect row a
   * second time", 21000). Valid until the target's next call.
   */
  readonly conflicts: {
    readonly count: number;
    readonly rows: Uint16Array;
    readonly existing: Batch;
    readonly locators: FlatVector;
  };
}

export interface ModifyTarget {
  readonly tableId: number;
  /** Inserts the active rows, in order (a later row sees an earlier one's key). */
  insert(rows: Batch, conflict: ConflictMode, signal?: AbortSignal): Promise<InsertResult>;
  /**
   * Replaces each named row with the full new row in `rows` (slot for slot; `rows` and
   * `locators` share the selection). Returns the active slots that were updated.
   */
  update(locators: FlatVector, rows: Batch, signal?: AbortSignal): Promise<Selection>;
  /**
   * Deletes each named row: slots 0 … `locators.length` − 1, or those `selection` names. Returns
   * the active slots that were deleted.
   */
  delete(
    locators: FlatVector,
    selection: Selection | null,
    signal?: AbortSignal,
  ): Promise<Selection>;
  /** Statement-end checks; afterwards the changes belong to the transaction. */
  finish(): Promise<void>;
  /** Discards everything this statement staged. Idempotent; never throws. */
  abort(reason: unknown): void;
}

/**
 * Sequence access for `nextval`/`currval`/`setval` calls (identity and serial defaults are
 * `nextval` calls lowering puts into the insert input). Synchronous: the write path reserves
 * blocks ahead in memory and logs reservations with the commit (format/log-frame.ts `sequence`
 * effects); values handed out are never reused, even when the statement rolls back (PostgreSQL).
 * Exhaustion raises PostgreSQL's 2200H error from inside the call.
 */
export interface SequenceAccess {
  /** Writes the next `count` values of the sequence into `out[0 … count)`. */
  next(sequenceId: number, count: number, out: BigInt64Array): void;
  /** `currval`: the last value `next` returned in this session, or the 55000 error. */
  current(sequenceId: number): bigint;
  /** `setval(id, value, isCalled)`. */
  set(sequenceId: number, value: bigint, isCalled: boolean): void;
}
