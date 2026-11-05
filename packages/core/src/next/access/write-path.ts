/**
 * The write path as the owner composes it: transactions, group commit, durability and delivery,
 * sequences, backpressure (owner: write path, track B3; consumer: ownership track B5, which maps
 * client requests onto it, and the executor, which runs statements inside it).
 *
 * ## One writer at a time
 * `begin()` hands out the writer slot in FIFO order (today's "turns"). A transaction holds it
 * until `commit()` resolves or `rollback()`; nothing else commits meanwhile, so a transaction
 * never meets a write conflict and its reads need no validation. Readers never take the slot.
 * DDL takes it too. Maintenance (memtable flushes, merges) runs beside it and publishes by root
 * swap; it never changes what any view sees.
 *
 * ## Commit and group commit
 * `commit(request)`:
 * 1. encodes the transaction's ordered effects (catalog changes, sequence reservations, row
 *    changes per table and schema version, the request outcome) as log frames of at most 1 MiB
 *    each (a bigger transaction is multipart: format/log-frame.ts);
 * 2. appends them, makes the transaction's changes committed in memory (restamping its pending
 *    seqs to its LSN: snapshot.ts), and releases the writer slot. `commit` resolves here with a
 *    receipt;
 * 3. the frames become durable with the next log flush. If no flush is running, one starts at
 *    once; commits appended while one runs share the next flush (one flush per group). That flush
 *    also writes the log header slot acknowledging the previous group; an idle flush ≤ 100 ms
 *    after the last commit acknowledges the newest. `receipt.durable` resolves after the flush,
 *    and the write path records the outcome in the request ledger (format/ledger.ts) then.
 * A transaction with no effects and no request writes no frame. A transaction with a request
 * always writes one (its outcome must be exactly-once even when it changed nothing).
 *
 * ## Durability and visibility
 * - `appliedLsn`: the last committed transaction in memory. `durableLsn`: the last one whose
 *   frames are flushed. `durableLsn ≤ appliedLsn`.
 * - **Strict** (default): the owner acknowledges a commit after `receipt.durable`. A read snapshot
 *   (`pin`) sees exactly the durable commits, so no reader ever observes a commit a crash could
 *   lose. A write transaction reads at `appliedLsn` (it must, to serialize after the previous
 *   writer); its results wait for its own commit's flush, which covers what it read.
 * - **Relaxed:** the owner acknowledges after `commit` resolves; `pin` sees `appliedLsn`. A crash
 *   can lose the newest acknowledged commits (never reorder them).
 * - `pin({ atLeastLsn })` raises a read snapshot's bound to a client's own last commit, for
 *   read-your-writes when that commit is not yet durable (relaxed).
 *
 * ## Delivery (the rule the owner and executor follow)
 * Under strict durability, nothing derived from a view (rows, RETURNING, counts, sequence values)
 * reaches a client before every commit the view observed and every sequence reservation it used
 * is durable: await `durable(lsn)` for `view.observedLsn` and the largest LSN returned by
 * `SequenceAccess`. `durable` returns null when that is already true, so the common case costs no
 * await. An autocommit write satisfies the rule by acknowledging after its own `receipt.durable`.
 *
 * ## Failure
 * If a log flush or append fails (quota, `OwnershipLostError`, I/O), the commits of that group
 * reject `durable` with `WritePathFailedError`, and so does every later call: later transactions
 * may already depend on the lost group in memory, so the only recovery is to close and reopen
 * (`openWritePath` replays the durable log; a request whose outcome is unknown is retried by its
 * client and answered through the ledger). The write path never pretends a failed group happened.
 *
 * ## Backpressure (no dials)
 * `begin()` and statement calls wait, never fail, while debt is over its bound: log bytes not yet
 * recyclable (every table and the global state must be checkpointed past the log's end before the
 * log is recycled, so a cold table with memtable rows is flushed for that), memtable bytes,
 * frozen memtables awaiting flush, level-0 index runs and overlapping groups, and garbage (dead
 * rows and space pinned by old snapshots). Maintenance runs only while debt exists, yields to
 * foreground work between batches, and stops when within bounds.
 *
 * ## Memory
 * The write path holds one spillable grant (memory/governor.ts) for memtables, index deltas,
 * Bloom filters and fences of runs, frozen memtables kept by pins, and its scratch. Its reclaim
 * callback flushes memtables (its way to spill). Durable bytes go through the page cache.
 * Target: ≤ 4 B of resident heap per keyed row (fences + Bloom ≈ 1.4 B/row; the rest is bounded
 * by the memtable thresholds, ~64k rows or ~8 MB per table).
 */
import type { CommitFrameBody } from "../format/log-frame.js";
import type { RequestLedger } from "../format/ledger.js";
import type { MemoryGovernor } from "../memory/governor.js";
import type { DataRange, StoreSession } from "../storage/contract.js";
import type { Int64Values } from "../vector/vector.js";
import type { CatalogView } from "./catalog.js";
import type { Statement, StatementOptions } from "./modify.js";
import type { Snapshot } from "./snapshot.js";

export type Durability = "strict" | "relaxed";

/** The request a commit answers (exactly-once: format/ledger.ts). */
export type CommitRequest = NonNullable<CommitFrameBody["request"]>;

export interface PinOptions {
  /** Diagnostic label (client and query), reported with old pins in `stats()`. */
  readonly label?: string;
  /** Raise the bound to at least this LSN (≤ `appliedLsn`): a client's own last commit. */
  readonly atLeastLsn?: number;
}

export interface BeginOptions {
  readonly label?: string;
  /** Cancels the wait for the writer slot or for backpressure (rejects with its reason). */
  readonly signal?: AbortSignal;
}

export interface WritePath {
  readonly durability: Durability;
  readonly appliedLsn: number;
  readonly durableLsn: number;
  /** The latest committed catalog. */
  readonly catalog: CatalogView;
  /** The owner admits requests and records failures here; the write path records commits. */
  readonly ledger: RequestLedger;
  /** Sequences used outside a write transaction (SELECT nextval(…)). */
  readonly sequences: SequenceAccess;
  /** Null while healthy; the `WritePathFailedError` once failed. */
  readonly failure: Error | null;

  /** A read snapshot (synchronous, O(1)). The caller releases it. */
  pin(options?: PinOptions): Snapshot;
  /** Waits for the writer slot (FIFO) and for backpressure. */
  begin(options?: BeginOptions): Promise<WriteTransaction>;
  /** Resolves when every commit with LSN ≤ `lsn` is durable; null if that is already so. */
  durable(lsn: number): Promise<void> | null;
  stats(): WritePathStats;
  /**
   * Flushes, writes the final header, releases pins and memtables. Rejects open transactions
   * (rolled back) and pending begins with `WritePathClosedError`. Idempotent.
   */
  close(): Promise<void>;
}

export interface WriteTransaction {
  /** The LSN this transaction's statements read from (`appliedLsn` at begin). */
  readonly baseLsn: number;
  /** Committed catalog plus this transaction's DDL. */
  readonly catalog: CatalogView;
  readonly state: "open" | "committing" | "committed" | "rolledBack" | "failed";
  /** Starts the next statement. One statement at a time: the previous one must have ended. */
  statement(options: StatementOptions): Statement;
  /** Sequence values inside the transaction (reservations ride this transaction's frame). */
  readonly sequences: SequenceAccess;
  /** A rollback point after every ended statement so far. */
  savepoint(): Savepoint;
  /** Undoes statements after `savepoint` (it stays usable); later savepoints are discarded. */
  rollbackTo(savepoint: Savepoint): void;
  /** Forgets `savepoint` and every later one. */
  release(savepoint: Savepoint): void;
  /** See "Commit and group commit". Rejects if a statement is open. */
  commit(request: CommitRequest | null): Promise<CommitReceipt>;
  /** Undoes everything and frees the writer slot. Idempotent; a no-op after commit. */
  rollback(): void;
}

export interface Savepoint {
  readonly statement: number;
}

export interface CommitReceipt {
  /** The commit's LSN, or the base LSN when nothing was written. */
  readonly lsn: number;
  /** Resolves when the commit is durable; rejects with `WritePathFailedError`. */
  readonly durable: Promise<void>;
}

/**
 * Sequences (PostgreSQL semantics: not transactional, never handed out twice while the owner
 * lives). Values come from an in-memory block reserved by a logged effect; a call that needs a new
 * block reserves it synchronously and returns that reservation's LSN, which callers include in
 * the delivery rule. Outside a transaction a reservation is a frame of its own, appended between
 * transactions and grouped like any commit; inside one, it rides the transaction's frame. A
 * reservation lost to a crash or a rollback can repeat values no committed row holds.
 */
export interface SequenceAccess {
  /**
   * Writes the next `count` values of `sequenceId` into `out` at `offset …`. With
   * `out.repr === "safe"`, a value outside ±(2^53 − 1) throws `RangeError` before anything is
   * consumed: widen to `int64` and call again. Past the bound of a non-cycling sequence it throws
   * `SequenceExhaustedError` (2200H) after consuming nothing. Returns the LSN to wait for.
   */
  next(sequenceId: number, count: number, out: Int64Values, offset: number): number;
  /** setval(): returns the LSN to wait for. */
  set(sequenceId: number, value: bigint, isCalled: boolean): number;
}

export interface WritePathStats {
  readonly appliedLsn: number;
  readonly durableLsn: number;
  readonly logBytes: number;
  readonly memtableBytes: number;
  readonly frozenMemtables: number;
  readonly levelZeroRuns: number;
  readonly garbageBytes: number;
  readonly pins: number;
  readonly oldestPin: { readonly label: string; readonly ageMs: number } | null;
  /** True while any debt is over its bound (writers are being slowed). */
  readonly backpressure: boolean;
}

export interface WritePathOptions {
  readonly durability: Durability;
  /** Device clock in ms (the ledger's clock); injectable for tests. */
  readonly now?: () => number;
}

/** Verified, cached reads of the data space (the storage track's page cache). */
export interface PageReads {
  readData(ranges: readonly DataRange[]): Promise<Uint8Array[]>;
}

/**
 * Opens the write path over an acquired store session (the owner acquired it: storage/contract.ts):
 * reads the root, replays the log incrementally (format/replay.ts rules, memory bounded by one
 * frame plus the memtables it rebuilds), truncates an invalid tail, rebuilds the free-space map by
 * walking the root (crash orphans are unreachable), restores the ledger, and admits its grant.
 * Throws `LogCorruptionError` / `StoreCorruptionError` rather than open a damaged database, and a
 * typed refusal for a pre-reset layout.
 */
export type OpenWritePath = (
  session: StoreSession,
  governor: MemoryGovernor,
  pages: PageReads,
  options: WritePathOptions,
) => Promise<WritePath>;

export class WritePathFailedError extends Error {
  readonly code = "MINNOW_WRITE_PATH_FAILED";
  constructor(options?: ErrorOptions) {
    super("A commit group could not be made durable; close and reopen the database", options);
    this.name = "WritePathFailedError";
  }
}

export class WritePathClosedError extends Error {
  readonly code = "MINNOW_WRITE_PATH_CLOSED";
  constructor() {
    super("The write path is closed");
    this.name = "WritePathClosedError";
  }
}

/** A call on a transaction that is no longer open, or out of order (a statement still open). */
export class TransactionStateError extends Error {
  readonly code = "MINNOW_TRANSACTION_STATE";
  constructor(message: string) {
    super(message);
    this.name = "TransactionStateError";
  }
}

/** 2200H: a non-cycling sequence reached its bound. */
export class SequenceExhaustedError extends Error {
  readonly code = "MINNOW_SEQUENCE_EXHAUSTED";
  readonly sqlState = "2200H";
  constructor(readonly sequenceId: number) {
    super(`Sequence ${String(sequenceId)} reached its limit`);
    this.name = "SequenceExhaustedError";
  }
}
