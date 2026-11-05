/**
 * The write path as the owner composes it: transactions, group commit, durability and delivery,
 * sequences, backpressure, memory, startup (owner: write path, track B3; consumers: the ownership
 * track B5, which maps client requests onto it, and the executor, which runs statements in it).
 *
 * ## One writer at a time
 * `begin()` hands out the writer slot in FIFO order (today's "turns"). A transaction holds it
 * until `commit()` resolves or `rollback()`; nothing else commits meanwhile, so a transaction
 * never meets a write conflict and its reads need no validation. Readers never take the slot.
 * DDL takes it too. Maintenance (memtable flushes, merges) runs beside it and publishes by root
 * swap; it never changes what any view sees. Sequence block frames do not take it either.
 *
 * ## Commit: the irreversible boundary (ruling R9)
 * The `commit()` call is the boundary. Before it, the owner may cancel (it calls `rollback()`);
 * `commit` takes no signal, and once called the transaction commits unless the write path fails,
 * in which case its outcome is unknown until reopen (the client retries; the ledger answers).
 * `commit(request)`:
 * 1. encodes the transaction's ordered effects (catalog changes, row changes per table and schema
 *    version, the request) as log frames of at most `MAX_FRAME_BYTES` (1 MiB) each, multipart
 *    when bigger (format/log-frame.ts);
 * 2. a **large transaction** (pending rows past the memtable bounds, already written as staged
 *    groups with their index runs: snapshot.ts "Segments") first publishes a root that stages
 *    those groups and runs under the transaction's id (`TableRoot.staged`), and its frame carries
 *    a `publishedRows` effect naming that root and the indexes it built runs for, instead of the
 *    rows (format/replay.ts "Large transactions"); the frame is still the commit point;
 * 3. appends the frames, makes the transaction's changes committed in memory (restamping its
 *    pending seqs to its LSN), and releases the writer slot. `commit` resolves here;
 * 4. the frames become durable with the next log flush: if none is running one starts at once;
 *    commits appended while one runs share the next (one flush per group). That flush also writes
 *    the header slot acknowledging the previous group; an idle flush ≤ 100 ms after the last
 *    commit acknowledges the newest. `receipt.durable` resolves after it.
 * **No-effect transactions** (nothing written, or everything rolled back) write no frame:
 * `receipt.wrote` is false, `lsn` is the base LSN. The owner then settles the request without an
 * outcome (format/ledger.ts `settle`); a retry runs again, which is correct for a no-op.
 *
 * ## Ledger (ruling R9)
 * The owner admits every request (`ledger.admit`), records failures and the session-scoped
 * `statement` outcomes of statements inside an open transaction, and settles requests that changed
 * nothing. Standalone queries, `SELECT nextval(…)` included, are at-least-once (ruling R16.2). The write path records **committed** outcomes (`ledger.record`, with the frame's LSN
 * and counts): under strict durability when `receipt.durable` resolves, under relaxed when
 * `commit` resolves. Replay re-records the outcomes of frames past the root's `globalLsn`.
 *
 * ## Durability and visibility
 * - `appliedLsn`: the last committed transaction in memory. `durableLsn`: the last one whose
 *   frames are flushed. `durableLsn ≤ appliedLsn`.
 * - **Strict** (default): the owner acknowledges a commit after `receipt.durable`. A read snapshot
 *   (`pin`) sees exactly the durable commits, so no reader observes a commit a crash could lose.
 *   A write transaction reads at `appliedLsn` (to serialize after the previous writer).
 * - **Relaxed:** the owner acknowledges after `commit` resolves; `pin` sees `appliedLsn`. A crash
 *   can lose the newest acknowledged commits (never reorder them).
 * - `pin({ atLeastLsn })` raises a read snapshot's bound to a client's own last commit, for
 *   read-your-writes when that commit is not yet durable (relaxed).
 *
 * ## Delivery (the rule the owner and executor follow)
 * Under strict durability, nothing derived from a view (rows, RETURNING, counts, sequence values)
 * reaches a client before every commit the view observed and every sequence block it drew from
 * is durable: await `durable(lsn)` for `view.observedLsn` and for the largest LSN `sequences`
 * returned. Both are ordinary group flushes that never wait on an open transaction, so a
 * statement inside an explicit transaction waits at most one flush, never for COMMIT. `durable`
 * returns null when nothing is pending: the common case costs no await. RETURNING of DML is
 * additionally held by the executor until the owner acknowledges the commit (ruling R2).
 *
 * ## Failure
 * If a log flush or append, or a publish, fails (quota, `OwnershipLostError`, I/O, the space's
 * `SpaceFailedError`), the affected commits reject `durable` with `WritePathFailedError`, and so
 * does every later call: later transactions may already depend on the lost group in memory, so
 * the only recovery is to close and reopen. The write path never pretends a failed group happened.
 *
 * ## Backpressure (no dials)
 * `begin()` and statement calls wait, never fail, while debt is over its bound: log bytes not yet
 * recyclable (every table and the global state must be checkpointed past the log's end before the
 * log is recycled, so a cold table with memtable rows is flushed for that), memtable bytes,
 * frozen memtables awaiting flush, level-0 index runs and overlapping groups, and garbage (dead
 * rows; space B1 holds for the next publish or an old pin: `Space.stats.waitingBytes`).
 * Maintenance runs only while debt exists, yields to foreground work between batches, and stops
 * when within bounds.
 *
 * ## Log recycling
 * The log is reset (`StoreSession.resetLog`, new salt) only after the publish of the root that
 * covers it has **resolved**: that root checkpoints global state and every table through the old
 * log's last LSN and names the new incarnation (format/manifest.ts). Until it resolves, the old
 * log is what recovery replays.
 *
 * ## Idle maintenance (ruling R10)
 * B1 reuses retired space only after the *next* publish, so a quiet database could strand garbage.
 * When the database is idle (no transaction, no commit for the idle-flush interval, no maintenance
 * due) and `Space.stats.waitingBytes > max(16 MiB, 5% of stats.dataBytes)`, the write path
 * publishes one no-change root (same content, next sequence, nothing retired) so the waiting space
 * becomes reusable. The space never publishes on its own. Then it is quiet again: no timers until
 * the next commit (zero wakeups when idle).
 *
 * ## Memory (rulings R5, R16.3)
 * - Memtables, index deltas, the resident fences and Bloom filters of index runs, and scratch are
 *   one write-path grant (memory/governor.ts). It is spillable in the governor's sense: its
 *   reclaim callback flushes memtables.
 * - Frozen memtables awaiting their flush's publication are charged to the write path,
 *   non-spillable, up to `FROZEN_MEMTABLE_CAP_BYTES` (writers wait past it). Once the flush
 *   publishes, readers still on that memtable are re-pointed to the published group and the
 *   memtable is freed; each reader keeps at most one borrowed chunk per memory segment, charged to
 *   the write path until the borrow ends (snapshot.ts, ruling R16.3). No pin holds unbounded memory
 *   and none blocks a writer.
 * - Durable bytes are read through B1's verified cache only (`Space`), with leases bounded per
 *   call. Target: ≤ 4 B of resident heap per keyed row (fences + Bloom ≈ 1.4 B/row; the rest is
 *   bounded by the memtable thresholds, ~64k rows or ~8 MB per table).
 *
 * ## Startup order (rulings R13, R16.6; storage/contract.ts and format/manifest.ts)
 * 1. acquire the fence (storage/fence.ts); 2. open the store session; 3. open B1's extent space
 * (it walks the two roots it keeps, current and previous, and builds the free map: the write path
 * keeps no free-space map of its own); 4. `openWritePath` performs the **single** log scan
 * (format/log-frame.ts `scanLogIncremental`, under its admitted `RECOVERY_SCAN_BYTES`, validated
 * against the durable root), truncates the log to `validEnd`, and replays the same scan into
 * memtables, catalog, sequences and the ledger (format/replay.ts rules, `publishedRows` staging
 * included: one frame plus the memtables it rebuilds in memory, flushing through the space when
 * they fill); it drops staging no frame claimed (rows and index runs together, retired at its first
 * publish) and admits its grant; 5. the owner sweeps the spill space (`SpillService.sweep`);
 * 6. ready.
 */
import type { CommitFrameBody } from "../format/log-frame.js";
import type { RequestLedger } from "../format/ledger.js";
import type { MemoryGovernor } from "../memory/governor.js";
import type { StoreSession } from "../storage/contract.js";
import type { Int64Values } from "../vector/vector.js";
import type { CatalogView } from "./catalog.js";
import type { Statement, StatementOptions } from "./modify.js";
import type { Snapshot } from "./snapshot.js";
import type { Space } from "./space.js";

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
  /** See "Ledger": the owner admits, records failures and settles; the write path records commits. */
  readonly ledger: RequestLedger;
  /** Sequences, for every caller (inside a transaction or not: they are not transactional). */
  readonly sequences: SequenceAccess;
  /** Null while healthy; the `WritePathFailedError` once failed. */
  readonly failure: Error | null;

  /** A read snapshot (synchronous, O(1)). The caller releases it. */
  pin(options?: PinOptions): Snapshot;
  /** Waits for the writer slot (FIFO) and for backpressure. */
  begin(options?: BeginOptions): Promise<WriteTransaction>;
  /** Resolves when every frame with LSN ≤ `lsn` is durable; null if that is already so. */
  durable(lsn: number): Promise<void> | null;
  /**
   * Starts a log flush now if anything is unflushed (the owner calls it on `hidden` / `pagehide`,
   * ruling R9's `flushPending`); resolves when what was appended before the call is durable.
   */
  flush(): Promise<void>;
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
  /** A rollback point after every ended statement so far. */
  savepoint(): Savepoint;
  /** Undoes statements after `savepoint` (it stays usable); later savepoints are discarded. */
  rollbackTo(savepoint: Savepoint): void;
  /** Forgets `savepoint` and every later one. */
  release(savepoint: Savepoint): void;
  /**
   * The irreversible boundary ("Commit"). `TransactionStateError` (nothing committed) if a
   * statement is open or the transaction is not open.
   */
  commit(request: CommitRequest | null): Promise<CommitReceipt>;
  /** Undoes everything and frees the writer slot. Idempotent; a no-op after commit. */
  rollback(): void;
}

export interface Savepoint {
  readonly statement: number;
}

export interface CommitReceipt {
  /** False for a no-effect transaction: no frame, nothing recorded (the owner settles). */
  readonly wrote: boolean;
  /** The commit's LSN, or the base LSN when nothing was written. */
  readonly lsn: number;
  /** Resolves when the commit is durable (at once when nothing was written). */
  readonly durable: Promise<void>;
}

/**
 * Sequences, PostgreSQL's CACHE semantics (ruling R4): not transactional, gaps allowed, never
 * handed out twice. Values come from in-memory blocks. A block is reserved by a **standalone
 * sequence frame** (a one-effect transaction of its own: format/log-frame.ts `sequence`), appended
 * between other frames, independent of any open transaction (it does not take the writer slot and
 * a rollback does not return it), and made durable by the ordinary group flush. Block sizes adapt:
 * the first block of a sequence holds 1 value; a block used up within a second of its reservation
 * doubles the next, up to 1,024; a sequence idle for a second shrinks back. A crash skips the unused
 * rest of the newest block. currval / lastval are the owner's session state, passed to the
 * executor per session.
 *
 * A standalone `SELECT nextval(…)` is **at-least-once** (ruling R16.2): no outcome is recorded, so
 * a retry after a lost reply runs again and draws new values; the lost ones are a gap, which
 * PostgreSQL allows. Only commits are exactly-once.
 */
export interface SequenceAccess {
  /**
   * Writes the next `count` values of `sequenceId` into `out` at `offset …` (synchronous: a new
   * block is reserved in memory and its frame appended in the same step). Returns the LSN of the
   * newest block frame the values came from: under strict durability, deliver them after
   * `durable(lsn)`. With `out.repr === "safe"`, a value outside ±(2^53 − 1) throws `RangeError`
   * before anything is consumed: widen to `int64` and call again. Past the bound of a non-cycling
   * sequence it throws `SequenceExhaustedError` (2200H) and consumes nothing.
   */
  next(sequenceId: number, count: number, out: Int64Values, offset: number): number;
  /** setval(): a standalone frame too; returns its LSN. */
  set(sequenceId: number, value: bigint, isCalled: boolean): number;
}

export interface WritePathStats {
  readonly appliedLsn: number;
  readonly durableLsn: number;
  readonly logBytes: number;
  readonly memtableBytes: number;
  readonly frozenMemtables: number;
  /** Bytes of frozen memtables awaiting publication (≤ `FROZEN_MEMTABLE_CAP_BYTES`). */
  readonly frozenMemtableBytes: number;
  /** Bytes of chunks readers still borrow from freed memtables (one per memory segment each). */
  readonly borrowedChunkBytes: number;
  /** Readers currently re-pointed to published groups, and the re-point pins held for them. */
  readonly repointedReaders: number;
  readonly repointPins: number;
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
  readonly signal?: AbortSignal;
}

export interface OpenWritePathInput {
  /** The session the owner opened under its fence (step 2): the log is read and cut through it. */
  readonly session: StoreSession;
  /** B1's extent space over that session (step 3): every data-space read, write and publish. */
  readonly space: Space;
  readonly governor: MemoryGovernor;
  readonly options: WritePathOptions;
}

/**
 * Step 4 of "Startup order": scan, truncate and replay the log, then serve. Throws
 * `LogCorruptionError` / `StoreCorruptionError` rather than open a damaged database (replay rule
 * violations included), and a typed refusal for a pre-reset layout. Recovery memory is admitted
 * from `governor` before anything is read.
 */
export type OpenWritePath = (input: OpenWritePathInput) => Promise<WritePath>;

export class WritePathFailedError extends Error {
  readonly code = "MINNOW_WRITE_PATH_FAILED";
  constructor(options?: ErrorOptions) {
    super("A commit or publish could not be made durable; close and reopen the database", options);
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
