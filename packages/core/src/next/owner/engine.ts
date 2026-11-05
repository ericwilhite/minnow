/**
 * The owner worker's inside: the boundary between the protocol server (this track) and the
 * engine composed from the other tracks. The server owns connections, request admission,
 * liveness, cancellation, frame delivery and session state; the engine owns planning, execution,
 * writes and storage.
 *
 * The composition root (owner/, Phase 2) implements `OwnerEngine` from:
 * - the front end + `Lowerer` (ir/lowering.ts), with a plan cache keyed by (text, catalog version);
 * - the executor (B4): `Executor.prepare(plan)` → `PreparedPlan.bind/execute` → `Execution`, whose
 *   `results` stream is wrapped as `FrameStream` here without copying;
 * - the write path and key index (B3, access/): `WritePath.pin`, `begin` → `WriteTransaction`,
 *   `commit(request)` → `CommitReceipt`, `durable(lsn)`, and the `RequestLedger` it holds;
 * - the store (B1) behind `acquireFenced`, opened in ruling R7's order: fence → store session →
 *   log recovery → extent space → write path → ready. Only then does the owner register with the
 *   broker and answer `hello`.
 *
 * ## Threading and ordering
 * - Everything runs on the owner worker's one thread. Planning is synchronous; execution and
 *   commit are async and yield at least every 4 ms (B4 `YIELD_INTERVAL_MS`).
 * - Requests of one session run one at a time, in arrival order. Sessions run concurrently;
 *   writers are serialized by B3's single writer slot, which `begin` (read-write) and autocommit
 *   writes wait for.
 *
 * ## Results: pulled, transferred, released on credit (ruling R3)
 * - The server pulls each run's `FrameStream`. It drains whatever is queued without awaiting
 *   (`poll`), posts those frames in one message with their transfer lists, and awaits `next()` only
 *   when nothing is queued. It never copies or re-encodes a payload.
 * - A posted frame's `release()` is called when the client's `release` signal covers it (or when
 *   the connection ends). Until then its bytes stay charged to the run's result grant, which
 *   outlives the run's `done` and closes when the last frame is released or the stream cancelled.
 * - The server keeps nothing per frame but its release handle; the client keeps the stream's
 *   dictionaries (frame.ts `StreamDictionaries`).
 *
 * ## The three completions of a write with RETURNING (ruling R2)
 * 1. **Executed**: the statement validated and its modify ended (`Run.done` resolves).
 * 2. **Committed**: for an autocommit write, the composition calls `WriteTransaction.commit` (the
 *    irreversible boundary, ruling R9), then waits for `receipt.durable` under strict durability or
 *    nothing more under relaxed; it then calls B4's `Execution.acknowledgeCommit()`. Inside an
 *    explicit transaction the statement's effect is as committed as it gets until COMMIT, so
 *    `acknowledgeCommit()` follows the statement's end (and B3's delivery rule wait on the
 *    view's observed LSN), as PostgreSQL returns RETURNING rows before COMMIT.
 * 3. **Delivered**: only after `acknowledgeCommit()` does the stream offer the spooled RETURNING
 *    frames; the server posts them and then `done`. If the commit fails, the stream is cancelled,
 *    the spool discarded, and the client gets the error alone.
 * A plain SELECT streams at once (its frames do not wait for `done`).
 *
 * ## Exactly-once (ruling R9)
 * - The server admits every request through the write path's `ledger.admit` first.
 * - An effectful request (autocommit write or DDL, `commit`) passes its identity in
 *   `RunContext.request`; B3 writes it into the commit frame and **records the committed outcome
 *   itself** once durable (strict) or applied (relaxed).
 * - The server records **failures** (`ledger.record` with `failed`) for effectful requests and
 *   **settles** everything with no durable effect (`ledger.settle`): reads, statements inside an
 *   open transaction, rollbacks, prepares, subscriptions.
 *
 * ## Cancellation and liveness
 * - `RunContext.signal` aborts when the client cancels, when the connection ends, or when the
 *   owner steps down. Before the write path's `commit()` call, the run aborts (B4 cancels its
 *   pipelines; B3 rolls the transaction back). From that call on, the signal is ignored and the
 *   run completes.
 */
import type { RequestIdentity } from "../format/ledger.js";
import type { PhysicalType } from "../types.js";
import type { ResultFrame, WireInputColumn } from "./frame.js";
import type { StatementSummary, WireColumnMeta, WireValue } from "./protocol.js";

/** A statement planned for the current catalog version. Cached; shared across connections. */
export interface PlannedStatement {
  readonly text: string;
  readonly kind: "query" | "modify" | "ddl" | "transactionControl" | "utility";
  readonly columns: readonly WireColumnMeta[];
  readonly parameters: readonly PhysicalType[];
  readonly controlsTransaction: boolean;
  readonly catalogVersion: number;
}

/**
 * One frame as the executor offers it (ruling R3): a postMessage-safe payload, its transfer list,
 * and, outside the payload, its size and release handle.
 */
export interface StreamedFrame {
  readonly payload: ResultFrame;
  readonly transfer: readonly ArrayBuffer[];
  /** Bytes charged to the result grant until `release`. */
  readonly byteLength: number;
  /** Returns the frame's bytes to the window. Idempotent. */
  release(): void;
}

/**
 * The executor's result stream as the server pulls it (B4 `ResultStream`, wire-ready payloads).
 * One consumer; frames in `sequence` order.
 */
export interface FrameStream {
  readonly columns: readonly WireColumnMeta[];
  /**
   * Synchronously: the next queued frame, `end` after the last, or `pending` when the server
   * must await `next()`. Throws the execution's error once queued frames are drained. Lets the
   * server batch queued frames into one message and fold the last ones into `done` without an
   * await (a requirement on B4's stream).
   */
  poll(): StreamedFrame | "end" | "pending";
  /** The next frame, or null after the last; rejects with the execution's error. */
  next(): Promise<StreamedFrame | null>;
  /** Releases queued frames and cancels the execution (before the commit boundary). Idempotent. */
  cancel(reason?: unknown): void;
}

export interface RunContext {
  readonly signal: AbortSignal;
  /** The request identity for the commit frame, or null if this run commits nothing itself. */
  readonly request: RequestIdentity | null;
  /** Unreleased frame bytes allowed (protocol.ts `ResultRequest.windowBytes`). */
  readonly windowBytes: number;
  /** Upper bound on rows per frame, or null. */
  readonly frameRows: number | null;
}

/** What a run reports; the server adds `replayed`, `closedEarly` and the session state. */
export type RunOutcome = Pick<StatementSummary, "command" | "rowCount" | "counts" | "lsn">;

export interface Run {
  readonly results: FrameStream;
  /** Settles after the last frame was offered (and, for an autocommit write, committed). */
  readonly done: Promise<RunOutcome>;
}

/** The synchronous fast path's whole answer (ruling R6). */
export interface SyncRun {
  /** The frames, all of them (a point read has one or none). */
  readonly frames: readonly StreamedFrame[];
  readonly columns: readonly WireColumnMeta[];
  readonly outcome: RunOutcome;
}

/**
 * Per-session sequence state (ruling R4): PostgreSQL's `currval` and `lastval` belong to the
 * session, not the transaction, and survive its rollback. The server owns it; the executor reads
 * and updates it through this interface, passed with every execution of the session.
 */
export interface SessionSequenceState {
  /** `currval(sequenceId)`: the last value `nextval` gave this session, or null (55000 error). */
  currval(sequenceId: number): bigint | null;
  /** `lastval()`: the last value `nextval` gave this session for any sequence, or null. */
  lastval(): bigint | null;
  /** Called by the executor after each `nextval` (after `setval` too, as PostgreSQL does). */
  note(sequenceId: number, value: bigint): void;
}

/** One session: its transaction, its pins, its sequence state. One per connection session id. */
export interface EngineSession {
  readonly transaction: StatementSummary["transaction"];
  readonly sequences: SessionSequenceState;
  /** This session's last commit LSN: relaxed read-your-writes pins at least this (B3 `atLeastLsn`). */
  readonly lastCommitLsn: number | null;
  /**
   * Runs `statement` to completion synchronously, or returns null without side effects when it
   * would have to await (a key-index or page-cache miss, a writer-slot wait, a plan that is not a
   * point read). The server tries it first for every statement whose plan is a point read; this
   * path takes no promise, microtask or timer (ruling R6).
   */
  runNow(
    statement: PlannedStatement,
    params: readonly WireValue[],
    context: RunContext,
  ): SyncRun | null;
  /** Runs `statement` (re-planned by the engine if the catalog moved). */
  run(statement: PlannedStatement, params: readonly WireValue[], context: RunContext): Run;
  /** A batch write (protocol.ts `WriteRequest`); RETURNING rows follow the R2 order. */
  write(
    input: {
      readonly table: string;
      readonly op: "insert" | "upsert" | "update" | "delete";
      readonly names: readonly string[];
      readonly columns: readonly WireInputColumn[];
      readonly rowCount: number;
      readonly ignoreConflicts: boolean;
      readonly returning: readonly string[] | null;
    },
    context: RunContext,
  ): Run;
  /** Read-write: waits for B3's writer slot (FIFO). Read-only: pins a snapshot, synchronously. */
  begin(readOnly: boolean, signal: AbortSignal): Promise<void>;
  /** Commits the open transaction; resolves when acknowledged per durability mode. */
  commit(context: RunContext): Promise<RunOutcome>;
  rollback(): void;
  /** Rolls back anything open and drops every pin. Idempotent. */
  close(): void;
}

export interface OwnerEngine {
  readonly durability: "strict" | "relaxed";
  readonly catalogVersion: number;
  /** Parses, rewrites and lowers `text`, or returns the cached plan. Throws SQL errors. */
  plan(text: string): PlannedStatement;
  openSession(): EngineSession;
  /** Tables a statement's result depends on: subscriptions re-run after commits touching them. */
  readSet(statement: PlannedStatement): readonly number[];
  /** Calls `listener` after each commit with the ids of the tables it changed. */
  onCommit(listener: (tables: readonly number[]) => void): () => void;
  /** Admin calls (protocol.ts `AdminOp`), each with its structured-clone value. */
  admin(op: string, args: readonly WireValue[], context: RunContext): Promise<unknown>;
  /**
   * Starts a log flush now if relaxed commits are pending; resolves when they are durable, at
   * once if none are (ruling R9: called when the host reports `hidden`, so a later freeze or
   * pagehide termination loses at most what was acknowledged after it).
   */
  flushPending(): Promise<void>;
  /** Flushes, then closes the write path and the store (graceful step-down). */
  close(): Promise<void>;
}
