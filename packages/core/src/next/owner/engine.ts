/**
 * The owner worker's inside: the boundary between the protocol server (this track) and the
 * engine composed from the other tracks. The server owns connections, request admission and
 * outcomes, liveness, cancellation, frame delivery, session state and the idle-transaction
 * timeout; the engine owns planning, execution, writes and storage.
 *
 * The composition root (owner/, Phase 2) implements `OwnerEngine` from:
 * - the front end + `Lowerer` (lower/lowerer.ts), with a plan cache keyed by (text, catalog
 *   version);
 * - the executor (B4, exec/executor.ts): `Executor.prepare(plan)` → `PreparedPlan.bind/execute`
 *   → `Execution`, whose `results` (exec/result.ts `ResultStream`) the server pulls as is;
 * - the write path (B3, access/write-path.ts): `WritePath.pin`, `begin` → `WriteTransaction`,
 *   `statement`, `commit(request)` → `CommitReceipt`, `durable(lsn)`, and its `RequestLedger`;
 * - the store (B1) behind `acquireFenced`.
 *
 * ## Startup (rulings R13, R16.6)
 * acquire the fence → open the store session → open the extent space (it walks the current and
 * previous roots from the store's root slots) → open the write path (`openWritePath`: the single
 * log scan, truncation and replay, including staged rows) → sweep the spill store → ready. Only
 * then does the owner register with the broker and answer `hello`.
 *
 * ## Threading and ordering
 * - Everything runs on the owner worker's one thread. Planning is synchronous; an execution runs
 *   synchronously until it meets a real Promise and yields at least every 4 ms of work
 *   (B4 `YIELD_INTERVAL_MS`, ruling R16.4).
 * - Requests of one session run one at a time, in arrival order. Sessions run concurrently;
 *   writers are serialized by B3's single writer slot: a read-write transaction takes it at
 *   `begin` and holds it to COMMIT/ROLLBACK (ruling R11), an autocommit write for its statement.
 *
 * ## Completions (rulings R2, R16.8)
 * The executor's three completions belong to the `Execution` (exec/executor.ts):
 * 1. `executed`: the statement ran (a query's last frame offered; DML's `statement.end()`);
 * 2. committed: the owner's acknowledgement of an autocommit commit, signalled to the executor by
 *    `acknowledgeCommit()` (`returning: "atCommit"`) so the RETURNING spool may flow. Inside an
 *    explicit transaction RETURNING flows at `executed` (`"atExecuted"`, ruling R11);
 * 3. `delivered`: every frame released, the result grant closed.
 * `Run.done` is **owner-facing**, not a fourth completion: it settles when the server may send the
 * request's terminal `done`: for a query, at `executed`; for an autocommit write, once committed
 * (`commit()` called, then `receipt.durable` under strict, nothing more under relaxed, then
 * `acknowledgeCommit()`); for a statement inside a transaction, at `executed`. The server sends
 * `done` once `Run.done` has settled and the stream has ended (the last frames ride in it).
 * `delivered` only closes the grant; nothing waits on it.
 *
 * ## Results (rulings R3, R16.5)
 * - The server drains `results.poll()` until it returns `undefined`, posts the frames it got in
 *   one message (`postMessage(payload, transfer)` per exec/result.ts), and awaits `next()` only
 *   then. Under strict durability it first awaits `writePath.durable(frame.requiresLsn)` when that
 *   returns a promise (null, no await, in the common case).
 * - It calls a posted frame's `release()` when the client's `release` signal covers it, and every
 *   outstanding frame's when the connection ends (then `results.cancel()`).
 *
 * ## Outcomes (outcomes.ts)
 * The server admits every request through the ledger first and, once answered, applies
 * `ledgerAction`: the write path records commits; the server records failures and session-scoped
 * `statement` outcomes, and settles the rest.
 *
 * ## Cancellation
 * `RunContext.signal` aborts when the client cancels, the connection ends, the idle timeout rolls
 * the session back, or the owner steps down. Before the write path's `commit()` call the run
 * aborts (the executor cancels; the transaction rolls back). From that call on the signal is
 * ignored and the run completes.
 */
import type { CommitRequest, Durability } from "../access/write-path.js";
import type { SequenceSession } from "../exec/executor.js";
import type { ResultFrame, ResultStream } from "../exec/result.js";
import type { PhysicalType } from "../types.js";
import type { WireInputColumn } from "./frame.js";
import type { LedgerSession, OwnerLedger } from "./outcomes.js";
import type { StatementSummary, TransactionState, WireColumnMeta, WireValue } from "./protocol.js";

/** A statement planned for the current catalog version. Cached; shared across connections. */
export interface PlannedStatement {
  readonly text: string;
  readonly kind: "query" | "modify" | "ddl" | "transactionControl" | "utility";
  readonly columns: readonly WireColumnMeta[];
  readonly parameters: readonly PhysicalType[];
  readonly controlsTransaction: boolean;
  /** For transaction control: what it does (outcomes.ts `AnsweredRequest.control`). */
  readonly control: "begin" | "commit" | "rollback" | "other" | null;
  readonly catalogVersion: number;
}

export interface RunContext {
  readonly signal: AbortSignal;
  /** The identity for the commit frame (B3 `CommitRequest`), or null if this run commits nothing. */
  readonly request: CommitRequest | null;
  /** Unreleased frame bytes allowed: the executor's `resultWindowBytes`. */
  readonly windowBytes: number;
  /** Upper bound on rows per frame, or null. */
  readonly frameRows: number | null;
}

/** What a run reports; the server adds `replayed`, `closedEarly` and the session state. */
export type RunOutcome = Pick<StatementSummary, "command" | "rowCount" | "counts" | "lsn">;

export interface Run {
  /** The executor's stream (B4), pulled by the server. Empty for statements without rows. */
  readonly results: ResultStream;
  /** Owner-facing: the terminal `done` may be sent (see "Completions"). */
  readonly done: Promise<RunOutcome>;
  /** Whether the server has called the write path's `commit()` for this run. */
  readonly reachedCommit: boolean;
}

/** The synchronous fast path's whole answer (ruling R6). */
export interface SyncRun {
  /** Every frame of the result (a point read has one or none). */
  readonly frames: readonly ResultFrame[];
  readonly columns: readonly WireColumnMeta[];
  readonly outcome: RunOutcome;
}

/** One session: its transaction, its pins, its sequence state. One per connection session id. */
export interface EngineSession {
  /** The ledger key of this session's `statement` outcomes (outcomes.ts). */
  readonly ledgerSession: LedgerSession;
  readonly transaction: TransactionState;
  readonly readOnly: boolean;
  /**
   * `currval`/`lastval` state (ruling R4), passed to every execution of the session as the
   * executor's `sequenceSession`. Survives rollback, as in PostgreSQL.
   */
  readonly sequences: SequenceSession;
  /** This session's last commit LSN: relaxed read-your-writes pins at least this (B3 `atLeastLsn`). */
  readonly lastCommitLsn: number | null;
  /**
   * Runs `statement` to completion synchronously when the executor finishes inside `execute`
   * (B4: `state === "executed"` on return, frames ready in `poll()`), or returns null without
   * side effects when it cannot know that up front (not a point-read plan, a writer-slot wait).
   * A run that turns out to need an await continues as a `Run` (`continued`). No promise,
   * microtask or timer on the hit path (ruling R6).
   */
  runNow(
    statement: PlannedStatement,
    params: readonly WireValue[],
    context: RunContext,
  ): SyncRun | { readonly continued: Run } | null;
  /** Runs `statement` (re-planned by the engine if the catalog moved). */
  run(statement: PlannedStatement, params: readonly WireValue[], context: RunContext): Run;
  /** A batch write (protocol.ts `WriteRequest`); RETURNING follows the completions above. */
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
  commit(context: RunContext): Run;
  /** Rolls back the open transaction (releasing the writer slot or the pin). Idempotent. */
  rollback(): void;
  /**
   * The idle timeout fired: roll back and enter `timedOut` (protocol.ts "Idle transactions") until
   * the client sends ROLLBACK or COMMIT.
   */
  expire(): void;
  /** Rolls back anything open and drops every pin. Idempotent. */
  close(): void;
}

export interface OwnerEngine {
  readonly durability: Durability;
  readonly catalogVersion: number;
  /** The write path's ledger (B3 `WritePath.ledger`), as the owner uses it. */
  readonly ledger: OwnerLedger;
  /** Parses, rewrites and lowers `text`, or returns the cached plan. Throws SQL errors. */
  plan(text: string): PlannedStatement;
  /** A new session; its ledger key comes from the connection's generation. */
  openSession(clientId: string, generation: number): EngineSession;
  /** Tables a statement's result depends on: subscriptions re-run after commits touching them. */
  readSet(statement: PlannedStatement): readonly number[];
  /** Calls `listener` after each commit with the ids of the tables it changed. */
  onCommit(listener: (tables: readonly number[]) => void): () => void;
  /** Admin calls (protocol.ts `AdminOp`), each with its structured-clone value. */
  admin(op: string, args: readonly WireValue[], context: RunContext): Promise<unknown>;
  /**
   * Starts a log flush now if relaxed commits are pending; resolves when they are durable, at
   * once if none are (ruling R9: called when the host reports `hidden`).
   */
  flushPending(): Promise<void>;
  /** Flushes, then closes the write path and the store (graceful step-down). */
  close(): Promise<void>;
}

/**
 * The idle-transaction timer of one session (ruling R16.7). The server calls `start()` when a
 * request on the session starts, `end()` when it ends, `touch()` when one of the session's streams
 * is released, and `setOpen` as its transaction opens and ends. It fires `onExpire` (which calls
 * `EngineSession.expire()` and sends `sessionAborted`) after `timeoutMs` with a transaction open
 * and no request running. A session without an open transaction arms no timer, so autocommit
 * requests pay nothing.
 */
export class IdleTransactionTimer {
  readonly #timeoutMs: number;
  readonly #onExpire: () => void;
  #timer: ReturnType<typeof setTimeout> | null = null;
  #running = 0;
  #open = false;

  constructor(timeoutMs: number, onExpire: () => void) {
    if (!(timeoutMs > 0)) throw new RangeError("transactionIdleTimeoutMs must be positive");
    this.#timeoutMs = timeoutMs;
    this.#onExpire = onExpire;
  }

  /** The session's transaction opened or ended. */
  setOpen(open: boolean): void {
    this.#open = open;
    this.#arm();
  }

  /** A request on the session started. */
  start(): void {
    this.#running++;
    this.#arm();
  }

  /** A request on the session ended. */
  end(): void {
    if (this.#running > 0) this.#running--;
    this.#arm();
  }

  /** Activity that is not a request (a frame release): restart the idle period. */
  touch(): void {
    this.#arm();
  }

  dispose(): void {
    this.#open = false;
    this.#arm();
  }

  #arm(): void {
    if (this.#timer !== null) clearTimeout(this.#timer);
    this.#timer = null;
    if (!this.#open || this.#running > 0) return;
    this.#timer = setTimeout(() => {
      this.#timer = null;
      this.#open = false;
      this.#onExpire();
    }, this.#timeoutMs);
  }
}
