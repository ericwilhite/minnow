/**
 * What the request ledger (format/ledger.ts) keeps for each request the owner has answered, so a
 * duplicate or a retry gets the right thing (rulings R9, R12, R16.1, R16.2).
 *
 * | Request                                                     | Ledger                                   | Retry gets          |
 * | ----------------------------------------------------------- | ---------------------------------------- | ------------------- |
 * | Reached the write path's `commit()` and committed           | `committed`, recorded by the write path  | the commit (counts) |
 * | Would have committed, failed before `commit()`              | `failed`, recorded by the owner          | the same error      |
 * | Inside a transaction: changes it, or fails                  | `statement`, session-scoped, by owner    | the same outcome    |
 * | Anything else: reads, standalone queries (`nextval` too), queries in a transaction, ROLLBACK, prepare, subscribe | `settle`: nothing recorded | a fresh run |
 * | Failed at or after `commit()` (the write path failed)       | `settle`; the owner reopens the write path, and its replayed ledger answers the retry | the commit, or a fresh run |
 *
 * - "Inside a transaction" means the session's transaction was open, failed or timed out when the
 *   request started, or the request opened it (`begin`, SQL `BEGIN`). A statement that changes it:
 *   DML, DDL, transaction control other than COMMIT/ROLLBACK (`BEGIN`, `SAVEPOINT`, `RELEASE`,
 *   `ROLLBACK TO`). A failing statement always changes it: it aborts the transaction (25P02), so
 *   re-running it would report 25P02 instead of the original error.
 * - A successful query inside a transaction is settled: re-running it reads the same transaction
 *   state, and a recorded outcome could not carry its rows.
 * - Only commits are exactly-once. A standalone `SELECT nextval(…)` is at-least-once: a lost reply
 *   and a retry skip a value, a gap PostgreSQL also allows.
 * - `statement` outcomes are in memory, keyed by (client, engine session, generation): a
 *   connection is one generation, and its end calls `endSession` for each of its sessions, which
 *   clears that session's outcomes only. A completion arriving after that is not recorded.
 */
import type { Admission, Outcome, RequestIdentity, SessionRef } from "../format/ledger.js";
import type { PlannedStatement } from "./engine.js";
import type { Request, TransactionState } from "./protocol.js";

/**
 * A session as the ledger keys `statement` outcomes: the contracts' `SessionRef` (format/ledger.ts,
 * ruling R16.1): the engine session id, unique within the client, and the connection's generation.
 */
export type LedgerSession = SessionRef;

/**
 * The ledger calls the owner makes; `RequestLedger` (format/ledger.ts) implements it. A request's
 * session generation and scope reach the ledger in its `RequestIdentity` at `admit`; the write
 * path records `committed` outcomes itself.
 */
export interface OwnerLedger {
  admit(request: RequestIdentity, nowMs: number): Admission;
  /** A request that changed nothing: no outcome, a retry runs again. */
  settle(clientId: string, sequence: number): void;
  /**
   * A `failed` or `statement` outcome (owner). False when it is ignored: its session generation
   * ended (or it was never admitted), so a retry is `sessionLost`.
   */
  record(clientId: string, sequence: number, outcome: Outcome): boolean;
  /**
   * Generations ≤ `generation` of the session ended: drop their `transaction`-scope requests and
   * outcomes; its standalone requests (commits in progress) and every other session keep theirs.
   */
  endSession(clientId: string, sessionId: string, generation: number, nowMs: number): void;
}

export interface AnsweredRequest {
  readonly kind: Request["t"];
  /** The planned statement's kind, for `statement` requests (null for other kinds). */
  readonly plan: PlannedStatement["kind"] | null;
  /** For SQL transaction control (`statement` requests): what it does; null otherwise. */
  readonly control: PlannedStatement["control"];
  /** The session's transaction state when the request started. */
  readonly before: TransactionState;
  /** The request opened the transaction (`begin`, or SQL `BEGIN`/`START TRANSACTION`). */
  readonly opened: boolean;
  /** The owner called the write path's `commit()` for this request (the irreversible boundary). */
  readonly reachedCommit: boolean;
  /** The request would commit if it succeeded: an autocommit write or DDL, or a COMMIT of a read-write transaction. */
  readonly commits: boolean;
  readonly failed: boolean;
}

export type LedgerAction =
  /** The write path records `committed`; the owner records nothing. */
  | "writePath"
  /** The owner records `failed`. */
  | "failed"
  /** The owner records a session-scoped `statement` outcome. */
  | "statement"
  /** The owner settles: nothing recorded. */
  | "settle";

/** The ledger action for an answered request. See the module table. */
export function ledgerAction(request: AnsweredRequest): LedgerAction {
  if (request.commits) {
    if (!request.failed) return "writePath";
    // Failing after the boundary means the write path failed (B3 `WritePathFailedError`): the
    // outcome is unknown until the owner reopens it, whose replayed ledger answers the retry.
    return request.reachedCommit ? "settle" : "failed";
  }
  if (request.kind === "rollback" || request.control === "rollback") return "settle";
  if (request.kind === "commit" || request.control === "commit") return "settle";
  const inTransaction = request.before !== "idle" || request.opened;
  if (!inTransaction) return "settle";
  if (request.failed || request.opened) return "statement";
  return request.plan === "query" ? "settle" : "statement";
}

/** What the owner knows about a request when it admits it (before running it). */
export interface ArrivingRequest {
  readonly kind: Request["t"];
  /** For SQL transaction control (`statement` requests), else null. */
  readonly control: PlannedStatement["control"];
  /** The session's transaction state when the request reaches the head of its session's queue. */
  readonly before: TransactionState;
  readonly retry: boolean;
  readonly resendable: boolean;
}

/**
 * The ledger scope of a request (ruling R18.2): `transaction` for a statement inside a transaction
 * the client opened (or opening one: `begin`, SQL `BEGIN`), `standalone` for everything else —
 * autocommit writes and DDL, COMMIT and ROLLBACK, queries outside a transaction, prepares,
 * subscriptions, admin calls. A resent request that is not resendable belonged to a session's
 * state; unless it ends a transaction (whose COMMIT may have committed and must find its outcome)
 * it is `transaction` scope, so an ended generation answers `sessionLost`.
 */
export function ledgerScope(request: ArrivingRequest): "transaction" | "standalone" {
  const ends =
    request.kind === "commit" ||
    request.kind === "rollback" ||
    request.control === "commit" ||
    request.control === "rollback";
  if (ends) return "standalone";
  if (request.kind === "begin" || request.control === "begin") return "transaction";
  if (request.retry && !request.resendable) return "transaction";
  if (request.kind === "prepare" || request.kind === "subscribe" || request.kind === "admin")
    return "standalone";
  return request.before === "idle" ? "standalone" : "transaction";
}

/** The ledger identity the owner derives for a request (ruling R18.2). */
export function ledgerIdentity(
  clientId: string,
  header: {
    readonly seq: number;
    readonly sentAtMs: number;
    readonly resolvedThrough: number;
    readonly generation: number;
  },
  session: number,
  scope: "transaction" | "standalone",
): RequestIdentity {
  return {
    clientId,
    sessionId: String(session),
    generation: header.generation,
    scope,
    sequence: header.seq,
    sentAtMs: header.sentAtMs,
    resolvedThrough: header.resolvedThrough,
  };
}
