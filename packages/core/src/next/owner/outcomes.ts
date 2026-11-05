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
import type { Admission, Outcome, RequestIdentity } from "../format/ledger.js";
import type { PlannedStatement } from "./engine.js";
import type { Request, TransactionState } from "./protocol.js";

/** A session as the ledger keys `statement` outcomes (ruling R16.1, contracts round 5). */
export interface LedgerSession {
  /** The owner's engine session id: unique among the owner's sessions. */
  readonly sessionId: number;
  /** The connection's generation for this client: increases with every connection. */
  readonly generation: number;
}

/**
 * The ledger calls the owner makes. `RequestLedger` (format/ledger.ts) implements it once contracts
 * round 5 keys `statement` outcomes and `endSession` by session; the write path records
 * `committed` outcomes itself.
 */
export interface OwnerLedger {
  admit(request: RequestIdentity, nowMs: number): Admission;
  /** A request that changed nothing: no outcome, a retry runs again. */
  settle(clientId: string, sequence: number): void;
  /** A `failed` outcome (owner), or a `statement` outcome with its session (owner). */
  record(clientId: string, sequence: number, outcome: Outcome, session?: LedgerSession): void;
  /** The session ended: drop its `statement` outcomes and in-flight statements; others stay. */
  endSession(clientId: string, session: LedgerSession): void;
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
