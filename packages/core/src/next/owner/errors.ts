/**
 * Errors of the owner/client boundary, and how any error crosses the port.
 *
 * - On the wire an error is a `WireError`: 0.19's `SerializedError` (worker-protocol), which
 *   keeps the name, message, cloneable own properties (`code` among them), the `cause` chain and
 *   `DOMException` identity. The client rehydrates it against a registry, so `instanceof` works and
 *   the stack is the owner's.
 * - Every class here has a stable `code` (`MINNOW_…`) and a literal `name`; match on either. The
 *   class identity holds only inside one realm; across the port it is the registry's.
 * - A silent owner fails reads with `OwnerUnresponsiveError` (next/errors.ts) and in-flight
 *   writes with `OutcomeUnknownError { reason: "unresponsive" }` carrying it as `cause`.
 * - Errors from the SQL front end, the executor and storage cross the same way; the client's
 *   registry includes their classes (composition, Phase 2).
 */
import { OwnerUnresponsiveError, RequestExpiredError, SessionLostError } from "../errors.js";
import {
  rehydrateError,
  serializeError,
  type ErrorRegistry,
  type SerializedError,
} from "../../worker-protocol/index.js";

/** Defined once for every track (next/errors.ts); re-exported for the client API. */
export { OwnerUnresponsiveError, RequestExpiredError, SessionLostError };

export type WireError = SerializedError;

export const toWireError = (error: unknown): WireError => serializeError(error);
export const fromWireError = (error: WireError, registry: ErrorRegistry): Error =>
  rehydrateError(error, registry);

/**
 * The write had already committed (its reply was lost with a connection, and the retry was
 * answered from the request ledger), but the caller asked for RETURNING rows, which are not kept.
 * `counts` are the original affected rows per table.
 */
export class CommittedWithoutResultError extends Error {
  readonly code = "MINNOW_COMMITTED_WITHOUT_RESULT";
  override readonly name = "CommittedWithoutResultError";
  readonly counts: Readonly<Record<string, number>>;
  constructor(counts: Readonly<Record<string, number>>) {
    super("The write committed, but its returned rows were lost with the connection");
    this.counts = counts;
  }
}

/**
 * Whether a write committed cannot be learned now.
 * - `unresponsive`: the owner went silent (`cause` is the `OwnerUnresponsiveError`) while the write
 *   was in flight. It may still commit when the owner thaws; the client keeps its seq unresolved
 *   and collects the terminal reply in the background, so the ledger stays exact.
 * - `expired`: no owner answered within the retry window (format/ledger.ts
 *   `CLIENT_RETRY_WINDOW_MS`), so the ledger can no longer say.
 * Reads never get this error: a silent owner fails them with `OwnerUnresponsiveError` itself.
 */
export class OutcomeUnknownError extends Error {
  readonly code = "MINNOW_OUTCOME_UNKNOWN";
  override readonly name = "OutcomeUnknownError";
  readonly reason: "unresponsive" | "expired";
  constructor(reason: "unresponsive" | "expired", options?: ErrorOptions) {
    super(
      reason === "unresponsive"
        ? "The database owner stopped answering; the write may still commit"
        : "No database owner answered within the retry window; the write may or may not have committed",
      options,
    );
    this.reason = reason;
  }
}

/**
 * Stopped by cancellation, with no effect. A caller that cancelled through an `AbortSignal` gets
 * the signal's reason instead (as `fetch` does); this class is what crosses the wire.
 */
export class CancelledError extends Error {
  readonly code = "MINNOW_CANCELLED";
  override readonly name = "CancelledError";
  constructor() {
    super("The request was cancelled before it took effect");
  }
}

/**
 * A newer build of the app owns the database, or is taking it over: this client's build can no
 * longer talk to it. In `whenSuperseded: "reload"` mode the page reloads (at most once a minute);
 * otherwise every call fails with this until the page reloads. (0.19: `StorageConnectionClosedError`
 * with `reason: "superseded"`.)
 */
export class SupersededError extends Error {
  readonly code = "MINNOW_SUPERSEDED";
  override readonly name = "SupersededError";
  readonly ownerProtocol: number;
  constructor(ownerProtocol: number) {
    super("A newer build of this app owns the database; reload the page");
    this.ownerProtocol = ownerProtocol;
  }
}

/** Another context owns the database and this one has no broker to reach it (no SharedWorker). */
export class OwnerUnreachableError extends Error {
  readonly code = "MINNOW_OWNER_UNREACHABLE";
  override readonly name = "OwnerUnreachableError";
  /** The request it failed, when it failed one (connector.ts "unresponsive"). */
  readonly sequence: number | null;
  /**
   * What is known about that request's effect, as `SessionLostError.effect`. `none`: it was never
   * sent on any connection, so nothing took effect (safe to retry). `unknown`: it was sent and
   * never answered; it may have taken effect: check before retrying. `none` when no request
   * failed (a connection attempt).
   */
  readonly effect: "none" | "unknown";
  constructor(
    detail: string,
    options: { readonly sequence?: number; readonly effect?: "none" | "unknown" } = {},
  ) {
    const effect = options.effect ?? "none";
    super(
      `Another tab owns the database and this context cannot reach it: ${detail}` +
        (options.sequence === undefined
          ? ""
          : effect === "none"
            ? `; request ${String(options.sequence)} was never sent, so nothing took effect`
            : `; request ${String(options.sequence)} was sent and never answered: it may have taken effect, check before retrying`),
    );
    this.sequence = options.sequence ?? null;
    this.effect = effect;
  }
}

/** The client was closed; nothing more can be sent. */
export class ClientClosedError extends Error {
  readonly code = "MINNOW_CLIENT_CLOSED";
  override readonly name = "ClientClosedError";
  constructor() {
    super("The database client is closed");
  }
}

/**
 * The session's transaction sat idle past `transactionIdleTimeoutMs` and was rolled back (ruling
 * R16.7; SQLSTATE 25P03, PostgreSQL's idle-in-transaction timeout). Every request on the session
 * fails with this until the client ends the transaction with ROLLBACK (or COMMIT, which commits
 * nothing). Nothing of the transaction took effect.
 */
export class TransactionIdleTimeoutError extends Error {
  readonly code = "MINNOW_TRANSACTION_IDLE_TIMEOUT";
  override readonly name = "TransactionIdleTimeoutError";
  readonly sqlState = "25P03";
  readonly timeoutMs: number;
  constructor(timeoutMs: number) {
    super(
      `The transaction was idle for more than ${String(timeoutMs)} ms and was rolled back; nothing of it took effect`,
    );
    this.timeoutMs = timeoutMs;
  }
}

/** SAVEPOINT, RELEASE or ROLLBACK TO outside a transaction (PostgreSQL 25P01). */
export class NoActiveTransactionError extends Error {
  readonly code = "MINNOW_NO_ACTIVE_TRANSACTION";
  override readonly name = "NoActiveTransactionError";
  readonly sqlState = "25P01";
  constructor(command: string) {
    super(`${command} can only be used in transaction blocks`);
  }
}

/** RELEASE or ROLLBACK TO a savepoint that does not exist (PostgreSQL 3B001). */
export class SavepointNotFoundError extends Error {
  readonly code = "MINNOW_SAVEPOINT_NOT_FOUND";
  override readonly name = "SavepointNotFoundError";
  readonly sqlState = "3B001";
  constructor(name: string) {
    super(`savepoint "${name}" does not exist`);
  }
}

/**
 * The owner refused the connection (protocol.ts `refused`): another protocol version, or the
 * owner is stepping down or superseded. The connector decides what follows (election.ts "Version
 * skew").
 */
export class ConnectionRefusedError extends Error {
  readonly code = "MINNOW_CONNECTION_REFUSED";
  override readonly name = "ConnectionRefusedError";
  readonly reason: "version" | "steppingDown" | "superseded" | "staleGeneration";
  readonly ownerProtocol: number;
  constructor(
    reason: "version" | "steppingDown" | "superseded" | "staleGeneration",
    ownerProtocol: number,
  ) {
    super(
      `The database owner refused the connection: ${reason} (owner protocol ${String(ownerProtocol)})`,
    );
    this.reason = reason;
    this.ownerProtocol = ownerProtocol;
  }
}

/** A statement inside a failed transaction (PostgreSQL 25P02): only ROLLBACK ends it. */
export class InFailedTransactionError extends Error {
  readonly code = "MINNOW_IN_FAILED_TRANSACTION";
  override readonly name = "InFailedTransactionError";
  readonly sqlState = "25P02";
  constructor() {
    super(
      "The current transaction is aborted; commands are ignored until the end of the transaction block",
    );
  }
}

/** A write inside a READ ONLY transaction (PostgreSQL 25006). */
export class ReadOnlyTransactionError extends Error {
  readonly code = "MINNOW_READ_ONLY_TRANSACTION";
  override readonly name = "ReadOnlyTransactionError";
  readonly sqlState = "25006";
  constructor(command: string) {
    super(`Cannot execute ${command} in a read-only transaction`);
  }
}

/** currval or lastval before nextval in this session (PostgreSQL 55000). */
export class SequenceStateError extends Error {
  readonly code = "MINNOW_SEQUENCE_STATE";
  override readonly name = "SequenceStateError";
  readonly sqlState = "55000";
  constructor(fn: "currval" | "lastval", sequenceId: number | null) {
    super(
      fn === "currval"
        ? `currval of sequence ${String(sequenceId)} is not yet defined in this session`
        : "lastval is not yet defined in this session",
    );
  }
}

/**
 * The answer to a duplicate of a request that failed: the ledger keeps the failure's code, not the
 * whole error, so the client gets this with the original `code` (format/ledger.ts `failed`).
 */
export class RecordedFailureError extends Error {
  override readonly name = "RecordedFailureError";
  readonly code: string;
  constructor(code: string) {
    super(`The request already failed with ${code}; this is its recorded outcome`);
    this.code = code;
  }
}

/** The classes above, keyed by name, for `fromWireError`. */
export const ownerErrorRegistry: ErrorRegistry = new Map<string, new (...args: never[]) => Error>([
  ["OwnerUnresponsiveError", OwnerUnresponsiveError],
  ["SessionLostError", SessionLostError],
  ["CommittedWithoutResultError", CommittedWithoutResultError],
  ["OutcomeUnknownError", OutcomeUnknownError],
  ["RequestExpiredError", RequestExpiredError],
  ["CancelledError", CancelledError],
  ["SupersededError", SupersededError],
  ["OwnerUnreachableError", OwnerUnreachableError],
  ["ClientClosedError", ClientClosedError],
  ["TransactionIdleTimeoutError", TransactionIdleTimeoutError],
  ["InFailedTransactionError", InFailedTransactionError],
  ["NoActiveTransactionError", NoActiveTransactionError],
  ["SavepointNotFoundError", SavepointNotFoundError],
  ["ReadOnlyTransactionError", ReadOnlyTransactionError],
  ["SequenceStateError", SequenceStateError],
  ["RecordedFailureError", RecordedFailureError],
  ["ConnectionRefusedError", ConnectionRefusedError],
]);
