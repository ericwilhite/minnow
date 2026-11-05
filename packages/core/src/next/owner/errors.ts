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
import { OwnerUnresponsiveError, RequestExpiredError } from "../errors.js";
import {
  rehydrateError,
  serializeError,
  type ErrorRegistry,
  type SerializedError,
} from "../../worker-protocol/index.js";

/** Defined once for every track (next/errors.ts); re-exported for the client API. */
export { OwnerUnresponsiveError, RequestExpiredError };

export type WireError = SerializedError;

export const toWireError = (error: unknown): WireError => serializeError(error);
export const fromWireError = (error: WireError, registry: ErrorRegistry): Error =>
  rehydrateError(error, registry);

/**
 * Connection state this request depended on is gone: the owner changed, stepped down, or dropped
 * the client's pins after its liveness lock was released (a page restored from bfcache). Nothing
 * of the request took effect. Open a new transaction, cursor or subscription.
 */
export class SessionLostError extends Error {
  readonly code = "MINNOW_SESSION_LOST";
  override readonly name = "SessionLostError";
  readonly lost: "transaction" | "cursor" | "subscription" | "result";
  constructor(lost: "transaction" | "cursor" | "subscription" | "result") {
    super(
      `The ${lost} was lost when the connection to the database owner changed; nothing took effect`,
    );
    this.lost = lost;
  }
}

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
  constructor(detail: string) {
    super(`Another tab owns the database and this context cannot reach it: ${detail}`);
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
]);
