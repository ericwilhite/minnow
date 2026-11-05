/**
 * Errors that cross module and track boundaries, defined once so every side (storage acquisition,
 * the owner, the client) throws and recognizes the same class and `code`.
 */

/**
 * The database owner did not respond in time: a frozen or hung owner still holds the storage
 * fence (storage/fence.ts acquisition), or did not answer a client (owner/client protocol). The
 * caller reports it and may retry later.
 */
export class OwnerUnresponsiveError extends Error {
  readonly code = "MINNOW_OWNER_UNRESPONSIVE";
  constructor(timeoutMs: number) {
    super(`The database owner did not respond within ${String(timeoutMs)} ms`);
    this.name = "OwnerUnresponsiveError";
  }
}

/**
 * A request arrived later than the retry window allows (format/ledger.ts `expired`). It was not
 * executed; its original attempt may or may not have committed, and that can no longer be known.
 */
export class RequestExpiredError extends Error {
  readonly code = "MINNOW_REQUEST_EXPIRED";
  constructor(sequence: number, ageMs: number) {
    super(
      `Request ${String(sequence)} expired: sent ${String(ageMs)} ms ago, past the retry window`,
    );
    this.name = "RequestExpiredError";
  }
}

/**
 * A statement of an explicit transaction arrived after its session's generation ended (a
 * reconnect, format/ledger.ts `sessionLost`). It was not executed: the transaction rolled back
 * with the old generation. The client starts the transaction again.
 */
export class SessionLostError extends Error {
  readonly code = "MINNOW_SESSION_LOST";
  constructor(sequence: number) {
    super(
      `Request ${String(sequence)} belongs to a session that ended; its transaction was rolled back`,
    );
    this.name = "SessionLostError";
  }
}

/**
 * The new core does not support a construct yet (ruling R17.6: one class for lowering and the
 * executor). SQLSTATE 0A000, feature_not_supported. The owner reports it as "not supported by the
 * new core"; the differential harness counts refusals per `construct` instead of treating them as
 * wrong answers.
 * - `stage: "lower"`: lowering refused the statement (lower/errors.ts lists the construct keys).
 * - `stage: "execute"`: the executor's `prepare` (or a kernel) lacks an operator, overload or type.
 */
export class UnsupportedPlanError extends Error {
  readonly code = "MINNOW_UNSUPPORTED_PLAN";
  readonly sqlState = "0A000";
  readonly stage: "lower" | "execute";
  /** A stable key for what is missing (lowering's construct keys; null when not classified). */
  readonly construct: string | null;
  constructor(
    what: string,
    options: { readonly stage?: "lower" | "execute"; readonly construct?: string } = {},
  ) {
    const stage = options.stage ?? "execute";
    super(
      stage === "execute"
        ? `The executor does not support ${what}`
        : `Not supported yet (${options.construct ?? "unclassified"}): ${what}`,
    );
    this.name = "UnsupportedPlanError";
    this.stage = stage;
    this.construct = options.construct ?? null;
  }
}
