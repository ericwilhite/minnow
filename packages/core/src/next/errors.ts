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
 * Connection state a request depended on is gone. Whether anything of the request took effect is
 * its `effect` (below): `none` only when that is proven, `unknown` otherwise. What was lost:
 * - `transaction`: a statement of an explicit transaction arrived after its session's generation
 *   ended (a reconnect, format/ledger.ts `sessionLost`), or the owner changed under it. The
 *   session refuses work until the client sends ROLLBACK and the owner confirms it.
 * - `cursor`, `subscription`, `result`: the owner changed or dropped the client's pins (liveness
 *   lock released, a page restored from bfcache, an abandoned stream). Open it again.
 * One class for every track (ruling R18.1): the owner re-exports it and rehydrates it on the wire.
 */
export class SessionLostError extends Error {
  readonly code = "MINNOW_SESSION_LOST";
  readonly lost: "transaction" | "cursor" | "subscription" | "result";
  /** The request's sequence, when one request was refused. */
  readonly sequence: number | null;
  /**
   * What is known about the request's effect. `none`: proven to have changed nothing (it was
   * never sent, it ran only inside a transaction that rolled back, or the ledger has no commit
   * for it). `unknown`: it may have taken effect (a multi-statement text, a result whose statement
   * had passed its commit point): check before retrying.
   */
  readonly effect: "none" | "unknown";
  constructor(
    lost: "transaction" | "cursor" | "subscription" | "result",
    sequence?: number,
    effect: "none" | "unknown" = "none",
  ) {
    const outcome =
      (effect === "none"
        ? "nothing took effect"
        : "it may have taken effect before the connection changed: check before retrying") +
      (lost === "transaction" ? "; send ROLLBACK to use the session again" : "");
    super(
      sequence === undefined
        ? `The ${lost} was lost when the connection to the database owner changed; ${outcome}`
        : `Request ${String(sequence)} belongs to a ${lost} that was lost when the connection to the database owner changed; ${outcome}`,
    );
    this.name = "SessionLostError";
    this.lost = lost;
    this.sequence = sequence ?? null;
    this.effect = effect;
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
