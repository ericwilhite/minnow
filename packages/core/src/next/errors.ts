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
