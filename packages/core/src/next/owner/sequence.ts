/**
 * The client's side of exactly-once (format/ledger.ts is the owner's): sequence allocation, the
 * `resolvedThrough` watermark, the outstanding window, and which requests may be resent after a
 * reconnect.
 *
 * ## Rules
 * - Sequences start at 1 and increase by one per request, in send order, for the client's whole
 *   page lifetime: across reconnects and owner changes.
 * - A seq is **resolved** when the client holds its terminal reply, or has failed it locally
 *   without ever sending it. A request failed early with `OwnerUnresponsiveError { outcome:
 *   "unknown" }` is **not** resolved: its reply is still collected (or it expires, below).
 * - `resolvedThrough` is the highest S with every seq ≤ S resolved. The owner uses it to drop
 *   outcomes the client holds (ledger `#acknowledge`).
 * - Window: a new seq is issued only while `seq ≤ resolvedThrough + maxOutstanding`, the bound the
 *   ledger enforces (`window`). Beyond it, the client queues new requests locally.
 * - Expiry: a seq unresolved for longer than the retry window can never be answered from the
 *   ledger (the owner refuses it as `expired`, or pruned its outcome). The client then resolves it
 *   and fails it with `OutcomeUnknownError`, so one lost reply cannot stall the window forever.
 *
 * ## Standalone (protocol.ts "Exactly-once")
 * A request is standalone when running it on a fresh connection means what it meant when sent:
 * - statements and writes on session 0 while session 0 is known idle: no transaction open, and no
 *   earlier session-0 request unresolved that may change that (`mayControlTransaction`), since it
 *   could have been `BEGIN`. Concurrent autocommit statements stay standalone;
 * - `prepare`, `subscribe`, `admin`.
 * Everything that depends on session state is not: statements and writes in sessions ≥ 1, or in
 * session 0 inside (or possibly inside) a transaction; `begin`, `commit`, `rollback`. On a new
 * connection, a non-standalone request is answered from the ledger if it committed, and is
 * otherwise refused with `SessionLostError`, untouched.
 */
import { MAX_OUTSTANDING_REQUESTS } from "../format/ledger.js";
import type { Request } from "./protocol.js";

export class RequestSequencer {
  readonly #maxOutstanding: number;
  #next = 1;
  #resolvedThrough = 0;
  /** Resolved seqs above `#resolvedThrough`. */
  readonly #resolvedAbove = new Set<number>();

  constructor(maxOutstanding: number = MAX_OUTSTANDING_REQUESTS) {
    if (!Number.isSafeInteger(maxOutstanding) || maxOutstanding < 1)
      throw new RangeError("maxOutstanding must be a positive integer");
    this.#maxOutstanding = maxOutstanding;
  }

  get resolvedThrough(): number {
    return this.#resolvedThrough;
  }

  /** Issued seqs not yet resolved. */
  get outstanding(): number {
    return this.#next - 1 - this.#resolvedThrough - this.#resolvedAbove.size;
  }

  /** Whether `issue` would succeed now. */
  get canIssue(): boolean {
    return this.#next <= this.#resolvedThrough + this.#maxOutstanding;
  }

  /** The next seq, or null when the window is full (queue the request locally). */
  issue(): number | null {
    if (!this.canIssue) return null;
    return this.#next++;
  }

  /** Marks `seq` resolved. Idempotent; throws for a seq never issued. */
  resolve(seq: number): void {
    if (!Number.isSafeInteger(seq) || seq < 1 || seq >= this.#next)
      throw new RangeError(`Sequence ${String(seq)} was never issued`);
    if (seq <= this.#resolvedThrough) return;
    this.#resolvedAbove.add(seq);
    while (this.#resolvedAbove.delete(this.#resolvedThrough + 1)) this.#resolvedThrough++;
  }
}

/** What the client knows about session 0 when it sends a request. */
export interface DefaultSessionView {
  /** Session 0's transaction state from the latest `done.summary.transaction`. */
  readonly transaction: "idle" | "open" | "failed";
  /** Session-0 requests sent, unresolved, for which `mayControlTransaction` was true. */
  readonly unresolvedControl: number;
}

/** Whether `request` (its header aside) is standalone. See the module comment. */
export function isStandalone(
  request: { readonly t: Request["t"]; readonly session?: number },
  defaultSession: DefaultSessionView,
): boolean {
  switch (request.t) {
    case "prepare":
    case "subscribe":
    case "admin":
      return true;
    case "begin":
    case "commit":
    case "rollback":
      return false;
    case "statement":
    case "write":
      return (
        request.session === 0 &&
        defaultSession.transaction === "idle" &&
        defaultSession.unresolvedControl === 0
      );
  }
}

const CONTROL_WORDS = new Set([
  "begin",
  "start",
  "commit",
  "end",
  "rollback",
  "abort",
  "savepoint",
  "release",
]);

/**
 * Whether SQL `text` may open, end or change a transaction: its first keyword is a
 * transaction-control word, or it may hold more than one statement (any `;` before trailing
 * whitespace and semicolons, even inside a literal: false positives only cost standalone-ness).
 * Prepared statements get this from the owner instead (`Prepared.controlsTransaction`).
 */
export function mayControlTransaction(text: string): boolean {
  let end = text.length;
  while (end > 0 && /[\s;]/u.test(text[end - 1] ?? "")) end--;
  if (text.slice(0, end).includes(";")) return true;
  let i = 0;
  for (;;) {
    while (i < end && /\s/u.test(text[i] ?? "")) i++;
    if (text.startsWith("--", i)) {
      const newline = text.indexOf("\n", i);
      i = newline < 0 ? end : newline + 1;
    } else if (text.startsWith("/*", i)) {
      let depth = 1;
      i += 2;
      while (i < end && depth > 0) {
        if (text.startsWith("/*", i)) {
          depth++;
          i += 2;
        } else if (text.startsWith("*/", i)) {
          depth--;
          i += 2;
        } else i++;
      }
    } else break;
  }
  const word = /^[A-Za-z_]+/u.exec(text.slice(i, Math.min(end, i + 16)));
  return word !== null && CONTROL_WORDS.has(word[0].toLowerCase());
}
