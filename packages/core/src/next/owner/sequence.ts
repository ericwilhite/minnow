/**
 * The client's side of exactly-once (format/ledger.ts is the owner's): sequence allocation, the
 * `resolvedThrough` watermark, the outstanding window, and which requests may be resent after a
 * reconnect.
 *
 * ## Rules
 * - Sequences start at 1 and increase by one per request, in send order, for the client's whole
 *   page lifetime: across reconnects and owner changes.
 * - A seq is **resolved** when the client holds its terminal reply, or has failed it locally
 *   without ever sending it. (The public API's early `OutcomeUnknownError { reason:
 *   "unresponsive" }`, api.ts, increment 3, leaves a write unresolved: its reply is still
 *   collected.)
 * - `resolvedThrough` is the highest S with every seq ≤ S resolved. The owner uses it to drop
 *   outcomes the client holds (ledger `#acknowledge`).
 * - Window: a new seq is issued only while `seq ≤ resolvedThrough + maxOutstanding`, the bound the
 *   ledger enforces (`window`). Beyond it, the client queues new requests locally.
 * - Expiry: a retry sent more than the retry window after its original is refused by the owner
 *   with `RequestExpiredError` (next/errors.ts), never answered from the ledger; that error says
 *   the original may have committed. The client resolves the seq on that reply. It has no expiry
 *   timer of its own that would fail a request without a reply.
 *
 * ## Resendable (protocol.ts "Exactly-once")
 * A request is resendable when running it on a fresh connection means what it meant when sent:
 * - statements and writes on session 0 while session 0 is known idle: no transaction open, and no
 *   earlier session-0 request unresolved that may change that (its kind is not `other`: see
 *   `classifyStatementText`), since it could have been `BEGIN`. Concurrent autocommit statements stay resendable;
 * - `prepare`, `subscribe`, `admin`.
 * Everything that depends on session state is not: statements and writes in sessions ≥ 1, or in
 * session 0 inside (or possibly inside) a transaction; `begin`, `commit`, `rollback`. On a new
 * connection, a non-resendable request is answered from the ledger if it committed, and is
 * otherwise refused with `SessionLostError`, untouched.
 *
 * Resendable is about meaning, not about exactly-once: a resendable query is still
 * at-least-once (outcomes.ts). Only commits are exactly-once.
 */
import { MAX_OUTSTANDING_REQUESTS } from "../format/ledger.js";
import type { PreparedStatementKind, Request, TransactionState } from "./protocol.js";

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
  readonly transaction: TransactionState;
  /** Session-0 requests sent, unresolved, for which `mayControlTransaction` was true. */
  readonly unresolvedControl: number;
}

/** Whether `request` (its header aside) is resendable. See the module comment. */
export function isResendable(
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

/**
 * A statement's effect on its session's transaction (protocol.ts `PreparedStatementKind`), or
 * `unknown` when the client cannot tell: more than one statement, or text it cannot lex.
 */
export type StatementKind = PreparedStatementKind | "unknown";

type Token =
  | { readonly kind: "word"; readonly upper: string }
  | { readonly kind: "semicolon" }
  | { readonly kind: "other" };

/**
 * The front end's lexical rules (engine/query.ts `tokenize`, `normalizeSql`), as far as finding
 * the leading keyword and statement separators needs: whitespace; `--` comments to the end of the
 * line; bracketed comments, not nested (the first `* /` ends one, as in the front end); string
 * literals `'…'` (`''` escapes), `E'…'` (backslash escapes) and `$tag$…$tag$`; quoted identifiers
 * `"…"`, which the front end routes like keywords when they spell one; bare identifiers. Returns
 * null for text the front end would refuse to lex (an unterminated literal or comment).
 */
function lex(sql: string): Token[] | null {
  const tokens: Token[] = [];
  let i = 0;
  const n = sql.length;
  while (i < n) {
    const c = sql[i] ?? "";
    if (/\s/u.test(c)) {
      i++;
    } else if ((c === "E" || c === "e") && sql[i + 1] === "'") {
      i += 2;
      for (;;) {
        if (i >= n) return null;
        const d = sql[i];
        if (d === "'" && sql[i + 1] === "'") i += 2;
        else if (d === "'") {
          i++;
          break;
        } else if (d === "\\") i += 2;
        else i++;
      }
      tokens.push({ kind: "other" });
    } else if (/[A-Za-z_]/u.test(c)) {
      const begin = i++;
      while (i < n && /[A-Za-z0-9_]/u.test(sql[i] ?? "")) i++;
      tokens.push({ kind: "word", upper: sql.slice(begin, i).toUpperCase() });
    } else if (c === "'" || c === '"') {
      i++;
      let text = "";
      for (;;) {
        if (i >= n) return null;
        if (sql[i] === c && sql[i + 1] === c) {
          text += c;
          i += 2;
        } else if (sql[i] === c) {
          i++;
          break;
        } else text += sql[i++] ?? "";
      }
      tokens.push(c === '"' ? { kind: "word", upper: text.toUpperCase() } : { kind: "other" });
    } else if (c === "$") {
      const tag = /^\$([A-Za-z_][A-Za-z0-9_]*)?\$/u.exec(sql.slice(i));
      if (tag !== null) {
        const close = sql.indexOf(tag[0], i + tag[0].length);
        if (close < 0) return null;
        i = close + tag[0].length;
      } else {
        i++;
        while (i < n && /\d/u.test(sql[i] ?? "")) i++;
      }
      tokens.push({ kind: "other" });
    } else if (sql.startsWith("--", i)) {
      const newline = sql.indexOf("\n", i);
      i = newline < 0 ? n : newline + 1;
    } else if (sql.startsWith("/*", i)) {
      const close = sql.indexOf("*/", i + 2);
      if (close < 0) return null;
      i = close + 2;
    } else if (c === ";") {
      tokens.push({ kind: "semicolon" });
      i++;
    } else {
      tokens.push({ kind: "other" });
      i++;
    }
  }
  return tokens;
}

/**
 * What SQL `text` does to its session's transaction, classified as the front end routes it
 * (engine/query.ts `compileStatement`, `parseTransactionStatement`): BEGIN, START [TRANSACTION]
 * → `begin`; COMMIT, END → `commit`; ROLLBACK, ABORT → `rollback`; ROLLBACK [WORK |
 * TRANSACTION] TO, SAVEPOINT, RELEASE → `savepoint`; any other leading word → `other`. Text with a statement separator
 * other than one trailing `;` (the front end strips exactly one) is `unknown`, as is text the
 * front end could not lex. The suite `owner-statement-kind.test.ts` checks it against the front
 * end itself. Prepared statements take their kind from the owner instead (`Prepared`).
 */
export function classifyStatementText(text: string): StatementKind {
  const normalized = text.trim().replace(/;$/u, "").trim();
  const tokens = lex(normalized);
  if (tokens === null) return "unknown";
  if (tokens.some((token) => token.kind === "semicolon")) return "unknown";
  const first = tokens[0];
  if (first?.kind !== "word") return "other";
  switch (first.upper) {
    case "BEGIN":
    case "START":
      return "begin";
    case "COMMIT":
    case "END":
      return "commit";
    case "ABORT":
      return "rollback";
    case "ROLLBACK": {
      // ROLLBACK [WORK | TRANSACTION] TO [SAVEPOINT] name undoes to a savepoint; the front end
      // accepts only the bare spelling and refuses the others, but none of them ends the
      // transaction, so none is a rollback.
      let next = tokens[1];
      if (next?.kind === "word" && (next.upper === "WORK" || next.upper === "TRANSACTION"))
        next = tokens[2];
      return next?.kind === "word" && next.upper === "TO" ? "savepoint" : "rollback";
    }
    case "SAVEPOINT":
    case "RELEASE":
      return "savepoint";
    default:
      return "other";
  }
}

/**
 * Whether SQL `text` is a BEGIN or START TRANSACTION whose modes include READ ONLY, read as the
 * front end reads them: it validates a transaction statement's identifier words only, ignoring
 * punctuation (engine/query.ts `parseTransactionStatement`), so `BEGIN READ, ONLY` and
 * `BEGIN READ (ONLY)` are READ ONLY too. This follows the front end's token semantics; it is not
 * a grammar, and it is temporary: the owner's refusal it serves goes once lowering reports a
 * BEGIN's access mode (B6).
 */
export function requestsReadOnly(text: string): boolean {
  if (classifyStatementText(text) !== "begin") return false;
  const words = (lex(text.trim().replace(/;$/u, "").trim()) ?? []).filter(
    (token): token is Extract<Token, { kind: "word" }> => token.kind === "word",
  );
  return words.some((word, i) => word.upper === "READ" && words[i + 1]?.upper === "ONLY");
}

/**
 * Whether SQL `text` may open, end or change a transaction (`classifyStatementText` is not
 * `other`): such a request is not resendable on session 0 while it is unresolved.
 */
export function mayControlTransaction(text: string): boolean {
  return classifyStatementText(text) !== "other";
}
