/**
 * Whether a statement next/ could not lower takes its transaction's snapshot, decided from its
 * first tokens (owner/compose.ts `statementFailed`, `prepared`). A statement that lowers decides
 * by its planned kind instead (compose.ts `#guard`, `prepared`).
 *
 * PostgreSQL 18 takes a transaction's snapshot for a statement once its raw parse succeeds: at
 * parse analysis for the statements `analyze_requires_snapshot` accepts (queries, data changes,
 * EXPLAIN, DECLARE, CREATE TABLE AS, CREATE MATERIALIZED VIEW, CALL), and when a utility
 * statement runs for every other one `PlannedStmtRequiresSnapshot` accepts (utility.c). next/'s
 * parser reads a subset of PostgreSQL's grammar, so its failure says nothing about PostgreSQL's
 * raw parse: a failed statement counts as raw-parsed, and its first tokens decide. A statement
 * PostgreSQL rejects before its snapshot (a raw syntax error, or several statements in one
 * request) takes the snapshot in next/ unless its first word starts a statement that takes none:
 * the divergence `syntax-error-snapshot` (testing/divergences.ts).
 */

/**
 * The first words of the statements `PlannedStmtRequiresSnapshot` (utility.c, PostgreSQL 18)
 * refuses: TransactionStmt (BEGIN, START, COMMIT, END, ROLLBACK, ABORT, SAVEPOINT, RELEASE,
 * PREPARE TRANSACTION, COMMIT PREPARED, ROLLBACK PREPARED), LockStmt, VariableSetStmt (SET,
 * RESET), VariableShowStmt, ConstraintsSetStmt (SET CONSTRAINTS), FetchStmt (FETCH, MOVE),
 * ListenStmt, NotifyStmt, UnlistenStmt and CheckPointStmt. PREPARE is decided by its third
 * token (`noSnapshotPrepare`).
 */
const NO_SNAPSHOT_WORDS = new Set([
  "ABORT",
  "BEGIN",
  "CHECKPOINT",
  "COMMIT",
  "END",
  "FETCH",
  "LISTEN",
  "LOCK",
  "MOVE",
  "NOTIFY",
  "RELEASE",
  "RESET",
  "ROLLBACK",
  "SAVEPOINT",
  "SET",
  "SHOW",
  "START",
  "UNLISTEN",
]);

/**
 * The first words of the statements `analyze_requires_snapshot` (analyze.c,
 * `stmt_requires_parse_analysis`) accepts, which take the snapshot when an extended-protocol
 * Parse analyses them: SelectStmt (`(`, SELECT, VALUES, TABLE, WITH), InsertStmt, UpdateStmt,
 * DeleteStmt, MergeStmt, ExplainStmt, DeclareCursorStmt and CallStmt. CreateTableAsStmt starts
 * with CREATE, as every CREATE does: `createTableAs` reads past it.
 */
const ANALYZED_WORDS = new Set([
  "(",
  "CALL",
  "DECLARE",
  "DELETE",
  "EXPLAIN",
  "INSERT",
  "MERGE",
  "SELECT",
  "TABLE",
  "UPDATE",
  "VALUES",
  "WITH",
]);

/** One token as PostgreSQL's scanner reads it, enough to tell statements apart by their start. */
export interface LeadingToken {
  /** `word`: an unquoted keyword or name, ASCII upper-cased as the scanner folds keywords;
   * `string`: a string constant; `name`: a quoted identifier; `punct`: `(`, `)` or `;`. */
  readonly kind: "word" | "string" | "name" | "punct" | "other";
  readonly text: string;
  /** Parenthesis depth before this token. */
  readonly depth: number;
}

/**
 * Whether a statement that failed to lower takes the snapshot when it runs: it counts as
 * raw-parsed, and takes one unless its first word starts a statement that takes none. Text with
 * no first token (empty, only comments or semicolons) is an empty query: none.
 */
export function failedStatementTakesSnapshot(text: string): boolean {
  const [first, second, third] = statementTokens(text, 3);
  if (first === undefined) return false;
  if (first.kind === "word" && first.text === "PREPARE") return !noSnapshotPrepare(second, third);
  return !(first.kind === "word" && NO_SNAPSHOT_WORDS.has(first.text));
}

/**
 * PREPARE TRANSACTION Sconst (TransactionStmt, no snapshot) against PrepareStmt (`PREPARE name
 * [(types)] AS ...`, which takes one), told apart as gram.y does: TRANSACTION is an unreserved
 * keyword, so a prepared statement may be named `transaction`; only the third token decides.
 */
function noSnapshotPrepare(second?: LeadingToken, third?: LeadingToken): boolean {
  return second?.kind === "word" && second.text === "TRANSACTION" && third?.kind === "string";
}

/**
 * Whether a statement that failed to lower takes the snapshot when it is prepared (an
 * extended-protocol Parse): when it starts a statement PostgreSQL analyses at Parse.
 */
export function failedPrepareTakesSnapshot(text: string): boolean {
  // The first token decides every statement but CREATE: read only it (a failed prepare of a
  // megabyte INSERT must not tokenize the megabyte).
  const [first] = statementTokens(text, 1);
  if (first === undefined) return false;
  if (first.text === "(" && first.kind === "punct") return true;
  if (first.kind !== "word") return false;
  if (ANALYZED_WORDS.has(first.text)) return true;
  if (first.text !== "CREATE") return false;
  // Then only CREATE [LOCAL | GLOBAL] [TEMP | TEMPORARY | UNLOGGED] { TABLE | MATERIALIZED }
  // reads further: its AS can be anywhere after TABLE.
  const head = statementTokens(text, 5);
  return createTableHead(head) && createTableAs(statementTokens(text, Infinity));
}

/** Whether `tokens` (CREATE and the next four) start a TABLE or MATERIALIZED VIEW. */
function createTableHead(tokens: readonly LeadingToken[]): boolean {
  return tokens
    .slice(1, 5)
    .some(
      (token) => token.kind === "word" && (token.text === "TABLE" || token.text === "MATERIALIZED"),
    );
}

/**
 * CREATE [ { TEMPORARY | TEMP } | { LOCAL | GLOBAL } { TEMPORARY | TEMP } | UNLOGGED ]
 * { TABLE ... AS | MATERIALIZED VIEW } (gram.y CreateAsStmt, CreateMatViewStmt): a table is
 * created from a query when an AS stands outside every parenthesis after TABLE (a CreateStmt's
 * AS only appears inside its element list, as in GENERATED ALWAYS AS).
 */
function createTableAs(tokens: readonly LeadingToken[]): boolean {
  let at = 1;
  const word = (index: number) => {
    const token = tokens[index];
    return token?.kind === "word" ? token.text : undefined;
  };
  if (word(at) === "LOCAL" || word(at) === "GLOBAL") at++;
  if (word(at) === "TEMP" || word(at) === "TEMPORARY" || word(at) === "UNLOGGED") at++;
  if (word(at) === "MATERIALIZED") return word(at + 1) === "VIEW";
  if (word(at) !== "TABLE") return false;
  for (let index = at + 1; index < tokens.length; index++) {
    const token = tokens[index];
    if (token?.depth === 0 && token.kind === "word" && token.text === "AS") return true;
  }
  return false;
}

/**
 * The first statement's tokens, at most `count`: leading whitespace, semicolons (empty
 * statements) and comments skipped (a `--` comment ends at a line feed or a carriage return;
 * block comments nest), whitespace and comments between tokens; it ends at a top-level `;`.
 * Unterminated text runs to the end.
 */
export function statementTokens(text: string, count: number): LeadingToken[] {
  const tokens: LeadingToken[] = [];
  let index = 0;
  let depth = 0;
  const skip = (): void => {
    for (;;) {
      const c = text.charCodeAt(index);
      // Space, tab, line feed, vertical tab, form feed, carriage return.
      if (c === 32 || (c >= 9 && c <= 13)) index += 1;
      else if (c === 45 && text.charCodeAt(index + 1) === 45) {
        index += 2;
        while (index < text.length && text[index] !== "\n" && text[index] !== "\r") index += 1;
      } else if (c === 47 && text.charCodeAt(index + 1) === 42) {
        let nesting = 0;
        while (index < text.length) {
          if (text[index] === "/" && text[index + 1] === "*") {
            nesting += 1;
            index += 2;
          } else if (text[index] === "*" && text[index + 1] === "/") {
            nesting -= 1;
            index += 2;
            if (nesting === 0) break;
          } else index += 1;
        }
      } else return;
    }
  };
  // Empty statements before the first.
  for (skip(); text[index] === ";"; skip()) index += 1;
  while (tokens.length < count && index < text.length) {
    const start = index;
    const c = text[index] ?? "";
    let kind: LeadingToken["kind"] = "other";
    if (c === "'") {
      index = quoted(text, index, "'", false);
      kind = "string";
    } else if ((c === "E" || c === "e") && text[index + 1] === "'") {
      index = quoted(text, index + 1, "'", true);
      kind = "string";
    } else if ((c === "U" || c === "u") && text[index + 1] === "&" && text[index + 2] === "'") {
      index = quoted(text, index + 2, "'", false);
      kind = "string";
    } else if (c === '"') {
      index = quoted(text, index, '"', false);
      kind = "name";
    } else if (c === "$" && !/[0-9]/u.test(text[index + 1] ?? "")) {
      DOLLAR_TAG.lastIndex = index;
      const tag = DOLLAR_TAG.exec(text)?.[0];
      if (tag === undefined) index += 1;
      else {
        const close = text.indexOf(tag, index + tag.length);
        index = close < 0 ? text.length : close + tag.length;
        kind = "string";
      }
    } else if (c === "(" || c === ")" || c === ";") {
      if (c === ";" && depth === 0) break;
      index += 1;
      kind = "punct";
    } else {
      WORD.lastIndex = index;
      const word = WORD.exec(text)?.[0];
      if (word !== undefined) {
        index += word.length;
        kind = "word";
      } else index += 1;
    }
    const raw = text.slice(start, index);
    tokens.push({ kind, text: kind === "word" ? asciiUpper(raw) : raw, depth });
    if (raw === "(" && kind === "punct") depth += 1;
    else if (raw === ")" && kind === "punct") depth = Math.max(0, depth - 1);
    skip();
  }
  return tokens;
}

/** The index past a quoted token opening at `from` (doubled quotes; backslash escapes in E''). */
function quoted(text: string, from: number, quote: string, backslash: boolean): number {
  let index = from + 1;
  while (index < text.length) {
    if (backslash && text[index] === "\\") index += 2;
    else if (text[index] === quote && text[index + 1] === quote) index += 2;
    else if (text[index] === quote) return index + 1;
    else index += 1;
  }
  return text.length;
}

/** PostgreSQL folds only ASCII letters of a keyword (`ſelect` is no SELECT). */
function asciiUpper(word: string): string {
  return word.replace(/[a-z]+/gu, (letters) => letters.toUpperCase());
}

/** An unquoted keyword or name (scan.l `identifier`: a letter, `_` or a non-ASCII character
 * first), read in place (sticky). */
const WORD = /[A-Za-z_\u0080-\u{10ffff}][A-Za-z0-9_$\u0080-\u{10ffff}]*/uy;
/** A dollar-quote tag (scan.l `dolqdelim`), read in place. */
const DOLLAR_TAG = /\$(?:[A-Za-z_\u0080-\u{10ffff}][A-Za-z0-9_\u0080-\u{10ffff}]*)?\$/uy;
