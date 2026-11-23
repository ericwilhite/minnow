/**
 * Whether a statement next/ could not lower takes its transaction's snapshot, decided from its
 * first words (owner/compose.ts `statementFailed`, `prepared`). A statement that lowers decides
 * by its planned kind instead (compose.ts `#guard`).
 *
 * PostgreSQL 18 takes a transaction's snapshot for a statement once its raw parse succeeds: at
 * parse analysis for the statements `analyze_requires_snapshot` accepts (queries, data changes,
 * EXPLAIN, DECLARE, CREATE TABLE AS, CALL), and when a utility statement runs for every other one
 * `PlannedStmtRequiresSnapshot` accepts (utility.c). next/'s parser reads a subset of
 * PostgreSQL's grammar, so its failure says nothing about PostgreSQL's raw parse: a failed
 * statement counts as raw-parsed, and its first word decides. A statement PostgreSQL rejects
 * before its snapshot (a raw syntax error, or several statements in one request) takes the
 * snapshot in next/ unless its first word is one of the no-snapshot words: the divergence
 * `syntax-error-snapshot` (testing/divergences.ts).
 */

/**
 * The first words of the statements `PlannedStmtRequiresSnapshot` (utility.c, PostgreSQL 18)
 * refuses: TransactionStmt (BEGIN, START, COMMIT, END, ROLLBACK, ABORT, SAVEPOINT, RELEASE,
 * PREPARE TRANSACTION, COMMIT PREPARED, ROLLBACK PREPARED), LockStmt, VariableSetStmt (SET,
 * RESET), VariableShowStmt, ConstraintsSetStmt (SET CONSTRAINTS), FetchStmt (FETCH, MOVE),
 * ListenStmt, NotifyStmt, UnlistenStmt and CheckPointStmt. PREPARE needs its second word:
 * `PREPARE name AS ...` takes one.
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
 * DeleteStmt, MergeStmt, ExplainStmt, DeclareCursorStmt and CallStmt. CREATE TABLE AS is one
 * too, but its first word is CREATE, as every other CREATE's.
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

/**
 * Whether a statement that failed to lower takes the snapshot when it runs: it counts as
 * raw-parsed, and takes one unless its first word starts a statement that takes none. Text with
 * no first word (empty, only comments or semicolons) is an empty query: none.
 */
export function failedStatementTakesSnapshot(text: string): boolean {
  const [first, second] = leadingWords(text, 2);
  if (first === undefined) return false;
  if (first === "PREPARE") return second !== "TRANSACTION";
  return !NO_SNAPSHOT_WORDS.has(first);
}

/**
 * Whether a statement that failed to lower takes the snapshot when it is prepared (an
 * extended-protocol Parse): when its first word starts a statement PostgreSQL analyses at Parse.
 */
export function failedPrepareTakesSnapshot(text: string): boolean {
  const [first] = leadingWords(text, 1);
  return first !== undefined && ANALYZED_WORDS.has(first);
}

/**
 * The statement's first `count` words, upper-cased, as PostgreSQL's scanner reaches them:
 * whitespace, semicolons (empty statements), `--` comments ending at a line feed or a carriage
 * return, and nested block comments are skipped before the first word; whitespace and comments
 * between words. A leading parenthesis is the word `(`. Fewer words when the text has fewer,
 * or something else comes first (an unterminated comment ends the text).
 */
export function leadingWords(text: string, count: number): string[] {
  const words: string[] = [];
  let index = 0;
  const skip = (semicolons: boolean): void => {
    for (;;) {
      const c = text[index];
      if (c === undefined) return;
      if (c === " " || c === "\t" || c === "\n" || c === "\r" || c === "\f" || c === "\v")
        index += 1;
      else if (semicolons && c === ";") index += 1;
      else if (c === "-" && text[index + 1] === "-") {
        index += 2;
        while (index < text.length && text[index] !== "\n" && text[index] !== "\r") index += 1;
      } else if (c === "/" && text[index + 1] === "*") {
        let depth = 0;
        while (index < text.length) {
          if (text[index] === "/" && text[index + 1] === "*") {
            depth += 1;
            index += 2;
          } else if (text[index] === "*" && text[index + 1] === "/") {
            depth -= 1;
            index += 2;
            if (depth === 0) break;
          } else index += 1;
        }
      } else return;
    }
  };
  skip(true);
  while (words.length < count) {
    if (text[index] === "(") {
      words.push("(");
      index += 1;
    } else {
      const word = WORD.exec(text.slice(index))?.[0];
      if (word === undefined) break;
      words.push(word.toUpperCase());
      index += word.length;
    }
    skip(false);
  }
  return words;
}

/** An unquoted identifier or keyword (scan.l `identifier`: a letter, `_` or a non-ASCII character first). */
const WORD = /^[A-Za-z_\u0080-￿][A-Za-z0-9_$\u0080-￿]*/u;
