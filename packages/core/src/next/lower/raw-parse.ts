/**
 * Whether PostgreSQL's raw parser certainly rejects a statement next/'s front end failed to read:
 * the part of a failed statement's snapshot decision (create.ts `takesSnapshot`) the front end
 * cannot give. Its parser reads a subset of PostgreSQL's grammar, so its failing to read on proves
 * nothing: a statement counts as parsed unless one of the rules below, each a place where gram.y
 * (or scan.l) has no alternative, rejects it over the tokenizer's output. It lives in next/, not
 * in engine/query.ts, so the shipped engine's bundles do not carry it.
 * testing/grammar-verdict-pglite.test.ts holds every rule to PostgreSQL over a seeded corpus.
 */
import { tokenize, type Token } from "../../engine/query.js";

/** The first words of PostgreSQL's statements (gram.y `stmt`, PostgreSQL 18). */
const STATEMENT_WORDS = new Set(
  "ABORT ALTER ANALYSE ANALYZE BEGIN CALL CHECKPOINT CLOSE CLUSTER COMMENT COMMIT COPY CREATE DEALLOCATE DECLARE DELETE DISCARD DO DROP END EXECUTE EXPLAIN FETCH GRANT IMPORT INSERT LISTEN LOAD LOCK MERGE MOVE NOTIFY PREPARE REASSIGN REFRESH REINDEX RELEASE RESET REVOKE ROLLBACK SAVEPOINT SECURITY SELECT SET SHOW START TABLE TRUNCATE UNLISTEN UPDATE VACUUM VALUES WITH".split(
    " ",
  ),
);

/** The statements PostgreSQL reads as one word. */
const ONE_WORD_STATEMENTS = new Set(
  "ABORT ANALYSE ANALYZE BEGIN CHECKPOINT CLUSTER COMMIT END ROLLBACK SELECT VACUUM".split(" "),
);

/** The first words of queries and data changes. */
const QUERY_WORDS = new Set("( SELECT WITH VALUES TABLE INSERT UPDATE DELETE MERGE".split(" "));

/** Reserved keywords no bare column label takes (gram.y `bare_label_keyword` lacks them). */
const NEVER_FINAL_WORDS = new Set(
  "WHERE FROM ON HAVING AS INTO UNION INTERSECT EXCEPT LIMIT OFFSET WITH".split(" "),
);

/**
 * Type keywords whose modifier is one integer constant (CharacterWithLength, ConstDatetime,
 * ConstInterval, opt_float), and those that take none (Numeric): none is a function name.
 */
const INTEGER_MODIFIER_TYPES = new Set(
  "CHAR CHARACTER VARCHAR NCHAR TIME TIMESTAMP INTERVAL FLOAT".split(" "),
);
const UNMODIFIED_TYPES = new Set("INT INTEGER SMALLINT BIGINT REAL BOOLEAN".split(" "));

/** scan.l's operator characters: any other character the front end refuses fails the scanner. */
const OPERATOR_CHARACTERS = "~!@#^&|`?+-*/%<>=";

/** Tokenizer failures that are scan.l's: unterminated quotes and comments, and the like. */
const LEXICAL_FAILURE =
  /^(?:Unterminated|Quoted identifiers cannot be empty|Expected a parameter number|Invalid number)/u;

/** Whether PostgreSQL's raw parser certainly rejects `sql`. */
export function rawParseRejects(sql: string): boolean {
  let all: Token[];
  try {
    all = tokenize(sql);
  } catch (error) {
    const message = error instanceof Error ? error.message : "";
    const character = /^Unsupported SQL character: (.)$/su.exec(message)?.[1];
    return (
      LEXICAL_FAILURE.test(message) ||
      (character !== undefined && !OPERATOR_CHARACTERS.includes(character))
    );
  }
  const tokens = all.filter((token) => token.kind !== "eof");
  while (tokens.at(-1)?.text === ";" && tokens.at(-1)?.kind === "punctuation") tokens.pop();
  const count = tokens.length;
  if (count === 0) return false;
  /** A keyword (an unquoted word, upper-cased) or a punctuation or operator; else undefined. */
  const at = (index: number): string | undefined => {
    const token = tokens[index];
    if (token === undefined || token.kind === "string" || token.kind === "number") return undefined;
    if (token.kind === "parameter" || token.quoted === true) return undefined;
    return token.kind === "identifier" ? token.text.toUpperCase() : token.text;
  };
  /** The keyword at `index`, unless a label (after `.` or AS) makes it a name. */
  const keyword = (index: number): string | undefined => {
    const before = at(index - 1);
    return before === "." || before === "AS" ? undefined : at(index);
  };
  const first = at(0);
  if (first !== "(" && first !== ";" && !STATEMENT_WORDS.has(first ?? "")) return true;

  // Brackets pair, and a comma separates two members, everywhere in the grammar.
  const edge = (index: number, brackets: string): boolean => {
    const text = at(index);
    return text === undefined
      ? tokens[index] === undefined
      : tokens[index]?.kind === "punctuation" && (text === "," || brackets.includes(text));
  };
  const open: string[] = [];
  let statements = 1;
  for (const [index, token] of tokens.entries()) {
    if (token.kind !== "punctuation") continue;
    if (token.text === "(" || token.text === "[") open.push(token.text);
    else if (token.text === ")" || token.text === "]") {
      if (open.pop() !== (token.text === ")" ? "(" : "[")) return true;
    } else if (token.text === "," && (edge(index - 1, "([") || edge(index + 1, ")]"))) return true;
    else if (token.text === ";") statements++;
  }
  if (open.length > 0) return true;
  // CREATE [OR REPLACE] [CONSTRAINT] TRIGGER always ends EXECUTE FUNCTION f(...): next/'s
  // BEGIN ... END body (whose statements end in semicolons) is not PostgreSQL's syntax.
  if (
    first === "CREATE" &&
    [1, 2, 3, 4].some((index) => at(index) === "TRIGGER") &&
    !tokens.some((_, index) => at(index) === "EXECUTE")
  )
    return true;
  // The rules below read one statement.
  if (statements > 1) return false;

  if (count === 1 && !ONE_WORD_STATEMENTS.has(first ?? "")) return true;
  if (count === 2 && (first === "CREATE" || first === "ALTER" || first === "DROP")) return true;

  const last = count - 1;
  if (QUERY_WORDS.has(first ?? "")) {
    // No query ends in an operator but `*` (and ORDER BY ... USING <op>), or a reserved word.
    if (tokens[last]?.kind === "operator" && at(last) !== "*" && at(last - 1) !== "USING")
      return true;
    if (NEVER_FINAL_WORDS.has(keyword(last) ?? "")) return true;
  }

  /** The index of the `)` closing the `(` at `index`. */
  const closing = (index: number): number => {
    let depth = 0;
    for (let at_ = index; at_ < count; at_++) {
      const text = tokens[at_]?.kind === "punctuation" ? tokens[at_]?.text : undefined;
      if (text === "(") depth++;
      else if (text === ")" && --depth === 0) return at_;
    }
    return count;
  };
  /** Whether `(` at `index` closes at once: an empty list. */
  const empty = (index: number): boolean => at(index) === "(" && at(index + 1) === ")";

  if (first === "CREATE") {
    // CREATE [OR REPLACE] [TEMP] [RECURSIVE] VIEW name: a column list, WITH (options) or AS
    // <query> follows the (qualified) name.
    let view = 1;
    if (at(view) === "OR" && at(view + 1) === "REPLACE") view += 2;
    if (["TEMP", "TEMPORARY"].includes(at(view) ?? "")) view++;
    if (at(view) === "RECURSIVE") view++;
    if (at(view) === "VIEW") {
      let next = view + 2;
      if (tokens[view + 1]?.kind !== "identifier") return true;
      while (at(next) === "." && tokens[next + 1]?.kind === "identifier") next += 2;
      if (!["(", "WITH", "AS"].includes(at(next) ?? "")) return true;
      if (at(next) === "AS" && next === last) return true;
    }
    // A column definition's type takes no empty modifier list: (name type () ...).
    if (at(1) === "TABLE" || at(2) === "TABLE")
      for (let index = 1; index < count; index++)
        if (
          (at(index - 1) === "(" || at(index - 1) === ",") &&
          tokens[index]?.kind === "identifier" &&
          tokens[index + 1]?.kind === "identifier" &&
          empty(index + 2)
        )
          return true;
  }
  if (first === "MERGE" && !tokens.some((_, index) => at(index) === "WHEN")) return true;

  for (let index = 0; index < count; index++) {
    // next/'s positional `?` is an operator to PostgreSQL (scan.l), and no operator is postfix
    // since PostgreSQL 14: before a comma, a closing bracket or the end it has no right operand.
    if (tokens[index]?.kind === "parameter" && tokens[index]?.text === "") {
      const after = at(index + 1);
      if (index === last || after === "," || after === ")" || after === "]") return true;
    }
    if (typeModifierRejected(index)) return true;
    const word = keyword(index);
    if (word === undefined) continue;
    const next = at(index + 1);
    if (word === "CASE") {
      // CASE [operand] WHEN ... [ELSE ...] END: at least one WHEN before its END.
      let depth = 0;
      let nested = 0;
      let when = false;
      let ended = false;
      for (let scan = index + 1; scan < count && depth >= 0 && !ended; scan++) {
        const text = keyword(scan);
        if (text === "(") depth++;
        else if (text === ")") depth--;
        else if (depth > 0) continue;
        else if (text === "CASE") nested++;
        else if (text === "WHEN" && nested === 0) when = true;
        else if (text === "END") {
          if (nested === 0) ended = true;
          else nested--;
        }
      }
      if (!ended || !when) return true;
    } else if (word === "WINDOW") {
      // WINDOW name AS (specification).
      if (tokens[index + 1]?.kind !== "identifier" || at(index + 2) !== "AS") return true;
      if (at(index + 3) !== "(") return true;
    } else if (word === "DISTINCT") {
      // DISTINCT takes a list: never `DISTINCT)`; an aggregate's never `(DISTINCT *`.
      if (next === ")" || (at(index - 1) === "(" && next === "*")) return true;
    } else if ((word === "IN" || word === "VALUES") && next === "(" && at(index + 2) === "*") {
      return true;
    } else if (word === "EXISTS" && next === "(") {
      // EXISTS takes a parenthesized query.
      const start = at(index + 2);
      if (!["SELECT", "VALUES", "TABLE", "WITH", "("].includes(start ?? "")) return true;
    } else if (["COALESCE", "GREATEST", "LEAST"].includes(word) && empty(index + 1)) {
      return true;
    } else if (word === "NULLIF" && next === "(") {
      // NULLIF(a, b): exactly one comma at its own depth.
      let depth = 0;
      let commas = 0;
      const end = closing(index + 1);
      for (let scan = index + 2; scan < end; scan++) {
        const text = at(scan);
        if (text === "(" || text === "[") depth++;
        else if (text === ")" || text === "]") depth--;
        else if (text === "," && depth === 0) commas++;
      }
      if (commas !== 1) return true;
    } else if (word === "AS" && at(index + 1) === "ENUM" && at(index + 2) === "(") {
      // AS ENUM ('a', ...) or AS ENUM (): string members only.
      const member = tokens[index + 3];
      if (member?.kind !== "string" && at(index + 3) !== ")") return true;
    } else if ((word === "::" || word === "AS") && tokens[index + 1]?.kind === "identifier") {
      // A type's (or an alias's column list's) members are never empty: `::text()`, `AS x()`.
      if (empty(index + 2)) return true;
    } else if (word === "WITH" && tokens[index + 1]?.kind === "identifier") {
      // A CTE's column list is never empty: WITH [RECURSIVE] x () AS.
      const name = at(index + 1) === "RECURSIVE" ? index + 2 : index + 1;
      if (tokens[name]?.kind === "identifier" && empty(name + 1)) return true;
    } else if (word === "GROUPING" && next === "SETS" && empty(index + 2)) {
      return true;
    } else if (
      (word === "ROLLUP" || word === "CUBE") &&
      at(index - 1) === "BY" &&
      at(index - 2) === "GROUP" &&
      empty(index + 1)
    ) {
      return true;
    } else if (word === "CONFLICT" && at(index - 1) === "ON" && empty(index + 1)) {
      if (at(index + 3) === "DO") return true;
    } else if (word === "ROWS" || word === "RANGE" || word === "GROUPS") {
      if (frameRejected(index)) return true;
    }
  }
  return false;

  /**
   * A type keyword's modifier gram.y refuses: any on INT, INTEGER, SMALLINT, BIGINT, REAL and
   * BOOLEAN; on CHAR, VARCHAR, TIME, TIMESTAMP, INTERVAL and FLOAT anything but one integer
   * constant. None is a function name, but after `.` it is a column (`t.time(...)` calls a
   * function), and after AS an alias may take a column list (`FROM t AS time (a)`).
   */
  function typeModifierRejected(index: number): boolean {
    const word = at(index);
    if (at(index + 1) !== "(" || at(index - 1) === ".") return false;
    const member = tokens[index + 2];
    if (at(index - 1) === "AS" && member?.kind === "identifier") return false;
    if (UNMODIFIED_TYPES.has(word ?? "")) return true;
    const varying =
      word === "VARYING" && ["CHAR", "CHARACTER", "NCHAR"].includes(at(index - 1) ?? "");
    if (!INTEGER_MODIFIER_TYPES.has(word ?? "") && !varying) return false;
    // One integer constant: (n).
    return member?.kind !== "number" || !/^\d+$/u.test(member.text) || at(index + 3) !== ")";
  }

  /**
   * A window frame gram.y refuses (frame_extent): start UNBOUNDED FOLLOWING, end UNBOUNDED
   * PRECEDING, a start that follows its end (FOLLOWING before CURRENT ROW or PRECEDING, CURRENT
   * ROW before PRECEDING), and one bound that FOLLOWs. False for a ROWS that is no frame (a table).
   */
  function frameRejected(index: number): boolean {
    // Only in a window specification: OVER (...) or WINDOW w AS (...).
    let depth = 0;
    let opening = index - 1;
    for (; opening >= 0; opening--) {
      const text = at(opening);
      if (text === ")") depth++;
      else if (text === "(" && depth-- === 0) break;
    }
    if (at(opening - 1) !== "OVER" && at(opening - 1) !== "AS") return false;
    /** A bound from `start` to before `stop` (the first of `stops` at depth 0). */
    const bound = (start: number, stops: readonly string[]) => {
      let end = start;
      let depth = 0;
      while (end < count) {
        const text = at(end);
        if (depth === 0 && (stops.includes(text ?? "") || text === ")")) break;
        if (text === "(") depth++;
        else if (text === ")") depth--;
        end++;
      }
      const tail = keyword(end - 1);
      const kind =
        tail === "PRECEDING" || tail === "FOLLOWING" ? tail : tail === "ROW" ? "CURRENT" : "";
      return { end, kind, unbounded: at(start) === "UNBOUNDED" };
    };
    if (at(index + 1) === "BETWEEN") {
      const start = bound(index + 2, ["AND"]);
      if (at(start.end) !== "AND" || start.kind === "") return false;
      const end = bound(start.end + 1, ["EXCLUDE"]);
      if (end.kind === "") return false;
      if (start.kind === "FOLLOWING" && start.unbounded) return true;
      if (end.kind === "PRECEDING" && end.unbounded) return true;
      if (start.kind === "FOLLOWING") return end.kind !== "FOLLOWING";
      return start.kind === "CURRENT" && end.kind === "PRECEDING";
    }
    // One bound: only a frame when it reads as one (`ROWS 1 PRECEDING`, not `FROM rows`).
    const only = bound(index + 1, ["EXCLUDE"]);
    return only.kind === "FOLLOWING" && at(only.end) === ")";
  }
}

/**
 * The statement's first word, upper-cased, as the router reads it: comments (nested block
 * comments too) and whitespace skipped as the tokenizer skips them, a leading parenthesis is `(`.
 * Null when the text starts with something else or an unterminated comment.
 */
export function leadingKeyword(text: string): string | null {
  // Its own scan of the leading whitespace and comments: the rest of the text may not tokenize
  // (an unsupported character later on is a refusal after reading).
  let index = 0;
  for (;;) {
    while (index < text.length && /\s/u.test(text[index] ?? "")) index += 1;
    if (text.startsWith("--", index)) {
      const newline = text.indexOf("\n", index);
      index = newline === -1 ? text.length : newline + 1;
      continue;
    }
    if (text.startsWith("/*", index)) {
      let depth = 0;
      while (index < text.length) {
        if (text.startsWith("/*", index)) {
          depth += 1;
          index += 2;
        } else if (text.startsWith("*/", index)) {
          depth -= 1;
          index += 2;
          if (depth === 0) break;
        } else index += 1;
      }
      if (depth !== 0) return null;
      continue;
    }
    break;
  }
  if (text[index] === "(") return "(";
  const word = /^[A-Za-z_][A-Za-z0-9_$]*/u.exec(text.slice(index))?.[0];
  return word === undefined ? null : word.toUpperCase();
}
