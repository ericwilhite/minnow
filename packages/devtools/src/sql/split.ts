/** One statement of a script, with where it sits in the original text. */
export interface ScriptStatement {
  sql: string;
  /** Offset of the statement's first character in the script, for pointing at errors. */
  from: number;
  /** Offset just past its last character, before the semicolon. */
  to: number;
}

/**
 * Splits a script at its top-level semicolons. The console runs one statement per call, as the
 * engine requires, so a script is cut here first — carefully, because a semicolon is not always
 * a boundary:
 *
 * - inside a string (`'a;b'`), a quoted identifier (`"a;b"`), or a comment (`-- ;`, `/* ; *\/`);
 * - inside a trigger body, where `BEGIN … END` holds statements of its own, each ending in one.
 *
 * `BEGIN` opens a block only when it is not the first word of a statement; first, it is the
 * transaction statement. `CASE … END` is tracked too, so the `END` that closes a CASE inside a
 * trigger body is not mistaken for the body's end. Empty statements — a trailing semicolon, two in
 * a row — are dropped.
 */
export function splitStatements(script: string): ScriptStatement[] {
  const statements: ScriptStatement[] = [];
  const blocks: Array<"begin" | "case"> = [];
  let start = 0;
  let index = 0;
  let statementHasWord = false;

  const push = (end: number): void => {
    const raw = script.slice(start, end);
    const leading = raw.length - raw.trimStart().length;
    const sql = raw.trim();
    if (sql.length > 0)
      statements.push({ sql, from: start + leading, to: start + leading + sql.length });
    start = end + 1;
    statementHasWord = false;
  };

  while (index < script.length) {
    const char = script[index] ?? "";
    const next = script[index + 1] ?? "";

    if (char === "-" && next === "-") {
      const eol = script.indexOf("\n", index);
      index = eol < 0 ? script.length : eol + 1;
      continue;
    }
    if (char === "/" && next === "*") {
      const close = script.indexOf("*/", index + 2);
      index = close < 0 ? script.length : close + 2;
      continue;
    }
    if (char === "'" || char === '"') {
      index = skipQuoted(script, index, char);
      statementHasWord = true;
      continue;
    }
    if (/[A-Za-z_]/.test(char)) {
      let end = index + 1;
      while (end < script.length && /[A-Za-z0-9_$]/.test(script[end] ?? "")) end += 1;
      const word = script.slice(index, end).toUpperCase();
      if (word === "CASE") blocks.push("case");
      else if (word === "BEGIN" && statementHasWord) blocks.push("begin");
      else if (word === "END") blocks.pop();
      statementHasWord = true;
      index = end;
      continue;
    }
    if (char === ";" && blocks.length === 0) {
      push(index);
      index += 1;
      continue;
    }
    if (!/\s/.test(char)) statementHasWord = true;
    index += 1;
  }
  push(script.length);
  return statements;
}

/** Past the closing quote, honouring the doubled-quote escape; an unterminated quote runs to the end. */
function skipQuoted(text: string, open: number, quote: string): number {
  let index = open + 1;
  while (index < text.length) {
    if (text[index] === quote) {
      if (text[index + 1] === quote) {
        index += 2;
        continue;
      }
      return index + 1;
    }
    index += 1;
  }
  return text.length;
}

/**
 * The statement without a trailing semicolon or whitespace, so a clause can be appended to it.
 * A trailing line comment is kept and the appendix goes on its own line, where the comment
 * cannot swallow it.
 */
export function withAppendedClause(sql: string, clause: string): string {
  const trimmed = sql.replace(/[\s;]+$/u, "");
  return `${trimmed}\n${clause}`;
}
