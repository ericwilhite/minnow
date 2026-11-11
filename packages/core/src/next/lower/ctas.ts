/** A legacy-compatible CTAS suffix candidate, not a SQL statement classifier.
 * The caller must validate the parsed CTAS kind: AS may occur inside a name/literal.
 * Unbounded lookbehind made Node 22 rescan the whole prefix at every position.
 */
const START = /^\s*CREATE\s+TABLE\b/iu;

export interface CtasDataClause {
  readonly index: number;
  readonly noData: boolean;
}

/** One anchored prefix, disjoint backward whitespace runs and one bounded AS search.
 * Work is O(statement length), with no repeated regex searches/backtracking over prefixes.
 * No header grammar is duplicated here: qualified names, column lists and comments retain
 * the legacy candidate decision. Prefix/suffix separators remain whitespace-only, with an
 * optional terminal semicolon, then whitespace, just as before.
 */
export function ctasDataClause(text: string): CtasDataClause | null {
  const start = START.exec(text);
  if (start === null) return null;
  const whitespace = (at: number): boolean => {
    const code = text.charCodeAt(at);
    return code === 32 || (code >= 9 && code <= 13) || (code >= 128 && /\s/u.test(text[at] ?? ""));
  };
  const skip = (end: number): number => {
    while (end > 0 && whitespace(end - 1)) end--;
    return end;
  };
  const word = (value: string, end: number): number | null => {
    const start = end - value.length;
    if (start < 0) return null;
    for (let i = 0; i < value.length; i++)
      if ((text.charCodeAt(start + i) | 32) !== (value.charCodeAt(i) | 32)) return null;
    return start;
  };
  let end = skip(text.length);
  if (text[end - 1] === ";") end = skip(end - 1);
  const data = word("DATA", end);
  if (data === null) return null;
  let before = skip(data);
  if (before === data) return null;
  let noData = false;
  const no = word("NO", before);
  if (no !== null) {
    before = skip(no);
    if (before === no) return null;
    noData = true;
  }
  const withAt = word("WITH", before);
  if (withAt === null || withAt === 0 || !whitespace(withAt - 1)) return null;
  // Fixed literal search has no unbounded lookbehind or repeated prefix scans.
  // Preserve the old candidate decision for every identifier/header spelling;
  // the caller must verify the parsed statement is CREATE TABLE AS before using it.
  if (!/\bAS\b/iu.test(text.slice(start[0].length, withAt))) return null;
  return { index: skip(withAt), noData };
}
