/** CTAS has an AS keyword after its table name, not merely an AS inside a literal/name.
 * Only that statement class may consume a trailing WITH [NO] DATA clause. Never use
 * an unbounded lookbehind here: Node 22 rescans the entire prefix at every position.
 */
const HEADER = /^\s*CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(?:"(?:[^"]|"")+"|[^\s("]+)\s+AS\b/iu;

export interface CtasDataClause {
  readonly index: number;
  readonly noData: boolean;
}

/** One anchored header check and one backward pass over disjoint whitespace runs.
 * Work is O(statement length), with no regex search/backtracking over the query body.
 * Keep the existing clause grammar: optional terminal semicolon, then whitespace.
 */
export function ctasDataClause(text: string): CtasDataClause | null {
  if (!HEADER.test(text)) return null;
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
  return { index: skip(withAt), noData };
}
