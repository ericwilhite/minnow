/** PG makeObjectName/ChooseRelationName: byte-budget each identifier, then try seq, seq1… */
import { utf8Length } from "../vector/varlen.js";
function clip(text: string, bytes: number): string {
  let at = 0,
    used = 0;
  while (at < text.length) {
    const point = text.codePointAt(at) ?? 0,
      width = point < 128 ? 1 : point < 2048 ? 2 : point < 65536 ? 3 : 4;
    if (used + width > bytes) break;
    used += width;
    at += point > 65535 ? 2 : 1;
  }
  return text.slice(0, at);
}
export function ownedSequenceName(
  table: string,
  column: string,
  taken: (name: string) => boolean,
): string {
  for (let attempt = 0; ; attempt++) {
    const suffix = attempt === 0 ? "seq" : `seq${String(attempt)}`,
      available = 63 - suffix.length - 2;
    let a = utf8Length(table),
      b = utf8Length(column);
    while (a + b > available) {
      if (a > b) a--;
      else b--;
    }
    const candidate = `${clip(table, a)}_${clip(column, b)}_${suffix}`;
    if (!taken(candidate)) return candidate;
  }
}
