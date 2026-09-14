/**
 * Shared glue for the differential oracles. Like ./seeds.ts, this module is excluded from the
 * published tarball, so shipped modules must not import it.
 */

/** Rewrites unquoted `?` placeholders to PostgreSQL's `$n`. */
export function positionalToNumbered(sql: string): string {
  let parameter = 0;
  let output = "";
  let cursor = 0;
  const copyQuoted = (delimiter: "'" | '"', backslashEscapes: boolean): void => {
    output += delimiter;
    cursor++;
    while (cursor < sql.length) {
      const character = sql[cursor] ?? "";
      output += character;
      cursor++;
      if (backslashEscapes && character === "\\" && cursor < sql.length) {
        output += sql[cursor] ?? "";
        cursor++;
        continue;
      }
      if (character !== delimiter) continue;
      if (sql[cursor] === delimiter) {
        output += delimiter;
        cursor++;
        continue;
      }
      return;
    }
  };
  while (cursor < sql.length) {
    const character = sql[cursor] ?? "";
    const next = sql[cursor + 1];
    if (character === "'" || character === '"') {
      const prefix = sql[cursor - 1];
      const beforePrefix = sql[cursor - 2];
      const escapedString =
        character === "'" &&
        (prefix === "E" || prefix === "e") &&
        (beforePrefix === undefined || !/[A-Za-z0-9_$]/.test(beforePrefix));
      copyQuoted(character, escapedString);
      continue;
    }
    if (character === "-" && next === "-") {
      const end = sql.indexOf("\n", cursor + 2);
      if (end < 0) return output + sql.slice(cursor);
      output += sql.slice(cursor, end + 1);
      cursor = end + 1;
      continue;
    }
    if (character === "/" && next === "*") {
      let depth = 1;
      output += "/*";
      cursor += 2;
      while (cursor < sql.length && depth > 0) {
        const current = sql[cursor] ?? "";
        const following = sql[cursor + 1];
        if (current === "/" && following === "*") {
          output += "/*";
          cursor += 2;
          depth++;
        } else if (current === "*" && following === "/") {
          output += "*/";
          cursor += 2;
          depth--;
        } else {
          output += current;
          cursor++;
        }
      }
      continue;
    }
    if (character === "$") {
      const dollarQuote = /^\$(?:[A-Za-z_][A-Za-z0-9_]*)?\$/.exec(sql.slice(cursor))?.[0];
      if (dollarQuote !== undefined) {
        const end = sql.indexOf(dollarQuote, cursor + dollarQuote.length);
        if (end < 0) return output + sql.slice(cursor);
        output += sql.slice(cursor, end + dollarQuote.length);
        cursor = end + dollarQuote.length;
        continue;
      }
    }
    if (character === "?") {
      output += `$${String(++parameter)}`;
      cursor++;
      continue;
    }
    output += character;
    cursor++;
  }
  return output;
}
