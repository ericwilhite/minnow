/**
 * Literal normalization for plan caching: `WHERE id = 42` and `WHERE id = 43` share one compiled
 * plan, `WHERE id = $1`, bound to 42 or 43. This scan only proposes the shape. The caller accepts
 * a shape only once the parameterized plan, bound to the literals, is proven identical to the
 * plan compiled from the literal text — so every typing and rewrite decision the compiler makes on
 * the shape is the one it makes on the literals — and keys it by the literals' kinds as well as
 * the text, so an integer and a string never share a shape.
 *
 * Only values that typically vary between otherwise identical statements are lifted, and only
 * where a placeholder is an ordinary value: plain integers and plain strings that are the right
 * operand of a comparison (`=`, `<>`, `<`, `<=`, `>`, `>=`), a BETWEEN bound, or an item of an
 * `IN (…)` list. Everything else stays in the shape's text: decimals and exponents (exact
 * numerics), typed literals (`DATE '…'`), casts (`'1'::int`), LIKE patterns (prefix ranges read
 * the pattern), LIMIT/OFFSET, GROUP BY and ORDER BY ordinals, function arguments, and negated
 * numbers. A statement with comments, placeholders, escape strings, or dollar quoting is left
 * whole. The scan only has to agree with the tokenizer on the values it lifts; anywhere else a
 * misreading costs a cache miss, never a wrong plan, since the shape is checked.
 */
import type { QueryValue } from "../plan/model.js";

export interface NormalizedLiterals {
  /** The statement with each lifted literal replaced by `$n`, in order. */
  readonly text: string;
  /** One character per lifted value: `i` for an integer, `s` for a string. */
  readonly kinds: string;
  readonly values: readonly QueryValue[];
}

const COMPARISONS = new Set(["=", "<>", "!=", "<", ">", "<=", ">="]);
const OPERATOR_PAIRS = new Set([">=", "<=", "!=", "<>", "||", "->", "::", "!~", "~~"]);
/** The AND that closes a BETWEEN: the literal after it is a bound. */
const BETWEEN_AND = "BETWEEN AND";

function isSpace(code: number): boolean {
  return code === 32 || (code >= 9 && code <= 13) || code === 160 || code === 0xfeff;
}

function isIdentifierStart(code: number): boolean {
  return (code >= 65 && code <= 90) || (code >= 97 && code <= 122) || code === 95;
}

function isDigit(code: number): boolean {
  return code >= 48 && code <= 57;
}

/** Whether `::` follows position `index`, past whitespace: the literal is a cast operand. */
function castFollows(sql: string, index: number): boolean {
  let at = index;
  while (at < sql.length && isSpace(sql.charCodeAt(at))) at += 1;
  return sql.charCodeAt(at) === 58 && sql.charCodeAt(at + 1) === 58;
}

/**
 * The statement's liftable literals, or undefined when it has none or holds a construct this
 * scan does not read.
 */
export function normalizeSqlLiterals(sql: string): NormalizedLiterals | undefined {
  const values: QueryValue[] = [];
  let kinds = "";
  let text = "";
  // Text copied verbatim since the last lifted literal starts here.
  let copied = 0;
  let previous = "";
  // One entry per open parenthesis: whether it opened an `IN (…)` list.
  const lists: boolean[] = [];
  let betweenOpen = false;
  const liftable = (): boolean =>
    COMPARISONS.has(previous) ||
    previous === "BETWEEN" ||
    previous === BETWEEN_AND ||
    ((previous === "(" || previous === ",") && lists[lists.length - 1] === true);
  let index = 0;
  while (index < sql.length) {
    const code = sql.charCodeAt(index);
    if (isSpace(code)) {
      index += 1;
      continue;
    }
    if (isIdentifierStart(code)) {
      const start = index;
      index += 1;
      while (index < sql.length) {
        const next = sql.charCodeAt(index);
        if (!isIdentifierStart(next) && !isDigit(next)) break;
        index += 1;
      }
      // E'…' strings take backslash escapes this scan does not decode.
      if (index - start === 1 && (code === 69 || code === 101) && sql.charCodeAt(index) === 39) {
        return undefined;
      }
      const word = sql.slice(start, index).toUpperCase();
      if (word === "BETWEEN") {
        betweenOpen = true;
        previous = word;
      } else if (word === "AND" && betweenOpen) {
        betweenOpen = false;
        previous = BETWEEN_AND;
      } else {
        previous = word;
      }
      continue;
    }
    const literalStart = index;
    let literal: { value: QueryValue; kind: string } | undefined;
    if (isDigit(code) || (code === 46 && isDigit(sql.charCodeAt(index + 1)))) {
      index += 1;
      let plain = code !== 46;
      while (index < sql.length) {
        const next = sql.charCodeAt(index);
        if (isDigit(next)) index += 1;
        else if (next === 46 || isIdentifierStart(next)) {
          // A decimal point, digit separator, exponent, radix prefix, or trailing junk: the
          // tokenizer's own number rules apply, and the literal keeps its spelling.
          plain = false;
          index += 1;
        } else break;
      }
      const value = plain ? Number(sql.slice(literalStart, index)) : Number.NaN;
      if (Number.isSafeInteger(value)) literal = { value, kind: "i" };
    } else if (code === 39) {
      index += 1;
      let value = "";
      let closed = false;
      while (index < sql.length) {
        const current = sql.charCodeAt(index);
        if (current === 39 && sql.charCodeAt(index + 1) === 39) {
          value += "'";
          index += 2;
        } else if (current === 39) {
          index += 1;
          closed = true;
          break;
        } else {
          value += sql[index] ?? "";
          index += 1;
        }
      }
      if (!closed) return undefined;
      // Protected NUL-namespace text and control characters keep their literal spelling.
      // eslint-disable-next-line no-control-regex
      if (!/[\u0000-\u001f\\]/.test(value)) literal = { value, kind: "s" };
    } else if (code === 34) {
      index += 1;
      let closed = false;
      while (index < sql.length) {
        if (sql.charCodeAt(index) === 34 && sql.charCodeAt(index + 1) === 34) index += 2;
        else if (sql.charCodeAt(index) === 34) {
          index += 1;
          closed = true;
          break;
        } else index += 1;
      }
      if (!closed) return undefined;
      previous = '"';
      continue;
    } else {
      // Placeholders, dollar quoting, and comments: the statement is left whole.
      if (code === 63 || code === 36) return undefined;
      const pair = sql.slice(index, index + 2);
      if (pair === "--" || pair === "/*") return undefined;
      if (code === 40) {
        lists.push(previous === "IN");
        previous = "(";
        index += 1;
      } else if (code === 41) {
        lists.pop();
        previous = ")";
        index += 1;
      } else if (OPERATOR_PAIRS.has(pair)) {
        previous = pair;
        index += 2;
      } else {
        previous = sql[index] ?? "";
        index += 1;
      }
      continue;
    }
    // A number or a string: lift it, or keep its spelling.
    if (literal !== undefined && liftable() && !castFollows(sql, index)) {
      values.push(literal.value);
      kinds += literal.kind;
      text += `${sql.slice(copied, literalStart)}$${String(values.length)}`;
      copied = index;
    }
    previous = "#";
  }
  if (values.length === 0) return undefined;
  return { text: text + sql.slice(copied), kinds, values };
}
