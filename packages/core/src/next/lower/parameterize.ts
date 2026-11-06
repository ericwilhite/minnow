/**
 * Literal auto-parameterization for the owner's plan cache. `SELECT … WHERE id = 42` and
 * `… = 43` should share one plan, as `… = $1` does. Two steps:
 *
 * 1. `parameterize(text)` scans the text (no parse) and lifts each integer or plain string
 *    literal that is the whole right operand of `=` into a trailing placeholder, `$k` after the
 *    statement's own. The cache key is the rewritten text plus each lifted literal's type class,
 *    so texts that differ only in those values share a key, and `42` (integer) never shares one
 *    with `5000000000` (bigint) or `'42'` (text).
 * 2. `lowerParameterized` lowers the rewritten text with every lifted slot fixed to its
 *    literal's own type (int4, int8 or text, PostgreSQL's types for those literals), then checks
 *    each use of a lifted slot in the IR. A slot may be only an operand of `=` against the same
 *    type, or a key of an index lookup or range, directly or through a widening integer cast.
 *    There a slot is equivalent to the literal for every value of its class: the comparison is
 *    the same, and no decision of lowering read the value (equality estimates use distinct
 *    counts, never the constant). Any other use (a cast, a function argument, an ordinal, a
 *    LIMIT, a range bound costed from the value), or a lowering failure, returns null: the owner
 *    lowers the original text and caches it under that text.
 *
 * Not lifted: decimal and float literals, typed literals (`DATE '…'`, `'…'::int`, `E'…'`),
 * assignments (`SET a = 5`), and literals anywhere but directly after `=`. Statements other than SELECT, WITH, INSERT, UPDATE and
 * DELETE are left alone.
 */
import type { Rel, ScalarExpr } from "../ir/ir.js";
import type { PhysicalType } from "../types.js";

/** A statement with its liftable literals moved into trailing placeholders. */
export interface ParameterizedText {
  /** The plan-cache key: the same for texts that differ only in lifted literal values. */
  readonly key: string;
  /** The rewritten text, lifted literals as `$k`. */
  readonly text: string;
  /** How many `$n` placeholders the statement itself has; lifted slots follow them. */
  readonly placeholders: number;
  /** The lifted literals' values, bound after the statement's own parameters. */
  readonly values: ReadonlyArray<number | bigint | string>;
  /** The lifted slots' types: the literals' own (int4, int8, text). */
  readonly types: readonly PhysicalType[];
}

const INT4: PhysicalType = { kind: "int32", bits: 32 };
const INT8: PhysicalType = { kind: "int64" };
const TEXT: PhysicalType = { kind: "text" };
const INT4_MIN = -2147483648n;
const INT4_MAX = 2147483647n;
const INT8_MIN = -9223372036854775808n;
const INT8_MAX = 9223372036854775807n;

/** Statements whose `=` operands are expressions (not DDL options or SET values). */
const LEADING = new Set(["SELECT", "WITH", "INSERT", "UPDATE", "DELETE"]);
/** Words that may follow a lifted literal: it is then the whole operand. */
const AFTER = new Set([
  "AND",
  "OR",
  "ORDER",
  "GROUP",
  "LIMIT",
  "OFFSET",
  "FETCH",
  "RETURNING",
  "UNION",
  "INTERSECT",
  "EXCEPT",
  "WINDOW",
  "HAVING",
  "THEN",
  "ELSE",
  "END",
  "WHEN",
  "FOR",
  "AS",
  "FROM",
  "WHERE",
  "ON",
  "JOIN",
  "INNER",
  "LEFT",
  "RIGHT",
  "FULL",
  "CROSS",
  "NATURAL",
]);

type Token =
  | { readonly kind: "word"; readonly text: string; readonly start: number; readonly end: number }
  | { readonly kind: "integer"; readonly start: number; readonly end: number }
  | { readonly kind: "string"; readonly start: number; readonly end: number }
  | { readonly kind: "other"; readonly text: string; readonly start: number; readonly end: number }
  | {
      readonly kind: "placeholder";
      readonly index: number;
      readonly start: number;
      readonly end: number;
    };

const isDigit = (code: number) => code >= 48 && code <= 57;
const isWordStart = (code: number) =>
  (code >= 65 && code <= 90) || (code >= 97 && code <= 122) || code === 95 || code > 127;
const isWord = (code: number) => isWordStart(code) || isDigit(code) || code === 36;

/**
 * The text's significant tokens, or null when the scanner meets something it does not model
 * (an unterminated string or comment): the statement is then left as written.
 */
function scan(text: string): Token[] | null {
  const tokens: Token[] = [];
  let index = 0;
  const length = text.length;
  while (index < length) {
    const code = text.charCodeAt(index);
    if (code === 32 || code === 9 || code === 10 || code === 13 || code === 12) {
      index += 1;
      continue;
    }
    const start = index;
    if (code === 45 && text.charCodeAt(index + 1) === 45) {
      const newline = text.indexOf("\n", index);
      index = newline === -1 ? length : newline + 1;
      continue;
    }
    if (code === 47 && text.charCodeAt(index + 1) === 42) {
      // Block comments nest in PostgreSQL.
      let depth = 0;
      while (index < length) {
        if (text.charCodeAt(index) === 47 && text.charCodeAt(index + 1) === 42) {
          depth += 1;
          index += 2;
        } else if (text.charCodeAt(index) === 42 && text.charCodeAt(index + 1) === 47) {
          depth -= 1;
          index += 2;
          if (depth === 0) break;
        } else index += 1;
      }
      if (depth !== 0) return null;
      continue;
    }
    if (code === 39) {
      index += 1;
      for (;;) {
        if (index >= length) return null;
        if (text.charCodeAt(index) === 39) {
          if (text.charCodeAt(index + 1) === 39) index += 2;
          else break;
        } else index += 1;
      }
      index += 1;
      tokens.push({ kind: "string", start, end: index });
      continue;
    }
    if (code === 34) {
      index += 1;
      for (;;) {
        if (index >= length) return null;
        if (text.charCodeAt(index) === 34) {
          if (text.charCodeAt(index + 1) === 34) index += 2;
          else break;
        } else index += 1;
      }
      index += 1;
      tokens.push({ kind: "word", text: "\u0000quoted", start, end: index });
      continue;
    }
    if (code === 36) {
      if (isDigit(text.charCodeAt(index + 1))) {
        index += 1;
        while (index < length && isDigit(text.charCodeAt(index))) index += 1;
        tokens.push({
          kind: "placeholder",
          index: Number(text.slice(start + 1, index)),
          start,
          end: index,
        });
        continue;
      }
      // A dollar-quoted string: $tag$ … $tag$.
      let close = index + 1;
      while (close < length && isWord(text.charCodeAt(close)) && text.charCodeAt(close) !== 36)
        close += 1;
      if (text.charCodeAt(close) !== 36) return null;
      const tag = text.slice(index, close + 1);
      const end = text.indexOf(tag, close + 1);
      if (end === -1) return null;
      index = end + tag.length;
      tokens.push({ kind: "other", text: "$$", start, end: index });
      continue;
    }
    if (isDigit(code) || (code === 46 && isDigit(text.charCodeAt(index + 1)))) {
      while (index < length && isDigit(text.charCodeAt(index))) index += 1;
      const digitsEnd = index;
      // A numeric literal runs on through its fraction, exponent and any letters (`1.5`, `1e5`,
      // `0x1F`); only plain digits are an integer literal.
      while (index < length && (isWord(text.charCodeAt(index)) || text.charCodeAt(index) === 46))
        index += 1;
      tokens.push(
        digitsEnd > start && index === digitsEnd
          ? { kind: "integer", start, end: index }
          : { kind: "other", text: "number", start, end: index },
      );
      continue;
    }
    if (isWordStart(code)) {
      while (index < length && isWord(text.charCodeAt(index))) index += 1;
      tokens.push({
        kind: "word",
        text: text.slice(start, index).toUpperCase(),
        start,
        end: index,
      });
      continue;
    }
    // Operators: a run of operator characters is one token (`=`, `<=`, `::`, `=>`).
    const OPERATOR = "+-*/<>=~!@#%^&|`?:";
    if (OPERATOR.includes(text[index] ?? "")) {
      while (index < length && OPERATOR.includes(text[index] ?? "")) index += 1;
      tokens.push({ kind: "other", text: text.slice(start, index), start, end: index });
      continue;
    }
    index += 1;
    tokens.push({ kind: "other", text: text.slice(start, index), start, end: index });
  }
  return tokens;
}

/**
 * `text` with its liftable literals as trailing placeholders (see the module comment), or null
 * when nothing is liftable.
 */
export function parameterize(text: string): ParameterizedText | null {
  const tokens = scan(text);
  if (tokens === null) return null;
  const [first] = tokens;
  if (first?.kind !== "word" || !LEADING.has(first.text)) return null;
  // A prefixed string (`E'…'`, `B'…'`, `U&'…'`) may use escapes this scan does not model.
  for (const [index, token] of tokens.entries()) {
    const before = tokens[index - 1];
    if (token.kind === "string" && before?.kind === "word" && before.end === token.start)
      return null;
  }
  let placeholders = 0;
  for (const token of tokens)
    if (token.kind === "placeholder") placeholders = Math.max(placeholders, token.index);
  const lifted: Array<{
    start: number;
    end: number;
    value: number | bigint | string;
    type: PhysicalType;
  }> = [];
  // Inside SET (UPDATE, ON CONFLICT DO UPDATE) `=` assigns: a cast of the literal, not a
  // comparison, so its literals stay (lowerParameterized would refuse the whole statement).
  let assigning = false;
  for (let index = 1; index < tokens.length; index += 1) {
    const token = tokens[index];
    if (token === undefined) break;
    if (token.kind === "word") {
      if (token.text === "SET") assigning = true;
      else if (token.text === "WHERE" || token.text === "FROM" || token.text === "RETURNING")
        assigning = false;
    }
    if (assigning || (token.kind !== "integer" && token.kind !== "string")) continue;
    // `= <literal>` or `= -<integer>`.
    let operator = tokens[index - 1];
    let negative = false;
    if (
      token.kind === "integer" &&
      operator?.kind === "other" &&
      operator.text === "-" &&
      operator.end === token.start
    ) {
      negative = true;
      operator = tokens[index - 2];
    }
    if (operator?.kind !== "other" || operator.text !== "=") continue;
    // The literal must be the whole operand: what follows ends it.
    const after = tokens[index + 1];
    const ends =
      after === undefined ||
      (after.kind === "other" &&
        (after.text === ")" || after.text === ";" || after.text === ",")) ||
      (after.kind === "word" && AFTER.has(after.text));
    if (!ends) continue;
    const start = negative ? (tokens[index - 1]?.start ?? token.start) : token.start;
    if (token.kind === "integer") {
      const digits = BigInt(text.slice(token.start, token.end));
      const value = negative ? -digits : digits;
      if (value >= INT4_MIN && value <= INT4_MAX)
        lifted.push({ start, end: token.end, value: Number(value), type: INT4 });
      else if (value >= INT8_MIN && value <= INT8_MAX)
        lifted.push({ start, end: token.end, value, type: INT8 });
      continue;
    }
    lifted.push({
      start,
      end: token.end,
      value: text.slice(token.start + 1, token.end - 1).replaceAll("''", "'"),
      type: TEXT,
    });
  }
  if (lifted.length === 0) return null;
  let rewritten = "";
  let classes = "";
  let cursor = 0;
  for (const [position, literal] of lifted.entries()) {
    rewritten += `${text.slice(cursor, literal.start)}$${String(placeholders + position + 1)}`;
    classes += literal.type.kind === "int32" ? "i" : literal.type.kind === "int64" ? "l" : "s";
    cursor = literal.end;
  }
  rewritten += text.slice(cursor);
  return {
    key: `${rewritten}\u0000${classes}`,
    text: rewritten,
    placeholders,
    values: lifted.map((literal) => literal.value),
    types: lifted.map((literal) => literal.type),
  };
}

/**
 * Whether every use of a slot in `lifted` is one where a slot is equivalent to the literal it
 * replaced (see the module comment): an operand of `=` against the same type, or a key of an
 * index lookup or range, either directly or through a widening integer cast.
 */
export function liftedSlotsAreEquivalent(
  plan: { readonly root: Rel; readonly initPlans: ReadonlyArray<{ readonly plan: Rel }> },
  lifted: ReadonlySet<number>,
): boolean {
  const widening = (expr: ScalarExpr): boolean =>
    expr.op === "cast" &&
    expr.input.type.kind === "int32" &&
    (expr.type.kind === "int64" ||
      (expr.type.kind === "int32" && expr.type.bits >= expr.input.type.bits));
  const isLifted = (expr: ScalarExpr) => expr.op === "param" && lifted.has(expr.slot);
  /** `expr` is a lifted slot, possibly under widening casts. */
  const liftedOperand = (expr: ScalarExpr): boolean =>
    isLifted(expr) || (widening(expr) && expr.op === "cast" && liftedOperand(expr.input));
  let safe = true;
  // Walks every value of the plan; `allowed` marks expressions already judged as permitted uses.
  const allowed = new Set<object>();
  const permit = (expr: ScalarExpr) => {
    allowed.add(expr);
    if (expr.op === "cast") permit(expr.input);
  };
  const visit = (value: unknown): void => {
    if (!safe || value === null || typeof value !== "object") return;
    if (Array.isArray(value)) {
      for (const item of value) visit(item);
      return;
    }
    const node = value as Record<string, unknown>;
    if (node.op === "compare") {
      const compare = node as unknown as Extract<ScalarExpr, { op: "compare" }>;
      for (const [side, other] of [
        [compare.left, compare.right],
        [compare.right, compare.left],
      ] as const)
        if (
          compare.cmp === "=" &&
          liftedOperand(side) &&
          !liftedOperand(other) &&
          sameBase(side.type, other.type)
        )
          permit(side);
    }
    if (node.op === "keyLookup") {
      for (const tuple of (node as unknown as Extract<Rel, { op: "keyLookup" }>).keys)
        for (const key of tuple) if (liftedOperand(key)) permit(key);
    }
    if (node.op === "indexRange") {
      const range = node as unknown as Extract<Rel, { op: "indexRange" }>;
      // Only the equality prefix, the same key in both bounds: a range bound appears in one
      // bound alone, and its value is costed (it moves the estimate).
      const lower = range.lower?.keys ?? [];
      const upper = range.upper?.keys ?? [];
      for (const [index, key] of lower.entries()) {
        const twin = upper[index];
        if (
          liftedOperand(key) &&
          twin !== undefined &&
          JSON.stringify(twin) === JSON.stringify(key)
        ) {
          permit(key);
          permit(twin);
        }
      }
    }
    if (node.op === "param" && lifted.has(node.slot as number) && !allowed.has(node)) {
      safe = false;
      return;
    }
    if (node.op === "cast" && !allowed.has(node) && liftedOperand(node as ScalarExpr)) {
      safe = false;
      return;
    }
    // A Bound ({ kind: "param" }) of LIMIT / OFFSET.
    if (node.kind === "param" && lifted.has(node.slot as number)) {
      safe = false;
      return;
    }
    for (const child of Object.values(node)) visit(child);
  };
  visit(plan.root);
  for (const init of plan.initPlans) visit(init.plan);
  return safe;
}

function sameBase(a: PhysicalType, b: PhysicalType): boolean {
  if (a.kind !== b.kind) return false;
  if (a.kind === "int32" && b.kind === "int32") return a.bits === b.bits;
  return true;
}
