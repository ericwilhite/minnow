/**
 * SQL value semantics shared by the row and vector executors.
 *
 * Keep comparison, equality-key, scalar, and pattern behavior here so an execution-path
 * optimization cannot silently change query results.
 */

/**
 * Deterministic SQL ordering. Strings use Unicode codepoint order rather than host locale data,
 * and signed zero compares equal because SQL numeric equality does not distinguish it.
 */
export function compareSqlValues(left: unknown, right: unknown): number {
  const a = left instanceof Date ? left.getTime() : left;
  const b = right instanceof Date ? right.getTime() : right;
  if (a === b) return 0;
  if (a === null || a === undefined) return -1;
  if (b === null || b === undefined) return 1;
  if (typeof a === "number" && typeof b === "number") {
    if (Number.isNaN(a)) return Number.isNaN(b) ? 0 : 1;
    if (Number.isNaN(b)) return -1;
    return a - b;
  }
  if (typeof a === "string" && typeof b === "string") return compareSqlStrings(a, b);
  if (typeof a === "boolean" && typeof b === "boolean") return Number(a) - Number(b);
  throw new TypeError("Values must have comparable SQL types");
}

/** Hot-path string comparison shared by executors without locale-dependent host state. */
export function compareSqlStrings(left: string, right: string): number {
  if (left === right) return 0;
  return left < right ? -1 : 1;
}

/** A JSON-safe equality token used by set operations and recursive-CTE deduplication. */
export function encodeSqlEqualityValue(value: unknown): readonly unknown[] {
  if (value === null || value === undefined) return [0];
  if (typeof value === "boolean") return [1, value];
  if (typeof value === "number") return [2, String(value === 0 ? 0 : value)];
  if (typeof value === "string") return [3, value];
  if (value instanceof Date) return [4, value.getTime()];
  throw new TypeError("Query produced an unsupported value");
}

/**
 * SQLite-compatible ROUND behavior for Minnow's finite number type: precision truncates to an
 * integer and clamps to 0..30, with ties rounded away from zero.
 */
export function roundSqlNumber(value: number, precision = 0): number {
  const digits = Math.min(30, Math.max(0, Math.trunc(precision)));
  // SQLite formats with the requested decimal precision and parses the result back. toFixed
  // follows the same decimal path, avoiding multiplication overflow and binary scaling drift.
  const rounded = Number(value.toFixed(digits));
  return rounded === 0 ? 0 : rounded;
}

const likeCache = new Map<string, RegExp>();

/** Compiles SQL LIKE (% = any run, _ = one codepoint) into a bounded cached RegExp. */
export function compileLikePattern(
  pattern: string,
  caseInsensitive = false,
  escape?: string,
): RegExp {
  const key = JSON.stringify([caseInsensitive, escape ?? null, pattern]);
  const cached = likeCache.get(key);
  if (cached !== undefined) return cached;
  let source = "^";
  let escaped = false;
  for (const character of pattern) {
    if (escaped) {
      source += escapeRegExp(character);
      escaped = false;
      continue;
    }
    if (escape !== undefined && character === escape) {
      escaped = true;
      continue;
    }
    if (character === "%") source += "[\\s\\S]*";
    else if (character === "_") source += "[\\s\\S]";
    else source += escapeRegExp(character);
  }
  if (escaped) throw new TypeError("LIKE pattern ends with a dangling escape character");
  source += "$";
  const regExp = new RegExp(source, caseInsensitive ? "iu" : "u");
  if (likeCache.size >= 128) likeCache.clear();
  likeCache.set(key, regExp);
  return regExp;
}

function escapeRegExp(value: string): string {
  return value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

/** Defines an own enumerable result-column property, including the special name `__proto__`. */
export function defineSqlResultProperty(
  target: Record<string, unknown>,
  name: string,
  value: unknown,
): void {
  Object.defineProperty(target, name, {
    value,
    enumerable: true,
    configurable: true,
    writable: true,
  });
}

/**
 * A scalar function's string operand, or a type error naming the function. Shared because both
 * the scalar evaluator and the SQL/JSON functions insist on real strings rather than coercing:
 * a silent coercion here would make `LENGTH(42)` answer instead of failing.
 */
export function stringArgument(name: string, value: unknown): string {
  if (typeof value !== "string") throw new TypeError(`${name} requires a string argument`);
  return value;
}
