import { dateMilliseconds } from "../date-value.js";
import { assertWellFormedString } from "../block-format/unicode.js";
import {
  MAX_SQL_NESTING_DEPTH,
  MAX_SQL_PATTERN_CHARACTERS,
  MAX_SQL_PATTERN_MATCH_STEPS,
} from "./cache-limits.js";
import {
  collatedDomainCompare,
  enumDomainCompare,
  exactNumericCompare,
  externalSqlDomainValue,
  externalSqlTextValue,
  intervalDomainCompare,
  isDateDomainValue,
  isSqlDomainValue,
} from "./sql-domains.js";

/**
 * SQL value semantics shared by the row and vector executors.
 *
 * Keep comparison, equality-key, scalar, and pattern behavior here so an execution-path
 * optimization cannot silently change query results.
 */

const SQL_TIMESTAMP_TEXT =
  /^(\d{4}-\d{2}-\d{2})(?:[ T](\d{2}:\d{2}(?::\d{2}(?:\.\d{1,3})?)?))?(Z|[+-]\d{2}:?\d{2})?$/;

/**
 * The standard's datetime text — `2026-01-02`, `2026-01-02 03:04:05`, `2026-01-02T03:04:05.250Z`,
 * an optional zone offset — read as an instant. A zoneless spelling is UTC, the reading every
 * datetime in a Minnow database has; `undefined` for text in any other shape.
 */
export function parseSqlTimestampText(text: string): Date | undefined {
  const match = SQL_TIMESTAMP_TEXT.exec(text.trim());
  if (match === null) return undefined;
  const [, day, time = "00:00:00", zone = "Z"] = match;
  const seconds = time.length === 5 ? `${time}:00` : time;
  const date = new Date(`${String(day)}T${seconds}${zone === "Z" ? "Z" : zone}`);
  return Number.isFinite(dateMilliseconds(date)) ? date : undefined;
}

/**
 * PostgreSQL reads an untyped string constant beside a typed value in that value's type:
 * `joined >= '2026-01-01'`, `id = '5'`, `active = 't'`. Catalog-backed plans coerce such literals
 * before execution; this runtime reading covers the schema-less row executor and anything the
 * plan rewrite could not see. Text that does not parse in the other side's type keeps the
 * comparable-types error below, so a genuine mismatch still fails.
 */
export function coercedComparable(text: string, other: unknown): unknown {
  if (other instanceof Date || isDateDomainValue(other)) return readUntypedText("datetime", text);
  if (typeof other === "number") return readUntypedText("number", text);
  if (typeof other === "boolean") return readUntypedText("boolean", text);
  return text;
}

/**
 * Reads an untyped string constant in a primitive column type, the way PostgreSQL types an
 * unknown-typed literal by its context: a timestamp spelling for datetime, a finite number for
 * number, `t`/`true`/`1` and `f`/`false`/`0` for boolean. Text that does not parse is returned
 * unchanged, so the caller's own type check still reports the mismatch.
 */
export function readUntypedText(
  type: "datetime" | "number" | "boolean" | "string",
  text: string,
): unknown {
  if (type === "datetime") return parseSqlTimestampText(text) ?? text;
  if (type === "number") {
    const trimmed = text.trim();
    if (trimmed === "") return text;
    const parsed = Number(trimmed);
    return Number.isFinite(parsed) ? parsed : text;
  }
  if (type === "boolean") {
    const lowered = text.trim().toLowerCase();
    if (lowered === "t" || lowered === "true" || lowered === "1") return true;
    if (lowered === "f" || lowered === "false" || lowered === "0") return false;
  }
  return text;
}

/**
 * Applies the untyped-literal reading to a comparison's two operands: a plain string beside a
 * typed value (datetime, DATE, number, boolean) is read in that value's type when it parses.
 * Both executors call this before their own comparisons, so the reading is one decision.
 */
export function coerceComparisonOperands(left: unknown, right: unknown): [unknown, unknown] {
  const plainText = (value: unknown): value is string =>
    typeof value === "string" && !isSqlDomainValue(value);
  const typed = (value: unknown): boolean => typeof value !== "string" || isDateDomainValue(value);
  if (plainText(left) && typed(right)) return [coercedComparable(left, right), right];
  if (plainText(right) && typed(left)) return [left, coercedComparable(right, left)];
  return [left, right];
}

/**
 * Deterministic SQL ordering. Strings use Unicode codepoint order rather than host locale data,
 * and signed zero compares equal because SQL numeric equality does not distinguish it.
 */
export function compareSqlValues(left: unknown, right: unknown): number {
  // Plain numbers and untagged strings are the bulk of every comparison; settle them before the
  // domain checks. NaN falls through so its placement stays with the general ordering below, and
  // a string starting with NUL may carry a domain tag, so only tag-free strings take the short path.
  if (typeof left === "number" && typeof right === "number") {
    if (left < right) return -1;
    if (left > right) return 1;
    if (left === right) return 0;
  } else if (
    typeof left === "string" &&
    typeof right === "string" &&
    left.charCodeAt(0) !== 0 &&
    right.charCodeAt(0) !== 0
  ) {
    return compareSqlStrings(left, right);
  }
  const [coercedLeft, coercedRight] = coerceComparisonOperands(left, right);
  // Object.is, not !==: NaN would otherwise look changed on every pass and recurse forever.
  if (!Object.is(coercedLeft, left) || !Object.is(coercedRight, right)) {
    return compareSqlValues(coercedLeft, coercedRight);
  }
  const plainLeft = externalSqlTextValue(left);
  const plainRight = externalSqlTextValue(right);
  if (
    (typeof left === "string" && plainLeft !== left) ||
    (typeof right === "string" && plainRight !== right)
  ) {
    if (typeof plainLeft !== "string" || typeof plainRight !== "string") {
      throw new TypeError("Values must have comparable SQL types");
    }
    return compareSqlStrings(plainLeft, plainRight);
  }
  const collated = collatedDomainCompare(left, right);
  if (collated !== undefined) return collated;
  const interval = intervalDomainCompare(left, right);
  if (interval !== undefined) return interval;
  const enumOrder = enumDomainCompare(left, right);
  if (enumOrder !== undefined) return enumOrder;
  const exact = exactNumericCompare(left, right);
  if (exact !== undefined) return exact;
  const temporal = (value: unknown): number | undefined => {
    if (value instanceof Date) return dateMilliseconds(value);
    if (!isDateDomainValue(value)) return undefined;
    const external = externalSqlDomainValue(value);
    return typeof external === "string" ? Date.parse(`${external}T00:00:00.000Z`) : undefined;
  };
  const temporalLeft = temporal(left);
  const temporalRight = temporal(right);
  if (temporalLeft !== undefined || temporalRight !== undefined) {
    const comparableLeft = temporalLeft ?? (typeof left === "number" ? left : undefined);
    const comparableRight = temporalRight ?? (typeof right === "number" ? right : undefined);
    if (comparableLeft === undefined || comparableRight === undefined) {
      throw new TypeError("Values must have comparable SQL types");
    }
    return comparableLeft - comparableRight;
  }
  const a = left;
  const b = right;
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
  if (typeof value === "string") return [3, externalSqlTextValue(value)];
  if (value instanceof Date) return [4, dateMilliseconds(value)];
  throw new TypeError("Query produced an unsupported value");
}

/**
 * SQLite-compatible ROUND behavior for Minnow's finite number type: precision truncates to an
 * integer and clamps to 0..30, with ties rounded away from zero.
 */
export function roundSqlNumber(value: number, precision = 0): number {
  const digits = Math.min(30, Math.max(0, Math.trunc(precision)));
  // Whole-number rounding is arithmetic: toFixed(0) picks the integer nearest the exact binary
  // value, ties toward larger magnitude. Below 2^52 the fractional part of a double is exact,
  // so floor and compare reproduce it without formatting a string per row.
  if (digits === 0 && Number.isFinite(value)) {
    const magnitude = Math.abs(value);
    if (magnitude >= 4_503_599_627_370_496) return value === 0 ? 0 : value;
    const whole = Math.floor(magnitude);
    const rounded = magnitude - whole >= 0.5 ? whole + 1 : whole;
    return rounded === 0 ? 0 : value < 0 ? -rounded : rounded;
  }
  // SQLite formats with the requested decimal precision and parses the result back. toFixed
  // follows the same decimal path, avoiding multiplication overflow and binary scaling drift.
  const rounded = Number(value.toFixed(digits));
  return rounded === 0 ? 0 : rounded;
}

/** A whole-string SQL pattern matcher. Unlike RegExp, test never coerces its input. */
interface SqlPatternMatcher {
  test(value: string): boolean;
}

interface WeightedPatternMatcher extends SqlPatternMatcher {
  readonly retainedSize: number;
}

type PatternCacheEntry = readonly [matcher: WeightedPatternMatcher, retainedSize: number];

const MAX_PATTERN_CACHE_ENTRIES = 128;
const MAX_PATTERN_CACHE_RETAINED_SIZE = 64 * 1024;
const likeCache = new Map<string, PatternCacheEntry>();
const similarCache = new Map<string, PatternCacheEntry>();
let likeCacheRetainedSize = 0;
let similarCacheRetainedSize = 0;

function cachedPattern(
  cache: Map<string, PatternCacheEntry>,
  key: string,
): WeightedPatternMatcher | undefined {
  const entry = cache.get(key);
  if (entry === undefined) return undefined;
  cache.delete(key);
  cache.set(key, entry);
  return entry[0];
}

function retainPattern(
  cache: Map<string, PatternCacheEntry>,
  key: string,
  matcher: WeightedPatternMatcher,
  retainedSize: number,
  setRetainedSize: (size: number) => void,
): void {
  const entrySize = key.length + matcher.retainedSize;
  if (entrySize > MAX_PATTERN_CACHE_RETAINED_SIZE) return;
  while (
    cache.size >= MAX_PATTERN_CACHE_ENTRIES ||
    retainedSize + entrySize > MAX_PATTERN_CACHE_RETAINED_SIZE
  ) {
    const oldest = cache.keys().next().value;
    if (oldest === undefined) break;
    const removed = cache.get(oldest);
    cache.delete(oldest);
    retainedSize -= removed?.[1] ?? 0;
  }
  cache.set(key, [matcher, entrySize]);
  setRetainedSize(retainedSize + entrySize);
}

function throwPatternWorkLimit(): never {
  throw new RangeError(
    `SQL pattern match exceeds ${String(MAX_SQL_PATTERN_MATCH_STEPS)} deterministic steps`,
  );
}

/** Strings are literals; 0 is `%` (many) and 1 is `_` (one). */
type LikeToken = string | 0 | 1;

function exactSingleCharacter(value: string, label: string): string {
  assertBoundedPattern(value, label, 2);
  const characters = value[Symbol.iterator]();
  const first = characters.next();
  if (first.done === true || characters.next().done !== true) {
    throw new TypeError(`${label.replace(" escape", " ESCAPE")} takes one character`);
  }
  return first.value;
}

function tokenizeLike(pattern: string, escape: string | undefined): LikeToken[] {
  const tokens: LikeToken[] = [];
  let escaped = false;
  for (const character of pattern) {
    if (escaped) {
      tokens.push(character);
      escaped = false;
    } else if (escape !== undefined && character === escape) {
      escaped = true;
    } else if (character === "%") {
      if (tokens.at(-1) !== 0) tokens.push(0);
    } else if (character === "_") tokens.push(1);
    else tokens.push(character);
  }
  if (escaped) throw new TypeError("LIKE pattern ends with a dangling escape character");
  return tokens;
}

/** Unicode simple-case approximation used consistently by row and vector LIKE execution. */
function simpleCaseFold(character: string): string {
  const upper = character.toUpperCase();
  const codePoints = upper[Symbol.iterator]();
  return codePoints.next().done !== true && codePoints.next().done === true ? upper : character;
}

function likeCharactersEqual(left: string, right: string, caseInsensitive: boolean): boolean {
  return left === right || (caseInsensitive && simpleCaseFold(left) === simpleCaseFold(right));
}

function literalLikeMatcher(
  tokens: readonly LikeToken[],
  caseInsensitive: boolean,
): WeightedPatternMatcher | undefined {
  if (tokens.includes(1)) return undefined;
  const leading = tokens[0] === 0;
  const trailing = tokens.at(-1) === 0;
  const begin = leading ? 1 : 0;
  const end = trailing ? tokens.length - 1 : tokens.length;
  if (tokens.slice(begin, end).some((token) => typeof token !== "string")) return undefined;
  const body = tokens
    .slice(begin, end)
    .map((token) => (typeof token === "string" ? token : ""))
    .join("");
  if (!caseInsensitive) {
    const test = (value: string): boolean => {
      if (typeof value !== "string") throw new TypeError("LIKE input must be a string");
      if (leading && trailing) return value.includes(body);
      if (leading) return value.endsWith(body);
      if (trailing) return value.startsWith(body);
      return value === body;
    };
    return { test, retainedSize: tokens.length * 16 + body.length * 2 };
  }

  const needle = Array.from(body, simpleCaseFold);
  const prefix = new Uint32Array(needle.length);
  for (let index = 1, matched = 0; index < needle.length; index += 1) {
    while (matched > 0 && needle[index] !== needle[matched]) matched = prefix[matched - 1] ?? 0;
    if (needle[index] === needle[matched]) matched += 1;
    prefix[index] = matched;
  }
  const test = (value: string): boolean => {
    if (typeof value !== "string") throw new TypeError("LIKE input must be a string");
    if (needle.length === 0) return leading || trailing || value.length === 0;
    let matched = 0;
    let position = 0;
    let matchedAtEnd = false;
    for (const rawCharacter of value) {
      matchedAtEnd = false;
      const character = simpleCaseFold(rawCharacter);
      while (matched > 0 && character !== needle[matched]) matched = prefix[matched - 1] ?? 0;
      if (character === needle[matched]) matched += 1;
      position += 1;
      if (matched === needle.length) {
        const startsAt = position - needle.length;
        if (leading && trailing) return true;
        if (!leading && startsAt === 0) {
          if (trailing) return true;
          matchedAtEnd = true;
        } else if (leading && !trailing) matchedAtEnd = true;
        matched = prefix[matched - 1] ?? 0;
      }
    }
    return matchedAtEnd;
  };
  return {
    test,
    retainedSize: tokens.length * 16 + body.length * 2 + prefix.byteLength,
  };
}

function generalLikeMatcher(
  tokens: readonly LikeToken[],
  caseInsensitive: boolean,
): WeightedPatternMatcher {
  const test = (value: string): boolean => {
    if (typeof value !== "string") throw new TypeError("LIKE input must be a string");
    let remaining = MAX_SQL_PATTERN_MATCH_STEPS;
    let tokenIndex = 0;
    let valueIndex = 0;
    let manyIndex = -1;
    let retryValueIndex = 0;
    while (valueIndex < value.length) {
      if (--remaining < 0) throwPatternWorkLimit();
      const token = tokens[tokenIndex];
      if (token === 0) {
        manyIndex = tokenIndex;
        tokenIndex += 1;
        retryValueIndex = valueIndex;
        continue;
      }
      const codePoint = value.codePointAt(valueIndex);
      if (codePoint === undefined) break;
      const character = String.fromCodePoint(codePoint);
      if (
        token === 1 ||
        (typeof token === "string" && likeCharactersEqual(token, character, caseInsensitive))
      ) {
        tokenIndex += 1;
        valueIndex += character.length;
        continue;
      }
      if (manyIndex < 0) return false;
      const retryCodePoint = value.codePointAt(retryValueIndex);
      if (retryCodePoint === undefined) return false;
      retryValueIndex += String.fromCodePoint(retryCodePoint).length;
      valueIndex = retryValueIndex;
      tokenIndex = manyIndex + 1;
    }
    while (tokens[tokenIndex] === 0) {
      if (--remaining < 0) throwPatternWorkLimit();
      tokenIndex += 1;
    }
    return tokenIndex === tokens.length;
  };
  return { test, retainedSize: tokens.length * 16 };
}

/** Compiles SQL LIKE without exposing input to the host regular-expression engine. */
export function compileLikePattern(
  pattern: string,
  caseInsensitive = false,
  escape?: string,
): SqlPatternMatcher {
  assertBoundedPattern(pattern, "LIKE pattern");
  const exactEscape =
    escape === undefined ? undefined : exactSingleCharacter(escape, "LIKE escape");
  const key = JSON.stringify([caseInsensitive, exactEscape ?? null, pattern]);
  const cached = cachedPattern(likeCache, key);
  if (cached !== undefined) return cached;
  const tokens = tokenizeLike(pattern, exactEscape);
  const matcher =
    literalLikeMatcher(tokens, caseInsensitive) ?? generalLikeMatcher(tokens, caseInsensitive);
  retainPattern(likeCache, key, matcher, likeCacheRetainedSize, (size) => {
    likeCacheRetainedSize = size;
  });
  return matcher;
}

type CharacterMatcher = (character: string) => boolean;
const ANY_CHARACTER: CharacterMatcher = () => true;

/** Compact compile-time state: epsilon targets followed by an optional [target, predicate]. */
type NfaState = [epsilon: number[], transition?: [target: number, matches: CharacterMatcher]];
type NfaFragment = [start: number, end: number];

/** Parses SIMILAR TO directly into an NFA, avoiding an allocation-heavy intermediate AST. */
class SimilarCompiler {
  readonly #characters: string[];
  readonly states: NfaState[] = [];
  #index = 0;

  constructor(
    pattern: string,
    readonly escape: string,
  ) {
    this.#characters = Array.from(pattern);
  }

  compile(): NfaFragment {
    const fragment = this.#alternative(0);
    if (this.#index !== this.#characters.length) {
      throw new TypeError("SIMILAR TO pattern has an unmatched closing parenthesis");
    }
    return fragment;
  }

  #state(): number {
    this.states.push([[]]);
    return this.states.length - 1;
  }

  #empty(): NfaFragment {
    const start = this.#state();
    const end = this.#state();
    this.states[start]?.[0].push(end);
    return [start, end];
  }

  #consume(matches: CharacterMatcher): NfaFragment {
    const start = this.#state();
    const end = this.#state();
    const state = this.states[start];
    if (state !== undefined) state[1] = [end, matches];
    return [start, end];
  }

  #repeat(fragment: NfaFragment, plus: boolean): NfaFragment {
    const start = this.#state();
    const end = this.#state();
    this.states[start]?.[0].push(fragment[0]);
    if (!plus) this.states[start]?.[0].push(end);
    this.states[fragment[1]]?.[0].push(fragment[0], end);
    return [start, end];
  }

  #alternative(depth: number): NfaFragment {
    const first = this.#sequence(depth);
    if (this.#characters[this.#index] !== "|" || this.#characters[this.#index] === this.escape) {
      return first;
    }
    const start = this.#state();
    const end = this.#state();
    this.states[start]?.[0].push(first[0]);
    this.states[first[1]]?.[0].push(end);
    while (this.#characters[this.#index] === "|" && this.#characters[this.#index] !== this.escape) {
      this.#index += 1;
      const next = this.#sequence(depth);
      this.states[start]?.[0].push(next[0]);
      this.states[next[1]]?.[0].push(end);
    }
    return [start, end];
  }

  #sequence(depth: number): NfaFragment {
    let result: NfaFragment | undefined;
    for (;;) {
      const character = this.#characters[this.#index];
      if (
        character === undefined ||
        ((character === "|" || character === ")") && character !== this.escape)
      ) {
        return result ?? this.#empty();
      }
      let fragment = this.#atom(depth);
      const quantifier = this.#characters[this.#index];
      if ((quantifier === "*" || quantifier === "+") && quantifier !== this.escape) {
        this.#index += 1;
        fragment = this.#repeat(fragment, quantifier === "+");
        const repeated = this.#characters[this.#index];
        if ((repeated === "*" || repeated === "+") && repeated !== this.escape) {
          throw new TypeError("SIMILAR TO pattern repeats a quantifier");
        }
      }
      if (result === undefined) result = fragment;
      else {
        this.states[result[1]]?.[0].push(fragment[0]);
        result = [result[0], fragment[1]];
      }
    }
  }

  #atom(depth: number): NfaFragment {
    const character = this.#characters[this.#index];
    if (character === undefined) return this.#empty();
    this.#index += 1;
    if (character === this.escape) {
      const literal = this.#characters[this.#index];
      if (literal === undefined) {
        throw new TypeError("SIMILAR TO pattern ends with its escape character");
      }
      this.#index += 1;
      return this.#consume((candidate) => candidate === literal);
    }
    if (character === "*" || character === "+") {
      throw new TypeError("SIMILAR TO quantifier has no preceding expression");
    }
    if (character === "(") {
      if (depth >= MAX_SQL_NESTING_DEPTH) {
        throw new RangeError(`SIMILAR TO nesting exceeds ${String(MAX_SQL_NESTING_DEPTH)} levels`);
      }
      const child = this.#alternative(depth + 1);
      if (this.#characters[this.#index] !== ")") {
        throw new TypeError("SIMILAR TO pattern has an unmatched opening parenthesis");
      }
      this.#index += 1;
      return child;
    }
    if (character === "[") return this.#consume(this.#characterClass());
    if (character === "%") return this.#repeat(this.#consume(ANY_CHARACTER), false);
    if (character === "_") return this.#consume(ANY_CHARACTER);
    return this.#consume((candidate) => candidate === character);
  }

  #characterClass(): CharacterMatcher {
    let source = "[";
    let closed = false;
    let hasMember = false;
    while (this.#index < this.#characters.length) {
      const character = this.#characters[this.#index] ?? "";
      this.#index += 1;
      if (character === this.escape) {
        const literal = this.#characters[this.#index];
        if (literal === undefined) {
          throw new TypeError("SIMILAR TO pattern ends with its escape character");
        }
        this.#index += 1;
        source += ["\\", "]", "^", "-"].includes(literal) ? `\\${literal}` : literal;
        hasMember = true;
        continue;
      }
      if (character === "]") {
        closed = true;
        break;
      }
      source += character;
      if (character !== "^" || source.length > 2) hasMember = true;
    }
    if (!closed) throw new TypeError("SIMILAR TO pattern has an unterminated character class");
    if (!hasMember) throw new TypeError("SIMILAR TO pattern has an empty character class");
    source += "]";
    let expression: RegExp;
    try {
      // The host expression is deliberately limited to one character class and one codepoint.
      // It cannot contain repetition, grouping, or alternation, so backtracking work is constant.
      expression = new RegExp(`^(?:${source})$`, "u");
    } catch {
      throw new TypeError("SIMILAR TO pattern has an invalid character class");
    }
    return (candidate) => expression.test(candidate);
  }
}

function nfaMatcher(pattern: string, escape: string): WeightedPatternMatcher {
  const compiler = new SimilarCompiler(pattern, escape);
  const fragment = compiler.compile();
  const { states } = compiler;
  const test = (value: string): boolean => {
    if (typeof value !== "string") throw new TypeError("SIMILAR TO input must be a string");
    let remaining = MAX_SQL_PATTERN_MATCH_STEPS;
    const marks = new Uint32Array(states.length);
    const stack: number[] = [];
    let active: number[] = [];
    let next: number[] = [];
    let generation = 1;
    const addClosure = (seed: number, output: number[]): void => {
      stack.push(seed);
      while (stack.length > 0) {
        if (--remaining < 0) throwPatternWorkLimit();
        const stateIndex = stack.pop();
        if (stateIndex === undefined || marks[stateIndex] === generation) continue;
        marks[stateIndex] = generation;
        output.push(stateIndex);
        for (const target of states[stateIndex]?.[0] ?? []) stack.push(target);
      }
    };
    addClosure(fragment[0], active);
    for (const character of value) {
      generation += 1;
      next.length = 0;
      for (const stateIndex of active) {
        if (--remaining < 0) throwPatternWorkLimit();
        const transition = states[stateIndex]?.[1];
        if (transition?.[1](character) === true) addClosure(transition[0], next);
      }
      if (next.length === 0) return false;
      [active, next] = [next, active];
    }
    return active.includes(fragment[1]);
  };
  // State objects, epsilon arrays, transition objects, and matcher closures all cost materially
  // more than their integer payload. A conservative model keeps large compiled patterns out of
  // the cache instead of pretending their Uint32-sized indexes are the whole retained graph.
  return { test, retainedSize: states.length * 32 };
}

const regexCache = new Map<string, RegExp>();

/**
 * PostgreSQL's ~ / ~* / !~ / !~* operators and REGEXP_REPLACE, compiled as JavaScript regular
 * expressions. Advanced regular expressions and JavaScript agree on the everyday syntax; the
 * `n` flag makes `.` and anchors newline-sensitive, `i` is case-insensitive, and `g` replaces
 * every match. Patterns are bounded like every other SQL pattern.
 */
export function compileRegexPattern(pattern: string, flags = ""): RegExp {
  assertBoundedPattern(pattern, "regular expression");
  const normalized = [...new Set(flags)].sort().join("");
  const key = `${normalized}\u0000${pattern}`;
  const cached = regexCache.get(key);
  if (cached !== undefined) {
    cached.lastIndex = 0;
    return cached;
  }
  let jsFlags = "u";
  if (normalized.includes("i")) jsFlags += "i";
  if (normalized.includes("g")) jsFlags += "g";
  if (normalized.includes("n")) jsFlags += "m";
  else jsFlags += "s";
  let expression: RegExp;
  try {
    expression = new RegExp(pattern, jsFlags);
  } catch {
    try {
      expression = new RegExp(pattern, jsFlags.replace("u", ""));
    } catch (error) {
      throw new TypeError(
        `Invalid regular expression: ${error instanceof Error ? error.message : pattern}`,
        { cause: error },
      );
    }
  }
  if (regexCache.size >= 256) regexCache.clear();
  regexCache.set(key, expression);
  return expression;
}

/** PostgreSQL SIMILAR TO compiled to a Thompson NFA with bounded deterministic work. */
export function compileSimilarPattern(pattern: string, escape = "\\"): SqlPatternMatcher {
  assertBoundedPattern(pattern, "SIMILAR TO pattern");
  const exactEscape = exactSingleCharacter(escape, "SIMILAR TO escape");
  const key = JSON.stringify([exactEscape, pattern]);
  const cached = cachedPattern(similarCache, key);
  if (cached !== undefined) return cached;
  const matcher = nfaMatcher(pattern, exactEscape);
  retainPattern(similarCache, key, matcher, similarCacheRetainedSize, (size) => {
    similarCacheRetainedSize = size;
  });
  return matcher;
}

function assertBoundedPattern(
  value: string,
  label: string,
  limit = MAX_SQL_PATTERN_CHARACTERS,
): void {
  if (value.length > limit) {
    throw new RangeError(`${label} exceeds ${String(limit)} characters`);
  }
  assertWellFormedString(value, label);
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
  const external = externalSqlDomainValue(value);
  if (typeof external !== "string") throw new TypeError(`${name} requires a string argument`);
  return external;
}
