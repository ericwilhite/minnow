/**
 * Predicates pushed down to encoded pages: a comparison of one column with constants, compiled
 * once per column chunk (`compilePredicate`) into intervals in the column's own order, then
 * used to prune groups and pages by their statistics (`chunkMayMatch`, `pageMayMatch`) and to
 * evaluate pages without materializing values (select.ts).
 *
 * ## Semantics (PostgreSQL's, as the executor evaluates them)
 * - A comparison is true, false or unknown; only true selects. NULL rows never satisfy a
 *   comparison, `<>` and `IN` included; `IS NULL` / `IS NOT NULL` test validity alone.
 * - Constants are non-null (the caller folds `x = NULL` to false). `IN` matches any listed value
 *   (an empty list matches nothing); `BETWEEN a AND b` is `a <= x AND x <= b` (empty if a > b).
 * - Integers (int32, date, int64, time, timestamp): exact. A constant may be a bigint or a finite
 *   number; a fractional number compares exactly (`x < 2.5` is `x <= 2`).
 * - Floats (float64, float32 widened): NaN equals NaN and sorts above +Infinity; −0 equals +0.
 * - Decimals of a fixed-scale chunk: exact comparison of `coefficient × 10^−scale` with the
 *   stored coefficients at the chunk's scale; specials order −Infinity < finite < +Infinity <
 *   NaN, and NaN = NaN.
 * - text, bytes, uuid: byte order (C collation, the executor's). enum: `=`, `<>`, `IN` only (its
 *   order is the declaration order, which pages do not store).
 * - Not pushed down (`compilePredicate` returns null: the caller evaluates it): interval, json,
 *   arrays, mixed-scale decimals, enum ordering. `IS [NOT] NULL` compiles for every type.
 *
 * ## Pruning
 * A group or page is skipped only when its statistics prove no row qualifies: `IS NULL` needs a
 * null, every other test a non-null row whose value can satisfy it. Statistics that are absent
 * or not in the expected form (a mixed-scale chunk's sort keys, a decimal page with specials)
 * never prune. Byte bounds are conservative (a truncated min is ≤ every value, a max is an upper
 * bound), so they prune soundly. When in doubt, nothing is pruned.
 */
import type { PageEntry } from "../format/manifest.js";
import type { ColumnStatistics, PhysicalType, StatValue } from "../types.js";
import { compareBytes } from "../vector/varlen.js";
import { chunkTagOf } from "./format.js";

export type CompareOperator = "=" | "<>" | "<" | "<=" | ">" | ">=";

/** A decimal constant: `coefficient × 10^−scale`, or a special. */
export interface DecimalConstant {
  readonly coefficient: bigint;
  readonly scale: number;
  readonly special?: "NaN" | "Infinity" | "-Infinity";
}

/** A constant in the column's stored family (see the module comment). */
export type PredicateConstant = boolean | number | bigint | Uint8Array | DecimalConstant;

export type ColumnPredicate =
  | { readonly op: CompareOperator; readonly value: PredicateConstant }
  | { readonly op: "in"; readonly values: readonly PredicateConstant[] }
  | { readonly op: "between"; readonly low: PredicateConstant; readonly high: PredicateConstant }
  | { readonly op: "isNull" }
  | { readonly op: "isNotNull" };

/** The order a compiled predicate's intervals live in. */
export type Domain = "int" | "float" | "bytes";

/** A closed or open bound; null is unbounded. */
interface Bound<T> {
  readonly value: T;
  readonly inclusive: boolean;
}

/** One interval of the domain; null bounds are infinite. */
export interface Interval<T> {
  readonly low: Bound<T> | null;
  readonly high: Bound<T> | null;
}

/** Matches of the three decimal specials (also used for float NaN: `nan`). */
export interface SpecialMatches {
  readonly nan: boolean;
  readonly positive: boolean;
  readonly negative: boolean;
}

/**
 * A predicate compiled for one chunk. `values` is null for `IS [NOT] NULL`. Integer intervals
 * are closed (bigint); float and byte intervals keep inclusive flags. Intervals are sorted and
 * disjoint.
 */
export interface CompiledPredicate {
  readonly tag: number;
  readonly nulls: "isNull" | "isNotNull" | null;
  readonly domain: Domain;
  readonly ints: ReadonlyArray<readonly [bigint | null, bigint | null]>;
  readonly floats: ReadonlyArray<Interval<number>>;
  readonly bytes: ReadonlyArray<Interval<Uint8Array>>;
  /** Floats: whether NaN matches. Decimals: what each special matches. */
  readonly specials: SpecialMatches;
  /** True when no non-null value can match (pages then select nothing). */
  readonly empty: boolean;
  /** True when every non-null finite value matches (`x <> c` for a fractional c, …). */
  readonly all: boolean;
}

// ── Comparisons ──────────────────────────────────────────────────────────────────────────────

/** PostgreSQL float order: NaN above +Infinity and equal to NaN; −0 equals +0. */
export function compareFloat(a: number, b: number): number {
  if (a !== a) return b !== b ? 0 : 1;
  if (b !== b) return -1;
  return a < b ? -1 : a > b ? 1 : 0;
}

const bytesCompare = (a: Uint8Array, b: Uint8Array): number =>
  compareBytes(a, 0, a.length, b, 0, b.length);

// ── Compilation ──────────────────────────────────────────────────────────────────────────────

type Op = CompareOperator;

/** The column's pushdown family, or null when it has none (only null tests compile). */
function familyOf(
  type: PhysicalType,
  scale: number | null,
): "int" | "bool" | "float" | "decimal" | "bytes" | "enum" | null {
  switch (type.kind) {
    case "int32":
    case "date":
    case "int64":
    case "time":
    case "timestamp":
      return "int";
    case "bool":
      return "bool";
    case "float64":
    case "float32":
      return "float";
    case "decimal":
      return scale === null ? null : "decimal";
    case "text":
    case "bytes":
    case "uuid":
      return "bytes";
    case "enum":
      return "enum";
    default:
      return null;
  }
}

const POW10: bigint[] = [];
function pow10(exponent: number): bigint {
  let value = POW10[exponent];
  if (value === undefined) {
    value = 10n ** BigInt(exponent);
    POW10[exponent] = value;
  }
  return value;
}

/** floor(numerator / denominator) for a positive denominator. */
function floorDiv(numerator: bigint, denominator: bigint): bigint {
  const quotient = numerator / denominator;
  return numerator % denominator !== 0n && numerator < 0n ? quotient - 1n : quotient;
}

/**
 * An exact rational constant as `floor` and whether it is an integer: x op c over integers is
 * then x op' floor (see `intIntervals`).
 */
interface Rational {
  readonly floor: bigint;
  readonly exact: boolean;
}

function intConstant(value: PredicateConstant): Rational {
  if (typeof value === "bigint") return { floor: value, exact: true };
  if (typeof value === "number" && Number.isFinite(value)) {
    const floor = Math.floor(value);
    return { floor: BigInt(floor), exact: floor === value };
  }
  throw new RangeError("An integer column compares with a bigint or a finite number");
}

/** The decimal constant at the chunk's scale; null for a special. */
function decimalConstant(value: PredicateConstant, scale: number): Rational | null {
  if (typeof value !== "object" || value instanceof Uint8Array)
    throw new RangeError("A decimal column compares with a decimal constant");
  if (value.special !== undefined) return null;
  if (!Number.isInteger(value.scale) || value.scale < 0)
    throw new RangeError("A decimal constant needs a non-negative integer scale");
  if (value.scale <= scale)
    return { floor: value.coefficient * pow10(scale - value.scale), exact: true };
  const divisor = pow10(value.scale - scale);
  return {
    floor: floorDiv(value.coefficient, divisor),
    exact: value.coefficient % divisor === 0n,
  };
}

/** Integer values x with `x op c`, as closed intervals. */
function intIntervals(op: Op, c: Rational): Array<[bigint | null, bigint | null]> {
  const { floor, exact } = c;
  switch (op) {
    case "=":
      return exact ? [[floor, floor]] : [];
    case "<>":
      return exact
        ? [
            [null, floor - 1n],
            [floor + 1n, null],
          ]
        : [[null, null]];
    case "<":
      return [[null, exact ? floor - 1n : floor]];
    case "<=":
      return [[null, floor]];
    case ">":
      return [[floor + 1n, null]];
    case ">=":
      return [[exact ? floor : floor + 1n, null]];
  }
}

const DECIMAL_RANK = { "-Infinity": 0, finite: 1, Infinity: 2, NaN: 3 } as const;

function rankTest(op: Op, a: number, b: number): boolean {
  switch (op) {
    case "=":
      return a === b;
    case "<>":
      return a !== b;
    case "<":
      return a < b;
    case "<=":
      return a <= b;
    case ">":
      return a > b;
    case ">=":
      return a >= b;
  }
}

/** One comparison, compiled: finite-value intervals plus what the specials match. */
interface Part {
  ints: Array<[bigint | null, bigint | null]>;
  floats: Array<Interval<number>>;
  bytes: Array<Interval<Uint8Array>>;
  specials: SpecialMatches;
}

const NONE: SpecialMatches = { nan: false, positive: false, negative: false };

function compareFloats(op: Op, c: number): Part {
  const part: Part = { ints: [], floats: [], bytes: [], specials: NONE };
  if (c !== c) {
    // Against NaN: only NaN equals it; every other value is below it.
    const below: Interval<number> = { low: null, high: null };
    const nanMatches = op === "=" || op === "<=" || op === ">=";
    part.specials = { ...NONE, nan: nanMatches };
    if (op === "<>" || op === "<" || op === "<=") part.floats = [below];
    return part;
  }
  const at = (inclusive: boolean): Bound<number> => ({ value: c, inclusive });
  switch (op) {
    case "=":
      part.floats = [{ low: at(true), high: at(true) }];
      break;
    case "<>":
      part.floats = [
        { low: null, high: at(false) },
        { low: at(false), high: null },
      ];
      part.specials = { ...NONE, nan: true };
      break;
    case "<":
      part.floats = [{ low: null, high: at(false) }];
      break;
    case "<=":
      part.floats = [{ low: null, high: at(true) }];
      break;
    case ">":
      part.floats = [{ low: at(false), high: null }];
      part.specials = { ...NONE, nan: true };
      break;
    case ">=":
      part.floats = [{ low: at(true), high: null }];
      part.specials = { ...NONE, nan: true };
      break;
  }
  return part;
}

function compareBytesConstant(op: Op, c: Uint8Array): Part {
  const at = (inclusive: boolean): Bound<Uint8Array> => ({ value: c, inclusive });
  const bytes: Array<Interval<Uint8Array>> =
    op === "="
      ? [{ low: at(true), high: at(true) }]
      : op === "<>"
        ? [
            { low: null, high: at(false) },
            { low: at(false), high: null },
          ]
        : op === "<"
          ? [{ low: null, high: at(false) }]
          : op === "<="
            ? [{ low: null, high: at(true) }]
            : op === ">"
              ? [{ low: at(false), high: null }]
              : [{ low: at(true), high: null }];
  return { ints: [], floats: [], bytes, specials: NONE };
}

function compareDecimals(op: Op, value: PredicateConstant, scale: number): Part {
  const c = decimalConstant(value, scale);
  const rank =
    c === null ? DECIMAL_RANK[(value as DecimalConstant).special ?? "NaN"] : DECIMAL_RANK.finite;
  const specials: SpecialMatches = {
    nan: rankTest(op, DECIMAL_RANK.NaN, rank),
    positive: rankTest(op, DECIMAL_RANK.Infinity, rank),
    negative: rankTest(op, DECIMAL_RANK["-Infinity"], rank),
  };
  const ints =
    c === null
      ? rankTest(op, DECIMAL_RANK.finite, rank)
        ? ([[null, null]] as Array<[bigint | null, bigint | null]>)
        : []
      : intIntervals(op, c);
  return { ints, floats: [], bytes: [], specials };
}

/** Sorts and merges integer intervals (closed; adjacent ones merge). */
function mergeInts(
  intervals: Array<[bigint | null, bigint | null]>,
): Array<[bigint | null, bigint | null]> {
  const live = intervals.filter(([low, high]) => low === null || high === null || low <= high);
  live.sort(([a], [b]) =>
    a === null ? (b === null ? 0 : -1) : b === null ? 1 : a < b ? -1 : a > b ? 1 : 0,
  );
  const out: Array<[bigint | null, bigint | null]> = [];
  for (const [low, high] of live) {
    const last = out[out.length - 1];
    if (last !== undefined && (last[1] === null || (low !== null && low <= last[1] + 1n))) {
      if (last[1] !== null && (high === null || high > last[1])) last[1] = high;
      continue;
    }
    out.push([low, high]);
  }
  return out;
}

/** `a` ≤ `b` as lower bounds (null = −∞). */
function lowBefore<T>(
  a: Bound<T> | null,
  b: Bound<T> | null,
  compare: (x: T, y: T) => number,
): number {
  if (a === null) return b === null ? 0 : -1;
  if (b === null) return 1;
  const order = compare(a.value, b.value);
  if (order !== 0) return order;
  return a.inclusive === b.inclusive ? 0 : a.inclusive ? -1 : 1;
}

/** Sorts and merges intervals with inclusive flags; drops empty ones. */
function mergeOrdered<T>(
  intervals: Array<Interval<T>>,
  compare: (x: T, y: T) => number,
): Array<Interval<T>> {
  const live = intervals.filter(({ low, high }) => {
    if (low === null || high === null) return true;
    const order = compare(low.value, high.value);
    return order < 0 || (order === 0 && low.inclusive && high.inclusive);
  });
  live.sort((a, b) => lowBefore(a.low, b.low, compare));
  const out: Array<Interval<T>> = [];
  for (const interval of live) {
    const last = out[out.length - 1];
    if (last !== undefined) {
      // Overlapping or touching (one side inclusive): extend.
      const touches =
        last.high === null ||
        interval.low === null ||
        (() => {
          const order = compare(interval.low.value, last.high.value);
          return order < 0 || (order === 0 && (interval.low.inclusive || last.high.inclusive));
        })();
      if (touches) {
        const high =
          last.high === null || interval.high === null
            ? null
            : (() => {
                const order = compare(interval.high.value, last.high.value);
                if (order > 0) return interval.high;
                if (order < 0) return last.high;
                return interval.high.inclusive ? interval.high : last.high;
              })();
        out[out.length - 1] = { low: last.low, high };
        continue;
      }
    }
    out.push(interval);
  }
  return out;
}

/**
 * Compiles `predicate` for a chunk of `type` (`scale`: the directory's decimal scale, null for a
 * mixed-scale chunk). Null when the comparison is not pushed down for this type (see the module
 * comment); throws `RangeError` for a constant of the wrong family.
 */
export function compilePredicate(
  type: PhysicalType,
  scale: number | null,
  predicate: ColumnPredicate,
): CompiledPredicate | null {
  const tag = chunkTagOf(type);
  if (predicate.op === "isNull" || predicate.op === "isNotNull")
    return {
      tag,
      nulls: predicate.op,
      domain: "int",
      ints: [],
      floats: [],
      bytes: [],
      specials: NONE,
      empty: false,
      all: true,
    };
  const family = familyOf(type, scale);
  if (family === null) return null;
  if (family === "enum" && predicate.op !== "=" && predicate.op !== "<>" && predicate.op !== "in")
    return null;
  const one = (op: Op, value: PredicateConstant): Part => {
    switch (family) {
      case "int":
        return {
          ints: intIntervals(op, intConstant(value)),
          floats: [],
          bytes: [],
          specials: NONE,
        };
      case "bool": {
        if (typeof value !== "boolean")
          throw new RangeError("A bool column compares with a boolean");
        return {
          ints: intIntervals(op, { floor: value ? 1n : 0n, exact: true }),
          floats: [],
          bytes: [],
          specials: NONE,
        };
      }
      case "float":
        if (typeof value !== "number")
          throw new RangeError("A float column compares with a number");
        return compareFloats(op, value);
      case "decimal":
        return compareDecimals(op, value, scale ?? 0);
      case "bytes":
      case "enum":
        if (!(value instanceof Uint8Array))
          throw new RangeError("A byte column compares with bytes");
        return compareBytesConstant(op, value);
    }
  };
  let parts: Part[];
  let intersect = false;
  if (predicate.op === "in") parts = predicate.values.map((value) => one("=", value));
  else if (predicate.op === "between") {
    parts = [one(">=", predicate.low), one("<=", predicate.high)];
    intersect = true;
  } else parts = [one(predicate.op, predicate.value)];
  const domain: Domain =
    family === "float" ? "float" : family === "bytes" || family === "enum" ? "bytes" : "int";
  let ints: Array<[bigint | null, bigint | null]>;
  let floats: Array<Interval<number>>;
  let bytes: Array<Interval<Uint8Array>>;
  let specials: SpecialMatches;
  if (intersect) {
    const [a, b] = [parts[0], parts[1]] as [Part, Part];
    ints = intersectInts(a.ints, b.ints);
    floats = intersectOrdered(a.floats, b.floats, compareFloat);
    bytes = intersectOrdered(a.bytes, b.bytes, bytesCompare);
    specials = {
      nan: a.specials.nan && b.specials.nan,
      positive: a.specials.positive && b.specials.positive,
      negative: a.specials.negative && b.specials.negative,
    };
  } else {
    ints = mergeInts(parts.flatMap((part) => part.ints));
    floats = mergeOrdered(
      parts.flatMap((part) => part.floats),
      compareFloat,
    );
    bytes = mergeOrdered(
      parts.flatMap((part) => part.bytes),
      bytesCompare,
    );
    specials = {
      nan: parts.some((part) => part.specials.nan),
      positive: parts.some((part) => part.specials.positive),
      negative: parts.some((part) => part.specials.negative),
    };
  }
  const intervals =
    domain === "int" ? ints.length : domain === "float" ? floats.length : bytes.length;
  const all =
    domain === "int"
      ? ints.length === 1 && ints[0]?.[0] === null && ints[0][1] === null
      : domain === "float"
        ? floats.length === 1 && floats[0]?.low === null && floats[0].high === null
        : bytes.length === 1 && bytes[0]?.low === null && bytes[0].high === null;
  return {
    tag,
    nulls: null,
    domain,
    ints,
    floats,
    bytes,
    specials,
    empty: intervals === 0 && !specials.nan && !specials.positive && !specials.negative,
    all,
  };
}

function intersectInts(
  a: Array<[bigint | null, bigint | null]>,
  b: Array<[bigint | null, bigint | null]>,
): Array<[bigint | null, bigint | null]> {
  const out: Array<[bigint | null, bigint | null]> = [];
  for (const [aLow, aHigh] of a)
    for (const [bLow, bHigh] of b) {
      const low = aLow === null ? bLow : bLow === null ? aLow : aLow > bLow ? aLow : bLow;
      const high = aHigh === null ? bHigh : bHigh === null ? aHigh : aHigh < bHigh ? aHigh : bHigh;
      out.push([low, high]);
    }
  return mergeInts(out);
}

function intersectOrdered<T>(
  a: Array<Interval<T>>,
  b: Array<Interval<T>>,
  compare: (x: T, y: T) => number,
): Array<Interval<T>> {
  const out: Array<Interval<T>> = [];
  for (const x of a)
    for (const y of b) {
      const low = lowBefore(x.low, y.low, compare) >= 0 ? x.low : y.low;
      const high = highAfter(x.high, y.high, compare) <= 0 ? x.high : y.high;
      out.push({ low, high });
    }
  return mergeOrdered(out, compare);
}

/** Compares upper bounds (null = +∞): an exclusive bound is below an inclusive one. */
function highAfter<T>(
  a: Bound<T> | null,
  b: Bound<T> | null,
  compare: (x: T, y: T) => number,
): number {
  if (a === null) return b === null ? 0 : 1;
  if (b === null) return -1;
  const order = compare(a.value, b.value);
  if (order !== 0) return order;
  return a.inclusive === b.inclusive ? 0 : a.inclusive ? 1 : -1;
}

// ── Pruning by statistics ────────────────────────────────────────────────────────────────────

/** Whether `[min, max]` (the non-null values' bounds, in the domain's order) meets an interval. */
function rangeMeets(compiled: CompiledPredicate, min: StatValue, max: StatValue): boolean {
  switch (compiled.domain) {
    case "int": {
      const toBig = (value: StatValue): bigint | null =>
        typeof value === "bigint"
          ? value
          : typeof value === "number" && Number.isInteger(value)
            ? BigInt(value)
            : typeof value === "boolean"
              ? value
                ? 1n
                : 0n
              : null;
      const low = toBig(min);
      const high = toBig(max);
      if (low === null || high === null) return true; // not in the expected form
      return compiled.ints.some(([a, b]) => (a === null || a <= high) && (b === null || b >= low));
    }
    case "float": {
      if (typeof min !== "number" || typeof max !== "number") return true;
      // NaN is the point above +Infinity.
      if (compiled.specials.nan && max !== max) return true;
      const top = max !== max ? Number.POSITIVE_INFINITY : max;
      if (min !== min) return false; // every value is NaN, and NaN does not match (above)
      return compiled.floats.some(
        ({ low, high }) =>
          (low === null ||
            (low.inclusive
              ? compareFloat(low.value, top) <= 0
              : compareFloat(low.value, top) < 0)) &&
          (high === null ||
            (high.inclusive
              ? compareFloat(high.value, min) >= 0
              : compareFloat(high.value, min) > 0)),
      );
    }
    case "bytes": {
      if (!(min instanceof Uint8Array) || !(max instanceof Uint8Array)) return true;
      // `min` ≤ every value and `max` ≥ every value (truncated bounds are conservative).
      return compiled.bytes.some(
        ({ low, high }) =>
          (low === null ||
            (low.inclusive
              ? bytesCompare(low.value, max) <= 0
              : bytesCompare(low.value, max) < 0)) &&
          (high === null ||
            (high.inclusive
              ? bytesCompare(high.value, min) >= 0
              : bytesCompare(high.value, min) > 0)),
      );
    }
  }
}

/**
 * Whether rows with these statistics may satisfy `compiled`: false only when provably none does.
 * `rowCount` rows, `nullCount` of them null, non-null values within `[min, max]` (null: unknown).
 */
export function statsMayMatch(
  compiled: CompiledPredicate,
  rowCount: number,
  nullCount: number,
  min: StatValue | null,
  max: StatValue | null,
): boolean {
  if (compiled.nulls === "isNull") return nullCount > 0;
  if (compiled.nulls === "isNotNull") return nullCount < rowCount;
  if (nullCount >= rowCount || compiled.empty) return false;
  if (compiled.all || min === null || max === null) return true;
  // Decimal specials never appear in a page or chunk with bounds (encode.ts `noStats`).
  return rangeMeets(compiled, min, max);
}

/** Group pruning: whether a column chunk of `rowCount` rows may hold a qualifying row. */
export function chunkMayMatch(
  compiled: CompiledPredicate,
  rowCount: number,
  statistics: ColumnStatistics,
): boolean {
  return statsMayMatch(compiled, rowCount, statistics.nullCount, statistics.min, statistics.max);
}

/** Page pruning, from the page's directory entry. */
export function pageMayMatch(compiled: CompiledPredicate, entry: PageEntry): boolean {
  return statsMayMatch(compiled, entry.rowCount, entry.nullCount, entry.min, entry.max);
}
