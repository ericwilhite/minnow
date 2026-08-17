import type { DatabaseRow } from "./database.js";
import type { ColumnDefault } from "../storage/types.js";
import { SqlCompileError } from "./errors.js";
import {
  cachedQueryTerms,
  ftsBm25Row,
  FtsStatsAccumulator,
  ftsMatchTruth,
  renderDocumentValue,
  tokenize as ftsTokenize,
  validateFtsQuery,
  type FtsStats,
} from "./fts.js";
import { QueryMemoryContext, type QueryMemoryUsage } from "./memory.js";
import { buildSortKeyColumn } from "./sort-keys.js";
import { stringArgument } from "./sql-semantics.js";
import { jsonAtPath, jsonConstructor, jsonIsValid, parseJsonPath } from "./sql-json.js";
import { optimizePlan } from "./optimizer.js";
import {
  compareSqlValues as compareValues,
  compileLikePattern,
  encodeSqlEqualityValue,
  roundSqlNumber,
} from "./sql-semantics.js";
import {
  columnarTableFromRows,
  prepareVectorQuery,
  type ColumnarTable,
  type AsyncQueryExecutionOptions,
  type PreparedVectorQuery,
} from "./vector.js";

export type QueryValue = boolean | number | string | Date | null;
export type QueryRow = Record<string, QueryValue>;

export interface QueryResult {
  columns: string[];
  rows: QueryRow[];
}

export interface PreparedQuery {
  readonly sql: string;
  readonly tables: string[];
  /**
   * Current retained and high-water byte counts for the documented modeled query-memory scope.
   * Result-construction bytes affect the peak but transfer out of the context when execute returns.
   */
  readonly memoryUsage: QueryMemoryUsage;
  execute(): QueryResult;
  executeAsync(options?: AsyncQueryExecutionOptions): Promise<QueryResult>;
  close(): void;
}

export interface QueryExecutionOptions {
  /**
   * Bounds the modeled vector, row-index, group/result payload, and ordering buffers. This is not a
   * total JavaScript heap limit; input preparation, container/allocator overhead, and returned-result
   * lifetime are excluded. The schema-less empty-table row adapter rejects configured budgets;
   * MinnowDatabase retains catalog types and does not use that fallback.
   */
  readonly executionMemoryBudgetBytes?: number;
}

export type BinaryOperator = "+" | "-" | "*" | "/" | "%" | "||";
export type ComparisonOperator = "=" | "!=" | "<>" | ">" | ">=" | "<" | "<=";
export type AggregateName = "COUNT" | "SUM" | "AVG" | "MIN" | "MAX";
export type ScalarFunctionName =
  | "ROUND"
  | "COALESCE"
  | "DATE_TRUNC"
  | "DATE_ADD"
  | "UPPER"
  | "LOWER"
  | "LENGTH"
  | "ABS"
  | "TRIM"
  | "LTRIM"
  | "RTRIM"
  | "SUBSTR"
  | "REPLACE"
  | "INSTR"
  | "NULLIF"
  | "GREATEST"
  | "LEAST"
  | "FLOOR"
  | "CEIL"
  | "MOD"
  | "POWER"
  | "SQRT"
  | "EXTRACT"
  | "CAST"
  | "OCTET_LENGTH"
  | "LPAD"
  | "RPAD"
  | "OVERLAY"
  | "CURRENT_DATE"
  | "CURRENT_TIMESTAMP"
  | "LOCALTIME"
  | "GROUPING"
  | "JSON_VALUE"
  | "JSON_QUERY"
  | "JSON_EXISTS"
  | "JSON_OBJECT"
  | "JSON_ARRAY"
  | "IS_JSON";

export const scalarFunctionNames: ReadonlySet<string> = new Set([
  "ROUND",
  "COALESCE",
  "DATE_TRUNC",
  "DATE_ADD",
  "UPPER",
  "LOWER",
  "LENGTH",
  "ABS",
  "TRIM",
  "LTRIM",
  "RTRIM",
  "SUBSTR",
  "REPLACE",
  "INSTR",
  "NULLIF",
  "GREATEST",
  "LEAST",
  "FLOOR",
  "CEIL",
  "MOD",
  "POWER",
  "SQRT",
  "EXTRACT",
  "CAST",
  "OCTET_LENGTH",
  "LPAD",
  "RPAD",
  "OVERLAY",
  "CURRENT_DATE",
  "CURRENT_TIMESTAMP",
  "LOCALTIME",
  "GROUPING",
  "JSON_VALUE",
  "JSON_QUERY",
  "JSON_EXISTS",
  "JSON_OBJECT",
  "JSON_ARRAY",
  "IS_JSON",
] satisfies ScalarFunctionName[]);

/**
 * The niladic datetime functions (F051-06/07/08). Their value is the statement's own clock
 * reading, so they are resolved once per execution rather than evaluated per row: every row of
 * one statement sees one instant, both executors agree, and constant folding leaves them alone.
 */
export const statementDatetimeNames: ReadonlySet<string> = new Set([
  "CURRENT_DATE",
  "CURRENT_TIMESTAMP",
  "LOCALTIME",
]);

/**
 * The spellings that reach those three. Every datetime the engine stores is an instant in UTC
 * and there is no session time zone, so LOCALTIMESTAMP and CURRENT_TIMESTAMP name one value,
 * as do LOCALTIME and CURRENT_TIME.
 */
/** Standard function spellings that share one canonical plan name. */
const functionSpellings: ReadonlyMap<string, string> = new Map([
  ["SUBSTRING", "SUBSTR"],
  ["CEILING", "CEIL"],
  ["CHAR_LENGTH", "LENGTH"],
  ["CHARACTER_LENGTH", "LENGTH"],
]);

const statementDatetimeAliases: ReadonlyMap<string, string> = new Map([
  ["CURRENT_DATE", "CURRENT_DATE"],
  ["CURRENT_TIMESTAMP", "CURRENT_TIMESTAMP"],
  ["LOCALTIMESTAMP", "CURRENT_TIMESTAMP"],
  ["CURRENT_TIME", "LOCALTIME"],
  ["LOCALTIME", "LOCALTIME"],
]);

export function isScalarFunctionName(
  name: AggregateName | ScalarFunctionName,
): name is ScalarFunctionName {
  return scalarFunctionNames.has(name);
}

/**
 * TRIM's shared body (E021-09, T056). The trim characters default to a single space and are
 * removed as a whole repeated unit, not as a set: `TRIM(LEADING 'ab' FROM 'ababc')` is `'c'`.
 * PostgreSQL reads a multi-character argument as a set instead, which is why the conformance
 * harness diffs single-character trims against both oracles and this form against neither.
 */
function trimEnds(
  name: string,
  value: unknown,
  characters: unknown,
  side: "leading" | "trailing" | "both",
): unknown {
  if (characters === null) return null;
  const text = stringArgument(name, value);
  const unit = characters === undefined ? " " : stringArgument(name, characters);
  if (unit.length === 0) return text;
  let start = 0;
  let end = text.length;
  if (side !== "trailing") {
    while (start + unit.length <= end && text.startsWith(unit, start)) start += unit.length;
  }
  if (side !== "leading") {
    while (end - unit.length >= start && text.startsWith(unit, end - unit.length)) {
      end -= unit.length;
    }
  }
  return text.slice(start, end);
}

/**
 * CAST conversions between the four logical types, matching the strict common ground of
 * SQLite and PostgreSQL: numeric strings parse or fail (never silently 0), integer targets
 * truncate toward zero, booleans render as 'true'/'false', datetimes as ISO strings, and a
 * number cast to datetime reads as milliseconds since the epoch.
 */
function castValue(value: unknown, target: string): unknown {
  if (target === "string") {
    if (typeof value === "string") return value;
    if (typeof value === "number") return String(value);
    if (typeof value === "boolean") return value ? "true" : "false";
    if (value instanceof Date) return value.toISOString();
  }
  if (target === "number" || target === "number-integer") {
    let parsed: number | undefined;
    if (typeof value === "number") parsed = value;
    else if (typeof value === "boolean") parsed = value ? 1 : 0;
    else if (typeof value === "string") {
      const text = value.trim();
      const candidate = text === "" ? Number.NaN : Number(text);
      if (!Number.isFinite(candidate)) {
        throw new TypeError(`Cannot cast this string to a number: ${value}`);
      }
      parsed = candidate;
    }
    if (parsed !== undefined) return target === "number-integer" ? Math.trunc(parsed) : parsed;
  }
  if (target === "boolean") {
    if (typeof value === "boolean") return value;
    if (typeof value === "number") {
      if (value === 0) return false;
      if (value === 1) return true;
      throw new TypeError(`Only 0 and 1 cast to boolean, got ${String(value)}`);
    }
    if (typeof value === "string") {
      const text = value.trim().toLowerCase();
      if (text === "true" || text === "t" || text === "1") return true;
      if (text === "false" || text === "f" || text === "0") return false;
      throw new TypeError(`Cannot cast this string to a boolean: ${value}`);
    }
  }
  if (target === "datetime") {
    if (value instanceof Date) return value;
    if (typeof value === "string" || typeof value === "number") {
      const parsed = new Date(value);
      if (Number.isFinite(parsed.getTime())) return parsed;
      throw new TypeError(`Cannot cast this value to a datetime: ${String(value)}`);
    }
  }
  throw new TypeError(`Unsupported CAST: ${typeof value} to ${target}`);
}

/**
 * Evaluates one scalar function over already-evaluated argument values. Every executor calls
 * through here, so a function behaves identically in the row executor, the vectorized executor,
 * and constant folding. COALESCE is not handled here — it short-circuits, so each call site
 * evaluates it lazily. A NULL first argument returns NULL (SQL scalar semantics); LENGTH and
 * SUBSTR count characters, not UTF-16 units, matching SQLite and PostgreSQL.
 */
export function scalarFunctionValue(
  name: Exclude<ScalarFunctionName, "COALESCE">,
  values: readonly unknown[],
): unknown {
  if (name === "GROUPING") {
    // T433. The grouping-sets desugar knows which columns each member aggregates away and
    // replaces every GROUPING call with its constant; reaching here means there was no
    // GROUP BY ROLLUP/CUBE/GROUPING SETS to answer it.
    throw new TypeError("GROUPING requires GROUP BY ROLLUP, CUBE, or GROUPING SETS");
  }
  if (statementDatetimeNames.has(name)) {
    // Resolution happens once per execution, before any executor sees the plan. Reaching here
    // means a call survived that pass, which would silently give one statement two clocks.
    throw new TypeError(`${name} must be resolved before execution`);
  }
  if (name === "DATE_TRUNC") return dateTruncValue(values[0], values[1]);
  if (name === "DATE_ADD") return dateAddValue(values[0], values[1], values[2]);
  if (name === "GREATEST" || name === "LEAST") {
    // NULL arguments are ignored, matching PostgreSQL; all-NULL yields NULL.
    let best: unknown;
    for (const value of values) {
      if (value === null || value === undefined) continue;
      if (
        best === undefined ||
        (name === "GREATEST" ? compareValues(value, best) > 0 : compareValues(value, best) < 0)
      ) {
        best = value;
      }
    }
    return best ?? null;
  }
  if (name === "JSON_ARRAY" || name === "JSON_OBJECT") {
    // These build from every argument, so a NULL first one is data, not an early exit.
    return jsonConstructor(name, values);
  }
  const first = values[0];
  if (first === null || first === undefined) return null;
  switch (name) {
    case "NULLIF": {
      const other = values[1];
      if (other === null || other === undefined) return first;
      return comparable(first) === comparable(other) ? null : first;
    }
    case "FLOOR":
      return Math.floor(numeric(first));
    case "CEIL":
      return Math.ceil(numeric(first));
    case "MOD": {
      if (values[1] === null || values[1] === undefined) return null;
      const divisor = numeric(values[1]);
      return divisor === 0 ? null : numeric(first) % divisor;
    }
    case "POWER": {
      if (values[1] === null || values[1] === undefined) return null;
      const raised = numeric(first) ** numeric(values[1]);
      if (!Number.isFinite(raised)) throw new TypeError("POWER produced a non-finite number");
      return raised;
    }
    case "SQRT": {
      const operand = numeric(first);
      if (operand < 0) throw new TypeError("SQRT requires a non-negative number");
      return Math.sqrt(operand);
    }
    case "LTRIM":
      return trimEnds("LTRIM", first, values[1], "leading");
    case "RTRIM":
      return trimEnds("RTRIM", first, values[1], "trailing");
    case "REPLACE": {
      if (values[1] === null || values[1] === undefined) return null;
      if (values[2] === null || values[2] === undefined) return null;
      const search = stringArgument("REPLACE", values[1]);
      if (search === "") return stringArgument("REPLACE", first);
      return stringArgument("REPLACE", first)
        .split(search)
        .join(stringArgument("REPLACE", values[2]));
    }
    case "INSTR": {
      if (values[1] === null || values[1] === undefined) return null;
      const haystack = stringArgument("INSTR", first);
      const needle = stringArgument("INSTR", values[1]);
      const index = haystack.indexOf(needle);
      // 1-based character position, 0 when absent, counting codepoints like LENGTH.
      return index === -1 ? 0 : Array.from(haystack.slice(0, index)).length + 1;
    }
    case "EXTRACT":
      return extractDatePart(typeof first === "string" ? first : "", values[1]);
    case "ROUND": {
      if (values.length > 1 && (values[1] === null || values[1] === undefined)) return null;
      const digits = values.length > 1 ? numeric(values[1]) : 0;
      return roundSqlNumber(numeric(first), digits);
    }
    case "ABS":
      return Math.abs(numeric(first));
    case "UPPER":
      return stringArgument("UPPER", first).toUpperCase();
    case "LOWER":
      return stringArgument("LOWER", first).toLowerCase();
    case "TRIM":
      // SQL TRIM removes spaces, not general whitespace.
      return trimEnds("TRIM", first, values[1], "both");
    case "LENGTH":
      return Array.from(stringArgument("LENGTH", first)).length;
    case "OCTET_LENGTH":
      return new TextEncoder().encode(stringArgument("OCTET_LENGTH", first)).length;
    case "IS_JSON": {
      const kind = values[1];
      return jsonIsValid(first, typeof kind === "string" ? kind : "value");
    }
    case "JSON_EXISTS":
      return jsonAtPath(first, values[1], "JSON_EXISTS").found;
    case "JSON_VALUE": {
      const found = jsonAtPath(first, values[1], "JSON_VALUE");
      if (!found.found) return null;
      const value = found.value;
      // A scalar comes back as itself; an object or array has no scalar value to give.
      if (value === null) return null;
      if (typeof value === "object") return null;
      return typeof value === "string" ? value : JSON.stringify(value);
    }
    case "JSON_QUERY": {
      const found = jsonAtPath(first, values[1], "JSON_QUERY");
      if (!found.found || found.value === undefined) return null;
      // JSON_QUERY returns JSON text, so a selected string keeps its quotes.
      return JSON.stringify(found.value);
    }

    case "LPAD":
    case "RPAD": {
      if (values[1] === null || values[1] === undefined) return null;
      const width = numeric(values[1]);
      if (!Number.isInteger(width) || width < 0) {
        throw new TypeError(`${name} length must be a non-negative integer`);
      }
      const text = Array.from(stringArgument(name, first));
      if (text.length >= width) return text.slice(0, width).join("");
      let fill = " ";
      if (values.length > 2) {
        if (values[2] === null || values[2] === undefined) return null;
        fill = stringArgument(name, values[2]);
      }
      const filler = Array.from(fill);
      // An empty fill cannot pad, so the value passes through, matching PostgreSQL.
      if (filler.length === 0) return text.join("");
      const padding: string[] = [];
      while (padding.length < width - text.length) {
        padding.push(filler[padding.length % filler.length] ?? "");
      }
      return name === "LPAD" ? padding.join("") + text.join("") : text.join("") + padding.join("");
    }
    case "OVERLAY": {
      // OVERLAY(s PLACING r FROM start [FOR length]) replaces `length` characters of `s`
      // starting at `start` with `r`; the default length is the replacement's own.
      for (const index of [1, 2, 3]) {
        if (index < values.length && (values[index] === null || values[index] === undefined)) {
          return null;
        }
      }
      const text = Array.from(stringArgument("OVERLAY", first));
      const replacement = Array.from(stringArgument("OVERLAY", values[1]));
      const start = numeric(values[2]);
      if (!Number.isInteger(start) || start < 1) {
        throw new TypeError("OVERLAY start must be a positive integer");
      }
      const span = values.length > 3 ? numeric(values[3]) : replacement.length;
      if (!Number.isInteger(span) || span < 0) {
        throw new TypeError("OVERLAY length must be a non-negative integer");
      }
      return [
        ...text.slice(0, start - 1),
        ...replacement,
        ...text.slice(Math.min(start - 1 + span, text.length)),
      ].join("");
    }
    case "CAST":
      return castValue(first, typeof values[1] === "string" ? values[1] : "");
    case "SUBSTR": {
      // SQL:2023 6.32: the result is the characters whose positions fall in both the requested
      // window and the string, so a start before 1 shortens the result instead of shifting it,
      // and a window entirely off the string is empty rather than an error.
      const text = Array.from(stringArgument("SUBSTR", first));
      if (values[1] === null || values[1] === undefined) return null;
      const start = numeric(values[1]);
      if (!Number.isInteger(start)) throw new TypeError("SUBSTR start must be an integer");
      let until = text.length + 1;
      if (values.length > 2) {
        if (values[2] === null || values[2] === undefined) return null;
        const length = numeric(values[2]);
        if (!Number.isInteger(length) || length < 0) {
          throw new TypeError("SUBSTR length must be a non-negative integer");
        }
        until = start + length;
      }
      const from = Math.max(start, 1);
      const to = Math.min(until, text.length + 1);
      return to <= from ? "" : text.slice(from - 1, to - 1).join("");
    }
  }
}

export const dateTruncUnits: ReadonlySet<string> = new Set([
  "year",
  "quarter",
  "month",
  "week",
  "day",
  "hour",
  "minute",
  "second",
]);

/**
 * Truncates a datetime to the start of the given unit, in UTC — the engine stores and
 * compares datetimes by instant and has no session time zone. Weeks start on Monday,
 * matching Postgres.
 */
const intervalUnits = new Map<string, { months: number; milliseconds: number }>([
  ["year", { months: 12, milliseconds: 0 }],
  ["quarter", { months: 3, milliseconds: 0 }],
  ["month", { months: 1, milliseconds: 0 }],
  ["week", { months: 0, milliseconds: 7 * 86_400_000 }],
  ["day", { months: 0, milliseconds: 86_400_000 }],
  ["hour", { months: 0, milliseconds: 3_600_000 }],
  ["minute", { months: 0, milliseconds: 60_000 }],
  ["second", { months: 0, milliseconds: 1_000 }],
]);

/**
 * `INTERVAL '1 month'`, `INTERVAL '2 years 3 days'`. Months are kept apart from milliseconds
 * rather than converted, because a month is not a fixed number of them: adding one to January 31
 * has to land on the end of February, which only calendar arithmetic can do.
 */
export function intervalLiteral(text: string): { months: number; milliseconds: number } {
  let months = 0;
  let milliseconds = 0;
  let matched = 0;
  for (const [, amount, unit] of text.trim().matchAll(/(-?\d+(?:\.\d+)?)\s*([a-zA-Z]+)/g)) {
    const singular = (unit ?? "").toLowerCase().replace(/s$/, "");
    const scale = intervalUnits.get(singular);
    if (scale === undefined) throw new TypeError(`Unknown INTERVAL unit: ${String(unit)}`);
    const count = Number(amount);
    if (scale.months !== 0 && !Number.isInteger(count * scale.months)) {
      throw new TypeError(`INTERVAL ${String(unit)} must be a whole number`);
    }
    months += count * scale.months;
    milliseconds += count * scale.milliseconds;
    matched += 1;
  }
  if (matched === 0) throw new TypeError(`Invalid INTERVAL literal: ${text}`);
  return { months, milliseconds };
}

/**
 * A datetime shifted by a calendar interval: whole months first, then milliseconds. A day that
 * does not exist in the target month clamps to that month's last day, which is what both SQLite
 * and PostgreSQL do with 31 January plus a month.
 */
export function dateAddValue(value: unknown, months: unknown, milliseconds: unknown): Date | null {
  if (value === null || value === undefined) return null;
  if (!(value instanceof Date)) throw new TypeError("Date arithmetic requires a datetime value");
  const monthCount = Number(months ?? 0);
  const shifted = new Date(value.getTime());
  if (monthCount !== 0) {
    const day = shifted.getUTCDate();
    shifted.setUTCDate(1);
    shifted.setUTCMonth(shifted.getUTCMonth() + monthCount);
    const lastDay = new Date(
      Date.UTC(shifted.getUTCFullYear(), shifted.getUTCMonth() + 1, 0),
    ).getUTCDate();
    shifted.setUTCDate(Math.min(day, lastDay));
  }
  return new Date(shifted.getTime() + Number(milliseconds ?? 0));
}

export function dateTruncValue(unit: unknown, value: unknown): Date | null {
  if (typeof unit !== "string" || !dateTruncUnits.has(unit.toLowerCase())) {
    throw new TypeError(
      "DATE_TRUNC requires a unit of year, quarter, month, week, day, hour, minute, or second",
    );
  }
  if (value === null || value === undefined) return null;
  if (!(value instanceof Date)) throw new TypeError("DATE_TRUNC requires a datetime value");
  const normalized = unit.toLowerCase();
  const year = value.getUTCFullYear();
  const month = value.getUTCMonth();
  const day = value.getUTCDate();
  switch (normalized) {
    case "year":
      return new Date(Date.UTC(year, 0, 1));
    case "quarter":
      return new Date(Date.UTC(year, Math.floor(month / 3) * 3, 1));
    case "month":
      return new Date(Date.UTC(year, month, 1));
    case "week": {
      const start = new Date(Date.UTC(year, month, day));
      start.setUTCDate(start.getUTCDate() - ((start.getUTCDay() + 6) % 7));
      return start;
    }
    case "day":
      return new Date(Date.UTC(year, month, day));
    case "hour":
      return new Date(Date.UTC(year, month, day, value.getUTCHours()));
    case "minute":
      return new Date(Date.UTC(year, month, day, value.getUTCHours(), value.getUTCMinutes()));
    default:
      return new Date(
        Date.UTC(
          year,
          month,
          day,
          value.getUTCHours(),
          value.getUTCMinutes(),
          value.getUTCSeconds(),
        ),
      );
  }
}

export type Expression =
  | { kind: "literal"; value: QueryValue }
  /** A `?` or `$n` placeholder; `index` is 0-based. Replaced by a literal at bind time. */
  | { kind: "parameter"; index: number }
  | { kind: "column"; reference: string }
  /**
   * `*`, or `alias.*` when `table` is set (E051-07). A qualified wildcard names one source and
   * may sit beside other select items; every executor entry expands it into that source's
   * columns, so past the entry only a bare wildcard — the whole-row projection — survives.
   */
  | { kind: "wildcard"; table?: string }
  | { kind: "binary"; operator: BinaryOperator; left: Expression; right: Expression }
  | {
      kind: "call";
      name: AggregateName | ScalarFunctionName;
      arguments: Expression[];
      distinct?: boolean;
    }
  | { kind: "list"; items: Expression[] }
  | { kind: "subquery"; block: CompiledQuery }
  | {
      kind: "condition";
      operator: PredicateOperator;
      left: Expression;
      right: Expression;
      /** LIKE/ILIKE escape character, from LIKE ... ESCAPE 'c'. */
      escape?: string;
    }
  | { kind: "logical"; operator: "and" | "or"; left: Expression; right: Expression }
  | { kind: "not"; operand: Expression }
  | { kind: "exists"; block: CompiledQuery; negated: boolean }
  | {
      kind: "case";
      branches: Array<{ when: Expression; then: Expression }>;
      otherwise?: Expression;
    }
  | {
      kind: "window";
      name: WindowFunctionName;
      partitionBy: Expression[];
      orderBy: Array<{
        expression: Expression;
        direction: "asc" | "desc";
        nulls?: "first" | "last";
      }>;
      argument?: Expression;
      /** LAG/LEAD row distance; parsed as a literal non-negative integer, defaulting to 1. */
      offset?: number;
      /** LAG/LEAD default when the offset row falls outside the partition; NULL when absent. */
      fallback?: QueryValue;
      frame?: WindowFrame;
    }
  | {
      kind: "fts";
      op: "match" | "bm25";
      /**
       * Column references forming the document, or "*" for every searchable column of the
       * single scan source. "*" stays unexpanded through compilation (both front ends emit the
       * identical node — plan parity), and the engine expands it against the catalog at
       * prepare time via expandFtsColumns.
       */
      columns: Expression[] | "*";
      query: string;
      /**
       * Corpus statistics for BM25, annotated by the executor onto its cloned plan before
       * evaluation; never set by compilation.
       */
      stats?: FtsStats;
    };

export type WindowFunctionName =
  | "ROW_NUMBER"
  | "RANK"
  | "DENSE_RANK"
  | "PERCENT_RANK"
  | "CUME_DIST"
  | "NTILE"
  | "LAG"
  | "LEAD"
  | "FIRST_VALUE"
  | "LAST_VALUE"
  | "NTH_VALUE"
  | AggregateName;

export interface WindowFrameBound {
  kind: "unbounded-preceding" | "preceding" | "current-row" | "following" | "unbounded-following";
  /** Row distance for preceding/following bounds (ROWS unit only). */
  offset?: number;
}

/** How a frame's rows are excluded around the current row (T612). */
export type WindowFrameExclusion = "no-others" | "current-row" | "group" | "ties";

/** An explicit frame clause; absent means the SQL default for the window's ordering. */
export interface WindowFrame {
  unit: "rows" | "range" | "groups";
  start: WindowFrameBound;
  end: WindowFrameBound;
  /** Rows excluded around the current row; absent means EXCLUDE NO OTHERS. */
  exclude?: WindowFrameExclusion;
}

export interface WindowSpec {
  alias: string;
  name: WindowFunctionName;
  partitionAliases: string[];
  orderAliases: Array<{ alias: string; direction: "asc" | "desc"; nulls?: "first" | "last" }>;
  /** Hidden inner alias of an aggregate window's argument; absent for COUNT(*) and rankings. */
  argumentAlias?: string;
  /** LAG/LEAD row distance. */
  offset?: number;
  /** LAG/LEAD default when the offset row falls outside the partition. */
  fallback?: QueryValue;
  frame?: WindowFrame;
}

/** The output column type of one window: rankings and most aggregates count, MIN/MAX carry. */
export function windowOutputType(
  window: WindowSpec,
  innerSchema: readonly SqlColumnSchema[],
): SqlColumnType {
  const carries =
    window.name === "MIN" ||
    window.name === "MAX" ||
    window.name === "LAG" ||
    window.name === "LEAD" ||
    window.name === "FIRST_VALUE" ||
    window.name === "LAST_VALUE" ||
    window.name === "NTH_VALUE";
  if (carries && window.argumentAlias !== undefined) {
    return innerSchema.find(({ name }) => name === window.argumentAlias)?.type ?? "number";
  }
  return "number";
}

export interface SelectItem {
  expression: Expression;
  alias: string;
}

export interface TableSource {
  table: string;
  alias: string;
  /** A parenthesized SELECT or expanded CTE body; `table` is then a unique synthetic name. */
  derived?: CompiledQuery;
  /** A top-level set operation; members combine positionally under the first member's schema. */
  union?: { blocks: CompiledQuery[]; ops: SetOperator[] };
  recursive?: RecursiveCte;
  /** A window-function desugar: the inner block executes, then window columns append. */
  windowed?: { block: CompiledQuery; windows: WindowSpec[] };
  /**
   * `FROM t AS y(a, b)`: positional new names for a base table's columns (E051-09). Every
   * executor entry turns the source into a derived projection, so past that point the rename
   * is an ordinary select list and nothing else has to know about it.
   */
  columnAliases?: string[];
}

export interface JoinPlan extends TableSource {
  kind: "inner" | "left";
  left: Expression;
  right: Expression;
  /** General ON condition for non-equi or multi-key joins; left/right are inert placeholders. */
  on?: Expression;
  /** Parser marker: FULL OUTER JOIN. Assembly desugars it into a union of two left joins. */
  full?: boolean;
  /**
   * Parser marker: NATURAL JOIN. Every execution entry replaces it with the equality
   * conjunction over the columns this source shares with the ones before it, so no executor
   * ever sees the marker (F401-01).
   */
  natural?: boolean;
}

export type SetOperator =
  "union" | "union all" | "intersect" | "intersect all" | "except" | "except all";

/**
 * A WITH RECURSIVE source: the base block seeds the working set, then the step block re-executes
 * with `reference` bound to the previous iteration's new rows (linear delta recursion) until no
 * new rows appear. UNION deduplicates against everything seen; UNION ALL appends raw.
 */
export interface RecursiveCte {
  reference: string;
  base: CompiledQuery;
  step: CompiledQuery;
  all: boolean;
}

const MAX_RECURSIVE_ITERATIONS = 10_000;
const MAX_RECURSIVE_ROWS = 1_000_000;

export type PredicateOperator =
  | ComparisonOperator
  | `${ComparisonOperator} ANY`
  | `${ComparisonOperator} ALL`
  | "IN"
  | "NOT IN"
  | "IS NULL"
  | "IS NOT NULL"
  | "LIKE"
  | "NOT LIKE"
  | "ILIKE"
  | "NOT ILIKE"
  | "IS DISTINCT FROM"
  | "IS NOT DISTINCT FROM"
  | "IS TRUE";

export interface Predicate {
  left: Expression;
  operator: PredicateOperator;
  right: Expression;
  /** LIKE/ILIKE escape character, carried from the parsed condition. */
  escape?: string;
}

export interface CompiledQuery {
  sql: string;
  base: TableSource;
  joins: JoinPlan[];
  select: SelectItem[];
  predicates: Predicate[];
  groupBy: Expression[];
  having: Predicate[];
  orderBy: Array<{
    expression: Expression;
    direction: "asc" | "desc";
    /**
     * Explicit NULL placement, absolute regardless of direction. Absent keeps the default:
     * NULL sorts smallest, so ASC puts NULLs first and DESC puts them last (SQLite's default).
     */
    nulls?: "first" | "last";
  }>;
  limit?: number;
  offset?: number;
  /**
   * SELECT DISTINCT * awaiting expansion: the wildcard's columns are unknown until input
   * schemas exist, so every executor entry expands this into a concrete select list plus a
   * matching GROUP BY exactly once (see expandDistinctWildcard), like MATCH(*).
   */
  distinctWildcard?: boolean;
  /** Parameter slots for LIMIT ? / OFFSET ?; binding resolves them into limit/offset. */
  limitParameter?: number;
  offsetParameter?: number;
  /**
   * FETCH FIRST n ROWS WITH TIES (F866): rows tying with the last retained row under the
   * ORDER BY are kept too. The limit cannot be pushed into a scan then, so every execution
   * entry runs the query unlimited and trims the ordered result.
   */
  limitWithTies?: boolean;
  /**
   * Number of `?`/`$n` placeholders in the whole statement; set only on the top-level plan.
   * A plan with placeholders must pass through bindPlanParameters before it prepares.
   */
  parameterCount?: number;
  /**
   * CURRENT_DATE / CURRENT_TIMESTAMP / LOCALTIME appear somewhere in the statement. Every
   * executor entry replaces them with one instant per execution, and results never memoize,
   * because the answer depends on the clock rather than on the data.
   */
  usesStatementDatetime?: boolean;
}

/** ORDER BY / LIMIT / OFFSET tail of a select or set operation. */
export interface SelectTail {
  orderBy: CompiledQuery["orderBy"];
  limit?: number;
  offset?: number;
  limitParameter?: number;
  offsetParameter?: number;
  limitWithTies?: boolean;
}

type RowContext = Record<string, DatabaseRow | undefined>;

interface Token {
  kind: "identifier" | "number" | "string" | "operator" | "punctuation" | "parameter" | "eof";
  /** For parameter tokens: the 1-based number of `$n`, or "" for positional `?`. */
  text: string;
  /** A `"double-quoted"` identifier: never a keyword, its text taken exactly as written. */
  quoted?: boolean;
  /**
   * Half-open character span in the tokenized text. String tokens span their quotes even though
   * `text` holds the unescaped value, so a squiggle covers what the author actually typed.
   */
  start: number;
  end: number;
}

/**
 * The virtual one-row source behind a FROM-less SELECT. The name cannot collide with a real
 * table (parentheses never survive identifier parsing unquoted), and its single hidden column
 * keeps the row alive through columnar conversion, which derives row count from column vectors.
 */
export const DUAL_TABLE = "(dual)";

/** The dual source's single row, shared by every resolution path. */
export function dualTableRows(): DatabaseRow[] {
  return [{ [DUAL_TABLE]: 1 }];
}

const createTableTypeNames: ReadonlyMap<string, SqlColumnType> = new Map([
  ["BOOLEAN", "boolean"],
  ["BOOL", "boolean"],
  ["INTEGER", "number"],
  ["INT", "number"],
  ["BIGINT", "number"],
  ["SMALLINT", "number"],
  ["REAL", "number"],
  ["FLOAT", "number"],
  ["NUMERIC", "number"],
  ["DECIMAL", "number"],
  ["TEXT", "string"],
  ["VARCHAR", "string"],
  ["CHAR", "string"],
  ["STRING", "string"],
  ["TIMESTAMP", "datetime"],
  ["TIMESTAMPTZ", "datetime"],
  ["DATETIME", "datetime"],
  ["DATE", "datetime"],
]);

const clauseKeywords = new Set([
  "WHERE",
  "NATURAL",
  "USING",
  "OUTER",
  "WINDOW",
  "GROUP",
  "HAVING",
  "ORDER",
  "LIMIT",
  "OFFSET",
  "FETCH",
  "INTERSECT",
  "EXCEPT",
  "JOIN",
  "INNER",
  "LEFT",
  "RIGHT",
  "FULL",
  "CROSS",
  "UNION",
  "RETURNING",
]);
const aggregateNames = new Set<AggregateName>(["COUNT", "SUM", "AVG", "MIN", "MAX"]);
/** Set functions the parser builds from COUNT/SUM rather than from their own accumulator. */
const statisticalAggregates = new Set([
  "VAR_POP",
  "VAR_SAMP",
  "VARIANCE",
  "STDDEV_POP",
  "STDDEV_SAMP",
  "STDDEV",
]);

export interface CompileQueryOptions {
  /** Set false to skip deterministic plan rewrites, for example to snapshot the raw plan. */
  readonly optimize?: boolean;
}

/**
 * Trims the statement the parser sees while remembering how far that shifted every position, so
 * compile errors report offsets into the caller's own text rather than the trimmed copy.
 */
/** Single-statement rule for every route except CREATE TRIGGER bodies. */
function rejectSemicolons(tokens: Token[]): void {
  for (const token of tokens) {
    if (token.kind === "punctuation" && token.text === ";") {
      throw new SqlCompileError("Run one SELECT statement at a time", token.start, 1);
    }
  }
}

function normalizeSql(sql: string): { text: string; offset: number } {
  return { text: sql.trim().replace(/;$/, "").trim(), offset: sql.length - sql.trimStart().length };
}

/**
 * Rethrows a compilation failure with a position. Errors that already carry one only shift into
 * the caller's coordinates; anything else takes the parser's current token as its anchor. Non-type
 * errors pass through untouched — they come from execution, not from reading the text.
 */
function throwLocated(error: unknown, offset: number, span: { start: number; end: number }): never {
  if (error instanceof SqlCompileError) {
    if (offset === 0) throw error;
    throw new SqlCompileError(error.message, error.offset + offset, error.length);
  }
  if (error instanceof TypeError) {
    throw new SqlCompileError(
      error.message,
      offset + span.start,
      Math.max(span.end - span.start, 0),
    );
  }
  throw error;
}

export function compileQuery(sql: string, options: CompileQueryOptions = {}): CompiledQuery {
  const { text, offset } = normalizeSql(sql);
  if (text.length === 0) throw new SqlCompileError("Enter a SELECT query", offset, 0);
  let parser: Parser | undefined;
  let plan: CompiledQuery;
  try {
    const tokens = tokenize(text);
    rejectSemicolons(tokens);
    parser = new Parser(tokens);
    plan = parser.parse(text);
  } catch (error) {
    throwLocated(error, offset, parser?.span ?? { start: 0, end: text.length });
  }
  let compiled: CompiledQuery;
  try {
    compiled = options.optimize === false ? plan : optimizePlan(plan);
  } catch (error) {
    // Compile-time rewrites (for example decorrelation) reject unsupported shapes; those
    // errors locate on the statement like any other compile failure.
    throwLocated(error, offset, { start: 0, end: text.length });
  }
  if (parser.parameterCount > 0) compiled.parameterCount = parser.parameterCount;
  if (parser.usesStatementDatetime) compiled.usesStatementDatetime = true;
  return compiled;
}

/**
 * Reads a column DEFAULT into the catalog's own representation: a constant, or the
 * CURRENT_TIMESTAMP family, which the catalog records as "now" and fills per inserted row.
 */
function columnDefaultFor(expression: Expression): ColumnDefault {
  if (expression.kind === "literal") {
    const value = expression.value;
    if (typeof value === "boolean" || typeof value === "number" || typeof value === "string") {
      return { kind: "literal", value };
    }
    if (value instanceof Date) return { kind: "literal", value: value.toISOString() };
  }
  if (
    expression.kind === "call" &&
    (expression.name === "CURRENT_TIMESTAMP" || expression.name === "CURRENT_DATE")
  ) {
    return { kind: "now" };
  }
  throw new TypeError("DEFAULT takes a constant or CURRENT_TIMESTAMP");
}

/**
 * One WHEN clause of a MERGE (F312). Branches are tried in order for each source row, and the
 * first whose match state and optional condition hold decides what happens to that row.
 */
export type MergeBranch =
  | {
      when: "matched";
      condition?: Expression;
      action:
        | { kind: "update"; assignments: Array<{ column: string; expression: Expression }> }
        | { kind: "delete" };
    }
  | {
      when: "not-matched";
      condition?: Expression;
      action: { kind: "insert"; columns: string[]; values: Expression[] };
    };

/**
 * One FOREIGN KEY as written (E141-04). The parent column is optional in the text and defaults
 * to the parent's unique key, which is the only column this engine can reference.
 */
export interface ForeignKeyDefinition {
  name: string;
  column: string;
  parentTable: string;
  parentColumn?: string;
  onDelete: "restrict" | "cascade" | "set null";
}

/** An INSERT value: a constant, or an unbound `?`/`$n` placeholder awaiting its parameter. */
export type InsertValue = QueryValue | { parameter: number };

export type CompiledStatement =
  | { kind: "select"; sql: string; parameterCount?: number }
  | {
      kind: "insert";
      table: string;
      columns: string[];
      rows: InsertValue[][];
      /** INSERT ... SELECT source; `rows` is then empty and fills at execution. */
      query?: CompiledQuery;
      /**
       * ON CONFLICT (column) DO NOTHING skips rows whose key exists. DO UPDATE SET with every
       * inserted column drawn from EXCLUDED is "replace" (whole-row upsert); a subset is
       * "update", which merges only the listed columns into existing rows.
       */
      onConflict?: {
        column: string;
        action: "nothing" | "replace" | "update";
        /** The EXCLUDED columns a partial DO UPDATE merges; present only for "update". */
        columns?: string[];
      };
      returning?: string[] | "*";
      parameterCount?: number;
    }
  | {
      kind: "update";
      table: string;
      assignments: Array<{ column: string; expression: Expression }>;
      predicates: Array<{ left: Expression; operator: PredicateOperator; right: Expression }>;
      returning?: string[] | "*";
      parameterCount?: number;
    }
  | {
      kind: "delete";
      table: string;
      predicates: Array<{ left: Expression; operator: PredicateOperator; right: Expression }>;
      returning?: string[] | "*";
      parameterCount?: number;
    }
  | {
      kind: "create-table";
      table: string;
      columns: Array<{
        name: string;
        type: SqlColumnType;
        nullable?: boolean;
        defaultValue?: ColumnDefault;
      }>;
      checks?: Array<{ name: string; sql: string }>;
      foreignKeys?: ForeignKeyDefinition[];
      uniqueKey?: string;
      /** CREATE TABLE IF NOT EXISTS: an existing table of that name is left alone. */
      ifNotExists?: boolean;
      parameterCount?: number;
    }
  | {
      kind: "create-trigger";
      table: string;
      trigger: {
        name: string;
        event: "insert" | "update" | "delete";
        timing: "before" | "after";
        statements: Array<{
          /** Body INSERT with every NEW.col / OLD.col rewritten to a positional placeholder. */
          sql: string;
          bindings: Array<{ source: "new" | "old"; column: string }>;
        }>;
      };
      parameterCount?: number;
    }
  | { kind: "drop-trigger"; name: string; parameterCount?: number }
  | {
      kind: "create-table-as";
      table: string;
      query: CompiledQuery;
      ifNotExists?: boolean;
      parameterCount?: number;
    }
  | { kind: "drop-table"; table: string; ifExists?: boolean; parameterCount?: number }
  | {
      kind: "create-view";
      view: string;
      sql: string;
      orReplace?: boolean;
      parameterCount?: number;
    }
  | { kind: "drop-view"; view: string; ifExists?: boolean; parameterCount?: number }
  | { kind: "transaction"; action: "begin" | "commit" | "rollback"; parameterCount?: number }
  | {
      kind: "merge";
      /** The target table, and the correlation name its columns are read under. */
      table: string;
      alias: string;
      /**
       * The source: a table or view by name, or a parenthesized query kept as its own text —
       * the statement re-reads it through the ordinary query pipeline, so a source query sees
       * views, CTEs, and joins exactly as it would on its own.
       */
      source: { alias: string; table?: string; sql?: string };
      /**
       * The match condition. The engine requires it to equate the target's unique key with a
       * source expression, which is also what makes each source row match at most one target
       * row — the standard's cardinality violation cannot arise.
       */
      on: Expression;
      branches: MergeBranch[];
      parameterCount?: number;
    }
  | {
      kind: "add-column";
      table: string;
      column: {
        name: string;
        type: SqlColumnType;
        nullable?: boolean;
        defaultValue?: ColumnDefault;
      };
      parameterCount?: number;
    };

/**
 * Parses CREATE TRIGGER name AFTER INSERT|UPDATE|DELETE ON table [FOR EACH ROW]
 * BEGIN insert; ... END. Body statements are INSERT ... VALUES with NEW.col / OLD.col
 * references; each reference rewrites to a positional placeholder recorded in order, so
 * firing is one bindStatementParameters call per affected row against machinery that
 * already exists. Bodies cannot carry their own parameters — placeholder order is the
 * binding order.
 */
/**
 * BEGIN / START TRANSACTION, COMMIT, and ROLLBACK (E151). The optional noise the standard and
 * the dialects allow around them — WORK, TRANSACTION, and the read-write access mode — parses
 * and carries no meaning here: this engine has one isolation level, snapshot, and no read-only
 * mode a transaction could relax into.
 */
function parseTransactionStatement(keyword: string, tokens: Token[]): CompiledStatement {
  const words = tokens
    .filter((token) => token.kind === "identifier")
    .map((token) => token.text.toUpperCase());
  const action = keyword === "START" || keyword === "BEGIN" ? "begin" : keyword.toLowerCase();
  const allowed = new Set(["BEGIN", "START", "COMMIT", "ROLLBACK", "WORK", "TRANSACTION"]);
  for (const word of words) {
    if (!allowed.has(word)) {
      throw new TypeError(`${keyword} takes no ${word}: this engine has one isolation level`);
    }
  }
  if (keyword === "START" && words[1] !== "TRANSACTION") {
    throw new TypeError("Expected START TRANSACTION");
  }
  return { kind: "transaction", action: action as "begin" | "commit" | "rollback" };
}

/**
 * Compiles one CHECK constraint's stored text into the expression the writer evaluates per row.
 * Kept beside the parser because the same restrictions apply wherever a constraint is read: a
 * row condition over this table's columns, with no aggregate, window, subquery, or parameter.
 */
export function compileCheckExpression(sql: string, name: string): Expression {
  const parser = new Parser(tokenize(sql), sql);
  const expression = parser.parseExpression();
  if (hasAggregate(expression) || containsWindow(expression) || containsParameter(expression)) {
    throw new TypeError(`CHECK ${name} takes a row condition over this table's own columns`);
  }
  return expression;
}

/**
 * CREATE [OR REPLACE] VIEW name AS <query>. The body is kept as the author's own text rather
 * than a re-rendered plan: the catalog stores what was written, and every read compiles it
 * through the same route any other statement takes.
 */
function parseCreateView(sql: string, tokens: Token[], orReplace: boolean): CompiledStatement {
  const cursor = orReplace ? 3 : 1;
  const keywordAt = (index: number, word: string): boolean => {
    const token = tokens[index];
    return (
      token?.kind === "identifier" && token.quoted !== true && token.text.toUpperCase() === word
    );
  };
  if (orReplace && !keywordAt(1, "OR")) throw new TypeError("Expected OR REPLACE");
  if (orReplace && !keywordAt(2, "REPLACE")) throw new TypeError("Expected OR REPLACE");
  const name = tokens[cursor + 1];
  if (name?.kind !== "identifier") throw new TypeError("Expected a view name");
  if (!keywordAt(cursor + 2, "AS")) {
    throw new TypeError("CREATE VIEW takes a name and AS <query>; column lists are not supported");
  }
  const body = tokens[cursor + 3];
  if (body === undefined || body.kind === "eof")
    throw new TypeError("CREATE VIEW requires a query");
  const view = name.text;
  const text = sql.slice(body.start).trim();
  // Compiling now means a broken view fails where it is written, not on the first read.
  compileQuery(text);
  return { kind: "create-view", view, sql: text, ...(orReplace ? { orReplace: true } : {}) };
}

function parseCreateTrigger(text: string, tokens: Token[]): CompiledStatement {
  let cursor = 2;
  const keywordAt = (index: number, keyword: string): boolean => {
    const token = tokens[index];
    return (
      token?.kind === "identifier" && token.quoted !== true && token.text.toUpperCase() === keyword
    );
  };
  const identifier = (what: string): string => {
    const token = tokens[cursor];
    if (token?.kind !== "identifier") throw new TypeError(`Expected ${what}`);
    cursor += 1;
    return token.text;
  };
  const name = identifier("a trigger name");
  let timing: "before" | "after";
  if (keywordAt(cursor, "AFTER")) timing = "after";
  else if (keywordAt(cursor, "BEFORE")) timing = "before";
  else throw new TypeError("CREATE TRIGGER supports BEFORE and AFTER triggers");
  cursor += 1;
  const eventText = identifier("a trigger event").toUpperCase();
  if (eventText !== "INSERT" && eventText !== "UPDATE" && eventText !== "DELETE") {
    throw new TypeError("Trigger event must be INSERT, UPDATE, or DELETE");
  }
  const event = eventText.toLowerCase() as "insert" | "update" | "delete";
  if (!keywordAt(cursor, "ON")) throw new TypeError("Expected ON before the trigger table");
  cursor += 1;
  const table = identifier("the trigger table");
  if (keywordAt(cursor, "FOR")) {
    if (!keywordAt(cursor + 1, "EACH") || !keywordAt(cursor + 2, "ROW")) {
      throw new TypeError("Expected FOR EACH ROW");
    }
    cursor += 3;
  }
  if (!keywordAt(cursor, "BEGIN")) throw new TypeError("Trigger bodies use BEGIN ... END");
  cursor += 1;
  let endIndex = -1;
  for (let index = tokens.length - 1; index > cursor; index -= 1) {
    if (keywordAt(index, "END")) {
      endIndex = index;
      break;
    }
  }
  if (endIndex <= cursor) throw new TypeError("Trigger bodies use BEGIN ... END");
  const spans: Array<[number, number]> = [];
  let statementStart = cursor;
  for (let index = cursor; index < endIndex; index += 1) {
    const token = tokens[index];
    if (token?.kind === "punctuation" && token.text === ";") {
      if (index > statementStart) spans.push([statementStart, index]);
      statementStart = index + 1;
    }
  }
  if (endIndex > statementStart) spans.push([statementStart, endIndex]);
  if (spans.length === 0) throw new TypeError("A trigger body needs at least one statement");
  const statements = spans.map(([from, to]) => {
    const bindings: Array<{ source: "new" | "old"; column: string }> = [];
    const rewrites: Array<{ start: number; end: number }> = [];
    for (let index = from; index < to; index += 1) {
      const token = tokens[index];
      if (token?.kind === "parameter") {
        throw new TypeError("Trigger bodies cannot contain parameters");
      }
      if (
        (keywordAt(index, "NEW") || keywordAt(index, "OLD")) &&
        tokens[index + 1]?.kind === "punctuation" &&
        tokens[index + 1]?.text === "." &&
        tokens[index + 2]?.kind === "identifier" &&
        index + 2 < to
      ) {
        const source = (tokens[index] ?? { text: "" }).text.toLowerCase() as "new" | "old";
        bindings.push({ source, column: (tokens[index + 2] ?? { text: "" }).text });
        rewrites.push({
          start: tokens[index]?.start ?? 0,
          end: tokens[index + 2]?.end ?? 0,
        });
        index += 2;
      }
    }
    const sliceStart = tokens[from]?.start ?? 0;
    const sliceEnd = tokens[to - 1]?.end ?? sliceStart;
    let sql = text.slice(sliceStart, sliceEnd);
    for (let index = rewrites.length - 1; index >= 0; index -= 1) {
      const rewrite = rewrites[index];
      if (rewrite === undefined) continue;
      sql = `${sql.slice(0, rewrite.start - sliceStart)}?${sql.slice(rewrite.end - sliceStart)}`;
    }
    const compiled = compileStatement(sql);
    if (compiled.kind !== "insert" && compiled.kind !== "update" && compiled.kind !== "delete") {
      throw new TypeError("Trigger bodies support INSERT, UPDATE, and DELETE statements");
    }
    if (compiled.kind === "insert" && compiled.query !== undefined) {
      throw new TypeError("Trigger body INSERTs use VALUES");
    }
    if (compiled.kind === "insert" && compiled.onConflict !== undefined) {
      throw new TypeError("Trigger body INSERTs cannot carry ON CONFLICT");
    }
    if (compiled.kind !== "insert" && compiled.returning !== undefined) {
      throw new TypeError("Trigger bodies cannot carry RETURNING");
    }
    if ((compiled.parameterCount ?? 0) !== bindings.length) {
      throw new TypeError("Trigger body placeholders must all come from NEW/OLD references");
    }
    return { sql, bindings };
  });
  if (tokens[endIndex + 1]?.kind !== "eof") {
    throw new TypeError("Unexpected input after the trigger body END");
  }
  return { kind: "create-trigger", table, trigger: { name, event, timing, statements } };
}

/**
 * Routes one SQL statement: SELECT and WITH compile through the read-only query pipeline, while
 * INSERT ... VALUES, UPDATE ... SET, and DELETE FROM parse into mutation statements. Any other
 * leading keyword fails explicitly.
 */
export function compileStatement(sql: string): CompiledStatement {
  const { text, offset } = normalizeSql(sql);
  if (text.length === 0) throw new SqlCompileError("Enter a SQL statement", offset, 0);
  let parser: Parser | undefined;
  try {
    const tokens = tokenize(text);
    const first = tokens[0];
    const keyword = first?.kind === "identifier" ? first.text.toUpperCase() : "";
    const second = tokens[1];
    const isTriggerDdl =
      (keyword === "CREATE" || keyword === "DROP") &&
      second?.kind === "identifier" &&
      second.text.toUpperCase() === "TRIGGER";
    if (!isTriggerDdl) rejectSemicolons(tokens);
    if (
      keyword === "BEGIN" ||
      keyword === "START" ||
      keyword === "COMMIT" ||
      keyword === "ROLLBACK"
    ) {
      return parseTransactionStatement(keyword, tokens);
    }
    if (keyword === "MERGE") {
      parser = new Parser(tokens, text);
      const statement = parser.parseMerge();
      if (parser.parameterCount > 0) statement.parameterCount = parser.parameterCount;
      return statement;
    }
    if (keyword === "INSERT" || keyword === "UPDATE" || keyword === "DELETE") {
      parser = new Parser(tokens);
      const statement = parser.parseMutation(keyword);
      if (parser.parameterCount > 0) statement.parameterCount = parser.parameterCount;
      return statement;
    }
    if (keyword === "CREATE") {
      if (isTriggerDdl) return parseCreateTrigger(text, tokens);
      const viewAt = second?.text.toUpperCase() === "OR" ? 3 : 1;
      if (tokens[viewAt]?.kind === "identifier" && tokens[viewAt].text.toUpperCase() === "VIEW") {
        return parseCreateView(text, tokens, viewAt === 3);
      }
      parser = new Parser(tokens, text);
      const statement = parser.parseCreateTable();
      if (parser.parameterCount > 0) statement.parameterCount = parser.parameterCount;
      return statement;
    }
    if (keyword === "ALTER") {
      parser = new Parser(tokens);
      return parser.parseAlterTable();
    }
    if (keyword === "DROP") {
      if (second?.kind === "identifier" && second.text.toUpperCase() === "VIEW") {
        parser = new Parser(tokens);
        return parser.parseDropView();
      }
      const name = tokens[2];
      if (isTriggerDdl && name?.kind === "identifier" && tokens[3]?.kind === "eof") {
        return { kind: "drop-trigger", name: name.text };
      }
      if (second?.kind === "identifier" && second.text.toUpperCase() === "TABLE") {
        parser = new Parser(tokens);
        return parser.parseDropTable();
      }
      throw new TypeError("DROP supports: DROP TABLE name, DROP TRIGGER name");
    }
    if (keyword === "WITH") {
      parser = new Parser(tokens);
      const statement = parser.parseStatementWithCtes();
      if (statement !== undefined) {
        if (parser.parameterCount > 0) statement.parameterCount = parser.parameterCount;
        return statement;
      }
      // Plain WITH ... SELECT: fall through to the ordinary query route below.
      parser = undefined;
    }
    const plan = compileQuery(text);
    return plan.parameterCount === undefined
      ? { kind: "select", sql: text }
      : { kind: "select", sql: text, parameterCount: plan.parameterCount };
  } catch (error) {
    throwLocated(error, offset, parser?.span ?? { start: 0, end: text.length });
  }
}

/**
 * Evaluates an expression against several named rows — the shape MERGE works in, where one
 * condition or assignment reads the target and the source at once.
 */
export function evaluateJoinedRowExpression(
  expression: Expression,
  rows: Readonly<Record<string, DatabaseRow | undefined>>,
): QueryValue {
  return asQueryValue(evaluate(expression, { ...rows }));
}

/** Evaluates an expression against one row, for UPDATE SET assignment computation. */
export function evaluateRowExpression(
  expression: Expression,
  alias: string,
  row: DatabaseRow,
): QueryValue {
  return asQueryValue(evaluate(expression, { [alias]: row }));
}

export interface SubqueryResolutionStep {
  /** The uncorrelated block to execute; earlier steps have already substituted inside it. */
  readonly block: CompiledQuery;
  /** Replaces the subquery node with the executed result as literals. */
  substitute(result: QueryResult): void;
}

/**
 * Clones a plan and returns its subquery sites in post-order: executing each step's block and
 * substituting its result leaves the returned plan free of subquery nodes. A scalar subquery must
 * select one column and return at most one row (empty is NULL); an IN subquery must select one
 * column and becomes a literal membership list. Correlated references fail inside the subquery's
 * own scope as unknown aliases.
 */
export function subqueryResolutionSteps(plan: CompiledQuery): {
  plan: CompiledQuery;
  steps: SubqueryResolutionStep[];
} {
  if (!blockHasSubqueries(plan)) return { plan, steps: [] };
  const clone = structuredClone(plan);
  const steps: SubqueryResolutionStep[] = [];
  const rewrite = (expression: Expression, replace: (next: Expression) => void): void => {
    if (expression.kind === "subquery") {
      collectBlock(expression.block);
      steps.push({
        block: expression.block,
        substitute(result) {
          if (result.columns.length !== 1) {
            throw new TypeError("A scalar subquery must select exactly one column");
          }
          if (result.rows.length > 1) {
            throw new TypeError(`A scalar subquery returned ${String(result.rows.length)} rows`);
          }
          replace({
            kind: "literal",
            value: result.rows[0]?.[result.columns[0] ?? ""] ?? null,
          });
        },
      });
      return;
    }
    if (expression.kind === "binary") {
      rewrite(expression.left, (next) => (expression.left = next));
      rewrite(expression.right, (next) => (expression.right = next));
      return;
    }
    if (expression.kind === "call") {
      expression.arguments.forEach((argument, index) => {
        rewrite(argument, (next) => (expression.arguments[index] = next));
      });
      return;
    }
    if (expression.kind === "list") {
      expression.items.forEach((item, index) => {
        rewrite(item, (next) => (expression.items[index] = next));
      });
      return;
    }
    if (expression.kind === "logical") {
      rewrite(expression.left, (next) => (expression.left = next));
      rewrite(expression.right, (next) => (expression.right = next));
      return;
    }
    if (expression.kind === "not") {
      rewrite(expression.operand, (next) => (expression.operand = next));
      return;
    }
    if (expression.kind === "case") {
      for (const branch of expression.branches) {
        rewrite(branch.when, (next) => (branch.when = next));
        rewrite(branch.then, (next) => (branch.then = next));
      }
      const otherwise = expression.otherwise;
      if (otherwise !== undefined) rewrite(otherwise, (next) => (expression.otherwise = next));
      return;
    }
    if (expression.kind === "exists") {
      collectBlock(expression.block);
      const negated = expression.negated;
      steps.push({
        block: expression.block,
        substitute(result) {
          replace({
            kind: "literal",
            value: negated ? result.rows.length === 0 : result.rows.length > 0,
          });
        },
      });
      return;
    }
    if (expression.kind === "condition") {
      rewrite(expression.left, (next) => (expression.left = next));
      const right = expression.right;
      if (
        (expression.operator === "IN" ||
          expression.operator === "NOT IN" ||
          parseQuantified(expression.operator) !== undefined) &&
        right.kind === "subquery"
      ) {
        collectBlock(right.block);
        steps.push({
          block: right.block,
          substitute(result) {
            if (result.columns.length !== 1) {
              throw new TypeError("An IN subquery must select exactly one column");
            }
            expression.right = {
              kind: "list",
              items: result.rows.map((row) => ({
                kind: "literal",
                value: row[result.columns[0] ?? ""] ?? null,
              })),
            };
          },
        });
        return;
      }
      rewrite(expression.right, (next) => (expression.right = next));
    }
  };
  const collectBlock = (block: CompiledQuery): void => {
    for (const source of [block.base, ...block.joins]) {
      if (source.derived !== undefined) collectBlock(source.derived);
      source.union?.blocks.forEach(collectBlock);
      if (source.windowed !== undefined) collectBlock(source.windowed.block);
      if (source.recursive !== undefined) {
        collectBlock(source.recursive.base);
        collectBlock(source.recursive.step);
      }
    }
    for (const item of block.select) {
      rewrite(item.expression, (next) => (item.expression = next));
    }
    block.groupBy.forEach((expression, index) => {
      rewrite(expression, (next) => (block.groupBy[index] = next));
    });
    for (const predicate of [...block.predicates, ...block.having]) {
      rewrite(predicate.left, (next) => (predicate.left = next));
      const right = predicate.right;
      if (
        (predicate.operator === "IN" ||
          predicate.operator === "NOT IN" ||
          parseQuantified(predicate.operator) !== undefined) &&
        right.kind === "subquery"
      ) {
        collectBlock(right.block);
        steps.push({
          block: right.block,
          substitute(result) {
            if (result.columns.length !== 1) {
              throw new TypeError("An IN subquery must select exactly one column");
            }
            predicate.right = {
              kind: "list",
              items: result.rows.map((row) => ({
                kind: "literal",
                value: row[result.columns[0] ?? ""] ?? null,
              })),
            };
          },
        });
        continue;
      }
      rewrite(predicate.right, (next) => (predicate.right = next));
    }
    for (const order of block.orderBy) {
      rewrite(order.expression, (next) => (order.expression = next));
    }
  };
  collectBlock(clone);
  return { plan: clone, steps };
}

export function blockHasSubqueries(plan: CompiledQuery): boolean {
  const expressionHas = (expression: Expression): boolean => {
    if (expression.kind === "subquery" || expression.kind === "exists") return true;
    return childExpressions(expression).some(expressionHas);
  };
  const blockHas = (block: CompiledQuery): boolean =>
    block.select.some((item) => expressionHas(item.expression)) ||
    block.groupBy.some(expressionHas) ||
    [...block.predicates, ...block.having].some(
      (predicate) => expressionHas(predicate.left) || expressionHas(predicate.right),
    ) ||
    block.orderBy.some((order) => expressionHas(order.expression)) ||
    [block.base, ...block.joins].some(
      (source) =>
        (source.derived !== undefined && blockHas(source.derived)) ||
        source.union?.blocks.some(blockHas) === true ||
        (source.windowed !== undefined && blockHas(source.windowed.block)) ||
        (source.recursive !== undefined &&
          (blockHas(source.recursive.base) || blockHas(source.recursive.step))),
    );
  return blockHas(plan);
}

/** True when any `?`/`$n` placeholder remains anywhere in the expression tree. */
export function containsParameter(expression: Expression): boolean {
  if (expression.kind === "parameter") return true;
  if (expression.kind === "subquery" || expression.kind === "exists") {
    return blockHasParameters(expression.block);
  }
  return childExpressions(expression).some(containsParameter);
}

export function blockHasParameters(block: CompiledQuery): boolean {
  if (block.limitParameter !== undefined || block.offsetParameter !== undefined) return true;
  const expressions: Expression[] = [];
  forEachBlockExpression(block, (expression) => expressions.push(expression));
  return (
    expressions.some(containsParameter) ||
    [block.base, ...block.joins].some(
      (source) =>
        (source.derived !== undefined && blockHasParameters(source.derived)) ||
        source.union?.blocks.some(blockHasParameters) === true ||
        (source.windowed !== undefined && blockHasParameters(source.windowed.block)) ||
        (source.recursive !== undefined &&
          (blockHasParameters(source.recursive.base) || blockHasParameters(source.recursive.step))),
    )
  );
}

function validateParameters(count: number, params: readonly QueryValue[] | undefined): void {
  const given = params?.length ?? 0;
  if (given !== count) {
    throw new TypeError(
      `This statement takes ${String(count)} parameter${count === 1 ? "" : "s"}, got ${String(given)}`,
    );
  }
  params?.forEach((value, index) => {
    const label = `$${String(index + 1)}`;
    if (value === null || typeof value === "boolean" || typeof value === "string") return;
    if (typeof value === "number") {
      if (!Number.isFinite(value)) {
        throw new TypeError(`Parameter ${label} must be a finite number`);
      }
      return;
    }
    if (value instanceof Date) {
      if (!Number.isFinite(value.getTime())) {
        throw new TypeError(`Parameter ${label} must be a valid date`);
      }
      return;
    }
    throw new TypeError(`Parameter ${label} must be null, boolean, number, string, or Date`);
  });
}

function bindExpression(expression: Expression, values: readonly QueryValue[]): Expression {
  if (expression.kind === "parameter") {
    return { kind: "literal", value: values[expression.index] ?? null };
  }
  if (expression.kind === "subquery" || expression.kind === "exists") {
    bindBlock(expression.block, values);
    return expression;
  }
  if (
    expression.kind === "binary" ||
    expression.kind === "condition" ||
    expression.kind === "logical"
  ) {
    expression.left = bindExpression(expression.left, values);
    expression.right = bindExpression(expression.right, values);
    return expression;
  }
  if (expression.kind === "call") {
    expression.arguments = expression.arguments.map((argument) => bindExpression(argument, values));
    return expression;
  }
  if (expression.kind === "list") {
    expression.items = expression.items.map((item) => bindExpression(item, values));
    return expression;
  }
  if (expression.kind === "not") {
    expression.operand = bindExpression(expression.operand, values);
    return expression;
  }
  if (expression.kind === "case") {
    for (const branch of expression.branches) {
      branch.when = bindExpression(branch.when, values);
      branch.then = bindExpression(branch.then, values);
    }
    if (expression.otherwise !== undefined) {
      expression.otherwise = bindExpression(expression.otherwise, values);
    }
    return expression;
  }
  if (expression.kind === "window") {
    expression.partitionBy = expression.partitionBy.map((part) => bindExpression(part, values));
    for (const order of expression.orderBy) {
      order.expression = bindExpression(order.expression, values);
    }
    if (expression.argument !== undefined) {
      expression.argument = bindExpression(expression.argument, values);
    }
    return expression;
  }
  return expression;
}

function bindBlock(block: CompiledQuery, values: readonly QueryValue[]): void {
  if (block.limitParameter !== undefined) {
    block.limit = validateLimit(numeric(values[block.limitParameter] ?? null));
    delete block.limitParameter;
  }
  if (block.offsetParameter !== undefined) {
    block.offset = validateOffset(numeric(values[block.offsetParameter] ?? null));
    delete block.offsetParameter;
  }
  for (const item of block.select) item.expression = bindExpression(item.expression, values);
  for (const predicate of [...block.predicates, ...block.having]) {
    predicate.left = bindExpression(predicate.left, values);
    predicate.right = bindExpression(predicate.right, values);
  }
  block.groupBy = block.groupBy.map((expression) => bindExpression(expression, values));
  for (const order of block.orderBy) {
    order.expression = bindExpression(order.expression, values);
  }
  for (const join of block.joins) {
    join.left = bindExpression(join.left, values);
    join.right = bindExpression(join.right, values);
    if (join.on !== undefined) join.on = bindExpression(join.on, values);
  }
  for (const source of [block.base, ...block.joins]) {
    if (source.derived !== undefined) bindBlock(source.derived, values);
    for (const member of source.union?.blocks ?? []) bindBlock(member, values);
    if (source.windowed !== undefined) bindBlock(source.windowed.block, values);
    if (source.recursive !== undefined) {
      bindBlock(source.recursive.base, values);
      bindBlock(source.recursive.step, values);
    }
  }
}

/**
 * Replaces every placeholder with its parameter value as a literal. The input plan is never
 * modified — plans come from the compile cache, so binding is copy-on-write. The parameter list
 * must match the statement's placeholder count exactly; a plan without placeholders passes
 * through untouched (and rejects a non-empty parameter list explicitly).
 */
export function bindPlanParameters(
  plan: CompiledQuery,
  params: readonly QueryValue[] | undefined,
): CompiledQuery {
  const count = plan.parameterCount ?? 0;
  validateParameters(count, params);
  if (count === 0) return plan;
  const clone = structuredClone(plan);
  bindBlock(clone, params ?? []);
  delete clone.parameterCount;
  return clone;
}

function isParameterSlot(value: InsertValue): value is { parameter: number } {
  return typeof value === "object" && value !== null && !(value instanceof Date);
}

/** The mutation-statement counterpart of bindPlanParameters; SELECTs bind through their plan. */
export function bindStatementParameters(
  statement: CompiledStatement,
  params: readonly QueryValue[] | undefined,
): CompiledStatement {
  const count = statement.parameterCount ?? 0;
  validateParameters(count, params);
  if (count === 0) return statement;
  if (statement.kind === "select") {
    throw new TypeError("SELECT parameters bind through the query pipeline, not the statement");
  }
  if (
    statement.kind === "create-table" ||
    statement.kind === "create-trigger" ||
    statement.kind === "drop-trigger"
  ) {
    throw new TypeError("DDL statements take no parameters");
  }
  const values = params ?? [];
  const clone = structuredClone(statement);
  delete clone.parameterCount;
  if (clone.kind === "insert") {
    if (clone.query !== undefined) bindBlock(clone.query, values);
    clone.rows = clone.rows.map((row) =>
      row.map((value) => (isParameterSlot(value) ? (values[value.parameter] ?? null) : value)),
    );
    return clone;
  }
  if (
    clone.kind === "drop-table" ||
    clone.kind === "create-view" ||
    clone.kind === "drop-view" ||
    clone.kind === "transaction"
  ) {
    return clone;
  }
  if (clone.kind === "merge") {
    clone.on = bindExpression(clone.on, values);
    for (const branch of clone.branches) {
      if (branch.condition !== undefined)
        branch.condition = bindExpression(branch.condition, values);
      if (branch.action.kind === "update") {
        for (const assignment of branch.action.assignments) {
          assignment.expression = bindExpression(assignment.expression, values);
        }
      } else if (branch.action.kind === "insert") {
        branch.action.values = branch.action.values.map((value) => bindExpression(value, values));
      }
    }
    return clone;
  }
  if (clone.kind === "create-table-as") {
    bindBlock(clone.query, values);
    return clone;
  }
  if (clone.kind !== "update" && clone.kind !== "delete") return clone;
  if (clone.kind === "update") {
    for (const assignment of clone.assignments) {
      assignment.expression = bindExpression(assignment.expression, values);
    }
  }
  for (const predicate of clone.predicates) {
    predicate.left = bindExpression(predicate.left, values);
    predicate.right = bindExpression(predicate.right, values);
  }
  return clone;
}

export type SqlColumnType = "boolean" | "number" | "string" | "datetime";

export interface SqlColumnSchema {
  name: string;
  type: SqlColumnType;
}

/**
 * Infers the typed output schema of one select block from typed source schemas. A column whose
 * type cannot be established (for example a bare NULL literal) is rejected explicitly, so a
 * derived table always has concrete column types even when its result is empty.
 */
export function inferBlockSchema(
  plan: CompiledQuery,
  schemas: ReadonlyMap<string, readonly SqlColumnSchema[]>,
): SqlColumnSchema[] {
  const sources = [plan.base, ...plan.joins];
  const multipleSources = sources.length > 1;
  const wildcardSchema = (source: TableSource): SqlColumnSchema[] =>
    (schemas.get(source.table) ?? []).map((column) => ({
      name: multipleSources ? `${source.alias}.${column.name}` : column.name,
      type: column.type,
    }));
  if (
    plan.select[0]?.expression.kind === "wildcard" &&
    plan.select[0].expression.table === undefined
  )
    return sources.flatMap(wildcardSchema);
  const resolveColumnType = (reference: string): SqlColumnType | "null" => {
    const parts = reference.split(".");
    if (parts.length === 2) {
      const source = sources.find(({ alias }) => alias === parts[0]);
      if (source === undefined) throw new TypeError(`Unknown table alias: ${parts[0] ?? ""}`);
      const column = (schemas.get(source.table) ?? []).find(({ name }) => name === parts[1]);
      if (column === undefined) throw new TypeError(`Unknown column: ${reference}`);
      return column.type;
    }
    const matches = sources.flatMap((source) =>
      (schemas.get(source.table) ?? [])
        .filter(({ name }) => name === parts[0])
        .map((column) => column.type),
    );
    if (matches.length !== 1) throw new TypeError(`Ambiguous or missing column: ${reference}`);
    return matches[0] ?? "string";
  };
  const infer = (expression: Expression): SqlColumnType | "null" => {
    if (expression.kind === "subquery" || expression.kind === "list") {
      throw new TypeError("Subqueries must be resolved before schema inference");
    }
    if (expression.kind === "parameter") {
      throw new TypeError("Parameters must be bound before schema inference");
    }
    if (expression.kind === "window") {
      throw new TypeError("Window functions must be desugared before schema inference");
    }
    if (
      expression.kind === "condition" ||
      expression.kind === "logical" ||
      expression.kind === "not" ||
      expression.kind === "exists"
    ) {
      return "boolean";
    }
    if (expression.kind === "case") {
      const outcomes = [
        ...expression.branches.map((branch) => branch.then),
        ...(expression.otherwise === undefined ? [] : [expression.otherwise]),
      ];
      let resolved: SqlColumnType | "null" = "null";
      for (const outcome of outcomes) {
        const type = infer(outcome);
        if (type === "null") continue;
        if (resolved !== "null" && resolved !== type) {
          throw new TypeError("CASE branches must produce one value type");
        }
        resolved = type;
      }
      return resolved;
    }
    if (expression.kind === "literal") {
      const value = expression.value;
      if (value === null) return "null";
      if (typeof value === "boolean") return "boolean";
      if (typeof value === "number") return "number";
      if (typeof value === "string") return "string";
      return "datetime";
    }
    if (expression.kind === "wildcard") return "number";
    if (expression.kind === "column") return resolveColumnType(expression.reference);
    if (expression.kind === "fts") {
      if (expression.columns !== "*") {
        for (const columnExpression of expression.columns) {
          if (columnExpression.kind !== "column") {
            throw new TypeError("Full-text search takes column references");
          }
          if (infer(columnExpression) === "boolean") {
            throw new TypeError(
              `Full-text search cannot search a boolean column: ${columnExpression.reference}`,
            );
          }
        }
      }
      return expression.op === "match" ? "boolean" : "number";
    }
    if (expression.kind === "binary") {
      if (expression.operator === "||") {
        for (const side of [expression.left, expression.right]) {
          const type = infer(side);
          if (type !== "string" && type !== "null") {
            throw new TypeError("|| requires string operands");
          }
        }
        return "string";
      }
      for (const side of [expression.left, expression.right]) {
        const type = infer(side);
        if (type === "string" || type === "boolean") {
          throw new TypeError(`Arithmetic requires numeric operands: ${plan.sql}`);
        }
      }
      return "number";
    }
    if (expression.name === "COUNT" || expression.name === "SUM" || expression.name === "AVG") {
      return "number";
    }
    if (
      expression.name === "ROUND" ||
      expression.name === "LENGTH" ||
      expression.name === "ABS" ||
      expression.name === "FLOOR" ||
      expression.name === "CEIL" ||
      expression.name === "MOD" ||
      expression.name === "POWER" ||
      expression.name === "SQRT" ||
      expression.name === "INSTR" ||
      expression.name === "EXTRACT" ||
      expression.name === "OCTET_LENGTH" ||
      expression.name === "GROUPING"
    ) {
      return "number";
    }
    if (
      expression.name === "JSON_VALUE" ||
      expression.name === "JSON_QUERY" ||
      expression.name === "JSON_OBJECT" ||
      expression.name === "JSON_ARRAY"
    ) {
      return "string";
    }
    if (expression.name === "JSON_EXISTS" || expression.name === "IS_JSON") return "boolean";
    if (expression.name === "DATE_TRUNC") return "datetime";
    if (expression.name === "CURRENT_DATE" || expression.name === "CURRENT_TIMESTAMP") {
      return "datetime";
    }
    // The engine has no TIME type, so LOCALTIME reads as an 'HH:MM:SS' string, like SQLite's
    // CURRENT_TIME.
    if (expression.name === "LOCALTIME") return "string";
    if (
      expression.name === "NULLIF" ||
      expression.name === "GREATEST" ||
      expression.name === "LEAST"
    ) {
      // The output carries the arguments' common type, like COALESCE.
      let resolved: SqlColumnType | "null" = "null";
      for (const argument of expression.arguments) {
        const type = infer(argument);
        if (type === "null") continue;
        if (resolved !== "null" && resolved !== type) {
          throw new TypeError(`${expression.name} arguments must produce one value type`);
        }
        resolved = type;
      }
      return resolved;
    }
    if (expression.name === "CAST") {
      const target = expression.arguments[1];
      const word =
        target?.kind === "literal" && typeof target.value === "string" ? target.value : "";
      if (word === "number-integer") return "number";
      if (word === "number" || word === "string" || word === "boolean" || word === "datetime") {
        return word;
      }
      throw new TypeError("CAST target must be a type name");
    }
    if (
      expression.name === "UPPER" ||
      expression.name === "LOWER" ||
      expression.name === "TRIM" ||
      expression.name === "LTRIM" ||
      expression.name === "RTRIM" ||
      expression.name === "REPLACE" ||
      expression.name === "SUBSTR" ||
      expression.name === "LPAD" ||
      expression.name === "RPAD" ||
      expression.name === "OVERLAY"
    ) {
      const argument = expression.arguments[0];
      if (argument !== undefined) {
        const type = infer(argument);
        if (type !== "string" && type !== "null") {
          throw new TypeError(`${expression.name} requires a string argument`);
        }
      }
      return "string";
    }
    if (expression.name === "COALESCE") {
      let resolved: SqlColumnType | "null" = "null";
      for (const argument of expression.arguments) {
        const type = infer(argument);
        if (type === "null") continue;
        if (resolved !== "null" && resolved !== type) {
          throw new TypeError("COALESCE arguments must produce one value type");
        }
        resolved = type;
      }
      return resolved;
    }
    const argument = expression.arguments[0];
    return argument === undefined ? "null" : infer(argument);
  };
  return plan.select.flatMap((item) => {
    if (item.expression.kind === "wildcard" && item.expression.table !== undefined) {
      const table = item.expression.table;
      const source = sources.find((candidate) => candidate.alias === table);
      if (source === undefined) throw new TypeError(`Unknown table for ${table}.*: ${table}`);
      return wildcardSchema(source);
    }
    const type = infer(item.expression);
    if (type === "null") {
      throw new TypeError(`Cannot infer a column type for output ${item.alias}`);
    }
    return [{ name: item.alias, type }];
  });
}

export function referencedColumns(
  plan: CompiledQuery,
  schemas: ReadonlyMap<string, readonly string[]>,
): Map<string, string[]> {
  const sources = [plan.base, ...plan.joins];
  const sourceAliases = sources.map((source) => source.alias);
  if (new Set(sourceAliases).size !== sourceAliases.length)
    throw new TypeError("Table aliases must be unique");
  validateGrouping(plan);
  const aliases = new Map(sources.map((source) => [source.alias, source.table]));
  const requested = new Map<string, Set<string>>(
    sources.map((source) => [source.table, new Set<string>()]),
  );
  const outputAliases = new Set(plan.select.map((item) => item.alias));
  const expressions = [
    ...plan.select.map((item) => item.expression),
    ...plan.joins.flatMap((join) => [
      join.left,
      join.right,
      ...(join.on === undefined ? [] : [join.on]),
    ]),
    ...plan.predicates.flatMap((predicate) => [predicate.left, predicate.right]),
    ...plan.groupBy,
    ...plan.having.flatMap((predicate) => [predicate.left, predicate.right]),
  ];
  const collect = (expression: Expression, allowOutputAlias: boolean) => {
    for (const reference of expressionColumns(expression)) {
      if (allowOutputAlias && !reference.includes(".") && outputAliases.has(reference)) continue;
      const parts = reference.split(".");
      if (parts.length === 2) {
        const table = aliases.get(parts[0] ?? "");
        if (table === undefined) throw new TypeError(`Unknown table alias: ${parts[0] ?? ""}`);
        const column = parts[1] ?? "";
        if (!(schemas.get(table) ?? []).includes(column)) {
          throw new TypeError(`Unknown column: ${reference}`);
        }
        requested.get(table)?.add(column);
      } else {
        const column = parts[0] ?? "";
        const matches = sources.filter((source) =>
          (schemas.get(source.table) ?? []).includes(column),
        );
        if (matches.length !== 1) throw new TypeError(`Ambiguous or missing column: ${reference}`);
        requested.get(matches[0]?.table ?? "")?.add(column);
      }
    }
    if (expression.kind === "wildcard") {
      // A qualified wildcard reads only its own source's columns.
      const named = expression.table;
      for (const source of sources) {
        if (named !== undefined && source.alias !== named) continue;
        for (const column of schemas.get(source.table) ?? [])
          requested.get(source.table)?.add(column);
      }
    }
  };
  for (const expression of expressions) collect(expression, false);
  for (const { expression } of plan.orderBy) collect(expression, true);
  return new Map([...requested].map(([table, columns]) => [table, [...columns]]));
}

/**
 * Derives a schema-less table's searchable document columns for MATCH(*) expansion: every
 * column that ever held a non-boolean value. Boolean-only and all-null columns contribute
 * nothing to a document either way, so excluding them preserves match semantics exactly.
 */
/** Every column a row table's rows ever mention, in first-seen order, for wildcard expansion. */
function wildcardRowColumns(
  tables: ReadonlyMap<string, DatabaseRow[]>,
): (tableName: string) => readonly string[] | undefined {
  return (tableName) => {
    const rows = tables.get(tableName);
    if (rows === undefined) return undefined;
    const names: string[] = [];
    const seen = new Set<string>();
    for (const row of rows) {
      for (const name of Object.keys(row)) {
        if (seen.has(name)) continue;
        seen.add(name);
        names.push(name);
      }
    }
    return names;
  };
}

function rowTableSearchableColumns(
  tables: ReadonlyMap<string, DatabaseRow[]>,
): (tableName: string) => readonly string[] | undefined {
  return (tableName) => {
    const rows = tables.get(tableName);
    if (rows === undefined) return undefined;
    const searchable = new Set<string>();
    const booleans = new Set<string>();
    for (const row of rows) {
      for (const [name, value] of Object.entries(row)) {
        if (typeof value === "boolean") booleans.add(name);
        else if (value !== null) searchable.add(name);
      }
    }
    return [...searchable].filter((name) => !booleans.has(name));
  };
}

/** LIMIT/OFFSET placeholders never reach an executor: unresolved ones would silently no-op. */
function assertTailParametersBound(plan: CompiledQuery): void {
  const check = (block: CompiledQuery): void => {
    if (block.limitParameter !== undefined || block.offsetParameter !== undefined) {
      throw new TypeError("LIMIT/OFFSET placeholders are unbound; pass parameters when executing");
    }
    forEachNestedBlock(block, check);
  };
  check(plan);
}

/** Wraps a prepared query so every result passes through one more projection or trim. */
function trimPreparedResults(
  prepared: PreparedQuery,
  trim: (result: QueryResult) => QueryResult,
): PreparedQuery {
  return {
    sql: prepared.sql,
    tables: prepared.tables,
    get memoryUsage() {
      return prepared.memoryUsage;
    },
    execute: () => trim(prepared.execute()),
    executeAsync: async (options) => trim(await prepared.executeAsync(options)),
    close: () => {
      prepared.close();
    },
  };
}

export function createPreparedQuery(
  plan: CompiledQuery,
  tables: ReadonlyMap<string, DatabaseRow[]>,
  options: QueryExecutionOptions = {},
): PreparedQuery {
  assertTailParametersBound(plan);
  validateGrouping(plan);
  plan = resolveStatementDatetimes(plan);
  // The schema-dependent rewrites run before derived sources materialize, because both can
  // turn a scanned table into one more derived block.
  plan = expandSourceColumnAliases(plan, wildcardRowColumns(tables));
  plan = expandNaturalJoins(plan, wildcardRowColumns(tables));
  plan = expandQualifiedWildcards(plan, wildcardRowColumns(tables));
  const ties = withTiesPlan(plan);
  if (ties.plan !== plan)
    return trimPreparedResults(createPreparedQuery(ties.plan, tables, options), ties.trim);
  // Every engine entry expands MATCH(*) exactly once, here against the row tables' own
  // columns; past this point no executor sees the "*" sentinel.
  plan = expandFtsColumns(plan, rowTableSearchableColumns(tables));
  const resolution = subqueryResolutionSteps(plan);
  for (const step of resolution.steps) step.substitute(executeRowQuery(step.block, tables));
  plan = resolution.plan;
  tables = resolveDerivedRowTables(plan, tables);
  plan = expandDistinctWildcard(plan, wildcardRowColumns(tables));
  const memory = new QueryMemoryContext(options.executionMemoryBudgetBytes);
  if ([...tables.values()].some((rows) => rows.length === 0)) {
    if (options.executionMemoryBudgetBytes !== undefined) {
      memory.close();
      throw new TypeError(
        "Query memory budgets require typed columnar schemas when an input table is empty",
      );
    }
    return createPreparedRowQuery(plan, tables, memory);
  }
  try {
    return createPreparedColumnarQuery(plan, normalizeColumnarTables(plan, tables), memory);
  } catch (error) {
    memory.close();
    throw error;
  }
}

/** Internal columnar entry point used after MinnowDatabase materializes a stable snapshot. */
export function createPreparedColumnarQuery(
  plan: CompiledQuery,
  tables: ReadonlyMap<string, ColumnarTable>,
  memory: QueryMemoryContext = new QueryMemoryContext(),
  options: { ftsStats?: ReadonlyMap<string, FtsStats> } = {},
): PreparedQuery {
  plan = resolveStatementDatetimes(plan);
  const ties = withTiesPlan(plan);
  if (ties.plan !== plan) {
    return trimPreparedResults(
      createPreparedColumnarQuery(ties.plan, tables, memory, options),
      ties.trim,
    );
  }
  const columnarColumns = (tableName: string): readonly string[] | undefined => {
    const table = tables.get(tableName);
    return table === undefined ? undefined : [...table.columns.keys()];
  };
  plan = expandSourceColumnAliases(plan, columnarColumns);
  plan = expandNaturalJoins(plan, columnarColumns);
  plan = expandQualifiedWildcards(plan, columnarColumns);
  plan = expandDistinctWildcard(plan, columnarColumns);
  validateGrouping(plan);
  let closed = false;
  let prepared: PreparedVectorQuery | undefined;
  try {
    prepared = prepareVectorQuery(plan, tables, {
      memoryContext: memory,
      ...(options.ftsStats === undefined ? {} : { ftsStats: options.ftsStats }),
    });
  } catch (error) {
    memory.close();
    throw error;
  }
  return {
    sql: plan.sql,
    tables: [plan.base.table, ...plan.joins.map((join) => join.table)],
    get memoryUsage() {
      return memory.usage;
    },
    execute() {
      if (closed || prepared === undefined) throw new Error("Prepared query is closed");
      return prepared.execute();
    },
    async executeAsync(options) {
      if (closed || prepared === undefined) throw new Error("Prepared query is closed");
      return prepared.executeAsync(options);
    },
    close() {
      if (closed) return;
      closed = true;
      prepared?.close();
      prepared = undefined;
      memory.close();
    },
  };
}

function createPreparedRowQuery(
  plan: CompiledQuery,
  tables: ReadonlyMap<string, DatabaseRow[]>,
  memory: QueryMemoryContext,
): PreparedQuery {
  let closed = false;
  let rows: ReadonlyMap<string, DatabaseRow[]> | undefined = cloneRowTables(tables);
  return {
    sql: plan.sql,
    tables: [plan.base.table, ...plan.joins.map((join) => join.table)],
    get memoryUsage() {
      return memory.usage;
    },
    execute() {
      if (closed || rows === undefined) throw new Error("Prepared query is closed");
      return executeRowQuery(plan, cloneRowTables(rows));
    },
    async executeAsync() {
      if (closed || rows === undefined) throw new Error("Prepared query is closed");
      return executeRowQuery(plan, cloneRowTables(rows));
    },
    close() {
      closed = true;
      rows = undefined;
      memory.close();
    },
  };
}

function cloneRowTables(
  tables: ReadonlyMap<string, DatabaseRow[]>,
): ReadonlyMap<string, DatabaseRow[]> {
  return new Map(
    [...tables].map(([name, rows]) => [
      name,
      rows.map((row) =>
        Object.fromEntries(
          Object.entries(row).map(([column, value]) => [
            column,
            value instanceof Date ? new Date(value.getTime()) : value,
          ]),
        ),
      ),
    ]),
  );
}

export function executeQuery(
  plan: CompiledQuery,
  tables: ReadonlyMap<string, DatabaseRow[]>,
  options: QueryExecutionOptions = {},
): QueryResult {
  const prepared = createPreparedQuery(plan, tables, options);
  try {
    return prepared.execute();
  } finally {
    prepared.close();
  }
}

/**
 * Combines set-operation member results positionally under the first member's column names,
 * folding left: UNION deduplicates the entire accumulated set, UNION ALL concatenates. Values
 * compare with SQL grouping semantics (NULLs equal, dates by instant, signed zeros equal).
 */
export function combineUnionResults(
  results: readonly QueryResult[],
  ops: readonly SetOperator[],
): QueryResult {
  const first = results[0];
  if (first === undefined) throw new TypeError("A UNION requires at least one member");
  const columns = first.columns;
  const renamed = results.map((result, index) => {
    if (result.columns.length !== columns.length) {
      throw new TypeError("UNION members must select the same number of columns");
    }
    if (index === 0) return result.rows;
    return result.rows.map((row) =>
      Object.fromEntries(
        columns.map((name, column) => [name, row[result.columns[column] ?? ""] ?? null]),
      ),
    );
  });
  const encode = (row: QueryRow): string =>
    JSON.stringify(
      columns.map((name) => {
        return encodeSqlEqualityValue(row[name] ?? null);
      }),
    );
  const dedupe = (rows: readonly QueryRow[]): QueryRow[] => {
    const seen = new Set<string>();
    const deduplicated: QueryRow[] = [];
    for (const row of rows) {
      const key = encode(row);
      if (seen.has(key)) continue;
      seen.add(key);
      deduplicated.push(row);
    }
    return deduplicated;
  };
  let combined = renamed[0] ?? [];
  for (let index = 1; index < renamed.length; index += 1) {
    const next = renamed[index] ?? [];
    const op = ops[index - 1] ?? "union";
    if (op === "union all") {
      combined = [...combined, ...next];
      continue;
    }
    if (op === "union") {
      combined = dedupe([...combined, ...next]);
      continue;
    }
    if (op === "intersect all" || op === "except all") {
      // Bag semantics: each right-side occurrence consumes at most one left-side occurrence.
      const counts = new Map<string, number>();
      for (const row of next) {
        const key = encode(row);
        counts.set(key, (counts.get(key) ?? 0) + 1);
      }
      combined = combined.filter((row) => {
        const key = encode(row);
        const remaining = counts.get(key) ?? 0;
        if (remaining > 0) counts.set(key, remaining - 1);
        return op === "intersect all" ? remaining > 0 : remaining === 0;
      });
      continue;
    }
    // INTERSECT and EXCEPT return distinct left-side rows filtered by right-side membership.
    const membership = new Set(next.map(encode));
    combined = dedupe(combined).filter((row) =>
      op === "intersect" ? membership.has(encode(row)) : !membership.has(encode(row)),
    );
  }
  return { columns: [...columns], rows: combined };
}

function encodeRowKey(columns: readonly string[]): (row: QueryRow) => string {
  return (row) =>
    JSON.stringify(
      columns.map((name) => {
        return encodeSqlEqualityValue(row[name] ?? null);
      }),
    );
}

interface RecursiveCteState {
  readonly columns: readonly string[];
  readonly rows: QueryRow[];
  frontier: QueryRow[];
  iterations: number;
  absorb(step: QueryResult): void;
}

/**
 * Drives one recursive CTE to its fixpoint: absorb() renames a step result positionally onto the
 * base columns, appends what is new (all rows for UNION ALL, unseen rows for UNION), and refreshes
 * the frontier. Hard caps bound runaway recursions explicitly instead of exhausting memory.
 */
export function createRecursiveCteState(base: QueryResult, all: boolean): RecursiveCteState {
  const columns = base.columns;
  const encode = encodeRowKey(columns);
  const seen = new Set<string>();
  const admit = (candidates: readonly QueryRow[]): QueryRow[] => {
    if (all) return [...candidates];
    const fresh: QueryRow[] = [];
    for (const row of candidates) {
      const key = encode(row);
      if (seen.has(key)) continue;
      seen.add(key);
      fresh.push(row);
    }
    return fresh;
  };
  const state: RecursiveCteState = {
    columns,
    rows: [],
    frontier: [],
    iterations: 0,
    absorb(step: QueryResult): void {
      state.iterations += 1;
      if (state.iterations > MAX_RECURSIVE_ITERATIONS) {
        throw new RangeError(
          `Recursive CTE exceeded ${String(MAX_RECURSIVE_ITERATIONS)} iterations`,
        );
      }
      if (step.columns.length !== columns.length) {
        throw new TypeError("Recursive CTE members must select the same number of columns");
      }
      const renamed = step.rows.map((row) =>
        Object.fromEntries(
          columns.map((name, column) => [name, row[step.columns[column] ?? ""] ?? null]),
        ),
      );
      state.frontier = admit(renamed);
      state.rows.push(...state.frontier);
      if (state.rows.length > MAX_RECURSIVE_ROWS) {
        throw new RangeError(`Recursive CTE exceeded ${String(MAX_RECURSIVE_ROWS)} rows`);
      }
    },
  };
  const seeded = admit(base.rows);
  state.rows.push(...seeded);
  state.frontier = seeded;
  return state;
}

/** The direct child expressions of a node; subqueries and EXISTS scope their own blocks. */
export function childExpressions(expression: Expression): Expression[] {
  if (expression.kind === "binary") return [expression.left, expression.right];
  if (expression.kind === "call") return [...expression.arguments];
  if (expression.kind === "list") return [...expression.items];
  if (expression.kind === "condition" || expression.kind === "logical") {
    return [expression.left, expression.right];
  }
  if (expression.kind === "not") return [expression.operand];
  if (expression.kind === "case") {
    return [
      ...expression.branches.flatMap((branch) => [branch.when, branch.then]),
      ...(expression.otherwise === undefined ? [] : [expression.otherwise]),
    ];
  }
  if (expression.kind === "fts") return expression.columns === "*" ? [] : [...expression.columns];
  if (expression.kind === "window") {
    return [
      ...expression.partitionBy,
      ...expression.orderBy.map((order) => order.expression),
      ...(expression.argument === undefined ? [] : [expression.argument]),
    ];
  }
  return [];
}

/**
 * `childExpressions` in reverse: the same node with each child replaced by `map(child)`. A node
 * with no children — or one whose children are positions this cannot rebuild, like a window's
 * own clauses — comes back untouched, so a caller that rewrites a subtree has to reach those
 * itself. Kept beside `childExpressions` because the two have to agree about what a child is.
 */
export function mapChildExpressions(
  expression: Expression,
  map: (child: Expression) => Expression,
): Expression {
  if (expression.kind === "binary" || expression.kind === "condition") {
    return { ...expression, left: map(expression.left), right: map(expression.right) };
  }
  if (expression.kind === "logical") {
    return { ...expression, left: map(expression.left), right: map(expression.right) };
  }
  if (expression.kind === "not") return { ...expression, operand: map(expression.operand) };
  if (expression.kind === "call") {
    return { ...expression, arguments: expression.arguments.map(map) };
  }
  if (expression.kind === "list") return { ...expression, items: expression.items.map(map) };
  if (expression.kind === "case") {
    return {
      ...expression,
      branches: expression.branches.map((branch) => ({
        when: map(branch.when),
        then: map(branch.then),
      })),
      ...(expression.otherwise === undefined ? {} : { otherwise: map(expression.otherwise) }),
    };
  }
  return expression;
}

/**
 * Visits every expression position of one block — select, predicates, HAVING, GROUP BY,
 * ORDER BY, and join conditions — without descending into nested blocks. Every plan feature
 * that scans "all expressions of a block" goes through here, so a future clause is added to
 * one list instead of one per feature.
 */
export function forEachBlockExpression(
  block: CompiledQuery,
  visit: (expression: Expression) => void,
): void {
  for (const item of block.select) visit(item.expression);
  for (const predicate of [...block.predicates, ...block.having]) {
    visit(predicate.left);
    visit(predicate.right);
  }
  block.groupBy.forEach(visit);
  for (const order of block.orderBy) visit(order.expression);
  for (const join of block.joins) {
    visit(join.left);
    visit(join.right);
    if (join.on !== undefined) visit(join.on);
  }
}

/** Visits every nested block of one block's sources (derived, union, windowed, recursive). */
export function forEachNestedBlock(
  block: CompiledQuery,
  visit: (nested: CompiledQuery) => void,
): void {
  for (const source of [block.base, ...block.joins]) {
    if (source.derived !== undefined) visit(source.derived);
    if (source.union !== undefined) source.union.blocks.forEach(visit);
    if (source.windowed !== undefined) visit(source.windowed.block);
    if (source.recursive !== undefined) {
      visit(source.recursive.base);
      visit(source.recursive.step);
    }
  }
}

/**
 * Rewrites every expression slot of one block in place, the writing counterpart to
 * `forEachBlockExpression`. The mapper receives each root expression and returns its
 * replacement; returning the same object leaves the slot untouched.
 */
export function mapBlockExpressions(
  block: CompiledQuery,
  map: (expression: Expression) => Expression,
): void {
  for (const item of block.select) item.expression = map(item.expression);
  for (const predicate of [...block.predicates, ...block.having]) {
    predicate.left = map(predicate.left);
    predicate.right = map(predicate.right);
  }
  block.groupBy = block.groupBy.map(map);
  for (const order of block.orderBy) order.expression = map(order.expression);
  for (const join of block.joins) {
    join.left = map(join.left);
    join.right = map(join.right);
    if (join.on !== undefined) join.on = map(join.on);
  }
}

/**
 * Replaces CURRENT_DATE / CURRENT_TIMESTAMP / LOCALTIME with one reading of the clock, so that
 * every row of one execution — and both executors — see a single instant. Plans that never
 * mention them are returned untouched, which is every plan the parser did not flag.
 */
export function resolveStatementDatetimes(
  plan: CompiledQuery,
  now: Date = new Date(),
): CompiledQuery {
  if (plan.usesStatementDatetime !== true) return plan;
  const iso = now.toISOString();
  const values = new Map<string, QueryValue>([
    ["CURRENT_DATE", new Date(`${iso.slice(0, 10)}T00:00:00.000Z`)],
    ["CURRENT_TIMESTAMP", new Date(now.getTime())],
    ["LOCALTIME", iso.slice(11, 19)],
  ]);
  const resolved = structuredClone(plan);
  const substitute = (expression: Expression): Expression => {
    if (expression.kind === "subquery" || expression.kind === "exists") {
      resolveBlock(expression.block);
      return expression;
    }
    if (expression.kind === "call" && values.has(expression.name)) {
      return { kind: "literal", value: values.get(expression.name) ?? null };
    }
    if (expression.kind === "window") {
      // A window's own clauses are positions mapChildExpressions cannot rebuild; the plan is
      // already a private clone, so they are rewritten in place.
      expression.partitionBy = expression.partitionBy.map(substitute);
      for (const order of expression.orderBy) order.expression = substitute(order.expression);
      if (expression.argument !== undefined) expression.argument = substitute(expression.argument);
      return expression;
    }
    return mapChildExpressions(expression, substitute);
  };
  const resolveBlock = (block: CompiledQuery): void => {
    forEachNestedBlock(block, resolveBlock);
    mapBlockExpressions(block, substitute);
  };
  resolveBlock(resolved);
  delete resolved.usesStatementDatetime;
  return resolved;
}

/** One top-level full-text MATCH conjunct of a plan, with its resolved column expressions. */
export interface FtsMatchConjunct {
  columns: Expression[];
  query: string;
}

/**
 * The plan's top-level `MATCH ... AGAINST` conjuncts — the ones index pruning may exploit,
 * because every result row must satisfy them. This is the sole authority on how a bare MATCH
 * appears in `plan.predicates`: `splitCondition` wraps non-comparison booleans as
 * `IS TRUE` predicates, and a test pins that pairing so an optimizer or parser change that
 * rewraps the conjunct fails loudly instead of silently disabling pruning. Unexpanded `"*"`
 * documents and negated/OR-wrapped matches are deliberately excluded.
 */
export function topLevelFtsMatchConjuncts(plan: CompiledQuery): FtsMatchConjunct[] {
  return plan.predicates.flatMap((predicate) =>
    predicate.operator === "IS TRUE" &&
    predicate.left.kind === "fts" &&
    predicate.left.op === "match" &&
    predicate.left.columns !== "*"
      ? [{ columns: predicate.left.columns, query: predicate.left.query }]
      : [],
  );
}

/**
 * True when the plan reads anything beyond its single flat base scan: a nested block (derived
 * table, set-operation branch, windowed or recursive source) or a subquery/EXISTS expression.
 * Selectivity proofs that reason only about the base scan's predicates (the live-query zone
 * gate) are unsound for such plans — a change the base predicates reject can still shift a
 * value the nested read produces — so they must widen to "can affect".
 */
export function planReadsBeyondSingleScan(plan: CompiledQuery): boolean {
  let found = false;
  forEachNestedBlock(plan, () => {
    found = true;
  });
  const check = (expression: Expression): void => {
    if (expression.kind === "subquery" || expression.kind === "exists") {
      found = true;
      return;
    }
    childExpressions(expression).forEach(check);
  };
  forEachBlockExpression(plan, check);
  return found;
}

/** True when any expression in the plan or its nested blocks is a full-text node. */
export function planContainsFts(plan: CompiledQuery, op?: "match" | "bm25"): boolean {
  let found = false;
  const expressionHasFts = (expression: Expression): void => {
    if (found) return;
    if (expression.kind === "fts") {
      found ||= op === undefined || expression.op === op;
      return;
    }
    if (expression.kind === "subquery" || expression.kind === "exists") {
      found ||= planContainsFts(expression.block, op);
      return;
    }
    childExpressions(expression).forEach(expressionHasFts);
  };
  forEachNestedBlock(plan, (nested) => {
    if (!found) found = planContainsFts(nested, op);
  });
  forEachBlockExpression(plan, expressionHasFts);
  return found;
}

/**
 * Expands every `MATCH(*)`/`BM25(*)` in the plan into the single scan source's searchable
 * columns (string, number, datetime — booleans are excluded from documents), and enforces the
 * v1 restriction that a full-text document draws from exactly one base table source. Runs at
 * prepare time when the catalog is known. Copy-on-write: plans without full-text nodes pass
 * through untouched, and full-text plans are cloned before rewriting — the input is often the
 * compile cache's own copy (subquery resolution returns the original plan when it has nothing
 * to resolve), and freezing an expansion into the cache would pin a stale column list across
 * later migrations.
 */
export function expandFtsColumns(
  plan: CompiledQuery,
  searchableColumnsFor: (tableName: string) => readonly string[] | undefined,
): CompiledQuery {
  if (!planContainsFts(plan)) return plan;
  plan = structuredClone(plan);
  const expand = (expression: Expression, block: CompiledQuery): void => {
    if (expression.kind === "subquery" || expression.kind === "exists") {
      expandBlock(expression.block);
      return;
    }
    if (expression.kind !== "fts") {
      for (const child of childExpressions(expression)) expand(child, block);
      return;
    }
    if (block.base.derived !== undefined || block.base.union !== undefined) {
      throw new TypeError("Full-text search requires a scanned base table");
    }
    if (expression.columns !== "*") return;
    if (block.joins.length > 0) {
      throw new TypeError(
        "Full-text search over every column requires a single-table query; name the columns explicitly",
      );
    }
    const names = searchableColumnsFor(block.base.table);
    if (names === undefined || names.length === 0) {
      throw new TypeError(`Full-text search found no searchable columns: ${block.base.table}`);
    }
    expression.columns = names.map((name) => ({ kind: "column", reference: name }));
  };
  const expandBlock = (block: CompiledQuery): void => {
    forEachNestedBlock(block, expandBlock);
    forEachBlockExpression(block, (expression) => {
      expand(expression, block);
    });
  };
  expandBlock(plan);
  return plan;
}

/**
 * Aggregates the frame members named by position, for the frames whose rows are not contiguous.
 * EXCLUDE puts a hole in the middle of a frame, which the prefix sums the common path uses
 * cannot represent, so those positions walk their members instead.
 */
function aggregateWindowMembers(
  window: WindowSpec,
  values: readonly unknown[],
  members: readonly number[],
): unknown {
  const present = members.filter((member) => {
    const value = values[member];
    return value !== null && value !== undefined;
  });
  if (window.name === "COUNT") {
    return window.argumentAlias === undefined ? members.length : present.length;
  }
  if (window.name === "FIRST_VALUE") return values[members[0] ?? -1] ?? null;
  if (window.name === "LAST_VALUE") return values[members[members.length - 1] ?? -1] ?? null;
  if (window.name === "NTH_VALUE") return values[members[(window.offset ?? 1) - 1] ?? -1] ?? null;
  if (present.length === 0) return null;
  if (window.name === "SUM" || window.name === "AVG") {
    const total = present.reduce<number>((sum, member) => sum + numeric(values[member]), 0);
    return window.name === "SUM" ? total : total / present.length;
  }
  let best: unknown;
  for (const member of present) {
    const candidate = values[member];
    if (
      best === undefined ||
      (window.name === "MIN"
        ? compareValues(candidate, best) < 0
        : compareValues(candidate, best) > 0)
    ) {
      best = candidate;
    }
  }
  return best ?? null;
}

/**
 * Computes one aggregate window over partition-sorted row indexes. Without an explicit frame the
 * SQL default applies: no OVER ordering makes every partition row share the whole-partition
 * aggregate, and ordering gives each row the running aggregate through its ordering peers
 * (RANGE UNBOUNDED PRECEDING AND CURRENT ROW). Explicit ROWS frames bound by row distance;
 * RANGE frames take only UNBOUNDED and CURRENT ROW bounds, where CURRENT ROW spans the peer
 * group. COUNT of an empty frame is 0 and every other aggregate NULL.
 */
function applyAggregateWindow(
  rows: QueryRow[],
  indexes: readonly number[],
  window: WindowSpec,
  samePartition: (left: number, right: number) => boolean,
  sameOrderKeys: (left: number, right: number) => boolean,
): void {
  const frame: WindowFrame = window.frame ?? {
    unit: "range",
    start: { kind: "unbounded-preceding" },
    end:
      window.orderAliases.length === 0 ? { kind: "unbounded-following" } : { kind: "current-row" },
  };
  let start = 0;
  while (start < indexes.length) {
    let end = start + 1;
    while (end < indexes.length && samePartition(indexes[start] ?? 0, indexes[end] ?? 0)) {
      end += 1;
    }
    applyAggregateWindowPartition(rows, indexes, window, frame, sameOrderKeys, start, end);
    start = end;
  }
}

function applyAggregateWindowPartition(
  rows: QueryRow[],
  indexes: readonly number[],
  window: WindowSpec,
  frame: WindowFrame,
  sameOrderKeys: (left: number, right: number) => boolean,
  start: number,
  end: number,
): void {
  const size = end - start;
  // Peer-group bounds per position; with no OVER ordering the whole partition is one peer group.
  const peerStart = new Array<number>(size).fill(0);
  const peerEnd = new Array<number>(size).fill(size);
  if (window.orderAliases.length > 0) {
    let groupBegin = 0;
    for (let position = 1; position <= size; position += 1) {
      if (
        position === size ||
        !sameOrderKeys(indexes[start + groupBegin] ?? 0, indexes[start + position] ?? 0)
      ) {
        for (let member = groupBegin; member < position; member += 1) {
          peerStart[member] = groupBegin;
          peerEnd[member] = position;
        }
        groupBegin = position;
      }
    }
  }
  const values: unknown[] = [];
  const prefixNonNull = new Float64Array(size + 1);
  const prefixSum = new Float64Array(size + 1);
  const sums = window.name === "SUM" || window.name === "AVG";
  for (let position = 0; position < size; position += 1) {
    const value =
      window.argumentAlias === undefined
        ? undefined
        : (rows[indexes[start + position] ?? -1]?.[window.argumentAlias] ?? null);
    values.push(value);
    const nonNull = window.argumentAlias !== undefined && value !== null && value !== undefined;
    prefixNonNull[position + 1] = (prefixNonNull[position] ?? 0) + (nonNull ? 1 : 0);
    prefixSum[position + 1] = (prefixSum[position] ?? 0) + (sums && nonNull ? numeric(value) : 0);
  }
  // GROUPS frames count peer groups rather than rows, so each position needs its group's
  // ordinal and the group boundaries to translate an offset back into row positions.
  const groupOrdinal = new Array<number>(size).fill(0);
  const groupStarts: number[] = [];
  if (frame.unit === "groups") {
    for (let position = 0; position < size; position += 1) {
      if (position === 0 || peerStart[position] !== peerStart[position - 1]) {
        groupStarts.push(peerStart[position] ?? position);
      }
      groupOrdinal[position] = groupStarts.length - 1;
    }
  }
  const groupEdge = (ordinal: number, isStart: boolean): number => {
    if (ordinal < 0) return isStart ? 0 : 0;
    if (ordinal >= groupStarts.length) return size;
    return isStart ? (groupStarts[ordinal] ?? 0) : (groupStarts[ordinal + 1] ?? size);
  };
  const bound = (edge: WindowFrameBound, position: number, isStart: boolean): number => {
    switch (edge.kind) {
      case "unbounded-preceding":
        return 0;
      case "unbounded-following":
        return size;
      case "preceding":
        if (frame.unit === "groups") {
          return groupEdge((groupOrdinal[position] ?? 0) - (edge.offset ?? 0), isStart);
        }
        return position - (edge.offset ?? 0) + (isStart ? 0 : 1);
      case "following":
        if (frame.unit === "groups") {
          return groupEdge((groupOrdinal[position] ?? 0) + (edge.offset ?? 0), isStart);
        }
        return position + (edge.offset ?? 0) + (isStart ? 0 : 1);
      case "current-row":
        if (frame.unit === "range" || frame.unit === "groups") {
          return (isStart ? peerStart[position] : peerEnd[position]) ?? position;
        }
        return position + (isStart ? 0 : 1);
    }
  };
  /**
   * The positions EXCLUDE removes from one frame: the current row, its whole peer group, or the
   * peers other than the current row (T612).
   */
  const excluded = (position: number): { from: number; to: number; keepCurrent: boolean } => {
    switch (frame.exclude) {
      case "current-row":
        return { from: position, to: position + 1, keepCurrent: false };
      case "group":
        return {
          from: peerStart[position] ?? position,
          to: peerEnd[position] ?? position + 1,
          keepCurrent: false,
        };
      case "ties":
        return {
          from: peerStart[position] ?? position,
          to: peerEnd[position] ?? position + 1,
          keepCurrent: true,
        };
      default:
        return { from: 0, to: 0, keepCurrent: true };
    }
  };
  for (let position = 0; position < size; position += 1) {
    const low = Math.max(0, Math.min(size, bound(frame.start, position, true)));
    const high = Math.max(0, Math.min(size, bound(frame.end, position, false)));
    let value: unknown;
    if (frame.exclude !== undefined) {
      const skip = excluded(position);
      const members: number[] = [];
      for (let member = low; member < high; member += 1) {
        const dropped =
          member >= skip.from && member < skip.to && !(skip.keepCurrent && member === position);
        if (!dropped) members.push(member);
      }
      value = aggregateWindowMembers(window, values, members);
      const row = rows[indexes[start + position] ?? -1];
      if (row !== undefined) row[window.alias] = asQueryValue(value);
      continue;
    }
    if (high <= low) {
      value = window.name === "COUNT" ? 0 : null;
    } else if (window.name === "FIRST_VALUE") {
      value = values[low] ?? null;
    } else if (window.name === "NTH_VALUE") {
      const position = low + (window.offset ?? 1) - 1;
      value = position < high ? (values[position] ?? null) : null;
    } else if (window.name === "LAST_VALUE") {
      value = values[high - 1] ?? null;
    } else if (window.name === "COUNT") {
      value =
        window.argumentAlias === undefined
          ? high - low
          : (prefixNonNull[high] ?? 0) - (prefixNonNull[low] ?? 0);
    } else if (sums) {
      const nonNull = (prefixNonNull[high] ?? 0) - (prefixNonNull[low] ?? 0);
      const total = (prefixSum[high] ?? 0) - (prefixSum[low] ?? 0);
      value = nonNull === 0 ? null : window.name === "SUM" ? total : total / nonNull;
    } else {
      let best: unknown;
      for (let member = low; member < high; member += 1) {
        const candidate = values[member];
        if (candidate === null || candidate === undefined) continue;
        if (
          best === undefined ||
          (window.name === "MIN"
            ? compareValues(candidate, best) < 0
            : compareValues(candidate, best) > 0)
        ) {
          best = candidate;
        }
      }
      value = best ?? null;
    }
    const row = rows[indexes[start + position] ?? -1];
    if (row !== undefined) row[window.alias] = asQueryValue(value);
  }
}

/**
 * NTILE, PERCENT_RANK, and CUME_DIST over partition-sorted row indexes. NTILE deals rows into
 * `offset` buckets with the larger buckets first; PERCENT_RANK is (rank - 1) / (rows - 1) with a
 * lone row at 0; CUME_DIST is the fraction of partition rows at or before the current peer group.
 */
function applyDistributionWindow(
  rows: QueryRow[],
  indexes: readonly number[],
  window: WindowSpec,
  samePartition: (left: number, right: number) => boolean,
  sameOrderKeys: (left: number, right: number) => boolean,
): void {
  let start = 0;
  while (start < indexes.length) {
    let end = start + 1;
    while (end < indexes.length && samePartition(indexes[start] ?? 0, indexes[end] ?? 0)) {
      end += 1;
    }
    const size = end - start;
    let groupBegin = 0;
    const assign = (position: number, value: number): void => {
      const row = rows[indexes[start + position] ?? -1];
      if (row !== undefined) row[window.alias] = value;
    };
    for (let position = 1; position <= size; position += 1) {
      if (
        position === size ||
        !sameOrderKeys(indexes[start + groupBegin] ?? 0, indexes[start + position] ?? 0)
      ) {
        for (let member = groupBegin; member < position; member += 1) {
          if (window.name === "PERCENT_RANK") {
            assign(member, size === 1 ? 0 : groupBegin / (size - 1));
          } else if (window.name === "CUME_DIST") {
            assign(member, position / size);
          }
        }
        groupBegin = position;
      }
    }
    if (window.name === "NTILE") {
      const buckets = window.offset ?? 1;
      const bucketSize = Math.floor(size / buckets);
      const remainder = size % buckets;
      let position = 0;
      for (let bucket = 1; bucket <= buckets && position < size; bucket += 1) {
        const width = bucketSize + (bucket <= remainder ? 1 : 0);
        for (let member = 0; member < width && position < size; member += 1, position += 1) {
          assign(position, bucket);
        }
      }
    }
    start = end;
  }
}

/** Computes LAG/LEAD over partition-sorted row indexes: the argument value offset rows away. */
function applyOffsetWindow(
  rows: QueryRow[],
  indexes: readonly number[],
  window: WindowSpec,
  samePartition: (left: number, right: number) => boolean,
): void {
  const offset = window.offset ?? 1;
  const fallback = window.fallback ?? null;
  let start = 0;
  while (start < indexes.length) {
    let end = start + 1;
    while (end < indexes.length && samePartition(indexes[start] ?? 0, indexes[end] ?? 0)) {
      end += 1;
    }
    for (let position = start; position < end; position += 1) {
      const source = window.name === "LAG" ? position - offset : position + offset;
      const row = rows[indexes[position] ?? -1];
      if (row === undefined) continue;
      if (source < start || source >= end) {
        row[window.alias] = fallback;
        continue;
      }
      const sourceRow = rows[indexes[source] ?? -1];
      row[window.alias] =
        window.argumentAlias === undefined ? fallback : (sourceRow?.[window.argumentAlias] ?? null);
    }
    start = end;
  }
}

function containsDistinctCount(expression: Expression): boolean {
  if (expression.kind === "call" && expression.distinct === true) return true;
  return childExpressions(expression).some(containsDistinctCount);
}

function containsWindow(expression: Expression): boolean {
  if (expression.kind === "window") return true;
  return childExpressions(expression).some(containsWindow);
}

/** Whether an expression still contains an unresolved GROUPING call (T433). */
function containsGrouping(expression: Expression): boolean {
  if (expression.kind === "call" && expression.name === "GROUPING") return true;
  return childExpressions(expression).some(containsGrouping);
}

function containsFtsExpression(expression: Expression): boolean {
  if (expression.kind === "fts") return true;
  return childExpressions(expression).some(containsFtsExpression);
}

/**
 * Appends window-function columns to an executed inner-block result. Rows sort stably by the
 * hidden partition and ordering aliases with the same comparison semantics as ORDER BY;
 * ROW_NUMBER numbers rows per partition, RANK shares ranks across ordering ties with gaps, and
 * DENSE_RANK shares without gaps. Without OVER ordering every partition row is a peer.
 */
export function applyWindowFunctions(
  result: QueryResult,
  windows: readonly WindowSpec[],
): QueryResult {
  const rows = result.rows.map((row) => ({ ...row }));
  for (const window of windows) {
    const indexes = rows.map((_, index) => index);
    // Decorate before sorting: `comparable` was being re-run inside the comparator, so every
    // key was converted O(n log n) times per alias instead of once per row. Precomputing the
    // comparable value per row per alias makes the comparator pure array reads, and the
    // partition/peer checks below read the same arrays.
    const partitionKeys = window.partitionAliases.map((alias) =>
      rows.map((row) => comparable(row[alias] ?? null)),
    );
    const orderKeys = window.orderAliases.map(({ alias }) =>
      rows.map((row) => comparable(row[alias] ?? null)),
    );
    // A window sorts by partition then by its own ORDER BY, and then walks the result several
    // times over — for peer groups, frame bounds, and the values themselves. Every one of those
    // passes compares keys, so the terms are prepared once into comparison-ready columns rather
    // than re-read and re-dispatched per comparison; see sort-keys.ts.
    const partitionColumns = partitionKeys.map((keys) =>
      buildSortKeyColumn(keys.length, (index) => keys[index]),
    );
    const orderColumns = orderKeys.map((keys) =>
      buildSortKeyColumn(keys.length, (index) => keys[index]),
    );
    const orderTerms = window.orderAliases.map((term) => ({
      nullPlacement: term.nulls === undefined ? 0 : term.nulls === "first" ? 1 : -1,
      descending: term.direction === "desc",
    }));
    const compare = (left: number, right: number): number => {
      for (const column of partitionColumns) {
        const comparison = column.compare(left, right);
        if (comparison !== 0) return comparison;
      }
      for (let index = 0; index < orderColumns.length; index += 1) {
        const column = orderColumns[index];
        const term = orderTerms[index];
        if (column === undefined || term === undefined) continue;
        if (term.nullPlacement !== 0) {
          const leftNull = column.isNull(left);
          const rightNull = column.isNull(right);
          if (leftNull || rightNull) {
            if (leftNull && rightNull) continue;
            return (leftNull ? -1 : 1) * term.nullPlacement;
          }
        }
        const comparison = column.compare(left, right);
        if (comparison !== 0) return term.descending ? -comparison : comparison;
      }
      // Arrival order breaks the remaining ties, which is what makes the walk deterministic.
      return left - right;
    };
    indexes.sort(compare);
    const samePartition = (left: number, right: number): boolean => {
      for (const column of partitionColumns) {
        if (column.compare(left, right) !== 0) return false;
      }
      return true;
    };
    const sameOrderKeys = (left: number, right: number): boolean => {
      for (const column of orderColumns) {
        if (column.compare(left, right) !== 0) return false;
      }
      return true;
    };
    if (window.name === "LAG" || window.name === "LEAD") {
      applyOffsetWindow(rows, indexes, window, samePartition);
      continue;
    }
    if (window.name === "NTILE" || window.name === "PERCENT_RANK" || window.name === "CUME_DIST") {
      applyDistributionWindow(rows, indexes, window, samePartition, sameOrderKeys);
      continue;
    }
    if (window.name !== "ROW_NUMBER" && window.name !== "RANK" && window.name !== "DENSE_RANK") {
      applyAggregateWindow(rows, indexes, window, samePartition, sameOrderKeys);
      continue;
    }
    let rowNumber = 0;
    let rank = 0;
    let denseRank = 0;
    for (const [position, index] of indexes.entries()) {
      const previous = position > 0 ? indexes[position - 1] : undefined;
      if (previous === undefined || !samePartition(previous, index)) {
        rowNumber = 1;
        rank = 1;
        denseRank = 1;
      } else {
        rowNumber += 1;
        if (!sameOrderKeys(previous, index)) {
          rank = rowNumber;
          denseRank += 1;
        }
      }
      const row = rows[index];
      if (row === undefined) continue;
      row[window.alias] =
        window.name === "ROW_NUMBER" ? rowNumber : window.name === "RANK" ? rank : denseRank;
    }
  }
  return {
    columns: [...result.columns, ...windows.map((window) => window.alias)],
    rows,
  };
}

/** Executes each derived or set-operation source with the row reference. */
function resolveDerivedRowTables(
  plan: CompiledQuery,
  tables: ReadonlyMap<string, DatabaseRow[]>,
): ReadonlyMap<string, DatabaseRow[]> {
  const sources = [plan.base, ...plan.joins];
  const needsDual = sources.some(
    (source) => source.derived === undefined && source.table === DUAL_TABLE,
  );
  if (
    !needsDual &&
    !sources.some(
      (source) =>
        source.derived !== undefined ||
        source.union !== undefined ||
        source.windowed !== undefined ||
        source.recursive !== undefined,
    )
  ) {
    return tables;
  }
  const resolved = new Map(tables);
  if (needsDual) resolved.set(DUAL_TABLE, dualTableRows());
  for (const source of sources) {
    if (source.recursive !== undefined) {
      const { reference, base, step, all } = source.recursive;
      const state = createRecursiveCteState(executeRowQuery(base, tables), all);
      while (state.frontier.length > 0) {
        const stepTables = new Map(tables);
        stepTables.set(reference, state.frontier);
        state.absorb(executeRowQuery(step, stepTables));
      }
      resolved.set(source.table, state.rows);
      continue;
    }
    if (source.union !== undefined) {
      const results = source.union.blocks.map((block) => executeRowQuery(block, tables));
      resolved.set(source.table, combineUnionResults(results, source.union.ops).rows);
      continue;
    }
    if (source.windowed !== undefined) {
      const inner = executeRowQuery(source.windowed.block, tables);
      resolved.set(source.table, applyWindowFunctions(inner, source.windowed.windows).rows);
      continue;
    }
    if (source.derived === undefined) continue;
    resolved.set(source.table, executeRowQuery(source.derived, tables).rows);
  }
  return resolved;
}

function normalizeColumnarTables(
  plan: CompiledQuery,
  tables: ReadonlyMap<string, DatabaseRow[]>,
): Map<string, ColumnarTable> {
  const requiredTables = new Set([plan.base.table, ...plan.joins.map((join) => join.table)]);
  const schemas = new Map(
    [...tables].map(([name, rows]) => [
      name,
      [...new Set(rows.flatMap((row) => Object.keys(row)))],
    ]),
  );
  const requestedColumns = referencedColumns(plan, schemas);
  return new Map(
    [...tables]
      .filter(([name]) => requiredTables.has(name))
      .map(([name, table]) => [
        name,
        columnarTableFromRows(name, table, requestedColumns.get(name) ?? []),
      ]),
  );
}

/** Correctness reference retained while the vector executor matures. */
export function executeRowQuery(
  plan: CompiledQuery,
  tables: ReadonlyMap<string, DatabaseRow[]>,
): QueryResult {
  assertTailParametersBound(plan);
  validateGrouping(plan);
  plan = resolveStatementDatetimes(plan);
  plan = expandSourceColumnAliases(plan, wildcardRowColumns(tables));
  plan = expandNaturalJoins(plan, wildcardRowColumns(tables));
  plan = expandQualifiedWildcards(plan, wildcardRowColumns(tables));
  const ties = withTiesPlan(plan);
  plan = ties.plan;
  plan = expandFtsColumns(plan, rowTableSearchableColumns(tables));
  const resolution = subqueryResolutionSteps(plan);
  for (const step of resolution.steps) step.substitute(executeRowQuery(step.block, tables));
  // Stats annotation writes into the plan; when resolution had nothing to clone, the plan is
  // still the caller's (possibly cached) object, and frozen statistics would survive into later
  // executions against different rows.
  plan =
    resolution.plan === plan && planContainsFts(plan, "bm25")
      ? structuredClone(plan)
      : resolution.plan;
  tables = resolveDerivedRowTables(plan, tables);
  plan = expandDistinctWildcard(plan, wildcardRowColumns(tables));
  annotateRowFtsStats(plan, tables);
  let contexts: RowContext[] = (tables.get(plan.base.table) ?? []).map((row) => ({
    [plan.base.alias]: row,
  }));
  for (const join of plan.joins)
    contexts = executeJoin(contexts, join, tables.get(join.table) ?? []);
  contexts = contexts.filter((context) =>
    plan.predicates.every((predicate) => evaluatePredicate(predicate, context)),
  );
  const grouped =
    plan.groupBy.length > 0 || plan.select.some((item) => hasAggregate(item.expression));
  let rows: QueryRow[];
  if (grouped) {
    const groups = new Map<string, RowContext[]>();
    if (contexts.length === 0 && plan.groupBy.length === 0) groups.set("[]", []);
    for (const context of contexts) {
      const key = JSON.stringify(
        plan.groupBy.map((expression) => comparable(evaluate(expression, context))),
      );
      const group = groups.get(key) ?? [];
      group.push(context);
      groups.set(key, group);
    }
    rows = [...groups.values()]
      .filter((group) =>
        plan.having.every(
          (predicate) =>
            evaluateBooleanExpression(
              {
                kind: "condition",
                operator: predicate.operator,
                left: predicate.left,
                right: predicate.right,
              },
              (nested) => evaluate(nested, group[0] ?? {}, group),
            ) === true,
        ),
      )
      .map((group) => project(plan.select, group[0] ?? {}, group));
  } else {
    rows = contexts.map((context) => project(plan.select, context));
  }
  if (plan.orderBy.length > 0) {
    // Only a wildcard select needs the source shapes: every other select resolves against its
    // own output aliases.
    const orderSources =
      plan.select[0]?.expression.kind === "wildcard"
        ? [plan.base, ...plan.joins].map((source) => ({
            alias: source.alias,
            columns: rowTableColumnNames(tables.get(source.table) ?? []),
          }))
        : [];
    const sortColumns = plan.orderBy.map(({ expression, direction, nulls }) => ({
      outputName: orderOutputName(expression, plan.select, orderSources),
      multiplier: direction === "desc" ? -1 : 1,
      nulls,
    }));
    rows.sort((left, right) => {
      for (const { outputName, multiplier, nulls } of sortColumns) {
        const placed = explicitNullOrder(left[outputName], right[outputName], nulls);
        if (placed !== undefined && placed !== 0) return placed;
        const comparison = compareValues(left[outputName], right[outputName]);
        if (comparison !== 0) return comparison * multiplier;
      }
      return 0;
    });
  }
  if (plan.limit !== undefined || plan.offset !== undefined) {
    const start = plan.offset ?? 0;
    rows = rows.slice(start, plan.limit === undefined ? undefined : start + plan.limit);
  }
  const columns =
    plan.select[0]?.expression.kind === "wildcard"
      ? Object.keys(rows[0] ?? {})
      : plan.select.map((item) => item.alias);
  return ties.trim({ columns, rows });
}

/** One ORDER BY resolution source: an alias and the columns a wildcard select exposes from it. */
export interface OrderSourceShape {
  readonly alias: string;
  readonly columns: readonly string[];
}

/**
 * Resolves one ORDER BY reference to the output column that carries its values, throwing when
 * nothing matches. Dropping an unresolved sort key silently would return rows in an arbitrary
 * order with no sign of the problem, which paging built on ORDER BY turns into skipped and
 * repeated rows.
 */
export function orderOutputName(
  expression: Expression,
  select: readonly SelectItem[],
  sources: readonly OrderSourceShape[],
): string {
  const resolved = resolveOrderOutputName(expression, select, sources);
  if (resolved !== undefined) return resolved;
  const target = expression.kind === "column" ? `: ${expression.reference}` : "";
  throw new TypeError(`ORDER BY requires a selected column or output alias${target}`);
}

function resolveOrderOutputName(
  expression: Expression,
  select: readonly SelectItem[],
  sources: readonly OrderSourceShape[],
): string | undefined {
  if (expression.kind !== "column") return undefined;
  // A wildcard select names its outputs after the source columns: bare from one source, and
  // `alias.column` from several. A reference is rewritten into that naming rather than looked up
  // verbatim, so `SELECT * FROM people ORDER BY people.name` orders by the `name` output.
  if (select[0]?.expression.kind === "wildcard") {
    return wildcardOrderOutputName(expression.reference, sources);
  }
  const selected = select.find(
    (item) =>
      (item.expression.kind === "column" && item.expression.reference === expression.reference) ||
      (!expression.reference.includes(".") && item.alias === expression.reference),
  );
  return selected?.alias;
}

function wildcardOrderOutputName(
  reference: string,
  sources: readonly OrderSourceShape[],
): string | undefined {
  const prefixed = sources.length > 1;
  const separator = reference.indexOf(".");
  if (separator === -1) {
    // One source keeps the column's own name; unknown names are rejected by column resolution
    // before execution, so no shape lookup is needed here.
    if (!prefixed) return reference;
    const matches = sources.filter((source) => source.columns.includes(reference));
    const match = matches.length === 1 ? matches[0] : undefined;
    return match === undefined ? undefined : `${match.alias}.${reference}`;
  }
  const alias = reference.slice(0, separator);
  const column = reference.slice(separator + 1);
  const source = sources.find((candidate) => candidate.alias === alias);
  if (source === undefined) return undefined;
  // A source with no known columns is one whose shape the caller could not supply (a row table
  // with no rows), so only a column known to be absent is rejected.
  if (source.columns.length > 0 && !source.columns.includes(column)) return undefined;
  return prefixed ? `${alias}.${column}` : column;
}

/** Column names a row table exposes, as the union of its rows' keys. */
function rowTableColumnNames(rows: readonly DatabaseRow[]): string[] {
  const names = new Set<string>();
  for (const row of rows) for (const name of Object.keys(row)) names.add(name);
  return [...names];
}

function executeJoin(contexts: RowContext[], join: JoinPlan, rows: DatabaseRow[]): RowContext[] {
  if (join.on !== undefined) {
    const condition = join.on;
    const joined: RowContext[] = [];
    for (const context of contexts) {
      let matched = false;
      for (const row of rows) {
        const candidate = { ...context, [join.alias]: row };
        if (
          evaluateBooleanExpression(condition, (nested) => evaluate(nested, candidate)) === true
        ) {
          matched = true;
          joined.push(candidate);
        }
      }
      if (!matched && join.kind === "left") joined.push({ ...context, [join.alias]: undefined });
    }
    return joined;
  }
  const leftAliases = expressionAliases(join.left);
  const rightAliases = expressionAliases(join.right);
  const rightExpression = rightAliases.has(join.alias) ? join.right : join.left;
  const leftExpression = rightExpression === join.right ? join.left : join.right;
  if (
    !expressionAliases(rightExpression).has(join.alias) ||
    leftAliases.has(join.alias) === rightAliases.has(join.alias)
  ) {
    throw new TypeError(`JOIN condition must reference the new alias ${join.alias} on one side`);
  }
  const index = new Map<unknown, DatabaseRow[]>();
  for (const row of rows) {
    const key = comparable(evaluate(rightExpression, { [join.alias]: row }));
    if (!isSqlJoinKey(key)) continue;
    const matches = index.get(key) ?? [];
    matches.push(row);
    index.set(key, matches);
  }
  const joined: RowContext[] = [];
  for (const context of contexts) {
    const leftKey = comparable(evaluate(leftExpression, context));
    const matches = isSqlJoinKey(leftKey) ? (index.get(leftKey) ?? []) : [];
    if (matches.length === 0 && join.kind === "left")
      joined.push({ ...context, [join.alias]: undefined });
    else for (const row of matches) joined.push({ ...context, [join.alias]: row });
  }
  return joined;
}

function isSqlJoinKey(value: unknown): boolean {
  return (
    value !== null && value !== undefined && !(typeof value === "number" && Number.isNaN(value))
  );
}

function project(
  select: readonly SelectItem[],
  context: RowContext,
  group?: RowContext[],
): QueryRow {
  if (select[0]?.expression.kind === "wildcard") {
    const aliases = Object.keys(context);
    return Object.fromEntries(
      aliases.flatMap((alias) =>
        Object.entries(context[alias] ?? {}).map(([name, value]) => [
          aliases.length === 1 ? name : `${alias}.${name}`,
          value,
        ]),
      ),
    );
  }
  return Object.fromEntries(
    select.map((item) => [item.alias, asQueryValue(evaluate(item.expression, context, group))]),
  );
}

function evaluate(expression: Expression, context: RowContext, group?: RowContext[]): unknown {
  switch (expression.kind) {
    case "literal":
      return expression.value;
    case "parameter":
      throw new TypeError(
        `Placeholder $${String(expression.index + 1)} is unbound; pass parameters when executing`,
      );
    case "wildcard":
      return 1;
    case "subquery":
      throw new TypeError("Subqueries are only supported in WHERE, HAVING, SELECT, and IN");
    case "list":
      throw new TypeError("Value lists are only supported with IN");
    case "window":
      throw new TypeError("Window functions are only allowed in the select list");
    case "column":
      return resolveColumn(context, expression.reference);
    case "condition":
    case "logical":
    case "not":
      return evaluateBooleanExpression(expression, (nested) => evaluate(nested, context, group));
    case "exists":
      throw new TypeError("EXISTS subqueries must be resolved before evaluation");
    case "fts": {
      if (expression.columns === "*") {
        throw new TypeError("Full-text search columns must be expanded before evaluation");
      }
      const values = expression.columns.map((column) => evaluate(column, context, group));
      if (expression.op === "match") {
        return ftsMatchTruth(cachedQueryTerms(expression.query), values);
      }
      const stats = expression.stats;
      if (stats === undefined) {
        throw new TypeError("BM25 corpus statistics are missing from the plan");
      }
      return ftsBm25Row(cachedQueryTerms(expression.query), values, stats);
    }
    case "case": {
      for (const branch of expression.branches) {
        const matched = evaluateBooleanExpression(branch.when, (nested) =>
          evaluate(nested, context, group),
        );
        if (matched === true) return evaluate(branch.then, context, group);
      }
      return expression.otherwise === undefined
        ? null
        : evaluate(expression.otherwise, context, group);
    }
    case "binary": {
      const left = evaluate(expression.left, context, group);
      const right = evaluate(expression.right, context, group);
      if (left === null || left === undefined || right === null || right === undefined) return null;
      if (expression.operator === "||") {
        if (typeof left !== "string" || typeof right !== "string") {
          throw new TypeError("|| requires string operands");
        }
        return left + right;
      }
      const a = numeric(left);
      const b = numeric(right);
      if (expression.operator === "+") return a + b;
      if (expression.operator === "-") return a - b;
      if (expression.operator === "*") return a * b;
      // Division and remainder by zero are NULL, matching SQLite, not Infinity/NaN.
      if (b === 0) return null;
      return expression.operator === "%" ? a % b : a / b;
    }
    case "call": {
      if (aggregateNames.has(expression.name as AggregateName)) {
        if (group === undefined)
          throw new TypeError(`${expression.name} requires grouped execution`);
        const argument = expression.arguments[0] ?? { kind: "wildcard" as const };
        let values =
          argument.kind === "wildcard"
            ? group.map(() => 1)
            : group
                .map((row) => evaluate(argument, row))
                .filter((value) => value !== null && value !== undefined);
        if (expression.distinct === true) {
          const seen = new Set<unknown>();
          values = values.filter((value) => {
            const key = value instanceof Date ? ` d${String(value.getTime())}` : value;
            if (seen.has(key)) return false;
            seen.add(key);
            return true;
          });
        }
        if (expression.name === "COUNT") return values.length;
        if (expression.name === "SUM")
          return values.length === 0
            ? null
            : values.reduce<number>((sum, value) => sum + numeric(value), 0);
        if (expression.name === "AVG")
          return values.length === 0
            ? null
            : values.reduce<number>((sum, value) => sum + numeric(value), 0) / values.length;
        if (expression.name === "MIN")
          return values.reduce<unknown>(
            (best, value) => (best === undefined || compareValues(value, best) < 0 ? value : best),
            undefined,
          );
        return values.reduce<unknown>(
          (best, value) => (best === undefined || compareValues(value, best) > 0 ? value : best),
          undefined,
        );
      }
      if (expression.name === "COALESCE") {
        for (const argument of expression.arguments) {
          const candidate = evaluate(argument, context, group);
          if (candidate !== null && candidate !== undefined) return candidate;
        }
        return null;
      }
      if (isScalarFunctionName(expression.name)) {
        return scalarFunctionValue(
          expression.name,
          expression.arguments.map((argument) => evaluate(argument, context, group)),
        );
      }
      throw new TypeError(`${expression.name} requires grouped execution`);
    }
  }
}

function evaluatePredicate(predicate: Predicate, context: RowContext): boolean {
  if (predicate.operator === "IS TRUE") {
    return (
      evaluateBooleanExpression(predicate.left, (nested) => evaluate(nested, context)) === true
    );
  }
  if (
    predicate.operator === "LIKE" ||
    predicate.operator === "NOT LIKE" ||
    predicate.operator === "ILIKE" ||
    predicate.operator === "NOT ILIKE"
  ) {
    return (
      evaluateBooleanExpression(
        {
          kind: "condition",
          operator: predicate.operator,
          left: predicate.left,
          right: predicate.right,
          ...(predicate.escape === undefined ? {} : { escape: predicate.escape }),
        },
        (nested) => evaluate(nested, context),
      ) === true
    );
  }
  if (predicate.operator === "IN" || predicate.operator === "NOT IN") {
    if (predicate.right.kind !== "list") throw new TypeError("IN requires a value list");
    const membership = cachedListMembership(predicate.right, predicate.right.items);
    if (membership !== null) {
      const value = evaluate(predicate.left, context);
      if (value === null || value === undefined) return false;
      if (membership.set.has(comparable(value))) return predicate.operator === "IN";
      return predicate.operator === "NOT IN" && !membership.hasNull;
    }
    return inListHolds(
      predicate.operator,
      evaluate(predicate.left, context),
      predicate.right.items.map((item) => evaluate(item, context)),
    );
  }
  {
    const quantified = parseQuantified(predicate.operator);
    if (quantified !== undefined) {
      if (predicate.right.kind !== "list") {
        throw new TypeError("ANY/ALL subqueries must be resolved before evaluation");
      }
      return (
        quantifiedComparison(
          quantified.comparison,
          quantified.quantifier,
          evaluate(predicate.left, context),
          predicate.right.items.map((item) => evaluate(item, context)),
        ) === true
      );
    }
  }
  return comparisonHolds(
    predicate.operator,
    evaluate(predicate.left, context),
    evaluate(predicate.right, context),
  );
}

/**
 * SQL membership semantics: a NULL probe never matches, and NOT IN cannot be satisfied when the
 * list contains NULL because the comparison is unknown rather than false.
 */
function inListHolds(
  operator: "IN" | "NOT IN",
  value: unknown,
  items: readonly unknown[],
): boolean {
  if (value === null || value === undefined) return false;
  let hasNull = false;
  for (const item of items) {
    if (item === null || item === undefined) {
      hasNull = true;
      continue;
    }
    if (comparable(value) === comparable(item)) return operator === "IN";
  }
  return operator === "NOT IN" && !hasNull;
}

export interface ListMembership {
  set: ReadonlySet<unknown>;
  hasNull: boolean;
}

const listMembershipCache = new WeakMap<object, ListMembership | null>();

/**
 * IN lists produced by subquery materialization can hold many thousands of literals, and
 * probing them per row makes membership quadratic. A list whose items are all literals
 * gets one hash set of comparable values (plus a null flag), cached weakly on the list
 * node so repeated executions of a prepared plan reuse it. Lists with non-literal items
 * return null and take the general per-row path.
 */
export function cachedListMembership(
  node: object,
  items: ReadonlyArray<{ kind: string }>,
): ListMembership | null {
  let cached = listMembershipCache.get(node);
  if (cached === undefined) {
    if (items.every((item) => item.kind === "literal")) {
      const set = new Set<unknown>();
      let hasNull = false;
      for (const item of items as ReadonlyArray<{ kind: "literal"; value: unknown }>) {
        if (item.value === null || item.value === undefined) hasNull = true;
        else set.add(comparable(item.value));
      }
      cached = { set, hasNull };
    } else {
      cached = null;
    }
    listMembershipCache.set(node, cached);
  }
  return cached;
}

/** Compiles a LIKE pattern (% = any run, _ = any character) to an anchored RegExp, cached. */
export function likeRegExp(pattern: string, caseInsensitive = false, escape?: string): RegExp {
  return compileLikePattern(pattern, caseInsensitive, escape);
}

const extractFields: ReadonlySet<string> = new Set([
  "year",
  "quarter",
  "month",
  "week",
  "day",
  "hour",
  "minute",
  "second",
  "epoch",
  "dow",
]);

/**
 * EXTRACT(field FROM datetime), in UTC. `week` is the ISO 8601 week number and `dow` counts
 * 0 = Sunday through 6 = Saturday, both matching PostgreSQL; `epoch` is fractional seconds.
 */
function extractDatePart(field: string, value: unknown): number | null {
  const normalized = field.toLowerCase();
  if (!extractFields.has(normalized)) {
    throw new TypeError(`Unsupported EXTRACT field: ${field}`);
  }
  if (value === null || value === undefined) return null;
  if (!(value instanceof Date)) throw new TypeError("EXTRACT requires a datetime value");
  switch (normalized) {
    case "year":
      return value.getUTCFullYear();
    case "quarter":
      return Math.floor(value.getUTCMonth() / 3) + 1;
    case "month":
      return value.getUTCMonth() + 1;
    case "week": {
      const date = new Date(
        Date.UTC(value.getUTCFullYear(), value.getUTCMonth(), value.getUTCDate()),
      );
      // ISO week: shift to the Thursday of this week, then count weeks from January 1st.
      date.setUTCDate(date.getUTCDate() + 4 - (date.getUTCDay() || 7));
      const yearStart = Date.UTC(date.getUTCFullYear(), 0, 1);
      return Math.ceil(((date.getTime() - yearStart) / 86_400_000 + 1) / 7);
    }
    case "day":
      return value.getUTCDate();
    case "hour":
      return value.getUTCHours();
    case "minute":
      return value.getUTCMinutes();
    case "second":
      return value.getUTCSeconds();
    case "epoch":
      return value.getTime() / 1000;
    default:
      return value.getUTCDay();
  }
}

type LikeMatcher = (value: string) => boolean;
const likeMatcherCache = new Map<string, LikeMatcher>();

/**
 * A compiled LIKE matcher. Patterns shaped `abc%`, `%abc`, `%abc%`, and `abc` skip the regular
 * expression entirely — prefix/suffix/containment string scans are several times faster and
 * dominate real workloads — and everything else falls back to the anchored RegExp.
 */
export function likeMatches(
  pattern: string,
  value: string,
  caseInsensitive = false,
  escape?: string,
): boolean {
  const key = `${caseInsensitive ? "i" : "s"}${escape ?? ""}\0${pattern}`;
  let matcher = likeMatcherCache.get(key);
  if (matcher === undefined) {
    matcher = buildLikeMatcher(pattern, caseInsensitive, escape);
    if (likeMatcherCache.size >= 128) likeMatcherCache.clear();
    likeMatcherCache.set(key, matcher);
  }
  return matcher(value);
}

function buildLikeMatcher(
  pattern: string,
  caseInsensitive: boolean,
  escape: string | undefined,
): LikeMatcher {
  if (escape === undefined && !pattern.includes("_")) {
    const leading = pattern.startsWith("%");
    const trailing = pattern.endsWith("%");
    const body = pattern.slice(leading ? 1 : 0, trailing ? pattern.length - 1 : undefined);
    if (!body.includes("%")) {
      const needle = caseInsensitive ? body.toLowerCase() : body;
      const fold = caseInsensitive
        ? (value: string) => value.toLowerCase()
        : (value: string) => value;
      if (leading && trailing) return (value) => fold(value).includes(needle);
      if (trailing) return (value) => fold(value).startsWith(needle);
      if (leading) return (value) => fold(value).endsWith(needle);
      return (value) => fold(value) === needle;
    }
  }
  const regExp = likeRegExp(pattern, caseInsensitive, escape);
  return (value) => regExp.test(value);
}

/** Splits a quantified operator like "> ANY" into its comparison and quantifier. */
export function parseQuantified(
  operator: PredicateOperator,
): { comparison: ComparisonOperator; quantifier: "any" | "all" } | undefined {
  if (operator.endsWith(" ANY")) {
    return { comparison: operator.slice(0, -4) as ComparisonOperator, quantifier: "any" };
  }
  if (operator.endsWith(" ALL")) {
    return { comparison: operator.slice(0, -4) as ComparisonOperator, quantifier: "all" };
  }
  return undefined;
}

/**
 * SQL quantified comparison over a resolved value list, with three-valued logic: an empty list
 * is false for ANY and true for ALL before any comparison happens; otherwise NULL operands make
 * the result unknown unless a definite answer exists.
 */
export function quantifiedComparison(
  comparison: ComparisonOperator,
  quantifier: "any" | "all",
  left: unknown,
  values: readonly unknown[],
): boolean | null {
  if (values.length === 0) return quantifier === "all";
  if (left === null || left === undefined) return null;
  let sawNull = false;
  for (const value of values) {
    if (value === null || value === undefined) {
      sawNull = true;
      continue;
    }
    const holds = comparisonHolds(comparison, left, value);
    if (quantifier === "any" && holds) return true;
    if (quantifier === "all" && !holds) return false;
  }
  if (sawNull) return null;
  return quantifier === "all";
}

/** Null-safe distinctness: NULL is not distinct from NULL, and distinct from every value. */
export function distinctFromComparison(left: unknown, right: unknown): boolean {
  const leftNull = left === null || left === undefined;
  const rightNull = right === null || right === undefined;
  if (leftNull || rightNull) return leftNull !== rightNull;
  return comparable(left) !== comparable(right);
}

/**
 * Evaluates a boolean expression tree with SQL three-valued logic: comparisons over NULL are
 * unknown (null), AND/OR/NOT propagate unknown, and only the caller collapses unknown to false.
 * Leaf values come from the executor-specific callback so both executors share these semantics.
 */
export function evaluateBooleanExpression(
  expression: Expression,
  evaluateValue: (expression: Expression) => unknown,
): boolean | null {
  if (expression.kind === "logical") {
    const left = evaluateBooleanExpression(expression.left, evaluateValue);
    if (expression.operator === "and") {
      if (left === false) return false;
      const right = evaluateBooleanExpression(expression.right, evaluateValue);
      if (right === false) return false;
      return left === null || right === null ? null : true;
    }
    if (left === true) return true;
    const right = evaluateBooleanExpression(expression.right, evaluateValue);
    if (right === true) return true;
    return left === null || right === null ? null : false;
  }
  if (expression.kind === "not") {
    const value = evaluateBooleanExpression(expression.operand, evaluateValue);
    return value === null ? null : !value;
  }
  if (expression.kind === "exists") {
    throw new TypeError("EXISTS subqueries must be resolved before evaluation");
  }
  if (expression.kind === "condition") {
    const operator = expression.operator;
    if (operator === "IS TRUE") return evaluateBooleanExpression(expression.left, evaluateValue);
    if (operator === "IS NULL" || operator === "IS NOT NULL") {
      const value = evaluateValue(expression.left);
      const isNull = value === null || value === undefined;
      return operator === "IS NULL" ? isNull : !isNull;
    }
    if (operator === "IN" || operator === "NOT IN") {
      if (expression.right.kind !== "list") throw new TypeError("IN requires a value list");
      const probe = evaluateValue(expression.left);
      if (probe === null || probe === undefined) return null;
      let sawNull = false;
      for (const item of expression.right.items) {
        const value = evaluateValue(item);
        if (value === null || value === undefined) {
          sawNull = true;
          continue;
        }
        if (comparable(value) === comparable(probe)) return operator === "IN";
      }
      if (sawNull) return null;
      return operator === "NOT IN";
    }
    {
      const quantified = parseQuantified(operator);
      if (quantified !== undefined) {
        if (expression.right.kind !== "list") {
          throw new TypeError("ANY/ALL subqueries must be resolved before evaluation");
        }
        return quantifiedComparison(
          quantified.comparison,
          quantified.quantifier,
          evaluateValue(expression.left),
          expression.right.items.map((item) => evaluateValue(item)),
        );
      }
    }
    if (
      operator === "LIKE" ||
      operator === "NOT LIKE" ||
      operator === "ILIKE" ||
      operator === "NOT ILIKE"
    ) {
      const value = evaluateValue(expression.left);
      const pattern = evaluateValue(expression.right);
      if (value === null || value === undefined || pattern === null || pattern === undefined) {
        return null;
      }
      if (typeof value !== "string" || typeof pattern !== "string") {
        throw new TypeError("LIKE requires string operands");
      }
      const matched = likeMatches(
        pattern,
        value,
        operator === "ILIKE" || operator === "NOT ILIKE",
        expression.escape,
      );
      return operator === "LIKE" || operator === "ILIKE" ? matched : !matched;
    }
    if (operator === "IS DISTINCT FROM" || operator === "IS NOT DISTINCT FROM") {
      const distinct = distinctFromComparison(
        evaluateValue(expression.left),
        evaluateValue(expression.right),
      );
      return operator === "IS DISTINCT FROM" ? distinct : !distinct;
    }
    const left = evaluateValue(expression.left);
    const right = evaluateValue(expression.right);
    if (left === null || left === undefined || right === null || right === undefined) return null;
    const a = comparable(left);
    const b = comparable(right);
    if (operator === "=") return a === b;
    if (operator === "!=" || operator === "<>") return a !== b;
    const comparison = compareValues(a, b);
    if (operator === ">") return comparison > 0;
    if (operator === ">=") return comparison >= 0;
    if (operator === "<") return comparison < 0;
    return comparison <= 0;
  }
  const value = evaluateValue(expression);
  if (value === null || value === undefined) return null;
  if (typeof value === "boolean") return value;
  throw new TypeError("Boolean conditions require boolean operands");
}

function comparisonHolds(
  operator: PredicateOperator,
  leftValue: unknown,
  rightValue: unknown,
): boolean {
  if (operator === "IS NULL") return leftValue === null || leftValue === undefined;
  if (operator === "IS NOT NULL") return leftValue !== null && leftValue !== undefined;
  if (operator === "IN" || operator === "NOT IN") {
    throw new TypeError("IN is only supported in WHERE predicates");
  }
  if (operator === "IS DISTINCT FROM") return distinctFromComparison(leftValue, rightValue);
  if (operator === "IS NOT DISTINCT FROM") return !distinctFromComparison(leftValue, rightValue);
  if (
    operator === "LIKE" ||
    operator === "NOT LIKE" ||
    operator === "ILIKE" ||
    operator === "NOT ILIKE"
  ) {
    if (
      leftValue === null ||
      leftValue === undefined ||
      rightValue === null ||
      rightValue === undefined
    ) {
      return false;
    }
    if (typeof leftValue !== "string" || typeof rightValue !== "string") {
      throw new TypeError("LIKE requires string operands");
    }
    const matched = likeMatches(
      rightValue,
      leftValue,
      operator === "ILIKE" || operator === "NOT ILIKE",
    );
    return operator === "LIKE" || operator === "ILIKE" ? matched : !matched;
  }
  if (
    leftValue === null ||
    leftValue === undefined ||
    rightValue === null ||
    rightValue === undefined
  )
    return false;
  const left = comparable(leftValue);
  const right = comparable(rightValue);
  if (operator === "=") return left === right;
  if (operator === "!=" || operator === "<>") return left !== right;
  const comparison = compareValues(left, right);
  if (operator === ">") return comparison > 0;
  if (operator === ">=") return comparison >= 0;
  if (operator === "<") return comparison < 0;
  return comparison <= 0;
}

/**
 * Computes BM25 corpus statistics for every scoring node in the (already cloned) plan by one
 * pass over the base table's rows, and annotates them onto the nodes before evaluation.
 */
function annotateRowFtsStats(
  plan: CompiledQuery,
  tables: ReadonlyMap<string, DatabaseRow[]>,
): void {
  const annotate = (expression: Expression): void => {
    if (expression.kind !== "fts") {
      childExpressions(expression).forEach(annotate);
      return;
    }
    if (expression.op !== "bm25" || expression.stats !== undefined) return;
    if (plan.joins.length > 0) {
      throw new TypeError("BM25 requires a single-table query");
    }
    if (expression.columns === "*") {
      throw new TypeError("Full-text search columns must be expanded before evaluation");
    }
    const terms = cachedQueryTerms(expression.query);
    const accumulator = new FtsStatsAccumulator(terms);
    const columnNames = expression.columns.map((column) => {
      if (column.kind !== "column") {
        throw new TypeError("Full-text search takes column references");
      }
      return column.reference.split(".").at(-1) ?? column.reference;
    });
    for (const row of tables.get(plan.base.table) ?? []) {
      const values = columnNames.map((name) => row[name]);
      const tokens: string[] = [];
      for (const value of values) {
        const rendered = renderDocumentValue(value);
        if (rendered === undefined) continue;
        tokens.push(...ftsTokenize(rendered));
      }
      accumulator.addDocument(tokens);
    }
    expression.stats = accumulator.stats;
  };
  forEachBlockExpression(plan, annotate);
}

function resolveColumn(context: RowContext, reference: string): unknown {
  const parts = reference.split(".");
  if (parts.length === 2) {
    const alias = parts[0] ?? "";
    if (!(alias in context)) throw new TypeError(`Unknown table alias: ${alias}`);
    return context[alias]?.[parts[1] ?? ""];
  }
  const name = parts[0] ?? "";
  const matches = Object.values(context).filter((row) => row !== undefined && name in row);
  if (matches.length !== 1) throw new TypeError(`Ambiguous or missing column: ${reference}`);
  return matches[0]?.[name];
}

export function hasAggregate(expression: Expression): boolean {
  if (expression.kind === "call" && aggregateNames.has(expression.name as AggregateName)) {
    return true;
  }
  return childExpressions(expression).some(hasAggregate);
}

function validateGrouping(plan: CompiledQuery): void {
  const grouped =
    plan.groupBy.length > 0 || plan.select.some((item) => hasAggregate(item.expression));
  if (!grouped) return;
  const groupExpressions = new Set(plan.groupBy.map((expression) => JSON.stringify(expression)));
  for (const item of plan.select) {
    if (hasAggregate(item.expression)) continue;
    // A constant expression is the same for every group, as standard SQL allows. A full-text
    // node is never constant — MATCH(*) carries no column children before expansion, but it
    // reads every searchable column of its row.
    if (
      expressionColumns(item.expression).length === 0 &&
      !containsFtsExpression(item.expression)
    ) {
      continue;
    }
    if (!groupExpressions.has(JSON.stringify(item.expression))) {
      throw new TypeError(`Selected column must appear in GROUP BY: ${item.alias}`);
    }
  }
  const forbiddenAggregates = [
    ...plan.joins.flatMap((join) => [
      join.left,
      join.right,
      ...(join.on === undefined ? [] : [join.on]),
    ]),
    ...plan.predicates.flatMap((predicate) => [predicate.left, predicate.right]),
    ...plan.groupBy,
  ];
  if (forbiddenAggregates.some(hasAggregate))
    throw new TypeError("Aggregate functions are not allowed in JOIN, WHERE, or GROUP BY");
}

/** The column references inside one expression; a subquery contributes none (its own scope). */
export function expressionColumnNames(expression: Expression): string[] {
  return expressionColumns(expression);
}

function expressionColumns(expression: Expression): string[] {
  if (expression.kind === "column") return [expression.reference];
  // A subquery or EXISTS resolves inside its own scope before the enclosing block binds.
  return childExpressions(expression).flatMap(expressionColumns);
}

export function expressionAliases(expression: Expression): Set<string> {
  return new Set(
    expressionColumns(expression).flatMap((reference) =>
      reference.includes(".") ? [reference.split(".")[0] ?? ""] : [],
    ),
  );
}

function comparable(value: unknown): unknown {
  return value instanceof Date ? value.getTime() : value;
}

/**
 * Resolves an explicit NULLS FIRST/LAST placement for one order term. Returns undefined when
 * no explicit placement applies (either none was requested or neither side is NULL); otherwise
 * the signed placement, which is absolute — direction negation must not apply to it. Two NULLs
 * return 0 so the comparison falls through to the next term.
 */
export function explicitNullOrder(
  left: unknown,
  right: unknown,
  nulls: "first" | "last" | undefined,
): number | undefined {
  if (nulls === undefined) return undefined;
  const leftNull = left === null || left === undefined;
  const rightNull = right === null || right === undefined;
  if (!leftNull && !rightNull) return undefined;
  if (leftNull && rightNull) return 0;
  return (leftNull ? -1 : 1) * (nulls === "first" ? 1 : -1);
}

function numeric(value: unknown): number {
  if (typeof value !== "number")
    throw new TypeError("Arithmetic and numeric aggregates require numbers");
  return value;
}

function asQueryValue(value: unknown): QueryValue {
  if (
    value === null ||
    typeof value === "boolean" ||
    typeof value === "number" ||
    typeof value === "string" ||
    value instanceof Date
  )
    return value;
  if (value === undefined) return null;
  throw new TypeError("Query produced an unsupported value");
}

class Parser {
  /** The statement text the tokens came from, for the clauses stored verbatim (CHECK bodies). */
  readonly text: string;
  #index = 0;
  #derivedSequence = 0;
  /** Placeholders seen so far: positional `?` count, and the highest `$n` number. */
  #positionalParameters = 0;
  #highestNumberedParameter = 0;

  /** Set when the statement names CURRENT_DATE, CURRENT_TIMESTAMP, or LOCALTIME. */
  usesStatementDatetime = false;

  /** Total parameter slots the statement expects; 0 when it has no placeholders. */
  get parameterCount(): number {
    return Math.max(this.#positionalParameters, this.#highestNumberedParameter);
  }
  /** Shared with the assembly helpers so parser plans and builder plans number sources alike. */
  readonly nextDerivedSequence = (): number => {
    this.#derivedSequence += 1;
    return this.#derivedSequence;
  };
  readonly #ctes = new Map<string, CompiledQuery>();
  /** Named windows of the block being parsed: `WINDOW w AS (...)`, referenced by `OVER w`. */
  #namedWindows = new Map<string, { start: number; end: number }>();
  readonly #ctesInProgress = new Set<string>();
  readonly #recursiveCtes = new Map<string, RecursiveCte>();
  readonly #recursiveUses = new Set<string>();
  #recursiveCandidate: { name: string; reference: string } | undefined;

  constructor(
    private readonly tokens: Token[],
    text = "",
  ) {
    this.text = text;
  }

  #withClause(): void {
    if (!this.#isKeyword("WITH")) return;
    this.#keyword("WITH");
    let recursive = false;
    if (this.#isKeyword("RECURSIVE")) {
      this.#keyword("RECURSIVE");
      recursive = true;
    }
    for (;;) {
      const name = this.#identifier();
      if (this.#ctes.has(name) || this.#ctesInProgress.has(name) || this.#recursiveCtes.has(name)) {
        throw new TypeError(`Duplicate CTE name: ${name}`);
      }
      // `WITH months(month) AS (…)`: the CTE names its own output columns, whatever the block
      // inside called them. A recursive CTE takes the names before its step member parses,
      // because the step refers to the working set by these names and not by the base's.
      const columns = this.#cteColumnList();
      this.#keyword("AS");
      this.#expectPunctuation("(");
      if (recursive) {
        this.#recursiveCte(name, columns);
      } else {
        this.#ctesInProgress.add(name);
        const block = this.#queryExpression("(cte)");
        this.#ctesInProgress.delete(name);
        this.#expectPunctuation(")");
        if (columns !== undefined) renameBlockOutputs(block, columns, name);
        this.#ctes.set(name, block);
      }
      if (!this.#punctuation(",")) break;
    }
  }

  /** The optional `(a, b, c)` between a CTE's name and its AS. */
  #cteColumnList(): string[] | undefined {
    if (!this.#punctuation("(")) return undefined;
    const columns: string[] = [];
    for (;;) {
      columns.push(this.#identifier());
      if (!this.#punctuation(",")) break;
    }
    this.#expectPunctuation(")");
    if (columns.length === 0) throw new TypeError("A CTE column list cannot be empty");
    return columns;
  }

  /**
   * A statement opening with WITH: the CTEs parse first, and a following INSERT/UPDATE/DELETE
   * becomes a mutation whose subqueries see them. Returns undefined for a plain WITH ... SELECT,
   * which the caller routes through the query pipeline.
   */
  parseStatementWithCtes(): CompiledStatement | undefined {
    this.#withClause();
    const token = this.#peek();
    const keyword =
      token.kind === "identifier" && token.quoted !== true ? token.text.toUpperCase() : "";
    if (keyword === "INSERT" || keyword === "UPDATE" || keyword === "DELETE") {
      return this.parseMutation(keyword);
    }
    return undefined;
  }

  parse(sql: string): CompiledQuery {
    this.#withClause();
    const plan = this.#queryExpression(sql);
    this.#take("eof");
    return plan;
  }

  /**
   * A whole query expression: INTERSECT terms combined by UNION and EXCEPT. Every context that
   * takes one — the statement itself, a derived table, a CTE body, a parenthesized member —
   * parses through here, so set operations nest wherever the standard allows (E071-06).
   */
  #queryExpression(sql: string): CompiledQuery {
    if (this.#isKeyword("WITH")) {
      // T122: a nested WITH belongs to its own query expression only, so the outer names are
      // restored afterwards and an inner CTE cannot leak out of the subquery that declared it.
      const outer = new Map(this.#ctes);
      try {
        this.#withClause();
        return this.#queryExpression(sql);
      } finally {
        this.#ctes.clear();
        for (const [name, block] of outer) this.#ctes.set(name, block);
      }
    }
    // INTERSECT binds tighter than UNION and EXCEPT, per the SQL standard.
    const firstTerm = this.#setTerm(sql);
    let plan = firstTerm.block;
    if (this.#isKeyword("UNION") || this.#isKeyword("EXCEPT")) {
      const members = [firstTerm];
      const ops: SetOperator[] = [];
      while (this.#isKeyword("UNION") || this.#isKeyword("EXCEPT")) {
        if (this.#isKeyword("UNION")) {
          this.#keyword("UNION");
          if (this.#isKeyword("ALL")) {
            this.#keyword("ALL");
            ops.push("union all");
          } else ops.push("union");
        } else {
          this.#keyword("EXCEPT");
          if (this.#isKeyword("ALL")) {
            this.#keyword("ALL");
            ops.push("except all");
          } else ops.push("except");
        }
        members.push(this.#setTerm("(union member)"));
      }
      plan = this.#compoundBlock(sql, members, ops);
    }
    return plan;
  }

  #setTerm(sql: string): { block: CompiledQuery; parenthesized: boolean } {
    const first = this.#unionMember(sql);
    if (!this.#isKeyword("INTERSECT")) return first;
    const members = [first];
    const ops: SetOperator[] = [];
    while (this.#isKeyword("INTERSECT")) {
      this.#keyword("INTERSECT");
      if (this.#isKeyword("ALL")) {
        this.#keyword("ALL");
        ops.push("intersect all");
      } else ops.push("intersect");
      members.push(this.#unionMember("(intersect member)"));
    }
    return { block: this.#compoundBlock(sql, members, ops), parenthesized: false };
  }

  #compoundBlock(
    sql: string,
    members: ReadonlyArray<{ block: CompiledQuery; parenthesized: boolean }>,
    ops: SetOperator[],
  ): CompiledQuery {
    for (const [index, member] of members.entries()) {
      const last = index === members.length - 1;
      if (
        !member.parenthesized &&
        !last &&
        (member.block.orderBy.length > 0 || member.block.limit !== undefined)
      ) {
        throw new TypeError("ORDER BY or LIMIT in a UNION member requires parentheses");
      }
    }
    // Standard SQL assigns a trailing ORDER BY or LIMIT to the whole compound. After an
    // unparenthesized last member the clause was greedily parsed into that member and lifts
    // out; after a parenthesized member it is still unparsed.
    const last = members[members.length - 1];
    let tail: SelectTail;
    if (last !== undefined && !last.parenthesized) {
      tail = {
        orderBy: last.block.orderBy,
        ...(last.block.limit === undefined ? {} : { limit: last.block.limit }),
        ...(last.block.offset === undefined ? {} : { offset: last.block.offset }),
        ...(last.block.limitWithTies === true ? { limitWithTies: true } : {}),
        ...(last.block.limitParameter === undefined
          ? {}
          : { limitParameter: last.block.limitParameter }),
        ...(last.block.offsetParameter === undefined
          ? {}
          : { offsetParameter: last.block.offsetParameter }),
      };
      last.block.orderBy = [];
      delete last.block.limit;
      delete last.block.offset;
      delete last.block.limitParameter;
      delete last.block.offsetParameter;
    } else {
      tail = { orderBy: this.#orderByClause(), ...this.#tailClauses() };
    }
    return compoundSelectBlock(
      sql,
      members.map((member) => member.block),
      ops,
      tail,
      this.nextDerivedSequence,
    );
  }

  /**
   * CREATE TABLE name (col TYPE [PRIMARY KEY | UNIQUE] [NOT NULL | NULL], ...). Standard type
   * names map onto the engine's four logical types; widths in parentheses parse and are
   * ignored, because numeric widths and encodings are the engine's job, not schema choices.
   */
  parseCreateTable(): CompiledStatement {
    this.#keyword("CREATE");
    this.#keyword("TABLE");
    let ifNotExists = false;
    if (this.#isKeyword("IF")) {
      this.#keyword("IF");
      this.#keyword("NOT");
      this.#keyword("EXISTS");
      ifNotExists = true;
    }
    const table = this.#identifier();
    if (this.#isKeyword("AS")) {
      // T172: CREATE TABLE name AS SELECT ... takes its columns from the query's own schema.
      this.#keyword("AS");
      const query = this.#queryExpression("(create table as)");
      this.#take("eof");
      return {
        kind: "create-table-as",
        table,
        query,
        ...(ifNotExists ? { ifNotExists: true } : {}),
      };
    }
    this.#expectPunctuation("(");
    const columns: Array<{
      name: string;
      type: SqlColumnType;
      nullable?: boolean;
      defaultValue?: ColumnDefault;
    }> = [];
    const checks: Array<{ name: string; sql: string }> = [];
    const foreignKeys: ForeignKeyDefinition[] = [];
    let uniqueKey: string | undefined;
    for (;;) {
      if (this.#isKeyword("CONSTRAINT") || this.#isKeyword("CHECK") || this.#isKeyword("FOREIGN")) {
        // A table-level constraint, named or not.
        let constraintName: string | undefined;
        if (this.#isKeyword("CONSTRAINT")) {
          this.#keyword("CONSTRAINT");
          constraintName = this.#identifier();
        }
        if (this.#isKeyword("FOREIGN")) {
          this.#keyword("FOREIGN");
          this.#keyword("KEY");
          this.#expectPunctuation("(");
          const column = this.#identifier();
          if (this.#peek().text === ",") {
            throw new TypeError("FOREIGN KEY supports one column, the parent's unique key");
          }
          this.#expectPunctuation(")");
          foreignKeys.push(this.#references(constraintName ?? `${table}_${column}_fkey`, column));
          if (!this.#punctuation(",")) break;
          continue;
        }
        if (!this.#isKeyword("CHECK")) {
          throw new TypeError("Table constraints are CHECK and FOREIGN KEY");
        }
        checks.push(
          this.#checkConstraint(constraintName ?? `${table}_check_${String(checks.length + 1)}`),
        );
        if (!this.#punctuation(",")) break;
        continue;
      }
      if (this.#isKeyword("PRIMARY") || this.#isKeyword("UNIQUE")) {
        // E141-08: the table-level spelling of the same single-column key.
        if (this.#isKeyword("PRIMARY")) {
          this.#keyword("PRIMARY");
          this.#keyword("KEY");
        } else this.#keyword("UNIQUE");
        this.#expectPunctuation("(");
        const keyColumn = this.#identifier();
        if (this.#peek().text === ",") {
          throw new TypeError("CREATE TABLE supports one unique key column");
        }
        this.#expectPunctuation(")");
        if (uniqueKey !== undefined && uniqueKey !== keyColumn) {
          throw new TypeError("CREATE TABLE supports one unique key column");
        }
        uniqueKey = keyColumn;
        if (!this.#punctuation(",")) break;
        continue;
      }
      const name = this.#identifier();
      const type = this.#columnType();
      let nullable = true;
      let explicitlyNullable = false;
      let defaultValue: ColumnDefault | undefined;
      for (;;) {
        if (this.#isKeyword("DEFAULT")) {
          // E141-07. The catalog fills defaults at insert time, so they are constants: a
          // literal, or the CURRENT_TIMESTAMP family, which it stores as "now".
          this.#keyword("DEFAULT");
          const expression = this.#expression();
          defaultValue = columnDefaultFor(expression);
          continue;
        }
        if (this.#isKeyword("CHECK")) {
          checks.push(this.#checkConstraint(`${table}_${name}_check`));
          continue;
        }
        if (this.#isKeyword("REFERENCES")) {
          foreignKeys.push(this.#references(`${table}_${name}_fkey`, name));
          continue;
        }
        if (this.#isKeyword("PRIMARY") || this.#isKeyword("UNIQUE")) {
          if (this.#isKeyword("PRIMARY")) {
            this.#keyword("PRIMARY");
            this.#keyword("KEY");
          } else {
            this.#keyword("UNIQUE");
          }
          if (uniqueKey !== undefined) {
            throw new TypeError("CREATE TABLE supports one unique key column");
          }
          uniqueKey = name;
          nullable = false;
          continue;
        }
        if (this.#isKeyword("NOT")) {
          this.#keyword("NOT");
          const token = this.#peek();
          if (token.kind !== "identifier" || token.text.toUpperCase() !== "NULL") {
            throw new TypeError(`Expected NULL after NOT, found ${token.text || "end of input"}`);
          }
          this.#identifier();
          nullable = false;
          continue;
        }
        if (this.#isKeyword("NULL")) {
          this.#keyword("NULL");
          nullable = true;
          explicitlyNullable = true;
          continue;
        }
        break;
      }
      // A default answers what an absent value means, so the column cannot also be nullable
      // unless the author says so — and then the engine rejects the pair, since NULL and the
      // default would both claim the same slot.
      if (defaultValue !== undefined && !explicitlyNullable) nullable = false;
      columns.push({
        name,
        type,
        ...(nullable ? { nullable: true } : {}),
        ...(defaultValue === undefined ? {} : { defaultValue }),
      });
      if (!this.#punctuation(",")) break;
    }
    this.#expectPunctuation(")");
    this.#take("eof");
    if (new Set(columns.map((column) => column.name)).size !== columns.length) {
      throw new TypeError("CREATE TABLE column names must be unique");
    }
    if (uniqueKey !== undefined && !columns.some((column) => column.name === uniqueKey)) {
      throw new TypeError(`CREATE TABLE key column is not declared: ${uniqueKey}`);
    }
    const declared = new Set(columns.map((column) => column.name));
    for (const check of checks) {
      for (const reference of expressionColumnNames(
        compileCheckExpression(check.sql, check.name),
      )) {
        const column = reference.split(".").at(-1) ?? reference;
        if (!declared.has(column)) {
          throw new TypeError(
            `CHECK ${check.name} refers to a column this table has no: ${column}`,
          );
        }
      }
    }
    return {
      kind: "create-table",
      table,
      columns: columns.map((column) =>
        column.name === uniqueKey ? { ...column, nullable: false } : column,
      ),
      ...(uniqueKey === undefined ? {} : { uniqueKey }),
      ...(checks.length === 0 ? {} : { checks }),
      ...(foreignKeys.length === 0 ? {} : { foreignKeys }),
      ...(ifNotExists ? { ifNotExists: true } : {}),
    };
  }

  /**
   * `REFERENCES parent(column) [ON DELETE action] [ON UPDATE action]` (E141-04, T191). A parent
   * key cannot change in this engine, so ON UPDATE has nothing to act on and only the
   * no-op actions parse; ON DELETE takes the three the engine can carry out.
   */
  #references(name: string, column: string): ForeignKeyDefinition {
    this.#keyword("REFERENCES");
    const parentTable = this.#identifier();
    let parentColumn: string | undefined;
    if (this.#punctuation("(")) {
      parentColumn = this.#identifier();
      this.#expectPunctuation(")");
    }
    let onDelete: ForeignKeyDefinition["onDelete"] = "restrict";
    while (this.#isKeyword("ON")) {
      this.#keyword("ON");
      const event = this.#isKeyword("DELETE") ? "delete" : "update";
      this.#keyword(event === "delete" ? "DELETE" : "UPDATE");
      const action = this.#referentialAction();
      if (event === "update") {
        if (action !== "restrict") {
          throw new TypeError(
            `ON UPDATE ${action.toUpperCase()} has nothing to act on: a unique key cannot change`,
          );
        }
        continue;
      }
      onDelete = action;
    }
    return {
      name,
      column,
      parentTable,
      ...(parentColumn === undefined ? {} : { parentColumn }),
      onDelete,
    };
  }

  #referentialAction(): ForeignKeyDefinition["onDelete"] {
    if (this.#isKeyword("CASCADE")) {
      this.#keyword("CASCADE");
      return "cascade";
    }
    if (this.#isKeyword("SET")) {
      this.#keyword("SET");
      if (this.#isKeyword("DEFAULT")) {
        throw new TypeError("SET DEFAULT is not supported; use SET NULL or CASCADE");
      }
      this.#keyword("NULL");
      return "set null";
    }
    if (this.#isKeyword("NO")) {
      this.#keyword("NO");
      this.#keyword("ACTION");
      return "restrict";
    }
    this.#keyword("RESTRICT");
    return "restrict";
  }

  /**
   * `CHECK (expression)`: the text is kept as written so the catalog stores what the author
   * declared, and compiled once here to reject a constraint the engine could never evaluate.
   */
  #checkConstraint(fallbackName: string): { name: string; sql: string } {
    this.#keyword("CHECK");
    const open = this.#peek();
    this.#expectPunctuation("(");
    const expression = this.#expression();
    const close = this.#peek();
    this.#expectPunctuation(")");
    if (hasAggregate(expression) || containsWindow(expression)) {
      throw new TypeError(`CHECK ${fallbackName} takes a row condition, not an aggregate`);
    }
    if (containsParameter(expression)) {
      throw new TypeError(`CHECK ${fallbackName} cannot take a parameter`);
    }
    return { name: fallbackName, sql: this.text.slice(open.start + 1, close.start).trim() };
  }

  /** Parses one whole expression and nothing else — the form a CHECK constraint stores. */
  parseExpression(): Expression {
    const expression = this.#expression();
    this.#take("eof");
    return expression;
  }

  /** DROP VIEW [IF EXISTS] name (F031-16). */
  parseDropView(): CompiledStatement {
    this.#keyword("DROP");
    this.#keyword("VIEW");
    let ifExists = false;
    if (this.#isKeyword("IF")) {
      this.#keyword("IF");
      this.#keyword("EXISTS");
      ifExists = true;
    }
    const view = this.#identifier();
    if (this.#isKeyword("RESTRICT")) this.#keyword("RESTRICT");
    this.#take("eof");
    return { kind: "drop-view", view, ...(ifExists ? { ifExists: true } : {}) };
  }

  /** DROP TABLE [IF EXISTS] name (F031-13). */
  parseDropTable(): CompiledStatement {
    this.#keyword("DROP");
    this.#keyword("TABLE");
    let ifExists = false;
    if (this.#isKeyword("IF")) {
      this.#keyword("IF");
      this.#keyword("EXISTS");
      ifExists = true;
    }
    const table = this.#identifier();
    // RESTRICT is the standard's default and the only behaviour here: nothing cascades, because
    // the engine has no dependent objects a drop could reach.
    if (this.#isKeyword("RESTRICT")) this.#keyword("RESTRICT");
    else if (this.#isKeyword("CASCADE")) {
      throw new TypeError("DROP TABLE CASCADE is not supported; drop dependents explicitly");
    }
    this.#take("eof");
    return { kind: "drop-table", table, ...(ifExists ? { ifExists: true } : {}) };
  }

  /** ALTER TABLE name ADD [COLUMN] col TYPE [NOT NULL] [DEFAULT v] (F031-04). */
  parseAlterTable(): CompiledStatement {
    this.#keyword("ALTER");
    this.#keyword("TABLE");
    const table = this.#identifier();
    this.#keyword("ADD");
    if (this.#isKeyword("COLUMN")) this.#keyword("COLUMN");
    const name = this.#identifier();
    const type = this.#columnType();
    let nullable = true;
    let defaultValue: ColumnDefault | undefined;
    for (;;) {
      if (this.#isKeyword("DEFAULT")) {
        this.#keyword("DEFAULT");
        defaultValue = columnDefaultFor(this.#expression());
        continue;
      }
      if (this.#isKeyword("NOT")) {
        this.#keyword("NOT");
        this.#keyword("NULL");
        nullable = false;
        continue;
      }
      if (this.#isKeyword("NULL")) {
        this.#keyword("NULL");
        nullable = true;
        continue;
      }
      break;
    }
    this.#take("eof");
    return {
      kind: "add-column",
      table,
      column: {
        name,
        type,
        ...(nullable ? { nullable: true } : {}),
        ...(defaultValue === undefined ? {} : { defaultValue }),
      },
    };
  }

  /** A CAST target: the SqlColumnType, or "number-integer" for the truncating integer names. */
  #castTarget(): string {
    const word = this.#identifier().toUpperCase();
    if (word === "DOUBLE") {
      this.#keyword("PRECISION");
      return "number";
    }
    const mapped = createTableTypeNames.get(word);
    if (mapped === undefined) throw new TypeError(`Unsupported CAST target: ${word}`);
    if (this.#punctuation("(")) {
      this.#take("number");
      if (this.#punctuation(",")) this.#take("number");
      this.#expectPunctuation(")");
    }
    const integer =
      word === "INTEGER" || word === "INT" || word === "BIGINT" || word === "SMALLINT";
    return integer ? "number-integer" : mapped;
  }

  #columnType(): SqlColumnType {
    const word = this.#identifier().toUpperCase();
    if (word === "DOUBLE") {
      this.#keyword("PRECISION");
      return "number";
    }
    const mapped = createTableTypeNames.get(word);
    if (mapped === undefined) throw new TypeError(`Unsupported column type: ${word}`);
    // A width like VARCHAR(80) or NUMERIC(10, 2) parses and is discarded.
    if (this.#punctuation("(")) {
      this.#take("number");
      if (this.#punctuation(",")) this.#take("number");
      this.#expectPunctuation(")");
    }
    return mapped;
  }

  parseMutation(keyword: "INSERT" | "UPDATE" | "DELETE"): CompiledStatement {
    const statement =
      keyword === "INSERT"
        ? this.#insertStatement()
        : keyword === "UPDATE"
          ? this.#updateStatement()
          : this.#deleteStatement();
    this.#take("eof");
    return statement;
  }

  #insertStatement(): CompiledStatement {
    this.#keyword("INSERT");
    this.#keyword("INTO");
    const table = this.#identifier();
    this.#expectPunctuation("(");
    const columns: string[] = [];
    for (;;) {
      columns.push(this.#identifier());
      if (!this.#punctuation(",")) break;
    }
    this.#expectPunctuation(")");
    if (new Set(columns).size !== columns.length) {
      throw new TypeError("INSERT columns must be unique");
    }
    if (this.#isKeyword("SELECT")) {
      const query = optimizePlan(this.#selectBlock("(insert select)"));
      if (query.select.some((item) => item.expression.kind === "wildcard")) {
        throw new TypeError("INSERT ... SELECT requires an explicit select list");
      }
      if (query.select.length !== columns.length) {
        throw new TypeError("INSERT ... SELECT must produce exactly the insert column count");
      }
      return {
        kind: "insert",
        table,
        columns,
        rows: [],
        query,
        ...this.#returningClause(),
      };
    }
    this.#keyword("VALUES");
    const rows: InsertValue[][] = [];
    for (;;) {
      this.#expectPunctuation("(");
      const values: InsertValue[] = [];
      for (;;) {
        values.push(this.#insertValue("INSERT values"));
        if (!this.#punctuation(",")) break;
      }
      this.#expectPunctuation(")");
      if (values.length !== columns.length) {
        throw new TypeError("Each INSERT row must match the column list length");
      }
      rows.push(values);
      if (!this.#punctuation(",")) break;
    }
    return {
      kind: "insert",
      table,
      columns,
      rows,
      ...this.#onConflictClause(columns),
      ...this.#returningClause(),
    };
  }

  #onConflictClause(columns: readonly string[]): {
    onConflict?: {
      column: string;
      action: "nothing" | "replace" | "update";
      columns?: string[];
    };
  } {
    if (!this.#isKeyword("ON")) return {};
    this.#keyword("ON");
    this.#keyword("CONFLICT");
    this.#expectPunctuation("(");
    const column = this.#identifier();
    this.#expectPunctuation(")");
    this.#keyword("DO");
    if (this.#isKeyword("NOTHING")) {
      this.#keyword("NOTHING");
      return { onConflict: { column, action: "nothing" } };
    }
    this.#keyword("UPDATE");
    this.#keyword("SET");
    const assigned = new Set<string>();
    for (;;) {
      const target = this.#identifier();
      this.#operator("=");
      const value = this.#expression();
      const parts = value.kind === "column" ? value.reference.split(".") : [];
      if (parts.length !== 2 || parts[0]?.toUpperCase() !== "EXCLUDED" || parts[1] !== target) {
        throw new TypeError(
          "ON CONFLICT DO UPDATE supports assignments of the form column = EXCLUDED.column",
        );
      }
      if (assigned.has(target)) {
        throw new TypeError(`ON CONFLICT DO UPDATE sets a column twice: ${target}`);
      }
      if (target === column) {
        throw new TypeError("ON CONFLICT DO UPDATE cannot reassign the conflict key");
      }
      assigned.add(target);
      if (!this.#punctuation(",")) break;
    }
    for (const name of assigned) {
      if (!columns.includes(name)) {
        throw new TypeError(`ON CONFLICT DO UPDATE sets a column that is not inserted: ${name}`);
      }
    }
    // Every inserted column assigned is a whole-row upsert; a subset merges into existing rows.
    const complete = columns.every((name) => name === column || assigned.has(name));
    return {
      onConflict: complete
        ? { column, action: "replace" }
        : { column, action: "update", columns: [...assigned] },
    };
  }

  /** RETURNING * or RETURNING col, ... — the engine's runStatement implements the semantics. */
  #returningClause(): { returning?: string[] | "*" } {
    if (!this.#isKeyword("RETURNING")) return {};
    this.#keyword("RETURNING");
    if (this.#peek().text === "*") {
      this.#index += 1;
      return { returning: "*" };
    }
    const columns: string[] = [];
    for (;;) {
      columns.push(this.#identifier());
      if (!this.#punctuation(",")) break;
    }
    return { returning: columns };
  }

  /**
   * MERGE INTO target USING source ON condition WHEN [NOT] MATCHED [AND …] THEN … (F312).
   * The source may be a table, a view, or a query; each WHEN clause carries its own optional
   * condition, and the branches are kept in the order written, which is the order they are tried.
   */
  parseMerge(): CompiledStatement {
    this.#keyword("MERGE");
    this.#keyword("INTO");
    const table = this.#identifier();
    const alias = this.#sourceAlias() ?? table;
    this.#keyword("USING");
    let source: { alias: string; table?: string; sql?: string };
    if (this.#peek().text === "(") {
      const open = this.#peek();
      this.#expectPunctuation("(");
      const before = this.parameterCount;
      this.#queryExpression("(merge source)");
      if (this.parameterCount !== before) {
        throw new TypeError(
          "A MERGE source query cannot take parameters; bind them in the branches",
        );
      }
      const close = this.#peek();
      this.#expectPunctuation(")");
      const sourceAlias = this.#sourceAlias();
      if (sourceAlias === undefined) throw new TypeError("A MERGE source query requires an alias");
      source = { alias: sourceAlias, sql: this.text.slice(open.start + 1, close.start).trim() };
    } else {
      const name = this.#identifier();
      source = { alias: this.#sourceAlias() ?? name, table: name };
    }
    if (source.alias === alias) throw new TypeError("MERGE source and target need distinct names");
    this.#keyword("ON");
    const on = this.#expression();
    const branches: MergeBranch[] = [];
    while (this.#isKeyword("WHEN")) {
      this.#keyword("WHEN");
      let matched = true;
      if (this.#isKeyword("NOT")) {
        this.#keyword("NOT");
        matched = false;
      }
      this.#keyword("MATCHED");
      if (this.#isKeyword("BY")) {
        // WHEN NOT MATCHED BY SOURCE/TARGET walks the target rather than the source, which this
        // implementation does not do.
        throw new TypeError("MERGE ... MATCHED BY SOURCE and BY TARGET are not supported");
      }
      let condition: Expression | undefined;
      if (this.#isKeyword("AND")) {
        this.#keyword("AND");
        condition = this.#expression();
      }
      this.#keyword("THEN");
      if (matched) {
        if (this.#isKeyword("DELETE")) {
          this.#keyword("DELETE");
          branches.push({
            when: "matched",
            ...(condition === undefined ? {} : { condition }),
            action: { kind: "delete" },
          });
          continue;
        }
        this.#keyword("UPDATE");
        this.#keyword("SET");
        const assignments: Array<{ column: string; expression: Expression }> = [];
        for (;;) {
          const column = this.#identifier();
          this.#operator("=");
          const expression = this.#expression();
          if (hasAggregate(expression)) {
            throw new TypeError("Aggregate functions are not allowed in MERGE assignments");
          }
          assignments.push({ column, expression });
          if (!this.#punctuation(",")) break;
        }
        if (new Set(assignments.map((entry) => entry.column)).size !== assignments.length) {
          throw new TypeError("MERGE assignments must set each column once");
        }
        branches.push({
          when: "matched",
          ...(condition === undefined ? {} : { condition }),
          action: { kind: "update", assignments },
        });
        continue;
      }
      this.#keyword("INSERT");
      const columns: string[] = [];
      if (this.#punctuation("(")) {
        for (;;) {
          columns.push(this.#identifier());
          if (!this.#punctuation(",")) break;
        }
        this.#expectPunctuation(")");
      }
      this.#keyword("VALUES");
      this.#expectPunctuation("(");
      const values: Expression[] = [];
      for (;;) {
        values.push(this.#expression());
        if (!this.#punctuation(",")) break;
      }
      this.#expectPunctuation(")");
      if (columns.length > 0 && columns.length !== values.length) {
        throw new TypeError("MERGE INSERT values must match its column list");
      }
      branches.push({
        when: "not-matched",
        ...(condition === undefined ? {} : { condition }),
        action: { kind: "insert", columns, values },
      });
    }
    if (branches.length === 0) throw new TypeError("MERGE needs at least one WHEN clause");
    if (this.#isKeyword("RETURNING")) {
      throw new TypeError("MERGE does not support RETURNING; read the rows back with a SELECT");
    }
    this.#take("eof");
    return { kind: "merge", table, alias, source, on, branches };
  }

  #updateStatement(): CompiledStatement {
    this.#keyword("UPDATE");
    const table = this.#identifier();
    this.#keyword("SET");
    const assignments: Array<{ column: string; expression: Expression }> = [];
    for (;;) {
      const column = this.#identifier();
      this.#operator("=");
      const expression = this.#expression();
      if (hasAggregate(expression)) {
        throw new TypeError("Aggregate functions are not allowed in UPDATE assignments");
      }
      assignments.push({ column, expression });
      if (!this.#punctuation(",")) break;
    }
    if (new Set(assignments.map(({ column }) => column)).size !== assignments.length) {
      throw new TypeError("UPDATE assignments must set each column once");
    }
    const predicates = this.#mutationPredicates();
    return { kind: "update", table, assignments, predicates, ...this.#returningClause() };
  }

  #deleteStatement(): CompiledStatement {
    this.#keyword("DELETE");
    this.#keyword("FROM");
    const table = this.#identifier();
    const predicates = this.#mutationPredicates();
    return { kind: "delete", table, predicates, ...this.#returningClause() };
  }

  #mutationPredicates(): Array<{
    left: Expression;
    operator: PredicateOperator;
    right: Expression;
  }> {
    const predicates: Array<{
      left: Expression;
      operator: PredicateOperator;
      right: Expression;
    }> = [];
    if (this.#isKeyword("WHERE")) {
      this.#keyword("WHERE");
      for (const predicate of splitCondition(this.#expression())) {
        if (hasAggregate(predicate.left) || hasAggregate(predicate.right)) {
          throw new TypeError("Aggregate functions are not allowed in mutation predicates");
        }
        predicates.push(predicate);
      }
    }
    return predicates;
  }

  #insertValue(label: string): InsertValue {
    const expression = this.#expression();
    // A bare placeholder stays a slot; the batch write binds it later. Placeholders nested in
    // arithmetic would need expression retention through the batch path, so they stay rejected.
    if (expression.kind === "parameter") return { parameter: expression.index };
    if (containsParameter(expression)) {
      throw new TypeError(`${label} take a bare ? placeholder or a constant expression`);
    }
    if (hasAggregate(expression) || expressionColumns(expression).length > 0) {
      throw new TypeError(`${label} must be constant expressions`);
    }
    return asQueryValue(evaluate(expression, {}));
  }

  #unionMember(sql: string): { block: CompiledQuery; parenthesized: boolean } {
    if (this.#punctuation("(")) {
      const block = this.#isKeyword("VALUES") ? this.#valuesBlock() : this.#queryExpression(sql);
      this.#expectPunctuation(")");
      return { block, parenthesized: true };
    }
    if (this.#isKeyword("VALUES")) {
      return { block: this.#valuesBlock(), parenthesized: false };
    }
    return { block: this.#selectBlock(sql), parenthesized: false };
  }

  #orderByClause(): CompiledQuery["orderBy"] {
    const orderBy: CompiledQuery["orderBy"] = [];
    if (this.#isKeyword("ORDER")) {
      this.#keyword("ORDER");
      this.#keyword("BY");
      for (;;) {
        const expression = this.#expression();
        let direction: "asc" | "desc" = "asc";
        if (this.#isKeyword("DESC")) {
          this.#keyword("DESC");
          direction = "desc";
        } else if (this.#isKeyword("ASC")) this.#keyword("ASC");
        let nulls: "first" | "last" | undefined;
        if (this.#isKeyword("NULLS")) {
          this.#keyword("NULLS");
          if (this.#isKeyword("FIRST")) {
            this.#keyword("FIRST");
            nulls = "first";
          } else {
            this.#keyword("LAST");
            nulls = "last";
          }
        }
        orderBy.push({ expression, direction, ...(nulls === undefined ? {} : { nulls }) });
        if (!this.#punctuation(",")) break;
      }
    }
    return orderBy;
  }

  #limitClause(): { limit?: number; limitParameter?: number } {
    if (this.#isKeyword("LIMIT")) {
      this.#keyword("LIMIT");
      if (this.#peek().kind === "parameter") {
        const parameter = this.#parameterExpression();
        return parameter.kind === "parameter" ? { limitParameter: parameter.index } : {};
      }
      return { limit: validateLimit(Number(this.#take("number").text)) };
    }
    return {};
  }

  /** The standard fetch clause: FETCH FIRST|NEXT [n] ROW|ROWS ONLY, a spelling of LIMIT. */
  #fetchClause(): { limit?: number; limitParameter?: number; limitWithTies?: boolean } {
    if (!this.#isKeyword("FETCH")) return {};
    this.#keyword("FETCH");
    if (this.#isKeyword("FIRST")) this.#keyword("FIRST");
    else this.#keyword("NEXT");
    let result: { limit?: number; limitParameter?: number; limitWithTies?: boolean } = { limit: 1 };
    if (this.#peek().kind === "parameter") {
      const parameter = this.#parameterExpression();
      if (parameter.kind === "parameter") result = { limitParameter: parameter.index };
    } else if (this.#peek().kind === "number") {
      result = { limit: validateLimit(Number(this.#take("number").text)) };
    }
    if (this.#isKeyword("ROWS")) this.#keyword("ROWS");
    else this.#keyword("ROW");
    // F866: WITH TIES keeps every row that ties with the last one under the ORDER BY.
    if (this.#isKeyword("WITH")) {
      this.#keyword("WITH");
      this.#keyword("TIES");
      return { ...result, limitWithTies: true };
    }
    this.#keyword("ONLY");
    return result;
  }

  #offsetClause(): { offset?: number; offsetParameter?: number } {
    if (!this.#isKeyword("OFFSET")) return {};
    this.#keyword("OFFSET");
    let result: { offset?: number; offsetParameter?: number };
    if (this.#peek().kind === "parameter") {
      const parameter = this.#parameterExpression();
      result = parameter.kind === "parameter" ? { offsetParameter: parameter.index } : {};
    } else {
      result = { offset: validateOffset(Number(this.#take("number").text)) };
    }
    // The standard allows OFFSET n ROW[S].
    if (this.#isKeyword("ROWS")) this.#keyword("ROWS");
    else if (this.#isKeyword("ROW")) this.#keyword("ROW");
    return result;
  }

  /** LIMIT/OFFSET in either dialect order: LIMIT n [OFFSET m], or OFFSET m [FETCH FIRST ...]. */
  #tailClauses(): {
    limit?: number;
    limitParameter?: number;
    limitWithTies?: boolean;
    offset?: number;
    offsetParameter?: number;
  } {
    if (this.#isKeyword("LIMIT")) {
      return { ...this.#limitClause(), ...this.#offsetClause() };
    }
    return { ...this.#offsetClause(), ...this.#fetchClause() };
  }

  #selectBlock(sql: string): CompiledQuery {
    const enclosingWindows = this.#namedWindows;
    this.#namedWindows = this.#scanNamedWindows();
    try {
      return this.#selectBlockBody(sql);
    } finally {
      this.#namedWindows = enclosingWindows;
    }
  }

  /**
   * Finds this block's `WINDOW name AS (...)` definitions before its select list parses, since
   * `OVER name` in the select list refers forward to them (T620). Each name maps to the token
   * span of its definition, which `#overClause` re-enters when it meets the reference.
   */
  #scanNamedWindows(): Map<string, { start: number; end: number }> {
    const windows = new Map<string, { start: number; end: number }>();
    let depth = 0;
    for (let index = this.#index; index < this.tokens.length; index += 1) {
      const token = this.tokens[index];
      if (token === undefined || token.kind === "eof") break;
      if (token.kind === "punctuation" && token.text === "(") depth += 1;
      else if (token.kind === "punctuation" && token.text === ")") {
        if (depth === 0) break;
        depth -= 1;
      }
      if (depth !== 0 || token.kind !== "identifier" || token.quoted === true) continue;
      const keyword = token.text.toUpperCase();
      // A set operator ends this block; anything past it belongs to another one.
      if (keyword === "UNION" || keyword === "INTERSECT" || keyword === "EXCEPT") break;
      if (keyword !== "WINDOW") continue;
      let cursor = index + 1;
      for (;;) {
        const name = this.tokens[cursor];
        const as = this.tokens[cursor + 1];
        const open = this.tokens[cursor + 2];
        if (
          name?.kind !== "identifier" ||
          as?.kind !== "identifier" ||
          as.text.toUpperCase() !== "AS" ||
          open?.kind !== "punctuation" ||
          open.text !== "("
        ) {
          throw new TypeError("WINDOW takes name AS (window specification)");
        }
        let inner = 1;
        let end = cursor + 3;
        while (end < this.tokens.length && inner > 0) {
          const scan = this.tokens[end];
          if (scan?.kind === "punctuation" && scan.text === "(") inner += 1;
          if (scan?.kind === "punctuation" && scan.text === ")") inner -= 1;
          end += 1;
        }
        if (inner !== 0) throw new TypeError("Unterminated WINDOW specification");
        if (windows.has(name.text)) throw new TypeError(`Duplicate window name: ${name.text}`);
        windows.set(name.text, { start: cursor + 2, end });
        const next = this.tokens[end];
        if (next?.kind !== "punctuation" || next.text !== ",") break;
        cursor = end + 1;
      }
      break;
    }
    return windows;
  }

  #selectBlockBody(sql: string): CompiledQuery {
    this.#keyword("SELECT");
    let distinct = false;
    if (this.#isKeyword("DISTINCT")) {
      this.#keyword("DISTINCT");
      distinct = true;
    }
    const select = this.#selectList();
    let base: TableSource;
    if (this.#isKeyword("FROM")) {
      this.#keyword("FROM");
      base = this.#source();
    } else {
      // A FROM-less SELECT evaluates its expressions over the one-row dual source.
      if (select.some((item) => item.expression.kind === "wildcard")) {
        throw new TypeError("SELECT * requires a FROM clause");
      }
      base = { table: DUAL_TABLE, alias: DUAL_TABLE };
    }
    const joins: JoinPlan[] = [];
    let rightJoins = 0;
    /** A cross join: the nested-loop path with a condition every row pair satisfies. */
    const crossJoin = (source: TableSource): JoinPlan => ({
      ...source,
      kind: "inner",
      left: { kind: "literal", value: null },
      right: { kind: "literal", value: null },
      on: {
        kind: "condition",
        operator: "=",
        left: { kind: "literal", value: 1 },
        right: { kind: "literal", value: 1 },
      },
    });
    while (
      this.#peek().text === "," ||
      this.#isKeyword("NATURAL") ||
      this.#isKeyword("JOIN") ||
      this.#isKeyword("INNER") ||
      this.#isKeyword("LEFT") ||
      this.#isKeyword("RIGHT") ||
      this.#isKeyword("FULL") ||
      this.#isKeyword("CROSS")
    ) {
      if (this.#punctuation(",")) {
        // F041-07: a comma between table references is a cross join.
        joins.push(crossJoin(this.#source()));
        continue;
      }
      if (this.#isKeyword("CROSS")) {
        this.#keyword("CROSS");
        this.#keyword("JOIN");
        joins.push(crossJoin(this.#source()));
        continue;
      }
      let kind: JoinPlan["kind"] = "inner";
      let right = false;
      let full = false;
      let natural = false;
      if (this.#isKeyword("NATURAL")) {
        // F401-01: the join columns are the names both sides share, which only the catalog
        // knows; the join carries the marker until an execution entry resolves it.
        this.#keyword("NATURAL");
        natural = true;
      }
      if (this.#isKeyword("INNER")) this.#keyword("INNER");
      else if (this.#isKeyword("LEFT")) {
        this.#keyword("LEFT");
        kind = "left";
      } else if (this.#isKeyword("RIGHT")) {
        this.#keyword("RIGHT");
        right = true;
        rightJoins += 1;
      } else if (this.#isKeyword("FULL")) {
        this.#keyword("FULL");
        kind = "left";
        full = true;
      }
      if (this.#isKeyword("OUTER")) this.#keyword("OUTER");
      this.#keyword("JOIN");
      const source = this.#source();
      if (natural) {
        if (this.#isKeyword("ON") || this.#isKeyword("USING")) {
          throw new TypeError("A NATURAL join takes no ON or USING clause");
        }
        if (right) {
          // The RIGHT mirror below rewrites the join's sources, which the shared-column search
          // has to see; a natural right join would resolve against the wrong side.
          throw new TypeError("NATURAL RIGHT JOIN is not supported; write it as a LEFT join");
        }
        joins.push({
          ...source,
          kind,
          left: { kind: "literal", value: null },
          right: { kind: "literal", value: null },
          natural: true,
          ...(full ? { full } : {}),
        });
        continue;
      }
      const condition = this.#isKeyword("USING")
        ? this.#usingCondition(base.alias, source.alias)
        : (this.#keyword("ON"), this.#expression());
      if (condition.kind === "condition" && condition.operator === "=") {
        joins.push({
          ...source,
          kind,
          left: condition.left,
          right: condition.right,
          ...(full ? { full } : {}),
        });
      } else {
        // Any other boolean condition becomes a nested-loop join.
        joins.push({
          ...source,
          kind,
          left: { kind: "literal", value: null },
          right: { kind: "literal", value: null },
          on: condition,
          ...(full ? { full } : {}),
        });
      }
      if (right) {
        // A RIGHT JOIN is the mirrored LEFT JOIN; with a single join the sources just swap.
        if (joins.length !== 1 || rightJoins !== 1) {
          throw new TypeError("RIGHT JOIN is only supported as the sole join");
        }
        if (select.some((item) => item.expression.kind === "wildcard")) {
          throw new TypeError("RIGHT JOIN cannot be combined with SELECT *");
        }
        const [only] = joins;
        if (only !== undefined) {
          const { kind: joinKind, left, right: rightSide, ...joinSource } = only;
          void joinKind;
          const previousBase = base;
          base = joinSource;
          joins[0] = {
            ...previousBase,
            kind: "left",
            left,
            right: rightSide,
            ...(only.on === undefined ? {} : { on: only.on }),
          };
        }
      }
    }
    const predicates: Predicate[] = [];
    if (this.#isKeyword("WHERE")) {
      this.#keyword("WHERE");
      predicates.push(...splitCondition(this.#expression()));
    }
    const groupBy: Expression[] = [];
    let groupingSets: Expression[][] | undefined;
    if (this.#isKeyword("GROUP")) {
      this.#keyword("GROUP");
      this.#keyword("BY");
      if (this.#isKeyword("GROUPING") || this.#isKeyword("ROLLUP") || this.#isKeyword("CUBE")) {
        groupingSets = this.#groupingSets();
      } else {
        groupBy.push(...this.#expressionList());
      }
    }
    const having: Predicate[] = [];
    if (this.#isKeyword("HAVING")) {
      this.#keyword("HAVING");
      if (distinct) throw new TypeError("SELECT DISTINCT cannot be combined with HAVING");
      having.push(...splitCondition(this.#expression()));
    }
    if (this.#isKeyword("WINDOW")) {
      // The definitions were read before the select list; here they only have to be stepped over.
      this.#keyword("WINDOW");
      for (;;) {
        this.#identifier();
        this.#keyword("AS");
        const definition = this.#namedWindows.get(this.tokens[this.#index - 2]?.text ?? "");
        this.#index = definition?.end ?? this.#index;
        if (!this.#punctuation(",")) break;
      }
    }
    const orderBy = this.#orderByClause();
    return assembleSelectBlock(
      {
        sql,
        base,
        joins,
        select,
        distinct,
        predicates,
        groupBy,
        having,
        orderBy,
        ...this.#tailClauses(),
        ...(groupingSets === undefined ? {} : { groupingSets }),
      },
      this.nextDerivedSequence,
    );
  }

  /** GROUPING SETS ((a,b),(a),()), ROLLUP(a,b), or CUBE(a,b), expanded to grouping lists. */
  #groupingSets(): Expression[][] {
    if (this.#isKeyword("GROUPING")) {
      this.#keyword("GROUPING");
      this.#keyword("SETS");
      this.#expectPunctuation("(");
      const sets: Expression[][] = [];
      for (;;) {
        this.#expectPunctuation("(");
        const set: Expression[] = [];
        if (!this.#punctuation(")")) {
          set.push(...this.#expressionList());
          this.#expectPunctuation(")");
        }
        sets.push(set);
        if (!this.#punctuation(",")) break;
      }
      this.#expectPunctuation(")");
      return sets;
    }
    const cube = this.#isKeyword("CUBE");
    this.#keyword(cube ? "CUBE" : "ROLLUP");
    this.#expectPunctuation("(");
    const columns = this.#expressionList();
    this.#expectPunctuation(")");
    if (columns.length === 0) throw new TypeError("ROLLUP/CUBE take at least one expression");
    if (cube) {
      if (columns.length > 5) {
        throw new TypeError("CUBE supports at most 5 expressions (32 grouping sets)");
      }
      const sets: Expression[][] = [];
      for (let mask = (1 << columns.length) - 1; mask >= 0; mask -= 1) {
        sets.push(columns.filter((_, index) => (mask & (1 << index)) !== 0));
      }
      return sets;
    }
    const sets: Expression[][] = [];
    for (let length = columns.length; length >= 0; length -= 1) {
      sets.push(columns.slice(0, length));
    }
    return sets;
  }

  #selectList(): SelectItem[] {
    const items: SelectItem[] = [];
    for (;;) {
      const expression = this.#expression();
      let alias = defaultAlias(expression);
      if (this.#isKeyword("AS")) {
        this.#keyword("AS");
        alias = this.#identifier();
      }
      items.push({ expression, alias });
      if (!this.#punctuation(",")) break;
    }
    if (
      items.some(
        (item) => item.expression.kind === "wildcard" && item.expression.table === undefined,
      ) &&
      items.length > 1
    ) {
      throw new TypeError("SELECT * cannot be mixed with other expressions");
    }
    const aliases = new Set<string>();
    for (const item of items) {
      if (item.expression.kind === "list") {
        throw new TypeError("A row constructor is only allowed in a comparison or IN list");
      }
      // A qualified wildcard has no single output name until expansion resolves its columns.
      if (item.expression.kind === "wildcard" && item.expression.table !== undefined) continue;
      if (aliases.has(item.alias)) throw new TypeError(`Duplicate output column: ${item.alias}`);
      aliases.add(item.alias);
    }
    return items;
  }

  /**
   * Parses one WITH RECURSIVE body: `base UNION [ALL] step`, where only the step may reference
   * the CTE's own name. A body without a self-reference or UNION registers as a plain CTE.
   */
  #recursiveCte(name: string, columns?: readonly string[]): void {
    const candidate = { name, reference: `(recursive ${name})` };
    this.#recursiveCandidate = candidate;
    try {
      const base = this.#selectBlock("(cte)");
      // Before the step parses: it reads the working set by the declared names.
      if (columns !== undefined) renameBlockOutputs(base, columns, name);
      if (this.#recursiveUses.has(name)) {
        throw new TypeError("A recursive CTE may only reference itself in its step member");
      }
      if (!this.#isKeyword("UNION")) {
        this.#expectPunctuation(")");
        this.#ctes.set(name, base);
        return;
      }
      this.#keyword("UNION");
      let all = false;
      if (this.#isKeyword("ALL")) {
        this.#keyword("ALL");
        all = true;
      }
      const step = this.#selectBlock("(cte step)");
      if (this.#isKeyword("UNION") || this.#isKeyword("INTERSECT") || this.#isKeyword("EXCEPT")) {
        throw new TypeError("A recursive CTE takes exactly one UNION between base and step");
      }
      this.#expectPunctuation(")");
      if (columns !== undefined) renameBlockOutputs(step, columns, name);
      if (!this.#recursiveUses.has(name)) {
        // No self-reference: the body is an ordinary two-member compound.
        this.#ctes.set(
          name,
          compoundSelectBlock(
            "(cte)",
            [base, step],
            [all ? "union all" : "union"],
            { orderBy: [] },
            this.nextDerivedSequence,
          ),
        );
        return;
      }
      this.#recursiveCtes.set(name, { reference: candidate.reference, base, step, all });
    } finally {
      this.#recursiveCandidate = undefined;
    }
  }

  /** VALUES (…), (…) desugars into FROM-less selects over dual, unioned with UNION ALL. */
  #valuesBlock(): CompiledQuery {
    this.#keyword("VALUES");
    const blocks: CompiledQuery[] = [];
    let width: number | undefined;
    for (;;) {
      this.#expectPunctuation("(");
      const row: Expression[] = [];
      for (;;) {
        row.push(this.#expression());
        if (!this.#punctuation(",")) break;
      }
      this.#expectPunctuation(")");
      if (row.some((expression) => expression.kind === "wildcard")) {
        throw new TypeError("VALUES rows take scalar expressions");
      }
      if (width === undefined) width = row.length;
      else if (row.length !== width) throw new TypeError("VALUES rows must share one width");
      blocks.push({
        sql: "(values)",
        base: { table: DUAL_TABLE, alias: DUAL_TABLE },
        joins: [],
        select: row.map((expression, index) => ({
          expression,
          alias: `column${String(index + 1)}`,
        })),
        predicates: [],
        groupBy: [],
        having: [],
        orderBy: [],
      });
      if (!this.#punctuation(",")) break;
    }
    const first = blocks[0];
    if (blocks.length === 1 && first !== undefined) return first;
    return compoundSelectBlock(
      "(values)",
      blocks,
      blocks.slice(1).map(() => "union all" as const),
      { orderBy: [] },
      this.nextDerivedSequence,
    );
  }

  /** Renames a derived table's output columns from an AS alias(name, ...) list. */
  #applyColumnAliases(derived: CompiledQuery): void {
    const names: string[] = [];
    for (;;) {
      names.push(this.#identifier());
      if (!this.#punctuation(",")) break;
    }
    this.#expectPunctuation(")");
    // Set-operation output takes the first member's aliases, so renaming targets it.
    const target =
      derived.base.union !== undefined && derived.select[0]?.expression.kind === "wildcard"
        ? derived.base.union.blocks[0]
        : derived;
    if (target?.select.length !== names.length) {
      throw new TypeError("Column alias list must match the derived table's column count");
    }
    names.forEach((name, index) => {
      const item = target.select[index];
      if (item !== undefined) item.alias = name;
    });
  }

  /**
   * `JOIN t USING (a, b)`: the named columns must exist on both sides, and the join condition
   * is their conjunction (F401-04). Unlike the standard, the joined columns still appear once
   * per side in a `SELECT *`, because this engine names wildcard outputs per source anyway.
   */
  #usingCondition(leftAlias: string, rightAlias: string): Expression {
    this.#keyword("USING");
    this.#expectPunctuation("(");
    const names: string[] = [];
    for (;;) {
      names.push(this.#identifier());
      if (!this.#punctuation(",")) break;
    }
    this.#expectPunctuation(")");
    return names
      .map<Expression>((name) => ({
        kind: "condition",
        operator: "=",
        left: { kind: "column", reference: `${leftAlias}.${name}` },
        right: { kind: "column", reference: `${rightAlias}.${name}` },
      }))
      .reduce((accumulated, condition) => ({
        kind: "logical",
        operator: "and",
        left: accumulated,
        right: condition,
      }));
  }

  #source(): TableSource {
    if (this.#punctuation("(")) {
      const derived = this.#isKeyword("VALUES")
        ? this.#valuesBlock()
        : this.#queryExpression("(derived)");
      this.#expectPunctuation(")");
      const alias = this.#sourceAlias();
      if (alias === undefined) throw new TypeError("A derived table requires an alias");
      if (this.#punctuation("(")) this.#applyColumnAliases(derived);
      return this.#derivedSource(derived, alias);
    }
    const table = this.#identifier();
    if (this.#peek().text === "(") {
      // Two row-producing sources the executors have no operator for; both parse far enough to
      // say so, rather than failing on the punctuation that follows.
      const upper = table.toUpperCase();
      if (upper === "LATERAL") {
        throw new TypeError("LATERAL sources are not supported");
      }
      if (upper === "JSON_TABLE") {
        throw new TypeError(
          "JSON_TABLE is not supported; JSON_VALUE and JSON_QUERY read one value",
        );
      }
    }
    const candidate = this.#recursiveCandidate;
    if (candidate?.name === table) {
      this.#recursiveUses.add(table);
      return { table: candidate.reference, alias: this.#sourceAlias() ?? table };
    }
    if (this.#ctesInProgress.has(table)) {
      throw new TypeError(`Recursive CTE references require WITH RECURSIVE: ${table}`);
    }
    const alias = this.#sourceAlias() ?? table;
    const cte = this.#ctes.get(table);
    if (cte !== undefined) {
      const source = structuredClone(cte);
      if (this.#peek().text === "(") {
        this.#expectPunctuation("(");
        this.#applyColumnAliases(source);
      }
      return this.#derivedSource(source, alias);
    }
    const recursive = this.#recursiveCtes.get(table);
    if (recursive !== undefined) {
      return {
        table: `(recursive ${String(this.nextDerivedSequence())}) ${table}`,
        alias,
        recursive: structuredClone(recursive),
      };
    }
    if (this.#peek().text === "(") {
      // E051-09: `FROM t AS y(a, b)` renames the table's columns positionally. The names are
      // kept on the source because the table's own column order is only known at execution.
      this.#expectPunctuation("(");
      const columnAliases: string[] = [];
      for (;;) {
        columnAliases.push(this.#identifier());
        if (!this.#punctuation(",")) break;
      }
      this.#expectPunctuation(")");
      return { table, alias, columnAliases };
    }
    return { table, alias };
  }

  #sourceAlias(): string | undefined {
    if (this.#isKeyword("AS")) {
      this.#keyword("AS");
      return this.#identifier();
    }
    if (
      this.#peek().kind === "identifier" &&
      !clauseKeywords.has(this.#peek().text.toUpperCase()) &&
      !this.#isKeyword("ON")
    ) {
      return this.#identifier();
    }
    return undefined;
  }

  #derivedSource(derived: CompiledQuery, alias: string): TableSource {
    return derivedTableSource(derived, alias, this.nextDerivedSequence);
  }

  #expressionList(): Expression[] {
    const values = [this.#expression()];
    while (this.#punctuation(",")) values.push(this.#expression());
    return values;
  }

  // Full-precedence expression grammar: OR < AND < NOT < comparison < additive < multiplicative.
  // Comparisons are ordinary boolean-valued expressions, so parenthesized conditions, boolean
  // select items, and NOT/OR trees all share one parse path.
  #expression(): Expression {
    let left = this.#andExpression();
    while (this.#isKeyword("OR")) {
      this.#keyword("OR");
      left = { kind: "logical", operator: "or", left, right: this.#andExpression() };
    }
    return left;
  }

  #andExpression(): Expression {
    let left = this.#notExpression();
    while (this.#isKeyword("AND")) {
      this.#keyword("AND");
      left = { kind: "logical", operator: "and", left, right: this.#notExpression() };
    }
    return left;
  }

  #notExpression(): Expression {
    if (this.#isKeyword("NOT")) {
      this.#keyword("NOT");
      return { kind: "not", operand: this.#notExpression() };
    }
    return this.#comparisonExpression();
  }

  /**
   * Field-wise expansion of a row comparison (F641). Equality is the conjunction of the field
   * equalities and inequality their disjunction; the ordering operators compare
   * lexicographically, each field deciding only when every earlier field is equal.
   */
  #rowComparison(
    operator: ComparisonOperator,
    left: Expression[],
    right: Expression[],
  ): Expression {
    if (left.length !== right.length) {
      throw new TypeError("Compared rows must have the same number of fields");
    }
    for (const side of [left, right]) {
      if (side.some((field) => field.kind === "list")) {
        throw new TypeError("Row constructors cannot nest");
      }
    }
    const pair = (index: number, op: ComparisonOperator): Expression => ({
      kind: "condition",
      operator: op,
      left: structuredClone(left[index] ?? { kind: "literal", value: null }),
      right: structuredClone(right[index] ?? { kind: "literal", value: null }),
    });
    if (operator === "=" || operator === "!=" || operator === "<>") {
      const join = operator === "=" ? "and" : "or";
      return left
        .map((_, index) => pair(index, operator))
        .reduce((accumulated, condition) => ({
          kind: "logical",
          operator: join,
          left: accumulated,
          right: condition,
        }));
    }
    const strict: ComparisonOperator = operator === "<" || operator === "<=" ? "<" : ">";
    const lexicographic = (index: number): Expression => {
      // The final field decides with the original operator, so <= and >= stay inclusive.
      if (index === left.length - 1) return pair(index, operator);
      return {
        kind: "logical",
        operator: "or",
        left: pair(index, strict),
        right: {
          kind: "logical",
          operator: "and",
          left: pair(index, "="),
          right: lexicographic(index + 1),
        },
      };
    };
    return lexicographic(0);
  }

  /**
   * The predicates a row constructor may head: `IS [NOT] NULL`, a comparison against another
   * row, and `IN` over a list of rows. `R IS NOT NULL` is true when every field is non-null,
   * which is why it is built directly rather than as the negation of `R IS NULL`.
   */
  #rowPredicate(row: Expression & { kind: "list" }): Expression {
    const fields = row.items;
    const conjunction = (operator: PredicateOperator): Expression =>
      fields
        .map<Expression>((field) => ({
          kind: "condition",
          operator,
          left: field,
          right: { kind: "literal", value: null },
        }))
        .reduce((accumulated, condition) => ({
          kind: "logical",
          operator: "and",
          left: accumulated,
          right: condition,
        }));
    if (this.#isKeyword("IS")) {
      this.#keyword("IS");
      if (this.#isKeyword("NOT")) {
        this.#keyword("NOT");
        this.#keyword("NULL");
        return conjunction("IS NOT NULL");
      }
      this.#keyword("NULL");
      return conjunction("IS NULL");
    }
    let negated = false;
    if (this.#isKeyword("NOT")) {
      this.#keyword("NOT");
      negated = true;
    }
    if (this.#isKeyword("IN")) {
      this.#keyword("IN");
      this.#expectPunctuation("(");
      const items = this.#expressionList();
      this.#expectPunctuation(")");
      const membership = items
        .map((item) => {
          if (item.kind !== "list") throw new TypeError("A row IN list takes row constructors");
          return this.#rowComparison("=", fields, item.items);
        })
        .reduce((accumulated, condition) => ({
          kind: "logical" as const,
          operator: "or" as const,
          left: accumulated,
          right: condition,
        }));
      return negated ? { kind: "not", operand: membership } : membership;
    }
    if (negated) throw new TypeError("Expected IN after NOT");
    const token = this.#peek();
    if (
      token.kind !== "operator" ||
      !["=", "!=", "<>", ">", ">=", "<", "<="].includes(token.text)
    ) {
      throw new TypeError("A row constructor takes a comparison, IN, or IS NULL");
    }
    const operator = this.#comparison();
    const right = this.#additive();
    if (right.kind !== "list") throw new TypeError("A row comparison takes a row on both sides");
    return this.#rowComparison(operator, fields, right.items);
  }

  #comparisonExpression(): Expression {
    const left = this.#additive();
    if (left.kind === "list") {
      // A row constructor either heads a predicate here, or is one member of an IN list and
      // belongs to the enclosing comparison, which reads it as a row.
      const token = this.#peek();
      const heads =
        this.#isKeyword("IS") ||
        this.#isKeyword("IN") ||
        this.#isKeyword("NOT") ||
        (token.kind === "operator" && ["=", "!=", "<>", ">", ">=", "<", "<="].includes(token.text));
      return heads ? this.#rowPredicate(left) : left;
    }
    if (this.#isKeyword("IS")) {
      this.#keyword("IS");
      let negatedIs = false;
      if (this.#isKeyword("NOT")) {
        this.#keyword("NOT");
        negatedIs = true;
      }
      if (this.#isKeyword("DISTINCT")) {
        this.#keyword("DISTINCT");
        this.#keyword("FROM");
        return {
          kind: "condition",
          operator: negatedIs ? "IS NOT DISTINCT FROM" : "IS DISTINCT FROM",
          left,
          right: this.#additive(),
        };
      }
      if (this.#isKeyword("JSON")) {
        // T825: IS JSON [VALUE|OBJECT|ARRAY|SCALAR], optionally negated.
        this.#keyword("JSON");
        let kind = "value";
        for (const shape of ["VALUE", "OBJECT", "ARRAY", "SCALAR"] as const) {
          if (!this.#isKeyword(shape)) continue;
          this.#keyword(shape);
          kind = shape.toLowerCase();
          break;
        }
        const test: Expression = {
          kind: "call",
          name: "IS_JSON",
          arguments: [left, { kind: "literal", value: kind }],
        };
        return negatedIs ? { kind: "not", operand: test } : test;
      }
      // IS [NOT] TRUE/FALSE never return UNKNOWN, which is exactly null-safe (in)equality.
      if (this.#isKeyword("TRUE") || this.#isKeyword("FALSE")) {
        const truth = this.#isKeyword("TRUE");
        this.#keyword(truth ? "TRUE" : "FALSE");
        return {
          kind: "condition",
          operator: negatedIs ? "IS DISTINCT FROM" : "IS NOT DISTINCT FROM",
          left,
          right: { kind: "literal", value: truth },
        };
      }
      if (this.#isKeyword("UNKNOWN")) {
        this.#keyword("UNKNOWN");
        return {
          kind: "condition",
          operator: negatedIs ? "IS NOT NULL" : "IS NULL",
          left,
          right: { kind: "literal", value: null },
        };
      }
      const token = this.#peek();
      if (token.kind !== "identifier" || token.text.toUpperCase() !== "NULL") {
        throw new TypeError(`Expected NULL, found ${token.text || "end of query"}`);
      }
      this.#identifier();
      return {
        kind: "condition",
        operator: negatedIs ? "IS NOT NULL" : "IS NULL",
        left,
        right: { kind: "literal", value: null },
      };
    }
    // After a value expression, NOT can only introduce NOT BETWEEN / NOT IN / NOT LIKE.
    let negated = false;
    if (this.#isKeyword("NOT")) {
      this.#keyword("NOT");
      negated = true;
      if (
        !this.#isKeyword("BETWEEN") &&
        !this.#isKeyword("IN") &&
        !this.#isKeyword("LIKE") &&
        !this.#isKeyword("ILIKE")
      ) {
        throw new TypeError(`Expected BETWEEN, IN, LIKE, or ILIKE after NOT`);
      }
    }
    if (this.#isKeyword("BETWEEN")) {
      this.#keyword("BETWEEN");
      let symmetric = false;
      if (this.#isKeyword("SYMMETRIC")) {
        this.#keyword("SYMMETRIC");
        symmetric = true;
      } else if (this.#isKeyword("ASYMMETRIC")) {
        this.#keyword("ASYMMETRIC");
      }
      const lower = this.#additive();
      this.#keyword("AND");
      const upper = this.#additive();
      // BETWEEN is inclusive-range sugar; the AND here binds at comparison level by the grammar.
      const rangeOver = (low: Expression, high: Expression): Expression => ({
        kind: "logical",
        operator: "and",
        left: { kind: "condition", operator: ">=", left: structuredClone(left), right: low },
        right: { kind: "condition", operator: "<=", left: structuredClone(left), right: high },
      });
      // SYMMETRIC also accepts the bounds reversed.
      const range: Expression = symmetric
        ? {
            kind: "logical",
            operator: "or",
            left: rangeOver(lower, upper),
            right: rangeOver(structuredClone(upper), structuredClone(lower)),
          }
        : rangeOver(lower, upper);
      return negated ? { kind: "not", operand: range } : range;
    }
    if (this.#isKeyword("IN")) {
      this.#keyword("IN");
      const operator: PredicateOperator = negated ? "NOT IN" : "IN";
      this.#expectPunctuation("(");
      if (this.#isKeyword("SELECT")) {
        const block = this.#selectBlock("(subquery)");
        this.#expectPunctuation(")");
        return { kind: "condition", operator, left, right: { kind: "subquery", block } };
      }
      const items = this.#expressionList();
      this.#expectPunctuation(")");
      if (items.some((item) => item.kind === "wildcard" || item.kind === "subquery")) {
        throw new TypeError("IN lists accept only scalar expressions");
      }
      return { kind: "condition", operator, left, right: { kind: "list", items } };
    }
    if (this.#isKeyword("LIKE") || this.#isKeyword("ILIKE")) {
      const insensitive = this.#isKeyword("ILIKE");
      this.#keyword(insensitive ? "ILIKE" : "LIKE");
      const operator: PredicateOperator = insensitive
        ? negated
          ? "NOT ILIKE"
          : "ILIKE"
        : negated
          ? "NOT LIKE"
          : "LIKE";
      const pattern = this.#additive();
      let escape: string | undefined;
      if (this.#isKeyword("ESCAPE")) {
        this.#keyword("ESCAPE");
        const token = this.#take("string");
        if (Array.from(token.text).length !== 1) {
          throw new TypeError("LIKE ESCAPE takes a single character");
        }
        escape = token.text;
      }
      return {
        kind: "condition",
        operator,
        left,
        right: pattern,
        ...(escape === undefined ? {} : { escape }),
      };
    }
    const token = this.#peek();
    if (token.kind === "operator" && ["=", "!=", "<>", ">", ">=", "<", "<="].includes(token.text)) {
      const operator = this.#comparison();
      if (this.#isKeyword("ANY") || this.#isKeyword("SOME") || this.#isKeyword("ALL")) {
        const all = this.#isKeyword("ALL");
        this.#keyword(this.#isKeyword("ANY") ? "ANY" : all ? "ALL" : "SOME");
        this.#expectPunctuation("(");
        if (!this.#isKeyword("SELECT")) {
          throw new TypeError("ANY/ALL take a subquery");
        }
        const block = this.#selectBlock("(quantified subquery)");
        this.#expectPunctuation(")");
        return {
          kind: "condition",
          operator: `${operator} ${all ? "ALL" : "ANY"}`,
          left,
          right: { kind: "subquery", block },
        };
      }
      return { kind: "condition", operator, left, right: this.#additive() };
    }
    return left;
  }

  #additive(minimumPrecedence = 0): Expression {
    let left = this.#primary();
    for (;;) {
      const operator = this.#peek().text;
      // || binds loosest, matching PostgreSQL: concatenation applies to whole arithmetic terms.
      const precedence =
        operator === "*" || operator === "/" || operator === "%"
          ? 20
          : operator === "+" || operator === "-"
            ? 10
            : operator === "||"
              ? 5
              : -1;
      if (precedence < minimumPrecedence) break;
      this.#index += 1;
      // `placed_at + INTERVAL '1 month'`. An interval is not a value any column can hold, so it
      // never becomes an expression of its own: it is read here, where the thing it applies to is
      // already in hand, and folds into the date arithmetic DATE_ADD performs.
      if ((operator === "+" || operator === "-") && this.#isKeyword("INTERVAL")) {
        this.#keyword("INTERVAL");
        const { months, milliseconds } = intervalLiteral(this.#take("string").text);
        const sign = operator === "-" ? -1 : 1;
        left = {
          kind: "call",
          name: "DATE_ADD",
          arguments: [
            left,
            { kind: "literal", value: sign * months },
            { kind: "literal", value: sign * milliseconds },
          ],
        };
        continue;
      }
      left = {
        kind: "binary",
        operator: operator as BinaryOperator,
        left,
        right: this.#additive(precedence + 1),
      };
    }
    return left;
  }

  #parameterExpression(): Expression {
    const token = this.#take("parameter");
    if (token.text === "") {
      if (this.#highestNumberedParameter > 0) {
        throw new TypeError("Use either ? or $n placeholders in one statement, not both");
      }
      this.#positionalParameters += 1;
      return { kind: "parameter", index: this.#positionalParameters - 1 };
    }
    if (this.#positionalParameters > 0) {
      throw new TypeError("Use either ? or $n placeholders in one statement, not both");
    }
    const number = Number(token.text);
    if (!Number.isInteger(number) || number < 1) {
      throw new TypeError(`Parameter numbers start at $1: $${token.text}`);
    }
    this.#highestNumberedParameter = Math.max(this.#highestNumberedParameter, number);
    return { kind: "parameter", index: number - 1 };
  }

  #primary(): Expression {
    const token = this.#peek();
    if (token.kind === "parameter") return this.#parameterExpression();
    if (token.kind === "number") {
      this.#index += 1;
      return { kind: "literal", value: Number(token.text) };
    }
    if (token.kind === "string") {
      this.#index += 1;
      return { kind: "literal", value: token.text };
    }
    if (token.kind === "operator" && token.text === "-") {
      this.#index += 1;
      return {
        kind: "binary",
        operator: "*",
        left: { kind: "literal", value: -1 },
        right: this.#primary(),
      };
    }
    if (token.kind === "operator" && token.text === "+") {
      // Unary plus is the identity the standard allows in front of any numeric operand.
      this.#index += 1;
      return this.#primary();
    }
    if (token.kind === "operator" && token.text === "*") {
      this.#index += 1;
      return { kind: "wildcard" };
    }
    if (this.#punctuation("(")) {
      if (this.#isKeyword("SELECT")) {
        const block = this.#selectBlock("(subquery)");
        this.#expectPunctuation(")");
        return { kind: "subquery", block };
      }
      const expression = this.#expression();
      if (this.#peek().text === ",") {
        // F641: a parenthesized list of values is a row constructor. Comparisons against it
        // desugar into field-wise conditions, so no executor ever sees a row value.
        const items = [expression];
        while (this.#punctuation(",")) items.push(this.#expression());
        this.#expectPunctuation(")");
        return { kind: "list", items };
      }
      this.#expectPunctuation(")");
      return expression;
    }
    const quotedIdentifier = this.#peek().quoted === true;
    const identifier = this.#identifier();
    if (quotedIdentifier) {
      // A quoted identifier is always a plain reference — never a keyword, literal, or function.
      let reference = identifier;
      if (this.#punctuation(".")) reference += `.${this.#identifier()}`;
      return { kind: "column", reference };
    }
    const upper = identifier.toUpperCase();
    if (upper === "TRUE" || upper === "FALSE" || upper === "NULL")
      return { kind: "literal", value: upper === "NULL" ? null : upper === "TRUE" };
    if (upper === "CASE") {
      let operand: Expression | undefined;
      if (!this.#isKeyword("WHEN")) operand = this.#expression();
      const branches: Array<{ when: Expression; then: Expression }> = [];
      while (this.#isKeyword("WHEN")) {
        this.#keyword("WHEN");
        let when = this.#expression();
        // The simple form CASE x WHEN v desugars to searched equality comparisons.
        if (operand !== undefined) {
          when = { kind: "condition", operator: "=", left: structuredClone(operand), right: when };
        }
        this.#keyword("THEN");
        branches.push({ when, then: this.#expression() });
      }
      if (branches.length === 0) throw new TypeError("CASE requires at least one WHEN branch");
      let otherwise: Expression | undefined;
      if (this.#isKeyword("ELSE")) {
        this.#keyword("ELSE");
        otherwise = this.#expression();
      }
      this.#keyword("END");
      return { kind: "case", branches, ...(otherwise === undefined ? {} : { otherwise }) };
    }
    if (upper === "EXISTS" && this.#peek().text === "(") {
      this.#expectPunctuation("(");
      if (!this.#isKeyword("SELECT")) throw new TypeError("EXISTS requires a subquery");
      const block = this.#selectBlock("(subquery)");
      this.#expectPunctuation(")");
      // Existence needs at most one row, so cap the subquery unless it already limits itself.
      block.limit ??= 1;
      return { kind: "exists", block, negated: false };
    }
    if ((upper === "MATCH" || upper === "BM25") && this.#peek().text === "(") {
      // MySQL-style full-text grammar for both nodes: MATCH(col, ... | *) AGAINST 'query' is
      // the boolean document predicate, BM25(col, ... | *) AGAINST 'query' the relevance score.
      const columns = this.#ftsColumns();
      this.#keyword("AGAINST");
      const queryToken = this.#peek();
      if (queryToken.kind !== "string") {
        throw new TypeError(`${upper} ... AGAINST requires a string literal query`);
      }
      this.#index += 1;
      validateFtsQuery(queryToken.text);
      return {
        kind: "fts",
        op: upper === "MATCH" ? "match" : "bm25",
        columns,
        query: queryToken.text,
      };
    }
    if (upper === "EXTRACT" && this.#peek().text === "(") {
      this.#expectPunctuation("(");
      const field = this.#identifier();
      this.#keyword("FROM");
      const operand = this.#expression();
      this.#expectPunctuation(")");
      // EXTRACT rides the call machinery: the field travels as a literal first argument.
      return {
        kind: "call",
        name: "EXTRACT",
        arguments: [{ kind: "literal", value: field.toLowerCase() }, operand],
      };
    }
    if (statementDatetimeAliases.has(upper)) {
      // F051-06/07/08. The optional empty parentheses are the CURRENT_TIMESTAMP() spelling;
      // an explicit precision is not accepted, since the engine keeps milliseconds either way.
      if (this.#peek().text === "(") {
        this.#expectPunctuation("(");
        this.#expectPunctuation(")");
      }
      this.usesStatementDatetime = true;
      return {
        kind: "call",
        name: statementDatetimeAliases.get(upper) as ScalarFunctionName,
        arguments: [],
      };
    }
    if (upper === "POSITION" && this.#peek().text === "(") {
      // E021-11. POSITION(sub IN str) is INSTR's argument order reversed. The needle parses
      // below the comparison level, so its IN belongs to POSITION rather than to an IN list.
      this.#expectPunctuation("(");
      const needle = this.#additive();
      this.#keyword("IN");
      const haystack = this.#expression();
      this.#expectPunctuation(")");
      return { kind: "call", name: "INSTR", arguments: [haystack, needle] };
    }
    if (upper === "SUBSTRING" && this.#peek().text === "(") {
      // E021-06. Both the standard SUBSTRING(s FROM a FOR b) and the comma spelling parse into
      // one SUBSTR call; the comma form falls through to the ordinary call path below.
      const restore = this.#index;
      this.#expectPunctuation("(");
      const operand = this.#expression();
      if (this.#isKeyword("FROM")) {
        this.#keyword("FROM");
        const args = [operand, this.#expression()];
        if (this.#isKeyword("FOR")) {
          this.#keyword("FOR");
          args.push(this.#expression());
        }
        this.#expectPunctuation(")");
        return { kind: "call", name: "SUBSTR", arguments: args };
      }
      this.#index = restore;
    }
    if (upper === "OVERLAY" && this.#peek().text === "(") {
      // OVERLAY(s PLACING r FROM start [FOR length]).
      this.#expectPunctuation("(");
      const args = [this.#expression()];
      this.#keyword("PLACING");
      args.push(this.#expression());
      this.#keyword("FROM");
      args.push(this.#expression());
      if (this.#isKeyword("FOR")) {
        this.#keyword("FOR");
        args.push(this.#expression());
      }
      this.#expectPunctuation(")");
      return { kind: "call", name: "OVERLAY", arguments: args };
    }
    if (upper === "TRIM" && this.#peek().text === "(") {
      // E021-09 and T056: TRIM([[LEADING|TRAILING|BOTH] [characters] FROM] source). The plain
      // TRIM(x) and TRIM(x, chars) spellings fall through to the ordinary call path.
      const restore = this.#index;
      this.#expectPunctuation("(");
      let side: "LEADING" | "TRAILING" | "BOTH" | undefined;
      for (const candidate of ["LEADING", "TRAILING", "BOTH"] as const) {
        if (this.#isKeyword(candidate)) {
          this.#keyword(candidate);
          side = candidate;
          break;
        }
      }
      if (side !== undefined || !this.#isKeyword("FROM")) {
        // Without a side keyword this is only the standard form if a FROM follows the
        // characters, so parse one expression and check.
        let characters: Expression | undefined;
        if (!this.#isKeyword("FROM")) characters = this.#expression();
        if (this.#isKeyword("FROM")) {
          this.#keyword("FROM");
          const source = this.#expression();
          this.#expectPunctuation(")");
          const name = side === "LEADING" ? "LTRIM" : side === "TRAILING" ? "RTRIM" : "TRIM";
          return {
            kind: "call",
            name,
            arguments: characters === undefined ? [source] : [source, characters],
          };
        }
        if (side !== undefined) throw new TypeError(`TRIM ${side} requires FROM`);
      }
      this.#index = restore;
    }
    if (upper === "CAST" && this.#peek().text === "(") {
      this.#expectPunctuation("(");
      const operand = this.#expression();
      this.#keyword("AS");
      const target = this.#castTarget();
      this.#expectPunctuation(")");
      // CAST rides the call machinery: the target travels as a literal second argument.
      return {
        kind: "call",
        name: "CAST",
        arguments: [operand, { kind: "literal", value: target }],
      };
    }
    if (upper === "DATE" && this.#peek().kind === "string") {
      const date = new Date(`${this.#take("string").text}T00:00:00.000Z`);
      if (!Number.isFinite(date.getTime())) throw new TypeError("Invalid DATE literal");
      return { kind: "literal", value: date };
    }
    if ((upper === "TIMESTAMP" || upper === "DATETIME") && this.#peek().kind === "string") {
      return { kind: "literal", value: timestampLiteral(this.#take("string").text) };
    }
    if (this.#punctuation("(")) {
      if (
        upper === "ROW_NUMBER" ||
        upper === "RANK" ||
        upper === "DENSE_RANK" ||
        upper === "PERCENT_RANK" ||
        upper === "CUME_DIST"
      ) {
        this.#expectPunctuation(")");
        this.#keyword("OVER");
        const { partitionBy, orderBy, frame } = this.#overClause();
        if (frame !== undefined) {
          throw new TypeError(`${upper} does not take a window frame`);
        }
        return { kind: "window", name: upper, partitionBy, orderBy };
      }
      if (upper === "NTILE") {
        const bucketExpression = this.#expression();
        this.#expectPunctuation(")");
        if (
          bucketExpression.kind !== "literal" ||
          typeof bucketExpression.value !== "number" ||
          !Number.isInteger(bucketExpression.value) ||
          bucketExpression.value < 1
        ) {
          throw new TypeError("NTILE requires a positive integer bucket count");
        }
        this.#keyword("OVER");
        const { partitionBy, orderBy, frame } = this.#overClause();
        if (frame !== undefined) throw new TypeError("NTILE does not take a window frame");
        if (orderBy.length === 0) {
          throw new TypeError("NTILE requires ORDER BY inside OVER (...)");
        }
        return {
          kind: "window",
          name: "NTILE",
          partitionBy,
          orderBy,
          offset: bucketExpression.value,
        };
      }
      if (upper === "FIRST_VALUE" || upper === "LAST_VALUE" || upper === "NTH_VALUE") {
        const argument = this.#expression();
        // T618: NTH_VALUE takes the position as a second, constant argument.
        let position = 1;
        if (upper === "NTH_VALUE") {
          this.#expectPunctuation(",");
          const nth = this.#expression();
          if (
            nth.kind !== "literal" ||
            typeof nth.value !== "number" ||
            !Number.isInteger(nth.value) ||
            nth.value < 1
          ) {
            throw new TypeError("NTH_VALUE requires a positive integer position");
          }
          position = nth.value;
        }
        this.#expectPunctuation(")");
        if (argument.kind === "wildcard") {
          throw new TypeError(`${upper} requires a value argument`);
        }
        this.#keyword("OVER");
        const { partitionBy, orderBy, frame } = this.#overClause();
        if (orderBy.length === 0) {
          throw new TypeError(`${upper} requires ORDER BY inside OVER (...)`);
        }
        return {
          kind: "window",
          name: upper,
          partitionBy,
          orderBy,
          argument,
          ...(upper === "NTH_VALUE" ? { offset: position } : {}),
          ...(frame === undefined ? {} : { frame }),
        };
      }
      if (upper === "LAG" || upper === "LEAD") {
        const lagArgs: Expression[] = [];
        if (!this.#punctuation(")")) {
          lagArgs.push(...this.#expressionList());
          this.#expectPunctuation(")");
        }
        if (lagArgs.length < 1 || lagArgs.length > 3) {
          throw new TypeError(
            `${upper} takes a value, an optional offset, and an optional default`,
          );
        }
        const constant = (expression: Expression, label: string): QueryValue => {
          if (
            hasAggregate(expression) ||
            expressionColumns(expression).length > 0 ||
            containsParameter(expression)
          ) {
            throw new TypeError(`${upper} ${label} must be a constant`);
          }
          return asQueryValue(evaluate(expression, {}));
        };
        const offsetExpression = lagArgs[1];
        let offset = 1;
        if (offsetExpression !== undefined) {
          const value = constant(offsetExpression, "offset");
          if (typeof value !== "number" || !Number.isInteger(value) || value < 0) {
            throw new TypeError(`${upper} offset must be a non-negative integer`);
          }
          offset = value;
        }
        const fallbackExpression = lagArgs[2];
        const fallback: QueryValue =
          fallbackExpression === undefined ? null : constant(fallbackExpression, "default");
        this.#keyword("OVER");
        const { partitionBy, orderBy, frame } = this.#overClause();
        if (frame !== undefined) throw new TypeError(`${upper} does not take a window frame`);
        if (orderBy.length === 0) {
          throw new TypeError(`${upper} requires ORDER BY inside OVER (...)`);
        }
        const argument = lagArgs[0];
        if (argument === undefined || argument.kind === "wildcard") {
          throw new TypeError(`${upper} requires a value argument`);
        }
        return {
          kind: "window",
          name: upper,
          partitionBy,
          orderBy,
          argument,
          offset,
          ...(fallback === null ? {} : { fallback }),
        };
      }
      if (upper === "JSON_OBJECT") return this.#jsonObject();
      if (statisticalAggregates.has(upper)) return this.#statisticalAggregate(upper);
      if (upper === "EVERY" || upper === "BOOL_AND" || upper === "BOOL_OR") {
        return this.#booleanAggregate(upper);
      }
      // SUBSTRING/CEILING/CHAR_LENGTH are standard spellings; SUBSTR/CEIL/LENGTH are the
      // canonical plan names. ANY_VALUE picks an implementation-dependent row of the group
      // (T626); MIN is one such choice and reuses its accumulator exactly.
      const name = (upper === "ANY_VALUE" ? "MIN" : (functionSpellings.get(upper) ?? upper)) as
        AggregateName | ScalarFunctionName;
      if (!aggregateNames.has(name as AggregateName) && !scalarFunctionNames.has(name))
        throw new TypeError(`Unsupported function: ${identifier}`);
      let distinct = false;
      if (this.#isKeyword("DISTINCT")) {
        if (!aggregateNames.has(name as AggregateName)) {
          throw new TypeError("DISTINCT is only supported inside aggregate functions");
        }
        this.#keyword("DISTINCT");
        distinct = true;
      } else if (this.#isKeyword("ALL") && aggregateNames.has(name as AggregateName)) {
        // E091-06: ALL is the default set quantifier, so it only has to parse.
        this.#keyword("ALL");
      }
      const args: Expression[] = [];
      if (!this.#punctuation(")")) {
        args.push(...this.#expressionList());
        this.#expectPunctuation(")");
      }
      if (distinct && (args.length !== 1 || args[0]?.kind === "wildcard")) {
        throw new TypeError(`${name}(DISTINCT) requires exactly one scalar argument`);
      }
      if (aggregateNames.has(name as AggregateName) && args.length !== 1)
        throw new TypeError(`${name} requires exactly one argument`);
      if (name === "ROUND" && (args.length < 1 || args.length > 2))
        throw new TypeError("ROUND requires one or two arguments");
      if (name === "COALESCE" && args.length < 1)
        throw new TypeError("COALESCE requires at least one argument");
      if (
        (name === "UPPER" ||
          name === "LOWER" ||
          name === "LENGTH" ||
          name === "OCTET_LENGTH" ||
          name === "ABS" ||
          name === "FLOOR" ||
          name === "CEIL" ||
          name === "SQRT") &&
        args.length !== 1
      ) {
        throw new TypeError(`${name} requires exactly one argument`);
      }
      if (
        (name === "TRIM" || name === "LTRIM" || name === "RTRIM") &&
        (args.length < 1 || args.length > 2)
      ) {
        throw new TypeError(`${name} takes a string and optional trim characters`);
      }
      if ((name === "LPAD" || name === "RPAD") && (args.length < 2 || args.length > 3)) {
        throw new TypeError(`${name} requires a string, a length, and an optional fill`);
      }
      if (name === "OVERLAY" && (args.length < 3 || args.length > 4)) {
        throw new TypeError("OVERLAY requires a string, a replacement, a start, and a length");
      }
      if (statementDatetimeNames.has(name) && args.length !== 0) {
        throw new TypeError(`${name} takes no arguments`);
      }
      if (
        (name === "JSON_VALUE" || name === "JSON_QUERY" || name === "JSON_EXISTS") &&
        args.length !== 2
      ) {
        throw new TypeError(`${name} requires a JSON document and a path`);
      }
      if (name === "JSON_OBJECT" && args.length % 2 !== 0) {
        throw new TypeError("JSON_OBJECT takes key and value pairs");
      }
      if (name === "IS_JSON") {
        throw new TypeError("Use the IS JSON predicate rather than calling IS_JSON");
      }
      // The path is fixed at compile time, so a malformed one fails here rather than per row.
      if (name === "JSON_VALUE" || name === "JSON_QUERY" || name === "JSON_EXISTS") {
        const path = args[1];
        if (path?.kind === "literal" && typeof path.value === "string") {
          parseJsonPath(path.value, name);
        }
      }
      if (
        (name === "NULLIF" || name === "MOD" || name === "POWER" || name === "INSTR") &&
        args.length !== 2
      ) {
        throw new TypeError(`${name} requires exactly two arguments`);
      }
      if ((name === "GREATEST" || name === "LEAST") && args.length < 1) {
        throw new TypeError(`${name} requires at least one argument`);
      }
      if (name === "REPLACE" && args.length !== 3) {
        throw new TypeError("REPLACE requires a string, a search, and a replacement");
      }
      if (name === "SUBSTR" && (args.length < 2 || args.length > 3))
        throw new TypeError("SUBSTR requires a string, a start, and an optional length");
      if (name === "DATE_TRUNC") {
        if (args.length !== 2)
          throw new TypeError("DATE_TRUNC requires a unit and a datetime argument");
        const unit = args[0];
        if (
          unit?.kind === "literal" &&
          typeof unit.value === "string" &&
          !dateTruncUnits.has(unit.value.toLowerCase())
        ) {
          throw new TypeError(`Unsupported DATE_TRUNC unit: ${unit.value}`);
        }
      }
      if (aggregateNames.has(name as AggregateName) && this.#isKeyword("FILTER")) {
        // FILTER (WHERE cond) desugars into the aggregate's argument: rows failing the filter
        // contribute NULL, which every aggregate skips — COUNT(*) counts a CASE over 1.
        this.#keyword("FILTER");
        this.#expectPunctuation("(");
        this.#keyword("WHERE");
        const condition = this.#expression();
        this.#expectPunctuation(")");
        if (hasAggregate(condition)) {
          throw new TypeError("FILTER conditions cannot contain aggregates");
        }
        const argument = args[0];
        const kept: Expression =
          argument === undefined || argument.kind === "wildcard"
            ? { kind: "literal", value: 1 }
            : argument;
        args.splice(0, args.length, {
          kind: "case",
          branches: [{ when: condition, then: kept }],
        });
      }
      if (aggregateNames.has(name as AggregateName) && this.#isKeyword("OVER")) {
        if (distinct) throw new TypeError("DISTINCT window aggregates are not supported");
        this.#keyword("OVER");
        const { partitionBy, orderBy, frame } = this.#overClause();
        const argument = args[0];
        // An aggregate inside a window argument — SUM(SUM(total)) OVER (PARTITION BY region) —
        // is the running total of a grouped result, and legal wherever the block is grouped.
        // The parser cannot see the GROUP BY yet, so the block assembler decides.
        return {
          kind: "window",
          name: name as AggregateName,
          partitionBy,
          orderBy,
          ...(argument === undefined || argument.kind === "wildcard" ? {} : { argument }),
          ...(frame === undefined ? {} : { frame }),
        };
      }
      return { kind: "call", name, arguments: args, ...(distinct ? { distinct: true } : {}) };
    }
    let reference = identifier;
    if (this.#punctuation(".")) {
      // E051-07: `alias.*` selects one source's columns.
      if (this.#peek().kind === "operator" && this.#peek().text === "*") {
        this.#index += 1;
        return { kind: "wildcard", table: identifier };
      }
      reference += `.${this.#identifier()}`;
    }
    return { kind: "column", reference };
  }

  /** The parenthesized column set of a full-text expression: `(*)` or `(col[, col...])`. */
  #ftsColumns(): Expression[] | "*" {
    this.#expectPunctuation("(");
    if (this.#peek().text === "*") {
      this.#index += 1;
      this.#expectPunctuation(")");
      return "*";
    }
    const columns: Expression[] = [];
    for (;;) {
      let reference = this.#identifier();
      if (this.#punctuation(".")) reference += `.${this.#identifier()}`;
      columns.push({ kind: "column", reference });
      if (!this.#punctuation(",")) break;
    }
    this.#expectPunctuation(")");
    return columns;
  }

  /**
   * VAR_POP/VAR_SAMP/STDDEV_POP/STDDEV_SAMP, built from the aggregates the executors already
   * have: the variance is E(x²) − E(x)², so one pass over SUM(x), SUM(x·x) and COUNT(x) gives
   * every form. Both executors and the vectorized group state stay untouched, and the NULL
   * rules fall out — COUNT skips NULLs, and a sample variance of one row divides by zero,
   * which this engine reads as NULL.
   */
  #statisticalAggregate(name: string): Expression {
    // The opening parenthesis is already consumed by the call dispatch.
    const argument = this.#expression();
    this.#expectPunctuation(")");
    if (hasAggregate(argument)) {
      throw new TypeError(`${name} cannot take another aggregate as its argument`);
    }
    const aggregate = (fn: AggregateName, operand: Expression): Expression => ({
      kind: "call",
      name: fn,
      arguments: [structuredClone(operand)],
    });
    const squares = aggregate("SUM", {
      kind: "binary",
      operator: "*",
      left: structuredClone(argument),
      right: structuredClone(argument),
    });
    const total = aggregate("SUM", argument);
    const count = aggregate("COUNT", argument);
    // sum(x²) − sum(x)² / n
    const spread: Expression = {
      kind: "binary",
      operator: "-",
      left: squares,
      right: {
        kind: "binary",
        operator: "/",
        left: {
          kind: "binary",
          operator: "*",
          left: total,
          right: structuredClone(total),
        },
        right: structuredClone(count),
      },
    };
    // Bare VARIANCE and STDDEV are the sample forms, as in PostgreSQL.
    const sample = name !== "VAR_POP" && name !== "STDDEV_POP";
    const divisor: Expression = sample
      ? {
          kind: "binary",
          operator: "-",
          left: structuredClone(count),
          right: { kind: "literal", value: 1 },
        }
      : structuredClone(count);
    const variance: Expression = { kind: "binary", operator: "/", left: spread, right: divisor };
    if (name.startsWith("VAR")) return variance;
    return { kind: "call", name: "SQRT", arguments: [variance] };
  }

  /**
   * EVERY/BOOL_AND and BOOL_OR over a boolean argument: the extremes of the argument read as
   * 0 or 1 answer both, so they reuse MIN and MAX rather than adding an accumulator.
   */
  #booleanAggregate(name: string): Expression {
    const argument = this.#expression();
    this.#expectPunctuation(")");
    if (hasAggregate(argument)) {
      throw new TypeError(`${name} cannot take another aggregate as its argument`);
    }
    const indicator: Expression = {
      kind: "case",
      branches: [{ when: argument, then: { kind: "literal", value: 1 } }],
      otherwise: { kind: "literal", value: 0 },
    };
    return {
      kind: "condition",
      operator: "=",
      left: {
        kind: "call",
        name: name === "BOOL_OR" ? "MAX" : "MIN",
        arguments: [indicator],
      },
      right: { kind: "literal", value: 1 },
    };
  }

  /**
   * JSON_OBJECT (T811) in both spellings: the standard's `KEY 'k' VALUE v` (with KEY optional)
   * and the flat `'k', v` pair list. Both become one call whose arguments alternate key and
   * value, so the evaluator has a single shape to read.
   */
  #jsonObject(): Expression {
    const args: Expression[] = [];
    if (!this.#punctuation(")")) {
      for (;;) {
        if (this.#isKeyword("KEY")) this.#keyword("KEY");
        args.push(this.#expression());
        if (this.#isKeyword("VALUE")) {
          this.#keyword("VALUE");
          args.push(this.#expression());
        } else {
          this.#expectPunctuation(",");
          args.push(this.#expression());
        }
        if (!this.#punctuation(",")) break;
      }
      this.#expectPunctuation(")");
    }
    return { kind: "call", name: "JSON_OBJECT", arguments: args };
  }

  #overClause(): {
    partitionBy: Expression[];
    orderBy: Array<{ expression: Expression; direction: "asc" | "desc"; nulls?: "first" | "last" }>;
    frame?: WindowFrame;
  } {
    if (this.#peek().kind === "identifier") {
      // T620: OVER w re-enters the definition the WINDOW clause gave that name.
      const name = this.#identifier();
      const definition = this.#namedWindows.get(name);
      if (definition === undefined) throw new TypeError(`Unknown window name: ${name}`);
      const resume = this.#index;
      this.#index = definition.start;
      try {
        return this.#overClause();
      } finally {
        this.#index = resume;
      }
    }
    this.#expectPunctuation("(");
    const partitionBy: Expression[] = [];
    if (this.#isKeyword("PARTITION")) {
      this.#keyword("PARTITION");
      this.#keyword("BY");
      partitionBy.push(...this.#expressionList());
    }
    const orderBy = this.#orderByClause();
    let frame: WindowFrame | undefined;
    if (this.#isKeyword("ROWS") || this.#isKeyword("RANGE") || this.#isKeyword("GROUPS")) {
      const unit = this.#isKeyword("ROWS") ? "rows" : this.#isKeyword("RANGE") ? "range" : "groups";
      this.#keyword(unit.toUpperCase());
      let start: WindowFrameBound;
      let end: WindowFrameBound;
      if (this.#isKeyword("BETWEEN")) {
        this.#keyword("BETWEEN");
        start = this.#frameBound(unit);
        this.#keyword("AND");
        end = this.#frameBound(unit);
      } else {
        // The single-bound shorthand: the bound is the frame start, the end is the current row.
        start = this.#frameBound(unit);
        end = { kind: "current-row" };
      }
      if (start.kind === "unbounded-following" || end.kind === "unbounded-preceding") {
        throw new TypeError("Window frame bounds are reversed");
      }
      if (unit === "groups" && orderBy.length === 0) {
        throw new TypeError("GROUPS frames require ORDER BY inside OVER (...)");
      }
      let exclude: WindowFrameExclusion | undefined;
      if (this.#isKeyword("EXCLUDE")) {
        this.#keyword("EXCLUDE");
        if (this.#isKeyword("CURRENT")) {
          this.#keyword("CURRENT");
          this.#keyword("ROW");
          exclude = "current-row";
        } else if (this.#isKeyword("GROUP")) {
          this.#keyword("GROUP");
          exclude = "group";
        } else if (this.#isKeyword("TIES")) {
          this.#keyword("TIES");
          exclude = "ties";
        } else {
          this.#keyword("NO");
          this.#keyword("OTHERS");
          exclude = "no-others";
        }
      }
      frame = {
        unit,
        start,
        end,
        ...(exclude === undefined || exclude === "no-others" ? {} : { exclude }),
      };
    }
    this.#expectPunctuation(")");
    return { partitionBy, orderBy, ...(frame === undefined ? {} : { frame }) };
  }

  #frameBound(unit: "rows" | "range" | "groups"): WindowFrameBound {
    if (this.#isKeyword("UNBOUNDED")) {
      this.#keyword("UNBOUNDED");
      if (this.#isKeyword("PRECEDING")) {
        this.#keyword("PRECEDING");
        return { kind: "unbounded-preceding" };
      }
      this.#keyword("FOLLOWING");
      return { kind: "unbounded-following" };
    }
    if (this.#isKeyword("CURRENT")) {
      this.#keyword("CURRENT");
      this.#keyword("ROW");
      return { kind: "current-row" };
    }
    const offset = Number(this.#take("number").text);
    if (!Number.isInteger(offset) || offset < 0) {
      throw new TypeError("Window frame offsets must be non-negative integers");
    }
    if (unit === "range") {
      throw new TypeError("RANGE frames take only UNBOUNDED and CURRENT ROW bounds; use ROWS");
    }
    if (this.#isKeyword("PRECEDING")) {
      this.#keyword("PRECEDING");
      return { kind: "preceding", offset };
    }
    this.#keyword("FOLLOWING");
    return { kind: "following", offset };
  }

  #comparison(): ComparisonOperator {
    const token = this.#take("operator");
    if (!["=", "!=", "<>", ">", ">=", "<", "<="].includes(token.text))
      throw new TypeError(`Expected comparison operator, found ${token.text}`);
    return token.text as ComparisonOperator;
  }

  #identifier(): string {
    return this.#take("identifier").text;
  }

  #isKeyword(value: string): boolean {
    const token = this.#peek();
    return (
      token.kind === "identifier" && token.quoted !== true && token.text.toUpperCase() === value
    );
  }

  #keyword(value: string): void {
    if (!this.#isKeyword(value))
      throw new TypeError(`Expected ${value}, found ${this.#peek().text || "end of query"}`);
    this.#index += 1;
  }

  #operator(value: string): void {
    const token = this.#take("operator");
    if (token.text !== value) throw new TypeError(`Expected ${value}, found ${token.text}`);
  }

  #punctuation(value: string): boolean {
    const token = this.#peek();
    if (token.kind !== "punctuation" || token.text !== value) return false;
    this.#index += 1;
    return true;
  }

  #expectPunctuation(value: string): void {
    if (!this.#punctuation(value)) throw new TypeError(`Expected ${value}`);
  }

  #take(kind: Token["kind"]): Token {
    const token = this.#peek();
    if (token.kind !== kind)
      throw new TypeError(`Expected ${kind}, found ${token.text || "end of query"}`);
    this.#index += 1;
    return token;
  }

  #peek(): Token {
    const last = this.tokens[this.tokens.length - 1];
    const end = last === undefined ? 0 : last.end;
    return this.tokens[this.#index] ?? { kind: "eof", text: "", start: end, end };
  }

  /**
   * Span of the token the parser is positioned on. Syntax errors throw while sitting on the token
   * that failed, so this is where the failure belongs; errors raised after a block finishes
   * parsing land on the token that follows it.
   */
  get span(): { start: number; end: number } {
    const { start, end } = this.#peek();
    return { start, end };
  }
}

// --- Shared select-block assembly ---------------------------------------------------------------
//
// The SQL parser and the typed query builder both end at these functions, so a builder query and
// its equivalent SQL produce byte-identical plans: the same validation errors, the same DISTINCT /
// COUNT(DISTINCT) / window desugars, and the same derived-source numbering (callers thread one
// sequence counter through every source they create, in parse order).

export interface SelectBlockParts {
  sql: string;
  base: TableSource;
  joins: JoinPlan[];
  select: SelectItem[];
  distinct: boolean;
  predicates: Predicate[];
  /** Raw GROUP BY expressions; DISTINCT desugaring appends to a copy here. */
  groupBy: Expression[];
  having: Predicate[];
  orderBy: CompiledQuery["orderBy"];
  limit?: number;
  offset?: number;
  limitParameter?: number;
  offsetParameter?: number;
  limitWithTies?: boolean;
  /** GROUP BY GROUPING SETS/ROLLUP/CUBE: the grouping lists to union; groupBy is then unused. */
  groupingSets?: Expression[][];
}

/**
 * GROUPING SETS desugar: one grouped block per set, combined with UNION ALL. A grouped column
 * absent from a member's set projects as NULLIF(expr, expr) — always NULL, but carrying the
 * expression's type through schema inference. The GROUPING() marker function is deliberately
 * unsupported, so rollup NULLs and data NULLs are indistinguishable; the docs call this out.
 */
function desugarGroupingSets(parts: SelectBlockParts, nextSequence: () => number): CompiledQuery {
  const { groupingSets, limit, offset, limitParameter, offsetParameter, ...blockParts } = parts;
  const sets = groupingSets ?? [];
  if (parts.distinct) {
    throw new TypeError("GROUPING SETS cannot be combined with SELECT DISTINCT");
  }
  for (const predicate of parts.having) {
    for (const side of [predicate.left, predicate.right]) {
      if (!hasAggregate(side) && expressionColumns(side).length > 0) {
        throw new TypeError(
          "HAVING with GROUPING SETS supports aggregate and literal conditions only",
        );
      }
    }
  }
  const signatureOf = (expression: Expression): string => JSON.stringify(expression);
  const universe = new Set(sets.flat().map(signatureOf));
  const members = sets.map((set) => {
    const setSignatures = new Set(set.map(signatureOf));
    /**
     * T433: GROUPING(a, b, ...) is a bitmask saying which of its arguments this grouping set
     * aggregated away — constant per member block, so it folds to a literal here rather than
     * needing an executor of its own. The most significant bit is the first argument.
     */
    const resolveGrouping = (expression: Expression): Expression => {
      if (expression.kind === "call" && expression.name === "GROUPING") {
        if (expression.arguments.length === 0) {
          throw new TypeError("GROUPING requires at least one grouping column");
        }
        let mask = 0;
        for (const argument of expression.arguments) {
          const signature = signatureOf(argument);
          if (!universe.has(signature)) {
            throw new TypeError("GROUPING arguments must appear in the GROUP BY clause");
          }
          mask = mask * 2 + (setSignatures.has(signature) ? 0 : 1);
        }
        // MIN over the constant, not the bare constant: the mask is the same either way, and
        // being an aggregate keeps the member block grouped. A literal-only select list over
        // the empty grouping set would otherwise degenerate into one row per input row.
        return {
          kind: "call",
          name: "MIN",
          arguments: [{ kind: "literal", value: mask }],
        };
      }
      return mapChildExpressions(expression, resolveGrouping);
    };
    const select = blockParts.select
      .map((item) => ({
        alias: item.alias,
        expression: resolveGrouping(item.expression),
      }))
      .map((item) => {
        if (hasAggregate(item.expression)) return item;
        const signature = signatureOf(item.expression);
        if (setSignatures.has(signature) || !universe.has(signature)) return item;
        // MIN over an always-NULL argument: legal in a grouped select, NULL in every group, and
        // typed like the original expression through MIN's carry and NULLIF's first argument.
        return {
          alias: item.alias,
          expression: {
            kind: "call",
            name: "MIN",
            arguments: [
              {
                kind: "call",
                name: "NULLIF",
                arguments: [structuredClone(item.expression), structuredClone(item.expression)],
              },
            ],
          } satisfies Expression,
        };
      });
    return assembleSelectBlock(
      {
        ...blockParts,
        sql: "(grouping set)",
        select,
        groupBy: structuredClone(set),
        orderBy: [],
      },
      nextSequence,
    );
  });
  const first = members[0];
  if (members.length === 1 && first !== undefined) return first;
  return compoundSelectBlock(
    parts.sql,
    members,
    members.slice(1).map(() => "union all" as const),
    {
      orderBy: parts.orderBy,
      ...(limit === undefined ? {} : { limit }),
      ...(offset === undefined ? {} : { offset }),
      ...(limitParameter === undefined ? {} : { limitParameter }),
      ...(offsetParameter === undefined ? {} : { offsetParameter }),
    },
    nextSequence,
  );
}

/** Validates and assembles one select block, applying every parser desugar. */
/** Resolves ORDER BY ordinals (ORDER BY 2) to the matching select item's output alias. */
function resolveOrderByOrdinals(
  orderBy: CompiledQuery["orderBy"],
  select: readonly SelectItem[],
): CompiledQuery["orderBy"] {
  return orderBy.map((order) => {
    if (order.expression.kind !== "literal" || typeof order.expression.value !== "number") {
      return order;
    }
    const ordinal = order.expression.value;
    if (select.some((item) => item.expression.kind === "wildcard")) {
      throw new TypeError("ORDER BY ordinals cannot be used with SELECT *");
    }
    const item = Number.isInteger(ordinal) ? select[ordinal - 1] : undefined;
    if (item === undefined) {
      throw new TypeError(`ORDER BY ordinal is out of range: ${String(ordinal)}`);
    }
    return { ...order, expression: { kind: "column" as const, reference: item.alias } };
  });
}

/**
 * FULL OUTER JOIN desugars into UNION ALL of two left joins: the plain left join carries every
 * base row (matched or not), and the swapped left join filtered to a NULL-extended base side
 * carries the joined source's unmatched rows. Sound because an equality join key never matches
 * NULL, so "base side IS NULL" identifies exactly the null-extended rows.
 */
function desugarFullJoin(parts: SelectBlockParts, nextSequence: () => number): CompiledQuery {
  const join = parts.joins[0];
  if (parts.joins.length !== 1 || join?.full !== true) {
    throw new TypeError("FULL JOIN is only supported as the sole join");
  }
  if (parts.select.some((item) => item.expression.kind === "wildcard")) {
    throw new TypeError("FULL JOIN cannot be combined with SELECT *");
  }
  if (
    parts.groupBy.length > 0 ||
    parts.having.length > 0 ||
    parts.distinct ||
    parts.select.some((item) => hasAggregate(item.expression) || containsWindow(item.expression))
  ) {
    throw new TypeError(
      "FULL JOIN cannot be combined with grouping, DISTINCT, or window functions yet",
    );
  }
  if (join.on !== undefined) {
    throw new TypeError("FULL JOIN requires a single equality ON condition");
  }
  // `on` is absent here: the guard above rejected non-equi conditions.
  const { full, kind, left, right, ...joinSource } = join;
  void full;
  void kind;
  const { limit, offset, limitParameter, offsetParameter, ...blockParts } = parts;
  const baseSide = expressionAliases(left).has(join.alias) ? right : left;
  const matched = assembleSelectBlock(
    {
      ...blockParts,
      sql: "(full join left)",
      joins: [{ ...joinSource, kind: "left", left, right }],
      orderBy: [],
    },
    nextSequence,
  );
  const unmatchedRight = assembleSelectBlock(
    {
      ...blockParts,
      sql: "(full join right)",
      base: joinSource,
      joins: [{ ...parts.base, kind: "left", left, right }],
      predicates: [
        ...parts.predicates,
        { left: baseSide, operator: "IS NULL", right: { kind: "literal", value: null } },
      ],
      orderBy: [],
    },
    nextSequence,
  );
  return compoundSelectBlock(
    parts.sql,
    [matched, unmatchedRight],
    ["union all"],
    {
      orderBy: parts.orderBy,
      ...(limit === undefined ? {} : { limit }),
      ...(offset === undefined ? {} : { offset }),
      ...(limitParameter === undefined ? {} : { limitParameter }),
      ...(offsetParameter === undefined ? {} : { offsetParameter }),
    },
    nextSequence,
  );
}

export function assembleSelectBlock(
  parts: SelectBlockParts,
  nextSequence: () => number,
): CompiledQuery {
  parts = { ...parts, orderBy: resolveOrderByOrdinals(parts.orderBy, parts.select) };
  if (parts.joins.some((join) => join.full === true)) {
    return desugarFullJoin(parts, nextSequence);
  }
  if (parts.groupingSets !== undefined && parts.groupingSets.length > 0) {
    return desugarGroupingSets(parts, nextSequence);
  }
  if (parts.select.some((item) => containsGrouping(item.expression))) {
    // Only the grouping-sets desugar can answer GROUPING, and it has already run above.
    throw new TypeError("GROUPING requires GROUP BY ROLLUP, CUBE, or GROUPING SETS");
  }
  if (parts.orderBy.some((order) => orderNeedsHiddenColumn(order.expression, parts))) {
    return assembleOrderByExpressionBlock(parts, nextSequence);
  }
  const { sql, base, joins, select, distinct, predicates, having, orderBy, limit, offset } = parts;
  const { limitParameter, offsetParameter } = parts;
  const groupBy = [...parts.groupBy];
  let distinctWildcard = false;
  if (distinct) {
    if (select.some((item) => hasAggregate(item.expression)))
      throw new TypeError("SELECT DISTINCT cannot be combined with aggregate functions");
    if (groupBy.length > 0) throw new TypeError("SELECT DISTINCT cannot be combined with GROUP BY");
    if (having.length > 0) throw new TypeError("SELECT DISTINCT cannot be combined with HAVING");
    if (select.some((item) => item.expression.kind === "wildcard")) {
      // The wildcard's columns are unknown until input schemas exist; executor entries
      // expand the flag into a concrete select list plus GROUP BY (expandDistinctWildcard).
      distinctWildcard = true;
    } else {
      // DISTINCT is grouping by every selected expression, so it reuses the grouped executor,
      // its value-carrying spill, and streamed scan inputs unchanged.
      groupBy.push(...select.map((item) => item.expression));
    }
  }
  if (having.length > 0) {
    const grouped =
      parts.groupBy.length > 0 || select.some((item) => hasAggregate(item.expression));
    if (!grouped) throw new TypeError("HAVING requires GROUP BY or aggregate functions");
    const groupExpressions = new Set(groupBy.map((expression) => JSON.stringify(expression)));
    for (const predicate of having) {
      for (const side of [predicate.left, predicate.right]) {
        if (hasAggregate(side)) continue;
        if (expressionColumns(side).length === 0) continue;
        if (groupExpressions.has(JSON.stringify(side))) continue;
        throw new TypeError(
          "HAVING conditions must use aggregates, literals, or GROUP BY expressions",
        );
      }
    }
  }
  const clauseExpressions = [
    ...predicates.flatMap((predicate) => [predicate.left, predicate.right]),
    ...groupBy,
    ...having.flatMap((predicate) => [predicate.left, predicate.right]),
    ...orderBy.map((order) => order.expression),
  ];
  if (clauseExpressions.some(containsWindow)) {
    throw new TypeError("Window functions are only allowed in the select list");
  }
  // A DISTINCT aggregate is an aggregate: it belongs where any other one does — anywhere in the
  // select list, including inside arithmetic — and nowhere an aggregate cannot go.
  if (
    predicates.some((predicate) => [predicate.left, predicate.right].some(containsDistinctCount))
  ) {
    throw new TypeError("DISTINCT aggregates are not allowed in WHERE");
  }
  if (groupBy.some(containsDistinctCount)) {
    throw new TypeError("DISTINCT aggregates are not allowed in GROUP BY");
  }
  const tail = {
    orderBy,
    ...(limit === undefined ? {} : { limit }),
    ...(offset === undefined ? {} : { offset }),
    ...(limitParameter === undefined ? {} : { limitParameter }),
    ...(offsetParameter === undefined ? {} : { offsetParameter }),
    ...(parts.limitWithTies === true ? { limitWithTies: true } : {}),
  };
  if (select.some((item) => containsWindow(item.expression))) {
    return desugarWindows(
      sql,
      base,
      joins,
      select,
      predicates,
      groupBy,
      having,
      tail,
      nextSequence,
    );
  }
  return {
    sql,
    base,
    joins,
    select,
    predicates,
    groupBy,
    having,
    ...(distinctWildcard ? { distinctWildcard: true } : {}),
    ...tail,
  };
}

/**
 * Expands a pending SELECT DISTINCT * against known input columns: the select list becomes
 * every wildcard output (alias-qualified when more than one source contributes), and GROUP BY
 * over those same columns provides the deduplication through the grouped executor. Runs
 * exactly once per execution entry, like MATCH(*) expansion.
 */
export function expandDistinctWildcard(
  plan: CompiledQuery,
  columnsOf: (tableName: string) => readonly string[] | undefined,
): CompiledQuery {
  if (plan.distinctWildcard !== true) return plan;
  const sources = [plan.base, ...plan.joins];
  const multiple = sources.length > 1;
  const select: SelectItem[] = sources.flatMap((source) => {
    const columns = columnsOf(source.table);
    if (columns === undefined || columns.length === 0) {
      throw new TypeError(`SELECT DISTINCT * requires known columns for: ${source.table}`);
    }
    return columns.map((name) => {
      const output = multiple ? `${source.alias}.${name}` : name;
      return { expression: { kind: "column" as const, reference: output }, alias: output };
    });
  });
  const { distinctWildcard, ...rest } = plan;
  void distinctWildcard;
  return { ...rest, select, groupBy: select.map((item) => item.expression) };
}

/**
 * Turns every source carrying a column alias list into a derived projection that renames the
 * table's columns positionally (E051-09). The table's own column order is only known here, so
 * the parser records the names and this pass — one per execution entry — applies them.
 */
/** Whether any block of the plan reads a table name the catalog answers with a view. */
export function planReadsViews(plan: CompiledQuery, isView: (name: string) => boolean): boolean {
  if (
    [plan.base, ...plan.joins].some(
      (source) => source.derived === undefined && isView(source.table),
    )
  )
    return true;
  let nested = false;
  forEachNestedBlock(plan, (inner) => {
    nested ||= planReadsViews(inner, isView);
  });
  return nested;
}

/**
 * Replaces every reference to a view with the query it stands for, as a derived table under the
 * reference's own alias (F031-02). A view whose body reads another view expands too, up to a
 * depth that stops a cycle from recursing forever — a view cannot be defined in terms of itself,
 * but two views can be redefined into a loop after the fact.
 */
export function expandViewSources(
  plan: CompiledQuery,
  viewFor: (tableName: string) => CompiledQuery | undefined,
  maxDepth = 16,
): CompiledQuery {
  const isView = (name: string): boolean => viewFor(name) !== undefined;
  if (!planReadsViews(plan, isView)) return plan;
  let sequence = 0;
  const nextSequence = (): number => {
    sequence += 1;
    return sequence;
  };
  const expandBlock = (block: CompiledQuery, depth: number): void => {
    if (depth > maxDepth) {
      throw new TypeError("A view cannot be defined in terms of itself");
    }
    for (const source of [block.base, ...block.joins]) {
      if (source.derived !== undefined || source.union !== undefined) continue;
      const body = viewFor(source.table);
      if (body === undefined) continue;
      const expanded = structuredClone(body);
      expandBlock(expanded, depth + 1);
      Object.assign(source, derivedTableSource(expanded, source.alias, nextSequence));
    }
    forEachNestedBlock(block, (inner) => {
      expandBlock(inner, depth);
    });
  };
  const resolved = structuredClone(plan);
  expandBlock(resolved, 0);
  return resolved;
}

export function planHasSourceColumnAliases(plan: CompiledQuery): boolean {
  if ([plan.base, ...plan.joins].some((source) => source.columnAliases !== undefined)) return true;
  let nested = false;
  forEachNestedBlock(plan, (inner) => {
    nested ||= planHasSourceColumnAliases(inner);
  });
  return nested;
}

/**
 * FETCH FIRST n ROWS WITH TIES (F866). The plan runs without its limit — a limit pushed into a
 * scan cannot know whether the next row ties — and the ordered result is trimmed here: rows up
 * to the limit, plus every following row equal to the last one on all ORDER BY columns.
 */
export function withTiesPlan(plan: CompiledQuery): {
  plan: CompiledQuery;
  trim: (result: QueryResult) => QueryResult;
} {
  // Ordering by an expression or an unselected column wraps the real block in a projection that
  // hides the sort column. The tie test needs that column, so the inner block runs and the
  // projection is applied after trimming instead of before.
  const wrapper = transparentProjectionSource(plan);
  if (wrapper?.inner.limitWithTies === true) {
    const inner = withTiesPlan(wrapper.inner);
    return {
      plan: inner.plan,
      trim: (result) => projectResultColumns(inner.trim(result), wrapper.aliases),
    };
  }
  const limit = plan.limit;
  if (plan.limitWithTies !== true || limit === undefined) return { plan, trim: (result) => result };
  if (plan.orderBy.length === 0) throw new TypeError("FETCH ... WITH TIES requires ORDER BY");
  const sources = [plan.base, ...plan.joins].map((source) => ({
    alias: source.alias,
    columns: source.derived?.select.map((item) => item.alias) ?? [],
  }));
  // orderOutputName throws when a sort key has no output column, which is the same failure the
  // executors report; nothing here has to re-check it.
  const keys = plan.orderBy.map(({ expression }) =>
    orderOutputName(expression, plan.select, sources),
  );
  const unlimited = { ...plan };
  delete unlimited.limit;
  delete unlimited.limitWithTies;
  return {
    plan: unlimited,
    trim: (result) => {
      if (result.rows.length <= limit) return result;
      const last = result.rows[limit - 1];
      let end = limit;
      while (
        end < result.rows.length &&
        keys.every((key) => comparable(result.rows[end]?.[key]) === comparable(last?.[key]))
      ) {
        end += 1;
      }
      return { ...result, rows: result.rows.slice(0, end) };
    },
  };
}

/**
 * The output columns one source contributes to a wildcard, in declaration order: a derived
 * table's own aliases, a set operation's first member's, and otherwise the input table's.
 */
function sourceWildcardColumns(
  source: TableSource,
  columnsOf: (tableName: string) => readonly string[] | undefined,
): readonly string[] | undefined {
  if (source.derived !== undefined) return source.derived.select.map((item) => item.alias);
  if (source.union !== undefined) {
    return source.union.blocks[0]?.select.map((item) => item.alias);
  }
  return columnsOf(source.table);
}

/** Whether any block of the plan still carries an unresolved NATURAL join marker. */
export function planHasNaturalJoins(plan: CompiledQuery): boolean {
  if (plan.joins.some((join) => join.natural === true)) return true;
  let nested = false;
  forEachNestedBlock(plan, (inner) => {
    nested ||= planHasNaturalJoins(inner);
  });
  return nested;
}

/**
 * Resolves NATURAL joins into ordinary equality conditions (F401-01): the join columns are the
 * names the new source shares with the sources already joined, compared in the order they are
 * declared. A pair with no shared column is a cross join, which is what the standard says.
 */
export function expandNaturalJoins(
  plan: CompiledQuery,
  columnsOf: (tableName: string) => readonly string[] | undefined,
): CompiledQuery {
  if (!planHasNaturalJoins(plan)) return plan;
  const expandBlock = (block: CompiledQuery): void => {
    forEachNestedBlock(block, expandBlock);
    const sources = [block.base, ...block.joins];
    block.joins.forEach((join, index) => {
      if (join.natural !== true) return;
      const columnsFor = (source: TableSource): readonly string[] => {
        const own = sourceWildcardColumns(source, columnsOf);
        if (own === undefined)
          throw new TypeError(`A NATURAL join needs known columns for: ${source.table}`);
        return own;
      };
      const preceding = sources.slice(0, index + 1);
      const shared: Array<{ left: string; name: string }> = [];
      for (const name of columnsFor(join)) {
        const owner = preceding.find((source) => columnsFor(source).includes(name));
        if (owner !== undefined) shared.push({ left: owner.alias, name });
      }
      delete join.natural;
      if (shared.length === 0) {
        // No shared column: the standard's degenerate natural join is a cross join.
        join.left = { kind: "literal", value: 1 };
        join.right = { kind: "literal", value: 1 };
        return;
      }
      const conditions = shared.map<Expression>(({ left, name }) => ({
        kind: "condition",
        operator: "=",
        left: { kind: "column", reference: `${left}.${name}` },
        right: { kind: "column", reference: `${join.alias}.${name}` },
      }));
      const [first] = conditions;
      if (conditions.length === 1 && first?.kind === "condition") {
        join.left = first.left;
        join.right = first.right;
        return;
      }
      join.on = conditions.reduce((accumulated, condition) => ({
        kind: "logical",
        operator: "and",
        left: accumulated,
        right: condition,
      }));
    });
  };
  const expanded = structuredClone(plan);
  expandBlock(expanded);
  return expanded;
}

export function expandSourceColumnAliases(
  plan: CompiledQuery,
  columnsOf: (tableName: string) => readonly string[] | undefined,
): CompiledQuery {
  if (!planHasSourceColumnAliases(plan)) return plan;
  let sequence = 0;
  const nextSequence = (): number => {
    sequence += 1;
    return sequence;
  };
  const rename = (source: TableSource): void => {
    const names = source.columnAliases;
    if (names === undefined) return;
    // A view has already become a derived source by now, and it answers with its own output
    // names rather than a catalog entry — which is what sourceWildcardColumns knows.
    const columns = sourceWildcardColumns(source, columnsOf);
    if (columns === undefined || columns.length === 0) {
      throw new TypeError(`A column alias list requires known columns for: ${source.table}`);
    }
    if (columns.length !== names.length) {
      throw new TypeError(
        `Column alias list must match the table's column count: ${source.table} has ${String(columns.length)}`,
      );
    }
    // The projection reads from the source exactly as it stands — which for an expanded view is
    // a derived block, not a table name — so the rename wraps it rather than replacing it.
    const inner: TableSource = structuredClone(source);
    delete inner.columnAliases;
    const projection: CompiledQuery = {
      sql: "(column aliases)",
      base: inner,
      joins: [],
      select: names.map((name, index) => ({
        expression: { kind: "column" as const, reference: columns[index] ?? name },
        alias: name,
      })),
      predicates: [],
      groupBy: [],
      having: [],
      orderBy: [],
    };
    const wrapper = derivedTableSource(projection, source.alias, nextSequence);
    delete source.columnAliases;
    delete source.union;
    delete source.recursive;
    delete source.windowed;
    source.table = wrapper.table;
    source.derived = projection;
  };
  const renameBlock = (block: CompiledQuery): void => {
    forEachNestedBlock(block, renameBlock);
    for (const source of [block.base, ...block.joins]) rename(source);
  };
  const expanded = structuredClone(plan);
  renameBlock(expanded);
  return expanded;
}

/**
 * Expands `alias.*` select items into that source's columns (E051-07), at every nesting depth.
 * Output names follow the same rule as a bare `*`: the column's own name when the block reads
 * one source, and `alias.column` when it reads several, so two sources cannot collide.
 * Runs once per execution entry, like MATCH(*) and DISTINCT * expansion.
 */
export function expandQualifiedWildcards(
  plan: CompiledQuery,
  columnsOf: (tableName: string) => readonly string[] | undefined,
): CompiledQuery {
  const qualified = (block: CompiledQuery): boolean => {
    if (block.select.some((item) => item.expression.kind === "wildcard" && item.expression.table))
      return true;
    let nested = false;
    forEachNestedBlock(block, (inner) => {
      nested ||= qualified(inner);
    });
    return nested;
  };
  if (!qualified(plan)) return plan;
  const expandBlock = (block: CompiledQuery): void => {
    forEachNestedBlock(block, expandBlock);
    const sources = [block.base, ...block.joins];
    const multiple = sources.length > 1;
    block.select = block.select.flatMap((item) => {
      if (item.expression.kind !== "wildcard" || item.expression.table === undefined) return [item];
      const table = item.expression.table;
      const source = sources.find((candidate) => candidate.alias === table);
      if (source === undefined) throw new TypeError(`Unknown table for ${table}.*: ${table}`);
      const columns = sourceWildcardColumns(source, columnsOf);
      if (columns === undefined || columns.length === 0) {
        throw new TypeError(`${table}.* requires known columns for: ${source.table}`);
      }
      return columns.map((name) => {
        const output = multiple ? `${source.alias}.${name}` : name;
        return { expression: { kind: "column" as const, reference: output }, alias: output };
      });
    });
    const aliases = new Set<string>();
    for (const item of block.select) {
      if (aliases.has(item.alias)) throw new TypeError(`Duplicate output column: ${item.alias}`);
      aliases.add(item.alias);
    }
  };
  const expanded = structuredClone(plan);
  expandBlock(expanded);
  return expanded;
}

/** Wraps compound members into the set-operation source the executor folds left to right. */
export function compoundSelectBlock(
  sql: string,
  blocks: CompiledQuery[],
  ops: SetOperator[],
  tail: SelectTail,
  nextSequence: () => number,
): CompiledQuery {
  // Set-operation output columns are the first member's, so ordinals resolve against them.
  tail = { ...tail, orderBy: resolveOrderByOrdinals(tail.orderBy, blocks[0]?.select ?? []) };
  return {
    sql,
    base: {
      table: `(union ${String(nextSequence())})`,
      alias: "union",
      union: { blocks, ops },
    },
    joins: [],
    select: [{ expression: { kind: "wildcard" }, alias: "*" }],
    predicates: [],
    groupBy: [],
    having: [],
    orderBy: tail.orderBy,
    ...(tail.limit === undefined ? {} : { limit: tail.limit }),
    ...(tail.offset === undefined ? {} : { offset: tail.offset }),
    ...(tail.limitWithTies === true ? { limitWithTies: true } : {}),
  };
}

/**
 * Desugars ORDER BY expressions the way windows already hide their machinery: each expression
 * either reuses a structurally identical select item's alias or becomes a hidden "(order N)"
 * select item, the ordering (and LIMIT/OFFSET) applies inside that block, and an outer block
 * projects only the visible aliases away from a derived source. Runs in the shared assembly,
 * so the builder and SQL front ends produce identical plans. A wildcard select has no named
 * output list to hide items behind, and DISTINCT's output would change if hidden expressions
 * joined its grouping, so both keep the named-column restriction.
 */
/**
 * Whether an ORDER BY item has to travel as a hidden select item: any expression, and also a
 * column the select list does not already carry. The standard lets a query sort by a column of
 * its source that it does not return, and hiding it is how this engine keeps the value
 * available to the sort without returning it.
 */
function orderNeedsHiddenColumn(expression: Expression, parts: SelectBlockParts): boolean {
  if (expression.kind !== "column") return true;
  if (parts.select.some((item) => item.expression.kind === "wildcard")) return false;
  const reference = expression.reference;
  const bare = reference.split(".").at(-1) ?? reference;
  return !parts.select.some(
    (item) =>
      item.alias === reference ||
      item.alias === bare ||
      (item.expression.kind === "column" && item.expression.reference === reference),
  );
}

function assembleOrderByExpressionBlock(
  parts: SelectBlockParts,
  nextSequence: () => number,
): CompiledQuery {
  if (parts.select.some((item) => item.expression.kind === "wildcard")) {
    throw new TypeError(
      "ORDER BY expressions require a named select list; SELECT * orders by column names only",
    );
  }
  if (parts.distinct) {
    throw new TypeError("SELECT DISTINCT orders by selected columns or output aliases only");
  }
  for (const order of parts.orderBy) {
    if (!orderNeedsHiddenColumn(order.expression, parts)) continue;
    if (containsWindow(order.expression)) {
      throw new TypeError("Window functions are only allowed in the select list");
    }
    // A bare literal is almost always a SQL ordinal (ORDER BY 2); sorting by a constant would
    // silently do nothing, so reject it the way the engine always has.
    if (order.expression.kind === "literal") {
      throw new TypeError("ORDER BY ordinals are not supported; name the column or alias");
    }
  }
  const selectSignatures = new Map(
    parts.select.map((item) => [JSON.stringify(item.expression), item.alias] as const),
  );
  const hiddenItems: SelectItem[] = [];
  const rewrittenOrder = parts.orderBy.map((order) => {
    if (!orderNeedsHiddenColumn(order.expression, parts)) return order;
    const existingAlias = selectSignatures.get(JSON.stringify(order.expression));
    if (existingAlias !== undefined) {
      return {
        ...order,
        expression: { kind: "column", reference: existingAlias } satisfies Expression,
      };
    }
    const alias = `(order ${String(hiddenItems.length + 1)})`;
    hiddenItems.push({ expression: order.expression, alias });
    return {
      ...order,
      expression: { kind: "column", reference: alias } satisfies Expression,
    };
  });
  const inner = assembleSelectBlock(
    { ...parts, select: [...parts.select, ...hiddenItems], orderBy: rewrittenOrder },
    nextSequence,
  );
  // Every ordering expression matched a visible select item — no hidden columns, no wrap.
  if (hiddenItems.length === 0) return inner;
  const source = derivedTableSource(inner, "(ordered)", nextSequence);
  return {
    sql: parts.sql,
    base: source,
    joins: [],
    select: parts.select.map((item) => ({
      expression: { kind: "column", reference: item.alias } satisfies Expression,
      alias: item.alias,
    })),
    predicates: [],
    groupBy: [],
    having: [],
    orderBy: [],
  };
}

/**
 * Detects a pure projection wrapper over one derived block — the shape the ORDER-BY-expression
 * desugar emits: no joins, filters, grouping, ordering, or paging of its own, and every select
 * item passing an inner output alias through under the same name. Whole-plan strategies (the
 * streamed scan today) run the inner block and project its result, so a hidden ordering column
 * never changes which execution paths a query is eligible for — `.search()` performs the same
 * whether or not the caller also selects the score.
 */
export function transparentProjectionSource(
  plan: CompiledQuery,
): { inner: CompiledQuery; aliases: string[] } | undefined {
  if (
    plan.joins.length > 0 ||
    plan.predicates.length > 0 ||
    plan.groupBy.length > 0 ||
    plan.having.length > 0 ||
    plan.orderBy.length > 0 ||
    plan.limit !== undefined ||
    plan.offset !== undefined
  ) {
    return undefined;
  }
  const inner = plan.base.derived;
  if (
    inner === undefined ||
    plan.base.union !== undefined ||
    plan.base.windowed !== undefined ||
    plan.base.recursive !== undefined
  ) {
    return undefined;
  }
  if (inner.select[0]?.expression.kind === "wildcard") return undefined;
  const innerAliases = new Set(inner.select.map((item) => item.alias));
  const aliases: string[] = [];
  for (const item of plan.select) {
    if (
      item.expression.kind !== "column" ||
      item.expression.reference !== item.alias ||
      !innerAliases.has(item.alias)
    ) {
      return undefined;
    }
    aliases.push(item.alias);
  }
  if (aliases.length === 0) return undefined;
  return { inner, aliases };
}

/** Projects a result to a wrapper's visible aliases, preserving row order. */
export function projectResultColumns(result: QueryResult, aliases: readonly string[]): QueryResult {
  return {
    columns: [...aliases],
    rows: result.rows.map((row) => {
      const projected: QueryRow = {};
      for (const alias of aliases) projected[alias] = row[alias] ?? null;
      return projected;
    }),
  };
}

/** Names a derived (subquery or expanded CTE) source under the shared sequence. */
export function derivedTableSource(
  derived: CompiledQuery,
  alias: string,
  nextSequence: () => number,
): TableSource {
  return { table: `(derived ${String(nextSequence())}) ${alias}`, alias, derived };
}

/** The parser's LIMIT range contract, shared with the typed builder. */
export function validateLimit(limit: number): number {
  if (!Number.isSafeInteger(limit) || limit < 1 || limit > 100_000)
    throw new RangeError("LIMIT must be between 1 and 100,000");
  return limit;
}

/**
 * Applies a CTE's declared column names to the block that fills it. The names are positional, so
 * the count has to match; a wildcard has no names to replace until its input schema is known,
 * which is later than this.
 */
function renameBlockOutputs(block: CompiledQuery, columns: readonly string[], name: string): void {
  if (block.select.some((item) => item.expression.kind === "wildcard")) {
    throw new TypeError(`A column list needs named columns in the CTE body: ${name}`);
  }
  if (block.select.length !== columns.length) {
    throw new TypeError(
      `CTE ${name} declares ${String(columns.length)} columns but selects ${String(block.select.length)}`,
    );
  }
  block.select = block.select.map((item, index) => ({
    ...item,
    alias: columns[index] ?? item.alias,
  }));
}

/**
 * `TIMESTAMP '2026-01-02 03:04:05'` — the standard's spelling, which writes a space where ISO
 * writes a T and leaves the time off entirely for midnight. A literal without a zone is UTC, the
 * same reading `DATE` already takes and the same one every datetime in a Minnow database has.
 */
export function timestampLiteral(text: string): Date {
  const trimmed = text.trim();
  const match =
    /^(\d{4}-\d{2}-\d{2})(?:[ T](\d{2}:\d{2}(?::\d{2}(?:\.\d{1,3})?)?))?(Z|[+-]\d{2}:?\d{2})?$/.exec(
      trimmed,
    );
  if (match === null) throw new TypeError(`Invalid TIMESTAMP literal: ${text}`);
  const [, day, time = "00:00:00", zone = "Z"] = match;
  const seconds = time.length === 5 ? `${time}:00` : time;
  const date = new Date(`${String(day)}T${seconds}${zone === "Z" ? "Z" : zone}`);
  if (!Number.isFinite(date.getTime())) throw new TypeError(`Invalid TIMESTAMP literal: ${text}`);
  return date;
}

/** The parser's OFFSET range contract, shared with the typed builder. */
export function validateOffset(offset: number): number {
  if (!Number.isSafeInteger(offset) || offset < 0 || offset > 100_000_000)
    throw new RangeError("OFFSET must be between 0 and 100,000,000");
  return offset;
}

/**
 * Rewrites a block with window select items into a wrapper over a windowed source: the inner
 * block computes every non-window item plus hidden partition and ordering columns, the window
 * columns append after execution, and the wrapper projects the visible aliases and applies the
 * block's ORDER BY and LIMIT after window computation, as SQL requires.
 *
 * The inner block carries the grouping when there is one. SQL evaluates windows after GROUP BY
 * and HAVING, so a window over a grouped block ranks the groups — `ROW_NUMBER() OVER (PARTITION
 * BY category ORDER BY SUM(total) DESC)` — and its partition and ordering expressions are read
 * from the grouped output, aggregates included.
 */
function desugarWindows(
  sql: string,
  base: TableSource,
  joins: JoinPlan[],
  select: SelectItem[],
  predicates: Predicate[],
  groupBy: Expression[],
  having: Predicate[],
  tail: SelectTail,
  nextSequence: () => number,
): CompiledQuery {
  const grouped = groupBy.length > 0 || select.some((item) => hasAggregate(item.expression));
  const groupExpressions = new Set(groupBy.map((expression) => JSON.stringify(expression)));
  /**
   * What a window may read once the rows it sees are groups: an aggregate over the group, a
   * GROUP BY expression, or a constant — the same rule the select list itself follows.
   */
  const readableWhenGrouped = (expression: Expression): boolean =>
    hasAggregate(expression) ||
    expressionColumns(expression).length === 0 ||
    groupExpressions.has(JSON.stringify(expression));
  const innerSelect: SelectItem[] = select.filter((item) => !containsWindow(item.expression));
  const windows: WindowSpec[] = [];
  let hidden = 0;

  /** A name for one more column the inner block computes for the wrapper to read back. */
  const hide = (expression: Expression): string => {
    hidden += 1;
    const alias = `(window ${String(hidden)})`;
    innerSelect.push({ expression, alias });
    return alias;
  };

  /** Registers one window under the output name the wrapper will read it by. */
  const registerWindow = (
    expression: Extract<Expression, { kind: "window" }>,
    alias: string,
  ): void => {
    // Full-text nodes evaluate against a scanned base table; a window's hidden columns
    // compute over the windowed wrapper where no such scan exists.
    const windowInputs = [
      ...expression.partitionBy,
      ...expression.orderBy.map((order) => order.expression),
      ...(expression.argument === undefined ? [] : [expression.argument]),
    ];
    if (windowInputs.some(containsFtsExpression)) {
      throw new TypeError("Full-text MATCH and BM25 are not supported inside OVER(...)");
    }
    if (grouped && !windowInputs.every(readableWhenGrouped)) {
      throw new TypeError(
        `OVER(...) must use aggregates or GROUP BY expressions in a grouped query: ${alias}`,
      );
    }
    if (windowInputs.some(containsWindow)) {
      throw new TypeError("Window functions cannot be nested inside OVER(...)");
    }
    windows.push({
      alias,
      name: expression.name,
      partitionAliases: expression.partitionBy.map(hide),
      orderAliases: expression.orderBy.map((order) => ({
        alias: hide(order.expression),
        direction: order.direction,
        ...(order.nulls === undefined ? {} : { nulls: order.nulls }),
      })),
      ...(expression.argument === undefined ? {} : { argumentAlias: hide(expression.argument) }),
      ...(expression.offset === undefined ? {} : { offset: expression.offset }),
      ...(expression.fallback === undefined ? {} : { fallback: expression.fallback }),
      ...(expression.frame === undefined ? {} : { frame: expression.frame }),
    });
  };

  /**
   * Splits an expression across the two sides of the rewrite. A window becomes a windowed
   * column; a subtree with no window in it becomes an inner column, computed before the windows
   * run; anything holding a window somewhere below keeps its shape and has its children split
   * the same way. The wrapper then evaluates what comes back — which is how
   * `revenue - LAG(revenue) OVER (ORDER BY month)` works: the subtraction happens after the
   * window, over two columns the windowed source hands it.
   */
  const split = (expression: Expression): Expression => {
    if (expression.kind === "window") {
      const alias = `(window column ${String(windows.length)})`;
      registerWindow(expression, alias);
      return { kind: "column", reference: alias };
    }
    if (!containsWindow(expression)) {
      return { kind: "column", reference: hide(expression) };
    }
    return mapChildExpressions(expression, split);
  };

  const projections = select.map((item) => {
    if (!containsWindow(item.expression)) {
      return { expression: { kind: "column" as const, reference: item.alias }, alias: item.alias };
    }
    // A window that is the whole select item keeps carrying that item's own name, which is what
    // the executor's windowed source and every existing plan already expect.
    if (item.expression.kind === "window") {
      registerWindow(item.expression, item.alias);
      return { expression: { kind: "column" as const, reference: item.alias }, alias: item.alias };
    }
    return { expression: split(item.expression), alias: item.alias };
  });
  if (innerSelect.length === 0) {
    innerSelect.push({ expression: { kind: "literal", value: 1 }, alias: "(window 0)" });
  }
  const inner: CompiledQuery = {
    sql: "(window input)",
    base,
    joins,
    select: innerSelect,
    predicates,
    groupBy,
    having,
    orderBy: [],
  };
  return {
    sql,
    base: {
      table: `(window ${String(nextSequence())})`,
      alias: "window",
      windowed: { block: inner, windows },
    },
    joins: [],
    select: projections,
    predicates: [],
    groupBy: [],
    having: [],
    ...tail,
  };
}

/**
 * Splits a parsed boolean expression into the plan's AND-list of predicates. Top-level ANDs and
 * plain comparisons keep the classic {left, operator, right} shape (so predicate pushdown, zone
 * maps, and the dictionary fast path see exactly what they always saw); any OR/NOT subtree or bare
 * boolean expression becomes a single IS TRUE predicate evaluated with three-valued logic.
 */
export function splitCondition(expression: Expression): Predicate[] {
  if (expression.kind === "logical" && expression.operator === "and") {
    return [...splitCondition(expression.left), ...splitCondition(expression.right)];
  }
  if (expression.kind === "condition") {
    return [
      {
        left: expression.left,
        operator: expression.operator,
        right: expression.right,
        ...(expression.escape === undefined ? {} : { escape: expression.escape }),
      },
    ];
  }
  return [{ left: expression, operator: "IS TRUE", right: { kind: "literal", value: null } }];
}

function defaultAlias(expression: Expression): string {
  if (expression.kind === "column")
    return expression.reference.split(".").at(-1) ?? expression.reference;
  if (expression.kind === "call") return expression.name.toLowerCase();
  if (expression.kind === "wildcard")
    return expression.table === undefined ? "*" : `${expression.table}.*`;
  return "expression";
}

/**
 * Whether a numeric literal's digits are well formed: at most one decimal point (radix 10 only)
 * and underscores only between digits, never leading, trailing, or doubled (T662).
 */
function validNumericLiteral(text: string, radix: number): boolean {
  const digits = radix === 16 ? "0-9a-fA-F" : radix === 8 ? "0-7" : radix === 2 ? "01" : "0-9";
  const group = `[${digits}]+(?:_[${digits}]+)*`;
  const pattern = radix === 10 ? `^${group}(?:\\.${group})?$` : `^${group}$`;
  return new RegExp(pattern).test(text);
}

function tokenize(sql: string): Token[] {
  const tokens: Token[] = [];
  let index = 0;
  while (index < sql.length) {
    const character = sql[index] ?? "";
    if (/\s/.test(character)) {
      index += 1;
      continue;
    }
    if (/[A-Za-z_]/.test(character)) {
      const start = index++;
      while (index < sql.length && /[A-Za-z0-9_]/.test(sql[index] ?? "")) index += 1;
      tokens.push({ kind: "identifier", text: sql.slice(start, index), start, end: index });
      continue;
    }
    if (/\d/.test(character)) {
      const start = index;
      // T661: 0x/0o/0b integers. The radix prefix is checked before the decimal scan, which
      // would otherwise stop at the letter and leave `x1F` looking like an identifier.
      const radix = { x: 16, o: 8, b: 2 }[(sql[index + 1] ?? "").toLowerCase()];
      if (character === "0" && radix !== undefined) {
        index += 2;
        while (index < sql.length && /[0-9a-fA-F_]/.test(sql[index] ?? "")) index += 1;
        const digits = sql.slice(start + 2, index);
        const value = Number.parseInt(digits.replaceAll("_", ""), radix);
        if (!validNumericLiteral(digits, radix) || !Number.isSafeInteger(value)) {
          throw new SqlCompileError(
            `Invalid number: ${sql.slice(start, index)}`,
            start,
            index - start,
          );
        }
        tokens.push({ kind: "number", text: String(value), start, end: index });
        continue;
      }
      index += 1;
      // T662: underscores may separate digits.
      while (index < sql.length && /[\d._]/.test(sql[index] ?? "")) index += 1;
      const text = sql.slice(start, index);
      if (!validNumericLiteral(text, 10))
        throw new SqlCompileError(`Invalid number: ${text}`, start, index - start);
      tokens.push({ kind: "number", text: text.replaceAll("_", ""), start, end: index });
      continue;
    }
    if (character === "'") {
      const start = index++;
      let value = "";
      let closed = false;
      while (index < sql.length) {
        if (sql[index] === "'" && sql[index + 1] === "'") {
          value += "'";
          index += 2;
        } else if (sql[index] === "'") {
          index += 1;
          closed = true;
          break;
        } else value += sql[index++] ?? "";
      }
      if (!closed)
        throw new SqlCompileError("Unterminated string literal", start, sql.length - start);
      tokens.push({ kind: "string", text: value, start, end: index });
      continue;
    }
    if (character === '"') {
      const start = index++;
      let value = "";
      let closed = false;
      while (index < sql.length) {
        if (sql[index] === '"' && sql[index + 1] === '"') {
          value += '"';
          index += 2;
        } else if (sql[index] === '"') {
          index += 1;
          closed = true;
          break;
        } else value += sql[index++] ?? "";
      }
      if (!closed) {
        throw new SqlCompileError("Unterminated quoted identifier", start, sql.length - start);
      }
      if (value.length === 0) {
        throw new SqlCompileError("Quoted identifiers cannot be empty", start, index - start);
      }
      tokens.push({ kind: "identifier", text: value, quoted: true, start, end: index });
      continue;
    }
    if (character === "?") {
      tokens.push({ kind: "parameter", text: "", start: index, end: index + 1 });
      index += 1;
      continue;
    }
    if (character === "$") {
      const start = index++;
      while (index < sql.length && /\d/.test(sql[index] ?? "")) index += 1;
      const digits = sql.slice(start + 1, index);
      if (digits.length === 0) {
        throw new SqlCompileError("Expected a parameter number after $", start, 1);
      }
      tokens.push({ kind: "parameter", text: digits, start, end: index });
      continue;
    }
    const pair = sql.slice(index, index + 2);
    if (pair === "--") {
      // E161: a simple comment runs to the end of the line.
      const newline = sql.indexOf("\n", index);
      index = newline === -1 ? sql.length : newline + 1;
      continue;
    }
    if (pair === "/*") {
      // T351: bracketed comments, which the standard does not nest.
      const close = sql.indexOf("*/", index + 2);
      if (close === -1) {
        throw new SqlCompileError("Unterminated comment", index, sql.length - index);
      }
      index = close + 2;
      continue;
    }
    if (character === ";") {
      // Statement separators lex normally; the routers reject them everywhere except inside
      // a CREATE TRIGGER body, which is the one multi-statement construct.
      tokens.push({ kind: "punctuation", text: ";", start: index, end: index + 1 });
      index += 1;
      continue;
    }
    if ([">=", "<=", "!=", "<>", "||"].includes(pair)) {
      tokens.push({ kind: "operator", text: pair, start: index, end: index + 2 });
      index += 2;
      continue;
    }
    if (["+", "-", "*", "/", "%", "=", ">", "<"].includes(character))
      tokens.push({ kind: "operator", text: character, start: index, end: index + 1 });
    else if (["(", ")", ",", "."].includes(character))
      tokens.push({ kind: "punctuation", text: character, start: index, end: index + 1 });
    else throw new SqlCompileError(`Unsupported SQL character: ${character}`, index, 1);
    index += 1;
  }
  tokens.push({ kind: "eof", text: "", start: sql.length, end: sql.length });
  return tokens;
}
