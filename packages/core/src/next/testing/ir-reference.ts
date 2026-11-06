/**
 * A row-at-a-time reference interpreter for the IR lowering emits (milestones M1 and M3), used
 * only by tests: it checks that lowered plans compute what PostgreSQL and the current engine
 * compute (the lowering-pglite and lowering-differential suites) before the real executor
 * exists. It is deliberately simple and slow, and it is not the executor's specification: the
 * IR contract comments are. Init plans run lazily, on the first read of their slot (R24.1);
 * window frames follow PostgreSQL's ROWS / RANGE / GROUPS and EXCLUDE rules.
 *
 * Values: null; booleans; every integer as an exact `bigint`, range-checked at its type
 * (PostgreSQL's 22003); floats as numbers; text as strings. Decimal constants are approximated
 * as numbers, and so are casts to decimal (from integers, floats, text and decimals, rounded
 * half away from zero to the scale); decimal arithmetic, intervals, dates and JSON are outside
 * the subset and throw `ReferenceUnsupportedError`.
 * Tables are arrays of rows in catalog order; a locator is the row's index.
 */
import type { CatalogView, TableDef } from "../access/catalog.js";
import type { Rel, ScalarExpr, SortKey, WindowCall, WindowFrameBound } from "../ir/ir.js";
import type { TableRuleExpressions } from "../ir/rules.js";
import {
  INT16_MAX,
  INT16_MIN,
  INT32_MAX,
  INT32_MIN,
  INT64_MAX,
  INT64_MIN,
  type PhysicalType,
} from "../types.js";
import { isValid } from "../vector/validity.js";
import type { FlatVector } from "../vector/vector.js";

export type Value = null | boolean | bigint | number | string;
export type Row = Value[];

export class ReferenceUnsupportedError extends Error {
  constructor(what: string) {
    super(`The reference interpreter does not cover ${what}`);
    this.name = "ReferenceUnsupportedError";
  }
}

/** A SQL error of the reference semantics, with PostgreSQL's SQLSTATE. */
export class ReferenceSqlError extends Error {
  constructor(
    message: string,
    readonly sqlState: string,
  ) {
    super(message);
    this.name = "ReferenceSqlError";
  }
}

const decoder = new TextDecoder();

export function literalValue(vector: FlatVector): Value {
  if (!isValid(vector.validity, 0)) return null;
  const data = vector.data;
  switch (data.layout) {
    case "bool":
      return data.values[0] === 1;
    case "i32":
      return BigInt(data.values[0] ?? 0);
    case "int64":
      return data.values.repr === "safe"
        ? BigInt(data.values.values[0] ?? 0)
        : (data.values.values[0] ?? 0n);
    case "f64":
    case "f32":
      return data.values[0] ?? 0;
    case "varlen":
      return decoder.decode(data.bytes.subarray(data.offsets[0] ?? 0, data.offsets[1] ?? 0));
    case "decimal": {
      // Approximated as a number: the tests only cast decimal constants to floats and integers.
      const coefficients = data.coefficients;
      if (coefficients.repr === "int128" || data.specials !== null)
        throw new ReferenceUnsupportedError("wide or special decimal literals");
      const coefficient =
        coefficients.repr === "wide"
          ? (coefficients.values[0] ?? 0n)
          : coefficients.repr === "safe"
            ? BigInt(coefficients.values[0] ?? 0)
            : (coefficients.values[0] ?? 0n);
      const scale = data.scale ?? data.scales?.[0] ?? 0;
      return Number(coefficient) / 10 ** scale;
    }
    default:
      throw new ReferenceUnsupportedError(`${data.layout} literals`);
  }
}

function checkRange(value: bigint, type: PhysicalType): bigint {
  const [min, max] =
    type.kind === "int32"
      ? type.bits === 16
        ? [BigInt(INT16_MIN), BigInt(INT16_MAX)]
        : [BigInt(INT32_MIN), BigInt(INT32_MAX)]
      : [INT64_MIN, INT64_MAX];
  if (value < min || value > max) throw new ReferenceSqlError(`${type.kind} out of range`, "22003");
  return value;
}

function roundHalfEven(value: number): number {
  const floor = Math.floor(value);
  const fraction = value - floor;
  if (fraction !== 0.5) return Math.round(value);
  return floor % 2 === 0 ? floor : floor + 1;
}

function roundHalfAway(value: number): number {
  return Math.sign(value) * Math.round(Math.abs(value));
}

const isInteger = (type: PhysicalType) => type.kind === "int32" || type.kind === "int64";
const isFloat = (type: PhysicalType) => type.kind === "float32" || type.kind === "float64";

function castValue(value: Value, from: PhysicalType, to: PhysicalType): Value {
  if (value === null) return null;
  if (isInteger(to)) {
    if (typeof value === "bigint") return checkRange(value, to);
    if (typeof value === "number")
      // PostgreSQL: float → int is rint (ties to even); numeric → int rounds half away from zero.
      return checkRange(BigInt(isFloat(from) ? roundHalfEven(value) : roundHalfAway(value)), to);
    if (typeof value === "string") {
      if (!/^\s*[+-]?\d+\s*$/u.test(value))
        throw new ReferenceSqlError(`invalid input syntax: "${value}"`, "22P02");
      return checkRange(BigInt(value.trim()), to);
    }
    if (typeof value === "boolean" && to.kind === "int32") return value ? 1n : 0n;
  }
  if (isFloat(to)) {
    const number =
      typeof value === "bigint"
        ? Number(value)
        : typeof value === "string"
          ? Number(value)
          : (value as number);
    if (Number.isNaN(number) && typeof value === "string" && value.trim().toLowerCase() !== "nan")
      throw new ReferenceSqlError(`invalid input syntax: "${value}"`, "22P02");
    return to.kind === "float32" ? Math.fround(number) : number;
  }
  if (to.kind === "decimal") {
    // Approximated as a number, like decimal constants; PostgreSQL rounds half away from zero
    // to the scale and raises 22003 when the integer digits exceed precision − scale.
    let number: number;
    if (typeof value === "bigint" || typeof value === "number") number = Number(value);
    else if (typeof value === "string") {
      if (!/^\s*[+-]?(\d+\.?\d*|\.\d+)([eE][+-]?\d+)?\s*$/u.test(value))
        throw new ReferenceSqlError(`invalid input syntax for type numeric: "${value}"`, "22P02");
      number = Number(value);
    } else throw new ReferenceUnsupportedError(`casts from ${from.kind} to decimal`);
    if (to.scale === null) return number;
    const factor = 10 ** to.scale;
    const rounded = roundHalfAway(number * factor) / factor;
    if (to.precision !== null && Math.abs(rounded) >= 10 ** (to.precision - to.scale))
      throw new ReferenceSqlError("numeric field overflow", "22003");
    return rounded;
  }
  if (to.kind === "text") {
    const text =
      typeof value === "bigint"
        ? value.toString()
        : typeof value === "boolean"
          ? value
            ? "true"
            : "false"
          : String(value);
    return to.maxLength !== undefined && Array.from(text).length > to.maxLength
      ? Array.from(text).slice(0, to.maxLength).join("")
      : text;
  }
  if (to.kind === "bool" && typeof value === "boolean") return value;
  if (to.kind === "bool" && typeof value === "bigint" && from.kind === "int32") return value !== 0n;
  if (to.kind === "bool" && typeof value === "string") {
    const word = value.trim().toLowerCase();
    if (["t", "true", "yes", "y", "on", "1"].includes(word)) return true;
    if (["f", "false", "no", "n", "off", "0"].includes(word)) return false;
  }
  throw new ReferenceUnsupportedError(`casts from ${from.kind} to ${to.kind}`);
}

function compareValues(left: Value, right: Value): number {
  if (typeof left === "bigint" && typeof right === "bigint")
    return left < right ? -1 : left > right ? 1 : 0;
  if (typeof left === "number" && typeof right === "number")
    return left < right ? -1 : left > right ? 1 : 0;
  if (typeof left === "string" && typeof right === "string") {
    // Byte (code point) order, C8.
    const a = Array.from(left);
    const b = Array.from(right);
    for (let index = 0; index < Math.min(a.length, b.length); index += 1) {
      const x = a[index]?.codePointAt(0) ?? 0;
      const y = b[index]?.codePointAt(0) ?? 0;
      if (x !== y) return x < y ? -1 : 1;
    }
    return a.length - b.length;
  }
  if (typeof left === "boolean" && typeof right === "boolean") return Number(left) - Number(right);
  throw new ReferenceUnsupportedError(`comparing ${typeof left} with ${typeof right}`);
}

function likeRegex(pattern: string, escape: string | null, insensitive: boolean): RegExp {
  let source = "";
  const chars = Array.from(pattern);
  for (let index = 0; index < chars.length; index += 1) {
    const char = chars[index] ?? "";
    if (char === (escape ?? "\\")) {
      source += (chars[index + 1] ?? "").replace(/[.*+?^${}()|[\]\\]/gu, "\\$&");
      index += 1;
    } else if (char === "%") source += "[\\s\\S]*";
    else if (char === "_") source += "[\\s\\S]";
    else source += char.replace(/[.*+?^${}()|[\]\\]/gu, "\\$&");
  }
  return new RegExp(`^${source}$`, insensitive ? "iu" : "u");
}

function arithmetic(fn: string, args: readonly Value[], type: PhysicalType): Value {
  const [a, b] = args;
  if (isInteger(type)) {
    const x = a as bigint;
    const y = b as bigint;
    switch (fn) {
      case "add":
        return checkRange(x + y, type);
      case "sub":
        return checkRange(x - y, type);
      case "mul":
        return checkRange(x * y, type);
      case "div":
        if (y === 0n) throw new ReferenceSqlError("division by zero", "22012");
        return checkRange(x / y, type);
      case "mod":
        if (y === 0n) throw new ReferenceSqlError("division by zero", "22012");
        return x % y;
      case "neg":
        return checkRange(-x, type);
      case "abs":
        return checkRange(x < 0n ? -x : x, type);
    }
  }
  if (isFloat(type)) {
    const x = a as number;
    const y = b as number;
    const round = (value: number) => (type.kind === "float32" ? Math.fround(value) : value);
    switch (fn) {
      case "add":
        return round(x + y);
      case "sub":
        return round(x - y);
      case "mul":
        return round(x * y);
      case "div":
        if (y === 0) throw new ReferenceSqlError("division by zero", "22012");
        return round(x / y);
      case "neg":
        return round(-x);
      case "abs":
        return round(Math.abs(x));
      case "round":
        return roundHalfEven(x);
    }
  }
  if (type.kind === "decimal") {
    // Approximated as numbers, like decimal constants (see the module comment): enough for the
    // small constants the tests compute with, not an exact NUMERIC.
    const x = Number(a);
    const y = Number(b);
    switch (fn) {
      case "add":
        return x + y;
      case "sub":
        return x - y;
      case "mul":
        return x * y;
      case "div":
        if (y === 0) throw new ReferenceSqlError("division by zero", "22012");
        return x / y;
      case "neg":
        return -x;
      case "abs":
        return Math.abs(x);
    }
  }
  throw new ReferenceUnsupportedError(`${fn} over ${type.kind}`);
}

export interface Environment {
  readonly tables: Map<number, Row[]>;
  /** The rows of the plan's `input` relation (batch writes). */
  readonly input?: readonly Row[];
  readonly catalog: CatalogView;
  readonly params: readonly Value[];
  readonly rules: (table: TableDef) => TableRuleExpressions;
  sequences: Map<number, bigint>;
  /** Recursive work tables in scope, by id. */
  readonly workTables?: ReadonlyMap<number, readonly Row[]>;
  /**
   * Init plans by slot (ir.ts `initPlans`, R24.1): each runs on the first read of its slot and
   * keeps its value, so a query whose rows never reach the read never runs it.
   */
  readonly initPlans?: ReadonlyMap<number, () => Value>;
}

/** `env` with `plan`'s init plans evaluated lazily, once each, on the first read of their slot. */
export function withInitPlans(
  plan: { readonly initPlans: ReadonlyArray<{ readonly slot: number; readonly plan: Rel }> },
  env: Environment,
): Environment {
  const thunks = new Map<number, () => Value>();
  const lazy: Environment = { ...env, initPlans: thunks };
  for (const init of plan.initPlans) {
    let done = false;
    let value: Value = null;
    thunks.set(init.slot, () => {
      if (!done) {
        value = run(init.plan, lazy)[0]?.[0] ?? null;
        done = true;
      }
      return value;
    });
  }
  return lazy;
}

export function evaluate(expr: ScalarExpr, row: readonly Value[], env: Environment): Value {
  switch (expr.op) {
    case "column": {
      const value = row[expr.index];
      if (value === undefined)
        throw new ReferenceUnsupportedError(
          `column ${String(expr.index)} of a ${String(row.length)}-column row`,
        );
      return value;
    }
    case "literal":
      return literalValue(expr.value);
    case "param": {
      const init = env.initPlans?.get(expr.slot);
      return init === undefined ? (env.params[expr.slot] ?? null) : init();
    }
    case "cast": {
      const value = evaluate(expr.input, row, env);
      if (
        expr.assignment === true &&
        expr.type.kind === "text" &&
        typeof value === "string" &&
        expr.type.maxLength !== undefined &&
        Array.from(value).length > expr.type.maxLength
      )
        throw new ReferenceSqlError("value too long", "22001");
      return castValue(value, expr.input.type, expr.type);
    }
    case "compare": {
      const left = evaluate(expr.left, row, env);
      const right = evaluate(expr.right, row, env);
      if (left === null || right === null) return null;
      const order = compareValues(left, right);
      switch (expr.cmp) {
        case "=":
          return order === 0;
        case "<>":
          return order !== 0;
        case "<":
          return order < 0;
        case "<=":
          return order <= 0;
        case ">":
          return order > 0;
        case ">=":
          return order >= 0;
      }
      break;
    }
    case "distinctFrom": {
      const left = evaluate(expr.left, row, env);
      const right = evaluate(expr.right, row, env);
      const same =
        left === null || right === null ? left === right : compareValues(left, right) === 0;
      return expr.negated ? same : !same;
    }
    case "and":
    case "or": {
      let unknown = false;
      for (const arg of expr.args) {
        const value = evaluate(arg, row, env);
        if (value === null) unknown = true;
        else if (value === (expr.op === "or")) return value;
      }
      return unknown ? null : expr.op === "and";
    }
    case "not": {
      const value = evaluate(expr.arg, row, env);
      return value === null ? null : !value;
    }
    case "isNull": {
      const value = evaluate(expr.arg, row, env);
      return expr.negated ? value !== null : value === null;
    }
    case "case": {
      for (const branch of expr.branches)
        if (evaluate(branch.when, row, env) === true) return evaluate(branch.then, row, env);
      return expr.otherwise === null ? null : evaluate(expr.otherwise, row, env);
    }
    case "inList": {
      const value = evaluate(expr.arg, row, env);
      if (value === null) return null;
      let unknown = false;
      for (const item of expr.list) {
        const candidate = evaluate(item, row, env);
        if (candidate === null) unknown = true;
        else if (compareValues(value, candidate) === 0) return !expr.negated;
      }
      return unknown ? null : expr.negated;
    }
    case "like": {
      const value = evaluate(expr.arg, row, env);
      const pattern = evaluate(expr.pattern, row, env);
      if (value === null || pattern === null) return null;
      if (expr.mode === "similar") throw new ReferenceUnsupportedError("SIMILAR TO");
      const matched = likeRegex(String(pattern), expr.escape, expr.mode === "ilike").test(
        String(value),
      );
      return expr.negated ? !matched : matched;
    }
    case "call": {
      if (expr.fn === "coalesce") {
        for (const arg of expr.args) {
          const value = evaluate(arg, row, env);
          if (value !== null) return value;
        }
        return null;
      }
      if (expr.fn === "random") return Math.random();
      if (expr.fn === "gen_random_uuid") return crypto.randomUUID();
      if (expr.fn === "nextval") {
        const id = Number(evaluate(expr.args[0] ?? expr, row, env));
        const next = (env.sequences.get(id) ?? 0n) + 1n;
        env.sequences.set(id, next);
        return next;
      }
      const args = expr.args.map((arg) => evaluate(arg, row, env));
      if (expr.fn === "nullif") {
        const [a, b] = args;
        return a !== null &&
          b !== null &&
          a !== undefined &&
          b !== undefined &&
          compareValues(a, b) === 0
          ? null
          : (a ?? null);
      }
      if (expr.fn === "greatest" || expr.fn === "least") {
        const present = args.filter((arg): arg is Exclude<Value, null> => arg !== null);
        if (present.length === 0) return null;
        return present.reduce((best, value) =>
          compareValues(value, best) * (expr.fn === "greatest" ? 1 : -1) > 0 ? value : best,
        );
      }
      if (args.some((arg) => arg === null)) return null;
      switch (expr.fn) {
        case "textcat":
          return `${String(args[0])}${String(args[1])}`;
        case "lower":
          return String(args[0]).toLowerCase();
        case "upper":
          return String(args[0]).toUpperCase();
        case "length":
          return BigInt(Array.from(String(args[0])).length);
        case "btrim":
          return String(args[0]).trim();
        case "replace":
          return String(args[0]).replaceAll(String(args[1]), String(args[2]));
        case "substr": {
          const chars = Array.from(String(args[0]));
          const start = Number(args[1]);
          const count = args[2] === undefined ? chars.length : Number(args[2]);
          const from = Math.max(start, 1);
          const to = start + count;
          return chars.slice(from - 1, Math.max(to - 1, from - 1)).join("");
        }
        default:
          return arithmetic(expr.fn, args, expr.type);
      }
    }
  }
  throw new ReferenceUnsupportedError((expr as ScalarExpr).op);
}

function sortRows(rows: Row[], keys: readonly SortKey[], env: Environment): Row[] {
  return rows
    .map((row) => ({ row, values: keys.map((key) => evaluate(key.expr, row, env)) }))
    .sort((a, b) => {
      for (const [index, key] of keys.entries()) {
        const x = a.values[index] ?? null;
        const y = b.values[index] ?? null;
        if (x === null || y === null) {
          if (x === y) continue;
          const nullFirst = key.nulls === "first";
          return (x === null) === nullFirst ? -1 : 1;
        }
        const order = compareValues(x, y);
        if (order !== 0) return key.direction === "asc" ? order : -order;
      }
      return 0;
    })
    .map((entry) => entry.row);
}

function boundValue(
  bound:
    | { readonly kind: "const"; readonly value: number }
    | { readonly kind: "param"; readonly slot: number }
    | null,
  env: Environment,
): number | null {
  if (bound === null) return null;
  if (bound.kind === "const") return bound.value;
  const value = env.params[bound.slot];
  return value === null || value === undefined ? null : Number(value);
}

/** The aggregate of `values` (one row per input row, null skipped except by COUNT(*)). */
function aggregate(fn: string, values: readonly Value[], type: PhysicalType): Value {
  const present = values.filter((value) => value !== null);
  switch (fn) {
    case "count_star":
      return BigInt(values.length);
    case "count":
      return BigInt(present.length);
    case "sum":
      if (present.length === 0) return null;
      if (type.kind === "int64")
        return checkRange(
          present.reduce<bigint>((sum, value) => sum + (value as bigint), 0n),
          type,
        );
      if (type.kind === "decimal")
        return present.reduce<bigint>((sum, value) => sum + (value as bigint), 0n);
      return present.reduce<number>((sum, value) => sum + (value as number), 0);
    case "avg":
      if (present.length === 0) return null;
      return present.reduce<number>((sum, value) => sum + Number(value), 0) / present.length;
    case "single_value":
      if (values.length > 1)
        throw new ReferenceSqlError(
          "more than one row returned by a subquery used as an expression",
          "21000",
        );
      return values[0] ?? null;
    case "min":
    case "max":
      if (present.length === 0) return null;
      return present.reduce((best, value) =>
        compareValues(value, best) * (fn === "max" ? 1 : -1) > 0 ? value : best,
      );
  }
  throw new ReferenceUnsupportedError(`aggregate ${fn}`);
}

/** Runs a relation; for `modify`, writes into `env.tables` and returns RETURNING rows. */
export function run(rel: Rel, env: Environment): Row[] {
  switch (rel.op) {
    case "values":
      return rel.rows.map((row) => row.map((cell) => evaluate(cell, [], env)));
    case "scan":
    case "keyLookup": {
      const table = env.catalog.table(rel.tableId);
      const stored = env.tables.get(rel.tableId) ?? [];
      if (table === undefined) throw new ReferenceUnsupportedError("a missing table");
      const positions = rel.columns.map((id) =>
        table.columns.findIndex((column) => column.id === id),
      );
      const locator = rel.op === "scan" ? rel.emitLocator : rel.emitLocator === true;
      let candidates = stored
        .map((row, index) => ({ row, index }))
        .filter((entry) => entry.row.length > 0);
      if (rel.op === "keyLookup") {
        const index = table.indexes.find((entry) => entry.id === rel.indexId);
        const keyPositions =
          index?.columnIds.map((id) => table.columns.findIndex((column) => column.id === id)) ?? [];
        const tuples = rel.keys.map((keys) => keys.map((key) => evaluate(key, [], env)));
        candidates = candidates.filter(({ row }) =>
          tuples.some((tuple) =>
            tuple.every(
              (value, k) =>
                value !== null &&
                row[keyPositions[k] ?? -1] !== null &&
                compareValues(value, row[keyPositions[k] ?? -1] ?? null) === 0,
            ),
          ),
        );
      }
      const out = candidates.map(({ row, index }) => {
        const projected = positions.map((position) => row[position] ?? null);
        if (locator) projected.push(BigInt(index));
        return projected;
      });
      const filter = rel.op === "scan" ? rel.filter : null;
      return filter === null ? out : out.filter((row) => evaluate(filter, row, env) === true);
    }
    case "filter":
      return run(rel.input, env).filter((row) => evaluate(rel.predicate, row, env) === true);
    case "project":
      return run(rel.input, env).map((row) => rel.exprs.map((expr) => evaluate(expr, row, env)));
    case "aggregate": {
      const rows = run(rel.input, env);
      const groups = new Map<string, Row[]>();
      for (const row of rows) {
        const key = JSON.stringify(
          rel.groupBy.map((index) => row[index] ?? null),
          (_k, v: unknown) => (typeof v === "bigint" ? `${v.toString()}n` : v),
        );
        const group = groups.get(key);
        if (group === undefined) groups.set(key, [row]);
        else group.push(row);
      }
      if (groups.size === 0 && rel.groupBy.length === 0) groups.set("[]", []);
      return [...groups.values()].map((group) => [
        ...rel.groupBy.map((index) => group[0]?.[index] ?? null),
        ...rel.aggregates.map((call) => {
          let values = group.map((row) =>
            call.args[0] === undefined ? true : evaluate(call.args[0], row, env),
          );
          if (call.distinct)
            values = [
              ...new Map(
                values.map((value) => [
                  typeof value === "bigint" ? `${value.toString()}n` : JSON.stringify(value),
                  value,
                ]),
              ).values(),
            ];
          return aggregate(call.fn, values, call.type);
        }),
      ]);
    }
    case "sort": {
      const limit = boundValue(rel.limit, env);
      // LIMIT 0 pulls no row, so nothing below it is evaluated (PostgreSQL's Limit node).
      if (limit === 0) return [];
      const sorted = sortRows(run(rel.input, env), rel.keys, env);
      const offset = boundValue(rel.offset, env) ?? 0;
      return sorted.slice(offset, limit === null ? undefined : offset + limit);
    }
    case "limit": {
      const limit = boundValue(rel.limit, env);
      if (limit === 0) return [];
      const rows = run(rel.input, env);
      const offset = boundValue(rel.offset, env) ?? 0;
      return rows.slice(offset, limit === null ? undefined : offset + limit);
    }
    case "modify":
      return modifyRows(rel, env);
    case "join":
      return joinRows(rel, env);
    case "apply":
      return applyRows(rel, env);
    case "window":
      return windowRows(rel, env);
    case "setOp":
      return setOpRows(rel, env);
    case "recursive":
      return recursiveRows(rel, env);
    case "workTableScan":
      return (env.workTables?.get(rel.workTable) ?? []).map((row) => [...row]);
    case "indexRange":
      return indexRangeRows(rel, env);
    case "input":
      return (env.input ?? []).map((row) => [...row]);
    default:
      throw new ReferenceUnsupportedError((rel as Rel).op);
  }
}

function modifyRows(rel: Extract<Rel, { op: "modify" }>, env: Environment): Row[] {
  const table = env.catalog.table(rel.tableId);
  if (table === undefined) throw new ReferenceUnsupportedError("a missing table");
  const stored = env.tables.get(table.id) ?? [];
  env.tables.set(table.id, stored);
  const rules = env.rules(table);
  if (rel.kind === "truncate") {
    stored.length = 0;
    return [];
  }
  const input = run(rel.input, env);
  const returned: Row[] = [];
  const touched = new Set<number>();
  const validate = (row: Row): void => {
    for (const [position, column] of table.columns.entries())
      if (!column.nullable && row[position] === null)
        throw new ReferenceSqlError(`null in ${column.name}`, "23502");
    for (const check of rules.checks)
      if (evaluate(check.condition, row, env) === false)
        throw new ReferenceSqlError("check violated", "23514");
  };
  const unique = table.indexes.filter((index) => index.unique);
  const positionsOf = (ids: readonly number[]) =>
    ids.map((id) => table.columns.findIndex((column) => column.id === id));
  const conflict = (row: Row, indexIds: readonly number[]): number => {
    const arbiters =
      indexIds.length === 0 ? unique : unique.filter((index) => indexIds.includes(index.id));
    for (const index of arbiters) {
      const positions = positionsOf(index.columnIds);
      if (positions.some((position) => row[position] === null)) continue;
      const found = stored.findIndex(
        (other) =>
          other.length > 0 &&
          positions.every(
            (position) => compareValues(other[position] ?? null, row[position] ?? null) === 0,
          ),
      );
      if (found !== -1) return found;
    }
    return -1;
  };
  const returning = (row: Row, extra: readonly Value[] = []): void => {
    if (rel.returning !== null)
      returned.push(rel.returning.map((expr) => evaluate(expr, [...row, ...extra], env)));
  };
  const passthrough = rel.passthrough ?? 0;
  if (rel.kind === "merge" && rel.merge !== undefined) {
    const spec = rel.merge;
    for (const row of input) {
      const locatorValue = row[spec.sourceColumns] ?? null;
      const matched = locatorValue !== null;
      for (const action of spec.actions) {
        if (action.when !== (matched ? "matched" : "notMatched")) continue;
        if (action.condition !== null && evaluate(action.condition, row, env) !== true) continue;
        if (action.action.kind === "nothing") break;
        if (action.when === "matched") {
          const locator = Number(locatorValue);
          if (touched.has(locator))
            throw new ReferenceSqlError("MERGE affects a row a second time", "21000");
          touched.add(locator);
          if (action.action.kind === "delete") {
            stored[locator] = [];
            break;
          }
          const next = [...(stored[locator] ?? [])];
          const reads = [...row];
          for (const [j, id] of action.action.assigned.entries()) {
            const value = evaluate(at(action.action.values, j), reads, env);
            reads.push(value);
            next[table.columns.findIndex((column) => column.id === id)] = value;
          }
          validate(next);
          stored[locator] = [];
          if (conflict(next, []) !== -1) throw new ReferenceSqlError("duplicate key", "23505");
          stored[locator] = next;
          break;
        }
        const reads = [...row];
        const next: Row = [];
        for (const value of action.action.values) {
          const computed = evaluate(value, reads, env);
          reads.push(computed);
          next.push(computed);
        }
        validate(next);
        if (conflict(next, []) !== -1) throw new ReferenceSqlError("duplicate key", "23505");
        stored.push(next);
        touched.add(stored.length - 1);
        break;
      }
    }
    return [];
  }
  for (const row of input) {
    if (rel.kind === "insert" || rel.kind === "upsert") {
      validate(row);
      if (rel.kind === "upsert" && rel.onConflict !== null) {
        const found = conflict(row, rel.onConflict.arbiters);
        if (found !== -1) {
          if (rel.onConflict.action === "nothing") continue;
          if (touched.has(found))
            throw new ReferenceSqlError("affects a row a second time", "21000");
          const existing = stored[found] ?? [];
          const both = [...existing, ...row];
          if (rel.onConflict.where !== null && evaluate(rel.onConflict.where, both, env) !== true)
            continue;
          const next = [...existing];
          // In list order; assignment j reads earlier values as columns 2n + i (ir.ts).
          for (const assignment of rel.onConflict.assignments) {
            const value = evaluate(assignment.expr, both, env);
            both.push(value);
            next[table.columns.findIndex((column) => column.id === assignment.column)] = value;
          }
          validate(next);
          stored[found] = next;
          touched.add(found);
          returning(next);
          continue;
        }
      } else if (conflict(row, []) !== -1) throw new ReferenceSqlError("duplicate key", "23505");
      stored.push(row);
      touched.add(stored.length - 1);
      returning(row);
    } else {
      const locator = Number(row[0]);
      const existing = stored[locator];
      const extra = passthrough === 0 ? [] : row.slice(row.length - passthrough);
      if (touched.has(locator)) {
        // A row affected twice: the first wins (UPDATE … FROM), else 21000.
        if (rel.onRepeat === "first") continue;
        throw new ReferenceSqlError("command affects a row a second time", "21000");
      }
      if (existing === undefined || existing.length === 0) continue;
      touched.add(locator);
      if (rel.kind === "delete") {
        returning(existing, extra);
        stored[locator] = [];
        continue;
      }
      const next = [...existing];
      for (const [k, id] of rel.assigned.entries())
        next[table.columns.findIndex((column) => column.id === id)] = row[k + 1] ?? null;
      validate(next);
      stored[locator] = [];
      if (conflict(next, []) !== -1) throw new ReferenceSqlError("duplicate key", "23505");
      stored[locator] = next;
      returning(next, extra);
    }
  }
  return returned;
}

const rowKey = (row: readonly Value[]): string =>
  JSON.stringify(row, (_key, value: unknown) =>
    typeof value === "bigint" ? `${value.toString()}n` : value,
  );

function keysMatch(
  rel: Extract<Rel, { op: "join" }>,
  left: Row,
  right: Row,
  from: number,
  to: number,
  env: Environment,
): boolean | null {
  let result: boolean | null = true;
  for (let index = from; index < to; index += 1) {
    const l = evaluate(at(rel.leftKeys, index), left, env);
    const r = evaluate(at(rel.rightKeys, index), right, env);
    let equal: boolean | null;
    if (rel.nullSafe[index] === true)
      equal = l === null || r === null ? l === r : compareValues(l, r) === 0;
    else equal = l === null || r === null ? null : compareValues(l, r) === 0;
    if (equal === false) return false;
    if (equal === null) result = null;
  }
  return result;
}

function joinRows(rel: Extract<Rel, { op: "join" }>, env: Environment): Row[] {
  const left = run(rel.left, env);
  const right = run(rel.right, env);
  const keys = rel.leftKeys.length;
  const nullsLeft = (): Row => rel.left.output.map(() => null);
  const nullsRight = (): Row => rel.right.output.map(() => null);
  if (rel.kind === "mark") {
    // R20.5: participation by correlation keys and `filter`; the mark from the tested keys and
    // `residual`, three-valued.
    const correlated = rel.correlated ?? 0;
    return left.map((l) => {
      let sawTrue = false;
      let sawUnknown = false;
      for (const r of right) {
        if (keysMatch(rel, l, r, 0, correlated, env) !== true) continue;
        const both = [...l, ...r];
        if (
          rel.filter !== undefined &&
          rel.filter !== null &&
          evaluate(rel.filter, both, env) !== true
        )
          continue;
        let compared = keysMatch(rel, l, r, correlated, keys, env);
        if (compared !== false && rel.residual !== null) {
          const residual = evaluate(rel.residual, both, env);
          compared = residual === false ? false : residual === null ? null : compared;
        }
        if (compared === true) {
          sawTrue = true;
          break;
        }
        if (compared === null) sawUnknown = true;
      }
      return [...l, sawTrue ? true : sawUnknown ? null : false];
    });
  }
  const matches = (l: Row, r: Row): boolean =>
    keysMatch(rel, l, r, 0, keys, env) === true &&
    (rel.residual === null || evaluate(rel.residual, [...l, ...r], env) === true);
  const out: Row[] = [];
  const matchedRight = new Set<number>();
  for (const l of left) {
    let any = false;
    for (const [index, r] of right.entries()) {
      if (!matches(l, r)) continue;
      any = true;
      matchedRight.add(index);
      if (rel.kind === "semi" || rel.kind === "anti") break;
      out.push([...l, ...r]);
    }
    if (rel.kind === "semi" && any) out.push([...l]);
    if (rel.kind === "anti" && !any) out.push([...l]);
    if ((rel.kind === "left" || rel.kind === "full") && !any) out.push([...l, ...nullsRight()]);
  }
  if (rel.kind === "full" || rel.kind === "right")
    for (const [index, r] of right.entries())
      if (!matchedRight.has(index)) out.push([...nullsLeft(), ...r]);
  return out;
}

function quantify(
  test: Value,
  values: readonly Value[],
  cmp: "=" | "<>" | "<" | "<=" | ">" | ">=",
  quantifier: "any" | "all",
): boolean | null {
  let unknown = false;
  for (const value of values) {
    let result: boolean | null;
    if (test === null || value === null) result = null;
    else {
      const order = compareValues(test, value);
      result =
        cmp === "="
          ? order === 0
          : cmp === "<>"
            ? order !== 0
            : cmp === "<"
              ? order < 0
              : cmp === "<="
                ? order <= 0
                : cmp === ">"
                  ? order > 0
                  : order >= 0;
    }
    if (quantifier === "any" && result === true) return true;
    if (quantifier === "all" && result === false) return false;
    if (result === null) unknown = true;
  }
  if (unknown) return null;
  return quantifier === "all";
}

function applyRows(rel: Extract<Rel, { op: "apply" }>, env: Environment): Row[] {
  return run(rel.outer, env).map((row) => {
    const params = [...env.params];
    for (const binding of rel.bindings) params[binding.slot] = evaluate(binding.expr, row, env);
    const rows = run(rel.inner, { ...env, params });
    if (rel.mode === "scalar") {
      if (rows.length > 1)
        throw new ReferenceSqlError(
          "more than one row returned by a subquery used as an expression",
          "21000",
        );
      return [...row, rows[0]?.[0] ?? null];
    }
    if (rel.mode === "exists") return [...row, rows.length > 0];
    const quantified = rel.quantified;
    if (quantified === null)
      throw new ReferenceUnsupportedError("a quantified apply without its test");
    const test = evaluate(quantified.test, row, env);
    return [
      ...row,
      quantify(
        test,
        rows.map((inner) => inner[0] ?? null),
        quantified.cmp,
        quantified.quantifier,
      ),
    ];
  });
}

function windowRows(rel: Extract<Rel, { op: "window" }>, env: Environment): Row[] {
  const rows = run(rel.input, env);
  const partitions = new Map<string, Row[]>();
  for (const row of rows) {
    const key = rowKey(rel.partitionBy.map((index) => row[index] ?? null));
    partitions.set(key, [...(partitions.get(key) ?? []), row]);
  }
  const out: Row[] = [];
  for (const partition of partitions.values()) {
    const sorted = sortRows(partition, rel.orderBy, env);
    const orderKey = (row: Row) => rowKey(rel.orderBy.map((key) => evaluate(key.expr, row, env)));
    const peersEnd = sorted.map((row, index) => {
      let end = index;
      while (end + 1 < sorted.length && orderKey(at(sorted, end + 1)) === orderKey(row)) end += 1;
      return end;
    });
    const peersStart = sorted.map((row, index) => {
      let start = index;
      while (start > 0 && orderKey(at(sorted, start - 1)) === orderKey(row)) start -= 1;
      return start;
    });
    const n = sorted.length;
    // Peer groups in order: each row's group number, and each group's first and last row.
    const groupOf: number[] = [];
    const groups: Array<[number, number]> = [];
    for (let index = 0; index < n; index += 1) {
      if (index === 0 || peersStart[index] === index)
        groups.push([index, peersEnd[index] ?? index]);
      groupOf.push(groups.length - 1);
    }
    const sortKey = (row: Row): Value => {
      const [key] = rel.orderBy;
      return key === undefined ? null : evaluate(key.expr, row, env);
    };
    const descending = rel.orderBy[0]?.direction === "desc";
    const nullsFirst = (rel.orderBy[0]?.nulls ?? (descending ? "first" : "last")) === "first";
    /** The rows of `call`'s frame for the row at `index`, in order (PostgreSQL's frame rules). */
    const frameOf = (call: WindowCall, row: Row, index: number): Row[] => {
      const frame = call.frame;
      const peers: [number, number] = [peersStart[index] ?? index, peersEnd[index] ?? index];
      if (frame === null) return rel.orderBy.length === 0 ? sorted : sorted.slice(0, peers[1] + 1);
      const offset = (bound: WindowFrameBound): Value =>
        bound.offset === null ? null : evaluate(bound.offset, row, env);
      const position = (bound: WindowFrameBound, end: boolean): number => {
        switch (bound.kind) {
          case "unbounded-preceding":
            return 0;
          case "unbounded-following":
            return n - 1;
          case "current-row":
            return frame.unit === "rows" ? index : end ? peers[1] : peers[0];
        }
        const amount = offset(bound);
        if (amount === null) throw new ReferenceSqlError("frame offset must not be null", "22004");
        if (Number(amount) < 0)
          throw new ReferenceSqlError("frame offset must not be negative", "22013");
        const sign = bound.kind === "preceding" ? -1 : 1;
        if (frame.unit === "rows") return index + sign * Number(amount);
        if (frame.unit === "groups") {
          const group = (groupOf[index] ?? 0) + sign * Number(amount);
          if (group < 0) return end ? -1 : 0;
          if (group >= groups.length) return end ? n - 1 : n;
          const [first, last] = at(groups, group);
          return end ? last : first;
        }
        // RANGE with an offset (PostgreSQL's in_range): the target is this row's key moved by
        // `amount` along the sort direction, exactly (bigint for integer keys). A frame start is
        // the first row not before the target in sort order, a frame end the last row not after
        // it; NULL keys sort where NULLS FIRST / LAST puts them, so a range can run into them.
        // A NULL key's own frame is its NULL peers.
        const own = sortKey(row);
        if (own === null) return end ? peers[1] : peers[0];
        const step = descending ? -sign : sign;
        const target: Value =
          typeof own === "bigint"
            ? own + BigInt(step) * BigInt(amount)
            : Number(own) + step * Number(amount);
        const relative = (key: Value): number => {
          if (key === null) return nullsFirst ? -1 : 1;
          const order =
            typeof key === "bigint" && typeof target === "bigint"
              ? key < target
                ? -1
                : key > target
                  ? 1
                  : 0
              : Math.sign(Number(key) - Number(target));
          return descending ? -order : order;
        };
        if (end) {
          for (let candidate = n - 1; candidate >= 0; candidate -= 1)
            if (relative(sortKey(at(sorted, candidate))) <= 0) return candidate;
          return -1;
        }
        for (let candidate = 0; candidate < n; candidate += 1)
          if (relative(sortKey(at(sorted, candidate))) >= 0) return candidate;
        return n;
      };
      const start = Math.max(0, position(frame.start, false));
      const end = Math.min(n - 1, position(frame.end, true));
      const members: number[] = [];
      for (let candidate = start; candidate <= end; candidate += 1) {
        const peer = candidate >= peers[0] && candidate <= peers[1];
        if (frame.exclude === "current-row" && candidate === index) continue;
        if (frame.exclude === "group" && peer) continue;
        if (frame.exclude === "ties" && peer && candidate !== index) continue;
        members.push(candidate);
      }
      return members.map((member) => at(sorted, member));
    };
    for (const [index, row] of sorted.entries()) {
      const values = rel.functions.map((call) => {
        const frame = frameOf(call, row, index);
        const arg = (target: Row) =>
          call.args[0] === undefined ? true : evaluate(call.args[0], target, env);
        switch (call.fn) {
          case "row_number":
            return BigInt(index + 1);
          case "rank":
            return BigInt((peersStart[index] ?? index) + 1);
          case "dense_rank":
            return BigInt((groupOf[index] ?? 0) + 1);
          case "percent_rank":
            return n <= 1 ? 0 : (peersStart[index] ?? index) / (n - 1);
          case "cume_dist":
            return ((peersEnd[index] ?? index) + 1) / n;
          case "ntile": {
            const buckets = call.args[0] === undefined ? null : evaluate(call.args[0], row, env);
            if (buckets === null) return null;
            const count = Number(buckets);
            if (count <= 0)
              throw new ReferenceSqlError("argument of ntile must be greater than zero", "22014");
            // PostgreSQL: n / count rows per bucket, the first n % count buckets one row more.
            const size = Math.floor(n / count);
            const larger = n % count;
            const boundary = larger * (size + 1);
            return BigInt(
              index < boundary
                ? Math.floor(index / (size + 1)) + 1
                : larger + Math.floor((index - boundary) / Math.max(size, 1)) + 1,
            );
          }
          case "nth_value": {
            const nth = call.args[1] === undefined ? null : evaluate(call.args[1], row, env);
            if (nth === null) return null;
            if (Number(nth) <= 0)
              throw new ReferenceSqlError(
                "argument of nth_value must be greater than zero",
                "22016",
              );
            const target = frame[Number(nth) - 1];
            return target === undefined ? null : arg(target);
          }
          case "lag":
          case "lead": {
            const offset = Number(
              call.args[1] === undefined ? 1n : evaluate(call.args[1], row, env),
            );
            const target = sorted[call.fn === "lag" ? index - offset : index + offset];
            if (target !== undefined) return arg(target);
            return call.args[2] === undefined ? null : evaluate(call.args[2], row, env);
          }
          case "first_value":
            return frame[0] === undefined ? null : arg(frame[0]);
          case "last_value": {
            const last = frame.at(-1);
            return last === undefined ? null : arg(last);
          }
          default:
            return aggregate(call.fn, frame.map(arg), call.type);
        }
      });
      out.push([...row, ...values]);
    }
  }
  return out;
}

function setOpRows(rel: Extract<Rel, { op: "setOp" }>, env: Environment): Row[] {
  const [first = [], ...others] = rel.inputs.map((input) => run(input, env));
  const distinct = (rows: readonly Row[]): Row[] => {
    const seen = new Set<string>();
    return rows.filter((row) => {
      const key = rowKey(row);
      if (seen.has(key)) return false;
      seen.add(key);
      return true;
    });
  };
  let acc = first;
  for (const other of others) {
    if (rel.kind === "union") acc = [...acc, ...other];
    else {
      // Multiset counts of the right side: INTERSECT keeps, EXCEPT removes, one per occurrence
      // with ALL; any occurrence without.
      const counts = new Map<string, number>();
      for (const row of other) counts.set(rowKey(row), (counts.get(rowKey(row)) ?? 0) + 1);
      acc = acc.filter((row) => {
        const key = rowKey(row);
        const count = counts.get(key) ?? 0;
        if (rel.all && count > 0) counts.set(key, count - 1);
        return rel.kind === "intersect" ? count > 0 : count === 0;
      });
    }
    if (!rel.all) acc = distinct(acc);
  }
  return acc;
}

function recursiveRows(rel: Extract<Rel, { op: "recursive" }>, env: Environment): Row[] {
  const seen = new Set<string>();
  const fresh = (rows: readonly Row[]): Row[] =>
    rel.all
      ? [...rows]
      : rows.filter((row) => {
          const key = rowKey(row);
          if (seen.has(key)) return false;
          seen.add(key);
          return true;
        });
  let work = fresh(run(rel.base, env));
  const result = [...work];
  for (let iteration = 0; work.length > 0; iteration += 1) {
    if (iteration > 10_000) throw new ReferenceUnsupportedError("recursion past 10,000 steps");
    const workTables = new Map(env.workTables ?? []);
    workTables.set(rel.workTable, work);
    work = fresh(run(rel.step, { ...env, workTables }));
    result.push(...work);
  }
  return result;
}

function indexRangeRows(rel: Extract<Rel, { op: "indexRange" }>, env: Environment): Row[] {
  const table = env.catalog.table(rel.tableId);
  const index = table?.indexes.find((candidate) => candidate.id === rel.indexId);
  if (table === undefined || index === undefined)
    throw new ReferenceUnsupportedError("a missing index");
  const keyPositions = index.columnIds.map((id) =>
    table.columns.findIndex((column) => column.id === id),
  );
  const positions = rel.columns.map((id) => table.columns.findIndex((column) => column.id === id));
  const bound = (side: typeof rel.lower) =>
    side === null
      ? null
      : { keys: side.keys.map((key) => evaluate(key, [], env)), inclusive: side.inclusive };
  const lower = bound(rel.lower);
  const upper = bound(rel.upper);
  if ([...(lower?.keys ?? []), ...(upper?.keys ?? [])].some((value) => value === null)) return [];
  const compareTo = (row: Row, keys: readonly Value[]): number => {
    for (const [k, key] of keys.entries()) {
      const value = row[keyPositions[k] ?? -1] ?? null;
      if (value === null || key === null) return Number.NaN;
      const order = compareValues(value, key);
      if (order !== 0) return order;
    }
    return 0;
  };
  const stored = env.tables.get(rel.tableId) ?? [];
  const selected = stored
    .map((row, locator) => ({ row, locator }))
    .filter(({ row }) => {
      if (row.length === 0) return false;
      if (lower !== null) {
        const order = compareTo(row, lower.keys);
        if (Number.isNaN(order) || order < 0 || (order === 0 && !lower.inclusive)) return false;
      }
      if (upper !== null) {
        const order = compareTo(row, upper.keys);
        if (Number.isNaN(order) || order > 0 || (order === 0 && !upper.inclusive)) return false;
      }
      return true;
    })
    .sort((a, b) => {
      for (const position of keyPositions) {
        const x = a.row[position] ?? null;
        const y = b.row[position] ?? null;
        if (x === null || y === null) continue;
        const order = compareValues(x, y);
        if (order !== 0) return rel.descending ? -order : order;
      }
      return a.locator - b.locator;
    });
  return selected.map(({ row, locator }) => {
    const out = positions.map((position) => row[position] ?? null);
    if (rel.emitLocator) out.push(BigInt(locator));
    return out;
  });
}

/** `items[index]`, which the plan's shape guarantees exists. */
function at<T>(items: ArrayLike<T>, index: number): T {
  const item = items[index];
  if (item === undefined) throw new Error(`item ${String(index)} is missing`);
  return item;
}
