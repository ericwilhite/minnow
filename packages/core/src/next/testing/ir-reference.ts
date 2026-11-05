/**
 * A row-at-a-time reference interpreter for the IR subset lowering emits at milestone M1, used
 * only by tests: it checks that lowered plans compute what the current engine computes
 * (lowering-differential.test.ts) before the real executor exists. It is deliberately simple
 * and slow, and it is not the executor's specification: the IR contract comments are.
 *
 * Values: null; booleans; every integer as an exact `bigint`, range-checked at its type
 * (PostgreSQL's 22003); floats as numbers; text as strings. Decimal constants are approximated
 * as numbers (the tests only cast them to floats and integers); decimal arithmetic, intervals,
 * dates and JSON are outside the subset and throw `ReferenceUnsupportedError`.
 * Tables are arrays of rows in catalog order; a locator is the row's index.
 */
import type { CatalogView, TableDef } from "../access/catalog.js";
import type { Rel, ScalarExpr, SortKey } from "../ir/ir.js";
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

const isInteger = (type: PhysicalType) => type.kind === "int32" || type.kind === "int64";
const isFloat = (type: PhysicalType) => type.kind === "float32" || type.kind === "float64";

function castValue(value: Value, from: PhysicalType, to: PhysicalType): Value {
  if (value === null) return null;
  if (isInteger(to)) {
    if (typeof value === "bigint") return checkRange(value, to);
    if (typeof value === "number")
      return checkRange(BigInt(Math.sign(value) * Math.round(Math.abs(value))), to);
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
        return Math.sign(x) * Math.round(Math.abs(x));
    }
  }
  throw new ReferenceUnsupportedError(`${fn} over ${type.kind}`);
}

export interface Environment {
  readonly tables: Map<number, Row[]>;
  readonly catalog: CatalogView;
  readonly params: readonly Value[];
  readonly rules: (table: TableDef) => TableRuleExpressions;
  sequences: Map<number, bigint>;
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
    case "param":
      return env.params[expr.slot] ?? null;
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
      const sorted = sortRows(run(rel.input, env), rel.keys, env);
      const offset = boundValue(rel.offset, env) ?? 0;
      const limit = boundValue(rel.limit, env);
      return sorted.slice(offset, limit === null ? undefined : offset + limit);
    }
    case "limit": {
      const rows = run(rel.input, env);
      const offset = boundValue(rel.offset, env) ?? 0;
      const limit = boundValue(rel.limit, env);
      return rows.slice(offset, limit === null ? undefined : offset + limit);
    }
    case "modify":
      return modifyRows(rel, env);
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
  const returning = (row: Row): void => {
    if (rel.returning !== null)
      returned.push(rel.returning.map((expr) => evaluate(expr, row, env)));
  };
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
          for (const assignment of rel.onConflict.assignments)
            next[table.columns.findIndex((column) => column.id === assignment.column)] = evaluate(
              assignment.expr,
              both,
              env,
            );
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
      if (existing === undefined || existing.length === 0) continue;
      if (rel.kind === "delete") {
        returning(existing);
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
      returning(next);
    }
  }
  return returned;
}
