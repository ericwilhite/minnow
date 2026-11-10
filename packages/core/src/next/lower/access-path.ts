/**
 * How one table is read: `keyLookup` when a unique key is fixed by constants (a point read,
 * ruling R6), `indexRange` when an ordered index bounds the rows selectively, else `scan` with
 * the conjuncts as its filter.
 *
 * Works on typed conjuncts over the table read's own positions (plan.ts `TableRead`), so it sees
 * exactly what the executor will evaluate: a comparison PostgreSQL resolves by widening the
 * column (`int4_col = 5000000000` compares `int8`) is never a key, and a key or bound is the
 * constant itself at the column's base type (review B6-p2a finding 1). Only lossless access is
 * chosen; every conjunct an access path does not answer stays a filter.
 *
 * ## Ranges (ir.ts `indexRange`)
 * - Only `ordered` indexes (never an enum's `equality` index), and only comparisons under the
 *   default byte-order collation (C8): text bounds with an explicit COLLATE stay filters.
 * - The bounds are an equality prefix of the index's columns, then at most one range on the
 *   next column (`<`, `<=`, `>`, `>=`; BETWEEN is two of them).
 * - Chosen when the estimated fraction of rows is at most `INDEX_RANGE_MAX_FRACTION`
 *   (statistics bounds and distinct counts when known; otherwise PostgreSQL's defaults:
 *   DEFAULT_EQ_SEL 0.005 per equality, DEFAULT_INEQ_SEL 1/3 one-sided, DEFAULT_RANGE_INEQ_SEL
 *   0.005 two-sided).
 */
import type { ColumnDef, IndexDef } from "../access/catalog.js";
import type { Rel, ScalarExpr } from "../ir/ir.js";
import { jsonExpression } from "../ir/json-expression.js";
import type { StatValue } from "../types.js";
import type { Typed } from "./coerce.js";
import { integerLiteralValue } from "./literals.js";
import { andOf, DEFAULT_TABLE_ROWS, required, type PlanBuilder, type TableRead } from "./plan.js";

/** Index ranges are chosen only when they keep at most this fraction of the table's rows. */
export const INDEX_RANGE_MAX_FRACTION = 0.05;

const DEFAULT_EQ_SEL = 0.005;
const DEFAULT_INEQ_SEL = 1 / 3;
const DEFAULT_RANGE_INEQ_SEL = 0.005;

export interface AccessPath {
  readonly rel: Rel;
  readonly pointRead: boolean;
  /** Estimated output rows. */
  readonly rows: number;
}

/** A constant usable as a key: a literal (not NULL), a parameter, or a text constant parsed once. */
function isKey(expr: ScalarExpr): boolean {
  if (expr.op === "literal") return expr.value.validity === null;
  if (expr.op === "param") return true;
  return (
    expr.op === "cast" &&
    expr.assignment !== true &&
    expr.input.op === "literal" &&
    expr.input.type.kind === "text" &&
    expr.type.kind !== "text"
  );
}

const sameBaseKind = (a: ScalarExpr, b: ScalarExpr) => a.type.kind === b.type.kind;

interface Bound {
  readonly key: ScalarExpr;
  readonly inclusive: boolean;
  readonly conjunct: number;
}

/** What the conjuncts say about each read position. */
interface Facts {
  readonly equal: Map<number, { readonly key: ScalarExpr; readonly conjunct: number }>;
  readonly lists: Map<number, { readonly keys: readonly ScalarExpr[]; readonly conjunct: number }>;
  readonly lower: Map<number, Bound>;
  readonly upper: Map<number, Bound>;
}

function factsOf(conjuncts: readonly Typed[]): Facts {
  const facts: Facts = { equal: new Map(), lists: new Map(), lower: new Map(), upper: new Map() };
  for (const [index, conjunct] of conjuncts.entries()) {
    const expr = conjunct.expr;
    if (expr.op === "inList" && !expr.negated && expr.arg.op === "column") {
      const position = expr.arg.index;
      if (
        expr.list.length > 0 &&
        expr.list.every((item) => isKey(item) && sameBaseKind(item, expr.arg))
      )
        if (!facts.lists.has(position))
          facts.lists.set(position, { keys: expr.list, conjunct: index });
      continue;
    }
    if (expr.op !== "compare") continue;
    const flip = { "<": ">", "<=": ">=", ">": "<", ">=": "<=", "=": "=", "<>": "<>" } as const;
    const [columnSide, keySide, cmp] =
      expr.left.op === "column" && isKey(expr.right)
        ? [expr.left, expr.right, expr.cmp]
        : expr.right.op === "column" && isKey(expr.left)
          ? [expr.right, expr.left, flip[expr.cmp]]
          : [null, null, null];
    if (columnSide === null) continue;
    if (!sameBaseKind(columnSide, keySide)) continue;
    const position = columnSide.index;
    if (cmp === "=") {
      if (!facts.equal.has(position)) facts.equal.set(position, { key: keySide, conjunct: index });
      continue;
    }
    // Byte order is index order only under the default collation (C8).
    if (expr.collation !== null && expr.collation !== "C") continue;
    if (cmp === ">" || cmp === ">=") {
      if (!facts.lower.has(position))
        facts.lower.set(position, { key: keySide, inclusive: cmp === ">=", conjunct: index });
    } else if (cmp === "<" || cmp === "<=") {
      if (!facts.upper.has(position))
        facts.upper.set(position, { key: keySide, inclusive: cmp === "<=", conjunct: index });
    }
  }
  return facts;
}

/** A stable identity of a key constant, to read each distinct key tuple once. */
function keyIdentity(expr: ScalarExpr): string | null {
  if (expr.op === "param") return `p${String(expr.slot)}`;
  const integer = integerLiteralValue(expr);
  if (integer !== null) return `i${String(integer)}`;
  if (expr.op === "literal" && expr.value.data.layout === "varlen") {
    const data = expr.value.data;
    return `t${new TextDecoder().decode(data.bytes.subarray(data.offsets[0] ?? 0, data.offsets[1] ?? 0))}`;
  }
  return null;
}

function dedupe(tuples: ReadonlyArray<readonly ScalarExpr[]>): Array<readonly ScalarExpr[]> {
  const seen = new Set<string>();
  const out: Array<readonly ScalarExpr[]> = [];
  for (const tuple of tuples) {
    const parts = tuple.map(keyIdentity);
    const identity = parts.every((part) => part !== null) ? parts.join("\u0000") : null;
    if (identity !== null) {
      if (seen.has(identity)) continue;
      seen.add(identity);
    }
    out.push(tuple);
  }
  return out;
}

function numberOf(value: StatValue | null): number | null {
  if (typeof value === "number") return value;
  if (typeof value === "bigint") return Number(value);
  return null;
}

/** Estimated fraction of rows a range on one column keeps (statistics bounds, else defaults). */
function rangeFraction(
  def: ColumnDef,
  lower: Bound | undefined,
  upper: Bound | undefined,
  builder: PlanBuilder,
  tableId: number,
): number {
  const stats = builder.statistics.table(tableId)?.column(def.id) ?? null;
  const min = numberOf(stats?.min ?? null);
  const max = numberOf(stats?.max ?? null);
  const at = (bound: Bound | undefined): number | null => {
    if (bound === undefined) return null;
    const value = integerLiteralValue(bound.key);
    return value === null ? null : Number(value);
  };
  const low = at(lower);
  const high = at(upper);
  if (
    min !== null &&
    max !== null &&
    max > min &&
    (lower === undefined || low !== null) &&
    (upper === undefined || high !== null)
  ) {
    const from = Math.max(min, low ?? min);
    const to = Math.min(max, high ?? max);
    return Math.max(0, Math.min(1, (to - from) / (max - min)));
  }
  if (lower !== undefined && upper !== undefined) return DEFAULT_RANGE_INEQ_SEL;
  return DEFAULT_INEQ_SEL;
}

/**
 * The access path of `read` under `conjuncts` (typed over the read's positions). `locator`: the
 * output also carries the row locator, last.
 */
export function accessPath(
  read: TableRead,
  conjuncts: readonly Typed[],
  builder: PlanBuilder,
  locator: boolean,
): AccessPath {
  const table = read.table;
  const facts = factsOf(conjuncts);
  const positionOf = (columnId: number): number | undefined => {
    const position = read.columns.indexOf(columnId);
    return position === -1 ? undefined : position;
  };
  const tableRows = builder.statistics.table(table.id)?.rowCount ?? DEFAULT_TABLE_ROWS;
  const output = read.output(locator);
  const residual = (used: ReadonlySet<number>): ScalarExpr | null =>
    andOf(conjuncts.filter((_, index) => !used.has(index)));
  const withFilter = (rel: Rel, used: ReadonlySet<number>): Rel => {
    const predicate = residual(used);
    return predicate === null
      ? rel
      : { op: "filter", id: builder.id(), input: rel, predicate, output };
  };

  // 1. A unique index fully fixed by equalities (or one column by an IN list): keyLookup.
  const unique = table.indexes
    .filter((index) => index.unique)
    .sort((a, b) => Number(b.role === "primary") - Number(a.role === "primary"));
  for (const index of unique) {
    const positions = index.columnIds.map(positionOf);
    if (positions.some((position) => position === undefined)) continue;
    const fixed = positions as number[];
    let tuples: Array<readonly ScalarExpr[]> | null = null;
    let used = new Set<number>();
    if (fixed.every((position) => facts.equal.has(position))) {
      tuples = [
        fixed.map((position) => required(facts.equal.get(position)?.key, "an equality key")),
      ];
      used = new Set(fixed.map((position) => facts.equal.get(position)?.conjunct ?? -1));
    } else if (fixed.length === 1 && facts.lists.has(fixed[0] ?? -1)) {
      const list = facts.lists.get(fixed[0] ?? -1);
      if (list !== undefined) {
        tuples = list.keys.map((key) => [key]);
        used = new Set([list.conjunct]);
      }
    }
    if (tuples === null) continue;
    const keys = dedupe(tuples);
    const lookup: Rel = {
      op: "keyLookup",
      id: builder.id(),
      tableId: table.id,
      indexId: index.id,
      keys,
      columns: [...read.columns],
      ...(locator ? { emitLocator: true } : {}),
      output,
    };
    return { rel: withFilter(lookup, used), pointRead: true, rows: keys.length };
  }

  // 2. An ordered index bounding few rows: indexRange.
  let best: { index: IndexDef; fraction: number; rel: Rel; used: Set<number> } | null = null;
  for (const index of table.indexes) {
    if (index.kind !== "ordered") continue;
    const prefix: ScalarExpr[] = [];
    const used = new Set<number>();
    let fraction = 1;
    let next: ColumnDef | undefined;
    for (const columnId of index.columnIds) {
      const position = positionOf(columnId);
      const def = table.columns.find((candidate) => candidate.id === columnId);
      const equal = position === undefined ? undefined : facts.equal.get(position);
      if (equal === undefined || def === undefined) {
        next = def;
        break;
      }
      prefix.push(equal.key);
      used.add(equal.conjunct);
      const distinct = builder.statistics.table(table.id)?.column(columnId)?.distinct ?? null;
      fraction *= distinct !== null && distinct > 0 ? 1 / distinct : DEFAULT_EQ_SEL;
    }
    const position = next === undefined ? undefined : positionOf(next.id);
    const lower = position === undefined ? undefined : facts.lower.get(position);
    const upper = position === undefined ? undefined : facts.upper.get(position);
    if (prefix.length === 0 && lower === undefined && upper === undefined) continue;
    // A range is costed against the scan: the choice depends on the statistics.
    builder.dependsOnStatistics();
    if (next !== undefined && (lower !== undefined || upper !== undefined)) {
      fraction *= rangeFraction(next, lower, upper, builder, table.id);
      if (lower !== undefined) used.add(lower.conjunct);
      if (upper !== undefined) used.add(upper.conjunct);
    }
    if (fraction > INDEX_RANGE_MAX_FRACTION || (best !== null && fraction >= best.fraction))
      continue;
    const bound = (range: Bound | undefined) =>
      prefix.length === 0 && range === undefined
        ? null
        : {
            keys: range === undefined ? prefix : [...prefix, range.key],
            inclusive: range?.inclusive ?? true,
          };
    const rel: Rel = {
      op: "indexRange",
      id: builder.id(),
      tableId: table.id,
      indexId: index.id,
      lower: bound(lower),
      upper: bound(upper),
      descending: false,
      columns: [...read.columns],
      emitLocator: locator,
      output,
    };
    best = { index, fraction, rel, used };
  }
  if (best !== null)
    return {
      rel: withFilter(best.rel, best.used),
      pointRead: false,
      rows: Math.max(1, tableRows * best.fraction),
    };

  // 3. Synchronous scan predicates cannot own resumable JSON workspaces. Keep the full
  // conjunction in one filter frame, preserving its short-circuit order and IR profiles.
  const predicate = andOf(conjuncts);
  const deferred = predicate !== null && jsonExpression(predicate);
  const scan: Rel = {
    op: "scan",
    id: builder.id(),
    tableId: table.id,
    columns: [...read.columns],
    filter: deferred ? null : predicate,
    runtimeFilters: [],
    emitLocator: locator,
    output,
  };
  return {
    rel: deferred ? { op: "filter", id: builder.id(), input: scan, predicate, output } : scan,
    pointRead: false,
    rows: Math.max(1, tableRows * selectivityOf(conjuncts)),
  };
}

/** A rough fraction of rows a set of conjuncts keeps (planning only). */
export function selectivityOf(conjuncts: readonly Typed[]): number {
  let fraction = 1;
  for (const conjunct of conjuncts) {
    const expr = conjunct.expr;
    if (expr.op === "compare") fraction *= expr.cmp === "=" ? DEFAULT_EQ_SEL : DEFAULT_INEQ_SEL;
    else if (expr.op === "inList") fraction *= Math.min(1, DEFAULT_EQ_SEL * expr.list.length);
    else if (expr.op === "isNull") fraction *= expr.negated ? 0.95 : 0.05;
    else fraction *= DEFAULT_INEQ_SEL;
  }
  return fraction;
}
