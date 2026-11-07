/**
 * The scan's predicate pushdown: which filter conjuncts go to the page format, and the one small
 * interface the scan calls for them. The format's read-side pushdown (page/predicate.ts,
 * page/select.ts; next/README "Predicates on encoded pages": `ChunkPages.compilePredicate`,
 * `pagesMatching`, `select`, `refine`, `selectMask`, and `chunkMayMatch`) evaluates them on
 * encoded pages. Only where it does not push a predicate down for a chunk (`compilePredicate`
 * returns null) does `DecodedColumn` evaluate the same comparison with the executor's kernels on
 * decoded values; that path does not prune by statistics.
 *
 * ## What is pushed down
 * A conjunct comparing one scan column with non-null constants, the shapes B2 compiles:
 * `col <op> literal` (either side), `col IN (literals)` and `col IS [NOT] NULL`, over types whose
 * constants have a stored form (integers, dates, times, timestamps, floats, NUMERIC, bool, text,
 * bytes, uuid) and, for text, under the C (byte-order) collation. Everything else is the
 * residual, evaluated on decoded values after the pushed conjuncts. Semantics are PostgreSQL's
 * either way (NULL never satisfies a comparison; NaN = NaN, above +Infinity; −0 = 0; NUMERIC
 * exact), so where a conjunct runs never changes the answer.
 *
 * ## The scan's calls, per column chunk (`PushedColumn`)
 * prune the group (`mayMatch` over the chunk's statistics), `prepare` what a range or a selection
 * needs (pages; the fallback also out-of-line values), then `select` the qualifying positions of a
 * range, `refine` an existing selection, or `selectMask` a 0/1 mask (the dense fused path).
 */
import type { ScalarExpr } from "../ir/ir.js";
import type { ColumnStatistics, PhysicalType } from "../types.js";
import {
  chunkMayMatch,
  type ColumnPredicate,
  type CompiledPredicate,
  type PredicateConstant,
} from "../page/predicate.js";
import type { FlatVector, Vector } from "../vector/vector.js";
import type { MaybePromise } from "../access/key-index.js";
import { compileExpr, selectTrue, type CompileContext } from "./kernels/compile.js";
import { int128At } from "../vector/int128.js";
import type { Compiled } from "./kernels/vectors.js";
import { isPromise } from "./pipeline.js";
import type { ChunkPages, ColumnChunkRef } from "./scan.js";
import { decodeInto, ColumnTarget } from "./column-target.js";

/** `xs[i]`, which must exist. */
function at<T>(xs: readonly T[], i: number): T {
  const x = xs[i];
  if (x === undefined) throw new RangeError(`no element ${String(i)}`);
  return x;
}

// ── B2's predicate shapes (page/predicate.ts) ─────────────────────────────────────────────────

export type { ColumnPredicate, DecimalConstant, PredicateConstant } from "../page/predicate.js";

// ── Extraction ──────────────────────────────────────────────────────────────────────────────

/** A conjunct pushed to one scan column: B2's form and the IR form (for the fallback). */
export interface PushedPredicate {
  /** Scan output column it tests. */
  readonly column: number;
  readonly predicate: ColumnPredicate;
  /** The conjunct with that column renumbered to 0 (the fallback compiles it). */
  readonly expr: ScalarExpr;
}

export interface SplitFilter {
  readonly pushed: readonly PushedPredicate[];
  /** The conjuncts left, over the scan's output (null: none). */
  readonly residual: ScalarExpr | null;
}

const FLIP = { "=": "=", "<>": "<>", "<": ">", "<=": ">=", ">": "<", ">=": "<=" } as const;

function slotOf(vector: FlatVector): { valid: boolean } {
  const validity = vector.validity;
  return { valid: validity === null || ((validity[0] ?? 0) & 1) === 1 };
}

/** A literal's stored-form constant for a column of `type`, or null when it has none. */
export function constantOf(literal: FlatVector, type: PhysicalType): PredicateConstant | null {
  if (literal.length < 1 || !slotOf(literal).valid) return null;
  const data = literal.data;
  switch (type.kind) {
    case "int32":
    case "date":
      return data.layout === "i32" ? (data.values[0] ?? 0) : null;
    case "int64":
    case "time":
    case "timestamp":
      if (data.layout !== "int64") return null;
      return data.values.repr === "safe"
        ? (data.values.values[0] ?? 0)
        : (data.values.values[0] ?? 0n);
    case "float64":
    case "float32":
      return data.layout === "f64" || data.layout === "f32" ? (data.values[0] ?? 0) : null;
    case "bool":
      return data.layout === "bool" ? data.values[0] === 1 : null;
    case "decimal": {
      if (data.layout !== "decimal") return null;
      const special = data.specials?.[0] ?? 0;
      const scale = data.scale ?? data.scales?.[0] ?? 0;
      if (special !== 0)
        return {
          coefficient: 0n,
          scale,
          special: special === 1 ? "NaN" : special === 2 ? "Infinity" : "-Infinity",
        };
      const c = data.coefficients;
      const coefficient =
        c.repr === "safe"
          ? BigInt(c.values[0] ?? 0)
          : c.repr === "int64"
            ? (c.values[0] ?? 0n)
            : c.repr === "int128"
              ? int128At(c.limbs, 0)
              : (c.values[0] ?? 0n);
      return { coefficient, scale };
    }
    case "text":
    case "bytes":
      return data.layout === "varlen"
        ? data.bytes.slice(data.offsets[0] ?? 0, data.offsets[1] ?? 0)
        : null;
    case "uuid":
      return data.layout === "fixed16" ? data.bytes.slice(0, 16) : null;
    default:
      return null;
  }
}

const isLiteral = (expr: ScalarExpr): expr is Extract<ScalarExpr, { op: "literal" }> =>
  expr.op === "literal";
const isColumn = (expr: ScalarExpr): expr is Extract<ScalarExpr, { op: "column" }> =>
  expr.op === "column";
const byteOrder = (collation: string | null): boolean =>
  collation === null || collation === "C" || collation === "default";

/** The conjunct as a pushed predicate over scan column `types`, or null (it stays residual). */
function pushable(expr: ScalarExpr, types: readonly PhysicalType[]): PushedPredicate | null {
  const renumber = (column: number, rest: (column0: ScalarExpr) => ScalarExpr): ScalarExpr =>
    rest({ op: "column", index: 0, type: at(types, column) });
  switch (expr.op) {
    case "compare": {
      let column: Extract<ScalarExpr, { op: "column" }>;
      let literal: Extract<ScalarExpr, { op: "literal" }>;
      let op = expr.cmp;
      if (isColumn(expr.left) && isLiteral(expr.right)) {
        column = expr.left;
        literal = expr.right;
      } else if (isLiteral(expr.left) && isColumn(expr.right)) {
        column = expr.right;
        literal = expr.left;
        op = FLIP[op];
      } else return null;
      const type = types[column.index];
      if (type === undefined || (type.kind === "text" && !byteOrder(expr.collation))) return null;
      const value = constantOf(literal.value, type);
      if (value === null) return null;
      return {
        column: column.index,
        predicate: { op, value },
        expr: renumber(column.index, (c) => ({
          ...expr,
          left: c,
          right: literal,
          cmp: op,
        })),
      };
    }
    case "inList": {
      if (expr.negated || !isColumn(expr.arg) || !expr.list.every(isLiteral)) return null;
      const column = expr.arg;
      const type = types[column.index];
      if (type === undefined || (type.kind === "text" && !byteOrder(expr.collation))) return null;
      const values: PredicateConstant[] = [];
      for (const item of expr.list) {
        // A NULL item never makes IN true (it only turns false into NULL, which WHERE drops).
        if (!slotOf(item.value).valid) continue;
        const value = constantOf(item.value, type);
        if (value === null) return null;
        values.push(value);
      }
      return {
        column: column.index,
        predicate: { op: "in", values },
        expr: renumber(column.index, (c) => ({ ...expr, arg: c })),
      };
    }
    case "isNull": {
      if (!isColumn(expr.arg) || types[expr.arg.index] === undefined) return null;
      return {
        column: expr.arg.index,
        predicate: { op: expr.negated ? "isNotNull" : "isNull" },
        expr: renumber(expr.arg.index, (c) => ({ ...expr, arg: c })),
      };
    }
    default:
      return null;
  }
}

/** Splits a scan filter's conjuncts into pushed predicates and the residual. */
export function splitFilter(
  filter: ScalarExpr | null,
  types: readonly PhysicalType[],
): SplitFilter {
  if (filter === null) return { pushed: [], residual: null };
  const all = filter.op === "and" ? flatten(filter) : [filter];
  const pushed: PushedPredicate[] = [];
  const rest: ScalarExpr[] = [];
  for (const conjunct of all) {
    const p = pushable(conjunct, types);
    if (p === null) rest.push(conjunct);
    else pushed.push(p);
  }
  const residual: ScalarExpr | null =
    rest.length === 0
      ? null
      : rest.length === 1
        ? at(rest, 0)
        : { op: "and", args: rest, type: { kind: "bool" } };
  return { pushed, residual };
}

function flatten(expr: ScalarExpr): ScalarExpr[] {
  return expr.op === "and" ? expr.args.flatMap(flatten) : [expr];
}

// ── The seam ────────────────────────────────────────────────────────────────────────────────

/** One pushed predicate over one column chunk. */
export interface PushedColumn {
  /** Evaluated on encoded pages (B2), or on decoded values (the fallback). */
  readonly encoded: boolean;
  /** Whether the chunk's statistics allow a qualifying row (false prunes the group). */
  mayMatch(rowCount: number, statistics: ColumnStatistics): boolean;
  /** Loads what `select(from, to)` needs (pages it cannot rule out). */
  prepare(from: number, to: number): MaybePromise<void>;
  /** Loads what `refine(rows, …)` needs. */
  prepareRows(rows: Uint16Array, count: number): MaybePromise<void>;
  /** Qualifying group positions of `[from, to)`, ascending, into `out[0 …]`; returns how many. */
  select(from: number, to: number, out: Uint16Array): number;
  /** Keeps the positions `rows[0 … count)` that qualify (in place); returns how many. */
  refine(rows: Uint16Array, count: number): number;
  /** 1 at `mask[k]` when row `from + k` qualifies, else 0; returns how many are 1. */
  selectMask(from: number, to: number, mask: Uint8Array): number;
}

/** Per-scan counters; cache hits still open an encoded page in this chunk reader. */
export interface PageWork {
  pagesRead: number;
  pagesPruned: number;
}

export function loadProfiled(
  pages: ChunkPages,
  indexes: readonly number[],
  work?: PageWork,
): MaybePromise<void> {
  if (work !== undefined) for (const index of indexes) if (!pages.isLoaded(index)) work.pagesRead++;
  return pages.load(indexes);
}

/**
 * The pushed predicate over a chunk: B2's encoded-page evaluation (`ChunkPages.compilePredicate`
 * and the calls after it, `chunkMayMatch` for the group), or, when it does not push the predicate
 * down for this chunk (it returns null: interval, json, arrays, mixed-scale decimals, enum order)
 * or `encoded` is false (tests), the decoded fallback.
 */
export function pushedColumn(
  pages: ChunkPages,
  _chunk: ColumnChunkRef,
  pushed: PushedPredicate,
  fallback: CompiledFallback,
  encoded = true,
  work?: PageWork,
): PushedColumn {
  if (encoded) {
    const compiled = pages.compilePredicate(pushed.predicate);
    if (compiled !== null) return new EncodedColumn(pages, compiled, work);
  }
  return new DecodedColumn(pages, fallback, work);
}

/** B2's pushdown on encoded pages. */
class EncodedColumn implements PushedColumn {
  readonly encoded = true;
  #prunedThrough = 0;
  constructor(
    readonly pages: ChunkPages,
    readonly compiled: CompiledPredicate,
    readonly work?: PageWork,
  ) {}
  mayMatch(rowCount: number, statistics: ColumnStatistics): boolean {
    return chunkMayMatch(this.compiled, rowCount, statistics);
  }
  prepare(from: number, to: number): MaybePromise<void> {
    const matching = this.pages.pagesMatching(this.compiled, from, to);
    const [first, end] = this.pages.pagesInRange(from, to);
    if (this.work !== undefined) {
      // Scan ranges advance monotonically. A page straddling two ranges is credited once.
      let kept = 0;
      for (const page of matching) if (page >= this.#prunedThrough) kept++;
      this.work.pagesPruned += Math.max(0, end - Math.max(first, this.#prunedThrough) - kept);
    }
    this.#prunedThrough = Math.max(this.#prunedThrough, end);
    return loadProfiled(this.pages, matching, this.work);
  }
  prepareRows(rows: Uint16Array, count: number): MaybePromise<void> {
    return loadProfiled(this.pages, this.pages.pagesForRows(rows, 0, count), this.work);
  }
  select(from: number, to: number, out: Uint16Array): number {
    return this.pages.select(this.compiled, from, to, out, 0);
  }
  refine(rows: Uint16Array, count: number): number {
    return this.pages.refine(this.compiled, rows, 0, count, rows, 0);
  }
  selectMask(from: number, to: number, mask: Uint8Array): number {
    return this.pages.selectMask(this.compiled, from, to, mask, 0);
  }
}

/** A pushed predicate compiled once with the executor's kernels (over column 0). */
export class CompiledFallback {
  readonly predicate: Compiled;
  readonly target: ColumnTarget;
  readonly selection = { rows: new Uint16Array(0), count: 0 };
  readonly #out: { rows: Uint16Array; count: number };
  constructor(pushed: PushedPredicate, type: PhysicalType, context: CompileContext) {
    this.predicate = compileExpr(pushed.expr, { ...context, input: [type] });
    const capacity = context.arena.capacity;
    context.arena.take(2 * capacity);
    this.#out = { rows: new Uint16Array(capacity), count: 0 };
    this.target = new ColumnTarget(type, capacity, context.arena);
  }
  /** Slots of `vector` (a batch of `length` rows) where the predicate is true. */
  evaluate(vector: Vector, length: number): { rows: Uint16Array; count: number } {
    return selectTrue(
      this.predicate,
      { length, columns: [vector], selection: null },
      null,
      this.#out,
    );
  }
}

/** The fallback: decode the column, evaluate the comparison with the kernels. */
class DecodedColumn implements PushedColumn {
  readonly encoded = false;
  constructor(
    readonly pages: ChunkPages,
    readonly fallback: CompiledFallback,
    readonly work?: PageWork,
  ) {}
  mayMatch(): boolean {
    return true;
  }
  prepare(from: number, to: number): MaybePromise<void> {
    const [first, end] = this.pages.pagesInRange(from, to);
    const loaded = loadProfiled(
      this.pages,
      Array.from({ length: end - first }, (_, k) => first + k),
      this.work,
    );
    return andThen(loaded, () => this.pages.loadValues(from, to));
  }
  prepareRows(rows: Uint16Array, count: number): MaybePromise<void> {
    const loaded = loadProfiled(this.pages, this.pages.pagesForRows(rows, 0, count), this.work);
    return andThen(loaded, () => this.pages.loadValuesOf(rows, 0, count));
  }
  select(from: number, to: number, out: Uint16Array): number {
    const vector = decodeInto(this.fallback.target, this.pages, { from, to });
    const hits = this.fallback.evaluate(vector, to - from);
    for (let k = 0; k < hits.count; k++) out[k] = from + (hits.rows[k] ?? 0);
    return hits.count;
  }
  refine(rows: Uint16Array, count: number): number {
    const vector = decodeInto(this.fallback.target, this.pages, { rows, count });
    const hits = this.fallback.evaluate(vector, count);
    for (let k = 0; k < hits.count; k++) rows[k] = rows[hits.rows[k] ?? 0] ?? 0;
    return hits.count;
  }
  selectMask(from: number, to: number, mask: Uint8Array): number {
    const vector = decodeInto(this.fallback.target, this.pages, { from, to });
    const hits = this.fallback.evaluate(vector, to - from);
    mask.fill(0, 0, to - from);
    for (let k = 0; k < hits.count; k++) mask[hits.rows[k] ?? 0] = 1;
    return hits.count;
  }
}

/** `f()` after `value` (a value or a Promise), synchronously when it is a value. */
export function andThen<T, U>(
  value: MaybePromise<T>,
  f: (value: T) => MaybePromise<U>,
): MaybePromise<U> {
  return isPromise(value) ? value.then(f) : f(value);
}
