/**
 * The `filter` and `project` operators (ir/ir.ts), over compiled kernels (exec/kernels/).
 *
 * - Filter splits its predicate's top-level AND into conjuncts and evaluates each only on the
 *   rows the previous ones kept, writing a selection vector: no row is copied, and a later
 *   conjunct never sees (or raises an error on) a row an earlier one rejected. A batch with no
 *   survivors is not pushed.
 * - Project evaluates every expression on the batch's active rows and pushes a batch of the
 *   results with the same selection (columns are the kernels' own output vectors, or input
 *   columns passed through).
 * - Both are synchronous and pass readiness through: `blocked` from downstream keeps the input
 *   batch borrowed (exec/pipeline.ts), and their output buffers are not touched until the next
 *   push.
 * - Computed projections and filters do not forward a `transfer`: the batch they push carries their own reusable selection or
 *   output vectors, which a retaining consumer must copy. They hold an incoming transfer until
 *   downstream is ready, then release it.
 */
import type { ScalarExpr } from "../ir/ir.js";
import type { PhysicalType } from "../types.js";
import type { Batch, Selection, Vector } from "../vector/vector.js";
import { compileExpr, selectTrue, type CompileContext } from "./kernels/compile.js";
import type { Compiled } from "./kernels/vectors.js";
import { projectionStep } from "./projection-step.js";
import { ExpressionMemoryError, IDENTITY } from "./kernels/vectors.js";
import { ExecutionError } from "./errors.js";
import { Slice } from "./slices.js";
import { afterYield, yieldTask, YIELD_INTERVAL_MS } from "./pipeline.js";
import type { MaybePromise } from "../access/key-index.js";
import type { Operator, PushResult, Transfer } from "./pipeline.js";

/**
 * Only data errors select precise replay. Cancellation, admission, capability and internal
 * failures retain their original ownership/error path. Replay is safe because volatile
 * kernel bindings are refused; revisit this protocol BEFORE adding any volatile kernel.
 */
function dataError(error: unknown): boolean {
  return error instanceof ExecutionError && error.sqlState !== "57014";
}

/** The conjuncts of `predicate` (nested ANDs flattened), in order. */
export function conjuncts(predicate: ScalarExpr): ScalarExpr[] {
  if (predicate.op !== "and") return [predicate];
  return predicate.args.flatMap(conjuncts);
}

/** Narrows a batch's selection to the rows every conjunct holds for. */
export class Predicate {
  readonly #conjuncts: readonly Compiled[];
  readonly #a: Selection;
  readonly #b: Selection;
  constructor(predicate: ScalarExpr, context: CompileContext) {
    this.#conjuncts = conjuncts(predicate).map((conjunct) => compileExpr(conjunct, context));
    const capacity = context.arena.capacity;
    context.arena.take(4 * capacity);
    this.#a = { rows: new Uint16Array(capacity), count: 0 };
    this.#b = { rows: new Uint16Array(capacity), count: 0 };
  }
  /** The surviving rows (owned by this predicate until its next call). */
  select(batch: Batch): Selection {
    let rows: Selection | null = batch.selection;
    let out = this.#a;
    for (const conjunct of this.#conjuncts) {
      rows = selectTrue(conjunct, batch, rows, out);
      if (rows.count === 0) return rows;
      out = out === this.#a ? this.#b : this.#a;
    }
    if (rows === null) {
      // No conjuncts: every row.
      const all = this.#a;
      for (let k = 0; k < batch.length; k++) all.rows[k] = k;
      all.count = batch.length;
      return all;
    }
    return rows;
  }
}

/** Holds an input transfer until downstream readiness, then releases it. */
class TransferHold {
  #held: Transfer | null = null;
  after(result: PushResult, transfer: Transfer | undefined): PushResult {
    if (transfer === undefined) return result;
    if (result === "ready") transfer.release();
    else this.#held = transfer;
    return result;
  }
  async ready(downstream: Operator): Promise<void> {
    await downstream.ready();
    this.#held?.release();
    this.#held = null;
  }
  abort(): void {
    this.#held?.release();
    this.#held = null;
  }
}

export class FilterOperator implements Operator {
  readonly #predicate: Predicate;
  readonly #slice = new Slice(YIELD_INTERVAL_MS);
  readonly #hold = new TransferHold();
  #pending: Promise<void> | null = null;
  #aborted = false;
  #reason: unknown;
  constructor(
    predicate: ScalarExpr,
    readonly context: CompileContext,
    readonly downstream: Operator,
  ) {
    this.#predicate = new Predicate(predicate, context);
  }
  push(batch: Batch, transfer?: Transfer): PushResult {
    this.#pending = null;
    let selection: Selection;
    try {
      selection = this.#predicate.select(batch);
    } catch (error) {
      if (!dataError(error)) throw error;
      this.#pending = this.#precise(batch).finally(() => transfer?.release());
      return "blocked";
    }
    if (selection.count === 0) {
      transfer?.release();
      return "ready";
    }
    const all = selection.count === batch.length;
    const result = this.downstream.push({
      length: batch.length,
      columns: batch.columns,
      selection: all ? null : selection,
    });
    return this.#hold.after(result, transfer);
  }
  async #precise(batch: Batch): Promise<void> {
    const arena = this.context.arena;
    arena.retireVarlen();
    const borrow = arena.borrowVarlen;
    arena.borrowVarlen = true;
    try {
      const rows = batch.selection?.rows ?? IDENTITY;
      for (let k = 0; k < (batch.selection?.count ?? batch.length); k++) {
        if (this.#aborted) throw this.#reason;
        const one = { ...batch, selection: { rows: rows.subarray(k, k + 1), count: 1 } };
        const selection = this.#predicate.select(one);
        if (selection.count && this.downstream.push({ ...one, selection }) === "blocked")
          await this.downstream.ready();
        arena.retireVarlen();
        if (this.#slice.due()) {
          await yieldTask();
          afterYield(this.#slice.clock);
        }
      }
    } finally {
      arena.borrowVarlen = borrow;
    }
  }
  get closed(): MaybePromise<void> {
    return this.#pending?.catch(() => undefined);
  }
  ready(): Promise<void> {
    return this.#pending ?? this.#hold.ready(this.downstream);
  }
  finish(): MaybePromise<void> {
    return this.downstream.finish();
  }
  abort(reason: unknown): void {
    this.#aborted = true;
    this.#reason = reason;
    this.#hold.abort();
    this.downstream.abort(reason);
  }
}

export class ProjectOperator implements Operator {
  #pending: Promise<void> | null = null;
  #aborted = false;
  #reason: unknown;
  readonly #step: ReturnType<typeof projectionStep>;
  readonly #slice = new Slice(YIELD_INTERVAL_MS);
  readonly #exprs: readonly Compiled[];
  readonly #columnsOnly: readonly number[] | null;
  readonly #columns: Vector[];
  readonly types: readonly PhysicalType[];
  readonly #hold = new TransferHold();
  constructor(
    exprs: readonly ScalarExpr[],
    context: CompileContext,
    readonly downstream: Operator,
    readonly progress?: { available(): number; readonly signal: AbortSignal },
  ) {
    this.#step = progress ? projectionStep(exprs, context.params) : null;
    this.#context = context;
    this.#columnsOnly = exprs.every(
      (expr) =>
        expr.op === "column" ||
        (expr.op === "cast" && expr.jsonCanonical && expr.input.op === "column"),
    )
      ? exprs.map((expr) =>
          expr.op === "column"
            ? expr.index
            : expr.op === "cast" && expr.input.op === "column"
              ? expr.input.index
              : -1,
        )
      : null;
    this.#exprs = exprs.map((expr) => compileExpr(expr, context));
    this.#columns = new Array<Vector>(exprs.length);
    this.types = this.#exprs.map((expr) => expr.type);
  }
  readonly #context: CompileContext;
  get reclassifiesTransfers(): boolean {
    return this.#columnsOnly !== null && this.downstream.reclassifiesTransfers === true;
  }
  push(batch: Batch, transfer?: Transfer): PushResult {
    this.#check();
    this.#pending = null;
    if (transfer?.detachableBuffers && this.#columnsOnly !== null) {
      // New column-list ownership: a retaining receiver must never observe #columns reused.
      const columns = this.#columnsOnly.map((index, c) => {
        const vector = batch.columns[index];
        if (vector === undefined) throw new TypeError("Missing projected column");
        const type = this.types[c];
        return type && type.kind !== vector.type.kind ? { ...vector, type } : vector;
      });
      return this.downstream.push({ ...batch, columns }, transfer);
    }
    try {
      this.#evaluate(batch);
    } catch (error) {
      if (error instanceof ExpressionMemoryError && this.#step && this.progress) {
        // Failed attempts delivered nothing. Release their partial payload before sizing;
        // successful ordinary batches never scan widths or consult the preflight recipe.
        this.#context.arena.retireVarlen();
        this.#pending = this.#steps(batch).finally(() => transfer?.release());
      } else if (dataError(error)) {
        this.#pending = this.#precise(batch).finally(() => transfer?.release());
      } else throw error;
      return "blocked";
    }
    return this.#deliver(batch, transfer);
  }
  #evaluate(batch: Batch): void {
    for (let c = 0; c < this.#exprs.length; c++) {
      const expr = this.#exprs[c];
      if (expr !== undefined) this.#columns[c] = expr.evaluate(batch, batch.selection);
    }
  }
  #deliver(batch: Batch, transfer?: Transfer): PushResult {
    const result = this.downstream.push({
      length: batch.length,
      columns: this.#columns,
      selection: batch.selection,
    });
    return this.#hold.after(result, transfer);
  }
  async #precise(batch: Batch): Promise<void> {
    const arena = this.#context.arena;
    arena.retireVarlen();
    const borrow = arena.borrowVarlen,
      exact = arena.exactVarlenGrowth;
    arena.borrowVarlen = true;
    arena.exactVarlenGrowth = true;
    try {
      const rows = batch.selection?.rows ?? IDENTITY;
      for (let k = 0; k < (batch.selection?.count ?? batch.length); k++) {
        this.#check();
        const one = { ...batch, selection: { rows: rows.subarray(k, k + 1), count: 1 } };
        // Row first, then target-list order. A row is offered only after every expression
        // succeeded, and its borrow expires before evaluating the next row.
        this.#evaluate(one);
        if (this.#deliver(one) === "blocked") await this.#hold.ready(this.downstream);
        this.#columns.length = 0;
        arena.retireVarlen();
        if (this.#slice.due()) {
          await yieldTask();
          afterYield(this.#slice.clock);
        }
      }
    } finally {
      arena.borrowVarlen = borrow;
      arena.exactVarlenGrowth = exact;
    }
  }
  async #steps(batch: Batch): Promise<void> {
    const n = batch.selection?.count ?? batch.length;
    const progress = this.progress,
      step = this.#step;
    if (!progress || !step) throw new Error("Missing projection preflight");
    const arena = this.#context.arena;
    const borrow = arena.borrowVarlen,
      exact = arena.exactVarlenGrowth;
    try {
      arena.borrowVarlen = true;
      arena.exactVarlenGrowth = true;
      for (let start = 0; start < n;) {
        this.#check();
        let count = n - start;
        while (count > 1 && step(batch, start, count) > progress.available())
          count = Math.max(1, Math.floor(count / 2));
        const rows = (batch.selection?.rows ?? IDENTITY).subarray(start, start + count);
        const part = { ...batch, selection: { rows, count } };
        try {
          this.#evaluate(part);
        } catch (error) {
          if (!dataError(error)) throw error;
          await this.#precise(part);
          start += count;
          continue;
        }
        if (this.#deliver(part) === "blocked") await this.#hold.ready(this.downstream);
        // Retire only expired outputs. Otherwise a small pressure arena overlaps a
        // following full batch's replacement, exceeding the normal fitting peak.
        this.#columns.length = 0;
        arena.retireVarlen();
        start += count;
        if (this.#slice.due()) {
          await yieldTask();
          afterYield(this.#slice.clock);
        }
      }
    } finally {
      arena.borrowVarlen = borrow;
      arena.exactVarlenGrowth = exact;
    }
  }
  #check(): void {
    if (this.#aborted) throw this.#reason;
    this.progress?.signal.throwIfAborted();
  }
  get closed(): MaybePromise<void> {
    // An ordinary projection never suspended. Do not turn synchronous execution
    // into a microtask merely by registering its optional borrow fence.
    return this.#pending?.catch(() => undefined);
  }
  ready(): Promise<void> {
    return this.#pending ?? this.#hold.ready(this.downstream);
  }
  finish(): MaybePromise<void> {
    return this.downstream.finish();
  }
  abort(reason: unknown): void {
    this.#aborted = true;
    this.#reason = reason;
    this.#hold.abort();
    this.downstream.abort(reason);
  }
}
