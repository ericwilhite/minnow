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
import type { MaybePromise } from "../access/key-index.js";
import type { Operator, PushResult, Transfer } from "./pipeline.js";

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
  readonly #hold = new TransferHold();
  constructor(
    predicate: ScalarExpr,
    context: CompileContext,
    readonly downstream: Operator,
  ) {
    this.#predicate = new Predicate(predicate, context);
  }
  push(batch: Batch, transfer?: Transfer): PushResult {
    const selection = this.#predicate.select(batch);
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
  ready(): Promise<void> {
    return this.#hold.ready(this.downstream);
  }
  finish(): MaybePromise<void> {
    return this.downstream.finish();
  }
  abort(reason: unknown): void {
    this.#hold.abort();
    this.downstream.abort(reason);
  }
}

export class ProjectOperator implements Operator {
  readonly #exprs: readonly Compiled[];
  readonly #columnsOnly: readonly number[] | null;
  readonly #columns: Vector[];
  readonly types: readonly PhysicalType[];
  readonly #hold = new TransferHold();
  constructor(
    exprs: readonly ScalarExpr[],
    context: CompileContext,
    readonly downstream: Operator,
  ) {
    this.#columnsOnly = exprs.every((expr) => expr.op === "column")
      ? exprs.map((expr) => expr.index)
      : null;
    this.#exprs = exprs.map((expr) => compileExpr(expr, context));
    this.#columns = new Array<Vector>(exprs.length);
    this.types = this.#exprs.map((expr) => expr.type);
  }
  get reclassifiesTransfers(): boolean {
    return this.#columnsOnly !== null && this.downstream.reclassifiesTransfers === true;
  }
  push(batch: Batch, transfer?: Transfer): PushResult {
    if (transfer?.detachableBuffers && this.#columnsOnly !== null) {
      // New column-list ownership: a retaining receiver must never observe #columns reused.
      const columns = this.#columnsOnly.map((index) => {
        const vector = batch.columns[index];
        if (vector === undefined) throw new TypeError("Missing projected column");
        return vector;
      });
      return this.downstream.push({ ...batch, columns }, transfer);
    }
    for (let c = 0; c < this.#exprs.length; c++) {
      const expr = this.#exprs[c];
      if (expr === undefined) continue;
      this.#columns[c] = expr.evaluate(batch, batch.selection);
    }
    const result = this.downstream.push({
      length: batch.length,
      columns: this.#columns,
      selection: batch.selection,
    });
    return this.#hold.after(result, transfer);
  }
  ready(): Promise<void> {
    return this.#hold.ready(this.downstream);
  }
  finish(): MaybePromise<void> {
    return this.downstream.finish();
  }
  abort(reason: unknown): void {
    this.#hold.abort();
    this.downstream.abort(reason);
  }
}
