/** Task-sliced, inert FTS preparation; ordinary kernels still decide CASE/AND/OR error demand. */
import type { ScalarExpr } from "../ir/ir.js";
import type { Batch, Selection } from "../vector/vector.js";
import type { CompileContext } from "./kernels/compile.js";
import { installFtsHost, type FtsMatchNode } from "./kernels/fts.js";
import { FilterOperator, ProjectOperator } from "./filter.js";
import { Slice, runSliced } from "./slices.js";
import {
  yieldTask,
  afterYield,
  type Operator,
  type PushResult,
  type Transfer,
} from "./pipeline.js";
export class FtsExpressions implements Operator {
  readonly #nodes: FtsMatchNode[] = [];
  readonly #inner: Operator;
  readonly #slice = new Slice(4);
  readonly #one: Selection;
  #pending: Promise<void> = Promise.resolve();
  #aborted = false;
  #reason: unknown;
  constructor(
    exprs: readonly ScalarExpr[] | ScalarExpr,
    context: CompileContext,
    downstream: Operator,
    readonly options: { signal?: AbortSignal; stopped?: () => boolean } = {},
  ) {
    context.arena.take(512 + 2);
    this.#one = { rows: new Uint16Array(1), count: 1 };
    installFtsHost(context.arena, (node) => this.#nodes.push(node));
    try {
      this.#inner = Array.isArray(exprs)
        ? new ProjectOperator(exprs, context, downstream)
        : new FilterOperator(exprs as ScalarExpr, context, downstream);
    } finally {
      installFtsHost(context.arena, null);
    }
  }
  #check(): void {
    if (this.#aborted) throw this.#reason;
    this.options.signal?.throwIfAborted();
  }
  push(batch: Batch, transfer?: Transfer): PushResult {
    this.#check();
    this.#pending = this.#rows(batch).finally(() => {
      for (const node of this.#nodes) node.clear();
      transfer?.release();
    });
    return "blocked";
  }
  async #rows(batch: Batch): Promise<void> {
    const count = batch.selection?.count ?? batch.length;
    for (let k = 0; k < count; k++) {
      this.#check();
      if (this.options.stopped?.()) break;
      this.#one.rows[0] = batch.selection?.rows[k] ?? k;
      const current = { ...batch, selection: this.#one };
      for (const node of this.#nodes) {
        try {
          await runSliced(node.compute(current, this.#one, this.#slice), this.#slice, () =>
            this.#check(),
          );
        } catch (error) {
          this.#check();
          node.fail(error);
        }
      }
      if (this.#inner.push(current) === "blocked") await this.#inner.ready();
      for (const node of this.#nodes) node.clear();
      if (this.#slice.due()) {
        await yieldTask();
        afterYield(this.#slice.clock);
      }
    }
  }
  ready(): Promise<void> {
    return this.#pending;
  }
  async finish(): Promise<void> {
    await this.#pending;
    this.#check();
    await this.#inner.finish();
  }
  abort(reason: unknown): void {
    if (this.#aborted) return;
    this.#aborted = true;
    this.#reason = reason;
    this.#inner.abort(reason);
  }
  get closed(): Promise<void> {
    return this.#pending.catch(() => undefined);
  }
}
