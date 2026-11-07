/** LIMIT/OFFSET over active rows. The factory stops this pipeline's source once `done` is true. */
import type { MaybePromise } from "../access/key-index.js";
import type { Batch, Selection } from "../vector/vector.js";
import type { OutputArena } from "./kernels/vectors.js";
import type { Operator, PushResult, Transfer } from "./pipeline.js";

export class LimitOperator implements Operator {
  readonly #selection: Selection;
  #skip: number;
  #remaining: number;
  #held: Transfer | null = null;
  constructor(
    limit: number | null,
    offset: number,
    arena: OutputArena,
    readonly downstream: Operator,
  ) {
    if (
      offset < 0 ||
      !Number.isSafeInteger(offset) ||
      (limit !== null && (limit < 0 || !Number.isSafeInteger(limit)))
    )
      throw new TypeError("LIMIT/OFFSET must be bound to nonnegative safe integers");
    this.#skip = offset;
    this.#remaining = limit ?? Infinity;
    arena.take(2 * arena.capacity);
    this.#selection = { rows: new Uint16Array(arena.capacity), count: 0 };
  }

  get done(): boolean {
    return this.#remaining === 0;
  }

  push(batch: Batch, transfer?: Transfer): PushResult {
    const count = batch.selection?.count ?? batch.length;
    const skip = Math.min(this.#skip, count);
    this.#skip -= skip;
    const take = Math.min(this.#remaining, count - skip);
    this.#remaining -= take;
    if (take === 0) {
      transfer?.release();
      return "ready";
    }
    const selection = this.#selection;
    selection.count = take;
    for (let k = 0; k < take; k++) selection.rows[k] = batch.selection?.rows[skip + k] ?? skip + k;
    try {
      const result = this.downstream.push({
        length: batch.length,
        columns: batch.columns,
        selection: batch.selection === null && take === batch.length ? null : selection,
      });
      if (result === "blocked") this.#held = transfer ?? null;
      else transfer?.release();
      return result;
    } catch (error) {
      transfer?.release();
      throw error;
    }
  }

  async ready(): Promise<void> {
    try {
      await this.downstream.ready();
    } finally {
      this.#release();
    }
  }

  finish(): MaybePromise<void> {
    return this.downstream.finish();
  }

  abort(reason: unknown): void {
    this.#release();
    this.downstream.abort(reason);
  }

  #release(): void {
    const held = this.#held;
    this.#held = null;
    held?.release();
  }
}
