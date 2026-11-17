/** Body duplicate targets are checked before assignment expressions, in source row order.
 * A duplicate follows the already sealed prefix rather than masking its earlier row rules. */
import type { Batch, Vector } from "../vector/vector.js";
import type { ByteCharge } from "../vector/varlen.js";
import { Slice } from "./slices.js";
import {
  pushAndWait,
  yieldTask,
  YIELD_INTERVAL_MS,
  type Operator,
  type Transfer,
} from "./pipeline.js";
export interface NativeInputTargets {
  readonly charge: ByteCharge;
  visit(locator: Vector, row: number): Promise<boolean>;
}
export class NativeTargetGuard implements Operator {
  #rows: Uint16Array | null;
  #pending: Promise<void> | null = null;
  #failure: { reason: unknown } | null = null;
  #closed = false;
  constructor(
    readonly target: NativeInputTargets,
    readonly column: number,
    readonly downstream: Operator,
    readonly signal: AbortSignal,
  ) {
    if (!target.charge.allocate(256)) throw new RangeError("Native target guard must be admitted");
    try {
      this.#rows = new Uint16Array(1);
    } catch (error) {
      target.charge.free(256);
      throw error;
    }
  }
  #check(): void {
    this.signal.throwIfAborted();
    if (this.#failure) throw this.#failure.reason;
  }
  push(batch: Batch, transfer?: Transfer): "blocked" {
    if (this.#closed || this.#pending)
      throw new TypeError("Native input guard is closed or active");
    const work = (async () => {
      const rows = this.#rows;
      if (!rows) throw new TypeError("Native input guard retired");
      const slice = new Slice(YIELD_INTERVAL_MS),
        locator = batch.columns[this.column];
      if (!locator) throw new TypeError("Native target locator missing");
      try {
        for (let k = 0; k < (batch.selection?.count ?? batch.length); k++) {
          this.#check();
          const row = batch.selection?.rows[k] ?? k;
          if (!(await this.target.visit(locator, row))) continue;
          this.#check();
          rows[0] = row;
          await pushAndWait(this.downstream, { ...batch, selection: { rows, count: 1 } });
          if (slice.due()) await yieldTask();
        }
      } finally {
        transfer?.release();
      }
    })();
    const pending = work.finally(() => {
      if (this.#pending === pending) this.#pending = null;
    });
    this.#pending = pending;
    return "blocked";
  }
  ready(): Promise<void> {
    return this.#pending ?? Promise.resolve();
  }
  finish() {
    return this.downstream.finish();
  }
  abort(reason: unknown): void {
    this.#failure ??= { reason };
    this.downstream.abort(reason);
  }
  async close(): Promise<void> {
    if (this.#closed) return;
    this.#closed = true;
    await this.#pending?.catch(() => undefined);
    this.#rows = null;
    this.target.charge.free(256);
  }
}
