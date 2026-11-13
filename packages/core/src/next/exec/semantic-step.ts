/** One charged pair of decoded key values, reused across native JSON consumers. */
import type { Grant } from "../memory/governor.js";
import type { PhysicalType } from "../types.js";
import type { Vector } from "../vector/vector.js";
import type { ByteCharge } from "../vector/varlen.js";
import type { Pool } from "./spill-pool.js";
import {
  compareRows,
  decodeOperands,
  dropOperands,
  emptyOperands,
  needsOperands,
  operandBytes,
  type SortColumn,
} from "./sort-keys.js";

/** At most two values of ONE key column coexist, never the sum across keys. */
export function semanticStepBytes(
  types: readonly PhysicalType[],
  keys: readonly SortColumn[],
  columns: readonly Vector[],
  from: number,
  to: number,
): number {
  let maximum = 0;
  for (const key of keys) {
    const one = [key];
    if (!needsOperands(types, one)) continue;
    for (let row = from; row < to; row++)
      maximum = Math.max(maximum, 2 * operandBytes(types, one, columns, row, row + 1));
  }
  return maximum;
}

class StepBudget implements ByteCharge {
  #used = 0;
  constructor(readonly bytes: number) {}
  allocate(bytes: number): boolean {
    if (this.#used + bytes > this.bytes) return false;
    this.#used += bytes;
    return true;
  }
  free(bytes: number): void {
    this.#used -= bytes;
    if (this.#used < 0) throw new Error("Semantic step released unowned bytes");
  }
}

/**
 * Optional cached operands remain ordinary native allocations. When they do not fit,
 * each sort compares through this one prepaid workspace. It must release the workspace
 * before calling its downstream consumer, which may itself compare group boundaries.
 */
export class SemanticStep {
  #owner: object | null = null;
  #bytes = 0;
  #unpin: (() => void) | null = null;
  #pending: Promise<void> | null = null;
  #closed = false;
  peakBytes = 0;
  comparisons = 0;
  constructor(
    readonly pool: Pool,
    readonly grant: Grant,
    readonly signal: AbortSignal,
  ) {}
  prepare(owner: object, bytes: number): Promise<void> {
    this.signal.throwIfAborted();
    if (this.#closed) throw new Error("Semantic workspace is closed");
    if (this.#owner !== null || this.#pending !== null)
      throw new Error("Semantic workspace retained across a consumer handoff");
    this.#owner = owner;
    const work = (async () => {
      try {
        await this.pool.need(bytes, this.grant, this.signal);
        this.signal.throwIfAborted();
        if (this.#closed) throw new Error("Semantic workspace closed during admission");
        this.#unpin = this.pool.pinStep();
        if (!this.pool.allocate(bytes)) throw new Error("Semantic workspace was not admitted");
        this.#bytes = bytes;
        this.peakBytes = Math.max(this.peakBytes, bytes);
      } catch (error) {
        this.release(owner);
        // A cancelled admission may have reserved room without consuming it yet.
        this.pool.allocate(0);
        this.pool.settle();
        throw error;
      }
    })();
    this.#pending = work;
    void work.then(
      () => {
        if (this.#pending === work) this.#pending = null;
      },
      () => {
        if (this.#pending === work) this.#pending = null;
      },
    );
    return work;
  }
  compare(
    owner: object,
    types: readonly PhysicalType[],
    keys: readonly SortColumn[],
    left: readonly Vector[],
    i: number,
    right: readonly Vector[],
    j: number,
  ): number {
    if (this.#owner !== owner) throw new Error("Semantic comparison has no admitted owner");
    const budget = new StepBudget(this.#bytes);
    this.comparisons++;
    for (const key of keys) {
      this.signal.throwIfAborted();
      const one = [key];
      if (!needsOperands(types, one)) {
        const order = compareRows(types, one, left, i, right, j);
        if (order !== 0) return order;
        continue;
      }
      const a = emptyOperands(types, one),
        b = emptyOperands(types, one);
      try {
        if (
          decodeOperands(types, one, left, i, i + 1, budget, a) !== 0 ||
          decodeOperands(types, one, right, j, j + 1, budget, b) !== 0
        )
          throw new Error("Semantic comparison exceeded its prepaid workspace");
        const order = compareRows(types, one, left, i, right, j, a, b, 0, 0);
        if (order !== 0) return order;
      } finally {
        dropOperands(a, budget);
        dropOperands(b, budget);
      }
    }
    return 0;
  }
  release(owner: object): void {
    if (this.#owner !== owner) return;
    if (this.#bytes > 0) this.pool.free(this.#bytes);
    this.#bytes = 0;
    this.#unpin?.();
    this.#unpin = null;
    this.#owner = null;
  }
  async close(): Promise<void> {
    this.#closed = true;
    await this.#pending?.catch(() => undefined);
    if (this.#owner) this.release(this.#owner);
  }
}
