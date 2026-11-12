/** Ordinary one-group transitions alongside a file-backed JSON aggregate. */
import type { AggregateCall } from "../../ir/ir.js";
import { BATCH_ROWS, type Batch, type Vector } from "../../vector/vector.js";
import { runSliced } from "../slices.js";
import { UnsupportedPlanError } from "../errors.js";
import { accumulatorFor, type Accumulator } from "./states.js";
import {
  jsonArgument,
  jsonFilter,
  type JsonAggregateHost,
  type JsonTransition,
} from "./json-values.js";

const SUPPORTED = new Set([
  "count_star",
  "count",
  "sum",
  "avg",
  "min",
  "max",
  "bool_and",
  "bool_or",
  "single_value",
]);
export function ordinaryJsonCall(call: AggregateCall): void {
  if (!SUPPORTED.has(call.fn) || call.distinct || call.orderBy.length)
    throw new UnsupportedPlanError(
      "ordered/distinct or non-scalar transition beside JSON aggregation",
      { construct: "aggregate" },
    );
}
export class JsonOrdinaryTransition implements JsonTransition {
  readonly #ids: Uint16Array;
  readonly #gids: Int32Array;
  #state: Accumulator;
  #output: Vector | null = null;
  #outputBytes = 0;
  #closed = false;
  constructor(
    readonly call: AggregateCall,
    readonly host: JsonAggregateHost,
  ) {
    ordinaryJsonCall(call);
    if (!host.pool.allocate(6 * BATCH_ROWS))
      throw new Error("JSON sibling transition metadata not admitted");
    this.#ids = new Uint16Array(BATCH_ROWS);
    this.#gids = new Int32Array(BATCH_ROWS);
    this.#state = this.#create();
  }
  #create(): Accumulator {
    const state = accumulatorFor(
      this.call.fn,
      this.call.type,
      this.call.args[0]?.type ?? null,
      this.host.pool,
    );
    state.grow(1);
    return state;
  }
  async add(batch: Batch, ids: Uint16Array, from: number, count: number): Promise<void> {
    const value = this.call.args.length
      ? (batch.columns[jsonArgument(this.call, 0)] ?? null)
      : null;
    let n = 0;
    for (let i = from; i < from + count; i++) {
      const row = ids[i] ?? 0;
      if (jsonFilter(this.call, batch, row)) this.#ids[n++] = row;
    }
    if (!n) return;
    const need = this.#state.growthBytes?.(1, value, n) ?? 0;
    if (need)
      await runSliced(this.host.memory.take(need), this.host.slice, () => this.host.check());
    this.host.pool.withStep(() => {
      if (need) this.host.pool.free(need);
      this.#state.update(value, this.#gids, this.#ids, n, true);
    });
  }
  async result(): Promise<Vector> {
    if (this.#output) return this.#output;
    this.#outputBytes = this.#state.outputBytes(0, 1);
    await runSliced(this.host.memory.take(this.#outputBytes), this.host.slice, () =>
      this.host.check(),
    );
    while (this.#state.prepare && this.#state.prepare(0, 1, () => this.host.slice.due()) === 0)
      await this.host.pace();
    return (this.#output = this.#state.emit(0, 1));
  }
  #reset(): void {
    if (this.#outputBytes) this.host.pool.free(this.#outputBytes);
    this.#outputBytes = 0;
    this.#output = null;
    this.#state.close();
  }
  restart(): Promise<void> {
    this.#reset();
    this.#state = this.#create();
    return Promise.resolve();
  }
  abort(): void {
    return undefined;
  }
  close(): Promise<void> {
    if (!this.#closed) {
      this.#closed = true;
      this.#reset();
      this.host.pool.free(6 * BATCH_ROWS);
    }
    return Promise.resolve();
  }
}
