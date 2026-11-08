/** Exact, opt-in COUNT(*)/SUM(float8) over a certified int32 address range.
 *
 * A complete probe (or sound snapshot statistics) must precede seal(). Every
 * actual key is still range checked. NULL/out-of-range keys always use a
 * disjoint ordinary aggregate, so an obsolete/wider hint cannot alias groups.
 * No prefix is replayed, merged or reassociated. Direct-state admission is
 * optional: refusal retains the ordinary plan, without losing any input.
 */
import type { Rel } from "../../ir/ir.js";
import { OBJECT_OVERHEAD_BYTES } from "../../memory/governor.js";
import type { ByteCharge } from "../../vector/varlen.js";
import { BATCH_ROWS, type Batch, type FlatVector, type Vector } from "../../vector/vector.js";
import { NumericOutOfRangeError } from "../errors.js";
import type { CompileContext } from "../kernels/compile.js";
import { IDENTITY, isValidSlot, slotOf } from "../kernels/vectors.js";
import {
  afterYield,
  clockFor,
  yieldTask,
  YIELD_INTERVAL_MS,
  type Operator,
  type TaskClock,
} from "../pipeline.js";
import { AggregateOperator, type AggregateOptions } from "./aggregate.js";
import { Charged } from "./groups.js";

type AggregateRel = Extract<Rel, { op: "aggregate" }>;

/** Deliberately narrow: both finalizers are copy-only and ranking has a total tie break. */
export function supportsDomainCountSum(
  rel: AggregateRel,
  context: CompileContext,
  options: AggregateOptions,
): boolean {
  const [count, sum] = rel.aggregates,
    top = options.outputTopN;
  return (
    rel.groupBy.length === 1 &&
    context.input[rel.groupBy[0] ?? -1]?.kind === "int32" &&
    rel.aggregates.length === 2 &&
    count?.fn === "count_star" &&
    count.type.kind === "int64" &&
    count.args.length === 0 &&
    sum?.fn === "sum" &&
    sum.type.kind === "float64" &&
    sum.args.length === 1 &&
    sum.args[0]?.op === "column" &&
    context.input[sum.args[0].index]?.kind === "float64" &&
    rel.aggregates.every((a) => !a.distinct && a.filter === null && a.orderBy.length === 0) &&
    top !== undefined &&
    Number.isSafeInteger(top.count) &&
    top.count > 0 &&
    top.count <= BATCH_ROWS &&
    top.keys.length === 2 &&
    top.keys[0]?.column === 1 &&
    top.keys[0].direction === "desc" &&
    top.keys[1]?.column === 0 &&
    top.keys[1].direction === "asc"
  );
}

function numberAt(vector: Vector, row: number): number | null {
  if (
    vector.form !== "constant" &&
    vector.validity !== null &&
    ((vector.validity[row >>> 5] ?? 0) & (1 << (row & 31))) === 0
  )
    return null;
  const [flat, slot] = slotOf(vector, row);
  if (!isValidSlot(flat, slot)) return null;
  if (flat.data.layout !== "i32" && flat.data.layout !== "f64")
    throw new TypeError("Domain aggregation requires int32/float64 vectors");
  return flat.data.values[slot] ?? 0;
}

export class DomainCountSumOperator implements Operator {
  readonly #fallback: AggregateOperator;
  readonly #memory: Charged;
  readonly #outside: { rows: Uint16Array; count: number };
  readonly #heap: Int32Array;
  readonly #outputKeys: Int32Array;
  readonly #outputCounts: Float64Array;
  readonly #outputSums: Float64Array;
  readonly #outputValidity: Uint32Array;
  readonly #clock: TaskClock;
  readonly #key: number;
  readonly #sum: number;
  readonly #countType;
  readonly #keyType;
  readonly #sumType;
  #state = new Float64Array(0);
  #stateBytes = 0;
  #low = Infinity;
  #high = -Infinity;
  #probeRows = 0;
  #sealed = false;
  #ranked = 0;
  #used = 0;
  #directDone = false;
  #closed = false;
  #aborted = false;
  #reason: unknown;
  #emitted = false;
  #nullable = false;
  directRows = 0;

  constructor(
    rel: AggregateRel,
    context: CompileContext,
    readonly charge: ByteCharge,
    readonly downstream: Operator,
    readonly options: AggregateOptions,
  ) {
    if (!supportsDomainCountSum(rel, context, options))
      throw new TypeError("Unsupported direct-domain shape");
    this.#key = rel.groupBy[0] ?? 0;
    const sum = rel.aggregates[1],
      arg = sum?.args[0];
    if (arg?.op !== "column" || !sum) throw new TypeError("Direct SUM column required");
    this.#sum = arg.index;
    const keyType = context.input[this.#key],
      countType = rel.aggregates[0]?.type;
    if (!keyType || !countType) throw new TypeError("Output types");
    this.#keyType = keyType;
    this.#countType = countType;
    this.#sumType = sum.type;
    this.#clock = clockFor(options.now);
    this.#fallback = new AggregateOperator(rel, context, charge, downstream, options);
    this.#memory = new Charged(charge);
    try {
      // Fixed scratch, output buffers and their headers are part of admission.
      // Selecting winners needs no growth while the direct table is retained.
      this.#memory.take(32 * OBJECT_OVERHEAD_BYTES);
      this.#outside = { rows: this.#memory.make(Uint16Array, context.arena.capacity), count: 0 };
      this.#heap = this.#memory.make(Int32Array, options.outputTopN?.count ?? 0);
      this.#outputKeys = this.#memory.make(Int32Array, this.#heap.length);
      this.#outputCounts = this.#memory.make(Float64Array, this.#heap.length);
      this.#outputSums = this.#memory.make(Float64Array, this.#heap.length);
      this.#outputValidity = this.#memory.make(Uint32Array, (this.#heap.length + 31) >>> 5);
    } catch (error) {
      this.#memory.close();
      this.#fallback.abort(error);
      throw error;
    }
  }

  /** Probe only active keys; retain no borrowed vector or page. */
  probe(batch: Batch): void {
    if (this.#sealed) throw new TypeError("Domain already sealed");
    const key = batch.columns[this.#key];
    if (!key) throw new TypeError("Missing domain key");
    const count = batch.selection?.count ?? batch.length,
      rows = batch.selection?.rows ?? IDENTITY;
    this.#probeRows += count;
    if (key.form === "flat" && key.validity === null && key.data.layout === "i32") {
      const values = key.data.values;
      let low = this.#low,
        high = this.#high;
      for (let k = 0; k < count; k++) {
        const value = values[rows[k] ?? 0] ?? 0;
        if (value < low) low = value;
        if (value > high) high = value;
      }
      this.#low = low;
      this.#high = high;
      return;
    }
    for (let k = 0; k < count; k++) {
      const value = numberAt(key, rows[k] ?? 0);
      if (value === null) continue;
      this.#low = Math.min(this.#low, value);
      this.#high = Math.max(this.#high, value);
    }
  }

  /** Bounds may be conservative, but must be sound over the same pinned snapshot. */
  bounds(low: number, high: number, rows: number): void {
    if (this.#sealed) throw new TypeError("Domain already sealed");
    this.#low = low;
    this.#high = high;
    this.#probeRows = rows;
  }

  seal(): void {
    if (this.#sealed) throw new TypeError("Domain already sealed");
    this.#sealed = true;
    const span = this.#high - this.#low + 1;
    if (
      !Number.isSafeInteger(this.#probeRows) ||
      this.#probeRows < 0 ||
      !Number.isInteger(this.#low) ||
      this.#low < -2147483648 ||
      !Number.isInteger(this.#high) ||
      this.#high > 2147483647 ||
      span < 1 ||
      span > 1 << 22
    )
      return;
    const bytes = 16 * span + 2 * OBJECT_OVERHEAD_BYTES;
    if (!this.charge.allocate(bytes)) return;
    this.#stateBytes = bytes;
    try {
      this.#state = new Float64Array(2 * span);
      this.#state.fill(-0);
    } catch (error) {
      this.#releaseState();
      throw error;
    }
  }

  push(batch: Batch): "ready" {
    this.#check();
    if (!this.#sealed) throw new TypeError("Direct domain requires a completed probe");
    if (this.#state.length === 0) {
      this.#fallback.push(batch);
      return "ready";
    }
    const keys = batch.columns[this.#key],
      values = batch.columns[this.#sum];
    if (!keys || !values) throw new TypeError("Missing aggregate input");
    const n = batch.selection?.count ?? batch.length,
      rows = batch.selection?.rows ?? IDENTITY;
    const state = this.#state,
      low = this.#low,
      high = this.#high,
      out = this.#outside;
    out.count = 0;
    if (
      keys.form === "flat" &&
      keys.validity === null &&
      keys.data.layout === "i32" &&
      values.form === "flat" &&
      values.validity === null &&
      values.data.layout === "f64"
    ) {
      const keyData = keys.data.values,
        valueData = values.data.values;
      for (let k = 0; k < n; k++) {
        const row = rows[k] ?? 0,
          key = keyData[row] ?? 0;
        if (key < low || key > high) {
          out.rows[out.count++] = row;
          continue;
        }
        const at = (key - low) * 2,
          value = valueData[row] ?? 0,
          before = state[at + 1] ?? -0;
        const sum = before + value;
        if (
          !Number.isFinite(sum) &&
          !Number.isNaN(sum) &&
          Number.isFinite(before) &&
          Number.isFinite(value)
        )
          throw new NumericOutOfRangeError("value out of range: overflow");
        state[at] = (this.#nullable ? Math.abs(state[at] ?? 0) : (state[at] ?? 0)) + 1;
        state[at + 1] = sum;
        this.directRows++;
      }
    } else {
      this.#nullable = true;
      for (let k = 0; k < n; k++) {
        const row = rows[k] ?? 0,
          key = numberAt(keys, row);
        if (key === null || key < low || key > high) {
          out.rows[out.count++] = row;
          continue;
        }
        const at = (key - low) * 2,
          value = numberAt(values, row),
          count = state[at] ?? 0;
        if (value === null) state[at] = count > 0 ? count + 1 : count - 1;
        else {
          const before = state[at + 1] ?? -0,
            sum = before + value;
          if (
            !Number.isFinite(sum) &&
            !Number.isNaN(sum) &&
            Number.isFinite(before) &&
            Number.isFinite(value)
          )
            throw new NumericOutOfRangeError("value out of range: overflow");
          state[at] = Math.abs(count) + 1;
          state[at + 1] = sum;
        }
        this.directRows++;
      }
    }
    if (out.count > 0) this.#fallback.push({ ...batch, selection: out });
    return "ready";
  }

  ready(): Promise<void> {
    return Promise.resolve();
  }

  #worse(a: number, b: number): boolean {
    const x = Math.abs(this.#state[a * 2] ?? 0),
      y = Math.abs(this.#state[b * 2] ?? 0);
    return x < y || (x === y && a > b);
  }

  #offer(g: number): void {
    const heap = this.#heap;
    if (this.#used < heap.length) {
      let p = this.#used++;
      while (p > 0) {
        const up = (p - 1) >>> 1,
          prior = heap[up] ?? 0;
        if (!this.#worse(g, prior)) break;
        heap[p] = prior;
        p = up;
      }
      heap[p] = g;
    } else if (this.#worse(heap[0] ?? 0, g)) {
      let p = 0;
      while (2 * p + 1 < this.#used) {
        let child = p * 2 + 1;
        if (child + 1 < this.#used && this.#worse(heap[child + 1] ?? 0, heap[child] ?? 0)) child++;
        if (!this.#worse(heap[child] ?? 0, g)) break;
        heap[p] = heap[child] ?? 0;
        p = child;
      }
      heap[p] = g;
    }
  }

  finish(): void | Promise<void> {
    this.#check();
    if (this.#directDone) return this.#fallback.finish();
    while (this.#ranked < this.#state.length / 2) {
      if (this.#clock.due(this.options.yieldIntervalMs ?? YIELD_INTERVAL_MS))
        return yieldTask().then(() => {
          afterYield(this.#clock);
          return this.finish();
        });
      const until = Math.min(this.#ranked + BATCH_ROWS, this.#state.length / 2);
      let worst = this.#heap[0] ?? 0,
        cutoff = Math.abs(this.#state[worst * 2] ?? 0);
      while (this.#ranked < until) {
        const g = this.#ranked++;
        const count = Math.abs(this.#state[g * 2] ?? 0);
        if (count === 0) continue;
        // The table is immutable during finish. Most groups lose against a
        // stable root; read it only after an offer actually changes the heap.
        if (
          this.#used === this.#heap.length &&
          (count < cutoff || (count === cutoff && g >= worst))
        )
          continue;
        this.#offer(g);
        worst = this.#heap[0] ?? 0;
        cutoff = Math.abs(this.#state[worst * 2] ?? 0);
      }
      this.#check();
    }
    if (!this.#emitted && this.#used > 0) {
      const n = this.#used,
        keys = this.#outputKeys,
        counts = this.#outputCounts,
        sums = this.#outputSums,
        validity = this.#outputValidity;
      for (let i = 0; i < n; i++) {
        const g = this.#heap[i] ?? 0,
          count = this.#state[g * 2] ?? 0;
        keys[i] = g + this.#low;
        counts[i] = Math.abs(count);
        sums[i] = this.#state[g * 2 + 1] ?? -0;
        if (count > 0) validity[i >>> 5] = (validity[i >>> 5] ?? 0) | (1 << (i & 31));
      }
      // No read lease or table charge crosses a downstream request. The bounded
      // winning payload was reserved at admission and is owned by this operator.
      this.#releaseState();
      const columns: FlatVector[] = [
        {
          form: "flat",
          type: this.#keyType,
          length: n,
          validity: null,
          data: { layout: "i32", values: keys },
        },
        {
          form: "flat",
          type: this.#countType,
          length: n,
          validity: null,
          data: { layout: "int64", values: { repr: "safe", values: counts } },
        },
        {
          form: "flat",
          type: this.#sumType,
          length: n,
          validity,
          data: { layout: "f64", values: sums },
        },
      ];
      this.#emitted = true;
      if (this.downstream.push({ length: n, columns, selection: null }) === "blocked")
        return this.downstream.ready().then(
          () => {
            return this.finish();
          },
          (reason: unknown) => {
            this.abort(reason);
            throw reason;
          },
        );
    }
    this.#directDone = true;
    this.#close();
    return this.#fallback.finish();
  }

  #check(): void {
    if (!this.#aborted && this.options.signal?.aborted) this.abort(this.options.signal.reason);
    if (this.#aborted) throw this.#reason;
  }
  #releaseState(): void {
    if (this.#stateBytes > 0) this.charge.free(this.#stateBytes);
    this.#stateBytes = 0;
    this.#state = new Float64Array(0);
  }
  #close(): void {
    if (this.#closed) return;
    this.#closed = true;
    this.#releaseState();
    this.#memory.close();
  }
  abort(reason: unknown): void {
    this.#aborted = true;
    this.#reason = reason;
    this.#close();
    this.#fallback.abort(reason);
  }
}
