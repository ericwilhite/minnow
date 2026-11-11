/** Fully prepaid numeric percentiles: hash keys once, scatter values per group, select ranks.
 * No transition state is combined or reassociated. Unknown/big workspaces use ordered-set.ts.
 */
import type { Rel } from "../../ir/ir.js";
import type { PhysicalType } from "../../types.js";
import type { ByteCharge } from "../../vector/varlen.js";
import { BATCH_ROWS, type Batch, type Vector, type FlatVector } from "../../vector/vector.js";
import { ExecutionError } from "../errors.js";
import { type CompileContext } from "../kernels/compile.js";
import {
  countIn,
  rowsIn,
  slotOf,
  isValidSlot,
  type Compiled,
  type OutputVector,
} from "../kernels/vectors.js";
import { type Operator, type PushResult, YIELD_INTERVAL_MS } from "../pipeline.js";
import { Slice, runSliced, type SlicedJob } from "../slices.js";
import { Charged, GroupTable } from "./groups.js";
import { orderedSetArguments } from "./ordered-set.js";
import { selectNumber } from "./selection.js";

type AggregateRel = Extract<Rel, { op: "aggregate" }>;
const EMPTY: Batch = { length: 1, selection: null, columns: [] };

export function supportsPercentile(rel: AggregateRel, input: readonly PhysicalType[]): boolean {
  return (
    rel.groupingSets === null &&
    rel.aggregates.length > 0 &&
    rel.aggregates.length <= 8 &&
    rel.aggregates.every((call) => {
      const order = call.orderBy[0];
      return (
        (call.fn === "percentile_cont" || call.fn === "percentile_disc") &&
        !call.distinct &&
        call.filter === null &&
        call.orderBy.length === 1 &&
        order?.expr.op === "column" &&
        ["float64", "int32"].includes(input[order.expr.index]?.kind ?? "") &&
        (call.fn !== "percentile_cont" || order.expr.type.kind === "float64")
      );
    })
  );
}

export interface PercentileBound {
  readonly rows: number;
  readonly groups: number;
  readonly keyMinimum: number;
}
/** Values+validity per call, row group ids, shared partition workspace, group offsets,
 * and output/constructor overhead. The independently proved keyMinimum covers GroupTable.
 */
export function percentileMinimum(
  rel: AggregateRel,
  bound: PercentileBound,
  compiler: number,
): number {
  if (
    !Number.isSafeInteger(bound.rows) ||
    bound.rows < 0 ||
    bound.rows > 2 ** 21 ||
    !Number.isSafeInteger(bound.groups) ||
    bound.groups < 1 ||
    bound.groups > 1024
  )
    throw new RangeError("Invalid percentile certificate");
  return (
    compiler +
    bound.keyMinimum +
    bound.rows * (12 + 9 * rel.aggregates.length) +
    256 * 1024 +
    32 * (bound.groups + 1)
  );
}

export class PercentileOperator implements Operator {
  readonly #memory: Charged;
  readonly #groups: GroupTable | null;
  readonly #ids: Int32Array;
  readonly #batchIds: Int32Array;
  readonly #values: Float64Array[] = [];
  readonly #valid: Uint8Array[] = [];
  readonly #scratch: Float64Array;
  readonly #counts: Int32Array;
  readonly #offsets: Int32Array;
  readonly #heads: Int32Array;
  readonly #fractions: Array<Compiled | null>;
  readonly #output: OutputVector[];
  readonly #keys: Vector[] = [];
  readonly #slice = new Slice(YIELD_INTERVAL_MS);
  #rows = 0;
  #pending: Promise<void> | null = null;
  #aborted = false;
  #reason: unknown;
  #closed = false;
  #outputHeld = 0;
  constructor(
    readonly rel: AggregateRel,
    readonly context: CompileContext,
    readonly charge: ByteCharge,
    readonly bound: PercentileBound,
    readonly signal: AbortSignal,
    readonly downstream: Operator,
  ) {
    this.#memory = new Charged(charge);
    this.#fractions = orderedSetArguments(rel, context);
    this.#groups = rel.groupBy.length
      ? new GroupTable(
          rel.groupBy.map((i) => {
            const type = context.input[i];
            if (!type) throw new TypeError("Missing percentile group type");
            return type;
          }),
          charge,
        )
      : null;
    try {
      this.#ids = this.#memory.make(Int32Array, bound.rows);
      this.#batchIds = this.#memory.make(Int32Array, BATCH_ROWS);
      this.#scratch = this.#memory.make(Float64Array, bound.rows);
      this.#counts = this.#memory.make(Int32Array, bound.groups + 1);
      this.#offsets = this.#memory.make(Int32Array, bound.groups + 1);
      this.#heads = this.#memory.make(Int32Array, bound.groups + 1);
      for (let remaining = rel.aggregates.length; remaining > 0; remaining--) {
        this.#values.push(this.#memory.make(Float64Array, bound.rows));
        this.#valid.push(this.#memory.make(Uint8Array, bound.rows));
      }
      this.#output = rel.aggregates.map((call) => context.arena.flat(call.type));
    } catch (error) {
      this.#close();
      throw error;
    }
  }
  #check(): void {
    if (this.#aborted) throw this.#reason;
    this.signal.throwIfAborted();
  }
  push(batch: Batch): PushResult {
    this.#check();
    const count = countIn(batch.selection, batch.length),
      rows = rowsIn(batch.selection);
    if (this.#rows + count > this.bound.rows)
      throw new Error("Percentile row certificate exceeded");
    this.#keys.length = 0;
    for (const column of this.rel.groupBy) {
      const v = batch.columns[column];
      if (!v) throw new TypeError("Missing group key");
      this.#keys.push(v);
    }
    this.#groups?.assign(this.#keys, batch.length, batch.selection, this.#batchIds);
    this.#keys.length = 0;
    if ((this.#groups?.groupCount ?? 1) > this.bound.groups)
      throw new Error("Percentile group certificate exceeded");
    if (this.#groups && batch.selection === null)
      this.#ids.set(this.#batchIds.subarray(0, count), this.#rows);
    else if (this.#groups)
      for (let k = 0; k < count; k++) this.#ids[this.#rows + k] = this.#batchIds[rows[k] ?? 0] ?? 0;
    for (let c = 0; c < this.rel.aggregates.length; c++) {
      const order = this.rel.aggregates[c]?.orderBy[0]?.expr;
      if (order?.op !== "column") throw new TypeError("Missing ordered percentile input");
      const v = batch.columns[order.index],
        values = this.#values[c],
        valid = this.#valid[c];
      if (!v || !values || !valid) throw new TypeError("Missing percentile input");
      const source =
        v.form === "flat" && (v.data.layout === "i32" || v.data.layout === "f64")
          ? v.data.values
          : null;
      if (source && v.form === "flat" && v.validity === null && batch.selection === null) {
        values.set(source.subarray(0, count), this.#rows);
        valid.fill(1, this.#rows, this.#rows + count);
        continue;
      }
      for (let k = 0; k < count; k++) {
        const row = rows[k] ?? 0;
        if (!isValidSlot(v, row)) continue;
        if (source) values[this.#rows + k] = source[row] ?? NaN;
        else {
          const [flat, at] = slotOf(v, row),
            data = flat.data;
          if (data.layout !== "i32" && data.layout !== "f64")
            throw new TypeError("Invalid percentile layout");
          values[this.#rows + k] = data.values[at] ?? NaN;
        }
        valid[this.#rows + k] = 1;
      }
    }
    this.#rows += count;
    return "ready";
  }
  async ready(): Promise<void> {
    this.#check();
    if (this.#pending) await this.#pending;
  }
  finish(): Promise<void> {
    this.#check();
    if (this.#pending) throw new TypeError("Percentile finish already started");
    // Install the owned cleanup promise before executing a job that can call downstream
    // and cancel reentrantly. Every suspension retains state/output charges until settlement.
    this.#pending = Promise.resolve()
      .then(() => {
        this.#check();
        return runSliced(this.#emit(), this.#slice, () => this.#check());
      })
      .finally(() => this.#close());
    return this.#pending;
  }
  *#emit(): SlicedJob<void> {
    const groups = this.#groups?.groupCount ?? 1;
    for (let c = 0; c < this.rel.aggregates.length; c++) {
      const call = this.rel.aggregates[c],
        compiled = this.#fractions[c],
        output = this.#output[c],
        values = this.#values[c],
        valid = this.#valid[c];
      if (!call || !compiled || !output || !values || !valid)
        throw new TypeError("Missing percentile state");
      const descending = call.orderBy[0]?.direction === "desc";
      this.#counts.fill(0);
      this.#offsets.fill(0);
      for (let row = 0; row < this.#rows; row++) {
        if (valid[row]) {
          const id = this.#ids[row] ?? 0;
          this.#counts[id] = (this.#counts[id] ?? 0) + 1;
        }
        if ((row & 8191) === 0 && this.#slice.due()) yield;
      }
      for (let g = 0; g < groups; g++)
        this.#offsets[g + 1] = (this.#offsets[g] ?? 0) + (this.#counts[g] ?? 0);
      this.#heads.set(this.#offsets);
      for (let row = 0; row < this.#rows; row++) {
        if (valid[row]) {
          const id = this.#ids[row] ?? 0,
            head = this.#heads[id] ?? 0;
          this.#scratch[head] = values[row] ?? NaN;
          this.#heads[id] = head + 1;
        }
        if ((row & 8191) === 0 && this.#slice.due()) yield;
      }
      output.vector.length = groups;
      output.validity.fill(0);
      output.vector.validity = output.validity;
      for (let g = 0; g < groups; g++) {
        this.#check();
        const fraction = compiled.evaluate(EMPTY, null);
        if (!isValidSlot(fraction, 0)) continue;
        const [f, at] = slotOf(fraction, 0);
        if (f.data.layout !== "f64") throw new TypeError("Percentile fraction layout");
        const p = f.data.values[at] ?? NaN;
        if (!(p >= 0 && p <= 1))
          throw new ExecutionError(
            "percentile value is not between 0 and 1",
            "MINNOW_PERCENTILE_RANGE",
            "22003",
          );
        const count = this.#counts[g] ?? 0,
          from = this.#offsets[g] ?? 0;
        if (count === 0) continue;
        const position = p * (count - 1),
          low =
            call.fn === "percentile_disc"
              ? Math.max(Math.ceil(p * count), 1) - 1
              : Math.floor(position),
          high = call.fn === "percentile_disc" ? low : Math.ceil(position);
        const lower = yield* selectNumber(
          this.#scratch,
          from,
          from + count,
          from + low,
          descending,
          this.#slice,
        );
        const upper =
          low === high
            ? lower
            : yield* selectNumber(
                this.#scratch,
                from + low + 1,
                from + count,
                from + high,
                descending,
                this.#slice,
              );
        const value =
          low === high ? lower : lower + (position - Math.floor(position)) * (upper - lower);
        const data = output.vector.data;
        if (data.layout !== "i32" && data.layout !== "f64")
          throw new TypeError("Percentile result layout");
        data.values[g] = value;
        output.validity[g >>> 5] = (output.validity[g >>> 5] ?? 0) | (1 << (g & 31));
        if (this.#slice.due()) yield;
      }
    }
    this.#check();
    if (groups) {
      let bytes = 0;
      for (let c = 0; c < this.rel.groupBy.length; c++)
        bytes += this.#groups?.keyBytes(c, 0, groups) ?? 0;
      if (!this.charge.allocate(bytes)) throw new Error("Percentile certified output not admitted");
      this.#outputHeld = bytes;
      const columns: FlatVector[] = [];
      for (let c = 0; c < this.rel.groupBy.length; c++) {
        const v = this.#groups?.emitKey(c, 0, groups);
        if (v) columns.push(v);
      }
      for (const out of this.#output) columns.push(out.vector);
      if (this.downstream.push({ length: groups, columns, selection: null }) === "blocked")
        yield this.downstream.ready();
      this.#check();
      this.charge.free(this.#outputHeld);
      this.#outputHeld = 0;
    }
    const finished = this.downstream.finish();
    if (finished) yield Promise.resolve(finished);
  }
  abort(reason: unknown): void {
    if (this.#aborted) return;
    this.#aborted = true;
    this.#reason = reason;
    try {
      this.downstream.abort(reason);
    } finally {
      if (!this.#pending) this.#close();
    }
  }
  get closed(): Promise<void> {
    return this.#pending ?? Promise.resolve();
  }
  #close(): void {
    if (this.#closed) return;
    this.#closed = true;
    this.#keys.length = 0;
    this.#groups?.close();
    this.#memory.close();
    if (this.#outputHeld) this.charge.free(this.#outputHeld);
    this.#outputHeld = 0;
  }
}
