/** One external group's transition state. DISTINCT follows PostgreSQL's ordered
 * value transitions, including floating SUM/AVG; the input ordinal breaks equal-value ties.
 */
import type { AggregateCall } from "../../ir/ir.js";
import type { Grant } from "../../memory/governor.js";
import type { PhysicalType } from "../../types.js";
import type { ByteCharge } from "../../vector/varlen.js";
import { BATCH_ROWS, type Batch, type Selection, type Vector } from "../../vector/vector.js";
import { ColumnStore } from "../column-store.js";
import { Predicate } from "../filter.js";
import { compileExpr, type CompileContext } from "../kernels/compile.js";
import { isValidSlot, slotOf, type Compiled } from "../kernels/vectors.js";
import type { Operator } from "../pipeline.js";
import { keysEqual, type SortColumn } from "../sort-keys.js";
import { SortOperator, sortMinimumBytes, type SortContext } from "../sort.js";
import { Charge, Pool } from "../spill-pool.js";
import { SortGrants } from "./sort-grants.js";
import { accumulatorFor, type Accumulator } from "./states.js";

const ORDINAL: PhysicalType = { kind: "float64" };
const ZERO_GROUPS = new Int32Array(BATCH_ROWS); // Immutable: every row of this group updates slot 0.
const asc = (column: number): SortColumn => ({
  column,
  direction: "asc",
  nulls: "last",
  collation: null,
});
const VALUE_ORDER = [asc(0)];
const numericCall = (call: AggregateCall) =>
  call.type.kind === "decimal" || call.args[0]?.type.kind === "decimal";
const NUMERIC_SCRATCH = 384 * 1024;
export const externalCallMinimum = (call: AggregateCall): number =>
  numericCall(call) ? 640 * 1024 : 128 * 1024;
export function externalCallDistinct(call: AggregateCall): boolean {
  if (!call.distinct || call.args.length === 0) return false;
  if (call.fn === "bool_and" || call.fn === "bool_or") return false;
  // Byte-equal extrema have the same display. Float signed zero and decimal scales
  // have distinct representations: retain DISTINCT's first representative there.
  if (call.fn === "min" || call.fn === "max")
    return !["text", "bytes", "uuid", "enum", "bool"].includes(call.args[0]?.type.kind ?? "");
  return true;
}
/** Compiler buffers only: do not reserve unused hash tables in the external recipe. */
export function externalCallArguments(
  call: AggregateCall,
  context: CompileContext,
): {
  argument: Compiled | null;
  filter: Predicate | null;
} {
  const arg = call.args[0];
  return {
    argument: call.fn === "count_star" || arg === undefined ? null : compileExpr(arg, context),
    filter: call.filter === null ? null : new Predicate(call.filter, context),
  };
}

export interface ExternalCallHost {
  readonly grants: SortGrants;
  readonly context: CompileContext;
  check(): void;
  pace(): Promise<void>;
  sortContext(grant: Grant): SortContext;
}

/** All retained state and its next resize/emit overlap live in this call's own admitted lane.
 * The pool never waits on another execution's pinned minimum. The host gives every call and
 * each nested sort an independent progress owner as part of one atomic execution admission.
 */
export class ExternalCall {
  readonly #argument: Compiled | null;
  readonly #filter: Predicate | null;
  readonly #rows: Uint16Array;
  readonly #selection: Selection;
  readonly #grant: Grant;
  readonly #capacity: number;
  readonly #scratch: number;
  readonly #pool: Pool;
  readonly #charge: ByteCharge;
  readonly #previous: ColumnStore | null;
  readonly #valueType: PhysicalType | null;
  readonly #types: readonly PhysicalType[];
  readonly #valueGrant: Grant | null;
  #values: SortOperator | null = null;
  #accumulator: Accumulator;
  #used = 0;
  #largest = 0;
  #emitted = 0;
  #output: Vector | null = null;
  #pending: Promise<void> | null = null;
  #cleanup: Promise<void> | null = null;
  readonly #sorts = new Set<SortOperator>();

  constructor(
    readonly call: AggregateCall,
    readonly host: ExternalCallHost,
    slot: number,
  ) {
    const compiled = externalCallArguments(call, host.context);
    this.#argument = compiled.argument;
    this.#filter = compiled.filter;
    this.#valueType = this.#argument?.type ?? null;
    const minimum = externalCallMinimum(call);
    this.#grant = host.grants.fork(minimum, slot);
    this.#capacity = host.grants.stepCapacity(this.#grant);
    this.#pool = new Pool(new Charge(this.#grant), minimum);
    this.#charge = {
      allocate: (bytes) => {
        if (!this.#pool.allocate(bytes)) return false;
        this.#used += bytes;
        return true;
      },
      free: (bytes) => {
        this.#used -= bytes;
        this.#pool.free(bytes);
      },
    };
    // At most 131072 integer +16383 fractional decimal digits, plus the safe
    // ordinal's <=53 count bits: a coefficient is <62KiB. Precharge alignment/
    // division temporaries and the <=295KiB UTF-16 range-check string BEFORE any
    // BigInt computation. The remaining 256KiB covers state, prepared/output
    // coefficients and metadata, including conservative ownership overlap.
    this.#scratch = numericCall(call) ? NUMERIC_SCRATCH : 0;
    if (!this.#charge.allocate(this.#scratch))
      throw new Error("Numeric aggregate scratch was not admitted");
    if (!this.#charge.allocate(2 * BATCH_ROWS))
      throw new Error("External selection was not admitted");
    this.#rows = new Uint16Array(BATCH_ROWS);
    this.#selection = { rows: this.#rows, count: 0 };
    this.#accumulator = this.#newAccumulator();
    this.#types = this.#valueType === null ? [] : [this.#valueType, ORDINAL];
    this.#previous =
      externalCallDistinct(call) && this.#valueType !== null
        ? new ColumnStore([this.#valueType], this.#charge, { chunkRows: 1, targetBytes: Infinity })
        : null;
    this.#valueGrant =
      this.#previous === null ? null : host.grants.fork(sortMinimumBytes(this.#types), slot + 1);
    this.#restartSorts();
  }
  #newAccumulator(): Accumulator {
    const a = accumulatorFor(this.call.fn, this.call.type, this.#valueType, this.#charge);
    a.grow(1);
    return a;
  }
  #makeSort(grant: Grant, keys: readonly SortColumn[], sink: Operator): SortOperator {
    const sort = new SortOperator(
      { types: this.#types, keys, limit: null, offset: 0, withTies: false },
      this.host.sortContext(grant),
      sink,
    );
    this.#sorts.add(sort);
    return sort;
  }
  #restartSorts(): void {
    if (this.#valueGrant === null) return;
    const valuesSink: Operator = {
      push: (batch) => {
        this.#pending = this.#deduplicate(batch);
        return "blocked";
      },
      ready: () => this.#pending ?? Promise.resolve(),
      finish: () => undefined,
      abort: () => undefined,
    };
    this.#values = this.#makeSort(this.#valueGrant, [asc(0), asc(1)], valuesSink);
  }
  async #room(peak: number): Promise<void> {
    this.host.check();
    // Retained extrema can grow again before they are emitted. Take their whole
    // already-admitted lane once, rather than ask for a second loan while pinning
    // the first. Never turn optional later headroom into a larger capacity target.
    const target = peak > externalCallMinimum(this.call) ? Math.max(peak, this.#capacity) : peak;
    await this.#pool.need(
      Math.max(0, target - this.#used),
      this.#grant,
      this.host.sortContext(this.#grant).signal,
      this.#capacity > externalCallMinimum(this.call),
    );
    this.host.check();
  }
  #width(vector: Vector | null, rows: Uint16Array, count: number): number {
    let largest = 0;
    if (vector === null) return 0;
    for (let k = 0; k < count; k++) {
      const row = rows[k] ?? 0;
      if (!isValidSlot(vector, row)) continue;
      largest = Math.max(largest, this.#widthAt(vector, row));
    }
    return largest;
  }
  #widthAt(vector: Vector, row: number): number {
    const [flat, slot] = slotOf(vector, row);
    return flat.data.layout === "varlen"
      ? (flat.data.offsets[slot + 1] ?? 0) - (flat.data.offsets[slot] ?? 0)
      : 0;
  }
  async add(batch: Batch, from: number, count: number, ordinal: Vector): Promise<void> {
    this.host.check();
    for (let i = 0; i < count; i++) this.#rows[i] = from + i;
    this.#selection.count = count;
    const selected =
      this.#filter?.select({ ...batch, selection: this.#selection }) ?? this.#selection;
    const value = this.#argument?.evaluate(batch, selected) ?? null;
    if (this.#values !== null) {
      let kept = 0;
      for (let k = 0; k < selected.count; k++) {
        const row = selected.rows[k] ?? 0;
        if (value !== null && isValidSlot(value, row)) this.#rows[kept++] = row;
      }
      this.#selection.count = kept;
      if (
        kept !== 0 &&
        value !== null &&
        this.#values.push({
          length: batch.length,
          columns: [value, ordinal],
          selection: this.#selection,
        }) === "blocked"
      )
        await this.#values.ready();
    } else {
      // FILTER rows can be sparse; preserve their exact positions and order.
      this.#largest = Math.max(this.#largest, this.#width(value, selected.rows, selected.count));
      await this.#room(Math.max(externalCallMinimum(this.call), 32768 + 4 * this.#largest));
      await this.host.pace();
      this.#pool.withStep(() =>
        this.#accumulator.update(value, ZERO_GROUPS, selected.rows, selected.count, true),
      );
    }
    this.host.check();
  }
  async #deduplicate(batch: Batch): Promise<void> {
    const previous = this.#previous,
      value = batch.columns[0];
    if (previous === null || value === undefined)
      throw new Error("Missing external DISTINCT state");
    let kept = 0;
    for (let row = 0; row < batch.length; row++) {
      await this.host.pace();
      if (
        previous.rows > 0 &&
        keysEqual([value.type], VALUE_ORDER, previous.vectors(), 0, [value], row)
      )
        continue;
      previous.reset(true);
      this.#pool.settle();
      const size = this.#widthAt(value, row);
      await this.#room(Math.max(externalCallMinimum(this.call), 32768 + 4 * size));
      if (this.#pool.withStep(() => previous.append([value], null, row, 1)) !== 1)
        throw new Error("External DISTINCT capture exceeded its admitted step");
      this.#rows[kept++] = row;
    }
    this.#pool.withStep(() => this.#accumulator.update(value, ZERO_GROUPS, this.#rows, kept, true));
  }

  async emit(): Promise<Vector> {
    this.host.check();
    if (this.#values !== null) {
      await this.#values.finish();
      await this.#values.closed;
      if (this.#valueGrant !== null) this.host.grants.reset(this.#valueGrant);
      this.#previous?.reset(true);
      this.#pool.settle();
      this.#sorts.clear();
      this.#values = null;
    }
    const bytes = this.#accumulator.outputBytes(0, 1);
    await this.#room(this.#used + bytes);
    if (!this.#charge.allocate(bytes))
      throw new Error("External aggregate output was not admitted");
    this.#emitted = bytes;
    this.#output = this.#pool.withStep(() => this.#accumulator.emit(0, 1));
    return this.#output;
  }
  restart(): void {
    this.#output = null;
    this.#charge.free(this.#emitted);
    this.#emitted = 0;
    this.#accumulator.close();
    this.#pool.settle();
    this.#accumulator = this.#newAccumulator();
    this.#largest = 0;
    this.#restartSorts();
  }
  abort(reason: unknown): void {
    for (const sort of this.#sorts) sort.abort(reason);
  }
  close(): Promise<void> {
    return (this.#cleanup ??= (async () => {
      const results = await Promise.allSettled([...this.#sorts].map((s) => s.closed));
      await this.#pending?.catch(() => undefined);
      this.#output = null;
      this.#charge.free(this.#emitted);
      this.#emitted = 0;
      this.#accumulator.close();
      this.#previous?.close();
      this.#sorts.clear();
      this.#values = null;
      this.#pending = null;
      this.#charge.free(2 * BATCH_ROWS);
      this.#charge.free(this.#scratch);
      this.#pool.close();
      const errors = results
        .filter((r): r is PromiseRejectedResult => r.status === "rejected")
        .map((r) => r.reason as unknown);
      if (errors.length) throw new AggregateError(errors, "External DISTINCT cleanup failed");
    })());
  }
}
