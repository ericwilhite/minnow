/** Ordered-set accumulation: shared external sorts, one group at a time, no unbounded group arrays. */
import type { AggregateCall, Rel } from "../../ir/ir.js";
import type { Grant } from "../../memory/governor.js";
import type { PhysicalType } from "../../types.js";
import { BATCH_ROWS, type Batch, type FlatVector, type Vector } from "../../vector/vector.js";
import { SortGrants } from "./sort-grants.js";
import { ColumnStore, nullConstant } from "../column-store.js";
import { ExecutionError, UnsupportedPlanError } from "../errors.js";
import { compileExpr, type CompileContext } from "../kernels/compile.js";
import { isValidSlot, slotOf, type Compiled } from "../kernels/vectors.js";
import {
  afterYield,
  yieldTask,
  YIELD_INTERVAL_MS,
  type Operator,
  type PushResult,
} from "../pipeline.js";
import { Slice } from "../slices.js";
import { keysEqual, type SortColumn } from "../sort-keys.js";
import { SortOperator, sortMinimumBytes, type SortContext } from "../sort.js";
import { Charge, Pool } from "../spill-pool.js";

function present<T>(value: T | undefined): T {
  if (value === undefined) throw new TypeError("Missing ordered-set plan field");
  return value;
}

type AggregateRel = Extract<Rel, { op: "aggregate" }>;
export const isOrderedSet = (call: AggregateCall): boolean =>
  call.fn === "percentile_cont" || call.fn === "percentile_disc" || call.fn === "mode";
const NUMERIC = new Set(["int32", "int64", "float32", "float64", "decimal"]);
const EMPTY: Batch = { length: 1, columns: [], selection: null };
const POOL_BASE = 1024 * 1024;

/** Validate before constructing any breaker, also used during recipe preparation. */
export function orderedSetArguments(
  rel: AggregateRel,
  context: CompileContext,
): Array<Compiled | null> {
  if (rel.groupingSets !== null)
    throw new UnsupportedPlanError("ordered-set grouping sets", { construct: "aggregate" });
  return rel.aggregates.map((call) => {
    const order = call.orderBy[0];
    if (
      !isOrderedSet(call) ||
      call.distinct ||
      call.filter !== null ||
      call.orderBy.length !== 1 ||
      order?.expr.op !== "column"
    )
      throw new UnsupportedPlanError("ordered-set aggregate shape", { construct: "aggregate" });
    if (!NUMERIC.has(order.expr.type.kind))
      throw new UnsupportedPlanError("non-numeric ordered-set input", { construct: "aggregate" });
    if (
      call.fn === "percentile_cont" &&
      (call.type.kind !== "float64" || order.expr.type.kind !== "float64")
    )
      throw new UnsupportedPlanError("interval interpolation", { construct: "aggregate" });
    if (call.fn === "mode") {
      if (call.args.length !== 0) throw new TypeError("mode has no direct arguments");
      return null;
    }
    const fraction = call.args[0];
    if (call.args.length !== 1 || fraction?.type.kind !== "float64")
      throw new TypeError("percentile fraction must be float8");
    // Lowering leaves direct arguments outside its input projection. Explicit IR must do likewise.
    const check = (value: unknown): void => {
      if (value === null || typeof value !== "object") return;
      if ("op" in value && value.op === "column")
        throw new UnsupportedPlanError("column-valued percentile fraction", {
          construct: "aggregate",
        });
      for (const nested of Object.values(value)) check(nested);
    };
    check(fraction);
    return compileExpr(fraction, context);
  });
}

export function orderedSetMinimumBytes(rel: AggregateRel, types: readonly PhysicalType[]): number {
  return (
    sortMinimumBytes(types) +
    POOL_BASE +
    rel.aggregates.reduce(
      (n, call) => n + 128 * 1024 + sortMinimumBytes([present(call.orderBy[0]).expr.type]),
      0,
    )
  );
}

/** One group's values. SortOperator charges copies first, spills via its shared pool, and paces sorting. */
export class OrderedSetAccumulator implements Operator {
  readonly result: ColumnStore;
  readonly previous: ColumnStore;
  readonly #rows: Uint16Array;
  readonly #sortKey: readonly SortColumn[];
  #sort: SortOperator;
  readonly #grant: Grant;
  #pending: Promise<void> | null = null;
  #count = 0;
  #position = 0;
  #low = 0;
  #high = 0;
  #weight = 0;
  #lower = 0;
  #fraction: number | null = null;
  #run = 0;
  #best = 0;
  constructor(
    readonly call: AggregateCall,
    readonly valueType: PhysicalType,
    readonly fraction: Compiled | null,
    readonly host: OrderedSetOperator,
    slot: number,
  ) {
    this.result = new ColumnStore([call.type], host.pool, { chunkRows: 1, targetBytes: Infinity });
    this.previous = new ColumnStore([valueType], host.pool, {
      chunkRows: 1,
      targetBytes: Infinity,
    });
    if (!host.pool.allocate(2 * BATCH_ROWS))
      throw new RangeError("Ordered-set row selection not admitted");
    this.#rows = new Uint16Array(BATCH_ROWS);
    const order = present(call.orderBy[0]);
    this.#sortKey = [
      { column: 0, direction: order.direction, nulls: order.nulls, collation: order.collation },
    ];
    this.#grant = host.sortGrant(sortMinimumBytes([valueType]), slot);
    this.#sort = this.#newSort();
  }
  #newSort(): SortOperator {
    return new SortOperator(
      { types: [this.valueType], keys: this.#sortKey, limit: null, offset: 0, withTies: false },
      this.host.sortContext(this.#grant),
      this,
    );
  }
  async add(batch: Batch, from: number, count: number): Promise<void> {
    const index = present(this.call.orderBy[0]).expr;
    if (index.op !== "column") throw new TypeError("Missing ordered column");
    const vector = batch.columns[index.index];
    if (!vector) throw new TypeError("Missing ordered input");
    let selected = 0;
    for (let row = from; row < from + count; row++)
      if (isValidSlot(vector, row)) this.#rows[selected++] = row;
    this.#count += selected;
    if (
      selected &&
      this.#sort.push({
        length: batch.length,
        columns: [vector],
        selection: { rows: this.#rows, count: selected },
      }) === "blocked"
    )
      await this.#sort.ready();
  }
  async emit(): Promise<Vector> {
    this.host.check();
    if (this.fraction !== null) {
      const value = this.fraction.evaluate(EMPTY, null);
      if (isValidSlot(value, 0)) {
        const [flat, slot] = slotOf(value, 0);
        if (flat.data.layout !== "f64") throw new TypeError("Fraction is not float8");
        const f = flat.data.values[slot] ?? NaN;
        if (!(f >= 0 && f <= 1))
          throw new ExecutionError(
            "percentile value is not between 0 and 1",
            "MINNOW_PERCENTILE_RANGE",
            "22003",
          );
        this.#fraction = f;
      }
      const position = (this.#fraction ?? 0) * (this.#count - 1);
      this.#low =
        this.call.fn === "percentile_disc"
          ? Math.max(Math.ceil((this.#fraction ?? 0) * this.#count), 1) - 1
          : Math.floor(position);
      this.#high = Math.ceil(position);
      this.#weight = position - Math.floor(position);
    }
    await this.#sort.finish();
    await this.#sort.closed;
    this.host.resetGrant(this.#grant);
    return this.result.rows === 0 ? nullConstant(this.call.type, 1) : this.result.chunk(0, 0);
  }
  push(batch: Batch): PushResult {
    this.#pending = this.#consume(batch);
    return "blocked";
  }
  ready(): Promise<void> {
    const pending = this.#pending ?? Promise.resolve();
    this.#pending = null;
    return pending;
  }
  async #consume(batch: Batch): Promise<void> {
    const vector = batch.columns[0];
    if (!vector) throw new TypeError("Missing sorted values");
    for (let row = 0; row < batch.length; row++, this.#position++) {
      await this.host.pace();
      if (this.call.fn === "mode") {
        const equal =
          this.previous.rows > 0 &&
          keysEqual(
            [this.valueType],
            [{ ...present(this.#sortKey[0]), column: 0 }],
            this.previous.vectors(),
            0,
            [vector],
            row,
          );
        this.#run = equal ? this.#run + 1 : 1;
        if (!equal) await this.host.capture(this.previous, [vector], row);
        if (this.#run > this.#best) {
          this.#best = this.#run;
          await this.host.capture(this.result, [vector], row);
        }
      } else if (this.#fraction !== null) {
        if (this.call.fn === "percentile_disc") {
          if (this.#position === this.#low) await this.host.capture(this.result, [vector], row);
        } else {
          const [flat, slot] = slotOf(vector, row);
          if (flat.data.layout !== "f64")
            throw new TypeError("Interpolation requires float8 input");
          const value = flat.data.values[slot] ?? NaN;
          if (this.#position === this.#low) this.#lower = value;
          if (this.#position === this.#high) {
            const interpolated =
              this.#low === this.#high ? value : this.#lower + (value - this.#lower) * this.#weight;
            const v = this.host.numericResult;
            v.data.values[0] = interpolated;
            await this.host.capture(this.result, [v], 0);
          }
        }
      }
    }
  }
  finish(): void {
    return undefined;
  }
  abort(reason: unknown): void {
    this.#sort.abort(reason);
  }
  get closed(): Promise<void> {
    return this.#sort.closed;
  }
  /** Keep next group's fixed sort buffers resident before upstream/downstream can reclaim them. */
  restart(): void {
    this.#count = 0;
    this.#position = 0;
    this.#run = 0;
    this.#best = 0;
    this.#fraction = null;
    this.#sort = this.#newSort();
  }
  resetResult(): void {
    this.host.pool.settle();
    this.result.reset(true);
    this.previous.reset(true);
  }
  close(): void {
    this.result.close();
    this.previous.close();
    this.host.pool.free(2 * BATCH_ROWS);
  }
}

/** Group sort followed by one bounded accumulator per ordered-set call. */
export class OrderedSetOperator implements Operator {
  readonly pool: Pool;
  readonly numericResult: FlatVector & { data: { layout: "f64"; values: Float64Array } };
  readonly #grants: SortGrants;
  readonly #ownGrant: Grant;
  readonly #sort: SortOperator;
  readonly #accumulators: OrderedSetAccumulator[] = [];
  readonly #keys: ColumnStore;
  readonly #keyTypes: readonly PhysicalType[];
  readonly #keyOrder: readonly SortColumn[];
  readonly #slice = new Slice(YIELD_INTERVAL_MS);
  #pending: Promise<void> | null = null;
  #started = false;
  #aborted = false;
  #reason: unknown;
  #cleanup: Promise<void> | null = null;

  constructor(
    readonly rel: AggregateRel,
    readonly context: CompileContext,
    readonly shared: SortContext & { readonly loans: readonly Grant[] },
    readonly downstream: Operator,
  ) {
    const fractions = orderedSetArguments(rel, context);
    this.#grants = new SortGrants(shared.grant, shared.loans);
    const ownMinimum = POOL_BASE + rel.aggregates.length * 128 * 1024;
    this.#ownGrant = this.#grants.fork(ownMinimum, 0);
    this.pool = new Pool(new Charge(this.#ownGrant), ownMinimum);
    this.#keyTypes = rel.groupBy.map((i) => present(context.input[i]));
    this.#keyOrder = this.#keyTypes.map((_, column) => ({
      column,
      direction: "asc",
      nulls: "last",
      collation: null,
    }));
    this.#keys = new ColumnStore(this.#keyTypes, this.pool, {
      chunkRows: 1,
      targetBytes: Infinity,
    });
    if (!this.pool.allocate(8)) throw new RangeError("Ordered-set output not admitted");
    this.numericResult = {
      form: "flat",
      type: { kind: "float64" },
      length: 1,
      validity: null,
      data: { layout: "f64", values: new Float64Array(1) },
    };
    for (let i = 0; i < rel.aggregates.length; i++) {
      const call = present(rel.aggregates[i]);
      this.#accumulators.push(
        new OrderedSetAccumulator(
          call,
          present(call.orderBy[0]).expr.type,
          fractions[i] ?? null,
          this,
          i + 2,
        ),
      );
    }
    const sink: Operator = {
      push: (batch) => {
        this.#pending = this.#groups(batch);
        return "blocked";
      },
      ready: () => this.#pending ?? Promise.resolve(),
      finish: () => this.#finishGroups(),
      abort: (reason) => this.abort(reason),
    };
    this.#sort = new SortOperator(
      {
        types: context.input,
        keys: rel.groupBy.map((column) => ({
          column,
          direction: "asc",
          nulls: "last",
          collation: null,
        })),
        limit: null,
        offset: 0,
        withTies: false,
      },
      this.sortContext(this.#grants.fork(sortMinimumBytes(context.input), 1)),
      sink,
    );
  }
  sortGrant(minimum: number, slot: number): Grant {
    return this.#grants.fork(minimum, slot);
  }
  resetGrant(grant: Grant): void {
    this.#grants.reset(grant);
  }
  sortContext(grant: Grant): SortContext {
    return { ...this.shared, grant };
  }
  check(): void {
    if (this.#aborted) throw this.#reason;
    if (this.shared.signal?.aborted) throw this.shared.signal.reason;
  }
  async pace(): Promise<void> {
    this.check();
    if (this.#slice.due()) {
      await yieldTask();
      afterYield(this.#slice.clock);
      this.check();
    }
  }
  async capture(store: ColumnStore, vectors: readonly Vector[], row: number): Promise<void> {
    this.check();
    store.reset(true);
    this.pool.settle();
    const needed = store.appendNeed(vectors, row);
    if (!this.pool.allocate(needed)) {
      await this.pool.need(needed, this.#ownGrant, this.shared.signal);
    } else this.pool.free(needed);
    this.check();
    if (store.append(vectors, null, row, 1) !== 1)
      throw new RangeError("Ordered-set capture failed after admission");
  }
  push(batch: Batch): PushResult {
    return this.#sort.push(batch);
  }
  ready(): Promise<void> {
    return this.#sort.ready();
  }
  async #groups(batch: Batch): Promise<void> {
    let from = 0;
    const keys = this.rel.groupBy.map((i) => present(batch.columns[i]));
    while (from < batch.length) {
      await this.pace();
      if (
        this.#started &&
        !keysEqual(this.#keyTypes, this.#keyOrder, this.#keys.vectors(), 0, keys, from)
      )
        await this.#emitGroup(true);
      if (!this.#started) {
        for (const a of this.#accumulators) a.resetResult();
        if (keys.length > 0) await this.capture(this.#keys, keys, from);
        this.#started = true;
      }
      let end = from + 1;
      while (
        end < batch.length &&
        keysEqual(this.#keyTypes, this.#keyOrder, keys, from, keys, end)
      ) {
        end++;
        if ((end & 7) === 0) await this.pace();
      }
      for (const accumulator of this.#accumulators) await accumulator.add(batch, from, end - from);
      from = end;
    }
  }
  async #emitGroup(restart: boolean): Promise<void> {
    this.check();
    const columns: Vector[] = this.rel.groupBy.length === 0 ? [] : this.#keys.vectors();
    for (const a of this.#accumulators) columns.push(await a.emit());
    if (restart) for (const a of this.#accumulators) a.restart();
    if (this.downstream.push({ length: 1, columns, selection: null }) === "blocked")
      await this.downstream.ready();
    this.check();
    this.#started = false;
  }
  async #finishGroups(): Promise<void> {
    if (this.#started || this.rel.groupBy.length === 0) await this.#emitGroup(false);
    await this.downstream.finish();
  }
  async finish(): Promise<void> {
    try {
      await this.#sort.finish();
    } catch (error) {
      this.abort(error);
      throw error;
    } finally {
      await this.#close();
    }
  }
  abort(reason: unknown): void {
    if (this.#aborted) return;
    this.#aborted = true;
    this.#reason = reason;
    this.#grants.holdFrees = true;
    this.#sort.abort(reason);
    for (const a of this.#accumulators) a.abort(reason);
    this.downstream.abort(reason);
    void this.#close().catch(() => undefined);
  }
  #close(): Promise<void> {
    return (this.#cleanup ??= (async () => {
      const results = await Promise.allSettled([
        this.#sort.closed,
        ...this.#accumulators.map((a) => a.closed),
      ]);
      await this.#pending?.catch(() => undefined);
      for (const a of this.#accumulators) a.close();
      this.#keys.close();
      this.pool.free(8);
      this.pool.close();
      this.#grants.close();
      const failures = results.filter((r): r is PromiseRejectedResult => r.status === "rejected");
      if (failures.length)
        throw new AggregateError(
          failures.map((r): unknown => r.reason),
          "Ordered-set spill cleanup failed",
        );
    })());
  }
  get closed(): Promise<void> {
    return this.#cleanup ?? Promise.resolve();
  }
}
