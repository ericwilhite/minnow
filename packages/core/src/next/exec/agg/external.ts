/** Bounded external aggregation: sort group keys plus a total input ordinal,
 * then finish one group's transitions. No transition states are merged.
 */
import type { Rel } from "../../ir/ir.js";
import type { Grant } from "../../memory/governor.js";
import type { PhysicalType } from "../../types.js";
import { BATCH_ROWS, type Batch, type FlatVector, type Vector } from "../../vector/vector.js";
import { ColumnStore } from "../column-store.js";
import { UnsupportedPlanError } from "../errors.js";
import type { CompileContext } from "../kernels/compile.js";
import { rowsIn, countIn } from "../kernels/vectors.js";
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
import { ExternalCall, externalCallMinimum, externalCallDistinct } from "./external-call.js";
import { SortGrants } from "./sort-grants.js";
import { supportsGroupTopN, type GroupTopNSpec } from "./group-top-n.js";

type AggregateRel = Extract<Rel, { op: "aggregate" }>;
const ORDINAL: PhysicalType = { kind: "float64" };
const OWN_MINIMUM = 128 * 1024;
const at = <T>(xs: readonly T[], i: number): T => {
  const value = xs[i];
  if (value === undefined) throw new TypeError("Missing external aggregate column");
  return value;
};
const sortedDistinct = externalCallDistinct;
export function externalAggregateMinimum(
  rel: AggregateRel,
  input: readonly PhysicalType[],
  top?: GroupTopNSpec,
): number {
  return (
    OWN_MINIMUM +
    (top && supportsGroupTopN(rel, input, top)
      ? sortMinimumBytes([
          ...rel.groupBy.map((i) => at(input, i)),
          ...rel.aggregates.map((a) => a.type),
        ])
      : 0) +
    sortMinimumBytes([...input, ORDINAL]) +
    rel.aggregates.reduce((n, call) => {
      const arg = call.args[0];
      return (
        n +
        externalCallMinimum(call) +
        (sortedDistinct(call) && arg ? sortMinimumBytes([arg.type, ORDINAL]) : 0)
      );
    }, 0)
  );
}
export function externalAggregateLoans(
  rel: AggregateRel,
  input?: readonly PhysicalType[],
  top?: GroupTopNSpec,
): number {
  return (
    2 +
    (top && input && supportsGroupTopN(rel, input, top) ? 1 : 0) +
    rel.aggregates.reduce((n, call) => n + 1 + Number(sortedDistinct(call)), 0)
  );
}

export class ExternalAggregateOperator implements Operator {
  readonly grants: SortGrants;
  readonly #ownGrant: Grant;
  readonly #pool: Pool;
  readonly #keys: ColumnStore;
  readonly #keyTypes: readonly PhysicalType[];
  readonly #keyOrder: readonly SortColumn[];
  readonly #sort: SortOperator;
  readonly #top: SortOperator | null;
  readonly #output: Operator;
  readonly #calls: ExternalCall[] = [];
  readonly #ordinal: FlatVector & { data: { layout: "f64"; values: Float64Array } };
  readonly #slice = new Slice(YIELD_INTERVAL_MS);
  readonly #controller = new AbortController();
  readonly #parentAbort: () => void;
  #nextOrdinal = 0;
  #started = false;
  #aborted = false;
  #reason: unknown;
  #pending: Promise<void> | null = null;
  #cleanup: Promise<void> | null = null;
  constructor(
    readonly rel: AggregateRel,
    readonly context: CompileContext,
    readonly shared: SortContext & { readonly loans: readonly Grant[] },
    readonly downstream: Operator,
    top?: GroupTopNSpec,
  ) {
    if (rel.groupingSets !== null && rel.groupingSets.length > 1)
      throw new UnsupportedPlanError("external aggregate grouping sets", {
        construct: "aggregate",
      });
    this.grants = new SortGrants(shared.grant, shared.loans);
    this.#ownGrant = this.grants.fork(OWN_MINIMUM, 0);
    this.#pool = new Pool(new Charge(this.#ownGrant), OWN_MINIMUM);
    this.#keyTypes = rel.groupBy.map((i) => at(context.input, i));
    this.#keyOrder = this.#keyTypes.map((_, column) => ({
      column,
      direction: "asc",
      nulls: "last",
      collation: null,
    }));
    this.#keys = new ColumnStore(this.#keyTypes, this.#pool, {
      chunkRows: 1,
      targetBytes: Infinity,
    });
    if (!this.#pool.allocate(8 * BATCH_ROWS))
      throw new Error("External ordinal buffer was not admitted");
    this.#ordinal = {
      form: "flat",
      type: ORDINAL,
      length: 0,
      validity: null,
      data: { layout: "f64", values: new Float64Array(BATCH_ROWS) },
    };
    let slot = 2;
    for (const call of rel.aggregates) {
      this.#calls.push(new ExternalCall(call, this, slot));
      slot += 1 + Number(sortedDistinct(call));
    }
    const outputTypes = [...this.#keyTypes, ...rel.aggregates.map((a) => a.type)];
    this.#top =
      top && supportsGroupTopN(rel, context.input, top)
        ? new SortOperator(
            { types: outputTypes, keys: top.keys, limit: top.count, offset: 0, withTies: false },
            this.sortContext(this.grants.fork(sortMinimumBytes(outputTypes), slot)),
            downstream,
          )
        : null;
    this.#output = this.#top ?? downstream;
    const sink: Operator = {
      push: (batch) => {
        this.#pending = this.#groups(batch);
        return "blocked";
      },
      ready: () => this.#pending ?? Promise.resolve(),
      finish: async () => {
        if (this.#started || this.rel.groupBy.length === 0) await this.#emit(false);
        await this.#output.finish();
      },
      abort: (reason) => this.abort(reason),
    };
    const input = [...context.input, ORDINAL];
    this.#sort = new SortOperator(
      {
        types: input,
        keys: [...rel.groupBy, context.input.length].map((column) => ({
          column,
          direction: "asc",
          nulls: "last",
          collation: null,
        })),
        limit: null,
        offset: 0,
        withTies: false,
      },
      this.sortContext(this.grants.fork(sortMinimumBytes(input), 1)),
      sink,
    );
    this.#parentAbort = () => this.abort(shared.signal?.reason);
    shared.signal?.addEventListener("abort", this.#parentAbort, { once: true });
    if (shared.signal?.aborted) this.#parentAbort();
  }
  check(): void {
    if (this.#aborted) throw this.#reason;
    this.shared.signal?.throwIfAborted();
  }
  async pace(): Promise<void> {
    this.check();
    if (this.#slice.due()) {
      await yieldTask();
      afterYield(this.#slice.clock);
      this.check();
    }
  }
  sortContext(grant: Grant): SortContext {
    return { ...this.shared, grant, signal: this.#controller.signal };
  }
  push(batch: Batch): PushResult {
    this.check();
    this.#ordinal.length = batch.length;
    const rows = rowsIn(batch.selection),
      count = countIn(batch.selection, batch.length);
    for (let i = 0; i < count; i++) this.#ordinal.data.values[rows[i] ?? 0] = this.#nextOrdinal++;
    if (!Number.isSafeInteger(this.#nextOrdinal))
      throw new RangeError("External aggregate input ordinal overflow");
    return this.#sort.push({ ...batch, columns: [...batch.columns, this.#ordinal] });
  }
  ready(): Promise<void> {
    return this.#sort.ready();
  }
  async #capture(keys: readonly Vector[], row: number): Promise<void> {
    this.#keys.reset(true);
    this.#pool.settle();
    const needed = this.#keys.appendNeed(keys, row);
    if (!this.#pool.allocate(needed))
      await this.#pool.need(needed, this.#ownGrant, this.#controller.signal);
    else this.#pool.free(needed);
    this.check();
    if (this.#pool.withStep(() => this.#keys.append(keys, null, row, 1)) !== 1)
      throw new Error("External group key was not admitted");
  }
  async #groups(batch: Batch): Promise<void> {
    const keys = this.rel.groupBy.map((i) => at(batch.columns, i)),
      ordinal = at(batch.columns, this.context.input.length);
    let from = 0;
    while (from < batch.length) {
      await this.pace();
      if (
        this.#started &&
        !keysEqual(this.#keyTypes, this.#keyOrder, this.#keys.vectors(), 0, keys, from)
      )
        await this.#emit(true);
      if (!this.#started) {
        if (keys.length) await this.#capture(keys, from);
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
      for (const call of this.#calls) await call.add(batch, from, end - from, ordinal);
      from = end;
    }
  }
  async #emit(restart: boolean): Promise<void> {
    this.check();
    const columns: Vector[] = this.rel.groupBy.length ? this.#keys.vectors() : [];
    for (const call of this.#calls) columns.push(await call.emit());
    if (this.#output.push({ length: 1, columns, selection: null }) === "blocked")
      await this.#output.ready();
    this.check();
    if (restart) for (const call of this.#calls) call.restart();
    this.#started = false;
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
    this.#controller.abort(reason);
    this.grants.holdFrees = true;
    this.#sort.abort(reason);
    for (const call of this.#calls) call.abort(reason);
    if (this.#top) this.#top.abort(reason);
    else this.downstream.abort(reason);
    void this.#close().catch(() => undefined);
  }
  #close(): Promise<void> {
    return (this.#cleanup ??= (async () => {
      const outer = await Promise.allSettled([this.#sort.closed]);
      await this.#pending?.catch(() => undefined);
      const all = [
        ...outer,
        ...(await Promise.allSettled(this.#top ? [this.#top.closed] : [])),
        ...(await Promise.allSettled(this.#calls.map((call) => call.close()))),
      ];
      this.#calls.length = 0;
      this.#keys.close();
      this.#pool.free(8 * BATCH_ROWS);
      this.#pool.close();
      this.grants.close();
      this.shared.signal?.removeEventListener("abort", this.#parentAbort);
      const errors = all
        .filter((r): r is PromiseRejectedResult => r.status === "rejected")
        .map((r) => r.reason as unknown);
      if (errors.length) throw new AggregateError(errors, "External aggregate cleanup failed");
    })());
  }
  get closed(): Promise<void> {
    return this.#cleanup ?? Promise.resolve();
  }
}
