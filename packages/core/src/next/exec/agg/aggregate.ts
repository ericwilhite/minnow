/**
 * The `aggregate` operator (ir/ir.ts): hash aggregation by group id, struct-of-arrays state.
 *
 * - push: the group keys (input columns `groupBy`) map each active row to a group id (groups.ts);
 *   each aggregate evaluates its argument on its rows — after its FILTER, and through its DISTINCT
 *   set — and updates its accumulator (states.ts). Synchronous; nothing is retained from the
 *   batch (keys and values are copied into the state), so every push is `ready`.
 * - finish: emits the groups in first-seen order (key columns, then one column per aggregate),
 *   then finishes downstream. Without GROUP BY there is exactly one output row, even over no
 *   input (COUNT 0, other aggregates NULL); with GROUP BY, no input means no rows.
 *   - Each output batch is at most `BATCH_ROWS` groups and about `BATCH_TARGET_BYTES` (the
 *     columns' `outputBytes` bounds, halving the group count until it fits), and its bytes are
 *     charged to the grant before any of its vectors is built. A batch is borrowed: its charge is
 *     returned once downstream is done with it (`ready`, or after its `ready()` resolves).
 *   - Output runs in slices: after `YIELD_INTERVAL_MS` of emitting (AVG's per-group division
 *     included) it yields with a task (`yieldTask`), never a resolved Promise, so owner messages
 *     and cancellation get through; an `abort` meanwhile ends it.
 * - Not compiled yet (`UnsupportedPlanError`, construct `aggregate`): GROUPING SETS with more than
 *   one set, ordered aggregates (`orderBy`), and string_agg / array_agg / json_agg / percentiles /
 *   mode. These are refused before anything is allocated; any later failure while constructing
 *   releases everything already taken.
 * - Memory: group table and states grow from the grant; a refused growth fails the query until
 *   partitioned aggregation spill (milestone 3).
 */
import type { MaybePromise } from "../../access/key-index.js";
import type { AggregateCall, Rel } from "../../ir/ir.js";
import type { PhysicalType } from "../../types.js";
import type { ByteCharge } from "../../vector/varlen.js";
import {
  BATCH_ROWS,
  BATCH_TARGET_BYTES,
  type Batch,
  type FlatVector,
  type Selection,
  type Vector,
} from "../../vector/vector.js";
import { UnsupportedPlanError } from "../errors.js";
import { Predicate } from "../filter.js";
import { compileExpr, type CompileContext } from "../kernels/compile.js";
import { countIn, rowsIn, type Compiled } from "../kernels/vectors.js";
import { YIELD_INTERVAL_MS, yieldTask, type Operator, type PushResult } from "../pipeline.js";
import { DistinctSet } from "./distinct.js";
import { GroupTable } from "./groups.js";
import { accumulatorFor, type Accumulator } from "./states.js";

type AggregateRel = Extract<Rel, { op: "aggregate" }>;

const UNSUPPORTED = new Set([
  "string_agg",
  "array_agg",
  "json_agg",
  "percentile_cont",
  "percentile_disc",
  "mode",
]);

interface CompiledAggregate {
  readonly accumulator: Accumulator;
  readonly argument: Compiled | null;
  readonly filter: Predicate | null;
  readonly distinct: DistinctSet | null;
  readonly selection: Selection;
}

/** Output pacing (tests inject a clock). */
export interface AggregateOptions {
  /** Synchronous emitting between task yields (default `YIELD_INTERVAL_MS`). */
  readonly yieldIntervalMs?: number;
  /** The clock in ms (default `performance.now`). */
  readonly now?: () => number;
  /** The output batch byte target (default `BATCH_TARGET_BYTES`). */
  readonly targetBytes?: number;
}

/** Refuses what the operator cannot compile, before anything is allocated. */
function preflight(rel: AggregateRel): void {
  if (
    rel.groupingSets !== null &&
    !(rel.groupingSets.length === 1 && rel.groupingSets[0]?.length === rel.groupBy.length)
  )
    throw new UnsupportedPlanError("GROUPING SETS", { construct: "aggregate" });
  for (const call of rel.aggregates) {
    if (UNSUPPORTED.has(call.fn))
      throw new UnsupportedPlanError(`the aggregate ${call.fn}`, { construct: "aggregate" });
    if (call.orderBy.length > 0)
      throw new UnsupportedPlanError("ordered aggregates", { construct: "aggregate" });
    if (call.args.length > 1)
      throw new UnsupportedPlanError(`${call.fn} with ${String(call.args.length)} arguments`, {
        construct: "aggregate",
      });
  }
}

export class AggregateOperator implements Operator {
  readonly #keys: readonly number[];
  readonly #groups: GroupTable | null = null;
  readonly #aggregates: CompiledAggregate[] = [];
  readonly #gids: Int32Array = new Int32Array(0);
  readonly #keyVectors: Vector[];
  readonly #charge: ByteCharge;
  readonly #interval: number;
  readonly #now: () => number;
  readonly #target: number;
  /** Bytes the operator took from the arena (gids, selections), returned on a failed build. */
  #arenaBytes = 0;
  #emitted = 0;
  /** The charged bytes of the output batch downstream still holds. */
  #outputHeld = 0;
  #closed = false;
  /** Set by `abort`: a later or resumed `finish` rejects with the reason. */
  #aborted = false;
  #abortReason: unknown = undefined;

  /**
   * Throws `UnsupportedPlanError` before allocating for a shape it cannot compile; any other
   * failure while building releases what was already taken, then rethrows.
   */
  constructor(
    rel: AggregateRel,
    context: CompileContext,
    charge: ByteCharge,
    readonly downstream: Operator,
    options: AggregateOptions = {},
  ) {
    preflight(rel);
    this.#charge = charge;
    this.#interval = options.yieldIntervalMs ?? YIELD_INTERVAL_MS;
    this.#now = options.now ?? (() => performance.now());
    this.#target = options.targetBytes ?? BATCH_TARGET_BYTES;
    this.#keys = rel.groupBy;
    const keyTypes: PhysicalType[] = rel.groupBy.map((index) => {
      const type = context.input[index];
      if (type === undefined)
        throw new RangeError(`Group key ${String(index)} is outside the input`);
      return type;
    });
    const capacity = context.arena.capacity;
    this.#keyVectors = new Array<Vector>(keyTypes.length);
    try {
      this.#takeArena(context, 4 * capacity);
      this.#gids = new Int32Array(capacity);
      for (const call of rel.aggregates)
        this.#aggregates.push(this.#compile(call, context, capacity));
      this.#groups = keyTypes.length === 0 ? null : new GroupTable(keyTypes, charge);
      if (this.#groups === null)
        for (const aggregate of this.#aggregates) aggregate.accumulator.grow(1);
    } catch (error) {
      this.#close();
      context.arena.give(this.#arenaBytes);
      throw error;
    }
  }

  #takeArena(context: CompileContext, bytes: number): void {
    context.arena.take(bytes);
    this.#arenaBytes += bytes;
  }

  #compile(call: AggregateCall, context: CompileContext, capacity: number): CompiledAggregate {
    const [arg] = call.args;
    const argument =
      call.fn === "count_star" || arg === undefined ? null : compileExpr(arg, context);
    const accumulator = accumulatorFor(call.fn, call.type, argument?.type ?? null, this.#charge);
    let distinct: DistinctSet | null = null;
    let filter: Predicate | null;
    let selection: Selection;
    // Every fallible step after the accumulator is inside this scope, so a failure closes it.
    try {
      filter = call.filter === null ? null : new Predicate(call.filter, context);
      distinct =
        call.distinct && argument !== null
          ? new DistinctSet(argument.type, this.#charge, capacity)
          : null;
      this.#takeArena(context, 2 * capacity);
      selection = { rows: new Uint16Array(capacity), count: 0 };
    } catch (error) {
      accumulator.close();
      distinct?.close();
      throw error;
    }
    return {
      accumulator,
      argument,
      filter,
      distinct,
      selection,
    };
  }

  push(batch: Batch): PushResult {
    const length = batch.length;
    const groups = this.#groups;
    const gids = this.#gids;
    const scalar = groups === null;
    if (groups !== null) {
      for (let c = 0; c < this.#keys.length; c++) {
        const column = batch.columns[this.#keys[c] ?? 0];
        if (column === undefined) throw new RangeError("Group key column missing from the batch");
        this.#keyVectors[c] = column;
      }
      groups.assign(this.#keyVectors, length, batch.selection, gids);
      for (const aggregate of this.#aggregates) aggregate.accumulator.grow(groups.groupCount);
    }
    for (const aggregate of this.#aggregates) {
      let rows: Selection | null = batch.selection;
      if (aggregate.filter !== null) {
        rows = aggregate.filter.select(batch);
        if (rows.count === 0) continue;
      }
      const value = aggregate.argument === null ? null : aggregate.argument.evaluate(batch, rows);
      if (aggregate.distinct !== null && value !== null)
        rows = aggregate.distinct.filter(value, gids, length, rows, scalar, aggregate.selection);
      aggregate.accumulator.update(value, gids, rowsIn(rows), countIn(rows, length), scalar);
    }
    return "ready";
  }

  ready(): Promise<void> {
    return Promise.resolve();
  }

  /** An upper bound of the bytes the output batch of groups [from, from + count) allocates. */
  #outputBytes(from: number, count: number): number {
    let bytes = 0;
    const groups = this.#groups;
    if (groups !== null)
      for (let c = 0; c < this.#keys.length; c++) bytes += groups.keyBytes(c, from, count);
    for (const aggregate of this.#aggregates)
      bytes += aggregate.accumulator.outputBytes(from, count);
    return bytes;
  }

  /** Emits every group in charged, byte-bounded batches and paced slices (see the header). */
  finish(): MaybePromise<void> {
    return this.#emit();
  }

  #emit(): MaybePromise<void> {
    if (this.#aborted) throw this.#abortReason;
    const total = this.#groups === null ? 1 : this.#groups.groupCount;
    const sliceStart = this.#now();
    const expired = (): boolean => this.#closed || this.#now() - sliceStart >= this.#interval;
    while (this.#emitted < total) {
      // A downstream push may abort this operator: check before every batch.
      if (this.#isAborted()) throw this.#abortReason;
      if (this.#now() - sliceStart >= this.#interval) return yieldTask().then(() => this.#emit());
      let count = Math.min(BATCH_ROWS, total - this.#emitted);
      // Expensive per-group results (AVG over NUMERIC) are prepared under the slice's deadline,
      // and the batch shrinks to what is ready: the rest resumes after a task yield.
      for (const { accumulator } of this.#aggregates)
        if (accumulator.prepare !== undefined)
          count = Math.min(count, accumulator.prepare(this.#emitted, count, expired));
      let bytes = this.#outputBytes(this.#emitted, count);
      while (count > 1 && bytes > this.#target) {
        count = Math.max(1, Math.floor(count / 2));
        bytes = this.#outputBytes(this.#emitted, count);
      }
      // Charge before building; a refusal halves the batch, down to one group.
      while (!this.#charge.allocate(bytes)) {
        if (count === 1)
          throw new RangeError(
            `A ${String(bytes)}-byte aggregate output batch was refused by its grant`,
          );
        count = Math.max(1, Math.floor(count / 2));
        bytes = this.#outputBytes(this.#emitted, count);
      }
      this.#outputHeld = bytes;
      const columns: FlatVector[] = [];
      try {
        const groups = this.#groups;
        if (groups !== null)
          for (let c = 0; c < this.#keys.length; c++)
            columns.push(groups.emitKey(c, this.#emitted, count));
        for (const aggregate of this.#aggregates)
          columns.push(aggregate.accumulator.emit(this.#emitted, count));
      } catch (error) {
        this.#releaseOutput();
        throw error;
      }
      this.#emitted += count;
      if (this.downstream.push({ length: count, columns, selection: null }) === "blocked")
        return this.downstream.ready().then(
          () => {
            this.#releaseOutput();
            return this.#emit();
          },
          (error: unknown) => {
            this.#releaseOutput();
            throw error;
          },
        );
      this.#releaseOutput();
    }
    this.#close();
    return this.downstream.finish();
  }

  #isAborted(): boolean {
    return this.#aborted;
  }

  #releaseOutput(): void {
    if (this.#outputHeld > 0) this.#charge.free(this.#outputHeld);
    this.#outputHeld = 0;
  }

  #close(): void {
    if (this.#closed) return;
    this.#closed = true;
    this.#releaseOutput();
    this.#groups?.close();
    for (const aggregate of this.#aggregates) {
      aggregate.accumulator.close();
      aggregate.distinct?.close();
    }
  }

  abort(reason: unknown): void {
    if (!this.#aborted) {
      this.#aborted = true;
      this.#abortReason = reason;
    }
    this.#close();
    this.downstream.abort(reason);
  }
}
