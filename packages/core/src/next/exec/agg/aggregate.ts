/**
 * The `aggregate` operator (ir/ir.ts): hash aggregation by group id, struct-of-arrays state.
 *
 * - push: the group keys (input columns `groupBy`) map each active row to a group id (groups.ts);
 *   each aggregate evaluates its argument on its rows — after its FILTER, and through its DISTINCT
 *   set — and updates its accumulator (states.ts). Synchronous; nothing is retained from the
 *   batch (keys and values are copied into the state), so every push is `ready`.
 * - finish: emits the groups in first-seen order, in batches of up to `BATCH_ROWS` groups (key
 *   columns, then one column per aggregate), waiting on downstream readiness between batches, then
 *   finishes downstream. Without GROUP BY there is exactly one output row, even over no input
 *   (COUNT 0, other aggregates NULL); with GROUP BY, no input means no rows.
 * - Not compiled yet (`UnsupportedPlanError`): GROUPING SETS with more than one set, ordered
 *   aggregates (`orderBy`), and string_agg / array_agg / json_agg / percentiles / mode.
 * - Memory: group table and states grow from the grant; a refused growth fails the query until
 *   partitioned aggregation spill (milestone 3).
 */
import type { MaybePromise } from "../../access/key-index.js";
import type { AggregateCall, Rel } from "../../ir/ir.js";
import type { PhysicalType } from "../../types.js";
import type { ByteCharge } from "../../vector/varlen.js";
import {
  BATCH_ROWS,
  type Batch,
  type FlatVector,
  type Selection,
  type Vector,
} from "../../vector/vector.js";
import { UnsupportedPlanError } from "../errors.js";
import { Predicate } from "../filter.js";
import { compileExpr, type CompileContext } from "../kernels/compile.js";
import { countIn, rowsIn, type Compiled } from "../kernels/vectors.js";
import type { Operator, PushResult } from "../pipeline.js";
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

export class AggregateOperator implements Operator {
  readonly #keys: readonly number[];
  readonly #groups: GroupTable | null;
  readonly #aggregates: readonly CompiledAggregate[];
  readonly #gids: Int32Array;
  readonly #keyVectors: Vector[];
  readonly #charge: ByteCharge;
  #emitted = 0;
  #closed = false;

  constructor(
    rel: AggregateRel,
    context: CompileContext,
    charge: ByteCharge,
    readonly downstream: Operator,
  ) {
    if (
      rel.groupingSets !== null &&
      !(rel.groupingSets.length === 1 && rel.groupingSets[0]?.length === rel.groupBy.length)
    )
      throw new UnsupportedPlanError("GROUPING SETS", { construct: "aggregate" });
    this.#charge = charge;
    this.#keys = rel.groupBy;
    const keyTypes: PhysicalType[] = rel.groupBy.map((index) => {
      const type = context.input[index];
      if (type === undefined)
        throw new RangeError(`Group key ${String(index)} is outside the input`);
      return type;
    });
    this.#groups = keyTypes.length === 0 ? null : new GroupTable(keyTypes, charge);
    const capacity = context.arena.capacity;
    context.arena.take(4 * capacity);
    this.#gids = new Int32Array(capacity);
    this.#keyVectors = new Array<Vector>(keyTypes.length);
    this.#aggregates = rel.aggregates.map((call) => this.#compile(call, context, capacity));
    if (this.#groups === null)
      for (const aggregate of this.#aggregates) aggregate.accumulator.grow(1);
  }

  #compile(call: AggregateCall, context: CompileContext, capacity: number): CompiledAggregate {
    if (UNSUPPORTED.has(call.fn))
      throw new UnsupportedPlanError(`the aggregate ${call.fn}`, { construct: "aggregate" });
    if (call.orderBy.length > 0)
      throw new UnsupportedPlanError("ordered aggregates", { construct: "aggregate" });
    if (call.args.length > 1)
      throw new UnsupportedPlanError(`${call.fn} with ${String(call.args.length)} arguments`, {
        construct: "aggregate",
      });
    const [arg] = call.args;
    const argument =
      call.fn === "count_star" || arg === undefined ? null : compileExpr(arg, context);
    context.arena.take(2 * capacity);
    return {
      accumulator: accumulatorFor(call.fn, call.type, argument?.type ?? null, this.#charge),
      argument,
      filter: call.filter === null ? null : new Predicate(call.filter, context),
      distinct:
        call.distinct && argument !== null
          ? new DistinctSet(argument.type, this.#charge, capacity)
          : null,
      selection: { rows: new Uint16Array(capacity), count: 0 },
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

  /** Emits every group, waiting on downstream readiness between output batches. */
  finish(): MaybePromise<void> {
    const total = this.#groups === null ? 1 : this.#groups.groupCount;
    while (this.#emitted < total) {
      const count = Math.min(BATCH_ROWS, total - this.#emitted);
      const columns: FlatVector[] = [];
      const groups = this.#groups;
      if (groups !== null)
        for (let c = 0; c < this.#keys.length; c++)
          columns.push(groups.emitKey(c, this.#emitted, count));
      for (const aggregate of this.#aggregates)
        columns.push(aggregate.accumulator.emit(this.#emitted, count));
      this.#emitted += count;
      if (this.downstream.push({ length: count, columns, selection: null }) === "blocked")
        return this.downstream.ready().then(() => this.finish());
    }
    this.#close();
    return this.downstream.finish();
  }

  #close(): void {
    if (this.#closed) return;
    this.#closed = true;
    this.#groups?.close();
    for (const aggregate of this.#aggregates) {
      aggregate.accumulator.close();
      aggregate.distinct?.close();
    }
  }

  abort(reason: unknown): void {
    this.#close();
    this.downstream.abort(reason);
  }
}
