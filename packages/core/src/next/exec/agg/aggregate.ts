/**
 * The `aggregate` operator (ir/ir.ts): hash aggregation by group id, struct-of-arrays state.
 *
 * - push: the group keys (input columns `groupBy`) map each active row to a group id (groups.ts);
 *   each aggregate evaluates its argument on its rows — after its FILTER, and through its DISTINCT
 *   set — and updates its accumulator (states.ts). Synchronous; nothing is retained from the
 *   batch (keys and values are copied into the state), so every push is `ready`.
 * - finish: normally emits groups in first-seen order (key columns, then one per aggregate),
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
import {
  YIELD_INTERVAL_MS,
  afterYield,
  clockFor,
  yieldTask,
  type Operator,
  type PushResult,
  type TaskClock,
} from "../pipeline.js";
import { DistinctSet } from "./distinct.js";
import { ScalarByteDistinct, supportsScalarByteDistinct } from "./scalar-byte-distinct.js";
import { GroupTopN, supportsGroupTopN, type GroupTopNSpec } from "./group-top-n.js";
import { GroupTable } from "./groups.js";
import { accumulatorFor, fusedSums, updateGroupedPair, type Accumulator } from "./states.js";

type AggregateRel = Extract<Rel, { op: "aggregate" }>;

/**
 * Group-ordered updates pay off once enough accumulators walk the rows over enough groups
 * (measured: 20 aggregates over 10,000 groups 2.2× faster sorted; 2 over 632,000 slower).
 */
const SORT_MIN_AGGREGATES = 4;
const SORT_MIN_GROUPS = 4096;

/** What a cleared fused-column slot holds (no borrowed array). */
const NO_COLUMN = new Float64Array(0);

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
  readonly distinct: DistinctSet | ScalarByteDistinct | null;
  readonly selection: Selection;
  /** The output's type and, for a shared state, which of its results it reads. */
  readonly type: PhysicalType;
  readonly reads: "own" | "sum" | "average";
  /** False for the second reader of a shared state (it neither updates nor closes it). */
  readonly updates: boolean;
  /** In the fused scalar pass (no GROUP BY, no FILTER or DISTINCT, an exact sum). */
  readonly fusable: boolean;
  /** undefined: not eligible; null: COUNT(*); number: plain SUM input column. */
  readonly groupedColumn: number | null | undefined;
}

/**
 * Plain SUM / AVG over one input column of an exact type (integers, NUMERIC): candidates for a
 * shared state. Floats never share: SUM starts from −0 and AVG from 0, and their overflow rules
 * differ (float8pl, float8_accum).
 */
function shareKey(call: AggregateCall, input: readonly PhysicalType[]): string | null {
  const [arg] = call.args;
  if ((call.fn !== "sum" && call.fn !== "avg") || call.distinct || call.filter !== null)
    return null;
  if (call.args.length !== 1 || arg?.op !== "column") return null;
  const kind = input[arg.index]?.kind;
  if (kind !== "int32" && kind !== "int64" && kind !== "decimal") return null;
  return String(arg.index);
}

/** Output pacing (tests inject a clock). */
export interface AggregateOptions {
  /** Synchronous emitting between task yields (default `YIELD_INTERVAL_MS`). */
  readonly yieldIntervalMs?: number;
  /** The clock in ms (default `performance.now`). */
  readonly now?: () => number;
  /** The output batch byte target (default `BATCH_TARGET_BYTES`). */
  readonly targetBytes?: number;
  /**
   * The execution's cancellation (R23.2): checked at every output slice and after downstream
   * backpressure; once aborted, `finish` rejects with its reason.
   */
  readonly signal?: AbortSignal;
  /** False: never order a batch's rows by group (tests compare against it). Default: by size. */
  readonly groupOrder?: boolean;
  /** Internal output reduction; the downstream sort still orders/applies OFFSET. */
  readonly outputTopN?: GroupTopNSpec;
  /** False keeps grouped COUNT(DISTINCT) on its legacy pair set (benchmark control). */
  readonly groupedDistinctBitmap?: boolean;
  /** False retains generic value interning for scalar byte COUNT(DISTINCT) (benchmark control). */
  readonly scalarByteDistinct?: boolean;
  /** False retains separate grouped update loops (benchmark control). */
  readonly groupedUpdateFusion?: boolean;
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
  /** The fused scalar pass's members, and per-batch scratch. */
  #fused: readonly CompiledAggregate[] = [];
  readonly #fusedColumns: Float64Array[] = [];
  readonly #fusedMembers: CompiledAggregate[] = [];
  /** Group-ordered rows (`#sortByGroup`): two buffers, the histogram, the selection handed out. */
  #sortA = new Uint16Array(0);
  #sortB = new Uint16Array(0);
  #histogram = new Int32Array(0);
  readonly #sorted: { rows: Uint16Array; count: number } = { rows: new Uint16Array(0), count: 0 };
  /** Accumulators updated over the batch's own rows (no FILTER): what a sort serves. */
  #sortPasses = 0;
  readonly #groupedUpdateFusion: boolean;
  #fusedTotals = new Float64Array(0);
  readonly #gids: Int32Array = new Int32Array(0);
  readonly #keyVectors: Vector[];
  readonly #charge: ByteCharge;
  readonly #interval: number;
  /** The worker's task clock, or a test's (R23.5): no slice start of its own. */
  readonly #clock: TaskClock;
  readonly #target: number;
  /** Bytes the operator took from the arena (gids, selections), returned on a failed build. */
  #arenaBytes = 0;
  #emitted = 0;
  #top: GroupTopN | null = null;
  #ranked = 0;
  #selected = false;
  /** The charged bytes of the output batch downstream still holds. */
  #outputHeld = 0;
  #closed = false;
  readonly #signal: AbortSignal | null;
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
    this.#groupedUpdateFusion = options.groupedUpdateFusion !== false;
    this.#interval = options.yieldIntervalMs ?? YIELD_INTERVAL_MS;
    this.#clock = clockFor(options.now);
    this.#target = options.targetBytes ?? BATCH_TARGET_BYTES;
    this.#signal = options.signal ?? null;
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
      // A SUM and an AVG of the same column share one exact-sum state (one pass over it).
      const keys = rel.aggregates.map((call) => shareKey(call, context.input));
      const both = new Set(
        keys.filter(
          (key, k) =>
            key !== null &&
            rel.aggregates.some(
              (other, o) => keys[o] === key && other.fn !== rel.aggregates[k]?.fn,
            ),
        ),
      );
      const shared = new Map<string, CompiledAggregate>();
      rel.aggregates.forEach((call, k) => {
        const key = keys[k] ?? null;
        const sharing = key !== null && both.has(key);
        const owner = sharing ? shared.get(key) : undefined;
        if (owner?.accumulator.emitAs !== undefined) {
          this.#aggregates.push({
            ...owner,
            type: call.type,
            reads: call.fn === "avg" ? "average" : "sum",
            updates: false,
            fusable: false,
          });
          return;
        }
        const compiled = this.#compile(
          call,
          context,
          capacity,
          sharing,
          rel.groupBy.length > 0 && options.groupedDistinctBitmap !== false,
          rel.groupBy.length === 0 && options.scalarByteDistinct !== false,
        );
        this.#aggregates.push(compiled);
        if (sharing) shared.set(key, compiled);
      });
      this.#groups = keyTypes.length === 0 ? null : new GroupTable(keyTypes, charge);
      const top = options.outputTopN;
      if (top && supportsGroupTopN(rel, context.input, top)) this.#top = new GroupTopN(top, charge);
      this.#sortPasses =
        options.groupOrder === false
          ? 0
          : this.#aggregates.filter((a) => a.updates && a.filter === null).length;
      if (this.#groups !== null && this.#sortPasses >= SORT_MIN_AGGREGATES) {
        this.#takeArena(context, 2 * 2 * capacity + 4 * 2049);
        this.#sortA = new Uint16Array(capacity);
        this.#sortB = new Uint16Array(capacity);
        this.#histogram = new Int32Array(2049);
      }
      if (this.#groups === null) {
        for (const aggregate of this.#aggregates)
          if (aggregate.updates) aggregate.accumulator.grow(1);
        this.#fused = this.#aggregates.filter((a) => a.fusable);
        // Charged like the operator's other per-instance buffers (returned on a failed build).
        this.#takeArena(context, 8 * this.#fused.length);
        this.#fusedTotals = new Float64Array(this.#fused.length);
      }
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

  #compile(
    call: AggregateCall,
    context: CompileContext,
    capacity: number,
    sharedState: boolean,
    groupedBitmap: boolean,
    scalarBytes: boolean,
  ): CompiledAggregate {
    const [arg] = call.args;
    const argument =
      call.fn === "count_star" || arg === undefined ? null : compileExpr(arg, context);
    // A shared state is built as AVG's (it also counts) and read as either.
    const accumulator = accumulatorFor(
      sharedState ? "avg" : call.fn,
      call.type,
      argument?.type ?? null,
      this.#charge,
    );
    let distinct: DistinctSet | ScalarByteDistinct | null = null;
    let filter: Predicate | null;
    let selection: Selection;
    // Every fallible step after the accumulator is inside this scope, so a failure closes it.
    try {
      filter = call.filter === null ? null : new Predicate(call.filter, context);
      distinct =
        call.distinct && argument !== null
          ? scalarBytes && call.fn === "count" && supportsScalarByteDistinct(argument.type)
            ? new ScalarByteDistinct(argument.type, this.#charge)
            : new DistinctSet(
                argument.type,
                this.#charge,
                capacity,
                groupedBitmap && call.fn === "count",
              )
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
      type: call.type,
      reads:
        sharedState && accumulator.emitAs !== undefined
          ? call.fn === "avg"
            ? "average"
            : "sum"
          : "own",
      updates: true,
      groupedColumn:
        call.filter === null && !call.distinct
          ? call.fn === "count_star" && call.args.length === 0
            ? null
            : call.fn === "sum" && call.args.length === 1 && arg?.op === "column"
              ? arg.index
              : undefined
          : undefined,
      fusable:
        call.filter === null &&
        !call.distinct &&
        argument !== null &&
        accumulator.fusedValues !== undefined,
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
    if (scalar && this.#fused.length > 0) this.#pushFused(batch, length);
    // Many groups and several aggregates: the accumulators walk the batch's rows in group order
    // (`#sortByGroup`), so each one's state arrays are read nearly sequentially.
    const sorted =
      groups !== null &&
      this.#sortPasses >= SORT_MIN_AGGREGATES &&
      groups.groupCount > SORT_MIN_GROUPS
        ? this.#sortByGroup(gids, batch.selection, length, groups.groupCount)
        : null;
    for (let index = 0; index < this.#aggregates.length; index++) {
      const aggregate = this.#aggregates[index];
      if (aggregate === undefined || !aggregate.updates || (scalar && aggregate.fusable)) continue;
      const next = this.#aggregates[index + 1];
      if (
        !scalar &&
        this.#groupedUpdateFusion &&
        next?.updates === true &&
        aggregate.groupedColumn !== undefined &&
        typeof next.groupedColumn === "number"
      ) {
        const left =
          aggregate.groupedColumn === null ? null : batch.columns[aggregate.groupedColumn];
        const right = batch.columns[next.groupedColumn];
        const rows = sorted ?? batch.selection;
        // Only direct columns: evaluating a later fallible expression before an earlier
        // aggregate finishes would change which error wins. NULL/encoded inputs fall back.
        if (
          left !== undefined &&
          right !== undefined &&
          updateGroupedPair(
            aggregate.accumulator,
            next.accumulator,
            left,
            right,
            gids,
            rowsIn(rows),
            countIn(rows, length),
          )
        ) {
          index++;
          continue;
        }
      }
      let rows: Selection | null = batch.selection;
      if (aggregate.filter !== null) {
        rows = aggregate.filter.select(batch);
        if (rows.count === 0) continue;
      }
      // Arguments evaluate over the batch's own (ascending) selection; only the accumulator and
      // the DISTINCT set read the rows in group order.
      const value = aggregate.argument === null ? null : aggregate.argument.evaluate(batch, rows);
      if (aggregate.filter === null && sorted !== null) rows = sorted;
      if (aggregate.distinct !== null && value !== null)
        rows = aggregate.distinct.filter(value, gids, length, rows, scalar, aggregate.selection);
      aggregate.accumulator.update(value, gids, rowsIn(rows), countIn(rows, length), scalar);
    }
    return "ready";
  }

  /**
   * The active rows ordered by group id, stably (a group's rows keep their input order, which
   * float sums and MIN/MAX ties depend on): an LSD radix sort, 11 bits of group id per pass.
   */
  #sortByGroup(
    gids: Int32Array,
    selection: Selection | null,
    length: number,
    groupCount: number,
  ): Selection {
    const rows = rowsIn(selection);
    const count = countIn(selection, length);
    const histogram = this.#histogram;
    let source = rows;
    let target = this.#sortA;
    let other = this.#sortB;
    for (let shift = 0; groupCount - 1 >= 1 << shift || shift === 0; shift += 11) {
      histogram.fill(0);
      for (let k = 0; k < count; k++) {
        const bucket = (((gids[source[k] ?? 0] ?? 0) >>> shift) & 2047) + 1;
        histogram[bucket] = (histogram[bucket] ?? 0) + 1;
      }
      for (let b = 1; b <= 2048; b++) histogram[b] = (histogram[b] ?? 0) + (histogram[b - 1] ?? 0);
      for (let k = 0; k < count; k++) {
        const row = source[k] ?? 0;
        const bucket = ((gids[row] ?? 0) >>> shift) & 2047;
        const at = histogram[bucket] ?? 0;
        target[at] = row;
        histogram[bucket] = at + 1;
      }
      source = target;
      [target, other] = [other, target];
      if (shift >= 22) break;
    }
    this.#sorted.rows = source;
    this.#sorted.count = count;
    return this.#sorted;
  }

  /**
   * The fused scalar pass: members whose batch qualifies (`Accumulator.fusedValues`) are summed in
   * one loop over their columns; the others update one by one as usual.
   */
  #pushFused(batch: Batch, length: number): void {
    const rows = rowsIn(batch.selection);
    const count = countIn(batch.selection, length);
    const columns = this.#fusedColumns;
    const members = this.#fusedMembers;
    let n = 0;
    try {
      for (const aggregate of this.#fused) {
        if (aggregate.argument === null) continue;
        const value = aggregate.argument.evaluate(batch, batch.selection);
        const values = aggregate.accumulator.fusedValues?.(value, rows, count) ?? null;
        if (values === null) {
          aggregate.accumulator.update(value, this.#gids, rows, count, true);
          continue;
        }
        columns[n] = values;
        members[n] = aggregate;
        n += 1;
      }
      if (n === 0) return;
      const totals = this.#fusedTotals;
      fusedSums(columns, n, count, totals);
      for (let j = 0; j < n; j++) members[j]?.accumulator.addFused?.(totals[j] ?? 0, count);
    } finally {
      // The batch's coefficient arrays are borrowed: keep no reference once the push returns
      // (on every exit, an exception included), so a released input is never kept alive.
      for (let j = 0; j < n; j++) columns[j] = NO_COLUMN;
    }
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

  /** Emits every group or the selected winners, charged and paced (see the header). */
  finish(): MaybePromise<void> {
    return this.#emit();
  }

  #emit(): MaybePromise<void> {
    if (this.#isAborted()) throw this.#abortReason;
    const groupsTotal = this.#groups === null ? 1 : this.#groups.groupCount;
    const top = this.#top;
    if (top !== null && !this.#selected) {
      while (this.#ranked < groupsTotal) {
        if (this.#clock.due(this.#interval))
          return yieldTask().then(() => {
            afterYield(this.#clock);
            return this.#emit();
          });
        const first = this.#ranked;
        let n = Math.min(BATCH_ROWS, groupsTotal - first);
        const needed = () =>
          top.spec.keys.reduce(
            (bytes, key) =>
              bytes +
              (key.column === 0
                ? (this.#groups?.keyBytes(0, first, n) ?? 0)
                : (this.#aggregates[key.column - 1]?.accumulator.outputBytes(first, n) ?? 0)),
            0,
          );
        let bytes = needed();
        while (!this.#charge.allocate(bytes)) {
          if (n === 1) throw new RangeError("Aggregate rank output exceeded its grant");
          n = Math.max(1, n >>> 1);
          bytes = needed();
        }
        try {
          const columns = top.spec.keys.map((key) => {
            const vector =
              key.column === 0
                ? this.#groups?.emitKey(0, first, n)
                : this.#aggregates[key.column - 1]?.accumulator.emit(first, n);
            if (!vector) throw new Error("Aggregate rank column");
            return vector;
          });
          top.offer(columns, first, n);
        } finally {
          this.#charge.free(bytes);
        }
        this.#ranked += n;
        if (this.#isAborted()) throw this.#abortReason;
      }
      this.#selected = true;
    }
    const total = top === null ? groupsTotal : top.size;
    // The task's deadline (R23.5): neither `finish` nor a resolved `ready()` starts a slice.
    const expired = (): boolean => this.#closed || this.#clock.due(this.#interval);
    while (this.#emitted < total) {
      // A downstream push may abort this operator: check before every batch.
      if (this.#isAborted()) throw this.#abortReason;
      if (this.#clock.due(this.#interval))
        return yieldTask().then(() => {
          afterYield(this.#clock);
          return this.#emit();
        });
      const from = top === null ? this.#emitted : top.group(this.#emitted);
      let count = top === null ? Math.min(BATCH_ROWS, total - this.#emitted) : 1;
      let bytes = this.#outputBytes(from, count);
      while (count > 1 && bytes > this.#target) {
        count = Math.max(1, Math.floor(count / 2));
        bytes = this.#outputBytes(from, count);
      }
      // Charge before building; a refusal halves the batch, down to one group.
      while (!this.#charge.allocate(bytes)) {
        if (count === 1)
          throw new RangeError(
            `A ${String(bytes)}-byte aggregate output batch was refused by its grant`,
          );
        count = Math.max(1, Math.floor(count / 2));
        bytes = this.#outputBytes(from, count);
      }
      this.#outputHeld = bytes;
      // Expensive per-group results (AVG over NUMERIC) are prepared under the slice's deadline,
      // only for this batch, after its output charge: the batch shrinks to what is ready (its
      // charge stays an upper bound) and the rest resumes after a task yield.
      try {
        for (const { accumulator } of this.#aggregates)
          if (accumulator.prepare !== undefined)
            count = Math.min(count, accumulator.prepare(from, count, expired));
      } catch (error) {
        this.#releaseOutput();
        throw error;
      }
      const columns: FlatVector[] = [];
      try {
        const groups = this.#groups;
        if (groups !== null)
          for (let c = 0; c < this.#keys.length; c++) columns.push(groups.emitKey(c, from, count));
        for (const aggregate of this.#aggregates)
          columns.push(
            aggregate.reads === "own" || aggregate.accumulator.emitAs === undefined
              ? aggregate.accumulator.emit(from, count)
              : aggregate.accumulator.emitAs(
                  aggregate.reads === "average",
                  aggregate.type,
                  from,
                  count,
                ),
          );
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

  /** Aborted by `abort` or by the signal (which then aborts the operator and downstream). */
  #isAborted(): boolean {
    const signal = this.#signal;
    if (!this.#aborted && signal?.aborted === true) this.abort(signal.reason);
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
    this.#top?.close();
    this.#groups?.close();
    for (const aggregate of this.#aggregates) {
      if (!aggregate.updates) continue;
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
