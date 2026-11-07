/** Immutable pipeline recipes; every execution instantiates its own operators and kernels. */
import type { MaybePromise } from "../access/key-index.js";
import type { PhysicalPlan, Rel, ScalarExpr, SortKey } from "../ir/ir.js";
import { ResultSink } from "./result-sink.js";
import { batchTargetFor, type Grant } from "../memory/governor.js";
import { reclaimingReader } from "./reclaim-reader.js";
import type { ByteCharge } from "../vector/varlen.js";
import { samePhysicalType, type PhysicalType } from "../types.js";
import { BATCH_ROWS, type Batch, type Selection, type Vector } from "../vector/vector.js";
import { int64At } from "../vector/int64.js";
import { AggregateOperator } from "./agg/aggregate.js";
import {
  isOrderedSet,
  orderedSetArguments,
  orderedSetMinimumBytes,
  OrderedSetOperator,
} from "./agg/ordered-set.js";
import { UnsupportedPlanError } from "./errors.js";
import { FilterOperator, ProjectOperator } from "./filter.js";
import {
  HashJoin,
  hashJoinMinimumBytes,
  validateHashJoinSpec,
  type HashJoinSpec,
  type VectorPredicate,
} from "./join.js";
import { compileExpr, type CompileContext, type CallResolver } from "./kernels/compile.js";
import { bindCall } from "./kernels/bindings.js";
import { OutputArena, isValidSlot, slotOf } from "./kernels/vectors.js";
import { LimitOperator } from "./limit.js";
import { nullParameter, integerParameterBytes } from "./parameters.js";
import { drive, isPromise, type BatchSource, type Operator } from "./pipeline.js";
import type { QueryRuntime, Program, OperatorIdentity } from "./run.js";
import { lateSortShape, type LateSortShape } from "./late-sort.js";
import { LateFetchOperator, ORDINAL_TYPE } from "./late-fetch.js";
import { OBJECT_OVERHEAD_BYTES } from "../memory/governor.js";
import { SortOperator, sortMinimumBytes } from "./sort.js";
import { sortMode } from "./sort-keys.js";
import type { SortColumn } from "./sort-keys.js";
import { windowInputLayout, remapWindowFunction, literalWindowAmount } from "./window-input.js";
import { TableScan, memoryScanBufferBytes } from "./table-scan.js";
import { orderedSort, indexMinimums, runIndexOrder, type IndexShape } from "./key-order.js";
import {
  WindowOperator,
  windowMinimumBytes,
  validateWindowSpec,
  type WindowFunctionSpec,
} from "./window.js";

type Stop = () => boolean;
type Recipe = (
  runtime: QueryRuntime,
  sink: Operator,
  stops: readonly Stop[],
  rowsNeeded?: () => number,
) => MaybePromise<void>;
type Setup<T> = (context: CompileContext, grant: Grant) => T;
const EMPTY: Batch = { length: 1, columns: [], selection: null };
const DISCARD: Operator = {
  push: (_batch, transfer) => {
    transfer?.release();
    return "ready";
  },
  ready: () => Promise.resolve(),
  finish: () => undefined,
  abort: () => undefined,
};

function then<T, U>(value: MaybePromise<T>, next: (value: T) => MaybePromise<U>): MaybePromise<U> {
  return isPromise(value) ? value.then(next) : next(value);
}

function at<T>(values: readonly T[], index: number): T {
  const value = values[index];
  if (value === undefined) throw new TypeError("IR column or argument is out of range");
  return value;
}
function types(rel: Rel): readonly PhysicalType[] {
  return rel.output.map((column) => column.type);
}
function columns(input: readonly PhysicalType[]): ScalarExpr[] {
  return input.map((type, index) => ({ op: "column", index, type }));
}

/** Never stamp the worker task clock here: construction can run in the middle of a task. */
class Resources {
  readonly arena: OutputArena;
  #closed = false;
  constructor(
    readonly grant: Grant,
    capacity: number,
  ) {
    // Breakers own their grant and may close it in finish/abort before the kernel arena closes.
    this.arena = new OutputArena(
      {
        allocate: (bytes) => grant.allocate(bytes),
        free: (bytes) => {
          if (grant.held !== 0) grant.free(bytes);
        },
      },
      capacity,
    );
  }
  close(): void {
    if (this.#closed) return;
    this.#closed = true;
    this.arena.close();
    this.grant.close();
  }
}

/** Validate kernels with small compiler scratch, never constructing breaker pools or grants.
 * Kernel buffers are affine in capacity at multiples of 32 (validity words and power-of-two
 * decimal scratch included). Two small probes retain the fixed term and measure the per-row
 * term; execution still allocates its real buffers only from the admitted grant. */
export function measureCompilerBuffers<T>(
  plan: { readonly parameters: PhysicalPlan["parameters"]; readonly resolveCall: CallResolver },
  input: readonly PhysicalType[],
  capacity: number,
  setup: (context: CompileContext, charge: ByteCharge) => T,
  dispose: (value: T) => void,
  construct = "expression",
): number {
  const probe = (rows: number): number => {
    let bytes = 0;
    const charge: ByteCharge = {
      allocate: (n) => {
        bytes += n;
        return true;
      },
      free: (n) => {
        bytes -= n;
      },
    };
    const arena = new OutputArena(charge, rows);
    try {
      const value = setup(
        {
          arena,
          input,
          params: { slots: plan.parameters.map(nullParameter) },
          resolveCall: plan.resolveCall,
        },
        charge,
      );
      const retained = bytes;
      dispose(value);
      return retained;
    } catch (error) {
      if (error instanceof TypeError || error instanceof RangeError)
        throw new UnsupportedPlanError(error.message, { construct });
      throw error;
    } finally {
      arena.close();
    }
  };
  if (capacity <= 32) return probe(capacity);
  const first = probe(32),
    second = probe(64);
  return Math.ceil(first + ((second - first) * (capacity - 32)) / 32);
}

function instantiate<T>(
  runtime: QueryRuntime,
  rel: OperatorIdentity,
  input: readonly PhysicalType[],
  minimum: number,
  capacity: number,
  setup: Setup<T>,
  use: (value: T) => MaybePromise<void>,
): MaybePromise<void> {
  return then(
    runtime.admit(`${rel.op}#${String(rel.id)}`, Math.max(4096, minimum), true, rel),
    (grant) => {
      const resources = new Resources(grant, capacity);
      runtime.dispose(() => resources.close());
      // Fixed buffers remain charged. Only unused holdings can be surrendered to another admission.
      grant.onReclaimRequest(() => {
        const free = grant.held - grant.allocated - grant.loaned;
        if (free > 0) grant.release(free);
      });
      try {
        const value = setup(
          {
            arena: resources.arena,
            params: runtime.params,
            input,
            ...(runtime.resolveCall === undefined ? {} : { resolveCall: runtime.resolveCall }),
          },
          grant,
        );
        const result = use(value);
        if (isPromise(result)) return result.finally(() => resources.close());
        resources.close();
      } catch (error) {
        resources.close();
        throw error;
      }
    },
  );
}

function counted(runtime: QueryRuntime, rel: OperatorIdentity, sink: Operator): Operator {
  const profile = runtime.profile(rel);
  return {
    push(batch, transfer) {
      profile.rowsOut += batch.selection?.count ?? batch.length;
      profile.batchesOut++;
      return sink.push(batch, transfer);
    },
    ready: () => sink.ready(),
    finish: () => sink.finish(),
    abort: (reason) => sink.abort(reason),
  };
}

/** Add non-column keys once, preserving the original columns at their original positions. */
function keyProjection(input: readonly PhysicalType[], expressions: readonly ScalarExpr[]) {
  const exprs = columns(input);
  const indexes = expressions.map((expr) => {
    if (expr.op === "column") return expr.index;
    const index = exprs.length;
    exprs.push(expr);
    return index;
  });
  return { exprs, indexes, types: exprs.map((expr) => expr.type) };
}
function order(keys: readonly SortKey[], indexes: readonly number[]): SortColumn[] {
  return keys.map((key, k) => ({
    column: at(indexes, k),
    direction: key.direction,
    nulls: key.nulls,
    collation: key.collation,
  }));
}

/** A compiled join condition observes original left ++ right positions, excluding hidden keys. */
function condition(
  expr: ScalarExpr | null,
  context: CompileContext,
  left: number,
  extendedLeft: number,
  right: number,
): VectorPredicate | null {
  if (expr === null) return null;
  const compiled = compileExpr(expr, context);
  const cols: Vector[] = [];
  const projected: { length: number; selection: Selection | null; columns: Vector[] } = {
    length: 0,
    selection: null,
    columns: cols,
  };
  const select = (batch: Batch, out: Selection, unknown: boolean): Selection => {
    for (let c = 0; c < left; c++) cols[c] = at(batch.columns, c);
    for (let c = 0; c < right; c++) cols[left + c] = at(batch.columns, extendedLeft + c);
    projected.length = batch.length;
    projected.selection = batch.selection;
    const vector = compiled.evaluate(projected, batch.selection);
    const n = batch.selection?.count ?? batch.length;
    let count = 0;
    for (let k = 0; k < n; k++) {
      const row = batch.selection?.rows[k] ?? k;
      const [flat, at] = slotOf(vector, row);
      const valid = isValidSlot(flat, at);
      if (unknown ? !valid : valid && flat.data.layout === "bool" && flat.data.values[at] === 1)
        out.rows[count++] = row;
    }
    out.count = count;
    return out;
  };
  return {
    filter: (batch, out) => select(batch, out, false),
    unknown: (batch, out) => select(batch, out, true),
  };
}

function setupWindow(
  rel: Extract<Rel, { op: "window" }>,
  input: readonly PhysicalType[],
  scalarAmounts: boolean,
) {
  const amounts = rel.functions.map((call) =>
    scalarAmounts ? literalWindowAmount(call.args[call.fn === "ntile" ? 0 : 1]) : undefined,
  );
  const exprs: ScalarExpr[] = rel.orderBy.map((key) => key.expr);
  const positions = rel.functions.map((call, index) =>
    call.args.map((expr, argIndex) => {
      if (amounts[index] !== undefined && argIndex === (call.fn === "ntile" ? 0 : 1)) return -1;
      const at = exprs.length;
      exprs.push(expr);
      return at;
    }),
  );
  const keys = keyProjection(input, exprs);
  const functions = (context: CompileContext, validating: boolean): WindowFunctionSpec[] =>
    rel.functions.map((call, index) => {
      const args = positions[index] ?? [];
      const arg =
        call.fn === "ntile" ? null : args[0] === undefined ? null : at(keys.indexes, args[0]);
      const amountAt = call.fn === "ntile" ? args[0] : args[1];
      const fallback = call.args[2];
      const scalarAmount = amounts[index];
      if (fallback !== undefined && fallback.op !== "literal" && fallback.op !== "param")
        throw new UnsupportedPlanError("a row-dependent lag/lead default", { construct: "window" });
      const scalar = (expr: ScalarExpr | null): number | null => {
        if (expr === null) return null;
        if (expr.op !== "literal" && expr.op !== "param")
          throw new UnsupportedPlanError("a row-dependent window frame offset", {
            construct: "window",
          });
        if (!["int32", "int64", "float32", "float64"].includes(expr.type.kind))
          throw new UnsupportedPlanError("a non-numeric window frame offset", {
            construct: "window",
          });
        const range = call.frame?.unit === "range";
        // B7 represents RANGE offsets as doubles. A parameter might require more than 53 bits;
        // refuse that capability at prepare rather than discovering it after reading input.
        if (range && expr.type.kind === "int64" && expr.op === "param")
          throw new UnsupportedPlanError("an int64 RANGE offset parameter", {
            construct: "window",
          });
        const compiled = compileExpr(expr, context);
        if (validating && !(range && expr.type.kind === "int64")) return 0;
        const result = compiled.evaluate(EMPTY, null);
        const [flat, at] = slotOf(result, 0);
        if (!isValidSlot(flat, at)) return null;
        const data = flat.data;
        if (data.layout === "int64") {
          const value = int64At(data.values, at);
          if (value < -BigInt(Number.MAX_SAFE_INTEGER) || value > BigInt(Number.MAX_SAFE_INTEGER)) {
            if (range)
              throw new UnsupportedPlanError("an inexact int64 RANGE offset", {
                construct: "window",
              });
            // ROWS/GROUPS count positions in a finite partition (at most a JS array's length).
            // Any larger positive offset reaches its edge; negative offsets retain PG's error.
            return value < 0n ? -Number.MAX_SAFE_INTEGER : Number.MAX_SAFE_INTEGER;
          }
          return Number(value);
        }
        if (data.layout === "i32" || data.layout === "f64" || data.layout === "f32")
          return data.values[at] ?? 0;
        throw new UnsupportedPlanError("a non-numeric window frame offset", {
          construct: "window",
        });
      };
      return {
        fn: call.fn,
        arg,
        type: call.type,
        ...(amountAt === undefined
          ? {}
          : amountAt === -1 && scalarAmount !== undefined
            ? { amount: scalarAmount }
            : { amount: { column: at(keys.indexes, amountAt) } }),
        fallback:
          fallback === undefined
            ? null
            : validating
              ? (compileExpr(fallback, context), null)
              : compileExpr(fallback, context).evaluate(EMPTY, null),
        frame:
          call.frame === null
            ? null
            : {
                ...call.frame,
                start: { kind: call.frame.start.kind, offset: scalar(call.frame.start.offset) },
                end: { kind: call.frame.end.kind, offset: scalar(call.frame.end.offset) },
              },
      };
    });
  return { ...keys, functions, orderBy: order(rel.orderBy, keys.indexes) };
}

/** Compile capability checks and recipes, never retaining a mutable kernel instance. */
export interface CompileOptions {
  readonly lateSort?: boolean;
  readonly keyOrder?: boolean;
  readonly lateFetchOnce?: boolean;
  /** False retains the original window inputs for component/query benchmark controls. */
  readonly windowInputDeduplication?: boolean;
  /** Parent control for literal-amount gather benchmarks. */
  readonly windowScalarArguments?: boolean;
}
export function compilePlan(plan: PhysicalPlan, options: CompileOptions = {}): Program {
  const bindings = new Map<string, ReturnType<typeof bindCall>>();
  const resolveCall: CallResolver = (fn, args) => {
    const key = fn + JSON.stringify(args);
    let binding = bindings.get(key);
    if (binding === undefined) {
      binding = bindCall(fn, args);
      bindings.set(key, binding);
    }
    return binding;
  };
  const compilation = { parameters: plan.parameters, resolveCall };

  if (plan.initPlans.length !== 0)
    throw new UnsupportedPlanError("init plans", { construct: "initPlan" });
  const admissions: Array<{
    rel: OperatorIdentity;
    minimum: number | ((cap: number) => number);
    owner: number;
    loanSlots: number;
  }> = [];
  const fixed = (
    rel: OperatorIdentity,
    minimum: number | ((cap: number) => number),
    owner = rel.id,
    loanSlots = 0,
  ) => admissions.push({ rel, minimum, owner, loanSlots });
  let nextPhysicalId = 0;
  const findIds = (rel: Rel): void => {
    nextPhysicalId = Math.max(nextPhysicalId, rel.id + 1);
    if ("input" in rel) findIds(rel.input);
    if ("left" in rel) findIds(rel.left);
    if ("right" in rel) findIds(rel.right);
  };
  findIds(plan.root);
  const lateSortIds = new Set<number>();
  const ids = new Set<number>();
  // A terminal names-only projection forwards export ownership while retaining profile counts.
  const exportOwners = new WeakMap<Operator, ResultSink>();
  // SQL SELECT * may retain an identity projection for output names. It neither retains
  // source batches nor re-reads them; computed/reordered projections and other consumers stop here.
  let terminalInput = plan.root;
  while (
    terminalInput.op === "project" &&
    terminalInput.exprs.length === terminalInput.input.output.length &&
    terminalInput.exprs.every((expr, k) => expr.op === "column" && expr.index === k)
  )
    terminalInput = terminalInput.input;
  const prepare = (rel: Rel): Recipe => {
    if (ids.has(rel.id))
      throw new UnsupportedPlanError("a repeated relational operator id", { construct: "plan" });
    ids.add(rel.id);
    switch (rel.op) {
      case "indexRange": {
        if (options.keyOrder !== true)
          throw new UnsupportedPlanError(
            "index range requires the diagnostic keyOrder opt-in until the charged streaming cursor lands",
            {
              construct: "indexRange",
            },
          );
        if (rel.lower !== null || rel.upper !== null || rel.emitLocator)
          throw new UnsupportedPlanError("bounded or locator-producing index range", {
            construct: "indexRange",
          });
        const shape: IndexShape = {
          tableId: rel.tableId,
          columns: rel.columns,
          types: types(rel),
          source: { id: nextPhysicalId++, op: "indexRange", profileAs: rel },
          fetch: rel,
          restore: { id: nextPhysicalId++, op: "sort", profileAs: rel },
          descending: rel.descending,
        };
        const placeholders = indexMinimums(shape, 1);
        for (let i = 0; i < placeholders.length; i++) {
          const entry = placeholders[i];
          if (!entry) throw new Error("index minimum");
          fixed(entry.rel, (cap) => indexMinimums(shape, cap)[i]?.minimum ?? 0, rel.id);
        }
        return (runtime, sink, stops, demand) =>
          runIndexOrder(runtime, sink, shape, rel.indexId, null, 0, demand, () =>
            stops.some((stop) => stop()),
          );
      }
      case "values": {
        const setup = (context: CompileContext) =>
          rel.rows.map((row) => row.map((expr) => compileExpr(expr, context)));
        const minimum = measureCompilerBuffers(compilation, [], 1, setup, () => undefined);
        fixed(rel, Math.max(4096, minimum));
        return (runtime, downstream, stops) =>
          instantiate(runtime, rel, [], minimum, 1, setup, (rows) => {
            let at = 0;
            const source: BatchSource = {
              next() {
                if (at === rows.length || stops.some((stop) => stop())) return null;
                const row = rows[at++];
                return {
                  batch: {
                    length: 1,
                    columns: (row ?? []).map((expr) => expr.evaluate(EMPTY, null)),
                    selection: null,
                  },
                };
              },
              abort() {
                at = rows.length;
              },
            };
            runtime.onAbort((reason) => source.abort(reason));
            runtime.pipeline();
            return drive(source, counted(runtime, rel, downstream), runtime.signal);
          });
      }
      case "scan": {
        const input = types(rel).slice(0, rel.columns.length),
          filter = rel.filter;
        // Only the terminal full-table export has no downstream consumer that may re-read.
        // Other scans keep their selective/cache policy; TableScan also handles deletion,
        // reader capability, memory refusal, pacing and cancellation fallbacks itself.
        const terminal =
          rel === terminalInput &&
          filter === null &&
          rel.runtimeFilters.length === 0 &&
          !rel.emitLocator;
        const kernelBytes =
          filter === null
            ? 0
            : measureCompilerBuffers(
                compilation,
                input,
                BATCH_ROWS,
                (context) => new FilterOperator(filter, context, DISCARD),
                (filter) => filter.abort(undefined),
              );
        fixed(rel, (cap) => kernelBytes + batchTargetFor(cap, 4) + BATCH_ROWS * 16);
        return (runtime, downstream, stops, rowsNeeded) => {
          if (stops.some((stop) => stop())) return downstream.finish();
          const target = batchTargetFor(runtime.request.governor.capBytes, 4);
          return instantiate(
            runtime,
            rel,
            input,
            kernelBytes + target + BATCH_ROWS * 16,
            BATCH_ROWS,
            (context, grant) => ({ context, grant }),
            ({ context, grant }) =>
              then(runtime.view.table(rel.tableId, { budget: grant }), (table) => {
                const terminalFull =
                  terminal &&
                  rel.columns.length === table.columns.length &&
                  rel.columns.every((id, k) => id === table.columns[k]?.id);
                const owner = terminalFull
                  ? downstream instanceof ResultSink
                    ? downstream
                    : exportOwners.get(downstream)
                  : undefined;
                const output = owner?.grant;
                const scan = new TableScan({
                  table,
                  pages: reclaimingReader(runtime.view.pages, grant, runtime.signal),
                  reads: runtime.request.reads,
                  budget: grant,
                  columns: rel.columns,
                  filter: rel.filter,
                  emitLocator: rel.emitLocator,
                  context,
                  loans: { borrow: (bytes) => grant.borrow(bytes, runtime.signal) },
                  // Larger batches remain restricted to explicit relinquishing export consumers.
                  targetBytes: runtime.request.outputBuffers
                    ? (owner?.terminalBatchTarget(2 * target) ?? target)
                    : target,
                  readWindowBytes: terminalFull ? 64 * 1024 * 1024 : 0,
                  ...(output === undefined
                    ? {}
                    : {
                        transferOutputs: "dictionary" as const,
                        outputBudget: output,
                        ...(runtime.request.outputBuffers
                          ? { outputBuffers: runtime.request.outputBuffers }
                          : {}),
                        outputDictionary: (dictionary) =>
                          owner?.admitDictionary(dictionary) ?? false,
                        borrowOutputOnRefusal: true,
                      }),
                  ...(rowsNeeded === undefined ? {} : { rowsNeeded }),
                });
                runtime.onAbort(() => scan.abort());
                const source: BatchSource = {
                  next: () => (stops.some((stop) => stop()) ? (scan.abort(), null) : scan.next()),
                  abort: () => scan.abort(),
                };
                runtime.pipeline();
                const result = drive(source, counted(runtime, rel, downstream), runtime.signal);
                return then(result, () => {
                  const profile = runtime.profile(rel);
                  profile.pagesRead = scan.stats.pagesRead;
                  profile.pagesPruned = scan.stats.pagesPruned;
                  scan.close();
                });
              }),
          );
        };
      }
      case "filter":
      case "project":
      case "limit": {
        const child = prepare(rel.input),
          input = types(rel.input);
        const setup = (context: CompileContext, sink: Operator) =>
          rel.op === "filter"
            ? new FilterOperator(rel.predicate, context, sink)
            : rel.op === "project"
              ? new ProjectOperator(rel.exprs, context, sink)
              : new LimitOperator(null, 0, context.arena, sink);
        const minimum = measureCompilerBuffers(
          compilation,
          input,
          BATCH_ROWS,
          (context) => setup(context, DISCARD),
          (operator) => operator.abort(undefined),
        );
        fixed(rel, Math.max(4096, minimum));
        const identity =
          rel.op === "project" &&
          rel.exprs.length === rel.input.output.length &&
          rel.output.length === rel.exprs.length &&
          rel.exprs.every((expr, k) => {
            const before = rel.input.output[k],
              after = rel.output[k];
            return (
              expr.op === "column" &&
              expr.index === k &&
              before !== undefined &&
              after !== undefined &&
              samePhysicalType(before.type, expr.type) &&
              samePhysicalType(expr.type, after.type)
            );
          });
        return (runtime, downstream, stops, rowsNeeded) => {
          const owner =
            runtime.request.outputBuffers && identity
              ? downstream instanceof ResultSink
                ? downstream
                : exportOwners.get(downstream)
              : undefined;
          if (owner) {
            const output = counted(runtime, rel, downstream);
            exportOwners.set(output, owner);
            return child(runtime, output, stops, rowsNeeded);
          }
          return instantiate(
            runtime,
            rel,
            input,
            minimum,
            BATCH_ROWS,
            (context) =>
              rel.op === "limit"
                ? new LimitOperator(
                    runtime.bounds(rel).limit,
                    runtime.bounds(rel).offset,
                    context.arena,
                    counted(runtime, rel, downstream),
                  )
                : setup(context, counted(runtime, rel, downstream)),
            (operator) => {
              runtime.onAbort((reason) => operator.abort(reason));
              const childStops =
                operator instanceof LimitOperator ? [...stops, () => operator.done] : stops;
              return childStops.some((stop) => stop())
                ? operator.finish()
                : child(
                    runtime,
                    operator,
                    childStops,
                    operator instanceof LimitOperator
                      ? () => operator.inputNeeded(rowsNeeded?.() ?? Infinity)
                      : rel.op === "filter"
                        ? undefined
                        : rowsNeeded,
                  );
            },
          );
        };
      }
      case "aggregate":
      case "distinct": {
        const child = prepare(rel.input),
          input = types(rel.input);
        if (rel.op === "distinct" && input.length === 0) {
          // Empty tuples form one class only when at least one arrived. Scalar aggregation
          // would invent its empty-input row. Consume the entire child (no early-stop hint).
          const minimum = 2 * BATCH_ROWS;
          fixed(rel, minimum);
          return (runtime, downstream) =>
            instantiate(
              runtime,
              rel,
              input,
              minimum,
              BATCH_ROWS,
              (context) =>
                new LimitOperator(1, 0, context.arena, counted(runtime, rel, downstream)),
              (operator) => {
                runtime.onAbort((reason) => operator.abort(reason));
                return child(runtime, operator, []);
              },
            );
        }
        const aggregate: Extract<Rel, { op: "aggregate" }> =
          rel.op === "aggregate"
            ? rel
            : {
                ...rel,
                op: "aggregate",
                groupBy: input.map((_, c) => c),
                groupingSets: null,
                aggregates: [],
              };
        if (aggregate.aggregates.some(isOrderedSet)) {
          const measured = measureCompilerBuffers(
            compilation,
            input,
            BATCH_ROWS,
            (context) => orderedSetArguments(aggregate, context),
            () => undefined,
            "aggregate",
          );
          const minimum = orderedSetMinimumBytes(aggregate, input) + measured;
          const loanSlots = aggregate.aggregates.length + 2;
          fixed(rel, minimum, rel.id, loanSlots);
          return (runtime, downstream) =>
            instantiate(
              runtime,
              rel,
              input,
              minimum,
              BATCH_ROWS,
              (context, grant) =>
                new OrderedSetOperator(
                  aggregate,
                  context,
                  {
                    grant,
                    loans: Array.from({ length: loanSlots }, (_, slot) => runtime.loan(rel, slot)),
                    spill: runtime.request.spill,
                    queryId: runtime.request.queryId,
                    signal: runtime.signal,
                  },
                  counted(runtime, rel, downstream),
                ),
              (operator) => {
                runtime.onAbort((reason) => operator.abort(reason));
                runtime.dispose(() => operator.closed);
                return (async () => {
                  try {
                    await child(runtime, operator, []);
                  } catch (error) {
                    operator.abort(error);
                    // Disposal also observes cleanup failures; retain this statement's first error.
                    await operator.closed.catch(() => undefined);
                    throw error;
                  }
                })();
              },
            );
        }
        const minimum = measureCompilerBuffers(
          compilation,
          input,
          BATCH_ROWS,
          (context, grant) => new AggregateOperator(aggregate, context, grant, DISCARD),
          (operator) => operator.abort(undefined),
        );
        fixed(rel, Math.max(4096, minimum));
        return (runtime, downstream) =>
          instantiate(
            runtime,
            rel,
            input,
            minimum,
            BATCH_ROWS,
            (context, grant) =>
              new AggregateOperator(aggregate, context, grant, counted(runtime, rel, downstream), {
                signal: runtime.signal,
              }),
            (operator) => {
              runtime.onAbort((reason) => operator.abort(reason));
              return child(runtime, operator, []);
            },
          );
      }
      case "sort":
      case "window": {
        const late =
          rel.op === "sort" && options.lateSort !== false && plan.parameters.length === 0
            ? lateSortShape(rel)
            : null;
        if (late !== null && rel.op === "sort") return prepareLate(rel, late);
        const child = prepare(rel.input),
          input = types(rel.input);
        const keys =
          rel.op === "sort"
            ? keyProjection(
                input,
                rel.keys.map((key) => key.expr),
              )
            : setupWindow(rel, input, options.windowScalarArguments !== false);
        const physical = windowInputLayout(
          keys.exprs,
          rel.op === "window" && rel.input.op === "project" ? rel.input.exprs : null,
          rel.op === "window" && options.windowInputDeduplication !== false,
        );
        const outputExprs = input.map((type, c): ScalarExpr => ({
          op: "column",
          index: physical.index(c),
          type,
        }));
        if (rel.op === "window")
          for (let k = 0; k < rel.functions.length; k++) {
            const type = rel.functions[k]?.type;
            if (type !== undefined)
              outputExprs.push({ op: "column", index: physical.types.length + k, type });
          }
        const make = (
          context: CompileContext,
          grant: Grant | null,
          downstream: Operator,
          runtime?: QueryRuntime,
        ) => {
          const extended = { ...context, input: keys.types };
          const output = new ProjectOperator(
            outputExprs,
            {
              ...context,
              input: [
                ...physical.types,
                ...(rel.op === "window" ? rel.functions.map((call) => call.type) : []),
              ],
            },
            downstream,
          );
          const windowSpec =
            rel.op === "window"
              ? {
                  types: physical.types,
                  partitionBy: rel.partitionBy.map(physical.index),
                  orderBy: (keys as ReturnType<typeof setupWindow>).orderBy.map((key) => ({
                    ...key,
                    column: physical.index(key.column),
                  })),
                  functions: (keys as ReturnType<typeof setupWindow>)
                    .functions(extended, runtime === undefined)
                    .map((call) => remapWindowFunction(call, physical.index)),
                }
              : null;
          if (runtime === undefined || grant === null) {
            if (windowSpec === null)
              sortMode(keys.types, order(rel.op === "sort" ? rel.keys : [], keys.indexes));
            else {
              validateWindowSpec(windowSpec);
              sortMode(physical.types, [
                ...windowSpec.partitionBy.map((column) => ({
                  column,
                  direction: "asc" as const,
                  nulls: "last" as const,
                  collation: null,
                })),
                ...windowSpec.orderBy,
              ]);
            }
            return {
              operator: new ProjectOperator(physical.exprs, context, DISCARD),
              breaker: null,
            };
          }
          const shared = {
            grant,
            spill: runtime.request.spill,
            queryId: runtime.request.queryId,
            signal: runtime.signal,
          };
          let breaker: SortOperator | WindowOperator;
          if (rel.op === "sort")
            breaker = new SortOperator(
              {
                types: keys.types,
                keys: order(rel.keys, keys.indexes),
                limit: runtime.bounds(rel).limit,
                offset: runtime.bounds(rel).offset,
                withTies: rel.withTies,
              },
              shared,
              output,
            );
          else {
            if (windowSpec === null) throw new TypeError("missing window specification");
            breaker = new WindowOperator(windowSpec, shared, output);
          }
          return { operator: new ProjectOperator(physical.exprs, context, breaker), breaker };
        };
        const measured = measureCompilerBuffers(
          compilation,
          input,
          BATCH_ROWS,
          (context) => make(context, null, DISCARD),
          (value) => value.operator.abort(undefined),
          rel.op,
        );
        const base =
          rel.op === "sort"
            ? sortMinimumBytes(keys.types)
            : windowMinimumBytes(physical.types, rel.functions.length);
        // The operator helper already includes its constructor's fixed buffers. Add only
        // this recipe's kernel arena; adding all measured bytes would count pools twice.
        const minimum = base + measured;
        fixed(rel, minimum);
        return (runtime, downstream) =>
          rel.op === "sort" && runtime.bounds(rel).limit === 0
            ? downstream.finish()
            : instantiate(
                runtime,
                rel,
                input,
                minimum,
                BATCH_ROWS,
                (context, grant) =>
                  make(context, grant, counted(runtime, rel, downstream), runtime),
                ({ operator, breaker }) => {
                  if (breaker === null) throw new TypeError("missing execution breaker");
                  runtime.onAbort((reason) => operator.abort(reason));
                  runtime.dispose(() => breaker.closed);
                  return then(child(runtime, operator, []), () => {
                    const p = breaker.profile,
                      out = runtime.profile(rel);
                    out.spilledBytes = p.spilledBytes + ("sort" in p ? p.sort.spilledBytes : 0);
                    out.spillPasses = "mergePasses" in p ? p.mergePasses : p.sort.mergePasses;
                  });
                },
              );
      }
      case "join": {
        if (
          rel.algorithm === "nestedLoop" &&
          (rel.leftKeys.length !== 0 || rel.rightKeys.length !== 0)
        )
          throw new UnsupportedPlanError("a keyed nested-loop physical join", {
            construct: "join",
          });
        const left = prepare(rel.left),
          right = prepare(rel.right),
          lt = types(rel.left),
          rt = types(rel.right);
        const lk = keyProjection(lt, rel.leftKeys),
          rk = keyProjection(rt, rel.rightKeys);
        const predicateTypes = [...lt, ...rt];
        const make = (
          context: CompileContext,
          grant: Grant | null,
          downstream: Operator,
          runtime?: QueryRuntime,
        ) => {
          const predicateContext = { ...context, input: predicateTypes };
          const spec: HashJoinSpec = {
            kind: rel.kind === "cross" ? "inner" : rel.kind,
            probeTypes: lk.types,
            buildTypes: rk.types,
            probeKeys: lk.indexes,
            buildKeys: rk.indexes,
            nullSafe: rel.nullSafe,
            ...(rel.correlated === undefined ? {} : { correlated: rel.correlated }),
            residual: condition(
              rel.residual,
              predicateContext,
              lt.length,
              lk.types.length,
              rt.length,
            ),
            filter: condition(
              rel.filter ?? null,
              predicateContext,
              lt.length,
              lk.types.length,
              rt.length,
            ),
            // Runtime filters are optional acceleration; scans still evaluate every predicate.
            publishes: [],
          };
          if (runtime === undefined || grant === null) validateHashJoinSpec(spec);
          const join =
            runtime === undefined || grant === null
              ? null
              : new HashJoin(spec, {
                  grant,
                  spill: runtime.request.spill,
                  queryId: runtime.request.queryId,
                  relId: rel.id,
                  signal: runtime.signal,
                });
          const output = columns(lk.types).slice(0, lt.length);
          if (rel.kind === "mark")
            output.push({ op: "column", index: lk.types.length, type: { kind: "bool" } });
          else if (rel.kind !== "semi" && rel.kind !== "anti")
            for (let c = 0; c < rt.length; c++)
              output.push({
                op: "column",
                index: lk.types.length + c,
                type: at(rt, c),
              });
          const joinTypes =
            rel.kind === "mark"
              ? [...lk.types, { kind: "bool" } as const]
              : rel.kind === "semi" || rel.kind === "anti"
                ? lk.types
                : [...lk.types, ...rk.types];
          const probe = new ProjectOperator(
            lk.exprs,
            { ...context, input: lt },
            join === null
              ? (new ProjectOperator(output, { ...context, input: joinTypes }, downstream), DISCARD)
              : join.probe(
                  new ProjectOperator(output, { ...context, input: joinTypes }, downstream),
                ),
          );
          const build = new ProjectOperator(
            rk.exprs,
            { ...context, input: rt },
            join?.build ?? DISCARD,
          );
          return { join, build, probe, spec };
        };
        let base = 0;
        const measured = measureCompilerBuffers(
          compilation,
          lt,
          BATCH_ROWS,
          (context) => {
            const value = make(context, null, DISCARD);
            base = hashJoinMinimumBytes(value.spec);
            return value;
          },
          (value) => {
            value.probe.abort(undefined);
            value.build.abort(undefined);
          },
        );
        // The operator helper already includes its constructor's fixed buffers. Add only
        // this recipe's kernel arena; adding all measured bytes would count pools twice.
        const minimum = base + measured;
        fixed(rel, minimum);
        return (runtime, downstream, stops) =>
          instantiate(
            runtime,
            rel,
            lt,
            minimum,
            BATCH_ROWS,
            (context, grant) => make(context, grant, counted(runtime, rel, downstream), runtime),
            ({ join, build, probe }) => {
              if (join === null) throw new TypeError("missing execution join");
              runtime.onAbort((reason) => join.abort(reason));
              runtime.dispose(() => join.closed);
              return then(right(runtime, build, []), () =>
                then(left(runtime, probe, stops), () => {
                  const p = join.profile,
                    out = runtime.profile(rel);
                  out.spilledBytes = p.spilledBytes;
                  out.spillPasses = p.spillPasses;
                }),
              );
            },
          );
      }
      default:
        throw new UnsupportedPlanError(`the ${rel.op} operator`, { construct: rel.op });
    }
  };
  function prepareLate(rel: Extract<Rel, { op: "sort" }>, shape: LateSortShape): Recipe {
    lateSortIds.add(rel.id);
    const { project, scan, narrow, keys, locator } = shape;
    if (ids.has(project.id))
      throw new UnsupportedPlanError("a repeated relational operator id", { construct: "plan" });
    ids.add(project.id);
    const child = prepare(narrow),
      input = types(narrow),
      payload = types(project);
    sortMode(input, keys);
    const restoreTypes = [...payload, ORDINAL_TYPE];
    sortMode(restoreTypes, [
      { column: payload.length, direction: "asc", nulls: "last", collation: null },
    ]);
    const restoreIdentity: OperatorIdentity = { id: nextPhysicalId++, op: "sort", profileAs: rel };
    const restoreMinimum = sortMinimumBytes(restoreTypes),
      sortMinimum = sortMinimumBytes(input);
    const fetchMinimum = (cap: number) =>
      batchTargetFor(cap, 4) +
      memoryScanBufferBytes(payload) +
      BATCH_ROWS * (80 + 8 * OBJECT_OVERHEAD_BYTES) +
      OBJECT_OVERHEAD_BYTES * (16 + 2 * payload.length);
    fixed(project, fetchMinimum);
    fixed(restoreIdentity, restoreMinimum, rel.id);
    fixed(rel, sortMinimum);
    return (runtime, downstream) =>
      runtime.bounds(rel).limit === 0
        ? downstream.finish()
        : instantiate(
            runtime,
            restoreIdentity,
            restoreTypes,
            restoreMinimum,
            BATCH_ROWS,
            (_context, grant) => grant,
            (restoreGrant) =>
              instantiate(
                runtime,
                project,
                input,
                fetchMinimum(runtime.request.governor.capBytes),
                BATCH_ROWS,
                (context, grant) =>
                  new LateFetchOperator(
                    {
                      onePass: options.lateFetchOnce !== false,
                      snapshot: runtime.view,
                      tableId: scan.tableId,
                      columns: scan.columns,
                      types: payload,
                      locator,
                      compile: context,
                      grant,
                      restoreGrant,
                      reads: runtime.request.reads,
                      spill: runtime.request.spill,
                      queryId: runtime.request.queryId,
                      targetBytes: batchTargetFor(runtime.request.governor.capBytes, 4),
                      signal: runtime.signal,
                      onPipeline: () => runtime.pipeline(),
                    },
                    counted(runtime, project, downstream),
                  ),
                (fetch) => {
                  runtime.onAbort((reason) => fetch.abort(reason));
                  runtime.dispose(() => fetch.closed);
                  return instantiate(
                    runtime,
                    rel,
                    input,
                    sortMinimum,
                    BATCH_ROWS,
                    (_context, grant) =>
                      new SortOperator(
                        {
                          types: input,
                          keys,
                          limit: runtime.bounds(rel).limit,
                          offset: runtime.bounds(rel).offset,
                          withTies: false,
                        },
                        {
                          grant,
                          spill: runtime.request.spill,
                          queryId: runtime.request.queryId,
                          signal: runtime.signal,
                        },
                        counted(runtime, rel, fetch),
                      ),
                    (sort) => {
                      runtime.onAbort((reason) => sort.abort(reason));
                      runtime.dispose(() => sort.closed);
                      return then(child(runtime, sort, []), () => {
                        const profile = runtime.profile(rel);
                        profile.spilledBytes = sort.profile.spilledBytes + fetch.spilledBytes;
                        profile.spillPasses = sort.profile.mergePasses + fetch.spillPasses;
                        runtime.profile(project).pagesRead = fetch.pagesRead;
                        profile.rowsOut = fetch.rowsOut;
                        profile.batchesOut = fetch.batchesOut;
                      });
                    },
                  );
                },
              ),
          );
  }
  const recipe = prepare(plan.root);
  // Main's cursor still collects the whole range before its first result. Keep this SQL
  // strategy opt-in until B3's streaming cursor passes the first-page CPU acceptance rule.
  // Explicit indexRange and SQL both require the diagnostic opt-in above.
  const ordered = options.keyOrder === true ? orderedSort(plan) : null;
  // Extra helper buffers must not turn a formerly admissible small-cap query into a refusal.
  // Both strategies are capability-checked at prepare; only their execution resources differ.
  const fallback =
    lateSortIds.size === 0 ? null : compilePlan(plan, { ...options, lateSort: false });
  return (runtime, sink) => {
    runtime.resolveCall = resolveCall;
    if (ordered !== null) {
      const { rel, scan, project } = ordered;
      const table = runtime.view.catalog.table(scan.tableId);
      const index = table?.indexes.find((i) => i.id === table.primaryKey);
      if (
        index?.kind === "ordered" &&
        index.unique &&
        index.columnIds.length === rel.keys.length &&
        ordered.keys.every((column, i) => column === index.columnIds[i])
      ) {
        const bounds = runtime.bounds(rel);
        if (bounds.limit === 0) return then(runtime.reserve([]), () => sink.finish());
        const shape: IndexShape = {
          tableId: scan.tableId,
          columns: ordered.columns,
          types: types(project),
          source: scan,
          fetch: project,
          restore: rel,
          descending: rel.keys[0]?.direction === "desc",
        };
        const entries = indexMinimums(shape, runtime.request.governor.capBytes);
        const inputs = integerParameterBytes(plan, runtime.request.parameters);
        if (
          inputs !== null &&
          inputs + entries.reduce((n, e) => n + e.minimum, 0) <=
            runtime.request.governor.capBytes - runtime.request.governor.reserveBytes
        )
          return then(runtime.reserve(entries), () =>
            then(runIndexOrder(runtime, sink, shape, index.id, bounds.limit, bounds.offset), () => {
              for (const projection of ordered.projects) {
                if (projection.id === project.id) continue;
                const profile = runtime.profile(projection),
                  output = runtime.profile(project);
                profile.rowsOut = output.rowsOut;
                profile.batchesOut = output.batchesOut;
              }
            }),
          );
      }
    }
    // LIMIT zero never opens its input. Keep capability checks at prepare, but do not reserve
    // buffers for those unreachable pipelines (including when zero is a bound parameter).
    const active = new Set<number>();
    const visit = (rel: Rel): void => {
      // Lowering stores ORDER BY's LIMIT on its sort. Zero must not open even its pools.
      if (rel.op === "sort" && runtime.bounds(rel).limit === 0) return;
      active.add(rel.id);
      if (rel.op === "limit" && runtime.bounds(rel).limit === 0) return;
      if (rel.op === "join") {
        visit(rel.left);
        visit(rel.right);
      } else if ("input" in rel) visit(rel.input);
    };
    visit(plan.root);
    const entries = admissions
      .filter((entry) => active.has(entry.owner))
      .map((entry) => ({
        rel: entry.rel,
        loanSlots: entry.loanSlots,
        minimum:
          typeof entry.minimum === "number"
            ? entry.minimum
            : entry.minimum(runtime.request.governor.capBytes),
      }));
    if (
      fallback !== null &&
      entries.reduce((sum, entry) => sum + entry.minimum, 0) >
        runtime.request.governor.capBytes - runtime.request.governor.reserveBytes
    )
      return fallback(runtime, sink);
    return then(runtime.reserve(entries), () => recipe(runtime, sink, []));
  };
}
