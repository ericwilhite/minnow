/** Immutable pipeline recipes; every execution instantiates its own operators and kernels. */
import type { MaybePromise } from "../access/key-index.js";
import type { Bound, PhysicalPlan, Rel, ScalarExpr, SortKey } from "../ir/ir.js";
import { batchTargetFor, type Grant } from "../memory/governor.js";
import type { ByteCharge } from "../vector/varlen.js";
import type { PhysicalType } from "../types.js";
import { BATCH_ROWS, type Batch, type Selection, type Vector } from "../vector/vector.js";
import { int64At } from "../vector/int64.js";
import { AggregateOperator } from "./agg/aggregate.js";
import { ExecutionError, UnsupportedPlanError } from "./errors.js";
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
import { nullParameter } from "./parameters.js";
import { drive, isPromise, type BatchSource, type Operator } from "./pipeline.js";
import type { QueryRuntime, Program } from "./run.js";
import { SortOperator, sortMinimumBytes } from "./sort.js";
import { sortMode } from "./sort-keys.js";
import type { SortColumn } from "./sort-keys.js";
import { TableScan } from "./table-scan.js";
import {
  WindowOperator,
  windowMinimumBytes,
  validateWindowSpec,
  type WindowFunctionSpec,
} from "./window.js";

type Stop = () => boolean;
type Recipe = (runtime: QueryRuntime, sink: Operator, stops: readonly Stop[]) => MaybePromise<void>;
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
  rel: Rel,
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

function counted(runtime: QueryRuntime, rel: Rel, sink: Operator): Operator {
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

function bound(value: Bound | null, runtime: QueryRuntime, offset: boolean): number | null {
  if (value === null) return offset ? 0 : null;
  let number: number;
  if (value.kind === "const") number = value.value;
  else {
    const vector = runtime.params.slots[value.slot];
    if (vector === undefined) throw new TypeError("LIMIT/OFFSET slot is missing");
    if (!isValidSlot(vector, 0)) return offset ? 0 : null;
    const data = vector.data;
    if (data.layout === "int64") {
      const integer = int64At(data.values, 0);
      number =
        integer > BigInt(Number.MAX_SAFE_INTEGER) ? Number.MAX_SAFE_INTEGER : Number(integer);
    } else if (data.layout === "i32") number = data.values[0] ?? 0;
    else throw new TypeError("LIMIT/OFFSET is not an integer");
  }
  if (number < 0)
    throw new ExecutionError(
      `${offset ? "OFFSET" : "LIMIT"} must not be negative`,
      "MINNOW_INVALID_BOUND",
      offset ? "2201X" : "2201W",
    );
  if (!Number.isSafeInteger(number)) throw new TypeError("invalid LIMIT/OFFSET constant");
  return number;
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

function setupWindow(rel: Extract<Rel, { op: "window" }>, input: readonly PhysicalType[]) {
  const exprs: ScalarExpr[] = rel.orderBy.map((key) => key.expr);
  const positions = rel.functions.map((call) =>
    call.args.map((expr) => {
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
        ...(amountAt === undefined ? {} : { amount: { column: at(keys.indexes, amountAt) } }),
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
export function compilePlan(plan: PhysicalPlan): Program {
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
  const admissions: Array<{ rel: Rel; minimum: number | ((cap: number) => number) }> = [];
  const fixed = (rel: Rel, minimum: number | ((cap: number) => number)) =>
    admissions.push({ rel, minimum });
  const ids = new Set<number>();
  const prepare = (rel: Rel): Recipe => {
    if (ids.has(rel.id))
      throw new UnsupportedPlanError("a repeated relational operator id", { construct: "plan" });
    ids.add(rel.id);
    switch (rel.op) {
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
        return (runtime, downstream, stops) => {
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
                const scan = new TableScan({
                  table,
                  pages: runtime.view.pages,
                  reads: runtime.request.reads,
                  budget: grant,
                  columns: rel.columns,
                  filter: rel.filter,
                  emitLocator: rel.emitLocator,
                  context,
                  loans: { borrow: (bytes) => grant.borrow(bytes, runtime.signal) },
                  targetBytes: target,
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
        return (runtime, downstream, stops) =>
          instantiate(
            runtime,
            rel,
            input,
            minimum,
            BATCH_ROWS,
            (context) =>
              rel.op === "limit"
                ? new LimitOperator(
                    bound(rel.limit, runtime, false),
                    bound(rel.offset, runtime, true) ?? 0,
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
                : child(runtime, operator, childStops);
            },
          );
      }
      case "aggregate":
      case "distinct": {
        const child = prepare(rel.input),
          input = types(rel.input);
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
        const child = prepare(rel.input),
          input = types(rel.input);
        const keys =
          rel.op === "sort"
            ? keyProjection(
                input,
                rel.keys.map((key) => key.expr),
              )
            : setupWindow(rel, input);
        const outputExprs = columns(keys.types).slice(0, input.length);
        if (rel.op === "window")
          for (let k = 0; k < rel.functions.length; k++) {
            const type = rel.functions[k]?.type;
            if (type !== undefined)
              outputExprs.push({ op: "column", index: keys.types.length + k, type });
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
                ...keys.types,
                ...(rel.op === "window" ? rel.functions.map((call) => call.type) : []),
              ],
            },
            downstream,
          );
          const windowSpec =
            rel.op === "window"
              ? {
                  types: keys.types,
                  partitionBy: rel.partitionBy,
                  orderBy: (keys as ReturnType<typeof setupWindow>).orderBy,
                  functions: (keys as ReturnType<typeof setupWindow>).functions(
                    extended,
                    runtime === undefined,
                  ),
                }
              : null;
          if (runtime === undefined || grant === null) {
            if (windowSpec === null)
              sortMode(keys.types, order(rel.op === "sort" ? rel.keys : [], keys.indexes));
            else {
              validateWindowSpec(windowSpec);
              sortMode(keys.types, [
                ...windowSpec.partitionBy.map((column) => ({
                  column,
                  direction: "asc" as const,
                  nulls: "last" as const,
                  collation: null,
                })),
                ...windowSpec.orderBy,
              ]);
            }
            return { operator: new ProjectOperator(keys.exprs, context, DISCARD), breaker: null };
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
                limit: bound(rel.limit, runtime, false),
                offset: bound(rel.offset, runtime, true) ?? 0,
                withTies: rel.withTies,
              },
              shared,
              output,
            );
          else {
            if (windowSpec === null) throw new TypeError("missing window specification");
            breaker = new WindowOperator(windowSpec, shared, output);
          }
          return { operator: new ProjectOperator(keys.exprs, context, breaker), breaker };
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
            : windowMinimumBytes(keys.types, rel.functions.length);
        // The operator helper already includes its constructor's fixed buffers. Add only
        // this recipe's kernel arena; adding all measured bytes would count pools twice.
        const minimum = base + measured;
        fixed(rel, minimum);
        return (runtime, downstream) =>
          rel.op === "sort" && bound(rel.limit, runtime, false) === 0
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
  const recipe = prepare(plan.root);
  return (runtime, sink) => {
    runtime.resolveCall = resolveCall;
    // LIMIT zero never opens its input. Keep capability checks at prepare, but do not reserve
    // buffers for those unreachable pipelines (including when zero is a bound parameter).
    const active = new Set<number>();
    const visit = (rel: Rel): void => {
      // Lowering stores ORDER BY's LIMIT on its sort. Zero must not open even its pools.
      if (rel.op === "sort" && bound(rel.limit, runtime, false) === 0) return;
      active.add(rel.id);
      if (rel.op === "limit" && bound(rel.limit, runtime, false) === 0) return;
      if (rel.op === "join") {
        visit(rel.left);
        visit(rel.right);
      } else if ("input" in rel) visit(rel.input);
    };
    visit(plan.root);
    return then(
      runtime.reserve(
        admissions
          .filter((entry) => active.has(entry.rel.id))
          .map((entry) => ({
            rel: entry.rel,
            minimum:
              typeof entry.minimum === "number"
                ? entry.minimum
                : entry.minimum(runtime.request.governor.capBytes),
          })),
      ),
      () => recipe(runtime, sink, []),
    );
  };
}
