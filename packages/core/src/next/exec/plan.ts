import {
  RecursiveOperator,
  recursiveLayout,
  validateRecursive,
  compileRecursiveStep,
} from "./recursive.js";
import { SeriesOperator } from "./series.js";
import { rankingBounds } from "./window-topk-plan.js";
import { Slice, runSliced, type SlicedJob } from "./slices.js";
/** Immutable pipeline recipes; every execution instantiates its own operators and kernels. */
import type { MaybePromise } from "../access/key-index.js";
import type { PhysicalPlan, Rel, ScalarExpr, SortKey } from "../ir/ir.js";
import { ResultSink } from "./result-sink.js";
import {
  ExceptOperator,
  exceptKeyLayout,
  exceptMinimumBytes,
  type ExceptCheckpoint,
} from "./except.js";
import { readValue, boundSource, type ReadSource, type ReadBound } from "../page/read-bounds.js";
import { batchTargetFor, type Grant } from "../memory/governor.js";
import { jsonLiteralBytes, jsonInputsBounded, jsonRetainingParents } from "./json-plan.js";
import { JsonExpressions } from "./json-expressions.js";
import {
  JsonPhaseOperator,
  closeJsonPhase,
  JSON_PHASE_IO,
  JSON_PHASE_FIXED,
  type JsonPhase,
} from "./json-phase.js";
import { jsonExpression, expandingJsonExpression } from "../ir/json-expression.js";
import type { ByteCharge } from "../vector/varlen.js";
import { samePhysicalType, type PhysicalType } from "../types.js";
import { BATCH_ROWS, type Batch, type Selection, type Vector } from "../vector/vector.js";
import { int64At } from "../vector/int64.js";
import { HybridAggregateOperator, closeHybridPhase, type HybridPhase } from "./agg/hybrid.js";
import { RestoredAggregateOperator, restoredMinimum, restoredLoans } from "./agg/restored.js";
import { LARGE_VALUE_BYTES } from "../page/large-value.js";
import { acquirePhasePool } from "./phase-pool.js";
import { AggregateOperator, type AggregateOptions } from "./agg/aggregate.js";
import { DomainCountSumOperator, supportsDomainCountSum } from "./agg/domain-count-sum.js";
import { groupTopNShape, type GroupTopNSpec } from "./agg/group-top-n.js";
import {
  ExternalAggregateOperator,
  externalAggregateMinimum,
  externalAggregateLoans,
} from "./agg/external.js";
import { scalarHashMinimum, dictionaryHashShape, dictionaryHashMinimum } from "./agg/hash-bound.js";
import { externalCallArguments } from "./agg/external-call.js";
import { probeDictionaryGroups } from "./agg/dictionary-probe.js";
import {
  PercentileOperator,
  supportsPercentile,
  percentileMinimum,
  type PercentileBound,
} from "./agg/percentile.js";
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
import {
  orderedSort,
  indexMinimums,
  admittedIndexMinimums,
  planIndexRange,
  runIndexOrder,
  type IndexShape,
  type AdmittedIndexRead,
} from "./key-order.js";
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
  bounded = false,
): MaybePromise<void> {
  return then(
    runtime.admit(`${rel.op}#${String(rel.id)}`, Math.max(4096, minimum), true, rel),
    (grant) => {
      if (bounded) {
        const underlying = grant;
        // This complete workspace was admitted atomically. No hash resize may silently
        // depend on shared headroom; a bad certificate is an accounting defect.
        grant = new Proxy(underlying, {
          get(target, key) {
            if (key === "allocate")
              return (bytes: number) =>
                target.allocated + bytes <= minimum && target.allocate(bytes);
            const value: unknown = Reflect.get(target, key, target);
            return typeof value === "function"
              ? (value as (...args: unknown[]) => unknown).bind(target)
              : value;
          },
        });
      }
      const resources = new Resources(grant, capacity);
      runtime.dispose(() => resources.close());
      // Fixed buffers remain charged. Only unused holdings can be surrendered to another admission.
      grant.onReclaimRequest(() => {
        const free = grant.held - grant.allocated - grant.loaned;
        if (free > 0) grant.release(free);
      });
      return runtime.resource(
        () => {
          const value = setup(
            {
              arena: resources.arena,
              params: runtime.params,
              input,
              ...(runtime.resolveCall === undefined ? {} : { resolveCall: runtime.resolveCall }),
            },
            grant,
          );
          return use(value);
        },
        () => resources.close(),
      );
    },
  );
}

function counted(runtime: QueryRuntime, rel: OperatorIdentity, sink: Operator): Operator {
  const profile = runtime.profile(rel);
  return {
    get reclassifiesTransfers() {
      return sink.reclassifiesTransfers === true;
    },
    push(batch, transfer) {
      runtime.readBoundary();
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
/** Follow only row-preserving direct key projections; filters/joins/computed keys keep hashing. */
function domainSource(rel: Extract<Rel, { op: "aggregate" }>) {
  const group = rel.groupBy[0];
  if (group === undefined) return null;
  let index: number = group;
  let source = rel.input;
  while (source.op === "project") {
    const key: ScalarExpr | undefined = source.exprs[index];
    if (key?.op !== "column") return null;
    index = key.index;
    source = source.input;
  }
  if (
    source.op !== "scan" ||
    source.filter !== null ||
    source.runtimeFilters.length > 0 ||
    source.emitLocator
  )
    return null;
  const column = source.columns[index];
  return column === undefined ? null : { scan: source, column };
}

export interface CompileOptions {
  /** Diagnostic control for bounded sole rank/row_number windows. */
  readonly windowTopK?: boolean;
  /** Parent control for the resident full numeric columnar sort. */
  readonly columnarSort?: boolean;
  readonly sortOwnership?: boolean;
  readonly sortDictionaries?: boolean;
  readonly sortCompression?: boolean;
  /** Keep the external ordered-set recipe for diagnostics. */
  readonly groupedPercentiles?: boolean;
  readonly lateSort?: boolean;
  /** False retains generic SQL sorting; true also permits legacy diagnostic cursors/IR. */
  readonly keyOrder?: boolean;
  readonly lateFetchOnce?: boolean;
  readonly groupTopN?: boolean;
  readonly groupedDistinctBitmap?: boolean;
  readonly scalarByteDistinct?: boolean;
  readonly groupedUpdateFusion?: boolean;
  /** False retains independent AVG states as a benchmark control. */
  readonly packedAverages?: boolean;
  /** False retains independent integer states as a benchmark control. */
  readonly packedIntegers?: boolean;
  readonly typedLayout?: boolean;
  /** False retains per-row dictionary MIN/MAX updates as a benchmark control. */
  readonly batchDictionaryMinMax?: boolean;
  /** False retains the general hash recipe as a benchmark control. */
  readonly domainCountSum?: boolean;
  /** Parent control for the aggregation memory-progress benchmark only. */
  readonly aggregationProgress?: boolean;
  /** False retains the original window inputs for component/query benchmark controls. */
  readonly windowInputDeduplication?: boolean;
  /** Parent control for literal-amount gather benchmarks. */
  readonly windowScalarArguments?: boolean;
}
export function compilePlan(plan: PhysicalPlan, options: CompileOptions = {}): Program {
  const windowBounds =
    options.windowTopK === false || plan.volatile
      ? new Map<number, number>()
      : rankingBounds(plan.root);
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
    if (rel.op === "recursive") {
      findIds(rel.base);
      findIds(rel.step);
    }
    if ("input" in rel) findIds(rel.input);
    if ("left" in rel) findIds(rel.left);
    if ("right" in rel) findIds(rel.right);
    if ("inputs" in rel) for (const input of rel.inputs) findIds(input);
  };
  findIds(plan.root);
  const readSources = new Map<number, ReadSource>();
  const jsonWorkspaces = new Map<number, number>();
  const jsonWidths = new WeakMap<QueryRuntime, number>();

  const lateSortIds = new Set<number>();
  const ids = new Set<number>();
  // A terminal names-only projection forwards export ownership while retaining profile counts.
  const exportOwners = new WeakMap<Operator, ResultSink>();
  const groupTops = new Map<number, GroupTopNSpec>();
  let domainPlans = 0;
  interface AggregateStrategy {
    readonly kind: "hash" | "domain" | "external" | "hybrid" | "restore";
    readonly minimum: number;
    readonly loans: number;
  }
  const aggregateChoices = new WeakMap<QueryRuntime, Map<number, AggregateStrategy>>();
  const percentileChoices = new WeakMap<
    QueryRuntime,
    Map<number, { bound: PercentileBound; minimum: number }>
  >();
  const percentilePlanners = new Map<
    number,
    {
      rel: Extract<Rel, { op: "aggregate" }>;
      input: readonly PhysicalType[];
      compiler: number;
      shape: NonNullable<ReturnType<typeof dictionaryHashShape>>;
    }
  >();
  const hybridIo = 2 << 20;
  type Cut =
    | { readonly kind: "aggregate"; readonly rel: number; readonly phase: HybridPhase }
    | { readonly kind: "json"; readonly rel: number; readonly phase: JsonPhase };
  const closePhase = (phase: HybridPhase | JsonPhase): Promise<void> =>
    "checkpoint" in phase ? closeHybridPhase(phase) : closeJsonPhase(phase);
  const jsonPlanners = new Map<number, number>();
  const jsonCapacities = new WeakMap<QueryRuntime, Map<number, number>>();
  const jsonFrontier = new WeakMap<QueryRuntime, number | null>();
  const jsonDescendants = new Set<number>();
  const findJson = (rel: Rel): boolean => {
    let found =
      (rel.op === "project" && rel.exprs.some(expandingJsonExpression)) ||
      (rel.op === "filter" && expandingJsonExpression(rel.predicate));
    if ("input" in rel) found = findJson(rel.input) || found;
    if ("left" in rel && "right" in rel) {
      const left = findJson(rel.left),
        right = findJson(rel.right);
      found = left || right || found;
    }
    if ("inputs" in rel) for (const input of rel.inputs) found = findJson(input) || found;
    if (found) jsonDescendants.add(rel.id);
    return found;
  };
  findJson(plan.root);
  // Ordinary plans need no per-relation JSON retention metadata.
  const { supported: jsonParents, excepts: jsonExceptParents } =
    jsonDescendants.size > 0
      ? jsonRetainingParents(plan.root)
      : { supported: new Set<number>(), excepts: new Map<number, ReadonlySet<number>>() };
  interface Phases {
    resume: Cut | null;
    pending: Cut | null;
    readonly excepts: Map<number, ExceptOperator>;
    readonly retained: Map<number, ExceptCheckpoint>;
  }
  const phases = new WeakMap<QueryRuntime, Phases>();
  // Unary ancestors are empty until their aggregate child emits. EXCEPT checkpoints
  // exact counted left/right records before retirement; joins still need their own seam.
  const phaseCapable = new Set<number>();
  const markPhases = (rel: Rel, unary: boolean): void => {
    if (unary && (rel.op === "aggregate" || rel.op === "distinct")) phaseCapable.add(rel.id);
    if ("input" in rel) markPhases(rel.input, unary);
    if ("left" in rel) markPhases(rel.left, false);
    if ("right" in rel) markPhases(rel.right, false);
    if ("inputs" in rel)
      for (const input of rel.inputs) markPhases(input, unary && rel.kind === "except");
  };
  markPhases(plan.root, true);
  const aggregatePlanners = new Map<
    number,
    {
      readonly rel: Extract<Rel, { op: "aggregate" }>;
      readonly input: readonly PhysicalType[];
      readonly compiler: number;
      readonly output: AggregateOptions;
      readonly external: AggregateStrategy;
      readonly hybrid: AggregateStrategy | null;
      readonly fixed: boolean;
      readonly hashMinimum: number;
      readonly restore: AggregateStrategy;
      readonly scalar: number | null;
      readonly dictionary: ReturnType<typeof dictionaryHashShape>;
      readonly domain: ReturnType<typeof domainSource>;
    }
  >();
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
            "index range requires the diagnostic keyOrder opt-in; the cursor and fetch lack query-minimum integration",
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
      case "recursive": {
        validateRecursive(rel);
        const input = types(rel),
          layout = recursiveLayout(input),
          base = prepare(rel.base);
        const stepMinimum = measureCompilerBuffers(
          compilation,
          input,
          BATCH_ROWS,
          (context) => compileRecursiveStep(rel.step, context, DISCARD),
          (operator) => operator.abort(undefined),
        );
        const minimum = stepMinimum + layout.metadata + layout.pool;
        fixed(rel, minimum);
        return (runtime, downstream, stops, rowsNeeded) =>
          instantiate(
            runtime,
            rel,
            input,
            minimum,
            BATCH_ROWS,
            (context, grant) =>
              new RecursiveOperator(
                rel,
                context,
                {
                  grant,
                  spill: runtime.request.spill,
                  queryId: runtime.request.queryId,
                  signal: runtime.signal,
                  stopped: () => stops.some((stop) => stop()),
                  ...(rowsNeeded === undefined ? {} : { rowsNeeded }),
                },
                counted(runtime, rel, downstream),
                (node, sink) => counted(runtime, node, sink),
              ),
            (operator) => {
              runtime.onAbort((reason) => operator.abort(reason));
              runtime.dispose(() => operator.closed);
              return (async () => {
                try {
                  await base(runtime, operator.base, stops, rowsNeeded);
                  const profile = runtime.profile(rel);
                  profile.spilledBytes = operator.profile.spilledBytes;
                  profile.spillPasses = operator.profile.spillPasses;
                } catch (error) {
                  operator.abort(error);
                  await operator.closed.catch(() => undefined);
                  throw error;
                }
              })();
            },
          );
      }
      case "series": {
        const input = types(rel.input),
          child = prepare(rel.input);
        const setup = (
          context: CompileContext,
          sink: Operator,
          options: ConstructorParameters<typeof SeriesOperator>[4] = {},
        ) => new SeriesOperator(rel.kind, input, context.arena, sink, options);
        const minimum = measureCompilerBuffers(
          compilation,
          [],
          BATCH_ROWS,
          (context) => setup(context, DISCARD),
          (operator) => operator.abort(undefined),
        );
        fixed(rel, Math.max(4096, minimum));
        return (runtime, downstream, stops, rowsNeeded) =>
          instantiate(
            runtime,
            rel,
            [],
            minimum,
            BATCH_ROWS,
            (context) =>
              setup(context, counted(runtime, rel, downstream), {
                signal: runtime.signal,
                stopped: () => stops.some((stop) => stop()),
                ...(rowsNeeded === undefined ? {} : { rowsNeeded }),
              }),
            (operator) => {
              runtime.onAbort((reason) => operator.abort(reason));
              runtime.dispose(() => operator.closed);
              return then(child(runtime, operator, stops), () => undefined);
            },
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
        readSources.set(rel.id, {
          id: rel.id,
          tableId: rel.tableId,
          columns: rel.columns,
          types: input,
          stagingBytes: memoryScanBufferBytes(input),
          windowBytes: terminal ? 64 * 1024 * 1024 : 0,
          rows: null,
        });
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
              then(runtime.table(rel.tableId, grant), (table) => {
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
                  pages: runtime.readPages(runtime.view.pages),
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
                  boundedMemory: true,
                  readWindowBytes: terminalFull ? runtime.sourceWindow(rel.id) : 0,
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
        if (
          rel.op !== "limit" &&
          ((rel.op === "project"
            ? rel.exprs.some(jsonExpression)
            : jsonExpression(rel.predicate)) ||
            jsonDescendants.has(rel.input.id))
        ) {
          if (plan.volatile)
            throw new UnsupportedPlanError("JSON expressions mixed with volatile evaluation", {
              construct: "function",
            });
          const exprs = rel.op === "project" ? rel.exprs : rel.predicate;
          if (
            rel.op === "project"
              ? rel.exprs.some((expr) => !jsonInputsBounded(expr, rel.input))
              : !jsonInputsBounded(exprs as ScalarExpr, rel.input)
          )
            throw new UnsupportedPlanError("JSON document without a bounded source envelope", {
              construct: "function",
            });
          let slots = 0;
          const minimum = measureCompilerBuffers(
            compilation,
            input,
            BATCH_ROWS,
            (context) => {
              const operator = new JsonExpressions(exprs, context, { bytes: 0 }, DISCARD);
              slots = operator.workspaceCount;
              return operator;
            },
            (operator) => operator.abort(undefined),
          );
          if (jsonDescendants.has(rel.id)) {
            if (!jsonParents.has(rel.id))
              throw new UnsupportedPlanError(
                "expanding JSON under a join/window retaining parent (checkpoint seam pending)",
                { construct: "function" },
              );
            const base = Math.max(4096, minimum) + JSON_PHASE_FIXED + 2 * JSON_PHASE_IO;
            jsonPlanners.set(rel.id, base);
            fixed(rel, base);
            jsonWorkspaces.set(rel.id, 0);
            return (runtime, downstream, stops, rowsNeeded) => {
              const resume = phases.get(runtime)?.resume;
              const saved =
                resume?.kind === "json" && resume.rel === rel.id ? resume.phase : undefined;
              if (!saved && jsonFrontier.get(runtime) !== rel.id) {
                // An ancestor remains empty while the frontier spools. It needs no evaluator,
                // row buffers or I/O lane until a completed child is delivered in a new phase.
                const waiting: Operator = {
                  push: () => {
                    throw new Error("JSON frontier emitted before checkpoint");
                  },
                  ready: () => Promise.resolve(),
                  finish: () => undefined,
                  abort: () => undefined,
                };
                return child(runtime, waiting, stops, rel.op === "filter" ? undefined : rowsNeeded);
              }
              const capacity = saved?.emit ? 0 : (jsonCapacities.get(runtime)?.get(rel.id) ?? 4096);
              const retainedBytes = saved?.retainedBytes ?? 0;
              return instantiate(
                runtime,
                rel,
                input,
                base + capacity + retainedBytes,
                BATCH_ROWS,
                (context, grant) =>
                  new JsonPhaseOperator(
                    exprs,
                    context,
                    {
                      grant,
                      spill: runtime.request.spill,
                      queryId: runtime.request.queryId,
                      capacity,
                      retainedBytes,
                      ...(rowsNeeded === undefined ? {} : { rowsNeeded }),
                      stopped: () => stops.some((stop) => stop()),
                      progressBytes: runtime.retainedProgressEnvelope(),
                      signal: runtime.signal,
                      ...(saved ? { resume: saved } : {}),
                      dispose: (release) => runtime.dispose(release),
                      handoff: async (phase) => {
                        const state = phases.get(runtime);
                        if (!state || state.pending)
                          throw new Error("Missing/overlapping JSON phase owner");
                        state.pending = { kind: "json", rel: rel.id, phase };
                        for (const [id, parent] of state.excepts) {
                          // A descendant may still be returning from finish(), awaiting this
                          // cut. Only retaining ANCESTORS belong to this continuation.
                          if (!jsonExceptParents.get(rel.id)?.has(id)) continue;
                          const saved = await parent.checkpoint(),
                            prior = state.retained.get(id);
                          state.retained.set(id, saved);
                          await prior?.records.close();
                        }
                        runtime.beginHandoff();
                      },
                    },
                    types(rel),
                    counted(runtime, rel, downstream),
                  ),
                (operator) => {
                  runtime.onAbort((reason) => operator.abort(reason));
                  runtime.dispose(() => operator.closed);
                  return (async () => {
                    try {
                      if (saved) await operator.resume();
                      else
                        await child(
                          runtime,
                          operator,
                          [...stops, () => operator.stopped],
                          rel.op === "filter" ? undefined : rowsNeeded,
                        );
                    } catch (error) {
                      if (!saved && operator.deferRowError(error)) {
                        await operator.finish();
                        return;
                      }
                      operator.abort(error);
                      await operator.closed.catch(() => undefined);
                      throw error;
                    } finally {
                      const profile = runtime.profile(rel);
                      profile.spilledBytes += operator.spilledBytes;
                      profile.spillPasses += operator.spillPasses;
                    }
                  })();
                },
              );
            };
          }
          fixed(rel, Math.max(4096, minimum));
          jsonWorkspaces.set(rel.id, slots);
          return (runtime, downstream, stops, rowsNeeded) =>
            instantiate(
              runtime,
              rel,
              input,
              minimum + slots * (jsonWidths.get(runtime) ?? 1),
              BATCH_ROWS,
              (context) =>
                new JsonExpressions(
                  exprs,
                  context,
                  {
                    bytes: jsonWidths.get(runtime) ?? 1,
                    signal: runtime.signal,
                    stopped: () => stops.some((stop) => stop()),
                  },
                  counted(runtime, rel, downstream),
                ),
              (operator) => {
                runtime.onAbort((reason) => operator.abort(reason));
                runtime.dispose(() => operator.closed);
                return (async () => {
                  try {
                    await child(
                      runtime,
                      operator,
                      stops,
                      rel.op === "filter" ? undefined : rowsNeeded,
                    );
                  } catch (error) {
                    operator.abort(error);
                    await operator.closed.catch(() => undefined);
                    throw error;
                  }
                })();
              },
            );
        }
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
          if (rel.op === "limit" && runtime.bounds(rel).limit === 0) return downstream.finish();
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
          if (
            options.groupedPercentiles !== false &&
            !plan.volatile &&
            supportsPercentile(aggregate, input)
          ) {
            const shape = dictionaryHashShape({ ...aggregate, aggregates: [] }, input);
            if (shape !== null)
              percentilePlanners.set(rel.id, { rel: aggregate, input, compiler: measured, shape });
          }
          return (runtime, downstream) => {
            const selected = percentileChoices.get(runtime)?.get(rel.id);
            if (selected)
              return instantiate(
                runtime,
                rel,
                input,
                selected.minimum,
                BATCH_ROWS,
                (context, grant) =>
                  new PercentileOperator(
                    aggregate,
                    context,
                    grant,
                    selected.bound,
                    runtime.signal,
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
                      await operator.closed.catch(() => undefined);
                      throw error;
                    }
                  })();
                },
                true,
              );
            return instantiate(
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
          };
        }
        const top = groupTops.get(rel.id);
        const outputOptions: AggregateOptions = {
          ...(top === undefined ? {} : { outputTopN: top }),
          groupedDistinctBitmap: options.groupedDistinctBitmap !== false,
          scalarByteDistinct: options.scalarByteDistinct !== false,
          groupedUpdateFusion: options.groupedUpdateFusion !== false,
          packedAverages: options.packedAverages !== false,
          packedIntegers: options.packedIntegers !== false,
          typedLayout: options.typedLayout !== false,
          batchDictionaryMinMax: options.batchDictionaryMinMax !== false,
        };
        const domain =
          options.domainCountSum !== false &&
          rel.op === "aggregate" &&
          aggregate.groupingSets === null &&
          supportsDomainCountSum(aggregate, { input }, outputOptions)
            ? domainSource(rel)
            : null;
        if (domain !== null) {
          const source = readSources.get(domain.scan.id);
          if (source === undefined) throw new TypeError("Missing domain scan recipe");
          readSources.set(domain.scan.id, { ...source, domainColumn: domain.column });
          domainPlans++;
        }
        const minimum = measureCompilerBuffers(
          compilation,
          input,
          BATCH_ROWS,
          (context, grant) =>
            domain === null
              ? new AggregateOperator(aggregate, context, grant, DISCARD, outputOptions)
              : new DomainCountSumOperator(aggregate, context, grant, DISCARD, outputOptions),
          (operator) => operator.abort(undefined),
        );
        if (options.aggregationProgress !== false) {
          const externalCompiler = measureCompilerBuffers(
            compilation,
            input,
            BATCH_ROWS,
            (context) => aggregate.aggregates.map((call) => externalCallArguments(call, context)),
            () => undefined,
          );
          const external: AggregateStrategy = {
            kind: "external",
            minimum:
              externalCompiler +
              externalAggregateMinimum(aggregate, input, outputOptions.outputTopN),
            loans: externalAggregateLoans(aggregate, input, outputOptions.outputTopN),
          };
          const fixedHash =
            aggregate.groupBy.length <= 8 &&
            aggregate.groupBy.every((c) =>
              [
                "bool",
                "int32",
                "int64",
                "float32",
                "float64",
                "date",
                "time",
                "timestamp",
                "uuid",
              ].includes(input[c]?.kind ?? ""),
            ) &&
            aggregate.aggregates.every(
              (call) =>
                !call.distinct &&
                call.filter === null &&
                call.orderBy.length === 0 &&
                (call.fn === "count" ||
                  call.fn === "count_star" ||
                  (["sum", "avg"].includes(call.fn) &&
                    ["float32", "float64"].includes(call.args[0]?.type.kind ?? ""))),
            );
          const restore: AggregateStrategy = {
            kind: "restore",
            minimum: minimum + restoredMinimum(aggregate, input),
            loans: restoredLoans(aggregate),
          };
          aggregatePlanners.set(rel.id, {
            rel: aggregate,
            input,
            compiler: minimum,
            output: outputOptions,
            external,
            fixed: fixedHash,
            hashMinimum: minimum + (512 << 10),
            restore,
            hybrid:
              !plan.volatile &&
              (phaseCapable.has(rel.id) || fixedHash) &&
              aggregate.groupingSets === null &&
              aggregate.aggregates.every(
                (call) =>
                  call.orderBy.length === 0 &&
                  (!call.distinct || call.fn === "count") &&
                  call.args.every((arg) => arg.op === "column"),
              )
                ? {
                    kind: "hybrid",
                    // Only this producer phase exists now. The sorted restore graph
                    // is atomically admitted after eviction retires these grants; reserving
                    // its dormant sort lanes here would inflate a memory-fitting hash minimum.
                    minimum: minimum + (512 << 10) + hybridIo,
                    loans: 2,
                  }
                : null,
            scalar: plan.volatile ? null : scalarHashMinimum(aggregate, input, minimum),
            dictionary: plan.volatile ? null : dictionaryHashShape(aggregate, input),
            domain,
          });
          fixed(rel, external.minimum, rel.id, external.loans);
          return (runtime, downstream) => {
            const strategy = aggregateChoices.get(runtime)?.get(rel.id);
            if (strategy === undefined) throw new Error("Aggregation strategy was not admitted");
            return instantiate(
              runtime,
              rel,
              input,
              strategy.minimum,
              BATCH_ROWS,
              (context, grant) => {
                const target = counted(runtime, rel, downstream),
                  config = { signal: runtime.signal, ...outputOptions };
                if (strategy.kind === "restore") {
                  const resume = phases.get(runtime)?.resume;
                  if (resume?.kind !== "aggregate" || resume.rel !== rel.id)
                    throw new Error("Missing saved restoration phase");
                  return new RestoredAggregateOperator(
                    aggregate,
                    context,
                    {
                      grant,
                      loans: Array.from({ length: strategy.loans }, (_, slot) =>
                        runtime.loan(rel, slot),
                      ),
                      spill: runtime.request.spill,
                      queryId: runtime.request.queryId,
                      signal: runtime.signal,
                    },
                    resume.phase,
                    target,
                  );
                }
                if (strategy.kind === "hybrid") {
                  const state = phases.get(runtime);
                  if (!state) throw new Error("Missing aggregate phase execution");
                  const resume =
                    state.resume?.kind === "aggregate" && state.resume.rel === rel.id
                      ? state.resume.phase
                      : undefined;
                  return new HybridAggregateOperator(
                    aggregate,
                    context,
                    {
                      grant,
                      extras: Array.from({ length: strategy.loans }, (_, slot) =>
                        runtime.loan(rel, slot),
                      ),
                      hashMinimum: aggregatePlanners.get(rel.id)?.hashMinimum ?? 0,
                      progressBytes: runtime.operatorStep(rel),
                      ioMinimum: hybridIo,
                      spill: runtime.request.spill,
                      queryId: runtime.request.queryId,
                      signal: runtime.signal,
                      fixed: aggregatePlanners.get(rel.id)?.fixed ?? false,
                      ...(resume === undefined ? {} : { resume }),
                      ...(phaseCapable.has(rel.id)
                        ? {
                            handoff: async (phase: HybridPhase) => {
                              if (state.pending) throw new Error("Concurrent aggregate handoffs");
                              state.pending = { kind: "aggregate", rel: rel.id, phase };
                              // An EXCEPT right arm can retire only after preserving its
                              // accumulated/spilled left counts and first representatives.
                              for (const [id, parent] of state.excepts) {
                                const saved = await parent.checkpoint();
                                const previous = state.retained.get(id);
                                state.retained.set(id, saved);
                                await previous?.records.close();
                              }
                              runtime.beginHandoff();
                            },
                          }
                        : {}),
                    },
                    target,
                    outputOptions,
                  );
                }
                if (strategy.kind === "external")
                  return new ExternalAggregateOperator(
                    aggregate,
                    context,
                    {
                      grant,
                      loans: Array.from({ length: strategy.loans }, (_, slot) =>
                        runtime.loan(rel, slot),
                      ),
                      spill: runtime.request.spill,
                      queryId: runtime.request.queryId,
                      signal: runtime.signal,
                    },
                    target,
                    outputOptions.outputTopN,
                  );
                if (strategy.kind === "domain" && domain !== null) {
                  const certificate = runtime.sourceDomain(domain.scan.id);
                  if (certificate === undefined) throw new Error("Missing admitted exact domain");
                  const operator = new DomainCountSumOperator(
                    aggregate,
                    context,
                    grant,
                    target,
                    config,
                  );
                  operator.bounds(certificate.low, certificate.high, certificate.rows);
                  operator.seal();
                  return operator;
                }
                return new AggregateOperator(aggregate, context, grant, target, config);
              },
              (operator) => {
                runtime.onAbort((reason) => operator.abort(reason));
                if (
                  !(operator instanceof ExternalAggregateOperator) &&
                  !(operator instanceof HybridAggregateOperator) &&
                  !(operator instanceof RestoredAggregateOperator)
                )
                  return child(runtime, operator, []);
                runtime.dispose(() => operator.closed);
                return (async () => {
                  try {
                    const state = phases.get(runtime);
                    if (
                      (operator instanceof HybridAggregateOperator ||
                        operator instanceof RestoredAggregateOperator) &&
                      state?.resume?.rel === rel.id
                    )
                      await operator.finish();
                    else await child(runtime, operator, []);
                    if (operator instanceof RestoredAggregateOperator) {
                      runtime.profile(rel).spilledBytes = operator.profile.spilledBytes;
                      runtime.profile(rel).spillPasses = operator.profile.spillPasses;
                    }
                    if (operator instanceof HybridAggregateOperator) {
                      runtime.profile(rel).spilledBytes = operator.profile.spilledBytes;
                      runtime.profile(rel).spillPasses =
                        operator.profile.passes + operator.profile.evictions;
                    }
                  } catch (error) {
                    operator.abort(error);
                    await operator.closed.catch(() => undefined);
                    throw error;
                  }
                })();
              },
              strategy.kind === "hash" || strategy.kind === "domain",
            );
          };
        }
        fixed(rel, Math.max(4096, minimum));
        return (runtime, downstream) =>
          instantiate(
            runtime,
            rel,
            input,
            minimum,
            BATCH_ROWS,
            (context, grant) => {
              const config = {
                signal: runtime.signal,
                ...outputOptions,
              };
              const certificate =
                domain === null ? undefined : runtime.sourceDomain(domain.scan.id);
              if (certificate === undefined || certificate.column !== domain?.column)
                return new AggregateOperator(
                  aggregate,
                  context,
                  grant,
                  counted(runtime, rel, downstream),
                  config,
                );
              const operator = new DomainCountSumOperator(
                aggregate,
                context,
                grant,
                counted(runtime, rel, downstream),
                config,
              );
              operator.bounds(certificate.low, certificate.high, certificate.rows);
              operator.seal();
              return operator;
            },
            (operator) => {
              runtime.onAbort((reason) => operator.abort(reason));
              return child(runtime, operator, []);
            },
          );
      }
      case "sort":
      case "window": {
        if (rel.op === "sort" && options.groupTopN !== false) {
          const shape = groupTopNShape(rel);
          if (shape) groupTops.set(shape.aggregate.id, shape.spec);
        }
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
          const bound = windowBounds.get(rel.id);
          const windowSpec =
            rel.op === "window"
              ? {
                  types: physical.types,
                  ...(bound === undefined ? {} : { topK: bound }),
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
              {
                ...shared,
                columnar: options.columnarSort !== false,
                columnarOwnership: options.sortOwnership !== false,
                columnarDictionaries: options.sortDictionaries !== false,
                columnarCompression: options.sortCompression !== false,
              },
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
      case "setOp": {
        if (rel.kind !== "except" || rel.inputs.length !== 2)
          throw new UnsupportedPlanError("set operation other than binary EXCEPT", {
            construct: "setOp",
          });
        const [leftInput, rightInput] = rel.inputs;
        if (leftInput === undefined || rightInput === undefined)
          throw new TypeError("Missing EXCEPT arm");
        const input = types(rel);
        exceptKeyLayout(input);
        const left = prepare(leftInput),
          right = prepare(rightInput);
        const minimum = exceptMinimumBytes(input);
        fixed(rel, minimum);
        return (runtime, downstream) =>
          instantiate(
            runtime,
            rel,
            input,
            minimum,
            1,
            (_context, grant) =>
              new ExceptOperator(
                input,
                rel.all,
                {
                  grant,
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
                const state = phases.get(runtime);
                state?.excepts.set(rel.id, operator);
                try {
                  const saved = state?.retained.get(rel.id);
                  if (saved) {
                    await operator.restore(saved);
                    state?.retained.delete(rel.id);
                  }
                  if (!operator.leftComplete) await left(runtime, operator.left, []);
                  if (runtime.handingOff) return;
                  await right(runtime, operator.right, []);
                  const profile = runtime.profile(rel);
                  profile.spilledBytes = operator.profile.spilledBytes;
                  profile.spillPasses = operator.profile.spillPasses;
                } catch (error) {
                  operator.abort(error);
                  await operator.closed.catch(() => undefined);
                  throw error;
                } finally {
                  state?.excepts.delete(rel.id);
                }
              })();
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
    readSources.set(project.id, {
      id: project.id,
      tableId: scan.tableId,
      columns: scan.columns,
      types: payload,
      windowBytes: 0,
      stagingBytes: memoryScanBufferBytes(payload),
      rows: null,
    });
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
                      readTable: () => runtime.table(scan.tableId, grant),
                      boundedMemory: true,
                      pages: runtime.readPages(runtime.view.pages),
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
  const hasJson = jsonWorkspaces.size !== 0;
  const hasInputSizedJson = [...jsonWorkspaces.values()].some((slots) => slots > 0);
  const jsonConstants = hasJson ? jsonLiteralBytes(plan) : 0;
  // One document may feed several output columns in the same copied result row.
  const jsonProgressColumns = Math.max(1, plan.columns.length);
  // SQL may use the exact PK order only with a complete caller-admitted cursor.
  // Explicit indexRange and legacy cursors retain their separate diagnostic opt-in.
  const ordered = options.keyOrder !== false ? orderedSort(plan) : null;
  const cursorIdentity: OperatorIdentity = {
    id: nextPhysicalId++,
    op: "indexRange",
    ...(ordered ? { profileAs: ordered.scan } : {}),
  };
  // Extra helper buffers must not turn a formerly admissible small-cap query into a refusal.
  // Both strategies are capability-checked at prepare; only their execution resources differ.
  const fallback =
    lateSortIds.size === 0 && domainPlans === 0
      ? null
      : compilePlan(plan, { ...options, lateSort: false, domainCountSum: false });
  const genericProgram: Program = (runtime, sink) => {
    runtime.resolveCall = resolveCall;
    // LIMIT zero never opens its input. Keep capability checks at prepare, but do not reserve
    // buffers for those unreachable pipelines (including when zero is a bound parameter).
    const active = new Set<number>();
    const visit = (rel: Rel): void => {
      // Lowering stores ORDER BY's LIMIT on its sort. Zero must not open even its pools.
      if ((rel.op === "sort" || rel.op === "limit") && runtime.bounds(rel).limit === 0) return;
      active.add(rel.id);
      if (rel.op === "recursive") {
        visit(rel.base);
        visit(rel.step);
      }
      if (rel.op === "limit" && runtime.bounds(rel).limit === 0) return;
      if (rel.op === "join") {
        visit(rel.left);
        visit(rel.right);
      } else if ("inputs" in rel) {
        for (const input of rel.inputs) visit(input);
      } else if ("input" in rel) visit(rel.input);
    };
    visit(plan.root);
    if ([...aggregatePlanners.keys()].some((id) => active.has(id))) {
      // Inline values are <=64KiB by the stored format. This compound envelope is
      // aggregation-only: B2's plain export minimum is unchanged. Larger source steps
      // and owned parameter copies are added by the runtime's admitted progress rule.
      // Plan-owned literals can exceed the stored inline threshold. Their already-built
      // vectors give an exact byte bound without re-encoding or copying their payloads.
      let literalBytes = LARGE_VALUE_BYTES;
      const inspected = new Set<object>();
      const inspect = (value: unknown): void => {
        if (
          value === null ||
          typeof value !== "object" ||
          ArrayBuffer.isView(value) ||
          inspected.has(value)
        )
          return;
        // An unopened zero-limit subtree contributes no buffers to its live sibling.
        if ("op" in value && "id" in value && typeof value.id === "number" && !active.has(value.id))
          return;
        inspected.add(value);
        if ("form" in value && value.form === "flat" && "data" in value) {
          const data = (value as { data: { layout: string; bytes?: Uint8Array } }).data;
          if (data.layout === "varlen")
            literalBytes = Math.max(literalBytes, data.bytes?.byteLength ?? 0);
          return;
        }
        for (const child of Object.values(value)) inspect(child);
      };
      inspect(plan.root);
      let inline = 0,
        sourceMultiplicity = 1;
      for (const planner of aggregatePlanners.values()) {
        if (!active.has(planner.rel.id)) continue;
        const variable = planner.input.reduce((n, type) => {
          if (["text", "bytes", "json", "array", "enum", "decimal"].includes(type.kind)) {
            return n + literalBytes;
          }
          return n + 16;
        }, 0);
        const copies = new Map<number, number>();
        for (const column of planner.rel.groupBy) copies.set(column, (copies.get(column) ?? 0) + 1);
        const multiplicity = Math.max(1, ...copies.values());
        inline = Math.max(inline, 4 * multiplicity * variable);
        // The source hint already bounds all selected columns together. Only a
        // repeated key position duplicates that whole prefix; extra inline/literal
        // and parameter output is added separately by the runtime.
        sourceMultiplicity = Math.max(sourceMultiplicity, multiplicity);
      }
      runtime.progressEnvelope(inline, sourceMultiplicity);
    }
    const entries = admissions
      .filter((entry) => active.has(entry.owner))
      .map((entry) => ({
        rel: entry.rel,
        owner: entry.owner,
        loanSlots: entry.loanSlots,
        minimum:
          typeof entry.minimum === "number"
            ? entry.minimum
            : entry.minimum(runtime.request.governor.capBytes),
      }));
    // At most one unfinished JSON producer is active. Completed children are delivered
    // from files, while unopened ancestors/EXCEPT siblings have no native JSON buffers.
    const chooseJsonFrontier = (cut: Cut | null): number | null => {
      const state = phases.get(runtime);
      const search = (node: Rel): number | null => {
        if (!active.has(node.id)) return null;
        if (node.id === cut?.rel) return cut.kind === "json" && !cut.phase.emit ? node.id : null;
        const children =
          "inputs" in node
            ? node.inputs.filter((_, i) => i !== 0 || !state?.retained.get(node.id)?.leftDone)
            : "input" in node
              ? [node.input]
              : [];
        for (const child of children) {
          const next = search(child);
          if (next !== null) return next;
        }
        return jsonPlanners.has(node.id) ? node.id : null;
      };
      return search(plan.root);
    };
    const configureJson = (list: typeof entries, cut: Cut | null): void => {
      if (jsonPlanners.size === 0) return;
      const frontier = chooseJsonFrontier(cut);
      jsonFrontier.set(runtime, frontier);
      if (jsonPlanners.size > 0)
        runtime.resultProgressEnvelope(frontier !== null ? 0 : (cut?.phase.progressBytes ?? null));
      for (const entry of list) {
        const base = jsonPlanners.get(entry.rel.id);
        if (base === undefined) continue;
        const saved = cut?.kind === "json" && cut.rel === entry.rel.id ? cut.phase : null;
        entry.minimum = saved
          ? base +
            (saved.emit ? 0 : (jsonCapacities.get(runtime)?.get(entry.rel.id) ?? saved.capacity)) +
            saved.retainedBytes
          : entry.rel.id === frontier
            ? base + (jsonCapacities.get(runtime)?.get(entry.rel.id) ?? 4096)
            : 0;
        entry.loanSlots = 0;
      }
    };
    configureJson(entries, null);
    const demand = new Map<number, number>();
    const findDemand = (node: Rel, need: number | null): void => {
      if (!active.has(node.id)) return;
      if (node.op === "recursive") {
        findDemand(node.base, need);
        return;
      }
      if (node.op === "scan") {
        if (need !== null && node.filter === null) demand.set(node.id, need);
        return;
      }
      if (node.op === "limit") {
        const bound = runtime.bounds(node);
        const count =
          bound.limit === null ? need : need === null ? bound.limit : Math.min(need, bound.limit);
        findDemand(node.input, count === null ? null : count + bound.offset);
        return;
      }
      if (node.op === "project") {
        findDemand(node.input, need);
        return;
      }
      if (node.op === "join") {
        findDemand(node.left, null);
        findDemand(node.right, null);
      } else if ("inputs" in node) {
        for (const input of node.inputs) findDemand(input, null);
      } else if ("input" in node) findDemand(node.input, null);
    };
    findDemand(plan.root, null);
    const sources = [...readSources.values()].filter((source) => active.has(source.id));
    const bounds = new Map<number, ReadBound>();
    const chooseAggregates = () => runSliced(choose(), new Slice(Infinity), () => undefined);
    const choose = function* (): SlicedJob<void> {
      const selectedPercentiles = new Map<number, { bound: PercentileBound; minimum: number }>();
      percentileChoices.set(runtime, selectedPercentiles);
      for (const entry of entries) {
        const planner = percentilePlanners.get(entry.rel.id);
        if (!planner) continue;
        const read = bounds.get(planner.shape.scanId);
        if (!read) continue;
        const proof = yield* readValue(
          probeDictionaryGroups(
            runtime.view,
            planner.shape.source,
            runtime.request.reads,
            runtime.request.governor,
            read,
            runtime.signal,
            `query#${String(runtime.request.queryId)}/percentile-domain-planning`,
          ),
        );
        if (proof === null) continue;
        const keys = dictionaryHashMinimum(
          { ...planner.rel, aggregates: [] },
          planner.input,
          planner.shape,
          proof,
          0,
          {},
        );
        if (keys === null) continue;
        const bound = { rows: proof.rows, groups: Math.max(1, proof.groups), keyMinimum: keys };
        const minimum = percentileMinimum(planner.rel, bound, planner.compiler);
        const saved = { minimum: entry.minimum, loanSlots: entry.loanSlots };
        entry.minimum = minimum;
        entry.loanSlots = 0;
        // Optional complete resident workspace: no private allocation or input has opened yet.
        if (runtime.minimumBytes(entries) <= runtime.request.governor.capBytes)
          selectedPercentiles.set(entry.rel.id, { bound, minimum });
        else Object.assign(entry, saved);
      }
      const choices = new Map<number, AggregateStrategy>();
      aggregateChoices.set(runtime, choices);
      for (const entry of entries) {
        const planner = aggregatePlanners.get(entry.rel.id);
        if (!planner) continue;
        // Ordered DISTINCT/ordered-set transitions retain the SQL-ordered path.
        let strategy: AggregateStrategy = planner.hybrid ?? planner.external;
        if (planner.scalar !== null) strategy = { kind: "hash", minimum: planner.scalar, loans: 0 };
        else if (planner.domain !== null) {
          const certificate = runtime.sourceDomain(planner.domain.scan.id);
          if (certificate !== undefined) {
            const span = certificate.high - certificate.low + 1;
            if (Number.isInteger(span) && span >= 1 && span <= 1 << 22)
              strategy = {
                kind: "domain",
                minimum: planner.compiler + 256 * 1024 + 16 * span,
                loans: 0,
              };
          }
        } else if (planner.dictionary !== null) {
          const read = bounds.get(planner.dictionary.scanId);
          if (read !== undefined) {
            const proof = yield* readValue(
              probeDictionaryGroups(
                runtime.view,
                planner.dictionary.source,
                runtime.request.reads,
                runtime.request.governor,
                read,
                runtime.signal,
                `query#${String(runtime.request.queryId)}/aggregate-domain-planning`,
              ),
            );
            if (proof !== null) {
              const minimum = dictionaryHashMinimum(
                planner.rel,
                planner.input,
                planner.dictionary,
                proof,
                planner.compiler,
                planner.output,
              );
              if (minimum !== null) strategy = { kind: "hash", minimum, loans: 0 };
            }
          }
        }
        choices.set(entry.rel.id, strategy);
        entry.minimum = strategy.minimum;
        entry.loanSlots = strategy.loans;
      }
      // A certified hash workspace is optional. Prefer external spilling when it has a smaller
      // complete minimum and is what makes this execution admissible. No credit is held yet.
      while (runtime.minimumBytes(entries) > runtime.request.governor.capBytes) {
        let best: (typeof entries)[number] | undefined,
          total = runtime.minimumBytes(entries);
        for (const entry of entries) {
          const planner = aggregatePlanners.get(entry.rel.id),
            prior = choices.get(entry.rel.id);
          const fallback = planner?.hybrid ?? planner?.external;
          if (!planner || !fallback || prior?.kind === fallback.kind) continue;
          const saved = { minimum: entry.minimum, loanSlots: entry.loanSlots };
          entry.minimum = fallback.minimum;
          entry.loanSlots = fallback.loans;
          const candidate = runtime.minimumBytes(entries);
          Object.assign(entry, saved);
          if (candidate < total) {
            best = entry;
            total = candidate;
          }
        }
        if (!best) break;
        const planner = aggregatePlanners.get(best.rel.id);
        const external = planner?.hybrid ?? planner?.external;
        if (!external) throw new Error("Missing external fallback");
        choices.set(best.rel.id, external);
        best.minimum = external.minimum;
        best.loanSlots = external.loans;
      }
    };
    const needsChoices = aggregatePlanners.size + percentilePlanners.size > 0;
    const execute = () =>
      then(needsChoices ? chooseAggregates() : undefined, () => {
        if (
          ![...(aggregateChoices.get(runtime)?.values() ?? [])].some(
            (choice) => choice.kind === "hybrid",
          ) &&
          ![...jsonPlanners.keys()].some((id) => active.has(id))
        )
          return then(runtime.reserve(entries), () => recipe(runtime, sink, []));
        runtime.enablePhases();
        const state: Phases = {
          resume: null,
          pending: null,
          excepts: new Map(),
          retained: new Map(),
        };
        phases.set(runtime, state);
        const guarded: Operator = {
          push: (batch, transfer) => sink.push(batch, transfer),
          ready: () => sink.ready(),
          finish: () => sink.finish(),
          abort: (reason) => {
            if (!runtime.isHandoff(reason)) sink.abort(reason);
          },
        };
        return (async () => {
          const idle = await acquirePhasePool(runtime.request.governor, runtime.signal);
          let failed = false,
            failure: unknown;
          try {
            await runtime.reserve(entries);
            await recipe(runtime, guarded, []);
            while (state.pending) {
              const cut: Cut = state.pending;
              // Keep a cleanup owner before clearing pending: producer suspension
              // itself awaits consumer credit and can reject on cancellation.
              state.resume = cut;
              state.pending = null;
              await runtime.suspendProducer(sink, idle.pool);
              const remaining = new Set<number>();
              const visitRemaining = (node: Rel): void => {
                remaining.add(node.id);
                if (node.id === cut.rel) return;
                if ("inputs" in node) {
                  for (let i = 0; i < node.inputs.length; i++) {
                    if (i === 0 && state.retained.get(node.id)?.leftDone) continue;
                    const input = node.inputs[i];
                    if (input) visitRemaining(input);
                  }
                } else if ("left" in node && "right" in node) {
                  visitRemaining(node.left);
                  visitRemaining(node.right);
                } else if ("input" in node) visitRemaining(node.input);
              };
              visitRemaining(plan.root);
              const next = entries.filter((entry) => remaining.has(entry.owner));
              if (cut.kind === "json") {
                const target = next.find((entry) => entry.rel.id === cut.rel),
                  base = jsonPlanners.get(cut.rel);
                if (!target || base === undefined) throw new Error("Missing JSON phase admission");
                target.minimum = base + cut.phase.capacity + cut.phase.retainedBytes;
                target.loanSlots = 0;
                let capacities = jsonCapacities.get(runtime);
                if (!capacities) {
                  capacities = new Map();
                  jsonCapacities.set(runtime, capacities);
                }
                capacities.set(
                  cut.rel,
                  cut.phase.emit
                    ? 0
                    : Math.max(cut.phase.capacity, 2 * (capacities.get(cut.rel) ?? 4096)),
                );
              } else {
                const aggregate = next.find((entry) => entry.rel.id === cut.rel);
                if (!aggregate) throw new Error("Missing restore aggregate admission");
                const restore = aggregatePlanners.get(cut.rel)?.restore;
                if (!restore) throw new Error("Missing admitted restoration recipe");
                aggregate.minimum = restore.minimum;
                aggregate.loanSlots = restore.loans;
                aggregateChoices.get(runtime)?.set(cut.rel, restore);
              }
              configureJson(next, cut);
              // A cut in EXCEPT's left arm leaves its right input unopened. Carry the
              // original publication-stable bounds for those future sources into admission.
              let io = 0,
                progress = 0;
              for (const [id, bound] of bounds)
                if (remaining.has(id)) {
                  io = Math.max(io, bound.io);
                  progress = Math.max(progress, bound.progress);
                }
              runtime.readMinimum(io, progress, cut.phase.progressBytes);
              const binding = cut.phase.parameters.phaseBinding(runtime.signal);
              const logicalInputs = binding.logicalBytes ?? binding.bytes;
              if (
                cut.kind === "json" &&
                !cut.phase.emit &&
                runtime.minimumBytes(next, logicalInputs, binding.largestSlot) +
                  binding.bytes -
                  logicalInputs >
                  runtime.request.governor.capBytes
              ) {
                // Geometric slack is optional. Only the exact complete minimum can refuse.
                jsonCapacities.get(runtime)?.set(cut.rel, cut.phase.capacity);
                configureJson(next, cut);
              }
              await runtime.restorePhase(next, binding, idle.pool);
              sink.resumeBudget();
              await recipe(runtime, guarded, []);
              state.resume = null;
            }
          } catch (error) {
            failed = true;
            failure = error;
          }
          {
            // A resumed recipe may already have installed its next cut. Both
            // slots remain owned until the corresponding files have drained.
            const retained = new Set(
              [state.pending, state.resume].flatMap((cut) => (cut ? [cut.phase] : [])),
            );
            const cleanup = await Promise.allSettled([
              ...[...retained].map((phase) => closePhase(phase)),
              ...[...state.retained.values()].map((saved) => saved.records.close()),
            ]);
            state.retained.clear();
            phases.delete(runtime);
            idle.release();
            const errors = cleanup
              .filter((r): r is PromiseRejectedResult => r.status === "rejected")
              .map((r) => r.reason as unknown);
            if (failed) throw failure;
            if (errors.length) throw new AggregateError(errors, "Phase files failed to close");
          }
        })();
      });
    if (sources.length === 0) {
      if (!hasJson) return execute();
      const width = jsonConstants + 1;
      // Literal documents also bound result copies; no storage read contributes that size.
      runtime.readMinimum(0, hasInputSizedJson ? 4 * jsonConstants * jsonProgressColumns : 0);
      jsonWidths.set(runtime, width);
      for (const entry of entries) entry.minimum += (jsonWorkspaces.get(entry.rel.id) ?? 0) * width;
      return execute();
    }
    const sizeSources = function* (): SlicedJob<void> {
      const fixedMinimums = entries.map((entry) => entry.minimum);
      let windows = true;
      for (;;) {
        let io = 0,
          progress = 0;
        bounds.clear();
        for (const source of sources) {
          const bound = yield* readValue(
            boundSource(
              runtime.view,
              {
                ...source,
                windowBytes: windows ? source.windowBytes : 0,
                ...(runtime.request.outputBuffers && source.windowBytes > 0
                  ? { retain: false }
                  : {}),
                rows: demand.get(source.id) ?? null,
              },
              runtime.request.governor,
              runtime.signal,
              batchTargetFor(runtime.request.governor.capBytes, 4),
            ),
          );
          bounds.set(source.id, bound);
          io = Math.max(io, bound.io);
          progress = Math.max(progress, bound.progress);
        }
        const jsonWidth = Math.max(jsonConstants, progress) + 1;
        if (hasJson) jsonWidths.set(runtime, jsonWidth);
        for (const [index, entry] of entries.entries())
          entry.minimum =
            (fixedMinimums[index] ?? 0) +
            (bounds.get(entry.rel.id)?.bytes ?? 0) +
            (hasJson ? (jsonWorkspaces.get(entry.rel.id) ?? 0) * jsonWidth : 0);
        runtime.sourceBounds(bounds);
        runtime.readMinimum(
          io,
          hasInputSizedJson
            ? Math.max(progress, 4 * jsonConstants) * jsonProgressColumns
            : progress,
        );
        if (
          windows &&
          runtime.minimumBytes(entries) > runtime.request.governor.capBytes &&
          [...bounds.values()].some((bound) => bound.windowBytes > 0)
        ) {
          // Coalesced export windows are optional. All sizing holds have ended: retry the
          // ordinary bounded batch before treating the execution minimum as over-cap.
          windows = false;
          continue;
        }
        break;
      }
      if (
        fallback !== null &&
        aggregatePlanners.size === 0 &&
        runtime.minimumBytes(entries) > runtime.request.governor.capBytes
      ) {
        runtime.sourceBounds(new Map());
        runtime.readMinimum(0, 0);
        return yield* readValue(fallback(runtime, sink));
      }
      return yield* readValue(execute());
    };
    return runSliced(sizeSources(), new Slice(Infinity), () => undefined);
  };
  const program: Program = (runtime, sink) => {
    runtime.resolveCall = resolveCall;
    if (ordered === null) return genericProgram(runtime, sink);
    const { rel, scan, project } = ordered;
    const table = runtime.view.catalog.table(scan.tableId);
    const index = table?.indexes.find((entry) => entry.id === table.primaryKey);
    if (
      index?.kind !== "ordered" ||
      !index.unique ||
      index.columnIds.length !== rel.keys.length ||
      !ordered.keys.every((column, k) => column === index.columnIds[k])
    )
      return genericProgram(runtime, sink);
    // Validate both bounds before zero admission, including OFFSET with LIMIT 0.
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
    const cap = runtime.request.governor.capBytes;
    const inputs = integerParameterBytes(plan, runtime.request.parameters);
    // Keep the generic recipe without even opening an index reader when the fixed
    // optional buffers plus owned inputs already do not fit.
    if (inputs === null || runtime.minimumBytes(indexMinimums(shape, cap), inputs) > cap)
      return genericProgram(runtime, sink);
    const range = planIndexRange(runtime.view.index(scan.tableId, index.id), shape.descending);
    const run = (read?: AdmittedIndexRead) => {
      const entries = read ? admittedIndexMinimums(shape, cap, read) : indexMinimums(shape, cap);
      return then(runtime.reserve(entries), () =>
        then(
          runIndexOrder(
            runtime,
            sink,
            shape,
            index.id,
            bounds.limit,
            bounds.offset,
            undefined,
            undefined,
            read,
          ),
          () => {
            for (const projection of ordered.projects) {
              if (projection.id === project.id) continue;
              const profile = runtime.profile(projection),
                output = runtime.profile(project);
              profile.rowsOut = output.rowsOut;
              profile.batchesOut = output.batchesOut;
            }
          },
        ),
      );
    };
    if (range === null) return options.keyOrder === true ? run() : genericProgram(runtime, sink);
    if (runtime.minimumBytes(indexMinimums(shape, cap), inputs) + range.minimumBytes > cap)
      return genericProgram(runtime, sink);
    const size = function* (): SlicedJob<void> {
      // A locator may name any group. Size only the output columns, but use the
      // complete pinned source rather than assuming a physical prefix for PK order.
      const payload = yield* readValue(
        boundSource(
          runtime.view,
          {
            id: project.id,
            tableId: scan.tableId,
            columns: shape.columns,
            types: shape.types,
            windowBytes: 0,
            stagingBytes: memoryScanBufferBytes(shape.types),
            rows: null,
          },
          runtime.request.governor,
          runtime.signal,
          batchTargetFor(cap, 4),
        ),
      );
      const read: AdmittedIndexRead = { plan: range, cursor: cursorIdentity, payload };
      runtime.sourceBounds(new Map([[project.id, payload]]));
      runtime.readMinimum(payload.io, payload.progress);
      if (runtime.minimumBytes(admittedIndexMinimums(shape, cap, read), inputs) > cap) {
        runtime.sourceBounds(new Map());
        runtime.readMinimum(0, 0);
        return yield* readValue(genericProgram(runtime, sink));
      }
      return yield* readValue(run(read));
    };
    return runSliced(size(), new Slice(Infinity), () => undefined);
  };
  return Object.assign(program, { atomic: true });
}
