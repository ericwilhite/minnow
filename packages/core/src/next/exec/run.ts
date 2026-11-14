/** One query's admissions, resources and the executed/delivered lifecycle. */
import type { MaybePromise } from "../access/key-index.js";
import { SnapshotReleasedError } from "../access/snapshot.js";
import { QueryRoots, type RootColumns } from "../page/query-roots.js";
import { readTable } from "./read-table.js";
import type { ByteBudget } from "../access/space.js";
import type { Snapshot, TableSnapshot, TableReadRecipe } from "../access/snapshot.js";
import type { Bound, PhysicalPlan, Rel } from "../ir/ir.js";
import type { Grant } from "../memory/governor.js";
import {
  ExecutionError,
  ParameterBindError,
  PlanStaleError,
  QueryCancelledError,
} from "./errors.js";
import { WindowError } from "./window-frames.js";
import type {
  ExecuteRequest,
  Execution,
  ExecutionProfile,
  ExecutionState,
  ExecutionSummary,
  OperatorProfile,
} from "./executor.js";
import {
  prepareParameters,
  needsParameterSizing,
  PARAMETER_SIZING_BYTES,
  type ParameterBinding,
} from "./parameters.js";
import { RESULT_FRAME_TARGET_BYTES, RESULT_WINDOW_BYTES, ResultChannel } from "./result.js";
import type { ResultAllocator } from "./result-buffer.js";
import { ResultSink, resultFrameMetadataBytes } from "./result-sink.js";
import type { ParameterSlots, CallResolver } from "./kernels/compile.js";
import { ReservedGrant } from "./admission.js";
import { DeferredGrant } from "./deferred-grant.js";
import { ReadStep } from "./read-step.js";
import { admitWorkingSet } from "../memory/working-set.js";
import { MAX_BLOB_BYTES } from "../access/space.js";
import { StepLoans } from "../memory/step-loans.js";
import {
  aggregateLaneBytes,
  aggregateSharedBytes,
  type AggregateProgressLayout,
} from "./agg/parameter-copies.js";
import type { ReadBound } from "../page/read-bounds.js";

/** A set/join transition can retain an encoded key, its hash copy and a
 * doubling representative arena (or a replacement page). Admit that compound
 * working set up front; joins also retain the probe page while looking up its
 * key against that representative/table. Callers still preflight one loan,
 * never grow a live loan.
 * This does not change the global reserve or the capacity of unrelated operators. */
function progressCopies(rel: OperatorIdentity): number {
  return rel.op === "join" ? 5 : rel.op === "setOp" ? 4 : 1;
}

interface Reservation {
  readonly rel: OperatorIdentity;
  readonly minimum: number;
  readonly loanSlot?: number;
}

type Mutable<T> = { -readonly [K in keyof T]: T[K] };
export type MutableOperatorProfile = Mutable<OperatorProfile>;
/** Identity for physical helper operators as well as lowered relational nodes. */
export type OperatorIdentity = Pick<Rel, "id" | "op"> & {
  /** Private helper grants retain unique labels, but statistics belong to an IR operator. */
  readonly profileAs?: Pick<Rel, "id" | "op">;
};
export type Program = ((runtime: QueryRuntime, sink: ResultSink) => MaybePromise<void>) & {
  readonly atomic?: boolean;
};

interface ResourcePhase {
  readonly parent: ResourcePhase | null;
  readonly jobs: Array<() => MaybePromise<void>>;
  readonly aborters: Array<(reason: unknown) => void>;
}
const HANDOFF = new Error("Producer retired at aggregation phase handoff");

/** Per-execution only: never retained by an immutable prepared plan. */
export class QueryRuntime {
  #phaseCapable = false;
  #handoff = false;
  #scope: ResourcePhase | null = null;
  enablePhases(): void {
    if (this.#scope !== null) throw new Error("Phase capability must precede graph construction");
    this.#phaseCapable = true;
  }
  isHandoff(reason: unknown): boolean {
    return reason === HANDOFF;
  }
  get handingOff(): boolean {
    return this.#handoff;
  }
  beginHandoff(): void {
    this.#handoff = true;
  }
  resumePhase(): void {
    this.#handoff = false;
  }
  /** Nested recipes retire their own readers/operators before returning native credit.
   * The snapshot pin belongs to Execution and is deliberately outside these scopes. */
  resource<T>(body: () => MaybePromise<T>, release: () => MaybePromise<void>): MaybePromise<T> {
    const scope: ResourcePhase | null = this.#phaseCapable
      ? { parent: this.#scope, jobs: [], aborters: [] }
      : null;
    if (scope) this.#scope = scope;
    let failed = false,
      reason: unknown;
    const finish = (): MaybePromise<void> => {
      if (!scope) return release();
      if (this.#scope !== scope) throw new Error("Producer scopes overlapped");
      this.#scope = scope.parent;
      const errors: unknown[] = [];
      const aborters = scope.aborters.splice(0);
      for (const abort of aborters) {
        const index = this.#aborters.indexOf(abort);
        if (index >= 0) this.#aborters.splice(index, 1);
        if (failed || this.#handoff || this.#aborted)
          try {
            abort(this.#aborted ? this.#reason : failed ? reason : HANDOFF);
          } catch (error) {
            errors.push(error);
          }
      }
      const jobs = scope.jobs.splice(0).reverse();
      const drain = (at: number): MaybePromise<void> => {
        for (let i = at; i < jobs.length; i++) {
          try {
            const work = jobs[i]?.();
            if (work instanceof Promise)
              return work
                .catch((error: unknown) => {
                  errors.push(error);
                })
                .then(() => drain(i + 1));
          } catch (error) {
            errors.push(error);
          }
        }
        scope.aborters.length = 0;
        const retired = release();
        const report = () => {
          if (errors.length) throw new AggregateError(errors, "Producer phase cleanup failed");
        };
        return retired instanceof Promise ? retired.then(report) : report();
      };
      return drain(0);
    };
    let result: MaybePromise<T>;
    try {
      result = body();
    } catch (error) {
      failed = true;
      reason = error;
      const done = finish();
      if (done instanceof Promise)
        return done.then(() => {
          throw error;
        });
      throw error;
    }
    if (result instanceof Promise)
      return result
        .catch((error: unknown) => {
          failed = true;
          reason = error;
          throw error;
        })
        .finally(finish);
    const done = finish();
    return done instanceof Promise ? done.then(() => result) : result;
  }

  resolveCall: CallResolver | undefined;
  readonly params: ParameterSlots = { slots: [] };
  readonly profiles = new Map<number, MutableOperatorProfile>();
  readonly #operatorGrants = new Map<number, Grant[]>();
  readonly #grants: Grant[] = [];
  readonly #dispose: Array<() => MaybePromise<void>> = [];
  readonly #aborters: Array<(reason: unknown) => void> = [];
  #aborted = false;
  #reason: unknown;
  #closed = false;
  #peak = 0;
  #pipelines = 0;
  #wait = 0;
  readonly #reserved = new Map<string, ReservedGrant>();
  readonly #phaseParts = new Map<ReservedGrant, Grant>();
  readonly #bounds = new Map<number, { readonly limit: number | null; readonly offset: number }>();
  #allocateResult: ResultAllocator | null = null;
  resultAllocator(allocate: ResultAllocator): void {
    this.#allocateResult = allocate;
  }
  allocateResultOwned(bytes: number) {
    if (this.#allocateResult === null) throw new Error("Missing result allocation recipe");
    return this.#allocateResult(bytes);
  }
  #atomicResult: DeferredGrant | null = null;
  #window = 0;
  #readBytes = 0;
  #progressBytes = 0;
  #savedProgressBytes = 0;
  #resultProgressOverride: number | null = null;
  #resultOwnedMinimum = false;
  #resultCopies = 1;
  #resultRowProgress: ((slots: readonly number[], largest: number) => number | null) | null = null;
  /** Complete emitted row, already including logical aliases; unrelated input is not output. */
  resultRowProgress(bound: (slots: readonly number[], largest: number) => number | null): void {
    this.#resultRowProgress = bound;
  }
  #growthSlack = 0;
  /** Smaller result minima return credit to shared admission. Keep speculative
   * operator growth at its previous target instead of spending that saving on
   * a larger failed projection/hash attempt. No credit is held here: other
   * executions can use it. Protected complete steps are unchanged; optional
   * allocation denial selects existing split/spill, never a typed refusal. */
  projectionAvailable(grant: Grant): number {
    return Math.max(0, grant.held - grant.allocated - grant.loaned) + this.#growthRoom();
  }
  #growthRoom(): number {
    return Math.max(
      0,
      this.request.governor.capBytes -
        this.request.governor.reserveBytes -
        this.request.governor.usedBytes -
        this.#growthSlack,
    );
  }
  #resultCopySourceProgress: number | null = null;
  resultCopySourceProgress(bytes: number): void {
    this.#resultCopySourceProgress = bytes;
  }
  resultCopies(copies: number): void {
    if (!Number.isSafeInteger(copies) || copies < 1)
      throw new TypeError("Positive result multiplicity required");
    this.#resultCopies = copies;
  }
  #resultWindow(): number {
    // A buffered producer cannot offer a frame before its delivery handoff. Keep only
    // bounded empty-sink metadata, rather than an unused full consumer byte window.
    return this.#resultProgressOverride === 0 ? Math.min(this.#window, 4096) : this.#window;
  }
  #tighterResultRow(inputs: number, largest: number, slots: readonly number[]): number | null {
    if (this.#resultProgressOverride !== null) return null;
    const row = this.#resultRowProgress?.(slots, largest) ?? null;
    // Refine an existing complete recipe; a looser output estimate must not raise
    // the minimum/peak of a fitting query. Opaque/expanding delivery stays unchanged.
    return row !== null &&
      Math.max(MAX_BLOB_BYTES, row) <= this.#fallbackResultStep(inputs, largest)
      ? row
      : null;
  }
  #resultStep(inputs: number, largestInput: number, slots: readonly number[] = []): number {
    const row = this.#tighterResultRow(inputs, largestInput, slots);
    return row === null
      ? this.#fallbackResultStep(inputs, largestInput)
      : Math.max(MAX_BLOB_BYTES, row);
  }
  #fallbackResultStep(inputs: number, largestInput: number): number {
    // Multiply the source/owned-value envelope, not the one-MiB blob floor.
    // Hundreds of tiny aliases need their actual frame minimum, not hundreds of blob loans.
    return this.#resultProgressOverride === null
      ? this.#stepBytes(
          inputs,
          Math.max(
            largestInput,
            this.#operatorLiteralBytes > 0
              ? this.#operatorLiteralBytes + 4096 + 256 * this.plan.columns.length
              : 0,
          ),
          this.#resultCopies,
          this.#resultCopySourceProgress ?? this.#progressBytes,
        )
      : // A JSON checkpoint bounds its complete serialized row, including repeated aliases.
        // Only an input/source envelope needs the separate logical copy multiplier.
        this.#resultProgressOverride * (this.#resultOwnedMinimum ? 1 : this.#resultCopies);
  }
  /** A checkpoint producer emits no result until its delivery phase is admitted. */
  resultProgressEnvelope(bytes: number | null, owned = false): void {
    this.#resultProgressOverride = bytes;
    this.#resultOwnedMinimum = owned;
  }
  retainedProgressEnvelope(): number {
    let bytes = 0;
    for (const step of this.#operatorProgress.values()) bytes = Math.max(bytes, step);
    return bytes;
  }
  #readStep: ReadStep | null = null;
  #operatorProgress = new Map<number, number>();
  #readBounds = new Map<number, ReadBound>();
  #nonRetainingReads = false;
  #queryRoots: QueryRoots | undefined;

  /** Query-local selected facts may borrow owner validation; no sizing lease survives admission. */
  metadataCaches(retain = true, sources?: readonly RootColumns[]): QueryRoots | undefined {
    if (!retain || this.#closed || this.signal.aborted) return undefined;
    if (this.#queryRoots !== undefined) return this.#queryRoots;
    if (sources === undefined) return undefined;
    const roots = QueryRoots.forReader(this.request.governor, sources, this.view.pages);
    this.#queryRoots = roots;
    // Global cleanup runs last, after phase readers, in-flight jobs and caller pins drain.
    this.#dispose.unshift(() => {
      roots.close();
      this.#queryRoots = undefined;
    });
    return roots;
  }

  atomicResult(grant: DeferredGrant, window: number): void {
    this.#atomicResult = grant;
    this.#window = Math.min(window, RESULT_FRAME_TARGET_BYTES);
  }
  readMinimum(bytes: number, progress: number, savedProgress = 0): void {
    this.#readBytes = bytes;
    this.#progressBytes = progress;
    this.#savedProgressBytes = savedProgress;
  }
  readPages(pages: Snapshot["pages"]): Snapshot["pages"] {
    return this.#readBytes === 0
      ? pages
      : (this.#readStep?.reader(pages, this.#nonRetainingReads) ?? pages);
  }
  readBoundary(): void {
    this.#readStep?.beforeDownstream();
  }
  readonly #sourceRecipes = new Map<number, TableReadRecipe>();
  /** Sizing and execution must use the SAME retained source; never reopen after publication. */
  sourceView(): Pick<Snapshot, "table" | "pages"> {
    if (this.#sourceRecipes.size === 0) return this.view;
    return {
      pages: this.view.pages,
      table: (id, options) => this.#sourceRecipes.get(id)?.table ?? this.view.table(id, options),
    };
  }
  retryResidentSources(
    sources: ReadonlyArray<{ readonly tableId: number }>,
  ): MaybePromise<boolean> {
    let changed = false;
    const remaining = sources.values();
    const install = (tableId: number, table: TableSnapshot): void => {
      this.signal.throwIfAborted();
      if (this.#closed) throw new QueryCancelledError();
      const recipe = table.prepareRead?.();
      if (!recipe) return;
      // Retain the source through producer handoffs until whole-query read jobs drain.
      this.#sourceRecipes.set(tableId, recipe);
      changed = true;
    };
    const next = (): MaybePromise<boolean> => {
      for (let item = remaining.next(); !item.done; item = remaining.next()) {
        this.signal.throwIfAborted();
        const { tableId } = item.value;
        if (this.#sourceRecipes.has(tableId)) continue;
        const table = this.view.table(tableId, {
          budget: { allocate: () => false, free: () => undefined },
        });
        if (table instanceof Promise)
          return table.then((table) => {
            install(tableId, table);
            return next();
          });
        install(tableId, table);
      }
      return changed;
    };
    return next();
  }

  table(id: number, budget: ByteBudget): MaybePromise<TableSnapshot> {
    const wrap = (table: TableSnapshot) =>
      readTable(
        table,
        this.readPages(this.view.pages),
        budget,
        () => {
          this.signal.throwIfAborted();
          if (this.view.released) throw new SnapshotReleasedError();
        },
        this.metadataCaches(!this.#nonRetainingReads),
      );
    const table = this.#sourceRecipes.get(id)?.table ?? this.view.table(id, { budget });
    return table instanceof Promise ? table.then(wrap) : wrap(table);
  }

  sourceBounds(bounds: ReadonlyMap<number, ReadBound>): void {
    this.#readBounds = new Map(bounds);
    this.#nonRetainingReads = bounds.size === 1 && bounds.values().next().value?.retain === false;
  }
  sourceBatchRows(id: number): number | undefined {
    return this.#readBounds.get(id)?.batchRows;
  }
  sourceWindow(id: number): number {
    return this.#readBounds.get(id)?.windowBytes ?? 0;
  }
  sourceDomain(id: number) {
    return this.#readBounds.get(id)?.domain;
  }
  #expressionProgress = 0;
  #expressionMultiplicity = 1;
  #sourceMultiplicity = 1;
  #parameterProgress = false;
  #operatorLiteralBytes = 0;
  readonly #operatorParameterColumns = new Map<number, number>();
  readonly #operatorCopies = new Map<number, number>();
  operatorParameterColumns(id: number, columns: number, copies?: number): void {
    this.#operatorParameterColumns.set(id, columns);
    if (copies !== undefined) this.#operatorCopies.set(id, copies);
  }
  operatorLiteralProgress(bytes: number): void {
    this.#operatorLiteralBytes = bytes;
  }
  #aggregateLanes = new Map<
    number,
    { kind: "hybrid" | "external" | "restore"; layout: AggregateProgressLayout }
  >();
  #parameterSlotBytes: readonly number[] | undefined;
  #sharedAggregateSteps = new Set<number>();
  aggregateSharedStep(rel: OperatorIdentity): boolean {
    return this.#sharedAggregateSteps.has(rel.id);
  }
  /** Complete variable-output envelope; ordinary project/filter leave it at zero. */
  progressEnvelope(bytes: number, multiplicity = 1, sourceMultiplicity = multiplicity): void {
    this.#expressionProgress = bytes;
    this.#expressionMultiplicity = multiplicity;
    this.#sourceMultiplicity = sourceMultiplicity;
    this.#parameterProgress = true;
  }
  readonly #helperOnlyProgress = new Set<number>();
  /** This parent allocates fixed shares; all of its borrowers are explicit helpers. */
  helperOnlyProgress(id: number): void {
    this.#helperOnlyProgress.add(id);
  }
  #groupKeyProgress:
    | ((
        id: number,
        loanSlot: number | undefined,
        slots: readonly number[],
        largest: number,
        minimum: number,
        ordinary: number,
      ) => number | { complete: number })
    | null = null;
  /** Only the native consumer's lane owns its decoded semantic grouping operands. */
  groupKeyProgress(
    bound: (
      id: number,
      loanSlot: number | undefined,
      slots: readonly number[],
      largest: number,
      minimum: number,
      ordinary: number,
    ) => number | { complete: number },
  ): void {
    this.#groupKeyProgress = bound;
  }
  #stepBytes(
    inputs: number,
    largestInput: number,
    copies = 1,
    readProgress = this.#progressBytes,
    savedCopies = copies,
  ): number {
    // ReadBound.progress already covers a complete source prefix, not one cell.
    // Add newly materialized inline/literal/owned-parameter columns; multiply only
    // for repeated group-key positions, never again for the number of source columns.
    const current = this.#parameterProgress
      ? readProgress * this.#sourceMultiplicity +
        this.#expressionProgress +
        4 * inputs * this.#expressionMultiplicity
      : Math.max(readProgress, largestInput);
    return Math.max(MAX_BLOB_BYTES, savedCopies * this.#savedProgressBytes, copies * current);
  }
  #operatorStepBytes(
    rel: OperatorIdentity,
    inputs: number,
    largestInput: number,
    slots: readonly number[] = [],
  ): number {
    const copies = this.#operatorCopies.get(rel.id) ?? progressCopies(rel);
    const columns = this.#columnProgress.get(rel.id)?.(slots, largestInput);
    if (columns !== undefined && columns !== null)
      return Math.ceil(
        Math.max(
          MAX_BLOB_BYTES,
          this.#savedProgressBytes,
          copies * (this.#progressBytes / 4 + columns.row + columns.repeated),
        ),
      );
    if (copies === 1) return this.#stepBytes(inputs, largestInput);
    // ReadBound.progress and the aggregate expression/parameter envelope already
    // cover FOUR copies. A raw parameter largestSlot does not. Normalize those
    // units before applying this operator's compound bound, and never multiply
    // a checkpoint's already complete restore/replay envelope again.
    const variableColumns = this.#operatorParameterColumns.get(rel.id) ?? 1;
    return Math.ceil(
      Math.max(
        copies * variableColumns * this.#operatorLiteralBytes,
        this.#stepBytes(
          Math.max(inputs, (largestInput * variableColumns) / this.#expressionMultiplicity),
          largestInput * variableColumns,
          this.#parameterProgress ? copies / 4 : copies,
          this.#parameterProgress ? this.#progressBytes : this.#progressBytes / 4,
          1,
        ),
      ),
    );
  }
  readonly #columnProgress = new Map<
    number,
    (slots: readonly number[], largest: number) => { row: number; repeated: number } | null
  >();
  operatorColumnProgress(
    id: number,
    bound: (slots: readonly number[], largest: number) => { row: number; repeated: number } | null,
  ): void {
    this.#columnProgress.set(id, bound);
  }
  aggregateProgress(id: number, kind: string, layout: AggregateProgressLayout): void {
    if (kind === "hybrid" || kind === "external" || kind === "restore")
      this.#aggregateLanes.set(id, { kind, layout });
    else this.#aggregateLanes.delete(id);
  }
  #laneBytes(
    rel: OperatorIdentity,
    slot: number | undefined,
    baseline: number,
    slots: readonly number[] | undefined = this.#parameterSlotBytes,
    largest = 0,
    minimum = 0,
  ): number {
    if (slot === undefined && rel.op === "aggregate" && this.#helperOnlyProgress.has(rel.id))
      return 0;
    const recipe = this.#aggregateLanes.get(rel.id);
    const ordinary = recipe
      ? aggregateLaneBytes(recipe.layout, recipe.kind, slot, baseline, slots)
      : baseline;
    return scopedProgress(
      ordinary,
      this.#groupKeyProgress?.(rel.id, slot, slots ?? [], largest, minimum, baseline) ?? 0,
    );
  }
  operatorStep(rel: OperatorIdentity): number {
    return this.#operatorProgress.get(rel.id) ?? 0;
  }

  minimumBytes(
    entries: ReadonlyArray<{ rel: OperatorIdentity; minimum: number; loanSlots?: number }>,
    inputs = 0,
    largestInput = 0,
    slots: readonly number[] = [],
  ): number {
    if (this.#atomicResult === null)
      return inputs + entries.reduce((sum, entry) => sum + entry.minimum, 0);
    let protectedBytes = 0;
    for (const entry of entries) {
      const canBorrow =
        (entry.loanSlots ?? 0) > 0 ||
        ["sort", "aggregate", "join", "window", "setOp"].includes(entry.rel.op);
      if (!canBorrow) continue;
      const progress = this.#operatorStepBytes(entry.rel, inputs, largestInput, slots);
      const lanes = Array.from({ length: entry.loanSlots ?? 0 }, (_, slot) =>
        this.#laneBytes(entry.rel, slot, progress, slots, largestInput, entry.minimum),
      );
      const recipe = this.#aggregateLanes.get(entry.rel.id);
      const shared = recipe ? aggregateSharedBytes(recipe.layout, recipe.kind, slots, lanes) : null;
      protectedBytes +=
        this.#laneBytes(entry.rel, undefined, progress, slots, largestInput, entry.minimum) +
        (shared ?? lanes.reduce((sum, bytes) => sum + bytes, 0));
    }
    return (
      inputs +
      entries.reduce((sum, entry) => sum + entry.minimum, 0) +
      protectedBytes +
      this.#readBytes +
      (entries.length === 0
        ? 0
        : Math.max(
            this.#resultWindow(),
            this.#resultStep(inputs, largestInput, slots) +
              (this.#resultOwnedMinimum ? resultFrameMetadataBytes(this.plan.columns.length) : 0),
          ))
    );
  }

  /** Reserve all operator and nested loan slots in the same atomic minimum. */
  reserve(
    pipelines: ReadonlyArray<{
      readonly rel: OperatorIdentity;
      readonly minimum: number;
      readonly loanSlots?: number;
    }>,
  ): MaybePromise<void> {
    // Zero-byte loan owners must be admitted before holding fixed buffers: a late admission
    // could queue behind another pipeline waiting for those same fixed bytes.
    const entries: Reservation[] = pipelines.flatMap((entry) => [
      { rel: entry.rel, minimum: entry.minimum },
      ...Array.from({ length: entry.loanSlots ?? 0 }, (_, loanSlot) => ({
        rel: entry.rel,
        minimum: 0,
        loanSlot,
      })),
    ]);
    if (!needsParameterSizing(this.plan, this.request.parameters))
      return this.#sizeAndReserve(entries);
    const label = `query#${String(this.request.queryId)}/parameter-sizing`;
    const admitted = admitWorkingSet(this.request.governor, {
      label,
      includesProgress: true,
      signal: this.signal,
      grants: [{ label, minimumBytes: PARAMETER_SIZING_BYTES, spillable: false }],
    });
    const adopt = (parts: readonly Grant[]) => {
      const grant = parts[0];
      if (!grant) throw new Error("parameter sizing did not receive its native minimum");
      return this.#adopt(grant, undefined);
    };
    const scratch = admitted instanceof Promise ? admitted.then(adopt) : adopt(admitted);
    return scratch instanceof Promise
      ? scratch.then((grant) => this.#sizeAndReserve(entries, grant))
      : this.#sizeAndReserve(entries, scratch);
  }

  #sizeAndReserve(entries: readonly Reservation[], scratch?: Grant): MaybePromise<void> {
    let binding: MaybePromise<ParameterBinding>;
    try {
      binding = prepareParameters(
        this.plan,
        this.request.parameters,
        this.signal,
        this.request.governor.capBytes,
        scratch,
      );
    } catch (error) {
      scratch?.close();
      throw error;
    }
    const next = (value: ParameterBinding) => {
      scratch?.close();
      return this.#reserveBound(entries, value);
    };
    return binding instanceof Promise
      ? binding.then(next, (error: unknown) => {
          scratch?.close();
          throw error;
        })
      : next(binding);
  }

  #reserveBound(
    entries: readonly Reservation[],
    binding: ParameterBinding,
    idleCredit?: Grant,
  ): MaybePromise<void> {
    this.#parameterSlotBytes = binding.slotBytes;
    const atomic = this.#atomicResult !== null;
    const progress = (rel: OperatorIdentity) =>
      atomic
        ? this.#operatorStepBytes(
            rel,
            binding.logicalBytes ?? binding.bytes,
            binding.largestSlot,
            binding.slotBytes,
          )
        : 0;
    // A helper slot explicitly declares bounded variable output, including project/filter
    // wrappers. Fund each native borrower together, also after a phase restore. A parent
    // that only splits fixed shares does not need another unused progress lane.
    const explicitLoans = new Set(
      entries.filter((entry) => entry.loanSlot !== undefined).map((entry) => entry.rel.id),
    );
    const canBorrow = (rel: OperatorIdentity) =>
      explicitLoans.has(rel.id) ||
      rel.op === "sort" ||
      rel.op === "aggregate" ||
      rel.op === "join" ||
      rel.op === "window" ||
      rel.op === "setOp";
    this.#operatorProgress = new Map(
      entries.filter(({ rel }) => canBorrow(rel)).map(({ rel }) => [rel.id, progress(rel)]),
    );
    const emittedRow = this.#tighterResultRow(
      binding.logicalBytes ?? binding.bytes,
      binding.largestSlot,
      binding.slotBytes ?? [],
    );
    const window =
      atomic && entries.length > 0
        ? Math.max(
            this.#resultWindow(),
            this.#resultOwnedMinimum
              ? this.#resultStep(
                  binding.logicalBytes ?? binding.bytes,
                  binding.largestSlot,
                  binding.slotBytes,
                ) + resultFrameMetadataBytes(this.plan.columns.length)
              : emittedRow === null
                ? 0
                : Math.max(MAX_BLOB_BYTES, emittedRow),
          )
        : 0;
    const resultProgress =
      atomic && entries.length > 0
        ? Math.max(
            0,
            this.#resultStep(
              binding.logicalBytes ?? binding.bytes,
              binding.largestSlot,
              binding.slotBytes,
            ) - window,
          )
        : 0;
    const prefix = `query#${String(this.request.queryId)}`;
    const parts: Array<{ label: string; minimumBytes: number; spillable: boolean }> = [];
    const add = (label: string, minimumBytes: number, spillable = false) => {
      const index = parts.length;
      parts.push({ label: `${prefix}/${label}`, minimumBytes, spillable });
      return index;
    };
    const parameterPart = add("parameters", binding.bytes);
    const ioPart = add("read-step", atomic ? this.#readBytes : 0);
    const resultParts = atomic
      ? {
          base: add("result/minimum", window),
          progress: add("result/progress", resultProgress),
          extra: add("result/growth", 0, true),
        }
      : null;
    const key = (entry: Reservation) =>
      `${entry.rel.op}#${String(entry.rel.id)}${entry.loanSlot === undefined ? "" : `/loan/${String(entry.loanSlot)}`}`;
    const sharedParts = new Map<number, number>();
    for (const entry of entries) {
      if (entry.loanSlot !== undefined) continue;
      const recipe = this.#aggregateLanes.get(entry.rel.id);
      if (!recipe) continue;
      const baseline = this.#operatorProgress.get(entry.rel.id) ?? 0;
      const lanes = entries
        .filter((e) => e.rel.id === entry.rel.id && e.loanSlot !== undefined)
        .map((e) =>
          this.#laneBytes(
            e.rel,
            e.loanSlot,
            baseline,
            binding.slotBytes,
            binding.largestSlot,
            e.minimum,
          ),
        );
      const peak = aggregateSharedBytes(
        recipe.layout,
        recipe.kind,
        this.#parameterSlotBytes,
        lanes,
      );
      if (peak !== null) sharedParts.set(entry.rel.id, add(`${key(entry)}/step-peak`, peak));
    }
    this.#sharedAggregateSteps = new Set(sharedParts.keys());
    const operators = entries.map((entry) => {
      const capacity = canBorrow(entry.rel)
        ? this.#laneBytes(
            entry.rel,
            entry.loanSlot,
            this.#operatorProgress.get(entry.rel.id) ?? 0,
            binding.slotBytes,
            binding.largestSlot,
            entry.minimum,
          )
        : 0;
      return {
        entry,
        capacity,
        base: add(`${key(entry)}/minimum`, entry.minimum),
        progress: add(`${key(entry)}/progress`, sharedParts.has(entry.rel.id) ? 0 : capacity),
        extra: add(`${key(entry)}/growth`, 0, true),
      };
    });
    const resultSaving =
      this.#resultProgressOverride === null
        ? Math.max(
            0,
            this.#fallbackResultStep(binding.logicalBytes ?? binding.bytes, binding.largestSlot) -
              window -
              resultProgress,
          )
        : 0;
    this.#growthSlack = resultSaving;
    const started = performance.now();
    const admitted = admitWorkingSet(this.request.governor, {
      label: `${prefix}/fixed-buffers`,
      ...(idleCredit === undefined ? {} : { idleCredit }),
      grants: parts,
      includesProgress: atomic,
      signal: this.signal,
    });
    const install = (grants: readonly Grant[]): MaybePromise<void> => {
      if (this.#closed || this.signal.aborted) {
        for (const grant of grants) grant.close();
        this.signal.throwIfAborted();
        throw new TypeError("admission completed after query disposal");
      }
      const retained = new Set<Grant>();
      // Producer cleanup returns only unclaimed parts. Frames and their source leases can
      // outlive execution; each phase returns its own native parts when those buffers close.
      this.dispose(() => {
        for (const grant of grants) if (!retained.has(grant)) grant.close();
      });
      for (const grant of grants) this.#meter(grant);
      const at = (index: number) => {
        const grant = grants[index];
        if (!grant) throw new TypeError("missing native minimum part");
        return grant;
      };
      const sharedUses = new Map<number, number>();
      const phase = (
        base: number,
        extra: number,
        extraProgress: number,
        bounded: boolean,
        shared?: { index: number; capacity: number },
      ) => {
        const fixed = at(base),
          growth = at(extra),
          lane = at(shared?.index ?? extraProgress);
        const capacity = shared?.capacity ?? parts[extraProgress]?.minimumBytes ?? 0;
        // Installation is synchronous. Count constructed owners, so an interrupted
        // install cannot leave references for helpers that were never created.
        if (shared) sharedUses.set(shared.index, (sharedUses.get(shared.index) ?? 0) + 1);
        const share = new ReservedGrant(
          fixed,
          growth,
          parts[base]?.minimumBytes ?? 0,
          atomic ? new StepLoans(lane, capacity, parts[extraProgress]?.label ?? lane.label) : null,
          bounded,
          () => {
            if (shared) {
              const left = (sharedUses.get(shared.index) ?? 1) - 1;
              sharedUses.set(shared.index, left);
              if (left > 0) return;
            }
            lane.close();
          },
          true,
        );
        this.#phaseParts.set(share, lane);
        retained.add(fixed);
        retained.add(growth);
        retained.add(lane);
        return share;
      };
      if (resultParts && this.#atomicResult) {
        this.#atomicResult.install(
          phase(resultParts.base, resultParts.extra, resultParts.progress, false),
          () => undefined,
        );
      }
      if (atomic) {
        const io = at(ioPart);
        this.#readStep = new ReadStep(io, this.signal);
        this.dispose(() => {
          if (io.allocated !== 0) throw new Error("read-step credit still pinned at close");
          io.close();
        });
      }
      const parameterGrant = at(parameterPart);
      const bound = binding.bind(parameterGrant, this.request.statementTime);
      this.dispose(() => parameterGrant.close());
      const attach = (slots: ParameterSlots) => {
        this.params.slots = slots.slots;
        for (const { entry, base, progress: lane, extra, capacity } of operators) {
          const sharedIndex = sharedParts.get(entry.rel.id);
          const share = phase(
            base,
            extra,
            lane,
            this.#readBounds.has(entry.rel.id),
            sharedIndex === undefined ? undefined : { index: sharedIndex, capacity },
          );
          this.#meter(share, entry.rel, false);
          this.#reserved.set(key(entry), share);
          this.dispose(() => share.close());
        }
      };
      return bound instanceof Promise ? bound.then(attach) : attach(bound);
    };
    return admitted instanceof Promise
      ? admitted.then((grants) => {
          this.#wait += performance.now() - started;
          return install(grants);
        })
      : install(admitted);
  }

  constructor(
    readonly plan: PhysicalPlan,
    readonly request: ExecuteRequest,
    readonly view: Snapshot,
    readonly signal: AbortSignal,
  ) {
    // No owned parameter buffers exist until reserve() admits inputs and operators atomically.
    if (plan.clockSlot !== null && plan.parameters[plan.clockSlot]?.kind !== "timestamp")
      throw new TypeError("invalid statement clock slot");
  }

  /** Bind an active node's own bounds once, before deciding whether to open its input.
   * PostgreSQL recompute_limits checks OFFSET before LIMIT. Parameter type/range checks
   * precede those sign checks; a child's bounds are never requested when its parent is zero. */
  bounds(rel: Extract<Rel, { op: "limit" | "sort" }>): {
    readonly limit: number | null;
    readonly offset: number;
  } {
    const prior = this.#bounds.get(rel.id);
    if (prior !== undefined) return prior;
    if (this.request.parameters.length !== this.plan.placeholderCount)
      throw new ParameterBindError(
        `Expected ${String(this.plan.placeholderCount)} parameters, received ${String(this.request.parameters.length)}`,
        null,
      );
    const read = (value: Bound | null, offset: boolean): number | null => {
      if (value === null) return offset ? 0 : null;
      if (value.kind === "const") return value.value;
      const integer = this.boundParameter(value.slot);
      if (integer === null) return offset ? 0 : null;
      // A finite relation cannot distinguish larger positive int8 bounds. Keep negative
      // values negative until the clause's SQLSTATE-specific sign check below.
      return typeof integer === "bigint" && integer > BigInt(Number.MAX_SAFE_INTEGER)
        ? Number.MAX_SAFE_INTEGER
        : Number(integer);
    };
    const limit = read(rel.limit, false),
      offset = read(rel.offset, true) ?? 0;
    if (offset < 0)
      throw new ExecutionError("OFFSET must not be negative", "MINNOW_INVALID_BOUND", "2201X");
    if (limit !== null && limit < 0)
      throw new ExecutionError("LIMIT must not be negative", "MINNOW_INVALID_BOUND", "2201W");
    if (!Number.isSafeInteger(offset) || (limit !== null && !Number.isSafeInteger(limit)))
      throw new TypeError("invalid LIMIT/OFFSET constant");
    const values = { limit, offset };
    this.#bounds.set(rel.id, values);
    return values;
  }

  /** Reservation pruning needs integer bounds before materializing any vectors. */
  boundParameter(slot: number): number | bigint | null {
    const type = this.plan.parameters[slot];
    const value = this.request.parameters[slot];
    if (value === null) return null;
    if (type?.kind !== "int32" && type?.kind !== "int64")
      throw new TypeError("LIMIT/OFFSET is not an integer");
    if (typeof value !== "bigint" && (typeof value !== "number" || !Number.isSafeInteger(value)))
      throw new ParameterBindError(`$${String(slot + 1)}: integer value expected`, slot);
    const bits = type.kind === "int64" ? 64 : type.bits;
    const low = bits === 64 ? -(1n << 63n) : bits === 16 ? -32768 : -2147483648;
    const high = bits === 64 ? (1n << 63n) - 1n : bits === 16 ? 32767 : 2147483647;
    if (value < low || value > high)
      throw new ParameterBindError(
        `$${String(slot + 1)}: integer value out of range`,
        slot,
        "22003",
      );
    return value;
  }

  profile(rel: OperatorIdentity): MutableOperatorProfile {
    const identity = rel.profileAs ?? rel;
    let profile = this.profiles.get(identity.id);
    if (profile === undefined) {
      profile = {
        relId: identity.id,
        op: identity.op,
        rowsOut: 0,
        batchesOut: 0,
        peakHeldBytes: 0,
        spilledBytes: 0,
        spillPasses: 0,
        pagesRead: 0,
        pagesPruned: 0,
        runtimeFilter: null,
      };
      this.profiles.set(identity.id, profile);
    }
    return profile;
  }

  /** Independent loan owner installed by the atomic plan reservation. */
  loan(rel: OperatorIdentity, slot: number): Grant {
    const grant = this.#reserved.get(`${rel.op}#${String(rel.id)}/loan/${String(slot)}`);
    if (grant === undefined) throw new TypeError("Missing reserved operator loan slot");
    return grant;
  }

  admit(
    label: string,
    minimumBytes: number,
    spillable: boolean,
    rel?: OperatorIdentity,
  ): MaybePromise<Grant> {
    this.signal.throwIfAborted();
    const reserved = this.#reserved.get(label);
    if (reserved !== undefined) {
      this.#reserved.delete(label);
      if (rel !== undefined) {
        const profile = this.profile(rel);
        profile.peakHeldBytes = Math.max(profile.peakHeldBytes, reserved.held);
      }
      return reserved;
    }
    const governor = this.request.governor;
    const options = {
      label: `query#${String(this.request.queryId)}/${label}`,
      minimumBytes,
      spillable,
    };
    const immediate = governor.tryAdmit(options);
    if (immediate !== null) return this.#adopt(immediate, rel);
    const start = performance.now();
    return governor.admit({ ...options, signal: this.signal }).then((grant) => {
      this.#wait += performance.now() - start;
      return this.#adopt(grant, rel);
    });
  }

  /** The result grant is tracked but survives pipeline disposal until result delivery. */
  resultGrant(grant: Grant): void {
    this.#meter(grant, undefined, this.#atomicResult === null);
  }

  /** One source-to-sink driver, independent of how many fixed or loan grants it owns. */
  pipeline(): void {
    this.#pipelines++;
  }

  dispose(cleanup: () => MaybePromise<void>): void {
    if (this.#closed) throw new TypeError("resource installed after query disposal");
    if (!this.#scope) {
      this.#dispose.push(cleanup);
      return;
    }
    let live = true;
    const once = () => {
      if (!live) return;
      live = false;
      const index = this.#dispose.indexOf(once);
      if (index >= 0) this.#dispose.splice(index, 1);
      return cleanup();
    };
    this.#dispose.push(once);
    this.#scope.jobs.push(once);
  }

  onAbort(abort: (reason: unknown) => void): void {
    if (this.#aborted) abort(this.#reason);
    else if (!this.#scope) this.#aborters.push(abort);
    else {
      let live = true;
      const once = (reason: unknown) => {
        if (!live) return;
        live = false;
        const index = this.#aborters.indexOf(once);
        if (index >= 0) this.#aborters.splice(index, 1);
        abort(reason);
      };
      this.#aborters.push(once);
      this.#scope.aborters.push(once);
    }
  }

  abort(reason: unknown): void {
    if (this.#aborted) return;
    this.#aborted = true;
    this.#reason = reason;
    // An abort must still reach every resource if one operator's cleanup itself fails.
    for (const abort of this.#aborters.splice(0)) {
      try {
        abort(reason);
      } catch {
        /* retain the execution's first failure */
      }
    }
  }

  /** Called only after the old graph unwound and parameter copies were checkpointed.
   * Neither this operation nor resource scopes release the owner-held snapshot pin. */
  async suspendProducer(sink: ResultSink, idleCredit: Grant): Promise<void> {
    if (!this.#handoff || this.#scope !== null || this.#atomicResult === null)
      throw new Error("Producer graph must retire before phase admission");
    await sink.suspendBudget();
    const target = this.#atomicResult.detach();
    try {
      if (!(target instanceof ReservedGrant))
        throw new Error("Phase result has no native reservation");
      const lane = this.#phaseParts.get(target);
      if (!lane) throw new Error("Missing native phase result parts");
      target.parkIdle(this.request.governor, idleCredit, lane);
      this.params.slots = [];
      await this.#releaseResources();
      this.#readStep = null;
      this.#readBytes = 0;
      this.#progressBytes = 0;
      this.#savedProgressBytes = 0;
      this.#reserved.clear();
      this.#phaseParts.clear();
      // Peak statistics are scalar now. Do not retain closed grant wrappers, their
      // metering closures and per-phase arrays for every later restore admission.
      this.#grants.length = 0;
      this.#operatorGrants.clear();
    } catch (error) {
      target.close();
      throw error;
    }
  }

  /** A checkpoint-backed binding restores owned inputs, never request.parameters. */
  restorePhase(
    pipelines: ReadonlyArray<{
      readonly rel: OperatorIdentity;
      readonly minimum: number;
      readonly loanSlots?: number;
    }>,
    binding: ParameterBinding,
    idleCredit: Grant,
  ): MaybePromise<void> {
    if (!this.#handoff || this.#scope !== null) throw new Error("No retired producer phase");
    const entries: Reservation[] = pipelines.flatMap((entry) => [
      { rel: entry.rel, minimum: entry.minimum },
      ...Array.from({ length: entry.loanSlots ?? 0 }, (_, loanSlot) => ({
        rel: entry.rel,
        minimum: 0,
        loanSlot,
      })),
    ]);
    const admitted = this.#reserveBound(entries, binding, idleCredit);
    return admitted instanceof Promise
      ? admitted.then(() => this.resumePhase())
      : this.resumePhase();
  }
  close(): MaybePromise<void> {
    if (this.#closed) return;
    this.#closed = true;
    this.#bounds.clear();
    this.params.slots = [];
    if (this.#sourceRecipes.size === 0) return this.#releaseResources();
    const closeSources = () => {
      let first: unknown,
        failed = false;
      for (const recipe of this.#sourceRecipes.values()) {
        try {
          recipe.close();
        } catch (error) {
          if (!failed) {
            first = error;
            failed = true;
          }
        }
      }
      this.#sourceRecipes.clear();
      if (failed) throw first;
    };
    const fail = (error: unknown): never => {
      try {
        closeSources();
      } catch {
        /* Preserve the first native cleanup failure. */
      }
      throw error;
    };
    try {
      const work = this.#releaseResources();
      if (work instanceof Promise) return work.then(closeSources, fail);
    } catch (error) {
      fail(error);
    }
    closeSources();
  }
  #releaseResources(): MaybePromise<void> {
    const jobs = this.#dispose.splice(0).reverse();
    let first: unknown;
    let failed = false;
    const run = (at: number): MaybePromise<void> => {
      for (let i = at; i < jobs.length; i++) {
        try {
          const result = jobs[i]?.();
          if (result instanceof Promise)
            return result
              .catch((error: unknown) => {
                if (!failed) {
                  first = error;
                  failed = true;
                }
              })
              .then(() => run(i + 1));
        } catch (error) {
          if (!failed) {
            first = error;
            failed = true;
          }
        }
      }
      if (failed) throw first;
      this.#aborters.length = 0;
    };
    return this.#readStep?.busy ? this.#readStep.closed.then(() => run(0)) : run(0);
  }

  summary(synchronous: boolean): ExecutionProfile {
    return {
      operators: [...this.profiles.values()].map((p) => ({ ...p })),
      pipelines: this.#pipelines,
      memoryWaitMs: this.#wait,
      peakHeldBytes: this.#peak,
      synchronous,
    };
  }

  #adopt(grant: Grant, rel: OperatorIdentity | undefined, close = true): Grant {
    if (this.#closed || this.signal.aborted) {
      grant.close();
      this.signal.throwIfAborted();
      throw new TypeError("admission completed after query disposal");
    }
    this.#meter(grant, rel);
    if (close) this.dispose(() => grant.close());
    return grant;
  }

  #meter(grant: Grant, rel?: OperatorIdentity, account = true): void {
    // A reserved share is already included in its fixed/extra real grants. Observe its
    // operator peak without double-counting the execution's holdings or loan wait time.
    if (account) this.#grants.push(grant);
    const profile = rel === undefined ? undefined : this.profile(rel);
    let operatorGrants: Grant[] | undefined;
    if (profile !== undefined) {
      operatorGrants = this.#operatorGrants.get(profile.relId);
      if (operatorGrants === undefined) {
        operatorGrants = [];
        this.#operatorGrants.set(profile.relId, operatorGrants);
      }
      operatorGrants.push(grant);
    }
    const observe = () => {
      this.#peak = Math.max(
        this.#peak,
        this.#grants.reduce((sum, g) => sum + g.held, 0),
      );
      if (profile !== undefined && operatorGrants !== undefined)
        profile.peakHeldBytes = Math.max(
          profile.peakHeldBytes,
          operatorGrants.reduce((sum, held) => sum + held.held, 0),
        );
    };
    const allocate = grant.allocate.bind(grant),
      grow = grant.tryGrow.bind(grant),
      borrow = grant.borrow.bind(grant);
    const optionalGrowth = account && this.#growthSlack > 0 && grant.label.endsWith("/growth");
    grant.allocate = (bytes) => {
      if (
        optionalGrowth &&
        this.#growthSlack > 0 &&
        bytes > Math.max(0, grant.held - grant.allocated) + this.#growthRoom()
      )
        return false;
      const ok = allocate(bytes);
      observe();
      return ok;
    };
    grant.tryGrow = (bytes) => {
      if (optionalGrowth && this.#growthSlack > 0 && bytes > this.#growthRoom()) return false;
      const ok = grow(bytes);
      observe();
      return ok;
    };
    if (optionalGrowth && grant.reclaimRoom !== undefined) {
      const reclaim = grant.reclaimRoom.bind(grant);
      grant.reclaimRoom = (bytes, signal) => {
        const free = Math.max(0, grant.held - grant.allocated - grant.loaned);
        const needed = Math.max(0, bytes - free);
        if (
          this.#growthSlack > 0 &&
          needed > this.#growthRoom() &&
          needed <=
            this.request.governor.capBytes -
              this.request.governor.reserveBytes -
              this.request.governor.usedBytes
        )
          return Promise.resolve(false);
        return reclaim(bytes, signal);
      };
    }
    grant.borrow = (bytes, signal) => {
      const start = performance.now();
      return borrow(bytes, signal).then((loan) => {
        if (account) this.#wait += performance.now() - start;
        observe();
        return loan;
      });
    };
    observe();
  }
}

function hasStatement(request: ExecuteRequest): boolean {
  return request.statement !== null;
}

export class QueryExecution implements Execution {
  readonly results: ResultChannel;
  readonly delivered: Promise<void>;
  readonly executed: Promise<ExecutionSummary>;
  readonly #controller = new AbortController();
  readonly #resolve: (summary: ExecutionSummary) => void;
  readonly #reject: (reason: unknown) => void;
  #state: ExecutionState = "running";
  #summary: ExecutionSummary | null = null;
  #runtime: QueryRuntime | null = null;
  #sink: ResultSink | null = null;
  #result: Grant | null = null;
  #async = false;
  #settling = false;
  #delivered = false;
  #cancelled = false;

  constructor(
    readonly plan: PhysicalPlan,
    readonly request: ExecuteRequest,
    program: Program,
  ) {
    if (request.snapshot === null || hasStatement(request))
      throw new TypeError("a query requires a read snapshot");
    const windowBytes = request.resultWindowBytes ?? RESULT_WINDOW_BYTES;
    if (!Number.isSafeInteger(windowBytes) || windowBytes <= 0)
      throw new TypeError("positive result window required");
    if (
      request.resultFrameRows !== undefined &&
      request.resultFrameRows !== null &&
      (!Number.isSafeInteger(request.resultFrameRows) || request.resultFrameRows <= 0)
    )
      throw new TypeError("positive result frame row limit required");
    let resolve: ((summary: ExecutionSummary) => void) | undefined,
      reject: ((reason: unknown) => void) | undefined;
    this.executed = new Promise<ExecutionSummary>((yes, no) => {
      resolve = yes;
      reject = no;
    });
    this.#resolve = resolve as (summary: ExecutionSummary) => void;
    this.#reject = reject as (reason: unknown) => void;
    this.executed.catch(() => undefined);
    this.results = new ResultChannel(plan.columns, {
      windowBytes,
      onCancel: (reason) => this.cancel(reason),
    });
    const onAbort = () => this.cancel(request.signal?.reason);
    this.delivered = this.results.delivered.then(
      () => {
        this.#delivered = true;
        this.#result?.close();
        request.signal?.removeEventListener("abort", onAbort);
      },
      async (error: unknown) => {
        this.#delivered = true;
        this.#sink?.discard(error);
        // A cancelled row reader may still be filling a result-owned backing allocation.
        // Producer cleanup fences its borrowed IO; frame credit alone cannot retire it.
        if (this.#sink?.ownsPartialResult) await this.executed.catch(() => undefined);
        this.#result?.close();
        request.signal?.removeEventListener("abort", onAbort);
        throw error;
      },
    );
    this.delivered.catch(() => undefined);
    request.signal?.addEventListener("abort", onAbort, { once: true });
    try {
      if (request.signal?.aborted === true) this.cancel(request.signal.reason);
      this.#controller.signal.throwIfAborted();
      if (request.snapshot.catalog.version !== plan.catalogVersion)
        throw new PlanStaleError(plan.catalogVersion, request.snapshot.catalog.version);
      this.#runtime = new QueryRuntime(plan, request, request.snapshot, this.#controller.signal);
      if (program.atomic === true) {
        const deferred = new DeferredGrant(`query#${String(request.queryId)}/result`);
        this.#runtime.atomicResult(deferred, windowBytes);
        this.#start(deferred, program);
        return;
      }
      const options = {
        label: `query#${String(request.queryId)}/result`,
        minimumBytes: windowBytes,
        spillable: true,
      };
      const grant = request.governor.tryAdmit(options);
      if (grant === null) {
        this.#async = true;
        request.governor
          .admit({ ...options, signal: this.#controller.signal })
          .then(
            (g) => this.#start(g, program),
            (error: unknown) => this.#fail(error),
          )
          .catch((error: unknown) => this.#fail(error));
      } else this.#start(grant, program);
    } catch (error) {
      this.#fail(error);
    }
  }

  get state(): ExecutionState {
    return this.#state;
  }
  get summary(): ExecutionSummary | null {
    return this.#summary;
  }
  acknowledgeCommit(): void {
    /* Queries stream atExecuted; there is no commit gate. */
  }

  cancel(reason?: unknown): void {
    if (this.#delivered) return;
    const error = new QueryCancelledError(reason);
    if (this.#controller.signal.aborted) {
      this.#sink?.discard(error);
      this.results.cancel(reason);
      return;
    }
    this.#cancelled = true;
    this.#controller.abort(error);
    this.#runtime?.abort(error);
    this.#sink?.discard(error);
    this.results.cancel(reason);
    if (this.#state === "running") this.#state = "cancelled";
  }

  #start(grant: Grant, program: Program): void {
    if (this.#controller.signal.aborted) {
      grant.close();
      this.#fail(this.#controller.signal.reason);
      return;
    }
    this.#result = grant;
    const runtime = this.#runtime;
    if (runtime === null) throw new TypeError("execution runtime missing");
    runtime.resultGrant(grant);
    // An empty result window must not prevent admission of its producer. Allocated frame
    // bytes remain held until credit; unused holdings can be reclaimed at any time.
    grant.onReclaimRequest(() => {
      const unused = grant.held - grant.allocated - grant.loaned;
      if (unused > 0) grant.release(unused);
    });
    this.#sink = new ResultSink(
      this.plan.columns.map((c) => c.type),
      grant,
      this.results,
      runtime.view.observedLsn,
      { deferEnd: true, maxRows: this.request.resultFrameRows ?? null },
    );
    runtime.resultAllocator((bytes) => {
      const sink = this.#sink;
      if (sink === null) throw new Error("Missing result sink");
      return sink.allocateOwnedResult(bytes);
    });
    runtime.onAbort((reason) => this.#sink?.abort(reason));
    try {
      const work = program(runtime, this.#sink);
      if (work instanceof Promise) {
        this.#async = true;
        work
          .then(
            () => this.#finish(),
            (error: unknown) => this.#fail(error),
          )
          .catch((error: unknown) => this.#fail(error));
      } else this.#finish();
    } catch (error) {
      this.#fail(error);
    }
  }

  #finish(): void {
    if (this.#settling) return;
    if (this.#controller.signal.aborted) {
      this.#fail(this.#controller.signal.reason);
      return;
    }
    this.#settling = true;
    const complete = () => {
      if (this.#controller.signal.aborted) {
        this.#settling = false;
        this.#fail(this.#controller.signal.reason);
        return;
      }
      const runtime = this.#runtime;
      if (runtime === null || this.#sink === null) throw new TypeError("execution runtime missing");
      this.#sink.complete();
      this.#summary = {
        rowCount: this.#sink.rowCount,
        counts: { inserted: 0, updated: 0, deleted: 0 },
        requiresLsn: runtime.view.observedLsn,
        profile: runtime.summary(!this.#async),
      };
      this.#state = "executed";
      this.#resolve(this.#summary);
    };
    try {
      const cleanup = this.#runtime?.close();
      if (cleanup instanceof Promise) {
        this.#async = true;
        cleanup
          .then(complete, (error: unknown) => {
            this.#settling = false;
            this.#fail(error);
          })
          .catch((error: unknown) => {
            this.#settling = false;
            this.#fail(error);
          });
      } else complete();
    } catch (error) {
      this.#settling = false;
      this.#fail(error);
    }
  }

  #fail(reason: unknown): void {
    if (this.#settling) return;
    this.#settling = true;
    const raw: unknown = this.#controller.signal.aborted ? this.#controller.signal.reason : reason;
    const error =
      raw instanceof WindowError
        ? new ExecutionError(raw.message, "MINNOW_WINDOW_ERROR", raw.code, { cause: raw })
        : raw;
    if (!this.#controller.signal.aborted) this.#controller.abort(error);
    this.#runtime?.abort(error);
    this.#sink?.abort(error);
    if (this.#cancelled) this.results.cancel(error);
    else this.results.fail(error);
    const reject = () => {
      this.#state = this.#cancelled ? "cancelled" : "failed";
      this.#reject(error);
    };
    try {
      const cleanup = this.#runtime?.close();
      if (cleanup instanceof Promise) cleanup.then(reject, reject).catch(reject);
      else reject();
    } catch {
      reject();
    }
  }
}

/** A known native helper can bound its complete step from its actual columns. */
function scopedProgress(ordinary: number, scoped: number | { complete: number }): number {
  return typeof scoped === "number" ? Math.max(ordinary, scoped) : scoped.complete;
}
