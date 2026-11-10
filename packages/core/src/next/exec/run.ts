/** One query's admissions, resources and the executed/delivered lifecycle. */
import type { MaybePromise } from "../access/key-index.js";
import { SnapshotReleasedError } from "../access/snapshot.js";
import { readTable } from "./read-table.js";
import type { ByteBudget } from "../access/space.js";
import type { Snapshot, TableSnapshot } from "../access/snapshot.js";
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
import { ResultSink } from "./result-sink.js";
import type { ParameterSlots, CallResolver } from "./kernels/compile.js";
import { ReservedGrant } from "./admission.js";
import { DeferredGrant } from "./deferred-grant.js";
import { ReadStep } from "./read-step.js";
import { admitWorkingSet } from "../memory/working-set.js";
import { MAX_BLOB_BYTES } from "../access/space.js";
import { StepLoans } from "../memory/step-loans.js";
import type { ReadBound } from "../page/read-bounds.js";

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

/** Per-execution only: never retained by an immutable prepared plan. */
export class QueryRuntime {
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
  readonly #bounds = new Map<number, { readonly limit: number | null; readonly offset: number }>();
  #atomicResult: DeferredGrant | null = null;
  #window = 0;
  #readBytes = 0;
  #progressBytes = 0;
  #readStep: ReadStep | null = null;
  #operatorProgress = new Map<number, number>();
  #readBounds = new Map<number, ReadBound>();
  #nonRetainingReads = false;

  atomicResult(grant: DeferredGrant, window: number): void {
    this.#atomicResult = grant;
    this.#window = Math.min(window, RESULT_FRAME_TARGET_BYTES);
  }
  readMinimum(bytes: number, progress: number): void {
    this.#readBytes = bytes;
    this.#progressBytes = progress;
  }
  readPages(pages: Snapshot["pages"]): Snapshot["pages"] {
    return this.#readBytes === 0
      ? pages
      : (this.#readStep?.reader(pages, this.#nonRetainingReads) ?? pages);
  }
  readBoundary(): void {
    this.#readStep?.beforeDownstream();
  }
  table(id: number, budget: ByteBudget): MaybePromise<TableSnapshot> {
    const wrap = (table: TableSnapshot) =>
      readTable(table, this.readPages(this.view.pages), budget, () => {
        this.signal.throwIfAborted();
        if (this.view.released) throw new SnapshotReleasedError();
      });
    const table = this.view.table(id, { budget });
    return table instanceof Promise ? table.then(wrap) : wrap(table);
  }

  sourceBounds(bounds: ReadonlyMap<number, ReadBound>): void {
    this.#readBounds = new Map(bounds);
    this.#nonRetainingReads = bounds.size === 1 && bounds.values().next().value?.retain === false;
  }
  sourceWindow(id: number): number {
    return this.#readBounds.get(id)?.windowBytes ?? 0;
  }
  sourceDomain(id: number) {
    return this.#readBounds.get(id)?.domain;
  }
  minimumBytes(
    entries: ReadonlyArray<{ rel: OperatorIdentity; minimum: number; loanSlots?: number }>,
    inputs = 0,
    largestInput = 0,
  ): number {
    if (this.#atomicResult === null)
      return inputs + entries.reduce((sum, entry) => sum + entry.minimum, 0);
    const progress = Math.max(MAX_BLOB_BYTES, this.#progressBytes, largestInput);
    const borrowers = entries
      .filter(
        ({ rel }) =>
          rel.op === "sort" ||
          rel.op === "aggregate" ||
          rel.op === "join" ||
          rel.op === "window" ||
          rel.op === "setOp",
      )
      .reduce((sum, entry) => sum + 1 + (entry.loanSlots ?? 0), 0);
    return (
      inputs +
      entries.reduce((sum, entry) => sum + entry.minimum, 0) +
      borrowers * progress +
      this.#readBytes +
      (entries.length === 0 ? 0 : this.#window + Math.max(0, progress - this.#window))
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

  #reserveBound(entries: readonly Reservation[], binding: ParameterBinding): MaybePromise<void> {
    const atomic = this.#atomicResult !== null;
    const progress = atomic
      ? Math.max(MAX_BLOB_BYTES, this.#progressBytes, binding.largestSlot)
      : 0;
    const canBorrow = (rel: OperatorIdentity) =>
      rel.op === "sort" ||
      rel.op === "aggregate" ||
      rel.op === "join" ||
      rel.op === "window" ||
      rel.op === "setOp";
    this.#operatorProgress = new Map(
      entries.filter(({ rel }) => canBorrow(rel)).map(({ rel }) => [rel.id, progress]),
    );
    const window = atomic && entries.length > 0 ? this.#window : 0;
    const resultProgress = atomic && entries.length > 0 ? Math.max(0, progress - this.#window) : 0;
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
    const operators = entries.map((entry) => ({
      entry,
      base: add(`${key(entry)}/minimum`, entry.minimum),
      progress: add(`${key(entry)}/progress`, this.#operatorProgress.get(entry.rel.id) ?? 0),
      extra: add(`${key(entry)}/growth`, 0, true),
    }));
    const started = performance.now();
    const admitted = admitWorkingSet(this.request.governor, {
      label: `${prefix}/fixed-buffers`,
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
      const phase = (base: number, extra: number, extraProgress: number, bounded: boolean) => {
        const fixed = at(base),
          growth = at(extra),
          lane = at(extraProgress);
        const capacity = parts[extraProgress]?.minimumBytes ?? 0;
        const share = new ReservedGrant(
          fixed,
          growth,
          parts[base]?.minimumBytes ?? 0,
          atomic ? new StepLoans(lane, capacity, lane.label) : null,
          bounded,
          () => lane.close(),
          true,
        );
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
        for (const { entry, base, progress: lane, extra } of operators) {
          const share = phase(base, extra, lane, this.#readBounds.has(entry.rel.id));
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
    this.#dispose.push(cleanup);
  }

  onAbort(abort: (reason: unknown) => void): void {
    if (this.#aborted) abort(this.#reason);
    else this.#aborters.push(abort);
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

  close(): MaybePromise<void> {
    if (this.#closed) return;
    this.#closed = true;
    this.#bounds.clear();
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
    grant.allocate = (bytes) => {
      const ok = allocate(bytes);
      observe();
      return ok;
    };
    grant.tryGrow = (bytes) => {
      const ok = grow(bytes);
      observe();
      return ok;
    };
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
      (error: unknown) => {
        this.#delivered = true;
        this.#sink?.discard(error);
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
