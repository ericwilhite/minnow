/** One query's admissions, resources and the executed/delivered lifecycle. */
import type { MaybePromise } from "../access/key-index.js";
import type { Snapshot } from "../access/snapshot.js";
import type { PhysicalPlan, Rel } from "../ir/ir.js";
import type { Grant } from "../memory/governor.js";
import { ExecutionError, PlanStaleError, QueryCancelledError } from "./errors.js";
import { WindowError } from "./window-frames.js";
import type {
  ExecuteRequest,
  Execution,
  ExecutionProfile,
  ExecutionState,
  ExecutionSummary,
  OperatorProfile,
} from "./executor.js";
import { clockParameter } from "./parameters.js";
import { RESULT_WINDOW_BYTES, ResultChannel } from "./result.js";
import { ResultSink } from "./result-sink.js";
import type { ParameterSlots, CallResolver } from "./kernels/compile.js";
import { ReservedGrant } from "./admission.js";

type Mutable<T> = { -readonly [K in keyof T]: T[K] };
export type MutableOperatorProfile = Mutable<OperatorProfile>;
export type Program = (runtime: QueryRuntime, sink: ResultSink) => MaybePromise<void>;

/** Per-execution only: never retained by an immutable prepared plan. */
export class QueryRuntime {
  resolveCall: CallResolver | undefined;
  readonly params: ParameterSlots;
  readonly profiles = new Map<number, MutableOperatorProfile>();
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

  /** Loan owners hold zero bytes until the one atomic fixed-buffer admission succeeds. */
  reserve(
    entries: ReadonlyArray<{ readonly rel: Rel; readonly minimum: number }>,
  ): MaybePromise<void> {
    const extras: Grant[] = [];
    const next = (at: number): MaybePromise<void> => {
      for (let k = at; k < entries.length; k++) {
        const entry = entries[k];
        if (entry === undefined) throw new TypeError("missing admission entry");
        const grant = this.admit(`loan#${String(entry.rel.id)}`, 0, true);
        if (grant instanceof Promise)
          return grant.then((value) => {
            extras.push(value);
            return next(k + 1);
          });
        extras.push(grant);
      }
      const minimum = entries.reduce((sum, entry) => sum + entry.minimum, 0);
      const fixed = this.admit("fixed-buffers", minimum, true);
      const install = (grant: Grant) => {
        const shares: ReservedGrant[] = [];
        for (let k = 0; k < entries.length; k++) {
          const entry = entries[k],
            extra = extras[k];
          if (entry === undefined || extra === undefined)
            throw new TypeError("missing reservation");
          const share = new ReservedGrant(grant, extra, entry.minimum);
          this.#meter(share, entry.rel, false);
          shares.push(share);
          this.#reserved.set(`${entry.rel.op}#${String(entry.rel.id)}`, share);
          this.dispose(() => share.close());
        }
        grant.onReclaimRequest((bytes) => {
          for (const share of shares) share.reclaim(bytes);
        });
      };
      return fixed instanceof Promise ? fixed.then(install) : install(fixed);
    };
    return next(0);
  }

  constructor(
    readonly plan: PhysicalPlan,
    readonly request: ExecuteRequest,
    readonly view: Snapshot,
    readonly signal: AbortSignal,
  ) {
    this.params = { slots: [...request.parameters.slots] };
    if (plan.clockSlot !== null) {
      const type = plan.parameters[plan.clockSlot];
      if (type?.kind !== "timestamp") throw new TypeError("invalid statement clock slot");
      const slots = [...this.params.slots];
      slots[plan.clockSlot] = clockParameter(type, request.statementTime);
      this.params.slots = slots;
    }
  }

  profile(rel: Rel): MutableOperatorProfile {
    let profile = this.profiles.get(rel.id);
    if (profile === undefined) {
      profile = {
        relId: rel.id,
        op: rel.op,
        rowsOut: 0,
        batchesOut: 0,
        peakHeldBytes: 0,
        spilledBytes: 0,
        spillPasses: 0,
        pagesRead: 0,
        pagesPruned: 0,
        runtimeFilter: null,
      };
      this.profiles.set(rel.id, profile);
    }
    return profile;
  }

  admit(label: string, minimumBytes: number, spillable: boolean, rel?: Rel): MaybePromise<Grant> {
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
    this.#meter(grant);
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
    return run(0);
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

  #adopt(grant: Grant, rel: Rel | undefined): Grant {
    if (this.#closed || this.signal.aborted) {
      grant.close();
      this.signal.throwIfAborted();
      throw new TypeError("admission completed after query disposal");
    }
    this.#meter(grant, rel);
    this.dispose(() => grant.close());
    return grant;
  }

  #meter(grant: Grant, rel?: Rel, account = true): void {
    // A reserved share is already included in its fixed/extra real grants. Observe its
    // operator peak without double-counting the execution's holdings or loan wait time.
    if (account) this.#grants.push(grant);
    const observe = () => {
      this.#peak = Math.max(
        this.#peak,
        this.#grants.reduce((sum, g) => sum + g.held, 0),
      );
      if (rel !== undefined) {
        const profile = this.profile(rel);
        profile.peakHeldBytes = Math.max(profile.peakHeldBytes, grant.held);
      }
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
    if (request.parameters.slots.length !== plan.parameters.length)
      throw new TypeError("parameters were not bound to this plan");
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
    grant.onReclaimRequest(() =>
      grant.release(Math.max(0, grant.held - grant.allocated - grant.loaned)),
    );
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
