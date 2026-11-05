/**
 * The executor's entry point: how a lowered `PhysicalPlan` runs as one query.
 *
 * ```text
 *  owner (B5)                         executor (B4)                        write path (B3)
 *  ─────────                          ─────────────                        ───────────────
 *  plan = lowerer.lower…(…)   ──▶  prepared = executor.prepare(plan)        (cached with the plan)
 *  params = prepared.bind(values)
 *  pin snapshot ◀──────────────────────────────────────────────────────── QuerySnapshot
 *  open ModifyTarget (DML) ◀───────────────────────────────────────────── ModifyTarget
 *  exec = prepared.execute({ parameters, snapshot, governor, spill, modify, … })
 *  loop: frame = await exec.results.next() → post to client → release() on client credit
 *  summary = await exec.done   → commit (autocommit DML) → release the pin
 * ```
 *
 * ## Prepare once, bind and execute many times
 * - `prepare` resolves every kernel (`fn` × argument types), every operator and the pipeline
 *   split once, so an execution only instantiates operator state. A `PreparedPlan` is immutable
 *   and shared by concurrent executions; the owner caches it with the plan (text, catalog
 *   version). It throws `UnsupportedPlanError` for anything the executor lacks: never a wrong
 *   answer later.
 * - `bind` turns the client's values into slot values once per execution (see `ParameterValue`).
 *   A value that does not fit its slot's type is `ParameterBindError`, never a re-plan.
 *
 * ## What `execute` needs
 * - **Snapshot.** A pinned `QuerySnapshot` (exec/snapshot.ts) whose `catalogVersion` equals the
 *   plan's; otherwise the execution fails with `PlanStaleError` before anything runs, and the
 *   owner re-lowers. The owner releases the pin only after `done` settles; locators never escape
 *   the execution (frames hold values, never locators).
 * - **Memory.** The governor, not grants: the executor admits each pipeline itself, when that
 *   pipeline starts, with the plan-derived minimum (memory/governor.ts rule 3), labelled
 *   `query#<queryId>/…`. State that outlives its pipeline (a hash build, sorted runs, aggregate
 *   partitions) lives in spillable grants, so a waiting admission elsewhere can always be served by
 *   reclaim; non-spillable grants hold only bounded batch, frame and I/O buffers. Operators that
 *   exceed their grant spill; nothing is refused for size (the one size error is a single value
 *   larger than the reserve, `ValueTooLargeError`).
 * - **Spill.** The spill service; files are created lazily per operator under `queryId` and all
 *   closed before `done` settles.
 * - **DML.** A `ModifyTarget` (exec/modify.ts) when the plan's root is `modify`, and `null`
 *   otherwise; a mismatch is a `TypeError` (owner bug). `SequenceAccess` when the plan calls a
 *   sequence function (`nextval` defaults included).
 * - **Clock.** `statementTime` fills `plan.clockSlot` (CURRENT_TIMESTAMP and friends): int64 µs on
 *   the 2000 epoch (temporal.ts). The owner passes the transaction's start time, as PostgreSQL.
 *
 * ## Running
 * - `execute` returns at once; work starts on a microtask. `initPlans` run first, in order, each
 *   filling its slot (zero rows: null; more than one: the 21000 cardinality error). Then the main
 *   plan's pipelines run, sources awaiting I/O and the result sink pausing them on credit.
 * - Several executions may run at once on the owner's thread. Each yields to the event loop at a
 *   batch boundary after at most `YIELD_INTERVAL_MS` of synchronous work, so a point read or a
 *   commit waits at most about that long behind an analytical query.
 * - Results stream through `results` (exec/result.ts): frames of bounded bytes, no row-count cap,
 *   credit backpressure. A plan without output columns (DML without RETURNING) produces no frames.
 * - RETURNING frames of a DML plan are provisional: they describe the statement's effect, which
 *   is final only when the owner's commit succeeds. Ledger duplicates replay counts only
 *   (format/ledger.ts).
 *
 * ## Ending
 * - `done` resolves after the last frame was offered (not consumed) and, for DML, after
 *   `ModifyTarget.finish()`. It rejects with the first error. Either way, by the time it settles
 *   every grant is closed, every spill file closed, the modify target finished or aborted, and the
 *   snapshot no longer read: the owner may release the pin and reuse the target's transaction.
 * - Cancellation: `request.signal`, `Execution.cancel(reason)`, or `results.cancel()`. All three
 *   abort every pipeline (exec/pipeline.ts `abort`), abort the modify target, reject `done` with
 *   `QueryCancelledError`, and release queued frames. Frames already handed out stay the
 *   consumer's to release.
 * - Errors: SQL errors are `ExecutionError`s with PostgreSQL's SQLSTATE (exec/errors.ts); storage
 *   and write-path errors pass through unchanged.
 *
 * ## Statistics and the cost model
 * - Planning reads statistics from page directories and sketches through the catalog view
 *   (ir/lowering.ts `TableStatistics`); the executor never plans. Data-dependent choices are made
 *   at run time from exact facts, not estimates: page pruning and the int64 fast path from page
 *   bounds (`SegmentSnapshot.bounds`/`pages`), direct addressing vs hashing and the radix
 *   partition count from the build side's real size, spilling from grants, dropping a runtime
 *   filter that keeps more than ~90% of probes.
 * - Every execution returns an `ExecutionProfile`: observed rows, bytes, spill and pruning per
 *   plan operator. The owner may feed observed cardinalities back to lowering's statistics; the
 *   perf lane and the reach declarations read it.
 * - `EXECUTOR_COSTS` are the executor's per-row CPU costs by operator class, measured by the
 *   benches, that the cost model multiplies with cardinalities. The owner hands them to the
 *   lowerer (lower/ cannot import exec/).
 */
import type { OutputColumn, PhysicalPlan } from "../ir/ir.js";
import type { MemoryGovernor } from "../memory/governor.js";
import type { SpillService } from "../spill/contract.js";
import type { FlatVector } from "../vector/vector.js";
import type { ModifyTarget, SequenceAccess } from "./modify.js";
import type { ResultStream } from "./result.js";
import type { QuerySnapshot } from "./snapshot.js";

/** Most synchronous work an execution does before yielding to the event loop. */
export const YIELD_INTERVAL_MS = 4;

/**
 * A client value for one parameter slot. Accepted forms by slot type (anything else, or a value
 * out of the type's range, is `ParameterBindError`; `null` binds SQL NULL to any slot):
 *
 * | Slot type           | Accepted                                                              |
 * | ------------------- | --------------------------------------------------------------------- |
 * | bool                | boolean                                                               |
 * | int32 (16/32 bits)  | integer `number` or `bigint` in range                                 |
 * | int64               | `bigint`, or a safe-integer `number` (an unsafe one was already rounded) |
 * | float64 / float32   | `number` (float32 rounds with `Math.fround`)                          |
 * | decimal             | exact text (`"12.50"`, `"NaN"`, `"Infinity"`), `bigint`, finite `number` (its shortest round-trip text) |
 * | date                | PostgreSQL date text (`"2024-02-29"`, `"infinity"`)                   |
 * | time, interval      | PostgreSQL text                                                       |
 * | timestamp(tz)       | ISO or PostgreSQL text (µs exact), or `Date` (ms exact)               |
 * | text, json          | `string` (JSONB is canonicalized)                                     |
 * | bytes               | `Uint8Array`                                                          |
 * | uuid                | `string`, any case                                                    |
 * | enum                | `string`: one of its labels                                           |
 * | array               | an array of element values, or PostgreSQL array text                  |
 *
 * Declared lengths, precision and scale are applied as a cast to the slot's type would.
 */
export type ParameterValue =
  null | boolean | number | bigint | string | Date | Uint8Array | readonly ParameterValue[];

/**
 * Slot values ready to execute: one length-1 flat vector per `plan.parameters` slot, in slot
 * order (placeholders bound, lowering's own slots unset until init plans and the clock fill
 * them). Immutable once bound; one `BoundParameters` serves one execution at a time.
 */
export interface BoundParameters {
  readonly slots: readonly FlatVector[];
}

export interface Executor {
  /** Validates and compiles a plan once; see "Prepare once". Throws `UnsupportedPlanError`. */
  prepare(plan: PhysicalPlan): PreparedPlan;
}

export interface PreparedPlan {
  readonly plan: PhysicalPlan;
  /** The result columns (`plan.columns`): RETURNING columns for DML, empty without it. */
  readonly columns: readonly OutputColumn[];
  /** `modify` when the root is a `modify` operator (needs a `ModifyTarget`). */
  readonly kind: "query" | "modify";
  /** Sequence functions are called (needs `SequenceAccess`). */
  readonly usesSequences: boolean;
  /**
   * Binds `values[i]` to placeholder `$i+1`. Throws `ParameterBindError` when the count is not
   * `plan.placeholderCount` or a value does not fit (the error names the slot).
   */
  bind(values: readonly ParameterValue[]): BoundParameters;
  execute(request: ExecuteRequest): Execution;
}

export interface ExecuteRequest {
  readonly parameters: BoundParameters;
  /** int64 µs since 2000-01-01 (temporal.ts): fills `plan.clockSlot`. */
  readonly statementTime: bigint;
  readonly snapshot: QuerySnapshot;
  readonly governor: MemoryGovernor;
  readonly spill: SpillService;
  /** Unique among the owner's running executions: grant labels and spill file ownership. */
  readonly queryId: number;
  readonly modify: ModifyTarget | null;
  readonly sequences: SequenceAccess | null;
  readonly signal?: AbortSignal;
  /** Credit window of the result stream (default `RESULT_WINDOW_BYTES`). */
  readonly resultWindowBytes?: number;
}

export interface Execution {
  readonly results: ResultStream;
  /** See "Ending". */
  readonly done: Promise<ExecutionSummary>;
  /** Cancels the execution. Idempotent; a no-op once `done` settled. */
  cancel(reason?: unknown): void;
}

export interface ExecutionSummary {
  /** Rows sent as results (SELECT), or rows affected (DML: inserted + updated + deleted). */
  readonly rowCount: number;
  /** DML only (zeros for a query): the counts the owner records with the commit (ledger). */
  readonly counts: {
    readonly inserted: number;
    readonly updated: number;
    readonly deleted: number;
  };
  readonly profile: ExecutionProfile;
}

/** What one plan operator (`Rel.id`) actually did. */
export interface OperatorProfile {
  readonly relId: number;
  readonly op: string;
  readonly rowsOut: number;
  readonly batchesOut: number;
  /** Most bytes its grants held at once. */
  readonly peakHeldBytes: number;
  /** Spill bytes written, and passes over spilled data (0 when it never spilled). */
  readonly spilledBytes: number;
  readonly spillPasses: number;
  /** Scans and fetches: pages decoded, and pages skipped by bounds or runtime filters. */
  readonly pagesRead: number;
  readonly pagesPruned: number;
  /** Runtime filters this operator applied: probes seen and probes kept (null when none). */
  readonly runtimeFilter: { readonly probes: number; readonly kept: number } | null;
}

export interface ExecutionProfile {
  readonly operators: readonly OperatorProfile[];
  /** Pipelines admitted, and the time they waited for admission or loans in total. */
  readonly pipelines: number;
  readonly memoryWaitMs: number;
  /** Most bytes the execution's grants held at once, summed across its pipelines. */
  readonly peakHeldBytes: number;
}

/**
 * Per-row CPU cost of each operator class in nanoseconds (single thread, warm), measured by the
 * executor's benches and versioned with the kernels. The cost model computes
 * `Σ rows × cost + spillBytes / spillBandwidth` (design-joins-agg-spill.md "Statistics and
 * ordering"); I/O costs come from the storage track.
 */
export interface ExecutorCosts {
  readonly decodePerValue: number;
  readonly filterPerRow: number;
  readonly hashBuildPerRow: number;
  readonly hashProbePerRow: number;
  readonly aggregatePerRow: number;
  readonly sortPerRowLog: number;
  readonly emitPerValue: number;
  /** Bytes per second of the spill path, write plus read back. */
  readonly spillBytesPerSecond: number;
}

/** Placeholder values until the Phase 2 benches measure them. */
export const EXECUTOR_COSTS: ExecutorCosts = {
  decodePerValue: 1,
  filterPerRow: 1,
  hashBuildPerRow: 20,
  hashProbePerRow: 15,
  aggregatePerRow: 5,
  sortPerRowLog: 3,
  emitPerValue: 2,
  spillBytesPerSecond: 600e6,
};
