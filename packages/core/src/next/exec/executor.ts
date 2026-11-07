/**
 * The executor's entry point: how a lowered `PhysicalPlan` runs as one query or one DML
 * statement, over the write path's access contracts (access/).
 *
 * ```text
 *  owner (B5)                              executor (B4)                    write path (B3)
 *  prepared = executor.prepare(plan)       (cached with the plan)
 *  // execute admits the inputs and binds them using its own grant.
 *  SELECT: view = writePath.pin()   ───────────────────────────────────▶  Snapshot
 *  DML:    statement = tx.statement({ rules })   (rules: compileTableRules, exec/rules.ts)
 *  exec = prepared.execute({ parameters: values, snapshot | statement, reads, governor, spill, … })
 *  await exec.executed        DML: statement ended (deferred checks passed) │ SELECT: last frame offered
 *  autocommit DML: commit (per durability) → exec.acknowledgeCommit()      │ (otherwise nothing)
 *  loop frame = exec.results.next() / poll(): wait durable(frame.requiresLsn) if needed,
 *       postMessage(frame.payload, frame.transfer); frame.release() on the client's credit
 *  await exec.delivered       every frame released: the result grant is closed
 * ```
 *
 * ## Prepare once, bind and execute many times
 * - `prepare` resolves every kernel (`fn` × argument types), operator and pipeline once. A
 *   `PreparedPlan` is immutable, shared by concurrent executions, and cached by the owner with the
 *   plan (text, catalog version). Anything the executor lacks is `UnsupportedPlanError` here,
 *   never a wrong answer later.
 * - `execute` admits input storage and invokes `bind(values, grant)` once per execution
 *   (`ParameterValue`). A value that does not
 *   fit its slot is `ParameterBindError`, never a re-plan.
 *
 * ## Inputs
 * - **What it reads.** A query reads `request.snapshot`, a read snapshot the owner pinned
 *   (`WritePath.pin`). A DML plan reads `request.statement.view`, the statement's view, which sees
 *   the transaction's earlier statements and never this one's writes. A plan lowered at another
 *   catalog version than the view's `catalog.version` fails with `PlanStaleError` before anything
 *   runs; the owner re-lowers.
 * - **Pages.** `request.reads` opens column chunks of group segments (exec/scan.ts); memory
 *   segments are read straight from the snapshot.
 * - **Memory.** The governor, not grants. The executor admits each pipeline itself when it starts,
 *   with the plan-derived minimum (memory/governor.ts rule 3), labelled `query#<queryId>/…`. State
 *   that outlives its pipeline (hash builds, sorted runs, aggregate partitions, the RETURNING
 *   spool) is in spillable grants, so an admission waiting elsewhere can be served by reclaim.
 *   Non-spillable inputs are admitted with the fixed buffers; inputs exceeding normal capacity
 *   raise MemoryAdmissionError. Other non-spillable grants hold bounded batch/I/O buffers. The
 *   progress-loan size error is a single value larger than the reserve (`ValueTooLargeError`). Dictionaries
 *   a scan decodes are charged to the query, not the cache (ruling R5).
 * - **Result grant.** One per execution, spillable, admitted at start with the credit window as
 *   its minimum. Frames are charged to it until released (exec/result.ts). It outlives `executed`
 *   and closes when every frame is released after the end, or at once when the stream is cancelled.
 * - **Sequences.** `SequenceAccess` (access/write-path.ts) serves `nextval`/`setval`; values come
 *   from blocks reserved by standalone log frames (ruling R4). Session state for `currval` and
 *   `lastval` is the owner's `SequenceSession`, passed per session.
 * - **Clock.** `statementTime` fills `plan.clockSlot`: int64 µs on the 2000 epoch (temporal.ts),
 *   the transaction's start time, as in PostgreSQL.
 *
 * ## DML
 * - The executor gets writers from `statement.table(tableId, { assigned, onRepeat })`, taking both
 *   from the IR's `modify` node (lowering sets `onRepeat: "first"` for UPDATE … FROM and DELETE …
 *   USING, `"error"` for ON CONFLICT DO UPDATE and MERGE). Batches use access/modify.ts shapes:
 *   insert = every column in catalog order; update = locator, then the assigned columns;
 *   delete = locator. ON CONFLICT asks `TableWriter.conflicts` first and routes rows by
 *   `ConflictKind`.
 * - The executor evaluates every expression, RETURNING over exactly the rows `written` reports,
 *   and CHECK and generated columns through `TableRules` (the write path calls them).
 * - When the input is exhausted the executor calls `statement.end()` (unique and foreign-key checks,
 *   cascades); `executed` resolves after it. On any error or cancellation before that it calls
 *   `statement.rollback()`. It never commits: the owner commits the transaction.
 *
 * ## The three completions (ruling R2)
 * Names (ruling R16.8): **executed** is `Execution.executed`, **committed** is the owner's
 * `acknowledgeCommit()` call, **delivered** is `Execution.delivered`.
 * 1. **`executed`** resolves with the summary when the plan has run: for a query, when its last
 *    frame has been offered; for DML, after `statement.end()`. By then every pipeline grant and
 *    spill file is closed except the result grant and the RETURNING spool, and the view is no
 *    longer read: the owner may release the read pin (frames and spool hold values, never
 *    locators). It rejects with the first error.
 * 2. **Committed** (autocommit DML with RETURNING: `request.returning === "atCommit"`).
 *    RETURNING rows go into the result spool, a charged, spillable store; no frame is offered
 *    until the owner calls `acknowledgeCommit()`, once the commit is acknowledged under the
 *    durability mode. A failed commit or a cancel discards the spool. Inside an explicit
 *    transaction (`"atExecuted"`, ruling R11) the spool flows when `executed` resolves, as
 *    PostgreSQL returns the rows before COMMIT; the delivery rule still applies (`requiresLsn`).
 *    A query (always `"atExecuted"`: it streams at once) or DML without RETURNING ignores it.
 * 3. **`delivered`** resolves when the stream has ended and every frame was released: the result
 *    grant and spool are closed. It rejects when the stream is cancelled or failed.
 *
 * ## Running, latency and the synchronous fast path (ruling R6)
 * - `execute` starts the work inside the call and runs synchronously until it meets a Promise
 *   (an uncached page, index block, deletion bitmap or group root, a memory wait, a spill) or
 *   `YIELD_INTERVAL_MS` of work. Every access call is `MaybePromise` (access/, exec/scan.ts): the
 *   executor awaits only an actual Promise.
 * - Admission on that path is `MemoryGovernor.tryAdmit` (synchronous when the minimum fits and
 *   nothing is queued); only when it returns null does the execution wait in `admit`.
 * - So a point read whose key-index blocks and pages are cached completes inside `execute`: on
 *   return `state` is `"executed"`, `summary` is set, and `results.poll()` yields its frame, with
 *   no await anywhere. The owner replies in the same task.
 * - Targets: owner-side warm PK read ≤ 10 µs; uncontended cross-tab warm PK read ≤ 0.25 ms on
 *   Chromium, ≤ 0.4 ms on Firefox, ≤ 0.6 ms on WebKit; p95 ≤ 5 ms beside a running analytical
 *   query.
 * - Long work yields to the event loop with a task (not a microtask) at a batch boundary after at
 *   most `YIELD_INTERVAL_MS` of synchronous work (exec/pipeline.ts `drive`), so point reads,
 *   commits and cancels wait at most about that long behind it, even behind a cached scan.
 *
 * ## Cancellation and errors
 * - `request.signal`, `cancel(reason)` or `results.cancel()` abort every pipeline (exec/pipeline.ts
 *   `abort`), roll the statement back if it has not ended, discard the spool, release queued
 *   frames and close the result grant. `executed` (if pending) and `delivered` reject with
 *   `QueryCancelledError`. After `executed` resolved, cancellation discards results only.
 * - SQL errors are `ExecutionError`s with PostgreSQL's SQLSTATE (exec/errors.ts); access/ and
 *   storage errors (constraint violations, corruption) pass through unchanged.
 *
 * ## Statistics and the cost model
 * - Planning reads statistics from directories and sketches through lowering; the executor never
 *   plans. Data-dependent choices are made at run time from exact facts: page pruning and the
 *   int64 fast path from page bounds, direct addressing vs hashing and radix partition counts from
 *   the real build size, spilling from grants, dropping a runtime filter keeping > ~90% of probes.
 * - Every execution reports an `ExecutionProfile` per plan operator; the owner may feed observed
 *   cardinalities back to lowering's statistics. `EXECUTOR_COSTS` are per-row CPU costs the cost
 *   model multiplies with cardinalities; the owner hands them to the lowerer.
 */
import type { Snapshot } from "../access/snapshot.js";
import type { Statement } from "../access/modify.js";
import type { SequenceAccess } from "../access/write-path.js";
import type { ExecutorCosts } from "../ir/costs.js";
import type { OutputColumn, PhysicalPlan } from "../ir/ir.js";
import type { Grant, MemoryGovernor } from "../memory/governor.js";
import type { SpillService } from "../spill/contract.js";
import type { FlatVector } from "../vector/vector.js";
import type { MaybePromise } from "../access/key-index.js";
import type { ResultStream } from "./result.js";
import type { ChunkReads } from "./scan.js";

export { YIELD_INTERVAL_MS } from "./pipeline.js";

/**
 * A client value for one parameter slot. Accepted forms by slot type (anything else, or a value
 * out of the type's range, is `ParameterBindError`; `null` binds SQL NULL to any slot):
 *
 * | Slot type          | Accepted                                                                 |
 * | ------------------ | ------------------------------------------------------------------------ |
 * | bool               | boolean                                                                  |
 * | int32 (16/32 bits) | integer `number` or `bigint` in range                                    |
 * | int64              | `bigint`, or a safe-integer `number` (an unsafe one was already rounded) |
 * | float64 / float32  | `number` (float32 rounds with `Math.fround`)                             |
 * | decimal            | exact text (`"12.50"`, `"NaN"`, `"Infinity"`), `bigint`, finite `number` (its shortest round-trip text) |
 * | date               | PostgreSQL date text (`"2024-02-29"`, `"infinity"`)                      |
 * | time, interval     | PostgreSQL text                                                          |
 * | timestamp(tz)      | ISO or PostgreSQL text (µs exact), or `Date` (ms exact)                  |
 * | text, json         | `string` (JSONB is canonicalized)                                        |
 * | bytes              | `Uint8Array`                                                             |
 * | uuid               | `string`, any case                                                       |
 * | enum               | `string`: one of its labels                                              |
 * | array              | an array of element values, or PostgreSQL array text                     |
 *
 * Declared lengths, precision and scale are applied as a cast to the slot's type would.
 */
export type ParameterValue =
  null | boolean | number | bigint | string | Date | Uint8Array | readonly ParameterValue[];

/**
 * Execution-owned slots: one length-1 flat vector per `plan.parameters` slot, in slot order.
 * These are internal kernel inputs, produced after admission; execute accepts the raw values.
 * Immutable once bound, with their binding grant alive for their entire use.
 */
export interface BoundParameters {
  readonly slots: readonly FlatVector[];
}

/**
 * Per-session sequence state (`currval`, `lastval`), owned by the owner's `EngineSession` and
 * passed with each execution (ruling R4). The executor records every value `nextval` returns.
 */
export interface SequenceSession {
  /** The last value `nextval(sequenceId)` returned in this session; 55000 error if none. */
  current(sequenceId: number): bigint;
  /** The last value any `nextval` returned in this session; 55000 error if none. */
  last(): bigint;
  /** Records `value` as the session's latest for `sequenceId`. */
  record(sequenceId: number, value: bigint): void;
}

export interface Executor {
  /** Validates and compiles a plan once; see "Prepare once". Throws `UnsupportedPlanError`. */
  prepare(plan: PhysicalPlan): PreparedPlan;
}

export interface PreparedPlan {
  readonly plan: PhysicalPlan;
  /** The result columns (`plan.columns`): RETURNING columns for DML, empty without it. */
  readonly columns: readonly OutputColumn[];
  /** `modify` when the root is a `modify` operator: it needs `request.statement`. */
  readonly kind: "query" | "modify";
  /** Sequence functions are called: it needs `sequences` and `sequenceSession`. */
  readonly usesSequences: boolean;
  /**
   * Binds `values[i]` to placeholder `$i+1`. Throws `ParameterBindError` when the count is not
   * `plan.placeholderCount` or a value does not fit (the error names the slot).
   */
  bind(values: readonly ParameterValue[], grant: Grant): MaybePromise<BoundParameters>;
  /** Starts the execution synchronously (see "Running"). Throws only `TypeError` for owner bugs. */
  execute(request: ExecuteRequest): Execution;
}

interface ExecuteBase {
  /** Caller-owned values, borrowed until executed/cancellation; do not mutate during execution.
   * The execution admits its input storage before creating any owned vectors or copies. */
  readonly parameters: readonly ParameterValue[];
  /** int64 µs since 2000-01-01 (temporal.ts): fills `plan.clockSlot`. */
  readonly statementTime: bigint;
  readonly reads: ChunkReads;
  readonly governor: MemoryGovernor;
  readonly spill: SpillService;
  /** Unique among the owner's running executions: grant labels and spill file ownership. */
  readonly queryId: number;
  readonly sequences: SequenceAccess | null;
  readonly sequenceSession: SequenceSession | null;
  readonly signal?: AbortSignal;
  /** Credit window of the result stream (default `RESULT_WINDOW_BYTES`). */
  readonly resultWindowBytes?: number;
  /** Optional maximum rows per result frame (null/absent: byte-bounded framing only). */
  readonly resultFrameRows?: number | null;
  /**
   * When RETURNING rows may flow: `"atCommit"` for an autocommit DML statement (after
   * `acknowledgeCommit`), `"atExecuted"` otherwise (queries, statements inside a transaction).
   */
  readonly returning: "atExecuted" | "atCommit";
}

export type ExecuteRequest =
  /** A query over a read snapshot the owner pinned and releases after `executed`. */
  | (ExecuteBase & { readonly snapshot: Snapshot; readonly statement: null })
  /** A DML plan inside an open statement: it reads `statement.view` and ends the statement. */
  | (ExecuteBase & { readonly snapshot: null; readonly statement: Statement });

export type ExecutionState = "running" | "executed" | "failed" | "cancelled";

export interface Execution {
  readonly results: ResultStream;
  /** Synchronous view of the run, for the fast path. */
  readonly state: ExecutionState;
  /** Set when `state` becomes `"executed"` (the value `executed` resolves with). */
  readonly summary: ExecutionSummary | null;
  /** Completion 1 (see "The three completions"). */
  readonly executed: Promise<ExecutionSummary>;
  /** Completion 2 (`returning: "atCommit"`): lets the RETURNING spool flow. Idempotent. */
  acknowledgeCommit(): void;
  /** Completion 3. */
  readonly delivered: Promise<void>;
  /** Cancels (see "Cancellation"). Idempotent; a no-op once `delivered` settled. */
  cancel(reason?: unknown): void;
}

export interface ExecutionSummary {
  /** Rows the result has (SELECT), or rows affected (DML: inserted + updated + deleted). */
  readonly rowCount: number;
  /** DML only (zeros for a query): the counts the owner records with the commit. */
  readonly counts: {
    readonly inserted: number;
    readonly updated: number;
    readonly deleted: number;
  };
  /**
   * The largest LSN anything delivered depends on: the view's `observedLsn` and every sequence
   * reservation the execution used. Under strict durability the owner delivers nothing (frames,
   * counts) before `writePath.durable(requiresLsn)` (null when already durable).
   */
  readonly requiresLsn: number;
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
  /** Scans/fetches: encoded data pages opened in their chunk readers (cache hits included),
   * and pages directly ruled out by group/page bounds or runtime filters. */
  readonly pagesRead: number;
  readonly pagesPruned: number;
  /** Runtime filters this operator applied: probes seen and kept (null when none). */
  readonly runtimeFilter: { readonly probes: number; readonly kept: number } | null;
}

export interface ExecutionProfile {
  readonly operators: readonly OperatorProfile[];
  /** Pipelines admitted, and the time spent waiting for admission or loans in total. */
  readonly pipelines: number;
  readonly memoryWaitMs: number;
  /** Most bytes the execution's grants held at once, summed across its pipelines. */
  readonly peakHeldBytes: number;
  /** The run finished inside `execute` with no await (the fast path). */
  readonly synchronous: boolean;
}

/**
 * Per-row CPU cost of each operator class in nanoseconds (single thread, warm), measured by the
 * executor's benches and versioned with the kernels. Defined once in ir/costs.ts, where lowering's
 * cost model reads it (lower/ may not import exec/).
 */
export type { ExecutorCosts } from "../ir/costs.js";

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
