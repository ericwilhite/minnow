/**
 * The push protocol between operators, and the driver that runs a pipeline.
 *
 * Kernels are synchronous. A pipeline is driven from its source: the driver pulls a batch, pushes
 * it through synchronous operators to a sink, and waits only on an actual Promise: a source that
 * must read (an uncached page, a spill read, a helper result), an operator that says it is
 * blocked (spilling, waiting for memory, consumer backpressure), or a breaker's `finish`.
 *
 * ## Synchronous progress, task-level yields (ruling R16.4)
 * - `BatchSource.next()` and `Operator.finish()` return a value when they can (a cached page, a
 *   memory chunk) and a Promise only when they must wait. The driver never awaits a value, so a
 *   pipeline whose sources and sinks are ready runs synchronously, and `drive` itself returns
 *   `undefined` (not a Promise) when the whole pipeline finished without waiting: a cached point
 *   read completes inside the call (exec/executor.ts "Running").
 * - Long synchronous work yields to the event loop with a **task** (`yieldTask`: `setImmediate`
 *   where it exists, else a `MessageChannel` message with a `setTimeout` timer turn at least every
 *   `TIMER_TURN_MS`; never a resolved Promise, which would only run microtasks) at a batch
 *   boundary once `YIELD_INTERVAL_MS` has passed since the task began: one clock per worker
 *   (`TaskClock`, ruling R23.5), stamped when a yield resolves and at the owner's task entries.
 *   So an owner message (a point read, a commit, a cancel) waits at most about that long behind an
 *   analytical query, even one served entirely from cache.
 *
 * ## `push(batch, transfer?)` returns `ready` or `blocked`
 * - `ready`: the operator is done with the batch (it copied what it keeps). The producer may
 *   recycle the batch's buffers.
 * - `blocked`: the operator accepted the batch but has not finished with it; it may be mid-way
 *   through it (an aggregate that must spill before inserting the rest, a join whose output
 *   consumer is full). The batch stays **borrowed** until `ready()` resolves: the producer must not
 *   recycle it before then, and the driver pushes nothing else into this operator. The operator
 *   resumes the remaining rows itself inside `ready()`.
 * - So each edge holds at most one in-flight batch: memory is bounded by the pipeline's admitted
 *   minimum (memory/governor.ts), whatever the data. A source is asked for its next batch only
 *   after the previous one's borrow ended, so it may reuse its buffers (and a memory-segment scan
 *   may give back its borrowed chunk: exec/scan.ts).
 *
 * ## Ownership handoff
 * - Default (no `transfer`): borrow, as above. Keeping anything past readiness means copying.
 * - `transfer`: the producer hands the buffers over (retain without copy). The operator owns the
 *   batch until it calls `transfer.release()`, which returns reusable buffers to the producer's pool
 *   or releases dedicated output charges. Only `detachableBuffers` authorizes irreversible transfer.
 *   The pool has a fixed number of batches (its credits); a producer with no free batch blocks
 *   in `next()` until one is released. Retained batches are therefore bounded and charged to the
 *   producer's grant, never invisible.
 *
 * ## End and cancellation
 * - `finish()` (a value or a Promise): a breaker (sort, aggregate, join build) produces its output
 *   there, pushing to its downstream and waiting for its readiness, then finishes the downstream.
 * - `abort(reason)` releases everything the operator holds (grants, spill files, transferred
 *   batches) and is idempotent; a pending `ready()` rejects with `reason`. The driver aborts the
 *   whole pipeline on error or cancellation, checking the signal at every batch boundary.
 *
 * ## Results
 * The result sink is an operator whose readiness is the consumer's credit: it holds at most a
 * fixed window of result bytes in flight to the client and returns `blocked` beyond it, so a
 * slow consumer pauses the pipeline instead of growing a buffer (exec/result.ts).
 */
import type { ByteBudget } from "../access/space.js";
import type { MaybePromise } from "../access/key-index.js";
import type { Batch } from "../vector/vector.js";

export type PushResult = "ready" | "blocked";

/** Retain-without-copy: the receiver owns the batch until it calls `release`. */
export interface Transfer {
  /** Explicit permission for irreversible transfer of dedicated output buffers. Never names
   * cached pages, leased slabs or reusable producer-pool buffers. An unmarked handoff only
   * permits retention; release returns its buffers to the producer pool as usual.
   * For detachable output, release returns the charge/credit, never a detached buffer to a pool.
   */
  readonly detachableBuffers?: ReadonlySet<ArrayBuffer>;
  /** Where those buffers remain charged. A result sink can adopt without another charge only
   * when this is its result budget; otherwise it must copy/reclassify before publishing.
   */
  readonly detachableBudget?: ByteBudget;
  /** Dedicated arrays may be adopted by another account only after that receiver charges their
   * complete size, before publishing or releasing this token. No credit or pooled slab moves. */
  readonly detachableReclassify?: boolean;
  /** Source-authorized full-buffer capacity for logical bytes; padding is dedicated and charged. */
  readonly detachableCapacity?: (bytes: number) => number;
  release(): void;
}

export interface Operator {
  /** Dedicated output may move into this chain. It copies or prepays/reclassifies before
   * readiness and returns the source token by then; it never retains source-account credit. */
  readonly reclassifiesTransfers?: boolean;
  push(batch: Batch, transfer?: Transfer): PushResult;
  /** Resolves when the operator can take the next batch; rejects if aborted. */
  ready(): Promise<void>;
  /** A value when nothing waits, else a Promise. */
  finish(): MaybePromise<void>;
  abort(reason: unknown): void;
}

export interface SourceItem {
  readonly batch: Batch;
  readonly transfer?: Transfer;
}

export interface BatchSource {
  /**
   * The next batch, or null at the end. A batch returned without a transfer is borrowed until the
   * following `next()` call; with one, until its `release`.
   */
  next(): MaybePromise<SourceItem | null>;
  abort(reason: unknown): void;
}

/** Pushes `batch` into `operator` and waits for readiness: the driver's step, also used inside operators. */
export async function pushAndWait(
  operator: Operator,
  batch: Batch,
  transfer?: Transfer,
): Promise<void> {
  if (operator.push(batch, transfer) === "blocked") await operator.ready();
}

function abortReason(signal: AbortSignal): Error {
  const reason: unknown = signal.reason;
  return reason instanceof Error ? reason : new Error("Aborted", { cause: reason });
}

/**
 * `promise`, rejected early if `signal` aborts. Without a signal it is `promise` itself; with one,
 * the listener lives only as long as this wait, so a long stream retains nothing per batch.
 */
export function abortable<T>(promise: Promise<T>, signal?: AbortSignal): Promise<T> {
  if (signal === undefined) return promise;
  if (signal.aborted) return Promise.reject(abortReason(signal));
  return new Promise<T>((resolve, reject) => {
    const onAbort = () => {
      reject(abortReason(signal));
    };
    signal.addEventListener("abort", onAbort, { once: true });
    promise.then(
      (value) => {
        signal.removeEventListener("abort", onAbort);
        resolve(value);
      },
      (error: unknown) => {
        signal.removeEventListener("abort", onAbort);
        reject(error instanceof Error ? error : new Error(String(error)));
      },
    );
  });
}

// The shared worker task clock (R23.5) lives in next/task.ts so storage can use it; re-exported here.
export {
  YIELD_INTERVAL_MS,
  TaskClock,
  taskClock,
  beginTask,
  clockFor,
  afterYield,
  createYieldTask,
  TIMER_TURN_MS,
  yieldTask,
  type TaskHost,
} from "../task.js";
import { YIELD_INTERVAL_MS, yieldTask, clockFor, afterYield, type TaskClock } from "../task.js";

/** True for a Promise (or any thenable): what the driver must wait on. */
export function isPromise<T>(value: MaybePromise<T>): value is Promise<T> {
  return typeof (value as { then?: unknown } | null)?.then === "function";
}

export interface DriveOptions {
  /** Synchronous work between task yields (default `YIELD_INTERVAL_MS`). */
  readonly yieldIntervalMs?: number;
  /** Tests: a clock in ms, read through a private task clock (default the worker's, R23.5). */
  readonly now?: () => number;
}

/**
 * Runs one pipeline to completion, aborting every stage on error or cancellation. Returns
 * `undefined` when it finished synchronously, else a Promise of its end. A synchronous failure
 * throws; once it has waited, failures reject.
 */
export function drive(
  source: BatchSource,
  sink: Operator,
  signal?: AbortSignal,
  options: DriveOptions = {},
): MaybePromise<void> {
  return new Driver(source, sink, signal, options).run();
}

class Driver {
  readonly #interval: number;
  /** The task clock (R23.5): the driver never stamps it, it only asks it. */
  readonly #clock: TaskClock;
  /** A pulled item not pushed yet (`undefined`: nothing pulled). */
  #item: SourceItem | null | undefined = undefined;
  #finished = false;

  constructor(
    readonly source: BatchSource,
    readonly sink: Operator,
    readonly signal: AbortSignal | undefined,
    options: DriveOptions,
  ) {
    this.#interval = options.yieldIntervalMs ?? YIELD_INTERVAL_MS;
    this.#clock = clockFor(options.now);
  }

  run(): MaybePromise<void> {
    let wait: Promise<unknown> | null;
    try {
      wait = this.#advance();
    } catch (error) {
      this.#abort(error);
      throw error;
    }
    return wait === null ? undefined : this.#resume(wait);
  }

  async #resume(first: Promise<unknown>): Promise<void> {
    try {
      for (let wait: Promise<unknown> | null = first; wait !== null; wait = this.#advance())
        await abortable(wait, this.signal);
    } catch (error) {
      this.#abort(error);
      throw error;
    }
  }

  #abort(error: unknown): void {
    this.source.abort(error);
    this.sink.abort(error);
  }

  /** Runs until the pipeline ends (null) or must wait (the Promise to wait on). */
  #advance(): Promise<unknown> | null {
    for (;;) {
      this.signal?.throwIfAborted();
      if (this.#finished) return null;
      if (this.#item === undefined) {
        if (this.#clock.due(this.#interval))
          return yieldTask().then(() => {
            afterYield(this.#clock);
          });
        const next = this.source.next();
        if (isPromise(next))
          return next.then((item) => {
            this.#item = item;
          });
        this.#item = next;
      }
      const item = this.#item;
      this.#item = undefined;
      if (item === null) {
        this.#finished = true;
        const finished = this.sink.finish();
        return isPromise(finished) ? finished : null;
      }
      if (this.sink.push(item.batch, item.transfer) === "blocked") return this.sink.ready();
    }
  }
}
