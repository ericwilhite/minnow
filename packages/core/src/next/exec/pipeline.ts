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
  release(): void;
}

export interface Operator {
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

/** Most synchronous work a pipeline does before yielding to the event loop with a task. */
export const YIELD_INTERVAL_MS = 4;

/** True for a Promise (or any thenable): what the driver must wait on. */
export function isPromise<T>(value: MaybePromise<T>): value is Promise<T> {
  return typeof (value as { then?: unknown } | null)?.then === "function";
}

/**
 * When the current task began (ruling R23.5). The 4 ms bound is on the worker's task, not on one
 * operator: the driver, every breaker, scans, aggregation, join, sort, top-k and window ask the
 * same clock whether `YIELD_INTERVAL_MS` has passed since the task began, and keep no slice start
 * of their own (per-operator starts let several operators in one task add up).
 *
 * It is stamped only where a task genuinely begins: when a `yieldTask` resolves, and at the
 * owner's task entry points (the server's message dispatch, timers that start exec or write
 * work: `beginTask`). Never in `drive()`, an operator's entry or a resolved promise, which can
 * run mid-task. A missing stamp errs safe: the start is older, and work yields early.
 *
 * The worker's clock (`taskClock`) is event-loop state, like `lastTimerTurn`, not query scratch
 * (R23.1). Tests inject a clock (an operator's `now` option makes a private one over it) or call
 * `beginTask()` before a run they expect to be synchronous.
 */
export class TaskClock {
  #start: number;
  /** Tasks begun on this clock (a job checks a yield by it: join-keys.ts `KeyWork`). */
  epoch = 0;
  constructor(public now: () => number) {
    this.#start = now();
  }
  /** A task begins now. */
  begin(): void {
    this.#start = this.now();
    this.epoch++;
  }
  /** Milliseconds since the current task began. */
  elapsed(): number {
    return this.now() - this.#start;
  }
  /** Whether `interval` ms have passed since the current task began. */
  due(interval = YIELD_INTERVAL_MS): boolean {
    return this.now() - this.#start >= interval;
  }
}

/** The worker's task clock (see `TaskClock`). */
export const taskClock = new TaskClock(() => performance.now());

/** The owner's task entry points stamp the worker's clock (R23.5). */
export function beginTask(): void {
  taskClock.begin();
}

/** The worker's clock, or for a test's `now` a private clock over it. */
export function clockFor(now?: () => number): TaskClock {
  return now === undefined ? taskClock : new TaskClock(now);
}

/**
 * After a `yieldTask`: the worker's clock was stamped by it; a private (test) clock begins its
 * task here.
 */
export function afterYield(clock: TaskClock): void {
  if (clock !== taskClock) clock.begin();
}

/** What `yieldTask` schedules with (the global scope; tests pass a model event loop). */
export interface TaskHost {
  readonly setImmediate?: (callback: () => void) => unknown;
  readonly setTimeout: (callback: () => void, ms: number) => unknown;
  readonly MessageChannel?: new () => MessageChannel;
  readonly now: () => number;
}

/** In a browser or worker, yields take a timer turn at least this often (ms). */
export const TIMER_TURN_MS = 50;

/**
 * A `yieldTask` over `host`. The returned Promise resolves in a later **task** (macrotask), never
 * a microtask:
 * - **Node** (`setImmediate`): the check phase follows the timers phase on every loop iteration,
 *   so due timers and pending I/O run before the work resumes.
 * - **Browsers and workers**: one shared `MessageChannel` message (no 4 ms timer clamp), except
 *   that once `TIMER_TURN_MS` has passed without a timer turn the yield is `setTimeout(…, 0)`.
 *   A user agent may service message tasks ahead of timers indefinitely (WebKit was seen to;
 *   review B5-p2d-1), so a query yielding only through messages could starve the owner's timers
 *   (liveness, idle flush, transaction timeouts). Timers due before ours run before it, so every
 *   timer due by then has run when the work resumes: a due timer waits about `TIMER_TURN_MS`
 *   plus one slice at most, whatever the task-source order.
 * `#lastTimerTurn` is the worker's event-loop state, shared by every pipeline on it on purpose
 * (not query scratch, R23): one timer turn serves them all.
 */
export function createYieldTask(host: TaskHost): () => Promise<void> {
  type Resolver = () => void;
  let channel: MessageChannel | null = null;
  const waiting: Resolver[] = [];
  let lastTimerTurn = host.now();
  const immediate = host.setImmediate;
  return () =>
    new Promise<void>((resolved) => {
      // A task begins as the yield resolves (R23.5), before anything awaiting it resumes.
      const resolve = () => {
        taskClock.begin();
        resolved();
      };
      if (immediate !== undefined) {
        immediate(resolve);
        return;
      }
      const Channel = host.MessageChannel;
      if (Channel === undefined || host.now() - lastTimerTurn >= TIMER_TURN_MS) {
        host.setTimeout(() => {
          lastTimerTurn = host.now();
          resolve();
        }, 0);
        return;
      }
      if (channel === null) {
        channel = new Channel();
        channel.port1.onmessage = () => {
          waiting.shift()?.();
        };
      }
      waiting.push(resolve);
      channel.port2.postMessage(null);
    });
}

const globalHost: TaskHost = {
  ...(typeof setImmediate === "function"
    ? { setImmediate: (callback: () => void) => setImmediate(callback) }
    : {}),
  setTimeout: (callback, ms) => setTimeout(callback, ms),
  ...(typeof MessageChannel === "function" ? { MessageChannel } : {}),
  now: () => performance.now(),
};

/** Resolves in a later task (see `createYieldTask`), over the global scope. */
export const yieldTask: () => Promise<void> = createYieldTask(globalHost);

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
