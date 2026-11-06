/** Shared worker task clock. exec/pipeline.ts reexports it; storage uses this lower layer. */
export const YIELD_INTERVAL_MS = 4;

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

/** Optional cancellation and explicit clock/scheduler injection; defaults are worker-wide. */
export interface TaskWork {
  readonly signal?: AbortSignal;
  readonly clock?: TaskClock;
  /** Tests/hosts only: must resolve in a genuine later task, never a microtask. */
  readonly yieldTask?: () => Promise<void>;
}

/** Does not stamp on entry or on resolved I/O. A missing owner stamp yields early (R23.5). */
export function pauseIfDue(work?: TaskWork): Promise<void> | undefined {
  work?.signal?.throwIfAborted();
  const clock = work?.clock ?? taskClock;
  if (!clock.due()) return undefined;
  return (work?.yieldTask ?? yieldTask)().then(() => {
    if (work?.yieldTask !== undefined || clock !== taskClock) clock.begin();
    work?.signal?.throwIfAborted();
  });
}

/** In-place heapsort: constant scratch, bounded loop checks, no uninterruptible native sort. */
export async function sortInSlices<T>(
  values: T[],
  compare: (a: T, b: T) => number,
  work?: TaskWork,
): Promise<void> {
  const item = (at: number): T => {
    const value = values[at];
    if (value === undefined) throw new RangeError("A sliced sort requires a dense array");
    return value;
  };
  const swap = (a: number, b: number) => {
    const value = item(a);
    values[a] = item(b);
    values[b] = value;
  };
  const sift = (root: number, end: number) => {
    while (root * 2 + 1 < end) {
      let child = root * 2 + 1;
      if (child + 1 < end && compare(item(child), item(child + 1)) < 0) child++;
      if (compare(item(root), item(child)) >= 0) return;
      swap(root, child);
      root = child;
    }
  };
  const check = pauseIfDue(work);
  if (check !== undefined) await check;
  for (let start = Math.floor(values.length / 2) - 1; start >= 0; start--) {
    if (start % 128 === 0) {
      const pending = pauseIfDue(work);
      if (pending !== undefined) await pending;
    }
    sift(start, values.length);
  }
  for (let end = values.length - 1; end > 0; end--) {
    if (end % 128 === 0) {
      const pending = pauseIfDue(work);
      if (pending !== undefined) await pending;
    }
    swap(0, end);
    sift(0, end);
  }
  const pending = pauseIfDue(work);
  if (pending !== undefined) await pending;
}
