/**
 * Bounded slices of synchronous work (ruling R23.2), shared by the heavy operators: a deadline
 * (`Slice`), jobs written as generators that yield when it is due or when they block
 * (`SlicedJob`), and the runner that gives the event loop a real task between slices.
 */
import { TaskClock, afterYield, taskClock, yieldTask } from "./pipeline.js";

/**
 * The deadline of synchronous work (R16.4, R23.2, R23.5): `due()` once `ms` have passed since the
 * current **task** began, on the worker's task clock (pipeline.ts `TaskClock`). A slice keeps no
 * start of its own: every operator in a task shares the one bound. Long loops check it per chunk
 * of rows, never per element. `Slice(0)` is always due, `Slice(Infinity)` never; tests pass a
 * private clock.
 */
export class Slice {
  constructor(
    readonly ms: number,
    readonly clock: TaskClock = taskClock,
  ) {}
  due(): boolean {
    return this.clock.due(this.ms);
  }
  #ticks = 0;
  /**
   * One unit of comparator-bound work (a comparison, whose cost a row count does not bound:
   * wide keys, decoded trees); true when the slice is due, checked every `TICKS` units.
   */
  tick(): boolean {
    return (++this.#ticks & (TICKS - 1)) === 0 && this.due();
  }
}

/** Comparisons between deadline checks (`Slice.tick`). */
export const TICKS = 8;

/** Rows a sliced loop runs between deadline checks (cheap, fixed-cost work per row). */
export const SLICE_ROWS = 8192;

/**
 * A sliced job: yields nothing when its slice is due, or a Promise it is blocked on (a
 * downstream's `ready()`, a progress loan).
 */
export type SlicedJob<T> = Generator<Promise<void> | void, T, void>;

/**
 * Runs a sliced job: synchronously while it neither blocks nor runs out of slice (the result,
 * no Promise). A due slice gets a task yield (`yieldTask`, never a resolved Promise) and a fresh
 * slice; a blocked job awaits its Promise. `between()` (the abort check) runs at every
 * resumption.
 */
export function runSliced<T>(job: SlicedJob<T>, slice: Slice, between: () => void): T | Promise<T> {
  let step = job.next();
  if (step.done === true) return step.value;
  return (async () => {
    for (;;) {
      const blocked = step.value;
      if (blocked === undefined) {
        // The yield begins a task (it stamps the worker's clock; a test clock begins here).
        await yieldTask();
        afterYield(slice.clock);
      } else await blocked;
      between();
      step = job.next();
      if (step.done === true) return step.value;
    }
  })();
}

/**
 * Runs a job that never blocks to completion, synchronously (no deadline: production passes a
 * `Slice(Infinity)`). Tests stepping a job whose slice comes due pass that slice: with a private
 * clock, each yield begins a task on it, as `runSliced`'s yield does (the worker's clock is
 * never stamped here: no task begins in a synchronous loop).
 */
export function runToEnd<T>(job: SlicedJob<T>, slice?: Slice): T {
  for (;;) {
    const step = job.next();
    if (step.done === true) return step.value;
    if (step.value !== undefined) throw new Error("A synchronous sort job blocked");
    if (slice !== undefined) afterYield(slice.clock);
  }
}
