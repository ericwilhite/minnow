/**
 * Bounded slices of synchronous work (ruling R23.2), shared by the heavy operators: a deadline
 * (`Slice`), jobs written as generators that yield when it is due or when they block
 * (`SlicedJob`), and the runner that gives the event loop a real task between slices.
 */
import { yieldTask } from "./pipeline.js";

/**
 * The deadline of one slice of synchronous work (R16.4, R23.2): `due()` once `ms` have passed
 * since the slice began. Long loops check it per chunk of rows, never per element.
 */
export class Slice {
  #start: number;
  constructor(
    readonly ms: number,
    readonly now: () => number = () => performance.now(),
  ) {
    this.#start = now();
  }
  due(): boolean {
    return this.now() - this.#start >= this.ms;
  }
  reset(): void {
    this.#start = this.now();
  }
}

/** Rows a sliced loop runs between deadline checks. */
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
        await yieldTask();
        slice.reset();
      } else await blocked;
      between();
      step = job.next();
      if (step.done === true) return step.value;
    }
  })();
}

/** Runs a job that never blocks to completion, synchronously (no deadline). */
export function runToEnd<T>(job: SlicedJob<T>): T {
  for (;;) {
    const step = job.next();
    if (step.done === true) return step.value;
    if (step.value !== undefined) throw new Error("A synchronous sort job blocked");
  }
}
