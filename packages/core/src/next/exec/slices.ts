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
    this.#last = this.#start;
  }
  due(): boolean {
    const now = this.now();
    this.#last = now;
    return now - this.#start >= this.ms;
  }
  /** When the slice was last used (a deadline check, a reset or a resume). */
  #last = 0;
  /**
   * An operator's entry point (a push, a finish): continues the current slice, so a chain of
   * short calls resumed by already-resolved `ready()`s comes due and yields a task (R23.4).
   * Only when the slice sat unused for at least `ms` — the time went to other work, or the
   * caller yielded — does a new slice start, so a short call after an idle wait still runs
   * synchronously.
   */
  resume(): void {
    const now = this.now();
    if (now - this.#last >= this.ms) {
      this.#start = now;
      this.resets++;
    }
    this.#last = now;
  }
  #ticks = 0;
  /**
   * One unit of comparator-bound work (a comparison, whose cost a row count does not bound:
   * wide keys, decoded trees); true when the slice is due, checked every `TICKS` units.
   */
  tick(): boolean {
    return (++this.#ticks & (TICKS - 1)) === 0 && this.due();
  }
  /** How many times the slice was reset (a new slice began): `KeyWork` checks yields by it. */
  resets = 0;
  reset(): void {
    this.#start = this.now();
    this.#last = this.#start;
    this.resets++;
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
        await yieldTask();
        slice.reset();
      } else await blocked;
      between();
      step = job.next();
      if (step.done === true) return step.value;
    }
  })();
}

/**
 * Runs a job that never blocks to completion, synchronously (no deadline). `slice`, when the
 * job has one that can come due, is reset at each of its yields, as `runSliced` does.
 */
export function runToEnd<T>(job: SlicedJob<T>, slice?: Slice): T {
  for (;;) {
    const step = job.next();
    if (step.done === true) return step.value;
    if (step.value !== undefined) throw new Error("A synchronous sort job blocked");
    slice?.reset();
  }
}
