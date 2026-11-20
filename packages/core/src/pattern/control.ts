/**
 * Resumable regex execution. The matcher's step loops are generators: every `REGEX_SLICE_STEPS`
 * work steps they yield, so a caller can return to its scheduler and keep any one synchronous
 * slice short. The released engine drives a job to completion without pausing (`runRegex`), so its
 * results, errors and step counts are unchanged.
 *
 * Scratch: the matcher registers each structure that grows with its work (visited states, branch
 * stacks, the accepted-path trace, the dissection memo and its endpoint walks). `RegexScratch.live`
 * is their current entry count times `REGEX_SCRATCH_ENTRY_BYTES`. One work step adds at most
 * `REGEX_STEP_ENTRIES` entries, so a caller that has funded `live + slice x step growth` before
 * resuming has charged every allocation of that slice before it happens.
 */

/** Work steps between yields: about 0.2 ms at the measured 6 M steps per second. */
export const REGEX_SLICE_STEPS = 1024;
/** A conservative size for one map/set entry, stack frame or memo cell (with its key). */
export const REGEX_SCRATCH_ENTRY_BYTES = 128;
/** The most scratch entries one work step can add (a visited state, a trace link, a branch
 * frame and a memo or endpoint entry). */
export const REGEX_STEP_ENTRIES = 4;

export type RegexJob<T> = Generator<void, T, void>;

export interface RegexControl {
  /** The fixed per-operation work ceiling (MAX_SQL_PATTERN_MATCH_STEPS). */
  readonly work: { remaining: number };
  /** Steps left in this slice; Infinity never pauses. */
  slice: number;
  readonly scratch?: RegexScratch;
}

export class RegexScratch {
  readonly #sources = new Set<() => number>();
  /** Registers a structure's entry count; returns its unregistration. */
  track(count: () => number): () => void {
    this.#sources.add(count);
    return () => {
      this.#sources.delete(count);
    };
  }
  /** Bytes the registered structures hold now. */
  get live(): number {
    let entries = 0;
    for (const count of this.#sources) entries += count();
    return entries * REGEX_SCRATCH_ENTRY_BYTES;
  }
}

/** Bytes one slice can add on top of `live`. */
export const REGEX_SLICE_GROWTH_BYTES =
  REGEX_SLICE_STEPS * REGEX_STEP_ENTRIES * REGEX_SCRATCH_ENTRY_BYTES;

/** A control that never pauses, for synchronous callers. */
export function unpaused(work: { remaining: number }): RegexControl {
  return { work, slice: Number.POSITIVE_INFINITY };
}

/** Runs a job to completion without pausing. */
export function runRegex<T>(job: RegexJob<T>): T {
  for (;;) {
    const step = job.next();
    if (step.done === true) return step.value;
  }
}
