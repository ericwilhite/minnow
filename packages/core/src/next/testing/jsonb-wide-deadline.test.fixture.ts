/** Test liveness follows completed byte work and I/O, never timer ticks or empty yields. */
import type { SlicedJob } from "../exec/slices.js";

export async function jsonbWideDeadline<T>(
  work: Promise<T>,
  activity: Set<() => void>,
  cancel: (error: Error) => void,
): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  let rejectStall: (error: Error) => void = () => undefined;
  const state = { finished: false };
  const reset = () => {
    if (state.finished) return;
    clearTimeout(timer);
    timer = setTimeout(() => {
      state.finished = true;
      const error = new Error("JSONB wide made no progress for 30000 ms");
      try {
        cancel(error);
      } finally {
        rejectStall(error);
      }
    }, 30000);
  };
  const stalled = new Promise<never>((_, reject) => {
    rejectStall = reject;
  });
  try {
    activity.add(reset);
    reset();
    return await Promise.race([work, stalled]);
  } finally {
    state.finished = true;
    clearTimeout(timer);
    activity.delete(reset);
  }
}

/** A generator yielding without advancing its input cursor is not progress. */
export function* jsonbByteProgress(
  job: SlicedJob<void>,
  position: () => number,
  advance: () => void,
): SlicedJob<void> {
  let previous = position(),
    finished = false;
  try {
    for (;;) {
      const step = job.next(),
        current = position();
      if (current > previous) advance();
      previous = current;
      if (step.done) {
        finished = true;
        return;
      }
      yield step.value;
    }
  } finally {
    if (!finished) job.return(undefined);
  }
}
