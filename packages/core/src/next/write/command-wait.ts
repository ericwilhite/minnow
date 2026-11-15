/** Command waits for independently owned committed maintenance. Never abandon staging or a
 * provisional writer here. One cached completion observer per maintenance promise prevents
 * cancelled commands from accumulating reactions on a stalled native job. The one active
 * writer supplies at most one waiter; abort removes its complete record immediately. */
interface Waiting {
  readonly signal: AbortSignal;
  readonly abort: () => void;
  readonly resolve: () => void;
  readonly reject: (error: unknown) => void;
}
interface Completion {
  outcome: { readonly failed: boolean; readonly reason: unknown } | null;
  waiting: Waiting | null;
}
/** Native owner object overhead funds its bounded appender/flush completion cells. Neither
 * keys nor values retain a command after cancellation. Finished jobs are weakly keyed. */
const completions = new WeakMap<Promise<void>, Completion>();
function completion(work: Promise<void>): Completion {
  const cached = completions.get(work);
  if (cached) return cached;
  const cell: Completion = { outcome: null, waiting: null };
  completions.set(work, cell);
  const finish = (failed: boolean, reason?: unknown) => {
    cell.outcome = { failed, reason };
    const current = cell.waiting;
    cell.waiting = null;
    if (!current) return;
    current.signal.removeEventListener("abort", current.abort);
    if (failed) current.reject(reason);
    else current.resolve();
  };
  work.then(
    () => finish(false),
    (error: unknown) => finish(true, error),
  );
  return cell;
}
export function waitCommandMaintenance(work: Promise<void>, signal: AbortSignal): Promise<void> {
  // eslint-disable-next-line @typescript-eslint/prefer-promise-reject-errors -- Preserve the exact caller cancellation reason.
  if (signal.aborted) return Promise.reject(signal.reason as unknown);
  const cell = completion(work);
  if (cell.outcome) {
    // eslint-disable-next-line @typescript-eslint/prefer-promise-reject-errors -- Preserve the native owner's original failure identity.
    return cell.outcome.failed ? Promise.reject(cell.outcome.reason) : Promise.resolve();
  }
  if (cell.waiting)
    return Promise.reject(new TypeError("Concurrent native maintenance command wait"));
  return new Promise<void>((resolve, reject) => {
    const abort = () => {
      const current = cell.waiting;
      if (current?.abort !== abort) return;
      cell.waiting = null;
      current.signal.removeEventListener("abort", abort);
      current.reject(current.signal.reason as unknown);
    };
    cell.waiting = { signal, abort, resolve, reject };
    signal.addEventListener("abort", abort, { once: true });
    if (signal.aborted) abort();
  });
}

/** Cache the native operation too: repeatedly calling an async flush factory would itself
 * accumulate wrappers on its stalled internal job, even with a cached completion observer. */
export class CommandMaintenance {
  #active: Promise<void> | null = null;
  wait(start: () => Promise<void>, signal: AbortSignal): Promise<void> {
    signal.throwIfAborted();
    let work = this.#active;
    if (!work) {
      work = start();
      this.#active = work;
      const retire = () => {
        if (this.#active === work) this.#active = null;
      };
      work.then(retire, retire);
    }
    return waitCommandMaintenance(work, signal);
  }
}
