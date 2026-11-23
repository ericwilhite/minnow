/**
 * A test-build guard for retry loops that never yield to the event loop.
 *
 * A loop that admits memory, finds it short and asks again can spin in microtasks forever: each
 * admission is served at once, so the governor never sees a queued request, and no timer (the
 * stall check, a test's timeout) ever runs again. A write step waiting for rows did, at a 40 MiB
 * cap, until `WriteCredits.holdRowsWhileWaiting`: reclaim for its own admission returned the rows
 * it already held. Each guarded loop counts the rounds it goes without the progress it waits for
 * (a round that advances it starts the count again); past the limit it throws `RetrySpinError`,
 * so any such loop in any suite fails loudly instead of hanging. `RetrySpinError` is an
 * invariant failure: no handler retries or recovers from it (the write path fails with it).
 *
 * With `MINNOW_RETRY_GUARD=<limit>` in the environment (vitest.config.ts sets it for every
 * suite), or `globalThis.__MINNOW_RETRY_GUARD__` set to a number before the engine loads (a
 * browser harness or a bundler define), the guard is on. Off, it is one branch on a constant.
 */

const environment = (globalThis as { process?: { env?: Record<string, string | undefined> } })
  .process?.env;

function configuredLimit(): number | null {
  const defined = (globalThis as { __MINNOW_RETRY_GUARD__?: unknown }).__MINNOW_RETRY_GUARD__;
  if (typeof defined === "number" && Number.isSafeInteger(defined) && defined > 0) return defined;
  const value = Number(environment?.MINNOW_RETRY_GUARD ?? "");
  return Number.isSafeInteger(value) && value > 0 ? value : null;
}

/** Rounds a guarded loop may go without progress (test builds only; null: off). */
export const RETRY_GUARD_LIMIT: number | null = configuredLimit();

/** A retry loop went round more often than any loop that makes progress does. */
export class RetrySpinError extends Error {
  readonly code = "MINNOW_RETRY_SPIN";
  constructor(
    readonly label: string,
    readonly rounds: number,
  ) {
    super(
      `${label}: ${String(rounds)} retries without progress (a loop that never yields; test builds only)`,
    );
    this.name = "RetrySpinError";
  }
}

/** Counts one loop's rounds without progress. */
export interface RetryGuard {
  /** One more round without the progress the loop waits for (a loop with no measure of its
   * progress: each round needs another owner's change, so every round counts). */
  retry(): void;
  /**
   * One more round, measured: `progress` is a quantity that rises as the loop gets closer to
   * done (credit funded, a requested size). A round that raises it above every earlier one
   * starts the count again; only rounds without progress count toward the limit.
   */
  round(progress: number): void;
}

const OFF: RetryGuard = { retry: () => undefined, round: () => undefined };

/** A guard for one run of one loop, labelled for its error. */
export function retryGuard(label: string, limit = RETRY_GUARD_LIMIT): RetryGuard {
  if (limit === null) return OFF;
  let rounds = 0;
  let best = -Infinity;
  const retry = () => {
    rounds++;
    if (rounds > limit) throw new RetrySpinError(label, rounds);
  };
  return {
    retry,
    round(progress) {
      if (progress > best) {
        best = progress;
        rounds = 0;
        return;
      }
      retry();
    },
  };
}

/** For a loop written as recursion: throws when `rounds` (counted by the caller, without
 * progress) passes it. */
export function checkRetries(label: string, rounds: number, limit = RETRY_GUARD_LIMIT): void {
  if (limit !== null && rounds > limit) throw new RetrySpinError(label, rounds);
}
