/**
 * Ownership acquisition shared by every backend: the Web Lock, then the backend's fence, with
 * bounded retry. The OPFS backend passes a function that opens its sync access handles; the
 * simulated disk passes one that opens simulated handles. Behaviour pinned by the browser probe
 * (2026-11-05): the lock frees 2 ms–2 s before the handle of a dead owner, and a lock steal does
 * not free a live owner's handle.
 */
import { OwnerUnresponsiveError, OwnershipLostError, isBusyError } from "./contract.js";

/** The slice of the Web Locks API ownership needs (`navigator.locks` adapts to it). */
export interface LockManagerLike {
  /**
   * Resolves when `name` is held. With `steal`, takes it from the current holder, whose `lost`
   * signal aborts (the platform's AbortError on its request). `signal` cancels a wait.
   */
  request(name: string, options: { steal?: boolean; signal?: AbortSignal }): Promise<HeldLock>;
}

export interface HeldLock {
  /** Aborted when another context steals the lock. */
  readonly lost: AbortSignal;
  /** Releases the lock if still held. Idempotent. */
  release(): void;
}

export const DEFAULT_ACQUIRE_TIMEOUT_MS = 5_000;
const FIRST_RETRY_MS = 1;
const MAX_RETRY_MS = 100;

/** Re-reads the lock's state after an await (the open may have taken a while). */
const stolen = (lock: HeldLock): boolean => lock.lost.aborted;

const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

type Outcome<T> =
  | { readonly kind: "opened"; readonly fence: T }
  | { readonly kind: "failed"; readonly error: unknown }
  | { readonly kind: "deadline" };

/** Settles with the open's outcome, or `deadline` after `ms` (the open keeps running). */
function withDeadline<T>(attempt: Promise<T>, ms: number): Promise<Outcome<T>> {
  return new Promise((resolve) => {
    const timer = setTimeout(
      () => {
        resolve({ kind: "deadline" });
      },
      Math.max(0, ms),
    );
    attempt.then(
      (fence) => {
        clearTimeout(timer);
        resolve({ kind: "opened", fence });
      },
      (error: unknown) => {
        clearTimeout(timer);
        resolve({ kind: "failed", error });
      },
    );
  });
}

/**
 * Takes the lock, then opens the backend fence until it stops failing busy (`isBusyError`),
 * backing off from 1 ms to 100 ms, all within `timeoutMs`.
 *
 * - `openFence` may be synchronous or return a promise (`createSyncAccessHandle` does; so does an
 *   IndexedDB epoch bump). The deadline applies while it is pending: if it passes, acquisition
 *   fails with `OwnerUnresponsiveError` and the fence, if it opens later, is closed at once.
 * - If the lock is stolen while the fence opens, the opened fence is closed and acquisition fails
 *   with `OwnershipLostError`: a newer owner is acquiring.
 * - Every failure releases the lock; a non-busy error is rethrown.
 */
export async function acquireFenced<T>(
  locks: LockManagerLike,
  name: string,
  openFence: () => T | Promise<T>,
  closeFence: (fence: T) => void,
  options: { steal?: boolean; timeoutMs?: number } = {},
): Promise<{ readonly lock: HeldLock; readonly fence: T }> {
  const timeoutMs = options.timeoutMs ?? DEFAULT_ACQUIRE_TIMEOUT_MS;
  const deadline = Date.now() + timeoutMs;
  const waiting = new AbortController();
  const timer = setTimeout(() => {
    waiting.abort();
  }, timeoutMs);
  let lock: HeldLock;
  try {
    lock = await locks.request(name, {
      ...(options.steal === true ? { steal: true } : {}),
      signal: waiting.signal,
    });
  } catch (error) {
    if (waiting.signal.aborted) throw new OwnerUnresponsiveError(timeoutMs);
    throw error;
  } finally {
    clearTimeout(timer);
  }
  let delay = FIRST_RETRY_MS;
  for (;;) {
    if (lock.lost.aborted) {
      lock.release();
      throw new OwnershipLostError(0);
    }
    const attempt = Promise.resolve().then(openFence);
    const outcome = await withDeadline(attempt, deadline - Date.now());
    if (outcome.kind === "deadline") {
      attempt.then(closeFence, () => undefined);
      lock.release();
      throw new OwnerUnresponsiveError(timeoutMs);
    }
    if (outcome.kind === "opened") {
      if (!stolen(lock)) return { lock, fence: outcome.fence };
      closeFence(outcome.fence);
      lock.release();
      throw new OwnershipLostError(0);
    }
    if (!isBusyError(outcome.error) || Date.now() + delay > deadline) {
      lock.release();
      throw isBusyError(outcome.error) ? new OwnerUnresponsiveError(timeoutMs) : outcome.error;
    }
    await sleep(delay);
    delay = Math.min(delay * 2, MAX_RETRY_MS);
  }
}
