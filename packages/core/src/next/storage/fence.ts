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
  /** Snapshot of held names, for native OPFS platforms that deny Blob reads of held files. */
  query?(): Promise<readonly string[]>;
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
export function withDeadline<T>(attempt: Promise<T>, ms: number): Promise<Outcome<T>> {
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

export type PublicationGate = (publish: () => void | Promise<void>) => Promise<void>;

export async function requestUntil(
  locks: LockManagerLike,
  name: string,
  deadline: number,
  timeoutMs: number,
  steal = false,
  signal?: AbortSignal,
): Promise<HeldLock> {
  if (signal?.aborted === true || Date.now() > deadline)
    throw new OwnerUnresponsiveError(timeoutMs);
  const waiting = new AbortController();
  const cancel = () => waiting.abort();
  signal?.addEventListener("abort", cancel, { once: true });
  const timer = setTimeout(
    () => {
      waiting.abort();
    },
    Math.max(0, deadline - Date.now()),
  );
  try {
    const lock = await locks.request(name, {
      ...(steal ? { steal: true } : {}),
      signal: waiting.signal,
    });
    if (waiting.signal.aborted || Date.now() > deadline) {
      lock.release();
      throw new OwnerUnresponsiveError(timeoutMs);
    }
    return lock;
  } catch (error) {
    if (waiting.signal.aborted) throw new OwnerUnresponsiveError(timeoutMs);
    throw error;
  } finally {
    clearTimeout(timer);
    signal?.removeEventListener("abort", cancel);
  }
}

/** Generic lock + asynchronous native-handle acquisition for memory, simulator and owner
 * host election. Native storage compatibility uses native-fence.ts's durable identities.
 * Retries busy opens within one deadline; closes every late/opened-after-loss resource.
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
  const lock = await requestUntil(locks, name, deadline, timeoutMs, options.steal === true);
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
      // A synchronous open can overrun the deadline without the timer ever firing: check after.
      const expired = Date.now() > deadline;
      if (!stolen(lock) && !expired) return { lock, fence: outcome.fence };
      closeFence(outcome.fence);
      lock.release();
      throw expired && !stolen(lock)
        ? new OwnerUnresponsiveError(timeoutMs)
        : new OwnershipLostError(0);
    }
    if (!isBusyError(outcome.error) || Date.now() + delay > deadline) {
      lock.release();
      throw isBusyError(outcome.error) ? new OwnerUnresponsiveError(timeoutMs) : outcome.error;
    }
    await sleep(delay);
    delay = Math.min(delay * 2, MAX_RETRY_MS);
  }
}
