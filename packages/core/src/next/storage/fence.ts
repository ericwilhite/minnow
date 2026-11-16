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

/** Separate namespace: no database name can make this equal an owner lock (`minnow:`). */
const gateName = (ownerName: string): string => `minnow-gate:${ownerName}`;

export type PublicationGate = (publish: () => void | Promise<void>) => Promise<void>;

/** Root publication and stealing preflight use the same NON-stealing exclusive gate.
 * A publisher queued behind a stealing opener must not write after losing its owner lease.
 */
export function publicationGate(
  locks: LockManagerLike,
  ownerName: string,
  signal: AbortSignal,
): PublicationGate {
  // Do not retain a pre-await narrowing of AbortSignal.aborted across lock acquisition.
  const ownerLost = () => signal.aborted;
  return async (publish) => {
    if (ownerLost()) throw new OwnershipLostError(0);
    let gate: HeldLock;
    try {
      gate = await locks.request(gateName(ownerName), { signal });
    } catch (error) {
      if (ownerLost()) throw new OwnershipLostError(0);
      throw error;
    }
    try {
      if (ownerLost() || gate.lost.aborted) throw new OwnershipLostError(0);
      await publish();
    } finally {
      gate.release();
    }
  };
}

async function requestUntil(
  locks: LockManagerLike,
  name: string,
  deadline: number,
  timeoutMs: number,
  steal = false,
): Promise<HeldLock> {
  const waiting = new AbortController();
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
  }
}

/**
 * Runs optional non-mutating compatibility preflight before requesting the Web Lock (including
 * a steal). A stealing opener holds the publication gate from before preflight until the owner
 * lock AND backend fence are acquired. This also covers delayed lock-loss delivery to another
 * worker: the epoch/handle fence becomes authoritative before an old publisher can resume.
 * Backend lease loss closes handles without awaiting queued publication. Non-stealing openers must
 * NOT hold the gate while queuing for ownership (the live owner may need it to publish).
 * The same deadline covers gate waiting and preflight; its owner cleans up late resources.
 * Takes the lock, then opens the backend fence until it stops failing busy (`isBusyError`),
 * backing off from 1 ms to 100 ms, all within `timeoutMs`.
 *
 * - `openFence` may be synchronous or return a promise (`createSyncAccessHandle` does; so does an
 *   IndexedDB epoch bump). The deadline applies while it is pending and is checked again after it
 *   opens: past it, acquisition fails with `OwnerUnresponsiveError`, and the fence (opened late,
 *   or opened after the deadline by a slow synchronous call) is closed at once.
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
  preflight?: () => Promise<void>,
): Promise<{ readonly lock: HeldLock; readonly fence: T }> {
  const timeoutMs = options.timeoutMs ?? DEFAULT_ACQUIRE_TIMEOUT_MS;
  const deadline = Date.now() + timeoutMs;
  let gate: HeldLock | undefined;
  let lock: HeldLock;
  try {
    if (preflight !== undefined && options.steal === true)
      gate = await requestUntil(locks, gateName(name), deadline, timeoutMs);
    if (preflight !== undefined) {
      const outcome = await withDeadline(Promise.resolve().then(preflight), deadline - Date.now());
      if (outcome.kind === "failed") throw outcome.error;
      if (outcome.kind === "deadline" || Date.now() > deadline)
        throw new OwnerUnresponsiveError(timeoutMs);
    }
    if (gate?.lost.aborted === true) throw new OwnershipLostError(0);
    lock = await requestUntil(locks, name, deadline, timeoutMs, options.steal === true);
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
  } finally {
    gate?.release();
  }
}
