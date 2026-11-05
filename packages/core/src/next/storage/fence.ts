/**
 * Ownership acquisition shared by every backend: the Web Lock, then the backend's fence, with
 * bounded retry. The OPFS backend passes a function that opens its sync access handles; the
 * simulated disk passes one that opens simulated handles. Behaviour pinned by the browser probe
 * (2026-11-05): the lock frees 2 ms–2 s before the handle of a dead owner, and a lock steal does
 * not free a live owner's handle.
 */
import { OwnerUnresponsiveError, isBusyError } from "./contract.js";

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

const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

/**
 * Takes the lock, then calls `openFence` until it stops throwing a busy error (`isBusyError`),
 * backing off from 1 ms to 100 ms, for at most `timeoutMs` in total. Any other error, or the
 * timeout, releases the lock and rejects (the timeout as `OwnerUnresponsiveError`).
 */
export async function acquireFenced<T>(
  locks: LockManagerLike,
  name: string,
  openFence: () => T,
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
    // Stolen while we waited for the fence: someone newer is acquiring; step aside.
    if (lock.lost.aborted) throw new OwnerUnresponsiveError(timeoutMs);
    try {
      return { lock, fence: openFence() };
    } catch (error) {
      if (!isBusyError(error) || Date.now() + delay > deadline) {
        lock.release();
        throw isBusyError(error) ? new OwnerUnresponsiveError(timeoutMs) : error;
      }
    }
    await sleep(delay);
    delay = Math.min(delay * 2, MAX_RETRY_MS);
  }
}
