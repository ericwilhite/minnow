/**
 * Web Locks as the owner uses them (election.ts "Locks"):
 * - `webLocks` adapts `navigator.locks` to storage/fence.ts `LockManagerLike`, for the fence lock
 *   and the host lock: the lock is held until `release()`, and `lost` aborts when another context
 *   steals it (the platform rejects the held request with `AbortError`).
 * - `watchLock` queues a request for a lock someone else holds and reports when it is granted:
 *   the holder released it (or died). It releases at once, so watching never blocks the holder's
 *   successor. The owner watches a client's liveness lock with it; a client watches the host lock
 *   while it needs an owner.
 */
import type { HeldLock, LockManagerLike } from "../storage/fence.js";

/** The slice of `navigator.locks` used here. */
export interface WebLockManager {
  request(
    name: string,
    options: {
      readonly steal?: boolean;
      readonly signal?: AbortSignal;
      readonly ifAvailable?: boolean;
    },
    callback: (lock: unknown) => Promise<void> | void,
  ): Promise<unknown>;
  /** The origin's locks; `clientId` names the holding context (owner registration, broker). */
  query?(): Promise<{
    readonly held?: ReadonlyArray<{ readonly name?: string; readonly clientId?: string }>;
  }>;
}

const toError = (reason: unknown): Error =>
  reason instanceof Error ? reason : new Error(String(reason));

export function webLocks(locks: WebLockManager): LockManagerLike {
  return {
    request(name, options): Promise<HeldLock> {
      return new Promise<HeldLock>((resolve, reject) => {
        const lost = new AbortController();
        let granted = false;
        let release: () => void = () => undefined;
        const held = new Promise<void>((done) => {
          release = done;
        });
        // `steal` cannot be combined with `signal` (NotSupportedError, probe table 3).
        const requestOptions =
          options.steal === true
            ? { steal: true }
            : options.signal === undefined
              ? {}
              : { signal: options.signal };
        locks
          .request(name, requestOptions, () => {
            granted = true;
            resolve({
              lost: lost.signal,
              release: () => {
                release();
              },
            });
            return held;
          })
          .catch((error: unknown) => {
            if (granted) lost.abort(toError(error));
            else reject(toError(error));
          });
      });
    },
  };
}

/**
 * Queues for `name` and aborts `gone` when it is granted (the holder let go). `stop()` cancels the
 * wait. The grant is released at once.
 */
export function watchLock(
  locks: WebLockManager,
  name: string,
): { readonly gone: AbortSignal; stop(): void } {
  const gone = new AbortController();
  const waiting = new AbortController();
  let granted = false;
  locks
    .request(name, { signal: waiting.signal }, () => {
      granted = true;
    })
    // Signalled only once the grant is released again: a watcher acting on `gone` (asking the
    // broker, electing) must not find the lock held by its own watch.
    .then(() => {
      if (granted) gone.abort(new Error(`${name} was released`));
    })
    .catch(() => undefined);
  return {
    gone: gone.signal,
    stop: () => {
      waiting.abort();
    },
  };
}
