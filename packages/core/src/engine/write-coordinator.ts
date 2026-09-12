import type { BlockStore } from "../storage/types.js";

const anonymous = new WeakMap<BlockStore, { tail: Promise<unknown> }>();
const named = new Map<string, { tail: Promise<unknown> }>();

/**
 * How long a write waits for the cross-tab admission lock before going ahead without it. The
 * lock is an optimization — it spares concurrent writers the rebase retries they would
 * otherwise spend on each other — never the correctness: every commit is still a
 * compare-and-swap against the store, so an uncoordinated write conflicts and retries rather
 * than corrupting anything. A holder that never lets go (a tab the browser paused with the lock
 * in hand) must therefore not stall every other tab's writes until each of them times out.
 */
export const WRITE_ADMISSION_WAIT_MS = 10_000;

export interface CoordinateWriteOptions {
  /** Test seam: the cross-tab lock wait; default `WRITE_ADMISSION_WAIT_MS`. */
  admissionWaitMs?: number;
  /** Hears a wait that ran out, with the write then proceeding uncoordinated. */
  onAdmissionWaitExceeded?: (waitedMs: number) => void;
}

/** Admit autocommit before it captures a snapshot. Explicit callbacks never hold this lock. */
export async function coordinateWrite<T>(
  store: BlockStore,
  run: () => Promise<T>,
  signal: AbortSignal,
  options: CoordinateWriteOptions = {},
): Promise<T> {
  signal.throwIfAborted();
  const name = store.liveQueryChannelName;
  let queue = name === undefined ? anonymous.get(store) : named.get(name);
  if (queue === undefined) {
    queue = { tail: Promise.resolve() };
    if (name === undefined) anonymous.set(store, queue);
    else named.set(name, queue);
  }
  const locks = typeof navigator === "undefined" ? undefined : navigator.locks;
  // A request-local signal avoids retaining completed lock requests on the engine lifetime.
  const lockController = new AbortController();
  let admitted = false;
  const enter = (): Promise<T> => {
    signal.throwIfAborted();
    admitted = true;
    return run();
  };
  const admissionWaitMs = options.admissionWaitMs ?? WRITE_ADMISSION_WAIT_MS;
  const operation = queue.tail.then(async () => {
    signal.throwIfAborted();
    if (name === undefined || locks === undefined) return enter();
    const startedAt = Date.now();
    const wait = { ranOut: false };
    const waitTimer = setTimeout(() => {
      wait.ranOut = true;
      lockController.abort(new Error("Write admission wait ran out"));
    }, admissionWaitMs);
    (waitTimer as { unref?: () => void }).unref?.();
    try {
      return await locks.request(
        `minnowdb-write:${name}`,
        { signal: lockController.signal },
        enter,
      );
    } catch (error) {
      // The lock may still have been granted in the same instant the wait ran out; then the
      // callback ran and its outcome is what surfaces. Only a refusal to grant is retried
      // without the lock.
      if (!wait.ranOut || admitted) throw error;
      signal.throwIfAborted();
      options.onAdmissionWaitExceeded?.(Date.now() - startedAt);
      return await enter();
    } finally {
      clearTimeout(waitTimer);
    }
  });
  const settled = operation.then(
    () => undefined,
    () => undefined,
  );
  queue.tail = settled;
  void settled.then(() => {
    if (queue.tail !== settled) return;
    if (name === undefined) anonymous.delete(store);
    else named.delete(name);
  });
  // Closing a waiting engine does not wait for a frozen lock holder. Once storage work starts,
  // drain it to a known outcome before allowing the caller to close its adapter.
  let abort = (): void => undefined;
  const cancelled = new Promise<never>((_resolve, reject) => {
    abort = () => {
      if (!admitted) {
        lockController.abort(signal.reason);
        reject(
          signal.reason instanceof Error
            ? signal.reason
            : new Error("Write admission cancelled", { cause: signal.reason }),
        );
      }
    };
    signal.addEventListener("abort", abort, { once: true });
  });
  try {
    return await Promise.race([operation, cancelled]);
  } finally {
    signal.removeEventListener("abort", abort);
  }
}
