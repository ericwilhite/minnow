import type { BlockStore } from "../storage/types.js";

const anonymous = new WeakMap<BlockStore, { tail: Promise<unknown> }>();
const named = new Map<string, { tail: Promise<unknown> }>();
/**
 * Lock names whose holder would not let go: once one write has waited the whole admission
 * wait for nothing, the writes after it ask for the lock only if it is free right now, and go
 * ahead uncoordinated otherwise, instead of each waiting the full wait in turn. The first
 * ordinary grant clears the mark, because a grant proves the holder let go.
 */
const bypassing = new Set<string>();
/** What an `ifAvailable` request answers when the lock is held: nothing ran under it. */
const NOT_GRANTED: unique symbol = Symbol("write admission lock not granted");

/**
 * How long a write waits for the cross-tab admission lock before going ahead without it. The
 * lock is an optimization — it spares concurrent writers the rebase retries they would
 * otherwise spend on each other — never the correctness: every commit is still a
 * compare-and-swap against the store, so an uncoordinated write conflicts and retries rather
 * than corrupting anything. A holder that never lets go (a tab the browser paused with the lock
 * in hand) must therefore not stall every other tab's writes until each of them times out.
 */
export const WRITE_ADMISSION_WAIT_MS = 10_000;

/** Test-only: forgets every lock name marked as held by a holder that would not let go. */
export function _resetWriteAdmissionForTests(): void {
  bypassing.clear();
}

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
    const lockName = `minnowdb-write:${name}`;
    if (bypassing.has(name)) {
      // A holder that would not let go was already waited out once. Take the lock only if it
      // is free this instant — a grant means the holder is gone, and coordination resumes —
      // and otherwise go ahead uncoordinated at once rather than wait the whole wait again.
      const result = await locks.request(
        lockName,
        { ifAvailable: true },
        async (lock): Promise<T | typeof NOT_GRANTED> => {
          if (lock === null) return NOT_GRANTED;
          bypassing.delete(name);
          return enter();
        },
      );
      if (result !== NOT_GRANTED) return result;
      signal.throwIfAborted();
      return await enter();
    }
    const startedAt = Date.now();
    const wait = { ranOut: false };
    const waitTimer = setTimeout(() => {
      wait.ranOut = true;
      lockController.abort(new Error("Write admission wait ran out"));
    }, admissionWaitMs);
    (waitTimer as { unref?: () => void }).unref?.();
    try {
      return await locks.request(lockName, { signal: lockController.signal }, enter);
    } catch (error) {
      // The lock may still have been granted in the same instant the wait ran out; then the
      // callback ran and its outcome is what surfaces. Only a refusal to grant is retried
      // without the lock.
      if (!wait.ranOut || admitted) throw error;
      signal.throwIfAborted();
      bypassing.add(name);
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
