import type { BlockStore } from "../storage/types.js";

const queues = new Map<BlockStore | string, Promise<unknown>>();
/**
 * Lock names whose holder would not let go: once one write has waited the whole admission
 * wait for nothing, the writes after it ask for the lock only if it is free right now, and go
 * ahead uncoordinated otherwise, instead of each waiting the full wait in turn. The first
 * ordinary grant clears the mark, because a grant proves the holder let go.
 */
const bypassing = new Set<string>();

/**
 * How long a write waits without progress on the cross-tab admission lock before going ahead
 * without it. The lock is an optimization — it spares concurrent writers the rebase retries they
 * would otherwise spend on each other — never the correctness: every commit is still a
 * compare-and-swap against the store. A busy queue may take longer while holders keep changing;
 * a holder that never lets go is still bypassed after this one bounded interval.
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
  const key = name ?? store;
  const preceding = queues.get(key) ?? Promise.resolve();
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
  const operation = preceding.then(async () => {
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
        async (lock): Promise<readonly [T] | undefined> => {
          if (lock === null) return;
          bypassing.delete(name);
          return [await enter()] as const;
        },
      );
      if (result !== undefined) return result[0];
      return enter();
    }
    const startedAt = Date.now();
    const lockRequest = locks.request(lockName, { signal: lockController.signal }, enter);
    const armWait = (): ReturnType<typeof setTimeout> =>
      setTimeout(() => lockController.abort(), admissionWaitMs);
    let waitTimer = armWait();
    let holder: string | null | undefined;
    let progressTimer: ReturnType<typeof setTimeout> | undefined;
    const waiting = (): boolean => !admitted && !lockController.signal.aborted;
    const checkProgress = async (): Promise<void> => {
      try {
        const snapshot = await locks.query();
        if (!waiting()) return;
        const nextHolder = snapshot.held?.find((lock) => lock.name === lockName)?.clientId ?? null;
        if (holder !== undefined && nextHolder !== holder) {
          clearTimeout(waitTimer);
          waitTimer = armWait();
        }
        holder = nextHolder;
      } catch {
        // The independent deadline still bounds a broken or unavailable queue probe.
      }
      if (!waiting()) return;
      // The async callback catches the queue probe; its promise cannot reject.
      // eslint-disable-next-line @typescript-eslint/no-misused-promises
      progressTimer = setTimeout(checkProgress, admissionWaitMs / 10);
    };
    void checkProgress();
    try {
      return await lockRequest;
    } catch (error) {
      // The lock may still have been granted in the same instant the wait ran out; then the
      // callback ran and its outcome is what surfaces. Only a refusal to grant is retried
      // without the lock.
      if (!lockController.signal.aborted || admitted) throw error;
    } finally {
      lockController.abort();
      clearTimeout(waitTimer);
      clearTimeout(progressTimer);
    }
    signal.throwIfAborted();
    bypassing.add(name);
    options.onAdmissionWaitExceeded?.(Date.now() - startedAt);
    return enter();
  });
  const settled = operation.catch(() => undefined);
  queues.set(key, settled);
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
  void settled.then(() => {
    if (queues.get(key) === settled) queues.delete(key);
    signal.removeEventListener("abort", abort);
  });
  return Promise.race([operation, cancelled]);
}
