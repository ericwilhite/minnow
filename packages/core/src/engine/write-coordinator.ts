import type { BlockStore } from "../storage/types.js";

/**
 * Writer admission: one logical writer per database among cooperating connections.
 *
 * Every path that publishes a manifest — a `write()` scope, an autocommit batch write, a SQL
 * mutation statement, a SQL `BEGIN` transaction, a catalog change, a compaction publication, a
 * snapshot import — takes a turn here before its first state-dependent read and keeps the turn
 * until its commit or abort has a known outcome. Two stages make up a turn:
 *
 * 1. A local FIFO per store identity, shared by every engine in this JavaScript context that
 *    opened the same store name (or the same store object, for a store without a name).
 * 2. The cross-context Web Lock `minnowdb-write:<identity>`, requested only by the head of the
 *    local queue, so each context holds at most one pending request and contexts take turns in
 *    the order the browser's lock manager grants them.
 *
 * The store's compare-and-swap on the manifest version and the schema epoch stay in force
 * underneath. They are the defensive check for writers that do not take part — an older build,
 * a custom adapter without a stable identity, a test seam — never the ordinary scheduler.
 *
 * A turn is never bypassed. A holder that stops making progress is reported through `onStalled`
 * after `stallReportMs`, and the waiter keeps waiting until the holder lets go, the browser
 * releases the holder's lock (the tab is unloaded or discarded), or the waiter is cancelled
 * through its signal. Taking the turn away from a live holder would need a fence at the
 * storage boundary that neither adapter provides, and a lock that a process-local timeout can
 * defeat is not a lock.
 */
export type WriterKind =
  "scope" | "autocommit" | "statement" | "transaction" | "catalog" | "maintenance" | "import";

/**
 * Proof that the holder is the database's one logical writer. Internal paths that publish pass
 * it explicitly rather than inferring admission from a flag, so a trigger body, a cascade, or a
 * fold lent a writer's turn never re-enters the queue it is already inside.
 */
export interface WriterAdmission {
  readonly kind: WriterKind;
  /** When this turn began, in `Date.now()` milliseconds. */
  readonly since: number;
}

/**
 * How far coordination reaches for a store: `cross-context` when the store has a stable identity
 * and Web Locks exist, `context` when only the identity exists (Node, or a browser without Web
 * Locks) and every engine in this context over that name shares one queue, `instance` when the
 * store has no identity and only engines sharing the same store object take turns.
 */
export type WriteCoordinationScope = "cross-context" | "context" | "instance";

export interface WriteAdmissionStall {
  /** How long this writer has waited for its turn. */
  readonly waitedMs: number;
  /** Where the current holder is: a writer in this context, or another context's lock grant. */
  readonly holder: "this-context" | "other-context";
  /** What the holder is, when it is in this context. */
  readonly holderKind?: WriterKind;
}

export interface AdmitWriterOptions {
  readonly kind: WriterKind;
  /** Cancels the wait for a turn. Once admitted, the callback owns cancellation. */
  readonly signal: AbortSignal;
  /**
   * Cancels only the wait on another context's lock, never the local queue: a writer queued
   * behind this context's own work still gets its turn, while one that would have to wait for
   * another tab is refused. A disposing worker connection uses this so closing answers what it
   * can and never waits for a tab that stopped.
   */
  readonly crossContextSignal?: AbortSignal;
  /** Milliseconds without a change of holder before `onStalled` fires. Default 10 s. */
  readonly stallReportMs?: number;
  /** Hears one report per stall episode: a holder that has not changed for `stallReportMs`. */
  readonly onStalled?: (stall: WriteAdmissionStall) => void;
}

/** How long a writer waits without a change of holder before the wait is reported. */
export const WRITE_ADMISSION_STALL_REPORT_MS = 10_000;

/** Test seam: shortens the stall report interval for every engine in the process. */
export const writeAdmissionTestHooks: { stallReportMs: number | undefined } = {
  stallReportMs: undefined,
};

interface LocalQueue {
  tail: Promise<unknown>;
  holder: WriterAdmission | undefined;
  waiting: number;
}

const queues = new Map<BlockStore | string, LocalQueue>();
/** Stores whose engines publish without taking a turn: the rogue-writer seam for tests. */
const uncoordinated = new WeakSet<BlockStore>();

/**
 * Test seam: engines over this store publish without admission, standing in for an older build
 * or a writer that cannot take part. Storage compare-and-swap is all that protects them.
 */
export function markStoreUncoordinatedForTests(store: BlockStore): void {
  uncoordinated.add(store);
}

/** Test seam: whether `markStoreUncoordinatedForTests` was applied, so a wrapper can inherit it. */
export function isStoreUncoordinatedForTests(store: BlockStore): boolean {
  return uncoordinated.has(store);
}

function lockManager(): LockManager | undefined {
  return typeof navigator === "undefined" ? undefined : navigator.locks;
}

export function writeCoordinationScope(store: BlockStore): WriteCoordinationScope {
  if (store.liveQueryChannelName === undefined) return "instance";
  return lockManager() === undefined ? "context" : "cross-context";
}

/** Test/diagnostic seam: the local queue's state for a store. */
export function writeAdmissionState(store: BlockStore): {
  holder: WriterAdmission | undefined;
  waiting: number;
} {
  const queue = queues.get(store.liveQueryChannelName ?? store);
  return { holder: queue?.holder, waiting: queue?.waiting ?? 0 };
}

/** Read through a call: the flag flips inside listeners, which narrowing would miss. */
function isAborted(signal: AbortSignal): boolean {
  return signal.aborted;
}

function abortError(signal: AbortSignal): Error {
  const reason: unknown = signal.reason;
  return reason instanceof Error
    ? reason
    : new Error("Write admission was cancelled", { cause: reason });
}

/**
 * Runs `run` as the database's one logical writer. Resolves with the callback's result once its
 * turn ends; rejects without running it when the signal aborts while it waits.
 */
export async function admitWriter<T>(
  store: BlockStore,
  options: AdmitWriterOptions,
  run: (admission: WriterAdmission) => Promise<T>,
): Promise<T> {
  const { signal } = options;
  signal.throwIfAborted();
  if (uncoordinated.has(store)) return run({ kind: options.kind, since: Date.now() });
  const name = store.liveQueryChannelName;
  const key = name ?? store;
  let queue = queues.get(key);
  if (queue === undefined) {
    queue = { tail: Promise.resolve(), holder: undefined, waiting: 0 };
    queues.set(key, queue);
  }
  const owned = queue;
  const locks = name === undefined ? undefined : lockManager();
  const lockName = `minnowdb-write:${name ?? ""}`;
  const stallReportMs =
    options.stallReportMs ??
    writeAdmissionTestHooks.stallReportMs ??
    WRITE_ADMISSION_STALL_REPORT_MS;
  const startedAt = Date.now();
  const state = { admitted: false, finished: false };

  const hold = async (): Promise<T> => {
    state.admitted = true;
    const admission: WriterAdmission = { kind: options.kind, since: Date.now() };
    owned.holder = admission;
    try {
      return await run(admission);
    } finally {
      if (owned.holder === admission) owned.holder = undefined;
    }
  };

  // Stall reporting: one report per episode of an unchanging holder. A local holder is
  // identified by its admission object; a remote one by the lock manager's client id.
  let stallTimer: ReturnType<typeof setTimeout> | undefined;
  let reported = false;
  let mark: unknown = undefined;
  let markedAt = Date.now();
  const lockController = new AbortController();
  const stopStallWatch = (): void => {
    if (stallTimer !== undefined) clearTimeout(stallTimer);
    stallTimer = undefined;
  };
  // A function, not an expression: the flags flip inside other callbacks, which control-flow
  // narrowing across an await would otherwise read as never changing.
  const waiting = (): boolean => !state.admitted && !state.finished && !signal.aborted;
  const watchStalls = (): void => {
    if (options.onStalled === undefined) return;
    const probe = async (): Promise<void> => {
      if (!waiting()) return;
      let nextMark: unknown;
      let holder: WriteAdmissionStall["holder"] = "this-context";
      let holderKind: WriterKind | undefined;
      if (owned.holder !== undefined) {
        nextMark = owned.holder;
        holderKind = owned.holder.kind;
      } else if (locks !== undefined) {
        try {
          const snapshot = await locks.query();
          if (!waiting()) return;
          const held = snapshot.held?.find((lock) => lock.name === lockName);
          nextMark = held?.clientId ?? null;
          holder = "other-context";
        } catch {
          nextMark = null;
        }
      } else {
        nextMark = null;
      }
      const now = Date.now();
      if (nextMark !== mark) {
        mark = nextMark;
        markedAt = now;
        reported = false;
      } else if (!reported && now - markedAt >= stallReportMs) {
        reported = true;
        options.onStalled?.({
          waitedMs: now - startedAt,
          holder,
          ...(holderKind === undefined ? {} : { holderKind }),
        });
      }
      // The probe only reads; its promise cannot reject.
      // eslint-disable-next-line @typescript-eslint/no-misused-promises
      stallTimer = setTimeout(probe, Math.max(50, stallReportMs / 10));
    };
    void probe();
  };

  const crossContext = options.crossContextSignal;
  const enter = async (): Promise<T> => {
    owned.waiting -= 1;
    if (signal.aborted) throw abortError(signal);
    if (locks === undefined) return hold();
    // The cross-context stage. Only the head of the local queue reaches here, so this context
    // never holds more than one pending request on the lock.
    if (crossContext !== undefined && isAborted(crossContext)) {
      // Cross-context waits are cancelled: take the lock only if nobody else holds it, so the
      // writers this context can still serve itself are answered, and only a wait for another
      // context is refused.
      const granted = await locks.request(
        lockName,
        { ifAvailable: true },
        async (lock): Promise<readonly [T] | undefined> =>
          lock === null ? undefined : ([await hold()] as const),
      );
      if (granted === undefined) throw abortError(crossContext);
      return granted[0];
    }
    const onAbort = (): void => lockController.abort(signal.reason);
    const onCrossContextAbort = (): void => lockController.abort(crossContext?.reason);
    signal.addEventListener("abort", onAbort, { once: true });
    crossContext?.addEventListener("abort", onCrossContextAbort, { once: true });
    try {
      return await locks.request(lockName, { signal: lockController.signal }, hold);
    } catch (error) {
      // The lock manager rejects with the abort reason when the request is cancelled before a
      // grant; a rejection after the grant is the callback's own.
      if (!state.admitted && lockController.signal.aborted) {
        throw abortError(isAborted(signal) || crossContext === undefined ? signal : crossContext);
      }
      throw error;
    } finally {
      signal.removeEventListener("abort", onAbort);
      crossContext?.removeEventListener("abort", onCrossContextAbort);
    }
  };

  owned.waiting += 1;
  const preceding = owned.tail;
  // A rejected turn releases the queue to the next writer; the settled tail never rejects.
  const operation = preceding.then(enter, enter);
  const settled = operation.then(
    () => undefined,
    () => undefined,
  );
  owned.tail = settled;
  void settled.then(() => {
    state.finished = true;
    stopStallWatch();
    if (owned.tail === settled && owned.waiting === 0 && owned.holder === undefined) {
      if (queues.get(key) === owned) queues.delete(key);
    }
  });
  watchStalls();
  // Cancelling a queued writer must not wait for the holder in front of it: the race rejects
  // at once, the queued entry later finds the signal aborted and steps aside.
  let detach = (): void => undefined;
  const cancelled = new Promise<never>((_resolve, reject) => {
    const abort = (): void => {
      if (!state.admitted) reject(abortError(signal));
    };
    signal.addEventListener("abort", abort, { once: true });
    detach = () => signal.removeEventListener("abort", abort);
  });
  try {
    return await Promise.race([operation, cancelled]);
  } finally {
    detach();
  }
}
