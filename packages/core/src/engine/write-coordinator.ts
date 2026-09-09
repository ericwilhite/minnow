import type { BlockStore } from "../storage/types.js";

const anonymous = new WeakMap<BlockStore, { tail: Promise<unknown> }>();
const named = new Map<string, { tail: Promise<unknown> }>();

/** Admit autocommit before it captures a snapshot. Explicit callbacks never hold this lock. */
export async function coordinateWrite<T>(
  store: BlockStore,
  run: () => Promise<T>,
  signal: AbortSignal,
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
  const operation = queue.tail.then(async () => {
    signal.throwIfAborted();
    return name !== undefined && locks !== undefined
      ? await locks.request(`minnowdb-write:${name}`, { signal: lockController.signal }, enter)
      : await enter();
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
