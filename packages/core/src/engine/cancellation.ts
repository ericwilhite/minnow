/** One compact check shared by every bounded query-execution path. */
export function throwIfAborted(signal: AbortSignal | undefined): void {
  signal?.throwIfAborted();
}

/** Links caller and owner cancellation, detaching listeners after the wait settles. */
export async function withLinkedSignal<T>(
  external: AbortSignal | undefined,
  run: (signal: AbortSignal) => Promise<T>,
  queue: AbortSignal,
): Promise<T> {
  if (external === undefined) return run(queue);
  const controller = new AbortController();
  const forward = (source: AbortSignal) => (): void => controller.abort(source.reason);
  const fromExternal = forward(external);
  const fromQueue = forward(queue);
  if (external.aborted) fromExternal();
  else if (queue.aborted) fromQueue();
  external.addEventListener("abort", fromExternal, { once: true });
  queue.addEventListener("abort", fromQueue, { once: true });
  try {
    return await run(controller.signal);
  } finally {
    external.removeEventListener("abort", fromExternal);
    queue.removeEventListener("abort", fromQueue);
  }
}
