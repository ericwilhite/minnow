/** A borrowed producer may stall forever; cancellation must not inherit its cleanup lifetime. */
export async function* snapshotInput<T>(
  source: AsyncIterable<T>,
  signal?: AbortSignal,
): AsyncGenerator<T> {
  const iterator = source[Symbol.asyncIterator]();
  let done = false;
  try {
    for (;;) {
      signal?.throwIfAborted();
      let remove: () => void = () => undefined;
      const cancelled = new Promise<never>((_, reject) => {
        const abort = () => {
          const reason: unknown = signal?.reason;
          reject(
            reason instanceof Error ? reason : new DOMException("Snapshot cancelled", "AbortError"),
          );
        };
        signal?.addEventListener("abort", abort, { once: true });
        remove = () => signal?.removeEventListener("abort", abort);
      });
      let step: IteratorResult<T>;
      try {
        step = await Promise.race([iterator.next(), cancelled]);
      } finally {
        remove();
      }
      signal?.throwIfAborted();
      if (step.done === true) {
        done = true;
        return;
      }
      yield step.value;
    }
  } finally {
    if (!done) {
      try {
        void Promise.resolve(iterator.return?.()).catch(() => undefined);
      } catch {
        // Producer cleanup must not replace the original failure or retain our pin/grant.
      }
    }
  }
}
