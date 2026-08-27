/** One compact check shared by every bounded query-execution path. */
export function throwIfAborted(signal: AbortSignal | undefined): void {
  signal?.throwIfAborted();
}
