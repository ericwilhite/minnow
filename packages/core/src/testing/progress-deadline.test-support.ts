/** Test-only watchdog: repeated observations are not progress, and progress cannot extend forever. */
export async function withProgressDeadline<T>(
  label: string,
  operation: (advance: (marker: string) => void) => Promise<T>,
  options: { idleMs?: number; totalMs?: number; now?: () => number } = {},
): Promise<T> {
  const idleMs = options.idleMs ?? 30_000;
  const totalMs = options.totalMs ?? 120_000;
  const now = options.now ?? (() => performance.now());
  const start = now();
  let last = start;
  let marker: string | undefined;
  let finished = false;
  let failure: Error | undefined;
  const check = () => {
    const elapsed = now() - start;
    const idle = now() - last;
    if (elapsed >= totalMs || idle >= idleMs)
      throw new Error(
        `${label}: ${elapsed >= totalMs ? "total deadline" : "no progress"}; elapsed ${String(Math.round(elapsed))} ms, idle ${String(Math.round(idle))} ms; last progress ${marker ?? "none"}`,
      );
  };
  let timer: ReturnType<typeof setInterval> | undefined;
  const expired = new Promise<never>((_, reject) => {
    timer = setInterval(
      () => {
        try {
          check();
        } catch (error) {
          failure = error as Error;
          reject(failure);
        }
      },
      Math.min(250, idleMs, totalMs),
    );
  });
  const advance = (next: string) => {
    if (failure !== undefined) throw failure;
    if (finished) return; // A native update already in flight may complete after this phase.
    check();
    if (next !== marker) {
      marker = next;
      last = now();
    }
  };
  try {
    const result = await Promise.race([operation(advance), expired]);
    // A CPU-heavy phase may finish before a starved timer gets its turn.
    check();
    return result;
  } finally {
    finished = true;
    clearInterval(timer);
  }
}
