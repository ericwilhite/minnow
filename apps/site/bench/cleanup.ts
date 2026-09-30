/** Preserve diagnostics even for opaque values thrown by a vendor driver. */
export function benchmarkErrorMessage(error: unknown): string {
  try {
    return error instanceof Error ? error.message : String(error);
  } catch {
    return "[unprintable error]";
  }
}

/** Attempts cleanup and retains both original failures, including their readable messages. */
export async function withBenchmarkCleanup<T>(
  operation: () => Promise<T>,
  cleanup: () => Promise<void>,
): Promise<T> {
  let result: { value: T } | undefined;
  let failure: { error: unknown } | undefined;
  try {
    result = { value: await operation() };
  } catch (error) {
    failure = { error };
  }
  try {
    await cleanup();
  } catch (error) {
    if (failure !== undefined)
      throw new AggregateError(
        [failure.error, error],
        `Benchmark execution and cleanup failed: ${benchmarkErrorMessage(failure.error)}; ${benchmarkErrorMessage(error)}`,
        { cause: error },
      );
    throw error;
  }
  if (failure !== undefined) throw failure.error;
  if (result === undefined) throw new Error("Benchmark operation did not settle");
  return result.value;
}
