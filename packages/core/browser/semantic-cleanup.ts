/** OpfsBlockStore.close() starts asynchronous checkpoint/handle shutdown. Keep its worker
 * alive until coordinated deletion has waited for the connection lock and removed the files.
 * A dispose reply alone is not that handle-drain fence. Always attempt every cleanup stage. */
export async function closeSemanticResources(
  worker: Pick<Worker, "terminate">,
  closeClient: () => Promise<void>,
  closeDatabase: () => Promise<void>,
  removeDatabase: () => Promise<void>,
): Promise<unknown[]> {
  const failures: unknown[] = [];
  const closes = await Promise.allSettled(
    [closeClient, closeDatabase].map(async (close) => close()),
  );
  for (const result of closes) if (result.status === "rejected") failures.push(result.reason);
  try {
    await removeDatabase();
  } catch (error) {
    failures.push(error);
  } finally {
    try {
      worker.terminate();
    } catch (error) {
      failures.push(error);
    }
  }
  return failures;
}

/** Firefox's page.evaluate transport omits AggregateError.errors. Keep every root cause in
 * the message too, while retaining the original objects for browsers that expose them. */
export function semanticRunFailure(failures: readonly unknown[]): AggregateError {
  return new AggregateError(
    failures,
    `Semantic comparison or cleanup failed: ${failures.map(String).join("; ")}`,
  );
}
