/** Abort requests, not promises: only a terminal no-effect cancellation becomes signal.reason. */
import type { ProtocolClient, ClientResult } from "./client.js";
import { decodeResult } from "./result.js";
import type { Result, Row } from "./api.js";
export type RequestBody = Parameters<ProtocolClient["send"]>[0];
export function cancelledReason(error: unknown, signal?: AbortSignal): unknown {
  return signal?.aborted &&
    error instanceof Error &&
    "code" in error &&
    error.code === "MINNOW_CANCELLED"
    ? signal.reason
    : error;
}
export async function callTask<T>(
  client: ProtocolClient,
  task: { readonly seq: number; readonly promise: Promise<T> },
  signal?: AbortSignal,
): Promise<T> {
  const abort = () => client.cancel(task.seq);
  signal?.addEventListener("abort", abort, { once: true });
  if (signal?.aborted) abort();
  try {
    return await task.promise;
  } catch (error) {
    throw cancelledReason(error, signal);
  } finally {
    signal?.removeEventListener("abort", abort);
  }
}
export function callResult<TRow = Row>(
  client: ProtocolClient,
  body: RequestBody,
  signal?: AbortSignal,
): Promise<Result<TRow>> {
  // Clone BEFORE awaiting readiness or a terminal reply. Retries keep precisely this input;
  // a user's later mutation cannot change the meaning of an unanswered write (R9).
  signal?.throwIfAborted();
  const task = client.send(structuredClone(body));
  return callTask<ClientResult>(client, task, signal).then(decodeResult<TRow>);
}
