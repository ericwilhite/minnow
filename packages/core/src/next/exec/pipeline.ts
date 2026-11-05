/**
 * The push protocol between operators, and the async driver that runs a pipeline.
 *
 * Kernels are synchronous. A pipeline is driven from its source: the driver awaits the source for
 * a batch (page reads, spill reads, helper results), pushes it through synchronous operators to
 * a sink, and awaits only when an operator says it is blocked. That is where spilling, waiting
 * for memory, consumer backpressure and cancellation happen.
 *
 * ## `push(batch, transfer?)` returns `ready` or `blocked`
 * - `ready`: the operator is done with the batch (it copied what it keeps). The producer may
 *   recycle the batch's buffers.
 * - `blocked`: the operator accepted the batch but has not finished with it; it may be mid-way
 *   through it (an aggregate that must spill before inserting the rest, a join whose output
 *   consumer is full). The batch stays **borrowed** until `ready()` resolves: the producer must not
 *   recycle it before then, and the driver pushes nothing else into this operator. The operator
 *   resumes the remaining rows itself inside `ready()`.
 * - So each edge holds at most one in-flight batch: memory is bounded by the pipeline's admitted
 *   minimum (memory/governor.ts), whatever the data.
 *
 * ## Ownership handoff
 * - Default (no `transfer`): borrow, as above. Keeping anything past readiness means copying.
 * - `transfer`: the producer hands the buffers over (retain without copy). The operator owns the
 *   batch until it calls `transfer.release()`, which returns the buffers to the producer's pool.
 *   The pool has a fixed number of batches (its credits); a producer with no free batch blocks
 *   in `next()` until one is released. Retained batches are therefore bounded and charged to the
 *   producer's grant, never invisible.
 *
 * ## End and cancellation
 * - `finish()` is async: a breaker (sort, aggregate, join build) produces its output there, pushing
 *   to its downstream and awaiting its readiness, then finishes the downstream.
 * - `abort(reason)` releases everything the operator holds (grants, spill files, transferred
 *   batches) and is idempotent; a pending `ready()` rejects with `reason`. The driver aborts the
 *   whole pipeline on error or cancellation.
 *
 * ## Results
 * The result sink is an operator whose readiness is the consumer's credit: it holds at most a
 * fixed window of result bytes in flight to the client and returns `blocked` beyond it, so a
 * slow consumer pauses the pipeline instead of growing a buffer.
 */
import type { Batch } from "../vector/vector.js";

export type PushResult = "ready" | "blocked";

/** Retain-without-copy: the receiver owns the batch until it calls `release`. */
export interface Transfer {
  release(): void;
}

export interface Operator {
  push(batch: Batch, transfer?: Transfer): PushResult;
  /** Resolves when the operator can take the next batch; rejects if aborted. */
  ready(): Promise<void>;
  finish(): Promise<void>;
  abort(reason: unknown): void;
}

export interface BatchSource {
  /**
   * The next batch, or null at the end. A batch returned without a transfer is borrowed until the
   * following `next()` call; with one, until its `release`.
   */
  next(): Promise<{ readonly batch: Batch; readonly transfer?: Transfer } | null>;
  abort(reason: unknown): void;
}

/** Pushes `batch` into `operator` and waits for readiness: the driver's step, also used inside operators. */
export async function pushAndWait(
  operator: Operator,
  batch: Batch,
  transfer?: Transfer,
): Promise<void> {
  if (operator.push(batch, transfer) === "blocked") await operator.ready();
}

function abortReason(signal: AbortSignal): Error {
  const reason: unknown = signal.reason;
  return reason instanceof Error ? reason : new Error("Aborted", { cause: reason });
}

/**
 * `promise`, rejected early if `signal` aborts. Without a signal it is `promise` itself; with one,
 * the listener lives only as long as this wait, so a long stream retains nothing per batch.
 */
export function abortable<T>(promise: Promise<T>, signal?: AbortSignal): Promise<T> {
  if (signal === undefined) return promise;
  if (signal.aborted) return Promise.reject(abortReason(signal));
  return new Promise<T>((resolve, reject) => {
    const onAbort = () => {
      reject(abortReason(signal));
    };
    signal.addEventListener("abort", onAbort, { once: true });
    promise.then(
      (value) => {
        signal.removeEventListener("abort", onAbort);
        resolve(value);
      },
      (error: unknown) => {
        signal.removeEventListener("abort", onAbort);
        reject(error instanceof Error ? error : new Error(String(error)));
      },
    );
  });
}

/** Runs one pipeline to completion, aborting every stage on error or cancellation. */
export async function drive(
  source: BatchSource,
  sink: Operator,
  signal?: AbortSignal,
): Promise<void> {
  try {
    for (;;) {
      signal?.throwIfAborted();
      const next = await abortable(source.next(), signal);
      if (next === null) break;
      if (sink.push(next.batch, next.transfer) === "blocked") await abortable(sink.ready(), signal);
    }
    signal?.throwIfAborted();
    await abortable(sink.finish(), signal);
  } catch (error) {
    source.abort(error);
    sink.abort(error);
    throw error;
  }
}
