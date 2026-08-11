import { type QueryResult } from "../query.js";
import { type TypedQueryEnvelope } from "./types.js";

/**
 * The `.live()` terminal of a select builder: one query, typed rows, push delivery. Subscribing
 * delivers the current result immediately and again after any commit that may have changed it;
 * unchanged results are digest-suppressed by the underlying live set. Async iteration coalesces
 * to the latest result — every yielded value is a true snapshot, but intermediate snapshots may
 * be skipped when the consumer is slower than the writer. Iteration ends cleanly when the
 * iterator is cancelled (break, return(), Promise.race timeouts) or when the subscription's
 * live set closes.
 */

export interface LiveSubscriptionHandle {
  close(): void | Promise<void>;
}

/** The slice of a live-query set the DSL needs; Database and the worker client both provide it. */
export interface LiveQueryServices {
  subscribe(
    query: TypedQueryEnvelope<unknown>,
    handlers: {
      onChange(result: QueryResult): void;
      onError?(error: unknown): void;
      onComplete?(): void;
    },
  ): Promise<LiveSubscriptionHandle>;
}

export interface LiveQueryHandlers<TRow> {
  onChange(rows: TRow[]): void;
  onError?(error: unknown): void;
  /** Called once when the subscription ends because it (or its live set) closed. */
  onComplete?(): void;
}

export class LiveQuery<TRow> implements AsyncIterable<TRow[]> {
  constructor(
    private readonly services: LiveQueryServices,
    private readonly query: TypedQueryEnvelope<TRow>,
  ) {}

  /** Delivers the initial result, then every changed result, until the handle is closed. */
  async subscribe(handlers: LiveQueryHandlers<TRow>): Promise<LiveSubscriptionHandle> {
    return this.services.subscribe(this.query, {
      onChange: (result) => {
        handlers.onChange(result.rows as TRow[]);
      },
      ...(handlers.onError === undefined ? {} : { onError: handlers.onError.bind(handlers) }),
      ...(handlers.onComplete === undefined
        ? {}
        : { onComplete: handlers.onComplete.bind(handlers) }),
    });
  }

  /**
   * A hand-rolled iterator rather than an async generator: a generator parked on an internal
   * await would defer a queued `return()` until the next database change, deadlocking
   * cancellation. Here `return()` settles the pending wait itself and closes the subscription
   * immediately.
   */
  [Symbol.asyncIterator](): AsyncIterator<TRow[], undefined> {
    const inbox = {
      latest: [] as TRow[],
      hasLatest: false,
      failure: undefined as { error: unknown } | undefined,
      completed: false,
      stopped: false,
      notify: undefined as (() => void) | undefined,
    };
    const wake = (): void => {
      const resolve = inbox.notify;
      inbox.notify = undefined;
      resolve?.();
    };
    let subscription: Promise<LiveSubscriptionHandle> | undefined;
    let closed = false;
    const closeOnce = async (): Promise<void> => {
      if (closed) return;
      closed = true;
      if (subscription === undefined) return;
      // A completed subscription was already torn down by its live set; closing again is a
      // best-effort no-op (the worker-side handle may be gone).
      try {
        const handle = await subscription;
        if (!inbox.completed) await handle.close();
      } catch {
        // The subscription itself failed to establish; there is nothing to close.
      }
    };
    const done = { done: true as const, value: undefined };
    return {
      next: async (): Promise<IteratorResult<TRow[], undefined>> => {
        subscription ??= this.subscribe({
          onChange: (rows) => {
            inbox.latest = rows;
            inbox.hasLatest = true;
            wake();
          },
          onError: (error) => {
            inbox.failure = { error };
            wake();
          },
          onComplete: () => {
            inbox.completed = true;
            wake();
          },
        });
        await subscription;
        for (;;) {
          if (inbox.stopped) return done;
          if (inbox.failure !== undefined) {
            const { error } = inbox.failure;
            inbox.stopped = true;
            await closeOnce();
            throw error;
          }
          if (inbox.hasLatest) {
            inbox.hasLatest = false;
            return { done: false, value: inbox.latest };
          }
          if (inbox.completed) {
            inbox.stopped = true;
            await closeOnce();
            return done;
          }
          await new Promise<void>((resolve) => {
            inbox.notify = resolve;
          });
        }
      },
      return: async (): Promise<IteratorResult<TRow[], undefined>> => {
        inbox.stopped = true;
        wake();
        await closeOnce();
        return done;
      },
      throw: async (error?: unknown): Promise<IteratorResult<TRow[], undefined>> => {
        inbox.stopped = true;
        wake();
        await closeOnce();
        throw error;
      },
    };
  }
}
