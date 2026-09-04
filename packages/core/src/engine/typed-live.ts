import { sameLiveValue } from "./live-equal.js";
import type {
  LiveQueryDelivery,
  LiveQueryInput,
  LiveQueryInvalidation,
  LiveQueryObserveOptions,
  LiveQuerySubscribeOptions,
} from "./live.js";
import type { QueryResult } from "./query.js";

/** The structural live-query surface shared by MinnowDatabase and its worker client. */
export interface LiveQueryBackend {
  observe(
    query: LiveQueryInput,
    options: LiveQueryObserveOptions,
  ): Promise<LiveQuerySubscriptionLike>;
  /** Result delivery, used when the source can decode the engine's result itself. */
  subscribe?(
    query: LiveQueryInput,
    options: LiveQuerySubscribeOptions,
  ): Promise<LiveQuerySubscriptionLike>;
  refresh(): Promise<void>;
  close(): void | Promise<void>;
}

export interface LiveQuerySubscriptionLike {
  readonly dependencyTableIds: readonly string[];
  close(): void | Promise<void>;
}

export interface LiveQueryDriver {
  liveQueries(options?: { channelName?: string; pollIntervalMs?: number }): LiveQueryBackend;
}

/**
 * Adapter-neutral query boundary. `query` drives dependency tracking in Minnow; `execute`
 * deliberately stays with the adapter so its result transforms and inferred row type survive.
 */
export interface LiveQuerySource<out TRow> {
  readonly query: LiveQueryInput;
  execute(signal?: AbortSignal): Promise<readonly TRow[]>;
  /**
   * Turns a result the engine delivered into the adapter's rows. With it, the query subscribes
   * for results rather than invalidations: the engine executes or patches the statement where
   * the data is, compares, and hands over a changed result once — over a worker channel, as
   * one columnar transfer — and `execute` is never called after the statement is registered.
   * Without it, an invalidation is followed by `execute`, which the engine's memo serves.
   */
  decode?(result: QueryResult): readonly TRow[] | Promise<readonly TRow[]>;
}

export type LiveSnapshot<TRow> =
  | { readonly status: "loading"; readonly rows: readonly TRow[] }
  | {
      readonly status: "ready";
      readonly rows: readonly TRow[];
      readonly version: number | null;
    }
  | {
      readonly status: "error";
      readonly rows: readonly TRow[];
      readonly error: unknown;
      readonly version: number | null;
    };

/**
 * The next snapshot's rows, or `undefined` when nothing changed. A row that is structurally
 * equal to the row at the same position keeps the previous object, so a renderer that keys on
 * row identity — `React.memo` over a list item, a memoized selector — re-renders only the rows
 * that actually differ. Adapters own their values, so the array is a fresh frozen copy either
 * way: mutating the builder's result cannot change the snapshot retained for exact suppression.
 */
function reconcileRows<TRow>(
  previous: readonly TRow[],
  next: readonly TRow[],
  retained: Int32Array | undefined,
): readonly TRow[] | undefined {
  const rows = new Array<TRow>(next.length);
  let changed = previous.length !== next.length;
  // The engine's word on which rows it kept, when it kept any: a row it retained is equal to the
  // one at that previous index whatever position it now holds, so a new row at the top does not
  // cost every row below it its object.
  const provenance = retained?.length === next.length ? retained : undefined;
  for (let index = 0; index < next.length; index += 1) {
    const row = next[index] as TRow;
    const was = provenance?.[index] ?? -1;
    if (was >= 0 && was < previous.length) {
      rows[index] = previous[was] as TRow;
      if (was !== index) changed = true;
      continue;
    }
    const before = previous[index];
    if (index < previous.length && sameLiveValue(before, row)) rows[index] = before as TRow;
    else {
      rows[index] = row;
      changed = true;
    }
  }
  return changed ? Object.freeze(rows) : undefined;
}

/**
 * One typed live SELECT. It is also a framework-neutral external store: getSnapshot is stable
 * until state changes and subscribe returns synchronous cleanup. Slow consumers coalesce to the
 * newest invalidation; errors retain the last good rows.
 */
export class LiveQuery<out TRow> implements AsyncIterable<LiveSnapshot<TRow>> {
  /** Type-only row helper (`typeof query.$inferRow`). */
  declare readonly $inferRow: TRow;

  readonly #listeners = new Set<() => void>();
  readonly #source: LiveQuerySource<TRow>;
  readonly #backend: LiveQueryBackend;
  readonly #onClose: (() => void) | undefined;
  #snapshot: LiveSnapshot<TRow> = { status: "loading", rows: [] };
  #subscription: Promise<LiveQuerySubscriptionLike> | undefined;
  #observationGeneration = 0;
  #queued: QueuedWork | undefined;
  /** The engine's last delivered result, so a decode failure can be retried without a commit. */
  #lastDelivered: Extract<QueuedWork, { result: QueryResult }> | undefined;
  /** Deliveries received, and the one whose rows the snapshot currently holds (0 for none). */
  #deliveriesReceived = 0;
  #rowsFromDelivery = 0;
  #execution: Promise<void> | undefined;
  #executionAbort: AbortController | undefined;
  #invalidationSequence = 0;
  #closed = false;

  constructor(backend: LiveQueryBackend, source: LiveQuerySource<TRow>, onClose?: () => void) {
    this.#backend = backend;
    this.#source = source;
    this.#onClose = onClose;
  }

  getSnapshot = (): LiveSnapshot<TRow> => this.#snapshot;

  subscribe = (listener: () => void): (() => void) => {
    if (this.#closed) throw new Error("Live query is closed");
    // A subscription is a lease, even when a framework reuses the same callback function.
    const registered = (): void => listener();
    this.#listeners.add(registered);
    if (this.#listeners.size === 1) this.#startObservation();
    let subscribed = true;
    return () => {
      if (!subscribed) return;
      subscribed = false;
      this.#listeners.delete(registered);
      if (this.#listeners.size === 0) this.#stopObservation();
    };
  };

  async refresh(): Promise<void> {
    if (this.#closed) throw new Error("Live query is closed");
    const sequence = this.#invalidationSequence;
    if (this.#listeners.size > 0) {
      // A transient dependency/observer setup failure must not strand existing subscribers.
      // Re-open first so this refresh and future commits reach the query again.
      if (this.#subscription === undefined) this.#startObservation();
      const opening = this.#subscription;
      if (opening !== undefined) await opening.catch(() => undefined);
      await this.#backend.refresh();
    }
    // A backend refresh normally invalidates observers itself. Only synthesize an execution
    // when it did not, which preserves manual retry without running every refreshed query twice.
    if (this.#invalidationSequence === sequence) {
      const version = this.#snapshot.status === "loading" ? null : this.#snapshot.version;
      if (this.#decodes()) {
        // The engine retries a failed statement itself on refresh and delivers the outcome;
        // what is left to retry here is a decode that failed on a result it did deliver.
        const last = this.#lastDelivered;
        if (this.#snapshot.status === "error" && last !== undefined) this.#schedule(last);
      } else this.#schedule({ manifestVersion: version, catalogEpoch: 0, initial: false });
    }
    await this.#waitForIdle();
  }

  #decodes(): boolean {
    return this.#source.decode !== undefined && this.#backend.subscribe !== undefined;
  }

  async #decodeDelivered(result: QueryResult): Promise<readonly TRow[]> {
    const source = this.#source;
    if (source.decode === undefined) throw new TypeError("Live query source lost its decoder");
    return source.decode(result);
  }

  close(): void {
    if (this.#closed) return;
    this.#closed = true;
    this.#observationGeneration += 1;
    this.#executionAbort?.abort(new Error("Live query is closed"));
    const subscription = this.#subscription;
    this.#subscription = undefined;
    if (subscription !== undefined) {
      void subscription.then((handle) => handle.close()).catch(() => undefined);
    }
    // Wake iterators before removing their listeners so a parked next() terminates.
    this.#emit();
    this.#listeners.clear();
    this.#onClose?.();
  }

  #startObservation(): void {
    if (this.#subscription !== undefined || this.#closed) return;
    const generation = (this.#observationGeneration += 1);
    if (this.#source.decode !== undefined && this.#backend.subscribe !== undefined) {
      const subscription = this.#backend.subscribe(this.#source.query, {
        onChange: (result, delivery) => {
          if (generation !== this.#observationGeneration || this.#closed) return;
          this.#deliveriesReceived += 1;
          this.#schedule({ result, delivery, sequence: this.#deliveriesReceived });
        },
        onError: (error) => {
          if (generation !== this.#observationGeneration || this.#closed) return;
          this.#setError(error, this.#currentVersion());
        },
        onComplete: () => {
          if (generation !== this.#observationGeneration) return;
          this.#subscription = undefined;
        },
      });
      this.#subscription = subscription;
      subscription.catch((error: unknown) => {
        if (generation !== this.#observationGeneration || this.#closed) return;
        this.#subscription = undefined;
        this.#setError(error, this.#currentVersion());
      });
      return;
    }
    const subscription = this.#backend.observe(this.#source.query, {
      // The engine executes and compares before invalidating: a commit that leaves these rows
      // as they were never reaches this thread, and one that changes them is served from the
      // engine's memo when the adapter re-executes below.
      suppressUnchanged: true,
      onInvalidate: (invalidation) => {
        if (generation !== this.#observationGeneration || this.#closed) return;
        this.#schedule(invalidation);
      },
      onError: (error) => {
        if (generation !== this.#observationGeneration || this.#closed) return;
        this.#setError(error, this.#currentVersion());
      },
      onComplete: () => {
        if (generation !== this.#observationGeneration) return;
        this.#subscription = undefined;
      },
    });
    this.#subscription = subscription;
    subscription.catch((error: unknown) => {
      if (generation !== this.#observationGeneration || this.#closed) return;
      this.#subscription = undefined;
      this.#setError(error, this.#currentVersion());
    });
  }

  #stopObservation(): void {
    this.#observationGeneration += 1;
    this.#executionAbort?.abort(new Error("Live query has no subscribers"));
    const subscription = this.#subscription;
    this.#subscription = undefined;
    if (subscription !== undefined) {
      void subscription.then((handle) => handle.close()).catch(() => undefined);
    }
  }

  #schedule(work: QueuedWork): void {
    this.#invalidationSequence += 1;
    this.#queued = work;
    if (this.#execution !== undefined) return;
    const execution = this.#drain();
    this.#execution = execution;
    void execution.finally(() => {
      if (this.#execution === execution) this.#execution = undefined;
      if (this.#queued !== undefined && !this.#closed) this.#schedule(this.#queued);
    });
  }

  async #drain(): Promise<void> {
    while (this.#queued !== undefined && !this.#closed) {
      const work = this.#queued;
      this.#queued = undefined;
      const abort = new AbortController();
      this.#executionAbort = abort;
      const invalidation = "result" in work ? work.delivery : work;
      try {
        let executed: readonly TRow[];
        if ("result" in work) {
          // Held only across a failed decode, so refresh() can retry it; a decoded result is
          // not kept a second time beside the adapter's rows.
          this.#lastDelivered = work;
          executed = await this.#decodeDelivered(work.result);
          this.#lastDelivered = undefined;
        } else executed = await this.#source.execute(abort.signal);
        if (this.#executionWasCancelled(abort)) continue;
        // A newer invalidation arrived while this ran. Skip the intermediate snapshot and let
        // the loop execute once more against the newest durable state.
        if (this.#hasQueuedInvalidation()) continue;
        const previous = this.#snapshot.rows;
        // The engine's row map is relative to the delivery before this one. It only applies
        // when that is the delivery these rows came from — not after a decode that failed, and
        // not after a delivery this drain skipped for a newer one.
        const provenance =
          "result" in work && this.#rowsFromDelivery === work.sequence - 1
            ? work.delivery.retained
            : undefined;
        const rows = reconcileRows(previous, executed, provenance);
        if ("result" in work) this.#rowsFromDelivery = work.sequence;
        if (rows === undefined) {
          if (
            this.#snapshot.status === "ready" &&
            this.#snapshot.version === invalidation.manifestVersion
          ) {
            continue;
          }
          // Advance the version (or settle a cold/error snapshot) without replacing the
          // immutable row array, so consumers comparing by identity see no change in rows.
          this.#snapshot = {
            status: "ready",
            rows: this.#snapshot.status === "loading" ? Object.freeze([...previous]) : previous,
            version: invalidation.manifestVersion,
          };
        } else {
          this.#snapshot = {
            status: "ready",
            rows,
            version: invalidation.manifestVersion,
          };
        }
        this.#emit();
      } catch (error) {
        if (this.#executionWasCancelled(abort) || this.#hasQueuedInvalidation()) continue;
        this.#setError(error, invalidation.manifestVersion);
      } finally {
        if (this.#executionAbort === abort) this.#executionAbort = undefined;
      }
    }
  }

  #executionWasCancelled(abort: AbortController): boolean {
    return abort.signal.aborted || this.#closed;
  }

  #hasQueuedInvalidation(): boolean {
    return this.#queued !== undefined;
  }

  async #waitForIdle(): Promise<void> {
    for (;;) {
      const execution = this.#execution;
      if (execution === undefined) return;
      await execution;
      if (this.#execution === undefined && this.#queued === undefined) return;
    }
  }

  #currentVersion(): number | null {
    return this.#snapshot.status === "loading" ? null : this.#snapshot.version;
  }

  #setError(error: unknown, version: number | null): void {
    this.#snapshot = { status: "error", rows: this.#snapshot.rows, error, version };
    this.#emit();
  }

  #emit(): void {
    for (const listener of [...this.#listeners]) {
      try {
        listener();
      } catch {
        // One framework listener must not prevent the others from observing the same snapshot.
      }
    }
  }

  [Symbol.asyncIterator](): AsyncIterator<LiveSnapshot<TRow>, undefined> {
    const inbox = {
      latest: this.#snapshot,
      hasLatest: true,
      stopped: false,
      notify: undefined as (() => void) | undefined,
    };
    const wake = (): void => {
      const notify = inbox.notify;
      inbox.notify = undefined;
      notify?.();
    };
    const unsubscribe = this.subscribe(() => {
      inbox.latest = this.#snapshot;
      inbox.hasLatest = true;
      wake();
    });
    const done = { done: true as const, value: undefined };
    const read = async (): Promise<IteratorResult<LiveSnapshot<TRow>, undefined>> => {
      for (;;) {
        if (inbox.stopped || this.#closed) return done;
        if (inbox.hasLatest) {
          inbox.hasLatest = false;
          return { done: false, value: inbox.latest };
        }
        await new Promise<void>((resolve) => {
          inbox.notify = resolve;
        });
      }
    };
    let queue = Promise.resolve();
    return {
      next: () => {
        const result = queue.then(read, read);
        queue = result.then(
          () => undefined,
          () => undefined,
        );
        return result;
      },
      return: async () => {
        inbox.stopped = true;
        wake();
        unsubscribe();
        return done;
      },
      throw: async (error?: unknown) => {
        inbox.stopped = true;
        wake();
        unsubscribe();
        throw error;
      },
    };
  }
}

/** What one drain step works from: an invalidation to execute, or a delivered result to decode. */
type QueuedWork =
  | LiveQueryInvalidation
  | {
      readonly result: QueryResult;
      readonly delivery: LiveQueryDelivery;
      /** The delivery's ordinal on this query, counted from one. */
      readonly sequence: number;
    };

export interface LiveQueryManagerOptions {
  readonly channelName?: string;
  readonly pollIntervalMs?: number;
}

/** Owns one shared low-level set and every typed query created from it. */
export class LiveQueryManager {
  readonly #backend: LiveQueryBackend;
  readonly #queries = new Set<LiveQuery<unknown>>();
  #closed = false;

  constructor(driver: LiveQueryDriver, options: LiveQueryManagerOptions = {}) {
    this.#backend = driver.liveQueries(options);
  }

  watch<TRow>(source: LiveQuerySource<TRow>): LiveQuery<TRow> {
    if (this.#closed) throw new Error("Live query manager is closed");
    const query = new LiveQuery(this.#backend, source, () => {
      this.#queries.delete(query);
    });
    this.#queries.add(query);
    return query;
  }

  async refresh(): Promise<void> {
    if (this.#closed) throw new Error("Live query manager is closed");
    // The shared backend sweep remains selective. Queries that were already in an adapter-level
    // error also run their local retry path; concurrent backend refresh calls collapse into the
    // same low-level sweep, so this does not multiply durable probes.
    const failed = [...this.#queries].filter((query) => query.getSnapshot().status === "error");
    await Promise.all([this.#backend.refresh(), ...failed.map((query) => query.refresh())]);
  }

  async close(): Promise<void> {
    if (this.#closed) return;
    this.#closed = true;
    for (const query of [...this.#queries]) query.close();
    this.#queries.clear();
    await this.#backend.close();
  }
}

export function createLiveQueryManager(
  driver: LiveQueryDriver,
  options: LiveQueryManagerOptions = {},
): LiveQueryManager {
  return new LiveQueryManager(driver, options);
}
