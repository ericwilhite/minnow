import { dateMilliseconds } from "../date-value.js";
import type { LiveQueryInput, LiveQueryInvalidation, LiveQueryObserveOptions } from "./live.js";

/** The structural live-query surface shared by MinnowDatabase and its worker client. */
export interface LiveQueryBackend {
  observe(
    query: LiveQueryInput,
    options: LiveQueryObserveOptions,
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

function sameLiveValue(left: unknown, right: unknown): boolean {
  if (Object.is(left, right)) return true;
  if (left instanceof Date || right instanceof Date) {
    return (
      left instanceof Date &&
      right instanceof Date &&
      Object.is(dateMilliseconds(left), dateMilliseconds(right))
    );
  }
  if (Array.isArray(left) || Array.isArray(right)) {
    if (!Array.isArray(left) || !Array.isArray(right) || left.length !== right.length) return false;
    for (let index = 0; index < left.length; index += 1) {
      if (!sameLiveValue(left[index], right[index])) return false;
    }
    return true;
  }
  if (typeof left !== "object" || left === null || typeof right !== "object" || right === null) {
    return false;
  }
  const leftRecord = left as Record<string, unknown>;
  const rightRecord = right as Record<string, unknown>;
  const leftKeys = Object.keys(leftRecord);
  const rightKeys = Object.keys(rightRecord);
  if (leftKeys.length !== rightKeys.length) return false;
  for (let index = 0; index < leftKeys.length; index += 1) {
    const key = leftKeys[index];
    if (key === undefined || key !== rightKeys[index]) return false;
    if (!sameLiveValue(leftRecord[key], rightRecord[key])) return false;
  }
  return true;
}

function sameRows(left: readonly unknown[], right: readonly unknown[]): boolean {
  return sameLiveValue(left, right);
}

function immutableRows<TRow>(rows: readonly TRow[]): readonly TRow[] {
  // Adapters own their values. Copy the array so mutating the returned builder result cannot
  // change the snapshot identity retained for exact suppression.
  return Object.freeze([...rows]);
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
  #queued: LiveQueryInvalidation | undefined;
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
      this.#schedule({ manifestVersion: version, catalogEpoch: 0, initial: false });
    }
    await this.#waitForIdle();
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
    const subscription = this.#backend.observe(this.#source.query, {
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

  #schedule(invalidation: LiveQueryInvalidation): void {
    this.#invalidationSequence += 1;
    this.#queued = invalidation;
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
      const invalidation = this.#queued;
      this.#queued = undefined;
      const abort = new AbortController();
      this.#executionAbort = abort;
      try {
        const rows = immutableRows(await this.#source.execute(abort.signal));
        if (this.#executionWasCancelled(abort)) continue;
        // A newer invalidation arrived while this ran. Skip the intermediate snapshot and let
        // the loop execute once more against the newest durable state.
        if (this.#hasQueuedInvalidation()) continue;
        const previous = this.#snapshot.rows;
        if (
          this.#snapshot.status === "ready" &&
          this.#snapshot.version === invalidation.manifestVersion &&
          sameRows(previous, rows)
        ) {
          continue;
        }
        if (sameRows(previous, rows) && this.#snapshot.status !== "loading") {
          // Advance the version without replacing the immutable row array.
          this.#snapshot = {
            status: "ready",
            rows: previous,
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
