import { type Manifest, type StoragePage } from "../storage/index.js";
import { type CompiledQuery, type QueryResult, type QueryValue } from "./query.js";

/**
 * Correctness-first live queries with selective re-execution. Every hint path — a local commit,
 * a cross-tab channel message, a poll tick, or an explicit refresh — converges on the same
 * authoritative check: read the durable manifest version, and when it moved, derive the changed
 * tables from the persisted per-commit change sets in bounded pages. Missed hints therefore delay
 * a refresh but can never produce a stale result, and a manifest without a change set (written
 * before tracking, or outside a transaction) conservatively affects every subscription.
 *
 * Memory discipline: a subscription retains its plan text, dependency table IDs, and a numeric
 * result digest — never result rows. Results flow to the subscriber and are dropped; unchanged
 * digests suppress the notification entirely.
 */

export interface LiveQueryHintChannel {
  postMessage(message: unknown): void;
  addEventListener(type: "message", listener: () => void): void;
  removeEventListener(type: "message", listener: () => void): void;
  close?(): void;
}

export interface LiveQuerySetOptions {
  readonly channel?: LiveQueryHintChannel;
  /**
   * Convenience form of `channel`: the set creates (and owns, closing it with the set) a
   * BroadcastChannel of this name when the platform provides one. Ignored when `channel` is
   * given or BroadcastChannel is unavailable.
   */
  readonly channelName?: string;
  readonly pollIntervalMs?: number;
  /** Called once when the set closes; the owner uses this to drop its reference. */
  readonly onClosed?: () => void;
}

export interface LiveQuerySubscribeOptions {
  onChange(result: QueryResult): void;
  onError?(error: unknown): void;
  /** Called once when the subscription ends because the subscription or its set closed. */
  onComplete?(): void;
}

export interface LiveQuerySubscription {
  readonly dependencyTableIds: readonly string[];
  close(): void;
}

export interface LiveQueryStats {
  hints: number;
  versionChecks: number;
  sweeps: number;
  reruns: number;
  rerunsAvoided: number;
  /** Re-runs skipped because the data layer proved the commits could not change the result. */
  zoneSkips: number;
  notificationsSuppressed: number;
  lastSweepMs: number;
}

/** A live query: plain SQL, parameterized SQL, or a compiled-plan envelope. */
export type LiveQueryInput =
  | string
  | { kind: "sql-query"; sql: string; params: readonly QueryValue[] }
  | { kind: "typed-query"; plan: CompiledQuery };

interface LiveQueryHost {
  currentVersion(): Promise<number | null>;
  manifestPage(afterVersion: number | null, limit: number): Promise<StoragePage<Manifest, number>>;
  dependencyTableIds(query: LiveQueryInput): Promise<Set<string>>;
  execute(query: LiveQueryInput): Promise<QueryResult>;
  /**
   * Optional data-layer selectivity: whether commits in (after, until] to the given
   * dependency tables can possibly change this query's result. A host answers false only
   * on proof — e.g. every introduced segment is a data-neutral compaction rewrite, or every
   * new insert block's zone statistics reject the query's predicates — so a false skips the
   * re-run entirely. Errors and unknown shapes must answer true.
   */
  changeCanAffect?(
    query: LiveQueryInput,
    tableIds: readonly string[],
    after: number | null,
    until: number,
  ): Promise<boolean>;
}

interface Subscription {
  readonly query: LiveQueryInput;
  readonly dependencies: ReadonlySet<string>;
  readonly options: LiveQuerySubscribeOptions;
  digest: number | undefined;
  /**
   * The manifest version this subscription's data is known fresh at: the version read just
   * before its initial execute, advanced by every sweep that re-runs or safely skips it. A
   * subscription whose seenVersion trails the current version lags — its initial execute raced
   * a concurrent sweep — and the next sweep re-runs it even when the set-level version already
   * matches.
   */
  seenVersion: number | null;
  closed: boolean;
}

/** Companion to LiveQuerySet#stillOpen for the per-subscription flag. */
function isSubscriptionClosed(subscription: Subscription): boolean {
  return subscription.closed;
}

const digestScratch = new DataView(new ArrayBuffer(8));

/**
 * A 32-bit FNV-1a digest of a result, mixed from typed values: a number's eight bytes, a
 * string's char codes, a tag per type and per null. Building a string per cell and hashing
 * that used to cost several times the query it digested.
 */
function digestResult(result: QueryResult): number {
  let hash = 0x811c9dc5;
  const mixByte = (byte: number): void => {
    hash = Math.imul(hash ^ byte, 0x01000193) >>> 0;
  };
  const mixNumber = (value: number): void => {
    digestScratch.setFloat64(0, value);
    for (let index = 0; index < 8; index += 1) mixByte(digestScratch.getUint8(index));
  };
  const mixString = (text: string): void => {
    for (let index = 0; index < text.length; index += 1) {
      const code = text.charCodeAt(index);
      mixByte(code & 0xff);
      mixByte(code >>> 8);
    }
    mixByte(0xff);
  };
  for (const column of result.columns) mixString(column);
  for (const row of result.rows) {
    for (const column of result.columns) {
      const value: QueryValue = row[column] ?? null;
      if (value === null) mixByte(1);
      else if (typeof value === "number") {
        mixByte(2);
        mixNumber(value);
      } else if (typeof value === "string") {
        mixByte(3);
        mixString(value);
      } else if (typeof value === "boolean") mixByte(value ? 4 : 5);
      else {
        mixByte(6);
        mixNumber(value.getTime());
      }
    }
    mixByte(0xfe);
  }
  return hash;
}

export class LiveQuerySet {
  readonly #host: LiveQueryHost;
  readonly #channel: LiveQueryHintChannel | undefined;
  readonly #channelListener = (): void => {
    this.#hint();
  };
  readonly #subscriptions = new Set<Subscription>();
  readonly #stats: LiveQueryStats = {
    hints: 0,
    versionChecks: 0,
    sweeps: 0,
    reruns: 0,
    rerunsAvoided: 0,
    zoneSkips: 0,
    notificationsSuppressed: 0,
    lastSweepMs: 0,
  };
  #lastSeenVersion: number | null = null;
  #sweepChain = Promise.resolve();
  #sweepQueued = false;
  #pollTimer: ReturnType<typeof setInterval> | undefined;
  #closed = false;
  readonly #ownsChannel: boolean;
  readonly #onClosed: (() => void) | undefined;

  constructor(host: LiveQueryHost, options: LiveQuerySetOptions = {}) {
    this.#host = host;
    if (options.channel !== undefined) {
      this.#channel = options.channel;
      this.#ownsChannel = false;
    } else if (options.channelName !== undefined && typeof BroadcastChannel !== "undefined") {
      this.#channel = new BroadcastChannel(options.channelName);
      this.#ownsChannel = true;
    } else {
      this.#channel = undefined;
      this.#ownsChannel = false;
    }
    this.#onClosed = options.onClosed;
    this.#channel?.addEventListener("message", this.#channelListener);
    if (options.pollIntervalMs !== undefined) {
      this.#pollTimer = setInterval(() => {
        this.#hint();
      }, options.pollIntervalMs);
    }
  }

  get stats(): LiveQueryStats {
    return { ...this.#stats };
  }

  /** Registers a query, delivers its initial result, and re-runs it when its tables change. */
  async subscribe(
    query: LiveQueryInput,
    options: LiveQuerySubscribeOptions,
  ): Promise<LiveQuerySubscription> {
    if (this.#closed) throw new Error("Live query set is closed");
    const dependencies = await this.#host.dependencyTableIds(query);
    const version = await this.#host.currentVersion();
    const subscription: Subscription = {
      query,
      dependencies,
      options,
      digest: undefined,
      seenVersion: version,
      closed: false,
    };
    if (this.#lastSeenVersion === null || (version !== null && version < this.#lastSeenVersion)) {
      this.#lastSeenVersion = version;
    }
    const result = await this.#host.execute(query);
    subscription.digest = digestResult(result);
    this.#subscriptions.add(subscription);
    try {
      options.onChange(result);
    } catch (error) {
      // A throwing initial handler would leak an unclosable subscription; unregister first.
      subscription.closed = true;
      this.#subscriptions.delete(subscription);
      throw error;
    }
    return {
      dependencyTableIds: [...dependencies],
      close: () => {
        if (subscription.closed) return;
        subscription.closed = true;
        this.#subscriptions.delete(subscription);
        subscription.options.onComplete?.();
      },
    };
  }

  /** Called by the owning database after each local write commit; also hints other tabs. */
  notifyLocalCommit(): void {
    this.#channel?.postMessage("minnow-commit");
    this.#hint();
  }

  /** Runs one authoritative version check and selective re-execution sweep. */
  async refresh(): Promise<void> {
    this.#hint();
    await this.#sweepChain;
  }

  close(): void {
    if (this.#closed) return;
    this.#closed = true;
    this.#channel?.removeEventListener("message", this.#channelListener);
    if (this.#ownsChannel) this.#channel?.close?.();
    if (this.#pollTimer !== undefined) clearInterval(this.#pollTimer);
    const subscriptions = [...this.#subscriptions];
    this.#subscriptions.clear();
    // Wake every remaining subscriber so iterators and UI bindings end instead of hanging.
    for (const subscription of subscriptions) {
      if (subscription.closed) continue;
      subscription.closed = true;
      subscription.options.onComplete?.();
    }
    this.#onClosed?.();
  }

  #hint(): void {
    if (this.#closed) return;
    this.#stats.hints += 1;
    if (this.#sweepQueued) return;
    this.#sweepQueued = true;
    this.#sweepChain = this.#sweepChain.then(async () => {
      this.#sweepQueued = false;
      try {
        await this.#sweep();
      } catch (error) {
        for (const subscription of this.#subscriptions) {
          subscription.options.onError?.(error);
        }
      }
    });
  }

  /**
   * Re-reads the closed flags through calls. `close()` runs while a sweep is suspended at an
   * await, but control-flow analysis assumes flags cannot change across one, so a direct read
   * would be narrowed to its pre-await value and the guard silently dropped.
   */
  #stillOpen(): boolean {
    return !this.#closed;
  }

  async #sweep(): Promise<void> {
    if (this.#closed || this.#subscriptions.size === 0) return;
    this.#stats.versionChecks += 1;
    const current = await this.#host.currentVersion();
    if (!this.#stillOpen() || current === null) return;
    const anyLagging = [...this.#subscriptions].some(
      (subscription) =>
        !subscription.closed &&
        (subscription.seenVersion === null || subscription.seenVersion < current),
    );
    if (current === this.#lastSeenVersion && !anyLagging) return;
    const started = performance.now();
    this.#stats.sweeps += 1;
    // The changed-table window starts at the set-level version, so it only gates subscriptions
    // that were up to date there. A lagging subscription's window starts earlier at its own
    // seenVersion, so it re-runs unconditionally — the digest suppresses no-op notifications.
    const changed =
      current === this.#lastSeenVersion
        ? new Set<string>()
        : await this.#changedTablesSince(this.#lastSeenVersion, current);
    if (!this.#stillOpen()) return;
    for (const subscription of [...this.#subscriptions]) {
      if (subscription.closed) continue;
      if (subscription.seenVersion !== null && subscription.seenVersion >= current) continue;
      if (subscription.seenVersion === this.#lastSeenVersion) {
        const affected =
          changed === "all" ||
          [...subscription.dependencies].some((tableId) => changed.has(tableId));
        if (!affected) {
          this.#stats.rerunsAvoided += 1;
          subscription.seenVersion = current;
          continue;
        }
        if (changed !== "all" && this.#host.changeCanAffect !== undefined) {
          const relevant = [...subscription.dependencies].filter((tableId) => changed.has(tableId));
          let canAffect: boolean;
          try {
            canAffect = await this.#host.changeCanAffect(
              subscription.query,
              relevant,
              this.#lastSeenVersion,
              current,
            );
          } catch {
            canAffect = true;
          }
          if (!this.#stillOpen()) return;
          if (isSubscriptionClosed(subscription)) continue;
          if (!canAffect) {
            this.#stats.rerunsAvoided += 1;
            this.#stats.zoneSkips += 1;
            subscription.seenVersion = current;
            continue;
          }
        }
      }
      this.#stats.reruns += 1;
      try {
        const result = await this.#host.execute(subscription.query);
        // The subscription (or the set) may have closed during the execute; a late onChange
        // after onComplete would break the subscriber contract.
        if (!this.#stillOpen() || isSubscriptionClosed(subscription)) continue;
        subscription.seenVersion = current;
        const digest = digestResult(result);
        if (digest === subscription.digest) {
          this.#stats.notificationsSuppressed += 1;
          continue;
        }
        subscription.digest = digest;
        subscription.options.onChange(result);
      } catch (error) {
        if (!this.#stillOpen() || isSubscriptionClosed(subscription)) continue;
        subscription.seenVersion = current;
        subscription.options.onError?.(error);
      }
    }
    this.#lastSeenVersion = current;
    this.#stats.lastSweepMs = performance.now() - started;
  }

  /**
   * Unions the persisted change sets of every manifest in (after, until], reading bounded pages.
   * A missing manifest version or a manifest without a change set widens to "all" — correctness
   * over selectivity.
   */
  async #changedTablesSince(after: number | null, until: number): Promise<Set<string> | "all"> {
    const changed = new Set<string>();
    let cursor = after;
    let expected = (after ?? -1) + 1;
    for (;;) {
      const page = await this.#host.manifestPage(cursor, 64);
      for (const manifest of page.records) {
        if (manifest.version > until) return changed;
        if (manifest.version !== expected) return "all";
        expected += 1;
        if (manifest.changedTableIds === undefined) return "all";
        for (const tableId of manifest.changedTableIds) changed.add(tableId);
        if (manifest.version === until) return changed;
      }
      if (page.nextCursor === null) {
        return expected > until ? changed : "all";
      }
      cursor = page.nextCursor;
    }
  }
}
