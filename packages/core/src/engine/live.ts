import { type CatalogProbe, type Manifest, type StoragePage } from "../storage/types.js";
import { type CompiledQuery, type QueryResult, type QueryRow, type QueryValue } from "./query.js";

/**
 * Live-query invalidation is a hint-driven cache-coherence problem, not a notification problem.
 * Every hint path converges on the store's durable manifest/catalog probe; persisted per-commit
 * table sets then decide which prepared queries may be stale. Hints may be lost, duplicated, or
 * reordered without changing correctness.
 *
 * Equal statements share one dependency record and one execution per sweep. Results retain an
 * exact private snapshot: a 32-bit digest is only a fast inequality check, never proof of equality.
 */

export interface LiveQueryHintChannel {
  postMessage(message: unknown): void;
  addEventListener(type: "message", listener: () => void): void;
  removeEventListener(type: "message", listener: () => void): void;
  close?(): void;
}

export interface LiveQuerySetOptions {
  readonly channel?: LiveQueryHintChannel;
  /** Creates and owns a BroadcastChannel of this name when the platform provides one. */
  readonly channelName?: string;
  readonly pollIntervalMs?: number;
  /** Maximum distinct query groups retained by this set. Defaults to 256. */
  readonly maxGroups?: number;
  /** Maximum result/observer subscriptions retained by this set. Defaults to 1,024. */
  readonly maxSubscriptions?: number;
  /** Called once when the set closes; the owner uses this to drop its reference. */
  readonly onClosed?: () => void;
}

export const DEFAULT_LIVE_QUERY_MAX_GROUPS = 256;
export const DEFAULT_LIVE_QUERY_MAX_SUBSCRIPTIONS = 1_024;
export const MAX_LIVE_QUERY_GROUPS = 4_096;
export const MAX_LIVE_QUERY_SUBSCRIPTIONS = 16_384;
export const MAX_LIVE_QUERY_SETS_PER_DATABASE = 256;

export class LiveQueryLimitError extends Error {
  override readonly name = "LiveQueryLimitError";

  constructor(
    readonly resource: "set" | "group" | "subscription",
    readonly limit: number,
  ) {
    super(
      resource === "set"
        ? `A database cannot retain more than ${String(limit)} live-query sets`
        : `A live-query set cannot retain more than ${String(limit)} ${resource} records`,
    );
  }
}

export interface LiveQuerySubscribeOptions {
  onChange(result: QueryResult): void;
  onError?(error: unknown): void;
  /** Called once when the subscription ends because the subscription or its set closed. */
  onComplete?(): void;
}

export interface LiveQueryInvalidation {
  readonly manifestVersion: number | null;
  readonly catalogEpoch: number;
  readonly initial: boolean;
}

export interface LiveQueryObserveOptions {
  onInvalidate(invalidation: LiveQueryInvalidation): void;
  onError?(error: unknown): void;
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
  /** Observer-only invalidations delivered without executing the statement inside the set. */
  invalidations: number;
  /** Work avoided because equal statements shared one query group or in-flight execution. */
  sharedExecutions: number;
  lastSweepMs: number;
}

/** A live query: plain SQL, parameterized SQL, or a compiled-plan envelope. */
export type LiveQueryInput =
  | string
  | { kind: "sql-query"; sql: string; params: readonly QueryValue[] }
  | { kind: "typed-query"; plan: CompiledQuery };

interface LiveQueryHost {
  currentProbe(): Promise<CatalogProbe>;
  manifestPage(afterVersion: number | null, limit: number): Promise<StoragePage<Manifest, number>>;
  dependencyTableIds(query: LiveQueryInput): Promise<Set<string>>;
  execute(query: LiveQueryInput): Promise<QueryResult>;
  /** Returns false only on proof that the commit window cannot affect the statement. */
  changeCanAffect?(
    query: LiveQueryInput,
    tableIds: readonly string[],
    after: number | null,
    until: number,
  ): Promise<boolean>;
}

interface ResultSubscriber {
  readonly kind: "result";
  readonly options: LiveQuerySubscribeOptions;
  delivered: boolean;
  closed: boolean;
}

interface ObserverSubscriber {
  readonly kind: "observer";
  readonly options: LiveQueryObserveOptions;
  delivered: boolean;
  closed: boolean;
}

type Subscriber = ResultSubscriber | ObserverSubscriber;

interface QueryGroup {
  readonly key: string;
  readonly query: LiveQueryInput;
  dependencies: ReadonlySet<string>;
  readonly subscribers: Set<Subscriber>;
  seenProbe: CatalogProbe;
  result: QueryResult | undefined;
  digest: number | undefined;
  execution: Promise<{ result: QueryResult; changed: boolean }> | undefined;
}

const digestScratch = new DataView(new ArrayBuffer(8));

/** Fast inequality filter. Equal digests are always followed by exact comparison. */
function digestResult(result: QueryResult): number {
  let hash = 0x811c9dc5;
  const mixByte = (byte: number): void => {
    hash = Math.imul(hash ^ byte, 0x01000193) >>> 0;
  };
  const mixNumber = (value: number): void => {
    digestScratch.setFloat64(0, value);
    for (let index = 0; index < 8; index += 1) mixByte(digestScratch.getUint8(index));
  };
  const mixString = (value: string): void => {
    for (let index = 0; index < value.length; index += 1) {
      const code = value.charCodeAt(index);
      mixByte(code & 0xff);
      mixByte(code >>> 8);
    }
    mixByte(0xff);
  };
  for (const column of result.columns) mixString(column);
  for (const row of result.rows) {
    for (const column of result.columns) {
      const value = row[column] ?? null;
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
        mixNumber(dateMilliseconds(value));
      }
    }
    mixByte(0xfe);
  }
  return hash;
}

function sameQueryValue(left: QueryValue, right: QueryValue): boolean {
  if (left instanceof Date || right instanceof Date) {
    return (
      left instanceof Date &&
      right instanceof Date &&
      Object.is(dateMilliseconds(left), dateMilliseconds(right))
    );
  }
  return Object.is(left, right);
}

function sameResult(left: QueryResult, right: QueryResult): boolean {
  if (left.columns.length !== right.columns.length || left.rows.length !== right.rows.length) {
    return false;
  }
  for (let index = 0; index < left.columns.length; index += 1) {
    if (left.columns[index] !== right.columns[index]) return false;
  }
  for (let rowIndex = 0; rowIndex < left.rows.length; rowIndex += 1) {
    const leftRow = left.rows[rowIndex];
    const rightRow = right.rows[rowIndex];
    if (leftRow === undefined || rightRow === undefined) return false;
    for (const column of left.columns) {
      if (!sameQueryValue(leftRow[column] ?? null, rightRow[column] ?? null)) return false;
    }
  }
  return true;
}

function cloneRow(row: QueryRow, columns: readonly string[]): QueryRow {
  const cloned: QueryRow = {};
  for (const column of columns) {
    const value = row[column] ?? null;
    const copy = value instanceof Date ? copyDate(value) : value;
    if (column === "__proto__") {
      Object.defineProperty(cloned, column, {
        value: copy,
        enumerable: true,
        configurable: true,
        writable: true,
      });
    } else cloned[column] = copy;
  }
  return cloned;
}

function cloneResult(result: QueryResult): QueryResult {
  const columns = [...result.columns];
  return { columns, rows: result.rows.map((row) => cloneRow(row, columns)) };
}

/** Type-tagged, length-delimited structural identity. Unlike JSON, it preserves Date vs string,
 * -0, non-finite numbers, and undefined fields, so deduplication cannot merge distinct plans. */
function encodeQueryIdentity(value: unknown, ancestors = new Set<object>()): string {
  if (value === null) return "z";
  if (typeof value === "undefined") return "u";
  if (typeof value === "boolean") return value ? "b1" : "b0";
  if (typeof value === "number") {
    if (Number.isNaN(value)) return "nNaN;";
    if (Object.is(value, -0)) return "n-0;";
    return `n${String(value)};`;
  }
  if (typeof value === "string") return `s${String(value.length)}:${value}`;
  if (value instanceof Date) return `d${String(dateMilliseconds(value))};`;
  if (typeof value !== "object") {
    throw new TypeError(`Unsupported live-query identity value: ${typeof value}`);
  }
  if (ancestors.has(value)) throw new TypeError("Live-query identity contains a cycle");
  ancestors.add(value);
  let encoded: string;
  if (Array.isArray(value)) {
    encoded = `a${String(value.length)}[${value
      .map((item) => encodeQueryIdentity(item, ancestors))
      .join("")}]`;
  } else {
    const record = value as Record<string, unknown>;
    const keys = Object.keys(record).sort();
    encoded = `o${String(keys.length)}{${keys
      .map(
        (key) =>
          `${encodeQueryIdentity(key, ancestors)}${encodeQueryIdentity(record[key], ancestors)}`,
      )
      .join("")}}`;
  }
  ancestors.delete(value);
  return encoded;
}

function queryKey(query: LiveQueryInput): string {
  return encodeQueryIdentity(
    typeof query === "string"
      ? ["sql", query]
      : query.kind === "sql-query"
        ? ["sql-query", query.sql, query.params]
        : ["typed-query", query.plan],
  );
}

function sameProbe(left: CatalogProbe, right: CatalogProbe): boolean {
  return left.manifestVersion === right.manifestVersion && left.schemaEpoch === right.schemaEpoch;
}

function versionOrdinal(version: number | null): number {
  return version ?? -1;
}

function boundedLiveLimit(value: number, maximum: number, label: string): number {
  if (!Number.isSafeInteger(value) || value < 1 || value > maximum) {
    throw new RangeError(`Live query ${label} limit must be between 1 and ${String(maximum)}`);
  }
  return value;
}

/** Structural changes have their own counter so ordinary commits and physical-index state churn
 * do not force dependency resolution. A manifest reset is still widened: it can only result from
 * replacing or rebuilding the durable database state. */
function catalogChangedBetween(previous: CatalogProbe, current: CatalogProbe): boolean {
  return (
    versionOrdinal(current.manifestVersion) < versionOrdinal(previous.manifestVersion) ||
    current.schemaEpoch !== previous.schemaEpoch
  );
}

export class LiveQuerySet {
  readonly #host: LiveQueryHost;
  readonly #channel: LiveQueryHintChannel | undefined;
  readonly #channelListener = (): void => {
    this.#hint();
  };
  readonly #groups = new Map<string, QueryGroup>();
  readonly #opening = new Map<string, Promise<QueryGroup>>();
  readonly #groupsByTable = new Map<string, Set<QueryGroup>>();
  readonly #maxGroups: number;
  readonly #maxSubscriptions: number;
  readonly #stats: LiveQueryStats = {
    hints: 0,
    versionChecks: 0,
    sweeps: 0,
    reruns: 0,
    rerunsAvoided: 0,
    zoneSkips: 0,
    notificationsSuppressed: 0,
    invalidations: 0,
    sharedExecutions: 0,
    lastSweepMs: 0,
  };
  #lastProbe: CatalogProbe | undefined;
  #sweepChain = Promise.resolve();
  #sweepQueued = false;
  #pollTimer: ReturnType<typeof setInterval> | undefined;
  #closed = false;
  #subscriptionCount = 0;
  readonly #ownsChannel: boolean;
  readonly #onClosed: (() => void) | undefined;

  constructor(host: LiveQueryHost, options: LiveQuerySetOptions = {}) {
    if (
      options.pollIntervalMs !== undefined &&
      (!Number.isSafeInteger(options.pollIntervalMs) || options.pollIntervalMs <= 0)
    ) {
      throw new RangeError("Live query poll interval must be a positive whole number");
    }
    this.#maxGroups = boundedLiveLimit(
      options.maxGroups ?? DEFAULT_LIVE_QUERY_MAX_GROUPS,
      MAX_LIVE_QUERY_GROUPS,
      "group",
    );
    this.#maxSubscriptions = boundedLiveLimit(
      options.maxSubscriptions ?? DEFAULT_LIVE_QUERY_MAX_SUBSCRIPTIONS,
      MAX_LIVE_QUERY_SUBSCRIPTIONS,
      "subscription",
    );
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

  #throwIfClosed(): void {
    if (this.#closed) throw new Error("Live query set is closed");
  }

  #reserveSubscription(): void {
    if (this.#subscriptionCount >= this.#maxSubscriptions) {
      throw new LiveQueryLimitError("subscription", this.#maxSubscriptions);
    }
    this.#subscriptionCount += 1;
  }

  /** Registers a query, delivers its current result, and shares work with equal statements. */
  async subscribe(
    query: LiveQueryInput,
    options: LiveQuerySubscribeOptions,
  ): Promise<LiveQuerySubscription> {
    this.#throwIfClosed();
    this.#reserveSubscription();
    let group: QueryGroup | undefined;
    let subscriber: ResultSubscriber | undefined;
    try {
      group = await this.#getOrOpenGroup(query);
      this.#throwIfClosed();
      subscriber = {
        kind: "result",
        options,
        delivered: false,
        closed: false,
      };
      group.subscribers.add(subscriber);
      // Reconcile before exposing a shared cached result to a late subscriber.
      await this.refresh();
      this.#throwIfClosed();
      if (!subscriber.delivered) {
        const result = group.result ?? (await this.#executeGroup(group)).result;
        this.#throwIfClosed();
        if (!subscriber.closed) this.#deliverResult(subscriber, result);
      }
    } catch (error) {
      if (group !== undefined && subscriber !== undefined) {
        this.#removeSubscriber(group, subscriber, false);
      } else {
        this.#subscriptionCount -= 1;
        if (group !== undefined) this.#removeEmptyGroup(group);
      }
      throw error;
    }
    return this.#subscriptionHandle(group, subscriber);
  }

  /** Observes invalidation without executing the statement inside the set. */
  async observe(
    query: LiveQueryInput,
    options: LiveQueryObserveOptions,
  ): Promise<LiveQuerySubscription> {
    this.#throwIfClosed();
    this.#reserveSubscription();
    let group: QueryGroup | undefined;
    let subscriber: ObserverSubscriber | undefined;
    try {
      group = await this.#getOrOpenGroup(query);
      this.#throwIfClosed();
      subscriber = {
        kind: "observer",
        options,
        delivered: false,
        closed: false,
      };
      group.subscribers.add(subscriber);
      await this.refresh();
      this.#throwIfClosed();
      if (!subscriber.delivered) {
        this.#deliverInvalidation(subscriber, { ...group.seenProbe, initial: true });
      }
    } catch (error) {
      if (group !== undefined && subscriber !== undefined) {
        this.#removeSubscriber(group, subscriber, false);
      } else {
        this.#subscriptionCount -= 1;
        if (group !== undefined) this.#removeEmptyGroup(group);
      }
      throw error;
    }
    return this.#subscriptionHandle(group, subscriber);
  }

  #subscriptionHandle(group: QueryGroup, subscriber: Subscriber): LiveQuerySubscription {
    return {
      dependencyTableIds: [...group.dependencies],
      close: () => {
        this.#removeSubscriber(group, subscriber, true);
      },
    };
  }

  #removeSubscriber(group: QueryGroup, subscriber: Subscriber, complete: boolean): void {
    if (subscriber.closed) return;
    subscriber.closed = true;
    this.#subscriptionCount -= 1;
    group.subscribers.delete(subscriber);
    if (complete) subscriber.options.onComplete?.();
    this.#removeEmptyGroup(group);
  }

  #removeEmptyGroup(group: QueryGroup): void {
    if (group.subscribers.size !== 0) return;
    if (this.#groups.get(group.key) !== group) return;
    this.#groups.delete(group.key);
    this.#unindexGroup(group, group.dependencies);
  }

  async #getOrOpenGroup(query: LiveQueryInput): Promise<QueryGroup> {
    const key = queryKey(query);
    const existing = this.#groups.get(key);
    if (existing !== undefined) {
      this.#stats.sharedExecutions += 1;
      return existing;
    }
    const opening = this.#opening.get(key);
    if (opening !== undefined) {
      this.#stats.sharedExecutions += 1;
      return opening;
    }
    if (this.#groups.size + this.#opening.size >= this.#maxGroups) {
      throw new LiveQueryLimitError("group", this.#maxGroups);
    }
    const created = this.#openGroup(key, query);
    this.#opening.set(key, created);
    try {
      return await created;
    } finally {
      this.#opening.delete(key);
    }
  }

  async #openGroup(key: string, query: LiveQueryInput): Promise<QueryGroup> {
    const { dependencies, probe: after } = await this.#stableDependencies(query);
    const group: QueryGroup = {
      key,
      query,
      dependencies,
      subscribers: new Set(),
      seenProbe: after,
      result: undefined,
      digest: undefined,
      execution: undefined,
    };
    this.#groups.set(key, group);
    this.#indexGroup(group, dependencies);
    if (
      this.#lastProbe === undefined ||
      versionOrdinal(after.manifestVersion) < versionOrdinal(this.#lastProbe.manifestVersion)
    ) {
      this.#lastProbe = after;
    }
    return group;
  }

  #indexGroup(group: QueryGroup, dependencies: ReadonlySet<string>): void {
    for (const tableId of dependencies) {
      let groups = this.#groupsByTable.get(tableId);
      if (groups === undefined) {
        groups = new Set();
        this.#groupsByTable.set(tableId, groups);
      }
      groups.add(group);
    }
  }

  #unindexGroup(group: QueryGroup, dependencies: ReadonlySet<string>): void {
    for (const tableId of dependencies) {
      const groups = this.#groupsByTable.get(tableId);
      groups?.delete(group);
      if (groups?.size === 0) this.#groupsByTable.delete(tableId);
    }
  }

  async #refreshDependencies(group: QueryGroup): Promise<void> {
    const { dependencies: next } = await this.#stableDependencies(group.query);
    if (!this.#stillOpen() || !this.#groups.has(group.key)) return;
    this.#unindexGroup(group, group.dependencies);
    group.dependencies = next;
    this.#indexGroup(group, next);
  }

  /** Resolve a view/table set inside a catalog-stable probe window or refuse stale indexing. */
  async #stableDependencies(
    query: LiveQueryInput,
  ): Promise<{ dependencies: Set<string>; probe: CatalogProbe }> {
    let before = await this.#host.currentProbe();
    this.#throwIfClosed();
    for (let attempt = 0; attempt < 8; attempt += 1) {
      const dependencies = await this.#host.dependencyTableIds(query);
      this.#throwIfClosed();
      const after = await this.#host.currentProbe();
      this.#throwIfClosed();
      if (!catalogChangedBetween(before, after)) return { dependencies, probe: after };
      before = after;
    }
    throw new Error("Catalog kept changing while live-query dependencies were resolved");
  }

  async #executeGroup(group: QueryGroup): Promise<{ result: QueryResult; changed: boolean }> {
    if (group.execution !== undefined) {
      this.#stats.sharedExecutions += 1;
      return group.execution;
    }
    const execution = (async () => {
      const executed = await this.#host.execute(group.query);
      const digest = digestResult(executed);
      const previous = group.result;
      const changed =
        previous === undefined || group.digest !== digest || !sameResult(previous, executed);
      const retained = cloneResult(executed);
      group.result = retained;
      group.digest = digest;
      return { result: retained, changed };
    })();
    group.execution = execution;
    try {
      return await execution;
    } finally {
      if (group.execution === execution) group.execution = undefined;
    }
  }

  #deliverResult(subscriber: ResultSubscriber, result: QueryResult): void {
    if (subscriber.closed) return;
    subscriber.delivered = true;
    subscriber.options.onChange(cloneResult(result));
  }

  #deliverInvalidation(subscriber: ObserverSubscriber, invalidation: LiveQueryInvalidation): void {
    if (subscriber.closed) return;
    subscriber.delivered = true;
    subscriber.options.onInvalidate(invalidation);
  }

  /** Called by the owning database after each local write commit; also hints other tabs. */
  notifyLocalCommit(): void {
    this.#channel?.postMessage("minnow-commit");
    this.#hint();
  }

  /** Runs one authoritative probe and selective re-execution sweep. */
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
    const groups = [...this.#groups.values()];
    this.#groups.clear();
    this.#groupsByTable.clear();
    for (const group of groups) {
      const subscribers = [...group.subscribers];
      group.subscribers.clear();
      for (const subscriber of subscribers) {
        if (subscriber.closed) continue;
        subscriber.closed = true;
        this.#subscriptionCount -= 1;
        subscriber.options.onComplete?.();
      }
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
        for (const group of this.#groups.values()) this.#notifyGroupError(group, error);
      }
    });
  }

  #stillOpen(): boolean {
    return !this.#closed;
  }

  async #sweep(): Promise<void> {
    if (this.#closed || this.#groups.size === 0) return;
    this.#stats.versionChecks += 1;
    const current = await this.#host.currentProbe();
    if (!this.#stillOpen()) return;
    const last = this.#lastProbe ?? current;
    const anyLagging = [...this.#groups.values()].some(
      (group) => !sameProbe(group.seenProbe, current),
    );
    if (sameProbe(last, current) && !anyLagging) return;

    const started = performance.now();
    this.#stats.sweeps += 1;
    const windowCache = new Map<number, Promise<Set<string> | "all">>();
    const changedSince = (after: number | null): Promise<Set<string> | "all"> => {
      const key = versionOrdinal(after);
      let pending = windowCache.get(key);
      if (pending === undefined) {
        pending = this.#changedTablesSince(after, current.manifestVersion);
        windowCache.set(key, pending);
      }
      return pending;
    };

    for (const group of [...this.#groups.values()]) {
      if (!this.#stillOpen()) return;
      if (group.subscribers.size === 0 || sameProbe(group.seenProbe, current)) continue;
      const prior = group.seenProbe;
      const catalogChanged = catalogChangedBetween(prior, current);
      if (catalogChanged) {
        try {
          await this.#refreshDependencies(group);
        } catch (error) {
          this.#notifyGroupError(group, error);
          continue;
        }
      }

      let relevant: string[] = [];
      let affected = catalogChanged;
      if (!affected && prior.manifestVersion !== current.manifestVersion) {
        const changed = await changedSince(prior.manifestVersion);
        if (!this.#stillOpen()) return;
        if (changed === "all") affected = true;
        else {
          relevant = [...group.dependencies].filter((tableId) => changed.has(tableId));
          affected = relevant.length > 0;
        }
      }
      if (!affected) {
        this.#stats.rerunsAvoided += 1;
        group.seenProbe = current;
        continue;
      }

      if (
        !catalogChanged &&
        relevant.length > 0 &&
        current.manifestVersion !== null &&
        this.#host.changeCanAffect !== undefined
      ) {
        let canAffect: boolean;
        try {
          canAffect = await this.#host.changeCanAffect(
            group.query,
            relevant,
            prior.manifestVersion,
            current.manifestVersion,
          );
        } catch {
          canAffect = true;
        }
        if (!this.#stillOpen()) return;
        if (!canAffect) {
          this.#stats.rerunsAvoided += 1;
          this.#stats.zoneSkips += 1;
          group.seenProbe = current;
          continue;
        }
      }

      const resultSubscribers = [...group.subscribers].filter(
        (subscriber): subscriber is ResultSubscriber =>
          subscriber.kind === "result" && !subscriber.closed,
      );
      const observers = [...group.subscribers].filter(
        (subscriber): subscriber is ObserverSubscriber =>
          subscriber.kind === "observer" && !subscriber.closed,
      );
      try {
        let execution: { result: QueryResult; changed: boolean } | undefined;
        if (resultSubscribers.length > 0) {
          // An initial delivery may be executing against the pre-sweep snapshot. Waiting for it
          // here can deadlock callers that intentionally hold that execute while awaiting this
          // refresh. Leave the group dirty; once the initial delivery lands, the next refresh
          // re-runs it against `current`.
          if (group.result === undefined && group.execution !== undefined) continue;
          this.#stats.reruns += 1;
          execution = await this.#executeGroup(group);
          if (!this.#stillOpen() || !this.#groups.has(group.key)) continue;
        }
        for (const observer of observers) {
          try {
            this.#deliverInvalidation(observer, { ...current, initial: false });
            this.#stats.invalidations += 1;
          } catch (error) {
            observer.options.onError?.(error);
          }
        }
        if (execution !== undefined) {
          if (execution.changed) {
            for (const subscriber of resultSubscribers) {
              try {
                this.#deliverResult(subscriber, execution.result);
              } catch (error) {
                subscriber.options.onError?.(error);
              }
            }
          } else {
            this.#stats.notificationsSuppressed += resultSubscribers.length;
          }
        }
        group.seenProbe = current;
      } catch (error) {
        // Do not advance seenProbe. A refresh with no newer commit retries the dirty group.
        this.#notifyGroupError(group, error);
      }
    }
    this.#lastProbe = current;
    this.#stats.lastSweepMs = performance.now() - started;
  }

  #notifyGroupError(group: QueryGroup, error: unknown): void {
    for (const subscriber of group.subscribers) {
      if (subscriber.closed) continue;
      subscriber.options.onError?.(error);
    }
  }

  /** Unions persisted table change sets in (after, until], widening on any history gap. */
  async #changedTablesSince(
    after: number | null,
    until: number | null,
  ): Promise<Set<string> | "all"> {
    if (until === null || after === until) return new Set();
    const changed = new Set<string>();
    let cursor = after;
    let expected = (after ?? -1) + 1;
    for (;;) {
      const page = await this.#host.manifestPage(cursor, 64);
      for (const manifest of page.records) {
        if (manifest.version > until) return changed;
        if (manifest.version !== expected) return "all";
        expected += 1;
        for (const tableId of manifest.changedTableIds) changed.add(tableId);
        if (manifest.version === until) return changed;
      }
      if (page.nextCursor === null) return expected > until ? changed : "all";
      cursor = page.nextCursor;
    }
  }
}
import { copyDate, dateMilliseconds } from "../date-value.js";
