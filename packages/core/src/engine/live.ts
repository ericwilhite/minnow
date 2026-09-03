import { type CatalogProbe, type Manifest, type StoragePage } from "../storage/types.js";
import { LiveQueryLimitError } from "./errors.js";
import { type CompiledQuery, type QueryResult, type QueryRow, type QueryValue } from "./query.js";

/**
 * Live-query invalidation is a hint-driven cache-coherence problem, not a notification problem.
 * Every hint path converges on the store's durable manifest/catalog probe; persisted per-commit
 * table sets then decide which prepared queries may be stale. Hints may be lost, duplicated, or
 * reordered without changing correctness.
 *
 * A sweep costs what changed, not what is subscribed: the commit window's table set selects the
 * groups to visit through a per-table index, and a group nobody visits keeps its result on the
 * strength of the invariant that every window touching one of its tables would have visited it.
 * Equal statements share one dependency record and one execution per sweep. Results are compared
 * exactly, row by row, so an unchanged result never reaches a subscriber.
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
  /**
   * Hand `onChange` the set's retained result instead of a private copy. The result is shared
   * with every equal subscription and with the next change comparison, so a subscriber must
   * treat it as read-only. A consumer that only reads it synchronously — the worker host that
   * encodes it for the channel, a renderer that copies what it displays — saves one full copy
   * per subscriber per change.
   */
  readonly sharedResults?: boolean;
  /** Called once when the set closes; the owner uses this to drop its reference. */
  readonly onClosed?: () => void;
}

export const DEFAULT_LIVE_QUERY_MAX_GROUPS = 256;
export const DEFAULT_LIVE_QUERY_MAX_SUBSCRIPTIONS = 1_024;
export const MAX_LIVE_QUERY_GROUPS = 4_096;
export const MAX_LIVE_QUERY_SUBSCRIPTIONS = 16_384;
export const MAX_LIVE_QUERY_SETS_PER_DATABASE = 256;
/**
 * Executions one set runs at once. A page mounting hundreds of subscriptions in one turn used
 * to queue them behind one sweep chain; without that, they would race straight into the
 * engine's active-read ceiling. This keeps the engine busy without exhausting it.
 */
const LIVE_QUERY_EXECUTION_CONCURRENCY = 8;

export { LiveQueryLimitError } from "./errors.js";

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
  /**
   * Execute the statement inside the set on every relevant commit and invalidate only when the
   * rows changed. The engine keeps that execution in its result memo, so an adapter that then
   * re-executes the same statement at the same version is served from cache rather than from a
   * second scan. A commit that leaves the rows as they were costs one execution and reaches no
   * observer at all — nothing crosses a worker channel and nothing re-renders.
   */
  readonly suppressUnchanged?: boolean;
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
  /** Subscribed groups a sweep did not re-run: nothing they read changed, or a proof said so. */
  rerunsAvoided: number;
  /** Re-runs skipped because the data layer proved the commits could not change the result. */
  zoneSkips: number;
  /** Deliveries withheld because an execution produced exactly the rows already delivered. */
  notificationsSuppressed: number;
  /** Observer invalidations delivered. */
  invalidations: number;
  /** Groups a sweep looked at: those whose tables the commits changed, plus any left lagging. */
  groupsVisited: number;
  /** Work avoided because equal statements shared one query group or in-flight execution. */
  sharedExecutions: number;
  lastSweepMs: number;
}

/** A live query: plain SQL, parameterized SQL, or a compiled-plan envelope. */
export type LiveQueryInput =
  | string
  | { kind: "sql-query"; sql: string; params: readonly QueryValue[] }
  | { kind: "typed-query"; plan: CompiledQuery };

/** What the set already knows when it asks the host to execute a statement. */
export interface LiveQueryExecuteContext {
  /**
   * A freshness probe the set read moments ago. The host may start execution from it instead of
   * reading its own; a result may still observe a newer commit, and the set treats the probe as
   * a lower bound on what the result reflects.
   */
  readonly probe: CatalogProbe;
  /**
   * Whether the host should keep the result in its memo. The set retains its own copy, so a
   * memo entry only pays off when another caller — an adapter re-executing after an
   * invalidation — will ask for the same statement at the same version.
   */
  readonly memoize: boolean;
}

export interface LiveQueryHost {
  currentProbe(): Promise<CatalogProbe>;
  manifestPage(afterVersion: number | null, limit: number): Promise<StoragePage<Manifest, number>>;
  /** The base tables the statement reads, resolved through views; `probe` is a recent read. */
  dependencyTableIds(query: LiveQueryInput, probe?: CatalogProbe): Promise<Set<string>>;
  /** Executes the statement; the returned result belongs to the set and is never shared. */
  execute(query: LiveQueryInput, context?: LiveQueryExecuteContext): Promise<QueryResult>;
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

interface Execution {
  readonly result: QueryResult;
  readonly changed: boolean;
}

interface QueryGroup {
  readonly key: string;
  readonly query: LiveQueryInput;
  dependencies: ReadonlySet<string>;
  readonly subscribers: Set<Subscriber>;
  /** The newest probe the retained result is known to reflect. */
  seenProbe: CatalogProbe;
  result: QueryResult | undefined;
  execution: Promise<Execution> | undefined;
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

/** Exact structural equality; stops at the first difference. */
function sameResult(left: QueryResult, right: QueryResult): boolean {
  if (left.columns.length !== right.columns.length || left.rows.length !== right.rows.length) {
    return false;
  }
  for (let index = 0; index < left.columns.length; index += 1) {
    if (left.columns[index] !== right.columns[index]) return false;
    if (JSON.stringify(left.columnDomains[index]) !== JSON.stringify(right.columnDomains[index])) {
      return false;
    }
  }
  const columns = left.columns;
  for (let rowIndex = 0; rowIndex < left.rows.length; rowIndex += 1) {
    const leftRow = left.rows[rowIndex];
    const rightRow = right.rows[rowIndex];
    if (leftRow === undefined || rightRow === undefined) return false;
    if (leftRow === rightRow) continue;
    for (const column of columns) {
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
  return {
    columns,
    columnDomains: structuredClone(result.columnDomains),
    rows: result.rows.map((row) => cloneRow(row, columns)),
  };
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

function newerProbe(left: CatalogProbe, right: CatalogProbe): CatalogProbe {
  return versionOrdinal(right.manifestVersion) > versionOrdinal(left.manifestVersion)
    ? right
    : left;
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

function isResultSubscriber(subscriber: Subscriber): subscriber is ResultSubscriber {
  return subscriber.kind === "result" && !subscriber.closed;
}

function isObserver(subscriber: Subscriber): subscriber is ObserverSubscriber {
  return subscriber.kind === "observer" && !subscriber.closed;
}

/** Whether a sweep must execute this group: someone wants rows, or wants silence unless rows changed. */
function groupExecutes(group: QueryGroup): boolean {
  for (const subscriber of group.subscribers) {
    if (subscriber.closed) continue;
    if (subscriber.kind === "result" || subscriber.options.suppressUnchanged === true) return true;
  }
  return false;
}

/** Whether an adapter will re-execute this statement after an invalidation and expect a memo hit. */
function groupMemoizes(group: QueryGroup): boolean {
  for (const subscriber of group.subscribers) {
    if (!subscriber.closed && subscriber.kind === "observer") return true;
  }
  return false;
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
  /**
   * Groups whose result is not known to reflect `#lastProbe`: opened against an older probe,
   * failed a re-run, or skipped while their initial execution was in flight. Every other group
   * is current as of the last sweep, because a sweep visits every group whose tables changed.
   */
  readonly #lagging = new Set<QueryGroup>();
  readonly #maxGroups: number;
  readonly #maxSubscriptions: number;
  readonly #sharedResults: boolean;
  readonly #stats: LiveQueryStats = {
    hints: 0,
    versionChecks: 0,
    sweeps: 0,
    reruns: 0,
    rerunsAvoided: 0,
    zoneSkips: 0,
    notificationsSuppressed: 0,
    invalidations: 0,
    groupsVisited: 0,
    sharedExecutions: 0,
    lastSweepMs: 0,
  };
  /** The probe the last completed sweep brought every non-lagging group up to. */
  #lastProbe: CatalogProbe | undefined;
  /** A probe read that has been requested but not yet started; see `#freshProbe`. */
  #pendingProbe: Promise<CatalogProbe> | undefined;
  #executing = 0;
  readonly #executionWaiters: Array<() => void> = [];
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
    this.#sharedResults = options.sharedResults === true;
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

  /**
   * One durable probe shared by every caller that asks for it in the same turn. The read
   * starts a microtask later, so it is at least as fresh as each caller needs: a commit any of
   * them awaited has landed before the read begins. A hundred subscriptions opening together
   * then pay two store reads between them instead of two each — on IndexedDB, two transactions
   * instead of two hundred. Sequential callers share nothing and lose nothing.
   */
  #freshProbe(): Promise<CatalogProbe> {
    let pending = this.#pendingProbe;
    if (pending === undefined) {
      pending = Promise.resolve().then(() => {
        if (this.#pendingProbe === pending) this.#pendingProbe = undefined;
        return this.#host.currentProbe();
      });
      this.#pendingProbe = pending;
    }
    return pending;
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
      const opened = await this.#getOrOpenGroup(query);
      group = opened.group;
      this.#throwIfClosed();
      subscriber = {
        kind: "result",
        options,
        delivered: false,
        closed: false,
      };
      group.subscribers.add(subscriber);
      await this.#settleGroup(group, opened.fresh, true);
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

  /** Observes invalidation; the statement executes inside the set only when asked to compare. */
  async observe(
    query: LiveQueryInput,
    options: LiveQueryObserveOptions,
  ): Promise<LiveQuerySubscription> {
    this.#throwIfClosed();
    this.#reserveSubscription();
    let group: QueryGroup | undefined;
    let subscriber: ObserverSubscriber | undefined;
    try {
      const opened = await this.#getOrOpenGroup(query);
      group = opened.group;
      this.#throwIfClosed();
      subscriber = {
        kind: "observer",
        options,
        delivered: false,
        closed: false,
      };
      group.subscribers.add(subscriber);
      await this.#settleGroup(group, opened.fresh, options.suppressUnchanged === true);
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

  /**
   * Brings a group to a state a new subscriber may read from. A group opened by this call is
   * current as of the probe its dependencies were resolved under, so it needs no sweep — only
   * its first execution, primed from that probe. A group that already existed, or one a
   * concurrent sweep left lagging, reconciles through one authoritative sweep first, which
   * also delivers to the new subscriber when the group changed.
   */
  async #settleGroup(group: QueryGroup, fresh: boolean, execute: boolean): Promise<void> {
    if (execute && group.result === undefined) {
      if (group.execution !== undefined) {
        this.#stats.sharedExecutions += 1;
        await group.execution;
      } else {
        await this.#executeGroup(group, {
          probe: group.seenProbe,
          memoize: groupMemoizes(group),
        });
      }
      this.#throwIfClosed();
    }
    if (!fresh || this.#lagging.has(group)) await this.refresh();
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
    this.#lagging.delete(group);
    this.#unindexGroup(group, group.dependencies);
  }

  async #getOrOpenGroup(query: LiveQueryInput): Promise<{ group: QueryGroup; fresh: boolean }> {
    const key = queryKey(query);
    const existing = this.#groups.get(key);
    if (existing !== undefined) {
      this.#stats.sharedExecutions += 1;
      return { group: existing, fresh: false };
    }
    const opening = this.#opening.get(key);
    if (opening !== undefined) {
      this.#stats.sharedExecutions += 1;
      return { group: await opening, fresh: false };
    }
    if (this.#groups.size + this.#opening.size >= this.#maxGroups) {
      throw new LiveQueryLimitError("group", this.#maxGroups);
    }
    const created = this.#openGroup(key, query);
    this.#opening.set(key, created);
    try {
      return { group: await created, fresh: true };
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
      execution: undefined,
    };
    this.#groups.set(key, group);
    this.#indexGroup(group, dependencies);
    if (this.#lastProbe === undefined) this.#lastProbe = after;
    else if (
      versionOrdinal(after.manifestVersion) < versionOrdinal(this.#lastProbe.manifestVersion) ||
      after.schemaEpoch !== this.#lastProbe.schemaEpoch
    ) {
      // Opened against a probe older than the last sweep: only a sweep can say whether the
      // commits in between touched its tables.
      this.#lagging.add(group);
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
    let before = await this.#freshProbe();
    this.#throwIfClosed();
    for (let attempt = 0; attempt < 8; attempt += 1) {
      const dependencies = await this.#host.dependencyTableIds(query, before);
      this.#throwIfClosed();
      const after = await this.#freshProbe();
      this.#throwIfClosed();
      if (!catalogChangedBetween(before, after)) return { dependencies, probe: after };
      before = after;
    }
    throw new Error("Catalog kept changing while live-query dependencies were resolved");
  }

  async #executeGroup(group: QueryGroup, context?: LiveQueryExecuteContext): Promise<Execution> {
    if (group.execution !== undefined) {
      this.#stats.sharedExecutions += 1;
      return group.execution;
    }
    const execution = (async (): Promise<Execution> => {
      await this.#acquireExecutionSlot();
      let executed: QueryResult;
      try {
        executed = await this.#host.execute(group.query, context);
      } finally {
        this.#releaseExecutionSlot();
      }
      const previous = group.result;
      const changed = previous === undefined || !sameResult(previous, executed);
      // The host hands over a result nobody else holds, so the set retains it as it is.
      group.result = executed;
      return { result: executed, changed };
    })();
    group.execution = execution;
    try {
      return await execution;
    } finally {
      if (group.execution === execution) group.execution = undefined;
    }
  }

  async #acquireExecutionSlot(): Promise<void> {
    if (this.#executing < LIVE_QUERY_EXECUTION_CONCURRENCY) {
      this.#executing += 1;
      return;
    }
    await new Promise<void>((resolve) => {
      this.#executionWaiters.push(resolve);
    });
    this.#executing += 1;
  }

  #releaseExecutionSlot(): void {
    this.#executing -= 1;
    this.#executionWaiters.shift()?.();
  }

  #deliverResult(subscriber: ResultSubscriber, result: QueryResult): void {
    if (subscriber.closed) return;
    subscriber.delivered = true;
    subscriber.options.onChange(this.#sharedResults ? result : cloneResult(result));
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
    this.#lagging.clear();
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

  /**
   * The groups one sweep must look at. A moved schema epoch or a manifest reset re-resolves
   * every dependency; a missing stretch of history re-runs everything; otherwise the tables
   * the window changed pick their groups from the index, plus whatever was already lagging.
   */
  async #sweepCandidates(
    last: CatalogProbe,
    current: CatalogProbe,
    changedSince: (after: number | null) => Promise<Set<string> | "all">,
  ): Promise<QueryGroup[] | undefined> {
    if (catalogChangedBetween(last, current)) return [...this.#groups.values()];
    if (sameProbe(last, current)) return [...this.#lagging];
    const changed = await changedSince(last.manifestVersion);
    if (!this.#stillOpen()) return undefined;
    if (changed === "all") return [...this.#groups.values()];
    const candidates = new Set(this.#lagging);
    for (const tableId of changed) {
      const groups = this.#groupsByTable.get(tableId);
      if (groups === undefined) continue;
      for (const group of groups) candidates.add(group);
    }
    return [...candidates];
  }

  async #sweep(): Promise<void> {
    if (this.#closed || this.#groups.size === 0) return;
    this.#stats.versionChecks += 1;
    const current = await this.#freshProbe();
    if (!this.#stillOpen()) return;
    const last = this.#lastProbe ?? current;
    if (sameProbe(last, current) && this.#lagging.size === 0) return;

    const started = performance.now();
    this.#stats.sweeps += 1;
    const rerunsBefore = this.#stats.reruns;
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

    const candidates = await this.#sweepCandidates(last, current, changedSince);
    if (candidates === undefined) return;
    for (const group of candidates) {
      if (!this.#stillOpen()) return;
      if (group.subscribers.size === 0 || this.#groups.get(group.key) !== group) continue;
      this.#stats.groupsVisited += 1;
      if (sameProbe(group.seenProbe, current)) {
        this.#lagging.delete(group);
        continue;
      }
      // A group nobody had to visit since its last sweep is current as of that sweep's probe:
      // its window starts there, not at the older probe it last executed under.
      const prior = this.#lagging.has(group) ? group.seenProbe : newerProbe(group.seenProbe, last);
      const catalogChanged = catalogChangedBetween(prior, current);
      if (catalogChanged) {
        try {
          await this.#refreshDependencies(group);
        } catch (error) {
          this.#lagging.add(group);
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
        this.#settleProbe(group, current);
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
          this.#stats.zoneSkips += 1;
          this.#settleProbe(group, current);
          continue;
        }
      }

      try {
        let execution: Execution | undefined;
        if (groupExecutes(group)) {
          // An initial delivery may be executing against the pre-sweep snapshot. Waiting for it
          // here can deadlock callers that intentionally hold that execute while awaiting this
          // refresh. Leave the group lagging; once the initial delivery lands, the next refresh
          // re-runs it against `current`.
          if (group.result === undefined && group.execution !== undefined) {
            this.#lagging.add(group);
            continue;
          }
          this.#stats.reruns += 1;
          execution = await this.#executeGroup(group, {
            probe: current,
            memoize: groupMemoizes(group),
          });
          if (!this.#stillOpen() || this.#groups.get(group.key) !== group) continue;
        }
        const invalidation: LiveQueryInvalidation = { ...current, initial: false };
        for (const subscriber of [...group.subscribers]) {
          if (isObserver(subscriber)) {
            if (
              execution !== undefined &&
              !execution.changed &&
              subscriber.options.suppressUnchanged === true
            ) {
              this.#stats.notificationsSuppressed += 1;
              continue;
            }
            try {
              this.#deliverInvalidation(subscriber, invalidation);
              this.#stats.invalidations += 1;
            } catch (error) {
              subscriber.options.onError?.(error);
            }
          } else if (isResultSubscriber(subscriber) && execution !== undefined) {
            if (!execution.changed) {
              this.#stats.notificationsSuppressed += 1;
              continue;
            }
            try {
              this.#deliverResult(subscriber, execution.result);
            } catch (error) {
              subscriber.options.onError?.(error);
            }
          }
        }
        this.#settleProbe(group, current);
      } catch (error) {
        // The group stays lagging. A refresh with no newer commit retries it.
        this.#lagging.add(group);
        this.#notifyGroupError(group, error);
      }
    }
    this.#lastProbe = current;
    this.#stats.rerunsAvoided += this.#groups.size - (this.#stats.reruns - rerunsBefore);
    this.#stats.lastSweepMs = performance.now() - started;
  }

  #settleProbe(group: QueryGroup, probe: CatalogProbe): void {
    group.seenProbe = probe;
    this.#lagging.delete(group);
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
