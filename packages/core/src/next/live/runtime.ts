/** Owner-only live execution. Commit notifications are hints backed by ChangeFeed.probe().
 * Each subscription owns one dirty bit and at most one unacknowledged result. A sweep pins
 * its complete cohort synchronously, executes at most eight at once and delivers together.
 */
import type { ChangeFeed, CommittedChange, RekeyEffect } from "../access/changes.js";
import { MemoryAdmissionError, stringBytes, type MemoryGovernor } from "../memory/governor.js";
import type { EngineSession, OwnerEngine, PlannedStatement } from "../owner/engine.js";
import type { FramePayload } from "../owner/frame.js";
import type { WireColumnMeta, WireValue } from "../owner/protocol.js";
import { LIVE_UNACKED_DROP_MS } from "./constants.js";

export interface LiveDelivery {
  readonly version: number;
  readonly lsn: number;
  readonly reset: boolean;
  readonly rekeys: readonly RekeyEffect[];
  readonly columns: readonly WireColumnMeta[];
  readonly frames: readonly FramePayload[];
}
export interface LiveHandle {
  ack(version: number, accepted?: boolean): void;
  refresh(): void;
  close(): void;
}
interface Entry {
  readonly sql: string;
  readonly params: readonly WireValue[];
  readonly deliver: (result: LiveDelivery) => void;
  readonly error: (error: unknown) => void;
  readonly ended: () => void;
  readonly metadata: number;
  readonly abort: AbortController;
  tables: readonly number[];
  dependencyBytes: number;
  retired: boolean;
  ready: Promise<void> | null;
  dirty: boolean;
  closed: boolean;
  running: boolean;
  waiting: number | null;
  sentAt: number;
  acknowledgedLsn: number | null;
  retained: LiveDelivery | null;
  bytes: number;
  stage: LiveDelivery | null;
  stageBytes: number;
  cloneBytes: number;
  failure: unknown;
  failed: boolean;
  session: EngineSession | null;
  plan: PlannedStatement | null;
}

function equal(left: unknown, right: unknown): boolean {
  if (Object.is(left, right)) return true;
  if (left === null || right === null || typeof left !== "object" || typeof right !== "object")
    return false;
  if (ArrayBuffer.isView(left) && ArrayBuffer.isView(right)) {
    if (left.constructor !== right.constructor || left.byteLength !== right.byteLength)
      return false;
    const a = new Uint8Array(left.buffer, left.byteOffset, left.byteLength);
    const b = new Uint8Array(right.buffer, right.byteOffset, right.byteLength);
    for (let i = 0; i < a.length; i++) if (a[i] !== b[i]) return false;
    return true;
  }
  const a = Object.keys(left),
    b = Object.keys(right);
  return (
    a.length === b.length &&
    a.every((key, i) => key === b[i] && equal(Reflect.get(left, key), Reflect.get(right, key)))
  );
}

/** Budget captured parameters before structured clone. Includes array/object/map descriptors. */
function valueBytes(value: unknown): number {
  if (typeof value === "string") return stringBytes(value);
  if (typeof value === "bigint") return 64 + value.toString(16).length;
  if (value === null || typeof value !== "object") return 32;
  if (ArrayBuffer.isView(value)) return 128 + value.byteLength;
  if (value instanceof Date) return 64;
  let bytes = 128;
  for (const key of Object.keys(value))
    bytes += stringBytes(key) + valueBytes(Reflect.get(value, key));
  return bytes;
}

export class LiveRuntime {
  readonly #entries = new Set<Entry>();
  readonly #byTable = new Map<number, Set<Entry>>();
  readonly #cache;
  readonly #stop: () => void;
  readonly #poll: ReturnType<typeof setInterval>;
  #used = 0;
  #seen: number;
  #timer: ReturnType<typeof setTimeout> | null = null;
  #sweeping = false;
  #work: Promise<void> | null = null;
  #closed = false;
  #id = 0;
  #version = 0;
  readonly #limit: number;
  constructor(
    readonly engine: OwnerEngine,
    readonly governor: MemoryGovernor,
    readonly feed: ChangeFeed,
    options: { readonly pollIntervalMs?: number } = {},
  ) {
    if (
      options.pollIntervalMs !== undefined &&
      (!Number.isSafeInteger(options.pollIntervalMs) || options.pollIntervalMs < 1)
    )
      throw new RangeError("Live poll interval must be a positive integer");
    this.#limit = Math.floor((governor.capBytes - governor.reserveBytes) / 4);
    const entries = this.#entries;
    this.#cache = governor.registerCache({
      get evictableBytes() {
        let bytes = 0;
        for (const entry of entries) if (entry.waiting === null) bytes += entry.bytes;
        return bytes;
      },
      evict: (wanted) => {
        let freed = 0;
        for (const entry of this.#entries) {
          if (entry.waiting !== null) continue;
          freed += entry.bytes;
          this.#releaseBaseline(entry);
          if (freed >= wanted) break;
        }
        return freed;
      },
    });
    try {
      this.#charge(512);
      this.#seen = feed.probe();
      this.#stop = feed.observe((event) => this.#invalidate(event));
    } catch (error) {
      if (this.#used > 0) this.#free(this.#used);
      this.#cache.unregister?.();
      throw error;
    }
    this.#poll = setInterval(() => {
      if (this.#seen !== feed.probe()) {
        this.#seen = feed.probe();
        for (const entry of this.#entries) entry.dirty = true;
      }
      const now = Date.now();
      for (const entry of this.#entries) {
        if (entry.waiting !== null && now - entry.sentAt >= LIVE_UNACKED_DROP_MS) {
          this.#closeEntry(entry);
          try {
            entry.ended();
          } catch {
            /* Consumer isolation. */
          }
        }
      }
      this.#schedule();
    }, options.pollIntervalMs ?? 1000);
    (this.#poll as unknown as { unref?: () => void }).unref?.();
  }
  get stats() {
    let dirty = 0,
      running = 0,
      waiting = 0;
    for (const entry of this.#entries) {
      dirty += Number(entry.dirty);
      running += Number(entry.running);
      waiting += Number(entry.waiting !== null);
    }
    return {
      subscriptions: this.#entries.size,
      dirty,
      running,
      waiting,
      bytes: this.#used,
    };
  }
  get retainedBytes(): number {
    return this.#used;
  }
  get subscriptionCount(): number {
    return this.#entries.size;
  }
  #charge(bytes: number): void {
    if (this.#used + bytes > this.#limit || !this.#cache.charge(bytes))
      throw new MemoryAdmissionError("live retained result", bytes, this.#limit - this.#used);
    this.#used += bytes;
  }
  #free(bytes: number): void {
    this.#used -= bytes;
    this.#cache.release(bytes);
  }
  subscribe(
    sql: string,
    params: readonly WireValue[],
    deliver: Entry["deliver"],
    error: Entry["error"],
    ended: Entry["ended"],
  ): LiveHandle {
    if (this.#closed) throw new Error("Live runtime is closed");
    const metadata = 2048 + stringBytes(sql) + valueBytes(params);
    this.#charge(metadata);
    let entry: Entry;
    try {
      entry = {
        sql,
        params: structuredClone(params),
        deliver,
        error,
        ended,
        metadata,
        abort: new AbortController(),
        tables: [],
        dependencyBytes: 0,
        retired: false,
        ready: null,
        dirty: true,
        closed: false,
        running: false,
        waiting: null,
        sentAt: 0,
        acknowledgedLsn: null,
        retained: null,
        bytes: 0,
        stage: null,
        stageBytes: 0,
        cloneBytes: 0,
        failure: undefined,
        failed: false,
        session: null,
        plan: null,
      };
      this.#entries.add(entry);
    } catch (failure) {
      this.#free(metadata);
      throw failure;
    }
    this.#schedule();
    return {
      ack: (version, accepted = true) => {
        if (entry.waiting === version) {
          if (accepted) entry.acknowledgedLsn = entry.retained?.lsn ?? entry.acknowledgedLsn;
          entry.waiting = null;
          const cloneBytes = entry.cloneBytes;
          entry.cloneBytes = 0;
          // Failed decode is not a seen snapshot. Never suppress its retry against this wire image.
          if (!accepted) this.#releaseBaseline(entry);
          this.#free(cloneBytes);
          this.#schedule();
        }
      },
      refresh: () => {
        entry.dirty = true;
        this.#schedule();
      },
      close: () => this.#closeEntry(entry),
    };
  }
  #invalidate(event: CommittedChange): void {
    if (event.lsn === this.#seen) return;
    const missed = event.previousLsn !== this.#seen;
    this.#seen = event.lsn;
    if (missed || event.catalog || event.replacement || event.unknown) {
      for (const entry of this.#entries) entry.dirty = true;
    } else {
      for (const entry of this.#entries)
        if (entry.tables.length === 0 && entry.retained === null) entry.dirty = true;
      for (let i = 0; i < event.tableCount; i++) {
        for (const entry of this.#byTable.get(event.tables[i] ?? -1) ?? []) entry.dirty = true;
      }
    }
    this.#schedule();
  }
  #schedule(): void {
    if (this.#closed || this.#sweeping || this.#timer !== null) return;
    if (![...this.#entries].some((entry) => entry.dirty && entry.waiting === null)) return;
    this.#timer = setTimeout(() => {
      this.#timer = null;
      const work = this.#sweep();
      this.#work = work;
      void work.finally(() => {
        if (this.#work === work) this.#work = null;
      });
    }, 0);
  }
  #dependencies(entry: Entry, plan: PlannedStatement): void {
    // Keep the old index intact if admission refuses the replacement.
    const tables = this.engine.readSet(plan);
    const bytes = 256 * tables.length;
    this.#charge(bytes);
    for (const table of entry.tables) {
      const bucket = this.#byTable.get(table);
      bucket?.delete(entry);
      if (bucket?.size === 0) this.#byTable.delete(table);
    }
    // Charge the complete new dependency index before allocating, including overlap.
    try {
      entry.tables = [...tables];
      for (const table of entry.tables) {
        let bucket = this.#byTable.get(table);
        if (bucket === undefined) {
          bucket = new Set();
          this.#byTable.set(table, bucket);
        }
        bucket.add(entry);
      }
    } catch (error) {
      this.#free(bytes);
      throw error;
    }
    this.#free(entry.dependencyBytes);
    entry.dependencyBytes = bytes;
  }
  async #sweep(): Promise<void> {
    this.#sweeping = true;
    // Per-entry metadata prepays this bounded cohort and eight worker continuations.
    const cohort = [...this.#entries].filter((entry) => entry.dirty && entry.waiting === null);
    const lsn = this.feed.probe();
    for (const entry of cohort) {
      entry.dirty = false;
      entry.running = true;
    }
    try {
      const durable = this.engine.durable(lsn);
      if (durable !== null) await durable;
      if (this.#closed) return;
      // No await between these pins: every session captures the same committed state.
      for (const entry of cohort) {
        if (entry.closed) continue;

        try {
          entry.session = this.engine.openSession("live", 0, `live:${String(++this.#id)}`);
          entry.ready = entry.session.begin(true, entry.abort.signal);
          void entry.ready.catch(() => undefined);
          entry.plan = entry.session.plan(entry.sql);
          if (entry.plan.kind !== "query") throw new TypeError("Live queries require SELECT");
          this.#dependencies(entry, entry.plan);
        } catch (error) {
          entry.failure = error;
          entry.failed = true;
        }
      }
      let next = 0;
      const worker = async () => {
        for (;;) {
          const entry = cohort[next++];
          if (entry === undefined) return;
          if (!entry.closed && !entry.failed) await this.#execute(entry, lsn);
        }
      };
      await Promise.all(Array.from({ length: Math.min(8, cohort.length) }, worker));
      // All affected queries publish in one synchronous turn.
      for (const entry of cohort) {
        if (entry.closed) continue;
        entry.running = false;
        if (entry.failed) {
          entry.failed = false;
          try {
            entry.error(entry.failure);
          } catch {
            /* Consumer isolation. */
          }
          entry.failure = undefined;
          continue;
        }
        const result = entry.stage;
        if (result === null) continue;
        if (
          entry.retained !== null &&
          equal(entry.retained.columns, result.columns) &&
          equal(entry.retained.frames, result.frames) &&
          result.rekeys.length === 0 &&
          !result.reset
        ) {
          this.#free(entry.stageBytes);
          entry.stageBytes = 0;
          entry.stage = null;
          continue;
        }
        try {
          const cloneBytes = valueBytes(result);
          this.#charge(cloneBytes);
          entry.cloneBytes = cloneBytes;
        } catch (error) {
          this.#free(entry.stageBytes);
          entry.stageBytes = 0;
          entry.stage = null;
          try {
            entry.error(error);
          } catch {
            /* Consumer isolation. */
          }
          continue;
        }
        this.#releaseBaseline(entry);
        entry.retained = result;
        entry.bytes = entry.stageBytes;
        entry.stage = null;
        entry.stageBytes = 0;
        entry.waiting = result.version;
        entry.sentAt = Date.now();
        try {
          entry.deliver(result);
        } catch (error) {
          entry.waiting = null;
          const cloneBytes = entry.cloneBytes;
          entry.cloneBytes = 0;
          this.#releaseBaseline(entry);
          this.#free(cloneBytes);
          try {
            entry.error(error);
          } catch {
            /* Consumer isolation. */
          }
        }
      }
    } catch (error) {
      for (const entry of cohort)
        if (!entry.closed) {
          try {
            entry.error(error);
          } catch {
            /* Consumer isolation. */
          }
        }
    } finally {
      for (const entry of cohort) {
        entry.session?.close();
        entry.session = null;
        entry.plan = null;
        entry.running = false;
        if (entry.stageBytes > 0) {
          this.#free(entry.stageBytes);
          entry.stageBytes = 0;
          entry.stage = null;
        }
        if (entry.closed) this.#retire(entry);
      }
      this.#sweeping = false;
      this.#schedule();
    }
  }
  async #execute(entry: Entry, lsn: number): Promise<void> {
    const session = entry.session,
      plan = entry.plan;
    if (session === null || plan === null) return;
    try {
      await entry.ready;
    } catch (error) {
      entry.failure = error;
      entry.failed = true;
      return;
    }
    let run;
    try {
      run = session.run(plan, entry.params, {
        signal: entry.abort.signal,
        request: null,
        windowBytes: 64 * 1024,
        frameRows: null,
      });
    } catch (error) {
      entry.failure = error;
      entry.failed = true;
      return;
    }
    // Observe the rejection while draining the stream; it may fail before next().
    void run.done.catch(() => undefined);
    void run.cleaned?.catch(() => undefined);
    const frames: FramePayload[] = [];
    let bytes = 0;
    try {
      for (;;) {
        const frame = await run.results.next();
        if (frame === null) break;
        try {
          const durable = this.engine.durable(frame.requiresLsn);
          if (durable !== null) await durable;
          entry.abort.signal.throwIfAborted();
          const adopted = frame.adopt?.((source, charge) => {
            if (
              this.#used + charge > this.#limit ||
              this.#cache.takeFrom?.(source, charge) !== true
            )
              return false;
            this.#used += charge;
            return true;
          });
          if (adopted === undefined || adopted === null)
            throw new MemoryAdmissionError(
              "live frame adoption",
              frame.byteLength,
              this.#limit - this.#used,
            );
          bytes += adopted;
          frames.push(frame.payload);
        } finally {
          frame.release();
        }
      }
      await run.done;
      await run.cleaned;
      const historyCharge = 2048 + 2 * this.feed.historyBytes;
      this.#charge(historyCharge);
      bytes += historyCharge;
      const history = this.feed.rekeysSince(
        entry.acknowledgedLsn ?? -1,
        session.snapshotLsn ?? lsn,
      );
      entry.stage = {
        version: ++this.#version,
        lsn: session.snapshotLsn ?? lsn,
        ...history,
        columns: plan.columns,
        frames,
      };
      entry.stageBytes = bytes;
    } catch (error) {
      run.results.cancel(error);
      await run.done.catch(() => undefined);
      await run.cleaned?.catch(() => undefined);
      this.#free(bytes);
      entry.failure = error;
      entry.failed = true;
    }
  }
  #releaseBaseline(entry: Entry): void {
    entry.retained = null;
    const bytes = entry.bytes;
    entry.bytes = 0;
    this.#free(bytes);
  }
  #retire(entry: Entry): void {
    if (entry.retired) return;
    entry.retired = true;
    this.#releaseBaseline(entry);
    this.#free(entry.cloneBytes);
    entry.cloneBytes = 0;
    this.#free(entry.dependencyBytes);
    entry.dependencyBytes = 0;
    entry.stage = null;
    this.#free(entry.stageBytes);
    entry.stageBytes = 0;
    this.#free(entry.metadata);
  }
  #closeEntry(entry: Entry): void {
    if (entry.closed) return;
    entry.closed = true;
    this.#entries.delete(entry);
    for (const table of entry.tables) {
      const bucket = this.#byTable.get(table);
      bucket?.delete(entry);
      if (bucket?.size === 0) this.#byTable.delete(table);
    }
    entry.abort.abort(new Error("Live query closed"));
    if (!entry.running) this.#retire(entry);
  }
  close(): Promise<void> {
    if (this.#closed) return this.#work ?? Promise.resolve();
    this.#closed = true;
    if (this.#timer !== null) clearTimeout(this.#timer);
    clearInterval(this.#poll);
    this.#stop();
    for (const entry of this.#entries) this.#closeEntry(entry);
    this.#free(512);
    // Running cleanup still owns cache charges; unregister only removes eviction callbacks.
    this.#cache.unregister?.();
    return this.#work ?? Promise.resolve();
  }
}
