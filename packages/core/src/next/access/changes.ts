import type { PhysicalType } from "../types.js";
/** Native publication feed. Events are borrowed until the listener returns; never retain them.
 * A missed event is recovered with probe(): any gap requires global invalidation. The fixed
 * table frontier deliberately widens to unknown instead of allocating transaction-sized state.
 */
import type { ByteBudget } from "../storage/extents.js";
import { MemoryAdmissionError } from "../memory/governor.js";

/** One bounded accepted primary-key UPDATE, in commit/statement/row order. Columns use the
 * existing lossless native column encoding; the facade decodes them with its normal decoder. */
export interface RekeyEffect {
  lsn: number;
  readonly tableId: number;
  readonly fromRowId: number;
  readonly toRowId: number;
  readonly keyChanged: boolean;
  readonly kind: "update" | "delete";
  readonly table: string;
  readonly columns: ReadonlyArray<{ readonly name: string; readonly type: PhysicalType }>;
  readonly from: readonly Uint8Array[];
  readonly to: readonly Uint8Array[];
  readonly bytes: number;
}
export const REKEY_HISTORY_LIMIT = 64;
export const REKEY_HISTORY_BYTES = 256 * 1024;

export interface CommittedChange {
  /** Previous publication in this feed, excluding sequence/log-fragment LSN gaps. */
  readonly previousLsn: number;
  readonly lsn: number;
  readonly tables: readonly number[];
  readonly tableCount: number;
  readonly catalog: boolean;
  readonly replacement: boolean;
  /** Globally dirty, never an empty set of effects. */
  readonly unknown: boolean;
}

// Includes two extra fixed frontiers/events and the single deferred dispatch task.
export const CHANGE_FEED_BYTES = 12288;
export const CHANGE_LISTENER_BYTES = 1024;
export const CHANGE_TABLE_LIMIT = 64;

interface ObserverCell {
  readonly epoch: number;
  readonly listener: (change: CommittedChange) => void;
}

/** Single-writer preparation and allocation-free publication. Not a persistent log. */
export class ChangeFeed {
  readonly #listeners = new Set<ObserverCell>();
  #observerEpoch = 0;
  readonly #tables: number[] = [];
  readonly #event = {
    lsn: 0,
    previousLsn: 0,
    tables: this.#tables,
    tableCount: 0,
    catalog: false,
    replacement: false,
    unknown: false,
  };
  readonly #queuedTables: number[] = [];
  readonly #dispatchTables: number[] = [];
  readonly #queued = {
    lsn: 0,
    previousLsn: 0,
    tables: this.#queuedTables,
    tableCount: 0,
    catalog: false,
    replacement: false,
    unknown: false,
  };
  readonly #dispatch = {
    lsn: 0,
    previousLsn: 0,
    tables: this.#dispatchTables,
    tableCount: 0,
    catalog: false,
    replacement: false,
    unknown: false,
  };
  #queuedPublication = false;
  #dispatching = false;
  #timer: ReturnType<typeof setTimeout> | null = null;
  readonly #deliver = () => {
    this.#timer = null;
    if (this.#closed || !this.#queuedPublication) return;
    this.#copy(this.#queued, this.#dispatch);
    this.#queuedPublication = false;
    this.#dispatching = true;
    // New registrations cannot join this borrowed event, including replacement callbacks.
    const through = this.#observerEpoch;
    try {
      for (const cell of this.#listeners) {
        // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition -- a preceding observer can close the feed reentrantly.
        if (cell.epoch > through || this.#closed) break;
        try {
          cell.listener(this.#dispatch);
        } catch {
          /* Consumer-owned error. */
        }
      }
    } finally {
      this.#dispatching = false;
      // A reentrant close leaves this borrowed dispatch storage funded until callout returns.
      // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition -- consumer callout can close the feed.
      if (this.#closed) this.budget.free(CHANGE_FEED_BYTES);
      else this.#schedule();
    }
  };
  readonly #history: Array<RekeyEffect | null> = Array.from(
    { length: REKEY_HISTORY_LIMIT },
    () => null,
  );
  readonly #pending: Array<RekeyEffect | null> = Array.from(
    { length: REKEY_HISTORY_LIMIT },
    () => null,
  );
  #historyCount = 0;
  #historyBytes = 0;
  #pendingCount = 0;
  #pendingBytes = 0;
  #rekeyReset = false;
  #floor = 0;
  #prepared: unknown = null;
  #closed = false;
  static create(budget: ByteBudget, lsn: number): ChangeFeed {
    if (!budget.allocate(CHANGE_FEED_BYTES))
      throw new MemoryAdmissionError("live publication", CHANGE_FEED_BYTES, 0);
    return new ChangeFeed(budget, lsn);
  }
  private constructor(
    readonly budget: ByteBudget,
    lsn: number,
  ) {
    this.#event.lsn = lsn;
    this.#event.previousLsn = lsn;
    this.#floor = lsn;
    // Preallocate array capacity before publication, including its maximum backing storage.
    for (let i = 0; i < CHANGE_TABLE_LIMIT; i++) {
      this.#tables.push(0);
      this.#queuedTables.push(0);
      this.#dispatchTables.push(0);
    }
    this.#event.tableCount = 0;
  }
  /** Compare with the last consumed LSN. A mismatch without its event means globally dirty. */
  probe(): number {
    return this.#event.lsn;
  }
  observe(listener: (change: CommittedChange) => void): () => void {
    if (this.#closed) throw new Error("Publication feed is closed");
    if (this.#observerEpoch === Number.MAX_SAFE_INTEGER)
      throw new RangeError("Live observer registration epoch exhausted");
    // Each registration owns an independent cell even when callbacks are identical.
    // Its existing reservation includes the epoch, Set membership and unsubscribe closure.
    if (!this.budget.allocate(CHANGE_LISTENER_BYTES))
      throw new MemoryAdmissionError("live observer", CHANGE_LISTENER_BYTES, 0);
    const cell: ObserverCell = { epoch: ++this.#observerEpoch, listener };
    this.#listeners.add(cell);
    return () => {
      if (this.#listeners.delete(cell)) this.budget.free(CHANGE_LISTENER_BYTES);
    };
  }
  /** Surviving undo only. Unknown future kinds conservatively widen, never disappear. */
  prepare(
    owner: unknown,
    undo: Iterable<{
      readonly kind: string;
      readonly tableId?: number;
      readonly liveRekey?: RekeyEffect | "reset" | null;
    }>,
  ): void {
    this.#dropPending();
    this.#rekeyReset = false;
    this.#prepared = owner;
    this.#event.tableCount = 0;
    this.#event.catalog = this.#event.replacement = this.#event.unknown = false;
    for (const entry of undo) {
      if ((entry.kind === "memDelete" || entry.kind === "groupDelete") && entry.liveRekey == null)
        this.#rekeyReset = true;
      if (entry.liveRekey === "reset") this.#rekeyReset = true;
      else if (entry.liveRekey) this.#prepareRekey(entry.liveRekey);
      switch (entry.kind) {
        case "rows":
        case "memDelete":
        case "groupDelete":
          if (entry.tableId === undefined) this.#event.unknown = true;
          else {
            let found = false;
            for (let i = 0; i < this.#event.tableCount; i++)
              if (this.#tables[i] === entry.tableId) found = true;
            if (!found) {
              if (this.#event.tableCount === CHANGE_TABLE_LIMIT) this.#event.unknown = true;
              else this.#tables[this.#event.tableCount++] = entry.tableId;
            }
          }
          break;
        case "catalog":
          this.#event.catalog = true;
          // Names and primary-key definitions can change; old identity proofs cannot cross DDL.
          this.#rekeyReset = true;
          break;
        case "delta":
        case "staged":
        case "freeze":
        case "indexDelta":
          break;
        default:
          this.#event.unknown = true;
          this.#rekeyReset = true;
      }
    }
  }
  /** Only field writes: no allocation, IO, observer callout or validation after publication. */
  install(owner: unknown, lsn: number): void {
    if (this.#prepared !== owner) {
      this.#event.tableCount = 0;
      this.#event.unknown = true;
      this.#event.catalog = true;
      this.#rekeyReset = true;
    }
    if (this.#rekeyReset) this.#floor = lsn;
    for (let i = 0; i < this.#pendingCount; i++) {
      const effect = this.#pending[i];
      if (effect !== null && effect !== undefined) {
        effect.lsn = lsn;
        this.#history[this.#historyCount++] = effect;
        this.#pending[i] = null;
      }
    }
    this.#historyBytes += this.#pendingBytes;
    this.#pendingBytes = this.#pendingCount = 0;
    this.#event.previousLsn = this.#event.lsn;
    this.#event.lsn = lsn;
    this.#prepared = null;
  }
  get historyBytes(): number {
    return this.#historyBytes;
  }
  /** Caller funds the bounded returned descriptor array before calling. */
  rekeysSince(after: number, through: number): { reset: boolean; rekeys: readonly RekeyEffect[] } {
    if (after < this.#floor) return { reset: true, rekeys: [] };
    const rekeys: RekeyEffect[] = [];
    for (let i = 0; i < this.#historyCount; i++) {
      const effect = this.#history[i];
      if (effect && effect.lsn > after && effect.lsn <= through) rekeys.push(effect);
    }
    return { reset: false, rekeys };
  }
  #dropPending(): void {
    for (let i = 0; i < this.#pendingCount; i++) this.#pending[i] = null;
    this.budget.free(this.#pendingBytes);
    this.#pendingBytes = this.#pendingCount = 0;
  }
  #dropHistory(): void {
    for (let i = 0; i < this.#historyCount; i++) this.#history[i] = null;
    this.budget.free(this.#historyBytes);
    this.#historyBytes = this.#historyCount = 0;
  }
  #prepareRekey(effect: RekeyEffect): void {
    if (
      this.#pendingCount >= REKEY_HISTORY_LIMIT ||
      this.#pendingBytes + effect.bytes > REKEY_HISTORY_BYTES
    ) {
      this.#rekeyReset = true;
      return;
    }
    if (
      this.#historyCount + this.#pendingCount >= REKEY_HISTORY_LIMIT ||
      this.#historyBytes + this.#pendingBytes + effect.bytes > REKEY_HISTORY_BYTES
    ) {
      this.#floor = this.#event.lsn;
      this.#dropHistory();
    }
    try {
      if (!this.budget.allocate(effect.bytes)) {
        this.#rekeyReset = true;
        return;
      }
    } catch {
      this.#rekeyReset = true;
      return;
    }
    this.#pending[this.#pendingCount++] = effect;
    this.#pendingBytes += effect.bytes;
  }
  #copy(
    from: CommittedChange,
    into: {
      lsn: number;
      previousLsn: number;
      tables: number[];
      tableCount: number;
      catalog: boolean;
      replacement: boolean;
      unknown: boolean;
    },
  ): void {
    into.lsn = from.lsn;
    into.previousLsn = from.previousLsn;
    into.tableCount = from.tableCount;
    into.catalog = from.catalog;
    into.replacement = from.replacement;
    into.unknown = from.unknown;
    for (let i = 0; i < from.tableCount; i++) into.tables[i] = from.tables[i] ?? 0;
  }
  #schedule(): void {
    if (this.#closed || !this.#queuedPublication || this.#timer !== null || this.#dispatching)
      return;
    // A scheduling failure cannot cross the outcome boundary. probe() still recovers it.
    try {
      this.#timer = setTimeout(this.#deliver, 0);
    } catch {
      /* Poll recovers the hint. */
    }
  }
  /** After bookkeeping and writer release. One funded task coalesces any publication burst. */
  notify(): void {
    if (this.#closed) return;
    if (!this.#queuedPublication) this.#copy(this.#event, this.#queued);
    else {
      const into = this.#queued,
        from = this.#event;
      into.lsn = from.lsn;
      into.catalog ||= from.catalog;
      into.replacement ||= from.replacement;
      into.unknown ||= from.unknown;
      for (let i = 0; i < from.tableCount; i++) {
        const id = from.tables[i] ?? 0;
        let found = false;
        for (let j = 0; j < into.tableCount; j++) if (into.tables[j] === id) found = true;
        if (!found) {
          if (into.tableCount === CHANGE_TABLE_LIMIT) into.unknown = true;
          else into.tables[into.tableCount++] = id;
        }
      }
    }
    this.#queuedPublication = true;
    this.#schedule();
  }
  /** Offline replacement owners call after their single root publication has succeeded. */
  replace(lsn: number): void {
    if (this.#closed) return;
    this.#dropPending();
    this.#dropHistory();
    this.#floor = lsn;
    this.#event.tableCount = 0;
    this.#event.catalog = this.#event.replacement = this.#event.unknown = true;
    this.#event.previousLsn = this.#event.lsn;
    this.#event.lsn = lsn;
    this.#prepared = null;
    this.notify();
  }
  close(): void {
    if (this.#closed) return;
    this.#closed = true;
    if (this.#timer !== null) clearTimeout(this.#timer);
    this.#timer = null;
    this.#queuedPublication = false;
    this.#dropPending();
    this.#dropHistory();
    this.budget.free(CHANGE_LISTENER_BYTES * this.#listeners.size);
    if (!this.#dispatching) this.budget.free(CHANGE_FEED_BYTES);
    this.#listeners.clear();
    this.#event.tableCount = 0;
  }
}
