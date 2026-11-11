/** Owned, compact result frames. Borrowed input is copied; authorized dedicated input stays charged through frame credit. */
import type { MaybePromise } from "../access/key-index.js";
import { OBJECT_OVERHEAD_BYTES, bigintBytes, type Grant, type Loan } from "../memory/governor.js";
import { type PhysicalType } from "../types.js";
import { BATCH_ROWS, type Batch, type FlatVector } from "../vector/vector.js";
import {
  ResultDictionaries,
  type DictionaryPlan,
  type DictionaryFrame,
} from "./result-dictionaries.js";
import { ownedResultColumns } from "./result-owned.js";
import { ColumnStore } from "./column-store.js";
import { slotOf, validAt } from "./kernels/vectors.js";
import {
  abortable,
  afterYield,
  clockFor,
  yieldTask,
  YIELD_INTERVAL_MS,
  type TaskClock,
  type Operator,
  type PushResult,
  type Transfer,
} from "./pipeline.js";
import {
  frameByteLength,
  frameTransferList,
  RESULT_FRAME_TARGET_BYTES,
  ResultChannel,
  type WireVector,
} from "./result.js";

interface FrameStore extends DictionaryFrame {
  readonly store: ColumnStore;
  columns: WireVector[] | null;
  metadata: boolean;
  loan: Loan | null;
  freed: boolean;
  returned: (() => void) | null;
}

export interface ResultSinkOptions {
  readonly deferEnd?: boolean;
  readonly maxRows?: number | null;
  /** Test clock only; production uses the worker's R23.5 clock, without starting a slice. */
  readonly now?: () => number;
}

export class ResultSink implements Operator {
  readonly reclassifiesTransfers = true;
  readonly deferEnd: boolean;
  readonly #maxRows: number | null;
  readonly #clock: TaskClock;
  #zeroSent = 0;
  readonly #dictionaries: ResultDictionaries;
  #dictionaryPlan: DictionaryPlan | null = null;
  readonly #abort = new AbortController();
  readonly #frames = new Set<FrameStore>();
  readonly #creditWaiters = new Set<() => void>();
  readonly #metadataBytes: number;
  readonly #rowsPerFrame: number;
  #target: number;
  #batch: Batch | null = null;
  #transfer: Transfer | null = null;
  #at = 0;
  #waiting: Promise<void> | null = null;
  #pending: FrameStore | null = null;
  #rows = 0;
  #finished = false;
  #budgetSuspended = false;

  constructor(
    readonly types: readonly PhysicalType[],
    readonly grant: Grant,
    readonly channel: ResultChannel,
    readonly requiresLsn: number,
    options: ResultSinkOptions = {},
  ) {
    this.#dictionaries = new ResultDictionaries(grant);
    this.deferEnd = options.deferEnd ?? false;
    this.#maxRows = options.maxRows ?? null;
    this.#clock = clockFor(options.now);
    if (this.#maxRows !== null && (!Number.isSafeInteger(this.#maxRows) || this.#maxRows <= 0))
      throw new TypeError("positive result frame row limit required");
    this.#metadataBytes = OBJECT_OVERHEAD_BYTES * (3 + 2 * types.length);
    this.#target = Math.min(RESULT_FRAME_TARGET_BYTES, channel.windowBytes);
    let rows = 1;
    const maximum = Math.min(BATCH_ROWS, this.#maxRows ?? BATCH_ROWS);
    while (2 * rows <= maximum) rows *= 2;
    for (;;) {
      const probe = new ColumnStore(types, grant, { chunkRows: rows, varlenBytes: 64 });
      const fits = probe.fixedBytes(1) <= this.#target;
      probe.close();
      if (fits || rows === 1) break;
      rows /= 2;
    }
    this.#rowsPerFrame = rows;
  }

  /** A direct terminal scan can amortize wide-column allocation with a larger owned batch.
   * Configure before input; the existing credit window is a hard bound. Copying fallbacks keep
   * their conservative row capacity. Nothing changes for other producers/consumer pipelines.
   */
  terminalBatchTarget(bytes: number): number {
    this.#check();
    if (!Number.isSafeInteger(bytes) || bytes <= 0)
      throw new TypeError("Positive terminal batch target required");
    if (this.#batch !== null || this.#rows !== 0 || this.#frames.size !== 0)
      throw new TypeError("Terminal batch target must precede input");
    this.#target = Math.max(this.#target, Math.min(bytes, this.channel.windowBytes));
    return this.#target;
  }

  /** Stable per-stream admission used by a direct terminal source before output reservation. */
  admitDictionary(dictionary: FlatVector): boolean {
    this.#check();
    return this.#dictionaries.admit(dictionary) !== null;
  }

  get rowCount(): number {
    return this.#rows;
  }

  /** A producer phase has stopped. Return every offered frame before moving its minimum;
   * consumer credit is external progress, never another execution's memory admission.
   * Published dictionary IDs are not reused after the registry releases its source objects.
   */
  async suspendBudget(): Promise<void> {
    this.#check();
    if (this.#budgetSuspended || this.#finished) throw new TypeError("Result sink cannot suspend");
    if (this.#batch !== null || this.#waiting !== null)
      throw new TypeError("Result handoff requires a ready producer");
    this.#budgetSuspended = true;
    await this.ready();
    while (this.#frames.size > 0) {
      this.#check();
      await abortable(
        new Promise<void>((resolve) => this.#creditWaiters.add(resolve)),
        this.#abort.signal,
      );
    }
    this.#check();
    this.#dictionaries.close();
    if (this.grant.allocated !== 0 || this.grant.loaned !== 0)
      throw new TypeError("Result budget retained buffers at handoff");
  }

  /** The runtime has atomically installed the next phase's native budget. */
  resumeBudget(): void {
    this.#check();
    if (!this.#budgetSuspended || this.grant.held === 0)
      throw new TypeError("Result budget is not admitted");
    this.#budgetSuspended = false;
  }

  push(batch: Batch, transfer?: Transfer): PushResult {
    try {
      this.#check();
      if (this.#budgetSuspended) throw new TypeError("Result input arrived between phases");
      if (this.#batch !== null) throw new TypeError("push before result readiness");
      if (batch.columns.length !== this.types.length)
        throw new TypeError("result columns differ from the plan");
    } catch (error) {
      transfer?.release();
      throw error;
    }
    const count = batch.selection?.count ?? batch.length;
    if (this.types.length === 0 || count === 0) {
      this.#rows += count;
      transfer?.release();
      return "ready";
    }
    this.#batch = batch;
    this.#transfer = transfer ?? null;
    this.#at = 0;
    try {
      this.#dictionaryPlan = this.#dictionaries.plan(this.types, batch.columns);
      return this.#drain();
    } catch (error) {
      this.abort(error);
      throw error;
    }
  }

  async ready(): Promise<void> {
    try {
      for (;;) {
        const waiting = this.#waiting;
        this.#waiting = null;
        if (waiting !== null) await abortable(waiting, this.#abort.signal);
        this.#check();
        if (this.#hasWaiting()) continue;
        if (this.#batch === null || this.#drain() === "ready") return;
      }
    } catch (error) {
      this.abort(error);
      throw error;
    }
  }

  finish(): MaybePromise<void> {
    this.#check();
    if (this.#budgetSuspended) throw new TypeError("Result finish arrived between phases");
    if (this.#batch !== null) throw new TypeError("finish before result readiness");
    // Zero-column rows have no payload bytes, hence no byte-window pressure. Coalesce their
    // count instead of queuing an unbounded number of zero-byte frames.
    if (this.types.length === 0 && this.#rows > 0) return this.#emptyFrames();
    this.#finishFrames();
  }

  #emptyFrames(): MaybePromise<void> {
    while (this.#zeroSent < this.#rows) {
      this.#check();
      if (this.#zeroSent > 0 && this.#clock.due(YIELD_INTERVAL_MS))
        return this.#yield().then(() => this.#emptyFrames());
      const rows = Math.min(this.#rows - this.#zeroSent, this.#maxRows ?? this.#rows);
      const frame = this.#make(1);
      if (frame.metadata) {
        this.#offer(frame, rows);
        this.#zeroSent += rows;
        continue;
      }
      if (this.#frames.size > 0) {
        this.#free(frame);
        return new Promise<void>((resolve) => this.#creditWaiters.add(resolve)).then(() =>
          this.#emptyFrames(),
        );
      }
      this.#pending = frame;
      return this.grant.borrow(this.#metadataBytes, this.#abort.signal).then((loan) => {
        if (this.#abort.signal.aborted) {
          if (this.grant.loaned > 0) loan.repay();
          this.#check();
        }
        frame.loan = loan;
        if (!this.grant.allocate(this.#metadataBytes))
          throw new Error("result metadata reservation failed");
        frame.metadata = true;
        this.#pending = null;
        const returned = new Promise<void>((resolve) => {
          frame.returned = resolve;
        });
        this.#offer(frame, rows);
        this.#zeroSent += rows;
        // Loan-backed metadata is returned on credit before asking for another loan.
        return returned.then(() => this.#emptyFrames());
      });
    }
    this.#finishFrames();
  }

  /** The execution ends delivery only after all pipeline cleanup succeeded. */
  complete(): void {
    this.#check();
    if (!this.#finished) throw new TypeError("pipeline did not finish its result sink");
    this.channel.end();
  }

  #finishFrames(): void {
    this.#dictionaries.close();
    this.#finished = true;
    if (!this.deferEnd) this.complete();
  }

  /** Stops production; already offered frames retain their credit until released or discarded. */
  abort(reason: unknown): void {
    if (!this.#abort.signal.aborted) this.#abort.abort(reason);
    this.#releaseInput();
    this.#dictionaries.close();
    for (const wake of this.#creditWaiters) wake();
    this.#creditWaiters.clear();
    if (this.#pending !== null) this.#free(this.#pending);
    this.#pending = null;
  }

  /** Execution cancellation discards queued and handed-out frame ownership alike. */
  discard(reason: unknown): void {
    this.abort(reason);
    for (const frame of this.#frames) this.#free(frame);
  }

  #make(rows: number, types = this.#dictionaryPlan?.types ?? this.types): FrameStore {
    const frame: FrameStore = {
      store: new ColumnStore(types, this.grant, {
        chunkRows: rows,
        varlenBytes: 64,
        // ColumnStore permits one oversized row even with a finite target. Keeping that
        // target makes its arena grow to the actual need, rather than doubling past reserve.
        targetBytes: this.#target,
      }),
      columns: null,
      dictionaries: [],
      extraBytes: 0,
      metadata: this.grant.allocate(this.#metadataBytes),
      loan: null,
      freed: false,
      returned: null,
    };
    this.#pending = frame;
    return frame;
  }

  #free(frame: FrameStore): void {
    if (frame.freed) return;
    frame.freed = true;
    if (this.#pending === frame) this.#pending = null;
    const offered = this.#frames.delete(frame);
    // close() uses retainedBytes, so it remains correct after transfer detached the buffers.
    frame.store.close();
    this.grant.free(frame.extraBytes);
    if (frame.metadata) this.grant.free(this.#metadataBytes);
    if (frame.loan !== null && this.grant.loaned > 0) frame.loan.repay();
    frame.returned?.();
    if (offered) {
      for (const wake of this.#creditWaiters) wake();
      this.#creditWaiters.clear();
    }
  }

  #offer(frame: FrameStore, rows: number): PushResult {
    if (this.#pending === frame) this.#pending = null;
    this.#frames.add(frame);
    try {
      const columns =
        frame.columns ??
        this.types.map((_, c) => {
          const { form, length, validity, data } = frame.store.chunk(c, 0);
          const flat = { form, length, validity, data };
          return this.#dictionaryPlan === null
            ? flat
            : this.#dictionaries.wire(this.#dictionaryPlan, flat, c);
        });
      if (this.#dictionaryPlan !== null) this.#dictionaries.publish(this.#dictionaryPlan, frame);
      return this.channel.offer({
        payload: {
          rowCount: rows,
          columns,
          dictionaries: frame.dictionaries,
        },
        requiresLsn: this.requiresLsn,
        free: () => this.#free(frame),
      });
    } catch (error) {
      this.#free(frame);
      throw error;
    }
  }

  /** One compact source batch, mixing authorized buffers with owned fallback copies.
   * The whole frame must fit the normal byte/row limits. Otherwise use the copying splitter.
   */
  #offerOwned(batch: Batch): PushResult | null {
    const plan = this.#dictionaryPlan;
    if (plan === null || (this.#maxRows !== null && batch.length > this.#maxRows)) return null;
    const columns = ownedResultColumns(batch, plan.ids, this.#transfer, this.grant);
    if (columns === null) return null;
    const copies = plan.columns.filter((_, c) => columns[c] === null);
    const types = plan.types.filter((_, c) => columns[c] === null);
    const frame = this.#make(2 ** Math.ceil(Math.log2(batch.length)), types);
    try {
      if (
        !frame.metadata ||
        !this.#dictionaries.stage(plan, frame) ||
        (copies.length > 0 && frame.store.append(copies, null, 0, batch.length) !== batch.length)
      ) {
        this.#free(frame);
        return null;
      }
      let copied = 0;
      frame.columns = columns.map((column, c) => {
        if (column !== null) return column;
        const { form, length, validity, data } = frame.store.chunk(copied++, 0);
        return this.#dictionaries.wire(plan, { form, length, validity, data }, c);
      });
      const bytes = frameByteLength({
        rowCount: batch.length,
        columns: frame.columns,
        dictionaries: frame.dictionaries,
      });
      if (bytes > this.#target && batch.length > 1) {
        this.#free(frame);
        return null;
      }
      // The source token also retains query dictionaries/readers. A wire frame has copied
      // those deltas and owns only dedicated arrays: reclassify their charge before returning
      // the source lifetime, so execution can close its query grant before frame credit.
      const adoptedBuffers = frameTransferList({
        rowCount: batch.length,
        columns: columns.filter((column): column is WireVector => column !== null),
        dictionaries: [],
      });
      const adoptedBytes = adoptedBuffers.reduce((sum, buffer) => sum + buffer.byteLength, 0);
      if (!this.grant.allocate(adoptedBytes)) {
        this.#free(frame);
        return null;
      }
      frame.extraBytes += adoptedBytes;
      this.#transfer?.reclassify?.(new Set(adoptedBuffers));
      this.#at = batch.length;
      this.#rows += batch.length;
      const result = this.#offer(frame, batch.length);
      this.#releaseInput();
      if (result === "blocked") this.#wait(this.channel.ready());
      return result;
    } catch (error) {
      this.#free(frame);
      throw error;
    }
  }

  #drain(): PushResult {
    const batch = this.#batch;
    if (batch === null) return "ready";
    if (this.#at === 0) {
      const owned = this.#offerOwned(batch);
      if (owned !== null) return owned;
    }
    const input = this.#dictionaryPlan?.columns ?? batch.columns;
    const stage = (frame: FrameStore) =>
      this.#dictionaryPlan === null || this.#dictionaries.stage(this.#dictionaryPlan, frame);
    const count = batch.selection?.count ?? batch.length;
    while (this.#at < count) {
      this.#check();
      if (this.#at > 0 && this.#clock.due(YIELD_INTERVAL_MS)) {
        this.#wait(this.#yield());
        return "blocked";
      }
      let capacity = count - this.#at === 1 ? 1 : this.#rowsPerFrame;
      let frame = this.#make(capacity);
      let copied =
        frame.metadata && stage(frame)
          ? frame.store.append(
              input,
              batch.selection?.rows ?? null,
              this.#at,
              Math.min(capacity, count - this.#at),
            )
          : 0;
      // The constructor's estimate covers fixed buffers. Several varlen arenas can each fit
      // the target while their sum exceeds it; seal by the whole retained payload, including
      // unused capacity. Retry without advancing the borrowed selection. Only one row may
      // exceed the target.
      while (
        (copied === 0 || frame.store.retainedBytes + frame.extraBytes > this.#target) &&
        capacity > 1
      ) {
        const maximum = copied === 0 ? capacity / 2 : Math.min(capacity / 2, copied);
        this.#free(frame);
        do capacity /= 2;
        while (capacity > maximum);
        frame = this.#make(capacity);
        copied =
          frame.metadata && stage(frame)
            ? frame.store.append(
                input,
                batch.selection?.rows ?? null,
                this.#at,
                Math.min(capacity, count - this.#at),
              )
            : 0;
      }
      if (copied === 0) {
        if (this.#frames.size > 0) {
          // At its admitted minimum, a result sink can own one frame. Return unused
          // construction scratch and wait for consumer credit before asking for another
          // bounded step; never retain one frame while seeking another execution's memory.
          this.#free(frame);
          this.#wait(new Promise<void>((resolve) => this.#creditWaiters.add(resolve)));
          return "blocked";
        }
        this.#pending = frame;
        const row = batch.selection?.rows[this.#at] ?? this.#at;
        let needed =
          frame.store.appendNeed(input, row) + (frame.metadata ? 0 : this.#metadataBytes);
        if (this.#dictionaryPlan !== null && frame.extraBytes === 0)
          needed += this.#dictionaries.needed(this.#dictionaryPlan);
        // An empty store may not yet have observed numeric input tiers. Allow fixed-width
        // promotions, and charge any wide decimal coefficient copied into its one row.
        needed += 32 * this.types.length;
        for (const vector of input) {
          const [source, slot] = slotOf(vector, row);
          const data = source.data;
          if (
            validAt(source.validity, slot) &&
            data.layout === "decimal" &&
            data.coefficients.repr === "wide"
          )
            needed += bigintBytes(data.coefficients.values[slot] ?? 0n);
        }
        this.#wait(
          this.grant.borrow(needed, this.#abort.signal).then((loan) => {
            if (this.#abort.signal.aborted) {
              if (this.grant.loaned > 0) loan.repay();
              this.#check();
            }
            frame.loan = loan;
            if (!frame.metadata) {
              if (!this.grant.allocate(this.#metadataBytes))
                throw new Error("result metadata reservation failed");
              frame.metadata = true;
            }
            if (frame.extraBytes === 0 && !stage(frame))
              throw new Error("result dictionary reservation failed");
            if (frame.store.append(input, batch.selection?.rows ?? null, this.#at, 1) !== 1)
              throw new Error("result reservation did not cover one row");
            this.#pending = null;
            this.#at++;
            this.#rows++;
            // A grant may own only one progress loan: do not start another frame until this one
            // returns its loan, even when its payload is smaller than the wire credit window.
            const returned = new Promise<void>((resolve) => {
              frame.returned = resolve;
            });
            this.#offer(frame, 1);
            this.#wait(returned);
          }),
        );
        return "blocked";
      }
      this.#at += copied;
      this.#rows += copied;
      if (this.#offer(frame, copied) === "blocked") {
        this.#wait(this.channel.ready());
        if (this.#at === count) this.#releaseInput();
        return "blocked";
      }
    }
    this.#releaseInput();
    return "ready";
  }

  #releaseInput(): void {
    this.#batch = null;
    if (this.#dictionaryPlan !== null) this.#dictionaries.abandon(this.#dictionaryPlan);
    this.#dictionaryPlan = null;
    const transfer = this.#transfer;
    this.#transfer = null;
    transfer?.release();
  }

  #check(): void {
    this.#abort.signal.throwIfAborted();
  }

  #hasWaiting(): boolean {
    return this.#waiting !== null;
  }

  #yield(): Promise<void> {
    return yieldTask().then(() => afterYield(this.#clock));
  }

  #wait(promise: Promise<void>): void {
    this.#waiting = promise;
    promise.catch(() => undefined);
  }
}
