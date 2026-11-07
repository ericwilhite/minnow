/** Owned, compact result frames. Input buffers never survive readiness or enter a wire frame. */
import type { MaybePromise } from "../access/key-index.js";
import { OBJECT_OVERHEAD_BYTES, bigintBytes, type Grant, type Loan } from "../memory/governor.js";
import type { PhysicalType } from "../types.js";
import { BATCH_ROWS, type Batch } from "../vector/vector.js";
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
import { RESULT_FRAME_TARGET_BYTES, ResultChannel } from "./result.js";

interface FrameStore {
  readonly store: ColumnStore;
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
  readonly deferEnd: boolean;
  readonly #maxRows: number | null;
  readonly #clock: TaskClock;
  #zeroSent = 0;
  readonly #abort = new AbortController();
  readonly #frames = new Set<FrameStore>();
  readonly #metadataBytes: number;
  readonly #rowsPerFrame: number;
  readonly #target: number;
  #batch: Batch | null = null;
  #transfer: Transfer | null = null;
  #at = 0;
  #waiting: Promise<void> | null = null;
  #pending: FrameStore | null = null;
  #rows = 0;
  #finished = false;

  constructor(
    readonly types: readonly PhysicalType[],
    readonly grant: Grant,
    readonly channel: ResultChannel,
    readonly requiresLsn: number,
    options: ResultSinkOptions = {},
  ) {
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

  get rowCount(): number {
    return this.#rows;
  }

  push(batch: Batch, transfer?: Transfer): PushResult {
    try {
      this.#check();
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
    this.#finished = true;
    if (!this.deferEnd) this.complete();
  }

  /** Stops production; already offered frames retain their credit until released or discarded. */
  abort(reason: unknown): void {
    if (!this.#abort.signal.aborted) this.#abort.abort(reason);
    this.#releaseInput();
    if (this.#pending !== null) this.#free(this.#pending);
    this.#pending = null;
  }

  /** Execution cancellation discards queued and handed-out frame ownership alike. */
  discard(reason: unknown): void {
    this.abort(reason);
    for (const frame of this.#frames) this.#free(frame);
  }

  #make(rows: number): FrameStore {
    const frame: FrameStore = {
      store: new ColumnStore(this.types, this.grant, {
        chunkRows: rows,
        varlenBytes: 64,
        // ColumnStore permits one oversized row even with a finite target. Keeping that
        // target makes its arena grow to the actual need, rather than doubling past reserve.
        targetBytes: this.#target,
      }),
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
    this.#frames.delete(frame);
    // close() uses retainedBytes, so it remains correct after transfer detached the buffers.
    frame.store.close();
    if (frame.metadata) this.grant.free(this.#metadataBytes);
    if (frame.loan !== null && this.grant.loaned > 0) frame.loan.repay();
    frame.returned?.();
  }

  #offer(frame: FrameStore, rows: number): PushResult {
    if (this.#pending === frame) this.#pending = null;
    this.#frames.add(frame);
    try {
      const columns = this.types.map((_, c) => {
        const { form, length, validity, data } = frame.store.chunk(c, 0);
        return { form, length, validity, data };
      });
      return this.channel.offer({
        payload: { rowCount: rows, columns, dictionaries: [] },
        requiresLsn: this.requiresLsn,
        free: () => this.#free(frame),
      });
    } catch (error) {
      this.#free(frame);
      throw error;
    }
  }

  #drain(): PushResult {
    const batch = this.#batch;
    if (batch === null) return "ready";
    const count = batch.selection?.count ?? batch.length;
    while (this.#at < count) {
      this.#check();
      if (this.#at > 0 && this.#clock.due(YIELD_INTERVAL_MS)) {
        this.#wait(this.#yield());
        return "blocked";
      }
      let capacity = count - this.#at === 1 ? 1 : this.#rowsPerFrame;
      let frame = this.#make(capacity);
      let copied = frame.metadata
        ? frame.store.append(
            batch.columns,
            batch.selection?.rows ?? null,
            this.#at,
            Math.min(capacity, count - this.#at),
          )
        : 0;
      // The constructor's estimate covers fixed buffers. Several varlen arenas can each fit
      // the target while their sum exceeds it; seal by the whole retained payload, including
      // unused capacity. Retry without advancing the borrowed selection. Only one row may
      // exceed the target.
      while ((copied === 0 || frame.store.retainedBytes > this.#target) && capacity > 1) {
        const maximum = copied === 0 ? capacity / 2 : Math.min(capacity / 2, copied);
        this.#free(frame);
        do capacity /= 2;
        while (capacity > maximum);
        frame = this.#make(capacity);
        copied = frame.metadata
          ? frame.store.append(
              batch.columns,
              batch.selection?.rows ?? null,
              this.#at,
              Math.min(capacity, count - this.#at),
            )
          : 0;
      }
      if (copied === 0) {
        this.#pending = frame;
        const row = batch.selection?.rows[this.#at] ?? this.#at;
        let needed =
          frame.store.appendNeed(batch.columns, row) + (frame.metadata ? 0 : this.#metadataBytes);
        // An empty store may not yet have observed numeric input tiers. Allow fixed-width
        // promotions, and charge any wide decimal coefficient copied into its one row.
        needed += 32 * this.types.length;
        for (const vector of batch.columns) {
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
            if (frame.store.append(batch.columns, batch.selection?.rows ?? null, this.#at, 1) !== 1)
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
