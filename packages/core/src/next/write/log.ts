/**
 * The log appender: group commit over a `StoreSession` log (access/write-path.ts "Commit",
 * format/log-frame.ts envelope and header slots).
 *
 * - `append(body)` is synchronous while its bounded queue fits, otherwise it waits for the
 *   storage drain (or a running streamed commit). It splits the body into parts of at most
 *   `MAX_FRAME_BODY_BYTES`, assigns contiguous LSNs, chains checksums, and queues the frames. The
 *   queued bytes reach `appendLog` in order on one promise chain.
 * - A flush takes every frame queued before it (a **group**), waits for their appends, writes the
 *   header slot not holding the newest header (generation + 1) acknowledging the previous durable
 *   group, and calls `flushLog` once. Frames queued meanwhile form the next group. Strict mode
 *   starts a flush as soon as the current task ends; relaxed mode within `RELAXED_DELAY_MS`.
 * - After the last flush, an idle flush writes a header acknowledging the newest group.
 *   Its target is `IDLE_FLUSH_MS` from the last append (commit), including the initial flush.
 *   The 45 ms timer budget starts at append; an initial flush that consumes that budget arms
 *   the idle timer immediately when it finishes. The 100 ms target assumes the initial flush,
 *   timer dispatch (up to ~55 ms under continuous execution), and acknowledging I/O fit that
 *   deadline. Slow storage or a suspended worker can exceed it; this is not an unconditional
 *   wall-clock guarantee. While commits continue each flush acknowledges the previous group.
 * - Any append or flush failure fails the appender: every waiter rejects with
 *   `WritePathFailedError` and later calls throw it.
 * - **Recycling** (access/write-path.ts "Log recycling"): `beginRecycle` fixes the cut (the last
 *   LSN appended); later appends keep their LSNs but are only kept as bodies (deferred). The
 *   engine makes everything through the cut durable (`flushEncoded`), publishes a root covering
 *   it with the new incarnation, resets the log, and calls `finishRecycle`, which starts the new
 *   incarnation empty at the cut and writes the deferred bodies as its first frames. A crash
 *   before then loses only deferred frames, which no caller waited for durably.
 *   `abandonRecycle` writes them into the old log instead.
 */
import { MAX_LSN } from "../access/snapshot.js";
import { WritePathFailedError } from "../access/write-path.js";
import {
  encodeFrame,
  frameLength,
  FRAME_HEADER_BYTES,
  LOG_HEADER_BYTES,
  encodeLogHeader,
  MAX_FRAME_BODY_BYTES,
  MAX_FRAME_PARTS,
} from "../format/log-frame.js";
import type { StoreSession } from "../storage/contract.js";
import { need } from "./bytes.js";
import type { MaybePromise } from "../access/key-index.js";
import { LOG_QUEUE_BYTES, COMMIT_BUFFER_OVERHEAD, type BufferAllocator } from "./commit-buffers.js";

/** Idle acknowledgement target from the last append, subject to scheduling and I/O latency. */
export const IDLE_FLUSH_MS = 100;
/** Timer budget from append, including initial flush time: leaves ~55 ms for dispatch and I/O. */
export const IDLE_ARM_MS = 45;
export const RELAXED_DELAY_MS = 5;

export interface LogPosition {
  readonly salt: number;
  /** Chain checksum at `end` (the next frame's seed). */
  readonly chain: number;
  /** Body length: where the next frame goes. */
  readonly end: number;
  /** LSN of the last frame (firstLsn − 1 when none). */
  readonly lastLsn: number;
  /** The header to write next: its generation and slot. */
  readonly generation: number;
  readonly nextSlot: 0 | 1;
  /** The newest boundary a header already acknowledges. */
  readonly acked: { readonly end: number; readonly chain: number; readonly lsn: number };
  readonly rootSequence: number;
}

interface Boundary {
  readonly end: number;
  readonly chain: number;
  readonly lsn: number;
}

/**
 * The log has used every committed LSN (`MAX_LSN`, 2^52 − 1): the transaction was not appended and
 * nothing changed. Unreachable in practice (a million commits a second for 142 years).
 */
export class LogExhaustedError extends Error {
  readonly code = "MINNOW_LOG_EXHAUSTED";
  constructor(
    readonly lastLsn: number,
    readonly parts: number,
  ) {
    super(
      `The log cannot take a ${String(parts)}-frame transaction after LSN ${String(lastLsn)}: committed LSNs end at ${String(MAX_LSN)}`,
    );
    this.name = "LogExhaustedError";
  }
}

export class LogAppender {
  readonly #session: StoreSession;
  readonly #strict: boolean;
  #salt: number;
  #chain: number;
  #end: number;
  #lastLsn: number;
  #generation: number;
  #nextSlot: 0 | 1;
  #acked: Boundary;
  /** The newest durable group's boundary. */
  #durable: Boundary;
  #rootSequence: number;
  /** LSN of the last frame encoded (`lastLsn` unless frames are deferred by a recycle). */
  #encodedLsn: number;
  /** Bodies appended since `beginRecycle` (copied), with their first LSNs; null when none is running. */
  #deferred: Array<{ readonly body: Uint8Array; readonly first: number }> | null = null;
  #recycled: Promise<void> | null = null;
  #recycledResolve: (() => void) | null = null;
  /** Frames appended and not yet written to the log file (written by the next flush). */
  #queued: Uint8Array[] = [];
  #queuedBytes = 0;
  #queuedCost = 0;
  #inFlightBytes = 0;
  #deferredBytes = 0;
  #streaming: Promise<number> | null = null;
  #header: Uint8Array | null;
  #flushing: Promise<void> | null = null;
  #scheduled = false;
  #idle: ReturnType<typeof setTimeout> | null = null;
  #lastAppendAt = performance.now();
  #failure: Error | null = null;
  readonly #waiters: Array<{ lsn: number; resolve: () => void; reject: (error: Error) => void }> =
    [];
  readonly #onFailure: (error: Error) => void;
  /** Frames flushed (diagnostics) and bytes appended. */
  flushes = 0;
  bytes = 0;

  constructor(
    session: StoreSession,
    position: LogPosition,
    strict: boolean,
    onFailure: (error: Error) => void,
    readonly buffers?: BufferAllocator,
    readonly checkAppend?: () => void,
  ) {
    this.#header = buffers?.create(LOG_HEADER_BYTES) ?? null;
    this.#session = session;
    this.#strict = strict;
    this.#salt = position.salt;
    this.#chain = position.chain;
    this.#end = position.end;
    this.#lastLsn = position.lastLsn;
    this.#encodedLsn = position.lastLsn;
    this.#generation = position.generation;
    this.#nextSlot = position.nextSlot;
    this.#acked = position.acked;
    this.#durable = { end: position.end, chain: position.chain, lsn: position.lastLsn };
    this.#rootSequence = position.rootSequence;
    this.#onFailure = onFailure;
  }

  get lastLsn(): number {
    return this.#lastLsn;
  }

  get durableLsn(): number {
    return this.#durable.lsn;
  }

  get end(): number {
    return this.#end;
  }

  get failure(): Error | null {
    return this.#failure;
  }

  #check(): void {
    if (this.#failure !== null) throw this.#failure;
  }

  /**
   * Queues one transaction (one or more frames); returns its last LSN, waiting only under
   * bounded-queue pressure. A transaction
   * whose last frame would pass `MAX_LSN` (committed LSNs end there; pending statement seqs lie
   * above it) is refused with `LogExhaustedError` before anything changes (review B3-p2a-3 #5);
   * so is one too large for the log.
   */
  append(body: Uint8Array): MaybePromise<number> {
    this.#check();
    this.checkAppend?.();
    if (this.#streaming !== null) return this.#streaming.then(() => this.append(body));
    const frameParts = Math.max(1, Math.ceil(body.length / MAX_FRAME_BODY_BYTES));
    const framed = body.length + frameParts * (40 + COMMIT_BUFFER_OVERHEAD);
    if (this.buffers !== undefined && framed > LOG_QUEUE_BYTES)
      return this.appendStream(body.length, (out, offset) => {
        out.set(body.subarray(offset, offset + out.length));
      });
    if (
      this.buffers !== undefined &&
      this.#queuedCost + this.#inFlightBytes + this.#deferredBytes + framed > LOG_QUEUE_BYTES
    ) {
      const drain = this.#deferred !== null ? this.#recycled : this.flushEncoded();
      return need(drain).then(() => this.append(body));
    }
    const parts = Math.max(1, Math.ceil(body.length / MAX_FRAME_BODY_BYTES));
    if (parts > MAX_FRAME_PARTS) throw new RangeError("A transaction is too large for the log");
    this.#lastAppendAt = performance.now();
    const first = this.#lastLsn + 1;
    if (first + parts - 1 > MAX_LSN) throw new LogExhaustedError(this.#lastLsn, parts);
    if (this.#deferred !== null) {
      // A recycle is switching incarnations: keep the body (the caller may reuse its buffer).
      const copy = this.buffers?.create(body.length) ?? new Uint8Array(body.length);
      copy.set(body);
      this.#deferred.push({ body: copy, first });
      this.#deferredBytes += framed;
      this.#lastLsn = first + parts - 1;
      return this.#lastLsn;
    }
    this.#encode(body, first, parts);
    this.#schedule();
    return this.#lastLsn;
  }

  /** Encodes one transaction's frames at `first …` and queues them (all or nothing). */
  #encode(body: Uint8Array, first: number, parts: number): void {
    // Frames are encoded first and committed together: a failure leaves the log unchanged.
    const frames: Uint8Array[] = [];
    let chain = this.#chain;
    try {
      for (let part = 0; part < parts; part++) {
        const slice =
          parts === 1
            ? body
            : body.subarray(part * MAX_FRAME_BODY_BYTES, (part + 1) * MAX_FRAME_BODY_BYTES);
        const frame = encodeFrame(
          { salt: this.#salt, lsn: first + part, partIndex: part, partCount: parts },
          slice,
          chain,
          this.buffers?.create(frameLength(slice.length)),
        );
        chain = frame.chain;
        frames.push(frame.bytes);
      }
    } catch (error) {
      for (const frame of frames) this.buffers?.release(frame);
      throw error;
    }
    this.#chain = chain;
    for (const bytes of frames) {
      this.#end += bytes.length;
      this.bytes += bytes.length;
      this.#queued.push(bytes);
      this.#queuedBytes += bytes.length;
      this.#queuedCost += bytes.length + COMMIT_BUFFER_OVERHEAD;
    }
    this.#encodedLsn = first + parts - 1;
    if (this.#deferred === null) this.#lastLsn = this.#encodedLsn;
  }

  /**
   * Bounded multipart append. Only complete transactions advance the header boundary. A crash
   * after any prefix is discarded by the existing incremental scanner. Other appends wait so
   * every part keeps its existing contiguous LSN and byte position.
   */
  appendStream(
    length: number,
    fill: (body: Uint8Array, offset: number) => MaybePromise<void>,
  ): Promise<number> {
    this.#check();
    if (this.#streaming !== null)
      return this.#streaming.then(() => this.appendStream(length, fill));
    const parts = Math.max(1, Math.ceil(length / MAX_FRAME_BODY_BYTES));
    if (parts > MAX_FRAME_PARTS) throw new RangeError("A transaction is too large for the log");
    const run = (async () => {
      if (this.#recycled !== null) await this.#recycled;
      await this.flushEncoded();
      this.#check();
      const first = this.#lastLsn + 1;
      if (first + parts - 1 > MAX_LSN) throw new LogExhaustedError(this.#lastLsn, parts);
      let chain = this.#chain;
      let end = this.#end;
      try {
        for (let part = 0; part < parts; part++) {
          const offset = part * MAX_FRAME_BODY_BYTES;
          const bodyLength = Math.min(MAX_FRAME_BODY_BYTES, length - offset);
          const bytes =
            this.buffers?.create(frameLength(bodyLength)) ??
            new Uint8Array(frameLength(bodyLength));
          try {
            const body = bytes.subarray(FRAME_HEADER_BYTES, FRAME_HEADER_BYTES + bodyLength);
            await fill(body, offset);
            const frame = encodeFrame(
              { salt: this.#salt, lsn: first + part, partIndex: part, partCount: parts },
              body,
              chain,
              bytes,
            );
            // Resolution transfers responsibility for retained copies to storage; see #writeQueued.
            await this.#session.appendLog(bytes);
            chain = frame.chain;
            end += bytes.length;
          } finally {
            this.buffers?.release(bytes);
          }
          this.#check();
        }
        this.bytes += end - this.#end;
        this.#end = end;
        this.#chain = chain;
        this.#encodedLsn = first + parts - 1;
        this.#lastLsn = this.#encodedLsn;
        this.#lastAppendAt = performance.now();
        this.#schedule();
        return this.#lastLsn;
      } catch (error) {
        this.#fail(error);
        throw need(this.#failure);
      }
    })();
    this.#streaming = run;
    void run
      .finally(() => {
        this.#streaming = null;
      })
      .catch(() => undefined);
    return run;
  }

  /** Transactions deferred by the running recycle. */
  get deferredCount(): number {
    return this.#deferred?.length ?? 0;
  }

  /** True while a recycle defers appends. */
  get recycling(): boolean {
    return this.#deferred !== null;
  }

  /** Starts a recycle: returns the cut (every LSN ≤ it is in the old log). See "Recycling". */
  beginRecycle(): number {
    this.#check();
    if (this.#deferred !== null || this.#streaming !== null)
      throw new RangeError("A log recycle or streamed append is already running");
    this.#deferred = [];
    this.#recycled = new Promise<void>((resolve) => {
      this.#recycledResolve = resolve;
    });
    return this.#encodedLsn;
  }

  /** Makes every encoded frame (through the cut, during a recycle) durable. */
  async flushEncoded(): Promise<void> {
    this.#check();
    while (this.#durable.lsn < this.#encodedLsn) {
      if (this.#flushing !== null) await this.#flushing;
      else await this.#flush();
      this.#check();
    }
    if (this.#flushing !== null) await this.#flushing;
  }

  /**
   * Starts `incarnation` (the log was reset after the root naming it was published): empty at the
   * cut, with fresh header slots; then writes the deferred bodies as its first frames.
   */
  async finishRecycle(incarnation: {
    readonly salt: number;
    readonly firstLsn: number;
    readonly rootSequence: number;
  }): Promise<void> {
    const deferred = this.#deferred;
    if (deferred === null) throw new RangeError("No log recycle is running");
    if (this.#flushing !== null) await this.#flushing;
    if (this.#idle !== null) {
      clearTimeout(this.#idle);
      this.#idle = null;
    }
    const cut = incarnation.firstLsn - 1;
    if (cut !== this.#encodedLsn || this.#durable.lsn !== cut)
      throw new RangeError("The new incarnation does not start at the durable cut");
    const salt = incarnation.salt >>> 0;
    this.#salt = salt;
    this.#chain = salt;
    this.#end = 0;
    this.#generation = 1;
    this.#nextSlot = 0;
    this.#acked = { end: 0, chain: salt, lsn: cut };
    this.#durable = { end: 0, chain: salt, lsn: cut };
    this.#rootSequence = incarnation.rootSequence;
    this.#release(deferred);
  }

  /** Ends a recycle that did not switch: the deferred bodies go to the current log. */
  abandonRecycle(): void {
    const deferred = this.#deferred;
    if (deferred === null) return;
    this.#release(deferred);
  }

  /** A recycle failed after its switch: the appender fails, and waiters on the deferred reject. */
  failRecycle(cause: unknown): void {
    if (this.#deferred === null) return;
    for (const item of this.#deferred) this.buffers?.release(item.body);
    this.#deferred = null;
    this.#deferredBytes = 0;
    this.#fail(cause);
    this.#recycledResolve?.();
    this.#recycled = null;
    this.#recycledResolve = null;
  }

  #release(deferred: ReadonlyArray<{ readonly body: Uint8Array; readonly first: number }>): void {
    const last = this.#lastLsn;
    this.#deferred = null;
    let released = 0;
    try {
      for (const { body, first } of deferred) {
        const parts = Math.max(1, Math.ceil(body.length / MAX_FRAME_BODY_BYTES));
        try {
          this.#encode(body, first, parts);
        } finally {
          this.buffers?.release(body);
          released++;
        }
      }
    } catch (error) {
      this.#fail(error);
    } finally {
      for (let k = released; k < deferred.length; k++)
        this.buffers?.release(need(deferred[k]).body);
    }
    this.#deferredBytes = 0;
    this.#lastLsn = last;
    this.#recycledResolve?.();
    this.#recycled = null;
    this.#recycledResolve = null;
    if (this.#lastLsn > this.#durable.lsn) this.#schedule();
  }

  #schedule(): void {
    if (this.#idle !== null) {
      clearTimeout(this.#idle);
      this.#idle = null;
    }
    if (this.#scheduled || this.#flushing !== null) return;
    this.#scheduled = true;
    const start = () => {
      this.#scheduled = false;
      void this.#flush();
    };
    if (this.#strict) queueMicrotask(start);
    else setTimeout(start, RELAXED_DELAY_MS);
  }

  /** Resolves when every frame with LSN ≤ `lsn` is durable; null when already so. */
  durable(lsn: number): Promise<void> | null {
    if (this.#failure !== null) return Promise.reject(this.#failure);
    if (lsn <= this.#durable.lsn) return null;
    return new Promise((resolve, reject) => {
      this.#waiters.push({ lsn, resolve, reject });
      if (!this.#scheduled && this.#flushing === null) this.#schedule();
    });
  }

  /** Flushes everything queued now; resolves when it is durable (after a running recycle). */
  async flush(): Promise<void> {
    this.#check();
    if (this.#streaming !== null) await this.#streaming;
    this.#check();
    if (this.#recycled !== null) await this.#recycled;
    this.#check();
    const target = this.#lastLsn;
    while (this.#durable.lsn < target) {
      if (this.#flushing !== null) await this.#flushing;
      else await this.#flush();
      this.#check();
    }
  }

  #flush(): Promise<void> {
    if (this.#flushing !== null) return this.#flushing;
    const group: Boundary = { end: this.#end, chain: this.#chain, lsn: this.#encodedLsn };
    if (group.lsn <= this.#durable.lsn) return Promise.resolve();
    const run = (async () => {
      try {
        await this.#writeQueued();
        await this.#writeHeader(this.#durable);
        await this.#session.flushLog();
        this.flushes += 1;
        this.#durable = group;
      } catch (error) {
        this.#fail(error);
        return;
      } finally {
        this.#flushing = null;
      }
      this.#settle();
      if (this.#encodedLsn > this.#durable.lsn) this.#schedule();
      else this.#armIdle();
    })();
    this.#flushing = run;
    return run;
  }

  /**
   * Writes the queued frames as one bounded append. The input charge ends when appendLog resolves;
   * any retained adapter copies belong to storage (IDB currently batches up to 4 MiB). Storage
   * must charge/backpressure those copies itself; they are not encoded-frame ownership here.
   */
  async #writeQueued(): Promise<void> {
    if (this.#queued.length === 0) return;
    const frames = this.#queued;
    const size = this.#queuedBytes;
    this.#queued = [];
    this.#queuedBytes = 0;
    let joined: Uint8Array | null = null;
    this.#inFlightBytes = this.#queuedCost;
    this.#queuedCost = 0;
    try {
      let bytes = need(frames[0]);
      if (frames.length > 1) {
        joined = this.buffers?.create(size) ?? new Uint8Array(size);
        this.#inFlightBytes += size + COMMIT_BUFFER_OVERHEAD;
        let at = 0;
        for (const frame of frames) {
          joined.set(frame, at);
          at += frame.length;
        }
        bytes = joined;
      }
      await this.#session.appendLog(bytes);
    } finally {
      if (joined !== null) this.buffers?.release(joined);
      for (const frame of frames) this.buffers?.release(frame);
      this.#inFlightBytes = 0;
    }
  }

  async #writeHeader(ack: Boundary): Promise<void> {
    const output = this.#header ?? undefined;
    const header = encodeLogHeader(
      {
        salt: this.#salt,
        generation: this.#generation,
        ackEnd: ack.end,
        ackChain: ack.chain,
        ackLsn: ack.lsn,
        rootSequence: this.#rootSequence,
      },
      output,
    );
    await this.#session.writeLogHeader(this.#nextSlot, header);
    this.#generation += 1;
    this.#nextSlot = this.#nextSlot === 0 ? 1 : 0;
    this.#acked = ack;
  }

  #armIdle(): void {
    if (this.#acked.lsn >= this.#durable.lsn || this.#idle !== null) return;
    this.#idle = setTimeout(
      () => {
        this.#idle = null;
        void this.#idleFlush();
      },
      Math.max(0, IDLE_ARM_MS - (performance.now() - this.#lastAppendAt)),
    );
    const timer = this.#idle as { unref?: () => void };
    timer.unref?.();
  }

  async #idleFlush(): Promise<void> {
    if (this.#failure !== null || this.#flushing !== null || this.#scheduled) return;
    if (this.#acked.lsn >= this.#durable.lsn) return;
    const run = (async () => {
      try {
        await this.#writeHeader(this.#durable);
        await this.#session.flushLog();
      } catch (error) {
        this.#fail(error);
      } finally {
        this.#flushing = null;
      }
      if (this.#encodedLsn > this.#durable.lsn) this.#schedule();
    })();
    this.#flushing = run;
    await run;
  }

  #settle(): void {
    for (let k = this.#waiters.length - 1; k >= 0; k--) {
      const waiter = this.#waiters[k];
      if (waiter !== undefined && waiter.lsn <= this.#durable.lsn) {
        this.#waiters.splice(k, 1);
        waiter.resolve();
      }
    }
  }

  #fail(error: unknown): void {
    if (this.#failure !== null) return;
    this.#failure = new WritePathFailedError({ cause: error });
    for (const waiter of this.#waiters.splice(0)) waiter.reject(this.#failure);
    this.#onFailure(this.#failure);
  }

  /** Flushes, acknowledges the newest group in a header, and stops the idle timer. */
  async close(): Promise<void> {
    try {
      if (this.#idle !== null) {
        clearTimeout(this.#idle);
        this.#idle = null;
      }
      if (this.#streaming !== null) await this.#streaming.catch(() => undefined);
      if (this.#flushing !== null) await this.#flushing;
      if (this.#failure !== null) {
        for (const frame of this.#queued.splice(0)) this.buffers?.release(frame);
        this.#queuedBytes = 0;
        this.#queuedCost = 0;
        for (const item of this.#deferred ?? []) this.buffers?.release(item.body);
        this.#deferred = null;
        this.#deferredBytes = 0;
        return;
      }
      await this.flush();
      if (this.#flushing !== null) await this.#flushing;
      if (this.#acked.lsn < this.#durable.lsn) {
        await this.#writeHeader(this.#durable);
        await this.#session.flushLog();
      }
    } finally {
      if (this.#header !== null) this.buffers?.release(this.#header);
      this.#header = null;
    }
  }

  /** The position a log reset or a root needs (salt, LSNs). */
  position(): { salt: number; lastLsn: number; end: number } {
    return { salt: this.#salt, lastLsn: this.#lastLsn, end: this.#end };
  }
}
