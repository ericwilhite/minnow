/**
 * The log appender: group commit over a `StoreSession` log (access/write-path.ts "Commit",
 * format/log-frame.ts envelope and header slots).
 *
 * - `append(body)` is synchronous: it splits the body into parts of at most
 *   `MAX_FRAME_BODY_BYTES`, assigns contiguous LSNs, chains checksums, and queues the frames. The
 *   queued bytes reach `appendLog` in order on one promise chain.
 * - A flush takes every frame queued before it (a **group**), waits for their appends, writes the
 *   header slot not holding the newest header (generation + 1) acknowledging the previous durable
 *   group, and calls `flushLog` once. Frames queued meanwhile form the next group. Strict mode
 *   starts a flush as soon as the current task ends; relaxed mode within `RELAXED_DELAY_MS`.
 * - After the last flush, an idle flush (`IDLE_FLUSH_MS`) writes a header acknowledging the newest
 *   group, so every acknowledged group but none is exposed to a cleanly truncated log.
 * - Any append or flush failure fails the appender: every waiter rejects with
 *   `WritePathFailedError` and later calls throw it.
 */
import { MAX_LSN } from "../access/snapshot.js";
import { WritePathFailedError } from "../access/write-path.js";
import {
  encodeFrame,
  encodeLogHeader,
  MAX_FRAME_BODY_BYTES,
  MAX_FRAME_PARTS,
} from "../format/log-frame.js";
import type { StoreSession } from "../storage/contract.js";
import { need } from "./bytes.js";

export const IDLE_FLUSH_MS = 100;
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
  /** Frames appended and not yet written to the log file (written by the next flush). */
  #queued: Uint8Array[] = [];
  #queuedBytes = 0;
  #flushing: Promise<void> | null = null;
  #scheduled = false;
  #idle: ReturnType<typeof setTimeout> | null = null;
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
  ) {
    this.#session = session;
    this.#strict = strict;
    this.#salt = position.salt;
    this.#chain = position.chain;
    this.#end = position.end;
    this.#lastLsn = position.lastLsn;
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
   * Queues one transaction (one or more frames); returns its last LSN. Synchronous. A transaction
   * whose last frame would pass `MAX_LSN` (committed LSNs end there; pending statement seqs lie
   * above it) is refused with `LogExhaustedError` before anything changes (review B3-p2a-3 #5);
   * so is one too large for the log.
   */
  append(body: Uint8Array): number {
    this.#check();
    const parts = Math.max(1, Math.ceil(body.length / MAX_FRAME_BODY_BYTES));
    if (parts > MAX_FRAME_PARTS) throw new RangeError("A transaction is too large for the log");
    const first = this.#lastLsn + 1;
    if (first + parts - 1 > MAX_LSN) throw new LogExhaustedError(this.#lastLsn, parts);
    // Frames are encoded first and committed together: a failure leaves the log unchanged.
    const frames: Uint8Array[] = [];
    let chain = this.#chain;
    for (let part = 0; part < parts; part++) {
      const slice =
        parts === 1
          ? body
          : body.subarray(part * MAX_FRAME_BODY_BYTES, (part + 1) * MAX_FRAME_BODY_BYTES);
      const frame = encodeFrame(
        { salt: this.#salt, lsn: first + part, partIndex: part, partCount: parts },
        slice,
        chain,
      );
      chain = frame.chain;
      frames.push(frame.bytes);
    }
    this.#chain = chain;
    for (const bytes of frames) {
      this.#end += bytes.length;
      this.bytes += bytes.length;
      this.#queued.push(bytes);
      this.#queuedBytes += bytes.length;
    }
    this.#lastLsn = first + parts - 1;
    this.#schedule();
    return this.#lastLsn;
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

  /** Flushes everything queued now; resolves when it is durable. */
  async flush(): Promise<void> {
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
    const group: Boundary = { end: this.#end, chain: this.#chain, lsn: this.#lastLsn };
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
      if (this.#lastLsn > this.#durable.lsn) this.#schedule();
      else this.#armIdle();
    })();
    this.#flushing = run;
    return run;
  }

  /** Writes the queued frames, in order, as one log append. */
  async #writeQueued(): Promise<void> {
    if (this.#queued.length === 0) return;
    const frames = this.#queued;
    let bytes = need(frames[0]);
    if (frames.length > 1) {
      bytes = new Uint8Array(this.#queuedBytes);
      let at = 0;
      for (const frame of frames) {
        bytes.set(frame, at);
        at += frame.length;
      }
    }
    this.#queued = [];
    this.#queuedBytes = 0;
    await this.#session.appendLog(bytes);
  }

  async #writeHeader(ack: Boundary): Promise<void> {
    const header = encodeLogHeader({
      salt: this.#salt,
      generation: this.#generation,
      ackEnd: ack.end,
      ackChain: ack.chain,
      ackLsn: ack.lsn,
      rootSequence: this.#rootSequence,
    });
    await this.#session.writeLogHeader(this.#nextSlot, header);
    this.#generation += 1;
    this.#nextSlot = this.#nextSlot === 0 ? 1 : 0;
    this.#acked = ack;
  }

  #armIdle(): void {
    if (this.#acked.lsn >= this.#durable.lsn || this.#idle !== null) return;
    this.#idle = setTimeout(() => {
      this.#idle = null;
      void this.#idleFlush();
    }, IDLE_FLUSH_MS);
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
      if (this.#lastLsn > this.#durable.lsn) this.#schedule();
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
    if (this.#idle !== null) {
      clearTimeout(this.#idle);
      this.#idle = null;
    }
    if (this.#failure !== null) return;
    await this.flush();
    if (this.#flushing !== null) await this.#flushing;
    if (this.#acked.lsn < this.#durable.lsn) {
      await this.#writeHeader(this.#durable);
      await this.#session.flushLog();
    }
  }

  /** The position a log reset or a root needs (salt, LSNs). */
  position(): { salt: number; lastLsn: number; end: number } {
    return { salt: this.#salt, lastLsn: this.#lastLsn, end: this.#end };
  }
}
