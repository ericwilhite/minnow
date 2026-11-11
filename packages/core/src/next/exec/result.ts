/**
 * Streaming results: wire-ready columnar frames of bounded bytes, pulled by the owner under a
 * credit window, with no row-count cap (ruling R3: this is the one frame-accounting model).
 *
 * ## Frames and payloads
 * - A `ResultFrame` is a dense run of result rows. Its `payload` is plain data, safe for
 *   `postMessage`; `transfer` lists its ArrayBuffers; `release` and `requiresLsn` stay outside the
 *   payload. The owner posts `postMessage(frame.payload, frame.transfer)` as is: no second
 *   compaction, no copy.
 * - Every typed array in a payload covers its whole ArrayBuffer (offset 0, full length), and no
 *   buffer is shared with another frame or with the engine, so transferring moves exactly the
 *   frame's bytes (`frameTransferList` checks it). Logical aliases own separate mutable
 *   data/codes/validity, so client mutation of one column cannot change another. Immutable
 *   stream-dictionary entries remain shared by dictionary id.
 * - Columns keep their executor form (vector/vector.ts `VectorData`): exact int64 (`safe` f64 or
 *   BigInt64Array), decimal tiers, UTF-8 bytes, constants as one value. Types travel once, in the
 *   stream's `columns`. JS values and row objects are built by the client, on request.
 * - **The one wire definition** (ruling R16.5). The owner imports and re-exports these payload
 *   types; nothing else defines result frames.
 * - **Dictionaries are per stream, deltas are per frame.** A dictionary column names a stream
 *   dictionary by id; its codes index that whole (cumulative) dictionary. A frame's
 *   `dictionaries` lists the entries it adds: each delta appends `values` to dictionary `id`
 *   starting at code `from`, which must equal that dictionary's size so far (otherwise a frame was
 *   lost or reordered: a protocol error). The client applies a frame's deltas, in order, before
 *   reading its columns, and keeps stream dictionaries until the stream ends. A frame whose codes
 *   use only entries sent earlier carries no delta. Dictionary bytes per stream are bounded by
 *   `STREAM_DICTIONARY_BYTES`: past it, columns go out plain (flat varlen), so the client's
 *   dictionary memory stays bounded.
 * - Size: the result sink seals a frame at about `RESULT_FRAME_TARGET_BYTES`, or earlier when the
 *   consumer is waiting with nothing queued (`demand`), so a point read or the first page of
 *   `SELECT *` leaves at once while a fast producer still sends large frames. One row larger than
 *   the target is a frame of one row.
 *
 * ## Credits and the result grant
 * - A frame is charged to the execution's result grant from the moment it is offered until the
 *   owner calls `release()` (idempotent), which it does when the client's credit for that frame
 *   returns: not at `postMessage`, so the client's backlog is bounded too. Unreleased bytes,
 *   queued or handed out, are the stream's outstanding bytes; while they reach `windowBytes`, the
 *   sink's `push` returns `blocked` (exec/pipeline.ts) and the pipeline pauses.
 * - The result grant outlives the execution's `executed`. It closes, and `delivered` settles, when
 *   the stream has ended and every frame was released, or at once when the stream is cancelled
 *   (the owner cancels on disconnect; frames already transferred then live in the client's heap).
 * - An owner that holds a window of frames without releasing them and then waits on `next()`
 *   deadlocks itself; that is an owner bug.
 *
 * ## Delivery rule
 * `frame.requiresLsn` is the largest LSN the frame's rows depend on (the view's `observedLsn` and
 * every sequence reservation used so far). Under strict durability the owner posts the frame only
 * after `writePath.durable(requiresLsn)`, which is null, so no await, when it is already durable.
 *
 * ## Completions and the RETURNING spool (rulings R2, R11, R16.8)
 * An execution has three completions: **executed** (`Execution.executed`: the plan ran; for DML
 * the statement ended), **committed** (the owner's `Execution.acknowledgeCommit()` after the
 * commit is acknowledged), **delivered** (`delivered`: every frame released, the result grant
 * closed). For DML with RETURNING the sink writes rows into the result spool (charged to the
 * result grant, spilled through the spill service under pressure) instead of offering frames:
 * - autocommit (`returning: "atCommit"`): the spool becomes frames only after committed;
 * - inside an explicit transaction (`returning: "atExecuted"`): the spool becomes frames as soon
 *   as executed, before COMMIT, as PostgreSQL returns them.
 * A cancel, a rollback or a failed commit discards the spool.
 *
 * ## Pulling, end, errors, cancellation
 * - `next()` returns frames in order, then `null` after the last; `poll()` is its synchronous
 *   form for the fast path (a frame, `null` at the end, `undefined` when none is ready yet). One
 *   pending `next()` at a time.
 * - On an execution error, queued frames are still delivered (a client may have seen rows before
 *   the error, as in PostgreSQL), then `next()` rejects (or `poll()` throws) with the error.
 * - `cancel(reason)`: queued frames are released, the producer's pending `ready()` rejects with
 *   `QueryCancelledError`, the execution is cancelled, the result grant closes. Afterwards `next()`
 *   rejects with `QueryCancelledError`.
 */
import { frameTransferList, flats } from "./frame-buffers.js";
export { frameTransferList, FrameBufferError } from "./frame-buffers.js";
import type { OutputColumn } from "../ir/ir.js";
import { bigintBytes } from "../memory/governor.js";
import { BATCH_TARGET_BYTES, type Validity, type VectorData } from "../vector/vector.js";
import { QueryCancelledError } from "./errors.js";
import type { PushResult } from "./pipeline.js";

/** About one batch's bytes: large enough to amortize a 55–280 µs cross-tab message. */
export const RESULT_FRAME_TARGET_BYTES = BATCH_TARGET_BYTES;
/** Default credit window: two frames, so the pipeline fills one while the other is in flight. */
export const RESULT_WINDOW_BYTES = 2 * RESULT_FRAME_TARGET_BYTES;
/** Most dictionary bytes one stream sends; later dictionary columns go out plain. */
export const STREAM_DICTIONARY_BYTES = 1024 * 1024;

export interface WireFlat {
  readonly form: "flat";
  readonly length: number;
  /** Bit set = valid; null when every slot is valid. */
  readonly validity: Validity | null;
  readonly data: VectorData;
}

export interface WireConstant {
  readonly form: "constant";
  readonly length: number;
  /** Length 1. */
  readonly value: WireFlat;
}

export interface WireDictionary {
  readonly form: "dictionary";
  readonly length: number;
  /** Codes into stream dictionary `dictionary` (meaningful only where `validity` says valid). */
  readonly codes: Int32Array;
  readonly validity: Validity | null;
  readonly dictionary: number;
}

export type WireVector = WireFlat | WireConstant | WireDictionary;

/** Entries appended to stream dictionary `id` at index `from` (distinct, non-null). */
export interface DictionaryDelta {
  readonly id: number;
  readonly from: number;
  readonly values: WireFlat;
}

/** Plain data: structured-clone safe, no functions. */
export interface FramePayload {
  /** 0, 1, 2… within the stream, so a protocol can detect a lost frame. */
  readonly sequence: number;
  readonly rowCount: number;
  /** One per output column, each `length === rowCount`. */
  readonly columns: readonly WireVector[];
  /** Dictionary entries first needed by this frame, applied before its columns are read. */
  readonly dictionaries: readonly DictionaryDelta[];
}

export interface ResultFrame {
  readonly payload: FramePayload;
  /** The payload's ArrayBuffers, for `postMessage`'s transfer list. */
  readonly transfer: readonly ArrayBuffer[];
  /** Bytes charged until `release`. */
  readonly byteLength: number;
  /** See "Delivery rule". */
  readonly requiresLsn: number;
  /** Returns the frame's credit and its bytes. Idempotent. */
  release(): void;
}

export interface ResultStream {
  readonly columns: readonly OutputColumn[];
  /** The next frame, or null after the last; rejects with the execution's error. */
  next(): Promise<ResultFrame | null>;
  /** Synchronous `next`: a frame, null at the end, undefined when none is ready; throws errors. */
  poll(): ResultFrame | null | undefined;
  /** The consumer no longer wants results: cancels the execution. Idempotent. */
  cancel(reason?: unknown): void;
}

/** A payload without its sequence number (the channel numbers frames). */
export type UnnumberedPayload = Omit<FramePayload, "sequence">;

/** What the result sink offers: a sealed payload and how to free its bytes. */
export interface FrameOffer {
  readonly payload: UnnumberedPayload;
  readonly requiresLsn: number;
  /** Frees the frame's bytes from the result grant (called once, on release). */
  readonly free: () => void;
}

export interface ResultChannelOptions {
  readonly windowBytes?: number;
  /** Called once when the consumer cancels an open stream; the execution aborts its pipelines. */
  readonly onCancel?: (reason: unknown) => void;
}

type State =
  | { readonly kind: "open" }
  | { readonly kind: "ended" }
  | { readonly kind: "failed"; readonly error: Error }
  | { readonly kind: "cancelled"; readonly error: QueryCancelledError };

interface Waiter<T> {
  readonly resolve: (value: T) => void;
  readonly reject: (reason: unknown) => void;
}

/**
 * The credit window between the result sink (producer) and the owner (consumer): the production
 * implementation of `ResultStream`.
 */
export class ResultChannel implements ResultStream {
  readonly windowBytes: number;
  /** Settles as "Credits and the result grant" says; the execution closes the grant on it. */
  readonly delivered: Promise<void>;
  readonly #onCancel: ((reason: unknown) => void) | undefined;
  readonly #queue: ResultFrame[] = [];
  #state: State = { kind: "open" };
  #outstanding = 0;
  #sequence = 0;
  #consumer: Waiter<ResultFrame | null> | null = null;
  #producer: Waiter<void> | null = null;
  readonly #settle: Waiter<void>;
  #settled = false;

  constructor(
    readonly columns: readonly OutputColumn[],
    options: ResultChannelOptions = {},
  ) {
    this.windowBytes = options.windowBytes ?? RESULT_WINDOW_BYTES;
    this.#onCancel = options.onCancel;
    let settle: Waiter<void> | null = null;
    this.delivered = new Promise<void>((resolve, reject) => {
      settle = { resolve, reject };
    });
    this.#settle = settle as unknown as Waiter<void>;
    this.delivered.catch(() => undefined);
  }

  /** Unreleased frame bytes (queued or handed out). */
  get outstandingBytes(): number {
    return this.#outstanding;
  }

  /** The consumer is waiting and nothing is queued: seal a partial frame now. */
  get demand(): boolean {
    return this.#consumer !== null;
  }

  // ---- producer side (the result sink) ----

  /**
   * Hands a sealed frame over. `blocked` means it was accepted but the window is full: push
   * nothing more until `ready()` resolves. After a cancel the frame is freed at once and the
   * result is `blocked` (the producer then meets the rejection in `ready()`).
   */
  offer(offer: FrameOffer): PushResult {
    if (this.#state.kind === "ended" || this.#state.kind === "failed")
      throw new TypeError("offer() after the result stream ended");
    if (this.#state.kind === "cancelled") {
      offer.free();
      return "blocked";
    }
    const frame = this.#frame(offer);
    this.#outstanding += frame.byteLength;
    const consumer = this.#consumer;
    if (consumer !== null) {
      this.#consumer = null;
      consumer.resolve(frame);
    } else this.#queue.push(frame);
    return this.#outstanding >= this.windowBytes ? "blocked" : "ready";
  }

  /** Resolves when outstanding bytes are below the window; rejects after a cancel. */
  ready(): Promise<void> {
    if (this.#state.kind === "cancelled") return Promise.reject(this.#state.error);
    if (this.#outstanding < this.windowBytes) return Promise.resolve();
    if (this.#producer !== null) throw new TypeError("ready() is already pending");
    return new Promise<void>((resolve, reject) => {
      this.#producer = { resolve, reject };
    });
  }

  /** The last frame was offered. */
  end(): void {
    if (this.#state.kind !== "open") return;
    this.#state = { kind: "ended" };
    this.#settleConsumer();
    this.#maybeSettle();
  }

  /** The execution failed: queued frames are still delivered, then `next()` rejects. */
  fail(error: unknown): void {
    if (this.#state.kind !== "open") return;
    this.#state = {
      kind: "failed",
      error: error instanceof Error ? error : new Error(String(error), { cause: error }),
    };
    this.#settleConsumer();
    this.#maybeSettle();
  }

  // ---- consumer side (the owner) ----

  next(): Promise<ResultFrame | null> {
    if (this.#consumer !== null) return Promise.reject(new TypeError("next() is already pending"));
    const frame = this.#queue.shift();
    if (frame !== undefined) return Promise.resolve(frame);
    switch (this.#state.kind) {
      case "ended":
        return Promise.resolve(null);
      case "failed":
      case "cancelled":
        return Promise.reject(this.#state.error);
      case "open":
        return new Promise<ResultFrame | null>((resolve, reject) => {
          this.#consumer = { resolve, reject };
        });
    }
  }

  poll(): ResultFrame | null | undefined {
    if (this.#consumer !== null) throw new TypeError("next() is pending");
    const frame = this.#queue.shift();
    if (frame !== undefined) return frame;
    switch (this.#state.kind) {
      case "ended":
        return null;
      case "failed":
      case "cancelled":
        throw this.#state.error;
      case "open":
        return undefined;
    }
  }

  cancel(reason?: unknown): void {
    if (this.#state.kind === "cancelled") return;
    const wasOpen = this.#state.kind === "open";
    const error = new QueryCancelledError(reason);
    this.#state = { kind: "cancelled", error };
    for (const frame of this.#queue.splice(0)) frame.release();
    const consumer = this.#consumer;
    this.#consumer = null;
    consumer?.reject(error);
    const producer = this.#producer;
    this.#producer = null;
    producer?.reject(error);
    if (!this.#settled) {
      this.#settled = true;
      this.#settle.reject(error);
    }
    if (wasOpen) this.#onCancel?.(reason);
  }

  #frame(offer: FrameOffer): ResultFrame {
    let released = false;
    const payload: FramePayload = { ...offer.payload, sequence: this.#sequence++ };
    const byteLength = frameByteLength(payload);
    return {
      payload,
      transfer: frameTransferList(payload),
      byteLength,
      requiresLsn: offer.requiresLsn,
      release: () => {
        if (released) return;
        released = true;
        this.#outstanding -= byteLength;
        offer.free();
        if (this.#outstanding < this.windowBytes && this.#producer !== null) {
          const producer = this.#producer;
          this.#producer = null;
          producer.resolve();
        }
        this.#maybeSettle();
      },
    };
  }

  #settleConsumer(): void {
    const consumer = this.#consumer;
    if (consumer === null) return;
    this.#consumer = null;
    if (this.#state.kind === "failed") consumer.reject(this.#state.error);
    else consumer.resolve(null);
  }

  #maybeSettle(): void {
    if (this.#settled || this.#queue.length > 0 || this.#outstanding > 0) return;
    if (this.#state.kind === "ended") {
      this.#settled = true;
      this.#settle.resolve();
    } else if (this.#state.kind === "failed") {
      this.#settled = true;
      this.#settle.reject(this.#state.error);
    }
  }
}

/**
 * Bytes a payload occupies: each distinct buffer once, plus wide coefficients as the governor
 * counts BigInts. The amount charged per frame.
 */
export function frameByteLength(payload: UnnumberedPayload): number {
  let bytes = 0;
  for (const buffer of frameTransferList(payload)) bytes += buffer.byteLength;
  let counted: Set<readonly bigint[]> | null = null;
  for (const flat of flats(payload)) {
    const data = flat.data;
    if (data.layout === "decimal" && data.coefficients.repr === "wide") {
      counted ??= new Set();
      if (counted.has(data.coefficients.values)) continue;
      counted.add(data.coefficients.values);
      for (const value of data.coefficients.values) bytes += bigintBytes(value);
    }
  }
  return bytes;
}
