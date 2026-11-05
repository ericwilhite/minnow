/**
 * Streaming results: columnar frames of bounded bytes, pulled by the consumer under a credit
 * window, with no row-count cap.
 *
 * ## Frames
 * - A `ResultFrame` is a dense run of result rows: one `FrameColumn` per output column (the
 *   stream's `columns` give names and types once), no selection, `rowCount` rows.
 * - A frame's buffers belong to that frame alone, sized to its rows, so the owner can post them
 *   with `frameTransferList(frame)` as the transfer list (zero copy; the owner's views detach).
 * - Text stays UTF-8 (`varlen`), dictionary columns stay codes plus the dictionary entries the
 *   frame needs, constants stay one value. The client builds JS values and row objects from them,
 *   only on request (PROPOSAL "Public API": columnar-first results).
 * - Size: the result sink seals a frame at about `RESULT_FRAME_TARGET_BYTES`, or earlier when the
 *   consumer is waiting with nothing queued (`demand`), so the first rows of `SELECT *` leave at
 *   the first batch while a fast producer still sends large frames. A single row larger than the
 *   target is a frame of one row.
 *
 * ## Credits and memory
 * - A frame is charged to the result sink's grant from the moment it is offered until the
 *   consumer calls `release()` (idempotent). Unreleased frame bytes, queued or handed out, are the
 *   stream's **outstanding** bytes; while they reach `windowBytes`, the sink's `push` returns
 *   `blocked` (exec/pipeline.ts) and the whole pipeline pauses. A slow consumer costs at most one
 *   window, never a growing buffer.
 * - The consumer (the owner's protocol layer) releases a frame when the client has taken it: on
 *   the client's credit message, not on `postMessage`, so the client's backlog is bounded too.
 *   A consumer that holds frames without releasing them, and then waits for `next()`, deadlocks
 *   itself once it holds a window; that is a consumer bug.
 *
 * ## End, errors, cancellation
 * - `next()` returns frames in order, then `null` after the last. One `next()` at a time.
 * - On an execution error, frames already queued are still delivered (as in PostgreSQL, a client
 *   may have seen rows before the error), then `next()` rejects with the error.
 * - `cancel(reason)` by the consumer: queued frames are released, the producer's pending
 *   `ready()` rejects with `QueryCancelledError`, and the execution is cancelled. Frames already
 *   handed out stay the consumer's to release. After `cancel`, `next()` rejects with
 *   `QueryCancelledError`. Cancelling a finished stream only releases its queue.
 */
import type { OutputColumn } from "../ir/ir.js";
import { bigintBytes } from "../memory/governor.js";
import { BATCH_TARGET_BYTES, type Validity, type VectorData } from "../vector/vector.js";
import { QueryCancelledError } from "./errors.js";
import type { PushResult } from "./pipeline.js";

/** About one batch's bytes: large enough to amortize a 55–280 µs cross-tab message. */
export const RESULT_FRAME_TARGET_BYTES = BATCH_TARGET_BYTES;
/** Default credit window: two frames, so the pipeline fills one while the other is in flight. */
export const RESULT_WINDOW_BYTES = 2 * RESULT_FRAME_TARGET_BYTES;

export interface FlatFrameColumn {
  readonly form: "flat";
  readonly validity: Validity | null;
  readonly data: VectorData;
}

export type FrameColumn =
  | FlatFrameColumn
  /** One value for every row of the frame: `value` has length 1. */
  | { readonly form: "constant"; readonly value: FlatFrameColumn }
  /** Codes into `dictionary` (distinct, non-null entries; null is in `validity`). */
  | {
      readonly form: "dictionary";
      readonly codes: Int32Array;
      readonly validity: Validity | null;
      readonly dictionary: FlatFrameColumn;
    };

export interface ResultFrame {
  /** 0, 1, 2… within the stream, so a protocol can detect a lost frame. */
  readonly sequence: number;
  readonly rowCount: number;
  /** Bytes of the frame's buffers, the amount charged until `release`. */
  readonly byteLength: number;
  readonly columns: readonly FrameColumn[];
  /** Returns the frame's credit and its bytes. Idempotent. */
  release(): void;
}

export interface ResultStream {
  readonly columns: readonly OutputColumn[];
  /** The next frame, or null after the last; rejects with the execution's error. */
  next(): Promise<ResultFrame | null>;
  /** The consumer no longer wants results: cancels the execution. Idempotent. */
  cancel(reason?: unknown): void;
}

/** What the result sink offers: a sealed frame and how to free its bytes. */
export interface FrameOffer {
  readonly rowCount: number;
  readonly byteLength: number;
  readonly columns: readonly FrameColumn[];
  /** Frees the frame's bytes from the sink's grant (called once, on release). */
  readonly free: () => void;
}

export interface ResultChannelOptions {
  readonly windowBytes?: number;
  /** Called once when the consumer cancels; the execution aborts its pipelines. */
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
 * The credit window between the result sink (producer) and the owner (consumer): the reference
 * and production implementation of `ResultStream`.
 */
export class ResultChannel implements ResultStream {
  readonly windowBytes: number;
  readonly #onCancel: ((reason: unknown) => void) | undefined;
  readonly #queue: ResultFrame[] = [];
  #state: State = { kind: "open" };
  #outstanding = 0;
  #sequence = 0;
  #consumer: Waiter<ResultFrame | null> | null = null;
  #producer: Waiter<void> | null = null;

  constructor(
    readonly columns: readonly OutputColumn[],
    options: ResultChannelOptions = {},
  ) {
    this.windowBytes = options.windowBytes ?? RESULT_WINDOW_BYTES;
    this.#onCancel = options.onCancel;
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
   * result is `blocked` (so the producer meets the rejection in `ready()`).
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
  }

  /** The execution failed: queued frames are still delivered, then `next()` rejects. */
  fail(error: unknown): void {
    if (this.#state.kind !== "open") return;
    this.#state = {
      kind: "failed",
      error: error instanceof Error ? error : new Error(String(error), { cause: error }),
    };
    this.#settleConsumer();
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
    if (wasOpen) this.#onCancel?.(reason);
  }

  #frame(offer: FrameOffer): ResultFrame {
    let released = false;
    return {
      sequence: this.#sequence++,
      rowCount: offer.rowCount,
      byteLength: offer.byteLength,
      columns: offer.columns,
      release: () => {
        if (released) return;
        released = true;
        this.#outstanding -= offer.byteLength;
        offer.free();
        if (this.#outstanding < this.windowBytes && this.#producer !== null) {
          const producer = this.#producer;
          this.#producer = null;
          producer.resolve();
        }
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
}

type TypedArray =
  Uint8Array | Uint16Array | Uint32Array | Int32Array | Float32Array | Float64Array | BigInt64Array;

function dataArrays(data: VectorData): TypedArray[] {
  switch (data.layout) {
    case "bool":
    case "i32":
    case "f64":
    case "f32":
      return [data.values];
    case "int64":
      return [data.values.values];
    case "decimal": {
      const arrays: TypedArray[] = [];
      if (data.scales !== null) arrays.push(data.scales);
      if (data.specials !== null) arrays.push(data.specials);
      const coefficients = data.coefficients;
      if (coefficients.repr === "int128") arrays.push(coefficients.limbs);
      else if (coefficients.repr !== "wide") arrays.push(coefficients.values);
      return arrays;
    }
    case "varlen":
      return [data.bytes, data.offsets];
    case "fixed16":
      return [data.bytes];
    case "interval":
      return [data.months, data.days, data.micros.values];
  }
}

function flatArrays(column: FlatFrameColumn): TypedArray[] {
  const arrays = dataArrays(column.data);
  if (column.validity !== null) arrays.push(column.validity);
  return arrays;
}

function columnArrays(column: FrameColumn): TypedArray[] {
  switch (column.form) {
    case "flat":
      return flatArrays(column);
    case "constant":
      return flatArrays(column.value);
    case "dictionary": {
      const arrays = [column.codes, ...flatArrays(column.dictionary)];
      if (column.validity !== null) arrays.push(column.validity);
      return arrays;
    }
  }
}

/**
 * The distinct ArrayBuffers of a frame's columns: `postMessage(frame, frameTransferList(frame))`.
 * Wide decimal coefficients (`bigint[]`) are structured-cloned, not transferred.
 */
export function frameTransferList(columns: readonly FrameColumn[]): ArrayBuffer[] {
  const buffers = new Set<ArrayBuffer>();
  for (const column of columns)
    for (const array of columnArrays(column)) {
      const buffer = array.buffer;
      if (buffer instanceof ArrayBuffer) buffers.add(buffer);
    }
  return [...buffers];
}

function wideBytes(column: FlatFrameColumn): number {
  const data = column.data;
  if (data.layout !== "decimal" || data.coefficients.repr !== "wide") return 0;
  let bytes = 0;
  for (const value of data.coefficients.values) bytes += bigintBytes(value);
  return bytes;
}

/**
 * Bytes a frame's columns occupy: each distinct buffer once, by capacity, plus wide coefficients
 * as the governor counts BigInts. The amount the sink charges per frame.
 */
export function frameByteLength(columns: readonly FrameColumn[]): number {
  let bytes = 0;
  for (const buffer of frameTransferList(columns)) bytes += buffer.byteLength;
  for (const column of columns)
    bytes += wideBytes(
      column.form === "flat"
        ? column
        : column.form === "constant"
          ? column.value
          : column.dictionary,
    );
  return bytes;
}
