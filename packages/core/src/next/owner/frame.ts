/**
 * Columnar data on the wire: result frames (owner → client) and batch-write input (client →
 * owner).
 *
 * ## Result frames (lead ruling R3)
 * - The executor (B4) produces each frame **wire-ready**: a `ResultFrame` payload of plain data
 *   (numbers, typed arrays, `bigint[]` for wide decimals) plus its transfer list. The release
 *   handle lives beside the payload (owner/engine.ts `StreamedFrame`), never inside it, so the
 *   payload is postMessage-safe. The owner posts it as is: no second compaction, no copy. The
 *   port adds no copy either: every typed-array buffer is transferred (`frameTransferables`
 *   checks a payload owns exactly what it transfers). Wide coefficients are cloned.
 * - Vectors keep their executor form (`VectorData`), so the client sees exact physical values:
 *   int64 as proven-safe `Float64Array` or `BigInt64Array`, the decimal tiers, UTF-8 bytes,
 *   dictionary codes. JS values are built on the client, on request (api.ts).
 * - **Accounting.** A frame stays charged to its result grant from the moment the executor
 *   offers it until the client's `release` signal for it reaches the owner (protocol.ts
 *   `Release`), not until `postMessage` returns: the client's backlog is bounded by the stream's
 *   window too. On disconnect (`bye`, step-down, a liveness grant, an abandoned non-live stream)
 *   the owner releases every outstanding frame of the connection and cancels its streams.
 * - **Dictionaries are per stream** (rulings 3 and R3). A dictionary column names a stream
 *   dictionary id and may carry a `delta`: entries appended to that dictionary, starting at code
 *   `delta.base`, which must equal the dictionary's current size. Codes index the cumulative
 *   dictionary. The executor stops adding entries once a stream's dictionaries reach
 *   `STREAM_DICTIONARY_LIMIT_BYTES` and sends that column as plain varlen from then on. The
 *   client keeps the dictionaries for the life of the result (`StreamDictionaries`).
 * - `sequence` numbers a stream's frames 0, 1, 2…; the client's `release` names them, and a gap
 *   is a protocol error.
 *
 * At integration this payload type moves to exec/result.ts (exec/ cannot import owner/), and
 * this module re-exports it. The shape here is the one both sides build against.
 *
 * ## Batch-write input (ruling R9)
 * Input columns are **structured-cloned, not transferred**: the client keeps them until the
 * request's terminal reply so it can resend them after a reconnect. One clone costs ~4–7 ms per
 * MiB (probe), paid once per write.
 */
import type { VectorData } from "../vector/vector.js";
import type { WireValue } from "./protocol.js";

/** Bytes of dictionary entries a stream may accumulate before columns go plain. */
export const STREAM_DICTIONARY_LIMIT_BYTES = 1024 * 1024;

/** A flat column without its type (the type travels once, in `WireColumnMeta`). */
export interface WireFlat {
  readonly form: "flat";
  /** vector/vector.ts `Validity` (bit set = valid), or null when all are valid. */
  readonly validity: Uint32Array | null;
  readonly data: VectorData;
}

/** One value for every row of the frame: `value` has one slot. */
export interface WireConstant {
  readonly form: "constant";
  readonly value: WireFlat;
}

export interface WireDictionaryColumn {
  readonly form: "dictionary";
  readonly codes: Int32Array;
  readonly validity: Uint32Array | null;
  /** Stream dictionary id: unique within the result, stable across its frames. */
  readonly dictionary: number;
  /** New entries, appended at code `base` (the dictionary's size before this frame). */
  readonly delta: { readonly base: number; readonly entries: WireFlat } | null;
}

export type WireColumn = WireFlat | WireConstant | WireDictionaryColumn;

/** One frame of a result: plain data, postMessage-safe. */
export interface ResultFrame {
  /** 0, 1, 2… within the stream. */
  readonly sequence: number;
  readonly rowCount: number;
  /** One per output column, in output order, each with `rowCount` rows. */
  readonly columns: readonly WireColumn[];
}

/** Client-encoded input for a batch write: one per input column, `rowCount` slots each. Cloned. */
export type WireInputColumn =
  | { readonly kind: "bool"; readonly values: Uint8Array; readonly validity: Uint32Array | null }
  | { readonly kind: "i32"; readonly values: Int32Array; readonly validity: Uint32Array | null }
  | { readonly kind: "f64"; readonly values: Float64Array; readonly validity: Uint32Array | null }
  | {
      readonly kind: "i64";
      readonly values: BigInt64Array;
      readonly validity: Uint32Array | null;
    }
  | {
      readonly kind: "utf8";
      /** Value i is `bytes[offsets[i] … offsets[i + 1])`. */
      readonly bytes: Uint8Array;
      readonly offsets: Int32Array;
      readonly validity: Uint32Array | null;
    }
  | { readonly kind: "values"; readonly values: readonly WireValue[] };

/** A frame payload that would transfer bytes it does not own, or shared memory. */
export class FrameBufferError extends Error {
  readonly code = "MINNOW_FRAME_BUFFER";
  constructor(message: string) {
    super(message);
    this.name = "FrameBufferError";
  }
}

type View = ArrayBufferView & { readonly buffer: ArrayBufferLike };

function addView(view: View | null, into: Set<ArrayBuffer>): void {
  if (view === null) return;
  const buffer = view.buffer;
  if (!(buffer instanceof ArrayBuffer))
    throw new FrameBufferError("Frame buffers must be ArrayBuffers, never SharedArrayBuffer");
  if (into.has(buffer)) return;
  if (view.byteOffset !== 0 || view.byteLength !== buffer.byteLength)
    throw new FrameBufferError(
      `A frame view covers ${String(view.byteLength)} of its buffer's ${String(buffer.byteLength)} bytes: transferring it would move bytes the frame does not own`,
    );
  into.add(buffer);
}

function addData(data: VectorData, into: Set<ArrayBuffer>): void {
  switch (data.layout) {
    case "bool":
    case "i32":
    case "f64":
    case "f32":
      addView(data.values, into);
      return;
    case "int64":
      addView(data.values.values, into);
      return;
    case "decimal":
      addView(data.scales, into);
      addView(data.specials, into);
      if (data.coefficients.repr === "int128") addView(data.coefficients.limbs, into);
      else if (data.coefficients.repr !== "wide") addView(data.coefficients.values, into);
      return;
    case "varlen":
      addView(data.bytes, into);
      addView(data.offsets, into);
      return;
    case "fixed16":
      addView(data.bytes, into);
      return;
    case "interval":
      addView(data.months, into);
      addView(data.days, into);
      addView(data.micros.values, into);
      return;
  }
}

function addFlat(flat: WireFlat, into: Set<ArrayBuffer>): void {
  addView(flat.validity, into);
  addData(flat.data, into);
}

/**
 * Every ArrayBuffer a frame references, once: what its transfer list must be. Throws
 * `FrameBufferError` if a view does not cover its whole buffer or a buffer is shared. The
 * executor's transfer list is checked against this in tests (and in debug builds).
 */
export function frameTransferables(frame: ResultFrame): ArrayBuffer[] {
  const buffers = new Set<ArrayBuffer>();
  for (const column of frame.columns) {
    if (column.form === "flat") addFlat(column, buffers);
    else if (column.form === "constant") addFlat(column.value, buffers);
    else {
      addView(column.codes, buffers);
      addView(column.validity, buffers);
      if (column.delta !== null) addFlat(column.delta.entries, buffers);
    }
  }
  return [...buffers];
}

/** Rows of a flat column's data (the length of its first dimension). */
function entryCount(flat: WireFlat): number {
  const data = flat.data;
  switch (data.layout) {
    case "varlen":
      return data.offsets.length - 1;
    case "fixed16":
      return data.bytes.length / 16;
    case "int64":
      return data.values.values.length;
    case "interval":
      return data.months.length;
    case "decimal":
      return data.coefficients.repr === "int128"
        ? data.coefficients.limbs.length / 4
        : data.coefficients.values.length;
    default:
      return data.values.length;
  }
}

/**
 * The client's per-stream dictionaries: deltas appended in frame order, entries kept as the
 * chunks that arrived (no copy). A delta whose `base` is not the dictionary's current size is a
 * protocol error (a lost or reordered frame).
 */
export class StreamDictionaries {
  readonly #chunks = new Map<number, { size: number; readonly parts: WireFlat[] }>();

  /** Applies the deltas of `frame`, in column order. */
  apply(frame: ResultFrame): void {
    for (const column of frame.columns) {
      if (column.form !== "dictionary" || column.delta === null) continue;
      let dictionary = this.#chunks.get(column.dictionary);
      if (dictionary === undefined) {
        dictionary = { size: 0, parts: [] };
        this.#chunks.set(column.dictionary, dictionary);
      }
      if (column.delta.base !== dictionary.size)
        throw new RangeError(
          `Dictionary ${String(column.dictionary)} delta starts at ${String(column.delta.base)}, expected ${String(dictionary.size)}`,
        );
      dictionary.parts.push(column.delta.entries);
      dictionary.size += entryCount(column.delta.entries);
    }
  }

  /** Entries of dictionary `id` so far. */
  size(id: number): number {
    return this.#chunks.get(id)?.size ?? 0;
  }

  /** The chunk holding `code`, and the code's index inside it. */
  locate(id: number, code: number): { readonly entries: WireFlat; readonly index: number } {
    const dictionary = this.#chunks.get(id);
    let index = code;
    for (const entries of dictionary?.parts ?? []) {
      const count = entryCount(entries);
      if (index < count) return { entries, index };
      index -= count;
    }
    throw new RangeError(`Code ${String(code)} is outside dictionary ${String(id)}`);
  }
}
