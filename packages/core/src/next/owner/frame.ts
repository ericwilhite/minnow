/**
 * Columnar frames on the wire: result frames (owner → client) and input columns (client →
 * owner). Both move by **transfer**: the sender gives up its ArrayBuffers and the port adds no
 * copy. SharedArrayBuffer is never used.
 *
 * ## Result frames
 * - A frame is one result batch (vector/vector.ts): at most `BATCH_ROWS` rows and about
 *   `OwnerLimits.frameBytes` bytes; a single larger row is a frame of one.
 * - Vectors keep their executor form: `VectorData` is sent as is, so the client sees the exact
 *   physical values (int64 as proven-safe Float64Array or BigInt64Array, decimal tiers, UTF-8
 *   bytes, dictionary codes). Conversion to JS values happens on the client, on request (api.ts).
 * - **Ownership.** The result sink (owner/engine.ts `FrameSink`) builds each frame into buffers it
 *   allocates for that frame alone, exactly sized, and transfers them. This compaction (applying
 *   the selection, cutting capacity slack) is the one copy a result row takes after the operator
 *   that produced it; the port adds none. A batch handed to the sink by `transfer`, with no
 *   selection and exact buffers, is sent as is and its `release` tells the producer the buffers
 *   are gone (not returned to its pool).
 * - **Dictionaries** are immutable for the life of a query (vector/vector.ts rule 2) and still
 *   referenced by the owner, so they are copied, once per result, into a buffer the frame owns.
 *   Each gets a wire id unique within the result and travels in the first frame that uses it;
 *   later frames refer to it by id. The client keeps them until the result is released.
 * - **Accounting.** A frame's bytes are charged to the statement's grant until the postMessage
 *   that transfers them returns, then freed: frames in flight live in the client's heap, bounded
 *   by its credits.
 *
 * ## Input columns
 * - The client encodes a batch write's rows into typed buffers it allocates, so it transfers
 *   them. Caller-provided typed arrays (api.ts `ColumnarInput`) are transferred only when the
 *   caller passes `transfer: true`; otherwise they are cloned, never silently detached.
 * - `values` is the fallback for columns of mixed or non-primitive JS values: cloned, converted
 *   on the owner like parameters.
 */
import type { VectorData } from "../vector/vector.js";
import type { WireValue } from "./protocol.js";

/** A flat vector without its type (the type travels once, in `WireColumnMeta`). */
export interface WireFlat {
  readonly form: "flat";
  readonly length: number;
  /** vector/vector.ts `Validity` (bit set = valid), or null when all are valid. */
  readonly validity: Uint32Array | null;
  readonly data: VectorData;
}

export interface WireConstant {
  readonly form: "constant";
  readonly length: number;
  /** Length 1. */
  readonly value: WireFlat;
}

export interface WireDictionaryVector {
  readonly form: "dictionary";
  readonly length: number;
  readonly codes: Int32Array;
  readonly validity: Uint32Array | null;
  /** Wire id of a dictionary sent in this frame or an earlier frame of the same result. */
  readonly dictionary: number;
}

export type WireVector = WireFlat | WireConstant | WireDictionaryVector;

export interface ResultFrame {
  readonly rowCount: number;
  /** One per output column, in output order; each has `length === rowCount`. */
  readonly columns: readonly WireVector[];
  /** Dictionaries first used by this frame. */
  readonly dictionaries: ReadonlyArray<{ readonly id: number; readonly values: WireFlat }>;
}

/** Client-encoded input for a batch write: one per input column, `rowCount` slots each. */
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

/** A typed array whose buffer can be transferred. */
type View = ArrayBufferView & { readonly buffer: ArrayBufferLike };

/** Thrown when a frame would transfer bytes it does not use, or a shared buffer. */
export class FrameBufferError extends Error {
  readonly code = "MINNOW_FRAME_BUFFER";
  constructor(message: string) {
    super(message);
    this.name = "FrameBufferError";
  }
}

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
 * The transfer list of a result frame: every ArrayBuffer it references, once. Throws
 * `FrameBufferError` if a view does not cover its whole buffer (the sink must allocate exactly)
 * or a buffer is shared, so a frame can never move bytes it does not own.
 */
export function frameTransferables(frames: readonly ResultFrame[]): ArrayBuffer[] {
  const buffers = new Set<ArrayBuffer>();
  for (const frame of frames) {
    for (const dictionary of frame.dictionaries) addFlat(dictionary.values, buffers);
    for (const column of frame.columns) {
      if (column.form === "flat") addFlat(column, buffers);
      else if (column.form === "constant") addFlat(column.value, buffers);
      else {
        addView(column.codes, buffers);
        addView(column.validity, buffers);
      }
    }
  }
  return [...buffers];
}

/** The transfer list of encoded input columns (`values` columns are cloned). */
export function inputTransferables(columns: readonly WireInputColumn[]): ArrayBuffer[] {
  const buffers = new Set<ArrayBuffer>();
  for (const column of columns) {
    if (column.kind === "values") continue;
    if (column.kind === "utf8") {
      addView(column.bytes, buffers);
      addView(column.offsets, buffers);
    } else addView(column.values, buffers);
    addView(column.validity, buffers);
  }
  return [...buffers];
}
