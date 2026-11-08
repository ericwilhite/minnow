/** Client-safe frame ownership validation. No executor/channel implementation is loaded. */
import type { VectorData } from "../vector/vector.js";
import type { UnnumberedPayload, WireFlat } from "./result.js";

/** Thrown when a payload would transfer bytes it does not own, or a shared buffer. */
export class FrameBufferError extends Error {
  readonly code = "MINNOW_FRAME_BUFFER";
  constructor(message: string) {
    super(message);
    this.name = "FrameBufferError";
  }
}

type TypedArray =
  Uint8Array | Uint16Array | Uint32Array | Int32Array | Float32Array | Float64Array | BigInt64Array;

function dataArrays(data: VectorData, buffers: Set<ArrayBuffer>): void {
  switch (data.layout) {
    case "bool":
    case "i32":
    case "f64":
    case "f32":
      add(data.values, buffers);
      return;
    case "int64":
      add(data.values.values, buffers);
      return;
    case "decimal": {
      if (data.scales !== null) add(data.scales, buffers);
      if (data.specials !== null) add(data.specials, buffers);
      const coefficients = data.coefficients;
      if (coefficients.repr === "int128") add(coefficients.limbs, buffers);
      else if (coefficients.repr !== "wide") add(coefficients.values, buffers);
      return;
    }
    case "varlen":
      add(data.bytes, buffers);
      add(data.offsets, buffers);
      return;
    case "fixed16":
      add(data.bytes, buffers);
      return;
    case "interval":
      add(data.months, buffers);
      add(data.days, buffers);
      add(data.micros.values, buffers);
      return;
  }
}

export function flats(payload: UnnumberedPayload): WireFlat[] {
  const out: WireFlat[] = payload.dictionaries.map((delta) => delta.values);
  for (const column of payload.columns)
    if (column.form === "flat") out.push(column);
    else if (column.form === "constant") out.push(column.value);
  return out;
}

function add(view: TypedArray, buffers: Set<ArrayBuffer>): void {
  const buffer = view.buffer;
  if (!(buffer instanceof ArrayBuffer))
    throw new FrameBufferError("Frame buffers must be ArrayBuffers, never SharedArrayBuffer");
  if (view.byteOffset !== 0 || view.byteLength !== buffer.byteLength)
    throw new FrameBufferError(
      `A frame view covers ${String(view.byteLength)} of its buffer's ${String(buffer.byteLength)} bytes`,
    );
  buffers.add(buffer);
}
function flat(column: WireFlat, buffers: Set<ArrayBuffer>): void {
  dataArrays(column.data, buffers);
  if (column.validity !== null) add(column.validity, buffers);
}

/**
 * The distinct ArrayBuffers of a payload, for `postMessage`'s transfer list. Throws
 * `FrameBufferError` if a view does not cover its whole buffer or the buffer is shared. Wide
 * decimal coefficients (`bigint[]`) are structured-cloned, not transferred.
 */
export function frameTransferList(payload: UnnumberedPayload): ArrayBuffer[] {
  const buffers = new Set<ArrayBuffer>();
  for (const delta of payload.dictionaries) flat(delta.values, buffers);
  for (const column of payload.columns) {
    if (column.form === "flat") flat(column, buffers);
    else if (column.form === "constant") flat(column.value, buffers);
  }
  for (const column of payload.columns) {
    if (column.form !== "dictionary") continue;
    add(column.codes, buffers);
    if (column.validity !== null) add(column.validity, buffers);
  }
  return [...buffers];
}
