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

function dataArrays(data: VectorData, into: TypedArray[]): void {
  switch (data.layout) {
    case "bool":
    case "i32":
    case "f64":
    case "f32":
      into.push(data.values);
      return;
    case "int64":
      into.push(data.values.values);
      return;
    case "decimal": {
      if (data.scales !== null) into.push(data.scales);
      if (data.specials !== null) into.push(data.specials);
      const coefficients = data.coefficients;
      if (coefficients.repr === "int128") into.push(coefficients.limbs);
      else if (coefficients.repr !== "wide") into.push(coefficients.values);
      return;
    }
    case "varlen":
      into.push(data.bytes, data.offsets);
      return;
    case "fixed16":
      into.push(data.bytes);
      return;
    case "interval":
      into.push(data.months, data.days, data.micros.values);
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

function payloadArrays(payload: UnnumberedPayload): TypedArray[] {
  const arrays: TypedArray[] = [];
  for (const flat of flats(payload)) {
    dataArrays(flat.data, arrays);
    if (flat.validity !== null) arrays.push(flat.validity);
  }
  for (const column of payload.columns)
    if (column.form === "dictionary") {
      arrays.push(column.codes);
      if (column.validity !== null) arrays.push(column.validity);
    }
  return arrays;
}

/**
 * The distinct ArrayBuffers of a payload, for `postMessage`'s transfer list. Throws
 * `FrameBufferError` if a view does not cover its whole buffer or the buffer is shared. Wide
 * decimal coefficients (`bigint[]`) are structured-cloned, not transferred.
 */
export function frameTransferList(payload: UnnumberedPayload): ArrayBuffer[] {
  const buffers = new Set<ArrayBuffer>();
  for (const view of payloadArrays(payload)) {
    const buffer = view.buffer;
    if (!(buffer instanceof ArrayBuffer))
      throw new FrameBufferError("Frame buffers must be ArrayBuffers, never SharedArrayBuffer");
    if (view.byteOffset !== 0 || view.byteLength !== buffer.byteLength)
      throw new FrameBufferError(
        `A frame view covers ${String(view.byteLength)} of its buffer's ${String(buffer.byteLength)} bytes`,
      );
    buffers.add(buffer);
  }
  return [...buffers];
}
