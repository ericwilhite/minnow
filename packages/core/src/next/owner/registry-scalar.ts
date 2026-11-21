/** One scalar in a root-owned directory record. Fixed lanes stay inline; only variable
 * payloads use the shared fragmented stream. No SQL/JS value conversion is involved. */
import { bigintBytes } from "../memory/governor.js";
import { SpillCorruptionError } from "../spill/contract.js";
import { layoutOf, type PhysicalType } from "../types.js";
import type { ByteCharge } from "../vector/varlen.js";
import type { MaybePromise } from "../access/key-index.js";
import {
  allocateData,
  dataBytes,
  type FlatVector,
  type Int64Values,
  type Vector,
  type VectorData,
} from "../vector/vector.js";
import { sourceSlot } from "../write/columns.js";
import type { RegistryFragments, RegistryPayload } from "./registry-fragments.js";

const INLINE = 64;
const LAYOUTS = [
  "bool",
  "i32",
  "f32",
  "f64",
  "int64",
  "decimal",
  "varlen",
  "fixed16",
  "interval",
] as const;
const viewOf = (bytes: Uint8Array) =>
  new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
function corrupt(): never {
  throw new SpillCorruptionError("Invalid registry scalar");
}
function take(charge: ByteCharge, bytes: number): void {
  if (!charge.allocate(bytes)) throw new RangeError("Registry scalar workspace must be admitted");
}
const raw = (array: ArrayBufferView, start: number, length: number) =>
  new Uint8Array(array.buffer, array.byteOffset + start, length);

/** Capture scratch beyond the fixed record/fragment buffers. The host prepays this while
 * reserving the source step, so storing a wide coefficient never waits on its own borrow. */
export function registryScalarStoreBytes(vector: Vector, row: number): number {
  const slot: { flat: FlatVector | null; index: number } = { flat: null, index: -1 };
  sourceSlot(vector, row, slot);
  if (
    slot.index < 0 ||
    slot.flat.data.layout !== "decimal" ||
    slot.flat.data.coefficients.repr !== "wide"
  )
    return 0;
  return 4 * (bigintBytes(slot.flat.data.coefficients.values[slot.index] ?? 0n) + 17);
}

/** Bytes 0..15 belong to the caller's linked-record identity; this codec owns 16..127.
 * The input remains borrowed through completion, including cancellation/failed I/O. */
export async function storeRegistryScalar(
  record: Uint8Array,
  vector: Vector,
  row: number,
  fragments: RegistryFragments,
  charge: ByteCharge,
): Promise<void> {
  record.fill(0, 16);
  const view = viewOf(record),
    slot: { flat: FlatVector | null; index: number } = { flat: null, index: -1 };
  sourceSlot(vector, row, slot);
  const data = slot.flat.data,
    at = slot.index;
  view.setUint8(23, LAYOUTS.indexOf(data.layout));
  if (at < 0) return;
  view.setUint8(16, 1);
  let payload: RegistryPayload | null = null;
  const inline = (array: ArrayBufferView, start: number, length: number, offset = INLINE) =>
    record.set(raw(array, start, length), offset);
  const int64 = (values: Int64Values, offset = INLINE) => {
    view.setUint8(17, values.repr === "safe" ? 0 : 1);
    inline(values.values, at * 8, 8, offset);
  };
  switch (data.layout) {
    case "bool":
      inline(data.values, at, 1);
      break;
    case "i32":
    case "f32":
      inline(data.values, at * 4, 4);
      break;
    case "f64":
      inline(data.values, at * 8, 8);
      break;
    case "int64":
      int64(data.values);
      break;
    case "fixed16":
      inline(data.bytes, at * 16, 16);
      break;
    case "interval":
      inline(data.months, at * 4, 4);
      inline(data.days, at * 4, 4, INLINE + 4);
      int64(data.micros, INLINE + 8);
      break;
    case "varlen": {
      const from = data.offsets[at] ?? 0,
        to = data.offsets[at + 1] ?? from;
      payload = await fragments.append(to - from, (into, position) => {
        into.set(data.bytes.subarray(from + position, from + position + into.length));
      });
      break;
    }
    case "decimal": {
      view.setUint8(20, data.scale === null ? 1 : 0);
      view.setUint16(18, data.scale ?? data.scales?.[at] ?? 0, true);
      view.setUint8(21, data.specials?.[at] ?? 0);
      view.setUint8(22, data.specials === null ? 0 : 1);
      const coefficient = data.coefficients;
      if (coefficient.repr === "safe" || coefficient.repr === "int64") int64(coefficient);
      else if (coefficient.repr === "int128") {
        view.setUint8(17, 2);
        inline(coefficient.limbs, at * 16, 16);
      } else {
        view.setUint8(17, 3);
        const value = coefficient.values[at] ?? 0n,
          bytes = bigintBytes(value) - 16 + 1;
        // Shifts create transient BigInts. Prepay both operands and their arithmetic result;
        // source ownership is borrowed, not transferred to this scratch codec.
        const workspace = 4 * (bytes + 32);
        take(charge, workspace);
        try {
          payload = await fragments.append(bytes, (into, position) => {
            const out = viewOf(into);
            let i = 0;
            for (; i + 8 <= into.length; i += 8)
              out.setBigUint64(i, BigInt.asUintN(64, value >> BigInt((position + i) * 8)), true);
            for (; i < into.length; i++)
              into[i] = Number((value >> BigInt((position + i) * 8)) & 255n);
          });
        } finally {
          charge.free(workspace);
        }
      }
      break;
    }
  }
  if (payload) {
    view.setFloat64(24, payload.length, true);
    view.setFloat64(32, payload.tail?.offset ?? -1, true);
    view.setUint32(40, payload.tail?.length ?? 0, true);
    view.setUint32(44, payload.tail?.checksum ?? 0, true);
  }
}

/** Retained scalar buffers only. Decode scratch is shared across columns, not multiplied
 * by the projected width. Unrequested payloads cost zero. */
export function registryScalarBytes(record: Uint8Array, type: PhysicalType): number {
  const view = viewOf(record),
    layout = layoutOf(type),
    valid = view.getUint8(16),
    repr = view.getUint8(17),
    length = view.getFloat64(24, true);
  if (
    view.getUint8(23) !== LAYOUTS.indexOf(layout) ||
    valid > 1 ||
    !Number.isSafeInteger(length) ||
    length < 0 ||
    length > 0x7fffffff ||
    (layout !== "varlen" && !(layout === "decimal" && repr === 3) && length !== 0) ||
    (layout === "decimal" &&
      (repr > 3 || view.getUint8(20) > 1 || view.getUint8(22) > 1 || view.getUint8(21) > 3)) ||
    ((layout === "int64" || layout === "interval") && repr > 1)
  )
    corrupt();
  if (!valid) return dataBytes(type, 1) + 4;
  if (layout === "decimal") {
    const coefficient = repr <= 1 ? 8 : repr === 2 ? 16 : length + 23;
    if (repr === 3 && length < 1) corrupt();
    return coefficient + (view.getUint8(20) ? 2 : 0) + (view.getUint8(22) ? 1 : 0);
  }
  return dataBytes(type, 1) + length;
}

/** Exact writer sizing from the scalar header. NULL and fixed-width cells need no
 * variable payload credit; measuring never opens a fragment or decodes a value. */
export function registryScalarVarlenBytes(record: Uint8Array, type: PhysicalType): number {
  const bytes = registryScalarBytes(record, type);
  return layoutOf(type) === "varlen" && viewOf(record).getUint8(16) === 1
    ? bytes - dataBytes(type, 1)
    : 0;
}

/** One decode step, released before the next column. Includes the encoded coefficient and
 * simultaneous arithmetic temporaries; the resulting BigInt is in registryScalarBytes. */
export function registryScalarWorkspaceBytes(record: Uint8Array, type: PhysicalType): number {
  registryScalarBytes(record, type);
  const view = viewOf(record);
  if (view.getUint8(16) === 0 || layoutOf(type) !== "decimal" || view.getUint8(17) !== 3) return 0;
  const length = view.getFloat64(24, true);
  return length + 3 * (length + 32);
}

/**
 * What one trigger binding copy of this stored value takes (exec/trigger-bindings.ts): its decoded
 * bytes (`registryScalarBytes`) plus a binding's own offsets and validity word. Known from the
 * record's header before anything is decoded, so the image read funds the copy with the image
 * (owner/registry-images.ts), and the copy charges exactly this: one function for both.
 */
export function bindingCopyBytes(record: Uint8Array, type: PhysicalType): number {
  return registryScalarBytes(record, type) + BINDING_COPY_OVERHEAD_BYTES;
}
/** A binding copy's offsets pair and validity word beyond the decoded value. */
export const BINDING_COPY_OVERHEAD_BYTES = 8 + 4;

/** The caller owns/refunds registryScalarBytes, and drops the vector before returning it.
 * Allocate only after that reservation; fixed lanes never alias the reusable record buffer. */
export async function loadRegistryScalar(
  record: Uint8Array,
  type: PhysicalType,
  fragments: RegistryFragments,
  charge: ByteCharge,
  checkpoint: () => MaybePromise<void>,
  signal?: AbortSignal,
): Promise<FlatVector> {
  registryScalarBytes(record, type);
  const view = viewOf(record),
    layout = layoutOf(type),
    valid = view.getUint8(16) === 1,
    repr = view.getUint8(17),
    length = view.getFloat64(24, true);
  if (!valid)
    return {
      form: "flat",
      type,
      length: 1,
      validity: new Uint32Array(1),
      data: allocateData(type, 1),
    };
  const fixed =
    layout === "varlen" || (layout === "decimal" && repr === 3)
      ? 0
      : layout === "decimal"
        ? repr === 2
          ? 16
          : 8
        : dataBytes(type, 1);
  const bytes = record.slice(INLINE, INLINE + fixed);
  const payload = async () => {
    const output = new Uint8Array(length),
      offset = view.getFloat64(32, true),
      size = view.getUint32(40, true),
      checksum = view.getUint32(44, true);
    await fragments.readInto(
      { length, tail: offset === -1 ? null : { offset, length: size, checksum } },
      output,
      signal,
    );
    return output;
  };
  const int64 = (offset = 0): Int64Values =>
    repr === 0
      ? { repr: "safe", values: new Float64Array(bytes.buffer, offset, 1) }
      : { repr: "int64", values: new BigInt64Array(bytes.buffer, offset, 1) };
  let data: VectorData;
  switch (layout) {
    case "bool":
      data = { layout, values: bytes.subarray(0, 1) };
      break;
    case "i32":
      data = { layout, values: new Int32Array(bytes.buffer, 0, 1) };
      break;
    case "f32":
      data = { layout, values: new Float32Array(bytes.buffer, 0, 1) };
      break;
    case "f64":
      data = { layout, values: new Float64Array(bytes.buffer, 0, 1) };
      break;
    case "fixed16":
      data = { layout, bytes };
      break;
    case "int64":
      data = { layout, values: int64() };
      break;
    case "interval":
      data = {
        layout,
        months: new Int32Array(bytes.buffer, 0, 1),
        days: new Int32Array(bytes.buffer, 4, 1),
        micros: int64(8),
      };
      break;
    case "varlen":
      data = { layout, bytes: await payload(), offsets: Int32Array.of(0, length) };
      break;
    case "decimal": {
      let coefficients: Extract<VectorData, { layout: "decimal" }>["coefficients"];
      if (repr < 2) coefficients = int64();
      else if (repr === 2)
        coefficients = { repr: "int128", limbs: new Uint32Array(bytes.buffer, 0, 4) };
      else {
        const workspace = registryScalarWorkspaceBytes(record, type);
        take(charge, workspace);
        try {
          const encoded = await payload(),
            encodedView = viewOf(encoded);
          let value = (encoded[length - 1] ?? 0) & 128 ? -1n : 0n,
            end = length;
          while (end % 8) {
            value = (value << 8n) | BigInt(encoded[--end] ?? 0);
          }
          while (end) {
            if (end % 65536 < 8) await checkpoint();
            end -= 8;
            value = (value << 64n) | encodedView.getBigUint64(end, true);
          }
          coefficients = { repr: "wide", values: [value] };
        } finally {
          charge.free(workspace);
        }
      }
      const scale = view.getUint16(18, true);
      data = {
        layout,
        coefficients,
        scale: view.getUint8(20) ? null : scale,
        scales: view.getUint8(20) ? Uint16Array.of(scale) : null,
        specials: view.getUint8(22) ? Uint8Array.of(view.getUint8(21)) : null,
      };
      break;
    }
  }
  return { form: "flat", type, length: 1, validity: null, data };
}
