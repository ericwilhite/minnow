/** Byte-valued SQL arrays can compare element views without a second decoded tree.
 * Inputs have already been validated by storage/the grouping sort.
 * Header flags and NULL bitmap padding are representations, not SQL equality.
 * Other element families keep the shared numeric/interval semantic comparator.
 */
import type { PhysicalType } from "../../types.js";
import {
  arrayElementTag,
  MalformedArrayError,
  MAX_ARRAY_DIMENSIONS,
} from "../../vector/array-value.js";
import type { Vector } from "../../vector/vector.js";
import { isValidSlot, slotOf } from "../kernels/vectors.js";
export function byteArrayGroupKey(type: PhysicalType | undefined): boolean {
  return (
    type?.kind === "array" &&
    (type.element.kind === "text" || type.element.kind === "bytes" || type.element.kind === "enum")
  );
}
class Cursor {
  readonly view: DataView;
  at = 0;
  readonly shape: Uint8Array;
  readonly count: number;
  readonly nulls: Uint8Array | null;
  constructor(
    readonly bytes: Uint8Array,
    element: PhysicalType,
  ) {
    this.view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
    if (
      this.view.getUint8(this.take(1)) !== 1 ||
      this.view.getUint8(this.take(1)) !== arrayElementTag(element)
    )
      throw new MalformedArrayError("Bad grouping array header");
    const dimensions = this.view.getUint8(this.take(1)),
      flags = this.view.getUint8(this.take(1));
    if (dimensions > MAX_ARRAY_DIMENSIONS || (flags & ~1) !== 0)
      throw new MalformedArrayError("Bad grouping array dimensions");
    const start = this.at;
    let count = dimensions === 0 ? 0 : 1;
    for (let d = 0; d < dimensions; d++) {
      const length = this.view.getInt32(this.take(4), true);
      this.take(4); // Lower bounds participate in SQL array equality.
      if (length < 0) throw new MalformedArrayError("Negative grouping array dimension");
      count *= length;
    }
    if (count > bytes.length * 8)
      throw new MalformedArrayError("Grouping array dimensions exceed value");
    this.shape = bytes.subarray(start, this.at);
    this.count = count;
    const bitmap = (flags & 1) === 0 ? 0 : Math.ceil(count / 8);
    this.nulls = bitmap ? bytes.subarray(this.take(bitmap), this.at) : null;
  }
  take(bytes: number): number {
    if (bytes < 0 || this.at + bytes > this.bytes.length)
      throw new MalformedArrayError("Grouping array is truncated");
    const start = this.at;
    this.at += bytes;
    return start;
  }
  next(index: number): Uint8Array | null {
    if (this.nulls && ((this.nulls[index >>> 3] ?? 0) & (1 << (index & 7))) !== 0) return null;
    const length = this.view.getUint32(this.take(4), true);
    return this.bytes.subarray(this.take(length), this.at);
  }
}
function equalBytes(a: Uint8Array, b: Uint8Array): boolean {
  if (a.length !== b.length) return false;
  for (let i = 0; i < a.length; i++) if (a[i] !== b[i]) return false;
  return true;
}
export async function equalByteArrayKeys(
  left: Vector,
  i: number,
  right: Vector,
  j: number,
  pace: () => Promise<void>,
): Promise<boolean> {
  const validLeft = isValidSlot(left, i),
    validRight = isValidSlot(right, j);
  if (!validLeft || !validRight) return validLeft === validRight;
  const type = left.type;
  if (type.kind !== "array" || !byteArrayGroupKey(type))
    throw new Error("Expected byte-valued SQL array group key");
  const read = (v: Vector, row: number): Cursor => {
    const [flat, slot] = slotOf(v, row);
    if (flat.data.layout !== "varlen") throw new Error("Invalid SQL array group vector");
    return new Cursor(
      flat.data.bytes.subarray(flat.data.offsets[slot] ?? 0, flat.data.offsets[slot + 1] ?? 0),
      type.element,
    );
  };
  const a = read(left, i),
    b = read(right, j);
  if (a.count !== b.count || !equalBytes(a.shape, b.shape)) return false;
  for (let index = 0; index < a.count; index++) {
    const x = a.next(index),
      y = b.next(index);
    if (x === null || y === null) {
      if (x !== y) return false;
    } else {
      if (x.length !== y.length) return false;
      for (let byte = 0; byte < x.length; byte++) {
        if (x[byte] !== y[byte]) return false;
        if ((byte & 65535) === 65535) await pace();
      }
    }
    if ((index & 1023) === 1023) await pace();
  }
  if (a.at !== a.bytes.length || b.at !== b.bytes.length)
    throw new MalformedArrayError("Trailing grouping array bytes");
  return true;
}
