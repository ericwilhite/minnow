/** A logical byte stream projected into one bounded, caller-owned byte window. */
import { utf8Length, type ByteSink } from "./bytes.js";

const encoder = new TextEncoder();

export class ByteWindow implements ByteSink {
  length = 0;
  readonly #view: DataView;
  constructor(
    readonly target: Uint8Array,
    readonly start: number,
    readonly scratch: Uint8Array,
  ) {
    this.#view = new DataView(scratch.buffer, scratch.byteOffset, scratch.byteLength);
  }

  ensure(extra: number): void {
    if (extra < 0) throw new RangeError("Negative byte count");
  }
  intersects(length: number): boolean {
    return this.length < this.start + this.target.length && this.length + length > this.start;
  }
  skip(length: number): void {
    this.length += length;
  }
  u8(value: number): void {
    if (this.intersects(1)) this.target[this.length - this.start] = value;
    this.length++;
  }
  u16(value: number): void {
    this.u8(value);
    this.u8(value >>> 8);
  }
  u32(value: number): void {
    this.u8(value);
    this.u8(value >>> 8);
    this.u8(value >>> 16);
    this.u8(value >>> 24);
  }
  f64(value: number): void {
    if (!this.intersects(8)) {
      this.skip(8);
      return;
    }
    this.#view.setFloat64(0, value, true);
    this.rawRange(this.scratch, 0, 8);
  }
  varuint(value: number): void {
    if (!(Number.isSafeInteger(value) && value >= 0)) throw new RangeError("Not a varuint");
    while (value >= 0x80) {
      this.u8((value % 0x80) | 0x80);
      value = Math.floor(value / 0x80);
    }
    this.u8(value);
  }
  varint(value: number): void {
    this.varuint(value < 0 ? -value * 2 - 1 : value * 2);
  }
  bigint(value: bigint): void {
    this.string(value.toString());
  }
  rawRange(source: Uint8Array, from: number, to: number): void {
    const length = to - from;
    const lo = Math.max(0, this.start - this.length);
    const hi = Math.min(length, this.start + this.target.length - this.length);
    if (hi > lo)
      this.target.set(source.subarray(from + lo, from + hi), this.length + lo - this.start);
    this.length += length;
  }
  raw(source: Uint8Array): void {
    this.rawRange(source, 0, source.length);
  }
  blob(source: Uint8Array): void {
    this.varuint(source.length);
    this.raw(source);
  }
  string(value: string): void {
    const length = utf8Length(value);
    this.varuint(length);
    this.utf8(value, length);
  }
  /** Raw UTF-8, chunked without breaking a surrogate pair. */
  utf8(value: string, length = utf8Length(value)): void {
    if (!this.intersects(length)) {
      this.skip(length);
      return;
    }
    for (let from = 0; from < value.length;) {
      let to = Math.min(value.length, from + Math.floor(this.scratch.length / 4));
      const last = value.charCodeAt(to - 1);
      if (to < value.length && last >= 0xd800 && last <= 0xdbff) to--;
      const { written } = encoder.encodeInto(value.slice(from, to), this.scratch);
      this.rawRange(this.scratch, 0, written);
      from = to;
    }
  }
  typed(array: ArrayBufferView): void {
    this.raw(new Uint8Array(array.buffer, array.byteOffset, array.byteLength));
  }
  range(
    array: Int32Array | Uint32Array | Float64Array | Float32Array | Uint8Array,
    from: number,
    count: number,
  ): void {
    const width = array.BYTES_PER_ELEMENT;
    if (count > 8 || width === 1) {
      this.raw(new Uint8Array(array.buffer, array.byteOffset + from * width, count * width));
      return;
    }
    // Match ByteWriter's short scalar path, including signaling-NaN quieting for REAL.
    if (array instanceof Float64Array)
      for (let k = 0; k < count; k++) this.f64(array[from + k] ?? 0);
    else if (array instanceof Float32Array)
      for (let k = 0; k < count; k++) {
        this.#view.setFloat32(0, array[from + k] ?? 0, true);
        this.rawRange(this.scratch, 0, 4);
      }
    else for (let k = 0; k < count; k++) this.u32(array[from + k] ?? 0);
  }
}
