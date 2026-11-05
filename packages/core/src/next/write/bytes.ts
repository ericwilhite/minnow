/**
 * Little-endian byte writer and reader for the write path's own stored structures (commit bodies,
 * index runs, deletion bitmaps, the catalog, and the provisional manifest codecs). Every reader
 * check failure is `StoreCorruptionError`: these bytes came from storage after a checksum passed,
 * so a structural mismatch is a format or software bug, never data.
 */
import { StoreCorruptionError } from "../storage/contract.js";

/** `value`, which the caller knows is present (a broken invariant is a `TypeError`, not data). */
export function need<T>(value: T | null | undefined): T {
  if (value === null || value === undefined)
    throw new TypeError("A write-path invariant does not hold");
  return value;
}

const encoder = new TextEncoder();
const decoder = new TextDecoder("utf-8", { fatal: true });

export class ByteWriter {
  bytes: Uint8Array;
  view: DataView;
  length = 0;

  constructor(initial = 256) {
    this.bytes = new Uint8Array(initial);
    this.view = new DataView(this.bytes.buffer);
  }

  reset(): void {
    this.length = 0;
  }

  ensure(extra: number): void {
    if (this.length + extra <= this.bytes.length) return;
    let size = this.bytes.length * 2;
    while (size < this.length + extra) size *= 2;
    const grown = new Uint8Array(size);
    grown.set(this.bytes.subarray(0, this.length));
    this.bytes = grown;
    this.view = new DataView(grown.buffer);
  }

  u8(value: number): void {
    this.ensure(1);
    this.bytes[this.length++] = value;
  }

  u16(value: number): void {
    this.ensure(2);
    this.view.setUint16(this.length, value, true);
    this.length += 2;
  }

  u32(value: number): void {
    this.ensure(4);
    this.view.setUint32(this.length, value >>> 0, true);
    this.length += 4;
  }

  f64(value: number): void {
    this.ensure(8);
    this.view.setFloat64(this.length, value, true);
    this.length += 8;
  }

  /** A non-negative safe integer, LEB128. */
  varuint(value: number): void {
    if (!(Number.isSafeInteger(value) && value >= 0)) throw new RangeError("Not a varuint");
    this.ensure(8);
    let rest = value;
    while (rest >= 0x80) {
      this.bytes[this.length++] = (rest % 0x80) | 0x80;
      rest = Math.floor(rest / 0x80);
    }
    this.bytes[this.length++] = rest;
  }

  /** A safe integer of either sign (zigzag over the doubled magnitude). */
  varint(value: number): void {
    if (!Number.isSafeInteger(value)) throw new RangeError("Not a safe integer");
    this.varuint(value < 0 ? -value * 2 - 1 : value * 2);
  }

  bigint(value: bigint): void {
    this.string(value.toString());
  }

  raw(source: Uint8Array): void {
    this.ensure(source.length);
    this.bytes.set(source, this.length);
    this.length += source.length;
  }

  /** Length-prefixed bytes. */
  blob(source: Uint8Array): void {
    this.varuint(source.length);
    this.raw(source);
  }

  string(value: string): void {
    this.blob(encoder.encode(value));
  }

  /** Raw bytes of a typed array (its own byte range). */
  typed(array: ArrayBufferView): void {
    this.raw(new Uint8Array(array.buffer, array.byteOffset, array.byteLength));
  }

  /** The bytes written (a copy, safe to keep). */
  finish(): Uint8Array {
    return this.bytes.slice(0, this.length);
  }
}

export class ByteReader {
  readonly view: DataView;
  offset: number;

  constructor(
    readonly bytes: Uint8Array,
    offset = 0,
  ) {
    this.view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
    this.offset = offset;
  }

  get done(): boolean {
    return this.offset >= this.bytes.length;
  }

  #need(count: number): void {
    if (this.offset + count > this.bytes.length)
      throw new StoreCorruptionError("A stored structure ends early");
  }

  u8(): number {
    this.#need(1);
    return this.bytes[this.offset++] ?? 0;
  }

  u16(): number {
    this.#need(2);
    const value = this.view.getUint16(this.offset, true);
    this.offset += 2;
    return value;
  }

  u32(): number {
    this.#need(4);
    const value = this.view.getUint32(this.offset, true);
    this.offset += 4;
    return value;
  }

  f64(): number {
    this.#need(8);
    const value = this.view.getFloat64(this.offset, true);
    this.offset += 8;
    return value;
  }

  varuint(): number {
    let value = 0;
    let scale = 1;
    for (let index = 0; index < 8; index++) {
      const byte = this.u8();
      value += (byte & 0x7f) * scale;
      if (byte < 0x80) {
        if (!Number.isSafeInteger(value)) break;
        return value;
      }
      scale *= 0x80;
    }
    throw new StoreCorruptionError("A stored integer is out of range");
  }

  varint(): number {
    const zigzag = this.varuint();
    return zigzag % 2 === 1 ? -(zigzag + 1) / 2 : zigzag / 2;
  }

  bigint(): bigint {
    const text = this.string();
    if (!/^-?\d+$/u.test(text)) throw new StoreCorruptionError("A stored bigint is malformed");
    return BigInt(text);
  }

  /** A view of `count` bytes (valid while `bytes` is). */
  raw(count: number): Uint8Array {
    this.#need(count);
    const out = this.bytes.subarray(this.offset, this.offset + count);
    this.offset += count;
    return out;
  }

  blob(): Uint8Array {
    return this.raw(this.varuint());
  }

  string(): string {
    try {
      return decoder.decode(this.blob());
    } catch (error) {
      throw new StoreCorruptionError(`A stored string is not UTF-8: ${String(error)}`);
    }
  }

  /** Copies `count` elements of a typed array (an aligned copy, safe to keep). */
  f64Array(count: number): Float64Array {
    const bytes = this.raw(count * 8);
    return new Float64Array(bytes.slice().buffer);
  }

  end(): void {
    if (this.offset !== this.bytes.length)
      throw new StoreCorruptionError("A stored structure has trailing bytes");
  }
}

/** JSON with bigint and byte-array values tagged, for the small structures stored as JSON. */
export function encodeJson(value: unknown): Uint8Array {
  return encoder.encode(
    JSON.stringify(value, (_key, item: unknown) => {
      if (typeof item === "bigint") return { $bigint: item.toString() };
      if (typeof item === "number" && !Number.isFinite(item)) return { $number: String(item) };
      if (Object.is(item, -0)) return { $number: "-0" };
      if (item instanceof Uint8Array) return { $bytes: Array.from(item) };
      return item;
    }),
  );
}

export function decodeJson(bytes: Uint8Array): unknown {
  try {
    return JSON.parse(decoder.decode(bytes), (_key, item: unknown) => {
      if (typeof item === "object" && item !== null) {
        if ("$bigint" in item && typeof item.$bigint === "string") return BigInt(item.$bigint);
        if ("$number" in item && typeof item.$number === "string") return Number(item.$number);
        if ("$bytes" in item && Array.isArray(item.$bytes))
          return Uint8Array.from(item.$bytes as number[]);
      }
      return item;
    }) as unknown;
  } catch (error) {
    throw new StoreCorruptionError(`A stored JSON structure is malformed: ${String(error)}`);
  }
}
