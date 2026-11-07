/**
 * Little-endian byte writer and reader for the write path's own stored structures (commit bodies,
 * index runs, deletion bitmaps, the catalog, and the provisional manifest codecs). Every reader
 * check failure is `StoreCorruptionError`: these bytes came from storage after a checksum passed,
 * so a structural mismatch is a format or software bug, never data.
 */
import { StoreCorruptionError } from "../storage/contract.js";
import { SMALL_COMMIT_BYTES, StreamCommit, type BufferAllocator } from "./commit-buffers.js";

/** `value`, which the caller knows is present (a broken invariant is a `TypeError`, not data). */
export function need<T>(value: T | null | undefined): T {
  if (value === null || value === undefined)
    throw new TypeError("A write-path invariant does not hold");
  return value;
}

/** Memory a structure charges before it allocates (a governor grant, or a memtable's counter). */
export interface Charge {
  allocate(bytes: number): boolean;
  free(bytes: number): void;
}

/** For scratch structures nobody accounts (tests, a statement's transient row copies). */
export const UNCHARGED: Charge = { allocate: () => true, free: () => undefined };

/** The write path's grant refused memory a memtable needs (after backpressure). */
export class MemtableMemoryError extends Error {
  readonly code = "MINNOW_MEMTABLE_MEMORY";
  constructor(readonly bytes: number) {
    super(`The write path could not charge ${String(bytes)} bytes of memtable memory`);
    this.name = "MemtableMemoryError";
  }
}

/** Charges `bytes` to `charge` before an allocation, or throws `MemtableMemoryError`. */
export function charged(charge: Charge, bytes: number): void {
  if (!charge.allocate(bytes)) throw new MemtableMemoryError(bytes);
}

/** UTF-8 byte count without materializing an encoded copy. Lone surrogates become U+FFFD. */
export function utf8Length(value: string): number {
  let bytes = 0;
  for (let k = 0; k < value.length; k++) {
    const code = value.charCodeAt(k);
    if (code < 0x80) bytes++;
    else if (code < 0x800) bytes += 2;
    else if (
      code >= 0xd800 &&
      code <= 0xdbff &&
      value.charCodeAt(k + 1) >= 0xdc00 &&
      value.charCodeAt(k + 1) <= 0xdfff
    ) {
      bytes += 4;
      k++;
    } else bytes += 3;
  }
  return bytes;
}

const encoder = new TextEncoder();
const decoder = new TextDecoder("utf-8", { fatal: true });
const EMPTY_BYTES = new Uint8Array(0);
const EMPTY_VIEW = new DataView(EMPTY_BYTES.buffer);

/** Copies `source[start … end)` to `target` at `at`; short ranges byte by byte (no view). */
export function copyRange(
  target: Uint8Array,
  at: number,
  source: Uint8Array,
  start: number,
  end: number,
): void {
  if (end - start > 32) {
    target.set(source.subarray(start, end), at);
    return;
  }
  for (let k = start; k < end; k++) target[at + k - start] = source[k] ?? 0;
}

/** Operations needed by the column codec, also implemented by the bounded log window. */
export type ByteSink = Pick<
  ByteWriter,
  | "length"
  | "ensure"
  | "u8"
  | "u16"
  | "u32"
  | "f64"
  | "varuint"
  | "varint"
  | "bigint"
  | "rawRange"
  | "raw"
  | "blob"
  | "string"
  | "typed"
  | "range"
>;

export class ByteWriter {
  bytes: Uint8Array;
  view: DataView;
  length = 0;

  constructor(
    initial = 256,
    readonly allocator?: BufferAllocator,
    readonly maximum = Infinity,
  ) {
    this.bytes = allocator?.create(initial) ?? new Uint8Array(initial);
    this.view = new DataView(this.bytes.buffer);
  }

  reset(): void {
    this.length = 0;
  }

  close(): void {
    if (this.bytes.length === 0) return;
    this.allocator?.release(this.bytes);
    this.bytes = EMPTY_BYTES;
    this.view = EMPTY_VIEW;
    this.length = 0;
  }

  ensure(extra: number): void {
    if (this.length + extra <= this.bytes.length) return;
    if (this.length + extra > this.maximum) throw new StreamCommit();
    let size = this.bytes.length * 2;
    while (size < this.length + extra) size *= 2;
    const grown = this.allocator?.create(size) ?? new Uint8Array(size);
    grown.set(this.bytes.subarray(0, this.length));
    this.allocator?.release(this.bytes);
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

  /** Bytes `start … end − 1` of `source` (short ranges copied without a view). */
  rawRange(source: Uint8Array, start: number, end: number): void {
    this.ensure(end - start);
    copyRange(this.bytes, this.length, source, start, end);
    this.length += end - start;
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
    if (this.allocator === undefined) {
      this.blob(encoder.encode(value));
      return;
    }
    const length = utf8Length(value);
    this.varuint(length);
    this.ensure(length);
    encoder.encodeInto(value, this.bytes.subarray(this.length, this.length + length));
    this.length += length;
  }

  /** Raw bytes of a typed array (its own byte range). */
  typed(array: ArrayBufferView): void {
    this.raw(new Uint8Array(array.buffer, array.byteOffset, array.byteLength));
  }

  /**
   * Elements `from … from + count − 1` of a typed array, in platform byte order (as `typed`).
   * Short ranges are copied element by element, so nothing is allocated for them.
   */
  range(
    array: Int32Array | Uint32Array | Float64Array | Float32Array | Uint8Array,
    from: number,
    count: number,
  ): void {
    const width = array.BYTES_PER_ELEMENT;
    const length = count * width;
    this.ensure(length);
    if (count > 8 || width === 1) {
      this.bytes.set(
        new Uint8Array(array.buffer, array.byteOffset + from * width, length),
        this.length,
      );
      this.length += length;
      return;
    }
    const view = this.view;
    let at = this.length;
    if (array instanceof Float64Array)
      for (let k = 0; k < count; k++, at += 8) view.setFloat64(at, array[from + k] ?? 0, true);
    else if (array instanceof Float32Array)
      for (let k = 0; k < count; k++, at += 4) view.setFloat32(at, array[from + k] ?? 0, true);
    else if (array instanceof Int32Array)
      for (let k = 0; k < count; k++, at += 4) view.setInt32(at, array[from + k] ?? 0, true);
    else for (let k = 0; k < count; k++, at += 4) view.setUint32(at, array[from + k] ?? 0, true);
    this.length = at;
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
export function encodeJson(value: unknown, allocator?: BufferAllocator): Uint8Array {
  const release = allocator?.reserve?.(24 + 2 * (SMALL_COMMIT_BYTES + 16));
  try {
    const text = JSON.stringify(value, (_key, item: unknown) => {
      if (typeof item === "bigint") return { $bigint: item.toString() };
      if (typeof item === "number" && !Number.isFinite(item)) return { $number: String(item) };
      if (Object.is(item, -0)) return { $number: "-0" };
      if (item instanceof Uint8Array) return { $bytes: Array.from(item) };
      return item;
    });
    if (allocator === undefined) return encoder.encode(text);
    const bytes = allocator.create(utf8Length(text));
    encoder.encodeInto(text, bytes);
    return bytes;
  } finally {
    release?.();
  }
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
