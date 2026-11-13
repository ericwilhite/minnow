/**
 * Byte reading and writing for the manifest codecs (manifest-codec.ts): little-endian, unsigned
 * LEB128 varints up to 2^53 − 1, zigzag signed varints, length-prefixed bytes and UTF-8 strings,
 * signed bigints as length-prefixed two's complement, and IEEE doubles.
 *
 * Every read checks bounds and ranges and throws `ManifestCorruptionError`, never a `RangeError`
 * from a typed array or `DataView`, so a damaged blob is a typed refusal, never a crash or a hang.
 */

/** Manifest metadata (root, group directory, group root) that cannot be what was written. */
export class ManifestCorruptionError extends Error {
  readonly code: string = "MINNOW_MANIFEST_CORRUPT";
  constructor(message: string, options?: ErrorOptions) {
    super(message, options);
    this.name = "ManifestCorruptionError";
  }
}

export function valid(condition: boolean, message: string): asserts condition {
  if (!condition) throw new ManifestCorruptionError(message);
}

const encoder = new TextEncoder();
// ignoreBOM: a leading U+FEFF is part of the string, not a marker to strip (review B2-p2a #3).
const decoder = new TextDecoder("utf-8", { fatal: true, ignoreBOM: true });

export class ByteWriter {
  #bytes = new Uint8Array(256);
  #view = new DataView(this.#bytes.buffer);
  length = 0;

  #reserve(extra: number): void {
    if (this.length + extra <= this.#bytes.length) return;
    let size = this.#bytes.length * 2;
    while (size < this.length + extra) size *= 2;
    const grown = new Uint8Array(size);
    grown.set(this.#bytes.subarray(0, this.length));
    this.#bytes = grown;
    this.#view = new DataView(grown.buffer);
  }

  u8(value: number): void {
    this.#reserve(1);
    this.#bytes[this.length++] = value & 0xff;
  }

  u32(value: number): void {
    this.#reserve(4);
    this.#view.setUint32(this.length, value >>> 0, true);
    this.length += 4;
  }

  /** An unsigned integer ≤ 2^53 − 1, 7 bits per byte. */
  uint(value: number): void {
    if (!(Number.isSafeInteger(value) && value >= 0))
      throw new RangeError(`Not an unsigned safe integer: ${String(value)}`);
    let rest = value;
    while (rest >= 0x80) {
      this.u8((rest % 0x80) | 0x80);
      rest = Math.floor(rest / 0x80);
    }
    this.u8(rest);
  }

  /** A signed safe integer, zigzag-encoded. */
  int(value: number): void {
    if (!Number.isSafeInteger(value)) throw new RangeError(`Not a safe integer: ${String(value)}`);
    this.uint(value < 0 ? -value * 2 - 1 : value * 2);
  }

  /** null as 0, else value + 1. */
  optionalUint(value: number | null): void {
    this.uint(value === null ? 0 : value + 1);
  }

  f64(value: number): void {
    this.#reserve(8);
    this.#view.setFloat64(this.length, value, true);
    this.length += 8;
  }

  bytes(value: Uint8Array): void {
    this.uint(value.length);
    this.#reserve(value.length);
    this.#bytes.set(value, this.length);
    this.length += value.length;
  }

  string(value: string): void {
    this.bytes(encoder.encode(value));
  }

  /** A signed bigint of any size: byte count, then two's complement, little-endian. */
  bigint(value: bigint): void {
    let bits = 8;
    while (BigInt.asIntN(bits, value) !== value) bits += 8;
    const count = bits / 8;
    this.uint(count);
    this.#reserve(count);
    let rest = BigInt.asUintN(bits, value);
    for (let k = 0; k < count; k++) {
      this.#bytes[this.length++] = Number(rest & 0xffn);
      rest >>= 8n;
    }
  }

  /** A copy of what was written. */
  finish(): Uint8Array {
    return this.#bytes.slice(0, this.length);
  }
}

export class ByteReader {
  readonly #bytes: Uint8Array;
  readonly #view: DataView;
  at = 0;

  constructor(bytes: Uint8Array) {
    this.#bytes = bytes;
    this.#view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  }

  get remaining(): number {
    return this.#bytes.length - this.at;
  }

  #need(count: number): void {
    valid(count >= 0 && this.at + count <= this.#bytes.length, "Manifest blob is truncated");
  }

  u8(): number {
    this.#need(1);
    return this.#bytes[this.at++] ?? 0;
  }

  u32(): number {
    this.#need(4);
    const value = this.#view.getUint32(this.at, true);
    this.at += 4;
    return value;
  }

  uint(): number {
    const bytes = this.#bytes;
    let at = this.at;
    let value = 0;
    let scale = 1;
    for (let k = 0; k < 8; k++) {
      const byte = bytes[at++];
      if (byte === undefined) throw new ManifestCorruptionError("Manifest blob is truncated");
      value += (byte & 0x7f) * scale;
      if (byte < 0x80) {
        valid(k === 0 || byte !== 0, "Varint is not minimal");
        valid(value <= Number.MAX_SAFE_INTEGER, "Varint past 2^53");
        this.at = at;
        return value;
      }
      scale *= 0x80;
    }
    throw new ManifestCorruptionError("Varint is too long");
  }

  int(): number {
    const zigzag = this.uint();
    return zigzag % 2 === 0 ? zigzag / 2 : -(zigzag + 1) / 2;
  }

  optionalUint(): number | null {
    const value = this.uint();
    return value === 0 ? null : value - 1;
  }

  /** A count of items each at least `minBytes` long: bounded by what the blob can hold. */
  count(minBytes: number): number {
    const value = this.uint();
    valid(value * Math.max(1, minBytes) <= this.remaining, "Count exceeds the blob");
    return value;
  }

  f64(): number {
    this.#need(8);
    const value = this.#view.getFloat64(this.at, true);
    this.at += 8;
    return value;
  }

  bytes(): Uint8Array {
    const length = this.uint();
    this.#need(length);
    const out = this.#bytes.slice(this.at, this.at + length);
    this.at += length;
    return out;
  }

  string(): string {
    try {
      return decoder.decode(this.bytes());
    } catch (error) {
      if (error instanceof ManifestCorruptionError) throw error;
      throw new ManifestCorruptionError("A manifest string is not UTF-8");
    }
  }

  bigint(): bigint {
    const count = this.uint();
    valid(count >= 1 && count <= 4096, "Bad bigint length");
    this.#need(count);
    let value = 0n;
    for (let k = count - 1; k >= 0; k--)
      value = (value << 8n) | BigInt(this.#bytes[this.at + k] ?? 0);
    this.at += count;
    const signed = BigInt.asIntN(count * 8, value);
    // Canonical: the fewest bytes that hold the value (what `ByteWriter.bigint` writes).
    valid(
      count === 1 || BigInt.asIntN((count - 1) * 8, signed) !== signed,
      "Bigint is not minimal",
    );
    return signed;
  }

  /** Throws unless every byte was consumed. */
  end(): void {
    valid(this.at === this.#bytes.length, "Manifest blob has trailing bytes");
  }
}
