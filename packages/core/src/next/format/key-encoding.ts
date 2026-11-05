/**
 * The memcomparable key encoding (DECISIONS.md C7), frozen with the format: key-index runs,
 * run fences and Bloom filters persist **equality keys**; sorts, merges and range bounds use
 * **sort keys** at query time. Comparing two encodings with unsigned byte order (`compareBytes`)
 * gives the SQL order; equal encodings are exactly the SQL-equal values; hashing an equality key's
 * bytes (xxHash32) is therefore consistent with equality.
 *
 * ## Component layout
 * A key is the concatenation of its components. Every component is prefix-free, so components
 * never bleed into each other.
 * - Sort-key component: one header byte, then the payload, with the payload's bytes inverted
 *   (`~b`) for DESC. Header (never inverted): NULLS FIRST: null `0x00`, value `0x01`;
 *   NULLS LAST: value `0x01`, null `0x02`. A null has no payload.
 * - Equality-key component: header null `0x00` (only for null-safe keys), value `0x01`, payload.
 *
 * ## Payloads (big-endian throughout)
 * | type | payload |
 * | --- | --- |
 * | bool | `0x00` false, `0x01` true |
 * | int32, date | 4 bytes: value XOR 0x8000_0000 (date ±infinity are INT32 min/max, so they sort at the ends) |
 * | int64, time, timestamp | 8 bytes: value + 2^63 (timestamp ±infinity are INT64 min/max) |
 * | float64, float32 | 8 bytes of the f64 (float32 widened): −0 → +0; every NaN → 0x7FF8…0; then if the sign bit is set invert all bits, else set the sign bit. NaN sorts above +Infinity and equals NaN (PostgreSQL). |
 * | decimal | class byte, then for finite non-zero values the normalized magnitude (below) |
 * | text, bytes, json, array | bytes with `0x00` → `0x00 0xFF`, then terminator `0x00 0x01` |
 * | uuid | the 16 bytes |
 * | enum | sort key: label rank as u16 (declaration order); equality key: the label's text payload |
 * | interval | 16 bytes: (months × 30 + days) × 86,400,000,000 + µs, as int128 + 2^127 (PostgreSQL compares the normalized span, so `1 mon` = `30 days`) |
 *
 * Decimal class byte: −Infinity `0x01`, negative `0x02`, zero `0x03`, positive `0x04`,
 * +Infinity `0x05`, NaN `0x06` (PostgreSQL: NaN above +Infinity, NaN = NaN). A finite non-zero
 * value is written as 0.d₁d₂… × 10^E with d₁ ≠ 0 and no trailing zero digit (so 1.0 = 1.00, as
 * PostgreSQL's equality ignores scale): E as 4 bytes (E XOR 0x8000_0000), then digit pairs as
 * bytes 10a + b + 1 (an odd last digit pairs with 0), then terminator `0x00`. A negative value's
 * exponent, pair and terminator bytes are inverted.
 *
 * Collation: these are C-collation keys. ICU collations are never encoded (DECISIONS.md C8).
 * Locators are never keys.
 */
import type { PhysicalType } from "../types.js";

export interface DecimalKey {
  readonly coefficient: bigint;
  /** Value = coefficient × 10^−scale. Any integer scale, including negative. */
  readonly scale: number;
  readonly special?: "NaN" | "Infinity" | "-Infinity";
}

export interface IntervalKey {
  readonly months: number;
  readonly days: number;
  readonly micros: bigint;
}

/**
 * One value in key form: boolean (bool); number (int32, date, float64, float32); bigint (int64,
 * time, timestamp); Uint8Array (text, bytes, json, array as stored bytes; uuid as 16 bytes; enum
 * as the label's UTF-8); DecimalKey; IntervalKey.
 */
export type KeyValue = boolean | number | bigint | Uint8Array | DecimalKey | IntervalKey;

export interface SortKeyPart {
  readonly type: PhysicalType;
  readonly value: KeyValue | null;
  readonly direction: "asc" | "desc";
  readonly nulls: "first" | "last";
}

export interface EqualityKeyPart {
  readonly type: PhysicalType;
  readonly value: KeyValue | null;
}

/** A growable byte buffer for building keys without per-byte allocation. */
class KeyBuffer {
  bytes = new Uint8Array(64);
  length = 0;

  push(byte: number): void {
    if (this.length === this.bytes.length) {
      const grown = new Uint8Array(this.bytes.length * 2);
      grown.set(this.bytes);
      this.bytes = grown;
    }
    this.bytes[this.length++] = byte;
  }

  invertFrom(start: number): void {
    for (let index = start; index < this.length; index++)
      this.bytes[index] = ~(this.bytes[index] ?? 0) & 0xff;
  }

  result(): Uint8Array {
    return this.bytes.slice(0, this.length);
  }
}

const scratch = new DataView(new ArrayBuffer(8));
const TWO_63 = 2n ** 63n;
const TWO_127 = 2n ** 127n;
const MICROS_PER_DAY = 86_400_000_000n;

function pushBig(buffer: KeyBuffer, value: bigint, bytes: number): void {
  for (let shift = BigInt((bytes - 1) * 8); shift >= 0n; shift -= 8n)
    buffer.push(Number((value >> shift) & 0xffn));
}

function pushEscaped(buffer: KeyBuffer, bytes: Uint8Array): void {
  for (const byte of bytes) {
    buffer.push(byte);
    if (byte === 0) buffer.push(0xff);
  }
  buffer.push(0x00);
  buffer.push(0x01);
}

function pushFloat(buffer: KeyBuffer, value: number): void {
  const canonical = Number.isNaN(value) ? Number.NaN : value === 0 ? 0 : value;
  scratch.setFloat64(0, canonical);
  const negative = (scratch.getUint8(0) & 0x80) !== 0;
  for (let index = 0; index < 8; index++) {
    const byte = scratch.getUint8(index);
    buffer.push(negative ? ~byte & 0xff : index === 0 ? byte | 0x80 : byte);
  }
}

function pushDecimal(buffer: KeyBuffer, value: DecimalKey): void {
  if (value.special === "-Infinity") return buffer.push(0x01);
  if (value.special === "Infinity") return buffer.push(0x05);
  if (value.special === "NaN") return buffer.push(0x06);
  if (value.coefficient === 0n) return buffer.push(0x03);
  const negative = value.coefficient < 0n;
  let digits = (negative ? -value.coefficient : value.coefficient).toString();
  let trailing = 0;
  while (digits.endsWith("0")) {
    digits = digits.slice(0, -1);
    trailing += 1;
  }
  // value = 0.digits × 10^exponent
  const exponent = digits.length + trailing - value.scale;
  if (!Number.isSafeInteger(exponent) || Math.abs(exponent) > 0x3fff_ffff)
    throw new RangeError("NUMERIC exponent is outside the key range");
  buffer.push(negative ? 0x02 : 0x04);
  const start = buffer.length;
  const biased = (exponent ^ 0x8000_0000) >>> 0;
  buffer.push(biased >>> 24);
  buffer.push((biased >>> 16) & 0xff);
  buffer.push((biased >>> 8) & 0xff);
  buffer.push(biased & 0xff);
  for (let index = 0; index < digits.length; index += 2) {
    const high = digits.charCodeAt(index) - 48;
    const low = index + 1 < digits.length ? digits.charCodeAt(index + 1) - 48 : 0;
    buffer.push(high * 10 + low + 1);
  }
  buffer.push(0x00);
  if (negative) buffer.invertFrom(start);
}

function enumRank(type: Extract<PhysicalType, { kind: "enum" }>, label: Uint8Array): number {
  const text = new TextDecoder().decode(label);
  const rank = type.labels.indexOf(text);
  if (rank < 0) throw new RangeError(`"${text}" is not a label of enum ${type.name}`);
  return rank;
}

function pushPayload(buffer: KeyBuffer, type: PhysicalType, value: KeyValue, sort: boolean): void {
  switch (type.kind) {
    case "bool":
      buffer.push(value === true ? 1 : 0);
      return;
    case "int32":
    case "date": {
      const biased = ((value as number) ^ 0x8000_0000) >>> 0;
      buffer.push(biased >>> 24);
      buffer.push((biased >>> 16) & 0xff);
      buffer.push((biased >>> 8) & 0xff);
      buffer.push(biased & 0xff);
      return;
    }
    case "int64":
    case "time":
    case "timestamp":
      pushBig(buffer, (value as bigint) + TWO_63, 8);
      return;
    case "float64":
    case "float32":
      pushFloat(buffer, value as number);
      return;
    case "decimal":
      pushDecimal(buffer, value as DecimalKey);
      return;
    case "text":
    case "bytes":
    case "json":
    case "array":
      pushEscaped(buffer, value as Uint8Array);
      return;
    case "uuid":
      for (const byte of value as Uint8Array) buffer.push(byte);
      return;
    case "enum":
      if (sort) {
        const rank = enumRank(type, value as Uint8Array);
        buffer.push(rank >>> 8);
        buffer.push(rank & 0xff);
      } else pushEscaped(buffer, value as Uint8Array);
      return;
    case "interval": {
      const { months, days, micros } = value as IntervalKey;
      const span = (BigInt(months) * 30n + BigInt(days)) * MICROS_PER_DAY + micros;
      pushBig(buffer, span + TWO_127, 16);
      return;
    }
    case "locator":
      throw new TypeError("Row locators are never keys");
  }
}

/** The sort key of a tuple: compare two with unsigned byte order. */
export function encodeSortKey(parts: readonly SortKeyPart[]): Uint8Array {
  const buffer = new KeyBuffer();
  for (const part of parts) {
    if (part.value === null) {
      buffer.push(part.nulls === "first" ? 0x00 : 0x02);
      continue;
    }
    buffer.push(0x01);
    const start = buffer.length;
    pushPayload(buffer, part.type, part.value, true);
    if (part.direction === "desc") buffer.invertFrom(start);
  }
  return buffer.result();
}

/** The equality key of a tuple: equal bytes ⇔ SQL-equal (null-safe: null equals null). */
export function encodeEqualityKey(parts: readonly EqualityKeyPart[]): Uint8Array {
  const buffer = new KeyBuffer();
  for (const part of parts) {
    if (part.value === null) {
      buffer.push(0x00);
      continue;
    }
    buffer.push(0x01);
    pushPayload(buffer, part.type, part.value, false);
  }
  return buffer.result();
}
