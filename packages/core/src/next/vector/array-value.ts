/**
 * The stored form of a SQL array value (DECISIONS.md C9): one reversible binary value per row in a
 * `varlen` vector, decoded to `ArrayValue` before any operation. It is not a key and is never
 * compared as bytes: ordering and equality use exec/semantic.ts (`compareArrays`,
 * `arrayEqualityKey`) on the decoded value, and arrays cannot be indexed (keys/key-encoding.ts).
 *
 * Reversible means every field PostgreSQL keeps survives: an INTERVAL's months, days and µs
 * separately (`1 mon` and `30 days` are equal but different values), a NUMERIC's display scale
 * (`1.50` is not `1.5` on output), dimensions, lower bounds and null elements.
 *
 * ## Layout (little-endian)
 *   u8 version (1) | u8 element tag | u8 dimensions (0 … 6) | u8 flags (bit 0: has nulls)
 *   per dimension: i32 length, i32 lower bound
 *   if has nulls: null bitmap, one bit per element in storage order (set = null)
 *   each non-null element, in storage (row-major) order:
 *     bool u8 · int16/int32/date i32 · int64/time/timestamp i64 · float32 f32 · float64 f64 ·
 *     decimal: u8 special (0 finite, 1 NaN, 2 +Inf, 3 −Inf), then if finite u16 scale,
 *       u32 byte length, coefficient as two's complement bytes ·
 *     interval: i32 months, i32 days, i64 µs · text/bytes/enum: u32 length + bytes · uuid: 16 bytes
 * Element types: every scalar type except JSON (and never arrays of arrays: use dimensions).
 * Zero dimensions is the empty array, and the only one (`validateArrayValue`). Decoding validates.
 */
import type { PhysicalType } from "../types.js";

export type ArrayElement =
  | boolean
  | number
  | bigint
  | Uint8Array
  | {
      readonly coefficient: bigint;
      readonly scale: number;
      readonly special?: "NaN" | "Infinity" | "-Infinity";
    }
  | { readonly months: number; readonly days: number; readonly micros: bigint };

export interface ArrayValue {
  readonly dimensions: readonly number[];
  readonly lowerBounds: readonly number[];
  readonly elements: ReadonlyArray<ArrayElement | null>;
}

export const MAX_ARRAY_DIMENSIONS = 6;

export class MalformedArrayError extends Error {
  readonly code = "MINNOW_MALFORMED_ARRAY";
  constructor(message: string) {
    super(message);
    this.name = "MalformedArrayError";
  }
}

/** The element tag of a supported element type (throws for JSON, arrays, locators). */
export function arrayElementTag(type: PhysicalType): number {
  switch (type.kind) {
    case "bool":
      return 1;
    case "int32":
      return type.bits === 16 ? 2 : 3;
    case "int64":
      return 4;
    case "float32":
      return 5;
    case "float64":
      return 6;
    case "decimal":
      return 7;
    case "date":
      return 8;
    case "time":
      return 9;
    case "timestamp":
      return type.zoned ? 11 : 10;
    case "interval":
      return 12;
    case "text":
      return 13;
    case "bytes":
      return 14;
    case "uuid":
      return 15;
    case "enum":
      return 16;
    case "json":
    case "array":
    case "locator":
      throw new TypeError(`Arrays of ${type.kind} are not supported`);
  }
}

class Writer {
  bytes = new Uint8Array(64);
  view = new DataView(this.bytes.buffer);
  length = 0;
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
    this.view.setUint8(this.length++, value);
  }
  i32(value: number): void {
    this.ensure(4);
    this.view.setInt32(this.length, value, true);
    this.length += 4;
  }
  u32(value: number): void {
    this.ensure(4);
    this.view.setUint32(this.length, value, true);
    this.length += 4;
  }
  i64(value: bigint): void {
    this.ensure(8);
    this.view.setBigInt64(this.length, value, true);
    this.length += 8;
  }
  raw(bytes: Uint8Array): void {
    this.ensure(bytes.length);
    this.bytes.set(bytes, this.length);
    this.length += bytes.length;
  }
}

/**
 * Minimal little-endian two's complement bytes. Linear time through hex text: a byte-at-a-time
 * BigInt shift is quadratic, seconds for a legal 131,072-digit NUMERIC.
 */
function twosComplement(value: bigint): Uint8Array {
  const negative = value < 0n;
  // For a negative value the bytes are the complement of −value − 1's.
  let hex = (negative ? -value - 1n : value).toString(16);
  if (hex.length % 2 === 1) hex = `0${hex}`;
  const count = hex.length / 2;
  const signByte = Number.parseInt(hex.slice(0, 2), 16) >= 0x80 ? 1 : 0;
  const out = new Uint8Array(count + signByte);
  for (let index = 0; index < count; index++) {
    const at = hex.length - 2 * (index + 1);
    const byte = Number.parseInt(hex.slice(at, at + 2), 16);
    out[index] = negative ? ~byte & 0xff : byte;
  }
  if (signByte === 1) out[count] = negative ? 0xff : 0;
  return out;
}

function fromTwosComplement(bytes: Uint8Array): bigint {
  if (bytes.length === 0) return 0n;
  const digits: string[] = [];
  for (let index = bytes.length - 1; index >= 0; index--)
    digits.push((bytes[index] ?? 0).toString(16).padStart(2, "0"));
  return BigInt.asIntN(bytes.length * 8, BigInt(`0x${digits.join("")}`));
}

/** PostgreSQL's MaxArraySize: the most elements an array may hold. */
export const MAX_ARRAY_ELEMENTS = 134_217_727;
const INT32_MIN = -2_147_483_648;
const INT32_MAX = 2_147_483_647;
const INT64_MIN = -(2n ** 63n);
const INT64_MAX = 2n ** 63n - 1n;

const isInt32 = (value: unknown): value is number =>
  typeof value === "number" && Number.isInteger(value) && value >= INT32_MIN && value <= INT32_MAX;
const isInt64 = (value: unknown): value is bigint =>
  typeof value === "bigint" && value >= INT64_MIN && value <= INT64_MAX;

/** PostgreSQL NUMERIC: display scale ≤ 16,383 and ≤ 131,072 digits before the point. */
const NUMERIC_MAX_SCALE = 16_383;
const NUMERIC_MAX_INTEGER_DIGITS = 131_072;
const LOG10_2 = Math.log10(2);

/**
 * Whether coefficient × 10^−scale has at most 131,072 digits before the point. Decides from the
 * hex length (linear time) unless the value is within a digit of the limit, so a huge coefficient
 * is refused without a quadratic decimal conversion.
 */
function numericFits(coefficient: bigint, scale: number): boolean {
  const magnitude = coefficient < 0n ? -coefficient : coefficient;
  if (magnitude === 0n) return true;
  const limit = NUMERIC_MAX_INTEGER_DIGITS + scale;
  const bits = (magnitude.toString(16).length - 1) * 4; // magnitude ≥ 2^bits
  if (Math.floor(bits * LOG10_2) + 1 > limit + 1) return false;
  if (Math.ceil((bits + 4) * LOG10_2) + 1 < limit) return true;
  return magnitude.toString().length <= limit;
}

function checkElement(element: PhysicalType, tag: number, item: ArrayElement): void {
  const bad = (what: string): never => {
    throw new MalformedArrayError(`Array element out of range for ${element.kind}: ${what}`);
  };
  switch (tag) {
    case 1:
      if (typeof item !== "boolean") bad("not a boolean");
      return;
    case 2:
      if (!isInt32(item) || item < -32_768 || item > 32_767) bad("not a SMALLINT");
      return;
    case 3:
    case 8:
      if (!isInt32(item)) bad("not an int32");
      return;
    case 4:
    case 10:
    case 11:
      if (!isInt64(item)) bad("not an int64");
      return;
    case 9:
      if (!isInt64(item) || item < 0n || item > 86_400_000_000n) bad("not a TIME");
      return;
    case 5:
      if (typeof item !== "number" || (!Number.isNaN(item) && Math.fround(item) !== item))
        bad("not a REAL");
      return;
    case 6:
      if (typeof item !== "number") bad("not a DOUBLE");
      return;
    case 7: {
      if (typeof item !== "object" || !("coefficient" in item)) return bad("not a NUMERIC");
      if (item.special !== undefined) return;
      if (typeof item.coefficient !== "bigint") return bad("NUMERIC coefficient");
      if (!Number.isInteger(item.scale) || item.scale < 0 || item.scale > NUMERIC_MAX_SCALE)
        bad("NUMERIC scale");
      if (!numericFits(item.coefficient, item.scale)) bad("NUMERIC value overflows");
      return;
    }
    case 12: {
      if (typeof item !== "object" || !("months" in item)) return bad("not an INTERVAL");
      if (!isInt32(item.months) || !isInt32(item.days) || !isInt64(item.micros))
        bad("INTERVAL field");
      return;
    }
    case 15:
      if (!(item instanceof Uint8Array) || item.length !== 16) bad("not 16 bytes");
      return;
    case 16: {
      if (!(item instanceof Uint8Array)) return bad("not a label");
      const label = new TextDecoder().decode(item);
      if (element.kind === "enum" && !element.labels.includes(label))
        bad(`unknown label "${label}"`);
      return;
    }
    default:
      if (!(item instanceof Uint8Array) || item.length > 0xffff_ffff) bad("not bytes");
  }
}

/**
 * Checks a value before anything is written and after it is read: dimensions and lower bounds are
 * int32, every upper bound (lower + length − 1) is int32, at most `MAX_ARRAY_ELEMENTS` elements,
 * and every element is in its type's range (NUMERIC: scale ≤ 16,383 and ≤ 131,072 digits before
 * the point). An empty array has zero dimensions, as PostgreSQL stores it (`'{{}}'` and
 * `array_fill(1, '{0}')` are both `{}` with no dimensions): a dimension of length 0 is refused,
 * never normalized here. A value that fails is never narrowed or truncated.
 */
export function validateArrayValue(element: PhysicalType, value: ArrayValue): void {
  const tag = arrayElementTag(element);
  const { dimensions, lowerBounds, elements } = value;
  if (dimensions.length > MAX_ARRAY_DIMENSIONS || lowerBounds.length !== dimensions.length)
    throw new MalformedArrayError("Bad array dimensions");
  let count = dimensions.length === 0 ? 0 : 1;
  dimensions.forEach((length, index) => {
    const lower = lowerBounds[index];
    if (!isInt32(length) || length < 0) throw new MalformedArrayError("Bad array dimension length");
    if (length === 0)
      throw new MalformedArrayError("An empty array has no dimensions (a dimension of length 0)");
    if (!isInt32(lower)) throw new MalformedArrayError("Array lower bound out of range");
    if (lower + length - 1 > INT32_MAX)
      throw new MalformedArrayError("Array upper bound out of range");
    count *= length;
  });
  if (count > MAX_ARRAY_ELEMENTS) throw new MalformedArrayError("Too many array elements");
  if (count !== elements.length)
    throw new MalformedArrayError("Dimensions do not match the elements");
  for (const item of elements) if (item !== null) checkElement(element, tag, item);
}

export function encodeArrayValue(element: PhysicalType, value: ArrayValue): Uint8Array {
  validateArrayValue(element, value);
  const tag = arrayElementTag(element);
  const { dimensions, lowerBounds, elements } = value;
  const count = elements.length;
  const writer = new Writer();
  const hasNulls = elements.some((item) => item === null);
  writer.u8(1);
  writer.u8(tag);
  writer.u8(dimensions.length);
  writer.u8(hasNulls ? 1 : 0);
  dimensions.forEach((length, index) => {
    writer.i32(length);
    writer.i32(lowerBounds[index] ?? 1);
  });
  if (hasNulls) {
    const bitmap = new Uint8Array(Math.ceil(count / 8));
    elements.forEach((item, index) => {
      if (item === null) bitmap[index >>> 3] = (bitmap[index >>> 3] ?? 0) | (1 << (index & 7));
    });
    writer.raw(bitmap);
  }
  for (const item of elements) {
    if (item === null) continue;
    switch (tag) {
      case 1:
        writer.u8(item === true ? 1 : 0);
        break;
      case 2:
      case 3:
      case 8:
        writer.i32(item as number);
        break;
      case 4:
      case 9:
      case 10:
      case 11:
        writer.i64(item as bigint);
        break;
      case 5:
        writer.ensure(4);
        writer.view.setFloat32(writer.length, item as number, true);
        writer.length += 4;
        break;
      case 6:
        writer.ensure(8);
        writer.view.setFloat64(writer.length, item as number, true);
        writer.length += 8;
        break;
      case 7: {
        const decimal = item as Extract<ArrayElement, { coefficient: bigint }>;
        const special = decimal.special;
        writer.u8(
          special === undefined ? 0 : special === "NaN" ? 1 : special === "Infinity" ? 2 : 3,
        );
        if (special !== undefined) break;
        if (!(decimal.scale >= 0 && decimal.scale <= 0xffff))
          throw new MalformedArrayError("Bad scale");
        writer.ensure(2);
        writer.view.setUint16(writer.length, decimal.scale, true);
        writer.length += 2;
        const bytes = twosComplement(decimal.coefficient);
        writer.u32(bytes.length);
        writer.raw(bytes);
        break;
      }
      case 12: {
        const interval = item as Extract<ArrayElement, { months: number }>;
        writer.i32(interval.months);
        writer.i32(interval.days);
        writer.i64(interval.micros);
        break;
      }
      case 15: {
        const uuid = item as Uint8Array;
        if (uuid.length !== 16) throw new MalformedArrayError("A UUID is 16 bytes");
        writer.raw(uuid);
        break;
      }
      default: {
        const bytes = item as Uint8Array;
        writer.u32(bytes.length);
        writer.raw(bytes);
      }
    }
  }
  return writer.bytes.slice(0, writer.length);
}

export function decodeArrayValue(element: PhysicalType, bytes: Uint8Array): ArrayValue {
  const tag = arrayElementTag(element);
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  let at = 0;
  const need = (size: number) => {
    if (at + size > bytes.length) throw new MalformedArrayError("Array value is truncated");
    const start = at;
    at += size;
    return start;
  };
  if (view.getUint8(need(1)) !== 1) throw new MalformedArrayError("Unknown array version");
  if (view.getUint8(need(1)) !== tag) throw new MalformedArrayError("Array element type mismatch");
  const ndims = view.getUint8(need(1));
  const flags = view.getUint8(need(1));
  if (ndims > MAX_ARRAY_DIMENSIONS || (flags & ~1) !== 0)
    throw new MalformedArrayError("Bad array header");
  const dimensions: number[] = [];
  const lowerBounds: number[] = [];
  let count = ndims === 0 ? 0 : 1;
  for (let d = 0; d < ndims; d++) {
    const length = view.getInt32(need(4), true);
    lowerBounds.push(view.getInt32(need(4), true));
    if (length < 0) throw new MalformedArrayError("Negative array dimension");
    dimensions.push(length);
    count *= length;
  }
  if (count > bytes.length * 8) throw new MalformedArrayError("Array dimensions exceed the value");
  const nulls = (flags & 1) === 1 ? bytes.subarray(need(Math.ceil(count / 8)), at) : null;
  const elements: Array<ArrayElement | null> = [];
  for (let index = 0; index < count; index++) {
    if (nulls !== null && ((nulls[index >>> 3] ?? 0) & (1 << (index & 7))) !== 0) {
      elements.push(null);
      continue;
    }
    switch (tag) {
      case 1:
        elements.push(view.getUint8(need(1)) === 1);
        break;
      case 2:
      case 3:
      case 8:
        elements.push(view.getInt32(need(4), true));
        break;
      case 4:
      case 9:
      case 10:
      case 11:
        elements.push(view.getBigInt64(need(8), true));
        break;
      case 5:
        elements.push(view.getFloat32(need(4), true));
        break;
      case 6:
        elements.push(view.getFloat64(need(8), true));
        break;
      case 7: {
        const special = view.getUint8(need(1));
        if (special > 3) throw new MalformedArrayError("Bad NUMERIC special");
        if (special !== 0) {
          elements.push({
            coefficient: 0n,
            scale: 0,
            special: special === 1 ? "NaN" : special === 2 ? "Infinity" : "-Infinity",
          });
          break;
        }
        const scale = view.getUint16(need(2), true);
        const length = view.getUint32(need(4), true);
        elements.push({ coefficient: fromTwosComplement(bytes.subarray(need(length), at)), scale });
        break;
      }
      case 12: {
        const months = view.getInt32(need(4), true);
        const days = view.getInt32(need(4), true);
        elements.push({ months, days, micros: view.getBigInt64(need(8), true) });
        break;
      }
      case 15:
        elements.push(bytes.slice(need(16), at));
        break;
      default: {
        const length = view.getUint32(need(4), true);
        elements.push(bytes.slice(need(length), at));
      }
    }
  }
  if (at !== bytes.length) throw new MalformedArrayError("Trailing bytes after the array value");
  const value = { dimensions, lowerBounds, elements };
  // Stored bytes are checked like written values: a decoded value is always a valid one.
  validateArrayValue(element, value);
  return value;
}
