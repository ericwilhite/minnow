import type {
  BlockMetadata,
  ColumnInput,
  DecodedColumn,
  LogicalType,
  ValidatedPhysicalColumn,
} from "./types.js";
import { dateMilliseconds } from "../date-value.js";
import {
  MAX_PHYSICAL_COLUMN_BYTE_LENGTH,
  physicalColumnByteLength,
  validatePhysicalColumn,
} from "./physical.js";
import { wellFormedUtf8ByteLength } from "./unicode.js";

const textEncoder = new TextEncoder();
const textDecoder = new TextDecoder("utf-8", { fatal: true });
// The on-disk format is little-endian; bulk Float64 writes use platform order when they match.
const PLATFORM_LITTLE_ENDIAN = new Uint8Array(new Uint32Array([1]).buffer)[0] === 1;

function bitmapLength(rows: number): number {
  return Math.ceil(rows / 8);
}

function setBit(bitmap: Uint8Array, index: number): void {
  const byte = index >>> 3;
  bitmap[byte] = (bitmap[byte] ?? 0) | (1 << (index & 7));
}

function hasBit(bitmap: Uint8Array, index: number): boolean {
  return ((bitmap[index >>> 3] ?? 0) & (1 << (index & 7))) !== 0;
}

export function encodeColumn(input: ColumnInput): {
  bytes: Uint8Array;
  metadata: BlockMetadata;
  nullCount: number;
} {
  // Index loops throughout: forEach/map skip holes in sparse arrays, which would desynchronize
  // the validity bitmap from the value payload and persist a corrupt block. A hole reads as
  // undefined and is rejected like any other non-value.
  const rowCount = input.values.length;
  // Validate the fixed portion before allocation. Its format-level row cap bounds the largest
  // fixed-width allocation to about 8 MiB, so boolean, number, and datetime columns validate and
  // encode each accessor value in one pass. Reading them twice would both waste a full scan and
  // introduce a needless time-of-check/time-of-use boundary for accessor-backed inputs.
  physicalColumnByteLength(input.type, rowCount);
  if (input.type === "string") return encodeStringColumn(input.values, rowCount);
  const validity = new Uint8Array(bitmapLength(rowCount));

  switch (input.type) {
    case "boolean": {
      const values = new Uint8Array(bitmapLength(rowCount));
      let nullCount = 0;
      for (let index = 0; index < rowCount; index += 1) {
        const value = input.values[index];
        if (value === undefined) throw new TypeError("Column values cannot be undefined");
        if (value !== null && typeof value !== "boolean") {
          throw new TypeError("Boolean columns accept only booleans or null");
        }
        if (value === null) nullCount += 1;
        else {
          setBit(validity, index);
          if (value) setBit(values, index);
        }
      }
      return { bytes: join(validity, values), metadata: {}, nullCount };
    }
    case "number": {
      const values = new Float64Array(rowCount);
      const view = PLATFORM_LITTLE_ENDIAN ? undefined : new DataView(values.buffer);
      let nullCount = 0;
      let min = Number.POSITIVE_INFINITY;
      let max = Number.NEGATIVE_INFINITY;
      for (let index = 0; index < rowCount; index += 1) {
        const value = input.values[index];
        if (value === undefined) throw new TypeError("Column values cannot be undefined");
        if (value === null) {
          nullCount += 1;
          continue;
        }
        if (typeof value !== "number") {
          throw new TypeError("Number columns accept only numbers or null");
        }
        if (!Number.isFinite(value)) throw new TypeError("Number values must be finite");
        setBit(validity, index);
        if (view === undefined) values[index] = value;
        else view.setFloat64(index * 8, value, true);
        min = Math.min(min, value);
        max = Math.max(max, value);
      }
      const metadata = min === Number.POSITIVE_INFINITY ? {} : { zoneMap: { min, max } };
      return { bytes: join(validity, new Uint8Array(values.buffer)), metadata, nullCount };
    }
    case "datetime": {
      const values = new Float64Array(rowCount);
      const view = PLATFORM_LITTLE_ENDIAN ? undefined : new DataView(values.buffer);
      let nullCount = 0;
      let min = Number.POSITIVE_INFINITY;
      let max = Number.NEGATIVE_INFINITY;
      for (let index = 0; index < rowCount; index += 1) {
        const value = input.values[index];
        if (value === undefined) throw new TypeError("Column values cannot be undefined");
        if (value === null) {
          nullCount += 1;
          continue;
        }
        if (!(value instanceof Date)) {
          throw new TypeError("Datetime columns accept only Date objects or null");
        }
        const milliseconds = dateMilliseconds(value);
        if (!Number.isFinite(milliseconds))
          throw new TypeError("Datetime values must be valid Dates");
        setBit(validity, index);
        if (view === undefined) values[index] = milliseconds;
        else view.setFloat64(index * 8, milliseconds, true);
        min = Math.min(min, milliseconds);
        max = Math.max(max, milliseconds);
      }
      const metadata = min === Number.POSITIVE_INFINITY ? {} : { zoneMap: { min, max } };
      return { bytes: join(validity, new Uint8Array(values.buffer)), metadata, nullCount };
    }
  }
}

/**
 * A string column's payload: validity, offsets, then each value's UTF-8 bytes. Every value is
 * read exactly once, into a local snapshot, so the bytes written are the strings that were
 * checked even for accessor-backed input. ASCII — nearly every value in practice — is copied a
 * code unit to a byte while it is checked, into a buffer the UTF-16 length already sizes exactly,
 * with no native call per value. The first value that is not ASCII sends the column to the exact
 * encoding: measured first, both to reject unpaired surrogates (UTF-8 cannot represent them, and
 * TextEncoder would silently replace them) and to check the 64 MiB format bound before the
 * content buffer is allocated. Errors arrive in row order either way, exactly as one checking
 * pass would raise them.
 */
function encodeStringColumn(
  values: ReadonlyArray<string | null>,
  rowCount: number,
): { bytes: Uint8Array; metadata: BlockMetadata; nullCount: number } {
  // A capacity guess, not a check: every code unit is at least one UTF-8 byte and an ASCII one
  // is exactly one, so for an all-ASCII column this is the payload. A guess past the format bound
  // allocates nothing and lets the exact path refuse the column.
  let units = 0;
  for (let index = 0; index < rowCount; index += 1) {
    const value: unknown = values[index];
    if (typeof value === "string") units += value.length;
  }
  const validity = new Uint8Array(bitmapLength(rowCount));
  const offsets = new Uint32Array(rowCount + 1);
  const snapshot = new Array<string | null>(rowCount);
  const ascii = new Uint8Array(units <= MAX_PHYSICAL_COLUMN_BYTE_LENGTH ? units : 0);
  let offset = 0;
  let copying = true;
  let byteLength = 0;
  let nullCount = 0;
  for (let index = 0; index < rowCount; index += 1) {
    const value = values[index];
    if (value === undefined) throw new TypeError("Column values cannot be undefined");
    if (value === null) {
      nullCount += 1;
      snapshot[index] = null;
      offsets[index + 1] = offset;
      continue;
    }
    if (typeof value !== "string") {
      throw new TypeError("String columns accept only strings or null");
    }
    snapshot[index] = value;
    setBit(validity, index);
    const length = value.length;
    if (copying) {
      let unit = 0;
      if (offset + length <= ascii.length) {
        for (; unit < length; unit += 1) {
          const code = value.charCodeAt(unit);
          if (code > 0x7f) break;
          ascii[offset + unit] = code;
        }
      }
      if (unit === length && offset + length <= ascii.length) {
        offset += length;
        byteLength += length;
        offsets[index + 1] = offset;
        continue;
      }
      copying = false;
    }
    byteLength += wellFormedUtf8ByteLength(value);
    if (!Number.isSafeInteger(byteLength)) {
      throw new RangeError("Invalid string content byte length");
    }
  }
  physicalColumnByteLength("string", rowCount, byteLength);
  if (copying) {
    return {
      bytes: join(validity, new Uint8Array(offsets.buffer), ascii.subarray(0, offset)),
      metadata: {},
      nullCount,
    };
  }
  // Some native encodeInto implementations stop at an internal chunk boundary when the
  // destination has exactly two or three bytes left, even though the final code point fits.
  // Three bytes of non-persisted tail room keeps the native fast path reliable; the returned
  // physical payload still contains exactly `byteLength` content bytes.
  const content = new Uint8Array(byteLength + 3);
  offset = 0;
  for (let index = 0; index < rowCount; index += 1) {
    const value = snapshot[index];
    if (value !== null && value !== undefined) {
      const result = textEncoder.encodeInto(value, content.subarray(offset));
      if (result.read !== value.length) throw new Error("UTF-8 output sizing mismatch");
      offset += result.written;
    }
    offsets[index + 1] = offset;
  }
  if (offset !== byteLength) throw new Error("UTF-8 output sizing mismatch");
  return {
    bytes: join(validity, new Uint8Array(offsets.buffer), content.subarray(0, offset)),
    metadata: {},
    nullCount,
  };
}

export function decodeColumn<T extends LogicalType>(
  type: T,
  bytes: Uint8Array,
  rowCount: number,
): DecodedColumn<T> {
  return decodeValidatedColumn(validatePhysicalColumn({ type, bytes, rowCount }));
}

/** Package-internal row materialization after the physical payload was already validated. */
export function decodeValidatedColumn<T extends LogicalType>(
  column: ValidatedPhysicalColumn<T>,
): DecodedColumn<T> {
  const { type, bytes, rowCount } = column;
  const validityLength = bitmapLength(rowCount);
  const validity = bytes.subarray(0, validityLength);
  const payload = bytes.subarray(validityLength);
  const values: unknown[] = [];

  if (type === "boolean") {
    if (payload.byteLength !== bitmapLength(rowCount)) throw new Error("Invalid boolean payload");
    for (let index = 0; index < rowCount; index += 1) {
      values.push(hasBit(validity, index) ? hasBit(payload, index) : null);
    }
  } else if (type === "number" || type === "datetime") {
    if (payload.byteLength !== rowCount * 8) throw new Error(`Invalid ${type} payload`);
    const view = new DataView(payload.buffer, payload.byteOffset, payload.byteLength);
    for (let index = 0; index < rowCount; index += 1) {
      const value = view.getFloat64(index * 8, true);
      values.push(hasBit(validity, index) ? (type === "datetime" ? new Date(value) : value) : null);
    }
  } else {
    const offsetsLength = (rowCount + 1) * 4;
    if (payload.byteLength < offsetsLength) throw new Error("Invalid string offsets");
    const offsets = new DataView(payload.buffer, payload.byteOffset, offsetsLength);
    const content = payload.subarray(offsetsLength);
    let previous = 0;
    for (let index = 0; index <= rowCount; index += 1) {
      const current = offsets.getUint32(index * 4, true);
      if (current < previous || current > content.byteLength)
        throw new Error("Invalid string offset");
      if (index > 0) {
        values.push(
          hasBit(validity, index - 1)
            ? textDecoder.decode(content.subarray(previous, current))
            : null,
        );
      }
      previous = current;
    }
    if (previous !== content.byteLength) throw new Error("Unreferenced string bytes");
  }
  return { type, values } as DecodedColumn<T>;
}

function join(...parts: Uint8Array[]): Uint8Array {
  const output = new Uint8Array(parts.reduce((total, part) => total + part.byteLength, 0));
  let offset = 0;
  for (const part of parts) {
    output.set(part, offset);
    offset += part.byteLength;
  }
  return output;
}
