import type {
  BlockMetadata,
  ColumnInput,
  DecodedColumn,
  LogicalType,
  ValidatedPhysicalColumn,
} from "./types.js";
import { dateMilliseconds } from "../date-value.js";
import { physicalColumnByteLength, validatePhysicalColumn } from "./physical.js";
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
  let stringContentByteLength = 0;
  if (input.type === "string") {
    // Variable-width output still needs an exact sizing pass before allocation. The encode pass
    // below revalidates the local value it writes, so a changing accessor cannot smuggle invalid
    // UTF-16 or make offsets disagree with the content buffer.
    for (let index = 0; index < rowCount; index += 1) {
      const value = input.values[index];
      if (value === undefined) throw new TypeError("Column values cannot be undefined");
      if (value !== null) {
        if (typeof value !== "string") {
          throw new TypeError("String columns accept only strings or null");
        }
        stringContentByteLength += wellFormedUtf8ByteLength(value);
        if (!Number.isSafeInteger(stringContentByteLength)) {
          throw new RangeError("Invalid string content byte length");
        }
      }
    }
  }
  physicalColumnByteLength(input.type, rowCount, stringContentByteLength);
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
    case "string": {
      // Measure exactly without allocating, both to reject lossy unpaired surrogates and to avoid
      // the former 3x worst-case scratch for ASCII-heavy data. The complete 64 MiB format bound is
      // checked before the content or offset arrays are allocated.
      const offsets = new Uint32Array(rowCount + 1);
      // Some native encodeInto implementations stop at an internal chunk boundary when the
      // destination has exactly two or three bytes left, even though the final code point fits.
      // Three bytes of non-persisted tail room keeps the native fast path reliable; the returned
      // physical payload still contains exactly `contentByteLength` content bytes.
      const content = new Uint8Array(stringContentByteLength + 3);
      let offset = 0;
      let nullCount = 0;
      for (let index = 0; index < rowCount; index += 1) {
        const value = input.values[index];
        if (value === undefined) throw new TypeError("Column values cannot be undefined");
        if (value === null) nullCount += 1;
        else {
          if (typeof value !== "string") {
            throw new TypeError("String columns accept only strings or null");
          }
          const localByteLength = wellFormedUtf8ByteLength(value);
          setBit(validity, index);
          const result = textEncoder.encodeInto(value, content.subarray(offset));
          if (result.read !== value.length) throw new Error("UTF-8 output sizing mismatch");
          if (result.written !== localByteLength) throw new Error("UTF-8 output sizing mismatch");
          offset += result.written;
        }
        offsets[index + 1] = offset;
      }
      if (offset !== stringContentByteLength) throw new Error("UTF-8 output sizing mismatch");
      return {
        bytes: join(validity, new Uint8Array(offsets.buffer), content.subarray(0, offset)),
        metadata: {},
        nullCount,
      };
    }
  }
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
