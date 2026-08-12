import type { BlockMetadata, ColumnInput, DecodedColumn, LogicalType } from "./types.js";

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
  const validity = new Uint8Array(bitmapLength(rowCount));
  let nullCount = 0;
  for (let index = 0; index < rowCount; index += 1) {
    const value = input.values[index];
    if (value === undefined) throw new TypeError("Column values cannot be undefined");
    if (value !== null) setBit(validity, index);
    else nullCount += 1;
  }

  switch (input.type) {
    case "boolean": {
      const values = new Uint8Array(bitmapLength(rowCount));
      for (let index = 0; index < rowCount; index += 1) {
        if (input.values[index] === true) setBit(values, index);
      }
      return { bytes: join(validity, values), metadata: {}, nullCount };
    }
    case "number": {
      const values = new Float64Array(rowCount);
      const view = PLATFORM_LITTLE_ENDIAN ? undefined : new DataView(values.buffer);
      let min = Number.POSITIVE_INFINITY;
      let max = Number.NEGATIVE_INFINITY;
      for (let index = 0; index < rowCount; index += 1) {
        const value = input.values[index];
        if (value === null || value === undefined) continue;
        if (!Number.isFinite(value)) throw new TypeError("Number values must be finite");
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
      let min = Number.POSITIVE_INFINITY;
      let max = Number.NEGATIVE_INFINITY;
      for (let index = 0; index < rowCount; index += 1) {
        const value = input.values[index];
        if (value === null || value === undefined) continue;
        const milliseconds = value.getTime();
        if (!Number.isFinite(milliseconds))
          throw new TypeError("Datetime values must be valid Dates");
        if (view === undefined) values[index] = milliseconds;
        else view.setFloat64(index * 8, milliseconds, true);
        min = Math.min(min, milliseconds);
        max = Math.max(max, milliseconds);
      }
      const metadata = min === Number.POSITIVE_INFINITY ? {} : { zoneMap: { min, max } };
      return { bytes: join(validity, new Uint8Array(values.buffer)), metadata, nullCount };
    }
    case "string": {
      // Every value UTF-8-encodes once, directly into a shared worst-case scratch (three bytes
      // per UTF-16 code unit bounds any UTF-8 expansion) instead of allocating a throwaway
      // buffer per string and copying it into place afterwards.
      let capacity = 0;
      for (let index = 0; index < rowCount; index += 1) {
        const value = input.values[index];
        if (value !== null && value !== undefined) capacity += value.length * 3;
      }
      const offsets = new Uint32Array(rowCount + 1);
      const content = new Uint8Array(capacity);
      let offset = 0;
      for (let index = 0; index < rowCount; index += 1) {
        const value = input.values[index];
        if (value !== null && value !== undefined) {
          offset += textEncoder.encodeInto(value, content.subarray(offset)).written;
        }
        offsets[index + 1] = offset;
      }
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
  const validityLength = bitmapLength(rowCount);
  if (bytes.byteLength < validityLength) throw new Error("Truncated validity bitmap");
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
