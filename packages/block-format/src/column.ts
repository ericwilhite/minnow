import type { BlockMetadata, ColumnInput, DecodedColumn, LogicalType } from "./types.js";

const textEncoder = new TextEncoder();
const textDecoder = new TextDecoder("utf-8", { fatal: true });

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

export function encodeColumn(input: ColumnInput): { bytes: Uint8Array; metadata: BlockMetadata } {
  // Index loops throughout: forEach/map skip holes in sparse arrays, which would desynchronize
  // the validity bitmap from the value payload and persist a corrupt block. A hole reads as
  // undefined and is rejected like any other non-value.
  const rowCount = input.values.length;
  const validity = new Uint8Array(bitmapLength(rowCount));
  for (let index = 0; index < rowCount; index += 1) {
    const value = input.values[index];
    if (value === undefined) throw new TypeError("Column values cannot be undefined");
    if (value !== null) setBit(validity, index);
  }

  switch (input.type) {
    case "boolean": {
      const values = new Uint8Array(bitmapLength(rowCount));
      for (let index = 0; index < rowCount; index += 1) {
        if (input.values[index] === true) setBit(values, index);
      }
      return { bytes: join(validity, values), metadata: {} };
    }
    case "number": {
      const buffer = new ArrayBuffer(rowCount * Float64Array.BYTES_PER_ELEMENT);
      const view = new DataView(buffer);
      let min = Number.POSITIVE_INFINITY;
      let max = Number.NEGATIVE_INFINITY;
      for (let index = 0; index < rowCount; index += 1) {
        const value = input.values[index];
        if (value === null || value === undefined) continue;
        if (!Number.isFinite(value)) throw new TypeError("Number values must be finite");
        view.setFloat64(index * 8, value, true);
        min = Math.min(min, value);
        max = Math.max(max, value);
      }
      const metadata = min === Number.POSITIVE_INFINITY ? {} : { zoneMap: { min, max } };
      return { bytes: join(validity, new Uint8Array(buffer)), metadata };
    }
    case "datetime": {
      const buffer = new ArrayBuffer(rowCount * Float64Array.BYTES_PER_ELEMENT);
      const view = new DataView(buffer);
      let min = Number.POSITIVE_INFINITY;
      let max = Number.NEGATIVE_INFINITY;
      for (let index = 0; index < rowCount; index += 1) {
        const value = input.values[index];
        if (value === null || value === undefined) continue;
        const milliseconds = value.getTime();
        if (!Number.isFinite(milliseconds))
          throw new TypeError("Datetime values must be valid Dates");
        view.setFloat64(index * 8, milliseconds, true);
        min = Math.min(min, milliseconds);
        max = Math.max(max, milliseconds);
      }
      const metadata = min === Number.POSITIVE_INFINITY ? {} : { zoneMap: { min, max } };
      return { bytes: join(validity, new Uint8Array(buffer)), metadata };
    }
    case "string": {
      const encoded: Array<Uint8Array | null> = new Array<Uint8Array | null>(rowCount).fill(null);
      let contentLength = 0;
      for (let index = 0; index < rowCount; index += 1) {
        const value = input.values[index];
        if (value === null || value === undefined) continue;
        const bytes = textEncoder.encode(value);
        encoded[index] = bytes;
        contentLength += bytes.byteLength;
      }
      const offsets = new Uint32Array(rowCount + 1);
      const content = new Uint8Array(contentLength);
      let offset = 0;
      for (let index = 0; index < rowCount; index += 1) {
        const bytes = encoded[index];
        if (bytes !== null && bytes !== undefined) {
          content.set(bytes, offset);
          offset += bytes.byteLength;
        }
        offsets[index + 1] = offset;
      }
      return {
        bytes: join(validity, new Uint8Array(offsets.buffer), content),
        metadata: {},
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
