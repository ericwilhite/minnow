import type {
  BlockMetadata,
  LogicalType,
  PhysicalColumnMeasurement,
  PhysicalColumnPayload,
  PhysicalColumnRange,
  ValidatedPhysicalColumn,
} from "./types.js";

export const MAX_PHYSICAL_COLUMN_BYTE_LENGTH = 64 * 1024 * 1024;
const MAX_DATETIME_MILLISECONDS = 8_640_000_000_000_000;

interface PreparedPhysicalRange<T extends LogicalType> {
  column: ValidatedPhysicalColumn<T>;
  start: number;
  end: number;
}

interface PreparedPhysicalColumn<T extends LogicalType> {
  ranges: Array<PreparedPhysicalRange<T>>;
  measurement: PhysicalColumnMeasurement<T>;
}

export function physicalBitmapByteLength(rowCount: number): number {
  assertRowCount(rowCount);
  return Math.ceil(rowCount / 8);
}

/** Returns the exact encoded byte length for a version-zero physical column. */
export function physicalColumnByteLength(
  type: LogicalType,
  rowCount: number,
  stringContentByteLength = 0,
): number {
  assertLogicalType(type);
  const validityByteLength = physicalBitmapByteLength(rowCount);
  assertNonnegativeSafeInteger(stringContentByteLength, "string content byte length");
  let byteLength: number;
  switch (type) {
    case "boolean":
      if (stringContentByteLength !== 0)
        throw new RangeError("Boolean columns have no string content");
      byteLength = checkedAdd(validityByteLength, validityByteLength, "physical column");
      break;
    case "number":
    case "datetime":
      if (stringContentByteLength !== 0)
        throw new RangeError(`${type} columns have no string content`);
      byteLength = checkedAdd(
        validityByteLength,
        checkedMultiply(rowCount, 8, "physical column"),
        "physical column",
      );
      break;
    case "string":
      byteLength = checkedAdd(
        validityByteLength,
        checkedAdd(
          checkedMultiply(checkedAdd(rowCount, 1, "string offsets"), 4, "string offsets"),
          stringContentByteLength,
          "physical column",
        ),
        "physical column",
      );
      break;
  }
  assertPhysicalByteLength(byteLength);
  return byteLength;
}

/**
 * Validates a physical payload without copying its bytes or materializing row
 * values. The returned object retains the input `bytes` reference.
 */
export function validatePhysicalColumn<T extends LogicalType>(
  input: PhysicalColumnPayload<T>,
): ValidatedPhysicalColumn<T> {
  assertLogicalType(input.type);
  assertRowCount(input.rowCount);
  assertPhysicalByteLength(input.bytes.byteLength);
  const validityByteLength = physicalBitmapByteLength(input.rowCount);
  if (input.bytes.byteLength < validityByteLength) throw new Error("Truncated validity bitmap");
  const validity = input.bytes.subarray(0, validityByteLength);
  assertZeroPaddingBits(validity, input.rowCount, "validity bitmap");
  const nullCount = input.rowCount - countSetBits(validity, input.rowCount);
  let metadata: BlockMetadata = {};

  switch (input.type) {
    case "boolean": {
      const expectedLength = physicalColumnByteLength(input.type, input.rowCount);
      if (input.bytes.byteLength !== expectedLength) throw new Error("Invalid boolean payload");
      const values = input.bytes.subarray(validityByteLength);
      assertZeroPaddingBits(values, input.rowCount, "boolean bitmap");
      for (let index = 0; index < values.byteLength; index += 1) {
        const valueByte = values[index] ?? 0;
        const validityByte = validity[index] ?? 0;
        if ((valueByte & ~validityByte) !== 0) {
          throw new Error("Boolean value is set for a null row");
        }
      }
      break;
    }
    case "number":
    case "datetime": {
      const expectedLength = physicalColumnByteLength(input.type, input.rowCount);
      if (input.bytes.byteLength !== expectedLength)
        throw new Error(`Invalid ${input.type} payload`);
      const values = new DataView(
        input.bytes.buffer,
        input.bytes.byteOffset + validityByteLength,
        input.rowCount * 8,
      );
      let min = Number.POSITIVE_INFINITY;
      let max = Number.NEGATIVE_INFINITY;
      for (let index = 0; index < input.rowCount; index += 1) {
        if (!hasBit(validity, index)) {
          assertZeroFixedWidthNullSlot(input.bytes, validityByteLength + index * 8, input.type);
          continue;
        }
        const value = values.getFloat64(index * 8, true);
        if (!Number.isFinite(value)) {
          throw new Error(`Invalid ${input.type} physical value`);
        }
        if (
          input.type === "datetime" &&
          (!Number.isInteger(value) || Math.abs(value) > MAX_DATETIME_MILLISECONDS)
        ) {
          throw new Error("Invalid datetime physical value");
        }
        min = Math.min(min, value);
        max = Math.max(max, value);
      }
      metadata = zoneMapMetadata(min, max);
      break;
    }
    case "string": {
      const offsetsByteLength = checkedMultiply(
        checkedAdd(input.rowCount, 1, "string offsets"),
        4,
        "string offsets",
      );
      if (input.bytes.byteLength < validityByteLength + offsetsByteLength) {
        throw new Error("Invalid string offsets");
      }
      const offsets = new DataView(
        input.bytes.buffer,
        input.bytes.byteOffset + validityByteLength,
        offsetsByteLength,
      );
      const content = input.bytes.subarray(validityByteLength + offsetsByteLength);
      let previous = offsets.getUint32(0, true);
      if (previous !== 0) throw new Error("Invalid first string offset");
      for (let index = 0; index < input.rowCount; index += 1) {
        const current = offsets.getUint32((index + 1) * 4, true);
        if (current < previous || current > content.byteLength) {
          throw new Error("Invalid string offset");
        }
        if (hasBit(validity, index)) {
          assertValidUtf8(content, previous, current);
        } else if (current !== previous) {
          throw new Error("Null strings must have empty content");
        }
        previous = current;
      }
      if (previous !== content.byteLength) throw new Error("Unreferenced string bytes");
      physicalColumnByteLength(input.type, input.rowCount, content.byteLength);
      break;
    }
  }

  return {
    type: input.type,
    rowCount: input.rowCount,
    nullCount,
    bytes: input.bytes,
    metadata,
  };
}

/**
 * Computes exact sizing and metadata for concatenated half-open row ranges.
 * No output byte buffer is allocated.
 */
export function measurePhysicalColumnRanges<T extends LogicalType>(
  type: T,
  ranges: ReadonlyArray<PhysicalColumnRange<T>>,
): PhysicalColumnMeasurement<T> {
  return preparePhysicalColumn(type, ranges).measurement;
}

/** Allocates exactly `measurement.encodedByteLength` bytes and fills them. */
export function buildPhysicalColumnFromRanges<T extends LogicalType>(
  type: T,
  ranges: ReadonlyArray<PhysicalColumnRange<T>>,
): ValidatedPhysicalColumn<T> {
  const prepared = preparePhysicalColumn(type, ranges);
  const { measurement } = prepared;
  const output = new Uint8Array(measurement.encodedByteLength);
  const outputValidity = output.subarray(0, measurement.validityByteLength);

  let outputRow = 0;
  for (const range of prepared.ranges) {
    const sourceValidityLength = physicalBitmapByteLength(range.column.rowCount);
    const sourceValidity = range.column.bytes.subarray(0, sourceValidityLength);
    for (let sourceRow = range.start; sourceRow < range.end; sourceRow += 1) {
      if (hasBit(sourceValidity, sourceRow)) setBit(outputValidity, outputRow);
      outputRow += 1;
    }
  }

  switch (type) {
    case "boolean": {
      const outputValues = output.subarray(measurement.validityByteLength);
      outputRow = 0;
      for (const range of prepared.ranges) {
        const sourceBitmapLength = physicalBitmapByteLength(range.column.rowCount);
        const sourceValues = range.column.bytes.subarray(sourceBitmapLength);
        for (let sourceRow = range.start; sourceRow < range.end; sourceRow += 1) {
          if (hasBit(sourceValues, sourceRow)) setBit(outputValues, outputRow);
          outputRow += 1;
        }
      }
      break;
    }
    case "number":
    case "datetime": {
      let outputValueOffset = measurement.validityByteLength;
      for (const range of prepared.ranges) {
        const sourceValidityLength = physicalBitmapByteLength(range.column.rowCount);
        const start = sourceValidityLength + range.start * 8;
        const end = sourceValidityLength + range.end * 8;
        const sourceValues = range.column.bytes.subarray(start, end);
        output.set(sourceValues, outputValueOffset);
        outputValueOffset += sourceValues.byteLength;
      }
      break;
    }
    case "string": {
      const outputOffsetsByteLength = (measurement.rowCount + 1) * 4;
      const outputOffsets = new DataView(
        output.buffer,
        output.byteOffset + measurement.validityByteLength,
        outputOffsetsByteLength,
      );
      const outputContentOffset = measurement.validityByteLength + outputOffsetsByteLength;
      let contentByteLength = 0;
      outputRow = 0;
      outputOffsets.setUint32(0, 0, true);
      for (const range of prepared.ranges) {
        const sourceValidityLength = physicalBitmapByteLength(range.column.rowCount);
        const sourceOffsetsByteLength = (range.column.rowCount + 1) * 4;
        const sourceOffsets = new DataView(
          range.column.bytes.buffer,
          range.column.bytes.byteOffset + sourceValidityLength,
          sourceOffsetsByteLength,
        );
        const sourceContent = range.column.bytes.subarray(
          sourceValidityLength + sourceOffsetsByteLength,
        );
        const sourceContentStart = sourceOffsets.getUint32(range.start * 4, true);
        const sourceContentEnd = sourceOffsets.getUint32(range.end * 4, true);
        output.set(
          sourceContent.subarray(sourceContentStart, sourceContentEnd),
          outputContentOffset + contentByteLength,
        );
        for (let sourceRow = range.start; sourceRow < range.end; sourceRow += 1) {
          const sourceEnd = sourceOffsets.getUint32((sourceRow + 1) * 4, true);
          outputRow += 1;
          outputOffsets.setUint32(
            outputRow * 4,
            contentByteLength + sourceEnd - sourceContentStart,
            true,
          );
        }
        contentByteLength += sourceContentEnd - sourceContentStart;
      }
      break;
    }
  }

  return {
    type,
    rowCount: measurement.rowCount,
    nullCount: measurement.nullCount,
    bytes: output,
    metadata: measurement.metadata,
  };
}

export function slicePhysicalColumn<T extends LogicalType>(
  column: PhysicalColumnPayload<T>,
  start: number,
  end: number,
): ValidatedPhysicalColumn<T> {
  return buildPhysicalColumnFromRanges(column.type, [{ column, start, end }]);
}

export function concatenatePhysicalColumns<T extends LogicalType>(
  type: T,
  columns: ReadonlyArray<PhysicalColumnPayload<T>>,
): ValidatedPhysicalColumn<T> {
  return buildPhysicalColumnFromRanges(
    type,
    columns.map((column) => ({ column, start: 0, end: column.rowCount })),
  );
}

function preparePhysicalColumn<T extends LogicalType>(
  type: T,
  ranges: ReadonlyArray<PhysicalColumnRange<T>>,
): PreparedPhysicalColumn<T> {
  const preparedRanges: Array<PreparedPhysicalRange<T>> = [];
  let rowCount = 0;
  let nullCount = 0;
  let stringContentByteLength = 0;
  let min = Number.POSITIVE_INFINITY;
  let max = Number.NEGATIVE_INFINITY;

  for (const range of ranges) {
    if (range.column.type !== type) {
      throw new TypeError(`Cannot append ${range.column.type} physical rows to a ${type} column`);
    }
    const column = validatePhysicalColumn(range.column);
    assertRangeBoundary(range.start, "range start");
    assertRangeBoundary(range.end, "range end");
    if (range.start > range.end || range.end > column.rowCount) {
      throw new RangeError("Physical row range is out of bounds");
    }
    const rangeRowCount = range.end - range.start;
    rowCount = checkedAdd(rowCount, rangeRowCount, "row count");
    if (rowCount > 0xffffffff) throw new RangeError("Too many rows in one physical column");
    const sourceValidityLength = physicalBitmapByteLength(column.rowCount);
    const sourceValidity = column.bytes.subarray(0, sourceValidityLength);

    if (type === "string") {
      const sourceOffsets = new DataView(
        column.bytes.buffer,
        column.bytes.byteOffset + sourceValidityLength,
        (column.rowCount + 1) * 4,
      );
      const contentStart = sourceOffsets.getUint32(range.start * 4, true);
      const contentEnd = sourceOffsets.getUint32(range.end * 4, true);
      stringContentByteLength = checkedAdd(
        stringContentByteLength,
        contentEnd - contentStart,
        "string content byte length",
      );
    }

    const sourceValues =
      type === "number" || type === "datetime"
        ? new DataView(
            column.bytes.buffer,
            column.bytes.byteOffset + sourceValidityLength,
            column.rowCount * 8,
          )
        : undefined;
    for (let sourceRow = range.start; sourceRow < range.end; sourceRow += 1) {
      if (!hasBit(sourceValidity, sourceRow)) {
        nullCount += 1;
      } else if (sourceValues !== undefined) {
        const value = sourceValues.getFloat64(sourceRow * 8, true);
        min = Math.min(min, value);
        max = Math.max(max, value);
      }
    }
    preparedRanges.push({ column, start: range.start, end: range.end });
  }

  const validityByteLength = physicalBitmapByteLength(rowCount);
  const encodedByteLength = physicalColumnByteLength(type, rowCount, stringContentByteLength);
  const valueByteLength = encodedByteLength - validityByteLength;
  return {
    ranges: preparedRanges,
    measurement: {
      type,
      rowCount,
      nullCount,
      validityByteLength,
      valueByteLength,
      stringContentByteLength,
      encodedByteLength,
      metadata: type === "number" || type === "datetime" ? zoneMapMetadata(min, max) : {},
    },
  };
}

function zoneMapMetadata(min: number, max: number): BlockMetadata {
  return min === Number.POSITIVE_INFINITY ? {} : { zoneMap: { min, max } };
}

function countSetBits(bytes: Uint8Array, rowCount: number): number {
  let count = 0;
  for (let index = 0; index < rowCount; index += 1) {
    if (hasBit(bytes, index)) count += 1;
  }
  return count;
}

function setBit(bitmap: Uint8Array, index: number): void {
  const byteOffset = index >>> 3;
  bitmap[byteOffset] = (bitmap[byteOffset] ?? 0) | (1 << (index & 7));
}

function hasBit(bitmap: Uint8Array, index: number): boolean {
  return ((bitmap[index >>> 3] ?? 0) & (1 << (index & 7))) !== 0;
}

function assertZeroPaddingBits(bitmap: Uint8Array, rowCount: number, name: string): void {
  const usedBits = rowCount & 7;
  if (usedBits === 0 || bitmap.byteLength === 0) return;
  const finalByte = bitmap[bitmap.byteLength - 1] ?? 0;
  const usedMask = (1 << usedBits) - 1;
  if ((finalByte & ~usedMask) !== 0) throw new Error(`Non-zero padding in ${name}`);
}

function assertZeroFixedWidthNullSlot(
  bytes: Uint8Array,
  offset: number,
  type: "number" | "datetime",
): void {
  for (let index = offset; index < offset + 8; index += 1) {
    if (bytes[index] !== 0) throw new Error(`${type} value bytes are set for a null row`);
  }
}

function assertValidUtf8(bytes: Uint8Array, start: number, end: number): void {
  let index = start;
  while (index < end) {
    const first = bytes[index] ?? 0;
    if (first <= 0x7f) {
      index += 1;
      continue;
    }
    if (first >= 0xc2 && first <= 0xdf) {
      assertContinuation(bytes, index + 1, end);
      index += 2;
      continue;
    }
    if (first >= 0xe0 && first <= 0xef) {
      const second = continuation(bytes, index + 1, end);
      if ((first === 0xe0 && second < 0xa0) || (first === 0xed && second > 0x9f)) {
        throw new Error("Invalid UTF-8 string payload");
      }
      assertContinuation(bytes, index + 2, end);
      index += 3;
      continue;
    }
    if (first >= 0xf0 && first <= 0xf4) {
      const second = continuation(bytes, index + 1, end);
      if ((first === 0xf0 && second < 0x90) || (first === 0xf4 && second > 0x8f)) {
        throw new Error("Invalid UTF-8 string payload");
      }
      assertContinuation(bytes, index + 2, end);
      assertContinuation(bytes, index + 3, end);
      index += 4;
      continue;
    }
    throw new Error("Invalid UTF-8 string payload");
  }
}

function continuation(bytes: Uint8Array, index: number, end: number): number {
  if (index >= end) throw new Error("Invalid UTF-8 string payload");
  const byte = bytes[index] ?? 0;
  if (byte < 0x80 || byte > 0xbf) throw new Error("Invalid UTF-8 string payload");
  return byte;
}

function assertContinuation(bytes: Uint8Array, index: number, end: number): void {
  continuation(bytes, index, end);
}

function assertRowCount(rowCount: number): void {
  assertNonnegativeSafeInteger(rowCount, "row count");
  if (rowCount > 0xffffffff) throw new RangeError("Too many rows in one physical column");
}

function assertLogicalType(type: unknown): asserts type is LogicalType {
  if (type !== "boolean" && type !== "number" && type !== "string" && type !== "datetime") {
    throw new TypeError(`Unknown logical type: ${String(type)}`);
  }
}

function assertRangeBoundary(value: number, name: string): void {
  assertNonnegativeSafeInteger(value, name);
}

function assertNonnegativeSafeInteger(value: number, name: string): void {
  if (!Number.isSafeInteger(value) || value < 0) throw new RangeError(`Invalid ${name}`);
}

function assertPhysicalByteLength(byteLength: number): void {
  assertNonnegativeSafeInteger(byteLength, "physical column byte length");
  if (byteLength > MAX_PHYSICAL_COLUMN_BYTE_LENGTH) {
    throw new RangeError("Physical column exceeds maximum byte length");
  }
}

function checkedAdd(left: number, right: number, name: string): number {
  if (left > Number.MAX_SAFE_INTEGER - right) throw new RangeError(`Invalid ${name} length`);
  return left + right;
}

function checkedMultiply(left: number, right: number, name: string): number {
  if (left !== 0 && right > Math.floor(Number.MAX_SAFE_INTEGER / left)) {
    throw new RangeError(`Invalid ${name} length`);
  }
  return left * right;
}
