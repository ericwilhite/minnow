import {
  MAX_BLOCK_ROW_COUNT,
  physicalColumnByteLength,
  wellFormedUtf8ByteLength,
  type LogicalType,
} from "../block-format/index.js";

export interface WriteColumnValues {
  readonly type: LogicalType;
  readonly values: readonly unknown[];
  readonly stringByteLengths?: readonly number[];
}

export interface WriteBlockRange {
  readonly start: number;
  readonly end: number;
}

/**
 * Plans aligned row groups for every column in one segment. The row-wise accumulator visits each
 * string value once, plus at most one overflow recheck at a block boundary, so a late wide column
 * cannot make its siblings rescan the rest of the batch for every output block.
 */
export function planAlignedWriteBlockRanges(
  columns: readonly WriteColumnValues[],
  rowCount: number,
  maximumRows: number,
  targetBytes: number,
  measureString: (value: string) => number = wellFormedUtf8ByteLength,
): WriteBlockRange[] {
  if (rowCount === 0) return [];
  if (!Number.isSafeInteger(maximumRows) || maximumRows <= 0 || maximumRows > MAX_BLOCK_ROW_COUNT) {
    throw new RangeError("Maximum rows per write block exceeds the format limit");
  }
  for (const column of columns) {
    if (column.values.length !== rowCount) {
      throw new RangeError("Column lengths must match while planning write blocks");
    }
  }
  let maximumRowsPerRange = maximumRows;
  const stringColumns: WriteColumnValues[] = [];
  for (const column of columns) {
    if (column.type === "string") stringColumns.push(column);
    else {
      maximumRowsPerRange = Math.min(
        maximumRowsPerRange,
        maximumFixedWidthRows(column.type, maximumRows, targetBytes),
      );
    }
  }
  const ranges: WriteBlockRange[] = [];
  if (stringColumns.length === 0) {
    // Fixed-width sizes depend only on row count. Do not add a value-reading pass to numeric and
    // boolean writes: calculate the cap once and emit the arithmetic ranges directly.
    for (let start = 0; start < rowCount; start += maximumRowsPerRange) {
      ranges.push({ start, end: Math.min(rowCount, start + maximumRowsPerRange) });
    }
    return ranges;
  }
  for (let start = 0; start < rowCount;) {
    const rowLimit = Math.min(rowCount, start + maximumRowsPerRange);
    const stringContentBytes = new Array<number>(stringColumns.length).fill(0);
    let end = start;
    for (let row = start; row < rowLimit; row += 1) {
      const count = row - start + 1;
      let fitsTarget = true;
      for (let columnIndex = 0; columnIndex < stringColumns.length; columnIndex += 1) {
        const column = stringColumns[columnIndex];
        if (column === undefined) continue;
        let contentBytes = stringContentBytes[columnIndex] ?? 0;
        const value = column.values[row];
        if (value !== null) {
          if (typeof value !== "string") {
            throw new TypeError("String column contains a non-string");
          }
          contentBytes += column.stringByteLengths?.[row] ?? measureString(value);
          stringContentBytes[columnIndex] = contentBytes;
        }
        if (physicalColumnBytesUnchecked("string", count, contentBytes) > targetBytes) {
          fitsTarget = false;
        }
      }
      if (!fitsTarget && count > 1) break;
      if (!fitsTarget) {
        // The target is soft for one wide row, but the format ceiling is always hard. Validate
        // every string column's exact one-row size before allowing that row to make progress.
        for (let columnIndex = 0; columnIndex < stringColumns.length; columnIndex += 1) {
          physicalColumnByteLength("string", 1, stringContentBytes[columnIndex] ?? 0);
        }
      }
      end = row + 1;
      if (!fitsTarget) break;
    }
    if (end <= start) throw new Error("Write block planner made no progress");
    ranges.push({ start, end });
    start = end;
  }
  return ranges;
}

function maximumFixedWidthRows(
  type: Exclude<LogicalType, "string">,
  maximumRows: number,
  targetBytes: number,
): number {
  if (physicalColumnBytesUnchecked(type, maximumRows, 0) <= targetBytes) return maximumRows;
  if (physicalColumnBytesUnchecked(type, 1, 0) > targetBytes) return 1;
  let low = 1;
  let high = maximumRows;
  while (low < high) {
    const middle = Math.ceil((low + high) / 2);
    if (physicalColumnBytesUnchecked(type, middle, 0) <= targetBytes) low = middle;
    else high = middle - 1;
  }
  return low;
}

/** Conservative first estimate used by both physical compaction planners. */
export function estimateCompactionRowsPerOutput(
  targetBlockBytes: number,
  maximumEncodedBytesPerRow: number,
): number {
  return Math.max(
    1,
    Math.min(MAX_BLOCK_ROW_COUNT, Math.floor(targetBlockBytes / maximumEncodedBytesPerRow)),
  );
}

/** Exact physical size without applying the 64 MiB hard limit during range search. */
function physicalColumnBytesUnchecked(
  type: LogicalType,
  rowCount: number,
  stringContentBytes: number,
): number {
  const validityBytes = Math.ceil(rowCount / 8);
  switch (type) {
    case "boolean":
      return validityBytes * 2;
    case "number":
    case "datetime":
      return validityBytes + rowCount * 8;
    case "string":
      return validityBytes + (rowCount + 1) * 4 + stringContentBytes;
  }
}
