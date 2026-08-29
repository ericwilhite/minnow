import type { ColumnarBatch } from "./batch.js";

/**
 * The one modeled per-value size used for flush thresholds, cache accounting, and write
 * metrics. One byte per UTF-16 code unit approximates the UTF-8 payload (exact for ASCII)
 * without encoding the string just to measure it — this estimate feeds metrics and flush
 * thresholds, not the physical format. Keeping a single implementation keeps those
 * accountings from drifting apart.
 */
export function estimateValuesBytes(values: readonly unknown[]): number {
  let bytes = 0;
  for (const value of values) {
    if (typeof value === "string") bytes += 4 + value.length;
    else if (typeof value === "number" || value instanceof Date) bytes += 8;
    else bytes += 1;
  }
  return bytes;
}

export function estimateRowBytes(row: Readonly<Record<string, unknown>>): number {
  return estimateValuesBytes(Object.values(row));
}

export function estimateBatchBytes(input: ColumnarBatch): number {
  return Object.values(input.columns).reduce(
    (total, values) => total + estimateValuesBytes(values),
    0,
  );
}
