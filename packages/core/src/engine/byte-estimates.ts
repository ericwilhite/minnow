import { sliceExpired, yieldToEventLoop } from "../work-slicer.js";
import type { ColumnarBatch } from "./batch.js";

/**
 * The one modeled per-value size used for flush thresholds, cache accounting, and write
 * metrics. One byte per UTF-16 code unit approximates the UTF-8 payload (exact for ASCII)
 * without encoding the string just to measure it — this estimate feeds metrics and flush
 * thresholds, not the physical format. Keeping a single implementation keeps those
 * accountings from drifting apart.
 */
export function estimateValuesBytes(
  values: readonly unknown[],
  /** A range, `values[start, end)`, is measured without copying it out first. */
  start = 0,
  end = values.length,
): number {
  let bytes = 0;
  for (let index = start; index < end; index += 1) {
    const value = values[index];
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

/** Values `estimateBatchBytesSliced` measures between turns. */
const ESTIMATE_SLICE_VALUES = 16_384;

/** `estimateBatchBytes` with the event loop offered a turn between slices of values. */
export async function estimateBatchBytesSliced(input: ColumnarBatch): Promise<number> {
  let total = 0;
  for (const values of Object.values(input.columns)) {
    for (let start = 0; start < values.length; start += ESTIMATE_SLICE_VALUES) {
      if (sliceExpired()) await yieldToEventLoop();
      total += estimateValuesBytes(
        values,
        start,
        Math.min(values.length, start + ESTIMATE_SLICE_VALUES),
      );
    }
  }
  return total;
}
