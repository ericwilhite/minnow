/**
 * The input shapes the batch writes accept. Rows are the ordinary form — one object per row, the
 * same shape queries return. The columnar form (one array per column) is what the engine encodes,
 * so it stays available for bulk loads that already hold columns and want to skip the pivot.
 *
 * This module deliberately has no imports: the main-thread client normalizes here too, and must
 * not pull the engine in behind it.
 */

/** A value a column can hold. */
export type BatchValue = boolean | number | string | Date | null;

/** One row. A missing property is SQL omission; an explicit `null` remains SQL NULL. */
export type BatchRow = Readonly<Record<string, BatchValue>>;

/**
 * The columnar form: one ordinary readonly array per column, all of the same length and aligned by
 * index. The engine reads these arrays without copying while the asynchronous write is active;
 * callers must not mutate them or use changing accessor/proxy values until the write settles.
 */
export interface ColumnarBatch {
  columns: Readonly<Record<string, readonly BatchValue[]>>;
  /**
   * Per-row SQL omission markers. A true slot asks the engine to use the column default (or NULL
   * when no default exists), without conflating that request with an explicit NULL value.
   */
  omitted?: Readonly<Record<string, readonly boolean[]>>;
  /**
   * The batch's row count when no column carries it — a pivot of rows whose every column is
   * default-generated produces an empty column map, and the count would otherwise be lost
   * across the worker boundary.
   */
  rowCount?: number;
}

/** What `insertBatch` and `upsertBatch` take: rows, or columns for a bulk load. */
export type InsertBatchInput = readonly BatchRow[] | ColumnarBatch;

function isColumnarBatch(input: InsertBatchInput): input is ColumnarBatch {
  return !Array.isArray(input);
}

/** Pivots rows into the engine's columnar form; a columnar batch passes straight through. */
export function toColumnarBatch(input: InsertBatchInput): ColumnarBatch {
  if (isColumnarBatch(input)) return input;
  if (input.length === 0) throw new TypeError("A batch needs at least one row");
  // One pass over the rows, discovering columns in first-seen order. Values and omission are
  // separate so an explicit NULL can never accidentally invoke a default.
  const columnsByName = new Map<string, BatchValue[]>();
  const omittedByName = new Map<string, boolean[]>();
  for (let index = 0; index < input.length; index += 1) {
    const row = input[index];
    if (row === undefined) continue;
    for (const name of Object.keys(row)) {
      const value: unknown = row[name];
      if (value === undefined) continue;
      let values = columnsByName.get(name);
      if (values === undefined) {
        values = new Array<BatchValue>(input.length).fill(null);
        columnsByName.set(name, values);
        omittedByName.set(name, new Array<boolean>(input.length).fill(true));
      }
      values[index] = value as BatchValue;
      const omitted = omittedByName.get(name);
      if (omitted !== undefined) omitted[index] = false;
    }
  }
  const omitted = Object.fromEntries(
    [...omittedByName].filter(([, values]) => values.some(Boolean)),
  );
  return {
    columns: Object.fromEntries(columnsByName),
    ...(Object.keys(omitted).length === 0 ? {} : { omitted }),
    rowCount: input.length,
  };
}
