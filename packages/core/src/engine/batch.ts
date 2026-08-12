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

/** One row. Columns left out are written as null, so nullable columns can be omitted. */
export type BatchRow = Readonly<Record<string, BatchValue>>;

/** The columnar form: one array per column, all of the same length, aligned by index. */
export interface ColumnarBatch {
  columns: Readonly<Record<string, readonly BatchValue[]>>;
}

/** What `insertBatch` and `upsertBatch` take: rows, or columns for a bulk load. */
export type InsertBatchInput = readonly BatchRow[] | ColumnarBatch;

export function isColumnarBatch(input: InsertBatchInput): input is ColumnarBatch {
  return !Array.isArray(input);
}

/** Pivots rows into the engine's columnar form; a columnar batch passes straight through. */
export function toColumnarBatch(input: InsertBatchInput): ColumnarBatch {
  if (isColumnarBatch(input)) return input;
  if (input.length === 0) throw new TypeError("A batch needs at least one row");
  // One pass over the rows, discovering columns in first-seen order. A column no row mentions
  // stays absent, so the write still fails with "Missing column"; a column only some rows
  // mention keeps its pre-filled nulls, which the per-column nullability check then accepts or
  // rejects. Reading each row object once beats one full re-walk of every row per column.
  const columnsByName = new Map<string, BatchValue[]>();
  for (let index = 0; index < input.length; index += 1) {
    const row = input[index];
    if (row === undefined) continue;
    for (const name of Object.keys(row)) {
      let values = columnsByName.get(name);
      if (values === undefined) {
        values = new Array<BatchValue>(input.length).fill(null);
        columnsByName.set(name, values);
      }
      values[index] = row[name] ?? null;
    }
  }
  return { columns: Object.fromEntries(columnsByName) };
}
