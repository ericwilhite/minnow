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

/**
 * `ColumnarBatch` as the schema-typed overloads hand it to the erased implementation: a column
 * vector or omission mask may be `undefined`, meaning that column is not in the batch. Only the
 * type is looser — `toColumnarBatch` drops such entries, so nothing downstream sees them.
 */
export interface ColumnarBatchLike {
  readonly columns: Readonly<Record<string, readonly BatchValue[] | undefined>>;
  readonly omitted?: Readonly<Record<string, readonly boolean[] | undefined>>;
  readonly rowCount?: number;
}

export type InsertBatchInputLike = readonly BatchRow[] | ColumnarBatchLike;

/** The entries of a per-column record whose vector is present. */
export function definedVectors<T>(
  vectors: Readonly<Record<string, T | undefined>>,
): Record<string, T> {
  const defined: Record<string, T> = {};
  for (const [name, vector] of Object.entries(vectors)) {
    if (vector !== undefined) {
      if (name === "__proto__") {
        Object.defineProperty(defined, name, {
          value: vector,
          enumerable: true,
          configurable: true,
          writable: true,
        });
      } else defined[name] = vector;
    }
  }
  return defined;
}

function hasUndefinedVector(vectors: Readonly<Record<string, unknown>>): boolean {
  return Object.values(vectors).includes(undefined);
}

function isColumnarBatch(input: InsertBatchInputLike): input is ColumnarBatchLike {
  return !Array.isArray(input);
}

/** Pivots rows into the engine's columnar form; a columnar batch passes straight through. */
export function toColumnarBatch(input: InsertBatchInputLike): ColumnarBatch {
  const steps = toColumnarBatchSteps(input);
  let step = steps.next();
  while (step.done !== true) step = steps.next();
  return step.value;
}

/** Rows `toColumnarBatchSteps` pivots between yields, less one: a mask. */
const PIVOT_SLICE_MASK = 4_095;

/**
 * `toColumnarBatch` as steps: the pivot yields every few thousand rows, so a caller with a
 * large batch can hand the event loop a turn between them.
 */
export function* toColumnarBatchSteps(input: InsertBatchInputLike): Generator<void, ColumnarBatch> {
  if (isColumnarBatch(input)) {
    const columnsComplete = !hasUndefinedVector(input.columns);
    const omittedComplete = input.omitted === undefined || !hasUndefinedVector(input.omitted);
    if (columnsComplete && omittedComplete) return input as ColumnarBatch;
    return {
      columns: definedVectors(input.columns),
      ...(input.omitted === undefined ? {} : { omitted: definedVectors(input.omitted) }),
      ...(input.rowCount === undefined ? {} : { rowCount: input.rowCount }),
    };
  }
  if (input.length === 0) throw new TypeError("A batch needs at least one row");
  // One pass over the rows, discovering columns in first-seen order. Values and omission are
  // separate so an explicit NULL can never accidentally invoke a default.
  const rowCount = input.length;
  /** Columns in first-seen order. */
  const found: PivotColumn[] = [];
  const byName = new Map<string, PivotColumn>();
  for (let index = 0; index < rowCount; index += 1) {
    if ((index & PIVOT_SLICE_MASK) === PIVOT_SLICE_MASK) yield;
    const row = input[index];
    if (row === undefined) {
      for (const column of found) omitRow(column, index, rowCount);
      continue;
    }
    const keys = Object.keys(row);
    // Rows of one batch nearly always carry the same keys in the same order; then each value
    // goes straight to its column without a name lookup.
    let sameShape = keys.length === found.length;
    for (let position = 0; sameShape && position < keys.length; position += 1) {
      sameShape = keys[position] === found[position]?.name;
    }
    if (sameShape) {
      for (const column of found) {
        const value: unknown = row[column.name];
        if (value === undefined) omitRow(column, index, rowCount);
        else column.values[index] = value as BatchValue;
      }
      continue;
    }
    for (const name of keys) {
      const value: unknown = row[name];
      if (value === undefined) continue;
      let column = byName.get(name);
      if (column === undefined) {
        column = {
          name,
          values: new Array<BatchValue>(rowCount).fill(null),
          // A column first seen past row 0 was omitted by every row before it.
          omitted:
            index === 0 ? undefined : new Array<boolean>(rowCount).fill(false).fill(true, 0, index),
          seenIn: -1,
        };
        found.push(column);
        byName.set(name, column);
      }
      column.values[index] = value as BatchValue;
      column.seenIn = index;
    }
    for (const column of found) if (column.seenIn !== index) omitRow(column, index, rowCount);
  }
  const columns: Record<string, BatchValue[]> = {};
  const omitted: Record<string, boolean[]> = {};
  for (const column of found) {
    defineVector(columns, column.name, column.values);
    if (column.omitted !== undefined) defineVector(omitted, column.name, column.omitted);
  }
  return {
    columns,
    ...(Object.keys(omitted).length === 0 ? {} : { omitted }),
    rowCount,
  };
}

interface PivotColumn {
  readonly name: string;
  readonly values: BatchValue[];
  /** Allocated on the column's first omission; most batches never omit anything. */
  omitted: boolean[] | undefined;
  /** The last row the column was present in, to find the columns a row left out. */
  seenIn: number;
}

function omitRow(column: PivotColumn, index: number, rowCount: number): void {
  column.omitted ??= new Array<boolean>(rowCount).fill(false);
  column.omitted[index] = true;
}

/** Stores a column vector under its name, `__proto__` included, as an own property. */
function defineVector<T>(target: Record<string, T>, name: string, vector: T): void {
  Object.defineProperty(target, name, {
    value: vector,
    enumerable: true,
    configurable: true,
    writable: true,
  });
}
