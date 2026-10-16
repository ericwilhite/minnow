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
  const names: string[] = [];
  const positions = new Map<string, number>();
  const vectors: BatchValue[][] = [];
  /** Allocated on a column's first omission; most batches never omit anything. */
  const omissions: Array<boolean[] | undefined> = [];
  /** The last row each column was present in, to find the columns a row left out. */
  const seenIn: number[] = [];
  const omit = (position: number, index: number): void => {
    let omitted = omissions[position];
    if (omitted === undefined) {
      omitted = new Array<boolean>(rowCount).fill(false);
      omissions[position] = omitted;
    }
    omitted[index] = true;
  };
  for (let index = 0; index < rowCount; index += 1) {
    if ((index & PIVOT_SLICE_MASK) === PIVOT_SLICE_MASK) yield;
    const row = input[index];
    if (row === undefined) {
      for (let position = 0; position < names.length; position += 1) omit(position, index);
      continue;
    }
    const keys = Object.keys(row);
    // Rows of one batch nearly always carry the same keys in the same order; then each value
    // goes straight to its column without a name lookup.
    let sameShape = keys.length === names.length;
    for (let position = 0; sameShape && position < keys.length; position += 1) {
      sameShape = keys[position] === names[position];
    }
    if (sameShape) {
      for (let position = 0; position < keys.length; position += 1) {
        const value: unknown = row[names[position] as string];
        if (value === undefined) omit(position, index);
        else (vectors[position] as BatchValue[])[index] = value as BatchValue;
      }
      continue;
    }
    for (const name of keys) {
      const value: unknown = row[name];
      if (value === undefined) continue;
      let position = positions.get(name);
      if (position === undefined) {
        position = names.length;
        names.push(name);
        positions.set(name, position);
        vectors.push(new Array<BatchValue>(rowCount).fill(null));
        seenIn.push(-1);
        if (index > 0) {
          const omitted = new Array<boolean>(rowCount).fill(false);
          omitted.fill(true, 0, index);
          omissions.push(omitted);
        } else omissions.push(undefined);
      }
      (vectors[position] as BatchValue[])[index] = value as BatchValue;
      seenIn[position] = index;
    }
    for (let position = 0; position < names.length; position += 1) {
      if (seenIn[position] !== index) omit(position, index);
    }
  }
  const columns: Record<string, BatchValue[]> = {};
  const omitted: Record<string, boolean[]> = {};
  for (const [position, name] of names.entries()) {
    defineVector(columns, name, vectors[position] as BatchValue[]);
    const omittedVector = omissions[position];
    if (omittedVector !== undefined) defineVector(omitted, name, omittedVector);
  }
  return {
    columns,
    ...(Object.keys(omitted).length === 0 ? {} : { omitted }),
    rowCount,
  };
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
