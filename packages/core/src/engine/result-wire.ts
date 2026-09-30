import { dateMilliseconds } from "../date-value.js";
import { validateSqlDomain, type SqlDomain } from "../storage/types.js";
import type { QueryResult, QueryRow, QueryValue } from "./query.js";
import { unknownColumnDomains } from "./query.js";

/**
 * The shape a query result takes on the worker channel. A result is rows of objects at the
 * public API on both sides, but row objects are the slowest thing structured clone can be
 * handed: every row is a fresh object with its own property names. Pivoted into one array per
 * column — numbers, booleans, and datetimes in typed arrays whose buffers are transferred, and
 * strings as one flat text with offsets — the same 20,000-row result crosses in a fraction of the
 * time, and the receiving side rebuilds the rows column by column in tight loops.
 *
 * Every value round-trips exactly: NaN, -0, and Infinity survive in a Float64Array, datetimes
 * travel as epoch milliseconds (an invalid Date's NaN included), nulls in a typed column live in
 * a byte mask, and a column name like `__proto__` comes back as the own property the engine
 * defined — never as a prototype assignment.
 */

type WireResultColumn =
  | { kind: "number"; values: Float64Array; nulls?: Uint8Array }
  | { kind: "boolean"; values: Uint8Array; nulls?: Uint8Array }
  | { kind: "datetime"; values: Float64Array; nulls?: Uint8Array }
  /**
   * Every string in the column joined into one flat text, with `offsets[i]..offsets[i + 1]`
   * delimiting row i. Structured clone copies one string instead of walking thousands, and the
   * receiver slices rows back out of it — far cheaper on the main thread than an array of
   * separate strings, which is the form the engine's decoded values would otherwise take.
   */
  | { kind: "string"; text: string; offsets: Uint32Array; nulls?: Uint8Array }
  /** A column holding more than one type, which SQL allows (`SELECT CASE … END`). */
  | { kind: "mixed"; values: QueryValue[] }
  /** Every value null; nothing to carry beyond the row count. */
  | { kind: "null" };

export interface WireQueryResult {
  readonly kind: "columnar-result";
  readonly columns: string[];
  readonly columnDomains: Array<SqlDomain | null>;
  readonly rowCount: number;
  readonly values: WireResultColumn[];
}

export interface EncodedQueryResult {
  payload: WireQueryResult;
  /** The typed-array buffers, freshly allocated here, so postMessage may transfer them. */
  transfer: ArrayBuffer[];
}

export function encodeQueryResult(result: QueryResult): EncodedQueryResult {
  return encodeRows(result.columns, result.rows, result.columnDomains);
}

/**
 * Encodes a bare row array (`run()` returns rows without a column list). Every row of one result
 * carries the same keys in the same order, so the first row names the columns.
 */
export function encodeQueryRows(rows: readonly QueryRow[]): EncodedQueryResult {
  const first = rows[0];
  const columns = first === undefined ? [] : Object.keys(first);
  return encodeRows(columns, rows, unknownColumnDomains(columns));
}

export function decodeQueryResult(payload: unknown): QueryResult {
  if (!isWireQueryResult(payload)) {
    throw new TypeError("Expected a columnar query result frame");
  }
  validateResultShape(payload.rowCount, payload.columns.length);
  const columns = [...payload.columns];
  if (payload.values.length !== columns.length || payload.columnDomains.length !== columns.length) {
    throw new TypeError("Columnar result frame metadata is not aligned");
  }
  for (const name of columns) {
    if (typeof name !== "string")
      throw new TypeError("Columnar result column names must be strings");
  }
  for (const domain of payload.columnDomains as readonly unknown[]) {
    if (domain === null) continue;
    if (
      typeof domain !== "object" ||
      !["numeric", "json", "jsonb", "uuid", "date", "time", "interval", "array", "enum"].includes(
        String((domain as { kind?: unknown }).kind),
      )
    ) {
      throw new TypeError("Invalid columnar result domain metadata");
    }
    validateSqlDomain(domain as SqlDomain, "worker result");
  }
  // Validate lengths before allocating rows: a missing vector must never fabricate SQL values.
  for (const column of payload.values) validateColumn(column, payload.rowCount);
  const columnDomains = structuredClone(payload.columnDomains);
  const rows: QueryRow[] = [];
  for (let index = 0; index < payload.rowCount; index += 1) rows.push({});
  // Column by column: each fill loop stores one property name into every row, which the engine
  // runs noticeably faster than visiting every column of each row in turn.
  for (const [position, name] of columns.entries()) {
    const column = payload.values[position];
    if (column === undefined) throw new TypeError("Columnar result frame is missing a column");
    fillColumn(rows, name, column);
  }
  return { columns, columnDomains, rows };
}

function validateResultShape(rowCount: number, columnCount: number): void {
  if (!Number.isSafeInteger(rowCount) || rowCount < 0 || rowCount > 1_000_000) {
    throw new TypeError("Columnar result frame row count must be between 0 and 1,000,000");
  }
  if (columnCount > 16_384 || rowCount * columnCount > 4_000_000) {
    throw new RangeError(
      "Worker result exceeds its column/cell limit; narrow the projection or use a query cursor",
    );
  }
}

function validateColumn(input: unknown, rowCount: number): void {
  const fail = (): never => {
    throw new TypeError("Invalid columnar result column encoding");
  };
  if (typeof input !== "object" || input === null) return fail();
  const column = input as {
    kind?: unknown;
    nulls?: unknown;
    values?: unknown;
    text?: unknown;
    offsets?: unknown;
  };
  if ("nulls" in column && column.nulls !== undefined) {
    if (!(column.nulls instanceof Uint8Array) || column.nulls.length !== rowCount) return fail();
    for (const value of column.nulls) if (value !== 0 && value !== 1) return fail();
  }
  switch (column.kind) {
    case "null":
      return;
    case "number":
    case "datetime":
      if (!(column.values instanceof Float64Array) || column.values.length !== rowCount)
        return fail();
      if (column.kind === "datetime")
        for (let row = 0; row < rowCount; row += 1) {
          if (column.nulls instanceof Uint8Array && column.nulls[row] === 1) continue;
          const value = column.values[row];
          if (typeof value === "number" && Number.isNaN(value)) continue; // Explicit Invalid Date sentinel.
          if (
            value === undefined ||
            !Number.isSafeInteger(value) ||
            Math.abs(value) > 8_640_000_000_000_000
          )
            return fail();
        }
      return;
    case "boolean":
      if (!(column.values instanceof Uint8Array) || column.values.length !== rowCount)
        return fail();
      for (const value of column.values) if (value !== 0 && value !== 1) return fail();
      return;
    case "string": {
      if (
        typeof column.text !== "string" ||
        !(column.offsets instanceof Uint32Array) ||
        column.offsets.length !== rowCount + 1 ||
        column.offsets[0] !== 0 ||
        column.offsets[rowCount] !== column.text.length
      )
        return fail();
      let previous = 0;
      for (const offset of column.offsets) {
        if (offset < previous || offset > column.text.length) return fail();
        previous = offset;
      }
      return;
    }
    case "mixed":
      if (!Array.isArray(column.values) || column.values.length !== rowCount) return fail();
      for (const value of column.values as readonly unknown[]) {
        if (
          value !== null &&
          typeof value !== "number" &&
          typeof value !== "string" &&
          typeof value !== "boolean" &&
          !(value instanceof Date)
        )
          return fail();
      }
      return;
    default:
      return fail();
  }
}

export function isWireQueryResult(value: unknown): value is WireQueryResult {
  return (
    typeof value === "object" &&
    value !== null &&
    (value as { kind?: unknown }).kind === "columnar-result" &&
    Array.isArray((value as { columns?: unknown }).columns) &&
    Array.isArray((value as { columnDomains?: unknown }).columnDomains) &&
    Array.isArray((value as { values?: unknown }).values) &&
    typeof (value as { rowCount?: unknown }).rowCount === "number"
  );
}

function encodeRows(
  columns: readonly string[],
  rows: readonly QueryRow[],
  columnDomains: ReadonlyArray<SqlDomain | null>,
): EncodedQueryResult {
  if (columnDomains.length !== columns.length) {
    throw new TypeError("Query result column domains must align with the result columns");
  }
  validateResultShape(rows.length, columns.length);
  const transfer: ArrayBuffer[] = [];
  const values = columns.map((name) => {
    const column = encodeColumn(name, rows);
    if (column.kind === "number" || column.kind === "boolean" || column.kind === "datetime") {
      transfer.push(column.values.buffer as ArrayBuffer);
      if (column.nulls !== undefined) transfer.push(column.nulls.buffer as ArrayBuffer);
    } else if (column.kind === "string") {
      transfer.push(column.offsets.buffer as ArrayBuffer);
      if (column.nulls !== undefined) transfer.push(column.nulls.buffer as ArrayBuffer);
    }
    return column;
  });
  return {
    payload: {
      kind: "columnar-result",
      columns: [...columns],
      columnDomains: structuredClone([...columnDomains]),
      rowCount: rows.length,
      values,
    },
    transfer,
  };
}

/**
 * The first non-null value picks the column's typed form; a later value of another type demotes
 * the whole column to the mixed form, which is rare enough to restart from scratch rather than
 * to plan for.
 */
function encodeColumn(name: string, rows: readonly QueryRow[]): WireResultColumn {
  const rowCount = rows.length;
  let start = 0;
  while (start < rowCount && (rows[start]?.[name] ?? null) === null) start += 1;
  const first = rows[start]?.[name] ?? null;
  if (first === null) return { kind: "null" };
  if (typeof first === "number") {
    const values = new Float64Array(rowCount);
    let nulls = start === 0 ? undefined : nullMask(rowCount, start);
    for (let index = start; index < rowCount; index += 1) {
      const value = rows[index]?.[name] ?? null;
      if (value === null) {
        nulls ??= new Uint8Array(rowCount);
        nulls[index] = 1;
      } else if (typeof value === "number") {
        values[index] = value;
      } else {
        return encodeMixed(name, rows);
      }
    }
    return nulls === undefined ? { kind: "number", values } : { kind: "number", values, nulls };
  }
  if (typeof first === "boolean") {
    const values = new Uint8Array(rowCount);
    let nulls = start === 0 ? undefined : nullMask(rowCount, start);
    for (let index = start; index < rowCount; index += 1) {
      const value = rows[index]?.[name] ?? null;
      if (value === null) {
        nulls ??= new Uint8Array(rowCount);
        nulls[index] = 1;
      } else if (typeof value === "boolean") {
        values[index] = value ? 1 : 0;
      } else {
        return encodeMixed(name, rows);
      }
    }
    return nulls === undefined ? { kind: "boolean", values } : { kind: "boolean", values, nulls };
  }
  if (first instanceof Date) {
    const values = new Float64Array(rowCount);
    let nulls = start === 0 ? undefined : nullMask(rowCount, start);
    for (let index = start; index < rowCount; index += 1) {
      const value = rows[index]?.[name] ?? null;
      if (value === null) {
        nulls ??= new Uint8Array(rowCount);
        nulls[index] = 1;
      } else if (value instanceof Date) {
        values[index] = dateMilliseconds(value);
      } else {
        return encodeMixed(name, rows);
      }
    }
    return nulls === undefined ? { kind: "datetime", values } : { kind: "datetime", values, nulls };
  }
  if (typeof first === "string") {
    const offsets = new Uint32Array(rowCount + 1);
    let nulls = start === 0 ? undefined : nullMask(rowCount, start);
    const parts: string[] = [];
    let length = 0;
    for (let index = start; index < rowCount; index += 1) {
      const value = rows[index]?.[name] ?? null;
      if (value === null) {
        nulls ??= new Uint8Array(rowCount);
        nulls[index] = 1;
      } else if (typeof value === "string") {
        parts.push(value);
        length += value.length;
      } else {
        return encodeMixed(name, rows);
      }
      offsets[index + 1] = length;
    }
    const text = parts.join("");
    return nulls === undefined
      ? { kind: "string", text, offsets }
      : { kind: "string", text, offsets, nulls };
  }
  return encodeMixed(name, rows);
}

function encodeMixed(name: string, rows: readonly QueryRow[]): WireResultColumn {
  return { kind: "mixed", values: rows.map((row) => row[name] ?? null) };
}

/** A null mask whose first `nullCount` rows are already null. */
function nullMask(rowCount: number, nullCount: number): Uint8Array {
  const nulls = new Uint8Array(rowCount);
  nulls.fill(1, 0, nullCount);
  return nulls;
}

/** Stores one decoded column into every row under `name`. */
function fillColumn(rows: QueryRow[], name: string, column: WireResultColumn): void {
  if (name === "__proto__") {
    // Assignment through `row["__proto__"]` would set the prototype; this column goes through
    // Object.defineProperty, which is what the engine itself does for the name.
    const values = decodeColumnValues(column, rows.length);
    let index = 0;
    for (const row of rows) {
      Object.defineProperty(row, name, {
        value: values[index] ?? null,
        enumerable: true,
        configurable: true,
        writable: true,
      });
      index += 1;
    }
    return;
  }
  let index = 0;
  switch (column.kind) {
    case "number": {
      const { values, nulls } = column;
      if (nulls === undefined) {
        for (const row of rows) {
          row[name] = values[index] ?? 0;
          index += 1;
        }
      } else {
        for (const row of rows) {
          row[name] = nulls[index] === 1 ? null : (values[index] ?? 0);
          index += 1;
        }
      }
      return;
    }
    case "boolean": {
      const { values, nulls } = column;
      for (const row of rows) {
        row[name] = nulls?.[index] === 1 ? null : values[index] === 1;
        index += 1;
      }
      return;
    }
    case "datetime": {
      const { values, nulls } = column;
      for (const row of rows) {
        row[name] = nulls?.[index] === 1 ? null : new Date(values[index] ?? 0);
        index += 1;
      }
      return;
    }
    case "string": {
      const { text, offsets, nulls } = column;
      for (const row of rows) {
        row[name] =
          nulls?.[index] === 1 ? null : text.slice(offsets[index] ?? 0, offsets[index + 1] ?? 0);
        index += 1;
      }
      return;
    }
    case "mixed": {
      const { values } = column;
      for (const row of rows) {
        row[name] = values[index] ?? null;
        index += 1;
      }
      return;
    }
    case "null":
      for (const row of rows) row[name] = null;
  }
}

/** One column back into plain values, for the one name that cannot be assigned through. */
function decodeColumnValues(column: WireResultColumn, rowCount: number): QueryValue[] {
  const values: QueryValue[] = [];
  for (let index = 0; index < rowCount; index += 1) {
    switch (column.kind) {
      case "number":
        values.push(column.nulls?.[index] === 1 ? null : (column.values[index] ?? 0));
        break;
      case "boolean":
        values.push(column.nulls?.[index] === 1 ? null : column.values[index] === 1);
        break;
      case "datetime":
        values.push(column.nulls?.[index] === 1 ? null : new Date(column.values[index] ?? 0));
        break;
      case "string":
        values.push(
          column.nulls?.[index] === 1
            ? null
            : column.text.slice(column.offsets[index] ?? 0, column.offsets[index + 1] ?? 0),
        );
        break;
      case "mixed":
        values.push(column.values[index] ?? null);
        break;
      case "null":
        values.push(null);
    }
  }
  return values;
}
