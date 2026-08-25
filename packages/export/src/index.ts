import type { QueryCursorOptions, QueryResult, QueryRow, QueryValue } from "@minnowdb/core";
import { dateIsoString, dateMilliseconds } from "./date-value.js";

/** Structural source implemented by both MinnowDatabase and MinnowDatabaseClient. */
export interface QueryCursorSource {
  queryCursor(
    sql: string,
    options?: QueryCursorOptions,
  ): AsyncIterableIterator<QueryResult, undefined>;
}

export interface CsvStreamOptions extends QueryCursorOptions {
  /** Include a column-name row. Defaults to true. */
  readonly header?: boolean;
  /** One-character field separator. Defaults to a comma. */
  readonly delimiter?: string;
  /** Record separator. Defaults to CRLF for RFC-compatible CSV. */
  readonly newline?: "\n" | "\r\n";
  /** Text emitted for SQL NULL. Defaults to the empty field. */
  readonly nullValue?: string;
}

export type NdjsonStreamOptions = QueryCursorOptions;

const encoder = new TextEncoder();

function isSingleCodePoint(value: string): boolean {
  const codePoints = value[Symbol.iterator]();
  return codePoints.next().done !== true && codePoints.next().done === true;
}

function sameColumns(left: readonly string[], right: readonly string[]): boolean {
  return left.length === right.length && left.every((name, index) => name === right[index]);
}

function csvScalar(value: QueryValue, nullValue: string): string {
  if (value === null) return nullValue;
  if (value instanceof Date) {
    if (!Number.isFinite(dateMilliseconds(value))) {
      throw new RangeError("Cannot export an invalid Date");
    }
    return dateIsoString(value);
  }
  if (typeof value === "number" && Object.is(value, -0)) return "-0";
  return String(value);
}

function csvField(value: string, delimiter: string): string {
  return value.includes(delimiter) ||
    value.includes('"') ||
    value.includes("\r") ||
    value.includes("\n")
    ? `"${value.replaceAll('"', '""')}"`
    : value;
}

function jsonScalar(value: QueryValue): string {
  if (value === null) return "null";
  if (typeof value === "boolean") return value ? "true" : "false";
  if (typeof value === "number") {
    if (!Number.isFinite(value)) {
      throw new RangeError("NDJSON cannot represent NaN or an infinite number");
    }
    return Object.is(value, -0) ? "-0" : String(value);
  }
  if (value instanceof Date) {
    if (!Number.isFinite(dateMilliseconds(value))) {
      throw new RangeError("Cannot export an invalid Date");
    }
    return JSON.stringify(dateIsoString(value));
  }
  return JSON.stringify(value);
}

function jsonRow(columns: readonly string[], row: QueryRow): string {
  return `{${columns
    .map((name) => `${JSON.stringify(name)}:${jsonScalar(row[name] ?? null)}`)
    .join(",")}}`;
}

function textStream(
  iterator: AsyncIterableIterator<QueryResult, undefined>,
  encode: (batch: QueryResult) => string,
): ReadableStream<Uint8Array> {
  let finished = false;
  return new ReadableStream<Uint8Array>({
    async pull(controller) {
      if (finished) return;
      try {
        for (;;) {
          const next = await iterator.next();
          if (next.done) {
            finished = true;
            controller.close();
            return;
          }
          const encoded = encode(next.value);
          if (encoded.length === 0) continue;
          controller.enqueue(encoder.encode(encoded));
          return;
        }
      } catch (error) {
        finished = true;
        try {
          await iterator.return?.();
        } catch {
          // Preserve the serialization/read error that made the stream fail.
        }
        controller.error(error);
      }
    },
    async cancel() {
      finished = true;
      await iterator.return?.();
    },
  });
}

/** Stream one SQL query as CSV without accumulating the complete result or output text. */
export function streamCsv(
  source: QueryCursorSource,
  sql: string,
  options: CsvStreamOptions = {},
): ReadableStream<Uint8Array> {
  const delimiterInput: unknown = options.delimiter;
  const delimiter = delimiterInput === undefined ? "," : delimiterInput;
  if (
    typeof delimiter !== "string" ||
    !isSingleCodePoint(delimiter) ||
    delimiter === '"' ||
    delimiter === "\r" ||
    delimiter === "\n"
  ) {
    throw new RangeError("CSV delimiter must be one character other than a quote or newline");
  }
  const newlineOption: unknown = options.newline;
  const newlineInput = newlineOption === undefined ? "\r\n" : newlineOption;
  if (newlineInput !== "\n" && newlineInput !== "\r\n") {
    throw new RangeError("CSV newline must be LF or CRLF");
  }
  const newline = newlineInput;
  const nullValueInput: unknown = options.nullValue;
  const nullValue = nullValueInput === undefined ? "" : nullValueInput;
  if (typeof nullValue !== "string") throw new TypeError("CSV nullValue must be a string");
  if (options.header !== undefined && typeof options.header !== "boolean") {
    throw new TypeError("CSV header must be a boolean");
  }
  const includeHeader = options.header !== false;
  const {
    header: _header,
    delimiter: _delimiter,
    newline: _newline,
    nullValue: _nullValue,
    ...cursorOptions
  } = options;
  void _header;
  void _delimiter;
  void _newline;
  void _nullValue;
  const iterator = source.queryCursor(sql, cursorOptions);
  let columns: readonly string[] | undefined;
  let headerPending = includeHeader;
  return textStream(iterator, (batch) => {
    if (columns === undefined) columns = batch.columns;
    else if (!sameColumns(columns, batch.columns)) throw new Error("Query cursor columns changed");
    const records: string[] = [];
    if (headerPending) {
      headerPending = false;
      records.push(columns.map((name) => csvField(name, delimiter)).join(delimiter));
    }
    for (const row of batch.rows) {
      records.push(
        columns
          .map((name) => csvField(csvScalar(row[name] ?? null, nullValue), delimiter))
          .join(delimiter),
      );
    }
    return records.length === 0 ? "" : `${records.join(newline)}${newline}`;
  });
}

/** Stream one SQL query as newline-delimited JSON with exact finite SQL scalar values. */
export function streamNdjson(
  source: QueryCursorSource,
  sql: string,
  options: NdjsonStreamOptions = {},
): ReadableStream<Uint8Array> {
  const iterator = source.queryCursor(sql, options);
  let columns: readonly string[] | undefined;
  return textStream(iterator, (batch) => {
    if (columns === undefined) columns = batch.columns;
    else if (!sameColumns(columns, batch.columns)) throw new Error("Query cursor columns changed");
    return batch.rows.length === 0
      ? ""
      : `${batch.rows.map((row) => jsonRow(columns ?? [], row)).join("\n")}\n`;
  });
}
