/** Internal canonical cursor exports; no published entry/default change. */
import type {
  Cursor,
  CursorOptions,
  Param,
  Result,
  ResultColumn,
  SqlExecutor,
  Value,
} from "../../../core/dist/next/owner/api.js";
export type QueryCursorSource = Pick<SqlExecutor, "cursor">;
export interface ExportOptions extends CursorOptions {
  readonly params?: readonly Param[];
}
export interface CsvStreamOptions extends ExportOptions {
  readonly header?: boolean;
  readonly delimiter?: string;
  readonly newline?: "\n" | "\r\n";
  readonly nullValue?: string;
}
const encoder = new TextEncoder();
function bytes(value: Uint8Array): string {
  const hex = "0123456789abcdef";
  let text = "\\x";
  for (const n of value) text += (hex[n >> 4] ?? "") + (hex[n & 15] ?? "");
  return text;
}
/** JSON number tokens keep bigint's exact decimal text; no intermediate Number conversion. */
function json(value: Value, active: Set<readonly Value[]>): string {
  if (value === null) return "null";
  if (typeof value === "boolean") return value ? "true" : "false";
  if (typeof value === "bigint") return value.toString();
  if (typeof value === "number") {
    if (!Number.isFinite(value))
      throw new RangeError("NDJSON cannot represent NaN or an infinite number");
    return Object.is(value, -0) ? "-0" : String(value);
  }
  if (typeof value === "string") return JSON.stringify(value);
  if (value instanceof Uint8Array) return JSON.stringify(bytes(value));
  if (active.has(value)) throw new TypeError("Cannot export a cyclic SQL array");
  active.add(value);
  try {
    return `[${value.map((item) => json(item, active)).join(",")}]`;
  } finally {
    active.delete(value);
  }
}
function scalar(value: Value, nullValue: string): string {
  if (value === null) return nullValue;
  if (typeof value === "string") return value;
  if (typeof value === "number" && Object.is(value, -0)) return "-0";
  if (value instanceof Uint8Array) return bytes(value);
  if (Array.isArray(value)) return json(value, new Set());
  return String(value);
}
function field(value: string, delimiter: string): string {
  return value.includes(delimiter) ||
    value.includes('"') ||
    value.includes("\n") ||
    value.includes("\r")
    ? `"${value.replaceAll('"', '""')}"`
    : value;
}
function matches(left: readonly ResultColumn[], right: readonly ResultColumn[]): boolean {
  return (
    left.length === right.length &&
    left.every((c, i) => {
      const other = right[i];
      return other?.name === c.name && c.sqlType === other.sqlType;
    })
  );
}
function cursor(source: QueryCursorSource, sql: string, options: ExportOptions): Cursor {
  return source.cursor(sql, options.params, {
    ...(options.signal === undefined ? {} : { signal: options.signal }),
    ...(options.frameRows === undefined ? {} : { frameRows: options.frameRows }),
    ...(options.prefetchBytes === undefined ? {} : { prefetchBytes: options.prefetchBytes }),
  });
}
function stream(
  iterator: Cursor,
  header: ((columns: readonly ResultColumn[]) => string) | null,
  encode: (result: Result) => string,
): ReadableStream<Uint8Array> {
  let ended = false,
    started = false,
    columns: readonly ResultColumn[] = [];
  const isEnded = (): boolean => ended;
  return new ReadableStream<Uint8Array>({
    async pull(controller) {
      if (isEnded()) return;
      try {
        if (!started) {
          started = true;
          columns = await iterator.columns;
          if (isEnded()) return;
          if (header !== null) {
            controller.enqueue(encoder.encode(header(columns)));
            return;
          }
        }
        for (;;) {
          const next = await iterator.next();
          if (isEnded()) return;
          if (next.done) {
            ended = true;
            controller.close();
            return;
          }
          if (!matches(columns, next.value.columns))
            throw new Error("Query cursor columns changed");
          const text = encode(next.value);
          if (text.length === 0) continue;
          controller.enqueue(encoder.encode(text));
          return;
        }
      } catch (error) {
        if (isEnded()) return;
        ended = true;
        try {
          await iterator.close();
        } catch {
          /* preserve the original read/serialization error */
        }
        controller.error(error);
      }
    },
    async cancel() {
      if (isEnded()) return;
      ended = true;
      await iterator.close();
    },
  });
}
export function streamCsv(
  source: QueryCursorSource,
  sql: string,
  options: CsvStreamOptions = {},
): ReadableStream<Uint8Array> {
  const delimiterInput: unknown = options.delimiter,
    newlineInput: unknown = options.newline,
    nullInput: unknown = options.nullValue;
  const delimiter = delimiterInput === undefined ? "," : delimiterInput,
    newline = newlineInput === undefined ? "\r\n" : newlineInput,
    nullValue = nullInput === undefined ? "" : nullInput;
  if (typeof delimiter !== "string")
    throw new RangeError("CSV delimiter must be one character other than a quote or newline");
  const points = delimiter[Symbol.iterator]();
  if (
    points.next().done === true ||
    points.next().done !== true ||
    delimiter === '"' ||
    delimiter === "\r" ||
    delimiter === "\n"
  )
    throw new RangeError("CSV delimiter must be one character other than a quote or newline");
  if (newline !== "\n" && newline !== "\r\n")
    throw new RangeError("CSV newline must be LF or CRLF");
  if (typeof nullValue !== "string") throw new TypeError("CSV nullValue must be a string");
  if (options.header !== undefined && typeof options.header !== "boolean")
    throw new TypeError("CSV header must be a boolean");
  const iterator = cursor(source, sql, options);
  return stream(
    iterator,
    options.header === false
      ? null
      : (columns) => columns.map((c) => field(c.name, delimiter)).join(delimiter) + newline,
    (result) => {
      const lines: string[] = [];
      for (const row of result.tuples())
        lines.push(row.map((value) => field(scalar(value, nullValue), delimiter)).join(delimiter));
      return lines.length === 0 ? "" : lines.join(newline) + newline;
    },
  );
}
export function streamNdjson(
  source: QueryCursorSource,
  sql: string,
  options: ExportOptions = {},
): ReadableStream<Uint8Array> {
  return stream(cursor(source, sql, options), null, (result) => {
    const lines: string[] = [];
    for (const row of result.tuples())
      lines.push(
        `{${result.columns.map((column, i) => `${JSON.stringify(column.name)}:${json(row[i] ?? null, new Set())}`).join(",")}}`,
      );
    return lines.length === 0 ? "" : lines.join("\n") + "\n";
  });
}
