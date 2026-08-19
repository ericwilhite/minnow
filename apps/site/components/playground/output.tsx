"use client";
/**
 * What a snippet printed, drawn the way the reader meant it.
 *
 * A query returns an array of row objects, and printing that as JSON would hide the shape the
 * whole console is about. So an array of rows becomes a table with the select list as its
 * columns — the same thing the SQL console shows for the same query — and everything else is
 * formatted as text.
 */
import type { OutputEntry } from "./run";

/** Enough to see the shape of an answer; the console is not a data browser. */
const MAX_ROWS = 100;

type Rows = ReadonlyArray<Record<string, unknown>>;

/** True for the one thing worth drawing as a table: a non-empty array of plain objects. */
function asRows(value: unknown): Rows | undefined {
  if (!Array.isArray(value) || value.length === 0) return undefined;
  const rows = value as unknown[];
  const plain = rows.every(
    (row) =>
      typeof row === "object" && row !== null && !Array.isArray(row) && !(row instanceof Date),
  );
  return plain ? (rows as Rows) : undefined;
}

/**
 * The values `JSON.stringify` answers `undefined` for. Naming them here is what lets everything
 * below treat a stringify result as the string its type says it is.
 */
function unprintable(value: unknown): string | undefined {
  if (value === undefined) return "undefined";
  if (typeof value === "function")
    return `[function ${value.name === "" ? "anonymous" : value.name}]`;
  if (typeof value === "symbol") return value.toString();
  if (typeof value === "bigint") return `${value.toString()}n`;
  return undefined;
}

/** JSON, with dates as dates and a cycle reported rather than thrown. */
function json(value: unknown, indent?: number): string {
  try {
    return JSON.stringify(
      value,
      (_key, item: unknown) => (item instanceof Date ? cell(item) : item),
      indent,
    );
  } catch {
    return "[value that cannot be shown]";
  }
}

/** One cell. Dates print as dates rather than as the ISO string a JSON round-trip would give. */
function cell(value: unknown): string {
  if (value === null) return "null";
  if (value === undefined) return "";
  if (value instanceof Date) return value.toISOString().replace("T", " ").slice(0, 19);
  if (typeof value === "number") return Number.isInteger(value) ? String(value) : value.toFixed(2);
  if (typeof value === "string") return value;
  return unprintable(value) ?? json(value);
}

function text(value: unknown): string {
  if (typeof value === "string") return value;
  if (value instanceof Date) return cell(value);
  return unprintable(value) ?? json(value, 2);
}

function Table({ rows }: { rows: Rows }) {
  const columns = [...new Set(rows.flatMap((row) => Object.keys(row)))];
  const shown = rows.slice(0, MAX_ROWS);

  return (
    <div className="overflow-x-auto rounded-md border border-fd-border">
      <table className="w-full border-collapse text-xs">
        <thead>
          <tr className="bg-fd-muted/50">
            {columns.map((column) => (
              <th
                key={column}
                className="border-b border-fd-border px-2.5 py-1.5 text-left font-medium whitespace-nowrap"
              >
                {column}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {shown.map((row, index) => (
            <tr key={index} className="border-b border-fd-border/60 last:border-0">
              {columns.map((column) => (
                <td key={column} className="px-2.5 py-1 font-mono tabular-nums whitespace-nowrap">
                  {cell(row[column])}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
      <p className="px-2.5 py-1.5 text-xs text-fd-muted-foreground">
        {rows.length > MAX_ROWS
          ? `${String(shown.length)} of ${rows.length.toLocaleString("en-US")} rows`
          : `${String(rows.length)} ${rows.length === 1 ? "row" : "rows"}`}
      </p>
    </div>
  );
}

export function Output({
  entries,
  failure,
  elapsedMs,
  idle,
}: {
  entries: readonly OutputEntry[];
  failure?: string;
  elapsedMs?: number;
  idle: string;
}) {
  if (failure === undefined && entries.length === 0) {
    return <p className="p-3 text-xs text-fd-muted-foreground">{idle}</p>;
  }

  return (
    <div className="flex flex-col gap-2 p-3">
      {entries.map((entry, index) => {
        const rows = entry.values.length === 1 ? asRows(entry.values[0]) : undefined;
        if (rows !== undefined) return <Table key={index} rows={rows} />;
        return (
          <pre
            key={index}
            className={`overflow-x-auto font-mono text-xs whitespace-pre-wrap ${
              entry.level === "error"
                ? "text-red-500"
                : entry.level === "warn"
                  ? "text-amber-600 dark:text-amber-400"
                  : ""
            }`}
          >
            {entry.values.map(text).join(" ")}
          </pre>
        );
      })}

      {failure === undefined ? null : (
        <pre className="overflow-x-auto font-mono text-xs whitespace-pre-wrap text-red-500">
          {failure}
        </pre>
      )}

      {elapsedMs === undefined ? null : (
        <p className="text-xs text-fd-muted-foreground">
          {failure === undefined ? "Ran" : "Failed"} in {elapsedMs.toFixed(1)} ms
        </p>
      )}
    </div>
  );
}
