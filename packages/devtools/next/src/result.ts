/** Positional, caller-owned result presentation. Exact values remain in the Result. */
import type { Result, Value } from "../../../core/dist/next/owner/api.js";
import type { QueryRow, QueryValue } from "@minnowdb/core";
import { createGrid, type GridColumn } from "../../dist/results/grid.js";

export const NATIVE_PREVIEW_ROWS = 1000;
/** Display text only: never a write value or a lossy JSON round-trip. */
export function valueText(value: Value): string {
  if (value === null) return "NULL";
  if (value instanceof Uint8Array) {
    let text = "\\x";
    for (const byte of value) text += byte.toString(16).padStart(2, "0");
    return text;
  }
  if (typeof value === "object") return `[${value.map(arrayValue).join(",")}]`;
  return String(value);
}
function arrayValue(value: Value): string {
  if (value === null) return "null";
  if (typeof value === "string" || value instanceof Uint8Array)
    return JSON.stringify(valueText(value));
  return valueText(value);
}
const field = (text: string, delimiter: string): string =>
  /[\r\n"]/.test(text) || text.includes(delimiter) ? `"${text.replaceAll('"', '""')}"` : text;
export function selectionText(columns: readonly GridColumn[], rows: readonly QueryRow[]): string {
  const lines = [
    columns.map((column) => field(column.displayName ?? column.name, "\t")).join("\t"),
  ];
  for (const row of rows)
    lines.push(
      columns
        .map((column) =>
          field(row[column.name] === null ? "" : String(row[column.name] ?? ""), "\t"),
        )
        .join("\t"),
    );
  return `${lines.join("\n")}\n`;
}
export function resultPresentation(result: Result, limit = NATIVE_PREVIEW_ROWS) {
  if (!Number.isInteger(limit) || limit < 0 || limit > NATIVE_PREVIEW_ROWS)
    throw new RangeError("Invalid native preview limit");
  const count = Math.min(result.rowCount, limit);
  const columns: GridColumn[] = result.columns.map((column, ordinal) => ({
    name: `@${String(ordinal)}`,
    displayName: column.name,
    label: column.sqlType,
    type: ["int32", "int64", "float32", "float64", "decimal"].includes(column.type.kind)
      ? "number"
      : "string",
  }));
  const value = (row: number, ordinal: number): Value => {
    if (
      !Number.isInteger(row) ||
      row < 0 ||
      row >= count ||
      !Number.isInteger(ordinal) ||
      ordinal < 0 ||
      ordinal >= columns.length
    )
      throw new RangeError("Invalid native preview position");
    return result.values(ordinal)[row] ?? null;
  };
  // Bounded row wrappers with lazy display getters. No Result.rows()/tuples() cache,
  // canonical payload copy, per-cell text cache or duplicate-label object lookup.
  const rows: QueryRow[] = Array.from({ length: count }, (_, row) => {
    const record: QueryRow = {};
    columns.forEach((column, ordinal) =>
      Object.defineProperty(record, column.name, {
        enumerable: true,
        get(): QueryValue {
          const cell = value(row, ordinal);
          return cell === null || typeof cell === "number" || typeof cell === "boolean"
            ? cell
            : valueText(cell);
        },
      }),
    );
    return record;
  });
  return {
    columns,
    rows,
    value,
    command: result.command,
    affectedRows: result.affectedRows,
    replayed: result.replayed,
    rowCount: result.rowCount,
    visibleRows: count,
    truncated: count < result.rowCount,
  };
}
export function createNativeResultGrid() {
  const grid = createGrid({ selectionText });
  return {
    node: grid.node,
    destroy: () => grid.destroy(),
    layout: () => grid.layout(),
    show(result: Result) {
      const presentation = resultPresentation(result);
      grid.setColumns(presentation.columns);
      grid.setRows(presentation.rows);
      return presentation;
    },
    clear() {
      grid.setRows([]);
      grid.setColumns([]);
    },
  };
}
