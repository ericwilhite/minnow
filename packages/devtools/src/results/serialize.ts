import type { QueryRow, QueryValue } from "@minnowdb/core";
import { dateIsoString } from "../date-value.js";
import { sqlIdentifier, sqlLiteral } from "../sql/literal.js";

/** A value as text for a file: dates as ISO instants, NULL as the empty field. */
function cellText(value: QueryValue): string {
  if (value === null) return "";
  if (value instanceof Date) return dateIsoString(value);
  return String(value);
}

function csvField(text: string, delimiter: string): string {
  return /[\r\n"]/.test(text) || text.includes(delimiter)
    ? `"${text.replaceAll('"', '""')}"`
    : text;
}

/** RFC 4180 CSV with a header row; a tab delimiter makes it paste into a spreadsheet as cells. */
export function toCsv(
  columns: readonly string[],
  rows: readonly QueryRow[],
  delimiter: "," | "\t" = ",",
): string {
  const lines = [columns.map((column) => csvField(column, delimiter)).join(delimiter)];
  for (const row of rows) {
    lines.push(
      columns.map((column) => csvField(cellText(row[column] ?? null), delimiter)).join(delimiter),
    );
  }
  return `${lines.join("\n")}\n`;
}

/** A JSON array of objects, dates as ISO strings, in the result's column order. */
export function toJson(columns: readonly string[], rows: readonly QueryRow[]): string {
  const objects = rows.map((row) =>
    Object.fromEntries(
      columns.map((column) => {
        const value = row[column] ?? null;
        return [column, value instanceof Date ? dateIsoString(value) : value];
      }),
    ),
  );
  return JSON.stringify(objects, null, 2);
}

/** One row, for a detail view or a clipboard. */
export function rowToJson(columns: readonly string[], row: QueryRow): string {
  return toJson(columns, [row])
    .replace(/^\[\n?|\n?\]$/g, "")
    .replace(/^ {2}/gm, "");
}

/**
 * The rows as INSERT statements the console can run against another database. Every value goes
 * through the literal encoder, typed by the value itself: the row is the only type information a
 * bare query result carries.
 */
export function toInsertSql(
  table: string,
  columns: readonly string[],
  rows: readonly QueryRow[],
): string {
  const target = sqlIdentifier(table);
  const names = columns.map(sqlIdentifier).join(", ");
  return rows
    .map((row) => {
      const values = columns
        .map((column) => {
          const value = row[column] ?? null;
          const type =
            typeof value === "number"
              ? "number"
              : typeof value === "boolean"
                ? "boolean"
                : value instanceof Date
                  ? "datetime"
                  : "string";
          return sqlLiteral(value, type);
        })
        .join(", ");
      return `INSERT INTO ${target} (${names}) VALUES (${values});`;
    })
    .join("\n");
}
