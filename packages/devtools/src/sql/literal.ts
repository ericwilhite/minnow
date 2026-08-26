import type { QueryValue } from "@minnowdb/core";
import { dateIsoString, dateMilliseconds } from "../date-value.js";

/** The four logical column types, mirrored from the catalog. */
export type ColumnType = "boolean" | "number" | "string" | "datetime";

/**
 * Explorer targets accept complete query text, so every filter value the explorer sends is text
 * inside the statement. This module is the only place that turns a value into that text; nothing
 * else in the package concatenates a value into SQL. Keeping it in one tested function stops an
 * apostrophe in a name from turning into a syntax error — or into a different query.
 */
export function sqlLiteral(value: QueryValue, type: ColumnType): string {
  if (value === null) return "NULL";
  switch (type) {
    case "string":
      return quoteString(String(value));
    case "number": {
      const numeric = typeof value === "number" ? value : Number(value);
      if (!Number.isFinite(numeric)) {
        throw new TypeError(`Not a finite number: ${String(value)}`);
      }
      return String(numeric);
    }
    case "boolean":
      return value === true || value === "true" || value === 1 ? "TRUE" : "FALSE";
    case "datetime":
      return `DATE ${quoteString(toDateOnly(value))}`;
  }
}

/** SQL escapes a quote by doubling it; there is no backslash escape to worry about. */
function quoteString(value: string): string {
  return `'${value.replaceAll("'", "''")}'`;
}

/**
 * The explorer deliberately renders datetime filters as `DATE '2026-08-12'`, so it drops the time
 * and compares at midnight UTC even though the SQL engine also accepts precise TIMESTAMP literals.
 * Callers must account for that day-granular explorer behavior rather than discover it.
 */
export function toDateOnly(value: QueryValue): string {
  const date = value instanceof Date ? value : new Date(String(value));
  if (!Number.isFinite(dateMilliseconds(date))) {
    throw new TypeError(`Not a date: ${String(value)}`);
  }
  return dateIsoString(date).slice(0, 10);
}

/** Whether a cursor can address this type exactly. Day-granular datetimes cannot. */
export function isExactlyComparable(type: ColumnType): boolean {
  return type !== "datetime";
}

const identifier = /^[A-Za-z_][A-Za-z0-9_]*$/;

function checkName(kind: string, name: string): string {
  if (!identifier.test(name)) throw new TypeError(`Unsupported ${kind} name: ${name}`);
  return name;
}

/**
 * A column reference, always qualified — in WHERE and in ORDER BY alike. The engine has no quoted
 * identifiers, so a column named after a keyword — `case`, `null` — only parses with its table in
 * front of it; qualifying every column means the explorer never has to know which names are
 * special. ORDER BY resolves a qualified reference against the source table of `SELECT *`, so the
 * same form serves both clauses.
 */
export function sqlColumn(table: string, column: string): string {
  return `${checkName("table", table)}.${checkName("column", column)}`;
}

/**
 * Whether a column can be sorted on at all. Qualifying covers every keyword name, so only a name
 * this module cannot render — one that is not a bare identifier — is refused. Sorting is dropped
 * rather than attempted for those, because rows in an order nobody asked for are worse than none.
 */
export function isSortable(column: string): boolean {
  return identifier.test(column);
}
