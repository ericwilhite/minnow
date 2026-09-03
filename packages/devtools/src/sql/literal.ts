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
      return `TIMESTAMP ${quoteString(toTimestampText(value))}`;
  }
}

/** SQL escapes a quote by doubling it; there is no backslash escape to worry about. */
function quoteString(value: string): string {
  return `'${value.replaceAll("'", "''")}'`;
}

/**
 * A datetime is written to the millisecond, as an ISO instant in UTC — the engine's `TIMESTAMP`
 * literal reads exactly that text, and every datetime in a Minnow database is UTC already. Full
 * precision is what lets a keyset cursor address a datetime row exactly, and what makes a filter
 * on `10:30` mean half past ten rather than midnight.
 */
function toTimestampText(value: QueryValue): string {
  const date = value instanceof Date ? value : new Date(String(value));
  if (!Number.isFinite(dateMilliseconds(date))) {
    throw new TypeError(`Not a date: ${String(value)}`);
  }
  return dateIsoString(date);
}

const bareIdentifier = /^[A-Za-z_][A-Za-z0-9_]*$/;

/**
 * A name as the engine will read it back. A bare identifier goes through as-is; anything else —
 * a space, a hyphen, a quote — is written as a quoted identifier, which the engine reads as a plain
 * reference whatever it contains. The table's own name goes through here too, since the catalog
 * accepts any non-empty name for either.
 */
export function sqlIdentifier(name: string): string {
  if (name.length === 0) throw new TypeError("Empty identifier");
  return bareIdentifier.test(name) ? name : `"${name.replaceAll('"', '""')}"`;
}

/**
 * A column reference, always qualified — in WHERE and in ORDER BY alike. A column named after a
 * keyword — `case`, `null` — only parses with its table in front of it; qualifying every column
 * means the explorer never has to know which names are special. ORDER BY resolves a qualified
 * reference against the source table of `SELECT *`, so the same form serves both clauses.
 */
export function sqlColumn(table: string, column: string): string {
  return `${sqlIdentifier(table)}.${sqlIdentifier(column)}`;
}
