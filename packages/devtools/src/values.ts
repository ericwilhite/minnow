import type { QueryValue } from "@minnowdb/core";
import { dateIsoString, dateMilliseconds } from "./date-value.js";
import type { ColumnType } from "./sql/literal.js";

type ParseResult = { ok: true; value: QueryValue } | { ok: false; message: string };

/** The sentinel a person types to mean SQL NULL, since an empty box is ambiguous for strings. */
const nullInput = "NULL";

/**
 * Turns typed text into a value of the column's type, or explains why it cannot. Writes go through
 * the batch API rather than SQL text, so the value here is the value stored. An enum column takes
 * only its declared values; the check is here so the form refuses `pending` before the engine
 * does, with the accepted values in the message.
 */
export function parseInput(
  text: string,
  type: ColumnType,
  nullable: boolean,
  enumValues?: readonly string[],
): ParseResult {
  const trimmed = text.trim();
  if (trimmed.length === 0 || trimmed.toUpperCase() === nullInput) {
    return nullable
      ? { ok: true, value: null }
      : { ok: false, message: "This column cannot be null." };
  }
  switch (type) {
    case "number": {
      const value = Number(trimmed);
      return Number.isFinite(value)
        ? { ok: true, value }
        : { ok: false, message: `Not a number: ${trimmed}` };
    }
    case "boolean": {
      const lowered = trimmed.toLowerCase();
      if (["true", "1", "yes"].includes(lowered)) return { ok: true, value: true };
      if (["false", "0", "no"].includes(lowered)) return { ok: true, value: false };
      return { ok: false, message: `Not a boolean: ${trimmed}` };
    }
    case "datetime": {
      const value = new Date(trimmed);
      return Number.isFinite(dateMilliseconds(value))
        ? { ok: true, value }
        : { ok: false, message: `Not a date: ${trimmed}` };
    }
    case "string":
      if (enumValues !== undefined && !enumValues.includes(trimmed)) {
        return { ok: false, message: `Not one of ${enumValues.join(", ")}: ${trimmed}` };
      }
      return { ok: true, value: trimmed };
  }
}

/** The text an editor starts with for an existing value; the inverse of `parseInput`. */
export function formatForInput(value: QueryValue): string {
  if (value === null) return "";
  if (value instanceof Date) return dateIsoString(value);
  return String(value);
}

/** How a value reads inside a confirmation, where NULL has to be distinguishable from "NULL". */
export function describeValue(value: QueryValue): string {
  if (value === null) return "NULL";
  if (value instanceof Date) return dateIsoString(value);
  if (typeof value === "string") return `'${value}'`;
  return String(value);
}

/** Placeholder text that shows the accepted shape rather than repeating the column name. */
export function inputHint(type: ColumnType, nullable: boolean): string {
  const base =
    type === "datetime" ? "2026-08-12T09:14:00Z" : type === "boolean" ? "true / false" : type;
  return nullable ? `${base} or NULL` : base;
}
