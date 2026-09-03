import type { QueryResult, QueryValue } from "@minnowdb/core";
import { domainLabel } from "../explorer/catalog.js";
import type { GridColumn } from "./grid.js";

/** How many rows are looked at to decide a column's type. */
const sample = 200;

function typeOf(value: QueryValue): GridColumn["type"] {
  if (typeof value === "number") return "number";
  if (typeof value === "boolean") return "boolean";
  if (value instanceof Date) return "datetime";
  return "string";
}

/**
 * Grid columns for a bare query result. A result carries no column types of its own — only the
 * SQL domain where one was declared — so each column's type is read off its first non-null
 * values: enough to right-align the numbers and size the dates. The domain, where there is one,
 * is the header's label.
 */
export function columnsOf(result: QueryResult): GridColumn[] {
  const rows = result.rows.slice(0, sample);
  return result.columns.map((name, index) => {
    let type: GridColumn["type"];
    for (const row of rows) {
      const value = row[name];
      if (value === null || value === undefined) continue;
      const seen = typeOf(value);
      if (type === undefined) type = seen;
      else if (type !== seen) {
        type = "string";
        break;
      }
    }
    const domain = result.columnDomains[index] ?? null;
    return {
      name,
      ...(type === undefined ? {} : { type }),
      ...(domain === null ? {} : { label: domainLabel(domain) }),
    };
  });
}
