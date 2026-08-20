import type { QueryResult, QueryRow } from "./query.js";
import { defineSqlResultProperty } from "./sql-semantics.js";

/** Modest per-entry cap so one giant result cannot thrash the shared artifact cache. */
export const RESULT_MEMO_MAX_BYTES = 4 * 1024 * 1024;

/** Stable, collision-free memo-key encoding for bound SQL parameters. */
export function queryResultMemoKey(sql: string, params: readonly unknown[]): string {
  return JSON.stringify([sql, params.map(encodeParameter)]);
}

/**
 * Defensive copy because callers own query results and may mutate both rows and Dates. Object
 * spread copies own enumerable properties with define semantics, so a column named `__proto__`
 * stays an own property; only a Date cell needs the explicit define, and only because its
 * value is replaced. This used to define every cell, which made a memo hit on a result of a
 * few thousand rows cost more than re-executing the query.
 */
export function copyQueryResult(result: QueryResult): QueryResult {
  const columns = result.columns;
  return {
    columns: [...columns],
    rows: result.rows.map((row) => {
      const copy: QueryRow = { ...row };
      for (const name of columns) {
        const value = copy[name];
        if (value instanceof Date) defineSqlResultProperty(copy, name, new Date(value.getTime()));
      }
      return copy;
    }),
  };
}

/** Modeled retained payload for one cached query result. */
export function queryResultRetainedBytes(result: QueryResult): number {
  let bytes = 64;
  for (const column of result.columns) bytes += 16 + column.length * 2;
  for (const row of result.rows) bytes += estimateValuesBytes(Object.values(row));
  return bytes;
}

function estimateValuesBytes(values: readonly unknown[]): number {
  let bytes = 0;
  for (const value of values) {
    if (typeof value === "string") bytes += 4 + value.length;
    else if (typeof value === "number" || value instanceof Date) bytes += 8;
    else bytes += 1;
  }
  return bytes;
}

function encodeParameter(value: unknown): readonly unknown[] {
  if (value === null) return [0];
  if (typeof value === "boolean") return [1, value];
  if (typeof value === "number") {
    if (Object.is(value, -0)) return [2, "-0"];
    return [2, String(value)];
  }
  if (typeof value === "string") return [3, value];
  if (value instanceof Date) return [4, value.getTime()];
  throw new TypeError("Query parameter has an unsupported value");
}
