import { copyDate, dateMilliseconds } from "../date-value.js";
import { estimateValuesBytes } from "./byte-estimates.js";
import { copyQueryResultExternalization, type QueryResult, type QueryRow } from "./query.js";
import { defineSqlResultProperty } from "./sql-semantics.js";

/** Modest per-entry cap so one giant result cannot thrash the shared artifact cache. */
export const RESULT_MEMO_MAX_BYTES = 4 * 1024 * 1024;

/** Stable, collision-free memo-key encoding for bound SQL parameters. */
export function queryResultMemoKey(sql: string, params: readonly unknown[]): string {
  return JSON.stringify([sql, params.map(encodeParameter)]);
}

/**
 * A memo key for a compiled plan: its JSON with the values JSON cannot tell apart made
 * distinct — a Date from its ISO string, -0 from 0, NaN and the infinities from null — so two
 * plans with the same key are the same query over the same literals.
 */
export function planMemoKey(plan: unknown): string {
  return JSON.stringify(plan, (_key, value: unknown) => {
    if (value instanceof Date) return { $date: dateMilliseconds(value) };
    if (typeof value === "bigint") return { $bigint: value.toString() };
    if (typeof value === "number") {
      if (Object.is(value, -0)) return { $number: "-0" };
      if (!Number.isFinite(value)) return { $number: String(value) };
    }
    return value;
  });
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
  const copy = {
    columns: [...columns],
    columnDomains: structuredClone(result.columnDomains),
    rows: result.rows.map((row) => {
      const copy: QueryRow = { ...row };
      for (const name of columns) {
        const value = copy[name];
        if (value instanceof Date) defineSqlResultProperty(copy, name, copyDate(value));
      }
      return copy;
    }),
  };
  return copyQueryResultExternalization(result, copy);
}

/** Modeled retained payload for one cached query result. */
export function queryResultRetainedBytes(result: QueryResult): number {
  let bytes = 64;
  for (const column of result.columns) bytes += 16 + column.length * 2;
  for (const row of result.rows) bytes += estimateValuesBytes(Object.values(row));
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
  if (value instanceof Date) return [4, dateMilliseconds(value)];
  throw new TypeError("Query parameter has an unsupported value");
}
