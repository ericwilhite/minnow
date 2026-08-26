import type { QueryRow, QueryValue } from "@minnowdb/core";
import { findColumn, type TableInfo } from "../explorer/catalog.js";
import { renderFilters, type Filter } from "../explorer/filters.js";
import { isExactlyComparable, isSortable, sqlColumn, sqlLiteral } from "./literal.js";

export interface Sort {
  column: string;
  direction: "asc" | "desc";
}

/** The values identifying the last row of a page: the sort value, then the key. */
export type Cursor = readonly QueryValue[];

export interface PageRequest {
  table: TableInfo;
  filters: readonly Filter[];
  sort?: Sort;
  limit: number;
  /** Where the previous page ended. Ignored when the table cannot be cursored. */
  cursor?: Cursor;
  /** Rows already read, for tables that fall back to counting from the start. */
  offset?: number;
}

/**
 * How the next page is found.
 *
 * `keyset` asks for the rows after the last one seen, so page one thousand costs the same as page
 * one. `offset` makes the engine count past everything skipped, which grows linearly — it is the
 * fallback, never the preference.
 */
export type PagingMode = "keyset" | "offset";

/**
 * A cursor can only address a row exactly when three things hold: the table has a unique key to
 * break ties, the sort column compares exactly (datetimes do not because the explorer's literal
 * encoder is intentionally day-granular), and the sort column has no NULLs, since every comparison
 * against NULL is unknown and would silently drop those rows from every page after the first.
 */
export function pagingMode(table: TableInfo, sort?: Sort): PagingMode {
  const key = table.uniqueKey;
  if (key === undefined || !isSortable(key)) return "offset";
  if (sort === undefined) return "keyset";
  const column = findColumn(table, sort.column);
  if (column === undefined) return "offset";
  if (column.name === key) return "keyset";
  return column.nullable || !isExactlyComparable(column.type) ? "offset" : "keyset";
}

/**
 * The ordering actually sent: the chosen column, then the key so the order is total. A name this
 * package cannot render as SQL is dropped rather than sent — see `isSortable`.
 */
function orderColumns(table: TableInfo, sort?: Sort): Sort[] {
  const key =
    table.uniqueKey !== undefined && isSortable(table.uniqueKey) ? table.uniqueKey : undefined;
  const chosen = sort !== undefined && isSortable(sort.column) ? sort : undefined;
  if (chosen === undefined) {
    return key === undefined ? [] : [{ column: key, direction: "asc" }];
  }
  if (key === undefined || chosen.column === key) return [chosen];
  return [chosen, { column: key, direction: chosen.direction }];
}

/**
 * `(a > A) OR (a = A AND b > B)` — the row-value comparison written out, because the engine has no
 * tuple comparison. With one ordering column it collapses to the plain inequality.
 */
function keysetPredicate(table: TableInfo, order: readonly Sort[], cursor: Cursor): string {
  const parts: string[] = [];
  for (let depth = 0; depth < order.length; depth += 1) {
    const terms: string[] = [];
    for (let index = 0; index < depth; index += 1) {
      terms.push(comparison(table, order[index], cursor[index], "="));
    }
    const step = order[depth];
    terms.push(comparison(table, step, cursor[depth], step?.direction === "desc" ? "<" : ">"));
    parts.push(terms.length === 1 ? (terms[0] ?? "") : `(${terms.join(" AND ")})`);
  }
  return parts.length === 1 ? (parts[0] ?? "") : `(${parts.join(" OR ")})`;
}

function comparison(
  table: TableInfo,
  sort: Sort | undefined,
  value: QueryValue | undefined,
  operator: string,
): string {
  if (sort === undefined) throw new TypeError("Missing ordering column for the cursor");
  const column = findColumn(table, sort.column);
  if (column === undefined) throw new TypeError(`Unknown column: ${sort.column}`);
  return `${sqlColumn(table.name, column.name)} ${operator} ${sqlLiteral(value ?? null, column.type)}`;
}

function whereClause(parts: ReadonlyArray<string | undefined>): string {
  const kept = parts.filter((part): part is string => part !== undefined && part.length > 0);
  return kept.length === 0 ? "" : ` WHERE ${kept.join(" AND ")}`;
}

/** Qualified, like every other column reference: it is what lets a keyword name be sorted on. */
function orderClause(table: string, order: readonly Sort[]): string {
  if (order.length === 0) return "";
  const rendered = order.map(
    (sort) => `${sqlColumn(table, sort.column)}${sort.direction === "desc" ? " DESC" : ""}`,
  );
  return ` ORDER BY ${rendered.join(", ")}`;
}

/** The statement for one page of rows. Every value in it went through the literal encoder. */
export function buildPageQuery(request: PageRequest): string {
  const { table, filters, sort, limit } = request;
  const mode = pagingMode(table, sort);
  const order = orderColumns(table, sort);
  const filterSql = renderFilters(table.name, filters);
  const cursorSql =
    mode === "keyset" && request.cursor !== undefined && order.length > 0
      ? keysetPredicate(table, order, request.cursor)
      : undefined;

  const offset = mode === "offset" ? (request.offset ?? 0) : 0;
  const tail =
    offset > 0 ? ` LIMIT ${String(limit)} OFFSET ${String(offset)}` : ` LIMIT ${String(limit)}`;
  return `SELECT * FROM ${table.name}${whereClause([filterSql, cursorSql])}${orderClause(table.name, order)}${tail}`;
}

/** Counting scans the whole table, so callers ask for it separately and never wait on it. */
export function buildCountQuery(table: TableInfo, filters: readonly Filter[]): string {
  return `SELECT COUNT(*) AS row_count FROM ${table.name}${whereClause([renderFilters(table.name, filters)])}`;
}

/** The cursor for the page after this row, or undefined when the table cannot be cursored. */
export function cursorFrom(row: QueryRow, table: TableInfo, sort?: Sort): Cursor | undefined {
  if (pagingMode(table, sort) !== "keyset") return undefined;
  const order = orderColumns(table, sort);
  if (order.length === 0) return undefined;
  return order.map((entry) => row[entry.column] ?? null);
}
