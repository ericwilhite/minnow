import type { QueryRow, QueryValue } from "@minnowdb/core";

export interface RowSort {
  column: string;
  direction: "asc" | "desc";
}

/** Where a value's type sorts among the others, for a column whose values are mixed. */
function rank(value: QueryValue): number {
  if (typeof value === "boolean") return 0;
  if (typeof value === "number") return 1;
  if (value instanceof Date) return 2;
  return 3;
}

/**
 * Orders two values the way the engine's default ORDER BY does for one type: numbers by value,
 * dates by instant, strings by code point. NULLs sort last in both directions — they are what a
 * reader is least often looking for — so they are handled by the caller, not here.
 */
function compare(a: Exclude<QueryValue, null>, b: Exclude<QueryValue, null>): number {
  const byType = rank(a) - rank(b);
  if (byType !== 0) return byType;
  const left = a instanceof Date ? a.getTime() : a;
  const right = b instanceof Date ? b.getTime() : b;
  return left < right ? -1 : left > right ? 1 : 0;
}

/**
 * The rows of a result reordered by one column, in memory. A stable sort, so rows that tie keep
 * the order the query returned them in, and a copy: the original order is what "unsorted" goes
 * back to.
 */
export function sortRows(rows: readonly QueryRow[], sort: RowSort): QueryRow[] {
  const sign = sort.direction === "asc" ? 1 : -1;
  return rows
    .map((row, index) => ({ row, index, value: row[sort.column] ?? null }))
    .sort((a, b) => {
      if (a.value === null || b.value === null) {
        if (a.value === b.value) return a.index - b.index;
        return a.value === null ? 1 : -1;
      }
      return sign * compare(a.value, b.value) || a.index - b.index;
    })
    .map(({ row }) => row);
}

/** The next state of a header that cycles ascending, descending, then back to the query's order. */
export function nextSort(current: RowSort | undefined, column: string): RowSort | undefined {
  if (current?.column !== column) return { column, direction: "asc" };
  if (current.direction === "asc") return { column, direction: "desc" };
  return undefined;
}
