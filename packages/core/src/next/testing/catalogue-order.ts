/** Catalogue-only oracle: SQL ORDER BY constrains keys, not the order within equal-key peers.
 * PostgreSQL authority: https://www.postgresql.org/docs/18/queries-order.html
 */
import { compileQuery } from "../../engine/query.js";
import { encodeValue } from "../../testing/differential-harness.js";

/** All frozen catalogue ORDER BY terms name selected columns. Fail closed if that changes.
 * Parse the original outer block without optimization; an ORDER BY
 * inside a window, derived table, CTE or aggregate does not constrain the final result.
 */
export function catalogueOrderColumns(sql: string, columns: readonly string[]): string[] {
  const plan = compileQuery(sql, { optimize: false });
  // The parser hides unselected sort expressions behind this projection wrapper. Such a
  // query needs a PostgreSQL key witness instead of silently losing its order check.
  if (plan.base.alias === "(ordered)")
    throw new Error("unselected catalogue ORDER BY key behind parser projection");
  return plan.orderBy.map(({ expression }) => {
    if (expression.kind !== "column") throw new Error("unmapped catalogue ORDER BY expression");
    const name = expression.reference.split(".").at(-1) ?? "";
    if (!columns.includes(name)) throw new Error(`unselected catalogue ORDER BY key: ${name}`);
    return name;
  });
}

export function assertCatalogueOrder(
  sql: string,
  columns: readonly string[],
  actual: ReadonlyArray<Record<string, unknown>>,
  expected: ReadonlyArray<Record<string, unknown>>,
): void {
  const keys = catalogueOrderColumns(sql, columns);
  if (keys.length === 0) return;
  const sequence = (rows: ReadonlyArray<Record<string, unknown>>) =>
    rows.map((row) =>
      encodeValue(
        keys.map((key) => {
          const value = row[key];
          // PostgreSQL treats signed zeros as peers. NaNs encode identically via encodeValue.
          return typeof value === "number" && value === 0 ? 0 : value;
        }),
      ),
    );
  if (JSON.stringify(sequence(actual)) !== JSON.stringify(sequence(expected)))
    throw new Error("catalogue ORDER BY key sequence differs from PostgreSQL");
}
