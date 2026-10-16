/**
 * Shared fixture for the prepared-plan suites: one table with integer, text, DATE, and NUMERIC
 * columns, and a runner that answers a statement through the ordinary path and through an
 * eagerly built template and requires the two to agree exactly.
 */
import { expect } from "vitest";
import { MemoryBlockStore } from "../storage/index.js";
import { MinnowDatabase } from "./database.js";
import { preparedPlanTestHooks } from "./prepared-plan.js";
import type { QueryParameter, QueryResult } from "./query.js";

export async function seeded(store = new MemoryBlockStore()): Promise<MinnowDatabase> {
  const database = new MinnowDatabase(store, { autoCompact: false });
  await database.execute(
    "CREATE TABLE items (id INTEGER PRIMARY KEY, grp INTEGER NOT NULL, name TEXT NOT NULL, day DATE, price NUMERIC(8, 2))",
  );
  await database.execute(
    `INSERT INTO items VALUES ${Array.from(
      { length: 40 },
      (_, index) =>
        `(${String(index + 1)}, ${String(index % 4)}, 'n${String(index % 7)}', DATE '2026-01-${String((index % 9) + 1).padStart(2, "0")}', ${String(index)}.25)`,
    ).join(", ")}`,
  );
  return database;
}

/** Runs one statement prepared (template built eagerly) and ordinarily, and compares them. */
export async function both(
  database: MinnowDatabase,
  sql: string,
  params?: readonly QueryParameter[],
): Promise<{ prepared: QueryResult; served: boolean }> {
  const options = { memoize: false, ...(params === undefined ? {} : { params: [...params] }) };
  preparedPlanTestHooks.disabled = true;
  let ordinary: QueryResult;
  try {
    ordinary = await database.query(sql, options);
  } finally {
    preparedPlanTestHooks.disabled = false;
  }
  const served = preparedPlanTestHooks.served;
  preparedPlanTestHooks.eager = true;
  let prepared: QueryResult;
  try {
    prepared = await database.query(sql, options);
  } finally {
    preparedPlanTestHooks.eager = false;
  }
  expect(prepared.rows, sql).toEqual(ordinary.rows);
  expect(prepared.columns, sql).toEqual(ordinary.columns);
  expect(prepared.columnDomains, sql).toEqual(ordinary.columnDomains);
  return { prepared, served: preparedPlanTestHooks.served > served };
}
