/**
 * The selective (key- and range-lookup) reference queries, checked at unit speed and exactly
 * the way the browser harness checks them: the oracle and the JavaScript baseline are mapped
 * through `project` — they return row objects, not tuples — and the row count must equal the
 * definition's `expectedRows`, because the harness's `verified` flag requires both. A previous
 * version of this test compared raw oracle output and so passed while the real suite failed.
 */
import { compileQuery, executeQuery, type DatabaseRow } from "@minnowdb/core";
import { describe, expect, it } from "vitest";
import { generateEntityBatch, getScenario } from "./benchmark.js";
import { buildReferenceQueryContext, referenceQueryDefinitions } from "./worker/reference-suite.js";

function generatedRows(table: string): DatabaseRow[] {
  const entity = getScenario("commerce").entities.find((c) => c.name === table);
  if (entity === undefined) throw new Error(`Missing entity: ${table}`);
  const rowCount = entity.rows(1);
  const columns = generateEntityBatch(entity, 0, rowCount, rowCount, 1);
  return Array.from({ length: rowCount }, (_, index) =>
    Object.fromEntries(entity.columns.map((c) => [c.name, columns[c.name]?.[index]])),
  ) as DatabaseRow[];
}

const cell = (value: unknown): unknown => (value instanceof Date ? value.toISOString() : value);
const tuples = (values: readonly unknown[], project: (row: never) => unknown[]): string =>
  JSON.stringify(values.map((row) => project(row as never).map(cell)));

describe("selective reference queries", () => {
  it("agree across SQL, the JavaScript baseline, and the oracle", () => {
    const orders = getScenario("commerce").entities.find((e) => e.name === "orders");
    if (orders === undefined) throw new Error("Missing orders entity");
    const defs = referenceQueryDefinitions(orders.rows(1)).filter((d) => d.id.startsWith("s"));
    expect(defs.length).toBeGreaterThan(0);
    const tables = new Map<string, DatabaseRow[]>();
    for (const entity of getScenario("commerce").entities) {
      tables.set(entity.name, generatedRows(entity.name));
    }
    const context = buildReferenceQueryContext(tables);
    for (const def of defs) {
      const sqlRows = executeQuery(compileQuery(def.sql), tables).rows;
      const oracle = def.oracle(tables);
      const baseline = def.baseline(context);
      expect({ id: def.id, rows: oracle.length }).toEqual({
        id: def.id,
        rows: def.expectedRows,
      });
      expect({ id: def.id, t: tuples(sqlRows, def.project) }).toEqual({
        id: def.id,
        t: tuples(oracle, def.project),
      });
      expect({ id: def.id, t: tuples(baseline, def.project) }).toEqual({
        id: def.id,
        t: tuples(oracle, def.project),
      });
    }
  });
});
