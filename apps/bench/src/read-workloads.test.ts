/**
 * The OLTP (key- and range-lookup) reads, checked at unit speed exactly as the browser harness
 * checks them. Oracle and baseline rows are projected into canonical tuples, and expected row
 * counts are part of correctness. This covers every definition tagged OLTP, independent of its id.
 */
import { compileQuery, executeQuery, type DatabaseRow } from "@minnowdb/core";
import { describe, expect, it } from "vitest";
import { generateEntityBatch, getScenario } from "./benchmark.js";
import { buildReferenceQueryContext, referenceQueryDefinitions } from "./worker/reference-suite.js";

function generatedRows(table: string): DatabaseRow[] {
  const entity = getScenario("commerce").entities.find((candidate) => candidate.name === table);
  if (entity === undefined) throw new Error(`Missing entity: ${table}`);
  const rowCount = entity.rows(1);
  const columns = generateEntityBatch(entity, 0, rowCount, rowCount, 1);
  return Array.from({ length: rowCount }, (_, index) =>
    Object.fromEntries(
      entity.columns.map((column) => [column.name, columns[column.name]?.[index]]),
    ),
  ) as DatabaseRow[];
}

const cell = (value: unknown): unknown => (value instanceof Date ? value.toISOString() : value);
const tuples = (values: readonly unknown[], project: (row: never) => unknown[]): string =>
  JSON.stringify(values.map((row) => project(row as never).map(cell)));

describe("OLTP read queries", () => {
  it("agree across SQL, the JavaScript baseline, and the oracle", () => {
    const orders = getScenario("commerce").entities.find((entity) => entity.name === "orders");
    if (orders === undefined) throw new Error("Missing orders entity");
    const definitions = referenceQueryDefinitions(orders.rows(1)).filter(
      (definition) => definition.workload === "oltp",
    );
    expect(definitions.length).toBeGreaterThan(0);
    const tables = new Map<string, DatabaseRow[]>();
    for (const entity of getScenario("commerce").entities) {
      tables.set(entity.name, generatedRows(entity.name));
    }
    const context = buildReferenceQueryContext(tables);
    for (const definition of definitions) {
      const sqlRows = executeQuery(compileQuery(definition.sql), tables).rows;
      const oracle = definition.oracle(tables);
      const baseline = definition.baseline(context);
      expect({ id: definition.id, rows: oracle.length }).toEqual({
        id: definition.id,
        rows: definition.expectedRows,
      });
      expect({ id: definition.id, tuples: tuples(sqlRows, definition.project) }).toEqual({
        id: definition.id,
        tuples: tuples(oracle, definition.project),
      });
      expect({ id: definition.id, tuples: tuples(baseline, definition.project) }).toEqual({
        id: definition.id,
        tuples: tuples(oracle, definition.project),
      });
    }
  });
});
