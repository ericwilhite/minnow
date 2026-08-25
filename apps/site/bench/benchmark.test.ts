import { MinnowDatabase } from "@minnowdb/core";
import { MemoryBlockStore } from "@minnowdb/core/storage/memory";
import { describe, expect, it } from "vitest";
import {
  getScenario,
  relationalRowCount,
  relationalTotalRows,
  scenarios,
  type EntityDefinition,
} from "./benchmark";
import { createSecondaryIndexes, createTableSql, secondaryIndexSql } from "./engines/shared";

describe("benchmark scenarios", () => {
  it("keeps the public logical type vocabulary simple", () => {
    const types = new Set(
      scenarios.flatMap((scenario) =>
        scenario.entities.flatMap((entity) => entity.columns.map((column) => column.type)),
      ),
    );
    expect([...types].sort()).toEqual(["boolean", "datetime", "number", "string"]);
  });

  it("defines deterministic multi-entity profiles", () => {
    const commerce = getScenario("commerce");
    expect(commerce.entities).toHaveLength(50);
    expect(commerce.entities.map((entity) => entity.name)).toContain("order_taxes");
    expect(commerce.entities.map((entity) => entity.name)).toContain("payment_transactions");
    expect(commerce.entities.map((entity) => entity.name)).toContain("order_events");
    expect(commerce.entities.map((entity) => entity.name)).toContain("audit_events");
    expect(relationalTotalRows(1)).toBe(95_616);
    expect(relationalRowCount("sales_channels", 100)).toBe(600);
    expect(relationalRowCount("order_events", 100)).toBe(2_500_000);
    expect(relationalRowCount("audit_events", 100)).toBe(2_200_000);
    const orders = commerce.entities.find((entity) => entity.name === "orders");
    const total = orders?.columns.find((column) => column.name === "total");
    expect(total?.valueAt(42, 1_000, 1)).toBe(total?.valueAt(42, 1_000, 1));
  });

  it("defines one shared secondary-index list for every engine", async () => {
    const entities = getScenario("commerce").entities;
    const expected = entities.flatMap((entity) =>
      (entity.secondaryIndexes ?? []).map(
        (column) => `CREATE INDEX "idx_${entity.name}_${column}" ON "${entity.name}" ("${column}")`,
      ),
    );
    const executed: string[] = [];

    await createSecondaryIndexes(entities, (sql) => {
      executed.push(sql);
    });

    expect(expected).toHaveLength(81);
    expect(
      entities.every((entity) =>
        (entity.secondaryIndexes ?? []).every(
          (column) =>
            column !== entity.primaryKey &&
            entity.columns.some((definition) => definition.name === column),
        ),
      ),
    ).toBe(true);
    expect(secondaryIndexSql(entities)).toEqual(expected);
    expect(executed).toEqual(expected);
  });

  it("builds the shared workload indexes in Minnow's catalog", async () => {
    const parent: EntityDefinition = {
      name: "parents",
      rows: () => 1,
      primaryKey: "parent_id",
      columns: [
        {
          name: "parent_id",
          type: "number",
          estimatedBytesPerRow: 8,
          valueAt: () => 1,
        },
      ],
    };
    const child: EntityDefinition = {
      name: "children",
      rows: () => 1,
      primaryKey: "child_id",
      secondaryIndexes: ["parent_id"],
      columns: [
        {
          name: "child_id",
          type: "number",
          estimatedBytesPerRow: 8,
          valueAt: () => 1,
        },
        {
          name: "parent_id",
          type: "number",
          estimatedBytesPerRow: 8,
          valueAt: () => 1,
        },
      ],
    };
    const store = new MemoryBlockStore();
    const database = new MinnowDatabase(store);
    try {
      await database.execute(createTableSql(parent));
      await database.execute(createTableSql(child));
      await database.execute("INSERT INTO children (child_id, parent_id) VALUES (1, 1)");

      await createSecondaryIndexes([parent, child], (sql) => database.execute(sql));

      const table = (await store.listTables()).find(({ name }) => name === "children");
      const index = Object.values(table?.secondaryIndexes ?? {}).find(
        ({ name }) => name === "idx_children_parent_id",
      );
      expect(index).toMatchObject({
        name: "idx_children_parent_id",
        state: "ready",
      });
      expect(index?.columnId).toBe(table?.columns.find(({ name }) => name === "parent_id")?.id);
    } finally {
      await database.close();
      store.close();
    }
  });
});
