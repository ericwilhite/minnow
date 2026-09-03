import { describe, expect, it } from "vitest";
import { tableToReopen, toCatalog, typeLabelOf, type TableInfo } from "./catalog.js";

const people: TableInfo = {
  name: "people",
  columns: [{ name: "id", type: "number", nullable: false, isUniqueKey: true }],
};
const orders: TableInfo = {
  name: "orders",
  columns: [{ name: "order_id", type: "number", nullable: false, isUniqueKey: true }],
};

/**
 * The explorer reports what it opened through `onOpen`, and after a catalog change what it opens
 * is decided here; the rail's highlight is only as right as this answer.
 */
describe("tableToReopen", () => {
  it("keeps the open table when the new catalog still has it", () => {
    expect(tableToReopen([people, orders], "orders")).toBe("orders");
  });

  it("falls back to the first table when the open one was dropped", () => {
    expect(tableToReopen([people, orders], "gone")).toBe("people");
  });

  it("opens the first table when nothing was open yet", () => {
    expect(tableToReopen([people, orders], undefined)).toBe("people");
  });

  it("prefers a real table over a view that sorts first", () => {
    const view: TableInfo = { name: "all_people", columns: [], view: { sql: "SELECT 1" } };
    expect(tableToReopen([view, people], undefined)).toBe("people");
    expect(tableToReopen([view], undefined)).toBe("all_people");
  });

  it("reports nothing to highlight when the catalog is empty", () => {
    expect(tableToReopen([], "people")).toBeUndefined();
    expect(tableToReopen([], undefined)).toBeUndefined();
  });
});

describe("toCatalog", () => {
  it("reads the declared type, the enum values, and the generated expression off a column", () => {
    const [table] = toCatalog([
      {
        name: "shipments",
        uniqueKey: "shipment_id",
        columns: [
          { name: "shipment_id", type: "number", integer: true },
          {
            name: "state",
            type: "string",
            sqlDomain: { kind: "enum", name: "shipment_state", values: ["packed", "shipped"] },
          },
          { name: "kind", type: "string", enumValues: ["a", "b"] },
          {
            name: "weight_kg",
            type: "string",
            sqlDomain: { kind: "numeric", precision: 6, scale: 2 },
          },
          {
            name: "weight_g",
            type: "string",
            nullable: true,
            sqlDomain: { kind: "numeric" },
            generatedValue: { kind: "stored", sql: "weight_kg * 1000" },
          },
          { name: "tags", type: "string", sqlDomain: { kind: "array", element: "text" } },
          { name: "doc", type: "string", sqlDomain: { kind: "jsonb" } },
        ],
      },
    ]);
    expect(table?.columns.map((column) => column.typeLabel)).toEqual([
      "INTEGER",
      "shipment_state",
      undefined,
      "NUMERIC(6,2)",
      "NUMERIC",
      "TEXT[]",
      "JSONB",
    ]);
    expect(table?.columns[1]?.enumValues).toEqual(["packed", "shipped"]);
    expect(table?.columns[2]?.enumValues).toEqual(["a", "b"]);
    expect(table?.columns[4]?.generated).toBe("weight_kg * 1000");
    expect(typeLabelOf({ name: "n", type: "number" })).toBeUndefined();
  });

  it("marks views from the introspection, and appends a view the table list omits", () => {
    const catalog = toCatalog(
      [
        { name: "people", uniqueKey: "id", columns: [{ name: "id", type: "number" }] },
        { name: "adults", columns: [{ name: "id", type: "number", nullable: true }] },
      ],
      {
        tables: [
          {
            name: "people",
            managed: true,
            columns: [
              { id: "c1", name: "id", type: "number", nullable: false, isAutoIncrementing: false },
            ],
            uniqueKeyColumnId: "c1",
            primaryKeyColumnIds: ["c1"],
            foreignKeys: [
              {
                name: "people_boss",
                columns: ["id"],
                parentTable: "people",
                parentColumns: ["id"],
                onDelete: "restrict",
                enforced: true,
              },
            ],
            checks: [{ name: "positive", sql: "id > 0" }],
            triggers: [{ id: "t1", name: "audit", event: "insert", timing: "after" }],
          },
        ],
        views: [
          { name: "adults", sql: "SELECT id FROM people", columns: [], managed: false },
          {
            name: "minors",
            sql: "SELECT id FROM people WHERE id < 0",
            columns: [
              { id: "v1", name: "id", type: "number", nullable: true, isAutoIncrementing: false },
            ],
            managed: false,
          },
        ],
      },
    );
    expect(catalog.map((table) => [table.name, table.view?.sql])).toEqual([
      ["people", undefined],
      ["adults", "SELECT id FROM people"],
      ["minors", "SELECT id FROM people WHERE id < 0"],
    ]);
    const people = catalog[0];
    expect(people?.foreignKeys?.[0]?.parentTable).toBe("people");
    expect(people?.checks).toEqual([{ name: "positive", sql: "id > 0" }]);
    expect(people?.triggers).toEqual([{ name: "audit", event: "insert", timing: "after" }]);
    expect(people?.primaryKey).toEqual(["id"]);
    expect(catalog[2]?.columns[0]?.nullable).toBe(true);
  });
});
