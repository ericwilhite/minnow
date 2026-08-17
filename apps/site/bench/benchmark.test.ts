import { describe, expect, it } from "vitest";
import { getScenario, relationalRowCount, relationalTotalRows, scenarios } from "./benchmark";

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
});
