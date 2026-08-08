import { describe, expect, it } from "vitest";
import { getScenario, scenarios } from "./benchmark.js";

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
    expect(commerce.entities).toHaveLength(27);
    expect(commerce.entities.map((entity) => entity.name)).toContain("order_taxes");
    expect(commerce.entities.map((entity) => entity.name)).toContain("payment_transactions");
    const orders = commerce.entities.find((entity) => entity.name === "orders");
    const total = orders?.columns.find((column) => column.name === "total");
    expect(total?.valueAt(42, 1_000, 1)).toBe(total?.valueAt(42, 1_000, 1));
  });
});
