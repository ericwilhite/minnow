import { describe, expect, it } from "vitest";
import { executeReferenceSql } from "./reference-sql.js";

const tables = new Map([
  [
    "customers",
    [
      { customer_id: 1, segment: "growth" },
      { customer_id: 2, segment: "starter" },
    ],
  ],
  [
    "orders",
    [
      { order_id: 1, customer_id: 1, status: "paid", total: 40 },
      { order_id: 2, customer_id: 1, status: "paid", total: 60 },
      { order_id: 3, customer_id: 2, status: "created", total: 25 },
    ],
  ],
]);

describe("reference SQL runner", () => {
  it("filters, joins, groups, orders, and reports measurements", () => {
    const result = executeReferenceSql(
      "SELECT c.segment, COUNT(*) AS orders, SUM(o.total) AS revenue FROM customers c JOIN orders o ON o.customer_id = c.customer_id WHERE o.status = 'paid' GROUP BY c.segment ORDER BY revenue DESC LIMIT 10",
      tables,
      "run-1",
      3,
    );

    expect(result.previewRows).toEqual([{ segment: "growth", orders: 2, revenue: 100 }]);
    expect(result.rowCount).toBe(1);
    expect(result.tables).toEqual(["customers", "orders"]);
    expect(result.metrics).toMatchObject({ iterations: 3, sourceRows: 5, joinedRows: 3 });
  });

  it("supports point filters and rejects mutation statements", () => {
    expect(
      executeReferenceSql(
        "SELECT order_id, total FROM orders WHERE total >= 40 ORDER BY total DESC",
        tables,
        "run-1",
      ).previewRows,
    ).toEqual([
      { order_id: 2, total: 60 },
      { order_id: 1, total: 40 },
    ]);
    expect(() => executeReferenceSql("DELETE FROM orders", tables, "run-1")).toThrow(
      "Supported shape",
    );
  });
});
