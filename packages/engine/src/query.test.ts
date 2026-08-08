import { MemoryBlockStore } from "@browserdatabase/storage-idb";
import { describe, expect, it } from "vitest";
import { BrowserDatabase } from "./database.js";
import { compileQuery, createPreparedQuery, executeQuery } from "./query.js";

describe("public SQL queries", () => {
  it("executes filters, arithmetic, grouping, joins, and multi-column ordering", async () => {
    const database = new BrowserDatabase(new MemoryBlockStore(), { rowsPerBlock: 2 });
    await database.createTable({
      name: "customers",
      uniqueKey: "customer_id",
      columns: [
        { name: "customer_id", type: "number" },
        { name: "segment", type: "string" },
      ],
    });
    await database.createTable({
      name: "orders",
      uniqueKey: "order_id",
      columns: [
        { name: "order_id", type: "number" },
        { name: "customer_id", type: "number" },
        { name: "status", type: "string" },
        { name: "total", type: "number" },
      ],
    });
    await database.insertBatch("customers", {
      columns: { customer_id: [1, 2, 3], segment: ["business", "consumer", "business"] },
    });
    await database.insertBatch("orders", {
      columns: {
        order_id: [1, 2, 3, 4],
        customer_id: [1, 2, 1, 3],
        status: ["paid", "paid", "pending", "paid"],
        total: [10.125, 20, 5, 30],
      },
    });

    expect(
      await database.query(
        "SELECT order_id, total * 2 AS doubled FROM orders WHERE total >= 10 ORDER BY order_id DESC LIMIT 2",
      ),
    ).toEqual({
      columns: ["order_id", "doubled"],
      rows: [
        { order_id: 4, doubled: 60 },
        { order_id: 2, doubled: 40 },
      ],
    });
    expect(
      await database.query(
        "SELECT c.segment, o.status, COUNT(*) AS order_count, ROUND(SUM(o.total), 2) AS revenue FROM customers c JOIN orders o ON o.customer_id = c.customer_id GROUP BY c.segment, o.status ORDER BY c.segment, o.status DESC",
      ),
    ).toEqual({
      columns: ["segment", "status", "order_count", "revenue"],
      rows: [
        { segment: "business", status: "pending", order_count: 1, revenue: 5 },
        { segment: "business", status: "paid", order_count: 2, revenue: 40.13 },
        { segment: "consumer", status: "paid", order_count: 1, revenue: 20 },
      ],
    });
  });

  it("keeps prepared queries on one immutable snapshot", async () => {
    const database = new BrowserDatabase(new MemoryBlockStore());
    await database.createTable({
      name: "events",
      columns: [{ name: "value", type: "number" }],
    });
    await database.insertBatch("events", { columns: { value: [1, 2] } });
    const prepared = await database.prepareQuery("SELECT COUNT(*) AS count FROM events");
    await database.insertBatch("events", { columns: { value: [3] } });

    expect(prepared.execute().rows).toEqual([{ count: 2 }]);
    expect((await database.query("SELECT COUNT(*) AS count FROM events")).rows).toEqual([
      { count: 3 },
    ]);
    prepared.close();
    expect(() => prepared.execute()).toThrow("Prepared query is closed");
  });

  it("implements left joins and SQL null comparison semantics", () => {
    const plan = compileQuery(
      "SELECT a.id, b.value FROM a LEFT JOIN b ON b.id = a.id WHERE b.value != NULL ORDER BY a.id",
    );
    expect(
      executeQuery(
        plan,
        new Map([
          ["a", [{ id: 1 }, { id: 2 }]],
          ["b", [{ id: 1, value: "one" }]],
        ]),
      ).rows,
    ).toEqual([]);

    expect(
      executeQuery(
        compileQuery("SELECT a.id, b.value FROM a LEFT JOIN b ON b.id = a.id"),
        new Map([
          ["a", [{ id: null }]],
          ["b", [{ id: null, value: "must not match" }]],
        ]),
      ).rows,
    ).toEqual([{ id: null, value: null }]);
  });

  it("rejects unsafe and malformed statements", async () => {
    expect(() => compileQuery("DELETE FROM events")).toThrow("Expected SELECT");
    expect(() => compileQuery("SELECT * FROM events; SELECT * FROM events")).toThrow(
      "one SELECT statement",
    );
    expect(() => compileQuery("SELECT ROUND(value, 1, 2) FROM events")).toThrow(
      "ROUND requires one or two arguments",
    );
    expect(() => compileQuery("SELECT 'unfinished FROM events")).toThrow(
      "Unterminated string literal",
    );
    const database = new BrowserDatabase(new MemoryBlockStore());
    await database.createTable({ name: "events", columns: [{ name: "value", type: "number" }] });
    await expect(database.query("SELECT missing FROM events")).rejects.toThrow(
      "Ambiguous or missing column",
    );
    await expect(database.query("SELECT value, COUNT(*) AS count FROM events")).rejects.toThrow(
      "must appear in GROUP BY",
    );
    await expect(
      database.query("SELECT e.value FROM events e JOIN events e ON e.value = e.value"),
    ).rejects.toThrow("Table aliases must be unique");
  });

  it("executes an already materialized prepared plan repeatedly", () => {
    const plan = compileQuery("SELECT COUNT(*) AS count FROM rows");
    const prepared = createPreparedQuery(plan, new Map([["rows", [{ value: 1 }, { value: 2 }]]]));
    expect(prepared.execute()).toEqual({ columns: ["count"], rows: [{ count: 2 }] });
    expect(prepared.execute()).toEqual({ columns: ["count"], rows: [{ count: 2 }] });
  });
});
