import { describe, expect, it } from "vitest";
import { MinnowDatabase } from "@minnowdb/core";
import { MemoryBlockStore } from "@minnowdb/core/storage";
import { retailBatches, retailEstimatedRows, retailSchema, type Row } from "./retail";

/** One small build, loaded once and shared: every assertion below reads the same database. */
async function build(scale = 0.25): Promise<{
  database: MinnowDatabase;
  counts: Record<string, number>;
  total: number;
}> {
  const database = new MinnowDatabase(new MemoryBlockStore());
  for (const table of retailSchema) {
    await database.createTable({
      name: table.name,
      uniqueKey: table.uniqueKey,
      columns: table.columns,
    });
  }
  const counts: Record<string, number> = {};
  let total = 0;
  for (const batch of retailBatches({ scale })) {
    counts[batch.table] = (counts[batch.table] ?? 0) + batch.rows.length;
    total += batch.rows.length;
    await database.insertBatch(batch.table, batch.rows);
  }
  return { database, counts, total };
}

function collect(scale: number): Map<string, Row[]> {
  const tables = new Map<string, Row[]>();
  for (const batch of retailBatches({ scale })) {
    const rows = tables.get(batch.table) ?? [];
    rows.push(...batch.rows);
    tables.set(batch.table, rows);
  }
  return tables;
}

const scalar = async (database: MinnowDatabase, sql: string): Promise<number> => {
  const result = await database.query(sql, { memoize: false });
  const value = Object.values(result.rows[0] ?? {})[0];
  return typeof value === "number" ? value : Number(value);
};

describe("retail dataset", () => {
  it("builds every table with the schema it declares", async () => {
    const { counts, total } = await build();
    for (const table of retailSchema) expect(counts[table.name] ?? 0).toBeGreaterThan(0);
    expect(total).toBeGreaterThan(100_000);
    // The estimate drives a progress bar before any row exists, so it has to be close.
    expect(Math.abs(total - retailEstimatedRows(0.25)) / total).toBeLessThan(0.1);
  });

  it("is deterministic", () => {
    const first = collect(0.1);
    const second = collect(0.1);
    expect([...second.keys()].sort()).toEqual([...first.keys()].sort());
    for (const [table, rows] of first) {
      expect(second.get(table)).toEqual(rows);
    }
  });

  it("changes completely with the seed", () => {
    const [a] = [...collect(0.1)].filter(([table]) => table === "customers");
    const other = new Map<string, Row[]>();
    for (const batch of retailBatches({ scale: 0.1, seed: 7 })) {
      if (batch.table !== "customers") continue;
      other.set("customers", [...(other.get("customers") ?? []), ...batch.rows]);
    }
    expect(other.get("customers")?.[0]).not.toEqual(a?.[1][0]);
  });

  it("has the cardinality of real data rather than a handful of repeated values", async () => {
    const { database } = await build();
    const customers = await scalar(database, "SELECT COUNT(*) AS n FROM customers");
    const names = await scalar(database, "SELECT COUNT(DISTINCT name) AS n FROM customers");
    const emails = await scalar(database, "SELECT COUNT(DISTINCT email) AS n FROM customers");
    const cities = await scalar(database, "SELECT COUNT(DISTINCT city) AS n FROM customers");
    const products = await scalar(database, "SELECT COUNT(DISTINCT name) AS n FROM products");
    const prices = await scalar(database, "SELECT COUNT(DISTINCT list_price) AS n FROM products");

    // Names collide the way real names do, but most people are distinct.
    expect(names / customers).toBeGreaterThan(0.6);
    // Emails are the identity column, so they are unique by construction.
    expect(emails).toBe(customers);
    expect(cities).toBeGreaterThan(50);
    expect(products).toBe(await scalar(database, "SELECT COUNT(*) AS n FROM products"));
    expect(prices).toBeGreaterThan(150);
  });

  it("puts the mode of purchase frequency at a single order, with a long tail", async () => {
    const { database } = await build();
    const spread = await database.query(
      `SELECT orders_placed, COUNT(*) AS customers FROM (
         SELECT customer_id, COUNT(*) AS orders_placed FROM orders GROUP BY customer_id
       ) AS per_customer GROUP BY orders_placed ORDER BY orders_placed`,
      { memoize: false },
    );
    const buckets = spread.rows.map((row) => Number(row.customers));
    expect(buckets[0]).toBeGreaterThan(buckets[1] ?? 0);
    expect(buckets[1]).toBeGreaterThan(buckets[2] ?? 0);
    // A real tail: somebody has ordered many times.
    expect(spread.rows.at(-1)?.orders_placed).toBeGreaterThan(15);

    // And a real population who signed up and never bought anything.
    const dormant = await scalar(
      database,
      `SELECT COUNT(*) AS n FROM customers c
       WHERE NOT EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.customer_id)`,
    );
    expect(dormant).toBeGreaterThan(0);
  });

  it("spreads revenue across categories instead of letting one dominate", async () => {
    const { database } = await build();
    const mix = await database.query(
      `SELECT p.category, SUM(i.line_total) AS revenue
       FROM order_items i JOIN products p ON p.product_id = i.product_id
       GROUP BY p.category ORDER BY revenue DESC`,
      { memoize: false },
    );
    const revenues = mix.rows.map((row) => Number(row.revenue));
    const total = revenues.reduce((sum, value) => sum + value, 0);
    expect(mix.rows.length).toBeGreaterThanOrEqual(6);
    expect((revenues[0] ?? 0) / total).toBeLessThan(0.4);
  });

  it("carries seasonality: December out-trades the summer", async () => {
    const { database } = await build();
    const byMonth = await database.query(
      `SELECT EXTRACT(month FROM placed_at) AS month, COUNT(*) AS orders
       FROM orders GROUP BY EXTRACT(month FROM placed_at) ORDER BY month`,
      { memoize: false },
    );
    const orders = new Map(byMonth.rows.map((row) => [Number(row.month), Number(row.orders)]));
    expect(orders.get(12) ?? 0).toBeGreaterThan((orders.get(7) ?? 0) * 1.2);
  });

  it("keeps the rows referentially and arithmetically consistent", async () => {
    const { database } = await build();
    expect(
      await scalar(
        database,
        `SELECT COUNT(*) AS n FROM orders o
         WHERE NOT EXISTS (SELECT 1 FROM customers c WHERE c.customer_id = o.customer_id)`,
      ),
    ).toBe(0);
    expect(
      await scalar(
        database,
        `SELECT COUNT(*) AS n FROM order_items i
         WHERE NOT EXISTS (SELECT 1 FROM orders o WHERE o.order_id = i.order_id)`,
      ),
    ).toBe(0);
    expect(
      await scalar(
        database,
        `SELECT COUNT(*) AS n FROM returns r
         WHERE NOT EXISTS (SELECT 1 FROM order_items i WHERE i.order_item_id = r.order_item_id)`,
      ),
    ).toBe(0);

    // Nobody ordered before they signed up.
    expect(
      await scalar(
        database,
        `SELECT COUNT(*) AS n FROM orders o JOIN customers c ON c.customer_id = o.customer_id
         WHERE o.placed_at < c.signed_up_on`,
      ),
    ).toBe(0);

    // The header totals agree with the lines they summarize, to the cent.
    expect(
      await scalar(
        database,
        `SELECT COUNT(*) AS n FROM (
           SELECT o.order_id, o.item_count, o.subtotal, o.discount, COUNT(i.order_item_id) AS lines,
                  ROUND(SUM(i.unit_price * i.quantity), 2) AS gross,
                  ROUND(SUM(i.discount), 2) AS off
           FROM orders o JOIN order_items i ON i.order_id = o.order_id
           GROUP BY o.order_id, o.item_count, o.subtotal, o.discount
         ) AS checked
         WHERE lines <> item_count OR ABS(gross - subtotal) > 0.02 OR ABS(off - discount) > 0.02`,
      ),
    ).toBe(0);

    // Only in-store baskets are paid in cash.
    expect(
      await scalar(
        database,
        "SELECT COUNT(*) AS n FROM orders WHERE payment_method = 'cash' AND channel <> 'in_store'",
      ),
    ).toBe(0);
  });
});
