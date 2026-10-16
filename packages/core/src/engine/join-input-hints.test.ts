/**
 * A LEFT join's key inherits WHERE constants on the preserved key it equals, as a hint that lets
 * input preparation read only the matching rows of that side (through its secondary index or
 * key). The hint is never evaluated, so every answer must equal the same statement written so
 * that no hint can form — the constant applied to `key + 0` — including unmatched outer rows,
 * NULL keys, ranges, lists, chained joins, and mutation histories.
 */
import { describe, expect, it } from "vitest";
import { MemoryBlockStore } from "../storage/index.js";
import { MinnowDatabase } from "./database.js";
import { compileQuery, type QueryValue } from "./query.js";

async function customersDatabase(): Promise<MinnowDatabase> {
  const database = new MinnowDatabase(new MemoryBlockStore(), {
    rowsPerBlock: 32,
    autoCompact: false,
  });
  await database.execute(
    "CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT NOT NULL, since TIMESTAMP NOT NULL)",
  );
  await database.execute(
    "CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER, amount REAL NOT NULL, placed TIMESTAMP NOT NULL)",
  );
  await database.execute("CREATE INDEX orders_customer ON orders (customer_id)");
  await database.execute("CREATE TABLE profiles (id INTEGER PRIMARY KEY, bio TEXT NOT NULL)");
  await database.insertBatch(
    "customers",
    Array.from({ length: 60 }, (_, index) => ({
      id: index + 1,
      name: `customer-${String(index + 1)}`,
      since: new Date(Date.UTC(2026, 0, 1 + (index % 20))),
    })),
  );
  for (let batch = 0; batch < 3; batch += 1) {
    await database.insertBatch(
      "orders",
      Array.from({ length: 200 }, (_, offset) => {
        const id = batch * 200 + offset + 1;
        return {
          id,
          // Customers above 50 have no orders; a few orders belong to nobody.
          customer_id: id % 23 === 0 ? null : (id % 50) + 1,
          amount: (id % 17) / 4,
          placed: new Date(Date.UTC(2026, 0, 1 + (id % 20))),
        };
      }),
    );
  }
  await database.insertBatch(
    "profiles",
    Array.from({ length: 30 }, (_, index) => ({ id: index * 2 + 1, bio: `bio-${String(index)}` })),
  );
  return database;
}

/** The statement, and the same statement with every `c.id` constant applied to `c.id + 0`. */
async function differential(
  database: MinnowDatabase,
  sql: string,
  params: QueryValue[] = [],
): Promise<Array<Record<string, QueryValue>>> {
  const hinted = await database.query(sql, { params, memoize: false });
  const unhinted = await database.query(sql.replaceAll("WHERE c.id", "WHERE c.id + 0"), {
    params,
    memoize: false,
  });
  expect(hinted.columns).toEqual(unhinted.columns);
  const sorted = (rows: ReadonlyArray<Record<string, QueryValue>>): string[] =>
    rows.map((row) => JSON.stringify(row)).sort();
  expect(sorted(hinted.rows)).toEqual(sorted(unhinted.rows));
  return hinted.rows;
}

describe("LEFT join input hints", () => {
  it("are derived from WHERE constants on the preserved key, and only for LEFT joins", () => {
    const plan = compileQuery(
      "SELECT c.name, o.id FROM customers c LEFT JOIN orders o ON o.customer_id = c.id WHERE c.id = ?",
    );
    expect(plan.joins[0]?.inputHints).toEqual([
      {
        operator: "=",
        left: { kind: "column", reference: "o.customer_id" },
        right: { kind: "parameter", index: 0 },
      },
    ]);
    const unpinned = compileQuery(
      "SELECT c.name, o.id FROM customers c LEFT JOIN orders o ON o.customer_id = c.id WHERE c.name = 'x'",
    );
    expect(unpinned.joins[0]?.inputHints).toBeUndefined();
  });

  it("answer exactly as the unhinted statement", async () => {
    const database = await customersDatabase();
    try {
      const grouped =
        "SELECT c.id, c.name, COUNT(o.id) AS n, SUM(o.amount) AS s FROM customers c LEFT JOIN orders o ON o.customer_id = c.id WHERE c.id = ? GROUP BY c.id, c.name";
      for (const id of [1, 7, 50, 51, 60, 1000]) {
        await differential(database, grouped, [id]);
      }
      const rows = await differential(database, grouped, [55]);
      expect(rows).toEqual([{ id: 55, name: "customer-55", n: 0, s: null }]);
      for (const [sql, params] of [
        [
          "SELECT c.name, o.id, o.amount FROM customers c LEFT JOIN orders o ON c.id = o.customer_id WHERE c.id = ?",
          [12],
        ],
        [
          "SELECT c.name, o.id FROM customers c LEFT JOIN orders o ON o.customer_id = c.id WHERE c.id IN (3, 52, 9)",
          [],
        ],
        [
          "SELECT c.name, o.id FROM customers c LEFT JOIN orders o ON o.customer_id = c.id WHERE c.id > ? AND c.id <= ?",
          [45, 53],
        ],
        [
          "SELECT c.name, o.id, p.bio FROM customers c LEFT JOIN orders o ON o.customer_id = c.id LEFT JOIN profiles p ON p.id = c.id WHERE c.id = ?",
          [5],
        ],
        [
          "SELECT c.name, p.bio FROM customers c LEFT JOIN profiles p ON p.id = c.id WHERE c.id = ?",
          [4],
        ],
        [
          "SELECT c.name, o.id FROM customers c LEFT JOIN orders o ON o.customer_id = c.id WHERE c.id = ? AND o.amount > 1",
          [8],
        ],
        [
          "SELECT c.name, o.id FROM customers c LEFT JOIN orders o ON o.customer_id = c.id WHERE c.id = ? AND o.id IS NULL",
          [57],
        ],
      ] as const) {
        await differential(database, sql, [...params]);
      }
    } finally {
      await database.close();
    }
  });

  it("stay exact over a mutation history of the hinted side", async () => {
    const database = await customersDatabase();
    try {
      await database.execute("DELETE FROM orders WHERE customer_id = 7 AND amount < 2");
      await database.execute("UPDATE orders SET customer_id = 7 WHERE id = 9");
      await database.execute("UPDATE orders SET customer_id = 11 WHERE id = 6");
      // The pruned side is narrowed to the blocks holding each lookup's candidates, so
      // successive lookups read differently narrowed copies of one segment; the inner join's
      // mirrored WHERE predicate takes the same path. (A pooled column vector keyed by segment
      // ids alone once answered a later lookup with an earlier one's rows.)
      for (const join of ["LEFT JOIN", "JOIN"]) {
        const sql = `SELECT c.id, COUNT(o.id) AS n, SUM(o.amount) AS s FROM customers c ${join} orders o ON o.customer_id = c.id WHERE c.id = ? GROUP BY c.id`;
        for (const id of [6, 7, 11, 13, 7, 11]) await differential(database, sql, [id]);
      }
    } finally {
      await database.close();
    }
  });
});
