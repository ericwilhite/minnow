/**
 * The index read serves a conjunction of equalities that does not cover the key through a ready
 * secondary index on the pinned columns: postings name candidate rows, and every candidate is
 * checked against every equality. Each statement runs with the fast paths enabled and disabled,
 * and the complete results — rows, their order, columns, domains — must be identical; shapes it
 * cannot prove must fall back.
 */
import { afterEach, describe, expect, it } from "vitest";
import { MemoryBlockStore } from "../storage/index.js";
import { MinnowDatabase } from "./database.js";
import type { QueryValue } from "./query.js";
import { pointReadTestHooks } from "./point-read.js";

afterEach(() => {
  pointReadTestHooks.disabled = false;
});

async function differential(
  database: MinnowDatabase,
  sql: string,
  params: QueryValue[] = [],
): Promise<{ served: boolean; rows: Array<Record<string, QueryValue>> }> {
  pointReadTestHooks.disabled = false;
  const before = pointReadTestHooks.served;
  const fast = await database.query(sql, { params, memoize: false });
  const served = pointReadTestHooks.served > before;
  pointReadTestHooks.disabled = true;
  const ordinary = await database.query(sql, { params, memoize: false });
  pointReadTestHooks.disabled = false;
  expect(fast.columns).toEqual(ordinary.columns);
  expect(fast.columnDomains).toEqual(ordinary.columnDomains);
  expect(fast.rows).toEqual(ordinary.rows);
  return { served, rows: fast.rows };
}

async function ordersDatabase(): Promise<MinnowDatabase> {
  const database = new MinnowDatabase(new MemoryBlockStore(), {
    rowsPerBlock: 64,
    autoCompact: false,
  });
  await database.execute(
    "CREATE TABLE orders (id INTEGER PRIMARY KEY, customer INTEGER, status TEXT, total NUMERIC(10, 2), placed TIMESTAMP NOT NULL, rush BOOLEAN NOT NULL)",
  );
  await database.execute("CREATE INDEX orders_customer ON orders (customer)");
  await database.execute("CREATE INDEX orders_status_customer ON orders (status, customer)");
  await database.execute("CREATE INDEX orders_placed ON orders (placed)");
  for (let batch = 0; batch < 4; batch += 1) {
    await database.insertBatch(
      "orders",
      Array.from({ length: 300 }, (_, offset) => {
        const id = batch * 300 + offset + 1;
        return {
          id,
          customer: id % 41 === 0 ? null : id % 60,
          status: id % 17 === 0 ? null : (["new", "paid", "shipped"][id % 3] ?? "new"),
          total: `${String(id % 90)}.50`,
          placed: new Date(Date.UTC(2026, 0, 1 + (id % 30))),
          rush: id % 4 === 0,
        };
      }),
    );
  }
  return database;
}

describe("index read", () => {
  it("serves rows and counts through a secondary index", async () => {
    const database = await ordersDatabase();
    try {
      for (const [sql, params] of [
        ["SELECT id, status, total FROM orders WHERE customer = ?", [7]],
        ["SELECT * FROM orders WHERE customer = ?", [13]],
        ["SELECT COUNT(*) AS n FROM orders WHERE customer = ?", [13]],
        ["SELECT COUNT(*) FROM orders WHERE customer = ?", [59]],
        ["SELECT id FROM orders WHERE customer = ?", [10_000]],
        ["SELECT COUNT(*) AS n FROM orders WHERE customer = ?", [10_000]],
        ["SELECT id, rush FROM orders WHERE customer = ? AND rush = ?", [8, true]],
        ["SELECT id FROM orders WHERE status = ? AND customer = ?", ["paid", 1]],
        ["SELECT id, customer FROM orders WHERE status = ?", ["shipped"]],
        ["SELECT COUNT(*) AS n FROM orders WHERE status = ?", ["new"]],
        ["SELECT id FROM orders WHERE placed = ?", [new Date(Date.UTC(2026, 0, 5))]],
        ["SELECT o.id AS x FROM orders o WHERE o.customer = ?", [3]],
        ["SELECT id FROM orders WHERE customer = ? AND customer = ?", [3, 3]],
        ["SELECT id FROM orders WHERE customer = ? AND customer = ?", [3, 4]],
        ["SELECT id FROM orders WHERE customer = 21", []],
      ] as const) {
        const { served } = await differential(database, sql, [...params]);
        expect(served, sql).toBe(true);
      }
    } finally {
      await database.close();
    }
  });

  it("falls back for every shape or history it cannot prove", async () => {
    const database = await ordersDatabase();
    try {
      for (const [sql, params] of [
        // NULL, cross-type, and domain-typed equalities.
        ["SELECT id FROM orders WHERE customer = ?", [null]],
        ["SELECT id FROM orders WHERE customer = ?", ["7"]],
        ["SELECT id FROM orders WHERE total = ?", ["7.50"]],
        // No ready index leads with a pinned column.
        ["SELECT id FROM orders WHERE rush = ?", [true]],
        ["SELECT id FROM orders WHERE customer > ?", [50]],
        ["SELECT SUM(id) AS s FROM orders WHERE customer = ?", [7]],
        ["SELECT COUNT(id) AS n FROM orders WHERE customer = ?", [7]],
        ["SELECT COUNT(*) AS n FROM orders WHERE customer = ? GROUP BY status", [7]],
        ["SELECT id FROM orders WHERE customer = ? ORDER BY id DESC", [7]],
      ] as const) {
        const { served } = await differential(database, sql, [...params]);
        expect(served, sql).toBe(false);
      }
      await database.execute("UPDATE orders SET customer = 7 WHERE id = 1");
      const { served } = await differential(
        database,
        "SELECT id FROM orders WHERE customer = ?",
        [7],
      );
      expect(served).toBe(false);
      await database.compactTable("orders");
      for (const customer of [7, 1, 8]) {
        await differential(database, "SELECT id FROM orders WHERE customer = ?", [customer]);
        await differential(database, "SELECT COUNT(*) AS n FROM orders WHERE customer = ?", [
          customer,
        ]);
      }
    } finally {
      await database.close();
    }
  });

  it("sees a transaction's own staged rows, which no index holds yet", async () => {
    const database = await ordersDatabase();
    try {
      await database.execute("BEGIN");
      await database.execute(
        "INSERT INTO orders (id, customer, status, total, placed, rush) VALUES (?, ?, ?, ?, ?, ?)",
        [20_000, 7, "new", "2.00", new Date(Date.UTC(2026, 0, 3)), false],
      );
      const inside = await database.query("SELECT id FROM orders WHERE customer = ?", {
        params: [7],
        memoize: false,
      });
      expect(inside.rows.some((row) => row.id === 20_000)).toBe(true);
      const counted = await database.query("SELECT COUNT(*) AS n FROM orders WHERE customer = ?", {
        params: [7],
        memoize: false,
      });
      expect(counted.rows[0]?.n).toBe(inside.rows.length);
      await database.execute("ROLLBACK");
      const { rows } = await differential(
        database,
        "SELECT id FROM orders WHERE customer = ?",
        [7],
      );
      expect(rows.some((row) => row.id === 20_000)).toBe(false);
    } finally {
      await database.close();
    }
  });

  it("sees rows appended after the index was built", async () => {
    const database = await ordersDatabase();
    try {
      for (let round = 0; round < 3; round += 1) {
        await database.insertBatch("orders", [
          {
            id: 10_000 + round,
            customer: 7,
            status: "new",
            total: "1.00",
            placed: new Date(Date.UTC(2026, 0, 2)),
            rush: false,
          },
        ]);
        const { served, rows } = await differential(
          database,
          "SELECT id FROM orders WHERE customer = ? AND status = ?",
          [7, "new"],
        );
        expect(served).toBe(true);
        expect(rows.some((row) => row.id === 10_000 + round)).toBe(true);
      }
    } finally {
      await database.close();
    }
  });
});
