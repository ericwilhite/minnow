/**
 * Every cache in the engine must be invisible: a query after a change answers as if nothing
 * had ever been cached. These tests exercise each layer — the result memo, the compiled plan
 * cache, the catalog state cache, and the buffer pool's pooled artifacts (decoded blocks, block
 * vectors, whole-segment column vectors, key locators, index candidates) — through the writes
 * and DDL that must bust them, across two engines on one store, and prove with the pool's own
 * counters and store spies that the caches are in fact being hit in between.
 */
import { describe, expect, it, vi } from "vitest";
import { MemoryBlockStore } from "../storage/index.js";
import { MinnowDatabase } from "./database.js";
import { pointReadTestHooks } from "./point-read.js";

async function seeded(
  options: { rowsPerBlock?: number; store?: MemoryBlockStore } = {},
): Promise<{ store: MemoryBlockStore; database: MinnowDatabase }> {
  const store = options.store ?? new MemoryBlockStore();
  const database = new MinnowDatabase(store, {
    autoCompact: false,
    ...(options.rowsPerBlock === undefined ? {} : { rowsPerBlock: options.rowsPerBlock }),
  });
  await database.execute(
    "CREATE TABLE customers (customer_id INTEGER PRIMARY KEY, name TEXT NOT NULL, tier TEXT NOT NULL)",
  );
  await database.execute(
    "CREATE TABLE orders (id INTEGER PRIMARY KEY, customer INTEGER NOT NULL, amount INTEGER NOT NULL, status TEXT NOT NULL)",
  );
  await database.execute(
    `INSERT INTO customers (customer_id, name, tier) VALUES ${Array.from(
      { length: 20 },
      (_, index) =>
        `(${String(index + 1)}, 'name-${String(index + 1)}', '${index % 2 === 0 ? "gold" : "silver"}')`,
    ).join(", ")}`,
  );
  await database.execute(
    `INSERT INTO orders (id, customer, amount, status) VALUES ${Array.from(
      { length: 200 },
      (_, index) =>
        `(${String(index + 1)}, ${String((index % 20) + 1)}, ${String((index * 7) % 100)}, '${index % 3 === 0 ? "paid" : "new"}')`,
    ).join(", ")}`,
  );
  return { store, database };
}

const rows = async (
  database: MinnowDatabase,
  sql: string,
  params: unknown[] = [],
): Promise<unknown[]> => (await database.query(sql, { params: params as never })).rows;

describe("cache invalidation", () => {
  it("serves the result memo only while nothing changed, across every kind of write", async () => {
    const { database } = await seeded();
    const sql =
      "SELECT status, COUNT(*) AS n, SUM(amount) AS total FROM orders GROUP BY status ORDER BY status";
    const initial = await rows(database, sql);
    const hitsBefore = database.bufferPoolStats().hits;
    expect(await rows(database, sql)).toEqual(initial);
    expect(database.bufferPoolStats().hits).toBeGreaterThan(hitsBefore);

    await database.execute(
      "INSERT INTO orders (id, customer, amount, status) VALUES (1001, 1, 50, 'paid')",
    );
    const afterInsert = await rows(database, sql);
    expect(afterInsert).not.toEqual(initial);
    expect(afterInsert).toEqual(await rows(database, sql, []));

    await database.execute("UPDATE orders SET status = 'refunded' WHERE id = 1001");
    expect(await rows(database, sql)).toContainEqual({ status: "refunded", n: 1, total: 50 });

    await database.execute("DELETE FROM orders WHERE id = 1001");
    expect(await rows(database, sql)).toEqual(initial);

    await database.execute(
      "INSERT INTO orders (id, customer, amount, status) VALUES (1002, 1, 1, 'paid') ON CONFLICT (id) DO NOTHING",
    );
    expect(await rows(database, sql)).not.toEqual(initial);
    await database.execute("DELETE FROM orders WHERE id = 1002");

    await database.execute("BEGIN");
    await database.execute(
      "INSERT INTO orders (id, customer, amount, status) VALUES (1003, 2, 9, 'paid')",
    );
    await database.execute("ROLLBACK");
    expect(await rows(database, sql)).toEqual(initial);
    await database.execute("BEGIN");
    await database.execute(
      "INSERT INTO orders (id, customer, amount, status) VALUES (1003, 2, 9, 'paid')",
    );
    await database.execute("COMMIT");
    expect(await rows(database, sql)).not.toEqual(initial);

    await database.compactTable("orders");
    const compacted = await rows(database, sql);
    expect(compacted).toEqual(await rows(database, sql));
    expect(compacted).not.toEqual(initial);
    await database.close();
  });

  it("never memoizes clock, random, or sequence reads", async () => {
    const { database } = await seeded();
    await database.execute("CREATE SEQUENCE ticket");
    const first = await rows(database, "SELECT NEXTVAL('ticket') AS n");
    const second = await rows(database, "SELECT NEXTVAL('ticket') AS n");
    expect(first).not.toEqual(second);
    const randoms = new Set<unknown>();
    for (let index = 0; index < 5; index += 1) {
      randoms.add(JSON.stringify(await rows(database, "SELECT RANDOM() AS r")));
    }
    expect(randoms.size).toBeGreaterThan(1);
    const clockA = (await rows(database, "SELECT CURRENT_TIMESTAMP AS t"))[0] as { t: Date };
    await new Promise((resolve) => setTimeout(resolve, 5));
    const clockB = (await rows(database, "SELECT CURRENT_TIMESTAMP AS t"))[0] as { t: Date };
    expect(clockB.t.getTime()).toBeGreaterThan(clockA.t.getTime());
    await database.close();
  });

  it("never serves a nested volatile block from the block result cache", async () => {
    const { database } = await seeded();
    for (const sql of [
      "SELECT (SELECT RANDOM()) AS r",
      "SELECT id, (SELECT RANDOM()) AS r FROM orders WHERE id <= 2",
      "SELECT r FROM (SELECT RANDOM() AS r) x",
      "WITH x AS (SELECT RANDOM() AS r) SELECT r FROM x",
      "SELECT COUNT(*) AS n FROM orders WHERE amount > (SELECT RANDOM() * 100)",
      "SELECT (SELECT GEN_RANDOM_UUID()) AS u",
    ]) {
      const seen = new Set<string>();
      for (let index = 0; index < 6; index += 1)
        seen.add(JSON.stringify(await rows(database, sql)));
      expect(seen.size, sql).toBeGreaterThan(1);
    }
    // One statement, one clock: the nested read equals the outer read, and a later statement
    // reads a later clock.
    const first = (
      await rows(database, "SELECT CURRENT_TIMESTAMP AS outer, (SELECT CURRENT_TIMESTAMP) AS inner")
    )[0] as {
      outer: Date;
      inner: Date;
    };
    expect(first.inner.getTime()).toBe(first.outer.getTime());
    await new Promise((resolve) => setTimeout(resolve, 5));
    const second = (
      await rows(database, "SELECT CURRENT_TIMESTAMP AS outer, (SELECT CURRENT_TIMESTAMP) AS inner")
    )[0] as {
      outer: Date;
      inner: Date;
    };
    expect(second.inner.getTime()).toBeGreaterThan(first.inner.getTime());
    expect(
      await rows(
        database,
        "SELECT id FROM orders WHERE id = 1 AND (SELECT CURRENT_DATE) IS NOT NULL",
      ),
    ).toEqual([{ id: 1 }]);
    await database.execute("CREATE SEQUENCE nested");
    await expect(rows(database, "SELECT (SELECT NEXTVAL('nested')) AS n")).rejects.toThrow(
      "NEXTVAL and CURRVAL are supported in the SELECT list of a SELECT without FROM",
    );
    await database.close();
  });

  it("keeps parameters and memoize:false apart from cached results", async () => {
    const { database } = await seeded();
    const byId = "SELECT amount FROM orders WHERE id = ?";
    expect(await rows(database, byId, [1])).toEqual([{ amount: 0 }]);
    expect(await rows(database, byId, [2])).toEqual([{ amount: 7 }]);
    expect(await rows(database, byId, ["2"])).toEqual([{ amount: 7 }]);
    expect(await rows(database, byId, [1])).toEqual([{ amount: 0 }]);
    await database.execute("UPDATE orders SET amount = 123 WHERE id = 2");
    expect(await rows(database, byId, [2])).toEqual([{ amount: 123 }]);
    expect((await database.query(byId, { params: [2], memoize: false })).rows).toEqual([
      { amount: 123 },
    ]);
    await database.close();
  });

  it("re-reads plans, columns, and catalog state after DDL", async () => {
    const { database } = await seeded();
    const star = "SELECT * FROM customers WHERE customer_id = 1";
    expect(await rows(database, star)).toEqual([{ customer_id: 1, name: "name-1", tier: "gold" }]);
    await database.execute("ALTER TABLE customers ADD COLUMN region TEXT DEFAULT 'west'");
    expect(await rows(database, star)).toEqual([
      { customer_id: 1, name: "name-1", tier: "gold", region: "west" },
    ]);
    await database.execute("ALTER TABLE customers DROP COLUMN tier");
    expect(await rows(database, star)).toEqual([
      { customer_id: 1, name: "name-1", region: "west" },
    ]);
    await database.execute("ALTER TABLE customers ADD COLUMN tier TEXT DEFAULT 'bronze'");
    expect(await rows(database, star)).toEqual([
      { customer_id: 1, name: "name-1", region: "west", tier: "bronze" },
    ]);
    // The pooled whole-segment vectors for the join side must follow the re-added column.
    const joined =
      "SELECT c.tier, COUNT(*) AS n FROM orders o JOIN customers c ON c.customer_id = o.customer GROUP BY c.tier ORDER BY c.tier";
    expect(await rows(database, joined)).toEqual([{ tier: "bronze", n: 200 }]);
    await database.execute("UPDATE customers SET tier = 'gold' WHERE customer_id <= 10");
    expect(await rows(database, joined)).toEqual([
      { tier: "bronze", n: 100 },
      { tier: "gold", n: 100 },
    ]);

    await database.execute("DROP TABLE customers");
    await database.execute(
      "CREATE TABLE customers (customer_id INTEGER PRIMARY KEY, label TEXT NOT NULL)",
    );
    await database.execute("INSERT INTO customers (customer_id, label) VALUES (1, 'fresh')");
    expect(await rows(database, star)).toEqual([{ customer_id: 1, label: "fresh" }]);

    await database.execute(
      "CREATE VIEW paid AS SELECT id, amount FROM orders WHERE status = 'paid'",
    );
    const viewSql = "SELECT COUNT(*) AS n FROM paid";
    expect(await rows(database, viewSql)).toEqual([{ n: 67 }]);
    await database.execute("DROP VIEW paid");
    await database.execute(
      "CREATE VIEW paid AS SELECT id, amount FROM orders WHERE status = 'new'",
    );
    expect(await rows(database, viewSql)).toEqual([{ n: 133 }]);
    await database.close();
  });

  it("keeps index-served lookups fresh through inserts, updates, deletes, and index DDL", async () => {
    const { store, database } = await seeded({ rowsPerBlock: 16 });
    await database.execute("CREATE INDEX orders_customer ON orders (customer)");
    const lookup = "SELECT id FROM orders WHERE customer = 7 ORDER BY id";
    const expected = (predicate: (id: number) => boolean): unknown[] =>
      Array.from({ length: 200 }, (_, index) => index + 1)
        .filter((id) => (id - 1) % 20 === 6 && predicate(id))
        .map((id) => ({ id }));
    const memoized = await rows(database, lookup);
    expect(memoized).toEqual(expected(() => true));
    const reads = vi.spyOn(store, "readFtsCandidates");
    // Repeats hit the pooled candidates and locators: no postings read at all.
    expect((await database.query(lookup, { memoize: false })).rows).toEqual(memoized);
    expect((await database.query(lookup, { memoize: false })).rows).toEqual(memoized);
    const postingReadsAfterRepeat = reads.mock.calls.length;
    expect(postingReadsAfterRepeat).toBeLessThanOrEqual(1);

    await database.execute(
      "INSERT INTO orders (id, customer, amount, status) VALUES (5007, 7, 1, 'new')",
    );
    expect(await rows(database, lookup)).toEqual([...expected(() => true), { id: 5007 }]);
    await database.execute("UPDATE orders SET customer = 8 WHERE id = 5007");
    expect(await rows(database, lookup)).toEqual(expected(() => true));
    await database.execute("UPDATE orders SET customer = 7 WHERE id = 1");
    expect(await rows(database, lookup)).toEqual([{ id: 1 }, ...expected(() => true)]);
    await database.execute("DELETE FROM orders WHERE id = 7");
    expect(await rows(database, lookup)).toEqual([{ id: 1 }, ...expected((id) => id !== 7)]);
    await database.compactTable("orders");
    expect(await rows(database, lookup)).toEqual([{ id: 1 }, ...expected((id) => id !== 7)]);
    await database.execute("DROP INDEX orders_customer");
    expect(await rows(database, lookup)).toEqual([{ id: 1 }, ...expected((id) => id !== 7)]);
    await database.execute("CREATE INDEX orders_customer ON orders (customer)");
    expect(await rows(database, lookup)).toEqual([{ id: 1 }, ...expected((id) => id !== 7)]);
    // The join side narrowed by the index follows the same history.
    const join =
      "SELECT o.id FROM customers c JOIN orders o ON o.customer = c.customer_id WHERE c.customer_id = 7 ORDER BY o.id";
    expect(await rows(database, join)).toEqual([{ id: 1 }, ...expected((id) => id !== 7)]);
    await database.close();
  });

  it("stays fresh across two engines sharing one store", async () => {
    const { store, database } = await seeded({ rowsPerBlock: 16 });
    const other = new MinnowDatabase(store, { autoCompact: false });
    const count = "SELECT COUNT(*) AS n FROM orders WHERE customer = 3";
    const point = "SELECT amount FROM orders WHERE id = 3";
    expect(await rows(database, count)).toEqual([{ n: 10 }]);
    expect(await rows(other, count)).toEqual([{ n: 10 }]);
    expect(await rows(other, point)).toEqual([{ amount: 14 }]);
    await other.execute(
      "INSERT INTO orders (id, customer, amount, status) VALUES (9001, 3, 1, 'new')",
    );
    await other.execute("UPDATE orders SET amount = 77 WHERE id = 3");
    expect(await rows(database, count)).toEqual([{ n: 11 }]);
    expect(await rows(database, point)).toEqual([{ amount: 77 }]);
    await database.execute("DELETE FROM orders WHERE id = 9001");
    await database.execute("ALTER TABLE orders ADD COLUMN note TEXT DEFAULT 'n'");
    expect(await rows(other, count)).toEqual([{ n: 10 }]);
    expect(await rows(other, "SELECT note FROM orders WHERE id = 3")).toEqual([{ note: "n" }]);
    await other.close();
    await database.close();
  });

  it("keeps point reads exact through deltas and compaction while serving from the fast path", async () => {
    const { database } = await seeded({ rowsPerBlock: 16 });
    const point = "SELECT id, amount, status FROM orders WHERE id = ?";
    const served = async (id: number): Promise<unknown[]> => {
      const before = pointReadTestHooks.served;
      const result = (await database.query(point, { params: [id], memoize: false })).rows;
      expect(pointReadTestHooks.served).toBe(before + 1);
      return result;
    };
    expect(await served(5)).toEqual([{ id: 5, amount: 28, status: "new" }]);
    await database.execute("UPDATE orders SET amount = 1 WHERE id = 5");
    expect(await served(5)).toEqual([{ id: 5, amount: 1, status: "new" }]);
    expect(await rows(database, point, [5])).toEqual([{ id: 5, amount: 1, status: "new" }]);
    await database.execute("DELETE FROM orders WHERE id = 5");
    expect(await served(5)).toEqual([]);
    expect(await rows(database, point, [5])).toEqual([]);
    await database.compactTable("orders");
    expect(await served(5)).toEqual([]);
    await database.execute(
      "INSERT INTO orders (id, customer, amount, status) VALUES (5, 1, 2, 'paid')",
    );
    expect(await served(5)).toEqual([{ id: 5, amount: 2, status: "paid" }]);
    expect(await rows(database, point, [5])).toEqual([{ id: 5, amount: 2, status: "paid" }]);
    await database.close();
  });
});
