import { IDBFactory } from "fake-indexeddb";
import {
  IndexedDbBlockStore,
  MemoryBlockStore,
  type BlockStore,
} from "@browserdatabase/storage-idb";
import { describe, expect, it } from "vitest";
import { BrowserDatabase, type DatabaseRow } from "./database.js";
import { QueryMemoryBudgetError } from "./memory.js";
import { compileQuery, createPreparedQuery, executeQuery, executeRowQuery } from "./query.js";

interface QueryStoreHarness {
  readonly store: BlockStore;
  reopen(): Promise<BlockStore>;
  close(): void;
}

function queryStoreImplementations(): Array<{
  name: string;
  create(): Promise<QueryStoreHarness>;
}> {
  return [
    {
      name: "memory",
      create: async () => {
        const store = new MemoryBlockStore();
        return { store, reopen: async () => store, close: () => undefined };
      },
    },
    {
      name: "fake IndexedDB reopen",
      create: async () => {
        const indexedDB = new IDBFactory();
        const name = crypto.randomUUID();
        let store = await IndexedDbBlockStore.open({ name, indexedDB });
        return {
          store,
          reopen: async () => {
            store.close();
            store = await IndexedDbBlockStore.open({ name, indexedDB });
            return store;
          },
          close: () => store.close(),
        };
      },
    },
  ];
}

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

  it("applies the execution memory budget through BrowserDatabase query preparation", async () => {
    const database = new BrowserDatabase(new MemoryBlockStore());
    await database.createTable({
      name: "events",
      columns: [{ name: "value", type: "number" }],
    });
    await database.insertBatch("events", { columns: { value: [1, 2, 3] } });
    const sql = "SELECT value FROM events WHERE value >= 2 ORDER BY value";
    const measured = await database.prepareQuery(sql);
    const retainedBytes = measured.memoryUsage.usedBytes;
    expect(measured.execute().rows).toEqual([{ value: 2 }, { value: 3 }]);
    const peakBytes = measured.memoryUsage.peakBytes;
    measured.close();

    await expect(
      database.prepareQuery(sql, { executionMemoryBudgetBytes: retainedBytes - 1 }),
    ).rejects.toThrow(QueryMemoryBudgetError);
    const below = await database.prepareQuery(sql, {
      executionMemoryBudgetBytes: peakBytes - 1,
    });
    expect(() => below.execute()).toThrow(QueryMemoryBudgetError);
    expect(below.memoryUsage.usedBytes).toBe(retainedBytes);
    below.close();

    const exact = await database.prepareQuery(sql, { executionMemoryBudgetBytes: peakBytes });
    expect(exact.execute().rows).toEqual([{ value: 2 }, { value: 3 }]);
    expect(exact.memoryUsage.peakBytes).toBe(peakBytes);
    exact.close();
  });

  it("keeps catalog-typed empty BrowserDatabase queries inside the memory model", async () => {
    const database = new BrowserDatabase(new MemoryBlockStore());
    await database.createTable({
      name: "empty_events",
      columns: [{ name: "value", type: "number" }],
    });
    const sql = "SELECT COUNT(*) AS count FROM empty_events";
    const exact = await database.prepareQuery(sql, { executionMemoryBudgetBytes: 49 });
    expect(exact.memoryUsage).toEqual({ budgetBytes: 49, usedBytes: 0, peakBytes: 0 });
    expect(exact.execute()).toEqual({ columns: ["count"], rows: [{ count: 0 }] });
    expect(exact.memoryUsage).toEqual({ budgetBytes: 49, usedBytes: 0, peakBytes: 49 });
    exact.close();

    const below = await database.prepareQuery(sql, { executionMemoryBudgetBytes: 48 });
    expect(() => below.execute()).toThrow(QueryMemoryBudgetError);
    expect(below.memoryUsage).toEqual({ budgetBytes: 48, usedBytes: 0, peakBytes: 32 });
    below.close();
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

  it("allows comment markers and statement delimiters inside string literals", () => {
    const plan = compileQuery("SELECT '--;/*' AS marker FROM events;");
    expect(executeQuery(plan, new Map([["events", [{ value: 1 }]]]))).toEqual({
      columns: ["marker"],
      rows: [{ marker: "--;/*" }],
    });
    expect(() => compileQuery("SELECT * FROM events -- comment")).toThrow(
      "comments are not supported",
    );
    expect(() => compileQuery("SELECT * FROM events /* comment */")).toThrow(
      "comments are not supported",
    );
  });

  it("executes an already materialized prepared plan repeatedly", () => {
    const plan = compileQuery("SELECT COUNT(*) AS count FROM rows");
    const prepared = createPreparedQuery(plan, new Map([["rows", [{ value: 1 }, { value: 2 }]]]));
    expect(prepared.execute()).toEqual({ columns: ["count"], rows: [{ count: 2 }] });
    expect(prepared.execute()).toEqual({ columns: ["count"], rows: [{ count: 2 }] });
  });

  it("isolates every prepared result from caller mutations", () => {
    const prepared = createPreparedQuery(
      compileQuery("SELECT id, happened FROM rows ORDER BY id"),
      new Map([
        [
          "rows",
          [
            { id: 1, happened: new Date("2025-01-01T00:00:00.000Z") },
            { id: 2, happened: new Date("2025-01-02T00:00:00.000Z") },
          ],
        ],
      ]),
    );
    const first = prepared.execute();
    first.columns[0] = "changed";
    const firstDate = first.rows[0]?.happened;
    if (!(firstDate instanceof Date)) throw new Error("Prepared datetime result is missing");
    firstDate.setUTCFullYear(2030);
    if (first.rows[0] !== undefined) first.rows[0].id = 99;
    first.rows.push({ id: 3, happened: null });

    expect(prepared.execute()).toEqual({
      columns: ["id", "happened"],
      rows: [
        { id: 1, happened: new Date("2025-01-01T00:00:00.000Z") },
        { id: 2, happened: new Date("2025-01-02T00:00:00.000Z") },
      ],
    });
    prepared.close();
    prepared.close();
    expect(() => prepared.execute()).toThrow("Prepared query is closed");
  });

  it("streams multi-batch joins directly into aggregate state", () => {
    const customers = Array.from({ length: 3_000 }, (_, index) => ({
      customer_id: index + 1,
      segment: ["business", "consumer", "public"][index % 3] ?? "business",
    }));
    const orders = Array.from({ length: 7_000 }, (_, index) => ({
      order_id: index + 1,
      customer_id: (index % customers.length) + 1,
      total: (index % 17) + 0.25,
    }));
    const sql =
      "SELECT c.segment, COUNT(*) AS order_count, SUM(o.total) AS revenue FROM customers c JOIN orders o ON o.customer_id = c.customer_id GROUP BY c.segment ORDER BY c.segment";
    const plan = compileQuery(sql);
    const input = new Map<string, DatabaseRow[]>([
      ["customers", customers],
      ["orders", orders],
    ]);

    expect(executeQuery(plan, input)).toEqual(executeRowQuery(plan, input));
  });

  it("deduplicates DISTINCT output through the grouped executor", () => {
    const rows = [
      { region: "west", tier: 1 },
      { region: "west", tier: 1 },
      { region: "east", tier: 1 },
      { region: "west", tier: 2 },
      { region: null, tier: 1 },
      { region: null, tier: 1 },
      { region: "east", tier: null },
    ];
    const input = new Map([["rows", rows]]);
    for (const sql of [
      "SELECT DISTINCT region FROM rows",
      "SELECT DISTINCT region, tier FROM rows ORDER BY region, tier LIMIT 4",
      "SELECT DISTINCT tier + 1 AS bumped FROM rows ORDER BY bumped",
    ]) {
      const plan = compileQuery(sql);
      const columnar = executeQuery(plan, input);
      expect(columnar).toEqual(executeRowQuery(plan, input));
    }
    const distinctPairs = executeQuery(
      compileQuery("SELECT DISTINCT region, tier FROM rows"),
      input,
    );
    expect(distinctPairs.rows).toHaveLength(5);
  });

  it("filters groups with HAVING in both executors", () => {
    const rows = [
      { category: "a", value: 1 },
      { category: "a", value: 2 },
      { category: "b", value: 10 },
      { category: "b", value: 20 },
      { category: "b", value: 30 },
      { category: "c", value: null },
    ];
    const input = new Map([["rows", rows]]);
    for (const sql of [
      "SELECT category, COUNT(*) AS count FROM rows GROUP BY category HAVING COUNT(*) > 1 ORDER BY category",
      "SELECT category, SUM(value) AS total FROM rows GROUP BY category HAVING SUM(value) >= 3 AND category != 'b' ORDER BY category",
      "SELECT category, AVG(value) AS mean FROM rows GROUP BY category HAVING MIN(value) > 1 ORDER BY category",
      "SELECT COUNT(*) AS count FROM rows HAVING COUNT(*) > 100",
      "SELECT COUNT(*) AS count FROM rows HAVING COUNT(*) > 1",
    ]) {
      const plan = compileQuery(sql);
      expect(executeQuery(plan, input)).toEqual(executeRowQuery(plan, input));
    }
    const filtered = executeQuery(
      compileQuery(
        "SELECT category, COUNT(*) AS count FROM rows GROUP BY category HAVING COUNT(*) > 1 ORDER BY category",
      ),
      input,
    );
    expect(filtered.rows).toEqual([
      { category: "a", count: 2 },
      { category: "b", count: 3 },
    ]);
  });

  it("executes derived tables and non-recursive CTEs in both executors", () => {
    const rows = [
      { region: "west", amount: 10 },
      { region: "west", amount: 20 },
      { region: "east", amount: 5 },
      { region: "east", amount: 40 },
      { region: null, amount: 7 },
    ];
    const input = new Map([["rows", rows]]);
    for (const sql of [
      "SELECT d.doubled FROM (SELECT amount * 2 AS doubled FROM rows) d WHERE d.doubled > 10 ORDER BY d.doubled",
      "SELECT t.region, t.total FROM (SELECT region, SUM(amount) AS total FROM rows GROUP BY region) t ORDER BY t.total DESC",
      "WITH totals AS (SELECT region, SUM(amount) AS total FROM rows GROUP BY region) SELECT COUNT(*) AS regions, MAX(total) AS peak FROM totals",
      "WITH big AS (SELECT amount FROM rows WHERE amount >= 10), bigger AS (SELECT amount FROM big WHERE amount >= 20) SELECT COUNT(*) AS count FROM bigger",
      "WITH west AS (SELECT region, amount FROM rows WHERE region = 'west') SELECT r.amount, w.amount AS west_amount FROM rows r JOIN west w ON w.amount = r.amount ORDER BY r.amount",
    ]) {
      const plan = compileQuery(sql);
      expect(executeQuery(plan, input)).toEqual(executeRowQuery(plan, input));
    }
    const nested = executeQuery(
      compileQuery(
        "WITH totals AS (SELECT region, SUM(amount) AS total FROM rows GROUP BY region) SELECT COUNT(*) AS regions, MAX(total) AS peak FROM totals",
      ),
      input,
    );
    expect(nested.rows).toEqual([{ regions: 3, peak: 45 }]);
  });

  it("evaluates IN lists and uncorrelated subqueries in both executors", () => {
    const rows = [
      { region: "west", amount: 10 },
      { region: "west", amount: 20 },
      { region: "east", amount: 5 },
      { region: "east", amount: 40 },
      { region: "north", amount: 15 },
      { region: null, amount: 7 },
    ];
    const input = new Map([["rows", rows]]);
    for (const sql of [
      "SELECT region, amount FROM rows WHERE region IN ('west', 'north') ORDER BY amount",
      "SELECT region, amount FROM rows WHERE amount NOT IN (5, 40) ORDER BY amount",
      "SELECT region, amount FROM rows WHERE amount IN (5, 10, 15, 100) ORDER BY amount",
      "SELECT region FROM rows WHERE amount > (SELECT AVG(amount) FROM rows) ORDER BY region",
      "SELECT region, amount FROM rows WHERE region IN (SELECT region FROM rows WHERE amount > 15) ORDER BY amount",
      "SELECT (SELECT MAX(amount) FROM rows) AS peak FROM rows LIMIT 1",
      "SELECT region FROM rows WHERE amount > (SELECT MIN(amount) FROM rows WHERE amount > 100) ORDER BY region",
    ]) {
      const plan = compileQuery(sql);
      expect(executeQuery(plan, input)).toEqual(executeRowQuery(plan, input));
    }

    const notInWithNull = executeQuery(
      compileQuery("SELECT amount FROM rows WHERE region NOT IN ('west', NULL)"),
      input,
    );
    expect(notInWithNull.rows).toEqual([]);
    const emptyScalar = executeQuery(
      compileQuery(
        "SELECT region FROM rows WHERE amount > (SELECT MAX(amount) FROM rows WHERE amount > 100)",
      ),
      input,
    );
    expect(emptyScalar.rows).toEqual([]);
  });

  it("computes ROW_NUMBER, RANK, and DENSE_RANK windows in both executors", () => {
    const rows = [
      { region: "west", amount: 10 },
      { region: "west", amount: 20 },
      { region: "west", amount: 20 },
      { region: "east", amount: 5 },
      { region: "east", amount: 40 },
      { region: null, amount: 7 },
    ];
    const input = new Map([["rows", rows]]);
    for (const sql of [
      "SELECT region, amount, ROW_NUMBER() OVER (PARTITION BY region ORDER BY amount DESC) AS rn FROM rows ORDER BY region, rn",
      "SELECT region, amount, RANK() OVER (PARTITION BY region ORDER BY amount) AS r, DENSE_RANK() OVER (PARTITION BY region ORDER BY amount) AS dr FROM rows ORDER BY region, amount",
      "SELECT amount, ROW_NUMBER() OVER (ORDER BY amount) AS rn FROM rows WHERE amount > 5 ORDER BY rn LIMIT 3",
      "SELECT region, ROW_NUMBER() OVER () AS rn FROM rows ORDER BY rn",
    ]) {
      const plan = compileQuery(sql);
      expect(executeQuery(plan, input)).toEqual(executeRowQuery(plan, input));
    }
    const ranked = executeQuery(
      compileQuery(
        "SELECT region, amount, RANK() OVER (PARTITION BY region ORDER BY amount) AS r, DENSE_RANK() OVER (PARTITION BY region ORDER BY amount) AS dr FROM rows WHERE region = 'west' ORDER BY amount",
      ),
      input,
    );
    expect(ranked.rows).toEqual([
      { region: "west", amount: 10, r: 1, dr: 1 },
      { region: "west", amount: 20, r: 2, dr: 2 },
      { region: "west", amount: 20, r: 2, dr: 2 },
    ]);
    expect(() => compileQuery("SELECT region FROM rows WHERE ROW_NUMBER() OVER () > 1")).toThrow(
      "Window functions are only allowed in the select list",
    );
    expect(() => compileQuery("SELECT ROW_NUMBER() OVER () + 1 AS rn FROM rows")).toThrow(
      "Window functions must be top-level select items",
    );
    expect(() =>
      compileQuery(
        "SELECT region, COUNT(*) AS c, ROW_NUMBER() OVER () AS rn FROM rows GROUP BY region",
      ),
    ).toThrow("Window functions cannot be combined with GROUP BY, DISTINCT, aggregates, or HAVING");
    const windowTotal = executeQuery(
      compileQuery("SELECT region, SUM(amount) OVER () AS s FROM rows ORDER BY region"),
      input,
    );
    expect(windowTotal.rows.every((row) => row.s === 102)).toBe(true);
  });

  it("combines UNION and UNION ALL members in both executors", () => {
    const west = [
      { region: "west", amount: 10 },
      { region: "west", amount: 10 },
      { region: "west", amount: 20 },
    ];
    const east = [
      { region: "east", amount: 10 },
      { region: "west", amount: 10 },
    ];
    const input = new Map([
      ["west", west],
      ["east", east],
    ]);
    for (const sql of [
      "SELECT region, amount FROM west UNION SELECT region, amount FROM east ORDER BY region, amount",
      "SELECT region, amount FROM west UNION ALL SELECT region, amount FROM east ORDER BY amount, region",
      "SELECT region, amount FROM west UNION SELECT region, amount FROM east UNION ALL SELECT region, amount FROM east ORDER BY region, amount LIMIT 4",
      "(SELECT region, amount FROM west ORDER BY amount DESC LIMIT 1) UNION (SELECT region, amount FROM east LIMIT 1) ORDER BY region",
      "SELECT amount FROM west WHERE amount IN (SELECT amount FROM east) UNION SELECT amount FROM east ORDER BY amount",
    ]) {
      const plan = compileQuery(sql);
      expect(executeQuery(plan, input)).toEqual(executeRowQuery(plan, input));
    }
    const distinctUnion = executeQuery(
      compileQuery(
        "SELECT region, amount FROM west UNION SELECT region, amount FROM east ORDER BY region, amount",
      ),
      input,
    );
    expect(distinctUnion.rows).toEqual([
      { region: "east", amount: 10 },
      { region: "west", amount: 10 },
      { region: "west", amount: 20 },
    ]);
    const allUnion = executeQuery(
      compileQuery("SELECT region, amount FROM west UNION ALL SELECT region, amount FROM east"),
      input,
    );
    expect(allUnion.rows).toHaveLength(5);
    expect(() =>
      compileQuery("SELECT region FROM west ORDER BY region UNION SELECT region FROM east"),
    ).toThrow("ORDER BY or LIMIT in a UNION member requires parentheses");
    expect(() =>
      executeRowQuery(
        compileQuery("SELECT region, amount FROM west UNION SELECT region FROM east"),
        input,
      ),
    ).toThrow("UNION members must select the same number of columns");
  });

  it("rejects unsupported subquery forms explicitly", () => {
    const rows = [{ region: "west", amount: 10 }];
    const input = new Map([["rows", rows]]);
    expect(() =>
      executeRowQuery(
        compileQuery("SELECT region FROM rows WHERE amount > (SELECT amount FROM rows)"),
        new Map([["rows", [...rows, { region: "east", amount: 4 }]]]),
      ),
    ).toThrow("A scalar subquery returned 2 rows");
    expect(() =>
      executeRowQuery(
        compileQuery("SELECT region FROM rows WHERE amount > (SELECT region, amount FROM rows)"),
        input,
      ),
    ).toThrow("A scalar subquery must select exactly one column");
    expect(() =>
      executeRowQuery(
        compileQuery(
          "SELECT region FROM rows r WHERE amount IN (SELECT amount FROM rows q WHERE q.amount = r.amount)",
        ),
        input,
      ),
    ).toThrow("Unknown table alias: r");
    expect(() => compileQuery("SELECT region FROM rows WHERE region IN (*)")).toThrow(
      "IN lists accept only scalar expressions",
    );
    const havingIn = compileQuery(
      "SELECT region, COUNT(*) AS count FROM rows GROUP BY region HAVING region IN ('west')",
    );
    expect(executeRowQuery(havingIn, input).rows).toEqual(
      executeQuery(havingIn, input).rows,
    );
  });

  it("rejects unsupported derived table and CTE forms explicitly", () => {
    expect(() => compileQuery("SELECT * FROM (SELECT value FROM rows)")).toThrow(
      "A derived table requires an alias",
    );
    expect(() => compileQuery("WITH r AS (SELECT value FROM r) SELECT value FROM r")).toThrow(
      "Recursive CTEs are not supported: r",
    );
    expect(() =>
      compileQuery(
        "WITH a AS (SELECT value FROM rows), a AS (SELECT value FROM rows) SELECT value FROM a",
      ),
    ).toThrow("Duplicate CTE name: a");
  });

  it("evaluates IS NULL, BETWEEN, and COUNT(DISTINCT) in both executors", () => {
    const rows = [
      { region: "west", amount: 10 },
      { region: "west", amount: 20 },
      { region: null, amount: 5 },
      { region: "east", amount: null },
      { region: "east", amount: 20 },
      { region: "west", amount: 20 },
    ];
    const input = new Map([["rows", rows]]);
    for (const sql of [
      "SELECT amount FROM rows WHERE region IS NULL",
      "SELECT region, amount FROM rows WHERE region IS NOT NULL AND amount IS NOT NULL ORDER BY region, amount",
      "SELECT region FROM rows WHERE amount BETWEEN 10 AND 20 ORDER BY region",
      "SELECT COUNT(DISTINCT region) AS regions FROM rows",
      "SELECT region, COUNT(DISTINCT amount) AS amounts FROM rows GROUP BY region ORDER BY region",
      "SELECT COUNT(DISTINCT amount) AS amounts FROM rows WHERE region IS NOT NULL",
      "SELECT region, COUNT(*) AS count FROM rows GROUP BY region HAVING MAX(amount) IS NOT NULL",
    ]) {
      const plan = compileQuery(sql);
      expect(executeQuery(plan, input), sql).toEqual(executeRowQuery(plan, input));
    }
    const distinctRegions = executeQuery(
      compileQuery("SELECT COUNT(DISTINCT region) AS regions FROM rows"),
      input,
    );
    expect(distinctRegions.rows).toEqual([{ regions: 2 }]);
    const grouped = executeQuery(
      compileQuery(
        "SELECT region, COUNT(DISTINCT amount) AS amounts FROM rows GROUP BY region ORDER BY region",
      ),
      input,
    );
    expect(grouped.rows).toEqual([
      { region: null, amounts: 1 },
      { region: "east", amounts: 1 },
      { region: "west", amounts: 2 },
    ]);
    const constants = executeQuery(
      compileQuery("SELECT 'total' AS tag, COUNT(*) AS count FROM rows"),
      input,
    );
    expect(constants.rows).toEqual([{ tag: "total", count: 6 }]);
    expect(() => compileQuery("SELECT SUM(DISTINCT amount) AS total FROM rows")).toThrow(
      "DISTINCT is only supported inside COUNT",
    );
    expect(() =>
      compileQuery("SELECT COUNT(DISTINCT region) AS r, SUM(amount) AS total FROM rows"),
    ).toThrow("COUNT(DISTINCT) cannot be combined with other aggregates yet");
    const notBetween = compileQuery("SELECT region FROM rows WHERE amount NOT BETWEEN 1 AND 5");
    expect(executeQuery(notBetween, input)).toEqual(executeRowQuery(notBetween, input));
    expect(executeQuery(notBetween, input).rows).toEqual([
      { region: "west" },
      { region: "west" },
      { region: "east" },
      { region: "west" },
    ]);
  });

  it("rejects unsupported DISTINCT and HAVING forms explicitly", () => {
    expect(() => compileQuery("SELECT DISTINCT * FROM rows")).toThrow(
      "SELECT DISTINCT * is not supported",
    );
    expect(() => compileQuery("SELECT DISTINCT COUNT(*) AS count FROM rows")).toThrow(
      "SELECT DISTINCT cannot be combined with aggregate functions",
    );
    expect(() => compileQuery("SELECT DISTINCT category FROM rows GROUP BY category")).toThrow(
      "SELECT DISTINCT cannot be combined with GROUP BY",
    );
    expect(() => compileQuery("SELECT DISTINCT category FROM rows HAVING COUNT(*) > 1")).toThrow(
      "SELECT DISTINCT cannot be combined with HAVING",
    );
    expect(() => compileQuery("SELECT value FROM rows HAVING value > 1")).toThrow(
      "HAVING requires GROUP BY or aggregate functions",
    );
    expect(() =>
      compileQuery(
        "SELECT category, COUNT(*) AS count FROM rows GROUP BY category HAVING value > 1",
      ),
    ).toThrow("HAVING conditions must use aggregates, literals, or GROUP BY expressions");
  });

  it("matches the row reference for empty and null aggregate groups", () => {
    const rows = [
      { category: null, value: null },
      { category: null, value: 2 },
      { category: "x", value: 4 },
    ];
    for (const sql of [
      "SELECT COUNT(*) AS count, SUM(value) AS sum, AVG(value) AS average FROM rows",
      "SELECT category, COUNT(value) AS count, MIN(value) AS minimum, MAX(value) AS maximum FROM rows GROUP BY category ORDER BY category",
    ]) {
      const plan = compileQuery(sql);
      const input = new Map([["rows", rows]]);
      expect(executeQuery(plan, input)).toEqual(executeRowQuery(plan, input));
    }
  });

  it("replays mutation segments into column vectors before execution", async () => {
    const database = new BrowserDatabase(new MemoryBlockStore(), { rowsPerBlock: 2 });
    await database.createTable({
      name: "accounts",
      uniqueKey: "account_id",
      columns: [
        { name: "account_id", type: "number" },
        { name: "kind", type: "string" },
        { name: "balance", type: "number" },
      ],
    });
    await database.insertBatch("accounts", {
      columns: {
        account_id: [1, 2, 3],
        kind: ["personal", "business", "personal"],
        balance: [10, 20, 30],
      },
    });
    await database.update("accounts", 2, { balance: 25 });
    await database.deleteBatch("accounts", { keys: [1] });
    await database.upsertBatch("accounts", {
      columns: {
        account_id: [3, 4],
        kind: ["business", "personal"],
        balance: [35, 40],
      },
    });

    expect(
      await database.query(
        "SELECT kind, COUNT(*) AS count, SUM(balance) AS total FROM accounts GROUP BY kind ORDER BY kind",
      ),
    ).toEqual({
      columns: ["kind", "count", "total"],
      rows: [
        { kind: "business", count: 2, total: 60 },
        { kind: "personal", count: 1, total: 40 },
      ],
    });
  });

  it("queries projected columns directly from compacted append-only segments", async () => {
    const database = new BrowserDatabase(new MemoryBlockStore(), { rowsPerBlock: 2 });
    await database.createTable({
      name: "events",
      uniqueKey: "event_id",
      columns: [
        { name: "event_id", type: "number" },
        { name: "category", type: "string" },
        { name: "value", type: "number" },
      ],
    });
    await database.insertBatch("events", {
      columns: { event_id: [1, 2], category: ["a", "b"], value: [10, 20] },
    });
    await database.insertBatch("events", {
      columns: { event_id: [3, 4], category: ["a", "b"], value: [30, 40] },
    });
    expect((await database.compactTable("events")).compacted).toBe(true);

    expect(
      await database.query(
        "SELECT category, COUNT(*) AS count, SUM(value) AS total FROM events GROUP BY category ORDER BY category",
      ),
    ).toEqual({
      columns: ["category", "count", "total"],
      rows: [
        { category: "a", count: 2, total: 40 },
        { category: "b", count: 2, total: 60 },
      ],
    });
  });

  for (const implementation of queryStoreImplementations()) {
    it(`preserves typed mutation queries across snapshots and compaction in ${implementation.name}`, async () => {
      const harness = await implementation.create();
      try {
        let database = new BrowserDatabase(harness.store, { compression: "raw", rowsPerBlock: 2 });
        await database.createTable({
          name: "accounts",
          uniqueKey: "account_id",
          columns: [
            { name: "account_id", type: "string" },
            { name: "kind", type: "string" },
            { name: "active", type: "boolean", nullable: true },
            { name: "balance", type: "number", nullable: true },
            { name: "opened", type: "datetime" },
          ],
        });
        const base = await database.insertBatch("accounts", {
          columns: {
            account_id: ["a", "b", "c"],
            kind: ["personal", "business", "personal"],
            active: [true, false, null],
            balance: [10, 20, null],
            opened: [
              new Date("2025-01-01T00:00:00.000Z"),
              new Date("2025-01-02T00:00:00.000Z"),
              new Date("2025-01-03T00:00:00.000Z"),
            ],
          },
        });
        await database.update("accounts", "b", { active: true, balance: 25 });
        await database.deleteBatch("accounts", { keys: ["a"] });
        await database.insert("accounts", {
          account_id: "a",
          kind: "returned",
          active: false,
          balance: 11,
          opened: new Date("2025-02-01T00:00:00.000Z"),
        });
        await database.upsertBatch("accounts", {
          columns: {
            account_id: ["c", "d"],
            kind: ["business", "personal"],
            active: [true, true],
            balance: [30, 40],
            opened: [new Date("2025-02-03T00:00:00.000Z"), new Date("2025-02-04T00:00:00.000Z")],
          },
        });

        const aggregateSql =
          "SELECT kind, active, COUNT(*) AS count, SUM(balance) AS total FROM accounts GROUP BY kind, active ORDER BY kind, active";
        const historical = {
          columns: ["kind", "active", "count", "total"],
          rows: [
            { kind: "business", active: false, count: 1, total: 20 },
            { kind: "personal", active: null, count: 1, total: null },
            { kind: "personal", active: true, count: 1, total: 10 },
          ],
        };
        const current = {
          columns: ["kind", "active", "count", "total"],
          rows: [
            { kind: "business", active: true, count: 2, total: 55 },
            { kind: "personal", active: true, count: 1, total: 40 },
            { kind: "returned", active: false, count: 1, total: 11 },
          ],
        };
        expect(await database.query(aggregateSql, { version: base.version })).toEqual(historical);
        expect(await database.query(aggregateSql)).toEqual(current);

        const compaction = await database.compactTable("accounts", { maxBlocksPerStep: 1 });
        expect(compaction.compacted).toBe(true);
        expect(await database.query(aggregateSql)).toEqual(current);
        expect(await database.query(aggregateSql, { version: base.version })).toEqual(historical);

        database = new BrowserDatabase(await harness.reopen());
        expect(await database.query(aggregateSql)).toEqual(current);
        expect(await database.query(aggregateSql, { version: base.version })).toEqual(historical);
        expect(
          await database.query("SELECT account_id, opened FROM accounts ORDER BY account_id"),
        ).toEqual({
          columns: ["account_id", "opened"],
          rows: [
            { account_id: "a", opened: new Date("2025-02-01T00:00:00.000Z") },
            { account_id: "b", opened: new Date("2025-01-02T00:00:00.000Z") },
            { account_id: "c", opened: new Date("2025-02-03T00:00:00.000Z") },
            { account_id: "d", opened: new Date("2025-02-04T00:00:00.000Z") },
          ],
        });
      } finally {
        harness.close();
      }
    });
  }
});
