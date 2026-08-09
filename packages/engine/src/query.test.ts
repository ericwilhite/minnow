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
