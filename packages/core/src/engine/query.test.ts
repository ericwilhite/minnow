import { IDBFactory } from "fake-indexeddb";
import { IndexedDbBlockStore, MemoryBlockStore, type BlockStore } from "../storage/index.js";
import { describe, expect, it } from "vitest";
import { MinnowDatabase, type DatabaseRow } from "./database.js";
import { SqlCompileError } from "./errors.js";
import { DEFAULT_QUERY_MEMORY_BUDGET_BYTES, QueryMemoryBudgetError } from "./memory.js";
import {
  compileQuery,
  compileStatement,
  createPreparedQuery,
  executeQuery,
  executeRowQuery,
  topLevelFtsMatchConjuncts,
  clonePlanTree,
  applyWindowFunctions,
  bindPlanParameters,
} from "./query.js";

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
    const database = new MinnowDatabase(new MemoryBlockStore(), { rowsPerBlock: 2 });
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
      columnDomains: [null, null],
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
      columnDomains: [null, null, null, null],
      rows: [
        { segment: "business", status: "pending", order_count: 1, revenue: 5 },
        { segment: "business", status: "paid", order_count: 2, revenue: 40.13 },
        { segment: "consumer", status: "paid", order_count: 1, revenue: 20 },
      ],
    });
  });

  it("expands SELECT DISTINCT * across executors and joins", async () => {
    const database = new MinnowDatabase(new MemoryBlockStore(), { rowsPerBlock: 2 });
    await database.createTable({
      name: "pairs",
      columns: [
        { name: "a", type: "number" },
        { name: "b", type: "string", nullable: true },
      ],
    });
    await database.insertBatch("pairs", {
      columns: { a: [1, 1, 2, 1, 2], b: ["x", "x", "y", "z", "y"] },
    });
    const single = await database.query("SELECT DISTINCT * FROM pairs ORDER BY a, b");
    expect(single).toEqual({
      columns: ["a", "b"],
      columnDomains: [null, null],
      rows: [
        { a: 1, b: "x" },
        { a: 1, b: "z" },
        { a: 2, b: "y" },
      ],
    });
    // NULLs group as equal under DISTINCT, and the row reference agrees with the engine.
    await database.insertBatch("pairs", { columns: { a: [3, 3], b: [null, null] } });
    const withNulls = await database.query("SELECT DISTINCT * FROM pairs WHERE a = 3");
    expect(withNulls.rows).toEqual([{ a: 3, b: null }]);
    const reference = executeQuery(
      compileQuery("SELECT DISTINCT * FROM pairs WHERE a = 3"),
      new Map([
        [
          "pairs",
          [
            { a: 3, b: null },
            { a: 3, b: null },
          ] as DatabaseRow[],
        ],
      ]),
    );
    expect(reference.rows).toEqual([{ a: 3, b: null }]);

    // Joined wildcard outputs stay alias-qualified, exactly like plain SELECT *.
    await database.createTable({
      name: "tags",
      columns: [
        { name: "a", type: "number" },
        { name: "tag", type: "string" },
      ],
    });
    await database.insertBatch("tags", { columns: { a: [1, 1], tag: ["t", "t"] } });
    const joined = await database.query(
      "SELECT DISTINCT * FROM pairs p JOIN tags t ON t.a = p.a WHERE p.b = 'x'",
    );
    expect(joined).toEqual({
      columns: ["p.a", "p.b", "t.a", "t.tag"],
      columnDomains: [null, null, null, null],
      rows: [{ "p.a": 1, "p.b": "x", "t.a": 1, "t.tag": "t" }],
    });
  });

  it("pins a snapshot scope while fresh queries observe new commits", async () => {
    const database = new MinnowDatabase(new MemoryBlockStore());
    await database.createTable({
      name: "events",
      columns: [{ name: "value", type: "number" }],
    });
    await database.insertBatch("events", { columns: { value: [1, 2] } });
    const observed = await database.snapshot(async (session) => {
      const before = await session.query("SELECT COUNT(*) AS count FROM events");
      await database.insertBatch("events", { columns: { value: [3] } });
      const still = await session.query("SELECT COUNT(*) AS count FROM events");
      const fresh = await database.query("SELECT COUNT(*) AS count FROM events");
      return { before: before.rows, still: still.rows, fresh: fresh.rows };
    });
    expect(observed.before).toEqual([{ count: 2 }]);
    expect(observed.still).toEqual([{ count: 2 }]);
    expect(observed.fresh).toEqual([{ count: 3 }]);
  });

  it("applies the execution memory budget through MinnowDatabase query preparation", async () => {
    const database = new MinnowDatabase(new MemoryBlockStore());
    await database.createTable({
      name: "events",
      columns: [{ name: "value", type: "number" }],
    });
    await database.insertBatch("events", { columns: { value: [1, 2, 3] } });
    const sql = "SELECT value FROM events WHERE value >= 2 ORDER BY value";
    // A budget too small for anything must reject with spill disabled, and a generous budget
    // must execute identically; the modeled-byte precision itself is covered at the executor
    // level in vector.test.ts.
    await expect(
      database.query(sql, { executionMemoryBudgetBytes: 8, spillToStorage: false }),
    ).rejects.toThrow(QueryMemoryBudgetError);
    const generous = await database.query(sql, {
      executionMemoryBudgetBytes: 1_000_000,
      spillToStorage: false,
    });
    expect(generous.rows).toEqual([{ value: 2 }, { value: 3 }]);
  });

  it("bounds low-level query preparation and pre-resolution subqueries by default", () => {
    const tables = new Map([["rows", [{ value: 1 }, { value: 2 }, { value: 3 }] as DatabaseRow[]]]);
    const prepared = createPreparedQuery(compileQuery("SELECT value FROM rows"), tables);
    expect(prepared.memoryUsage.budgetBytes).toBe(DEFAULT_QUERY_MEMORY_BUDGET_BYTES);
    prepared.close();

    expect(() =>
      createPreparedQuery(
        compileQuery(
          "SELECT value FROM rows WHERE value IN (SELECT value FROM rows) ORDER BY value",
        ),
        tables,
        { executionMemoryBudgetBytes: 20 },
      ),
    ).toThrow(QueryMemoryBudgetError);
  });

  it("keeps catalog-typed empty MinnowDatabase queries inside the memory model", async () => {
    const database = new MinnowDatabase(new MemoryBlockStore());
    await database.createTable({
      name: "empty_events",
      columns: [{ name: "value", type: "number" }],
    });
    const sql = "SELECT COUNT(*) AS count FROM empty_events";
    const exact = await database.query(sql, {
      executionMemoryBudgetBytes: 49,
      spillToStorage: false,
    });
    expect(exact).toEqual({ columns: ["count"], columnDomains: [null], rows: [{ count: 0 }] });
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

  it("keeps a derived ORDER BY column that only a differently-aliased item projects", () => {
    // Projection pruning must not drop `b AS y`: the derived block's ORDER BY names the
    // source column, not the alias, and the ordering decides which rows survive the LIMIT.
    const plan = compileQuery("SELECT x FROM (SELECT x AS x, b AS y FROM t ORDER BY b LIMIT 2) d");
    const input = new Map([
      [
        "t",
        [
          { x: "third", b: 3 },
          { x: "first", b: 1 },
          { x: "fourth", b: 4 },
          { x: "second", b: 2 },
        ],
      ],
    ]);
    expect(executeQuery(plan, input).rows).toEqual([{ x: "first" }, { x: "second" }]);
    expect(executeQuery(plan, input)).toEqual(executeRowQuery(plan, input));
  });

  it("keeps ordinary row-table text out of the internal SQL-domain namespace", () => {
    const values = [
      "\0minnow-domain:numeric:10",
      "\0minnow-domain:interval:not-json",
      "\0minnow-domain:text:\0minnow-domain:uuid:not-a-uuid",
    ];
    // The unrelated empty table selects the row executor, whose public input boundary must have
    // the same escaping semantics as the columnar executor and MinnowDatabase storage scans.
    const input = new Map<string, DatabaseRow[]>([
      ["t", values.map((value, index) => ({ id: index + 1, value }))],
      ["empty", []],
    ]);
    const plan = compileQuery("SELECT value FROM t ORDER BY id");
    const expected = {
      columns: ["value"],
      columnDomains: [null],
      rows: values.map((value) => ({ value })),
    };
    expect(executeQuery(plan, input)).toEqual(expected);
    expect(executeRowQuery(plan, input)).toEqual(expected);
  });

  it("orders a wildcard select by a qualified column reference", async () => {
    // A wildcard select names its outputs after the source columns, so a qualified ORDER BY
    // reference has to be rewritten into that naming. Looking it up verbatim used to match no
    // output column at all, leaving the rows in insertion order with no error raised.
    const database = new MinnowDatabase(new MemoryBlockStore(), { rowsPerBlock: 4 });
    await database.createTable({
      name: "people",
      uniqueKey: "id",
      columns: [
        { name: "id", type: "number" },
        { name: "name", type: "string" },
      ],
    });
    for (let id = 0; id < 10; id += 1) {
      await database.insert("people", { id, name: `p${String(id % 3)}` });
    }
    const sorted = ["p0/0", "p0/3", "p0/6", "p0/9", "p1/1", "p1/4", "p1/7", "p2/2", "p2/5", "p2/8"];
    for (const sql of [
      "SELECT * FROM people ORDER BY name, id LIMIT 10",
      "SELECT * FROM people ORDER BY people.name, people.id LIMIT 10",
      "SELECT * FROM people p ORDER BY p.name, p.id LIMIT 10",
    ]) {
      const result = await database.query(sql);
      expect(result.rows.map((row) => `${String(row.name)}/${String(row.id)}`)).toEqual(sorted);
    }

    // Several sources prefix the outputs instead, so a bare reference resolves the other way.
    await database.createTable({
      name: "pets",
      uniqueKey: "pet_id",
      columns: [
        { name: "pet_id", type: "number" },
        { name: "owner", type: "number" },
        { name: "pet_name", type: "string" },
      ],
    });
    for (let petId = 0; petId < 5; petId += 1) {
      await database.insert("pets", {
        pet_id: petId,
        owner: 9 - petId,
        pet_name: `pet${String(petId)}`,
      });
    }
    const petNames = ["pet0", "pet1", "pet2", "pet3", "pet4"];
    for (const sql of [
      "SELECT * FROM people JOIN pets ON people.id = pets.owner ORDER BY pet_name",
      "SELECT * FROM people JOIN pets ON people.id = pets.owner ORDER BY pets.pet_name",
      "SELECT * FROM people o JOIN pets t ON o.id = t.owner ORDER BY t.pet_name",
    ]) {
      const result = await database.query(sql);
      expect(result.rows.map((row) => row["pets.pet_name"] ?? row["t.pet_name"])).toEqual(petNames);
    }

    // A qualified reference that resolves to nothing is rejected, never dropped.
    await expect(database.query("SELECT * FROM people ORDER BY people.nope")).rejects.toThrow(
      "Unknown column: people.nope",
    );
    await expect(database.query("SELECT * FROM people p ORDER BY people.name")).rejects.toThrow(
      "Unknown table alias: people",
    );

    // Both executors resolve the same way, including for a schema-less row table.
    const plan = compileQuery("SELECT * FROM t ORDER BY t.x DESC");
    const input = new Map([
      [
        "t",
        [
          { x: 1, y: "a" },
          { x: 3, y: "b" },
          { x: 2, y: "c" },
        ],
      ],
    ]);
    expect(executeQuery(plan, input).rows).toEqual([
      { x: 3, y: "b" },
      { x: 2, y: "c" },
      { x: 1, y: "a" },
    ]);
    expect(executeRowQuery(plan, input)).toEqual(executeQuery(plan, input));
    expect(() => executeRowQuery(compileQuery("SELECT * FROM t ORDER BY u.x"), input)).toThrow(
      "ORDER BY requires a selected column or output alias: u.x",
    );
  });

  it("rejects a grouped full-text select item missing from GROUP BY", () => {
    // MATCH(*) carries no column children before expansion, but it is never constant across
    // a group; both executors share this rejection through validateGrouping.
    const input = new Map([["t", [{ x: "quick fox" }, { x: "lazy dog" }]]]);
    for (const sql of [
      "SELECT MATCH(*) AGAINST 'quick' AS m, COUNT(*) AS c FROM t",
      "SELECT MATCH(x) AGAINST 'quick' AS m, COUNT(*) AS c FROM t",
    ]) {
      const plan = compileQuery(sql);
      expect(() => executeQuery(plan, input)).toThrow("must appear in GROUP BY");
      expect(() => executeRowQuery(plan, input)).toThrow("must appear in GROUP BY");
    }
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
    const database = new MinnowDatabase(new MemoryBlockStore());
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
      columnDomains: [null],
      rows: [{ marker: "--;/*" }],
    });
    // The same markers outside a literal are comments (E161, T351), and skipping one leaves the
    // statement exactly as it would read without it.
    const commented = new Map([["events", [{ value: 1 }]]]);
    expect(executeQuery(compileQuery("SELECT value FROM events -- comment"), commented)).toEqual(
      executeQuery(compileQuery("SELECT value FROM events"), commented),
    );
    expect(executeQuery(compileQuery("SELECT /* comment */ value FROM events"), commented)).toEqual(
      executeQuery(compileQuery("SELECT value FROM events"), commented),
    );
    expect(() => compileQuery("SELECT value FROM events /* unterminated")).toThrow(
      "Unterminated comment",
    );
  });

  it("executes an already materialized prepared plan repeatedly", () => {
    const plan = compileQuery("SELECT COUNT(*) AS count FROM rows");
    const prepared = createPreparedQuery(plan, new Map([["rows", [{ value: 1 }, { value: 2 }]]]));
    expect(prepared.execute()).toEqual({
      columns: ["count"],
      columnDomains: [null],
      rows: [{ count: 2 }],
    });
    expect(prepared.execute()).toEqual({
      columns: ["count"],
      columnDomains: [null],
      rows: [{ count: 2 }],
    });
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
      columnDomains: [null, null],
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
    // A window is an expression, so it composes like one: the arithmetic around it happens after
    // the window has run, over the column it produced.
    const composed = compileQuery(
      `SELECT region,
              amount,
              ROW_NUMBER() OVER (ORDER BY amount, region) + 1 AS rn,
              amount - LAG(amount) OVER (ORDER BY amount, region) AS change,
              ROUND(100.0 * amount / SUM(amount) OVER (), 1) AS pct
       FROM rows ORDER BY amount, region`,
    );
    expect(executeQuery(composed, input).rows).toEqual([
      { region: "east", amount: 5, rn: 2, change: null, pct: 4.9 },
      { region: null, amount: 7, rn: 3, change: 2, pct: 6.9 },
      { region: "west", amount: 10, rn: 4, change: 3, pct: 9.8 },
      { region: "west", amount: 20, rn: 5, change: 10, pct: 19.6 },
      { region: "west", amount: 20, rn: 6, change: 0, pct: 19.6 },
      { region: "east", amount: 40, rn: 7, change: 20, pct: 39.2 },
    ]);
    expect(executeQuery(composed, input)).toEqual(executeRowQuery(composed, input));
    // A window over a grouped block ranks the groups: SQL runs it after GROUP BY and HAVING, so
    // its ORDER BY reads the aggregates.
    const rankedGroups = compileQuery(
      `SELECT region, COUNT(*) AS c, SUM(amount) AS total,
              ROW_NUMBER() OVER (ORDER BY SUM(amount) DESC) AS rn,
              SUM(SUM(amount)) OVER () AS everything
       FROM rows GROUP BY region HAVING COUNT(*) > 1 ORDER BY rn`,
    );
    expect(executeQuery(rankedGroups, input).rows).toEqual([
      { region: "west", c: 3, total: 50, rn: 1, everything: 95 },
      { region: "east", c: 2, total: 45, rn: 2, everything: 95 },
    ]);
    expect(executeQuery(rankedGroups, input)).toEqual(executeRowQuery(rankedGroups, input));
    // What is still refused is a window reading a column the grouping threw away.
    expect(() =>
      compileQuery(
        "SELECT region, COUNT(*) AS c, ROW_NUMBER() OVER (ORDER BY amount) AS rn FROM rows GROUP BY region",
      ),
    ).toThrow("OVER(...) must use aggregates or GROUP BY expressions");
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

    const signedZero = compileQuery("SELECT -0 AS value UNION SELECT 0 AS value");
    expect(executeRowQuery(signedZero, new Map()).rows).toHaveLength(1);
    expect(executeQuery(signedZero, new Map()).rows).toHaveLength(1);
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
    // Correlated predicates decorrelate inside larger boolean expressions too.
    const nestedCorrelation = compileQuery(
      "SELECT region FROM rows r WHERE amount < 0 OR amount IN " +
        "(SELECT amount FROM rows q WHERE q.amount = r.amount)",
    );
    expect(executeRowQuery(nestedCorrelation, input).rows).toEqual([{ region: "west" }]);
    expect(executeQuery(nestedCorrelation, input).rows).toEqual([{ region: "west" }]);
    expect(() => compileQuery("SELECT region FROM rows WHERE region IN (*)")).toThrow(
      "IN lists accept only scalar expressions",
    );
    const havingIn = compileQuery(
      "SELECT region, COUNT(*) AS count FROM rows GROUP BY region HAVING region IN ('west')",
    );
    expect(executeRowQuery(havingIn, input).rows).toEqual(executeQuery(havingIn, input).rows);
  });

  it("rejects unsupported derived table and CTE forms explicitly", () => {
    expect(() => compileQuery("SELECT * FROM (SELECT value FROM rows)")).toThrow(
      "A derived table requires an alias",
    );
    expect(() => compileQuery("WITH r AS (SELECT value FROM r) SELECT value FROM r")).toThrow(
      "Recursive CTE references require WITH RECURSIVE: r",
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
      { region: "east", amounts: 1 },
      { region: "west", amounts: 2 },
      { region: null, amounts: 1 },
    ]);
    const constants = executeQuery(
      compileQuery("SELECT 'total' AS tag, COUNT(*) AS count FROM rows"),
      input,
    );
    expect(constants.rows).toEqual([{ tag: "total", count: 6 }]);
    expect(() => compileQuery("SELECT UPPER(DISTINCT region) AS r FROM rows")).toThrow(
      "DISTINCT is only supported inside aggregate functions",
    );
    // A DISTINCT aggregate sits beside ordinary ones, several to a select, and inside arithmetic:
    // each carries its own set of seen values rather than the select carrying one.
    const mixed = compileQuery(
      `SELECT COUNT(DISTINCT region) AS regions,
              COUNT(DISTINCT amount) AS amounts,
              SUM(amount) AS total,
              COUNT(*) AS rows_seen,
              SUM(amount) / COUNT(DISTINCT region) AS per_region
       FROM rows`,
    );
    expect(executeQuery(mixed, input).rows).toEqual([
      { regions: 2, amounts: 3, total: 75, rows_seen: 6, per_region: 37.5 },
    ]);
    expect(executeQuery(mixed, input)).toEqual(executeRowQuery(mixed, input));
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
    // DISTINCT * compiles; its wildcard expands against input schemas at execution.
    expect(compileQuery("SELECT DISTINCT * FROM rows").distinctWildcard).toBe(true);
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
    const database = new MinnowDatabase(new MemoryBlockStore(), { rowsPerBlock: 2 });
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
      columnDomains: [null, null, null],
      rows: [
        { kind: "business", count: 2, total: 60 },
        { kind: "personal", count: 1, total: 40 },
      ],
    });
  });

  it("queries projected columns directly from compacted append-only segments", async () => {
    const database = new MinnowDatabase(new MemoryBlockStore(), { rowsPerBlock: 2 });
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
      columnDomains: [null, null, null],
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
        let database = new MinnowDatabase(harness.store, { compression: "raw", rowsPerBlock: 2 });
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
          columnDomains: [null, null, null, null],
          rows: [
            { kind: "business", active: false, count: 1, total: 20 },
            { kind: "personal", active: true, count: 1, total: 10 },
            { kind: "personal", active: null, count: 1, total: null },
          ],
        };
        const current = {
          columns: ["kind", "active", "count", "total"],
          columnDomains: [null, null, null, null],
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

        database = new MinnowDatabase(await harness.reopen());
        expect(await database.query(aggregateSql)).toEqual(current);
        expect(await database.query(aggregateSql, { version: base.version })).toEqual(historical);
        expect(
          await database.query("SELECT account_id, opened FROM accounts ORDER BY account_id"),
        ).toEqual({
          columns: ["account_id", "opened"],
          columnDomains: [null, null],
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

  it("evaluates OR, NOT, LIKE, and CASE with SQL null semantics in both executors", () => {
    const rows = [
      { region: "west", amount: 10, active: true },
      { region: "west coast", amount: null, active: false },
      { region: null, amount: 5, active: null },
      { region: "east", amount: -2, active: true },
      { region: "least", amount: 7, active: false },
    ];
    const input = new Map([["rows", rows]]);
    for (const sql of [
      "SELECT region FROM rows WHERE amount > 6 OR region = 'east'",
      "SELECT region FROM rows WHERE NOT amount > 6",
      "SELECT region FROM rows WHERE NOT (region = 'west' OR amount < 0)",
      "SELECT region FROM rows WHERE region LIKE 'w%'",
      "SELECT region FROM rows WHERE region LIKE '%east%' OR region NOT LIKE '_est%'",
      "SELECT region FROM rows WHERE region LIKE '%st' AND (active OR amount > 4)",
      "SELECT region FROM rows WHERE amount NOT BETWEEN 0 AND 6",
      "SELECT region, CASE WHEN amount > 6 THEN 'big' WHEN amount > 0 THEN 'small' ELSE 'other' END AS size FROM rows",
      "SELECT region, CASE region WHEN 'west' THEN 1 WHEN 'east' THEN 2 END AS code FROM rows",
      "SELECT region, amount > 5 AS high FROM rows",
      "SELECT region, COUNT(*) AS count FROM rows GROUP BY region HAVING COUNT(*) > 1 OR MAX(amount) >= 7",
    ]) {
      const plan = compileQuery(sql);
      const raw = compileQuery(sql, { optimize: false });
      expect(executeQuery(plan, input), sql).toEqual(executeRowQuery(raw, input));
    }
    // NULL region: LIKE is unknown, so the row filters out; NOT does not resurrect it.
    const notLike = executeQuery(
      compileQuery("SELECT amount FROM rows WHERE region NOT LIKE 'w%'"),
      input,
    );
    expect(notLike.rows).toEqual([{ amount: -2 }, { amount: 7 }]);
    const orNull = executeQuery(
      compileQuery("SELECT region FROM rows WHERE active OR amount > 4"),
      input,
    );
    expect(orNull.rows).toEqual([
      { region: "west" },
      { region: null },
      { region: "east" },
      { region: "least" },
    ]);

    const unicodeLike = compileQuery("SELECT '😀' LIKE '_' AS one, '😀' LIKE '__' AS two");
    const unicodeResult = {
      columns: ["one", "two"],
      columnDomains: [null, null],
      rows: [{ one: true, two: false }],
    };
    expect(executeRowQuery(unicodeLike, new Map())).toEqual(unicodeResult);
    expect(executeQuery(unicodeLike, new Map())).toEqual(unicodeResult);
  });

  it("evaluates EXISTS, set operations, and OFFSET in both executors", () => {
    const rows = [
      { region: "west", amount: 4 },
      { region: "east", amount: 2 },
      { region: "east", amount: 2 },
      { region: "north", amount: 9 },
    ];
    const dims = [{ region: "east" }, { region: "south" }];
    const input = new Map<string, DatabaseRow[]>([
      ["rows", rows],
      ["dims", dims],
    ]);
    for (const sql of [
      "SELECT region FROM rows WHERE EXISTS (SELECT region FROM dims WHERE region = 'east')",
      "SELECT region FROM rows WHERE NOT EXISTS (SELECT region FROM dims WHERE region = 'missing')",
      "SELECT region FROM rows INTERSECT SELECT region FROM dims",
      "SELECT region FROM rows EXCEPT SELECT region FROM dims",
      "SELECT region FROM rows EXCEPT SELECT region FROM dims UNION SELECT region FROM dims INTERSECT SELECT region FROM rows",
      "SELECT region, amount FROM rows ORDER BY amount, region LIMIT 2 OFFSET 1",
    ]) {
      const plan = compileQuery(sql);
      const raw = compileQuery(sql, { optimize: false });
      expect(executeQuery(plan, input), sql).toEqual(executeRowQuery(raw, input));
    }
    const intersect = executeQuery(
      compileQuery("SELECT region FROM rows INTERSECT SELECT region FROM dims"),
      input,
    );
    expect(intersect.rows).toEqual([{ region: "east" }]);
    const except = executeQuery(
      compileQuery("SELECT region FROM rows EXCEPT SELECT region FROM dims"),
      input,
    );
    expect(except.rows).toEqual([{ region: "west" }, { region: "north" }]);
    const offset = executeQuery(
      compileQuery("SELECT amount FROM rows ORDER BY amount DESC LIMIT 2 OFFSET 1"),
      input,
    );
    expect(offset.rows).toEqual([{ amount: 4 }, { amount: 2 }]);
  });

  it("computes aggregate windows, general joins, and recursive CTEs in both executors", () => {
    const rows = [
      { region: "west", amount: 10 },
      { region: "west", amount: 20 },
      { region: "west", amount: 20 },
      { region: "east", amount: 5 },
      { region: "east", amount: null },
    ];
    const dims = [
      { region: "west", weight: 3 },
      { region: "east", weight: 12 },
    ];
    const input = new Map<string, DatabaseRow[]>([
      ["rows", rows],
      ["dims", dims],
    ]);
    for (const sql of [
      "SELECT region, amount, SUM(amount) OVER (PARTITION BY region) AS total FROM rows ORDER BY region, amount",
      "SELECT region, amount, SUM(amount) OVER (PARTITION BY region ORDER BY amount) AS running FROM rows ORDER BY region, amount",
      "SELECT region, COUNT(*) OVER (PARTITION BY region) AS size, MIN(amount) OVER (PARTITION BY region) AS low FROM rows ORDER BY region",
      "SELECT r.region, d.weight FROM rows r JOIN dims d ON d.weight > r.amount",
      "SELECT r.region, r.amount, d.weight FROM rows r LEFT JOIN dims d ON d.region = r.region AND d.weight > r.amount ORDER BY region, amount",
      "SELECT r.region, d.weight FROM rows r RIGHT JOIN dims d ON d.region = r.region ORDER BY weight",
      "WITH RECURSIVE n AS (SELECT MIN(amount) AS v FROM rows UNION ALL SELECT v + 5 FROM n WHERE v < 20) SELECT v FROM n",
      "WITH RECURSIVE n AS (SELECT region FROM dims UNION SELECT region FROM rows WHERE region IN (SELECT region FROM n)) SELECT region FROM n",
    ]) {
      const plan = compileQuery(sql);
      const raw = compileQuery(sql, { optimize: false });
      expect(executeQuery(plan, input), sql).toEqual(executeRowQuery(raw, input));
    }
    const running = executeQuery(
      compileQuery(
        "SELECT region, amount, SUM(amount) OVER (PARTITION BY region ORDER BY amount) AS running FROM rows WHERE region = 'west' ORDER BY amount",
      ),
      input,
    );
    expect(running.rows).toEqual([
      { region: "west", amount: 10, running: 10 },
      { region: "west", amount: 20, running: 50 },
      { region: "west", amount: 20, running: 50 },
    ]);
    const counts = executeQuery(
      compileQuery(
        "WITH RECURSIVE n AS (SELECT MIN(amount) AS v FROM rows UNION ALL SELECT v * 2 FROM n WHERE v < 30) SELECT v FROM n ORDER BY v",
      ),
      input,
    );
    expect(counts.rows).toEqual([{ v: 5 }, { v: 10 }, { v: 20 }, { v: 40 }]);
    expect(() =>
      executeRowQuery(
        compileQuery(
          "WITH RECURSIVE n AS (SELECT MIN(amount) AS v FROM rows UNION ALL SELECT v FROM n) SELECT v FROM n",
        ),
        input,
      ),
    ).toThrow("Recursive CTE exceeded");
  });
});

describe("top-level full-text conjunct extraction", () => {
  // Pins the pairing between splitCondition's predicate shape and the extraction that index
  // pruning relies on: if a parser or optimizer change rewraps a bare MATCH conjunct, this
  // fails loudly instead of pruning silently turning off.
  it("extracts exactly the conjuncts every result row must satisfy", () => {
    const single = compileQuery(
      "SELECT a FROM rows WHERE MATCH(a, b) AGAINST 'quick fox' AND n > 1",
    );
    expect(topLevelFtsMatchConjuncts(single)).toEqual([
      {
        columns: [
          { kind: "column", reference: "a" },
          { kind: "column", reference: "b" },
        ],
        query: "quick fox",
      },
    ]);
    const two = compileQuery(
      "SELECT a FROM rows WHERE MATCH(a) AGAINST 'x' AND MATCH(b) AGAINST 'y'",
    );
    expect(topLevelFtsMatchConjuncts(two)).toHaveLength(2);
    // Negated, OR-wrapped, and unexpanded-star matches are not guaranteed by every row.
    expect(
      topLevelFtsMatchConjuncts(
        compileQuery("SELECT a FROM rows WHERE NOT (MATCH(a) AGAINST 'x')"),
      ),
    ).toEqual([]);
    expect(
      topLevelFtsMatchConjuncts(
        compileQuery("SELECT a FROM rows WHERE MATCH(a) AGAINST 'x' OR n > 1"),
      ),
    ).toEqual([]);
    expect(
      topLevelFtsMatchConjuncts(compileQuery("SELECT a FROM rows WHERE MATCH(*) AGAINST 'x'")),
    ).toEqual([]);
    // The optimizer must preserve the conjunct through its rewrites.
    const optimized = compileQuery(
      "SELECT a FROM rows WHERE MATCH(a) AGAINST 'x' AND 1 + 1 = 2 AND n >= 0",
    );
    expect(topLevelFtsMatchConjuncts(optimized)).toHaveLength(1);
  });
});

describe("scalar functions", () => {
  const rows: DatabaseRow[] = [
    { region: "west", amount: 10, joined: new Date("2026-02-14T13:45:30.500Z") },
    { region: null, amount: 6, joined: new Date("2025-12-30T00:00:00.000Z") },
    { region: "east", amount: 3, joined: null },
  ];
  const tables = new Map([["rows", rows]]);

  function both(sql: string): unknown[] {
    const plan = compileQuery(sql);
    const vectorized = executeQuery(plan, tables);
    expect(vectorized.rows, sql).toEqual(executeRowQuery(compileQuery(sql), tables).rows);
    return vectorized.rows;
  }

  it("COALESCE returns the first non-null argument and stays lazy over nulls", () => {
    expect(
      both("SELECT COALESCE(region, 'unknown') AS label, amount FROM rows ORDER BY amount"),
    ).toEqual([
      { label: "east", amount: 3 },
      { label: "unknown", amount: 6 },
      { label: "west", amount: 10 },
    ]);
    expect(both("SELECT COALESCE(NULL, NULL) AS value, amount FROM rows ORDER BY amount")).toEqual([
      { value: null, amount: 3 },
      { value: null, amount: 6 },
      { value: null, amount: 10 },
    ]);
    expect(both("SELECT SUM(COALESCE(NULL, amount, 0)) AS total FROM rows")).toEqual([
      { total: 19 },
    ]);
    expect(() => compileQuery("SELECT COALESCE() AS value FROM rows")).toThrow(
      "COALESCE requires at least one argument",
    );
  });

  it("DATE_TRUNC truncates in UTC for every supported unit and propagates NULL", () => {
    const truncated = both(
      "SELECT DATE_TRUNC('month', joined) AS month, DATE_TRUNC('year', joined) AS year, DATE_TRUNC('quarter', joined) AS quarter, DATE_TRUNC('week', joined) AS week, DATE_TRUNC('day', joined) AS day, DATE_TRUNC('hour', joined) AS hour, amount FROM rows ORDER BY amount",
    );
    expect(truncated[2]).toEqual({
      month: new Date("2026-02-01T00:00:00.000Z"),
      year: new Date("2026-01-01T00:00:00.000Z"),
      quarter: new Date("2026-01-01T00:00:00.000Z"),
      // 2026-02-14 is a Saturday; the Monday of that week is 2026-02-09.
      week: new Date("2026-02-09T00:00:00.000Z"),
      day: new Date("2026-02-14T00:00:00.000Z"),
      hour: new Date("2026-02-14T13:00:00.000Z"),
      amount: 10,
    });
    expect(truncated[0]).toEqual({
      month: null,
      year: null,
      quarter: null,
      week: null,
      day: null,
      hour: null,
      amount: 3,
    });
    expect(() => compileQuery("SELECT DATE_TRUNC('fortnight', joined) FROM rows")).toThrow(
      "Unsupported DATE_TRUNC unit: fortnight",
    );
    expect(() => compileQuery("SELECT DATE_TRUNC(joined) FROM rows")).toThrow(
      "DATE_TRUNC requires a unit and a datetime argument",
    );
    expect(() =>
      executeRowQuery(compileQuery("SELECT DATE_TRUNC('day', amount) AS d FROM rows"), tables),
    ).toThrow("DATE_TRUNC requires a date or datetime value");
  });

  it("keeps SQL NULL semantics for literal IN lists on the hashed membership path", () => {
    // Large enough literal lists take the cached-set path in both executors; semantics
    // must not change: a NULL probe never matches, and NOT IN over a list containing
    // NULL matches nothing.
    expect(both("SELECT amount FROM rows WHERE amount IN (3, 10, 99) ORDER BY amount")).toEqual([
      { amount: 3 },
      { amount: 10 },
    ]);
    expect(both("SELECT amount FROM rows WHERE amount NOT IN (3, NULL) ORDER BY amount")).toEqual(
      [],
    );
    expect(
      both("SELECT amount FROM rows WHERE region IN ('west', 'east') ORDER BY amount"),
    ).toEqual([{ amount: 3 }, { amount: 10 }]);
    expect(both("SELECT amount FROM rows WHERE amount NOT IN (10, 99) ORDER BY amount")).toEqual([
      { amount: 3 },
      { amount: 6 },
    ]);
  });
});

describe("compile error positions", () => {
  /** The text a `SqlCompileError` points at, sliced back out of the caller's own SQL. */
  function failurePoint(compile: () => unknown, sql: string): { at: string; message: string } {
    try {
      compile();
    } catch (error) {
      if (!(error instanceof SqlCompileError)) throw error;
      expect(error.offset).toBeGreaterThanOrEqual(0);
      expect(error.offset + error.length).toBeLessThanOrEqual(sql.length);
      return {
        at: sql.slice(error.offset, error.offset + error.length),
        message: error.message,
      };
    }
    throw new Error(`Expected compilation to fail: ${sql}`);
  }

  function queryFailure(sql: string): { at: string; message: string } {
    return failurePoint(() => compileQuery(sql), sql);
  }

  it("points tokenizer failures at the offending characters", () => {
    expect(queryFailure("SELECT * FROM events WHERE x = 'oops")).toEqual({
      at: "'oops",
      message: "Unterminated string literal",
    });
    expect(queryFailure("SELECT * FROM events /* note").at).toBe("/* note");
    expect(queryFailure("SELECT # FROM events").at).toBe("#");
    expect(queryFailure("SELECT * FROM a; SELECT * FROM b").at).toBe(";");
    expect(queryFailure("SELECT 1.2.3 FROM events").at).toBe("1.2.3");
  });

  it("points parser failures at the token that failed", () => {
    expect(queryFailure("DELETE FROM events")).toEqual({
      at: "DELETE",
      message: "Expected SELECT, found DELETE",
    });
    // A query that ends early has nowhere to point but the end, so the span is empty.
    expect(queryFailure("SELECT * FROM")).toEqual({
      at: "",
      message: "Expected identifier, found end of query",
    });
    expect(queryFailure("")).toEqual({ at: "", message: "Enter a SELECT query" });
  });

  it("reports positions in the caller's text, not the trimmed copy", () => {
    // compileQuery trims before parsing; the offset must still index the string it was given.
    expect(queryFailure("\n   DELETE FROM events").at).toBe("DELETE");
    expect(queryFailure("\t\t SELECT # FROM events").at).toBe("#");
  });

  it("locates statement failures too", () => {
    // INSERT ... SELECT is a statement now, so the failing keyword is a misspelled one.
    const sql = "  INSERT INTO t (a) SELEC a FROM t";
    expect(failurePoint(() => compileStatement(sql), sql)).toEqual({
      at: "SELEC",
      message: "Expected VALUES, found SELEC",
    });
  });

  it("stays a TypeError so existing handling keeps working", () => {
    const error = (() => {
      try {
        compileQuery("DELETE FROM events");
      } catch (thrown) {
        return thrown;
      }
      throw new Error("Expected compilation to fail");
    })();
    expect(error).toBeInstanceOf(TypeError);
    expect(error).toBeInstanceOf(SqlCompileError);
  });
});

describe("plan copies", () => {
  const corpus = [
    "SELECT 1 AS one",
    "SELECT id, amount FROM data WHERE amount > ? AND region = ? ORDER BY id LIMIT ? OFFSET ?",
    "SELECT region, COUNT(*) AS c, SUM(amount) AS s FROM data GROUP BY region HAVING COUNT(*) > ?",
    "SELECT d.id FROM data d JOIN dims m ON m.region = d.region WHERE d.joined > ?",
    "SELECT id FROM data WHERE id IN (SELECT id FROM data WHERE amount < ?) AND label LIKE ?",
    "WITH RECURSIVE n(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM n WHERE x < ?) SELECT x FROM n",
    "SELECT id, SUM(amount) OVER (PARTITION BY region ORDER BY id) AS running FROM data WHERE id <= ?",
    "SELECT CASE WHEN amount > ? THEN 'big' ELSE 'small' END AS size, DATE_TRUNC('day', joined) AS day FROM data",
  ];

  it("copies a compiled plan exactly, and shares nothing with the original", () => {
    for (const sql of corpus) {
      const plan = compileQuery(sql);
      const copy = clonePlanTree(plan);
      expect(copy).toEqual(structuredClone(plan));
      expect(copy).not.toBe(plan);
      expect(copy.select).not.toBe(plan.select);
      expect(copy.base).not.toBe(plan.base);
    }
  });

  it("leaves the original plan untouched by binding, and copies Date literals as new instances", () => {
    const plan = compileQuery("SELECT id FROM data WHERE joined > ? AND amount > ?");
    const before = structuredClone(plan);
    const when = new Date("2024-05-06T07:08:09.010Z");
    const bound = bindPlanParameters(plan, [when, 5]);
    expect(plan).toEqual(before);
    expect(bound.predicates[0]?.right).toEqual({ kind: "literal", value: when });
    // Binding again from the same cached plan yields an equal but distinct bound plan, and a
    // copy of a bound plan owns its Date literals.
    const again = bindPlanParameters(plan, [when, 5]);
    expect(again).toEqual(bound);
    expect(again).not.toBe(bound);
    const copy = clonePlanTree(bound);
    const literal = copy.predicates[0]?.right as { value: Date };
    expect(literal.value).toEqual(when);
    expect(literal.value).not.toBe(when);
  });

  it("falls back to structuredClone for values a plan is not expected to hold", () => {
    const odd = {
      items: new Map([["a", 1]]),
      bytes: new Uint8Array([1, 2, 3]),
      nested: [{ d: new Date(0) }],
    };
    const copy = clonePlanTree(odd);
    expect(copy).toEqual(odd);
    expect(copy.items).not.toBe(odd.items);
    expect(copy.bytes).not.toBe(odd.bytes);
    expect(copy.nested[0]?.d).not.toBe(odd.nested[0]?.d);
  });
});

describe("window functions over shared and private rows", () => {
  it("answers a windowed query the same whether its inner rows were cached or not", async () => {
    const database = new MinnowDatabase(new MemoryBlockStore());
    await database.createTable({
      name: "w",
      columns: [
        { name: "id", type: "number" },
        { name: "region", type: "string" },
        { name: "amount", type: "number" },
      ],
    });
    await database.insertBatch("w", [
      { id: 1, region: "west", amount: 5 },
      { id: 2, region: "west", amount: 7 },
      { id: 3, region: "east", amount: 1 },
    ]);
    const sql =
      "SELECT id, SUM(amount) OVER (PARTITION BY region ORDER BY id) AS running FROM w ORDER BY id";
    const expected = [
      { id: 1, running: 5 },
      { id: 2, running: 12 },
      { id: 3, running: 1 },
    ];
    // Twice without the memo: the second run reads the inner block from the block cache, which
    // the window step must not have written its aliases into the first time.
    expect((await database.query(sql, { memoize: false })).rows).toEqual(expected);
    expect((await database.query(sql, { memoize: false })).rows).toEqual(expected);
    expect((await database.query(sql)).rows).toEqual(expected);
    expect((await database.query(sql)).rows).toEqual(expected);
  });

  it("copies rows it may share and writes in place rows it owns", () => {
    const plan = compileQuery(
      "SELECT id, SUM(amount) OVER (PARTITION BY region ORDER BY id) AS running FROM w",
    );
    const windowed = plan.base.windowed;
    if (windowed === undefined) throw new Error("Expected a windowed source");
    // The inner block's rows carry the hidden partition/order aliases the window reads.
    const inner = windowed.block;
    const aliasOf = (index: number): string => inner.select[index]?.alias ?? "";
    const make = () => ({
      columns: inner.select.map((item) => item.alias),
      columnDomains: inner.select.map(() => null),
      rows: [
        { [aliasOf(0)]: 1, [aliasOf(1)]: "west", [aliasOf(2)]: 5 },
        { [aliasOf(0)]: 2, [aliasOf(1)]: "west", [aliasOf(2)]: 7 },
        { [aliasOf(0)]: 3, [aliasOf(1)]: "east", [aliasOf(2)]: 1 },
      ].map((row) => {
        // Every alias the window names, from whichever select item it came from.
        const full: Record<string, unknown> = { ...row };
        for (const item of inner.select) {
          if (!(item.alias in full)) {
            const ref = item.expression;
            const name = ref.kind === "column" ? ref.reference.split(".").pop() : undefined;
            full[item.alias] =
              name === "id"
                ? row[aliasOf(0)]
                : name === "region"
                  ? row[aliasOf(1)]
                  : name === "amount"
                    ? row[aliasOf(2)]
                    : null;
          }
        }
        return full as Record<string, number | string | null>;
      }),
    });
    const shared = make();
    const before = structuredClone(shared.rows[0]);
    const windowedCopy = applyWindowFunctions(shared, windowed.windows);
    expect(windowedCopy.rows.map((row) => row.running)).toEqual([5, 12, 1]);
    expect(shared.rows[0]).toEqual(before);
    const owned = make();
    const windowedInPlace = applyWindowFunctions(owned, windowed.windows, { copyRows: false });
    expect(windowedInPlace.rows.map((row) => row.running)).toEqual([5, 12, 1]);
    expect(owned.rows[0]?.running).toBe(5);
  });
});
