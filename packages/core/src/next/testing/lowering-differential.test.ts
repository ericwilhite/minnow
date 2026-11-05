/**
 * Lowering against the current engine (the oracle): the same DDL, rows and statements through
 * `MinnowDatabase` and through lowering + the IR reference interpreter (ir-reference.ts), with
 * identical results required. This checks lowering's semantics (names, types, constants,
 * grouping, ordering, defaults, generated columns, ON CONFLICT, RETURNING) on the milestone M1
 * surface before the executor exists. Values are compared in the oracle's representation:
 * integers as numbers, floats to 12 significant digits.
 */
import { describe, expect, it } from "vitest";
import { MinnowDatabase } from "../../engine/database.js";
import { MemoryBlockStore } from "../../storage/index.js";
import { ReferenceSqlError } from "./ir-reference.js";
import { LoweredDatabase } from "./lowering-harness.js";

/** A statement and its placeholder values. */
type Case = readonly [string, (readonly unknown[])?];

const SCHEMA = [
  "CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT NOT NULL, region TEXT, tier SMALLINT DEFAULT 1, score DOUBLE PRECISION, active BOOLEAN)",
  "CREATE TABLE orders (id BIGINT PRIMARY KEY, customer_id INTEGER NOT NULL, qty INTEGER NOT NULL, price DOUBLE PRECISION, note VARCHAR(20), total DOUBLE PRECISION GENERATED ALWAYS AS (qty * price) STORED, CHECK (qty >= 0))",
];

const SEED = [
  "INSERT INTO customers (id, name, region, tier, score, active) VALUES (1, 'Ada', 'west', 3, 9.5, TRUE), (2, 'Bo', 'east', 1, 4.25, FALSE), (3, 'Cy', 'west', 2, NULL, TRUE), (4, 'Di', NULL, 2, 7.0, NULL)",
  "INSERT INTO customers (id, name) VALUES (5, 'Ed')",
  "INSERT INTO orders (id, customer_id, qty, price, note) VALUES (10, 1, 2, 3.5, 'gift'), (11, 1, 5, 1.25, NULL), (12, 2, 1, 10.0, 'rush'), (13, 3, 0, 2.0, NULL), (14, 3, 7, 0.5, 'bulk'), (15, 4, 3, NULL, NULL)",
];

const QUERIES: readonly Case[] = [
  ["SELECT id, name FROM customers ORDER BY id"],
  ["SELECT * FROM customers ORDER BY id"],
  ["SELECT name, tier FROM customers WHERE id = 3"],
  ["SELECT name FROM customers WHERE id = $1", [2]],
  ["SELECT id FROM customers WHERE id IN (1, 4, 9) ORDER BY id"],
  ["SELECT id FROM customers WHERE id = 5000000000"],
  ["SELECT id FROM customers WHERE region = 'west' AND tier >= 2 ORDER BY id"],
  ["SELECT id FROM customers WHERE region IS NULL OR active IS NULL ORDER BY id"],
  ["SELECT id FROM customers WHERE name LIKE '_d%' ORDER BY id"],
  ["SELECT id FROM customers WHERE name ILIKE 'a%' OR name NOT LIKE '%o' ORDER BY id"],
  ["SELECT id, tier * 10 + 1 AS x, tier / 2 AS half, tier % 2 AS odd FROM customers ORDER BY id"],
  ["SELECT id, score * 2 AS doubled, -tier AS negative FROM customers ORDER BY id"],
  [
    "SELECT id, COALESCE(region, 'none') AS region, NULLIF(tier, 1) AS t FROM customers ORDER BY id",
  ],
  [
    "SELECT id, CASE WHEN tier > 2 THEN 'gold' WHEN tier = 2 THEN 'silver' ELSE 'basic' END AS level FROM customers ORDER BY id",
  ],
  [
    "SELECT id, UPPER(name) AS u, LOWER(name) AS l, LENGTH(name) AS n, name || '!' AS shout FROM customers ORDER BY id",
  ],
  [
    "SELECT id, CAST(score AS INTEGER) AS s, CAST(tier AS TEXT) AS t FROM customers WHERE score IS NOT NULL ORDER BY id",
  ],
  ["SELECT id FROM customers WHERE tier = '2' ORDER BY id"],
  ["SELECT id FROM customers WHERE active ORDER BY id"],
  ["SELECT id FROM customers WHERE NOT active ORDER BY id"],
  ["SELECT id FROM customers WHERE active IS TRUE ORDER BY id"],
  ["SELECT id FROM customers WHERE region IS DISTINCT FROM 'west' ORDER BY id"],
  ["SELECT name FROM customers ORDER BY score DESC, id LIMIT 3"],
  ["SELECT name FROM customers ORDER BY score NULLS FIRST, id"],
  ["SELECT name FROM customers ORDER BY tier, name DESC LIMIT 2 OFFSET 1"],
  ["SELECT name FROM customers ORDER BY id LIMIT $1", [2]],
  ["SELECT name FROM customers ORDER BY LENGTH(name) DESC, id"],
  ["SELECT count(*) AS n FROM customers"],
  ["SELECT count(region) AS n, count(DISTINCT region) AS d FROM customers"],
  ["SELECT region, count(*) AS n FROM customers GROUP BY region ORDER BY region NULLS LAST"],
  [
    "SELECT tier, sum(score) AS s, min(name) AS lo, max(name) AS hi FROM customers GROUP BY tier ORDER BY tier",
  ],
  [
    "SELECT customer_id, sum(qty) AS q, avg(qty) AS a, count(note) AS notes FROM orders GROUP BY customer_id ORDER BY customer_id",
  ],
  [
    "SELECT customer_id, sum(qty) AS q FROM orders GROUP BY customer_id HAVING sum(qty) > 3 ORDER BY 2 DESC",
  ],
  ["SELECT sum(id) AS s, max(id) AS m FROM orders"],
  ["SELECT DISTINCT region FROM customers ORDER BY region"],
  ["SELECT DISTINCT tier FROM customers WHERE tier > 1 ORDER BY 1 DESC"],
  ["SELECT id, total FROM orders ORDER BY id"],
  ["SELECT id, note FROM orders WHERE note IS NOT NULL ORDER BY total DESC NULLS LAST"],
  ["SELECT count(*) + 1 AS n1, sum(qty) * 2 AS q2 FROM orders WHERE customer_id = 1"],
  ["SELECT 1 + 2 AS three, 'x' AS t, TRUE AS b"],
  ["SELECT max(qty) AS m FROM orders WHERE customer_id = 99"],
  ["SELECT id FROM orders WHERE customer_id IN (SELECT 1) ORDER BY id"],
];

const MUTATIONS: readonly Case[] = [
  ["UPDATE customers SET tier = tier + 1 WHERE region = 'west'"],
  ["UPDATE customers SET region = 'north', score = score * 2 WHERE id = $1", [4]],
  ["UPDATE orders SET qty = qty + 1 WHERE id = 13"],
  ["UPDATE customers SET tier = DEFAULT WHERE id = 2"],
  ["DELETE FROM orders WHERE qty > 6"],
  ["DELETE FROM customers WHERE id = 5"],
  ["INSERT INTO customers (id, name) VALUES (1, 'dup') ON CONFLICT (id) DO NOTHING"],
  [
    "INSERT INTO customers (id, name, tier) VALUES (2, 'Bob', 9) ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name, tier = customers.tier + EXCLUDED.tier",
  ],
  [
    "INSERT INTO customers (id, name) VALUES (6, 'Fay') ON CONFLICT (id) DO UPDATE SET name = 'never'",
  ],
  [
    "INSERT INTO orders (id, customer_id, qty, price) SELECT id + 100, id, tier, score FROM customers WHERE score IS NOT NULL",
  ],
];

const FAILURES: readonly string[] = [
  "INSERT INTO customers (id) VALUES (7)",
  "INSERT INTO customers (id, name) VALUES (1, 'again')",
  "INSERT INTO orders (id, customer_id, qty) VALUES (99, 1, -1)",
  "UPDATE customers SET name = NULL WHERE id = 1",
];

const RETURNING: readonly string[] = [
  "INSERT INTO orders (id, customer_id, qty, price) VALUES (200, 1, 4, 2.0) RETURNING id, total",
  "UPDATE orders SET qty = 9 WHERE id = 10 RETURNING id, qty, total",
  "DELETE FROM customers WHERE id = 3 RETURNING name, tier",
];

function oracleValue(value: unknown): unknown {
  if (typeof value === "number" && !Number.isInteger(value)) return Number(value.toPrecision(12));
  return value;
}

async function both(): Promise<{ oracle: MinnowDatabase; next: LoweredDatabase }> {
  const oracle = new MinnowDatabase(new MemoryBlockStore());
  const next = new LoweredDatabase();
  for (const sql of [...SCHEMA, ...SEED]) {
    await oracle.execute(sql);
    next.execute(sql);
  }
  return { oracle, next };
}

async function oracleRows(oracle: MinnowDatabase, sql: string, params: readonly unknown[] = []) {
  const result = await oracle.query(sql, { params: params as never });
  return result.rows.map((row) =>
    Object.fromEntries(Object.entries(row).map(([key, value]) => [key, oracleValue(value)])),
  );
}

describe("lowering agrees with the current engine (milestone M1 surface)", () => {
  it("answers every query identically", async () => {
    const { oracle, next } = await both();
    const unsupported: string[] = [];
    for (const [sql, params = []] of QUERIES) {
      let ours: Array<Record<string, unknown>>;
      try {
        ours = next.execute(sql, params);
      } catch (error) {
        if ((error as { code?: string }).code === "MINNOW_UNSUPPORTED_PLAN") {
          unsupported.push(sql);
          continue;
        }
        throw new Error(`${sql}: ${String(error)}`, { cause: error });
      }
      expect(ours, sql).toEqual(await oracleRows(oracle, sql, params));
    }
    // Subqueries are milestone M3; everything else is covered.
    expect(unsupported).toEqual([
      "SELECT id FROM orders WHERE customer_id IN (SELECT 1) ORDER BY id",
    ]);
  });

  it("writes identically: UPDATE, DELETE, ON CONFLICT, INSERT … SELECT", async () => {
    const { oracle, next } = await both();
    for (const [sql, params = []] of MUTATIONS) {
      await oracle.execute(sql, params as never);
      next.execute(sql, params);
      for (const table of ["customers", "orders"]) {
        const all = `SELECT * FROM ${table} ORDER BY id`;
        expect(next.execute(all), `${sql} → ${table}`).toEqual(await oracleRows(oracle, all));
      }
    }
  });

  it("returns identical RETURNING rows", async () => {
    const { oracle, next } = await both();
    for (const sql of RETURNING) {
      const expected = (await oracle.execute(sql)) as {
        returnedRows?: Array<Record<string, unknown>>;
      };
      const rows = (expected.returnedRows ?? []).map((row) =>
        Object.fromEntries(Object.entries(row).map(([key, value]) => [key, oracleValue(value)])),
      );
      expect(next.execute(sql), sql).toEqual(rows);
    }
  });

  it("refuses the same invalid writes", async () => {
    const { oracle, next } = await both();
    for (const sql of FAILURES) {
      await expect(oracle.execute(sql), sql).rejects.toThrow();
      expect(() => next.execute(sql), sql).toThrow(ReferenceSqlError);
    }
  });
});

describe("lowered table rules (R14.3)", () => {
  it("are accepted by the executor's compileTableRules", async () => {
    const { compileTableRules } = await import("../exec/rules.js");
    const next = new LoweredDatabase();
    for (const sql of SCHEMA) next.execute(sql);
    next.execute(
      "CREATE TABLE ruled (a INTEGER NOT NULL, b TEXT DEFAULT 'x', c BIGINT GENERATED ALWAYS AS (a * 2) STORED, d VARCHAR(5), CHECK (a > 0), CHECK (d <> 'bad'))",
    );
    for (const table of next.catalog.view.tables) {
      const expressions = next.lowerer.lowerTableRules(table, next.catalog.view);
      expect(
        () => compileTableRules(table, next.catalog.view.version, expressions),
        table.name,
      ).not.toThrow();
      expect(expressions.checks).toHaveLength(table.checks.length);
    }
    const ruled = next.catalog.view.tableByName("ruled");
    const rules =
      ruled === undefined ? null : next.lowerer.lowerTableRules(ruled, next.catalog.view);
    expect(rules?.generated[0]?.expr).toMatchObject({
      op: "cast",
      assignment: true,
      type: { kind: "int64" },
    });
    expect(rules?.defaults?.map((entry) => entry.expr.type)).toEqual([{ kind: "text" }]);
  });
});
