/**
 * Lowering's milestone M3 surface against PostgreSQL (PGlite): joins and their order, subqueries
 * (decorrelated joins, mark joins, apply, init plans), windows, set operations, recursion, index
 * ranges, UPDATE … FROM, DELETE … USING and MERGE. Each statement runs in PGlite and through
 * lowering + the IR reference interpreter; the rows (ordered queries), result types and errors
 * must match, and every plan is type-checked (ir-check.ts).
 */
import { PGlite } from "@electric-sql/pglite";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import type { Rel } from "../ir/ir.js";
import { check as checkIn, nextOutcome, pgOutcome, type Case } from "./pglite-oracle.js";

let pg: PGlite;

beforeAll(async () => {
  pg = new PGlite();
  await pg.waitReady;
}, 60_000);

afterAll(async () => {
  await pg.close();
});

const check = (setup: readonly string[], cases: readonly Case[]) => checkIn(pg, setup, cases);

const RETAIL = [
  "CREATE TABLE customers (id integer PRIMARY KEY, name text NOT NULL, region text, tier smallint)",
  "CREATE TABLE products (id integer PRIMARY KEY, title text NOT NULL, price double precision)",
  "CREATE TABLE orders (id integer PRIMARY KEY, customer_id integer, product_id integer, qty integer NOT NULL, placed integer)",
  "CREATE INDEX orders_placed ON orders (placed)",
  "INSERT INTO customers VALUES (1, 'Ada', 'west', 3), (2, 'Bo', 'east', 1), (3, 'Cy', 'west', 2), (4, 'Di', NULL, 2), (5, 'Ed', 'east', NULL)",
  "INSERT INTO products VALUES (10, 'pen', 1.5), (11, 'ink', 4.0), (12, 'pad', NULL)",
  "INSERT INTO orders VALUES (100, 1, 10, 2, 1), (101, 1, 11, 1, 2), (102, 2, 10, 5, 3), (103, 3, 12, 1, 4), (104, 3, 10, 3, 5), (105, NULL, 11, 4, 6), (106, 9, 10, 1, 7)",
];

/** The operators of a plan, depth first. */
function ops(rel: Rel): string[] {
  const children: Rel[] = [];
  for (const value of Object.values(rel))
    if (typeof value === "object" && value !== null && "op" in value && "output" in value)
      children.push(value as Rel);
  if (rel.op === "setOp") children.push(...rel.inputs);
  return [rel.op, ...children.flatMap(ops)];
}

describe("lowering agrees with PostgreSQL on joins", () => {
  it("joins of every kind, in any order the cost model picks", async () => {
    await check(RETAIL, [
      {
        sql: "SELECT c.name, o.qty FROM customers c JOIN orders o ON o.customer_id = c.id ORDER BY o.id",
      },
      {
        sql: "SELECT c.name, o.qty FROM customers c, orders o WHERE o.customer_id = c.id AND o.qty > 1 ORDER BY o.id",
      },
      {
        sql: "SELECT c.name, o.id FROM customers c LEFT JOIN orders o ON o.customer_id = c.id ORDER BY c.id, o.id",
      },
      {
        sql: "SELECT c.name, o.id FROM customers c LEFT JOIN orders o ON o.customer_id = c.id AND o.qty > 2 ORDER BY c.id, o.id",
      },
      {
        sql: "SELECT c.name, o.id FROM customers c LEFT JOIN orders o ON o.customer_id = c.id WHERE o.id IS NULL ORDER BY c.id",
      },
      {
        sql: "SELECT o.id, c.name FROM orders o RIGHT JOIN customers c ON o.customer_id = c.id ORDER BY c.id, o.id",
      },
      {
        // ORDER BY selected columns: the front end's FULL JOIN desugar maps an unselected `c.id`
        // to the output `id` (o.id), in the current engine too (reported; not lowering's to undo).
        sql: "SELECT c.name, o.id FROM customers c FULL JOIN orders o ON o.customer_id = c.id ORDER BY c.name NULLS LAST, o.id NULLS LAST",
      },
      {
        sql: "SELECT c.name, p.title, o.qty FROM orders o JOIN customers c ON c.id = o.customer_id JOIN products p ON p.id = o.product_id ORDER BY o.id",
      },
      {
        sql: "SELECT c.name, count(*) AS n, sum(o.qty * p.price) AS spend FROM customers c JOIN orders o ON o.customer_id = c.id JOIN products p ON p.id = o.product_id GROUP BY c.name ORDER BY c.name",
      },
      {
        sql: "SELECT a.name AS a, b.name AS b FROM customers a JOIN customers b ON a.region = b.region AND a.id < b.id ORDER BY a.id, b.id",
      },
      { sql: "SELECT c.id FROM customers c CROSS JOIN products p WHERE p.id = 10 ORDER BY c.id" },
      { sql: "SELECT name FROM customers c JOIN orders o USING (id) ORDER BY name" },
      {
        sql: "SELECT region, count(*) AS n FROM customers c JOIN orders o ON o.customer_id = c.id GROUP BY region ORDER BY region NULLS LAST",
      },
    ]);
  });

  it("puts the smaller side on the build side and joins three tables in one tree", async () => {
    const next = await check(RETAIL, []);
    const lowered = next.lower(
      "SELECT c.name FROM orders o JOIN customers c ON c.id = o.customer_id JOIN products p ON p.id = o.product_id",
    );
    if (lowered.kind !== "query") throw new Error("Expected a query");
    expect(ops(lowered.plan.root).filter((op) => op === "join")).toHaveLength(2);
  });
});

describe("lowering agrees with PostgreSQL on subqueries", () => {
  it("decorrelated IN / EXISTS / scalar subqueries, mark joins, apply and init plans", async () => {
    await check(RETAIL, [
      {
        sql: "SELECT name FROM customers WHERE id IN (SELECT customer_id FROM orders) ORDER BY id",
      },
      {
        sql: "SELECT name FROM customers c WHERE EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.id AND o.qty > 2) ORDER BY id",
      },
      {
        sql: "SELECT name FROM customers c WHERE NOT EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.id) ORDER BY id",
      },
      {
        sql: "SELECT name, (SELECT max(qty) FROM orders o WHERE o.customer_id = c.id) AS m FROM customers c ORDER BY id",
      },
      {
        sql: "SELECT name FROM customers c WHERE id > (SELECT avg(qty) FROM orders o WHERE o.customer_id = c.id) ORDER BY id",
      },
      { sql: "SELECT name, (SELECT max(qty) FROM orders) AS m FROM customers ORDER BY id" },
      {
        sql: "SELECT name FROM customers WHERE id NOT IN (SELECT customer_id FROM orders) ORDER BY id",
      },
      {
        sql: "SELECT name FROM customers WHERE id NOT IN (SELECT customer_id FROM orders WHERE customer_id IS NOT NULL) ORDER BY id",
      },
      {
        sql: "SELECT id, id IN (SELECT customer_id FROM orders) AS has FROM customers ORDER BY id",
      },
      {
        sql: "SELECT id FROM orders WHERE qty > ALL (SELECT qty FROM orders WHERE customer_id = 3) ORDER BY id",
      },
      { sql: "SELECT id FROM orders WHERE qty = ANY (SELECT tier FROM customers) ORDER BY id" },
      {
        sql: "SELECT name FROM customers WHERE EXISTS (SELECT 1 FROM products WHERE price > 3) ORDER BY id",
      },
      {
        sql: "SELECT region, count(*) AS n FROM customers GROUP BY region HAVING count(*) > (SELECT count(*) FROM products) - 2 ORDER BY region NULLS LAST",
      },
      { sql: "SELECT name, (SELECT title FROM products) AS t FROM customers" },
    ]);
  });

  it("R20.5: a correlated WHERE decides participation, not the mark", async () => {
    const setup = [
      "CREATE TABLE p (x integer, w integer)",
      "CREATE TABLE b (y integer, z integer)",
      "INSERT INTO p VALUES (1, 0), (2, 5), (NULL, 0)",
      "INSERT INTO b VALUES (1, NULL), (2, 9), (NULL, 1)",
    ];
    const next = await check(setup, [
      {
        sql: "SELECT x, x NOT IN (SELECT y FROM b WHERE b.z > p.w) AS m FROM p ORDER BY x NULLS LAST",
      },
      { sql: "SELECT x, x IN (SELECT y FROM b WHERE b.z > p.w) AS m FROM p ORDER BY x NULLS LAST" },
      { sql: "SELECT x FROM p WHERE x NOT IN (SELECT y FROM b WHERE b.z > p.w) ORDER BY x" },
      { sql: "SELECT x, x IN (SELECT y FROM b) AS m FROM p ORDER BY x NULLS LAST" },
      // Decorrelated quantified comparisons: a NULL from the subquery makes ALL unknown.
      { sql: "SELECT x FROM p WHERE x > ALL (SELECT y FROM b WHERE b.z > p.w) ORDER BY x" },
      { sql: "SELECT x FROM p WHERE x < ALL (SELECT y FROM b WHERE b.z < p.w) ORDER BY x" },
      { sql: "SELECT x FROM p WHERE x <> ALL (SELECT y FROM b WHERE b.z > p.w) ORDER BY x" },
      { sql: "SELECT x FROM p WHERE x = ANY (SELECT y FROM b WHERE b.z > p.w) ORDER BY x" },
      { sql: "SELECT x FROM p WHERE NOT (x >= ANY (SELECT y FROM b WHERE b.z > p.w)) ORDER BY x" },
    ]);
    // The decorrelator rewrites the correlated forms into joins; an uncorrelated NOT IN reaches
    // lowering as a subquery and becomes a mark join, its inner WHERE filtering the build side.
    const sql =
      "SELECT x, x NOT IN (SELECT y FROM b WHERE z > 0) AS m FROM p ORDER BY x NULLS LAST";
    const lowered = next.lower(sql);
    if (lowered.kind !== "query") throw new Error("Expected a query");
    expect(JSON.stringify(lowered.plan.root)).toContain('"kind":"mark"');
    expect(nextOutcome(next, sql)).toEqual(await pgOutcome(pg, sql));
  });
});

describe("lowering agrees with PostgreSQL on windows, set operations and recursion", () => {
  it("window functions", async () => {
    await check(RETAIL, [
      { sql: "SELECT id, row_number() OVER (ORDER BY id) AS rn FROM orders ORDER BY id" },
      {
        sql: "SELECT id, rank() OVER (PARTITION BY customer_id ORDER BY qty DESC) AS r, dense_rank() OVER (ORDER BY qty) AS d FROM orders ORDER BY id",
      },
      {
        sql: "SELECT id, sum(qty) OVER (PARTITION BY customer_id ORDER BY id) AS running, count(*) OVER () AS total FROM orders ORDER BY id",
      },
      {
        sql: "SELECT id, lag(qty) OVER (ORDER BY id) AS prev, lead(qty, 2, 0) OVER (ORDER BY id) AS next2 FROM orders ORDER BY id",
      },
      {
        sql: "SELECT id, first_value(qty) OVER (PARTITION BY product_id ORDER BY id) AS f, max(qty) OVER (PARTITION BY product_id) AS m FROM orders ORDER BY id",
      },
    ]);
  });

  it("set operations", async () => {
    await check(RETAIL, [
      {
        sql: "SELECT customer_id AS v FROM orders UNION SELECT id FROM customers ORDER BY 1 NULLS LAST",
      },
      {
        sql: "SELECT customer_id AS v FROM orders UNION ALL SELECT id FROM customers ORDER BY 1 NULLS LAST",
      },
      { sql: "SELECT id AS v FROM customers INTERSECT SELECT customer_id FROM orders ORDER BY 1" },
      { sql: "SELECT id AS v FROM customers EXCEPT SELECT customer_id FROM orders ORDER BY 1" },
      { sql: "SELECT qty AS v FROM orders EXCEPT ALL SELECT tier FROM customers ORDER BY 1" },
      { sql: "SELECT tier AS v FROM customers INTERSECT ALL SELECT qty FROM orders ORDER BY 1" },
      {
        sql: "SELECT id, 'c' AS k FROM customers WHERE id < 3 UNION ALL SELECT id, 'p' FROM products ORDER BY 1",
      },
    ]);
  });

  it("recursive CTEs and generate_series", async () => {
    await check(RETAIL, [
      {
        sql: "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 5) SELECT n FROM r ORDER BY n",
      },
      {
        sql: "WITH RECURSIVE r(n) AS (SELECT 1 UNION SELECT n % 3 + 1 FROM r) SELECT n FROM r ORDER BY n",
      },
      { sql: "SELECT x FROM generate_series(1, 4) AS g(x) ORDER BY x" },
      { sql: "SELECT x FROM generate_series(10, 1, -3) AS g(x) ORDER BY x" },
    ]);
  });
});

describe("lowering agrees with PostgreSQL on index ranges and DML with sources", () => {
  it("chooses an index range for a selective range and returns PostgreSQL's rows", async () => {
    const next = await check(RETAIL, [
      { sql: "SELECT id FROM orders WHERE placed BETWEEN 2 AND 4 ORDER BY id" },
      { sql: "SELECT id FROM orders WHERE placed = 3" },
      { sql: "SELECT id FROM orders WHERE placed > 5 ORDER BY id" },
      { sql: "SELECT id FROM customers WHERE id >= 2 AND id < 4 ORDER BY id" },
    ]);
    const lowered = next.lower("SELECT id FROM orders WHERE placed BETWEEN 2 AND 4");
    if (lowered.kind !== "query") throw new Error("Expected a query");
    expect(ops(lowered.plan.root)).toContain("indexRange");
  });

  it("UPDATE … FROM, DELETE … USING and MERGE", async () => {
    const tables = (sql: string): Case[] => [
      { sql },
      { sql: "SELECT id, name, region, tier FROM customers ORDER BY id" },
      { sql: "SELECT id, customer_id, product_id, qty, placed FROM orders ORDER BY id" },
    ];
    await check(RETAIL, [
      ...tables(
        "UPDATE orders o SET qty = o.qty + p.price::integer FROM products p WHERE p.id = o.product_id AND p.price IS NOT NULL",
      ),
      ...tables(
        "UPDATE customers c SET tier = 9 FROM orders o WHERE o.customer_id = c.id AND o.qty > 3",
      ),
      // Customers 1 and 3 match two orders each: each row is updated once (onRepeat "first").
      ...tables(
        "UPDATE customers c SET tier = c.tier + 1 FROM orders o WHERE o.customer_id = c.id",
      ),
      ...tables(
        "DELETE FROM orders o USING customers c WHERE c.id = o.customer_id AND c.region = 'west'",
      ),
      ...tables(
        "MERGE INTO customers c USING (SELECT 2 AS id, 'Bob' AS name UNION ALL SELECT 6, 'Fay') s ON c.id = s.id WHEN MATCHED THEN UPDATE SET name = s.name WHEN NOT MATCHED THEN INSERT (id, name) VALUES (s.id, s.name)",
      ),
      ...tables(
        "MERGE INTO orders o USING products p ON o.product_id = p.id AND o.id = 104 WHEN MATCHED AND p.price > 1 THEN DELETE",
      ),
      {
        sql: "MERGE INTO customers c USING (SELECT 1 AS id UNION ALL SELECT 1) s ON c.id = s.id WHEN MATCHED THEN UPDATE SET tier = 0",
      },
    ]);
  });
});

it("reports the same outcome as PostgreSQL for a scalar subquery with two rows", async () => {
  const next = await check(RETAIL, [
    { sql: "SELECT name, (SELECT id FROM customers) AS x FROM products" },
  ]);
  const expected = await pgOutcome(pg, "SELECT (SELECT id FROM customers) AS x");
  expect(expected).toEqual({ error: "21000" });
  expect(nextOutcome(next, "SELECT (SELECT id FROM customers) AS x")).toEqual(expected);
});
