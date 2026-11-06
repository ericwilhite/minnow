/**
 * Review B6-p2b findings against PostgreSQL (PGlite), on the review's own fixture. Each case runs
 * in PGlite and through lowering + the IR reference interpreter (pglite-oracle.ts): rows, result
 * types and errors must match, and every executed plan is type-checked (ir-check.ts).
 */
import { PGlite } from "@electric-sql/pglite";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
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

const REVIEW = [
  "CREATE TABLE a (id integer, v integer)",
  "CREATE TABLE b (id integer, v integer)",
  "CREATE TABLE c (id integer, v integer)",
  "CREATE INDEX bv ON b (v, id)",
  "INSERT INTO a VALUES (1, 10), (2, 20), (3, NULL)",
  "INSERT INTO b VALUES (1, 1), (1, 2), (4, NULL), (NULL, 3)",
  "INSERT INTO c VALUES (1, 7), (4, 8)",
];

describe("init plans run on the first read of their slot (R24.1)", () => {
  it("never runs an init plan whose value no row needs, nor raises its errors", async () => {
    await check(REVIEW, [
      // b has four rows: reading the scalar subquery would raise 21000.
      { sql: "SELECT (SELECT id FROM b) AS x FROM a WHERE id = 999" },
      { sql: "SELECT (SELECT id FROM b) AS x FROM a LIMIT 0" },
      { sql: "SELECT (SELECT id FROM b) AS x FROM a ORDER BY id LIMIT 0" },
      {
        sql: "SELECT CASE WHEN id = 999 THEN (SELECT id FROM b) ELSE 0 END AS x FROM a ORDER BY id",
      },
      // Division by zero inside the init plan (22012), also never reached.
      { sql: "SELECT (SELECT 10 / (v - v) FROM b WHERE v = 1) AS x FROM a WHERE id = 999" },
    ]);
  });

  it("still raises an init plan's error once a row reads it", async () => {
    await check(REVIEW, [
      { sql: "SELECT (SELECT id FROM b) AS x FROM a WHERE id = 1" },
      { sql: "SELECT CASE WHEN id = 2 THEN (SELECT id FROM b) ELSE 0 END AS x FROM a ORDER BY id" },
      { sql: "SELECT (SELECT 10 / (v - v) FROM b WHERE v = 1) AS x FROM a WHERE id = 1" },
      // One evaluation per query: the same value on every row.
      { sql: "SELECT id, (SELECT max(v) FROM b) AS m FROM a ORDER BY id" },
    ]);
  });
});

describe("set operations resolve unknown-typed constants against the other side", () => {
  it("in either arm order, as each pair of a chain, and nested", async () => {
    await check(REVIEW, [
      { sql: "SELECT NULL AS n UNION SELECT 1 ORDER BY n NULLS LAST" },
      { sql: "SELECT 1 AS n UNION SELECT NULL ORDER BY n NULLS LAST" },
      { sql: "SELECT '1' AS n UNION SELECT 2 ORDER BY n" },
      { sql: "SELECT 2 AS n UNION SELECT '1' ORDER BY n" },
      { sql: "SELECT NULL AS n UNION ALL SELECT 1.5 ORDER BY n" },
      { sql: "SELECT NULL AS n UNION SELECT NULL" },
      { sql: "SELECT 'a' AS n UNION SELECT 'b' ORDER BY n" },
      { sql: "SELECT NULL AS a, 'x' AS b UNION SELECT 1, 'y' ORDER BY a" },
      { sql: "SELECT NULL AS n INTERSECT SELECT 1" },
      { sql: "SELECT 1 AS n EXCEPT SELECT NULL ORDER BY n" },
      { sql: "SELECT id AS n FROM a UNION SELECT NULL ORDER BY n NULLS FIRST" },
      { sql: "SELECT '7' AS n UNION SELECT id FROM a ORDER BY n" },
      { sql: "SELECT n FROM (SELECT NULL AS n UNION SELECT 1) s ORDER BY n" },
      // Each pair resolves on its own: two unknowns are text before the third arm is seen.
      { sql: "SELECT NULL AS n UNION SELECT NULL UNION SELECT 1 ORDER BY n" },
      { sql: "SELECT 'x' AS n UNION SELECT 'y' UNION SELECT 1" },
      { sql: "SELECT 1 AS n UNION (SELECT NULL UNION SELECT '3') ORDER BY n" },
      { sql: "(SELECT NULL AS n UNION SELECT NULL) UNION SELECT 1 ORDER BY n" },
      { sql: "SELECT 1 AS n UNION SELECT NULL UNION SELECT '3' ORDER BY n" },
      { sql: "SELECT 'a' AS n UNION SELECT 1" },
      { sql: "SELECT CAST(NULL AS text) AS n UNION SELECT 1" },
    ]);
  });
});

/**
 * R24.2: a FULL JOIN with no hash- or merge-joinable condition is an intended extension.
 * PostgreSQL refuses it (0A000) for an implementation reason; its result is still defined, and is
 * what PostgreSQL returns for the equivalent rewrite: the LEFT JOIN, UNION ALL the right side's
 * rows that match no left row, NULL-extended. Registered as an R21 divergence once the registry
 * (next/testing/divergences.ts, on rebuild/write-p2a) is on main.
 */
describe("FULL JOIN without a joinable condition (R24.2)", () => {
  it("matches PostgreSQL's LEFT JOIN UNION ALL anti-join rewrite", async () => {
    const next = await check(REVIEW, []);
    const shapes: ReadonlyArray<{ on: string; order: string }> = [
      { on: "a.id < b.id", order: "1 NULLS LAST, 2 NULLS LAST" },
      { on: "a.v > b.v", order: "1 NULLS LAST, 2 NULLS LAST" },
      { on: "a.id = b.id OR a.v < b.v", order: "1 NULLS LAST, 2 NULLS LAST" },
      { on: "a.id + 1 < b.id AND a.v IS NOT NULL", order: "1 NULLS LAST, 2 NULLS LAST" },
    ];
    for (const { on, order } of shapes) {
      const sql = `SELECT a.id, b.v FROM a FULL JOIN b ON ${on} ORDER BY ${order}`;
      expect(await pgOutcome(pg, sql), `${sql} in PostgreSQL`).toEqual({ error: "0A000" });
      const rewrite =
        `SELECT a.id, b.v FROM a LEFT JOIN b ON ${on} ` +
        `UNION ALL SELECT NULL::integer, b.v FROM b WHERE NOT EXISTS (SELECT 1 FROM a WHERE ${on}) ` +
        `ORDER BY ${order}`;
      expect(nextOutcome(next, sql), sql).toEqual(await pgOutcome(pg, rewrite));
    }
  });
});

// A unique order key (id), peer groups with gaps and a NULL (g), and a NULL value (x).
const FRAMES = [
  "CREATE TABLE w (id integer PRIMARY KEY, g integer, x integer)",
  "INSERT INTO w VALUES (1, 1, 4), (2, 1, 7), (3, 2, NULL), (4, 3, 2), (5, 3, 9), (6, 3, 1), (7, 5, 6), (8, 8, 3), (9, 8, 8), (10, NULL, 5)",
];

// AVG over integers is double precision here (ruling C11), numeric in PostgreSQL: the moving
// averages average a double precision column. A constant non-positive ntile / nth_value argument
// is refused by the front end before execution (no 22014 / 22016 to compare).
describe("window frames and functions against PostgreSQL (review B6-p2b finding 6)", () => {
  it("ROWS frames: moving averages and sums, empty frames", async () => {
    await check(FRAMES, [
      {
        sql: "SELECT id, avg(x::double precision) OVER (ORDER BY id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS m FROM w ORDER BY id",
      },
      {
        sql: "SELECT id, sum(x) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS s FROM w ORDER BY id",
      },
      {
        sql: "SELECT id, sum(x) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS s FROM w ORDER BY id",
      },
      {
        sql: "SELECT id, count(*) OVER (ORDER BY id ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS c FROM w ORDER BY id",
      },
      {
        sql: "SELECT id, max(x) OVER (ORDER BY id DESC ROWS BETWEEN 3 FOLLOWING AND 5 FOLLOWING) AS m FROM w ORDER BY id",
      },
      {
        sql: "SELECT id, avg(x::double precision) OVER (PARTITION BY g % 2 ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS m FROM w ORDER BY id",
      },
      {
        sql: "SELECT id, first_value(x) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS f, last_value(x) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS l, nth_value(x, 2) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS n FROM w ORDER BY id",
      },
    ]);
  });

  it("RANGE and GROUPS frames over peers, with offsets and NULL keys", async () => {
    await check(FRAMES, [
      {
        sql: "SELECT id, sum(x) OVER (ORDER BY g RANGE BETWEEN CURRENT ROW AND CURRENT ROW) AS s FROM w ORDER BY id",
      },
      {
        sql: "SELECT id, sum(x) OVER (ORDER BY g RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS s FROM w ORDER BY id",
      },
      {
        sql: "SELECT id, count(*) OVER (ORDER BY g DESC RANGE BETWEEN 2 PRECEDING AND CURRENT ROW) AS c FROM w ORDER BY id",
      },
      {
        sql: "SELECT id, count(x) OVER (ORDER BY g NULLS FIRST RANGE BETWEEN UNBOUNDED PRECEDING AND 1 FOLLOWING) AS c FROM w ORDER BY id",
      },
      {
        sql: "SELECT id, sum(x) OVER (ORDER BY g GROUPS BETWEEN 1 PRECEDING AND CURRENT ROW) AS s FROM w ORDER BY id",
      },
      {
        sql: "SELECT id, count(*) OVER (ORDER BY g GROUPS BETWEEN 1 FOLLOWING AND 2 FOLLOWING) AS c FROM w ORDER BY id",
      },
    ]);
  });

  it("EXCLUDE CURRENT ROW, GROUP and TIES", async () => {
    await check(FRAMES, [
      {
        sql: "SELECT id, sum(x) OVER (ORDER BY g ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE CURRENT ROW) AS s FROM w ORDER BY id",
      },
      {
        sql: "SELECT id, sum(x) OVER (ORDER BY g RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE GROUP) AS s FROM w ORDER BY id",
      },
      {
        sql: "SELECT id, count(*) OVER (ORDER BY g GROUPS BETWEEN 1 PRECEDING AND 1 FOLLOWING EXCLUDE TIES) AS c FROM w ORDER BY id",
      },
    ]);
  });

  it("ntile, percent_rank, cume_dist and nth_value", async () => {
    await check(FRAMES, [
      { sql: "SELECT id, ntile(3) OVER (ORDER BY id) AS t FROM w ORDER BY id" },
      { sql: "SELECT id, ntile(4) OVER (ORDER BY id DESC) AS t FROM w ORDER BY id" },
      { sql: "SELECT id, ntile(20) OVER (ORDER BY id) AS t FROM w ORDER BY id" },
      { sql: "SELECT id, ntile(2) OVER (PARTITION BY g % 2 ORDER BY id) AS t FROM w ORDER BY id" },
      {
        sql: "SELECT id, percent_rank() OVER (ORDER BY g) AS p, cume_dist() OVER (ORDER BY g) AS c FROM w ORDER BY id",
      },
      { sql: "SELECT id, nth_value(x, 3) OVER (ORDER BY id) AS n FROM w ORDER BY id" },
    ]);
    // The review's probes, on its fixture.
    await check(REVIEW, [
      { sql: "SELECT id, ntile(2) OVER (ORDER BY v) AS n FROM b ORDER BY v NULLS LAST" },
      { sql: "SELECT id, percent_rank() OVER (ORDER BY v) AS n FROM b ORDER BY v NULLS LAST" },
      { sql: "SELECT id, cume_dist() OVER (ORDER BY v) AS n FROM b ORDER BY v NULLS LAST" },
      { sql: "SELECT id, nth_value(v, 2) OVER (ORDER BY v) AS n FROM b ORDER BY v NULLS LAST" },
      {
        sql: "SELECT id, v, sum(v) OVER (ORDER BY v ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS s FROM b ORDER BY v NULLS LAST, id",
      },
    ]);
  });

  it("integer to decimal casts", async () => {
    await check(REVIEW, [
      { sql: "SELECT 1 AS n UNION ALL SELECT 1.5 ORDER BY n" },
      { sql: "SELECT id FROM b WHERE v >= 1.5 AND v <= 2.5 ORDER BY id" },
      { sql: "SELECT CAST(v AS numeric(4, 1)) AS d FROM b WHERE v IS NOT NULL ORDER BY v" },
      { sql: "SELECT CAST(id * 1000 AS numeric(3, 0)) AS d FROM a" },
    ]);
  });
});
