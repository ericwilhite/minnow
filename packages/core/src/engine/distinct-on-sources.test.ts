/**
 * DISTINCT ON keys against ORDER BY when several sources have the same column names (review B6
 * stream A 2, finding 4): `a.id + 0` and `b.id + 0` are different targets even though both read
 * a column `id`, so a DISTINCT ON over one and an ORDER BY over the other is PostgreSQL's 42P10.
 * Every key list × ORDER BY list runs in PGlite and Minnow: both refuse, or both return the same
 * rows.
 */
import { PGlite } from "@electric-sql/pglite";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { MemoryBlockStore } from "../storage/index.js";
import { createReach, REFUSAL_STATES } from "../testing/reach.js";
import { MinnowDatabase } from "./database.js";

let pg: PGlite;

beforeAll(async () => {
  pg = new PGlite();
  await pg.waitReady;
}, 60_000);

afterAll(async () => {
  await pg.close();
});

const SETUP = [
  "CREATE TABLE a (id integer, v integer)",
  "CREATE TABLE b (id integer, v integer)",
  // The review's data: a ids 1, 2 and b ids 2, 3.
  "INSERT INTO a VALUES (1, 10), (2, 20)",
  "INSERT INTO b VALUES (2, 5), (3, 7)",
];

/** Two sources with the same column names: two tables, or one table under two aliases. */
const SOURCES: ReadonlyArray<readonly [from: string, left: string, right: string]> = [
  ["a CROSS JOIN b", "a", "b"],
  ["a x CROSS JOIN a y", "x", "y"],
  ["a JOIN b ON b.v < a.v", "a", "b"],
];

describe("DISTINCT ON over sources with the same column names", () => {
  it("never matches a key to an ORDER BY item of another source, as PostgreSQL", async () => {
    const database = new MinnowDatabase(new MemoryBlockStore());
    for (const sql of SETUP) {
      await database.execute(sql);
      await pg.exec(sql);
    }
    const reach = createReach<string>("DISTINCT ON sources", REFUSAL_STATES);
    const mismatches: string[] = [];
    let cases = 0;
    try {
      for (const [from, l, r] of SOURCES) {
        const keys = [`${l}.id + 0`, `${r}.id + 0`, `${l}.id % 2`, `${r}.id % 2`, `${l}.v`];
        const pool = [`${l}.id + 0`, `${r}.id + 0`, `${l}.id % 2`, `${r}.id % 2`, `${r}.v DESC`];
        const orders: string[][] = [];
        for (const first of pool) {
          orders.push([first]);
          for (const second of pool.filter((item) => item !== first)) orders.push([first, second]);
        }
        for (const key of keys)
          for (const order of orders) {
            cases += 1;
            reach.beginHistory();
            const sql = `SELECT DISTINCT ON (${key}) ${l}.id AS lid, ${r}.id AS rid FROM ${from} ORDER BY ${order.join(", ")}, ${l}.id, ${r}.id`;
            let expected: string;
            try {
              expected = JSON.stringify((await pg.query(sql)).rows);
            } catch (error) {
              expected = `error ${String((error as { code?: string }).code)}`;
            }
            let actual: string;
            try {
              actual = JSON.stringify((await database.query(sql)).rows);
            } catch (error) {
              actual = String(error).includes(
                "SELECT DISTINCT ON expressions must match initial ORDER BY expressions",
              )
                ? "error 42P10"
                : `error ${String(error)}`;
            }
            if (actual === expected)
              reach.reach(expected.startsWith("error") ? "refused" : "accepted");
            else mismatches.push(`${sql}\n  pglite: ${expected}\n  minnow: ${actual}`);
          }
      }
    } finally {
      await database.close();
    }
    expect(
      mismatches.slice(0, 8).join("\n"),
      `${String(mismatches.length)} of ${String(cases)} differ`,
    ).toBe("");
    reach.assertReached();
  });
});
