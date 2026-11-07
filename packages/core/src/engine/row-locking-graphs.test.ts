/** CTE-reference admission follows PostgreSQL refcounts, not execution-result reachability. */
import { PGlite } from "@electric-sql/pglite";
import { afterAll, beforeAll, expect, it } from "vitest";
import { createReach, ROW_LOCK_GRAPH_STATES } from "../testing/reach.js";
import { GRAPH_DDL, rowLockGraphs } from "../testing/row-locking-graphs.test.fixture.js";
import { MemoryBlockStore } from "../storage/index.js";
import { MinnowDatabase } from "./database.js";
import { compileQuery, clonePlanTree } from "./query.js";
const pg = new PGlite();
beforeAll(async () => {
  for (const sql of GRAPH_DDL) await pg.exec(sql);
});
afterAll(async () => {
  await pg.close();
});
function outcome(error: unknown): string {
  if (typeof error === "object" && error !== null && "code" in error) return String(error.code);
  return String(error).includes("in a read-only transaction") ? "25006" : String(error);
}
it("refuses the same CTE dependency graphs as PostgreSQL through SQL and copied typed plans", async () => {
  const reach = createReach<string>("shipped locking CTE graph", ROW_LOCK_GRAPH_STATES);
  const failures: string[] = [];
  const databases = Array.from({ length: 4 }, () => new MinnowDatabase(new MemoryBlockStore()));
  for (const db of databases) for (const sql of GRAPH_DDL) await db.execute(sql);
  const cases = rowLockGraphs();
  expect(cases.length).toBe(3408);
  try {
    for (const test of cases) {
      await pg.exec("BEGIN READ ONLY");
      let expected = "rows";
      try {
        await pg.query(test.sql);
      } catch (error) {
        expected = outcome(error);
      } finally {
        await pg.exec("ROLLBACK");
      }
      expect(expected, test.sql).toBe(test.lock === undefined ? "rows" : "25006");
      for (const [entry, db] of databases.entries()) {
        reach.beginHistory();
        await db.execute("BEGIN READ ONLY");
        let actual = "rows";
        try {
          if (entry === 0) await db.execute(test.sql);
          else if (entry === 1) await db.query(test.sql);
          else
            await db.run({
              kind: "typed-query",
              plan:
                entry === 2
                  ? clonePlanTree(compileQuery(test.sql))
                  : structuredClone(compileQuery(test.sql)),
            });
        } catch (error) {
          actual = outcome(error);
        } finally {
          await db.execute("ROLLBACK");
        }
        if (actual !== expected)
          failures.push(`${String(entry)}: ${test.sql}\nPG ${expected}; Minnow ${actual}`);
        else for (const state of test.states) reach.reach(state);
      }
    }
    expect(failures.join("\n")).toBe("");
    reach.assertReached();
  } finally {
    for (const db of databases) await db.close();
  }
});
