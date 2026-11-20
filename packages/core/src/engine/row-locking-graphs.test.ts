/** CTE-reference admission follows PostgreSQL refcounts, not execution-result reachability. */
import { PGlite } from "@electric-sql/pglite";
import { afterAll, beforeAll, expect, it } from "vitest";
import { createReach, ROW_LOCK_GRAPH_STATES } from "../testing/reach.js";
import { GRAPH_DDL, rowLockGraphs } from "../testing/row-locking-graphs.test.fixture.js";
import { MemoryBlockStore } from "../storage/index.js";
import { MinnowDatabase } from "./database.js";
import { compileQuery, clonePlanTree } from "./query.js";
let pg = new PGlite();
const reach = createReach<string>("shipped locking CTE graph", ROW_LOCK_GRAPH_STATES);
const cases = rowLockGraphs();
const databases = Array.from({ length: 8 }, () => new MinnowDatabase(new MemoryBlockStore()));
const checked = new Uint8Array(cases.length);
const blocks = Array.from({ length: Math.ceil(cases.length / 512) }, (_, index) => ({
  first: index * 512,
  end: Math.min((index + 1) * 512, cases.length),
}));
beforeAll(async () => {
  for (const sql of GRAPH_DDL) await pg.exec(sql);
  for (const [index, db] of databases.entries())
    for (const sql of index < 4 ? GRAPH_DDL : GRAPH_DDL.slice(0, 2)) await db.execute(sql);
});
afterAll(async () => {
  await pg.close();
  for (const db of databases) await db.close();
});
function outcome(error: unknown): string {
  if (typeof error === "object" && error !== null && "code" in error) return String(error.code);
  return String(error).includes("in a read-only transaction") ? "25006" : String(error);
}
// Keep the original 512-case PostgreSQL lifetimes, but give each independent block its
// own unchanged deadline. Read-only Minnow fixtures are built once and shared by all blocks.
it.each(blocks)(
  "refuses the same CTE dependency graphs as PostgreSQL through SQL and copied typed plans [$first,$end)",
  async ({ first, end }) => {
    const failures: string[] = [];
    if (first > 0) {
      await pg.close();
      pg = new PGlite();
      for (const sql of GRAPH_DDL) await pg.exec(sql);
    }
    for (let caseNumber = first; caseNumber < end; caseNumber++) {
      const test = cases[caseNumber];
      if (test === undefined) throw new Error("Missing graph case");
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
      const emptyCatalog = test.states.includes("direct");
      const offset = emptyCatalog ? 4 : 0;
      for (const [entry, db] of databases.slice(offset, offset + 4).entries()) {
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
        checked[caseNumber] = (checked[caseNumber] ?? 0) + 1;
        if (actual !== expected)
          failures.push(`${String(entry)}: ${test.sql}\nPG ${expected}; Minnow ${actual}`);
        else {
          for (const state of test.states) reach.reach(state);
          reach.reach(emptyCatalog ? "empty view catalog" : "nonempty view catalog");
        }
      }
    }
    expect(failures.join("\n")).toBe("");
  },
  120_000,
);

it("checks all 5,568 graph shapes through all four entry points", () => {
  expect(cases.length).toBe(5568);
  expect([...checked].every((count) => count === 4)).toBe(true);
  reach.assertReached();
});
