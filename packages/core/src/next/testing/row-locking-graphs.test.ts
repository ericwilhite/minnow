/** PostgreSQL plans a SELECT CTE when its reference count is positive, even from an unused referrer. */
import { PGlite } from "@electric-sql/pglite";
import { afterAll, beforeAll, expect, it } from "vitest";
import { createReach, ROW_LOCK_GRAPH_STATES } from "../../testing/reach.js";
import { GRAPH_DDL, rowLockGraphs } from "../../testing/row-locking-graphs.test.fixture.js";
import { clonePlanTree } from "../../engine/query.js";
import { parseQuery, prepareQuery } from "../lower/frontend.js";
import { LoweredDatabase } from "./lowering-harness.js";
import { pgOutcome, nextOutcome } from "./pglite-oracle.js";
const pg = new PGlite();
const next = new LoweredDatabase();
beforeAll(async () => {
  for (const sql of GRAPH_DDL) {
    await pg.exec(sql);
    next.execute(sql);
  }
});
afterAll(async () => {
  await pg.close();
});
const INTENT = {
  "KEY SHARE": "keyShare",
  SHARE: "share",
  "NO KEY UPDATE": "noKeyUpdate",
  UPDATE: "update",
};
it("preserves refcounted CTE graph locks, views and strongest intent before execution pruning", async () => {
  const reach = createReach<string>("locking CTE graph", ROW_LOCK_GRAPH_STATES);
  const failures: string[] = [];
  const cases = rowLockGraphs();
  expect(cases.length).toBe(3408);
  for (const test of cases) {
    reach.beginHistory();
    await pg.exec("BEGIN READ ONLY");
    const expected = await pgOutcome(pg, test.sql);
    await pg.exec("ROLLBACK");
    const lock = next.lower(test.sql).rowLocking;
    const actual = lock === "none" ? nextOutcome(next, test.sql) : { error: "25006" };
    const wanted = test.lock === undefined ? "none" : INTENT[test.lock];
    if (JSON.stringify(expected) !== JSON.stringify(actual) || lock !== wanted)
      failures.push(
        `${test.sql}\nPG ${JSON.stringify(expected)}; intent ${lock} wanted ${wanted}; next ${JSON.stringify(actual)}`,
      );
    else for (const state of test.states) reach.reach(state);
    // Compiled plans cross the worker boundary as plain data; both supported copy paths count.
    for (const copy of [clonePlanTree, structuredClone]) {
      const copied = prepareQuery(copy(parseQuery(test.sql)), next.context().catalog);
      if (copied.rowLocking !== test.lock)
        failures.push(
          `copied: ${test.sql}\n${String(copied.rowLocking)} wanted ${String(test.lock)}`,
        );
    }
  }
  expect(failures.join("\n")).toBe("");
  reach.assertReached();
});
