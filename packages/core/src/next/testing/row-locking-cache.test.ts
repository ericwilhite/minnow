/** A cached/parameterized point read must carry the same admission intent as its SQL text. */
import { PGlite } from "@electric-sql/pglite";
import { afterAll, beforeAll, expect, it } from "vitest";
import { createReach, ROW_LOCK_CACHE_STATES } from "../../testing/reach.js";
import { bindParameterized, type ParameterizedPlan } from "../lower/parameterize.js";
import { LoweredDatabase } from "./lowering-harness.js";
import { pgOutcome } from "./pglite-oracle.js";

const pg = new PGlite();
const next = new LoweredDatabase();
beforeAll(async () => {
  for (const sql of ["CREATE TABLE t (id integer PRIMARY KEY)", "INSERT INTO t VALUES (1), (2)"]) {
    await pg.exec(sql);
    next.execute(sql);
  }
});
afterAll(async () => {
  await pg.close();
});

it("retains row-lock intent in cached parameterized point reads", async () => {
  const reach = createReach("cached row locks", ROW_LOCK_CACHE_STATES);
  const families = new Map<string, ParameterizedPlan>();
  for (const [clause, intent] of [
    ["", "none"],
    [" FOR KEY SHARE", "keyShare"],
    [" FOR SHARE", "share"],
    [" FOR NO KEY UPDATE", "noKeyUpdate"],
    [" FOR UPDATE", "update"],
  ] as const) {
    let first: ParameterizedPlan | undefined;
    for (const id of [1, 2]) {
      reach.beginHistory();
      const sql = `SELECT * FROM t WHERE id = ${String(id)}${clause}`;
      const parameterized = next.lowerer.parameterize(sql);
      if (parameterized === null) throw new Error(`not parameterized: ${sql}`);
      let family = families.get(parameterized.key);
      if (family === undefined) {
        const lowered = next.lowerer.lowerParameterized(parameterized, next.context());
        if (lowered.kind !== "parameterized") throw new Error(`refused family: ${sql}`);
        family = lowered;
        families.set(parameterized.key, family);
      }
      if (first === undefined) first = family;
      else expect(family, sql).toBe(first);
      const statement = family.statement;
      if (statement.kind !== "query") throw new Error(`not a query: ${sql}`);
      expect(statement, sql).toMatchObject({
        kind: "query",
        pointRead: true,
        rowLocking: intent,
      });
      expect(next.lower(sql), sql).toMatchObject({ rowLocking: intent });
      await pg.exec("BEGIN READ ONLY");
      const expected = await pgOutcome(pg, sql);
      await pg.exec("ROLLBACK");
      expect("error" in expected ? expected.error : "rows", sql).toBe(
        intent === "none" ? "rows" : "25006",
      );
      expect(
        next.runPlan(statement, bindParameterized(family, parameterized, []), []),
        sql,
      ).toEqual([{ id }]);
      reach.reach(intent);
    }
  }
  // Each strength and the nonlocking text have their own family, while both literals share it.
  expect(families.size).toBe(5);
  reach.assertReached();
});
