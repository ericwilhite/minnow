/** PG WindowAgg evaluates a frame only when its output row is demanded. */
import { PGlite } from "@electric-sql/pglite";
import { expect, it } from "vitest";
import { seedsFor, mulberry32 } from "../../testing/seeds.js";
import { createReach, WINDOW_FLOAT_SQL_DEMAND_REACH } from "../../testing/reach.js";
import { StorageCore } from "./storage-core.js";
import { oracleRows } from "./differential-adapter.js";

const state = (e: unknown): unknown => {
  if (typeof e === "object" && e !== null) {
    if ("sqlState" in e) return e.sqlState;
    if ("code" in e) return e.code;
  }
  throw e;
};
for (const seed of seedsFor("next-window-float-sql-demand", [0xb792, 0xb793]))
  it(`preserves SQL LIMIT, OFFSET and sorting demand (${String(seed)})`, async () => {
    const reach = createReach(`float SQL demand ${String(seed)}`, WINDOW_FLOAT_SQL_DEMAND_REACH);
    const core = await new StorageCore().open();
    const pg = new PGlite();
    const random = mulberry32(seed);
    try {
      await pg.waitReady;
      const ddl = "CREATE TABLE w(id INTEGER PRIMARY KEY, x DOUBLE PRECISION)";
      await core.execute(ddl);
      await pg.exec(ddl);
      await core.writeBatch("insertBatch", "w", [
        { id: 0, x: 1e308 },
        { id: 1, x: 1e308 },
        { id: 2, x: NaN },
      ]);
      await pg.exec("INSERT INTO w VALUES(0,1e308),(1,1e308),(2,'NaN')");
      for (const fn of ["sum", "avg"])
        for (const frame of [
          "ROWS UNBOUNDED PRECEDING",
          "ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING",
        ])
          for (const tail of [
            "LIMIT 0",
            "LIMIT 1",
            "LIMIT 1 OFFSET 1",
            "LIMIT 2",
            "ORDER BY id LIMIT 1",
            "ORDER BY id DESC LIMIT 1",
            "ORDER BY v LIMIT 1",
          ]) {
            reach.beginHistory();
            reach.reach("LIMIT");
            if (tail.includes("OFFSET")) reach.reach("OFFSET");
            if (tail.includes("ORDER BY id LIMIT")) reach.reach("existing order");
            if (tail.includes("ORDER BY v")) reach.reach("result sort");
            if (frame.includes("FOLLOWING")) reach.reach("whole partition");
            const sql = `SELECT ${fn}(x) OVER(ORDER BY id ${frame}) v FROM w ${tail}`;
            let expected;
            try {
              expected = {
                rows: (await pg.query<Record<string, unknown>>(sql)).rows.map((r) =>
                  Object.values(r),
                ),
              };
            } catch (error) {
              expected = { error: state(error) };
            }
            let actual;
            try {
              const result = await core.execute(sql);
              if (!("batches" in result)) throw Error("query");
              actual = { rows: oracleRows(result).map((r) => Object.values(r)) };
            } catch (error) {
              actual = { error: state(error) };
            }
            expect(actual, sql).toEqual(expected);
          }
      reach.reach("top-k filter");
      for (let n = 0; n < 3; n++) {
        const fn = random() < 0.5 ? "sum" : "avg";
        const sql = `SELECT v FROM(SELECT ${fn}(x) OVER(ORDER BY id ROWS UNBOUNDED PRECEDING) v,row_number() OVER(ORDER BY id) rn FROM w)t WHERE rn<=1 LIMIT 1`;
        const expected = (await pg.query<Record<string, unknown>>(sql)).rows.map((r) =>
          Object.values(r),
        );
        const actual = await core.execute(sql);
        if (!("batches" in actual)) throw Error("query");
        expect(oracleRows(actual).map((r) => Object.values(r))).toEqual(expected);
      }
      reach.assertReached();
    } finally {
      await core.close();
      await pg.close();
    }
  }, 60000);
