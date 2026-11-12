/** windowfuncs.c leadlag_common reads the current row's offset only when requested. */
import { PGlite } from "@electric-sql/pglite";
import { expect, it } from "vitest";
import { seedsFor, mulberry32 } from "../../testing/seeds.js";
import { createReach, WINDOW_OFFSET_DEMAND_REACH } from "../../testing/reach.js";
import { StorageCore } from "./storage-core.js";
import { oracleRows } from "./differential-adapter.js";
const code = (error: unknown): unknown => {
  if (typeof error === "object" && error !== null) {
    if ("sqlState" in error) return error.sqlState;
    if ("code" in error) return error.code;
  }
  throw error;
};
for (const seed of seedsFor("next-window-offset-demand", [0xb7a4, 0xb7a5]))
  it(`matches PG erroring offsets under partial demand (${String(seed)})`, async () => {
    const pg = new PGlite(),
      core = await new StorageCore().open();
    const reach = createReach(`offset demand ${String(seed)}`, WINDOW_OFFSET_DEMAND_REACH);
    const random = mulberry32(seed);
    try {
      await pg.waitReady;
      const ddl = "CREATE TABLE w(id INTEGER PRIMARY KEY,x DOUBLE PRECISION)";
      await pg.exec(ddl);
      await core.execute(ddl);
      const rows = Array.from({ length: 12 }, (_, id) => ({
        id,
        x: id < 3 ? 1e308 : Math.floor(random() * 100) - 50,
      }));
      await core.writeBatch("insertBatch", "w", rows);
      for (const row of rows) await pg.query("INSERT INTO w VALUES($1,$2)", [row.id, row.x]);
      const compare = async (sql: string) => {
        let expected, actual;
        try {
          expected = { rows: (await pg.query(sql)).rows };
        } catch (error) {
          expected = { error: code(error) };
        }
        try {
          const result = await core.execute(sql);
          if (!("batches" in result)) throw Error("query");
          actual = { rows: oracleRows(result) };
        } catch (error) {
          actual = { error: code(error) };
        }
        expect(actual, sql).toEqual(expected);
      };
      for (const size of [3, 12])
        for (const fn of ["lag", "lead"])
          for (const errorAt of [1, 3, 7].filter((row) => row < size))
            for (const tail of [
              "LIMIT 0",
              "LIMIT 1",
              `LIMIT ${String(errorAt)}`,
              `LIMIT ${String(errorAt + 1)}`,
              `LIMIT 1 OFFSET ${String(errorAt)}`,
            ]) {
              reach.beginHistory();
              reach.reach(fn === "lag" ? "lag" : "lead");
              reach.reach(
                tail === "LIMIT 0"
                  ? "zero demand"
                  : tail.includes("OFFSET") || tail === `LIMIT ${String(errorAt + 1)}`
                    ? "error demanded"
                    : "error after demand",
              );
              const sql = `SELECT ${fn}(x,10/(${String(errorAt)}-id),7) OVER(ORDER BY id) v FROM w WHERE id<${String(size)} ${tail}`;
              await compare(sql);
            }
      for (const fn of ["lag", "lead"])
        for (const offset of ["'bad'", "'2147483648'", "CAST('bad' AS INTEGER)"])
          for (const limit of [0, 1]) {
            reach.reach("offset coercion errors");
            await compare(
              `SELECT ${fn}(x,${offset},7) OVER(ORDER BY id) v FROM w LIMIT ${String(limit)}`,
            );
          }
      reach.assertReached();
    } finally {
      await core.close();
      await pg.close();
    }
  }, 60000);
