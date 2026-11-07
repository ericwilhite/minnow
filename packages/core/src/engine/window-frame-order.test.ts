/** PostgreSQL's frame-bound ordering is syntax, distinct from a legal empty frame. */
import { PGlite } from "@electric-sql/pglite";
import { expect, it } from "vitest";
import { MinnowDatabase } from "./database.js";
import { MemoryBlockStore } from "../storage/index.js";
import { createReach, WINDOW_FRAME_ORDER_REACH } from "../testing/reach.js";
import { mulberry32, seedsFor } from "../testing/seeds.js";

const reach = createReach("SQL frame bound order", WINDOW_FRAME_ORDER_REACH, { minimum: 2 });
for (const seed of seedsFor("sql-window-frame-order", [0x7741, 0x7742]))
  it(`refuses reversed bound kinds but permits empty members, seed ${String(seed)}`, async () => {
    const pg = new PGlite();
    await pg.waitReady;
    const current = new MinnowDatabase(new MemoryBlockStore());
    const random = mulberry32(seed);
    reach.beginHistory();
    try {
      const ddl = "CREATE TABLE f(id INTEGER PRIMARY KEY)";
      await pg.exec(ddl);
      await current.execute(ddl);
      const width = 1 + Math.floor(random() * 9);
      for (const unit of ["ROWS", "RANGE", "GROUPS"] as const) {
        for (const [state, start, end] of [
          ["current then preceding", "CURRENT ROW", `${String(width)} PRECEDING`],
          ["following then current", `${String(width)} FOLLOWING`, "CURRENT ROW"],
          ["following then preceding", `${String(width)} FOLLOWING`, `${String(width)} PRECEDING`],
        ] as const) {
          const sql = `SELECT COUNT(*)OVER(ORDER BY id ${unit} BETWEEN ${start} AND ${end})AS n FROM f`;
          await expect(pg.query(sql)).rejects.toMatchObject({ code: "42P20" });
          await expect(current.query(sql)).rejects.toThrow(/frame/u);
          if (state === "following then current") {
            const shorthand = `SELECT COUNT(*)OVER(ORDER BY id ${unit} ${start})AS n FROM f`;
            await expect(pg.query(shorthand)).rejects.toMatchObject({ code: "42P20" });
            await expect(current.query(shorthand)).rejects.toThrow(/frame/u);
            reach.reach("shorthand");
          }
          reach.reach(unit);
          reach.reach(state);
        }
      }
      await pg.exec("INSERT INTO f VALUES(1),(2)");
      await current.insertBatch("f", [{ id: 1 }, { id: 2 }]);
      const sql =
        "SELECT COUNT(*)OVER(ORDER BY id ROWS BETWEEN 1 PRECEDING AND 5 PRECEDING)AS n FROM f ORDER BY id";
      expect((await pg.query(sql)).rows).toEqual([{ n: 0 }, { n: 0 }]);
      expect((await current.query(sql)).rows).toEqual([{ n: 0 }, { n: 0 }]);
      reach.reach("allowed empty");
    } finally {
      await pg.close();
      await current.close();
    }
  }, 120_000);
it("reaches each frame-order state", () => reach.assertReached());
