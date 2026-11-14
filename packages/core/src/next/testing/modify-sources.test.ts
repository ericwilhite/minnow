/** PostgreSQL UPDATE FROM / DELETE USING touch a target once, with statement snapshots. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, MODIFY_SOURCES_REACH } from "../../testing/reach.js";
import { currentEngine } from "../../testing/differential-current.js";
import { StorageCore } from "./storage-core.js";
import { oracleRows } from "./differential-adapter.js";

it.each(seedsFor("next-modify-sources", [0xb5b3, 0xb5b4]))(
  "modifies joined targets once (seed %i)",
  async (seed) => {
    const core = await new StorageCore().open(),
      pg = new PGlite(),
      shipped = await currentEngine("memory");
    await pg.waitReady;
    const reach = createReach("modify sources", MODIFY_SOURCES_REACH);
    const all = async (sql: string) => {
      const result = await core.execute(sql);
      await pg.exec(sql);
      await shipped.run({ kind: "sql", sql });
      return result;
    };
    const compare = async (name: string) => {
      const sql = `SELECT * FROM ${name} ORDER BY id`;
      const result = await core.execute(sql);
      if (result.kind !== "rows" || !("batches" in result)) throw new Error("rows");
      const rows = oracleRows(result);
      expect(rows).toEqual((await pg.query(sql)).rows);
      expect((await shipped.run({ kind: "sql", sql })).rows).toEqual(rows);
    };
    try {
      await all("CREATE TABLE target (id INTEGER PRIMARY KEY, v INTEGER)");
      await all("CREATE TABLE source (id INTEGER PRIMARY KEY, link INTEGER, delta INTEGER)");
      await all(
        "CREATE TABLE child (id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES target(id) ON DELETE CASCADE ON UPDATE CASCADE)",
      );
      await fc.assert(
        fc.asyncProperty(fc.integer({ min: 1, max: 20 }), async (delta) => {
          reach.beginHistory();
          await all("DELETE FROM child");
          await all("DELETE FROM target");
          await all("DELETE FROM source");
          await all("INSERT INTO target VALUES (1,10),(2,20),(3,30)");
          await all("INSERT INTO child VALUES (11,1),(12,2),(13,3)");
          await all(
            `INSERT INTO source VALUES (1,1,${String(delta)}),(2,1,${String(delta)}),(3,2,${String(delta)})`,
          );
          const updated = await all(
            "UPDATE target SET v = target.v + source.delta FROM source WHERE target.id = source.link",
          );
          expect("rowCount" in updated ? updated.rowCount : null).toBe(2);
          reach.reach("UPDATE FROM duplicate matches");
          await compare("target");
          const deleted = await all(
            "DELETE FROM target USING source WHERE target.id = source.link",
          );
          expect("rowCount" in deleted ? deleted.rowCount : null).toBe(2);
          reach.reach("DELETE USING duplicate matches");
          reach.reach("delete cascade");
          await compare("target");
          await compare("child");
          await expect(core.execute("INSERT INTO child VALUES (14,999)")).rejects.toMatchObject({
            sqlState: "23503",
          });
          reach.reach("missing parent rollback");
          await compare("child");
        }),
        { seed, numRuns: 12 },
      );
      reach.assertReached();
    } finally {
      await core.close();
      await shipped.close();
      await pg.close();
    }
  },
  60000,
);
