/** PostgreSQL evaluates generated expressions even for a later duplicate join match. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, MODIFY_DUPLICATE_RULES_REACH } from "../../testing/reach.js";
import { StorageCore } from "./storage-core.js";
import { oracleRows } from "./differential-adapter.js";
it.each(seedsFor("next-modify-duplicate-rules", [0xb5cb]))(
  "keeps duplicate-match expression errors before repeat suppression (seed %i)",
  async (seed) => {
    const core = await new StorageCore().open(),
      pg = new PGlite();
    await pg.waitReady;
    const reach = createReach("duplicate rule errors", MODIFY_DUPLICATE_RULES_REACH);
    try {
      for (const sql of [
        "CREATE TABLE duplicate_rule(id INTEGER PRIMARY KEY,den INTEGER,g INTEGER GENERATED ALWAYS AS(100/den) STORED)",
        "CREATE TABLE duplicate_source(sid INTEGER PRIMARY KEY,tid INTEGER,den INTEGER)",
      ]) {
        await core.execute(sql);
        await pg.exec(sql);
      }
      await fc.assert(
        fc.asyncProperty(
          fc.integer({ min: 1, max: 8 }),
          fc.integer({ min: 1, max: 4 }),
          async (n, den) => {
            reach.beginHistory();
            for (const sql of [
              "DELETE FROM duplicate_rule",
              "DELETE FROM duplicate_source",
              `INSERT INTO duplicate_rule(id,den) VALUES ${Array.from({ length: n }, (_, k) => `(${String(k + 1)},2)`).join(",")}`,
              `INSERT INTO duplicate_source VALUES ${Array.from({ length: 2 * n }, (_, k) => `(${String(k + 1)},${String((k % n) + 1)},${String(k < n ? den : 0)})`).join(",")}`,
            ]) {
              await core.execute(sql);
              await pg.exec(sql);
            }
            const sql =
              "UPDATE duplicate_rule SET den=duplicate_source.den FROM duplicate_source WHERE duplicate_rule.id=duplicate_source.tid RETURNING duplicate_rule.id,duplicate_rule.g";
            await expect(pg.query(sql)).rejects.toMatchObject({ code: "22012" });
            await expect(core.execute(sql)).rejects.toMatchObject({ sqlState: "22012" });
            const rows = await core.execute("SELECT * FROM duplicate_rule ORDER BY id");
            if (rows.kind !== "rows" || !("batches" in rows)) throw new Error("rows");
            expect(oracleRows(rows)).toEqual(
              (await pg.query("SELECT * FROM duplicate_rule ORDER BY id")).rows,
            );
            expect(core.spill.bytesInUse).toBe(0);
            reach.reach("ignored generated error");
            reach.reach("statement rollback");
            reach.reach("duplicate source values");
          },
        ),
        { seed, numRuns: 12 },
      );
      reach.assertReached();
    } finally {
      await core.close();
      await pg.close();
    }
    expect(core.harness.governor.usedBytes).toBe(0);
  },
  60000,
);
