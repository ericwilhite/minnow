/** PG enum_out is stable, but implicit generation assignment is a different coercion. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { CATALOG_GENERATED_ENUM_REACH, createReach } from "../../testing/reach.js";
import { LoweredDatabase } from "./lowering-harness.js";
const state = async (work: () => unknown) => {
  try {
    await work();
    return null;
  } catch (error) {
    if (error && typeof error === "object") {
      if ("sqlState" in error) return error.sqlState;
      if ("code" in error) return error.code;
    }
    throw error;
  }
};
for (const seed of seedsFor("next-catalog-generated-enum", [0xb79c, 0xb79d]))
  it(`keeps stable casts out of generation seed ${String(seed)}`, async () => {
    const pg = new PGlite(),
      reach = createReach("generated enum coercion", CATALOG_GENERATED_ENUM_REACH);
    try {
      await pg.exec("CREATE TYPE e AS ENUM('x','y')");
      await fc.assert(
        fc.asyncProperty(
          fc.boolean(),
          fc.constantFrom("text", "varchar"),
          fc.boolean(),
          async (explicit, target, defaultValue) => {
            reach.beginHistory();
            const native = new LoweredDatabase();
            native.execute("CREATE TYPE e AS ENUM('x','y')");
            const value = explicit ? `('x'::e)::${target}` : "'x'::e";
            const sql = `CREATE TABLE t(n INTEGER PRIMARY KEY,v ${target} ${defaultValue ? `DEFAULT ${value}` : `GENERATED ALWAYS AS (${value}) STORED`})`;
            try {
              const expected = await state(() => pg.exec(sql));
              expect(await state(() => native.execute(sql))).toBe(expected);
              expect(expected).toBe(explicit && !defaultValue ? "42P17" : null);
              if (expected === null) {
                await pg.exec("INSERT INTO t(n) VALUES(1)");
                native.execute("INSERT INTO t(n) VALUES(1)");
                expect(native.execute("SELECT * FROM t")).toEqual(
                  (await pg.query("SELECT * FROM t")).rows,
                );
                reach.reach(defaultValue ? "default allowed" : "assignment allowed");
              } else {
                expect(native.catalog.view.tableByName("t")).toBeUndefined();
                reach.reach("explicit stable refused");
              }
              reach.reach(target === "text" ? "text" : "varchar");
            } finally {
              await pg.exec("DROP TABLE IF EXISTS t");
            }
          },
        ),
        {
          seed,
          numRuns: 16,
          examples: [
            [true, "text", false],
            [false, "text", false],
            [true, "text", true],
            [true, "varchar", false],
            [false, "varchar", false],
          ],
        },
      );
      reach.assertReached();
    } finally {
      await pg.close();
    }
  }, 120_000);
