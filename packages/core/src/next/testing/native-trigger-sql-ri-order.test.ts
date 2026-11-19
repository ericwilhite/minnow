/** System actions follow FK creation order within their reserved name family; user
 * names sort around it. PG executes its own functions and RI actions as authority. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, NATIVE_TRIGGER_SQL_RI_ORDER_REACH } from "../../testing/reach.js";
import { MinnowDatabaseClient } from "../owner/database-client.js";
import { publicOwner } from "./public-owner.test.fixture.js";
const capBytes = (process.env.MINNOW_TRIGGER_CAP_MIB === "32" ? 32 : 64) << 20;
for (const seed of seedsFor("next-native-trigger-sql-ri-order", [49543, 49544]))
  it(`orders complete multi-FK actions and user triggers (${String(seed)})`, async () => {
    const pg = new PGlite(),
      reach = createReach("multi-FK SQL order", NATIVE_TRIGGER_SQL_RI_ORDER_REACH);
    try {
      await fc.assert(
        fc.asyncProperty(
          fc.integer({ min: 1, max: 16 }),
          fc.boolean(),
          fc.integer({ min: 1, max: 2 }),
          fc.boolean(),
          async (count, update, rows, published) => {
            reach.beginHistory();
            const owner = await publicOwner({ capBytes }),
              db = new MinnowDatabaseClient(owner.client, { ready: Promise.resolve() });
            try {
              await pg.exec("DROP SCHEMA public CASCADE;CREATE SCHEMA public");
              const event = update ? "UPDATE" : "DELETE";
              for (const sql of [
                "CREATE TABLE p(id INT PRIMARY KEY)",
                "CREATE TABLE trace(id INT PRIMARY KEY,v TEXT)",
                "INSERT INTO trace VALUES(1,'')",
                `INSERT INTO p SELECT n FROM generate_series(1,${String(rows)})g(n)`,
              ]) {
                await db.execute(sql);
                await pg.exec(sql);
              }
              const trigger = async (
                name: string,
                table: string,
                timing: string,
                label: string,
                source: string,
              ) => {
                const body = `UPDATE trace SET v=v || '${label}:' || CAST(${source} AS TEXT) || '|' WHERE id=1`;
                await db.execute(
                  `CREATE TRIGGER "${name}" ${timing} ${event} ON ${table} BEGIN ${body}; END`,
                );
                await pg.exec(
                  `CREATE FUNCTION f_${name}() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN ${body}; RETURN ${update ? "NEW" : "OLD"}; END $$; CREATE TRIGGER "${name}" ${timing} ${event} ON ${table} FOR EACH ROW EXECUTE FUNCTION f_${name}()`,
                );
              };
              await trigger("A_first", "p", "AFTER", "pFirst", "OLD.id");
              await trigger("z_last", "p", "AFTER", "pLast", "OLD.id");
              for (let i = 1; i <= count; i++) {
                const table = `c${String(i)}`;
                for (const sql of [
                  `CREATE TABLE ${table}(id INT PRIMARY KEY,pid INT REFERENCES p(id) ON DELETE CASCADE ON UPDATE CASCADE)`,
                  `INSERT INTO ${table} SELECT n,n FROM generate_series(1,${String(rows)})g(n)`,
                ]) {
                  await db.execute(sql);
                  await pg.exec(sql);
                }
                await trigger(`b${String(i)}`, table, "BEFORE", `${table}B`, "OLD.pid");
                await trigger(`a${String(i)}`, table, "AFTER", `${table}A`, "OLD.pid");
              }
              if (published) {
                await owner.core.harness.path.recycleLog();
                await owner.core.harness.path.quiesce();
              }
              const sql = update ? "UPDATE p SET id=id+100" : "DELETE FROM p";
              await pg.exec(sql);
              expect((await db.execute(sql)).affectedRows).toBe(rows);
              expect(
                (await db.query("SELECT v FROM trace")).rows(),
                "PG action and user name order",
              ).toEqual((await pg.query("SELECT v FROM trace")).rows);
              for (const sql of [
                "SELECT * FROM p ORDER BY id",
                ...Array.from(
                  { length: count },
                  (_, i) => `SELECT * FROM c${String(i + 1)} ORDER BY id`,
                ),
              ])
                expect((await db.query(sql)).rows(), sql).toEqual((await pg.query(sql)).rows);
              reach.reach(
                count === 1 ? "one FK" : count >= 10 ? "IDs cross decimal width" : "several FKs",
              );
              reach.reach(update ? "UPDATE CASCADE" : "DELETE CASCADE");
              reach.reach(published ? "published" : "resident");
              reach.reach("user names before and after RI");
              reach.reach("child BEFORE and deferred AFTER");
              reach.reach("exact rows and trace");
            } finally {
              await db.close();
              await owner.close();
            }
            expect(owner.core.harness.governor.usedBytes).toBe(0);
            expect(owner.core.spill.bytesInUse).toBe(0);
            reach.reach("zero ownership");
          },
        ),
        {
          seed,
          numRuns: 4,
          examples: [
            [1, false, 1, false],
            [4, true, 1, false],
            [12, true, 2, true],
            [16, false, 1, true],
          ],
        },
      );
      reach.assertReached();
    } finally {
      await pg.close();
    }
  }, 120000);
