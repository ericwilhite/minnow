/** Follow publishes its whole group to later AFTER bodies. Explicit CASCADE keeps PG's
 * per-action boundary. A supported UPDATE predicate observes the current child prefix. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, NATIVE_BODY_FOLLOW_GROUP_REACH } from "../../testing/reach.js";
import { valueAt } from "../exec/kernels/test-support.js";
import { referentialFixture } from "./referential.test-support.js";
async function code(run: () => Promise<unknown>): Promise<string | null> {
  try {
    await run();
    return null;
  } catch (error) {
    if (typeof error === "object" && error !== null) {
      if ("sqlState" in error && typeof error.sqlState === "string") return error.sqlState;
      if ("code" in error && typeof error.code === "string") return error.code;
    }
    throw error;
  }
}
for (const seed of seedsFor("next-native-body-follow-group", [0xb673, 0xb674]))
  it(`exposes complete follow groups and PG action boundaries to compiled AFTER seed ${String(seed)}`, async () => {
    const pg = new PGlite(),
      reach = createReach("compiled AFTER follow groups", NATIVE_BODY_FOLLOW_GROUP_REACH);
    try {
      await fc.assert(
        fc.asyncProperty(
          fc.integer({ min: 2, max: 6 }),
          fc.integer({ min: 1, max: 9 }),
          fc.boolean(),
          fc.boolean(),
          fc.boolean(),
          async (count, step, bad, published, explicit) => {
            reach.beginHistory();
            const shift = 1 + (step % (count - 1)),
              sum = (count * (count + 1)) / 2,
              setup = [
                "CREATE TABLE trace(id INT PRIMARY KEY,p INT,c INT)",
                "INSERT INTO trace VALUES(1,0,0)",
                "CREATE TABLE p(id INT PRIMARY KEY)",
                `CREATE TABLE c(id INT PRIMARY KEY,pid INT REFERENCES p(id)${explicit ? " ON UPDATE CASCADE" : ""},seen INT,g INT GENERATED ALWAYS AS(CASE WHEN pid>100 THEN 1 ELSE pid END) STORED${bad ? ",CONSTRAINT group_unique UNIQUE(g)" : ""})`,
                `INSERT INTO p VALUES${Array.from({ length: count }, (_, i) => `(${String(i + 1)})`).join(",")}`,
                `INSERT INTO c(id,pid,seen) VALUES${Array.from({ length: count }, (_, i) => `(${String(i + 1)},${String(i + 1)},0)`).join(",")}`,
              ],
              parentBody = `UPDATE trace SET p=p+${bad ? "1/(id-1)" : "OLD.id"} WHERE id=1; UPDATE c SET seen=seen+1 WHERE pid=OLD.id`,
              childBody = "UPDATE trace SET c=c+NEW.pid WHERE id=1",
              mutation =
                bad || explicit
                  ? "UPDATE p SET id=id+100"
                  : `UPDATE p SET id=((id+${String(shift)}-1)%${String(count)})+1`,
              f = await referentialFixture(
                [
                  ...setup,
                  `CREATE TRIGGER "Z_parent" AFTER UPDATE ON p BEGIN ${parentBody}; END`,
                  `CREATE TRIGGER child_log AFTER UPDATE ON c BEGIN ${childBody}; END`,
                ],
                published,
                null,
                256 << 20,
                mutation,
              );
            try {
              let expected = bad ? "23505" : null;
              if (explicit) {
                await pg.exec("DROP TABLE IF EXISTS c,p,trace CASCADE");
                for (const sql of setup) await pg.exec(sql);
                await pg.exec(
                  `CREATE OR REPLACE FUNCTION group_parent() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN ${parentBody}; RETURN NEW; END $$; CREATE TRIGGER "Z_parent" AFTER UPDATE ON p FOR EACH ROW EXECUTE FUNCTION group_parent(); CREATE OR REPLACE FUNCTION group_child() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN ${childBody}; RETURN NEW; END $$; CREATE TRIGGER child_log AFTER UPDATE ON c FOR EACH ROW EXECUTE FUNCTION group_child()`,
                );
                expected = await code(() => pg.exec(mutation));
                expect(expected).toBe(bad ? "22012" : null);
                reach.reach("explicit CASCADE PG boundary");
              }
              const actual = await code(async () => {
                await f.nativeRoot();
                await f.end();
              });
              expect(actual).toBe(expected);
              if (actual) await f.rollback();
              await f.retire();
              const trace = await f.core.execute("SELECT p,c FROM trace");
              if (!("batches" in trace)) throw new Error("trace");
              const values = trace.batches.flatMap((b) =>
                Array.from({ length: b.length }, (_, i) => b.columns.map((v) => valueAt(v, i))),
              );
              expect(values).toEqual([
                [bad ? 0 : sum, bad ? 0 : explicit ? 100 * count + sum : 2 * sum],
              ]);
              const children = await f.core.execute("SELECT id,pid,seen FROM c ORDER BY id");
              if (!("batches" in children)) throw new Error("children");
              expect(
                children.batches.flatMap((b) =>
                  Array.from({ length: b.length }, (_, i) => b.columns.map((v) => valueAt(v, i))),
                ),
              ).toEqual(
                Array.from({ length: count }, (_, i) => [
                  i + 1,
                  bad ? i + 1 : explicit ? i + 101 : ((i + shift) % count) + 1,
                  bad || explicit ? 0 : 1,
                ]),
              );
              if (explicit) {
                expect((await pg.query("SELECT p,c FROM trace")).rows).toEqual(
                  values.map(([p, c]) => ({ p, c })),
                );
              }
              reach.reach(count === 2 ? "swap" : "rotation");
              reach.reach(published ? "published" : "resident");
              if (!explicit)
                reach.reach(
                  bad ? "23505 before user22012" : "parent and child AFTER see complete group",
                );
            } finally {
              await f.close();
            }
            reach.reach("zero ownership");
          },
        ),
        {
          seed,
          numRuns: 16,
          examples: [
            [2, 1, false, false, false],
            [3, 2, false, true, false],
            [2, 1, true, true, false],
            [2, 1, true, false, true],
            [3, 1, false, true, true],
          ],
        },
      );
      reach.assertReached();
    } finally {
      await pg.close();
    }
  }, 120000);
