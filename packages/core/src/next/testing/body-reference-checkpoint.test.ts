/** Real compiled user AFTER updates an unchanged child key before parent RI. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, NATIVE_BODY_REFERENCE_REACH } from "../../testing/reach.js";
import { batch, ints } from "../write/test-support.js";
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
for (const seed of seedsFor("next-native-body-reference", [0xb66d, 0xb66e]))
  it(`checks compiled child subcommands at the right RI checkpoint seed ${String(seed)}`, async () => {
    const pg = new PGlite(),
      reach = createReach("compiled RI reference checkpoint", NATIVE_BODY_REFERENCE_REACH);
    try {
      await fc.assert(
        fc.asyncProperty(
          fc.boolean(),
          fc.boolean(),
          fc.boolean(),
          fc.integer({ min: 1, max: 99 }),
          async (update, fresh, published, n) => {
            reach.beginHistory();
            const setup = [
                "CREATE TABLE p(id INT PRIMARY KEY)",
                "CREATE TABLE c(id INT PRIMARY KEY,pid INT REFERENCES p(id) ON DELETE CASCADE ON UPDATE CASCADE,v INT)",
                "INSERT INTO p VALUES(1)",
                ...(!fresh ? [`INSERT INTO c VALUES(1,1,${String(n)})`] : []),
              ],
              event = update ? "UPDATE" : "DELETE",
              mutation = update ? "UPDATE p SET id=2" : "DELETE FROM p",
              body = "UPDATE c SET v=v+1 WHERE pid=OLD.id",
              f = await referentialFixture(
                [...setup, `CREATE TRIGGER "A_before_RI" AFTER ${event} ON p BEGIN ${body}; END`],
                published,
                null,
                256 << 20,
                mutation,
                {
                  ...(fresh
                    ? {
                        prior: async (tx) => {
                          const s = tx.statement({
                            rules: () => ({ generate: () => undefined, check: () => null }),
                          });
                          await s.table(2).insert(batch(ints([1]), ints([1]), ints([n])), null);
                          await s.end();
                        },
                      }
                    : {}),
                },
              );
            try {
              await pg.exec("DROP TABLE IF EXISTS c,p CASCADE");
              for (const sql of setup) await pg.exec(sql);
              await pg.exec(
                `CREATE OR REPLACE FUNCTION body_reference() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN ${body}; RETURN ${update ? "NEW" : "OLD"}; END $$; CREATE TRIGGER "A_before_RI" AFTER ${event} ON p FOR EACH ROW EXECUTE FUNCTION body_reference()`,
              );
              await pg.exec("BEGIN");
              if (fresh) await pg.exec(`INSERT INTO c VALUES(1,1,${String(n)})`);
              const expected = await code(() => pg.exec(mutation)),
                actual = await code(async () => {
                  await f.nativeRoot();
                  await f.end();
                });
              expect(actual).toBe(expected);
              expect(actual).toBe(fresh ? "23503" : null);
              if (actual) await f.rollback();
              await pg.exec(actual ? "ROLLBACK" : "COMMIT");
              reach.reach(update ? "rekey" : "delete");
              reach.reach(fresh ? "earlier transaction INSERT" : "committed child");
              reach.reach(published ? "published" : "resident");
            } finally {
              await pg.exec("ROLLBACK");
              await f.close();
            }
            reach.reach("zero ownership");
          },
        ),
        { seed, numRuns: 16 },
      );
      reach.assertReached();
    } finally {
      await pg.close();
    }
  }, 120000);
