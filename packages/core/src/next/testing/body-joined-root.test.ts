/** A native root join seals its first target match before assignments; the accepted row
 * alone feeds compiled AFTER bodies. Different source values distinguish first/last policy. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, NATIVE_BODY_JOIN_REACH } from "../../testing/reach.js";
import { oracleRows } from "./differential-adapter.js";
import { referentialFixture } from "./referential.test-support.js";
for (const seed of seedsFor("next-native-body-joined-root", [0xb665, 0xb666]))
  it(`keeps first native join targets and accepted AFTER images seed ${String(seed)}`, async () => {
    const pg = new PGlite(),
      reach = createReach("native joined root", NATIVE_BODY_JOIN_REACH);
    try {
      await fc.assert(
        fc.asyncProperty(
          fc.integer({ min: 1, max: 8 }),
          fc.integer({ min: 1, max: 99 }),
          fc.boolean(),
          fc.boolean(),
          fc.boolean(),
          async (count, n, published, deleting, badRepeat) => {
            reach.beginHistory();
            const setup = [
                "CREATE TABLE p(id INT PRIMARY KEY,v INT CHECK(v>0))",
                "CREATE TABLE s(id INT,w INT)",
                "CREATE TABLE audit(id INT,v INT)",
                `INSERT INTO p VALUES${Array.from({ length: count }, (_, i) => `(${String(i + 1)},1)`).join(",")}`,
                `INSERT INTO s VALUES${Array.from({ length: count }, (_, i) => `(${String(i + 1)},${String(n)}),(${String(i + 1)},${String(badRepeat && !deleting ? 0 : n + 100)})`).join(",")}`,
              ],
              event = deleting ? "DELETE" : "UPDATE",
              source = deleting ? "OLD" : "NEW",
              body = `INSERT INTO audit VALUES(${source}.id,${source}.v)`,
              mutation = deleting
                ? "DELETE FROM p USING s WHERE p.id=s.id"
                : `UPDATE p SET v=${badRepeat ? "100/s.w" : "s.w"} FROM s WHERE p.id=s.id`,
              f = await referentialFixture(
                [...setup, `CREATE TRIGGER log AFTER ${event} ON p BEGIN ${body}; END`],
                published,
                null,
                256 << 20,
                mutation,
              );
            try {
              await pg.exec("DROP TABLE IF EXISTS audit,s,p CASCADE");
              for (const sql of setup) await pg.exec(sql);
              await pg.exec(
                `CREATE OR REPLACE FUNCTION join_log() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN ${body}; RETURN ${source}; END $$; CREATE TRIGGER log AFTER ${event} ON p FOR EACH ROW EXECUTE FUNCTION join_log()`,
              );
              const error = async (run: () => Promise<unknown>) => {
                try {
                  await run();
                  return null;
                } catch (error) {
                  if (typeof error === "object" && error !== null) {
                    if ("sqlState" in error && typeof error.sqlState === "string")
                      return error.sqlState;
                    if ("code" in error && typeof error.code === "string") return error.code;
                  }
                  throw error;
                }
              };
              const expected = await error(() => pg.exec(mutation));
              const actual = await error(async () => {
                await f.nativeRoot();
                await f.end();
              });
              expect(actual).toBe(expected);
              if (actual) await f.rollback();
              await f.retire();
              for (const table of ["p", "audit"]) {
                const sql = `SELECT * FROM ${table} ORDER BY id`;
                const got = await f.core.execute(sql);
                if (!("batches" in got)) throw new Error("rows");
                expect(oracleRows(got)).toEqual((await pg.query(sql)).rows);
                if (table === "audit")
                  expect(oracleRows(got)).toEqual(
                    actual
                      ? []
                      : Array.from({ length: count }, (_, i) => ({
                          id: i + 1,
                          v: deleting ? 1 : n,
                        })),
                  );
              }
              reach.reach(deleting ? "DELETE USING" : "UPDATE FROM");
              reach.reach(published ? "published" : "resident");
              reach.reach("duplicate targets");
              reach.reach("distinct source values");
              reach.reach("accepted images only");
              if (badRepeat && !deleting) reach.reach("duplicate expression error");
            } finally {
              await f.close();
            }
            reach.reach("zero ownership");
          },
        ),
        { seed, numRuns: 24 },
      );
      reach.assertReached();
    } finally {
      await pg.close();
    }
  }, 120000);
