/** Actual compiled user bodies and native RI share PG's named AFTER queue. A one-row
 * text accumulator makes every BEFORE/AFTER continuation observable in strict order. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, NATIVE_BODY_RI_REACH } from "../../testing/reach.js";
import { oracleRows } from "./differential-adapter.js";
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
const quote = (s: string) => `"${s.replaceAll('"', '""')}"`;
for (const seed of seedsFor("next-native-body-ri-order", [0xb65f, 0xb660]))
  it(`executes PG ordered RI and real body continuations seed ${String(seed)}`, async () => {
    const pg = new PGlite(),
      reach = createReach("compiled user bodies and RI", NATIVE_BODY_RI_REACH);
    try {
      await fc.assert(
        fc.asyncProperty(
          fc.constantFrom("CASCADE", "SET NULL", "SET DEFAULT", "RESTRICT", "NO ACTION"),
          fc.boolean(),
          fc.integer({ min: 0, max: 4 }),
          fc.boolean(),
          async (mode, update, count, published) => {
            reach.beginHistory();
            const event = update ? "UPDATE" : "DELETE",
              setup = [
                "CREATE TABLE trace(id INT PRIMARY KEY,v TEXT)",
                "INSERT INTO trace VALUES(1,'')",
                "CREATE TABLE p(id INT PRIMARY KEY)",
                `CREATE TABLE c(id INT PRIMARY KEY,pid INT DEFAULT 99 REFERENCES p(id) ON DELETE ${mode} ON UPDATE ${mode})`,
                "INSERT INTO p VALUES(99)",
                ...(count
                  ? [
                      `INSERT INTO p VALUES${Array.from({ length: count }, (_, i) => `(${String(i + 1)})`).join(",")}`,
                      `INSERT INTO c VALUES${Array.from({ length: count }, (_, i) => `(${String(i + 1)},${String(i + 1)})`).join(",")}`,
                    ]
                  : []),
              ],
              triggers = [
                { name: "pb", table: "p", timing: "BEFORE", label: "B" },
                { name: "A_before", table: "p", timing: "AFTER", label: "A" },
                { name: "Z_after", table: "p", timing: "AFTER", label: "Z" },
                { name: "a_after", table: "p", timing: "AFTER", label: "a" },
                { name: "child_before", table: "c", timing: "BEFORE", label: "b" },
                { name: "child_after", table: "c", timing: "AFTER", label: "c" },
              ],
              childEvent = !update && mode === "CASCADE" ? "DELETE" : "UPDATE",
              body = (label: string) =>
                `UPDATE trace SET v=v || '${label}' || CAST(OLD.id AS TEXT) || ';' WHERE id=1`,
              native = [
                ...setup,
                ...triggers.map(
                  (t) =>
                    `CREATE TRIGGER ${quote(t.name)} ${t.timing} ${t.table === "p" ? event : childEvent} ON ${t.table} BEGIN ${body(t.label)}; END`,
                ),
              ],
              mutation = update ? "UPDATE p SET id=id+10 WHERE id<99" : "DELETE FROM p WHERE id<99",
              f = await referentialFixture(native, published, null, 256 << 20, mutation);
            try {
              await pg.exec("DROP TABLE IF EXISTS c,p,trace CASCADE");
              for (const sql of setup) await pg.exec(sql);
              for (const [i, t] of triggers.entries())
                await pg.exec(
                  `CREATE OR REPLACE FUNCTION rt${String(i)}() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN ${body(t.label)}; RETURN ${t.table === "p" ? (event === "DELETE" ? "OLD" : "NEW") : childEvent === "DELETE" ? "OLD" : "NEW"}; END $$; CREATE TRIGGER ${quote(t.name)} ${t.timing} ${t.table === "p" ? event : childEvent} ON ${t.table} FOR EACH ROW EXECUTE FUNCTION rt${String(i)}()`,
                );
              const expected = await code(() => pg.exec(mutation)),
                actual = await code(async () => {
                  await f.nativeRoot();
                  await f.end();
                });
              expect(actual, `${event} ${mode}`).toBe(expected);
              if (actual) await f.rollback();
              await f.retire();
              for (const table of ["p", "c", "trace"]) {
                const sql = `SELECT * FROM ${table} ORDER BY id`;
                const got = await f.core.execute(sql);
                if (!("batches" in got)) throw new Error("rows");
                expect(oracleRows(got), `${event} ${mode} ${table}`).toEqual(
                  (await pg.query(sql)).rows,
                );
              }
              reach.reach(mode);
              reach.reach(update ? "rekey" : "delete");
              reach.reach(published ? "published" : "resident");
              reach.reach(count ? "rowset" : "empty");
              reach.reach(actual ? "rollback" : "named continuations");
            } finally {
              await f.close();
            }
            reach.reach("zero ownership");
          },
        ),
        { seed, numRuns: 40 },
      );
      reach.assertReached();
    } finally {
      await pg.close();
    }
  }, 120000);
