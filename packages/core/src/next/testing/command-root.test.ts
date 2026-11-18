/** Real root assembly owns all hooks but delegates end/commit exactly once; PG supplies
 * row/trace/error authority. Owner SQL dispatch remains guarded. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { expect, it } from "vitest";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, NATIVE_COMMAND_ROOT_REACH } from "../../testing/reach.js";
import { ints } from "../write/test-support.js";
import { valueAt } from "../exec/kernels/test-support.js";
import { commandRootFixture } from "./command-root.test-support.js";
async function state(run: () => Promise<unknown>): Promise<string | null> {
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
for (const seed of seedsFor("next-native-command-root", [0xb675, 0xb676]))
  it(`assembles complete native roots against PG seed ${String(seed)}`, async () => {
    const pg = new PGlite(),
      reach = createReach("native command root setup", NATIVE_COMMAND_ROOT_REACH);
    try {
      await fc.assert(
        fc.asyncProperty(
          fc.constantFrom("INSERT", "UPDATE", "DELETE"),
          fc.integer({ min: 0, max: 5 }),
          fc.boolean(),
          fc.boolean(),
          fc.boolean(),
          async (event, count, published, fail, rollback) => {
            reach.beginHistory();
            const setup = [
              "CREATE TABLE audit(id INT,label TEXT)",
              "CREATE TABLE p(id INT PRIMARY KEY,v INT CHECK(v>0))",
              "CREATE TABLE c(id INT PRIMARY KEY,pid INT REFERENCES p(id) ON DELETE CASCADE ON UPDATE CASCADE)",
            ];
            if (event !== "INSERT" && count) {
              setup.push(
                `INSERT INTO p VALUES${Array.from({ length: count }, (_, i) => `(${String(i + 1)},${String(i + 10)})`).join(",")}`,
                `INSERT INTO c VALUES${Array.from({ length: count }, (_, i) => `(${String(i + 1)},${String(i + 1)})`).join(",")}`,
              );
            }
            const bodies = [
                {
                  name: "before_parent",
                  table: "p",
                  timing: "BEFORE",
                  event,
                  body: `INSERT INTO audit VALUES(${event === "INSERT" ? "NEW" : "OLD"}.id,'PB')`,
                },
                {
                  name: "Z_parent",
                  table: "p",
                  timing: "AFTER",
                  event,
                  body: `INSERT INTO audit VALUES(${event === "DELETE" ? "OLD" : "NEW"}.id,'PA')`,
                },
                ...(event === "DELETE"
                  ? [
                      {
                        name: "before_child",
                        table: "c",
                        timing: "BEFORE",
                        event: "DELETE",
                        body: "INSERT INTO audit VALUES(OLD.id,'CB')",
                      },
                      {
                        name: "after_child",
                        table: "c",
                        timing: "AFTER",
                        event: "DELETE",
                        body: "INSERT INTO audit VALUES(OLD.id,'CA')",
                      },
                      ...(fail
                        ? [
                            {
                              name: "fail_parent",
                              table: "p",
                              timing: "BEFORE",
                              event: "DELETE",
                              body: "INSERT INTO audit VALUES(OLD.id/(OLD.id-1),'fail')",
                            },
                          ]
                        : []),
                    ]
                  : []),
              ],
              delta = fail ? -1000 : 5,
              sql =
                event === "INSERT"
                  ? count
                    ? `INSERT INTO p VALUES${Array.from({ length: count }, (_, i) => `(${String(i + 1)},${String(fail ? -1 : i + 10)})`).join(",")}`
                    : "INSERT INTO p SELECT id,v FROM p WHERE false"
                  : event === "UPDATE"
                    ? "UPDATE p SET v=v+$1"
                    : "DELETE FROM p",
              f = await commandRootFixture(
                [
                  ...setup,
                  ...bodies.map(
                    (b) =>
                      `CREATE TRIGGER "${b.name}" ${b.timing} ${b.event} ON ${b.table} BEGIN ${b.body}; END`,
                  ),
                ],
                sql,
                published,
              );
            try {
              await pg.exec("DROP TABLE IF EXISTS c,p,audit CASCADE");
              for (const q of setup) await pg.exec(q);
              for (const b of bodies)
                await pg.exec(
                  `CREATE OR REPLACE FUNCTION ${b.name.toLowerCase()}() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN ${b.body}; RETURN ${b.event === "DELETE" ? "OLD" : "NEW"}; END $$; CREATE TRIGGER "${b.name}" ${b.timing} ${b.event} ON ${b.table} FOR EACH ROW EXECUTE FUNCTION ${b.name.toLowerCase()}()`,
                );
              await pg.exec("BEGIN");
              const expected = await state(() => pg.query(sql, event === "UPDATE" ? [delta] : []));
              let affected = -1;
              const actual = await state(async () => {
                affected = await f.root.run({ slots: event === "UPDATE" ? [ints([delta])] : [] });
                await f.root.end();
              });
              expect(actual).toBe(expected);
              if (!actual) expect(affected).toBe(count);
              if (actual || rollback) {
                await pg.exec("ROLLBACK");
                await f.retire();
                reach.reach("rollback");
              } else {
                const receipt = await f.tx.commit(null);
                await receipt.durable;
                await f.retire();
                await pg.exec("COMMIT");
                reach.reach("commit");
              }
              for (const query of [
                "SELECT id,v FROM p ORDER BY id",
                "SELECT id,pid FROM c ORDER BY id",
                "SELECT id,label FROM audit",
              ]) {
                const expectedRows = (await pg.query<Record<string, unknown>>(query)).rows,
                  result = await f.core.execute(query);
                if (!("batches" in result)) throw new Error("rows");
                expect(
                  result.batches.flatMap((b) =>
                    Array.from({ length: b.length }, (_, i) => b.columns.map((v) => valueAt(v, i))),
                  ),
                ).toEqual(expectedRows.map((row) => Object.values(row)));
              }
              reach.reach(event);
              reach.reach(published ? "published" : "resident");
              if (count === 0) reach.reach("zero matches");
              if (actual) reach.reach("atomic row/BEFORE error");
            } finally {
              await pg.exec("ROLLBACK");
              await f.close();
            }
            reach.reach("zero ownership");
          },
        ),
        {
          seed,
          numRuns: 24,
          examples: [
            ["INSERT", 3, false, false, false],
            ["UPDATE", 4, true, false, true],
            ["DELETE", 3, true, false, false],
            ["DELETE", 3, false, true, false],
            ["UPDATE", 0, false, true, false],
          ],
        },
      );
      reach.assertReached();
    } finally {
      await pg.close();
    }
  }, 120000);
