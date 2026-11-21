/** Concrete native + shipped body results; PG is the authority below the product depth limit. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { expect, it } from "vitest";
import { MinnowDatabase } from "../../engine/database.js";
import { MemoryBlockStore } from "../../storage/index.js";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, NATIVE_TRIGGER_BODIES_REACH } from "../../testing/reach.js";
import { nativeTriggerSession } from "./native-trigger-session.test-support.js";
async function oldRows(old: MinnowDatabase, query: string) {
  const result = await old.execute(query);
  if (result.kind !== "rows") throw new Error("Expected shipped query");
  return result.result.rows;
}
const queries = [
  "SELECT * FROM src ORDER BY id",
  "SELECT * FROM target ORDER BY id",
  "SELECT * FROM audit ORDER BY tag,n NULLS FIRST",
  "SELECT * FROM mirror ORDER BY n NULLS FIRST",
  "SELECT * FROM leaf ORDER BY n NULLS FIRST",
];
for (const seed of seedsFor("next-native-trigger-bodies", [0xb7e6, 0xb7e7])) {
  const reach = createReach("native trigger bodies", NATIVE_TRIGGER_BODIES_REACH);
  for (const event of ["insert", "update", "delete"] as const)
    it(`executes native/shipped ${event} bodies and depth law seed ${String(seed)}`, async () => {
      const pg = new PGlite();
      try {
        await fc.assert(
          fc.asyncProperty(
            fc.integer({ min: 1, max: 3 }),
            fc.boolean(),
            fc.integer({ min: 0, max: 2 }),
            fc.integer({ min: 1, max: 4 }),
            fc.boolean(),
            async (rows, after, depth, bodies, published) => {
              reach.beginHistory();
              const schema = [
                "CREATE TABLE src(id INT PRIMARY KEY,n INT)",
                "CREATE TABLE target(id INT PRIMARY KEY,n INT)",
                "CREATE TABLE audit(tag TEXT,n INT)",
                "CREATE TABLE mirror(n INT)",
                "CREATE TABLE leaf(n INT)",
              ];
              const values = Array.from(
                { length: rows },
                (_, i) => `(${String(i + 1)},${String(i + 3)})`,
              ).join(",");
              const source = event === "delete" ? "OLD" : "NEW",
                value = event === "update" ? "NEW.n-OLD.n" : `${source}.n`;
              const commands = [
                `INSERT INTO audit VALUES('head',${source}.n),('null',NULL)`,
                `UPDATE target SET n=n+(${value}) WHERE id=${source}.id`,
                `UPDATE target SET n=n*2 WHERE id=${source}.id`,
                `DELETE FROM target WHERE id=${source}.id+100`,
              ].slice(0, bodies);
              const specs = [
                {
                  name: "root_body",
                  table: "src",
                  event,
                  timing: after ? "AFTER" : "BEFORE",
                  commands,
                },
                ...(depth > 0
                  ? [
                      {
                        name: "echo",
                        table: "audit",
                        event: "insert",
                        timing: "AFTER",
                        commands: ["INSERT INTO mirror VALUES(NEW.n)"],
                      },
                    ]
                  : []),
                ...(depth > 1
                  ? [
                      {
                        name: "too_deep",
                        table: "mirror",
                        event: "insert",
                        timing: "AFTER",
                        commands: ["INSERT INTO leaf VALUES(NEW.n)"],
                      },
                    ]
                  : []),
              ];
              const seedSql = [
                `INSERT INTO target VALUES ${values}`,
                `INSERT INTO target VALUES ${Array.from({ length: rows }, (_, i) => `(${String(i + 101)},${String(i + 9)})`).join(",")}`,
                ...(event !== "insert" ? [`INSERT INTO src VALUES ${values}`] : []),
              ];
              const nativeDdl = specs.map(
                (s) =>
                  `CREATE TRIGGER ${s.name} ${s.timing} ${s.event} ON ${s.table} BEGIN ${s.commands.join(";")}; END`,
              );
              const sql =
                event === "insert"
                  ? `INSERT INTO src VALUES ${values}`
                  : event === "update"
                    ? "UPDATE src SET n=n+5"
                    : "DELETE FROM src";
              const native = await nativeTriggerSession(
                  [...schema, ...seedSql, ...nativeDdl],
                  sql,
                  published,
                ),
                old = new MinnowDatabase(new MemoryBlockStore());
              try {
                await pg.exec("DROP TABLE IF EXISTS src,target,audit,mirror,leaf CASCADE");
                for (const text of [...schema, ...seedSql]) {
                  await old.execute(text);
                  await pg.exec(text);
                }
                for (const text of nativeDdl) await old.execute(text);
                for (const [index, s] of specs.entries())
                  await pg.exec(
                    `CREATE OR REPLACE FUNCTION body_f${String(index)}() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN ${s.commands.join(";")}; RETURN ${s.event === "delete" ? "OLD" : "NEW"}; END $$; CREATE TRIGGER ${s.name} ${s.timing} ${s.event} ON ${s.table} FOR EACH ROW EXECUTE FUNCTION body_f${String(index)}()`,
                  );
                const before = new Map<string, unknown>();
                for (const query of queries) before.set(query, (await pg.query(query)).rows);
                if (depth === 2) {
                  await expect(native.execute({ slots: [] })).rejects.toMatchObject({
                    sqlState: "54001",
                    message: "Trigger cascade depth exceeded",
                  });
                  await expect(old.execute(sql)).rejects.toThrow("Trigger cascade depth exceeded");
                  for (const query of queries) {
                    expect(await native.query(query), `native depth rollback ${query}`).toEqual(
                      before.get(query),
                    );
                    expect(await oldRows(old, query), `shipped depth rollback ${query}`).toEqual(
                      before.get(query),
                    );
                  }
                  // PostgreSQL does not impose Minnow's documented one-further-level product cap.
                  await pg.exec(sql);
                  expect((await pg.query("SELECT * FROM leaf")).rows.length).toBeGreaterThan(0);
                  reach.reach("third matching level refuses and undoes all effects");
                } else {
                  expect(await native.execute({ slots: [] })).toBe(rows);
                  await old.execute(sql);
                  await pg.exec(sql);
                  for (const query of queries) {
                    const expected = (await pg.query(query)).rows;
                    expect(await native.query(query), `PG native body ${query}`).toEqual(expected);
                    expect(await oldRows(old, query), `PG shipped body ${query}`).toEqual(expected);
                  }
                  reach.reach(
                    depth === 1 ? "one further trigger level succeeds" : "root trigger only",
                  );
                  reach.reach("PG exact supported body results");
                }
                await native.commit();
                for (const query of queries)
                  expect(await native.committedQuery(query)).toEqual(await oldRows(old, query));
                reach.reach(event);
                reach.reach(after ? "AFTER" : "BEFORE");
                reach.reach(published ? "published source" : "resident source");
                if (event === "update" && depth < 2 && bodies >= 2)
                  reach.reach("OLD and NEW bindings");
                if (depth < 2) reach.reach("multi-value INSERT and NULL binding");
                if (bodies >= 3 && depth < 2) reach.reach("noncommuting ordered UPDATE bodies");
                if (bodies === 4 && depth < 2) reach.reach("DELETE body");
              } finally {
                await old.close();
                await native.close();
              }
              reach.reach("zero native ownership");
            },
          ),
          {
            seed,
            numRuns: 8,
            examples: [
              [2, false, 0, 4, false],
              [3, true, 1, 4, true],
              [2, true, 2, 4, false],
            ],
          },
        );
      } finally {
        await pg.close();
      }
    }, 120000);
  it(`meets concrete body Reach seed ${String(seed)}`, () => reach.assertReached());
}
