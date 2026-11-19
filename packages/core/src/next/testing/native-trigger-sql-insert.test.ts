/** Root INSERT parameters, BEFORE/AFTER and one further body level through the real owner. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { expect, it } from "vitest";
import { MinnowDatabase } from "../../engine/database.js";
import { MemoryBlockStore } from "../../storage/index.js";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, NATIVE_TRIGGER_SQL_INSERT_REACH } from "../../testing/reach.js";
import { MinnowDatabaseClient } from "../owner/database-client.js";
import { publicOwner } from "./public-owner.test.fixture.js";
const capBytes = (process.env.MINNOW_TRIGGER_CAP_MIB === "32" ? 32 : 64) << 20;
for (const seed of seedsFor("next-native-trigger-sql-insert", [46986, 46987]))
  it(`qualifies funded INSERT with PG and shipped bodies (${String(seed)})`, async () => {
    const pg = new PGlite(),
      reach = createReach("funded SQL INSERT", NATIVE_TRIGGER_SQL_INSERT_REACH);
    try {
      await fc.assert(
        fc.asyncProperty(
          fc.integer({ min: 0, max: 4 }),
          fc.boolean(),
          fc.integer({ min: 1, max: 16 }),
          fc.boolean(),
          async (rows, nil, n, published) => {
            reach.beginHistory();
            const owner = await publicOwner({ capBytes }),
              db = new MinnowDatabaseClient(owner.client, { ready: Promise.resolve() }),
              old = new MinnowDatabase(new MemoryBlockStore());
            const schema = [
              "CREATE TABLE src(id INT PRIMARY KEY,n INT CHECK(n>0))",
              "CREATE TABLE audit(phase TEXT,id INT,n INT)",
              "CREATE TABLE mirror(phase TEXT,id INT,n INT)",
              "INSERT INTO src VALUES(0,1)",
            ];
            const bodies = [
              {
                name: "before_src",
                table: "src",
                event: "INSERT",
                timing: "BEFORE",
                sql: "INSERT INTO audit VALUES('B',NEW.id,NEW.n)",
              },
              {
                name: "after_src",
                table: "src",
                event: "INSERT",
                timing: "AFTER",
                sql: "INSERT INTO audit VALUES('A',NEW.id,NEW.n)",
              },
              {
                name: "echo_audit",
                table: "audit",
                event: "INSERT",
                timing: "AFTER",
                sql: "INSERT INTO mirror VALUES(NEW.phase,NEW.id,NEW.n)",
              },
            ];
            try {
              await pg.exec("DROP TABLE IF EXISTS src,audit,mirror CASCADE");
              for (const sql of schema) {
                await db.execute(sql);
                await old.execute(sql);
                await pg.exec(sql);
              }
              for (const body of bodies) {
                const ddl = `CREATE TRIGGER ${body.name} ${body.timing} ${body.event} ON ${body.table} FOR EACH ROW BEGIN ${body.sql}; END`;
                await db.execute(ddl);
                await old.execute(ddl);
                await pg.exec(
                  `CREATE OR REPLACE FUNCTION insert_${body.name}() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN ${body.sql}; RETURN NEW; END $$; CREATE TRIGGER ${body.name} ${body.timing} ${body.event} ON ${body.table} FOR EACH ROW EXECUTE FUNCTION insert_${body.name}()`,
                );
              }
              if (published) {
                await owner.core.harness.path.recycleLog();
                await owner.core.harness.path.quiesce();
              }
              const sql = `INSERT INTO src(id,n) SELECT i,$1::int FROM generate_series(1,${String(rows)})g(i)`,
                value = nil ? null : n;
              expect((await db.execute(sql, [value])).affectedRows).toBe(rows);
              await old.execute(sql, [value]);
              await pg.query(sql, [value]);
              for (const query of [
                "SELECT * FROM src ORDER BY id",
                "SELECT * FROM audit ORDER BY id,phase,n NULLS FIRST",
                "SELECT * FROM mirror ORDER BY id,phase,n NULLS FIRST",
              ]) {
                const expected = (await pg.query(query)).rows,
                  shipped = await old.execute(query);
                expect((await db.query(query)).rows(), query).toEqual(expected);
                if (shipped.kind !== "rows") throw new Error("Missing shipped body rows");
                expect(shipped.result.rows, query).toEqual(expected);
              }
              reach.reach(rows === 0 ? "empty input" : "nonempty input");
              reach.reach(nil ? "NULL binding" : "integer binding");
              reach.reach(published ? "published target" : "resident target");
              reach.reach("BEFORE and AFTER bodies");
              reach.reach("one further level");
              reach.reach("PG and shipped exact rows");
              reach.reach("root count");
            } finally {
              await old.close();
              await db.close();
              await owner.close();
              expect(owner.core.harness.governor.usedBytes).toBe(0);
              expect(owner.core.spill.bytesInUse).toBe(0);
            }
            reach.reach("zero ownership");
          },
        ),
        {
          seed,
          numRuns: 6,
          examples: [
            [0, false, 2, false],
            [3, true, 1, true],
            [4, false, 16, false],
          ],
        },
      );
      reach.assertReached();
    } finally {
      await pg.close();
    }
  }, 120000);
