/** Real owner qualification at the product's shipped one-further-level boundary.
 * PG is authority for accepted rows; deeper PG success is an explicit Minnow limit. */
import { PGlite } from "@electric-sql/pglite";
import fc from "fast-check";
import { expect, it } from "vitest";
import { MinnowDatabase } from "../../engine/database.js";
import { MemoryBlockStore } from "../../storage/index.js";
import { seedsFor } from "../../testing/seeds.js";
import { createReach, NATIVE_TRIGGER_SQL_DEPTH_REACH } from "../../testing/reach.js";
import { MinnowDatabaseClient } from "../owner/database-client.js";
import { publicOwner } from "./public-owner.test.fixture.js";
const capBytes = (process.env.MINNOW_TRIGGER_CAP_MIB === "32" ? 32 : 64) << 20;
const tables = ["src", "audit", "mirror", "leaf"];
for (const seed of seedsFor("next-native-trigger-sql-depth", [49541, 49542]))
  it(`qualifies the ordinary SQL cascade boundary (${String(seed)})`, async () => {
    const pg = new PGlite(),
      reach = createReach("ordinary SQL cascade boundary", NATIVE_TRIGGER_SQL_DEPTH_REACH);
    try {
      await fc.assert(
        fc.asyncProperty(
          fc.integer({ min: 0, max: 3 }),
          fc.integer({ min: 1, max: 3 }),
          fc.boolean(),
          fc.boolean(),
          async (rows, depth, before, published) => {
            reach.beginHistory();
            const owner = await publicOwner({ capBytes }),
              db = new MinnowDatabaseClient(owner.client, { ready: Promise.resolve() }),
              old = new MinnowDatabase(new MemoryBlockStore());
            try {
              await pg.exec("DROP TABLE IF EXISTS src,audit,mirror,leaf CASCADE");
              for (const table of tables) {
                const ddl = `CREATE TABLE ${table}(n INT)`;
                await db.execute(ddl);
                await old.execute(ddl);
                await pg.exec(ddl);
              }
              await db.execute("INSERT INTO src VALUES(0)");
              await old.execute("INSERT INTO src VALUES(0)");
              await pg.exec("INSERT INTO src VALUES(0)");
              const beforeRows = new Map<string, unknown>();
              for (const table of tables)
                beforeRows.set(table, (await pg.query(`SELECT n FROM ${table} ORDER BY n`)).rows);
              for (let i = 0; i < depth; i++) {
                const table = tables[i],
                  target = tables[i + 1];
                if (!table || !target) throw new Error("Missing cascade table");
                const timing = i === 0 && before ? "BEFORE" : "AFTER",
                  body = `INSERT INTO ${target} VALUES(NEW.n)`;
                const ddl = `CREATE TRIGGER t${String(i)} ${timing} INSERT ON ${table} BEGIN ${body}; END`;
                await db.execute(ddl);
                await old.execute(ddl);
                await pg.exec(
                  `CREATE OR REPLACE FUNCTION depth_f${String(i)}() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN ${body}; RETURN NEW; END $$; CREATE TRIGGER t${String(i)} ${timing} INSERT ON ${table} FOR EACH ROW EXECUTE FUNCTION depth_f${String(i)}()`,
                );
              }
              if (published) {
                await owner.core.harness.path.recycleLog();
                await owner.core.harness.path.quiesce();
              }
              const sql = `INSERT INTO src SELECT n FROM generate_series(1,${String(rows)})g(n)`;
              await pg.exec(sql);
              const refused = rows > 0 && depth === 3;
              if (refused) {
                await expect(db.execute(sql)).rejects.toMatchObject({
                  sqlState: "54001",
                  message: "Trigger cascade depth exceeded",
                });
                await expect(old.execute(sql)).rejects.toThrow("Trigger cascade depth exceeded");
                reach.reach("third matching level rolls back");
              } else {
                expect((await db.execute(sql)).affectedRows).toBe(rows);
                await old.execute(sql);
                reach.reach(depth === 1 ? "root trigger only" : "one further level");
              }
              for (const table of tables) {
                const query = `SELECT n FROM ${table} ORDER BY n`,
                  expected = refused ? beforeRows.get(table) : (await pg.query(query)).rows;
                expect((await db.query(query)).rows(), query).toEqual(expected);
                const shipped = await old.execute(query);
                if (shipped.kind !== "rows") throw new Error("Missing shipped rows");
                expect(shipped.result.rows, query).toEqual(expected);
              }
              reach.reach(before ? "BEFORE root" : "AFTER root");
              reach.reach(published ? "published" : "resident");
              if (rows === 0) reach.reach("empty source fires nothing");
              reach.reach("PG and shipped accepted rows");
            } finally {
              await old.close();
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
          numRuns: 6,
          examples: [
            [2, 1, false, false],
            [3, 2, true, true],
            [2, 3, false, false],
            [0, 3, true, true],
          ],
        },
      );
      reach.assertReached();
    } finally {
      await pg.close();
    }
  }, 120000);
